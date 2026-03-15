import json
from typing import Any

import redis.asyncio as redis

from app.config import (
    DEAD_LETTER_QUEUE,
    METRICS_KEY,
    RATE_LIMIT_PREFIX,
    REDIS_URL,
    TASK_QUEUE,
    TOKEN_REVOCATION_PREFIX,
)
from app.models import Job, JobStatus

_redis_client: redis.Redis | None = None


def _get_redis_client() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


async def close_redis_client() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None


def _job_key(job_id: str) -> str:
    return f"job:{job_id}"


def _serialize_job(job: Job) -> dict[str, str]:
    return {
        "job_id": job.job_id,
        "user_id": job.user_id,
        "task_type": job.task_type,
        "payload": json.dumps(job.payload),
        "status": job.status.value,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "attempts": str(job.attempts),
        "max_attempts": str(job.max_attempts),
        "timeout_seconds": str(job.timeout_seconds),
        "idempotency_key": job.idempotency_key or "",
        "cancel_requested": "1" if job.cancel_requested else "0",
        "result": job.result or "",
        "error": job.error or "",
    }


def _parse_payload(raw_payload: str) -> dict[str, Any]:
    if not raw_payload:
        return {}
    try:
        decoded = json.loads(raw_payload)
    except json.JSONDecodeError:
        return {}
    if isinstance(decoded, dict):
        return decoded
    return {"raw": decoded}


def _parse_int(raw: str | None, default: int) -> int:
    try:
        return int(raw or default)
    except (TypeError, ValueError):
        return default


def _parse_bool(raw: str | None, default: bool = False) -> bool:
    if raw is None:
        return default
    return raw in {"1", "true", "True", "yes"}


def _parse_status(raw_status: str | None) -> JobStatus:
    try:
        return JobStatus(raw_status or JobStatus.FAILED.value)
    except ValueError:
        return JobStatus.FAILED


async def save_job(job: Job) -> None:
    redis_client = _get_redis_client()
    await redis_client.hset(_job_key(job.job_id), mapping=_serialize_job(job))


async def update_job_fields(job_id: str, updates: dict[str, str | int | bool | None]) -> None:
    serialized: dict[str, str] = {}
    for key, value in updates.items():
        if value is None:
            serialized[key] = ""
        elif isinstance(value, bool):
            serialized[key] = "1" if value else "0"
        else:
            serialized[key] = str(value)
    if serialized:
        redis_client = _get_redis_client()
        await redis_client.hset(_job_key(job_id), mapping=serialized)


async def get_job(job_id: str) -> Job | None:
    redis_client = _get_redis_client()
    raw = await redis_client.hgetall(_job_key(job_id))
    if not raw:
        return None
    return Job(
        job_id=raw.get("job_id", job_id),
        user_id=raw.get("user_id", ""),
        task_type=raw.get("task_type", ""),
        payload=_parse_payload(raw.get("payload", "{}")),
        status=_parse_status(raw.get("status")),
        created_at=raw.get("created_at", ""),
        updated_at=raw.get("updated_at", raw.get("created_at", "")),
        attempts=_parse_int(raw.get("attempts"), 0),
        max_attempts=_parse_int(raw.get("max_attempts"), 3),
        timeout_seconds=_parse_int(raw.get("timeout_seconds"), 30),
        idempotency_key=raw.get("idempotency_key") or None,
        cancel_requested=_parse_bool(raw.get("cancel_requested"), False),
        result=raw.get("result") or None,
        error=raw.get("error") or None,
    )


async def enqueue_job(job: Job) -> None:
    await enqueue_job_item(
        {
            "job_id": job.job_id,
            "user_id": job.user_id,
            "task_type": job.task_type,
            "payload": job.payload,
            "timeout_seconds": job.timeout_seconds,
            "max_attempts": job.max_attempts,
        }
    )


async def enqueue_job_item(item: dict[str, Any]) -> None:
    redis_client = _get_redis_client()
    await redis_client.lpush(TASK_QUEUE, json.dumps(item))


async def pop_queue_item(timeout: int = 0) -> dict[str, Any] | None:
    redis_client = _get_redis_client()
    queue_data = await redis_client.brpop(TASK_QUEUE, timeout=timeout)
    if not queue_data:
        return None
    _, item = queue_data
    decoded = json.loads(item)
    if not isinstance(decoded, dict):
        return None
    return decoded


async def push_dead_letter(item: dict[str, Any]) -> None:
    redis_client = _get_redis_client()
    await redis_client.lpush(DEAD_LETTER_QUEUE, json.dumps(item))


async def get_dead_letter_items(limit: int = 50) -> list[dict[str, Any]]:
    redis_client = _get_redis_client()
    raw_items = await redis_client.lrange(DEAD_LETTER_QUEUE, 0, max(0, limit - 1))
    parsed: list[dict[str, Any]] = []
    for raw in raw_items:
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            parsed.append(item)
    return parsed


def _idempotency_key(user_id: str, idempotency_key: str) -> str:
    return f"idempotency:{user_id}:{idempotency_key}"


async def bind_idempotency_key(user_id: str, idempotency_key: str, job_id: str, ttl_seconds: int) -> bool:
    redis_client = _get_redis_client()
    result = await redis_client.set(_idempotency_key(user_id, idempotency_key), job_id, ex=ttl_seconds, nx=True)
    return bool(result)


async def get_job_id_by_idempotency_key(user_id: str, idempotency_key: str) -> str | None:
    redis_client = _get_redis_client()
    return await redis_client.get(_idempotency_key(user_id, idempotency_key))


async def increment_metric(metric_name: str, amount: int = 1) -> None:
    redis_client = _get_redis_client()
    await redis_client.hincrby(METRICS_KEY, metric_name, amount)


async def get_metrics() -> dict[str, int]:
    redis_client = _get_redis_client()
    raw = await redis_client.hgetall(METRICS_KEY)
    metrics: dict[str, int] = {}
    for key, value in raw.items():
        metrics[key] = _parse_int(value, 0)
    return metrics


async def get_queue_depth() -> int:
    redis_client = _get_redis_client()
    return int(await redis_client.llen(TASK_QUEUE))


async def get_dead_letter_depth() -> int:
    redis_client = _get_redis_client()
    return int(await redis_client.llen(DEAD_LETTER_QUEUE))


async def revoke_token_jti(jti: str, expires_in_seconds: int) -> None:
    redis_client = _get_redis_client()
    key = f"{TOKEN_REVOCATION_PREFIX}{jti}"
    await redis_client.set(key, "1", ex=max(1, expires_in_seconds))


async def is_token_revoked(jti: str) -> bool:
    redis_client = _get_redis_client()
    key = f"{TOKEN_REVOCATION_PREFIX}{jti}"
    exists = await redis_client.exists(key)
    return bool(exists)


def _rate_limit_key(bucket: str, identifier: str) -> str:
    safe_identifier = identifier.replace(" ", "_")
    return f"{RATE_LIMIT_PREFIX}{bucket}:{safe_identifier}"


async def check_rate_limit(
    bucket: str,
    identifier: str,
    limit: int,
    window_seconds: int,
) -> tuple[bool, int]:
    redis_client = _get_redis_client()
    key = _rate_limit_key(bucket, identifier)
    pipe = redis_client.pipeline()
    pipe.incr(key)
    pipe.ttl(key)
    current, ttl = await pipe.execute()

    if int(current) == 1:
        await redis_client.expire(key, window_seconds)
        ttl = window_seconds
    elif int(ttl) < 0:
        await redis_client.expire(key, window_seconds)
        ttl = window_seconds

    allowed = int(current) <= limit
    retry_after = max(1, int(ttl))
    return allowed, retry_after


async def ping_redis() -> bool:
    redis_client = _get_redis_client()
    return bool(await redis_client.ping())
