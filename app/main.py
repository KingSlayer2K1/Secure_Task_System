import logging
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.security import OAuth2PasswordRequestForm

from app.config import (
    AUTH_RATE_LIMIT_PER_WINDOW,
    DEFAULT_JOB_TIMEOUT_SECONDS,
    IDEMPOTENCY_TTL_SECONDS,
    JOB_SUBMIT_RATE_LIMIT_PER_WINDOW,
    MAX_ALLOWED_ATTEMPTS,
    RATE_LIMIT_WINDOW_SECONDS,
)
from app.logging_utils import clear_request_id, configure_logging, set_request_id
from app.models import Job, JobStatus, Role
from app.schemas import (
    CancelJobResponse,
    HealthResponse,
    JobCreateRequest,
    JobCreateResponse,
    JobStatusResponse,
    MetricsResponse,
    Token,
    TokenRefreshRequest,
    TokenRevokeRequest,
    UserInfoResponse,
)
from app.security import (
    authenticate_user,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
    get_user_from_refresh_token,
    require_role,
    revoke_token,
)
from app.storage import (
    bind_idempotency_key,
    check_rate_limit,
    close_redis_client,
    enqueue_job,
    get_dead_letter_depth,
    get_dead_letter_items,
    get_job,
    get_job_id_by_idempotency_key,
    get_metrics,
    get_queue_depth,
    increment_metric,
    ping_redis,
    save_job,
    update_job_fields,
)
from app.user_store import init_user_db, ping_users_db
from app.utils import clamp, current_time, generate_job_id
from app.validators import validate_payload_for_task

configure_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_user_db()
    logger.info("Initialized user database")
    yield
    await close_redis_client()


app = FastAPI(
    title="Secure Distributed Task API",
    description=(
        "Secure async job scheduling API with JWT auth, idempotency keys, retry/DLQ worker pattern, "
        "job cancellation, and Redis-backed observability."
    ),
    version="2.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid4().hex
    set_request_id(request_id)
    try:
        response = await call_next(request)
    finally:
        clear_request_id()
    response.headers["x-request-id"] = request_id
    return response


async def _enforce_rate_limit(bucket: str, identifier: str, limit: int) -> None:
    allowed, retry_after = await check_rate_limit(
        bucket=bucket,
        identifier=identifier,
        limit=limit,
        window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    )
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Please retry later.",
            headers={"Retry-After": str(retry_after)},
        )


def _assert_job_access(job: Job, current_user: dict[str, str]) -> None:
    is_owner = job.user_id == current_user["username"]
    is_admin = current_user.get("role") == Role.ADMIN.value
    if not is_owner and not is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized for this job")


@app.get("/health/liveness", response_model=HealthResponse, tags=["health"])
async def liveness() -> HealthResponse:
    return HealthResponse(status="alive", timestamp=current_time())


@app.get("/health/readiness", response_model=HealthResponse, tags=["health"])
async def readiness() -> HealthResponse:
    redis_ok = await ping_redis()
    db_ok = await ping_users_db()
    if redis_ok and db_ok:
        return HealthResponse(status="ready", timestamp=current_time(), redis="ok", users_db="ok")
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=HealthResponse(
            status="not_ready",
            timestamp=current_time(),
            redis="ok" if redis_ok else "down",
            users_db="ok" if db_ok else "down",
        ).model_dump(),
    )


@app.post("/token", response_model=Token, tags=["auth"])
async def login_for_access_token(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> Token:
    client_ip = request.client.host if request.client else "unknown"
    await _enforce_rate_limit("auth", f"{client_ip}:{form_data.username}", AUTH_RATE_LIMIT_PER_WINDOW)

    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        await increment_metric("auth_failures")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token, access_expires = create_access_token(user["username"], user["role"])
    refresh_token, _ = create_refresh_token(user["username"], user["role"])
    logger.info("Issued tokens", extra={"username": user["username"], "role": user["role"]})
    return Token(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in_seconds=access_expires,
    )


@app.post("/token/refresh", response_model=Token, tags=["auth"])
async def refresh_access_token(payload: TokenRefreshRequest) -> Token:
    user = await get_user_from_refresh_token(payload.refresh_token)
    await revoke_token(payload.refresh_token)

    access_token, access_expires = create_access_token(user["username"], user["role"])
    refresh_token, _ = create_refresh_token(user["username"], user["role"])
    logger.info("Refreshed token", extra={"username": user["username"]})
    return Token(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in_seconds=access_expires,
    )


@app.post("/token/revoke", status_code=status.HTTP_204_NO_CONTENT, tags=["auth"])
async def revoke_user_token(
    payload: TokenRevokeRequest,
    current_user: dict[str, str] = Depends(get_current_user),
) -> Response:
    token_payload = decode_token(payload.token)
    token_owner = token_payload.get("sub")
    if (
        not isinstance(token_owner, str)
        or (token_owner != current_user["username"] and current_user.get("role") != Role.ADMIN.value)
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to revoke this token.")
    await revoke_token(payload.token)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/users/me", response_model=UserInfoResponse, tags=["users"])
async def get_me(current_user: dict[str, str] = Depends(get_current_user)) -> UserInfoResponse:
    role = Role(current_user.get("role", Role.USER.value))
    return UserInfoResponse(username=current_user["username"], role=role)


@app.post("/jobs/", response_model=JobCreateResponse, status_code=status.HTTP_202_ACCEPTED, tags=["jobs"])
async def submit_job(
    request: Request,
    job_request: JobCreateRequest,
    current_user: dict[str, str] = Depends(get_current_user),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> JobCreateResponse:
    client_ip = request.client.host if request.client else "unknown"
    await _enforce_rate_limit(
        "submit_job",
        f"{current_user['username']}:{client_ip}",
        JOB_SUBMIT_RATE_LIMIT_PER_WINDOW,
    )

    try:
        payload = validate_payload_for_task(job_request.task_type, dict(job_request.payload))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    max_attempts = clamp(job_request.max_attempts, 1, MAX_ALLOWED_ATTEMPTS)
    timeout_seconds = clamp(job_request.timeout_seconds, 1, max(1, DEFAULT_JOB_TIMEOUT_SECONDS * 10))

    new_job_id = generate_job_id()
    if idempotency_key:
        normalized_key = idempotency_key.strip()
        if len(normalized_key) < 8 or len(normalized_key) > 128:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Idempotency-Key must be between 8 and 128 characters.",
            )
        bound = await bind_idempotency_key(
            user_id=current_user["username"],
            idempotency_key=normalized_key,
            job_id=new_job_id,
            ttl_seconds=IDEMPOTENCY_TTL_SECONDS,
        )
        if not bound:
            existing_job_id = await get_job_id_by_idempotency_key(current_user["username"], normalized_key)
            if not existing_job_id:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Idempotency key conflict.")
            existing_job = await get_job(existing_job_id)
            if existing_job:
                return JobCreateResponse(
                    job_id=existing_job.job_id,
                    status=existing_job.status,
                    idempotent_replay=True,
                )

    now = current_time()
    job = Job(
        job_id=new_job_id,
        user_id=current_user["username"],
        task_type=job_request.task_type,
        payload=payload,
        status=JobStatus.QUEUED,
        created_at=now,
        updated_at=now,
        attempts=0,
        max_attempts=max_attempts,
        timeout_seconds=timeout_seconds,
        idempotency_key=idempotency_key.strip() if idempotency_key else None,
        cancel_requested=False,
        result=None,
        error=None,
    )
    await save_job(job)
    await enqueue_job(job)
    await increment_metric("submitted")
    logger.info("Queued job", extra={"job_id": job.job_id, "task_type": job.task_type, "user_id": job.user_id})
    return JobCreateResponse(job_id=job.job_id, status=job.status, idempotent_replay=False)


@app.post("/tasks/", response_model=JobCreateResponse, status_code=status.HTTP_202_ACCEPTED, tags=["jobs"])
async def submit_task_alias(
    request: Request,
    job_request: JobCreateRequest,
    current_user: dict[str, str] = Depends(get_current_user),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> JobCreateResponse:
    return await submit_job(request, job_request, current_user, idempotency_key)


@app.get("/jobs/{job_id}", response_model=JobStatusResponse, tags=["jobs"])
async def get_job_status(job_id: str, current_user: dict[str, str] = Depends(get_current_user)) -> JobStatusResponse:
    job = await get_job(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    _assert_job_access(job, current_user)
    return JobStatusResponse(**job.model_dump())


@app.get("/tasks/{job_id}", response_model=JobStatusResponse, tags=["jobs"])
async def get_task_status_alias(job_id: str, current_user: dict[str, str] = Depends(get_current_user)) -> JobStatusResponse:
    return await get_job_status(job_id, current_user)


@app.post("/jobs/{job_id}/cancel", response_model=CancelJobResponse, tags=["jobs"])
async def cancel_job(job_id: str, current_user: dict[str, str] = Depends(get_current_user)) -> CancelJobResponse:
    job = await get_job(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    _assert_job_access(job, current_user)

    if job.status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.TIMED_OUT}:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Cannot cancel a {job.status.value} job.")

    if job.status == JobStatus.CANCELLED and job.cancel_requested:
        return CancelJobResponse(job_id=job.job_id, status=job.status, cancel_requested=True)

    updates: dict[str, str | int | bool | None] = {
        "cancel_requested": True,
        "updated_at": current_time(),
    }
    if job.status in {JobStatus.QUEUED, JobStatus.PROCESSING}:
        updates["status"] = JobStatus.CANCELLED.value
    await update_job_fields(job.job_id, updates)
    await increment_metric("cancelled")
    logger.info("Cancel requested", extra={"job_id": job.job_id, "user_id": current_user["username"]})
    return CancelJobResponse(job_id=job.job_id, status=JobStatus.CANCELLED, cancel_requested=True)


@app.get(
    "/admin/dead-letter",
    tags=["admin"],
    dependencies=[Depends(require_role(Role.ADMIN))],
)
async def list_dead_letters(limit: int = 25) -> list[dict[str, object]]:
    bounded_limit = clamp(limit, 1, 200)
    return await get_dead_letter_items(limit=bounded_limit)


@app.get(
    "/metrics",
    response_model=MetricsResponse,
    tags=["admin"],
    dependencies=[Depends(require_role(Role.ADMIN))],
)
async def metrics() -> MetricsResponse:
    metrics_map = await get_metrics()
    metrics_map["queue_depth"] = await get_queue_depth()
    metrics_map["dead_letter_depth"] = await get_dead_letter_depth()
    return MetricsResponse(**metrics_map)
