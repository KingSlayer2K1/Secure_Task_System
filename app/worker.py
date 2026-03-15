import asyncio
import logging
from typing import Any

from app.config import WORKER_IDLE_SLEEP_SECONDS, WORKER_SIMULATED_WORK_SECONDS
from app.logging_utils import configure_logging
from app.models import JobStatus
from app.storage import (
    enqueue_job_item,
    get_job,
    increment_metric,
    pop_queue_item,
    push_dead_letter,
    update_job_fields,
)
from app.utils import current_time

configure_logging()
logger = logging.getLogger(__name__)


class JobCancelledError(Exception):
    pass


async def _cooperative_sleep(job_id: str, seconds: float) -> None:
    deadline = asyncio.get_running_loop().time() + max(0.0, seconds)
    while asyncio.get_running_loop().time() < deadline:
        job = await get_job(job_id)
        if job and (job.cancel_requested or job.status == JobStatus.CANCELLED):
            raise JobCancelledError(f"Job {job_id} was cancelled.")
        await asyncio.sleep(0.2)


async def _execute_task(job_id: str, task_type: str, payload: dict[str, Any]) -> str:
    if task_type == "sleep":
        await _cooperative_sleep(job_id, float(payload.get("seconds", WORKER_SIMULATED_WORK_SECONDS)))
        return f"Sleep task finished for {payload.get('seconds', WORKER_SIMULATED_WORK_SECONDS)} seconds."

    if task_type == "media_transcode":
        quality = str(payload.get("quality", "720p"))
        duration = 3.5 if quality in {"1080p", "4k"} else 2.5
        await _cooperative_sleep(job_id, duration)
        return f"Transcoded {payload.get('input_format', 'unknown')} to {payload.get('output_format', 'unknown')} at {quality}."

    if task_type == "thumbnail_generate":
        await _cooperative_sleep(job_id, 1.5)
        return f"Thumbnail generated at second {payload.get('frame_second', 0)}."

    await _cooperative_sleep(job_id, WORKER_SIMULATED_WORK_SECONDS)
    return f"Processed {task_type} securely."


async def _handle_failure(job_id: str, error: str, timed_out: bool) -> None:
    job = await get_job(job_id)
    if job is None:
        return

    final_status = JobStatus.TIMED_OUT if timed_out else JobStatus.FAILED
    now = current_time()
    if job.attempts < job.max_attempts and not job.cancel_requested:
        await update_job_fields(
            job_id,
            {
                "status": JobStatus.QUEUED.value,
                "updated_at": now,
                "error": error,
            },
        )
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
        await increment_metric("retried")
        logger.warning("Retrying job", extra={"job_id": job_id, "attempts": job.attempts, "max_attempts": job.max_attempts})
        return

    await update_job_fields(
        job_id,
        {
            "status": final_status.value,
            "updated_at": now,
            "error": error,
        },
    )
    await push_dead_letter(
        {
            "job_id": job_id,
            "user_id": job.user_id,
            "task_type": job.task_type,
            "payload": job.payload,
            "status": final_status.value,
            "attempts": job.attempts,
            "max_attempts": job.max_attempts,
            "error": error,
            "dead_lettered_at": now,
        }
    )
    await increment_metric("dead_lettered")
    await increment_metric(final_status.value)
    logger.error("Job moved to dead letter queue", extra={"job_id": job_id, "status": final_status.value, "error": error})


async def process_job(queue_item: dict[str, Any]) -> None:
    job_id = str(queue_item.get("job_id", ""))
    if not job_id:
        logger.error("Queue item missing job_id", extra={"queue_item": queue_item})
        return

    job = await get_job(job_id)
    if job is None:
        logger.warning("Job not found for queue item", extra={"job_id": job_id})
        return

    if job.cancel_requested or job.status == JobStatus.CANCELLED:
        await update_job_fields(
            job_id,
            {"status": JobStatus.CANCELLED.value, "updated_at": current_time()},
        )
        await increment_metric("cancelled")
        logger.info("Skipping cancelled job", extra={"job_id": job_id})
        return

    attempt = job.attempts + 1
    await update_job_fields(
        job_id,
        {
            "status": JobStatus.PROCESSING.value,
            "updated_at": current_time(),
            "attempts": attempt,
            "error": "",
            "result": "",
        },
    )
    await increment_metric("processing")
    logger.info("Processing job", extra={"job_id": job_id, "task_type": job.task_type, "attempt": attempt})

    try:
        result = await asyncio.wait_for(_execute_task(job_id, job.task_type, job.payload), timeout=job.timeout_seconds)
        latest = await get_job(job_id)
        if latest and (latest.cancel_requested or latest.status == JobStatus.CANCELLED):
            await update_job_fields(
                job_id,
                {"status": JobStatus.CANCELLED.value, "updated_at": current_time(), "result": "", "error": "Cancelled by user"},
            )
            await increment_metric("cancelled")
            logger.info("Job cancelled during execution", extra={"job_id": job_id})
            return

        await update_job_fields(
            job_id,
            {"status": JobStatus.COMPLETED.value, "updated_at": current_time(), "result": result, "error": ""},
        )
        await increment_metric("completed")
        logger.info("Completed job", extra={"job_id": job_id, "result": result})
    except JobCancelledError as exc:
        await update_job_fields(
            job_id,
            {"status": JobStatus.CANCELLED.value, "updated_at": current_time(), "error": str(exc), "result": ""},
        )
        await increment_metric("cancelled")
        logger.info("Cancelled job", extra={"job_id": job_id})
    except asyncio.TimeoutError:
        await _handle_failure(job_id, f"Job exceeded timeout of {job.timeout_seconds} seconds.", timed_out=True)
    except Exception as exc:
        await _handle_failure(job_id, str(exc), timed_out=False)


async def main() -> None:
    logger.info("Worker started")
    while True:
        try:
            queue_item = await pop_queue_item(timeout=1)
            if queue_item is None:
                continue
            await process_job(queue_item)
        except Exception as exc:
            logger.exception("Worker loop error", extra={"error": str(exc)})
            await asyncio.sleep(WORKER_IDLE_SLEEP_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
