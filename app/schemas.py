from typing import Any, Dict

from pydantic import BaseModel, Field

from app.models import JobStatus, Role


class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in_seconds: int


class TokenRefreshRequest(BaseModel):
    refresh_token: str = Field(..., min_length=20)


class TokenRevokeRequest(BaseModel):
    token: str = Field(..., min_length=20)


class JobCreateRequest(BaseModel):
    task_type: str = Field(
        ...,
        min_length=1,
        max_length=128,
        pattern=r"^[a-zA-Z0-9_-]+$",
        description="Task category used by worker logic, e.g. media_transcode, thumbnail_generate, sleep.",
    )
    payload: Dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1, le=5)
    timeout_seconds: int = Field(default=30, ge=1, le=300)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "task_type": "media_transcode",
                    "payload": {
                        "input_format": "mp4",
                        "output_format": "hls",
                        "quality": "1080p",
                    },
                    "max_attempts": 3,
                    "timeout_seconds": 45,
                },
                {
                    "task_type": "thumbnail_generate",
                    "payload": {"video_url": "https://cdn.example.com/video.mp4", "frame_second": 12},
                    "max_attempts": 2,
                    "timeout_seconds": 20,
                },
            ]
        }
    }


class JobCreateResponse(BaseModel):
    job_id: str
    status: JobStatus
    idempotent_replay: bool = False


class JobStatusResponse(BaseModel):
    job_id: str
    user_id: str
    task_type: str
    payload: Dict[str, Any]
    status: JobStatus
    created_at: str
    updated_at: str
    attempts: int
    max_attempts: int
    timeout_seconds: int
    idempotency_key: str | None = None
    cancel_requested: bool
    result: str | None = None
    error: str | None = None


class CancelJobResponse(BaseModel):
    job_id: str
    status: JobStatus
    cancel_requested: bool


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    redis: str | None = None
    users_db: str | None = None


class MetricsResponse(BaseModel):
    submitted: int = 0
    processing: int = 0
    completed: int = 0
    failed: int = 0
    timed_out: int = 0
    retried: int = 0
    cancelled: int = 0
    dead_lettered: int = 0
    auth_failures: int = 0
    queue_depth: int = 0
    dead_letter_depth: int = 0


class UserInfoResponse(BaseModel):
    username: str
    role: Role
