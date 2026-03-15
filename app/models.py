from enum import Enum
from typing import Any, Dict

from pydantic import BaseModel, Field


class Role(str, Enum):
    ADMIN = "admin"
    USER = "user"


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class Job(BaseModel):
    job_id: str = Field(..., min_length=1, max_length=128)
    user_id: str = Field(..., min_length=1, max_length=128)
    task_type: str = Field(..., min_length=1, max_length=128)
    payload: Dict[str, Any] = Field(default_factory=dict)
    status: JobStatus
    created_at: str
    updated_at: str
    attempts: int = Field(default=0, ge=0, le=100)
    max_attempts: int = Field(default=3, ge=1, le=10)
    timeout_seconds: int = Field(default=30, ge=1, le=600)
    idempotency_key: str | None = None
    cancel_requested: bool = False
    result: str | None = None
    error: str | None = None
