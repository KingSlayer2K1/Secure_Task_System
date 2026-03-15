import os


def _as_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        value = default
    if minimum is not None and value < minimum:
        return minimum
    return value


ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()

REDIS_URL = os.getenv("REDIS_URL", "redis://:secure_redis_password@redis:6379/0")
SECRET_KEY = os.getenv("SECRET_KEY", "dev_only_change_me_please_32_chars_minimum_secret_key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = _as_int("ACCESS_TOKEN_EXPIRE_MINUTES", 30, minimum=1)
REFRESH_TOKEN_EXPIRE_DAYS = _as_int("REFRESH_TOKEN_EXPIRE_DAYS", 7, minimum=1)

IDEMPOTENCY_TTL_SECONDS = _as_int("IDEMPOTENCY_TTL_SECONDS", 24 * 60 * 60, minimum=60)
RATE_LIMIT_WINDOW_SECONDS = _as_int("RATE_LIMIT_WINDOW_SECONDS", 60, minimum=1)
AUTH_RATE_LIMIT_PER_WINDOW = _as_int("AUTH_RATE_LIMIT_PER_WINDOW", 10, minimum=1)
JOB_SUBMIT_RATE_LIMIT_PER_WINDOW = _as_int("JOB_SUBMIT_RATE_LIMIT_PER_WINDOW", 30, minimum=1)

DEFAULT_JOB_TIMEOUT_SECONDS = _as_int("DEFAULT_JOB_TIMEOUT_SECONDS", 30, minimum=1)
DEFAULT_MAX_ATTEMPTS = _as_int("DEFAULT_MAX_ATTEMPTS", 3, minimum=1)
MAX_ALLOWED_ATTEMPTS = _as_int("MAX_ALLOWED_ATTEMPTS", 5, minimum=1)

MAX_PAYLOAD_BYTES = _as_int("MAX_PAYLOAD_BYTES", 16 * 1024, minimum=512)

WORKER_IDLE_SLEEP_SECONDS = float(os.getenv("WORKER_IDLE_SLEEP_SECONDS", "1"))
WORKER_SIMULATED_WORK_SECONDS = float(os.getenv("WORKER_SIMULATED_WORK_SECONDS", "2"))

USER_DB_PATH = os.getenv("USER_DB_PATH", "data/users.db")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "password123")

TOKEN_REVOCATION_PREFIX = "token:revoked:"
RATE_LIMIT_PREFIX = "rate_limit:"
METRICS_KEY = "metrics:jobs"
TASK_QUEUE = "queue:jobs"
DEAD_LETTER_QUEUE = "queue:jobs:dead"

if ENVIRONMENT == "production" and SECRET_KEY == "dev_only_change_me_please_32_chars_minimum_secret_key":
    raise RuntimeError("SECRET_KEY must be set in production.")
