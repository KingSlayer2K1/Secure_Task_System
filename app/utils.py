from datetime import datetime, timezone
from uuid import uuid4


def generate_job_id() -> str:
    return f"job_{uuid4().hex}"


def current_time() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clamp(value: int, minimum: int, maximum: int) -> int:
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value
