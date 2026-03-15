import json
from typing import Any

from app.config import MAX_PAYLOAD_BYTES


def _require_keys(payload: dict[str, Any], keys: list[str]) -> None:
    missing = [key for key in keys if key not in payload]
    if missing:
        raise ValueError(f"Missing required payload keys: {', '.join(missing)}")


def _ensure_payload_size(payload: dict[str, Any]) -> None:
    try:
        encoded = json.dumps(payload)
    except (TypeError, ValueError) as exc:
        raise ValueError("Payload must be JSON serializable.") from exc
    if len(encoded.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise ValueError(f"Payload is too large. Limit is {MAX_PAYLOAD_BYTES} bytes.")


def validate_payload_for_task(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object.")

    _ensure_payload_size(payload)

    if task_type == "media_transcode":
        _require_keys(payload, ["input_format", "output_format"])
        input_format = str(payload["input_format"]).lower()
        output_format = str(payload["output_format"]).lower()
        if input_format not in {"mp4", "mov", "mkv", "avi"}:
            raise ValueError("input_format must be one of: mp4, mov, mkv, avi")
        if output_format not in {"hls", "mp4", "dash"}:
            raise ValueError("output_format must be one of: hls, mp4, dash")
        payload["input_format"] = input_format
        payload["output_format"] = output_format
        return payload

    if task_type == "thumbnail_generate":
        _require_keys(payload, ["video_url", "frame_second"])
        frame_second = payload["frame_second"]
        if not isinstance(frame_second, (int, float)) or frame_second < 0:
            raise ValueError("frame_second must be a positive number.")
        payload["frame_second"] = float(frame_second)
        payload["video_url"] = str(payload["video_url"])
        return payload

    if task_type == "sleep":
        seconds = payload.get("seconds", 2)
        if not isinstance(seconds, (int, float)) or seconds < 0 or seconds > 120:
            raise ValueError("sleep.seconds must be between 0 and 120.")
        payload["seconds"] = float(seconds)
        return payload

    return payload
