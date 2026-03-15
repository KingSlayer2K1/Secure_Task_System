import pytest

from app.validators import validate_payload_for_task


def test_validate_media_transcode_payload():
    payload = {"input_format": "MP4", "output_format": "hls", "quality": "1080p"}
    validated = validate_payload_for_task("media_transcode", payload)
    assert validated["input_format"] == "mp4"
    assert validated["output_format"] == "hls"


def test_validate_thumbnail_payload_missing_key():
    with pytest.raises(ValueError):
        validate_payload_for_task("thumbnail_generate", {"video_url": "https://example.com/vid.mp4"})


def test_validate_sleep_bounds():
    with pytest.raises(ValueError):
        validate_payload_for_task("sleep", {"seconds": 200})
