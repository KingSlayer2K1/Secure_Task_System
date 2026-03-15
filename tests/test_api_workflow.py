import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.storage import get_job, pop_queue_item
from app.worker import process_job


async def _login(client: AsyncClient) -> str:
    response = await client.post("/token", data={"username": "admin", "password": "password123"})
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


def _auth_headers(token: str, idempotency_key: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {token}"}
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    return headers


@pytest.mark.asyncio
async def test_job_lifecycle_end_to_end():
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
            token = await _login(client)
            submit = await client.post(
                "/jobs/",
                headers=_auth_headers(token, "idem-key-12345"),
                json={
                    "task_type": "sleep",
                    "payload": {"seconds": 0.2},
                    "max_attempts": 2,
                    "timeout_seconds": 5,
                },
            )
            assert submit.status_code == 202, submit.text
            job_id = submit.json()["job_id"]

            submit_again = await client.post(
                "/jobs/",
                headers=_auth_headers(token, "idem-key-12345"),
                json={
                    "task_type": "sleep",
                    "payload": {"seconds": 0.2},
                    "max_attempts": 2,
                    "timeout_seconds": 5,
                },
            )
            assert submit_again.status_code == 202, submit_again.text
            assert submit_again.json()["job_id"] == job_id
            assert submit_again.json()["idempotent_replay"] is True

            queue_item = await pop_queue_item(timeout=1)
            assert queue_item is not None
            await process_job(queue_item)

            job = await get_job(job_id)
            assert job is not None
            assert job.status.value == "completed"

            status_response = await client.get(f"/jobs/{job_id}", headers=_auth_headers(token))
            assert status_response.status_code == 200
            assert status_response.json()["status"] == "completed"


@pytest.mark.asyncio
async def test_cancel_job():
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
            token = await _login(client)
            submit = await client.post(
                "/jobs/",
                headers=_auth_headers(token),
                json={"task_type": "sleep", "payload": {"seconds": 2}, "max_attempts": 2, "timeout_seconds": 10},
            )
            assert submit.status_code == 202
            job_id = submit.json()["job_id"]

            cancel = await client.post(f"/jobs/{job_id}/cancel", headers=_auth_headers(token))
            assert cancel.status_code == 200
            assert cancel.json()["status"] == "cancelled"
