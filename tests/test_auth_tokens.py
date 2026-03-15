import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_refresh_and_revoke_token_flow():
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
            login = await client.post("/token", data={"username": "admin", "password": "password123"})
            assert login.status_code == 200, login.text
            tokens = login.json()
            access_token = tokens["access_token"]
            refresh_token = tokens["refresh_token"]

            refreshed = await client.post("/token/refresh", json={"refresh_token": refresh_token})
            assert refreshed.status_code == 200, refreshed.text
            new_refresh = refreshed.json()["refresh_token"]
            assert new_refresh != refresh_token

            revoke = await client.post(
                "/token/revoke",
                headers={"Authorization": f"Bearer {access_token}"},
                json={"token": access_token},
            )
            assert revoke.status_code == 204, revoke.text

            me = await client.get("/users/me", headers={"Authorization": f"Bearer {access_token}"})
            assert me.status_code == 401
