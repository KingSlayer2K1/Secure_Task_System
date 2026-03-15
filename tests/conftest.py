import fakeredis.aioredis
import pytest
import pytest_asyncio

from app import storage, user_store


@pytest_asyncio.fixture(autouse=True)
async def isolated_test_state(tmp_path):
    storage._redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)  # type: ignore[attr-defined]
    user_store.USER_DB_PATH = str(tmp_path / "users.db")
    await user_store.init_user_db()
    yield
    await storage.close_redis_client()
    storage._redis_client = None  # type: ignore[attr-defined]
