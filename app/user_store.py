import asyncio
import os
import sqlite3
from datetime import datetime, timezone

from passlib.context import CryptContext

from app.config import ADMIN_PASSWORD, ADMIN_USERNAME, USER_DB_PATH
from app.models import Role

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _connect() -> sqlite3.Connection:
    _ensure_parent_dir(USER_DB_PATH)
    conn = sqlite3.connect(USER_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_user_db_sync() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )
        row = conn.execute(
            "SELECT username FROM users WHERE username = ?",
            (ADMIN_USERNAME,),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO users (username, password_hash, role, is_active, created_at) VALUES (?, ?, ?, 1, ?)",
                (ADMIN_USERNAME, pwd_context.hash(ADMIN_PASSWORD), Role.ADMIN.value, _utc_now()),
            )
        conn.commit()


def _get_user_sync(username: str) -> dict[str, str] | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT username, password_hash, role, is_active FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    if row is None:
        return None
    return {
        "username": str(row["username"]),
        "password_hash": str(row["password_hash"]),
        "role": str(row["role"]),
        "is_active": bool(row["is_active"]),
    }


def _create_user_sync(username: str, plain_password: str, role: Role = Role.USER) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO users (username, password_hash, role, is_active, created_at)
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(username) DO UPDATE SET password_hash=excluded.password_hash, role=excluded.role, is_active=1
            """,
            (username, pwd_context.hash(plain_password), role.value, _utc_now()),
        )
        conn.commit()


async def init_user_db() -> None:
    await asyncio.to_thread(_init_user_db_sync)


async def get_user(username: str) -> dict[str, str] | None:
    return await asyncio.to_thread(_get_user_sync, username)


async def create_or_update_user(username: str, plain_password: str, role: Role = Role.USER) -> None:
    await asyncio.to_thread(_create_user_sync, username, plain_password, role)


def _ping_users_db_sync() -> bool:
    try:
        with _connect() as conn:
            row = conn.execute("SELECT 1").fetchone()
        return bool(row)
    except sqlite3.Error:
        return False


async def ping_users_db() -> bool:
    return await asyncio.to_thread(_ping_users_db_sync)
