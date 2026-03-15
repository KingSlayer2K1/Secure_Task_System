from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
import jwt

from app.config import ACCESS_TOKEN_EXPIRE_MINUTES, ALGORITHM, REFRESH_TOKEN_EXPIRE_DAYS, SECRET_KEY
from app.models import Role
from app.storage import is_token_revoked, revoke_token_jti
from app.user_store import get_user

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def hash_password(plain_password: str) -> str:
    return pwd_context.hash(plain_password)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _token_payload(username: str, role: str, expires_delta: timedelta, token_type: str) -> dict[str, str | int]:
    issued_at = _utc_now()
    expires_at = issued_at + expires_delta
    return {
        "sub": username,
        "role": role,
        "typ": token_type,
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
        "jti": uuid4().hex,
    }


def create_access_token(username: str, role: str) -> tuple[str, int]:
    expires_seconds = ACCESS_TOKEN_EXPIRE_MINUTES * 60
    payload = _token_payload(
        username=username,
        role=role,
        expires_delta=timedelta(seconds=expires_seconds),
        token_type="access",
    )
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM), expires_seconds


def create_refresh_token(username: str, role: str) -> tuple[str, int]:
    expires_seconds = REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60
    payload = _token_payload(
        username=username,
        role=role,
        expires_delta=timedelta(seconds=expires_seconds),
        token_type="refresh",
    )
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM), expires_seconds


def decode_token(token: str) -> dict[str, str | int]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    return payload


def _seconds_until_exp(payload: dict[str, str | int]) -> int:
    exp = payload.get("exp")
    if not isinstance(exp, int):
        return 1
    return max(1, exp - int(_utc_now().timestamp()))


async def authenticate_user(username: str, plain_password: str) -> dict[str, str] | None:
    user = await get_user(username)
    if user is None or not user.get("is_active"):
        return None
    if not verify_password(plain_password, user["password_hash"]):
        return None
    return {
        "username": user["username"],
        "role": user["role"],
    }


async def revoke_token(token: str) -> None:
    payload = decode_token(token)
    jti = payload.get("jti")
    if not isinstance(jti, str) or not jti:
        return
    await revoke_token_jti(jti, _seconds_until_exp(payload))


async def _validate_token_and_user(token: str, expected_type: str) -> dict[str, str]:
    payload = decode_token(token)
    token_type = payload.get("typ")
    username = payload.get("sub")
    jti = payload.get("jti")

    if token_type != expected_type or not isinstance(username, str):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if isinstance(jti, str) and jti and await is_token_revoked(jti):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has been revoked",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = await get_user(username)
    if user is None or not user.get("is_active"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
            headers={"WWW-Authenticate": "Bearer"},
        )
    role = str(user.get("role", Role.USER.value))
    return {"username": username, "role": role}


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict[str, str]:
    return await _validate_token_and_user(token, expected_type="access")


async def get_user_from_refresh_token(refresh_token: str) -> dict[str, str]:
    return await _validate_token_and_user(refresh_token, expected_type="refresh")


def require_role(required_role: Role):
    async def _checker(current_user: dict[str, str] = Depends(get_current_user)) -> dict[str, str]:
        role = current_user.get("role", Role.USER.value)
        if role != required_role.value:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role")
        return current_user

    return _checker
