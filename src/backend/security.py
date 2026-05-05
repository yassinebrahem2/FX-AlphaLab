"""Security helpers for authentication."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import bcrypt as _bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy.orm import Session

from src.backend.dependencies import get_db
from src.shared.config import Config
from src.shared.db.models import UserAccount

_bearer_scheme = HTTPBearer(auto_error=False)


def _require_jwt_secret() -> str:
    if not Config.AUTH_JWT_SECRET:
        raise RuntimeError("AUTH_JWT_SECRET is not configured")
    return Config.AUTH_JWT_SECRET


def hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return _bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _encode_token(payload: dict[str, Any], expires_delta: timedelta) -> tuple[str, int]:
    now = datetime.now(timezone.utc)
    exp = now + expires_delta
    payload.update({"iat": int(now.timestamp()), "exp": int(exp.timestamp())})
    token = jwt.encode(payload, _require_jwt_secret(), algorithm=Config.AUTH_JWT_ALGORITHM)
    return token, int(expires_delta.total_seconds())


def create_access_token(user: UserAccount) -> tuple[str, int]:
    payload = {
        "sub": str(user.id),
        "email": user.email,
        "role": user.role,
        "type": "access",
    }
    expires = timedelta(minutes=Config.AUTH_ACCESS_TOKEN_MINUTES)
    return _encode_token(payload, expires)


def create_refresh_token(user: UserAccount) -> tuple[str, str, datetime]:
    jti = str(uuid4())
    payload = {
        "sub": str(user.id),
        "email": user.email,
        "type": "refresh",
        "jti": jti,
    }
    expires = timedelta(days=Config.AUTH_REFRESH_TOKEN_DAYS)
    token, _ = _encode_token(payload, expires)
    expires_at = datetime.utcnow() + expires
    return token, jti, expires_at


def decode_token(token: str) -> dict[str, Any]:
    return jwt.decode(token, _require_jwt_secret(), algorithms=[Config.AUTH_JWT_ALGORITHM])


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
    db: Session = Depends(get_db),
) -> UserAccount:
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")

    try:
        payload = decode_token(credentials.credentials)
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        ) from exc

    if payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")

    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload"
        )

    user = db.get(UserAccount, int(user_id))
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not authorized")

    return user
