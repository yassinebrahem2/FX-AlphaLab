"""Auth endpoints for login and token management."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from jose import JWTError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.backend.dependencies import get_db
from src.backend.schemas.auth import (
    LoginRequest,
    RefreshRequest,
    SignupRequest,
    TokenResponse,
    UserResponse,
)
from src.backend.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
    hash_password,
    hash_token,
    verify_password,
)
from src.shared.db.models import RefreshToken, UserAccount

router = APIRouter(prefix="/auth", tags=["auth"])


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _issue_tokens(db: Session, user: UserAccount) -> TokenResponse:
    access_token, access_expires = create_access_token(user)
    refresh_token, jti, refresh_expires_at = create_refresh_token(user)

    db.add(
        RefreshToken(
            user_id=user.id,
            token_hash=hash_token(refresh_token),
            jti=jti,
            expires_at=refresh_expires_at,
        )
    )

    # Commit here so writes are visible to subsequent requests immediately.
    # FastAPI 0.136+ runs dependency cleanup (and thus the session's auto-commit)
    # AFTER the HTTP response is already sent, creating a race condition for
    # back-to-back requests. Explicit commit before building the response fixes this.
    db.commit()

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in_seconds=access_expires,
        user=UserResponse.model_validate(user),
    )


@router.post("/signup", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
def signup(payload: SignupRequest, db: Session = Depends(get_db)) -> TokenResponse:
    email = _normalize_email(payload.email)
    existing = db.execute(
        select(UserAccount).where(UserAccount.email == email)
    ).scalar_one_or_none()
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already registered")

    user = UserAccount(
        email=email,
        full_name=payload.full_name,
        role=payload.role or "trader",
        password_hash=hash_password(payload.password),
        is_active=True,
    )
    db.add(user)
    try:
        db.flush()
    except IntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="Email already registered"
        ) from exc

    return _issue_tokens(db, user)


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    email = _normalize_email(payload.email)
    user = db.execute(select(UserAccount).where(UserAccount.email == email)).scalar_one_or_none()
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User disabled")

    user.last_login_at = datetime.utcnow()
    return _issue_tokens(db, user)


@router.post("/refresh", response_model=TokenResponse)
def refresh(payload: RefreshRequest, db: Session = Depends(get_db)) -> TokenResponse:
    try:
        token_payload = decode_token(payload.refresh_token)
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        ) from exc

    if token_payload.get("type") != "refresh":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")

    token_hash = hash_token(payload.refresh_token)
    record = db.execute(
        select(RefreshToken).where(RefreshToken.token_hash == token_hash)
    ).scalar_one_or_none()

    if record is None or record.revoked_at is not None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token revoked")

    now = datetime.utcnow()
    if record.expires_at <= now:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")

    user = db.get(UserAccount, record.user_id)
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not authorized")

    record.revoked_at = now
    record.last_used_at = now

    return _issue_tokens(db, user)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(payload: RefreshRequest, db: Session = Depends(get_db)) -> None:
    token_hash = hash_token(payload.refresh_token)
    record = db.execute(
        select(RefreshToken).where(RefreshToken.token_hash == token_hash)
    ).scalar_one_or_none()

    if record is not None and record.revoked_at is None:
        record.revoked_at = datetime.utcnow()
        db.commit()


@router.get("/me", response_model=UserResponse)
def me(current_user: UserAccount = Depends(get_current_user)) -> UserResponse:
    return UserResponse.model_validate(current_user)
