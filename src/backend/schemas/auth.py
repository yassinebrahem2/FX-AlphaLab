"""Auth request/response schemas."""

from __future__ import annotations

import re
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field, field_validator

_PASSWORD_PATTERN = re.compile(r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^\w\s]).+$")


class SignupRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=12)
    full_name: str | None = None
    role: str | None = None

    @field_validator("password")
    @classmethod
    def validate_password(cls, value: str) -> str:
        if not _PASSWORD_PATTERN.match(value):
            raise ValueError("Password must include upper, lower, number, and symbol")
        return value


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class UserResponse(BaseModel):
    id: int
    email: EmailStr
    full_name: str | None = None
    role: str
    is_active: bool
    created_at: datetime
    last_login_at: datetime | None = None

    model_config = {"from_attributes": True}


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in_seconds: int
    user: UserResponse
