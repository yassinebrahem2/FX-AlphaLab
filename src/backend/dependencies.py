"""FastAPI dependencies shared across routers."""

from __future__ import annotations

from collections.abc import Generator

from sqlalchemy.orm import Session

from src.shared.db.session import get_db as _get_db


def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session, committing on success and rolling back on error."""
    with _get_db() as session:
        yield session
