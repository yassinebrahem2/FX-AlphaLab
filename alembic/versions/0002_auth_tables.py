"""auth_tables

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-05

Adds users and refresh_tokens tables.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("full_name", sa.String(120)),
        sa.Column("role", sa.String(50), nullable=False, server_default=sa.text("'trader'")),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("last_login_at", sa.TIMESTAMP),
        sa.UniqueConstraint("email", name="uq_users_email"),
    )
    op.create_index("idx_users_email", "users", ["email"])

    op.create_table(
        "refresh_tokens",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("user_id", sa.Integer, nullable=False),
        sa.Column("token_hash", sa.String(255), nullable=False),
        sa.Column("jti", sa.String(36), nullable=False),
        sa.Column("issued_at", sa.TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", sa.TIMESTAMP, nullable=False),
        sa.Column("revoked_at", sa.TIMESTAMP),
        sa.Column("last_used_at", sa.TIMESTAMP),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("token_hash", name="uq_refresh_tokens_hash"),
        sa.UniqueConstraint("jti", name="uq_refresh_tokens_jti"),
    )
    op.create_index("idx_refresh_tokens_user_id", "refresh_tokens", ["user_id"])
    op.create_index("idx_refresh_tokens_hash", "refresh_tokens", ["token_hash"])


def downgrade() -> None:
    op.drop_index("idx_refresh_tokens_hash", table_name="refresh_tokens")
    op.drop_index("idx_refresh_tokens_user_id", table_name="refresh_tokens")
    op.drop_table("refresh_tokens")
    op.drop_index("idx_users_email", table_name="users")
    op.drop_table("users")
