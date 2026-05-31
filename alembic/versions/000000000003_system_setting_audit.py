"""system_setting_audit table.

Adds the append-only audit-trail table that records every successful
``PUT /api/v1/settings/{key}`` write. The existing
``system_setting.updated_by`` column carries only the last writer; this
table preserves the full history (who, when, from what value, to what
value, why).

Revision ID: 000000000003
Revises: 000000000002
Create Date: 2026-05-15
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000003"
down_revision: Union[str, None] = "000000000002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    schema = DialectHelper(settings.db_type).schema("identity")
    if not sa.inspect(op.get_bind()).has_table("system_setting_audit", schema=schema):
        op.create_table(
            "system_setting_audit",
            sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
            sa.Column("key", sa.Text, nullable=False),
            sa.Column("old_value", sa.Text, nullable=True),
            sa.Column("new_value", sa.Text, nullable=False),
            sa.Column(
                "changed_at",
                postgresql.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column("changed_by", sa.Text, nullable=False),
            sa.Column("reason", sa.Text, nullable=True),
            schema=schema,
        )
    if "idx_system_setting_audit_key" not in {
        ix["name"]
        for ix in sa.inspect(op.get_bind()).get_indexes(
            "system_setting_audit", schema=schema
        )
    }:
        op.create_index(
            "idx_system_setting_audit_key",
            "system_setting_audit",
            [sa.text("key"), sa.text("changed_at DESC")],
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
