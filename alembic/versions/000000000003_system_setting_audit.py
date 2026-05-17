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
    )
    op.create_index(
        "idx_system_setting_audit_key",
        "system_setting_audit",
        [sa.text("key"), sa.text("changed_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("idx_system_setting_audit_key", table_name="system_setting_audit")
    op.drop_table("system_setting_audit")
