"""config.plugin_audit table.

Adds the append-only lifecycle audit trail the plugin supervisor writes:
one row per launch, handshake failure, health transition, restart, give-up
and shutdown. ADR-0019's Confirmation requires it, so a plugin's history
outlives the log file it was also written to.

Revision ID: 000000000012
Revises: 000000000011
Create Date: 2026-08-31
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000012"
down_revision: Union[str, None] = "000000000010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    helper = DialectHelper(settings.db_type)
    schema = helper.schema("config")
    inspector = sa.inspect(op.get_bind())

    if not inspector.has_table("plugin_audit", schema=schema):
        op.create_table(
            "plugin_audit",
            sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
            sa.Column("plugin_name", sa.Text, nullable=False),
            sa.Column("event_type", sa.Text, nullable=False),
            sa.Column("process_model", sa.Text, nullable=False),
            sa.Column("detail", sa.Text, nullable=True),
            sa.Column(
                "changed_at",
                postgresql.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            schema=schema,
        )

    if "idx_plugin_audit_plugin" not in {
        ix["name"]
        for ix in sa.inspect(op.get_bind()).get_indexes("plugin_audit", schema=schema)
    }:
        op.create_index(
            "idx_plugin_audit_plugin",
            "plugin_audit",
            [sa.text("plugin_name"), sa.text("changed_at DESC")],
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
