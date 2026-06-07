"""controller_clock_offset table.

Adds the raw per-ingest controller clock-offset trending table (ASC/3
integrity program, Inc-4): one row per ingest capturing the signed
controller clock offset (newest event timestamp minus server receive
time) for trending over time, keyed by signal_id with an observed_at
timestamp and the backing event_count.

Revision ID: 000000000006
Revises: 000000000005
Create Date: 2026-06-07
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000006"
down_revision: Union[str, None] = "000000000005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    schema = DialectHelper(settings.db_type).schema("config")
    if not sa.inspect(op.get_bind()).has_table(
        "controller_clock_offset", schema=schema
    ):
        op.create_table(
            "controller_clock_offset",
            sa.Column(
                "offset_id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column(
                "signal_id",
                sa.Text,
                sa.ForeignKey("signal.signal_id"),
                nullable=False,
            ),
            sa.Column("offset_seconds", sa.Float, nullable=False),
            sa.Column("event_count", sa.Integer, nullable=True),
            sa.Column(
                "observed_at",
                postgresql.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            schema=schema,
        )
    if "idx_clock_offset_signal_observed" not in {
        ix["name"]
        for ix in sa.inspect(op.get_bind()).get_indexes(
            "controller_clock_offset", schema=schema
        )
    }:
        op.create_index(
            "idx_clock_offset_signal_observed",
            "controller_clock_offset",
            ["signal_id", "observed_at"],
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
