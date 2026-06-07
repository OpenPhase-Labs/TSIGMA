"""file_ingest_provenance table.

Adds the append-only history table that records the header metadata of
every ingested file (ASC/3 integrity program, Inc-2): source filename,
the controller's reported network identity, log version, phases in use,
the log begin time and header anchor, and the decoder that produced the
row.

Revision ID: 000000000004
Revises: 000000000003
Create Date: 2026-06-07
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000004"
down_revision: Union[str, None] = "000000000003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    schema = DialectHelper(settings.db_type).schema("config")
    if not sa.inspect(op.get_bind()).has_table(
        "file_ingest_provenance", schema=schema
    ):
        op.create_table(
            "file_ingest_provenance",
            sa.Column(
                "provenance_id",
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
            sa.Column("source_filename", sa.Text, nullable=True),
            sa.Column("device_ip", sa.Text, nullable=True),
            sa.Column("device_mac", sa.Text, nullable=True),
            sa.Column("log_version", sa.Text, nullable=True),
            sa.Column("phases_in_use", postgresql.JSONB, nullable=True),
            sa.Column(
                "log_begin",
                postgresql.TIMESTAMP(timezone=True),
                nullable=True,
            ),
            sa.Column(
                "header_anchor",
                postgresql.TIMESTAMP(timezone=True),
                nullable=True,
            ),
            sa.Column("decoder_name", sa.Text, nullable=True),
            sa.Column(
                "ingested_at",
                postgresql.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            schema=schema,
        )
    if "idx_file_provenance_signal" not in {
        ix["name"]
        for ix in sa.inspect(op.get_bind()).get_indexes(
            "file_ingest_provenance", schema=schema
        )
    }:
        op.create_index(
            "idx_file_provenance_signal",
            "file_ingest_provenance",
            ["signal_id"],
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
