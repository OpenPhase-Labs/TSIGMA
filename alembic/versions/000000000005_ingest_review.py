"""ingest_review table.

Adds the general needs-review / correction worklist table (ASC/3 integrity
program, Inc-3): a reusable worklist of ingested artifacts flagged for
attention — decode anomalies, integrity discrepancies, correction
candidates — keyed by signal_id + reason + source_filename, with an
optional link to the file audit row (file_ingest_provenance) when one
exists.

Revision ID: 000000000005
Revises: 000000000004
Create Date: 2026-06-07
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000005"
down_revision: Union[str, None] = "000000000004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    schema = DialectHelper(settings.db_type).schema("config")
    if not sa.inspect(op.get_bind()).has_table("ingest_review", schema=schema):
        op.create_table(
            "ingest_review",
            sa.Column(
                "review_id",
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
            sa.Column("reason", sa.Text, nullable=False),
            sa.Column("source_filename", sa.Text, nullable=True),
            sa.Column(
                "provenance_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("file_ingest_provenance.provenance_id"),
                nullable=True,
            ),
            sa.Column("severity", sa.Text, nullable=True),
            sa.Column("summary", sa.Text, nullable=True),
            sa.Column("detail", postgresql.JSONB, nullable=True),
            sa.Column(
                "status",
                sa.Text,
                nullable=False,
                server_default=sa.text("'open'"),
            ),
            sa.Column(
                "detected_at",
                postgresql.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column(
                "resolved_at",
                postgresql.TIMESTAMP(timezone=True),
                nullable=True,
            ),
            sa.Column("resolved_by", sa.Text, nullable=True),
            schema=schema,
        )
    if "idx_ingest_review_signal_status" not in {
        ix["name"]
        for ix in sa.inspect(op.get_bind()).get_indexes(
            "ingest_review", schema=schema
        )
    }:
        op.create_index(
            "idx_ingest_review_signal_status",
            "ingest_review",
            ["signal_id", "status"],
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
