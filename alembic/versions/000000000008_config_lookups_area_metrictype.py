"""config lookup tables: area, signal_area, metric_type.

Adds three config-schema lookup/association tables:
  - ``area``: flat named operational grouping for signals.
  - ``signal_area``: many-to-many association of signals to areas, composite
    primary key ``(signal_id, area_id)``, cascading on either side.
  - ``metric_type``: catalogue of selectable metrics with display ordering.

Also adds the ``other_partners`` free-text column to ``jurisdiction``.

Revision ID: 000000000008
Revises: 000000000007
Create Date: 2026-06-29
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000008"
down_revision: Union[str, None] = "000000000007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    schema = DialectHelper(settings.db_type).schema("config")
    inspector = sa.inspect(op.get_bind())

    if not inspector.has_table("area", schema=schema):
        op.create_table(
            "area",
            sa.Column(
                "area_id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column("name", sa.Text, nullable=False),
            sa.Column("description", sa.Text, nullable=True),
            schema=schema,
        )

    if not inspector.has_table("signal_area", schema=schema):
        op.create_table(
            "signal_area",
            sa.Column(
                "signal_id",
                sa.Text,
                sa.ForeignKey("signal.signal_id", ondelete="CASCADE"),
                primary_key=True,
            ),
            sa.Column(
                "area_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("area.area_id", ondelete="CASCADE"),
                primary_key=True,
            ),
            schema=schema,
        )

    if not inspector.has_table("metric_type", schema=schema):
        op.create_table(
            "metric_type",
            sa.Column("key", sa.Text, primary_key=True),
            sa.Column("label", sa.Text, nullable=False),
            sa.Column("display_order", sa.Integer, nullable=False),
            schema=schema,
        )

    existing = {
        col["name"]
        for col in inspector.get_columns("jurisdiction", schema=schema)
    }
    if "other_partners" not in existing:
        op.add_column(
            "jurisdiction",
            sa.Column("other_partners", sa.Text(), nullable=True),
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
