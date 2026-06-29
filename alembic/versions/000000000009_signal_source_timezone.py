"""signal.source_timezone column for per-signal timezone normalization.

Adds the ``source_timezone`` column to the config-schema ``signal`` table:
a nullable Text column holding an IANA timezone name (e.g.
``"America/New_York"``) or NULL to fall back to the deployment default.

Revision ID: 000000000009
Revises: 000000000008
Create Date: 2026-06-29
"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000009"
down_revision: Union[str, None] = "000000000008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    schema = DialectHelper(settings.db_type).schema("config")
    inspector = sa.inspect(op.get_bind())

    existing = {
        col["name"]
        for col in inspector.get_columns("signal", schema=schema)
    }
    if "source_timezone" not in existing:
        op.add_column(
            "signal",
            sa.Column("source_timezone", sa.Text(), nullable=True),
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
