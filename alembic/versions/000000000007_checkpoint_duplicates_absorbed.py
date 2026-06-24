"""polling_checkpoint.duplicates_absorbed column.

Adds the ``duplicates_absorbed`` additive counter to ``polling_checkpoint``
(events schema). It records, per device per method, the number of duplicate
events absorbed by ``ON CONFLICT DO NOTHING`` across poll cycles (attempted
minus actually inserted), the complement of ``events_ingested``.

Revision ID: 000000000007
Revises: 000000000006
Create Date: 2026-06-22
"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000007"
down_revision: Union[str, None] = "000000000006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    schema = DialectHelper(settings.db_type).schema("events")
    existing = {
        col["name"]
        for col in sa.inspect(op.get_bind()).get_columns(
            "polling_checkpoint", schema=schema
        )
    }
    if "duplicates_absorbed" not in existing:
        op.add_column(
            "polling_checkpoint",
            sa.Column(
                "duplicates_absorbed",
                sa.BigInteger,
                nullable=False,
                server_default="0",
            ),
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
