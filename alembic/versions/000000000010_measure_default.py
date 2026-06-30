"""config.measure_default table for admin-configurable report parameter defaults.

Adds the config-schema ``measure_default`` table: a composite-keyed
``(report_name, param_name)`` lookup whose JSONB ``value`` holds the default
value for that report's named parameter.

Revision ID: 000000000010
Revises: 000000000009
Create Date: 2026-06-29
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000010"
down_revision: Union[str, None] = "000000000009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    schema = DialectHelper(settings.db_type).schema("config")
    inspector = sa.inspect(op.get_bind())

    if not inspector.has_table("measure_default", schema=schema):
        op.create_table(
            "measure_default",
            sa.Column("report_name", sa.Text, primary_key=True),
            sa.Column("param_name", sa.Text, primary_key=True),
            sa.Column("value", postgresql.JSONB, nullable=False),
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
