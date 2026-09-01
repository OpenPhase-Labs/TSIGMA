"""config.metric_comment and config.metric_comment_metric_type tables.

Adds the config-schema tables backing metric comments:
  - ``metric_comment``: user-authored annotation on a signal, optionally
    anchored to a point (``anchor_start`` only) or a range (both bounds).
  - ``metric_comment_metric_type``: many-to-many association of comments to
    ``metric_type`` rows, composite primary key ``(comment_id,
    metric_type_key)``, cascading on either side.

Authorship is denormalised -- ``author_uuid`` and ``author_username`` are
captured as written, with NO foreign key to ``auth_user``. A comment outlives
its author, and user-lifecycle policy (delete vs deactivate) stays an operator
choice that cannot cascade into annotations.

Revision ID: 000000000011
Revises: 000000000010
Create Date: 2026-08-31
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "000000000011"
down_revision: Union[str, None] = "000000000010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    helper = DialectHelper(settings.db_type)
    schema = helper.schema("config")
    inspector = sa.inspect(op.get_bind())

    if not inspector.has_table("metric_comment", schema=schema):
        op.create_table(
            "metric_comment",
            sa.Column(
                "id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column(
                "signal_id",
                sa.Text,
                sa.ForeignKey("signal.signal_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("text", sa.Text, nullable=False),
            sa.Column("author_uuid", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("author_username", sa.Text, nullable=False),
            sa.Column("anchor_start", postgresql.TIMESTAMP(timezone=True), nullable=True),
            sa.Column("anchor_end", postgresql.TIMESTAMP(timezone=True), nullable=True),
            sa.Column(
                "created_at",
                postgresql.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.Column(
                "updated_at",
                postgresql.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.CheckConstraint(
                "NOT (anchor_end IS NOT NULL AND anchor_start IS NULL)",
                name="ck_metric_comment_anchor_end_needs_start",
            ),
            sa.CheckConstraint(
                "NOT (anchor_end < anchor_start)",
                name="ck_metric_comment_anchor_order",
            ),
            schema=schema,
        )

    if not inspector.has_table("metric_comment_metric_type", schema=schema):
        op.create_table(
            "metric_comment_metric_type",
            sa.Column(
                "comment_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("metric_comment.id", ondelete="CASCADE"),
                primary_key=True,
            ),
            sa.Column(
                "metric_type_key",
                sa.Text,
                sa.ForeignKey("metric_type.key", ondelete="CASCADE"),
                primary_key=True,
            ),
            schema=schema,
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
