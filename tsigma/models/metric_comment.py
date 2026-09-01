"""
Metric comments: user-authored annotations on a signal's charts.

A ``MetricComment`` attaches free text to a signal, optionally anchored to a
point or a range on the time axis:

  - ``anchor_start`` NULL and ``anchor_end`` NULL -- unanchored note.
  - ``anchor_start`` set, ``anchor_end`` NULL -- point anchor.
  - both set -- range anchor.

``MetricCommentMetricType`` associates a comment with the metric types whose
charts should display it (many-to-many over ``config.metric_type``). A comment
with no association rows is not scoped to any particular metric.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import CheckConstraint, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, schema_fk, tsigma_schema


class MetricComment(TimestampMixin, Base):
    """
    A user-authored comment attached to a signal, optionally time-anchored.

    Authorship is a denormalised snapshot -- ``author_uuid`` plus
    ``author_username`` as written, with NO foreign key to ``auth_user``.
    A comment outlives its author: deleting or deactivating a user never
    removes or rewrites annotations. Matches ``system_setting.updated_by``
    and ``alert_suppression.created_by``, which attribute users the same way.
    """

    __tablename__ = "metric_comment"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    signal_id: Mapped[str] = mapped_column(
        Text,
        schema_fk("config", "signal.signal_id", ondelete="CASCADE"),
        nullable=False,
    )
    text: Mapped[str] = mapped_column(Text, nullable=False)
    author_uuid: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    author_username: Mapped[str] = mapped_column(Text, nullable=False)
    anchor_start: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    anchor_end: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )

    # Decision 1 allows exactly three anchor states: neither bound set
    # (unanchored), anchor_start alone (a point), or both (a range). An
    # end-without-start row has no defined meaning when matching a chart
    # window -- a point anchor is a point, not an open-ended interval -- so
    # it is rejected here rather than left for each reader to interpret.
    __table_args__ = (
        CheckConstraint(
            "NOT (anchor_end IS NOT NULL AND anchor_start IS NULL)",
            name="ck_metric_comment_anchor_end_needs_start",
        ),
        # NULL-safe: a comparison against NULL yields NULL, and a CHECK passes
        # on NULL, so the unanchored and point states are unaffected.
        CheckConstraint(
            "NOT (anchor_end < anchor_start)",
            name="ck_metric_comment_anchor_order",
        ),
        {"schema": tsigma_schema("config")},
    )


class MetricCommentMetricType(Base):
    """
    Association of metric comments to metric types (many-to-many).

    Composite primary key ``(comment_id, metric_type_key)``. Cascades on
    deletion of either the comment or the metric type.
    """

    __tablename__ = "metric_comment_metric_type"

    comment_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        schema_fk("config", "metric_comment.id", ondelete="CASCADE"),
        primary_key=True,
    )
    metric_type_key: Mapped[str] = mapped_column(
        Text,
        schema_fk("config", "metric_type.key", ondelete="CASCADE"),
        primary_key=True,
    )

    __table_args__ = {"schema": tsigma_schema("config")}
