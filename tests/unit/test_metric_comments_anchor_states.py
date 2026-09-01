import datetime

import pytest
import sqlalchemy
from pydantic import ValidationError

from tsigma.api.v1.metric_comments import MetricCommentCreate
from tsigma.models.base import Base


class TestMetricCommentAnchorStateSpace:
    def test_db_constraint_forbids_end_without_start(self):
        constraints = Base.metadata.tables['config.metric_comment'].constraints
        check_constraints = [c for c in constraints if isinstance(c, sqlalchemy.CheckConstraint)]
        found = False
        for c in check_constraints:
            sqltext = str(c.sqltext)
            if 'anchor_end' in sqltext and 'anchor_start' in sqltext:
                if 'anchor_end IS NOT NULL' in sqltext and 'anchor_start IS NULL' in sqltext:
                    found = True
                    break
        assert found, "Expected a CheckConstraint forbidding anchor_end without anchor_start"

    def test_db_constraint_forbids_inverted_range(self):
        constraints = Base.metadata.tables['config.metric_comment'].constraints
        check_constraints = [c for c in constraints if isinstance(c, sqlalchemy.CheckConstraint)]
        found = False
        for c in check_constraints:
            sqltext = str(c.sqltext)
            if 'anchor_end' in sqltext and 'anchor_start' in sqltext:
                if 'anchor_end' in sqltext and 'anchor_start' in sqltext and '<' in sqltext:
                    found = True
                    break
        assert found, "Expected a CheckConstraint forbidding anchor_end earlier than anchor_start"

    def test_api_rejects_anchor_end_without_anchor_start(self):
        dt = datetime.datetime(2026, 6, 1, 12, 0, tzinfo=datetime.timezone.utc)
        with pytest.raises(ValidationError):
            MetricCommentCreate(
                signal_id="sig1",
                text="test",
                anchor_end=dt,
            )

    def test_api_rejects_inverted_range(self):
        dt_start = datetime.datetime(2026, 6, 2, 12, 0, tzinfo=datetime.timezone.utc)
        dt_end = datetime.datetime(2026, 6, 1, 12, 0, tzinfo=datetime.timezone.utc)
        with pytest.raises(ValidationError):
            MetricCommentCreate(
                signal_id="sig1",
                text="test",
                anchor_start=dt_start,
                anchor_end=dt_end,
            )

    def test_api_accepts_unanchored(self):
        data = MetricCommentCreate(
            signal_id="sig1",
            text="test",
        )
        assert data.anchor_start is None
        assert data.anchor_end is None

    def test_api_accepts_point_anchor(self):
        dt = datetime.datetime(2026, 6, 1, 12, 0, tzinfo=datetime.timezone.utc)
        data = MetricCommentCreate(
            signal_id="sig1",
            text="test",
            anchor_start=dt,
        )
        assert data.anchor_start == dt
        assert data.anchor_end is None

    def test_api_accepts_range_anchor(self):
        dt_start = datetime.datetime(2026, 6, 1, 12, 0, tzinfo=datetime.timezone.utc)
        dt_end = datetime.datetime(2026, 6, 2, 12, 0, tzinfo=datetime.timezone.utc)
        data = MetricCommentCreate(
            signal_id="sig1",
            text="test",
            anchor_start=dt_start,
            anchor_end=dt_end,
        )
        assert data.anchor_start == dt_start
        assert data.anchor_end == dt_end
