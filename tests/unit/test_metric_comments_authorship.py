"""
Authorship shape for metric comments (plan Decision 5).

Authorship is a denormalised snapshot -- ``author_uuid`` + ``author_username``
with NO foreign key to ``auth_user`` -- so a comment outlives its author and
user-lifecycle policy cannot cascade into annotations.
"""

import uuid

from tsigma.models.base import Base
from tsigma.models.metric_comment import MetricComment


class TestMetricCommentAuthorshipSnapshot:
    """Tests for MetricComment authorship denormalization."""

    def test_table_has_required_columns(self):
        table = Base.metadata.tables["config.metric_comment"]
        column_names = {c.name for c in table.columns}
        expected = {
            "id",
            "signal_id",
            "text",
            "author_uuid",
            "author_username",
            "anchor_start",
            "anchor_end",
            "created_at",
            "updated_at",
        }
        assert expected.issubset(column_names)

    def test_author_uuid_is_not_nullable(self):
        table = Base.metadata.tables["config.metric_comment"]
        col = table.columns["author_uuid"]
        assert col.nullable is False

    def test_author_username_is_not_nullable(self):
        table = Base.metadata.tables["config.metric_comment"]
        col = table.columns["author_username"]
        assert col.nullable is False

    def test_author_uuid_has_no_foreign_keys(self):
        table = Base.metadata.tables["config.metric_comment"]
        col = table.columns["author_uuid"]
        assert len(col.foreign_keys) == 0

    def test_author_username_has_no_foreign_keys(self):
        table = Base.metadata.tables["config.metric_comment"]
        col = table.columns["author_username"]
        assert len(col.foreign_keys) == 0

    def test_no_foreign_keys_target_identity_schema(self):
        table = Base.metadata.tables["config.metric_comment"]
        for fk in table.foreign_keys:
            assert not str(fk.target_fullname).startswith("identity."), (
                f"Foreign key {fk.target_fullname} targets identity schema, "
                "which violates denormalized authorship policy."
            )

    def test_model_instantiates_with_author_columns(self):
        test_uuid = uuid.uuid4()
        comment = MetricComment(
            signal_id="SIG_001",
            text="test",
            author_uuid=test_uuid,
            author_username="testuser",
        )
        assert comment.author_uuid == test_uuid
        assert comment.author_username == "testuser"
