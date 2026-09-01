import glob
import importlib.util
import os
import uuid
from datetime import datetime, timezone

import pytest

import tsigma.models
from tsigma.models.base import Base, TimestampMixin
from tsigma.models.metric_comment import MetricComment, MetricCommentMetricType
from tsigma.settings_service import (
    ACCESS_CATEGORIES,
    DEFAULT_ACCESS_POLICY,
    LOCKED_CATEGORIES,
    seed_system_settings,
)


class TestMetricCommentModelInstantiation:
    def test_metric_comment_can_be_instantiated_with_all_fields(self):
        comment_id = uuid.uuid4()
        author_uuid = uuid.uuid4()
        start = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
        end = datetime(2026, 6, 1, 13, 0, tzinfo=timezone.utc)
        mc = MetricComment(
            id=comment_id,
            signal_id="SIG_001",
            text="Detector 3 was offline for maintenance.",
            author_uuid=author_uuid,
            author_username="jsmith",
            anchor_start=start,
            anchor_end=end,
        )
        assert mc.id == comment_id
        assert mc.signal_id == "SIG_001"
        assert mc.text == "Detector 3 was offline for maintenance."
        assert mc.author_uuid == author_uuid
        assert mc.anchor_start == start
        assert mc.anchor_end == end

    def test_metric_comment_defaults_are_none(self):
        mc = MetricComment()
        assert mc.signal_id is None
        assert mc.text is None
        assert mc.author_uuid is None
        assert mc.anchor_start is None
        assert mc.anchor_end is None


class TestMetricCommentAnchorStates:
    """Decision 1 - three valid anchor states: no anchor, point, range."""

    def test_unanchored_comment_has_no_anchor_bounds(self):
        mc = MetricComment(signal_id="SIG_001", text="General note", author_uuid=uuid.uuid4(), author_username="jsmith")
        assert mc.anchor_start is None
        assert mc.anchor_end is None

    def test_point_anchored_comment_sets_only_anchor_start(self):
        start = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
        mc = MetricComment(
            signal_id="SIG_001",
            text="Spike at this instant",
            author_uuid=uuid.uuid4(), author_username="jsmith",
            anchor_start=start,
        )
        assert mc.anchor_start == start
        assert mc.anchor_end is None

    def test_range_anchored_comment_sets_both_bounds(self):
        start = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
        end = datetime(2026, 6, 1, 18, 30, tzinfo=timezone.utc)
        mc = MetricComment(
            signal_id="SIG_001",
            text="Construction window",
            author_uuid=uuid.uuid4(), author_username="jsmith",
            anchor_start=start,
            anchor_end=end,
        )
        assert mc.anchor_start == start
        assert mc.anchor_end == end


class TestMetricCommentTimestampMixinReuse:
    def test_metric_comment_inherits_timestamp_mixin(self):
        assert issubclass(MetricComment, TimestampMixin), (
            "MetricComment must reuse TimestampMixin rather than hand-rolling created_at/updated_at"
        )

    def test_created_at_and_updated_at_come_from_the_mixin(self):
        table = MetricComment.__table__
        for name in ("created_at", "updated_at"):
            assert name in table.columns
            column = table.columns[name]
            assert column.nullable is False
            assert column.server_default is not None
        assert table.columns["updated_at"].onupdate is not None
        assert table.columns["created_at"].onupdate is None


class TestMetricCommentMetricTypeModelInstantiation:
    def test_assoc_can_be_instantiated_with_comment_id_and_metric_type_key(self):
        comment_id = uuid.uuid4()
        link = MetricCommentMetricType(comment_id=comment_id, metric_type_key="avg_speed")
        assert link.comment_id == comment_id
        assert link.metric_type_key == "avg_speed"

    def test_assoc_defaults_are_none(self):
        link = MetricCommentMetricType()
        assert link.comment_id is None
        assert link.metric_type_key is None


class TestModelImports:
    def test_metric_comment_importable_from_tsigma_models_package(self):
        from tsigma.models import MetricComment as MC
        assert MC is not None

    def test_metric_comment_metric_type_importable_from_tsigma_models_package(self):
        from tsigma.models import MetricCommentMetricType as MCMT
        assert MCMT is not None

    def test_models_listed_in_tsigma_models_all(self):
        assert hasattr(tsigma.models, "__all__"), "tsigma.models must define __all__"
        assert "MetricComment" in tsigma.models.__all__
        assert "MetricCommentMetricType" in tsigma.models.__all__


class TestMetadataIntrospection:
    def test_config_metric_comment_table_registered_with_correct_columns_and_pk(self):
        assert "config.metric_comment" in Base.metadata.tables
        table = Base.metadata.tables["config.metric_comment"]
        for name in (
            "id", "signal_id", "text", "author_uuid", "author_username",
            "anchor_start", "anchor_end",
        ):
            assert name in table.columns, f"metric_comment is missing column {name!r}"
        assert list(table.primary_key.columns.keys()) == ["id"]

    def test_anchor_columns_are_nullable_timestamptz(self):
        table = Base.metadata.tables["config.metric_comment"]
        for name in ("anchor_start", "anchor_end"):
            column = table.columns[name]
            assert column.nullable is True, f"{name} must be nullable (Decision 1)"
            assert getattr(column.type, "timezone", False) is True, f"{name} must be timestamptz"

    def test_signal_id_foreign_key_targets_config_signal(self):
        column = Base.metadata.tables["config.metric_comment"].columns["signal_id"]
        targets = {fk.target_fullname for fk in column.foreign_keys}
        assert targets == {"config.signal.signal_id"}

    def test_config_metric_comment_metric_type_table_registered_with_composite_pk(self):
        assert "config.metric_comment_metric_type" in Base.metadata.tables
        table = Base.metadata.tables["config.metric_comment_metric_type"]
        assert "comment_id" in table.columns
        assert "metric_type_key" in table.columns
        assert set(table.primary_key.columns.keys()) == {"comment_id", "metric_type_key"}

    def test_assoc_foreign_keys_target_parents_and_cascade(self):
        table = Base.metadata.tables["config.metric_comment_metric_type"]
        comment_fks = list(table.columns["comment_id"].foreign_keys)
        metric_type_fks = list(table.columns["metric_type_key"].foreign_keys)
        assert {fk.target_fullname for fk in comment_fks} == {"config.metric_comment.id"}
        assert {fk.target_fullname for fk in metric_type_fks} == {"config.metric_type.key"}
        for fk in comment_fks + metric_type_fks:
            assert fk.ondelete == "CASCADE"


class TestAlembicMigration:
    @staticmethod
    def _migration_files():
        versions_dir = os.path.join(os.path.dirname(__file__), "..", "..", "alembic", "versions")
        return glob.glob(os.path.join(versions_dir, "000000000011_*.py"))

    @classmethod
    def _load_migration(cls):
        files = cls._migration_files()
        assert len(files) == 1
        spec = importlib.util.spec_from_file_location("migration_module_000000000011", files[0])
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_migration_file_exists_with_correct_prefix(self):
        files = self._migration_files()
        assert len(files) == 1, f"Expected exactly one migration file starting with 000000000011_, found {files}"

    def test_migration_exposes_correct_revision_and_down_revision(self):
        module = self._load_migration()
        assert module.revision == "000000000011"
        assert module.down_revision == "000000000010"

    def test_downgrade_raises_not_implemented_error(self):
        module = self._load_migration()
        with pytest.raises(NotImplementedError):
            module.downgrade()

    def test_migration_creates_both_tables(self):
        with open(self._migration_files()[0], encoding="utf-8") as handle:
            source = handle.read()
        assert "metric_comment" in source
        assert "metric_comment_metric_type" in source

    def test_migration_creates_author_columns_without_an_auth_user_fk(self):
        with open(self._migration_files()[0], encoding="utf-8") as handle:
            source = handle.read()
        assert "author_uuid" in source
        assert "author_username" in source
        assert "auth_user" not in source.split('"""', 2)[-1], (
            "the migration must not create a foreign key to auth_user"
        )


class TestCommentsAccessPolicy:
    def test_comments_is_a_declared_access_category(self):
        assert "comments" in ACCESS_CATEGORIES

    def test_comments_is_not_locked(self):
        assert "comments" not in LOCKED_CATEGORIES

    def test_default_access_policy_seeds_comments_as_authenticated_and_editable(self):
        rows = [row for row in DEFAULT_ACCESS_POLICY if row["key"] == "access_policy.comments"]
        assert len(rows) == 1, "DEFAULT_ACCESS_POLICY must seed exactly one access_policy.comments row"
        row = rows[0]
        assert row["value"] == "authenticated"
        assert row["category"] == "access_policy"
        assert row["editable"] is True
        assert row["description"]

    def test_comments_policy_row_is_appended_last(self):
        assert DEFAULT_ACCESS_POLICY[-1]["key"] == "access_policy.comments"

    def test_seed_docstring_reports_seven_access_policy_rows(self):
        docstring = seed_system_settings.__doc__ or ""
        assert "7 ``access_policy.*`` rows" in docstring
        assert "6 ``access_policy.*`` rows" not in docstring
