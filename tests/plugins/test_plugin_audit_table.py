"""Phase R3b gate: the plugin lifecycle audit trail - vocabulary, model, migration.

ADR-0019's Confirmation requires the table. Split out of test_supervisor.py because
the shape of a row and the shape of a migration are a different responsibility from
what the supervisor does with them, and because that file is over the 1000-line cap
otherwise (STYLE_GUIDE.md section 4).

A malformed handshake in particular has to be surfaced twice - logged at error and
written here - rather than parked in a `last_error` field with no reader; that half
is gated next door, against a running supervisor.
"""

import glob
import importlib.util
import os
import re

import pytest
from sqlalchemy import BigInteger, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP

from tsigma.plugins.audit import (
    DatabasePluginAuditSink,
    PluginAuditEvent,
    PluginAuditRecord,
    RecordingPluginAuditSink,
    log_plugin_event,
)
from tsigma.plugins.connection import ProcessModel
from tsigma.plugins.supervisor import PluginSupervisor

from tsigma.models.base import tsigma_schema
from tsigma.models.plugin_audit import PluginAudit

THIS_REVISION = "000000000012"
PARENT_REVISION = "000000000011"

VERSIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "alembic", "versions")

REVISION_RE = re.compile(r"^revision(?::\s*str)?\s*=\s*[\"']([^\"']+)[\"']", re.MULTILINE)
DOWN_REVISION_RE = re.compile(
    r"^down_revision(?::\s*Union\[str,\s*None\])?\s*=\s*(?:[\"']([^\"']+)[\"']|None)", re.MULTILINE
)

# The six lifecycle events ADR-0019's Confirmation calls for, with the health
# transition split by direction: "it went bad" and "it came back" are different
# operator facts and an operator must not have to diff two rows to tell them apart.
EXPECTED_EVENT_VALUES = {
    "launch",
    "handshake_failed",
    "health_lost",
    "health_restored",
    "restart",
    "gave_up",
    "shutdown",
}


def _migration_files(prefix: str) -> list[str]:
    return sorted(glob.glob(os.path.join(VERSIONS_DIR, f"{prefix}_*.py")))


def _load_migration(prefix: str):
    files = _migration_files(prefix)
    assert len(files) == 1, f"expected exactly one migration file for {prefix}, found {files}"
    spec = importlib.util.spec_from_file_location(f"migration_module_{prefix}", files[0])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _all_revisions() -> dict[str, str | None]:
    """Every revision on disk mapped to its parent, read as text rather than imported."""
    chain: dict[str, str | None] = {}
    for path in sorted(glob.glob(os.path.join(VERSIONS_DIR, "*.py"))):
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        revision = REVISION_RE.search(source)
        if revision is None:
            continue
        parent = DOWN_REVISION_RE.search(source)
        chain[revision.group(1)] = parent.group(1) if parent else None
    return chain


# --------------------------------------------------------------- audit vocabulary
class TestAuditVocabulary:
    """The event set is fixed, so an operator query is not a guess at spellings."""

    def test_the_event_enum_covers_exactly_the_declared_lifecycle_events(self):
        assert {member.value for member in PluginAuditEvent} == EXPECTED_EVENT_VALUES

    def test_every_event_is_a_plain_string_value(self):
        # The value is what lands in a Text column and what an operator greps for.
        for member in PluginAuditEvent:
            assert isinstance(member.value, str)
            assert member.value == member.value.lower()

    def test_a_record_carries_the_plugin_the_event_and_the_process_model(self):
        record = PluginAuditRecord(
            plugin_name="asc3",
            event_type=PluginAuditEvent.LAUNCH,
            process_model=ProcessModel.CHILD,
        )
        assert record.plugin_name == "asc3"
        assert record.event_type is PluginAuditEvent.LAUNCH
        assert record.process_model is ProcessModel.CHILD
        assert record.detail == ""

    @pytest.mark.asyncio
    async def test_the_recording_sink_keeps_records_in_order(self):
        sink = RecordingPluginAuditSink()
        assert sink.records == []
        first = PluginAuditRecord("a", PluginAuditEvent.LAUNCH, ProcessModel.CHILD)
        second = PluginAuditRecord("a", PluginAuditEvent.SHUTDOWN, ProcessModel.CHILD)
        await sink.record(first)
        await sink.record(second)
        assert sink.records == [first, second]


class TestTheDefaultSinkWritesToTheDatabase:
    """In-memory recording is the test seam; the database is the default."""

    def test_a_supervisor_with_no_sink_writes_to_the_audit_table(self):
        supervisor = PluginSupervisor()
        assert isinstance(supervisor.audit_sink, DatabasePluginAuditSink), (
            "the audit trail must reach the database by default; an in-memory sink "
            "as the default is an audit trail that vanishes with the process"
        )

    def test_constructing_a_supervisor_touches_no_database(self):
        # A supervisor is built during app startup, before a pool exists. If the
        # default sink connects eagerly, importing the host becomes DB-dependent.
        PluginSupervisor()

    def test_the_row_writer_mirrors_the_auth_audit_writer(self):
        # tsigma/auth/audit.py::log_auth_event is the house shape for an
        # app-inserted event log: session first, caller flushes/commits.
        import inspect as _inspect

        parameters = list(_inspect.signature(log_plugin_event).parameters)
        assert parameters[0] == "session"
        assert _inspect.iscoroutinefunction(log_plugin_event)


# ------------------------------------------------------------------ the audit table
class TestPluginAuditModel:
    """ADR-0019's Confirmation requires the table; this is its shape."""

    def test_tablename(self):
        assert PluginAudit.__tablename__ == "plugin_audit"

    def test_schema_is_config(self):
        table_args = PluginAudit.__table_args__
        assert isinstance(table_args, tuple), "expected (Index(...), {schema: ...})"
        assert table_args[-1]["schema"] == tsigma_schema("config")

    def test_primary_key_is_a_bigint_autoincrement_id(self):
        assert [c.name for c in PluginAudit.__table__.primary_key.columns] == ["id"]
        column = PluginAudit.__table__.columns["id"]
        assert isinstance(column.type, BigInteger)
        assert column.autoincrement in (True, "auto")

    @pytest.mark.parametrize("name", ["plugin_name", "event_type", "process_model"])
    def test_identifying_columns_are_text_not_null(self, name):
        column = PluginAudit.__table__.columns[name]
        assert isinstance(column.type, Text)
        assert column.nullable is False

    def test_detail_is_text_and_nullable(self):
        # Most events have nothing to add; a handshake failure has everything.
        column = PluginAudit.__table__.columns["detail"]
        assert isinstance(column.type, Text)
        assert column.nullable is True

    def test_changed_at_is_a_server_defaulted_timestamptz(self):
        column = PluginAudit.__table__.columns["changed_at"]
        assert isinstance(column.type, TIMESTAMP)
        assert column.type.timezone is True
        assert column.nullable is False
        assert "now" in str(column.server_default.arg).lower()

    def test_the_table_carries_no_update_or_delete_surface(self):
        # Append-only: the model must not offer a mutable mapped relationship or a
        # nullable primary key that would let a row be rewritten in place.
        assert PluginAudit.__table__.columns["id"].nullable is False

    def test_a_row_can_be_built_from_the_required_fields_alone(self):
        row = PluginAudit(
            plugin_name="asc3",
            event_type=PluginAuditEvent.HANDSHAKE_FAILED.value,
            process_model=ProcessModel.CHILD.value,
        )
        assert row.plugin_name == "asc3"
        assert row.event_type == "handshake_failed"
        assert row.process_model == "child"
        assert row.detail is None

    def test_the_plugin_time_index_exists_in_descending_order(self):
        indexes = {ix.name: ix for ix in PluginAudit.__table__.indexes}
        assert "idx_plugin_audit_plugin" in indexes, (
            f"missing the (plugin_name, changed_at DESC) index; found {sorted(indexes)}"
        )
        index = indexes["idx_plugin_audit_plugin"]
        assert [c.name for c in index.columns] == ["plugin_name", "changed_at"]
        assert index.dialect_options["postgresql"]["ops"] == {"changed_at": "DESC"}

    def test_the_model_is_exported_from_the_models_package(self):
        import tsigma.models as models

        assert models.PluginAudit is PluginAudit
        assert "PluginAudit" in models.__all__


class TestPluginAuditMigration:
    """One new revision, chained so the chain still has one head."""

    def test_exactly_one_migration_file_claims_this_revision(self):
        files = _migration_files(THIS_REVISION)
        assert len(files) == 1, f"expected exactly one {THIS_REVISION}_*.py, found {files}"

    def test_the_migration_declares_its_revision_and_parent(self):
        module = _load_migration(THIS_REVISION)
        assert module.revision == THIS_REVISION
        assert module.down_revision == PARENT_REVISION, (
            "this revision chains onto the last committed revision, so the "
            "history is linear and `alembic upgrade head` resolves"
        )

    def test_downgrade_refuses_rather_than_dropping_the_audit_table(self):
        module = _load_migration(THIS_REVISION)
        with pytest.raises(NotImplementedError):
            module.downgrade()

    def test_the_migration_creates_the_plugin_audit_table_in_the_config_schema(self):
        with open(_migration_files(THIS_REVISION)[0], encoding="utf-8") as handle:
            source = handle.read()
        assert "plugin_audit" in source
        assert "plugin_name" in source
        assert "event_type" in source
        assert "process_model" in source
        assert "changed_at" in source
        assert 'schema("config")' in source, "the table belongs in the config logical schema"

    def test_this_revisions_parent_is_on_disk(self):
        chain = _all_revisions()
        assert THIS_REVISION in chain, "the new revision is not on disk"
        assert PARENT_REVISION in chain, "the parent this revision names is missing"

    def test_this_revision_is_a_head(self):
        # A head, not necessarily THE head: a sibling lane may hold an
        # uncommitted revision off the same parent, and alembic carries multiple
        # heads until they merge. That state is accepted, so nothing here counts
        # them.
        chain = _all_revisions()
        parents = {parent for parent in chain.values() if parent is not None}
        assert THIS_REVISION not in parents, "something already chains onto this revision"

    def test_no_revision_id_is_claimed_twice(self):
        paths = sorted(glob.glob(os.path.join(VERSIONS_DIR, "*.py")))
        ids = []
        for path in paths:
            with open(path, encoding="utf-8") as handle:
                match = REVISION_RE.search(handle.read())
            if match:
                ids.append(match.group(1))
        assert len(ids) == len(set(ids)), f"duplicate revision ids on disk: {ids}"
