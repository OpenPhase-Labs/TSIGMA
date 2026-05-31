"""Initial schema — all tables.

Squashed from prior dev migrations (c0f911213673 / a1b2c3d4e5f6 / b3c4d5e6f7a8)
into a single starting point now that there are no production deployments.

Revision ID: 000000000001
Revises:
Create Date: 2026-04-23
"""
import logging
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op
from tsigma.database.migration_aggregates_helpers import (
    create_second_batch_aggregate_tables as _create_second_batch_aggregate_tables,
)
from tsigma.database.migration_aggregates_helpers import (
    second_batch_continuous_aggregates as _second_batch_continuous_aggregates,
)

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = "000000000001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # Schema creation (PostgreSQL, MS-SQL, Oracle — MySQL uses default DB)
    # ------------------------------------------------------------------
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    dialect = DialectHelper(settings.db_type)
    for stmt in dialect.create_schemas_sql():
        op.execute(sa.text(stmt))

    # TimescaleDB is an explicit, installer-declared mode (PostgreSQL only).
    _timescale = settings.enable_timescaledb and settings.db_type == "postgresql"
    if _timescale and op.get_bind().execute(sa.text(
        "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
    )).scalar() is None:
        # Secondary guard: the flag is on but the extension isn't installed.
        # Build regular tables instead of failing on undefined TimescaleDB
        # functions; the scheduler's has_timescaledb probe keeps app-level
        # aggregation running.
        logger.warning(
            "TSIGMA_ENABLE_TIMESCALEDB is set but the timescaledb extension is "
            "not installed; creating regular tables (no hypertables / continuous "
            "aggregates)."
        )
        _timescale = False

    # ------------------------------------------------------------------
    # Schema-placement + idempotency shim
    #
    # The op.create_table()/op.create_index() calls below (and in
    # migration_aggregates_helpers) omit ``schema=``; inject it from an
    # authoritative table->logical-schema map via the same resolver the models
    # use (``dialect.schema()`` — None on MySQL, so tables land in the default
    # database).  Also make create_table/create_index skip when the object
    # already exists, so re-running upgrade head is a safe no-op.  Both shims
    # are restored at the end of upgrade().  Cross-schema FK / index / trigger
    # references resolve through the tsigma role's search_path.
    # ------------------------------------------------------------------
    _SCHEMA_BY_TABLE: dict[str, str] = {}
    for _t in (
        "alert_suppression", "approach", "approach_audit", "controller_type",
        "corridor", "detection_hardware", "detector", "detector_audit",
        "direction_type", "event_code_definition", "jurisdiction", "lane_type",
        "movement_type", "region", "roadside_sensor", "roadside_sensor_audit",
        "roadside_sensor_lane", "roadside_sensor_lane_audit",
        "roadside_sensor_model", "roadside_sensor_vendor", "route",
        "route_distance", "route_phase", "route_signal", "signal",
        "signal_audit", "signal_plan", "system_setting",
    ):
        _SCHEMA_BY_TABLE[_t] = "config"
    for _t in (
        "approach_delay_15min", "approach_speed_15min", "arrival_on_red_hourly",
        "coordination_quality_hourly", "cycle_boundary", "cycle_detector_arrival",
        "cycle_summary_15min", "detector_occupancy_hourly",
        "detector_volume_hourly", "phase_cycle_15min", "phase_left_turn_gap_15min",
        "phase_pedestrian_15min", "phase_termination_hourly", "preemption_15min",
        "priority_15min", "signal_event_count_15min", "split_failure_hourly",
        "yellow_red_activation_15min",
    ):
        _SCHEMA_BY_TABLE[_t] = "aggregation"
    for _t in ("controller_event_log", "polling_checkpoint", "roadside_event"):
        _SCHEMA_BY_TABLE[_t] = "events"
    for _t in ("api_key", "auth_audit_log", "auth_user", "system_setting_audit"):
        _SCHEMA_BY_TABLE[_t] = "identity"

    _orig_create_table = op.create_table
    _orig_create_index = op.create_index

    def _schema_of(table_name: str) -> str | None:
        logical = _SCHEMA_BY_TABLE.get(table_name)
        return dialect.schema(logical) if logical is not None else None

    def _create_table_schema_aware(name, *cols, **kw):
        if not kw.get("schema"):
            schema = _schema_of(name)
            if schema is not None:
                kw["schema"] = schema
        # Idempotent: skip if the table already exists (re-run / reset version).
        if sa.inspect(op.get_bind()).has_table(name, schema=kw.get("schema")):
            return
        return _orig_create_table(name, *cols, **kw)

    def _create_index_idempotent(index_name, table_name, *cols, **kw):
        schema = kw.get("schema") or _schema_of(table_name)
        insp = sa.inspect(op.get_bind())
        if insp.has_table(table_name, schema=schema) and index_name in {
            ix["name"] for ix in insp.get_indexes(table_name, schema=schema)
        }:
            return  # idempotent: index already present
        return _orig_create_index(index_name, table_name, *cols, **kw)

    op.create_table = _create_table_schema_aware
    op.create_index = _create_index_idempotent

    # ------------------------------------------------------------------
    # Enum types
    # ------------------------------------------------------------------
    user_role = postgresql.ENUM("ADMIN", "VIEWER", name="user_role", create_type=False)
    user_role.create(op.get_bind(), checkfirst=True)

    # ------------------------------------------------------------------
    # Reference / lookup tables (no foreign-key dependencies)
    # ------------------------------------------------------------------
    op.create_table(
        "direction_type",
        sa.Column("direction_type_id", sa.SmallInteger, primary_key=True),
        sa.Column("abbreviation", sa.Text, nullable=False),
        sa.Column("description", sa.Text, nullable=False),
    )

    op.create_table(
        "controller_type",
        sa.Column(
            "controller_type_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("description", sa.Text, nullable=False),
        sa.Column("snmp_port", sa.Integer, nullable=False, server_default="161"),
        sa.Column("ftp_directory", sa.Text, nullable=True),
        sa.Column("active_ftp", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("username", sa.Text, nullable=True),
        sa.Column("password", sa.Text, nullable=True),
    )

    op.create_table(
        "lane_type",
        sa.Column(
            "lane_type_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("description", sa.Text, nullable=False),
        sa.Column("abbreviation", sa.Text, nullable=True),
    )

    op.create_table(
        "movement_type",
        sa.Column(
            "movement_type_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("description", sa.Text, nullable=False),
        sa.Column("abbreviation", sa.Text, nullable=True),
        sa.Column("display_order", sa.SmallInteger, nullable=True),
    )

    op.create_table(
        "detection_hardware",
        sa.Column(
            "detection_hardware_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.Text, nullable=False),
    )

    op.create_table(
        "jurisdiction",
        sa.Column(
            "jurisdiction_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("mpo_name", sa.Text, nullable=True),
        sa.Column("county_name", sa.Text, nullable=True),
    )

    op.create_table(
        "event_code_definition",
        sa.Column("event_code", sa.SmallInteger, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("category", sa.Text, nullable=False),
        sa.Column("param_type", sa.Text, nullable=False),
    )

    op.create_table(
        "route",
        sa.Column(
            "route_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.Text, nullable=False),
    )

    # Self-referencing hierarchy
    op.create_table(
        "region",
        sa.Column(
            "region_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "parent_region_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("region.region_id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("description", sa.Text, nullable=False),
    )

    # ------------------------------------------------------------------
    # Corridor (references jurisdiction)
    # ------------------------------------------------------------------
    op.create_table(
        "corridor",
        sa.Column(
            "corridor_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column(
            "jurisdiction_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("jurisdiction.jurisdiction_id"),
            nullable=True,
        ),
    )

    # ------------------------------------------------------------------
    # Signal (references jurisdiction, region, corridor, controller_type)
    # ------------------------------------------------------------------
    op.create_table(
        "signal",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("primary_street", sa.Text, nullable=False),
        sa.Column("secondary_street", sa.Text, nullable=True),
        sa.Column("latitude", sa.Numeric, nullable=True),
        sa.Column("longitude", sa.Numeric, nullable=True),
        sa.Column(
            "jurisdiction_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("jurisdiction.jurisdiction_id"),
            nullable=True,
        ),
        sa.Column(
            "region_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("region.region_id"),
            nullable=True,
        ),
        sa.Column(
            "corridor_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("corridor.corridor_id"),
            nullable=True,
        ),
        sa.Column(
            "controller_type_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("controller_type.controller_type_id"),
            nullable=True,
        ),
        sa.Column("ip_address", postgresql.INET, nullable=True),
        sa.Column("note", sa.Text, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column("enabled", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("start_date", sa.Date, nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("idx_signal_region", "signal", ["region_id"])
    op.create_index("idx_signal_corridor", "signal", ["corridor_id"])
    op.create_index("idx_signal_controller_type", "signal", ["controller_type_id"])
    op.create_index(
        "idx_signal_metadata", "signal", ["metadata"], postgresql_using="gin",
    )

    # ------------------------------------------------------------------
    # Approach (references signal, direction_type)
    # ------------------------------------------------------------------
    op.create_table(
        "approach",
        sa.Column(
            "approach_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "signal_id",
            sa.Text,
            sa.ForeignKey("signal.signal_id"),
            nullable=False,
        ),
        sa.Column(
            "direction_type_id",
            sa.SmallInteger,
            sa.ForeignKey("direction_type.direction_type_id"),
            nullable=False,
        ),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("mph", sa.SmallInteger, nullable=True),
        sa.Column("protected_phase_number", sa.SmallInteger, nullable=True),
        sa.Column(
            "is_protected_phase_overlap",
            sa.Boolean,
            nullable=False,
            server_default="false",
        ),
        sa.Column("permissive_phase_number", sa.SmallInteger, nullable=True),
        sa.Column(
            "is_permissive_phase_overlap",
            sa.Boolean,
            nullable=False,
            server_default="false",
        ),
        sa.Column("ped_phase_number", sa.SmallInteger, nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("idx_approach_signal", "approach", ["signal_id"])

    # ------------------------------------------------------------------
    # Detector (references approach, lane_type, movement_type, detection_hardware)
    # ------------------------------------------------------------------
    op.create_table(
        "detector",
        sa.Column(
            "detector_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "approach_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("approach.approach_id"),
            nullable=False,
        ),
        sa.Column("detector_channel", sa.SmallInteger, nullable=False),
        sa.Column("distance_from_stop_bar", sa.Integer, nullable=True),
        sa.Column("min_speed_filter", sa.SmallInteger, nullable=True),
        sa.Column("decision_point", sa.Integer, nullable=True),
        sa.Column("movement_delay", sa.SmallInteger, nullable=True),
        sa.Column("lane_number", sa.SmallInteger, nullable=True),
        sa.Column(
            "lane_type_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("lane_type.lane_type_id"),
            nullable=True,
        ),
        sa.Column(
            "movement_type_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("movement_type.movement_type_id"),
            nullable=True,
        ),
        sa.Column(
            "detection_hardware_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("detection_hardware.detection_hardware_id"),
            nullable=True,
        ),
        sa.Column("lat_lon_distance", sa.Integer, nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("idx_detector_approach", "detector", ["approach_id"])

    # ------------------------------------------------------------------
    # Signal plan (composite PK: signal_id + effective_from)
    # ------------------------------------------------------------------
    op.create_table(
        "signal_plan",
        sa.Column(
            "signal_id",
            sa.Text,
            sa.ForeignKey("signal.signal_id"),
            primary_key=True,
        ),
        sa.Column(
            "effective_from",
            postgresql.TIMESTAMP(timezone=True),
            primary_key=True,
        ),
        sa.Column("effective_to", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("plan_number", sa.SmallInteger, nullable=False),
        sa.Column("cycle_length", sa.SmallInteger, nullable=True),
        sa.Column("offset", sa.SmallInteger, nullable=True),
        sa.Column("splits", postgresql.JSONB, nullable=True),
    )
    op.create_index(
        "idx_signal_plan_signal",
        "signal_plan",
        [sa.text("signal_id"), sa.text("effective_from DESC")],
    )
    op.create_index(
        "idx_signal_plan_active",
        "signal_plan",
        ["signal_id"],
        postgresql_where=sa.text("effective_to IS NULL"),
    )
    op.create_index(
        "idx_signal_plan_number", "signal_plan", ["signal_id", "plan_number"],
    )

    # ------------------------------------------------------------------
    # Polling checkpoint (composite PK: device_type + device_id + method)
    #
    # Device-polymorphic: device_type is 'controller' (device_id =
    # Signal.signal_id) or 'sensor' (device_id = stringified
    # RoadsideSensor.sensor_id).  No FK on device_id because the target
    # table varies with device_type — application layer (DeviceSource)
    # upholds referential integrity.
    # ------------------------------------------------------------------
    op.create_table(
        "polling_checkpoint",
        sa.Column("device_type", sa.Text, primary_key=True),
        sa.Column("device_id", sa.Text, primary_key=True),
        sa.Column("method", sa.Text, primary_key=True),
        sa.Column("last_filename", sa.Text, nullable=True),
        sa.Column("last_file_mtime", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("files_hash", sa.Text, nullable=True),
        sa.Column("last_event_timestamp", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_successful_poll", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("events_ingested", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("files_ingested", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("consecutive_silent_cycles", sa.Integer, nullable=False, server_default="0"),
        sa.Column("consecutive_errors", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text, nullable=True),
        sa.Column("last_error_time", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "idx_checkpoint_last_poll",
        "polling_checkpoint",
        ["method", "last_successful_poll"],
    )
    op.create_index(
        "idx_checkpoint_errors",
        "polling_checkpoint",
        ["consecutive_errors"],
        postgresql_where=sa.text("consecutive_errors > 0"),
    )

    # ------------------------------------------------------------------
    # Route tables (route_signal, route_phase, route_distance)
    # ------------------------------------------------------------------
    op.create_table(
        "route_signal",
        sa.Column(
            "route_signal_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "route_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("route.route_id"),
            nullable=False,
        ),
        sa.Column(
            "signal_id",
            sa.Text,
            sa.ForeignKey("signal.signal_id"),
            nullable=False,
        ),
        sa.Column("sequence_order", sa.SmallInteger, nullable=False),
        sa.UniqueConstraint("route_id", "sequence_order", name="uq_route_signal_order"),
        sa.UniqueConstraint("route_id", "signal_id", name="uq_route_signal_id"),
    )
    op.create_index(
        "idx_route_signal_route", "route_signal", ["route_id", "sequence_order"],
    )

    op.create_table(
        "route_phase",
        sa.Column(
            "route_phase_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "route_signal_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("route_signal.route_signal_id"),
            nullable=False,
        ),
        sa.Column("phase_number", sa.SmallInteger, nullable=False),
        sa.Column(
            "direction_type_id",
            sa.SmallInteger,
            sa.ForeignKey("direction_type.direction_type_id"),
            nullable=False,
        ),
        sa.Column("is_overlap", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("is_primary_approach", sa.Boolean, nullable=False, server_default="false"),
    )
    op.create_index("idx_route_phase_signal", "route_phase", ["route_signal_id"])

    op.create_table(
        "route_distance",
        sa.Column(
            "route_distance_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "from_route_signal_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("route_signal.route_signal_id"),
            nullable=False,
        ),
        sa.Column(
            "to_route_signal_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("route_signal.route_signal_id"),
            nullable=False,
        ),
        sa.Column("distance_feet", sa.Integer, nullable=False),
        sa.Column("travel_time_seconds", sa.SmallInteger, nullable=True),
        sa.UniqueConstraint(
            "from_route_signal_id", "to_route_signal_id", name="uq_route_distance",
        ),
    )
    op.create_index(
        "idx_route_distance_from", "route_distance", ["from_route_signal_id"],
    )

    # ------------------------------------------------------------------
    # Controller event log — partitioned on event_time across all dialects.
    #
    #   PostgreSQL  : standard table + TimescaleDB hypertable conversion.
    #   MS-SQL      : partition function + scheme, table created ON scheme.
    #   Oracle      : native INTERVAL partitioning (auto-creates partitions).
    #   MySQL       : RANGE partitioning by UNIX_TIMESTAMP(event_time).
    #
    # Chunk / partition interval is driven by
    # settings.event_log_partition_interval_days (default: 1 day) — the
    # same setting the manage_partitions scheduler job reads to extend the
    # rolling window.
    # ------------------------------------------------------------------
    chunk_days = int(settings.event_log_partition_interval_days)
    dialect_name = op.get_bind().dialect.name

    if dialect_name == "postgresql":
        _create_event_log_postgresql(chunk_days, _timescale)
    elif dialect_name == "mssql":
        _create_event_log_mssql(chunk_days)
    elif dialect_name == "oracle":
        _create_event_log_oracle(chunk_days)
    elif dialect_name == "mysql":
        _create_event_log_mysql(chunk_days)
    else:
        raise RuntimeError(
            f"controller_event_log migration: unsupported dialect "
            f"{dialect_name!r} — supported: postgresql, mssql, oracle, mysql"
        )

    # ------------------------------------------------------------------
    # Audit tables (no enforced FKs — populated by DB triggers)
    # ------------------------------------------------------------------
    op.create_table(
        "signal_audit",
        sa.Column("audit_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("signal_id", sa.Text, nullable=False),
        sa.Column(
            "changed_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("changed_by", sa.Text, nullable=True),
        sa.Column("operation", sa.Text, nullable=False),
        sa.Column("old_values", postgresql.JSONB, nullable=True),
        sa.Column("new_values", postgresql.JSONB, nullable=True),
    )
    op.create_index(
        "idx_signal_audit_signal",
        "signal_audit",
        [sa.text("signal_id"), sa.text("changed_at DESC")],
    )
    op.create_index(
        "idx_signal_audit_time",
        "signal_audit",
        [sa.text("changed_at DESC")],
    )

    op.create_table(
        "approach_audit",
        sa.Column("audit_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("approach_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("signal_id", sa.Text, nullable=False),
        sa.Column(
            "changed_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("changed_by", sa.Text, nullable=True),
        sa.Column("operation", sa.Text, nullable=False),
        sa.Column("old_values", postgresql.JSONB, nullable=True),
        sa.Column("new_values", postgresql.JSONB, nullable=True),
    )
    op.create_index(
        "idx_approach_audit_approach",
        "approach_audit",
        [sa.text("approach_id"), sa.text("changed_at DESC")],
    )
    op.create_index(
        "idx_approach_audit_signal",
        "approach_audit",
        [sa.text("signal_id"), sa.text("changed_at DESC")],
    )
    op.create_index(
        "idx_approach_audit_time",
        "approach_audit",
        [sa.text("changed_at DESC")],
    )

    op.create_table(
        "detector_audit",
        sa.Column("audit_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("detector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("approach_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "changed_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("changed_by", sa.Text, nullable=True),
        sa.Column("operation", sa.Text, nullable=False),
        sa.Column("old_values", postgresql.JSONB, nullable=True),
        sa.Column("new_values", postgresql.JSONB, nullable=True),
    )
    op.create_index(
        "idx_detector_audit_detector",
        "detector_audit",
        [sa.text("detector_id"), sa.text("changed_at DESC")],
    )
    op.create_index(
        "idx_detector_audit_approach",
        "detector_audit",
        [sa.text("approach_id"), sa.text("changed_at DESC")],
    )
    op.create_index(
        "idx_detector_audit_time",
        "detector_audit",
        [sa.text("changed_at DESC")],
    )

    op.create_table(
        "auth_audit_log",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.Text, nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("username", sa.Text, nullable=False),
        sa.Column("ip_address", sa.Text, nullable=True),
        sa.Column("user_agent", sa.Text, nullable=True),
        sa.Column(
            "timestamp",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "idx_auth_audit_user",
        "auth_audit_log",
        [sa.text("user_id"), sa.text("timestamp DESC")],
    )
    op.create_index(
        "idx_auth_audit_type",
        "auth_audit_log",
        [sa.text("event_type"), sa.text("timestamp DESC")],
    )
    op.create_index(
        "idx_auth_audit_time",
        "auth_audit_log",
        [sa.text("timestamp DESC")],
    )

    # ------------------------------------------------------------------
    # Auth user
    # ------------------------------------------------------------------
    op.create_table(
        "auth_user",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("username", sa.Text, nullable=False),
        sa.Column("password_hash", sa.Text, nullable=False),
        sa.Column(
            "role",
            user_role,
            nullable=False,
        ),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("external_id", sa.Text, nullable=True),
        sa.Column("external_provider", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("idx_auth_user_username", "auth_user", ["username"], unique=True)
    op.create_index(
        "idx_auth_user_external",
        "auth_user",
        ["external_provider", "external_id"],
        unique=True,
        postgresql_where=sa.text("external_id IS NOT NULL"),
    )

    # ------------------------------------------------------------------
    # API key (programmatic auth; references auth_user)
    # ------------------------------------------------------------------
    op.create_table(
        "api_key",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("auth_user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("key_hash", sa.Text, nullable=False),
        sa.Column("key_prefix", sa.Text, nullable=False),
        sa.Column(
            "role",
            user_role,
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("revoked_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_used_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index("idx_api_key_user_id", "api_key", ["user_id"])
    op.create_index("idx_api_key_prefix", "api_key", ["key_prefix"])
    op.create_index("idx_api_key_expires_at", "api_key", ["expires_at"])

    # ------------------------------------------------------------------
    # System setting
    # ------------------------------------------------------------------
    op.create_table(
        "system_setting",
        sa.Column("key", sa.String(255), primary_key=True),
        sa.Column("value", sa.Text, nullable=False),
        sa.Column("category", sa.String(100), nullable=False),
        sa.Column("description", sa.Text, nullable=False, server_default=""),
        sa.Column("editable", sa.Boolean, nullable=False, server_default="true"),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("updated_by", sa.String(255), nullable=True),
    )
    op.create_index("ix_system_setting_category", "system_setting", ["category"])

    # ------------------------------------------------------------------
    # Alert suppression — watchdog check throttling rules
    # ------------------------------------------------------------------
    insp = sa.inspect(op.get_bind())
    if not insp.has_table("alert_suppression"):
        op.create_table(
            "alert_suppression",
            sa.Column(
                "suppression_id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column("signal_id", sa.Text, nullable=True),
            sa.Column("check_name", sa.Text, nullable=False),
            sa.Column("reason", sa.Text, nullable=True),
            sa.Column("created_by", sa.Text, nullable=True),
            sa.Column(
                "created_at",
                postgresql.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column("expires_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        )
    if "idx_alert_suppression_lookup" not in _existing_index_names("alert_suppression"):
        op.create_index(
            "idx_alert_suppression_lookup",
            "alert_suppression",
            ["check_name", "signal_id", "expires_at"],
        )

    # ------------------------------------------------------------------
    # Aggregate tables (analytics — no FKs)
    # ------------------------------------------------------------------
    op.create_table(
        "detector_volume_hourly",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("detector_channel", sa.Integer, primary_key=True),
        sa.Column("hour_start", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("volume", sa.Integer, nullable=False, server_default="0"),
        sa.Column("activations", sa.Integer, nullable=False, server_default="0"),
    )
    op.create_index("idx_dvh_signal_hour", "detector_volume_hourly", ["signal_id", "hour_start"])
    op.create_index("idx_dvh_hour", "detector_volume_hourly", ["hour_start"])

    op.create_table(
        "detector_occupancy_hourly",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("detector_channel", sa.Integer, primary_key=True),
        sa.Column("hour_start", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("occupancy_pct", sa.Float, nullable=False, server_default="0"),
        sa.Column("total_on_seconds", sa.Float, nullable=False, server_default="0"),
        sa.Column("activation_count", sa.Integer, nullable=False, server_default="0"),
    )
    op.create_index("idx_doh_signal_hour", "detector_occupancy_hourly", ["signal_id", "hour_start"])
    op.create_index("idx_doh_hour", "detector_occupancy_hourly", ["hour_start"])

    op.create_table(
        "split_failure_hourly",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("phase", sa.Integer, primary_key=True),
        sa.Column("hour_start", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("total_cycles", sa.Integer, nullable=False, server_default="0"),
        sa.Column("failed_cycles", sa.Integer, nullable=False, server_default="0"),
        sa.Column("failure_rate_pct", sa.Float, nullable=False, server_default="0"),
    )
    op.create_index("idx_sfh_signal_hour", "split_failure_hourly", ["signal_id", "hour_start"])
    op.create_index("idx_sfh_hour", "split_failure_hourly", ["hour_start"])

    op.create_table(
        "approach_delay_15min",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("phase", sa.Integer, primary_key=True),
        sa.Column("bin_start", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("avg_delay_seconds", sa.Float, nullable=False, server_default="0"),
        sa.Column("max_delay_seconds", sa.Float, nullable=False, server_default="0"),
        sa.Column("total_arrivals", sa.Integer, nullable=False, server_default="0"),
    )
    op.create_index("idx_ad15_signal_bin", "approach_delay_15min", ["signal_id", "bin_start"])
    op.create_index("idx_ad15_bin", "approach_delay_15min", ["bin_start"])

    op.create_table(
        "arrival_on_red_hourly",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("phase", sa.Integer, primary_key=True),
        sa.Column("hour_start", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("total_arrivals", sa.Integer, nullable=False, server_default="0"),
        sa.Column("arrivals_on_red", sa.Integer, nullable=False, server_default="0"),
        sa.Column("arrivals_on_green", sa.Integer, nullable=False, server_default="0"),
        sa.Column("red_pct", sa.Float, nullable=False, server_default="0"),
        sa.Column("green_pct", sa.Float, nullable=False, server_default="0"),
    )
    op.create_index("idx_aor_signal_hour", "arrival_on_red_hourly", ["signal_id", "hour_start"])
    op.create_index("idx_aor_hour", "arrival_on_red_hourly", ["hour_start"])

    op.create_table(
        "coordination_quality_hourly",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("hour_start", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("total_cycles", sa.Integer, nullable=False, server_default="0"),
        sa.Column("cycles_within_tolerance", sa.Integer, nullable=False, server_default="0"),
        sa.Column("quality_pct", sa.Float, nullable=False, server_default="0"),
        sa.Column("avg_cycle_length_seconds", sa.Float, nullable=False, server_default="0"),
        sa.Column("avg_offset_error_seconds", sa.Float, nullable=False, server_default="0"),
    )
    op.create_index(
        "idx_cqh_signal_hour", "coordination_quality_hourly", ["signal_id", "hour_start"],
    )
    op.create_index("idx_cqh_hour", "coordination_quality_hourly", ["hour_start"])

    op.create_table(
        "phase_termination_hourly",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("phase", sa.Integer, primary_key=True),
        sa.Column("hour_start", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("total_cycles", sa.Integer, nullable=False, server_default="0"),
        sa.Column("gap_outs", sa.Integer, nullable=False, server_default="0"),
        sa.Column("max_outs", sa.Integer, nullable=False, server_default="0"),
        sa.Column("force_offs", sa.Integer, nullable=False, server_default="0"),
    )
    op.create_index(
        "idx_pth_signal_hour", "phase_termination_hourly", ["signal_id", "hour_start"],
    )
    op.create_index("idx_pth_hour", "phase_termination_hourly", ["hour_start"])

    # ------------------------------------------------------------------
    # Second-batch aggregates (speed / cycle / left-turn gap / pedestrian
    # / preemption / priority / signal event count / yellow-red activation).
    # All are 15-minute binned.  Idempotency is provided by the table-
    # creation path already — alembic op.create_table() is wrapped below
    # in inspect().has_table() checks so the migration is safe to re-run.
    # ------------------------------------------------------------------
    _create_second_batch_aggregate_tables()

    # ------------------------------------------------------------------
    # PCD Cycle Aggregate Tables
    # ------------------------------------------------------------------

    op.create_table(
        "cycle_boundary",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("phase", sa.Integer, primary_key=True),
        sa.Column("green_start", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("yellow_start", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("red_start", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("cycle_end", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("green_duration_seconds", sa.Float, nullable=True),
        sa.Column("yellow_duration_seconds", sa.Float, nullable=True),
        sa.Column("red_duration_seconds", sa.Float, nullable=True),
        sa.Column("cycle_duration_seconds", sa.Float, nullable=True),
        sa.Column("termination_type", sa.Text, nullable=True),
    )
    op.create_index(
        "idx_cb_signal_phase_green", "cycle_boundary",
        ["signal_id", "phase", sa.text("green_start DESC")],
    )
    op.create_index(
        "idx_cb_green_start", "cycle_boundary",
        [sa.text("green_start DESC")],
    )

    op.create_table(
        "cycle_detector_arrival",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("phase", sa.Integer, primary_key=True),
        sa.Column("detector_channel", sa.Integer, primary_key=True),
        sa.Column("arrival_time", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("green_start", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("time_in_cycle_seconds", sa.Float, nullable=False),
        sa.Column("phase_state", sa.Text, nullable=False),
    )
    op.create_index(
        "idx_cda_signal_phase_arrival", "cycle_detector_arrival",
        ["signal_id", "phase", sa.text("arrival_time DESC")],
    )
    op.create_index(
        "idx_cda_green_start", "cycle_detector_arrival",
        [sa.text("green_start DESC")],
    )
    op.create_index(
        "idx_cda_arrival_time", "cycle_detector_arrival",
        [sa.text("arrival_time DESC")],
    )

    op.create_table(
        "cycle_summary_15min",
        sa.Column("signal_id", sa.Text, primary_key=True),
        sa.Column("phase", sa.Integer, primary_key=True),
        sa.Column("bin_start", postgresql.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("total_cycles", sa.Integer, nullable=False, server_default="0"),
        sa.Column("avg_cycle_length_seconds", sa.Float, nullable=False, server_default="0"),
        sa.Column("avg_green_seconds", sa.Float, nullable=False, server_default="0"),
        sa.Column("total_arrivals", sa.Integer, nullable=False, server_default="0"),
        sa.Column("arrivals_on_green", sa.Integer, nullable=False, server_default="0"),
        sa.Column("arrivals_on_yellow", sa.Integer, nullable=False, server_default="0"),
        sa.Column("arrivals_on_red", sa.Integer, nullable=False, server_default="0"),
        sa.Column("arrival_on_green_pct", sa.Float, nullable=False, server_default="0"),
    )
    op.create_index(
        "idx_cs15_signal_phase_bin", "cycle_summary_15min",
        ["signal_id", "phase", sa.text("bin_start DESC")],
    )
    op.create_index(
        "idx_cs15_bin", "cycle_summary_15min",
        [sa.text("bin_start DESC")],
    )

    # ------------------------------------------------------------------
    # Audit triggers (dialect-aware)
    # ------------------------------------------------------------------
    for table, audit_table, id_cols in [
        ("signal", "signal_audit", ["signal_id"]),
        ("approach", "approach_audit", ["approach_id", "signal_id"]),
        ("detector", "detector_audit", ["detector_id", "approach_id"]),
    ]:
        for stmt in dialect.audit_trigger_sql(table, audit_table, id_cols):
            op.execute(sa.text(stmt))

    # ------------------------------------------------------------------
    # TimescaleDB Continuous Aggregates for PCD cycle data
    #
    # When TimescaleDB is available, replace the regular cycle tables
    # with continuous aggregate views. Same names, same columns —
    # reports query identically regardless of backend.
    #
    # The scheduler jobs (_should_skip) detect TimescaleDB and disable
    # themselves, since the continuous aggregates handle refresh.
    # ------------------------------------------------------------------
    _cycle_cagg_sql = (
        "DO $$ BEGIN "
        "  IF NOT EXISTS (SELECT 1 FROM timescaledb_information.continuous_aggregates "
        "                 WHERE view_schema = 'aggregation' "
        "                   AND view_name = 'cycle_boundary') THEN "
        "  DROP TABLE IF EXISTS aggregation.cycle_summary_15min; "
        "  DROP TABLE IF EXISTS aggregation.cycle_detector_arrival; "
        "  DROP TABLE IF EXISTS aggregation.cycle_boundary; "
        "  CREATE MATERIALIZED VIEW aggregation.cycle_boundary "
        "  WITH (timescaledb.continuous) AS "
        "  SELECT "
        "    signal_id, "
        "    event_param AS phase, "
        "    time_bucket('1 second', event_time) AS green_start, "
        "    MAX(event_time) FILTER (WHERE event_code = 8) AS yellow_start, "
        "    MAX(event_time) FILTER (WHERE event_code = 9) AS red_start, "
        "    MAX(event_time) FILTER (WHERE event_code = 10) AS cycle_end, "
        "    EXTRACT(EPOCH FROM "
        "      MAX(event_time) FILTER (WHERE event_code = 8) - "
        "      MIN(event_time) FILTER (WHERE event_code = 1) "
        "    ) AS green_duration_seconds, "
        "    EXTRACT(EPOCH FROM "
        "      MAX(event_time) FILTER (WHERE event_code = 9) - "
        "      MAX(event_time) FILTER (WHERE event_code = 8) "
        "    ) AS yellow_duration_seconds, "
        "    EXTRACT(EPOCH FROM "
        "      MAX(event_time) FILTER (WHERE event_code = 10) - "
        "      MAX(event_time) FILTER (WHERE event_code = 9) "
        "    ) AS red_duration_seconds, "
        "    EXTRACT(EPOCH FROM "
        "      MAX(event_time) FILTER (WHERE event_code = 10) - "
        "      MIN(event_time) FILTER (WHERE event_code = 1) "
        "    ) AS cycle_duration_seconds, "
        "    CASE "
        "      WHEN COUNT(*) FILTER (WHERE event_code = 4) > 0 THEN 'gap_out' "
        "      WHEN COUNT(*) FILTER (WHERE event_code = 5) > 0 THEN 'max_out' "
        "      WHEN COUNT(*) FILTER (WHERE event_code = 6) > 0 THEN 'force_off' "
        "      ELSE NULL "
        "    END AS termination_type "
        "  FROM controller_event_log "
        "  WHERE event_code IN (1, 4, 5, 6, 8, 9, 10) "
        "  GROUP BY signal_id, event_param, time_bucket('1 second', event_time) "
        "  WITH NO DATA; "
        "  PERFORM add_continuous_aggregate_policy('aggregation.cycle_boundary', "
        "    start_offset => INTERVAL '2 hours', "
        "    end_offset => INTERVAL '1 minute', "
        "    schedule_interval => INTERVAL '15 minutes', "
        "    if_not_exists => true "
        "  ); "
        "  CREATE TABLE aggregation.cycle_detector_arrival ( "
        "    signal_id TEXT NOT NULL, "
        "    phase INTEGER NOT NULL, "
        "    detector_channel INTEGER NOT NULL, "
        "    arrival_time TIMESTAMPTZ NOT NULL, "
        "    green_start TIMESTAMPTZ NOT NULL, "
        "    time_in_cycle_seconds DOUBLE PRECISION NOT NULL, "
        "    phase_state TEXT NOT NULL, "
        "    PRIMARY KEY (signal_id, phase, detector_channel, arrival_time) "
        "  ); "
        "  CREATE INDEX idx_cda_signal_phase_arrival "
        "    ON aggregation.cycle_detector_arrival (signal_id, phase, arrival_time DESC); "
        "  CREATE INDEX idx_cda_green_start "
        "    ON aggregation.cycle_detector_arrival (green_start DESC); "
        "  CREATE INDEX idx_cda_arrival_time "
        "    ON aggregation.cycle_detector_arrival (arrival_time DESC); "
        "  CREATE TABLE aggregation.cycle_summary_15min ( "
        "    signal_id TEXT NOT NULL, "
        "    phase INTEGER NOT NULL, "
        "    bin_start TIMESTAMPTZ NOT NULL, "
        "    total_cycles INTEGER NOT NULL DEFAULT 0, "
        "    avg_cycle_length_seconds DOUBLE PRECISION NOT NULL DEFAULT 0, "
        "    avg_green_seconds DOUBLE PRECISION NOT NULL DEFAULT 0, "
        "    total_arrivals INTEGER NOT NULL DEFAULT 0, "
        "    arrivals_on_green INTEGER NOT NULL DEFAULT 0, "
        "    arrivals_on_yellow INTEGER NOT NULL DEFAULT 0, "
        "    arrivals_on_red INTEGER NOT NULL DEFAULT 0, "
        "    arrival_on_green_pct DOUBLE PRECISION NOT NULL DEFAULT 0, "
        "    PRIMARY KEY (signal_id, phase, bin_start) "
        "  ); "
        "  CREATE INDEX idx_cs15_signal_phase_bin "
        "    ON aggregation.cycle_summary_15min (signal_id, phase, bin_start DESC); "
        "  CREATE INDEX idx_cs15_bin "
        "    ON aggregation.cycle_summary_15min (bin_start DESC); "
        "  END IF; "
        "END $$;"
    )
    if _timescale:
        op.execute(_cycle_cagg_sql)

    # ------------------------------------------------------------------
    # TimescaleDB Continuous Aggregates for the second-batch tables.
    # Each view replaces the regular table of the same name when
    # TimescaleDB is available; the scheduler ``agg_*`` job noops in
    # that case (see ``_should_skip``).  All DDL is guarded by the
    # ``undefined_function`` EXCEPTION so non-TimescaleDB PostgreSQL
    # deployments retain the regular tables.
    # ------------------------------------------------------------------
    if _timescale:
        for stmt in _second_batch_continuous_aggregates():
            op.execute(stmt)

    # ------------------------------------------------------------------
    # Roadside sensor configuration tables — radar / LiDAR / video
    # devices that live at the roadway edge (Wavetronix, Iteris, FLIR,
    # Houston Radar, Quanergy, etc.).  Parallel to the cabinet-connected
    # ``detector`` table.  See ``tsigma/models/roadside_sensor.py``.
    # ------------------------------------------------------------------
    _create_roadside_sensor_tables()

    # ------------------------------------------------------------------
    # Roadside event stream — per-detection records from the sensors
    # above.  Parallel to ``controller_event_log``: same partitioning
    # shape and chunk cadence, populated by the roadside ingestion
    # pipeline rather than the Indiana Hi-Res controller feed.
    # See ``tsigma/models/roadside_event.py``.
    # ------------------------------------------------------------------
    if dialect_name == "postgresql":
        _create_roadside_event_postgresql(chunk_days, _timescale)
    elif dialect_name == "mssql":
        _create_roadside_event_mssql(chunk_days)
    elif dialect_name == "oracle":
        _create_roadside_event_oracle(chunk_days)
    elif dialect_name == "mysql":
        _create_roadside_event_mysql(chunk_days)
    else:
        raise RuntimeError(
            f"roadside_event migration: unsupported dialect "
            f"{dialect_name!r} — supported: postgresql, mssql, oracle, mysql"
        )

    # Restore op.create_table / op.create_index (the shims are process-local).
    op.create_table = _orig_create_table
    op.create_index = _orig_create_index


# ---------------------------------------------------------------------------
# Dialect-specific CREATE TABLE for controller_event_log
#
# Fixed anchor for the initial partition boundary.  Any date before the
# deployment start is safe — the initial partition absorbs any data older
# than the anchor; the manage_partitions scheduler job extends the range
# forward from here daily.
# ---------------------------------------------------------------------------
_EVENT_LOG_INITIAL_ANCHOR = "2020-01-01 00:00:00"


def _existing_index_names(table_name: str) -> set[str]:
    """Return the set of existing index names on ``table_name``.

    Returns an empty set if the table does not exist — callers use this
    for idempotent index creation alongside ``inspect().has_table``.
    """
    insp = sa.inspect(op.get_bind())
    if not insp.has_table(table_name):
        return set()
    return {idx["name"] for idx in insp.get_indexes(table_name)}


def _create_event_log_postgresql(chunk_days: int, timescale: bool) -> None:
    """PostgreSQL: standard table + optional TimescaleDB hypertable.

    Idempotent: skips table creation when it already exists and guards
    each index.  The TimescaleDB ``create_hypertable`` call already uses
    ``if_not_exists => true``.
    """
    from datetime import date

    from tsigma.database.db import DialectHelper

    insp = sa.inspect(op.get_bind())
    if not insp.has_table("controller_event_log", schema="events"):
        if timescale:
            op.create_table(
                "controller_event_log",
                sa.Column("signal_id", sa.Text, primary_key=True),
                sa.Column(
                    "event_time",
                    postgresql.TIMESTAMP(timezone=True),
                    primary_key=True,
                ),
                sa.Column("event_code", sa.Integer, primary_key=True),
                sa.Column("event_param", sa.Integer, primary_key=True),
                sa.Column("device_id", sa.SmallInteger, nullable=False, server_default="1"),
                sa.Column("validation_metadata", postgresql.JSONB, nullable=True),
            )
        else:
            # Non-TimescaleDB PostgreSQL: native declarative range partitioning,
            # maintained by the manage_partitions scheduler job. A DEFAULT
            # partition catches out-of-window rows; an initial partition covers
            # "today" so inserts route correctly before the job first runs.
            op.execute(
                "CREATE TABLE events.controller_event_log ("
                "  signal_id TEXT NOT NULL, "
                "  event_time TIMESTAMPTZ NOT NULL, "
                "  event_code INTEGER NOT NULL, "
                "  event_param INTEGER NOT NULL, "
                "  device_id SMALLINT NOT NULL DEFAULT 1, "
                "  validation_metadata JSONB, "
                "  PRIMARY KEY (signal_id, event_time, event_code, event_param) "
                ") PARTITION BY RANGE (event_time)"
            )
            op.execute(
                "CREATE TABLE events.controller_event_log_default "
                "PARTITION OF events.controller_event_log DEFAULT"
            )
            for _stmt in DialectHelper("postgresql").ensure_partition_sql(
                "controller_event_log", date.today(), chunk_days,
            ):
                op.execute(_stmt)
    existing = _existing_index_names("controller_event_log")
    if "idx_cel_signal_time" not in existing:
        op.create_index(
            "idx_cel_signal_time",
            "controller_event_log",
            [sa.text("signal_id"), sa.text("event_time DESC")],
        )
    if "idx_cel_event_time" not in existing:
        op.create_index(
            "idx_cel_event_time",
            "controller_event_log",
            [sa.text("event_code"), sa.text("event_time DESC")],
        )
    if "idx_cel_validation_metadata" not in existing:
        op.create_index(
            "idx_cel_validation_metadata",
            "controller_event_log",
            ["validation_metadata"],
            postgresql_using="gin",
            postgresql_where=sa.text("validation_metadata IS NOT NULL"),
        )
    # TimescaleDB hypertable — only when enabled + extension present (the caller
    # passes the verified flag). Idempotent via if_not_exists => true.
    if timescale:
        op.execute(
            "DO $$ BEGIN "
            "  PERFORM create_hypertable("
            "    'events.controller_event_log', 'event_time', "
            f"    chunk_time_interval => INTERVAL '{chunk_days} days', "
            "    migrate_data => true, if_not_exists => true"
            "  ); "
            "END $$;"
        )


def _create_event_log_mssql(chunk_days: int) -> None:
    """MS-SQL: partition function + scheme first, then table ON scheme.

    Each step guarded by an ``IF NOT EXISTS`` sys-catalog check so the
    migration is safe to re-run after a partial failure.
    """
    # Partition function.
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.partition_functions "
        "WHERE name = 'pf_controller_event_log_event_time') "
        "CREATE PARTITION FUNCTION pf_controller_event_log_event_time (DATETIME2) "
        f"AS RANGE RIGHT FOR VALUES ('{_EVENT_LOG_INITIAL_ANCHOR}')"
    )
    # Partition scheme.
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes "
        "WHERE name = 'ps_controller_event_log_event_time') "
        "CREATE PARTITION SCHEME ps_controller_event_log_event_time "
        "AS PARTITION pf_controller_event_log_event_time ALL TO ([PRIMARY])"
    )
    # Table — clustered PK leads with event_time so the partition scheme aligns.
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.tables "
        "WHERE name = 'controller_event_log') "
        "CREATE TABLE controller_event_log ("
        "  signal_id NVARCHAR(64) NOT NULL, "
        "  event_time DATETIME2 NOT NULL, "
        "  event_code INT NOT NULL, "
        "  event_param INT NOT NULL, "
        "  device_id SMALLINT NOT NULL CONSTRAINT df_cel_device_id DEFAULT 1, "
        "  validation_metadata NVARCHAR(MAX) NULL, "
        "  CONSTRAINT pk_controller_event_log PRIMARY KEY CLUSTERED "
        "    (event_time, signal_id, event_code, event_param) "
        "    ON ps_controller_event_log_event_time(event_time) "
        ")"
    )
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes "
        "WHERE name = 'idx_cel_signal_time' "
        "  AND object_id = OBJECT_ID('controller_event_log')) "
        "CREATE INDEX idx_cel_signal_time "
        "ON controller_event_log (signal_id, event_time DESC) "
        "ON ps_controller_event_log_event_time(event_time)"
    )
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes "
        "WHERE name = 'idx_cel_event_time' "
        "  AND object_id = OBJECT_ID('controller_event_log')) "
        "CREATE INDEX idx_cel_event_time "
        "ON controller_event_log (event_code, event_time DESC) "
        "ON ps_controller_event_log_event_time(event_time)"
    )
    logger.info(
        "controller_event_log partitioned on ps_controller_event_log_event_time "
        "(partition cadence: %d day(s))", chunk_days,
    )


def _create_event_log_oracle(chunk_days: int) -> None:
    """Oracle: INTERVAL partitioning — DB auto-creates partitions on insert.

    Idempotent: skips CREATE TABLE when it already exists; index guards
    use SQLAlchemy introspection so a partial re-run picks up where it
    left off.
    """
    insp = sa.inspect(op.get_bind())
    if not insp.has_table("controller_event_log"):
        op.execute(
            "CREATE TABLE controller_event_log ("
            "  signal_id VARCHAR2(64) NOT NULL, "
            "  event_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "  event_code INTEGER NOT NULL, "
            "  event_param INTEGER NOT NULL, "
            "  device_id NUMBER(5) DEFAULT 1 NOT NULL, "
            "  validation_metadata CLOB NULL, "
            "  CONSTRAINT pk_controller_event_log PRIMARY KEY "
            "    (signal_id, event_time, event_code, event_param) "
            ") "
            "PARTITION BY RANGE (event_time) "
            f"INTERVAL (NUMTODSINTERVAL({chunk_days}, 'DAY')) "
            "(PARTITION p_initial VALUES LESS THAN "
            f" (TIMESTAMP '{_EVENT_LOG_INITIAL_ANCHOR}'))"
        )
    existing = _existing_index_names("controller_event_log")
    if "idx_cel_signal_time" not in existing:
        op.create_index(
            "idx_cel_signal_time",
            "controller_event_log",
            [sa.text("signal_id"), sa.text("event_time DESC")],
        )
    if "idx_cel_event_time" not in existing:
        op.create_index(
            "idx_cel_event_time",
            "controller_event_log",
            [sa.text("event_code"), sa.text("event_time DESC")],
        )


def _create_event_log_mysql(chunk_days: int) -> None:
    """MySQL: RANGE partitioning by UNIX_TIMESTAMP(event_time).

    Every unique key must include the partition expression's columns —
    the composite PK already includes event_time, so the constraint is
    met.  Idempotent: ``CREATE TABLE IF NOT EXISTS`` + introspection-based
    index guards.
    """
    op.execute(
        "CREATE TABLE IF NOT EXISTS controller_event_log ("
        "  signal_id VARCHAR(64) NOT NULL, "
        "  event_time DATETIME(6) NOT NULL, "
        "  event_code INT NOT NULL, "
        "  event_param INT NOT NULL, "
        "  device_id SMALLINT NOT NULL DEFAULT 1, "
        "  validation_metadata JSON NULL, "
        "  PRIMARY KEY (signal_id, event_time, event_code, event_param) "
        ") ENGINE=InnoDB "
        "PARTITION BY RANGE (UNIX_TIMESTAMP(event_time)) ("
        "  PARTITION p_initial VALUES LESS THAN "
        f" (UNIX_TIMESTAMP('{_EVENT_LOG_INITIAL_ANCHOR}'))"
        ")"
    )
    existing = _existing_index_names("controller_event_log")
    if "idx_cel_signal_time" not in existing:
        op.create_index(
            "idx_cel_signal_time",
            "controller_event_log",
            [sa.text("signal_id"), sa.text("event_time DESC")],
        )
    if "idx_cel_event_time" not in existing:
        op.create_index(
            "idx_cel_event_time",
            "controller_event_log",
            [sa.text("event_code"), sa.text("event_time DESC")],
        )
    logger.info(
        "controller_event_log partitioned by UNIX_TIMESTAMP(event_time) "
        "(partition cadence: %d day(s))", chunk_days,
    )


# ---------------------------------------------------------------------------
# Dialect-specific CREATE TABLE for roadside_event
#
# Parallel to controller_event_log — radar / LiDAR / video sensors emit
# per-detection records that never flow through the cabinet controller.
# Same partitioning shape (TimescaleDB chunk on PG, native partitioning
# elsewhere) and the same chunk cadence (event_log_partition_interval_days).
# ---------------------------------------------------------------------------


def _create_roadside_event_postgresql(chunk_days: int, timescale: bool) -> None:
    """PostgreSQL: standard table + optional TimescaleDB hypertable."""
    from datetime import date

    from tsigma.database.db import DialectHelper

    insp = sa.inspect(op.get_bind())
    if not insp.has_table("roadside_event", schema="events"):
        if timescale:
            op.create_table(
                "roadside_event",
                sa.Column("signal_id", sa.Text, primary_key=True),
                sa.Column(
                    "sensor_id",
                    postgresql.UUID(as_uuid=True),
                    primary_key=True,
                ),
                sa.Column(
                    "event_time",
                    postgresql.TIMESTAMP(timezone=True),
                    primary_key=True,
                ),
                sa.Column("event_type", sa.SmallInteger, primary_key=True),
                sa.Column("mph", sa.Integer, nullable=True),
                sa.Column("kph", sa.Integer, nullable=True),
                sa.Column("length_feet", sa.Integer, nullable=True),
                sa.Column("vehicle_class", sa.SmallInteger, nullable=True),
                sa.Column("lane_number", sa.SmallInteger, nullable=True),
                sa.Column("direction_id", sa.SmallInteger, nullable=True),
                sa.Column("occupancy_pct", sa.Numeric(5, 2), nullable=True),
                sa.Column("queue_length_feet", sa.Numeric(8, 2), nullable=True),
                sa.Column("vendor_metadata", postgresql.JSONB, nullable=True),
            )
        else:
            # Non-TimescaleDB PostgreSQL: native declarative range partitioning
            # (see _create_event_log_postgresql for the rationale).
            op.execute(
                "CREATE TABLE events.roadside_event ("
                "  signal_id TEXT NOT NULL, "
                "  sensor_id UUID NOT NULL, "
                "  event_time TIMESTAMPTZ NOT NULL, "
                "  event_type SMALLINT NOT NULL, "
                "  mph INTEGER, "
                "  kph INTEGER, "
                "  length_feet INTEGER, "
                "  vehicle_class SMALLINT, "
                "  lane_number SMALLINT, "
                "  direction_id SMALLINT, "
                "  occupancy_pct NUMERIC(5, 2), "
                "  queue_length_feet NUMERIC(8, 2), "
                "  vendor_metadata JSONB, "
                "  PRIMARY KEY (signal_id, sensor_id, event_time, event_type) "
                ") PARTITION BY RANGE (event_time)"
            )
            op.execute(
                "CREATE TABLE events.roadside_event_default "
                "PARTITION OF events.roadside_event DEFAULT"
            )
            for _stmt in DialectHelper("postgresql").ensure_partition_sql(
                "roadside_event", date.today(), chunk_days,
            ):
                op.execute(_stmt)
    existing = _existing_index_names("roadside_event")
    if "idx_re_sensor_time" not in existing:
        op.create_index(
            "idx_re_sensor_time",
            "roadside_event",
            [sa.text("sensor_id"), sa.text("event_time DESC")],
        )
    if "idx_re_signal_time" not in existing:
        op.create_index(
            "idx_re_signal_time",
            "roadside_event",
            [sa.text("signal_id"), sa.text("event_time DESC")],
        )
    if "idx_re_event_type_time" not in existing:
        op.create_index(
            "idx_re_event_type_time",
            "roadside_event",
            [sa.text("event_type"), sa.text("event_time DESC")],
        )
    if not timescale:
        return
    op.execute(
        "DO $$ BEGIN "
        "  PERFORM create_hypertable("
        "    'events.roadside_event', 'event_time', "
        f"    chunk_time_interval => INTERVAL '{chunk_days} days', "
        "    migrate_data => true, if_not_exists => true"
        "  ); "
        "END $$;"
    )


def _create_roadside_event_mssql(chunk_days: int) -> None:
    """MS-SQL: partition function + scheme, table ON scheme."""
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.partition_functions "
        "WHERE name = 'pf_roadside_event_event_time') "
        "CREATE PARTITION FUNCTION pf_roadside_event_event_time (DATETIME2) "
        f"AS RANGE RIGHT FOR VALUES ('{_EVENT_LOG_INITIAL_ANCHOR}')"
    )
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes "
        "WHERE name = 'ps_roadside_event_event_time') "
        "CREATE PARTITION SCHEME ps_roadside_event_event_time "
        "AS PARTITION pf_roadside_event_event_time ALL TO ([PRIMARY])"
    )
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.tables "
        "WHERE name = 'roadside_event') "
        "CREATE TABLE roadside_event ("
        "  signal_id NVARCHAR(64) NOT NULL, "
        "  sensor_id UNIQUEIDENTIFIER NOT NULL, "
        "  event_time DATETIME2 NOT NULL, "
        "  event_type SMALLINT NOT NULL, "
        "  mph INT NULL, "
        "  kph INT NULL, "
        "  length_feet INT NULL, "
        "  vehicle_class SMALLINT NULL, "
        "  lane_number SMALLINT NULL, "
        "  direction_id SMALLINT NULL, "
        "  occupancy_pct DECIMAL(5,2) NULL, "
        "  queue_length_feet DECIMAL(8,2) NULL, "
        "  vendor_metadata NVARCHAR(MAX) NULL, "
        "  CONSTRAINT pk_roadside_event PRIMARY KEY CLUSTERED "
        "    (event_time, signal_id, sensor_id, event_type) "
        "    ON ps_roadside_event_event_time(event_time) "
        ")"
    )
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes "
        "WHERE name = 'idx_re_sensor_time' "
        "  AND object_id = OBJECT_ID('roadside_event')) "
        "CREATE INDEX idx_re_sensor_time "
        "ON roadside_event (sensor_id, event_time DESC) "
        "ON ps_roadside_event_event_time(event_time)"
    )
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes "
        "WHERE name = 'idx_re_signal_time' "
        "  AND object_id = OBJECT_ID('roadside_event')) "
        "CREATE INDEX idx_re_signal_time "
        "ON roadside_event (signal_id, event_time DESC) "
        "ON ps_roadside_event_event_time(event_time)"
    )
    op.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes "
        "WHERE name = 'idx_re_event_type_time' "
        "  AND object_id = OBJECT_ID('roadside_event')) "
        "CREATE INDEX idx_re_event_type_time "
        "ON roadside_event (event_type, event_time DESC) "
        "ON ps_roadside_event_event_time(event_time)"
    )
    logger.info(
        "roadside_event partitioned on ps_roadside_event_event_time "
        "(partition cadence: %d day(s))", chunk_days,
    )


def _create_roadside_event_oracle(chunk_days: int) -> None:
    """Oracle: INTERVAL partitioning — DB auto-creates partitions."""
    insp = sa.inspect(op.get_bind())
    if not insp.has_table("roadside_event"):
        op.execute(
            "CREATE TABLE roadside_event ("
            "  signal_id VARCHAR2(64) NOT NULL, "
            "  sensor_id RAW(16) NOT NULL, "
            "  event_time TIMESTAMP WITH TIME ZONE NOT NULL, "
            "  event_type NUMBER(5) NOT NULL, "
            "  mph INTEGER NULL, "
            "  kph INTEGER NULL, "
            "  length_feet INTEGER NULL, "
            "  vehicle_class NUMBER(5) NULL, "
            "  lane_number NUMBER(5) NULL, "
            "  direction_id NUMBER(5) NULL, "
            "  occupancy_pct NUMBER(5,2) NULL, "
            "  queue_length_feet NUMBER(8,2) NULL, "
            "  vendor_metadata CLOB NULL, "
            "  CONSTRAINT pk_roadside_event PRIMARY KEY "
            "    (signal_id, sensor_id, event_time, event_type) "
            ") "
            "PARTITION BY RANGE (event_time) "
            f"INTERVAL (NUMTODSINTERVAL({chunk_days}, 'DAY')) "
            "(PARTITION p_initial VALUES LESS THAN "
            f" (TIMESTAMP '{_EVENT_LOG_INITIAL_ANCHOR}'))"
        )
    existing = _existing_index_names("roadside_event")
    if "idx_re_sensor_time" not in existing:
        op.create_index(
            "idx_re_sensor_time",
            "roadside_event",
            [sa.text("sensor_id"), sa.text("event_time DESC")],
        )
    if "idx_re_signal_time" not in existing:
        op.create_index(
            "idx_re_signal_time",
            "roadside_event",
            [sa.text("signal_id"), sa.text("event_time DESC")],
        )
    if "idx_re_event_type_time" not in existing:
        op.create_index(
            "idx_re_event_type_time",
            "roadside_event",
            [sa.text("event_type"), sa.text("event_time DESC")],
        )


def _create_roadside_event_mysql(chunk_days: int) -> None:
    """MySQL: RANGE partitioning by UNIX_TIMESTAMP(event_time)."""
    op.execute(
        "CREATE TABLE IF NOT EXISTS roadside_event ("
        "  signal_id VARCHAR(64) NOT NULL, "
        "  sensor_id CHAR(36) NOT NULL, "
        "  event_time DATETIME(6) NOT NULL, "
        "  event_type SMALLINT NOT NULL, "
        "  mph INT NULL, "
        "  kph INT NULL, "
        "  length_feet INT NULL, "
        "  vehicle_class SMALLINT NULL, "
        "  lane_number SMALLINT NULL, "
        "  direction_id SMALLINT NULL, "
        "  occupancy_pct DECIMAL(5,2) NULL, "
        "  queue_length_feet DECIMAL(8,2) NULL, "
        "  vendor_metadata JSON NULL, "
        "  PRIMARY KEY (signal_id, sensor_id, event_time, event_type) "
        ") ENGINE=InnoDB "
        "PARTITION BY RANGE (UNIX_TIMESTAMP(event_time)) ("
        "  PARTITION p_initial VALUES LESS THAN "
        f" (UNIX_TIMESTAMP('{_EVENT_LOG_INITIAL_ANCHOR}'))"
        ")"
    )
    existing = _existing_index_names("roadside_event")
    if "idx_re_sensor_time" not in existing:
        op.create_index(
            "idx_re_sensor_time",
            "roadside_event",
            [sa.text("sensor_id"), sa.text("event_time DESC")],
        )
    if "idx_re_signal_time" not in existing:
        op.create_index(
            "idx_re_signal_time",
            "roadside_event",
            [sa.text("signal_id"), sa.text("event_time DESC")],
        )
    if "idx_re_event_type_time" not in existing:
        op.create_index(
            "idx_re_event_type_time",
            "roadside_event",
            [sa.text("event_type"), sa.text("event_time DESC")],
        )
    logger.info(
        "roadside_event partitioned by UNIX_TIMESTAMP(event_time) "
        "(partition cadence: %d day(s))", chunk_days,
    )


# ---------------------------------------------------------------------------
# Roadside sensor configuration tables
# ---------------------------------------------------------------------------


def _create_roadside_sensor_tables() -> None:
    """Create the 6 roadside-sensor config tables idempotently.

    Order:
      1. ``roadside_sensor_vendor``         (reference)
      2. ``roadside_sensor_model``          (reference, FK → vendor)
      3. ``roadside_sensor``                (FK → model + signal)
      4. ``roadside_sensor_lane``           (FK → sensor + approach)
      5. ``roadside_sensor_audit``          (JSONB change history)
      6. ``roadside_sensor_lane_audit``     (JSONB change history)

    Each step guarded with ``inspect(bind).has_table(...)``; each index
    guarded with ``_existing_index_names(...)``.  Re-running after a
    partial failure resumes from where it stopped.
    """
    insp = sa.inspect(op.get_bind())

    if not insp.has_table("roadside_sensor_vendor"):
        op.create_table(
            "roadside_sensor_vendor",
            sa.Column(
                "vendor_id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column("name", sa.Text, nullable=False),
        )

    if not insp.has_table("roadside_sensor_model"):
        op.create_table(
            "roadside_sensor_model",
            sa.Column(
                "model_id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column(
                "vendor_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("roadside_sensor_vendor.vendor_id"),
                nullable=False,
            ),
            sa.Column("name", sa.Text, nullable=False),
            sa.Column("sensor_type", sa.Text, nullable=False),
            sa.Column("default_protocol", sa.Text, nullable=True),
        )

    if not insp.has_table("roadside_sensor"):
        op.create_table(
            "roadside_sensor",
            sa.Column(
                "sensor_id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column(
                "model_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("roadside_sensor_model.model_id"),
                nullable=False,
            ),
            sa.Column(
                "signal_id",
                sa.Text,
                sa.ForeignKey("signal.signal_id"),
                nullable=False,
            ),
            sa.Column("device_name", sa.Text, nullable=False),
            sa.Column("serial_number", sa.Text, nullable=True),
            sa.Column("firmware_version", sa.Text, nullable=True),
            sa.Column(
                "install_date", postgresql.TIMESTAMP(timezone=True), nullable=True,
            ),
            sa.Column("latitude", sa.Numeric, nullable=True),
            sa.Column("longitude", sa.Numeric, nullable=True),
            sa.Column("mounting_height_feet", sa.Numeric, nullable=True),
            sa.Column("azimuth_degrees", sa.Numeric, nullable=True),
            sa.Column("ip_address", postgresql.INET, nullable=True),
            sa.Column("port", sa.Integer, nullable=True),
            sa.Column("protocol", sa.Text, nullable=True),
            sa.Column("username", sa.Text, nullable=True),
            sa.Column("password", sa.Text, nullable=True),
            sa.Column(
                "emits_speed", sa.Boolean, nullable=False, server_default="false",
            ),
            sa.Column(
                "emits_classification", sa.Boolean, nullable=False,
                server_default="false",
            ),
            sa.Column(
                "emits_queue_length", sa.Boolean, nullable=False,
                server_default="false",
            ),
            sa.Column(
                "emits_occupancy", sa.Boolean, nullable=False,
                server_default="false",
            ),
            sa.Column(
                "is_active", sa.Boolean, nullable=False, server_default="true",
            ),
            sa.Column(
                "last_seen_at", postgresql.TIMESTAMP(timezone=True), nullable=True,
            ),
            sa.Column("metadata", postgresql.JSONB, nullable=True),
            sa.Column(
                "created_at", postgresql.TIMESTAMP(timezone=True), nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column(
                "updated_at", postgresql.TIMESTAMP(timezone=True), nullable=False,
                server_default=sa.func.now(),
            ),
        )
    existing = _existing_index_names("roadside_sensor")
    if "idx_roadside_sensor_signal" not in existing:
        op.create_index("idx_roadside_sensor_signal", "roadside_sensor", ["signal_id"])
    if "idx_roadside_sensor_model" not in existing:
        op.create_index("idx_roadside_sensor_model", "roadside_sensor", ["model_id"])
    if "idx_roadside_sensor_active" not in existing:
        op.create_index("idx_roadside_sensor_active", "roadside_sensor", ["is_active"])

    if not insp.has_table("roadside_sensor_lane"):
        op.create_table(
            "roadside_sensor_lane",
            sa.Column(
                "zone_id",
                postgresql.UUID(as_uuid=True),
                primary_key=True,
                server_default=sa.text("gen_random_uuid()"),
            ),
            sa.Column(
                "sensor_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("roadside_sensor.sensor_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "approach_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("approach.approach_id"),
                nullable=False,
            ),
            sa.Column("lane_number", sa.SmallInteger, nullable=False),
            sa.Column("vendor_lane_id", sa.Text, nullable=False),
            sa.Column(
                "created_at", postgresql.TIMESTAMP(timezone=True), nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column(
                "updated_at", postgresql.TIMESTAMP(timezone=True), nullable=False,
                server_default=sa.func.now(),
            ),
        )
    existing = _existing_index_names("roadside_sensor_lane")
    if "idx_roadside_sensor_lane_sensor" not in existing:
        op.create_index(
            "idx_roadside_sensor_lane_sensor", "roadside_sensor_lane", ["sensor_id"],
        )
    if "idx_roadside_sensor_lane_approach" not in existing:
        op.create_index(
            "idx_roadside_sensor_lane_approach", "roadside_sensor_lane", ["approach_id"],
        )

    if not insp.has_table("roadside_sensor_audit"):
        op.create_table(
            "roadside_sensor_audit",
            sa.Column(
                "audit_id", sa.BigInteger, primary_key=True, autoincrement=True,
            ),
            sa.Column(
                "sensor_id", postgresql.UUID(as_uuid=True), nullable=False,
            ),
            sa.Column("signal_id", sa.Text, nullable=False),
            sa.Column(
                "changed_at", postgresql.TIMESTAMP(timezone=True), nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column("changed_by", sa.Text, nullable=True),
            sa.Column("operation", sa.Text, nullable=False),
            sa.Column("old_values", postgresql.JSONB, nullable=True),
            sa.Column("new_values", postgresql.JSONB, nullable=True),
        )
    existing = _existing_index_names("roadside_sensor_audit")
    if "idx_roadside_sensor_audit_sensor" not in existing:
        op.create_index(
            "idx_roadside_sensor_audit_sensor", "roadside_sensor_audit",
            [sa.text("sensor_id"), sa.text("changed_at DESC")],
        )
    if "idx_roadside_sensor_audit_signal" not in existing:
        op.create_index(
            "idx_roadside_sensor_audit_signal", "roadside_sensor_audit",
            [sa.text("signal_id"), sa.text("changed_at DESC")],
        )
    if "idx_roadside_sensor_audit_time" not in existing:
        op.create_index(
            "idx_roadside_sensor_audit_time", "roadside_sensor_audit",
            [sa.text("changed_at DESC")],
        )

    if not insp.has_table("roadside_sensor_lane_audit"):
        op.create_table(
            "roadside_sensor_lane_audit",
            sa.Column(
                "audit_id", sa.BigInteger, primary_key=True, autoincrement=True,
            ),
            sa.Column("zone_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("sensor_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("approach_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column(
                "changed_at", postgresql.TIMESTAMP(timezone=True), nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column("changed_by", sa.Text, nullable=True),
            sa.Column("operation", sa.Text, nullable=False),
            sa.Column("old_values", postgresql.JSONB, nullable=True),
            sa.Column("new_values", postgresql.JSONB, nullable=True),
        )
    existing = _existing_index_names("roadside_sensor_lane_audit")
    if "idx_roadside_sensor_lane_audit_zone" not in existing:
        op.create_index(
            "idx_roadside_sensor_lane_audit_zone", "roadside_sensor_lane_audit",
            [sa.text("zone_id"), sa.text("changed_at DESC")],
        )
    if "idx_roadside_sensor_lane_audit_sensor" not in existing:
        op.create_index(
            "idx_roadside_sensor_lane_audit_sensor", "roadside_sensor_lane_audit",
            [sa.text("sensor_id"), sa.text("changed_at DESC")],
        )
    if "idx_roadside_sensor_lane_audit_time" not in existing:
        op.create_index(
            "idx_roadside_sensor_lane_audit_time", "roadside_sensor_lane_audit",
            [sa.text("changed_at DESC")],
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
