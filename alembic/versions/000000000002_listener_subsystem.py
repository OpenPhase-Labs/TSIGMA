"""Listener subsystem foundation — schema convergence (c).

Adds first-class network-triple columns to ``signal`` (``port``,
``protocol``) so the source-IP / port / transport fields no longer live
in ``signal_metadata.collection`` JSONB.  Also adds the partial B-tree
indexes on ``signal.ip_address`` and ``roadside_sensor.ip_address`` that
TCP/UDP listeners use to resolve inbound packet source IPs to a device
through the ``DeviceSource`` abstraction.

The JSONB ``collection.port`` / ``collection.protocol`` values are
backfilled into the new columns when present. The JSONB keys are left in
place (not stripped) so a still-running pre-cutover version that reads
``metadata.collection.port`` keeps working — blue/green safe. A later
forward migration can drop them once no old version depends on them.

Revision ID: 000000000002
Revises: 000000000001
Create Date: 2026-04-30
"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "000000000002"
down_revision: Union[str, None] = "000000000001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from tsigma.config import settings
    from tsigma.database.db import DialectHelper

    cfg = DialectHelper(settings.db_type).schema("config")
    insp = sa.inspect(op.get_bind())

    # ------------------------------------------------------------------
    # Signal: add first-class port + protocol columns (idempotent).
    # ------------------------------------------------------------------
    signal_cols = {c["name"] for c in insp.get_columns("signal", schema=cfg)}
    if "port" not in signal_cols:
        op.add_column("signal", sa.Column("port", sa.Integer, nullable=True))
    if "protocol" not in signal_cols:
        op.add_column("signal", sa.Column("protocol", sa.Text, nullable=True))

    # Backfill from existing JSONB.  Casts the JSON string to int for
    # port; protocol is already text.  Rows without those keys keep NULL.
    op.execute(sa.text("""
        UPDATE signal
        SET port = (metadata->'collection'->>'port')::INTEGER
        WHERE metadata IS NOT NULL
          AND metadata->'collection'->>'port' IS NOT NULL
          AND metadata->'collection'->>'port' ~ '^[0-9]+$'
    """))
    op.execute(sa.text("""
        UPDATE signal
        SET protocol = metadata->'collection'->>'protocol'
        WHERE metadata IS NOT NULL
          AND metadata->'collection'->>'protocol' IS NOT NULL
    """))

    # NOTE: the migrated JSONB keys are intentionally NOT stripped here.
    # Removing them pre-cutover would break a still-running old version that
    # reads metadata.collection.port/protocol (the blue/green hazard the
    # additive-only rule exists to prevent). A later forward migration can
    # drop them once no old version reads them.

    # ------------------------------------------------------------------
    # Partial B-tree indexes for source-IP listener lookups (idempotent).
    # ------------------------------------------------------------------
    if "idx_signal_ip_address" not in {
        ix["name"] for ix in insp.get_indexes("signal", schema=cfg)
    }:
        op.create_index(
            "idx_signal_ip_address",
            "signal",
            ["ip_address"],
            postgresql_where=sa.text("ip_address IS NOT NULL"),
        )
    if "idx_roadside_sensor_ip_address" not in {
        ix["name"] for ix in insp.get_indexes("roadside_sensor", schema=cfg)
    }:
        op.create_index(
            "idx_roadside_sensor_ip_address",
            "roadside_sensor",
            ["ip_address"],
            postgresql_where=sa.text("ip_address IS NOT NULL"),
        )


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
