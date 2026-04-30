"""Listener subsystem foundation — schema convergence (c).

Adds first-class network-triple columns to ``signal`` (``port``,
``protocol``) so the source-IP / port / transport fields no longer live
in ``signal_metadata.collection`` JSONB.  Also adds the partial B-tree
indexes on ``signal.ip_address`` and ``roadside_sensor.ip_address`` that
TCP/UDP listeners use to resolve inbound packet source IPs to a device
through the ``DeviceSource`` abstraction.

The JSONB ``collection.port`` / ``collection.protocol`` values are
backfilled into the new columns when present.  Stale keys are removed
from JSONB so the new column is the only source of truth.

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
    # ------------------------------------------------------------------
    # Signal: add first-class port + protocol columns.
    # ------------------------------------------------------------------
    op.add_column(
        "signal",
        sa.Column("port", sa.Integer, nullable=True),
    )
    op.add_column(
        "signal",
        sa.Column("protocol", sa.Text, nullable=True),
    )

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

    # Strip the migrated keys out of JSONB so the new columns are the
    # only source of truth.
    op.execute(sa.text("""
        UPDATE signal
        SET metadata = jsonb_set(
            metadata,
            '{collection}',
            (metadata->'collection') - 'port' - 'protocol',
            false
        )
        WHERE metadata IS NOT NULL
          AND metadata ? 'collection'
          AND (metadata->'collection' ? 'port'
               OR metadata->'collection' ? 'protocol')
    """))

    # ------------------------------------------------------------------
    # Partial B-tree indexes for source-IP listener lookups.
    # ------------------------------------------------------------------
    op.create_index(
        "idx_signal_ip_address",
        "signal",
        ["ip_address"],
        postgresql_where=sa.text("ip_address IS NOT NULL"),
    )
    op.create_index(
        "idx_roadside_sensor_ip_address",
        "roadside_sensor",
        ["ip_address"],
        postgresql_where=sa.text("ip_address IS NOT NULL"),
    )


def downgrade() -> None:
    # Drop indexes first (they depend on the columns).
    op.drop_index("idx_roadside_sensor_ip_address", table_name="roadside_sensor")
    op.drop_index("idx_signal_ip_address", table_name="signal")

    # Restore JSONB before dropping columns so a re-upgrade is lossless.
    op.execute(sa.text("""
        UPDATE signal
        SET metadata = jsonb_set(
            COALESCE(metadata, '{}'::jsonb),
            '{collection,port}',
            to_jsonb(port::text),
            true
        )
        WHERE port IS NOT NULL
    """))
    op.execute(sa.text("""
        UPDATE signal
        SET metadata = jsonb_set(
            COALESCE(metadata, '{}'::jsonb),
            '{collection,protocol}',
            to_jsonb(protocol),
            true
        )
        WHERE protocol IS NOT NULL
    """))

    op.drop_column("signal", "protocol")
    op.drop_column("signal", "port")
