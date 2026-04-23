"""
Historical configuration resolver.

Reconstructs signal/approach/detector configuration as it existed at a
given point in time by querying the audit snapshot tables.  When the
report date range is recent (i.e. no config changes have occurred since
the report start date), the resolver returns live table data for speed.

Usage in reports::

    from tsigma.config_resolver import get_config_at

    config = await get_config_at(session, signal_id, as_of=report_start)
    det_channels = config.detector_channels_for_phase(phase_number)

This replaces the pattern of querying live Approach/Detector tables
directly, which produces wrong results when config has changed since
the report period.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Approach, Detector, SignalAudit
from .models.audit import ApproachAudit, DetectorAudit
from .models.reference import MovementType

logger = logging.getLogger(__name__)


@dataclass
class ApproachSnapshot:
    """Point-in-time approach configuration."""

    approach_id: str
    signal_id: str
    direction_type_id: int
    protected_phase_number: int | None
    permissive_phase_number: int | None
    is_protected_phase_overlap: bool
    is_permissive_phase_overlap: bool
    ped_phase_number: int | None
    mph: int | None
    description: str | None

    @classmethod
    def from_orm(cls, a: Any, *, approach_id: str | None = None) -> "ApproachSnapshot":
        """Build from a live Approach ORM object."""
        return cls(
            approach_id=approach_id or str(a.approach_id),
            signal_id=a.signal_id,
            direction_type_id=a.direction_type_id,
            protected_phase_number=a.protected_phase_number,
            permissive_phase_number=a.permissive_phase_number,
            is_protected_phase_overlap=a.is_protected_phase_overlap,
            is_permissive_phase_overlap=a.is_permissive_phase_overlap,
            ped_phase_number=a.ped_phase_number,
            mph=a.mph,
            description=a.description,
        )

    @classmethod
    def from_audit(
        cls, approach_id: str, vals: dict[str, Any], signal_id: str,
    ) -> "ApproachSnapshot":
        """Build from an audit JSONB snapshot (new_values dict)."""
        return cls(
            approach_id=approach_id,
            signal_id=vals.get("signal_id", signal_id),
            direction_type_id=vals.get("direction_type_id", 0),
            protected_phase_number=vals.get("protected_phase_number"),
            permissive_phase_number=vals.get("permissive_phase_number"),
            is_protected_phase_overlap=vals.get("is_protected_phase_overlap", False),
            is_permissive_phase_overlap=vals.get("is_permissive_phase_overlap", False),
            ped_phase_number=vals.get("ped_phase_number"),
            mph=vals.get("mph"),
            description=vals.get("description"),
        )


@dataclass
class DetectorSnapshot:
    """Point-in-time detector configuration."""

    detector_id: str
    approach_id: str
    detector_channel: int
    distance_from_stop_bar: int | None
    min_speed_filter: int | None
    lane_number: int | None
    movement_type_id: str | None = None
    movement_type_code: str | None = None

    @classmethod
    def from_orm(
        cls,
        d: Any,
        *,
        detector_id: str | None = None,
        movement_type_code: str | None = None,
    ) -> "DetectorSnapshot":
        """Build from a live Detector ORM object.

        ``movement_type_code`` is optional — the config resolver resolves
        it via the MovementType reference table before constructing the
        snapshot. Reports should not look it up themselves.
        """
        return cls(
            detector_id=detector_id or str(d.detector_id),
            approach_id=str(d.approach_id),
            detector_channel=d.detector_channel,
            distance_from_stop_bar=d.distance_from_stop_bar,
            min_speed_filter=d.min_speed_filter,
            lane_number=d.lane_number,
            movement_type_id=str(d.movement_type_id) if d.movement_type_id else None,
            movement_type_code=movement_type_code,
        )

    @classmethod
    def from_audit(
        cls,
        detector_id: str,
        approach_id: str,
        vals: dict[str, Any],
        *,
        movement_type_code: str | None = None,
    ) -> "DetectorSnapshot":
        """Build from an audit JSONB snapshot (new_values dict)."""
        movement_type_id = vals.get("movement_type_id")
        return cls(
            detector_id=detector_id,
            approach_id=approach_id,
            detector_channel=vals.get("detector_channel", 0),
            distance_from_stop_bar=vals.get("distance_from_stop_bar"),
            min_speed_filter=vals.get("min_speed_filter"),
            lane_number=vals.get("lane_number"),
            movement_type_id=str(movement_type_id) if movement_type_id else None,
            movement_type_code=movement_type_code,
        )


@dataclass
class SignalConfig:
    """
    Complete signal configuration at a point in time.

    Includes all approaches and detectors that were active at ``as_of``.
    """

    signal_id: str
    as_of: datetime
    from_audit: bool  # True if reconstructed from audit snapshots
    approaches: list[ApproachSnapshot] = field(default_factory=list)
    detectors: list[DetectorSnapshot] = field(default_factory=list)

    def detector_channels_for_phase(self, phase_number: int) -> set[int]:
        """
        Get detector channels assigned to a phase at this point in time.

        Checks both protected and permissive phase assignments.
        """
        approach_ids = {
            a.approach_id
            for a in self.approaches
            if a.protected_phase_number == phase_number
            or a.permissive_phase_number == phase_number
        }
        return {
            d.detector_channel
            for d in self.detectors
            if d.approach_id in approach_ids
        }

    def ped_phase_for_approach(self, approach_id: str) -> int | None:
        """Get pedestrian phase number for an approach."""
        for a in self.approaches:
            if a.approach_id == approach_id:
                return a.ped_phase_number
        return None

    def approaches_for_signal(self) -> list[ApproachSnapshot]:
        """All approaches for this signal."""
        return self.approaches

    def detectors_for_approach(self, approach_id: str) -> list[DetectorSnapshot]:
        """All detectors for a specific approach."""
        return [d for d in self.detectors if d.approach_id == approach_id]


async def get_config_at(
    session: AsyncSession,
    signal_id: str,
    as_of: datetime,
) -> SignalConfig:
    """
    Resolve signal configuration as it existed at ``as_of``.

    Strategy:
        1. Check if any config changes occurred after ``as_of`` by looking
           at the signal_audit table.
        2. If no changes since ``as_of``, return live config (fast path).
        3. If changes exist, reconstruct from the most recent audit snapshot
           before ``as_of`` (slow path).

    Args:
        session: Database session.
        signal_id: Signal identifier.
        as_of: Point in time to resolve config for.

    Returns:
        SignalConfig with approaches and detectors as of that date.
    """
    # Check if any signal config changed after as_of
    has_changes = await _has_changes_after(session, signal_id, as_of)

    if not has_changes:
        # Fast path: live config is valid for this date
        return await _load_live_config(session, signal_id, as_of)

    # Slow path: reconstruct from audit snapshots
    logger.info(
        "Reconstructing config for %s as of %s from audit snapshots",
        signal_id, as_of,
    )
    return await _load_audit_config(session, signal_id, as_of)


async def _has_changes_after(
    session: AsyncSession,
    signal_id: str,
    as_of: datetime,
) -> bool:
    """Check if signal, approach, or detector config was modified after as_of."""
    # Signal-level changes
    sig_result = await session.execute(
        select(SignalAudit.audit_id)
        .where(SignalAudit.signal_id == signal_id, SignalAudit.changed_at > as_of)
        .limit(1)
    )
    if sig_result.scalar() is not None:
        return True

    # Approach-level changes
    app_result = await session.execute(
        select(ApproachAudit.audit_id)
        .where(ApproachAudit.signal_id == signal_id, ApproachAudit.changed_at > as_of)
        .limit(1)
    )
    if app_result.scalar() is not None:
        return True

    # Detector-level changes (join through approach to filter by signal)
    det_result = await session.execute(
        select(DetectorAudit.audit_id)
        .where(
            DetectorAudit.approach_id.in_(
                select(Approach.approach_id).where(Approach.signal_id == signal_id)
            ),
            DetectorAudit.changed_at > as_of,
        )
        .limit(1)
    )
    return det_result.scalar() is not None


async def _load_movement_type_map(session: AsyncSession) -> dict[str, str]:
    """Return a UUID-string → abbreviation map for all movement types.

    Movement types are a small reference table that changes rarely, so
    querying it per config resolution is cheap. Reports filter detectors
    by the abbreviation ("L", "T", "TR", "TL", "R", "U", "P", ...).
    """
    result = await session.execute(
        select(MovementType.movement_type_id, MovementType.abbreviation)
    )
    return {
        str(mt_id): abbrev
        for mt_id, abbrev in result.all()
        if abbrev is not None
    }


async def _load_live_config(
    session: AsyncSession,
    signal_id: str,
    as_of: datetime,
) -> SignalConfig:
    """Load current config from live tables (fast path)."""
    # Load approaches
    approach_result = await session.execute(
        select(Approach).where(Approach.signal_id == signal_id)
    )
    approaches = approach_result.scalars().all()

    approach_snapshots = [ApproachSnapshot.from_orm(a) for a in approaches]

    # Load detectors for these approaches
    approach_ids = [a.approach_id for a in approaches]
    detector_snapshots = []
    if approach_ids:
        movement_map = await _load_movement_type_map(session)
        det_result = await session.execute(
            select(Detector).where(Detector.approach_id.in_(approach_ids))
        )
        detectors = det_result.scalars().all()
        detector_snapshots = [
            DetectorSnapshot.from_orm(
                d,
                movement_type_code=movement_map.get(str(d.movement_type_id))
                if d.movement_type_id else None,
            )
            for d in detectors
        ]

    return SignalConfig(
        signal_id=signal_id,
        as_of=as_of,
        from_audit=False,
        approaches=approach_snapshots,
        detectors=detector_snapshots,
    )


async def _load_audit_config(
    session: AsyncSession,
    signal_id: str,
    as_of: datetime,
) -> SignalConfig:
    """
    Reconstruct config from audit snapshots (slow path).

    For each approach that existed at ``as_of``, finds the most recent
    approach_audit snapshot.  Same for detectors via detector_audit.
    Falls back to live config for any entity that has no audit history
    predating ``as_of`` (i.e. was created before auditing was enabled).
    """
    # --- Approaches ---
    # Step 1: Get all approach_audit rows for this signal at or before as_of.
    # For each approach_id, we want the latest INSERT or UPDATE snapshot.
    approach_snapshots = await _reconstruct_approaches(session, signal_id, as_of)

    # --- Detectors ---
    # Step 2: For each reconstructed approach, get detector snapshots.
    approach_ids = [a.approach_id for a in approach_snapshots]
    movement_map = await _load_movement_type_map(session)
    detector_snapshots = await _reconstruct_detectors(
        session, approach_ids, as_of, movement_map=movement_map,
    )

    return SignalConfig(
        signal_id=signal_id,
        as_of=as_of,
        from_audit=True,
        approaches=approach_snapshots,
        detectors=detector_snapshots,
    )


async def _reconstruct_approaches(
    session: AsyncSession,
    signal_id: str,
    as_of: datetime,
) -> list[ApproachSnapshot]:
    """
    Reconstruct approach config from approach_audit snapshots.

    For each distinct approach_id, finds the most recent audit row
    at or before ``as_of``.  If the most recent operation is DELETE,
    the approach did not exist at ``as_of`` and is excluded.

    Falls back to live approach data for approaches with no audit
    history (created before auditing was enabled).
    """
    # Get the most recent audit row per approach_id at or before as_of
    # Using a lateral join / window function approach:
    # SELECT DISTINCT ON (approach_id) ... ORDER BY changed_at DESC
    result = await session.execute(
        select(ApproachAudit)
        .where(
            ApproachAudit.signal_id == signal_id,
            ApproachAudit.changed_at <= as_of,
        )
        .order_by(ApproachAudit.approach_id, ApproachAudit.changed_at.desc())
    )
    audit_rows = result.scalars().all()

    # Deduplicate: keep only the most recent row per approach_id
    seen: dict[str, Any] = {}
    for row in audit_rows:
        aid = str(row.approach_id)
        if aid not in seen:
            seen[aid] = row

    snapshots = []
    audited_ids: set[str] = set()

    for aid, row in seen.items():
        # If the most recent operation is DELETE, this approach
        # did not exist at as_of — skip it.
        if row.operation == "DELETE":
            audited_ids.add(aid)
            continue

        # Reconstruct from JSONB snapshot
        vals = row.new_values or {}
        snapshots.append(ApproachSnapshot.from_audit(aid, vals, signal_id))
        audited_ids.add(aid)

    # Fall back to live data for approaches with no audit history
    live_result = await session.execute(
        select(Approach).where(Approach.signal_id == signal_id)
    )
    for a in live_result.scalars().all():
        aid = str(a.approach_id)
        if aid not in audited_ids:
            logger.debug(
                "Approach %s has no audit history before %s — using live data",
                aid, as_of,
            )
            snapshots.append(ApproachSnapshot.from_orm(a, approach_id=aid))

    return snapshots


async def _reconstruct_detectors(
    session: AsyncSession,
    approach_ids: list[str],
    as_of: datetime,
    *,
    movement_map: dict[str, str] | None = None,
) -> list[DetectorSnapshot]:
    """
    Reconstruct detector config from detector_audit snapshots.

    Same pattern as _reconstruct_approaches: latest audit row per
    detector_id, exclude DELETEs, fall back to live for unaudited.

    ``movement_map`` (UUID-string → abbreviation, from
    ``_load_movement_type_map``) is injected by the caller to avoid a
    redundant query in the normal code path. Defaults to None (equivalent
    to empty map, all detectors lack movement codes).
    """
    if not approach_ids:
        return []

    # Convert string IDs for the query
    from uuid import UUID
    uuid_ids = [UUID(aid) if isinstance(aid, str) else aid for aid in approach_ids]

    if movement_map is None:
        movement_map = {}

    result = await session.execute(
        select(DetectorAudit)
        .where(
            DetectorAudit.approach_id.in_(uuid_ids),
            DetectorAudit.changed_at <= as_of,
        )
        .order_by(DetectorAudit.detector_id, DetectorAudit.changed_at.desc())
    )
    audit_rows = result.scalars().all()

    # Deduplicate: keep only the most recent row per detector_id
    seen: dict[str, Any] = {}
    for row in audit_rows:
        did = str(row.detector_id)
        if did not in seen:
            seen[did] = row

    snapshots = []
    audited_ids: set[str] = set()

    for did, row in seen.items():
        if row.operation == "DELETE":
            audited_ids.add(did)
            continue

        vals = row.new_values or {}
        mt_id = vals.get("movement_type_id")
        snapshots.append(
            DetectorSnapshot.from_audit(
                did, str(row.approach_id), vals,
                movement_type_code=movement_map.get(str(mt_id)) if mt_id else None,
            )
        )
        audited_ids.add(did)

    # Fall back to live data for detectors with no audit history
    live_result = await session.execute(
        select(Detector).where(Detector.approach_id.in_(uuid_ids))
    )
    for d in live_result.scalars().all():
        did = str(d.detector_id)
        if did not in audited_ids:
            logger.debug(
                "Detector %s has no audit history before %s — using live data",
                did, as_of,
            )
            snapshots.append(
                DetectorSnapshot.from_orm(
                    d,
                    detector_id=did,
                    movement_type_code=movement_map.get(str(d.movement_type_id))
                    if d.movement_type_id else None,
                )
            )

    return snapshots
