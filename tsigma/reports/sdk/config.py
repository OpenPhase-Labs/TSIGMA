"""
Historical-config lookup helpers.

Thin wrappers around `tsigma.config_resolver.get_config_at` that
produce the detector-channel mappings reports need most often.
"""

from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ...config_resolver import get_config_at


async def load_channel_to_phase(
    session: AsyncSession, signal_id: str, as_of: datetime
) -> dict[int, int]:
    """
    Detector channel → protected phase number (from historical config).

    Only approaches with a protected phase are included.
    """
    config = await get_config_at(session, signal_id, as_of=as_of)
    mapping: dict[int, int] = {}
    for approach in config.approaches:
        if approach.protected_phase_number is None:
            continue
        for det in config.detectors_for_approach(approach.approach_id):
            mapping[det.detector_channel] = approach.protected_phase_number
    return mapping


async def load_channels_for_phase(
    session: AsyncSession, signal_id: str, phase: int, as_of: datetime
) -> set[int]:
    """
    Detector channels assigned to a specific phase (from historical config).

    Convenience wrapper — many reports need exactly this before calling
    ``fetch_events_split``.
    """
    config = await get_config_at(session, signal_id, as_of=as_of)
    return config.detector_channels_for_phase(phase)


async def load_channel_to_ped_phase(
    session: AsyncSession, signal_id: str, as_of: datetime
) -> dict[int, int]:
    """
    Detector channel → pedestrian phase number (from historical config).

    Only approaches that have a ped phase AND detectors are included.
    """
    config = await get_config_at(session, signal_id, as_of=as_of)
    mapping: dict[int, int] = {}
    for approach in config.approaches:
        ped_phase = config.ped_phase_for_approach(approach.approach_id)
        if ped_phase is None:
            continue
        for det in config.detectors_for_approach(approach.approach_id):
            mapping[det.detector_channel] = ped_phase
    return mapping


async def load_channel_to_approach(
    session: AsyncSession, signal_id: str, as_of: datetime
) -> dict[int, dict[str, Any]]:
    """
    Detector channel → approach info dict (from historical config).

    Each value is `{"approach_id", "direction_type_id", "distance_from_stop_bar"}`.
    Only keys that exist on the detector are populated.
    """
    config = await get_config_at(session, signal_id, as_of=as_of)
    mapping: dict[int, dict[str, Any]] = {}
    for approach in config.approaches:
        for det in config.detectors_for_approach(approach.approach_id):
            info: dict[str, Any] = {
                "approach_id": approach.approach_id,
                "direction_type_id": approach.direction_type_id,
            }
            distance = getattr(det, "distance_from_stop_bar", None)
            if distance is not None:
                info["distance_from_stop_bar"] = distance
            mapping[det.detector_channel] = info
    return mapping
