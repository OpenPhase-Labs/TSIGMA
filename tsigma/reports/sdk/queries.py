"""
Standard ControllerEventLog query helpers.

Every report needs events in a time window filtered by event code
(and sometimes by channel).  Centralising the query here means report
authors never hand-roll a SQL filter again.

All helpers return pandas DataFrames via db_facade.get_dataframe().
"""

from collections.abc import Iterable
from datetime import datetime

import pandas as pd
from sqlalchemy import and_, or_, select

from ...database.db import db_facade
from ...models.event import ControllerEventLog as CEL
from .events import EVENT_DETECTOR_ON


async def fetch_events(
    signal_id: str,
    start: datetime,
    end: datetime,
    event_codes: Iterable[int],
    *,
    event_param_in: Iterable[int] | None = None,
) -> pd.DataFrame:
    """
    Fetch ControllerEventLog rows for a signal in a time window.

    Args:
        signal_id: Signal ID to query.
        start: Window start (inclusive).
        end: Window end (inclusive).
        event_codes: Event codes to include.
        event_param_in: Optional additional filter on ``event_param``
            (e.g. restrict to a set of detector channels).

    Returns:
        DataFrame with columns: event_code, event_param, event_time.
        Ordered by event_time.
    """
    conditions = [
        CEL.signal_id == signal_id,
        CEL.event_time >= start,
        CEL.event_time <= end,
        CEL.event_code.in_(list(event_codes)),
    ]
    if event_param_in is not None:
        conditions.append(CEL.event_param.in_(list(event_param_in)))

    stmt = (
        select(CEL.event_code, CEL.event_param, CEL.event_time)
        .where(and_(*conditions))
        .order_by(CEL.event_time)
    )
    return await db_facade.get_dataframe(stmt)


async def fetch_events_split(
    signal_id: str,
    start: datetime,
    end: datetime,
    *,
    phase_codes: Iterable[int],
    det_channels: Iterable[int],
    det_codes: Iterable[int] = (EVENT_DETECTOR_ON,),
) -> pd.DataFrame:
    """
    Fetch phase events AND detector events for specific channels in one query.

    Equivalent to:
        phase_codes OR (det_codes AND event_param IN det_channels)

    Args:
        signal_id: Signal ID to query.
        start: Window start (inclusive).
        end: Window end (inclusive).
        phase_codes: Phase event codes to include unconditionally.
        det_channels: Detector channels to include.
        det_codes: Detector event codes to include (default: DETECTOR_ON).

    Returns:
        DataFrame with columns: event_code, event_param, event_time.
        Ordered by event_time.
    """
    stmt = (
        select(CEL.event_code, CEL.event_param, CEL.event_time)
        .where(
            and_(
                CEL.signal_id == signal_id,
                CEL.event_time >= start,
                CEL.event_time <= end,
                or_(
                    CEL.event_code.in_(list(phase_codes)),
                    and_(
                        CEL.event_code.in_(list(det_codes)),
                        CEL.event_param.in_(list(det_channels)),
                    ),
                ),
            )
        )
        .order_by(CEL.event_time)
    )
    return await db_facade.get_dataframe(stmt)
