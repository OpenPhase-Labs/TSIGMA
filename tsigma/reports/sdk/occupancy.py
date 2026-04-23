"""
Detector occupancy helpers.

`calculate_occupancy` measures on-time within a fixed time window.
`accumulate_on_time` distributes on-time across multiple time bins.
`bin_occupancy_pct` pairs on/off events and returns per-bin occupancy %.
All were duplicated across reports before being pulled in here.
"""

from collections import defaultdict
from collections.abc import MutableMapping
from datetime import datetime, timedelta

from .events import EVENT_DETECTOR_OFF, EVENT_DETECTOR_ON
from .time_bins import bin_timestamp


def calculate_occupancy(
    det_events: list[tuple[datetime, int]],
    window_start: datetime,
    window_seconds: float,
) -> float:
    """
    Detector occupancy ratio within a [window_start, window_start + window_seconds] window.

    Args:
        det_events: Sorted list of (timestamp, event_code) tuples — only
            EVENT_DETECTOR_ON / EVENT_DETECTOR_OFF codes are meaningful.
        window_start: Start of the measurement window.
        window_seconds: Duration of the window in seconds.

    Returns:
        Occupancy ratio clamped to [0.0, 1.0].
    """
    if window_seconds <= 0:
        return 0.0

    window_end = window_start + timedelta(seconds=window_seconds)
    on_time = 0.0
    detector_on_at: datetime | None = None

    for event_time, event_code in det_events:
        if event_time > window_end:
            break
        if event_code == EVENT_DETECTOR_ON:
            if detector_on_at is None and event_time < window_end:
                detector_on_at = max(event_time, window_start)
        elif event_code == EVENT_DETECTOR_OFF:
            if detector_on_at is not None:
                off_time = min(event_time, window_end)
                if off_time > window_start:
                    on_time += (off_time - max(detector_on_at, window_start)).total_seconds()
                detector_on_at = None

    if detector_on_at is not None:
        on_time += (window_end - max(detector_on_at, window_start)).total_seconds()

    return min(on_time / window_seconds, 1.0)


def accumulate_on_time(
    bin_on_time: MutableMapping[str, float],
    on_start: datetime,
    off_time: datetime,
    bin_size_minutes: int,
) -> None:
    """
    Accumulate detector on-time across bin boundaries.

    Splits a single detector on-interval across however many bins it
    crosses and adds the resulting durations into `bin_on_time` in
    place (keys are ISO bin start strings).
    """
    current = on_start
    while current < off_time:
        key = bin_timestamp(current, bin_size_minutes)
        bin_minute = (current.minute // bin_size_minutes) * bin_size_minutes
        bin_end = current.replace(
            minute=bin_minute, second=0, microsecond=0
        ) + timedelta(minutes=bin_size_minutes)
        segment_end = min(bin_end, off_time)
        bin_on_time[key] += (segment_end - current).total_seconds()
        current = segment_end


def bin_occupancy_pct(
    det_events: list[tuple[datetime, int]],
    end_time: datetime,
    bin_size_minutes: int,
) -> dict[str, float]:
    """
    Detector occupancy percentage per time bin.

    Pairs on/off events, distributes on-time across bins via
    ``accumulate_on_time``, then converts to 0-100 percentages.

    Args:
        det_events: Sorted list of (timestamp, event_code) tuples for
            a single detector channel.
        end_time: Analysis end time (closes any trailing on-interval).
        bin_size_minutes: Size of each time bin in minutes.

    Returns:
        Dict mapping bin key (ISO string) to occupancy percentage (0-100).
    """
    bin_seconds = bin_size_minutes * 60
    bin_on_time: dict[str, float] = defaultdict(float)
    detector_on_at: datetime | None = None

    for event_time, event_code in det_events:
        if event_code == EVENT_DETECTOR_ON and detector_on_at is None:
            detector_on_at = event_time
        elif event_code == EVENT_DETECTOR_OFF and detector_on_at is not None:
            accumulate_on_time(bin_on_time, detector_on_at, event_time, bin_size_minutes)
            detector_on_at = None

    if detector_on_at is not None:
        accumulate_on_time(bin_on_time, detector_on_at, end_time, bin_size_minutes)

    return {
        bin_key: min((on_secs / bin_seconds) * 100, 100.0)
        for bin_key, on_secs in bin_on_time.items()
    }
