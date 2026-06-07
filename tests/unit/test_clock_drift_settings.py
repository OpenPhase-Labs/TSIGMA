"""
Unit tests for the Inc-4 watchdog clock-drift runtime-settings registrations.

Inc-4 registers four admin-configurable runtime settings (category
``watchdog``) via ``tsigma.settings_service.register(...)`` at module load.
These tests assert each key is present in the module-level ``_REGISTRY`` with
the correct ``python_type``, ``default``, ``min``, ``max``, and ``category``.

The introspection idiom mirrors
``test_settings_typed_getters.py::TestRegistry.test_registry_entry_has_required_fields``,
which reads ``_REGISTRY[key]`` directly and asserts on the ``RegistryEntry``
fields ``key``, ``python_type``, ``default``, ``min``, ``max``, ``category``.
"""

import pytest

from tsigma.settings_service import _REGISTRY, RegistryEntry

# ---------------------------------------------------------------------------
# Expected registrations (key, default, min, max). All int, category watchdog.
# ---------------------------------------------------------------------------
EXPECTED_WATCHDOG_SETTINGS = [
    ("watchdog.controller_clock_drift_threshold_seconds", 120, 1, 3600),
    ("watchdog.controller_clock_drift_window_hours", 168, 1, 8760),
    ("watchdog.controller_clock_drift_min_samples", 5, 1, 100000),
    ("watchdog.controller_clock_offset_retention_days", 30, 1, 3650),
]


class TestWatchdogClockDriftSettings:
    """Each Inc-4 watchdog clock-drift key is registered with correct metadata."""

    @pytest.mark.parametrize(
        ("key", "default", "minimum", "maximum"),
        EXPECTED_WATCHDOG_SETTINGS,
    )
    def test_clock_drift_setting_registered(self, key, default, minimum, maximum):
        """The key is registered as an int watchdog setting with the right bounds."""
        assert key in _REGISTRY
        entry = _REGISTRY[key]
        assert isinstance(entry, RegistryEntry)
        assert entry.key == key
        assert entry.python_type is int
        assert entry.default == default
        assert entry.min == minimum
        assert entry.max == maximum
        assert entry.category == "watchdog"
