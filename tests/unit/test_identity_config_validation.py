"""Unit tests for the pure identity/config integrity-check predicates.

Covers ``check_controller_replacement`` (device-MAC drift) and
``check_configured_phases`` (phases-in-use drift) plus the shared
``IntegrityFinding`` dataclass. These are pure, synchronous, no-DB
functions living beside ``is_backward_poisoned`` in
``tsigma.collection.sdk``.
"""

from tsigma.collection.decoders.base import FileMetadata
from tsigma.collection.sdk import (
    IntegrityFinding,
    check_configured_phases,
    check_controller_replacement,
)
from tsigma.notifications.registry import WARNING
from tsigma.notifications.suppression import (
    CHECK_CONFIG_PHASE_DRIFT,
    CHECK_CONTROLLER_REPLACEMENT,
)


def _detail_values(finding: IntegrityFinding) -> list:
    """Flatten finding.detail values for tolerant membership assertions."""
    return list(finding.detail.values())


def _all_phase_numbers(finding: IntegrityFinding) -> set[int]:
    """Collect every int found in the finding.detail values (tolerant)."""
    found: set[int] = set()
    for value in finding.detail.values():
        if isinstance(value, int):
            found.add(value)
        elif isinstance(value, (list, tuple, set)):
            found.update(v for v in value if isinstance(v, int))
    return found


class TestIntegrityFinding:
    def test_constructs_with_four_fields(self):
        finding = IntegrityFinding(
            check_name="x",
            severity=WARNING,
            summary="a summary",
            detail={"k": "v"},
        )
        assert finding.check_name == "x"
        assert finding.severity == WARNING
        assert finding.summary == "a summary"
        assert finding.detail == {"k": "v"}


class TestControllerReplacement:
    def test_no_header_mac_returns_none(self):
        meta = FileMetadata(device_mac=None)
        assert check_controller_replacement(meta, "00:04:81:02:15:0d") is None

    def test_empty_header_mac_returns_none(self):
        meta = FileMetadata(device_mac="")
        assert check_controller_replacement(meta, "00:04:81:02:15:0d") is None

    def test_no_registered_mac_returns_none(self):
        meta = FileMetadata(device_mac="00:04:81:02:15:0d")
        assert check_controller_replacement(meta, None) is None

    def test_empty_registered_mac_returns_none(self):
        meta = FileMetadata(device_mac="00:04:81:02:15:0d")
        assert check_controller_replacement(meta, "") is None

    def test_equal_normalized_mixed_case_and_separators_returns_none(self):
        # case-insensitive, ignoring ':' and '-' separators -> equal -> None
        meta = FileMetadata(device_mac="00:04:81:02:15:0D")
        assert check_controller_replacement(meta, "00-04-81-02-15-0d") is None

    def test_differ_returns_finding(self):
        observed = "00:04:81:02:15:0d"
        meta = FileMetadata(device_mac=observed)
        finding = check_controller_replacement(meta, "AA:BB:CC:DD:EE:FF")
        assert finding is not None
        assert isinstance(finding, IntegrityFinding)
        assert finding.check_name == CHECK_CONTROLLER_REPLACEMENT
        assert finding.severity == WARNING
        # observed mac must appear somewhere in the detail values (tolerant of key naming)
        assert observed in _detail_values(finding)


class TestConfiguredPhases:
    def test_none_phases_in_use_returns_none(self):
        meta = FileMetadata(phases_in_use=None)
        assert check_configured_phases(meta, {2, 4, 6, 8}) is None

    def test_empty_phases_in_use_returns_none(self):
        meta = FileMetadata(phases_in_use=[])
        assert check_configured_phases(meta, {2, 4, 6, 8}) is None

    def test_empty_configured_phases_returns_none(self):
        meta = FileMetadata(phases_in_use=[2, 4, 6, 8])
        assert check_configured_phases(meta, set()) is None

    def test_equal_sets_returns_none(self):
        meta = FileMetadata(phases_in_use=[2, 4, 6, 8])
        assert check_configured_phases(meta, {8, 6, 4, 2}) is None

    def test_file_extra_phase_returns_finding_with_unexpected(self):
        # file has phase 5 that config does not -> "unexpected"
        meta = FileMetadata(phases_in_use=[2, 4, 5, 6, 8])
        finding = check_configured_phases(meta, {2, 4, 6, 8})
        assert finding is not None
        assert isinstance(finding, IntegrityFinding)
        assert finding.check_name == CHECK_CONFIG_PHASE_DRIFT
        assert finding.severity == WARNING
        assert finding.detail["unexpected"]  # non-empty
        assert 5 in _all_phase_numbers(finding)

    def test_config_extra_phase_returns_finding_with_missing(self):
        # config has phase 3 that the file does not -> "missing"
        meta = FileMetadata(phases_in_use=[2, 4, 6, 8])
        finding = check_configured_phases(meta, {2, 3, 4, 6, 8})
        assert finding is not None
        assert finding.check_name == CHECK_CONFIG_PHASE_DRIFT
        assert finding.severity == WARNING
        assert finding.detail["missing"]  # non-empty
        assert 3 in _all_phase_numbers(finding)

    def test_both_differ_returns_finding_with_both(self):
        # file has 5 (unexpected), config has 3 (missing)
        meta = FileMetadata(phases_in_use=[2, 4, 5, 6, 8])
        finding = check_configured_phases(meta, {2, 3, 4, 6, 8})
        assert finding is not None
        assert finding.check_name == CHECK_CONFIG_PHASE_DRIFT
        assert finding.severity == WARNING
        assert finding.detail["unexpected"]  # non-empty
        assert finding.detail["missing"]  # non-empty
        phases = _all_phase_numbers(finding)
        assert 5 in phases
        assert 3 in phases
