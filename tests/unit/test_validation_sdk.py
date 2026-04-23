"""Tests for tsigma.validation.sdk helpers."""

from tsigma.validation.sdk import (
    STATUS_CLEAN,
    STATUS_INVALID,
    STATUS_SUSPECT,
    build_result,
    merge_results,
)


def test_build_result_clean():
    result = build_result("schema_range", STATUS_CLEAN)
    assert result == {"validator": "schema_range", "status": "clean"}


def test_build_result_with_details():
    result = build_result(
        "schema_range",
        STATUS_SUSPECT,
        rules_failed=["phase_gap_too_large"],
        details="Gap of 5.2s exceeds 3s threshold",
    )
    assert result["validator"] == "schema_range"
    assert result["status"] == "suspect"
    assert result["rules_failed"] == ["phase_gap_too_large"]
    assert result["details"] == "Gap of 5.2s exceeds 3s threshold"


def test_merge_results_empty():
    merged = merge_results([])
    assert merged == {"status": "unvalidated", "validators": {}}


def test_merge_results_all_clean():
    results = [
        build_result("validator_a", STATUS_CLEAN),
        build_result("validator_b", STATUS_CLEAN),
    ]
    merged = merge_results(results)
    assert merged["status"] == "clean"
    assert "validator_a" in merged["validators"]
    assert "validator_b" in merged["validators"]


def test_merge_results_worst_wins():
    results = [
        build_result("validator_a", STATUS_CLEAN),
        build_result("validator_b", STATUS_SUSPECT),
    ]
    merged = merge_results(results)
    assert merged["status"] == "suspect"


def test_merge_results_invalid_beats_suspect():
    results = [
        build_result("validator_a", STATUS_SUSPECT),
        build_result("validator_b", STATUS_INVALID),
        build_result("validator_c", STATUS_CLEAN),
    ]
    merged = merge_results(results)
    assert merged["status"] == "invalid"
