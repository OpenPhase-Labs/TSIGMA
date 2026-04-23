"""
Validation plugin SDK.

Shared helpers for TSIGMA validation plugins. Plugins use these to
build standardized result dicts that get written to the
validation_metadata JSONB column on controller_event_log.

The SDK defines the canonical JSONB structure so all validators
(built-in and third-party) produce consistent, queryable output.
"""

from typing import Any, Optional

# Status constants
STATUS_UNVALIDATED = "unvalidated"
STATUS_CLEAN = "clean"
STATUS_SUSPECT = "suspect"
STATUS_INVALID = "invalid"

# Severity ordering (worst wins when merging)
_STATUS_SEVERITY = {
    STATUS_UNVALIDATED: 0,
    STATUS_CLEAN: 1,
    STATUS_SUSPECT: 2,
    STATUS_INVALID: 3,
}


def build_result(
    validator_name: str,
    status: str,
    *,
    rules_failed: Optional[list[str]] = None,
    confidence: Optional[float] = None,
    details: Optional[str] = None,
) -> dict[str, Any]:
    """
    Build a single validator result dict.

    Args:
        validator_name: Registered validator name.
        status: One of STATUS_CLEAN, STATUS_SUSPECT, STATUS_INVALID.
        rules_failed: List of rule identifiers that failed.
        confidence: Optional confidence score 0.0-1.0 (for ML validators).
        details: Optional human-readable detail string.

    Returns:
        Result dict with standardized keys.
    """
    result: dict[str, Any] = {
        "validator": validator_name,
        "status": status,
    }
    if rules_failed is not None:
        result["rules_failed"] = rules_failed
    if confidence is not None:
        result["confidence"] = confidence
    if details is not None:
        result["details"] = details
    return result


def merge_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Merge per-validator results into the final validation_metadata dict.

    The overall status is the worst (highest severity) individual status.

    Args:
        results: List of dicts from build_result().

    Returns:
        Complete validation_metadata dict:
        {"status": "clean", "validators": {"schema_range": {...}}}
    """
    if not results:
        return {"status": STATUS_UNVALIDATED, "validators": {}}

    validators: dict[str, dict[str, Any]] = {}
    worst_status = STATUS_CLEAN

    for r in results:
        name = r["validator"]
        validators[name] = r
        if _STATUS_SEVERITY.get(r["status"], 0) > _STATUS_SEVERITY.get(
            worst_status, 0
        ):
            worst_status = r["status"]

    return {
        "status": worst_status,
        "validators": validators,
    }


__all__ = [
    "STATUS_UNVALIDATED",
    "STATUS_CLEAN",
    "STATUS_SUSPECT",
    "STATUS_INVALID",
    "build_result",
    "merge_results",
]
