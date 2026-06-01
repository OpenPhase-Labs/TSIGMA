"""
Meta-tests for the root ``tests/conftest.py`` bootstrap.

Task A4 (Valkey pub/sub cache invalidation) requires the
``TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED`` env var to be set to
``"false"`` BEFORE any ``tsigma.*`` import in the root conftest. The
Pydantic ``Settings()`` singleton is constructed at ``tsigma.app`` import
time (via ``tests/conftest.py``'s ``from tsigma.app import create_app``),
and Pydantic reads env vars only at construction time — so an
``os.environ.setdefault(...)`` placed AFTER the import has no effect.

These tests guard the ordering invariant by reading the conftest source
directly.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Locate the root conftest source
# ---------------------------------------------------------------------------


def _conftest_source() -> str:
    """Return the raw text of ``tests/conftest.py``."""
    here = Path(__file__).resolve()
    # tests/unit/test_conftest_bootstrap.py -> tests/conftest.py
    conftest = here.parent.parent / "conftest.py"
    return conftest.read_text(encoding="utf-8")


def _line_index(source: str, needle: str) -> int:
    """Return the 0-based index of the first line containing ``needle``.

    Returns -1 if the needle is absent. Raises no exception so the assertions
    below can produce clear failure messages.
    """
    for i, line in enumerate(source.splitlines()):
        if needle in line:
            return i
    return -1


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRootConftestEnvBootstrap:
    """Guards the env-var setdefault ordering required by Task A4."""

    def test_env_setdefault_for_invalidation_flag_present(self):
        """The root conftest sets TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED."""
        source = _conftest_source()
        assert "TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED" in source, (
            "Root tests/conftest.py must contain a setdefault for "
            "TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED so the Pydantic "
            "Settings singleton constructed at tsigma.app import time sees "
            "the flag disabled for the test suite."
        )

    def test_env_setdefault_value_is_false(self):
        """The setdefault value is the literal string ``"false"``."""
        source = _conftest_source()
        # Accept either single or double quotes around the value, but the
        # quoted literal must be exactly ``false`` (lowercase) — matches the
        # existing TSIGMA convention (see test_config.py line 27).
        assert (
            '"TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED", "false"' in source
            or "'TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED', 'false'" in source
        ), (
            "Root conftest setdefault must use the literal value \"false\" "
            "(lowercase) to match the existing TSIGMA env-var convention."
        )

    def test_env_setdefault_precedes_any_tsigma_import(self):
        """The setdefault line appears BEFORE any ``tsigma.*`` import."""
        source = _conftest_source()
        env_line = _line_index(source, "TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED")
        assert env_line >= 0, (
            "No setdefault line found at all — the bootstrap is missing."
        )

        # Look at every line for tsigma imports.
        first_tsigma_import = -1
        for i, line in enumerate(source.splitlines()):
            stripped = line.strip()
            if (stripped.startswith("from tsigma") or
                    stripped.startswith("import tsigma")):
                first_tsigma_import = i
                break

        assert first_tsigma_import >= 0, (
            "Root conftest has no tsigma imports — has the file been "
            "refactored? Update this test to match."
        )
        assert env_line < first_tsigma_import, (
            f"Env-var setdefault for TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED "
            f"is at line {env_line + 1} but the first tsigma.* import is at "
            f"line {first_tsigma_import + 1}. The setdefault MUST precede the "
            f"import — otherwise the Pydantic Settings singleton is constructed "
            f"with the production default (True) before the override takes "
            f"effect, and the 11 lifespan tests break (see A4 v3 failure mode)."
        )
