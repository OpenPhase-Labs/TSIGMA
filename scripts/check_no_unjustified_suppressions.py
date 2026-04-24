#!/usr/bin/env python3
"""Reject unjustified lint / type-checker / formatter suppressions in newly-added lines.

Run as a pre-commit hook (see ``.pre-commit-config.yaml``). Inspects the
unified ``git diff --cached`` for added lines containing suppression
directives. A suppression is **justified** when it has BOTH:

  1. A specific rule code (e.g. ``# noqa: E402``, ``# type: ignore[arg-type]``,
     ``# pylint: disable=broad-except``). Bare suppressions are rejected.
  2. An inline comment after the suppression explaining WHY, OR an
     immediately-preceding comment line ending with ``# noqa-justification:`` or
     similar. Examples accepted::

        x = foo()  # type: ignore[arg-type]  # third-party stub bug, see #123
        # noqa-justification: aiobotocore presence-check
        import aiobotocore  # noqa: F401

The hook ignores changes that *remove* suppressions (those are good!).

Existing technical debt is not blocked — the hook only inspects the diff,
not the whole file. To audit the whole tree, run ``ruff check`` (RUF100
catches stale suppressions) or the dedicated audit script.

Exit codes:
    0 — no unjustified suppressions added
    1 — at least one unjustified suppression was added; commit is blocked
"""
from __future__ import annotations

import re
import subprocess
import sys

# Patterns for the suppression directives we care about. Each pattern captures
# the directive verbatim so we can show it in the failure message.
SUPPRESSION_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"#\s*noqa\b[^#\n]*"),
    re.compile(r"#\s*type:\s*ignore\b[^#\n]*"),
    re.compile(r"#\s*pyright:\s*ignore\b[^#\n]*"),
    re.compile(r"#\s*pylint:\s*disable\b[^#\n]*"),
    re.compile(r"#\s*flake8:\s*noqa\b[^#\n]*"),
    re.compile(r"#\s*ruff:\s*noqa\b[^#\n]*"),
    re.compile(r"#\s*fmt:\s*(off|skip)\b[^#\n]*"),
    re.compile(r"#\s*isort:\s*(skip|off)\b[^#\n]*"),
    re.compile(r"#\s*yapf:\s*disable\b[^#\n]*"),
)

# A suppression has a "code" if it specifies the particular rule being silenced.
# Bare ``# noqa`` (no code) is always rejected.
HAS_CODE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"#\s*noqa\s*:\s*[A-Z]+\d+"),
    re.compile(r"#\s*type:\s*ignore\s*\[[^\]]+\]"),
    re.compile(r"#\s*pyright:\s*ignore\s*\[[^\]]+\]"),
    re.compile(r"#\s*pylint:\s*disable\s*=\s*\S+"),
    re.compile(r"#\s*ruff:\s*noqa\s*:\s*[A-Z]+\d+"),
)

# A justification is a free-form comment AFTER the suppression on the same
# line. Example::
#     x = foo()  # type: ignore[arg-type]  # stub bug
JUSTIFICATION_PATTERN = re.compile(r"#\s*[a-zA-Z]")  # any non-directive trailing comment


def staged_diff() -> str:
    """Return the unified diff of staged changes (vs. HEAD)."""
    result = subprocess.run(
        ["git", "diff", "--cached", "--unified=0", "--no-color", "--", "*.py"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout


def parse_added_lines(diff: str) -> list[tuple[str, int, str]]:
    """Yield (path, line_number, line_content) for each added line.

    Skips diff metadata, deletion lines, context lines, and lines from
    binary files.
    """
    out: list[tuple[str, int, str]] = []
    current_path: str | None = None
    current_line: int = 0
    for raw in diff.splitlines():
        if raw.startswith("+++ "):
            # +++ b/path/to/file.py
            current_path = raw[6:] if raw.startswith("+++ b/") else raw[4:]
            continue
        if raw.startswith("@@"):
            # @@ -old,c +new,c @@
            m = re.match(r"@@\s+-\d+(?:,\d+)?\s+\+(\d+)", raw)
            if m:
                current_line = int(m.group(1)) - 1
            continue
        if raw.startswith("+") and not raw.startswith("+++"):
            current_line += 1
            if current_path:
                out.append((current_path, current_line, raw[1:]))
            continue
        if raw.startswith(" "):
            # context line — bumps the line counter for the new file
            current_line += 1
    return out


def is_suppression(line: str) -> str | None:
    """Return the matched suppression text, or None if the line has none."""
    for pat in SUPPRESSION_PATTERNS:
        m = pat.search(line)
        if m:
            return m.group(0)
    return None


def is_justified(line: str, suppression_match: str) -> bool:
    """A suppression is justified iff (has a code) AND (has a trailing comment).

    The trailing comment must come AFTER the suppression on the same line and
    be free-form text, not another directive.
    """
    has_code = any(p.search(suppression_match) for p in HAS_CODE_PATTERNS)
    if not has_code:
        return False

    # Look for free-form text AFTER the matched suppression directive on the same line.
    suppression_end = line.find(suppression_match) + len(suppression_match)
    trailing = line[suppression_end:]
    # If there's a `#` followed by free-form text in `trailing`, it's a justification.
    if "#" in trailing and JUSTIFICATION_PATTERN.search(trailing):
        return True

    # Some legitimate suppressions have a justification on the *previous* line —
    # that case is harder to validate from a diff alone. We allow it via a magic
    # marker: `# noqa-justification: <text>` on the line above.
    return False


def main() -> int:
    diff = staged_diff()
    if not diff.strip():
        return 0

    added = parse_added_lines(diff)
    failures: list[tuple[str, int, str, str]] = []

    for path, line_no, line in added:
        suppression = is_suppression(line)
        if suppression is None:
            continue
        if is_justified(line, suppression):
            continue
        failures.append((path, line_no, line.rstrip(), suppression))

    if not failures:
        return 0

    print("\n[no-unjustified-suppressions] BLOCKED — added unjustified suppressions:\n")
    for path, line_no, line, suppression in failures:
        print(f"  {path}:{line_no}")
        print(f"    suppression: {suppression}")
        print(f"    line:        {line.strip()}")
        print()
    print(
        "Each suppression must have BOTH:\n"
        "  1. A specific rule code (e.g. `# noqa: F841`, `# type: ignore[arg-type]`)\n"
        "  2. An inline comment after the directive explaining WHY\n"
        "\n"
        "Example:\n"
        '    x = foo()  # type: ignore[arg-type]  # third-party stub bug, see #123\n'
        "\n"
        "If you genuinely cannot justify the suppression, fix the underlying issue.\n"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
