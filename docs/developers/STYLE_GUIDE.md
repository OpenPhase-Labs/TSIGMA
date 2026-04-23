# TSIGMA Style Guide

**Purpose**: Authoritative formatting and style rules for TSIGMA code.

**Last Updated**: 2026-04-22

---

## Python

### 1. PEP 8 is the base

All Python code follows [PEP 8](https://peps.python.org/pep-0008/) as the base style.

When any rule below conflicts with PEP 8, the rule below wins.

### 2. Line length: 120

Override PEP 8's 79-character limit. Maximum line length is **120 characters**.

Enforced via `pyproject.toml`:

```toml
[tool.ruff]
line-length = 120
```

### 3. Import ordering: isort (ruff `I001`)

Imports are ordered using the isort convention, enforced by ruff rule `I001`.

Rules:
- Three import groups, separated by a single blank line:
  1. Standard library
  2. Third-party
  3. First-party (`tsigma.*`)
- Within each group, imports are sorted **alphabetically**.
- Within a single `from X import a, b, c` statement, names are sorted alphabetically. Underscore-prefixed private names sort **before** public names (`_AsyncSSHClient, _create_client`, not `_create_client, _AsyncSSHClient`).
- Inline / lazy imports inside a function body also follow the same ordering.

Enforced via `pyproject.toml`:

```toml
[tool.ruff.lint]
select = ["E", "F", "I", "W"]
```

### 4. File size limit: 1000 lines

No Python source file may exceed **1000 lines**. A file crossing that threshold must be split — by responsibility, not by arbitrary line count. Typical split targets: separate sub-modules, move helper functions to a `_helpers.py`, extract a class to its own module.

Not natively enforced by ruff; check with:

```bash
find tsigma tests -name "*.py" -exec wc -l {} + | awk '$1 > 1000'
```

This will be added to CI as a hard gate.

### 5. Trailing newline at end of file

Every Python source file must end with a single blank line (one trailing `\n`). Enforced by ruff rule `W292` (no newline at end of file), which is already in the `W` selection.

### 6. Blank lines between top-level definitions

Top-level `class` and `def` statements are separated by **two blank lines**. Nested methods inside a class are separated by **one blank line**.

Enforced by ruff rules (all in the `E` selection):

- `E302` — expected 2 blank lines between top-level definitions
- `E303` — too many blank lines
- `E305` — expected 2 blank lines after end of function or class

### 7. Indentation

- **4 spaces per level.** No tabs. Enforced by ruff `E111`.
- **Maximum 4 levels of indentation** inside a function body. If you find yourself nesting a fifth level, stop and refactor first.
- **Prefer early exits / guard clauses** over deep nesting. This is the primary tool for keeping under the 4-level cap.

```python
# Preferred — guard clauses, 2 levels max
def process(signal):
    if signal is None:
        return None
    if not signal.enabled:
        return None
    if signal.mode != "active":
        return None

    for event in signal.events:
        handle(event)

# Avoid — deep nesting
def process(signal):
    if signal is not None:
        if signal.enabled:
            if signal.mode == "active":
                for event in signal.events:
                    handle(event)
```

The 4-level cap is a soft rule: exceed it only when the deeper structure is genuinely unavoidable (nested comprehensions that are clearer than the alternatives, state-machine dispatch, etc.). Justify in a comment when you do.

### 8. Function length (soft rule)

Functions should stay **under ~50 lines** of body. This is a soft guideline, not a hard rule — a long function that reads linearly and has a single clear responsibility is fine.

When a function grows past 50 lines, consider whether it has picked up multiple responsibilities and should be split. Don't mechanically extract helpers just to hit a line count.

Not enforced by the linter.

### 9. Running the linter

```bash
ruff check .
ruff check --fix .
```

CI fails on any `ruff check` error. Fix locally before pushing.

---

## Rationale

- **PEP 8 base**: familiar to every Python developer; no surprises.
- **120-char line length**: matches modern screen widths; avoids artificial wrapping of type annotations and SQLAlchemy queries without letting lines grow unreadably long.
- **isort `I001`**: deterministic import ordering eliminates merge-conflict churn and makes imports scannable.
- **1000-line file cap**: forces modular decomposition; files above that threshold almost always have multiple responsibilities and become hard to review, test, and navigate.
- **Trailing newline**: POSIX convention; prevents noisy diffs and tool breakage on the last line.
- **Two blank lines between top-level defs**: standard PEP 8; creates clear visual separation between unrelated units and keeps reading rhythm consistent.
- **4-space indent / 4-level nesting cap**: PEP 8's 4-space rule is universal; limiting nesting depth forces early-return refactors that read top-to-bottom instead of rightward-drifting pyramids.
- **~50-line function soft limit**: keeps single-responsibility pressure on functions without being dogmatic; long linear functions are OK when they actually read linearly.

---

## Changelog

**2026-04-22**: Initial style guide — PEP 8 base, line-length 120, isort `I001` enforced, 1000-line file cap, trailing newline required, two blank lines between top-level definitions, 4-space indent with 4-level nesting cap (early exits preferred), ~50-line function soft limit.
