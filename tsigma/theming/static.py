"""Resolve static assets across theme → default layers with path-traversal guard."""

import logging
from pathlib import Path

from .resolver import DEFAULT_THEME

logger = logging.getLogger(__name__)


def resolve_static_dirs(
    theme: str,
    *,
    themes_root: Path,
    default_static: Path,
) -> list[Path]:
    """Return static search dirs, theme-first; fall back to default for absent theme."""
    if not theme or theme == DEFAULT_THEME:
        return [default_static]
    theme_static = Path(themes_root) / theme / "static"
    if not theme_static.is_dir():
        logger.warning(
            "Theme '%s' has no static dir at %s; falling back to default theme",
            theme,
            theme_static,
        )
        return [default_static]
    return [theme_static, default_static]


def resolve_static_file(rel_path: str, static_dirs: list[Path]) -> Path | None:
    """First existing ``<dir>/<rel_path>`` across dirs (theme first), else None.

    Rejects absolute paths and any ``..`` traversal by returning None.
    """
    rel = Path(rel_path)
    if rel.is_absolute() or ".." in rel.parts:
        return None
    for d in static_dirs:
        candidate = Path(d) / rel
        if candidate.is_file():
            return candidate
    return None
