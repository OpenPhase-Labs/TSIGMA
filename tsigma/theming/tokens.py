"""Render theme.toml token values into a CSS custom-property <style> block.

Two layers per spec: semantic tokens (what templates reference) and phase
tokens (kept independent of brand so a green phase always reads green). Each
has a light value set (``:root``) and a dark override (``[data-mode="dark"]``).
"""

import logging
import tomllib
from pathlib import Path

logger = logging.getLogger(__name__)

REQUIRED_SEMANTIC_TOKENS = frozenset(
    {
        "surface",
        "surface-raised",
        "foreground",
        "muted-foreground",
        "border",
        "ring",
        "brand",
        "brand-foreground",
        "success",
        "success-foreground",
        "warning",
        "warning-foreground",
        "error",
        "error-foreground",
    }
)
REQUIRED_PHASE_TOKENS = frozenset({"green", "yellow", "red"})


class TokenValidationError(Exception):
    """Raised when a theme is missing a required semantic or phase token."""


def default_theme_path() -> Path:
    """Path to the shipped default theme (Modern Airy / Indigo Continuity)."""
    return Path(__file__).resolve().parent / "default_theme.toml"


def load_theme_toml(path) -> dict:
    """Parse a theme.toml file into a dict."""
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def _validate(mapping: dict, required: frozenset, mode: str, kind: str) -> None:
    missing = required - set(mapping)
    if missing:
        raise TokenValidationError(
            f"Theme {kind} tokens for '{mode}' missing: {sorted(missing)}"
        )


def render_token_style(theme: dict) -> str:
    """Render the theme's tokens into ``:root`` + ``[data-mode="dark"]`` CSS.

    Validates that every required semantic and phase token is present in BOTH
    the light and dark sets; raises ``TokenValidationError`` otherwise.
    """
    semantic = theme.get("semantic", {})
    phase = theme.get("phase", {})
    blocks: list[str] = []
    for mode, selector in (("light", ":root"), ("dark", '[data-mode="dark"]')):
        sem = semantic.get(mode, {})
        ph = phase.get(mode, {})
        _validate(sem, REQUIRED_SEMANTIC_TOKENS, mode, "semantic")
        _validate(ph, REQUIRED_PHASE_TOKENS, mode, "phase")
        lines = [f"{selector} {{"]
        for key in sorted(sem):
            lines.append(f"  --color-{key}: {sem[key]};")
        for key in sorted(ph):
            lines.append(f"  --chart-phase-{key}: {ph[key]};")
        lines.append("}")
        blocks.append("\n".join(lines))
    return "\n".join(blocks) + "\n"


def token_style_for(theme: str, *, themes_root, default_toml=None) -> str:
    """Render the active theme's token ``<style>`` CSS.

    Falls back to the shipped default theme when ``theme`` is ``default``/empty
    or the agency ``themes_root/<theme>/theme.toml`` is absent (logs a WARNING).
    """
    toml_path = default_toml or default_theme_path()
    if theme and theme != "default":
        candidate = Path(themes_root) / theme / "theme.toml"
        if candidate.is_file():
            return render_token_style(load_theme_toml(candidate))
        logger.warning(
            "Theme '%s' has no theme.toml at %s; using default theme tokens",
            theme,
            candidate,
        )
    return render_token_style(load_theme_toml(toml_path))
