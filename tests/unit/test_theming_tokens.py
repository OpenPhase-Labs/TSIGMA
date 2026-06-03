"""Unit tests for theme token rendering (no DB/network).

The default-theme tests below are the authoritative lock on the approved
Modern Airy "Indigo Continuity" palette (32 values, light + dark).
"""

import textwrap

import pytest

from tsigma.theming.tokens import (
    REQUIRED_PHASE_TOKENS,
    REQUIRED_SEMANTIC_TOKENS,
    TokenValidationError,
    default_theme_path,
    load_theme_toml,
    render_token_style,
)


def _minimal_theme():
    sem = {t: "#000000" for t in REQUIRED_SEMANTIC_TOKENS}
    ph = {t: "#000000" for t in REQUIRED_PHASE_TOKENS}
    return {
        "semantic": {"light": dict(sem), "dark": dict(sem)},
        "phase": {"light": dict(ph), "dark": dict(ph)},
    }


# --- Generic renderer behaviour ------------------------------------------

def test_emits_root_and_dark_blocks():
    css = render_token_style(_minimal_theme())
    assert ":root" in css
    assert '[data-mode="dark"]' in css


def test_all_required_semantic_vars_present():
    css = render_token_style(_minimal_theme())
    for t in REQUIRED_SEMANTIC_TOKENS:
        assert f"--color-{t}:" in css


def test_phase_vars_present():
    css = render_token_style(_minimal_theme())
    for t in REQUIRED_PHASE_TOKENS:
        assert f"--chart-phase-{t}:" in css


def test_missing_required_semantic_token_raises():
    theme = _minimal_theme()
    del theme["semantic"]["dark"]["border"]
    with pytest.raises(TokenValidationError):
        render_token_style(theme)


def test_missing_required_phase_token_raises():
    theme = _minimal_theme()
    del theme["phase"]["light"]["yellow"]
    with pytest.raises(TokenValidationError):
        render_token_style(theme)


def test_load_theme_toml(tmp_path):
    p = tmp_path / "t.toml"
    p.write_text(
        textwrap.dedent(
            """
            [semantic.light]
            surface = "#ffffff"
            [semantic.dark]
            surface = "#000000"
            """
        ),
        encoding="utf-8",
    )
    data = load_theme_toml(p)
    assert data["semantic"]["light"]["surface"] == "#ffffff"
    assert data["semantic"]["dark"]["surface"] == "#000000"


# --- Default theme: exact locked "Indigo Continuity" values --------------

EXPECTED_LIGHT = {
    "surface": "#f9fafb",
    "surface-raised": "#ffffff",
    "foreground": "#111827",
    "muted-foreground": "#6b7280",
    "border": "#e5e7eb",
    "ring": "#6366f1",
    "brand": "#6366f1",
    "brand-foreground": "#ffffff",
    "success": "#16a34a",
    "success-foreground": "#ffffff",
    "warning": "#d97706",
    "warning-foreground": "#ffffff",
    "error": "#dc2626",
    "error-foreground": "#ffffff",
}
EXPECTED_LIGHT_PHASE = {"green": "#16a34a", "yellow": "#f59e0b", "red": "#dc2626"}

EXPECTED_DARK = {
    "surface": "#0a0a0f",
    "surface-raised": "#15151f",
    "foreground": "#ececf1",
    "muted-foreground": "#9ca3af",
    "border": "#26262f",
    "ring": "#818cf8",
    "brand": "#818cf8",
    "brand-foreground": "#0a0a0f",
    "success": "#22c55e",
    "success-foreground": "#052e13",
    "warning": "#f59e0b",
    "warning-foreground": "#2a1a00",
    "error": "#f87171",
    "error-foreground": "#2a0a0a",
}
EXPECTED_DARK_PHASE = {"green": "#4ade80", "yellow": "#fbbf24", "red": "#f87171"}


def test_default_theme_loads_and_renders():
    css = render_token_style(load_theme_toml(default_theme_path()))
    assert ":root" in css
    assert '[data-mode="dark"]' in css


def test_default_theme_exact_light_values():
    css = render_token_style(load_theme_toml(default_theme_path()))
    root = css.split('[data-mode="dark"]')[0]
    for k, v in EXPECTED_LIGHT.items():
        assert f"--color-{k}: {v};" in root
    for k, v in EXPECTED_LIGHT_PHASE.items():
        assert f"--chart-phase-{k}: {v};" in root


def test_default_theme_exact_dark_values():
    css = render_token_style(load_theme_toml(default_theme_path()))
    dark = css.split('[data-mode="dark"]')[1]
    for k, v in EXPECTED_DARK.items():
        assert f"--color-{k}: {v};" in dark
    for k, v in EXPECTED_DARK_PHASE.items():
        assert f"--chart-phase-{k}: {v};" in dark


def test_default_phase_independent_of_brand():
    theme = load_theme_toml(default_theme_path())
    assert theme["phase"]["light"]["green"] != theme["semantic"]["light"]["brand"]
    assert theme["phase"]["dark"]["green"] != theme["semantic"]["dark"]["brand"]
