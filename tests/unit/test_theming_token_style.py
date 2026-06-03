"""Tests for active-theme token-style resolution (no DB/network).

token_style_for(theme, themes_root) returns the rendered :root/[data-mode=dark]
token <style> CSS for the active theme, falling back to the shipped default
theme when the theme is 'default'/empty or an agency theme.toml is absent.
"""

import textwrap

from tsigma.theming.tokens import token_style_for

_AGENCY_TOML = """
[semantic.light]
surface = "#ffffff"
surface-raised = "#ffffff"
foreground = "#000000"
muted-foreground = "#555555"
border = "#dddddd"
ring = "#abcdef"
brand = "#123456"
brand-foreground = "#ffffff"
success = "#16a34a"
success-foreground = "#ffffff"
warning = "#d97706"
warning-foreground = "#ffffff"
error = "#dc2626"
error-foreground = "#ffffff"

[semantic.dark]
surface = "#000000"
surface-raised = "#111111"
foreground = "#ffffff"
muted-foreground = "#aaaaaa"
border = "#222222"
ring = "#abcdef"
brand = "#abcdef"
brand-foreground = "#000000"
success = "#22c55e"
success-foreground = "#052e13"
warning = "#f59e0b"
warning-foreground = "#2a1a00"
error = "#f87171"
error-foreground = "#2a0a0a"

[phase.light]
green = "#16a34a"
yellow = "#f59e0b"
red = "#dc2626"

[phase.dark]
green = "#4ade80"
yellow = "#fbbf24"
red = "#f87171"
"""


def test_default_theme_style_has_indigo_brand():
    css = token_style_for("default", themes_root="/nonexistent")
    assert ":root" in css
    assert '[data-mode="dark"]' in css
    assert "--color-brand: #6366f1;" in css


def test_empty_theme_uses_default():
    css = token_style_for("", themes_root="/nonexistent")
    assert "--color-brand: #6366f1;" in css


def test_missing_agency_theme_falls_back_to_default(tmp_path):
    css = token_style_for("ghost", themes_root=tmp_path)
    assert "--color-brand: #6366f1;" in css


def test_agency_theme_overrides_brand(tmp_path):
    toml = tmp_path / "acme" / "theme.toml"
    toml.parent.mkdir(parents=True)
    toml.write_text(textwrap.dedent(_AGENCY_TOML), encoding="utf-8")
    css = token_style_for("acme", themes_root=tmp_path)
    assert "--color-brand: #123456;" in css
