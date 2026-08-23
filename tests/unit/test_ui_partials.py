"""Render tests for the Modern Airy UI partial macros (no DB/network).

Locks the approved D-3 component recipes: each variant emits the agreed
semantic utilities. Visual "is it Modern Airy" is verified manually.
"""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

import tsigma

TEMPLATES = Path(tsigma.__path__[0]) / "templates"


def _env():
    return Environment(loader=FileSystemLoader(str(TEMPLATES)))


def _macros():
    return _env().get_template("components/ui.html").module


# --- button ---------------------------------------------------------------

def test_button_primary():
    html = _macros().button("Run report", variant="primary")
    assert "bg-brand" in html
    assert "text-brand-foreground" in html
    assert "focus:ring-ring" in html
    assert "Run report" in html


def test_button_secondary():
    html = _macros().button("Export", variant="secondary")
    assert "bg-surface-raised" in html
    assert "border-border" in html


def test_button_ghost():
    html = _macros().button("Cancel", variant="ghost")
    assert "bg-transparent" in html
    assert "text-muted-foreground" in html


def test_button_warning():
    html = _macros().button("Stop collection", variant="warning")
    assert "bg-warning" in html
    assert "text-warning-foreground" in html


def test_button_danger():
    html = _macros().button("Delete", variant="danger")
    assert "bg-error" in html
    assert "text-error-foreground" in html


def test_button_size_sm():
    html = _macros().button("Small", variant="primary", size="sm")
    assert "text-xs" in html


# --- badge ----------------------------------------------------------------

def test_badge_success():
    html = _macros().badge("Online", variant="success")
    assert "bg-success/15" in html
    assert "text-success" in html
    assert "Online" in html


def test_badge_neutral_default():
    html = _macros().badge("Unknown")
    assert "text-muted-foreground" in html


# --- field ----------------------------------------------------------------

def test_field_renders_label_input_hint():
    html = _macros().field(
        "signal_id", "Signal ID", placeholder="SIG-1042", hint="Cabinet id"
    )
    assert "Signal ID" in html
    assert 'name="signal_id"' in html
    assert "border-border" in html
    assert "focus:ring-ring" in html
    assert "Cabinet id" in html


# --- card (slot via {% call %}) -------------------------------------------

def test_card_wraps_content():
    out = _env().from_string(
        '{% from "components/ui.html" import card %}'
        '{% call card(title="SIG-1042") %}BODYCONTENT{% endcall %}'
    ).render()
    assert "bg-surface-raised" in out
    assert "rounded-2xl" in out
    assert "shadow-md" in out
    assert "SIG-1042" in out
    assert "BODYCONTENT" in out


# --- page_header (actions slot) -------------------------------------------

def test_page_header_with_actions():
    out = _env().from_string(
        '{% from "components/ui.html" import page_header %}'
        '{% call page_header("Performance Overview", eyebrow="Corridor",'
        ' description="Last 24h") %}ACTIONSLOT{% endcall %}'
    ).render()
    assert "Performance Overview" in out
    assert "font-display" in out
    assert "text-2xl" in out
    assert "Corridor" in out
    assert "Last 24h" in out
    assert "ACTIONSLOT" in out
