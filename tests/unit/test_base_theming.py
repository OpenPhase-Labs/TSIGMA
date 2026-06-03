"""Regression lock on the themed base template (no DB/network).

base.html was reskinned as manual-visual work; these assertions pin the
load-bearing structural facts (compiled CSS linked, token style injected,
mode toggle present, no-flash script, custom.css last) and exercise a real
render so a Jinja error in the template fails fast.
"""

from tsigma.api.ui import templates


def _render_base():
    return templates.env.get_template("base.html").render()


def test_base_links_compiled_tailwind_not_play_cdn():
    html = _render_base()
    assert "/static/css/tailwind.css" in html
    assert "vendor/tailwind/tailwind.min.js" not in html


def test_base_injects_token_style():
    html = _render_base()
    assert ":root" in html
    assert '[data-mode="dark"]' in html
    assert "--color-brand" in html


def test_base_has_mode_toggle():
    html = _render_base()
    assert "data-mode-toggle" in html


def test_base_no_flash_script_present():
    html = _render_base()
    assert "prefers-color-scheme" in html
    assert "data-mode" in html


def test_base_custom_css_loads_after_tailwind():
    html = _render_base()
    assert html.index("/static/css/custom.css") > html.index("/static/css/tailwind.css")
