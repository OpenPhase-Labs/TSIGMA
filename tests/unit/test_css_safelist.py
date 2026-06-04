"""Pre-generated Tailwind utility vocabulary lock on the compiled stylesheet.

The feature pre-generates a broad Tailwind v4 utility vocabulary into the
committed ``tsigma/static/css/tailwind.css`` so agencies can use those classes
without building CSS. These assertions pin that representative vocabulary is
PRESENT and that an off-contract raw palette hue is ABSENT (agencies get only
semantic tokens + ``gray-*``, never raw palette hues).

Pure file-content test: reads the compiled CSS, no DB, network, or fixtures.

Tailwind backslash-escapes special characters in class selectors (e.g.
``.md\\:grid-cols-3{...}``). To make matching robust we strip backslashes from
the CSS text once and substring-search the human-readable class string; the
same normalization is applied to the absent-guard check.
"""

from pathlib import Path

import pytest

# tests/unit/test_css_safelist.py -> repo root is three parents up.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_CSS_PATH = _REPO_ROOT / "tsigma" / "static" / "css" / "tailwind.css"

# Classes that must each appear as a usable utility in the compiled CSS.
PRESENT_CLASSES = [
    "md:grid-cols-3",
    "gap-12",
    "rounded-3xl",
    "hover:bg-brand",
    "dark:bg-surface-raised",
    "focus-visible:ring-ring",
    "bg-gray-500",
    "xl:flex",
]

# Off-contract raw palette hue that must NOT be present (color restriction).
ABSENT_CLASSES = [
    "bg-blue-500",
]


def _normalized_css() -> str:
    """Return the compiled CSS with selector backslash-escapes removed.

    Tailwind writes ``.md\\:grid-cols-3{...}``; stripping the backslashes lets a
    naive substring search for ``md:grid-cols-3`` match the escaped selector.
    """
    text = _CSS_PATH.read_text(encoding="utf-8")
    return text.replace("\\", "")


@pytest.mark.parametrize("css_class", PRESENT_CLASSES)
def test_present_class_in_compiled_css(css_class):
    """Each representative utility class is present in the compiled CSS."""
    css = _normalized_css()
    assert css_class in css, f"expected class '{css_class}' to be present in tailwind.css"


@pytest.mark.parametrize("css_class", ABSENT_CLASSES)
def test_absent_class_not_in_compiled_css(css_class):
    """Off-contract raw palette hues must not appear in the compiled CSS."""
    css = _normalized_css()
    assert css_class not in css, f"expected class '{css_class}' to be absent from tailwind.css"
