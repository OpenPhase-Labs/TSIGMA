#!/usr/bin/env python3
"""Generate the Tailwind v4 utility safelist for the committed TSIGMA stylesheet.

GOAL
----
Pre-generate a broad, standard Tailwind v4 utility vocabulary into the committed
``tsigma/static/css/tailwind.css`` so agencies can build/restyle template
overrides freely WITHOUT running Tailwind themselves. The committed CSS is served
as-is by deployers.

This script is the human-readable record of that vocabulary. It declaratively
defines the utility families, their standard scales, the (restricted) color
palette, the opacity steps, and the variants; expands them into a flat list of
class names; and writes them one-per-line to ``tsigma/static/css/safelist.txt``.

``tailwind.src.css`` references that file via ``@source "./safelist.txt";`` so the
compile force-emits every class listed here, in addition to whatever the package
templates/JS already use.

DESIGN NOTES
------------
* Dependency-free, pure-Python, portable. The expansion functions are pure (no
  I/O) so they are unit-testable in principle.
* COLOR is a HARD BOUNDARY: only the semantic theme tokens and ``gray-50..950``
  may appear. NO other literal palette hues (no blue/red/green/etc.). This
  restriction is the entire point of the color contract.
* Family/scale definitions are kept as readable data tables, not magic strings.
"""

from __future__ import annotations

import os
from collections.abc import Iterable

# ---------------------------------------------------------------------------
# Standard Tailwind scales (shared building blocks)
# ---------------------------------------------------------------------------

# The canonical Tailwind v4 numeric spacing scale (rem-based ramp).
SPACING_SCALE: list[str] = [
    "0", "px", "0.5", "1", "1.5", "2", "2.5", "3", "3.5", "4", "5", "6", "7",
    "8", "9", "10", "11", "12", "14", "16", "20", "24", "28", "32", "36", "40",
    "44", "48", "52", "56", "60", "64", "72", "80", "96",
]

# Common fraction keywords used by width/height/inset/translate/basis families.
FRACTIONS: list[str] = [
    "1/2",
    "1/3", "2/3",
    "1/4", "2/4", "3/4",
    "1/5", "2/5", "3/5", "4/5",
    "1/6", "2/6", "3/6", "4/6", "5/6",
    "1/12", "2/12", "3/12", "4/12", "5/12", "6/12",
    "7/12", "8/12", "9/12", "10/12", "11/12",
]

# max-width named sizes.
MAX_W_NAMED: list[str] = [
    "0", "none", "xs", "sm", "md", "lg", "xl", "2xl", "3xl", "4xl", "5xl",
    "6xl", "7xl", "full", "min", "max", "fit", "prose",
    "screen-sm", "screen-md", "screen-lg", "screen-xl", "screen-2xl",
]

# Border-radius named sizes (excluding the bare `rounded`).
RADIUS_SIZES: list[str] = ["none", "sm", "md", "lg", "xl", "2xl", "3xl", "full"]

# Per-corner / per-side radius position keys.
RADIUS_SIDES: list[str] = ["t", "r", "b", "l", "tl", "tr", "br", "bl"]

# Font sizes.
FONT_SIZES: list[str] = [
    "xs", "sm", "base", "lg", "xl", "2xl", "3xl", "4xl", "5xl", "6xl", "7xl",
    "8xl", "9xl",
]

# Font weights.
FONT_WEIGHTS: list[str] = [
    "thin", "extralight", "light", "normal", "medium", "semibold", "bold",
    "extrabold", "black",
]

# line-height (leading) scale.
LEADING_SCALE: list[str] = [
    "none", "tight", "snug", "normal", "relaxed", "loose",
    "3", "4", "5", "6", "7", "8", "9", "10",
]

# letter-spacing (tracking) scale.
TRACKING_SCALE: list[str] = [
    "tighter", "tight", "normal", "wide", "wider", "widest",
]

# z-index scale.
Z_SCALE: list[str] = ["0", "10", "20", "30", "40", "50", "auto"]

# shadow scale.
SHADOW_SIZES: list[str] = ["sm", "md", "lg", "xl", "2xl", "inner", "none"]

# opacity numeric scale (0..100 by 5).
OPACITY_SCALE: list[str] = [str(n) for n in range(0, 101, 5)]

# transition duration / delay scale (ms).
DURATION_SCALE: list[str] = [
    "0", "75", "100", "150", "200", "300", "500", "700", "1000",
]

# transition timing functions.
EASE_SCALE: list[str] = ["linear", "in", "out", "in-out"]

# scale-* transform scale.
SCALE_SCALE: list[str] = ["0", "50", "75", "90", "95", "100", "105", "110", "125", "150"]

# rotate-* transform scale.
ROTATE_SCALE: list[str] = ["0", "1", "2", "3", "6", "12", "45", "90", "180"]

# grid column/row count scales.
GRID_COLS: list[str] = [str(n) for n in range(1, 13)] + ["none"]
GRID_ROWS: list[str] = [str(n) for n in range(1, 7)] + ["none"]
SPAN_SCALE: list[str] = [str(n) for n in range(1, 13)] + ["full"]
COL_START_END: list[str] = [str(n) for n in range(1, 14)] + ["auto"]
ORDER_SCALE: list[str] = [str(n) for n in range(1, 13)] + ["first", "last", "none"]
ROW_SPAN_SCALE: list[str] = [str(n) for n in range(1, 7)] + ["full"]
ROW_START_END: list[str] = [str(n) for n in range(1, 8)] + ["auto"]

# line-clamp scale.
LINE_CLAMP: list[str] = ["1", "2", "3", "4", "5", "6", "none"]

# border width values.
BORDER_WIDTHS: list[str] = ["0", "2", "4", "8"]  # bare `border` handled separately
RING_WIDTHS: list[str] = ["0", "1", "2", "4", "8"]
RING_OFFSET_WIDTHS: list[str] = ["0", "1", "2", "4", "8"]
DIVIDE_WIDTHS: list[str] = ["0", "2", "4", "8"]
OUTLINE_WIDTHS: list[str] = ["0", "1", "2", "4", "8"]
BORDER_SIDES: list[str] = ["x", "y", "t", "r", "b", "l"]

# aspect-ratio keywords.
ASPECT: list[str] = ["auto", "square", "video"]


# ---------------------------------------------------------------------------
# Color contract (RESTRICTED — hard boundary)
# ---------------------------------------------------------------------------

# Semantic theme tokens. These bind to CSS vars injected at render time.
SEMANTIC_TOKENS: list[str] = [
    "surface", "surface-raised", "foreground", "muted-foreground", "border",
    "ring", "brand", "brand-foreground", "success", "success-foreground",
    "warning", "warning-foreground", "error", "error-foreground",
]

# The ONLY literal palette hue allowed: gray (standard steps).
GRAY_STEPS: list[str] = [
    "50", "100", "200", "300", "400", "500", "600", "700", "800", "900", "950",
]

# All color "names" available to bg-/text-/border-/ring- utilities.
COLOR_NAMES: list[str] = SEMANTIC_TOKENS + [f"gray-{s}" for s in GRAY_STEPS]

# Color utility prefixes.
COLOR_PREFIXES: list[str] = ["bg", "text", "border", "ring"]

# Standard Tailwind opacity steps applied to color utilities (e.g. bg-brand/50).
COLOR_OPACITY_STEPS: list[str] = [
    "0", "5", "10", "20", "25", "30", "40", "50", "60", "70", "75", "80", "90",
    "95", "100",
]


# ---------------------------------------------------------------------------
# Variants
# ---------------------------------------------------------------------------

RESPONSIVE_VARIANTS: list[str] = ["sm", "md", "lg", "xl", "2xl"]
STATE_VARIANTS: list[str] = ["hover", "focus", "focus-visible", "disabled"]
DARK_VARIANT: list[str] = ["dark"]


# ---------------------------------------------------------------------------
# Helpers for building family class lists
# ---------------------------------------------------------------------------


def _join(prefix: str, scale: Iterable[str]) -> list[str]:
    """``prefix-value`` for every value in ``scale`` (``-0`` etc. allowed)."""
    return [f"{prefix}-{v}" for v in scale]


def _negatives(prefix: str, scale: Iterable[str]) -> list[str]:
    """Negative variants ``-prefix-value`` for non-zero numeric/fraction values."""
    out: list[str] = []
    for v in scale:
        if v in ("0", "px", "auto", "full", "screen", "min", "max", "fit", "none"):
            # px and 0 still get negatives in Tailwind for margins/inset; keep px.
            if v == "px":
                out.append(f"-{prefix}-px")
            continue
        out.append(f"-{prefix}-{v}")
    return out


# ---------------------------------------------------------------------------
# Structural family expansions
# ---------------------------------------------------------------------------


def spacing_classes() -> list[str]:
    classes: list[str] = []
    # Padding (no negatives).
    for p in ("p", "px", "py", "pt", "pr", "pb", "pl"):
        classes += _join(p, SPACING_SCALE)
    # Margin (with negatives + auto).
    for m in ("m", "mx", "my", "mt", "mr", "mb", "ml"):
        classes += _join(m, SPACING_SCALE + ["auto"])
        classes += _negatives(m, SPACING_SCALE)
    # Gap.
    for g in ("gap", "gap-x", "gap-y"):
        classes += _join(g, SPACING_SCALE)
    # Space-between (with negatives + reverse).
    for s in ("space-x", "space-y"):
        classes += _join(s, SPACING_SCALE)
        classes += _negatives(s, SPACING_SCALE)
        classes.append(f"{s}-reverse")
    return classes


def sizing_classes() -> list[str]:
    classes: list[str] = []
    wh_extra = ["full", "screen", "min", "max", "fit", "auto"]
    for prefix in ("w", "h"):
        classes += _join(prefix, SPACING_SCALE + FRACTIONS + wh_extra)
    for prefix in ("min-w", "min-h"):
        classes += _join(prefix, ["0", "full", "min", "max", "fit"])
    # max-w named + numeric subset; max-h numeric + named.
    classes += _join("max-w", MAX_W_NAMED)
    classes += _join("max-h", SPACING_SCALE + ["full", "screen", "min", "max", "fit", "none"])
    # size-* shorthand (Tailwind v4).
    classes += _join("size", SPACING_SCALE + FRACTIONS + ["full", "min", "max", "fit", "auto"])
    return classes


def layout_classes() -> list[str]:
    classes: list[str] = []
    # Display.
    classes += [
        "block", "inline-block", "inline", "flex", "inline-flex", "grid",
        "inline-grid", "hidden", "contents", "flow-root", "table",
        "inline-table", "table-row", "table-cell", "table-column",
        "table-caption", "table-header-group", "table-row-group",
        "table-footer-group", "list-item",
    ]
    # Flex direction / wrap.
    classes += [
        "flex-row", "flex-row-reverse", "flex-col", "flex-col-reverse",
        "flex-wrap", "flex-wrap-reverse", "flex-nowrap",
    ]
    # Flex grow/shrink/basis/flex.
    classes += ["flex-1", "flex-auto", "flex-initial", "flex-none"]
    classes += ["grow", "grow-0", "shrink", "shrink-0"]
    classes += _join("basis", SPACING_SCALE + FRACTIONS + ["auto", "full"])
    # Alignment.
    classes += [
        "items-start", "items-end", "items-center", "items-baseline",
        "items-stretch",
    ]
    classes += [
        "justify-start", "justify-end", "justify-center", "justify-between",
        "justify-around", "justify-evenly", "justify-stretch",
    ]
    classes += [
        "justify-items-start", "justify-items-end", "justify-items-center",
        "justify-items-stretch",
    ]
    classes += [
        "justify-self-auto", "justify-self-start", "justify-self-end",
        "justify-self-center", "justify-self-stretch",
    ]
    classes += [
        "content-start", "content-end", "content-center", "content-between",
        "content-around", "content-evenly", "content-stretch", "content-normal",
        "content-baseline",
    ]
    classes += [
        "self-auto", "self-start", "self-end", "self-center", "self-stretch",
        "self-baseline",
    ]
    classes += [
        "place-content-start", "place-content-end", "place-content-center",
        "place-content-between", "place-content-around", "place-content-evenly",
        "place-content-stretch", "place-content-baseline",
        "place-items-start", "place-items-end", "place-items-center",
        "place-items-stretch", "place-items-baseline",
        "place-self-auto", "place-self-start", "place-self-end",
        "place-self-center", "place-self-stretch",
    ]
    # Grid.
    classes += _join("grid-cols", GRID_COLS)
    classes += _join("grid-rows", GRID_ROWS)
    classes += _join("col-span", SPAN_SCALE) + ["col-auto"]
    classes += _join("row-span", ROW_SPAN_SCALE) + ["row-auto"]
    classes += _join("col-start", COL_START_END)
    classes += _join("col-end", COL_START_END)
    classes += _join("row-start", ROW_START_END)
    classes += _join("row-end", ROW_START_END)
    classes += [
        "grid-flow-row", "grid-flow-col", "grid-flow-dense",
        "grid-flow-row-dense", "grid-flow-col-dense",
    ]
    classes += _join("auto-cols", ["auto", "min", "max", "fr"])
    classes += _join("auto-rows", ["auto", "min", "max", "fr"])
    # Order.
    classes += _join("order", ORDER_SCALE)
    classes += _negatives("order", [str(n) for n in range(1, 13)])
    return classes


def position_classes() -> list[str]:
    classes: list[str] = []
    classes += ["static", "relative", "absolute", "fixed", "sticky"]
    inset_scale = SPACING_SCALE + FRACTIONS + ["auto", "full"]
    for prefix in ("inset", "inset-x", "inset-y", "top", "right", "bottom", "left", "start", "end"):
        classes += _join(prefix, inset_scale)
        classes += _negatives(prefix, SPACING_SCALE + FRACTIONS)
    classes += _join("z", Z_SCALE)
    classes += _negatives("z", ["10", "20", "30", "40", "50"])
    return classes


def typography_classes() -> list[str]:
    classes: list[str] = []
    classes += _join("text", FONT_SIZES)
    classes += _join("font", FONT_WEIGHTS)
    classes += ["italic", "not-italic"]
    classes += _join("leading", LEADING_SCALE)
    classes += _join("tracking", TRACKING_SCALE)
    classes += ["text-left", "text-center", "text-right", "text-justify", "text-start", "text-end"]
    classes += ["underline", "overline", "line-through", "no-underline"]
    classes += _join("decoration", ["solid", "double", "dotted", "dashed", "wavy"])
    classes += _join("decoration", ["auto", "from-font", "0", "1", "2", "4", "8"])
    classes += _join("underline-offset", ["auto", "0", "1", "2", "4", "8"])
    classes += ["uppercase", "lowercase", "capitalize", "normal-case"]
    classes += ["truncate", "text-ellipsis", "text-clip"]
    classes += _join("line-clamp", LINE_CLAMP)
    classes += [
        "whitespace-normal", "whitespace-nowrap", "whitespace-pre",
        "whitespace-pre-line", "whitespace-pre-wrap", "whitespace-break-spaces",
    ]
    classes += ["break-normal", "break-words", "break-all", "break-keep"]
    classes += [
        "list-none", "list-disc", "list-decimal", "list-inside", "list-outside",
    ]
    classes += _join("indent", SPACING_SCALE)
    classes += ["align-baseline", "align-top", "align-middle", "align-bottom",
                "align-text-top", "align-text-bottom", "align-sub", "align-super"]
    classes += _join("font", ["sans", "serif", "mono", "display"])
    return classes


def border_classes() -> list[str]:
    classes: list[str] = []
    # Border widths.
    classes += ["border"]
    classes += _join("border", BORDER_WIDTHS)
    for side in BORDER_SIDES:
        classes.append(f"border-{side}")
        classes += [f"border-{side}-{w}" for w in BORDER_WIDTHS]
    # Border style.
    classes += _join("border", ["solid", "dashed", "dotted", "double", "hidden", "none"])
    # Radius.
    classes += ["rounded"]
    classes += _join("rounded", RADIUS_SIZES)
    for side in RADIUS_SIDES:
        classes.append(f"rounded-{side}")
        classes += [f"rounded-{side}-{sz}" for sz in RADIUS_SIZES]
    # Ring.
    classes += ["ring", "ring-inset"]
    classes += _join("ring", RING_WIDTHS)
    classes += _join("ring-offset", RING_OFFSET_WIDTHS)
    # Divide.
    for d in ("divide-x", "divide-y"):
        classes.append(d)
        classes += [f"{d}-{w}" for w in DIVIDE_WIDTHS]
        classes.append(f"{d}-reverse")
    classes += _join("divide", ["solid", "dashed", "dotted", "double", "none"])
    # Outline.
    classes += ["outline", "outline-none"]
    classes += _join("outline", OUTLINE_WIDTHS)
    classes += _join("outline", ["solid", "dashed", "dotted", "double"])
    classes += _join("outline-offset", ["0", "1", "2", "4", "8"])
    return classes


def effect_classes() -> list[str]:
    classes: list[str] = []
    classes += ["shadow"]
    classes += _join("shadow", SHADOW_SIZES)
    classes += _join("opacity", OPACITY_SCALE)
    classes += _join("mix-blend", ["normal", "multiply", "screen", "overlay", "darken", "lighten"])
    return classes


def transition_transform_classes() -> list[str]:
    classes: list[str] = []
    classes += [
        "transition", "transition-none", "transition-all", "transition-colors",
        "transition-opacity", "transition-shadow", "transition-transform",
    ]
    classes += _join("duration", DURATION_SCALE)
    classes += _join("ease", EASE_SCALE)
    classes += _join("delay", DURATION_SCALE)
    classes += ["animate-none", "animate-spin", "animate-ping", "animate-pulse", "animate-bounce"]
    # Transform enable + origins.
    classes += ["transform", "transform-none", "transform-gpu"]
    classes += _join("origin", [
        "center", "top", "top-right", "right", "bottom-right", "bottom",
        "bottom-left", "left", "top-left",
    ])
    # Scale.
    classes += _join("scale", SCALE_SCALE)
    classes += _join("scale-x", SCALE_SCALE)
    classes += _join("scale-y", SCALE_SCALE)
    # Rotate (with negatives).
    classes += _join("rotate", ROTATE_SCALE)
    classes += _negatives("rotate", [v for v in ROTATE_SCALE if v != "0"])
    # Translate (with negatives, fractions, full).
    translate_scale = SPACING_SCALE + ["1/2", "1/3", "2/3", "1/4", "2/4", "3/4", "full"]
    for prefix in ("translate-x", "translate-y"):
        classes += _join(prefix, translate_scale)
        classes += _negatives(prefix, SPACING_SCALE + ["1/2", "1/3", "2/3", "1/4", "2/4", "3/4", "full"])
    # Skew.
    for prefix in ("skew-x", "skew-y"):
        classes += _join(prefix, ["0", "1", "2", "3", "6", "12"])
        classes += _negatives(prefix, ["1", "2", "3", "6", "12"])
    return classes


def misc_classes() -> list[str]:
    classes: list[str] = []
    for prefix in ("overflow", "overflow-x", "overflow-y"):
        classes += _join(prefix, ["auto", "hidden", "clip", "visible", "scroll"])
    classes += _join("object", ["contain", "cover", "fill", "none", "scale-down"])
    classes += _join("object", [
        "bottom", "center", "left", "left-bottom", "left-top", "right",
        "right-bottom", "right-top", "top",
    ])
    classes += _join("aspect", ASPECT)
    classes += _join("cursor", [
        "auto", "default", "pointer", "wait", "text", "move", "help",
        "not-allowed", "none", "progress", "grab", "grabbing",
    ])
    classes += _join("select", ["none", "text", "all", "auto"])
    classes += _join("pointer-events", ["none", "auto"])
    classes += _join("visibility", [])  # placeholder; visibility utilities below
    classes += ["visible", "invisible", "collapse"]
    classes += _join("isolation", [])  # placeholder
    classes += ["isolate", "isolation-auto"]
    classes += _join("box", ["border", "content"])
    classes += _join("scroll", ["auto", "smooth"])  # scroll-behavior
    classes += _join("whitespace", [])  # placeholder (handled in typography)
    classes += _join("resize", ["none", "y", "x"]) + ["resize"]
    return [c for c in classes if not c.endswith("-")]  # drop placeholder stubs


def color_classes() -> list[str]:
    """RESTRICTED color utilities: semantic tokens + gray only, with opacity."""
    classes: list[str] = []
    for prefix in COLOR_PREFIXES:
        for name in COLOR_NAMES:
            base = f"{prefix}-{name}"
            classes.append(base)
            for op in COLOR_OPACITY_STEPS:
                classes.append(f"{base}/{op}")
    return classes


# ---------------------------------------------------------------------------
# Assembly + variant expansion
# ---------------------------------------------------------------------------


def structural_classes() -> list[str]:
    """All non-color structural utilities (base, unprefixed)."""
    classes: list[str] = []
    classes += spacing_classes()
    classes += sizing_classes()
    classes += layout_classes()
    classes += position_classes()
    classes += typography_classes()
    classes += border_classes()
    classes += effect_classes()
    classes += transition_transform_classes()
    classes += misc_classes()
    return classes


def _dedupe_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out


def build_safelist() -> list[str]:
    """Expand every family + variant into the flat, sorted list of class names."""
    structural = _dedupe_preserve_order(structural_classes())
    color = _dedupe_preserve_order(color_classes())

    final: list[str] = []

    # Base (unprefixed) forms — always emitted.
    final += structural
    final += color

    # Responsive variants across structural + color.
    for v in RESPONSIVE_VARIANTS:
        final += [f"{v}:{c}" for c in structural]
        final += [f"{v}:{c}" for c in color]

    # State variants + dark: applied to color utilities (and structural where
    # sensible — we apply across the structural set too, which is harmless and
    # gives agencies full reach).
    for v in STATE_VARIANTS + DARK_VARIANT:
        final += [f"{v}:{c}" for c in color]
        final += [f"{v}:{c}" for c in structural]

    return _dedupe_preserve_order(final)


def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.normpath(
        os.path.join(here, "..", "tsigma", "static", "css", "safelist.txt")
    )
    classes = build_safelist()
    # Sort for a stable, diff-friendly committed artifact.
    classes_sorted = sorted(classes)
    with open(out_path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(classes_sorted))
        fh.write("\n")
    print(f"Wrote {len(classes_sorted)} classes to {out_path}")


if __name__ == "__main__":
    main()
