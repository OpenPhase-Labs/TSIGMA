"""Resolve the active theme into Jinja template-search layers.

A theme is a directory ``themes/<name>/`` whose ``templates/`` shadow the
default templates. ``"default"`` means "no override layer" — the built-in
``tsigma/templates`` is the only (fallback) layer. Single-agency: the active
theme is resolved once at startup.
"""

import logging
from pathlib import Path

import jinja2

logger = logging.getLogger(__name__)

DEFAULT_THEME = "default"


def resolve_template_dirs(
    theme: str,
    *,
    themes_root: Path,
    default_templates: Path,
) -> list[Path]:
    """Return template search dirs, theme-first, default last.

    - ``default`` / empty → ``[default_templates]``.
    - named + present     → ``[<themes_root>/<theme>/templates, default_templates]``.
    - named + absent      → log WARNING and fall back to ``[default_templates]``.
    """
    if not theme or theme == DEFAULT_THEME:
        return [default_templates]
    theme_templates = Path(themes_root) / theme / "templates"
    if not theme_templates.is_dir():
        logger.warning(
            "Theme '%s' has no templates dir at %s; falling back to default theme",
            theme,
            theme_templates,
        )
        return [default_templates]
    return [theme_templates, default_templates]


def build_template_loader(template_dirs: list[Path]) -> jinja2.ChoiceLoader:
    """ChoiceLoader over the search dirs; earlier dirs shadow later ones."""
    return jinja2.ChoiceLoader(
        [jinja2.FileSystemLoader(str(d)) for d in template_dirs]
    )
