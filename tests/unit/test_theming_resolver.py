"""Unit tests for theme template resolution (no DB/network)."""

import jinja2
import pytest

from tsigma.theming.resolver import (
    build_template_loader,
    resolve_template_dirs,
)


def _write(p, name, body):
    f = p / name
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(body, encoding="utf-8")
    return f


def test_default_theme_uses_only_default_dir(tmp_path):
    default_t = tmp_path / "default" / "templates"
    default_t.mkdir(parents=True)
    dirs = resolve_template_dirs(
        "default", themes_root=tmp_path / "themes", default_templates=default_t
    )
    assert dirs == [default_t]


def test_named_theme_prepends_theme_dir(tmp_path):
    default_t = tmp_path / "default" / "templates"
    default_t.mkdir(parents=True)
    theme_t = tmp_path / "themes" / "acme" / "templates"
    theme_t.mkdir(parents=True)
    dirs = resolve_template_dirs(
        "acme", themes_root=tmp_path / "themes", default_templates=default_t
    )
    assert dirs == [theme_t, default_t]


def test_unknown_theme_falls_back_to_default(tmp_path):
    default_t = tmp_path / "default" / "templates"
    default_t.mkdir(parents=True)
    dirs = resolve_template_dirs(
        "ghost", themes_root=tmp_path / "themes", default_templates=default_t
    )
    assert dirs == [default_t]


def test_unknown_theme_logs_warning(tmp_path, caplog):
    default_t = tmp_path / "default" / "templates"
    default_t.mkdir(parents=True)
    with caplog.at_level("WARNING"):
        resolve_template_dirs(
            "ghost", themes_root=tmp_path / "themes", default_templates=default_t
        )
    assert any("ghost" in r.message for r in caplog.records)


def test_theme_template_shadows_default(tmp_path):
    default_t = tmp_path / "default" / "templates"
    theme_t = tmp_path / "themes" / "acme" / "templates"
    _write(default_t, "base.html", "DEFAULT-BASE")
    _write(default_t, "only_default.html", "ONLY-DEFAULT")
    _write(theme_t, "base.html", "THEME-BASE")

    loader = build_template_loader([theme_t, default_t])
    env = jinja2.Environment(loader=loader)

    # Overridden template resolves to the theme copy
    assert env.get_template("base.html").render() == "THEME-BASE"
    # Un-overridden template falls through to default
    assert env.get_template("only_default.html").render() == "ONLY-DEFAULT"


def test_unknown_template_raises_template_not_found(tmp_path):
    default_t = tmp_path / "default" / "templates"
    default_t.mkdir(parents=True)
    loader = build_template_loader([default_t])
    env = jinja2.Environment(loader=loader)
    with pytest.raises(jinja2.TemplateNotFound):
        env.get_template("does_not_exist.html")
