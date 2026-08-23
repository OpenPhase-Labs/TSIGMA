"""Unit tests for theme static-asset resolution (no DB/network)."""

from tsigma.theming.static import resolve_static_dirs, resolve_static_file


def _write(p, name, body=b"x"):
    f = p / name
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_bytes(body)
    return f


def test_serves_theme_asset_when_present(tmp_path):
    theme_s = tmp_path / "theme"
    default_s = tmp_path / "default"
    _write(theme_s, "img/logo.svg", b"THEME")
    _write(default_s, "img/logo.svg", b"DEFAULT")
    got = resolve_static_file("img/logo.svg", [theme_s, default_s])
    assert got is not None and got.read_bytes() == b"THEME"


def test_falls_back_to_default_when_not_overridden(tmp_path):
    theme_s = tmp_path / "theme"
    default_s = tmp_path / "default"
    theme_s.mkdir()
    _write(default_s, "css/custom.css", b"DEFAULT")
    got = resolve_static_file("css/custom.css", [theme_s, default_s])
    assert got is not None and got.read_bytes() == b"DEFAULT"


def test_missing_everywhere_returns_none(tmp_path):
    theme_s = tmp_path / "theme"
    default_s = tmp_path / "default"
    theme_s.mkdir()
    default_s.mkdir()
    assert resolve_static_file("nope.png", [theme_s, default_s]) is None


def test_path_traversal_rejected(tmp_path):
    default_s = tmp_path / "default"
    _write(default_s, "ok.txt", b"OK")
    assert resolve_static_file("../ok.txt", [default_s]) is None
    assert resolve_static_file("/etc/passwd", [default_s]) is None


def test_resolve_static_dirs_default_only(tmp_path):
    default_s = tmp_path / "default" / "static"
    default_s.mkdir(parents=True)
    assert resolve_static_dirs(
        "default", themes_root=tmp_path / "themes", default_static=default_s
    ) == [default_s]


def test_resolve_static_dirs_unknown_theme_falls_back(tmp_path):
    default_s = tmp_path / "default" / "static"
    default_s.mkdir(parents=True)
    dirs = resolve_static_dirs(
        "ghost", themes_root=tmp_path / "themes", default_static=default_s
    )
    assert dirs == [default_s]
