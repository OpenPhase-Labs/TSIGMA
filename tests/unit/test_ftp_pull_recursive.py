"""
Unit tests for recursive directory listing in the FTP/FTPS/SFTP clients.

Phase A: ``_AioFTPClient.list_dir`` and ``_AsyncSSHClient.list_dir`` gain
``recursive``, ``max_depth`` and ``follow_symlinks`` parameters.

Behavior under test:
1. Backward-compat (recursive=False, the default) returns ONLY regular files
   directly in ``path`` with BARE filenames.
2. recursive=True descends subdirectories and returns nested files with
   names that are the relative subpath from the listed root (POSIX ``/``).
3. ``max_depth`` caps descent (fail-closed: simply does not descend past it).
4. ``follow_symlinks`` (default False) controls whether symlinked directories
   are descended.
5. Symlink cycles do not cause infinite recursion when follow_symlinks=True.
6. Both client classes (FTP/FTPS via _AioFTPClient, SFTP via _AsyncSSHClient).
"""

from pathlib import PurePosixPath
from unittest.mock import AsyncMock, MagicMock

import pytest

from tsigma.collection.methods.ftp_pull import (
    FTPProtocol,
    FTPPullConfig,
    _AioFTPClient,
    _AsyncSSHClient,
)

# ---------------------------------------------------------------------------
# Helpers - config builders
# ---------------------------------------------------------------------------


def _ftp_config(protocol=FTPProtocol.FTP, **kw):
    return FTPPullConfig(
        host="192.168.1.1", signal_id="SIG-001", protocol=protocol, **kw,
    )


def _sftp_config(**kw):
    return FTPPullConfig(
        host="192.168.1.1", signal_id="SIG-001",
        protocol=FTPProtocol.SFTP, **kw,
    )


# ---------------------------------------------------------------------------
# Helpers - _AioFTPClient mock tree
#
# aioftp's ``ctx.list(path)`` is an async generator yielding
# ``(item_path, info)`` tuples.  ``info["type"]`` is a string:
# "file" / "dir" / "link".  To support recursion the mock dispatches on the
# requested path, returning that directory's direct children.
# ---------------------------------------------------------------------------

def _install_aioftp_tree(client, tree):
    """Wire ``client._ctx.list`` to dispatch by path against ``tree``.

    ``tree`` maps an absolute POSIX dir path -> list of ``(name, info)`` where
    ``info`` is a dict with at least ``"type"`` and optional ``"size"``.
    The returned ``item_path`` for each child is the joined absolute path,
    mirroring how aioftp reports full paths that production strips via
    ``PurePosixPath(item_path).name``.
    """

    def list_factory(path):
        key = str(path)
        children = tree.get(key, [])

        async def _gen():
            for name, info in children:
                child_path = PurePosixPath(key) / name
                yield (child_path, info)

        return _gen()

    client._ctx = MagicMock()
    client._ctx.list = MagicMock(side_effect=list_factory)


def _file_info(size=100):
    return {"type": "file", "size": str(size)}


def _dir_info():
    return {"type": "dir"}


def _link_info():
    return {"type": "link"}


# ---------------------------------------------------------------------------
# Helpers - _AsyncSSHClient mock tree
#
# asyncssh's ``sftp.readdir(path)`` returns a list of entries, each with
# ``.filename`` and ``.attrs`` (``.type``: 1=file, 2=dir, 3=symlink;
# ``.size``; ``.mtime``).  The mock dispatches on the requested path.
# ---------------------------------------------------------------------------

def _sftp_entry(filename, type_, size=100, mtime=None):
    entry = MagicMock()
    entry.filename = filename
    entry.attrs.type = type_
    entry.attrs.size = size
    entry.attrs.mtime = mtime
    return entry


def _install_sftp_tree(client, tree):
    """Wire ``client._sftp.readdir`` to dispatch by path against ``tree``.

    ``tree`` maps an absolute POSIX dir path -> list of entries (built with
    ``_sftp_entry``).  Recursion descends by calling readdir on the child's
    absolute path.
    """

    async def readdir(path):
        return list(tree.get(str(path), []))

    client._sftp = MagicMock()
    client._sftp.readdir = AsyncMock(side_effect=readdir)
    # realpath is used for cycle detection: identity mapping unless overridden
    client._sftp.realpath = AsyncMock(side_effect=lambda p: str(p))


def _names(result):
    return sorted(rf.name for rf in result)


# ===========================================================================
# _AioFTPClient (FTP / FTPS)
# ===========================================================================


class TestAioFTPClientRecursive:
    """Recursive list_dir behavior for the FTP/FTPS client."""

    @pytest.mark.asyncio
    async def test_non_recursive_default_bare_names_files_only(self):
        """recursive=False (default): only direct regular files, bare names."""
        client = _AioFTPClient(_ftp_config())
        _install_aioftp_tree(client, {
            "/data": [
                ("root.dat", _file_info()),
                ("sram", _dir_info()),
                ("link_dir", _link_info()),
            ],
            "/data/sram": [
                ("nested.dat", _file_info()),
            ],
        })

        result = await client.list_dir("/data")

        assert _names(result) == ["root.dat"]
        assert result[0].name == "root.dat"
        assert result[0].size == 100

    @pytest.mark.asyncio
    async def test_recursive_returns_relative_subpath_names(self):
        """recursive=True: nested files returned as relative subpath names."""
        client = _AioFTPClient(_ftp_config())
        _install_aioftp_tree(client, {
            "/data": [
                ("root.dat", _file_info()),
                ("sram", _dir_info()),
            ],
            "/data/sram": [
                ("f.dat", _file_info()),
                ("deep", _dir_info()),
            ],
            "/data/sram/deep": [
                ("g.dat", _file_info()),
            ],
        })

        result = await client.list_dir("/data", recursive=True)

        assert _names(result) == ["root.dat", "sram/deep/g.dat", "sram/f.dat"]

    @pytest.mark.asyncio
    async def test_recursive_max_depth_bound(self):
        """max_depth caps descent; deeper files are excluded (fail-closed)."""
        client = _AioFTPClient(_ftp_config())
        _install_aioftp_tree(client, {
            "/data": [
                ("root.dat", _file_info()),
                ("lvl1", _dir_info()),
            ],
            "/data/lvl1": [
                ("a.dat", _file_info()),
                ("lvl2", _dir_info()),
            ],
            "/data/lvl1/lvl2": [
                ("b.dat", _file_info()),
            ],
        })

        result = await client.list_dir("/data", recursive=True, max_depth=1)

        # root + one level down included; two levels deep excluded
        assert _names(result) == ["lvl1/a.dat", "root.dat"]

    @pytest.mark.asyncio
    async def test_recursive_no_follow_symlinks_default(self):
        """follow_symlinks=False (default): symlinked dirs are not descended."""
        client = _AioFTPClient(_ftp_config())
        _install_aioftp_tree(client, {
            "/data": [
                ("root.dat", _file_info()),
                ("ln", _link_info()),
            ],
            "/data/ln": [
                ("hidden.dat", _file_info()),
            ],
        })

        result = await client.list_dir("/data", recursive=True)

        assert _names(result) == ["root.dat"]

    @pytest.mark.asyncio
    async def test_recursive_follow_symlinks_true_descends(self):
        """follow_symlinks=True: symlinked dirs ARE descended."""
        client = _AioFTPClient(_ftp_config())
        _install_aioftp_tree(client, {
            "/data": [
                ("root.dat", _file_info()),
                ("ln", _link_info()),
            ],
            "/data/ln": [
                ("inside.dat", _file_info()),
            ],
        })

        result = await client.list_dir(
            "/data", recursive=True, follow_symlinks=True,
        )

        assert "root.dat" in _names(result)
        assert "ln/inside.dat" in _names(result)

    @pytest.mark.asyncio
    async def test_recursive_symlink_cycle_bounded_by_max_depth(self):
        """FTP symlink cycle is bounded by max_depth (FTP has no realpath).

        Unlike SFTP, aioftp exposes no ``realpath``, so there is no way to
        detect a true filesystem cycle by identity -- cycle safety for FTP
        rests SOLELY on ``max_depth``.  This test therefore verifies the
        max_depth bound (not a nonexistent FTP cycle-dedup): a cyclic-looking
        ``loop`` symlink that always lists the same contents must yield
        exactly the depth-bounded set and no deeper, proving traversal
        terminates by the cap.
        """
        client = _AioFTPClient(_ftp_config())
        # /data/loop is a symlink whose listing mirrors its parent (a cycle).
        _install_aioftp_tree(client, {
            "/data": [
                ("root.dat", _file_info()),
                ("loop", _link_info()),
            ],
            "/data/loop": [
                ("root.dat", _file_info()),
                ("loop", _link_info()),
            ],
            "/data/loop/loop": [
                ("root.dat", _file_info()),
                ("loop", _link_info()),
            ],
            "/data/loop/loop/loop": [
                ("root.dat", _file_info()),
                ("loop", _link_info()),
            ],
        })

        result = await client.list_dir(
            "/data", recursive=True, follow_symlinks=True, max_depth=2,
        )

        # max_depth=2: root (depth 0), loop/ (depth 1), loop/loop/ (depth 2);
        # the depth-3 loop/loop/loop/ dir is NOT descended -> exact bounded set.
        assert sorted(_names(result)) == [
            "loop/loop/root.dat",
            "loop/root.dat",
            "root.dat",
        ]


# ===========================================================================
# _AsyncSSHClient (SFTP)
# ===========================================================================


class TestAsyncSSHClientRecursive:
    """Recursive list_dir behavior for the SFTP client."""

    @pytest.mark.asyncio
    async def test_non_recursive_default_bare_names_files_only(self):
        """recursive=False (default): only direct regular files, bare names."""
        client = _AsyncSSHClient(_sftp_config())
        _install_sftp_tree(client, {
            "/data": [
                _sftp_entry("root.dat", 1, size=100),
                _sftp_entry("sram", 2),
                _sftp_entry("link_dir", 3),
            ],
            "/data/sram": [
                _sftp_entry("nested.dat", 1),
            ],
        })

        result = await client.list_dir("/data")

        assert _names(result) == ["root.dat"]
        assert result[0].name == "root.dat"
        assert result[0].size == 100

    @pytest.mark.asyncio
    async def test_recursive_returns_relative_subpath_names(self):
        """recursive=True: nested files returned as relative subpath names."""
        client = _AsyncSSHClient(_sftp_config())
        _install_sftp_tree(client, {
            "/data": [
                _sftp_entry("root.dat", 1),
                _sftp_entry("sram", 2),
            ],
            "/data/sram": [
                _sftp_entry("f.dat", 1),
                _sftp_entry("deep", 2),
            ],
            "/data/sram/deep": [
                _sftp_entry("g.dat", 1),
            ],
        })

        result = await client.list_dir("/data", recursive=True)

        assert _names(result) == ["root.dat", "sram/deep/g.dat", "sram/f.dat"]

    @pytest.mark.asyncio
    async def test_recursive_max_depth_bound(self):
        """max_depth caps descent; deeper files are excluded (fail-closed)."""
        client = _AsyncSSHClient(_sftp_config())
        _install_sftp_tree(client, {
            "/data": [
                _sftp_entry("root.dat", 1),
                _sftp_entry("lvl1", 2),
            ],
            "/data/lvl1": [
                _sftp_entry("a.dat", 1),
                _sftp_entry("lvl2", 2),
            ],
            "/data/lvl1/lvl2": [
                _sftp_entry("b.dat", 1),
            ],
        })

        result = await client.list_dir("/data", recursive=True, max_depth=1)

        assert _names(result) == ["lvl1/a.dat", "root.dat"]

    @pytest.mark.asyncio
    async def test_recursive_no_follow_symlinks_default(self):
        """follow_symlinks=False (default): symlinked dirs are not descended."""
        client = _AsyncSSHClient(_sftp_config())
        _install_sftp_tree(client, {
            "/data": [
                _sftp_entry("root.dat", 1),
                _sftp_entry("ln", 3),  # symlink
            ],
            "/data/ln": [
                _sftp_entry("hidden.dat", 1),
            ],
        })

        result = await client.list_dir("/data", recursive=True)

        assert _names(result) == ["root.dat"]

    @pytest.mark.asyncio
    async def test_recursive_follow_symlinks_true_descends(self):
        """follow_symlinks=True: symlinked dirs ARE descended."""
        client = _AsyncSSHClient(_sftp_config())
        _install_sftp_tree(client, {
            "/data": [
                _sftp_entry("root.dat", 1),
                _sftp_entry("ln", 3),  # symlink to a dir
            ],
            "/data/ln": [
                _sftp_entry("inside.dat", 1),
            ],
        })

        result = await client.list_dir(
            "/data", recursive=True, follow_symlinks=True,
        )

        assert "root.dat" in _names(result)
        assert "ln/inside.dat" in _names(result)

    @pytest.mark.asyncio
    async def test_recursive_symlink_cycle_dedup_via_realpath(self):
        """follow_symlinks=True: a realpath cycle is detected and pruned.

        The symlink ``/data/loop`` resolves (via ``sftp.realpath``) back to
        the already-visited root ``/data``.  The dedup check must skip it, so
        the file that lives only behind the loop symlink (``extra.dat``) must
        NOT appear.  If the ``if real in visited`` check were removed, the
        symlink would be descended and ``loop/extra.dat`` (and deeper
        ``loop/loop/...`` copies) would leak into the result -- so asserting
        the EXACT set fails if dedup is deleted.
        """
        client = _AsyncSSHClient(_sftp_config())
        _install_sftp_tree(client, {
            "/data": [
                _sftp_entry("root.dat", 1),
                _sftp_entry("loop", 3),  # symlink, realpath -> /data (cycle)
            ],
            # Listing behind the loop symlink. Contains a UNIQUE file that
            # only surfaces if the cycle is (incorrectly) descended.
            "/data/loop": [
                _sftp_entry("root.dat", 1),
                _sftp_entry("extra.dat", 1),
                _sftp_entry("loop", 3),
            ],
        })
        # /data resolves to itself; the loop symlink resolves back to /data.
        client._sftp.realpath = AsyncMock(
            side_effect=lambda p: "/data" if str(p) == "/data/loop" else str(p)
        )

        result = await client.list_dir(
            "/data", recursive=True, follow_symlinks=True,
        )

        # Only the root file: the cycle (and the unique extra.dat behind it)
        # is pruned by realpath-based dedup.
        assert sorted(_names(result)) == ["root.dat"]
