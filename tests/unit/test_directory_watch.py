"""
Unit tests for directory watch ingestion method plugin.

Covers the new contract: Layer-2 server config (paths, patterns,
decoder default) sourced from process env via ``ListenerService``;
per-device decoder overrides come from the orchestrator ``devices``
argument; events persisted through the ``IngestionTarget``.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tsigma.collection.methods.directory_watch import (
    DirectoryWatchMethod,
    DirectoryWatchServerConfig,
    _FileEventHandler,
)
from tsigma.collection.registry import (
    EventDrivenIngestionMethod,
    ExecutionMode,
    IngestionMethodRegistry,
)

_MOD = "tsigma.collection.methods.directory_watch"


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_registered(self):
        assert IngestionMethodRegistry.get("directory_watch") is DirectoryWatchMethod

    def test_execution_mode(self):
        assert DirectoryWatchMethod.execution_mode is ExecutionMode.EVENT_DRIVEN

    def test_is_event_driven_subclass(self):
        assert issubclass(DirectoryWatchMethod, EventDrivenIngestionMethod)

    def test_name(self):
        assert DirectoryWatchMethod.name == "directory_watch"


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_not_running(self):
        method = DirectoryWatchMethod()
        assert await method.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_observer_alive(self):
        method = DirectoryWatchMethod()
        observer = MagicMock()
        observer.is_alive.return_value = True
        method._observers = [observer]
        assert await method.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_observer_dead(self):
        method = DirectoryWatchMethod()
        observer = MagicMock()
        observer.is_alive.return_value = False
        method._observers = [observer]
        assert await method.health_check() is False


# ---------------------------------------------------------------------------
# Server config build
# ---------------------------------------------------------------------------


class TestBuildServerConfig:
    def test_minimal_config(self, tmp_path):
        cfg = DirectoryWatchMethod._build_server_config(
            {"paths": [str(tmp_path)]},
        )
        assert cfg.paths == [str(tmp_path.resolve())]
        assert cfg.patterns == ["*.dat", "*.csv", "*.DAT", "*.CSV"]
        assert cfg.decoder == "auto"
        assert cfg.move_after_processing is True
        assert cfg.processed_subdir == "processed"
        assert cfg.error_subdir == "errors"
        assert cfg.recursive is False

    def test_no_paths_raises(self):
        with pytest.raises(ValueError, match="requires at least one path"):
            DirectoryWatchMethod._build_server_config({})

    def test_empty_paths_raises(self):
        with pytest.raises(ValueError, match="requires at least one path"):
            DirectoryWatchMethod._build_server_config({"paths": []})

    def test_custom_patterns_and_decoder(self, tmp_path):
        cfg = DirectoryWatchMethod._build_server_config({
            "paths": [str(tmp_path)],
            "patterns": ["*.bin"],
            "decoder": "asc3",
            "recursive": True,
            "move_after_processing": False,
            "processed_subdir": "done",
            "error_subdir": "failed",
        })
        assert cfg.patterns == ["*.bin"]
        assert cfg.decoder == "asc3"
        assert cfg.recursive is True
        assert cfg.move_after_processing is False
        assert cfg.processed_subdir == "done"
        assert cfg.error_subdir == "failed"

    def test_paths_resolved_to_absolute(self, tmp_path, monkeypatch):
        # cd to tmp_path so a relative path resolves predictably.
        monkeypatch.chdir(tmp_path)
        cfg = DirectoryWatchMethod._build_server_config({"paths": ["."]})
        assert Path(cfg.paths[0]).is_absolute()


# ---------------------------------------------------------------------------
# Filename → device_id resolution
# ---------------------------------------------------------------------------


class TestResolveDeviceId:
    def test_infer_from_filename_with_underscore(self):
        method = DirectoryWatchMethod()
        result = method._resolve_device_id("gdot-0142_20260415_events.dat")
        assert result == "gdot-0142"

    def test_no_underscore_returns_none(self):
        method = DirectoryWatchMethod()
        assert method._resolve_device_id("randomname.dat") is None


# ---------------------------------------------------------------------------
# Pattern matching via _FileEventHandler
# ---------------------------------------------------------------------------


class TestFilePatternMatching:
    def _handler(self, patterns):
        cfg = DirectoryWatchServerConfig(paths=["/tmp"], patterns=patterns)
        return _FileEventHandler(
            "/tmp", cfg, MagicMock(), MagicMock(),
        )

    def test_matches_dat(self):
        assert self._handler(["*.dat"])._matches_patterns("file.dat") is True

    def test_matches_csv(self):
        assert self._handler(["*.csv"])._matches_patterns("file.csv") is True

    def test_matches_uppercase(self):
        assert (
            self._handler(["*.DAT"])._matches_patterns("FILE.DAT") is True
        )

    def test_rejects_unmatched_extension(self):
        assert (
            self._handler(["*.dat"])._matches_patterns("file.txt") is False
        )

    def test_rejects_no_extension(self):
        assert (
            self._handler(["*.dat"])._matches_patterns("noext") is False
        )

    def test_custom_patterns(self):
        assert (
            self._handler(["controller_*.bin"])
            ._matches_patterns("controller_42.bin")
            is True
        )


# ---------------------------------------------------------------------------
# _FileEventHandler scheduling
# ---------------------------------------------------------------------------


class TestStop:
    @pytest.mark.asyncio
    async def test_stop_when_not_started(self):
        method = DirectoryWatchMethod()
        # Should not raise.
        await method.stop()

    @pytest.mark.asyncio
    async def test_stop_stops_all_observers(self):
        method = DirectoryWatchMethod()
        observer1 = MagicMock()
        observer2 = MagicMock()
        method._observers = [observer1, observer2]
        method._cfg = DirectoryWatchServerConfig(paths=["/tmp"])

        await method.stop()

        observer1.stop.assert_called_once()
        observer1.join.assert_called_once_with(timeout=5.0)
        observer2.stop.assert_called_once()
        assert method._observers == []


# ---------------------------------------------------------------------------
# Start lifecycle
# ---------------------------------------------------------------------------


class TestMoveOps:
    @patch(f"{_MOD}.shutil.move")
    def test_move_to_processed_creates_dir(self, mock_move, tmp_path):
        method = DirectoryWatchMethod()
        method._cfg = DirectoryWatchServerConfig(
            paths=[str(tmp_path)], processed_subdir="processed",
        )
        f = tmp_path / "x.dat"
        f.write_bytes(b"")
        method._move_to_processed(f, str(tmp_path))
        assert (tmp_path / "processed").is_dir()
        mock_move.assert_called_once()

    @patch(f"{_MOD}.shutil.move", side_effect=OSError("move failed"))
    def test_move_to_processed_handles_error(self, mock_move, tmp_path, caplog):
        import logging
        method = DirectoryWatchMethod()
        method._cfg = DirectoryWatchServerConfig(
            paths=[str(tmp_path)], processed_subdir="processed",
        )
        f = tmp_path / "x.dat"
        f.write_bytes(b"")
        with caplog.at_level(logging.ERROR, logger=_MOD):
            method._move_to_processed(f, str(tmp_path))
        assert "failed to move" in caplog.text.lower()

    @patch(f"{_MOD}.shutil.move")
    def test_move_to_error_creates_dir(self, mock_move, tmp_path):
        method = DirectoryWatchMethod()
        method._cfg = DirectoryWatchServerConfig(
            paths=[str(tmp_path)], error_subdir="errors",
        )
        f = tmp_path / "x.dat"
        f.write_bytes(b"")
        method._move_to_error(f, str(tmp_path))
        assert (tmp_path / "errors").is_dir()
        mock_move.assert_called_once()
