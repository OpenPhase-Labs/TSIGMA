"""
Unit tests for directory watch ingestion method plugin.

Covers the new contract: Layer-2 server config (paths, patterns,
decoder default) sourced from process env via ``ListenerService``;
per-device decoder overrides come from the orchestrator ``devices``
argument; events persisted through the ``IngestionTarget``.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.decoders.base import DecodedEvent
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
from tsigma.collection.targets import ControllerTarget

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


class TestFileEventHandlerScheduling:
    def _setup(self):
        cfg = DirectoryWatchServerConfig(
            paths=["/tmp"], patterns=["*.dat"],
        )
        method = MagicMock()
        loop = MagicMock()
        return _FileEventHandler("/tmp", cfg, loop, method)

    @patch(f"{_MOD}.asyncio.run_coroutine_threadsafe")
    def test_on_created_schedules_matching_file(self, mock_rcts):
        h = self._setup()
        event = MagicMock()
        event.is_directory = False
        event.src_path = "/tmp/x.dat"
        h.on_created(event)
        mock_rcts.assert_called_once()

    @patch(f"{_MOD}.asyncio.run_coroutine_threadsafe")
    def test_on_created_skips_directory(self, mock_rcts):
        h = self._setup()
        event = MagicMock()
        event.is_directory = True
        h.on_created(event)
        mock_rcts.assert_not_called()

    @patch(f"{_MOD}.asyncio.run_coroutine_threadsafe")
    def test_on_created_skips_non_matching(self, mock_rcts):
        h = self._setup()
        event = MagicMock()
        event.is_directory = False
        event.src_path = "/tmp/note.txt"
        h.on_created(event)
        mock_rcts.assert_not_called()

    @patch(f"{_MOD}.asyncio.run_coroutine_threadsafe")
    def test_on_moved_schedules_matching_file(self, mock_rcts):
        h = self._setup()
        event = MagicMock()
        event.is_directory = False
        event.dest_path = "/tmp/y.dat"
        h.on_moved(event)
        mock_rcts.assert_called_once()


# ---------------------------------------------------------------------------
# _process_file end-to-end
# ---------------------------------------------------------------------------


class TestProcessFile:
    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_successful_processing(self, mock_sleep, tmp_path):
        method = DirectoryWatchMethod()
        method._cfg = DirectoryWatchServerConfig(
            paths=[str(tmp_path)], decoder="asc3",
            move_after_processing=False,
        )
        method._target = ControllerTarget()
        method._target.persist = AsyncMock()
        method._session_factory = AsyncMock()
        method._device_overrides = {}

        # Real file the method can read.
        f = tmp_path / "SIG-001_20260415_events.dat"
        f.write_bytes(b"\x01\x02\x03")

        decoder_cls = MagicMock()
        decoder_inst = MagicMock()
        decoder_inst.decode_bytes.return_value = [
            DecodedEvent(timestamp=None, event_code=82, event_param=1),
        ]
        decoder_cls.return_value = decoder_inst
        with patch(f"{_MOD}.DecoderRegistry.get", return_value=decoder_cls):
            await method._process_file(str(f), str(tmp_path))

        method._target.persist.assert_awaited_once()
        # device_id resolved from filename prefix
        assert method._target.persist.call_args[0][1] == "SIG-001"

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_file_not_exists_skipped(self, mock_sleep, tmp_path):
        method = DirectoryWatchMethod()
        method._cfg = DirectoryWatchServerConfig(paths=[str(tmp_path)])
        method._target = ControllerTarget()
        method._target.persist = AsyncMock()
        method._session_factory = AsyncMock()
        method._device_overrides = {}

        await method._process_file(
            str(tmp_path / "ghost.dat"), str(tmp_path),
        )
        method._target.persist.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_no_device_id_moves_to_error(self, mock_sleep, tmp_path):
        method = DirectoryWatchMethod()
        method._cfg = DirectoryWatchServerConfig(paths=[str(tmp_path)])
        method._target = ControllerTarget()
        method._target.persist = AsyncMock()
        method._session_factory = AsyncMock()
        method._device_overrides = {}

        # No underscore in filename → no resolved device_id.
        f = tmp_path / "noprefix.dat"
        f.write_bytes(b"\x01")

        with patch.object(method, "_move_to_error") as mock_move:
            await method._process_file(str(f), str(tmp_path))
        mock_move.assert_called_once()
        method._target.persist.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_decode_error_moves_to_error(self, mock_sleep, tmp_path):
        method = DirectoryWatchMethod()
        method._cfg = DirectoryWatchServerConfig(paths=[str(tmp_path)])
        method._target = ControllerTarget()
        method._target.persist = AsyncMock()
        method._session_factory = AsyncMock()
        method._device_overrides = {}

        f = tmp_path / "SIG-001_x.dat"
        f.write_bytes(b"\xff")

        decoder_cls = MagicMock()
        decoder_inst = MagicMock()
        decoder_inst.decode_bytes.side_effect = ValueError("bad bytes")
        decoder_cls.return_value = decoder_inst

        with (
            patch(f"{_MOD}.DecoderRegistry.get", return_value=decoder_cls),
            patch.object(method, "_move_to_error") as mock_move,
        ):
            await method._process_file(str(f), str(tmp_path))

        mock_move.assert_called_once()
        method._target.persist.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_persist_error_moves_to_error(self, mock_sleep, tmp_path):
        method = DirectoryWatchMethod()
        method._cfg = DirectoryWatchServerConfig(paths=[str(tmp_path)])
        method._target = ControllerTarget()
        method._target.persist = AsyncMock(side_effect=RuntimeError("db down"))
        method._session_factory = AsyncMock()
        method._device_overrides = {}

        f = tmp_path / "SIG-001_x.dat"
        f.write_bytes(b"\x01")

        decoder_cls = MagicMock()
        decoder_inst = MagicMock()
        decoder_inst.decode_bytes.return_value = [
            DecodedEvent(timestamp=None, event_code=82, event_param=1),
        ]
        decoder_cls.return_value = decoder_inst

        with (
            patch(f"{_MOD}.DecoderRegistry.get", return_value=decoder_cls),
            patch.object(method, "_move_to_error") as mock_move,
        ):
            await method._process_file(str(f), str(tmp_path))

        mock_move.assert_called_once()


# ---------------------------------------------------------------------------
# _read_file_with_retry
# ---------------------------------------------------------------------------


class TestReadFileWithRetry:
    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_reads_successfully_first_attempt(self, mock_sleep, tmp_path):
        f = tmp_path / "file.dat"
        f.write_bytes(b"\x01\x02")
        result = await DirectoryWatchMethod._read_file_with_retry(f)
        assert result == b"\x01\x02"

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_retries_then_succeeds(self, mock_sleep):
        fp = MagicMock(spec=Path)
        fp.read_bytes = MagicMock(side_effect=[OSError("locked"), b"ok"])
        fp.name = "f.dat"
        result = await DirectoryWatchMethod._read_file_with_retry(fp)
        assert result == b"ok"
        assert fp.read_bytes.call_count == 2

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_returns_none_after_both_attempts_fail(self, mock_sleep):
        fp = MagicMock(spec=Path)
        fp.read_bytes = MagicMock(side_effect=OSError("locked"))
        fp.name = "f.dat"
        result = await DirectoryWatchMethod._read_file_with_retry(fp)
        assert result is None
        assert fp.read_bytes.call_count == 2


# ---------------------------------------------------------------------------
# Decoder resolution
# ---------------------------------------------------------------------------


class TestResolveDecoder:
    def _method(self, decoder_default="auto", per_device=None):
        m = DirectoryWatchMethod()
        m._cfg = DirectoryWatchServerConfig(
            paths=["/tmp"], decoder=decoder_default,
        )
        m._device_overrides = per_device or {}
        return m

    def test_per_device_decoder_overrides_default(self):
        method = self._method(per_device={"SIG-1": {"decoder": "asc3"}})
        decoder_cls = MagicMock()
        decoder_cls.return_value = "instance"
        with patch(f"{_MOD}.DecoderRegistry.get", return_value=decoder_cls) as g:
            result = method._resolve_decoder("file.dat", "SIG-1")
        g.assert_called_once_with("asc3")
        assert result == "instance"

    def test_server_default_used_when_no_override(self):
        method = self._method(decoder_default="csv")
        decoder_cls = MagicMock()
        decoder_cls.return_value = "instance"
        with patch(f"{_MOD}.DecoderRegistry.get", return_value=decoder_cls) as g:
            result = method._resolve_decoder("file.dat", "SIG-1")
        g.assert_called_once_with("csv")
        assert result == "instance"

    def test_dat_extension_maps_to_asc3_when_default_is_auto(self):
        method = self._method(decoder_default="auto")
        decoder_cls = MagicMock()
        decoder_cls.return_value = "instance"
        with patch(f"{_MOD}.DecoderRegistry.get", return_value=decoder_cls) as g:
            method._resolve_decoder("file.dat", "SIG-1")
        g.assert_called_once_with("asc3")

    def test_csv_extension_maps_to_csv_when_default_is_auto(self):
        method = self._method(decoder_default="auto")
        decoder_cls = MagicMock()
        decoder_cls.return_value = "instance"
        with patch(f"{_MOD}.DecoderRegistry.get", return_value=decoder_cls) as g:
            method._resolve_decoder("file.csv", "SIG-1")
        g.assert_called_once_with("csv")

    def test_unknown_extension_falls_back_to_registry(self):
        method = self._method(decoder_default="auto")
        decoder_cls = MagicMock()
        decoder_cls.return_value = "instance"
        with patch(
            f"{_MOD}.DecoderRegistry.get_for_extension",
            return_value=[decoder_cls],
        ) as g:
            method._resolve_decoder("file.xyz", "SIG-1")
        g.assert_called_once_with(".xyz")

    def test_no_decoder_found_raises(self):
        method = self._method(decoder_default="auto")
        with patch(
            f"{_MOD}.DecoderRegistry.get_for_extension", return_value=[],
        ):
            with pytest.raises(ValueError, match="No decoder"):
                method._resolve_decoder("file.unknown", "SIG-1")


# ---------------------------------------------------------------------------
# Stop
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


class TestStartup:
    @pytest.mark.asyncio
    async def test_start_with_existing_files_triggers_scan(self, tmp_path):
        method = DirectoryWatchMethod()
        f = tmp_path / "SIG-001_x.dat"
        f.write_bytes(b"\x01")

        with (
            patch(f"{_MOD}.Observer") as MockObserver,
            patch.object(method, "_process_file", new_callable=AsyncMock) as proc,
        ):
            MockObserver.return_value = MagicMock()
            await method.start(
                {"paths": [str(tmp_path)]},
                AsyncMock(),
                target=ControllerTarget(),
                devices=[],
            )

        # The startup scan should have processed the existing file.
        proc.assert_awaited_once()
        # First positional arg is the file path.
        assert proc.call_args[0][0].endswith("SIG-001_x.dat")
        await method.stop()

    @pytest.mark.asyncio
    async def test_start_skips_nonexistent_path(self, caplog, tmp_path):
        import logging
        method = DirectoryWatchMethod()
        ghost = tmp_path / "ghost"  # doesn't exist
        with caplog.at_level(logging.ERROR, logger=_MOD):
            await method.start(
                {"paths": [str(ghost)]},
                AsyncMock(),
                target=ControllerTarget(),
                devices=[],
            )
        assert "does not exist" in caplog.text.lower()
        # No observer started for that path.
        assert method._observers == []

    @pytest.mark.asyncio
    async def test_start_recursive_scan(self, tmp_path):
        method = DirectoryWatchMethod()
        sub = tmp_path / "deep"
        sub.mkdir()
        f = sub / "SIG-123_x.dat"
        f.write_bytes(b"\x01")

        with (
            patch(f"{_MOD}.Observer") as MockObserver,
            patch.object(method, "_process_file", new_callable=AsyncMock) as proc,
        ):
            MockObserver.return_value = MagicMock()
            await method.start(
                {"paths": [str(tmp_path)], "recursive": True},
                AsyncMock(),
                target=ControllerTarget(),
                devices=[],
            )

        proc.assert_awaited_once()
        assert proc.call_args[0][0].endswith("SIG-123_x.dat")
        await method.stop()


# ---------------------------------------------------------------------------
# Move-to-processed / Move-to-error
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
