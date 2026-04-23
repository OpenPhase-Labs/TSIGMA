"""
Unit tests for directory watch ingestion method plugin.

Tests configuration, registry registration, health checks,
file pattern matching, and file processing pipeline.
All filesystem and watchdog interactions are mocked.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.decoders.base import DecodedEvent
from tsigma.collection.methods.directory_watch import (
    DirectoryWatchConfig,
    DirectoryWatchMethod,
    _FileEventHandler,
)
from tsigma.collection.registry import (
    EventDrivenIngestionMethod,
    ExecutionMode,
    IngestionMethodRegistry,
)

# Module path prefix for patching imports used in directory_watch.py
_MOD = "tsigma.collection.methods.directory_watch"


# -----------------------------------------------------------------------
# Registry
# -----------------------------------------------------------------------


class TestRegistration:
    """Tests for registry self-registration."""

    def test_registered(self):
        """IngestionMethodRegistry.get('directory_watch') returns the class."""
        cls = IngestionMethodRegistry.get("directory_watch")
        assert cls is DirectoryWatchMethod

    def test_execution_mode(self):
        """DirectoryWatchMethod has EVENT_DRIVEN execution mode."""
        assert DirectoryWatchMethod.execution_mode is ExecutionMode.EVENT_DRIVEN

    def test_is_event_driven_subclass(self):
        """DirectoryWatchMethod is an EventDrivenIngestionMethod."""
        assert issubclass(DirectoryWatchMethod, EventDrivenIngestionMethod)

    def test_name(self):
        """DirectoryWatchMethod.name is 'directory_watch'."""
        assert DirectoryWatchMethod.name == "directory_watch"


# -----------------------------------------------------------------------
# Health Check
# -----------------------------------------------------------------------


class TestHealthCheck:
    """Tests for DirectoryWatchMethod.health_check()."""

    @pytest.mark.asyncio
    async def test_health_check_not_running(self):
        """health_check returns False when observer is None (not started)."""
        method = DirectoryWatchMethod()
        assert await method.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_observer_alive(self):
        """health_check returns True when observer thread is alive."""
        method = DirectoryWatchMethod()
        mock_observer = MagicMock()
        mock_observer.is_alive.return_value = True
        method._observer = mock_observer
        assert await method.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_observer_dead(self):
        """health_check returns False when observer thread is not alive."""
        method = DirectoryWatchMethod()
        mock_observer = MagicMock()
        mock_observer.is_alive.return_value = False
        method._observer = mock_observer
        assert await method.health_check() is False


# -----------------------------------------------------------------------
# DirectoryWatchConfig
# -----------------------------------------------------------------------


class TestDirectoryWatchConfig:
    """Tests for the DirectoryWatchConfig dataclass."""

    def test_defaults(self):
        """Config defaults are populated correctly."""
        cfg = DirectoryWatchConfig(watch_dir="/tmp/watch")
        assert cfg.watch_dir == "/tmp/watch"
        assert cfg.file_patterns == ["*.dat", "*.csv", "*.DAT", "*.CSV"]
        assert cfg.decoder is None
        assert cfg.signal_id is None
        assert cfg.move_after_processing is True
        assert cfg.processed_subdir == "processed"
        assert cfg.error_subdir == "errors"
        assert cfg.recursive is False

    def test_custom_values(self):
        """Config accepts custom values for all fields."""
        cfg = DirectoryWatchConfig(
            watch_dir="/data/incoming",
            file_patterns=["*.bin"],
            decoder="asc3",
            signal_id="SIG-042",
            move_after_processing=False,
            processed_subdir="done",
            error_subdir="failed",
            recursive=True,
        )
        assert cfg.watch_dir == "/data/incoming"
        assert cfg.file_patterns == ["*.bin"]
        assert cfg.decoder == "asc3"
        assert cfg.signal_id == "SIG-042"
        assert cfg.move_after_processing is False
        assert cfg.processed_subdir == "done"
        assert cfg.error_subdir == "failed"
        assert cfg.recursive is True


# -----------------------------------------------------------------------
# _build_config
# -----------------------------------------------------------------------


class TestBuildConfig:
    """Tests for DirectoryWatchMethod._build_config()."""

    def test_minimal_config(self):
        """Builds config with only watch_dir specified."""
        cfg = DirectoryWatchMethod._build_config({"watch_dir": "/tmp/watch"})
        assert isinstance(cfg, DirectoryWatchConfig)
        # watch_dir is resolved to absolute
        assert Path(cfg.watch_dir).is_absolute()
        assert cfg.file_patterns == ["*.dat", "*.csv", "*.DAT", "*.CSV"]

    def test_missing_watch_dir_raises(self):
        """Raises ValueError when watch_dir is not provided."""
        with pytest.raises(ValueError, match="requires 'watch_dir'"):
            DirectoryWatchMethod._build_config({})

    def test_empty_watch_dir_raises(self):
        """Raises ValueError when watch_dir is empty string."""
        with pytest.raises(ValueError, match="requires 'watch_dir'"):
            DirectoryWatchMethod._build_config({"watch_dir": ""})

    def test_custom_file_patterns(self):
        """Custom file_patterns are passed through."""
        cfg = DirectoryWatchMethod._build_config({
            "watch_dir": "/tmp/watch",
            "file_patterns": ["*.bin", "*.log"],
        })
        assert cfg.file_patterns == ["*.bin", "*.log"]

    def test_explicit_decoder(self):
        """Explicit decoder name is passed through."""
        cfg = DirectoryWatchMethod._build_config({
            "watch_dir": "/tmp/watch",
            "decoder": "asc3",
        })
        assert cfg.decoder == "asc3"

    def test_explicit_signal_id(self):
        """Explicit signal_id is passed through."""
        cfg = DirectoryWatchMethod._build_config({
            "watch_dir": "/tmp/watch",
            "signal_id": "SIG-001",
        })
        assert cfg.signal_id == "SIG-001"

    def test_recursive_flag(self):
        """Recursive flag is passed through."""
        cfg = DirectoryWatchMethod._build_config({
            "watch_dir": "/tmp/watch",
            "recursive": True,
        })
        assert cfg.recursive is True

    def test_move_after_processing_disabled(self):
        """move_after_processing can be disabled."""
        cfg = DirectoryWatchMethod._build_config({
            "watch_dir": "/tmp/watch",
            "move_after_processing": False,
        })
        assert cfg.move_after_processing is False

    def test_subdir_traversal_rejected(self):
        """Subdirectory paths that escape watch_dir raise ValueError."""
        with pytest.raises(ValueError, match="escapes watch_dir"):
            DirectoryWatchMethod._build_config({
                "watch_dir": "/tmp/watch",
                "processed_subdir": "../../etc",
            })

    def test_error_subdir_traversal_rejected(self):
        """Error subdirectory paths that escape watch_dir raise ValueError."""
        with pytest.raises(ValueError, match="escapes watch_dir"):
            DirectoryWatchMethod._build_config({
                "watch_dir": "/tmp/watch",
                "error_subdir": "../../../tmp/evil",
            })


# -----------------------------------------------------------------------
# Signal ID Resolution
# -----------------------------------------------------------------------


class TestResolveSignalId:
    """Tests for DirectoryWatchMethod._resolve_signal_id()."""

    def test_explicit_signal_id_from_config(self):
        """Uses signal_id from config when set."""
        cfg = DirectoryWatchConfig(watch_dir="/tmp", signal_id="SIG-099")
        result = DirectoryWatchMethod._resolve_signal_id("anything.dat", cfg)
        assert result == "SIG-099"

    def test_infer_from_filename_with_underscore(self):
        """Extracts signal_id before the first underscore in filename."""
        cfg = DirectoryWatchConfig(watch_dir="/tmp")
        result = DirectoryWatchMethod._resolve_signal_id(
            "gdot-0142_20240115_events.dat", cfg
        )
        assert result == "gdot-0142"

    def test_no_underscore_returns_none(self):
        """Returns None when filename has no underscore and no config signal_id."""
        cfg = DirectoryWatchConfig(watch_dir="/tmp")
        result = DirectoryWatchMethod._resolve_signal_id("nounder.dat", cfg)
        assert result is None

    def test_underscore_at_start(self):
        """Filename starting with underscore returns empty string prefix."""
        cfg = DirectoryWatchConfig(watch_dir="/tmp")
        result = DirectoryWatchMethod._resolve_signal_id("_data.dat", cfg)
        assert result == ""


# -----------------------------------------------------------------------
# File Pattern Matching (_FileEventHandler)
# -----------------------------------------------------------------------


class TestFilePatternMatching:
    """Tests for _FileEventHandler._matches_patterns()."""

    def _make_handler(self, patterns=None):
        """Create a _FileEventHandler with given patterns."""
        cfg = DirectoryWatchConfig(
            watch_dir="/tmp/watch",
            file_patterns=patterns or ["*.dat", "*.csv", "*.DAT", "*.CSV"],
        )
        return _FileEventHandler(
            config=cfg,
            session_factory=MagicMock(),
            loop=MagicMock(),
            method=MagicMock(),
        )

    def test_matches_dat(self):
        """*.dat pattern matches .dat files."""
        handler = self._make_handler()
        assert handler._matches_patterns("events_20240101.dat") is True

    def test_matches_csv(self):
        """*.csv pattern matches .csv files."""
        handler = self._make_handler()
        assert handler._matches_patterns("data.csv") is True

    def test_matches_uppercase_dat(self):
        """*.DAT pattern matches uppercase .DAT files."""
        handler = self._make_handler()
        assert handler._matches_patterns("FILE.DAT") is True

    def test_rejects_unmatched_extension(self):
        """Non-matching extensions are rejected."""
        handler = self._make_handler()
        assert handler._matches_patterns("readme.txt") is False

    def test_rejects_no_extension(self):
        """Files without extension are rejected."""
        handler = self._make_handler()
        assert handler._matches_patterns("Makefile") is False

    def test_custom_patterns(self):
        """Custom patterns work correctly."""
        handler = self._make_handler(["*.bin", "report_*"])
        assert handler._matches_patterns("output.bin") is True
        assert handler._matches_patterns("report_daily") is True
        assert handler._matches_patterns("data.csv") is False


# -----------------------------------------------------------------------
# Event Handler Scheduling
# -----------------------------------------------------------------------


class TestFileEventHandlerScheduling:
    """Tests for _FileEventHandler event dispatching."""

    def _make_handler(self, patterns=None):
        cfg = DirectoryWatchConfig(
            watch_dir="/tmp/watch",
            file_patterns=patterns or ["*.dat"],
        )
        mock_loop = MagicMock()
        mock_method = MagicMock()
        handler = _FileEventHandler(
            config=cfg,
            session_factory=MagicMock(),
            loop=mock_loop,
            method=mock_method,
        )
        return handler, mock_loop, mock_method

    @patch(f"{_MOD}.asyncio.run_coroutine_threadsafe")
    def test_on_created_schedules_matching_file(self, mock_rcts):
        """on_created schedules processing for a matching file."""
        handler, mock_loop, mock_method = self._make_handler()
        event = MagicMock()
        event.is_directory = False
        event.src_path = "/tmp/watch/signal_001.dat"
        handler.on_created(event)
        mock_rcts.assert_called_once()

    @patch(f"{_MOD}.asyncio.run_coroutine_threadsafe")
    def test_on_created_skips_directory(self, mock_rcts):
        """on_created ignores directory events."""
        handler, _, _ = self._make_handler()
        event = MagicMock()
        event.is_directory = True
        handler.on_created(event)
        mock_rcts.assert_not_called()

    @patch(f"{_MOD}.asyncio.run_coroutine_threadsafe")
    def test_on_created_skips_non_matching(self, mock_rcts):
        """on_created ignores files that don't match patterns."""
        handler, _, _ = self._make_handler()
        event = MagicMock()
        event.is_directory = False
        event.src_path = "/tmp/watch/readme.txt"
        handler.on_created(event)
        mock_rcts.assert_not_called()

    @patch(f"{_MOD}.asyncio.run_coroutine_threadsafe")
    def test_on_moved_schedules_matching_file(self, mock_rcts):
        """on_moved schedules processing using dest_path."""
        handler, _, _ = self._make_handler()
        event = MagicMock()
        event.is_directory = False
        event.dest_path = "/tmp/watch/signal_001.dat"
        handler.on_moved(event)
        mock_rcts.assert_called_once()

    @patch(f"{_MOD}.asyncio.run_coroutine_threadsafe")
    def test_on_moved_skips_directory(self, mock_rcts):
        """on_moved ignores directory events."""
        handler, _, _ = self._make_handler()
        event = MagicMock()
        event.is_directory = True
        handler.on_moved(event)
        mock_rcts.assert_not_called()


# -----------------------------------------------------------------------
# File Processing Pipeline
# -----------------------------------------------------------------------


class TestProcessFile:
    """Tests for DirectoryWatchMethod._process_file()."""

    @pytest.mark.asyncio
    @patch(f"{_MOD}.shutil.move")
    @patch(f"{_MOD}.persist_events", new_callable=AsyncMock)
    @patch(f"{_MOD}.DecoderRegistry")
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_successful_processing(
        self, mock_sleep, mock_decoder_reg, mock_persist, mock_move
    ):
        """File is decoded, events persisted, and file moved to processed."""
        from datetime import datetime, timezone

        fake_events = [
            DecodedEvent(
                timestamp=datetime(2024, 1, 15, tzinfo=timezone.utc),
                event_code=1,
                event_param=2,
            )
        ]

        mock_decoder_cls = MagicMock()
        mock_decoder_instance = MagicMock()
        mock_decoder_instance.decode_bytes.return_value = fake_events
        mock_decoder_cls.return_value = mock_decoder_instance
        mock_decoder_reg.get.return_value = mock_decoder_cls

        method = DirectoryWatchMethod()
        config = DirectoryWatchConfig(
            watch_dir="/tmp/watch",
            signal_id="SIG-001",
        )
        session_factory = AsyncMock()

        file_path = "/tmp/watch/SIG-001_20240115.dat"

        with patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "read_bytes", return_value=b"fake data"), \
             patch.object(Path, "name", new_callable=lambda: property(lambda s: "SIG-001_20240115.dat")), \
             patch.object(Path, "mkdir"):
            await method._process_file(file_path, config, session_factory)

        # Decoder was called
        mock_decoder_instance.decode_bytes.assert_called_once_with(b"fake data")
        # Events were persisted
        mock_persist.assert_awaited_once_with(fake_events, "SIG-001", session_factory)

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_file_not_exists_skipped(self, mock_sleep):
        """File that no longer exists is skipped without error."""
        method = DirectoryWatchMethod()
        config = DirectoryWatchConfig(watch_dir="/tmp/watch", signal_id="SIG-001")
        session_factory = AsyncMock()

        with patch.object(Path, "exists", return_value=False):
            # Should not raise
            await method._process_file(
                "/tmp/watch/gone.dat", config, session_factory
            )

    @pytest.mark.asyncio
    @patch(f"{_MOD}.shutil.move")
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_no_signal_id_moves_to_error(self, mock_sleep, mock_move):
        """File with unresolvable signal_id is moved to error subdir."""
        method = DirectoryWatchMethod()
        config = DirectoryWatchConfig(watch_dir="/tmp/watch")
        session_factory = AsyncMock()

        # Filename with no underscore -> signal_id is None
        file_path = "/tmp/watch/nounder.dat"

        with patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "mkdir"):
            await method._process_file(file_path, config, session_factory)

        # File should be moved to error subdir
        mock_move.assert_called_once()
        call_args = mock_move.call_args
        assert "errors" in call_args[0][1]

    @pytest.mark.asyncio
    @patch(f"{_MOD}.shutil.move")
    @patch(f"{_MOD}.persist_events", new_callable=AsyncMock)
    @patch(f"{_MOD}.DecoderRegistry")
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_decode_error_moves_to_error(
        self, mock_sleep, mock_decoder_reg, mock_persist, mock_move
    ):
        """File that fails to decode is moved to error subdir."""
        mock_decoder_cls = MagicMock()
        mock_decoder_instance = MagicMock()
        mock_decoder_instance.decode_bytes.side_effect = ValueError("bad data")
        mock_decoder_cls.return_value = mock_decoder_instance
        mock_decoder_reg.get.return_value = mock_decoder_cls

        method = DirectoryWatchMethod()
        config = DirectoryWatchConfig(
            watch_dir="/tmp/watch",
            signal_id="SIG-001",
        )
        session_factory = AsyncMock()

        file_path = "/tmp/watch/SIG-001_bad.dat"

        with patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "read_bytes", return_value=b"bad data"), \
             patch.object(Path, "mkdir"):
            await method._process_file(file_path, config, session_factory)

        # persist_events should NOT have been called
        mock_persist.assert_not_awaited()
        # File moved to errors
        mock_move.assert_called_once()
        assert "errors" in mock_move.call_args[0][1]

    @pytest.mark.asyncio
    @patch(f"{_MOD}.shutil.move")
    @patch(f"{_MOD}.persist_events", new_callable=AsyncMock)
    @patch(f"{_MOD}.DecoderRegistry")
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_persist_error_moves_to_error(
        self, mock_sleep, mock_decoder_reg, mock_persist, mock_move
    ):
        """File whose events fail to persist is moved to error subdir."""
        mock_decoder_cls = MagicMock()
        mock_decoder_instance = MagicMock()
        mock_decoder_instance.decode_bytes.return_value = [
            DecodedEvent(
                timestamp=MagicMock(),
                event_code=1,
                event_param=0,
            )
        ]
        mock_decoder_cls.return_value = mock_decoder_instance
        mock_decoder_reg.get.return_value = mock_decoder_cls
        mock_persist.side_effect = RuntimeError("db down")

        method = DirectoryWatchMethod()
        config = DirectoryWatchConfig(
            watch_dir="/tmp/watch",
            signal_id="SIG-001",
        )
        session_factory = AsyncMock()

        file_path = "/tmp/watch/SIG-001_data.dat"

        with patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "read_bytes", return_value=b"data"), \
             patch.object(Path, "mkdir"):
            await method._process_file(file_path, config, session_factory)

        # File moved to errors
        mock_move.assert_called_once()
        assert "errors" in mock_move.call_args[0][1]

    @pytest.mark.asyncio
    @patch(f"{_MOD}.shutil.move")
    @patch(f"{_MOD}.persist_events", new_callable=AsyncMock)
    @patch(f"{_MOD}.DecoderRegistry")
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_move_disabled(
        self, mock_sleep, mock_decoder_reg, mock_persist, mock_move
    ):
        """When move_after_processing is False, file is not moved."""
        mock_decoder_cls = MagicMock()
        mock_decoder_instance = MagicMock()
        mock_decoder_instance.decode_bytes.return_value = []
        mock_decoder_cls.return_value = mock_decoder_instance
        mock_decoder_reg.get.return_value = mock_decoder_cls

        method = DirectoryWatchMethod()
        config = DirectoryWatchConfig(
            watch_dir="/tmp/watch",
            signal_id="SIG-001",
            move_after_processing=False,
        )
        session_factory = AsyncMock()

        file_path = "/tmp/watch/SIG-001_data.dat"

        with patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "read_bytes", return_value=b"data"), \
             patch.object(Path, "mkdir"):
            await method._process_file(file_path, config, session_factory)

        mock_move.assert_not_called()


# -----------------------------------------------------------------------
# Read File With Retry
# -----------------------------------------------------------------------


class TestReadFileWithRetry:
    """Tests for DirectoryWatchMethod._read_file_with_retry()."""

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_reads_successfully_first_attempt(self, mock_sleep):
        """Returns file bytes on first successful read."""
        mock_path = MagicMock(spec=Path)
        mock_path.read_bytes.return_value = b"file content"
        result = await DirectoryWatchMethod._read_file_with_retry(mock_path)
        assert result == b"file content"
        mock_sleep.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_retries_on_permission_error(self, mock_sleep):
        """Retries once on PermissionError, then succeeds."""
        mock_path = MagicMock(spec=Path)
        mock_path.name = "locked.dat"
        mock_path.read_bytes.side_effect = [
            PermissionError("locked"),
            b"content after retry",
        ]
        result = await DirectoryWatchMethod._read_file_with_retry(mock_path)
        assert result == b"content after retry"
        mock_sleep.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_returns_none_after_both_attempts_fail(self, mock_sleep):
        """Returns None when both read attempts fail."""
        mock_path = MagicMock(spec=Path)
        mock_path.name = "locked.dat"
        mock_path.read_bytes.side_effect = PermissionError("locked")
        result = await DirectoryWatchMethod._read_file_with_retry(mock_path)
        assert result is None


# -----------------------------------------------------------------------
# Decoder Resolution
# -----------------------------------------------------------------------


class TestResolveDecoder:
    """Tests for DirectoryWatchMethod._resolve_decoder()."""

    @patch(f"{_MOD}.DecoderRegistry")
    def test_explicit_decoder_from_config(self, mock_registry):
        """Uses explicit decoder name from config when set."""
        mock_cls = MagicMock()
        mock_registry.get.return_value = mock_cls
        cfg = DirectoryWatchConfig(watch_dir="/tmp", decoder="asc3")

        result = DirectoryWatchMethod._resolve_decoder("file.dat", cfg)
        mock_registry.get.assert_called_once_with("asc3")
        mock_cls.assert_called_once()
        assert result == mock_cls()

    @patch(f"{_MOD}.DecoderRegistry")
    def test_dat_extension_maps_to_asc3(self, mock_registry):
        """'.dat' extension maps to 'asc3' decoder via extension map."""
        mock_cls = MagicMock()
        mock_registry.get.return_value = mock_cls
        cfg = DirectoryWatchConfig(watch_dir="/tmp")

        DirectoryWatchMethod._resolve_decoder("events.dat", cfg)
        mock_registry.get.assert_called_once_with("asc3")

    @patch(f"{_MOD}.DecoderRegistry")
    def test_csv_extension_maps_to_csv(self, mock_registry):
        """'.csv' extension maps to 'csv' decoder via extension map."""
        mock_cls = MagicMock()
        mock_registry.get.return_value = mock_cls
        cfg = DirectoryWatchConfig(watch_dir="/tmp")

        DirectoryWatchMethod._resolve_decoder("data.csv", cfg)
        mock_registry.get.assert_called_once_with("csv")

    @patch(f"{_MOD}.DecoderRegistry")
    def test_unknown_extension_falls_back_to_registry(self, mock_registry):
        """Unknown extensions fall back to DecoderRegistry.get_for_extension."""
        mock_decoder = MagicMock()
        mock_registry.get_for_extension.return_value = [mock_decoder]
        cfg = DirectoryWatchConfig(watch_dir="/tmp")

        # .bin is not in the extension map
        DirectoryWatchMethod._resolve_decoder("data.bin", cfg)
        mock_registry.get_for_extension.assert_called_once_with(".bin")
        mock_decoder.assert_called_once()

    @patch(f"{_MOD}.DecoderRegistry")
    def test_no_decoder_found_raises(self, mock_registry):
        """Raises ValueError when no decoder can be found."""
        mock_registry.get_for_extension.return_value = []
        cfg = DirectoryWatchConfig(watch_dir="/tmp")

        with pytest.raises(ValueError, match="No decoder found"):
            DirectoryWatchMethod._resolve_decoder("file.xyz", cfg)


# -----------------------------------------------------------------------
# Stop
# -----------------------------------------------------------------------


class TestStop:
    """Tests for DirectoryWatchMethod.stop()."""

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self):
        """stop() is a no-op when observer is None."""
        method = DirectoryWatchMethod()
        await method.stop()  # Should not raise

    @pytest.mark.asyncio
    async def test_stop_stops_observer(self):
        """stop() stops and joins the observer thread."""
        method = DirectoryWatchMethod()
        mock_observer = MagicMock()
        method._observer = mock_observer

        await method.stop()

        mock_observer.stop.assert_called_once()
        mock_observer.join.assert_called_once_with(timeout=5.0)
        assert method._observer is None


# -----------------------------------------------------------------------
# Move-after-ingest logic (lines 223-251, 286-312, 354-356, 501-502,
# 525-526)
# -----------------------------------------------------------------------


class TestStartup:
    """Tests for DirectoryWatchMethod.start() and _startup_scan()."""

    @pytest.mark.asyncio
    @patch(f"{_MOD}.Observer")
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_start_with_existing_files(self, mock_sleep, MockObserver):
        """start() performs startup scan of existing files (lines 286-312)."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create some files
            for name in ["SIG-001_data1.dat", "SIG-001_data2.dat"]:
                (Path(tmpdir) / name).write_bytes(b"test data")

            method = DirectoryWatchMethod()
            mock_observer = MagicMock()
            MockObserver.return_value = mock_observer

            config = {"watch_dir": tmpdir, "signal_id": "SIG-001"}

            with patch.object(method, "_process_file", new_callable=AsyncMock) as mock_process:
                await method.start(config, AsyncMock())

            # Both files should be processed
            assert mock_process.await_count == 2

            # Clean up observer
            await method.stop()

    @pytest.mark.asyncio
    @patch(f"{_MOD}.Observer")
    async def test_start_nonexistent_dir_raises(self, MockObserver):
        """start() raises FileNotFoundError for missing directory."""
        method = DirectoryWatchMethod()
        config = {"watch_dir": "/nonexistent/path/that/does/not/exist"}

        with pytest.raises(FileNotFoundError, match="does not exist"):
            await method.start(config, AsyncMock())

    @pytest.mark.asyncio
    @patch(f"{_MOD}.Observer")
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_startup_scan_recursive(self, mock_sleep, MockObserver):
        """_startup_scan processes files in subdirectories when recursive=True (lines 290-291)."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = Path(tmpdir) / "sub"
            subdir.mkdir()
            (subdir / "SIG-001_nested.dat").write_bytes(b"data")

            method = DirectoryWatchMethod()
            mock_observer = MagicMock()
            MockObserver.return_value = mock_observer

            config = {"watch_dir": tmpdir, "signal_id": "SIG-001", "recursive": True}

            with patch.object(method, "_process_file", new_callable=AsyncMock) as mock_process:
                await method.start(config, AsyncMock())

            assert mock_process.await_count >= 1

            await method.stop()


class TestMoveToProcessed:
    """Tests for _move_to_processed (lines 481-502)."""

    @patch(f"{_MOD}.shutil.move")
    def test_move_to_processed_creates_dir(self, mock_move):
        """_move_to_processed creates the processed subdirectory."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "SIG-001_data.dat"
            file_path.write_bytes(b"data")

            config = DirectoryWatchConfig(watch_dir=tmpdir)

            DirectoryWatchMethod._move_to_processed(file_path, config)

            # Verify processed dir was created
            processed_dir = Path(tmpdir) / "processed"
            assert processed_dir.exists()

            # Verify shutil.move was called
            mock_move.assert_called_once()
            dest = mock_move.call_args[0][1]
            assert "processed" in dest

    @patch(f"{_MOD}.shutil.move", side_effect=OSError("permission denied"))
    def test_move_to_processed_handles_error(self, mock_move):
        """_move_to_processed logs error when move fails (lines 501-502)."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "SIG-001_data.dat"
            file_path.write_bytes(b"data")

            config = DirectoryWatchConfig(watch_dir=tmpdir)

            # Should not raise
            DirectoryWatchMethod._move_to_processed(file_path, config)


class TestMoveToError:
    """Tests for _move_to_error (lines 505-526)."""

    @patch(f"{_MOD}.shutil.move")
    def test_move_to_error_creates_dir(self, mock_move):
        """_move_to_error creates the error subdirectory."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "bad_file.dat"
            file_path.write_bytes(b"data")

            config = DirectoryWatchConfig(watch_dir=tmpdir)

            DirectoryWatchMethod._move_to_error(file_path, config)

            error_dir = Path(tmpdir) / "errors"
            assert error_dir.exists()

            mock_move.assert_called_once()
            dest = mock_move.call_args[0][1]
            assert "errors" in dest

    @patch(f"{_MOD}.shutil.move", side_effect=OSError("disk full"))
    def test_move_to_error_handles_error(self, mock_move):
        """_move_to_error logs error when move fails (lines 525-526)."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "bad_file.dat"
            file_path.write_bytes(b"data")

            config = DirectoryWatchConfig(watch_dir=tmpdir)

            # Should not raise
            DirectoryWatchMethod._move_to_error(file_path, config)


class TestProcessFileReadRetryFailure:
    """Tests for _process_file when file read fails (lines 354-356)."""

    @pytest.mark.asyncio
    @patch(f"{_MOD}.shutil.move")
    @patch(f"{_MOD}.asyncio.sleep", new_callable=AsyncMock)
    async def test_file_read_failure_moves_to_error(self, mock_sleep, mock_move):
        """File that can't be read after retry is moved to error subdir (lines 354-356)."""
        method = DirectoryWatchMethod()
        config = DirectoryWatchConfig(
            watch_dir="/tmp/watch",
            signal_id="SIG-001",
        )
        session_factory = AsyncMock()

        file_path = "/tmp/watch/SIG-001_locked.dat"

        with patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "read_bytes", side_effect=PermissionError("locked")), \
             patch.object(Path, "mkdir"):
            await method._process_file(file_path, config, session_factory)

        # File should be moved to errors
        mock_move.assert_called_once()
        assert "errors" in mock_move.call_args[0][1]
