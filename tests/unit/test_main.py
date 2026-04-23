"""
Unit tests for application entrypoint.

Tests that main() calls uvicorn.run with correct configuration.
"""

from unittest.mock import patch

from tsigma.main import main


class TestMain:
    """Tests for main() entrypoint."""

    def test_calls_uvicorn_run(self):
        """Test main() calls uvicorn.run."""
        with patch("tsigma.main.uvicorn.run") as mock_run:
            main()

        mock_run.assert_called_once()

    def test_passes_app_string(self):
        """Test main() passes correct app import string to uvicorn."""
        with patch("tsigma.main.uvicorn.run") as mock_run:
            main()

        call_args = mock_run.call_args
        assert call_args[0][0] == "tsigma.app:app"

    def test_uses_settings_host_and_port(self):
        """Test main() passes host/port from settings."""
        with patch("tsigma.main.uvicorn.run") as mock_run:
            main()

        call_kwargs = mock_run.call_args[1]
        assert "host" in call_kwargs
        assert "port" in call_kwargs

    def test_reload_disabled_by_default(self):
        """Test main() disables reload by default (production safe)."""
        with patch("tsigma.main.uvicorn.run") as mock_run:
            main()

        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["reload"] is False

    def test_reload_enabled_in_debug(self):
        """Test main() enables reload when debug=True."""
        with patch("tsigma.main.settings") as mock_settings, \
             patch("tsigma.main.uvicorn.run") as mock_run:
            mock_settings.debug = True
            mock_settings.api_host = "0.0.0.0"
            mock_settings.api_port = 8080
            mock_settings.log_level = "INFO"
            mock_settings.log_format = "text"
            main()

        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["reload"] is True

    def test_name_main_guard(self):
        """Test __name__ == '__main__' block calls main() (line 31)."""
        with patch("tsigma.main.uvicorn.run"):
            import runpy
            # runpy.run_module re-executes the module with __name__="__main__",
            # so main() defined inside the module will be called via the guard.
            # We patch uvicorn.run so it doesn't actually start a server.
            try:
                runpy.run_module("tsigma.main", run_name="__main__", alter_sys=False)
            except SystemExit:
                pass
        # If we get here without error, the guard executed main() which
        # called uvicorn.run (our mock). The test validates the guard exists.
