"""
Listener service — orchestration layer for listener and event-driven
ingestion methods.

Parallel to ``CollectorService`` but for the long-lived ``start()`` /
``stop()`` shape rather than the scheduled ``poll_once()`` shape.
Discovers listener and event-driven plugins from
``IngestionMethodRegistry``, applies the lifecycle gates
(``settings.enable_listeners`` umbrella + per-method
``settings.enable_*_listener`` flags), builds Layer-2 server config
from process env vars, queries each registered ``DeviceSource`` for
devices configured against each method (filtered by the ``instance``
discriminator on per-device JSONB when applicable), and calls each
plugin's ``start()`` once with the merged config + matched devices.

A method with zero matched devices across all sources is skipped — no
orphan broker connection or empty bound port.

See ``docs/developers/LISTENERS.md`` for the full design.
"""

import logging
from typing import Any, Optional

from .registry import BaseIngestionMethod, IngestionMethodRegistry
from .sources import DeviceSource, SignalDeviceSource
from .targets import ControllerTarget

logger = logging.getLogger(__name__)


class ListenerService:
    """
    Orchestration service for listener + event-driven ingestion plugins.
    """

    # Methods whose Layer-2 config supports an ``instance`` discriminator
    # (the listener filters per-device configs by collection.instance ==
    # settings.{method}_instance).  Single-broker listeners (TCP, UDP,
    # gRPC, directory_watch) ignore instance entirely.
    _INSTANCE_AWARE = {"mqtt_listener", "nats_listener"}

    def __init__(
        self,
        session_factory,
        settings,
        *,
        sources: Optional[list[DeviceSource]] = None,
    ) -> None:
        """
        Initialize the listener service.

        Args:
            session_factory: Async session factory for DB access.
            settings: Application settings (enable flags, Layer-2
                server config, instance discriminators).
            sources: Device sources this listener service operates on.
                When omitted, defaults to a single ``SignalDeviceSource``
                paired with ``ControllerTarget`` so listeners run for
                controller signals only.  Pass an explicit list to
                serve sensors as well.
        """
        self._session_factory = session_factory
        self._settings = settings
        self._sources: list[DeviceSource] = (
            sources if sources is not None else [
                SignalDeviceSource(
                    poll_interval_seconds=settings.collector_poll_interval,
                    target=ControllerTarget(),
                ),
            ]
        )
        # Methods that have actually been started, in the order they
        # were started.  ``stop()`` walks this list in reverse.
        self._started: list[BaseIngestionMethod] = []

    async def start(self) -> None:
        """
        Discover and start all enabled listener + event-driven methods.

        For each registered method:
          1. Skip if its enable flag (and the umbrella) are both unset.
          2. Build Layer-2 config from process env settings.
          3. For each registered source, query devices configured for
             this method, filtered by instance discriminator if the
             method is instance-aware.
          4. Skip the method entirely if zero devices match across all
             sources (no orphan broker / port bind).
          5. Call ``method.start(config, session_factory, target=...,
             devices=...)`` once per source that has matching devices.
        """
        listener_classes = IngestionMethodRegistry.get_listener_methods()
        event_driven_classes = IngestionMethodRegistry.get_event_driven_methods()
        all_classes: dict[str, type[BaseIngestionMethod]] = {
            **listener_classes,
            **event_driven_classes,
        }

        for method_name, method_cls in all_classes.items():
            if not self._is_enabled(method_name):
                logger.debug(
                    "ListenerService: %s skipped (enable flag not set)",
                    method_name,
                )
                continue

            layer2_config = self._build_layer2_config(method_name)
            instance = self._instance_for(method_name)

            # Each source gets its own listener instance because the
            # source determines the target (and therefore the event
            # table + checkpoint scope).  A method that serves both
            # device classes runs twice — once per source — sharing
            # the same Layer-2 server config but writing to different
            # event tables.
            for source in self._sources:
                devices = await self._matched_devices(
                    source, method_name, instance,
                )
                if not devices:
                    logger.info(
                        "ListenerService: %s/%s — zero matching devices, "
                        "skipping",
                        method_name, source.device_type,
                    )
                    continue

                instance_obj = method_cls()
                try:
                    await instance_obj.start(
                        layer2_config,
                        self._session_factory,
                        target=source.target,
                        devices=devices,
                    )
                except Exception:
                    logger.exception(
                        "ListenerService: %s/%s failed to start",
                        method_name, source.device_type,
                    )
                    continue

                self._started.append(instance_obj)
                logger.info(
                    "ListenerService: %s/%s started with %d device(s)",
                    method_name, source.device_type, len(devices),
                )

        logger.info(
            "ListenerService started — %d method instance(s) running",
            len(self._started),
        )

    async def stop(self) -> None:
        """
        Stop every method instance previously started, in reverse order.

        Exceptions during one ``stop()`` do not prevent the others from
        running; all are gathered and logged.
        """
        # Reverse order so dependent shutdowns happen before their
        # dependencies (currently no dependencies between listeners,
        # but the convention matches CollectorService).
        for instance_obj in reversed(self._started):
            try:
                await instance_obj.stop()
            except Exception:
                logger.exception(
                    "ListenerService: %s.stop() raised",
                    type(instance_obj).__name__,
                )
        self._started.clear()

    async def health_check(self) -> dict[str, bool]:
        """
        Aggregate health status from all started method instances.

        Returns:
            Dictionary keyed by ``"{method_name}/{device_type}"`` with
            health-check booleans.  Multiple entries for the same
            method are produced when one method runs against multiple
            sources.
        """
        results: dict[str, bool] = {}
        for instance_obj in self._started:
            key = type(instance_obj).__name__
            try:
                results[key] = await instance_obj.health_check()
            except Exception:
                results[key] = False
        return results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _is_enabled(self, method_name: str) -> bool:
        """Return True if the umbrella or per-method flag is set."""
        per_method_attr = f"enable_{method_name.replace('-', '_')}"
        # Listener plugin names map directly to settings field stems:
        #   tcp_server      -> enable_tcp_listener     (special-case)
        #   udp_server      -> enable_udp_listener     (special-case)
        #   grpc_server     -> enable_grpc_listener    (special-case)
        #   mqtt_listener   -> enable_mqtt_listener
        #   nats_listener   -> enable_nats_listener
        #   directory_watch -> enable_directory_watch
        attr_map = {
            "tcp_server": "enable_tcp_listener",
            "udp_server": "enable_udp_listener",
            "grpc_server": "enable_grpc_listener",
            "mqtt_listener": "enable_mqtt_listener",
            "nats_listener": "enable_nats_listener",
            "directory_watch": "enable_directory_watch",
        }
        per_method_attr = attr_map.get(method_name, per_method_attr)
        per_method = getattr(self._settings, per_method_attr, False)
        return bool(self._settings.enable_listeners or per_method)

    def _instance_for(self, method_name: str) -> Optional[str]:
        """Return the instance discriminator for instance-aware methods."""
        if method_name not in self._INSTANCE_AWARE:
            return None
        attr_map = {
            "mqtt_listener": "mqtt_instance",
            "nats_listener": "nats_instance",
        }
        return getattr(self._settings, attr_map[method_name], "default")

    def _build_layer2_config(self, method_name: str) -> dict[str, Any]:
        """Pull the Layer-2 server config for ``method_name`` from settings.

        Returns a dict shaped to what each plugin's ``start()`` expects
        for the listener-server bits (bind, URL, credentials, instance).
        Per-device routing fields stay on the ``devices`` argument
        passed alongside.
        """
        s = self._settings
        if method_name == "tcp_server":
            return {
                "bind_address": s.tcp_bind_host,
                "port": s.tcp_bind_port,
                "max_connections": s.tcp_max_connections,
                "read_timeout_seconds": s.tcp_idle_timeout,
                "buffer_size": s.tcp_read_buffer_size,
                "decoder": s.tcp_decoder or None,
            }
        if method_name == "udp_server":
            return {
                "bind_address": s.udp_bind_host,
                "port": s.udp_bind_port,
                "max_packet_size": s.udp_max_packet_size,
                "decoder": s.udp_decoder or None,
            }
        if method_name == "grpc_server":
            return {
                "bind_address": s.grpc_bind_host,
                "port": s.grpc_bind_port,
                "tls_cert_file": s.grpc_tls_cert_file or None,
                "tls_key_file": s.grpc_tls_key_file or None,
                "max_message_size": s.grpc_max_message_size,
            }
        if method_name == "mqtt_listener":
            return {
                "broker_url": s.mqtt_broker_url,
                "client_id": s.mqtt_client_id,
                "username": s.mqtt_username,
                "username_file": s.mqtt_username_file or None,
                "password": s.mqtt_password,
                "password_file": s.mqtt_password_file or None,
                "keepalive": s.mqtt_keepalive,
                "tls": s.mqtt_tls,
                "instance": s.mqtt_instance,
            }
        if method_name == "nats_listener":
            return {
                "url": s.nats_url,
                "credentials_file": s.nats_credentials_file or None,
                "tls": s.nats_tls,
                "max_reconnects": s.nats_max_reconnects,
                "instance": s.nats_instance,
            }
        if method_name == "directory_watch":
            paths = [
                p.strip()
                for p in (s.directory_watch_paths or "").split(",")
                if p.strip()
            ]
            patterns = [
                p.strip()
                for p in (s.directory_watch_patterns or "*").split(",")
                if p.strip()
            ] or ["*"]
            return {
                "paths": paths,
                "patterns": patterns,
                "decoder": s.directory_watch_decoder or "auto",
            }
        # Unknown method — pass empty config and let the plugin error.
        return {}

    async def _matched_devices(
        self,
        source: DeviceSource,
        method_name: str,
        instance: Optional[str],
    ) -> list[tuple[str, dict[str, Any]]]:
        """Query a source for devices matching method + instance.

        Args:
            source: Device source to query.
            method_name: Listener method name.
            instance: Discriminator name to match against
                ``collection.instance`` on per-device JSONB, or ``None``
                if the method does not honour the instance discriminator.
                ``None`` matches every device; an explicit instance
                value matches devices with that instance OR devices
                without an ``instance`` key (treated as ``"default"``).
        """
        async with self._session_factory() as session:
            devices = await source.list_devices_for_method(session, method_name)

        if instance is None:
            return devices

        # Filter on collection.instance.  Devices without an explicit
        # instance default to "default" so single-broker DOTs that omit
        # the field continue to work.
        return [
            (device_id, config)
            for device_id, config in devices
            if config.get("instance", "default") == instance
        ]


__all__ = ["ListenerService"]
