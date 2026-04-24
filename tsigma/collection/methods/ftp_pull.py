"""
FTP/FTPS/SFTP pull ingestion method.

Polls remote servers for event log files, decodes them,
and persists events to the database. Supports FTP, FTPS (TLS),
and SFTP (SSH) protocols via a unified configuration.

Two operating modes:

**Passive mode** (default): Non-destructive polling. Lists remote files,
downloads new ones based on file-identity checkpoint (name + size + hash),
never modifies or deletes files on the controller. Safe for shared access.

**Rotate mode**: SNMP-controlled file rotation for controllers that
append to log files (Econolite ASC3, etc.). Sequence:
  1. Ingest any leftover renamed files from previous crashed cycles
  2. FTP RENAME all matching files → <name>.tsigma.<UTC timestamp>
  3. SNMP SET logging OFF (controller closes all file handles)
  4. SNMP SET logging ON (controller creates fresh log files)
  5. FTP DOWNLOAD each renamed file
  6. Idempotent upsert to DB per file
  7. FTP DELETE each renamed file after successful ingest
Timestamps in renamed filenames guarantee no collisions — a leftover
``event1.dat.tsigma.20260408T150000`` from a crashed cycle is never
overwritten by a new rename ``event1.dat.tsigma.20260408T153000``.
SNMP cycles once per poll (not per file). Files the controller creates
between polls (event1..eventN) are all captured.
This eliminates the ATSPM 4x data gap (download → delete → stop → start).

Checkpoint strategy: FILE-BASED ONLY. Uses file identity (name + size
+ directory hash) to determine what has been ingested. Never uses event
timestamps from inside files for checkpointing — a controller with a
bad clock cannot poison the checkpoint.

Future-dated events are flagged and trigger notifications but are still
ingested (data is not discarded).

This is a PollingIngestionMethod — the CollectorService calls
poll_once() on a schedule with per-signal config from the database.
"""

import enum
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Optional

from pydantic import BaseModel, Field
from sqlalchemy import select

from ...models.checkpoint import PollingCheckpoint
from ..registry import IngestionMethodRegistry, PollingIngestionMethod
from ..sdk import (
    load_checkpoint,
    persist_events_with_drift_check,
    record_error,
    resolve_decoder_by_extension,
)

logger = logging.getLogger(__name__)

_DEFAULT_PORTS = {"ftp": 21, "ftps": 990, "sftp": 22}


class FTPProtocol(str, enum.Enum):
    """Supported file transfer protocols."""

    FTP = "ftp"
    FTPS = "ftps"
    SFTP = "sftp"


class FTPMode(str, enum.Enum):
    """FTP pull operating mode.

    PASSIVE: Non-destructive polling — never modifies controller files.
    ROTATE: SNMP-controlled file rotation — rename, cycle logging,
            download, ingest, delete. Zero data gaps.
    """

    PASSIVE = "passive"
    ROTATE = "rotate"


# Default SNMP OID for Econolite ASC3 event logging control
# 1.3.6.1.4.1.1206.3.5.2.9.17.1.0 — SET 0=OFF, 1=ON
_ASC3_LOGGING_OID = "1.3.6.1.4.1.1206.3.5.2.9.17.1.0"
_SNMP_LOGGING_OFF = 0
_SNMP_LOGGING_ON = 1

# Protocol constant maps — lazy-loaded to avoid import when SNMP extra not installed.
_AUTH_PROTOCOLS: dict[str, tuple] | None = None
_PRIV_PROTOCOLS: dict[str, tuple] | None = None


def _load_protocol_maps() -> None:
    """Lazy-load pysnmp protocol OID constants."""
    global _AUTH_PROTOCOLS, _PRIV_PROTOCOLS
    if _AUTH_PROTOCOLS is not None:
        return
    from pysnmp.hlapi.v3arch.asyncio import (
        usmAesCfb128Protocol,
        usmAesCfb192Protocol,
        usmAesCfb256Protocol,
        usmDESPrivProtocol,
        usmHMACMD5AuthProtocol,
        usmHMACSHA256AuthProtocol,
        usmHMACSHA384AuthProtocol,
        usmHMACSHA512AuthProtocol,
        usmHMACSHAAuthProtocol,
    )
    _AUTH_PROTOCOLS = {
        "MD5": usmHMACMD5AuthProtocol,
        "SHA": usmHMACSHAAuthProtocol,
        "SHA256": usmHMACSHA256AuthProtocol,
        "SHA384": usmHMACSHA384AuthProtocol,
        "SHA512": usmHMACSHA512AuthProtocol,
    }
    _PRIV_PROTOCOLS = {
        "DES": usmDESPrivProtocol,
        "AES128": usmAesCfb128Protocol,
        "AES192": usmAesCfb192Protocol,
        "AES256": usmAesCfb256Protocol,
    }


def _build_usm_user_data(config: "FTPPullConfig"):
    """Build pysnmp UsmUserData from FTPPullConfig for SNMPv3.

    Args:
        config: FTP pull configuration with v3 fields populated.

    Returns:
        UsmUserData instance configured per the signal's security level.
    """
    from pysnmp.hlapi.v3arch.asyncio import UsmUserData

    _load_protocol_maps()

    kwargs: dict = {"userName": config.snmp_username}

    if config.snmp_security_level in ("authNoPriv", "authPriv"):
        kwargs["authKey"] = config.snmp_auth_passphrase
        kwargs["authProtocol"] = _AUTH_PROTOCOLS[config.snmp_auth_protocol]

    if config.snmp_security_level == "authPriv":
        kwargs["privKey"] = config.snmp_priv_passphrase
        kwargs["privProtocol"] = _PRIV_PROTOCOLS[config.snmp_priv_protocol]

    return UsmUserData(**kwargs)


# Module-level SnmpEngine — reused across all _snmp_set() calls.
_snmp_engine = None


def _get_snmp_engine():
    """Lazy-initialize and return the shared SnmpEngine."""
    global _snmp_engine
    if _snmp_engine is None:
        from pysnmp.hlapi.v3arch.asyncio import SnmpEngine
        _snmp_engine = SnmpEngine()
    return _snmp_engine


try:
    from pysnmp.hlapi.v3arch.asyncio import UdpTransportTarget, set_cmd
except ImportError:
    UdpTransportTarget = None  # type: ignore[assignment,misc]
    set_cmd = None  # type: ignore[assignment,misc]

_INGESTING_TAG = ".tsigma."


@dataclass
class RemoteFile:
    """Metadata for a file on the remote server."""

    name: str
    size: int
    mtime: Optional[datetime]


class FTPPullConfig(BaseModel):
    """
    Configuration for the FTP pull ingestion method.

    Args:
        host: Remote server hostname or IP.
        signal_id: Traffic signal ID these files belong to.
        protocol: Transfer protocol (ftp, ftps, sftp).
        port: Server port. None = use protocol default.
        username: Login username.
        password: Login password.
        remote_dir: Directory to scan for files.
        file_extensions: File extensions to download.
        decoder: Explicit decoder name, or None for auto-detect.
        ssh_key_path: Path to SSH private key (SFTP only).
        passive_mode: Use passive mode (FTP/FTPS only).
        mode: Operating mode — passive (non-destructive) or rotate (SNMP).
        snmp_version: SNMP version — "v1", "v2c", or "v3" (rotate mode).
        snmp_community: SNMP v1/v2c community string (rotate mode).
        snmp_port: SNMP agent port on the controller (rotate mode).
        snmp_username: SNMPv3 USM username (rotate mode, v3 only).
        snmp_security_level: SNMPv3 security level — "noAuthNoPriv",
            "authNoPriv", or "authPriv" (rotate mode, v3 only).
        snmp_auth_protocol: SNMPv3 authentication protocol — "MD5", "SHA",
            "SHA256", "SHA384", or "SHA512" (rotate mode, v3 only).
        snmp_auth_passphrase: SNMPv3 authentication passphrase (v3 only).
        snmp_priv_protocol: SNMPv3 privacy protocol — "DES", "AES128",
            "AES192", or "AES256" (rotate mode, v3 only).
        snmp_priv_passphrase: SNMPv3 privacy passphrase (v3 only).
        logging_oid: SNMP OID for event logging control (rotate mode).
        rotate_filename: Filename to rotate in rotate mode (e.g. "ATSPM.dat").
    """

    host: str
    signal_id: str
    protocol: FTPProtocol = FTPProtocol.FTP
    port: Optional[int] = None
    username: str = "anonymous"
    password: str = ""
    remote_dir: str = "/"
    file_extensions: list[str] = Field(
        default_factory=lambda: [".dat", ".csv", ".log"]
    )
    decoder: Optional[str] = None
    ssh_key_path: Optional[str] = None
    known_hosts_path: Optional[str] = None
    passive_mode: bool = True
    mode: FTPMode = FTPMode.PASSIVE
    snmp_version: str = "v1"
    snmp_community: str = "public"
    snmp_port: int = 161
    snmp_username: str = ""
    snmp_security_level: str = "authPriv"
    snmp_auth_protocol: str = "SHA"
    snmp_auth_passphrase: str = ""
    snmp_priv_protocol: str = "AES128"
    snmp_priv_passphrase: str = ""
    logging_oid: str = _ASC3_LOGGING_OID
    rotate_filename: Optional[str] = None

    @property
    def default_port(self) -> int:
        """Default port for the configured protocol."""
        return _DEFAULT_PORTS[self.protocol.value]

    @property
    def effective_port(self) -> int:
        """Port to use: explicit if set, otherwise protocol default."""
        return self.port if self.port is not None else self.default_port


# ---------------------------------------------------------------------------
# Internal protocol adapter
# ---------------------------------------------------------------------------


class _FileTransferClient(ABC):
    """Internal ABC for FTP/FTPS/SFTP operations."""

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the remote server."""
        ...

    @abstractmethod
    async def list_dir(self, path: str) -> list[RemoteFile]:
        """List files in a remote directory."""
        ...

    @abstractmethod
    async def download(self, path: str) -> bytes:
        """Download a remote file and return its bytes."""
        ...

    @abstractmethod
    async def rename(self, src: str, dst: str) -> None:
        """Rename a remote file."""
        ...

    @abstractmethod
    async def delete(self, path: str) -> None:
        """Delete a remote file."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the server."""
        ...


class _AioFTPClient(_FileTransferClient):
    """FTP/FTPS client using aioftp."""

    def __init__(self, config: FTPPullConfig):
        self._config = config
        self._client = None

    async def connect(self) -> None:
        """Connect to FTP/FTPS server."""
        import aioftp

        if self._config.protocol == FTPProtocol.FTP:
            logger.warning(
                "Plain FTP (unencrypted) connection to %s "
                "— credentials and data sent in cleartext. "
                "Use FTPS or SFTP when the controller supports it.",
                self._config.host,
            )

        if self._config.protocol == FTPProtocol.FTPS:
            import ssl

            ctx = ssl.create_default_context()
            self._client = aioftp.Client.context(
                self._config.host,
                port=self._config.effective_port,
                user=self._config.username,
                password=self._config.password,
                ssl=ctx,
            )
        else:
            self._client = aioftp.Client.context(
                self._config.host,
                port=self._config.effective_port,
                user=self._config.username,
                password=self._config.password,
            )
        self._ctx = await self._client.__aenter__()

    async def list_dir(self, path: str) -> list[RemoteFile]:
        """List files in remote FTP directory."""
        result = []
        async for item_path, info in self._ctx.list(path):
            if info.get("type") == "file":
                name = PurePosixPath(item_path).name
                size = int(info.get("size", 0))
                result.append(RemoteFile(name=name, size=size, mtime=None))
        return result

    async def download(self, path: str) -> bytes:
        """Download file from FTP server."""
        import io

        stream = await self._ctx.download_stream(path)
        buf = io.BytesIO()
        async for block in stream.iter_by_block():
            buf.write(block)
        await stream.finish()
        return buf.getvalue()

    async def rename(self, src: str, dst: str) -> None:
        """Rename file on FTP server."""
        await self._ctx.rename(src, dst)

    async def delete(self, path: str) -> None:
        """Delete file from FTP server."""
        await self._ctx.remove(path)

    async def disconnect(self) -> None:
        """Disconnect from FTP server."""
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None


class _AsyncSSHClient(_FileTransferClient):
    """SFTP client using asyncssh."""

    def __init__(self, config: FTPPullConfig):
        self._config = config
        self._conn = None
        self._sftp = None

    async def connect(self) -> None:
        """Connect to SFTP server."""
        import asyncssh

        if self._config.known_hosts_path is None:
            logger.warning(
                "SFTP host key verification disabled for %s "
                "— set known_hosts_path to enable",
                self._config.host,
            )

        kwargs = {
            "host": self._config.host,
            "port": self._config.effective_port,
            "username": self._config.username,
            "known_hosts": self._config.known_hosts_path,
        }
        if self._config.ssh_key_path:
            kwargs["client_keys"] = [self._config.ssh_key_path]
        else:
            kwargs["password"] = self._config.password

        self._conn = await asyncssh.connect(**kwargs)
        self._sftp = await self._conn.start_sftp_client()

    async def list_dir(self, path: str) -> list[RemoteFile]:
        """List files in remote SFTP directory."""
        result = []
        for entry in await self._sftp.readdir(path):
            attrs = entry.attrs
            if attrs.type != 1:  # Not a regular file
                continue
            mtime = None
            if attrs.mtime is not None:
                mtime = datetime.fromtimestamp(attrs.mtime, tz=timezone.utc)
            result.append(
                RemoteFile(name=entry.filename, size=attrs.size or 0, mtime=mtime)
            )
        return result

    async def download(self, path: str) -> bytes:
        """Download file from SFTP server."""
        async with self._sftp.open(path, "rb") as f:
            return await f.read()

    async def rename(self, src: str, dst: str) -> None:
        """Rename file on SFTP server."""
        await self._sftp.rename(src, dst)

    async def delete(self, path: str) -> None:
        """Delete file from SFTP server."""
        await self._sftp.remove(path)

    async def disconnect(self) -> None:
        """Disconnect from SFTP server."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._sftp = None


def _create_client(config: FTPPullConfig) -> _FileTransferClient:
    """
    Create the appropriate file transfer client for the protocol.

    Args:
        config: FTP pull configuration.

    Returns:
        Protocol-specific file transfer client.
    """
    if config.protocol == FTPProtocol.SFTP:
        return _AsyncSSHClient(config)
    return _AioFTPClient(config)


def _compute_files_hash(filenames: list[str]) -> str:
    """
    Compute SHA-256 hash of sorted filenames for change detection.

    Args:
        filenames: List of filenames from remote directory listing.

    Returns:
        Hex digest string.
    """
    joined = "\n".join(sorted(filenames))
    return hashlib.sha256(joined.encode()).hexdigest()


# ---------------------------------------------------------------------------
# FTPPullMethod — the registered plugin
# ---------------------------------------------------------------------------


@IngestionMethodRegistry.register("ftp_pull")
class FTPPullMethod(PollingIngestionMethod):
    """
    FTP/FTPS/SFTP pull ingestion method.

    A polling plugin: the CollectorService calls poll_once() on a
    schedule with per-signal config from signal_metadata JSONB.

    Uses persistent polling_checkpoint table to track what has been
    ingested. Files are never deleted from the controller.
    """

    name = "ftp_pull"

    @staticmethod
    def _build_config(signal_id: str, raw: dict[str, Any]) -> FTPPullConfig:
        """
        Build FTPPullConfig from a signal_metadata collection dict.

        Args:
            signal_id: Traffic signal identifier.
            raw: Collection config dict from signal_metadata JSONB.

        Returns:
            FTPPullConfig instance.
        """
        return FTPPullConfig(
            host=raw.get("host", ""),
            signal_id=signal_id,
            protocol=FTPProtocol(raw.get("protocol", "ftp")),
            port=raw.get("port"),
            username=raw.get("username", "anonymous"),
            password=raw.get("password", ""),
            remote_dir=raw.get("remote_dir", "/"),
            file_extensions=raw.get("file_extensions", [".dat", ".csv", ".log"]),
            decoder=raw.get("decoder"),
            ssh_key_path=raw.get("ssh_key_path"),
            passive_mode=raw.get("passive_mode", True),
            mode=FTPMode(raw.get("mode", "passive")),
            snmp_version=raw.get("snmp_version", "v1"),
            snmp_community=raw.get("snmp_community", "public"),
            snmp_port=raw.get("snmp_port", 161),
            snmp_username=raw.get("snmp_username", ""),
            snmp_security_level=raw.get("snmp_security_level", "authPriv"),
            snmp_auth_protocol=raw.get("snmp_auth_protocol", "SHA"),
            snmp_auth_passphrase=raw.get("snmp_auth_passphrase", ""),
            snmp_priv_protocol=raw.get("snmp_priv_protocol", "AES128"),
            snmp_priv_passphrase=raw.get("snmp_priv_passphrase", ""),
            logging_oid=raw.get("logging_oid", _ASC3_LOGGING_OID),
            rotate_filename=raw.get("rotate_filename"),
        )

    def _create_client(self, config: FTPPullConfig) -> _FileTransferClient:
        """Create a file transfer client from config."""
        return _create_client(config)

    async def health_check(self) -> bool:
        """
        Polling methods are always considered healthy.

        Per-signal connectivity is validated during poll_once.

        Returns:
            True always.
        """
        return True

    async def _save_checkpoint(
        self,
        signal_id: str,
        session_factory,
        *,
        last_filename: Optional[str] = None,
        last_file_mtime: Optional[datetime] = None,
        files_hash: Optional[str] = None,
        new_events: int = 0,
        new_files: int = 0,
    ) -> None:
        """
        Create or update the checkpoint after successful ingest.

        Args:
            signal_id: Traffic signal identifier.
            session_factory: Async session factory.
            last_filename: Most recently ingested filename.
            last_file_mtime: Modification time of newest ingested file.
            files_hash: SHA-256 of sorted filenames from directory listing.
            new_events: Number of events ingested this cycle.
            new_files: Number of files ingested this cycle.
        """
        now = datetime.now(timezone.utc)
        async with session_factory() as session:
            # Controller-side FTP pull — device_type always "controller".
            # A roadside-sensor FTP trace pull (legacy Wavetronix etc.)
            # will run through a separate subclass / target.
            stmt = select(PollingCheckpoint).where(
                PollingCheckpoint.device_type == "controller",
                PollingCheckpoint.device_id == signal_id,
                PollingCheckpoint.method == self.name,
            )
            result = await session.execute(stmt)
            checkpoint = result.scalar_one_or_none()

            if checkpoint is None:
                checkpoint = PollingCheckpoint(
                    device_type="controller",
                    device_id=signal_id,
                    method=self.name,
                )
                session.add(checkpoint)

            checkpoint.last_filename = last_filename
            checkpoint.last_file_mtime = last_file_mtime
            checkpoint.files_hash = files_hash
            checkpoint.last_successful_poll = now
            checkpoint.events_ingested += new_events
            checkpoint.files_ingested += new_files
            checkpoint.consecutive_errors = 0
            checkpoint.consecutive_silent_cycles = 0
            checkpoint.last_error = None
            checkpoint.updated_at = now

            await session.flush()

    def _filter_new_files(
        self,
        matching_files: list[RemoteFile],
        checkpoint: Optional[PollingCheckpoint],
        current_hash: str,
    ) -> list[RemoteFile]:
        """
        Filter remote files to only those not yet ingested.

        FILE-BASED checkpoint only. Uses directory listing hash to detect
        changes, then file mtime as a tiebreaker for controllers that
        reuse filenames. Never uses event timestamps from inside files
        — a controller with a bad clock cannot poison the checkpoint.

        Args:
            matching_files: Files matching extension filter.
            checkpoint: Current checkpoint, or None for first poll.
            current_hash: SHA-256 of current filenames.

        Returns:
            List of new files to download, sorted by mtime ascending.
        """
        if not checkpoint or not checkpoint.files_hash:
            # First poll — download everything
            new_files = matching_files
        elif current_hash == checkpoint.files_hash:
            # Directory listing unchanged — nothing new
            new_files = []
        else:
            # Directory changed — use file mtime to narrow if available
            if checkpoint.last_file_mtime:
                new_files = [
                    rf for rf in matching_files
                    if rf.mtime is not None and rf.mtime > checkpoint.last_file_mtime
                ]
                # FTP without MDTM: mtime unavailable, download all on hash change
                if not new_files:
                    new_files = matching_files
            else:
                new_files = matching_files

        # Sort by mtime so file-based checkpoint advances monotonically
        new_files.sort(key=lambda rf: rf.mtime or datetime.min)
        return new_files

    async def _download_and_ingest(
        self,
        client: _FileTransferClient,
        new_files: list[RemoteFile],
        ftp_config: FTPPullConfig,
        signal_id: str,
        session_factory,
        prior_mtime: Optional[datetime],
    ) -> tuple[int, int, Optional[str], Optional[datetime]]:
        """
        Download, decode, and persist a list of remote files.

        Args:
            client: Connected file transfer client.
            new_files: Files to download.
            ftp_config: FTP pull configuration.
            signal_id: Traffic signal identifier.
            session_factory: Async session factory.
            prior_mtime: Last file mtime from checkpoint, or None.

        Returns:
            Tuple of (total_events, total_files, newest_filename, newest_mtime).
        """
        total_events = 0
        total_files = 0
        newest_filename = None
        newest_mtime = prior_mtime

        for rf in new_files:
            file_path = PurePosixPath(ftp_config.remote_dir) / rf.name
            try:
                data = await client.download(str(file_path))
                decoder = resolve_decoder_by_extension(
                    rf.name, explicit_decoder=ftp_config.decoder,
                )
                events = decoder.decode_bytes(data)
                await persist_events_with_drift_check(
                    events, signal_id, session_factory
                )

                total_events += len(events)
                total_files += 1
                newest_filename = rf.name
                if rf.mtime and (newest_mtime is None or rf.mtime > newest_mtime):
                    newest_mtime = rf.mtime

                logger.info(
                    "Processed %s: %d events for %s",
                    rf.name, len(events), signal_id,
                )
            except Exception:
                logger.exception("Failed to process %s for %s", rf.name, signal_id)

        return total_events, total_files, newest_filename, newest_mtime

    # -------------------------------------------------------------------
    # SNMP helpers (rotate mode)
    # -------------------------------------------------------------------

    @staticmethod
    async def _snmp_set(host: str, config: FTPPullConfig, value: int) -> None:
        """SET the logging OID on the controller via SNMP.

        Supports v1, v2c, and v3 based on config.snmp_version.

        Args:
            host: Controller IP / hostname.
            config: FTP pull config (version, credentials, port, OID).
            value: Integer value to SET (0=OFF, 1=ON).

        Raises:
            RuntimeError: If the SNMP SET operation fails.
        """
        from pysnmp.hlapi.v3arch.asyncio import (
            CommunityData,
            ContextData,
            Integer32,
            ObjectIdentity,
            ObjectType,
        )

        engine = _get_snmp_engine()
        target = await UdpTransportTarget.create((host, config.snmp_port))

        if config.snmp_version == "v3":
            auth_data = _build_usm_user_data(config)
        else:
            mp_model = 0 if config.snmp_version == "v1" else 1
            auth_data = CommunityData(config.snmp_community, mpModel=mp_model)

        error_indication, error_status, error_index, var_binds = await set_cmd(
            engine,
            auth_data,
            target,
            ContextData(),
            ObjectType(ObjectIdentity(config.logging_oid), Integer32(value)),
        )

        if error_indication:
            raise RuntimeError(f"SNMP SET failed: {error_indication}")
        if error_status:
            raise RuntimeError(
                f"SNMP SET error: {error_status.prettyPrint()} "
                f"at {var_binds[int(error_index) - 1][0] if error_index else '?'}"
            )

    async def _snmp_stop_logging(self, config: FTPPullConfig) -> None:
        """SNMP SET logging OFF on the controller."""
        logger.debug(
            "SNMP SET %s = %d (stop logging) on %s",
            config.logging_oid, _SNMP_LOGGING_OFF, config.host,
        )
        await self._snmp_set(config.host, config, _SNMP_LOGGING_OFF)

    async def _snmp_start_logging(self, config: FTPPullConfig) -> None:
        """SNMP SET logging ON on the controller."""
        logger.debug(
            "SNMP SET %s = %d (start logging) on %s",
            config.logging_oid, _SNMP_LOGGING_ON, config.host,
        )
        await self._snmp_set(config.host, config, _SNMP_LOGGING_ON)

    # -------------------------------------------------------------------
    # Rotate mode poll
    # -------------------------------------------------------------------

    @staticmethod
    def _is_tsigma_renamed(filename: str) -> bool:
        """Check if a filename was renamed by TSIGMA (contains the tag)."""
        return _INGESTING_TAG in filename

    @staticmethod
    def _original_name_from_renamed(filename: str) -> str:
        """Extract the original filename from a TSIGMA-renamed file.

        ``event1.dat.tsigma.20260408T153000`` → ``event1.dat``
        """
        idx = filename.find(_INGESTING_TAG)
        if idx == -1:
            return filename
        return filename[:idx]

    def _resolve_rotate_targets(
        self,
        config: FTPPullConfig,
        remote_files: list[RemoteFile],
    ) -> list[str]:
        """Determine which filenames to rotate.

        If ``rotate_filename`` is set, use that single file.
        Otherwise, rotate all files matching ``file_extensions``,
        excluding files already renamed by TSIGMA.

        Args:
            config: FTP pull config.
            remote_files: Current directory listing.

        Returns:
            List of filenames (without directory) to rotate.
        """
        if config.rotate_filename:
            # Only include if it actually exists in the listing
            names = {rf.name for rf in remote_files}
            if config.rotate_filename in names:
                return [config.rotate_filename]
            return []

        return [
            rf.name for rf in remote_files
            if PurePosixPath(rf.name).suffix.lower() in config.file_extensions
            and not self._is_tsigma_renamed(rf.name)
        ]

    async def _poll_rotate(
        self,
        client: _FileTransferClient,
        ftp_config: FTPPullConfig,
        signal_id: str,
        session_factory,
    ) -> None:
        """Execute one rotate-mode poll cycle.

        Sequence:
          1. Ingest any leftover renamed files from previous crashed cycles
          2. FTP RENAME all matching files → <name>.tsigma.<UTC timestamp>
          3. SNMP SET 0 (stop logging — controller closes all fds)
          4. SNMP SET 1 (start logging — controller creates fresh files)
          5. FTP DOWNLOAD each renamed file
          6. Decode → idempotent upsert to DB
          7. FTP DELETE each renamed file after successful ingest

        Timestamps in renamed filenames guarantee no collisions with
        leftovers from previous cycles or new files the controller
        creates after step 4.

        Args:
            client: Connected file transfer client.
            ftp_config: FTP pull configuration.
            signal_id: Traffic signal identifier.
            session_factory: Async session factory for DB writes.
        """
        # Phase 1: Ingest leftovers from previous crashed cycles
        all_files = await client.list_dir(ftp_config.remote_dir)
        leftovers = [
            rf for rf in all_files
            if self._is_tsigma_renamed(rf.name)
        ]
        for rf in leftovers:
            await self._ingest_and_delete(
                client, rf.name, ftp_config, signal_id, session_factory,
            )

        # Phase 2: Rename all active files with UTC timestamp
        all_files = await client.list_dir(ftp_config.remote_dir)
        targets = self._resolve_rotate_targets(ftp_config, all_files)
        if not targets:
            logger.debug("No files to rotate for signal %s", signal_id)
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        renamed: list[str] = []

        for filename in targets:
            dst_name = f"{filename}{_INGESTING_TAG}{timestamp}"
            src = str(PurePosixPath(ftp_config.remote_dir) / filename)
            dst = str(PurePosixPath(ftp_config.remote_dir) / dst_name)

            try:
                await client.rename(src, dst)
                renamed.append(dst_name)
            except Exception:
                logger.exception(
                    "Failed to rename %s on %s — skipping",
                    filename, ftp_config.host,
                )

        if not renamed:
            return

        # Phase 3: SNMP cycle — one stop/start after all renames
        try:
            await self._snmp_stop_logging(ftp_config)
            await self._snmp_start_logging(ftp_config)
        except Exception:
            logger.exception(
                "SNMP logging cycle failed for %s on %s "
                "— files already renamed, will ingest anyway",
                signal_id, ftp_config.host,
            )
            # Continue — renamed files are safe to download even if
            # the controller is still writing to old fds.  The next
            # SNMP cycle will clean up.

        # Phase 4: Download, ingest, delete each renamed file
        for dst_name in renamed:
            await self._ingest_and_delete(
                client, dst_name, ftp_config, signal_id, session_factory,
            )

    async def _ingest_and_delete(
        self,
        client: _FileTransferClient,
        filename: str,
        ftp_config: FTPPullConfig,
        signal_id: str,
        session_factory,
    ) -> None:
        """Download, decode, persist, then delete a remote file.

        Args:
            client: Connected file transfer client.
            filename: Name of the file (relative to remote_dir).
            ftp_config: FTP pull config.
            signal_id: Traffic signal identifier.
            session_factory: Async session factory.
        """
        file_path = str(PurePosixPath(ftp_config.remote_dir) / filename)

        # Strip TSIGMA rename tag to get the original name for decoder lookup
        original_name = self._original_name_from_renamed(filename)

        try:
            data = await client.download(file_path)
        except Exception:
            logger.exception(
                "Failed to download %s for signal %s — "
                "will retry next cycle",
                filename, signal_id,
            )
            return

        try:
            decoder = resolve_decoder_by_extension(
                original_name, explicit_decoder=ftp_config.decoder,
            )
            events = decoder.decode_bytes(data)
        except Exception:
            logger.exception(
                "Failed to decode %s for signal %s", filename, signal_id,
            )
            return

        try:
            await persist_events_with_drift_check(
                events, signal_id, session_factory,
            )
        except Exception:
            logger.exception(
                "Failed to persist events from %s for signal %s — "
                "file NOT deleted, will retry next cycle",
                filename, signal_id,
            )
            return

        # Only delete after successful ingest
        try:
            await client.delete(file_path)
        except Exception:
            logger.exception(
                "Failed to delete %s after successful ingest — "
                "idempotent upsert will dedup on next cycle",
                filename,
            )

        logger.info(
            "Rotate ingested %s: %d events for signal %s",
            filename, len(events), signal_id,
        )

        await self._save_checkpoint(
            signal_id,
            session_factory,
            last_filename=original_name,
            new_events=len(events),
            new_files=1,
        )

    # -------------------------------------------------------------------
    # Poll dispatch
    # -------------------------------------------------------------------

    async def poll_once(
        self, signal_id: str, config: dict[str, Any], session_factory
    ) -> None:
        """
        Execute one poll cycle for a single signal.

        Dispatches to passive or rotate mode based on config.

        Args:
            signal_id: Traffic signal identifier.
            config: Collection config dict from signal_metadata JSONB.
            session_factory: Async session factory for DB writes.
        """
        ftp_config = self._build_config(signal_id, config)
        client = self._create_client(ftp_config)

        try:
            await client.connect()
        except Exception as exc:
            logger.error(
                "Connection failed to %s://%s:%d for signal %s",
                ftp_config.protocol.value,
                ftp_config.host,
                ftp_config.effective_port,
                signal_id,
            )
            await record_error(
                self.name, "controller", signal_id, session_factory, str(exc),
            )
            return

        try:
            if ftp_config.mode == FTPMode.ROTATE:
                await self._poll_rotate(
                    client, ftp_config, signal_id, session_factory,
                )
            else:
                await self._poll_passive(
                    client, ftp_config, signal_id, session_factory,
                )
        except Exception as exc:
            logger.exception("Poll cycle failed for signal %s", signal_id)
            await record_error(
                self.name, "controller", signal_id, session_factory, str(exc),
            )
        finally:
            await client.disconnect()

    async def _poll_passive(
        self,
        client: _FileTransferClient,
        ftp_config: FTPPullConfig,
        signal_id: str,
        session_factory,
    ) -> None:
        """Execute one passive-mode poll cycle (original behavior).

        Non-destructive: lists files, filters by checkpoint, downloads
        new ones, decodes, persists. Never modifies controller files.

        Args:
            client: Connected file transfer client.
            ftp_config: FTP pull configuration.
            signal_id: Traffic signal identifier.
            session_factory: Async session factory for DB writes.
        """
        checkpoint = await load_checkpoint(
            self.name, "controller", signal_id, session_factory,
        )

        # List and filter remote files
        all_files = await client.list_dir(ftp_config.remote_dir)
        matching_files = [
            rf for rf in all_files
            if PurePosixPath(rf.name).suffix.lower() in ftp_config.file_extensions
        ]
        if not matching_files:
            logger.debug("No matching files for signal %s", signal_id)
            return

        # Quick change detection via files_hash
        current_hash = _compute_files_hash([rf.name for rf in matching_files])
        if checkpoint and checkpoint.files_hash == current_hash:
            logger.debug("No new files for signal %s (hash unchanged)", signal_id)
            return

        # Determine which files are new
        new_files = self._filter_new_files(matching_files, checkpoint, current_hash)
        if not new_files:
            logger.debug("No new files after checkpoint filter for %s", signal_id)
            return

        # Download, decode, persist
        prior_mtime = checkpoint.last_file_mtime if checkpoint else None
        total_events, total_files, newest_filename, newest_mtime = (
            await self._download_and_ingest(
                client, new_files, ftp_config,
                signal_id, session_factory, prior_mtime,
            )
        )

        # Update checkpoint after successful ingest
        if total_files > 0:
            await self._save_checkpoint(
                signal_id,
                session_factory,
                last_filename=newest_filename,
                last_file_mtime=newest_mtime,
                files_hash=current_hash,
                new_events=total_events,
                new_files=total_files,
            )
