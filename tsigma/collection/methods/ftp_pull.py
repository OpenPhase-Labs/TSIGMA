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

from ...models.checkpoint import PollingCheckpoint
from ..advancement import (
    Advancement,
    alert_repeated_failures,
    decide_advancement,
)
from ..registry import IngestionMethodRegistry, PollingIngestionMethod
from ..targets import ControllerTarget, IngestionTarget

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
    recursive: bool = False
    max_depth: int = 5
    follow_symlinks: bool = False

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
    async def list_dir(
        self,
        path: str,
        recursive: bool = False,
        max_depth: int = 5,
        follow_symlinks: bool = False,
    ) -> list[RemoteFile]:
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

    async def list_dir(
        self,
        path: str,
        recursive: bool = False,
        max_depth: int = 5,
        follow_symlinks: bool = False,
    ) -> list[RemoteFile]:
        """List files in a remote FTP directory.

        With ``recursive=False`` (default) only regular files directly in
        ``path`` are returned with bare filenames. With ``recursive=True``
        the listing descends subdirectories up to ``max_depth`` levels below
        the root; each returned ``RemoteFile.name`` is the POSIX-relative
        subpath from ``path``. Symlinked directories are descended only when
        ``follow_symlinks=True``.

        Cycle safety on FTP relies on ``max_depth``: aioftp exposes no
        ``realpath``, so link targets cannot be resolved and a symlink cycle
        cannot be detected by identity. The ``visited`` set below only dedups
        identical downward paths reached via multiple routes; it does NOT
        guard against symlink cycles. ``max_depth`` is what bounds an FTP
        cycle.
        """
        root = PurePosixPath(path)
        result: list[RemoteFile] = []
        # Dedup of identical downward paths only (see method docstring):
        # this is NOT symlink-cycle protection on FTP — max_depth bounds that.
        visited: set[str] = {str(root)}
        # Stack of (dir_path, depth) to walk iteratively.
        stack: list[tuple[PurePosixPath, int]] = [(root, 0)]

        while stack:
            current, depth = stack.pop()
            async for item_path, info in self._ctx.list(str(current)):
                item_type = info.get("type")
                child = PurePosixPath(current) / PurePosixPath(item_path).name
                if item_type == "file":
                    size = int(info.get("size", 0))
                    rel = child.relative_to(root)
                    result.append(
                        RemoteFile(name=rel.as_posix(), size=size, mtime=None)
                    )
                    continue
                if not recursive:
                    continue
                is_dir = item_type == "dir"
                is_link = item_type == "link"
                if not (is_dir or (is_link and follow_symlinks)):
                    continue
                if depth + 1 > max_depth:
                    logger.debug(
                        "Max recursion depth %d reached at %s — not descending",
                        max_depth, child,
                    )
                    continue
                key = str(child)
                if key in visited:
                    continue
                visited.add(key)
                stack.append((child, depth + 1))
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

    async def list_dir(
        self,
        path: str,
        recursive: bool = False,
        max_depth: int = 5,
        follow_symlinks: bool = False,
    ) -> list[RemoteFile]:
        """List files in a remote SFTP directory.

        With ``recursive=False`` (default) only regular files directly in
        ``path`` are returned with bare filenames. With ``recursive=True``
        the listing descends subdirectories up to ``max_depth`` levels below
        the root; each returned ``RemoteFile.name`` is the POSIX-relative
        subpath from ``path``. Symlinked directories (SFTP type 3) are
        descended only when ``follow_symlinks=True``; in that case resolved
        real paths are tracked to guard against symlink cycles.

        ``realpath`` is only invoked when cycle tracking is actually needed
        (``recursive`` and ``follow_symlinks``). The non-recursive default
        path and the recursive-without-symlink-follow path never resolve real
        paths, so the common 9000+ signal poll pays no extra round-trip.
        """
        root = PurePosixPath(path)
        result: list[RemoteFile] = []
        track_cycles = recursive and follow_symlinks
        visited: set[str] = set()
        if track_cycles:
            visited.add(await self._sftp.realpath(str(root)))
        # Stack of (dir_path, depth) to walk iteratively.
        stack: list[tuple[PurePosixPath, int]] = [(root, 0)]

        while stack:
            current, depth = stack.pop()
            for entry in await self._sftp.readdir(str(current)):
                attrs = entry.attrs
                child = PurePosixPath(current) / entry.filename
                if attrs.type == 1:  # Regular file
                    mtime = None
                    if attrs.mtime is not None:
                        mtime = datetime.fromtimestamp(
                            attrs.mtime, tz=timezone.utc
                        )
                    rel = child.relative_to(root)
                    result.append(
                        RemoteFile(
                            name=rel.as_posix(),
                            size=attrs.size or 0,
                            mtime=mtime,
                        )
                    )
                    continue
                if not recursive:
                    continue
                is_dir = attrs.type == 2
                is_link = attrs.type == 3
                if not (is_dir or (is_link and follow_symlinks)):
                    continue
                if depth + 1 > max_depth:
                    logger.debug(
                        "Max recursion depth %d reached at %s — not descending",
                        max_depth, child,
                    )
                    continue
                if track_cycles:
                    real = await self._sftp.realpath(str(child))
                    if real in visited:
                        continue
                    visited.add(real)
                stack.append((child, depth + 1))
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
            recursive=raw.get("recursive", False),
            max_depth=raw.get("max_depth", 5),
            follow_symlinks=raw.get("follow_symlinks", False),
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
        target: IngestionTarget,
        device_id: str,
        session_factory,
        *,
        last_filename: Optional[str] = None,
        last_file_mtime: Optional[datetime] = None,
        files_hash: Optional[str] = None,
        new_events: int = 0,
        new_absorbed: int = 0,
        new_files: int = 0,
    ) -> None:
        """
        Create or update the checkpoint after successful ingest.

        Delegates to ``target.save_checkpoint`` so the row lands in
        ``polling_checkpoint`` scoped to the target's ``device_type``
        without the FTP pull caring which kind of device it just
        polled.

        A ``None`` file-identity field is OMITTED rather than written.  These
        three are what make a file look already-read; passing None writes NULL
        over the stored marker, and the caller needs a way to say "this cycle
        records nothing here" when a file is still outstanding.
        """
        identity = {
            "last_filename": last_filename,
            "last_file_mtime": last_file_mtime,
            "files_hash": files_hash,
        }
        await target.save_checkpoint(
            self.name,
            device_id,
            session_factory,
            events_ingested=new_events,
            duplicates_absorbed=new_absorbed,
            files_ingested=new_files,
            **{key: value for key, value in identity.items() if value is not None},
        )

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
        device_id: str,
        session_factory,
        prior_mtime: Optional[datetime],
        target: IngestionTarget,
        last_successful_poll: Optional[datetime] = None,
        consecutive_errors: int = 0,
    ) -> tuple[int, int, Optional[str], Optional[datetime], int, bool]:
        """
        Download, decode, and persist a list of remote files via
        ``target``.

        Every file's verdict comes from ``decide_advancement``.  Passive mode
        keeps a FILE-IDENTITY checkpoint, so only a full ADVANCE counts a file
        as read: a PARTIAL holds, because the identity markers cannot express
        "half of this file" and moving them would skip the rest of it.

        ``consecutive_errors`` is the count from before this cycle; it rises
        locally as files hold, so a run of bad files escalates on the file that
        reaches the threshold rather than one cycle later.

        Returns:
            Tuple of (total_inserted, total_files, newest_filename,
            newest_mtime, total_absorbed, all_advanced).  ``all_advanced`` is
            False when any file is still outstanding.
        """
        total_inserted = 0
        total_files = 0
        newest_filename = None
        newest_mtime = prior_mtime
        total_absorbed = 0
        all_advanced = True
        errors = consecutive_errors

        for rf in new_files:
            file_path = PurePosixPath(ftp_config.remote_dir) / rf.name
            try:
                data = await client.download(str(file_path))
            except Exception:
                logger.exception(
                    "Failed to download %s for %s - will retry next cycle",
                    rf.name, device_id,
                )
                all_advanced = False
                continue

            # Transport-only: the host decodes, normalizes, runs the integrity
            # spine, and persists.  No guard around the seam - `ingest_raw` is
            # total and returns an outcome naming the stage that failed, so a
            # broad except here would only hide a real defect.
            ingest = await target.ingest(
                data, device_id, session_factory,
                decoder_name=ftp_config.decoder or None,
                filename=None if ftp_config.decoder else rf.name,
                last_successful_poll=last_successful_poll,
            )
            decision = decide_advancement(ingest, consecutive_errors=errors)

            if decision.action is not Advancement.ADVANCE:
                error = decision.error or ingest.error
                logger.error(
                    "Ingest of %s for %s did not complete (%s) - checkpoint "
                    "held, will retry next cycle: %s",
                    rf.name, device_id, ingest.outcome.value, error,
                )
                all_advanced = False
                errors += 1
                await target.record_error(
                    self.name, device_id, session_factory, error,
                )
                if decision.alert:
                    await alert_repeated_failures(
                        device_type=target.device_type,
                        device_id=device_id,
                        method=self.name,
                        consecutive_errors=errors,
                        error=error,
                    )
                continue

            total_inserted += ingest.events_inserted
            total_absorbed += ingest.duplicates_absorbed
            total_files += 1
            newest_filename = rf.name
            if rf.mtime and (newest_mtime is None or rf.mtime > newest_mtime):
                newest_mtime = rf.mtime

            logger.info(
                "Processed %s: %d events for %s",
                rf.name, ingest.events_decoded, device_id,
            )

        return (
            total_inserted,
            total_files,
            newest_filename,
            newest_mtime,
            total_absorbed,
            all_advanced,
        )

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
        device_id: str,
        session_factory,
        target: IngestionTarget,
    ) -> None:
        """Execute one rotate-mode poll cycle.

        Sequence:
          1. Ingest any leftover renamed files from previous crashed cycles
          2. FTP RENAME all matching files → <name>.tsigma.<UTC timestamp>
          3. SNMP SET 0 (stop logging — controller closes all fds)
          4. SNMP SET 1 (start logging — controller creates fresh files)
          5. FTP DOWNLOAD each renamed file
          6. Decode → idempotent upsert to DB via ``target``
          7. FTP DELETE each renamed file after successful ingest

        Timestamps in renamed filenames guarantee no collisions with
        leftovers from previous cycles or new files the controller
        creates after step 4.
        """
        checkpoint = await target.load_checkpoint(
            self.name, device_id, session_factory,
        )
        last_successful_poll = (
            checkpoint.last_successful_poll if checkpoint else None
        )
        # Rises as files hold within this cycle, so a run of bad files
        # escalates on the one that reaches the threshold.
        errors = (checkpoint.consecutive_errors or 0) if checkpoint else 0

        # Phase 1: Ingest leftovers from previous crashed cycles
        all_files = await client.list_dir(
            ftp_config.remote_dir,
            recursive=ftp_config.recursive,
            max_depth=ftp_config.max_depth,
            follow_symlinks=ftp_config.follow_symlinks,
        )
        leftovers = [
            rf for rf in all_files
            if self._is_tsigma_renamed(rf.name)
        ]
        for rf in leftovers:
            advanced = await self._ingest_and_delete(
                client, rf.name, ftp_config, device_id, session_factory, target,
                last_successful_poll, errors,
            )
            if not advanced:
                errors += 1

        # Phase 2: Rename all active files with UTC timestamp
        all_files = await client.list_dir(
            ftp_config.remote_dir,
            recursive=ftp_config.recursive,
            max_depth=ftp_config.max_depth,
            follow_symlinks=ftp_config.follow_symlinks,
        )
        rotate_targets = self._resolve_rotate_targets(ftp_config, all_files)
        if not rotate_targets:
            logger.debug("No files to rotate for device %s", device_id)
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        renamed: list[str] = []

        for filename in rotate_targets:
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
                device_id, ftp_config.host,
            )
            # Continue — renamed files are safe to download even if
            # the controller is still writing to old fds.  The next
            # SNMP cycle will clean up.

        # Phase 4: Download, ingest, delete each renamed file
        for dst_name in renamed:
            advanced = await self._ingest_and_delete(
                client, dst_name, ftp_config, device_id, session_factory, target,
                last_successful_poll, errors,
            )
            if not advanced:
                errors += 1

    async def _ingest_and_delete(
        self,
        client: _FileTransferClient,
        filename: str,
        ftp_config: FTPPullConfig,
        device_id: str,
        session_factory,
        target: IngestionTarget,
        last_successful_poll: Optional[datetime] = None,
        consecutive_errors: int = 0,
    ) -> bool:
        """Download, decode, persist, then delete a remote file.

        Returns True when the ingest fully advanced and the remote file was
        therefore eligible for deletion; False when it was left in place.
        """
        file_path = str(PurePosixPath(ftp_config.remote_dir) / filename)

        # Strip TSIGMA rename tag to get the original name for decoder lookup
        original_name = self._original_name_from_renamed(filename)

        try:
            data = await client.download(file_path)
        except Exception:
            logger.exception(
                "Failed to download %s for device %s - "
                "will retry next cycle",
                filename, device_id,
            )
            return False

        # Transport-only: the host owns decode -> spine -> persist.  No guard
        # around the seam - `ingest_raw` is total, so a broad except here would
        # swallow a genuine defect while reporting nothing.
        ingest = await target.ingest(
            data, device_id, session_factory,
            decoder_name=ftp_config.decoder or None,
            filename=None if ftp_config.decoder else original_name,
            last_successful_poll=last_successful_poll,
        )
        decision = decide_advancement(
            ingest, consecutive_errors=consecutive_errors,
        )

        # Rotate mode is holding the controller's ONLY copy.  Deletion needs a
        # full ADVANCE: on a PARTIAL the spine has just opened a review that
        # says some of this file was not read, and correct-later is impossible
        # once the bytes are gone (ADR-0034).  The file keeps its `.tsigma.`
        # name and is re-offered as a leftover next cycle.
        if decision.action is not Advancement.ADVANCE:
            error = decision.error or ingest.error
            logger.error(
                "Ingest of %s for device %s did not complete (%s) - file NOT "
                "deleted, will retry next cycle: %s",
                filename, device_id, ingest.outcome.value, error,
            )
            await target.record_error(
                self.name, device_id, session_factory, error,
            )
            if decision.alert:
                await alert_repeated_failures(
                    device_type=target.device_type,
                    device_id=device_id,
                    method=self.name,
                    consecutive_errors=consecutive_errors + 1,
                    error=error,
                )
            return False

        # Only delete after a fully advanced ingest
        try:
            await client.delete(file_path)
        except Exception:
            logger.exception(
                "Failed to delete %s after successful ingest — "
                "idempotent upsert will dedup on next cycle",
                filename,
            )

        logger.info(
            "Rotate ingested %s: %d events for device %s",
            filename, ingest.events_decoded, device_id,
        )

        await self._save_checkpoint(
            target,
            device_id,
            session_factory,
            last_filename=original_name,
            new_events=ingest.events_inserted,
            new_absorbed=ingest.duplicates_absorbed,
            new_files=1,
        )
        return True

    # -------------------------------------------------------------------
    # Poll dispatch
    # -------------------------------------------------------------------

    async def poll_once(
        self,
        device_id: str,
        config: dict[str, Any],
        session_factory,
        *,
        target: Optional[IngestionTarget] = None,
    ) -> None:
        """
        Execute one poll cycle for a single device.

        Dispatches to passive or rotate mode based on config.  The
        ``target`` selects where decoded events and checkpoints go;
        ``None`` defaults to ``ControllerTarget()`` for back-compat.
        """
        if target is None:
            target = ControllerTarget()

        ftp_config = self._build_config(device_id, config)
        client = self._create_client(ftp_config)

        try:
            await client.connect()
        except Exception as exc:
            logger.error(
                "Connection failed to %s://%s:%d for device %s",
                ftp_config.protocol.value,
                ftp_config.host,
                ftp_config.effective_port,
                device_id,
            )
            await target.record_error(
                self.name, device_id, session_factory, str(exc),
            )
            return

        try:
            if ftp_config.mode == FTPMode.ROTATE:
                await self._poll_rotate(
                    client, ftp_config, device_id, session_factory, target,
                )
            else:
                await self._poll_passive(
                    client, ftp_config, device_id, session_factory, target,
                )
        except Exception as exc:
            logger.exception("Poll cycle failed for device %s", device_id)
            await target.record_error(
                self.name, device_id, session_factory, str(exc),
            )
        finally:
            await client.disconnect()

    async def _poll_passive(
        self,
        client: _FileTransferClient,
        ftp_config: FTPPullConfig,
        device_id: str,
        session_factory,
        target: IngestionTarget,
    ) -> None:
        """Execute one passive-mode poll cycle (original behavior).

        Non-destructive: lists files, filters by checkpoint, downloads
        new ones, decodes, persists.  Never modifies controller files.
        """
        checkpoint = await target.load_checkpoint(
            self.name, device_id, session_factory,
        )

        # List and filter remote files
        all_files = await client.list_dir(
            ftp_config.remote_dir,
            recursive=ftp_config.recursive,
            max_depth=ftp_config.max_depth,
            follow_symlinks=ftp_config.follow_symlinks,
        )
        matching_files = [
            rf for rf in all_files
            if PurePosixPath(rf.name).suffix.lower() in ftp_config.file_extensions
        ]
        if not matching_files:
            logger.debug("No matching files for device %s", device_id)
            return

        # Quick change detection via files_hash
        current_hash = _compute_files_hash([rf.name for rf in matching_files])
        if checkpoint and checkpoint.files_hash == current_hash:
            logger.debug(
                "No new files for device %s (hash unchanged)", device_id,
            )
            return

        # Determine which files are new
        new_files = self._filter_new_files(matching_files, checkpoint, current_hash)
        if not new_files:
            logger.debug(
                "No new files after checkpoint filter for %s", device_id,
            )
            return

        # Download, decode, persist
        prior_mtime = checkpoint.last_file_mtime if checkpoint else None
        (
            total_inserted,
            total_files,
            newest_filename,
            newest_mtime,
            total_absorbed,
            all_advanced,
        ) = await self._download_and_ingest(
            client, new_files, ftp_config,
            device_id, session_factory, prior_mtime, target,
            checkpoint.last_successful_poll if checkpoint else None,
            (checkpoint.consecutive_errors or 0) if checkpoint else 0,
        )

        # Update checkpoint after successful ingest.
        #
        # While ANY file is still outstanding both file-identity markers are
        # withheld, and this is the whole of the stranding fix.  `files_hash`
        # covers the entire listing, so storing it after a partial cycle makes
        # the next poll match on the hash and return before it looks at any
        # file; `last_file_mtime`, advanced to a later sibling's mtime, strands
        # the same file a second way through the mtime filter.  Either one, on
        # its own, is a success quietly burying an earlier failure.
        if total_files > 0:
            await self._save_checkpoint(
                target,
                device_id,
                session_factory,
                last_filename=newest_filename,
                last_file_mtime=newest_mtime if all_advanced else None,
                files_hash=current_hash if all_advanced else None,
                new_events=total_inserted,
                new_absorbed=total_absorbed,
                new_files=total_files,
            )
