"""Cold-tier query adapter.

Reads partitioned Parquet archives written by `tsigma/scheduler/jobs/export_cold.py`:

    {storage_cold_path}/{signal_id}/{YYYY-MM-DD}/events.parquet

Supports both the FilesystemBackend and the S3Backend via internal isinstance
dispatch — one concrete class, branch per backend type, no abstract base.

See `docs/developers/STORAGE.md#cold-tier-query` for the API and design.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd

from tsigma.storage.base import StorageBackend
from tsigma.storage.filesystem import FilesystemBackend
from tsigma.storage.s3 import S3Backend

# Pattern: {signal_id}/{YYYY-MM-DD}/events.parquet — capture the date segment
_PARTITION_KEY_RE = re.compile(r"^[^/]+/(\d{4}-\d{2}-\d{2})/events\.parquet$")

# Canonical DataFrame columns returned by fetch_events. Matches the hot-tier
# SDK shape so tier-aware callers can pd.concat without column alignment.
_EVENT_COLUMNS = ["event_code", "event_param", "event_time"]

# Hardcoded allowlist of cold-tier event-schema columns. Used to validate any
# column reference coming from group_by, filters keys, or column-flavored
# agg specs. count_if / max_if raw expressions are NOT validated here — they
# are SDK-internal trusted code, not API-surface input.
_ALLOWED_COLUMNS: frozenset[str] = frozenset(
    {"signal_id", "event_time", "event_code", "event_param", "device_id"}
)


def _validate_column(name: str) -> None:
    if name not in _ALLOWED_COLUMNS:
        raise ValueError(
            f"column {name!r} is not in the cold-tier event schema "
            f"(allowed: {sorted(_ALLOWED_COLUMNS)})"
        )


def _empty_events_df() -> pd.DataFrame:
    """Return an empty DataFrame with the canonical event-column schema."""
    return pd.DataFrame(
        {
            "event_code": pd.Series([], dtype="int64"),
            "event_param": pd.Series([], dtype="int64"),
            "event_time": pd.Series([], dtype="datetime64[us]"),
        }
    )


def _int_list_sql(values: Iterable[int]) -> str:
    """Inline a list of ints into a SQL IN-list. Safe: each value is cast to int."""
    return ",".join(str(int(v)) for v in values)


@dataclass(frozen=True)
class ColdPartition:
    """One cold-tier Parquet partition (one signal, one date).

    `key` is the storage key (relative path for filesystem, bucket-relative
    key for S3) that the caller passes to `backend.get(...)` or builds into
    a DuckDB URL.
    """

    signal_id: str
    event_date: date
    key: str


class ColdTierQuery:
    """Query adapter for the cold-tier Parquet archive."""

    def __init__(self, backend: StorageBackend) -> None:
        self.backend = backend

    async def list_partitions(
        self,
        signal_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[ColdPartition]:
        """List existing cold partitions for a signal within an optional date range.

        Returns partitions sorted by `event_date` ascending. Empty list when
        the signal has no partitions or when the date range matches nothing.
        """
        if isinstance(self.backend, FilesystemBackend):
            return self._list_partitions_filesystem(signal_id, start_date, end_date)
        if isinstance(self.backend, S3Backend):
            return await self._list_partitions_s3(signal_id, start_date, end_date)
        raise TypeError(
            f"Unsupported storage backend type: {type(self.backend).__name__}"
        )

    def _list_partitions_filesystem(
        self,
        signal_id: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[ColdPartition]:
        signal_dir = self.backend._base / signal_id
        if not signal_dir.is_dir():
            return []

        partitions: list[ColdPartition] = []
        for date_dir in signal_dir.iterdir():
            if not date_dir.is_dir():
                continue
            try:
                event_date = date.fromisoformat(date_dir.name)
            except ValueError:
                continue
            if start_date is not None and event_date < start_date:
                continue
            if end_date is not None and event_date > end_date:
                continue
            parquet = date_dir / "events.parquet"
            if not parquet.is_file():
                continue
            partitions.append(
                ColdPartition(
                    signal_id=signal_id,
                    event_date=event_date,
                    key=f"{signal_id}/{date_dir.name}/events.parquet",
                )
            )

        partitions.sort(key=lambda p: p.event_date)
        return partitions

    async def _list_partitions_s3(
        self,
        signal_id: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[ColdPartition]:
        partitions: list[ColdPartition] = []
        async for stored in self.backend.list_files(prefix=f"{signal_id}/"):
            match = _PARTITION_KEY_RE.match(stored.key)
            if match is None:
                continue
            try:
                event_date = date.fromisoformat(match.group(1))
            except ValueError:
                continue
            if start_date is not None and event_date < start_date:
                continue
            if end_date is not None and event_date > end_date:
                continue
            partitions.append(
                ColdPartition(
                    signal_id=signal_id,
                    event_date=event_date,
                    key=stored.key,
                )
            )

        partitions.sort(key=lambda p: p.event_date)
        return partitions

    async def _list_all_signal_ids(self) -> list[str]:
        """Enumerate every signal_id present at the top level of cold storage.

        Used by ``aggregate_events`` when ``signal_id_or_ids="All"`` to scan
        the whole archive (fleet-wide mode). Returns deduplicated, sorted IDs;
        empty list when storage is empty.

        Filesystem branch: iterates ``backend._base`` directly.
        S3 branch: paginates ``list_files(prefix="")``, splits each key on the
        first ``/`` to get the signal_id segment, deduplicates.
        """
        if isinstance(self.backend, FilesystemBackend):
            base = self.backend._base
            if not base.is_dir():
                return []
            return sorted(d.name for d in base.iterdir() if d.is_dir())
        if isinstance(self.backend, S3Backend):
            signal_ids: set[str] = set()
            async for stored in self.backend.list_files(prefix=""):
                head, sep, _ = stored.key.partition("/")
                if sep:
                    signal_ids.add(head)
            return sorted(signal_ids)
        raise TypeError(
            f"Unsupported storage backend type: {type(self.backend).__name__}"
        )

    async def fetch_events(
        self,
        signal_id: str,
        start: datetime,
        end: datetime,
        event_codes: Iterable[int] | None = None,
        *,
        event_param_in: Iterable[int] | None = None,
        where_sql_fragment: str | None = None,
    ) -> pd.DataFrame:
        """Read cold-tier events for a signal in a time window.

        Three predicate modes — at most one may be supplied:

        * **Filter mode** (default): pass ``event_codes`` (and optionally
          ``event_param_in``). The WHERE clause is built as
          ``event_code IN (...) [AND event_param IN (...)]``.
        * **Fragment mode**: pass ``where_sql_fragment`` — a DuckDB SQL
          expression that replaces the IN-list predicate entirely. Used by
          ``fetch_events_split`` (B8) to express ``phase_codes OR
          (det_codes AND event_param IN det_channels)`` in one scan.
          **Trust contract:** ``where_sql_fragment`` is SDK-internal code,
          NOT validated against an allowlist — same boundary as
          ``aggregate_events``'s ``count_if``/``max_if`` expressions.
          Callers must never interpolate API-surface input.
        * **No-filter mode**: pass neither. Returns every row in the
          ``[start, end]`` window for the signal, regardless of event code.
          Used by the GraphQL ``events`` query when the client omits the
          ``event_codes`` argument (B9).

        Passing BOTH ``event_codes`` and ``where_sql_fragment`` is
        rejected as ambiguous.

        Returns DataFrame with columns ``event_code``, ``event_param``,
        ``event_time``, ordered by ``event_time`` ascending. Empty
        DataFrame with the canonical column schema when no partitions
        exist for the signal or no rows match the filters.
        """
        if event_codes is not None and where_sql_fragment is not None:
            raise ValueError(
                "fetch_events: pass at most one of event_codes or "
                "where_sql_fragment, not both"
            )

        partitions = await self.list_partitions(
            signal_id, start_date=start.date(), end_date=end.date()
        )
        if not partitions:
            return _empty_events_df()

        codes = list(event_codes) if event_codes is not None else None
        params = list(event_param_in) if event_param_in is not None else None

        if isinstance(self.backend, FilesystemBackend):
            return await asyncio.to_thread(
                self._fetch_events_filesystem,
                partitions, start, end, codes, params, where_sql_fragment,
            )
        if isinstance(self.backend, S3Backend):
            return await asyncio.to_thread(
                self._fetch_events_s3,
                partitions, start, end, codes, params, where_sql_fragment,
            )
        raise TypeError(
            f"Unsupported storage backend type: {type(self.backend).__name__}"
        )

    def _fetch_events_filesystem(
        self,
        partitions: list[ColdPartition],
        start: datetime,
        end: datetime,
        event_codes: list[int] | None,
        event_param_in: list[int] | None,
        where_sql_fragment: str | None,
    ) -> pd.DataFrame:
        import duckdb

        paths = [str(self.backend._resolve(p.key)) for p in partitions]
        sql, params = _build_fetch_events_sql(
            event_codes, event_param_in, where_sql_fragment
        )
        con = duckdb.connect(":memory:")
        try:
            return con.execute(sql, [paths, start, end, *params]).fetchdf()
        finally:
            con.close()

    def _fetch_events_s3(
        self,
        partitions: list[ColdPartition],
        start: datetime,
        end: datetime,
        event_codes: list[int] | None,
        event_param_in: list[int] | None,
        where_sql_fragment: str | None,
    ) -> pd.DataFrame:
        import duckdb

        backend = self.backend  # narrowed S3Backend
        s3_urls = [f"s3://{backend._bucket}/{p.key}" for p in partitions]
        sql, params = _build_fetch_events_sql(
            event_codes, event_param_in, where_sql_fragment
        )

        con = duckdb.connect(":memory:")
        try:
            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")
            if backend._access_key and backend._secret_key:
                use_ssl = (
                    backend._endpoint_url is None
                    or backend._endpoint_url.startswith("https://")
                )
                endpoint = (
                    _strip_scheme(backend._endpoint_url)
                    if backend._endpoint_url
                    else "s3.amazonaws.com"
                )
                con.execute(
                    """
                    CREATE SECRET cold_tier_s3 (
                        TYPE S3,
                        KEY_ID ?,
                        SECRET ?,
                        REGION ?,
                        ENDPOINT ?,
                        URL_STYLE 'path',
                        USE_SSL ?
                    )
                    """,
                    [
                        backend._access_key,
                        backend._secret_key,
                        backend._region,
                        endpoint,
                        use_ssl,
                    ],
                )
            return con.execute(sql, [s3_urls, start, end, *params]).fetchdf()
        finally:
            con.close()

    async def aggregate_events(
        self,
        signal_id_or_ids: str | list[str] | None,
        start: datetime,
        end: datetime,
        *,
        agg: list[tuple],
        group_by: list[str] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Aggregate cold-tier events for one or more signals in a time window.

        See ``docs/developers/STORAGE.md#cold-tier-query`` for the full agg-spec
        table. Returns a DataFrame with the group_by columns first (preserved
        order), then one ``agg_<i>`` column per spec. Empty DataFrame with the
        same column shape when no partitions exist or no rows match.

        ``signal_id_or_ids`` supports four modes:

        * ``None`` or ``[]`` → no signals requested; returns an empty
          DataFrame immediately with the canonical column shape. No
          partition enumeration, no I/O.
        * ``"All"`` → enumerates every signal present in the storage
          backend (fleet-wide mode). Falls through the empty-partitions
          short-circuit when storage is empty.
        * ``str`` → single specific signal.
        * ``list[str]`` → specific signal subset.
        """
        group_by_list = list(group_by) if group_by else []

        # Empty-result short-circuit: None or [] means "no signals requested",
        # so return an empty-but-shaped DataFrame without enumerating signals
        # or scanning any partitions.
        if signal_id_or_ids is None or (
            isinstance(signal_id_or_ids, list) and len(signal_id_or_ids) == 0
        ):
            return _empty_aggregate_df(agg, group_by_list)

        if signal_id_or_ids == "All":
            signal_ids = await self._list_all_signal_ids()
        elif isinstance(signal_id_or_ids, str):
            signal_ids = [signal_id_or_ids]
        else:
            signal_ids = list(signal_id_or_ids)

        all_partitions: list[ColdPartition] = []
        for sid in signal_ids:
            partitions = await self.list_partitions(
                sid, start_date=start.date(), end_date=end.date()
            )
            all_partitions.extend(partitions)

        if not all_partitions:
            return _empty_aggregate_df(agg, group_by_list)

        if isinstance(self.backend, FilesystemBackend):
            return await asyncio.to_thread(
                self._aggregate_events_filesystem,
                all_partitions, start, end, agg, group_by_list, filters,
            )
        if isinstance(self.backend, S3Backend):
            return await asyncio.to_thread(
                self._aggregate_events_s3,
                all_partitions, start, end, agg, group_by_list, filters,
            )
        raise TypeError(
            f"Unsupported storage backend type: {type(self.backend).__name__}"
        )

    def _aggregate_events_filesystem(
        self,
        partitions: list[ColdPartition],
        start: datetime,
        end: datetime,
        agg: list[tuple],
        group_by: list[str],
        filters: dict[str, Any] | None,
    ) -> pd.DataFrame:
        import duckdb

        paths = [str(self.backend._resolve(p.key)) for p in partitions]
        sql = _build_aggregate_events_sql(agg, group_by, filters)
        con = duckdb.connect(":memory:")
        try:
            return con.execute(sql, [paths, start, end]).fetchdf()
        finally:
            con.close()

    def _aggregate_events_s3(
        self,
        partitions: list[ColdPartition],
        start: datetime,
        end: datetime,
        agg: list[tuple],
        group_by: list[str],
        filters: dict[str, Any] | None,
    ) -> pd.DataFrame:
        import duckdb

        backend = self.backend  # narrowed S3Backend
        s3_urls = [f"s3://{backend._bucket}/{p.key}" for p in partitions]
        sql = _build_aggregate_events_sql(agg, group_by, filters)

        con = duckdb.connect(":memory:")
        try:
            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")
            if backend._access_key and backend._secret_key:
                use_ssl = (
                    backend._endpoint_url is None
                    or backend._endpoint_url.startswith("https://")
                )
                endpoint = (
                    _strip_scheme(backend._endpoint_url)
                    if backend._endpoint_url
                    else "s3.amazonaws.com"
                )
                con.execute(
                    """
                    CREATE SECRET cold_tier_s3 (
                        TYPE S3,
                        KEY_ID ?,
                        SECRET ?,
                        REGION ?,
                        ENDPOINT ?,
                        URL_STYLE 'path',
                        USE_SSL ?
                    )
                    """,
                    [
                        backend._access_key,
                        backend._secret_key,
                        backend._region,
                        endpoint,
                        use_ssl,
                    ],
                )
            return con.execute(sql, [s3_urls, start, end]).fetchdf()
        finally:
            con.close()


def _build_aggregate_events_sql(
    agg: list[tuple],
    group_by: list[str],
    filters: dict[str, Any] | None,
) -> str:
    """Compose the aggregation SELECT used by both backend branches.

    The caller binds the parquet-source list, start, and end as the three
    placeholders. Column references from group_by, filters keys, and
    column-flavored agg specs are validated against ``_ALLOWED_COLUMNS``;
    ``count_if`` / ``max_if`` raw expressions are NOT validated (SDK-internal
    trust). Filter values are int-cast and inlined (controlled values).
    """
    projections: list[str] = []

    for col in group_by:
        _validate_column(col)
        projections.append(col)

    for i, spec in enumerate(agg):
        op = spec[0]
        if op == "count" and len(spec) == 2 and spec[1] == "*":
            projections.append(f"COUNT(*) AS agg_{i}")
        elif op == "count_if" and len(spec) == 2:
            expr = spec[1]
            projections.append(f"COUNT(*) FILTER (WHERE {expr}) AS agg_{i}")
        elif op == "max" and len(spec) == 2:
            col = spec[1]
            _validate_column(col)
            projections.append(f"MAX({col}) AS agg_{i}")
        elif op == "max_if" and len(spec) == 3:
            col, expr = spec[1], spec[2]
            _validate_column(col)
            projections.append(f"MAX({col}) FILTER (WHERE {expr}) AS agg_{i}")
        elif op == "min" and len(spec) == 2:
            col = spec[1]
            _validate_column(col)
            projections.append(f"MIN({col}) AS agg_{i}")
        elif op == "sum" and len(spec) == 2:
            col = spec[1]
            _validate_column(col)
            projections.append(f"SUM({col}) AS agg_{i}")
        else:
            raise ValueError(f"unsupported aggregation spec: {spec!r}")

    where_parts: list[str] = ["event_time >= ?", "event_time <= ?"]
    if filters:
        for col, val in filters.items():
            _validate_column(col)
            if isinstance(val, list):
                where_parts.append(f"{col} IN ({_int_list_sql(val)})")
            else:
                where_parts.append(f"{col} = {int(val)}")

    sql = (
        f"SELECT {', '.join(projections)}\n"
        f"FROM read_parquet(?)\n"
        f"WHERE {' AND '.join(where_parts)}\n"
    )
    if group_by:
        sql += f"GROUP BY {', '.join(group_by)}\n"
    return sql


def _empty_aggregate_df(agg: list[tuple], group_by: list[str]) -> pd.DataFrame:
    """Return an empty DataFrame with the columns aggregate_events would produce."""
    cols = list(group_by) + [f"agg_{i}" for i in range(len(agg))]
    return pd.DataFrame({c: [] for c in cols})


def _build_fetch_events_sql(
    event_codes: list[int] | None,
    event_param_in: list[int] | None,
    where_sql_fragment: str | None,
) -> tuple[str, list]:
    """Compose the SELECT statement used by both backend branches.

    Returns (sql, extra_params). The caller binds the parquet-source list,
    start, and end as the first three placeholders; ``extra_params`` is empty
    in the current shape (event_codes / event_param are inlined after int
    casting — controlled values, no injection surface).

    When ``where_sql_fragment`` is supplied, it replaces the
    ``event_code IN (...) [AND event_param IN (...)]`` predicate entirely.
    The fragment is interpolated as-is; ``ColdTierQuery.fetch_events``
    enforces the trust contract (SDK-internal only).
    """
    sql = (
        "        SELECT event_code, event_param, event_time\n"
        "        FROM read_parquet(?)\n"
        "        WHERE event_time >= ?\n"
        "          AND event_time <= ?\n"
    )
    if where_sql_fragment is not None:
        sql += f"          AND ({where_sql_fragment})\n"
    elif event_codes is not None:
        codes_sql = _int_list_sql(event_codes)
        sql += f"          AND event_code IN ({codes_sql})\n"
        if event_param_in is not None:
            params_sql = _int_list_sql(event_param_in)
            sql += f"          AND event_param IN ({params_sql})\n"
    # else: no event-code filter (B9 third mode). Time bounds alone constrain rows.
    sql += "        ORDER BY event_time"
    return sql, []


def _strip_scheme(url: str) -> str:
    """Return ``host:port`` portion of a URL — DuckDB CREATE SECRET ENDPOINT wants no scheme."""
    for scheme in ("http://", "https://"):
        if url.startswith(scheme):
            return url[len(scheme):]
    return url
