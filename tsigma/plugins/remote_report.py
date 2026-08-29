"""RemoteReport - a report plugin that runs out of process.

Satisfies the same `Report` interface the in-process reports do, so everything
above `ReportRegistry` (the API layer, exporters, the scheduler) cannot tell the
difference. `execute` calls `Report.Generate` over gRPC and reassembles the
streamed result - one ViewModel header then zero or more Arrow record batches -
into the DataFrame the rest of the app expects.

The `session` argument is accepted and NOT used: a remote report gets its data by
dialling back to host-served broker services, never by touching the database. The
signature keeps it because the in-process contract has it.
"""

import logging

import pandas as pd
import pyarrow as pa
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct
from sqlalchemy.ext.asyncio import AsyncSession

from ..reports.registry import Report, ReportMetadata
from .connection import PluginConnection

logger = logging.getLogger(__name__)

_CATEGORY = {0: "standard", 1: "dashboard", 2: "standard", 3: "detailed"}
_ESTIMATED = {0: "medium", 1: "fast", 2: "medium", 3: "slow"}


class RemoteReportError(RuntimeError):
    """The remote report stream was malformed or the plugin failed."""


def arrow_batches_to_dataframe(batches: list[bytes], columns: list[str]) -> pd.DataFrame:
    """Reassemble streamed Arrow IPC batches into one DataFrame.

    An empty result still yields a DataFrame WITH the declared columns - the
    in-process reports guarantee that and the exporters rely on it.
    """
    if not batches:
        return pd.DataFrame(columns=columns)

    tables = []
    for blob in batches:
        with pa.ipc.open_stream(pa.BufferReader(blob)) as reader:
            tables.append(reader.read_all())
    table = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    frame = table.to_pandas()
    if columns and list(frame.columns) != columns:
        # Trust the declared column order; the wire schema is authoritative for
        # types, the ViewModel for order.
        frame = frame.reindex(columns=columns)
    return frame


def dataframe_to_arrow_batch(frame: pd.DataFrame) -> bytes:
    """Serialize a DataFrame as one Arrow IPC stream. Used by report plugins."""
    table = pa.Table.from_pandas(frame, preserve_index=False)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def metadata_from_describe(response) -> ReportMetadata:
    """Build the in-process ReportMetadata from a plugin's Describe response."""
    return ReportMetadata(
        name=response.name,
        description=response.description,
        category=_CATEGORY.get(response.category, "standard"),
        estimated_time=_ESTIMATED.get(response.estimated_time, "medium"),
        supports_export=response.supports_export,
        export_formats=list(response.export_formats) or None,
    )


class RemoteReport(Report):
    """A `Report` whose work happens in a plugin process."""

    def __init__(
        self,
        connection: PluginConnection,
        metadata: ReportMetadata,
        params_schema: dict | None = None,
    ):
        self.connection = connection
        self.metadata = metadata
        self.params_schema = params_schema or {}
        self._preferred_status: int | None = None

    # ------------------------------------------------------------------ stub
    def _stub(self):
        if self.connection.channel is None:
            raise RemoteReportError(f"{self.metadata.name}: plugin is not connected")
        from tsigma.report.v1 import report_pb2_grpc

        return report_pb2_grpc.ReportStub(self.connection.channel)

    # --------------------------------------------------------------- execute
    async def execute(self, params, session: AsyncSession) -> pd.DataFrame:
        """Run the report in the plugin and return its rows.

        `session` is intentionally unused - see the module docstring.
        """
        from tsigma.report.v1 import report_pb2

        payload = params if isinstance(params, dict) else params.model_dump(mode="json")
        request = report_pb2.GenerateRequest(params=ParseDict(payload, Struct()))

        view_model = None
        batches: list[bytes] = []
        async for result in self._stub().Generate(request):
            kind = result.WhichOneof("payload")
            if kind == "view_model":
                if view_model is not None:
                    raise RemoteReportError(
                        f"{self.metadata.name}: more than one ViewModel in the stream"
                    )
                view_model = result.view_model
            elif kind == "rows_arrow_ipc":
                if view_model is None:
                    raise RemoteReportError(
                        f"{self.metadata.name}: Arrow batch before the ViewModel"
                    )
                batches.append(result.rows_arrow_ipc)

        if view_model is None:
            raise RemoteReportError(f"{self.metadata.name}: stream ended with no ViewModel")

        self._preferred_status = view_model.preferred_http_status or None
        columns = list(view_model.columns)
        frame = arrow_batches_to_dataframe(batches, columns)

        if view_model.empty and not frame.empty:
            logger.warning(
                "%s: ViewModel says empty but %d rows arrived", self.metadata.name, len(frame)
            )
        return frame

    # -------------------------------------------------------- status override
    def preferred_http_status_for_run(self) -> int | None:
        """The status the last run asked for, if any (ViewModel.preferred_http_status)."""
        return self._preferred_status

    def describe_params(self) -> dict:
        """The plugin's param schema, as the API layer serves it."""
        return self.params_schema


def params_schema_from_describe(response) -> dict:
    """Flatten DescribeResponse.params into a JSON-schema-ish dict."""
    return {
        "properties": {f.name: MessageToDict(f) for f in response.params},
        "required": [f.name for f in response.params if getattr(f, "required", False)],
    }
