"""
Report registry for TSIGMA.

Reports are self-registering plugins that generate analytics outputs.
Each report defines typed Pydantic params. Reports return pandas
DataFrames; the framework handles serialization to CSV/JSON/NDJSON.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

import pandas as pd
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

TParams = TypeVar("TParams", bound=BaseModel)


class ReportResourceNotFoundError(LookupError):
    """Raised by a report when a required resource (signal, approach, etc.)
    does not exist. The Reports API translates this to HTTP 404.
    """


@dataclass
class ReportMetadata:
    """Metadata describing a report plugin."""

    name: str
    description: str
    category: str  # 'dashboard' | 'standard' | 'detailed'
    estimated_time: str  # 'fast' | 'medium' | 'slow'
    supports_export: bool = True
    export_formats: list[str] | None = None

    def __post_init__(self):
        if self.export_formats is None:
            self.export_formats = ["csv", "json"]


class Report(ABC, Generic[TParams]):
    """
    Base class for all report plugins.

    Subclass with typed Pydantic params:

        class MyParams(BaseModel):
            signal_id: str
            start: str
            end: str

        @ReportRegistry.register("my-report")
        class MyReport(Report[MyParams]):
            metadata = ReportMetadata(...)

            async def execute(self, params: MyParams, session) -> pd.DataFrame:
                ...
                return df

    The framework serializes the returned DataFrame to the requested
    export format (CSV, JSON, NDJSON). Report authors never handle
    serialization.
    """

    metadata: ReportMetadata

    @abstractmethod
    async def execute(
        self,
        params: TParams,
        session: AsyncSession,
    ) -> pd.DataFrame:
        """
        Execute the report and return results as a DataFrame.

        Args:
            params: Validated Pydantic params model.
            session: Database session.

        Returns:
            DataFrame with report results.

        Raises:
            ReportResourceNotFoundError: When a required resource
                (signal, approach, etc.) does not exist.  Surfaces as
                HTTP 404 via the Reports API.
        """
        ...

    @classmethod
    def preferred_http_status(cls, result: pd.DataFrame) -> int | None:
        """
        Optional hook: influence the HTTP status code the Reports API
        returns for a successful execution.

        Default returns ``None`` (use 200).  Gating/pre-check reports can
        override to return, e.g., 422 when the result indicates the
        signal is not eligible for downstream analysis.  The response
        body is the serialized DataFrame regardless.
        """
        return None

    async def export(
        self,
        params: TParams,
        session: AsyncSession,
        format: str = "csv",
    ) -> bytes:
        """
        Execute and serialize results to the requested format.

        Args:
            params: Validated Pydantic params model.
            session: Database session.
            format: Export format ('csv', 'json', 'ndjson').

        Returns:
            Encoded bytes of the serialized report.

        Raises:
            ValueError: If format is not supported by this report.
        """
        if (
            self.metadata.export_formats
            and format not in self.metadata.export_formats
        ):
            raise ValueError(
                f"Report '{self.metadata.name}' does not support "
                f"format '{format}'. Supported: {self.metadata.export_formats}"
            )
        df = await self.execute(params, session)
        return self._serialize(df, format)

    @staticmethod
    def _serialize(df: pd.DataFrame, format: str) -> bytes:
        """Serialize a DataFrame to the requested format."""
        if df.empty:
            if format == "csv":
                return (",".join(df.columns) + "\n").encode() if len(df.columns) else b""
            return b"[]\n" if format == "json" else b""

        if format == "json":
            return df.to_json(orient="records", date_format="iso").encode()
        elif format == "ndjson":
            lines = df.to_json(orient="records", lines=True, date_format="iso")
            return lines.encode()
        elif format == "csv":
            return df.to_csv(index=False).encode()
        raise ValueError(f"Unsupported format: {format}")


# Backward-compatible alias
BaseReport = Report


class ReportRegistry:
    """
    Central registry for all report plugins.

    Reports self-register using the @ReportRegistry.register decorator.
    """

    _reports: dict[str, type[Report]] = {}

    @classmethod
    def register(cls, name: str):
        """
        Register a report plugin.

        Usage:
            @ReportRegistry.register("approach-delay")
            class ApproachDelayReport(Report[ApproachDelayParams]):
                ...
        """

        def wrapper(report_class: type[Report]) -> type[Report]:
            cls._reports[name] = report_class
            return report_class

        return wrapper

    @classmethod
    def get(cls, name: str) -> type[Report]:
        """Get a registered report by name."""
        if name not in cls._reports:
            raise ValueError(f"Unknown report: {name}")
        return cls._reports[name]

    @classmethod
    def list_all(cls) -> dict[str, type[Report]]:
        """List all registered reports."""
        return cls._reports.copy()
