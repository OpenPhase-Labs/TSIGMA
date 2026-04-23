"""
GraphQL schema and query definitions for TSIGMA.

Defines the root Query type and creates the Strawberry schema
with a FastAPI-compatible GraphQLRouter.
"""

import logging
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import strawberry
from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from strawberry.fastapi import GraphQLRouter

from tsigma.dependencies import get_session
from tsigma.models import (
    Approach,
    ControllerEventLog,
    Corridor,
    Detector,
    Jurisdiction,
    Region,
    Signal,
)
from tsigma.reports.registry import ReportRegistry

from .types import (
    ApproachType,
    CorridorType,
    DetectorType,
    EventType,
    JurisdictionType,
    RegionType,
    ReportInfoType,
    ReportResultType,
    SignalType,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ORM -> Strawberry converters
# ---------------------------------------------------------------------------

def _decimal_to_float(value: Decimal | None) -> float | None:
    """Convert a Decimal to float, returning None if input is None."""
    return float(value) if value is not None else None


def _uuid_to_str(value: UUID | None) -> str | None:
    """Convert a UUID to string, returning None if input is None."""
    return str(value) if value is not None else None


def _detector_to_type(det: Detector) -> DetectorType:
    return DetectorType(
        detector_id=str(det.detector_id),
        approach_id=str(det.approach_id),
        detector_channel=det.detector_channel,
        distance_from_stop_bar=det.distance_from_stop_bar,
        min_speed_filter=det.min_speed_filter,
        decision_point=det.decision_point,
        movement_delay=det.movement_delay,
        lane_number=det.lane_number,
    )


def _approach_to_type(
    app: Approach,
    detectors: list[DetectorType] | None = None,
) -> ApproachType:
    return ApproachType(
        approach_id=str(app.approach_id),
        signal_id=app.signal_id,
        direction_type_id=app.direction_type_id,
        description=app.description,
        mph=app.mph,
        protected_phase_number=app.protected_phase_number,
        is_protected_phase_overlap=app.is_protected_phase_overlap,
        permissive_phase_number=app.permissive_phase_number,
        is_permissive_phase_overlap=app.is_permissive_phase_overlap,
        ped_phase_number=app.ped_phase_number,
        detectors=detectors or [],
    )


def _signal_to_type(
    sig: Signal,
    approaches: list[ApproachType] | None = None,
) -> SignalType:
    return SignalType(
        signal_id=sig.signal_id,
        primary_street=sig.primary_street,
        secondary_street=sig.secondary_street,
        latitude=_decimal_to_float(sig.latitude),
        longitude=_decimal_to_float(sig.longitude),
        enabled=sig.enabled,
        note=sig.note,
        approaches=approaches or [],
    )


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

@strawberry.type
class Query:
    """Root query type for the TSIGMA GraphQL API."""

    @strawberry.field(description="List signals with optional filters.")
    async def signals(
        self,
        info: strawberry.types.Info,
        region_id: str | None = None,
        corridor_id: str | None = None,
        jurisdiction_id: str | None = None,
        enabled_only: bool = True,
    ) -> list[SignalType]:
        session: AsyncSession = info.context["session"]

        stmt = select(Signal)
        if enabled_only:
            stmt = stmt.where(Signal.enabled.is_(True))
        if region_id is not None:
            stmt = stmt.where(Signal.region_id == region_id)
        if corridor_id is not None:
            stmt = stmt.where(Signal.corridor_id == corridor_id)
        if jurisdiction_id is not None:
            stmt = stmt.where(Signal.jurisdiction_id == jurisdiction_id)

        result = await session.execute(stmt)
        rows = result.scalars().all()
        return [_signal_to_type(sig) for sig in rows]

    @strawberry.field(
        description="Get a single signal by ID with nested approaches and detectors.",
    )
    async def signal(
        self,
        info: strawberry.types.Info,
        signal_id: str,
    ) -> SignalType | None:
        session: AsyncSession = info.context["session"]

        sig = await session.get(Signal, signal_id)
        if sig is None:
            return None

        # Fetch approaches for this signal
        app_result = await session.execute(
            select(Approach).where(Approach.signal_id == signal_id)
        )
        approaches = app_result.scalars().all()

        # Batch-fetch detectors for all approaches (avoid N+1)
        approach_ids = [a.approach_id for a in approaches]
        detector_map: dict[UUID, list[DetectorType]] = {}
        if approach_ids:
            det_result = await session.execute(
                select(Detector).where(Detector.approach_id.in_(approach_ids))
            )
            for det in det_result.scalars().all():
                detector_map.setdefault(det.approach_id, []).append(
                    _detector_to_type(det)
                )

        approach_types = [
            _approach_to_type(a, detector_map.get(a.approach_id, []))
            for a in approaches
        ]

        return _signal_to_type(sig, approach_types)

    @strawberry.field(description="List all regions.")
    async def regions(self, info: strawberry.types.Info) -> list[RegionType]:
        session: AsyncSession = info.context["session"]
        result = await session.execute(select(Region))
        return [
            RegionType(
                region_id=str(r.region_id),
                description=r.description,
                parent_region_id=_uuid_to_str(r.parent_region_id),
            )
            for r in result.scalars().all()
        ]

    @strawberry.field(description="List all jurisdictions.")
    async def jurisdictions(self, info: strawberry.types.Info) -> list[JurisdictionType]:
        session: AsyncSession = info.context["session"]
        result = await session.execute(select(Jurisdiction))
        return [
            JurisdictionType(
                jurisdiction_id=str(j.jurisdiction_id),
                name=j.name,
                mpo_name=j.mpo_name,
                county_name=j.county_name,
            )
            for j in result.scalars().all()
        ]

    @strawberry.field(description="List all corridors.")
    async def corridors(self, info: strawberry.types.Info) -> list[CorridorType]:
        session: AsyncSession = info.context["session"]
        result = await session.execute(select(Corridor))
        return [
            CorridorType(
                corridor_id=str(c.corridor_id),
                name=c.name,
                description=c.description,
            )
            for c in result.scalars().all()
        ]

    @strawberry.field(description="Query controller event log entries.")
    async def events(
        self,
        info: strawberry.types.Info,
        signal_id: str,
        start: datetime,
        end: datetime,
        event_codes: list[int] | None = None,
        limit: int = 10000,
    ) -> list[EventType]:
        session: AsyncSession = info.context["session"]

        stmt = (
            select(ControllerEventLog)
            .where(
                ControllerEventLog.signal_id == signal_id,
                ControllerEventLog.event_time >= start,
                ControllerEventLog.event_time <= end,
            )
            .order_by(ControllerEventLog.event_time)
            .limit(limit)
        )
        if event_codes:
            stmt = stmt.where(ControllerEventLog.event_code.in_(event_codes))

        result = await session.execute(stmt)
        return [
            EventType(
                signal_id=e.signal_id,
                event_time=e.event_time,
                event_code=e.event_code,
                event_param=e.event_param,
            )
            for e in result.scalars().all()
        ]

    @strawberry.field(description="List all registered report plugins.")
    async def available_reports(self, info: strawberry.types.Info) -> list[ReportInfoType]:
        reports = ReportRegistry.list_all()
        return [
            ReportInfoType(
                name=name,
                description=getattr(cls, "description", ""),
                category=getattr(cls, "category", ""),
                estimated_time=getattr(cls, "estimated_time", ""),
                export_formats=getattr(cls, "export_formats", ["csv", "json"]),
            )
            for name, cls in reports.items()
        ]

    @strawberry.field(description="Execute a registered report and return its results.")
    async def run_report(
        self,
        info: strawberry.types.Info,
        report_name: str,
        params: strawberry.scalars.JSON,
    ) -> ReportResultType:
        session: AsyncSession = info.context["session"]

        try:
            report_cls = ReportRegistry.get(report_name)
        except ValueError:
            return ReportResultType(
                status="error",
                data={"error": f"Unknown report: {report_name}"},
            )

        try:
            report = report_cls()
            data = await report.execute(params, session)
            return ReportResultType(status="ok", data=data)
        except Exception:
            logger.exception("Report '%s' failed", report_name)
            return ReportResultType(
                status="error",
                data={"error": f"Report execution failed: {report_name}"},
            )


# ---------------------------------------------------------------------------
# Schema + Router
# ---------------------------------------------------------------------------

schema = strawberry.Schema(query=Query)


def get_context(session: AsyncSession = Depends(get_session)):
    """Provide the database session to all GraphQL resolvers via context."""
    return {"session": session}


graphql_router = GraphQLRouter(schema, context_getter=get_context)
