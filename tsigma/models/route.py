"""
Route (progression/coordination) models.

Represents ordered sequences of signals for progression analysis and coordination.
Different from Corridor - routes have ordered signals and phase configuration.
"""

from typing import Optional
from uuid import UUID

from sqlalchemy import (
    Boolean,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class Route(Base):
    """
    Progression/coordination route (e.g., "EB Progression - Peachtree St").

    Defines a coordinated route through multiple signals.
    Signals in route are ordered via RouteSignal.sequence_order.
    """

    __tablename__ = "route"

    route_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = {"schema": tsigma_schema("config")}


class RouteSignal(Base):
    """
    Signals in a route, with order/sequence.

    Links signals to routes with progression order (1st signal, 2nd signal, etc.).
    """

    __tablename__ = "route_signal"

    route_signal_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    route_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("route.route_id"),
        nullable=False,
    )
    signal_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("signal.signal_id"),
        nullable=False,
    )
    sequence_order: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint("route_id", "sequence_order", name="uq_route_signal_order"),
        UniqueConstraint("route_id", "signal_id", name="uq_route_signal_id"),
        Index("idx_route_signal_route", "route_id", "sequence_order"),
        {"schema": tsigma_schema("config")},
    )


class RoutePhase(Base):
    """
    Phase configuration for route signals.

    Defines which phases participate in the progression at each signal.
    """

    __tablename__ = "route_phase"

    route_phase_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    route_signal_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("route_signal.route_signal_id"),
        nullable=False,
    )
    phase_number: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    direction_type_id: Mapped[int] = mapped_column(
        SmallInteger,
        ForeignKey("direction_type.direction_type_id"),
        nullable=False,
    )
    is_overlap: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false",
    )
    is_primary_approach: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false",
    )

    __table_args__ = (
        Index("idx_route_phase_signal", "route_signal_id"),
        {"schema": tsigma_schema("config")},
    )


class RouteDistance(Base):
    """
    Distance and travel time between consecutive signals in a route.

    Used for progression/bandwidth analysis and travel time calculations.
    """

    __tablename__ = "route_distance"

    route_distance_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    from_route_signal_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("route_signal.route_signal_id"),
        nullable=False,
    )
    to_route_signal_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("route_signal.route_signal_id"),
        nullable=False,
    )
    distance_feet: Mapped[int] = mapped_column(Integer, nullable=False)
    travel_time_seconds: Mapped[Optional[int]] = mapped_column(SmallInteger)

    __table_args__ = (
        UniqueConstraint(
            "from_route_signal_id", "to_route_signal_id",
            name="uq_route_distance",
        ),
        Index("idx_route_distance_from", "from_route_signal_id"),
        {"schema": tsigma_schema("config")},
    )
