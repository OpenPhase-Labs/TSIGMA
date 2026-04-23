"""
Reference data models for TSIGMA.

These are lookup tables with relatively static data (directions, controller types, etc.).
Most have seed data loaded during database initialization.
"""

from typing import Optional
from uuid import UUID

from sqlalchemy import Boolean, ForeignKey, Integer, SmallInteger, Text, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, tsigma_schema


class DirectionType(Base):
    """
    Cardinal and intercardinal directions (NB, SB, EB, WB, etc.).

    Seed data: 8 directions (NB=1, SB=2, EB=3, WB=4, NE=5, NW=6, SE=7, SW=8).
    """

    __tablename__ = "direction_type"

    direction_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    abbreviation: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = {"schema": tsigma_schema("config")}


class ControllerType(Base):
    """
    Traffic controller hardware/firmware profiles.

    Examples: Econolite ASC/3, McCain ATC, Siemens SEPAC, Intelight MaxTime.
    """

    __tablename__ = "controller_type"

    controller_type_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    snmp_port: Mapped[int] = mapped_column(Integer, nullable=False, server_default="161")
    ftp_directory: Mapped[Optional[str]] = mapped_column(Text)
    active_ftp: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    username: Mapped[Optional[str]] = mapped_column(Text)
    password: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = {"schema": tsigma_schema("config")}


class LaneType(Base):
    """
    Lane classifications (Vehicle, Bicycle, Pedestrian, HOV, Exit).

    Used to classify detector lanes.
    """

    __tablename__ = "lane_type"

    lane_type_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    abbreviation: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = {"schema": tsigma_schema("config")}


class MovementType(Base):
    """
    Traffic movements (Left Turn, Through, Right Turn, U-Turn, Pedestrian).

    Used to classify detector movements and approaches.
    """

    __tablename__ = "movement_type"

    movement_type_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    abbreviation: Mapped[Optional[str]] = mapped_column(Text)
    display_order: Mapped[Optional[int]] = mapped_column(SmallInteger)

    __table_args__ = {"schema": tsigma_schema("config")}


class DetectionHardware(Base):
    """
    Detection hardware types (Inductive Loop, Video, Radar, etc.).

    Identifies the physical detector technology.
    """

    __tablename__ = "detection_hardware"

    detection_hardware_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = {"schema": tsigma_schema("config")}


class Jurisdiction(Base):
    """
    Jurisdictional boundaries (cities, counties, MPOs).

    Organizational grouping for signals.
    """

    __tablename__ = "jurisdiction"

    jurisdiction_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    mpo_name: Mapped[Optional[str]] = mapped_column(Text)
    county_name: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = {"schema": tsigma_schema("config")}


class Region(Base):
    """
    Regional groupings with hierarchical support.

    Supports unlimited nesting via parent_region_id (e.g., State → District → Zone).
    Regions can be called districts, areas, zones, etc. based on agency preference.
    """

    __tablename__ = "region"

    region_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    parent_region_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("region.region_id", ondelete="CASCADE"),
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = {"schema": tsigma_schema("config")}


class Corridor(Base):
    """
    Simple organizational grouping of signals (e.g., "Peachtree Street Corridor").

    Different from Route - corridor is just tagging, route is ordered progression.
    """

    __tablename__ = "corridor"

    corridor_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    jurisdiction_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("jurisdiction.jurisdiction_id"),
    )

    __table_args__ = {"schema": tsigma_schema("config")}


class EventCodeDefinition(Base):
    """
    Indiana Traffic Signal Hi-Resolution Data Logger event code definitions.

    Reference: https://docs.lib.purdue.edu/jtrpdata/4/

    Defines event codes 0-255+ with categories and parameter types.
    Seed data loaded during database initialization.
    """

    __tablename__ = "event_code_definition"

    event_code: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    param_type: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = {"schema": tsigma_schema("config")}
