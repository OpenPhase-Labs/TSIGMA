"""
Reference data API endpoints.

CRUD operations for lookup tables (directions, controller types, lane types,
movement types, detection hardware, event codes).
GET endpoints respect the 'signal_detail' access policy. POST/PUT/DELETE require admin role.
"""

from typing import Optional
from uuid import UUID

from fastapi import APIRouter
from pydantic import BaseModel, Field, model_validator

from ...models import (
    ControllerType,
    DetectionHardware,
    DirectionType,
    EventCodeDefinition,
    LaneType,
    MovementType,
)
from .crud_factory import crud_router
from .schemas import UPDATE_REQUIRED_MSG

# ---------------------------------------------------------------------------
# DirectionType schemas
# ---------------------------------------------------------------------------


class DirectionTypeCreate(BaseModel):
    """Schema for creating a direction type."""

    direction_type_id: int = Field(...)
    abbreviation: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)


class DirectionTypeUpdate(BaseModel):
    """Schema for updating a direction type (partial update)."""

    abbreviation: Optional[str] = Field(None, min_length=1)
    description: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "DirectionTypeUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class DirectionTypeResponse(BaseModel):
    """Direction type data returned in API responses."""

    model_config = {"from_attributes": True}

    direction_type_id: int
    abbreviation: str
    description: str


# ---------------------------------------------------------------------------
# ControllerType schemas
# ---------------------------------------------------------------------------


class ControllerTypeCreate(BaseModel):
    """Schema for creating a controller type."""

    description: str = Field(..., min_length=1)
    snmp_port: int = Field(161)
    ftp_directory: Optional[str] = None
    active_ftp: bool = False
    username: Optional[str] = None
    password: Optional[str] = None


class ControllerTypeUpdate(BaseModel):
    """Schema for updating a controller type (partial update)."""

    description: Optional[str] = Field(None, min_length=1)
    snmp_port: Optional[int] = None
    ftp_directory: Optional[str] = None
    active_ftp: Optional[bool] = None
    username: Optional[str] = None
    password: Optional[str] = None

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "ControllerTypeUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class ControllerTypeResponse(BaseModel):
    """Controller type data returned in API responses.

    Password is never returned — credentials are only used internally
    at poll time. Username is returned for display/reference only.
    """

    model_config = {"from_attributes": True}

    controller_type_id: UUID
    description: str
    snmp_port: int
    ftp_directory: Optional[str] = None
    active_ftp: bool
    username: Optional[str] = None


# ---------------------------------------------------------------------------
# LaneType schemas
# ---------------------------------------------------------------------------


class LaneTypeCreate(BaseModel):
    """Schema for creating a lane type."""

    description: str = Field(..., min_length=1)
    abbreviation: Optional[str] = None


class LaneTypeUpdate(BaseModel):
    """Schema for updating a lane type (partial update)."""

    description: Optional[str] = Field(None, min_length=1)
    abbreviation: Optional[str] = None

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "LaneTypeUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class LaneTypeResponse(BaseModel):
    """Lane type data returned in API responses."""

    model_config = {"from_attributes": True}

    lane_type_id: UUID
    description: str
    abbreviation: Optional[str] = None


# ---------------------------------------------------------------------------
# MovementType schemas
# ---------------------------------------------------------------------------


class MovementTypeCreate(BaseModel):
    """Schema for creating a movement type."""

    description: str = Field(..., min_length=1)
    abbreviation: Optional[str] = None
    display_order: Optional[int] = None


class MovementTypeUpdate(BaseModel):
    """Schema for updating a movement type (partial update)."""

    description: Optional[str] = Field(None, min_length=1)
    abbreviation: Optional[str] = None
    display_order: Optional[int] = None

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "MovementTypeUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class MovementTypeResponse(BaseModel):
    """Movement type data returned in API responses."""

    model_config = {"from_attributes": True}

    movement_type_id: UUID
    description: str
    abbreviation: Optional[str] = None
    display_order: Optional[int] = None


# ---------------------------------------------------------------------------
# DetectionHardware schemas
# ---------------------------------------------------------------------------


class DetectionHardwareCreate(BaseModel):
    """Schema for creating a detection hardware entry."""

    name: str = Field(..., min_length=1)


class DetectionHardwareUpdate(BaseModel):
    """Schema for updating a detection hardware entry (partial update)."""

    name: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "DetectionHardwareUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class DetectionHardwareResponse(BaseModel):
    """Detection hardware data returned in API responses."""

    model_config = {"from_attributes": True}

    detection_hardware_id: UUID
    name: str


# ---------------------------------------------------------------------------
# EventCodeDefinition schemas
# ---------------------------------------------------------------------------


class EventCodeDefinitionCreate(BaseModel):
    """Schema for creating an event code definition."""

    event_code: int = Field(...)
    name: str = Field(..., min_length=1)
    description: Optional[str] = None
    category: str = Field(..., min_length=1)
    param_type: str = Field(..., min_length=1)


class EventCodeDefinitionUpdate(BaseModel):
    """Schema for updating an event code definition (partial update)."""

    name: Optional[str] = Field(None, min_length=1)
    description: Optional[str] = None
    category: Optional[str] = Field(None, min_length=1)
    param_type: Optional[str] = Field(None, min_length=1)

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "EventCodeDefinitionUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class EventCodeDefinitionResponse(BaseModel):
    """Event code definition data returned in API responses."""

    model_config = {"from_attributes": True}

    event_code: int
    name: str
    description: Optional[str] = None
    category: str
    param_type: str


# ===========================================================================
# Assemble router from factory
# ===========================================================================

router = APIRouter()

for sub in [
    crud_router(
        model=DirectionType,
        create_schema=DirectionTypeCreate,
        update_schema=DirectionTypeUpdate,
        response_schema=DirectionTypeResponse,
        pk_field="direction_type_id",
        prefix="/direction-types",
        resource_name="DirectionType",
        user_supplied_pk=True,
    ),
    crud_router(
        model=ControllerType,
        create_schema=ControllerTypeCreate,
        update_schema=ControllerTypeUpdate,
        response_schema=ControllerTypeResponse,
        pk_field="controller_type_id",
        prefix="/controller-types",
        resource_name="ControllerType",
    ),
    crud_router(
        model=LaneType,
        create_schema=LaneTypeCreate,
        update_schema=LaneTypeUpdate,
        response_schema=LaneTypeResponse,
        pk_field="lane_type_id",
        prefix="/lane-types",
        resource_name="LaneType",
    ),
    crud_router(
        model=MovementType,
        create_schema=MovementTypeCreate,
        update_schema=MovementTypeUpdate,
        response_schema=MovementTypeResponse,
        pk_field="movement_type_id",
        prefix="/movement-types",
        resource_name="MovementType",
    ),
    crud_router(
        model=DetectionHardware,
        create_schema=DetectionHardwareCreate,
        update_schema=DetectionHardwareUpdate,
        response_schema=DetectionHardwareResponse,
        pk_field="detection_hardware_id",
        prefix="/detection-hardware",
        resource_name="DetectionHardware",
    ),
    crud_router(
        model=EventCodeDefinition,
        create_schema=EventCodeDefinitionCreate,
        update_schema=EventCodeDefinitionUpdate,
        response_schema=EventCodeDefinitionResponse,
        pk_field="event_code",
        prefix="/event-codes",
        resource_name="EventCodeDefinition",
        user_supplied_pk=True,
    ),
]:
    router.include_router(sub)
