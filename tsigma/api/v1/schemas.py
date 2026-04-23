"""
API Pydantic schemas.

Request/response models for configuration CRUD endpoints.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

UPDATE_REQUIRED_MSG = "At least one field must be provided for update"


class SignalCreate(BaseModel):
    """Schema for creating a new signal."""

    signal_id: str = Field(..., min_length=1)
    primary_street: str = Field(..., min_length=1)
    secondary_street: Optional[str] = None
    latitude: Optional[Decimal] = None
    longitude: Optional[Decimal] = None
    jurisdiction_id: Optional[UUID] = None
    region_id: Optional[UUID] = None
    corridor_id: Optional[UUID] = None
    controller_type_id: Optional[UUID] = None
    ip_address: Optional[str] = None
    note: Optional[str] = None
    enabled: bool = True
    start_date: Optional[date] = None
    metadata: Optional[dict] = None


class SignalUpdate(BaseModel):
    """Schema for updating an existing signal (partial update)."""

    primary_street: Optional[str] = None
    secondary_street: Optional[str] = None
    latitude: Optional[Decimal] = None
    longitude: Optional[Decimal] = None
    jurisdiction_id: Optional[UUID] = None
    region_id: Optional[UUID] = None
    corridor_id: Optional[UUID] = None
    controller_type_id: Optional[UUID] = None
    ip_address: Optional[str] = None
    note: Optional[str] = None
    enabled: Optional[bool] = None
    start_date: Optional[date] = None
    metadata: Optional[dict] = None

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "SignalUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class SignalResponse(BaseModel):
    """Signal data returned in API responses."""

    model_config = {"from_attributes": True}

    signal_id: str
    primary_street: str
    secondary_street: Optional[str] = None
    latitude: Optional[Decimal] = None
    longitude: Optional[Decimal] = None
    jurisdiction_id: Optional[UUID] = None
    region_id: Optional[UUID] = None
    corridor_id: Optional[UUID] = None
    controller_type_id: Optional[UUID] = None
    ip_address: Optional[str] = None
    note: Optional[str] = None
    enabled: bool
    start_date: Optional[date] = None
    metadata: Optional[dict] = None
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Approach schemas
# ---------------------------------------------------------------------------


class ApproachCreate(BaseModel):
    """Schema for creating an approach under a signal."""

    direction_type_id: int = Field(..., ge=1)
    description: Optional[str] = None
    mph: Optional[int] = Field(None, ge=0, le=100)
    protected_phase_number: Optional[int] = Field(None, ge=1, le=16)
    is_protected_phase_overlap: bool = False
    permissive_phase_number: Optional[int] = Field(None, ge=1, le=16)
    is_permissive_phase_overlap: bool = False
    ped_phase_number: Optional[int] = Field(None, ge=1, le=16)


class ApproachUpdate(BaseModel):
    """Schema for updating an approach (partial update)."""

    direction_type_id: Optional[int] = Field(None, ge=1)
    description: Optional[str] = None
    mph: Optional[int] = Field(None, ge=0, le=100)
    protected_phase_number: Optional[int] = Field(None, ge=1, le=16)
    is_protected_phase_overlap: Optional[bool] = None
    permissive_phase_number: Optional[int] = Field(None, ge=1, le=16)
    is_permissive_phase_overlap: Optional[bool] = None
    ped_phase_number: Optional[int] = Field(None, ge=1, le=16)

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "ApproachUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class ApproachResponse(BaseModel):
    """Approach data returned in API responses."""

    model_config = {"from_attributes": True}

    approach_id: UUID
    signal_id: str
    direction_type_id: int
    description: Optional[str] = None
    mph: Optional[int] = None
    protected_phase_number: Optional[int] = None
    is_protected_phase_overlap: bool
    permissive_phase_number: Optional[int] = None
    is_permissive_phase_overlap: bool
    ped_phase_number: Optional[int] = None
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Detector schemas
# ---------------------------------------------------------------------------


class DetectorCreate(BaseModel):
    """Schema for creating a detector under an approach."""

    detector_channel: int = Field(..., ge=1)
    distance_from_stop_bar: Optional[int] = Field(None, ge=0)
    min_speed_filter: Optional[int] = Field(None, ge=0)
    decision_point: Optional[int] = Field(None, ge=0)
    movement_delay: Optional[int] = Field(None, ge=0)
    lane_number: Optional[int] = Field(None, ge=1)
    lane_type_id: Optional[UUID] = None
    movement_type_id: Optional[UUID] = None
    detection_hardware_id: Optional[UUID] = None
    lat_lon_distance: Optional[int] = Field(None, ge=0)


class DetectorUpdate(BaseModel):
    """Schema for updating a detector (partial update)."""

    detector_channel: Optional[int] = Field(None, ge=1)
    distance_from_stop_bar: Optional[int] = Field(None, ge=0)
    min_speed_filter: Optional[int] = Field(None, ge=0)
    decision_point: Optional[int] = Field(None, ge=0)
    movement_delay: Optional[int] = Field(None, ge=0)
    lane_number: Optional[int] = Field(None, ge=1)
    lane_type_id: Optional[UUID] = None
    movement_type_id: Optional[UUID] = None
    detection_hardware_id: Optional[UUID] = None
    lat_lon_distance: Optional[int] = Field(None, ge=0)

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "DetectorUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class DetectorResponse(BaseModel):
    """Detector data returned in API responses."""

    model_config = {"from_attributes": True}

    detector_id: UUID
    approach_id: UUID
    detector_channel: int
    distance_from_stop_bar: Optional[int] = None
    min_speed_filter: Optional[int] = None
    decision_point: Optional[int] = None
    movement_delay: Optional[int] = None
    lane_number: Optional[int] = None
    lane_type_id: Optional[UUID] = None
    movement_type_id: Optional[UUID] = None
    detection_hardware_id: Optional[UUID] = None
    lat_lon_distance: Optional[int] = None
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Jurisdiction schemas
# ---------------------------------------------------------------------------


class JurisdictionCreate(BaseModel):
    """Schema for creating a jurisdiction."""

    name: str = Field(..., min_length=1)
    mpo_name: Optional[str] = None
    county_name: Optional[str] = None


class JurisdictionUpdate(BaseModel):
    """Schema for updating a jurisdiction (partial update)."""

    name: Optional[str] = Field(None, min_length=1)
    mpo_name: Optional[str] = None
    county_name: Optional[str] = None

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "JurisdictionUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class JurisdictionResponse(BaseModel):
    """Jurisdiction data returned in API responses."""

    model_config = {"from_attributes": True}

    jurisdiction_id: UUID
    name: str
    mpo_name: Optional[str] = None
    county_name: Optional[str] = None
