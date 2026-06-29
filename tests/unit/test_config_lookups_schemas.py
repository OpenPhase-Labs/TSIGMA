from uuid import uuid4

import pytest
from pydantic import ValidationError

from tsigma.api.v1.schemas import (
    AreaCreate,
    AreaResponse,
    AreaSignalCreate,
    AreaUpdate,
    JurisdictionCreate,
    JurisdictionResponse,
    JurisdictionUpdate,
    LookupItem,
    MetricTypeItem,
    RegionLookupItem,
)


# (a) LookupItem and RegionLookupItem
def test_lookup_item_constructs_with_valid_data():
    item = LookupItem(id="1", label="Test")
    assert item.id == "1"
    assert item.label == "Test"


def test_region_lookup_item_defaults_parent_id_to_none():
    item = RegionLookupItem(id="2", label="Region")
    assert item.parent_id is None


def test_region_lookup_item_accepts_parent_id():
    item = RegionLookupItem(id="3", label="Region", parent_id="4")
    assert item.parent_id == "4"


# (b) MetricTypeItem
def test_metric_type_item_constructs_from_kwargs():
    item = MetricTypeItem(key="temp", label="Temperature", display_order=1)
    assert item.key == "temp"
    assert item.label == "Temperature"
    assert item.display_order == 1


def test_metric_type_item_builds_from_object_via_model_validate():
    class MockMetricType:
        key = "temp"
        label = "Temperature"
        display_order = 1

    item = MetricTypeItem.model_validate(MockMetricType())
    assert item.key == "temp"
    assert item.label == "Temperature"
    assert item.display_order == 1


# (c) AreaCreate
def test_area_create_requires_non_empty_name():
    with pytest.raises(ValidationError):
        AreaCreate(name="")


def test_area_create_accepts_optional_description():
    area = AreaCreate(name="Area", description="Desc")
    assert area.name == "Area"
    assert area.description == "Desc"


# (d) AreaUpdate
def test_area_update_no_fields_raises_validation_error():
    with pytest.raises(ValidationError) as exc_info:
        AreaUpdate()
    assert "At least one field" in str(exc_info.value)


def test_area_update_with_one_field_is_valid():
    area = AreaUpdate(name="New Name")
    assert area.name == "New Name"


# (e) AreaResponse
def test_area_response_round_trips_uuid_name_description():
    area_id = uuid4()
    area = AreaResponse(area_id=area_id, name="Area", description="Desc")
    assert area.area_id == area_id
    assert area.name == "Area"
    assert area.description == "Desc"


# (f) AreaSignalCreate
def test_area_signal_create_requires_non_empty_signal_id():
    with pytest.raises(ValidationError):
        AreaSignalCreate(signal_id="")


# (g) JurisdictionCreate/Update/Response other_partners
def test_jurisdiction_create_other_partners_defaults_to_none():
    jc = JurisdictionCreate(name="Test")
    assert jc.other_partners is None


def test_jurisdiction_create_other_partners_accepts_value():
    jc = JurisdictionCreate(name="Test", other_partners="Partners")
    assert jc.other_partners == "Partners"


def test_jurisdiction_update_other_partners_accepts_value():
    ju = JurisdictionUpdate(other_partners="Partners")
    assert ju.other_partners == "Partners"


def test_jurisdiction_response_other_partners_defaults_to_none():
    jr = JurisdictionResponse(jurisdiction_id=uuid4(), name="Test")
    assert jr.other_partners is None


def test_jurisdiction_response_other_partners_accepts_value():
    jr = JurisdictionResponse(jurisdiction_id=uuid4(), name="Test", other_partners="Partners")
    assert jr.other_partners == "Partners"
