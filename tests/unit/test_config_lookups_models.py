import glob
import importlib.util
import os
import uuid

import tsigma.models
from tsigma.models.base import Base
from tsigma.models.reference import Area, Jurisdiction, MetricType, SignalArea


class TestAreaModelInstantiation:
    def test_area_can_be_instantiated_with_name_and_description(self):
        area = Area(name="Downtown District", description="Central business area")
        assert area.name == "Downtown District"
        assert area.description == "Central business area"

    def test_area_default_values_are_none(self):
        area = Area()
        assert area.name is None
        assert area.description is None


class TestSignalAreaModelInstantiation:
    def test_signal_area_can_be_instantiated_with_signal_id_and_area_id(self):
        area_id = uuid.uuid4()
        sa = SignalArea(signal_id="SIG_001", area_id=area_id)
        assert sa.signal_id == "SIG_001"
        assert sa.area_id == area_id

    def test_signal_area_defaults_are_none(self):
        sa = SignalArea()
        assert sa.signal_id is None
        assert sa.area_id is None


class TestMetricTypeModelInstantiation:
    def test_metric_type_can_be_instantiated_with_key_label_and_display_order(self):
        mt = MetricType(key="avg_speed", label="Average Speed", display_order=10)
        assert mt.key == "avg_speed"
        assert mt.label == "Average Speed"
        assert mt.display_order == 10

    def test_metric_type_defaults_are_none(self):
        mt = MetricType()
        assert mt.key is None
        assert mt.label is None
        assert mt.display_order is None


class TestModelImports:
    def test_area_importable_from_tsigma_models_package(self):
        from tsigma.models import Area
        assert Area is not None

    def test_signal_area_importable_from_tsigma_models_package(self):
        from tsigma.models import SignalArea
        assert SignalArea is not None

    def test_metric_type_importable_from_tsigma_models_package(self):
        from tsigma.models import MetricType
        assert MetricType is not None

    def test_models_listed_in_tsigma_models_all(self):
        assert hasattr(tsigma.models, "__all__"), "tsigma.models must define __all__"
        assert "Area" in tsigma.models.__all__
        assert "SignalArea" in tsigma.models.__all__
        assert "MetricType" in tsigma.models.__all__


class TestMetadataIntrospection:
    def test_config_area_table_registered_with_correct_columns_and_pk(self):
        assert "config.area" in Base.metadata.tables
        table = Base.metadata.tables["config.area"]
        assert "area_id" in table.columns
        assert "name" in table.columns
        assert "description" in table.columns
        assert list(table.primary_key.columns.keys()) == ["area_id"]

    def test_config_signal_area_table_registered_with_correct_columns_and_pk(self):
        assert "config.signal_area" in Base.metadata.tables
        table = Base.metadata.tables["config.signal_area"]
        assert "signal_id" in table.columns
        assert "area_id" in table.columns
        assert set(table.primary_key.columns.keys()) == {"signal_id", "area_id"}

    def test_config_metric_type_table_registered_with_correct_columns_and_pk(self):
        assert "config.metric_type" in Base.metadata.tables
        table = Base.metadata.tables["config.metric_type"]
        assert "key" in table.columns
        assert "label" in table.columns
        assert "display_order" in table.columns
        assert list(table.primary_key.columns.keys()) == ["key"]


class TestJurisdictionOtherPartnersColumn:
    def test_jurisdiction_has_other_partners_column(self):
        assert "other_partners" in Jurisdiction.__table__.columns


class TestAlembicMigration:
    def test_migration_file_exists_with_correct_prefix(self):
        versions_dir = os.path.join(os.path.dirname(__file__), "..", "..", "alembic", "versions")
        files = glob.glob(os.path.join(versions_dir, "000000000008_*.py"))
        assert len(files) == 1, f"Expected exactly one migration file starting with 000000000008_, found {files}"

    def test_migration_exposes_correct_revision_and_down_revision(self):
        versions_dir = os.path.join(os.path.dirname(__file__), "..", "..", "alembic", "versions")
        files = glob.glob(os.path.join(versions_dir, "000000000008_*.py"))
        assert len(files) == 1
        migration_file = files[0]
        spec = importlib.util.spec_from_file_location("migration_module", migration_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.revision == "000000000008"
        assert module.down_revision == "000000000007"
