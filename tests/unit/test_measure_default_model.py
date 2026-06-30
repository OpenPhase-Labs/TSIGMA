import glob
import importlib.util
import os

import tsigma.models
from tsigma.models.base import Base
from tsigma.models.reference import MeasureDefault


class TestMeasureDefaultModelInstantiation:
    def test_measure_default_can_be_instantiated_with_report_param_and_value(self):
        md = MeasureDefault(
            report_name="split-failure",
            param_name="green_occ_threshold",
            value={"v": 0.79},
        )
        assert md.report_name == "split-failure"
        assert md.param_name == "green_occ_threshold"
        assert md.value == {"v": 0.79}

    def test_measure_default_defaults_are_none(self):
        md = MeasureDefault()
        assert md.report_name is None
        assert md.param_name is None
        assert md.value is None


class TestModelImports:
    def test_measure_default_importable_from_tsigma_models_package(self):
        from tsigma.models import MeasureDefault as MD
        assert MD is not None

    def test_measure_default_listed_in_tsigma_models_all(self):
        assert hasattr(tsigma.models, "__all__"), "tsigma.models must define __all__"
        assert "MeasureDefault" in tsigma.models.__all__


class TestMetadataIntrospection:
    def test_config_measure_default_table_registered_with_correct_columns_and_pk(self):
        assert "config.measure_default" in Base.metadata.tables
        table = Base.metadata.tables["config.measure_default"]
        assert "report_name" in table.columns
        assert "param_name" in table.columns
        assert "value" in table.columns
        assert set(table.primary_key.columns.keys()) == {"report_name", "param_name"}


class TestAlembicMigration:
    def test_migration_file_exists_with_correct_prefix(self):
        versions_dir = os.path.join(os.path.dirname(__file__), "..", "..", "alembic", "versions")
        files = glob.glob(os.path.join(versions_dir, "000000000010_*.py"))
        assert len(files) == 1, f"Expected exactly one migration file starting with 000000000010_, found {files}"

    def test_migration_exposes_correct_revision_and_down_revision(self):
        versions_dir = os.path.join(os.path.dirname(__file__), "..", "..", "alembic", "versions")
        files = glob.glob(os.path.join(versions_dir, "000000000010_*.py"))
        assert len(files) == 1
        migration_file = files[0]
        spec = importlib.util.spec_from_file_location("migration_module_000000000010", migration_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.revision == "000000000010"
        assert module.down_revision == "000000000009"
