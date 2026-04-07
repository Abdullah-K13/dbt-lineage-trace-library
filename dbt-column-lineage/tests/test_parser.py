"""Tests for manifest/catalog parsing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_lineage.exceptions import ManifestParseError, CatalogParseError
from dbt_lineage.models import ResourceType
from dbt_lineage.parser import (
    build_schema_dict,
    build_table_lookup,
    parse_catalog,
    parse_manifest,
)


class TestParseManifest:
    def test_parses_models(self, simple_manifest_path):
        models, metadata = parse_manifest(simple_manifest_path)
        assert "model.jaffle_shop.stg_orders" in models
        assert "model.jaffle_shop.orders" in models

    def test_parses_sources(self, simple_manifest_path):
        models, metadata = parse_manifest(simple_manifest_path)
        assert "source.jaffle_shop.raw_orders" in models

    def test_metadata_adapter_type(self, simple_manifest_path):
        _, metadata = parse_manifest(simple_manifest_path)
        assert metadata.adapter_type == "postgres"

    def test_metadata_project_name(self, simple_manifest_path):
        _, metadata = parse_manifest(simple_manifest_path)
        assert metadata.project_name == "jaffle_shop"

    def test_model_resource_type(self, simple_manifest_path):
        models, _ = parse_manifest(simple_manifest_path)
        stg = models["model.jaffle_shop.stg_orders"]
        assert stg.resource_type == ResourceType.MODEL

    def test_source_resource_type(self, simple_manifest_path):
        models, _ = parse_manifest(simple_manifest_path)
        src = models["source.jaffle_shop.raw_orders"]
        assert src.resource_type == ResourceType.SOURCE

    def test_compiled_sql_extracted(self, simple_manifest_path):
        models, _ = parse_manifest(simple_manifest_path)
        stg = models["model.jaffle_shop.stg_orders"]
        assert "SELECT" in stg.compiled_sql.upper()

    def test_depends_on_extracted(self, simple_manifest_path):
        models, _ = parse_manifest(simple_manifest_path)
        stg = models["model.jaffle_shop.stg_orders"]
        assert "source.jaffle_shop.raw_orders" in stg.depends_on

    def test_source_has_no_compiled_sql(self, simple_manifest_path):
        models, _ = parse_manifest(simple_manifest_path)
        src = models["source.jaffle_shop.raw_orders"]
        assert src.compiled_sql == ""

    def test_skips_unsupported_resource_types(self, tmp_path):
        manifest = {
            "metadata": {"dbt_version": "1.7.0", "adapter_type": "postgres", "project_name": "test"},
            "nodes": {
                "test.test.some_test": {
                    "unique_id": "test.test.some_test",
                    "name": "some_test",
                    "resource_type": "test",
                    "depends_on": {"nodes": [], "macros": []},
                    "columns": {},
                }
            },
            "sources": {},
        }
        p = tmp_path / "manifest.json"
        p.write_text(json.dumps(manifest), encoding="utf-8")
        models, _ = parse_manifest(p)
        assert len(models) == 0

    def test_raises_on_invalid_json(self, tmp_path):
        p = tmp_path / "manifest.json"
        p.write_text("not valid json", encoding="utf-8")
        with pytest.raises(ManifestParseError):
            parse_manifest(p)


class TestParseCatalog:
    def test_parses_catalog(self, tmp_path, simple_catalog_path):
        data = parse_catalog(simple_catalog_path)
        assert "nodes" in data
        assert "model.jaffle_shop.stg_orders" in data["nodes"]

    def test_raises_on_invalid_json(self, tmp_path):
        p = tmp_path / "catalog.json"
        p.write_text("not valid json", encoding="utf-8")
        with pytest.raises(CatalogParseError):
            parse_catalog(p)


class TestBuildSchemaDict:
    def test_builds_schema_from_catalog(self, simple_manifest_path, simple_catalog_path):
        models, _ = parse_manifest(simple_manifest_path)
        catalog_data = parse_catalog(simple_catalog_path)
        schema = build_schema_dict(catalog_data, models)

        assert "stg_orders" in schema
        assert "order_id" in schema["stg_orders"]

    def test_column_types_extracted(self, simple_manifest_path, simple_catalog_path):
        models, _ = parse_manifest(simple_manifest_path)
        catalog_data = parse_catalog(simple_catalog_path)
        schema = build_schema_dict(catalog_data, models)

        assert schema["stg_orders"]["order_id"] == "INTEGER"


class TestBuildTableLookup:
    def test_short_name_lookup(self, simple_manifest_path):
        models, _ = parse_manifest(simple_manifest_path)
        lookup = build_table_lookup(models)
        assert lookup.get("stg_orders") == "stg_orders"
        assert lookup.get("raw_orders") == "raw_orders"

    def test_schema_qualified_lookup(self, simple_manifest_path):
        models, _ = parse_manifest(simple_manifest_path)
        lookup = build_table_lookup(models)
        assert lookup.get("public.stg_orders") == "stg_orders"

    def test_fully_qualified_lookup(self, simple_manifest_path):
        models, _ = parse_manifest(simple_manifest_path)
        lookup = build_table_lookup(models)
        assert lookup.get("dev.public.stg_orders") == "stg_orders"
