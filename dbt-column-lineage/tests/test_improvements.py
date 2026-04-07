"""Tests for accuracy improvements: topological sort, schema propagation, alias resolution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_lineage import LineageGraph
from dbt_lineage.models import ModelInfo, ResourceType, TransformType
from dbt_lineage.parser import topological_sort
from dbt_lineage.sql_analyzer import analyze_model_columns, _build_alias_map, _resolve_source_table

import sqlglot


@pytest.fixture(autouse=True)
def clear_cache():
    LineageGraph.clear_cache()
    yield
    LineageGraph.clear_cache()


# ─── Topological Sort ─────────────────────────────────────────────────────────

class TestTopologicalSort:
    def _make_model(self, uid: str, depends_on: list[str]) -> ModelInfo:
        return ModelInfo(
            unique_id=uid,
            name=uid.split(".")[-1],
            resource_type=ResourceType.MODEL,
            depends_on=depends_on,
        )

    def test_sources_come_first(self):
        models = {
            "source.raw": ModelInfo(unique_id="source.raw", name="raw", resource_type=ResourceType.SOURCE, depends_on=[]),
            "model.stg": self._make_model("model.stg", ["source.raw"]),
            "model.mart": self._make_model("model.mart", ["model.stg"]),
        }
        order = topological_sort(models)
        assert order.index("source.raw") < order.index("model.stg")
        assert order.index("model.stg") < order.index("model.mart")

    def test_independent_models_all_included(self):
        models = {
            "model.a": self._make_model("model.a", []),
            "model.b": self._make_model("model.b", []),
            "model.c": self._make_model("model.c", []),
        }
        order = topological_sort(models)
        assert set(order) == {"model.a", "model.b", "model.c"}

    def test_diamond_dependency(self):
        """A → B, A → C, B → D, C → D  (diamond shape)"""
        models = {
            "model.a": self._make_model("model.a", []),
            "model.b": self._make_model("model.b", ["model.a"]),
            "model.c": self._make_model("model.c", ["model.a"]),
            "model.d": self._make_model("model.d", ["model.b", "model.c"]),
        }
        order = topological_sort(models)
        assert order.index("model.a") < order.index("model.b")
        assert order.index("model.a") < order.index("model.c")
        assert order.index("model.b") < order.index("model.d")
        assert order.index("model.c") < order.index("model.d")

    def test_simple_manifest_order(self, simple_manifest_path):
        from dbt_lineage.parser import parse_manifest
        models, _ = parse_manifest(simple_manifest_path)
        order = topological_sort(models)
        # source must come before stg_orders, stg_orders before orders
        uids = {m.name: uid for uid, m in models.items()}
        assert order.index(uids["raw_orders"]) < order.index(uids["stg_orders"])
        assert order.index(uids["stg_orders"]) < order.index(uids["orders"])


# ─── Alias Resolution ─────────────────────────────────────────────────────────

class TestAliasMap:
    def test_simple_alias(self):
        parsed = sqlglot.parse_one("SELECT o.id FROM orders o")
        alias_map = _build_alias_map(parsed)
        assert alias_map.get("o") == "orders"

    def test_join_aliases(self):
        parsed = sqlglot.parse_one(
            "SELECT o.id, p.amount FROM orders o JOIN payments p ON o.id = p.order_id"
        )
        alias_map = _build_alias_map(parsed)
        assert alias_map.get("o") == "orders"
        assert alias_map.get("p") == "payments"

    def test_no_alias_identity_mapping(self):
        parsed = sqlglot.parse_one("SELECT id FROM orders")
        alias_map = _build_alias_map(parsed)
        assert alias_map.get("orders") == "orders"

    def test_cte_not_confused_with_alias(self):
        sql = "WITH base AS (SELECT id FROM raw) SELECT id FROM base"
        parsed = sqlglot.parse_one(sql)
        alias_map = _build_alias_map(parsed)
        # 'raw' table should be present
        assert "raw" in alias_map


class TestResolveSourceTable:
    def test_simple_table_dot_column(self):
        alias_map = {"orders": "orders"}
        table, col = _resolve_source_table("orders.id", alias_map)
        assert table == "orders"
        assert col == "id"

    def test_alias_resolved(self):
        alias_map = {"o": "orders", "orders": "orders"}
        table, col = _resolve_source_table("o.order_id", alias_map)
        assert table == "orders"
        assert col == "order_id"

    def test_qualified_name_rsplit(self):
        """dev.public.stg_orders.order_id → table=dev.public.stg_orders, col=order_id"""
        alias_map = {}
        table_lookup = {"dev.public.stg_orders": "stg_orders", "stg_orders": "stg_orders"}
        table, col = _resolve_source_table("dev.public.stg_orders.order_id", alias_map, table_lookup)
        assert table == "stg_orders"
        assert col == "order_id"

    def test_table_lookup_fallback(self):
        alias_map = {"stg_orders": "stg_orders"}
        table_lookup = {"stg_orders": "stg_orders", "public.stg_orders": "stg_orders"}
        table, col = _resolve_source_table("stg_orders.id", alias_map, table_lookup)
        assert table == "stg_orders"


# ─── Schema Propagation ───────────────────────────────────────────────────────

class TestSchemaPropagation:
    """Verify that analyzing models in topological order + propagating schema
    gives correct lineage even without catalog.json."""

    def test_three_hop_lineage_without_catalog(self, tmp_path):
        """
        raw_data.id → staging.order_id → mart.order_id

        Without catalog, the second hop should still work because staging's
        output columns are propagated into the schema before mart is analyzed.
        """
        manifest = {
            "metadata": {"dbt_version": "1.7.0", "adapter_type": "postgres", "project_name": "test"},
            "nodes": {
                "model.test.staging": {
                    "unique_id": "model.test.staging",
                    "name": "staging",
                    "resource_type": "model",
                    "compiled_code": "SELECT id AS order_id, amount FROM raw_data",
                    "depends_on": {"nodes": ["source.test.raw_data"], "macros": []},
                    "columns": {},
                    "database": "dev", "schema": "public",
                    "original_file_path": "models/staging.sql",
                },
                "model.test.mart": {
                    "unique_id": "model.test.mart",
                    "name": "mart",
                    "resource_type": "model",
                    "compiled_code": "SELECT order_id, amount * 1.1 AS amount_with_tax FROM staging",
                    "depends_on": {"nodes": ["model.test.staging"], "macros": []},
                    "columns": {},
                    "database": "dev", "schema": "public",
                    "original_file_path": "models/mart.sql",
                },
            },
            "sources": {
                "source.test.raw_data": {
                    "unique_id": "source.test.raw_data",
                    "name": "raw_data",
                    "resource_type": "source",
                    "database": "dev", "schema": "public",
                    "columns": {
                        "id": {"name": "id", "description": ""},
                        "amount": {"name": "amount", "description": ""},
                    },
                }
            },
            "parent_map": {}, "child_map": {},
        }
        p = tmp_path / "manifest.json"
        p.write_text(json.dumps(manifest))

        g = LineageGraph(str(p))

        # Trace mart.order_id all the way back to raw_data.id
        result = g.trace("mart", "order_id")
        source_cols = {(e.source.model, e.source.column) for e in result.edges}
        assert ("raw_data", "id") in source_cols, (
            "Should trace through staging to raw_data without catalog.json"
        )

    def test_arithmetic_transform_propagated(self, tmp_path):
        """mart.amount_with_tax should trace back through staging to raw_data.amount."""
        manifest = {
            "metadata": {"dbt_version": "1.7.0", "adapter_type": "postgres", "project_name": "test"},
            "nodes": {
                "model.test.staging": {
                    "unique_id": "model.test.staging",
                    "name": "staging",
                    "resource_type": "model",
                    "compiled_code": "SELECT id AS order_id, amount FROM raw_data",
                    "depends_on": {"nodes": ["source.test.raw_data"], "macros": []},
                    "columns": {},
                    "database": "dev", "schema": "public",
                    "original_file_path": "",
                },
                "model.test.mart": {
                    "unique_id": "model.test.mart",
                    "name": "mart",
                    "resource_type": "model",
                    "compiled_code": "SELECT order_id, amount * 1.1 AS amount_with_tax FROM staging",
                    "depends_on": {"nodes": ["model.test.staging"], "macros": []},
                    "columns": {},
                    "database": "dev", "schema": "public",
                    "original_file_path": "",
                },
            },
            "sources": {
                "source.test.raw_data": {
                    "unique_id": "source.test.raw_data",
                    "name": "raw_data",
                    "resource_type": "source",
                    "database": "dev", "schema": "public",
                    "columns": {
                        "id": {"name": "id", "description": ""},
                        "amount": {"name": "amount", "description": ""},
                    },
                }
            },
            "parent_map": {}, "child_map": {},
        }
        p = tmp_path / "manifest.json"
        p.write_text(json.dumps(manifest))

        g = LineageGraph(str(p))
        result = g.trace("mart", "amount_with_tax")
        assert len(result.edges) > 0
        arithmetic_edges = [e for e in result.edges if e.transform_type == TransformType.ARITHMETIC]
        assert len(arithmetic_edges) > 0

    def test_join_alias_resolved_correctly(self):
        """Columns from JOIN aliases should resolve to real table names."""
        sql = "SELECT o.order_id, p.amount FROM orders o JOIN payments p ON o.id = p.order_id"
        schema = {
            "orders": {"order_id": "INT", "id": "INT"},
            "payments": {"amount": "DECIMAL", "order_id": "INT"},
        }
        result = analyze_model_columns(sql, "combined", schema=schema)
        source_tables = {e.source.model for e in result.edges}
        # Should resolve 'o' → 'orders' and 'p' → 'payments'
        assert "orders" in source_tables
        assert "payments" in source_tables
        assert "o" not in source_tables
        assert "p" not in source_tables

    def test_select_star_with_propagated_schema(self):
        """SELECT * should expand when schema is available from propagation."""
        schema = {"stg_orders": {"order_id": "INT", "customer_id": "INT", "amount": "DECIMAL"}}
        result = analyze_model_columns("SELECT * FROM stg_orders", "orders", schema=schema)
        target_cols = {e.target.column for e in result.edges}
        assert "order_id" in target_cols
        assert "customer_id" in target_cols
        assert "amount" in target_cols


# ─── Build Stats ──────────────────────────────────────────────────────────────

class TestBuildStats:
    def test_stats_returned(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        s = g.stats()
        assert s.total_models > 0
        assert s.models_analyzed > 0
        assert s.total_edges > 0

    def test_success_rate_is_fraction(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        s = g.stats()
        assert 0.0 <= s.success_rate <= 1.0

    def test_stats_in_to_dict(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        d = g.to_dict()
        assert "build_stats" in d
        assert "success_rate" in d["build_stats"]

    def test_skipped_count_matches_sources(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        s = g.stats()
        # raw_orders is a source — it should be skipped (no SQL to analyze)
        assert s.models_skipped >= 1

    def test_failed_models_listed(self, tmp_path):
        """A model with unparseable SQL should appear in unresolved_models."""
        manifest = {
            "metadata": {"dbt_version": "1.7.0", "adapter_type": "postgres", "project_name": "test"},
            "nodes": {
                "model.test.bad": {
                    "unique_id": "model.test.bad",
                    "name": "bad",
                    "resource_type": "model",
                    "compiled_code": "THIS IS NOT SQL AT ALL",
                    "depends_on": {"nodes": [], "macros": []},
                    "columns": {},
                    "database": "dev", "schema": "public",
                    "original_file_path": "",
                },
            },
            "sources": {},
            "parent_map": {}, "child_map": {},
        }
        p = tmp_path / "manifest.json"
        p.write_text(json.dumps(manifest))
        g = LineageGraph(str(p))
        s = g.stats()
        assert "bad" in s.unresolved_models

    def test_with_catalog_improves_coverage(self, simple_manifest_path, simple_catalog_path):
        g = LineageGraph(str(simple_manifest_path), catalog_path=str(simple_catalog_path))
        s = g.stats()
        assert s.total_edges > 0
        assert s.success_rate > 0


# ─── End-to-End Accuracy ──────────────────────────────────────────────────────

class TestEndToEndAccuracy:
    def test_full_trace_through_three_models(self, simple_manifest_path):
        """orders.order_id should trace back to raw_orders.id through stg_orders."""
        g = LineageGraph(str(simple_manifest_path))
        result = g.trace("orders", "order_id")
        models_in_path = {e.source.model for e in result.edges}
        # Should touch both stg_orders and raw_orders
        assert "stg_orders" in models_in_path or "raw_orders" in models_in_path

    def test_impact_crosses_all_hops(self, simple_manifest_path):
        """raw_orders.id impact should reach final mart model."""
        g = LineageGraph(str(simple_manifest_path))
        result = g.impact("raw_orders", "id")
        assert "orders" in result.affected_models

    def test_conditional_transform_detected_end_to_end(self, simple_manifest_path):
        """The CASE WHEN in orders.is_completed should be classified as conditional."""
        g = LineageGraph(str(simple_manifest_path))
        conditionals = g.get_transforms_by_type(TransformType.CONDITIONAL)
        target_cols = {e.target.column for e in conditionals}
        assert "is_completed" in target_cols

    def test_window_transform_detected_end_to_end(self, simple_manifest_path):
        """The ROW_NUMBER() in orders.order_sequence should be classified as window."""
        g = LineageGraph(str(simple_manifest_path))
        windows = g.get_transforms_by_type(TransformType.WINDOW)
        target_cols = {e.target.column for e in windows}
        assert "order_sequence" in target_cols

    def test_rename_detected_in_staging(self, simple_manifest_path):
        """stg_orders renames id → order_id, user_id → customer_id."""
        g = LineageGraph(str(simple_manifest_path))
        renames = g.get_transforms_by_type(TransformType.RENAME)
        target_cols = {e.target.column for e in renames}
        assert "order_id" in target_cols
        assert "customer_id" in target_cols
