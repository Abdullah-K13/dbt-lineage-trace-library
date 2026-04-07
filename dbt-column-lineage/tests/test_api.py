"""Integration tests for the LineageGraph API."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_lineage import LineageGraph
from dbt_lineage.exceptions import ManifestNotFoundError


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear the LineageGraph cache between tests to ensure isolation."""
    LineageGraph.clear_cache()
    yield
    LineageGraph.clear_cache()


class TestLineageGraphInit:
    def test_loads_from_manifest(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        assert "stg_orders" in g.all_models()

    def test_raises_when_manifest_missing(self, tmp_path):
        with pytest.raises(ManifestNotFoundError):
            LineageGraph(str(tmp_path / "nonexistent.json"))

    def test_auto_discovers_catalog(self, simple_manifest_path, simple_catalog_path):
        # catalog is in same dir as manifest
        g = LineageGraph(str(simple_manifest_path))
        # Just verify it loads without error
        assert g is not None

    def test_explicit_catalog_path(self, simple_manifest_path, simple_catalog_path):
        g = LineageGraph(str(simple_manifest_path), catalog_path=str(simple_catalog_path))
        assert g is not None

    def test_cache_reuse(self, simple_manifest_path):
        g1 = LineageGraph(str(simple_manifest_path))
        g2 = LineageGraph(str(simple_manifest_path))
        assert g1._graph is g2._graph  # Same object from cache


class TestFullPipeline:
    def test_models_discovered(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        models = g.all_models()
        assert "stg_orders" in models
        assert "orders" in models

    def test_trace_passthrough_column(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        result = g.trace("orders", "order_id")
        assert len(result.edges) > 0
        # order_id flows from stg_orders
        source_cols = [e.source.column for e in result.edges]
        assert "order_id" in source_cols or "id" in source_cols

    def test_impact_analysis(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        result = g.impact("stg_orders", "order_id")
        assert "orders" in result.affected_models

    def test_trace_conditional_column(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        result = g.trace("orders", "is_completed")
        assert len(result.edges) > 0

    def test_trace_window_column(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        result = g.trace("orders", "order_sequence")
        assert len(result.edges) > 0

    def test_edges_between_models(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        edges = g.edges_between("stg_orders", "orders")
        assert len(edges) > 0

    def test_model_dependencies(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        deps = g.model_dependencies("orders")
        assert "stg_orders" in deps

    def test_all_columns_for_model(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        cols = g.all_columns("orders")
        assert len(cols) > 0

    def test_search_columns(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        results = g.search_columns("order_id")
        assert len(results) > 0

    def test_to_dict_exportable(self, simple_manifest_path):
        g = LineageGraph(str(simple_manifest_path))
        d = g.to_dict()
        assert "stats" in d
        assert d["stats"]["total_models"] > 0
        # Must be JSON serializable
        json.dumps(d)

    def test_to_networkx_returns_graph(self, simple_manifest_path):
        import networkx as nx
        g = LineageGraph(str(simple_manifest_path))
        nx_g = g.to_networkx()
        assert isinstance(nx_g, nx.DiGraph)


class TestGetTransformsByType:
    def test_find_conditional_transforms(self, simple_manifest_path):
        from dbt_lineage.models import TransformType
        g = LineageGraph(str(simple_manifest_path))
        conditionals = g.get_transforms_by_type(TransformType.CONDITIONAL)
        assert len(conditionals) > 0

    def test_find_window_transforms(self, simple_manifest_path):
        from dbt_lineage.models import TransformType
        g = LineageGraph(str(simple_manifest_path))
        windows = g.get_transforms_by_type(TransformType.WINDOW)
        assert len(windows) > 0
