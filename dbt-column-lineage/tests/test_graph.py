"""Tests for the ColumnLineageGraph."""

from __future__ import annotations

import pytest

from dbt_lineage.exceptions import ColumnNotFoundError
from dbt_lineage.graph import ColumnLineageGraph
from dbt_lineage.models import (
    ColumnEdge,
    ColumnRef,
    ModelInfo,
    ResourceType,
    TransformType,
)


def make_edge(
    src_model: str,
    src_col: str,
    tgt_model: str,
    tgt_col: str,
    transform_type: TransformType = TransformType.PASSTHROUGH,
    transform_sql: str = "",
) -> ColumnEdge:
    return ColumnEdge(
        source=ColumnRef(model=src_model, column=src_col),
        target=ColumnRef(model=tgt_model, column=tgt_col),
        transform_sql=transform_sql or src_col,
        transform_type=transform_type,
    )


@pytest.fixture
def simple_graph() -> ColumnLineageGraph:
    """
    raw_orders.id → stg_orders.order_id (rename)
    raw_orders.status → stg_orders.order_status (rename)
    stg_orders.order_id → orders.order_id (passthrough)
    stg_orders.order_status → orders.is_completed (conditional)
    """
    g = ColumnLineageGraph()
    g.add_edge(make_edge("raw_orders", "id", "stg_orders", "order_id", TransformType.RENAME, "id AS order_id"))
    g.add_edge(make_edge("raw_orders", "status", "stg_orders", "order_status", TransformType.RENAME, "status AS order_status"))
    g.add_edge(make_edge("stg_orders", "order_id", "orders", "order_id", TransformType.PASSTHROUGH, "order_id"))
    g.add_edge(make_edge("stg_orders", "order_status", "orders", "is_completed", TransformType.CONDITIONAL, "CASE WHEN order_status = 'completed' THEN 1 ELSE 0 END"))
    return g


class TestAddModel:
    def test_add_model_stores_info(self):
        g = ColumnLineageGraph()
        m = ModelInfo(unique_id="model.x.foo", name="foo", resource_type=ResourceType.MODEL)
        g.add_model(m)
        assert "foo" in g._models


class TestAddEdge:
    def test_nodes_created(self, simple_graph):
        assert ColumnRef(model="raw_orders", column="id") in simple_graph._graph
        assert ColumnRef(model="stg_orders", column="order_id") in simple_graph._graph

    def test_edge_exists(self, simple_graph):
        src = ColumnRef(model="raw_orders", column="id")
        tgt = ColumnRef(model="stg_orders", column="order_id")
        assert simple_graph._graph.has_edge(src, tgt)

    def test_edge_data_stored(self, simple_graph):
        src = ColumnRef(model="raw_orders", column="id")
        tgt = ColumnRef(model="stg_orders", column="order_id")
        data = simple_graph._graph[src][tgt]
        assert data["transform_type"] == "rename"
        assert data["transform_sql"] == "id AS order_id"


class TestTraceColumn:
    def test_trace_finds_root_source(self, simple_graph):
        result = simple_graph.trace_column("orders", "order_id")
        source_cols = result.source_columns
        assert ColumnRef(model="raw_orders", column="id") in source_cols

    def test_trace_returns_edges(self, simple_graph):
        result = simple_graph.trace_column("orders", "order_id")
        assert len(result.edges) >= 2

    def test_trace_target_is_set(self, simple_graph):
        result = simple_graph.trace_column("orders", "order_id")
        assert result.target == ColumnRef(model="orders", column="order_id")

    def test_trace_source_models(self, simple_graph):
        result = simple_graph.trace_column("orders", "order_id")
        assert "raw_orders" in result.source_models

    def test_trace_not_found_raises(self, simple_graph):
        with pytest.raises(ColumnNotFoundError):
            simple_graph.trace_column("nonexistent", "col")


class TestImpactColumn:
    def test_impact_finds_downstream(self, simple_graph):
        result = simple_graph.impact_column("raw_orders", "id")
        affected_models = result.affected_models
        assert "stg_orders" in affected_models
        assert "orders" in affected_models

    def test_impact_returns_edges(self, simple_graph):
        result = simple_graph.impact_column("raw_orders", "id")
        assert len(result.edges) >= 2

    def test_impact_source_is_set(self, simple_graph):
        result = simple_graph.impact_column("raw_orders", "id")
        assert result.source == ColumnRef(model="raw_orders", column="id")

    def test_impact_not_found_raises(self, simple_graph):
        with pytest.raises(ColumnNotFoundError):
            simple_graph.impact_column("nonexistent", "col")


class TestEdgesBetween:
    def test_edges_between_models(self, simple_graph):
        edges = simple_graph.edges_between("raw_orders", "stg_orders")
        assert len(edges) == 2

    def test_no_edges_returns_empty(self, simple_graph):
        edges = simple_graph.edges_between("raw_orders", "orders")
        assert edges == []


class TestModelDependencies:
    def test_model_deps(self, simple_graph):
        deps = simple_graph.model_dependencies("stg_orders")
        assert "raw_orders" in deps

    def test_multi_hop_deps(self, simple_graph):
        deps = simple_graph.model_dependencies("orders")
        assert "stg_orders" in deps


class TestAllColumns:
    def test_all_columns(self, simple_graph):
        cols = simple_graph.all_columns("stg_orders")
        assert "order_id" in cols
        assert "order_status" in cols

    def test_sorted(self, simple_graph):
        cols = simple_graph.all_columns("stg_orders")
        assert cols == sorted(cols)


class TestAllModels:
    def test_all_models(self, simple_graph):
        models = simple_graph.all_models()
        assert "raw_orders" in models
        assert "stg_orders" in models
        assert "orders" in models

    def test_sorted(self, simple_graph):
        models = simple_graph.all_models()
        assert models == sorted(models)


class TestSearchColumns:
    def test_search_by_substring(self, simple_graph):
        results = simple_graph.search_columns("order")
        col_names = [r.column for r in results]
        assert "order_id" in col_names

    def test_case_insensitive(self, simple_graph):
        results = simple_graph.search_columns("ORDER_ID")
        assert len(results) > 0


class TestGetTransformsByType:
    def test_get_renames(self, simple_graph):
        renames = simple_graph.get_transforms_by_type(TransformType.RENAME)
        assert len(renames) == 2

    def test_get_passthroughs(self, simple_graph):
        passthroughs = simple_graph.get_transforms_by_type(TransformType.PASSTHROUGH)
        assert len(passthroughs) == 1

    def test_empty_for_missing_type(self, simple_graph):
        windows = simple_graph.get_transforms_by_type(TransformType.WINDOW)
        assert windows == []


class TestToDict:
    def test_to_dict_structure(self, simple_graph):
        d = simple_graph.to_dict()
        assert "models" in d
        assert "nodes" in d
        assert "edges" in d
        assert "stats" in d

    def test_stats_correct(self, simple_graph):
        d = simple_graph.to_dict()
        assert d["stats"]["total_edges"] == 4
        assert d["stats"]["total_models"] == 3

    def test_json_serializable(self, simple_graph):
        import json
        d = simple_graph.to_dict()
        # Should not raise
        json.dumps(d)


class TestToNetworkx:
    def test_returns_digraph(self, simple_graph):
        import networkx as nx
        g = simple_graph.to_networkx()
        assert isinstance(g, nx.DiGraph)
