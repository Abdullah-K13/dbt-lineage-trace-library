"""Tests for data models."""

from __future__ import annotations

import pytest

from dbt_lineage.models import (
    ColumnEdge,
    ColumnRef,
    ImpactResult,
    ManifestNode,
    ModelInfo,
    ResourceType,
    TraceResult,
    TransformType,
)


class TestColumnRef:
    def test_str_representation(self):
        ref = ColumnRef(model="orders", column="order_id")
        assert str(ref) == "orders.order_id"

    def test_frozen_is_hashable(self):
        ref = ColumnRef(model="orders", column="order_id")
        s = {ref}  # Should not raise
        assert ref in s

    def test_equality(self):
        a = ColumnRef(model="orders", column="order_id")
        b = ColumnRef(model="orders", column="order_id")
        assert a == b

    def test_usable_as_dict_key(self):
        ref = ColumnRef(model="orders", column="order_id")
        d = {ref: "value"}
        assert d[ref] == "value"


class TestTransformType:
    def test_str_values(self):
        assert str(TransformType.PASSTHROUGH) == "passthrough"
        assert str(TransformType.AGGREGATION) == "aggregation"
        assert str(TransformType.WINDOW) == "window"

    def test_all_values_exist(self):
        expected = {
            "passthrough", "rename", "cast", "arithmetic", "aggregation",
            "conditional", "function", "window", "join_derived", "complex", "unknown",
        }
        actual = {t.value for t in TransformType}
        assert actual == expected


class TestColumnEdge:
    def test_to_dict(self):
        edge = ColumnEdge(
            source=ColumnRef(model="raw_orders", column="id"),
            target=ColumnRef(model="stg_orders", column="order_id"),
            transform_sql="id AS order_id",
            transform_type=TransformType.RENAME,
            model_unique_id="model.jaffle_shop.stg_orders",
        )
        d = edge.to_dict()
        assert d["source_model"] == "raw_orders"
        assert d["source_column"] == "id"
        assert d["target_model"] == "stg_orders"
        assert d["target_column"] == "order_id"
        assert d["transform_sql"] == "id AS order_id"
        assert d["transform_type"] == "rename"
        assert d["model_unique_id"] == "model.jaffle_shop.stg_orders"


class TestManifestNode:
    def test_schema_field_remapping(self):
        """'schema' in JSON should map to schema_ in the model."""
        node = ManifestNode.model_validate({
            "unique_id": "model.test.foo",
            "name": "foo",
            "resource_type": "model",
            "schema": "my_schema",
        })
        assert node.schema_ == "my_schema"

    def test_get_compiled_sql_prefers_compiled_code(self):
        node = ManifestNode(
            compiled_code="SELECT 1",
            compiled_sql="SELECT 2",
        )
        assert node.get_compiled_sql() == "SELECT 1"

    def test_get_compiled_sql_falls_back_to_compiled_sql(self):
        node = ManifestNode(compiled_sql="SELECT 2")
        assert node.get_compiled_sql() == "SELECT 2"

    def test_get_compiled_sql_falls_back_to_raw(self):
        node = ManifestNode(raw_code="SELECT 3")
        assert node.get_compiled_sql() == "SELECT 3"

    def test_get_compiled_sql_returns_empty_when_none(self):
        node = ManifestNode()
        assert node.get_compiled_sql() == ""

    def test_get_table_alias_prefers_alias(self):
        node = ManifestNode(name="orders", alias="orders_aliased")
        assert node.get_table_alias() == "orders_aliased"

    def test_get_table_alias_falls_back_to_name(self):
        node = ManifestNode(name="orders")
        assert node.get_table_alias() == "orders"

    def test_extra_fields_allowed(self):
        """Unknown fields must not raise (manifest.json varies across dbt versions)."""
        node = ManifestNode.model_validate({
            "unique_id": "model.test.foo",
            "name": "foo",
            "unknown_future_field": "some_value",
            "another_unknown": {"nested": True},
        })
        assert node.name == "foo"
