"""Tests for SQL analyzer — transform classification."""

from __future__ import annotations

import pytest

from dbt_lineage.models import TransformType
from dbt_lineage.sql_analyzer import analyze_model_columns, classify_transform

import sqlglot.expressions as exp


class TestClassifyTransform:
    def test_column_is_passthrough(self):
        expr = exp.Column(this=exp.Identifier(this="order_id"))
        assert classify_transform(expr) == TransformType.PASSTHROUGH

    def test_alias_of_column_is_rename(self):
        inner = exp.Column(this=exp.Identifier(this="id"))
        expr = exp.Alias(this=inner, alias=exp.Identifier(this="order_id"))
        assert classify_transform(expr) == TransformType.RENAME

    def test_cast_is_cast(self):
        expr = exp.Cast(this=exp.Column(this=exp.Identifier(this="id")), to=exp.DataType(this=exp.DataType.Type.VARCHAR))
        assert classify_transform(expr) == TransformType.CAST

    def test_add_is_arithmetic(self):
        expr = exp.Add(
            this=exp.Column(this=exp.Identifier(this="a")),
            expression=exp.Column(this=exp.Identifier(this="b")),
        )
        assert classify_transform(expr) == TransformType.ARITHMETIC

    def test_sum_is_aggregation(self):
        expr = exp.Sum(this=exp.Column(this=exp.Identifier(this="amount")))
        assert classify_transform(expr) == TransformType.AGGREGATION

    def test_window_is_window(self):
        expr = exp.Window(this=exp.RowNumber(), partition_by=[], order=None)
        assert classify_transform(expr) == TransformType.WINDOW

    def test_case_is_conditional(self):
        expr = exp.Case()
        assert classify_transform(expr) == TransformType.CONDITIONAL

    def test_coalesce_is_function(self):
        expr = exp.Coalesce(this=exp.Column(this=exp.Identifier(this="name")))
        assert classify_transform(expr) == TransformType.FUNCTION

    def test_none_is_unknown(self):
        assert classify_transform(None) == TransformType.UNKNOWN


class TestAnalyzeModelColumns:
    def test_passthrough(self):
        result = analyze_model_columns("SELECT order_id FROM stg_orders", "orders")
        assert any(e.transform_type == TransformType.PASSTHROUGH for e in result.edges)

    def test_rename(self):
        result = analyze_model_columns("SELECT id AS order_id FROM raw_orders", "stg_orders")
        assert any(e.transform_type == TransformType.RENAME for e in result.edges)

    def test_aggregation(self):
        result = analyze_model_columns(
            "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
            "customer_totals",
            schema={"orders": {"customer_id": "INT", "amount": "DECIMAL"}},
        )
        assert any(e.transform_type == TransformType.AGGREGATION for e in result.edges)

    def test_case_when(self):
        sql = "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active FROM users"
        result = analyze_model_columns(sql, "user_flags")
        assert any(e.transform_type == TransformType.CONDITIONAL for e in result.edges)

    def test_window_function(self):
        sql = "SELECT ROW_NUMBER() OVER (ORDER BY created_at) AS row_num FROM events"
        result = analyze_model_columns(sql, "numbered_events")
        assert any(e.transform_type == TransformType.WINDOW for e in result.edges)

    def test_cast(self):
        sql = "SELECT CAST(id AS VARCHAR) AS id_str FROM users"
        result = analyze_model_columns(sql, "users_str")
        assert any(e.transform_type == TransformType.CAST for e in result.edges)

    def test_arithmetic(self):
        sql = "SELECT amount * 1.1 AS amount_with_tax FROM orders"
        result = analyze_model_columns(sql, "orders_taxed")
        assert any(e.transform_type == TransformType.ARITHMETIC for e in result.edges)

    def test_function(self):
        sql = "SELECT COALESCE(name, 'unknown') AS name FROM users"
        result = analyze_model_columns(sql, "clean_users")
        assert any(e.transform_type == TransformType.FUNCTION for e in result.edges)

    def test_transform_sql_is_captured(self):
        """The actual SQL expression must be on the edge."""
        sql = "SELECT amount * 1.1 AS amount_with_tax FROM orders"
        result = analyze_model_columns(sql, "orders_taxed")
        assert len(result.edges) > 0
        assert result.edges[0].transform_sql  # Must not be empty

    def test_unparseable_sql_does_not_crash(self):
        """Garbage SQL should return empty edges, not raise."""
        result = analyze_model_columns("THIS IS NOT SQL", "bad_model")
        assert result.edges == []

    def test_empty_sql_returns_empty(self):
        result = analyze_model_columns("", "empty_model")
        assert result.edges == []

    def test_cte(self):
        sql = """
        WITH base AS (
            SELECT id, amount FROM raw_orders
        )
        SELECT id AS order_id, amount * 100 AS amount_cents FROM base
        """
        result = analyze_model_columns(sql, "orders")
        assert len(result.edges) >= 2

    def test_source_column_is_set(self):
        result = analyze_model_columns("SELECT order_id FROM stg_orders", "orders")
        assert len(result.edges) > 0
        assert result.edges[0].source.column == "order_id"
        assert result.edges[0].source.model == "stg_orders"

    def test_target_model_and_column_set(self):
        result = analyze_model_columns("SELECT order_id FROM stg_orders", "orders")
        assert len(result.edges) > 0
        edge = result.edges[0]
        assert edge.target.model == "orders"
        assert edge.target.column == "order_id"

    def test_with_schema_hint(self):
        sql = "SELECT id AS order_id FROM raw_orders"
        schema = {"raw_orders": {"id": "INT"}}
        result = analyze_model_columns(sql, "stg_orders", schema=schema)
        assert len(result.edges) > 0

    def test_with_dialect(self):
        sql = "SELECT order_id FROM stg_orders"
        result = analyze_model_columns(sql, "orders", dialect="postgres")
        assert len(result.edges) > 0

    def test_no_duplicate_edges(self):
        sql = "SELECT order_id FROM stg_orders"
        result = analyze_model_columns(sql, "orders")
        keys = [(e.source.model, e.source.column, e.target.model, e.target.column) for e in result.edges]
        assert len(keys) == len(set(keys))

    def test_rename_source_column_preserved(self):
        result = analyze_model_columns("SELECT id AS order_id FROM raw_orders", "stg_orders")
        rename_edges = [e for e in result.edges if e.target.column == "order_id"]
        assert len(rename_edges) > 0
        assert rename_edges[0].source.column == "id"

    def test_columns_attempted_tracked(self):
        """columns_attempted should equal number of output columns in SELECT."""
        sql = "SELECT id, amount, status FROM orders"
        result = analyze_model_columns(sql, "test_model")
        assert result.columns_attempted == 3

    def test_columns_traced_tracked(self):
        """columns_traced should be > 0 when edges are found."""
        sql = "SELECT order_id FROM stg_orders"
        result = analyze_model_columns(sql, "orders")
        assert result.columns_traced > 0

    def test_select_star_with_schema_expands(self):
        """SELECT * should expand to explicit columns when schema is provided."""
        schema = {"stg_orders": {"order_id": "INT", "customer_id": "INT", "amount": "DECIMAL"}}
        result = analyze_model_columns("SELECT * FROM stg_orders", "orders", schema=schema)
        target_cols = {e.target.column for e in result.edges}
        assert "order_id" in target_cols
        assert "customer_id" in target_cols
        assert "amount" in target_cols

    def test_select_star_no_schema_returns_empty(self):
        """SELECT * without schema cannot be expanded — returns 0 edges."""
        result = analyze_model_columns("SELECT * FROM stg_orders", "orders")
        assert result.edges == []
