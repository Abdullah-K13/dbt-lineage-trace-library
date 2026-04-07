"""Tests for dialect detection."""

from __future__ import annotations

import pytest

from dbt_lineage.dialect import detect_dialect, ADAPTER_TO_DIALECT


class TestDetectDialect:
    def test_snowflake(self):
        assert detect_dialect("snowflake") == "snowflake"

    def test_bigquery(self):
        assert detect_dialect("bigquery") == "bigquery"

    def test_postgres(self):
        assert detect_dialect("postgres") == "postgres"

    def test_redshift(self):
        assert detect_dialect("redshift") == "redshift"

    def test_databricks(self):
        assert detect_dialect("databricks") == "databricks"

    def test_duckdb(self):
        assert detect_dialect("duckdb") == "duckdb"

    def test_mssql_maps_to_tsql(self):
        assert detect_dialect("mssql") == "tsql"

    def test_sqlserver_maps_to_tsql(self):
        assert detect_dialect("sqlserver") == "tsql"

    def test_athena_maps_to_presto(self):
        assert detect_dialect("athena") == "presto"

    def test_unknown_returns_none(self):
        assert detect_dialect("unknown_warehouse") is None

    def test_case_insensitive(self):
        assert detect_dialect("SNOWFLAKE") == "snowflake"
        assert detect_dialect("BigQuery") == "bigquery"

    def test_all_known_adapters_have_mapping(self):
        for adapter in ADAPTER_TO_DIALECT:
            result = detect_dialect(adapter)
            assert result is not None, f"Adapter '{adapter}' returned None"
