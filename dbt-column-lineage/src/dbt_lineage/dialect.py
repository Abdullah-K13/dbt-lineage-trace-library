"""Auto-detect SQL dialect from dbt manifest metadata."""

ADAPTER_TO_DIALECT: dict[str, str] = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "postgres": "postgres",
    "redshift": "redshift",
    "databricks": "databricks",
    "duckdb": "duckdb",
    "trino": "trino",
    "spark": "spark",
    "clickhouse": "clickhouse",
    "mysql": "mysql",
    "mssql": "tsql",
    "sqlserver": "tsql",
    "athena": "presto",
    "presto": "presto",
}


def detect_dialect(adapter_type: str) -> str | None:
    """Map a dbt adapter type to a SQLGlot dialect string.
    Returns None if the adapter is unknown (SQLGlot will use default parsing)."""
    return ADAPTER_TO_DIALECT.get(adapter_type.lower())
