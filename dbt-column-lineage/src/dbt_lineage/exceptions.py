"""Custom exceptions for dbt-column-lineage."""


class DbtLineageError(Exception):
    """Base exception for all dbt-lineage errors."""


class ManifestNotFoundError(DbtLineageError):
    """Raised when manifest.json cannot be found."""


class ManifestParseError(DbtLineageError):
    """Raised when manifest.json cannot be parsed."""


class CatalogParseError(DbtLineageError):
    """Raised when catalog.json cannot be parsed."""


class ColumnNotFoundError(DbtLineageError):
    """Raised when a queried column doesn't exist in the graph."""


class SqlParseError(DbtLineageError):
    """Raised when a model's SQL cannot be parsed by SQLGlot."""
