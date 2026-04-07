"""Data models for dbt-column-lineage."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pydantic import BaseModel


# ─── Enums ────────────────────────────────────────────────────────────────

class TransformType(str, Enum):
    """Classification of how a column is transformed between models."""
    PASSTHROUGH = "passthrough"       # SELECT col (no change)
    RENAME = "rename"                 # SELECT col AS new_name (value unchanged)
    CAST = "cast"                     # CAST(col AS INT), col::TEXT
    ARITHMETIC = "arithmetic"         # col * 100, col_a + col_b
    AGGREGATION = "aggregation"       # SUM(col), COUNT(col), AVG(col)
    CONDITIONAL = "conditional"       # CASE WHEN ... THEN col, IF(), IIF()
    FUNCTION = "function"             # COALESCE(col, 0), UPPER(col), TRIM()
    WINDOW = "window"                 # ROW_NUMBER() OVER(), LAG(), LEAD()
    JOIN_DERIVED = "join_derived"     # Column brought in via JOIN condition
    COMPLEX = "complex"               # Nested/compound that doesn't fit above
    LITERAL = "literal"               # Pure literal / constant (0, 'Stripe', NULL, CURRENT_DATE)
    UNKNOWN = "unknown"               # Could not determine

    def __str__(self) -> str:
        return self.value


class ResolutionStatus(str, Enum):
    """Confidence level for a resolved column lineage path.

    Tells callers how much to trust a given edge or trace result.

    RESOLVED   — Fully traced through explicit column references to a named source.
    PARTIAL    — Some branches of a UNION / CASE were resolved; others were not.
    AMBIGUOUS  — Multiple possible source tables; attribution is uncertain
                 (e.g. unqualified column in a multi-table JOIN scope).
    UNRESOLVED — No source found: pure literal, recursive depth exceeded,
                 unqualified column in a multi-source scope, or unsupported construct.
    """
    RESOLVED   = "resolved"
    PARTIAL    = "partial"
    AMBIGUOUS  = "ambiguous"
    UNRESOLVED = "unresolved"

    def __str__(self) -> str:
        return self.value


class ResourceType(str, Enum):
    """dbt resource types we care about."""
    MODEL = "model"
    SOURCE = "source"
    SEED = "seed"
    SNAPSHOT = "snapshot"


# ─── Core data structures ────────────────────────────────────────────────

@dataclass(frozen=True)
class ColumnRef:
    """A reference to a specific column in a specific model.
    Used as node identifiers in the graph.
    Frozen so it can be used as a dict key and networkx node."""
    model: str      # Short model name (e.g., "stg_orders")
    column: str     # Column name (e.g., "order_id")

    def __str__(self) -> str:
        return f"{self.model}.{self.column}"


@dataclass
class ColumnEdge:
    """An edge in the column lineage graph representing one transformation.
    This is the core data structure — it carries the transformation SQL."""
    source: ColumnRef
    target: ColumnRef
    transform_sql: str                       # The actual SQL expression string
    transform_type: TransformType            # Classification of the transform
    model_unique_id: str = ""               # dbt unique_id of the model where this transform occurs
    transform_chain: list[dict] = field(default_factory=list)
    # transform_chain captures every CTE/intermediate step between source and target.
    # Each entry: {"step": "cte_name_or_column", "sql": "...", "type": "arithmetic"}
    # Ordered from source → target (raw first, mart last).
    # Empty for single-hop transforms with no intermediate CTEs.
    resolution_status: "ResolutionStatus" = field(default=None)
    # resolution_status records how confidently this edge was resolved.
    # None means the edge was created before status tracking was introduced;
    # treat it as RESOLVED for backward compatibility.

    def __post_init__(self) -> None:
        if self.resolution_status is None:
            # Import here to avoid circular at module level; ResolutionStatus is
            # defined just above in the same module but the default= in field()
            # is evaluated at class-definition time, before the enum exists.
            self.resolution_status = ResolutionStatus.RESOLVED

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_model": self.source.model,
            "source_column": self.source.column,
            "target_model": self.target.model,
            "target_column": self.target.column,
            "transform_sql": self.transform_sql,
            "transform_type": str(self.transform_type),
            "transform_chain": self.transform_chain,
            "model_unique_id": self.model_unique_id,
            "resolution_status": str(self.resolution_status) if self.resolution_status else "resolved",
        }


@dataclass
class ModelInfo:
    """Metadata about a dbt model, source, seed, or snapshot."""
    unique_id: str              # e.g., "model.jaffle_shop.orders"
    name: str                   # e.g., "orders"
    resource_type: ResourceType
    database: str = ""
    schema_name: str = ""       # "schema" is a Python builtin, avoid shadowing
    compiled_sql: str = ""      # The fully compiled SQL (Jinja resolved)
    depends_on: list[str] = field(default_factory=list)  # list of upstream unique_ids
    columns: dict[str, str] = field(default_factory=dict)  # column_name → description
    original_file_path: str = ""


@dataclass
class GraphBuildStats:
    """Statistics from building the lineage graph — tells you how accurate the result is."""
    total_models: int = 0               # Models attempted
    models_analyzed: int = 0            # Models successfully analyzed
    models_skipped: int = 0             # Sources/seeds with no SQL
    models_failed: int = 0              # Models whose SQL could not be parsed
    total_edges: int = 0                # Column edges built
    total_columns: int = 0              # Unique column nodes in graph
    schema_coverage: int = 0            # Tables with known schema at analysis time
    columns_attempted: int = 0          # Total columns SQLGlot tried to trace
    columns_traced: int = 0             # Columns that produced at least one edge
    unresolved_models: list[str] = field(default_factory=list)        # Models with 0 edges
    partially_analyzed_models: list[str] = field(default_factory=list)  # Models where some columns failed

    @property
    def success_rate(self) -> float:
        """Fraction of non-skipped models that were successfully analyzed."""
        attempted = self.total_models - self.models_skipped
        if attempted == 0:
            return 1.0
        return round(self.models_analyzed / attempted, 3)

    @property
    def column_coverage(self) -> float:
        """Fraction of attempted columns that produced at least one lineage edge."""
        if self.columns_attempted == 0:
            return 1.0
        return round(self.columns_traced / self.columns_attempted, 3)

    def to_dict(self) -> dict:
        return {
            "total_models": self.total_models,
            "models_analyzed": self.models_analyzed,
            "models_skipped": self.models_skipped,
            "models_failed": self.models_failed,
            "total_edges": self.total_edges,
            "total_columns": self.total_columns,
            "schema_coverage": self.schema_coverage,
            "columns_attempted": self.columns_attempted,
            "columns_traced": self.columns_traced,
            "column_coverage": self.column_coverage,
            "success_rate": self.success_rate,
            "unresolved_models": self.unresolved_models,
            "partially_analyzed_models": self.partially_analyzed_models,
        }


@dataclass
class ModelAnalysisResult:
    """Result of analyzing a single model's compiled SQL.

    Returned by analyze_model_columns(). Carries lineage edges plus
    per-column coverage stats so the caller knows if lineage is complete
    or only partial (some columns failed to trace).
    """
    edges: list[ColumnEdge]
    columns_attempted: int = 0       # Output columns SQLGlot tried to trace
    columns_traced: int = 0          # Columns that produced at least one edge
    failed_columns: list[str] = field(default_factory=list)   # Column names that failed
    ambiguous_columns: list[str] = field(default_factory=list) # Unqualified in multi-table scope
    unresolved_columns: list[str] = field(default_factory=list) # Pure literals / depth exceeded
    output_column_names: list[str] = field(default_factory=list) # All output cols attempted (incl. no-edge)


@dataclass
class ImpactResult:
    """Result of an impact analysis query."""
    source: ColumnRef                    # The column being changed
    affected_columns: list[ColumnRef]    # All downstream columns affected
    affected_models: list[str]           # Unique model names affected
    edges: list[ColumnEdge]              # All edges in the impact path


@dataclass
class TraceResult:
    """Result of an upstream trace query."""
    target: ColumnRef                    # The column being traced
    source_columns: list[ColumnRef]      # All upstream source columns
    source_models: list[str]             # Unique model names in the trace
    edges: list[ColumnEdge]              # All edges in the trace path


# ─── Pydantic models for parsing manifest.json ───────────────────────────

class ManifestMetadata(BaseModel):
    """Top-level metadata from manifest.json."""
    dbt_version: str = ""
    adapter_type: str = ""
    project_name: str = ""

    model_config = {"extra": "allow"}


class ManifestNodeDependsOn(BaseModel):
    """depends_on block within a manifest node."""
    nodes: list[str] = []
    macros: list[str] = []

    model_config = {"extra": "allow"}


class ManifestColumnInfo(BaseModel):
    """Column info within a manifest node."""
    name: str = ""
    description: str = ""
    data_type: str | None = None

    model_config = {"extra": "allow"}


class ManifestNode(BaseModel):
    """A single node from manifest.json's 'nodes' or 'sources' dict.
    We only extract the fields we need."""
    unique_id: str = ""
    name: str = ""
    resource_type: str = ""
    database: str | None = None
    schema_: str | None = None  # 'schema' in the JSON
    compiled_code: str | None = None  # Post-Jinja SQL; may also appear as compiled_sql
    compiled_sql: str | None = None   # Older dbt versions use this field name
    raw_code: str | None = None
    raw_sql: str | None = None
    depends_on: ManifestNodeDependsOn = ManifestNodeDependsOn()
    columns: dict[str, ManifestColumnInfo] = {}
    original_file_path: str = ""
    alias: str | None = None

    model_config = {
        "extra": "allow",
        "populate_by_name": True,
    }

    @classmethod
    def model_validate(cls, obj: Any, **kwargs) -> "ManifestNode":
        # Remap 'schema' key to 'schema_' before validation
        if isinstance(obj, dict) and "schema" in obj and "schema_" not in obj:
            obj = dict(obj)
            obj["schema_"] = obj.pop("schema")
        return super().model_validate(obj, **kwargs)

    def get_compiled_sql(self) -> str:
        """Return compiled SQL, handling different dbt versions.
        Older dbt uses 'compiled_sql', newer uses 'compiled_code'.
        Falls back to raw SQL if compiled is not available."""
        return self.compiled_code or self.compiled_sql or self.raw_code or self.raw_sql or ""

    def get_table_alias(self) -> str:
        """Return the table name as it appears in SQL.
        dbt uses 'alias' if set, otherwise 'name'."""
        return self.alias or self.name
