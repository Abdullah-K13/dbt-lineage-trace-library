# dbt-column-lineage

Column-level lineage for dbt projects — no database connection required.

Parses dbt's `manifest.json` and `catalog.json`, analyzes compiled SQL with SQLGlot, and builds an in-memory directed graph of column-to-column transformations. Once built, the graph can answer upstream trace and downstream impact queries in milliseconds, and is persisted to a SQLite cache so subsequent loads are instant.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI](#cli)
- [How It Works — In Depth](#how-it-works--in-depth)
  - [1. Parsing the dbt Artifacts](#1-parsing-the-dbt-artifacts)
  - [2. Schema Construction](#2-schema-construction)
  - [3. Topological Ordering](#3-topological-ordering)
  - [4. SQL Analysis — Per Model](#4-sql-analysis--per-model)
  - [5. Graph Construction](#5-graph-construction)
  - [6. Caching — In-Memory and SQLite](#6-caching--in-memory-and-sqlite)
  - [7. Querying the Graph](#7-querying-the-graph)
- [API Reference](#api-reference)
- [Transform Types](#transform-types)
- [Accuracy Notes](#accuracy-notes)
- [Supported Warehouses](#supported-warehouses)
- [License](#license)

---

## Installation

```bash
pip install dbt-column-lineage
```

---

## Quick Start

```python
from dbt_lineage import LineageGraph

# Point at your dbt target directory. catalog.json is auto-discovered.
g = LineageGraph("target/manifest.json")

# Trace a column upstream to all its raw sources
result = g.trace("orders", "total_with_tax")
for edge in result.edges:
    print(f"{edge.source} → {edge.target}  [{edge.transform_type}]")
print("Root sources:", result.source_columns)

# Find everything affected by changing a column
result = g.impact("stg_orders", "order_id")
print("Affected models:", sorted(result.affected_models))
print("Affected columns:", sorted(result.affected_columns))
```

**For large projects**, the first build may take up to a few minutes. Subsequent runs load from the SQLite cache in under a second:

```python
# Second run — instant load from .lineage_<hash>.db
g = LineageGraph("target/manifest.json")
```

**Targeted builds** (analyze only the models you care about):

```python
g = LineageGraph("target/manifest.json", select="rep_user_feedback")
```

---

## CLI

```bash
# Trace a column upstream
dbt-lineage -m target/manifest.json trace orders total_with_tax

# Impact analysis
dbt-lineage -m target/manifest.json impact stg_orders order_id

# List all models in the graph
dbt-lineage -m target/manifest.json list-models

# Export full graph as JSON
dbt-lineage -m target/manifest.json export -o lineage.json

# Show build stats
dbt-lineage -m target/manifest.json stats
```

---

## How It Works — In Depth

This section explains the complete pipeline from dbt artifacts to a queryable lineage graph.

### 1. Parsing the dbt Artifacts

**File:** `parser.py`

The library reads two dbt artifacts:

- **`manifest.json`** — contains every model, source, seed, and snapshot in the dbt project, along with their compiled SQL (Jinja already resolved), dependencies (`depends_on.nodes`), schema, database, and column documentation.
- **`catalog.json`** (optional) — produced by `dbt docs generate`, contains the actual column names and data types from the warehouse for every materialized model and source.

`parse_manifest()` iterates over `nodes` and `sources`, constructs a `ModelInfo` object for each supported resource type (`model`, `source`, `seed`, `snapshot`), and returns them keyed by `unique_id`.

**Source name disambiguation:** In multi-market dbt projects, multiple sources can share the same table name across different schemas (e.g., `p4h.users`, `pp.users`, `ma.users`). The parser detects these collisions and qualifies such names as `schema.table` to keep all graph nodes unambiguous.

`parse_catalog()` and `build_schema_dict()` extract column types from `catalog.json` and flatten them into a `{table_name: {col_name: data_type}}` dict that SQLGlot's qualify step can consume.

`build_table_lookup()` builds a multi-form lookup so that any way compiled SQL might reference a table — short name, `schema.table`, or `database.schema.table` — resolves to the canonical model name used as the graph node identifier.

---

### 2. Schema Construction

**File:** `api.py` — `LineageGraph.__init__`

Before any SQL is analyzed, the library builds two schemas:

**`cumulative_schema`** — starts with all source columns (from manifest YAML definitions, typed via catalog) plus all catalog-backed model columns. It grows during analysis: after each model is analyzed, its output columns are propagated into `cumulative_schema` so that downstream models can resolve those references.

**`analysis_schema`** — a stable snapshot of `catalog + sources` used as the actual input to `analyze_model_columns()`. It only grows with columns from non-catalog models whose analysis is reliable (see promotion rules below). The separation prevents a contamination cascade: if a staging model's star expansion produces wrong columns, those wrong columns never feed into analysis of downstream catalog-backed models.

**Safe promotion to `analysis_schema`:** A non-catalog model's output columns are promoted only if:
1. The model's outermost `FROM` clause references a real table (not a CTE name). Models whose outer `SELECT * FROM some_cte` rely on qualify() for star expansion — their output may be incomplete if qualify partially fails.
2. At least 3 non-metadata columns (excluding `_sdc_*`, `_dbt_*` prefixes) are present. This filters out models that produced only internal Stitch/dbt metadata columns.

---

### 3. Topological Ordering

**File:** `parser.py` — `topological_levels()`

Models must be analyzed in dependency order: an upstream model's output columns must be known before a downstream model that references it is analyzed. The library uses `topological_sort()` (NetworkX if available, otherwise Kahn's algorithm) and groups models into **parallel-safe levels** via `topological_levels()`.

```
Level 0:  sources, seeds (no SQL to analyze)
Level 1:  base_* models that directly wrap sources
Level 2:  stg_* staging models
Level 3:  intermediate models
...
Level N:  reporting / mart models
```

Models within the same level have no dependencies on each other and can theoretically run in parallel. Levels are processed sequentially so that `cumulative_schema` is fully populated from level N before level N+1 is analyzed.

---

### 4. SQL Analysis — Per Model

**File:** `sql_analyzer.py` — `analyze_model_columns()`

This is the core of the library. For each model with compiled SQL, it extracts all column-level lineage edges. The pipeline has multiple stages:

#### 4a. Jinja Guard

Models without compiled SQL (only `raw_sql` with unresolved `{{ }}` blocks) are skipped immediately. Attempting to parse Jinja templates causes 30–60 second hangs in SQLGlot.

#### 4b. Fast Path: Simple Passthrough

`_try_passthrough_select_star_ast()` handles the extremely common pattern:

```sql
SELECT * FROM source_table   -- no JOINs, no CTEs, no expressions
```

For these models, it generates one `PASSTHROUGH` edge per column from the schema without running SQLGlot's lineage engine. This covers all `base_*` models in one function call.

#### 4c. CTE Guard for Star Expansion

`_expand_star_with_schema()` rewrites `SELECT *` to explicit column names using the schema. However, if the outermost `SELECT * FROM <name>` references a **CTE name** (not a real table), this expansion is skipped. The reason: `_expand_star_with_schema` scans ALL tables in the AST — including those inside CTE bodies — so it would inject pre-rename inner column names into the outer SELECT, corrupting the analysis.

When the outer FROM is a CTE, SQLGlot's `qualify(expand_stars=True)` handles star expansion using its own CTE-scope-aware logic.

#### 4d. Qualify Step

SQLGlot's `qualify()` is called with `expand_stars=True` and a **filtered local schema** (only the tables referenced in this specific SQL). Passing the full 800+ model schema to every call would add ~1.5 s per model; filtering to referenced tables only drops this to ~2 ms.

`qualify()` does three important things:
1. **Expands `SELECT *`** to explicit column lists using the schema and CTE definitions.
2. **Adds table prefixes** to all column references (`id_user` → `stg_orders.id_user`), making it unambiguous which table each column comes from.
3. **Resolves BigQuery `SELECT * EXCEPT (...)`** syntax.

The qualified AST is serialized back to SQL and re-parsed into a clean AST (this avoids internal state left on the AST by qualify that can confuse CTE map detection).

**Virtual CTE detection:** `qualify()` sometimes injects "virtual passthrough CTEs" for real tables to help with star expansion, e.g.:

```sql
dim_users AS (SELECT dim_users.id_user AS id_user, ... FROM dim_users AS dim_users)
```

`_build_cte_map()` detects and skips these: a CTE is considered virtual if its body is a plain `SELECT` (no star) from exactly one table whose last name segment matches the CTE alias. Real wrapping CTEs like `top_breeders AS (SELECT * FROM owp-dw.core.top_breeders)` use `SELECT *` and are preserved.

#### 4e. Single-Pass Analysis

`_single_pass_analyze_ast()` walks the qualified AST once, processing every expression in the outermost SELECT. For each output column it:

1. Classifies the transform type (`classify_transform()`).
2. If it's a passthrough or rename, walks into the referenced CTE to find the real transform type deeper in the chain (`_collect_cte_transform()`).
3. Calls `_resolve_expr_sources()` to find all source `(table, column)` pairs for the expression.

The single-pass approach avoids N redundant parse calls for N output columns.

#### 4f. CTE Chain Resolution

`_resolve_expr_sources()` and `_resolve_col_through_cte()` form a recursive pair that follows column references through arbitrarily deep CTE chains:

- **`_resolve_expr_sources(expr)`** — walks all `Column` nodes inside an expression. For each `table.column` reference:
  - If `table` is a CTE name in the current scope (and not being used as a local alias for something else), recurse via `_resolve_col_through_cte`.
  - If `table` is a table alias pointing to a CTE, resolve the alias first, then recurse.
  - Otherwise, resolve via `table_lookup` and return the real source table + column.

- **`_resolve_col_through_cte(cte_body, col_name)`** — searches `col_name` in each SELECT branch of the CTE body (handles `UNION ALL` by flattening with `_flatten_union()`). For each branch:
  - If the column is **explicitly listed** → recurse into its defining expression.
  - If the CTE has **`SELECT *`** → iterate all FROM-clause tables and recurse into each (or return them as real sources if they're not CTEs).

  Critically, each call builds a **local alias map** from the current SELECT's FROM/JOIN tables. This prevents a scoping bug: a CTE named `nps` can use `nps` as a local alias for `nps_raw` inside its own body. A flat global alias map would confuse these two meanings; the local map always takes precedence.

A **depth guard** (max depth 15) prevents infinite recursion in pathological cases.

#### 4g. Per-Column Fallback

If the single-pass analysis produces zero edges (complex subqueries, unsupported syntax), the library falls back to SQLGlot's built-in `sqlglot_lineage()` called per output column. This is slower (one call per column) but handles more SQL patterns.

---

### 5. Graph Construction

**File:** `graph.py` — `ColumnLineageGraph`

The graph is a **NetworkX `DiGraph`** where:
- **Nodes** are `ColumnRef(model, column)` frozen dataclasses (hashable).
- **Edges** point from source column → target column, carrying:
  - `transform_type` — the most significant transform in the chain (e.g., `AGGREGATION` beats `PASSTHROUGH`)
  - `transform_sql` — the SQL expression of the most significant step
  - `transform_chain` — full list of intermediate CTE steps, source→target ordered
  - `model_unique_id` — the dbt unique ID of the model that produced this edge

After each model is analyzed, its edges are added to the graph. Source model names are resolved through `table_lookup` to ensure qualified names (e.g., `owp-dw.staging.stg_orders`) collapse to their canonical short form.

---

### 6. Caching — In-Memory and SQLite

**Files:** `api.py`, `storage.py`

**In-memory cache** (`LineageGraph._cache`) — a class-level dict keyed by a SHA-256 hash of the manifest + catalog file contents, dialect, and select filter. If the same combination is requested twice in one Python process, the second call reuses the existing graph object instantly.

**SQLite disk cache** (`.lineage_<hash16>.db`) — saved next to `manifest.json` after every successful full build. On the next process startup, `load_graph()` checks for this file and validates the full cache key. If it matches, the graph is reconstructed from the SQLite `edges` table in ~0.2 s rather than re-running the full SQL analysis (which can take minutes for large projects).

The SQLite schema stores each edge as a row with source/target model+column, transform type, transform SQL, transform chain (JSON), and model unique ID. Indexes on `(target_model, target_column)` and `(source_model, source_column)` make point lookups fast.

To force a complete rebuild, delete the `.lineage_*.db` file next to your `manifest.json`.

---

### 7. Querying the Graph

**File:** `graph.py` — `ColumnLineageGraph`

All query methods run on the in-memory NetworkX graph:

**`trace(model, column)`** — returns a `TraceResult` with all upstream edges and root source columns. Implemented as a reverse BFS from the target node through all incoming edges.

**`impact(model, column)`** — returns an `ImpactResult` with all downstream affected columns and models. Implemented as a forward BFS from the source node through all outgoing edges.

Both methods are case-insensitive (matches Snowflake's uppercase-everything behavior and BigQuery's lowercase convention).

---

## API Reference

```python
from dbt_lineage import LineageGraph, TransformType

# Construct the graph
g = LineageGraph(
    manifest_path="target/manifest.json",
    catalog_path="target/catalog.json",   # optional, auto-discovered if omitted
    dialect="bigquery",                    # optional, auto-detected from manifest
    select="rep_orders",                   # optional, restrict to a model's lineage
)

# Upstream trace
result = g.trace("model_name", "column_name")
result.edges           # List[ColumnEdge] — all hops, topologically ordered
result.source_columns  # List[ColumnRef] — root sources (no further upstream)

# Downstream impact
result = g.impact("model_name", "column_name")
result.affected_columns  # List[ColumnRef]
result.affected_models   # Set[str]
result.edges             # List[ColumnEdge] — propagation path

# Other queries
g.edges_between("source_model", "target_model")  # List[ColumnEdge]
g.model_dependencies("model")                     # List[str] — upstream model names
g.all_models()                                    # List[str]
g.all_columns("model")                            # List[str]
g.search_columns("user_id")                       # List[ColumnRef] — substring match
g.get_transforms_by_type(TransformType.CAST)      # List[ColumnEdge]
g.stats()                                         # GraphBuildStats
g.to_dict()                                       # JSON-serializable dict
g.to_networkx()                                   # nx.DiGraph for advanced queries

# Cache management
LineageGraph.clear_cache()   # flush the in-memory cache
```

### ColumnEdge

```python
edge.source           # ColumnRef(model, column) — upstream column
edge.target           # ColumnRef(model, column) — downstream column
edge.transform_type   # TransformType enum value
edge.transform_sql    # str — SQL expression of the primary transform
edge.transform_chain  # list[dict] — full CTE chain, innermost → outermost
                      #   each step: {"step": str, "sql": str, "type": str}
edge.model_unique_id  # str — dbt unique ID of the producing model
```

---

## Transform Types

| Type | Description | Example |
|------|-------------|---------|
| `passthrough` | Column flows through unchanged | `SELECT col` |
| `rename` | Column renamed with no value change | `SELECT col AS new_name` |
| `cast` | Type conversion | `CAST(col AS INT64)` |
| `arithmetic` | Math expression | `price * quantity` |
| `aggregation` | Aggregate function | `SUM(amount)`, `COUNT(*)` |
| `conditional` | Branch expression | `CASE WHEN active THEN 1 ELSE 0 END` |
| `function` | Scalar function | `COALESCE(a, b)`, `LOWER(email)` |
| `window` | Window function | `ROW_NUMBER() OVER (PARTITION BY ...)` |
| `complex` | Nested compound expression | Combination of the above |

For multi-step CTE chains, the **most significant** transform in the chain is surfaced as the primary `transform_type`. For example, if a column passes through three CTEs where one step applies `MIN(...)`, the edge reports `aggregation` even if the final SELECT is a plain passthrough.

---

## Accuracy Notes

**Always provide `catalog.json`** (`dbt docs generate`). Without it:
- `SELECT *` cannot be expanded unless all source columns are documented in dbt YAML.
- Ambiguous column references in JOINs may not be attributed to the correct source table.

**Columns not traceable** in the current implementation:
- Columns derived entirely from **literal values** (e.g., `'NPS' AS feedback_source`, `5 AS max_score`) — no column-level source exists.
- Columns where the source expression uses **unqualified column references** inside a multi-table CTE (e.g., `CASE WHEN LOWER(survey) LIKE '%nps%' THEN 10 END` where `survey` has no table prefix). These require inferring the FROM table from context, which is not yet implemented.
- Models with **unresolved Jinja** (`{{`, `{%`) — only compiled SQL is analyzed.

The build statistics (`g.stats()`) report `success_rate` (models that produced at least one edge) and `column_coverage` (columns traced / columns attempted). A high `column_coverage` with a lower `success_rate` typically means many models are sources or seeds with no SQL to analyze.

---

## Supported Warehouses

| Warehouse | SQLGlot Dialect |
|-----------|----------------|
| Snowflake | `snowflake` |
| BigQuery | `bigquery` |
| Postgres / Redshift | `postgres` |
| Databricks / Spark | `databricks` / `spark` |
| DuckDB | `duckdb` |
| Trino / Presto / Athena | `trino` |
| ClickHouse | `clickhouse` |
| MySQL | `mysql` |
| SQL Server | `tsql` |

The dialect is auto-detected from `manifest.json`'s `metadata.adapter_type` field. Override it with the `dialect=` parameter if needed.

---

## License

Apache 2.0
