"""SQLGlot-based column lineage and transform extraction.

Architecture
------------
Resolution flows through scoped symbol tables — one SelectScope per SELECT
block — instead of walking the raw AST with a global alias map.

  1. _build_select_scope()   — builds a SelectScope from a single SELECT node:
                               relations (alias → canonical name) and
                               output_exprs (col_name → AST expression).
  2. _resolve_col_through_cte() — recursively walks CTE bodies using per-SELECT
                                  scopes; handles UNION ALL branch splitting.
  3. _resolve_expr_sources()  — walks Column nodes inside an expression,
                                resolves via scope (includes single-source
                                fallback for unqualified references), and
                                returns (source_table, source_col, status) triples.
  4. _single_pass_analyze_ast() — orchestrates the above for the outermost SELECT.
  5. analyze_model_columns()  — entry point; handles star expansion, qualify(),
                                single-pass → per-column fallback chain.

Confidence scoring
------------------
Every resolution attempt returns a ResolutionStatus alongside the source pair:
  RESOLVED   — explicit table qualifier found and followed
  PARTIAL    — UNION branches with mixed success
  AMBIGUOUS  — unqualified column in a multi-table scope
  UNRESOLVED — no source (literal, depth exceeded, unsupported construct)
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sqlglot_lineage
from sqlglot.optimizer.qualify import qualify

from .models import ColumnEdge, ColumnRef, ModelAnalysisResult, ResolutionStatus, TransformType

logger = logging.getLogger("dbt_lineage")

# Priority ranking for picking the "most significant" transform in a multi-step chain.
# Higher number = shown as the primary transform_type on the edge.
_TYPE_PRIORITY: dict[TransformType, int] = {
    TransformType.WINDOW:      8,
    TransformType.AGGREGATION: 7,
    TransformType.CONDITIONAL: 6,
    TransformType.ARITHMETIC:  5,
    TransformType.CAST:        4,
    TransformType.FUNCTION:    3,
    TransformType.RENAME:      2,
    TransformType.COMPLEX:     1,
    TransformType.LITERAL:     1,  # Show literal over passthrough
    TransformType.PASSTHROUGH: 0,
    TransformType.UNKNOWN:     0,
}

# Max worker threads for parallel column tracing. Bounded to avoid spawning
# hundreds of threads on very wide models.
_MAX_WORKERS = 1  # SQLGlot is CPU-bound Python; threading adds GIL contention overhead

# Max columns to trace per model. Models with more columns are capped to
# avoid runaway analysis time. Override with DBT_LINEAGE_MAX_COLUMNS env var.
_MAX_COLUMNS_PER_MODEL = int(os.environ.get("DBT_LINEAGE_MAX_COLUMNS", "60"))

# Sentinel model name for pure literal / constant column values.
# Edges with this as the source have no upstream column — the value is the SQL
# literal itself (e.g. 0, 'Stripe', NULL, CURRENT_DATE).
_LITERAL_MODEL = "__literal__"


def _is_literal_expr(expr: exp.Expression | None) -> bool:
    """Return True when *expr* is a pure literal with no column references.

    Examples that return True:
        0, 1, 'Stripe', NULL, CAST(NULL AS FLOAT64), CURRENT_DATE, TRUE
    Examples that return False:
        col, COALESCE(col, 0), col * 2, CASE WHEN col > 0 THEN 1 ELSE 0 END
    """
    if expr is None:
        return False
    return len(list(expr.find_all(exp.Column))) == 0


@dataclass
class SelectScope:
    """Per-SELECT-block symbol table.

    Captures every source relation visible in the FROM / JOIN clause of one
    SELECT node plus the single-source shortcut used for unqualified columns.

    Attributes
    ----------
    relations     : alias → canonical name mapping for every table/CTE
                    reference inside this SELECT's FROM and JOIN clauses.
                    Both the alias and the real name are registered so that
                    ``t.col`` and ``table_name.col`` both resolve correctly.
    single_source : When exactly one relation is in scope this holds the
                    canonical name.  Used to attribute unqualified column
                    references (e.g. bare ``amount`` inside a single-table CTE).
                    None when multiple relations are in scope (ambiguous).
    """
    relations: dict[str, str] = field(default_factory=dict)
    single_source: str | None = None


def _collect_window_partition_col_ids(expr: exp.Expression) -> set[int]:
    """Return the Python object-ids of every Column node that appears
    exclusively inside a window function's PARTITION BY or ORDER BY clause.

    These columns affect grouping / ordering but are NOT value contributors
    to the output column.  Treating them as sources produces spurious lineage
    (e.g. id_vas_order appearing as a source of amt_discount just because it
    is used in PARTITION BY id_vas_order inside revenue_ratio).

    We use object-id (id()) rather than sql() because the same column name
    may appear both inside the window clause and in the aggregated expression
    — they are different AST nodes with different ids.
    """
    over_bound: set[int] = set()
    for win in expr.find_all(exp.Window):
        # partition_by is a list of expressions
        for pb_expr in (win.args.get("partition_by") or []):
            for col in pb_expr.find_all(exp.Column):
                over_bound.add(id(col))
        # order is an Order node whose expressions are Ordered wrappers
        order_node = win.args.get("order")
        if order_node is not None:
            for col in order_node.find_all(exp.Column):
                over_bound.add(id(col))
    return over_bound


def _build_select_scope(
    select: exp.Select,
    alias_map: dict[str, str],
    table_lookup: dict[str, str] | None,
) -> SelectScope:
    """Build a SelectScope for one SELECT node.

    Scans the FROM and all JOINs, registers every table by its alias AND its
    real name, and resolves through alias_map / table_lookup so the scope
    contains canonical names (the same names used in cte_map / the graph).
    """
    relations: dict[str, str] = {}

    def _register(tbl: exp.Table) -> None:
        if not tbl.name:
            return
        raw = tbl.name
        # Resolve multi-part qualified name (db.schema.table → canonical)
        canonical = raw
        if table_lookup:
            cat = tbl.catalog.lower() if tbl.catalog else ""
            db = tbl.db.lower() if tbl.db else ""
            if cat and db:
                canonical = table_lookup.get(f"{cat}.{db}.{raw.lower()}", raw)
            if canonical == raw and db:
                canonical = table_lookup.get(f"{db}.{raw.lower()}", raw)
            if canonical == raw:
                canonical = table_lookup.get(raw.lower(), raw)
        # Also try alias_map for names already normalised upstream
        canonical = alias_map.get(canonical.lower(), canonical)

        relations[raw.lower()] = canonical
        if tbl.alias:
            relations[tbl.alias.lower()] = canonical

    from_node = select.find(exp.From)
    if from_node:
        for tbl in from_node.find_all(exp.Table):
            _register(tbl)

    for join in select.find_all(exp.Join):
        for tbl in join.find_all(exp.Table):
            _register(tbl)

    single = next(iter(relations.values())) if len(set(relations.values())) == 1 else None
    return SelectScope(relations=relations, single_source=single)


def classify_transform(expression: exp.Expression | None) -> TransformType:
    """Classify a SQLGlot expression into a TransformType."""
    if expression is None:
        return TransformType.UNKNOWN

    if isinstance(expression, exp.Column):
        return TransformType.PASSTHROUGH

    if isinstance(expression, exp.Alias):
        inner = expression.this
        if isinstance(inner, exp.Column):
            alias_name = expression.alias.lower() if expression.alias else ""
            col_name = inner.name.lower() if inner.name else ""
            if alias_name == col_name:
                return TransformType.PASSTHROUGH
            return TransformType.RENAME
        return classify_transform(inner)

    if isinstance(expression, (exp.Cast, exp.TryCast)):
        return TransformType.CAST

    if isinstance(expression, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod,
                                exp.IntDiv, exp.BitwiseAnd, exp.BitwiseOr)):
        return TransformType.ARITHMETIC

    if isinstance(expression, exp.AggFunc):
        return TransformType.AGGREGATION

    if isinstance(expression, exp.Window):
        return TransformType.WINDOW

    if isinstance(expression, (exp.Case, exp.If)):
        return TransformType.CONDITIONAL

    if isinstance(expression, exp.Func):
        return TransformType.FUNCTION

    # Pure literal / constant — no column references anywhere in the expression
    if _is_literal_expr(expression):
        return TransformType.LITERAL

    return TransformType.COMPLEX


def _build_alias_map(parsed: exp.Expression) -> dict[str, str]:
    """Map table aliases → real table names from the AST.
    Example: FROM orders o → {'o': 'orders', 'orders': 'orders'}
    """
    alias_map: dict[str, str] = {}
    for table in parsed.find_all(exp.Table):
        real_name = table.name
        if not real_name:
            continue
        alias_map[real_name.lower()] = real_name
        if table.alias:
            alias_map[table.alias.lower()] = real_name
    return alias_map


def _get_output_columns(parsed: exp.Expression) -> list[str]:
    """Extract output column names from the outermost SELECT."""
    select = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
    if select is None:
        return []
    return [
        expr.alias_or_name
        for expr in select.expressions
        if expr.alias_or_name and expr.alias_or_name != "*"
    ]


def _has_select_star(parsed: exp.Expression) -> bool:
    """Return True if the outermost SELECT contains a bare star (SELECT *)."""
    select = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
    if select is None:
        return False
    return any(isinstance(e, exp.Star) for e in select.expressions)


def _expand_star_with_schema(
    sql: str,
    parsed: exp.Expression,
    schema: dict[str, dict[str, str]],
    dialect: str | None,
    table_lookup: dict[str, str] | None = None,
) -> tuple[str, exp.Expression]:
    """Rewrite SELECT * to explicit column names using schema.

    Scans all source tables in the FROM/JOIN clauses and collects their
    known columns from schema. Returns (new_sql, new_parsed).
    Falls back to original (sql, parsed) if schema has no useful data.
    """
    # Gather columns from all referenced tables
    expanded_cols: list[str] = []
    seen: set[str] = set()
    for table in parsed.find_all(exp.Table):
        table_name = table.name.lower() if table.name else ""
        if not table_name:
            continue

        # Resolve to canonical model name via table_lookup so qualified
        # source names (e.g. "p4h.account_holder") are found in schema.
        canonical = table_name
        if table_lookup:
            catalog = table.catalog.lower() if table.catalog else ""
            db = table.db.lower() if table.db else ""
            if catalog and db:
                canonical = table_lookup.get(f"{catalog}.{db}.{table_name}", table_name)
            if canonical == table_name and db:
                canonical = table_lookup.get(f"{db}.{table_name}", table_name)
            if canonical == table_name:
                canonical = table_lookup.get(table_name, table_name)

        for col_name in schema.get(canonical, schema.get(canonical.lower(), {})):
            # Skip BigQuery RECORD sub-fields (e.g. "contact.active",
            # "avatar.attachment_url") — these are nested struct paths,
            # not top-level columns that belong in a SELECT * expansion.
            if "." in col_name:
                continue
            key = col_name.lower()
            if key not in seen:
                seen.add(key)
                expanded_cols.append(col_name)

    if not expanded_cols:
        return sql, parsed

    # Rewrite the AST: replace Star with explicit Column expressions
    try:
        new_parsed = parsed.copy()
        select_node = new_parsed if isinstance(new_parsed, exp.Select) else new_parsed.find(exp.Select)
        if select_node is None:
            return sql, parsed

        new_exprs = [exp.Column(this=exp.Identifier(this=c)) for c in expanded_cols]
        select_node.set("expressions", new_exprs)
        return new_parsed.sql(dialect=dialect), new_parsed
    except Exception:
        return sql, parsed


def _resolve_source_table(
    raw_node_name: str,
    alias_map: dict[str, str],
    table_lookup: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Parse a lineage node name into (source_table, source_column).

    Node names are "table_or_alias.column" or "db.schema.table.column".
    Uses rsplit on the last dot so qualified names work correctly.
    """
    if "." not in raw_node_name:
        return raw_node_name, raw_node_name

    raw_table, source_col = raw_node_name.rsplit(".", 1)

    # Resolve alias (e.g. 'o' → 'orders')
    resolved = alias_map.get(raw_table.lower(), raw_table)

    # Resolve qualified name (e.g. 'dev.public.orders' → 'orders')
    if table_lookup:
        resolved = table_lookup.get(resolved.lower(), resolved)

    return resolved, source_col


def _collect_transform_chain(
    root_node: object,
    dialect: str | None,
) -> list[dict]:
    """Walk all intermediate lineage nodes (CTE steps) and return the full
    transform chain from source → target.

    Each entry: {"step": str, "sql": str, "type": str}

    The walk order from SQLGlot is root → intermediates → leaf.
    We reverse it so the chain reads source → target (raw first).
    """
    chain: list[dict] = []

    for node in root_node.walk():  # type: ignore[attr-defined]
        node_expr = getattr(node, "expression", None)
        # Skip leaf nodes (exp.Table) — those are the source references, not transforms
        if isinstance(node_expr, exp.Table):
            continue
        if node_expr is None:
            continue

        t = classify_transform(node_expr)
        sql_str = node_expr.sql(dialect=dialect)
        chain.append({
            "step": getattr(node, "name", ""),
            "sql": sql_str,
            "type": str(t),
        })

    # walk() order is root-first; reverse for source-first ordering
    chain.reverse()
    return chain


def _pick_most_significant_type(chain: list[dict]) -> TransformType:
    """From a transform chain, return the highest-priority TransformType.

    This ensures that if a column goes through ARITHMETIC in a CTE and then
    PASSTHROUGH in the final SELECT, we report ARITHMETIC (not PASSTHROUGH).
    """
    best = TransformType.PASSTHROUGH
    best_priority = _TYPE_PRIORITY[best]

    for step in chain:
        try:
            t = TransformType(step["type"])
        except ValueError:
            continue
        p = _TYPE_PRIORITY.get(t, 0)
        if p > best_priority:
            best = t
            best_priority = p

    return best


def _collect_cte_transform(
    col_name: str,
    cte_ref: str,
    cte_map: dict[str, exp.Expression],
    dialect: str | None,
    depth: int = 0,
) -> tuple[TransformType, str]:
    """Walk the CTE chain for col_name and return the most significant TransformType.

    Recursively follows PASSTHROUGH hops into nested CTEs so that an aggregation
    or window function buried several CTE levels deep is surfaced as the primary type.
    """
    if depth > 10 or cte_ref not in cte_map:
        return TransformType.PASSTHROUGH, col_name

    col_lower = col_name.lower()
    best_type = TransformType.PASSTHROUGH
    best_sql = col_name

    for select in _flatten_union(cte_map[cte_ref]):
        for sel_expr in select.expressions:
            if sel_expr.alias_or_name.lower() != col_lower:
                continue
            inner = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr

            # Skip NULL / CAST(NULL AS ...) placeholder expressions.
            # In UNION ALL models it is common to write
            #   cast(null as float64) as amt_vas_discount
            # in branches that don't carry a real value.  Treating that as the
            # primary transform type (CAST) would be misleading — the real
            # transform is in the branch that actually provides the value.
            if inner is not None:
                is_null_literal = isinstance(inner, exp.Null)
                is_cast_null = (
                    isinstance(inner, (exp.Cast, exp.TryCast))
                    and isinstance(inner.this, exp.Null)
                )
                is_numeric_zero = (
                    isinstance(inner, exp.Literal) and inner.is_number
                    and inner.this in ("0", "0.0")
                )
                if is_null_literal or is_cast_null or is_numeric_zero:
                    continue  # Placeholder — don't let it pollute the transform type

            t = classify_transform(sel_expr)
            sql_str = inner.sql(dialect=dialect) if inner is not None else col_name

            if _TYPE_PRIORITY.get(t, 0) > _TYPE_PRIORITY.get(best_type, 0):
                best_type = t
                best_sql = sql_str

            # If this hop is a plain passthrough, recurse into the next CTE
            if t in (TransformType.PASSTHROUGH, TransformType.RENAME) and inner is not None:
                col_ref = inner if isinstance(inner, exp.Column) else None
                if col_ref is not None:
                    next_table = col_ref.table.lower() if col_ref.table else ""
                    next_col = col_ref.name if col_ref.name else col_name
                    if next_table and next_table in cte_map:
                        deeper_type, deeper_sql = _collect_cte_transform(
                            next_col, next_table, cte_map, dialect, depth + 1
                        )
                        if _TYPE_PRIORITY.get(deeper_type, 0) > _TYPE_PRIORITY.get(best_type, 0):
                            best_type = deeper_type
                            best_sql = deeper_sql

    return best_type, best_sql


def _trace_one_column(
    col_name: str,
    qualified_sql: str,
    schema: dict | None,
    dialect: str | None,
    alias_map: dict[str, str],
    table_lookup: dict[str, str] | None,
    model_name: str,
) -> tuple[list[ColumnEdge], bool]:
    """Trace lineage for a single output column.

    Returns (edges, success). success=False means SQLGlot raised an exception
    and the column should be counted as a failure.
    """
    try:
        root_node = sqlglot_lineage(
            column=col_name,
            sql=qualified_sql,
            schema=schema or {},
            dialect=dialect,
        )
    except Exception as e:
        logger.debug(f"[{model_name}] Lineage failed for '{col_name}': {e}")
        return [], False

    # Collect the full transform chain (all CTE/intermediate steps)
    chain = _collect_transform_chain(root_node, dialect)

    # Pick the most meaningful transform type from the full chain
    primary_type = _pick_most_significant_type(chain)

    # The "primary" SQL is the most significant step in the chain
    primary_sql = col_name  # fallback
    for step in chain:
        try:
            t = TransformType(step["type"])
        except ValueError:
            continue
        if _TYPE_PRIORITY.get(t, 0) == _TYPE_PRIORITY.get(primary_type, 0):
            primary_sql = step["sql"]
            break

    # Walk leaf nodes to find source tables
    edges: list[ColumnEdge] = []
    for lineage_node in root_node.walk():
        node_expr = getattr(lineage_node, "expression", None)
        if not isinstance(node_expr, exp.Table):
            continue

        raw_name = getattr(lineage_node, "name", "")
        if not raw_name or "." not in raw_name:
            continue

        source_table, source_col = _resolve_source_table(raw_name, alias_map, table_lookup)
        if not source_table or not source_col:
            continue

        edges.append(ColumnEdge(
            source=ColumnRef(model=source_table, column=source_col),
            target=ColumnRef(model=model_name, column=col_name),
            transform_sql=primary_sql,
            transform_type=primary_type,
            transform_chain=chain,
        ))

    # No source table found — check if the root expression is a pure literal
    # and emit a sentinel edge so the column appears in the lineage graph.
    if not edges:
        root_expr = getattr(root_node, "expression", None)
        if root_expr is not None and _is_literal_expr(root_expr):
            lit_sql = root_expr.sql(dialect=dialect) if dialect else root_expr.sql()
            edges.append(ColumnEdge(
                source=ColumnRef(model=_LITERAL_MODEL, column=lit_sql or col_name),
                target=ColumnRef(model=model_name, column=col_name),
                transform_sql=lit_sql or col_name,
                transform_type=TransformType.LITERAL,
                transform_chain=chain,
                resolution_status=ResolutionStatus.RESOLVED,
            ))

    return edges, True


def _build_cte_map(parsed: exp.Expression) -> dict[str, exp.Expression]:
    """Extract CTE definitions from a WITH clause.

    Returns a mapping of {cte_alias.lower(): cte_select_expression} so that
    later resolution steps can look up a CTE by its alias name.

    Skips "virtual" passthrough CTEs that sqlglot.qualify() injects for real
    tables when expanding stars.  These CTEs look like:
        dim_users AS (SELECT dim_users.col1, ... FROM dim_users AS dim_users)
    where the CTE name equals the single FROM-table name (possibly just the
    last segment of a fully-qualified path).  Treating them as real CTEs
    causes circular recursion (depth guard returns empty) and loses the real
    table attribution.
    """
    cte_map: dict[str, exp.Expression] = {}
    with_clause = parsed.find(exp.With)
    if with_clause is None:
        return cte_map
    for cte in with_clause.expressions:
        alias_name = (cte.alias or "").lower()
        if not alias_name:
            alias_node = cte.find(exp.TableAlias)
            alias_name = alias_node.name.lower() if alias_node and alias_node.name else ""
        if not alias_name:
            continue
        # The CTE body is the SELECT (or UNION) inside the CTE expression
        body = cte.this
        if body is None:
            continue

        # Skip virtual passthrough CTEs created by qualify(expand_stars=True).
        # qualify() injects these for real tables to help expand stars:
        #   dim_users AS (SELECT dim_users.id_user AS id_user, ... FROM dim_users AS dim_users)
        # The tell-tale signs are:
        #   1. Single FROM table whose last name segment matches the CTE alias
        #   2. Body has NO SELECT * — all columns are explicit (table.col AS col style)
        # Real wrapping CTEs like:
        #   top_breeders AS (SELECT * FROM `owp-dw-prod`.`core`.`top_breeders`)
        # use SELECT * so they are preserved.
        if isinstance(body, exp.Select):
            from_node = body.find(exp.From)
            if from_node:
                tables_in_from = list(from_node.find_all(exp.Table))
                if len(tables_in_from) == 1:
                    tbl = tables_in_from[0]
                    tbl_last = (tbl.name or "").lower()
                    if tbl_last == alias_name:
                        # Only skip if the body has NO bare SELECT * (i.e., qualify()
                        # fully expanded it into an explicit column list).
                        has_any_star = any(
                            isinstance(e, exp.Star) or
                            (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
                            for e in body.expressions
                        )
                        if not has_any_star:
                            # Virtual passthrough CTE — skip it
                            continue

        cte_map[alias_name] = body
    return cte_map


def _flatten_union(query: exp.Expression) -> list[exp.Select]:
    """Recursively flatten UNION / INTERSECT / EXCEPT into individual SELECTs.

    Returns [query] if it is already a plain SELECT.
    """
    if isinstance(query, exp.Select):
        return [query]
    if isinstance(query, (exp.Union, exp.Intersect, exp.Except)):
        left = _flatten_union(query.left) if query.left is not None else []
        right = _flatten_union(query.right) if query.right is not None else []
        return left + right
    # Fallback: try to extract any embedded SELECT
    sel = query.find(exp.Select)
    return [sel] if sel is not None else []


def _resolve_col_through_cte(
    cte_query: exp.Expression,
    col_name: str,
    cte_map: dict[str, exp.Expression],
    alias_map: dict[str, str],
    table_lookup: dict[str, str] | None,
    dialect: str | None,
    depth: int,
) -> list[tuple[str, str, ResolutionStatus, str | None]]:
    """Find what col_name maps to inside a CTE body (handles UNION ALL).

    Returns a list of (source_table, source_col, status, branch_sql) tuples
    gathered from every SELECT branch of the CTE (UNION ALL branches included).

    branch_sql is the SQL expression from the specific branch where this source
    was found (e.g. ``petpay_snapshot.amt_petpay_fee_incl_vat / 100.0``), or
    None for simple passthrough / SELECT * cases.

    Per-branch SelectScope
    ----------------------
    Each UNION branch gets its own scope built from _build_select_scope().
    This correctly handles:
    - Local aliases that shadow CTE names  (e.g. ``FROM nps_raw AS nps``)
    - Single-source unqualified column attribution
    - Multi-source ambiguity detection
    """
    results: list[tuple[str, str, ResolutionStatus, str | None]] = []
    col_lower = col_name.lower()

    for select in _flatten_union(cte_query):
        # Build a scope for this specific SELECT branch.
        # scope.relations overrides alias_map for names in this FROM/JOIN,
        # preventing outer-scope aliases from leaking in.
        branch_scope = _build_select_scope(select, alias_map, table_lookup)
        # Merge: local scope takes priority over global alias_map
        effective_alias_map = {**alias_map, **branch_scope.relations}

        found_explicit = False

        for sel_expr in select.expressions:
            expr_name = sel_expr.alias_or_name
            if not expr_name or expr_name.lower() != col_lower:
                continue
            found_explicit = True
            inner = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr

            # Compute per-branch SQL for this expression.  Only attach it when
            # the expression is non-trivial (not a bare column reference) so
            # that callers can use it instead of the global CTE transform SQL.
            is_passthrough = isinstance(inner, exp.Column)
            branch_sql: str | None = None
            if not is_passthrough and inner is not None:
                branch_sql = inner.sql(dialect=dialect) or None

            sources = _resolve_expr_sources(
                inner, cte_map, effective_alias_map, table_lookup,
                depth + 1, scope=branch_scope,
            )
            # Propagate branch_sql to each source from this branch
            for st, sc, ss, *rest in sources:
                inherited = rest[0] if rest else None
                results.append((st, sc, ss, inherited or branch_sql))
            # Pure literal — no column references found.  Return a sentinel so
            # callers know this branch has a constant value.
            if not sources and _is_literal_expr(inner if inner is not None else sel_expr):
                lit_sql = (inner or sel_expr).sql(dialect=dialect)
                results.append((_LITERAL_MODEL, lit_sql, ResolutionStatus.RESOLVED, lit_sql))

        # If no explicit match, check for SELECT * — the column may pass through
        # from a source that uses a bare star (e.g. SELECT *, extra_col FROM src).
        if not found_explicit and depth <= 15:
            has_star = any(
                isinstance(e, exp.Star) or
                (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
                for e in select.expressions
            )
            if has_star:
                for tbl in select.find_all(exp.Table):
                    tbl_name = tbl.name.lower() if tbl.name else ""
                    if not tbl_name:
                        continue
                    canonical = tbl_name
                    if table_lookup:
                        cat_part = tbl.catalog.lower() if tbl.catalog else ""
                        db_part = tbl.db.lower() if tbl.db else ""
                        if cat_part and db_part:
                            canonical = table_lookup.get(
                                f"{cat_part}.{db_part}.{tbl_name}", tbl_name
                            )
                        if canonical == tbl_name and db_part:
                            canonical = table_lookup.get(f"{db_part}.{tbl_name}", tbl_name)
                        if canonical == tbl_name:
                            canonical = table_lookup.get(tbl_name, tbl_name)
                    ref = canonical or tbl_name
                    if ref.lower() in cte_map:
                        results.extend(
                            _resolve_col_through_cte(
                                cte_map[ref.lower()], col_name, cte_map,
                                effective_alias_map, table_lookup, dialect, depth + 1,
                            )
                        )
                    else:
                        results.append((ref, col_name, ResolutionStatus.RESOLVED, None))

    return results


def _resolve_expr_sources(
    expr: exp.Expression,
    cte_map: dict[str, exp.Expression],
    alias_map: dict[str, str],
    table_lookup: dict[str, str] | None,
    depth: int = 0,
    scope: SelectScope | None = None,
) -> list[tuple[str, str, ResolutionStatus, str | None]]:
    """Walk expr's Column leaf nodes and resolve each to (source_table, source_col, status, branch_sql).

    branch_sql (4th element) carries the SQL expression from the specific CTE
    branch where this source was found, or None for direct table references.

    Resolution order per column node
    ---------------------------------
    1. Qualified (``tbl.col``): resolve via scope.relations → alias_map → table_lookup.
       If the resolved name is in cte_map, recurse into the CTE.
    2. Unqualified (``col`` with no table prefix):
       a. scope has exactly one source → attribute to that source (RESOLVED).
       b. scope has multiple sources → record AMBIGUOUS, skip.
       c. no scope → skip (UNRESOLVED).

    Guard: depth > 15 returns [] to prevent infinite recursion.
    """
    if depth > 15:
        return []

    results: list[tuple[str, str, ResolutionStatus, str | None]] = []

    # Collect Column nodes that only appear inside PARTITION BY / ORDER BY of
    # window functions.  These are grouping keys, not value contributors, so
    # we skip them to avoid spurious sources like id_vas_order feeding into
    # amt_discount just because it is used in OVER (PARTITION BY id_vas_order).
    window_partition_ids = _collect_window_partition_col_ids(expr)

    for col_node in expr.find_all(exp.Column):
        if id(col_node) in window_partition_ids:
            continue  # skip PARTITION BY / ORDER BY grouping keys
        col_table = col_node.table.lower() if col_node.table else ""
        col_name = col_node.name if col_node.name else ""
        if not col_name:
            continue

        # ── Unqualified column reference ──────────────────────────────────────
        if not col_table:
            if scope is not None and scope.single_source:
                # Single table in scope — unambiguous attribution
                real = scope.single_source
                if real.lower() in cte_map:
                    results.extend(_resolve_col_through_cte(
                        cte_map[real.lower()], col_name, cte_map, alias_map,
                        table_lookup, None, depth,
                    ))
                else:
                    if table_lookup:
                        real = table_lookup.get(real.lower(), real)
                    results.append((real, col_name, ResolutionStatus.RESOLVED, None))
            elif scope is not None and scope.relations:
                # Multiple tables in scope — ambiguous
                results.append(("", col_name, ResolutionStatus.AMBIGUOUS, None))
            # else: no scope info → skip silently
            continue

        # ── Qualified column reference ────────────────────────────────────────
        # Use scope.relations first (most local), then fall back to alias_map.
        # scope.relations is built from the current SELECT's FROM/JOIN, so it
        # correctly overrides any stale outer-scope alias_map entries.
        if scope is not None and col_table in scope.relations:
            canonical = scope.relations[col_table]
        else:
            canonical = alias_map.get(col_table, col_table)

        canonical_lower = canonical.lower()

        if canonical_lower in cte_map:
            results.extend(_resolve_col_through_cte(
                cte_map[canonical_lower], col_name, cte_map, alias_map,
                table_lookup, None, depth,
            ))
        else:
            # Check alias_map again in case scope didn't resolve it
            if canonical_lower not in (scope.relations if scope else {}):
                via_alias = alias_map.get(col_table, col_table)
                if via_alias.lower() != col_table and via_alias.lower() in cte_map:
                    results.extend(_resolve_col_through_cte(
                        cte_map[via_alias.lower()], col_name, cte_map, alias_map,
                        table_lookup, None, depth,
                    ))
                    continue
            # Real table — resolve qualified name to short model name
            resolved = canonical
            if table_lookup and resolved:
                resolved = table_lookup.get(resolved.lower(), resolved)
            if resolved:
                results.append((resolved, col_name, ResolutionStatus.RESOLVED, None))

    return results


def _single_pass_analyze_ast(
    parsed: exp.Expression,
    model_name: str,
    dialect: str | None,
    alias_map: dict[str, str],
    table_lookup: dict[str, str] | None,
) -> list[ColumnEdge] | None:
    """Extract all column lineage from an already-parsed AST in a single traversal.

    Accepts a pre-parsed (and optionally qualified) AST to avoid redundant
    parse_one() calls. Returns None on exception so the caller can fall back.
    """
    try:
        cte_map = _build_cte_map(parsed)

        # Find the outermost SELECT(s) — could be a UNION at the top level
        # The outermost query is whatever is outside any WITH clause
        outer: exp.Expression = parsed
        # If the top-level expression is a With, get its body
        if isinstance(outer, exp.With):
            outer = outer.this  # type: ignore[assignment]
        elif hasattr(outer, "this") and isinstance(outer.find(exp.With), exp.With):
            # parsed is e.g. a Select that *contains* a With; outermost selects are correct
            pass

        final_selects = _flatten_union(outer)
        if not final_selects:
            return None

        edges: list[ColumnEdge] = []

        for select in final_selects:
            for sel_expr in select.expressions:
                col_name = sel_expr.alias_or_name
                if not col_name or col_name == "*":
                    continue

                inner = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr

                transform_type = classify_transform(sel_expr)
                transform_sql = inner.sql(dialect=dialect) if inner is not None else col_name

                # If the outer expression is a passthrough, look into CTE hops to find
                # the real transform type (e.g. AGGREGATION buried inside a CTE).
                if transform_type in (TransformType.PASSTHROUGH, TransformType.RENAME):
                    col_ref = (
                        inner if isinstance(inner, exp.Column)
                        else (inner.find(exp.Column) if inner is not None else None)
                    )
                    if col_ref is not None:
                        col_table = col_ref.table.lower() if col_ref.table else ""
                        col_inner_name = col_ref.name if col_ref.name else col_name
                        # Resolve col_table through alias_map in case it's a CTE alias
                        cte_target = col_table
                        if col_table and col_table not in cte_map:
                            resolved_tbl = alias_map.get(col_table, col_table)
                            if resolved_tbl.lower() in cte_map:
                                cte_target = resolved_tbl.lower()
                        if cte_target and cte_target in cte_map:
                            deeper_type, deeper_sql = _collect_cte_transform(
                                col_inner_name, cte_target, cte_map, dialect
                            )
                            if _TYPE_PRIORITY.get(deeper_type, 0) > _TYPE_PRIORITY.get(transform_type, 0):
                                transform_type = deeper_type
                                transform_sql = deeper_sql

                # Build a scope for this specific SELECT branch so that
                # unqualified column references can be attributed correctly.
                outer_scope = _build_select_scope(select, alias_map, table_lookup)

                sources = _resolve_expr_sources(
                    inner if inner is not None else sel_expr,
                    cte_map,
                    alias_map,
                    table_lookup,
                    depth=0,
                    scope=outer_scope,
                )

                for source_table, source_col, res_status, branch_sql in sources:
                    if not source_table:
                        # AMBIGUOUS — skip inserting a misleading edge
                        continue
                    # Literal sentinel: override transform type and SQL so the
                    # edge reflects the constant value, not a passthrough hop.
                    if source_table == _LITERAL_MODEL:
                        edges.append(ColumnEdge(
                            source=ColumnRef(model=_LITERAL_MODEL, column=source_col),
                            target=ColumnRef(model=model_name, column=col_name),
                            transform_sql=source_col,
                            transform_type=TransformType.LITERAL,
                            transform_chain=[],
                            resolution_status=res_status,
                        ))
                        continue
                    # Use per-branch SQL when available (non-trivial CTE expression).
                    # This ensures each UNION ALL branch shows its own formula rather
                    # than the global transform SQL picked by _collect_cte_transform.
                    if branch_sql:
                        edge_sql = branch_sql
                        # Re-classify using the branch-specific expression SQL so the
                        # transform type is accurate for this particular source.
                        try:
                            branch_expr = sqlglot.parse_one(branch_sql, dialect=dialect)
                            branch_type = classify_transform(branch_expr)
                            edge_type = (branch_type
                                         if branch_type not in (TransformType.PASSTHROUGH, TransformType.UNKNOWN)
                                         else transform_type)
                        except Exception:
                            edge_type = transform_type
                    else:
                        # No branch-specific SQL — this is a passthrough/rename in this
                        # specific branch.  Avoid inheriting a formula from a different
                        # UNION branch (which _collect_cte_transform may have picked).
                        # Use the source column itself as the SQL.
                        edge_sql = source_col
                        edge_type = (TransformType.RENAME
                                     if source_col.lower() != col_name.lower()
                                     else TransformType.PASSTHROUGH)
                    edges.append(ColumnEdge(
                        source=ColumnRef(model=source_table, column=source_col),
                        target=ColumnRef(model=model_name, column=col_name),
                        transform_sql=edge_sql,
                        transform_type=edge_type,
                        transform_chain=[],
                        resolution_status=res_status,
                    ))

                # No source found — emit a literal sentinel edge so the column
                # is still visible in the lineage graph with its SQL value.
                if not sources and _is_literal_expr(inner if inner is not None else sel_expr):
                    edges.append(ColumnEdge(
                        source=ColumnRef(model=_LITERAL_MODEL, column=transform_sql),
                        target=ColumnRef(model=model_name, column=col_name),
                        transform_sql=transform_sql,
                        transform_type=TransformType.LITERAL,
                        transform_chain=[],
                        resolution_status=ResolutionStatus.RESOLVED,
                    ))

        return edges

    except Exception as exc:
        logger.debug(f"[{model_name}] Single-pass analysis failed: {exc}")
        return None


def _single_pass_analyze(
    qualified_sql: str,
    model_name: str,
    dialect: str | None,
    alias_map: dict[str, str],
    table_lookup: dict[str, str] | None,
) -> list[ColumnEdge] | None:
    """Backward-compat wrapper — parses SQL then delegates to _single_pass_analyze_ast."""
    try:
        parsed = sqlglot.parse_one(qualified_sql, dialect=dialect)
    except Exception:
        return None
    if parsed is None:
        return None
    return _single_pass_analyze_ast(parsed, model_name, dialect, alias_map, table_lookup)


def _try_passthrough_select_star(
    sql: str,
    model_name: str,
    schema: dict[str, dict[str, str]] | None,
    dialect: str | None,
    table_lookup: dict[str, str] | None,
) -> list[ColumnEdge] | None:
    """Backward-compat wrapper — parses SQL then delegates to _try_passthrough_select_star_ast."""
    if not schema:
        return None
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        return None
    return _try_passthrough_select_star_ast(parsed, model_name, schema, dialect, table_lookup)


def _try_passthrough_select_star_ast(
    parsed: exp.Expression,
    model_name: str,
    schema: dict[str, dict[str, str]] | None,
    dialect: str | None,
    table_lookup: dict[str, str] | None,
) -> list[ColumnEdge] | None:
    """Fast path for SELECT * FROM single_source (no JOINs, no CTEs).

    Accepts a pre-parsed AST — avoids a redundant parse_one() call.
    Returns None if the SQL doesn't match the simple pattern.
    """
    if not schema:
        return None
    if not isinstance(parsed, exp.Select):
        return None
    # Must be SELECT * only (no aliases, no expressions alongside the star)
    if not parsed.expressions or not all(isinstance(e, exp.Star) for e in parsed.expressions):
        return None
    # No CTEs or JOINs — must be a simple single-table scan
    if parsed.find(exp.With) or parsed.find(exp.Join):
        return None
    tables = list(parsed.find_all(exp.Table))
    if len(tables) != 1:
        return None

    table = tables[0]
    table_name = table.name.lower() if table.name else ""
    if not table_name:
        return None

    # Resolve to canonical model name via table_lookup
    canonical = table_name
    if table_lookup:
        catalog_part = table.catalog.lower() if table.catalog else ""
        db_part = table.db.lower() if table.db else ""
        if catalog_part and db_part:
            canonical = table_lookup.get(f"{catalog_part}.{db_part}.{table_name}", table_name)
        if canonical == table_name and db_part:
            canonical = table_lookup.get(f"{db_part}.{table_name}", table_name)
        if canonical == table_name:
            canonical = table_lookup.get(table_name, table_name)

    cols = schema.get(canonical) or schema.get(canonical.lower()) or {}
    if not cols:
        return None

    return [
        ColumnEdge(
            source=ColumnRef(model=canonical, column=col),
            target=ColumnRef(model=model_name, column=col),
            transform_sql=col,
            transform_type=TransformType.PASSTHROUGH,
            transform_chain=[],
        )
        for col in cols
        if "." not in col  # skip BigQuery RECORD sub-fields
    ]


def _find_single_upstream_table(
    parsed: exp.Expression,
    cte_map: dict[str, exp.Expression],
    table_lookup: dict[str, str] | None,
) -> str | None:
    """Return the single real (non-CTE) source table referenced in the SQL.

    Used as a last-resort fallback when output column detection fails but the
    model is clearly a single-source wrapper — for example:

    - BigQuery array_agg deduplication:
        SELECT unique.*
        FROM (SELECT array_agg(r ORDER BY v DESC LIMIT 1)[OFFSET(0)] AS unique
              FROM source_table r  GROUP BY id)

    - QUALIFY-based dedup (Snowflake / BigQuery):
        SELECT * FROM source_table
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

    - SELECT AS STRUCT expansion, ANY_VALUE wrappers, and similar patterns
      where the wrapper does not add, remove, or rename columns.

    Returns the canonical table name if exactly one real (non-CTE) table is
    found anywhere in the AST, None if zero or multiple real tables appear
    (e.g. a JOIN would produce two tables → fallback is not applied).
    """
    cte_names = set(cte_map.keys())
    # All known canonical model/source names — used to reject struct aliases
    # (e.g. 'unique' in BigQuery array_agg patterns) that SQLGlot may parse as
    # Table nodes even though they are not real tables in the DAG.
    known_models: set[str] = set(table_lookup.values()) if table_lookup else set()
    real_tables: set[str] = set()

    for table in parsed.find_all(exp.Table):
        name = table.name.lower() if table.name else ""
        if not name or name in cte_names:
            continue
        canonical = name
        if table_lookup:
            cat = table.catalog.lower() if table.catalog else ""
            db = table.db.lower() if table.db else ""
            if cat and db:
                canonical = table_lookup.get(f"{cat}.{db}.{name}", name)
            if canonical == name and db:
                canonical = table_lookup.get(f"{db}.{name}", name)
            if canonical == name:
                canonical = table_lookup.get(name, name)
        # When table_lookup is available, only accept tables that resolve to a
        # known model/source.  This rejects struct column aliases that SQLGlot
        # parses as table references (e.g. `unique` in `SELECT unique.*`).
        if known_models and canonical not in known_models:
            continue
        real_tables.add(canonical)

    return next(iter(real_tables)) if len(real_tables) == 1 else None


def _infer_cte_output_columns(
    cte_name: str,
    cte_map: dict[str, exp.Expression],
    schema: dict[str, dict[str, str]] | None,
    table_lookup: dict[str, str] | None,
    dialect: str | None,
    depth: int = 0,
) -> list[str]:
    """Infer the output column names of a CTE without requiring the full schema.

    Handles all common dbt patterns:

    - Explicit column list:
        joined AS (SELECT a.col1, b.col2 AS renamed FROM a LEFT JOIN b ON ...)
        → ['col1', 'renamed']

    - SELECT * from a real table — looks up schema:
        src AS (SELECT * FROM raw_table)
        → all columns from schema['raw_table']

    - SELECT * from another CTE — recurses:
        stage AS (SELECT * FROM src)
        → same columns as src

    - SELECT * EXCEPT(col1, col2) — BigQuery/Snowflake:
        renamed AS (SELECT * EXCEPT(_sdc_seq) FROM src)
        → src columns minus _sdc_seq

    - Qualified star  SELECT t.* [EXCEPT(...)]:
        dims AS (SELECT joined.* EXCEPT(id_market), dim.id_listing FROM joined JOIN dim)
        → joined columns minus id_market, plus id_listing

    - UNION ALL — uses first branch as representative:
        unioned AS (SELECT * FROM a UNION ALL SELECT * FROM b)
        → columns of a

    Returns [] if inference fails (depth exceeded, unknown CTE, empty schema).
    """
    if depth > 8 or cte_name not in cte_map:
        return []

    cte_body = cte_map[cte_name]
    selects = _flatten_union(cte_body)
    if not selects:
        return []

    # Use first UNION branch — all branches must share the same column names
    select = selects[0]

    # Build scope so qualified references (e.g. t.col) resolve to canonical names
    scope = _build_select_scope(select, {}, table_lookup)

    def _resolve_tbl(tbl: exp.Table) -> str:
        name = tbl.name.lower() if tbl.name else ""
        if not name:
            return ""
        canonical = name
        if table_lookup:
            cat = tbl.catalog.lower() if tbl.catalog else ""
            db = tbl.db.lower() if tbl.db else ""
            if cat and db:
                canonical = table_lookup.get(f"{cat}.{db}.{name}", name)
            if canonical == name and db:
                canonical = table_lookup.get(f"{db}.{name}", name)
            if canonical == name:
                canonical = table_lookup.get(name, name)
        return canonical

    def _except_set(star_node: exp.Star) -> set[str]:
        exc = star_node.args.get("except") or []
        return {
            (e.name if hasattr(e, "name") else "").lower()
            for e in exc
            if e is not None
        }

    def _cols_for(source: str, skip: set[str]) -> list[str]:
        """Return columns for source (CTE or real table), excluding skip set."""
        src_lower = source.lower()
        if src_lower in cte_map:
            sub = _infer_cte_output_columns(
                src_lower, cte_map, schema, table_lookup, dialect, depth + 1
            )
            return [c for c in sub if c.lower() not in skip]
        if schema:
            raw = schema.get(source) or schema.get(src_lower) or {}
            return [c for c in raw if "." not in c and c.lower() not in skip]
        return []

    output: list[str] = []
    seen: set[str] = set()

    def _add(col: str) -> None:
        if col and col not in seen and "." not in col:
            seen.add(col)
            output.append(col)

    for expr in select.expressions:
        alias_name = expr.alias_or_name

        # ── Named column (explicit alias or bare column reference) ────────────
        if alias_name and alias_name != "*":
            _add(alias_name)
            continue

        inner = expr.this if isinstance(expr, exp.Alias) else expr

        # ── Bare star: SELECT * [EXCEPT(...)] ─────────────────────────────────
        if isinstance(inner, exp.Star):
            skip = _except_set(inner)
            from_node = select.find(exp.From)
            if from_node:
                for tbl in from_node.find_all(exp.Table):
                    canonical = _resolve_tbl(tbl)
                    if not canonical:
                        continue
                    for c in _cols_for(canonical, skip):
                        _add(c)
            continue

        # ── Qualified star: SELECT t.* [EXCEPT(...)] ──────────────────────────
        if isinstance(inner, exp.Column) and isinstance(inner.this, exp.Star):
            qualifier = inner.table.lower() if inner.table else ""
            skip = _except_set(inner.this)
            # Resolve alias → canonical name via scope
            canonical_tbl = scope.relations.get(qualifier, qualifier)
            if table_lookup:
                canonical_tbl = table_lookup.get(canonical_tbl.lower(), canonical_tbl)
            for c in _cols_for(canonical_tbl, skip):
                _add(c)
            continue

    return output


def _remap_unresolvable_sources(
    edges: list[ColumnEdge],
    parsed: exp.Expression,
    table_lookup: dict[str, str] | None,
    schema: dict[str, dict[str, str]] | None,
    model_name: str,
) -> list[ColumnEdge]:
    """Replace edges whose source model is an unresolvable alias with the real source.

    Dialect-specific wrappers — BigQuery ``array_agg`` dedup, ``QUALIFY``
    filters, ``SELECT AS STRUCT`` expansion — cause ``sqlglot_lineage()`` to
    attribute columns to an intermediate alias (e.g. ``unique``, ``_0``) that
    is not a real model in the DAG.

    When **all** source models in *edges* are unknown and the SQL contains
    exactly one real source table whose columns are a superset of the output
    columns, every source is replaced with a passthrough edge from that table.

    Three conditions must all be true before the remap fires:
    1. All source models resolve to names absent from ``table_lookup.values()``.
    2. ``_find_single_upstream_table`` identifies exactly one real (non-CTE) table.
    3. Every output column name exists in that table's schema entry (passthrough
       quality gate — rejects models that add or rename columns).
    """
    if not edges or not table_lookup or not schema:
        return edges

    known_models = set(table_lookup.values())
    if not all(e.source.model not in known_models for e in edges):
        return edges  # at least one source is known — nothing to fix

    cte_map = _build_cte_map(parsed)
    real_src = _find_single_upstream_table(parsed, cte_map, table_lookup)
    if not real_src:
        return edges

    src_cols = {
        c.lower()
        for c in (schema.get(real_src) or schema.get(real_src.lower()) or {})
        if "." not in c
    }
    tgt_cols = {e.target.column.lower() for e in edges}
    if not src_cols or not tgt_cols.issubset(src_cols):
        return edges  # output columns don't match source — not a passthrough wrapper

    remapped = [
        ColumnEdge(
            source=ColumnRef(model=real_src, column=e.target.column),
            target=e.target,
            transform_sql=e.target.column,
            transform_type=TransformType.PASSTHROUGH,
            transform_chain=[],
            resolution_status=ResolutionStatus.RESOLVED,
        )
        for e in edges
    ]
    logger.debug(
        f"[{model_name}] Remapped {len(remapped)} edges from unresolvable "
        f"alias -> '{real_src}' (single-source passthrough)"
    )
    return remapped


def analyze_model_columns(
    compiled_sql: str,
    model_name: str,
    schema: dict[str, dict[str, str]] | None = None,
    dialect: str | None = None,
    table_lookup: dict[str, str] | None = None,
    parallelize_columns: bool = True,
    catalog_model_names: frozenset[str] | None = None,
) -> ModelAnalysisResult:
    """Analyze a single model's SQL and return all column lineage edges.

    Handles:
    - Simple SELECT, renames, casts, arithmetic
    - SELECT * — expanded to explicit columns when schema is provided
    - CTEs (including multi-step CTE chains — each step captured in transform_chain)
    - JOINs with table aliases
    - Window and aggregate functions
    - UNION ALL (multiple source edges per output column)

    Args:
        compiled_sql:  Fully compiled SQL (Jinja resolved).
        model_name:    Short model name — used as the target side of every edge.
        schema:        Optional schema dict {"table": {"col": "type"}} for qualify step.
        dialect:       SQLGlot dialect string (e.g. "snowflake", "bigquery").
        table_lookup:  Optional qualified-name → short-name mapping.

    Returns:
        ModelAnalysisResult with edges, coverage stats, and any failed column names.
    """
    if not compiled_sql or not compiled_sql.strip():
        return ModelAnalysisResult(edges=[])

    # ── Reject uncompiled Jinja templates ─────────────────────────────────────
    # dbt's manifest falls back to raw_sql when compiled_sql is absent.
    # Raw SQL contains {{ macro_call() }} and {% if ... %} blocks that SQLGlot
    # cannot parse — and attempting to qualify() them causes 30-60 s hangs.
    # Detect early and bail out in microseconds instead.
    if "{{" in compiled_sql or "{%" in compiled_sql:
        logger.debug(f"[{model_name}] Skipping — SQL contains unresolved Jinja templates.")
        return ModelAnalysisResult(edges=[])

    # ── Parse once ────────────────────────────────────────────────────────────
    # All subsequent steps reuse this AST — no redundant parse_one() calls.
    try:
        parsed = sqlglot.parse_one(compiled_sql, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        logger.warning(f"[{model_name}] Failed to parse SQL: {e}")
        return ModelAnalysisResult(edges=[])

    if parsed is None:
        return ModelAnalysisResult(edges=[])

    # ── Fast path: SELECT * FROM single_table ────────────────────────────────
    # Catches all base_* models (simple passthrough of every source column).
    # Avoids per-column SQLGlot lineage calls — generates edges directly.
    fast_edges = _try_passthrough_select_star_ast(parsed, model_name, schema, dialect, table_lookup)
    if fast_edges is not None:
        n = len(fast_edges)
        return ModelAnalysisResult(edges=fast_edges, columns_attempted=n, columns_traced=n)

    alias_map = _build_alias_map(parsed)

    # ── SELECT * expansion ────────────────────────────────────────────────────
    # If the outermost SELECT is a bare *, expand it to explicit columns now,
    # before the qualify step, so sqlglot_lineage can trace each column.
    #
    # Skip when the outermost FROM references a CTE name.
    # _expand_star_with_schema scans ALL tables in the AST (including inner CTE
    # bodies) and uses their columns to expand the outer *.  When the outer
    # SELECT reads from a CTE that renames columns (e.g. "SELECT * FROM final"
    # where "final" renames stg model columns), this injects pre-rename column
    # names into the outer SELECT which breaks analysis.
    # qualify(expand_stars=True) is CTE-scope-aware and handles these correctly.
    _cte_names_star: set[str] = set()
    _with_star = parsed.find(exp.With)
    if _with_star:
        for _cte in _with_star.expressions:
            _alias_node = _cte.find(exp.TableAlias)
            if _alias_node and _alias_node.name:
                _cte_names_star.add(_alias_node.name.lower())

    _outer_sel = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
    _outer_from_is_cte = False
    _outer_cte_ref: str | None = None
    if _outer_sel and _cte_names_star:
        _from_node = _outer_sel.find(exp.From)
        if _from_node:
            for _tbl in _from_node.find_all(exp.Table):
                if _tbl.name and _tbl.name.lower() in _cte_names_star:
                    _outer_from_is_cte = True
                    _outer_cte_ref = _tbl.name.lower()
                    break

    if _has_select_star(parsed) and schema and not _outer_from_is_cte:
        compiled_sql, parsed = _expand_star_with_schema(
            compiled_sql, parsed, schema, dialect, table_lookup
        )
        alias_map = _build_alias_map(parsed)

    # ── Qualify (attach table prefixes for unambiguous column attribution) ────
    # Only pass schema entries for tables actually referenced in this SQL.
    # Passing the full schema (840+ tables) causes qualify() to scan every entry
    # for every column — taking ~1.5s per model regardless of SQL size.
    # Filtering to referenced tables only drops that to ~2ms.
    qualified_ast = parsed
    local_schema: dict[str, dict[str, str]] = {}
    _trace_schema: dict[str, dict[str, str]] | None = None
    if schema:
        referenced_tables = {
            table.name.lower()
            for table in parsed.find_all(exp.Table)
            if table.name
        }
        # Filter to referenced tables and strip BigQuery RECORD sub-fields
        # (dot-notation like "contact.active") so qualify(expand_stars=True)
        # doesn't inject struct paths as top-level SELECT columns.
        local_schema = {
            k: {cn: ct for cn, ct in v.items() if "." not in cn}
            for k, v in schema.items()
            if k.lower() in referenced_tables
        }
        # Trace schema: use all referenced-table schemas (catalog + promoted).
        # Catalog-only tracing excluded models like stg_owp__nvas_payment which
        # caused SQLGlot to produce wrong attributions (id_market → amt_vas_discount)
        # because it couldn't resolve SELECT * EXCEPT (...) without the schema.
        # Partial promoted schemas can cause missed edges but not wrong edges:
        # if a column is genuinely missing, sqlglot_lineage returns UNRESOLVED,
        # not a wrong source column.
        _trace_schema = local_schema or None
        try:
            qualified_ast = qualify(
                parsed.copy(),
                schema=local_schema,
                dialect=dialect,
                validate_qualify_columns=False,
                identify=False,
                expand_stars=True,   # expand any remaining SELECT * using schema
            )
            alias_map = _build_alias_map(qualified_ast)
        except Exception as e:
            logger.debug(f"[{model_name}] Qualify step failed (continuing without): {e}")
            # Retry qualify with expand_stars=False — promoted-model schemas are
            # often incomplete (missing columns not captured by lineage), which
            # causes qualify(expand_stars=True) to fail.  Disabling star expansion
            # lets qualify() still attach table prefixes to explicit column refs,
            # which is all _single_pass_analyze_ast needs.
            try:
                qualified_ast = qualify(
                    parsed.copy(),
                    schema=local_schema,
                    dialect=dialect,
                    validate_qualify_columns=False,
                    identify=False,
                    expand_stars=False,
                )
                alias_map = _build_alias_map(qualified_ast)
                logger.debug(f"[{model_name}] Qualify retry (no star expansion) succeeded.")
            except Exception:
                pass

    # ── Single-pass analysis ──────────────────────────────────────────────────
    # Serialize the qualified AST back to SQL once and re-parse into a clean
    # AST for single-pass analysis. qualify() leaves internal state on the AST
    # that can break CTE map detection; a fresh parse_one() avoids that.
    try:
        qualified_sql = qualified_ast.sql(dialect=dialect)
        clean_ast = sqlglot.parse_one(qualified_sql, dialect=dialect) or qualified_ast
    except Exception:
        qualified_sql = compiled_sql
        clean_ast = qualified_ast

    sp_edges = _single_pass_analyze_ast(clean_ast, model_name, dialect, alias_map, table_lookup)

    if sp_edges is not None and len(sp_edges) > 0:
        sp_edges = _remap_unresolvable_sources(sp_edges, parsed, table_lookup, schema, model_name)

        # Deduplicate
        seen: set[tuple] = set()
        edges: list[ColumnEdge] = []
        for e in sp_edges:
            key = (e.source.model, e.source.column, model_name, e.target.column)
            if key not in seen:
                seen.add(key)
                edges.append(e)

        # Count columns attempted/traced from unique target columns
        target_cols = {e.target.column for e in edges}
        n = len(target_cols)

        # Collect ambiguous/unresolved columns for diagnostics
        ambiguous = list({
            e.target.column for e in edges
            if e.resolution_status == ResolutionStatus.AMBIGUOUS
        })
        unresolved = list({
            e.target.column for e in edges
            if e.resolution_status == ResolutionStatus.UNRESOLVED
        })
        return ModelAnalysisResult(
            edges=edges,
            columns_attempted=n,
            columns_traced=n,
            ambiguous_columns=ambiguous,
            unresolved_columns=unresolved,
        )

    # ── Fallback: per-column sqlglot_lineage() ────────────────────────────────
    # Used when single-pass couldn't resolve (complex subqueries, etc.)
    logger.debug(f"[{model_name}] Single-pass yielded no edges — using per-column fallback.")

    # Reuse the clean AST for output-column extraction.
    output_columns = _get_output_columns(clean_ast)

    # Cap column count to avoid runaway analysis on very wide models
    if len(output_columns) > _MAX_COLUMNS_PER_MODEL:
        logger.debug(
            f"[{model_name}] Capping at {_MAX_COLUMNS_PER_MODEL}/{len(output_columns)} columns "
            f"(set DBT_LINEAGE_MAX_COLUMNS env var to raise the limit)"
        )
        output_columns = output_columns[:_MAX_COLUMNS_PER_MODEL]

    if not output_columns and _outer_from_is_cte and _outer_cte_ref:
        # ── CTE output column inference ───────────────────────────────────────
        # The outer SELECT is SELECT * FROM <cte_name>.  Instead of giving up,
        # parse the CTE's own SELECT clause to extract its output column names.
        # This resolves all models that end with SELECT * FROM final_cte where
        # the final CTE has explicit columns (JOINs, * EXCEPT, transforms, etc.)
        _cte_map_infer = _build_cte_map(parsed)
        _inferred = _infer_cte_output_columns(
            _outer_cte_ref, _cte_map_infer, schema, table_lookup, dialect
        )
        if _inferred:
            output_columns = _inferred
            logger.debug(
                f"[{model_name}] CTE inference: {len(output_columns)} columns "
                f"inferred from '{_outer_cte_ref}'"
            )

    if not output_columns:
        # ── Last resort: single-source passthrough ────────────────────────────
        # When the outermost SELECT is SELECT * FROM <cte> and the entire CTE
        # chain wraps exactly one real source table (dedup, struct expansion,
        # QUALIFY filter, ANY_VALUE wrapper, etc.), treat the model as a
        # column-preserving passthrough of that source table.
        #
        # Trigger conditions (all must be true):
        #   1. schema is available (quality gate — we need known column names)
        #   2. Exactly one non-CTE table appears anywhere in the SQL
        #   3. That table has columns in schema
        #
        # This handles patterns that SQLGlot cannot parse on its own, such as
        # the BigQuery array_agg dedup:
        #
        #   WITH src AS (SELECT * FROM `project`.`schema`.`table`),
        #        deduped AS (
        #            SELECT unique.*
        #            FROM (SELECT array_agg(r ORDER BY v DESC LIMIT 1)[OFFSET(0)]
        #                         AS unique
        #                  FROM src r  GROUP BY id)
        #        )
        #   SELECT * FROM deduped
        #
        # — and any other single-source wrapper regardless of SQL dialect.
        if schema:
            _cte_map_fb = _build_cte_map(parsed)
            _single_src = _find_single_upstream_table(parsed, _cte_map_fb, table_lookup)
            if _single_src:
                _src_cols = schema.get(_single_src) or schema.get(_single_src.lower()) or {}
                _fb_edges = [
                    ColumnEdge(
                        source=ColumnRef(model=_single_src, column=col),
                        target=ColumnRef(model=model_name, column=col),
                        transform_sql=col,
                        transform_type=TransformType.PASSTHROUGH,
                        transform_chain=[],
                        resolution_status=ResolutionStatus.RESOLVED,
                    )
                    for col in _src_cols
                    if "." not in col  # skip BigQuery RECORD sub-fields
                ]
                if _fb_edges:
                    logger.debug(
                        f"[{model_name}] Single-source passthrough fallback: "
                        f"{len(_fb_edges)} edges from '{_single_src}'"
                    )
                    _n = len(_fb_edges)
                    return ModelAnalysisResult(
                        edges=_fb_edges, columns_attempted=_n, columns_traced=_n
                    )

        logger.debug(
            f"[{model_name}] No output columns detected — likely bare SELECT * with no "
            "schema available. Provide catalog.json or document columns in dbt YAML."
        )
        return ModelAnalysisResult(edges=[])

    # ── Trace lineage per output column ──────────────────────────────────────
    # parallelize_columns=False when called from a worker process — avoids
    # spawning threads inside an already-parallel process pool.
    columns_attempted = len(output_columns)
    columns_traced = 0
    failed_columns: list[str] = []
    raw_edges: list[ColumnEdge] = []

    workers = min(_MAX_WORKERS, columns_attempted) if parallelize_columns else 1

    if workers <= 1:
        for col_name in output_columns:
            col_edges, success = _trace_one_column(
                col_name, qualified_sql, _trace_schema, dialect, alias_map, table_lookup, model_name
            )
            if not success:
                failed_columns.append(col_name)
            elif col_edges:
                columns_traced += 1
                raw_edges.extend(col_edges)
            else:
                columns_traced += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _trace_one_column,
                    col_name, qualified_sql, _trace_schema, dialect, alias_map, table_lookup, model_name,
                ): col_name
                for col_name in output_columns
            }
            for future in as_completed(futures):
                col_name = futures[future]
                try:
                    col_edges, success = future.result()
                except Exception as e:
                    logger.debug(f"[{model_name}] Thread error for '{col_name}': {e}")
                    failed_columns.append(col_name)
                    continue

                if not success:
                    failed_columns.append(col_name)
                elif col_edges:
                    columns_traced += 1
                    raw_edges.extend(col_edges)
                else:
                    columns_traced += 1

    raw_edges = _remap_unresolvable_sources(raw_edges, parsed, table_lookup, schema, model_name)

    # ── Deduplicate edges ─────────────────────────────────────────────────────
    seen_edges: set[tuple[str, str, str, str]] = set()
    edges: list[ColumnEdge] = []
    for e in raw_edges:
        key = (e.source.model, e.source.column, model_name, e.target.column)
        if key not in seen_edges:
            seen_edges.add(key)
            edges.append(e)

    # ── All-column-fail recovery: single-source passthrough ───────────────────
    # If CTE inference provided output_columns but sqlglot_lineage() failed for
    # ALL of them (complex SQL that qualify()/lineage() can't handle), fall back
    # to the single-source passthrough — same result as if output_columns had
    # been empty.  Only fires when all columns failed (partial success is kept).
    if not edges and columns_attempted > 0 and len(failed_columns) == columns_attempted and schema:
        _cte_map_fb = _build_cte_map(parsed)
        _single_src = _find_single_upstream_table(parsed, _cte_map_fb, table_lookup)
        if _single_src:
            _src_cols = schema.get(_single_src) or schema.get(_single_src.lower()) or {}
            _fb_edges = [
                ColumnEdge(
                    source=ColumnRef(model=_single_src, column=col),
                    target=ColumnRef(model=model_name, column=col),
                    transform_sql=col,
                    transform_type=TransformType.PASSTHROUGH,
                    transform_chain=[],
                    resolution_status=ResolutionStatus.RESOLVED,
                )
                for col in _src_cols
                if "." not in col
            ]
            if _fb_edges:
                logger.debug(
                    f"[{model_name}] All-column-fail recovery: single-source passthrough "
                    f"from '{_single_src}' ({len(_fb_edges)} edges)"
                )
                _n = len(_fb_edges)
                return ModelAnalysisResult(
                    edges=_fb_edges, columns_attempted=_n, columns_traced=_n
                )

    return ModelAnalysisResult(
        edges=edges,
        columns_attempted=columns_attempted,
        columns_traced=columns_traced,
        failed_columns=failed_columns,
        output_column_names=list(output_columns),
    )
