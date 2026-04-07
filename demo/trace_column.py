"""
dbt-column-lineage  —  Interactive Column Tracer
================================================
Usage:
    python demo/trace_column.py                          # interactive prompts
    python demo/trace_column.py rep_user_feedback max_score
    python demo/trace_column.py fct_net_revenue amt_discount

For every hop in the lineage path this script shows:
  - Source model + column  →  target model + column
  - The transform type (passthrough, cast, conditional, aggregation, ...)
  - The actual SQL expression for the transform
  - A full-SQL block for any hop that is CONDITIONAL, FUNCTION, ARITHMETIC,
    AGGREGATION, COMPLEX, or WINDOW — so you can see what CASE / SUM / CAST
    expression connects the two columns even when the column is renamed or
    derived from an expression that reads a differently-named raw column.

For AMBIGUOUS edges (unqualified column in a multi-table scope) the
transform SQL is shown as the best available explanation of the derivation.
"""

from __future__ import annotations

import os
import sys
import textwrap
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
logging.basicConfig(level=logging.WARNING, format="%(levelname)s  %(message)s")

MANIFEST_PATH = "demo/manifest.json"
CATALOG_PATH  = "demo/catalog.json"

# Transform types whose SQL is always shown in full (they change the value)
_SHOW_SQL_TYPES = {
    "conditional",   # CASE / IF
    "function",      # COALESCE, UPPER, TRIM, ...
    "arithmetic",    # col * rate, col_a + col_b
    "aggregation",   # SUM, COUNT, MIN, MAX, AVG
    "window",        # ROW_NUMBER OVER, LAG, LEAD
    "complex",       # anything else with nested expressions
    "cast",          # CAST(x AS TYPE), x::TYPE
    "rename",        # shows the mapping when name changed
}

SEP   = "=" * 72
THIN  = "-" * 72
ARROW = " -> "


# ── Formatting helpers ────────────────────────────────────────────────────────

def _sql_block(sql: str, indent: int = 6, width: int = 68) -> str:
    """Wrap and indent a SQL string for display."""
    sql = sql.strip()
    if not sql:
        return ""
    lines = textwrap.wrap(sql, width=width - indent)
    pad = " " * indent
    return "\n".join(pad + line for line in lines)


def _is_trivial_sql(sql: str, source_col: str, target_col: str) -> bool:
    """Return True when the SQL is just a bare column reference — nothing to show."""
    if not sql:
        return True
    stripped = sql.strip().lower()
    # Bare column name (possibly table-qualified) — no value change
    # e.g. "survey", "source_table.survey", "t.survey"
    bare = stripped.split(".")[-1].strip("`\"'")
    return bare in (source_col.lower(), target_col.lower())


def _transform_label(transform_type: str, source_col: str, target_col: str) -> str:
    """Return a short label describing the transform."""
    t = transform_type.lower()
    renamed = source_col.lower() != target_col.lower()
    if t == "passthrough" and not renamed:
        return "passthrough"
    if t == "passthrough" and renamed:
        return f"rename  ({source_col} -> {target_col})"
    if t == "rename":
        return f"rename  ({source_col} -> {target_col})"
    return t


def _status_note(status: str) -> str:
    if status == "ambiguous":
        return "  [AMBIGUOUS — unqualified column, single-source inference used]"
    if status == "unresolved":
        return "  [UNRESOLVED — literal value or unsupported construct]"
    return ""


# ── Core display ──────────────────────────────────────────────────────────────

def _print_hop(edge, idx: int, total: int) -> None:
    """Print one hop in the lineage path."""
    tt   = str(edge.transform_type)
    sql  = (edge.transform_sql or "").strip()
    stat = str(edge.resolution_status)

    # Literal sentinel: show differently — no fake source column label
    if edge.source.model == "__literal__":
        tgt = str(edge.target)
        print(f"  [{idx:>2}/{total}]  <literal>  {sql!r:<44}{ARROW}{tgt}")
        print(f"         Transform : literal  (constant, no upstream column)")
        print(f"         Value     : {sql}")
        print()
        return

    src  = str(edge.source)
    tgt  = str(edge.target)

    label = _transform_label(tt, edge.source.column, edge.target.column)
    status_note = _status_note(stat)

    # Header line: source -> target [type]
    print(f"  [{idx:>2}/{total}]  {src:<48}{ARROW}{tgt}")
    print(f"         Transform : {label}{status_note}")

    # SQL block — always show for non-trivial transforms
    SHOW_SQL_TYPES = {
        "conditional", "function", "arithmetic", "aggregation",
        "window", "complex", "cast", "rename",
    }
    show_sql = (
        tt.lower() in SHOW_SQL_TYPES
        or stat in ("ambiguous", "unresolved")
        or not _is_trivial_sql(sql, edge.source.column, edge.target.column)
    )
    if show_sql and sql:
        print(f"         SQL       :")
        print(_sql_block(sql, indent=20, width=72))

    # Transform chain — show when it exists and has meaningful steps
    chain = [
        s for s in (edge.transform_chain or [])
        if s.get("type", "passthrough") not in ("passthrough", "unknown")
        and s.get("sql", "").strip()
    ]
    if chain:
        print(f"         CTE steps : ({len(chain)} intermediate step(s))")
        for step in chain[:4]:
            step_sql = (step.get("sql") or "").strip()
            step_type = step.get("type", "")
            step_name = step.get("step", "")
            label_str = f"[{step_type}]" if step_type else ""
            name_str  = f" ({step_name})" if step_name else ""
            print(f"           {label_str}{name_str}")
            if step_sql:
                print(_sql_block(step_sql, indent=14, width=70))
        if len(chain) > 4:
            print(f"           ... +{len(chain)-4} more steps")

    print()


def _group_edges_by_source_model(edges):
    """Group edges by their source model for cleaner display of UNION sources."""
    groups: dict[str, list] = {}
    for e in edges:
        groups.setdefault(e.source.model, []).append(e)
    return groups


# ── Main tracer ───────────────────────────────────────────────────────────────

def trace_column(g, model_name: str, col_name: str) -> None:
    """Run and display the full upstream trace for model_name.col_name."""

    # Validate inputs
    available_models = set(g.all_models())
    if model_name not in available_models:
        # Try case-insensitive match
        matches = [m for m in available_models if m.lower() == model_name.lower()]
        if matches:
            model_name = matches[0]
            print(f"  Note: matched model '{model_name}' (case-corrected)")
        else:
            query = model_name.lower()
            close = [
                m for m in available_models
                if query in m.lower()
                or m.lower() in query
                or m.lower().startswith(query[:min(6, len(query))])
                or query.startswith(m.lower()[:min(6, len(m))])
            ][:8]
            print(f"\n  Model '{model_name}' not found in graph.")
            if close:
                print(f"  Did you mean one of these?")
                for m in sorted(close):
                    print(f"    {m}")
            else:
                print(f"  Try running: python demo/run_demo.py to see available models.")
            return

    available_cols = g.all_columns(model_name)
    if col_name not in available_cols:
        matches = [c for c in available_cols if c.lower() == col_name.lower()]
        if matches:
            col_name = matches[0]
            print(f"  Note: matched column '{col_name}' (case-corrected)")
        else:
            close = [c for c in available_cols if col_name.lower() in c.lower()][:10]
            print(f"\n  Column '{col_name}' not found in '{model_name}'.")
            print(f"  Available columns ({len(available_cols)}):")
            for c in sorted(available_cols)[:30]:
                marker = "  <--" if c in close else ""
                print(f"    {c}{marker}")
            if len(available_cols) > 30:
                print(f"    ... and {len(available_cols)-30} more")
            return

    # Run trace
    result = g.trace(model_name, col_name)

    print(f"\n{SEP}")
    print(f"  TRACE: {model_name}.{col_name}")
    print(SEP)

    # ── 1. Root sources ───────────────────────────────────────────────────────
    # Separate real sources from literals
    literal_edges = [e for e in result.edges if e.source.model == "__literal__"]
    real_source_cols = [c for c in result.source_columns if c.model != "__literal__"]
    literal_source_cols = [c for c in result.source_columns if c.model == "__literal__"]

    print(f"\n  ROOT SOURCES  ({len(real_source_cols)} upstream column(s)"
          + (f", {len(literal_source_cols)} literal(s)" if literal_source_cols else "") + ")")
    print(f"  {THIN}")
    if not result.source_columns:
        print("  (none — column may be a literal or the graph has no edges for it)")
        print("  Check that the model was analyzed (run build_full_cache.py or")
        print("  ensure catalog.json covers the upstream tables).")
    else:
        source_groups = _group_edges_by_source_model(
            [e for e in result.edges
             if e.source.model != "__literal__"
             and e.source.model not in {e2.target.model for e2 in result.edges}]
        )
        for model, model_edges in sorted(source_groups.items()):
            cols = sorted({e.source.column for e in model_edges})
            for col in cols:
                print(f"    {model}.{col}")
        # Show literals as a distinct group
        if literal_source_cols:
            print(f"  LITERAL VALUES  (constant expressions with no upstream column)")
            print(f"  {THIN}")
            seen_lits: set[tuple] = set()
            for e in literal_edges:
                key = (e.target.model, e.target.column, e.source.column)
                if key not in seen_lits:
                    seen_lits.add(key)
                    print(f"    {e.target.model}.{e.target.column}  =  {e.source.column}")

    # ── 2. Hop-by-hop path ────────────────────────────────────────────────────
    if not result.edges:
        print(f"\n  No hops in trace.")
        return

    total = len(result.edges)
    print(f"\n  LINEAGE PATH  ({total} hop(s))")
    print(f"  {THIN}")
    print()

    # Group by whether SQL is interesting — show non-trivial hops inline
    for idx, edge in enumerate(result.edges, 1):
        _print_hop(edge, idx, total)

    # ── 3. Transform type summary ─────────────────────────────────────────────
    from collections import Counter
    non_literal_edges = [e for e in result.edges if e.source.model != "__literal__"]
    literal_edges_all = [e for e in result.edges if e.source.model == "__literal__"]
    type_counts = Counter(str(e.transform_type) for e in non_literal_edges)
    status_counts = Counter(str(e.resolution_status) for e in non_literal_edges)

    print(f"  {THIN}")
    print(f"  TRANSFORM SUMMARY")
    for t, count in type_counts.most_common():
        print(f"    {t:<18} {count:>4} hop(s)")
    if literal_edges_all:
        print(f"    {'literal':<18} {len(literal_edges_all):>4} hop(s)  (constant values, no upstream column)")

    # ── 4. Resolution status ──────────────────────────────────────────────────
    print()
    print(f"  RESOLUTION STATUS")
    for s, count in status_counts.most_common():
        flag = ""
        if s == "ambiguous":
            flag = "  <- unqualified column, attribution inferred from single-table scope"
        elif s == "unresolved":
            flag = "  <- literal / unsupported construct, no source column exists"
        print(f"    {s:<14} {count:>4} hop(s){flag}")

    # ── 5. Literal values summary ─────────────────────────────────────────────
    if literal_edges_all:
        print()
        print(f"  LITERAL VALUES  (columns with constant / hardcoded values)")
        seen_lit: set[tuple] = set()
        for e in literal_edges_all:
            key = (e.target.model, e.target.column)
            if key not in seen_lit:
                seen_lit.add(key)
                print(f"    {e.target.model}.{e.target.column}  =  {e.source.column}")

    # ── 6. Derivation note for renamed / cross-column edges ───────────────────
    rename_hops = [
        e for e in result.edges
        if e.source.model != "__literal__"
        and e.source.column.lower() != e.target.column.lower()
    ]
    if rename_hops:
        print()
        print(f"  NAME CHANGES ALONG THE PATH")
        seen_pairs: set[tuple] = set()
        for e in rename_hops:
            pair = (e.source.column, e.target.column)
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                t = str(e.transform_type)
                sql = (e.transform_sql or "").strip()
                note = f"  [{t}]"
                if t in ("cast", "conditional", "function", "arithmetic", "aggregation", "complex"):
                    short_sql = sql[:60] + ("..." if len(sql) > 60 else "")
                    note = f"  [{t}]  {short_sql}"
                print(f"    {e.source.column}  ->  {e.target.column}{note}")

    print(f"\n{SEP}\n")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    from dbt_lineage import LineageGraph

    print("\n  Loading lineage graph (cached)...")
    g = LineageGraph(MANIFEST_PATH, catalog_path=CATALOG_PATH)
    print(f"  {g.stats().total_columns} columns across {g.stats().models_analyzed} analyzed models\n")

    # Accept args from command line or prompt interactively
    args = sys.argv[1:]
    if len(args) >= 2:
        model_name = args[0].strip()
        col_name   = args[1].strip()
    else:
        print("  Enter the model and column to trace.")
        print(f"  (leave blank to use example: rep_user_feedback / max_score)\n")
        model_name = input("  Model name  : ").strip() or "rep_user_feedback"
        col_name   = input("  Column name : ").strip() or "max_score"

    trace_column(g, model_name, col_name)

    # Loop for more traces in interactive mode
    if len(args) < 2:
        while True:
            again = input("  Trace another column? (y/n): ").strip().lower()
            if again != "y":
                break
            print()
            model_name = input("  Model name  : ").strip()
            col_name   = input("  Column name : ").strip()
            if model_name and col_name:
                trace_column(g, model_name, col_name)


if __name__ == "__main__":
    main()
