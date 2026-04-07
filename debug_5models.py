"""
Debug benchmark: 5 models around rep_listing_liquidity
=======================================================

Layout:
    dim_listing_states          (upstream level 2)
        └── fct_listing_creations   (upstream level 1)
                └── rep_listing_liquidity        (target)
                        └── dash_owp_marketplace_activity__liquidity  (downstream 1)
                                └── dash_owp_finance_model__strategic_metrics_2026  (downstream 2)

Run:
    python debug_5models.py

What this measures per model, per stage:
    1. Jinja check         - instant skip if unresolved templates
    2. parse_one()         - SQLGlot AST parse
    3. qualify()           - full schema vs filtered schema
    4. single_pass()       - AST traversal lineage extraction
    5. full analyze()      - end-to-end analyze_model_columns()
"""

import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, "dbt-column-lineage/src")

# ── Config ────────────────────────────────────────────────────────────────────
MANIFEST = "demo/manifest.json"
CATALOG  = "demo/catalog.json"
DIALECT  = "bigquery"

MODELS_TO_TEST = [
    "dim_listing_states",                               # upstream level 2
    "fct_listing_creations",                            # upstream level 1
    "rep_listing_liquidity",                            # target
    "dash_owp_marketplace_activity__liquidity",         # downstream level 1
    "dash_owp_finance_model__strategic_metrics_2026",   # downstream level 2
]

REPEATS = 3   # how many times to repeat each timed step for stable averages
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.WARNING,       # suppress library noise
    format="%(levelname)s  %(message)s",
)
log = logging.getLogger("debug_5models")
log.setLevel(logging.DEBUG)

SEP  = "=" * 75
DASH = "-" * 75


def hdr(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def row(label: str, ms: float, extra: str = "") -> None:
    bar_len = min(int(ms / 10), 50)   # 1 char = 10ms
    bar = "#" * bar_len
    extra_str = f"  {extra}" if extra else ""
    print(f"  {label:<22} {ms:>8.1f} ms  {bar}{extra_str}")


def avg_ms(fn, repeats: int) -> float:
    """Run fn() `repeats` times and return average elapsed in ms."""
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return sum(times) / len(times) * 1000


# ── Load shared state ─────────────────────────────────────────────────────────
hdr("SETUP — loading manifest + catalog")

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify as sqlglot_qualify

from dbt_lineage.parser import (
    parse_manifest, parse_catalog, build_schema_dict, build_table_lookup
)
from dbt_lineage.models import ResourceType
from dbt_lineage.sql_analyzer import (
    analyze_model_columns,
    _build_alias_map,
    _get_output_columns,
    _single_pass_analyze_ast,
    _has_select_star,
)

t0 = time.perf_counter()
models, metadata = parse_manifest(Path(MANIFEST))
t_manifest = time.perf_counter() - t0

t0 = time.perf_counter()
catalog_data = parse_catalog(Path(CATALOG))
full_schema  = build_schema_dict(catalog_data, models)
table_lookup = build_table_lookup(models)
t_catalog = time.perf_counter() - t0

print(f"  manifest parsed:   {t_manifest*1000:.1f} ms  ({len(models)} nodes)")
print(f"  catalog parsed:    {t_catalog*1000:.1f} ms  ({len(full_schema)} tables, "
      f"{sum(len(v) for v in full_schema.values())} total columns)")
print(f"  dialect:           {DIALECT}")


# ── Per-model benchmark ───────────────────────────────────────────────────────
summary = []   # (name, total_ms, edges)

for model_name in MODELS_TO_TEST:
    hdr(f"MODEL: {model_name}")

    # Locate model
    m = next((x for x in models.values() if x.name == model_name), None)
    if m is None:
        print(f"  !! NOT FOUND in manifest — skipping")
        continue

    sql = m.compiled_sql
    print(f"  SQL:    {len(sql):,} chars  |  {sql.count(chr(10))} lines")
    print(f"  type:   {m.resource_type.value}")
    print()

    # ── Stage 1: Jinja check ─────────────────────────────────────────────────
    has_jinja = "{{" in sql or "{%" in sql
    print(f"  [1] Jinja check      has_jinja={has_jinja}")
    if has_jinja:
        print("      → WOULD SKIP (unresolved Jinja — early exit in analyze_model_columns)")
        summary.append((model_name, 0.0, 0, "jinja-skip"))
        continue

    print(DASH)

    # ── Stage 2: parse_one ───────────────────────────────────────────────────
    parsed = None
    parse_error = None
    try:
        parsed = sqlglot.parse_one(sql, dialect=DIALECT)
    except Exception as e:
        parse_error = str(e)[:80]

    if parse_error:
        ms = avg_ms(lambda: sqlglot.parse_one(sql, dialect=DIALECT) if not parse_error else None, 1)
        row("[2] parse_one()", ms, f"ERROR: {parse_error}")
        summary.append((model_name, ms, 0, "parse-error"))
        continue

    ms_parse = avg_ms(lambda: sqlglot.parse_one(sql, dialect=DIALECT), REPEATS)
    cols_detected = _get_output_columns(parsed)
    row("[2] parse_one()", ms_parse, f"output cols detected: {len(cols_detected)}")
    print(f"       output cols: {cols_detected[:8]}{'...' if len(cols_detected) > 8 else ''}")
    print(f"       SELECT *:    {_has_select_star(parsed)}")

    # ── Stage 3a: qualify() — full schema ────────────────────────────────────
    def _qualify_full():
        try:
            sqlglot_qualify(
                parsed.copy(), schema=full_schema, dialect=DIALECT,
                validate_qualify_columns=False, identify=False, expand_stars=True,
            )
        except Exception:
            pass

    ms_qualify_full = avg_ms(_qualify_full, REPEATS)
    row("[3a] qualify() FULL schema", ms_qualify_full,
        f"({len(full_schema)} tables in schema)")

    # ── Stage 3b: qualify() — filtered schema (only referenced tables) ───────
    referenced = {t.name.lower() for t in parsed.find_all(exp.Table) if t.name}
    local_schema = {k: v for k, v in full_schema.items() if k.lower() in referenced}

    def _qualify_local():
        try:
            sqlglot_qualify(
                parsed.copy(), schema=local_schema, dialect=DIALECT,
                validate_qualify_columns=False, identify=False, expand_stars=True,
            )
        except Exception:
            pass

    ms_qualify_local = avg_ms(_qualify_local, REPEATS)
    speedup = ms_qualify_full / ms_qualify_local if ms_qualify_local > 0 else 0
    row("[3b] qualify() FILTERED schema", ms_qualify_local,
        f"({len(local_schema)} tables, {speedup:.0f}x faster)")
    print(f"       referenced tables: {sorted(referenced)}")

    # ── Stage 4: single-pass AST analysis ────────────────────────────────────
    alias_map = _build_alias_map(parsed)

    def _single():
        _single_pass_analyze_ast(parsed, model_name, DIALECT, alias_map, table_lookup)

    ms_single = avg_ms(_single, REPEATS)
    sp_edges = _single_pass_analyze_ast(parsed, model_name, DIALECT, alias_map, table_lookup)
    row("[4] single_pass_analyze()", ms_single,
        f"edges={len(sp_edges) if sp_edges else 0}")

    # ── Stage 5: full analyze_model_columns() ────────────────────────────────
    def _full():
        analyze_model_columns(
            sql, model_name, schema=full_schema,
            dialect=DIALECT, table_lookup=table_lookup,
        )

    ms_full = avg_ms(_full, REPEATS)
    result = analyze_model_columns(
        sql, model_name, schema=full_schema,
        dialect=DIALECT, table_lookup=table_lookup,
    )
    row("[5] analyze_model_columns()", ms_full,
        f"edges={len(result.edges)}  cols_traced={result.columns_traced}/{result.columns_attempted}")

    print()
    if result.edges:
        print("  Edges found:")
        for e in result.edges[:10]:
            print(f"    {e.source.model}.{e.source.column:<30} -> {e.target.column:<25} [{e.transform_type}]")
        if len(result.edges) > 10:
            print(f"    ... and {len(result.edges) - 10} more")
    else:
        print("  No edges produced.")

    if result.failed_columns:
        print(f"  Failed columns: {result.failed_columns[:5]}")

    summary.append((model_name, ms_full, len(result.edges), "ok"))


# ── Summary ───────────────────────────────────────────────────────────────────
hdr("SUMMARY")
print(f"  {'Model':<52} {'Total ms':>9}  {'Edges':>6}  Status")
print(f"  {'-'*52} {'-'*9}  {'-'*6}  {'-'*10}")
grand_total = 0.0
for name, ms, edges, status in summary:
    print(f"  {name:<52} {ms:>9.1f}  {edges:>6}  {status}")
    grand_total += ms

print(f"\n  Grand total (all 5 models): {grand_total:.1f} ms  ({grand_total/1000:.2f}s)")
print(f"\n  Note: second run will load from SQLite cache — near-instant.")
print(SEP)
