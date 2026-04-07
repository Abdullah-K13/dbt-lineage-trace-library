"""
dbt-column-lineage  —  Demo & Consistency Test Suite
=====================================================
Run:   python demo/run_demo.py

What this script does
---------------------
1.  Loads the full lineage graph from manifest.json + catalog.json.
2.  Prints build stats.
3.  Runs a consistency checker — every trace is executed twice and the two
    results are diffed to confirm determinism.
4.  Tests specific known models and verifies expected source counts /
    resolution status are within acceptable ranges.
5.  Demonstrates multi-source UNION traces, transform chains, impact analysis,
    and the unqualified-column fix (max_score / feedback_type).
6.  Prints a transform-type distribution and resolution-status breakdown.
7.  Exports the full graph to demo/lineage_output.json.

Changing the target project
---------------------------
    MANIFEST_PATH = "path/to/manifest.json"
    CATALOG_PATH  = "path/to/catalog.json"

For large projects, pre-build the cache first:
    python build_full_cache.py
then subsequent runs load in under a second.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
MANIFEST_PATH = "demo/manifest.json"
CATALOG_PATH  = "demo/catalog.json"
OUTPUT_PATH   = "demo/lineage_output.json"

# Set to a model name to scope the graph (much faster for ad-hoc use).
# None = full graph.
SELECT_MODEL: str | None = None
# ─────────────────────────────────────────────────────────────────────────────

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

logging.basicConfig(level=logging.WARNING, format="%(levelname)s  %(message)s")

from dbt_lineage import LineageGraph, ResolutionStatus, TransformType
from dbt_lineage.sql_analyzer import analyze_model_columns

SEP  = "=" * 70
LINE = "-" * 70

PASS = "[PASS]"
FAIL = "[FAIL]"
WARN = "[WARN]"
INFO = "[INFO]"


def section(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def _fmt_sources(sources: list, limit: int = 6) -> str:
    strs = [str(c) for c in sources]
    if len(strs) <= limit:
        return str(strs)
    return f"[{', '.join(strs[:limit])}, ... +{len(strs)-limit} more]"


# ── Known test cases ──────────────────────────────────────────────────────────
# Each entry: (model, column, min_sources, max_sources, expected_status_set)
# min/max_sources: inclusive range for how many source columns are acceptable.
# expected_status_set: every edge's resolution_status must be in this set.
KNOWN_CASES: list[tuple[str, str, int, int, set]] = [
    # Multi-market UNION model — id_user comes from 12 base tables
    ("rep_user_feedback",    "id_user",         8,  20, {"resolved"}),
    # Unqualified column fix — survey column was previously invisible
    ("rep_user_feedback",    "max_score",        1,   5, {"resolved"}),
    ("rep_user_feedback",    "feedback_type",    1,   5, {"resolved"}),
    # CASE + rating from multiple survey sources
    ("rep_user_feedback",    "score",            5,  20, {"resolved"}),
    # Many-to-one comment from tickets + surveys
    ("rep_user_feedback",    "comment",         10,  40, {"resolved"}),
    # Support tickets — UNION across markets
    ("fct_support_tickets",  "tm_created",       2,   6, {"resolved"}),
    ("fct_support_tickets",  "arr_support_ticket_tags", 2, 6, {"resolved"}),
    # Net revenue — complex multi-source join
    ("fct_net_revenue",      "amt_discount",    10,  60, {"resolved"}),
    ("fct_net_revenue",      "id_listing",       5,  40, {"resolved"}),
    # Dim users — aggregated from multiple market sources
    ("dim_users",            "id_user",          4,  10, {"resolved"}),
    ("dim_users",            "avg_rating_seller", 1,   5, {"resolved"}),
    # Petpay snapshot — aggregation via CTE chain
    ("fct_petpay_snapshot",  "amt_petpay_balance", 5, 20, {"resolved"}),
    ("fct_petpay_snapshot",  "tm_petpay_kyc_pending", 5, 20, {"resolved"}),
    # Listing details — passthrough from INT layer
    ("dim_listing_details",  "id_listing",       1,   5, {"resolved"}),
]


def main() -> None:
    # ── Load graph ────────────────────────────────────────────────────────────
    LineageGraph.clear_cache()
    label = f"select='{SELECT_MODEL}'" if SELECT_MODEL else "full graph"
    print(f"\n  Loading lineage graph ({label}) ...")
    t0 = time.perf_counter()
    g = LineageGraph(
        MANIFEST_PATH,
        catalog_path=CATALOG_PATH or None,
        select=SELECT_MODEL,
    )
    load_time = time.perf_counter() - t0
    print(f"  Loaded in {load_time:.2f}s")

    # ── 1. Build stats ────────────────────────────────────────────────────────
    section("1. BUILD STATS")
    s = g.stats()
    print(f"  Total models    : {s.total_models}")
    print(f"  Analyzed        : {s.models_analyzed}  (produced >= 1 edge)")
    print(f"  Skipped         : {s.models_skipped}  (sources / seeds — no SQL)")
    print(f"  Failed          : {s.models_failed}  (SELECT * without schema, unparseable)")
    print(f"  Total columns   : {s.total_columns}")
    print(f"  Total edges     : {s.total_edges}")
    print(f"  Success rate    : {s.success_rate:.0%}")
    print(f"  Column coverage : {s.column_coverage:.0%}")
    if s.partially_analyzed_models:
        print(f"  Partial models  : {len(s.partially_analyzed_models)} with incomplete lineage")

    # ── 2. Consistency check ──────────────────────────────────────────────────
    section("2. CONSISTENCY CHECK — every trace run twice, results diffed")
    consistency_pass = 0
    consistency_fail = 0

    # Pick up to 15 columns across multiple models to cross-check
    consistency_cols: list[tuple[str, str]] = []
    for model_name in g.all_models()[:60]:
        cols = g.all_columns(model_name)
        if cols:
            consistency_cols.append((model_name, cols[0]))
        if len(consistency_cols) >= 15:
            break

    for model_name, col_name in consistency_cols:
        r1 = g.trace(model_name, col_name)
        r2 = g.trace(model_name, col_name)
        srcs1 = sorted(str(c) for c in r1.source_columns)
        srcs2 = sorted(str(c) for c in r2.source_columns)
        if srcs1 == srcs2:
            print(f"  {PASS} {model_name}.{col_name:<30} sources={len(srcs1)}")
            consistency_pass += 1
        else:
            print(f"  {FAIL} {model_name}.{col_name}")
            print(f"         Run1: {srcs1[:5]}")
            print(f"         Run2: {srcs2[:5]}")
            consistency_fail += 1

    print(f"\n  Result: {consistency_pass} passed, {consistency_fail} failed")
    if consistency_fail:
        print(f"  {WARN} Non-determinism detected — check for dict ordering or shared state.")

    # ── 3. Known model assertions ─────────────────────────────────────────────
    section("3. KNOWN MODEL ASSERTIONS")
    print(f"  {'Model.Column':<55} {'Sources':>7}  {'Status':<10}  Result")
    print(f"  {'-'*55} {'-'*7}  {'-'*10}  {'-'*6}")

    assertions_pass = 0
    assertions_fail = 0
    assertions_skip = 0

    for model, col, min_src, max_src, expected_statuses in KNOWN_CASES:
        available = g.all_columns(model)
        if col not in available:
            print(f"  {WARN} {model}.{col:<44} (not in graph — model may not be analyzed)")
            assertions_skip += 1
            continue

        result = g.trace(model, col)
        n_sources = len(result.source_columns)
        edge_statuses = {str(e.resolution_status) for e in result.edges}
        status_ok = edge_statuses.issubset(expected_statuses) or not edge_statuses

        label = f"{model}.{col}"
        count_ok = min_src <= n_sources <= max_src

        if count_ok and status_ok:
            verdict = PASS
            assertions_pass += 1
        else:
            verdict = FAIL
            assertions_fail += 1

        status_str = ",".join(sorted(edge_statuses)) or "—"
        print(f"  {verdict} {label:<55} {n_sources:>7}  {status_str:<10}  "
              f"(expected {min_src}-{max_src})")

        if not count_ok:
            print(f"       Sources: {_fmt_sources(result.source_columns)}")
        if not status_ok:
            print(f"       Unexpected status: {edge_statuses - expected_statuses}")

    print(f"\n  Result: {assertions_pass} passed, {assertions_fail} failed, {assertions_skip} skipped (not in graph)")

    # ── 4. Multi-source UNION trace ───────────────────────────────────────────
    section("4. MULTI-SOURCE UNION TRACE — rep_user_feedback.id_user")
    model_name, col_name = "rep_user_feedback", "id_user"
    if col_name in g.all_columns(model_name):
        result = g.trace(model_name, col_name)
        print(f"  Column  : {model_name}.{col_name}")
        print(f"  Sources : {len(result.source_columns)} (from {len(result.source_models)} model(s))")
        print(f"  Models  : {sorted(result.source_models)[:10]}")
        print(f"\n  Full source list:")
        for c in sorted(result.source_columns, key=lambda x: (x.model, x.column)):
            print(f"    {c.model}.{c.column}")
        if result.edges:
            print(f"\n  Hops (first 15):")
            for e in result.edges[:15]:
                print(f"    {str(e.source):<50} -> {str(e.target):<35} [{e.transform_type}]  [{e.resolution_status}]")
    else:
        print(f"  {WARN} {model_name} not in graph")

    # ── 5. Unqualified column fix ─────────────────────────────────────────────
    section("5. UNQUALIFIED COLUMN FIX — max_score & feedback_type")
    print("  These columns previously could NOT be traced because their")
    print("  source expressions used unqualified column references in CTEs.")
    print("  The SelectScope single-source fallback now resolves them.\n")
    for col in ("max_score", "feedback_type", "score", "comment"):
        if col in g.all_columns("rep_user_feedback"):
            r = g.trace("rep_user_feedback", col)
            statuses = sorted({str(e.resolution_status) for e in r.edges})
            print(f"  rep_user_feedback.{col:<20} -> {len(r.source_columns)} source(s)  status={statuses}")
            for c in sorted(r.source_columns, key=lambda x: str(x))[:4]:
                print(f"    <- {c}")
        else:
            print(f"  {WARN} rep_user_feedback.{col} not in graph")

    # ── 6. Transform chain — aggregation through CTEs ─────────────────────────
    section("6. CTE TRANSFORM CHAIN — fct_petpay_snapshot.tm_petpay_kyc_pending")
    print("  Demonstrates AGGREGATION type surfacing through passthrough CTE hops.\n")

    kyc_sql = """
WITH petpay_hist AS (
    SELECT
        *,
        first_value(cat_petpay_status) OVER (
            PARTITION BY id_market, id_listing, id_chat_channel, id_petpay
            ORDER BY tm_created DESC
        ) AS cat_petpay_status_latest
    FROM fct_petpay_states
),
acm_snap AS (
    SELECT
        id_petpay,
        cat_petpay_status_latest AS cat_petpay_status,
        MIN(CASE WHEN cat_petpay_status = 'AwaitingKycVerification'
                 THEN tm_created ELSE NULL END) AS tm_petpay_kyc_pending
    FROM petpay_hist
    GROUP BY id_petpay, cat_petpay_status_latest
)
SELECT id_petpay, cat_petpay_status, tm_petpay_kyc_pending FROM acm_snap
"""
    kyc_schema = {
        "fct_petpay_states": {
            "id_petpay": "INT64",
            "cat_petpay_status": "STRING",
            "tm_created": "TIMESTAMP",
        }
    }
    kyc_result = analyze_model_columns(
        kyc_sql, "fct_petpay_snapshot", schema=kyc_schema, dialect="bigquery"
    )
    for e in kyc_result.edges:
        if e.target.column.lower() == "tm_petpay_kyc_pending":
            print(f"  Edge         : {e.source}  ->  {e.target}")
            print(f"  Primary type : {e.transform_type}  (should be AGGREGATION)")
            sql_preview = e.transform_sql.replace("\n", " ").strip()
            print(f"  Primary SQL  : {sql_preview[:100]}{'...' if len(sql_preview) > 100 else ''}")
            if e.transform_chain:
                print(f"\n  Full transform chain (innermost -> outermost):")
                for step in e.transform_chain:
                    print(f"    [{step['type']:14s}]  {step['sql'][:80]}")
            verdict = PASS if str(e.transform_type) == "aggregation" else FAIL
            print(f"\n  {verdict} Primary transform is AGGREGATION (MIN surfaces through passthrough hops)")
            break
    else:
        print(f"  {WARN} No edge found for tm_petpay_kyc_pending — model may not be in graph")

    # ── 7. Impact analysis ────────────────────────────────────────────────────
    section("7. IMPACT ANALYSIS — dim_users.id_user downstream")

    im_model, im_col = "dim_users", "id_user"
    if im_col in g.all_columns(im_model):
        result = g.impact(im_model, im_col)
        print(f"  Source  : {im_model}.{im_col}")
        print(f"  Affected models  ({len(result.affected_models)}): {sorted(result.affected_models)[:10]}")
        print(f"  Affected columns ({len(result.affected_columns)}):")
        for c in sorted(result.affected_columns, key=lambda x: (x.model, x.column))[:20]:
            print(f"    {c.model}.{c.column}")
        if len(result.affected_columns) > 20:
            print(f"    ... and {len(result.affected_columns) - 20} more")
    else:
        print(f"  {WARN} {im_model}.{im_col} not in graph")

    # ── 8. Edges between two connected models ─────────────────────────────────
    section("8. EDGES BETWEEN CONNECTED MODELS")

    # Pick a connected pair: prefer models with many columns
    pair: tuple[str, str] | None = None
    for model_name in ["fct_net_revenue", "fct_support_tickets", "dim_users", "rep_user_feedback"]:
        deps = g.model_dependencies(model_name)
        for dep in deps:
            if g.edges_between(dep, model_name):
                pair = (dep, model_name)
                break
        if pair:
            break
    if pair is None:
        for model_name in g.all_models():
            for dep in g.model_dependencies(model_name):
                if g.edges_between(dep, model_name):
                    pair = (dep, model_name)
                    break
            if pair:
                break

    if pair:
        src, tgt = pair
        edges = g.edges_between(src, tgt)
        print(f"  {src}  ->  {tgt}")
        print(f"  {len(edges)} column edge(s)\n")
        for e in edges[:20]:
            transform_note = ""
            if e.transform_sql and e.transform_sql.strip() != e.source.column:
                transform_note = f"   SQL: {e.transform_sql[:60]}"
            print(f"  {e.source.column:<35} -> {e.target.column:<35} [{e.transform_type}]{transform_note}")
        if len(edges) > 20:
            print(f"  ... and {len(edges) - 20} more")
    else:
        print(f"  {WARN} No connected model pairs found")

    # ── 9. Transform type distribution ───────────────────────────────────────
    section("9. TRANSFORM TYPE DISTRIBUTION")
    type_totals: dict[str, int] = {}
    for t in TransformType:
        count = len(g.get_transforms_by_type(t))
        if count:
            type_totals[str(t)] = count

    total_edges = sum(type_totals.values())
    for t_name, count in sorted(type_totals.items(), key=lambda x: -x[1]):
        bar = "#" * min(40, int(40 * count / max(total_edges, 1)))
        print(f"  {t_name.upper():<20} {count:>6}  {bar}")
    print(f"\n  Total: {total_edges} edges")

    # ── 10. Resolution status breakdown ──────────────────────────────────────
    section("10. RESOLUTION STATUS BREAKDOWN")
    status_counts: dict[str, int] = defaultdict(int)
    for model_name in g.all_models():
        for col_name in g.all_columns(model_name):
            for e in g.trace(model_name, col_name).edges:
                status_counts[str(e.resolution_status)] += 1

    for status_val in [s.value for s in ResolutionStatus]:
        count = status_counts.get(status_val, 0)
        if count:
            print(f"  {status_val.upper():<12} {count:>6} edge(s)")
    if not any(status_counts.values()):
        print("  (no edges with status metadata — rebuild the cache to populate)")

    # Verdict
    ambiguous = status_counts.get("ambiguous", 0)
    unresolved = status_counts.get("unresolved", 0)
    if ambiguous == 0 and unresolved == 0:
        print(f"\n  {PASS} All traced edges are RESOLVED — no ambiguous or unresolved paths")
    else:
        if ambiguous:
            print(f"\n  {WARN} {ambiguous} AMBIGUOUS edge(s) — unqualified column in multi-table scope")
        if unresolved:
            print(f"  {WARN} {unresolved} UNRESOLVED edge(s) — literal, depth exceeded, or unsupported construct")

    # ── 11. SQL unit tests ────────────────────────────────────────────────────
    section("11. SQL UNIT TESTS — inline patterns")
    unit_tests: list[tuple[str, str, str, dict, str, list[str]]] = [
        # (description, sql, target_column, schema, dialect, expected_sources_containing)
        (
            "Passthrough",
            "SELECT order_id, amount FROM orders",
            "amount",
            {"orders": {"order_id": "INT64", "amount": "NUMERIC"}},
            "bigquery",
            ["orders.amount"],
        ),
        (
            "Rename only",
            "SELECT user_id AS customer_id FROM users",
            "customer_id",
            {"users": {"user_id": "INT64"}},
            "bigquery",
            ["users.user_id"],
        ),
        (
            "Cast + rename",
            "SELECT CAST(amount AS STRING) AS amount_str FROM payments",
            "amount_str",
            {"payments": {"amount": "NUMERIC"}},
            "bigquery",
            ["payments.amount"],
        ),
        (
            "Unqualified in single-table CTE (core fix)",
            """
            WITH base AS (
                SELECT survey_id,
                       CASE WHEN LOWER(survey) LIKE '%nps%' THEN 10 ELSE 5 END AS max_score
                FROM raw_surveys
            )
            SELECT survey_id, max_score FROM base
            """,
            "max_score",
            {"raw_surveys": {"survey_id": "INT64", "survey": "STRING"}},
            "bigquery",
            ["raw_surveys.survey"],
        ),
        (
            "UNION ALL — column from both branches",
            """
            SELECT id, name FROM table_a
            UNION ALL
            SELECT id, name FROM table_b
            """,
            "name",
            {"table_a": {"id": "INT64", "name": "STRING"},
             "table_b": {"id": "INT64", "name": "STRING"}},
            "bigquery",
            ["table_a.name", "table_b.name"],
        ),
        (
            "CTE alias shadow (nps-style)",
            """
            WITH raw AS (
                SELECT id, score FROM upstream AS raw
            ),
            final AS (SELECT id, score FROM raw)
            SELECT id, score FROM final
            """,
            "score",
            {"upstream": {"id": "INT64", "score": "NUMERIC"}},
            "bigquery",
            ["upstream.score"],
        ),
        (
            "Aggregation after rename",
            """
            WITH renamed AS (
                SELECT user_id AS uid, revenue AS rev FROM orders
            )
            SELECT uid, SUM(rev) AS total_rev FROM renamed GROUP BY uid
            """,
            "total_rev",
            {"orders": {"user_id": "INT64", "revenue": "NUMERIC"}},
            "bigquery",
            ["orders.revenue"],
        ),
        (
            "JOIN — qualified columns",
            """
            SELECT u.user_id, o.order_id, o.amount
            FROM users u
            JOIN orders o ON u.user_id = o.user_id
            """,
            "amount",
            {"users": {"user_id": "INT64"},
             "orders": {"order_id": "INT64", "user_id": "INT64", "amount": "NUMERIC"}},
            "bigquery",
            ["orders.amount"],
        ),
    ]

    unit_pass = 0
    unit_fail = 0
    print(f"  {'Test':<45} {'Expected sources found':<22}  Result")
    print(f"  {'-'*45} {'-'*22}  {'-'*6}")

    for desc, sql, target_col, schema, dialect, expected_srcs in unit_tests:
        result = analyze_model_columns(
            sql.strip(), "test_model", schema=schema, dialect=dialect
        )
        found_sources = [
            f"{e.source.model}.{e.source.column}"
            for e in result.edges
            if e.target.column == target_col
        ]
        missing = [s for s in expected_srcs if s not in found_sources]
        if not missing:
            verdict = PASS
            unit_pass += 1
        else:
            verdict = FAIL
            unit_fail += 1
        print(f"  {verdict} {desc:<43} {str(found_sources)[:22]}")
        if missing:
            print(f"       Missing: {missing}  Got: {found_sources}")

    print(f"\n  Result: {unit_pass} passed, {unit_fail} failed")

    # ── 12. Export ────────────────────────────────────────────────────────────
    section("12. EXPORT FULL GRAPH TO JSON")
    with open(OUTPUT_PATH, "w") as f:
        json.dump(g.to_dict(), f, indent=2)
    size_kb = os.path.getsize(OUTPUT_PATH) // 1024
    print(f"  Saved to {OUTPUT_PATH}  ({size_kb} KB)")
    print(f"  Top-level keys: {list(g.to_dict().keys())}")

    # ── Summary ───────────────────────────────────────────────────────────────
    section("SUMMARY")
    total_pass = consistency_pass + assertions_pass + unit_pass
    total_fail = consistency_fail + assertions_fail + unit_fail

    print(f"  Consistency check : {consistency_pass:>3} pass  {consistency_fail:>3} fail")
    print(f"  Model assertions  : {assertions_pass:>3} pass  {assertions_fail:>3} fail  "
          f"({assertions_skip} skipped)")
    print(f"  SQL unit tests    : {unit_pass:>3} pass  {unit_fail:>3} fail")
    print(f"  {'-'*40}")
    print(f"  TOTAL             : {total_pass:>3} pass  {total_fail:>3} fail")

    if total_fail == 0:
        print(f"\n  {PASS} All checks passed — lineage results are consistent and correct.")
    else:
        print(f"\n  {FAIL} {total_fail} check(s) failed — review output above for details.")

    print(f"\n  Tip: set SELECT_MODEL = 'your_model' at the top for targeted analysis.")
    print(SEP)


if __name__ == "__main__":
    main()
