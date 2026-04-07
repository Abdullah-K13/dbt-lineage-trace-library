"""
Query the SQLite lineage cache file.
=====================================
Run:
    python query_cache.py

Shows what's inside the .lineage_*.db cache file:
    - Build metadata (dialect, stats, cache key)
    - Model list with edge counts
    - Sample edges
    - Column search
    - Model deep-dive
"""

import glob
import sqlite3
import sys
from pathlib import Path

# ── Find cache file ───────────────────────────────────────────────────────────
CACHE_DIR = Path("demo")

cache_files = sorted(CACHE_DIR.glob(".lineage_*.db"))
if not cache_files:
    print("No .lineage_*.db cache file found in demo/. Run build_full_cache.py first.")
    sys.exit(1)

# Use the largest file (most complete graph) if multiple exist
cache_file = max(cache_files, key=lambda f: f.stat().st_size)
print(f"Using cache: {cache_file}  ({cache_file.stat().st_size / 1024 / 1024:.1f} MB)\n")

conn = sqlite3.connect(cache_file)
conn.row_factory = sqlite3.Row

SEP  = "=" * 65
DASH = "-" * 65


def section(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


# ── 1. Metadata ───────────────────────────────────────────────────────────────
section("1. CACHE METADATA")

meta = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM meta")}
for k, v in meta.items():
    if k == "cache_key":
        print(f"  {k:<20}: {v[:32]}...")
    else:
        try:
            import json
            stats = json.loads(v)
            print(f"  {k:<20}:")
            for sk, sv in stats.items():
                print(f"    {sk:<30}: {sv}")
        except Exception:
            print(f"  {k:<20}: {v}")


# ── 2. Summary counts ─────────────────────────────────────────────────────────
section("2. SUMMARY COUNTS")

total_models = conn.execute("SELECT COUNT(*) FROM models").fetchone()[0]
total_edges  = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
total_cols_as_target = conn.execute("SELECT COUNT(DISTINCT target_column) FROM edges").fetchone()[0]
total_target_models  = conn.execute("SELECT COUNT(DISTINCT target_model) FROM edges").fetchone()[0]

print(f"  Models in cache     : {total_models}")
print(f"  Edges               : {total_edges}")
print(f"  Distinct target cols: {total_cols_as_target}")
print(f"  Models with edges   : {total_target_models}")


# ── 3. Transform type breakdown ───────────────────────────────────────────────
section("3. TRANSFORM TYPE BREAKDOWN")

rows = conn.execute("""
    SELECT transform_type, COUNT(*) as cnt
    FROM edges
    GROUP BY transform_type
    ORDER BY cnt DESC
""").fetchall()

print(f"  {'Transform Type':<20} {'Edges':>8}")
print(f"  {'-'*20} {'-'*8}")
for r in rows:
    print(f"  {r['transform_type']:<20} {r['cnt']:>8,}")


# ── 4. Top 20 models by edge count ───────────────────────────────────────────
section("4. TOP 20 MODELS BY EDGE COUNT (as target)")

rows = conn.execute("""
    SELECT target_model, COUNT(*) as edge_count
    FROM edges
    GROUP BY target_model
    ORDER BY edge_count DESC
    LIMIT 20
""").fetchall()

print(f"  {'Model':<55} {'Edges':>6}")
print(f"  {'-'*55} {'-'*6}")
for r in rows:
    print(f"  {r['target_model']:<55} {r['edge_count']:>6}")


# ── 5. Sample edges ───────────────────────────────────────────────────────────
section("5. SAMPLE EDGES (first 20)")

rows = conn.execute("""
    SELECT source_model, source_column, target_model, target_column, transform_type
    FROM edges
    LIMIT 20
""").fetchall()

print(f"  {'Source model.column':<50}  {'Target column':<30} Type")
print(DASH)
for r in rows:
    src = f"{r['source_model']}.{r['source_column']}"
    print(f"  {src:<50}  {r['target_column']:<30} {r['transform_type']}")


# ── 6. Search edges for a specific model ─────────────────────────────────────
section("6. DEEP DIVE — specific model")

# Change this to any model you want to inspect
INSPECT_MODEL = "rep_user_feedback"

rows = conn.execute("""
    SELECT source_model, source_column, target_column, transform_type, transform_sql
    FROM edges
    WHERE target_model = ?
    ORDER BY target_column
""", (INSPECT_MODEL,)).fetchall()

print(f"  Edges flowing INTO '{INSPECT_MODEL}': {len(rows)}")
print()
if rows:
    print(f"  {'Source model.column':<50}  {'-> Target col':<30} Type")
    print(DASH)
    for r in rows:
        src = f"{r['source_model']}.{r['source_column']}"
        sql_preview = (r['transform_sql'] or "").replace("\n", " ").strip()[:40]
        print(f"  {src:<50}  {r['target_column']:<30} {r['transform_type']}")
        if sql_preview and sql_preview != r['source_column']:
            print(f"    SQL: {sql_preview}")
else:
    print(f"  (no edges found — model may have failed analysis or is a source)")

print()
rows_out = conn.execute("""
    SELECT source_column, target_model, target_column, transform_type
    FROM edges
    WHERE source_model = ?
    ORDER BY target_model, source_column
    LIMIT 30
""", (INSPECT_MODEL,)).fetchall()

print(f"  Edges flowing OUT OF '{INSPECT_MODEL}': {len(rows_out)}")
if rows_out:
    print()
    print(f"  {'Source col':<30}  {'-> Target model.column':<55} Type")
    print(DASH)
    for r in rows_out:
        tgt = f"{r['target_model']}.{r['target_column']}"
        print(f"  {r['source_column']:<30}  {tgt:<55} {r['transform_type']}")


# ── 7. Column search ──────────────────────────────────────────────────────────
section("7. COLUMN SEARCH — find 'id_user' across all models")

SEARCH_COL = "id_user"

rows = conn.execute("""
    SELECT DISTINCT target_model, target_column, source_model, source_column, transform_type
    FROM edges
    WHERE LOWER(target_column) = LOWER(?)
    ORDER BY target_model
    LIMIT 30
""", (SEARCH_COL,)).fetchall()

print(f"  Models where '{SEARCH_COL}' is an output column: {len(rows)}")
print()
for r in rows:
    print(f"  {r['source_model']}.{r['source_column']:<30} -> {r['target_model']}.{r['target_column']} [{r['transform_type']}]")


# ── 8. Models list (all) ──────────────────────────────────────────────────────
section("8. ALL MODELS IN CACHE")

rows = conn.execute("SELECT unique_id, name, resource_type FROM models ORDER BY name").fetchall()
print(f"  {'Name':<55} {'Type':<15} Unique ID")
print(DASH)
for r in rows[:50]:
    print(f"  {r['name']:<55} {r['resource_type']:<15} {r['unique_id'][:40]}")
if len(rows) > 50:
    print(f"  ... and {len(rows) - 50} more models")


conn.close()
print(f"\n{SEP}")
print(f"  Done. To inspect further, open the file in DB Browser for SQLite")
print(f"  or run: sqlite3 {cache_file}")
print(SEP)
