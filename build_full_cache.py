"""
Build and cache the full lineage graph for all models in manifest.json + catalog.json.
======================================================================================

Run:
    python build_full_cache.py

What this does:
    1. Deletes any existing SQLite cache so a fresh full build is always performed.
    2. Parses manifest.json + catalog.json and analyzes every model (no select filter).
    3. Saves the result to a SQLite cache file (.lineage_<hash>.db) next to manifest.json.
    4. Runs a second pass to confirm the cache loads correctly and prints the load time.

After this script completes, any LineageGraph() call pointing at the same
manifest.json + catalog.json will load from the SQLite cache in under a second
instead of rebuilding from scratch.

To point at your own artifacts, change MANIFEST and CATALOG below.
"""

import glob
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, "dbt-column-lineage/src")

MANIFEST = "demo/manifest.json"
CATALOG  = "demo/catalog.json"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_full_cache")

SEP = "=" * 65

# ── Delete existing cache so we always do a true cold build ──────────────────
cache_dir = Path(MANIFEST).parent
deleted = []
for f in glob.glob(str(cache_dir / ".lineage_*.db")):
    try:
        os.remove(f)
        deleted.append(Path(f).name)
    except PermissionError:
        log.warning(f"Could not delete {Path(f).name} — file is in use. Close any other process using it and retry.")

if deleted:
    for name in deleted:
        log.info(f"Deleted existing cache: {name}")
else:
    log.info("No existing cache found — starting fresh build.")

# ── Import after path setup ───────────────────────────────────────────────────
from dbt_lineage import LineageGraph

LineageGraph.clear_cache()

# ── Cold build — full graph, no select filter ─────────────────────────────────
print(f"\n{SEP}")
print(f"  Building FULL lineage graph")
print(f"  manifest : {MANIFEST}")
print(f"  catalog  : {CATALOG}")
print(SEP)

t0 = time.perf_counter()
g = LineageGraph(MANIFEST, catalog_path=CATALOG)
elapsed = time.perf_counter() - t0

s = g.stats()
print(f"\n{SEP}")
print(f"  Cold build complete")
print(f"  Time            : {elapsed:.1f}s")
print(f"  Total models    : {s.total_models}")
print(f"  Analyzed        : {s.models_analyzed}  (produced >= 1 edge)")
print(f"  Skipped         : {s.models_skipped}  (sources / seeds — no SQL)")
print(f"  Failed          : {s.models_failed}  (Jinja, unparseable, or SELECT * without schema)")
print(f"  Total columns   : {s.total_columns}")
print(f"  Total edges     : {s.total_edges}")
print(f"  Success rate    : {s.success_rate:.0%}")
print(f"  Col coverage    : {s.column_coverage:.0%}")
print(SEP)

# ── Confirm cache exists ──────────────────────────────────────────────────────
cache_files = list(cache_dir.glob(".lineage_*.db"))
if cache_files:
    cache_file = cache_files[0]
    size_mb = cache_file.stat().st_size / (1024 * 1024)
    print(f"\n  Cache saved: {cache_file.name}  ({size_mb:.1f} MB)")
else:
    print("\n  WARNING: cache file not found — SQLite save may have failed.")

# ── Warm load — confirm cache works ──────────────────────────────────────────
print(f"\n  Verifying cache load ...")
LineageGraph.clear_cache()
t1 = time.perf_counter()
g2 = LineageGraph(MANIFEST, catalog_path=CATALOG)
cached_elapsed = time.perf_counter() - t1

s2 = g2.stats()
print(f"  Cached load time : {cached_elapsed:.2f}s")
print(f"  Edges loaded     : {s2.total_edges}  (matches: {s2.total_edges == s.total_edges})")
print(f"\n  Done. Future LineageGraph() calls will load from cache in ~{cached_elapsed:.1f}s.")
print(SEP)
