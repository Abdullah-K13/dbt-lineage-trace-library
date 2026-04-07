"""
Benchmark: time a cold build of the lineage graph.

Run:
    python benchmark.py

Deletes the SQLite cache first so it always measures a full rebuild.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

# Delete cached .db files so we time a true cold build
for f in glob.glob("demo/.lineage_*.db"):
    os.remove(f)
    print(f"Deleted cache: {f}")

from dbt_lineage import LineageGraph

LineageGraph.clear_cache()

print(f"\nBuilding lineage graph from {MANIFEST} ...")
t0 = time.perf_counter()
g = LineageGraph(MANIFEST, catalog_path=CATALOG)
elapsed = time.perf_counter() - t0

s = g.stats()
print(f"\n{'='*50}")
print(f"  Cold build time : {elapsed:.1f}s")
print(f"  Total models    : {s.total_models}")
print(f"  Analyzed        : {s.models_analyzed}")
print(f"  Failed          : {s.models_failed}")
print(f"  Skipped         : {s.models_skipped}")
print(f"  Total edges     : {s.total_edges}")
print(f"  Total columns   : {s.total_columns}")
print(f"  Col coverage    : {s.column_coverage:.0%}")
print(f"  Success rate    : {s.success_rate:.0%}")
print(f"{'='*50}")

# Second run — should hit the SQLite disk cache
print(f"\nRunning again (should hit SQLite cache) ...")
LineageGraph.clear_cache()
t1 = time.perf_counter()
g2 = LineageGraph(MANIFEST, catalog_path=CATALOG)
cached_elapsed = time.perf_counter() - t1
print(f"  Cached load time: {cached_elapsed:.1f}s")

if __name__ == "__main__":
    pass
