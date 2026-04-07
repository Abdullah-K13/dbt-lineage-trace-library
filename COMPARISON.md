# dbt-column-lineage vs Standalone SQL Lineage — What's the Difference?

## The Simple Difference

**dbt-column-lineage (built):** A tool that reads dbt's output files and tells you how columns connect.

**Standalone SQL Lineage (proposed):** A tool that reads your actual SQL files directly and tells you how columns connect — no dbt required.

---

## The Analogy

Think of it like GPS navigation.

**dbt-column-lineage** = GPS that only works if you've already printed a map from MapQuest first (the `manifest.json`). No printed map, no navigation.

**Standalone version** = GPS that figures out the roads itself by looking out the window. Point it anywhere, it works.

---

## Side by Side

| | dbt-column-lineage (built) | Standalone (proposed) |
|---|---|---|
| **Who can use it** | Only dbt users | Anyone with SQL files |
| **What you point it at** | `target/manifest.json` | Any folder with `.sql` files |
| **Requires running dbt first** | Yes | No |
| **Requires warehouse connection** | No | No |
| **Works with Snowflake SQL files** | Only if using dbt | Yes |
| **Works with plain `.sql` files** | No | Yes |
| **Works with Python ETL scripts** | No | Yes (extracts SQL from them) |
| **Remembers previous analysis** | No (rebuilds every time) | Yes (SQLite database) |
| **Updates when files change** | No | Yes (watches for changes) |
| **Works in Claude / Cursor** | No | Yes (MCP server) |
| **Visualization** | No | Yes |
| **Market size** | dbt users only (~50k teams) | Anyone writing SQL (~millions) |

---

## Pros and Cons

### dbt-column-lineage (what we built)

**Pros**
- Already done and working
- Simpler codebase — easier to maintain
- dbt users get deep integration: model metadata, descriptions, tags, test coverage
- The manifest gives you things you cannot get from SQL alone (model ownership, documentation, test status)

**Cons**
- Useless if you do not use dbt
- Useless if you have not run `dbt compile` recently
- Accuracy drops significantly without `catalog.json`, which requires a live warehouse connection and a separate `dbt docs generate` step most teams skip
- Competes directly with dbt Cloud's own column lineage feature — on dbt Cloud's home turf, targeting dbt Cloud's own paying customers
- Most serious dbt teams already pay for dbt Cloud which has this built in

---

### Standalone SQL Lineage (proposed)

**Pros**
- Works for everyone who writes SQL — not just dbt shops
- No prerequisites — just point at a folder of `.sql` files
- Solves the `catalog.json` problem entirely: cross-file resolution replaces warehouse schema lookups by analyzing the source SQL files themselves
- MCP server makes it genuinely useful day-to-day inside Claude, Cursor, Windsurf, and other AI coding tools
- Nothing like it exists as a simple open source tool with MCP integration
- The dbt-column-lineage library becomes a free bonus feature — one input mode among many, not the whole product

**Cons**
- More work to build (estimated 3–4 weeks vs already done)
- Cross-file reference resolution is harder to get right than reading a pre-built manifest
- Less rich metadata than dbt provides (no test coverage, no model owners, no descriptions unless you add them yourself)

---

## The Brutal Truth

The dbt library as-is has a small addressable audience and competes with dbt Cloud on dbt Cloud's home turf. Most serious dbt teams already pay for dbt Cloud which has column lineage built in.

The standalone version has a massive audience — every data engineer, every analyst writing SQL, every ETL developer — and nothing like it exists as a simple open source tool with MCP integration.

---

## The Good News: Nothing Gets Thrown Away

You do not throw away what was already built. The dbt-column-lineage library becomes roughly 200 lines of adapter code inside the bigger tool.

All the hard work carries over completely:

- SQLGlot SQL analyzer
- Transform classification (passthrough, rename, cast, aggregation, window, etc.)
- NetworkX graph construction and query methods
- The entire test suite

The dbt manifest reader becomes one input adapter alongside plain SQL files, Python ETL scripts, and anything else containing SQL.

---

## What the Standalone Version Looks Like

```bash
# Point at any folder of SQL files
sql-lineage init ./models/

# Point at a dbt project (reads compiled SQL automatically)
sql-lineage init ./dbt-project/target/

# Point at multiple directories
sql-lineage init ./warehouse_scripts/ ./etl_jobs/ ./reports/

# Query from anywhere
sql-lineage trace orders.total_revenue
sql-lineage impact stg_payments.amount

# Open browser visualization
sql-lineage serve
```

And inside Claude Code or Cursor, without typing anything:

> **Claude:** "You are modifying `stg_payments.amount`. This column flows into `orders.total_amount` → `finance_mart.mrr` → `board_reporting.arr`. Do you want me to check those downstream models for breaking changes?"

That is the product. The library is the engine underneath it.

---

## Proposed Architecture

```
sql-lineage/
├── src/sql_lineage/
│   ├── scanner.py        # Walk directory trees, find SQL files
│   ├── extractor.py      # Pull SQL from .py, .ipynb, .yaml files
│   ├── sql_analyzer.py   # SQLGlot per-file column lineage (already built)
│   ├── resolver.py       # Cross-file reference resolution (replaces catalog.json)
│   ├── store.py          # SQLite persistent storage + incremental updates
│   ├── graph.py          # NetworkX graph (already built)
│   ├── watcher.py        # File system watcher for auto-sync
│   ├── mcp_server.py     # MCP server — the killer feature
│   ├── api.py            # Clean Python API
│   └── cli.py            # CLI
```

The piece that eliminates the `catalog.json` dependency is `resolver.py`:

```python
def resolve_table_ref(table_name: str, search_paths: list[Path]) -> Path | None:
    """
    Given a table name referenced in SQL, find the SQL file that defines it.

    stg_orders            → ./models/staging/stg_orders.sql
    public.stg_orders     → ./models/staging/stg_orders.sql
    dev.public.stg_orders → ./models/staging/stg_orders.sql
    """
```

When SQLGlot finds a leaf node pointing at `stg_orders`, instead of stopping and asking for a catalog, the resolver finds `stg_orders.sql`, analyzes it, and substitutes its output columns. No warehouse. No catalog. No dbt.

---

## Comparison to Similar Projects

| Project | What it does | Gap it leaves |
|---|---|---|
| **dbt Cloud column lineage** | Column lineage for dbt, in the browser | Paid, cloud-only, no API, no MCP |
| **SQLMesh** | Full dbt alternative with lineage built in | Requires migrating off dbt entirely |
| **DataHub** | Enterprise data catalog with lineage | Heavy platform, complex to self-host |
| **sqllineage (PyPI)** | Basic SQL lineage extraction | No dbt context, no transform types, no MCP, no persistence |
| **OpenLineage / Marquez** | Runtime lineage (what actually ran) | Requires instrumented pipelines, not static analysis |
| **code-review-graph** | Code change impact analysis for source code | SQL and data unaware |

**The gap:** A lightweight, static-analysis-only, file-system-native SQL column lineage tool with MCP integration. Zero warehouse connection. Zero platform dependency. Works on any SQL.
