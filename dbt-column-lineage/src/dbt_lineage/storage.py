"""SQLite persistence for the lineage graph — queryable by AI agents."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

from .graph import ColumnLineageGraph
from .models import (
    ColumnEdge,
    ColumnRef,
    GraphBuildStats,
    ModelAnalysisResult,
    ModelInfo,
    ResolutionStatus,
    ResourceType,
    TransformType,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger("dbt_lineage")

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS models (
    unique_id TEXT, name TEXT NOT NULL, resource_type TEXT,
    schema_name TEXT, database_name TEXT
);
CREATE TABLE IF NOT EXISTS edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_model TEXT NOT NULL, source_column TEXT NOT NULL,
    target_model TEXT NOT NULL, target_column TEXT NOT NULL,
    transform_type TEXT, transform_sql TEXT,
    transform_chain TEXT,
    model_unique_id TEXT,
    resolution_status TEXT DEFAULT 'resolved'
);
CREATE INDEX IF NOT EXISTS idx_edge_target ON edges (target_model, target_column);
CREATE INDEX IF NOT EXISTS idx_edge_source ON edges (source_model, source_column);
CREATE TABLE IF NOT EXISTS model_cache (
    unique_id      TEXT PRIMARY KEY,
    sql_hash       TEXT NOT NULL,
    edges_json     TEXT,
    analyzed_at    TEXT,
    columns_attempted  INTEGER DEFAULT 0,
    columns_traced     INTEGER DEFAULT 0,
    failed_columns     TEXT DEFAULT '[]',
    ambiguous_columns  TEXT DEFAULT '[]',
    unresolved_columns TEXT DEFAULT '[]',
    output_column_names TEXT DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_model_cache_hash ON model_cache (sql_hash);
"""

# Migration: add resolution_status column to edges table if missing (for
# databases created before this column was introduced).
_MIGRATE_SQL = """
ALTER TABLE edges ADD COLUMN resolution_status TEXT DEFAULT 'resolved';
"""


def save_graph(
    db_path: Path | str,
    graph: ColumnLineageGraph,
    stats: GraphBuildStats,
    dialect: str | None,
    cache_key: str,
) -> None:
    """Persist a ColumnLineageGraph to a SQLite file.

    Creates (or replaces) the file at db_path. Stores the cache_key and
    dialect in the meta table so load_graph can validate them on reload.
    """
    db_path = Path(db_path)
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(_SCHEMA_SQL)
        # Migrate older DBs that don't have resolution_status on edges
        try:
            con.execute("SELECT resolution_status FROM edges LIMIT 0")
        except sqlite3.OperationalError:
            try:
                con.execute(_MIGRATE_SQL)
                con.commit()
            except Exception:
                pass

        # Wipe existing data so we get a clean write (handles re-saves)
        con.execute("DELETE FROM meta")
        con.execute("DELETE FROM models")
        con.execute("DELETE FROM edges")

        # --- meta ---
        con.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("cache_key", cache_key),
        )
        con.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("dialect", dialect or ""),
        )
        con.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("stats", json.dumps(stats.to_dict())),
        )

        # --- models ---
        model_rows = [
            (
                m.unique_id,
                m.name,
                str(m.resource_type),
                m.schema_name,
                m.database,
            )
            for m in graph._models.values()
        ]
        con.executemany(
            "INSERT INTO models (unique_id, name, resource_type, schema_name, database_name) "
            "VALUES (?, ?, ?, ?, ?)",
            model_rows,
        )

        # --- edges ---
        edge_rows = []
        for source_ref, target_ref, data in graph._graph.edges(data=True):
            edge_rows.append((
                source_ref.model,
                source_ref.column,
                target_ref.model,
                target_ref.column,
                data.get("transform_type", ""),
                data.get("transform_sql", ""),
                json.dumps(data.get("transform_chain", [])),
                data.get("model_unique_id", ""),
                data.get("resolution_status", "resolved"),
            ))
        con.executemany(
            "INSERT INTO edges "
            "(source_model, source_column, target_model, target_column, "
            " transform_type, transform_sql, transform_chain, model_unique_id, "
            " resolution_status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            edge_rows,
        )

        con.commit()
    finally:
        con.close()


def load_graph(
    db_path: Path | str,
    expected_cache_key: str,
) -> "tuple[ColumnLineageGraph, GraphBuildStats, str | None] | None":
    """Load a ColumnLineageGraph from a SQLite file.

    Returns None if:
    - The file does not exist.
    - The stored cache_key does not match expected_cache_key.
    - Any exception occurs during loading.

    On success returns (graph, stats, dialect).
    """
    db_path = Path(db_path)
    if not db_path.exists():
        return None

    try:
        con = sqlite3.connect(str(db_path))
        try:
            con.row_factory = sqlite3.Row

            # --- validate cache key ---
            row = con.execute(
                "SELECT value FROM meta WHERE key = 'cache_key'"
            ).fetchone()
            if row is None or row["value"] != expected_cache_key:
                return None

            # --- read dialect ---
            row = con.execute(
                "SELECT value FROM meta WHERE key = 'dialect'"
            ).fetchone()
            dialect: str | None = row["value"] if row and row["value"] else None

            # --- read stats ---
            row = con.execute(
                "SELECT value FROM meta WHERE key = 'stats'"
            ).fetchone()
            stats = GraphBuildStats()
            if row and row["value"]:
                try:
                    stats_dict = json.loads(row["value"])
                    stats.total_models = stats_dict.get("total_models", 0)
                    stats.models_analyzed = stats_dict.get("models_analyzed", 0)
                    stats.models_skipped = stats_dict.get("models_skipped", 0)
                    stats.models_failed = stats_dict.get("models_failed", 0)
                    stats.total_edges = stats_dict.get("total_edges", 0)
                    stats.total_columns = stats_dict.get("total_columns", 0)
                    stats.schema_coverage = stats_dict.get("schema_coverage", 0)
                    stats.columns_attempted = stats_dict.get("columns_attempted", 0)
                    stats.columns_traced = stats_dict.get("columns_traced", 0)
                    stats.unresolved_models = stats_dict.get("unresolved_models", [])
                    stats.partially_analyzed_models = stats_dict.get(
                        "partially_analyzed_models", []
                    )
                except Exception:
                    pass  # use default stats on JSON parse failure

            # --- rebuild graph ---
            graph = ColumnLineageGraph()

            # Restore models
            for mrow in con.execute(
                "SELECT unique_id, name, resource_type, schema_name, database_name FROM models"
            ):
                try:
                    rt = ResourceType(mrow["resource_type"])
                except ValueError:
                    rt = ResourceType.MODEL
                model_info = ModelInfo(
                    unique_id=mrow["unique_id"] or "",
                    name=mrow["name"],
                    resource_type=rt,
                    schema_name=mrow["schema_name"] or "",
                    database=mrow["database_name"] or "",
                )
                graph.add_model(model_info)

            # Migrate if resolution_status column is missing
            try:
                con.execute("SELECT resolution_status FROM edges LIMIT 0")
            except sqlite3.OperationalError:
                try:
                    con.execute(_MIGRATE_SQL)
                    con.commit()
                except Exception:
                    pass

            # Restore edges
            for erow in con.execute(
                "SELECT source_model, source_column, target_model, target_column, "
                "transform_type, transform_sql, transform_chain, model_unique_id, "
                "resolution_status "
                "FROM edges"
            ):
                try:
                    tt = TransformType(erow["transform_type"])
                except (ValueError, KeyError):
                    tt = TransformType.UNKNOWN

                try:
                    chain = json.loads(erow["transform_chain"] or "[]")
                except Exception:
                    chain = []

                try:
                    rs = ResolutionStatus(erow["resolution_status"] or "resolved")
                except (ValueError, KeyError):
                    rs = ResolutionStatus.RESOLVED

                edge = ColumnEdge(
                    source=ColumnRef(
                        model=erow["source_model"],
                        column=erow["source_column"],
                    ),
                    target=ColumnRef(
                        model=erow["target_model"],
                        column=erow["target_column"],
                    ),
                    transform_type=tt,
                    transform_sql=erow["transform_sql"] or "",
                    transform_chain=chain,
                    model_unique_id=erow["model_unique_id"] or "",
                    resolution_status=rs,
                )
                graph.add_edge(edge)

            return graph, stats, dialect

        finally:
            con.close()

    except Exception as exc:
        logger.debug(f"SQLite cache load failed: {exc}")
        return None


# ── Per-model incremental cache ───────────────────────────────────────────────

def save_model_result(
    db_path: Path | str,
    unique_id: str,
    sql_hash: str,
    result: "ModelAnalysisResult",
) -> None:
    """Persist a single model's lineage result keyed by its SQL hash.

    Called after analyzing each model so subsequent runs can skip re-analysis
    if the compiled SQL has not changed.
    """
    import datetime

    db_path = Path(db_path)
    if not db_path.exists():
        return  # DB not initialised yet — skip silently

    edges_data = [
        {
            "source_model": e.source.model,
            "source_column": e.source.column,
            "target_model": e.target.model,
            "target_column": e.target.column,
            "transform_type": str(e.transform_type),
            "transform_sql": e.transform_sql,
            "transform_chain": e.transform_chain,
            "model_unique_id": e.model_unique_id,
            "resolution_status": str(e.resolution_status) if e.resolution_status else "resolved",
        }
        for e in result.edges
    ]

    try:
        con = sqlite3.connect(str(db_path))
        try:
            con.executescript(_SCHEMA_SQL)
            con.execute(
                """
                INSERT OR REPLACE INTO model_cache
                  (unique_id, sql_hash, edges_json, analyzed_at,
                   columns_attempted, columns_traced,
                   failed_columns, ambiguous_columns, unresolved_columns,
                   output_column_names)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    unique_id,
                    sql_hash,
                    json.dumps(edges_data),
                    datetime.datetime.utcnow().isoformat(),
                    result.columns_attempted,
                    result.columns_traced,
                    json.dumps(result.failed_columns),
                    json.dumps(result.ambiguous_columns),
                    json.dumps(result.unresolved_columns),
                    json.dumps(result.output_column_names),
                ),
            )
            con.commit()
        finally:
            con.close()
    except Exception as exc:
        logger.debug(f"model_cache save failed for {unique_id}: {exc}")


def load_model_result(
    db_path: Path | str,
    unique_id: str,
    sql_hash: str,
) -> "ModelAnalysisResult | None":
    """Load a cached ModelAnalysisResult if the SQL hash matches.

    Returns None if:
    - The DB file does not exist.
    - No entry for unique_id, or the stored hash does not match sql_hash.
    - Any exception during loading.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        return None

    try:
        con = sqlite3.connect(str(db_path))
        try:
            con.row_factory = sqlite3.Row
            # Ensure model_cache table exists (may be an older DB)
            con.execute(
                "CREATE TABLE IF NOT EXISTS model_cache ("
                "unique_id TEXT PRIMARY KEY, sql_hash TEXT NOT NULL, "
                "edges_json TEXT, analyzed_at TEXT, "
                "columns_attempted INTEGER DEFAULT 0, columns_traced INTEGER DEFAULT 0, "
                "failed_columns TEXT DEFAULT '[]', "
                "ambiguous_columns TEXT DEFAULT '[]', "
                "unresolved_columns TEXT DEFAULT '[]', "
                "output_column_names TEXT DEFAULT '[]')"
            )
            # Add output_column_names column if missing (upgrade older DBs)
            try:
                con.execute("ALTER TABLE model_cache ADD COLUMN output_column_names TEXT DEFAULT '[]'")
                con.commit()
            except Exception:
                pass
            row = con.execute(
                "SELECT * FROM model_cache WHERE unique_id = ? AND sql_hash = ?",
                (unique_id, sql_hash),
            ).fetchone()
            if row is None:
                return None

            edges_data = json.loads(row["edges_json"] or "[]")
            edges: list[ColumnEdge] = []
            for ed in edges_data:
                try:
                    tt = TransformType(ed.get("transform_type", "unknown"))
                except ValueError:
                    tt = TransformType.UNKNOWN
                try:
                    rs = ResolutionStatus(ed.get("resolution_status", "resolved"))
                except ValueError:
                    rs = ResolutionStatus.RESOLVED
                edges.append(ColumnEdge(
                    source=ColumnRef(model=ed["source_model"], column=ed["source_column"]),
                    target=ColumnRef(model=ed["target_model"], column=ed["target_column"]),
                    transform_type=tt,
                    transform_sql=ed.get("transform_sql", ""),
                    transform_chain=ed.get("transform_chain", []),
                    model_unique_id=ed.get("model_unique_id", ""),
                    resolution_status=rs,
                ))

            return ModelAnalysisResult(
                edges=edges,
                columns_attempted=row["columns_attempted"] or 0,
                columns_traced=row["columns_traced"] or 0,
                failed_columns=json.loads(row["failed_columns"] or "[]"),
                ambiguous_columns=json.loads(row["ambiguous_columns"] or "[]"),
                unresolved_columns=json.loads(row["unresolved_columns"] or "[]"),
                output_column_names=json.loads(row["output_column_names"] or "[]"),
            )
        finally:
            con.close()
    except Exception as exc:
        logger.debug(f"model_cache load failed for {unique_id}: {exc}")
        return None
