"""High-level API for dbt column lineage."""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path

from .dialect import detect_dialect
from .exceptions import ManifestNotFoundError
from .graph import ColumnLineageGraph
from .models import (
    ColumnEdge,
    ColumnRef,
    GraphBuildStats,
    ImpactResult,
    ModelAnalysisResult,
    ResourceType,
    TraceResult,
    TransformType,
)
from .parser import (
    build_schema_dict,
    build_table_lookup,
    filter_models_by_select,
    parse_catalog,
    parse_manifest,
    topological_levels,
)
from .sql_analyzer import analyze_model_columns
from .storage import load_model_result, save_model_result

logger = logging.getLogger("dbt_lineage")


class LineageGraph:
    """Main entry point for dbt column lineage analysis.

    Usage:
        g = LineageGraph("target/manifest.json")
        result = g.trace("orders", "total_with_tax")
    """

    _cache: dict[str, "LineageGraph"] = {}

    def __init__(
        self,
        manifest_path: str | Path,
        catalog_path: str | Path | None = None,
        dialect: str | None = None,
        select: str | list[str] | None = None,
    ) -> None:
        manifest_path = Path(manifest_path)
        if not manifest_path.exists():
            raise ManifestNotFoundError(f"manifest.json not found at {manifest_path}")

        # Auto-discover catalog.json in the same directory
        if catalog_path is None:
            auto_catalog = manifest_path.parent / "catalog.json"
            if auto_catalog.exists():
                catalog_path = auto_catalog
                logger.info(f"Auto-discovered catalog.json at {catalog_path}")
            else:
                logger.warning(
                    "No catalog.json found. Column lineage will use schema propagation "
                    "from upstream models. For best accuracy on SELECT * and complex JOINs, "
                    "run 'dbt docs generate' to produce catalog.json."
                )
        elif catalog_path is not None:
            catalog_path = Path(catalog_path)

        # Check in-memory cache
        cache_key = self._compute_cache_key(manifest_path, catalog_path, dialect, select)
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            self._graph = cached._graph
            self._dialect = cached._dialect
            self._manifest_path = cached._manifest_path
            self._stats = cached._stats
            logger.debug("Using cached lineage graph.")
            return

        # Check SQLite disk cache — skip full rebuild if manifest/catalog unchanged
        self._manifest_path = manifest_path
        disk_cache = manifest_path.parent / f".lineage_{cache_key[:16]}.db"
        from .storage import load_graph, save_graph
        _loaded = load_graph(disk_cache, cache_key)
        if _loaded is not None:
            self._graph, self._stats, _d = _loaded
            self._dialect = _d or dialect
            self._cache[cache_key] = self
            logger.info(f"Loaded lineage graph from SQLite cache ({disk_cache.name}) — delete to rebuild.")
            return

        # ── Parse inputs ──────────────────────────────────────────────────────
        models, metadata = parse_manifest(manifest_path)
        self._dialect = dialect or detect_dialect(metadata.adapter_type)

        if select:
            models = filter_models_by_select(models, select, include_downstream=False)

        catalog_schema: dict[str, dict[str, str]] = {}
        if catalog_path and Path(catalog_path).exists():
            catalog_data = parse_catalog(Path(catalog_path))
            catalog_schema = build_schema_dict(catalog_data, models)
            logger.debug(f"Loaded catalog schema for {len(catalog_schema)} tables.")

        table_lookup = build_table_lookup(models)

        # ── Build graph ───────────────────────────────────────────────────────
        self._graph = ColumnLineageGraph()
        self._stats = GraphBuildStats()

        # Seed cumulative schema from sources + catalog.
        # analysis_schema is a stable snapshot of catalog + source columns used
        # as the schema input for every analyze_model_columns() call.  It grows
        # only with verified data (catalog entries and analysis-derived columns
        # for models that are NOT in the catalog).  Using the full growing
        # cumulative_schema for analysis caused wrong stg-model columns to
        # contaminate the qualify() step for downstream catalog-backed models.
        cumulative_schema: dict[str, dict[str, str]] = {}
        for model in models.values():
            if model.resource_type == ResourceType.SOURCE and model.columns:
                cumulative_schema[model.name] = {
                    col.lower(): catalog_schema.get(model.name, {}).get(col.lower(), "TEXT")
                    for col in model.columns
                }
        for table_name, col_types in catalog_schema.items():
            if table_name not in cumulative_schema:
                cumulative_schema[table_name] = col_types
            else:
                cumulative_schema[table_name].update(col_types)

        # analysis_schema starts as a copy of cumulative_schema (catalog+sources)
        # and is extended only with columns from non-catalog models whose analysis
        # succeeded, so downstream models can resolve those column references.
        catalog_model_names: frozenset[str] = frozenset(cumulative_schema.keys())
        analysis_schema: dict[str, dict[str, str]] = dict(cumulative_schema)

        # ── Process models level by level (serial) ────────────────────────────
        # Levels are processed in topological order so upstream output columns
        # are available in cumulative_schema before downstream models are analyzed.
        # analyze_model_columns() skips Jinja-template SQL instantly, so serial
        # is fast enough (typically under 2 minutes for large projects).
        levels = topological_levels(models)
        self._stats.total_models = len(models)
        total_levels = len(levels)

        for level_idx, level_uids in enumerate(levels):
            for uid in level_uids:
                self._graph.add_model(models[uid])

            to_analyze = [
                uid for uid in level_uids
                if models[uid].resource_type != ResourceType.SOURCE
                and models[uid].compiled_sql
            ]
            skipped = len(level_uids) - len(to_analyze)
            self._stats.models_skipped += skipped

            logger.info(
                f"Level {level_idx + 1}/{total_levels}: "
                f"{len(to_analyze)} to analyze, {skipped} skipped"
            )

            for i, uid in enumerate(to_analyze, 1):
                model = models[uid]

                # ── Per-model incremental cache ───────────────────────────────
                # Compute a short hash of the compiled SQL to detect changes.
                # If the hash matches what is stored in model_cache, reload the
                # previous result instead of re-running the full analysis.
                import hashlib as _hl
                _sql_hash = _hl.sha1(
                    (model.compiled_sql or "").encode(), usedforsecurity=False
                ).hexdigest()[:20]

                _cached_result = load_model_result(disk_cache, uid, _sql_hash)
                if _cached_result is not None:
                    result = _cached_result
                    logger.debug(f"[{model.name}] Loaded from per-model cache (sql unchanged).")
                else:
                    try:
                        result = analyze_model_columns(
                            compiled_sql=model.compiled_sql,
                            model_name=model.name,
                            schema=analysis_schema or None,
                            dialect=self._dialect,
                            table_lookup=table_lookup,
                            catalog_model_names=catalog_model_names,
                        )
                    except Exception as e:
                        logger.warning(f"[{model.name}] Unexpected error: {e}")
                        result = ModelAnalysisResult(edges=[])

                    # ── Retry with catalog-only schema ───────────────────
                    # Analysis-derived schema entries may have incomplete
                    # column lists.  When SELECT * or SELECT * EXCEPT(...)
                    # is expanded against such entries the SQL is corrupted
                    # and the entire analysis fails (0 edges).  Retry with
                    # only catalog-backed schema so the per-column / single-
                    # source fallback paths can work on the original SQL.
                    if (
                        not result.edges
                        and result.columns_attempted > 0
                    ):
                        # Check if any upstream dep has analysis-derived
                        # (non-catalog) schema that may have poisoned star
                        # expansion with incomplete columns.
                        _has_analysis_derived_upstream = any(
                            dep_name in analysis_schema
                            and dep_name not in catalog_model_names
                            for dep in model.depends_on
                            for dep_name in [dep.split(".")[-1]]
                        )
                        if _has_analysis_derived_upstream:
                            # Build a catalog-only schema (remove analysis-
                            # derived entries for this model's upstreams)
                            _catalog_only = {
                                k: v for k, v in analysis_schema.items()
                                if k in catalog_model_names
                            }
                            try:
                                result = analyze_model_columns(
                                    compiled_sql=model.compiled_sql,
                                    model_name=model.name,
                                    schema=_catalog_only or None,
                                    dialect=self._dialect,
                                    table_lookup=table_lookup,
                                    catalog_model_names=catalog_model_names,
                                )
                                if result.edges:
                                    logger.info(
                                        f"[{model.name}] Retry with catalog-only schema "
                                        f"recovered {len(result.edges)} edges."
                                    )
                            except Exception:
                                result = ModelAnalysisResult(edges=[])

                    # Persist so next run can skip this model if SQL unchanged
                    try:
                        save_model_result(disk_cache, uid, _sql_hash, result)
                    except Exception:
                        pass

                if i % 100 == 0:
                    logger.info(f"  ... {i}/{len(to_analyze)} done")

                edges = result.edges

                self._stats.schema_coverage += (
                    1 if model.name in cumulative_schema or
                    any(dep.split(".")[-1] in cumulative_schema for dep in model.depends_on)
                    else 0
                )
                self._stats.columns_attempted += result.columns_attempted
                self._stats.columns_traced += result.columns_traced

                if not edges:
                    self._stats.models_failed += 1
                    self._stats.unresolved_models.append(model.name)
                    logger.debug(f"[{model.name}] No edges extracted.")
                else:
                    self._stats.models_analyzed += 1
                    if result.failed_columns:
                        self._stats.partially_analyzed_models.append(model.name)

                for edge in edges:
                    # Literal sentinel edges keep their model name verbatim —
                    # never resolve through table_lookup.
                    if edge.source.model == "__literal__":
                        resolved_source = "__literal__"
                    else:
                        resolved_source = table_lookup.get(
                            edge.source.model.lower(), edge.source.model
                        )
                    self._graph.add_edge(ColumnEdge(
                        source=ColumnRef(model=resolved_source, column=edge.source.column),
                        target=edge.target,
                        transform_sql=edge.transform_sql,
                        transform_type=edge.transform_type,
                        model_unique_id=model.unique_id,
                        transform_chain=edge.transform_chain,
                        resolution_status=edge.resolution_status,
                    ))

                # Propagate output columns into cumulative schema for downstream levels.
                output_cols: dict[str, str] = {}
                for edge in edges:
                    if edge.target.model != model.name:
                        continue
                    col = edge.target.column.lower()
                    catalog_type = cumulative_schema.get(model.name, {}).get(col)
                    if catalog_type:
                        output_cols[col] = catalog_type
                        continue
                    # Literal sentinel has no real source schema — default to TEXT
                    if edge.source.model == "__literal__":
                        output_cols.setdefault(col, "TEXT")
                        continue
                    resolved_source = table_lookup.get(
                        edge.source.model.lower(), edge.source.model
                    )
                    source_schema = cumulative_schema.get(
                        resolved_source.lower(),
                        cumulative_schema.get(resolved_source, {}),
                    )
                    output_cols.setdefault(col, source_schema.get(edge.source.column.lower(), "TEXT"))

                # Also include ALL attempted output columns (even those that traced
                # to no edges — literals, regexp_extract of literals, etc.).
                # These ARE real output columns of this model (appear in the final
                # SELECT) and must be in the promoted schema so downstream models
                # can resolve SELECT * EXCEPT (...) and UNION ALL alignment correctly.
                for _oc in result.output_column_names:
                    _oc_lower = _oc.lower()
                    if _oc_lower not in output_cols and "." not in _oc_lower:
                        _catalog_type = cumulative_schema.get(model.name, {}).get(_oc_lower)
                        output_cols[_oc_lower] = _catalog_type or "TEXT"

                if output_cols:
                    if model.name in cumulative_schema:
                        cumulative_schema[model.name] = {**output_cols, **cumulative_schema[model.name]}
                    else:
                        cumulative_schema[model.name] = output_cols

                # Promote to analysis_schema so downstream models can expand
                # SELECT * against this model's output columns.
                # Quality gate: >= 3 non-metadata columns ensures we don't
                # promote models that only partially resolved (e.g. only _sdc_
                # or _dbt_ bookkeeping columns surfaced).
                if model.name not in catalog_model_names and output_cols:
                    _sdc_prefixes = ("_sdc_", "_dbt_")
                    non_meta = [
                        c for c in output_cols
                        if not any(c.startswith(p) for p in _sdc_prefixes)
                        and "." not in c
                    ]
                    if len(non_meta) >= 3:
                        if model.name in analysis_schema:
                            analysis_schema[model.name] = {**output_cols, **analysis_schema[model.name]}
                        else:
                            analysis_schema[model.name] = output_cols

        # ── Final stats ───────────────────────────────────────────────────────
        self._stats.total_edges = self._graph._graph.number_of_edges()
        self._stats.total_columns = self._graph._graph.number_of_nodes()

        if self._stats.unresolved_models:
            sample = self._stats.unresolved_models[:20]
            more = len(self._stats.unresolved_models) - len(sample)
            tail = f" (and {more} more)" if more else ""
            logger.warning(
                f"{len(self._stats.unresolved_models)} model(s) produced no edges "
                f"(SELECT * without schema, unparseable SQL, or no FROM clause): "
                f"{sample}{tail}. "
                f"Run 'dbt docs generate' to improve coverage."
            )

        if self._stats.partially_analyzed_models:
            logger.warning(
                f"{len(self._stats.partially_analyzed_models)} model(s) have partial lineage "
                f"(some columns could not be traced): {self._stats.partially_analyzed_models}."
            )

        logger.info(
            f"Built lineage graph: {self._stats.total_columns} columns, "
            f"{self._stats.total_edges} edges across {len(self._graph.all_models())} models "
            f"(model success: {self._stats.success_rate:.0%}, "
            f"column coverage: {self._stats.column_coverage:.0%})"
        )

        # Save to SQLite disk cache so the next run is instant
        try:
            save_graph(disk_cache, self._graph, self._stats, self._dialect, cache_key)
            logger.info(f"Saved lineage graph to SQLite cache ({disk_cache.name})")
        except Exception as _e:
            logger.debug(f"SQLite cache save failed: {_e}")

        self._cache[cache_key] = self

    # ── Cache ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _compute_cache_key(
        manifest_path: Path,
        catalog_path: Path | None,
        dialect: str | None = None,
        select: str | list[str] | None = None,
    ) -> str:
        h = hashlib.sha256()
        with open(manifest_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        if catalog_path and Path(catalog_path).exists():
            with open(catalog_path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
        if dialect:
            h.update(dialect.encode())
        if select:
            key = ",".join(select) if isinstance(select, list) else select
            h.update(key.encode())
        return h.hexdigest()

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the in-memory graph cache."""
        cls._cache.clear()

    # ── Public query API ──────────────────────────────────────────────────────

    def trace(self, model: str, column: str) -> TraceResult:
        """Trace a column upstream to all its data sources."""
        return self._graph.trace_column(model, column)

    def impact(self, model: str, column: str) -> ImpactResult:
        """Find all downstream columns affected by changing this column."""
        return self._graph.impact_column(model, column)

    def edges_between(self, source_model: str, target_model: str) -> list[ColumnEdge]:
        """Get all column-level edges between two models."""
        return self._graph.edges_between(source_model, target_model)

    def model_dependencies(self, model: str) -> list[str]:
        """Get upstream model dependencies for a model."""
        return self._graph.model_dependencies(model)

    def all_models(self) -> list[str]:
        """List all models in the graph."""
        return self._graph.all_models()

    def all_columns(self, model: str) -> list[str]:
        """List all columns for a model that appear in the graph."""
        return self._graph.all_columns(model)

    def search_columns(self, pattern: str) -> list[ColumnRef]:
        """Search for columns by name substring (case-insensitive)."""
        return self._graph.search_columns(pattern)

    def get_transforms_by_type(self, transform_type: TransformType) -> list[ColumnEdge]:
        """Find all edges with a given transform type."""
        return self._graph.get_transforms_by_type(transform_type)

    def stats(self) -> GraphBuildStats:
        """Return build statistics — shows how much of the project was covered."""
        return self._stats

    def to_dict(self) -> dict:
        """Export the full graph as a JSON-serializable dict."""
        d = self._graph.to_dict()
        d["build_stats"] = self._stats.to_dict()
        return d

    def to_networkx(self):
        """Get the raw networkx DiGraph for advanced queries."""
        return self._graph.to_networkx()
