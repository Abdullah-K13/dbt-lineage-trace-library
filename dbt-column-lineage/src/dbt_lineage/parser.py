"""Read and parse manifest.json + catalog.json.

Streaming JSON support
----------------------
For large manifest / catalog files (100 MB+), loading the entire JSON into
memory at once is wasteful.  This module tries to use ``ijson`` for streaming
extraction of only the fields we need.  If ``ijson`` is not installed, it
falls back to the standard ``json.loads`` path — behaviour is identical, only
memory profile differs.

Install the optional dependency with:  pip install ijson
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict, deque
from itertools import chain
from pathlib import Path
from typing import Any

from .exceptions import CatalogParseError, ManifestParseError
from .models import (
    ManifestMetadata,
    ManifestNode,
    ModelInfo,
    ResourceType,
)

try:
    import networkx as nx
    _HAS_NX = True
except ImportError:
    _HAS_NX = False

try:
    import ijson  # type: ignore[import]
    _HAS_IJSON = True
except ImportError:
    _HAS_IJSON = False

logger = logging.getLogger("dbt_lineage")

SUPPORTED_RESOURCE_TYPES = {"model", "source", "seed", "snapshot"}

# Size threshold above which we prefer the streaming path (bytes).
_STREAM_THRESHOLD_BYTES = 20 * 1024 * 1024  # 20 MB


# ── Streaming helpers ─────────────────────────────────────────────────────────

def _stream_manifest(path: Path) -> dict[str, Any]:
    """Stream-read manifest.json and return a minimal dict with only the keys
    needed for lineage analysis.

    Extracted top-level keys: ``metadata``, ``nodes``, ``sources``.
    All other keys (``macros``, ``exposures``, ``metrics``, etc.) are ignored,
    keeping peak memory proportional to the number of nodes rather than the
    total file size.
    """
    wanted_roots = {"metadata", "nodes", "sources"}
    result: dict[str, Any] = {"metadata": {}, "nodes": {}, "sources": {}}

    with open(path, "rb") as fh:
        # ijson.parse() emits (prefix, event, value) triples as it reads.
        # We collect objects under the three top-level keys we care about.
        parser = ijson.items(fh, "", multiple_values=False)
        for obj in parser:
            # ijson.items("") yields the whole document as one Python object —
            # that is not actually streaming.  Use the lower-level kvitems API
            # to get top-level key→value pairs without building the full dict.
            break  # handled below

    # Fall back: ijson.items("") loads everything — use kvitems for real streaming.
    with open(path, "rb") as fh:
        for key, value in ijson.kvitems(fh, ""):
            if key in wanted_roots:
                result[key] = value
            if len(result.get("nodes", {})) + len(result.get("sources", {})) > 0 \
                    and key not in wanted_roots and key not in ("metadata",):
                # Skip large unused top-level keys early
                continue

    return result


def _stream_catalog(path: Path) -> dict[str, Any]:
    """Stream-read catalog.json and return only ``nodes`` and ``sources``."""
    wanted_roots = {"nodes", "sources"}
    result: dict[str, Any] = {"nodes": {}, "sources": {}}

    with open(path, "rb") as fh:
        for key, value in ijson.kvitems(fh, ""):
            if key in wanted_roots:
                result[key] = value

    return result


def _load_json_file(path: Path, stream: bool = False) -> dict[str, Any]:
    """Load a JSON file using streaming (ijson) or standard json.

    Streaming is used when:
    - ``stream=True`` is explicitly requested, OR
    - the file exceeds ``_STREAM_THRESHOLD_BYTES`` AND ijson is available.
    """
    use_stream = _HAS_IJSON and (
        stream or path.stat().st_size >= _STREAM_THRESHOLD_BYTES
    )
    if use_stream:
        logger.debug(f"Streaming JSON from {path.name} ({path.stat().st_size // 1024 // 1024} MB)")
        return None  # sentinel — caller uses the specific stream function
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        raise e


def parse_manifest(
    manifest_path: Path,
) -> tuple[dict[str, ModelInfo], ManifestMetadata]:
    """Parse manifest.json and return a dict of ModelInfo keyed by unique_id,
    plus the manifest metadata (for adapter_type etc.)."""
    try:
        raw_or_none = _load_json_file(manifest_path)
        if raw_or_none is None:
            # Large file — use streaming extractor
            raw = _stream_manifest(manifest_path)
        else:
            raw = raw_or_none
    except (json.JSONDecodeError, OSError) as e:
        raise ManifestParseError(f"Failed to read/parse {manifest_path}: {e}") from e

    # Parse metadata
    metadata_raw = raw.get("metadata", {})
    try:
        metadata = ManifestMetadata.model_validate(metadata_raw)
    except Exception as e:
        logger.warning(f"Could not parse manifest metadata: {e}")
        metadata = ManifestMetadata()

    # Gather all nodes (models, seeds, snapshots) + sources
    all_raw_nodes: dict[str, Any] = {}
    all_raw_nodes.update(raw.get("nodes", {}))
    all_raw_nodes.update(raw.get("sources", {}))

    # ── Pre-scan sources to detect short-name collisions ─────────────────────
    # Sources across different schemas often share the same table name
    # (e.g. p4h.user, pp.user, ma.user in a multi-market dbt project).
    # When a short name is shared by more than one source we qualify it as
    # "schema.table" so every node in the graph has a unique, unambiguous name.
    source_short_name_counts: Counter[str] = Counter(
        node_raw.get("alias") or node_raw.get("name", "")
        for node_raw in all_raw_nodes.values()
        if node_raw.get("resource_type") == "source"
    )

    models: dict[str, ModelInfo] = {}
    name_collision_tracker: dict[str, list[str]] = {}  # name → [unique_ids]

    for unique_id, node_raw in all_raw_nodes.items():
        resource_type_str = node_raw.get("resource_type", "")
        if resource_type_str not in SUPPORTED_RESOURCE_TYPES:
            continue

        try:
            node = ManifestNode.model_validate(node_raw)
        except Exception as e:
            logger.warning(f"Could not parse node '{unique_id}': {e}. Skipping.")
            continue

        try:
            resource_type = ResourceType(resource_type_str)
        except ValueError:
            logger.warning(f"Unknown resource_type '{resource_type_str}' for '{unique_id}'. Skipping.")
            continue

        compiled_sql = node.get_compiled_sql()
        table_alias = node.get_table_alias()

        # For sources whose short name collides across schemas, use "schema.table"
        # so the graph node is unambiguous. Non-colliding sources keep the short
        # name for backward compatibility.
        if (
            resource_type == ResourceType.SOURCE
            and source_short_name_counts.get(table_alias, 0) > 1
            and node.schema_
        ):
            model_name = f"{node.schema_}.{table_alias}"
        else:
            model_name = table_alias

        columns = {
            col_name: col_info.description
            for col_name, col_info in node.columns.items()
        }

        model_info = ModelInfo(
            unique_id=unique_id,
            name=model_name,
            resource_type=resource_type,
            database=node.database or "",
            schema_name=node.schema_ or "",
            compiled_sql=compiled_sql,
            depends_on=node.depends_on.nodes,
            columns=columns,
            original_file_path=node.original_file_path,
        )

        models[unique_id] = model_info

        # Track any remaining collisions (non-source nodes that share a name)
        name_collision_tracker.setdefault(model_name, []).append(unique_id)

    # Warn only on non-source collisions that could not be auto-resolved
    for name, uid_list in name_collision_tracker.items():
        if len(uid_list) > 1:
            logger.warning(
                f"Name collision: multiple nodes share the name '{name}': "
                f"{uid_list}. Queries by name may be ambiguous."
            )

    logger.debug(f"Parsed {len(models)} supported nodes from manifest.")
    return models, metadata


def parse_catalog(catalog_path: Path) -> dict[str, Any]:
    """Parse catalog.json and return the raw dict."""
    try:
        raw_or_none = _load_json_file(catalog_path)
        if raw_or_none is None:
            raw = _stream_catalog(catalog_path)
        else:
            raw = raw_or_none
    except (json.JSONDecodeError, OSError) as e:
        raise CatalogParseError(f"Failed to read/parse {catalog_path}: {e}") from e
    return raw


def build_schema_dict(
    catalog_data: dict[str, Any],
    models: dict[str, ModelInfo],
) -> dict[str, dict[str, str]]:
    """Build a schema dict in the format SQLGlot expects.

    Format:
        {"table_name": {"col_name": "data_type", ...}, ...}

    We use the short model name (alias/name) as the table key because that
    is what appears in the compiled SQL references.
    """
    schema: dict[str, dict[str, str]] = {}

    # Build a mapping from unique_id → short model name
    uid_to_name: dict[str, str] = {uid: m.name for uid, m in models.items()}

    # Flatten nodes + sources into a single iteration
    all_catalog_items = chain(
        catalog_data.get("nodes", {}).items(),
        catalog_data.get("sources", {}).items(),
    )
    for unique_id, node_data in all_catalog_items:
        short_name = uid_to_name.get(unique_id) or unique_id.split(".")[-1]
        col_types = {
            col_name.lower(): (col_info.get("type", "TEXT") if isinstance(col_info, dict) else "TEXT")
            for col_name, col_info in node_data.get("columns", {}).items()
        }
        if col_types:
            schema[short_name] = col_types

    logger.debug(f"Built schema dict with {len(schema)} tables from catalog.")
    return schema


def topological_sort(models: dict[str, ModelInfo]) -> list[str]:
    """Return unique_ids sorted so upstream models come before downstream models.

    Sources and seeds (no depends_on) come first.
    If networkx is unavailable, falls back to a simple iterative sort.
    """
    if _HAS_NX:
        import networkx as nx
        G: nx.DiGraph = nx.DiGraph()
        for uid in models:
            G.add_node(uid)
        for uid, model in models.items():
            for dep_uid in model.depends_on:
                if dep_uid in models:
                    G.add_edge(dep_uid, uid)  # dep must be processed before uid
        try:
            return list(nx.topological_sort(G))
        except nx.NetworkXUnfeasible:
            logger.warning("Cycle detected in dbt DAG — falling back to manifest order.")
            return list(models.keys())

    # Fallback: iterative Kahn's algorithm (no networkx required)
    # Build in-edges (deps) and out-edges (dependents) in one pass — O(n+E)
    deps: dict[str, set[str]] = {
        uid: {d for d in m.depends_on if d in models}
        for uid, m in models.items()
    }
    dependents: dict[str, list[str]] = defaultdict(list)
    for uid, d in deps.items():
        for dep in d:
            dependents[dep].append(uid)

    # deque.popleft() is O(1); list.pop(0) was O(n)
    ready: deque[str] = deque(uid for uid, d in deps.items() if not d)
    order: list[str] = []
    while ready:
        uid = ready.popleft()
        order.append(uid)
        for child in dependents[uid]:          # O(out-degree) not O(n)
            deps[child].discard(uid)
            if not deps[child]:
                ready.append(child)

    # Append anything not reached (handles cycles gracefully)
    order_set = set(order)                     # O(1) membership vs O(n) on list
    remaining = [uid for uid in models if uid not in order_set]
    if remaining:
        logger.warning(f"Could not sort {len(remaining)} nodes topologically — possible cycle.")
        order.extend(remaining)
    return order


def topological_levels(models: dict[str, ModelInfo]) -> list[list[str]]:
    """Group model unique_ids into parallel-safe levels.

    Models within the same level have no dependencies on each other and can
    be analyzed concurrently. Levels must still be processed sequentially so
    that upstream output columns are available in the schema before downstream
    models are analyzed.

    Example DAG:           raw (level 0)
                          /             \\
                       stg_a           stg_b     (level 1 — parallel)
                          \\             /
                            mart        (level 2)

    Returns a list of levels, each level being a list of unique_ids.
    """
    topo = topological_sort(models)

    # Compute depth: max(parent depths) + 1, traversing in topo order
    depths: dict[str, int] = {}
    for uid in topo:
        model = models[uid]
        parent_depths = [
            depths[dep]
            for dep in model.depends_on
            if dep in models and dep in depths
        ]
        depths[uid] = (max(parent_depths) + 1) if parent_depths else 0

    # Group by depth
    groups: dict[int, list[str]] = {}
    for uid, depth in depths.items():
        groups.setdefault(depth, []).append(uid)

    return [groups[d] for d in sorted(groups.keys())]


def filter_models_by_select(
    models: dict[str, ModelInfo],
    select: str | list[str],
    include_downstream: bool = False,
) -> dict[str, ModelInfo]:
    """Filter the model dict to only models in the lineage of the selected names.

    Supports a single name, comma-separated string, or list of names.
    dbt-style +/- prefixes/suffixes are stripped.

    include_downstream=False (default) keeps only ancestors — sufficient for
    trace/lineage queries and much faster on large projects.
    include_downstream=True also keeps all descendants — needed for impact analysis
    that spans beyond the selected model itself.

    Example:
        filter_models_by_select(models, "rep_listing_liquidity")
        filter_models_by_select(models, "+rep_listing_liquidity+")
        filter_models_by_select(models, ["dim_pet", "fact_orders"])
    """
    if isinstance(select, str):
        names: list[str] = [s.strip().strip("+") for s in select.split(",")]
    else:
        names = [s.strip().strip("+") for s in select]
    names = [n for n in names if n]

    # Case-insensitive lookup: build a lowercase → uid map
    name_lower_to_uid: dict[str, str] = {m.name.lower(): uid for uid, m in models.items()}
    seed_uids: set[str] = {
        name_lower_to_uid[n.lower()]
        for n in names
        if n.lower() in name_lower_to_uid
    }

    unmatched = [n for n in names if n.lower() not in name_lower_to_uid]
    if unmatched:
        # Show closest matches to help the user
        all_names = sorted(m.name for m in models.values())
        for u in unmatched:
            similar = [nm for nm in all_names if u.lower() in nm.lower()][:8]
            if similar:
                logger.warning(
                    f"select filter: '{u}' not found. Similar model names: {similar}"
                )
            else:
                logger.warning(
                    f"select filter: '{u}' not found and no similar names. "
                    f"Check g.all_models() for valid names."
                )
    if not seed_uids:
        logger.warning("select filter matched nothing — returning all models")
        return models

    # BFS upstream — collect all ancestors (always needed for schema propagation)
    keep: set[str] = set(seed_uids)
    queue: list[str] = list(seed_uids)
    while queue:
        uid = queue.pop()
        for dep in models[uid].depends_on:
            if dep in models and dep not in keep:
                keep.add(dep)
                queue.append(dep)

    # BFS downstream — only when caller needs impact/downstream analysis
    if include_downstream:
        rev_deps: dict[str, list[str]] = {}
        for uid, m in models.items():
            for dep in m.depends_on:
                rev_deps.setdefault(dep, []).append(uid)

        queue = list(seed_uids)
        while queue:
            uid = queue.pop()
            for child in rev_deps.get(uid, []):
                if child in models and child not in keep:
                    keep.add(child)
                    queue.append(child)

    filtered = {uid: m for uid, m in models.items() if uid in keep}
    logger.info(
        f"select '{', '.join(names)}': keeping {len(filtered)}/{len(models)} models "
        f"({len(models) - len(filtered)} excluded)"
    )
    return filtered


def build_table_lookup(models: dict[str, ModelInfo]) -> dict[str, str]:
    """Build a lookup from various table identifier forms → canonical model name.

    dbt compiled SQL may reference tables as:
      - short name:          stg_orders
      - schema-qualified:    staging.stg_orders
      - fully qualified:     dev.staging.stg_orders
      - qualified source:    p4h.account_holder   (multi-market projects)
      - fully qualified src: owp_dw.p4h.account_holder

    For sources whose short name was qualified to "schema.table" (to resolve
    multi-schema collisions), all three forms map to the qualified name so
    the compiled SQL references still resolve correctly.
    """
    lookup: dict[str, str] = {}
    for model in models.values():
        name = model.name  # may be "p4h.account_holder" or "stg_orders"

        # The canonical name itself (handles both short and qualified forms)
        lookup[name.lower()] = name

        # Use the raw table alias (last segment) for building qualified lookups.
        # This prevents double-qualification like "p4h.p4h.account_holder".
        raw_table = name.rsplit(".", 1)[-1]

        # schema.table → canonical name
        if model.schema_name:
            lookup[f"{model.schema_name}.{raw_table}".lower()] = name

        # database.schema.table → canonical name
        if model.database and model.schema_name:
            lookup[f"{model.database}.{model.schema_name}.{raw_table}".lower()] = name

    return lookup
