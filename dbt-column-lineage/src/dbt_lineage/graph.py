"""NetworkX graph construction and query methods for column lineage."""

from __future__ import annotations

from collections import defaultdict

import networkx as nx

from .exceptions import ColumnNotFoundError
from .models import (
    ColumnEdge,
    ColumnRef,
    ImpactResult,
    ModelInfo,
    TraceResult,
    TransformType,
)


class ColumnLineageGraph:
    """Directed graph of column-to-column lineage.

    Nodes: ColumnRef objects (frozen dataclass, hashable)
    Edges: Directed from source column → target column, with transform metadata.
    """

    def __init__(self) -> None:
        self._graph: nx.DiGraph = nx.DiGraph()
        self._models: dict[str, ModelInfo] = {}  # short name → ModelInfo
        self._edges_by_target: dict[ColumnRef, list[ColumnEdge]] = defaultdict(list)

    def add_model(self, model: ModelInfo) -> None:
        """Register a model's metadata."""
        self._models[model.name] = model

    def _resolve_ref(self, model: str, column: str) -> ColumnRef:
        """Find a ColumnRef by model + column, case-insensitively.

        dbt adapters like Snowflake uppercase all identifiers. This lets callers
        use any case — 'daily_revenue', 'DAILY_REVENUE', 'Daily_Revenue' all work.
        """
        exact = ColumnRef(model=model, column=column)
        if exact in self._graph:
            return exact

        model_lower = model.lower()
        column_lower = column.lower()
        for node in self._graph.nodes:
            if node.model.lower() == model_lower and node.column.lower() == column_lower:
                return node

        raise ColumnNotFoundError(
            f"Column '{model}.{column}' not found in graph. "
            f"Available columns for this model: {self.all_columns(model) or '(model not found)'}"
        )

    def add_edge(self, edge: ColumnEdge) -> None:
        """Add a column lineage edge to the graph."""
        self._graph.add_node(
            edge.source,
            model=edge.source.model,
            column=edge.source.column,
        )
        self._graph.add_node(
            edge.target,
            model=edge.target.model,
            column=edge.target.column,
        )
        self._graph.add_edge(
            edge.source,
            edge.target,
            transform_sql=edge.transform_sql,
            transform_type=str(edge.transform_type),
            model_unique_id=edge.model_unique_id,
            transform_chain=edge.transform_chain,
            resolution_status=str(edge.resolution_status) if edge.resolution_status else "resolved",
        )
        self._edges_by_target[edge.target].append(edge)

    def _edges_sorted(self, subgraph: nx.DiGraph) -> list[ColumnEdge]:
        """Return edges from a subgraph sorted in topological order — raw first, mart last.

        Topological order on the subgraph guarantees that for any edge A→B,
        A always appears before B. Combined with source ranking, this gives
        a clean raw→staging→mart reading order.
        """
        try:
            topo_nodes = list(nx.topological_sort(subgraph))
        except nx.NetworkXUnfeasible:
            topo_nodes = list(subgraph.nodes)

        node_rank = {n: i for i, n in enumerate(topo_nodes)}

        edges: list[ColumnEdge] = []
        for u, v, data in subgraph.edges(data=True):
            edges.append(ColumnEdge(
                source=u,
                target=v,
                transform_sql=data["transform_sql"],
                transform_type=TransformType(data["transform_type"]),
                model_unique_id=data.get("model_unique_id", ""),
                transform_chain=data.get("transform_chain", []),
            ))

        edges.sort(key=lambda e: node_rank.get(e.source, 0))
        return edges

    def trace_column(self, model: str, column: str) -> TraceResult:
        """Trace a column upstream to all its sources.

        Uses BFS backwards through the graph.
        Returns all upstream source columns and the edges along the way.
        Column name matching is case-insensitive.
        """
        target = self._resolve_ref(model, column)

        ancestors = nx.ancestors(self._graph, target)
        ancestors.add(target)

        subgraph = self._graph.subgraph(ancestors)
        edges = self._edges_sorted(subgraph)

        source_columns = [n for n in ancestors if self._graph.in_degree(n) == 0]
        source_models = list(set(c.model for c in source_columns))

        return TraceResult(
            target=target,
            source_columns=source_columns,
            source_models=source_models,
            edges=edges,
        )

    def impact_column(self, model: str, column: str) -> ImpactResult:
        """Find all downstream columns affected by a change to this column.

        Uses BFS forward through the graph.
        Column name matching is case-insensitive.
        """
        source = self._resolve_ref(model, column)

        descendants = nx.descendants(self._graph, source)

        subgraph = self._graph.subgraph(descendants | {source})
        edges = self._edges_sorted(subgraph)

        affected_columns = list(descendants)
        affected_models = list(set(c.model for c in descendants))

        return ImpactResult(
            source=source,
            affected_columns=affected_columns,
            affected_models=affected_models,
            edges=edges,
        )

    def edges_between(self, source_model: str, target_model: str) -> list[ColumnEdge]:
        """Get all column edges flowing from one model to another."""
        result: list[ColumnEdge] = []
        for u, v, data in self._graph.edges(data=True):
            if u.model == source_model and v.model == target_model:
                result.append(ColumnEdge(
                    source=u,
                    target=v,
                    transform_sql=data["transform_sql"],
                    transform_type=TransformType(data["transform_type"]),
                    model_unique_id=data.get("model_unique_id", ""),
                ))
        return result

    def model_dependencies(self, model: str) -> list[str]:
        """Get all models that feed into the given model (model-level view)."""
        models: set[str] = set()
        for node in self._graph.nodes:
            if node.model == model:
                for pred in self._graph.predecessors(node):
                    if pred.model != model:
                        models.add(pred.model)
        return sorted(models)

    def all_columns(self, model: str) -> list[str]:
        """Get all known columns for a model (those that appear in the graph)."""
        return sorted(set(
            n.column for n in self._graph.nodes if n.model == model
        ))

    def all_models(self) -> list[str]:
        """Get all model names in the graph."""
        return sorted(set(n.model for n in self._graph.nodes))

    def search_columns(self, pattern: str) -> list[ColumnRef]:
        """Search for columns by name (case-insensitive substring match)."""
        pattern_lower = pattern.lower()
        return [n for n in self._graph.nodes if pattern_lower in n.column.lower()]

    def get_transforms_by_type(self, transform_type: TransformType) -> list[ColumnEdge]:
        """Find all edges with a specific transform type."""
        result: list[ColumnEdge] = []
        for u, v, data in self._graph.edges(data=True):
            if data.get("transform_type") == str(transform_type):
                result.append(ColumnEdge(
                    source=u,
                    target=v,
                    transform_sql=data["transform_sql"],
                    transform_type=transform_type,
                    model_unique_id=data.get("model_unique_id", ""),
                ))
        return result

    def to_dict(self) -> dict:
        """Export the entire graph as a JSON-serializable dict."""
        return {
            "models": list(self.all_models()),
            "nodes": [{"model": n.model, "column": n.column} for n in self._graph.nodes],
            "edges": [
                {
                    "source": str(u),
                    "target": str(v),
                    "transform_sql": data["transform_sql"],
                    "transform_type": data["transform_type"],
                }
                for u, v, data in self._graph.edges(data=True)
            ],
            "stats": {
                "total_models": len(self.all_models()),
                "total_columns": self._graph.number_of_nodes(),
                "total_edges": self._graph.number_of_edges(),
            },
        }

    def to_networkx(self) -> nx.DiGraph:
        """Return the raw networkx graph for advanced queries."""
        return self._graph
