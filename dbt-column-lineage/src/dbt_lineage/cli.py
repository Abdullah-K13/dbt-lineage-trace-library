"""CLI for dbt-column-lineage."""

from __future__ import annotations

import json
import logging

import click

from .api import LineageGraph
from .models import TransformType


@click.group()
@click.option("--manifest", "-m", default="target/manifest.json", help="Path to manifest.json")
@click.option("--catalog", "-c", default=None, help="Path to catalog.json")
@click.option("--dialect", "-d", default=None, help="SQL dialect override")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx, manifest, catalog, dialect, verbose):
    """dbt Column Lineage — trace column transformations across your dbt project."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )
    ctx.ensure_object(dict)
    ctx.obj["manifest"] = manifest
    ctx.obj["catalog"] = catalog
    ctx.obj["dialect"] = dialect


def _get_graph(ctx) -> LineageGraph:
    return LineageGraph(
        ctx.obj["manifest"],
        ctx.obj.get("catalog"),
        ctx.obj.get("dialect"),
    )


@cli.command()
@click.argument("model")
@click.argument("column")
@click.pass_context
def trace(ctx, model, column):
    """Trace a column upstream to its sources."""
    g = _get_graph(ctx)
    result = g.trace(model, column)
    click.echo(json.dumps({
        "target": str(result.target),
        "source_columns": [str(c) for c in result.source_columns],
        "source_models": result.source_models,
        "edges": [e.to_dict() for e in result.edges],
    }, indent=2))


@cli.command()
@click.argument("model")
@click.argument("column")
@click.pass_context
def impact(ctx, model, column):
    """Find downstream impact of changing a column."""
    g = _get_graph(ctx)
    result = g.impact(model, column)
    click.echo(json.dumps({
        "source": str(result.source),
        "affected_columns": [str(c) for c in result.affected_columns],
        "affected_models": result.affected_models,
        "edges": [e.to_dict() for e in result.edges],
    }, indent=2))


@cli.command()
@click.pass_context
def stats(ctx):
    """Show graph statistics and build coverage."""
    g = _get_graph(ctx)
    data = g.to_dict()
    output = {
        "graph": data["stats"],
        "build": data.get("build_stats", {}),
    }
    click.echo(json.dumps(output, indent=2))


@cli.command()
@click.option("--output", "-o", default=None, help="Output file path")
@click.pass_context
def export(ctx, output):
    """Export the full lineage graph as JSON."""
    g = _get_graph(ctx)
    data = g.to_dict()
    if output:
        with open(output, "w") as f:
            json.dump(data, f, indent=2)
        click.echo(f"Exported to {output}")
    else:
        click.echo(json.dumps(data, indent=2))


@cli.command("list-models")
@click.pass_context
def list_models(ctx):
    """List all models in the graph."""
    g = _get_graph(ctx)
    for model in g.all_models():
        click.echo(model)


@cli.command("list-columns")
@click.argument("model")
@click.pass_context
def list_columns(ctx, model):
    """List all columns for a model."""
    g = _get_graph(ctx)
    for col in g.all_columns(model):
        click.echo(col)
