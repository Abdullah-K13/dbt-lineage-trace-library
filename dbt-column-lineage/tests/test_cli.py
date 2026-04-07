"""Tests for the CLI."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from dbt_lineage import LineageGraph
from dbt_lineage.cli import cli


@pytest.fixture(autouse=True)
def clear_cache():
    LineageGraph.clear_cache()
    yield
    LineageGraph.clear_cache()


@pytest.fixture
def runner():
    return CliRunner()


class TestStatsCLI:
    def test_stats_command(self, runner, simple_manifest_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "stats",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert "graph" in data
        assert "build" in data
        assert data["graph"]["total_models"] > 0
        assert data["build"]["models_analyzed"] > 0

    def test_stats_with_catalog(self, runner, simple_manifest_path, simple_catalog_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "--catalog", str(simple_catalog_path),
            "stats",
        ])
        assert result.exit_code == 0, result.output


class TestListModelsCLI:
    def test_list_models(self, runner, simple_manifest_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "list-models",
        ])
        assert result.exit_code == 0, result.output
        lines = result.output.strip().splitlines()
        assert "stg_orders" in lines
        assert "orders" in lines


class TestListColumnsCLI:
    def test_list_columns(self, runner, simple_manifest_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "list-columns", "orders",
        ])
        assert result.exit_code == 0, result.output
        lines = result.output.strip().splitlines()
        assert len(lines) > 0


class TestTraceCLI:
    def test_trace_command(self, runner, simple_manifest_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "trace", "orders", "order_id",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert "target" in data
        assert "edges" in data
        assert "source_columns" in data

    def test_trace_target_correct(self, runner, simple_manifest_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "trace", "orders", "order_id",
        ])
        data = json.loads(result.output)
        assert data["target"] == "orders.order_id"


class TestImpactCLI:
    def test_impact_command(self, runner, simple_manifest_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "impact", "stg_orders", "order_id",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert "source" in data
        assert "affected_columns" in data
        assert "affected_models" in data

    def test_impact_finds_downstream_model(self, runner, simple_manifest_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "impact", "stg_orders", "order_id",
        ])
        data = json.loads(result.output)
        assert "orders" in data["affected_models"]


class TestExportCLI:
    def test_export_to_stdout(self, runner, simple_manifest_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "export",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert "stats" in data
        assert "edges" in data

    def test_export_to_file(self, runner, simple_manifest_path, tmp_path):
        out_file = tmp_path / "lineage.json"
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "export", "--output", str(out_file),
        ])
        assert result.exit_code == 0, result.output
        assert out_file.exists()
        data = json.loads(out_file.read_text())
        assert "stats" in data


class TestVerboseFlag:
    def test_verbose_flag_accepted(self, runner, simple_manifest_path):
        result = runner.invoke(cli, [
            "--manifest", str(simple_manifest_path),
            "--verbose",
            "stats",
        ])
        assert result.exit_code == 0, result.output
