"""Shared fixtures for dbt-column-lineage tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"

SIMPLE_MANIFEST_JSON = (FIXTURES_DIR / "simple_manifest.json").read_text(encoding="utf-8")
SIMPLE_CATALOG_JSON = (FIXTURES_DIR / "simple_catalog.json").read_text(encoding="utf-8")


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def simple_manifest_path(tmp_path) -> Path:
    p = tmp_path / "manifest.json"
    p.write_text(SIMPLE_MANIFEST_JSON, encoding="utf-8")
    return p


@pytest.fixture
def simple_catalog_path(tmp_path, simple_manifest_path) -> Path:
    p = tmp_path / "catalog.json"
    p.write_text(SIMPLE_CATALOG_JSON, encoding="utf-8")
    return p


@pytest.fixture
def simple_manifest_data() -> dict:
    return json.loads(SIMPLE_MANIFEST_JSON)


@pytest.fixture
def simple_catalog_data() -> dict:
    return json.loads(SIMPLE_CATALOG_JSON)
