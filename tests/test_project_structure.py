"""Cheap guards that run without a Databricks workspace."""

import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_pipeline_and_job_sources_parse():
    files = list((ROOT / "src").rglob("*.py"))
    assert files
    for path in files:
        ast.parse(path.read_text(), filename=str(path))


def test_bundle_has_all_deployment_targets():
    text = (ROOT / "databricks.yml").read_text()
    for target in ("dev:", "staging:", "prod:"):
        assert target in text
