"""Shape of the incident fixtures in evals/incidents/ (ground truth for the Phase 4 agent)."""

import datetime as dt
import pathlib
import re

import pytest
import yaml

DIR = pathlib.Path(__file__).resolve().parents[1] / "evals" / "incidents"
FILES = sorted(DIR.glob("*.yaml"))
REQUIRED = {"id", "title", "detected", "occurred", "environment", "category", "severity", "symptoms", "evidence",
            "root_cause", "not_the_cause", "fix", "grading"}


def test_there_are_fixtures():
    assert FILES


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.stem)
def test_fixture_shape(path):
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert REQUIRED <= set(doc), REQUIRED - set(doc)
    assert doc["id"] == path.stem
    assert path.stem.startswith(str(doc["occurred"][0]))
    assert isinstance(doc["detected"], dt.date)
    assert doc["environment"] in ("dev", "prod")
    assert doc["symptoms"] and doc["root_cause"].strip()
    assert all({"step", "query", "finding"} <= set(e) for e in doc["evidence"])
    assert {"must_identify", "must_not_claim", "tools_expected"} <= set(doc["grading"])
    assert doc["grading"]["must_identify"]


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.stem)
def test_fixture_has_no_environment_identifiers(path):
    text = path.read_text(encoding="utf-8")
    assert not re.search(r"https?://|@[\w-]+\.(com|net|org)|\.cloud\.databricks\.com|database\.\w+\.cloud", text)
    assert not re.search(r"\b\d{12,16}\b", text), "looks like a job or run id"
