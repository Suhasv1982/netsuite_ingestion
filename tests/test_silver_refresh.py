"""The silver-only refresh: the job task (silver only, never bronze) and the before/after comparison (pure)."""

import pathlib
import sys
import types

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "evals"))
sys.path.insert(0, str(ROOT / "tools"))

import silver_refresh as job  # noqa: E402
import silver_refresh_demo as demo  # noqa: E402


def test_only_silver_is_fully_refreshed_and_never_bronze():
    assert job.check_tables(["poc_silver.a"], ["poc_reject.rejected_rows", "poc_gold.g"]) == (
        ["poc_silver.a"], ["poc_reject.rejected_rows", "poc_gold.g"])
    for tables, also in ((["poc_bronze.a"], []), ([], []), (["poc_silver.a"], ["poc_bronze.a"]),
                         (["poc_silver.a"], ["poc_silver.b"])):
        with pytest.raises(SystemExit, match="poc_silver"):
            job.check_tables(tables, also)


def test_refresh_starts_one_selective_update_and_waits():
    calls = []
    states = iter(["RUNNING", "COMPLETED"])

    class P:
        def start_update(self, pid, full_refresh_selection=None, refresh_selection=None):
            calls.append((pid, full_refresh_selection, refresh_selection))
            return types.SimpleNamespace(update_id="u1")

        def get_update(self, pid, uid):
            return types.SimpleNamespace(update=types.SimpleNamespace(state=types.SimpleNamespace(value=next(states))))

    w = types.SimpleNamespace(pipelines=P())
    assert job.refresh(w, "p", ["poc_silver.a"], ["poc_gold.g"], poll_s=0, log=lambda m: None) == "COMPLETED"
    assert calls == [("p", ["poc_silver.a"], ["poc_gold.g"])]


def test_the_bundle_job_refreshes_credentials_first_and_names_no_bronze_table():
    dev = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))["targets"]["dev"]
    j = dev["resources"]["jobs"]["silver_refresh"]
    assert [t["task_key"] for t in j["tasks"]] == ["refresh_credentials", "refresh_silver"]
    assert j["tasks"][1]["depends_on"] == [{"task_key": "refresh_credentials"}]
    params = j["tasks"][1]["spark_python_task"]["parameters"]
    assert "poc_bronze" not in " ".join(params) and "schedule" not in j


SNAP = {"bronze": {"m": {"rows": 10, "checksum": "7"}}, "silver": {"m": 8}, "gold": {"g": 3}, "rejects": {"m": 1},
        "rules": [{"rule_id": 11, "table": "m", "rule_name": "Key", "proposal_id": 14, "silver_violations": 2,
                   "reject_rows": 2},
                  {"rule_id": 1, "table": "m", "rule_name": "Old", "proposal_id": None, "silver_violations": 0,
                   "reject_rows": 1}],
        "taken_at": "t0", "date": "2026-10-15"}


def test_comparison_passes_when_bronze_is_unchanged_and_violations_left_silver():
    after = {**SNAP, "silver": {"m": 6}, "taken_at": "t1",
             "rules": [{**SNAP["rules"][0], "silver_violations": 0}, SNAP["rules"][1]]}
    c = demo.compare(SNAP, after)
    assert c["bronze_unchanged"] and c["violating_rows_moved"]
    [new, old] = c["rules"]
    assert new["moved"] is True and old["moved"] is None                 # only rules from proposals are judged
    md = demo.render(SNAP, after, c)
    assert "Bronze unchanged (rows and content checksum per table): **PASS**" in md
    assert "| silver | m | 8 | 6 |" in md and "| 11 Key | m | 14 | 2 | 0 | 2 | 2 | PASS |" in md


def test_comparison_fails_if_bronze_changed_or_violations_stayed():
    changed = {**SNAP, "bronze": {"m": {"rows": 11, "checksum": "9"}}}
    assert not demo.compare(SNAP, changed)["bronze_unchanged"]
    stayed = demo.compare(SNAP, SNAP)
    assert not stayed["violating_rows_moved"] and stayed["rules"][0]["moved"] is False
