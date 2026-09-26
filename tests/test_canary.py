"""Unit tests for the canary's pass/fail evaluation (run_canary.evaluate_canary). No Databricks access."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "netsuite_ingestion" / "canary"))

from run_canary import evaluate_canary  # noqa: E402

BLOCKED = "BLOCKED: update x was stopped before it changed any data. ... bronze_rebuild=true ..."


def healthy():
    return {
        "normal_1": {"state": "COMPLETED", "error": "", "rows": 1},
        "full_refresh": {"state": "FAILED", "error": BLOCKED, "rows": 1},
        "selective_refresh": {"state": "FAILED", "error": BLOCKED, "rows": 1},
        "normal_2": {"state": "COMPLETED", "error": "", "rows": 1},
    }


def failures(runs):
    return evaluate_canary(runs)[0]


def test_healthy_platform_passes():
    assert evaluate_canary(healthy()) == ([], [])


def test_a_full_refresh_that_completes_means_the_guard_did_not_stop_it():
    runs = healthy()
    runs["full_refresh"] = {"state": "COMPLETED", "error": "", "rows": 1}
    (failure,) = failures(runs)
    assert "full_refresh" in failure and "DID NOT STOP" in failure


def test_a_selective_refresh_that_completes_is_reported():
    runs = healthy()
    runs["selective_refresh"] = {"state": "COMPLETED", "error": "", "rows": 1}
    assert any("selective_refresh" in f and "DID NOT STOP" in f for f in failures(runs))


def test_a_failure_without_the_blocked_message_is_not_accepted():
    runs = healthy()
    runs["full_refresh"] = {"state": "FAILED", "error": "some unrelated error", "rows": 1}
    (failure,) = failures(runs)
    assert "not with the guard's BLOCKED message" in failure


def test_data_changed_by_a_blocked_refresh_is_reported():
    runs = healthy()
    runs["full_refresh"]["rows"] = 0
    (failure,) = failures(runs)
    assert "0 rows" in failure and "data changed" in failure


def test_a_blocked_normal_update_is_reported_with_the_likely_cause():
    runs = healthy()
    runs["normal_1"] = {"state": "FAILED", "error": "cannot read this update's refresh mode", "rows": None}
    (failure,) = failures(runs)
    assert "normal_1" in failure and "event log" in failure


def test_recovery_run_must_also_complete():
    runs = healthy()
    runs["normal_2"] = {"state": "FAILED", "error": BLOCKED, "rows": 1}
    assert any("normal_2" in f for f in failures(runs))


def test_a_missing_run_is_reported():
    runs = healthy()
    del runs["selective_refresh"]
    assert any("selective_refresh" in f and "did not run" in f for f in failures(runs))


def test_a_fail_closed_block_is_a_warning_not_a_failure():
    runs = healthy()
    runs["full_refresh"] = {"state": "FAILED", "error": "BLOCKED: update x ... refresh mode could not be read from the pipeline event log ...", "rows": 1}
    fails, warns = evaluate_canary(runs)
    assert fails == [] and len(warns) == 1 and "fail-closed" in warns[0] and "full_refresh" in warns[0]


def test_a_fail_closed_block_that_changed_data_is_still_a_failure():
    runs = healthy()
    runs["selective_refresh"] = {"state": "FAILED", "error": "BLOCKED: ... could not be read ...", "rows": 0}
    fails, _ = evaluate_canary(runs)
    assert any("data changed" in f for f in fails)
