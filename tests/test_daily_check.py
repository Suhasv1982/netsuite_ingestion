"""tools/daily_check.py: the pure parts (run selection by UTC date, log classification, count and audit findings)."""

import datetime as dt

from daily_check import audit_findings, classify_generator_log, count_gaps, same_day_version_lines, runs_on_date

D = dt.date(2026, 10, 4)


def _ms(y, m, d, h=5):
    return int(dt.datetime(y, m, d, h, tzinfo=dt.timezone.utc).timestamp() * 1000)


def test_runs_on_date_uses_the_utc_start_day():
    runs = [{"run_id": 2, "start_time": _ms(2026, 10, 4, 5)}, {"run_id": 1, "start_time": _ms(2026, 10, 3, 23)},
            {"run_id": 3, "start_time": _ms(2026, 10, 4, 0)}, {"run_id": 4}]
    assert [r["run_id"] for r in runs_on_date(runs, D)] == [3, 2]


def test_generator_log_classification():
    assert classify_generator_log("SKIPPED: --batch-date 2026-10-02 is not later than ...") == "skipped"
    assert classify_generator_log("increment seed=7 rows: customers=100, memberships=397") == "increment"
    assert classify_generator_log("Traceback ...") == "unknown"


SRC = {("netsuite_customers", None): 10, ("netsuite_transactions", "2026-07-11"): 50,
       ("netsuite_transactions", "2026-10-02"): 40}


def test_count_gaps():
    assert count_gaps(SRC, dict(SRC)) == []
    assert count_gaps(SRC, {**SRC, ("netsuite_transactions", "2026-10-02"): 37}) == [
        "netsuite_transactions@2026-10-02: source=40 dev=37"]
    assert count_gaps(SRC, {**SRC, ("netsuite_customers", None): 9}) == ["netsuite_customers: source=10 dev=9"]
    assert count_gaps(SRC, {**SRC, ("netsuite_transactions", "2026-10-09"): 1}) == [
        "netsuite_transactions@2026-10-09: source=0 dev=1"]


def test_a_date_short_by_its_same_day_versions_is_a_failure_now():
    # before the row-hash fix this was a known-defect WARN (incident 2026-10-02); every version is loaded now
    assert count_gaps(SRC, {**SRC, ("netsuite_transactions", "2026-10-02"): 37}) != []


def test_same_day_version_lines_are_info_with_the_keys():
    lines = same_day_version_lines({("netsuite_transactions", "2026-10-02"): [12, 3],
                                    ("netsuite_memberships", "2026-10-03"): list(range(12))})
    assert lines == [
        "memberships@2026-10-03: 12 key(s) with several versions: 0, 1, 10, 11, 2, 3, 4, 5, 6, 7 (+2 more)",
        "transactions@2026-10-02: 2 key(s) with several versions: 12, 3"]
    assert same_day_version_lines({}) == []


def _audit(ledger="OK", guard="OK", reads=1, failed=False):
    rows = [{"table": t, "layer": "ledger", "status": ledger, "rows_read": 1}
            for t in ("netsuite_memberships", "netsuite_certifications", "netsuite_transactions",
                      "netsuite_transaction_lines")]
    rows.append({"table": "(pipeline)", "layer": "guard", "status": guard, "rows_read": reads})
    if failed:
        rows.append({"table": "netsuite_transactions", "layer": "silver", "status": "FAILED", "rows_read": None})
    return rows


def test_clean_run_has_no_findings():
    assert audit_findings(_audit()) == ([], [])


def test_guard_retries_warn_and_ledger_warn_fails():
    fails, warns = audit_findings(_audit(ledger="WARN", guard="WARN", reads=2))
    assert len(fails) == 4 and all(f.startswith("ledger") for f in fails)
    assert warns == ["guard WARN (2 reads)"]


def test_failed_layers_fail():
    fails, _ = audit_findings(_audit(failed=True))
    assert fails == ["FAILED rows: netsuite_transactions/silver"]
