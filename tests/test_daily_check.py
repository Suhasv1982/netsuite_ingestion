"""tools/daily_check.py: the pure parts (run selection by UTC date, log classification, count and audit findings)."""

import datetime as dt

from daily_check import audit_findings, classify_generator_log, compare_counts, runs_on_date

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


def test_compare_counts():
    src = {"netsuite_customers": 1, "netsuite_memberships": 2, "netsuite_certifications": 3,
           "netsuite_transactions": 4, "netsuite_transaction_lines": 5}
    assert compare_counts(src, dict(src)) == []
    assert compare_counts(src, {**src, "netsuite_transactions": 3}) == ["netsuite_transactions: source=4 dev=3"]


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
