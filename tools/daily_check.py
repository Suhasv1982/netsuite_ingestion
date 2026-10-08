"""Read-only daily check of the generator -> dev (-> prod) cycle for one UTC date.

    python tools/daily_check.py --date 2026-10-04 --profile DEFAULT

Changes nothing. For the given date it reports, with OK / WARN / FAIL per line:
  1. generator job (`netsuite_daily_generator`): runs that day, result, and whether the log says SKIPPED or an
     increment; source rows with that updated_date; rows dated after it; daily backup schemas of that day.
  2. dev job (`[dev ci_dev] netsuite_ingestion_daily`) and prod job (`netsuite_ingestion_daily`): runs that day and
     their result; for each, the run_audit rows of that run (ledger status per table, guard status and reads).
  3. dev bronze now: exact row counts against the source per table and snapshot date (FAIL on any difference:
     since bronze keys rows by content, every version of a key on a day is loaded); an INFO line with the business
     keys that have several versions on one day in the source; rows loaded into dev bronze that day, per table and
     _snapshot_date.
  4. rejects (dev, now) per table and per reason; gold row counts and as_of_date.
Exit 1 if any line is FAIL. Needs: Databricks CLI (profile), psycopg, a SQL warehouse (starts it if stopped).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time

GENERATOR_JOB = "[dev ci_dev] netsuite_daily_generator"
DEV_JOB = "[dev ci_dev] netsuite_ingestion_daily"
PROD_JOB = "netsuite_ingestion_daily"
SOURCE_ENDPOINT = "projects/netsuite-sample/branches/production/endpoints/primary"
META_ENDPOINTS = {"dev": "projects/aidq-metadata/branches/dev/endpoints/primary",
                  "prod": "projects/aidq-metadata/branches/production/endpoints/primary"}
TABLES = ["netsuite_customers", "netsuite_memberships", "netsuite_certifications", "netsuite_transactions",
          "netsuite_transaction_lines"]
INCREMENTAL = TABLES[1:]
BUSINESS_KEY = {"netsuite_memberships": "membership_internal_id", "netsuite_certifications": "certification_internal_id",
                "netsuite_transactions": "transaction_internal_id", "netsuite_transaction_lines": "transaction_line_id"}


# -- pure -------------------------------------------------------------------


def runs_on_date(runs: list[dict], day: dt.date) -> list[dict]:
    """Job runs whose start_time (epoch ms, UTC) falls on `day`, oldest first."""
    def started(r):
        return dt.datetime.fromtimestamp(r["start_time"] / 1000, dt.timezone.utc).date()
    return sorted((r for r in runs if r.get("start_time") and started(r) == day), key=lambda r: r["start_time"])


def classify_generator_log(log: str) -> str:
    """'skipped', 'increment' or 'unknown' from the generator task's stdout."""
    if "SKIPPED:" in log:
        return "skipped"
    if "increment seed=" in log and "rows:" in log:
        return "increment"
    return "unknown"


def count_gaps(source: dict[tuple, int], target: dict[tuple, int]) -> list[str]:
    """One FAIL line per (table, snapshot date) whose dev bronze row count differs from the source (date None for a
    full-load table). Since bronze keys rows by (key, date, row hash) a second version of a key on a loaded date is
    loaded too (incident 2026-10-02), so a same-day version no longer explains a shortfall."""
    fails = []
    for t, d in sorted(set(source) | set(target), key=lambda k: (k[0], str(k[1]))):
        s, n = source.get((t, d), 0), target.get((t, d), 0)
        if s != n:
            fails.append(f"{t if d is None else f'{t}@{d}'}: source={s} dev={n}")
    return fails


def same_day_version_lines(keys: dict[tuple, list], max_keys: int = 10) -> list[str]:
    """INFO lines: per (table, date), the business keys with several source rows on that updated_date day."""
    lines = []
    for (t, d), ks in sorted(keys.items(), key=lambda x: (x[0][0], str(x[0][1]))):
        ks = sorted(ks, key=str)
        more = f" (+{len(ks) - max_keys} more)" if len(ks) > max_keys else ""
        lines.append(f"{t.replace('netsuite_', '')}@{d}: {len(ks)} key(s) with several versions: "
                     + ", ".join(str(k) for k in ks[:max_keys]) + more)
    return lines


def audit_findings(rows: list[dict]) -> tuple[list[str], list[str]]:
    """(fails, warns) from the run_audit rows of one job run."""
    fails, warns = [], []
    ledger = {r["table"]: r["status"] for r in rows if r["layer"] == "ledger"}
    for t in INCREMENTAL:
        if ledger.get(t) != "OK":
            fails.append(f"ledger {t}: {ledger.get(t, 'missing')}")
    guard = [r for r in rows if r["layer"] == "guard"]
    if not guard:
        warns.append("no guard row")
    elif guard[0]["status"] not in ("OK", "WARN"):
        fails.append(f"guard {guard[0]['status']}")
    elif guard[0]["status"] == "WARN":
        warns.append(f"guard WARN ({guard[0].get('rows_read')} reads)")
    failed_layers = sorted({f"{r['table']}/{r['layer']}" for r in rows if r["status"] == "FAILED"})
    if failed_layers:
        fails.append("FAILED rows: " + ", ".join(failed_layers))
    return fails, warns


# -- I/O --------------------------------------------------------------------


class Report:
    def __init__(self):
        self.fail = 0

    def line(self, level: str, msg: str):
        self.fail += level == "FAIL"
        print(f"{level:<5} {msg}")


def _cli(profile, *args, body=None):
    cmd = ["databricks", *args, "-o", "json", "--profile", profile] + (["--json", json.dumps(body)] if body else [])
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode:
        raise RuntimeError(f"databricks {' '.join(args[:3])}: {p.stderr.strip()[:300]}")
    return json.loads(p.stdout) if p.stdout.strip() else {}


def pg(profile, endpoint):
    import psycopg

    ep = _cli(profile, "postgres", "get-endpoint", endpoint)
    if ep["status"].get("disabled"):
        raise RuntimeError(f"{endpoint} is disabled (this check never enables it)")
    token = _cli(profile, "postgres", "generate-database-credential", endpoint)["token"]
    user = _cli(profile, "current-user", "me")["userName"]
    return psycopg.connect(host=ep["status"]["hosts"]["host"], dbname="databricks_postgres", user=user,
                           password=token, sslmode="require", connect_timeout=60)


def sql(profile, warehouse_id, statement) -> list[dict]:
    body = {"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "50s", "disposition": "INLINE",
            "format": "JSON_ARRAY"}
    r = _cli(profile, "api", "post", "/api/2.0/sql/statements", body=body)
    while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(5)
        r = _cli(profile, "api", "get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"query failed: {json.dumps(r.get('status'))[:300]}")
    cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    return [dict(zip(cols, row)) for row in (r.get("result", {}).get("data_array") or [])]


def job_id(profile, name):
    for j in _cli(profile, "jobs", "list"):
        if j["settings"]["name"] == name:
            return j["job_id"]
    return None


def task_log(profile, run) -> str:
    tasks = _cli(profile, "jobs", "get-run", str(run["run_id"])).get("tasks", [])
    if not tasks:
        return ""
    out = _cli(profile, "jobs", "get-run-output", str(tasks[0]["run_id"]))
    return (out.get("logs") or "") + (out.get("error") or "")


def audit_rows(profile, env, run_id) -> list[dict]:
    with pg(profile, META_ENDPOINTS[env]) as c:
        c.execute("SET TRANSACTION READ ONLY")
        rows = c.execute(
            "SELECT coalesce(d.source_table, '(pipeline)'), a.layer, a.status, a.rows_read "
            "FROM aidq_metadata.run_audit a LEFT JOIN aidq_metadata.source_table_def d USING (table_id) "
            "WHERE a.run_id = %s", (str(run_id),)).fetchall()
        c.rollback()
    return [{"table": t, "layer": layer, "status": s, "rows_read": n} for t, layer, s, n in rows]


def check_job(profile, rep, env, name, day):
    jid = job_id(profile, name)
    if jid is None:
        rep.line("WARN", f"{env} job {name!r} not found")
        return
    runs = runs_on_date(_cli(profile, "jobs", "list-runs", "--job-id", str(jid), "--limit", "25"), day)
    if not runs:
        rep.line("WARN" if env == "prod" else "FAIL", f"{env} job: no run on {day}")
        return
    for r in runs:
        state = r["state"].get("result_state")
        rep.line("OK" if state == "SUCCESS" else "FAIL", f"{env} job run {r['run_id']}: {state}")
        if state != "SUCCESS":
            continue
        fails, warns = audit_findings(audit_rows(profile, env, r["run_id"]))
        for f in fails:
            rep.line("FAIL", f"{env} run_audit: {f}")
        for w in warns:
            rep.line("WARN", f"{env} run_audit: {w}")
        if not fails:
            rep.line("OK", f"{env} run_audit: ledger OK for {len(INCREMENTAL)} tables")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--date", required=True, type=dt.date.fromisoformat, help="UTC date to check (YYYY-MM-DD)")
    p.add_argument("--profile", default="DEFAULT")
    p.add_argument("--warehouse-id", help="default: the first SQL warehouse")
    args = p.parse_args(argv)
    day, prof, rep = args.date, args.profile, Report()
    print(f"== daily check {day} (read-only)")

    # 1. generator and the source
    gid = job_id(prof, GENERATOR_JOB)
    gen_runs = runs_on_date(_cli(prof, "jobs", "list-runs", "--job-id", str(gid), "--limit", "25"), day) if gid else []
    if not gen_runs:
        rep.line("FAIL", f"generator: no run on {day}")
    for r in gen_runs:
        state = r["state"].get("result_state")
        kind = classify_generator_log(task_log(prof, r)) if state == "SUCCESS" else "-"
        rep.line("OK" if state == "SUCCESS" and kind != "unknown" else "FAIL", f"generator run {r['run_id']}: {state} ({kind})")
    with pg(prof, SOURCE_ENDPOINT) as c:
        c.execute("SET TRANSACTION READ ONLY")
        source = {t: c.execute(f"SELECT count(*) FROM netsuite.{t}").fetchone()[0] for t in TABLES}
        dated = {t: c.execute(f"SELECT count(*) FROM netsuite.{t} WHERE updated_date::date = %s", (day,)).fetchone()[0]
                 for t in INCREMENTAL}
        future = {t: c.execute(f"SELECT count(*) FROM netsuite.{t} WHERE updated_date::date > %s", (day,)).fetchone()[0]
                  for t in INCREMENTAL}
        src_by_day, versions = {("netsuite_customers", None): source["netsuite_customers"]}, {}
        for t in INCREMENTAL:
            for d, n in c.execute(f"SELECT updated_date::date, count(*) FROM netsuite.{t} GROUP BY 1").fetchall():
                src_by_day[(t, str(d))] = n
            for d, k in c.execute(f"SELECT updated_date::date, {BUSINESS_KEY[t]} FROM netsuite.{t} "
                                  "GROUP BY 1, 2 HAVING count(*) > 1").fetchall():
                versions.setdefault((t, str(d)), []).append(k)
        backups = [r[0] for r in c.execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE %s ORDER BY 1",
                                           ("netsuite_backup_daily_%",)).fetchall()]
        c.rollback()
    rep.line("INFO", f"source rows dated {day}: " + ", ".join(f"{t.replace('netsuite_', '')}={n}" for t, n in dated.items()))
    if any(future.values()):
        rep.line("INFO", "source rows dated after it (incl. the known 10-02/10-03 rows of 2026-10-01): "
                 + ", ".join(f"{t.replace('netsuite_', '')}={n}" for t, n in future.items() if n))
    today_backups = [b for b in backups if b.startswith(f"netsuite_backup_daily_{day:%Y%m%d}")]
    rep.line("INFO", f"daily backup schemas: {len(backups)} total, {len(today_backups)} from {day}")
    if len(backups) > 7:
        rep.line("WARN", "more than 7 daily backup schemas (retention keeps 7)")

    # 2. dev and prod job runs + run_audit
    check_job(prof, rep, "dev", DEV_JOB, day)
    check_job(prof, rep, "prod", PROD_JOB, day)

    # 3. dev bronze vs source, rows loaded that day; 4. rejects, gold
    wh = args.warehouse_id or _cli(prof, "warehouses", "list")[0]["id"]
    q = " UNION ALL ".join(
        f"SELECT '{t}' t, cast(_snapshot_date AS string) d, count(*) n FROM workspace.poc_bronze.{t} GROUP BY 2"
        for t in INCREMENTAL)
    dev_by_day = {(r["t"], r["d"]): int(r["n"]) for r in sql(prof, wh, q)}
    dev_by_day[("netsuite_customers", None)] = int(
        sql(prof, wh, "SELECT count(*) n FROM workspace.poc_bronze.netsuite_customers")[0]["n"])
    fails = count_gaps(src_by_day, dev_by_day)
    for msg in fails:
        rep.line("FAIL", f"dev bronze vs source (now): {msg}")
    if not fails:
        rep.line("OK", "dev bronze vs source (now): identical counts per table and snapshot date")
    for msg in same_day_version_lines(versions):
        rep.line("INFO", f"source keys with several same-day versions (all loaded; silver keeps one): {msg}")
    q = " UNION ALL ".join(
        f"SELECT '{t}' t, cast(_snapshot_date AS string) d, count(*) n FROM workspace.poc_bronze.{t} "
        f"WHERE cast(_loaded_at AS date) = DATE'{day}' GROUP BY _snapshot_date" for t in INCREMENTAL)
    loaded = sorted(sql(prof, wh, q), key=lambda r: (r["t"], r["d"]))
    rep.line("INFO", f"dev bronze rows loaded on {day}: "
             + (", ".join(f"{r['t'].replace('netsuite_', '')}@{r['d']}={r['n']}" for r in loaded) or "none"))
    rej = sql(prof, wh, "SELECT source_table, reason, count(*) n FROM workspace.poc_reject.rejected_rows GROUP BY 1, 2 ORDER BY 1, 2")
    rep.line("INFO", "dev rejects (now): " + ", ".join(f"{r['source_table'].replace('netsuite_', '')}/{r['reason']}={r['n']}" for r in rej))
    gold = sql(prof, wh, "SELECT (SELECT count(*) FROM workspace.poc_gold.gold_customer_revenue) rev, "
                         "(SELECT count(*) FROM workspace.poc_gold.gold_customer_status) st, "
                         "(SELECT cast(max(as_of_date) AS string) FROM workspace.poc_gold.gold_customer_status) asof")[0]
    rep.line("INFO", f"dev gold: revenue rows {gold['rev']}, status rows {gold['st']}, as_of_date {gold['asof']}")

    print(f"== {'FAILED' if rep.fail else 'PASSED'} ({rep.fail} failure(s))")
    return 1 if rep.fail else 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
