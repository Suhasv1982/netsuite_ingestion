"""Check that every active SOFT data-quality rule shows up as an expectation metric in the pipeline event log.

SOFT rules never reject rows (dq.py attaches them to "<table>_valid" with dp.expect_all), so the event log
is the only place their violations are visible. This script reads the active SOFT rules from the metadata
database, reads the `flow_progress` events of one pipeline update (the latest by default) through a SQL
warehouse, and reports, per rule, the expectation's dataset and passed / failed record counts. It exits 1
if any SOFT rule is missing from the event log.

    python tools/check_soft_expectations.py --pipeline-id <id> --meta-branch dev --profile DEFAULT

Read-only: it runs SELECTs only. Record counts are summed over the update's flow_progress events.
The pure functions (expected_rules, collect_expectations, compare) are unit-tested without a database.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time

sys.path.insert(0, __file__.rsplit("tools", 1)[0] + "src/netsuite_ingestion")  # metadata.py (pure builders)

from metadata import build_soft_expectations, filter_active_rules  # noqa: E402

META_PROJECT = "aidq-metadata"
DATABASE = "databricks_postgres"
_UUID = re.compile(r"[0-9a-fA-F-]{36}")


# -- pure -------------------------------------------------------------------


def expected_rules(table_defs: list[dict], rules: list[dict]) -> list[tuple[str, str]]:
    """(source_table, expectation name) for every active SOFT rule, named exactly as dq.py names them."""
    by_table: dict[int, list[dict]] = {}
    for r in filter_active_rules(rules):
        by_table.setdefault(r["table_id"], []).append(r)
    out = []
    for td in table_defs:
        for name in build_soft_expectations(by_table.get(td["table_id"], [])):
            out.append((td["source_table"], name))
    return out


def collect_expectations(rows: list[dict]) -> dict[str, list[dict]]:
    """Event rows {flow_name, expectations (JSON text or list)} -> name -> [{dataset, flow, passed, failed}]."""
    acc: dict[tuple, dict] = {}
    for row in rows:
        exps = row.get("expectations")
        if isinstance(exps, str):
            exps = json.loads(exps) if exps.strip() else []
        for e in exps or []:
            key = (e.get("name"), e.get("dataset"), row.get("flow_name"))
            cur = acc.setdefault(key, {"dataset": e.get("dataset"), "flow": row.get("flow_name"), "passed": 0, "failed": 0})
            cur["passed"] += int(e.get("passed_records") or 0)
            cur["failed"] += int(e.get("failed_records") or 0)
    found: dict[str, list[dict]] = {}
    for (name, _, _), v in acc.items():
        found.setdefault(name, []).append(v)
    return found


def compare(expected: list[tuple[str, str]], found: dict[str, list[dict]]):
    """(present, missing): present = [(table, name, metrics)] where an expectation of that name is reported
    for a dataset or flow of that table; missing = [(table, name)]."""
    present, missing = [], []
    for table, name in expected:
        hits = [m for m in found.get(name, []) if table in (m.get("dataset") or "") or table in (m.get("flow") or "")]
        if hits:
            present.append((table, name, hits))
        else:
            missing.append((table, name))
    return present, missing


def event_log_sql(pipeline_id: str, update_id: str | None = None) -> str:
    for v in (pipeline_id, update_id):
        if v is not None and not _UUID.fullmatch(v):
            raise ValueError(f"not a pipeline/update id: {v!r}")
    update = (
        f"'{update_id}'"
        if update_id
        else f"(SELECT origin.update_id FROM event_log('{pipeline_id}') WHERE event_type = 'create_update' "
        "ORDER BY timestamp DESC LIMIT 1)"
    )
    return (
        "SELECT origin.update_id AS update_id, origin.flow_name AS flow_name, "
        "details:flow_progress.data_quality.expectations AS expectations "  # JSON text

        f"FROM event_log('{pipeline_id}') "
        f"WHERE event_type = 'flow_progress' AND origin.update_id = {update} "
        "AND details:flow_progress.data_quality.expectations IS NOT NULL"
    )


# -- I/O --------------------------------------------------------------------


def _cli_json(profile: str, *args: str):
    proc = subprocess.run(["databricks", *args, "--profile", profile, "-o", "json"], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args[:2])} failed: {proc.stderr.strip()[:400]}")
    return json.loads(proc.stdout) if proc.stdout.strip() else {}


def read_metadata(profile: str, branch: str):
    import psycopg
    from psycopg.rows import dict_row

    endpoint_name = f"projects/{META_PROJECT}/branches/{branch}/endpoints/primary"
    endpoint = _cli_json(profile, "postgres", "get-endpoint", endpoint_name)
    if endpoint["status"].get("disabled"):
        raise RuntimeError(f"Endpoint {endpoint_name} is disabled; enable it first (this script never does).")
    token = _cli_json(profile, "postgres", "generate-database-credential", endpoint_name)["token"]
    user = _cli_json(profile, "current-user", "me")["userName"]
    with psycopg.connect(host=endpoint["status"]["hosts"]["host"], dbname=DATABASE, user=user, password=token,
                         sslmode="require", row_factory=dict_row) as conn:
        table_defs = conn.execute("SELECT table_id, source_table FROM aidq_metadata.source_table_def").fetchall()
        rules = conn.execute("SELECT * FROM aidq_metadata.data_quality_rules").fetchall()
    return table_defs, rules


def query_event_log(profile: str, warehouse_id: str, sql: str) -> list[dict]:
    body = {"warehouse_id": warehouse_id, "statement": sql, "wait_timeout": "50s", "disposition": "INLINE", "format": "JSON_ARRAY"}
    resp = _cli_json(profile, "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body))
    while resp.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(5)
        resp = _cli_json(profile, "api", "get", f"/api/2.0/sql/statements/{resp['statement_id']}")
    if resp.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"event_log query failed: {json.dumps(resp.get('status'))[:400]}")
    cols = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    return [dict(zip(cols, r)) for r in resp.get("result", {}).get("data_array", []) or []]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--pipeline-id", required=True)
    p.add_argument("--update-id", help="default: the pipeline's latest update")
    p.add_argument("--meta-branch", default="dev", help="aidq-metadata branch holding the rules (dev | production)")
    p.add_argument("--warehouse-id", help="default: the first SQL warehouse")
    p.add_argument("--profile", default="DEFAULT")
    args = p.parse_args(argv)

    table_defs, rules = read_metadata(args.profile, args.meta_branch)
    expected = expected_rules(table_defs, rules)
    if not expected:
        print("No active SOFT rules in metadata: nothing to check.")
        return 0
    warehouse_id = args.warehouse_id or _cli_json(args.profile, "warehouses", "list")[0]["id"]
    rows = query_event_log(args.profile, warehouse_id, event_log_sql(args.pipeline_id, args.update_id))
    update_ids = sorted({r["update_id"] for r in rows})
    present, missing = compare(expected, collect_expectations(rows))

    print(f"update(s): {', '.join(update_ids) or '<none with expectation metrics>'}")
    for table, name, hits in present:
        for m in hits:
            print(f"OK       {table}: '{name}' on {m['dataset']} (flow {m['flow']}): passed={m['passed']} failed={m['failed']}")
    for table, name in missing:
        print(f"MISSING  {table}: '{name}' not in the event log of this update")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
