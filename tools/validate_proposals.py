"""Validate PENDING ADD_DQ_RULE proposals (design v2 section 6; plan #39 step 5).

    python tools/validate_proposals.py --profile DEFAULT [--dry-run] [--as-caller]     # locally
    python tools/validate_proposals.py --auth sdk                                       # the dq_validator job
    (job: run as ci-dev; it does SET LOCAL ROLE aidq_validator before any work)

For each PENDING proposal (oldest first, locked FOR UPDATE SKIP LOCKED): static checks, dry run on dev bronze through
the SQL warehouse, NULL check, impact; then ONE UPDATE sets status (VALIDATED / VALIDATION_FAILED) together with
validation_result, the only transition migration 006 allows for validation_result. Never approves anything.

--as-caller skips SET ROLE aidq_validator (the owner is not a member; use it only for local tests). --dry-run
validates and prints, and rolls back. Rules are in src/aidq_dq_agent/validator.py.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _here() -> Path:
    """This file's directory. A Databricks serverless python task runs the file without `__file__` (2026-10-14:
    NameError); its working directory is then the file's directory, so fall back to that."""
    try:
        return Path(__file__).resolve().parent
    except NameError:
        return Path(os.getcwd())


ROOT = _here().parent
sys.path.insert(0, str(ROOT / "src"))

from aidq_dq_agent import validator  # noqa: E402

PENDING_SQL = (
    "SELECT p.proposal_id, p.table_id, d.source_table, d.discard_threshold, p.proposed_change "
    "FROM aidq_metadata.config_proposals p JOIN aidq_metadata.source_table_def d USING (table_id) "
    "WHERE p.status = 'PENDING' AND p.proposal_type = 'ADD_DQ_RULE' ORDER BY p.created_at "
    "FOR UPDATE OF p SKIP LOCKED")
COLUMNS_SQL = ("SELECT column_name FROM aidq_metadata.source_columns WHERE table_id = %s AND coalesce(is_active, true) "
               "ORDER BY ordinal NULLS LAST")
UPDATE_SQL = ("UPDATE aidq_metadata.config_proposals SET status = %s, validation_result = %s::jsonb "
              "WHERE proposal_id = %s AND status = 'PENDING'")


def validate_all(conn, sql, catalog: str, set_role: bool = True, log=print) -> list[dict]:
    """Validate every PENDING proposal on `conn` (caller commits or rolls back). `sql(statement)` runs a warehouse
    query and returns rows as dicts."""
    if set_role:
        conn.execute("SET LOCAL ROLE aidq_validator")
    try:
        version = sql("SELECT version() AS v")[0]["v"]
    except Exception:  # noqa: BLE001 -- recorded as unknown, not a reason to stop
        version = None
    out = []
    for pid, table_id, table, threshold, change in conn.execute(PENDING_SQL).fetchall():
        expr = (change or {}).get("rule_expr", "")
        cols = [r[0] for r in conn.execute(COLUMNS_SQL, (table_id,)).fetchall()]
        static = validator.static_check(expr, cols)
        counts = None
        if static.ok:
            try:
                counts = sql(validator.dry_run_sql(f"{catalog}.poc_bronze.{table}", expr))[0]
            except Exception as exc:  # noqa: BLE001 -- the dry run failed: the rule fails, with the reason
                static.problems.append(f"dry run failed: {str(exc)[:300]}")
                static.ok = False
        result = validator.judge(static, counts, None if threshold is None else float(threshold), version)
        conn.execute(UPDATE_SQL, (result["status"], json.dumps(result, default=str), pid))
        log(f"proposal {pid} ({table}): {result['summary']}")
        out.append({"proposal_id": pid, **result})
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--profile", default=None, help="Databricks CLI profile (omit for environment auth)")
    p.add_argument("--dry-run", action="store_true", help="validate and print, then roll back")
    p.add_argument("--as-caller", action="store_true", help="do not SET ROLE aidq_validator (local tests only)")
    p.add_argument("--auth", choices=["cli", "sdk"], default="cli",
                   help="sdk: the run identity of a Databricks job (no CLI; plan #39 step 10)")
    args = p.parse_args(argv)

    sys.path.insert(0, str(ROOT / "tools"))
    import migrate  # noqa: E402  (connect to the dev metadata branch as the caller)

    from aidq_mcp.config import env_config
    from aidq_mcp.reads import Reads

    if args.auth == "sdk":
        from aidq_dq_agent.write_tools import sdk_connect
        from aidq_mcp.sdk_reads import SdkReads

        reads = SdkReads()
        conn = sdk_connect(env_config("dev").metadata_endpoint)()
    else:
        reads = Reads(args.profile)
        conn = migrate.connect("dev", args.profile)
    try:
        results = validate_all(conn, reads.sql, env_config("dev").catalog, set_role=not args.as_caller)
        if args.dry_run:
            conn.rollback()
            print(f"dry run: {len(results)} proposal(s) validated, nothing written")
        else:
            conn.commit()
            print(f"{len(results)} proposal(s) validated")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
