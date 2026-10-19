"""Validate PENDING ADD_DQ_RULE proposals (design v2 section 6; plan #39 step 5).

    python tools/validate_proposals.py --profile DEFAULT [--dry-run] [--as-caller]     # locally
    python tools/validate_proposals.py --auth sdk                                       # the dq_validator job
    (job: run as ci-dev; it does SET LOCAL ROLE aidq_validator before any work)

For each PENDING proposal (oldest first, locked FOR UPDATE SKIP LOCKED): static checks, dry run on dev bronze through
the SQL warehouse, NULL check, impact; then ONE UPDATE sets status (VALIDATED / VALIDATION_FAILED) together with
validation_result, the only transition migration 006 allows for validation_result. Never approves anything.

Also ADD_COLUMN / DEACTIVATE_COLUMN (schema drift). PENDING ONBOARD_TABLE proposals are listed and left PENDING
until the onboarding validator branch exists (onboarding plan #77, step 6).

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


COLUMN_PENDING_SQL = (
    "SELECT p.proposal_id, p.table_id, d.source_table, d.load_mode, d.business_key, d.watermark_col, p.proposal_type, "
    "p.proposed_change FROM aidq_metadata.config_proposals p JOIN aidq_metadata.source_table_def d USING (table_id) "
    "WHERE p.status = 'PENDING' AND p.proposal_type IN ('ADD_COLUMN', 'DEACTIVATE_COLUMN') ORDER BY p.created_at "
    "FOR UPDATE OF p SKIP LOCKED")
META_COLUMNS_SQL = ("SELECT column_name, ordinal, coalesce(is_active, true) AS active FROM aidq_metadata.source_columns "
                    "WHERE table_id = %s")
COLUMN_UPDATE_SQL = ("UPDATE aidq_metadata.config_proposals SET status = %s, validation_result = %s::jsonb, "
                     "requires_rebuild = %s WHERE proposal_id = %s AND status = 'PENDING'")
# ONBOARD_TABLE (table_id NULL until applied, migration 008): listed so it is visible, validated by the onboarding
# validator branch (onboarding plan #77, step 6). Until then it stays PENDING: nothing is written for it here.
ONBOARD_PENDING_SQL = (
    "SELECT p.proposal_id, COALESCE(d.source_table, p.proposed_change->'table'->>'source_table') AS source_table "
    "FROM aidq_metadata.config_proposals p LEFT JOIN aidq_metadata.source_table_def d USING (table_id) "
    "WHERE p.status = 'PENDING' AND p.proposal_type = 'ONBOARD_TABLE' ORDER BY p.created_at")
SOURCE_COLUMNS_SQL = ("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'netsuite' "
                      "AND table_name = %s")


def judge_column(ptype: str, change: dict, table: dict, source_cols: dict[str, str], meta_cols: list[tuple]) -> dict:
    """The decision for one ADD_COLUMN / DEACTIVATE_COLUMN proposal (design 3.4). Pure. `table`: load_mode,
    business_key, watermark_col; `source_cols`: column -> source type; `meta_cols`: (column, ordinal, active).
    requires_rebuild only for incremental tables (owner decision 2026-10-15: a full-load table has no row hash)."""
    from aidq_drift.detect import metadata_type

    col = (change or {}).get("column_name")
    active = {c for c, _, a in meta_cols if a}
    problems = []
    if not col:
        problems.append("no column_name")
    elif ptype == "ADD_COLUMN":
        if col not in source_cols:
            problems.append(f"column {col} is not in the source table")
        if col in active:
            problems.append(f"column {col} is already active in the metadata")
        mapped = metadata_type(source_cols.get(col))
        if col in source_cols and mapped is None:
            problems.append(f"source type {source_cols[col]!r} has no metadata type mapping")
        elif mapped and (change.get("data_type") or "").upper() != mapped:
            problems.append(f"data_type {change.get('data_type')!r} does not match the source type (expected {mapped})")
        ordinal = change.get("ordinal")
        if ordinal is not None and any(o == ordinal for c, o, a in meta_cols if a and c != col):
            problems.append(f"ordinal {ordinal} is already used by an active column")
    else:
        if col not in active:
            problems.append(f"column {col} is not an active column")
        if col in source_cols:
            problems.append(f"column {col} still exists in the source")
        if col in (table.get("business_key"), table.get("watermark_col")):
            problems.append(f"column {col} is the business key or the watermark column")
    incremental = (table.get("load_mode") or "").lower() == "incremental"
    ok = not problems
    return {"status": "VALIDATED" if ok else "VALIDATION_FAILED", "checks": {"column": ok}, "problems": problems,
            "requires_rebuild": incremental if ok else None,
            "summary": ("VALIDATED" if ok else "FAILED: " + "; ".join(problems[:3]))
            + (" | requires bronze rebuild (incremental table)" if ok and incremental else "")}


def validate_columns(conn, source_query, set_role: bool = True, log=print) -> list[dict]:
    """Every PENDING column proposal. `source_query(sql, params)` reads the source (the caller's identity)."""
    if set_role:
        conn.execute("SET LOCAL ROLE aidq_validator")
    out = []
    for pid, table_id, table, load_mode, key, wm, ptype, change in conn.execute(COLUMN_PENDING_SQL).fetchall():
        try:
            source_cols = {r["column_name"]: r["data_type"] for r in source_query(SOURCE_COLUMNS_SQL, (table,))}
        except Exception as exc:  # noqa: BLE001 -- cannot check against the source: fail, with the reason
            source_cols = None
            result = {"status": "VALIDATION_FAILED", "checks": {"column": False}, "requires_rebuild": None,
                      "problems": [f"source not readable: {str(exc)[:200]}"], "summary": "FAILED: source not readable"}
        if source_cols is not None:
            meta_cols = [tuple(r) for r in conn.execute(META_COLUMNS_SQL, (table_id,)).fetchall()]
            result = judge_column(ptype, change, {"load_mode": load_mode, "business_key": key, "watermark_col": wm},
                                  source_cols, meta_cols)
        conn.execute(COLUMN_UPDATE_SQL, (result["status"], json.dumps(result, default=str), result["requires_rebuild"],
                                         pid))
        log(f"proposal {pid} ({ptype} {table}.{(change or {}).get('column_name')}): {result['summary']}")
        out.append({"proposal_id": pid, **result})
    return out


def pending_onboard(conn, log=print) -> list[dict]:
    """PENDING ONBOARD_TABLE proposals: reported, left PENDING (no validator branch yet: onboarding step 6)."""
    out = []
    for pid, table in conn.execute(ONBOARD_PENDING_SQL).fetchall():
        log(f"proposal {pid} (ONBOARD_TABLE {table}): left PENDING, the ONBOARD_TABLE validation is not built yet "
            "(onboarding plan step 6)")
        out.append({"proposal_id": pid, "source_table": table, "status": "PENDING"})
    return out


def validate_all(conn, sql, catalog: str, set_role: bool = True, log=print) -> list[dict]:
    """Validate every PENDING ADD_DQ_RULE proposal on `conn` (caller commits or rolls back). `sql(statement)` runs a
    warehouse query and returns rows as dicts."""
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
        source = env_config("dev").source_endpoint
        results += validate_columns(conn, lambda q, params=(): reads.pg_query(source, q, params),
                                    set_role=False)   # the role is already set for this transaction
        waiting = pending_onboard(conn)                     # listed only, never written
        if args.dry_run:
            conn.rollback()
            print(f"dry run: {len(results)} proposal(s) validated, nothing written")
        else:
            conn.commit()
            print(f"{len(results)} proposal(s) validated"
                  + (f", {len(waiting)} ONBOARD_TABLE left PENDING" if waiting else ""))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    # only a failure exits: in a job's IPython kernel even sys.exit(0) marks the run FAILED
    _code = main()
    if _code:
        sys.exit(_code)
