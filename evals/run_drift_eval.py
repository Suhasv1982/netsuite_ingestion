"""Schema drift agent eval (design 3.7; plan docs/phase6_schema_drift_plan.md step 7), on a DEV BRANCH of the source.

    python evals/run_drift_eval.py --profile DEFAULT --branch drift-eval

Never touches the shared production source: the DDL below runs only on the branch's endpoint (owner pre-approval
2026-10-15 for a dev branch of netsuite-sample). Two phases, each a dry run of the real agent (model included) with
detection pointed at the branch:

1. clean: the branch is made to match the metadata (the three real drift columns dropped on the branch) -> targets:
   no change detected, no model call, nothing proposed.
2. injected: one added column, one dropped unused column, one type change -> targets: every injected change detected
   and nothing else (deterministic: 100%), the right proposal type or incident category, the rebuild flag right
   (validator decision for the two proposals), the rebuild sentence in every incremental rationale.

The report goes to evals/results/drift_eval_<date>.md and .json.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))

CLEAN_DDL = [
    "ALTER TABLE netsuite.netsuite_customers DROP COLUMN IF EXISTS created_date",
    "ALTER TABLE netsuite.netsuite_customers DROP COLUMN IF EXISTS updated_date",
    "ALTER TABLE netsuite.netsuite_transactions DROP COLUMN IF EXISTS custbody_region",
]
INJECT_DDL = [
    "ALTER TABLE netsuite.netsuite_transactions ADD COLUMN IF NOT EXISTS custcol_eval_channel text",
    "ALTER TABLE netsuite.netsuite_memberships DROP COLUMN IF EXISTS membership_type",
    "ALTER TABLE netsuite.netsuite_transaction_lines ALTER COLUMN line_number TYPE bigint",
]
EXPECTED = {   # change id -> (what the agent writes, requires_rebuild of the validator for a proposal)
    "netsuite_transactions:added:custcol_eval_channel": ("ADD_COLUMN proposal", True),
    "netsuite_memberships:removed:membership_type": ("DEACTIVATE_COLUMN proposal", True),
    "netsuite_transaction_lines:type_changed:line_number": ("SCHEMA_TYPE_CHANGE incident", None),
}


def run_ddl(profile: str, endpoint: str, statements: list[str]) -> None:
    import psycopg

    from aidq_mcp.reads import Reads

    r = Reads(profile)
    ep = r.cli("postgres", "get-endpoint", endpoint)
    token = r.cli("postgres", "generate-database-credential", endpoint)["token"]
    with psycopg.connect(host=ep["status"]["hosts"]["host"], dbname="databricks_postgres", user=r.current_user(),
                         password=token, sslmode="require", connect_timeout=60) as conn:
        for s in statements:
            conn.execute(s)
        conn.commit()


class CountingReviewer:
    def __init__(self, inner):
        self.inner, self.calls = inner, 0

    async def review(self, changes, profiles):
        self.calls += 1
        return await self.inner.review(changes, profiles)


def phase(profile: str, endpoint: str) -> dict:
    from aidq_agent.dbx import DatabricksChat
    from aidq_dq_agent.__main__ import cli_connect
    from aidq_dq_agent.write_tools import WriteTools
    from aidq_drift import VERSION, agent
    from aidq_llm.router import Router
    from aidq_mcp.config import env_config
    from aidq_mcp.reads import Reads

    cfg, reads = env_config("dev"), Reads(profile)
    wt = WriteTools(cli_connect(reads, cfg.metadata_endpoint), f"schema_drift@{VERSION}-eval", "eval", dry_run=True)
    reviewer = CountingReviewer(agent.ModelReviewer(Router(DatabricksChat(profile), "aidq_drift_eval", trace_id="eval")))
    state = asyncio.run(agent.run(reads.pg_query, endpoint, cfg.metadata_endpoint, reviewer, wt, "eval"))
    return {"state": state, "model_calls": reviewer.calls, "reads": reads, "endpoint": endpoint}


def score(clean: dict, injected: dict) -> dict:
    import validate_proposals as vp

    from aidq_mcp.config import env_config

    checks = {"clean: no change detected": not clean["state"]["changes"],
              "clean: no model call": clean["model_calls"] == 0,
              "clean: nothing written": not clean["state"]["submitted"]}
    got = {s["change"].id: s for s in injected["state"]["submitted"]}
    detected = {c.id for c in injected["state"]["changes"]}
    checks["injected: exactly the injected changes detected"] = detected == set(EXPECTED)
    reads, cfg = injected["reads"], env_config("dev")
    rows = []
    for cid, (written, rebuild) in EXPECTED.items():
        s = got.get(cid)
        ok_kind = bool(s) and s["written"] == written and s["result"].get("status") == "ok"
        c = s["change"] if s else None
        ok_rebuild = None
        if s and written.endswith("proposal"):
            table = reads.pg_query(cfg.metadata_endpoint, "SELECT table_id, load_mode, business_key, watermark_col FROM "
                                   "aidq_metadata.source_table_def WHERE source_table = %s", (c.table,))[0]
            meta = [(r["column_name"], r["ordinal"], r["active"]) for r in reads.pg_query(
                cfg.metadata_endpoint, vp.META_COLUMNS_SQL, (table["table_id"],))]
            source = {r["column_name"]: r["data_type"] for r in reads.pg_query(
                injected["endpoint"], vp.SOURCE_COLUMNS_SQL, (c.table,))}
            change = ({"column_name": c.column, "data_type": c.proposed_type, "ordinal": c.ordinal}
                      if c.kind == "added" else {"column_name": c.column})
            v = vp.judge_column("ADD_COLUMN" if c.kind == "added" else "DEACTIVATE_COLUMN", change, table, source, meta)
            ok_rebuild = v["status"] == "VALIDATED" and v["requires_rebuild"] is rebuild
        rows.append({"change": cid, "expected": written, "written": s["written"] if s else None, "kind_ok": ok_kind,
                     "validator_rebuild_ok": ok_rebuild})
        checks[f"injected: {cid} -> {written}"] = ok_kind
        if ok_rebuild is not None:
            checks[f"injected: {cid} validated, requires_rebuild={rebuild}"] = ok_rebuild
    report = injected["state"]["report"]
    checks["injected: rebuild sentence in every incremental proposal rationale"] = report.count("bronze rebuild") >= 2
    return {"checks": checks, "rows": rows, "pass": all(checks.values())}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", required=True)
    ap.add_argument("--branch", default="drift-eval")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    endpoint = f"projects/netsuite-sample/branches/{args.branch}/endpoints/primary"
    assert "/branches/production/" not in endpoint, "never on the shared production source"
    run_ddl(args.profile, endpoint, CLEAN_DDL)
    clean = phase(args.profile, endpoint)
    run_ddl(args.profile, endpoint, INJECT_DDL)
    injected = phase(args.profile, endpoint)
    sc = score(clean, injected)
    date = dt.date.today().isoformat()
    lines = [f"# Schema drift eval, dev, {date}", "",
             f"Dev branch `{args.branch}` of netsuite-sample (never the shared production source). Real agent, dry run, "
             "model included.", "", "| Check | Result |", "|---|---|"]
    lines += [f"| {k} | {'PASS' if v else 'FAIL'} |" for k, v in sc["checks"].items()]
    lines += ["", f"**Overall: {'PASS' if sc['pass'] else 'FAIL'}**", "", "## Clean phase report", "",
              clean["state"]["report"], "", "## Injected phase report", "", injected["state"]["report"], "",
              "## DDL on the branch", ""] + [f"- `{s}`" for s in CLEAN_DDL + INJECT_DDL]
    md = "\n".join(lines) + "\n"
    out = ROOT / "evals" / "results"
    (out / f"drift_eval_{date}.md").write_text(md, encoding="utf-8")
    (out / f"drift_eval_{date}.json").write_text(json.dumps({"checks": sc["checks"], "rows": sc["rows"],
                                                             "branch": args.branch}, indent=2), encoding="utf-8")
    print(md)
    return 0 if sc["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
