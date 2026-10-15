"""Review CLI for agent proposals (design v2 section 7; plan #39 step 7). Run by a PERSON, never by dq-agent.

    python tools/review.py list                                  # pending and validated proposals
    python tools/review.py show 12                               # rationale, evidence, validation, trace
    python tools/review.py approve 12 [--severity HARD|SOFT] [--comment "..."]
    python tools/review.py reject 12 --reason "..."                # a real reason, at least 10 characters
    python tools/review.py correct-reason 12 --reason "..."        # fix the reason of your own rejection
    (all take --profile DEFAULT; the dev metadata branch only)

Every command runs as the caller in a transaction that starts with SET LOCAL ROLE aidq_reviewer (UPDATE of the
review columns and EXECUTE apply_proposal only). approve = VALIDATED -> APPROVED (reviewed_by, reviewed_at,
review_comment, approved_severity) and apply_proposal() in the same transaction, so a rule is applied exactly when
it is approved. The lifecycle trigger (migrations 001, 006) refuses anything else: approving an unvalidated proposal,
editing a proposal, or reviewing one's own (no_self_approval).

Proposals that change a hashed column set (ADD_COLUMN on an incremental table) are flagged "requires bronze rebuild";
after approval the rebuild commands are printed (the row-hash guard blocks normal runs until it is done).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

LIST_SQL = (
    "SELECT p.proposal_id, d.source_table, p.proposal_type, p.status, p.proposed_change->>'severity', "
    "p.proposed_change->>'rule_name', p.validation_result->>'summary', p.created_at "
    "FROM aidq_metadata.config_proposals p JOIN aidq_metadata.source_table_def d USING (table_id) "
    "WHERE p.status IN ('PENDING', 'VALIDATED', 'VALIDATION_FAILED') ORDER BY p.created_at")
SHOW_SQL = (
    "SELECT p.proposal_id, d.source_table, d.load_mode, p.proposal_type, p.status, p.proposed_change, p.rationale, "
    "p.evidence, p.confidence, p.proposed_by, p.agent_trace_id, p.validation_result, p.reviewed_by, p.reviewed_at, "
    "p.review_comment, p.approved_severity, p.requires_rebuild, p.created_at FROM aidq_metadata.config_proposals p "
    "JOIN aidq_metadata.source_table_def d USING (table_id) WHERE p.proposal_id = %s")
APPROVE_SQL = (
    "UPDATE aidq_metadata.config_proposals SET status = 'APPROVED', reviewed_by = %s, reviewed_at = now(), "
    "review_comment = %s, approved_severity = %s WHERE proposal_id = %s AND status = 'VALIDATED'")
REJECT_SQL = (
    "UPDATE aidq_metadata.config_proposals SET status = 'REJECTED', reviewed_by = %s, reviewed_at = now(), "
    "review_comment = %s WHERE proposal_id = %s AND status IN ('PENDING', 'VALIDATED', 'VALIDATION_FAILED', 'APPROVED')")
CORRECT_REASON_SQL = (
    "UPDATE aidq_metadata.config_proposals SET review_comment = %s WHERE proposal_id = %s AND status = 'REJECTED' "
    "AND reviewed_by = %s")
MIN_REASON = 10      # a rejection reason is shown to the agent (no-nagging) and is the audit trail: no placeholders
REBUILD_HINT = ("This change alters the hashed column set of an incremental table: normal runs are blocked until\n"
                "bronze is rebuilt. Dev: run the deploy-dev workflow with bronze_rebuild=true.")


def requires_rebuild(proposal_type: str, load_mode: str | None) -> bool:
    return proposal_type in ("ADD_COLUMN", "DEACTIVATE_COLUMN") and (load_mode or "").lower() == "incremental"


def needs_rebuild(row: dict) -> bool:
    """The validator's decision (config_proposals.requires_rebuild, migration 008) when it is set; else the rule it
    applies (column changes on incremental tables)."""
    if row.get("requires_rebuild") is not None:
        return bool(row["requires_rebuild"])
    return requires_rebuild(row["proposal_type"], row.get("load_mode"))


def render(row: dict) -> str:
    """Human-readable view of one proposal (pure)."""
    change, ev, val = row["proposed_change"] or {}, row["evidence"] or {}, row["validation_result"] or {}
    lines = [f"Proposal {row['proposal_id']}  {row['proposal_type']}  on {row['source_table']}  [{row['status']}]",
             f"  by {row['proposed_by']}  at {row['created_at']}  confidence {row['confidence']}",
             f"  trace: {row['agent_trace_id'] or '-'}"]
    if row.get("trace_url"):
        lines.append(f"  trace link: {row['trace_url']}")
    if needs_rebuild(row):
        lines.append("  ** requires bronze rebuild **")
    lines += ["", "Change:"] + [f"  {k}: {v}" for k, v in change.items()]
    lines += ["", "Rationale:", "  " + (row["rationale"] or "").replace("\n", "\n  ")]
    if ev:
        lines += ["", "Evidence:", "  " + json.dumps(ev, default=str)[:800]]
    if val:
        lines += ["", f"Validation: {val.get('summary')}"]
        for p in val.get("problems", []):
            lines.append(f"  problem: {p}")
        for f in val.get("flags", []):
            lines.append(f"  flag: {f}")
    if row.get("reviewed_by"):
        lines += ["", f"Reviewed by {row['reviewed_by']} at {row['reviewed_at']}: {row['review_comment'] or ''}"
                  + (f" (severity set to {row['approved_severity']})" if row.get("approved_severity") else "")]
    return "\n".join(lines)


def _row(cur_row, cols) -> dict:
    return dict(zip(cols, cur_row))


def show(conn, pid: int) -> dict | None:
    cur = conn.execute(SHOW_SQL, (pid,))
    r = cur.fetchone()
    return None if r is None else _row(r, [c.name for c in cur.description])


def approve(conn, pid: int, reviewer: str, severity: str | None, comment: str | None) -> str:
    row = show(conn, pid)
    if row is None:
        raise SystemExit(f"proposal {pid} not found")
    if row["status"] != "VALIDATED":
        raise SystemExit(f"proposal {pid} is {row['status']}: only VALIDATED proposals can be approved")
    if severity and row["proposal_type"] != "ADD_DQ_RULE":
        raise SystemExit("--severity applies to ADD_DQ_RULE proposals only")
    conn.execute(APPROVE_SQL, (reviewer, comment, severity, pid))
    applied = conn.execute("SELECT aidq_metadata.apply_proposal(%s, %s)", (pid, reviewer)).fetchone()[0]
    return applied + (("\n" + REBUILD_HINT) if needs_rebuild(row) else "")


def check_reason(reason: str | None) -> str:
    reason = (reason or "").strip()
    if len(reason) < MIN_REASON:
        raise SystemExit(f"--reason must be a real reason of at least {MIN_REASON} characters (it is shown to the agent "
                         "so it does not propose this again, and it is the audit trail)")
    return reason


def reject(conn, pid: int, reviewer: str, reason: str) -> str:
    reason = check_reason(reason)
    n = conn.execute(REJECT_SQL, (reviewer, reason, pid)).rowcount
    if n != 1:
        raise SystemExit(f"proposal {pid} was not rejected (not found, or already decided)")
    return f"Proposal {pid} rejected by {reviewer}"


def trace_url(profile: str | None, row: dict) -> str | None:
    """The MLflow trace link of a proposal (plan #39 step 8): the workspace host from the CLI profile, the experiment
    id from the proposal's evidence. None for untraced runs; never fails `show`."""
    exp = (row.get("evidence") or {}).get("trace_experiment_id")
    if not exp or not row.get("agent_trace_id"):
        return None
    try:
        sys.path.insert(0, str(ROOT.parent / "src"))
        from aidq_dq_agent.tracing import trace_link
        from databricks.sdk.core import Config

        return trace_link(Config(profile=profile).host, str(exp), row["agent_trace_id"])
    except Exception:  # noqa: BLE001
        return None


def correct_reason(conn, pid: int, reviewer: str, reason: str, now: str) -> str:
    """Replace the reason of a REJECTED proposal, by the reviewer who rejected it. The status, reviewer and review
    time stay; the previous reason is kept at the end of the comment (audit trail)."""
    reason = check_reason(reason)
    row = show(conn, pid)
    if row is None:
        raise SystemExit(f"proposal {pid} not found")
    if row["status"] != "REJECTED":
        raise SystemExit(f"proposal {pid} is {row['status']}: only a REJECTED proposal's reason can be corrected")
    if row["reviewed_by"] != reviewer:
        raise SystemExit(f"proposal {pid} was rejected by {row['reviewed_by']}: only that reviewer can correct the reason")
    old = (row["review_comment"] or "").split("\n[corrected ")[0]
    comment = f"{reason}\n[corrected {now} by {reviewer}; was: {old!r}]"
    n = conn.execute(CORRECT_REASON_SQL, (comment, pid, reviewer)).rowcount
    if n != 1:
        raise SystemExit(f"proposal {pid}: the reason was not corrected")
    return f"Proposal {pid}: reason corrected by {reviewer}"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--profile", default="DEFAULT")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    s = sub.add_parser("show")
    s.add_argument("proposal_id", type=int)
    a = sub.add_parser("approve")
    a.add_argument("proposal_id", type=int)
    a.add_argument("--severity", choices=["HARD", "SOFT"])
    a.add_argument("--comment")
    r = sub.add_parser("reject")
    r.add_argument("proposal_id", type=int)
    r.add_argument("--reason", required=True)
    c = sub.add_parser("correct-reason", help="replace the reason of a proposal you rejected (the old one is kept)")
    c.add_argument("proposal_id", type=int)
    c.add_argument("--reason", required=True)
    args = p.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):     # agent rationales may hold non-ASCII (Windows consoles default to cp1252)
        sys.stdout.reconfigure(encoding="utf-8")

    sys.path.insert(0, str(ROOT))
    import migrate  # noqa: E402

    conn = migrate.connect("dev", args.profile)
    try:
        reviewer = conn.execute("SELECT session_user").fetchone()[0]
        conn.execute("SET LOCAL ROLE aidq_reviewer")
        if args.cmd == "list":
            rows = conn.execute(LIST_SQL).fetchall()
            for pid, table, ptype, status, sev, name, summary, created in rows:
                print(f"{pid:>5}  {status:<17} {ptype:<12} {table:<28} {sev or '':<4} {name or ''}  | {summary or '-'}")
            print(f"{len(rows)} open proposal(s)")
            conn.rollback()
        elif args.cmd == "show":
            row = show(conn, args.proposal_id)
            if row:
                row["trace_url"] = trace_url(args.profile, row)
            print(render(row) if row else f"proposal {args.proposal_id} not found")
            conn.rollback()
        elif args.cmd == "approve":
            print(approve(conn, args.proposal_id, reviewer, args.severity, args.comment))
            conn.commit()
        elif args.cmd == "correct-reason":
            import datetime as dt

            now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
            print(correct_reason(conn, args.proposal_id, reviewer, args.reason, now))
            conn.commit()
        else:
            print(reject(conn, args.proposal_id, reviewer, args.reason))
            conn.commit()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
