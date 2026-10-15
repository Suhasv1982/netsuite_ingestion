"""The DQ agent's write toolbox: its only two writes (design v2 section 4; plan #39 step 3).

* submit_proposal: one ADD_DQ_RULE proposal, status PENDING (the lifecycle trigger enforces it), with rationale,
  structured evidence, confidence, proposed_by and agent_trace_id.
* report_finding: one incident, category DQ_NOT_EXPRESSIBLE, for problems a row rule cannot express (duplicates,
  orphans, late rows). One OPEN incident per (table, finding type), via the open-fingerprint unique index.

Deterministic code decides before anything is written: arguments are checked (table known, severity, a single
non-empty expression of bounded length, evidence with counts), and the no-nagging rule skips a rule that is
already open (PENDING / VALIDATED), or was rejected in the last 30 days unless its violation count changed by more
than 50%. The SQL validator (step 5) checks the expression itself; nothing here trusts it.

The connection is the caller's: dq-agent in its job (role aidq_agent: INSERT only), or the owner locally.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable

SEVERITIES = ("SOFT", "HARD")
MAX_EXPR = 1000
REJECTED_LOOKBACK_DAYS = 30
NAG_CHANGE = 0.5          # a rejected rule may be proposed again when its violation count changed by more than 50%
FINDING_TYPES = ("duplicate_business_key", "orphan_records", "late_arriving_rows", "other")

TOOL_DEFS = [
    {"name": "submit_proposal",
     "description": "Propose one row-level data-quality rule (ADD_DQ_RULE) for a table. It is validated by code and "
                    "reviewed by a person before it applies. Default severity SOFT; HARD only for unambiguous "
                    "violations such as a NULL business key. The expression must handle NULLs explicitly unless it "
                    "is a NULL check, e.g. `amount IS NULL OR amount >= 0`.",
     "input_schema": {"type": "object", "additionalProperties": False,
                      "required": ["table", "rule_name", "rule_expr", "severity", "rationale", "evidence", "confidence"],
                      "properties": {
                          "table": {"type": "string"},
                          "rule_name": {"type": "string", "description": "short, e.g. 'Amount Not Negative'"},
                          "rule_expr": {"type": "string", "description": "one Spark SQL boolean expression that is "
                                        "TRUE for a valid row"},
                          "severity": {"type": "string", "enum": list(SEVERITIES)},
                          "rationale": {"type": "string", "description": "violation count and rate on bronze, example "
                                        "values (aggregated), and why SOFT or HARD"},
                          "evidence": {"type": "object", "additionalProperties": True, "required": ["violations", "rows"],
                                       "properties": {"violations": {"type": "integer", "minimum": 0},
                                                      "rows": {"type": "integer", "minimum": 1},
                                                      "profile_fields": {"type": "array", "items": {"type": "string"}}}},
                          "confidence": {"type": "number", "minimum": 0, "maximum": 1}}}},
    {"name": "report_finding",
     "description": "Report a data-quality problem that a row-level rule cannot express (duplicate business keys, "
                    "orphan records, late-arriving rows) as an incident. Do not invent a rule for these.",
     "input_schema": {"type": "object", "additionalProperties": False,
                      "required": ["table", "finding_type", "summary", "evidence"],
                      "properties": {
                          "table": {"type": "string"},
                          "finding_type": {"type": "string", "enum": list(FINDING_TYPES)},
                          "summary": {"type": "string"},
                          "evidence": {"type": "object", "additionalProperties": True}}}},
]


def normalize_expr(expr: str) -> str:
    """For the no-nagging comparison only: case and whitespace do not make a different rule."""
    return re.sub(r"\s+", " ", expr.strip().lower())


def check_proposal(args: dict, known_tables: set[str]) -> list[str]:
    """Argument problems (empty list = acceptable to consider)."""
    p = []
    if args.get("table") not in known_tables:
        p.append(f"unknown table {args.get('table')!r}")
    if args.get("severity") not in SEVERITIES:
        p.append(f"severity must be one of {SEVERITIES}")
    expr = args.get("rule_expr")
    if not isinstance(expr, str) or not expr.strip():
        p.append("rule_expr is empty")
    elif len(expr) > MAX_EXPR:
        p.append(f"rule_expr is longer than {MAX_EXPR} characters")
    if not isinstance(args.get("rule_name"), str) or not args["rule_name"].strip():
        p.append("rule_name is empty")
    if not isinstance(args.get("rationale"), str) or len(args["rationale"].strip()) < 20:
        p.append("rationale is missing or too short")
    ev = args.get("evidence")
    if not isinstance(ev, dict) or not isinstance(ev.get("violations"), int) or not isinstance(ev.get("rows"), int) \
            or ev["violations"] < 0 or ev["rows"] < 1:
        p.append("evidence needs integer violations >= 0 and rows >= 1")
    c = args.get("confidence")
    if not isinstance(c, (int, float)) or isinstance(c, bool) or not 0 <= c <= 1:
        p.append("confidence must be a number from 0 to 1")
    return p


def nag_decision(expr: str, violations: int, existing: list[dict]) -> str | None:
    """Why the proposal must be skipped (no nagging), or None. `existing`: proposals of the same table with keys
    status, rule_expr, violations (from their evidence; None if absent)."""
    key = normalize_expr(expr)
    for e in existing:
        if normalize_expr(e.get("rule_expr") or "") != key:
            continue
        if e["status"] in ("PENDING", "VALIDATED"):
            return f"an equivalent proposal is already {e['status']} (proposal {e.get('proposal_id')})"
        if e["status"] == "REJECTED":
            old = e.get("violations")
            if old is None or old == 0:
                changed = violations > 0 and old == 0
            else:
                changed = abs(violations - old) / old > NAG_CHANGE
            if not changed:
                return (f"an equivalent proposal was rejected in the last {REJECTED_LOOKBACK_DAYS} days (proposal "
                        f"{e.get('proposal_id')}, {old} violations; now {violations}, change <= 50%)")
    return None


class WriteTools:
    def __init__(self, connect: Callable[[], Any], proposed_by: str, trace_id: str, dry_run: bool = False):
        """`connect()` returns a psycopg connection as the agent's identity. `dry_run`: decide and report, write
        nothing (evals)."""
        self.connect, self.proposed_by, self.trace_id, self.dry_run = connect, proposed_by, trace_id, dry_run
        self.written: list[dict] = []

    def _tables(self, conn) -> dict[str, int]:
        """source_table -> table_id."""
        rows = conn.execute("SELECT table_id, source_table FROM aidq_metadata.source_table_def").fetchall()
        return {name: tid for tid, name in rows}

    def submit_proposal(self, args: dict) -> dict:
        with self.connect() as conn:
            tables = self._tables(conn)
            problems = check_proposal(args, set(tables))
            if problems:
                conn.rollback()
                return {"status": "rejected_by_code", "problems": problems}
            rows = conn.execute(
                "SELECT proposal_id, status, proposed_change->>'rule_expr', (evidence->>'violations')::bigint "
                "FROM aidq_metadata.config_proposals WHERE table_id = %s AND proposal_type = 'ADD_DQ_RULE' AND "
                "(status IN ('PENDING', 'VALIDATED') OR (status = 'REJECTED' AND reviewed_at >= now() - "
                "make_interval(days => %s)))", (tables[args["table"]], REJECTED_LOOKBACK_DAYS)).fetchall()
            existing = [{"proposal_id": r[0], "status": r[1], "rule_expr": r[2], "violations": r[3]} for r in rows]
            why = nag_decision(args["rule_expr"], args["evidence"]["violations"], existing)
            if why:
                conn.rollback()
                return {"status": "skipped", "reason": why}
            change = {"rule_name": args["rule_name"].strip(), "rule_expr": args["rule_expr"].strip(),
                      "severity": args["severity"]}
            if self.dry_run:
                conn.rollback()
                self.written.append({"kind": "proposal", "table": args["table"], **change, "dry_run": True})
                return {"status": "ok", "dry_run": True, "proposal_id": None}
            pid = conn.execute(
                "INSERT INTO aidq_metadata.config_proposals (proposal_type, table_id, proposed_change, rationale, "
                "evidence, proposed_by, agent_trace_id, confidence) VALUES ('ADD_DQ_RULE', %s, %s::jsonb, %s, "
                "%s::jsonb, %s, %s, %s) RETURNING proposal_id",
                (tables[args["table"]], json.dumps(change), args["rationale"].strip(),
                 json.dumps(args["evidence"], default=str), self.proposed_by, self.trace_id,
                 float(args["confidence"]))).fetchone()[0]
            conn.commit()
        self.written.append({"kind": "proposal", "proposal_id": pid, "table": args["table"], **change})
        return {"status": "ok", "proposal_id": pid}

    def report_finding(self, args: dict) -> dict:
        if args.get("finding_type") not in FINDING_TYPES or not str(args.get("summary") or "").strip():
            return {"status": "rejected_by_code", "problems": [f"finding_type must be one of {FINDING_TYPES} and "
                                                               "summary non-empty"]}
        fingerprint = f"dq:{args.get('table')}:{args['finding_type']}"
        with self.connect() as conn:
            tables = self._tables(conn)
            if args.get("table") not in tables:
                conn.rollback()
                return {"status": "rejected_by_code", "problems": [f"unknown table {args.get('table')!r}"]}
            if self.dry_run:
                conn.rollback()
                self.written.append({"kind": "incident", "fingerprint": fingerprint, "dry_run": True})
                return {"status": "ok", "dry_run": True, "incident_id": None}
            row = conn.execute(
                "INSERT INTO aidq_metadata.incidents (table_id, category, summary, agent_trace_id, fingerprint, "
                "evidence) VALUES (%s, 'DQ_NOT_EXPRESSIBLE', %s, %s, %s, %s::jsonb) "
                "ON CONFLICT (fingerprint) WHERE status = 'OPEN' AND fingerprint IS NOT NULL DO NOTHING "
                "RETURNING incident_id",
                (tables[args["table"]], args["summary"].strip(), self.trace_id, fingerprint,
                 json.dumps({"finding_type": args["finding_type"], "evidence": args.get("evidence"),
                             "reported_by": self.proposed_by}, default=str))).fetchone()
            conn.commit()
        if row is None:
            return {"status": "skipped", "reason": f"an OPEN incident {fingerprint} already exists"}
        self.written.append({"kind": "incident", "incident_id": row[0], "fingerprint": fingerprint})
        return {"status": "ok", "incident_id": row[0]}

    # -- shared foundation (agents_next_design section 1): column proposals and incidents of any category ---------

    def submit_column_proposal(self, args: dict) -> dict:
        """One ADD_COLUMN / DEACTIVATE_COLUMN proposal (schema drift agent), status PENDING. No nagging: skipped when
        an equivalent proposal (same type, table and column) is open, or was rejected in the last 30 days."""
        ptype, col = args.get("proposal_type"), str(args.get("column_name") or "").strip()
        problems = [] if ptype in COLUMN_PROPOSAL_TYPES else [f"proposal_type must be one of {COLUMN_PROPOSAL_TYPES}"]
        if not col:
            problems.append("column_name is empty")
        if ptype == "ADD_COLUMN" and not str(args.get("data_type") or "").strip():
            problems.append("ADD_COLUMN needs a data_type")
        if not isinstance(args.get("rationale"), str) or len(args["rationale"].strip()) < 20:
            problems.append("rationale is missing or too short")
        with self.connect() as conn:
            tables = self._tables(conn)
            if args.get("table") not in tables:
                problems.append(f"unknown table {args.get('table')!r}")
            if problems:
                conn.rollback()
                return {"status": "rejected_by_code", "problems": problems}
            open_ = conn.execute(
                "SELECT proposal_id, status FROM aidq_metadata.config_proposals WHERE table_id = %s AND proposal_type = "
                "%s AND proposed_change->>'column_name' = %s AND (status IN ('PENDING', 'VALIDATED', 'APPROVED') OR "
                "(status = 'REJECTED' AND reviewed_at >= now() - make_interval(days => %s)))",
                (tables[args["table"]], ptype, col, REJECTED_LOOKBACK_DAYS)).fetchone()
            if open_:
                conn.rollback()
                return {"status": "skipped", "reason": f"an equivalent proposal is {open_[1]} (proposal {open_[0]})"}
            change = {"column_name": col, **({"data_type": args["data_type"].strip().upper()} if args.get("data_type")
                                             else {}), **({"ordinal": int(args["ordinal"])} if args.get("ordinal") else {})}
            if self.dry_run:
                conn.rollback()
                self.written.append({"kind": "proposal", "table": args["table"], "proposal_type": ptype, **change,
                                     "dry_run": True})
                return {"status": "ok", "dry_run": True, "proposal_id": None}
            pid = conn.execute(
                "INSERT INTO aidq_metadata.config_proposals (proposal_type, table_id, proposed_change, rationale, "
                "evidence, proposed_by, agent_trace_id, confidence) VALUES (%s, %s, %s::jsonb, %s, %s::jsonb, %s, %s, "
                "%s) RETURNING proposal_id",
                (ptype, tables[args["table"]], json.dumps(change), args["rationale"].strip(),
                 json.dumps(args.get("evidence") or {}, default=str), self.proposed_by, self.trace_id,
                 None if args.get("confidence") is None else float(args["confidence"]))).fetchone()[0]
            conn.commit()
        self.written.append({"kind": "proposal", "proposal_id": pid, "table": args["table"], "proposal_type": ptype,
                             **change})
        return {"status": "ok", "proposal_id": pid}

    def report_incident(self, args: dict) -> dict:
        """One incident of an allowed category with a stable fingerprint (one OPEN incident per fingerprint). The
        table may be unknown to the metadata (a source table not onboarded yet): then table_id stays NULL."""
        cat, fp, summary = args.get("category"), str(args.get("fingerprint") or "").strip(), str(args.get("summary") or "")
        if cat not in INCIDENT_CATEGORIES or not fp or not summary.strip():
            return {"status": "rejected_by_code",
                    "problems": [f"category must be one of {INCIDENT_CATEGORIES}; fingerprint and summary non-empty"]}
        with self.connect() as conn:
            table_id = self._tables(conn).get(args.get("table"))
            if self.dry_run:
                conn.rollback()
                self.written.append({"kind": "incident", "fingerprint": fp, "category": cat, "dry_run": True})
                return {"status": "ok", "dry_run": True, "incident_id": None}
            row = conn.execute(
                "INSERT INTO aidq_metadata.incidents (table_id, category, summary, suggested_fix, agent_trace_id, "
                "fingerprint, evidence) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) "
                "ON CONFLICT (fingerprint) WHERE status = 'OPEN' AND fingerprint IS NOT NULL DO NOTHING "
                "RETURNING incident_id",
                (table_id, cat, summary.strip(), args.get("suggested_fix"), self.trace_id, fp,
                 json.dumps({**(args.get("evidence") or {}), "reported_by": self.proposed_by}, default=str))).fetchone()
            conn.commit()
        if row is None:
            return {"status": "skipped", "reason": f"an OPEN incident {fp} already exists"}
        self.written.append({"kind": "incident", "incident_id": row[0], "fingerprint": fp, "category": cat})
        return {"status": "ok", "incident_id": row[0]}


COLUMN_PROPOSAL_TYPES = ("ADD_COLUMN", "DEACTIVATE_COLUMN")
INCIDENT_CATEGORIES = ("SCHEMA_TYPE_CHANGE", "SCHEMA_DRIFT")    # the drift agent's; DQ findings use report_finding


def sdk_connect(endpoint: str):
    """A connect() for the dq-agent job (spark_python_task as run_as dq-agent): no CLI, the run identity's token."""
    def connect():
        import psycopg
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        ep = w.postgres.get_endpoint(name=endpoint)
        token = w.postgres.generate_database_credential(endpoint=endpoint).token
        user = w.current_user.me().user_name
        return psycopg.connect(host=ep.status.hosts.host, dbname="databricks_postgres", user=user, password=token,
                               sslmode="require", connect_timeout=60)
    return connect
