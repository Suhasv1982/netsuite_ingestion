"""The read-only tools and the MCP server: the monitor's five (docs/phase3_mcp_plan.md, section 3) and the DQ
recommender's four (dq_tools.py; docs/dq_recommender_design.md section 4).

Each tool returns {"status": "ok" | "unavailable" | "error", ...}; results pass through logic.redact. `Tools` holds
the logic and takes a Reads-like object and a clock, so tests run it with fakes.
"""

from __future__ import annotations

import datetime as dt
import functools
from typing import Callable

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from . import config, logic
from .dq_tools import DqTools
from .knowledge_tools import KnowledgeTools
from .reads import Unavailable

UTC = dt.timezone.utc
AUDIT_CONFIG_ACTIONS = ("create", "update", "reset", "delete", "edit", "changeJobAcl", "changePipelineAcl",
                        "setPermissions", "updatePermissions")


def _iso(value) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def tool_result(fn: Callable) -> Callable:
    """Turn exceptions into a status, and redact every result."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            out = {"status": "ok", **fn(*args, **kwargs)}
        except config.ConfigError as e:
            out = {"status": "error", "error": str(e)}
        except Unavailable as e:
            out = {"status": "unavailable", "reason": str(e)}
        except Exception as e:  # noqa: BLE001 -- report, never crash the session
            out = {"status": "error", "error": f"{type(e).__name__}: {logic.truncate(str(e), 300)}"}
        return logic.redact(out)
    return wrapper


MONITOR_TOOLS = ("get_recent_job_runs", "get_table_health", "get_pipeline_errors", "compare_bronze_to_source",
                 "get_recent_deploys")
DQ_TOOLS = ("get_dq_rules", "get_open_and_rejected_proposals", "profile_table", "get_rejects_summary")
KNOWLEDGE_TOOLS = ("get_impact", "get_upstream", "explain_history", "search_metadata")


class Tools(DqTools, KnowledgeTools):
    def __init__(self, reads, now: Callable[[], dt.datetime] = lambda: dt.datetime.now(UTC)):
        self.reads, self.now = reads, now

    # -- get_table_health ----------------------------------------------------------

    @tool_result
    def get_table_health(self, env: str = "dev", table: str | None = None) -> dict:
        cfg = config.env_config(env)
        tables = list(config.check_table(cfg, table))
        rows = self.reads.pg_query(
            cfg.metadata_endpoint,
            "SELECT source_table, layer, run_id, status, error, rows_read, rows_written, rows_rejected, reject_rate, "
            "discard_threshold, threshold_breached, active_rules, open_proposals, started_at, ended_at "
            "FROM aidq_metadata.v_table_health WHERE source_table = ANY(%s) ORDER BY source_table, layer",
            (tables,))
        guard = self.reads.pg_query(
            cfg.metadata_endpoint,
            "SELECT run_id, status, rows_read AS event_log_reads, error, started_at FROM aidq_metadata.run_audit "
            "WHERE layer = 'guard' ORDER BY started_at DESC NULLS LAST, audit_id DESC LIMIT 1")
        health = [{**r, "reject_rate": None if r["reject_rate"] is None else float(r["reject_rate"]),
                   "discard_threshold": None if r["discard_threshold"] is None else float(r["discard_threshold"]),
                   "error": logic.truncate(r["error"]), "started_at": _iso(r["started_at"]),
                   "ended_at": _iso(r["ended_at"])} for r in rows]
        latest_guard = {**guard[0], "started_at": _iso(guard[0]["started_at"])} if guard else None
        return {"env": env, "tables": health, "latest_guard": latest_guard,
                "note": "layer 'ledger' stores ledger pairs in rows_read and mismatches in rows_rejected; "
                        "guard WARN with 2 event-log reads is the normal baseline"}

    # -- get_recent_job_runs -----------------------------------------------------------

    @tool_result
    def get_recent_job_runs(self, env: str = "dev", days: int | None = None, job: str | None = None) -> dict:
        cfg = config.env_config(env)
        days = config.clamp("days", days, 3, 14)
        jobs = config.check_alias("job", cfg.jobs, job)
        end = self.now()
        start = end - dt.timedelta(days=days)
        try:   # successful deploys (any could have changed a schedule); None when GitHub is unavailable
            deploy_times = [dt.datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
                            for r in [r for wf in cfg.deploy_workflows for r in self.reads.workflow_runs(wf, (end - dt.timedelta(days=60)).date())]
                            if r.get("conclusion") == "success"]
        except Exception:  # noqa: BLE001 -- the old rule (first PERIODIC run) still applies
            deploy_times = []
        out = []
        for alias, name in jobs.items():
            jid = self.reads.job_id(name)
            if jid is None:
                out.append({"job": alias, "name": name, "found": False})
                continue
            schedule = self.reads.job_settings(jid).get("schedule") or {}
            runs = sorted(self.reads.job_runs(jid, start), key=lambda r: r.get("start_time") or 0)
            items = []
            for r in runs:
                st = r.get("state", {})
                items.append({"run_id": r.get("run_id"), "start": _iso(logic.from_epoch_ms(r.get("start_time"))),
                              "end": _iso(logic.from_epoch_ms(r.get("end_time"))), "trigger": r.get("trigger"),
                              "life_cycle_state": st.get("life_cycle_state"), "result_state": st.get("result_state"),
                              "state_message": logic.truncate(st.get("state_message"))})
            entry = {"job": alias, "name": name, "found": True,
                     "schedule": {"cron": schedule.get("quartz_cron_expression"),
                                  "timezone": schedule.get("timezone_id"),
                                  "pause_status": schedule.get("pause_status")} if schedule else None,
                     "runs": logic.bounded(items, config.DEFAULT_LIMIT)}
            if schedule.get("quartz_cron_expression"):
                if schedule.get("timezone_id", "UTC") != "UTC":
                    entry["missed_schedules"] = None
                    entry["missed_schedules_note"] = "only UTC schedules are checked"
                else:
                    try:
                        periodic = [logic.from_epoch_ms(r["start_time"]) for r in runs
                                    if r.get("start_time") and r.get("trigger") == "PERIODIC"]
                        known = logic.schedule_known_since(self.reads.job_created(jid), deploy_times, end)
                        missed = logic.missed_schedules(
                            schedule["quartz_cron_expression"], schedule.get("pause_status") == "PAUSED",
                            [logic.from_epoch_ms(r["start_time"]) for r in runs if r.get("start_time")], start, end,
                            first_scheduled_run=min(periodic, default=None), schedule_known_since=known)
                        entry["schedule_known_since"] = _iso(known)
                        entry["missed_schedules"] = [_iso(m) for m in missed["confirmed"]]
                        entry["missed_schedules_unconfirmed"] = [_iso(m) for m in missed["unconfirmed"]]
                    except ValueError as e:
                        entry["missed_schedules"] = None
                        entry["missed_schedules_note"] = str(e)
            out.append(entry)
        return {"env": env, "window": {"start": _iso(start), "end": _iso(end)}, "jobs": out,
                "note": "missed_schedules = cron fire times with no run started within 15 minutes, after proof "
                        "the schedule was active (the first scheduled (PERIODIC) run in the window, or "
                        "schedule_known_since: the later of job creation and the last successful deploy); "
                        "missed_schedules_unconfirmed = such fire times before that proof (the schedule may not "
                        "have existed or been unpaused yet); a missed run sends no failure email"}

    # -- get_pipeline_errors ----------------------------------------------------------------

    @tool_result
    def get_pipeline_errors(self, env: str = "dev", hours: int | None = None) -> dict:
        cfg = config.env_config(env)
        hours = config.clamp("hours", hours, 24, 168)
        since = self.now() - dt.timedelta(hours=hours)
        pipelines = []
        for alias, name in cfg.pipelines.items():
            pid = self.reads.pipeline_id(name)
            if pid is None:
                pipelines.append({"pipeline": alias, "name": name, "found": False})
                continue
            events = []
            for e in self.reads.pipeline_events(pid, config.HARD_LIMIT):
                ts = e.get("timestamp")
                if ts and dt.datetime.fromisoformat(ts.replace("Z", "+00:00")) < since:
                    continue
                exc = ((e.get("error") or {}).get("exceptions") or [{}])[0]
                events.append({"time": ts, "level": e.get("level"), "event_type": e.get("event_type"),
                               "flow": (e.get("origin") or {}).get("flow_name"),
                               "update_id": (e.get("origin") or {}).get("update_id"),
                               "message": logic.truncate(e.get("message")),
                               "exception": logic.truncate(exc.get("message"))})
            entry = {"pipeline": alias, "name": name, "found": True,
                     "events": logic.bounded(events, config.DEFAULT_LIMIT)}
            if alias == "ingestion":
                entry["failed_updates"] = self._credential_checks(cfg, pid, since)
            pipelines.append(entry)
        try:
            failed = self.reads.pg_query(
                cfg.metadata_endpoint,
                "SELECT a.run_id, coalesce(d.source_table, '(pipeline)') AS source_table, a.layer, a.status, a.error, "
                "a.started_at FROM aidq_metadata.run_audit a LEFT JOIN aidq_metadata.source_table_def d "
                "USING (table_id) WHERE a.status = 'FAILED' AND a.started_at >= %s ORDER BY a.started_at DESC LIMIT %s",
                (since, config.HARD_LIMIT))
            audit = logic.bounded([{**r, "error": logic.truncate(r["error"]), "started_at": _iso(r["started_at"])}
                                   for r in failed], config.DEFAULT_LIMIT)
        except Unavailable as e:
            audit = {"unavailable": str(e)}
        return {"env": env, "since": _iso(since), "pipelines": pipelines, "failed_run_audit": audit}

    def _credential_checks(self, cfg, pipeline_id: str, since: dt.datetime):
        """Failed updates of the ingestion pipeline with who started them and the stored credential's age then
        (logic.credential_checks). Refreshes: successful refresh_credentials tasks of the ingestion job, from two
        days before the window (the credential in use may predate it)."""
        try:
            updates = self.reads.pipeline_updates(pipeline_id)
            job_id = self.reads.job_id(cfg.jobs["ingestion"])
            refreshes = []
            for run in (self.reads.job_runs_with_tasks(job_id, since - dt.timedelta(days=2)) if job_id else []):
                for t in run.get("tasks") or []:
                    if t.get("task_key") == logic.REFRESH_TASK and (t.get("state") or {}).get("result_state") == "SUCCESS"                             and t.get("end_time"):
                        refreshes.append(dt.datetime.fromtimestamp(t["end_time"] / 1000, dt.timezone.utc))
            return logic.credential_checks(updates, refreshes, since)
        except Exception as e:  # noqa: BLE001 -- an extra fact; it must never break the error report
            return {"unavailable": logic.truncate(f"{type(e).__name__}: {e}")}

    # -- compare_bronze_to_source -------------------------------------------------------------

    @tool_result
    def compare_bronze_to_source(self, env: str = "dev", table: str | None = None, since: str | None = None) -> dict:
        cfg = config.env_config(env)
        tables = config.check_table(cfg, table)
        since_d = None
        if since is not None:
            try:
                since_d = dt.date.fromisoformat(since)
            except ValueError:
                raise config.ConfigError("since must be a date YYYY-MM-DD") from None
        source, extra, bronze, versions = {}, {}, {}, []
        for t in tables:
            if t in cfg.full_load_tables:
                [row] = self.reads.pg_query(cfg.source_endpoint, f"SELECT count(*) AS n FROM netsuite.{t}")
                source[(t, None)] = row["n"]
                [row] = self.reads.sql(f"SELECT count(*) AS n FROM {cfg.catalog}.poc_bronze.{t}")
                bronze[(t, None)] = int(row["n"])
                continue
            key = cfg.business_keys[t]
            where = "WHERE updated_date >= %s" if since_d else ""
            for r in self.reads.pg_query(
                    cfg.source_endpoint,
                    f"SELECT updated_date::date::text AS d, count(*) AS n, count(DISTINCT {key}) AS k "
                    f"FROM netsuite.{t} {where} GROUP BY 1", (since_d,) if since_d else ()):
                source[(t, r["d"])] = r["n"]
                if r["n"] > r["k"]:
                    extra[(t, r["d"])] = r["n"] - r["k"]
            if any(tt == t for tt, _ in extra):
                versions += [{"table": t, **r} for r in self.reads.pg_query(
                    cfg.source_endpoint,
                    f"SELECT updated_date::date::text AS d, {key}::text AS k FROM netsuite.{t} {where} "
                    "GROUP BY 1, 2 HAVING count(*) > 1", (since_d,) if since_d else ())]
            bwhere = f"WHERE _snapshot_date >= DATE'{since_d.isoformat()}'" if since_d else ""
            for r in self.reads.sql(f"SELECT cast(_snapshot_date AS string) AS d, count(*) AS n "
                                    f"FROM {cfg.catalog}.poc_bronze.{t} {bwhere} GROUP BY 1"):
                bronze[(t, r["d"])] = int(r["n"])
        gaps = logic.date_gaps(source, bronze, extra)
        totals = [{"table": t, "source": sum(n for (tt, _), n in source.items() if tt == t),
                   "bronze": sum(n for (tt, _), n in bronze.items() if tt == t)} for t in tables]
        return {"env": env, "since": since, "verdict": "gaps" if gaps else "identical", "totals": totals,
                "gaps": logic.bounded(gaps, config.HARD_LIMIT),
                "same_day_versions": logic.bounded(logic.same_day_versions(versions), config.HARD_LIMIT),
                "note": "gaps: every (table, date) whose bronze row count differs from the source. same_day_versions "
                        "(info, not a problem): business keys with several source rows on one updated_date day; "
                        "bronze loads every version (rows are keyed by content), silver keeps one per key"}

    # -- get_recent_deploys ---------------------------------------------------------------------

    @tool_result
    def get_recent_deploys(self, env: str = "dev", days: int | None = None) -> dict:
        cfg = config.env_config(env)
        days = config.clamp("days", days, 7, 30)
        since = self.now() - dt.timedelta(days=days)
        try:
            deploys = []
            for wf in cfg.deploy_workflows:
                for r in self.reads.workflow_runs(wf, since.date()):
                    deploys.append({"workflow": wf, "created": r.get("created_at"), "event": r.get("event"),
                                    "status": r.get("status"), "conclusion": r.get("conclusion"),
                                    "sha": (r.get("head_sha") or "")[:7], "title": logic.truncate(r.get("display_title"), 120)})
            deploys.sort(key=lambda d: d["created"] or "", reverse=True)
            deploy_out = logic.bounded(deploys, config.DEFAULT_LIMIT)
        except Unavailable as e:
            deploy_out = {"unavailable": str(e)}

        try:
            changes_out = logic.bounded(self._config_changes(cfg, since), config.DEFAULT_LIMIT)
        except Unavailable as e:
            # e.g. a service principal without SELECT on system.access.audit (the system catalog cannot be granted
            # on Free Edition): deploys are still reported; changes made outside CI are then not visible
            changes_out = {"unavailable": str(e)}
        return {"env": env, "since": _iso(since), "deploys": deploy_out,
                "config_changes": changes_out,
                "note": "config_changes come from system.access.audit, which lags by several minutes; 'unavailable' "
                        "when this identity cannot read it (then only changes made through CI deploys are visible)"}

    def _config_changes(self, cfg, since) -> list[dict]:
        ids = {}
        for alias, name in cfg.jobs.items():
            if (jid := self.reads.job_id(name)) is not None:
                ids[str(jid)] = f"job:{alias}"
        for alias, name in cfg.pipelines.items():
            if (pid := self.reads.pipeline_id(name)) is not None:
                ids[pid] = f"pipeline:{alias}"
        roles = {self.reads.current_user(): "owner",
                 **{app: name for app, name in self.reads.service_principals().items()
                    if name in ("ci-dev", "ci-prod", "data-generator")}}
        actions = ", ".join(f"'{a}'" for a in AUDIT_CONFIG_ACTIONS)
        pattern = "|".join(ids) or "^$"
        rows = self.reads.sql(
            "SELECT cast(event_time AS string) AS t, service_name AS service, action_name AS action, "
            "user_identity.email AS actor, response.status_code AS rc, to_json(request_params) AS params "
            "FROM system.access.audit "
            f"WHERE event_time >= TIMESTAMP'{since.strftime('%Y-%m-%d %H:%M:%S')}' "
            f"AND service_name IN ('jobs', 'deltaPipelines') AND action_name IN ({actions}) "
            f"AND to_json(request_params) RLIKE '{pattern}' ORDER BY event_time DESC LIMIT {config.HARD_LIMIT}")
        changes = []
        for r in rows:
            resource = next((label for rid, label in ids.items() if rid in (r["params"] or "")), None)
            changes.append({"time": r["t"], "service": r["service"], "action": r["action"], "resource": resource,
                            "actor": logic.actor_role(r["actor"], roles), "status_code": r["rc"]})
        return changes


for _name in DQ_TOOLS:   # the same status / redaction wrapper as the monitor's tools
    setattr(Tools, _name, tool_result(getattr(DqTools, _name)))
for _name in KNOWLEDGE_TOOLS:
    setattr(Tools, _name, tool_result(getattr(KnowledgeTools, _name)))


INSTRUCTIONS = (
    "Read-only diagnostics for the NetSuite ingestion platform (dev only in phase 3). Start with "
    "get_recent_job_runs (did the daily generator and ingestion run? missed_schedules), then get_table_health and "
    "get_pipeline_errors, then compare_bronze_to_source for data completeness, and get_recent_deploys for changes. "
    "No tool changes anything; a disabled endpoint is reported as status 'unavailable', never enabled."
)
READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)


def build_server(tools: Tools) -> MCPServer:
    server = MCPServer("aidq-netsuite", instructions=INSTRUCTIONS)

    @server.tool(annotations=READ_ONLY)
    def get_table_health(env: str = "dev", table: str | None = None) -> dict:
        """Latest status per source table and layer (bronze/silver/reject/ledger) from aidq_metadata.v_table_health:
        status, error, rows read/written/rejected, reject rate vs threshold, active rules, open proposals; plus the
        latest bronze guard row. `table` limits it to one source table, e.g. netsuite_transactions."""
        return tools.get_table_health(env, table)

    @server.tool(annotations=READ_ONLY)
    def get_recent_job_runs(env: str = "dev", days: int | None = None, job: str | None = None) -> dict:
        """Runs of the daily jobs in the last `days` (default 3, max 14): start/end, trigger, result, state message;
        each job's schedule (cron, pause status) and missed_schedules (fire times with no run started within 15
        minutes). `job`: generator, ingestion or canary (default all)."""
        return tools.get_recent_job_runs(env, days, job)

    @server.tool(annotations=READ_ONLY)
    def get_pipeline_errors(env: str = "dev", hours: int | None = None) -> dict:
        """ERROR and WARN events from the ingestion and canary pipelines' event logs in the last `hours` (default
        24, max 168), and run_audit rows with status FAILED in the same window. For the ingestion pipeline,
        `failed_updates`: each FAILED update with who started it (cause JOB_TASK = through the job, whose first task
        refreshes the stored database credentials) and the credential's age then (they expire after ~60 min)."""
        return tools.get_pipeline_errors(env, hours)

    @server.tool(annotations=READ_ONLY)
    def compare_bronze_to_source(env: str = "dev", table: str | None = None, since: str | None = None) -> dict:
        """Row counts of dev bronze against the source per table and snapshot date (updated_date day). Lists every
        date that differs (verdict identical / gaps), plus, for information, the business keys that have several
        versions on one day in the source. `since` (YYYY-MM-DD) limits the dates compared."""
        return tools.compare_bronze_to_source(env, table, since)

    @server.tool(annotations=READ_ONLY)
    def get_recent_deploys(env: str = "dev", days: int | None = None) -> dict:
        """Deployments in the last `days` (default 7, max 30): GitHub Actions deploy workflow runs, and config
        changes (create/update/reset/delete/permission changes) on this env's jobs and pipelines from the audit
        log, with the actor as a role (owner, ci-dev, data-generator, system, other)."""
        return tools.get_recent_deploys(env, days)

    @server.tool(annotations=READ_ONLY)
    def get_dq_rules(env: str = "dev", table: str | None = None) -> dict:
        """Data-quality rules per table (active and inactive): rule_id, name, SQL expression, severity HARD/SOFT,
        who created it and the proposal it came from."""
        return tools.get_dq_rules(env, table)

    @server.tool(annotations=READ_ONLY)
    def get_open_and_rejected_proposals(env: str = "dev", table: str | None = None, days: int | None = None) -> dict:
        """config_proposals that are open (PENDING / VALIDATED / VALIDATION_FAILED) and those REJECTED in the last
        `days` (default 30, max 90), with the reviewer's reason. Use it to avoid proposing again."""
        return tools.get_open_and_rejected_proposals(env, table, days)

    @server.tool(annotations=READ_ONLY)
    def profile_table(env: str = "dev", table: str | None = None) -> dict:
        """Profile of one table on BRONZE (aggregates only, no raw rows): per active column type, null rate,
        distinct count, min/max, top values for low-cardinality columns, future-date and negative counts; candidate
        checks (end before start, amount vs quantity x rate, NULL business key); duplicates (same key and day,
        different created_date) vs normal same-day versions; references to keys missing in another table."""
        return tools.profile_table(env, table)

    @server.tool(annotations=READ_ONLY)
    def get_rejects_summary(env: str = "dev", table: str | None = None, runs: int | None = None) -> dict:
        """Rows currently in poc_reject.rejected_rows per table and failed HARD rule, and the reject counts of the
        last `runs` runs (default 7, max 30) from run_audit."""
        return tools.get_rejects_summary(env, table, runs)

    @server.tool(annotations=READ_ONLY)
    def get_impact(env: str = "dev", object: str | None = None) -> dict:  # noqa: A002
        """Everything downstream of a table, column, gold table/column or rule:<id>, from the lineage graph (bronze,
        silver, gold columns derived from it, gold tables that filter or join on it, DQ rules that validate it), each
        with its path. Deterministic: use it for "what breaks if I change X"."""
        return tools.get_impact(env, object)

    @server.tool(annotations=READ_ONLY)
    def get_upstream(env: str = "dev", object: str | None = None) -> dict:  # noqa: A002
        """Everything upstream of an object in the lineage graph (for a gold column: the source columns it is derived
        from and the columns its table filters and joins on), each with its path. For root-cause questions."""
        return tools.get_upstream(env, object)

    @server.tool(annotations=READ_ONLY)
    def explain_history(env: str = "dev", object: str | None = None) -> dict:  # noqa: A002
        """Proposals (with rationale and the reviewer's comment) and incidents for a table, table.column or
        rule:<id>, oldest first: "why was this rule added?", "what happened to this table?"."""
        return tools.explain_history(env, object)

    @server.tool(annotations=READ_ONLY)
    def search_metadata(env: str = "dev", question: str | None = None, k: int | None = None) -> dict:
        """Semantic search over the platform's metadata, rules, proposals, incidents, changes and repo docs (top k,
        default 5, max 10). Each hit has its document id and source: cite them. No hits means nothing relevant was
        found: say so, do not answer from memory."""
        return tools.search_metadata(env, question, k)

    return server
