"""I/O behind the tools: Databricks CLI, Lakebase Postgres, SQL warehouse statements, GitHub (gh). Read-only.

Every Postgres session is read-only (`default_transaction_read_only`), every warehouse statement is a fixed SELECT
template from server.py, and only get/list calls are made. Nothing here enables an endpoint, starts or stops
anything, or deploys.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import subprocess
import time
from functools import lru_cache

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SUBPROCESS_TIMEOUT = 120


class Unavailable(RuntimeError):
    """A dependency is down or switched off (e.g. a disabled Lakebase endpoint): reported as status unavailable."""


class Reads:
    def __init__(self, profile: str | None):
        """`profile` None: the CLI authenticates from DATABRICKS_* variables (CI)."""
        self.profile = profile

    # -- Databricks CLI ---------------------------------------------------------

    def cli(self, *args: str, body: dict | None = None):
        cmd = ["databricks", *args, "-o", "json"] + (["--profile", self.profile] if self.profile else [])
        if body is not None:
            cmd += ["--json", json.dumps(body)]
        env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT, env=env)
        if p.returncode:
            raise RuntimeError(f"databricks {' '.join(args[:2])}: {p.stderr.strip()[:300]}")
        return json.loads(p.stdout) if p.stdout.strip() else {}

    @lru_cache(maxsize=None)
    def current_user(self) -> str:
        return self.cli("current-user", "me")["userName"]

    @lru_cache(maxsize=None)
    def service_principals(self) -> dict[str, str]:
        """application id -> display name."""
        return {p["applicationId"]: p.get("displayName", "") for p in self.cli("service-principals", "list")
                if p.get("applicationId")}

    @lru_cache(maxsize=None)
    def job_id(self, name: str) -> int | None:
        return next((j["job_id"] for j in self.cli("jobs", "list") if j["settings"]["name"] == name), None)

    def job_settings(self, job_id: int) -> dict:
        return self.cli("jobs", "get", str(job_id)).get("settings", {})

    def job_runs(self, job_id: int, since: dt.datetime) -> list[dict]:
        return self.cli("jobs", "list-runs", "--job-id", str(job_id), "--start-time-from",
                        str(int(since.timestamp() * 1000)), "--limit", "100") or []

    @lru_cache(maxsize=None)
    def pipeline_id(self, name: str) -> str | None:
        return next((p["pipeline_id"] for p in self.cli("pipelines", "list-pipelines") if p["name"] == name), None)

    def pipeline_events(self, pipeline_id: str, limit: int) -> list[dict]:
        return self.cli("pipelines", "list-pipeline-events", pipeline_id, "--filter", "level in ('ERROR','WARN')",
                        "--limit", str(limit)) or []

    # -- Lakebase Postgres --------------------------------------------------------

    def pg_query(self, endpoint: str, statement: str, params: tuple = ()) -> list[dict]:
        import psycopg

        ep = self.cli("postgres", "get-endpoint", endpoint)
        if ep.get("status", {}).get("disabled"):
            raise Unavailable(f"Lakebase endpoint {endpoint.split('/')[1]}/{endpoint.split('/')[3]} is disabled "
                              "(this server never enables it)")
        token = self.cli("postgres", "generate-database-credential", endpoint)["token"]
        with psycopg.connect(host=ep["status"]["hosts"]["host"], dbname="databricks_postgres",
                             user=self.current_user(), password=token, sslmode="require", connect_timeout=60,
                             options="-c default_transaction_read_only=on") as conn:
            cur = conn.execute(statement, params)
            cols = [c.name for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            conn.rollback()
        return rows

    # -- SQL warehouse -------------------------------------------------------------

    @lru_cache(maxsize=None)
    def warehouse_id(self) -> str:
        whs = self.cli("warehouses", "list")
        if not whs:
            raise Unavailable("no SQL warehouse")
        return whs[0]["id"]

    def sql(self, statement: str) -> list[dict]:
        body = {"warehouse_id": self.warehouse_id(), "statement": statement, "wait_timeout": "50s",
                "disposition": "INLINE", "format": "JSON_ARRAY"}
        r = self.cli("api", "post", "/api/2.0/sql/statements", body=body)
        deadline = time.monotonic() + 300
        while r.get("status", {}).get("state") in ("PENDING", "RUNNING") and time.monotonic() < deadline:
            time.sleep(5)
            r = self.cli("api", "get", f"/api/2.0/sql/statements/{r['statement_id']}")
        state = r.get("status", {}).get("state")
        if state != "SUCCEEDED":
            raise Unavailable(f"SQL warehouse statement {state}: {json.dumps(r.get('status', {}).get('error'))[:300]}")
        cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
        return [dict(zip(cols, row)) for row in (r.get("result", {}).get("data_array") or [])]

    # -- GitHub ----------------------------------------------------------------------

    def workflow_runs(self, workflow: str, since: dt.date) -> list[dict]:
        cmd = ["gh", "api", f"repos/{{owner}}/{{repo}}/actions/workflows/{workflow}/runs",
               "-X", "GET", "-f", "per_page=100", "-f", f"created=>={since.isoformat()}"]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT, cwd=REPO_ROOT)
        if p.returncode:
            raise Unavailable(f"gh api: {p.stderr.strip()[:300]}")
        return json.loads(p.stdout).get("workflow_runs", [])
