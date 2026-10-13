"""Re-enable a disabled Lakebase endpoint before use, and say so (owner decision 2026-10-13).

On Free Edition the Lakebase endpoints end up disabled after the workspace is idle (2026-09-27, 09-30, 10-05,
10-11). Every component that needs one wakes it first instead of failing: the generator (pg_writer), the dev
job's first task (refresh_credentials), the monitor (aidq_agent, before its read-only tools run) and
tools/daily_check.py. Each prints one line when it re-enabled an endpoint, so the run's log records it.

The identity needs CAN_MANAGE on the Lakebase project (docs/STATUS.md, section 0h). One module, three adapters:
`wake_sdk` (job tasks: the run identity, no CLI), `wake_cli` (a profile, or environment auth in CI).
"""

from __future__ import annotations

import json
import subprocess
import time
from typing import Callable

MESSAGE = "ENABLED Lakebase endpoint {name} (it was disabled; last active {last}): {who}"


def wake(name: str, get_status: Callable[[], dict], enable: Callable[[], None], who: str,
         log: Callable[[str], None] = print, timeout_s: float = 600, poll_s: float = 10) -> bool:
    """Enable `name` if it is disabled and wait until it is enabled. Returns True when it had to enable it.
    `get_status` returns the endpoint's status as a dict ({"disabled": bool, "last_active_time": ...})."""
    status = get_status()
    if not status.get("disabled"):
        return False
    enable()
    deadline = time.monotonic() + timeout_s
    while get_status().get("disabled"):
        if time.monotonic() > deadline:
            raise TimeoutError(f"{name} is still disabled {timeout_s:.0f}s after enabling it")
        time.sleep(poll_s)
    log(MESSAGE.format(name=name, last=status.get("last_active_time", "unknown"), who=who))
    return True


def _body() -> dict:
    return {"spec": {"disabled": False}}


def wake_sdk(w, name: str, who: str, log: Callable[[str], None] = print, **kw) -> bool:
    """For job tasks (spark_python_task): `w` is a databricks.sdk WorkspaceClient as the run identity. Uses the raw
    PATCH so it works with older SDKs on serverless environments."""
    def get_status() -> dict:
        status = w.postgres.get_endpoint(name=name).status
        return {"disabled": bool(status and status.disabled),
                "last_active_time": getattr(status, "last_active_time", None)}

    def enable() -> None:
        w.api_client.do("PATCH", f"/api/2.0/postgres/{name}", query={"update_mask": "spec.disabled"}, body=_body())

    return wake(name, get_status, enable, who, log, **kw)


def wake_cli(name: str, who: str, profile: str | None = None, log: Callable[[str], None] = print, **kw) -> bool:
    """For local runs and CI: the Databricks CLI with `profile`, or environment auth when profile is None."""
    def cli(*args: str, body: dict | None = None) -> dict:
        cmd = ["databricks", *args, "-o", "json"] + (["--profile", profile] if profile else [])
        if body is not None:
            cmd += ["--json", json.dumps(body)]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode:
            raise RuntimeError(f"databricks {' '.join(args[:2])}: {p.stderr.strip()[:300]}")
        return json.loads(p.stdout) if p.stdout.strip() else {}

    def get_status() -> dict:
        return cli("postgres", "get-endpoint", name).get("status", {})

    def enable() -> None:
        cli("postgres", "update-endpoint", name, "spec.disabled", body=_body())

    return wake(name, get_status, enable, who, log, **kw)
