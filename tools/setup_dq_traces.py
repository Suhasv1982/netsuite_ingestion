"""One-time setup of the DQ recommender's trace store in DEV (plan #39 step 8). Run by the OWNER (via
tools/setup_dq_roles.sh, step 6); idempotent.

    python tools/setup_dq_traces.py --profile DEFAULT [--check]

1. the MLflow experiment /Shared/aidq/dq_recommender, bound to the Unity Catalog trace location
   workspace.aidq_traces (owner choice 2026-10-14). MLflow provisions its trace tables there; a binding is
   permanent, so an experiment already bound elsewhere is reported, never re-bound. If Unity Catalog trace storage
   cannot be set up (e.g. not available on Free Edition), the experiment uses standard MLflow experiment storage
   instead: owner decision 2026-10-14, no question asked; the output says which storage is used and why
   (`STORAGE:` line), for STATUS.md;
2. dq-agent CAN_EDIT on the experiment (it writes the traces), added to the experiment's permissions (nothing is
   removed).
The schema itself and dq-agent's USE SCHEMA / SELECT / MODIFY on it come from setup_dq_roles.sh step 4
(grants/dev.yml, owner-applied). --check only reads and fails on anything missing.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from aidq_dq_agent.tracing import EXPERIMENT  # noqa: E402

CATALOG, SCHEMA = "workspace", "aidq_traces"


def _cli(profile, *args, body=None):
    cmd = ["databricks", *args, "-o", "json", "--profile", profile]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "MSYS_NO_PATHCONV": "1"})
    if p.returncode:
        raise RuntimeError(f"databricks {' '.join(args[:2])}: {p.stderr.strip()[:300]}")
    return json.loads(p.stdout) if p.stdout.strip() else {}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", required=True)
    ap.add_argument("--check", action="store_true", help="read only: fail on anything missing")
    args = ap.parse_args(argv)
    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

    import mlflow
    from mlflow.entities.trace_location import UnityCatalog

    agent = next((s["applicationId"] for s in _cli(args.profile, "service-principals", "list")
                  if s.get("displayName") == "dq-agent"), None)
    if not agent:
        print("FAIL service principal dq-agent not found (setup_dq_roles.sh step 1)")
        return 1
    warehouses = _cli(args.profile, "warehouses", "list")
    if not warehouses:
        print("FAIL no SQL warehouse (needed to provision the trace tables)")
        return 1
    os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouses[0]["id"]
    mlflow.set_tracking_uri(f"databricks://{args.profile}")

    failed, uc_error = False, None
    exp = mlflow.get_experiment_by_name(EXPERIMENT)
    if exp is None and not args.check:
        _cli(args.profile, "workspace", "mkdirs", EXPERIMENT.rsplit("/", 1)[0])
        try:
            exp = mlflow.set_experiment(experiment_name=EXPERIMENT,
                                        trace_location=UnityCatalog(catalog_name=CATALOG, schema_name=SCHEMA))
            print(f"created experiment {EXPERIMENT}, bound to {CATALOG}.{SCHEMA}")
        except Exception as exc:  # noqa: BLE001 -- owner decision: fall back to standard experiment storage
            uc_error = f"{type(exc).__name__}: {str(exc)[:300]}"
            if mlflow.get_experiment_by_name(EXPERIMENT) is None:
                mlflow.create_experiment(EXPERIMENT)
            print(f"created experiment {EXPERIMENT} with standard experiment storage (Unity Catalog trace storage "
                  f"failed: {uc_error})")
    elif exp is not None and getattr(exp, "trace_location", None) is None and not args.check:
        try:
            exp = mlflow.set_experiment(experiment_name=EXPERIMENT,
                                        trace_location=UnityCatalog(catalog_name=CATALOG, schema_name=SCHEMA))
            print(f"bound experiment {EXPERIMENT} to {CATALOG}.{SCHEMA}")
        except Exception as exc:  # noqa: BLE001
            uc_error = f"{type(exc).__name__}: {str(exc)[:300]}"
            print(f"kept standard experiment storage for {EXPERIMENT} (Unity Catalog trace storage failed: {uc_error})")
    exp = mlflow.get_experiment_by_name(EXPERIMENT)
    loc = getattr(exp, "trace_location", None) if exp else None
    if exp is None:
        print(f"FAIL experiment {EXPERIMENT} does not exist")
        failed = True
    elif isinstance(loc, UnityCatalog) and loc.catalog_name == CATALOG and loc.schema_name == SCHEMA:
        print(f"OK experiment {EXPERIMENT} ({exp.experiment_id}) -> {CATALOG}.{SCHEMA}")
        print(f"STORAGE: unity-catalog {CATALOG}.{SCHEMA}")
    elif loc is None:
        print(f"OK experiment {EXPERIMENT} ({exp.experiment_id}) with standard experiment storage")
        print("STORAGE: experiment (standard MLflow storage)"
              + (f"; Unity Catalog trace storage failed: {uc_error}" if uc_error else "; not bound to Unity Catalog"))
    else:
        print(f"FAIL experiment {EXPERIMENT} trace location is {loc!r}, expected {CATALOG}.{SCHEMA} (a binding is "
              "permanent: not changed)")
        failed = True

    if exp is not None:
        acl = _cli(args.profile, "experiments", "get-permissions", exp.experiment_id).get("access_control_list") or []
        has = any(a.get("service_principal_name") == agent and any(p.get("permission_level") in ("CAN_EDIT", "CAN_MANAGE")
                                                                    for p in a.get("all_permissions") or [])
                  for a in acl)
        if not has and not args.check:
            _cli(args.profile, "experiments", "update-permissions", exp.experiment_id,
                 body={"access_control_list": [{"service_principal_name": agent, "permission_level": "CAN_EDIT"}]})
            print("granted dq-agent CAN_EDIT on the experiment")
            has = True
        print(("OK" if has else "FAIL") + " dq-agent CAN_EDIT on the experiment")
        failed |= not has
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
