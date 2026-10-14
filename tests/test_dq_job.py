"""Step 10: the dq_recommender / dq_validator jobs: SDK reads, the in-process toolbox, the job entry, and the bundle."""

import asyncio
import pathlib
import types

import pytest
import yaml

from aidq_agent.toolbox import LocalToolBox
from aidq_mcp.reads import Unavailable
from aidq_mcp.sdk_reads import SdkReads

ROOT = pathlib.Path(__file__).resolve().parents[1]


class D:
    def __init__(self, d):
        self.d = d

    def as_dict(self):
        return self.d


class FakeW:
    def __init__(self):
        self.calls = []
        self.postgres = types.SimpleNamespace(
            get_endpoint=lambda name: D({"status": {"hosts": {"host": "h"}, "disabled": False}}),
            generate_database_credential=lambda endpoint: types.SimpleNamespace(token="t"))
        self.current_user = types.SimpleNamespace(me=lambda: D({"userName": "dq-agent-app"}))
        self.warehouses = types.SimpleNamespace(list=lambda: [D({"id": "wh"})])
        self.api_client = types.SimpleNamespace(do=lambda method, path, body=None: self.calls.append((method, path, body))
                                                or {"status": {"state": "SUCCEEDED"}})


def test_sdk_reads_map_the_cli_calls_the_dq_tools_make():
    r = SdkReads(FakeW())
    assert r.cli("postgres", "get-endpoint", "projects/x/branches/dev/endpoints/primary")["status"]["hosts"]["host"] == "h"
    assert r.cli("postgres", "generate-database-credential", "e") == {"token": "t"}
    assert r.current_user() == "dq-agent-app" and r.warehouse_id() == "wh"
    r.cli("api", "post", "/api/2.0/sql/statements", body={"statement": "SELECT 1"})
    assert r.w.calls == [("POST", "/api/2.0/sql/statements", {"statement": "SELECT 1"})]
    with pytest.raises(Unavailable, match="not available in a job"):
        r.cli("jobs", "list")


class FakeTools:
    def get_dq_rules(self, env="dev", table=None):
        return {"status": "ok", "env": env, "table": table}

    def get_recent_job_runs(self, env="dev"):
        raise AssertionError("not exposed")


def test_local_toolbox_calls_in_process_and_only_the_allowed_tools(monkeypatch):
    tb = LocalToolBox(FakeTools(), ("get_dq_rules",))
    assert asyncio.run(tb.call("get_dq_rules", {"env": "dev", "table": "t"})) == {"status": "ok", "env": "dev", "table": "t"}
    assert "not available" in asyncio.run(tb.call("get_recent_job_runs", {}))["error"]
    assert "bad arguments" in asyncio.run(tb.call("get_dq_rules", {"nope": 1}))["error"]


def test_local_toolbox_lists_the_mcp_definitions_of_the_allowed_tools():
    from aidq_mcp.server import DQ_TOOLS, Tools

    defs = asyncio.run(LocalToolBox(Tools(reads=None), DQ_TOOLS).list_tools())
    assert sorted(d["name"] for d in defs) == sorted(DQ_TOOLS)
    assert all(d["input_schema"] and d["description"] for d in defs)


def test_job_entry_is_a_dry_run_unless_write_is_true(monkeypatch, capsys):
    from aidq_dq_agent import graph, job, write_tools

    seen = {}

    async def fake_run(toolbox, recommender, wt, tables, trace_id=None):
        seen.update(dry_run=wt.dry_run, tables=tables, toolbox=type(toolbox).__name__)
        return {"report": "# report", "exit_code": 0}

    monkeypatch.setattr(graph, "run", fake_run)
    monkeypatch.setattr(write_tools, "sdk_connect", lambda endpoint: (lambda: None))
    monkeypatch.setattr("aidq_mcp.sdk_reads.SdkReads.__init__", lambda self, client=None: None)
    assert job.main(["--write", "false", "--table", "netsuite_memberships"]) == 0
    assert seen == {"dry_run": True, "tables": ["netsuite_memberships"], "toolbox": "LocalToolBox"}
    assert job.main(["--write", "true"]) == 0 and seen["dry_run"] is False
    assert "DQ recommender job: WRITE" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        job.main(["--table", "nope"])


def test_bundle_dev_jobs_run_as_dq_agent_and_the_deployer():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    dev = bundle["targets"]["dev"]
    jobs = dev["resources"]["jobs"]
    rec, val = jobs["dq_recommender"], jobs["dq_validator"]
    assert rec["run_as"] == {"service_principal_name": "${var.dq_agent_sp}"}
    assert "run_as" not in val                       # the deploying identity (ci-dev), which SETs ROLE aidq_validator
    assert "schedule" not in rec and "schedule" not in val    # manual runs only for now
    assert rec["parameters"] == [{"name": "write", "default": "false"}]
    assert rec["tasks"][0]["spark_python_task"] == {"python_file": "src/dq_recommender_job.py",
                                                    "parameters": ["--write", "{{job.parameters.write}}"]}
    assert val["tasks"][0]["spark_python_task"]["parameters"] == ["--auth", "sdk"]
    for j in (rec, val):
        deps = j["environments"][0]["spec"]["dependencies"]
        assert not any("psycopg[binary]" in d or "psycopg-binary" in d for d in deps)
    assert {"service_principal_name": "${var.dq_agent_sp}", "level": "CAN_VIEW"} in dev["permissions"]
    prod_jobs = (bundle["targets"]["prod"].get("resources") or {}).get("jobs") or {}
    assert not {"dq_recommender", "dq_validator"} & set(prod_jobs)          # dev only


def test_deploy_dev_passes_the_dq_agent_variable_to_every_deploy():
    wf = (ROOT / ".github" / "workflows" / "deploy-dev.yml").read_text(encoding="utf-8")
    assert wf.count("BUNDLE_VAR_dq_agent_sp: ${{ vars.DQ_AGENT_SP }}") == wf.count("BUNDLE_VAR_data_generator_sp:") == 2


def test_validator_job_mode_uses_the_sdk(monkeypatch):
    import sys
    sys.path.insert(0, str(ROOT / "tools"))
    import validate_proposals as vp

    seen = {}

    class Conn:
        def commit(self):
            seen["commit"] = True

        def rollback(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr("aidq_dq_agent.write_tools.sdk_connect", lambda endpoint: (lambda: Conn()))
    monkeypatch.setattr("aidq_mcp.sdk_reads.SdkReads.__init__", lambda self, client=None: None)
    monkeypatch.setattr(vp, "validate_all", lambda conn, sql, catalog, set_role=True: seen.update(set_role=set_role) or [])
    assert vp.main(["--auth", "sdk"]) == 0
    assert seen == {"set_role": True, "commit": True}


def _exec_like_a_job(path: pathlib.Path, monkeypatch):
    """Run a task file the way a serverless python task does: exec'd, without __file__, cwd = its directory."""
    monkeypatch.chdir(path.parent)
    code = compile(path.read_text(encoding="utf-8"), str(path), "exec")
    exec(code, {"__name__": "__main__"})


def test_the_recommender_entry_runs_without___file__(monkeypatch):
    import aidq_dq_agent.job as job

    monkeypatch.setattr(job, "main", lambda argv=None: 0)
    with pytest.raises(SystemExit) as e:
        _exec_like_a_job(ROOT / "src" / "dq_recommender_job.py", monkeypatch)
    assert e.value.code == 0


def test_the_validator_module_loads_without___file__(monkeypatch):
    path = ROOT / "tools" / "validate_proposals.py"
    monkeypatch.chdir(path.parent)
    g = {"__name__": "validate_proposals_as_job"}           # not __main__: load it, do not run main()
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), g)
    assert g["ROOT"] == ROOT and callable(g["main"])


def test_the_job_runs_inside_a_kernel_like_environment(monkeypatch, capsys):
    """Serverless python tasks run in an IPython kernel: stdout without reconfigure(), and a running event loop."""
    from aidq_dq_agent import graph, job, write_tools

    class KernelStream:
        def __init__(self):
            self.parts = []

        def write(self, s):
            self.parts.append(s)

        def flush(self):
            pass

    async def fake_run(toolbox, recommender, wt, tables, trace_id=None):
        await asyncio.sleep(0)
        return {"report": "# report", "exit_code": 0}

    monkeypatch.setattr(graph, "run", fake_run)
    monkeypatch.setattr(write_tools, "sdk_connect", lambda endpoint: (lambda: None))
    monkeypatch.setattr("aidq_mcp.sdk_reads.SdkReads.__init__", lambda self, client=None: None)
    stream = KernelStream()
    monkeypatch.setattr("sys.stdout", stream)

    async def inside_a_running_loop():
        return job.main(["--write", "false"])

    assert asyncio.run(inside_a_running_loop()) == 0
    assert "# report" in "".join(stream.parts)
