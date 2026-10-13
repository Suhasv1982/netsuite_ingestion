"""lakebase_wake: re-enable a disabled Lakebase endpoint before use, and log it (owner decision 2026-10-13)."""

import types

import pytest

import lakebase_wake as lw

EP = "projects/netsuite-sample/branches/production/endpoints/primary"


class FakeEndpoint:
    def __init__(self, disabled, stays_disabled_polls=0):
        self.disabled, self.enabled_calls, self.left = disabled, 0, stays_disabled_polls

    def status(self):
        if not self.disabled:
            return {"disabled": False}
        if self.enabled_calls and self.left <= 0:
            self.disabled = False
            return {"disabled": False}
        if self.enabled_calls:
            self.left -= 1
        return {"disabled": True, "last_active_time": "2026-10-10T12:37:40Z"}

    def enable(self):
        self.enabled_calls += 1


def test_an_enabled_endpoint_is_left_alone_and_nothing_is_logged():
    ep, logs = FakeEndpoint(disabled=False), []
    assert lw.wake(EP, ep.status, ep.enable, "test", logs.append) is False
    assert ep.enabled_calls == 0 and logs == []


def test_a_disabled_endpoint_is_enabled_waited_for_and_logged():
    ep, logs = FakeEndpoint(disabled=True, stays_disabled_polls=2), []
    assert lw.wake(EP, ep.status, ep.enable, "netsuite_daily_generator", logs.append, poll_s=0) is True
    assert ep.enabled_calls == 1
    assert logs == [f"ENABLED Lakebase endpoint {EP} (it was disabled; last active 2026-10-10T12:37:40Z): "
                    "netsuite_daily_generator"]


def test_it_gives_up_after_the_timeout():
    ep = FakeEndpoint(disabled=True, stays_disabled_polls=10**6)
    with pytest.raises(TimeoutError, match="still disabled"):
        lw.wake(EP, ep.status, ep.enable, "t", lambda m: None, timeout_s=0, poll_s=0)


def test_sdk_adapter_patches_only_spec_disabled():
    calls = []
    state = {"disabled": True}

    def get_endpoint(name):
        return types.SimpleNamespace(status=types.SimpleNamespace(disabled=state["disabled"], last_active_time="x"))

    def do(method, path, query=None, body=None):
        calls.append((method, path, query, body))
        state["disabled"] = False

    w = types.SimpleNamespace(postgres=types.SimpleNamespace(get_endpoint=get_endpoint),
                              api_client=types.SimpleNamespace(do=do))
    assert lw.wake_sdk(w, EP, "job", log=lambda m: None, poll_s=0) is True
    assert calls == [("PATCH", f"/api/2.0/postgres/{EP}", {"update_mask": "spec.disabled"},
                      {"spec": {"disabled": False}})]


def test_refresh_credentials_wakes_only_when_asked(monkeypatch):
    import refresh_credentials as rc

    woke, refreshed = [], []
    monkeypatch.setattr(rc, "WorkspaceClient", lambda: object())
    monkeypatch.setattr(rc, "wake_sdk", lambda w, ep, who: woke.append(ep))
    monkeypatch.setattr(rc, "refresh", lambda w, ep, scope, key: refreshed.append(ep))
    base = ["x", "--secret-scope", "s", "--source-endpoint", "src", "--source-key", "k1",
            "--meta-endpoint", "meta", "--meta-key", "k2"]
    monkeypatch.setattr("sys.argv", base)
    rc.main()
    assert woke == [] and refreshed == ["src", "meta"]
    monkeypatch.setattr("sys.argv", base + ["--wake-endpoints", "true"])
    rc.main()
    assert woke == ["src", "meta"]


def test_monitor_wake_failure_does_not_stop_the_run(monkeypatch):
    import aidq_agent.__main__ as cli
    import netsuite_ingestion.lakebase_wake as nlw

    def boom(endpoint, who, profile=None, log=print):
        raise RuntimeError("PERMISSION_DENIED")

    monkeypatch.setattr(nlw, "wake_cli", boom)
    lines = cli.wake_endpoints(None)
    assert len(lines) == 2 and all("could not re-enable" in line and "PERMISSION_DENIED" in line for line in lines)


def test_only_the_dev_target_wakes_endpoints():
    yaml = pytest.importorskip("yaml")
    from pathlib import Path

    bundle = yaml.safe_load((Path(__file__).resolve().parent.parent / "databricks.yml").read_text(encoding="utf-8"))
    assert bundle["variables"]["wake_lakebase_endpoints"]["default"] == "false"
    assert bundle["targets"]["dev"]["variables"]["wake_lakebase_endpoints"] == "true"
    assert "wake_lakebase_endpoints" not in (bundle["targets"]["prod"].get("variables") or {})
