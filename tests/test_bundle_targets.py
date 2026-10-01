"""Static checks on the bundle config: dev and prod must never share per-target state.

The key ledger (<catalog>.ledger.bronze_keys / bronze_fingerprints) is written by
sync_ledger and read by bronze.py and ledger_check. Its location is derived from
${var.catalog}, which differs per target, so a dev run cannot touch prod's ledger.
These tests fail if a change pins the ledger (or the metadata credential key) to a
value that both targets would share. No workspace is needed: they only parse YAML.
"""

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parent.parent
LEDGER_TASKS = ("sync_ledger", "ledger_check")


def _load(rel: str) -> dict:
    return yaml.safe_load((ROOT / rel).read_text(encoding="utf-8"))


def _resolved_var(bundle: dict, target: str, name: str) -> str:
    override = (bundle["targets"][target].get("variables") or {}).get(name)
    return override if override is not None else bundle["variables"][name]["default"]


@pytest.fixture(scope="module")
def bundle() -> dict:
    return _load("databricks.yml")


@pytest.fixture(scope="module")
def job_tasks() -> dict:
    job = _load("resources/netsuite_ingestion_job.job.yml")["resources"]["jobs"]["netsuite_ingestion_daily"]
    return {t["task_key"]: t for t in job["tasks"]}


class TestPerTargetIsolation:
    @pytest.mark.parametrize("var", ["catalog", "secret_scope", "meta_pg_token_key", "meta_pg_endpoint", "meta_pg_host"])
    def test_dev_and_prod_resolve_different_values(self, bundle, var):
        assert _resolved_var(bundle, "dev", var) != _resolved_var(bundle, "prod", var)


class TestLedgerLocation:
    def test_pipeline_ledger_tables_follow_the_catalog_variable(self):
        conf = _load("resources/netsuite_ingestion_poc.pipeline.yml")["resources"]["pipelines"][
            "netsuite_ingestion_poc"
        ]["configuration"]
        assert conf["ledger_table"] == "${var.catalog}.ledger.bronze_keys"
        assert conf["ledger_fingerprint_table"] == "${var.catalog}.ledger.bronze_fingerprints"

    @pytest.mark.parametrize("task", LEDGER_TASKS)
    def test_ledger_tasks_take_the_catalog_variable(self, job_tasks, task):
        params = job_tasks[task]["spark_python_task"]["parameters"]
        assert params[params.index("--catalog") + 1] == "${var.catalog}"

    @pytest.mark.parametrize("task", LEDGER_TASKS + ("refresh_credentials", "log_run_audit"))
    def test_secret_scope_is_the_per_target_variable(self, job_tasks, task):
        params = job_tasks[task]["spark_python_task"]["parameters"]
        assert params[params.index("--secret-scope") + 1] == "${var.secret_scope}"

    def test_pipeline_reads_the_per_target_scope(self):
        conf = _load("resources/netsuite_ingestion_poc.pipeline.yml")["resources"]["pipelines"][
            "netsuite_ingestion_poc"
        ]["configuration"]
        assert conf["secret_scope"] == "${var.secret_scope}"

    def test_no_resource_file_names_a_scope_literally(self):
        for f in (ROOT / "resources").glob("*.yml"):
            assert "netsuite_ingestion_poc\"" not in f.read_text(encoding="utf-8"), f.name

    @pytest.mark.parametrize("task", LEDGER_TASKS + ("refresh_credentials", "log_run_audit"))
    def test_metadata_key_is_the_per_target_variable(self, job_tasks, task):
        params = job_tasks[task]["spark_python_task"]["parameters"]
        flag = "--meta-key" if task == "refresh_credentials" else "--meta-pg-key"
        assert params[params.index(flag) + 1] == "${var.meta_pg_token_key}"

    def test_job_code_derives_the_same_names_as_the_pipeline(self):
        from metadata import fingerprint_table_name, ledger_table_name

        assert ledger_table_name("c") == "c.ledger.bronze_keys"
        assert fingerprint_table_name("c") == "c.ledger.bronze_fingerprints"


class TestDeployIdentity:
    """Dev and prod are deployed by CI service principals, which are also their run identities."""

    @pytest.mark.parametrize("target", ["dev", "prod"])
    def test_runs_as_the_deploying_service_principal(self, bundle, target):
        assert bundle["targets"][target]["run_as"] == {"service_principal_name": "${workspace.current_user.userName}"}

    def test_prod_bundle_does_not_manage_acls(self, bundle):
        # bundle permissions would change the pipeline owner, which only a metastore admin may do (Free Edition)
        assert "permissions" not in bundle["targets"]["prod"]
        assert "permissions" not in bundle

    def test_prod_uses_its_own_scope(self, bundle):
        assert _resolved_var(bundle, "prod", "secret_scope") == "netsuite_ingestion_prod"
