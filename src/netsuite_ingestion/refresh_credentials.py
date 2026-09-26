"""Job task: refresh the Lakebase OAuth database credentials used by the
netsuite_ingestion_poc pipeline.

The pipeline reads Lakebase Postgres over plain JDBC (see metadata.py) using
a short-lived (~1hr) OAuth database credential stored in the
netsuite_ingestion_poc secret scope. Since the pipeline runs on a daily
schedule, this task runs immediately before it (see the job's task
dependency) to mint a fresh credential for each Lakebase endpoint and store
it, so the pipeline always sees a token generated moments ago rather than
one that may be hours or days stale.

Run as a Databricks Jobs spark_python_task -- WorkspaceClient() picks up the
job's run-as identity automatically, no profile/token needed.
"""

import argparse

from databricks.sdk import WorkspaceClient


def refresh(w: WorkspaceClient, endpoint: str, scope: str, key: str) -> None:
    credential = w.postgres.generate_database_credential(endpoint=endpoint)
    w.secrets.put_secret(scope=scope, key=key, string_value=credential.token)
    print(f"Refreshed secret {scope}/{key} from endpoint {endpoint} (expires {credential.expire_time})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--source-endpoint", required=True)
    parser.add_argument("--source-key", required=True)
    parser.add_argument("--meta-endpoint", required=True)
    parser.add_argument("--meta-key", required=True)
    args = parser.parse_args()

    w = WorkspaceClient()
    refresh(w, args.source_endpoint, args.secret_scope, args.source_key)
    refresh(w, args.meta_endpoint, args.secret_scope, args.meta_key)


if __name__ == "__main__":
    main()
