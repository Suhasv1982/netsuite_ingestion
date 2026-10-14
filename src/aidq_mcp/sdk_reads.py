"""Reads through the Databricks SDK instead of the CLI, for code that runs inside a Databricks job (serverless has no
`databricks` CLI). The identity is the job's run-as principal (WorkspaceClient() picks it up).

Only the calls the DQ tools and the validator make are mapped (Lakebase endpoint and credential, current user,
warehouses, SQL statements); anything else raises Unavailable, so a tool that needs the CLI says so instead of
failing obscurely. Still read-only: the mapped calls are the same get/list/statement calls Reads makes.
"""

from __future__ import annotations

from functools import lru_cache

from .reads import Reads, Unavailable


class SdkReads(Reads):
    def __init__(self, client=None):
        super().__init__(profile=None)
        self._client = client

    @property
    def w(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient

            self._client = WorkspaceClient()
        return self._client

    def cli(self, *args: str, body: dict | None = None):
        cmd = args[:2]
        if cmd == ("postgres", "get-endpoint"):
            return self.w.postgres.get_endpoint(name=args[2]).as_dict()
        if cmd == ("postgres", "generate-database-credential"):
            return {"token": self.w.postgres.generate_database_credential(endpoint=args[2]).token}
        if cmd == ("current-user", "me"):
            return self.w.current_user.me().as_dict()
        if cmd == ("warehouses", "list"):
            return [x.as_dict() for x in self.w.warehouses.list()]
        if cmd == ("api", "post"):
            return self.w.api_client.do("POST", args[2], body=body)
        if cmd == ("api", "get"):
            return self.w.api_client.do("GET", args[2])
        raise Unavailable(f"databricks {' '.join(args[:2])} is not available in a job (SdkReads maps only the "
                          "DQ tools' and the validator's calls)")

    @lru_cache(maxsize=None)
    def current_user(self) -> str:
        return self.cli("current-user", "me")["userName"]
