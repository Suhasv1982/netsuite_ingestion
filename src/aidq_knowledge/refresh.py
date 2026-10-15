"""Rebuild the knowledge store (docs/phase7_rag_lineage_plan.md steps 1-3, 5): graph + corpus -> Delta, then the
vector index. Runs in the `knowledge_refresh` job as ci-dev (USE SCHEMA, CREATE TABLE, SELECT, MODIFY on
workspace.knowledge; CAN_USE on vs_endpoint_1; owner-applied, #71).

* workspace.knowledge.nodes / edges: the lineage graph (graph.py), overwritten each run (small).
* workspace.knowledge.docs: the corpus (corpus.py), overwritten each run, Change Data Feed on (the index syncs from it).
* workspace.knowledge.docs_index: a Delta-sync index on vs_endpoint_1 with databricks-gte-large-en embeddings over
  `text`, created if missing, then synced (TRIGGERED). No demo index on the endpoint is touched.
"""

from __future__ import annotations

import pathlib
import sys
import time

CATALOG, SCHEMA = "workspace", "knowledge"
ENDPOINT = "vs_endpoint_1"


def embedding_model() -> str:
    """The embedding endpoint, named only in src/aidq_llm/routing.yml (no agent code names a model)."""
    from aidq_llm.router import load_routes

    return load_routes()["embeddings"]["knowledge_docs"]
INDEX = f"{CATALOG}.{SCHEMA}.docs_index"
DOCS_TABLE = f"{CATALOG}.{SCHEMA}.docs"

DEFS_SQL = ("SELECT source_table, business_key, watermark_col, load_mode, (watermark_col IS NOT NULL AND "
            "btrim(business_key) <> '') AS has_silver FROM aidq_metadata.source_table_def")
COLS_SQL = ("SELECT d.source_table, c.column_name, c.data_type, COALESCE(c.is_active, true) AS is_active FROM "
            "aidq_metadata.source_columns c JOIN aidq_metadata.source_table_def d USING (table_id) ORDER BY c.ordinal")
RULES_SQL = ("SELECT r.rule_id, d.source_table, r.rule_name, r.rule_expr, r.severity, r.is_active, r.proposal_id FROM "
             "aidq_metadata.data_quality_rules r JOIN aidq_metadata.source_table_def d USING (table_id)")
PROPOSALS_SQL = ("SELECT p.proposal_id, p.proposal_type, d.source_table, p.status, p.proposed_change, p.rationale, "
                 "p.review_comment, p.proposed_by, p.created_at FROM aidq_metadata.config_proposals p "
                 "LEFT JOIN aidq_metadata.source_table_def d USING (table_id)")
INCIDENTS_SQL = ("SELECT i.incident_id, d.source_table, i.category, i.status, i.summary, i.suggested_fix, i.detected_at "
                 "FROM aidq_metadata.incidents i LEFT JOIN aidq_metadata.source_table_def d USING (table_id)")
CHANGES_SQL = ("SELECT change_id, table_name, operation, old_row, new_row, proposal_id, changed_at FROM "
               "aidq_metadata.config_change_log ORDER BY change_id")


def gold_queries() -> dict:
    import gold_sql as gs

    return {gs.CUSTOMER_REVENUE: (gs.customer_revenue_sql(*gs.REVENUE_INPUTS), gs.REVENUE_INPUTS),
            gs.CUSTOMER_STATUS: (gs.customer_status_sql(*gs.STATUS_INPUTS), gs.STATUS_INPUTS)}


def repo_docs(root: pathlib.Path) -> dict[str, str]:
    paths = [root / "README.md", *sorted((root / "docs").glob("*.md"))]
    return {p.relative_to(root).as_posix(): p.read_text(encoding="utf-8") for p in paths if p.exists()}


def load(pg_query, metadata_endpoint: str) -> dict:
    q = lambda sql: pg_query(metadata_endpoint, sql)  # noqa: E731
    return {"table_defs": q(DEFS_SQL), "columns": q(COLS_SQL), "rules": q(RULES_SQL), "proposals": q(PROPOSALS_SQL),
            "incidents": q(INCIDENTS_SQL), "changes": q(CHANGES_SQL)}


def build_all(inputs: dict, root: pathlib.Path) -> tuple[list[dict], list[dict], list[dict]]:
    from . import corpus, graph

    g = graph.build(inputs["table_defs"], inputs["columns"], inputs["rules"], inputs["proposals"], inputs["incidents"],
                    gold_queries())
    nodes, edges = g.rows()
    docs = corpus.build(inputs["table_defs"], inputs["columns"], inputs["rules"], inputs["proposals"],
                        inputs["incidents"], inputs["changes"], repo_docs(root))
    return nodes, edges, docs


def write(spark, nodes: list[dict], edges: list[dict], docs: list[dict], log=print) -> None:
    from pyspark.sql.types import StringType, StructField, StructType

    def table(name, rows, cols):
        schema = StructType([StructField(c, StringType(), True) for c in cols])
        df = spark.createDataFrame([[None if r.get(c) is None else str(r.get(c)) for c in cols] for r in rows], schema)
        (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
        log(f"wrote {CATALOG}.{SCHEMA}.{name}: {len(rows)} rows")

    table("nodes", nodes, ["id", "type", "name", "attrs"])
    table("edges", edges, ["src", "dst", "type"])
    table("docs", docs, ["id", "type", "object", "text", "source", "updated_at"])
    spark.sql(f"ALTER TABLE {DOCS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


def ensure_index(w, log=print, wait_s: int = 900) -> str:
    """Create the Delta-sync index if it does not exist, else trigger a sync. Returns its state."""
    from databricks.sdk.service.vectorsearch import (DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn,
                                                     PipelineType, VectorIndexType)

    try:
        w.vector_search_indexes.get_index(INDEX)
        w.vector_search_indexes.sync_index(INDEX)
        log(f"synced {INDEX}")
    except Exception as exc:  # noqa: BLE001 -- not found: create it
        if "not found" not in str(exc).lower() and "does not exist" not in str(exc).lower():
            raise
        w.vector_search_indexes.create_index(
            name=INDEX, endpoint_name=ENDPOINT, primary_key="id", index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=DOCS_TABLE, pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[EmbeddingSourceColumn(name="text", embedding_model_endpoint_name=embedding_model())],
                columns_to_sync=["id", "type", "object", "text", "source", "updated_at"]))
        log(f"created {INDEX} on {ENDPOINT} ({embedding_model()})")
    deadline, state = time.monotonic() + wait_s, "UNKNOWN"
    while time.monotonic() < deadline:
        st = w.vector_search_indexes.get_index(INDEX).status
        state = "READY" if getattr(st, "ready", False) else str(getattr(st, "detailed_state", "PENDING"))
        if getattr(st, "ready", False):
            break
        time.sleep(20)
    log(f"{INDEX}: {state}")
    return state


def main(root: pathlib.Path) -> int:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    from aidq_mcp.config import env_config
    from aidq_mcp.sdk_reads import SdkReads

    sys.path.insert(0, str(root / "src" / "netsuite_ingestion"))
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    inputs = load(SdkReads().pg_query, env_config("dev").metadata_endpoint)
    nodes, edges, docs = build_all(inputs, root)
    write(SparkSession.builder.getOrCreate(), nodes, edges, docs)
    state = ensure_index(WorkspaceClient())
    return 0 if state == "READY" else 1
