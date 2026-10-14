"""The DQ rule recommender (docs/dq_recommender_design.md v2; plan docs/phase5_dq_recommender_plan.md).

The LLM proposes, deterministic code validates, a human approves, the database applies. This package holds the
agent's write toolbox (write_tools.py) and, in later steps, the agent itself. It runs as the dq-agent service
principal (Postgres role aidq_agent: SELECT, and INSERT on config_proposals and incidents only).
"""
