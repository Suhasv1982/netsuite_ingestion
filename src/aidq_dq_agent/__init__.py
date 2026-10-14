"""The DQ rule recommender (docs/dq_recommender_design.md v2; plan docs/phase5_dq_recommender_plan.md).

The LLM proposes, deterministic code validates, a human approves, the database applies. This package holds the
agent (graph.py: collect -> recommend -> submit -> report; recommend.py: the model step; prompts/), its write
toolbox (write_tools.py) and the validator's checks (validator.py). The agent runs as the dq-agent service principal
(Postgres role aidq_agent: SELECT, and INSERT on config_proposals and incidents only).
"""

VERSION = "0.1"     # proposed_by = dq_agent@<VERSION>
