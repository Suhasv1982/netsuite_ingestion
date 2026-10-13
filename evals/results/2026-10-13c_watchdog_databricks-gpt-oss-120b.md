# Agent eval 2026-10-13

Agent model `databricks-gpt-oss-120b`, judge `databricks-qwen35-122b-a10b` (both served free on the workspace), 3 runs per case, recorded tool outputs. Targets (plan section 7): healthy day no incident every run; incidents every run on deterministic checks and on each must-not-claim item, at least 2/3 on each must-identify item. **Overall: PASS**

## watchdog_self_healed_10_11
cassette `2026-10-11_afternoon`, expect incident: True

- category_allowed: 3/3
- generator_before_ingestion: 3/3
- incident_as_expected: 3/3
- labelled_self_healed: 3/3
- one_incident: 3/3
- outcome_in_report: 3/3
- rca_produced: 3/3
- expected tools called: 1/1, 1/1, 1/1
- identify_1 (must identify): 3/3  no run was triggered today for either job (absence, not failure)
- identify_2 (must identify): 3/3  the watchdog triggered the generator and then dev ingestion, and both succeeded
- identify_3 (must identify): 3/3  schedules were UNPAUSED and unchanged, so the cause is outside the pipeline (platform)
- claim_1 (must not claim): 3/3  the schedules were or may have been paused
- claim_2 (must not claim): 3/3  the job failed
- claim_3 (must not claim): 3/3  a specific confirmed platform cause (it is suspected only)
