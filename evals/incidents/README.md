# Incident fixtures

Real incidents, written up after the root cause was verified, as ground truth for evaluating the Phase 4
monitor / RCA agent. One YAML file per incident, named `<first date>_<slug>.yaml`.

| Key | Meaning |
|---|---|
| `symptoms` | what a monitor sees first, before any diagnosis |
| `evidence` | the queries that led to the cause, each with its finding |
| `root_cause` | the verified cause (or the owner's ruling, when it cannot be verified) |
| `not_the_cause` | plausible explanations that the evidence rules out |
| `fix` | what was changed (or decided), and how it was verified |
| `grading` | `must_identify`, `must_not_claim` and `tools_expected` for scoring an agent's report |

Rules: no hosts, URLs, emails, run ids or raw API responses (see `baseline/README.md`); placeholders such as
`<id>` stand in for environment identifiers. `tests/test_incident_fixtures.py` checks every file's shape.
