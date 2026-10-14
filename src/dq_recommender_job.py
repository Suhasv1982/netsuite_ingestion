"""spark_python_task entry of the dq_recommender job (resources in databricks.yml, dev target). The work is in
aidq_dq_agent/job.py.

A serverless python task runs this file without `__file__` (NameError on the first run, 2026-10-14), with the
file's directory as the working directory and on the import path; the fallback keeps it importable either way."""

import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = os.getcwd()
if _here not in sys.path:
    sys.path.insert(0, _here)

from aidq_dq_agent.job import main  # noqa: E402

# Only a failure exits: in the task's IPython kernel even sys.exit(0) raises SystemExit and marks the run FAILED
# (dry run as dq-agent, 2026-10-14). Finishing normally is success.
_code = main()
if _code:
    raise SystemExit(_code)
