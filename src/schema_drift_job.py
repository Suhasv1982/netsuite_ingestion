"""spark_python_task entry of the schema_drift job (databricks.yml, dev target; runs as dq-agent). The work is in
aidq_drift/__main__.py (execute). A serverless python task runs this file without `__file__`, with its directory as
the working directory and on the import path; it only exits on a failure (even sys.exit(0) fails a kernel task)."""

import argparse
import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = os.getcwd()
if _here not in sys.path:
    sys.path.insert(0, _here)

from aidq_drift.__main__ import execute  # noqa: E402

_ap = argparse.ArgumentParser(prog="schema_drift_job")
_ap.add_argument("--write", default="true")
_ap.add_argument("--trace-experiment", default="")
_args = _ap.parse_args()
_code = execute(sdk=True, profile=None, write=str(_args.write).strip().lower() == "true", source_endpoint=None,
                trace_experiment=_args.trace_experiment, model=None)
if _code:
    raise SystemExit(_code)
