"""spark_python_task entry of the knowledge_refresh job (databricks.yml, dev target; runs as ci-dev). The work is in
aidq_knowledge/refresh.py. A serverless python task runs this file without `__file__`, with its directory (src/) as
the working directory; it only exits on a failure (even sys.exit(0) fails a kernel task)."""

import os
import pathlib
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = os.getcwd()
if _here not in sys.path:
    sys.path.insert(0, _here)

from aidq_knowledge.refresh import main  # noqa: E402

_code = main(pathlib.Path(_here).parent)
if _code:
    raise SystemExit(_code)
