"""spark_python_task entry of the dq_recommender job (resources in databricks.yml, dev target). A task file runs as a
script, so the package directory is put on the path first; the work is in aidq_dq_agent/job.py."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aidq_dq_agent.job import main  # noqa: E402

sys.exit(main())
