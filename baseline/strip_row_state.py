"""Write the committable form of the baseline metric snapshots.

collect_metrics.py produces full snapshots that include `row_state`: the business key
(and dates) of every row in every silver table plus every rejected row, roughly 30,000
entries per table. That is a raw row dump, so the full files live in the git-ignored
`baseline/raw/` folder. This tool copies each one to its committed place with `row_state`
removed. Everything the report builders read (row counts, reject breakdown, run_audit,
pipeline and job state, columns, ledger stats) is kept, so the reports rebuild identically.

    python baseline/strip_row_state.py            # baseline/raw/**.json -> baseline/**.json

`classify_defects.py` needs `row_state`, so it must be pointed at the raw file:

    python baseline/classify_defects.py baseline/manifest_defects.json baseline/raw/defects_full.json out.json

The committed classification_*.json files are the results of that step.
"""

import json
from pathlib import Path

BASELINE = Path(__file__).resolve().parent
RAW = BASELINE / "raw"


def strip(snapshot: dict) -> dict:
    return {k: v for k, v in snapshot.items() if k != "row_state"}


def main() -> None:
    for raw_path in sorted(RAW.rglob("*.json")):
        target = BASELINE / raw_path.relative_to(RAW)
        stripped = strip(json.loads(raw_path.read_text(encoding="utf-8")))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(stripped, indent=2, default=str), encoding="utf-8")
        print(f"{raw_path.relative_to(BASELINE)} -> {target.relative_to(BASELINE)} ({target.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
