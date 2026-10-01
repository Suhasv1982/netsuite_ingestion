"""Keep only the newest N (and any younger than --min-age-days) pre-release backup branches of a Lakebase project (promote-prod runs it right after it
creates a new backup branch). Only branches named `pre-release-*` are ever considered; production, dev and every
other branch are never touched. Deletion is permanent. Policy (defaults): keep the newest 3, which includes the
backup the current release just took, AND every backup younger than 14 days, so a release's own backups survive
until the release is long settled.

    python tools/prune_backup_branches.py --project aidq-metadata --keep 3 --dry-run
    python tools/prune_backup_branches.py --project aidq-metadata --keep 3
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys

PREFIX = "pre-release-"
PROTECTED = {"production", "dev"}


def _created(b: dict) -> dt.datetime | None:
    t = b.get("create_time")
    return dt.datetime.fromisoformat(t.replace("Z", "+00:00")) if t else None


def select_for_deletion(branches: list[dict], keep: int, min_age_days: int = 14, now: dt.datetime | None = None) -> list[str]:
    """Branch ids to delete: `pre-release-*` branches beyond the `keep` newest (by create_time) that are also
    older than `min_age_days`. A branch without a create_time is never deleted."""
    if keep < 1:
        raise ValueError("keep must be at least 1: the backup just taken must survive")
    now = now or dt.datetime.now(dt.timezone.utc)
    backups = [b for b in branches if b["id"].startswith(PREFIX) and b["id"] not in PROTECTED]
    backups.sort(key=lambda b: (b.get("create_time") or "", b["id"]), reverse=True)
    cutoff = now - dt.timedelta(days=min_age_days)
    return [b["id"] for b in backups[keep:] if _created(b) is not None and _created(b) < cutoff]


def _cli(profile, *args):
    cmd = ["databricks", *args, "-o", "json"] + (["--profile", profile] if profile else [])
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode:
        raise RuntimeError(f"databricks {' '.join(args[:3])}: {p.stderr.strip()[:300]}")
    return json.loads(p.stdout) if p.stdout.strip() else {}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--project", required=True)
    p.add_argument("--keep", type=int, default=3)
    p.add_argument("--min-age-days", type=int, default=14)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--profile")
    args = p.parse_args(argv)

    raw = _cli(args.profile, "postgres", "list-branches", f"projects/{args.project}")
    branches = [{"id": b["name"].rsplit("/", 1)[-1], "create_time": b.get("create_time")} for b in raw]
    doomed = select_for_deletion(branches, args.keep, args.min_age_days)
    kept = [b["id"] for b in branches if b["id"].startswith(PREFIX) and b["id"] not in doomed]
    print(f"keep newest {args.keep} {PREFIX}* branches and those younger than {args.min_age_days} days: {', '.join(sorted(kept)) or '-'}")
    for bid in doomed:
        if args.dry_run:
            print(f"would delete {bid}")
            continue
        _cli(args.profile, "postgres", "delete-branch", f"projects/{args.project}/branches/{bid}")
        print(f"deleted {bid}")
    if not doomed:
        print("nothing to delete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
