"""tools/prune_backup_branches.py: only the oldest pre-release-* branches beyond the newest N are deleted."""

import datetime as dt

import pytest

from prune_backup_branches import select_for_deletion

LATER = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)   # every backup below is older than 14 days by then
SAME_DAY = dt.datetime(2026, 10, 1, 17, 0, tzinfo=dt.timezone.utc)


def _b(i, t):
    return {"id": i, "create_time": t}


BRANCHES = [
    _b("production", "2026-08-20T00:00:00Z"),
    _b("dev", "2026-09-26T00:00:00Z"),
    _b("pre-release-202610010053", "2026-10-01T00:53:00Z"),
    _b("pre-release-339b24fe3ac5-202610010109", "2026-10-01T01:09:00Z"),
    _b("pre-release-bb48fef86b3d-202610010124", "2026-10-01T01:24:00Z"),
    _b("pre-release-bb48fef86b3d-202610011620", "2026-10-01T16:20:00Z"),
    _b("scratch-pre-release-x", "2020-01-01T00:00:00Z"),
]


def test_keeps_the_newest_n_and_deletes_the_rest_oldest_first():
    assert select_for_deletion(BRANCHES, 3, now=LATER) == ["pre-release-202610010053"]
    assert select_for_deletion(BRANCHES, 2, now=LATER) == ["pre-release-339b24fe3ac5-202610010109", "pre-release-202610010053"]


def test_never_touches_production_dev_or_other_names():
    doomed = select_for_deletion(BRANCHES, 1, now=LATER)
    assert not {"production", "dev", "scratch-pre-release-x"} & set(doomed)


def test_nothing_to_delete_within_the_limit():
    assert select_for_deletion(BRANCHES, 10, now=LATER) == []


def test_young_backups_survive_even_beyond_the_newest_n():
    assert select_for_deletion(BRANCHES, 1, now=SAME_DAY) == []


def test_a_branch_without_create_time_is_never_deleted():
    assert select_for_deletion([_b("pre-release-a", None), _b("pre-release-b", "2026-10-01T00:00:00Z")], 1, now=LATER) == []


def test_keep_must_protect_the_new_backup():
    with pytest.raises(ValueError):
        select_for_deletion(BRANCHES, 0)
