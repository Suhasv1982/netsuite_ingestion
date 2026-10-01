"""Daily-schedule generator options: schema-only backups and their retention (tools/pg_writer.py, netsuite_gen.py)."""

import pytest

import netsuite_gen
from pg_writer import DAILY_BACKUP_PREFIX, daily_backups_to_drop

NAMES = [
    "netsuite", "netsuite_backup_202609242053", "netsuite_backup_202609260109",   # historical copies
    DAILY_BACKUP_PREFIX + "202610050900", DAILY_BACKUP_PREFIX + "202610030900", DAILY_BACKUP_PREFIX + "202610040900",
]


def test_keeps_the_newest_daily_backups_and_drops_older_ones():
    assert daily_backups_to_drop(NAMES, 2) == [DAILY_BACKUP_PREFIX + "202610030900"]


def test_historical_backups_are_never_dropped():
    assert not [n for n in daily_backups_to_drop(NAMES, 1) if not n.startswith(DAILY_BACKUP_PREFIX)]


def test_keep_must_protect_todays_backup():
    with pytest.raises(ValueError):
        daily_backups_to_drop(NAMES, 0)


def test_backup_keep_needs_schema_mode():
    assert netsuite_gen.main(["--increment", "--seed", "1", "--backup-keep", "7", "--dry-run"]) == 2


def test_daily_flags_parse():
    args = netsuite_gen.build_parser().parse_args(
        ["--increment", "--seed", "7", "--auth", "sdk", "--backup", "schema", "--backup-keep", "7"])
    assert (args.auth, args.backup, args.backup_keep) == ("sdk", "schema", 7)
