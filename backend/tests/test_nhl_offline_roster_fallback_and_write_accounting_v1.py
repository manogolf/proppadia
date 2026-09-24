from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

os.environ.setdefault("SUPABASE_DB_URL", "postgresql://offline.invalid/test")
os.environ.setdefault("SLATE_DATE", "2026-09-23")

from backend.nhl import cli  # noqa: E402
from backend.nhl.daily_orchestration import DailyRunRecorder  # noqa: E402
from backend.nhl.scripts.refresh_players_and_roster_today import (  # noqa: E402
    RosterStatusConflict,
    _roster_status_upsert_parts,
    normalize_roster_status_rows,
    upsert_roster_status_from_features,
)


SLATE = "2026-09-23"
KEYS = {"game_id", "team_id", "player_id"}


class FeatureCursor:
    def __init__(self, *, source_rows=4, unique=2, affected=2):
        self.source_rows = source_rows
        self.unique = unique
        self.affected = affected
        self.executions: list[tuple[str, object]] = []
        self.rowcount = -1

    def execute(self, sql, params=None):
        text = str(sql)
        self.executions.append((text, params))
        if "INSERT INTO nhl.roster_status" in text:
            self.rowcount = self.affected
        else:
            self.rowcount = 1

    def fetchone(self):
        return {
            "source_rows": self.source_rows,
            "unique_identities": self.unique,
            "exact_rows_collapsed": self.source_rows - self.unique,
            "complementary_groups_merged": 0,
            "conflicting_groups_rejected": 0,
            "roster_status_upsert_rows": self.affected,
        }


@pytest.mark.parametrize(
    "columns,present,absent",
    [
        (KEYS, (), ("active_flag", "line_role", "pp_unit", "asof_ts")),
        (KEYS | {"active_flag"}, ("active_flag",), ("line_role", "pp_unit", "asof_ts")),
        (KEYS | {"line_role"}, ("line_role",), ("active_flag", "pp_unit", "asof_ts")),
        (KEYS | {"pp_unit"}, ("pp_unit",), ("active_flag", "line_role", "asof_ts")),
        (
            KEYS | {"active_flag", "line_role", "pp_unit", "asof_ts"},
            ("active_flag", "line_role", "pp_unit", "asof_ts"),
            (),
        ),
    ],
)
def test_explicit_target_contract_supports_live_and_legacy_shapes(columns, present, absent):
    insert, select, update = _roster_status_upsert_parts(
        target_columns=columns, source_alias="sc")
    assert insert.split(", ")[:3] == ["game_id", "team_id", "player_id"]
    assert select.split(", ")[:3] == ["sc.game_id", "sc.team_id", "sc.player_id"]
    for name in present:
        assert name in insert
    for name in absent:
        assert name not in insert
        assert f"EXCLUDED.{name}" not in update
    assert "*" not in select


def test_fallback_source_supplies_typed_active_and_typed_null_roles():
    cursor = FeatureCursor()
    counts = upsert_roster_status_from_features(
        cursor, SLATE,
        target_columns=KEYS | {"active_flag", "line_role", "pp_unit", "asof_ts"},
    )
    sql, params = cursor.executions[-1]
    assert "TRUE::boolean AS active_flag" in sql
    assert sql.count("NULL::text AS line_role") >= 2
    assert sql.count("NULL::text AS pp_unit") >= 2
    assert "SELECT *" not in sql
    assert params == (SLATE, SLATE, SLATE, SLATE)
    assert counts["source_rows"] == 4
    assert counts["unique_identities"] == 2
    assert counts["exact_rows_collapsed"] == 2
    assert counts["roster_status_upsert_rows"] == 2


def test_null_fallback_roles_preserve_existing_nonnull_roles():
    _, _, update = _roster_status_upsert_parts(
        target_columns=KEYS | {"line_role", "pp_unit"}, source_alias="sc")
    assert "line_role = COALESCE(EXCLUDED.line_role, nhl.roster_status.line_role)" in update
    assert "pp_unit = COALESCE(EXCLUDED.pp_unit, nhl.roster_status.pp_unit)" in update


def roster_row(**changes):
    row = {
        "game_date": SLATE,
        "team_id": 10,
        "player_id": 99,
        "active_flag": True,
        "line_role": None,
        "pp_unit": None,
    }
    row.update(changes)
    return row


def test_roster_exact_duplicate_collapse_is_deterministic():
    normalized, counts = normalize_roster_status_rows([roster_row(), roster_row()])
    assert len(normalized) == 1
    assert counts == {
        "source_rows": 2,
        "unique_identities": 1,
        "duplicate_source_rows": 1,
        "exact_rows_collapsed": 1,
        "complementary_groups_merged": 0,
        "conflicting_groups_rejected": 0,
    }


def test_roster_complementary_nulls_merge():
    normalized, counts = normalize_roster_status_rows([
        roster_row(line_role="L1"), roster_row(pp_unit="PP1"),
    ])
    assert normalized[0]["line_role"] == "L1"
    assert normalized[0]["pp_unit"] == "PP1"
    assert counts["complementary_groups_merged"] == 1


@pytest.mark.parametrize("field,values", [
    ("active_flag", (True, False)),
    ("line_role", ("L1", "L2")),
    ("pp_unit", ("PP1", "PP2")),
])
def test_roster_protected_conflict_fails_before_dml(field, values):
    with pytest.raises(RosterStatusConflict) as raised:
        normalize_roster_status_rows([
            roster_row(**{field: values[0]}), roster_row(**{field: values[1]}),
        ])
    assert raised.value.counts["conflicting_groups_rejected"] == 1


def test_feature_fallback_is_exact_date_and_verified_canonical_game_scoped():
    cursor = FeatureCursor()
    upsert_roster_status_from_features(cursor, SLATE, target_columns=KEYS | {"active_flag"})
    sql = cursor.executions[-1][0]
    assert sql.count("WHERE f.game_date = %s::date") == 2
    assert sql.count("g.game_date = %s::date") == 2
    assert sql.count("g.game_id = f.game_id") == 2
    assert sql.count("g.home_team_id = f.team_id OR g.away_team_id = f.team_id") == 2
    assert "ON CONFLICT (game_id, team_id, player_id)" in sql


def test_prior_day_feature_fallback_succeeds_provider_free_when_fetch_disabled(monkeypatch):
    monkeypatch.setenv("NHL_FETCH_DISABLE", "1")
    cursor = FeatureCursor()
    with patch(
        "backend.nhl.scripts.refresh_players_and_roster_today.official_get",
        side_effect=AssertionError("provider forbidden"),
    ) as provider:
        upsert_roster_status_from_features(cursor, SLATE, target_columns=KEYS)
    provider.assert_not_called()


def test_fallback_sql_failure_rolls_back_transaction_state():
    state: list[str] = []

    class FailingCursor(FeatureCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "INSERT INTO nhl.roster_status" in str(sql):
                state.append("partial")
                raise RuntimeError("fixture SQL failure")

    class Transaction:
        def __enter__(self):
            self.before = list(state)
            return self

        def __exit__(self, kind, value, traceback):
            if kind is not None:
                state[:] = self.before
            return False

    with pytest.raises(RuntimeError, match="fixture SQL failure"):
        with Transaction():
            upsert_roster_status_from_features(
                FailingCursor(), SLATE, target_columns=KEYS | {"active_flag"})
    assert state == []


def test_fallback_replay_is_row_idempotent_by_natural_key():
    first = FeatureCursor(affected=2)
    second = FeatureCursor(affected=2)
    upsert_roster_status_from_features(first, SLATE, target_columns=KEYS | {"active_flag"})
    upsert_roster_status_from_features(second, SLATE, target_columns=KEYS | {"active_flag"})
    assert first.executions[-1][0] == second.executions[-1][0]
    assert "ON CONFLICT (game_id, team_id, player_id) DO UPDATE" in first.executions[-1][0]


def recorder() -> DailyRunRecorder:
    return DailyRunRecorder(run_id="offline", command=["daily"], phase="EARLY")


def test_successful_write_capable_child_without_counts_is_possible_unquantified():
    value = recorder()
    completed = subprocess.CompletedProcess(["child"], 0, stdout="", stderr="")
    with patch.object(cli, "_ACTIVE_DAILY_RECORDER", value), patch.object(
        cli, "_ACTIVE_DAILY_LANE", "shared_prerequisites",
    ), patch.object(cli.sp, "run", return_value=completed):
        cli.run(["child"], database_write_capable=True)
    lane = value.lane("shared_prerequisites")
    assert lane.database_write_status == "POSSIBLE_UNQUANTIFIED"
    assert lane.database_rows_written is None
    value.finish_lane("shared_prerequisites", database_rows_written=True)
    assert lane.database_write_status == "POSSIBLE_UNQUANTIFIED"


def test_successful_child_with_positive_committed_count_is_committed():
    value = recorder()
    stdout = "NHL_CHILD_SUMMARY_JSON=" + json.dumps({
        "database_write_capable": True,
        "database_write_status": "COMMITTED",
        "transaction_disposition": "COMMITTED",
        "database_row_counts": {"roster_status_upsert_rows": 7},
    })
    completed = subprocess.CompletedProcess(["child"], 0, stdout=stdout, stderr="")
    with patch.object(cli, "_ACTIVE_DAILY_RECORDER", value), patch.object(
        cli.sp, "run", return_value=completed,
    ):
        cli.run(["child"], database_write_capable=True)
    assert value.lane("shared_prerequisites").database_write_status == "COMMITTED"
    assert value.children[0]["database_row_counts"] == {"roster_status_upsert_rows": 7}


def test_later_rollback_does_not_erase_earlier_independent_commit():
    value = recorder()
    value.record_child({
        "lane": "shared_prerequisites", "command_identity": "seed-goalie",
        "exit_status": 0, "database_write_capable": True,
        "database_write_status": "COMMITTED", "transaction_disposition": "COMMITTED",
        "database_row_counts": {"rows": 3},
    })
    value.record_child({
        "lane": "shared_prerequisites", "command_identity": "roster-fallback",
        "exit_status": 1, "database_write_capable": True,
        "database_write_status": "ROLLED_BACK", "transaction_disposition": "ROLLED_BACK",
        "database_row_counts": {},
    })
    assert value.lane("shared_prerequisites").database_write_status == "COMMITTED"
    assert value.database_write_status() == "COMMITTED"
    assert [event["database_write_status"] for event in value.lane(
        "shared_prerequisites").database_write_children] == ["COMMITTED", "ROLLED_BACK"]


def test_successful_zero_count_is_affirmatively_none():
    value = recorder()
    value.record_child({
        "lane": "shared_prerequisites", "command_identity": "zero-write",
        "exit_status": 0, "database_write_capable": True,
        "database_write_status": "COMMITTED", "transaction_disposition": "COMMITTED",
        "database_row_counts": {"rows": 0},
        "database_row_counts_complete": True,
    })
    assert value.lane("shared_prerequisites").database_write_status == "NONE"
    assert value.lane("shared_prerequisites").database_rows_written is False


def test_retained_failed_receipt_is_not_rewritten():
    package = Path(
        "artifacts/operational/nhl/daily_runs/"
        "run_id=nhldaily_20260924T184502006092Z_37f46bad"
    )
    from backend.nhl.daily_capture import sha256_file
    assert sha256_file(package / "SHA256SUMS") == (
        "5c8c311b9806513a60e699fed583c783761d6a5aafbba24860504687d1041241")
    assert sha256_file(package / "parent_receipt.json") == (
        "ddfc5adb46865c00e2137aea2773db0dd4e0eb4163fb101d75e46c4948dff952")
