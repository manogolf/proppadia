from __future__ import annotations

import itertools
import os
import unittest


os.environ.setdefault("SUPABASE_DB_URL", "postgresql://offline.invalid/test")
os.environ.setdefault("SLATE_DATE", "2026-09-23")

from backend.nhl.player_stage_normalizer import (  # noqa: E402
    PlayerStageConflict,
    normalize_player_stage_rows,
)
from backend.nhl.scripts.refresh_players_and_roster_today import (  # noqa: E402
    _dedupe_roster_rows,
    merge_roster_status_from_temp,
    stage_and_upsert_players,
)


def _player(player_id: int, team_id: int, **changes):
    row = {
        "player_id": player_id,
        "team_id": team_id,
        "first_name": "Morgan",
        "last_name": "Player",
        "position": "C",
        "shoots_catches": None,
        "active": True,
    }
    row.update(changes)
    return row


class _RecordingCursor:
    def __init__(self):
        self.executions = []
        self.batches = []

    def execute(self, sql, params=None):
        self.executions.append((str(sql), params))

    def executemany(self, sql, rows):
        self.batches.append((str(sql), [dict(row) for row in rows]))


class _RosterMergeCursor(_RecordingCursor):
    def __init__(self):
        super().__init__()
        self.last_sql = ""

    def execute(self, sql, params=None):
        self.last_sql = str(sql)
        super().execute(sql, params)

    def fetchone(self):
        if "to_regclass" in self.last_sql:
            return {"has_tmp": True}
        if "information_schema.columns" in self.last_sql:
            return {"has_col": False}
        raise AssertionError(self.last_sql)

    def fetchall(self):
        if "information_schema.columns" in self.last_sql:
            return [
                {"column_name": name}
                for name in ("game_id", "team_id", "player_id", "active_flag", "asof_ts")
            ]
        raise AssertionError(self.last_sql)


class _FixtureTransaction:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        self.before = {
            key: [dict(row) for row in value]
            for key, value in self.state.items()
        }
        return self

    def __exit__(self, kind, value, traceback):
        if kind is not None:
            self.state.clear()
            self.state.update(self.before)
        return False


class NhlPlayerStageNormalizerTest(unittest.TestCase):
    def test_split_squad_duplicate_players_collapse_before_upsert(self):
        rows = [
            _player(10, 10), _player(10, 10),  # TOR in both games
            _player(20, 9), _player(20, 9),    # OTT in both games
            _player(30, 24),                   # distinct ANA player
        ]
        normalized, counts = normalize_player_stage_rows(rows)
        self.assertEqual([row["player_id"] for row in normalized], [10, 20, 30])
        self.assertEqual(counts, {
            "source_rows": 5,
            "unique_player_ids": 3,
            "duplicate_source_rows": 2,
            "exact_rows_collapsed": 2,
            "complementary_groups_merged": 0,
            "conflicting_groups_rejected": 0,
        })

    def test_complementary_nullable_values_merge_deterministically(self):
        rows = [
            _player(10, 10, first_name="Morgan", last_name=None,
                    position=None, shoots_catches="L", active=None),
            _player(10, 10, first_name=None, last_name="Rielly",
                    position="D", shoots_catches=None, active=True),
        ]
        normalized, counts = normalize_player_stage_rows(rows)
        self.assertEqual(normalized, [{
            "player_id": 10,
            "team_id": 10,
            "first_name": "Morgan",
            "last_name": "Rielly",
            "position": "D",
            "shoots_catches": "L",
            "active": True,
        }])
        self.assertEqual(counts["complementary_groups_merged"], 1)
        self.assertEqual(counts["exact_rows_collapsed"], 0)

    def test_protected_non_null_conflicts_fail_closed(self):
        conflicts = {
            "first_name": (_player(10, 10), _player(10, 10, first_name="Mitch")),
            "last_name": (_player(10, 10), _player(10, 10, last_name="Marner")),
            "position": (_player(10, 10, position="C"), _player(10, 10, position="D")),
            "team_id": (_player(10, 10), _player(10, 9)),
            "active": (_player(10, 10, active=True), _player(10, 10, active=False)),
            "provider_identity": (
                _player(10, 10, provider="nhl", provider_player_id=10),
                _player(10, 10, provider="nhl", provider_player_id=99),
            ),
        }
        for expected_field, rows in conflicts.items():
            with self.subTest(field=expected_field):
                with self.assertRaises(PlayerStageConflict) as raised:
                    normalize_player_stage_rows(rows)
                actual_fields = set(raised.exception.conflicts[0]["fields"])
                if expected_field == "provider_identity":
                    self.assertTrue(
                        {"provider_identity", "provider_player_id"} & actual_fields)
                else:
                    self.assertIn(expected_field, actual_fields)
                self.assertEqual(
                    raised.exception.counts["conflicting_groups_rejected"], 1)

    def test_wrong_provider_fails_even_for_a_single_row(self):
        with self.assertRaisesRegex(PlayerStageConflict, "provider"):
            normalize_player_stage_rows([_player(10, 10, provider="other")])

    def test_input_order_does_not_change_rows_or_counts(self):
        rows = [
            _player(20, 9),
            _player(10, 10, first_name="Morgan", last_name=None, position=None),
            _player(10, 10, first_name=None, last_name="Rielly", position="D"),
            _player(20, 9),
        ]
        expected = normalize_player_stage_rows(rows)
        for ordering in itertools.permutations(rows):
            self.assertEqual(normalize_player_stage_rows(ordering), expected)

    def test_distinct_empty_and_single_rows(self):
        self.assertEqual(normalize_player_stage_rows([]), ([], {
            "source_rows": 0,
            "unique_player_ids": 0,
            "duplicate_source_rows": 0,
            "exact_rows_collapsed": 0,
            "complementary_groups_merged": 0,
            "conflicting_groups_rejected": 0,
        }))
        single = _player(2, 24, position="LW", active=None)
        normalized, counts = normalize_player_stage_rows([single])
        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["position"], "F")
        self.assertTrue(normalized[0]["active"])
        self.assertEqual(counts["unique_player_ids"], 1)

        distinct, counts = normalize_player_stage_rows([
            _player(3, 24), _player(1, 26), _player(2, 25),
        ])
        self.assertEqual([row["player_id"] for row in distinct], [1, 2, 3])
        self.assertEqual(counts["duplicate_source_rows"], 0)

    def test_exactly_one_destination_row_per_player_and_replay_is_idempotent(self):
        rows = [_player(10, 10), _player(10, 10), _player(20, 9)]
        first, first_counts = normalize_player_stage_rows(rows)
        second, second_counts = normalize_player_stage_rows(rows)
        self.assertEqual(first, second)
        self.assertEqual(first_counts, second_counts)
        self.assertEqual(len(first), len({row["player_id"] for row in first}))

    def test_conflict_is_rejected_before_any_dml(self):
        cursor = _RecordingCursor()
        with self.assertRaises(PlayerStageConflict):
            stage_and_upsert_players(
                cursor, [_player(10, 10), _player(10, 9)])
        self.assertEqual(cursor.executions, [])
        self.assertEqual(cursor.batches, [])

    def test_conflict_leaves_transaction_fixture_unchanged(self):
        state = {"staged": [{"player_id": 99}], "players": [{"player_id": 99}]}
        before = {key: [dict(row) for row in value] for key, value in state.items()}
        cursor = _RecordingCursor()
        with self.assertRaises(PlayerStageConflict):
            with _FixtureTransaction(state):
                stage_and_upsert_players(
                    cursor, [_player(10, 10), _player(10, 9)])
        self.assertEqual(state, before)
        self.assertEqual(cursor.executions, [])
        self.assertEqual(cursor.batches, [])

    def test_stage_boundary_advances_past_former_cardinality_failure(self):
        cursor = _RecordingCursor()
        counts = stage_and_upsert_players(cursor, [
            _player(10, 10), _player(10, 10),
            _player(20, 9), _player(20, 9),
        ])
        self.assertEqual(counts["source_rows"], 4)
        self.assertEqual(counts["unique_player_ids"], 2)
        self.assertEqual(len(cursor.batches), 1)
        staged = cursor.batches[0][1]
        self.assertEqual([row["player_id"] for row in staged], [10, 20])
        self.assertEqual(len(cursor.executions), 2)
        self.assertIn("TRUNCATE nhl.import_players_stage", cursor.executions[0][0])
        self.assertIn("ON CONFLICT (player_id) DO UPDATE", cursor.executions[1][0])

    def test_split_squad_roster_status_expansion_remains_game_scoped(self):
        roster_rows = [
            {"game_date": "2026-09-23", "team_id": 10, "player_id": 10,
             "active_flag": True, "pp_unit": "None"},
            {"game_date": "2026-09-23", "team_id": 10, "player_id": 10,
             "active_flag": True, "pp_unit": "None"},
        ]
        self.assertEqual(len(_dedupe_roster_rows(roster_rows)), 1)
        cursor = _RosterMergeCursor()
        merge_roster_status_from_temp(cursor, "2026-09-23")
        merge_sql = cursor.executions[-1][0]
        self.assertIn("g.home_team_id = r.team_id OR g.away_team_id = r.team_id", merge_sql)
        self.assertIn("g.game_id", merge_sql)
        self.assertIn("ON CONFLICT (game_id, team_id, player_id)", merge_sql)


if __name__ == "__main__":
    unittest.main()
