"""Offline regression boundary for the active legacy stat-derived writer.

These tests deliberately use no database adapter or network fixture.  They
prove the active source admits an exact playable game identity before its
date-scoped transaction and serializes the legacy daily replacement steps.
"""
from __future__ import annotations

import inspect
import copy
import hashlib
import json

import pytest

from backend.mlb.scripts import insert_mlb_stat_derived as active


class _StreakCursor:
    def __init__(self, row, last_game_date=None):
        self.row = row
        self.last_game_date = last_game_date
        self.sql = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params
        if self.last_game_date is not None and self.last_game_date >= params[2]:
            self.row = None

    def fetchone(self):
        return self.row


class _StreakConnection:
    def __init__(self, row, last_game_date=None):
        self.cursor_value = _StreakCursor(row, last_game_date)

    def cursor(self):
        return self.cursor_value


def test_streak_profile_lookup_requires_a_strictly_prior_last_game_date():
    conn = _StreakConnection(("hot", 3))
    assert active._get_streak(conn, 453286, "hits", "2026-09-25") == ("hot", 3)
    assert "last_game_date < %s::date" in conn.cursor_value.sql
    assert conn.cursor_value.params == ("453286", "hits", "2026-09-25")


@pytest.mark.parametrize("profile_last_game_date", ["2026-09-25", "2026-09-26"])
def test_streak_profile_lookup_rejects_same_day_or_future_profile(profile_last_game_date):
    conn = _StreakConnection(("hot", 3), profile_last_game_date)
    assert active._get_streak(conn, 453286, "hits", "2026-09-25") == (None, None)
    assert "last_game_date < %s::date" in conn.cursor_value.sql


def _game(game_pk, official_date, detailed, coded, status_code, **relationships):
    return {
        "gamePk": game_pk,
        "gameType": "R",
        "season": "2026",
        "officialDate": official_date,
        "gameDate": f"{official_date}T20:00:00Z",
        "teams": {"away": {"team": {"id": 1}}, "home": {"team": {"id": 2}}},
        "status": {
            "abstractGameState": "Final",
            "detailedState": detailed,
            "codedGameState": coded,
            "statusCode": status_code,
        },
        **relationships,
    }


def test_active_admission_uses_exact_game_pk_for_postponed_makeup_and_doubleheader():
    postponed = _game(824785, "2026-09-22", "Postponed", "D", "DR", rescheduleDate="2026-09-23T20:00:00Z")
    makeup = _game(824785, "2026-09-23", "Final", "F", "F", rescheduledFrom="2026-09-22T20:00:00Z")
    sibling = _game(824784, "2026-09-23", "Final", "F", "F", gameNumber=2, doubleHeader="Y")

    selected = active._resolved_final_game_entries(
        [postponed, makeup, sibling], require_regular_season=True
    )
    assert [(game_pk, game["officialDate"]) for game_pk, _, game in selected] == [
        (824784, "2026-09-23"),
        (824785, "2026-09-23"),
    ]
    assert len({game_pk for game_pk, _, _ in selected}) == 2
    receipts = active._schedule_decision_records(
        [postponed, makeup, sibling],
        {824784, 824785},
        {824784, 824785},
        require_regular_season=True,
    )
    makeup_receipt = next(item for item in receipts if item["game_pk"] == 824785)
    assert makeup_receipt["loader_decision"] == "SELECTED_FOR_PROCESSING"
    assert makeup_receipt["operational_date"] == "2026-09-23"
    assert len(makeup_receipt["appearances"]) == 2
    assert makeup_receipt["appearances"][0]["relations"]["rescheduleDate"]
    assert makeup_receipt["appearances"][1]["status"]["classification"] == active.PLAYABLE_TERMINAL
    sibling_receipt = next(item for item in receipts if item["game_pk"] == 824784)
    assert sibling_receipt["loader_decision"] == "SELECTED_FOR_PROCESSING"
    assert sibling_receipt["operational_date"] == "2026-09-23"


def test_active_admission_fails_closed_for_unknown_or_conflicting_finality():
    incomplete = _game(824785, "2026-09-23", "Final", "F", "")
    with pytest.raises(active.ActiveLoaderFinalityError, match="ACTIVE_FINALITY_CANDIDATE_UNRESOLVED"):
        active._resolved_final_game_entries([incomplete], require_regular_season=True)


def test_terminal_feed_confirms_selected_game_pk_date_and_playable_tuple():
    schedule = _game(824785, "2026-09-23", "Final", "F", "F")
    feed = {
        "gamePk": 824785,
        "gameData": {
            "status": schedule["status"],
            "datetime": {"officialDate": "2026-09-23"},
        },
    }
    active._validate_terminal_feed_for_candidate(824785, schedule, feed)
    feed["gameData"]["status"] = _game(1, "2026-09-22", "Postponed", "D", "DR")["status"]
    with pytest.raises(active.ActiveLoaderFinalityError, match="ACTIVE_TERMINAL_FEED_NOT_PLAYABLE"):
        active._validate_terminal_feed_for_candidate(824785, schedule, feed)


def test_candidate_validation_rejects_duplicate_exact_and_daily_keys_before_mutation():
    duplicate_exact = [
        {"player_id": 7, "game_id": 824785, "game_date": "2026-09-23"},
        {"player_id": 7, "game_id": 824785, "game_date": "2026-09-23"},
    ]
    with pytest.raises(RuntimeError, match="DUPLICATE_EXACT_KEY"):
        active._validate_legacy_derived_candidates(duplicate_exact)
    duplicate_daily = [
        {"player_id": 7, "game_id": 824784, "game_date": "2026-09-23"},
        {"player_id": 7, "game_id": 824785, "game_date": "2026-09-23"},
    ]
    with pytest.raises(RuntimeError, match="DUPLICATE_DAILY_KEY"):
        active._validate_legacy_derived_candidates(duplicate_daily)


class _Cursor:
    def __init__(self, candidates, *, insert_rowcount=1, fail_insert=False):
        self.candidates = candidates
        self.insert_rowcount = insert_rowcount
        self.fail_insert = fail_insert
        self.calls = []
        self.parameters = []
        self.rowcount = 0
        self.description = ()
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        return False

    def execute(self, sql, params=None):
        self.calls.append(sql)
        self.parameters.append(params)
        if "FROM target t" in sql:
            self._rows = self.candidates
            self.rowcount = 0
        elif "FOR UPDATE" in sql or "DELETE FROM mlb.player_derived_stats" in sql:
            self._rows = []
            self.rowcount = 0
        elif "INSERT INTO mlb.player_derived_stats" in sql:
            if self.fail_insert:
                raise RuntimeError("synthetic insert failure")
            self._rows = []
            self.rowcount = self.insert_rowcount

    def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_active_legacy_writer_orders_lock_delete_then_insert_and_is_repeat_safe():
    row = {"player_id": 7, "game_id": 824785, "game_date": "2026-09-23", "team": "NYY", "is_home": True}
    for metric in active.ROLLING_METRICS:
        row[f"d7_{metric}"] = 0
        row[f"d15_{metric}"] = 0
        row[f"d30_{metric}"] = 0
    cur = _Cursor([row])
    assert active._refresh_player_derived_stats(_Connection(cur), "2026-09-23", "2026-09-23") == 1
    ordered = "\n".join(cur.calls)
    assert ordered.index("FOR UPDATE") < ordered.index("DELETE FROM mlb.player_derived_stats")
    assert ordered.index("DELETE FROM mlb.player_derived_stats") < ordered.index("INSERT INTO mlb.player_derived_stats")
    assert "purged_conflicts" not in ordered
    assert "MAX(ps.game_id)" not in ordered

    repeat = _Cursor([row], insert_rowcount=0)
    assert active._refresh_player_derived_stats(_Connection(repeat), "2026-09-23", "2026-09-23") == 0


def test_exact_candidate_query_executes_through_fake_cursor_and_reaches_validation(monkeypatch):
    candidate = {"player_id": 9, "game_id": 824785, "game_date": "2026-09-23"}
    cur = _Cursor([candidate])
    observed = []
    original = active._validate_legacy_derived_candidates

    def validate(rows):
        observed.append(rows)
        original(rows)

    monkeypatch.setattr(active, "_validate_legacy_derived_candidates", validate)
    # _Connection/_Cursor are the fake backend: no operational connection or
    # database write is possible in this test.
    assert active._refresh_player_derived_stats(
        _Connection(cur), "2026-09-23", "2026-09-23"
    ) == 1
    query = cur.calls[0]
    assert "target AS (" in query
    assert ")\n        SELECT\n" in query
    assert "),\n        SELECT" not in query
    assert cur.parameters[0] == ("2026-09-23", "2026-09-23", "2026-09-23", "2026-09-23")
    assert observed == [[candidate]]


def test_active_legacy_writer_blocks_before_write_and_propagates_transaction_failure():
    duplicate = [
        {"player_id": 7, "game_id": 824785, "game_date": "2026-09-23"},
        {"player_id": 7, "game_id": 824785, "game_date": "2026-09-23"},
    ]
    cur = _Cursor(duplicate)
    with pytest.raises(RuntimeError, match="DUPLICATE_EXACT_KEY"):
        active._refresh_player_derived_stats(_Connection(cur), "2026-09-23", "2026-09-23")
    assert len(cur.calls) == 1  # candidate validation precedes lock/delete/insert

    row = {"player_id": 8, "game_id": 824785, "game_date": "2026-09-23", "team": "NYY", "is_home": True}
    for metric in active.ROLLING_METRICS:
        row[f"d7_{metric}"] = row[f"d15_{metric}"] = row[f"d30_{metric}"] = 0
    with pytest.raises(RuntimeError, match="synthetic insert failure"):
        active._refresh_player_derived_stats(
            _Connection(_Cursor([row], fail_insert=True)), "2026-09-23", "2026-09-23"
        )


def test_active_path_keeps_exact_feature_state_outside_legacy_writer_and_rolls_back_date_failure():
    source = inspect.getsource(active)
    run_source = inspect.getsource(active.run)
    assert "MAX(ps.game_id)" not in source
    assert "player_game_feature_state_v1" not in source
    assert "conn.rollback()" in run_source
    assert "game_date=operational_date" in run_source
    assert '"game_date": operational_date' in run_source
    assert run_source.index("_validate_terminal_feed_for_candidate") < run_source.index(
        "_upsert_game_info_min"
    )


class _TrainingCursor:
    """Small fake backend for the real MTP upsert SQL; never connects to PG."""

    def __init__(self, logical_rows):
        self.logical_rows = logical_rows
        self.rowcount = 0
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        return False

    def execute(self, sql, params=None):
        assert "INSERT INTO mlb.model_training_props" in sql
        assert "ON CONFLICT (player_id, game_id, prop_type, prop_source)" in sql
        self.params = params
        key = (int(params["player_id"]), int(params["game_id"]), params["prop_type"], params["prop_source"])
        comparable = {
            field: params.get(field)
            for field in (
                "prop_value", "line", "over_under", "outcome", "status", "was_correct",
                "game_time", "game_day_of_week", "time_of_day_bucket", "streak_type",
                "streak_count", "team", "opponent", "team_id", "opponent_team_id",
                "opponent_encoded", "is_home", "game_type",
            )
        }
        prior = self.logical_rows.get(key)
        if prior == comparable:
            self.rowcount = 0
        else:
            self.logical_rows[key] = comparable
            self.rowcount = 1


class _TrainingConnection:
    def __init__(self, logical_rows):
        self.logical_rows = logical_rows

    def cursor(self):
        return _TrainingCursor(self.logical_rows)


def _training_row(game_pk, outcome):
    return {
        "id": f"synthetic-{game_pk}",
        "game_id": game_pk,
        "player_id": "453286",
        "player_name": "fixture-player",
        "team": "1",
        "opponent": "2",
        "team_id": 1,
        "opponent_team_id": 2,
        "opponent_encoded": 2,
        "is_home": True,
        "prop_type": "hits",
        "prop_value": 1.0,
        "line": 0.5,
        "over_under": "over",
        "outcome": outcome,
        "status": "resolved",
        "created_at": "2026-09-23T12:00:00+00:00",
        "updated_at": "2026-09-23T12:00:00+00:00",
        "prop_source": "mlb_api",
        "was_correct": outcome == "win",
        "game_date": "2026-09-23",
        "game_time": "2026-09-23T20:00:00+00:00",
        "game_day_of_week": 2,
        "time_of_day_bucket": "evening",
        "streak_type": None,
        "streak_count": None,
    }


def test_mtp_fake_backend_replay_is_idempotent_and_keeps_824785_824784_distinct():
    logical_rows = {}
    conn = _TrainingConnection(logical_rows)
    proposals = [_training_row(824785, "win"), _training_row(824784, "loss")]
    first_writes = [
        active._upsert_training_row(conn, copy.deepcopy(row)) for row in proposals
    ]
    assert first_writes == [1, 1]
    assert len(logical_rows) == 2
    assert {key[1] for key in logical_rows} == {824785, 824784}

    repeat_writes = [
        active._upsert_training_row(conn, copy.deepcopy(row)) for row in proposals
    ]
    assert repeat_writes == [0, 0]
    assert len(logical_rows) == 2
    assert logical_rows[(453286, 824785, "hits", "mlb_api")] != logical_rows[
        (453286, 824784, "hits", "mlb_api")
    ]


def test_natural_evidence_source_files_are_hash_bound_private_and_non_overwriting(tmp_path):
    body = b'{"gamePk":824785,"status":{"detailedState":"Final"}}\n'
    path = tmp_path / "run" / "2026-09-23" / "live_feed_game_824785.json"
    active._write_immutable_evidence_file(path, body)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == active._sha256_bytes(body)
    assert path.stat().st_mode & 0o777 == 0o600
    receipt = {
        "source": {"path": str(path), "sha256": active._sha256_bytes(path.read_bytes())},
        "model_training_props_semantics": "SYNTHETIC_OUTCOME_TRAINING_NOT_PREGAME_PREDICTION",
    }
    assert json.loads(json.dumps(receipt))["source"]["sha256"] == hashlib.sha256(body).hexdigest()
    with pytest.raises(RuntimeError, match="STAT_DERIVED_EVIDENCE_COLLISION"):
        active._write_immutable_evidence_file(path, body)


def test_schedule_and_live_capture_retain_response_bytes_without_second_request(monkeypatch):
    schedule_body = b'{"dates":[{"games":[{"gamePk":824785}]}]}'
    feed_body = b'{"gamePk":824785,"gameData":{}}'
    calls = []

    class _Response:
        def __init__(self, body, payload):
            self.content = body
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    def fake_get(url, timeout):
        calls.append((url, timeout))
        if "/schedule?" in url:
            return _Response(schedule_body, {"dates": [{"games": [{"gamePk": 824785}]}]})
        return _Response(feed_body, {"gamePk": 824785, "gameData": {}})

    monkeypatch.setattr(active.requests, "get", fake_get)
    games, schedule_hash, retained_schedule_body = active._fetch_schedule(
        "2026-09-23", include_payload=True
    )
    feed, retained_feed_body = active._fetch_live_feed(824785, include_payload=True)
    assert games == [{"gamePk": 824785}]
    assert feed["gamePk"] == 824785
    assert retained_schedule_body == schedule_body
    assert retained_feed_body == feed_body
    assert schedule_hash == hashlib.sha256(schedule_body).hexdigest()
    assert len(calls) == 2
    assert calls[0][0].endswith("date=2026-09-23")
    assert calls[1][0].endswith("/824785/feed/live")


def test_per_game_count_reconciliation_fails_closed_when_any_total_does_not_balance():
    counts = {
        824785: {
            "game_info": {"candidate_rows": 1, "admitted_rows": 1, "quarantined_rows": 0, "written_rows": 1, "unchanged_rows": 0},
            "player_stats": {"candidate_rows": 1, "admitted_rows": 1, "quarantined_rows": 0, "written_rows": 1, "unchanged_rows": 0},
            "model_training_props": {"candidate_rows": 2, "admitted_rows": 1, "quarantined_rows": 1, "written_rows": 1, "unchanged_rows": 0},
        },
        824784: {
            "game_info": {"candidate_rows": 1, "admitted_rows": 1, "quarantined_rows": 0, "written_rows": 1, "unchanged_rows": 0},
            "player_stats": {"candidate_rows": 1, "admitted_rows": 1, "quarantined_rows": 0, "written_rows": 0, "unchanged_rows": 1},
            "model_training_props": {"candidate_rows": 1, "admitted_rows": 1, "quarantined_rows": 0, "written_rows": 0, "unchanged_rows": 1},
        },
    }
    active._validate_game_row_count_reconciliation(counts)
    counts[824784]["model_training_props"]["quarantined_rows"] = 1
    with pytest.raises(RuntimeError, match="STAT_DERIVED_ROW_COUNT_CANDIDATE_RECONCILIATION:model_training_props:824784"):
        active._validate_game_row_count_reconciliation(counts)
