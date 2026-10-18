"""Regression coverage for duplicate MLB display names on one organization."""

from __future__ import annotations

import json
from types import SimpleNamespace

import psycopg
import pytest

from backend.mlb.scripts.refresh_mlb_players_rosters import _upsert_player


class _UniquePlayerTeamViolation(psycopg.errors.UniqueViolation):
    @property
    def diag(self):
        return SimpleNamespace(constraint_name="unique_player_team")


class _DifferentUniqueViolation(psycopg.errors.UniqueViolation):
    @property
    def diag(self):
        return SimpleNamespace(constraint_name="player_ids_player_id_key")


class _Cursor:
    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append((statement, params))
        if statement.startswith("SELECT mlb.safe_upsert_player_roster") and params:
            payload = json.loads(params[0])
            if "team" in payload and self.error is not None:
                error, self.error = self.error, None
                raise error


def test_jared_jones_pit_collision_retry_preserves_existing_display_labels():
    cur = _Cursor(_UniquePlayerTeamViolation("duplicate display label"))

    label_conflict = _upsert_player(
        cur,
        set(),
        player_id=683003,
        player_name="Jared Jones",
        team_abbr="PIT",
        team_id=134,
    )

    assert label_conflict is True
    assert cur.calls[0][0] == "SAVEPOINT mlb_roster_player_upsert"
    first_payload = json.loads(cur.calls[1][1][0])
    retry_payload = json.loads(cur.calls[3][1][0])
    assert first_payload == {
        "player_id": 683003,
        "player_name": "Jared Jones",
        "team": "PIT",
        "team_id": 134,
    }
    assert cur.calls[2][0] == "ROLLBACK TO SAVEPOINT mlb_roster_player_upsert"
    assert retry_payload == {
        "player_id": 683003,
        "team_id": 134,
    }
    assert cur.calls[4][0] == "RELEASE SAVEPOINT mlb_roster_player_upsert"


def test_new_player_id_insert_keeps_verified_display_labels():
    cur = _Cursor()

    label_conflict = _upsert_player(
        cur,
        set(),
        player_id=683003,
        player_name="Jared Jones",
        team_abbr="PIT",
        team_id=134,
    )

    assert label_conflict is False
    assert json.loads(cur.calls[1][1][0]) == {
        "player_id": 683003,
        "player_name": "Jared Jones",
        "team": "PIT",
        "team_id": 134,
    }
    assert cur.calls[-1][0] == "RELEASE SAVEPOINT mlb_roster_player_upsert"


def test_other_unique_violation_is_not_retried_as_a_label_collision():
    cur = _Cursor(_DifferentUniqueViolation("duplicate canonical ID"))

    with pytest.raises(_DifferentUniqueViolation):
        _upsert_player(
            cur,
            set(),
            player_id=683003,
            player_name="Jared Jones",
            team_abbr="PIT",
            team_id=134,
        )

    assert [sql for sql, _ in cur.calls].count(
        "SELECT mlb.safe_upsert_player_roster(%s::jsonb)"
    ) == 1
