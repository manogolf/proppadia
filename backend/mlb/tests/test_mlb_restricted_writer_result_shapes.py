"""Offline regression tests for scalar results on dict_row connections."""

from __future__ import annotations

import importlib
import sys
import types

import pytest

from backend.shared.db.pg import scalar_int_result
from backend.mlb.scripts import insert_mlb_stat_derived as stat_derived
from backend.mlb.scripts import refresh_mlb_bvp_pvb as bvp_pvb


class _ScalarCursor:
    def __init__(self, row):
        self.row = row
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchone(self):
        return self.row


class _ScalarConnection:
    def __init__(self, row):
        self.cursor_value = _ScalarCursor(row)

    def cursor(self):
        return self.cursor_value


def test_scalar_integer_result_accepts_mapping_and_tuple_rows():
    assert scalar_int_result({"result_count": 3}) == 3
    assert scalar_int_result((2,)) == 2
    assert scalar_int_result({"result_count": None}) == 0


def test_scalar_integer_result_fails_closed_on_missing_mapping_field():
    with pytest.raises(RuntimeError, match="missing expected column 'result_count'"):
        scalar_int_result({"safe_upsert_game_info": 1})


def test_scalar_integer_result_fails_closed_on_missing_row():
    with pytest.raises(RuntimeError, match="returned no row"):
        scalar_int_result(None)


def test_at_bats_safe_fill_reads_dict_row_result():
    conn = _ScalarConnection({"result_count": 1})
    assert stat_derived._backfill_player_stats_at_bats(
        conn, player_id=123, game_id=456, at_bats=4
    ) == 1
    assert "safe_fill_player_stats_at_bats" in conn.cursor_value.sql
    assert "AS result_count" in conn.cursor_value.sql


def test_placeholder_identity_safe_insert_reads_dict_row_result():
    conn = _ScalarConnection({"result_count": 1})
    assert stat_derived._upsert_player_id_min(
        conn,
        player_id=123,
        player_name="Fixture Player",
        team_abbr="LAD",
        team_id=119,
    ) == 1
    assert "safe_insert_player_id_if_absent" in conn.cursor_value.sql
    assert "AS result_count" in conn.cursor_value.sql


def test_bvp_pvb_safe_upsert_reads_dict_row_result():
    conn = _ScalarConnection({"result_count": 2})
    rows = [("hits", 123, 456, "2026-10-15", {"bvp_hits": 1}, "v1", "test")]
    assert bvp_pvb._upsert_rows_on_connection(conn, rows, batch_size=10) == 2
    assert "safe_upsert_prop_features_precomputed" in conn.cursor_value.sql
    assert "AS result_count" in conn.cursor_value.sql


def test_prediction_backfill_safe_fill_reads_dict_row_result(monkeypatch, tmp_path):
    monkeypatch.setenv("SUPABASE_URL", "https://offline-fixture.invalid")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "offline-fixture-key")
    monkeypatch.setenv("MODEL_DIR", str(tmp_path / "models"))
    supabase_stub = types.ModuleType("supabase")
    supabase_stub.create_client = lambda *_args, **_kwargs: object()
    monkeypatch.setitem(sys.modules, "supabase", supabase_stub)

    module_name = "backend.mlb.modeling.backfill_predictions"
    sys.modules.pop(module_name, None)
    predictions = importlib.import_module(module_name)
    conn = _ScalarConnection({"result_count": 1})

    class _ConnectionContext:
        def __enter__(self):
            return conn

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(predictions, "pg_connect", lambda: _ConnectionContext())
    assert predictions._persist_prediction_backfill(
        "00000000-0000-0000-0000-000000000001", "over", 0.7,
        "2026-10-17T12:00:00+00:00",
    ) == 1
    assert "safe_fill_prediction_backfill" in conn.cursor_value.sql
    assert "AS result_count" in conn.cursor_value.sql
