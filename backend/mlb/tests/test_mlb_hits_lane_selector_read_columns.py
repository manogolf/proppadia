from __future__ import annotations

import pandas as pd

from backend.mlb.scripts import run_mlb_hits_lane_selector_daily as selector


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Engine:
    def connect(self):
        return _Connection()


def test_selector_projection_covers_join_and_frozen_model_features_only():
    columns = selector.PLAYER_DERIVED_STATS_SELECTOR_COLUMNS
    assert len(columns) == 61
    assert columns[:4] == ("player_id", "game_id", "game_date", "team")
    assert set(columns[4:]) == {
        f"d{window}_{stat}"
        for window in (7, 15, 30)
        for stat in (
            "hits", "runs_scored", "rbis", "home_runs", "singles", "doubles",
            "triples", "walks", "strikeouts_batting", "stolen_bases", "total_bases",
            "hits_runs_rbis", "runs_rbis", "outs_recorded", "strikeouts_pitching",
            "walks_allowed", "earned_runs", "hits_allowed", "at_bats",
        )
    }
    audit_columns = pd.read_csv(selector.DEFAULT_TRAIN_AUDIT, nrows=0).columns
    audit_stat_columns = {
        name.removeprefix("pds_")
        for name in audit_columns
        if name.startswith("pds_")
    }
    assert audit_stat_columns == set(columns[4:])


def test_both_selector_reads_use_explicit_columns_and_preserve_row_dedup(monkeypatch):
    captured = []

    def read_sql(query, _conn, params=None):
        sql = str(query)
        captured.append((sql, params))
        return pd.DataFrame(
            [
                {"player_id": "10", "game_id": "100", "game_date": "2026-08-01", "team": "NYY"},
                {"player_id": "10", "game_id": "100", "game_date": "2026-08-01", "team": "NYY"},
            ]
        )

    monkeypatch.setattr(selector.pd, "read_sql", read_sql)

    exact = selector._load_player_derived_stats(_Engine(), [100], 20)
    latest = selector._load_latest_player_derived_stats(_Engine(), [10], "2026-08-02", 20)

    expected_projection = ", ".join(selector.PLAYER_DERIVED_STATS_SELECTOR_COLUMNS)
    assert len(captured) == 2
    for sql, _params in captured:
        assert "SELECT *" not in sql
        assert expected_projection in sql
        assert "FROM mlb.player_derived_stats" in sql
    assert "DISTINCT ON (player_id) " + expected_projection in captured[1][0]
    assert exact[["player_id", "game_id"]].values.tolist() == [[10, 100]]
    assert latest["player_id"].tolist() == [10]
