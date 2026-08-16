from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from backend.mlb.totals_predictions.live_context_bridge_v1 import _bullpen


OUTPUT = Path("artifacts/analysis/model_development/mlb_totals_bullpen_recency_freshness_repair_impact_audit_v1/2026-08-16")


def record(day: int, game_pk: int, outs: int, *, acquired: str | None = None, team_id: int = 1):
    return {
        "date": date(2026, 8, day), "game_pk": game_pk, "pitcher_id": game_pk,
        "team_id": team_id, "outs": outs, "runs": 0, "source_sha256": str(game_pk),
        "source_acquired_at_utc": acquired,
    }


def history(records, available_dates, **provenance):
    return {
        "league_total": 9.0, "team_relievers": {1: records},
        "bullpen_history_provenance": {
            "available_completed_game_dates": available_dates,
            **provenance,
        },
    }


def test_valid_zero_is_distinct_from_stale_history():
    valid = history([record(12, 1, 0)], ["2026-08-14"])
    stale = history([record(10, 1, 9)], ["2026-08-10"])
    valid_state = _bullpen(1, date(2026, 8, 15), valid)
    stale_state = _bullpen(1, date(2026, 8, 15), stale)
    assert valid_state["recent_innings_burden"] == 0
    assert valid_state["freshness_status"] == "CURRENT_STRICT_PRIOR_HISTORY"
    assert stale_state["recent_innings_burden"] is None
    assert stale_state["certification_status"] == "BULLPEN_HISTORY_STALE"


def test_three_day_formula_is_strict_prior_and_same_date_safe():
    state = _bullpen(1, date(2026, 8, 15), history([
        record(11, 1, 30),  # outside three-calendar-day window
        record(12, 2, 3), record(13, 3, 6), record(14, 4, 9),
        record(15, 5, 27),  # same-date/doubleheader state is excluded
    ], ["2026-08-14"]))
    assert state["recent_innings_burden"] == 6.0
    assert state["source_last_team_game_date"] == "2026-08-14"
    assert state["expected_latest_prior_date"] == "2026-08-14"


def test_prediction_time_acquisition_cutoff_fails_closed_until_source_exists():
    acquired = "2026-08-15T12:45:00+00:00"
    retained = record(14, 4, 9, acquired=acquired)
    source = {"official_date": "2026-08-14", "source_acquired_at_utc": acquired}
    governed = history(
        [retained], ["2026-08-05", "2026-08-14"],
        frozen_base_last_game_date="2026-08-05", supplement_sources=[source],
    )
    before = _bullpen(1, date(2026, 8, 15), governed, "2026-08-15T12:44:59+00:00")
    after = _bullpen(1, date(2026, 8, 15), governed, "2026-08-15T12:45:01+00:00")
    assert before["certification_status"] == "BULLPEN_HISTORY_STALE"
    assert before["recent_innings_burden"] is None
    assert after["certification_status"] == "GOVERNED_TEAM_RELIEVER_HISTORY"
    assert after["recent_innings_burden"] == 3.0


def test_team_identity_isolation_and_no_target_outcome_input():
    governed = history([record(14, 4, 9, team_id=1)], ["2026-08-14"])
    governed["team_relievers"][2] = [record(14, 5, 3, team_id=2)]
    assert _bullpen(1, date(2026, 8, 15), governed)["recent_innings_burden"] == 3.0
    assert _bullpen(2, date(2026, 8, 15), governed)["recent_innings_burden"] == 1.0
    assert "outcome" not in _bullpen.__code__.co_varnames


def test_audit_package_preserves_population_and_reports_repaired_gate():
    affected = pd.read_csv(OUTPUT / "totals_bullpen_recency_affected_dates.csv")
    states = pd.read_csv(OUTPUT / "totals_bullpen_recency_corrected_feature_states.csv")
    sources = pd.read_csv(OUTPUT / "totals_bullpen_recency_external_source_manifest.csv")
    gates = pd.read_csv(OUTPUT / "totals_c_bullpen_gate_recheck.csv")
    assert len(states) == 141 and states.canonical_identity.nunique() == 141
    assert affected.loc[affected.percentage_games_affected.gt(0), "scoring_date"].min() == "2026-08-07"
    assert len(sources) == 134 and sources.game_pk.nunique() == 134
    assert set(gates[gates.gate.isin(["D", "H"])].status) == {"PASS"}
    decision = (OUTPUT / "totals_bullpen_recency_repair_decision.md").read_text()
    assert "BULLPEN_RECENCY_FRESHNESS_REPAIR_VALIDATED" in decision
    assert "RAW_PROSPECTIVE_RECORD_PARTIALLY_CONTAMINATED_BY_STALE_BULLPEN_STATE" in decision
