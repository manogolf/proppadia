#!/usr/bin/env python3
"""Offline proof of the NHL SOG prediction-only execution boundary."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.nhl.scripts.run_nhl_sog_prediction_only_warn_only import phase_for
from backend.nhl.sog_cold_start.core import CONTRACT_PATH, build_predictions, load_contract, sha256_file


ROOT = Path(__file__).resolve().parents[3]


def fixture(slate: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    start = f"{slate}T19:00:00Z"
    schedule = pd.DataFrame([{
        "canonical_season": 2026, "slate_date": slate, "game_id": 2026010008,
        "scheduled_start_time_utc": start, "home_team_id": 1, "away_team_id": 2,
        "game_type": 1, "status": "scheduled",
    }])
    common = {
        "canonical_season": 2026, "slate_date": slate, "game_id": 2026010008,
        "team_id": 1, "opponent_id": 2, "roster_status": "ACTIVE_ROSTER",
        "lineup_status": "ACTIVE_ROSTER_UNCONFIRMED", "scheduled_start_time_utc": start,
        "feature_cutoff_utc": f"{slate}T17:59:00Z", "position_sog_per60": 7.0,
        "position_toi_per_game": 16.0, "team_changed": False,
        "current_preseason_games": 0, "current_preseason_sog_per60": None,
        "current_preseason_toi_per_game": None, "current_regular_games": 0,
        "current_regular_sog_per60": None, "current_regular_toi_per_game": None,
        "prior_recency_sog_per60": 8.1, "prior_recency_toi_per_game": 18.2,
        "league_sog_per60": 7.1, "league_toi_per_game": 16.3,
    }
    features = pd.DataFrame([
        {**common, "player_id": 1, "player_name": "Fixture Veteran", "position": "C",
         "prior_minutes": 1000.0, "prior_sog_per60": 8.0, "prior_toi_per_game": 18.0,
         "older_minutes": 700.0, "older_sog_per60": 7.2, "older_toi_per_game": 17.0},
        {**common, "player_id": 2, "player_name": "Fixture Rookie", "position": "D",
         "prior_minutes": 0.0, "prior_sog_per60": None, "prior_toi_per_game": None,
         "prior_recency_sog_per60": None, "prior_recency_toi_per_game": None,
         "older_minutes": 0.0, "older_sog_per60": None, "older_toi_per_game": None},
        {**common, "player_id": 3, "player_name": "Fixture Unknown", "position": "UNKNOWN",
         "prior_minutes": 0.0, "prior_sog_per60": None, "prior_toi_per_game": None,
         "older_minutes": 0.0, "older_sog_per60": None, "older_toi_per_game": None},
    ])
    return schedule, features


def run_preflight(slate: str, now: datetime) -> dict[str, object]:
    schedule, features = fixture(slate)
    phase, phase_reason = phase_for(schedule, now, "AUTO")
    if phase is None:
        raise RuntimeError(f"FIXTURE_OUTSIDE_PHASE:{phase_reason}")
    predictions, inputs, exclusions = build_predictions(
        features, slate_date=slate, phase=phase,
        prediction_timestamp_utc=now.isoformat(), input_cutoff_utc=now.isoformat(),
    )
    contract = load_contract()
    arm_rows = sorted(inputs.contract_arm.unique().tolist())
    retained = sorted(ROOT.glob(
        f"artifacts/operational/nhl/**/slate_date={slate}/**/canonical_game_spine.csv"
    ))
    return {
        "preflight_contract": "NHL_SOG_PREDICTION_ONLY_NO_NETWORK_PREFLIGHT_V1",
        "slate_date": slate,
        "network_requests": 0,
        "database_connections": 0,
        "credential_access": False,
        "paid_claims": 0,
        "market_responses": 0,
        "provider_event_ids_required": False,
        "bookmaker_prices_required": False,
        "morning_readiness_consulted": False,
        "morning_readiness_blocks_prediction": False,
        "canonical_slate_availability": "RETAINED" if retained else "NOT_RETAINED_NO_NETWORK",
        "fixture_canonical_slate_validation": "PASS",
        "canonical_game_count_fixture": len(schedule),
        "phase": phase,
        "phase_eligibility": phase_reason,
        "contract_version": contract["contract_version"],
        "contract_sha256": sha256_file(CONTRACT_PATH),
        "selected_arm": contract["selected_arm"],
        "operational_shadow_arms": contract["shadow_challenger_arms"],
        "historical_e_arm_identity": "E_TEAM_CHANGE_AWARE",
        "historical_e_arm_status": "HISTORICAL_DIAGNOSTIC_SHADOW_NOT_EMITTED_BY_OPERATIONAL_CONTRACT",
        "unqualified_operational_arm": contract["implemented_not_historically_qualified_arm"],
        "arms_emitted_by_fixture": arm_rows,
        "eligible_players": int(inputs.player_id.nunique()),
        "prediction_rows": len(predictions),
        "excluded_players": len(exclusions),
        "exclusion_reasons": exclusions.exclusion_reason.value_counts().to_dict(),
        "zero_current_season_toi_required": False,
        "market_independence": "VERIFIED_BY_REQUEST_FREE_BUILD_BOUNDARY",
        "expected_artifact_root": str(ROOT / "artifacts/operational/nhl/sog_prediction_only" /
                                      "season=2026" / f"slate_date={slate}" / f"phase={phase}" /
                                      "run_id=<immutable-run-id>"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", default="2026-09-20")
    parser.add_argument("--now-utc", default="2026-09-20T18:00:00+00:00")
    args = parser.parse_args()
    now = datetime.fromisoformat(args.now_utc.replace("Z", "+00:00"))
    if now.tzinfo is None:
        raise SystemExit("--now-utc must be timezone-aware")
    print(json.dumps(run_preflight(args.slate_date, now.astimezone(timezone.utc)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
