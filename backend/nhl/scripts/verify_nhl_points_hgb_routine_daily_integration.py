#!/usr/bin/env python3
"""Bounded no-DB proof of the same HGB capture helper called by daily CLI."""
from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path

import pandas as pd

from backend.nhl.points_hgb_shadow import ROOT, capture_from_files, sha


def main() -> None:
    evidence_path = ROOT / "artifacts/analysis/nhl/points_hgb_operational_bridge/2026-10-09/routine_daily_integration_evidence.json"
    with tempfile.TemporaryDirectory(prefix="nhl_points_hgb_daily_integration_") as temp:
        work = Path(temp)
        logs_path, outcomes_path, slate_path, phoenix_path = [work / name for name in
            ("logs.csv", "outcomes.csv", "slate.csv", "phoenix.csv")]
        pd.DataFrame([{"game_id":2026020001,"player_id":11,"team_id":1,"is_home":True,
            "toi_minutes":20,"pp_toi_minutes":2,"shots_on_goal":4,"shot_attempts":8}]).to_csv(logs_path,index=False)
        pd.DataFrame([{"canonical_season":2026,"game_date":"2026-10-08","game_id":2026020001,
            "player_id":11,"team_id":1,"official_goals":1,"official_assists":0,"official_final":True}]).to_csv(outcomes_path,index=False)
        pd.DataFrame([{"game_id":2026020066,"player_id":11,"game_date":"2026-10-09",
            "game_start_utc":"2026-10-10T01:00:00Z","is_home":True,"home_team_id":1,"away_team_id":2}]).to_csv(slate_path,index=False)
        pd.DataFrame([{"game_id":2026020066,"player_id":11,"line":0.5,"prob_over":0.7}]).to_csv(phoenix_path,index=False)
        phoenix_sha = hashlib.sha256(phoenix_path.read_bytes()).hexdigest()
        capture = capture_from_files(logs_path=logs_path,outcomes_paths=[outcomes_path],slate_path=slate_path,
            phoenix_predictions_path=phoenix_path,phoenix_prediction_sha256=phoenix_sha,
            phoenix_model_identity_sha256="bounded-phoenix-identity",
            phoenix_feature_contract_sha256="bounded-phoenix-feature-contract",
            phoenix_feature_cutoff_utc="2026-10-09T14:00:00Z",slate_date="2026-10-09",season=2026,
            capture_phase="EARLY",parent_run_id="bounded-dry-run",
            feature_cutoff_utc="2026-10-09T15:00:00Z",output_root=work/"operational")
        if capture["deterministic_replay"] != "DETERMINISTIC_REPLAY_PASS" or capture["coherence_crossing_count"] != 0:
            raise RuntimeError("HGB_ROUTINE_DAILY_INTEGRATION_VALIDATION_FAILED")
        evidence = {
            "schema_version":"NHL_POINTS_HGB_ROUTINE_DAILY_INTEGRATION_EVIDENCE_V1",
            "classification":"HGB_ROUTINE_DAILY_INTEGRATION_PASS",
            "normal_daily_entrypoint":"python -m backend.nhl.cli daily --with-odds",
            "proof_scope":"BOUNDED_DRY_RUN_SHARED_PRODUCTION_CAPTURE_HELPER",
            "production_orchestration_helper":"backend/nhl/points_hgb_shadow.py:capture_from_files",
            "production_orchestration_helper_sha256":sha(ROOT/"backend/nhl/points_hgb_shadow.py"),
            "daily_cli_integration_sha256":sha(ROOT/"backend/nhl/cli.py"),
            "feature_exporter_sha256":sha(ROOT/"backend/nhl/scripts/export_nhl_points_hgb_features.py"),
            "scorer_sha256":sha(ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py"),
            "model_artifact_sha256":sha(ROOT/"artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/nhl_points_count_hgb_v1.joblib"),
            "feature_contract":"NHL_POINTS_LEADER_CORE_FEATURES_V1",
            "history_contract":"120_DAY_LEGACY_BOUND",
            "deterministic_replay":capture["deterministic_replay"],
            "prediction_sha256":capture["prediction_sha256"],
            "coherence_crossing_count":capture["coherence_crossing_count"],
            "phoenix_control_prediction_sha256":phoenix_sha,
            "database_mutations":0,"provider_calls":0,"paid_credits":0,
        }
    evidence_path.parent.mkdir(parents=True,exist_ok=True)
    evidence_path.write_text(json.dumps(evidence,indent=2,sort_keys=True)+"\n")
    print(json.dumps(evidence,indent=2,sort_keys=True))


if __name__ == "__main__":
    main()
