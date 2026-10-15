"""Offline validation against one retained prospective daily SOG capture."""
from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path

import pandas as pd

from backend.nhl.daily_capture import verify_package
from backend.nhl.sog_structural_correction_shadow import capture
from backend.nhl.sog_feature_input import sha256_file

ROOT=Path(__file__).resolve().parents[3]


def main() -> None:
    run_id="nhldaily_20261014T192554086407Z_2b980c9e"
    receipt_path=ROOT/"artifacts/operational/nhl/daily_runs"/f"run_id={run_id}"/"parent_receipt.json"
    receipt=json.loads(receipt_path.read_text()); lanes=receipt["lanes"]
    outputs=lanes["legacy_sog"]["outputs"]
    production=next(x for x in outputs if x.get("classification")=="PROSPECTIVELY_BOUND_FEATURE_INPUT")
    feature=Path(production["feature_input_path"]); pred=Path(production["path"])
    div=next(x for x in lanes["sog_d10_d20_divergence_shadow"]["outputs"])
    div_package=Path(div["path"]); verify_package(div_package)
    before={"production_prediction_sha256":sha256_file(pred),"divergence_manifest_sha256":sha256_file(div_package/"manifest.json"),"divergence_prediction_sha256":sha256_file(div_package/"predictions.csv")}
    with tempfile.TemporaryDirectory(prefix="nhl-sog-structural-offline-") as tmp:
        result=capture(feature_path=feature,production_path=pred,output_root=Path(tmp)/"capture",run_id=run_id,
            slate_date=receipt["slate_date"],season=int(receipt["canonical_season"]),feature_sha256=production["feature_input_sha256"],
            feature_cutoff_utc=production["feature_cutoff_utc"],canonical_game_ids=receipt["canonical_game_ids"],
            started_excluded_game_ids=receipt["started_excluded_game_ids"],scorer_path=ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py")
        package=Path(result["path"]); verify_package(package); frame=pd.read_csv(package/"predictions.csv")
        counts=frame.drop_duplicates(["game_id","player_id"]).activation_state.value_counts().to_dict()
        assert result["deterministic_replay"]=="PASS"
        assert len(frame)==result["player_game_count"]*3
        assert result["threshold_coherence_crossing_count"]==0
        assert result["provider_requests"]==result["credits_consumed"]==0
        assert result["database_write_status"]=="NONE"
        assert sha256_file(pred)==before["production_prediction_sha256"]
        assert sha256_file(div_package/"manifest.json")==before["divergence_manifest_sha256"]
        assert sha256_file(div_package/"predictions.csv")==before["divergence_prediction_sha256"]
        evidence={"status":"PASS","mode":"OFFLINE_TEMPORARY_CAPTURE","slate_date":receipt["slate_date"],"parent_run_id":run_id,
          "production_model_identity":"poisson_baseline/baseline_v1","production_prediction_sha256_before_after":before["production_prediction_sha256"],
          "divergence_capture_path":str(div_package),"divergence_hashes_before_after":before,
          "feature_input_path":str(feature),"feature_input_sha256":production["feature_input_sha256"],"combined_capture_manifest":result,
          "activation_state_counts":{k:int(v) for k,v in counts.items()},"provider_requests":0,"credits_consumed":0,"production_database_writes":0,
          "receipt_lane_api":"UNIT_TESTED","live_canary":"NOT_RUN_NO_CURRENT_PREGAME_POPULATION"}
    out=ROOT/"artifacts/analysis/nhl/sog_structural_correction_shadow_activation/2026-10-14"
    out.mkdir(parents=True,exist_ok=True)
    (out/"offline_validation.json").write_text(json.dumps(evidence,indent=2,sort_keys=True)+"\n")
    (out/"implementation_summary.md").write_text("""# NHL SOG combined structural-correction shadow\n\n- Role: `PROSPECTIVE_SHADOW`; production authority remains `poisson_baseline/baseline_v1`.\n- Stage 1: when `abs(D10-D20) > 0.5`, use the 50/50 midpoint; otherwise preserve the production-selected rate.\n- Stage 2: only when D10 and the Stage 1 rate both exceed 9.0, use `9 + 0.5 * (rate - 9)`.\n- Missing D20 skips Stage 1 and is recorded; Stage 2 remains independently eligible.\n- The new lane reuses retained production features, selected rate/exposure, scorer identity, eligible games, cutoff, and exclusions. It performs no provider calls, credit use, or database writes.\n- Immutable live captures are created only by the daily lane after deterministic replay and validation. Offline validation uses a temporary capture and leaves production and divergence artifacts unchanged.\n- Grading and summary integration report combined shadow outcomes; authority and exports remain unchanged.\n- Preserved backlog: prospective grading, fixed-blend collection, historical official-TOI binding, Puck Line work tomorrow, Points sanity audit, and no Tier 3 override.\n""")
    print(out/"offline_validation.json")


if __name__=="__main__": main()
