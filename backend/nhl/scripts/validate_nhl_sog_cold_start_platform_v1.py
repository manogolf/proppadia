#!/usr/bin/env python3
"""Read-only deterministic validator for the NHL SOG cold-start V1 package."""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import pandas as pd

from backend.nhl.sog_cold_start.core import build_predictions, grade_predictions, sha256_file

ROOT=Path(__file__).resolve().parents[3]
DEFAULT=ROOT/"artifacts/analysis/model_development/nhl_sog_cold_start_prediction_platform_v1/2026-09-19"

def fixture()->pd.DataFrame:
    common={"canonical_season":2026,"slate_date":"2026-09-20","game_id":2026010001,"team_id":1,"opponent_id":2,"position":"C","roster_status":"ACTIVE_ROSTER","lineup_status":"ACTIVE_ROSTER_UNCONFIRMED","scheduled_start_time_utc":"2026-09-20T23:00:00Z","feature_cutoff_utc":"2026-09-20T18:00:00Z","position_sog_per60":7.0,"position_toi_per_game":16.0,"team_changed":False,"current_preseason_games":0,"current_preseason_sog_per60":None,"current_preseason_toi_per_game":None,"current_regular_games":0,"current_regular_sog_per60":None,"current_regular_toi_per_game":None}
    return pd.DataFrame([{**common,"player_id":10,"player_name":"Returning Player","prior_minutes":900,"prior_sog_per60":8.0,"prior_toi_per_game":18.0,"older_minutes":700,"older_sog_per60":7.5,"older_toi_per_game":17.5},{**common,"player_id":11,"player_name":"Position Prior Rookie","prior_minutes":0,"prior_sog_per60":None,"prior_toi_per_game":None,"older_minutes":0,"older_sog_per60":None,"older_toi_per_game":None},{**common,"player_id":12,"player_name":"Unknown Position","position":"UNK","prior_minutes":0,"prior_sog_per60":None,"prior_toi_per_game":None,"older_minutes":0,"older_sog_per60":None,"older_toi_per_game":None}])
def main()->int:
    ap=argparse.ArgumentParser();ap.add_argument("--package",type=Path,default=DEFAULT);a=ap.parse_args();fail=[];checks=[]
    for line in (a.package/"SHA256SUMS").read_text().splitlines():
        expected,name=line.split("  ",1);ok=(a.package/name).is_file() and sha256_file(a.package/name)==expected;checks.append({"check":f"manifest:{name}","passed":ok});fail.extend([] if ok else [name])
    f=fixture();pred,inputs,excluded=build_predictions(f,slate_date="2026-09-20",phase="MIDDAY",prediction_timestamp_utc="2026-09-20T18:01:00Z",input_cutoff_utc="2026-09-20T18:01:00Z")
    tests={"two_admitted_players":inputs.player_id.nunique()==2,"three_lines_per_arm":len(pred)==len(inputs)*3,"selected_arm_present":inputs.contract_arm.eq("D_PLAYER_ROLE_HIERARCHICAL").any(),"rookie_position_prior_not_zero":float(inputs.loc[(inputs.player_id.eq(11))&inputs.contract_arm.eq("D_PLAYER_ROLE_HIERARCHICAL"),"expected_sog"].iloc[0])>0,"unknown_position_excluded":len(excluded)==1 and excluded.exclusion_reason.iloc[0]=="CERTIFIED_SKATER_POSITION_UNAVAILABLE","market_columns_unavailable":pred.market_attachment_status.eq("UNAVAILABLE_NOT_REQUIRED").all() and pred.price.isna().all(),"unique_prediction_identity":not pred.prediction_identity.duplicated().any()}
    try:build_predictions(f.assign(slate_date="2026-09-19"),slate_date="2026-09-19",phase="MIDDAY",prediction_timestamp_utc="2026-09-19T18:01:00Z",input_cutoff_utc="2026-09-19T18:01:00Z");tests["september_19_blocked"]=False
    except RuntimeError as e:tests["september_19_blocked"]=str(e)=="SEPTEMBER_19_RETROSPECTIVE_PREDICTION_FORBIDDEN"
    outcomes=pd.DataFrame([{"canonical_season":2026,"slate_date":"2026-09-20","game_id":2026010001,"player_id":10,"official_final":True,"official_sog":3,"participation_status":"APPEARED","outcome_source":"fixture","outcome_source_timestamp_utc":"2026-09-21T02:00:00Z"},{"canonical_season":2026,"slate_date":"2026-09-20","game_id":2026010001,"player_id":11,"official_final":True,"official_sog":None,"participation_status":"LATE_SCRATCH","outcome_source":"fixture","outcome_source_timestamp_utc":"2026-09-21T02:00:00Z"}])
    graded=grade_predictions(pred,outcomes,grading_timestamp_utc="2026-09-21T03:00:00Z");tests["grading_distinguishes_late_scratch"]=graded.loc[graded.player_id.eq(11),"grading_state"].eq("LATE_SCRATCH_UNGRADED").all()
    comparison=pd.read_csv(a.package/"variant_comparison.csv");tests["two_seasons_replayed"]=set(comparison.loc[comparison.scope.eq("SEASON"),"scope_value"].astype(int))=={2024,2025};tests["identical_row_comparison_present"]=comparison.scope.eq("COMMON_ABCDG").any()
    for name,ok in tests.items():checks.append({"check":name,"passed":bool(ok)});fail.extend([] if ok else [name])
    result={"validator":"NHL_SOG_COLD_START_PLATFORM_VALIDATOR_V1","checks":checks,"passed":not fail,"failures":fail,"network_requests":0,"database_writes":0}
    print(json.dumps(result,indent=2,sort_keys=True));return 0 if not fail else 1
if __name__=="__main__":raise SystemExit(main())
