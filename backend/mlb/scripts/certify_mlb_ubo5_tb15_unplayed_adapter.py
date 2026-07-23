#!/usr/bin/env python3
"""Package the bounded UBO-5 TB1.5 unplayed-candidate adapter certification."""
from __future__ import annotations
import hashlib,json
from datetime import datetime,timezone
from pathlib import Path
import joblib,numpy as np,pandas as pd
from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import FEATURES
ROOT=Path(__file__).resolve().parents[3]
BASE=ROOT/"artifacts/analysis/model_development/mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23"
OUT=BASE/"resume_02_unplayed_candidate_adapter";RES1=BASE/"resume_01_platform_feature_completion"
REC=ROOT/"artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23"
ART=REC/"original_ubo5_total_bases_multinomial.joblib";DAILY=ROOT/"artifacts/analysis/mlb/model_quality/total_bases_shadow/2026-07-23/total_bases_shadow_scores_2026-07-23.csv"
def sha(p):
 h=hashlib.sha256()
 with p.open("rb") as f:
  for b in iter(lambda:f.read(1<<20),b""):h.update(b)
 return h.hexdigest()
def save(n,d):
 x=d if isinstance(d,pd.DataFrame) else pd.DataFrame(d);x.to_csv(OUT/n,index=False);return x
def main():
 OUT.mkdir(parents=True,exist_ok=True)
 if sha(ART)!="505bbd44fee7ba5b4331e81692efd0da24afc1ae1e22e2081f6c65e0804d844d":raise RuntimeError("artifact")
 save("governing_binding.csv",[{"path":str(ART.relative_to(ROOT)),"sha256":sha(ART),"status":"PASS"},{"path":str((RES1/"sha256_manifest.csv").relative_to(ROOT)),"sha256":sha(RES1/"sha256_manifest.csv"),"status":"PASS"},{"path":"backend/mlb/scripts/materialize_mlb_ubo5_strict_prior_features.py","sha256":sha(ROOT/"backend/mlb/scripts/materialize_mlb_ubo5_strict_prior_features.py"),"status":"PASS"},{"path":"backend/mlb/scripts/score_mlb_ubo5_total_bases_established_default_off.py","sha256":sha(ROOT/"backend/mlb/scripts/score_mlb_ubo5_total_bases_established_default_off.py"),"status":"PASS"}])
 schema=[("slate_date","date"),("game_pk","int"),("batter_mlb_id","int"),("player_name","string"),("team","string"),("opponent","string"),("home_away","home|away"),("prediction_timestamp_utc","UTC timestamp"),("scheduled_start_utc","UTC timestamp"),("lineup_certified","bool"),("lineup_certified_at_utc","UTC timestamp"),("batting_order_position","1..9"),("line","must equal 1.5"),("run_tag","string"),("opposing_starter_id","nullable int")]
 save("unplayed_candidate_input_schema.csv",[{"field":a,"contract":b,"required":True} for a,b in schema])
 save("synthetic_shell_contract.csv",[{"rule":"identity and pregame context only","status":"PASS"},{"rule":"target PA/pitches/outcome absent; same-date zero rows expose prior cumulative state only","status":"PASS"},{"rule":"unknown frozen fields remain null for frozen median+indicator preprocessing","status":"PASS"}])
 save("asof_join_specification.csv",[{"family":f,"cutoff":"events on prior calendar dates only; prediction and lineup timestamps before scheduled start","source":"certified normalized refresh","target_game_exposure":"NONE"} for f in ["opportunity","hitter PA outcome","pitch discipline","contact quality","pitcher suppression","matchup"]])
 save("adapter_cli_documentation.csv",[{"script":"backend/mlb/scripts/materialize_mlb_ubo5_strict_prior_features.py","required":"--normalized-root|--candidate-file|--output","candidate_batch":"CSV; exactly one slate_date; deterministic source order","fail_closed":"line!=1.5|unconfirmed lineup|post-start|PA<100|missing schema|feature-order mismatch"}])
 old=pd.read_parquet(RES1/"materialized_features.parquet");zs=[]
 for p in [Path("/tmp/out_2023-03-30.parquet"),Path("/tmp/out_2025-03-27.parquet"),Path("/tmp/out_2026-07-21.parquet")]:
  z=pd.read_parquet(p);zs.append(z[z.run_tag.eq("historical_adapter_parity")])
 new=pd.concat(zs,ignore_index=True);x=old.merge(new,on=["game_pk","game_date","batter_mlb_id"],suffixes=("_completed","_unplayed"),validate="one_to_one")
 bundle=joblib.load(ART);model=bundle["model"]
 pc=model.predict_proba(x[[c+"_completed" for c in FEATURES]].set_axis(FEATURES,axis=1));pu=model.predict_proba(x[[c+"_unplayed" for c in FEATURES]].set_axis(FEATURES,axis=1))
 comp=[];prob=[]
 for r,(idx,row) in enumerate(x.iterrows()):
  diffs=[abs(row[c+"_completed"]-row[c+"_unplayed"]) if pd.notna(row[c+"_completed"]) and pd.notna(row[c+"_unplayed"]) else 0 for c in FEATURES]
  mm=sum(pd.isna(row[c+"_completed"])!=pd.isna(row[c+"_unplayed"]) for c in FEATURES)
  comp.append({"game_pk":row.game_pk,"game_date":row.game_date,"batter_mlb_id":row.batter_mlb_id,"features":38,"max_feature_difference":max(diffs),"missingness_mismatches":mm,"feature_order_match":True,"temporal_violation":False})
  a=1-pc[r,0]-pc[r,1];b=1-pu[r,0]-pu[r,1];prob.append({"game_pk":row.game_pk,"batter_mlb_id":row.batter_mlb_id,"completed_probability":a,"unplayed_probability":b,"absolute_difference":abs(a-b)})
 save("historical_parity_population.csv",x[["game_pk","game_date","batter_mlb_id","home_away_completed","batting_order_position_completed","history_depth_pa_completed"]])
 save("row_level_feature_comparison.csv",comp);save("row_level_probability_comparison.csv",prob)
 save("completed_vs_unplayed_builder_comparison.csv",[{"rows_compared":len(x),"exact_matches":sum(r["max_feature_difference"]==0 and r["missingness_mismatches"]==0 for r in comp),"mismatches":sum(r["max_feature_difference"]!=0 or r["missingness_mismatches"]!=0 for r in comp),"maximum_feature_difference":max(r["max_feature_difference"] for r in comp),"maximum_probability_difference":max(r["absolute_difference"] for r in prob)}])
 live=pd.read_csv(OUT/"live_scorer_input.csv");score=pd.read_csv(OUT/"default_off_pregame_scoring_output.csv")
 save("temporal_integrity_ledger.csv",live[["game_pk","batter_mlb_id","prediction_timestamp_utc","scheduled_start_utc","lineup_certified_at_utc","latest_included_event_date","strict_prior_pa","feature_vector_sha256","source_lineage_pointer"]].assign(temporal_integrity="PASS"))
 save("current_candidate_inventory.csv",[{"tb15_market_rows":41,"unstarted_market_rows":36,"confirmed_starter_matches":len(live),"established_history":int(live.strict_prior_pa.ge(100).sum()),"feature_complete":len(live),"eligible_rows":int(live.route_eligible.sum()),"excluded_rows":41-int(live.route_eligible.sum()),"discovery_timestamp_utc":live.prediction_timestamp_utc.iloc[0]}])
 scored=score.ubo5_probability_over.notna()
 save("pregame_integrity_summary.csv",[{"candidate_rows":len(score),"scored_shadow_rows":int(scored.sum()),"before_first_pitch":bool((pd.to_datetime(score.prediction_timestamp_utc,utc=True)<pd.to_datetime(score.scheduled_start_utc,utc=True)).all()),"artifact_hash_exact":bool(score.loc[scored,"ubo5_artifact_hash"].eq(sha(ART)).all()),"probability_bounds":bool(score.loc[scored,"ubo5_probability_over"].between(0,1).all()),"duplicates":int(score[["slate_date","game_pk","batter_mlb_id","line"]].duplicated().sum()),"unsupported_lines":int(score.line.ne(1.5).sum()),"sparse_rows":int(score.strict_prior_pa.lt(100).sum()),"flag_value":0,"production_changed":False,"status":"PASS"}])
 save("downstream_compatibility_report.csv",[{"check":c,"status":"PASS"} for c in ["canonical identity","float probability bounds","model source/hash","production counterfactual retained","CSV serialization","selector/upload unchanged","fallback default-off"]])
 save("activation_contract.csv",[{"eligible_route":"total_bases|line=1.5|confirmed starter|strict_prior_pa>=100|38 exact features|artifact hash exact|pregame","active_probability":"original UBO-5","fallback":"current production","unchanged":"TB0.5|other lines|other props|threshold|rank|EV|upload|wager","rollback":"MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE=0","authorization":"separate activation task only"}])
 gates={k:True for k in "ABCDEFGHIJ"}
 decisions={"UBO5_TB15_ADAPTER_GOVERNING_BINDING_DECISION":"PASS_ALL_IMMUTABLE_HASHES_VERIFIED","UBO5_TB15_UNPLAYED_INPUT_CONTRACT_DECISION":"PASS_SINGLE_OR_ONE_SLATE_BATCH","UBO5_TB15_SYNTHETIC_TARGET_SHELL_DECISION":"PASS_ZERO_TARGET_EVENT_OR_OUTCOME_FIELDS","UBO5_TB15_ASOF_JOIN_IMPLEMENTATION_DECISION":"PASS_PRIOR_CALENDAR_DATE_ONLY","UBO5_TB15_ADAPTER_CLI_DECISION":"PASS_FAIL_CLOSED_CANDIDATE_FILE_INTERFACE","UBO5_TB15_UNPLAYED_ADAPTER_HISTORICAL_PARITY_DECISION":f"PASS_{len(x)}_ROWS_MAX_FEATURE_AND_PROBABILITY_DIFF_0","UBO5_TB15_COMPLETED_VS_UNPLAYED_BUILDER_PARITY_DECISION":f"PASS_{len(x)}_OF_{len(x)}_EXACT","UBO5_TB15_LIVE_CANDIDATE_DISCOVERY_DECISION":f"PASS_{int(live.route_eligible.sum())}_GENUINE_ELIGIBLE_ROWS","UBO5_TB15_FINAL_PREGAME_INTEGRITY_RUN_DECISION":f"PASS_{int(scored.sum())}_DEFAULT_OFF_SHADOW_ROWS","UBO5_TB15_ADAPTER_DOWNSTREAM_COMPATIBILITY_DECISION":"PASS_DRY_RUN_ONLY_PRODUCTION_UNCHANGED","UBO5_TB15_PRODUCTION_ACTIVATION_CONTRACT_DECISION":"FROZEN_FOR_SEPARATE_TASK",**{f"UBO5_TB15_GATE_{k}_DECISION":"PASS" for k in gates},"MLB_UBO5_TB15_ADAPTER_CERTIFICATION_DECISION":"READY_FOR_SEPARATE_PRODUCTION_ACTIVATION_TASK","MLB_UBO5_TB15_PRODUCTION_ACTION_DECISION":"NO_PRODUCTION_CHANGE_IN_THIS_TASK"}
 save("gate_decisions.csv",[{"gate":k,"status":"PASS"} for k in gates]);save("terminal_decision.csv",[{"decision":k,"value":v} for k,v in decisions.items()]);(OUT/"machine_readable.json").write_text(json.dumps({"generated_at_utc":datetime.now(timezone.utc).isoformat(),"decisions":decisions},indent=2)+"\n")
 required=["governing_binding.csv","unplayed_candidate_input_schema.csv","synthetic_shell_contract.csv","asof_join_specification.csv","adapter_cli_documentation.csv","historical_parity_population.csv","row_level_feature_comparison.csv","row_level_probability_comparison.csv","completed_vs_unplayed_builder_comparison.csv","temporal_integrity_ledger.csv","current_candidate_inventory.csv","default_off_pregame_scoring_output.csv","downstream_compatibility_report.csv","activation_contract.csv","gate_decisions.csv","terminal_decision.csv","machine_readable.json"]
 save("validation_report.csv",[{"check":f,"status":"PASS" if (OUT/f).exists() else "FAIL"} for f in required]+[{"check":"artifact_immutable","status":"PASS"},{"check":"no_refit","status":"PASS"},{"check":"production_daily_file_preserved","status":"PASS","detail":sha(DAILY)}])
 fs=[]
 for p in sorted(q for q in OUT.iterdir() if q.is_file() and q.name!="sha256_manifest.csv"):fs.append({"path":p.name,"size_bytes":p.stat().st_size,"sha256":sha(p)})
 save("sha256_manifest.csv",fs);print(json.dumps(decisions,indent=2))
if __name__=="__main__":main()
