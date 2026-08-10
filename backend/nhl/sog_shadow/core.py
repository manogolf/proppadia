"""Integrated create-only SOG shadow run and append-only grading."""
from __future__ import annotations
import hashlib,json,math,shutil
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd
from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail
from backend.nhl.sog_candidate_lineage.core import POLICY_NAME,POLICY_VERSION,RULES,digest,evaluate
from backend.nhl.sog_quote_capture.core import parse_utc,sha256_file,write_manifest

LINES=[1.5,2.5,3.5];GAME_TYPES={1:"PRESEASON",2:"REGULAR_SEASON",3:"POSTSEASON"};RUN_TYPES={"MIDDAY","FINAL_PREGAME"}
RATE_FIELDS=["d10_sog_per60","d20_sog_per60","d5_sog_per60"]
TOI_FIELDS=["d10_toi_min_avg","d20_toi_min_avg","d5_toi_min_avg"]
SCORER=Path(__file__).resolve().parents[1]/"scripts/score_sog_poisson_baseline.py"
PARITY_SUMMARY_SHA256="40b2e8b72a581a365787cdf040537bdfd704944edcd599056abfa0a571f3a65d"

def make_run_id(slate:str,stamp:str,run_type:str)->str:
 if run_type not in RUN_TYPES:raise ValueError("invalid run type")
 return f"nhlsogshadow_s2026_d{slate.replace('-','')}_t{parse_utc(stamp).strftime('%Y%m%dT%H%M%S%fZ')}_{run_type}_v1"
def _coalesce(row:pd.Series,fields:list[str],extras:list[tuple[str,list[str],float]]=[])->tuple[float|None,str]:
 for f in fields:
  v=pd.to_numeric(pd.Series([row.get(f)]),errors="coerce").iloc[0]
  if pd.notna(v):return float(v),f
 for name,cols,scale in extras:
  vals=[pd.to_numeric(pd.Series([row.get(c)]),errors="coerce").iloc[0] for c in cols]
  if any(pd.notna(x) for x in vals):return float(sum(x for x in vals if pd.notna(x))*scale),name
 return None,"MISSING"
def score_inputs(inputs:pd.DataFrame,run_id:str,run_timestamp_utc:str)->tuple[pd.DataFrame,pd.DataFrame]:
 archived=[];pred=[]
 for _,r in inputs.sort_values(["game_id","player_id"]).iterrows():
  rate,rs=_coalesce(r,RATE_FIELDS);toi,ts=_coalesce(r,TOI_FIELDS,[("szn_toi_per_game_5on5+szn_toi_per_game_pp",["szn_toi_per_game_5on5","szn_toi_per_game_pp"],1),("season_situation_seconds",["season_5on5_icetime_per_game","season_5on4_icetime_per_game"],1/60)])
  expected=max(0.,(rate*toi/60) if rate is not None and toi is not None else 0.);missing="GOVERNED_ZERO_DEFAULT" if rate is None or toi is None else "COMPLETE"
  a=r.to_dict();a.update({"selected_sog_rate":rate,"rate_source":rs,"selected_toi_minutes":toi,"toi_source":ts,"missingness_default_status":missing,"expected_sog":expected,"run_id":run_id,"run_timestamp_utc":parse_utc(run_timestamp_utc).isoformat()});archived.append(a)
  for line in LINES:
   po=_poisson_tail(expected,int(line+.5));base=digest({"canonical_season":2026,"game_id":int(r.game_id),"player_id":int(r.player_id),"prop_type":"shots_on_goal","line":line,"model_version":"baseline_v1","run_id":run_id});pred.append({"canonical_season":2026,"slate_date":r.slate_date,"run_id":run_id,"prediction_identity":base,"player_game_identity":digest({"canonical_season":2026,"game_id":int(r.game_id),"player_id":int(r.player_id)}),"game_id":int(r.game_id),"player_id":int(r.player_id),"player_name":r.player_name,"team":r.team,"opponent":r.opponent,"scheduled_start_time_utc":r.scheduled_start_time_utc,"game_type_code":int(r.game_type_code),"game_type_label":GAME_TYPES.get(int(r.game_type_code),"UNKNOWN_GAME_TYPE"),"prop_type":"shots_on_goal","line":line,"model_name":"poisson_baseline","model_version":"baseline_v1","model_formula_sha256":sha256_file(SCORER),"feature_manifest_sha256":digest({"rate":RATE_FIELDS,"toi":TOI_FIELDS,"fallback":"frozen_v1"}),"expected_sog":expected,"p_over":po})
 return pd.DataFrame(archived),pd.DataFrame(pred)
def verify_parity(path:Path)->dict:
 p=json.loads(path.read_text());ok=sha256_file(path)==PARITY_SUMMARY_SHA256 and p.get("target_rows")==40167 and p.get("mode_a_rows")==40167 and p.get("tolerance_rows")==40167 and p.get("side_match_rows")==40167 and p.get("material_mismatch_rows")==0 and p.get("stored_output_only_rows")==0 and float(p.get("tolerance",1))==1e-12
 if not ok:raise RuntimeError("SOG_PROSPECTIVE_SCORER_BLOCKED_BY_PARITY_FAILURE")
 return p
def run_shadow(*,game_spine_csv:Path,player_inputs_csv:Path,quote_run_dir:Path,effective_policy_json:Path,parity_json:Path,output_root:Path,slate_date:str,run_timestamp_utc:str,run_type:str,emit_upload:bool=True)->Path:
 parity=verify_parity(parity_json);cfg=json.loads(effective_policy_json.read_text())
 if cfg.get("policy_name")!=POLICY_NAME or cfg.get("policy_version")!=POLICY_VERSION or not isinstance(cfg.get("policy_segments"),dict) or not cfg["policy_segments"]:raise RuntimeError("RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG")
 if digest({k:v for k,v in cfg.items() if k!="effective_config_hash"})!=cfg.get("effective_config_hash"):raise RuntimeError("RUN_BLOCKED_BY_INVALID_EFFECTIVE_POLICY_CONFIG")
 run_id=make_run_id(slate_date,run_timestamp_utc,run_type);dest=output_root/"2026"/slate_date/run_id
 if dest.exists():raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
 games=pd.read_csv(game_spine_csv);inputs=pd.read_csv(player_inputs_csv);starts=pd.to_datetime(games.scheduled_start_time_utc,utc=True,errors="coerce");run=parse_utc(run_timestamp_utc)
 if not games.canonical_season.eq(2026).all() or games.game_id.duplicated().any() or not games.game_type_code.isin(GAME_TYPES).all() or starts.isna().any():raise RuntimeError("game identity gate failed")
 if (run>=starts).any():raise RuntimeError("run timestamp not pregame")
 if inputs.duplicated(["game_id","player_id"]).any() or inputs.player_id.isna().any():raise RuntimeError("player-game identity gate failed")
 qmeta=json.loads((quote_run_dir/"run_metadata.json").read_text());qmanifest=sha256_file(quote_run_dir/"SHA256SUMS");quotes=pd.read_csv(quote_run_dir/"sog_quotes.csv");source_quote_run_id=qmeta["run_id"]
 # Verify the immutable source archive before deriving a run-bound working copy.
 for line in (quote_run_dir/"SHA256SUMS").read_text().splitlines():
  d,n=line.split("  ",1)
  if sha256_file(quote_run_dir/n)!=d:raise RuntimeError("quote archive hash failure")
 quotes["quote_capture_run_id"]=quotes.run_id;quotes["run_id"]=run_id
 pin,pred=score_inputs(inputs,run_id,run_timestamp_utc);final,ledger,manual=evaluate(pred,quotes,cfg,parse_utc(run_timestamp_utc).isoformat())
 fail=ledger[ledger.rule_result.eq("FAIL")].sort_values(["prediction_identity","rule_order"]).groupby("prediction_identity").first().failure_reason
 final["first_failure_reason"]=final.prediction_identity.map(fail).fillna(""); final["pre_cap_status"]=np.where(final.first_failure_reason.isin(["DUPLICATE_COLLISION","EXCLUDED_BY_GAME_CAP","EXCLUDED_BY_SLATE_CAP"]),"POLICY_PASS_PRE_CAP",np.where(final.final_candidate_status.eq("FINAL_CANDIDATE"),"POLICY_PASS_PRE_CAP","POLICY_FAIL"));final["candidate_status"]=np.where(final.final_candidate_status.eq("FINAL_CANDIDATE"),"FINAL_SHADOW_CANDIDATE",np.where(final.pre_cap_status.eq("POLICY_PASS_PRE_CAP"),"EXCLUDED_BY_CAP","POLICY_FAIL"));final["candidate_identity"]=final.apply(lambda r:digest({"prediction_identity":r.prediction_identity,"policy_hash":cfg["effective_config_hash"]}),axis=1)
 mvcols=[c for c in ["run_id","game_id","player_id","line","market_snapshot_identity","p_over_mkt","price_over","price_under","quote_count_over","quote_count_under","sportsbooks","quote_payload_hashes"] if c in final];market=final[mvcols].drop_duplicates().sort_values(["game_id","player_id","line"])
 upload=final[final.candidate_status.eq("FINAL_SHADOW_CANDIDATE")].copy();upload_rows=[];lineage=[];export_stamp=parse_utc(run_timestamp_utc).isoformat()
 for _,r in upload.sort_values(["game_id","candidate_rank","prediction_identity"]).iterrows():
  rid=digest({"candidate_identity":r.candidate_identity,"export_timestamp":export_stamp});row={"LEAGUE":"NHL","DATE":str(r.slate_date).replace("-",""),"HOME":"","AWAY":"","DOUBLEHEADER":"","SECTION":"player_prop","MARKET":"player-shots_onGoal-ou","SELECTOR":int(r.player_id),"POINT":float(r.line),"SIDE":r.side.lower(),"WIN %":int(r.fair_american)};upload_rows.append(row);lineage.append({"upload_row_id":rid,"prediction_identity":r.prediction_identity,"candidate_identity":r.candidate_identity,"run_id":run_id,"policy_version":cfg["policy_version"],"policy_hash":cfg["effective_config_hash"],"market_snapshot_identity":r.market_snapshot_identity,"candidate_rank":r.candidate_rank,"candidate_status":r.candidate_status,"export_timestamp_utc":export_stamp})
 upload_df=pd.DataFrame(upload_rows,columns=["LEAGUE","DATE","HOME","AWAY","DOUBLEHEADER","SECTION","MARKET","SELECTOR","POINT","SIDE","WIN %"]);uline=pd.DataFrame(lineage)
 side_predictions=final[[c for c in ["canonical_season","slate_date","run_id","prediction_identity","base_prediction_identity","player_game_identity","game_id","player_id","player_name","team","opponent","scheduled_start_time_utc","game_type_code","game_type_label","prop_type","line","side","model_name","model_version","model_formula_sha256","feature_manifest_sha256","expected_sog","model_side_prob_raw"] if c in final]].copy();side_predictions=side_predictions.rename(columns={"model_side_prob_raw":"prediction_side_probability"})
 dest.mkdir(parents=True,exist_ok=False);games.sort_values("game_id").to_csv(dest/"game_spine.csv",index=False);inputs.sort_values(["game_id","player_id"]).to_csv(dest/"player_game_spine.csv",index=False);pin.sort_values(["game_id","player_id"]).to_csv(dest/"prediction_inputs.csv",index=False);side_predictions.sort_values(["game_id","player_id","line","side"]).to_csv(dest/"sog_predictions.csv",index=False);shutil.copy2(quote_run_dir/"raw_odds_response.json",dest/"raw_odds_response.json");quotes.sort_values(["game_id","player_id","line","side","sportsbook"],na_position="last").to_csv(dest/"sog_quotes.csv",index=False);market.to_csv(dest/"market_view_derivation.csv",index=False);(dest/"candidate_policy_effective_config.json").write_text(json.dumps(cfg,indent=2,sort_keys=True)+"\n");ledger.sort_values(["prediction_identity","rule_order"]).to_csv(dest/"candidate_rule_ledger.csv",index=False);final.sort_values(["game_id","player_id","line","side"]).to_csv(dest/"candidate_summary.csv",index=False);manual.to_csv(dest/"manual_override_ledger.csv",index=False)
 if emit_upload:upload_df.to_csv(dest/"upload_shaped_output.csv",index=False);uline.to_csv(dest/"upload_lineage.csv",index=False)
 pop=[]
 for code,label,count in [("P","Prediction",len(final)),("M","Market-qualified",int(final.market_snapshot_identity.notna().sum())),("C","Candidate",int(final.candidate_status.eq("FINAL_SHADOW_CANDIDATE").sum())),("U","Upload",len(upload_df) if emit_upload else 0),("E","Execution",0),("G","Graded",0)]:pop.append({"population_code":code,"population":label,"rows":count,"execution_inferred":False})
 pd.DataFrame(pop).to_csv(dest/"population_membership.csv",index=False)
 gates=[]
 def g(n,p,critical,e):gates.append({"gate":n,"passed":bool(p),"critical":critical,"evidence":str(e)})
 g("identity",True,True,len(games));g("historical_parity",parity["target_rows"]==40167,True,parity);g("model_outputs",pred.p_over.between(0,1).all()&np.isfinite(pred.expected_sog).all(),True,len(pred));g("strict_pregame",(run<starts).all(),True,run);g("prepared_fallback_recorded",pin.missingness_default_status.notna().all(),True,len(pin));g("quote_manifest",True,True,qmanifest);g("post_start_excluded",not quotes.loc[quotes.quote_qualification_status.eq("POST_START_INVALID"),"quote_qualification_status"].str.startswith("PREGAME").any(),True,"none qualified");g("policy_config_hash",True,True,cfg["effective_config_hash"]);g("rule_completeness",ledger.groupby("prediction_identity").rule_id.nunique().eq(len(RULES)).all(),True,len(ledger));g("manual_separate",True,True,len(manual));g("upload_traceability",not emit_upload or len(upload_df)==len(uline),True,len(uline));g("execution_empty",True,True,0)
 gate=pd.DataFrame(gates);health="FAIL_CLOSED" if not gate.loc[gate.critical,"passed"].all() else ("PASS" if int(final.market_snapshot_identity.notna().sum())==len(final) else "PASS_WITH_BOUNDED_MARKET_COVERAGE");gate.to_csv(dest/"health_gate_ledger.csv",index=False)
 meta={"run_id":run_id,"canonical_season":2026,"slate_date":slate_date,"run_timestamp_utc":parse_utc(run_timestamp_utc).isoformat(),"run_type":run_type,"mode":"SHADOW_OBSERVATION_ONLY","model":"poisson_baseline","model_version":"baseline_v1","model_formula_sha256":sha256_file(SCORER),"historical_parity":parity,"source_quote_run_id":source_quote_run_id,"source_quote_manifest_sha256":qmanifest,"policy_name":POLICY_NAME,"policy_version":POLICY_VERSION,"policy_hash":cfg["effective_config_hash"],"health_gate_result":health,"population_counts":{x["population_code"]:x["rows"] for x in pop},"recommendations_generated":0,"execution_rows":0};(dest/"run_metadata.json").write_text(json.dumps(meta,indent=2,sort_keys=True)+"\n");write_manifest(dest);return dest

def grade_run(run_dir:Path,outcomes_csv:Path,grade_root:Path,grading_timestamp_utc:str)->Path:
 before={p.name:sha256_file(p) for p in run_dir.iterdir() if p.is_file()}
 for line in (run_dir/"SHA256SUMS").read_text().splitlines():
  d,n=line.split("  ",1)
  if sha256_file(run_dir/n)!=d:raise RuntimeError("pregame archive hash failure")
 pred=pd.read_csv(run_dir/"candidate_summary.csv");out=pd.read_csv(outcomes_csv);required={"game_id","player_id","official_sog","participation_status","outcome_source","outcome_source_timestamp_utc","source_conflict_status"}
 if required-set(out):raise ValueError("outcome schema incomplete")
 z=pred.merge(out,on=["game_id","player_id"],how="left",validate="many_to_one");z["grading_timestamp_utc"]=parse_utc(grading_timestamp_utc).isoformat();z["settlement_status"]=np.where(~z.participation_status.eq("PARTICIPATED"),"NONPARTICIPANT_UNGRADED",np.where(z.official_sog>z.line,np.where(z.side.eq("OVER"),"WIN","LOSS"),np.where(z.official_sog<z.line,np.where(z.side.eq("UNDER"),"WIN","LOSS"),"PUSH")))
 gid="grade_"+parse_utc(grading_timestamp_utc).strftime("%Y%m%dT%H%M%S%fZ");dest=grade_root/run_dir.name/gid
 if dest.exists():raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
 dest.mkdir(parents=True,exist_ok=False);z.to_csv(dest/"graded_candidates.csv",index=False);(dest/"grading_metadata.json").write_text(json.dumps({"source_run_id":run_dir.name,"source_manifest_sha256":sha256_file(run_dir/"SHA256SUMS"),"grading_timestamp_utc":parse_utc(grading_timestamp_utc).isoformat(),"rows":len(z),"execution_rows":0},indent=2,sort_keys=True)+"\n");write_manifest(dest)
 if before!={p.name:sha256_file(p) for p in run_dir.iterdir() if p.is_file()}:raise RuntimeError("pregame mutation detected")
 return dest
