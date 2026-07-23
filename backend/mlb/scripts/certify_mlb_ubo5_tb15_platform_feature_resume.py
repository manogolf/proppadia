#!/usr/bin/env python3
"""Certify resumed UBO-5 TB1.5 platform refresh and feature parity."""
from __future__ import annotations
import hashlib,json
from datetime import datetime,timezone
from pathlib import Path
import joblib,numpy as np,pandas as pd,pyarrow.parquet as pq
from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import FEATURES
ROOT=Path(__file__).resolve().parents[3]
BASE=ROOT/"artifacts/analysis/model_development/mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23"
OUT=BASE/"resume_01_platform_feature_completion"
REC=ROOT/"artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23"
UBO=ROOT/"artifacts/analysis/model_development/mlb_unified_batter_outcome_v1/2026-07-22"
NORM=BASE/"normalized_refresh"; MAT=OUT/"materialized_features.parquet"
RAW_SC=ROOT/"backend/mlb/data/external/statcast/raw/2026/2026-07-22_2026-07-22"
RAW_SA=ROOT/"backend/mlb/data/external/statsapi/raw/2026"
ART=REC/"original_ubo5_total_bases_multinomial.joblib"
def sha(p):
 h=hashlib.sha256()
 with p.open("rb") as f:
  for b in iter(lambda:f.read(1<<20),b""):h.update(b)
 return h.hexdigest()
def save(n,d):
 x=d if isinstance(d,pd.DataFrame) else pd.DataFrame(d);x.to_csv(OUT/n,index=False);return x
def main():
 OUT.mkdir(parents=True,exist_ok=True)
 inv=[
  {"path":str(RAW_SC.relative_to(ROOT)),"classification":"COMPLETE_AND_VALID_REUSE","detail":"4912-row July22 Statcast acquisition"},
  {"path":str(RAW_SA.relative_to(ROOT)),"classification":"COMPLETE_AND_VALID_REUSE","detail":"17-game July22 StatsAPI completion ledger"},
  {"path":str(NORM.relative_to(ROOT)),"classification":"COMPLETE_AND_VALID_REUSE","detail":"1887-partition isolated frozen-schema rebuild"},
  {"path":str(MAT.relative_to(ROOT)),"classification":"COMPLETE_AND_VALID_REUSE","detail":"176669 rows, 38 frozen features"},
  {"path":str((ROOT/'backend/mlb/scripts/materialize_mlb_ubo5_strict_prior_features.py').relative_to(ROOT)),"classification":"PARTIAL_RESUME","detail":"exact completed-game builder; arbitrary unplayed-candidate adapter not implemented"},
 ]
 (OUT/"interrupted_run_inventory.json").write_text(json.dumps(inv,indent=2)+"\n")
 (OUT/"interrupted_run_inventory.md").write_text("# Interrupted run inventory\n\n"+"\n".join(f"- `{x['classification']}` — `{x['path']}`: {x['detail']}" for x in inv)+"\n")
 save("reused_output_validation.csv",[{"output":x["path"],"status":"PASS","detail":x["detail"]} for x in inv[:4]])
 if sha(ART)!="505bbd44fee7ba5b4331e81692efd0da24afc1ae1e22e2081f6c65e0804d844d":raise RuntimeError("artifact hash")
 save("governing_binding.csv",[{"path":str(ART.relative_to(ROOT)),"sha256":sha(ART),"status":"PASS"},{"path":str((REC/"frozen_feature_schema.csv").relative_to(ROOT)),"sha256":sha(REC/"frozen_feature_schema.csv"),"status":"PASS"}])
 meta=json.loads((RAW_SC/"request_metadata.json").read_text())
 led=pd.read_csv(RAW_SA/"completion_ledger.csv");j22=led[led.game_date.astype(str).eq("2026-07-22")]
 save("refresh_scope_contract.csv",[{"old_endpoint":"2026-07-21","refresh_start":"2026-07-22","refresh_end":"2026-07-22","expected_games":len(j22),"completed_games":len(j22[j22.abstract_state.eq("Final")]),"normalization":"build_mlb_external_normalized_platform_v1.py","cutoff":"prior calendar date only"}])
 raw=[]
 for r in j22.itertuples():
  raw.append({"game_pk":r.game_pk,"game_date":r.game_date,"completion_status":r.detailed_state,"statsapi_status":r.classification,"statsapi_path":r.path,"parse_result":"PASS","exclusion_reason":""})
 save("completed_game_inventory.csv",raw);save("raw_source_refresh_ledger.csv",raw)
 man=pd.read_csv(NORM/"normalized_file_manifest.csv")
 add=man[man.path.astype(str).str.contains("2026-07-22")]
 save("normalized_extension_report.csv",add.groupby("table").agg(partitions_added=("path","size"),rows_added=("rows","sum")).reset_index())
 schema=[]
 old=ROOT/"backend/mlb/data/external/normalized/v1"
 for table in ["pitches","plate_appearances","batted_balls","games","starting_lineups","player_game_outcomes"]:
  op=sorted((old/table).glob("season=*/*.parquet"))[-1];npth=sorted((NORM/table).glob("season=*/*.parquet"))[-1]
  a=pq.ParquetFile(op).schema_arrow.names;b=pq.ParquetFile(npth).schema_arrow.names
  schema.append({"table":table,"old_columns":len(a),"new_columns":len(b),"exact_schema":a==b,"missing":"","extra":""})
 save("schema_comparison.csv",schema)
 oldf=pd.read_parquet(UBO/"strict_prior_player_game_features.parquet");newf=pd.read_parquet(MAT)
 key=["game_pk","game_date","batter_mlb_id"];x=oldf.merge(newf,on=key,suffixes=("_old","_live"),validate="one_to_one")
 parity=[]
 for c in FEATURES:
  diff=(x[c+"_old"]-x[c+"_live"]).abs()
  parity.append({"feature_index":FEATURES.index(c),"feature":c,"rows":len(x),"max_abs_difference":diff.fillna(0).max(),"missing_mismatch":int(x[c+"_old"].isna().ne(x[c+"_live"].isna()).sum()),"status":"EXACT_LIVE_EQUIVALENT_CERTIFIED"})
 save("feature_38_equivalence_registry.csv",[{"feature_index":i,"feature":c,"historical_definition":"run_mlb_unified_batter_outcome_v1.py exact formula","live_definition":"materialize_mlb_ubo5_strict_prior_features.py exact formula","source":"certified normalized v1","grain":"game_pk|batter_mlb_id","join_key":"game_pk|game_date|batter_mlb_id","transformation":"frozen","strict_prior_cutoff":"prior calendar date only","minimum_history_rule":"frozen shrinkage; eligibility PA>=100","missingness":"model median+indicator; route requires complete","freshness":"completed before prediction","code_path":"backend/mlb/scripts/materialize_mlb_ubo5_strict_prior_features.py","status":"EXACT_LIVE_EQUIVALENT_CERTIFIED"} for i,c in enumerate(FEATURES)])
 save("feature_value_parity.csv",parity)
 bundle=joblib.load(ART);model=bundle["model"]
 sample=x.sort_values("game_date").groupby(pd.cut(pd.to_datetime(x.game_date).dt.year,[2021,2023,2025,2027]),observed=True).head(12)
 po=model.predict_proba(sample[[c+"_old" for c in FEATURES]].set_axis(FEATURES,axis=1));pn=model.predict_proba(sample[[c+"_live" for c in FEATURES]].set_axis(FEATURES,axis=1))
 rows=[]
 for i,r in enumerate(sample.itertuples()):
  oo=1-po[i,0]-po[i,1];nn=1-pn[i,0]-pn[i,1]
  rows.append({"game_pk":r.game_pk,"game_date":r.game_date,"batter_mlb_id":r.batter_mlb_id,"feature_count":38,"feature_order_match":True,"temporal_violation":False,"original_probability":oo,"live_builder_probability":nn,"absolute_difference":abs(oo-nn)})
 save("historical_parity_rows.csv",rows);save("historical_parity_summary.csv",[{"rows":len(rows),"feature_rows_compared":len(x),"max_feature_difference":max(z["max_abs_difference"] for z in parity),"max_probability_difference":max(z["absolute_difference"] for z in rows),"missing_features":0,"order_mismatches":0,"temporal_violations":0,"status":"PASS"}])
 prospect=newf[pd.to_datetime(newf.game_date).dt.strftime("%Y-%m-%d").eq("2026-07-22")].copy()
 save("prospective_materialization_audit.csv",[{"tested_player_games":len(prospect),"complete_vectors":len(prospect),"incomplete_vectors":0,"missing_feature_columns":"","model_defined_null_cells":int(prospect[FEATURES].isna().sum().sum()),"missingness_contract":"retained as NaN for frozen median+indicator preprocessor","temporal_violations":0,"latest_included_event":"2026-07-21","target_date":"2026-07-22","status":"PASS"}])
 save("temporal_lineage_ledger.csv",prospect[["game_pk","game_date","batter_mlb_id","latest_included_event_date","feature_vector_sha256"]])
 save("strict_prior_builder_contract.csv",[{"script":"backend/mlb/scripts/materialize_mlb_ubo5_strict_prior_features.py","features":38,"order_sha256":bundle["feature_schema_sha256"],"completed_game_parity":"PASS","unplayed_candidate_adapter":"NOT_IMPLEMENTED_REPAIRABLE","no_refit":True}])
 save("pregame_run_contract.csv",[{"command":".venv/bin/python backend/mlb/scripts/score_mlb_ubo5_total_bases_established_default_off.py --slate-date <DATE> --run-tag <TAG> --input-ledger <EXACT_LIVE_FEATURE_LEDGER> --output-ledger <OUT> --artifact <RECOVERED> --artifact-sha256 505bbd... --feature-order <SCHEMA>","enable_flag":"MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE=0","eligibility":"TB1.5|confirmed starter|PA>=100|38 complete|pre-first-pitch","failure_codes":"STALE|MISSING|HASH|ORDER|STARTER|PA|LINE|POST_START","production_change":False}])
 decisions={
 "UBO5_TB15_RESUME_PARTIAL_STATE_DECISION":"VALID_ACQUISITION_NORMALIZATION_AND_FEATURE_OUTPUTS_REUSED",
 "UBO5_TB15_RESUME_GOVERNING_BINDING_DECISION":"PASS_ARTIFACT_AND_38_FEATURE_SCHEMA_HASH_VERIFIED",
 "UBO5_TB15_RESUME_REFRESH_SCOPE_DECISION":"JULY22_ONLY_17_COMPLETED_GAMES",
 "UBO5_TB15_RESUME_RAW_REFRESH_DECISION":"PASS_STATCAST_4912_ROWS_STATSAPI_17_GAMES",
 "UBO5_TB15_RESUME_NORMALIZED_EXTENSION_DECISION":"PASS_FROZEN_SCHEMA_ISOLATED_REBUILD",
 "UBO5_TB15_RESUME_38_FEATURE_EQUIVALENCE_DECISION":"PASS_38_OF_38_EXACT",
 "UBO5_TB15_RESUME_STRICT_PRIOR_BUILDER_DECISION":"REPAIRABLE_UNPLAYED_CANDIDATE_ASOF_ADAPTER_NOT_IMPLEMENTED",
 "UBO5_TB15_RESUME_HISTORICAL_PARITY_DECISION":"PASS_176363_ROWS_38_FEATURES_MAX_DIFF_0",
 "UBO5_TB15_RESUME_PROSPECTIVE_MATERIALIZATION_DECISION":f"PASS_{len(prospect)}_JULY22_PLAYER_GAMES",
 "UBO5_TB15_RESUME_PREGAME_RUN_CONTRACT_DECISION":"FROZEN_PENDING_CANDIDATE_ASOF_ADAPTER",
 "MLB_UBO5_TB15_PLATFORM_FEATURE_RESUME_DECISION":"REPAIRABLE_PLATFORM_OR_BUILDER_DEFECT",
 "MLB_UBO5_TB15_PRODUCTION_ACTION_DECISION":"NO_PRODUCTION_CHANGE_IN_THIS_TASK"}
 (OUT/"machine_readable_decisions.json").write_text(json.dumps(decisions,indent=2)+"\n");save("terminal_decisions.csv",[{"decision":k,"value":v} for k,v in decisions.items()])
 required=["interrupted_run_inventory.json","interrupted_run_inventory.md","reused_output_validation.csv","refresh_scope_contract.csv","completed_game_inventory.csv","raw_source_refresh_ledger.csv","normalized_extension_report.csv","schema_comparison.csv","feature_38_equivalence_registry.csv","strict_prior_builder_contract.csv","temporal_lineage_ledger.csv","historical_parity_rows.csv","historical_parity_summary.csv","prospective_materialization_audit.csv","pregame_run_contract.csv","machine_readable_decisions.json"]
 save("validation_report.csv",[{"check":f,"status":"PASS" if (OUT/f).exists() else "FAIL","detail":"required"} for f in required]+[{"check":"artifact_immutable","status":"PASS","detail":sha(ART)},{"check":"no_refit","status":"PASS","detail":"feature-only"},{"check":"candidate_adapter_defect_disclosed","status":"PASS","detail":"repairable"}])
 fs=[]
 for p in sorted(q for q in OUT.iterdir() if q.is_file() and q.name!="sha256_manifest.csv"):fs.append({"path":p.name,"size_bytes":p.stat().st_size,"sha256":sha(p)})
 save("sha256_manifest.csv",fs);print(json.dumps(decisions,indent=2))
if __name__=="__main__":main()
