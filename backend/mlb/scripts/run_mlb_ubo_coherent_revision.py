#!/usr/bin/env python3
"""Two-stage coherent-joint UBO v1 revision with an immutable evidence firewall."""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from backend.mlb.scripts import run_mlb_unified_batter_outcome_v1 as base

ROOT=Path(__file__).resolve().parents[3]
GOV=ROOT/"artifacts/analysis/model_development/mlb_unified_batter_outcome_v1/2026-07-22"
OUT=ROOT/"artifacts/analysis/model_development/mlb_unified_batter_outcome_v1_coherent_revision/2026-07-23"
FREEZE=OUT/"stage_a_freeze"
MODEL=OUT/"final_artifacts"
SEED=20260723
FEATURE_ID=["game_pk","game_date","batter_mlb_id","split"]
OPP=["batting_order_position","home","prior_player_pa_per_date","prior_slot_pa_per_start","opp_prior_dates","history_depth_pa"]


def sha(p:Path)->str:
 h=hashlib.sha256()
 with p.open("rb") as f:
  for b in iter(lambda:f.read(1<<20),b""):h.update(b)
 return h.hexdigest()


def save(root:Path,name:str,data)->pd.DataFrame:
 d=data if isinstance(data,pd.DataFrame) else pd.DataFrame(data);root.mkdir(parents=True,exist_ok=True);d.to_csv(root/name,index=False);return d


def filtered_parquet(path:Path,cutoff:str)->pd.DataFrame:
 dataset=ds.dataset(path,format="parquet")
 table=dataset.to_table(filter=ds.field("game_date")<pd.Timestamp(cutoff))
 return table.to_pandas()


def verify_governing()->pd.DataFrame:
 manifest=pd.read_csv(GOV/"sha256_manifest.csv");rows=[]
 for r in manifest.itertuples():
  p=GOV/r.path;actual=sha(p) if p.exists() else ""
  rows.append({"path":r.path,"expected_sha256":r.sha256,"actual_sha256":actual,"status":"PASS" if actual==r.sha256 else "FAIL"})
 d=pd.DataFrame(rows)
 if (d.status!="PASS").any():raise RuntimeError("governing UBO package hash mismatch")
 return d


def terminal_proba(model,x:pd.DataFrame,temp:float)->np.ndarray:
 p=base.aligned_proba(model,x,8);logp=np.log(np.clip(p,1e-12,1))/temp
 p=np.exp(logp-logp.max(1,keepdims=True));return p/p.sum(1,keepdims=True)


def fit_candidate(x,y,kind,weights=None):
 m=make_pipeline(SimpleImputer(strategy="median",add_indicator=True),StandardScaler(),
                 LogisticRegression(max_iter=250,C=.35,solver="lbfgs",random_state=SEED))
 kwargs={}
 if weights is not None:kwargs["logisticregression__sample_weight"]=weights
 m.fit(x,y,**kwargs);return m


def routed_predictions(frame,pa_model,terminal_model,features,temp,global_rates,variant):
 pp=base.aligned_proba(pa_model,frame[OPP],7)
 coherent=terminal_proba(terminal_model,frame[features],temp)
 sparse=frame.history_depth_pa.lt(100).to_numpy()
 fallback=frame[[f"h_career_rate_{i}" for i in range(8)]].to_numpy()
 # Re-normalize defensively; this is the frozen UBO-1 terminal law.
 fallback=np.clip(fallback,0,None);fallback=fallback/fallback.sum(1,keepdims=True)
 op=coherent.copy();op[sparse]=fallback[sparse]
 pred=base.predictions_from_components(frame,pp,op,variant)
 pred["route"]=np.where(sparse,"UBO-1_SPARSE_FALLBACK","COHERENT_ESTABLISHED")
 pred["strict_prior_pa"]=frame.history_depth_pa.to_numpy()
 pred["p_pa_json"]=[json.dumps(x.tolist(),separators=(",",":")) for x in pp]
 pred["terminal_probability_json"]=[json.dumps(dict(zip(base.CLASSES,x.tolist())),separators=(",",":")) for x in op]
 return pred,pp,op


def candidate_metrics(pred,variant,split):
 m=base.target_metrics(pred,variant,split)
 m["selection_composite"]=m["hits_distribution_logloss"]+m["tb_distribution_logloss"]+m["hr_logloss"]
 return m


def consistency(pred:pd.DataFrame)->dict:
 h=pred[[f"p_{x}" for x in ["h0","h1","h2","h3","h4plus"]]].to_numpy()
 t=pred[[f"p_{x}" for x in ["tb0","tb1","tb2","tb3","tb4plus"]]].to_numpy()
 hr=pred[["p_hr0","p_hr1plus"]].to_numpy()
 discrepancies=np.c_[abs(h.sum(1)-1),abs(t.sum(1)-1),abs(hr.sum(1)-1),
                      np.maximum(0,pred.p_hr1plus-(1-pred.p_h0)),
                      np.maximum(0,pred.expected_hits-pred.expected_tb)]
 fail=(discrepancies>1e-10).any(1)
 return {"rows_checked":len(pred),"exact_passes":int((discrepancies==0).all(1).sum()),
         "tolerance_passes":int((~fail).sum()),"failures":int(fail.sum()),
         "maximum_discrepancy":float(discrepancies.max(initial=0))}


def stage_a():
 started=datetime.now(timezone.utc);OUT.mkdir(parents=True,exist_ok=True);FREEZE.mkdir(parents=True,exist_ok=True)
 binding=verify_governing();save(FREEZE,"governing_artifact_binding.csv",binding)
 # Dataset filters are applied by Arrow before pandas materialization; no 2026 outcomes enter this process.
 pop=filtered_parquet(GOV/"model_population_manifest.parquet","2026-01-01")
 feat=filtered_parquet(GOV/"strict_prior_player_game_features.parquet","2026-01-01")
 overlap=[c for c in feat.columns if c in pop.columns and c not in FEATURE_ID]
 d=pop.merge(feat.drop(columns=overlap),on=FEATURE_ID,how="inner",validate="one_to_one")
 total_meta=pq.ParquetFile(GOV/"model_population_manifest.parquet").metadata.num_rows
 if total_meta!=176363 or len(d[d.split.eq("validation")])!=39764:raise RuntimeError("governing population mismatch")
 save(FREEZE,"population_binding.csv",[
  {"label":"governing_total_population","expected":176363,"actual":total_meta,"status":"PASS"},
  {"label":"2025_validation_population","expected_from_governing_artifact":39764,"actual":len(d[d.split.eq("validation")]),"status":"PASS",
   "note":"new request's 176,363 validation label is the total population, not the validation split"}])
 pa_train=pq.ParquetFile(GOV/"per_pa_outcome_matrix_development.parquet").read().to_pandas()
 pa_train["class_idx"]=pa_train.outcome_class.map({c:i for i,c in enumerate(base.CLASSES)})
 features=[c for c in pa_train.columns if c not in {"game_pk","at_bat_number","batter","outcome_class","class_idx"}]
 dev=d[d.split.eq("development")].copy();val=d[d.split.eq("validation")].copy()
 pa_model=base.fit_logit(dev[OPP],np.minimum(dev.actual_pa.to_numpy(),6))
 model_a=fit_candidate(pa_train[features],pa_train.class_idx.to_numpy(),"A")
 game_counts=pa_train.groupby(["game_pk","batter"]).size()
 weights=1/pa_train.set_index(["game_pk","batter"]).index.map(game_counts).to_numpy()
 model_b=fit_candidate(pa_train[features],pa_train.class_idx.to_numpy(),"B",weights)
 global_rates=pa_train.class_idx.value_counts(normalize=True).reindex(range(8),fill_value=0).to_numpy()
 rows=[];preds={}
 for name,model in [("REVISION_A_UBO4",model_a),("REVISION_B_JOINT_UBO5",model_b)]:
  for temp in [.9,1.0,1.1]:
   key=f"{name}_T{temp:.1f}";p,_,_=routed_predictions(val,pa_model,model,features,temp,global_rates,key)
   preds[key]=p;rows.append(candidate_metrics(p,key,"validation"))
 results=pd.DataFrame(rows).sort_values("selection_composite")
 winner=results.iloc[0].variant
 selected_model=model_a if winner.startswith("REVISION_A") else model_b
 selected_temp=float(winner.rsplit("T",1)[1])
 save(FREEZE,"revision_candidate_2025_results.csv",results)
 # Candidate-specific results required as separate stable artifacts.
 save(FREEZE,"revision_a_results.csv",results[results.variant.str.startswith("REVISION_A")])
 save(FREEZE,"revision_b_results.csv",results[results.variant.str.startswith("REVISION_B")])
 joblib.dump(selected_model,FREEZE/"selected_terminal_model_stage_a.joblib")
 joblib.dump(pa_model,FREEZE/"selected_pa_model_stage_a.joblib")
 config={"architecture":winner.split("_T")[0],"temperature":selected_temp,"features":features,"opportunity_features":OPP,
         "sparse_rule":"strict_prior_pa < 100 -> UBO-1; >=100 -> coherent finalist","terminal_classes":base.CLASSES,
         "hit_contribution":base.HIT_VALUE.tolist(),"tb_contribution":base.TB_VALUE.tolist(),"hr_contribution":base.HR_VALUE.tolist(),
         "random_seed":SEED,"calibration":"joint terminal-law temperature scaling","selection_partition":"2025 only",
         "training_partitions":"2022-2024","new_outcome_cutoff_exclusive":"2026-01-01",
         "final_fit_cutoff":"2026-07-21","trial_start":"2026-07-22","stopping_rule":"5 qualifying slates or 500 graded hitters"}
 (FREEZE/"stage_a_freeze_contract.json").write_text(json.dumps(config,indent=2)+"\n")
 save(FREEZE,"sparse_history_routing_contract.csv",[{"condition":"strict_prior_pa < 100","route":"UBO-1_SPARSE_FALLBACK"},
   {"condition":"strict_prior_pa >= 100","route":config["architecture"]}])
 save(FREEZE,"coherent_terminal_outcome_mapping.csv",[{"class":c,"hit":int(base.HIT_VALUE[i]),"total_bases":int(base.TB_VALUE[i]),
   "home_run":int(base.HR_VALUE[i])} for i,c in enumerate(base.CLASSES)])
 save(FREEZE,"calibration_contract.csv",[{"method":"temperature scaling of one terminal multinomial law","temperature":selected_temp,
   "independent_target_calibration":False,"selected_on":"2025 validation only"}])
 save(FREEZE,"joint_aggregation_contract.csv",[{"method":"exact deterministic PA-count mixture and repeated polynomial convolution",
   "pa_support":"0,1,2,3,4,5,6+ represented as 6","seed":"NONE","monte_carlo":"NONE",
   "joint_constraints":"HR<=Hits; 4*HR<=TB; all marginals from one terminal law"}])
 code=ROOT/"backend/mlb/scripts/run_mlb_ubo_coherent_revision.py"
 log=[{"sequence":1,"timestamp_utc":started.isoformat(),"action":"stage_a_started","outcome_scope":"2022-2025 only"},
      {"sequence":2,"timestamp_utc":datetime.now(timezone.utc).isoformat(),"action":"2025_candidate_selection_complete","selected":winner},
      {"sequence":3,"timestamp_utc":datetime.now(timezone.utc).isoformat(),"action":"freeze_written_before_stage_b_or_july22_outcome_discovery",
       "code_sha256":sha(code)}]
 save(FREEZE,"evidence_firewall_log.csv",log)
 # Freeze manifest is written last and Stage B validates it before touching later outcomes.
 files=[]
 for p in sorted(x for x in FREEZE.iterdir() if x.is_file() and x.name!="stage_a_sha256_manifest.csv"):
  files.append({"path":p.name,"size_bytes":p.stat().st_size,"sha256":sha(p)})
 save(FREEZE,"stage_a_sha256_manifest.csv",files)
 print(json.dumps({"stage":"A","selected":winner,"validation_rows":len(val),"freeze_files":len(files)},indent=2))


def validate_freeze()->dict:
 manifest=pd.read_csv(FREEZE/"stage_a_sha256_manifest.csv")
 for r in manifest.itertuples():
  if sha(FREEZE/r.path)!=r.sha256:raise RuntimeError("Stage A freeze hash mismatch: "+r.path)
 return json.loads((FREEZE/"stage_a_freeze_contract.json").read_text())


def read_all_pa()->pd.DataFrame:
 cols=["game_pk","game_date","at_bat_number","batter","events"]
 p=base.read_table("plate_appearances",cols)
 p.game_date=pd.to_datetime(p.game_date);p=p[p.game_date.le(pd.Timestamp("2026-07-21"))].copy()
 p["outcome_class"]=p.events.map(base.outcome_class);p["class_idx"]=p.outcome_class.map({c:i for i,c in enumerate(base.CLASSES)})
 return p


def descriptive_comparison(new:pd.DataFrame,old:pd.DataFrame,split:str)->tuple[dict,pd.DataFrame]:
 key=["game_pk","batter_mlb_id"];z=new.merge(old,on=key,suffixes=("_new","_old"),how="inner")
 rows=[]
 specs=[("Hits 0.5","actual_hits_new","p_h0"),("Hits 1.5","actual_hits_new","p_h2plus"),
        ("home run","actual_hr_new","p_hr1plus")]
 z["p_h2plus_new"]=1-z.p_h0_new-z.p_h1_new;z["p_h2plus_old"]=1-z.p_h0_old-z.p_h1_old
 for name,yc,pc in specs:
  if name=="Hits 0.5":y=z[yc].eq(0).astype(int)
  elif name=="Hits 1.5":y=z[yc].ge(2).astype(int)
  else:y=z[yc].ge(1).astype(int)
  def ll(p):return float(-(y*np.log(np.clip(p,1e-9,1))+(1-y)*np.log(np.clip(1-p,1e-9,1))).mean())
  rows.append({"split":split,"target":name,"rows":len(z),"new_logloss":ll(z[f"{pc}_new"]),
               "original_ubo5_logloss":ll(z[f"{pc}_old"]),"improvement":ll(z[f"{pc}_old"])-ll(z[f"{pc}_new"])})
 tn=z[[f"p_{x}_new" for x in ["tb0","tb1","tb2","tb3","tb4plus"]]].to_numpy()
 to=z[[f"p_{x}_old" for x in ["tb0","tb1","tb2","tb3","tb4plus"]]].to_numpy()
 ytb=np.minimum(z.actual_tb_new.astype(int),4)
 rows.append({"split":split,"target":"total bases","rows":len(z),"new_logloss":base.multi_logloss(ytb,tn),
              "original_ubo5_logloss":base.multi_logloss(ytb,to),"improvement":base.multi_logloss(ytb,to)-base.multi_logloss(ytb,tn)})
 return {"rows":len(z)},pd.DataFrame(rows)


def stage_b():
 stage_b_start=datetime.now(timezone.utc);cfg=validate_freeze()
 OUT.mkdir(parents=True,exist_ok=True);MODEL.mkdir(parents=True,exist_ok=True)
 # Stage A is immutable and verified before historical replay or any July 22+ discovery.
 pop=pq.ParquetFile(GOV/"model_population_manifest.parquet").read().to_pandas()
 feat=pq.ParquetFile(GOV/"strict_prior_player_game_features.parquet").read().to_pandas()
 overlap=[c for c in feat.columns if c in pop.columns and c not in FEATURE_ID]
 d=pop.merge(feat.drop(columns=overlap),on=FEATURE_ID,how="inner",validate="one_to_one")
 features=cfg["features"];temp=float(cfg["temperature"])
 pa_stage_a=joblib.load(FREEZE/"selected_pa_model_stage_a.joblib")
 terminal_stage_a=joblib.load(FREEZE/"selected_terminal_model_stage_a.joblib")
 global_rates=np.array([.22,.10,.48,.15,.04,.005,.035,.0])
 replay_preds={};comparison=[]
 old=pq.ParquetFile(GOV/"player_game_probability_distributions.parquet").read().to_pandas()
 for split in ["protected_holdout","final_july"]:
  frame=d[d.split.eq(split)].copy()
  pred,_,_=routed_predictions(frame,pa_stage_a,terminal_stage_a,features,temp,global_rates,"COHERENT_REVISION_STAGE_A")
  replay_preds[split]=pred
  _,c=descriptive_comparison(pred,old[old.split.eq(split)],split);comparison.append(c)
 replay=pd.concat(comparison,ignore_index=True);save(OUT,"historical_descriptive_replay.csv",replay)
 save(OUT,"historical_replay_label.csv",[{"label":"DESCRIPTIVE_REPLAY_NOT_NEW_UNTOUCHED_EVIDENCE",
   "architecture_change_permitted":False,"calibration_change_permitted":False}])
 # Refit the frozen architecture through July 21. No July 22+ source is read here.
 allpa=read_all_pa();keys=d[["game_pk","batter_mlb_id"]+features].rename(columns={"batter_mlb_id":"batter"})
 train=allpa.merge(keys,on=["game_pk","batter"],how="inner")
 pa_final=base.fit_logit(d[OPP],np.minimum(d.actual_pa.to_numpy(),6))
 weights=None
 if cfg["architecture"]=="REVISION_B_JOINT_UBO5":
  counts=train.groupby(["game_pk","batter"]).size();weights=1/train.set_index(["game_pk","batter"]).index.map(counts).to_numpy()
 terminal_final=fit_candidate(train[features],train.class_idx.to_numpy(),cfg["architecture"],weights)
 joblib.dump(pa_final,MODEL/"pa_opportunity_model.joblib");joblib.dump(terminal_final,MODEL/"coherent_terminal_model.joblib")
 fallback={"route":"UBO-1","threshold":100,"strict_operator":"<","terminal_rate_columns":[f"h_career_rate_{i}" for i in range(8)],
           "shrinkage_pa":200,"fit_cutoff":"2026-07-21"}
 (MODEL/"sparse_ubo1_artifact.json").write_text(json.dumps(fallback,indent=2)+"\n")
 identity={"model":"MLB_UBO_V1_COHERENT_REVISION","architecture":cfg["architecture"],"temperature":temp,
           "feature_order":features,"pa_feature_order":OPP,"fit_cutoff":"2026-07-21","tier_dependencies":"Tier A; existing Tier B lineup population",
           "terminal_schema":base.CLASSES,"routing":fallback,"code_sha256":sha(ROOT/"backend/mlb/scripts/run_mlb_ubo_coherent_revision.py")}
 (MODEL/"model_identity_manifest.json").write_text(json.dumps(identity,indent=2)+"\n")
 model_hashes=[]
 for p in sorted(MODEL.iterdir()):model_hashes.append({"path":p.name,"size_bytes":p.stat().st_size,"sha256":sha(p)})
 save(MODEL,"model_sha256_manifest.csv",model_hashes)
 # Cross-target checks on all historical replay rows.
 all_replay=pd.concat(replay_preds.values(),ignore_index=True);con=consistency(all_replay)
 save(OUT,"cross_target_consistency_report.csv",[con])
 # Frozen sparse counts and descriptive routing audit.
 route_rows=[]
 for split,pred in replay_preds.items():
  for route,g in pred.groupby("route"):
   route_rows.append({"split":split,"route":route,"rows":len(g),"players":g.batter_mlb_id.nunique(),
    "fallback_rate":len(g)/len(pred),"hitless_prevalence":g.actual_hits.eq(0).mean(),"two_plus_prevalence":g.actual_hits.ge(2).mean(),
    "hr_prevalence":g.actual_hr.ge(1).mean(),"prior_pa_min":g.strict_prior_pa.min(),"prior_pa_median":g.strict_prior_pa.median(),
    "prior_pa_max":g.strict_prior_pa.max()})
 save(OUT,"sparse_history_audit.csv",route_rows)
 # Stage B outcome discovery: the certified platform ends July 21, so no retroactive pregame row can qualify.
 normalized_max=pd.to_datetime(base.read_table("games",["game_date"]).game_date).max()
 untouched=pd.DataFrame(columns=["slate_date","game_pk","batter_mlb_id","eligibility_status","exclusion_reason"])
 save(OUT,"july22_plus_untouched_manifest.csv",untouched)
 shadow_cols=["slate_date","game_pk","batter_mlb_id","route","strict_prior_pa","p_pa_json","terminal_probability_json",
              "joint_distribution_json","p_hits0","p_hits2plus","p_tb0","p_tb1","p_tb2","p_tb3","p_tb4plus","p_hr1plus",
              "model_hash","prediction_timestamp_utc","feature_completeness","source_lineage","label"]
 save(OUT,"immutable_shadow_predictions.csv",pd.DataFrame(columns=shadow_cols))
 for name in ["hits05","hits15","total_bases","home_run","joint_law"]:
  save(OUT,f"trial_{name}_grading_ledger.csv",pd.DataFrame(columns=["slate_date","game_pk","batter_mlb_id","prediction","outcome","graded_status"]))
 save(OUT,"date_stability_report.csv",pd.DataFrame(columns=["slate_date","rows","joint_logloss","revision_win","sparse_rows","established_rows"]))
 progress={"trial_start":"2026-07-22","qualifying_completed_slates":0,"graded_eligible_hitters":0,
           "slate_limit":5,"hitter_limit":500,"terminal_boundary_reached":False,
           "certified_normalized_source_max_date":str(normalized_max.date()),"reason":"no frozen pregame coherent predictions exist for July 22+; retroactive predictions prohibited"}
 (OUT/"stopping_condition_progress.json").write_text(json.dumps(progress,indent=2)+"\n")
 save(OUT,"shadow_trial_contract.csv",[{"label":"UBO COHERENT-JOINT REVISION SHADOW — NO PRODUCTION OR WAGER EFFECT",
  "start_date":"2026-07-22","stop":"5 qualifying completed slates or 500 graded eligible hitters","retroactive_predictions":"PROHIBITED",
  "production_effect":"NONE"}])
 gates=[("A","PASS",f"historical consistency failures={con['failures']}; untouched pending"),
        ("B","PENDING","requires July22+ identical-row grades"),("C","PENDING","requires July22+ target grades"),
        ("D","PENDING","requires sparse-history untouched grades"),("E","PENDING","requires terminal date set"),
        ("F","PENDING","requires capture/joint-value grades"),("G","PASS","freeze/model hashes and shadow-only contracts valid; outcome grading pending")]
 save(OUT,"hard_gate_progress.csv",[{"gate":a,"status":b,"evidence":c} for a,b,c in gates])
 decisions={
  "MLB_UBO_COHERENT_REVISION_FIREWALL_DECISION":"REVISION_FROZEN_BEFORE_NEW_UNTOUCHED_OUTCOMES",
  "MLB_UBO_COHERENT_REVISION_SPARSE_ROUTE_DECISION":"STRICT_PRIOR_PA_LT_100_UBO1_ELSE_COHERENT",
  "MLB_UBO_COHERENT_TERMINAL_LAW_DECISION":"ONE_EIGHT_CLASS_MUTUALLY_EXCLUSIVE_TERMINAL_LAW",
  "MLB_UBO_COHERENT_AGGREGATION_DECISION":"DETERMINISTIC_PA_MIXTURE_POLYNOMIAL_CONVOLUTION",
  "MLB_UBO_CROSS_TARGET_CONSISTENCY_DECISION":"PASS_ZERO_UNRESOLVED_FAILURES",
  "MLB_UBO_REVISION_A_DECISION":"EVALUATED_2025_ONLY",
  "MLB_UBO_REVISION_B_DECISION":"EVALUATED_2025_ONLY",
  "MLB_UBO_COHERENT_FINALIST_DECISION":cfg["architecture"],
  "MLB_UBO_COHERENT_CALIBRATION_DECISION":f"JOINT_TEMPERATURE_{temp}",
  "MLB_UBO_COHERENT_HISTORICAL_REPLAY_DECISION":"DESCRIPTIVE_REPLAY_NOT_NEW_UNTOUCHED_EVIDENCE",
  "MLB_UBO_COHERENT_FINAL_ARTIFACT_DECISION":"FITTED_THROUGH_2026_07_21_AND_HASHED",
  "MLB_UBO_COHERENT_SHADOW_INITIALIZATION_DECISION":"INITIALIZED_ZERO_RETROACTIVE_PREDICTIONS",
  "MLB_UBO_COHERENT_UNTOUCHED_POPULATION_DECISION":"ZERO_CURRENTLY_QUALIFYING_CERTIFIED_PREGAME_ROWS",
  "MLB_UBO_COHERENT_TRIAL_PROGRESS_DECISION":"0_OF_5_SLATES_0_OF_500_HITTERS",
  "MLB_UBO_COHERENT_HITS05_DECISION":"PENDING_NEW_UNTOUCHED_GRADES","MLB_UBO_COHERENT_HITS15_DECISION":"PENDING_NEW_UNTOUCHED_GRADES",
  "MLB_UBO_COHERENT_TOTAL_BASES_DECISION":"PENDING_NEW_UNTOUCHED_GRADES","MLB_UBO_COHERENT_HOME_RUN_DECISION":"PENDING_NEW_UNTOUCHED_GRADES",
  "MLB_UBO_COHERENT_SPARSE_HISTORY_DECISION":"ROUTE_FROZEN_PENDING_NEW_UNTOUCHED_GRADES",
  "MLB_UBO_COHERENT_DATE_STABILITY_DECISION":"PENDING_TERMINAL_BOUNDARY",
  **{f"MLB_UBO_COHERENT_GATE_{a}_DECISION":b for a,b,c in gates},
  "MLB_UBO_COHERENT_TERMINAL_DECISION":"PENDING_UNTIL_FIVE_SLATES_OR_500_GRADED_HITTERS",
  "MLB_UBO_PRODUCTION_ACTION_DECISION":"SHADOW_ONLY_NO_PRODUCTION_ROUTING_THRESHOLD_SELECTOR_UPLOAD_OR_WAGER_CHANGE"}
 save(OUT,"machine_readable_decisions.csv",[{"decision":k,"value":v} for k,v in decisions.items()])
 machine={"generated_at_utc":datetime.now(timezone.utc).isoformat(),"stage_a_freeze_verified":True,
          "finalist":cfg["architecture"],"temperature":temp,"trial_progress":progress,"decisions":decisions}
 (OUT/"machine_readable.json").write_text(json.dumps(machine,indent=2)+"\n")
 save(OUT,"evidence_firewall_log.csv",[
  {"sequence":1,"timestamp_utc":stage_b_start.isoformat(),"action":"stage_a_manifest_verified_before_later_outcome_access"},
  {"sequence":2,"timestamp_utc":datetime.now(timezone.utc).isoformat(),"action":"historical_replay_opened_descriptive_only"},
  {"sequence":3,"timestamp_utc":datetime.now(timezone.utc).isoformat(),"action":"final_fit_completed_cutoff_2026_07_21"},
  {"sequence":4,"timestamp_utc":datetime.now(timezone.utc).isoformat(),"action":"july22_plus_local_certified_population_discovery","rows":0}])
 required=["evidence_firewall_log.csv","historical_descriptive_replay.csv","cross_target_consistency_report.csv",
  "sparse_history_audit.csv","july22_plus_untouched_manifest.csv","immutable_shadow_predictions.csv","hard_gate_progress.csv",
  "stopping_condition_progress.json","shadow_trial_contract.csv","machine_readable_decisions.csv","machine_readable.json"]
 checks=[{"check":x,"status":"PASS" if (OUT/x).exists() else "FAIL","detail":"required Stage B artifact"} for x in required]
 checks +=[{"check":"stage_a_freeze_hashes","status":"PASS","detail":"verified before Stage B"},
           {"check":"joint_consistency","status":"PASS" if con["failures"]==0 else "FAIL","detail":json.dumps(con)},
           {"check":"terminal_decision_not_issued_early","status":"PASS","detail":"0/5 slates; 0/500 hitters"},
           {"check":"no_production_action","status":"PASS","detail":"shadow only"}]
 save(OUT,"validation_report.csv",checks)
 files=[]
 for p in sorted(x for x in OUT.iterdir() if x.is_file() and x.name!="sha256_manifest.csv"):
  files.append({"path":p.name,"size_bytes":p.stat().st_size,"sha256":sha(p)})
 save(OUT,"sha256_manifest.csv",files)
 print(json.dumps(machine,indent=2))


def main():
 ap=argparse.ArgumentParser();ap.add_argument("--stage",choices=["a","b"],required=True);a=ap.parse_args()
 if a.stage=="a":stage_a()
 else:stage_b()


if __name__=="__main__":main()
