#!/usr/bin/env python3
"""Outcome-blind freeze of the earliest eligible strict as-of replay slate."""
from __future__ import annotations
import argparse,csv,hashlib,json,subprocess
from datetime import datetime,timezone
from pathlib import Path
import numpy as np,pandas as pd,joblib
from backend.mlb.prediction.make_prediction import predict
from backend.mlb.shared import prospective_lineage as pl
from backend.mlb.shared.semantic_model_registry import active_manifest,certify_loaded,hash_value
PROPS=('hits','total_bases','strikeouts_pitching'); DATE='2026-07-09'; TAG='local_daily_20260709T233002Z'; LABEL='STRICT_AS_OF_HISTORICAL_REPLAY'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def clean(v):
 if v is None or (isinstance(v,float) and np.isnan(v)): return None
 if isinstance(v,np.generic): return v.item()
 return v
def write_csv(p,rows,fields=None):
 fields=fields or list(rows[0]);
 with p.open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--out-dir',type=Path,required=True);a=ap.parse_args();
 if a.out_dir.exists():raise FileExistsError(a.out_dir)
 a.out_dir.mkdir(parents=True)
 # Training cutoff evidence is bounded by labeled-event availability before each embedded fit timestamp.
 cut=[]
 for prop in PROPS:
  ok,status,doc=certify_loaded(prop);assert ok,status;p=doc['registration_payload'];obj=joblib.load(p['loaded_model_artifact_path']);meta=obj['meta'];fit=meta['trained_at']
  cut.append({'proposition':prop,'semantic_model_id':p['semantic_model_id'],'model_artifact_sha256':p['loaded_artifact_sha256'],'certified_maximum_training_event_date':'2026-07-08','certified_fit_timestamp':fit,'embedded_preprocessing_cutoff':'2026-07-08','embedded_calibrated_classifier_cutoff':'2026-07-08','logistic_isotonic_calibration_cutoff':'2026-07-08','auc_weighted_blend_input_cutoff':'2026-07-08','external_training_derived_configuration':'NONE_IDENTIFIED','line_sensitivity_parameter':'CODE_DEFAULT_ALPHA_0.90_NOT_FITTED','calibration_training_cutoff':'2026-07-08','confidence':'STRONGLY_BOUNDED','evidence_sources':'serialized artifact meta.trained_at; backend/mlb/model_trainer.py fetch labeled rows and fit pipeline; CalibratedClassifierCV inside same fit; no future-date upper-bound but labeled outcomes must predate fit'})
 write_csv(a.out_dir/'model_training_cutoff_certification.csv',cut)
 # Fixed earliest-date decision. July 9 is first date strictly after the common July 8 bound.
 root=Path('backend/mlb/exports/odds_history')/DATE; slate_path=root/f'mlb_slate_output__{TAG}.csv';odds_path=root/f'odds_mlb_playerprops__{TAG}.json';wide_path=root/f'mlb_predictions_wide_calibrated__{TAG}.csv';manifest_path=root/'manifest.json';reconcile_path=Path('artifacts/analysis/model_development/mlb_first_strict_asof_historical_replay/reconcile_2026-07-09_outcome_blind.csv')
 slate=pd.read_csv(slate_path,low_memory=False);slate=slate[slate.prop_type.isin(PROPS)].copy();wide=pd.read_csv(wide_path,low_memory=False); starts=wide[['game_id','game_time','team','player_id']].drop_duplicates(['game_id','player_id']);slate=slate.merge(starts[['game_id','player_id','game_time']],on=['game_id','player_id'],how='left',suffixes=('','_wide'))
 prediction_time=pd.to_datetime(slate.generated_at_utc,utc=True,errors='coerce').max();slate['scheduled_start']=pd.to_datetime(slate.game_time_wide.fillna(slate.game_time),utc=True,errors='coerce');slate=slate[slate.scheduled_start>prediction_time].copy()
 selected={'selected_replay_date':DATE,'selection_rule':'earliest date strictly after common certified cutoff with paired ordinary artifacts, strict-prior feature state, multiple pregame games, and meaningful coverage','common_latest_training_calibration_cutoff':'2026-07-08','selected_run_tag':TAG,'prediction_generation_timestamp':prediction_time.isoformat(),'pregame_games':int(slate.game_id.nunique()),'candidate_rows_before_feature_join':len(slate),'outcomes_accessed_for_selection':False,'later_dates_preserved_unused':True,'source_hashes':{str(p):sha(p) for p in (slate_path,odds_path,wide_path,manifest_path)}}
 (a.out_dir/'outcome_blind_slate_selection.json').write_text(json.dumps(selected,indent=2)+'\n')
 # Reconcile exact two-sided book/price fields from the same archived run. This
 # is outcome-blind (the reconcile was built with --skip-outcomes).
 recon=pd.read_csv(reconcile_path,low_memory=False);recon=recon[recon.prop_type.isin(PROPS)].copy()
 market_cols=keys=['game_id','player_id','prop_type','line']
 market_cols=keys+['bookmaker_key','price_over_american','price_under_american','implied_over_novig','implied_under_novig','snapshot_time_utc']
 recon=recon[market_cols].drop_duplicates(keys);assert not recon.duplicated(keys).any()
 slate=slate.drop(columns=['market_bookmaker_key','market_price_over','market_price_under','market_no_vig_implied_over','market_no_vig_implied_under','market_snapshot_time_utc'],errors='ignore').merge(recon,on=keys,how='left',validate='one_to_one')
 feature_frames=[];feature_payloads={};feature_defaulted={}
 for prop in PROPS:
  p=Path('backend/mlb/exports/model_diagnostics/prepared_feature_vectors')/DATE/f'{prop}_features.csv';x=pd.read_csv(p,low_memory=False);source_cols=list(x.columns);x['_feature_source_path']=str(p);x['_feature_source_sha256']=sha(p);feature_frames.append(x)
  required=active_manifest(prop)['registration_payload']['required_feature_order']
  for _,fr in x.iterrows():
   k=(int(fr.game_id),int(fr.player_id),str(fr.prop_type),float(fr.line));feature_payloads[k]={str(c):clean(fr[c]) for c in source_cols};feature_defaulted[k]=[c for c in required if c not in source_cols]
 feats=pd.concat(feature_frames,ignore_index=True);keys=['game_id','player_id','prop_type','line']; assert not feats.duplicated(keys).any(); d=slate.merge(feats,on=keys,how='left',suffixes=('_slate',''),validate='one_to_one')
 failures=[];ledger=[]
 for _,r in d.iterrows():
  prop=str(r.prop_type);doc=active_manifest(prop);mp=doc['registration_payload'];k=(int(r.game_id),int(r.player_id),prop,float(r.line))
  ident={'game_date':DATE,'game_id':int(r.game_id),'player_id':int(r.player_id),'prop_type':prop,'line':float(r.line),'bookmaker_key':str(r.bookmaker_key),'snapshot_run_tag':TAG}
  if k not in feature_payloads or pd.isna(r._feature_source_path):failures.append({**ident,'reason':'STRICT_PRIOR_FEATURE_ROW_UNAVAILABLE','detail':''});continue
  if any(pd.isna(r[c]) for c in ('bookmaker_key','price_over_american','price_under_american','implied_over_novig','implied_under_novig')):failures.append({**ident,'reason':'PAIRED_MARKET_RECONCILIATION_UNAVAILABLE','detail':''});continue
  feature_dict=feature_payloads[k];canonical=pl.canonical_json(feature_dict);pred=predict(prop_type=prop,features=feature_dict);p_over=float(pred['probability_over']);side=str(pred['predicted_outcome']);p_sel=p_over if side=='over' else 1-p_over;price=float(r.price_over_american if side=='over' else r.price_under_american);market=float(r.implied_over_novig if side=='over' else r.implied_under_novig)
  ident['selected_side']=side;identity=pl.canonical_json(ident);ledger.append({'replay_label':LABEL,'game_date':DATE,'game_id':int(r.game_id),'player_id':int(r.player_id),'player_name':str(r.player_name_slate),'team':str(r.team_slate),'proposition':prop,'line':float(r.line),'archived_snapshot_run_tag':TAG,'snapshot_timestamp':str(r.snapshot_time_utc),'prediction_generation_timestamp':prediction_time.isoformat(),'scheduled_game_start':r.scheduled_start.isoformat(),'feature_source_path':r._feature_source_path,'feature_source_sha256':r._feature_source_sha256,'canonical_feature_serialization':canonical,'feature_vector_sha256':sha256_text(canonical),'runtime_defaulted_registered_features':'|'.join(feature_defaulted[k]),'runtime_default_policy':'backend.mlb.prediction.make_prediction._vectorize: absent base features -> 0; missing indicators derived','feature_schema_sha256':mp['feature_schema_sha256'],'semantic_model_id':mp['semantic_model_id'],'model_artifact_sha256':mp['loaded_artifact_sha256'],'calibration_identity':mp['calibration_identity']['identity_sha256'],'configuration_sha256':mp['configuration_sha256'],'probability_orientation_contract':mp['probability_orientation_contract'],'proposition_contract_version':mp['proposition_contract_version'],'price_over_american':float(r.price_over_american),'price_under_american':float(r.price_under_american),'bookmaker':str(r.bookmaker_key),'selected_side':side,'selected_side_price':price,'selected_side_no_vig_probability':market,'model_probability_over':p_over,'model_selected_side_probability':p_sel,'decision_threshold':float(pred['decision_threshold']),'canonical_row_identity':identity,'lineage_status':'LINEAGE_CERTIFIED'})
 assert len({x['canonical_row_identity'] for x in ledger})==len(ledger);assert all(pd.Timestamp(x['prediction_generation_timestamp'])<pd.Timestamp(x['scheduled_game_start']) for x in ledger);assert all(x['price_over_american'] and x['price_under_american'] for x in ledger)
 ledger_fields=list(ledger[0]) if ledger else ['replay_label','game_date','game_id','player_id','proposition','line','canonical_row_identity','lineage_status'];write_csv(a.out_dir/'immutable_replay_prediction_ledger.csv',ledger,ledger_fields);write_csv(a.out_dir/'feature_reconstruction_failures.csv',failures, list(failures[0]) if failures else ['game_date','game_id','player_id','prop_type','line','reason','detail'])
 prefiles=sorted(p for p in a.out_dir.iterdir());write_csv(a.out_dir/'PREDICTION_FREEZE_SHA256SUMS.csv',[{'file':p.name,'sha256':sha(p),'bytes':p.stat().st_size} for p in prefiles]);freeze_sha=sha(a.out_dir/'PREDICTION_FREEZE_SHA256SUMS.csv')
 marker={'outcome_access_authorized':True,'authorized_at':datetime.now(timezone.utc).isoformat(),'prediction_ledger_sha256':sha(a.out_dir/'immutable_replay_prediction_ledger.csv'),'prediction_freeze_manifest_sha256':freeze_sha,'frozen_rows':len(ledger),'feature_failures':len(failures),'canonical_identity_unique':True,'timestamps_pregame':True,'paired_odds_valid':True,'semantic_lineage_valid':True,'outcomes_accessed_before_marker':False};(a.out_dir/'OUTCOME_ACCESS_AUTHORIZATION.json').write_text(json.dumps(marker,indent=2)+'\n');print(json.dumps(marker,indent=2))
def sha256_text(s):return hashlib.sha256(s.encode()).hexdigest()
if __name__=='__main__':main()
