#!/usr/bin/env python3
"""Outcome-blind multi-slate replay freeze; never reads outcome sources."""
from __future__ import annotations
import csv,hashlib,json,shutil
from datetime import datetime,timezone
from pathlib import Path
import numpy as np,pandas as pd
from backend.mlb.prediction.make_prediction import predict
from backend.mlb.shared import prospective_lineage as pl
from backend.mlb.shared.semantic_model_registry import active_manifest
PROPS=('hits','total_bases','strikeouts_pitching'); START='2026-07-09'; END='2026-08-02'
ROOT=Path('backend/mlb/exports'); OUT=Path('artifacts/analysis/model_development/mlb_multi_slate_strict_asof_replay/2026-07-09_2026-08-02')
ACCEPTED=Path('artifacts/analysis/model_development/mlb_first_strict_asof_historical_replay/2026-07-09_final/immutable_replay_prediction_ledger.csv')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def clean(v):
 if v is None or (isinstance(v,float) and np.isnan(v)):return None
 return v.item() if isinstance(v,np.generic) else v
def main():
 led=OUT/'prediction_ledgers';led.mkdir(parents=True,exist_ok=True); manifests=[];date_rows=[];all_rows=[]
 for day in pd.date_range(START,END):
  ds=str(day.date());od=ROOT/'odds_history'/ds;fd=ROOT/'model_diagnostics/prepared_feature_vectors'/ds;candidates=[]
  for sp in sorted(od.glob('mlb_slate_output__local_daily_*.csv')):
   x=pd.read_csv(sp,low_memory=False);x=x[x.prop_type.isin(PROPS)].copy()
   if x.empty:continue
   pt=pd.to_datetime(x.generated_at_utc,utc=True,errors='coerce').max();x['start']=pd.to_datetime(x.game_time,utc=True,errors='coerce');pre=x[x.start>pt]
   if len(pre):candidates.append((sp,pt,pre))
  if not candidates:
   reason='no ordinary decision run' if not list(od.glob('mlb_slate_output__local_daily_*.csv')) else 'snapshot after game start'
   date_rows.append({'date':ds,'eligibility_status':'EXCLUDED','selected_ordinary_run_tag':'','snapshot_timestamp':'','games_available':0,'games_pregame_at_snapshot':0,'feature_state_availability':False,'paired_odds_availability':False,'semantic_compatibility':True,'inclusion_or_exclusion_reason':reason});continue
  sp,pt,slate=candidates[-1];tag=sp.stem.split('__',1)[1]
  required=[fd/f'{p}_features.csv' for p in PROPS]
  if not all(p.exists() for p in required):
   date_rows.append({'date':ds,'eligibility_status':'EXCLUDED','selected_ordinary_run_tag':tag,'snapshot_timestamp':pt.isoformat(),'games_available':int(slate.game_id.nunique()),'games_pregame_at_snapshot':int(slate.game_id.nunique()),'feature_state_availability':False,'paired_odds_availability':False,'semantic_compatibility':True,'inclusion_or_exclusion_reason':'strict-prior feature state unavailable'});continue
  if ds==START:
   d=pd.read_csv(ACCEPTED);dst=led/f'{ds}.csv';shutil.copyfile(ACCEPTED,dst);fail=pd.read_csv(ACCEPTED.parent/'feature_reconstruction_failures.csv');fail.to_csv(led/f'{ds}_failures.csv',index=False)
  else:
   recp=OUT/'outcome_blind_reconcile'/f'{ds}.csv';rec=pd.read_csv(recp,low_memory=False);rec=rec[rec.prop_type.isin(PROPS)];keys=['game_id','player_id','prop_type','line'];mc=keys+['bookmaker_key','price_over_american','price_under_american','implied_over_novig','implied_under_novig','snapshot_time_utc'];rec=rec[mc].drop_duplicates(keys)
   slate=slate.drop(columns=['market_bookmaker_key','market_price_over','market_price_under','market_no_vig_implied_over','market_no_vig_implied_under','market_snapshot_time_utc'],errors='ignore').merge(rec,on=keys,how='left',validate='one_to_one')
   payload={};meta={}
   for prop,p in zip(PROPS,required):
    f=pd.read_csv(p,low_memory=False);cols=list(f.columns);req=active_manifest(prop)['registration_payload']['required_feature_order']
    for _,r in f.iterrows():
     k=(int(r.game_id),int(r.player_id),str(r.prop_type),float(r.line));payload[k]={c:clean(r[c]) for c in cols};meta[k]=(str(p),sha(p),'|'.join(c for c in req if c not in cols))
   rows=[];fails=[]
   for _,r in slate.iterrows():
    k=(int(r.game_id),int(r.player_id),str(r.prop_type),float(r.line));base={'game_date':ds,'game_id':k[0],'player_id':k[1],'prop_type':k[2],'line':k[3],'snapshot_run_tag':tag}
    if k not in payload:fails.append({**base,'reason':'strict-prior feature state unavailable'});continue
    if any(pd.isna(r[c]) for c in ['bookmaker_key','price_over_american','price_under_american','implied_over_novig','implied_under_novig']):fails.append({**base,'reason':'exact market reconciliation unavailable'});continue
    mp=active_manifest(k[2])['registration_payload'];feat=payload[k];canon=pl.canonical_json(feat);pr=predict(prop_type=k[2],features=feat);po=float(pr['probability_over']);side=str(pr['predicted_outcome']);ps=po if side=='over' else 1-po;fav='over' if r.implied_over_novig>=r.implied_under_novig else 'under';identity=pl.canonical_json({**base,'bookmaker_key':r.bookmaker_key,'selected_side':side})
    rows.append({'replay_label':'STRICT_AS_OF_HISTORICAL_REPLAY','game_date':ds,'game_id':k[0],'player_id':k[1],'player_name':r.player_name,'team':r.team,'proposition':k[2],'line':k[3],'selected_side':side,'bookmaker':r.bookmaker_key,'price_over_american':r.price_over_american,'price_under_american':r.price_under_american,'selected_side_price':r.price_over_american if side=='over' else r.price_under_american,'selected_side_no_vig_probability':r.implied_over_novig if side=='over' else r.implied_under_novig,'model_selected_side_probability':ps,'model_probability_over':po,'market_favorite_side':fav,'model_market_agreement':side==fav,'archived_snapshot_run_tag':tag,'snapshot_timestamp':r.snapshot_time_utc,'prediction_generation_timestamp':pt.isoformat(),'scheduled_game_start':r.start.isoformat(),'semantic_model_id':mp['semantic_model_id'],'model_artifact_sha256':mp['loaded_artifact_sha256'],'feature_source_path':meta[k][0],'feature_source_sha256':meta[k][1],'canonical_feature_serialization':canon,'feature_vector_sha256':hashlib.sha256(canon.encode()).hexdigest(),'feature_schema_sha256':mp['feature_schema_sha256'],'configuration_sha256':mp['configuration_sha256'],'calibration_identity':mp['calibration_identity']['identity_sha256'],'provenance_source':feat.get('bvp_source'),'fallback_status':'FALLBACK_OR_PROXY' if 'fallback' in str(feat.get('bvp_source','')).lower() else 'DIRECT_OR_PRECOMPUTED','contributing_history_age_status':'NOT_DEFINED_IN_FROZEN_CONTRACT','contributing_history_completeness_status':'NOT_DEFINED_IN_FROZEN_CONTRACT','runtime_defaulted_registered_features':meta[k][2],'canonical_row_identity':identity,'lineage_status':'LINEAGE_CERTIFIED'})
   d=pd.DataFrame(rows);assert not d.canonical_row_identity.duplicated().any();assert (pd.to_datetime(d.prediction_generation_timestamp,utc=True)<pd.to_datetime(d.scheduled_game_start,utc=True)).all();dst=led/f'{ds}.csv';d.to_csv(dst,index=False);pd.DataFrame(fails).to_csv(led/f'{ds}_failures.csv',index=False)
  all_rows.append(d.assign(source_prediction_ledger=str(dst)));files=[dst,led/f'{ds}_failures.csv'];mf=led/f'{ds}_SHA256SUMS.csv';pd.DataFrame([{'file':q.name,'sha256':sha(q),'bytes':q.stat().st_size} for q in files]).to_csv(mf,index=False);manifests.append({'date':ds,'ledger':str(dst),'ledger_sha256':sha(dst),'per_date_manifest':str(mf),'per_date_manifest_sha256':sha(mf),'rows':len(d)})
  date_rows.append({'date':ds,'eligibility_status':'INCLUDED','selected_ordinary_run_tag':tag,'snapshot_timestamp':pt.isoformat(),'games_available':int(slate.game_id.nunique()),'games_pregame_at_snapshot':int(slate.game_id.nunique()),'feature_state_availability':True,'paired_odds_availability':True,'semantic_compatibility':True,'inclusion_or_exclusion_reason':'eligible; rows may fail closed individually'})
  print(ds,len(d))
 datep=OUT/'date_eligibility_manifest.csv';pd.DataFrame(date_rows).to_csv(datep,index=False);agg=pd.concat(all_rows,ignore_index=True,sort=False);assert not agg.canonical_row_identity.duplicated().any();aggp=OUT/'aggregate_frozen_prediction_ledger.csv';agg.to_csv(aggp,index=False)
 pm=OUT/'aggregate_prediction_manifest.csv';pd.DataFrame(manifests).to_csv(pm,index=False);freeze=OUT/'AGGREGATE_PREDICTION_FREEZE_SHA256SUMS.csv';targets=[datep,aggp,pm]+[Path(x['per_date_manifest']) for x in manifests];pd.DataFrame([{'file':str(q.relative_to(OUT)),'sha256':sha(q),'bytes':q.stat().st_size} for q in targets]).to_csv(freeze,index=False)
 auth={'outcome_access_authorized':True,'authorized_at':datetime.now(timezone.utc).isoformat(),'window':[START,END],'eligible_dates':len(manifests),'frozen_rows':len(agg),'aggregate_prediction_ledger_sha256':sha(aggp),'aggregate_prediction_manifest_sha256':sha(pm),'aggregate_freeze_manifest_sha256':sha(freeze),'accepted_july9_ledger_sha256':sha(ACCEPTED),'outcomes_accessed_for_new_dates_before_marker':False};(OUT/'OUTCOME_ACCESS_AUTHORIZATION.json').write_text(json.dumps(auth,indent=2)+'\n');print(json.dumps(auth,indent=2))
if __name__=='__main__':main()
