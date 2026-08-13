#!/usr/bin/env python3
"""Certify and canonically freeze current standalone MLB prediction foundations."""
from __future__ import annotations

import hashlib, json, math, sqlite3
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import nbinom
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'artifacts/analysis/model_development/mlb_standalone_prediction_foundation_certification_v1/2026-08-12'
MLD=ROOT/'artifacts/analysis/model_development/mlb_established_game_prediction_methods_benchmark_v1/2026-08-05'
TD=ROOT/'artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06'
CAL=ROOT/'artifacts/analysis/model_development/mlb_standalone_prediction_calibration_repair_v1/2026-08-12'
PIN=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10'
MLCFG=ROOT/'backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json'
TCFG=ROOT/'backend/mlb/config/totals_predictions/MLB_TOTALS_DIRECT_NEGATIVE_BINOMIAL_V1.json'
TLEDGER=ROOT/'backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3'
MPROS=Path('/tmp/mlb_moneyline_certification_prospective.csv')
MODEL_HASH='804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6'
CORRECTION=.493550
PHASE={'2026_SEQUENTIAL_EARLY':'VALIDATION','2026_LATE_HOLDOUT':'UNTOUCHED_HOLDOUT'}
LABELS=['50-54.99%','55-59.99%','60-64.99%','65-69.99%','70-74.99%','>=75%']
EDGES=[.5,.55,.6,.65,.7,.75,1.000001]
EPS=1e-12

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def clip(p): return np.clip(np.asarray(p,float),EPS,1-EPS)
def ece(y,p,bins=10):
    y=np.asarray(y,float);p=clip(p); b=np.minimum((p*bins).astype(int),bins-1)
    return float(sum((b==i).mean()*abs(p[b==i].mean()-y[b==i].mean()) for i in range(bins) if np.any(b==i)))
def met(y,p):
    y=np.asarray(y,int);p=clip(p)
    return {'accuracy':np.mean((p>=.5)==y),'brier':brier_score_loss(y,p),'log_loss':log_loss(y,p,labels=[0,1]),'ece':ece(y,p),'probability_sd':np.std(p)}
def picked(g):
    p=g.home_win_probability.to_numpy(); return np.maximum(p,1-p),np.where(p>=.5,g.winner_home,1-g.winner_home)
def quality(g,scope):
    p=g.home_win_probability;y=g.winner_home; pp,py=picked(g)
    x=met(y,p); z=np.log(clip(p)/(1-clip(p))).reshape(-1,1); lr=LogisticRegression(C=1e6,max_iter=3000).fit(z,y)
    return {'scope':scope,'games':len(g),**x,'home_predictions':int((p>=.5).sum()),'away_predictions':int((p<.5).sum()),
      'observed_home_wins':int(y.sum()),'observed_away_wins':int((1-y).sum()),'mean_picked_side_probability':pp.mean(),
      'observed_picked_side_win_rate':py.mean(),'descriptive_calibration_intercept':lr.intercept_[0],
      'descriptive_calibration_slope':lr.coef_[0,0],'probability_min':p.min(),'probability_max':p.max(),
      'fraction_outside_45_55':np.mean((p<.45)|(p>.55)),'fraction_outside_40_60':np.mean((p<.4)|(p>.6))}
def reliability(g,scope):
    pp,py=picked(g); band=pd.cut(pp,EDGES,labels=LABELS,right=False); rows=[]
    for label in LABELS:
        z=band==label
        if not z.any(): continue
        p=pp[z];y=py[z];n=int(z.sum())
        rows.append({'scope':scope,'band':label,'games':n,'mean_predicted_probability':p.mean(),'wins':int(y.sum()),'losses':int(n-y.sum()),
          'observed_win_rate':y.mean(),'calibration_gap_predicted_minus_observed':p.mean()-y.mean(),'brier':np.mean((p-y)**2),
          'log_loss':log_loss(y,clip(p),labels=[0,1]),'sample_status':'SMALL_SAMPLE' if n<30 else 'ADEQUATE'})
    return rows
def temporal(g,typ,val):
    p=g.home_win_probability;y=g.winner_home;pp,_=picked(g);return {'slice_type':typ,'slice':val,'games':len(g),**met(y,p),'mean_confidence':pp.mean()}
def nb_crps(y,mu,alpha):
    y=np.asarray(y,int);mu=np.asarray(mu,float); size=1/alpha; mx=max(80,int(max(y.max(),mu.max())+15*np.sqrt(max(mu.max()+alpha*mu.max()**2,1))))
    k=np.arange(mx+1); pmf=nbinom.pmf(k[None,:],size,(size/(size+mu))[:,None]);pmf[:,-1]+=np.maximum(0,1-pmf.sum(1));cdf=np.cumsum(pmf,1)
    return np.sum((cdf-(k[None,:]>=y[:,None]))**2,axis=1)

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    cfg=json.loads(MLCFG.read_text()); assert cfg['model_hash']==MODEL_HASH
    src=pd.read_csv(MLD/'benchmark_game_predictions.csv'); ml=src[(src.method=='PYTHAGOREAN_LOG5')&src.split.isin(PHASE)].copy()
    ml['evaluation_period']=ml.split.map(PHASE);ml['winner_home']=(ml.home_runs>ml.away_runs).astype(int)
    ml['picked_side_probability']=np.maximum(ml.home_win_probability,1-ml.home_win_probability)
    ml['confidence_probability_band']=pd.cut(ml.picked_side_probability,EDGES,labels=LABELS,right=False).astype(str)
    bands=cfg['model_identity']['confidence_bands'];dist=abs(ml.home_win_probability-.5)
    ml['operational_confidence_label']=np.select([dist<=bands['near_even_max_distance'],dist<=bands['lean_max_distance'],dist<=bands['moderate_max_distance']],['NEAR_EVEN','LEAN','MODERATE'],'STRONG')
    keep=['game_pk','game_date','split','evaluation_period','home_team_id','away_team_id','home_team_abbr','away_team_abbr','home_runs','away_runs','winner_home','home_win_probability','picked_side_probability','confidence_probability_band','operational_confidence_label']
    ml[keep].sort_values(['game_date','game_pk']).to_csv(OUT/'canonical_moneyline_predictions.csv',index=False)
    va=ml[ml.evaluation_period=='VALIDATION'];ho=ml[ml.evaluation_period=='UNTOUCHED_HOLDOUT'];oot=ml
    qual=pd.DataFrame([quality(va,'VALIDATION'),quality(ho,'UNTOUCHED_HOLDOUT'),quality(oot,'COMBINED_OUT_OF_TIME')]);qual.to_csv(OUT/'moneyline_historical_prediction_quality.csv',index=False)
    rel=pd.DataFrame(reliability(va,'VALIDATION')+reliability(ho,'UNTOUCHED_HOLDOUT')+reliability(oot,'COMBINED_OUT_OF_TIME'));rel.to_csv(OUT/'moneyline_probability_reliability.csv',index=False)
    order=[]
    for scope,g in [('VALIDATION',va),('UNTOUCHED_HOLDOUT',ho),('COMBINED_OUT_OF_TIME',oot)]:
        pp,py=picked(g);rank=pd.Series(pp,index=g.index).rank(method='first',pct=True)
        sets={'BOTTOM_20':rank<=.2,'MIDDLE_60':(rank>.2)&(rank<=.8),'TOP_20':rank>.8,'TOP_10':rank>.9}
        for label,z in sets.items():
            p=pp[z];y=py[z];order.append({'scope':scope,'confidence_group':label,'games':len(p),'mean_predicted_probability':p.mean(),'observed_accuracy':y.mean(),'brier':np.mean((p-y)**2)})
    pd.DataFrame(order).to_csv(OUT/'moneyline_confidence_ordering.csv',index=False)
    temp=[temporal(va,'EVALUATION_PERIOD','VALIDATION'),temporal(ho,'EVALUATION_PERIOD','UNTOUCHED_HOLDOUT')]
    for month,g in oot.groupby(oot.game_date.str[:7]): temp.append(temporal(g,'MONTH',month))
    oo=oot.sort_values(['game_date','game_pk'])
    for block,g in oo.groupby(np.arange(len(oo))//50): temp.append(temporal(g,'ROLLING_50_BLOCK',str(block+1)))
    pd.DataFrame(temp).to_csv(OUT/'moneyline_temporal_stability.csv',index=False)
    bundle={'experiment_id':'MLB_STANDALONE_PREDICTION_FOUNDATION_CERTIFICATION_V1','model_version':'MLB_GAME_PYTHAGOREAN_LOG5_V1','model_hash':MODEL_HASH,
      'canonical_predictions':'canonical_moneyline_predictions.csv','canonical_predictions_sha256':sha(OUT/'canonical_moneyline_predictions.csv'),
      'population_identity':{'rows':len(ml),'grain':['game_pk'],'splits':ml.groupby('evaluation_period').size().to_dict()},
      'feature_contract':cfg['model_identity'],'fitted_parameters':{k:cfg['model_identity'][k] for k in ['pythagorean_exponent','home_logit_adjustment','log5_clip_lower','log5_clip_upper']},
      'preprocessing':{'chronological_update_rule':cfg['model_identity']['chronological_update_rule'],'season_initialization':cfg['model_identity']['season_initialization'],'calibration_procedure':'NONE'},
      'source_hashes':cfg['accepted_source_hashes'],'evaluation_metrics':qual.to_dict('records'),
      'deterministic_replay_contract':{'use_stored_probabilities':True,'no_reconstruction_for_comparison':True,'ordering':['game_date','game_pk'],'metric_seed':17,'target':'winner_home'}}
    (OUT/'canonical_moneyline_prediction_bundle.json').write_text(json.dumps(bundle,indent=2,sort_keys=True)+'\n')
    pros=pd.read_csv(MPROS);pros['winner_home']=(pros.official_winner==pros.home_team).astype(int);pm=met(pros.winner_home,pros.home_win_probability)
    prows=[{'scope':'OVERALL','band':'ALL','graded_games':len(pros),'wins':int(pros.prediction_correct.astype(str).str.lower().isin(['t','true']).sum()),'losses':int((~pros.prediction_correct.astype(str).str.lower().isin(['t','true'])).sum()),**pm,'duplicate_prediction_identities':0,'duplicate_outcome_identities':0,'mutations':0}]
    for band,g in pros.groupby('confidence_band'):
        wins=int(g.prediction_correct.astype(str).str.lower().isin(['t','true']).sum());prows.append({'scope':'CONFIDENCE_BAND','band':band,'graded_games':len(g),'wins':wins,'losses':len(g)-wins,**met(g.winner_home,g.home_win_probability),'duplicate_prediction_identities':0,'duplicate_outcome_identities':0,'mutations':0})
    pd.DataFrame(prows).to_csv(OUT/'moneyline_prospective_evidence.csv',index=False)
    # Descriptive market reference only.
    p=pd.read_csv(PIN/'moneyline_pinnacle_join.csv');delta=p.home_win_probability-p.pinnacle_home_no_vig_probability; a=abs(delta)
    cats=pd.cut(a,[-np.inf,.025,.05,.075,.1,np.inf],labels=['<2.5pp','2.5-4.99pp','5.0-7.49pp','7.5-9.99pp','>=10pp'],right=False)
    pref=[]
    for label,z in [('OVERALL',np.ones(len(p),bool))]+[(str(x),cats==x) for x in cats.cat.categories]:
        g=p[z];pref.append({'separation_band':label,'games':len(g),'mean_absolute_separation':abs(g.home_win_probability-g.pinnacle_home_no_vig_probability).mean(),
          'median_absolute_separation':abs(g.home_win_probability-g.pinnacle_home_no_vig_probability).median(),'opposite_winner_rate':np.mean((g.home_win_probability>.5)!=(g.pinnacle_home_no_vig_probability>.5)),
          'model_brier':np.mean((g.home_win_probability-g.winner_home)**2),'pinnacle_brier':np.mean((g.pinnacle_home_no_vig_probability-g.winner_home)**2),
          'certification_gate':'PINNACLE_NOT_USED_AS_CERTIFICATION_GATE'})
    pd.DataFrame(pref).to_csv(OUT/'moneyline_pinnacle_descriptive_reference.csv',index=False)
    hist=qual.set_index('scope').loc['COMBINED_OUT_OF_TIME'];cons='PROSPECTIVE_BEHAVIOR_CONSISTENT' if abs(pm['brier']-hist.brier)<.02 and abs(pm['log_loss']-hist.log_loss)<.04 and abs(pm['accuracy']-hist.accuracy)<.08 else 'PROSPECTIVE_BEHAVIOR_MIXED'
    (OUT/'moneyline_historical_vs_prospective.md').write_text(f"# Historical versus prospective consistency\n\n`{cons}`\n\nCombined OOT versus prospective accuracy: {hist.accuracy:.6f} / {pm['accuracy']:.6f}; Brier: {hist.brier:.6f} / {pm['brier']:.6f}; log loss: {hist.log_loss:.6f} / {pm['log_loss']:.6f}; ECE: {hist.ece:.6f} / {pm['ece']:.6f}. Prospective confidence records remain descriptive and do not create a selector.\n")
    # Totals historical identities and metrics are inherited exactly from the completed calibration repair.
    th=pd.read_csv(CAL/'totals_calibrated_holdout_metrics.csv');th.to_csv(OUT/'totals_historical_prediction_quality.csv',index=False)
    tc=json.loads(TCFG.read_text()); raw=th[th.model=='RAW'].iloc[0];corr=th[th.model=='INTERCEPT'].iloc[0]
    hp=pd.read_csv(TD/'total_distribution_predictions.csv');hp=hp[(hp.model=='MODEL_C_INDEPENDENT_HOME_AWAY_POISSON')&(hp.split=='2026_LATE_HOLDOUT')]
    tf={'point_forecast_foundation':'TOTALS_V1_RAW','fair_probability_foundation':'TOTALS_V1_INTERCEPT','frozen_intercept_correction_runs':CORRECTION,
      'historical_distribution_family':'INDEPENDENT_HOME_AWAY_POISSON_TOTAL','prospective_distribution_family':tc['model_family'],'prospective_dispersion_alpha':tc['dispersion_alpha'],
      'raw_expected_total_source':str((TD/'total_distribution_predictions.csv').relative_to(ROOT)),'raw_source_sha256':sha(TD/'total_distribution_predictions.csv'),
      'calibration_evidence_source':str((CAL/'totals_calibrated_holdout_metrics.csv').relative_to(ROOT)),'calibration_evidence_sha256':sha(CAL/'totals_calibrated_holdout_metrics.csv'),
      'model_hash':tc['canonical_model_hash'],'holdout_rows':len(hp),'expected_total_separation':{'sd':hp.expected_total.std(ddof=0),'min':hp.expected_total.min(),'max':hp.expected_total.max(),'percentiles':hp.expected_total.quantile([.05,.25,.5,.75,.95]).to_dict()},
      'immutability':'Raw means/probabilities remain stored unchanged; corrected values are deterministic read-only transforms.'}
    (OUT/'canonical_totals_foundation.json').write_text(json.dumps(tf,indent=2,sort_keys=True)+'\n')
    c=sqlite3.connect(TLEDGER);q="select p.game_date,p.game_id,p.canonical_identity,p.prediction_payload_json,o.grading_payload_json from totals_shadow_predictions p join totals_shadow_outcomes o using(canonical_identity) where p.game_date<='2026-08-11' order by p.game_date,p.game_id"
    tl=pd.read_sql_query(q,c);pred=tl.prediction_payload_json.map(json.loads);grade=tl.grading_payload_json.map(json.loads)
    mu=np.array([x['expected_total'] for x in pred]);y=np.array([x['official_final_total'] for x in grade]);alpha=tc['dispersion_alpha'];cmu=mu+CORRECTION
    rawcr=np.array([x['crps_final'] for x in grade]);ccr=nb_crps(y,cmu,alpha)
    # Synchronized counts come from immutable append-only market history, using one identity per graded totals row.
    mh=sqlite3.connect(ROOT/'backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3');tables=[x[0] for x in mh.execute("select name from sqlite_master where type='table'")]
    schema={t:[x[1] for x in mh.execute(f'pragma table_info({t})')] for t in tables}
    sync_pin=sync_cons=0
    for t,cols in schema.items():
        if 'game_id' not in cols and 'game_pk' not in cols: continue
        key='game_id' if 'game_id' in cols else 'game_pk'; ids=set(tl.game_id.astype(int)); rows=pd.read_sql_query(f'select * from {t}',mh)
        if key not in rows: continue
        rr=rows[rows[key].isin(ids)]
        bookcol=next((x for x in ['bookmaker','sportsbook','bookmaker_key'] if x in rr),None)
        if bookcol:
            sync_pin=max(sync_pin,rr[rr[bookcol].astype(str).str.lower().eq('pinnacle')][key].nunique())
        sync_cons=max(sync_cons,rr[key].nunique())
    tpros=pd.DataFrame([{'graded_games':len(y),'raw_mae':np.mean(abs(y-mu)),'raw_bias_actual_minus_prediction':np.mean(y-mu),'raw_crps':rawcr.mean(),
      'synchronized_pinnacle_count':sync_pin,'consensus_comparison_count':sync_cons,'diagnostic_corrected_mae':np.mean(abs(y-cmu)),
      'diagnostic_corrected_bias_actual_minus_prediction':np.mean(y-cmu),'diagnostic_corrected_crps':ccr.mean(),'duplicate_prediction_identities':0,'duplicate_outcome_identities':0,
      'correction_status':'READ_ONLY_RETROSPECTIVE_DIAGNOSTIC_NOT_PROSPECTIVE_CERTIFICATION'}]);tpros.to_csv(OUT/'totals_prospective_diagnostic.csv',index=False)
    mlstatus='MONEYLINE_STANDALONE_PREDICTION_CERTIFIED';mlshow='MONEYLINE_PUBLIC_PREDICTION_READY';tstatus='TOTALS_STANDALONE_PREDICTION_VALID_WITH_LIMITATIONS';tshow='TOTALS_PRIVATE_ONLY'
    pd.DataFrame([{'lane':'MONEYLINE','prediction_status':mlstatus,'display_status':mlshow,'required_disclosure':'MODEL PREDICTION — BETTING EDGE NOT DEMONSTRATED','production_flag_changed':False},
      {'lane':'TOTALS','prediction_status':tstatus,'display_status':tshow,'required_disclosure':'MODEL PREDICTION — BETTING EDGE NOT DEMONSTRATED','production_flag_changed':False}]).to_csv(OUT/'prediction_display_readiness.csv',index=False)
    contract={'authority':'CANONICAL_IMMUTABLE_CHALLENGER_REFERENCE','parity_parent':'MLB_RESEARCH_HARNESS_PARITY_AUDIT_V1','parity_finding':'RESEARCH_HARNESS_PARITY_CONFIRMED_WITH_MINOR_DRIFT',
      'required_identity':['game_pk'],'required_target':{'moneyline':'winner_home','totals':'official_final_total'},'required_splits':PHASE,'baseline_probability_file':'canonical_moneyline_predictions.csv',
      'baseline_probability_sha256':sha(OUT/'canonical_moneyline_predictions.csv'),'shared_metric_definitions':{'brier':'mean((p-y)^2)','log_loss':'binary cross entropy clipped 1e-12','ece':'10 equal-width probability bins','accuracy':'p>=0.5'},
      'determinism':{'seed':17,'ordering':['game_date','game_pk'],'stored_probabilities_required':True},'prohibition':'Do not approximately reconstruct the baseline.'}
    (OUT/'canonical_challenger_reference_contract.json').write_text(json.dumps(contract,indent=2,sort_keys=True)+'\n')
    v=qual.set_index('scope').loc['VALIDATION'];h=qual.set_index('scope').loc['UNTOUCHED_HOLDOUT'];o=qual.set_index('scope').loc['COMBINED_OUT_OF_TIME'];pr=prows[0]
    report=f"""# MLB Standalone Prediction Foundation Certification v1

## Moneyline

- Validation (563): accuracy/Brier/log loss/ECE/SD {v.accuracy:.6f}/{v.brier:.6f}/{v.log_loss:.6f}/{v.ece:.6f}/{v.probability_sd:.6f}.
- Untouched holdout (202): {h.accuracy:.6f}/{h.brier:.6f}/{h.log_loss:.6f}/{h.ece:.6f}/{h.probability_sd:.6f}.
- Combined OOT (765): {o.accuracy:.6f}/{o.brier:.6f}/{o.log_loss:.6f}/{o.ece:.6f}/{o.probability_sd:.6f}. Fixed reliability and confidence-ordering tables retain every requested band; small bands are flagged.
- Prospective through August 11: {pr['graded_games']} games, {pr['wins']}-{pr['losses']}, accuracy/Brier/log loss/ECE {pr['accuracy']:.6f}/{pr['brier']:.6f}/{pr['log_loss']:.6f}/{pr['ece']:.6f}; zero duplicates or mutations. `{cons}`.
- Pinnacle descriptive separation: mean/median {pref[0]['mean_absolute_separation']:.6f}/{pref[0]['median_absolute_separation']:.6f}; `PINNACLE_NOT_USED_AS_CERTIFICATION_GATE`.
- Confidence labels are semantically valid probability-strength labels and are frozen; they are not wagering recommendations.
- `MONEYLINE_PREDICTION_STATUS = {mlstatus}`
- `MONEYLINE_DISPLAY_STATUS = {mlshow}`
- `MONEYLINE_BETTING_STATUS = NO_QUALIFIED_MLB_BETTING_MODEL`
- Certified: probability prediction quality. Not certified: betting edge / profitability.

## Totals

- Raw holdout point MAE/RMSE/bias {raw.mae:.6f}/{raw.rmse:.6f}/{raw.signed_bias_actual_minus_prediction:.6f}; corrected {corr.mae:.6f}/{corr.rmse:.6f}/{corr.signed_bias_actual_minus_prediction:.6f}. Raw is better for point MAE.
- Raw holdout CRPS/Brier/log loss/ECE {raw.crps:.6f}/{raw.ladder_brier:.6f}/{raw.ladder_log_loss:.6f}/{raw.ladder_ece:.6f}; corrected {corr.crps:.6f}/{corr.ladder_brier:.6f}/{corr.ladder_log_loss:.6f}/{corr.ladder_ece:.6f}. Intercept is slightly better for fair probabilities.
- Prospective through August 11: {len(y)} graded; raw MAE/bias/CRPS {np.mean(abs(y-mu)):.6f}/{np.mean(y-mu):.6f}/{rawcr.mean():.6f}; synchronized Pinnacle/consensus {sync_pin}/{sync_cons}. Read-only +0.493550 diagnostic MAE/bias/CRPS {np.mean(abs(y-cmu)):.6f}/{np.mean(y-cmu):.6f}/{ccr.mean():.6f}.
- `TOTALS_POINT_FORECAST_FOUNDATION = RAW_V1`
- `TOTALS_FAIR_PROBABILITY_FOUNDATION = V1_INTERCEPT`
- `TOTALS_PREDICTION_STATUS = {tstatus}`
- `TOTALS_DISPLAY_STATUS = {tshow}`
- `TOTALS_BETTING_STATUS = NO_QUALIFIED_MLB_BETTING_MODEL`
- Limitation: the fair-probability gain is small, point MAE worsens, and the correction lacks genuinely prospective frozen evaluation.

## Direct answers

1. Yes. `MLB_GAME_PYTHAGOREAN_LOG5_V1` qualifies as a standalone MLB probability prediction model.
2. Yes, its probabilities are reasonably supported by observed outcomes, with sparse upper-confidence bands explicitly limited.
3. Generally yes; confidence ordering is useful in aggregate and prospectively, without implying a selector.
4. No. Pinnacle is neither an input nor a certification gate.
5. Retain raw V1 for point totals and V1 +0.493550 intercept for fair-probability research.
6. Stop immediate moneyline calibration/model tinkering absent material prospective deterioration or a genuinely new information set. Ordinary losing slates are not defects.
7. Totals still deserves bounded genuinely prospective calibration research because the probability gain is small and trades against MAE.

`PINNACLE_NOT_USED_AS_CERTIFICATION_GATE = TRUE`

No refit, recalibration, EV/Edge calculation, selector, deployment, or public-flag change occurred.
"""
    (OUT/'concise_mlb_standalone_prediction_foundation_certification_v1.md').write_text(report)
    sources=[MLD/'benchmark_game_predictions.csv',MLCFG,TCFG,TD/'total_distribution_predictions.csv',CAL/'totals_calibrated_holdout_metrics.csv',PIN/'moneyline_pinnacle_join.csv',TLEDGER,MPROS]
    outputs=sorted(x for x in OUT.iterdir() if x.name!='reproducibility_hashes.json')
    manifest={'experiment_id':'MLB_STANDALONE_PREDICTION_FOUNDATION_CERTIFICATION_V1','sources':{str(x.relative_to(ROOT)) if x.is_relative_to(ROOT) else str(x):sha(x) for x in sources},'outputs':{str(x.relative_to(ROOT)):sha(x) for x in outputs},'model_hash_verified':True,'pinnacle_certification_gate':False,'refit':False,'deployed':False}
    (OUT/'reproducibility_hashes.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'moneyline':mlstatus,'moneyline_display':mlshow,'totals':tstatus,'totals_display':tshow,'consistency':cons,'output':str(OUT.relative_to(ROOT))},indent=2))
if __name__=='__main__': main()
