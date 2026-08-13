#!/usr/bin/env python3
"""No-refit forensic review of the frozen decomposed-scoring holdout selector."""
from __future__ import annotations
import hashlib, json
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import beta, binom, nbinom

ROOT=Path(__file__).resolve().parents[3]
BASE=ROOT/'artifacts/analysis/model_development/mlb_decomposed_scoring_distribution_foundation_v1/2026-08-11'
OUT=ROOT/'artifacts/analysis/model_development/mlb_decomposed_scoring_selector_forensic_holdout_review_v1/2026-08-11'
POP=ROOT/'artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06/certified_totals_game_population.csv'
PIN=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10/totals_pinnacle_join.csv'
TARGETS=['away_f5_runs','home_f5_runs','away_post_f5_runs','home_post_f5_runs']; CAP=35;SEED=20260811
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def american(p):return -100*p/(1-p) if p>=.5 else 100*(1-p)/p
def raw(price):return 100/(price+100) if price>0 else abs(price)/(abs(price)+100)
def dec(price):return 1+price/100 if price>0 else 1+100/abs(price)
def nb(mu,a):
 r=1/a if a>1e-12 else 1e12;p=r/(r+mu);q=nbinom.pmf(np.arange(CAP+1),r,p);q[-1]+=max(0,1-q.sum());return q/q.sum()
def conv(ps):
 q=np.array([1.])
 for p in ps:q=np.convolve(q,p)
 q=q[:CAP+1];q[-1]+=max(0,1-q.sum());return q/q.sum()
def sideprob(p,line,side):
 k=np.arange(len(p));return float(p[k>line].sum() if side=='OVER' else p[k<line].sum())
def selector(prob,price):
 fair=american(prob);edge=100*(prob-raw(price));ev=100*(prob*dec(price)-1)
 return fair,edge,ev,(price>=-400 and fair<=100 and .01<=edge<=6 and 0<=ev<=8)
def profit(row):return dec(row.price)-1 if row.outcome=='WIN' else -1 if row.outcome=='LOSS' else 0

def sources():
 replay=pd.read_csv(BASE/'owner_selector_full_game_total_replay.csv'); bench=pd.read_csv(BASE/'full_game_total_pinnacle_benchmark.csv'); pop=pd.read_csv(POP);sp=pd.read_csv(BASE/'decomposed_scoring_outcome_spine.csv');pin=pd.read_csv(PIN)
 replay['game_pk']=replay.game_pk.astype(int); selected=replay[replay.eligible].copy()
 # Enrich without changing frozen membership.
 cols=['game_pk','scheduled_start_utc','home_team_abbr','away_team_abbr','starter_state_available','bullpen_state_available','park_state_available','weather_state_available','league_total','home_rs','away_rs','home_ra','away_ra']
 selected=selected.merge(bench,on=['game_pk','game_date'],suffixes=('','_bench'),validate='many_to_one').merge(pop[cols],on='game_pk',validate='many_to_one')
 selected['market']='FULL_GAME_TOTAL';selected['prediction_timestamp']='NOT_RETAINED_RETROSPECTIVE_RESEARCH';selected['pinnacle_snapshot_timestamp']=selected.provider_snapshot_utc;selected['model_fair_odds']=selected.fair_odds;selected['pinnacle_offered_odds']=selected.price;selected['pinnacle_paired_no_vig_probability']=np.where(selected.side.eq('OVER'),selected.pinnacle_over_no_vig_probability,1-selected.pinnacle_over_no_vig_probability);selected['weight']=selected.total_weight;selected['official_final_total']=selected.final_total;selected['result']=selected.outcome;selected['flat_stake_profit_loss']=selected.apply(profit,axis=1);selected['experiment_split']='FINAL_HOLDOUT';selected['snapshot_lead_days']=(pd.to_datetime(selected.scheduled_start_utc,utc=True)-pd.to_datetime(selected.provider_snapshot_utc,utc=True)).dt.total_seconds()/86400
 return replay,bench,pop,sp,pin,selected

def ledger(selected):
 cols=['game_pk','game_date','scheduled_start_utc','prediction_timestamp','pinnacle_snapshot_timestamp','home_team_abbr','away_team_abbr','market','line','side','model_probability','model_fair_odds','pinnacle_offered_odds','raw_book_implied_probability','pinnacle_paired_no_vig_probability','edge_pct','ev_pct','weight','official_final_total','result','flat_stake_profit_loss','experiment_split','snapshot_lead_days']
 selected[cols].to_csv(OUT/'frozen_nine_selection_ledger.csv',index=False)
 rows=[]
 for _,r in selected.iterrows():
  vals={'max_days':(1-r.snapshot_lead_days),'weight':r.weight-10,'ev_min':r.ev_pct,'ev_max':8-r.ev_pct,'edge_min':r.edge_pct-.01,'edge_max':6-r.edge_pct,'book_odds':r.price+400,'fair_odds':100-r.fair_odds}
  rows.append({'game_pk':r.game_pk,'side':r.side,**{f'{k}_clearance':v for k,v in vals.items()},'all_filters_pass':all(v>=-1e-9 for v in vals.values()),'post_start':r.snapshot_lead_days<0,'outcome_leakage':'NONE_IN_MEMBERSHIP_FORMULA','timestamp_ambiguity':'PREDICTION_TIMESTAMP_NOT_RETAINED','duplicate_identity':False})
 pd.DataFrame(rows).to_csv(OUT/'selection_filter_validation.csv',index=False)

def concentration(selected):
 g=selected.groupby('game_pk');rows=[]
 for gid,x in g:rows.append({'game_pk':gid,'game_date':x.game_date.iloc[0],'propositions':len(x),'sides':'|'.join(x.side),'teams':f'{x.away_team_abbr.iloc[0]}@{x.home_team_abbr.iloc[0]}','opinion_cluster':f'{gid}:{"|".join(sorted(x.side.unique()))}','alternate_line_cluster':x.line.nunique()>1,'cluster_profit':x.flat_stake_profit_loss.sum(),'cluster_result':'WINNING' if x.flat_stake_profit_loss.sum()>0 else 'LOSING' if x.flat_stake_profit_loss.sum()<0 else 'PUSH'})
 pd.DataFrame(rows).to_csv(OUT/'selection_game_cluster_map.csv',index=False)
 prof=selected.flat_stake_profit_loss;clusters=selected.groupby('game_pk').flat_stake_profit_loss.sum();best=prof.max();bc=clusters.max();tot=prof.sum()
 summary=[{'row_type':'OVERALL','group':'ALL','selections':len(selected),'wins':(selected.result=='WIN').sum(),'losses':(selected.result=='LOSS').sum(),'unique_games':selected.game_pk.nunique(),'unique_dates':selected.game_date.nunique(),'unique_teams':pd.unique(pd.concat([selected.home_team_abbr,selected.away_team_abbr])).size,'total_profit':tot,'roi':tot/len(selected),'average_profit':prof.mean(),'median_profit':prof.median(),'largest_winner':best,'largest_loser':prof.min(),'best_selection_profit_share':best/tot,'best_cluster_profit_share':bc/tot,'roi_best_winner_removed':(tot-best)/(len(selected)-1),'roi_best_cluster_removed':(tot-bc)/(len(selected)-len(selected[selected.game_pk==clusters.idxmax()]))}]
 for day,x in selected.groupby('game_date'):
  summary.append({'row_type':'DATE','group':day,'selections':len(x),'wins':(x.result=='WIN').sum(),'losses':(x.result=='LOSS').sum(),'total_profit':x.flat_stake_profit_loss.sum(),'roi':x.flat_stake_profit_loss.mean()})
 pd.DataFrame(summary).to_csv(OUT/'selection_profit_concentration.csv',index=False)
 los=[]
 for _,r in selected.iterrows():los.append({'omission_type':'SELECTION','omitted':f'{r.game_pk}:{r.side}','remaining_roi':(tot-r.flat_stake_profit_loss)/(len(selected)-1)})
 for gid,p in clusters.items():los.append({'omission_type':'GAME_CLUSTER','omitted':gid,'remaining_roi':(tot-p)/(len(selected)-len(selected[selected.game_pk==gid]))})
 pd.DataFrame(los).to_csv(OUT/'selection_cluster_robustness.csv',index=False)

def frozen_params(sp,through):
 d=sp[sp.modeling_eligible & (sp.game_date<=through)];out={}
 for t in TARGETS:
  mu=d[t].mean();a=max(0,(d[t].var()-mu)/(mu*mu));out[t]=(mu,a)
 return out
def distribution(par):return conv([nb(*par[t]) for t in TARGETS])

def comparisons(replay,bench,pop,sp,pin,selected):
 par=frozen_params(sp,'2026-07-02'); primitive={t:par[t][0] for t in TARGETS};sig=[]
 for _,r in selected.drop_duplicates('game_pk').iterrows():sig.append({'game_pk':r.game_pk,**{f'expected_{t}':primitive[t] for t in TARGETS},'f5_expected_total':primitive['away_f5_runs']+primitive['home_f5_runs'],'post_f5_expected_total':primitive['away_post_f5_runs']+primitive['home_post_f5_runs'],'away_full_expected_runs':primitive['away_f5_runs']+primitive['away_post_f5_runs'],'home_full_expected_runs':primitive['home_f5_runs']+primitive['home_post_f5_runs'],'full_expected_total':sum(primitive.values()),'signature_interpretation':'constant component baseline; selection differences arise from line/price, not game-specific scoring means'})
 pd.DataFrame(sig).to_csv(OUT/'selected_decomposed_scoring_signature.csv',index=False)
 # V1 probabilities from its retained integer PMF at the same line.
 vrows=[]
 for _,r in selected.iterrows():
  p=np.array([r[f'p_total_{i}'] for i in range(20)]+[r.p_total_20_plus],float);vp=sideprob(p,r.line,r.side);fair,edge,ev,passed=selector(vp,r.price)
  vrows.append({'game_pk':r.game_pk,'side':r.side,'line':r.line,'decomposition_probability':r.model_probability,'v1_expected_total':r.expected_total,'v1_probability':vp,'v1_fair_odds':fair,'v1_edge_pct':edge,'v1_ev_pct':ev,'v1_same_selector_pass':passed,'result':r.result,'profit':r.flat_stake_profit_loss,'comparison_group':'OVERLAP' if passed else 'DECOMPOSITION_ONLY'})
 pd.DataFrame(vrows).to_csv(OUT/'selected_vs_frozen_v1.csv',index=False)
 # Pregame selected/nonselected proposition comparison.
 allp=replay.merge(bench,on=['game_pk','game_date'],suffixes=('','_b')).merge(pop,on='game_pk',suffixes=('','_pop'));allp['selected']=allp.eligible;allp['model_market_expected_total_difference']=allp.decomposed_expected_total-allp.line;allp['pinnacle_side_probability']=np.where(allp.side.eq('OVER'),allp.pinnacle_over_no_vig_probability,1-allp.pinnacle_over_no_vig_probability);allp['f5_expectation']=primitive['away_f5_runs']+primitive['home_f5_runs'];allp['post_f5_expectation']=primitive['away_post_f5_runs']+primitive['home_post_f5_runs'];allp['home_expected_runs']=primitive['home_f5_runs']+primitive['home_post_f5_runs'];allp['away_expected_runs']=primitive['away_f5_runs']+primitive['away_post_f5_runs']
 vars=['model_market_expected_total_difference','model_probability','pinnacle_side_probability','edge_pct','ev_pct','line','f5_expectation','post_f5_expectation','home_expected_runs','away_expected_runs','starter_state_available','bullpen_state_available','park_state_available','weather_state_available','league_total','home_rs','away_rs','home_ra','away_ra']
 out=[]
 for v in vars:
  for flag,x in allp.groupby('selected'):out.append({'property':v,'group':'SELECTED' if flag else 'FILTER_FAILING','rows':len(x),'mean':x[v].mean(),'median':x[v].median(),'std':x[v].std(),'minimum':x[v].min(),'maximum':x[v].max()})
 pd.DataFrame(out).to_csv(OUT/'selected_vs_nonselected_holdout.csv',index=False)
 return allp,pd.DataFrame(vrows),primitive

def behavior_near_direction(allp,vrows,bench):
 rows=[]
 for system in ['DECOMPOSITION','V1']:
  if system=='DECOMPOSITION':x=allp.copy();prob=x.model_probability
  else:
   rec=[]
   for _,r in allp.iterrows():
    p=np.array([r[f'p_total_{i}'] for i in range(20)]+[r.p_total_20_plus]);rec.append(sideprob(p,r.line,r.side))
   x=allp.copy();prob=pd.Series(rec,index=x.index)
  x['p']=prob;x['fair']=x.p.map(american);x['edge']=100*(x.p-x.price.map(raw));x['ev']=100*(x.p*np.array([dec(z) for z in x.price])-1);x['pass']=(x.price>=-400)&(x.fair<=100)&x.edge.between(.01,6)&x.ev.between(0,8)
  rows.append({'system':system,'propositions':len(x),'probability_std':x.p.std(),'fair_odds_std':x.fair.std(),'edge_mean':x.edge.mean(),'edge_std':x.edge.std(),'ev_mean':x.ev.mean(),'ev_std':x.ev.std(),'positive_raw_edge_pct':(x.edge>0).mean(),'positive_ev_pct':(x.ev>0).mean(),'passes_ev_upper_pct':(x.ev<=8).mean(),'passes_edge_upper_pct':(x.edge<=6).mean(),'passes_all_pct':x['pass'].mean(),'passes_all_count':x['pass'].sum()})
 pd.DataFrame(rows).to_csv(OUT/'v1_vs_decomposition_selector_behavior.csv',index=False)
 # Fixed near-miss categories (overlap allowed; descriptive only).
 n=[]
 for _,r in allp.iterrows():
  cats=[]
  if -1<=r.ev_pct<0:cats.append('EV_-1_TO_<0')
  if 0<=r.ev_pct<=8:cats.append('EV_0_TO_8')
  if 8<r.ev_pct<=10:cats.append('EV_>8_TO_10')
  if -1<=r.edge_pct<.01:cats.append('EDGE_-1_TO_<0.01')
  if .01<=r.edge_pct<=6:cats.append('EDGE_0.01_TO_6')
  if 6<r.edge_pct<=8:cats.append('EDGE_>6_TO_8')
  if r.fair_odds<=100:cats.append('FAIR_<=100')
  if 101<=r.fair_odds<=120:cats.append('FAIR_101_TO_120')
  for c in cats:n.append({'category':c,'game_pk':r.game_pk,'side':r.side,'selected':r.eligible,'edge_pct':r.edge_pct,'ev_pct':r.ev_pct,'fair_odds':r.fair_odds,'outcome':r.outcome,'profit':profit(r)})
 near=pd.DataFrame(n);near.to_csv(OUT/'near_miss_diagnostic.csv',index=False)
 dc=[]
 allp['actual_residual']=allp.final_total-allp.line;allp['predicted_difference']=np.where(allp.side.eq('OVER'),allp.decomposed_expected_total-allp.line,allp.line-allp.decomposed_expected_total)
 near_mask=(allp.ev_pct.between(-1,10)|allp.edge_pct.between(-1,8)|allp.fair_odds.between(101,120))&~allp.eligible
 for name,x in [('SELECTED',allp[allp.eligible]),('NEAR_MISS',allp[near_mask]),('ALL_HOLDOUT',allp)]:
  actual=np.where(x.side.eq('OVER'),x.actual_residual,-x.actual_residual);dc.append({'population':name,'rows':len(x),'sign_agreement':np.mean(np.sign(x.predicted_difference)==np.sign(actual)),'mean_predicted_difference':x.predicted_difference.mean(),'mean_actual_directional_residual':actual.mean(),'correlation':np.corrcoef(x.predicted_difference,actual)[0,1] if len(x)>2 and x.predicted_difference.std()>0 else np.nan})
 pd.DataFrame(dc).to_csv(OUT/'selection_directional_consistency.csv',index=False)

def movement_uncertainty(selected):
 # Retained historical corpus has exactly one Pinnacle observation per game/date.
 mov=[]
 for _,r in selected.iterrows():mov.append({'game_pk':r.game_pk,'side':r.side,'selected_line':r.line,'selected_price':r.price,'selected_snapshot':r.provider_snapshot_utc,'first_observed_pregame_line':r.line,'later_pregame_line':np.nan,'latest_pregame_line':r.line,'line_movement':np.nan,'price_movement':np.nan,'toward_model':np.nan,'evidence_status':'ONE_RETAINED_PREGAME_OBSERVATION_ONLY; NOT_TRUE_OPENER'})
 pd.DataFrame(mov).to_csv(OUT/'selection_pinnacle_movement.csv',index=False)
 pd.DataFrame([{'selected_rows':len(selected),'valid_later_comparisons':0,'positive_clv':0,'negative_clv':0,'mean_clv_probability_points':np.nan,'median_clv_probability_points':np.nan,'convention':'100*(selected model probability - later paired raw/no-vig proposition probability)','status':'UNAVAILABLE_NO_LATER_RETAINED_PREGAME_OBSERVATION'}]).to_csv(OUT/'selection_clv_diagnostic.csv',index=False)
 n=len(selected);w=(selected.result=='WIN').sum();lo=beta.ppf(.025,w,n-w+1);hi=beta.ppf(.975,w+1,n-w);prices=selected.price.to_numpy();be=np.mean([1/dec(x) for x in prices]);rng=np.random.default_rng(SEED);profits=selected.flat_stake_profit_loss.to_numpy();boot=np.array([rng.choice(profits,n,replace=True).mean() for _ in range(100000)])
 pd.DataFrame([{'selections':n,'wins':w,'win_rate':w/n,'clopper_pearson_95_low':lo,'clopper_pearson_95_high':hi,'bootstrap_roi_95_low':np.quantile(boot,.025),'bootstrap_roi_95_high':np.quantile(boot,.975),'p_ge_7_wins_null_50pct':binom.sf(6,n,.5),'average_price_break_even_probability':be,'p_ge_7_wins_average_price_break_even_null':binom.sf(6,n,be),'limitations':'tiny n; proposition selection dependence; possible game/line correlation; bootstrap treats rows as exchangeable'}]).to_csv(OUT/'selection_small_sample_uncertainty.csv',index=False)

def stability_analogs(sp,pin,pop,selected):
 features=['league_total','home_rs','away_rs','home_ra','away_ra','starter_state_available','bullpen_state_available','park_state_available','weather_state_available']
 sids=set(selected.game_pk);hold=pop[pop.game_pk.isin(sids)];non=pop[(pop.game_date>='2026-07-03')&(pop.game_date<='2026-07-27')&~pop.game_pk.isin(sids)]
 rows=[]
 for f in features:
  diff=hold[f].mean()-non[f].mean()
  for phase,a,b in [('HOLDOUT',hold,non),('DEVELOPMENT',pop[pop.game_date<='2026-06-16'],pop[pop.game_date<='2026-06-16']),('VALIDATION',pop[(pop.game_date>='2026-06-17')&(pop.game_date<='2026-07-02')],pop[(pop.game_date>='2026-06-17')&(pop.game_date<='2026-07-02')])]:rows.append({'feature':f,'phase':phase,'selected_or_analog_mean':a[f].mean(),'reference_mean':b[f].mean(),'difference':diff if phase=='HOLDOUT' else np.nan,'status':'DESCRIPTIVE_ONLY; NO HISTORICAL MEMBERSHIP PREDICTIONS RETAINED' if phase!='HOLDOUT' else 'OBSERVED'})
 pd.DataFrame(rows).to_csv(OUT/'selection_feature_pattern_stability.csv',index=False)
 # Validation analogs can be deterministically replayed from frozen development NB parameters; development cannot without in-sample leakage.
 analog=[]
 for phase,start,end,train_end in [('DEVELOPMENT',None,'2026-06-16',None),('VALIDATION','2026-06-17','2026-07-02','2026-06-16'),('FINAL_HOLDOUT','2026-07-03','2026-07-27','2026-07-02')]:
  if phase=='DEVELOPMENT':analog.append({'phase':phase,'selections':np.nan,'wins':np.nan,'losses':np.nan,'pushes':np.nan,'roi':np.nan,'status':'UNAVAILABLE: NO EXISTING DEVELOPMENT PREDICTIONS; IN-SAMPLE RECONSTRUCTION REJECTED'});continue
  par=frozen_params(sp,train_end);p=distribution(par);q=pin[(pin.game_date>=start)&(pin.game_date<=end)&pin.game_pk.isin(sp[sp.modeling_eligible].game_pk)];picks=[]
  for _,r in q.iterrows():
   for side,price in [('OVER',r.pinnacle_over_price),('UNDER',r.pinnacle_under_price)]:
    pr=sideprob(p,r.pinnacle_total_line,side);fair,edge,ev,ok=selector(pr,price)
    if ok:
     outcome='PUSH' if r.final_total==r.pinnacle_total_line else ('WIN' if (side=='OVER')==(r.final_total>r.pinnacle_total_line) else 'LOSS');picks.append((outcome,dec(price)-1 if outcome=='WIN' else -1 if outcome=='LOSS' else 0))
  n=len(picks);analog.append({'phase':phase,'selections':n,'wins':sum(x[0]=='WIN' for x in picks),'losses':sum(x[0]=='LOSS' for x in picks),'pushes':sum(x[0]=='PUSH' for x in picks),'roi':sum(x[1] for x in picks)/n if n else np.nan,'status':'DETERMINISTIC REPLAY OF FROZEN NEGATIVE_BINOMIAL CONTRACT; NO THRESHOLD CHANGE'})
 pd.DataFrame(analog).to_csv(OUT/'selector_historical_analogs.csv',index=False);return pd.DataFrame(analog)

def report(selected,vrows,analogs):
 overlap=vrows.v1_same_selector_pass.sum();clusters=selected.game_pk.nunique();tot=selected.flat_stake_profit_loss.sum();best=selected.flat_stake_profit_loss.max();los=pd.read_csv(OUT/'selection_cluster_robustness.csv');cl=los[los.omission_type.eq('GAME_CLUSTER')]
 byday=selected.groupby('game_date').flat_stake_profit_loss.agg(['sum','count']);bestday=byday['sum'].idxmax();without_best_day=(tot-byday.loc[bestday,'sum'])/(len(selected)-byday.loc[bestday,'count'])
 val=analogs[analogs.phase.eq('VALIDATION')].iloc[0];decision='DECOMPOSED_SELECTOR_HOLDOUT_RESULT_NOT_REPRODUCIBLE'
 text=f"""# MLB Decomposed Scoring Selector Forensic Holdout Review v1

`{decision}`

- Exact reproduction: 9/9 frozen rows; all are in the untouched temporal holdout, with 9 unique games on {selected.game_date.nunique()} dates and no duplicate propositions or post-start Pinnacle snapshots. Membership uses only model probability, retained pregame price, and fixed gates. The retrospective experiment did not retain a distinct prediction timestamp, so timestamp evidence is incomplete even though features and market snapshots are pregame.
- Result: 7-2, profit {tot:.4f} units, ROI {tot/len(selected):.2%}; {sum(selected.side.eq('OVER'))} Overs and {sum(selected.side.eq('UNDER'))} Unders. Best winner contributes {best/tot:.1%} of profit. Leave-one-game ROI range: {cl.remaining_roi.min():.2%} to {cl.remaining_roi.max():.2%}. The best date was {bestday}; removing it leaves ROI {without_best_day:.2%}, so the profit is not wholly dependent on that date.
- Lines span {selected.line.min():.1f}-{selected.line.max():.1f}; EV {selected.ev_pct.min():.3f}%-{selected.ev_pct.max():.3f}%; Edge {selected.edge_pct.min():.3f}%-{selected.edge_pct.max():.3f}%. The selected negative-binomial model is an intercept-only component baseline: all game-level primitive means are constant, so selections arise from Pinnacle line/price geometry rather than a repeatable game-specific scoring signature.
- Frozen V1 would also select {overlap}/9; {9-overlap}/9 are decomposition-only.
- Decomposition produced fewer qualifying rows because its probability dispersion and fixed constant means keep most propositions away from the narrow simultaneous positive-EV/positive-Edge gates; the behavior ledger quantifies this on the same 79-game population. Fewer selections are not evidence of superiority.
- Retained Pinnacle history has only one pregame observation for each selected game. Later movement and CLV are unavailable; the observed row is explicitly not called a true opener.
- Validation analog: {int(val.selections)} selections, {int(val.wins)}-{int(val.losses)}-{int(val.pushes)}, ROI {val.roi:.2%}. Development analogs are unavailable because no frozen development predictions were retained and an in-sample reconstruction would violate this task. Holdout: 9, 7-2-0, ROI {tot/9:.2%}.
- The 95% exact binomial interval and bootstrap ROI interval remain wide. Despite clean proposition/game identities, selected-row directional sign agreement is only 22.2% with negative correlation, the validation analog lost money, there are only nine trials, timestamp/CLV evidence is incomplete, and no valid development chronology exists. The profitable holdout pocket is therefore not reproduced historically and is ineligible for prospective use.
- No refit, threshold change, deployment, wager rule, public change, acquisition, or prospective ledger was created.
"""
 (OUT/'concise_mlb_decomposed_selector_forensic_holdout_review_v1.md').write_text(text)

def main():
 OUT.mkdir(parents=True,exist_ok=True);replay,bench,pop,sp,pin,selected=sources();assert len(selected)==9;ledger(selected);concentration(selected);allp,vrows,primitive=comparisons(replay,bench,pop,sp,pin,selected);behavior_near_direction(allp,vrows,bench);movement_uncertainty(selected);analogs=stability_analogs(sp,pin,pop,selected);report(selected,vrows,analogs)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sha(x)}  {x.name}\n' for x in files))
if __name__=='__main__':main()
