#!/usr/bin/env python3
"""Official grading and dependence-aware characterization of frozen multi-slate replay."""
from __future__ import annotations
import csv,hashlib,json,math
from datetime import datetime,timezone
from pathlib import Path
import numpy as np,pandas as pd
P=Path('artifacts/analysis/model_development/mlb_multi_slate_strict_asof_replay/2026-07-09_2026-08-02')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(n,d):d.to_csv(P/n,index=False,quoting=csv.QUOTE_MINIMAL)
def profit(price,win):return (price/100 if price>0 else 100/abs(price)) if win else -1.
def score(d):
 x=d[d.outcome_status.isin(['WIN','LOSS'])].copy();y=(x.outcome_status=='WIN').astype(float);mp=x.model_selected_side_probability.clip(1e-6,1-1e-6);mk=x.selected_side_no_vig_probability.clip(1e-6,1-1e-6)
 return {'rows':len(d),'resolved_rows':len(x),'model_brier':np.mean((mp-y)**2) if len(x) else np.nan,'market_brier':np.mean((mk-y)**2) if len(x) else np.nan,'model_log_loss':np.mean(-(y*np.log(mp)+(1-y)*np.log(1-mp))) if len(x) else np.nan,'market_log_loss':np.mean(-(y*np.log(mk)+(1-y)*np.log(1-mk))) if len(x) else np.nan,'roi':x.pnl_1u.mean() if len(x) else np.nan,'win_rate':y.mean() if len(x) else np.nan}
def clustered_ci(d,cluster,metric='brier_delta',B=2000):
 x=d[d.outcome_status.isin(['WIN','LOSS'])].copy();y=(x.outcome_status=='WIN').astype(float);mp=x.model_selected_side_probability.clip(1e-6,1-1e-6);mk=x.selected_side_no_vig_probability.clip(1e-6,1-1e-6);x['_delta']=(mp-y)**2-(mk-y)**2 if metric=='brier_delta' else -(y*np.log(mp)+(1-y)*np.log(1-mp))+y*np.log(mk)+(1-y)*np.log(1-mk)
 sums=x.groupby(cluster)['_delta'].sum().to_numpy();counts=x.groupby(cluster).size().to_numpy();rng=np.random.default_rng(7302026)
 if len(sums)<2:return (np.nan,np.nan)
 idx=rng.integers(0,len(sums),(B,len(sums)));vals=sums[idx].sum(axis=1)/counts[idx].sum(axis=1);return tuple(np.quantile(vals,[.025,.975]))
def summarize_groups(d,cols,name):
 out=[]
 for key,x in d.groupby(cols,dropna=False):
  s=score(x);key=(key,) if not isinstance(key,tuple) else key;out.append({'analysis':name,**{c:v for c,v in zip(cols,key)},**s,'brier_model_minus_market':s['model_brier']-s['market_brier'],'logloss_model_minus_market':s['model_log_loss']-s['market_log_loss']})
 return out
def main():
 auth=json.loads((P/'OUTCOME_ACCESS_AUTHORIZATION.json').read_text());pred=P/'aggregate_frozen_prediction_ledger.csv';assert auth['outcome_access_authorized'] and sha(pred)==auth['aggregate_prediction_ledger_sha256']
 d=pd.read_csv(pred,low_memory=False);sources={};stats={}
 for gid in sorted(d.game_id.unique()):
  q=P/'official_sources'/f'{gid}_boxscore.json';sources[int(gid)]={'file':str(q),'sha256':sha(q)};j=json.loads(q.read_text())
  for side in ('away','home'):
   for v in j['teams'][side]['players'].values():stats[(int(gid),int(v['person']['id']))]=v.get('stats',{})
 outcomes=[]
 for _,r in d.iterrows():
  st=stats.get((int(r.game_id),int(r.player_id)),{});actual=None;status='IDENTITY_UNRESOLVED';detail='official player/stat lane absent'
  if r.proposition in ('hits','total_bases') and 'batting' in st:
   b=st['batting'];pa=int(b.get('plateAppearances',0));actual=float(b.get('hits',0)) if r.proposition=='hits' else float(b.get('hits',0)+b.get('doubles',0)+2*b.get('triples',0)+3*b.get('homeRuns',0));status='RESOLVED' if pa else 'DNP_VOID';detail=f'official batting PA={pa}'
  elif r.proposition=='strikeouts_pitching' and 'pitching' in st:
   b=st['pitching'];bf=int(b.get('battersFaced',0));actual=float(b.get('strikeOuts',0));status='RESOLVED' if bf else 'DNP_VOID';detail=f'official pitching BF={bf}'
  outcome=status;pnl=np.nan
  if status=='RESOLVED':
   if actual==float(r.line):outcome='PUSH';pnl=0.
   else:win=((actual>r.line)==(r.selected_side=='over'));outcome='WIN' if win else 'LOSS';pnl=profit(float(r.selected_side_price),win)
  outcomes.append({'canonical_row_identity':r.canonical_row_identity,'grading_timestamp':datetime.now(timezone.utc).isoformat(),'outcome_status':outcome,'actual_value':actual,'pnl_1u':pnl,'detail':detail,'official_source':sources[int(r.game_id)]['file'],'official_source_sha256':sources[int(r.game_id)]['sha256']})
 out=pd.DataFrame(outcomes);write('immutable_aggregate_official_outcome_ledger.csv',out);g=d.merge(out,on='canonical_row_identity',validate='one_to_one')
 # Derive requested frozen-contract views without mutating predictions.
 g['market_favorite_side']=g.market_favorite_side.fillna(pd.Series(np.where(g.price_over_american<g.price_under_american,'over','under'),index=g.index));g['model_market_agreement']=g.model_market_agreement.fillna(g.selected_side==g.market_favorite_side).astype(bool);g['month']=g.game_date.str[:7];g['favorite_price']=np.where(g.market_favorite_side=='over',g.price_over_american,g.price_under_american);g['favorite_novig_probability']=np.maximum(g.selected_side_no_vig_probability,1-g.selected_side_no_vig_probability);g['favorite_outcome_win']=np.where(g.actual_value>g.line,g.market_favorite_side=='over',np.where(g.actual_value<g.line,g.market_favorite_side=='under',np.nan));g['favorite_price_band']=pd.cut(g.favorite_price,[-np.inf,-249,-199,-149,-99],labels=['-250_or_shorter','-200_to_-249','-150_to_-199','-100_to_-149'],right=True)
 g['gap']=g.model_selected_side_probability-g.selected_side_no_vig_probability;g['gap_band']=pd.cut(g.gap,[-np.inf,-.10,-.05,0,.05,.10,np.inf],labels=['<=-0.10','>-0.10_to_-0.05','>-0.05_to_0','>0_to_0.05','>0.05_to_0.10','>0.10'],right=True)
 resolved=g[g.outcome_status.isin(['WIN','LOSS'])];overall=score(g);ci={k:{'brier':clustered_ci(g,k,'brier_delta'),'log_loss':clustered_ci(g,k,'logloss_delta')} for k in ['game_date','game_id']};ci['player_game']= {'brier':clustered_ci(g,['game_id','player_id'],'brier_delta'),'log_loss':clustered_ci(g,['game_id','player_id'],'logloss_delta')}
 analyses=[{'analysis':'overall',**overall,'brier_model_minus_market':overall['model_brier']-overall['market_brier'],'logloss_model_minus_market':overall['model_log_loss']-overall['market_log_loss']}]
 for cols,name in [(['proposition'],'proposition'),(['selected_side'],'selected_side'),(['line'],'line'),(['month'],'month'),(['semantic_model_id'],'semantic_model'),(['model_market_agreement'],'agreement'),(['market_favorite_side'],'market_favorite_side')]:analyses+=summarize_groups(g,cols,name)
 write('model_vs_market_probability_analysis.csv',pd.DataFrame(analyses));(P/'clustered_confidence_intervals.json').write_text(json.dumps(ci,indent=2)+'\n')
 unit=[]
 for col in ['game_date','game_id']:
  for key,x in g.groupby(col):
   s=score(x);unit.append({'unit':col,'key':key,'model_outperformed_market_brier':s['model_brier']<s['market_brier'],'model_outperformed_market_logloss':s['model_log_loss']<s['market_log_loss']})
 write('date_and_game_outperformance.csv',pd.DataFrame(unit))
 # False-favorite and opposite-dog questions.
 ff=[]
 for agree,x in g.groupby('model_market_agreement'):
  xr=x[x.outcome_status.isin(['WIN','LOSS'])];wins=xr.favorite_outcome_win.astype(float);prices=xr.favorite_price.astype(float);profits=[profit(p,bool(w)) for p,w in zip(prices,wins)];be=len(xr)/(len(xr)+sum(p/100 if p>0 else 100/abs(p) for p in prices)) if len(xr) else np.nan;fp=xr.favorite_novig_probability.clip(1e-6,1-1e-6)
  ff.append({'population':'favorite_supported' if agree else 'favorite_opposed','rows':len(x),'resolved_rows':len(xr),'distinct_player_games':xr[['game_id','player_id']].drop_duplicates().shape[0],'games':xr.game_id.nunique(),'dates':xr.game_date.nunique(),'win_rate':wins.mean(),'roi':np.mean(profits),'average_price':prices.mean(),'aggregate_break_even_rate':be,'observed_minus_break_even':wins.mean()-be,'market_brier':np.mean((fp-wins)**2),'market_log_loss':np.mean(-(wins*np.log(fp)+(1-wins)*np.log(1-fp))),'unresolved_rate':1-len(xr)/len(x)})
 dog=g[~g.model_market_agreement];sd=score(dog);ff.append({'population':'model_selected_dog_in_disagreement',**sd})
 write('false_favorite_analysis.csv',pd.DataFrame(ff))
 # Direct standardization over common fixed favorite strata.
 fav=g[g.outcome_status.isin(['WIN','LOSS'])].copy();fav['stratum']=fav[['proposition','line','favorite_price_band','month']].astype(str).agg('|'.join,axis=1);common=set(fav[fav.model_market_agreement].stratum)&set(fav[~fav.model_market_agreement].stratum);std=[]
 for a in [True,False]:
  vals=[]
  for st in sorted(common):vals.append(fav[(fav.model_market_agreement==a)&(fav.stratum==st)].favorite_outcome_win.mean())
  std.append({'population':'favorite_supported' if a else 'favorite_opposed','common_strata':len(common),'direct_standardized_win_rate':np.mean(vals) if vals else np.nan})
 write('composition_adjusted_agreement_test.csv',pd.DataFrame(std))
 write('model_market_gap_analysis.csv',pd.DataFrame(summarize_groups(g,['gap_band'],'fixed_gap_band')))
 pb=[]
 for band,x in g.groupby('favorite_price_band',observed=True):
  xr=x[x.outcome_status.isin(['WIN','LOSS'])];w=xr.favorite_outcome_win.astype(float);be=len(xr)/(len(xr)+sum((p/100 if p>0 else 100/abs(p)) for p in xr.favorite_price)) if len(xr) else np.nan;pb.append({'price_band':band,'rows':len(x),'resolved_rows':len(xr),'average_price':xr.favorite_price.mean(),'aggregate_break_even':be,'observed_win_rate':w.mean(),'shortfall_vs_break_even':w.mean()-be,'roi':np.mean([profit(p,bool(y)) for p,y in zip(xr.favorite_price,w)]) if len(xr) else np.nan,'model_calibration_gap':np.mean(xr.model_selected_side_probability-(xr.outcome_status=='WIN')),'market_calibration_gap':np.mean(xr.selected_side_no_vig_probability-(xr.outcome_status=='WIN'))})
 write('favorite_price_burden.csv',pd.DataFrame(pb))
 prov=[]
 for col in ['fallback_status','contributing_history_age_status','contributing_history_completeness_status']:prov+=summarize_groups(g,[col],col)
 write('provenance_and_history_quality.csv',pd.DataFrame(prov))
 coh=[]
 for key,x in g.groupby(['game_date','game_id','player_id','proposition']):
  x=x.sort_values('line');assess=len(x)>1;ok=bool(np.all(np.diff(x.model_probability_over)<=1e-12)) if assess else None;coh.append({'game_date':key[0],'game_id':key[1],'player_id':key[2],'proposition':key[3],'rows':len(x),'status':'COHERENT' if ok else 'INCOHERENT' if assess else 'ABSENT_UNASSESSABLE'})
 write('adjacent_line_coherence.csv',pd.DataFrame(coh))
 dep={'total_rows':len(g),'resolved_rows':len(resolved),'distinct_dates':g.game_date.nunique(),'distinct_games':g.game_id.nunique(),'distinct_player_games':g[['game_id','player_id']].drop_duplicates().shape[0],'distinct_market_identities':g[['game_date','game_id','player_id','proposition','line','bookmaker']].drop_duplicates().shape[0],'rows_per_player_game':len(g)/g[['game_id','player_id']].drop_duplicates().shape[0],'rows_per_game':len(g)/g.game_id.nunique(),'maximum_date_share':g.game_date.value_counts(normalize=True).max(),'maximum_game_share':g.game_id.value_counts(normalize=True).max(),'maximum_player_game_share':g.groupby(['game_id','player_id']).size().max()/len(g)};(P/'dependence_and_concentration.json').write_text(json.dumps(dep,indent=2)+'\n')
 # Fixed robustness removals, reported without selection.
 date_roi=resolved.groupby('game_date').pnl_1u.mean();largest_game=g.game_id.value_counts().idxmax();largest_pg=g.groupby(['game_id','player_id']).size().idxmax();tests={'remove_strongest_date':g.game_date!=date_roi.idxmax(),'remove_weakest_date':g.game_date!=date_roi.idxmin(),'remove_largest_game':g.game_id!=largest_game,'remove_largest_player_game':~((g.game_id==largest_pg[0])&(g.player_id==largest_pg[1])),'remove_-250_or_shorter':g.favorite_price_band.astype(str)!='-250_or_shorter'}
 for prop in sorted(g.proposition.unique()):tests[f'remove_{prop}']=g.proposition!=prop
 rob=[]
 for name,m in tests.items():s=score(g[m]);rob.append({'removal':name,**s,'brier_model_minus_market':s['model_brier']-s['market_brier'],'logloss_model_minus_market':s['model_log_loss']-s['market_log_loss']})
 write('robustness_removals.csv',pd.DataFrame(rob))
 ffd=pd.DataFrame(ff).set_index('population');supported=ffd.loc['favorite_supported'];opposed=ffd.loc['favorite_opposed'];dogroi=ffd.loc['model_selected_dog_in_disagreement','roi'];model_delta=overall['model_brier']-overall['market_brier'];incoherent=sum(x['status']=='INCOHERENT' for x in coh)
 h={'H1':'DESCRIPTIVELY_CONSISTENT' if opposed.win_rate<supported.win_rate else 'DESCRIPTIVELY_INCONSISTENT','H2':'MIXED','H3':'MIXED','H4':'NOT_EVALUABLE' if g.fallback_status.nunique()<2 else 'MIXED','H5':'NOT_EVALUABLE','H6':'NOT_EVALUABLE' if incoherent==0 else 'MIXED','H6_operational_status':'OPERATIONALLY_INACTIVE_FOR_THIS_SEMANTIC_MODEL_VERSION' if incoherent==0 else 'ACTIVE'}
 model_decision='CURRENT_MODEL_UNDERPERFORMED_MARKET_IN_REPLAY' if overall['model_brier']>overall['market_brier'] and overall['model_log_loss']>overall['market_log_loss'] else 'CURRENT_MODEL_OUTPERFORMED_MARKET_IN_REPLAY' if overall['model_brier']<overall['market_brier'] and overall['model_log_loss']<overall['market_log_loss'] else 'CURRENT_MODEL_MATCHED_MARKET_IN_REPLAY'
 false_decision='MODEL_OPPOSITION_IDENTIFIED_WEAKER_FAVORITES_AND_PROFITABLE_DOGS' if opposed.win_rate<supported.win_rate and dogroi>0 else 'MODEL_OPPOSITION_IDENTIFIED_WEAKER_FAVORITES_DOGS_NOT_PROFITABLE' if opposed.win_rate<supported.win_rate else 'MODEL_OPPOSITION_DID_NOT_IDENTIFY_WEAKER_FAVORITES'
 decisions={'replay_decision':'MULTI_SLATE_REPLAY_COMPLETED_WITH_MATERIAL_UNRESOLVED_ROWS' if (~g.outcome_status.isin(['WIN','LOSS','PUSH'])).sum()>0 else 'MULTI_SLATE_REPLAY_COMPLETED','model_market_decision':model_decision,'false_favorite_decision':false_decision,'evidence_decision':'PROSPECTIVE_STYLE_REPLAY_CHARACTERIZATION_READY' if g.game_date.nunique()>1 and dep['maximum_date_share']<.5 and set(g.model_market_agreement)=={True,False} else 'NOT_READY_INSUFFICIENT_MULTI_SLATE_EVIDENCE','production':'NOT_AUTHORIZED','hypotheses':h,'integrity':{'prediction_sha_verified':True,'semantic_ids':sorted(g.semantic_model_id.unique()),'eligible_dates':g.game_date.nunique(),'official_source_hashes':sources}}
 (P/'decisions_and_hypotheses.json').write_text(json.dumps(decisions,indent=2)+'\n');dates=pd.DataFrame(unit);summary={'rows':len(g),'resolved_rows':len(resolved),'unresolved_rows':int((~g.outcome_status.isin(['WIN','LOSS','PUSH'])).sum()),'dates':g.game_date.nunique(),'games':g.game_id.nunique(),'win_rate':overall['win_rate'],'roi':overall['roi'],'model_brier':overall['model_brier'],'market_brier':overall['market_brier'],'model_log_loss':overall['model_log_loss'],'market_log_loss':overall['market_log_loss'],'model_minus_market_brier':overall['model_brier']-overall['market_brier'],'model_minus_market_log_loss':overall['model_log_loss']-overall['market_log_loss'],'dates_model_outperformed_market_brier_pct':float(dates[dates.unit=='game_date'].model_outperformed_market_brier.mean()),'games_model_outperformed_market_brier_pct':float(dates[dates.unit=='game_id'].model_outperformed_market_brier.mean()),'adjacent_line_coherent_groups':sum(x['status']=='COHERENT' for x in coh),'adjacent_line_incoherent_groups':incoherent,'adjacent_line_absent_unassessable_groups':sum(x['status']=='ABSENT_UNASSESSABLE' for x in coh)};(P/'executive_summary.json').write_text(json.dumps(summary,indent=2)+'\n');allf=sorted(q for q in P.rglob('*') if q.is_file() and q.name!='FINAL_SHA256SUMS.csv');write('FINAL_SHA256SUMS.csv',pd.DataFrame([{'file':str(q.relative_to(P)),'sha256':sha(q),'bytes':q.stat().st_size} for q in allf]));print(json.dumps({**decisions,'summary':summary,'final_manifest_sha256':sha(P/'FINAL_SHA256SUMS.csv')},indent=2,default=str))
if __name__=='__main__':main()
