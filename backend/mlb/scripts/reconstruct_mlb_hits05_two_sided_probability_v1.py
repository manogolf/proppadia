#!/usr/bin/env python3
"""Reconstruct and review one canonical P(1+ hit) forecast per player-game."""
from __future__ import annotations
import hashlib,json,math
from pathlib import Path
import numpy as np
import pandas as pd
from backend.mlb.scripts.recover_mlb_hits_historical_identity_v1 import load_model,load_raw,load_reconcile,map_games,map_players,add_raw_identity

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'artifacts/analysis/model_development/mlb_hits05_two_sided_probability_reconstruction_v1/2026-08-14'
RECPOP=ROOT/'artifacts/analysis/model_development/mlb_hits_historical_identity_recovery_v1/2026-08-14/hits_recovered_synchronized_population.csv'
START,END='2026-05-08','2026-08-02'

def write(name,rows):pd.DataFrame(rows).to_csv(OUT/name,index=False,lineterminator='\n')
def american(p):
 p=float(p); return round(-100*p/(1-p)) if p>=.5 else round(100*(1-p)/p)
def implied(price):
 p=float(price); return 100/(p+100) if p>0 else abs(p)/(abs(p)+100)
def ll(p,y):
 p=np.clip(np.asarray(p,float),1e-12,1-1e-12);y=np.asarray(y,float);return float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p)))) if len(p) else None
def ece(p,y):
 p=np.asarray(p,float);y=np.asarray(y,float)
 if not len(p):return None
 z=0.
 for lo,hi in [(0,.35),(.35,.4),(.4,.45),(.45,.5),(.5,.55),(.55,.6),(.6,.65),(.65,.7),(.7,.75),(.75,1.00001)]:
  m=(p>=lo)&(p<hi)
  if m.any():z+=m.mean()*abs(p[m].mean()-y[m].mean())
 return float(z)
def metrics(g,col):
 if not len(g):return {k:None for k in ['brier','log_loss','ece','accuracy','mean_probability','observed_rate','probability_sd','probability_min','probability_max','calibration_gap']}
 p=pd.to_numeric(g[col]).astype(float);y=g.hit_1plus.astype(float)
 return {'brier':float(((p-y)**2).mean()),'log_loss':ll(p,y),'ece':ece(p,y),'accuracy':float(((p>=.5)==(y==1)).mean()),'mean_probability':float(p.mean()),'observed_rate':float(y.mean()),'probability_sd':float(p.std(ddof=0)),'probability_min':float(p.min()),'probability_max':float(p.max()),'calibration_gap':float(y.mean()-p.mean())}
def pband(p):return '<35%' if p<.35 else '35-39.99%' if p<.4 else '40-44.99%' if p<.45 else '45-49.99%' if p<.5 else '50-54.99%' if p<.55 else '55-59.99%' if p<.6 else '60-64.99%' if p<.65 else '65-69.99%' if p<.7 else '70-74.99%' if p<.75 else '>=75%'
def sband(x):return '<2.5pp' if x<.025 else '2.5-4.99pp' if x<.05 else '5.0-7.49pp' if x<.075 else '7.5-9.99pp' if x<.1 else '10.0-14.99pp' if x<.15 else '>=15pp'

def main():
 OUT.mkdir(parents=True,exist_ok=True);obs,_=load_model();rec=load_reconcile();raw,rawprops=load_raw()
 h=obs[(obs.capture_date.between(START,END))&pd.to_numeric(obs.line,errors='coerce').eq(.5)].copy()
 h['p_over']=pd.to_numeric(h.prob_over,errors='coerce');h['p_under']=pd.to_numeric(h.prob_under,errors='coerce');h['stored_probability']=pd.to_numeric(h.model_pick_prob,errors='coerce');h['selected_side']=h.model_pick_side.astype(str).str.lower()
 # Reconcile rows supply the first retained scheduled-start field for this exact period.
 times=rec[rec.capture_date.between(START,END)][['capture_date','game_id','game_time']].dropna().drop_duplicates()
 times=times.sort_values('game_time').drop_duplicates(['capture_date','game_id']);h=h.drop(columns=['game_time'],errors='ignore').merge(times,on=['capture_date','game_id'],how='left')
 h['start_dt']=pd.to_datetime(h.game_time,utc=True,format='mixed',errors='coerce');h['valid_strict_pregame']=h.generated_dt.notna()&h.start_dt.notna()&(h.generated_dt<h.start_dt)
 h['reconstructed_over']=np.where(h.selected_side.eq('over'),h.stored_probability,1-h.stored_probability);h['reconstructed_under']=1-h.reconstructed_over
 h['selected_reconstructed']=np.where(h.selected_side.eq('over'),h.reconstructed_over,h.reconstructed_under)
 tol=1e-9
 inv=[
  {'check':'source_prob_over_plus_under','rows':len(h),'violations':int((h.p_over.add(h.p_under).sub(1).abs()>tol).sum()),'tolerance':tol},
  {'check':'reconstructed_over_plus_under','rows':len(h),'violations':int((h.reconstructed_over.add(h.reconstructed_under).sub(1).abs()>tol).sum()),'tolerance':tol},
  {'check':'values_in_unit_interval','rows':len(h),'violations':int((~h.reconstructed_over.between(0,1)|~h.reconstructed_under.between(0,1)).sum()),'tolerance':tol},
  {'check':'selected_probability_matches_reconstruction','rows':len(h),'violations':int((h.selected_reconstructed-h.stored_probability).abs().gt(tol).sum()),'tolerance':tol},
  {'check':'reconstructed_matches_stored_prob_over','rows':len(h),'violations':int((h.reconstructed_over-h.p_over).abs().gt(1.1e-6).sum()),'tolerance':1.1e-6},
  {'check':'fair_odds_coherent','rows':len(h),'violations':int(h.reconstructed_over.map(american).isna().sum()),'tolerance':'finite'}]
 write('hits05_complement_invariant.csv',inv);assert sum(x['violations'] for x in inv)==0
 higher=np.where(h.p_over>h.p_under,'over',np.where(h.p_under>h.p_over,'under','tie'));h['higher_side']=higher
 side=[{'classification':'PICK_EQUALS_HIGHER_PROBABILITY_SIDE','rows':int(((h.selected_side==h.higher_side)&h.higher_side.ne('tie')).sum())},
       {'classification':'PICK_DIFFERS_FROM_HIGHER_PROBABILITY_SIDE','rows':int(((h.selected_side!=h.higher_side)&h.higher_side.ne('tie')).sum())},
       {'classification':'EXACT_50_PERCENT_TIE','rows':int(h.higher_side.eq('tie').sum())},
       {'classification':'ALTERNATIVE_THRESHOLD_FOUND','rows':0}]
 write('hits05_side_selection_contract.csv',side);assert side[1]['rows']==0
 semantics="""# Hits 0.5 probability semantics audit\n\n`SELECTED_SIDE_PROBABILITY_CONTRACT_CONFIRMED`\n\n`backend/mlb/scripts/build_mlb_slate_output.py::build_slate_output` reads `prob_over`, optionally applies the configured calibrator to that Over probability, assigns `p_under = 1 - p_over`, selects Over at `p_over >= 0.5` and Under otherwise, and stores `model_pick_prob = p_over` for Over or `p_under` for Under. Fair odds are computed from the same complementary probabilities before side selection. No threshold other than 0.5 and no post-selection probability transformation is present. Historical semantic model version/hash remains unresolved.\n"""
 (OUT/'hits05_probability_semantics_audit.md').write_text(semantics)

 v=h[h.valid_strict_pregame].copy();v['snapshot_key']=v.generated_dt.astype(str)+'|'+v.capture_file
 # Collapse identical source duplicates at a timestamp; conflicting probabilities fail closed.
 conflicts=v.groupby(['capture_date','game_id','player_id','generated_dt']).p_over.nunique().gt(1); conflict_keys=set(conflicts[conflicts].index)
 v=v[~v.set_index(['capture_date','game_id','player_id','generated_dt']).index.isin(conflict_keys)].copy();v=v.sort_values(['generated_dt','capture_file']).drop_duplicates(['capture_date','game_id','player_id','generated_dt'],keep='first')
 mult=[]
 for key,g in v.groupby(['capture_date','game_id','player_id']):
  mult.append({'date':key[0],'game_id':key[1],'player_id':key[2],'prediction_snapshots':len(g),'multiplicity_class':'ONE' if len(g)==1 else 'TWO' if len(g)==2 else 'THREE' if len(g)==3 else 'FOUR_PLUS','side_changed':g.selected_side.nunique()>1,'probability_changed':g.p_over.nunique()>1,'earliest_prediction_time':g.generated_dt.min(),'latest_valid_pregame_prediction_time':g.generated_dt.max(),'absolute_earliest_latest_change':abs(g.sort_values('generated_dt').p_over.iloc[-1]-g.sort_values('generated_dt').p_over.iloc[0])})
 mult=pd.DataFrame(mult);write('hits05_snapshot_multiplicity.csv',mult)
 earliest=v.sort_values(['generated_dt','capture_file']).drop_duplicates(['capture_date','game_id','player_id'],keep='first').copy();latest=v.sort_values(['generated_dt','capture_file']).drop_duplicates(['capture_date','game_id','player_id'],keep='last').copy()
 policy={'policy':'EARLIEST_VALID_STRICT_PREGAME_MODEL_PREDICTION','keys':['game_date','game_pk','player_id','hits','0.5'],'outcome_independent':True,'market_independent':True,'supported_player_games':len(earliest),'excluded_same_timestamp_probability_conflicts':len(conflict_keys),'reason':'No retained designated daily model snapshot is uniformly authoritative; earliest strict-pregame is uniformly auditable and avoids repeated-snapshot weighting.'}
 (OUT/'hits05_canonical_prediction_policy.json').write_text(json.dumps(policy,indent=2)+'\n')
 outcomes=rec[(rec.capture_date.between(START,END))&rec.prop_type.astype(str).eq('hits')&pd.to_numeric(rec.line,errors='coerce').eq(.5)].copy();outcomes['actual_hits']=pd.to_numeric(outcomes.actual_value,errors='coerce');outcomes=outcomes.dropna(subset=['actual_hits']).sort_values('bookmaker_key').drop_duplicates(['capture_date','game_id','player_id'])
 outmap=outcomes[['capture_date','game_id','player_id','actual_hits']]
 board=earliest.merge(outmap,on=['capture_date','game_id','player_id'],how='left');board['hit_1plus']=np.where(board.actual_hits.notna(),board.actual_hits.ge(1).astype(int),np.nan)
 rp=pd.read_csv(RECPOP,dtype={'game_id':str,'player_id':str});rp=rp[rp.line.eq(.5)][['date','game_id','player_id','provenance']].drop_duplicates();rp=rp.groupby(['date','game_id','player_id'],as_index=False).provenance.agg(lambda x:'|'.join(sorted(set(x))))
 board=board.merge(rp,left_on=['capture_date','game_id','player_id'],right_on=['date','game_id','player_id'],how='left',validate='one_to_one');assert not board.duplicated(['capture_date','game_id','player_id']).any()
 for c in ['team','opponent']:
  if c not in board:board[c]=''
 board['provenance']=board.provenance.fillna('NO_SYNCHRONIZED_IDENTITY_PROVENANCE')
 board_out=pd.DataFrame({'game_date':board.capture_date,'game_pk':board.game_id,'scheduled_start':board.game_time,'player_id':board.player_id,'player_name':board.player_name,'team':board.team,'opponent':board.opponent,
  'prediction_timestamp':board.generated_at_utc,'original_model_pick_side':board.selected_side,'original_stored_probability':board.stored_probability,'p_over_0_5':board.reconstructed_over,'p_under_0_5':board.reconstructed_under,
  'actual_hits':board.actual_hits,'hit_1plus':board.hit_1plus,'prediction_source':board.capture_file,'identity_provenance':board.provenance,'snapshot_policy':policy['policy']})
 board_out.to_csv(OUT/'hits05_canonical_player_game_board.csv',index=False,lineterminator='\n')

 # Build exact paired BetOnline observations from the already-retained raw payloads.
 gm,gl=map_games(raw,obs,rec);pm,pl=map_players(rawprops,gl,obs);ri=add_raw_identity(raw,gl,pl);ri=ri[ri.line.eq(.5)&ri.side.isin(['over','under'])&ri.game_id.ne('')&ri.player_id.ne('')&ri.pregame].copy()
 pairkeys=['date','event_id','game_id','player_id','player_name','capture_file','captured_at','commence_time'];pairs=[]
 for key,g in ri.groupby(pairkeys,dropna=False):
  prices=g.sort_values('market_last_update').drop_duplicates('side',keep='last').set_index('side').price
  if not {'over','under'}<=set(prices.index):continue
  io,iu=implied(prices['over']),implied(prices['under']);den=io+iu
  pairs.append(dict(zip(pairkeys,key),price_over_american=prices['over'],price_under_american=prices['under'],implied_over=io,implied_under=iu,betonline_p_over_novig=io/den,betonline_p_under_novig=iu/den,market_timestamp=key[6]))
 pairs=pd.DataFrame(pairs);pairs['market_dt']=pd.to_datetime(pairs.market_timestamp,utc=True,format='mixed')
 pairs_by_key={key:g for key,g in pairs.groupby(['date','game_id','player_id'],sort=False)}
 attachments=[]
 for r in board.itertuples(index=False):
  x=pairs_by_key.get((r.capture_date,r.game_id,r.player_id),pairs.iloc[0:0])
  if x.empty:continue
  prior=x[x.market_dt<=r.generated_dt]
  if len(prior):z=prior.sort_values('market_dt').iloc[-1];timing='LATEST_AT_OR_BEFORE_MODEL'
  else:z=x[x.market_dt<r.start_dt].sort_values('market_dt').iloc[0] if len(x[x.market_dt<r.start_dt]) else None;timing='FALLBACK_NEXT_PREGAME'
  if z is None:continue
  attachments.append({'game_date':r.capture_date,'game_pk':r.game_id,'player_id':r.player_id,'player_name':r.player_name,'model_prediction_timestamp':r.generated_at_utc,'model_p_over_0_5':r.reconstructed_over,'actual_hits':r.actual_hits,'hit_1plus':int(r.actual_hits>=1) if pd.notna(r.actual_hits) else None,
   'price_over_american':z.price_over_american,'price_under_american':z.price_under_american,'raw_implied_over':z.implied_over,'raw_implied_under':z.implied_under,'betonline_p_over_novig':z.betonline_p_over_novig,'betonline_p_under_novig':z.betonline_p_under_novig,'market_timestamp':z.market_timestamp,'timing_policy':timing,'market_source':z.capture_file})
 market=pd.DataFrame(attachments);market.to_csv(OUT/'hits05_betonline_player_game_board.csv',index=False,lineterminator='\n')
 common=market[market.hit_1plus.notna()].copy();common['hit_1plus']=common.hit_1plus.astype(int);common['model_market_gap']=common.model_p_over_0_5-common.betonline_p_over_novig;common['absolute_gap']=common.model_market_gap.abs()
 quality=[]
 for actor,col in [('PROPPADIA','model_p_over_0_5'),('BETONLINE','betonline_p_over_novig')]:quality.append({'actor':actor,'canonical_model_player_games':len(board),'outcome_complete_model_player_games':int(board.actual_hits.notna().sum()),'common_player_games':len(common),'games':common.game_pk.nunique(),'date_start':common.game_date.min(),'date_end':common.game_date.max(),**metrics(common,col)})
 quality.append({'actor':'PROPPADIA_MINUS_BETONLINE','brier':quality[0]['brier']-quality[1]['brier'],'log_loss':quality[0]['log_loss']-quality[1]['log_loss'],'ece':quality[0]['ece']-quality[1]['ece'],'accuracy':quality[0]['accuracy']-quality[1]['accuracy']})
 write('hits05_full_board_prediction_quality.csv',quality)
 common['probability_band']=common.model_p_over_0_5.map(pband);reliability=[]
 bands=['<35%','35-39.99%','40-44.99%','45-49.99%','50-54.99%','55-59.99%','60-64.99%','65-69.99%','70-74.99%','>=75%']
 for b in bands:
  x=common[common.probability_band.eq(b)];m=metrics(x,'model_p_over_0_5');reliability.append({'probability_band':b,'player_games':len(x),'mean_predicted_probability':m['mean_probability'],'observed_hit_rate':m['observed_rate'],'calibration_gap':m['calibration_gap'],'brier':m['brier'],'sample_status':'SMALL_SAMPLE' if len(x)<100 else 'ADEQUATE_DESCRIPTIVE_SAMPLE'})
 write('hits05_full_board_reliability.csv',reliability)
 ranked=common.sort_values(['model_p_over_0_5','game_pk','player_id']).copy();ranked['q']=pd.qcut(np.arange(len(ranked)),5,labels=['bottom20','second20','middle20','fourth20','top20']);ordering=[]
 for q in ['bottom20','second20','middle20','fourth20','top20']:
  x=ranked[ranked.q.astype(str).eq(q)];m=metrics(x,'model_p_over_0_5');ordering.append({'confidence_group':q,'rows':len(x),'mean_p_over':m['mean_probability'],'observed_hit_rate':m['observed_rate'],'brier':m['brier']})
 x=ranked.tail(math.ceil(len(ranked)*.1));m=metrics(x,'model_p_over_0_5');ordering.append({'confidence_group':'top10','rows':len(x),'mean_p_over':m['mean_probability'],'observed_hit_rate':m['observed_rate'],'brier':m['brier']});write('hits05_full_board_confidence_ordering.csv',ordering)
 common['separation_band']=common.absolute_gap.map(sband);sep=[]
 for b in ['<2.5pp','2.5-4.99pp','5.0-7.49pp','7.5-9.99pp','10.0-14.99pp','>=15pp']:
  x=common[common.separation_band.eq(b)];mm=metrics(x,'model_p_over_0_5');bb=metrics(x,'betonline_p_over_novig');me=(x.model_p_over_0_5-x.hit_1plus).abs();be=(x.betonline_p_over_novig-x.hit_1plus).abs()
  sep.append({'separation_band':b,'rows':len(x),'model_brier':mm['brier'],'betonline_brier':bb['brier'],'model_closer':int((me<be).sum()),'betonline_closer':int((be<me).sum()),'observed_hit_rate':mm['observed_rate'],'mean_model_probability':mm['mean_probability'],'mean_betonline_probability':bb['mean_probability']})
 overall={'separation_band':'OVERALL_DISTRIBUTION','rows':len(common),'mean_signed_separation':common.model_market_gap.mean(),'mean_absolute_separation':common.absolute_gap.mean(),'median_absolute_separation':common.absolute_gap.median(),'separation_sd':common.model_market_gap.std(ddof=0),'absolute_p10':common.absolute_gap.quantile(.1),'absolute_p25':common.absolute_gap.quantile(.25),'absolute_p75':common.absolute_gap.quantile(.75),'absolute_p90':common.absolute_gap.quantile(.9),'absolute_p95':common.absolute_gap.quantile(.95)};write('hits05_full_board_market_separation.csv',[overall]+sep)
 signed=[]
 for label,mask in [('PROPPADIA_HIGHER',common.model_market_gap>0),('PROPPADIA_LOWER',common.model_market_gap<0),('PROPPADIA_HIGHER_GE10PP',common.model_market_gap>=.1),('PROPPADIA_LOWER_GE10PP',common.model_market_gap<=-.1),('PROPPADIA_HIGHER_GE15PP',common.model_market_gap>=.15),('PROPPADIA_LOWER_GE15PP',common.model_market_gap<=-.15)]:
  x=common[mask];mm=metrics(x,'model_p_over_0_5');bb=metrics(x,'betonline_p_over_novig');signed.append({'group':label,'rows':len(x),'mean_signed_gap':x.model_market_gap.mean() if len(x) else None,'model_brier':mm['brier'],'betonline_brier':bb['brier'],'observed_hit_rate':mm['observed_rate'],'calibration_gap':mm['calibration_gap']})
 write('hits05_full_board_signed_separation.csv',signed)
 selected_win=np.where(board.selected_side.eq('over'),board.actual_hits.ge(1),board.actual_hits.eq(0));selected=board[board.actual_hits.notna()].copy();selected['selected_win']=selected_win[board.actual_hits.notna()]
 fullm=metrics(common,'model_p_over_0_5');selected_rows=[{'view':'HISTORICAL_SELECTED_SIDE','rows':len(selected),'over_fraction':selected.selected_side.eq('over').mean(),'under_fraction':selected.selected_side.eq('under').mean(),'accuracy_or_observed_win_rate':selected.selected_win.mean(),'mean_probability':selected.stored_probability.mean(),'interpretation':'selected preferred direction; lane asymmetry is selection-conditioned'},
  {'view':'CANONICAL_FULL_BOARD','rows':len(common),'over_fraction':'not applicable','under_fraction':'not applicable','accuracy_or_observed_win_rate':fullm['observed_rate'],'mean_probability':fullm['mean_probability'],'interpretation':'one P(1+ hit) per player-game; natural binary calibration and ordering'}]
 write('hits05_selected_vs_full_board.csv',selected_rows)
 temporal=[];common['month']=common.game_date.str[:7]
 for month,x in common.groupby('month'):
  mm=metrics(x,'model_p_over_0_5');temporal.append({'period_type':'month','period':month,'rows':len(x),**{f'model_{k}':v for k,v in mm.items()},'betonline_brier':metrics(x,'betonline_p_over_novig')['brier'],'mean_model_market_separation':x.model_market_gap.mean()})
 thirds=[]
 for i,x in enumerate(np.array_split(common.sort_values(['game_date','game_pk','player_id']),3),1):
  mm=metrics(x,'model_p_over_0_5');z={'period_type':'chronological_third','period':f'third_{i}','rows':len(x),**{f'model_{k}':v for k,v in mm.items()},'betonline_brier':metrics(x,'betonline_p_over_novig')['brier'],'mean_model_market_separation':x.model_market_gap.mean()};temporal.append(z);thirds.append(z)
 bd=thirds[-1]['model_brier']-thirds[0]['model_brier'];ed=thirds[-1]['model_ece']-thirds[0]['model_ece'];temp='DETERIORATING' if bd>.015 or ed>.05 else 'IMPROVING' if bd<-.015 and ed<0 else 'STABLE' if abs(bd)<.005 and abs(ed)<.03 else 'MILDLY_DRIFTING';write('hits05_temporal_stability.csv',temporal)
 multi=mult[mult.prediction_snapshots>1][['date','game_id','player_id']];early=earliest.merge(multi,left_on=['capture_date','game_id','player_id'],right_on=['date','game_id','player_id']).merge(outmap,on=['capture_date','game_id','player_id']);late=latest.merge(multi,left_on=['capture_date','game_id','player_id'],right_on=['date','game_id','player_id']).merge(outmap,on=['capture_date','game_id','player_id'])
 diag=[]
 for label,x in [('EARLIEST_VALID',early),('LATEST_VALID',late)]:x=x.copy();x['hit_1plus']=x.actual_hits.ge(1).astype(int);diag.append({'snapshot':label,'rows':len(x),**metrics(x,'p_over')})
 changes=mult[mult.prediction_snapshots>1];diag.append({'snapshot':'STABILITY_CHANGE','rows':len(changes),'mean_absolute_probability_change':changes.absolute_earliest_latest_change.mean(),'median_absolute_probability_change':changes.absolute_earliest_latest_change.median(),'p90_absolute_probability_change':changes.absolute_earliest_latest_change.quantile(.9),'side_flip_rate':changes.side_changed.mean()});write('hits05_snapshot_timing_diagnostic.csv',diag)
 rates=[x['observed_hit_rate'] for x in ordering[:5]];ordered=sum(b>=a for a,b in zip(rates,rates[1:]))>=3 and rates[-1]>rates[0];high=next(x for x in reliability if x['probability_band']=='>=75%');large=next(x for x in sep if x['separation_band']=='>=15pp')
 evidence='HITS05_TWO_SIDED_PROBABILITY_EVIDENCE_STRONG' if fullm['ece']<.05 and ordered and fullm['brier']<=quality[1]['brier']+.01 and abs(high['calibration_gap'])<.1 and large['model_brier']<=large['betonline_brier']+.005 else 'HITS05_TWO_SIDED_PROBABILITY_EVIDENCE_MIXED' if ordered and fullm['ece']<.1 else 'HITS05_TWO_SIDED_PROBABILITY_EVIDENCE_WEAK'
 summary={'task_id':'MLB_HITS05_TWO_SIDED_PROBABILITY_RECONSTRUCTION_V1','probability_contract':'SELECTED_SIDE_PROBABILITY_CONTRACT_CONFIRMED','complement_reconstruction_valid':True,'canonical_player_games':len(board),'outcome_complete':int(board.actual_hits.notna().sum()),'common_betonline_player_games':len(common),'games':common.game_pk.nunique(),'date_start':common.game_date.min(),'date_end':common.game_date.max(),'proppadia':quality[0],'betonline':quality[1],'confidence_ordered':ordered,'high_probability_band':high,'large_separation_band':large,'temporal_status':temp,'historical_model_identity':'UNRESOLVED','evidence_status':evidence,'provenance_work_justified':'YES','prospective_capture_review_justified':'YES'}
 md=f"""# MLB Hits 0.5 two-sided probability reconstruction v1\n\n- Contract: `SELECTED_SIDE_PROBABILITY_CONTRACT_CONFIRMED`; complement invariants pass with zero violations.\n- Canonical earliest-strict-pregame board: {len(board):,} player-games; outcome complete {int(board.actual_hits.notna().sum()):,}; common paired BetOnline board {len(common):,}.\n- Proppadia full-board Brier {quality[0]['brier']:.6f}, log loss {quality[0]['log_loss']:.6f}, ECE {quality[0]['ece']:.6f}; BetOnline {quality[1]['brier']:.6f}, {quality[1]['log_loss']:.6f}, {quality[1]['ece']:.6f}.\n- Confidence ordering: {'present' if ordered else 'not reliable'}; temporal `{temp}`. At >=15pp separation, model Brier {large['model_brier']} vs BetOnline {large['betonline_brier']}.\n- The selected-side framing materially conditioned the prior Over/Under lanes; the full board evaluates one coherent P(1+ hit) forecast instead of treating selected directions as independent boards.\n- `HISTORICAL_MODEL_IDENTITY = UNRESOLVED`. Evidence: `{evidence}`.\n- `PROVENANCE_WORK_JUSTIFIED = YES`; `PROSPECTIVE_CAPTURE_REVIEW_JUSTIFIED = YES`. No next step was executed.\n""";(OUT/'concise_mlb_hits05_two_sided_probability_reconstruction_v1.md').write_text(md);(OUT/'reconstruction_summary.json').write_text(json.dumps(summary,indent=2,default=str)+'\n')
 products=sorted(p for p in OUT.iterdir() if p.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n' for p in products));print(json.dumps(summary,indent=2,default=str));return 0
if __name__=='__main__':raise SystemExit(main())
