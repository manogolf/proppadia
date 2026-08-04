#!/usr/bin/env python3
"""Grade an authorized frozen replay ledger from official MLB boxscores."""
from __future__ import annotations
import argparse,csv,hashlib,json,math
from datetime import datetime,timezone
from pathlib import Path
import numpy as np,pandas as pd

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write_csv(p,d):d.to_csv(p,index=False,quoting=csv.QUOTE_MINIMAL)
def roi(price,win):return (price/100 if price>0 else 100/abs(price)) if win else -1.0
def band(v,cuts,labels):
 for lo,hi,label in zip(cuts[:-1],cuts[1:],labels):
  if lo<=v<hi:return label
 return labels[-1]
def summarize(d,segment,value):
 r=d[d[segment].astype(str)==str(value)] if segment!='overall' else d
 resolved=r[r.outcome_status.isin(['WIN','LOSS'])];wins=(resolved.outcome_status=='WIN').sum();n=len(resolved)
 return {'segment':segment,'value':value,'frozen_rows':len(r),'resolved_rows':n,'wins':int(wins),'losses':int(n-wins),'pushes':int((r.outcome_status=='PUSH').sum()),'void_unresolved':int((~r.outcome_status.isin(['WIN','LOSS','PUSH'])).sum()),'win_rate':wins/n if n else np.nan,'units':r.pnl_1u.sum(min_count=1),'roi':r.pnl_1u.sum()/n if n else np.nan,'model_brier':((resolved.model_selected_side_probability-(resolved.outcome_status=='WIN').astype(int))**2).mean() if n else np.nan,'market_brier':((resolved.selected_side_no_vig_probability-(resolved.outcome_status=='WIN').astype(int))**2).mean() if n else np.nan,'model_log_loss':(-((resolved.outcome_status=='WIN').astype(int)*np.log(resolved.model_selected_side_probability.clip(.000001,.999999))+(resolved.outcome_status=='LOSS').astype(int)*np.log((1-resolved.model_selected_side_probability).clip(.000001,.999999)))).mean() if n else np.nan,'market_log_loss':(-((resolved.outcome_status=='WIN').astype(int)*np.log(resolved.selected_side_no_vig_probability.clip(.000001,.999999))+(resolved.outcome_status=='LOSS').astype(int)*np.log((1-resolved.selected_side_no_vig_probability).clip(.000001,.999999)))).mean() if n else np.nan}
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--package',type=Path,required=True);a=ap.parse_args();p=a.package
 ledger_path=p/'immutable_replay_prediction_ledger.csv';freeze=p/'PREDICTION_FREEZE_SHA256SUMS.csv';auth=json.loads((p/'OUTCOME_ACCESS_AUTHORIZATION.json').read_text())
 assert auth['outcome_access_authorized'] and sha(ledger_path)==auth['prediction_ledger_sha256'] and sha(freeze)==auth['prediction_freeze_manifest_sha256']
 d=pd.read_csv(ledger_path);sources={};stats={}
 for gid in sorted(d.game_id.unique()):
  sp=p/'official_sources'/f'{gid}_boxscore.json';sources[int(gid)]={'path':str(sp),'sha256':sha(sp)};j=json.loads(sp.read_text())
  for side in ('away','home'):
   for key,v in j['teams'][side]['players'].items():stats[(int(gid),int(v['person']['id']))]=v.get('stats',{})
 rows=[]
 for _,r in d.iterrows():
  s=stats.get((int(r.game_id),int(r.player_id)),{});actual=None;status='IDENTITY_UNRESOLVED';detail='player/stat lane absent from official boxscore'
  if r.proposition in ('hits','total_bases') and 'batting' in s:
   b=s['batting'];pa=int(b.get('plateAppearances',0));actual=float(b.get('hits',0)) if r.proposition=='hits' else float(b.get('hits',0)+b.get('doubles',0)+2*b.get('triples',0)+3*b.get('homeRuns',0));status='RESOLVED' if pa>0 else 'DNP_VOID';detail=f'official batting; PA={pa}'
  elif r.proposition=='strikeouts_pitching' and 'pitching' in s:
   q=s['pitching'];bf=int(q.get('battersFaced',0));actual=float(q.get('strikeOuts',0));status='RESOLVED' if bf>0 else 'DNP_VOID';detail=f'official pitching; BF={bf}'
  outcome=status;pnl=np.nan
  if status=='RESOLVED':
   if actual==float(r.line):outcome='PUSH';pnl=0.0
   else:
    win=(actual>r.line)==(r.selected_side=='over');outcome='WIN' if win else 'LOSS';pnl=roi(float(r.selected_side_price),win)
  rows.append({'canonical_row_identity':r.canonical_row_identity,'grading_timestamp':datetime.now(timezone.utc).isoformat(),'outcome_status':outcome,'actual_value':actual,'selected_side_outcome':outcome,'pnl_1u':pnl,'participation_detail':detail,'outcome_source':sources[int(r.game_id)]['path'],'outcome_source_sha256':sources[int(r.game_id)]['sha256']})
 out=pd.DataFrame(rows);write_csv(p/'immutable_official_outcome_ledger.csv',out);g=d.merge(out,on='canonical_row_identity',validate='one_to_one')
 g['agreement']=np.where(((g.selected_side=='over')&(g.selected_side_no_vig_probability>=.5))|((g.selected_side=='under')&(g.selected_side_no_vig_probability>=.5)),'market_agreement','model_market_disagreement')
 g['favorite_dog']=np.where(g.selected_side_price<0,'favorite','dog_or_even');g['price_band']=g.selected_side_price.apply(lambda x:'plus_money_or_even' if x>=0 else ('-100_to_-149' if x>=-149 else '-150_to_-199' if x>=-199 else '-200_to_-249' if x>=-249 else '-250_or_shorter'))
 g['model_probability_band']=g.model_selected_side_probability.apply(lambda x:band(x,[0,.50,.55,.60,.65,.70,2],['below_0.50','0.50_to_0.549','0.55_to_0.599','0.60_to_0.649','0.65_to_0.699','0.70_plus']))
 g['market_probability_band']=g.selected_side_no_vig_probability.apply(lambda x:band(x,[0,.50,.55,.60,.65,.70,2],['below_0.50','0.50_to_0.549','0.55_to_0.599','0.60_to_0.649','0.65_to_0.699','0.70_plus']))
 g['bvp_provenance']=g.canonical_feature_serialization.apply(lambda x:'direct' if 'prop_features_precomputed' in x else 'fallback_or_none')
 segs=[]
 for col in ['proposition','line','selected_side','bookmaker','agreement','favorite_dog','price_band','model_probability_band','market_probability_band','bvp_provenance','game_id','team']:
  for v in sorted(g[col].astype(str).unique()):segs.append(summarize(g,col,v))
 segs.insert(0,summarize(g,'overall','all'));write_csv(p/'performance_by_required_segment.csv',pd.DataFrame(segs))
 # Adjacent-line probability coherence is descriptive; repeated player/game/prop ladders are clustered.
 coh=[]
 for key,x in g.groupby(['game_id','player_id','proposition']):
  x=x.sort_values('line');probs=x.model_probability_over.to_numpy();coherent=bool(np.all(np.diff(probs)<=1e-12))
  coh.append({'game_id':key[0],'player_id':key[1],'proposition':key[2],'row_count':len(x),'lines':'|'.join(map(str,x.line)),'probability_over':'|'.join(map(str,x.model_probability_over)),'nonincreasing_probability_over':coherent})
 write_csv(p/'adjacent_line_probability_coherence.csv',pd.DataFrame(coh))
 dep=[]
 for unit,cols in [('slate',['game_date']),('game',['game_id']),('team',['game_id','team']),('player_game',['game_id','player_id']),('adjacent_line_group',['game_id','player_id','proposition']),('proposition_family',['proposition'])]:
  sizes=g.groupby(cols).size();dep.append({'dependence_unit':unit,'clusters':len(sizes),'rows':int(sizes.sum()),'max_cluster_size':int(sizes.max()),'effective_independent_units_upper_bound':len(sizes),'interpretation':'Rows inside a cluster are not independent; no inferential significance claimed.'})
 write_csv(p/'dependence_structure.csv',pd.DataFrame(dep))
 resolved=g[g.outcome_status.isin(['WIN','LOSS'])];overall=summarize(g,'overall','all');win_profit=g.selected_side_price.apply(lambda x:x/100 if x>0 else 100/abs(x));break_even=len(resolved)/(len(resolved)+win_profit.loc[resolved.index].sum()) if len(resolved) else np.nan
 summary={'replay_label':'STRICT_AS_OF_HISTORICAL_REPLAY','selected_replay_date':'2026-07-09','replay_validity_decision':'FIRST_STRICT_AS_OF_HISTORICAL_REPLAY_COMPLETED','evidence_decision':'NOT_READY_SINGLE_REPLAY_SLATE','production_readiness':'NOT_AUTHORIZED','games':int(g.game_id.nunique()),'captured_rows':len(g),'resolved_rows':len(resolved),'distinct_player_games':int(g[['game_id','player_id']].drop_duplicates().shape[0]),'distinct_market_identities':int(g[['game_id','player_id','proposition','line','bookmaker']].drop_duplicates().shape[0]),'counts_by_proposition':g.proposition.value_counts().to_dict(),'counts_by_semantic_model_id':g.semantic_model_id.value_counts().to_dict(),'lineage_failures':int((g.lineage_status!='LINEAGE_CERTIFIED').sum()),'feature_reconstruction_failures':int(pd.read_csv(p/'feature_reconstruction_failures.csv').shape[0]),'unresolved_outcomes':int((~g.outcome_status.isin(['WIN','LOSS','PUSH'])).sum()),'wins':int((g.outcome_status=='WIN').sum()),'losses':int((g.outcome_status=='LOSS').sum()),'win_rate':overall['win_rate'],'one_unit_roi':overall['roi'],'units':overall['units'],'aggregate_break_even_rate':break_even,'average_executable_price':float(g.selected_side_price.mean()),'model_brier':overall['model_brier'],'model_log_loss':overall['model_log_loss'],'market_brier':overall['market_brier'],'market_log_loss':overall['market_log_loss'],'paired_model_minus_market_brier':overall['model_brier']-overall['market_brier'],'paired_model_minus_market_log_loss':overall['model_log_loss']-overall['market_log_loss'],'average_model_minus_market_selected_probability':float((g.model_selected_side_probability-g.selected_side_no_vig_probability).mean())}
 (p/'replay_summary_and_decisions.json').write_text(json.dumps(summary,indent=2)+'\n')
 report=f"""# First strict as-of historical replay\n\nLabel: `STRICT_AS_OF_HISTORICAL_REPLAY`\n\nFrozen predictions: {len(g)} across {g.game_id.nunique()} games. Resolved decisions: {len(resolved)}; pushes: {(g.outcome_status=='PUSH').sum()}; void/unresolved: {(~g.outcome_status.isin(['WIN','LOSS','PUSH'])).sum()}. Win rate: {overall['win_rate']:.4f}. Units: {overall['units']:.4f}. ROI: {overall['roi']:.4f}. Model Brier: {overall['model_brier']:.4f}; market Brier: {overall['market_brier']:.4f}.\n\n## Descriptive hypotheses H1-H6\n\n- H1 — `FIRST_STRICT_AS_OF_REPLAY_DESCRIPTIVE_ONLY`: model/market disagreement is descriptive only.\n- H2 — `FIRST_STRICT_AS_OF_REPLAY_DESCRIPTIVE_ONLY`: favorite/dog and price-band results are descriptive only.\n- H3 — `FIRST_STRICT_AS_OF_REPLAY_DESCRIPTIVE_ONLY`: probability-band calibration shape is descriptive only.\n- H4 — `FIRST_STRICT_AS_OF_REPLAY_DESCRIPTIVE_ONLY`: direct/fallback input results are descriptive only.\n- H5 — `FIRST_STRICT_AS_OF_REPLAY_DESCRIPTIVE_ONLY`: adjacent-line coherence is descriptive only.\n- H6 — `FIRST_STRICT_AS_OF_REPLAY_DESCRIPTIVE_ONLY`: proposition differences are descriptive only.\n\n## Decision\n\nReplay validity: **FIRST_STRICT_AS_OF_HISTORICAL_REPLAY_COMPLETED**. The predictions were frozen before outcome access, use registered current semantic identities, exact archived strict-prior payloads, paired same-run prices, and official outcomes.\n\nEvidence decision: **NOT_READY_SINGLE_REPLAY_SLATE**. Production readiness: **NOT_AUTHORIZED**. This one correlated replay slate cannot authorize a selector, wager rule, rejection gate, residual population, EV rule, model change, probability modification, promotion, threshold, calibration, stake, or routing change.\n"""
 (p/'replay_evidence_report.md').write_text(report)
 (p/'official_outcome_source_manifest.json').write_text(json.dumps(sources,indent=2)+'\n')
 allfiles=sorted(q for q in p.rglob('*') if q.is_file() and q.name!='FINAL_SHA256SUMS.csv');write_csv(p/'FINAL_SHA256SUMS.csv',pd.DataFrame([{'file':str(q.relative_to(p)),'sha256':sha(q),'bytes':q.stat().st_size} for q in allfiles]))
 print(json.dumps({'frozen':len(g),'resolved':len(resolved),'decision':'NOT_READY_SINGLE_REPLAY_SLATE','final_manifest_sha256':sha(p/'FINAL_SHA256SUMS.csv')},indent=2))
if __name__=='__main__':main()
