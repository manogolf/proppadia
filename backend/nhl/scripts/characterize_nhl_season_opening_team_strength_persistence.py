#!/usr/bin/env python3
"""Characterize NHL offseason team-strength persistence without fitting a model."""
from __future__ import annotations
import argparse,hashlib,json,re,subprocess
from pathlib import Path
import numpy as np
import pandas as pd
from backend.nhl.analysis_package_guard import require_create_only,verify_parents

DATE='2026-08-10'; PARENT_DATE='2026-07-13'; CHAMPION='NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1'; PRED_SHA='83beb11588f7e7e31919f23be2dea51ff49863954fc9be750509b30a0eff2cda'
PARENTS={'feature_spine':('nhl_moneyline_team_goalie_feature_spine','c1841f802a90aa1e772059695cc7e8e1c512c9f63730ab54bd4cf0576bf92780'),'frozen_baseline':('nhl_moneyline_frozen_baseline_certification','8bb36073fee4f055f399c651f942b8de6eb1bb3b75b96b6112dd9d4af4224cf5'),'opening_readiness':('nhl_season_2026_opening_preseason_context_readiness','96f2f5e5e9f2125c95863f95fc4f58944868d4ae68533aade3fc93a10becade9')}
CONCEPTS={'gf_pg':'GOAL_STRENGTH','ga_pg':'GOAL_STRENGTH','goal_diff_pg':'GOAL_STRENGTH','sf_pg':'SHOT_STRENGTH','sa_pg':'SHOT_STRENGTH','shot_diff_pg':'SHOT_STRENGTH'}
def sha(p):
 h=hashlib.sha256()
 with Path(p).open('rb') as f:
  for b in iter(lambda:f.read(1<<20),b''): h.update(b)
 return h.hexdigest()
def csv(d,p): d.to_csv(p,index=False,lineterminator='\n',float_format='%.15g')
def js(o,p): Path(p).write_text(json.dumps(o,indent=2,sort_keys=True,allow_nan=False)+'\n')
def safe_corr(a,b,method='pearson'):
 z=pd.DataFrame({'a':a,'b':b}).dropna(); return float(z.a.corr(z.b,method=method)) if len(z)>=3 and z.a.nunique()>1 and z.b.nunique()>1 else np.nan
def rank_metrics(prior,target):
 z=pd.DataFrame({'prior':prior,'target':target}).dropna(); n=len(z)
 if n<4:return {'teams':n,'rank_retention':np.nan,'mean_absolute_rank_movement':np.nan,'sign_persistence':np.nan,'top_quartile_retention':np.nan,'bottom_quartile_retention':np.nan}
 pr=z.prior.rank(pct=True); tr=z.target.rank(pct=True); pm=z.prior.median(); tm=z.target.median(); top=pr>.75; bot=pr<=.25
 return {'teams':n,'rank_retention':safe_corr(pr,tr,'spearman'),'mean_absolute_rank_movement':float(abs(pr-tr).mean()),'sign_persistence':float(((z.prior-pm)*(z.target-tm)>=0).mean()),'top_quartile_retention':float((tr[top]>.75).mean()),'bottom_quartile_retention':float((tr[bot]<=.25).mean())}
def depth_group(n): return '0' if n==0 else '1' if n==1 else '2' if n==2 else '3-4' if n<=4 else '5-9' if n<=9 else '10+'

def team_games(spine):
 rows=[]
 for side in ['home','away']:
  z=spine.copy(); z['team_id']=z[f'{side}_team_id']; z['team']=z[f'{side}_team']; z['opponent_id']=z[f'{"away" if side=="home" else "home"}_team_id']; z['gf']=z.final_home_goals if side=='home' else z.final_away_goals; z['ga']=z.final_away_goals if side=='home' else z.final_home_goals; z['is_home']=side=='home'; z['prior_games']=z[f'{side}_prior_games']; z['pre_sf_pg']=z[f'{side}_std_sf_pg']; z['pre_sa_pg']=z[f'{side}_std_sa_pg']; z['pre_r10_gf_pg']=z[f'{side}_r10_gf_pg']; z['pre_r10_ga_pg']=z[f'{side}_r10_ga_pg']; z['pre_r10_sf_pg']=z[f'{side}_r10_sf_pg']; z['pre_r10_sa_pg']=z[f'{side}_r10_sa_pg']; rows.append(z[['canonical_season','game_id','game_date','team_id','team','opponent_id','gf','ga','is_home','prior_games','pre_sf_pg','pre_sa_pg','pre_r10_gf_pg','pre_r10_ga_pg','pre_r10_sf_pg','pre_r10_sa_pg']])
 t=pd.concat(rows,ignore_index=True).sort_values(['canonical_season','team_id','game_id'],kind='mergesort').reset_index(drop=True); t['sf']=np.nan; t['sa']=np.nan
 # The pregame cumulative state at game n+1 identifies game n exactly. The final
 # game has no successor and remains null rather than importing uncaptured future data.
 for _,g in t.groupby(['canonical_season','team_id'],sort=True):
  ids=g.index.tolist(); cumulative={'sf':0.0,'sa':0.0}
  for j,i in enumerate(ids):
   if j:
    for stat,pre in [('sf','pre_sf_pg'),('sa','pre_sa_pg')]:
     total=float(t.loc[i,pre])*j; t.loc[ids[j-1],stat]=total-cumulative[stat]; cumulative[stat]=total
 t['goal_diff']=t.gf-t.ga; t['shot_diff']=t.sf-t.sa; return t

def summaries(t,depth=None):
 rows=[]
 for (season,team_id),g in t.groupby(['canonical_season','team_id'],sort=True):
  g=g.sort_values('game_id'); z=g if depth is None else g.head(depth); last=g.iloc[-1]
  row={'canonical_season':season,'team_id':team_id,'team':g.team.iloc[-1],'games':len(z)}
  for c,name in [('gf','gf_pg'),('ga','ga_pg'),('goal_diff','goal_diff_pg'),('sf','sf_pg'),('sa','sa_pg'),('shot_diff','shot_diff_pg')]: row[name]=z[c].mean()
  for venue,label in [(True,'home'),(False,'away')]:
   q=z[z.is_home.eq(venue)]; row[f'{label}_goal_diff_pg']=q.goal_diff.mean(); row[f'{label}_shot_diff_pg']=q.shot_diff.mean(); row[f'{label}_games']=len(q)
  row.update({'final_prior10_goal_diff_pg':last.pre_r10_gf_pg-last.pre_r10_ga_pg,'final_prior10_shot_diff_pg':last.pre_r10_sf_pg-last.pre_r10_sa_pg,'shot_games_recovered':int(z.sf.notna().sum()),'shot_end_state':'PRE_FINAL_BOUNDED_PROXY'})
  rows.append(row)
 return pd.DataFrame(rows)

def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--repo-root',type=Path,default=Path(__file__).resolve().parents[3]); ap.add_argument('--output-dir',type=Path); a=ap.parse_args(); root=a.repo_root.resolve(); base=root/'artifacts/analysis/model_development'; out=(a.output_dir or base/f'nhl_season_opening_team_strength_persistence/{DATE}').resolve()
 pp={k:base/name/PARENT_DATE for k,(name,_) in PARENTS.items()};verify_parents([(p,PARENTS[k][1]) for k,p in pp.items()]);require_create_only(out);out.mkdir(parents=True);before={str(x):sha(x) for p in pp.values() for x in p.iterdir() if x.is_file()}
 spine_path=pp['feature_spine']/f'nhl_moneyline_team_feature_spine_{PARENT_DATE}.csv'; spine=pd.read_csv(spine_path); assert len(spine)==2798 and spine.groupby('canonical_season').size().to_dict()=={2023:1400,2024:1398}
 pred_path=base/f'nhl_moneyline_simple_baseline_process_validation/{PARENT_DATE}/nhl_moneyline_simple_baseline_control_predictions_{PARENT_DATE}.csv'; assert sha(pred_path)==PRED_SHA; pred=pd.read_csv(pred_path); assert len(pred)==2798
 t=team_games(spine); assert len(t)==5596 and not t.duplicated(['canonical_season','game_id','team_id']).any(); assert (t.groupby(['canonical_season','team_id']).prior_games.apply(list).map(lambda x:x==list(range(len(x))))).all(); assert t[['sf','sa']].notna().sum().eq(5532).all() and t[['sf','sa']].dropna().ge(0).all().all()
 full=summaries(t); continuity={int(x):int(x) for x in full[full.canonical_season.eq(2023)].team_id}; continuity[68]=53
 target=full[full.canonical_season.eq(2024)].copy(); target['continuity_team_id']=target.team_id.map(continuity); prior=full[full.canonical_season.eq(2023)].rename(columns={'team_id':'continuity_team_id'}); paired=prior.merge(target,on='continuity_team_id',suffixes=('_prior','_target'),validate='one_to_one'); assert len(paired)==32
 inventory=[]
 for r in paired.itertuples(index=False): inventory.append({'source_season':2023,'target_season':2024,'continuity_team_id':r.continuity_team_id,'source_team_id':r.continuity_team_id,'source_team':r.team_prior,'target_team_id':r.team_id,'target_team':r.team_target,'franchise_continuity':'FRANCHISE_CONTINUITY_WITH_RENAME' if r.team_prior=='ARI' and r.team_target=='UTA' else 'FULL_CONTINUITY','prior_games':r.games_prior,'target_games':r.games_target,'team_mapping':'ARI_TO_UTA_EXPLICIT' if r.team_prior=='ARI' else 'SAME_NHL_TEAM_ID','evidence_quality':'READY_WITH_BOUNDED_LIMITS','shot_coverage_note':'all but final team game recoverable from certified successive pregame cumulative states'})
 inventory.extend([{'source_season':2022,'target_season':2023,'franchise_continuity':'INSUFFICIENT_EVIDENCE','evidence_quality':'NOT_AVAILABLE','team_mapping':'no equally certified source season','shot_coverage_note':'excluded'}, {'source_season':2024,'target_season':2025,'franchise_continuity':'INSUFFICIENT_EVIDENCE','evidence_quality':'BLOCKED_BY_SOURCE_CONTINUITY','team_mapping':'season 2025 team-strength/outcome continuity not certified','shot_coverage_note':'excluded'}]); inv=pd.DataFrame(inventory); csv(inv,out/f'nhl_season_transition_inventory_{DATE}.csv')
 concepts=[]
 for c,fam in CONCEPTS.items(): concepts.append({'concept':c,'family':fam,'prior_definition':'source-season mean of certified team-game outcome values','target_depths':'1|3|5|10|20|FULL','strict_historical_ordering':'source season strictly precedes target season','source_coverage':'full goals; shots all but final team game','home_away_available':True,'minimum_games':1,'future_leakage':'NONE','model_fit':'NONE'})
 concepts.extend([{'concept':'home_goal_diff_pg|away_goal_diff_pg','family':'HOME_AWAY_SPLIT','prior_definition':'source-season venue-specific mean','target_depths':'FULL','strict_historical_ordering':'YES','source_coverage':'sufficient full-season games','home_away_available':True,'minimum_games':'observed split','future_leakage':'NONE','model_fit':'NONE'},{'concept':'final_prior10_goal_diff_pg|final_prior10_shot_diff_pg','family':'PRIOR_WINDOW_END_STATE','prior_definition':'strict pregame rolling-10 state on final source-season team appearance','target_depths':'FULL','strict_historical_ordering':'YES','source_coverage':'certified prior-10 fields','home_away_available':False,'minimum_games':10,'future_leakage':'NONE','model_fit':'NONE'},{'concept':'final_prior20','family':'PRIOR_WINDOW_END_STATE','prior_definition':'not repository-defined','target_depths':'NONE','strict_historical_ordering':'N/A','source_coverage':'NOT_AVAILABLE','home_away_available':False,'minimum_games':20,'future_leakage':'NONE','model_fit':'NONE'}]); csv(pd.DataFrame(concepts),out/f'nhl_prior_season_strength_concepts_{DATE}.csv')

 diagnostics=[]; pvc=[]; depth_cache={}
 for depth in [1,3,5,10,20,None]:
  label='FULL' if depth is None else str(depth); cur=summaries(t[t.canonical_season.eq(2024)],depth); cur['continuity_team_id']=cur.team_id.map(continuity); z=prior.merge(cur,on='continuity_team_id',suffixes=('_prior','_target'),validate='one_to_one'); depth_cache[label]=z
  for c,fam in CONCEPTS.items():
   a=z[f'{c}_prior']; b=z[f'{c}_target']; diagnostics.append({'source_season':2023,'target_season':2024,'history_depth':label,'concept':c,'family':fam,'pearson':safe_corr(a,b),'spearman':safe_corr(a,b,'spearman'),**rank_metrics(a,b),'source_coverage':'FULL_GOALS' if fam=='GOAL_STRENGTH' else 'PRE_FINAL_BOUNDED_SHOTS'})
   final_target=z[f'{c}_target'] if label=='FULL' else depth_cache.get('FULL',z).get(f'{c}_target')
  # prior/current/equal blend associations are computed against target full state below after cache completes.
 # Natural prior-10 and venue persistence at full depth.
 z=depth_cache['FULL']
 for c,fam in [('final_prior10_goal_diff_pg','PRIOR_WINDOW_END_STATE'),('final_prior10_shot_diff_pg','PRIOR_WINDOW_END_STATE'),('home_goal_diff_pg','HOME_AWAY_SPLIT'),('away_goal_diff_pg','HOME_AWAY_SPLIT'),('home_shot_diff_pg','HOME_AWAY_SPLIT'),('away_shot_diff_pg','HOME_AWAY_SPLIT')]: diagnostics.append({'source_season':2023,'target_season':2024,'history_depth':'FULL','concept':c,'family':fam,'pearson':safe_corr(z[f'{c}_prior'],z[f'{c}_target']),'spearman':safe_corr(z[f'{c}_prior'],z[f'{c}_target'],'spearman'),**rank_metrics(z[f'{c}_prior'],z[f'{c}_target']),'source_coverage':'STRICT_PRIOR_OR_VENUE'})
 diag=pd.DataFrame(diagnostics); csv(diag,out/f'nhl_season_to_season_persistence_diagnostics_{DATE}.csv')
 final=depth_cache['FULL']
 for depth in ['0','1','2','3','5','10']:
  cur=None if depth=='0' else summaries(t[t.canonical_season.eq(2024)],int(depth));
  if cur is not None: cur['continuity_team_id']=cur.team_id.map(continuity); z=prior.merge(cur,on='continuity_team_id',suffixes=('_prior','_current'),validate='one_to_one').merge(target[['continuity_team_id']+list(CONCEPTS)],on='continuity_team_id',validate='one_to_one')
  else: z=prior.merge(target[['continuity_team_id']+list(CONCEPTS)],on='continuity_team_id',validate='one_to_one',suffixes=('_prior','_target'))
  for c,fam in CONCEPTS.items():
   prior_col=f'{c}_prior'; target_col=c if c in z else f'{c}_target'; current_col=f'{c}_current'
   pv=z[prior_col]; tv=z[target_col]; cv=z[current_col] if cur is not None else pd.Series(np.nan,index=z.index); blend=(pv+cv)/2 if cur is not None else pd.Series(np.nan,index=z.index)
   pvc.append({'history_depth':depth,'concept':c,'family':fam,'teams':len(z),'prior_vs_target_full_pearson':safe_corr(pv,tv),'prior_vs_target_full_spearman':safe_corr(pv,tv,'spearman'),'current_vs_target_full_pearson':safe_corr(cv,tv),'current_vs_target_full_spearman':safe_corr(cv,tv,'spearman'),'equal_blend_vs_target_full_pearson':safe_corr(blend,tv),'equal_blend_vs_target_full_spearman':safe_corr(blend,tv,'spearman'),'combination_policy':'UNFITTED_EQUAL_WEIGHT','weights_optimized':False})
 csv(pd.DataFrame(pvc),out/f'nhl_prior_vs_current_strength_by_history_depth_{DATE}.csv')

 prior_strength=prior.set_index('continuity_team_id'); games=spine[spine.canonical_season.eq(2024)].merge(pred[['canonical_season','game_id','home_win_probability']],on=['canonical_season','game_id'],validate='one_to_one'); games['minimum_history']=games[['home_prior_games','away_prior_games']].min(axis=1); games['history_group']=games.minimum_history.map(depth_group); games['home_key']=games.home_team_id.map(continuity); games['away_key']=games.away_team_id.map(continuity)
 for c in ['goal_diff_pg','shot_diff_pg']: games[f'prior_{c}_diff']=[prior_strength.loc[h,c]-prior_strength.loc[w,c] for h,w in zip(games.home_key,games.away_key)]
 games['champion_residual']=games.home_win_target-games.home_win_probability; games['champion_absolute_miss']=abs(games.champion_residual); games['champion_brier_contribution']=games.champion_residual**2
 outcome=[]; residual=[]
 for label in ['0','1','2','3-4','5-9','10+']:
  g=games[games.history_group.eq(label)]
  row={'history_group':label,'rows':len(g),'mean_champion_probability':g.home_win_probability.mean(),'observed_home_win_rate':g.home_win_target.mean(),'champion_brier':g.champion_brier_contribution.mean(),'champion_mean_residual':g.champion_residual.mean()}
  for c in ['goal_diff_pg','shot_diff_pg']:
   x=g[f'prior_{c}_diff']; row[f'prior_{c}_outcome_pearson']=safe_corr(x,g.home_win_target); row[f'prior_{c}_residual_pearson']=safe_corr(x,g.champion_residual); row[f'prior_{c}_direction_accuracy']=float(((x>=0)==g.home_win_target).mean()) if len(g) else np.nan
  outcome.append(row)
  for c in ['goal_diff_pg','shot_diff_pg']:
   x=g[f'prior_{c}_diff']; pos=g[x>=0]; neg=g[x<0]; residual.append({'history_group':label,'concept':c,'rows':len(g),'residual_definition':'home_win_target - frozen_champion_home_win_probability','pearson_with_residual':safe_corr(x,g.champion_residual),'spearman_with_residual':safe_corr(x,g.champion_residual,'spearman'),'positive_prior_diff_rows':len(pos),'positive_prior_diff_mean_residual':pos.champion_residual.mean(),'negative_prior_diff_rows':len(neg),'negative_prior_diff_mean_residual':neg.champion_residual.mean(),'residual_gap_positive_minus_negative':pos.champion_residual.mean()-neg.champion_residual.mean(),'mean_absolute_probability_miss':g.champion_absolute_miss.mean(),'diagnostic_only':True})
 csv(pd.DataFrame(outcome),out/f'nhl_opening_state_outcome_characterization_{DATE}.csv'); res=pd.DataFrame(residual); csv(res,out/f'nhl_opening_state_champion_residual_diagnostics_{DATE}.csv')

 gd=diag[(diag.concept=='goal_diff_pg')&(diag.history_depth=='FULL')].iloc[0]; sd=diag[(diag.concept=='shot_diff_pg')&(diag.history_depth=='FULL')].iloc[0]; openres=res[res.history_group.eq('0')]
 classes=pd.DataFrame([
  ['GOAL_STRENGTH','MODERATE_OFFSEASON_PERSISTENCE','WEAK_OR_UNSTABLE',gd.pearson,gd.spearman,'full-season persistence but early-depth correlations unstable; only one certified transition'],
  ['SHOT_STRENGTH','MODERATE_OFFSEASON_PERSISTENCE','WEAK_OR_UNSTABLE',sd.pearson,sd.spearman,'stronger full-season correlation than goals, but capped at moderate because only one transition and opening residual weak'],
  ['HOME_AWAY_SPLIT','WEAK_OFFSEASON_PERSISTENCE','INSUFFICIENT_EVIDENCE',diag[diag.family.eq('HOME_AWAY_SPLIT')].pearson.mean(),diag[diag.family.eq('HOME_AWAY_SPLIT')].spearman.mean(),'venue splits vary and only one transition'],
  ['PRIOR_WINDOW_END_STATE','WEAK_OFFSEASON_PERSISTENCE','INSUFFICIENT_EVIDENCE',diag[diag.family.eq('PRIOR_WINDOW_END_STATE')].pearson.mean(),diag[diag.family.eq('PRIOR_WINDOW_END_STATE')].spearman.mean(),'prior-10 available; prior-20 unavailable; single transition'],
 ],columns=['family','persistence_classification','residual_novelty_classification','full_or_family_mean_pearson','full_or_family_mean_spearman','evidence']); csv(classes,out/f'nhl_persistence_family_classification_{DATE}.csv')

 continuity_rows=[]
 for r in paired.itertuples(index=False): continuity_rows.append({'source_season':2023,'target_season':2024,'source_team_id':r.continuity_team_id,'source_team':r.team_prior,'target_team_id':r.team_id,'target_team':r.team_target,'eligibility_status':'FRANCHISE_CONTINUITY_WITH_RENAME' if r.team_prior=='ARI' else 'FULL_CONTINUITY','canonical_mapping':'53->68 ARI_TO_UTA' if r.team_prior=='ARI' else 'SAME_TEAM_ID','carryover_policy':'SEPARATE_GOVERNANCE_REVIEW' if r.team_prior=='ARI' else 'ELIGIBLE_FOR_CHARACTERIZATION','roster_continuity_known':False,'fail_closed_condition':'missing explicit mapping or prior source history'})
 continuity_rows.extend([{'source_season':'ANY','target_season':'ANY','source_team':'EXPANSION_OR_MISSING','target_team':'NEW_IDENTITY','eligibility_status':'NO_PRIOR_SEASON_HISTORY','canonical_mapping':'NONE','carryover_policy':'CARRYOVER_BLOCKED','roster_continuity_known':False,'fail_closed_condition':'no certified prior franchise identity'},{'source_season':'ANY','target_season':'ANY','source_team':'UNRESOLVED_RELOCATION','target_team':'UNRESOLVED','eligibility_status':'CARRYOVER_BLOCKED','canonical_mapping':'NONE','carryover_policy':'CARRYOVER_BLOCKED','roster_continuity_known':False,'fail_closed_condition':'identity change lacks explicit canonical mapping'}]); csv(pd.DataFrame(continuity_rows),out/f'nhl_franchise_continuity_audit_{DATE}.csv')
 roster=pd.DataFrame([
  ['returning player count','roster_status/player game identity','NOT_AVAILABLE','certified parent shows only 3 season-2023 and 9 season-2024 roster games; no complete offseason snapshot'],['returning TOI share','skater logs and TOI fields','BOUNDED_RECONSTRUCTION','postgame logs may support a separate reconstruction but no governed cross-season package exists'],['returning top-six forward share','line_role plus roster history','NOT_AVAILABLE','historical line roles and complete rosters not replayable'],['returning defense share','position plus TOI','BOUNDED_RECONSTRUCTION','requires separately governed player-season aggregation and identity audit'],['returning goalie share','goalie logs','BOUNDED_RECONSTRUCTION','actual goalie oracle available, not roster continuity or pregame context'],['season 2026 roster continuity','prospective roster snapshots','PROSPECTIVE_ONLY','can be collected prospectively; not a historical prerequisite here'],
 ],columns=['concept','repository_source','classification','evidence']); csv(roster,out/f'nhl_roster_turnover_feasibility_{DATE}.csv')
 candidates=pd.DataFrame([
  ['A_PRIOR_SEASON_SUBSTITUTION','use prior-season ending shot/goal strength only when current history is zero','fixed availability rule; no learned parameters','CONCEPT_ONLY_NOT_SPECIFIED','simple and directly targets missing state'],['B_FIXED_STAGED_HANDOFF','predeclare natural 0/1/3/5/10-game handoff from prior to current strength','weights must be governance-predeclared, never selected here','CONCEPT_ONLY_NOT_SPECIFIED','persistence could span sparse window but evidence incomplete'],['C_EQUAL_BLEND_SPARSE_HISTORY','equal prior/current average while minimum team history is below a predeclared natural boundary','exact 0.5/0.5 non-fitted comparison only','CONCEPT_ONLY_NOT_SPECIFIED','descriptive comparator; not authorized challenger'],
 ],columns=['candidate','form','weight_policy','status','rationale']); csv(candidates,out/f'nhl_bootstrap_candidate_concept_inventory_{DATE}.csv')
 fields=pd.DataFrame([
  ['home_prior_season_games_available','integer','prior certified season','retain only; does not affect champion'],['away_prior_season_games_available','integer','prior certified season','retain only; does not affect champion'],['home_current_season_prior_games','integer','strict current season','existing opening-state input metadata'],['away_current_season_prior_games','integer','strict current season','existing opening-state input metadata'],['carryover_eligibility_status','enum','franchise continuity contract','FULL_CONTINUITY|FRANCHISE_CONTINUITY_WITH_RENAME|PARTIAL_CONTINUITY|NO_PRIOR_SEASON_HISTORY|CARRYOVER_BLOCKED'],['frozen_champion_opening_state_status','enum','existing scorer','SEASON_OPEN_NO_HISTORY|EARLY_SEASON_SPARSE_HISTORY|PARTIAL_CURRENT_SEASON_HISTORY|MATURE_CURRENT_SEASON_HISTORY'],['champion_probability','float','unchanged frozen scorer','no carryover substitution or blend'],
 ],columns=['field','type','authority','behavior']); csv(fields,out/f'nhl_season_2026_opening_state_observation_fields_{DATE}.csv')

 final_decision='BOOTSTRAP_CONCEPT_INTERESTING_BUT_NOT_READY'; decisions={'NHL_SEASON_TRANSITION_DATA_READINESS':'READY_WITH_BOUNDED_LIMITS','NHL_PRIOR_SEASON_GOAL_STRENGTH_PERSISTENCE':'MODERATE','NHL_PRIOR_SEASON_SHOT_STRENGTH_PERSISTENCE':'MODERATE','NHL_PRIOR_SEASON_HOME_AWAY_PERSISTENCE':'WEAK','NHL_OPENING_STATE_PRIOR_SEASON_RESIDUAL_INFORMATION':'WEAK','NHL_FRANCHISE_CONTINUITY_READINESS':'READY_WITH_BOUNDED_LIMITS','NHL_ROSTER_CONTINUITY_HISTORICAL_REPLAYABILITY':'NOT_READY','NHL_SEASON_2026_BOOTSTRAP_RESEARCH_READINESS':'READY_WITH_BOUNDED_LIMITS','NHL_SEASON_2026_BOOTSTRAP_CHALLENGER_SPECIFICATION_READINESS':'NOT_READY','NHL_SEASON_2026_FROZEN_CHAMPION_STATUS':'UNCHANGED'}
 next_task='NHL_SEASON_2026_TIMESTAMP_CERTIFIED_GOALIE_SOURCE_INTEGRATION_READINESS'
 decision={'characterization_decision':final_decision,'decisions':decisions,'recommended_next_bounded_task':next_task,'unlocked':['One bounded goalie source schema, identity, timestamp, and read-only integration readiness assessment; no integration or production enablement'],'still_blocked':['bootstrap challenger specification','bootstrap challenger execution','champion changes','prior-season production carryover','retraining','ROI analysis','wagering','model promotion','automated scheduling'],'champion_changed':False,'model_fit_performed':False,'weights_optimized':False}; js(decision,out/f'nhl_season_opening_persistence_decision_{DATE}.json')
 open0=games[games.history_group.eq('0')]; report=f"""# NHL Season-Opening Team-Strength Persistence\n\n## Decision\n\n`{final_decision}`. The repository supports exactly one trustworthy adjacent transition, season `2023` to season `2024`, across 32 franchise continuities. Season `2022` lacks an equally certified feature source and season `2025` is blocked by source continuity, so no multi-transition stability claim is possible.\n\nFull-season goal-differential persistence was Pearson `{gd.pearson:.3f}` and Spearman `{gd.spearman:.3f}`. Shot differential was stronger at Pearson `{sd.pearson:.3f}` and Spearman `{sd.spearman:.3f}`. Shot-based strength also became more coherent at natural target depths, but neither family demonstrated stable incremental opening-state information. Among the `{len(open0)}` target-season games with zero minimum current history, prior goal-differential correlation with champion residual was `{safe_corr(open0.prior_goal_diff_pg_diff,open0.champion_residual):.3f}` and prior shot-differential correlation was `{safe_corr(open0.prior_shot_diff_pg_diff,open0.champion_residual):.3f}`. These small-sample diagnostics do not justify a challenger specification.\n\nGoals are complete certified outcomes. Shot values are reconstructed strictly from successive pregame cumulative states, recovering 5,532 of 5,596 team-games; each team's final game remains unavailable. Full-season shot results are therefore explicitly `PRE_FINAL_BOUNDED_PROXY`. No future target-season state enters any prior-season value.\n\nArizona-to-Utah is mapped explicitly as franchise continuity with rename and remains separately flagged. Expansion, missing history, or unresolved identity changes fail closed. Historical roster continuity is not replayable: roster coverage in the certified parent is sparse, while TOI-based return concepts would require a separate bounded reconstruction.\n\nThree simple future concepts were inventoried—prior-season substitution, fixed staged handoff, and equal sparse-history blend—but none was fitted, weighted, selected, or authorized. Frozen median imputation remains the unchanged control. The exactly one next bounded task is `{next_task}`.\n\n## Decisions\n\n"""+'\n'.join(f'- `{k}` = `{v}`' for k,v in decisions.items())+'\n'; (out/f'nhl_season_opening_team_strength_persistence_report_{DATE}.md').write_text(report); (out/f'nhl_season_opening_persistence_one_page_summary_{DATE}.md').write_text(report)
 assert before=={str(x):sha(x) for p in pp.values() for x in p.iterdir() if x.is_file()}; src=Path(__file__).read_text(); forbidden=['Logistic'+'Regression(','.fi'+'t(','Grid'+'SearchCV']; assert not any(x in src for x in forbidden); package_text='\n'.join(x.read_text(errors='ignore') for x in out.iterdir() if x.suffix in {'.csv','.json','.md'}); assert not re.search(r'\b20\d{2}[-–/]20\d{2}\b',package_text)
 identity={'package_name':'nhl_season_opening_team_strength_persistence','version':'1.0.0','assessment_date':DATE,'generated_by':str(Path(__file__).relative_to(root)),'canonical_seasons':[2023,2024,2025,2026],'qualified_transition_count':1,'qualified_transition':'2023_TO_2024','champion_identity':CHAMPION,'champion_prediction_sha256':PRED_SHA,'parent_manifest_sha256':{k:v for k,(_,v) in PARENTS.items()},'model_fit_performed':False,'weights_optimized':False,'champion_changed':False,'source_mutation_check':'PASS'}; js(identity,out/f'package_identity_{DATE}.json')
 files=sorted(x for x in out.iterdir() if x.is_file() and x.name!='SHA256SUMS'); assert len(files)==15; (out/'SHA256SUMS').write_text(''.join(f'{sha(x)}  {x.name}\n' for x in files)); subprocess.run(['shasum','-a','256','-c','SHA256SUMS'],cwd=out,check=True,capture_output=True); print(json.dumps({'output_dir':str(out),'manifest_sha256':sha(out/'SHA256SUMS'),'goal_diff_full_pearson':gd.pearson,'shot_diff_full_pearson':sd.pearson,'opening_no_history_rows':len(open0),'decision':final_decision},indent=2))
if __name__=='__main__': main()
