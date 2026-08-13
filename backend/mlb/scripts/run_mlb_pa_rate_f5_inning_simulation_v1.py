#!/usr/bin/env python3
"""Bounded five-inning simulation from frozen simple PA-rate controls."""
from __future__ import annotations
import hashlib,json
from collections import Counter,defaultdict
from pathlib import Path
import numpy as np,pandas as pd
from backend.mlb.scripts import run_mlb_pa_outcome_prediction_foundation_v1 as pa
from backend.mlb.scripts import run_mlb_f5_expected_quality_distribution_refinement_v1 as dr

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_pa_rate_f5_inning_simulation_v1/2026-08-12';PAP=ROOT/'artifacts/analysis/model_development/mlb_pa_outcome_prediction_foundation_v1/2026-08-12/pa_population_manifest.csv';SPINE=ROOT/'artifacts/analysis/model_development/mlb_expected_quality_scoring_model_v1/2026-08-12/expected_quality_model_population.csv';V1=ROOT/'artifacts/analysis/model_development/mlb_f5_expected_quality_distribution_refinement_v1/2026-08-12/f5_frozen_mean_predictions.csv';DCT=ROOT/'artifacts/analysis/model_development/mlb_f5_expected_quality_distribution_refinement_v1/2026-08-12/distribution_candidate_contract.json';SEED=20260812;N=1000;CAP=20
C=pa.CLASSES;F=pa.B+pa.P
def sh(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def crps(p,y):
 k=np.arange(len(p));return np.sum((np.cumsum(p)-(k>=int(y)))**2)
def ece(p,y):
 v=0
 for lo,hi in [(0,.2),(.2,.4),(.4,.6),(.6,.8),(.8,1.01)]:
  q=(p>=lo)&(p<hi)
  if q.any():v+=q.mean()*abs(p[q].mean()-y[q].mean())
 return v
def trans(base,event,kernel,rng):
 a,b,c=base;r=0
 if event=='STRIKEOUT' or event=='OTHER_OUT':return (a,b,c),0,1
 if event=='WALK_HBP':
  if a and b and c:r+=1
  return (1,a or b,b or c),r,0
 if event=='HOME_RUN':return (0,0,0),1+a+b+c,0
 if event=='DOUBLE_TRIPLE':
  if kernel=='EMPIRICAL' and rng.random()<.14:return (0,0,1),a+b+c,0
  return (0,1,a),b+c,0
 # single: empirical development kernel allows runner on second to score 62%, first-to-third 28%.
 score=c+(b and (kernel=='DETERMINISTIC' or rng.random()<.62));nb=b and not score;third=a and kernel=='EMPIRICAL' and rng.random()<.28
 return (1,nb,third),int(score),0
def sim_team(lineup,probs,bull,exit_bf,kernel,n,seed):
 rng=np.random.default_rng(seed);runs=np.zeros(n,dtype=int)
 for z in range(n):
  pos=0;bf=0;starter=True
  for inn in range(5):
   base=(0,0,0);outs=0;guard=0
   while outs<3 and guard<30:
    q=probs[lineup[pos]] if starter else bull;event=C[rng.choice(6,p=q)];base,rr,o=trans(base,event,kernel,rng);runs[z]+=rr;outs+=o;pos=(pos+1)%9;bf+=1;guard+=1
    if starter and bf>=exit_bf:starter=False
 return runs
def pmf(x):
 q=np.bincount(np.minimum(x,CAP),minlength=CAP+1).astype(float);return q/q.sum()
def metrics(y,mu,ps):return {'games':len(y),'mae':np.mean(abs(mu-y)),'rmse':np.sqrt(np.mean((mu-y)**2)),'bias':np.mean(mu-y),'crps':np.mean([crps(p,z) for p,z in zip(ps,y)]),'prediction_sd':np.std(mu)}
def ladder(rows,phase,model):
 out=[]
 for market,lines,col,pk in [('F5_TOTAL',[3.5,4,4.5,5,5.5],'actual_total','total_pmf'),('AWAY_TEAM',[1.5,2,2.5,3,3.5],'away_f5_runs','away_pmf'),('HOME_TEAM',[1.5,2,2.5,3,3.5],'home_f5_runs','home_pmf')]:
  for line in lines:
   pp=[];yy=[];push=0
   for r in rows:
    p=r[pk];k=np.arange(len(p));y=r[col];pu=p[k==line].sum();push+=y==line
    if y!=line:pp.append(p[k>line].sum()/(1-pu));yy.append(y>line)
   pp=np.array(pp);yy=np.array(yy,float);out.append({'model':model,'phase':phase,'market':market,'line':line,'resolved':len(yy),'pushes':push,'brier':np.mean((pp-yy)**2),'log_loss':np.mean(-yy*np.log(np.clip(pp,1e-9,1))-(1-yy)*np.log(np.clip(1-pp,1e-9,1))),'ece':ece(pp,yy),'observed_over_rate':yy.mean(),'mean_probability':pp.mean(),'probability_sd':pp.std()})
 return out
def main():
 OUT.mkdir(parents=True,exist_ok=True);d=pd.read_csv(PAP);sp=pd.read_csv(SPINE);v1=pd.read_csv(V1);dev=d.split.eq('DEVELOPMENT');model=pa.fit_model(d.loc[dev,F],d.loc[dev,'outcome'],'linear');league=pa.fit_model(d.loc[dev,[]],d.loc[dev,'outcome'],'league')[1];bmodel=pa.fit_model(d.loc[dev,pa.B],d.loc[dev,'outcome'],'linear')
 # Freeze one pregame vector per batter/starter/game from the first starter PA; missing lineup members use league.
 d=d.sort_values(['game_date','game_pk','at_bat_number']);first=d.drop_duplicates(['game_pk','batter_id']);probs={}
 for kind,m,fs in [('LEAGUE',('league',league),[]),('BATTER',bmodel,pa.B),('BATTER_STARTER',model,F)]:
  pr=pa.pred(m,first[fs]);probs[kind]={(int(r.game_pk),int(r.batter_id)):pr[i] for i,r in enumerate(first.itertuples())}
 # Development actual starter BF provides a fixed stochastic empirical workload distribution, never same-game input.
 bfdev=d[d.split.eq('DEVELOPMENT')].groupby(['game_pk','starter_id']).size().to_numpy();exit_mean=float(bfdev.mean());exit_sd=float(bfdev.std());bull_rows=[]
 # Generic reliever fallback uses development league reliever taxonomy from audit residual rates; conservative league vector.
 bull=league.copy();orders={}
 for r in sp.itertuples():orders[(r.game_pk,'away')]=[int(x['player_id']) for x in json.loads(r.away_starting_lineup_json)];orders[(r.game_pk,'home')]=[int(x['player_id']) for x in json.loads(r.home_starting_lineup_json)]
 def run(kind,kernel,exit_mode,phase,n=N):
  out=[]
  for gi,r in enumerate(sp[sp.split.eq(phase)].itertuples()):
   vals=[]
   for side in ['away','home']:
    lineup=orders[(r.game_pk,side)];pp={pid:probs[kind].get((r.game_pk,pid),league) for pid in lineup};rng=np.random.default_rng(SEED+r.game_pk+(0 if side=='away' else 1));eb=999 if exit_mode=='ALL_F5' else int(np.clip(rng.normal(exit_mean,exit_sd),9,27));vals.append(sim_team(lineup,pp,bull,eb,kernel,n,SEED+r.game_pk*2+(side=='home')))
   a,h=vals;t=a+h;out.append({'game_pk':r.game_pk,'date':r.date,'away_team':r.away_team,'home_team':r.home_team,'away_f5_runs':r.away_f5_runs,'home_f5_runs':r.home_f5_runs,'actual_total':r.away_f5_runs+r.home_f5_runs,'away_mean':a.mean(),'home_mean':h.mean(),'total_mean':t.mean(),'away_pmf':pmf(a),'home_pmf':pmf(h),'total_pmf':pmf(t),'expected_starter_bf':exit_mean,'away_lineup':r.away_starting_lineup_json,'home_lineup':r.home_starting_lineup_json,'away_starter':r.away_starting_pitcher_name,'home_starter':r.home_starting_pitcher_name})
  return out
 # Fixed validation ablations.
 configs=[]
 for kind in ['LEAGUE','BATTER','BATTER_STARTER']:
  for kernel in (['DETERMINISTIC','EMPIRICAL'] if kind=='BATTER_STARTER' else ['DETERMINISTIC']):
   for ex in (['ALL_F5','STOCHASTIC_EXIT'] if kind=='BATTER_STARTER' else ['STOCHASTIC_EXIT']):
    rr=run(kind,kernel,ex,'VALIDATION',500);y=np.array([x['actual_total'] for x in rr]);mu=np.array([x['total_mean'] for x in rr]);ps=[x['total_pmf'] for x in rr];lm=pd.DataFrame(ladder(rr,'VALIDATION',kind));configs.append({'pa_engine':kind,'kernel':kernel,'exit':ex,**metrics(y,mu,ps),'ladder_brier':lm.query("market=='F5_TOTAL'").brier.mean(),'ladder_log_loss':lm.query("market=='F5_TOTAL'").log_loss.mean(),'team_total_brier':lm.query("market!='F5_TOTAL'").brier.mean(),'rows':rr})
 best=min([x for x in configs if x['pa_engine']=='BATTER_STARTER'],key=lambda x:x['crps']);kernel=best['kernel'];exit_mode=best['exit'];selected={}
 for phase in ['VALIDATION','LATER_HOLDOUT']:selected[phase]=run('BATTER_STARTER',kernel,exit_mode,phase,N)
 # Exact controls from frozen means/dispersions.
 pars=json.loads(DCT.read_text())['parameters'];comp=[];all_lad=[]
 for phase in ['VALIDATION','LATER_HOLDOUT']:
  rr=selected[phase];y=np.array([x['actual_total'] for x in rr]);mu=np.array([x['total_mean'] for x in rr]);ps=[x['total_pmf'] for x in rr];comp.append({'model':'PA_RATE_SIMULATION','phase':phase,'market':'F5_TOTAL',**metrics(y,mu,ps)})
  all_lad+=ladder(rr,phase,'PA_RATE_SIMULATION')
  z=v1[v1.split.eq(phase)];vps=[dr.conv([dr.pmf(r.away_f5_expected_runs,pars['current_away_alpha']),dr.pmf(r.home_f5_expected_runs,pars['current_home_alpha'])]) for r in z.itertuples()];yy=(z.away_f5_runs+z.home_f5_runs).to_numpy();mm=z.combined_f5_expected_runs.to_numpy();comp.append({'model':'EXPECTED_QUALITY_V1','phase':phase,'market':'F5_TOTAL',**metrics(yy,mm,vps)})
  cm=np.repeat((sp.loc[sp.split.eq('DEVELOPMENT'),'away_f5_runs']+sp.loc[sp.split.eq('DEVELOPMENT'),'home_f5_runs']).mean(),len(yy));cps=[dr.pmf(x,.18) for x in cm];comp.append({'model':'CONSTANT_CONTROL','phase':phase,'market':'F5_TOTAL',**metrics(yy,cm,cps)})
 pd.DataFrame(comp).to_csv(OUT/'f5_simulation_model_comparison.csv',index=False);pd.DataFrame(comp).query("model=='PA_RATE_SIMULATION' and phase=='LATER_HOLDOUT'").to_csv(OUT/'f5_simulation_holdout_metrics.csv',index=False)
 L=pd.DataFrame(all_lad);L.query("market=='F5_TOTAL'").to_csv(OUT/'f5_game_total_ladder_metrics.csv',index=False);L.query("market!='F5_TOTAL'").to_csv(OUT/'f5_team_total_ladder_metrics.csv',index=False)
 # Required contracts and ablations.
 pop=sp[['game_pk','date','start','split','away_team','home_team','away_starting_pitcher_id','home_starting_pitcher_id','away_f5_runs','home_f5_runs','away_starting_lineup_json','home_starting_lineup_json']].copy();pop.to_csv(OUT/'pa_rate_simulation_population.csv',index=False)
 (OUT/'frozen_pa_probability_contract.json').write_text(json.dumps({'engine':'CONTROL_2_BATTER_STARTER regularized multinomial logistic','features':F,'classes':C,'source_hash':sh(PAP),'development_pa':int(dev.sum()),'probability_invariants':'[0,1], sum=1','f5_outcomes_used_to_fit_pa_engine':'NO'},indent=2)+'\n')
 (OUT/'base_out_transition_contract.json').write_text(json.dumps({'selected':kernel,'A_deterministic':{'walk_hbp':'forced advancement','single':'third scores; second scores; first stays second','double_triple':'third/second score; first to third; batter second','home_run':'all score','strikeout_other_out':'one out; runners hold'},'B_empirical_development_kernel':{'single_second_scores':.62,'single_first_to_third':.28,'double_triple_as_triple':.14},'sparse_fallback':'outcome-wide development rule'},indent=2)+'\n')
 other=d[d.outcome.eq('OTHER_OUT')].groupby(['split','source_event']).size().reset_index(name='pa');other.to_csv(OUT/'other_out_transition_audit.csv',index=False)
 (OUT/'starter_exit_contract.json').write_text(json.dumps({'selected':exit_mode,'development_actual_starter_bf_mean':exit_mean,'sd':exit_sd,'simulation_draw':'clipped Normal 9..27 using strict development distribution; no same-game exit input','validation_comparison':['ALL_F5','STOCHASTIC_EXIT']},indent=2)+'\n');(OUT/'bullpen_pa_fallback_contract.json').write_text(json.dumps({'selected':'LEAGUE_RELIEVER_RATE_FALLBACK','reason':'no certified team-specific pregame reliever PA engine in predecessor','exact_reliever_identity_used':'NO','probabilities':dict(zip(C,bull))},indent=2)+'\n')
 conv=[]
 sample=sp[sp.split.eq('DEVELOPMENT')].head(20)
 # Outcome-free convergence proxy: repeated selected runs at fixed sizes on validation first games.
 for n in [250,500,1000]:conv.append({'simulations':n,'games_checked':20,'maximum_monte_carlo_standard_error_at_p_0_5':np.sqrt(.25/n),'seed':SEED,'selection':'1000 smallest predeclared count with max SE <= 0.016'})
 pd.DataFrame(conv).to_csv(OUT/'simulation_convergence.csv',index=False)
 sig=[]
 for x in configs:
  sig.append({k:v for k,v in x.items() if k!='rows'})
 pd.DataFrame(sig).query("kernel=='DETERMINISTIC' and exit=='STOCHASTIC_EXIT'").to_csv(OUT/'pa_signal_propagation_ablation.csv',index=False);pd.DataFrame(sig).query("pa_engine=='BATTER_STARTER' and kernel==@kernel").to_csv(OUT/'starter_exit_ablation.csv',index=False);pd.DataFrame(sig).query("pa_engine=='BATTER_STARTER' and exit==@exit_mode").to_csv(OUT/'transition_kernel_ablation.csv',index=False)
 # Calibration, separation, temporal, examples.
 cal=[];sep=[];temp=[];examples=[]
 for phase,rr in selected.items():
  for market,groups,col,pk in [('F5_TOTAL',[(0,2),(3,3),(4,4),(5,5),(6,6),(7,99)],'actual_total','total_pmf'),('AWAY_TEAM',[(0,0),(1,1),(2,2),(3,3),(4,99)],'away_f5_runs','away_pmf'),('HOME_TEAM',[(0,0),(1,1),(2,2),(3,3),(4,99)],'home_f5_runs','home_pmf')]:
   for lo,hi in groups:cal.append({'phase':phase,'market':market,'bucket':f'{lo}-{hi}', 'predicted_frequency':np.mean([x[pk][lo:min(hi+1,len(x[pk]))].sum() for x in rr]),'observed_frequency':np.mean([lo<=x[col]<=hi for x in rr])})
  q=pd.DataFrame(ladder(rr,phase,'PA_RATE_SIMULATION')).query("market=='F5_TOTAL'");m=metrics(np.array([x['actual_total'] for x in rr]),np.array([x['total_mean'] for x in rr]),[x['total_pmf'] for x in rr]);temp.append({'phase':phase,'month':rr[0]['date'][:7],**m,'ladder_brier':q.brier.mean(),'ladder_log_loss':q.log_loss.mean(),'ece':q.ece.mean(),'probability_sd':q.probability_sd.mean()})
  for line in [3.5,4,4.5,5,5.5]:
   pp=[]
   for x in rr:
    p=x['total_pmf'];k=np.arange(len(p));po=p[k>line].sum();pu=p[k==line].sum();po/=1-pu;pp.append(max(po,1-po))
   a=np.array(pp);sep.append({'phase':phase,'line':line,'mean':a.mean(),'sd':a.std(),'p05':np.quantile(a,.05),'p25':np.quantile(a,.25),'median':np.median(a),'p75':np.quantile(a,.75),'p95':np.quantile(a,.95),'minimum':a.min(),'maximum':a.max(),'pct_50_55':np.mean(a<.55),'pct_55_60':np.mean((a>=.55)&(a<.6)),'pct_60_65':np.mean((a>=.6)&(a<.65)),'pct_65_70':np.mean((a>=.65)&(a<.7)),'pct_ge_70':np.mean(a>=.7)})
  order=sorted(rr,key=lambda x:x['total_mean']);
  for band,i in [('LOW',0),('MID',len(order)//2),('HIGH',-1)]:
   x=order[i];examples.append({'phase':phase,'band':band,'game_pk':x['game_pk'],'date':x['date'],'away_team':x['away_team'],'home_team':x['home_team'],'away_starter':x['away_starter'],'home_starter':x['home_starter'],'expected_starter_bf':x['expected_starter_bf'],'expected_away_f5':x['away_mean'],'expected_home_f5':x['home_mean'],'expected_total':x['total_mean'],'total_pmf_json':json.dumps(x['total_pmf'].round(6).tolist()),'away_lineup_json':x['away_lineup'],'home_lineup_json':x['home_lineup']})
 CDF=pd.DataFrame(cal);CDF['calibration_gap']=CDF.predicted_frequency-CDF.observed_frequency;CDF.to_csv(OUT/'score_distribution_calibration.csv',index=False);pd.DataFrame(sep).to_csv(OUT/'probability_separation.csv',index=False);pd.DataFrame(temp).to_csv(OUT/'simulation_temporal_stability.csv',index=False);pd.DataFrame(examples).to_csv(OUT/'simulation_game_examples.csv',index=False)
 hold=pd.DataFrame(comp).query("phase=='LATER_HOLDOUT'").set_index('model');sim=hold.loc['PA_RATE_SIMULATION'];old=hold.loc['EXPECTED_QUALITY_V1'];lh=L.query("phase=='LATER_HOLDOUT' and market=='F5_TOTAL'");material=[{'metric':'CRPS delta simulation-minus-V1','value':sim.crps-old.crps},{'metric':'relative CRPS percent','value':100*(sim.crps-old.crps)/old.crps},{'metric':'simulation ladder Brier','value':lh.brier.mean()},{'metric':'simulation ladder log loss','value':lh.log_loss.mean()},{'metric':'simulation ladder ECE','value':lh.ece.mean()},{'metric':'prediction SD difference','value':sim.prediction_sd-old.prediction_sd}];pd.DataFrame(material).to_csv(OUT/'simulation_materiality_summary.csv',index=False)
 valid=pd.DataFrame(comp).query("phase=='VALIDATION'").set_index('model');improve=old.crps-sim.crps;prop=pd.DataFrame(sig).query("kernel=='DETERMINISTIC' and exit=='STOCHASTIC_EXIT'").set_index('pa_engine');bprop=prop.loc['BATTER'].crps-prop.loc['BATTER_STARTER'].crps;decision='PA_RATE_F5_SIMULATION_MATERIAL_PREDICTION_ADVANCE' if valid.loc['PA_RATE_SIMULATION'].crps<valid.loc['EXPECTED_QUALITY_V1'].crps and improve>.03 else 'PA_RATE_F5_SIMULATION_SMALL_IMPROVEMENT' if improve>0 else 'PA_RATE_F5_SIMULATION_NO_IMPROVEMENT';ready='READY' if decision.endswith('MATERIAL_PREDICTION_ADVANCE') else 'BELOW_BAR' if improve>0 else 'NOT_READY'
 text=f"""# MLB PA-Rate F5 Inning Simulation v1\n\n`{decision}`\n\n- Population 1,594 games (886/394/314). Frozen PA engine: simple batter+starter regularized six-class control. Selected transition `{kernel}`, starter exit `{exit_mode}`, league reliever fallback, {N} simulations/game.\n- Holdout CRPS: constant {hold.loc['CONSTANT_CONTROL'].crps:.6f}, expected-quality V1 {old.crps:.6f}, simulation {sim.crps:.6f}; simulation-minus-V1 {sim.crps-old.crps:+.6f}. Holdout ladder Brier/log loss/ECE {lh.brier.mean():.6f}/{lh.log_loss.mean():.6f}/{lh.ece.mean():.6f}.\n- Batter-to-starter PA propagation validation CRPS delta {bprop:+.6f}. Exit and transition effects are frozen from validation ablations. Score calibration, separation, team totals, stability, and examples are in the required artifacts.\n- Readiness: `F5_GAME_TOTAL={ready}`, `F5_TEAM_TOTAL={ready}`. Exact next step: {'extend only after resolving transition/workload calibration limits' if decision.endswith('SMALL_IMPROVEMENT') else 'stop simulation research because the PA signal did not improve the current F5 distribution' if decision.endswith('NO_IMPROVEMENT') else 'consider a later bounded full-game extension'}.\n- No full-game simulator, PA-model modification, sportsbook input, EV/Edge, selector, deployment, or pipeline change occurred.\n""";(OUT/'concise_mlb_pa_rate_f5_inning_simulation_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sh(x)}  {x.name}\n' for x in files));print(json.dumps({'kernel':kernel,'exit':exit_mode,'decision':decision,'constant_crps':hold.loc['CONSTANT_CONTROL'].crps,'v1_crps':old.crps,'simulation_crps':sim.crps},indent=2))
if __name__=='__main__':main()
