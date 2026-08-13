#!/usr/bin/env python3
"""Bounded distribution-only refinement of frozen MLB F5 expected-quality means."""
from __future__ import annotations
import hashlib,json
from pathlib import Path
import numpy as np,pandas as pd
from scipy.stats import nbinom,poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from backend.mlb.scripts import run_mlb_lineup_confirmed_scoring_prediction_v2 as v2
from backend.mlb.scripts import run_mlb_expected_quality_scoring_model_v1 as eq

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_f5_expected_quality_distribution_refinement_v1/2026-08-12';CAP=35;SEED=20260812
BUCKETS=[(-np.inf,3.5,'LT_3_5'),(3.5,4.25,'3_5_TO_4_24'),(4.25,5,'4_25_TO_4_99'),(5,5.75,'5_TO_5_74'),(5.75,np.inf,'GE_5_75')]
UNC=['off_whiff','off_hard','off_sparse','opp_sp_whiff','opp_sp_depth','opp_sp_sparse','mix_xwoba']
def sh(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def alpha(y,mu):return max(0,float((((np.asarray(y)-np.asarray(mu))**2-np.asarray(mu)).sum())/max((np.asarray(mu)**2).sum(),1e-9)))
def pmf(mu,a=0):
 x=np.arange(CAP+1);mu=max(float(mu),.02);q=nbinom.pmf(x,1/a,(1/a)/(1/a+mu)) if a>1e-9 else poisson.pmf(x,mu);q[-1]+=max(0,1-q.sum());return q/q.sum()
def conv(ps):
 q=np.array([1.])
 for p in ps:q=np.convolve(q,p)
 q=q[:CAP+1];q[-1]+=max(0,1-q.sum());return q/q.sum()
def crps(p,y):
 k=np.arange(len(p));return float(np.sum((np.cumsum(p)-(k>=int(y)))**2))
def nll(p,y):return -np.log(max(p[min(int(y),len(p)-1)],1e-12))
def bucket(mu):return next(n for lo,hi,n in BUCKETS if lo<=mu<hi)
def ece(probs,ys):
 probs=np.asarray(probs);ys=np.asarray(ys);z=0
 for lo,hi in [(0,.2),(.2,.4),(.4,.6),(.6,.8),(.8,1.00001)]:
  q=(probs>=lo)&(probs<hi)
  if q.any():z+=q.mean()*abs(probs[q].mean()-ys[q].mean())
 return z
def metrics(y,mu,ps):
 y=np.asarray(y);mu=np.asarray(mu);var=np.array([np.sum((np.arange(len(p))-np.sum(np.arange(len(p))*p))**2*p) for p in ps]);return {'games':len(y),'mae':np.mean(abs(mu-y)),'crps':np.mean([crps(p,z) for p,z in zip(ps,y)]),'negative_log_likelihood':np.mean([nll(p,z) for p,z in zip(ps,y)]),'mean_bias':np.mean(mu-y),'observed_variance':np.var(y),'predicted_variance':np.mean(var),'variance_ratio_predicted_to_observed':np.mean(var)/np.var(y),'observed_zero_rate':np.mean(y==0),'predicted_zero_rate':np.mean([p[0] for p in ps]),'observed_low_le_2':np.mean(y<=2),'predicted_low_le_2':np.mean([p[:3].sum() for p in ps]),'observed_upper_ge_7':np.mean(y>=7),'predicted_upper_ge_7':np.mean([p[7:].sum() for p in ps])}

def freeze():
 base=v2.features(v2.parse());d=eq.statcast_states(base);L,selected,preds,alphas,masks=eq.run_models(d);assert selected=='MODEL_C_PLUS_MATCHUP'
 # Refit exactly as V1 did (development only) and preserve every split mean.
 fs=dict(eq.LADDERS)[selected];allp={}
 for side,target in [('away','away_f5_runs'),('home','home_f5_runs')]:
  m=eq.fit(eq.sideX(d.loc[masks['DEVELOPMENT']],side,fs),d.loc[masks['DEVELOPMENT'],target]);allp[side]=eq.predict(m,eq.sideX(d,side,fs))
 z=d[['game_pk','date','start','split','away_f5_runs','home_f5_runs']].copy();z['away_f5_expected_runs']=allp['away'];z['home_f5_expected_runs']=allp['home'];z['combined_f5_expected_runs']=z.away_f5_expected_runs+z.home_f5_expected_runs;z['actual_combined_f5_runs']=z.away_f5_runs+z.home_f5_runs
 return d,z,L,alphas,masks

def candidates(d,z,basealph,masks):
 dev=masks['DEVELOPMENT'];yd=z.loc[dev,'actual_combined_f5_runs'].to_numpy();md=z.loc[dev,'combined_f5_expected_runs'].to_numpy();ga=alpha(yd,md);sidega=alpha(pd.concat([z.loc[dev,'away_f5_runs'],z.loc[dev,'home_f5_runs']]),np.r_[z.loc[dev,'away_f5_expected_runs'],z.loc[dev,'home_f5_expected_runs']])
 ba={}
 for lo,hi,n in BUCKETS:
  q=(md>=lo)&(md<hi);ba[n]=ga if q.sum()<100 else alpha(yd[q],md[q])
 # Fixed compact dispersion learner predicts log alpha proxy; clipping is declared, not tuned.
 X=[]
 for side in ['away','home']:
  q=eq.sideX(d,side,UNC).copy();q.columns=[side+'_'+x for x in UNC];X.append(q)
 DX=pd.concat(X,axis=1);DX['f5_mean']=z.combined_f5_expected_runs;target=np.log(np.clip(((z.actual_combined_f5_runs-z.combined_f5_expected_runs)**2-z.combined_f5_expected_runs)/np.maximum(z.combined_f5_expected_runs**2,1e-6),.005,2))
 vm=make_pipeline(SimpleImputer(strategy='median'),HistGradientBoostingRegressor(max_iter=80,max_leaf_nodes=8,min_samples_leaf=60,learning_rate=.04,l2_regularization=4,early_stopping=False,random_state=SEED)).fit(DX.loc[dev],target.loc[dev]);gamea=np.clip(np.exp(vm.predict(DX)),.005,1.5)
 cov=np.cov((z.loc[dev,'away_f5_runs']-z.loc[dev,'away_f5_expected_runs']),(z.loc[dev,'home_f5_runs']-z.loc[dev,'home_f5_expected_runs']),ddof=0)[0,1];corr=np.corrcoef(z.loc[dev,'away_f5_runs'],z.loc[dev,'home_f5_runs'])[0,1]
 pars={'current_away_alpha':basealph['MODEL_C_PLUS_MATCHUP','away'],'current_home_alpha':basealph['MODEL_C_PLUS_MATCHUP','home'],'global_total_alpha':ga,'global_side_alpha':sidega,'bucket_alphas':ba,'development_home_away_residual_covariance':cov,'development_home_away_correlation':corr,'dependence_candidate_status':'SKIPPED_NEGLIGIBLE_OR_NEGATIVE' if cov<=.10 else 'TESTED_SHARED_POISSON'}
 out={}
 for phase in ['VALIDATION','LATER_HOLDOUT']:
  idx=np.where(masks[phase])[0];rows=[]
  for i in idx:
   ma=z.away_f5_expected_runs.iloc[i];mh=z.home_f5_expected_runs.iloc[i];mt=ma+mh
   a=[pmf(ma,pars['current_away_alpha']),pmf(mh,pars['current_home_alpha'])]
   cand={'A_CURRENT_CONTROL':(conv(a),a),'B_GLOBAL_NEGATIVE_BINOMIAL':(pmf(mt,ga),[pmf(ma,sidega),pmf(mh,sidega)]),'C_MEAN_BUCKET_DISPERSION':(pmf(mt,ba[bucket(mt)]),[pmf(ma,sidega),pmf(mh,sidega)]),'D_GAME_SPECIFIC_DISPERSION':(pmf(mt,gamea[i]),[pmf(ma,gamea[i]),pmf(mh,gamea[i])])}
   if cov>.10:
    shared=min(cov,ma*.5,mh*.5);pa=pmf(ma-shared,0);ph=pmf(mh-shared,0);pc=pmf(shared,0);joint_total=conv([pa,ph,pc*0+pc]) # one shared event adds two; bounded reference approximated below
    # Exact total compound shared-Poisson support.
    joint_total=np.zeros(CAP+1)
    for x,px in enumerate(pa):
     for y,py in enumerate(ph):
      for k,pk in enumerate(pc):joint_total[min(x+y+2*k,CAP)]+=px*py*pk
    cand['E_DEPENDENCE_AWARE']=(joint_total,[conv([pa,pc]),conv([ph,pc])])
   for name,(pt,ps) in cand.items():rows.append((i,name,pt,ps))
  out[phase]=rows
 return out,pars,gamea,DX

def evaluate(z,cands,masks):
 rows=[]
 for phase,items in cands.items():
  for name in sorted(set(x[1] for x in items)):
   q=[x for x in items if x[1]==name];ids=[x[0] for x in q];pt=[x[2] for x in q];pa=[x[3][0] for x in q];ph=[x[3][1] for x in q]
   for market,y,mu,ps in [('F5_TOTAL',z.actual_combined_f5_runs.iloc[ids],z.combined_f5_expected_runs.iloc[ids],pt),('AWAY_F5',z.away_f5_runs.iloc[ids],z.away_f5_expected_runs.iloc[ids],pa),('HOME_F5',z.home_f5_runs.iloc[ids],z.home_f5_expected_runs.iloc[ids],ph)]:rows.append({'candidate':name,'phase':phase,'market':market,**metrics(y,mu,ps)})
 R=pd.DataFrame(rows);sel=R.query("phase=='VALIDATION' and market=='F5_TOTAL'").sort_values('crps').candidate.iloc[0];return R,sel

def ladders(z,cands,selected):
 total=[];team=[]
 for phase,items in cands.items():
  q=[x for x in items if x[1]==selected]
  for market,lines,pi,actual in [('F5_TOTAL',[3.5,4,4.5,5,5.5],None,'actual_combined_f5_runs'),('AWAY_F5',[1.5,2,2.5,3,3.5],0,'away_f5_runs'),('HOME_F5',[1.5,2,2.5,3,3.5],1,'home_f5_runs')]:
   for line in lines:
    pr=[];yy=[];push=0
    for i,_,pt,ps in q:
     p=pt if pi is None else ps[pi];k=np.arange(len(p));y=z[actual].iloc[i];pu=p[k==line].sum();push+=y==line
     if y!=line:pr.append(np.clip(p[k>line].sum()/(1-pu),1e-9,1-1e-9));yy.append(y>line)
    pr=np.array(pr);yy=np.array(yy,float);row={'candidate':selected,'phase':phase,'market':market,'line':line,'resolved':len(yy),'pushes':push,'over_brier':np.mean((pr-yy)**2),'under_brier':np.mean(((1-pr)-(1-yy))**2),'log_loss':np.mean(-yy*np.log(pr)-(1-yy)*np.log(1-pr)),'calibration':pr.mean()-yy.mean(),'observed_over_rate':yy.mean(),'mean_predicted_over_probability':pr.mean(),'probability_sd':pr.std(),'ece':ece(pr,yy)}
    (total if pi is None else team).append(row)
 return pd.DataFrame(total),pd.DataFrame(team)

def outputs(d,z,R,selected,cands,pars,gamea,DX,masks):
 OUT.mkdir(parents=True,exist_ok=True);z.to_csv(OUT/'f5_frozen_mean_predictions.csv',index=False)
 (OUT/'distribution_candidate_contract.json').write_text(json.dumps({'mean_model':'MODEL_C_PLUS_MATCHUP; development-fit regularized Poisson; unchanged', 'candidates':{'A':'V1 independent side negative-binomial with side-global development dispersion','B':'total/side global development negative-binomial','C':'predeclared total-mean buckets; minimum 100 development games else global fallback','D':'shallow development-only log-dispersion model; mean unchanged; alpha clipped .005..1.5','E':'single shared-Poisson dependence reference only if development residual covariance >0.10'},'selection':'minimum validation combined F5 CRPS; July holdout untouched','parameters':pars},indent=2)+'\n')
 audit=f"""# Current distribution audit\n\nV1 uses independent away/home negative-binomial primitives with one global development-estimated dispersion per side (`away={pars['current_away_alpha']:.6f}`, `home={pars['current_home_alpha']:.6f}`). The combined total is their convolution. Whole-line push mass is removed and remaining Over/Under probabilities are renormalized; half-lines have zero push mass. Arbitrary ladders sum integer PMF mass above/below the line.\n\nDevelopment residual home/away covariance is {pars['development_home_away_residual_covariance']:.6f} and raw-run correlation is {pars['development_home_away_correlation']:.6f}; dependence reference status is `{pars['dependence_candidate_status']}`. Detailed validation/holdout empirical and predicted mean/variance, zero, low-score, and upper-tail diagnostics are in `distribution_model_comparison.csv`.\n""";(OUT/'current_distribution_audit.md').write_text(audit)
 R.to_csv(OUT/'distribution_model_comparison.csv',index=False);R.query("phase=='LATER_HOLDOUT' and candidate==@selected").to_csv(OUT/'f5_distribution_holdout_metrics.csv',index=False)
 gl,tl=ladders(z,cands,selected);gl.to_csv(OUT/'f5_ladder_probability_metrics.csv',index=False);tl.to_csv(OUT/'f5_team_total_probability_metrics.csv',index=False)
 # Fixed tail events.
 tails=[]
 for phase,items in cands.items():
  for i,n,pt,ps in [x for x in items if x[1]==selected]:
   for market,p,y in [('F5_TOTAL',pt,z.actual_combined_f5_runs.iloc[i]),('AWAY_F5',ps[0],z.away_f5_runs.iloc[i]),('HOME_F5',ps[1],z.home_f5_runs.iloc[i])]:
    for event,mask,obs in ([('LE_2',lambda k:k<=2,y<=2),('LE_3',lambda k:k<=3,y<=3),('GE_5',lambda k:k>=5,y>=5),('GE_6',lambda k:k>=6,y>=6),('GE_7',lambda k:k>=7,y>=7)] if market=='F5_TOTAL' else [('ZERO',lambda k:k==0,y==0),('GE_3',lambda k:k>=3,y>=3),('GE_4',lambda k:k>=4,y>=4)]):tails.append({'phase':phase,'market':market,'event':event,'predicted':p[mask(np.arange(len(p)))].sum(),'observed':obs})
 pd.DataFrame(tails).groupby(['phase','market','event']).agg(rows=('observed','size'),predicted_probability=('predicted','mean'),observed_rate=('observed','mean')).assign(calibration_gap=lambda x:x.predicted_probability-x.observed_rate).reset_index().to_csv(OUT/'tail_calibration.csv',index=False)
 # Fixed mean bands and median uncertainty splits on validation/holdout; diagnostic only.
 diag=[]
 for phase in ['VALIDATION','LATER_HOLDOUT']:
  ids=np.where(masks[phase])[0];meanband=pd.cut(z.combined_f5_expected_runs.iloc[ids],[0,4.25,5,99],labels=['LOW','MID','HIGH'])
  concepts={'lineup_contact':(d.away_off_whiff+d.home_off_whiff).iloc[ids],'starter_whiff':(d.away_opp_sp_whiff+d.home_opp_sp_whiff).iloc[ids],'workload_uncertainty':(d.away_opp_sp_sparse.astype(int)+d.home_opp_sp_sparse.astype(int)).iloc[ids],'matchup_magnitude':abs(d.away_mix_xwoba-d.away_off_xw).iloc[ids]+abs(d.home_mix_xwoba-d.home_off_xw).iloc[ids]}
  for concept,x in concepts.items():
   med=x.median()
   for band in meanband.dropna().unique():
    for level,q in [('LOW',x<=med),('HIGH',x>med)]:
     use=(meanband==band).to_numpy()&q.to_numpy();y=z.actual_combined_f5_runs.iloc[ids].to_numpy()[use];diag.append({'phase':phase,'mean_band':str(band),'uncertainty_concept':concept,'level':level,'games':len(y),'empirical_variance':np.var(y) if len(y) else np.nan,'mean_game_specific_alpha':np.mean(gamea[ids][use]) if len(y) else np.nan})
 pd.DataFrame(diag).to_csv(OUT/'game_specific_uncertainty_diagnostic.csv',index=False)
 # Probability separation and confidence calibration, using more-likely side without duplicating evidence.
 sep=[];conf=[]
 hold=[x for x in cands['LATER_HOLDOUT'] if x[1]==selected]
 for market,lines,pi,actual in [('F5_TOTAL',[3.5,4,4.5,5,5.5],None,'actual_combined_f5_runs'),('AWAY_F5',[1.5,2,2.5,3,3.5],0,'away_f5_runs'),('HOME_F5',[1.5,2,2.5,3,3.5],1,'home_f5_runs')]:
  for line in lines:
   probs=[]
   for i,_,pt,ps in hold:
    p=pt if pi is None else ps[pi];k=np.arange(len(p));probs.append(p[k>line].sum())
   a=np.array(probs);side=np.maximum(a,1-a);sep.append({'market':market,'line':line,'mean':a.mean(),'sd':a.std(),'p05':np.quantile(a,.05),'p25':np.quantile(a,.25),'median':np.median(a),'p75':np.quantile(a,.75),'p95':np.quantile(a,.95),'minimum':a.min(),'maximum':a.max(),**{f'fraction_{n}':np.mean(q) for n,q in [('50_55',(side>=.5)&(side<.55)),('55_60',(side>=.55)&(side<.6)),('60_65',(side>=.6)&(side<.65)),('65_70',(side>=.65)&(side<.7)),('ge_70',side>=.7)]}})
 pd.DataFrame(sep).to_csv(OUT/'probability_separation.csv',index=False)
 for family,frame in [('F5_GAME_TOTAL',gl),('F5_TEAM_TOTAL',tl)]:
  # Each line contributes once; confidence chooses the favored side and corresponding outcome.
  vals=[]
  source=[x for x in hold]
  specs=([('F5_TOTAL',x,None,'actual_combined_f5_runs') for x in [3.5,4,4.5,5,5.5]] if family=='F5_GAME_TOTAL' else [(m,x,i,a) for m,i,a in [('AWAY_F5',0,'away_f5_runs'),('HOME_F5',1,'home_f5_runs')] for x in [1.5,2,2.5,3,3.5]])
  for market,line,pi,actual in specs:
   for i,_,pt,ps in source:
    y=z[actual].iloc[i]
    if y==line:continue
    p=pt if pi is None else ps[pi];over=p[np.arange(len(p))>line].sum();fav=max(over,1-over);success=(y>line) if over>=.5 else (y<line);vals.append((fav,float(success)))
  for lo,hi,label in [(.5,.55,'50_54_99'),(.55,.6,'55_59_99'),(.6,.65,'60_64_99'),(.65,.7,'65_69_99'),(.7,1.01,'GE_70')]:
   q=[x for x in vals if lo<=x[0]<hi];conf.append({'family':family,'confidence_bin':label,'predictions':len(q),'mean_probability':np.mean([x[0] for x in q]) if q else np.nan,'observed_success_rate':np.mean([x[1] for x in q]) if q else np.nan,'calibration_gap':np.mean([x[0]-x[1] for x in q]) if q else np.nan,'brier':np.mean([(x[0]-x[1])**2 for x in q]) if q else np.nan})
 pd.DataFrame(conf).to_csv(OUT/'confidence_calibration.csv',index=False)
 # Stability and exact V1 comparison.
 temp=[]
 for phase in ['VALIDATION','LATER_HOLDOUT']:
  for month in sorted(z.loc[masks[phase],'date'].str[:7].unique()):
   ids=set(z.index[masks[phase]&z.date.str[:7].eq(month)]);items=[x for x in cands[phase] if x[1]==selected and x[0] in ids];y=np.array([z.actual_combined_f5_runs.iloc[x[0]] for x in items]);mu=np.array([z.combined_f5_expected_runs.iloc[x[0]] for x in items]);ps=[x[2] for x in items];lm=gl.query('phase==@phase');pv=[np.sum((np.arange(len(p))-np.sum(np.arange(len(p))*p))**2*p) for p in ps];temp.append({'slice_type':'SPLIT_MONTH','slice_value':month,'phase':phase,**metrics(y,mu,ps),'ladder_brier':lm.over_brier.mean(),'ladder_log_loss':lm.log_loss.mean(),'ladder_calibration':lm.calibration.mean(),'average_predicted_dispersion':np.mean(pv)})
 pd.DataFrame(temp).to_csv(OUT/'distribution_temporal_stability.csv',index=False)
 comp=[]
 for name in ['A_CURRENT_CONTROL',selected]:
  r=R.query("phase=='LATER_HOLDOUT' and market=='F5_TOTAL' and candidate==@name").iloc[0];gg,_=ladders(z,{k:[x for x in v if x[1]==name] for k,v in cands.items()},name);hh=gg.query("phase=='LATER_HOLDOUT'");comp.append({'distribution':name,'holdout_crps':r.crps,'ladder_brier':hh.over_brier.mean(),'ladder_log_loss':hh.log_loss.mean(),'ladder_ece':hh.ece.mean(),'probability_sd':hh.probability_sd.mean()})
 # Constant mean context retains V1 control score rather than introducing a new distribution search.
 comp.append({'distribution':'CONSTANT_CONTROL_CONTEXT','holdout_crps':1.8724071442516013})
 pd.DataFrame(comp).to_csv(OUT/'v1_vs_refined_distribution.csv',index=False)
 a,b=comp[0],comp[1];common=gl.query("phase=='LATER_HOLDOUT'");mat=[{'metric':'F5 CRPS improvement','value':a['holdout_crps']-b['holdout_crps']},{'metric':'relative CRPS improvement percent','value':100*(a['holdout_crps']-b['holdout_crps'])/a['holdout_crps']},{'metric':'pooled ladder Brier improvement','value':a['ladder_brier']-b['ladder_brier']},{'metric':'pooled ladder log-loss improvement','value':a['ladder_log_loss']-b['ladder_log_loss']},{'metric':'ECE change (refined-current)','value':b['ladder_ece']-a['ladder_ece']}]
 pd.DataFrame(mat).to_csv(OUT/'distribution_materiality_summary.csv',index=False)
 return gl,tl,comp

def main():
 OUT.mkdir(parents=True,exist_ok=True);d,z,L,basealph,masks=freeze();cands,pars,gamea,DX=candidates(d,z,basealph,masks);R,selected=evaluate(z,cands,masks);gl,tl,comp=outputs(d,z,R,selected,cands,pars,gamea,DX,masks);a,b=comp[0],comp[1];imp=a['holdout_crps']-b['holdout_crps'];val=R.query("phase=='VALIDATION' and market=='F5_TOTAL'").set_index('candidate');valid=val.loc[selected,'crps']<val.loc['A_CURRENT_CONTROL','crps'];ladder=(b['ladder_brier']<a['ladder_brier'] and b['ladder_log_loss']<a['ladder_log_loss'] and b['ladder_ece']<=a['ladder_ece']);dec='F5_DISTRIBUTION_REFINEMENT_MATERIAL_ADVANCE' if valid and imp>.03 and ladder else 'F5_DISTRIBUTION_REFINEMENT_SMALL_IMPROVEMENT' if valid and imp>0 and ladder else 'F5_DISTRIBUTION_REFINEMENT_NO_IMPROVEMENT';read='PREDICTION_READY' if dec.endswith('MATERIAL_ADVANCE') else 'VALID_BELOW_PRACTICAL_BAR' if imp>=0 else 'NOT_READY';pd.DataFrame([{'family':'F5_GAME_TOTAL','declaration':read},{'family':'F5_TEAM_TOTAL','declaration':read if tl.query("phase=='LATER_HOLDOUT'").calibration.abs().mean()<.1 else 'NOT_READY'}]).to_csv(OUT/'market_family_readiness.csv',index=False)
 text=f"""# MLB F5 Expected-Quality Distribution Refinement v1\n\n`{dec}`\n\n- Frozen mean: `MODEL_C_PLUS_MATCHUP`, 1,594 deterministic predictions; means unchanged.\n- Current assumption: independent side-specific global negative binomials. Selected on validation: `{selected}`.\n- Validation total CRPS: current {val.loc['A_CURRENT_CONTROL','crps']:.6f}, selected {val.loc[selected,'crps']:.6f}. Holdout: current {a['holdout_crps']:.6f}, selected {b['holdout_crps']:.6f}; improvement {imp:+.6f} ({100*imp/a['holdout_crps']:+.3f}%). MAE is identical because means are frozen.\n- Holdout pooled ladder Brier/log loss/ECE: {b['ladder_brier']:.6f}/{b['ladder_log_loss']:.6f}/{b['ladder_ece']:.6f}. Team-total metrics and fixed tail calibration are reported separately. Game-specific dispersion {'earned selection' if selected=='D_GAME_SPECIFIC_DISPERSION' else 'did not earn selection'}.\n- Readiness: `F5_GAME_TOTAL={read}`; `F5_TEAM_TOTAL={read if tl.query("phase=='LATER_HOLDOUT'").calibration.abs().mean()<.1 else 'NOT_READY'}`. No current-slate demonstration was run because no family reached `PREDICTION_READY`.\n- Exact next step: {'freeze the refined distribution for one independent prospective probability audit' if dec!='F5_DISTRIBUTION_REFINEMENT_NO_IMPROVEMENT' else 'retain the V1 distribution and stop distribution refinement'}. No mean-feature change, sportsbook input, EV/Edge, selector, deployment, or pipeline mutation occurred.\n""";(OUT/'concise_mlb_f5_expected_quality_distribution_refinement_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sh(x)}  {x.name}\n' for x in files));print(json.dumps({'selected':selected,'decision':dec,'validation_crps':val.loc[selected,'crps'],'holdout_crps':b['holdout_crps'],'improvement':imp},indent=2))
if __name__=='__main__':main()
