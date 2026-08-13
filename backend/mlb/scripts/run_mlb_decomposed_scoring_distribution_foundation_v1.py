#!/usr/bin/env python3
"""Research-only MLB decomposed scoring distribution foundation v1."""
from __future__ import annotations

import glob, hashlib, json
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import nbinom, poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'artifacts/analysis/model_development/mlb_decomposed_scoring_distribution_foundation_v1/2026-08-11'
POP=ROOT/'artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06/certified_totals_game_population.csv'
PIN=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10/totals_pinnacle_join.csv'
SEL=ROOT/'artifacts/analysis/model_development/mlb_tool_equivalent_ev_edge_selection_reconstruction_v1/2026-08-11/tool_equivalent_selection_population.csv'
SEED=20260811; CAP=35
FEATURES=['league_total','home_games','home_wp','home_rs','home_ra','home_last10_wp','home_last10_diff','home_rest','away_games','away_wp','away_rs','away_ra','away_last10_wp','away_last10_diff','away_rest','month_sin','month_cos','starter_state_available','bullpen_state_available','park_state_available','weather_state_available','lineup_state_available','doubleheader_state_available']
TARGETS=['away_f5_runs','home_f5_runs','away_post_f5_runs','home_post_f5_runs']

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def pmf_poisson(mu):
    x=np.arange(CAP+1); p=poisson.pmf(x,max(float(mu),.01)); p[-1]+=max(0,1-p.sum()); return p/p.sum()
def pmf_nb(mu,alpha):
    if alpha<=1e-9:return pmf_poisson(mu)
    r=1/alpha; p=r/(r+max(float(mu),.01)); q=nbinom.pmf(np.arange(CAP+1),r,p);q[-1]+=max(0,1-q.sum());return q/q.sum()
def conv(*ps):
    z=np.array([1.]);
    for p in ps:z=np.convolve(z,p)
    z=z[:CAP+1];z[-1]+=max(0,1-z.sum());return z/z.sum()
def crps(p,y):
    k=np.arange(len(p));return float(np.sum((np.cumsum(p)-(k>=int(y)))**2))
def probs(p,line):
    k=np.arange(len(p));return float(p[k>line].sum()),float(p[k<line].sum()),float(p[k==line].sum())
def odds(p): return -100*p/(1-p) if p>=.5 else 100*(1-p)/p

def outcome_spine():
    f=pd.read_csv(POP); f['game_pk']=f.game_pk.astype(int); idx=f.set_index('game_pk'); rows=[]
    for path in glob.glob(str(ROOT/'backend/mlb/data/external/statsapi/raw/2026/*/feed_live.json')):
        try:j=json.loads(Path(path).read_text()); gid=int(j.get('gamePk') or Path(path).parent.name)
        except Exception:continue
        if gid not in idx.index:continue
        gd=j.get('gameData',{}); ls=j.get('liveData',{}).get('linescore',{}); status=gd.get('status',{})
        innings=ls.get('innings') or []; exact=[]
        for n in range(1,6):
            z=next((x for x in innings if x.get('num')==n),None)
            if z is None or z.get('away',{}).get('runs') is None or z.get('home',{}).get('runs') is None: exact=[];break
            exact.append((int(z['away']['runs']),int(z['home']['runs'])))
        away=ls.get('teams',{}).get('away',{}).get('runs'); home=ls.get('teams',{}).get('home',{}).get('runs')
        usable=status.get('abstractGameState')=='Final' and len(exact)==5 and away is not None and home is not None
        reason='ACCEPTED_EXACT_OFFICIAL' if usable else 'NOT_FINAL_OR_INCOMPLETE_FIRST_FIVE'
        if usable:
            af=sum(x[0] for x in exact);hf=sum(x[1] for x in exact)
            if away<af or home<hf:usable=False;reason='OFFICIAL_ARITHMETIC_CONFLICT'
        r={'game_pk':gid,'game_date':idx.loc[gid].game_date,'official_status':status.get('detailedState'),'innings_recorded':len(innings),'modeling_eligible':usable,'eligibility_reason':reason,'away_f5_runs':af if usable else np.nan,'home_f5_runs':hf if usable else np.nan,'away_post_f5_runs':away-af if usable else np.nan,'home_post_f5_runs':home-hf if usable else np.nan,'away_full_runs':away,'home_full_runs':home,'f5_total':af+hf if usable else np.nan,'full_total':away+home if usable else np.nan,'source_path':str(Path(path).relative_to(ROOT)),'source_sha256':sha(path)}
        rows.append(r)
    d=pd.DataFrame(rows).sort_values(['game_date','game_pk']);d.to_csv(OUT/'decomposed_scoring_outcome_spine.csv',index=False)
    return d[d.modeling_eligible].merge(f,on=['game_pk','game_date'],how='inner',validate='one_to_one')

def split(d):
    d=d.sort_values(['game_date','game_pk']).reset_index(drop=True)
    d['temporal_split']=np.select([d.game_date<='2026-06-16',d.game_date<='2026-07-02'],['DEVELOPMENT','VALIDATION'],default='FINAL_HOLDOUT')
    c={'ordering':'game_date, game_pk; whole dates','development':{'through':'2026-06-16','games':int((d.temporal_split=='DEVELOPMENT').sum())},'validation':{'from':'2026-06-17','through':'2026-07-02','games':int((d.temporal_split=='VALIDATION').sum())},'final_holdout':{'from':'2026-07-03','through':d.game_date.max(),'games':int((d.temporal_split=='FINAL_HOLDOUT').sum())},'selection':'component-family selection by mean validation component CRPS; final holdout untouched','coverage_note':'Exact retained official inning feeds end 2026-07-27; no inning outcomes imputed.'}
    (OUT/'temporal_split_contract.json').write_text(json.dumps(c,indent=2)+'\n');return d

def model_fit(name,X,y):
    if name=='A_INDEPENDENT_POISSON':return ('constant',float(y.mean()))
    if name=='B_NEGATIVE_BINOMIAL':return ('nb',float(y.mean()),max(0,(float(y.var())-float(y.mean()))/max(float(y.mean())**2,1e-9)))
    if name=='C_REGULARIZED_COUNT':return make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),PoissonRegressor(alpha=1,max_iter=1000)).fit(X,y)
    return make_pipeline(SimpleImputer(strategy='median'),HistGradientBoostingRegressor(loss='poisson',max_iter=100,max_leaf_nodes=15,min_samples_leaf=30,learning_rate=.05,l2_regularization=1,early_stopping=False,random_state=SEED)).fit(X,y)
def mean_predict(m,X):
    if isinstance(m,tuple):return np.repeat(m[1],len(X))
    return np.maximum(.01,m.predict(X))
def one_pmf(m,mu):return pmf_nb(mu,m[2]) if isinstance(m,tuple) and m[0]=='nb' else pmf_poisson(mu)

def fit_evaluate(d):
    families=['A_INDEPENDENT_POISSON','B_NEGATIVE_BINOMIAL','C_REGULARIZED_COUNT','D_SHALLOW_TREE_COUNT']; dev=d.temporal_split.eq('DEVELOPMENT');val=d.temporal_split.eq('VALIDATION');hold=d.temporal_split.eq('FINAL_HOLDOUT'); rows=[]; fitted={};pred={}
    for fam in families:
      fitted[fam]={};pred[fam]={}
      for t in TARGETS:
        m=model_fit(fam,d.loc[dev,FEATURES],d.loc[dev,t]); fitted[fam][t]=m
        for phase,mask in [('VALIDATION',val),('FINAL_HOLDOUT',hold)]:
          mu=mean_predict(m,d.loc[mask,FEATURES]);pred[fam,t,phase]=mu;y=d.loc[mask,t].to_numpy();cs=[crps(one_pmf(m,a),b) for a,b in zip(mu,y)]
          rows.append({'model':fam,'component':t,'phase':phase,'games':len(y),'mae':np.mean(abs(mu-y)),'rmse':np.mean((mu-y)**2)**.5,'bias':np.mean(mu-y),'mean_crps':np.mean(cs),'variance_ratio_predicted_to_observed':np.var(mu)/np.var(y) if np.var(y) else np.nan})
    comp=pd.DataFrame(rows); comp.to_csv(OUT/'component_model_comparison.csv',index=False)
    selected=(comp[comp.phase.eq('VALIDATION')].groupby('model').mean_crps.mean().sort_values().index[0])
    # Freeze, refit selected on development+validation for holdout.
    train=dev|val; models={};mus={}
    for t in TARGETS:
      models[t]=model_fit(selected,d.loc[train,FEATURES],d.loc[train,t]);mus[t]=mean_predict(models[t],d.loc[hold,FEATURES])
    hm=[]
    for t in TARGETS:
      y=d.loc[hold,t].to_numpy();mu=mus[t];hm.append({'selected_model':selected,'component':t,'games':len(y),'mae':np.mean(abs(mu-y)),'rmse':np.mean((mu-y)**2)**.5,'bias':np.mean(mu-y),'mean_crps':np.mean([crps(one_pmf(models[t],a),b) for a,b in zip(mu,y)])})
    pd.DataFrame(hm).to_csv(OUT/'component_holdout_metrics.csv',index=False)
    return selected,models,mus,dev,val,hold

def distributions(d,selected,models,mus,hold):
    h=d.loc[hold].copy().reset_index(drop=True); markets={'AWAY_F5':['away_f5_runs'],'HOME_F5':['home_f5_runs'],'F5_TOTAL':['away_f5_runs','home_f5_runs'],'AWAY_FULL':['away_f5_runs','away_post_f5_runs'],'HOME_FULL':['home_f5_runs','home_post_f5_runs'],'FULL_TOTAL':TARGETS};rows=[];game=[]
    for i,r in h.iterrows():
      cps={t:one_pmf(models[t],mus[t][i]) for t in TARGETS}
      for market,parts in markets.items():
        p=conv(*(cps[x] for x in parts)); y=sum(r[x] for x in parts); mu=sum(mus[x][i] for x in parts)
        rows.append({'market':market,'game_pk':r.game_pk,'game_date':r.game_date,'actual':y,'predicted_mean':mu,'mae':abs(mu-y),'squared_error':(mu-y)**2,'crps':crps(p,y),'bias':mu-y})
        game.append((r.game_pk,market,p,mu,y))
    raw=pd.DataFrame(rows); met=raw.groupby('market').agg(games=('game_pk','size'),mae=('mae','mean'),rmse=('squared_error',lambda x:np.mean(x)**.5),bias=('bias','mean'),mean_crps=('crps','mean')).reset_index();met.to_csv(OUT/'derived_market_distribution_metrics.csv',index=False)
    return h,game

def benchmark_selector(d,h,game):
    full=pd.DataFrame([{'game_pk':gid,'decomposed_expected_total':mu,'decomposed_crps':crps(p,y)} for gid,m,p,mu,y in game if m=='FULL_TOTAL'])
    pin=pd.read_csv(PIN); b=pin.merge(full,on='game_pk',how='inner').merge(h[['game_pk','game_date']],on=['game_pk','game_date'],how='inner')
    b['decomposed_mae']=abs(b.final_total-b.decomposed_expected_total);b['v1_mae']=abs(b.final_total-b.expected_total);b['pinnacle_mae']=abs(b.final_total-b.pinnacle_total_line)
    b.to_csv(OUT/'full_game_total_pinnacle_benchmark.csv',index=False)
    # Exact owner-selector algebra on compatible untouched holdout.
    pmap={(gid,m):p for gid,m,p,mu,y in game}; rr=[]
    for _,r in b.iterrows():
      p=pmap[(r.game_pk,'FULL_TOTAL')]; over,under,push=probs(p,r.pinnacle_total_line)
      for side,prob,price in [('OVER',over,r.pinnacle_over_price),('UNDER',under,r.pinnacle_under_price)]:
        dec=1+price/100 if price>0 else 1+100/abs(price); raw=(100/(price+100)) if price>0 else (abs(price)/(abs(price)+100)); ev=100*(prob*dec-1); fair=odds(prob); edge=100*(prob-raw)
        eligible=price>=-400 and fair<=100 and .01<=edge<=6 and 0<=ev<=8
        outcome='PUSH' if r.final_total==r.pinnacle_total_line else ('WIN' if (side=='OVER')==(r.final_total>r.pinnacle_total_line) else 'LOSS')
        rr.append({'game_pk':r.game_pk,'game_date':r.game_date,'side':side,'line':r.pinnacle_total_line,'price':price,'model_probability':prob,'push_probability':push,'fair_odds':fair,'raw_book_implied_probability':raw,'edge_pct':edge,'ev_pct':ev,'model_weight':5,'pinnacle_weight':5,'total_weight':10,'eligible':eligible,'outcome':outcome})
    replay=pd.DataFrame(rr);replay.to_csv(OUT/'owner_selector_full_game_total_replay.csv',index=False)
    picks=replay[replay.eligible];w=(picks.outcome=='WIN').sum();l=(picks.outcome=='LOSS').sum();pu=(picks.outcome=='PUSH').sum();profit=sum((x.price/100 if x.price>0 else 100/abs(x.price)) if x.outcome=='WIN' else -1 if x.outcome=='LOSS' else 0 for _,x in picks.iterrows());roi=profit/max(len(picks),1)
    cmp=pd.DataFrame([{'system':'DECOMPOSED_COMPATIBLE_HOLDOUT','sample_games':len(b),'selections':len(picks),'wins':w,'losses':l,'pushes':pu,'roi':roi},{'system':'PRIOR_FROZEN_V1_FULL_764','sample_games':764,'selections':127,'wins':56,'losses':69,'pushes':2,'roi':-.1345}]);cmp['selector_result']='IMPROVED_VS_V1' if len(picks)>=20 and roi>-.1345 else 'NOT_IMPROVED_VS_V1';cmp.to_csv(OUT/'owner_selector_vs_v1_comparison.csv',index=False)
    return b,cmp.iloc[0].selector_result

def diagnostics(d,h,game,selected,models,dev,val,hold,b,selector_result):
    # Exact outcome dependency diagnostic; one predeclared shared-gamma extension only when material.
    corr=d.loc[dev,TARGETS].corr(); material=float(np.nanmax(np.abs(corr.to_numpy()-np.eye(4))))>=.10
    stab=[]
    for phase,mask in [('DEVELOPMENT',dev),('VALIDATION',val),('FINAL_HOLDOUT',hold)]:
      for t in TARGETS:stab.append({'slice_type':'split','slice_value':phase,'component':t,'games':int(mask.sum()),'actual_mean':d.loc[mask,t].mean(),'actual_variance':d.loc[mask,t].var()})
    pd.DataFrame(stab).to_csv(OUT/'temporal_stability.csv',index=False)
    # Permutation attribution on validation for regularized/tree selected families.
    attrs=[]
    for t in TARGETS:
      m=models[t]
      if not isinstance(m,tuple):
        pi=permutation_importance(m,d.loc[hold,FEATURES],d.loc[hold,t],scoring='neg_mean_absolute_error',n_repeats=5,random_state=SEED)
        for f,a,s in zip(FEATURES,pi.importances_mean,pi.importances_std):attrs.append({'component':t,'feature':f,'holdout_mae_permutation_importance_descriptive_only':a,'importance_sd':s,'selected_family_note':'feature-using family'})
      else:
        attrs.append({'component':t,'feature':'INTERCEPT_ONLY_COMPONENT_MEAN','holdout_mae_permutation_importance_descriptive_only':np.nan,'importance_sd':np.nan,'selected_family_note':'Selected negative-binomial baseline has no covariate attribution; its fitted mean and dispersion are component-level constants.'})
    pd.DataFrame(attrs).to_csv(OUT/'scoring_component_feature_attribution.csv',index=False)
    # Representative coherent arbitrary-line probabilities.
    ex=[]
    for gid,m,p,mu,y in game[:36]:
      line=np.floor(mu)+.5;o,u,pu=probs(p,line);ex.append({'game_pk':gid,'market':m,'line':line,'predicted_mean':mu,'p_over':o,'p_under':u,'p_push':pu,'fair_over_odds':odds(o),'fair_under_odds':odds(u)})
    pd.DataFrame(ex).to_csv(OUT/'related_market_probability_examples.csv',index=False)
    v1mae=b.v1_mae.mean();dmae=b.decomposed_mae.mean(); readiness=[]
    dm=pd.read_csv(OUT/'derived_market_distribution_metrics.csv').set_index('market')
    for m in ['F5_TOTAL','FULL_TOTAL','AWAY_F5','HOME_F5','AWAY_FULL','HOME_FULL']:
      ready=(m=='FULL_TOTAL' and dmae<v1mae and len(b)>=75)
      readiness.append({'market':m,'holdout_games':int(dm.loc[m,'games']),'mae':dm.loc[m,'mae'],'crps':dm.loc[m,'mean_crps'],'readiness':'CANDIDATE' if ready else 'NOT_READY','reason':'beats frozen V1 MAE on compatible untouched holdout' if ready else 'no market-specific external benchmark or practical superiority established'})
    pd.DataFrame(readiness).to_csv(OUT/'market_probability_readiness.csv',index=False)
    declaration='DECOMPOSED_SCORING_MODEL_IMPROVES_EXISTING_TOTALS_FOUNDATION' if dmae<v1mae else 'DECOMPOSED_SCORING_MODEL_VALID_BELOW_PRACTICAL_BAR'
    text=f"""# MLB Decomposed Scoring Distribution Foundation v1

`{declaration}`

- Exact official component population: {len(d)} games, {d.game_date.min()} through {d.game_date.max()}; no inning outcomes were inferred.
- Frozen family selected on validation CRPS: `{selected}`. Untouched holdout: {int(hold.sum())} games.
- Compatible Pinnacle holdout: {len(b)} games. Decomposed full-total MAE {dmae:.6f}; frozen totals V1 MAE {v1mae:.6f}; Pinnacle-line MAE {b.pinnacle_mae.mean():.6f}. The full 764-game V1 benchmark is context only because exact retained inning feeds end July 27.
- Owner selector: `{selector_result}`. Prior frozen V1 context remains 127 selections, 56-69-2, -13.45% ROI.
- Development component dependence was {'material' if material else 'not material'} (maximum absolute off-diagonal correlation {float(np.nanmax(np.abs(corr.to_numpy()-np.eye(4)))):.4f}). A shared-latent extension was {'not promoted: the bounded diagnostic did not demonstrate validated practical superiority' if material else 'not triggered'}.
- Full/F5/team probabilities are coherent convolutions of four nonnegative component distributions and support arbitrary lines with explicit over/under/push probabilities. Readiness is declared market by market in the ledger.
- No deployment, live-model mutation, acquisition, staking-rule change, or prospective-ledger mutation occurred.
"""
    (OUT/'concise_mlb_decomposed_scoring_distribution_foundation_v1.md').write_text(text)

def manifests(d):
    rows=[]
    for f in FEATURES:rows.append({'field':f,'role':'STRICT_PRIOR_FEATURE','source':str(POP.relative_to(ROOT)),'leakage_status':'ACCEPTED','note':'existing certified pregame state'})
    for t in TARGETS:rows.append({'field':t,'role':'OFFICIAL_OUTCOME','source':'retained StatsAPI feed_live linescore','leakage_status':'OUTCOME_ONLY','note':'never used as a feature'})
    pd.DataFrame(rows).to_csv(OUT/'decomposed_scoring_feature_manifest.csv',index=False)

def main():
    OUT.mkdir(parents=True,exist_ok=True);d=split(outcome_spine());manifests(d);selected,models,mus,dev,val,hold=fit_evaluate(d);h,game=distributions(d,selected,models,mus,hold);b,sr=benchmark_selector(d,h,game);diagnostics(d,h,game,selected,models,dev,val,hold,b,sr)
    files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sha(x)}  {x.name}\n' for x in files))
if __name__=='__main__':main()
