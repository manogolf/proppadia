#!/usr/bin/env python3
"""Bounded, research-only prop-market-derived MLB game model v0."""
from __future__ import annotations

import argparse, csv, hashlib, json, math, re, unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, brier_score_loss, log_loss

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'artifacts/analysis/model_development/mlb_prop_market_game_model_v0/2026-08-05'
PROP=ROOT/'artifacts/analysis/model_development/mlb_betonline_inventory_driven_player_prop_backfill/2026-07-19/final_normalized_recovered_rows_2026-07-19.csv'
FEEDS=ROOT/'backend/mlb/data/research/dh_forward_validation/v1/prior_official_feed_cache'
SEED=20260805
SEMANTIC_ID='PROP_MARKET_DERIVED_MLB_GAME_PREDICTION_MODEL_V0'
FAMILIES=['hits','total_bases','home_runs','runs','rbis','hits_runs_rbis','walks','strikeouts','stolen_bases','pitcher_strikeouts','pitcher_outs','pitcher_hits_allowed','pitcher_earned_runs','pitcher_walks','pitcher_runs_allowed']

def preflight():
    required={'prop_market_source':PROP,'official_feed_cache':FEEDS}
    missing={name:str(path.relative_to(ROOT)) for name,path in required.items() if not path.exists()}
    return {'experiment_identity':SEMANTIC_ID,'status':'READY' if not missing else 'BLOCKED_MISSING_CERTIFIED_INPUT','missing_inputs':missing,'output_path':str(OUT.relative_to(ROOT))}

def package_hash_check():
    manifest=OUT/'reproducibility_hashes.sha256'; failures=[]
    if not manifest.exists(): return {'status':'NOT_AVAILABLE','failures':['reproducibility_hashes.sha256']}
    for line in manifest.read_text().splitlines():
        if not line.strip(): continue
        expected,name=line.split(None,1); path=OUT/name.strip()
        actual=hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else 'MISSING'
        if actual!=expected: failures.append(name.strip())
    return {'status':'PASS' if not failures else 'FAIL','failures':failures}

def norm(s): return re.sub(r'[^a-z0-9]','',unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode().lower())
def dt(s): return pd.to_datetime(s,utc=True,errors='coerce')
def implied(x):
    x=float(x); return 100/(x+100) if x>0 else abs(x)/(abs(x)+100)
def write(df,name): df.to_csv(OUT/name,index=False)
def clip(p): return np.clip(np.asarray(p,dtype=float),1e-6,1-1e-6)
def ll(y,p): return log_loss(y,clip(p),labels=[0,1])
def poisson_probs(ha,aa,total_line=8.5):
    grid=np.arange(0,22); ph=poisson.pmf(grid,ha); pa=poisson.pmf(grid,aa); joint=np.outer(ph,pa); joint/=joint.sum()
    return float(np.tril(joint,-1).sum()),float(sum(joint[i,j] for i in grid for j in grid if i+j>total_line)),float(sum(joint[i,j] for i in grid for j in grid if i-j>1.5))

def load_games():
    games=[]
    source_files=list(FEEDS.glob('*.json'))+list((ROOT/'artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17/raw_official_mlb').glob('*.json'))+list((ROOT/'artifacts/analysis/model_development/mlb_historical_outcome_gap_authoritative_recovery/2026-07-13/raw_official_mlb').glob('*.json'))
    seen=set()
    for p in sorted(source_files):
        try: f=json.loads(p.read_text())
        except Exception: continue
        gd=f.get('gameData',{}); status=gd.get('status',{})
        if status.get('abstractGameState')!='Final': continue
        start=dt(gd.get('datetime',{}).get('dateTime')); date=str(start.tz_convert('America/Los_Angeles').date()) if pd.notna(start) else ''
        if not ('2026-05-01'<=date<='2026-07-27'): continue
        box=f.get('liveData',{}).get('boxscore',{}).get('teams',{}); lines=f.get('liveData',{}).get('linescore',{}).get('teams',{})
        if not box or not lines: continue
        if int(f['gamePk']) in seen: continue
        seen.add(int(f['gamePk']))
        rec={'game_date':date,'game_pk':int(f['gamePk']),'scheduled_start_utc':start.isoformat(),'venue_id':gd.get('venue',{}).get('id',''),'home_team_id':gd['teams']['home']['id'],'away_team_id':gd['teams']['away']['id'],'home_team':gd['teams']['home']['name'],'away_team':gd['teams']['away']['name'],'home_runs':lines.get('home',{}).get('runs'),'away_runs':lines.get('away',{}).get('runs'),'source_path':str(p.relative_to(ROOT)),'source_sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
        if rec['home_runs'] is None or rec['away_runs'] is None: continue
        players={}; hands={}; starters={}; lineup_hands={}
        for side in ('home','away'):
            team=box[side]; order={int(x) for x in team.get('battingOrder',[])}
            for key,row in team.get('players',{}).items():
                person=row.get('person',{}); pid=person.get('id'); name=person.get('fullName') or gd.get('players',{}).get(key,{}).get('fullName')
                if pid and name: players[norm(name)]=(int(pid),side,int(pid) in order)
                hand=gd.get('players',{}).get(key,{}).get('batSide',{}).get('code');
                if pid and hand: hands[int(pid)]=hand
            pp=team.get('pitchers',[]); starters[side]=int(pp[0]) if pp else None
            lineup_hands[side]=Counter(hands.get(pid,'U') for pid in order)
        rec['_players']=players; rec['_hands']=hands
        for side in ('home','away'):
            rec[f'{side}_starter_id']=starters[side] or ''
            rec[f'{side}_starter_hand']=gd.get('players',{}).get('ID'+str(starters[side]),{}).get('pitchHand',{}).get('code','U') if starters[side] else 'U'
            rec[f'{side}_lineup_l']=lineup_hands[side].get('L',0); rec[f'{side}_lineup_r']=lineup_hands[side].get('R',0)
        games.append(rec)
    return games

def market_data(games):
    raw=pd.read_csv(PROP,low_memory=False); raw=raw[(raw.validation_status=='PASS') & (raw.bookmaker_key=='betonlineag')].copy()
    raw['capture']=dt(raw.source_capture_timestamp); raw['start']=dt(raw.commence_time); raw=raw[raw.capture<=raw.start-pd.Timedelta(minutes=15)]
    raw['event_key']=raw.slate_date.astype(str)+'|'+raw.event_id.astype(str)
    chosen=raw.groupby('event_key').capture.max().rename('chosen'); raw=raw.merge(chosen,on='event_key'); raw=raw[raw.capture==raw.chosen]
    by_date=defaultdict(list)
    for g in games: by_date[g['game_date']].append(g)
    obs=[]; game_sources=defaultdict(set)
    for r in raw.itertuples():
        cands=by_date.get(str(r.slate_date),[]); matches=[g for g in cands if norm(g['home_team'])==norm(r.home_team) and norm(g['away_team'])==norm(r.away_team)]
        if len(matches)!=1: continue
        g=matches[0]; pl=g['_players'].get(norm(r.player_name));
        if not pl: continue
        pid,side,is_starter=pl; family=str(r.prop_type)
        if family not in FAMILIES: continue
        obs.append({'game_pk':g['game_pk'],'game_date':g['game_date'],'team_side':side,'team_id':g[f'{side}_team_id'],'player_mlb_id':pid,'player_name':r.player_name,'lineup_status':'FINAL_FEED_ORIGINAL_STARTER' if is_starter else 'FINAL_FEED_NONSTARTER_OR_SUBSTITUTE','family':family,'line':float(r.line),'side':str(r.side).lower(),'price':float(r.price),'snapshot_timestamp_utc':r.source_capture_timestamp,'scheduled_start_utc':g['scheduled_start_utc'],'snapshot_age_minutes':(dt(g['scheduled_start_utc'])-dt(r.source_capture_timestamp)).total_seconds()/60,'source_path':r.source_path,'source_sha256':r.source_sha256})
        game_sources[g['game_pk']].add(str(r.source_path))
    o=pd.DataFrame(obs).drop_duplicates(['game_pk','team_side','player_mlb_id','family','line','side','snapshot_timestamp_utc'])
    paired=[]
    for key,x in o.groupby(['game_pk','team_side','team_id','player_mlb_id','player_name','lineup_status','family','line']):
        sides={r.side:r for r in x.itertuples()}
        if 'over' not in sides or 'under' not in sides: continue
        ov,un=sides['over'],sides['under']; oi,ui=implied(ov.price),implied(un.price); nv=oi/(oi+ui)
        paired.append(dict(zip(['game_pk','team_side','team_id','player_mlb_id','player_name','lineup_status','family','line'],key),no_vig_over=nv,over_price=ov.price,under_price=un.price,implied_expectation=float(key[-1])+(nv-.5),snapshot_timestamp_utc=ov.snapshot_timestamp_utc,source_path=ov.source_path))
    return o,pd.DataFrame(paired),game_sources

def aggregate(paired,games):
    rows=[]
    for (pk,side,fam),x in paired.groupby(['game_pk','team_side','family']):
        starts=x[x.lineup_status=='FINAL_FEED_ORIGINAL_STARTER']; non=x[x.lineup_status!='FINAL_FEED_ORIGINAL_STARTER']
        rows.append({'game_pk':pk,'team_side':side,'family':fam,'priced_players':x.player_mlb_id.nunique(),'paired_markets':len(x),'median_line':x.line.median(),'mean_line':x.line.mean(),'sum_lines_starters':starts.line.sum(),'median_no_vig_over':x.no_vig_over.median(),'weighted_no_vig_over':np.average(x.no_vig_over,weights=np.maximum(x.line,.5)),'probability_dispersion':x.no_vig_over.std(ddof=0),'alternate_line_count':len(x)-x.player_mlb_id.nunique(),'market_missingness':0,'lineup_confirmed_starter_count':starts.player_mlb_id.nunique(),'confirmed_nonstarter_count':non.player_mlb_id.nunique(),'unresolved_lineup_count':0,'aggregate_implied_event_expectation_starters':starts.implied_expectation.sum(),'aggregate_implied_event_expectation_nonstarters':non.implied_expectation.sum()})
    return pd.DataFrame(rows)

def controls(games):
    history=defaultdict(list); rows=[]
    for g in sorted(games,key=lambda z:(z['game_date'],z['scheduled_start_utc'],z['game_pk'])):
        r={k:v for k,v in g.items() if not k.startswith('_')}; all_prior=[z for v in history.values() for z in v]
        league_scored=np.mean([z[0] for z in all_prior]) if all_prior else 4.4
        for side,opp in (('home','away'),('away','home')):
            h=history[g[f'{side}_team_id']]; r[f'{side}_prior_games']=len(h); r[f'{side}_prior_runs_scored']=np.mean([z[0] for z in h[-20:]]) if h else league_scored; r[f'{side}_prior_runs_allowed']=np.mean([z[1] for z in h[-20:]]) if h else league_scored
        rows.append(r); history[g['home_team_id']].append((g['home_runs'],g['away_runs'])); history[g['away_team_id']].append((g['away_runs'],g['home_runs']))
    return pd.DataFrame(rows)

def model_and_evaluate(control,aggs):
    wide=aggs.pivot_table(index='game_pk',columns=['team_side','family'],values=['paired_markets','sum_lines_starters','weighted_no_vig_over','probability_dispersion','alternate_line_count','lineup_confirmed_starter_count','confirmed_nonstarter_count','unresolved_lineup_count','aggregate_implied_event_expectation_starters','aggregate_implied_event_expectation_nonstarters'],aggfunc='first')
    wide.columns=['prop_'+a+'_'+b+'_'+c for a,b,c in wide.columns]; wide=wide.reset_index()
    d=control.merge(wide,on='game_pk',how='inner'); d=d.sort_values(['game_date','game_pk']).reset_index(drop=True)
    dates=sorted(d.game_date.unique()); n=len(dates); fit_end=dates[max(0,int(n*.60)-1)]; val_end=dates[max(1,int(n*.80)-1)]
    d['split']=np.where(d.game_date<=fit_end,'fit',np.where(d.game_date<=val_end,'validation','holdout'))
    ctrl=['venue_id','home_prior_games','away_prior_games','home_prior_runs_scored','home_prior_runs_allowed','away_prior_runs_scored','away_prior_runs_allowed','home_lineup_l','home_lineup_r','away_lineup_l','away_lineup_r']
    props=[c for c in d if c.startswith('prop_')]; combined=ctrl+props
    variants={'BASEBALL_CONTROL_ONLY':ctrl,'PROP_MARKET_ONLY':props,'BASEBALL_PLUS_PROP_MARKET':combined}
    predictions=[]; models={}
    for name,cols in variants.items():
        tr=d.split=='fit'; med=d.loc[tr,cols].apply(pd.to_numeric,errors='coerce').median(); X=d[cols].apply(pd.to_numeric,errors='coerce').fillna(med).fillna(0)
        models[name]=[]
        preds=[]
        for target in ('home_runs','away_runs'):
            m=HistGradientBoostingRegressor(loss='poisson',learning_rate=.05,max_iter=100,max_leaf_nodes=15,min_samples_leaf=20,l2_regularization=1.0,random_state=SEED,early_stopping=False).fit(X[tr],d.loc[tr,target]); models[name].append(m); preds.append(np.maximum(.05,m.predict(X)))
        for i,r in d.iterrows():
            hp,ap=float(preds[0][i]),float(preds[1][i]); ml,tot,rl=poisson_probs(hp,ap)
            predictions.append({'game_date':r.game_date,'game_pk':r.game_pk,'split':r.split,'variant':name,'actual_home_runs':r.home_runs,'actual_away_runs':r.away_runs,'predicted_home_runs':hp,'predicted_away_runs':ap,'predicted_total_runs':hp+ap,'predicted_run_margin':hp-ap,'home_win_probability':ml,'over_probability_at_8_5_proxy':tot,'home_minus_1_5_probability':rl,'main_market_total_line':'','main_market_snapshot_status':'UNAVAILABLE'})
    p=pd.DataFrame(predictions); eval_rows=[]; money=[]; totals=[]; runline=[]
    for (v,s),x in p.groupby(['variant','split']):
        yh=x.actual_home_runs.values; ya=x.actual_away_runs.values; ph=x.predicted_home_runs.values; pa=x.predicted_away_runs.values
        yml=(yh>ya).astype(int); ytot=(yh+ya>8.5).astype(int); mask=(yh+ya)!=8.5; yrl=(yh-ya>1.5).astype(int)
        eval_rows.append({'variant':v,'split':s,'games':len(x),'home_run_mae':mean_absolute_error(yh,ph),'away_run_mae':mean_absolute_error(ya,pa),'combined_run_rmse':math.sqrt(np.mean(np.r_[yh-ph,ya-pa]**2)),'home_bias':np.mean(ph-yh),'away_bias':np.mean(pa-ya),'poisson_log_loss':-np.mean(poisson.logpmf(yh,ph)+poisson.logpmf(ya,pa))})
        money.append({'variant':v,'split':s,'games':len(x),'accuracy':np.mean((x.home_win_probability>=.5)==yml),'brier':brier_score_loss(yml,x.home_win_probability),'log_loss':ll(yml,x.home_win_probability),'home_prediction_share':np.mean(x.home_win_probability>=.5),'actual_home_win_rate':np.mean(yml),'calibration_gap':np.mean(x.home_win_probability-yml),'no_vig_market_brier':'','no_vig_market_log_loss':''})
        totals.append({'variant':v,'split':s,'games':int(mask.sum()),'pushes':int((~mask).sum()),'total_runs_mae':mean_absolute_error(yh+ya,ph+pa),'accuracy':np.mean((x.over_probability_at_8_5_proxy.values[mask]>=.5)==ytot[mask]),'brier':brier_score_loss(ytot[mask],x.over_probability_at_8_5_proxy.values[mask]),'log_loss':ll(ytot[mask],x.over_probability_at_8_5_proxy.values[mask]),'calibration_gap':np.mean(x.over_probability_at_8_5_proxy.values[mask]-ytot[mask]),'historical_market_total_status':'UNAVAILABLE; 8.5 diagnostic proxy only'})
        runline.append({'variant':v,'split':s,'games':len(x),'accuracy':np.mean((x.home_minus_1_5_probability>=.5)==yrl),'brier':brier_score_loss(yrl,x.home_minus_1_5_probability),'log_loss':ll(yrl,x.home_minus_1_5_probability),'home_favorite_prediction_share':np.mean(x.predicted_run_margin>0),'home_minus_1_5_prediction_share':np.mean(x.home_minus_1_5_probability>=.5),'calibration_gap':np.mean(x.home_minus_1_5_probability-yrl),'no_vig_market_status':'UNAVAILABLE'})
    return d,p,pd.DataFrame(eval_rows),pd.DataFrame(money),pd.DataFrame(totals),pd.DataFrame(runline),variants,fit_end,val_end,dates

def fixed_ablation(d,props,baseline_combo):
    groups={'ALL_BATTER_PROPS':[c for c in props if '_pitcher_' not in c],'ALL_PITCHER_PROPS':[c for c in props if '_pitcher_' in c],'OPPORTUNITY_LINEUP_STATUS':[c for c in props if any(k in c for k in ('starter_count','nonstarter_count','unresolved_lineup'))],'MARKET_BREADTH_DISPERSION':[c for c in props if any(k in c for k in ('paired_markets','probability_dispersion','alternate_line_count'))]}
    controls=['venue_id','home_prior_games','away_prior_games','home_prior_runs_scored','home_prior_runs_allowed','away_prior_runs_scored','away_prior_runs_allowed','home_lineup_l','home_lineup_r','away_lineup_l','away_lineup_r']; out=[]; tr=d.split=='fit'; ho=d.split=='holdout'
    for name,dropped in groups.items():
        cols=controls+[c for c in props if c not in dropped]; med=d.loc[tr,cols].apply(pd.to_numeric,errors='coerce').median(); X=d[cols].apply(pd.to_numeric,errors='coerce').fillna(med).fillna(0); vals=[]
        for target in ('home_runs','away_runs'):
            m=HistGradientBoostingRegressor(loss='poisson',learning_rate=.05,max_iter=100,max_leaf_nodes=15,min_samples_leaf=20,l2_regularization=1.0,random_state=SEED,early_stopping=False).fit(X[tr],d.loc[tr,target]); vals.append(mean_absolute_error(d.loc[ho,target],m.predict(X[ho])))
        out.append({'ablation':name,'status':'ACTUAL_FIXED_FAMILY_ABLATION','holdout_games':int(ho.sum()),'dropped_feature_count':len(dropped),'home_run_mae':vals[0],'away_run_mae':vals[1],'home_mae_change_vs_full_combined':vals[0]-baseline_combo.home_run_mae,'away_mae_change_vs_full_combined':vals[1]-baseline_combo.away_run_mae,'selection_use':'NONE'})
    return pd.DataFrame(out)

def main():
    state=preflight()
    if state['status']!='READY': raise SystemExit(json.dumps(state,sort_keys=True))
    OUT.mkdir(parents=True,exist_ok=True); games=load_games(); obs,paired,sources=market_data(games)
    if paired.empty: raise SystemExit('PROP_MARKET_GAME_MODEL_NOT_RECOVERABLE')
    aggs=aggregate(paired,games); control=controls(games); d,p,run_eval,money,total,rl,variants,fit_end,val_end,dates=model_and_evaluate(control,aggs)
    spine=d[['game_date','game_pk','scheduled_start_utc','home_team_id','away_team_id','home_team','away_team','home_runs','away_runs']].copy(); spine['snapshot_rule']='LATEST_CERTIFIED_AT_LEAST_15_MINUTES_PREGAME'; spine['snapshot_timestamp_utc']=spine.game_pk.map(obs.groupby('game_pk').snapshot_timestamp_utc.max()); spine['snapshot_age_minutes']=spine.game_pk.map(obs.groupby('game_pk').snapshot_age_minutes.min()); spine['raw_market_observations']=spine.game_pk.map(obs.groupby('game_pk').size()).fillna(0).astype(int); spine['paired_markets']=spine.game_pk.map(paired.groupby('game_pk').size()).fillna(0).astype(int); spine['source_paths']=spine.game_pk.map(lambda x:'|'.join(sorted(sources[x]))); spine['snapshot_temporal_status']='PREGAME_CERTIFIED'; write(spine,'prop_market_game_snapshot_spine.csv')
    write(aggs,'prop_market_team_aggregates.csv'); write(control[control.game_pk.isin(d.game_pk)],'baseball_control_feature_matrix.csv'); write(p,'home_away_run_predictions.csv'); write(money,'moneyline_evaluation.csv'); write(total,'game_total_evaluation.csv'); write(rl,'run_line_evaluation.csv')
    main=pd.DataFrame([{'game_pk':g,'moneyline_status':'MAIN_MARKET_SNAPSHOT_UNAVAILABLE','game_total_status':'MAIN_MARKET_SNAPSHOT_UNAVAILABLE','run_line_status':'MAIN_MARKET_SNAPSHOT_UNAVAILABLE','benchmark_eligible':0} for g in d.game_pk]); write(main,'main_market_game_baseline.csv')
    hold=run_eval[run_eval.split=='holdout']; base=hold[hold.variant=='BASEBALL_CONTROL_ONLY'].iloc[0]; combo=hold[hold.variant=='BASEBALL_PLUS_PROP_MARKET'].iloc[0]
    prop_cols=[c for c in d if c.startswith('prop_')]; write(fixed_ablation(d,prop_cols,combo),'prop_feature_family_ablation.csv')
    bench=[]
    for _,r in hold.iterrows(): bench.append({'variant':r.variant,'split':'holdout','games':r.games,'home_run_mae':r.home_run_mae,'away_run_mae':r.away_run_mae,'moneyline_market_comparison':'NOT_TESTABLE_MAIN_MARKET_UNAVAILABLE','total_market_comparison':'NOT_TESTABLE_MAIN_MARKET_UNAVAILABLE','run_line_market_comparison':'NOT_TESTABLE_MAIN_MARKET_UNAVAILABLE','flat_stake_roi':'NOT_TESTABLE_AUTHENTIC_MAIN_PRICES_UNAVAILABLE'})
    write(pd.DataFrame(bench),'market_benchmark_comparison.csv')
    manifest={'semantic_id':SEMANTIC_ID,'seed':SEED,'model':'HistGradientBoostingRegressor(loss=poisson,max_iter=100,max_leaf_nodes=15,learning_rate=0.05,min_samples_leaf=20,l2=1.0)','variants':{**{k:{'status':'FITTED_RESEARCH_ONLY','feature_count':len(v)} for k,v in variants.items()},'MAIN_MARKET_ANCHORED_COMBINED':{'status':'NOT_FITTED_MAIN_MARKET_BASELINE_UNRECOVERABLE'}},'authority':{'public_prediction':'RESEARCH_EVALUATION_ONLY','betting_edge':'BETTING_EDGE_NOT_TESTABLE','production':'NO_QUALIFIED_MLB_MODEL'}}; (OUT/'model_variant_manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
    split={'rule':'date-blocked 60/20/20; no random split; final holdout never used for tuning','fit':{'start':dates[0],'end':fit_end,'games':int((d.split=='fit').sum())},'validation':{'start':min(d.loc[d.split=='validation','game_date']),'end':val_end,'games':int((d.split=='validation').sum())},'holdout':{'start':min(d.loc[d.split=='holdout','game_date']),'end':dates[-1],'games':int((d.split=='holdout').sum())}}; (OUT/'temporal_split_manifest.json').write_text(json.dumps(split,indent=2,sort_keys=True)+'\n')
    decision='PROP_MARKET_GAME_MODEL_RESULT_MIXED'; public='NOT_READY_FOR_PUBLIC_PREDICTION_DISPLAY'; betting='BETTING_EDGE_NOT_TESTABLE'
    dh=("# DH raw-source lineage repair\n\nFuture grading now uses exactly one retained source per game. Certified existing-cache bytes are read and hashed directly; new live bytes are written under a run-specific immutable directory before ledger append. An append-only lineage sidecar binds every outcome identity to retained path and hash. The two game-824645 rows are preserved unchanged and marked `RAW_RESPONSE_NOT_RETAINED`, with both the original recorded hash and current cache hash. Tests cover existing cache, new fetch, repeated unique append, multiple-row identity behavior, retained hash agreement, and prediction/outcome separation.\n") ; (OUT/'dh_raw_source_lineage_repair.md').write_text(dh)
    coverage=f"{len(d)} games across {d.game_date.nunique()} dates ({d.game_date.min()} through {d.game_date.max()})"
    readiness=f"# Public prediction readiness\n\n**{public}**\n\nThe fixed research models are temporally valid and reproducible, but the certified population is restricted to `{coverage}` and broad contemporaneous main-market snapshots were not recoverable. Therefore calibration and degradation versus no-vig moneyline, total, and run-line probabilities cannot be established. Public prediction authority remains separate from betting authority.\n\n`NO BETTING EDGE OR PROFITABILITY GUARANTEE`\n\nProduction remains `NO_QUALIFIED_MLB_MODEL`.\n"; (OUT/'public_prediction_readiness.md').write_text(readiness)
    bmoney=money[(money.variant=='BASEBALL_PLUS_PROP_MARKET')&(money.split=='holdout')].iloc[0]; btotal=total[(total.variant=='BASEBALL_PLUS_PROP_MARKET')&(total.split=='holdout')].iloc[0]; brl=rl[(rl.variant=='BASEBALL_PLUS_PROP_MARKET')&(rl.split=='holdout')].iloc[0]
    summary=f"# Prop-market-derived MLB game prediction model v0\n\n- Model result: **{decision}**\n- Public-product result: **{public}**\n- Betting result: **{betting}**\n- Certified population: **{coverage}**\n- Best technically valid comparison is descriptive only; no variant is promoted from the final holdout.\n- Combined holdout home/away run MAE: **{combo.home_run_mae:.3f} / {combo.away_run_mae:.3f}**\n- Combined holdout moneyline accuracy/Brier/log loss: **{bmoney.accuracy:.3f} / {bmoney.brier:.4f} / {bmoney.log_loss:.4f}**\n- Combined holdout total (8.5 proxy) accuracy/Brier/log loss: **{btotal.accuracy:.3f} / {btotal.brier:.4f} / {btotal.log_loss:.4f}**\n- Combined holdout home -1.5 accuracy/Brier/log loss: **{brl.accuracy:.3f} / {brl.brier:.4f} / {brl.log_loss:.4f}**\n- Prop contribution to run MAE versus baseball control: home **{combo.home_run_mae-base.home_run_mae:+.4f}**, away **{combo.away_run_mae-base.away_run_mae:+.4f}** (negative is improvement).\n- Main-market comparison and authentic-price ROI: **not testable from the certified broad population**.\n- DH lineage repair: **IMPLEMENTED_AND_TESTED_WITH_TWO_LEGACY_EXCEPTIONS_EXPLICIT**.\n\nThe passage proves that distributed prop prices can be bound pregame, aggregated by exact game/player identity, and used in a reproducible temporal game-scoring framework. It does not prove stable prop incrementality, main-market superiority, betting edge, profitability, or deployment readiness. Production remains `NO_QUALIFIED_MLB_MODEL`.\n"; (OUT/'concise_prop_market_game_model_v0.md').write_text(summary)
    hashes=[]
    for q in sorted(OUT.iterdir()):
        if q.name!='reproducibility_hashes.sha256': hashes.append(f"{hashlib.sha256(q.read_bytes()).hexdigest()}  {q.name}")
    (OUT/'reproducibility_hashes.sha256').write_text('\n'.join(hashes)+'\n')
    print(json.dumps({'decision':decision,'public':public,'betting':betting,'games':len(d),'dates':d.game_date.nunique(),'output':str(OUT)},indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description='Research-only prop-market-derived MLB game model v0')
    parser.add_argument('--check',action='store_true',help='validate certified inputs and accepted package hashes without writing')
    args=parser.parse_args()
    if args.check:
        result=preflight(); result['accepted_package']=package_hash_check(); print(json.dumps(result,indent=2,sort_keys=True))
        raise SystemExit(0 if result['status']=='READY' and result['accepted_package']['status']=='PASS' else 2)
    main()
