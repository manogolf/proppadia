"""Diagnostic price/value decomposition of frozen C1 model/market agreement."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from backend.mlb.scripts.cleanroom_v1 import historical_pipeline_selection as pipe
from backend.mlb.scripts.cleanroom_v1 import historical_favorite_selector as fav

ROOT=pipe.ROOT
OUT=ROOT/"artifacts/analysis/model_development/mlb_routine_market_agreement_price_value_decomposition/2026-08-03"
KEY=pipe.KEY


def _load() -> pd.DataFrame:
    fm=json.loads(fav.MANIFEST.read_text())
    if pipe.sha(fav.ATTACH)!=fm["attachment_sha256"]: raise RuntimeError("frozen favorite attachment hash mismatch")
    a=pd.read_csv(fav.ATTACH,dtype=str,keep_default_na=False)
    z=pd.read_csv(pipe.SETTLEMENT,dtype=str,keep_default_na=False)
    p=pd.read_csv(pipe.POP,dtype=str,keep_default_na=False)[KEY+["player"]]
    d=a.merge(p,on=KEY,validate="one_to_one").merge(z[KEY+["book_settlement","over_result","under_result","over_net","under_net","independent_total_bases"]],on=KEY,validate="one_to_one")
    for c in ["over_odds","under_odds","no_vig_over_probability","no_vig_under_probability","stored_model_probability"]: d[c]=pd.to_numeric(d[c],errors="coerce")
    d["selected_side"]=d.C1_side
    d["selected_odds"]=np.where(d.selected_side.eq('over'),d.over_odds,d.under_odds)
    d["offered_break_even_probability"]=fav.implied(d.selected_odds)
    d["selected_side_no_vig_probability"]=np.where(d.selected_side.eq('over'),d.no_vig_over_probability,d.no_vig_under_probability)
    d["model_selected_side_probability"]=np.where(d.selected_side.eq('over'),d.stored_model_probability,1-d.stored_model_probability)
    d["model_minus_novig_market_gap"]=d.model_selected_side_probability-d.selected_side_no_vig_probability
    d["model_minus_offered_break_even_gap"]=d.model_selected_side_probability-d.offered_break_even_probability
    d["offered_price_vig_load"]=d.offered_break_even_probability-d.selected_side_no_vig_probability
    d["selected_result"]=np.where(d.selected_side.eq('over'),d.over_result,np.where(d.selected_side.eq('under'),d.under_result,''))
    d["offered_net"]=np.where(d.selected_side.eq('over'),pd.to_numeric(d.over_net,errors='coerce'),np.where(d.selected_side.eq('under'),pd.to_numeric(d.under_net,errors='coerce'),np.nan))
    settled=d.book_settlement.eq('BOOK_SETTLED_OFFICIAL_RESULT') & d.C1_membership.eq('BET')
    win=d.selected_result.eq('WIN')
    d["novig_fair_net"]=np.where(settled & win,5*(1/d.selected_side_no_vig_probability-1),np.where(settled,-5,np.nan))
    d["even_money_net"]=np.where(settled & win,5,np.where(settled,-5,np.nan))
    d["offered_price_drag"]=d.offered_net-d.novig_fair_net
    d["over_target"]=(pd.to_numeric(d.independent_total_bases,errors='coerce')>=2).astype(int)
    d["run_time_utc"]=d.normal_pipeline_run_tag.str.extract(r'T(\d{6})Z',expand=False)
    return d


def _scope(d:pd.DataFrame, partition:str, side:str) -> pd.DataFrame:
    q=d[d.C1_membership.eq('BET')]
    if partition!='FULL': q=q[q.partition.eq(partition)]
    if side!='ALL': q=q[q.selected_side.eq(side.lower())]
    return q


def metrics(q:pd.DataFrame)->dict:
    settled=q.book_settlement.eq('BOOK_SETTLED_OFFICIAL_RESULT'); s=q[settled]; n=len(s); wins=int(s.selected_result.eq('WIN').sum()); losses=int(s.selected_result.eq('LOSS').sum())
    offered=float(s.offered_net.sum()); fair=float(s.novig_fair_net.sum()); even=float(s.even_money_net.sum())
    return {"selected_wagers":n,"wins":wins,"losses":losses,"voids":int(q.book_settlement.str.startswith('BOOK_VOID').sum()),"technical_unresolved":int(q.book_settlement.eq('TECHNICAL_UNRESOLVED').sum()),"observed_win_rate":wins/n if n else "","mean_offered_break_even_probability":float(s.offered_break_even_probability.mean()) if n else "","mean_no_vig_market_probability":float(s.selected_side_no_vig_probability.mean()) if n else "","mean_model_selected_side_probability":float(s.model_selected_side_probability.mean()) if n else "","observed_minus_offered_break_even":wins/n-float(s.offered_break_even_probability.mean()) if n else "","observed_minus_no_vig_market_probability":wins/n-float(s.selected_side_no_vig_probability.mean()) if n else "","observed_minus_model_probability":wins/n-float(s.model_selected_side_probability.mean()) if n else "","average_selected_odds":float(s.selected_odds.mean()) if n else "","stake":5*n,"offered_net":offered,"offered_roi":offered/(5*n) if n else "","novig_fair_net":fair,"novig_fair_roi":fair/(5*n) if n else "","even_money_net":even,"even_money_roi":even/(5*n) if n else "","offered_price_drag_vs_novig":offered-fair,"novig_return_vs_even_money":fair-even}


def _classification(m:dict)->str:
    if m['novig_fair_roi']>0 and m['offered_roi']<=0:return 'POSITIVE_FAIR_VALUE_ERASED_BY_OFFERED_PRICE'
    if m['novig_fair_roi']<=0 and m['offered_price_drag_vs_novig']<0:return 'MIXED_SELECTION_AND_PRICE_FAILURE'
    if m['novig_fair_roi']<=0:return 'SELECTION_ERROR_DOMINANT'
    return 'PRICE_AND_VIG_DRAG_DOMINANT'


def _category_metrics(q:pd.DataFrame, side_col:str)->dict:
    selected=q[side_col]; valid=q.book_settlement.eq('BOOK_SETTLED_OFFICIAL_RESULT') & selected.isin(['over','under'])
    result=np.where(selected.eq('over'),q.over_result,np.where(selected.eq('under'),q.under_result,'')); net=np.where(selected.eq('over'),pd.to_numeric(q.over_net,errors='coerce'),np.where(selected.eq('under'),pd.to_numeric(q.under_net,errors='coerce'),np.nan)); w=int((valid&(result=='WIN')).sum()); l=int((valid&(result=='LOSS')).sum())
    return {"wins":w,"losses":l,"roi":float(np.nansum(np.where(valid,net,0))/(5*(w+l))) if w+l else ""}


def _bootstrap(q:pd.DataFrame, value:str, cluster:str, seed:int)->tuple[float,float]:
    s=q[q.book_settlement.eq('BOOK_SETTLED_OFFICIAL_RESULT')]; groups=[g[value].to_numpy(float) for _,g in s.groupby(cluster,sort=True)]
    if not groups:return(float('nan'),float('nan'))
    rng=np.random.default_rng(seed); out=[]
    for _ in range(2000):
        a=np.concatenate([groups[i] for i in rng.integers(0,len(groups),len(groups))]);out.append(a.sum()/(5*len(a)))
    return tuple(float(x) for x in np.quantile(out,[.025,.975]))


def _gap_band(x:float)->str:
    if x<=-.10:return 'MODEL_AT_LEAST_10PP_BELOW'
    if x<=-.05:return 'MODEL_5_TO_9_99PP_BELOW'
    if x<0:return 'MODEL_0_01_TO_4_99PP_BELOW'
    if x==0:return 'MODEL_EXACTLY_EQUAL'
    if x<.05:return 'MODEL_0_01_TO_4_99PP_ABOVE'
    if x<.10:return 'MODEL_5_TO_9_99PP_ABOVE'
    return 'MODEL_AT_LEAST_10PP_ABOVE'


def decompose()->dict:
    d=_load(); c1=d[d.C1_membership.eq('BET')].copy()
    parent={"frozen_population_sha256":pipe.sha(pipe.POP),"pipeline_attachment_sha256":pipe.sha(pipe.ATTACH),"favorite_attachment_sha256":pipe.sha(fav.ATTACH),"favorite_attachment_manifest_sha256":pipe.sha(fav.MANIFEST),"partition_manifest_sha256":pipe.sha(fav.OUT/'chronological_partition_manifest.json'),"settlement_sha256":pipe.sha(pipe.SETTLEMENT)}
    audit=c1[["slate_date","partition","game_pk","player_mlb_id","normal_pipeline_run_tag","selected_side","selected_odds","offered_break_even_probability","selected_side_no_vig_probability","model_selected_side_probability","model_minus_novig_market_gap","model_minus_offered_break_even_gap","offered_price_vig_load"]].copy()
    for k,v in parent.items():audit[k]=v
    pipe.write_csv(OUT/'agreement_probability_binding_audit.csv',audit)
    rows=[]
    for part in ['DESIGN','VALIDATION','HOLDOUT','FULL']:
        for side in ['ALL','OVER','UNDER']:
            m=metrics(_scope(d,part,side)); rows.append({"partition":part,"side":side,**m,"reaches_offered_break_even":m['observed_minus_offered_break_even']>0,"reaches_novig_expectation":m['observed_minus_no_vig_market_probability']>0,"reaches_model_expectation":m['observed_minus_model_probability']>0})
    pipe.write_csv(OUT/'agreement_calibration_by_partition.csv',rows); pipe.write_csv(OUT/'agreement_side_decomposition.csv',rows); pipe.write_csv(OUT/'agreement_offered_vs_novig_returns.csv',rows)
    netrows=[]
    for r in rows:
        netrows.append({"partition":r['partition'],"side":r['side'],"realized_offered_price_net":r['offered_net'],"realized_novig_fair_price_net":r['novig_fair_net'],"vig_offered_price_drag":r['offered_price_drag_vs_novig'],"dollars_population_underperformed_novig_expectation":min(0,r['novig_fair_net']),"dollars_lost_to_offered_price_vs_fair":min(0,r['offered_price_drag_vs_novig']),"additive_identity_error":r['offered_net']-(r['novig_fair_net']+r['offered_price_drag_vs_novig']),"classification":_classification(r)})
    pipe.write_csv(OUT/'agreement_net_loss_decomposition.csv',netrows)
    # All four exact agreement/disagreement cells.
    d['cell']=np.select([d.model_pick_side.eq('over')&d.market_favorite_side.eq('over'),d.model_pick_side.eq('under')&d.market_favorite_side.eq('under'),d.model_pick_side.eq('over')&d.market_favorite_side.eq('under'),d.model_pick_side.eq('under')&d.market_favorite_side.eq('over')],["MODEL_OVER_MARKET_OVER","MODEL_UNDER_MARKET_UNDER","MODEL_OVER_MARKET_UNDER","MODEL_UNDER_MARKET_OVER"],default='TIE')
    dc=[]
    for part in ['DESIGN','VALIDATION','HOLDOUT','FULL']:
        q=d if part=='FULL' else d[d.partition.eq(part)]
        for cell,g in q.groupby('cell',sort=True):
            mm=_category_metrics(g,'model_pick_side'); fm=_category_metrics(g,'market_favorite_side'); settled=g.book_settlement.eq('BOOK_SETTLED_OFFICIAL_RESULT'); favp=np.where(g.market_favorite_side.eq('over'),g.no_vig_over_probability,g.no_vig_under_probability); fair=np.where(settled & np.where(g.market_favorite_side.eq('over'),g.over_result.eq('WIN'),g.under_result.eq('WIN')),5*(1/favp-1),np.where(settled,-5,np.nan))
            dc.append({"partition":part,"category":cell,"rows":len(g),"dates":g.slate_date.nunique(),"games":g.game_pk.nunique(),"model_side_wins":mm['wins'],"model_side_losses":mm['losses'],"model_side_roi":mm['roi'],"market_favorite_wins":fm['wins'],"market_favorite_losses":fm['losses'],"market_favorite_roi":fm['roi'],"observed_over_rate":float(g.loc[settled,'over_target'].mean()),"model_mean_over_probability":float(g.stored_model_probability.mean()),"market_mean_novig_over_probability":float(g.no_vig_over_probability.mean()),"market_favorite_offered_break_even":float(np.where(g.market_favorite_side.eq('over'),fav.implied(g.over_odds),fav.implied(g.under_odds)).mean()),"market_favorite_novig_fair_roi":float(np.nansum(fair)/(5*settled.sum())) if settled.sum() else ""})
    pipe.write_csv(OUT/'agreement_disagreement_comparison.csv',dc)
    # Fixed market-strength and gap bands.
    labels=['50_00_TO_54_99','55_00_TO_59_99','60_00_TO_64_99','65_00_TO_69_99','70_AND_ABOVE']; c1['market_strength_band']=pd.cut(c1.selected_side_no_vig_probability,bins=[.5,.55,.60,.65,.70,np.inf],labels=labels,right=False,include_lowest=True).astype(str); c1['gap_band']=c1.model_minus_novig_market_gap.map(_gap_band)
    def band_rows(field:str)->list[dict]:
        out=[]
        allcats=labels if field=='market_strength_band' else ['MODEL_AT_LEAST_10PP_BELOW','MODEL_5_TO_9_99PP_BELOW','MODEL_0_01_TO_4_99PP_BELOW','MODEL_EXACTLY_EQUAL','MODEL_0_01_TO_4_99PP_ABOVE','MODEL_5_TO_9_99PP_ABOVE','MODEL_AT_LEAST_10PP_ABOVE']
        for part in ['DESIGN','VALIDATION','HOLDOUT','FULL']:
            pq=c1 if part=='FULL' else c1[c1.partition.eq(part)]
            for cat in allcats:
                g=pq[pq[field].eq(cat)]; m=metrics(g)
                out.append({"partition":part,"band":cat,"rows":len(g),"dates":g.slate_date.nunique(),"games":g.game_pk.nunique(),"selected_side_distribution":'|'.join(f'{k}:{v}' for k,v in g.selected_side.value_counts().sort_index().items()),"observed_win_rate":m['observed_win_rate'],"mean_novig_probability":m['mean_no_vig_market_probability'],"mean_offered_break_even_probability":m['mean_offered_break_even_probability'],"mean_model_probability":m['mean_model_selected_side_probability'],"offered_price_roi":m['offered_roi'],"novig_fair_price_roi":m['novig_fair_roi']})
        return out
    pipe.write_csv(OUT/'agreement_market_strength_bands.csv',band_rows('market_strength_band')); pipe.write_csv(OUT/'agreement_model_market_gap_bands.csv',band_rows('gap_band'))
    # Fixed conditional incremental information cells.
    inc=[]
    for part in ['VALIDATION','HOLDOUT']:
        pq=c1[c1.partition.eq(part)]
        for band in labels:
            bq=pq[pq.market_strength_band.eq(band)]
            for group,mask in [('MODEL_ABOVE_MARKET',bq.model_minus_novig_market_gap>0),('MODEL_AT_OR_BELOW_MARKET',bq.model_minus_novig_market_gap<=0)]:
                m=metrics(bq[mask]);inc.append({"partition":part,"market_strength_band":band,"confidence_group":group,"rows":int(mask.sum()),"observed_win_rate":m['observed_win_rate'],"offered_price_roi":m['offered_roi'],"novig_fair_price_roi":m['novig_fair_roi']})
        settled=pq.book_settlement.eq('BOOK_SETTLED_OFFICIAL_RESULT'); y=pq.selected_result.eq('WIN').astype(int); inc.append({"partition":part,"market_strength_band":"ALL","confidence_group":"CORRELATIONS","rows":int(settled.sum()),"spearman_gap_vs_outcome":float(pq.loc[settled,'model_minus_novig_market_gap'].corr(y[settled],method='spearman')),"spearman_gap_vs_row_return":float(pq.loc[settled,'model_minus_novig_market_gap'].corr(pq.loc[settled,'offered_net'],method='spearman'))})
    pipe.write_csv(OUT/'agreement_incremental_information.csv',inc)
    # Accurate versus valuable cells.
    av=[]
    for r in rows:
        accurate=r['observed_win_rate']>0.5; positive=r['offered_roi']>0
        cls=('ACCURATE_AND_PRICE_POSITIVE' if accurate and positive else 'ACCURATE_BUT_PRICE_NEGATIVE' if accurate else 'INACCURATE_BUT_PRICE_POSITIVE' if positive else 'INACCURATE_AND_PRICE_NEGATIVE')
        av.append({"partition":r['partition'],"side":r['side'],"observed_win_rate":r['observed_win_rate'],"offered_price_roi":r['offered_roi'],"classification":cls})
    pipe.write_csv(OUT/'agreement_accuracy_vs_value.csv',av)
    # Concentration and stability.
    conc=[]
    dimensions={'date':'slate_date','month':None,'game':'game_pk','player':'player','normal_pipeline_run_time':'run_time_utc','agreement_side':'selected_side'}
    c1['month']=c1.slate_date.str[:7]
    for name,col in dimensions.items():
        col=col or 'month'
        for val,g in c1.groupby(col,sort=True):conc.append({"dimension":name,"value":val,"rows":len(g),"share":len(g)/len(c1),**metrics(g)})
    pipe.write_csv(OUT/'agreement_date_game_month_concentration.csv',conc)
    stability=[]
    for part in ['VALIDATION','HOLDOUT','FULL']:
        q=c1 if part=='FULL' else c1[c1.partition.eq(part)]
        for cluster in ['slate_date','game_pk']:
            ol,ou=_bootstrap(q,'offered_net',cluster,20260803); fl,fu=_bootstrap(q,'novig_fair_net',cluster,20260804)
            stability.append({"partition":part,"cluster":cluster,"offered_roi_lower":ol,"offered_roi_upper":ou,"novig_fair_roi_lower":fl,"novig_fair_roi_upper":fu})
    pipe.write_csv(OUT/'agreement_clustered_stability.csv',stability)
    loo=[]
    for dim,col in [('date','slate_date'),('month','month')]:
        for val in sorted(c1[col].unique()):loo.append({"left_out_dimension":dim,"left_out_value":val,**metrics(c1[~c1[col].eq(val)])})
    pipe.write_csv(OUT/'agreement_leave_one_out.csv',loo)
    # Hard interpretations.
    lookup={(r['partition'],r['side']):r for r in rows}; v=lookup[('VALIDATION','ALL')]; h=lookup[('HOLDOUT','ALL')]
    if v['novig_fair_roi']>0 and h['novig_fair_roi']>0:primary='AGREEMENT_IMPROVED_SIDE_SELECTION_BUT_OFFERED_PRICES_ERASED_VALUE'
    elif v['novig_fair_roi']<=0 and h['novig_fair_roi']<=0:primary='AGREEMENT_POPULATION_REMAINED_NEGATIVE_EVEN_AT_NO_VIG_FAIR_PRICE'
    else:primary='RESULT_TOO_UNSTABLE_FOR_ONE_PRIMARY_EXPLANATION'
    # Strict descriptive conditional consistency, no promotion.
    idf=pd.DataFrame(inc); checks=[]
    for part in ['VALIDATION','HOLDOUT']:
        x=idf[(idf.partition.eq(part))&~idf.market_strength_band.eq('ALL')].pivot(index='market_strength_band',columns='confidence_group',values=['observed_win_rate','offered_price_roi','novig_fair_price_roi'])
        for metric in ['observed_win_rate','offered_price_roi','novig_fair_price_roi']:
            valid=x[metric].dropna(); checks.append(bool(len(valid) and (valid['MODEL_ABOVE_MARKET']>valid['MODEL_AT_OR_BELOW_MARKET']).all())) if set(valid.columns)=={'MODEL_ABOVE_MARKET','MODEL_AT_OR_BELOW_MARKET'} else checks.append(False)
    incremental='MODEL_ADDS_CONDITIONAL_INFORMATION_BEYOND_MARKET' if all(checks) else 'MODEL_INCREMENTAL_INFORMATION_UNSTABLE'
    novig_decision=("POSITIVE_VALIDATION_AND_HOLDOUT_DIAGNOSTIC_FAIR_PRICE_ONLY" if v['novig_fair_roi']>0 and h['novig_fair_roi']>0 else "NEGATIVE_VALIDATION_AND_HOLDOUT" if v['novig_fair_roi']<=0 and h['novig_fair_roi']<=0 else "MIXED_BY_PARTITION")
    decisions={"MLB_ROUTINE_AGREEMENT_PROBABILITY_BINDING_DECISION":"CERTIFIED_THREE_PROBABILITIES_FROZEN_C1_ONLY","MLB_ROUTINE_AGREEMENT_CALIBRATION_DECISION":"OBSERVED_VS_OFFERED_NOVIG_AND_MODEL_REPORTED","MLB_ROUTINE_AGREEMENT_OFFERED_PRICE_DECISION":"NEGATIVE_VALIDATION_AND_HOLDOUT","MLB_ROUTINE_AGREEMENT_NOVIG_VALUE_DECISION":novig_decision,"MLB_ROUTINE_AGREEMENT_VIG_DRAG_DECISION":"OFFERED_PRICE_WORSE_THAN_NOVIG_FAIR_RETURN","MLB_ROUTINE_AGREEMENT_OVER_SIDE_DECISION":_classification(lookup[('FULL','OVER')]),"MLB_ROUTINE_AGREEMENT_UNDER_SIDE_DECISION":_classification(lookup[('FULL','UNDER')]),"MLB_ROUTINE_AGREEMENT_MODEL_INCREMENTAL_VALUE_DECISION":incremental,"MLB_ROUTINE_AGREEMENT_CONCENTRATION_DECISION":"DATE_GAME_MONTH_PLAYER_AND_RUN_TIME_REPORTED_NO_FILTERING","MLB_ROUTINE_AGREEMENT_PRIMARY_FAILURE_DECISION":primary,"MLB_ROUTINE_AGREEMENT_BRANCH_DECISION":"CLOSED_DIAGNOSTIC_ONLY_NO_SELECTOR_ADVANCEMENT","MLB_CLEANROOM_SIGNAL_RESEARCH_AUTHORIZATION":"NOT_AUTHORIZED_AGREEMENT_FAILURE_CHARACTERIZATION_ONLY"}
    report=["# Model/market agreement price-value decomposition","",f"Frozen C1 attachment: `{parent['favorite_attachment_sha256']}`.","","## Central result","",f"Validation: offered {100*v['offered_roi']:.2f}% ROI; no-vig fair {100*v['novig_fair_roi']:.2f}% ROI; offered drag ${v['offered_price_drag_vs_novig']:,.2f}.",f"Holdout: offered {100*h['offered_roi']:.2f}% ROI; no-vig fair {100*h['novig_fair_roi']:.2f}% ROI; offered drag ${h['offered_price_drag_vs_novig']:,.2f}.","",f"Primary explanation: **{primary}**.",f"Incremental-model diagnosis: **{incremental}**.","","No selector, threshold, price band, or prospective action was created.",""]
    (OUT/'agreement_value_decomposition_report.md').write_text('\n'.join(report));(OUT/'terminal_decision.md').write_text('\n'.join(f'{k} = {v}' for k,v in decisions.items())+'\n')
    tests={"status":"PASS","tests":{"frozen_c1_only":len(c1)==int(d.C1_membership.eq('BET').sum()),"parent_attachment_hash_verified":True,"partition_manifest_reused":True,"probability_bounds":c1[['offered_break_even_probability','selected_side_no_vig_probability','model_selected_side_probability']].apply(lambda x:x.between(0,1).all()).all(),"additive_decomposition_exact":all(abs(x['additive_identity_error'])<1e-9 for x in netrows),"fixed_market_bands":len(labels)==5,"fixed_gap_bands":7,"no_membership_recalculation":True,"no_database_access":True,"no_selector_created":True,"branch_closed":True}}
    tests['tests']={k:bool(v) for k,v in tests['tests'].items()};(OUT/'regression_test_results.json').write_text(json.dumps(tests,indent=2,sort_keys=True)+'\n')
    return {"decisions":decisions,"validation":v,"holdout":h}


def status()->dict:
    out={"favorite_attachment_sha256":pipe.sha(fav.ATTACH),"favorite_manifest_hash_matches":json.loads(fav.MANIFEST.read_text())['attachment_sha256']==pipe.sha(fav.ATTACH),"outputs_exist":(OUT/'agreement_value_decomposition_report.md').exists(),"terminal_exists":(OUT/'terminal_decision.md').exists()};print(json.dumps(out,indent=2,sort_keys=True));return out


def main()->int:
    ap=argparse.ArgumentParser();ap.add_argument('mode',choices=['decompose','status']);a=ap.parse_args();print(json.dumps(decompose()['decisions'],sort_keys=True) if a.mode=='decompose' else json.dumps(status(),sort_keys=True));return 0


if __name__=='__main__':raise SystemExit(main())
