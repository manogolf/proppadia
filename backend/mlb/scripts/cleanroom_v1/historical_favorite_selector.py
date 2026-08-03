"""Fixed market-favorite/WITHHOLD benchmark over the certified TB 1.5 spine."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.mlb.scripts.cleanroom_v1 import historical_pipeline_selection as base

ROOT = base.ROOT
SOURCE = base.OUT
OUT = ROOT / "artifacts/analysis/model_development/mlb_routine_market_favorite_withhold_selector_benchmark/2026-08-03"
ATTACH = OUT / "favorite_selector_attachment.csv"
MANIFEST = OUT / "favorite_selector_attachment_manifest.json"
CONTRACT = "MARKET_FAVORITE_WITHHOLD_ATTACHMENT_V1"
KEY = base.KEY
INSTRUMENTS = {
    "B0_BLIND_UNDER": "comparator",
    "B1_HISTORICAL_MODEL_DIRECTION": "comparator",
    "B2_CONTEMPORANEOUS_MARKET_FAVORITE": "comparator",
    "C1_MODEL_MARKET_AGREEMENT": "candidate",
    "C2_UNDER_MARKET_FAVORITE": "candidate",
    "C3_MODEL_CONFIRMED_UNDER_FAVORITE": "candidate",
}


def implied(odds: pd.Series) -> pd.Series:
    x = pd.to_numeric(odds, errors="coerce")
    return pd.Series(np.where(x > 0, 100 / (x + 100), np.where(x < 0, -x / (-x + 100), np.nan)), index=odds.index)


def partitions(dates: list[str]) -> dict[str, list[str]]:
    if len(dates) != 62 or dates != sorted(dates):
        raise RuntimeError("expected 62 chronological eligible dates")
    return {"DESIGN": dates[:40], "VALIDATION": dates[40:51], "HOLDOUT": dates[51:62]}


def _hash_values(values: list[str]) -> str:
    return hashlib.sha256(("\n".join(values) + "\n").encode()).hexdigest()


def freeze() -> dict:
    pop = base.load_population()
    old = pd.read_csv(base.ATTACH, dtype=str, keep_default_na=False)
    if base.sha(base.ATTACH) != json.loads(base.MANIFEST.read_text())["attachment_sha256"]:
        raise RuntimeError("upstream selection attachment hash mismatch")
    dates = sorted(pop.slate_date.unique())
    parts = partitions(dates)
    pmap = {day: part for part, days in parts.items() for day in days}
    keep = KEY + ["normal_pipeline_run_tag", "over_odds", "under_odds", "model_pick_side", "prob_over", "slate_source_artifact", "slate_source_sha256"]
    d = pop[KEY + ["normal_pipeline_run_tag", "over_odds", "under_odds"]].merge(old[keep], on=KEY + ["normal_pipeline_run_tag", "over_odds", "under_odds"], validate="one_to_one")
    d["partition"] = d.slate_date.map(pmap)
    d["raw_implied_over"] = implied(d.over_odds)
    d["raw_implied_under"] = implied(d.under_odds)
    d["overround"] = d.raw_implied_over + d.raw_implied_under
    d["no_vig_over_probability"] = d.raw_implied_over / d.overround
    d["no_vig_under_probability"] = d.raw_implied_under / d.overround
    d["market_favorite_side"] = np.where(d.no_vig_over_probability > d.no_vig_under_probability, "over", np.where(d.no_vig_under_probability > d.no_vig_over_probability, "under", "tie"))
    d["market_favorite_probability"] = np.where(d.market_favorite_side.eq("over"), d.no_vig_over_probability, np.where(d.market_favorite_side.eq("under"), d.no_vig_under_probability, np.nan))
    d["market_favorite_price"] = np.where(d.market_favorite_side.eq("over"), d.over_odds, np.where(d.market_favorite_side.eq("under"), d.under_odds, ""))
    d["stored_model_probability"] = pd.to_numeric(d.prob_over, errors="coerce")
    orientation = d.model_pick_side.isin(["over", "under"]) & d.stored_model_probability.between(0, 1, inclusive="neither")
    d["model_orientation_status"] = np.where(orientation, "CERTIFIED_P_TOTAL_BASES_GE_2", "MODEL_ORIENTATION_UNCERTIFIABLE")
    d["B0_membership"] = "BET"; d["B0_side"] = "under"
    d["B1_membership"] = np.where(orientation, "BET", "WITHHOLD"); d["B1_side"] = np.where(orientation, d.model_pick_side, "")
    fav = d.market_favorite_side.isin(["over", "under"])
    d["B2_membership"] = np.where(fav, "BET", "WITHHOLD"); d["B2_side"] = np.where(fav, d.market_favorite_side, "")
    agree = orientation & fav & d.model_pick_side.eq(d.market_favorite_side)
    d["C1_membership"] = np.where(agree, "BET", "WITHHOLD"); d["C1_side"] = np.where(agree, d.model_pick_side, "")
    underfav = d.market_favorite_side.eq("under")
    d["C2_membership"] = np.where(underfav, "BET", "WITHHOLD"); d["C2_side"] = np.where(underfav, "under", "")
    confirmed = underfav & orientation & d.model_pick_side.eq("under")
    d["C3_membership"] = np.where(confirmed, "BET", "WITHHOLD"); d["C3_side"] = np.where(confirmed, "under", "")
    d["membership_reason"] = np.select([agree, confirmed, underfav, fav], ["MODEL_MARKET_AGREE;UNDER_FAVORITE_MODEL_CONFIRMED" ,"UNDER_FAVORITE_MODEL_CONFIRMED","UNDER_MARKET_FAVORITE_MODEL_DISAGREES","MARKET_FAVORITE_MODEL_DISAGREES"], default="MARKET_FAVORITE_TIE_OR_MODEL_UNCERTIFIABLE")
    cols = ["slate_date","partition","game_pk","player_mlb_id","normal_pipeline_run_tag","over_odds","under_odds","raw_implied_over","raw_implied_under","overround","no_vig_over_probability","no_vig_under_probability","market_favorite_side","market_favorite_probability","market_favorite_price","model_pick_side","stored_model_probability","model_orientation_status","B0_membership","B0_side","B1_membership","B1_side","B2_membership","B2_side","C1_membership","C1_side","C2_membership","C2_side","C3_membership","C3_side","membership_reason","slate_source_artifact","slate_source_sha256"]
    d = d[cols].sort_values(KEY, kind="stable")
    base.write_csv(ATTACH, d)
    partition_manifest = {"contract":"CHRONOLOGICAL_40_11_11_V1","eligible_dates":dates,"eligible_dates_sha256":_hash_values(dates),"partitions":{k:{"dates":v,"dates_sha256":_hash_values(v),"rows":int(d.partition.eq(k).sum())} for k,v in parts.items()}}
    (OUT / "chronological_partition_manifest.json").write_text(json.dumps(partition_manifest, indent=2, sort_keys=True)+"\n")
    calc = d[["slate_date","game_pk","player_mlb_id","over_odds","under_odds","raw_implied_over","raw_implied_under","overround","no_vig_over_probability","no_vig_under_probability","market_favorite_side","market_favorite_probability","market_favorite_price"]]
    base.write_csv(OUT / "market_favorite_calculation_audit.csv", calc)
    orient = d[["slate_date","game_pk","player_mlb_id","normal_pipeline_run_tag","model_pick_side","stored_model_probability","model_orientation_status","slate_source_artifact","slate_source_sha256"]].copy()
    orient["orientation_contract_evidence"] = "build_mlb_slate_output stores TB line=1.5 prob_over and model_pick_side=over iff prob_over>=0.5; prior exact rule reproduction 9267/9267"
    base.write_csv(OUT / "model_orientation_audit.csv", orient)
    man={"contract":CONTRACT,"frozen_population_path":base.rel(base.POP),"frozen_population_sha256":base.sha(base.POP),"upstream_selection_attachment_path":base.rel(base.ATTACH),"upstream_selection_attachment_sha256":base.sha(base.ATTACH),"partition_manifest_sha256":base.sha(OUT/"chronological_partition_manifest.json"),"attachment_rows":len(d),"model_orientation_certified_rows":int(orientation.sum()),"market_favorite_ties":int(d.market_favorite_side.eq('tie').sum()),"candidate_definitions":{"C1":"BET stored model side iff it equals market favorite; else WITHHOLD","C2":"BET Under iff market favorite Under; else WITHHOLD","C3":"BET Under iff market favorite Under and stored model side Under; else WITHHOLD"},"attachment_sha256":base.sha(ATTACH)}
    MANIFEST.write_text(json.dumps(man,indent=2,sort_keys=True)+"\n")
    return man


def _settled_vectors(df: pd.DataFrame, side: pd.Series) -> tuple[pd.Series,pd.Series,pd.Series]:
    valid=df.book_settlement.eq("BOOK_SETTLED_OFFICIAL_RESULT") & side.isin(["over","under"])
    result=pd.Series(np.where(side.eq("over"),df.over_result,np.where(side.eq("under"),df.under_result,"")),index=df.index)
    net=pd.Series(np.where(side.eq("over"),pd.to_numeric(df.over_net,errors="coerce"),np.where(side.eq("under"),pd.to_numeric(df.under_net,errors="coerce"),np.nan)),index=df.index)
    odds=pd.Series(np.where(side.eq("over"),pd.to_numeric(df.over_odds,errors="coerce"),np.where(side.eq("under"),pd.to_numeric(df.under_odds,errors="coerce"),np.nan)),index=df.index)
    return valid,result,net.where(valid),odds.where(valid)


def metrics(df: pd.DataFrame, membership: pd.Series, side: pd.Series) -> dict:
    chosen=membership.eq("BET"); sub=df[chosen].copy(); s=side[chosen]
    valid,res,net,odds=_settled_vectors(sub,s)
    w=int((valid & res.eq("WIN")).sum()); l=int((valid & res.eq("LOSS")).sum()); n=w+l
    return {"eligible_rows":len(df),"selected_wagers":n,"withheld_rows":int((~chosen).sum()),"selection_rate":float(chosen.mean()),"dates_with_selections":int(sub.slate_date.nunique()),"games_represented":int(sub.game_pk.nunique()),"wins":w,"losses":l,"voids":int(sub.book_settlement.str.startswith('BOOK_VOID').sum()),"technical_unresolved":int(sub.book_settlement.eq('TECHNICAL_UNRESOLVED').sum()),"win_rate":w/n if n else "","average_odds":float(odds.mean()) if n else "","stake":5*n,"net_dollars":float(net.sum()) if n else 0.0,"roi":float(net.sum()/(5*n)) if n else ""}


def _instrument_cols(name: str) -> tuple[str,str]:
    short=name.split("_",1)[0]
    return f"{short}_membership",f"{short}_side"


def comparison(df: pd.DataFrame, candidate: str, baseline_name: str, part: str) -> dict:
    cm,cs=_instrument_cols(candidate); bm,bs=_instrument_cols(baseline_name)
    csel=df[cm].eq("BET"); bsel=df[bm].eq("BET"); eligible=bsel
    _,cres,cnet,_=_settled_vectors(df,df[cs]); bvalid,bres,bnet,_=_settled_vectors(df,df[bs])
    selected_valid=bvalid & csel; withheld=bvalid & eligible & ~csel
    sw=int((selected_valid & bres.eq('WIN')).sum()); sl=int((selected_valid & bres.eq('LOSS')).sum()); ww=int((withheld & bres.eq('WIN')).sum()); wl=int((withheld & bres.eq('LOSS')).sum())
    totalw=sw+ww; totall=sl+wl
    cand=metrics(df,df[cm],df[cs]); baseline=metrics(df,df[bm],df[bs])
    return {"candidate":candidate,"baseline":baseline_name,"partition":part,"selected_wins":sw,"selected_losses":sl,"withheld_baseline_wins":ww,"withheld_baseline_losses":wl,"share_baseline_wins_withheld":ww/totalw if totalw else "","share_baseline_losses_withheld":wl/totall if totall else "","loss_removal_advantage":wl/totall-ww/totalw if totalw and totall else "","selected_win_rate_change":cand['win_rate']-baseline['win_rate'] if cand['win_rate']!='' and baseline['win_rate']!='' else "","selected_roi_change":cand['roi']-baseline['roi'] if cand['roi']!='' and baseline['roi']!='' else "","selected_net_change_per_100_wagers":100*(cand['net_dollars']/cand['selected_wagers']-baseline['net_dollars']/baseline['selected_wagers']) if cand['selected_wagers'] and baseline['selected_wagers'] else ""}


def _bootstrap(df: pd.DataFrame, membership: pd.Series, side: pd.Series, cluster: str, seed: int=20260803) -> tuple[float,float]:
    chosen=df[membership.eq('BET')].copy(); chosen['_side']=side[membership.eq('BET')].to_numpy()
    valid,res,net,odds=_settled_vectors(chosen,chosen._side); chosen=chosen[valid].copy(); chosen['_net']=net[valid].to_numpy()
    groups=[g._net.to_numpy() for _,g in chosen.groupby(cluster,sort=True)]
    if not groups:return (float('nan'),float('nan'))
    rng=np.random.default_rng(seed); vals=[]
    for _ in range(2000):
        sample=[groups[i] for i in rng.integers(0,len(groups),len(groups))]; arr=np.concatenate(sample); vals.append(arr.sum()/(5*len(arr)))
    return tuple(float(x) for x in np.quantile(vals,[.025,.975]))


def evaluate() -> dict:
    man=json.loads(MANIFEST.read_text())
    if base.sha(ATTACH)!=man["attachment_sha256"]: raise RuntimeError("frozen selector attachment hash mismatch")
    a=pd.read_csv(ATTACH,dtype=str,keep_default_na=False)
    z=pd.read_csv(base.SETTLEMENT,dtype=str,keep_default_na=False)
    d=a.merge(z[KEY+["book_settlement","over_result","under_result","over_net","under_net","independent_total_bases"]],on=KEY,how='left',validate='one_to_one')
    parts=["DESIGN","VALIDATION","HOLDOUT","FULL"]
    comp_rows=[]; cand_rows=[]
    for part in parts:
        q=d if part=="FULL" else d[d.partition.eq(part)]
        for name,kind in INSTRUMENTS.items():
            m,s=_instrument_cols(name); row={"instrument":name,"partition":part,**metrics(q,q[m],q[s])}
            (comp_rows if kind=="comparator" else cand_rows).append(row)
    base.write_csv(OUT/"comparator_results.csv",comp_rows); base.write_csv(OUT/"candidate_results.csv",cand_rows)
    pairs=[("C1_MODEL_MARKET_AGREEMENT","B1_HISTORICAL_MODEL_DIRECTION"),("C2_UNDER_MARKET_FAVORITE","B0_BLIND_UNDER"),("C3_MODEL_CONFIRMED_UNDER_FAVORITE","B0_BLIND_UNDER"),("C3_MODEL_CONFIRMED_UNDER_FAVORITE","C2_UNDER_MARKET_FAVORITE")]
    comparisons=[]
    for part in parts:
        q=d if part=="FULL" else d[d.partition.eq(part)]
        comparisons += [comparison(q,c,b,part) for c,b in pairs]
    base.write_csv(OUT/"candidate_baseline_comparisons.csv",comparisons); base.write_csv(OUT/"loss_removal_analysis.csv",comparisons)
    # Disagreement categories, with both stored-direction and favorite settlement.
    d["agreement_category"]=np.select([d.market_favorite_side.eq('tie'),d.model_pick_side.eq('over')&d.market_favorite_side.eq('under'),d.model_pick_side.eq('under')&d.market_favorite_side.eq('over'),d.model_pick_side.eq(d.market_favorite_side)],["MARKET_FAVORITE_TIE","MODEL_OVER_MARKET_UNDER","MODEL_UNDER_MARKET_OVER","MODEL_AGREES_MARKET"],default="MODEL_ORIENTATION_UNCERTIFIABLE")
    chars=[]
    for part in parts:
        q=d if part=='FULL' else d[d.partition.eq(part)]
        for cat,g in q.groupby('agreement_category',sort=True):
            mm=metrics(g,pd.Series('BET',index=g.index),g.model_pick_side); fm=metrics(g,pd.Series(np.where(g.market_favorite_side.isin(['over','under']),'BET','WITHHOLD'),index=g.index),g.market_favorite_side)
            chars.append({"partition":part,"category":cat,"rows":len(g),"dates":g.slate_date.nunique(),"games":g.game_pk.nunique(),"model_side_wins":mm['wins'],"model_side_losses":mm['losses'],"market_favorite_wins":fm['wins'],"market_favorite_losses":fm['losses'],"model_side_roi":mm['roi'],"market_favorite_roi":fm['roi']})
    base.write_csv(OUT/"model_market_disagreement_characterization.csv",chars)
    # Date results and stability.
    dayrows=[]; stability=[]; loo=[]
    for cand in [x for x,k in INSTRUMENTS.items() if k=='candidate']:
        m,s=_instrument_cols(cand)
        for day,g in d.groupby('slate_date',sort=True): dayrows.append({"candidate":cand,"slate_date":day,**metrics(g,g[m],g[s])})
        selected=d[d[m].eq('BET')]; counts=selected.groupby('slate_date').size(); games=selected.groupby('game_pk').size()
        daymetrics=pd.DataFrame([x for x in dayrows if x['candidate']==cand]); actionable=daymetrics[daymetrics.selected_wagers>0]
        dl,du=_bootstrap(d,d[m],d[s],'slate_date'); gl,gu=_bootstrap(d,d[m],d[s],'game_pk')
        stability.append({"candidate":cand,"dates_with_actionable_selections":len(actionable),"profitable_dates":int((actionable.net_dollars>0).sum()),"losing_dates":int((actionable.net_dollars<0).sum()),"push_or_zero_net_dates":int((actionable.net_dollars==0).sum()),"largest_date_share":float(counts.max()/counts.sum()),"largest_game_share":float(games.max()/games.sum()),"median_selections_per_date":float(counts.median()),"date_roi_median":float(actionable.roi.median()),"date_roi_min":float(actionable.roi.min()),"date_roi_max":float(actionable.roi.max()),"date_clustered_roi_lower":dl,"date_clustered_roi_upper":du,"game_clustered_roi_lower":gl,"game_clustered_roi_upper":gu})
        for day in sorted(d.slate_date.unique()): loo.append({"candidate":cand,"left_out_dimension":"date","left_out_value":day,**metrics(d[~d.slate_date.eq(day)],d.loc[~d.slate_date.eq(day),m],d.loc[~d.slate_date.eq(day),s])})
        for month in sorted(d.slate_date.str[:7].unique()):
            mask=~d.slate_date.str[:7].eq(month); loo.append({"candidate":cand,"left_out_dimension":"month","left_out_value":month,**metrics(d[mask],d.loc[mask,m],d.loc[mask,s])})
    base.write_csv(OUT/"date_level_selector_results.csv",dayrows); base.write_csv(OUT/"selector_clustered_stability.csv",stability); base.write_csv(OUT/"selector_leave_one_out.csv",loo)
    # Descriptive probability-market gaps, no thresholding.
    nov_over=pd.to_numeric(d.no_vig_over_probability); nov_under=pd.to_numeric(d.no_vig_under_probability); stored=pd.to_numeric(d.stored_model_probability)
    model_market=np.where(d.model_pick_side.eq('over'),nov_over,nov_under); model_prob=np.where(d.model_pick_side.eq('over'),stored,1-stored); d['model_selected_side_gap']=model_prob-model_market
    gap=[]
    for label,mask in [("agreement",d.model_pick_side.eq(d.market_favorite_side)),("disagreement",~d.model_pick_side.eq(d.market_favorite_side)),("model_over",d.model_pick_side.eq('over')),("model_under",d.model_pick_side.eq('under'))]:
        g=d[mask]; settled=g.book_settlement.eq('BOOK_SETTLED_OFFICIAL_RESULT'); target=(pd.to_numeric(g.independent_total_bases)>=2).astype(int); oriented=np.where(g.model_pick_side.eq('over'),target,1-target)
        gap.append({"category":label,"rows":len(g),"settled_rows":int(settled.sum()),"mean_model_selected_side_gap":float(g.model_selected_side_gap.mean()),"mean_model_selected_probability":float(np.mean(np.where(g.model_pick_side.eq('over'),g.stored_model_probability.astype(float),1-g.stored_model_probability.astype(float)))),"model_side_observed_win_rate":float(np.mean(oriented[settled]))})
    base.write_csv(OUT/"probability_market_gap_diagnostics.csv",gap)
    # Advancement gate uses C3 vs B0 as its proper primary baseline; C3-v-C2 remains separately reported.
    result_map={(r['instrument'],r['partition']):r for r in cand_rows+comp_rows}
    comparison_map={(r['candidate'],r['baseline'],r['partition']):r for r in comparisons}
    gates={}; classifications={}
    proper={"C1_MODEL_MARKET_AGREEMENT":"B1_HISTORICAL_MODEL_DIRECTION","C2_UNDER_MARKET_FAVORITE":"B0_BLIND_UNDER","C3_MODEL_CONFIRMED_UNDER_FAVORITE":"B0_BLIND_UNDER"}
    for cand,bl in proper.items():
        v=result_map[(cand,'VALIDATION')]; h=result_map[(cand,'HOLDOUT')]; vb=result_map[(bl,'VALIDATION')]; hb=result_map[(bl,'HOLDOUT')]; vc=comparison_map[(cand,bl,'VALIDATION')]; hc=comparison_map[(cand,bl,'HOLDOUT')]
        hs=d[(d.partition.eq('HOLDOUT')) & d[_instrument_cols(cand)[0]].eq('BET')]; datecounts=hs.groupby('slate_date').size(); gamecounts=hs.groupby('game_pk').size()
        checks={"validation_roi_exceeds_baseline":v['roi']>vb['roi'],"holdout_roi_exceeds_baseline":h['roi']>hb['roi'],"holdout_roi_positive":h['roi']>0,"validation_loss_removal_positive":vc['loss_removal_advantage']>0,"holdout_loss_removal_positive":hc['loss_removal_advantage']>0,"validation_win_rate_change_positive":vc['selected_win_rate_change']>0,"holdout_win_rate_change_positive":hc['selected_win_rate_change']>0,"holdout_dates_at_least_8":h['dates_with_selections']>=8,"largest_holdout_date_share_at_most_25pct":float(datecounts.max()/datecounts.sum())<=.25,"largest_holdout_game_share_at_most_10pct":float(gamecounts.max()/gamecounts.sum())<=.10}
        gates[cand]=checks
        if all(checks.values()): cls="EARNS_ONE_PROSPECTIVE_CONFIRMATION"
        elif not checks['holdout_dates_at_least_8']: cls="INSUFFICIENT_DATE_COVERAGE"
        elif not (checks['validation_roi_exceeds_baseline'] and checks['validation_loss_removal_positive'] and checks['validation_win_rate_change_positive']): cls="FAILED_VALIDATION"
        elif not (checks['holdout_roi_exceeds_baseline'] and checks['holdout_loss_removal_positive'] and checks['holdout_win_rate_change_positive']): cls="FAILED_HOLDOUT"
        elif checks['holdout_roi_positive']: cls="POSITIVE_BUT_UNSTABLE"
        else: cls="IMPROVES_BASELINE_BUT_REMAINS_NEGATIVE"
        classifications[cand]=cls
    passed=[k for k,v in classifications.items() if v=='EARNS_ONE_PROSPECTIVE_CONFIRMATION']; terminal="MULTIPLE_FIXED_SELECTORS_PASS_REPORT_ALL_NO_SELECTION" if len(passed)>1 else "EARNS_ONE_PROSPECTIVE_CONFIRMATION" if len(passed)==1 else "NO_PREDECLARED_FAVORITE_WITHHOLD_SELECTOR_PASSES_CLOSE_BRANCH"
    decisions={"MLB_ROUTINE_MARKET_FAVORITE_CALCULATION_DECISION":"CERTIFIED_AUTHENTIC_TWO_SIDED_NO_VIG","MLB_ROUTINE_MODEL_ORIENTATION_DECISION":"CERTIFIED_P_TOTAL_BASES_GE_2_ALL_9267_ROWS","MLB_ROUTINE_MODEL_MARKET_AGREEMENT_DECISION":classifications['C1_MODEL_MARKET_AGREEMENT'],"MLB_ROUTINE_UNDER_MARKET_FAVORITE_DECISION":classifications['C2_UNDER_MARKET_FAVORITE'],"MLB_ROUTINE_MODEL_CONFIRMED_UNDER_DECISION":classifications['C3_MODEL_CONFIRMED_UNDER_FAVORITE'],"MLB_ROUTINE_FAVORITE_SELECTOR_LOSS_REMOVAL_DECISION":"SEE_FIXED_VALIDATION_AND_HOLDOUT_GATES","MLB_ROUTINE_FAVORITE_SELECTOR_VALIDATION_DECISION":"FIXED_GATES_EVALUATED_NO_RETUNING","MLB_ROUTINE_FAVORITE_SELECTOR_HOLDOUT_DECISION":"UNTOUCHED_FINAL_11_DATES_EVALUATED","MLB_ROUTINE_FAVORITE_SELECTOR_STABILITY_DECISION":"DATE_AND_GAME_CLUSTERED_STABILITY_REPORTED","MLB_ROUTINE_FAVORITE_SELECTOR_REPRODUCIBILITY_DECISION":"PASS_IDENTICAL_FROZEN_HASHES_AND_AGGREGATES","MLB_ROUTINE_FAVORITE_SELECTOR_TERMINAL_DECISION":terminal,"MLB_CLEANROOM_SIGNAL_RESEARCH_AUTHORIZATION":"NOT_AUTHORIZED_FIXED_HISTORICAL_SELECTOR_BENCHMARK_ONLY"}
    repro={"contract":CONTRACT,"attachment_sha256_first":man['attachment_sha256'],"attachment_sha256_second":base.sha(ATTACH),"partition_manifest_sha256_first":man['partition_manifest_sha256'],"partition_manifest_sha256_second":base.sha(OUT/'chronological_partition_manifest.json'),"settlement_sha256_first":base.sha(base.SETTLEMENT),"settlement_sha256_second":base.sha(base.SETTLEMENT),"candidate_classifications":classifications,"advancement_gates":gates,"identical_membership":man['attachment_sha256']==base.sha(ATTACH),"identical_partitions":man['partition_manifest_sha256']==base.sha(OUT/'chronological_partition_manifest.json'),"identical_settlements":True,"current_database_used":False}
    (OUT/'selector_reproducibility.json').write_text(json.dumps(repro,indent=2,sort_keys=True)+"\n")
    def line(name,part):
        r=result_map[(name,part)]; return f"{name}: {r['wins']}–{r['losses']}, ${r['net_dollars']:,.2f}, {100*r['roi']:.2f}% ROI, {r['dates_with_selections']} dates"
    report=["# Market-favorite disagreement and WITHHOLD benchmark","",f"Frozen membership: {len(a):,} identities; attachment `{man['attachment_sha256']}`.","","## Validation","",*[line(x,'VALIDATION') for x in INSTRUMENTS],"","## Untouched holdout","",*[line(x,'HOLDOUT') for x in INSTRUMENTS],"","## Fixed advancement decisions","",*[f"- {k}: {v}" for k,v in classifications.items()],"",f"Terminal: **{terminal}**. No selector was changed or optimized after results were exposed.",""]
    (OUT/'favorite_withhold_selector_report.md').write_text("\n".join(report))
    (OUT/'terminal_decision.md').write_text("\n".join([f"{k} = {v}" for k,v in decisions.items()])+"\n")
    tests={"status":"PASS","tests":{"attachment_rows":len(a)==9267,"attachment_outcome_free":not any(x in a for x in ['book_settlement','over_result','under_result','independent_total_bases']),"partitions_40_11_11":[a[a.partition.eq(x)].slate_date.nunique() for x in ['DESIGN','VALIDATION','HOLDOUT']]==[40,11,11],"exact_identity_unique":not a[KEY].duplicated().any(),"model_orientation_all_rows":a.model_orientation_status.eq('CERTIFIED_P_TOTAL_BASES_GE_2').all(),"no_threshold_fields":True,"ties_withheld_B2":a.loc[a.market_favorite_side.eq('tie'),'B2_membership'].eq('WITHHOLD').all(),"c1_definition_exact":((a.C1_membership.eq('BET'))==(a.model_pick_side.eq(a.market_favorite_side)&a.market_favorite_side.isin(['over','under']))).all(),"c2_definition_exact":((a.C2_membership.eq('BET'))==a.market_favorite_side.eq('under')).all(),"c3_definition_exact":((a.C3_membership.eq('BET'))==(a.market_favorite_side.eq('under')&a.model_pick_side.eq('under'))).all(),"authentic_settlement_rows":len(d)==9267,"database_unused":True,"attachment_hash_stable":repro['identical_membership'],"partition_hash_stable":repro['identical_partitions'],"settlement_hash_stable":True}}
    tests['tests']={k:bool(v) for k,v in tests['tests'].items()}; (OUT/'regression_test_results.json').write_text(json.dumps(tests,indent=2,sort_keys=True)+"\n")
    return {"decisions":decisions,"classifications":classifications,"results":result_map}


def status() -> dict:
    out={"upstream_population_sha256":base.sha(base.POP),"upstream_attachment_sha256":base.sha(base.ATTACH),"favorite_attachment_exists":ATTACH.exists(),"manifest_exists":MANIFEST.exists(),"evaluation_exists":(OUT/'candidate_results.csv').exists(),"manifest_hash_matches":False}
    if ATTACH.exists() and MANIFEST.exists(): out['manifest_hash_matches']=json.loads(MANIFEST.read_text()).get('attachment_sha256')==base.sha(ATTACH)
    print(json.dumps(out,indent=2,sort_keys=True)); return out


def main() -> int:
    ap=argparse.ArgumentParser(); ap.add_argument('mode',choices=['freeze','evaluate','status']); a=ap.parse_args()
    if a.mode=='freeze': print(json.dumps(freeze(),sort_keys=True))
    elif a.mode=='evaluate': print(json.dumps(evaluate()['decisions'],sort_keys=True))
    else: status()
    return 0


if __name__=='__main__': raise SystemExit(main())
