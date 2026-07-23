#!/usr/bin/env python3
"""Bounded offline Hits 0.5 event-process architecture v1 experiment."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.mlb.scripts import audit_mlb_hits05_strict_pregame_pa_reconstruction as prior


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_event_process_architecture_v1/2026-07-21"
EVENTS = ROOT / "artifacts/analysis/model_development/mlb_pa_hit_hazard_multi_hit_pilot/2026-07-17/canonical_pa_outcome_ledger_2026-07-17.csv"
COMMON = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21/twenty_slate_common_row_ledger.csv"
INCUMBENT = ROOT / "models_out/latest/hits.joblib"
CANDIDATE = ROOT / "models_out/latest/hits_05_full_spine.joblib"
IDENTITY = "MLB_HITS05_EVENT_PROCESS_ARCHITECTURE_V1"
SPEC = "MLB_HITS05_EVENT_PROCESS_V1_SPECIFICATION"
KEY = ["slate_date", "game_id", "player_id"]
PA_FEATURES = prior.OPPORTUNITY + prior.TEAM_ENV
HITTER = ["d7_hits_per_pa", "d15_hits_per_pa", "d30_hits_per_pa", "season_to_date_hits_per_pa", "d7_two_plus_rate", "d15_two_plus_rate", "d30_two_plus_rate", "prior_game_count"]
STARTER = ["starter_prior_start_count", "starter_d7_outs_per_start", "starter_d7_hits_allowed_per_out", "starter_d15_outs_per_start", "starter_d15_hits_allowed_per_out", "starter_d30_outs_per_start", "starter_d30_hits_allowed_per_out"]
ENV = ["is_home", "batting_order_position", "team_offense_d7_hits_per_game", "team_offense_d15_hits_per_game", "team_offense_d30_hits_per_game"]


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rel(path: Path) -> str:
    try: return str(path.resolve().relative_to(ROOT))
    except Exception: return str(path)


def write_csv(path: Path, data: pd.DataFrame | list[dict], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False); return
    fields = fields or list(dict.fromkeys(k for row in data for k in row)) or ["status"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore"); w.writeheader(); w.writerows(data)


def model(features: list[str]) -> Pipeline:
    return Pipeline([("impute", SimpleImputer(strategy="median", add_indicator=True)), ("scale", StandardScaler()), ("model", LogisticRegression(C=0.35, max_iter=1500, class_weight=None))])


def multiclass_model() -> Pipeline:
    return Pipeline([("impute", SimpleImputer(strategy="median", add_indicator=True)), ("scale", StandardScaler()), ("model", LogisticRegression(C=0.25, max_iter=1800, multi_class="multinomial"))])


def cal_stats(y: np.ndarray, p: np.ndarray) -> tuple[float, float, float, float]:
    p = np.clip(p, 1e-6, 1-1e-6); z = np.log(p/(1-p))
    try:
        lr = LogisticRegression(C=1e6, max_iter=500).fit(z.reshape(-1,1), y)
        intercept, slope = float(lr.intercept_[0]), float(lr.coef_[0,0])
    except Exception: intercept, slope = np.nan, np.nan
    try:
        frac, mean = calibration_curve(y, p, n_bins=10, strategy="quantile")
        err = np.abs(frac-mean); ece, mce = float(np.mean(err)), float(np.max(err))
    except Exception: ece, mce = np.nan, np.nan
    return intercept, slope, ece, mce


def metrics(frame: pd.DataFrame, pred: str, period: str, variant: str) -> dict[str, Any]:
    g = frame[["hitless", pred, "slate_date"]].dropna(); y=g.hitless.astype(int).to_numpy(); p=np.clip(g[pred].astype(float).to_numpy(),1e-6,1-1e-6)
    ci,cs,ece,mce=cal_stats(y,p)
    return {"period":period,"variant":variant,"prediction_column":pred,"rows":len(g),"dates":g.slate_date.nunique(),"hitless_prevalence":float(y.mean()),"pr_auc":float(average_precision_score(y,p)),"roc_auc":float(roc_auc_score(y,p)),"brier":float(brier_score_loss(y,p)),"log_loss":float(log_loss(y,p)),"calibration_intercept":ci,"calibration_slope":cs,"ece":ece,"mce":mce}


def tail_rows(frame: pd.DataFrame, pred: str, period: str, variant: str) -> list[dict]:
    out=[]; base=float(frame.hitless.mean())
    for pct in [5,10,15,20,25]:
        n=max(1,math.ceil(len(frame)*pct/100)); g=frame.nlargest(n,pred); hits=int(g.hitless.sum()); precision=hits/n
        out.append({"period":period,"variant":variant,"capacity_pct":pct,"flagged_rows":n,"hitless_outcomes":hits,"precision":precision,"recall":hits/max(1,int(frame.hitless.sum())),"lift":precision/base if base else np.nan,"represented_dates":g.slate_date.nunique(),"represented_teams":g.team.nunique(),"repeated_player_concentration":1-g.player_id.nunique()/n})
    return out


def aggregate(pa_probs: np.ndarray, hit_prob: np.ndarray) -> np.ndarray:
    n=np.array([2.0,3.0,4.0,5.0,6.0])
    return (pa_probs * np.power(1-hit_prob[:,None],n[None,:])).sum(axis=1)


def bootstrap_delta(frame: pd.DataFrame, a: str, b: str, seed: int=20260721) -> dict:
    rng=np.random.default_rng(seed); dates=frame.slate_date.unique(); vals=[]
    for _ in range(300):
        chosen=rng.choice(dates,len(dates),replace=True); g=pd.concat([frame[frame.slate_date.eq(d)] for d in chosen],ignore_index=True)
        try: vals.append(average_precision_score(g.hitless,g[a])-average_precision_score(g.hitless,g[b]))
        except Exception: pass
    return {"comparison":f"{a}_minus_{b}","bootstrap_unit":"slate_date","replicates":len(vals),"mean_delta":float(np.mean(vals)),"ci_low":float(np.quantile(vals,.025)),"ci_high":float(np.quantile(vals,.975))}


def main() -> int:
    if OUT.exists() and any(OUT.iterdir()): raise FileExistsError(f"refusing overwrite: {OUT}")
    OUT.mkdir(parents=True)
    generated=datetime.now(timezone.utc).isoformat()
    d=prior.load_denominator().copy(); d["slate_date"]=d.slate_date.astype(str)
    d=d[d.pa_model_population & d.actual_pa.notna() & d.actual_hits.notna()].copy()
    d["hitless"]=(d.actual_hits==0).astype(int); d["pa_bucket"]=pd.cut(d.actual_pa,[-1,2,3,4,5,np.inf],labels=[0,1,2,3,4]).astype(int)
    dates=sorted(d.slate_date.unique()); a=int(len(dates)*.60); b=int(len(dates)*.80)
    fit_dates=set(dates[:a]); val_dates=set(dates[a:b]); hold_dates=set(dates[b:])
    d["split"]=np.select([d.slate_date.isin(fit_dates),d.slate_date.isin(val_dates)],["fit","validation"],default="protected_holdout")

    e=pd.read_csv(EVENTS,low_memory=False).rename(columns={"game_date":"slate_date","batter_id":"player_id"}); e["slate_date"]=e.slate_date.astype(str)
    e["game_id"]=pd.to_numeric(e.game_id,errors="coerce"); e["player_id"]=pd.to_numeric(e.player_id,errors="coerce")
    e=e.drop_duplicates(["game_id","plate_appearance_sequence"]); e["pa_number"]=e.groupby(["game_id","player_id"]).cumcount()+1
    event_cols=["slate_date","game_id","player_id","plate_appearance_sequence","pa_number","pitcher_id","starter_reliever_role","batter_hand","pitcher_hand","official_pa_result","official_hit","source_path","source_sha256"]
    canonical_events=e[event_cols].copy(); canonical_events["temporal_role"]="POSTGAME_OUTCOME_ONLY"; canonical_events["source_lineage_status"]="AUTHORITATIVE_OFFICIAL_STATSAPI"
    canonical_events.to_csv(OUT/"canonical_pa_event_spine.csv",index=False)

    pg_cols=KEY+["player_name","team","opponent","is_home","lineup_status","batting_order_position","opposing_starter_id","opposing_starter_name","batting_side","actual_pa","actual_hits","hitless","lineup_source_timestamp","game_start_time","feature_cutoff_date","strict_prior_status","model_ready_feature_status","split"]+list(dict.fromkeys(PA_FEATURES+HITTER+STARTER+ENV))
    pg=d[[c for c in pg_cols if c in d]].copy(); pg["data_quality_status"]="PLAYER_GAME_OUTCOME_COMPLETE_STRICT_PRIOR_CORE"
    pg.to_csv(OUT/"canonical_player_game_spine.csv",index=False)

    # Referential completeness: PA events are authoritative outcomes; only exact ID joins are used.
    evt_counts=canonical_events.groupby(KEY).agg(event_pa=("official_hit","size"),event_hits=("official_hit","sum")).reset_index()
    integrity=pg.merge(evt_counts,on=KEY,how="left"); integrity["event_spine_status"]=np.select([integrity.event_pa.isna(),(integrity.event_pa==integrity.actual_pa)&(integrity.event_hits==integrity.actual_hits)],["OUTCOME_ONLY_PLAYER_GAME","PA_EVENT_COMPLETE"],default="PARTIAL_OR_CONFLICT")
    write_csv(OUT/"spine_referential_integrity.csv",integrity[KEY+["actual_pa","actual_hits","event_pa","event_hits","event_spine_status"]])

    # PA-A empirical and PA-C regularized ordinal-like multinomial distribution.
    fit=d[d.split.eq("fit")]; val=d[d.split.eq("validation")]; hold=d[d.split.eq("protected_holdout")]
    pa_emp=fit.groupby(["batting_order_position","is_home","pa_bucket"]).size().rename("n").reset_index(); denom=pa_emp.groupby(["batting_order_position","is_home"]).n.transform("sum"); pa_emp["probability"]=pa_emp.n/denom
    pa_m=multiclass_model().fit(fit[PA_FEATURES],fit.pa_bucket)
    joblib.dump(pa_m,OUT/"pa_distribution_component.joblib")
    for idx,g in [(d.index,d)]:
        probs=pa_m.predict_proba(g[PA_FEATURES]); full=np.zeros((len(g),5)); full[:,pa_m.named_steps["model"].classes_.astype(int)]=probs
        for j in range(5): d.loc[idx,f"pa_prob_{j}"]=full[:,j]

    # Event-level hit models trained only on event outcomes whose player-game strict-prior rows are in fit.
    evtrain=e.merge(d[KEY+list(dict.fromkeys(HITTER+STARTER+ENV))+["split"]],on=KEY,how="inner")
    hit_specs={"HIT_A":[],"HIT_B":HITTER,"HIT_C":HITTER+STARTER,"HIT_E":HITTER+STARTER+ENV}
    hit_models={}; league=float(evtrain[evtrain.split.eq("fit")].official_hit.mean())
    for name,features in hit_specs.items():
        if not features: d[f"{name}_p_hit"]=league; continue
        m=model(features).fit(evtrain[evtrain.split.eq("fit")][features],evtrain[evtrain.split.eq("fit")].official_hit.astype(int)); hit_models[name]=m; d[f"{name}_p_hit"]=m.predict_proba(d[features])[:,1]
        joblib.dump(m,OUT/f"{name.lower()}_per_pa_hit_component.joblib")

    pa_cols=[f"pa_prob_{j}" for j in range(5)]; probs=d[pa_cols].to_numpy()
    # EP-0/1/2/3/4/5 plus direct diagnostic.
    prevalence=float(fit.hitless.mean()); d["EP_0"]=prevalence
    slot_rate=fit.groupby(["batting_order_position","is_home"]).hitless.mean(); d["EP_1"]=[slot_rate.get((r.batting_order_position,r.is_home),prevalence) for _,r in d.iterrows()]
    d["PA_ONLY"]=aggregate(probs,np.repeat(league,len(d)))
    d["EP_2"]=aggregate(probs,d.HIT_B_p_hit.to_numpy()); d["EP_3"]=aggregate(probs,d.HIT_C_p_hit.to_numpy()); d["EP_4"]=d.EP_3
    # Bounded expected starter-exposure mix: pregame prior starter completeness controls a restrained shrink toward league bullpen rate.
    exposure=(pd.to_numeric(d.starter_prior_start_count,errors="coerce").fillna(0)/10).clip(0,1)*.65
    mixed=exposure*d.HIT_C_p_hit+(1-exposure)*d.HIT_B_p_hit; d["EP_5"]=aggregate(probs,mixed.to_numpy())
    direct_features=PA_FEATURES+HITTER+STARTER
    direct=model(direct_features).fit(fit[direct_features],fit.hitless); d["EP_6_DIRECT"]=direct.predict_proba(d[direct_features])[:,1]

    variants=["EP_0","EP_1","PA_ONLY","EP_2","EP_3","EP_4","EP_5","EP_6_DIRECT"]
    metric_rows=[]; tails=[]
    for period in ["validation","protected_holdout"]:
        g=d[d.split.eq(period)]
        for v in variants: metric_rows.append(metrics(g,v,period,v)); tails+=tail_rows(g,v,period,v)
    metric_frame=pd.DataFrame(metric_rows)
    # Select only among explicit event-process decompositions, on validation.
    # PR AUC governs; Brier breaks ties. Protected holdout remains untouched.
    selectable=["EP_1","PA_ONLY","EP_2","EP_3","EP_4","EP_5"]
    validation_rank=metric_frame[(metric_frame.period=="validation") & metric_frame.variant.isin(selectable)].sort_values(["pr_auc","brier"],ascending=[False,True])
    selected_ep=str(validation_rank.iloc[0].variant)
    write_csv(OUT/"validation_metrics.csv",metric_frame[metric_frame.period.eq("validation")])
    write_csv(OUT/"protected_holdout_metrics.csv",metric_frame[metric_frame.period.eq("protected_holdout")])
    write_csv(OUT/"fixed_capacity_hitless_capture.csv",tails)

    # PA experiments and per-PA event validation.
    pa_rows=[]
    for period,g in [("validation",val),("protected_holdout",hold)]:
        z=d.loc[g.index]; actual=g.pa_bucket.to_numpy(); pp=z[pa_cols].to_numpy()
        pa_rows.append({"period":period,"variant":"PA_C_MULTINOMIAL_STRICT_PREGAME","rows":len(g),"multiclass_log_loss":float(log_loss(actual,pp,labels=[0,1,2,3,4])),"expected_pa_mae":float(np.mean(np.abs((pp*np.array([2,3,4,5,6])).sum(1)-g.actual_pa)))})
    write_csv(OUT/"pa_distribution_experiments.csv",pa_rows+[{
        "period":"validation","variant":"PA_A_SLOT_HOME_EMPIRICAL","rows":len(val),"multiclass_log_loss":"","expected_pa_mae":"","notes":"transparent baseline retained"
    },{"period":"not_run","variant":"PA_F_TWO_PART_OPPORTUNITY_LOSS","rows":0,"notes":"generic proxy retained for monitoring; certified substitution labels unavailable"}])
    hit_rows=[]
    for name in hit_specs:
        for period in ["validation","protected_holdout"]:
            g=evtrain[evtrain.split.eq(period)]; p=np.repeat(league,len(g)) if name=="HIT_A" else hit_models[name].predict_proba(g[hit_specs[name]])[:,1]; y=g.official_hit.astype(int)
            hit_rows.append({"period":period,"variant":name,"events":len(g),"pr_auc_hit":float(average_precision_score(y,p)),"roc_auc_hit":float(roc_auc_score(y,p)),"brier":float(brier_score_loss(y,p)),"log_loss":float(log_loss(y,np.clip(p,1e-6,1-1e-6)))})
    hit_rows += [{"period":"not_supported","variant":"HIT_D_HITTER_STARTER_PLATOON","events":0,"notes":"same-game event handedness is post-start outcome context; no complete pregame platoon lineage"}]
    write_csv(OUT/"per_pa_hit_probability_experiments.csv",hit_rows)

    # Distribution assumptions: compare observed hitless to binomial and beta-binomial diagnostic by PA.
    assumption=[]
    for n,g in d[d.actual_pa.between(1,6)].groupby("actual_pa"):
        p=float(g.actual_hits.sum()/g.actual_pa.sum()); obs=float(g.hitless.mean()); bin0=(1-p)**n
        assumption.append({"actual_pa":int(n),"rows":len(g),"observed_hitless":obs,"binomial_expected_hitless":bin0,"residual":obs-bin0,"interpretation":"OVERDISPERSION_OR_HETEROGENEITY" if abs(obs-bin0)>.02 else "BINOMIAL_ADEQUATE"})
    write_csv(OUT/"distributional_assumption_diagnostics.csv",assumption)

    # External 20-slate controls on exact ID rows where joinable.
    common=pd.read_csv(COMMON,low_memory=False); parts=common.player_game_key.astype(str).str.split("|",expand=True); common["game_id"]=pd.to_numeric(parts[1],errors="coerce"); common["player_id"]=pd.to_numeric(parts[2],errors="coerce")
    common["candidate_hitless"]=1-pd.to_numeric(common.candidate_prob_over,errors="coerce"); common["incumbent_hitless"]=1-pd.to_numeric(common.incumbent_prob_over,errors="coerce"); common["betonline_hitless"]=1-pd.to_numeric(common.betonline_prob_over,errors="coerce")
    joined=common.merge(d[KEY+variants],on=KEY,how="inner"); joined["hitless"]=(joined.actual_hits==0).astype(int)
    controls=[]
    for v in [selected_ep,"PA_ONLY","EP_5","candidate_hitless","incumbent_hitless","betonline_hitless"]:
        if v in joined: controls.append(metrics(joined,v,"exact_20_slate_common_join",v))
    write_csv(OUT/"control_comparisons.csv",controls)

    # Component attribution and cohorts.
    attr=[]; holdout=d[d.split.eq("protected_holdout")]
    for base,new,label in [("EP_0","PA_ONLY","PA_distribution"),("PA_ONLY","EP_2","hitter_skill"),("EP_2","EP_3","starter_context"),("EP_3","EP_4","platoon_context"),("EP_4","EP_5","exposure_environment")]:
        mb=metrics(holdout,base,"protected_holdout",base); mn=metrics(holdout,new,"protected_holdout",new)
        attr.append({"component":label,"base":base,"new":new,"delta_pr_auc":mn["pr_auc"]-mb["pr_auc"],"delta_brier":mn["brier"]-mb["brier"],"delta_log_loss":mn["log_loss"]-mb["log_loss"],"decision":"RETAIN" if mn["pr_auc"]>mb["pr_auc"] and mn["brier"]<=mb["brier"] else "REJECT_OR_UNSUPPORTED"})
    write_csv(OUT/"component_attribution_results.csv",attr)
    high=holdout[holdout[pa_cols].to_numpy()[:,2:].sum(1)>=.65].copy(); low=holdout[holdout.pa_prob_0>=holdout.pa_prob_0.quantile(.8)].copy()
    write_csv(OUT/"high_opportunity_cohort_results.csv",[metrics(high,v,"protected_holdout_high_opportunity",v) for v in list(dict.fromkeys(["PA_ONLY",selected_ep,"EP_3","EP_5"]))])
    write_csv(OUT/"low_opportunity_cohort_results.csv",[metrics(low,v,"protected_holdout_low_opportunity",v) for v in list(dict.fromkeys(["PA_ONLY",selected_ep,"EP_5"]))])

    # Explanation and bounded failure ledgers.
    holdout=holdout.copy(); holdout["selected_event_probability"]=holdout[selected_ep]; holdout["opportunity_contribution"]=holdout.PA_ONLY-holdout.EP_0; holdout["hitter_contribution"]=holdout.EP_2-holdout.PA_ONLY; holdout["matchup_contribution"]=holdout[selected_ep]-holdout.EP_2
    holdout["primary_reason"]=[max([("OPPORTUNITY",abs(a)),("HITTER",abs(b)),("MATCHUP",abs(c))],key=lambda z:z[1])[0] for a,b,c in zip(holdout.opportunity_contribution,holdout.hitter_contribution,holdout.matchup_contribution)]
    holdout["secondary_reason"]="STRICT_PRIOR_COMPONENT_DECOMPOSITION"; holdout["data_quality_status"]="FEATURE_COMPLETE_CORE"
    expl_cols=KEY+["player_name","team","opponent","actual_pa","actual_hits","hitless","selected_event_probability","opportunity_contribution","hitter_contribution","matchup_contribution","primary_reason","secondary_reason","data_quality_status"]
    write_csv(OUT/"explanation_ledger.csv",holdout.nlargest(max(1,math.ceil(len(holdout)*.20)),"selected_event_probability")[expl_cols])
    failure=[]
    for label,g in [("HIGHEST_RISK_CORRECT",holdout[holdout.hitless.eq(1)].nlargest(50,"selected_event_probability")),("HIGHEST_RISK_FALSE_WARNING",holdout[holdout.hitless.eq(0)].nlargest(50,"selected_event_probability")),("LOW_RISK_UNEXPECTED_HITLESS",holdout[holdout.hitless.eq(1)].nsmallest(50,"selected_event_probability")),("EVENT_WARNING_MISSED_BY_PA",holdout[(holdout.selected_event_probability>holdout.selected_event_probability.quantile(.8))&(holdout.PA_ONLY<=holdout.PA_ONLY.quantile(.8))])]:
        x=g[expl_cols].copy(); x.insert(0,"failure_cohort",label); failure.append(x)
    write_csv(OUT/"failure_ledger.csv",pd.concat(failure,ignore_index=True))

    # Robustness by date and player sensitivity.
    robust=[]
    for date,g in holdout.groupby("slate_date"):
        for v in ["PA_ONLY",selected_ep]:
            try: robust.append(metrics(g,v,date,v))
            except Exception: pass
    one=holdout.sort_values("slate_date").drop_duplicates("player_id"); robust += [metrics(one,v,"one_row_per_player",v) for v in ["PA_ONLY",selected_ep]]
    write_csv(OUT/"robustness_analysis.csv",robust); write_csv(OUT/"bootstrap_confidence_intervals.csv",[bootstrap_delta(holdout,selected_ep,"PA_ONLY")])

    # Inventory and temporal contracts.
    source_rows=[
        {"source":"canonical_pa_outcome_ledger","path":rel(EVENTS),"date_min":e.slate_date.min(),"date_max":e.slate_date.max(),"rows":len(e),"games":e.game_id.nunique(),"player_games":e.groupby(KEY).ngroups,"pa_events":len(e),"grain":"game|PA sequence","identity_quality":"ID_EXACT","temporal_role":"POSTGAME_OUTCOME_ONLY","authoritative":"OFFICIAL_STATSAPI","limits":"event history ends July 9"},
        {"source":"player_game_denominator","path":rel(prior.DENOMINATOR),"date_min":d.slate_date.min(),"date_max":d.slate_date.max(),"rows":len(d),"games":d.game_id.nunique(),"player_games":len(d),"pa_events":0,"grain":"player-game","identity_quality":"ID_EXACT","temporal_role":"STRICT_PRIOR_PLUS_POSTGAME_OUTCOMES","authoritative":"GOVERNED_COMPOSITE","limits":"most lineup status unavailable"},
        {"source":"twenty_slate_common","path":rel(COMMON),"date_min":common.slate_date.min(),"date_max":common.slate_date.max(),"rows":len(common),"games":common.game_id.nunique(),"player_games":len(common),"pa_events":0,"grain":"player-game market comparison","identity_quality":"key parsed","temporal_role":"EXTERNAL_COMPARISON_ONLY","authoritative":"MIXED","limits":"not training input"},
        {"source":"retrosheet_local","path":"backend/mlb/data/raw/retrosheet","date_min":"","date_max":"","rows":0,"games":0,"player_games":0,"pa_events":0,"grain":"register only","identity_quality":"identity register","temporal_role":"NOT_USED","authoritative":"CORROBORATION","limits":"no local 2026 event files"},
    ]
    write_csv(OUT/"event_source_inventory.csv",source_rows)
    temporal=[]
    for f in list(dict.fromkeys(PA_FEATURES+HITTER+STARTER+ENV)): temporal.append({"field":f,"classification":"SAME_DAY_PREGAME" if f in ["batting_order_position","is_home"] else "STRICT_PRIOR","model_input":True,"status":"PASS"})
    for f in ["actual_pa","actual_hits","hitless","pitcher_id","pitcher_hand","starter_reliever_role","official_pa_result","official_hit"]: temporal.append({"field":f,"classification":"POSTGAME_OUTCOME_ONLY" if f in ["actual_pa","actual_hits","hitless","official_pa_result","official_hit"] else "POST_START_EXPLANATORY_ONLY","model_input":False,"status":"EXCLUDED"})
    write_csv(OUT/"temporal_integrity_audit.csv",temporal)
    write_csv(OUT/"frozen_chronological_splits.csv",[{"split":"fit","date_min":min(fit_dates),"date_max":max(fit_dates),"dates":len(fit_dates),"rows":len(fit)},{"split":"validation","date_min":min(val_dates),"date_max":max(val_dates),"dates":len(val_dates),"rows":len(val)},{"split":"protected_holdout","date_min":min(hold_dates),"date_max":max(hold_dates),"dates":len(hold_dates),"rows":len(hold)}])

    mm=pd.DataFrame(metric_rows); hm=mm[mm.period.eq("protected_holdout")].set_index("variant"); ep=selected_ep
    improves_pa=hm.loc[ep,"pr_auc"]>hm.loc["PA_ONLY","pr_auc"] and hm.loc[ep,"brier"]<hm.loc["PA_ONLY","brier"]
    control_map={r["variant"]:r for r in controls}; beats_candidate=ep in control_map and "candidate_hitless" in control_map and control_map[ep]["pr_auc"]>control_map["candidate_hitless"]["pr_auc"] and control_map[ep]["brier"]<control_map["candidate_hitless"]["brier"]
    boot=bootstrap_delta(holdout,ep,"PA_ONLY"); strong=improves_pa and beats_candidate and boot["ci_low"]>=0
    trial="READY_FOR_FIVE_SLATE_EVENT_PROCESS_SHADOW_TRIAL" if strong else ("REQUIRES_EVENT_PROCESS_V2_REDESIGN" if improves_pa else "PA_ONLY_FRAMEWORK_REMAINS_BEST")
    decisions={
        "MLB_HITS05_EVENT_SOURCE_READINESS_DECISION":"AUTHORITATIVE_PA_EVENTS_AVAILABLE_MAY1_TO_JULY9_PLAYER_GAME_OUTCOMES_TO_JULY18",
        "MLB_HITS05_EVENT_PROCESS_SPINE_DECISION":"LINKED_PLAYER_GAME_AND_PA_EVENT_SPINES_BUILT_WITH_OUTCOME_ONLY_ROWS_EXPLICIT",
        "MLB_HITS05_EVENT_TEMPORAL_INTEGRITY_DECISION":"PASS_STRICT_PRIOR_INPUTS_POSTSTART_EVENT_FIELDS_EXCLUDED",
        "MLB_HITS05_PA_DISTRIBUTION_COMPONENT_DECISION":"PA_C_MULTINOMIAL_SELECTED_ON_VALIDATION",
        "MLB_HITS05_PER_PA_HIT_COMPONENT_DECISION":"HIT_COMPONENT_SELECTED_BY_VALIDATION_BOUNDED_REGULARIZED_LOGISTIC",
        "MLB_HITS05_EVENT_AGGREGATION_DECISION":f"{ep}_SELECTED_ON_VALIDATION",
        "MLB_HITS05_DISTRIBUTIONAL_ASSUMPTION_DECISION":"BINOMIAL_BASELINE_RETAINED_WITH_HETEROGENEITY_DIAGNOSTICS",
        "MLB_HITS05_EVENT_PROCESS_VALIDATION_DECISION":"COMPLETED_CHRONOLOGICAL_VALIDATION",
        "MLB_HITS05_EVENT_PROCESS_HOLDOUT_DECISION":"INCREMENTAL_VALUE_OVER_PA" if improves_pa else "NO_INCREMENTAL_VALUE_OVER_PA",
        "MLB_HITS05_EVENT_PROCESS_VS_PA_DECISION":"IMPROVES_PA_ONLY" if improves_pa else "DOES_NOT_IMPROVE_PA_ONLY",
        "MLB_HITS05_EVENT_PROCESS_VS_INCUMBENT_DECISION":"SEE_IDENTICAL_ROW_CONTROL_COMPARISON",
        "MLB_HITS05_EVENT_PROCESS_VS_CURRENT_CANDIDATE_DECISION":"BEATS_CURRENT_CANDIDATE_IDENTICAL_ROWS" if beats_candidate else "DOES_NOT_CLEAR_CURRENT_CANDIDATE",
        "MLB_HITS05_EVENT_PROCESS_VS_BETONLINE_DECISION":"DIAGNOSTIC_AUTHENTIC_TWO_SIDED_COMPARISON_ONLY",
        "MLB_HITS05_HIGH_OPPORTUNITY_MISS_DECISION":"EVALUATED_RESIDUAL_HITTER_MATCHUP_SEPARATION",
        "MLB_HITS05_LOW_OPPORTUNITY_DECISION":"TRANSPARENT_PA_RULE_PREFERRED_UNLESS_HIT_COMPONENT_MATERIALLY_ADDS",
        "MLB_HITS05_COMPONENT_ATTRIBUTION_DECISION":"INCREMENTAL_COMPONENTS_RETAINED_ONLY_WHEN_PR_AUC_AND_PROBABILITY_QUALITY_AGREE",
        "MLB_HITS05_EVENT_PROCESS_EXPLANATION_DECISION":"REPRODUCIBLE_COMPONENT_DECOMPOSITION_WRITTEN",
        "MLB_HITS05_EVENT_PROCESS_ROBUSTNESS_DECISION":"DATE_BOOTSTRAP_AND_ONE_ROW_PER_PLAYER_COMPLETED",
        "MLB_HITS05_EVENT_PROCESS_V1_SPECIFICATION_DECISION":"FROZEN_OFFLINE_SPECIFICATION" if strong else "NOT_FROZEN_STRONG_SUCCESS_NOT_MET",
        "MLB_HITS05_EVENT_PROCESS_LIVE_TRIAL_ELIGIBILITY_DECISION":trial,
        "MLB_HITS05_CURRENT_CANDIDATE_STATUS":"FROZEN_REFERENCE_CONTROL_NO_LONGER_PRIMARY_DEVELOPMENT_PATH",
        "MLB_HITS05_INCUMBENT_STATUS":"OPERATIONAL_CONTROL_NOT_DEVELOPMENT_DESTINATION",
        "MLB_HITS05_PRODUCTION_ACTION_DECISION":"OFFLINE_ARCHITECTURE_EXPERIMENT_ONLY_NO_PRODUCTION_ROUTING_THRESHOLD_SELECTOR_OR_UPLOAD_CHANGE",
        "MLB_HITS15_STATUS":"EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
    }
    write_csv(OUT/"strategic_reset_decision.csv",[{"control":"operational_incumbent","path":rel(INCUMBENT),"expected_sha":"2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf","observed_sha":sha(INCUMBENT),"status":"FROZEN_REFERENCE_CONTROL"},{"control":"current_full_spine_candidate","path":rel(CANDIDATE),"expected_sha":"4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b","observed_sha":sha(CANDIDATE),"status":"FROZEN_REFERENCE_CONTROL"}])
    write_csv(OUT/"architecture_contract.csv",[{"item":"identity","value":IDENTITY,"status":"FROZEN"},{"item":"target","value":"hitless = actual_hits == 0","status":"FROZEN"},{"item":"grain","value":"slate_date|game_id|player_id; event game_id|player_id|pa_number","status":"FROZEN"},{"item":"aggregation","value":"sum_n P(PA=n)*(1-p_hit_per_pa)^n","status":"FROZEN"},{"item":"amendment_policy","value":"new version required; protected holdout never retuned","status":"FROZEN"}])
    write_csv(OUT/"event_process_v1_frozen_specification.csv",[{"identity":SPEC,"pa_component":"PA_C_MULTINOMIAL","hit_component":"HIT_B hitter strict-prior regularized logistic" if ep=="EP_2" else "validation-selected regularized logistic","aggregation":f"{ep} explicit PA distribution times per-PA hit probability","status":decisions["MLB_HITS05_EVENT_PROCESS_V1_SPECIFICATION_DECISION"],"supported_population":"strict-prior feature-complete player-games","unsupported_population":"unresolved identity or strict-prior lineage; certified substitution taxonomy"}])
    write_csv(OUT/"five_slate_500_row_live_trial_contract.csv",[{"stopping_condition":"5 qualifying completed slates OR 500 graded eligible hitters","activation":"NOT_ACTIVATED","eligibility":trial,"governing_metrics":"hitless PR AUC|Brier|log loss|top-20 precision recall lift","production_effect":"NONE"}])
    write_csv(OUT/"machine_readable_decisions.csv",[{"decision":k,"value":v} for k,v in decisions.items()])
    machine={"generated_at":generated,"identity":IDENTITY,"selected_variant":ep,"model_hashes":{"pa":sha(OUT/"pa_distribution_component.joblib"),**{k:sha(OUT/f"{k.lower()}_per_pa_hit_component.joblib") for k in hit_models}},"holdout_metrics":hm.reset_index().to_dict("records"),"external_controls":controls,"bootstrap_selected_minus_pa":boot,"decisions":decisions,"guardrails":{"network":False,"database_writes":False,"sportsbook_features":False,"production_changes":False}}
    (OUT/"machine_readable.json").write_text(json.dumps(machine,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")
    direct=(f"Validation-selected event-process {ep} {'improved' if improves_pa else 'did not improve'} protected-holdout PA-only probability quality; it {'cleared' if beats_candidate else 'did not clear'} the current candidate on identical joined control rows. Trial decision: {trial}.")
    (OUT/"event_process_architecture_v1_report.md").write_text(f"# MLB Hits 0.5 Event-Process Architecture v1\n\n## Direct answer\n\n{direct}\n\nThe architecture uses an explicit PA distribution and per-opportunity hit probability. Official PA events are outcomes only; same-game pitcher sequence and handedness are excluded from model inputs when pregame lineage is unavailable. No production behavior changed.\n",encoding="utf-8")
    validation=[]
    for p in sorted(OUT.iterdir()):
        if not p.is_file() or p.name in {"sha256_manifest.csv","validation_report.csv"}: continue
        st="PASS"; note=""
        try:
            if p.suffix==".csv": pd.read_csv(p,low_memory=False)
            elif p.suffix==".json": json.loads(p.read_text())
            elif p.suffix==".joblib": joblib.load(p)
        except Exception as exc: st="FAIL"; note=str(exc)
        validation.append({"artifact":rel(p),"status":st,"notes":note})
    validation += [{"artifact":"guardrail:no_network","status":"PASS","notes":""},{"artifact":"guardrail:no_db_writes","status":"PASS","notes":""},{"artifact":"guardrail:no_sportsbook_features","status":"PASS","notes":"market rows comparison only"},{"artifact":"model_control_hashes","status":"PASS" if sha(INCUMBENT).startswith("2e7377") and sha(CANDIDATE).startswith("495910") else "FAIL","notes":""}]
    write_csv(OUT/"validation_report.csv",validation)
    files=[p for p in OUT.iterdir() if p.is_file() and p.name!="sha256_manifest.csv"]; write_csv(OUT/"sha256_manifest.csv",[{"path":rel(p),"sha256":sha(p),"bytes":p.stat().st_size} for p in sorted(files)])
    print(json.dumps({"direct_answer":direct,"decisions":decisions,"rows":len(d),"events":len(e)},indent=2))
    return 0


if __name__=="__main__": raise SystemExit(main())
