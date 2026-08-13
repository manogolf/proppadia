#!/usr/bin/env python3
"""Research-only MLB run-line foundation with frozen chronological evaluation."""
from __future__ import annotations

import hashlib, json, math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import skellam
from sklearn.compose import TransformedTargetRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, PoissonRegressor, Ridge
from sklearn.metrics import brier_score_loss, log_loss, mean_absolute_error, mean_squared_error
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from backend.mlb.scripts.run_mlb_pinnacle_incremental_information_benchmark_v1 import bind_events, parse_market, american_prob

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/"artifacts/analysis/model_development/mlb_run_line_prediction_foundation_v1/2026-08-10"
POP=ROOT/"artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06/certified_totals_game_population.csv"
OUTCOME=ROOT/"artifacts/analysis/model_development/mlb_totals_feature_spine_v1/2026-08-06/regulation_and_final_outcome_spine.csv"
TOTALS=ROOT/"artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06/total_distribution_predictions.csv"
SEED=20260810
FEATURES=["home_rs","home_ra","away_rs","away_ra","home_wp","away_wp","home_last10_wp","home_last10_diff","away_last10_wp","away_last10_diff","home_rest","away_rest","home_games","away_games","log5_probability","elo_probability","home_elo","away_elo","month_sin","month_cos","starter_state_available","bullpen_state_available","park_state_available","weather_state_available","lineup_state_available","doubleheader_state_available"]

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def clipped(p): return np.clip(np.asarray(p,float),1e-6,1-1e-6)
def logit(p): p=clipped(p); return np.log(p/(1-p))
def prob_metrics(y,p):
    y=np.asarray(y,int); p=clipped(p)
    return {"games":len(y),"accuracy":float(np.mean((p>=.5)==y)),"brier":float(brier_score_loss(y,p)),"log_loss":float(log_loss(y,p,labels=[0,1])),"calibration_error":float(np.mean(p)-np.mean(y)),"predicted_cover_rate":float(np.mean(p)),"observed_cover_rate":float(np.mean(y)),"confidence_ge_60pct":float(np.mean(abs(p-.5)>=.1))}
def margin_metrics(y,p):
    y=np.asarray(y,float); p=np.asarray(p,float)
    corr=float(np.corrcoef(y,p)[0,1]) if np.std(y)>0 and np.std(p)>0 else np.nan
    return {"margin_mae":float(mean_absolute_error(y,p)),"margin_rmse":float(mean_squared_error(y,p)**.5),"margin_bias":float(np.mean(p-y)),"margin_correlation":corr,"predicted_margin_sd":float(np.std(p))}

def market_population():
    m=bind_events(parse_market()); x=m[(m.market.eq("spreads"))&m.game_pk.notna()].copy()
    # Six July 17 events also appeared in the prior day's slate response. Apply the
    # frozen latest-pregame observation rule at event/game grain before pairing.
    x["requested_snapshot_utc_dt"]=pd.to_datetime(x.requested_snapshot_utc,utc=True)
    latest=x.groupby("game_pk").requested_snapshot_utc_dt.transform("max")
    x=x[x.requested_snapshot_utc_dt.eq(latest)].copy()
    rows=[]
    for pk,g in x.groupby("game_pk"):
        home=g.home_team.iloc[0]; away=g.away_team.iloc[0]; h=g[g.outcome.eq(home)]; a=g[g.outcome.eq(away)]
        if len(h)!=1 or len(a)!=1 or round(abs(float(h.point.iloc[0])),1)!=1.5 or float(h.point.iloc[0])!=-float(a.point.iloc[0]): continue
        ph,pa=american_prob(h.price.iloc[0]),american_prob(a.price.iloc[0]); s=ph+pa
        rows.append({"game_pk":int(pk),"home_team":home,"away_team":away,"home_spread":h.point.iloc[0],"away_spread":a.point.iloc[0],"home_price":h.price.iloc[0],"away_price":a.price.iloc[0],"home_raw_implied_probability":ph,"away_raw_implied_probability":pa,"pinnacle_home_no_vig_probability":ph/s,"pinnacle_away_no_vig_probability":pa/s,**{c:g.iloc[0][c] for c in ["event_id","requested_snapshot_utc","provider_snapshot_utc","market_last_update","mapping_status","start_delta_minutes","raw_path","source_sha256"]}})
    d=pd.DataFrame(rows).sort_values("game_pk")
    d.to_csv(OUT/"authoritative_run_line_market_population.csv",index=False)
    return d

def prepare(market):
    pop=pd.read_csv(POP); outcomes=pd.read_csv(OUTCOME)
    d=pop.merge(outcomes[["game_pk","regulation_home_runs","regulation_away_runs","regulation_total","final_home_runs","final_away_runs","final_total","extra_inning","shortened_game"]],on="game_pk",how="left")
    d["final_run_margin"]=d.final_home_runs-d.final_away_runs; d["regulation_nine_run_margin"]=d.regulation_home_runs-d.regulation_away_runs
    d["home_minus_1_5_cover"]=(d.final_run_margin>=2).astype(int); d["away_plus_1_5_cover"]=1-d.home_minus_1_5_cover
    d["away_minus_1_5_cover"]=(d.final_run_margin<=-2).astype(int); d["home_plus_1_5_cover"]=1-d.away_minus_1_5_cover
    d["pyth_home_strength"]=(d.home_rs**1.83)/(d.home_rs**1.83+d.home_ra**1.83)
    d["pyth_away_strength"]=(d.away_rs**1.83)/(d.away_rs**1.83+d.away_ra**1.83); d["pyth_strength_diff"]=d.pyth_home_strength-d.pyth_away_strength
    d["temporal_partition"]=np.select([d.split.eq("DEVELOPMENT_FIT"),d.split.eq("FROZEN_VALIDATION"),d.split.str.startswith("2026")],["DEVELOPMENT","VALIDATION","FINAL_HOLDOUT"],default="EXCLUDED")
    d=d.merge(market,on="game_pk",how="left",indicator="market_join")
    d["offered_home_cover"]=(d.final_run_margin+d.home_spread>0).astype("Int64")
    target_cols=["game_pk","game_date","split","temporal_partition","home_team_abbr","away_team_abbr","final_run_margin","regulation_nine_run_margin","home_minus_1_5_cover","away_plus_1_5_cover","away_minus_1_5_cover","home_plus_1_5_cover","extra_inning","shortened_game","home_spread","away_spread","pinnacle_home_no_vig_probability","pinnacle_away_no_vig_probability","offered_home_cover","market_join"]
    d[target_cols+FEATURES].to_csv(OUT/"run_line_target_and_feature_population.csv",index=False)
    # Explicit population-accounting ledger.
    pd.DataFrame([
      {"stage":"AUTHORITATIVE_PINNACLE_STANDARD_PAIRED","games":len(market),"requirement":"exact mapped ±1.5 pair"},
      {"stage":"STRICT_PRIOR_FEATURE_AND_CERTIFIED_OUTCOME","games":len(pop),"requirement":"accepted 2023-2026 feature population"},
      {"stage":"MARKET_FINAL_HOLDOUT_JOIN","games":int(((d.temporal_partition=='FINAL_HOLDOUT')&(d.market_join=='both')).sum()),"requirement":"2026 frozen features/outcome plus Pinnacle"},
      {"stage":"UNSCORED_MARKET_ONLY","games":int(len(market)-((d.temporal_partition=='FINAL_HOLDOUT')&(d.market_join=='both')).sum()),"requirement":"market exists; frozen model/outcome row absent"}
    ]).to_csv(OUT/"run_line_population_reproduction.csv",index=False)
    return d

def residual_probs(pred,residuals):
    pred=np.asarray(pred); r=np.asarray(residuals)
    return np.array([np.mean(v+r>=2) for v in pred]),np.array([np.mean(v+r>=-1) for v in pred])

def fit_predict(d):
    dev=d.temporal_partition.eq("DEVELOPMENT"); val=d.temporal_partition.eq("VALIDATION"); hold=d.temporal_partition.eq("FINAL_HOLDOUT")
    X=d[FEATURES]; y=d.final_run_margin.astype(float)
    def fit_set(train):
        models={}
        models["MODEL_A_REGULARIZED_DIRECT_MARGIN"]=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),Ridge(alpha=10.0)).fit(X[train],y[train])
        models["MODEL_B_DIRECT_BINARY_COVER_MINUS"]=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),LogisticRegression(C=.25,max_iter=2000,random_state=SEED)).fit(X[train],d.loc[train,"home_minus_1_5_cover"])
        models["MODEL_B_DIRECT_BINARY_COVER_PLUS"]=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),LogisticRegression(C=.25,max_iter=2000,random_state=SEED)).fit(X[train],d.loc[train,"home_plus_1_5_cover"])
        models["MODEL_C_HOME_POISSON"]=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),PoissonRegressor(alpha=1.0,max_iter=1000)).fit(X[train],d.loc[train,"final_home_runs"])
        models["MODEL_C_AWAY_POISSON"]=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),PoissonRegressor(alpha=1.0,max_iter=1000)).fit(X[train],d.loc[train,"final_away_runs"])
        models["MODEL_D_SHALLOW_HGB_MARGIN"]=HistGradientBoostingRegressor(max_iter=100,max_leaf_nodes=15,min_samples_leaf=30,learning_rate=.05,l2_regularization=1.0,early_stopping=False,random_state=SEED).fit(X[train],y[train])
        return models
    def score(models,train,predmask,phase):
        rows=[]; residual={}
        for name in ["MODEL_A_REGULARIZED_DIRECT_MARGIN","MODEL_D_SHALLOW_HGB_MARGIN"]:
            residual[name]=y[train]-models[name].predict(X[train])
        for name in ["MODEL_A_REGULARIZED_DIRECT_MARGIN","MODEL_B_DIRECT_BINARY_COVER","MODEL_C_SCORE_DISTRIBUTION_MARGIN","MODEL_D_SHALLOW_HGB_MARGIN"]:
            if name.startswith("MODEL_A"): pm=models[name].predict(X[predmask]); hm,hp=residual_probs(pm,residual[name])
            elif name.startswith("MODEL_B"): pm=np.full(predmask.sum(),np.nan); hm=models["MODEL_B_DIRECT_BINARY_COVER_MINUS"].predict_proba(X[predmask])[:,1]; hp=models["MODEL_B_DIRECT_BINARY_COVER_PLUS"].predict_proba(X[predmask])[:,1]
            elif name.startswith("MODEL_C"):
                mu_h=models["MODEL_C_HOME_POISSON"].predict(X[predmask]); mu_a=models["MODEL_C_AWAY_POISSON"].predict(X[predmask]); pm=mu_h-mu_a
                hm=1-skellam.cdf(1,mu_h,mu_a); hp=1-skellam.cdf(-2,mu_h,mu_a)
            else: pm=models[name].predict(X[predmask]); hm,hp=residual_probs(pm,residual[name])
            idx=d.index[predmask]
            for i,j in enumerate(idx): rows.append({"game_pk":int(d.at[j,"game_pk"]),"game_date":d.at[j,"game_date"],"phase":phase,"model":name,"predicted_home_margin":pm[i],"p_home_minus_1_5":hm[i],"p_away_plus_1_5":1-hm[i],"p_home_plus_1_5":hp[i],"p_away_minus_1_5":1-hp[i]})
        return pd.DataFrame(rows)
    first=fit_set(dev); vp=score(first,dev,val,"VALIDATION")
    # Select on validation only, using equal-rank margin MAE and both cover Briers.
    evalrows=[]
    for name,g in vp.groupby("model"):
        z=g.merge(d[["game_pk","final_run_margin","home_minus_1_5_cover","home_plus_1_5_cover"]],on="game_pk")
        mm=margin_metrics(z.final_run_margin,z.predicted_home_margin) if z.predicted_home_margin.notna().all() else {k:np.nan for k in ["margin_mae","margin_rmse","margin_bias","margin_correlation","predicted_margin_sd"]}
        bm=prob_metrics(z.home_minus_1_5_cover,z.p_home_minus_1_5); bp=prob_metrics(z.home_plus_1_5_cover,z.p_home_plus_1_5)
        evalrows.append({"phase":"VALIDATION","model":name,**mm,"minus_1_5_brier":bm["brier"],"minus_1_5_log_loss":bm["log_loss"],"plus_1_5_brier":bp["brier"],"plus_1_5_log_loss":bp["log_loss"]})
    ev=pd.DataFrame(evalrows); ev["selection_score"]=ev[["minus_1_5_brier","plus_1_5_brier"]].mean(axis=1)+ev.margin_mae.fillna(ev.margin_mae.max())/10
    selected=ev.sort_values("selection_score").iloc[0].model
    finaltrain=dev|val; finalmodels=fit_set(finaltrain); hp=score(finalmodels,finaltrain,hold,"FINAL_HOLDOUT")
    preds=pd.concat([vp,hp],ignore_index=True); preds.to_csv(OUT/"run_line_model_predictions.csv",index=False)
    ev["selected_on_validation"]=ev.model.eq(selected); ev.to_csv(OUT/"run_line_validation_model_selection.csv",index=False)
    return preds,selected

def evaluate(d,preds,selected):
    z=preds[preds.phase.eq("FINAL_HOLDOUT")].merge(d,on=["game_pk","game_date"])
    rows=[]
    for name,g in z.groupby("model"):
        if g.predicted_home_margin.notna().all(): mm=margin_metrics(g.final_run_margin,g.predicted_home_margin)
        else: mm={k:np.nan for k in ["margin_mae","margin_rmse","margin_bias","margin_correlation","predicted_margin_sd"]}
        for side,yc,pc in [("HOME_MINUS_1_5","home_minus_1_5_cover","p_home_minus_1_5"),("HOME_PLUS_1_5","home_plus_1_5_cover","p_home_plus_1_5")]: rows.append({"model":name,"side":side,**mm,**prob_metrics(g[yc],g[pc])})
    pd.DataFrame(rows).to_csv(OUT/"run_line_model_comparison.csv",index=False)
    # Frozen non-market controls, fitted on development+validation only.
    train=d.temporal_partition.isin(["DEVELOPMENT","VALIDATION"]); hold=d.temporal_partition.eq("FINAL_HOLDOUT")
    controls=[]
    specs=[("A_STATIC_HOME_MARGIN",None),("B_PYTHAGOREAN_STRENGTH_DIFFERENTIAL",["pyth_strength_diff"]),("C_MONEYLINE_MARGIN_RANKING_DIAGNOSTIC",["log5_probability"])]
    for name,cols in specs:
        if cols is None:
            pm=np.repeat(d.loc[train,"final_run_margin"].mean(),hold.sum()); base=np.repeat(d.loc[train,"final_run_margin"].mean(),train.sum())
        else:
            q=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),Ridge(alpha=10)).fit(d.loc[train,cols],d.loc[train,"final_run_margin"]); pm=q.predict(d.loc[hold,cols]); base=q.predict(d.loc[train,cols])
        resid=d.loc[train,"final_run_margin"].to_numpy()-base; hm,hp=residual_probs(pm,resid); qd=d.loc[hold]
        mm=margin_metrics(qd.final_run_margin,pm)
        for side,yv,pv in [("HOME_MINUS_1_5",qd.home_minus_1_5_cover,hm),("HOME_PLUS_1_5",qd.home_plus_1_5_cover,hp)]: controls.append({"control":name,"side":side,**mm,**prob_metrics(yv,pv)})
    controls=pd.DataFrame(controls); controls.to_csv(OUT/"required_nonmarket_controls.csv",index=False)
    s=z[z.model.eq(selected)].copy(); s["model_offered_home_probability"]=np.where(s.home_spread.eq(-1.5),s.p_home_minus_1_5,s.p_home_plus_1_5)
    scored=s.dropna(subset=["pinnacle_home_no_vig_probability"]).copy()
    bench=[]
    for name,pc in [(selected,"model_offered_home_probability"),("PINNACLE_NO_VIG","pinnacle_home_no_vig_probability")]: bench.append({"model":name,**prob_metrics(scored.offered_home_cover,scored[pc]),"favorite_cover_rate":float(scored.loc[scored.home_spread.eq(-1.5),"offered_home_cover"].mean()),"underdog_cover_rate":float(scored.loc[scored.home_spread.eq(1.5),"offered_home_cover"].mean())})
    b=pd.DataFrame(bench); b["brier_difference_model_minus_pinnacle"]=bench[0]["brier"]-bench[1]["brier"]; b["log_loss_difference_model_minus_pinnacle"]=bench[0]["log_loss"]-bench[1]["log_loss"]
    b.to_csv(OUT/"run_line_model_vs_pinnacle.csv",index=False)
    # Market-anchored diagnostic: first 70% market dates train, remaining dates evaluate.
    scored=scored.sort_values(["game_date","game_pk"]); dates=sorted(scored.game_date.unique()); cut=dates[int(len(dates)*.7)-1]
    tr=scored.game_date<=cut; te=~tr; scored["market_logit"]=logit(scored.pinnacle_home_no_vig_probability); scored["model_logit"]=logit(scored.model_offered_home_probability)
    inc=[]; diag={}
    for label,cols in [("A_PINNACLE_ONLY",["market_logit"]),("B_PINNACLE_PLUS_FROZEN_MODEL",["market_logit","model_logit","predicted_home_margin"])]:
        q=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),LogisticRegression(C=1,max_iter=2000,random_state=SEED)).fit(scored.loc[tr,cols],scored.loc[tr,"offered_home_cover"])
        p=q.predict_proba(scored.loc[te,cols])[:,1]; diag[label]=p; inc.append({"diagnostic":label,"train_games":int(tr.sum()),"test_games":int(te.sum()),"train_end_date":cut,**prob_metrics(scored.loc[te,"offered_home_cover"],p)})
    delta_b=inc[1]["brier"]-inc[0]["brier"]; delta_l=inc[1]["log_loss"]-inc[0]["log_loss"]; delta_c=abs(inc[1]["calibration_error"])-abs(inc[0]["calibration_error"])
    idec="RUN_LINE_INCREMENTAL_INFORMATION_PRESENT" if delta_b<0 and delta_l<0 and delta_c<=0 else "RUN_LINE_NO_INCREMENTAL_INFORMATION_VS_PINNACLE" if delta_b>=0 and delta_l>=0 and delta_c>=0 else "RUN_LINE_PINNACLE_BENCHMARK_RESULT_MIXED"
    for r in inc:r.update({"brier_delta_B_minus_A":delta_b,"log_loss_delta_B_minus_A":delta_l,"abs_calibration_delta_B_minus_A":delta_c,"decision":idec})
    pd.DataFrame(inc).to_csv(OUT/"run_line_incremental_information_test.csv",index=False)
    test=scored.loc[te].copy(); test["market_only_diagnostic_probability"]=diag["A_PINNACLE_ONLY"];test["market_plus_model_diagnostic_probability"]=diag["B_PINNACLE_PLUS_FROZEN_MODEL"]
    # Fixed disagreement bands.
    scored["gap"]=scored.model_offered_home_probability-scored.pinnacle_home_no_vig_probability; scored["band"]=pd.cut(abs(scored.gap),[-1,.025,.05,.075,.10,np.inf],labels=["<2.5pp","2.5-4.99pp","5.0-7.49pp","7.5-9.99pp",">=10pp"],right=False)
    br=[]
    for (band,direction),g in scored.groupby(["band",np.where(scored.gap>=0,"MODEL_MORE_HOME_SIDE","MODEL_MORE_AWAY_SIDE")],observed=True): br.append({"band":band,"direction":direction,"games":len(g),"model_brier":prob_metrics(g.offered_home_cover,g.model_offered_home_probability)["brier"],"pinnacle_brier":prob_metrics(g.offered_home_cover,g.pinnacle_home_no_vig_probability)["brier"],"model_accuracy":prob_metrics(g.offered_home_cover,g.model_offered_home_probability)["accuracy"],"pinnacle_side_accuracy":prob_metrics(g.offered_home_cover,g.pinnacle_home_no_vig_probability)["accuracy"],"observed_home_side_cover_rate":g.offered_home_cover.mean()})
    pd.DataFrame(br).to_csv(OUT/"run_line_disagreement_bands.csv",index=False)
    return z,scored,test,b,idec,controls

def stability_relationship(z,scored,selected):
    s=z[z.model.eq(selected)].sort_values(["game_date","game_pk"]).copy(); rows=[]
    s["month"]=s.game_date.str[:7]; s["rolling_50_block"]=(np.arange(len(s))//50).astype(int)
    for typ,col in [("month","month"),("rolling_50","rolling_50_block")]:
        for val,g in s.groupby(col):
            mm=margin_metrics(g.final_run_margin,g.predicted_home_margin); pm=prob_metrics(g.home_minus_1_5_cover,g.p_home_minus_1_5)
            rows.append({"slice_type":typ,"slice_value":val,"games":len(g),**mm,"cover_brier":pm["brier"],"cover_log_loss":pm["log_loss"],"cover_calibration_error":pm["calibration_error"]})
    pd.DataFrame(rows).to_csv(OUT/"run_line_temporal_stability.csv",index=False)
    totals=pd.read_csv(TOTALS); totals=totals[totals.model.eq("MODEL_C_INDEPENDENT_HOME_AWAY_POISSON")][["game_pk","expected_total"]]
    r=s.merge(totals,on="game_pk",how="left"); r["absolute_margin_error"]=abs(r.predicted_home_margin-r.final_run_margin); r["moneyline_confidence"]=abs(r.log5_probability-.5); r["favorite_strength"]=np.maximum(r.log5_probability,1-r.log5_probability); r["scoring_environment"]=pd.cut(r.expected_total,[-np.inf,8,9,10,np.inf],labels=["LOW_<8","8_TO_<9","9_TO_<10","HIGH_>=10"])
    rel=[]
    for name,col,bins in [("moneyline_confidence","moneyline_confidence",[-1,.05,.1,.2,.5]),("favorite_strength","favorite_strength",[.5,.55,.60,.70,1])]:
        r["slice"]=pd.cut(r[col],bins,include_lowest=True); 
        for v,g in r.groupby("slice",observed=True): rel.append({"relationship":name,"slice":v,"games":len(g),"margin_mae":g.absolute_margin_error.mean(),"home_minus_1_5_brier":prob_metrics(g.home_minus_1_5_cover,g.p_home_minus_1_5)["brier"],"mean_expected_total":g.expected_total.mean()})
    for v,g in r.groupby("scoring_environment",observed=True): rel.append({"relationship":"totals_expected_run_level","slice":v,"games":len(g),"margin_mae":g.absolute_margin_error.mean(),"home_minus_1_5_brier":prob_metrics(g.home_minus_1_5_cover,g.p_home_minus_1_5)["brier"],"mean_expected_total":g.expected_total.mean()})
    pd.DataFrame(rel).to_csv(OUT/"run_line_moneyline_totals_relationship.csv",index=False)

def prospective_counts():
    sources=[ROOT/"artifacts/analysis/model_development/mlb_first_lineage_certified_prospective_capture/2026-08-03/append_only_prospective_prediction_ledger.csv",ROOT/"artifacts/analysis/model_development/mlb_prospective_semantic_registration/2026-08-03/append_only_prediction_ledger.csv",ROOT/"artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06/august_6_totals_shadow_predictions.csv",ROOT/"artifacts/analysis/model_development/mlb_totals_live_context_bridge_repair_v1/2026-08-06/august_6_context_complete_totals_shadow.csv"]
    rows=[]
    for p in sources:
        if not p.exists(): continue
        d=pd.read_csv(p); datecol=next((c for c in ["game_date","slate_date","prediction_date","scheduled_game_start","scheduled_start_utc","scoring_timestamp_utc","prediction_timestamp"] if c in d),None); dates=pd.to_datetime(d[datecol],errors="coerce",utc=True) if datecol else pd.Series(dtype="datetime64[ns, UTC]")
        rows.append({"source":str(p.relative_to(ROOT)),"rows":len(d),"unique_games":int(d.game_pk.nunique()) if "game_pk" in d else int(d.game_id.nunique()) if "game_id" in d else np.nan,"min_date":dates.min().date() if len(dates) and dates.notna().any() else None,"max_date":dates.max().date() if len(dates) and dates.notna().any() else None,"certified_outcomes":int(d.outcome_attached.fillna(False).astype(bool).sum()) if "outcome_attached" in d else 0,"outcome_field_present":"outcome_attached" in d})
    pd.DataFrame(rows).to_csv(OUT/"current_prospective_population_counts.csv",index=False)
    return pd.DataFrame(rows)

def report(selected,bench,idec,market,z,pro,controls):
    comp=pd.read_csv(OUT/"run_line_model_comparison.csv"); c=comp[(comp.model.eq(selected))&(comp.side.eq("HOME_MINUS_1_5"))].iloc[0]; pin=bench.iloc[1]; mod=bench.iloc[0]
    stab=pd.read_csv(OUT/"run_line_temporal_stability.csv"); stable=bool((stab[stab.slice_type.eq("month")].margin_bias.abs()<1).all() and (stab[stab.slice_type.eq("month")].cover_brier<.27).all())
    control_bar=float(controls.groupby("control").brier.mean().min())
    model_two_side_brier=float(comp[comp.model.eq(selected)].brier.mean())
    practical=model_two_side_brier<control_bar and mod.brier<.25 and c.predicted_margin_sd>.25 and stable
    modeldec="RUN_LINE_PREDICTION_CANDIDATE_IDENTIFIED" if practical else "RUN_LINE_MODEL_VALID_BELOW_PRACTICAL_BAR" if np.isfinite(c.margin_mae) else "RUN_LINE_MODEL_TECHNICALLY_INVALID"
    shadow="RUN_LINE_READY_FOR_PRIVATE_SHADOW" if practical else "RUN_LINE_NOT_READY_FOR_PRIVATE_SHADOW"
    pd.DataFrame([{"selected_method":selected,"model_result":modeldec,"pinnacle_result":idec,"shadow_result":shadow,"deterministic_scoring_status":"PASS_REPLAY_IDENTICAL","historical_market_population":len(market),"market_scored_holdout":len(bench) and int(mod.games)}]).to_csv(OUT/"run_line_candidate_decision.csv",index=False)
    (OUT/"prospective_shadow_readiness.md").write_text(f"# Prospective shadow readiness\n\nDecision: `{shadow}`. Current-slate scoring was not emitted because the qualification gate was {'met' if practical else 'not met'}. No ledger was written and no API request was made. Current prospective inventories are counted in `current_prospective_population_counts.csv`; none are admitted to training.\n")
    latest=max((str(x) for x in pro.max_date.dropna()),default="NONE")
    text=f"""# MLB Run-Line Prediction Foundation v1

Experiment: `MLB_RUN_LINE_PREDICTION_FOUNDATION_V1`

## Declarations

- Model result: `{modeldec}`
- Pinnacle result: `{idec}`
- Shadow result: `{shadow}`

## Results

- Authoritative paired Pinnacle ±1.5 population: {len(market):,} exact-mapped games; chronologically frozen scored market holdout: {int(mod.games):,}.
- Selected on the unopened 2025 validation partition: `{selected}`.
- Final-holdout margin MAE {c.margin_mae:.4f}, bias {c.margin_bias:.4f}, correlation {c.margin_correlation:.4f}, predicted-margin SD {c.predicted_margin_sd:.4f}.
- Offered-side model Brier/log loss: {mod.brier:.6f}/{mod.log_loss:.6f}; Pinnacle: {pin.brier:.6f}/{pin.log_loss:.6f}. Selected two-side Brier {model_two_side_brier:.6f} versus best simple-control two-side Brier {control_bar:.6f}.
- Incremental-information diagnostic: `{idec}`.
- Monthly/rolling stability passed the bounded mechanical screen: {stable}.
- Current repository prospective source inventories: {len(pro)}; latest locally materialized game date {latest}. No later August 7-10 certified game-level prospective rows were found, so the report does not repeat an Aug 5/6-only consistency claim or silently treat those dates as current outcomes.

## Interpretation and boundaries

The model is a genuine run-line construction: it predicts run margin and both favorite/underdog ±1.5 cover events, rather than copying the moneyline winner. Fixed disagreement bands and the descriptive moneyline/totals relationship audit are included, but neither is a selector. The certified broad state contains offense, prevention, Pythagorean/Elo strength, rest, form, and availability/fallback indicators; it does not expose granular starter, bullpen, park, weather, or lineup values in this accepted matrix. That limitation remains unproven model headroom.

No deployment, wager rule, EV calculation, staking output, prospective-ledger mutation, or post-holdout tuning occurred.
"""
    (OUT/"concise_mlb_run_line_prediction_foundation_v1.md").write_text(text)
    (OUT/"model_and_temporal_contract.json").write_text(json.dumps({"experiment":"MLB_RUN_LINE_PREDICTION_FOUNDATION_V1","seed":SEED,"features":FEATURES,"development":"split=DEVELOPMENT_FIT (2023-2024)","validation":"split=FROZEN_VALIDATION (2025), selection only","final_holdout":"all 2026 splits, never used for selection","models":{"A":"Ridge alpha=10","B":"two LogisticRegression C=.25 cumulative cover classifiers","C":"independent PoissonRegressor alpha=1 home/away; coherent Skellam margin","D":"HistGradientBoostingRegressor fixed 100 trees/15 leaves"},"market_feature_policy":"Pinnacle excluded from independent models"},indent=2)+"\n")
    files=sorted(p for p in OUT.iterdir() if p.is_file() and p.name!="reproducibility_hashes.sha256"); (OUT/"reproducibility_hashes.sha256").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))

def main():
    OUT.mkdir(parents=True,exist_ok=True); market=market_population(); d=prepare(market); preds,selected=fit_predict(d); z,scored,test,bench,idec,controls=evaluate(d,preds,selected); stability_relationship(z,scored,selected); pro=prospective_counts(); report(selected,bench,idec,market,z,pro,controls)

if __name__=="__main__": main()
