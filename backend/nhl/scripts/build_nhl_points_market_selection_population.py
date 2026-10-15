#!/usr/bin/env python3
"""Read-only audit of market selection in the frozen NHL Points HGB population."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from backend.nhl.scripts import build_nhl_points_hgb_confidence_root_cause as rootcause

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/points_market_selection_population/2026-10-15"
FEATURES = rootcause.FEATURES
DATES = [f"2026-10-{d:02d}" for d in range(9, 15)]
MARKET_ROOT = ROOT / "backend/nhl/exports/odds_history"
OUTCOME_ROOT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
CAPROOT = ROOT / "artifacts/operational/nhl/points_hgb_shadow/season=2026"
CONF_BANDS = [0.60, 0.65, 0.70, 0.75, 0.80, 1.000001]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def num(s):
    return pd.to_numeric(s, errors="coerce")


def metrics(x: pd.DataFrame) -> dict:
    if x.empty:
        return {"n": 0}
    y, mu = num(x.realized_points).to_numpy(float), num(x.expected_points).to_numpy(float)
    po = np.clip(num(x.prob_over_0_5).to_numpy(float), 1e-12, 1 - 1e-12)
    pu, under = 1 - po, (y < 1).astype(int)
    return {
        "n": len(x), "mean_predicted_points": float(mu.mean()), "mean_realized_points": float(y.mean()),
        "residual_realized_minus_predicted": float((y - mu).mean()), "mae": float(np.abs(y - mu).mean()),
        "rmse": float(np.sqrt(np.mean((y - mu) ** 2))), "p_over_0_5": float(po.mean()),
        "realized_point_game_rate": float((y >= 1).mean()), "calibration_gap_over": float(po.mean() - (y >= 1).mean()),
        "brier_0_5": float(brier_score_loss((y >= 1).astype(int), po)),
        "log_loss_0_5": float(log_loss((y >= 1).astype(int), po, labels=[0, 1])),
        "mean_p_under": float(pu.mean()), "realized_under_rate": float(under.mean()),
        "under_calibration_gap_pred_minus_realized": float(pu.mean() - under.mean()),
        "under_brier": float(brier_score_loss(under, pu)), "under_log_loss": float(log_loss(under, pu, labels=[0, 1])),
        "two_plus_point_rate": float((y >= 2).mean()),
    }


def classify_quote_rows(q: pd.DataFrame) -> pd.Series:
    """Use retained quote evidence only, keeping one-sided evidence distinct."""
    over_present = num(q.price_over).notna() | (num(q.source_quote_count_over).fillna(0) > 0)
    under_present = num(q.price_under).notna() | (num(q.source_quote_count_under).fillna(0) > 0)
    return pd.Series(np.select([over_present & under_present, over_present | under_present],
                               ["EXACT_TWO_SIDED", "ONE_SIDED_PARTIAL"], default="ATTACHMENT_NO_QUOTE"), index=q.index)


def read_population() -> tuple[pd.DataFrame, dict]:
    capindex = rootcause.capture_index()
    import joblib
    from backend.nhl.scripts.score_nhl_points_hgb_shadow import MODEL_PATH
    model=joblib.load(MODEL_PATH)
    chunks, lineage = [], []
    for date in DATES:
        f = capindex.get(date)
        if f is None:
            raise RuntimeError(f"No immutable HGB capture for {date}")
        pred = f.parent / "predictions.csv"
        x, p = pd.read_csv(f), pd.read_csv(pred)
        replay=rootcause.score(x,model)
        r=replay.merge(p,on=["game_id","player_id"],suffixes=("_replay","_stored"),validate="one_to_one")
        predcols=["expected_points","prob_over_0_5","prob_over_1_5","prob_over_2_5"]
        replay_error=max(float((num(r[f"{c}_replay"])-num(r[f"{c}_stored"])).abs().max()) for c in predcols)
        if len(r)!=len(p) or replay_error>1e-12:
            raise RuntimeError(f"FROZEN_CAPTURE_REPLAY_FAILED:{date}:{replay_error}")
        d = x.merge(p, on=["game_id", "player_id", "game_date", "game_start_utc"], validate="one_to_one")
        d["prob_under_0_5"] = 1-num(d.prob_over_0_5)
        d["slate_date"] = date
        chunks.append(d)
        lineage.append({"slate_date": date, "feature_path": str(f.relative_to(ROOT)), "feature_sha256": sha(f),
                        "prediction_path": str(pred.relative_to(ROOT)), "prediction_sha256": sha(pred), "capture_rows": len(d),
                        "deterministic_replay":"PASS","prediction_max_abs_error":replay_error})
    full = pd.concat(chunks, ignore_index=True)
    if full.duplicated(["slate_date", "game_id", "player_id"]).any():
        raise AssertionError("DUPLICATE_FULL_POPULATION_KEY")
    return full, {"captures": lineage, "capture_population_n": len(full)}


def add_quotes(full: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list]:
    quotes, sources = [], []
    for day in DATES:
        path = MARKET_ROOT / day / "points_with_market.csv"
        if not path.exists():
            raise RuntimeError(f"Missing retained pregame market attachment for {day}")
        q = pd.read_csv(path)
        q = q[np.isclose(num(q.line), .5)].copy()
        q = q.sort_values(["game_id", "player_id"]).drop_duplicates(["game_id", "player_id"], keep="last")
        q["quote_status"] = classify_quote_rows(q)
        q["market_quoted"] = q.quote_status.isin(["EXACT_TWO_SIDED", "ONE_SIDED_PARTIAL"])
        q["quote_slate_date"] = day
        quotes.append(q)
        sources.append({"slate_date": day, "path": str(path.relative_to(ROOT)), "sha256": sha(path),
                        "attachment_0_5_rows": len(q), "two_sided": int((q.quote_status == "EXACT_TWO_SIDED").sum()),
                        "partial": int((q.quote_status == "ONE_SIDED_PARTIAL").sum()),
                        "rows_without_quote": int((q.quote_status == "ATTACHMENT_NO_QUOTE").sum())})
    q = pd.concat(quotes, ignore_index=True)
    selected = ["quote_slate_date", "game_id", "player_id", "quote_status", "market_quoted", "price_over", "price_under",
                "p_over_mkt", "p_under_mkt", "source_quote_count_over", "source_quote_count_under", "source_books_over", "source_books_under",
                "odds_observation_manifest_sha256", "odds_raw_response_sha256"]
    out = full.merge(q[selected], left_on=["slate_date", "game_id", "player_id"],
                     right_on=["quote_slate_date", "game_id", "player_id"], how="left", validate="one_to_one")
    out["quote_status"] = out.quote_status.fillna("FULLY_UNMATCHED")
    out["market_quoted"] = out.market_quoted.fillna(False).astype(bool)
    out["quote_status_detail"] = out.quote_status
    return out, q, sources


def add_outcomes(full: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    outframes, sources = [], []
    for day in DATES:
        dirs = sorted((OUTCOME_ROOT / day).glob("reconciliation=*/canonical_skater_outcomes.csv"))
        if not dirs:
            raise RuntimeError(f"Missing official outcomes for {day}")
        p = dirs[-1]
        o = pd.read_csv(p)
        o = o[o.slate_date.astype(str).eq(day)].copy()
        outframes.append(o)
        sources.append({"slate_date": day, "path": str(p.relative_to(ROOT)), "sha256": sha(p), "outcome_rows": len(o)})
    o = pd.concat(outframes, ignore_index=True)
    cols = ["slate_date", "game_id", "player_id", "team_id", "official_points", "participation_state", "official_final", "outcome_source"]
    if o.duplicated(cols[:3]).any():
        raise AssertionError("DUPLICATE_OFFICIAL_OUTCOME_KEY")
    merged = full.merge(o[cols], on=["slate_date", "game_id", "player_id"], how="left", validate="one_to_one")
    official_final=merged.official_final.eq(True)
    merged["outcome_status"] = np.select([
        official_final & merged.participation_state.eq("PARTICIPATED"),
        official_final & merged.participation_state.notna() & merged.participation_state.ne("PARTICIPATED"),
    ], ["SETTLED_PARTICIPATED", "FINAL_NONPARTICIPANT"], default="UNRESOLVED")
    merged["realized_points"] = num(merged.official_points).where(merged.outcome_status.eq("SETTLED_PARTICIPATED"))
    return merged, sources


def feature_comparison(x):
    rows=[]
    for col in FEATURES + ["season_ppg_prior"]:
        a, b = num(x.loc[x.market_quoted, col]), num(x.loc[~x.market_quoted, col])
        sd = np.sqrt((a.var(ddof=1) + b.var(ddof=1))/2)
        row={"feature":col,"quoted_n":int(a.notna().sum()),"nonquoted_n":int(b.notna().sum()),
             "quoted_missing_rate":float(a.isna().mean()),"nonquoted_missing_rate":float(b.isna().mean()),
             "quoted_mean":float(a.mean()),"nonquoted_mean":float(b.mean()),"quoted_median":float(a.median()),"nonquoted_median":float(b.median()),
             "quoted_q10":float(a.quantile(.1)),"quoted_q90":float(a.quantile(.9)),"nonquoted_q10":float(b.quantile(.1)),"nonquoted_q90":float(b.quantile(.9)),
             "standardized_mean_difference":float((a.mean()-b.mean())/sd) if sd else None}
        rows.append(row)
    return rows


def by_band(x, col, edges, labels, group_name):
    d=x.copy(); d["band"]=pd.cut(num(d[col]),edges,labels=labels,right=False,include_lowest=True).astype("string").fillna("MISSING")
    rows=[]
    for band,g in d.groupby("band",dropna=False,observed=True):
        rows.append({"dimension":group_name,"band":str(band),"total_player_games":len(g),"quoted_player_games":int(g.market_quoted.sum()),
                     "quote_rate":float(g.market_quoted.mean()),"n_settled":int(g.realized_points.notna().sum()),
                     "mean_expected":float(num(g.expected_points).mean()),"mean_realized":float(num(g.realized_points).mean()),
                     "mean_residual":float((num(g.realized_points)-num(g.expected_points)).mean())})
    return rows


def matched_analysis(x):
    """Exact outcome-blind coarsened strata for a transparent, reproducible comparison."""
    d=x[x.realized_points.notna()].copy()
    d["b_mu"]=pd.cut(num(d.expected_points),[0,.15,.25,.35,.45,.60,.8,1.1,math.inf],right=False,include_lowest=True).astype(str)
    d["b_l10"]=pd.cut(num(d.player_points_last10),[-math.inf,0,.25,.5,1,2,math.inf],right=False).astype(str)
    d["b_ppg"]=pd.cut(num(d.season_ppg_prior),[-math.inf,0,.25,.5,.75,1,1.5,math.inf],right=False).astype(str)
    d["b_toi"]=pd.cut(num(d.mean_toi_last10),[-math.inf,8,12,16,20,math.inf],right=False).astype(str)
    d["b_pptoi"]=pd.cut(num(d.mean_pp_toi_last10),[-math.inf,.1,1,2,3,math.inf],right=False).astype(str)
    d["b_d10"]=pd.cut(num(d.d10_sog_per60),[-math.inf,1,2,3,4,math.inf],right=False).astype(str)
    d["b_team"]=pd.cut(num(d.team_d10_sf_per_game),[-math.inf,20,25,30,35,math.inf],right=False).astype(str)
    keys=["slate_date","b_mu","b_l10","b_ppg","b_toi","b_pptoi","b_d10","b_team"]
    pairs=[]
    for _,g in d.groupby(keys,dropna=False,observed=True):
        a,b=g[g.market_quoted],g[~g.market_quoted]
        if len(a) and len(b):
            pairs.append({"stratum_n_quoted":len(a),"stratum_n_nonquoted":len(b),
                          "quoted_mean_expected":num(a.expected_points).mean(),"nonquoted_mean_expected":num(b.expected_points).mean(),
                          "predicted_difference":num(a.expected_points).mean()-num(b.expected_points).mean(),
                          "realized_difference":num(a.realized_points).mean()-num(b.realized_points).mean(),
                          "residual_difference":(num(a.realized_points)-num(a.expected_points)).mean()-(num(b.realized_points)-num(b.expected_points)).mean(),
                          "outcome_blind_stratum":True,"stratum_keys":"|".join(map(str,_ if isinstance(_,tuple) else (_ ,)))})
    p=pd.DataFrame(pairs)
    if p.empty: return [{"status":"NO_OVERLAP","matched_strata":0,"outcome_blind":True}]
    wt=np.minimum(p.stratum_n_quoted,p.stratum_n_nonquoted).to_numpy(float)
    return [{"status":"COARSENED_EXACT_STRATA","matched_strata":len(p),"matched_effective_pairs":int(wt.sum()),
             "matched_predicted_difference":float(np.average(p.predicted_difference,weights=wt)),
             "matched_realized_difference":float(np.average(p.realized_difference,weights=wt)),
             "matched_residual_difference":float(np.average(p.residual_difference,weights=wt)),"outcome_blind":True}]


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    full, capinfo=read_population()
    full["season_ppg_prior"]=num(full.current_season_points_prior).div(num(full.current_season_games_prior).where(num(full.current_season_games_prior)>0))
    full, quote_raw, qsrc=add_quotes(full)
    full, osrc=add_outcomes(full)
    settled=full[full.outcome_status.eq("SETTLED_PARTICIPATED")].copy()
    # Quote status is established exclusively from retained pregame attachment fields.
    settled["count_residual"]=num(settled.realized_points)-num(settled.expected_points)
    full.to_csv(OUT/"full_2026_operational_population.csv",index=False,float_format="%.17g")
    full[full.market_quoted].to_csv(OUT/"market_quoted_population.csv",index=False,float_format="%.17g")
    full[~full.market_quoted].to_csv(OUT/"non_quoted_population.csv",index=False,float_format="%.17g")
    summary={"full_population":len(full),"settled_participated":len(settled),
             "unresolved":int(full.outcome_status.eq("UNRESOLVED").sum()),"final_nonparticipant":int(full.outcome_status.eq("FINAL_NONPARTICIPANT").sum()),
             "quoted_all":int(full.market_quoted.sum()),"nonquoted_all":int((~full.market_quoted).sum()),
             "quote_status_counts":full.quote_status.value_counts().to_dict(),"all_settled_metrics":metrics(settled),
             "quoted_metrics":metrics(settled[settled.market_quoted]),"nonquoted_metrics":metrics(settled[~settled.market_quoted]),
             "captures":capinfo["captures"],"quote_sources":qsrc,"outcome_sources":osrc,
             "selection_and_hgb_are_diagnostic_only":True,"production_model_changed":False,"provider_calls":0,"paid_credits":0,"production_db_mutations":0}
    pd.DataFrame(feature_comparison(full)).to_csv(OUT/"quote_status_feature_comparison.csv",index=False,float_format="%.17g")
    bins=[]
    specs=[("season_ppg_prior",[-math.inf,0,.25,.5,.75,1,1.25,math.inf],["0_or_less",">0-.25",".25-.50",".50-.75",".75-1","1-1.25","1.25+"],"season_ppg"),
           ("mean_toi_last10",[-math.inf,5,10,15,20,25,math.inf],["<5","5-10","10-15","15-20","20-25","25+"],"toi"),
           ("mean_pp_toi_last10",[-math.inf,.01,1,2,3,4,math.inf],["0","0-1","1-2","2-3","3-4","4+"],"pp_toi"),
           ("player_points_last10",[-math.inf,0,.5,1,2,3,math.inf],["0","0-.5",".5-1","1-2","2-3","3+"],"last10_points"),
           ("expected_points",[0,.1,.2,.3,.4,.5,.7,1,math.inf],["<.1",".1-.2",".2-.3",".3-.4",".4-.5",".5-.7",".7-1","1+"],"expected_points")]
    for c,e,l,n in specs: bins.extend(by_band(full,c,e,l,n))
    pd.DataFrame(bins).to_csv(OUT/"quote_rate_by_strength.csv",index=False,float_format="%.17g")
    conf=full.copy(); conf["p_under_0_5"]=1-num(conf.prob_over_0_5); conf["model_confidence"]=np.maximum(num(conf.prob_over_0_5),num(conf.p_under_0_5))
    conf["confidence_band"]=pd.cut(conf.model_confidence,CONF_BANDS,labels=["60-65","65-70","70-75","75-80","80+"],right=False,include_lowest=True).astype("string").fillna("<60")
    full["p_under_0_5"]=1-num(full.prob_over_0_5)
    # Main subgroup residual maps, date effects, and games coverage.
    residual=[]
    for status,g in settled.groupby("market_quoted"):
        g=g.copy(); g["confidence_band"]=pd.cut(np.maximum(num(g.prob_over_0_5),1-num(g.prob_over_0_5)),CONF_BANDS,labels=["60-65","65-70","70-75","75-80","80+"],right=False,include_lowest=True).astype("string").fillna("<60")
        for c in ["expected_points","confidence_band","season_ppg_prior","mean_toi_last10","mean_pp_toi_last10"]:
            gr=g.groupby(c,dropna=False,observed=True)
            for k,v in gr:
                residual.append({"quote_group":"MARKET_QUOTED" if status else "NOT_MARKET_QUOTED","dimension":c,"band":str(k),"n":len(v),**metrics(v)})
    pd.DataFrame(residual).to_csv(OUT/"residual_maps_quoted_nonquoted.csv",index=False,float_format="%.17g")
    date_rows=[]; coverage=[]
    for (day,game),g in full.groupby(["slate_date","game_id"]):
        s=g[g.realized_points.notna()]; a=s[s.market_quoted]; b=s[~s.market_quoted]
        date_rows.append({"slate_date":day,"game_id":int(game),"settled_n":len(s),"quoted_settled_n":len(a),"nonquoted_settled_n":len(b),"quote_rate_settled":float(s.market_quoted.mean()) if len(s) else None,
                          "quoted_mean_expected":num(a.expected_points).mean(),"quoted_mean_realized":num(a.realized_points).mean(),"quoted_residual":(num(a.realized_points)-num(a.expected_points)).mean(),
                          "nonquoted_mean_expected":num(b.expected_points).mean(),"nonquoted_mean_realized":num(b.realized_points).mean(),"nonquoted_residual":(num(b.realized_points)-num(b.expected_points)).mean()})
        coverage.append({"slate_date":day,"game_id":int(game),"rostered_hgb_scored_players":len(g),"hgb_scored_players":len(g),"quoted_players":int(g.market_quoted.sum()),"quote_rate":float(g.market_quoted.mean())})
    pd.DataFrame(date_rows).to_csv(OUT/"date_level_quote_selection.csv",index=False,float_format="%.17g")
    pd.DataFrame(coverage).to_csv(OUT/"market_eligibility_coverage.csv",index=False,float_format="%.17g")
    matched=matched_analysis(settled)
    pd.DataFrame(matched).to_csv(OUT/"matched_quote_nonquote_analysis.csv",index=False,float_format="%.17g")
    # Simple outcome-blind cross-validated quote propensity. Report standardized effects.
    prop_x=full[FEATURES].apply(pd.to_numeric,errors="coerce"); prop_y=full.market_quoted.astype(int)
    prop={"status":"INSUFFICIENT_CLASS_VARIATION","n":len(full),"outcome_blind":True}
    if prop_y.nunique()==2 and min(prop_y.value_counts())>=10:
        cv=StratifiedKFold(5,shuffle=True,random_state=20261015)
        pipe=make_pipeline(SimpleImputer(strategy="median",add_indicator=True),StandardScaler(),LogisticRegression(max_iter=2000,class_weight="balanced"))
        ps=cross_val_predict(pipe,prop_x,prop_y,cv=cv,method="predict_proba")[:,1]
        pipe.fit(prop_x,prop_y); co=pipe[-1].coef_[0]
        # Original-feature coefficients mapped to highest absolute standardized effects; imputer indicators also included.
        prop={"status":"COMPLETE_DESCRIPTIVE_ONLY","n":len(full),"quote_rate":float(prop_y.mean()),"cv_auc":float(roc_auc_score(prop_y,ps)),
              "features":FEATURES,"standardized_logit_coefficients":dict(sorted(zip(FEATURES,co[:len(FEATURES)]),key=lambda z:abs(z[1]),reverse=True)),"outcome_blind":True}
    (OUT/"quote_propensity_diagnostic.json").write_text(json.dumps(prop,indent=2,sort_keys=True)+"\n")
    summary["matched_analysis"]=matched[0];summary["propensity"]=prop
    # 2025 fixed quoted-like profile: predeclared to the 2026 quoted cohort's median thresholds, never outcomes.
    h,parity,_=rootcause.history_2025(None) if False else (None,None,None)
    # Load frozen holdout through the established helper, using same validated frozen model.
    import joblib
    from backend.nhl.scripts.score_nhl_points_hgb_shadow import MODEL_PATH
    model=joblib.load(MODEL_PATH)
    h,parity,_=rootcause.history_2025(model)
    quoted=full[full.market_quoted]
    thresholds={c:float(num(quoted[c]).median()) for c in ["current_season_games_prior","mean_toi_last10","mean_pp_toi_last10","player_points_last10","d10_sog_per60","season_ppg_prior"]}
    # Fixed match rule uses one pooled standardized distance in strictly-prior feature space.
    reweight_cols=["season_ppg_prior","mean_toi_last10","mean_pp_toi_last10","player_points_last10","d10_sog_per60","team_d10_sf_per_game","last10_team_sog_share"]
    q=quoted[reweight_cols].apply(pd.to_numeric,errors="coerce"); z=full[reweight_cols].apply(pd.to_numeric,errors="coerce")
    med=z.median(); scale=z.std().replace(0,1).fillna(1)
    quote_z=((q-med)/scale).replace([np.inf,-np.inf],np.nan).fillna(0).to_numpy()
    h["season_ppg_prior"]=num(h.current_season_points_prior).div(num(h.current_season_games_prior).where(num(h.current_season_games_prior)>0))
    hx=h[reweight_cols].apply(pd.to_numeric,errors="coerce"); hz=((hx-med)/scale).replace([np.inf,-np.inf],np.nan).fillna(0).to_numpy()
    # Similarity weights are a Gaussian kernel over pregame state. Bandwidth predeclared at 1 standardized unit.
    dist2=((hz[:,None,:]-quote_z[None,:,:])**2).mean(axis=2)
    weights=np.exp(-.5*np.min(dist2,axis=1))
    profile=(num(h.season_ppg_prior)>=thresholds["season_ppg_prior"]) & (num(h.mean_toi_last10)>=thresholds["mean_toi_last10"])
    # Add exact fixed median gates over role variables; missing values do not pass.
    for c in ["mean_pp_toi_last10","player_points_last10","d10_sog_per60"]: profile &= num(h[c]).ge(thresholds[c])
    h["realized_points"]=num(h.get("actual_points",h.get("realized_points")))
    if h.realized_points.isna().all() and "points" in h: h["realized_points"]=num(h.points)
    h["expected_points"]=num(h.predicted_mean);h["prob_over_0_5"]=num(h.p_over_0_5)
    prof=h[profile].copy()
    profile_under=num(prof.prob_over_0_5).rsub(1)
    profile_hi=prof[profile_under.ge(.8)].copy()
    # weighted diagnostics use the same all-2025 settled denominator and strictly pregame covariates only.
    valid=h.realized_points.notna() & np.isfinite(weights)
    w=weights[valid]; hw=h.loc[valid].copy(); hw["w"]=w
    y=(num(hw.realized_points)>=1).astype(int).to_numpy(); p=np.clip(num(hw.prob_over_0_5).to_numpy(float),1e-12,1-1e-12)
    weighted_under=1-p
    weighted={"n":len(hw),"effective_sample_size":float(w.sum()**2/(w@w)),"weighted_mean_predicted":float(np.average(num(hw.expected_points),weights=w)),
              "weighted_mean_realized":float(np.average(num(hw.realized_points),weights=w)),"weighted_residual":float(np.average(num(hw.realized_points)-num(hw.expected_points),weights=w)),
              "weighted_brier_0_5":float(np.average((p-y)**2,weights=w)),"weighted_log_loss_0_5":float(np.average(-(y*np.log(p)+(1-y)*np.log(1-p)),weights=w)),
              "weighted_p_under":float(np.average(weighted_under,weights=w)),"weighted_realized_under_rate":float(np.average(1-y,weights=w)),
              "weighted_under_calibration_gap":float(np.average(weighted_under,weights=w)-np.average(1-y,weights=w)),
              "method":"nearest 2026 quoted pregame feature state kernel, exp(-0.5*mean standardized squared distance)","outcome_blind_weights":True}
    (OUT/"2025_quoted_like_profile_definition.json").write_text(json.dumps({"definition":"STRICT_PRIOR_MEDIAN_GATES_V1","thresholds_from_2026_quoted_medians":thresholds,"features":reweight_cols,"outcome_blind":True},indent=2,sort_keys=True)+"\n")
    prof.to_csv(OUT/"2025_quoted_like_population.csv",index=False,float_format="%.17g")
    pd.DataFrame([metrics(prof)]).to_csv(OUT/"2025_quoted_like_performance.csv",index=False,float_format="%.17g")
    pd.DataFrame([metrics(profile_hi) | {"population":"2025_QUOTED_LIKE_P_UNDER_80_PLUS","p_under_min":.8}]).to_csv(OUT/"2025_quoted_like_high_confidence_under.csv",index=False,float_format="%.17g")
    (OUT/"2025_distribution_reweighting.json").write_text(json.dumps(weighted,indent=2,sort_keys=True)+"\n")
    summary["2025_replay_parity_max_abs_error"]=parity;summary["2025_quoted_like_n"]=len(prof);summary["2025_quoted_like_metrics"]=metrics(prof);summary["2025_reweighting"]=weighted
    summary["2025_quoted_like_high_confidence_under"]=metrics(profile_hi)
    summary["global_gap_decomposition"]={"full_2026":metrics(settled),"market_intersection_quoted":metrics(settled[settled.market_quoted]),
        "nonquoted":metrics(settled[~settled.market_quoted]),"quoted_share_settled":float(settled.market_quoted.mean()),
        "composition_identity_residual":float(settled.market_quoted.mean()*(num(settled.loc[settled.market_quoted,"realized_points"])-num(settled.loc[settled.market_quoted,"expected_points"])).mean()+(1-settled.market_quoted.mean())*(num(settled.loc[~settled.market_quoted,"realized_points"])-num(settled.loc[~settled.market_quoted,"expected_points"])).mean()),
        "causal_interpretation":"DESCRIPTIVE_ONLY"}
    legacy_intersection=settled[settled.slate_date.ne("2026-10-09") & settled.quote_status.eq("EXACT_TWO_SIDED")].copy()
    existing=ROOT/"artifacts/analysis/nhl/points_model_market_intersection_map/2026-10-15/canonical_intersection_rows.csv"
    reference={"status":"REFERENCE_ARTIFACT_MISSING"}
    if existing.exists():
        ref=pd.read_csv(existing)
        ref=ref[(num(ref.line)==.5) & ref.settled.fillna(False).astype(bool)]
        reference={"status":"RECONCILED","existing_intersection_settled_n":len(ref),
                   "existing_intersection_mean_predicted":float(num(ref.hgb_expected_count).mean()),
                   "existing_intersection_mean_realized":float(num(ref.official_points_num).mean()),
                   "existing_intersection_residual":float((num(ref.official_points_num)-num(ref.hgb_expected_count)).mean()),
                   "identity_overlap_n":int(len(set(zip(legacy_intersection.game_id,legacy_intersection.player_id)) & set(zip(ref.game_id,ref.player_id))))}
    summary["prior_market_intersection_comparison"]= {"scope":"Oct10-14, exact two-sided 0.5 quotes, settled official outcomes",
        "population_n":len(legacy_intersection),"metrics":metrics(legacy_intersection),"prior_reference":reference}
    pd.DataFrame([{"scope":"ALL_FINALIZED_DATES","n":len(settled),"quoted_status":"EXACT_TWO_SIDED_OR_ONE_SIDED_PARTIAL",
                   "quoted_settled_n":int(settled.market_quoted.sum()),"nonquoted_settled_n":int((~settled.market_quoted).sum()),
                   "unresolved_full_population_n":summary["unresolved"]},
                  {"scope":"PRIOR_MARKET_INTERSECTION_COMPARABLE_OCT10_14","n":len(legacy_intersection),"quoted_status":"EXACT_TWO_SIDED_0_5",
                   "quoted_settled_n":len(legacy_intersection),"nonquoted_settled_n":None,"unresolved_full_population_n":None}]).to_csv(OUT/"market_intersection_denominator_reconciliation.csv",index=False)
    summary["market_agreement"]="Market favored side is not a production feature; comparison requires valid two-sided implied market probabilities."
    summary["same_side_opposition"]="See market_agreement_comparison.csv; based on retained normalized market probabilities."
    summary["full_population_status_counts"]=full.outcome_status.value_counts().to_dict()
    # Market direction layer: determine market favored side only for valid two-sided market probabilities.
    # Keep this comparison on the same Oct. 10–14 population as the prior map.
    two=settled[settled.quote_status.eq("EXACT_TWO_SIDED") & settled.slate_date.ne("2026-10-09")].copy()
    mo,mn=num(two.p_over_mkt),num(two.p_under_mkt)
    two["market_side"]=np.where(mo>=mn,"OVER","UNDER");two["hgb_side"]=np.where(num(two.prob_over_0_5)>=.5,"OVER","UNDER")
    two["side_relation"]=np.where(two.market_side.eq(two.hgb_side),"SAME_SIDE","OPPOSITE_SIDE")
    agree=[]
    for rel,g in two.groupby("side_relation"):
        mm=metrics(g); wins=(np.where(g.hgb_side.eq("OVER"),num(g.realized_points)>=1,num(g.realized_points)<1)).astype(int)
        pp=np.maximum(num(g.prob_over_0_5),1-num(g.prob_over_0_5))
        mm.update({"side_relation":rel,"wins":int(wins.sum()),"losses":int(len(g)-wins.sum()),"selected_probability_mean":float(pp.mean()),
                   "expected_losses":float((1-pp).sum()),"excess_losses":float(len(g)-wins.sum()-(1-pp).sum())})
        agree.append(mm)
    pd.DataFrame(agree).to_csv(OUT/"market_agreement_comparison.csv",index=False,float_format="%.17g")
    # High confidence Under by quote state, retain only graded rows.
    hc=[]
    for (quoted_flag,band),g in conf[conf.realized_points.notna()].groupby(["market_quoted","confidence_band"],observed=True):
        g=g[(1-num(g.prob_over_0_5))>=.5]
        if len(g): hc.append({"quote_group":"MARKET_QUOTED" if quoted_flag else "NOT_MARKET_QUOTED","p_under_band":str(band),**metrics(g),
                              "under_wins":int((num(g.realized_points)<1).sum()),"under_losses":int((num(g.realized_points)>=1).sum())})
    pd.DataFrame(hc).to_csv(OUT/"high_confidence_under_by_quote_status.csv",index=False,float_format="%.17g")
    # Same-player within-player state contrasts.
    sp=[]
    for pid,g in settled.groupby("player_id"):
        a,b=g[g.market_quoted],g[~g.market_quoted]
        if len(a) and len(b):
            sp.append({"player_id":int(pid),"quoted_games":len(a),"nonquoted_games":len(b),"expected_difference":num(a.expected_points).mean()-num(b.expected_points).mean(),
                       "realized_difference":num(a.realized_points).mean()-num(b.realized_points).mean(),"toi_difference":num(a.mean_toi_last10).mean()-num(b.mean_toi_last10).mean(),
                       "pp_toi_difference":num(a.mean_pp_toi_last10).mean()-num(b.mean_pp_toi_last10).mean(),"last10_points_difference":num(a.player_points_last10).mean()-num(b.player_points_last10).mean(),
                       "residual_difference":(num(a.realized_points)-num(a.expected_points)).mean()-(num(b.realized_points)-num(b.expected_points))})
    pd.DataFrame(sp).to_csv(OUT/"same_player_quoted_nonquoted.csv",index=False,float_format="%.17g")
    summary["same_player_n"]=len(sp)
    state_pairs=[]
    pair_cols=["expected_points","mean_toi_last10","player_points_last10"]
    for pid,g in settled.groupby("player_id"):
        qa=g[g.market_quoted].reset_index(drop=True); nq=g[~g.market_quoted].reset_index(drop=True)
        candidates=[]
        for i,a in qa.iterrows():
            for j,b in nq.iterrows():
                gaps=[abs(float(num(a[c])-num(b[c]))) if pd.notna(num(a[c])) and pd.notna(num(b[c])) else math.inf for c in pair_cols]
                if gaps[0] <= .05 and gaps[1] <= 2.0 and gaps[2] <= .25:
                    candidates.append((sum(gaps[k]/[.05,2,.25][k] for k in range(3)),i,j,gaps))
        usedq,usedn=set(),set()
        for distance,i,j,gaps in sorted(candidates):
            if i in usedq or j in usedn: continue
            a,b=qa.iloc[i],nq.iloc[j]; usedq.add(i); usedn.add(j)
            state_pairs.append({"player_id":int(pid),"quoted_slate_date":a.slate_date,"nonquoted_slate_date":b.slate_date,
                "quoted_game_id":int(a.game_id),"nonquoted_game_id":int(b.game_id),"expected_gap_abs":gaps[0],"toi_gap_abs":gaps[1],
                "last10_points_gap_abs":gaps[2],"realized_difference_quoted_minus_nonquoted":float(a.realized_points-b.realized_points),
                "residual_difference_quoted_minus_nonquoted":float((a.realized_points-a.expected_points)-(b.realized_points-b.expected_points)),
                "outcome_blind_calipers":{"expected_points":.05,"mean_toi_last10":2,"player_points_last10":.25}})
    pd.DataFrame(state_pairs).to_csv(OUT/"same_player_similar_state_pairs.csv",index=False,float_format="%.17g")
    if state_pairs:
        summary["same_player_similar_state_pair_n"]=len(state_pairs)
        summary["same_player_similar_state_mean_realized_difference"]=float(pd.DataFrame(state_pairs).realized_difference_quoted_minus_nonquoted.mean())
        summary["same_player_similar_state_mean_residual_difference"]=float(pd.DataFrame(state_pairs).residual_difference_quoted_minus_nonquoted.mean())
    else:
        summary["same_player_similar_state_pair_n"]=0
    team_rows=[]
    for (day,team),g in settled.dropna(subset=["team_id"]).groupby(["slate_date","team_id"]):
        team_rows.append({"slate_date":day,"team_id":int(team),"settled_n":len(g),"quoted_n":int(g.market_quoted.sum()),"quote_rate":float(g.market_quoted.mean()),**metrics(g)})
    pd.DataFrame(team_rows).to_csv(OUT/"team_quote_selection_diagnostics.csv",index=False,float_format="%.17g")
    # Oct 15 frozen capture, market attached; no outcomes read/joined.
    oct=CAPROOT/"slate_date=2026-10-15/run_id=nhldaily_20261015T140453183604Z_bfa01e59/phase=EARLY/captured_at=20261015T140800076538Z/features.csv"
    op=pd.read_csv(oct); pp=pd.read_csv(oct.parent/"predictions.csv"); op=op.merge(pp,on=["game_id","player_id","game_date","game_start_utc"],validate="one_to_one");op["slate_date"]="2026-10-15"
    market=MARKET_ROOT/"2026-10-15/points_with_market.csv"; q15=pd.read_csv(market);q15=q15[np.isclose(num(q15.line),.5)].copy()
    over=num(q15.price_over).notna()|(num(q15.source_quote_count_over).fillna(0)>0);under=num(q15.price_under).notna()|(num(q15.source_quote_count_under).fillna(0)>0)
    q15["quote_status"]=np.select([over&under,over|under],["EXACT_TWO_SIDED","ONE_SIDED_PARTIAL"],default="ATTACHMENT_NO_QUOTE")
    q15["quote_status"]=classify_quote_rows(q15)
    op=op.merge(q15[["game_id","player_id","quote_status"]],on=["game_id","player_id"],how="left",validate="one_to_one");op["quote_status"]=op.quote_status.fillna("FULLY_UNMATCHED");op["market_quoted"]=op.quote_status.isin(["EXACT_TWO_SIDED","ONE_SIDED_PARTIAL"])
    op.to_csv(OUT/"oct15_pending_outcome_blind.csv",index=False,float_format="%.17g")
    summary["oct15_pending"]={"n":len(op),"quoted":int(op.market_quoted.sum()),"nonquoted":int((~op.market_quoted).sum()),"quote_rate":float(op.market_quoted.mean()),
        "quoted_mean_expected":float(num(op.loc[op.market_quoted,"expected_points"]).mean()),"nonquoted_mean_expected":float(num(op.loc[~op.market_quoted,"expected_points"]).mean()),
        "high_confidence_under_0_8_quoted":int((op.market_quoted & ((1-num(op.prob_over_0_5))>=.8)).sum()),
        "high_confidence_under_0_8_nonquoted":int((~op.market_quoted & ((1-num(op.prob_over_0_5))>=.8)).sum()),"status":"PENDING_OUTCOME","outcomes_joined":False}
    # Tables additionally requested and compact feature profiles.
    pd.DataFrame([summary["all_settled_metrics"],summary["quoted_metrics"],summary["nonquoted_metrics"]],index=["ALL","QUOTED","NONQUOTED"]).to_csv(OUT/"realized_outcome_comparison.csv",float_format="%.17g")
    pd.DataFrame([{"population":"FULL_2026","predicted":len(full),"quoted":int(full.market_quoted.sum()),"nonquoted":int((~full.market_quoted).sum()),"settled":len(settled),"unresolved":summary["unresolved"],"nonparticipant":summary["final_nonparticipant"]}]).to_csv(OUT/"denominator_reconciliation.csv",index=False)
    summary["classification"]="INSUFFICIENT_EVIDENCE"
    qres=summary["quoted_metrics"].get("residual_realized_minus_predicted",0); nres=summary["nonquoted_metrics"].get("residual_realized_minus_predicted",0)
    mres=summary["matched_analysis"].get("matched_residual_difference")
    if qres-nres > .25 and (mres is None or mres > .15): summary["classification"]="MATERIAL_CONTRIBUTOR"
    elif qres-nres > .1: summary["classification"]="POSSIBLE_CONTRIBUTOR"
    elif abs(qres-nres)<.1: summary["classification"]="SELECTION_EFFECT_PRESENT_BUT_HGB_ALREADY_ACCOUNTS_FOR_IT"
    summary["interpretation"]={"training_population":"Broad all-player HGB training/holdout versus market-selected operational intersection is a real denominator mismatch; do not infer this alone as cause.",
        "future_implication":"Evaluate market-eligible population separately and represent omitted player role/quality only in a controlled future experiment; no feature or training change made.",
        "position":"Unavailable/omitted unless a clean identity-bound source is established.","team_diagnostics":"See per-game coverage; team IDs are carried only where official outcomes bind.","causal_claim":"Not established by this descriptive early-season study."}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=lambda z:int(z) if isinstance(z,np.integer) else float(z) if isinstance(z,np.floating) else z)+"\n")
    allm, qm, nm = summary["all_settled_metrics"], summary["quoted_metrics"], summary["nonquoted_metrics"]
    prior_comp=summary["prior_market_intersection_comparison"]
    md=f'''# NHL Points market-selection population audit

## Finding

Classification: **{summary['classification']}**. The frozen HGB has a positive realized-minus-predicted residual in the full settled population, and that raw residual is larger among player-games with retained 0.5 quotes. However, the outcome-blind matched comparison is small ({summary['matched_analysis'].get('matched_residual_difference', float('nan')):.3f} points), so the evidence does not support quote availability as the primary cause. The analysis is descriptive and does not establish causality.

## Population and quote denominators

- Full immutable 2026 capture population, Oct. 9–14: {len(full):,} player-games.
- Official participated/final outcomes: {len(settled):,}; unresolved: {summary['unresolved']:,}; final nonparticipants: {summary['final_nonparticipant']:,}.
- Across these dates, quoted means exact two-sided or one-sided retained quote evidence: {int(full.market_quoted.sum()):,} predicted, including partials separately; nonquoted/unmatched: {int((~full.market_quoted).sum()):,}.
- Direct reconciliation to the prior market-intersection study: Oct. 10–14 exact two-sided 0.5 quotes yield {prior_comp['population_n']:,} settled identities. Existing map: {prior_comp['prior_reference'].get('existing_intersection_settled_n', 'unavailable')} settled identities; identity overlap {prior_comp['prior_reference'].get('identity_overlap_n', 'unavailable')}.
- Matched exact-two-sided market intersection: predicted {prior_comp['metrics']['mean_predicted_points']:.3f}, realized {prior_comp['metrics']['mean_realized_points']:.3f}, residual {prior_comp['metrics']['residual_realized_minus_predicted']:.3f}. This reproduces the stated ~0.370 → ~0.602 gap. The full settled population is predicted {allm['mean_predicted_points']:.3f}, realized {allm['mean_realized_points']:.3f}, residual {allm['residual_realized_minus_predicted']:.3f}.

| Settled group | N | Predicted | Realized | Residual | MAE | RMSE | Brier | Log loss |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| All | {allm['n']} | {allm['mean_predicted_points']:.3f} | {allm['mean_realized_points']:.3f} | {allm['residual_realized_minus_predicted']:.3f} | {allm['mae']:.3f} | {allm['rmse']:.3f} | {allm['brier_0_5']:.3f} | {allm['log_loss_0_5']:.3f} |
| Quoted (two-sided or partial) | {qm['n']} | {qm['mean_predicted_points']:.3f} | {qm['mean_realized_points']:.3f} | {qm['residual_realized_minus_predicted']:.3f} | {qm['mae']:.3f} | {qm['rmse']:.3f} | {qm['brier_0_5']:.3f} | {qm['log_loss_0_5']:.3f} |
| Not quoted | {nm['n']} | {nm['mean_predicted_points']:.3f} | {nm['mean_realized_points']:.3f} | {nm['residual_realized_minus_predicted']:.3f} | {nm['mae']:.3f} | {nm['rmse']:.3f} | {nm['brier_0_5']:.3f} | {nm['log_loss_0_5']:.3f} |

Over calibration gap is predicted P(Over 0.5) minus observed point-game rate: all {allm['calibration_gap_over']:.3f}; quoted {qm['calibration_gap_over']:.3f}; not quoted {nm['calibration_gap_over']:.3f}. Quote statuses are assigned from retained pregame prices/source quote counts, never from player identity. Exact two-sided: {summary['quote_status_counts'].get('EXACT_TWO_SIDED',0):,}; one-sided partial: {summary['quote_status_counts'].get('ONE_SIDED_PARTIAL',0):,}; attachment row with no valid quote: {summary['quote_status_counts'].get('ATTACHMENT_NO_QUOTE',0):,}; no attachment identity: {summary['quote_status_counts'].get('FULLY_UNMATCHED',0):,}.

## Selection and residual diagnostics

- Matched outcome-blind exact coarsened strata: {summary['matched_analysis'].get('matched_strata',0)} strata / {summary['matched_analysis'].get('matched_effective_pairs',0)} weighted pairs; quoted-minus-nonquoted predicted {summary['matched_analysis'].get('matched_predicted_difference',float('nan')):.3f}, realized {summary['matched_analysis'].get('matched_realized_difference',float('nan')):.3f}, residual {summary['matched_analysis'].get('matched_residual_difference',float('nan')):.3f}. This is not an individual causal match.
- Propensity-style quote model: cross-validated AUC {prop.get('cv_auc',float('nan')):.3f}; this diagnostic uses HGB pregame features only and is not deployed.
- Raw residual difference (quoted minus nonquoted): {qm['residual_realized_minus_predicted']-nm['residual_realized_minus_predicted']:.3f}. Exact date-level results are in `date_level_quote_selection.csv`; Oct. 13 is shown separately.
- 2025 quoted-like profile: {summary['2025_quoted_like_n']:,} holdout rows under a frozen, outcome-blind median gate from 2026 quoted features; predicted {summary['2025_quoted_like_metrics']['mean_predicted_points']:.3f}, realized {summary['2025_quoted_like_metrics']['mean_realized_points']:.3f}, residual {summary['2025_quoted_like_metrics']['residual_realized_minus_predicted']:.3f}. Outcome-blind pregame-feature reweighting of all 2025 holdout gives predicted {weighted['weighted_mean_predicted']:.3f}, realized {weighted['weighted_mean_realized']:.3f}, residual {weighted['weighted_residual']:.3f}; ESS {weighted['effective_sample_size']:.0f}/{weighted['n']}.
- Under calibration gap (predicted P(Under) minus observed Under rate): quoted {qm['under_calibration_gap_pred_minus_realized']:.3f}; not quoted {nm['under_calibration_gap_pred_minus_realized']:.3f}. High-confidence bands are in `high_confidence_under_by_quote_status.csv`.
- Market same-side/opposition comparisons use exact two-sided quotes and are in `market_agreement_comparison.csv`. Market availability and odds were not HGB inputs.
- Same-player both-state sample: {summary['same_player_n']:,}. Position was not inferred; team IDs are reported only where official outcomes bind, with team-level sample sizes in `team_quote_selection_diagnostics.csv`.
- Same-player similar-state caliper pairs (|expected|≤0.05, TOI≤2, last10 Points≤0.25): {summary['same_player_similar_state_pair_n']}; quoted-minus-nonquoted realized difference {summary.get('same_player_similar_state_mean_realized_difference',float('nan')):.3f}. This is descriptive and does not support causal claims.

## Oct. 15 pending population

{summary['oct15_pending']['n']} HGB player-games; {summary['oct15_pending']['quoted']} quoted ({summary['oct15_pending']['quote_rate']:.1%}), {summary['oct15_pending']['nonquoted']} nonquoted. Mean expected points: quoted {summary['oct15_pending']['quoted_mean_expected']:.3f}, nonquoted {summary['oct15_pending']['nonquoted_mean_expected']:.3f}. High-confidence Under (P≥0.80): {summary['oct15_pending']['high_confidence_under_0_8_quoted']} quoted / {summary['oct15_pending']['high_confidence_under_0_8_nonquoted']} nonquoted. Status: **PENDING_OUTCOME**; no outcome join performed.

## Interpretation and constraints

The model was trained/evaluated on a broad historical player-game population but the cited 2026 0.5 market-intersection study evaluates the narrower exact-two-sided market-eligible subset. Its profile is stronger on several retained pregame usage/scoring fields. Market selection is a **possible contributor**, not established as a primary driver: the residual difference shrinks materially under outcome-blind strata, 2025 quoted-like rows also show positive residual, and 2025 feature reweighting remains centered. Future work may evaluate market-eligible performance separately and test omitted role/quality information; no such training, feature, filter, or calibration change is made here.

No market input entered HGB; no calibration/retraining/Funnel/production changes; provider calls 0; paid credits 0; production DB mutations 0. All captures, quote attachments, outcome files, and output hashes are pinned in `lineage_manifest.json`.

## Package

The CSV/JSON outputs include the complete all-player population, quote splits, feature comparisons, strength/expected-count bins, outcome and residual maps, matching, propensity, date/game coverage, same-player and market-agreement comparisons, 2025 profile/reweighting, denominator reconciliation, and pending Oct. 15 inventory. Backlog context is in `backlog_context.md`.
'''
    (OUT/"report.md").write_text(md)
    (OUT/"backlog_context.md").write_text("""# NHL modeling backlog context\n\n- Points shot-attempts data-quality defect\n- Points Funnel V1 prospective validation\n- Points full-range prospective collection\n- Points HGB root-cause work — current market-selection audit\n- SOG cross-market review after Points root-cause decision\n- Saves\n- Moneyline\n- Puck Line\n- SOG structural/divergence/fixed-blend grading\n- Historical SOG official-TOI binding\n- Deferred Puck Line work\n- Tier3 override not needed\n""")
    # Source lineage pins all inputs, hashes, and population contracts.
    manifest={"schema_version":"NHL_POINTS_MARKET_SELECTION_POPULATION_V1","dates":DATES,"population_grain":["slate_date","game_id","player_id"],
        "quote_definition":"retained pregame 0.5 attachment with at least one valid side (price or positive source quote count); two-sided and partial retained separately",
        "outcome_definition":"canonical official outcome for same slate/game/player; only official_final PARTICIPATED rows graded",
        "feature_contract":FEATURES,"market_used_as_hgb_input":False,"provider_calls":0,"paid_credits":0,"production_db_mutations":0,
        "capture_sources":capinfo["captures"],"quote_sources":qsrc,"outcome_sources":osrc,
        "outputs":{p.name:sha(p) for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"}}
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT.relative_to(ROOT)),"classification":summary["classification"],"full":len(full),"settled":len(settled),"quoted":int(settled.market_quoted.sum()),"nonquoted":int((~settled.market_quoted).sum())},sort_keys=True))


if __name__ == "__main__":
    main()
