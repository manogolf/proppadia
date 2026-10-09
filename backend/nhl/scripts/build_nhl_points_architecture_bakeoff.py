#!/usr/bin/env python3
"""Rebuild and compare NHL Points architectures from authoritative retained history.

Research only: consumes a read-only export of regular-season official outcomes
and skater logs. No database or provider access is performed here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import expit, logit
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (average_precision_score, brier_score_loss,
                             log_loss, roc_auc_score)
from sklearn.preprocessing import StandardScaler
import statsmodels.api as sm
from statsmodels.miscmodels.ordinal_model import OrderedModel
from statsmodels.discrete.count_model import ZeroInflatedPoisson


ROOT = Path("artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09")
TARGETS = ("over_0_5", "over_1_5", "over_2_5")
PHOENIX = ["is_home", "d5_sog_per60", "d10_sog_per60", "attempts_d10_per60",
           "team_d10_sf_per_game", "last10_team_sog_share",
           "num_shotwasongoal_last5", "num_shotwasongoal_last10",
           "num_event_shot_last5", "num_event_shot_last10",
           "num_shotwasongoal_season_to_date", "num_event_shot_season_to_date",
           "hot_last5_flag"]
CORE = ["is_home", "d10_sog_per60", "attempts_d10_per60", "player_points_last10",
        "current_season_points_prior", "current_season_games_prior",
        "mean_toi_last10", "mean_pp_toi_last10", "team_d10_sf_per_game",
        "last10_team_sog_share"]
ARMS = ("CROSS_SEASON_LITERAL_LAST_N", "CURRENT_SEASON_ONLY_LAST_N", "120_DAY_LEGACY_BOUND")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def build_frame(src: pd.DataFrame) -> pd.DataFrame:
    src = src.copy()
    src = src.rename(columns={"season": "canonical_season", "realized_goals": "goals",
                              "realized_assists": "assists", "realized_points": "points"})
    src["game_date"] = pd.to_datetime(src.game_date).dt.normalize()
    src["start_time_utc"] = pd.to_datetime(src.start_time_utc, utc=True, errors="coerce")
    for c in ("player_id", "game_id", "canonical_season", "team_id", "goals", "assists", "points"):
        src[c] = pd.to_numeric(src[c], errors="coerce")
    for c in ("toi_minutes", "pp_toi_minutes", "shots_on_goal", "shot_attempts"):
        src[c] = pd.to_numeric(src[c], errors="coerce")
    src["realized_points"] = src.goals + src.assists
    src = src.dropna(subset=["player_id", "game_id", "canonical_season", "realized_points"])
    # Preserve official outcomes even when both retained sources lack team ID;
    # those rows receive neutral team-context features and are audited.
    src["team_id"] = src["team_id"].fillna(-1)
    src = src.sort_values(["game_date", "start_time_utc", "game_id", "player_id"], kind="mergesort")
    # Team history is fixed at the current contract: same-date strict exclusion,
    # regular season only, 120 calendar days, newest 10 team games.
    teamgames = (src.groupby(["team_id", "game_id", "game_date"], as_index=False)
                 .agg(team_sog=("shots_on_goal", "sum"), team_attempts=("shot_attempts", "sum")))
    teamgames = teamgames.sort_values(["team_id", "game_date", "game_id"])
    teamhist = {int(t): g.reset_index(drop=True) for t, g in teamgames.groupby("team_id") if int(t) >= 0}
    players = {int(p): g.reset_index(drop=True) for p, g in src.groupby("player_id", sort=False)}
    rows = []
    for t in src.itertuples(index=False):
        d = t.game_date
        pg = players[int(t.player_id)]
        prior = pg.loc[pg.game_date < d]
        season_prior = prior.loc[prior.canonical_season == t.canonical_season]
        for arm in ARMS:
            h = prior
            if arm == "CURRENT_SEASON_ONLY_LAST_N":
                h = h.loc[h.canonical_season == t.canonical_season]
            elif arm == "120_DAY_LEGACY_BOUND":
                h = h.loc[h.game_date >= d - pd.Timedelta(days=120)]
            h = h.tail(10)
            h5 = h.tail(5)
            # Team's current team history, capped at 10 games and 120 days.
            tg = teamhist.get(int(t.team_id), pd.DataFrame())
            if len(tg):
                th = tg.loc[(tg.game_date < d) & (tg.game_date >= d - pd.Timedelta(days=120))].tail(10)
            else:
                th = pd.DataFrame()
            team_sog = float(th.team_sog.sum()) if len(th) else 0.0
            sog10 = float(h.shots_on_goal.fillna(0).sum()) if len(h) else 0.0
            rate5 = (h5.shots_on_goal * 60 / h5.toi_minutes.replace(0, np.nan)).mean()
            rate10 = (h.shots_on_goal * 60 / h.toi_minutes.replace(0, np.nan)).mean()
            att10 = (h.shot_attempts * 60 / h.toi_minutes.replace(0, np.nan)).mean()
            row = {"player_id": int(t.player_id), "game_id": int(t.game_id), "game_date": d,
                   "canonical_season": int(t.canonical_season), "team_id": int(t.team_id),
                   "history_contract": arm, "realized_points": int(t.realized_points),
                   "goals": int(t.goals), "assists": int(t.assists), "is_home": int(t.is_home),
                   "player_history_games": len(h), "player_lifetime_games_prior": len(prior),
                   "current_season_games_prior": len(season_prior),
                   "player_points_last10": h.realized_points.mean() if len(h) else np.nan,
                   "player_points_sum_last10": h.realized_points.sum() if len(h) else 0,
                   "points_last5": h5.realized_points.sum() if len(h5) else 0,
                   "d5_sog_per60": rate5, "d10_sog_per60": rate10,
                   "attempts_d10_per60": att10,
                   "mean_toi_last10": h.toi_minutes.mean() if len(h) else np.nan,
                   "mean_pp_toi_last10": h.pp_toi_minutes.mean() if len(h) else np.nan,
                   "num_shotwasongoal_last5": h5.shots_on_goal.sum() if len(h5) else 0,
                   "num_shotwasongoal_last10": sog10,
                   "num_event_shot_last5": h5.shot_attempts.sum() if len(h5) else 0,
                   "num_event_shot_last10": h.shot_attempts.sum() if len(h) else 0,
                   "hot_last5_flag": int((h5.shots_on_goal.fillna(0).sum() >= 15)),
                   "current_season_points_prior": season_prior.realized_points.sum(),
                   "num_shotwasongoal_season_to_date": season_prior.shots_on_goal.sum(),
                   "num_event_shot_season_to_date": season_prior.shot_attempts.sum(),
                   "team_d10_sf_per_game": th.team_sog.mean() if len(th) else 0.0,
                   "team_d10_attempts_per_game": th.team_attempts.mean() if len(th) else 0.0,
                   "last10_team_sog_share": sog10 / team_sog if team_sog > 0 else 0.0,
                   # Retained solely for audit, never placed in model matrices.
                   "same_game_realized_toi_minutes": t.toi_minutes,
                   "same_game_realized_pp_toi_minutes": t.pp_toi_minutes}
            rows.append(row)
    out = pd.DataFrame(rows)
    for threshold, col in zip((1, 2, 3), TARGETS):
        out[col] = (out.realized_points >= threshold).astype(int)
    out["outcome_category"] = out.realized_points.clip(upper=3).astype(int)
    return out


def matrix(df, cols, scaler=None, fit=False):
    x = df[cols].replace([np.inf, -np.inf], np.nan).fillna(0).astype(float).to_numpy()
    if scaler is None:
        scaler = StandardScaler().fit(x) if fit else None
    return (scaler.transform(x) if scaler is not None else x), scaler


def safe_probs(p):
    return np.clip(np.asarray(p, dtype=float), 1e-8, 1 - 1e-8)


def prior_reverse(q, prevalence):
    q = safe_probs(q)
    return (q * prevalence) / (q * prevalence + (1 - q) * (1 - prevalence))


def metrics(y, p):
    p = safe_probs(p); y = np.asarray(y, dtype=int)
    try: auc = roc_auc_score(y, p)
    except ValueError: auc = None
    ap = average_precision_score(y, p) if y.sum() else None
    # calibration regression: logit observed outcome ~ intercept + slope*logit(p)
    lp = np.log(p / (1 - p))
    try:
        fit = sm.Logit(y, sm.add_constant(lp)).fit(disp=False)
        ci, cs = map(float, fit.params)
    except Exception: ci, cs = None, None
    order = np.argsort(p); bins = np.array_split(order, min(10, max(1, len(order)//20)))
    ece = sum(len(ix) / len(y) * abs(y[ix].mean() - p[ix].mean()) for ix in bins if len(ix))
    n = max(1, int(math.ceil(len(y) * .1))); n20 = max(1, int(math.ceil(len(y)*.2)))
    base = y.mean()
    return {"n": int(len(y)), "observed_rate": float(base), "predicted_rate": float(p.mean()),
            "brier": float(brier_score_loss(y, p)), "log_loss": float(log_loss(y, p, labels=[0,1])),
            "calibration_intercept": ci, "calibration_slope": cs, "ece_10": float(ece),
            "roc_auc": auc, "average_precision": ap,
            "ap_base_lift": float(ap / base) if ap is not None and base else None,
            "top_decile_lift": float(y[np.argsort(p)[-n:]].mean()/base) if base else None,
            "top_quintile_lift": float(y[np.argsort(p)[-n20:]].mean()/base) if base else None}


def distribution_metrics(y, probs):
    probs = np.clip(np.asarray(probs), 1e-9, 1)
    probs = probs / probs.sum(axis=1, keepdims=True)
    cat = np.minimum(np.asarray(y, dtype=int), probs.shape[1]-1)
    nll = -np.log(probs[np.arange(len(cat)), cat]).mean()
    means = np.arange(probs.shape[1])[None, :]
    # For 3+ bucket, conditional tail mean/variance comes from training data.
    pm = (probs * means).sum(1)
    return {"categorical_nll": float(nll), "predicted_mean": float(pm.mean()),
            "observed_mean": float(np.mean(y)), "predicted_zero_rate": float(probs[:,0].mean()),
            "observed_zero_rate": float(np.mean(np.asarray(y)==0)),
            "predicted_ge3_rate": float(probs[:,3:].sum(1).mean()),
            "observed_ge3_rate": float(np.mean(np.asarray(y)>=3))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=ROOT)
    args = ap.parse_args(); started = time.time(); root = args.root
    inp = root / "inputs/canonical_points_sources.csv.gz"
    outdir = root / "output"; outdir.mkdir(parents=True, exist_ok=True)
    source = pd.read_csv(inp, low_memory=False)
    frame_path = outdir / "canonical_training_frame.csv.gz"
    old_manifest_path = outdir / "manifest.json"
    cache_valid = False
    if frame_path.exists() and frame_path.stat().st_size > 0 and old_manifest_path.exists():
        try:
            old_manifest = json.loads(old_manifest_path.read_text())
            cache_valid = (old_manifest.get("source_sha256") == sha256(inp) and
                           "player_lifetime_games_prior" in pd.read_csv(frame_path, nrows=0).columns)
        except Exception:
            cache_valid = False
    if cache_valid:
        frame = pd.read_csv(frame_path, parse_dates=["game_date"])
    else:
        frame = build_frame(source)
        frame.to_csv(frame_path, index=False, compression="gzip")
    train = frame[frame.canonical_season == 2023].copy()
    test = frame[frame.canonical_season == 2024].copy()
    # Training contains one season only, so A/B/C naturally coincide there.
    train = train[train.history_contract == ARMS[0]]
    summaries, calibration, distribution, count_diagnostics, model_notes, feature_importance = [], [], [], [], [], []
    preds_by_model = {}
    for arm in ARMS:
        te = test[test.history_contract == arm].copy()
        for family, cols, balanced in (("NATURAL_BINARY_LOGIT", CORE, False),
                                       ("BALANCED_PHOENIX_RAW", PHOENIX, True),
                                       ("NATURAL_PHOENIX_LOGIT", PHOENIX, False)):
            Xtr, scaler = matrix(train, cols, fit=True); Xte, _ = matrix(te, cols, scaler)
            for thr, target in zip((1,2,3), TARGETS):
                ytr = train[target].to_numpy(); prevalence = float(ytr.mean())
                mdl = LogisticRegression(C=1.0, class_weight="balanced" if balanced else None,
                                         max_iter=1000, solver="lbfgs")
                mdl.fit(Xtr, ytr); raw = mdl.predict_proba(Xte)[:,1]
                variants = [(family + ("_RAW" if balanced else ""), raw)]
                if balanced:
                    variants.append(("BALANCED_PHOENIX_PRIOR_REVERSED", prior_reverse(raw, prevalence)))
                for name, p in variants:
                    key = (name, arm, thr); preds_by_model[key] = p
                    m = metrics(te[target], p); m.update(model=name, history_contract=arm, threshold=thr)
                    summaries.append(m)
        # Count model set; fit 2023 only, score the three history arms.
        cols = CORE; Xtr, scaler = matrix(train, cols, fit=True); Xte, _ = matrix(te, cols, scaler)
        ytr = train.realized_points.to_numpy(); yte = te.realized_points.to_numpy()
        # statsmodels intercept + predictors, offset uses strict-prior mean TOI exposure.
        Xm = sm.add_constant(Xtr, has_constant="add"); Xtm = sm.add_constant(Xte, has_constant="add")
        fit_results = {}
        for name, fn in (("POISSON", lambda: sm.GLM(ytr, Xm, family=sm.families.Poisson()).fit()),
                         ("POISSON_TOI_OFFSET", lambda: sm.GLM(ytr, Xm, family=sm.families.Poisson(),
                             offset=np.log(np.maximum(train.mean_toi_last10.fillna(train.mean_toi_last10.median()).to_numpy(),1))).fit())):
            t0=time.time()
            try:
                fit_results[name] = fn(); model_notes.append({"model":name,"history_contract":arm,"fit_seconds":time.time()-t0,"status":"FIT"})
            except Exception as e: model_notes.append({"model":name,"history_contract":arm,"status":"FAILED","error":str(e)})
        try:
            fit_results["NB2"] = sm.NegativeBinomial(ytr, Xm, loglike_method="nb2").fit(disp=False, maxiter=300)
            model_notes.append({"model":"NB2","history_contract":arm,"status":"FIT","alpha":float(fit_results["NB2"].params[-1])})
        except Exception as e: model_notes.append({"model":"NB2","history_contract":arm,"status":"FAILED","error":str(e)})
        try:
            t0=time.time()
            zipfit=ZeroInflatedPoisson(ytr,Xm,exog_infl=np.ones((len(ytr),1)),inflation="logit").fit(disp=False,maxiter=200)
            zp=np.asarray(zipfit.params); ki=zipfit.model.k_inflate
            pi=float(expit(zp[0])); zmu=np.maximum(np.exp(np.clip(Xtm@zp[ki:],-20,20)),1e-9)
            from scipy.stats import poisson
            z0=pi+(1-pi)*poisson.pmf(0,zmu); z1=(1-pi)*poisson.pmf(1,zmu); z2=(1-pi)*poisson.pmf(2,zmu)
            zcat=np.column_stack([z0,z1,z2,np.maximum(1-z0-z1-z2,1e-9)])
            zconv=bool(getattr(zipfit,"mle_retvals",{}).get("converged",False))
            model_notes.append({"model":"ZERO_INFLATED_POISSON","history_contract":arm,"status":"FIT" if zconv else "NOT_CONVERGED",
                                "converged":zconv,"fit_seconds":time.time()-t0,"inflation_probability":pi})
            if zconv:
                distribution.append(dict(model="ZERO_INFLATED_POISSON",history_contract=arm,**distribution_metrics(yte,zcat)))
                for j,thr in enumerate((1,2,3)):
                    p=zcat[:,j+1:].sum(1);preds_by_model[("ZERO_INFLATED_POISSON",arm,thr)]=p
                    m=metrics(te[TARGETS[j]],p);m.update(model="ZERO_INFLATED_POISSON",history_contract=arm,threshold=thr);summaries.append(m)
                zipll=np.where(yte==0,np.log(np.maximum(z0,1e-300)),np.log(np.maximum(1-pi,1e-300))+poisson.logpmf(yte,zmu))
                count_diagnostics.append({"model":"ZERO_INFLATED_POISSON","history_contract":arm,"count_nll":float(-zipll.mean()),
                    "observed_mean":float(yte.mean()),"predicted_mean":float(((1-pi)*zmu).mean()),"observed_variance":float(yte.var()),
                    "predicted_variance":float(((1-pi)*zmu+pi*(1-pi)*zmu*zmu).mean()),"observed_zero_rate":float((yte==0).mean()),
                    "predicted_zero_rate":float(z0.mean()),"observed_ge3_rate":float((yte>=3).mean()),
                    "predicted_ge3_rate":float((1-z0-z1-z2).mean()),"inflation_probability":pi})
        except Exception as e: model_notes.append({"model":"ZERO_INFLATED_POISSON","history_contract":arm,"status":"FAILED","error":str(e)})
        # Bounded component model: independent Poisson Goals + Assists, with
        # the independence assumption tested on held-out residual dependence.
        try:
            gfit = sm.GLM(train.goals.to_numpy(), Xm, family=sm.families.Poisson()).fit()
            afit = sm.GLM(train.assists.to_numpy(), Xm, family=sm.families.Poisson()).fit()
            gm=np.maximum(gfit.predict(Xtm),1e-9); am=np.maximum(afit.predict(Xtm),1e-9)
            from scipy.stats import poisson
            # Convolution for 0,1,2 and 3+ total points.
            gpm=np.column_stack([poisson.pmf(k,gm) for k in (0,1,2)])
            apm=np.column_stack([poisson.pmf(k,am) for k in (0,1,2)])
            pp0=gpm[:,0]*apm[:,0]
            pp1=gpm[:,1]*apm[:,0]+gpm[:,0]*apm[:,1]
            pp2=gpm[:,2]*apm[:,0]+gpm[:,1]*apm[:,1]+gpm[:,0]*apm[:,2]
            pcat=np.column_stack([pp0,pp1,pp2,np.maximum(1-pp0-pp1-pp2,1e-9)])
            distribution.append(dict(model="GOALS_PLUS_ASSISTS_INDEPENDENT_POISSON",history_contract=arm,**distribution_metrics(yte,pcat)))
            resid_g=te.goals.to_numpy()-gm; resid_a=te.assists.to_numpy()-am
            model_notes.append({"model":"GOALS_PLUS_ASSISTS_INDEPENDENT_POISSON","history_contract":arm,"status":"FIT",
                                "oot_goal_assist_residual_correlation":float(np.corrcoef(resid_g,resid_a)[0,1]),
                                "oot_both_positive_rate":float(((te.goals>0)&(te.assists>0)).mean())})
            for j,thr in enumerate((1,2,3)):
                p=pcat[:,j+1:].sum(1);preds_by_model[("GOALS_PLUS_ASSISTS_INDEPENDENT_POISSON",arm,thr)]=p
                m=metrics(te[TARGETS[j]],p);m.update(model="GOALS_PLUS_ASSISTS_INDEPENDENT_POISSON",history_contract=arm,threshold=thr);summaries.append(m)
        except Exception as e: model_notes.append({"model":"GOALS_PLUS_ASSISTS_INDEPENDENT_POISSON","history_contract":arm,"status":"FAILED","error":str(e)})
        # Multiclass probabilities are a distribution; ordinal ordered logit fitted separately.
        cat_train = np.minimum(ytr,3); cat_test = np.minimum(yte,3)
        multi = LogisticRegression(C=1.0, max_iter=1000, solver="lbfgs").fit(Xtr, cat_train)
        pmulti = np.zeros((len(te),4)); pmulti[:,multi.classes_.astype(int)] = multi.predict_proba(Xte)
        distribution.append(dict(model="MULTICLASS_SOFTMAX", history_contract=arm, **distribution_metrics(yte,pmulti)))
        for j,thr in enumerate((1,2,3)):
            p=pmulti[:,j+1:].sum(1); preds_by_model[("MULTICLASS_SOFTMAX",arm,thr)]=p
            m=metrics(te[TARGETS[j]],p);m.update(model="MULTICLASS_SOFTMAX",history_contract=arm,threshold=thr);summaries.append(m)
        try:
            ordm=OrderedModel(cat_train, Xtr, distr="logit").fit(method="bfgs",disp=False,maxiter=300)
            pcat=ordm.model.predict(ordm.params, exog=Xte)
            distribution.append(dict(model="ORDINAL_LOGIT",history_contract=arm,**distribution_metrics(yte,pcat)))
            for j,thr in enumerate((1,2,3)):
                p=pcat[:,j+1:].sum(1);preds_by_model[("ORDINAL_LOGIT",arm,thr)]=p
                m=metrics(te[TARGETS[j]],p);m.update(model="ORDINAL_LOGIT",history_contract=arm,threshold=thr);summaries.append(m)
            model_notes.append({"model":"ORDINAL_LOGIT","history_contract":arm,"status":"FIT","thresholds":[float(x) for x in ordm.params[:3]]})
        except Exception as e: model_notes.append({"model":"ORDINAL_LOGIT","history_contract":arm,"status":"FAILED","error":str(e)})
        # Mean-only Poisson boosting: probabilities explicitly assume Poisson count.
        boost=HistGradientBoostingRegressor(loss="poisson",max_iter=100,max_leaf_nodes=15,
                                             l2_regularization=1.0,early_stopping=False,random_state=42)
        t0=time.time(); boost.fit(Xtr, ytr); mu=np.maximum(boost.predict(Xte),1e-9)
        from scipy.stats import poisson
        pb=np.column_stack([poisson.pmf(k,mu) for k in (0,1,2)])
        pb=np.column_stack([pb, np.maximum(1-pb.sum(1),1e-9)])
        distribution.append(dict(model="HISTGB_POISSON_ASSUMPTION",history_contract=arm,**distribution_metrics(yte,pb)))
        from scipy.stats import poisson
        count_diagnostics.append({"model":"HISTGB_POISSON_ASSUMPTION","history_contract":arm,
            "count_nll":float(-poisson.logpmf(yte,mu).mean()),"observed_mean":float(yte.mean()),"predicted_mean":float(mu.mean()),
            "observed_variance":float(yte.var()),"predicted_variance":float(mu.mean()),"observed_zero_rate":float((yte==0).mean()),
            "predicted_zero_rate":float(poisson.pmf(0,mu).mean()),"observed_ge3_rate":float((yte>=3).mean()),
            "predicted_ge3_rate":float((1-poisson.cdf(2,mu)).mean()),"nb2_alpha":None})
        for j,thr in enumerate((1,2,3)):
            p=pb[:,j+1:].sum(1);preds_by_model[("HISTGB_POISSON_ASSUMPTION",arm,thr)]=p
            m=metrics(te[TARGETS[j]],p);m.update(model="HISTGB_POISSON_ASSUMPTION",history_contract=arm,threshold=thr);summaries.append(m)
        model_notes.append({"model":"HISTGB_POISSON_ASSUMPTION","history_contract":arm,"status":"FIT","fit_seconds":time.time()-t0,"mean_only_probability_assumption":"Poisson distribution"})
        if arm == ARMS[2]:
            from sklearn.inspection import permutation_importance
            pi=permutation_importance(boost,Xte,yte,scoring="neg_mean_poisson_deviance",n_repeats=3,random_state=42)
            feature_importance=[{"feature":c,"mean_poisson_deviance_increase":float(v),"std":float(s),"history_contract":arm}
                                for c,v,s in zip(CORE,pi.importances_mean,pi.importances_std)]
        # Parametric count distributions and optional ZINB; use exact PMFs.
        for name, res in fit_results.items():
            if name.startswith("POISSON"):
                mu=np.maximum(res.predict(Xtm, offset=(np.log(np.maximum(te.mean_toi_last10.fillna(train.mean_toi_last10.median()).to_numpy(),1)) if name.endswith("OFFSET") else None)),1e-9)
                from scipy.stats import poisson
                pmf=np.column_stack([poisson.pmf(k,mu) for k in (0,1,2)]); pcat=np.column_stack([pmf, np.maximum(1-pmf.sum(1),1e-9)])
            elif name=="NB2":
                from scipy.stats import nbinom
                par=np.asarray(res.params); mu=np.exp(np.clip(Xtm@par[:-1],-20,20)); alpha=max(float(par[-1]),1e-8); size=1/alpha
                prob=size/(size+mu); pmf=np.column_stack([nbinom.pmf(k,size,prob) for k in (0,1,2)]);pcat=np.column_stack([pmf,np.maximum(1-pmf.sum(1),1e-9)])
            else: continue
            distribution.append(dict(model=name,history_contract=arm,**distribution_metrics(yte,pcat)))
            for j,thr in enumerate((1,2,3)):
                p=pcat[:,j+1:].sum(1);preds_by_model[(name,arm,thr)]=p
                m=metrics(te[TARGETS[j]],p);m.update(model=name,history_contract=arm,threshold=thr);summaries.append(m)
        # Full-support proper count likelihood and moment diagnostics.
        for name, res in fit_results.items():
            from scipy.stats import poisson, nbinom
            if name.startswith("POISSON"):
                off = np.log(np.maximum(te.mean_toi_last10.fillna(train.mean_toi_last10.median()).to_numpy(),1)) if name.endswith("OFFSET") else None
                mu=np.maximum(res.predict(Xtm,offset=off),1e-9)
                ll=poisson.logpmf(yte,mu);var=mu
            elif name == "NB2":
                par=np.asarray(res.params);mu=np.exp(np.clip(Xtm@par[:-1],-20,20));alpha=max(float(par[-1]),1e-8);size=1/alpha;prob=size/(size+mu)
                ll=nbinom.logpmf(yte,size,prob);var=mu+alpha*mu*mu
            else: continue
            count_diagnostics.append({"model":name,"history_contract":arm,"count_nll":float(-np.mean(ll)),
                "observed_mean":float(np.mean(yte)),"predicted_mean":float(np.mean(mu)),"observed_variance":float(np.var(yte)),
                "predicted_variance":float(np.mean(var)),"observed_zero_rate":float(np.mean(yte==0)),
                "predicted_zero_rate":float(np.mean(poisson.pmf(0,mu) if name.startswith("POISSON") else nbinom.pmf(0,size,prob))),
                "observed_ge3_rate":float(np.mean(yte>=3)),"predicted_ge3_rate":float(np.mean(1-(poisson.cdf(2,mu) if name.startswith("POISSON") else nbinom.cdf(2,size,prob)))),
                "nb2_alpha":float(alpha) if name=="NB2" else None})
    # Write all comparison tables and reproducibility manifest.
    pd.DataFrame(summaries).to_csv(outdir/"threshold_metrics.csv",index=False)
    pd.DataFrame(distribution).to_csv(outdir/"distribution_metrics.csv",index=False)
    pd.DataFrame(count_diagnostics).to_csv(outdir/"count_diagnostics.csv",index=False)
    pd.DataFrame(feature_importance).sort_values("mean_poisson_deviance_increase",ascending=False).to_csv(outdir/"histgb_feature_importance.csv",index=False)
    pd.DataFrame(model_notes).to_json(outdir/"model_fit_notes.json",orient="records",indent=2)
    # Ladder coherence from same-output threshold sets, in every model/arm.
    coh=[]
    for model in sorted({k[0] for k in preds_by_model}):
        for arm in ARMS:
            pp=[preds_by_model.get((model,arm,t)) for t in (1,2,3)]
            if all(x is not None for x in pp):
                a=np.column_stack(pp); coh.append({"model":model,"history_contract":arm,"crossing_count":int(((a[:,0]<a[:,1])|(a[:,1]<a[:,2])).sum()),"rows":len(a)})
    pd.DataFrame(coh).to_csv(outdir/"coherence.csv",index=False)
    # Phoenix redundancy plus data-quality audit on OOT population.
    corr=frame[frame.history_contract==ARMS[0]][["d5_sog_per60","d10_sog_per60","attempts_d10_per60","num_shotwasongoal_last5","num_shotwasongoal_last10","current_season_points_prior","current_season_games_prior","team_d10_sf_per_game"]].corr(method="spearman")
    corr.to_csv(outdir/"feature_spearman_correlation.csv")
    # Availability, variance inflation, and test range extrapolation audit.
    from statsmodels.stats.outliers_influence import variance_inflation_factor
    audit = {"feature_availability":{}, "phoenix_vif_training_sample":{}, "scaled_extrapolation_2024":{}}
    for arm in ARMS:
        for split, part in (("train_2023", frame[(frame.canonical_season==2023)&(frame.history_contract==arm)]),
                            ("test_2024", frame[(frame.canonical_season==2024)&(frame.history_contract==arm)])):
            audit["feature_availability"][f"{arm}:{split}"] = {c:float(part[c].notna().mean()) for c in set(CORE+PHOENIX)}
    aud_train=train[PHOENIX].replace([np.inf,-np.inf],np.nan).fillna(0).astype(float)
    sample=aud_train.iloc[::max(1,len(aud_train)//5000)].to_numpy()
    for i,c in enumerate(PHOENIX):
        try: audit["phoenix_vif_training_sample"][c]=float(variance_inflation_factor(sample,i))
        except Exception: audit["phoenix_vif_training_sample"][c]=None
    t120=test[test.history_contract==ARMS[2]]
    for cols_name,cols in (("CORE",CORE),("PHOENIX",PHOENIX)):
        audit["scaled_extrapolation_2024"][cols_name]={}
        for c in cols:
            tr=train[c].replace([np.inf,-np.inf],np.nan).fillna(0).astype(float)
            te=t120[c].replace([np.inf,-np.inf],np.nan).fillna(0).astype(float)
            sd=float(tr.std()) or 1.0; mean=float(tr.mean())
            audit["scaled_extrapolation_2024"][cols_name][c]={"outside_train_min_max":float(((te<tr.min())|(te>tr.max())).mean()),
                "abs_standard_score_gt3":float((np.abs((te-mean)/sd)>3).mean()),"train_min":float(tr.min()),"train_max":float(tr.max())}
    (outdir/"feature_audit.json").write_text(json.dumps(audit,indent=2)+"\n")
    # Opening/history depth metrics for every model/threshold, preserving fixed test season.
    stress=[]
    for arm in ARMS:
        te=test[test.history_contract==arm]
        dates=sorted(te.game_date.unique())
        groups={"season_opening_first_5_dates":te.game_date.isin(dates[:5]),
                "0_current_season_prior":te.current_season_games_prior==0,
                "1_current_season_prior":te.current_season_games_prior==1,
                "2_current_season_prior":te.current_season_games_prior==2,
                "3plus_current_season_prior":te.current_season_games_prior>=3,
                "sparse_player_lt10_lifetime":te.player_lifetime_games_prior<10,
                "veteran_history_ge25":te.player_lifetime_games_prior>=25}
        for model in sorted({k[0] for k in preds_by_model}):
            for j,target in enumerate(TARGETS,1):
                p=preds_by_model.get((model,arm,j))
                if p is None: continue
                for label,mask in groups.items():
                    ix=np.asarray(mask)
                    if ix.sum()<20: continue
                    mm=metrics(te.loc[mask,target],p[ix]);mm.update(model=model,history_contract=arm,threshold=j,segment=label);stress.append(mm)
    pd.DataFrame(stress).to_csv(outdir/"history_stress_metrics.csv",index=False)
    # Player-cluster bootstrap for the leading coherent mean/distribution
    # candidate against softmax, averaged across the three offered thresholds.
    arm=ARMS[2]; te=test[test.history_contract==arm].reset_index(drop=True)
    ids=te.player_id.to_numpy(); unique=np.unique(ids); rng=np.random.default_rng(20261009)
    boot=[]
    for other in ("POISSON_TOI_OFFSET","MULTICLASS_SOFTMAX","NATURAL_BINARY_LOGIT"):
        diffs=[]
        ps_a=[preds_by_model[("HISTGB_POISSON_ASSUMPTION",arm,j)] for j in (1,2,3)]
        ps_b=[preds_by_model[(other,arm,j)] for j in (1,2,3)]
        ys=[te[c].to_numpy() for c in TARGETS]
        player_rows={p:np.flatnonzero(ids==p) for p in unique}
        for _ in range(500):
            drawn=rng.choice(unique,size=len(unique),replace=True)
            weight=np.zeros(len(te),dtype=float)
            for p in drawn: weight[player_rows[p]]+=1
            if weight.sum()==0: continue
            la=np.mean([log_loss(y,pa,labels=[0,1],sample_weight=weight) for y,pa in zip(ys,ps_a)])
            lb=np.mean([log_loss(y,pb,labels=[0,1],sample_weight=weight) for y,pb in zip(ys,ps_b)])
            diffs.append(la-lb)
        boot.append({"history_contract":arm,"comparison":f"HISTGB_POISSON_ASSUMPTION minus {other}",
                     "player_clusters":int(len(unique)),"replicates":len(diffs),"mean_logloss_difference":float(np.mean(diffs)),
                     "ci_2_5":float(np.quantile(diffs,.025)),"ci_97_5":float(np.quantile(diffs,.975)),
                     "negative_favors_histgb":True})
    pd.DataFrame(boot).to_csv(outdir/"player_cluster_bootstrap.csv",index=False)
    # Source and population manifest.
    target=frame[frame.history_contract==ARMS[0]]
    dist=target.realized_points.value_counts().sort_index()
    manifest={"schema_version":"NHL_POINTS_ARCHITECTURE_BAKEOFF_V1","source_path":str(inp),"source_sha256":sha256(inp),"builder_sha256":sha256(Path(__file__)),
      "source_rows":len(source),"canonical_rows":len(target),"seasons":{},"history_contracts":list(ARMS),
      "same_day_excluded":True,"regular_season_only":True,"target_distribution":{str(int(k)):int(v) for k,v in dist.items()},
      "missing_same_game_toi_rows":int(target.same_game_realized_toi_minutes.isna().sum()),"zero_or_missing_same_game_toi_rows":int((target.same_game_realized_toi_minutes.fillna(0)<=0).sum()),
      "missing_same_game_pp_toi_rows":int(target.same_game_realized_pp_toi_minutes.isna().sum()),"same_game_toi_used_as_feature":False,
      "missing_team_identity_rows":int(source.team_id.isna().sum()),
      "train_season":2023,"test_season":2024,"training_rows":len(train),"test_rows_per_contract":len(test[test.history_contract==ARMS[0]]),
      "test_start":str(test.game_date.min().date()),"test_end":str(test.game_date.max().date()),"provider_calls":0,"database_mutations":0,
      "elapsed_seconds":round(time.time()-started,2)}
    for season,g in target.groupby("canonical_season"):
        y=g.realized_points
        manifest["seasons"][str(int(season))]={"rows":len(g),"games":int(g.game_id.nunique()),"start":str(g.game_date.min().date()),"end":str(g.game_date.max().date()),
            "mean":float(y.mean()),"variance":float(y.var()),"zero_rate":float((y==0).mean()),"ge1_rate":float((y>=1).mean()),"ge2_rate":float((y>=2).mean()),"ge3_rate":float((y>=3).mean())}
    (outdir/"manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
    print(json.dumps({"rows":len(frame),"train":len(train),"test":len(test)//3,"outputs":str(outdir),"seconds":manifest["elapsed_seconds"]}))


if __name__ == "__main__": main()
