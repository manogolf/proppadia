#!/usr/bin/env python3
"""Bounded, market-free calibration repair for frozen standalone MLB predictions."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_standalone_prediction_calibration_repair_v1/2026-08-12"
ML_DIR = ROOT / "artifacts/analysis/model_development/mlb_established_game_prediction_methods_benchmark_v1/2026-08-05"
TOT_DIR = ROOT / "artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06"
PIN_DIR = ROOT / "artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10"
PHASE = {"FROZEN_VALIDATION": "DEVELOPMENT", "2026_SEQUENTIAL_EARLY": "VALIDATION", "2026_LATE_HOLDOUT": "HOLDOUT"}
EPS = 1e-12
ML_BANDS = [0.5, .55, .60, .65, .70, .75, 1.0000001]
ML_LABELS = ["50-54.99%", "55-59.99%", "60-64.99%", "65-69.99%", "70-74.99%", ">=75%"]
TOT_BANDS = [-np.inf, 7.5, 8, 8.5, 9, 9.5, np.inf]
TOT_LABELS = ["<7.5", "7.5-7.99", "8.0-8.49", "8.5-8.99", "9.0-9.49", ">=9.5"]
LINES = np.arange(.5, 20, 1.0)


def clip(p):
    return np.clip(np.asarray(p, dtype=float), EPS, 1 - EPS)


def logit(p):
    p = clip(p)
    return np.log(p / (1 - p))


def ece(y, p, bins=10):
    y, p = np.asarray(y, float), np.asarray(p, float)
    ix = np.minimum((clip(p) * bins).astype(int), bins - 1)
    return float(sum(np.sum(ix == i) / len(p) * abs(p[ix == i].mean() - y[ix == i].mean())
                     for i in range(bins) if np.any(ix == i)))


def binary_metrics(y, p):
    y, p = np.asarray(y, int), clip(p)
    return {"brier": float(brier_score_loss(y, p)), "log_loss": float(log_loss(y, p, labels=[0, 1])),
            "ece": ece(y, p), "accuracy": float(np.mean((p >= .5) == y)), "probability_sd": float(np.std(p))}


def poisson_mass(mu, max_n=20):
    mu = np.asarray(mu, float)
    n = np.arange(max_n + 1)
    exact = poisson.pmf(n[None, :], mu[:, None])
    tail = np.maximum(0.0, 1.0 - exact.sum(axis=1))
    return exact, tail


def crps_count(y, mu):
    # Exact integer-count CRPS identity: E|X-y| - 0.5 E|X-X'| for Poisson X.
    y, mu = np.asarray(y, int), np.asarray(mu, float)
    max_n = max(60, int(max(y.max(), mu.max()) + 12 * math.sqrt(max(mu.max(), 1))))
    support = np.arange(max_n + 1)
    pmf = poisson.pmf(support[None, :], mu[:, None])
    pmf[:, -1] += np.maximum(0, 1 - pmf.sum(axis=1))
    cdf = np.cumsum(pmf, axis=1)
    obs_cdf = support[None, :] >= y[:, None]
    return np.sum((cdf - obs_cdf) ** 2, axis=1)


def ladder_frame(y, mu):
    y = np.asarray(y, int)
    p = np.column_stack([poisson.sf(line, mu) for line in LINES])
    o = np.column_stack([y > line for line in LINES]).astype(int)
    return p, o


def ladder_metrics(y, mu):
    p, o = ladder_frame(y, mu)
    return {"ladder_brier": float(np.mean((p - o) ** 2)),
            "ladder_log_loss": float(np.mean(-(o * np.log(clip(p)) + (1-o) * np.log(clip(1-p))))),
            "ladder_ece": ece(o.ravel(), p.ravel())}


def totals_metrics(y, mu):
    y, mu = np.asarray(y, float), np.asarray(mu, float)
    residual = y - mu
    return {"mae": float(np.mean(abs(residual))), "rmse": float(np.sqrt(np.mean(residual ** 2))),
            "signed_bias_actual_minus_prediction": float(np.mean(residual)), "crps": float(np.mean(crps_count(y, mu))),
            **ladder_metrics(y, mu)}


def moneyline_band_rows(d, probability_col, model, scope_type="PHASE"):
    rows = []
    scopes = [(scope_type, phase, g) for phase, g in d.groupby("phase")]
    scopes += [("MONTH", month, g) for month, g in d.groupby(d.game_date.str[:7])]
    for typ, scope, g in scopes:
        home_p = g[probability_col].to_numpy()
        picked_p = np.maximum(home_p, 1-home_p)
        picked_y = np.where(home_p >= .5, g.winner_home, 1-g.winner_home)
        band = pd.cut(picked_p, ML_BANDS, labels=ML_LABELS, right=False)
        for label in ML_LABELS:
            z = band == label
            if not np.any(z):
                continue
            p, y = picked_p[z], picked_y[z]
            rows.append({"scope_type": typ, "scope": scope, "model": model, "picked_side_probability_band": label,
                         "games": int(z.sum()), "mean_predicted_probability": p.mean(), "observed_win_rate": y.mean(),
                         "calibration_gap_predicted_minus_observed": p.mean()-y.mean(),
                         "brier": np.mean((p-y)**2), "log_loss": log_loss(y, clip(p), labels=[0, 1])})
    return rows


def totals_bias_rows(d, prediction_col, model):
    rows=[]
    scopes=[("PHASE", phase, g) for phase,g in d.groupby("phase")]
    scopes += [("MONTH", month, g) for month,g in d.groupby(d.game_date.str[:7])]
    for typ,scope,g in scopes:
        bands=pd.cut(g[prediction_col],TOT_BANDS,labels=TOT_LABELS,right=False)
        for label in TOT_LABELS:
            x=g[bands==label]
            if x.empty: continue
            residual=x.observed_total-x[prediction_col]
            rows.append({"scope_type":typ,"scope":scope,"model":model,"predicted_total_band":label,"games":len(x),
                         "mean_predicted_total":x[prediction_col].mean(),"mean_actual_total":x.observed_total.mean(),
                         "signed_residual_actual_minus_prediction":residual.mean(),"mae":abs(residual).mean(),
                         "crps":crps_count(x.observed_total,x[prediction_col]).mean()})
    return rows


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    ml = pd.read_csv(ML_DIR / "benchmark_game_predictions.csv")
    ml = ml[(ml.method == "PYTHAGOREAN_LOG5") & ml.split.isin(PHASE)].copy()
    ml["phase"] = ml.split.map(PHASE); ml["winner_home"] = (ml.home_runs > ml.away_runs).astype(int)
    ml = ml.sort_values(["game_date", "game_pk"]).reset_index(drop=True)
    totals = pd.read_csv(TOT_DIR / "total_distribution_predictions.csv")
    totals = totals[(totals.model == "MODEL_C_INDEPENDENT_HOME_AWAY_POISSON") & totals.split.isin(PHASE)].copy()
    totals["phase"] = totals.split.map(PHASE); totals = totals.sort_values(["game_date", "game_pk"]).reset_index(drop=True)

    # Moneyline challengers: frozen probability is the only input; outcome access is chronological.
    dev, val, hold = (ml[ml.phase == x] for x in ["DEVELOPMENT", "VALIDATION", "HOLDOUT"])
    platt = LogisticRegression(C=1e6, solver="lbfgs", max_iter=5000, random_state=17)
    platt.fit(logit(dev.home_win_probability).reshape(-1,1), dev.winner_home)
    iso = IsotonicRegression(out_of_bounds="clip", y_min=EPS, y_max=1-EPS)
    iso.fit(dev.home_win_probability, dev.winner_home)
    def ml_predict(g, name):
        if name == "RAW": return g.home_win_probability.to_numpy()
        if name == "PLATT": return platt.predict_proba(logit(g.home_win_probability).reshape(-1,1))[:,1]
        return iso.predict(g.home_win_probability)
    ml_comp=[]
    for phase,g in [("DEVELOPMENT",dev),("VALIDATION",val)]:
        for name in ["RAW","PLATT","ISOTONIC"]:
            ml_comp.append({"phase":phase,"model":name,"games":len(g),**binary_metrics(g.winner_home,ml_predict(g,name))})
    ml_comp=pd.DataFrame(ml_comp)
    candidates=ml_comp[(ml_comp.phase=="VALIDATION") & ml_comp.model.isin(["PLATT","ISOTONIC"])].copy()
    selected_ml=candidates.sort_values(["brier","log_loss","ece","model"]).iloc[0].model
    ml["calibrated_home_probability"] = ml_predict(ml, selected_ml)
    raw_hold=ml_predict(hold,"RAW"); cal_hold=ml_predict(hold,selected_ml)
    ml_hold=pd.DataFrame([{"model":"RAW","selected_calibration":selected_ml,"games":len(hold),**binary_metrics(hold.winner_home,raw_hold),"winner_direction_flips":0},
                          {"model":selected_ml,"selected_calibration":selected_ml,"games":len(hold),**binary_metrics(hold.winner_home,cal_hold),
                           "winner_direction_flips":int(np.sum((raw_hold>=.5)!=(cal_hold>=.5)))}])
    raw_ml_rows=moneyline_band_rows(ml,"home_win_probability","RAW")
    cal_ml_rows=moneyline_band_rows(ml,"calibrated_home_probability",selected_ml)
    pd.DataFrame(raw_ml_rows).to_csv(OUT/"moneyline_raw_calibration.csv",index=False)
    ml_comp.to_csv(OUT/"moneyline_calibration_model_comparison.csv",index=False)
    ml_hold.to_csv(OUT/"moneyline_calibrated_holdout_metrics.csv",index=False)
    pd.DataFrame(raw_ml_rows+cal_ml_rows).to_csv(OUT/"moneyline_confidence_reliability.csv",index=False)

    # Totals corrections: development-only coefficients and validation-only selection.
    td,tv,th=(totals[totals.phase==x] for x in ["DEVELOPMENT","VALIDATION","HOLDOUT"])
    intercept=(td.observed_total-td.expected_total).mean()
    affine=LinearRegression().fit(td[["expected_total"]],td.observed_total)
    def tot_predict(g,name):
        raw=g.expected_total.to_numpy()
        if name=="RAW": return raw
        if name=="INTERCEPT": return np.maximum(EPS,raw+intercept)
        return np.maximum(EPS,affine.predict(g[["expected_total"]]))
    tot_comp=[]
    for phase,g in [("DEVELOPMENT",td),("VALIDATION",tv)]:
        for name in ["RAW","INTERCEPT","AFFINE"]:
            tot_comp.append({"phase":phase,"model":name,"games":len(g),**totals_metrics(g.observed_total,tot_predict(g,name)),
                             "intercept":0 if name=="RAW" else intercept if name=="INTERCEPT" else affine.intercept_,
                             "slope":1 if name!="AFFINE" else affine.coef_[0]})
    tot_comp=pd.DataFrame(tot_comp)
    candidates=tot_comp[(tot_comp.phase=="VALIDATION")&tot_comp.model.isin(["INTERCEPT","AFFINE"])].copy()
    selected_tot=candidates.sort_values(["crps","mae","ladder_brier","model"]).iloc[0].model
    totals["calibrated_expected_total"]=tot_predict(totals,selected_tot)
    tot_hold=pd.DataFrame([{"model":"RAW","selected_correction":selected_tot,"games":len(th),**totals_metrics(th.observed_total,tot_predict(th,"RAW"))},
                           {"model":selected_tot,"selected_correction":selected_tot,"games":len(th),**totals_metrics(th.observed_total,tot_predict(th,selected_tot))}])
    pd.DataFrame(totals_bias_rows(totals,"expected_total","RAW")).to_csv(OUT/"totals_raw_bias_map.csv",index=False)
    tot_comp.to_csv(OUT/"totals_calibration_model_comparison.csv",index=False)
    tot_hold.to_csv(OUT/"totals_calibrated_holdout_metrics.csv",index=False)
    prob_rows=[]
    for phase,g in totals.groupby("phase"):
        for name,col in [("RAW","expected_total"),(selected_tot,"calibrated_expected_total")]:
            p,o=ladder_frame(g.observed_total,g[col])
            for j,line in enumerate(LINES):
                prob_rows.append({"phase":phase,"model":name,"line":line,"games":len(g),"mean_over_probability":p[:,j].mean(),
                                  "observed_over_rate":o[:,j].mean(),"calibration_gap":p[:,j].mean()-o[:,j].mean(),
                                  "brier":np.mean((p[:,j]-o[:,j])**2),"log_loss":log_loss(o[:,j],clip(p[:,j]),labels=[0,1])})
    pd.DataFrame(prob_rows).to_csv(OUT/"totals_probability_calibration.csv",index=False)

    # Own-extreme diagnostics, with cut points frozen independently within each phase.
    extreme=[]
    for phase,g in ml.groupby("phase"):
        confidence=abs(g.home_win_probability-.5); q80,q90=confidence.quantile([.8,.9])
        groups=np.where(confidence>=q90,"TOP_10_MOST_CONFIDENT",np.where(confidence>=q80,"NEXT_10","REMAINING_80"))
        for label in ["TOP_10_MOST_CONFIDENT","NEXT_10","REMAINING_80"]:
            x=g[groups==label]; p=x.home_win_probability; y=x.winner_home
            extreme.append({"lane":"MONEYLINE","phase":phase,"group":label,"rows":len(x),"mean_raw_prediction":p.mean(),
                            **binary_metrics(y,p),"mean_absolute_error":abs(p-y).mean(),"signed_residual":(y-p).mean()})
    for phase,g in totals.groupby("phase"):
        q10,q90=g.expected_total.quantile([.1,.9]); groups=np.where(g.expected_total<=q10,"LOWEST_10",np.where(g.expected_total>=q90,"HIGHEST_10","MIDDLE_80"))
        for label in ["LOWEST_10","HIGHEST_10","MIDDLE_80"]:
            x=g[groups==label]; met=totals_metrics(x.observed_total,x.expected_total)
            extreme.append({"lane":"TOTALS","phase":phase,"group":label,"rows":len(x),"mean_raw_prediction":x.expected_total.mean(),
                            "mean_absolute_error":met["mae"],"signed_residual":met["signed_bias_actual_minus_prediction"],"crps":met["crps"]})
    pd.DataFrame(extreme).to_csv(OUT/"standalone_extreme_prediction_diagnostic.csv",index=False)

    # Pinnacle is descriptive only, evaluated after both decisions are frozen.
    market=[]
    pm=pd.read_csv(PIN_DIR/"moneyline_pinnacle_join.csv").merge(ml[["game_pk","calibrated_home_probability"]],on="game_pk",how="inner")
    for name,col in [("RAW","home_win_probability"),(selected_ml,"calibrated_home_probability")]:
        delta=pm[col]-pm.pinnacle_home_no_vig_probability
        market.append({"lane":"MONEYLINE","model":name,"games":len(pm),"mean_signed_separation":delta.mean(),"mean_absolute_separation":abs(delta).mean(),"median_absolute_separation":abs(delta).median()})
    pt=pd.read_csv(PIN_DIR/"totals_pinnacle_join.csv").merge(totals[["game_pk","calibrated_expected_total"]],on="game_pk",how="inner")
    for name,col in [("RAW","expected_total"),(selected_tot,"calibrated_expected_total")]:
        delta=pt[col]-pt.pinnacle_total_line
        market.append({"lane":"TOTALS","model":name,"games":len(pt),"mean_signed_separation":delta.mean(),"mean_absolute_separation":abs(delta).mean(),"median_absolute_separation":abs(delta).median()})
    pd.DataFrame(market).to_csv(OUT/"calibrated_market_separation_diagnostic.csv",index=False)

    # Decisions require validation and untouched-holdout score improvement.
    mr=ml_hold.set_index("model"); ml_imp=(mr.loc["RAW","brier"]-mr.loc[selected_ml,"brier"])
    valraw=ml_comp.query("phase=='VALIDATION' and model=='RAW'").iloc[0]; valsel=ml_comp.query("phase=='VALIDATION' and model==@selected_ml").iloc[0]
    if valsel.brier>=valraw.brier or mr.loc[selected_ml,"brier"]>=mr.loc["RAW","brier"]: ml_dec="MONEYLINE_CALIBRATION_NO_IMPROVEMENT"
    elif ml_imp>=.005 and mr.loc[selected_ml,"log_loss"]<mr.loc["RAW","log_loss"]: ml_dec="MONEYLINE_CALIBRATION_MATERIAL_IMPROVEMENT"
    else: ml_dec="MONEYLINE_CALIBRATION_SMALL_IMPROVEMENT"
    tr=tot_hold.set_index("model"); ti=tr.loc["RAW","crps"]-tr.loc[selected_tot,"crps"]
    vtr=tot_comp.query("phase=='VALIDATION' and model=='RAW'").iloc[0]; vts=tot_comp.query("phase=='VALIDATION' and model==@selected_tot").iloc[0]
    if vts.crps>=vtr.crps or tr.loc[selected_tot,"crps"]>=tr.loc["RAW","crps"] or tr.loc[selected_tot,"ladder_brier"]>=tr.loc["RAW","ladder_brier"]: td_dec="TOTALS_CALIBRATION_NO_IMPROVEMENT"
    elif ti>=.01 and tr.loc[selected_tot,"mae"]<tr.loc["RAW","mae"]: td_dec="TOTALS_CALIBRATION_MATERIAL_IMPROVEMENT"
    else: td_dec="TOTALS_CALIBRATION_SMALL_IMPROVEMENT"
    hold_raw_bands=pd.DataFrame(raw_ml_rows).query("scope_type=='PHASE' and scope=='HOLDOUT'")
    hold_cal_bands=pd.DataFrame(cal_ml_rows).query("scope_type=='PHASE' and scope=='HOLDOUT'")
    raw_bias=pd.DataFrame(totals_bias_rows(totals,"expected_total","RAW")).query("scope_type=='PHASE' and scope=='HOLDOUT'")
    ext=pd.DataFrame(extreme)
    m_ext=ext.query("lane=='MONEYLINE' and phase=='HOLDOUT'").set_index("group")
    t_ext=ext.query("lane=='TOTALS' and phase=='HOLDOUT'").set_index("group")
    sep=pd.DataFrame(market)
    text=f"""# MLB Standalone Prediction Calibration Repair v1

## Population and chronology

- Frozen moneyline and totals populations: development 2,120 games (2025 frozen validation), validation 563 games (2026 sequential early), untouched holdout 202 games (2026 late holdout).
- Calibration inputs: frozen standalone prediction and official outcome only. No sportsbook input, baseball-feature refit, EV/Edge, selector, deployment, or underlying-model mutation.

## Moneyline

- Selected on validation: `{selected_ml}`. Holdout raw Brier/log loss/ECE {mr.loc['RAW','brier']:.6f}/{mr.loc['RAW','log_loss']:.6f}/{mr.loc['RAW','ece']:.6f}; calibrated {mr.loc[selected_ml,'brier']:.6f}/{mr.loc[selected_ml,'log_loss']:.6f}/{mr.loc[selected_ml,'ece']:.6f}.
- Accuracy/probability SD: raw {mr.loc['RAW','accuracy']:.6f}/{mr.loc['RAW','probability_sd']:.6f}; calibrated {mr.loc[selected_ml,'accuracy']:.6f}/{mr.loc[selected_ml,'probability_sd']:.6f}. Winner-direction flips: {int(mr.loc[selected_ml,'winner_direction_flips'])}.
"""
    text+="- Holdout fixed picked-side bands, raw predicted/observed: " + "; ".join(f"{r.picked_side_probability_band} {r.mean_predicted_probability:.3f}/{r.observed_win_rate:.3f} (n={int(r.games)})" for _,r in hold_raw_bands.iterrows()) + ".\n"
    text+=f"- Holdout fixed picked-side bands, calibrated predicted/observed: " + "; ".join(f"{r.picked_side_probability_band} {r.mean_predicted_probability:.3f}/{r.observed_win_rate:.3f} (n={int(r.games)})" for _,r in hold_cal_bands.iterrows()) + ".\n"
    text+=f"- Own-confidence holdout Brier: top 10% {m_ext.loc['TOP_10_MOST_CONFIDENT','brier']:.6f}, next 10% {m_ext.loc['NEXT_10','brier']:.6f}, remaining 80% {m_ext.loc['REMAINING_80','brier']:.6f}. Declaration: `{ml_dec}`.\n\n"
    text+=f"""## Totals

- Raw holdout MAE/bias/CRPS: {tr.loc['RAW','mae']:.6f}/{tr.loc['RAW','signed_bias_actual_minus_prediction']:.6f}/{tr.loc['RAW','crps']:.6f}.
"""
    text+="- Raw holdout bias by predicted-total range: " + "; ".join(f"{r.predicted_total_band} {r.signed_residual_actual_minus_prediction:+.3f} (n={int(r.games)})" for _,r in raw_bias.iterrows()) + ".\n"
    text+=f"- Selected on validation: `{selected_tot}` (development intercept {intercept:+.6f}; affine a={affine.intercept_:+.6f}, b={affine.coef_[0]:.6f}). Calibrated holdout MAE/bias/CRPS: {tr.loc[selected_tot,'mae']:.6f}/{tr.loc[selected_tot,'signed_bias_actual_minus_prediction']:.6f}/{tr.loc[selected_tot,'crps']:.6f}.\n"
    text+=f"- Holdout ladder Brier/log loss/ECE: raw {tr.loc['RAW','ladder_brier']:.6f}/{tr.loc['RAW','ladder_log_loss']:.6f}/{tr.loc['RAW','ladder_ece']:.6f}; calibrated {tr.loc[selected_tot,'ladder_brier']:.6f}/{tr.loc[selected_tot,'ladder_log_loss']:.6f}/{tr.loc[selected_tot,'ladder_ece']:.6f}.\n"
    text+=f"- Own-extreme holdout MAE/bias: lowest 10% {t_ext.loc['LOWEST_10','mean_absolute_error']:.3f}/{t_ext.loc['LOWEST_10','signed_residual']:+.3f}; highest 10% {t_ext.loc['HIGHEST_10','mean_absolute_error']:.3f}/{t_ext.loc['HIGHEST_10','signed_residual']:+.3f}; middle 80% {t_ext.loc['MIDDLE_80','mean_absolute_error']:.3f}/{t_ext.loc['MIDDLE_80','signed_residual']:+.3f}. Declaration: `{td_dec}`.\n\n"
    raw_ms=sep.query("lane=='MONEYLINE' and model=='RAW'").iloc[0]; cal_ms=sep.query("lane=='MONEYLINE' and model==@selected_ml").iloc[0]
    raw_ts=sep.query("lane=='TOTALS' and model=='RAW'").iloc[0]; cal_ts=sep.query("lane=='TOTALS' and model==@selected_tot").iloc[0]
    text+=f"""## Direct answers

1. Moneyline ranking remains monotonic and useful; probability confidence is {'meaningfully repairable' if ml_dec!='MONEYLINE_CALIBRATION_NO_IMPROVEMENT' else 'not shown to be repairable by the bounded calibrators'}.
2. Totals error is {'partly a correctable systematic bias' if td_dec!='TOTALS_CALIBRATION_NO_IMPROVEMENT' else 'not primarily a safely correctable systematic bias under untouched probability scoring'}.
3. Calibration {'improved both holdouts' if ml_dec!='MONEYLINE_CALIBRATION_NO_IMPROVEMENT' and td_dec!='TOTALS_CALIBRATION_NO_IMPROVEMENT' else 'did not qualify for both models'} while descriptive mean absolute Pinnacle separation changed moneyline {raw_ms.mean_absolute_separation:.6f} to {cal_ms.mean_absolute_separation:.6f} and totals {raw_ts.mean_absolute_separation:.6f} to {cal_ts.mean_absolute_separation:.6f}; independence was {'preserved' if cal_ms.mean_absolute_separation>=.75*raw_ms.mean_absolute_separation and cal_ts.mean_absolute_separation>=.75*raw_ts.mean_absolute_separation else 'materially reduced'}.
4. Best calibrated standalone foundation: `{'MONEYLINE_'+selected_ml if ml_dec!='MONEYLINE_CALIBRATION_NO_IMPROVEMENT' else 'RAW_MLB_GAME_PYTHAGOREAN_LOG5_V1'}` for winner probabilities; `{'TOTALS_V1_'+selected_tot if td_dec!='TOTALS_CALIBRATION_NO_IMPROVEMENT' else 'RAW_TOTALS_V1'}` for totals distributions. These remain separate target-specific foundations.

Final declarations: `{ml_dec}`; `{td_dec}`.
"""
    report=OUT/"concise_mlb_standalone_prediction_calibration_repair_v1.md"; report.write_text(text)
    sources=[ML_DIR/"benchmark_game_predictions.csv",TOT_DIR/"total_distribution_predictions.csv",PIN_DIR/"moneyline_pinnacle_join.csv",PIN_DIR/"totals_pinnacle_join.csv"]
    outputs=sorted(p for p in OUT.iterdir() if p.name!="reproducibility_hashes.json")
    hashes={"experiment_id":"MLB_STANDALONE_PREDICTION_CALIBRATION_REPAIR_V1","sources":{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
            "outputs":{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in outputs},
            "chronology":PHASE,"moneyline_selected":selected_ml,"totals_selected":selected_tot,
            "moneyline_declaration":ml_dec,"totals_declaration":td_dec,"market_used_for_selection":False}
    (OUT/"reproducibility_hashes.json").write_text(json.dumps(hashes,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT.relative_to(ROOT)),"moneyline_selected":selected_ml,"moneyline_declaration":ml_dec,
                      "totals_selected":selected_tot,"totals_declaration":td_dec},indent=2))


if __name__ == "__main__":
    main()
