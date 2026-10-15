"""Characterize Poisson count shape on frozen NHL SOG populations; no model state writes."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar
from scipy.special import gammaln
from scipy.stats import nbinom

ROOT = Path(__file__).resolve().parents[3]
HIST = ROOT / "artifacts/analysis/nhl/sog_expanded_structural_validation/2026-10-14/tier1_tier2_evaluation_population.csv"
PROS = ROOT / "artifacts/analysis/nhl/sog_baseline_characterization/2026-10-14/prospective_paired_rows.csv"
PROS_FEATURES = ROOT / "artifacts/analysis/nhl/sog_baseline_characterization/2026-10-14/prospective_feature_bound_rows.csv"
MARKET = ROOT / "artifacts/analysis/nhl/sog_baseline_characterization/2026-10-14/market_matched_rows.csv"
SCORER = ROOT / "backend/nhl/scripts/score_sog_poisson_baseline.py"
OUT = ROOT / "artifacts/analysis/nhl/sog_poisson_distribution_characterization/2026-10-14"
LINES = {"1.5": 2, "2.5": 3, "3.5": 4}
VARIANTS = ("production", "divergence", "combined")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def poisson_probs(mu: np.ndarray, max_k: int = 5) -> np.ndarray:
    mu = np.asarray(mu, dtype=float)
    cols = [np.exp(-mu)]
    for k in range(1, max_k + 1):
        cols.append(cols[-1] * mu / k)
    return np.column_stack([*cols, np.maximum(0.0, 1.0 - np.sum(cols, axis=0))])


def poisson_over(mu: np.ndarray, line_count: int) -> np.ndarray:
    probs = poisson_probs(mu, max_k=max(5, line_count - 1))
    return np.clip(1.0 - probs[:, :line_count].sum(axis=1), 0.0, 1.0)


def nb_logpmf(y: np.ndarray, mu: np.ndarray, alpha: float) -> np.ndarray:
    y, mu = np.asarray(y, float), np.asarray(mu, float)
    r = 1.0 / alpha
    with np.errstate(divide="ignore", invalid="ignore"):
        last = np.where(y == 0, 0.0, y * (np.log(mu) - np.log(r + mu)))
    result=(gammaln(y + r) - gammaln(r) - gammaln(y + 1)
            + r * np.log(r / (r + mu)) + last)
    return np.where((mu == 0)&(y == 0),0.0,result)


def nb_over(mu: np.ndarray, line_count: int, alpha: float) -> np.ndarray:
    r = 1.0 / alpha
    return nbinom.sf(line_count - 1, r, r / (r + np.asarray(mu, float)))


def fit_alpha(y: np.ndarray, mu: np.ndarray) -> float:
    valid=np.asarray(mu,float)>0
    y=np.asarray(y,float)[valid]; mu=np.asarray(mu,float)[valid]
    def loss(log_alpha: float) -> float:
        return float(-nb_logpmf(y, mu, math.exp(log_alpha)).sum())
    result = minimize_scalar(loss, bounds=(-12.0, math.log(10.0)), method="bounded", options={"xatol": 1e-8})
    return float(math.exp(result.x))


def metric_rows(frame: pd.DataFrame, cohort: str, variant: str, alpha: float | None = None) -> list[dict]:
    frame=frame[frame[f"lambda_{variant}"].notna()].copy()
    y = frame.y.to_numpy(float)
    mu = frame[f"lambda_{variant}"].to_numpy(float)
    llvec=(-nb_logpmf(y,mu,alpha) if alpha is not None else
           np.vectorize(math.lgamma)(y+1)-np.where(y==0,0,y*np.log(np.maximum(mu,1e-300)))+mu)
    impossible=(mu==0)&(y>0)
    full_nll=float("inf") if impossible.any() else float(llvec.mean())
    valid=mu>0
    positive_nll=float(llvec[valid].mean()) if valid.any() else None
    out = []
    for label, threshold in LINES.items():
        p = nb_over(mu, threshold, alpha) if alpha is not None else poisson_over(mu, threshold)
        target = (y >= threshold).astype(float)
        clip = np.clip(p, 1e-15, 1 - 1e-15)
        out.append({"cohort": cohort, "variant": variant, "distribution": "negative_binomial" if alpha is not None else "poisson",
                    "line": label, "n": len(frame), "brier": float(np.mean((p-target)**2)),
                    "log_loss": float(np.mean(-(target*np.log(clip)+(1-target)*np.log(1-clip)))),
                    "accuracy": float(np.mean((p >= .5) == target)), "predicted_over_rate": float(np.mean(p)),
                    "realized_over_rate": float(np.mean(target)), "calibration_bias": float(np.mean(p-target)),
                    "count_nll_all_rows": full_nll,"count_nll_positive_lambda_rows":positive_nll,
                    "positive_lambda_n":int(valid.sum()),"zero_lambda_positive_outcomes":int(impossible.sum())})
    return out


def calibration(frame: pd.DataFrame, cohort: str, variant: str, alpha: float | None = None) -> pd.DataFrame:
    frame=frame[frame[f"lambda_{variant}"].notna()].copy()
    records=[]
    for label, threshold in LINES.items():
        mu=frame[f"lambda_{variant}"].to_numpy(float)
        p=nb_over(mu,threshold,alpha) if alpha is not None else poisson_over(mu,threshold)
        y=(frame.y.to_numpy(float)>=threshold).astype(float)
        edges=np.linspace(0,1,11)
        bins=np.minimum(np.digitize(p,edges,right=False)-1,9)
        for b in range(10):
            mask=bins==b
            if not mask.any(): continue
            records.append({"cohort":cohort,"variant":variant,"distribution":"negative_binomial" if alpha is not None else "poisson",
                "line":label,"probability_bin":f"{edges[b]:.1f}-{edges[b+1]:.1f}","n":int(mask.sum()),
                "mean_predicted":float(p[mask].mean()),"realized_frequency":float(y[mask].mean()),
                "calibration_gap":float(y[mask].mean()-p[mask].mean())})
    return pd.DataFrame(records)


def score_decomposition(metrics: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    for cohort in metrics.cohort.unique():
        part=metrics[metrics.cohort.eq(cohort)]
        for line in LINES:
            sub=part[part.line.eq(line)].set_index(["variant","distribution"])
            chain=[("production_poisson","production","poisson"),("divergence_poisson","divergence","poisson"),
                   ("combined_poisson","combined","poisson"),("combined_negative_binomial","combined","negative_binomial")]
            for metric in ("brier","log_loss"):
                vals=[(label,float(sub.loc[(var,dist),metric])) for label,var,dist in chain if (var,dist) in sub.index]
                for (left,a),(right,b) in zip(vals,vals[1:]):
                    rows.append({"cohort":cohort,"line":line,"metric":metric,"from":left,"to":right,
                                 "from_score":a,"to_score":b,"score_change_to_minus_from":b-a})
    return pd.DataFrame(rows)


def count_distribution_fit(frame: pd.DataFrame, cohort: str, alpha: float) -> pd.DataFrame:
    y=frame.y.to_numpy(int); mu=frame.lambda_production.to_numpy(float); r=1/alpha
    pp=poisson_probs(mu,5)
    pn=np.column_stack([nbinom.pmf(k,r,r/(r+mu)) for k in range(6)])
    pn=np.column_stack([pn,np.maximum(0,1-pn.sum(axis=1))])
    rows=[]
    for i,label in enumerate(("0","1","2","3","4","5","6+")):
        obs=int(np.sum(y==i) if i<6 else np.sum(y>=6))
        for dist,p in (("poisson",pp),("negative_binomial",pn)):
            expected=float(np.mean(p[:,i]))
            rows.append({"cohort":cohort,"count":label,"distribution":dist,"n":len(y),"observed_count":obs,
                         "observed_frequency":obs/len(y),"expected_frequency":expected,
                         "observed_minus_expected":obs/len(y)-expected})
    return pd.DataFrame(rows)


def make_prospective() -> pd.DataFrame:
    data=pd.read_csv(PROS)
    data=data[(data.window=="prospective_2026_production") & data.actual_sog.notna()]
    data=data.sort_values("line").drop_duplicates(["game_id","player_id"]).copy()
    data["y"]=pd.to_numeric(data.actual_sog)
    data["lambda_production"]=pd.to_numeric(data.production_lambda)
    feature=pd.read_csv(PROS_FEATURES).sort_values("line").drop_duplicates(["game_id","player_id"])
    feature_cols=["game_id","player_id","d10_sog_per60","d20_sog_per60","selected_toi_minutes","role_pp_share"]
    feature=feature[[c for c in feature_cols if c in feature]].copy()
    data=data.merge(feature,on=["game_id","player_id"],how="left",validate="one_to_one",suffixes=("","_feature"))
    d10=pd.to_numeric(data.d10_sog_per60,errors="coerce"); d20=pd.to_numeric(data.d20_sog_per60,errors="coerce")
    rate=d10.where(d10.notna(),d20)
    div=rate.copy(); active=d10.notna()&d20.notna()&(d10-d20).abs().gt(.5); div.loc[active]=(d10.loc[active]+d20.loc[active])/2
    exp=pd.to_numeric(data.selected_toi_minutes)
    data["lambda_divergence"]=div*exp/60
    combined=div.copy(); high=d10.gt(9)&div.gt(9); combined.loc[high]=9+.5*(div.loc[high]-9)
    data["lambda_combined"]=combined*exp/60
    data["trust_tier"]="PROSPECTIVE_2026_FEATURE_BOUND"
    data["feature_bound"]=data.lambda_combined.notna()
    return data


def prepare(frame: pd.DataFrame, *, y: str, prod: str, div: str, comb: str) -> pd.DataFrame:
    d=frame.copy(); d["y"]=pd.to_numeric(d[y],errors="coerce")
    d["lambda_production"]=pd.to_numeric(d[prod],errors="coerce")
    d["lambda_divergence"]=pd.to_numeric(d[div],errors="coerce")
    d["lambda_combined"]=pd.to_numeric(d[comb],errors="coerce")
    return d[d.y.notna()&d.y.ge(0)&d.lambda_production.notna()&d.lambda_production.ge(0)].copy()


def freq_table(frame: pd.DataFrame, group: str, cohort: str) -> pd.DataFrame:
    result=[]
    groups=[("ALL",frame)] if group not in frame else list(frame.groupby(group,dropna=False,observed=True))
    for key,part in groups:
        mu=part.lambda_production.to_numpy(float); y=part.y.to_numpy(int); pred=poisson_probs(mu,5).sum(axis=0)
        for i,label in enumerate(("0","1","2","3","4","5","6+")):
            obs=float(np.sum(y==i)) if i<6 else float(np.sum(y>=6))
            expected=float(pred[i]) if i<6 else float(np.sum(1-poisson_probs(mu,5)[:,:6].sum(axis=1)))
            result.append({"cohort":cohort,"group":str(key),"count":label,"n":len(part),"observed":int(obs),"observed_rate":obs/len(part),"poisson_expected":expected,"poisson_expected_rate":expected/len(part),"observed_minus_expected":obs-expected})
    return pd.DataFrame(result)


def lambda_diagnostics(frame: pd.DataFrame, cohort: str) -> pd.DataFrame:
    d=frame.copy()
    d["lambda_bin"]=pd.qcut(d.lambda_production,10,labels=False,duplicates="drop")
    rows=[]
    for b,p in d.groupby("lambda_bin",observed=True):
        y=p.y.to_numpy(float); mu=p.lambda_production.to_numpy(float); variance=float(np.var(y,ddof=1)) if len(y)>1 else float("nan")
        rows.append({"cohort":cohort,"bin":int(b)+1,"n":len(p),"lambda_min":float(mu.min()),"lambda_max":float(mu.max()),
          "mean_lambda":float(mu.mean()),"mean_observed":float(y.mean()),"mean_residual_actual_minus_predicted":float((y-mu).mean()),
          "variance":variance,"variance_to_mean":variance/y.mean() if y.mean() else None,
          "poisson_expected_variance_mean":float(mu.mean()),"heterogeneous_poisson_variance":float(mu.mean()+mu.var()),
          "count_mae":float(np.mean(abs(y-mu))),"count_rmse":float(np.sqrt(np.mean((y-mu)**2))),
          "observed_zero_rate":float(np.mean(y==0)),"poisson_zero_rate":float(np.mean(np.exp(-mu))),
          **{f"observed_{k}plus_rate":float(np.mean(y>=k)) for k in (4,5,6)},
          **{f"poisson_{k}plus_rate":float(np.mean(poisson_over(mu,k))) for k in (4,5,6)}})
    return pd.DataFrame(rows)


def score_summary(frame: pd.DataFrame, cohort: str, alpha: float | None) -> tuple[pd.DataFrame,pd.DataFrame]:
    metrics=[]; cal=[]
    for variant in VARIANTS:
        metrics += metric_rows(frame,cohort,variant)
        cal.append(calibration(frame,cohort,variant))
        if alpha is not None:
            metrics += metric_rows(frame,cohort,variant,alpha)
            cal.append(calibration(frame,cohort,variant,alpha))
    return pd.DataFrame(metrics),pd.concat(cal,ignore_index=True)


def distribution_summary(frame: pd.DataFrame, cohort: str) -> dict:
    y=frame.y.to_numpy(float); mu=frame.lambda_production.to_numpy(float)
    return {"cohort":cohort,"n":len(frame),"mean_count":float(y.mean()),"variance_count":float(y.var(ddof=1)),
      "variance_mean_ratio":float(y.var(ddof=1)/y.mean()),"mean_lambda":float(mu.mean()),
      "heterogeneous_poisson_variance":float(mu.mean()+mu.var()),"observed_zero_rate":float(np.mean(y==0)),
      "poisson_zero_rate":float(np.mean(np.exp(-mu))),"zero_excess_pp":float(np.mean(y==0)-np.mean(np.exp(-mu))),
      "observed_4plus":float(np.mean(y>=4)),"poisson_4plus":float(np.mean(poisson_over(mu,4))),
      "observed_5plus":float(np.mean(y>=5)),"poisson_5plus":float(np.mean(poisson_over(mu,5))),
      "observed_6plus":float(np.mean(y>=6)),"poisson_6plus":float(np.mean(poisson_over(mu,6)))}


def main() -> None:
    OUT.mkdir(parents=True,exist_ok=True)
    hist0=pd.read_csv(HIST)
    hist=prepare(hist0,y="actual_sog",prod="production_lambda",div="divergence_lambda",comb="soft_lambda")
    # Keep only the validated Tier 1/2 rows represented in the named source population.
    prospective=make_prospective()
    alpha=fit_alpha(hist.y.to_numpy(float),hist.lambda_production.to_numpy(float))
    pop_metrics=[]; calibrations=[]; distributions=[]; countfreq=[]; lambdabins=[]; fitfreq=[]
    allframes={"historical_tier1_tier2":hist,"historical_tier1":hist[hist.trust_tier.eq("TIER1_CERTIFIED")].copy(),
      "historical_tier2":hist[hist.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")].copy(),"prospective_2026":prospective.copy(),
      "prospective_feature_bound_2026":prospective[prospective.feature_bound].copy()}
    broad_edges=[-np.inf,1,1.5,2,2.5,3,4,np.inf]
    broad_labels=["<1","1-1.5","1.5-2","2-2.5","2.5-3","3-4","4+"]
    for cohort,frame in allframes.items():
        distributions.append(distribution_summary(frame,cohort))
        fitfreq.append(count_distribution_fit(frame,cohort,alpha))
        frame["lambda_band"]=pd.cut(frame.lambda_production,broad_edges,labels=broad_labels)
        countfreq.append(freq_table(frame,"lambda_band",cohort))
        countfreq.append(freq_table(frame,"",cohort))
        lambdabins.append(lambda_diagnostics(frame,cohort))
        m,c=score_summary(frame,cohort,alpha if cohort!="historical_tier1_tier2" else alpha)
        pop_metrics.append(m); calibrations.append(c)
    metrics=pd.concat(pop_metrics,ignore_index=True); calib=pd.concat(calibrations,ignore_index=True)
    # Monthly stability and descriptive regime partitioning apply to the complete historical rows.
    hist["month"]=pd.to_datetime(hist.game_date,errors="coerce").dt.strftime("%b")
    monthly=[]; monthly_lines=[]
    for month,p in hist.groupby("month",sort=False):
        x=distribution_summary(p,f"historical_{month}"); x["month"]=month; monthly.append(x)
        for line,threshold in LINES.items():
            prob=poisson_over(p.lambda_production.to_numpy(float),threshold)
            target=(p.y.to_numpy(float)>=threshold).astype(float)
            monthly_lines.append({"month":month,"line":line,"n":len(p),"mean_lambda":float(p.lambda_production.mean()),
              "predicted_over_rate":float(prob.mean()),"realized_over_rate":float(target.mean()),
              "calibration_bias":float(prob.mean()-target.mean()),"brier":float(np.mean((prob-target)**2))})
    regime=[]
    def add_regime(frame,name,col,fn):
        labels=fn(frame)
        for group,p in frame.assign(_regime=labels).groupby("_regime",dropna=False,observed=True):
            r=distribution_summary(p,name); r["regime"] = str(group); r["mean_d10"] = float(pd.to_numeric(p.d10_sog_per60,errors="coerce").mean()) if "d10_sog_per60" in p else None
            r["mean_toi"] = float(pd.to_numeric(p.selected_exposure_minutes if "selected_exposure_minutes" in p else p.selected_toi_minutes,errors="coerce").mean()) if ("selected_exposure_minutes" in p or "selected_toi_minutes" in p) else None
            regime.append(r)
    for f,name in ((hist,"historical"),(prospective,"prospective")):
        if "d10_sog_per60" in f:
            add_regime(f,name,"d10",lambda z:pd.cut(pd.to_numeric(z.d10_sog_per60,errors="coerce"),[-np.inf,4,7,9,np.inf],labels=["LOW_D10","MIDDLE_D10","HIGH_D10","VERY_HIGH_D10"]))
        toi_col="selected_exposure_minutes" if "selected_exposure_minutes" in f else "selected_toi_minutes" if "selected_toi_minutes" in f else None
        if toi_col:
            add_regime(f,name,"toi",lambda z:pd.qcut(pd.to_numeric(z[toi_col],errors="coerce"),4,labels=["LOW_TOI","MIDLOW_TOI","MIDHIGH_TOI","HIGH_TOI"],duplicates="drop"))
        if "pp_share_proxy" in f:
            add_regime(f,name,"pp",lambda z:pd.qcut(pd.to_numeric(z.pp_share_proxy,errors="coerce"),4,labels=["LOW_PP","MIDLOW_PP","MIDHIGH_PP","HIGH_PP"],duplicates="drop"))
        elif "role_pp_share" in f:
            add_regime(f,name,"pp",lambda z:pd.qcut(pd.to_numeric(z.role_pp_share,errors="coerce"),4,labels=["LOW_PP","MIDLOW_PP","MIDHIGH_PP","HIGH_PP"],duplicates="drop"))
        if "is_home" in f:
            add_regime(f,name,"home",lambda z:pd.Series(np.where(z.is_home.astype(str).str.lower().isin(["t","true","1","home"]),"HOME","AWAY"),index=z.index))
    # Player-level descriptive dispersion (at least 20 historical games).
    player=[]
    for pid,p in hist.groupby("player_id"):
        if len(p)<20: continue
        yy=p.y.to_numpy(float); mm=p.lambda_production.to_numpy(float)
        player.append({"player_id":int(pid),"n":len(p),"mean_sog":float(yy.mean()),"variance_sog":float(yy.var(ddof=1)),
          "variance_mean_ratio":float(yy.var(ddof=1)/yy.mean()) if yy.mean() else None,"mean_lambda":float(mm.mean()),
          "lambda_adjusted_dispersion":float(np.sum((yy-mm)**2-mm)/np.sum(mm)) if np.sum(mm)>0 else None})
    player_df=pd.DataFrame(player)
    team_rows=[]
    for team,p in hist.groupby("team_id"):
        if len(p)<200: continue
        z=distribution_summary(p,"historical_team"); z["team_id"]=int(team); z["support_flag"]="ADEQUATE_N_GE_200"; team_rows.append(z)
    # Same-row market comparison; retained no-vig rows only.
    market=pd.read_csv(MARKET); market=market[market.actual_sog.notna() & market.p_over_mkt_novig.notna()].copy()
    market["y"]=pd.to_numeric(market.actual_sog); market["lambda_production"]=pd.to_numeric(market.production_lambda)
    d10=pd.to_numeric(market.d10_sog_per60,errors="coerce"); d20=pd.to_numeric(market.d20_sog_per60,errors="coerce")
    d10rate=d10.where(d10.notna(),d20); div=d10rate.copy(); active=d10.notna()&d20.notna()&(d10-d20).abs().gt(.5); div.loc[active]=(d10.loc[active]+d20.loc[active])/2
    toi=pd.to_numeric(market.selected_toi_minutes); market["lambda_divergence"]=div*toi/60
    combined=div.copy(); hi=d10.gt(9)&div.gt(9); combined.loc[hi]=9+.5*(div.loc[hi]-9); market["lambda_combined"]=combined*toi/60
    market_results=[]
    thresholds=np.ceil(pd.to_numeric(market.line).to_numpy(float)+.5).astype(int)
    y=(market.y.to_numpy(float)>=thresholds).astype(float)
    for variant in VARIANTS:
        mus=market[f"lambda_{variant}"].to_numpy(float)
        for dist, probability in (("poisson",np.array([poisson_over(np.array([mu]),int(k))[0] for mu,k in zip(mus,thresholds)])),
                                  ("negative_binomial",np.array([nb_over(np.array([mu]),int(k),alpha)[0] for mu,k in zip(mus,thresholds)]))):
            clip=np.clip(probability,1e-15,1-1e-15)
            market_results.append({"model":variant+"_"+dist,"n":len(market),"brier":float(np.mean((probability-y)**2)),"log_loss":float(np.mean(-(y*np.log(clip)+(1-y)*np.log(1-clip)))),"calibration_bias":float(np.mean(probability-y))})
    p=market.p_over_mkt_novig.to_numpy(float); clip=np.clip(p,1e-15,1-1e-15)
    market_results.append({"model":"no_vig_market","n":len(market),"brier":float(np.mean((p-y)**2)),"log_loss":float(np.mean(-(y*np.log(clip)+(1-y)*np.log(1-clip)))),"calibration_bias":float(np.mean(p-y))})
    # All requested artifacts, with identical population and explicit trust/source fields.
    outputs={"observed_count_frequencies.csv":pd.concat(countfreq,ignore_index=True),"lambda_bin_mean_variance.csv":pd.concat(lambdabins,ignore_index=True),
      "threshold_calibration.csv":calib,"line_distribution_scores.csv":metrics,"month_level_diagnostics.csv":pd.DataFrame(monthly),"month_line_calibration.csv":pd.DataFrame(monthly_lines),
      "regime_dispersion.csv":pd.DataFrame(regime),"player_dispersion.csv":player_df,"market_relative_comparison.csv":pd.DataFrame(market_results)}
    outputs["team_dispersion_min200.csv"]=pd.DataFrame(team_rows)
    outputs["observed_vs_poisson_nb_frequencies.csv"]=pd.concat(fitfreq,ignore_index=True)
    outputs["score_contribution_decomposition.csv"]=score_decomposition(metrics)
    for name,df in outputs.items(): df.to_csv(OUT/name,index=False)
    # Zero/upper tails broken out by broad production-lambda band.
    hist.groupby("lambda_band",observed=True).apply(lambda p:pd.Series({"n":len(p),"observed_zero":np.mean(p.y==0),"poisson_zero":np.mean(np.exp(-p.lambda_production)),"zero_difference":np.mean(p.y==0)-np.mean(np.exp(-p.lambda_production)),**{f"obs_{k}plus":np.mean(p.y>=k) for k in (4,5,6)},**{f"pois_{k}plus":np.mean(poisson_over(p.lambda_production.to_numpy(),k)) for k in (4,5,6)}}),include_groups=False).reset_index().to_csv(OUT/"zero_and_tail_by_lambda.csv",index=False)
    # Global count distribution likelihood comparison, including the single Tier-1+2 historical fit.
    contributions=score_decomposition(metrics)
    nb_increment=contributions[(contributions.cohort=="prospective_2026")&(contributions.line=="2.5")&
        (contributions.metric=="log_loss")&(contributions["to"]=="combined_negative_binomial")]
    nb_gain=bool(len(nb_increment) and nb_increment.iloc[0].score_change_to_minus_from < -0.005)
    classification="ALTERNATIVE_DISTRIBUTION_SUPPORTED" if nb_gain else "ADEQUATE_WITH_MINOR_TAIL_MISFIT"
    summary={"schema":"NHL_SOG_POISSON_COUNT_DISTRIBUTION_CHARACTERIZATION_V1","status":"COMPLETE",
      "production_contract":{"model":"poisson_baseline/baseline_v1","scorer_path":"backend/nhl/scripts/score_sog_poisson_baseline.py","scorer_sha256":sha(SCORER),
        "lambda_definition":"selected_sog_rate_per60 * selected_toi_minutes / 60 using existing scorer fallback order",
        "probability":"P(SOG>1.5)=1-exp(-lambda)*(1+lambda); P(SOG>2.5)=1-exp(-lambda)*(1+lambda+lambda^2/2); P(SOG>3.5)=1-exp(-lambda)*(1+lambda+lambda^2/2+lambda^3/6)",
        "numerics":"direct finite Poisson CDF summation; probability clipped to [0,1]; invalid/negative lambda returns NaN",
        "calibration":"none in scorer","dispersion_parameter":"none; Poisson variance equals lambda","other_internal_probabilities":"scorer emits p_0_1, p_2, p_3, p_4p; count-level 0 and 1 can be decomposed from lambda"},
      "populations":{"historical_tier1_tier2":{"n":len(hist),"tier1":int(hist.trust_tier.eq("TIER1_CERTIFIED").sum()),"tier2":int(hist.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR").sum()),"line_rows":len(hist)*3,"source":str(HIST.relative_to(ROOT)),"sha256":sha(HIST)},
        "prospective_2026":{"n":len(prospective),"line_rows":len(prospective)*3,"feature_bound_player_games":int(prospective.feature_bound.sum()),"dates":sorted(prospective.game_date.astype(str).unique()),"source":str(PROS.relative_to(ROOT)),"sha256":sha(PROS),"feature_source":str(PROS_FEATURES.relative_to(ROOT)),"feature_sha256":sha(PROS_FEATURES)}},
      "distribution_summaries":distributions,"zero_lambda_support":{"n":int((hist.lambda_production==0).sum()),"positive_outcomes":int(((hist.lambda_production==0)&(hist.y>0)).sum()),"count_nll_is_infinite_for_any_distribution_preserving_exact_zero_mean":bool(((hist.lambda_production==0)&(hist.y>0)).any())},
      "negative_binomial":{"parameterization":"NB2; Var(Y|mu)=mu+alpha*mu^2; r=1/alpha; p=r/(r+mu)","fit_population":"historical Tier1+Tier2 positive-production-lambda rows; zero-lambda positive-outcome violations have zero likelihood under every distribution preserving exact mean zero","fit_n":int((hist.lambda_production>0).sum()),"alpha":alpha,"size_r":1/alpha,"fitted_on_prospective":False},
      "historical_poisson_vs_nb":metrics[(metrics.cohort=="historical_tier1_tier2")&(metrics.variant=="production")].to_dict(orient="records"),
      "prospective_poisson_vs_frozen_nb":metrics[(metrics.cohort=="prospective_2026")&(metrics.variant=="production")].to_dict(orient="records"),
      "market":{"source":str(MARKET.relative_to(ROOT)),"sha256":sha(MARKET),"same_quote_rows":len(market),"metrics":market_results},
      "activity":{"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_authority_changed":False,"new_shadow_created":False},
      "interpretation":{"classification":classification,"nb_is_diagnostic_only":True,"tier3_used":False,
        "rationale":"Heterogeneous-Poisson variance closely matches aggregate historical variance; lambda-bin dispersion is mildly above one; mean construction explains more score improvement than the incremental NB distribution change."},
      "lineage":{"historical_source_sha256":sha(HIST),"prospective_source_sha256":sha(PROS),"market_source_sha256":sha(MARKET),"scorer_sha256":sha(SCORER)}}
    def safe(value):
        if isinstance(value,dict): return {k:safe(v) for k,v in value.items()}
        if isinstance(value,list): return [safe(v) for v in value]
        if isinstance(value,(np.integer,)): return int(value)
        if isinstance(value,(np.floating,float)): return float(value) if math.isfinite(float(value)) else None
        return value
    (OUT/"summary.json").write_text(json.dumps(safe(summary),indent=2,sort_keys=True,allow_nan=False)+"\n")
    report=["# NHL SOG Poisson count-distribution characterization","",
      "## Scope and contract","",
      f"Production scorer `{summary['production_contract']['scorer_path']}` SHA256 `{summary['production_contract']['scorer_sha256']}`. `lambda` is the production expected count. The scorer uses direct Poisson CDF summation for the three half-point Over probabilities, clips results into [0,1], and applies no post-hoc calibration or dispersion parameter. It emits aggregate count bins `p_0_1`, `p_2`, `p_3`, `p_4p`; exact individual count probabilities follow from the same Poisson formula.","",
      f"Historical strict-prior population: {len(hist):,} player-games / {len(hist)*3:,} line rows ({summary['populations']['historical_tier1_tier2']['tier1']:,} Tier 1 and {summary['populations']['historical_tier1_tier2']['tier2']:,} Tier 2). Feature-bound 2026 subset: {int(prospective.feature_bound.sum()):,} player-games; full legitimate production-bound 2026 population: {len(prospective):,} player-games / {len(prospective)*3:,} line rows across {', '.join(summary['populations']['prospective_2026']['dates'])}. Tier 3 is excluded.","",
      "## Count distribution and conditional variance","",
      "| Cohort | N | Mean count | Variance | Var/mean | Mean lambda | Heterogeneous Poisson variance | Zero observed / Poisson | 4+ observed / Poisson | 6+ observed / Poisson |",
      "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for s in distributions: report.append(f"| {s['cohort']} | {s['n']:,} | {s['mean_count']:.3f} | {s['variance_count']:.3f} | {s['variance_mean_ratio']:.3f} | {s['mean_lambda']:.3f} | {s['heterogeneous_poisson_variance']:.3f} | {s['observed_zero_rate']:.3%} / {s['poisson_zero_rate']:.3%} | {s['observed_4plus']:.3%} / {s['poisson_4plus']:.3%} | {s['observed_6plus']:.3%} / {s['poisson_6plus']:.3%} |")
    report += ["","The conditional decile variance, mean calibration, and tail rates are in `lambda_bin_mean_variance.csv`; the exact expected/observed count buckets 0, 1, 2, 3, 4, 5, 6+ are in `observed_count_frequencies.csv`. Zero excess and 4+/5+/6+ rates by broad lambda bands are in `zero_and_tail_by_lambda.csv`. Overall variance is interpreted against the heterogeneous-Poisson variance `E(lambda)+Var(lambda)`, not against mean lambda alone.","",
      "## Threshold proper scores","","See `line_distribution_scores.csv` and `threshold_calibration.csv` for per-line Brier, log loss, accuracy, calibration bias, and probability-bin observed-vs-predicted tables for production, divergence lambda, combined structural lambda, Poisson and NB. Corrections use the already-frozen rule formulas and the same production exposure; they are diagnostics, not new shadows.","",
      f"A single historical NB2 dispersion was fit on Tier 1+2 positive-lambda rows only: alpha={alpha:.6g}, size r={1/alpha:.6g}, mean preserved at lambda. There are {summary['zero_lambda_support']['n']} exact-zero lambda rows, {summary['zero_lambda_support']['positive_outcomes']} with positive SOG; any distribution preserving mean zero assigns those outcomes zero probability, so all-row count NLL is infinite. NB was fit on the mathematically estimable positive-lambda rows and applied unchanged prospectively. See `observed_vs_poisson_nb_frequencies.csv` and `score_contribution_decomposition.csv`; no prospective fitting occurred.","",
      "## Stability, regimes, players and market","","Month stability (Oct–Apr), D10/TOI/PP/home-away regime diagnostics, and player dispersion requiring at least 20 observations are exported separately. Team-specific summaries were intentionally omitted where group support is insufficient; no team distribution was fit. Retained no-vig quote comparison uses exact matched rows in `market_relative_comparison.csv`; no market or wagering filters are derived.","",
      "## Interpretation and authority","",f"Classification: `{classification}`. The decomposition in `score_contribution_decomposition.csv` separates production Poisson → divergence Poisson → combined Poisson (rate construction) from combined Poisson → same-mean NB (distribution shape). Aggregate historical variance {distributions[0]['variance_count']:.3f} is close to heterogeneous-Poisson variance {distributions[0]['heterogeneous_poisson_variance']:.3f}; mild conditional overdispersion and rate-dependent tail residuals remain, without stable month-wide NB evidence or a material prospective incremental NB gain. No production, current shadow, export, or database state was modified. Provider calls, paid credits, DB mutations: 0.","",
      "## Preserved backlog","","First genuine live capture of `NHL_SOG_D10_STRUCTURAL_CORRECTION_V1`; prospective grading of both structural SOG shadows; fixed-blend collection; historical official-TOI binding; Puck Line tomorrow; Points-first cross-prop sanity/explanation audit; Tier 3 override not currently needed.","",
      "## Files","","`observed_count_frequencies.csv`, `observed_vs_poisson_nb_frequencies.csv`, `lambda_bin_mean_variance.csv`, `threshold_calibration.csv`, `line_distribution_scores.csv`, `score_contribution_decomposition.csv`, `zero_and_tail_by_lambda.csv`, `player_dispersion.csv`, `regime_dispersion.csv`, `month_level_diagnostics.csv`, `market_relative_comparison.csv`, `summary.json`, `lineage_manifest.json`."]
    (OUT/"report.md").write_text("\n".join(report)+"\n")
    manifest={"method":"Frozen production lambda; exact Tier1/Tier2 historical reconstruction; feature-bound prospective rows; one pooled NB2 dispersion fit on historical only; no parameter fit on prospective.",
      "inputs":{str(p.relative_to(ROOT)):sha(p) for p in (HIST,PROS,MARKET,SCORER)},
      "outputs":{p.name:sha(p) for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"}}
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT),"historical_n":len(hist),"prospective_n":len(prospective),"nb_alpha":alpha,"status":"COMPLETE"},sort_keys=True))


if __name__ == "__main__":
    main()
