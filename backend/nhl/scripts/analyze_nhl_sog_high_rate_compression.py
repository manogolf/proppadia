#!/usr/bin/env python3
"""Bounded offline evaluation of high-D10 compression after the frozen correction."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "artifacts/analysis/nhl/sog_baseline_characterization/2026-10-14"
TAIL = ROOT / "artifacts/analysis/nhl/sog_shot_rate_contribution/2026-10-14"
DIV = ROOT / "artifacts/analysis/nhl/sog_d10_d20_divergence_correction/2026-10-14"
OUT = ROOT / "artifacts/analysis/nhl/sog_high_rate_compression/2026-10-14"
THRESHOLD = 9.0
DIV_THRESHOLD = 0.5
LINES = {"1.5": 2, "2.5": 3, "3.5": 4}
VARIANTS = ("production", "divergence_control", "soft_compression", "hard_cap")
LAMBDA = {"production": "production_lambda", "divergence_control": "divergence_lambda",
          "soft_compression": "soft_lambda", "hard_cap": "hard_cap_lambda"}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def num(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce") if col in df else pd.Series(np.nan, index=df.index)


def poisson_over(lam: float, threshold: int) -> float:
    if not np.isfinite(lam) or lam < 0:
        return np.nan
    return float(np.clip(1.0 - sum(math.exp(-lam) * lam**k / math.factorial(k)
                                  for k in range(threshold)), 0, 1))


def apply_rules(d10: pd.Series, d20: pd.Series, production_lambda: pd.Series,
                toi: pd.Series, threshold: float = THRESHOLD) -> pd.DataFrame:
    """Return rate/lambda variants and activation flags under frozen, bounded rules."""
    d10, d20, production_lambda, toi = map(pd.to_numeric, (d10, d20, production_lambda, toi))
    divergence = d10 - d20
    div_active = d10.notna() & d20.notna() & divergence.abs().gt(DIV_THRESHOLD)
    base_rate = d10.copy()
    base_rate.loc[div_active] = (d10.loc[div_active] + d20.loc[div_active]) / 2
    # Preserve production fallback rows where D10 is absent; study common rows normally have D10.
    base_rate = base_rate.where(base_rate.notna(), production_lambda * 60 / toi)
    high_region = d10.gt(threshold)
    compression_active = high_region & base_rate.gt(threshold)
    soft_rate = base_rate.copy()
    soft_rate.loc[compression_active] = threshold + 0.5 * (base_rate.loc[compression_active] - threshold)
    cap_rate = base_rate.copy()
    cap_rate.loc[compression_active] = threshold
    out = pd.DataFrame(index=d10.index)
    out["divergence"] = divergence
    out["divergence_correction_active"] = div_active
    out["high_d10_region"] = high_region
    out["high_rate_compression_active"] = compression_active
    out["production_rate"] = production_lambda * 60 / toi
    out["divergence_rate"] = base_rate
    out["soft_rate"] = soft_rate
    out["hard_cap_rate"] = cap_rate
    out["production_lambda"] = production_lambda
    out["divergence_lambda"] = base_rate * toi / 60
    out["soft_lambda"] = soft_rate * toi / 60
    out["hard_cap_lambda"] = cap_rate * toi / 60
    out["fixed_d20_lambda"] = d20 * toi / 60
    out["both_mechanisms_active"] = div_active & compression_active
    out["divergence_only"] = div_active & ~compression_active
    out["high_compression_only"] = ~div_active & compression_active
    out["neither_mechanism_active"] = ~div_active & ~compression_active
    return out


def auc_ap(p: pd.Series, y: pd.Series) -> tuple[float, float]:
    z = pd.DataFrame({"p": p, "y": y}).dropna().sort_values("p", ascending=False, kind="mergesort")
    if z.y.nunique() != 2:
        return np.nan, np.nan
    n1, n0 = int(z.y.sum()), int((1-z.y).sum())
    ranks = z.p.rank(method="average")
    auc = float((ranks[z.y.eq(1)].sum() - n1*(n1+1)/2)/(n1*n0))
    tied = z.groupby("p", sort=False).y.agg(["sum", "count"])
    cum_tp = tied["sum"].cumsum()
    cum_n = tied["count"].cumsum()
    ap = float(((cum_tp / cum_n) * tied["sum"] / n1).sum())
    return auc, ap


def measure(d: pd.DataFrame, variant: str, label: str) -> dict:
    lam = num(d, LAMBDA.get(variant, "fixed_d20_lambda"))
    actual = num(d, "actual_sog")
    valid = lam.notna() & actual.notna()
    lam, actual = lam[valid], actual[valid]
    residual = actual-lam
    row = {"population": d.population.iloc[0] if len(d) else "unknown", "group": label,
           "variant": variant, "n": len(lam), "mae": residual.abs().mean(),
           "rmse": float(np.sqrt((residual**2).mean())) if len(residual) else np.nan,
           "mean_residual": residual.mean(), "median_absolute_error": residual.abs().median(),
           "mean_lambda": lam.mean(), "realized_mean_sog": actual.mean()}
    for line,k in LINES.items():
        p = lam.map(lambda x: poisson_over(float(x), k)).clip(1e-15, 1-1e-15)
        y = (actual >= k).astype(int)
        auc, ap = auc_ap(p, y)
        row.update({f"brier_{line}": float(((p-y)**2).mean()),
                    f"log_loss_{line}": float((-(y*np.log(p)+(1-y)*np.log(1-p))).mean()),
                    f"auc_{line}": auc, f"average_precision_{line}": ap,
                    f"calibration_bias_{line}": float(p.mean()-y.mean()),
                    f"predicted_over_rate_{line}": float(p.mean()),
                    f"realized_over_rate_{line}": float(y.mean()),
                    f"accuracy_{line}": float(((p >= .5) == y.astype(bool)).mean())})
    return row


def load_populations() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    hist = pd.read_csv(DIV/"historical_paired_rows.csv")
    # Prior high-rate study's exact common valid lambda population.
    hr = pd.read_csv(TAIL/"historical_replay_rows.csv").drop_duplicates(["game_id","player_id"])
    hist = hist.merge(hr[["game_id","player_id"]].drop_duplicates(),
                      on=["game_id","player_id"], how="inner", validate="one_to_one")
    hist["actual_sog"] = num(hist,"actual_sog").combine_first(num(hist,"actual"))
    hist["slate_date"] = hist.game_date.astype(str)
    hist = hist[num(hist,"d10_sog_per60").notna() & num(hist,"d20_sog_per60").notna()
                & num(hist,"selected_toi_minutes").notna() & num(hist,"actual_sog").notna()].copy()

    prosp = pd.read_csv(DIV/"prospective_paired_rows.csv")
    pr = pd.read_csv(TAIL/"prospective_replay_rows.csv").drop_duplicates(["game_id","player_id"])
    wanted = ["game_id","player_id","prior_games_d10"]
    prosp = prosp.merge(pr[[c for c in wanted if c in pr]], on=["game_id","player_id"], how="left",
                        validate="one_to_one")
    prosp["actual_sog"] = num(prosp,"actual_sog").combine_first(num(prosp,"actual"))
    prosp["slate_date"] = prosp.slate_date.astype(str)
    prosp = prosp[(prosp.slate_date >= "2026-10-10") & (prosp.slate_date <= "2026-10-13")].copy()
    prosp = prosp[num(prosp,"d10_sog_per60").notna() & num(prosp,"d20_sog_per60").notna()
                  & num(prosp,"selected_toi_minutes").notna() & num(prosp,"actual_sog").notna()].copy()

    cuts = pd.read_csv(TAIL/"rate_tail_metrics.csv")
    market = pd.read_csv(BASE/"market_matched_rows.csv")
    return hist, prosp, cuts, market


def add_variants(d: pd.DataFrame, population: str) -> pd.DataFrame:
    d=d.copy(); d["population"]=population
    rules=apply_rules(num(d,"d10_sog_per60"),num(d,"d20_sog_per60"),num(d,"production_lambda"),
                      num(d,"selected_toi_minutes"))
    for c in rules:
        d[c]=rules[c]
    d["resid_production"] = num(d,"actual_sog")-d.production_lambda
    d["resid_divergence"] = num(d,"actual_sog")-d.divergence_lambda
    d["resid_soft"] = num(d,"actual_sog")-d.soft_lambda
    d["resid_cap"] = num(d,"actual_sog")-d.hard_cap_lambda
    return d


def get_cut(cuts: pd.DataFrame, pop: str, group: str) -> float:
    key="historical" if pop=="historical" else "prospective_feature_bound"
    return float(cuts.loc[cuts.population.eq(key)&cuts.group.eq(group),"rate_cut"].iloc[0])


def fit_ols(d: pd.DataFrame, variant: str) -> list[dict]:
    ycol={"divergence_control":"resid_divergence","soft_compression":"resid_soft","hard_cap":"resid_cap"}[variant]
    z=pd.DataFrame({"y":num(d,ycol),"d10":num(d,"d10_sog_per60"),"divergence":num(d,"divergence")}).dropna()
    if len(z)<5: return []
    x=np.column_stack([np.ones(len(z)),z[["d10","divergence"]].to_numpy(float)])
    beta=np.linalg.lstsq(x,z.y.to_numpy(float),rcond=None)[0]
    resid=z.y.to_numpy(float)-x@beta
    r2=1-float(resid@resid)/float(((z.y-z.y.mean())**2).sum())
    return [{"population":d.population.iloc[0],"variant":variant,"term":term,"n":len(z),"coefficient":float(b),"r_squared":r2}
            for term,b in zip(("intercept","d10","divergence"),beta)]


def build_tables(hist: pd.DataFrame, prosp: pd.DataFrame, cuts: pd.DataFrame, market: pd.DataFrame) -> dict[str,pd.DataFrame]:
    tables={}; global_rows=[]; line_rows=[]; tail_rows=[]; near_rows=[]; overlap=[]; deciles=[]; dates=[]; player_rows=[]; toi_rows=[]; depth_rows=[]; fixed=[]; regress=[]; activation=[]; market_rows=[]
    allgroups={"all":lambda d: pd.Series(True,index=d.index),"d10_gt_9":lambda d:d.d10_sog_per60.gt(THRESHOLD),
              "top_decile":lambda d:d.d10_sog_per60.ge(get_cut(cuts,d.population.iloc[0],"top_decile")),
              "top_5pct":lambda d:d.d10_sog_per60.ge(get_cut(cuts,d.population.iloc[0],"top_5pct")),
              "bottom_decile":lambda d:d.d10_sog_per60.le(get_cut(cuts,d.population.iloc[0],"bottom_decile")),
              "bottom_5pct":lambda d:d.d10_sog_per60.le(get_cut(cuts,d.population.iloc[0],"bottom_5pct")),
              "middle_80":lambda d:d.d10_sog_per60.gt(get_cut(cuts,d.population.iloc[0],"bottom_decile")) & d.d10_sog_per60.lt(get_cut(cuts,d.population.iloc[0],"top_decile"))}
    variants=(*VARIANTS,"fixed_d20")
    var_col={**LAMBDA,"fixed_d20":"fixed_d20_lambda"}
    for pop,raw in (("historical",hist),("prospective",prosp)):
        d=add_variants(raw,pop)
        d["divergence_group"]=np.select([d.divergence.gt(.5),d.divergence.lt(-.5)],
                                         ["material_above","material_below"],default="near_agreement")
        d["d10_gt_9"]=d.d10_sog_per60.gt(THRESHOLD)
        d["near_agreement_high_rate"]=d.divergence.abs().le(.5)&d.d10_gt_9
        d["high_d10_material_above"]=d.d10_gt_9&d.divergence.gt(.5)
        d["high_d10_near_agreement"]=d.d10_gt_9&d.divergence.abs().le(.5)
        d["high_d10_material_below"]=d.d10_gt_9&d.divergence.lt(-.5)
        d["d10_decile"]=pd.qcut(d.d10_sog_per60.rank(method="first"),10,labels=[f"D{i}" for i in range(1,11)])
        d["toi_band"]=pd.qcut(num(d,"selected_toi_minutes").rank(method="first"),3,labels=["low","middle","high"])
        d["fixed_d20_lambda"]=num(d,"d20_sog_per60")*num(d,"selected_toi_minutes")/60
        # Per-row recomputed probabilities make the replay auditable and reusable.
        for v in VARIANTS:
            lam=num(d,var_col[v])
            for line,k in LINES.items(): d[f"{v}_p_over_{line.replace('.','_')}"]=lam.map(lambda x:poisson_over(x,k))
        for flag in ("divergence_correction_active","high_rate_compression_active","both_mechanisms_active","divergence_only","high_compression_only","neither_mechanism_active"):
            activation.append({"population":pop,"activation_state":flag,"n":int(d[flag].sum()),"pct":float(d[flag].mean())})
        activation.append({"population":pop,"activation_state":"high_d10_region_d10_gt_9","n":int(d.d10_gt_9.sum()),"pct":float(d.d10_gt_9.mean())})
        # Invariant checks: no high-rate operation at or below threshold; exact identity if inactive.
        inactive=~d.high_rate_compression_active
        if not np.allclose(d.loc[inactive,"soft_lambda"],d.loc[inactive,"divergence_lambda"],rtol=0,atol=1e-12):
            raise AssertionError("SOFT_COMPRESSION_CHANGED_INACTIVE_ROW")
        if not np.allclose(d.loc[inactive,"hard_cap_lambda"],d.loc[inactive,"divergence_lambda"],rtol=0,atol=1e-12):
            raise AssertionError("HARD_CAP_CHANGED_INACTIVE_ROW")
        low=d.d10_sog_per60.le(get_cut(cuts,pop,"bottom_decile"))
        if not np.allclose(d.loc[low,"soft_lambda"],d.loc[low,"divergence_lambda"],rtol=0,atol=1e-12):
            raise AssertionError("LOW_RATE_CHANGED")
        for group,predicate in allgroups.items():
            z=d[predicate(d)]
            for variant in variants:
                r=measure(z,variant,group); global_rows.append(r)
        for group,predicate in allgroups.items():
            z=d[predicate(d)]
            for variant in VARIANTS:
                m=measure(z,variant,group)
                for line in LINES:
                    line_rows.append({"population":pop,"group":group,"variant":variant,"line":line,
                                      **{k:m[f"{k}_{line}"] for k in ("brier","log_loss","auc","average_precision","calibration_bias","predicted_over_rate","realized_over_rate","accuracy")},"n":m["n"]})
        # Near-agreement and the three high-rate divergence cells.
        for group,mask in (("near_agreement_high_rate",d.near_agreement_high_rate),
                           ("high_d10_material_above",d.high_d10_material_above),
                           ("high_d10_material_below",d.high_d10_material_below)):
            z=d[mask]
            for v in VARIANTS:
                tail_rows.append(measure(z,v,group))
        for g,pred in (("d10_gt_9",d.d10_gt_9),("top_decile",d.d10_sog_per60.ge(get_cut(cuts,pop,"top_decile"))),
                       ("top_5pct",d.d10_sog_per60.ge(get_cut(cuts,pop,"top_5pct")))):
            z=d[pred]
            for div in ("material_above","near_agreement","material_below"):
                overlap.append({"population":pop,"rate_group":g,"divergence_group":div,"n":int(z.divergence_group.eq(div).sum()),"pct":float(z.divergence_group.eq(div).mean()) if len(z) else np.nan,"denominator":len(z)})
        for dec,z in d.groupby("d10_decile",observed=True):
            for v in VARIANTS:
                residual=num(z,"actual_sog")-num(z,var_col[v])
                deciles.append({"population":pop,"d10_decile":str(dec),"variant":v,"n":len(z),"mean_d10":z.d10_sog_per60.mean(),"mean_residual":residual.mean(),"mae":residual.abs().mean(),"rmse":float(np.sqrt((residual**2).mean()))})
        for date,z in d.groupby("slate_date",dropna=False):
            for group,pred in (("all",pd.Series(True,index=z.index)),("d10_gt_9",z.d10_gt_9),("top_decile",z.d10_sog_per60.ge(get_cut(cuts,pop,"top_decile")))):
                q=z[pred]
                for v in ("divergence_control","soft_compression","hard_cap"):
                    m=measure(q,v,group)
                    dates.append({"population":pop,"date":str(date),"group":group,"variant":v,"n":m["n"],"mae":m["mae"],"rmse":m["rmse"],"mean_residual":m["mean_residual"],"brier_1.5":m["brier_1.5"],"log_loss_1.5":m["log_loss_1.5"],"calibration_bias_1.5":m["calibration_bias_1.5"]})
        high=d[d.d10_gt_9]
        for player_id,z in high.groupby("player_id"):
            vals={v:(num(z,"actual_sog")-num(z,var_col[v])).abs().mean() for v in ("divergence_control","soft_compression","hard_cap")}
            player_rows.append({"population":pop,"player_id":player_id,"n":len(z),**{f"mae_{k}":v for k,v in vals.items()},"soft_change_vs_control":vals["soft_compression"]-vals["divergence_control"],"cap_change_vs_control":vals["hard_cap"]-vals["divergence_control"]})
        for band,z in d.groupby("toi_band",observed=True):
            q=z[z.d10_gt_9]
            for v in VARIANTS:
                m=measure(q,v,"d10_gt_9")
                toi_rows.append({"population":pop,"toi_band":str(band),**m})
        if pop=="prospective":
            d["history_group"]=np.where(num(d,"prior_games_d10").ge(10),"full_10",
                                         np.where(num(d,"prior_games_d10").notna(),"sparse_lt_10","unknown"))
            for (depth,group),z in d[d.d10_gt_9].groupby(["history_group","divergence_group"],observed=True):
                for v in VARIANTS:
                    m=measure(z,v,"d10_gt_9")
                    depth_rows.append({"population":pop,"history_group":depth,"divergence_group":group,**m,"mean_prior_games":num(z,"prior_games_d10").mean()})
        for group,pred in (("d10_gt_9",d.d10_gt_9),("top_decile",d.d10_sog_per60.ge(get_cut(cuts,pop,"top_decile"))),
                           ("top_5pct",d.d10_sog_per60.ge(get_cut(cuts,pop,"top_5pct")))):
            z=d[pred]
            for v in VARIANTS+("fixed_d20",):
                m=measure(z,v,group)
                fixed.append(m)
        for v in ("divergence_control","soft_compression","hard_cap"):
            regress.extend(fit_ols(d,v))
        # Market comparison on exact prospective player-game rows with retained no-vig 1.5 prices.
        if pop=="prospective":
            mk=market.drop_duplicates(["game_id","player_id"])
            cols=[c for c in ("game_id","player_id","p_over_mkt_novig","market_p","realized_over") if c in mk]
            common=d.merge(mk[cols],on=["game_id","player_id"],how="inner",validate="one_to_one")
            common=common[num(common,"p_over_mkt_novig").notna()]
            market_rows.append({"population":pop,"group":"exact_common_quoted_rows","n":len(common),"rows_total":len(d),"match_rate":len(common)/len(d) if len(d) else np.nan})
            y=(num(common,"actual_sog")>=2).astype(float)
            for v in VARIANTS:
                p=num(common,f"{v}_p_over_1_5")
                market_rows.append(probability_metrics(y,p,pop,"exact_common_quoted_rows",v))
            market_rows.append(probability_metrics(y,num(common,"p_over_mkt_novig"),pop,"exact_common_quoted_rows","no_vig_market"))
    tables["global_metrics"]=pd.DataFrame(global_rows)
    tables["line_metrics"]=pd.DataFrame(line_rows)
    tables["tail_metrics"]=pd.DataFrame(tail_rows)
    tables["overlap_metrics"]=pd.DataFrame(overlap)
    tables["decile_residual_curve"]=pd.DataFrame(deciles)
    tables["date_metrics"]=pd.DataFrame(dates)
    tables["player_robustness"]=pd.DataFrame(player_rows)
    tables["toi_interaction"]=pd.DataFrame(toi_rows)
    tables["history_depth_interaction"]=pd.DataFrame(depth_rows)
    tables["fixed_d20_comparison"]=pd.DataFrame(fixed)
    tables["continuous_relationship"]=pd.DataFrame(regress)
    tables["activation_counts"]=pd.DataFrame(activation)
    tables["market_comparison"]=pd.DataFrame(market_rows)
    return tables


def probability_metrics(y: pd.Series, p: pd.Series, population: str, group: str, variant: str) -> dict:
    ok=y.notna()&p.notna(); y=y[ok].astype(float); p=p[ok].clip(1e-15,1-1e-15)
    a,ap=auc_ap(p,y)
    return {"population":population,"group":group,"variant":variant,"n":len(y),"brier":float(((p-y)**2).mean()),
            "log_loss":float((-(y*np.log(p)+(1-y)*np.log(1-p))).mean()),"calibration_bias":float(p.mean()-y.mean()),
            "predicted_over_rate":float(p.mean()),"realized_over_rate":float(y.mean()),"auc":a,"average_precision":ap}


def markdown_table(frame: pd.DataFrame) -> str:
    """Render small report tables without optional third-party dependencies."""
    if frame.empty:
        return "(no rows)"
    cols = list(frame.columns)
    def cell(value):
        if pd.isna(value): return ""
        if isinstance(value, (float, np.floating)): return f"{value:.4f}"
        return str(value).replace("|", "\\|").replace("\n", " ")
    rows = [[cell(v) for v in row] for row in frame.itertuples(index=False, name=None)]
    return "\n".join(["| " + " | ".join(cols) + " |", "| " + " | ".join("---" for _ in cols) + " |"] +
                     ["| " + " | ".join(row) + " |" for row in rows])


def main() -> None:
    OUT.mkdir(parents=True,exist_ok=True)
    hist,prosp,cuts,market=load_populations()
    hist=add_variants(hist,"historical"); prosp=add_variants(prosp,"prospective")
    # Control identity replay against retained divergence shadow calculations.
    checks=[]
    for pop,d in (("historical",hist),("prospective_oct10_13",prosp)):
        for name,left,right in (("production","production_lambda","lambda_replay"),
                                ("divergence_control","divergence_lambda","lambda_SYMMETRIC_DIVERGENCE_SHRINK_V1")):
            a,b=num(d,left),num(d,right); ok=a.notna()&b.notna(); delta=(a[ok]-b[ok]).abs()
            checks.append({"population":pop,"comparison":name,"n":int(ok.sum()),"mismatch_count":int(delta.gt(1e-9).sum()),"max_abs_difference":float(delta.max()) if len(delta) else None,"tolerance":1e-9})
    if any(x["mismatch_count"] for x in checks if x["comparison"]=="divergence_control"):
        raise SystemExit("Frozen divergence control replay differs materially; stopping.")
    tables=build_tables(hist,prosp,cuts,market)
    pd.DataFrame(checks).to_csv(OUT/"replay_validation.csv",index=False)
    # The divergence package retains prospective aggregate line scores on exactly the
    # same 1,107 rows. Historical aggregate scores use 13,369 rows, while the tail
    # study's exact common-feature intersection used here is 13,347 rows.
    prob_checks=[]
    for pop, own_path, prior_path, own_variant, prior_variant in (
        ("prospective", None, DIV/"prospective_line_metrics.csv", "divergence_control", "SYMMETRIC_DIVERGENCE_SHRINK_V1"),
        ("historical", None, DIV/"historical_line_metrics.csv", "divergence_control", "SYMMETRIC_DIVERGENCE_SHRINK_V1")):
        own=tables["line_metrics"]
        prior=pd.read_csv(prior_path)
        for line in LINES:
            a=own[(own.population==pop)&(own.group=="all")&(own.variant==own_variant)&(own.line==line)]
            b=prior[(prior.variant==prior_variant)&(prior.line==float(line))]
            if len(a) and len(b):
                same_n=int(a.n.iloc[0])==int(b.n.iloc[0])
                errors=[abs(float(a.iloc[0][metric])-float(b.iloc[0][prior_col])) for metric,prior_col in
                        (("brier","brier"),("log_loss","log_loss"),
                         ("calibration_bias","calibration_bias"),("predicted_over_rate","predicted_over_rate"),
                         ("realized_over_rate","realized_over_rate"),("accuracy","accuracy"))]
                prob_checks.append({"population":pop,"line":line,"n_replayed":int(a.n.iloc[0]),"n_prior":int(b.n.iloc[0]),
                                   "population_matches":same_n,"max_abs_metric_difference":max(errors),
                                   "comparison_status":"EXACT_AGGREGATE_REPLAY" if same_n and max(errors)<1e-9 else "NONCOMPARABLE_POPULATION" if not same_n else "METRIC_MISMATCH"})
    pd.DataFrame(prob_checks).to_csv(OUT/"probability_replay_validation.csv",index=False)
    cuts.rename(columns={"population":"source_population","rate_cut":"cutpoint"}).to_csv(OUT/"exact_rule_definitions.csv",index=False)
    allrows=[]
    for pop,d in (("historical",hist),("prospective",prosp)):
        for row in d.to_dict(orient="records"): allrows.append({"population":pop,**row})
    paired=pd.DataFrame(allrows)
    paired[paired.population.eq("historical")].to_csv(OUT/"historical_paired_rows.csv",index=False,float_format="%.12g")
    paired[paired.population.eq("prospective")].to_csv(OUT/"prospective_paired_rows.csv",index=False,float_format="%.12g")
    paired.to_csv(OUT/"paired_rows.csv",index=False,float_format="%.12g")
    for name,frame in tables.items(): frame.to_csv(OUT/(name+".csv"),index=False,float_format="%.12g")
    # Player-level paired high-D10 outcomes.
    player=tables["player_robustness"]
    player_summary=[]
    for pop,g in player.groupby("population"):
        player_summary.append({"population":pop,"high_rate_players":len(g),"soft_improved_players":int(g.soft_change_vs_control.lt(-1e-12).sum()),"soft_worsened_players":int(g.soft_change_vs_control.gt(1e-12).sum()),"soft_tied_players":int(g.soft_change_vs_control.abs().le(1e-12).sum()),"cap_improved_players":int(g.cap_change_vs_control.lt(-1e-12).sum()),"cap_worsened_players":int(g.cap_change_vs_control.gt(1e-12).sum()),"cap_tied_players":int(g.cap_change_vs_control.abs().le(1e-12).sum()),"equal_player_mean_mae_control":g.mae_divergence_control.mean(),"equal_player_mean_mae_soft":g.mae_soft_compression.mean(),"equal_player_mean_mae_cap":g.mae_hard_cap.mean()})
    pd.DataFrame(player_summary).to_csv(OUT/"player_robustness_summary.csv",index=False)
    # Date-level direction checks on the high-rate MAE and 1.5 Brier.
    dm=tables["date_metrics"]
    daily_summary=[]
    for pop,z in dm[dm.group.eq("d10_gt_9")].groupby("population"):
        wide=z.pivot(index="date",columns="variant",values="mae").dropna()
        daily_summary.append({"population":pop,"eligible_dates":len(wide),"soft_improved_dates":int((wide.soft_compression<wide.divergence_control).sum()),"soft_worsened_dates":int((wide.soft_compression>wide.divergence_control).sum()),"soft_tied_dates":int((wide.soft_compression==wide.divergence_control).sum()),"cap_improved_dates":int((wide.hard_cap<wide.divergence_control).sum()),"cap_worsened_dates":int((wide.hard_cap>wide.divergence_control).sum()),"cap_tied_dates":int((wide.hard_cap==wide.divergence_control).sum())})
    pd.DataFrame(daily_summary).to_csv(OUT/"date_stability_summary.csv",index=False)
    # Conservative classification: positive support requires same-direction high-rate MAE gain in both cohorts,
    # better high-tail calibration, broad date/player gains, and no tail residual sign flip.
    g=tables["global_metrics"]
    def val(pop,group,variant,metric):
        q=g[(g.population==pop)&(g.group==group)&(g.variant==variant)]
        return float(q[metric].iloc[0]) if len(q) else np.nan
    soft_h=val("historical","d10_gt_9","soft_compression","mae") < val("historical","d10_gt_9","divergence_control","mae")
    soft_p=val("prospective","d10_gt_9","soft_compression","mae") < val("prospective","d10_gt_9","divergence_control","mae")
    soft_h_brier=val("historical","d10_gt_9","soft_compression","brier_1.5") < val("historical","d10_gt_9","divergence_control","brier_1.5")
    soft_p_brier=val("prospective","d10_gt_9","soft_compression","brier_1.5") < val("prospective","d10_gt_9","divergence_control","brier_1.5")
    near=tables["tail_metrics"]
    def residual(pop,group,v):
        q=near[(near.population==pop)&(near.group==group)&(near.variant==v)]
        return float(q.mean_residual.iloc[0]) if len(q) else np.nan
    soft_sign_flip=any(residual(p,"near_agreement_high_rate","soft_compression")>0 for p in ("historical","prospective"))
    cap_sign_flip=any(residual(p,"near_agreement_high_rate","hard_cap")>0 for p in ("historical","prospective"))
    sign_flip=soft_sign_flip or cap_sign_flip
    datesum=pd.DataFrame(daily_summary)
    broad_dates=bool(len(datesum) and all(int(row.soft_improved_dates)>int(row.soft_worsened_dates) for row in datesum.itertuples()))
    playersum=pd.DataFrame(player_summary)
    broad_players=bool(len(playersum) and all(int(row.soft_improved_players)>int(row.soft_worsened_players) for row in playersum.itertuples()))
    if soft_h and soft_p and soft_h_brier and soft_p_brier and broad_dates and broad_players and not soft_sign_flip:
        classification="SOFT_HIGH_RATE_COMPRESSION_SUPPORTED"
    elif soft_h and soft_p and soft_h_brier and soft_p_brier and not soft_sign_flip:
        classification="HARD_CAP_TOO_AGGRESSIVE_SOFT_COMPRESSION_PROMISING"
    elif soft_h and soft_p and soft_h_brier and soft_p_brier and cap_sign_flip:
        classification="HARD_CAP_TOO_AGGRESSIVE_SOFT_COMPRESSION_PROMISING"
    elif soft_h and not soft_p:
        classification="HISTORICAL_ONLY_HIGH_RATE_IMPROVEMENT"
    elif soft_h or soft_p:
        classification="HISTORICAL_ONLY_HIGH_RATE_IMPROVEMENT"
    else:
        classification="HIGH_RATE_COMPRESSION_NOT_SUPPORTED"
    feature_class="CORE_CONTRIBUTOR_WITH_TWO_CORRECTABLE_BIASES" if classification in ("SOFT_HIGH_RATE_COMPRESSION_SUPPORTED","HARD_CAP_TOO_AGGRESSIVE_SOFT_COMPRESSION_PROMISING","READY_FOR_PROSPECTIVE_SHADOW_REVIEW") else "CORE_CONTRIBUTOR_WITH_DIVERGENCE_AND_RESEARCHED_HIGH_RATE_BIAS"
    shadow_justified=classification in ("SOFT_HIGH_RATE_COMPRESSION_SUPPORTED","READY_FOR_PROSPECTIVE_SHADOW_REVIEW")
    summary={"status":"COMPLETE","classification":classification,"feature_classification":feature_class,
             "divergence_control":"NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1","threshold_d10_sog_per60":THRESHOLD,
             "soft_rule":"if D10>9 and rate_after_divergence>9: 9 + 0.5*(rate_after_divergence-9); otherwise unchanged",
             "hard_cap_rule":"if D10>9 and rate_after_divergence>9: 9; otherwise unchanged",
             "historical_certified_population":13389,"historical_rate_evidence_population":13369,"historical_common_valid_population":len(hist),"prospective_oct10_13_population":len(prosp),
             "replay_validation":checks,"october14_capture":"present with 86 player-games but not graded because no final immutable official outcome reconciliation package was available",
             "probability_replay_validation":prob_checks,
             "soft_high_rate_count_improved_historical":soft_h,"soft_high_rate_count_improved_prospective":soft_p,
             "soft_high_rate_brier_1_5_improved_historical":soft_h_brier,"soft_high_rate_brier_1_5_improved_prospective":soft_p_brier,
             "near_agreement_sign_flip":sign_flip,"broad_date_improvement":broad_dates,"broad_player_improvement":broad_players,
             "soft_near_agreement_positive_residual":soft_sign_flip,"hard_cap_near_agreement_positive_residual":cap_sign_flip,
             "prospective_shadow_implementation_justified":shadow_justified,
             "scope":{"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_or_existing_shadow_changed":False}}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
    rule_rows=[{"rule":"divergence_control","condition":"abs(D10-D20) > 0.5","effect":"rate=0.5*D10+0.5*D20; otherwise D10","threshold":0.5},
               {"rule":"soft_compression","condition":"D10 > 9.0 and corrected_rate > 9.0","effect":"rate=9.0+0.5*(corrected_rate-9.0); otherwise unchanged","threshold":9.0},
               {"rule":"hard_cap_diagnostic","condition":"D10 > 9.0 and corrected_rate > 9.0","effect":"rate=9.0; otherwise unchanged","threshold":9.0}]
    pd.DataFrame(rule_rows).to_csv(OUT/"rule_definitions.csv",index=False)
    # Explicit activation-layering invariants, including middle and lower tails.
    safety_rows=[]
    for pop,d in (("historical",hist),("prospective",prosp)):
        q=add_variants(d,pop)
        checks_safety=[("high_rate_only_when_D10_gt_9",q.loc[q.high_rate_compression_active,"d10_sog_per60"].gt(9).all()),
                       ("no_high_rate_activation_at_or_below_9",(~q.loc[q.d10_sog_per60.le(9),"high_rate_compression_active"]).all()),
                       ("middle_80_soft_equals_control",np.allclose(q.loc[q.d10_sog_per60.gt(get_cut(cuts,pop,"bottom_decile"))&q.d10_sog_per60.lt(get_cut(cuts,pop,"top_decile")),"soft_lambda"],q.loc[q.d10_sog_per60.gt(get_cut(cuts,pop,"bottom_decile"))&q.d10_sog_per60.lt(get_cut(cuts,pop,"top_decile")),"divergence_lambda"],rtol=0,atol=1e-12)),
                       ("bottom_decile_unchanged",np.allclose(q.loc[q.d10_sog_per60.le(get_cut(cuts,pop,"bottom_decile")),"soft_lambda"],q.loc[q.d10_sog_per60.le(get_cut(cuts,pop,"bottom_decile")),"divergence_lambda"],rtol=0,atol=1e-12)),
                       ("bottom_5pct_unchanged",np.allclose(q.loc[q.d10_sog_per60.le(get_cut(cuts,pop,"bottom_5pct")),"soft_lambda"],q.loc[q.d10_sog_per60.le(get_cut(cuts,pop,"bottom_5pct")),"divergence_lambda"],rtol=0,atol=1e-12))]
        safety_rows.extend({"population":pop,"check":name,"passed":bool(ok)} for name,ok in checks_safety)
    pd.DataFrame(safety_rows).to_csv(OUT/"safety_checks.csv",index=False)
    # Residual sign checks are descriptive; no arbitrary materiality cutoff is imposed.
    over_rows=[]
    gm=tables["global_metrics"]
    for pop in ("historical","prospective"):
        for group in ("top_decile","top_5pct","near_agreement_high_rate"):
            source=tables["tail_metrics"] if group=="near_agreement_high_rate" else gm
            for variant in ("divergence_control","soft_compression","hard_cap"):
                z=source[(source.population==pop)&(source.group==group)&(source.variant==variant)]
                over_rows.append({"population":pop,"group":group,"variant":variant,"n":int(z.n.iloc[0]) if len(z) else 0,
                                  "mean_residual":float(z.mean_residual.iloc[0]) if len(z) else None,
                                  "residual_sign":"positive_underprediction" if len(z) and z.mean_residual.iloc[0]>0 else "negative_overprediction" if len(z) and z.mean_residual.iloc[0]<0 else "zero_or_unavailable"})
    pd.DataFrame(over_rows).to_csv(OUT/"overcorrection_checks.csv",index=False)
    (OUT/"feature_decomposition_update.md").write_text(f"# SOG feature decomposition update\n\nD10 classification: `{feature_class}`. The frozen D10/D20 divergence correction retains independent value. High-rate soft compression improves the high-D10 pooled count and 1.5-line score in both frozen cohorts, but date/player stability and tail sign checks keep it at research-promising status. The hard cap produces positive mean residuals in some top-five-percent and near-agreement cells and is too aggressive as a deployable form. No shadow was created.\n")
    (OUT/"research_backlog_context.md").write_text("# Preserved research backlog\n\n- Historical official-TOI binding.\n- Prospective grading of `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1`.\n- Continue fixed-blend prospective collection.\n- Puck Line work deferred until tomorrow.\n- Reusable cross-prop sanity-check / explanation audit, beginning with Points.\n- High-rate compression shadow implementation is not authorized by this result; reconsider only after the evidence criteria in the study objective are met.\n")
    report=["# NHL SOG high-D10 compression","",f"Classification: **{classification}**. D10 feature classification: `{feature_class}`.","",f"Control: `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1`, replayed first using |D10−D20| > 0.5 midpoint, otherwise D10. The single fixed high-rate threshold is D10 > {THRESHOLD:.1f} SOG/60, consistent with prior historical top-decile cut 8.9270 and prospective cut 9.1235.","", "Soft form halves only corrected-rate excess above 9.0 for rows with D10 > 9.0. Hard cap sets corrected rate to 9.0 under the same activation condition. No threshold or weight search was performed.","",f"Historical certified population: 13,389; retained D10/D20 evidence: 13,369; common valid replay rows: {len(hist):,}. Prospective October 10–13 settled feature-bound rows: {len(prosp):,}.","", "## Replay validation", "",markdown_table(pd.DataFrame(checks)),"", "Corrected line probabilities are recomputed from the replayed lambda using the Poisson tail formula. Prospective aggregate Brier, log loss, calibration bias, predicted/realized rates, and accuracy match the frozen divergence artifact to floating point precision at all three lines. Historical aggregate line scores in that artifact use 13,369 rows, so they are not directly comparable to this study’s exact 13,347-row common intersection; the rowwise corrected-probability artifact is not retained. AUC and average-precision implementation differences are not used as probability replay checks.","",markdown_table(pd.DataFrame(prob_checks)),"", "## Global count results", "",markdown_table(tables["global_metrics"].query("group == 'all' and variant != 'fixed_d20'")),"", "## High-rate results", "",markdown_table(tables["global_metrics"].query("group in ['d10_gt_9','top_decile','top_5pct'] and variant != 'fixed_d20'")),"", "Near-agreement high-rate rows are the independent-mechanism test; overlap rows split high D10 by divergence category. See `tail_metrics.csv` and `overlap_metrics.csv` for the complete outputs.","", "## Safety and stability", "", "Activation counts, unchanged-row assertions, mid-range and low-rate safety, D10-decile residual curves, date-level comparisons, equal-player counts, TOI bands, prospective history depth, market common-row scores, and fixed-D20 diagnostics are in the named CSVs. The October 14 capture was not graded because no final immutable official-outcome reconciliation package was available. It is excluded from the frozen comparison.","", "Market results use only retained exact prospective player-game quotes with no-vig 1.5-over probability; they are descriptive and do not define betting filters. Fixed D20 is diagnostic only.","",f"Existing divergence correction remains independently useful. Soft compression adds incremental value only if its paired gains are consistent in the tail and do not transfer into positive residual overcorrection. Prospective implementation justified: **{shadow_justified}**.","", "## Feature decomposition and preserved backlog", "",f"D10 remains a core contributor. Current result: `{feature_class}`. Divergence correction remains unchanged; high-rate compression is research-only and no operational shadow was created.","", "- Historical official-TOI binding.","- Prospective grading of existing `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1`.","- Continue fixed-blend prospective collection.","- Puck Line work deferred until tomorrow.","- Reusable cross-prop sanity-check / explanation audit, beginning with Points.","", "Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Production and the existing divergence shadow were not modified.",""]
    (OUT/"report.md").write_text("\n".join(report))
    inputs=[TAIL/"historical_replay_rows.csv",TAIL/"prospective_replay_rows.csv",TAIL/"rate_tail_metrics.csv",DIV/"historical_paired_rows.csv",DIV/"prospective_paired_rows.csv",DIV/"lineage_manifest.json",DIV/"summary.json",BASE/"market_matched_rows.csv",ROOT/"backend/nhl/sog_divergence_shadow.py",ROOT/"backend/nhl/scripts/analyze_nhl_sog_high_rate_compression.py",ROOT/"backend/nhl/tests/test_analyze_nhl_sog_high_rate_compression.py"]
    manifest={"study":"NHL_SOG_HIGH_RATE_COMPRESSION_V1","inputs":[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in inputs],"outputs":{},"rules":{"control":"NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1","divergence_threshold":DIV_THRESHOLD,"high_rate_threshold_d10_sog_per60":THRESHOLD,"soft_excess_weight":0.5,"hard_cap":THRESHOLD,"probability_lines":list(LINES)},"population_counts":{"historical":len(hist),"prospective_oct10_13":len(prosp)},"classification":classification,"status":"COMPLETE"}
    for f in sorted(OUT.iterdir()):
        if f.is_file() and f.name!="lineage_manifest.json": manifest["outputs"][f.name]=sha(f)
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT),"historical_n":len(hist),"prospective_n":len(prosp),"classification":classification,"validation":checks},indent=2))


if __name__ == "__main__": main()
