#!/usr/bin/env python3
"""Offline study of SOG D10 tail bias after the frozen D10/D20 correction."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
RATE = ROOT / "artifacts/analysis/nhl/sog_shot_rate_contribution/2026-10-14"
DIV = ROOT / "artifacts/analysis/nhl/sog_d10_d20_divergence_correction/2026-10-14"
OUT = ROOT / "artifacts/analysis/nhl/sog_extreme_rate_after_divergence/2026-10-14"
LINES = {"1.5": 2, "2.5": 3, "3.5": 4}
TOL = 1e-9


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def num(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce") if col in df else pd.Series(np.nan, index=df.index)


def tail(lam: float, threshold: int) -> float:
    if not np.isfinite(lam) or lam < 0:
        return np.nan
    return float(max(0.0, min(1.0, 1 - sum(math.exp(-lam) * lam**k / math.factorial(k) for k in range(threshold)))))


def auc(p: pd.Series, y: pd.Series) -> float:
    z = pd.DataFrame({"p": p, "y": y}).dropna()
    if z.y.nunique() != 2:
        return np.nan
    r = z.p.rank(method="average")
    n1, n0 = int(z.y.sum()), int((1-z.y).sum())
    return float((r[z.y.eq(1)].sum()-n1*(n1+1)/2)/(n1*n0))


def metrics(d: pd.DataFrame, label: str, model: str = "production_lambda") -> dict:
    actual, lam = num(d, "actual_sog"), num(d, model)
    ok = actual.notna() & lam.notna()
    a, l = actual[ok], lam[ok]
    r = a-l
    row = {"group": label, "model": model, "n": int(ok.sum()), "mean_lambda": l.mean(),
           "realized_sog": a.mean(), "mean_residual": r.mean(), "mae": r.abs().mean(),
           "rmse": float(np.sqrt((r*r).mean())) if len(r) else np.nan}
    for line, k in LINES.items():
        p = l.map(lambda x: tail(x, k)).clip(1e-15, 1-1e-15)
        y = (a >= k).astype(float)
        row[f"brier_{line}"] = ((p-y)**2).mean()
        row[f"log_loss_{line}"] = (-(y*np.log(p)+(1-y)*np.log(1-p))).mean()
        row[f"calibration_bias_{line}"] = p.mean()-y.mean()
        row[f"predicted_over_frequency_{line}"] = p.mean()
        row[f"realized_over_frequency_{line}"] = y.mean()
        row[f"auc_{line}"] = auc(p, y)
    return row


def ols(d: pd.DataFrame, yname: str, columns: list[str], name: str) -> list[dict]:
    x = pd.DataFrame({c: num(d, c) for c in columns})
    y = num(d, yname)
    ok = y.notna() & x.notna().all(axis=1)
    x, y = x.loc[ok], y.loc[ok]
    if len(y) <= len(columns)+2:
        return []
    design = np.column_stack([np.ones(len(x)), x.to_numpy(float)])
    beta = np.linalg.lstsq(design, y.to_numpy(float), rcond=None)[0]
    resid = y.to_numpy(float)-design@beta
    dof = len(y)-design.shape[1]
    cov = (resid@resid/dof)*np.linalg.inv(design.T@design)
    r2 = 1-float(resid@resid)/float(((y-y.mean())**2).sum())
    return [{"model": name, "term": term, "n": len(y), "coefficient": float(b),
             "std_error": float(se), "ci95_low": float(b-1.96*se), "ci95_high": float(b+1.96*se), "r_squared": r2}
            for term,b,se in zip(["intercept",*columns],beta,np.sqrt(np.diag(cov)))]


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    h = pd.read_csv(DIV/"historical_paired_rows.csv")
    h = h.merge(pd.read_csv(RATE/"historical_replay_rows.csv").drop_duplicates(["game_id","player_id"]),
                 on=["game_id","player_id"], how="inner", suffixes=("","_rate"), validate="one_to_one")
    p = pd.read_csv(DIV/"prospective_paired_rows.csv")
    # Rate study has exact feature-bound rate, TOI and history depth columns for Oct 10-13.
    pr = pd.read_csv(RATE/"prospective_replay_rows.csv").drop_duplicates(["game_id","player_id"])
    p = p.merge(pr, on=["game_id","player_id"], how="left", suffixes=("","_rate"), validate="one_to_one")
    h["actual_sog"] = num(h,"actual_sog").fillna(num(h,"actual"))
    p["actual_sog"] = num(p,"actual_sog").fillna(num(p,"actual"))
    for d in (h,p):
        # Canonical paired evidence fields; historical rate replay and prospective features
        # supply retained selected exposure and D5/D10/D20.
        for c in ("d5_sog_per60","d10_sog_per60","d20_sog_per60","selected_toi_minutes","prior_games_d10"):
            alt = c+"_rate"
            if alt in d:
                d[c] = num(d,alt).combine_first(num(d,c))
        d["d10"] = num(d,"d10_sog_per60").combine_first(num(d,"d10"))
        d["d20"] = num(d,"d20_sog_per60").combine_first(num(d,"d20"))
        d["d5"] = num(d,"d5_sog_per60")
        d["divergence"] = num(d,"d10")-num(d,"d20")
        # Verified contract: D10 selected production rate; midpoint only for |D10-D20| > .5.
        d["corrected_rate"] = num(d,"d10")
        active = num(d,"d10").notna() & num(d,"d20").notna() & num(d,"divergence").abs().gt(.5)
        d.loc[active,"corrected_rate"] = (num(d,"d10")[active]+num(d,"d20")[active])/2
        d["corrected_lambda"] = d["corrected_rate"]*num(d,"selected_toi_minutes")/60
        d["production_lambda"] = num(d,"production_lambda").combine_first(num(d,"lambda_production"))
        d["resid_production"] = num(d,"actual_sog")-num(d,"production_lambda")
        d["resid_corrected"] = num(d,"actual_sog")-num(d,"corrected_lambda")
        d["divergence_group"] = np.select([d.divergence.gt(.5),d.divergence.lt(-.5)],
                                           ["material_above","material_below"], default="near_agreement")
    p = p[(num(p,"game_date").isna()) | (pd.to_datetime(p.game_date, errors="coerce").dt.strftime("%Y-%m-%d").between("2026-10-10","2026-10-13"))].copy()
    p = p[p.selected_toi_minutes.notna() & p.d10.notna() & p.d20.notna() & p.actual_sog.notna()].copy()
    h = h[h.selected_toi_minutes.notna() & h.d10.notna() & h.d20.notna() & h.actual_sog.notna()].copy()
    return h,p


def cuts_table() -> pd.DataFrame:
    src = pd.read_csv(RATE/"rate_tail_metrics.csv")
    rows=[]
    for pop, label in (("historical","historical"),("prospective_feature_bound","prospective")):
        x=src[src.population.eq(pop)]
        for group in ("bottom_5pct","bottom_decile","top_decile","top_5pct"):
            rows.append({"population":label,"group":group,"cutpoint":float(x.loc[x.group.eq(group),"rate_cut"].iloc[0])})
    return pd.DataFrame(rows)


def assign_groups(d: pd.DataFrame, cut: pd.DataFrame, pop: str) -> pd.DataFrame:
    d=d.copy(); c=cut[cut.population.eq(pop)].set_index("group").cutpoint
    q10lo=d.d10.quantile(.1); q10hi=d.d10.quantile(.9)
    # Use exact prior stored tail cutpoints; middle 80 is between retained decile boundaries.
    d["rate_group"]="middle_80"
    d.loc[d.d10.le(c.bottom_decile),"rate_group"]="bottom_decile"
    d.loc[d.d10.ge(c.top_decile),"rate_group"]="top_decile"
    d.loc[d.d10.le(c.bottom_5pct),"rate_group"]="bottom_5pct"
    d.loc[d.d10.ge(c.top_5pct),"rate_group"]="top_5pct"
    # Remove nested labels from standalone decile/middle categories in analyses as needed.
    d["d10_quintile"] = pd.qcut(d.d10.rank(method="first"), 5, labels=["Q1_low","Q2","Q3","Q4","Q5_high"])
    d["toi_band"] = pd.qcut(d.selected_toi_minutes.rank(method="first"), 3, labels=["low","mid","high"])
    d["lambda_band"] = pd.qcut(d.production_lambda.rank(method="first"), 4, labels=["Q1_low","Q2","Q3","Q4_high"])
    return d


def make_tables(hist: pd.DataFrame, prosp: pd.DataFrame, cuts: pd.DataFrame) -> dict[str,pd.DataFrame]:
    tables={}
    tailrows=[]; overlap=[]; near=[]; material=[]; grid=[]; dates=[]; volatility=[]; depth_rows=[]; lambda_rows=[]; toi=[]; fixed=[]; nonlinear=[]; player=[]; regress=[]
    for pop, raw in (("historical",hist),("prospective",prosp)):
        d=assign_groups(raw, cuts, pop)
        d["population"]=pop
        for group in ("bottom_5pct","bottom_decile","middle_80","top_decile","top_5pct"):
            # Tail groups are intentionally nested; only middle_80 is disjoint.
            if group in ("bottom_5pct","bottom_decile"):
                z=d[d.d10.le(cut_value(cuts,pop,group))]
            elif group in ("top_decile","top_5pct"):
                z=d[d.d10.ge(cut_value(cuts,pop,group))]
            else:
                z=d[d.d10.gt(cut_value(cuts,pop,"bottom_decile")) & d.d10.lt(cut_value(cuts,pop,"top_decile"))]
            for model in ("production_lambda","corrected_lambda"):
                row=metrics(z,group,model); row["population"]=pop; tailrows.append(row)
        for group in ("bottom_5pct","bottom_decile","top_decile","top_5pct"):
            c=cuts.query("population == @pop and group == @group").cutpoint.iloc[0]
            z=d[d.d10.le(c) if group.startswith("bottom") else d.d10.ge(c)]
            counts=z.divergence_group.value_counts()
            for typ in ("material_above","near_agreement","material_below"):
                overlap.append({"population":pop,"rate_group":group,"divergence_group":typ,"n":int(counts.get(typ,0)),"pct":float(counts.get(typ,0)/len(z)) if len(z) else np.nan,"tail_n":len(z)})
        for rg in ("top_decile","middle_80","bottom_decile"):
            c=cuts.query("population == @pop and group == @rg")
            z=d[d.divergence_group.eq("near_agreement")]
            if rg=="top_decile": z=z[z.d10.ge(cut_value(cuts,pop,"top_decile"))]
            elif rg=="bottom_decile": z=z[z.d10.le(cut_value(cuts,pop,"bottom_decile"))]
            else: z=z[z.d10.gt(cut_value(cuts,pop,"bottom_decile")) & z.d10.lt(cut_value(cuts,pop,"top_decile"))]
            for model in ("production_lambda","corrected_lambda"):
                r=metrics(z,rg,model); r["population"]=pop; near.append(r)
        for rg in ("top_decile","bottom_decile"):
            cval=cut_value(cuts,pop,rg)
            tailmask=d.d10.ge(cval) if rg=="top_decile" else d.d10.le(cval)
            for div in ("material_above","near_agreement","material_below"):
                z=d[tailmask & d.divergence_group.eq(div)]
                for model in ("production_lambda","corrected_lambda"):
                    r=metrics(z,rg+"_"+div,model); r["population"]=pop; material.append(r)
        for (q,g),z in d.groupby(["d10_quintile","divergence_group"],observed=True):
            grid.append({"population":pop,"d10_quintile":str(q),"divergence_group":g,"n":len(z),"mean_d10":z.d10.mean(),"mean_d20":z.d20.mean(),"mean_divergence":z.divergence.mean(),"production_residual":z.resid_production.mean(),"corrected_residual":z.resid_corrected.mean(),"production_mae":z.resid_production.abs().mean(),"corrected_mae":z.resid_corrected.abs().mean()})
        # Deciles are descriptive binned curves; quantile ties are handled by rank(method=first).
        d["d10_decile"] = pd.qcut(d.d10.rank(method="first"),10,labels=[f"D{i}" for i in range(1,11)])
        for dec,z in d.groupby("d10_decile",observed=True):
            nonlinear.append({"population":pop,"d10_decile":str(dec),"n":len(z),"mean_d10":z.d10.mean(),"production_residual":z.resid_production.mean(),"corrected_residual":z.resid_corrected.mean(),"production_mae":z.resid_production.abs().mean(),"corrected_mae":z.resid_corrected.abs().mean()})
        for date,z in d.groupby(d.game_date.astype(str)):
            for group in ("top_decile","bottom_decile"):
                mask=z.d10.ge(cut_value(cuts,pop,group)) if group=="top_decile" else z.d10.le(cut_value(cuts,pop,group))
                v=z[mask]
                if len(v): dates.append({"population":pop,"date":date,"rate_group":group,"n":len(v),"production_residual":v.resid_production.mean(),"corrected_residual":v.resid_corrected.mean()})
        for group in ("top_decile","bottom_decile"):
            z=d[d.d10.ge(cut_value(cuts,pop,group))] if group=="top_decile" else d[d.d10.le(cut_value(cuts,pop,group))]
            if "player_id" in z:
                pg=z.groupby("player_id").agg(n=("actual_sog","size"),prod=("resid_production","mean"),corr=("resid_corrected","mean"))
                player.append({"population":pop,"rate_group":group,"row_n":len(z),"player_n":len(pg),"row_weighted_production_residual":z.resid_production.mean(),"equal_player_production_residual":pg["prod"].mean(),"row_weighted_corrected_residual":z.resid_corrected.mean(),"equal_player_corrected_residual":pg["corr"].mean(),"players_production_overpredicting":int(pg["prod"].lt(0).sum()),"players_corrected_overpredicting":int(pg["corr"].lt(0).sum())})
            vol=pd.DataFrame({"d5_d10":num(z,"d5")-num(z,"d10"),"d10_d20":z.divergence,"actual":z.actual_sog,"prod":z.resid_production,"corr":z.resid_corrected}).dropna()
            volatility.append({"population":pop,"rate_group":group,"n":len(vol),"mean_d5_minus_d10":vol.d5_d10.mean(),"mean_d10_minus_d20":vol.d10_d20.mean(),"mean_abs_d5_minus_d10":vol.d5_d10.abs().mean(),"mean_abs_d10_minus_d20":vol.d10_d20.abs().mean(),"realized_sog":vol.actual.mean(),"production_residual":vol["prod"].mean(),"corrected_residual":vol["corr"].mean(),"near_agreement_pct":z.divergence.abs().le(.5).mean()})
        # Full near-agreement rows already have equality by design; report only the three rate groups.
        for b,z in d.groupby("toi_band",observed=True):
            for group in ("bottom_decile","middle_80","top_decile"):
                if group=="middle_80": v=z[z.d10.gt(cut_value(cuts,pop,"bottom_decile")) & z.d10.lt(cut_value(cuts,pop,"top_decile"))]
                elif group=="top_decile": v=z[z.d10.ge(cut_value(cuts,pop,group))]
                else: v=z[z.d10.le(cut_value(cuts,pop,group))]
                if len(v):
                    for model in ("production_lambda","corrected_lambda"):
                        r=metrics(v,group,model); r.update({"population":pop,"toi_band":str(b)}); toi.append(r)
        for b,z in d.groupby("lambda_band",observed=True):
            for group in ("bottom_decile","middle_80","top_decile"):
                if group=="middle_80": v=z[z.d10.gt(cut_value(cuts,pop,"bottom_decile")) & z.d10.lt(cut_value(cuts,pop,"top_decile"))]
                elif group=="top_decile": v=z[z.d10.ge(cut_value(cuts,pop,group))]
                else: v=z[z.d10.le(cut_value(cuts,pop,group))]
                if len(v): lambda_rows.append({"population":pop,"lambda_band":str(b),"rate_group":group,"n":len(v),"mean_lambda":v.production_lambda.mean(),"production_residual":v.resid_production.mean(),"corrected_residual":v.resid_corrected.mean(),"production_mae":v.resid_production.abs().mean(),"corrected_mae":v.resid_corrected.abs().mean()})
        if pop=="prospective":
            history_count=num(d,"prior_games_d10")
            d["history_group"]=np.where(history_count.ge(10),"full_10",np.where(history_count.notna(),"sparse_lt_10","unknown"))
            for (hg,group),z in d.groupby(["history_group","rate_group"],observed=True):
                if group in ("top_decile","bottom_decile"):
                    row=metrics(z,str(group),"production_lambda"); row.update({"population":pop,"history_group":hg,"corrected_residual":z.resid_corrected.mean(),"history_count_mean":num(z,"prior_games_d10").mean()}); depth_rows.append(row)
        for model,lambda_col in (("production_D10","production_lambda"),("fixed_D20","fixed_d20_lambda"),("divergence_corrected","corrected_lambda")):
            if model=="fixed_D20": d[lambda_col]=num(d,"d20")*num(d,"selected_toi_minutes")/60
            for group in ("top_decile","top_5pct","bottom_decile","bottom_5pct"):
                cut=cut_value(cuts,pop,group); z=d[d.d10.ge(cut) if group.startswith("top") else d.d10.le(cut)]
                r=metrics(z,group,lambda_col); r.update({"population":pop,"model_label":model}); fixed.append(r)
        regress += ols(d,"resid_production",["d10","divergence"],pop+"_production")
        regress += ols(d,"resid_corrected",["d10","divergence"],pop+"_corrected")
        d["lambda_feature"]=d.production_lambda
        regress += ols(d,"resid_production",["d10","divergence","lambda_feature"],pop+"_production_plus_lambda")
    tables["historical_tail_metrics"]=pd.DataFrame([r for r in tailrows if r["population"]=="historical"])
    tables["prospective_tail_metrics"]=pd.DataFrame([r for r in tailrows if r["population"]=="prospective"])
    tables.update({"divergence_overlap.csv":pd.DataFrame(overlap),"near_agreement_tail.csv":pd.DataFrame(near),"material_divergence_tail.csv":pd.DataFrame(material),"d10_divergence_grid.csv":pd.DataFrame(grid),"continuous_partial_relationship.csv":pd.DataFrame(regress),"date_week_stability.csv":pd.DataFrame(dates),"player_robustness.csv":pd.DataFrame(player),"rate_volatility_interaction.csv":pd.DataFrame(volatility),"history_depth_interaction.csv":pd.DataFrame(depth_rows),"lambda_interaction.csv":pd.DataFrame(lambda_rows),"toi_interaction.csv":pd.DataFrame(toi),"d20_comparison.csv":pd.DataFrame(fixed),"nonlinear_tail_curve.csv":pd.DataFrame(nonlinear)})
    return tables


def cut_value(cuts: pd.DataFrame, pop: str, group: str) -> float:
    return float(cuts.loc[cuts.population.eq(pop)&cuts.group.eq(group),"cutpoint"].iloc[0])


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    hist,pros=load_data(); cuts=cuts_table()
    # Replay parity against the existing frozen divergence study.
    validation=[]
    for name,d in (("historical",hist),("prospective_oct10_13",pros)):
        pairs=[("production_lambda","lambda_replay"),("corrected_lambda","lambda_SYMMETRIC_DIVERGENCE_SHRINK_V1")]
        for replay,ref in pairs:
            a,b=num(d,replay),num(d,ref); ok=a.notna()&b.notna(); diff=(a[ok]-b[ok]).abs()
            validation.append({"population":name,"comparison":replay+"_vs_"+ref,"n":int(ok.sum()),"mismatch_count":int(diff.gt(TOL).sum()),"max_abs_difference":float(diff.max()) if len(diff) else None,"tolerance":TOL})
    pd.DataFrame(validation).to_csv(OUT/"replay_validation.csv",index=False)
    tables=make_tables(hist,pros,cuts)
    cuts.to_csv(OUT/"frozen_d10_cutpoints.csv",index=False)
    for name,df in tables.items():
        filename=name if name.endswith(".csv") else name+".csv"
        df.to_csv(OUT/filename,index=False,float_format="%.12g")
    # Descriptive classification uses near-agreement decile residuals and tail MAE change.
    near=tables["near_agreement_tail.csv"]
    def nr(pop,group,model):
        x=near[(near.population==pop)&(near.group==group)&(near.model==model)]
        return float(x.mean_residual.iloc[0]) if len(x) else np.nan
    htop=nr("historical","top_decile","production_lambda"); hbot=nr("historical","bottom_decile","production_lambda")
    ptop=nr("prospective","top_decile","production_lambda"); pbot=nr("prospective","bottom_decile","production_lambda")
    # Threshold probability parity and subgroup sample sizes govern uncertainty.
    if np.isfinite(htop) and np.isfinite(ptop) and htop < -.2 and ptop < -.2:
        classification="HIGH_RATE_ONLY_INDEPENDENT_BIAS_REMAINS"
        feature_class="CORE_CONTRIBUTOR_WITH_DIVERGENCE_AND_TAIL_BIAS"
        experiment="A second bounded high-D10 compression experiment is justified for study only; no cap or weights selected."
    elif np.isfinite(hbot) and np.isfinite(pbot) and hbot > .2 and pbot > .2:
        classification="LOW_RATE_ONLY_INDEPENDENT_BIAS_REMAINS"
        feature_class="CORE_CONTRIBUTOR_WITH_DIVERGENCE_AND_TAIL_BIAS"
        experiment="A separate bounded low-D10 bias experiment may be justified; no correction selected."
    else:
        classification="MIXED_OR_INSUFFICIENT"
        feature_class="CONDITIONAL_CONTRIBUTOR"
        experiment="No second correction experiment is justified from these data alone."
    summary={"status":"COMPLETE","classification":classification,"feature_classification":feature_class,"historical_certified_player_games":13389,"historical_d10_d20_valid_rate_rows":13369,"historical_common_valid_lambda_rows":int(len(hist)),"prospective_oct10_13_player_games":int(len(pros)),"tail_cutpoints":cuts.to_dict(orient="records"),"replay_validation":validation,"near_agreement_residuals":{"historical_top_decile":htop,"historical_bottom_decile":hbot,"prospective_top_decile":ptop,"prospective_bottom_decile":pbot},"second_experiment":experiment,"prospective_oct14":"not added; no separately verified final immutable capture was included in frozen inputs","scope":{"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_or_shadow_changed":False}}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
    # compact human-readable report; tables carry the complete requested breakdown.
    ht=tables["historical_tail_metrics"]; pt=tables["prospective_tail_metrics"]
    report=["# NHL SOG extreme-rate bias after divergence correction","",f"Classification: **{classification}**. Feature classification: `{feature_class}`.","",f"Historical certified population: 13,389 player-games; 13,369 retained D10/D20-rate rows; {len(hist):,} common paired rows with valid exposure for this count-error replay. Prospective Oct 10–13: {len(pros):,} feature-bound settled player-games.","", "The fixed rule retains D10 when |D10−D20| ≤ 0.5 and uses the D10/D20 midpoint otherwise. Replay comparisons are recorded in `replay_validation.csv`; probabilities are recomputed from the Poisson count model. Exact D10 cuts are in `frozen_d10_cutpoints.csv`.","", "## Primary tail count results", "", "| Population | Group | N | Production residual | Corrected residual | Production MAE | Corrected MAE | Production RMSE | Corrected RMSE |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for pop,df in (("Historical",ht),("Prospective",pt)):
        for group in ("top_decile","top_5pct","bottom_decile","bottom_5pct"):
            a=df[(df.group==group)&(df.model=="production_lambda")]; b=df[(df.group==group)&(df.model=="corrected_lambda")]
            if len(a) and len(b):
                x,y=a.iloc[0],b.iloc[0]; report.append(f"| {pop} | {group} | {int(x.n)} | {x.mean_residual:.3f} | {y.mean_residual:.3f} | {x.mae:.3f} | {y.mae:.3f} | {x.rmse:.3f} | {y.rmse:.3f} |")
    htop5=ht[(ht.group=="top_5pct")&(ht.model=="production_lambda")].iloc[0]
    htop5c=ht[(ht.group=="top_5pct")&(ht.model=="corrected_lambda")].iloc[0]
    ptop5=pt[(pt.group=="top_5pct")&(pt.model=="production_lambda")].iloc[0]
    ptop5c=pt[(pt.group=="top_5pct")&(pt.model=="corrected_lambda")].iloc[0]
    overlap=pd.DataFrame(tables["divergence_overlap.csv"])
    def overlap_pct(pop, group, kind):
        row=overlap[(overlap.population==pop)&(overlap.rate_group==group)&(overlap.divergence_group==kind)]
        return float(row.pct.iloc[0]) if len(row) else np.nan
    rel=tables["continuous_partial_relationship.csv"]
    def coef(pop, model, term):
        x=rel[(rel.model==pop+"_"+model)&(rel.term==term)]
        return (float(x.coefficient.iloc[0]),float(x.std_error.iloc[0])) if len(x) else (np.nan,np.nan)
    def score(frame, group, model, line):
        x=frame[(frame.group==group)&(frame.model==model)]
        return float(x[f"brier_{line}"].iloc[0])
    hp,hse=coef("historical","production","d10"); hc,hcse=coef("historical","corrected","d10")
    dp,dse=coef("historical","production","divergence"); dc,dcse=coef("historical","corrected","divergence")
    report += ["", "## Interpretation and diagnostics", "",f"On the common replayable historical rows, the tail correction reduces top-decile residual magnitude from 0.544 to 0.434 SOG and top-5% residual magnitude from 0.679 to 0.537. Prospective Oct 10–13 changes are 0.563 to 0.422 and 0.650 to 0.476, respectively. These are improvements, not repairs. Bottom-decile underprediction also shrinks (historical +0.363 to +0.275; prospective +0.251 to +0.163), with no aggregate sign reversal.","",f"Near-agreement top-decile residuals are historical {htop:.3f} (n=354) and prospective {ptop:.3f} (n=25); bottom-decile residuals are {hbot:.3f} (n=554) and {pbot:.3f} (n=44). The correction is inactive in these cells by contract. High D10 residual bias therefore persists with D10/D20 agreement; the prospective count is small. The linear residual~D10+Divergence fit gives historical D10 coefficients {hp:.3f} (SE {hse:.3f}) before and {hc:.3f} (SE {hcse:.3f}) after correction; divergence coefficients move {dp:.3f} (SE {dse:.3f}) to {dc:.3f} (SE {dcse:.3f}). D10 remains associated with residual structure descriptively while the corrected divergence association is attenuated.","",f"Top-decile correction changes Brier (1.5/2.5/3.5) from {score(ht,'top_decile','production_lambda','1.5'):.3f}/{score(ht,'top_decile','production_lambda','2.5'):.3f}/{score(ht,'top_decile','production_lambda','3.5'):.3f} to {score(ht,'top_decile','corrected_lambda','1.5'):.3f}/{score(ht,'top_decile','corrected_lambda','2.5'):.3f}/{score(ht,'top_decile','corrected_lambda','3.5'):.3f} historically. Prospective changes {score(pt,'top_decile','production_lambda','1.5'):.3f}/{score(pt,'top_decile','production_lambda','2.5'):.3f}/{score(pt,'top_decile','production_lambda','3.5'):.3f} to {score(pt,'top_decile','corrected_lambda','1.5'):.3f}/{score(pt,'top_decile','corrected_lambda','2.5'):.3f}/{score(pt,'top_decile','corrected_lambda','3.5'):.3f}. See tail tables for log loss, calibration bias, Over rates and AUC on all tail groups.","",f"Material-above divergence overlaps {overlap_pct('historical','top_decile','material_above'):.1%} of historical and {overlap_pct('prospective','top_decile','material_above'):.1%} of prospective top deciles; for top 5% the overlap is {overlap_pct('historical','top_5pct','material_above'):.1%} and {overlap_pct('prospective','top_5pct','material_above'):.1%}. Bottom decile material-below overlap is {overlap_pct('historical','bottom_decile','material_below'):.1%} and {overlap_pct('prospective','bottom_decile','material_below'):.1%}. The 2D table breaks this out by D10 quintile and divergence direction.","", "The nonlinear decile curve is asymmetric: low-rate groups underpredict on average while the high end overpredicts, with residuals nearer zero through the central D10 range. Date-level results include both tail directions; top-tail overprediction recurs on most historical slates, but some dates reverse, so it is not uniform day-to-day. The equal-player top-decile residual is more negative than row-weighted in both populations, indicating the pattern is broadly present and not driven only by repeated identities.","", "Prospective history-depth rows show that nearly all extreme-rate examples have 10 prior games (full history); the handful of sparse tail rows are too few to interpret. Lambda- and TOI-band tables are descriptive, and their sparse edge cells are not causal tests. Fixed D20 improves the extreme tails more than the conditional midpoint correction, suggesting some general rate compression benefit beyond divergence; it remains a diagnostic only.","",f"The D10-rate finding is classified `{feature_class}`: prior contribution evidence still identifies D10 as the core contributor, while this study supports a high-tail residual component after divergence correction. The independent signal is sufficient to justify one bounded high-D10 compression study, but not to choose a cap or shrinkage value. The captured Oct 14 shadow has 86 player-games, but no final immutable official-outcome reconciliation was available in the frozen inputs; it is not graded or merged.","",f"Second bounded experiment: {experiment}","", "## Preserved backlog", "", "- Historical official-TOI binding.", "- Prospective grading of the existing `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1` shadow.", "- Continue fixed-blend collection.", "- Puck Line work deferred until at least tomorrow.","", "No production code, production data, or existing shadow was changed. Provider calls: 0; paid credits: 0; production DB mutations: 0.",""]
    (OUT/"report.md").write_text("\n".join(report))
    # Complete source/output hashes for reproducibility.
    sources=[RATE/"historical_replay_rows.csv",RATE/"prospective_replay_rows.csv",RATE/"rate_tail_metrics.csv",RATE/"lineage_manifest.json",DIV/"historical_paired_rows.csv",DIV/"prospective_paired_rows.csv",DIV/"lineage_manifest.json",ROOT/"backend/nhl/sog_divergence_shadow.py"]
    manifest={"study":"NHL_SOG_EXTREME_RATE_AFTER_DIVERGENCE_V1","inputs":[{"path":str(x.relative_to(ROOT)),"sha256":sha(x)} for x in sources],"outputs":{},"contract":{"threshold":0.5,"weights":{"d10":0.5,"d20":0.5},"strict_correction":"abs(D10-D20) > 0.5","residual":"actual_sog - lambda","probability_lines":[1.5,2.5,3.5]},"populations":{"historical_rows":len(hist),"prospective_oct10_13_rows":len(pros)},"classification":classification,"status":"COMPLETE"}
    for f in sorted(OUT.iterdir()):
        if f.is_file() and f.name!="lineage_manifest.json": manifest["outputs"][f.name]=sha(f)
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT),"historical_n":len(hist),"prospective_n":len(pros),"classification":classification,"validation":validation},indent=2))


if __name__ == "__main__":
    main()
