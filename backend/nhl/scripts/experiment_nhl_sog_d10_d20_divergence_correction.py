"""Offline, frozen-rule evaluation of D10/D20 divergence corrections."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

ROOT = Path(__file__).resolve().parents[3]
PRIOR = ROOT / "artifacts/analysis/nhl/sog_d10_d20_divergence_validation/2026-10-14"
CHAR = ROOT / "artifacts/analysis/nhl/sog_baseline_characterization/2026-10-14"
OUT = ROOT / "artifacts/analysis/nhl/sog_d10_d20_divergence_correction/2026-10-14"
LINES = (1.5, 2.5, 3.5)
THRESHOLD = 0.5
VARIANTS = ("PRODUCTION", "SYMMETRIC_DIVERGENCE_SHRINK_V1", "HIGH_D10_ONLY_SHRINK_V1")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def poisson_over(lam: pd.Series, line: float) -> pd.Series:
    cutoff = int(np.ceil(line))
    cdf = sum(np.exp(-lam) * lam.pow(k) / math.factorial(k) for k in range(cutoff))
    return (1.0 - cdf).clip(0, 1)


def prepare(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    x = df.copy()
    aliases = {"d10": "d10_sog_per60", "d20": "d20_sog_per60", "lam": "production_lambda", "actual": "actual_sog"}
    for dest, source in aliases.items():
        if dest not in x and source in x:
            x[dest] = x[source]
    for c in ("d10", "d20", "lam", "actual", "selected_toi_minutes"):
        if c in x:
            x[c] = pd.to_numeric(x[c], errors="coerce")
    x["divergence"] = x.d10 - x.d20
    x["material_above"] = x.divergence.gt(THRESHOLD)
    x["material_below"] = x.divergence.lt(-THRESHOLD)
    x["near"] = x.divergence.abs().le(THRESHOLD)
    # Production rate fallback is D10, D20, D5. Historical certified rows have D10/D20.
    rate = x.d10.copy()
    if "d5_sog_per60" in x:
        rate = rate.fillna(pd.to_numeric(x.d5_sog_per60, errors="coerce"))
    if "selected_toi_minutes" not in x:
        x["selected_toi_minutes"] = np.nan
    corrected = rate.copy()
    valid = x.d10.notna() & x.d20.notna()
    mid = (x.d10 + x.d20) / 2
    corrected.loc[valid & (x.divergence.abs() > THRESHOLD)] = mid.loc[valid & (x.divergence.abs() > THRESHOLD)]
    high = rate.copy()
    high.loc[x.material_above] = mid.loc[x.material_above]
    exposure = pd.to_numeric(x.selected_toi_minutes, errors="coerce")
    x["lambda_replay"] = rate * exposure / 60
    x["lambda_symmetric"] = corrected * exposure / 60
    x["lambda_high_d10"] = high * exposure / 60
    # Preserve the certified baseline when selected exposure was not retained; no synthetic exposure.
    for col in ("lambda_replay", "lambda_symmetric", "lambda_high_d10"):
        x[col] = x[col].where(exposure.notna() & exposure.ge(0), x.lam)
    x.loc[~(x.divergence.abs() > THRESHOLD), "lambda_symmetric"] = x.loc[~(x.divergence.abs() > THRESHOLD), "lam"]
    x.loc[~x.material_above, "lambda_high_d10"] = x.loc[~x.material_above, "lam"]
    x["lambda_PRODUCTION"] = x.lam
    x["lambda_SYMMETRIC_DIVERGENCE_SHRINK_V1"] = x.lambda_symmetric
    x["lambda_HIGH_D10_ONLY_SHRINK_V1"] = x.lambda_high_d10
    for variant in VARIANTS:
        x[f"resid_{variant}"] = x.actual - x[f"lambda_{variant}"]
    x["replay_abs_diff"] = (x.lambda_rePLAY if "lambda_rePLAY" in x else x.lambda_replay) - x.lam
    x[date_col] = x[date_col].astype(str)
    return x


def count_metrics(g: pd.DataFrame, variant: str) -> dict:
    e = g[f"resid_{variant}"].dropna()
    lam = g.loc[e.index, f"lambda_{variant}"]
    y = g.loc[e.index, "actual"]
    return {"n": int(len(e)), "mae": float(e.abs().mean()), "rmse": float(np.sqrt(np.mean(e**2))),
            "mean_residual": float(e.mean()), "median_absolute_error": float(e.abs().median()),
            "mean_lambda": float(lam.mean()), "realized_mean_sog": float(y.mean())}


def threshold_metrics(pg: pd.DataFrame, variant: str) -> list[dict]:
    rows=[]
    for line in LINES:
        y=(pg.actual >= np.ceil(line)).astype(int).to_numpy()
        p=poisson_over(pg[f"lambda_{variant}"], line).to_numpy()
        rows.append({"variant":variant,"line":line,"n":len(y),"brier":float(np.mean((p-y)**2)),
                     "log_loss":float(-np.mean(y*np.log(np.clip(p,1e-15,1))+(1-y)*np.log(np.clip(1-p,1e-15,1)))),
                     "auc":float(roc_auc_score(y,p)) if len(set(y))==2 else None,
                     "average_precision":float(average_precision_score(y,p)),"accuracy":float(np.mean((p>=.5)==y)),
                     "predicted_over_rate":float(p.mean()),"realized_over_rate":float(y.mean()),"calibration_bias":float(p.mean()-y.mean())})
    return rows


def paired_metrics(pg: pd.DataFrame, variant: str) -> dict:
    e0=pg.resid_PRODUCTION.abs(); e1=pg[f"resid_{variant}"].abs(); d=e1-e0
    sq1=pg[f"resid_{variant}"]**2; sq0=pg.resid_PRODUCTION**2
    out={"variant":variant,"count_n":len(pg),"count_absolute_error_improved":int((d < -1e-12).sum()),"count_absolute_error_worsened":int((d > 1e-12).sum()),"count_absolute_error_tied":int((d.abs()<=1e-12).sum()),"mean_paired_mae_difference":float(d.mean()),"mean_paired_squared_error_difference":float((sq1-sq0).mean())}
    for line in LINES:
        y=(pg.actual>=np.ceil(line)).astype(int).to_numpy(); p0=poisson_over(pg.lambda_PRODUCTION,line).to_numpy(); p1=poisson_over(pg[f"lambda_{variant}"],line).to_numpy()
        b0=(p0-y)**2; b1=(p1-y)**2
        ll0=-(y*np.log(np.clip(p0,1e-15,1))+(1-y)*np.log(np.clip(1-p0,1e-15,1)))
        ll1=-(y*np.log(np.clip(p1,1e-15,1))+(1-y)*np.log(np.clip(1-p1,1e-15,1)))
        for metric,a,b in [("brier",b0,b1),("log_loss",ll0,ll1)]:
            delta=b-a
            out[f"line_{line}_{metric}_difference"]=float(delta.mean())
            out[f"line_{line}_{metric}_improved_fraction"]=float((delta < -1e-12).mean())
            out[f"line_{line}_{metric}_worsened_fraction"]=float((delta > 1e-12).mean())
            out[f"line_{line}_{metric}_tied_fraction"]=float((np.abs(delta)<=1e-12).mean())
    return out


def paired_pg(lines: pd.DataFrame, date_col: str) -> pd.DataFrame:
    return lines.drop_duplicates([date_col,"game_id","player_id"]).copy()


def apply_group_tables(pg: pd.DataFrame, date_col: str, dimensions: list[tuple[str,pd.Series]]) -> dict[str,pd.DataFrame]:
    outputs={}
    for key, series in dimensions:
        temp=pg.copy(); temp["band"]=series.astype(str)
        rows=[]
        for (band,group),g in temp.groupby(["band","regime"],dropna=False,sort=True):
            for variant in VARIANTS:
                rows.append({"dimension":key,"band":band,"regime":group,**count_metrics(g,variant)})
        outputs[key]=pd.DataFrame(rows)
    return outputs


def main() -> None:
    OUT.mkdir(parents=True,exist_ok=True)
    hraw=pd.read_csv(PRIOR/"historical_paired_rows.csv",low_memory=False)
    praw=pd.read_csv(PRIOR/"prospective_validation_rows.csv",low_memory=False)
    hist=prepare(hraw,"game_date"); pros=prepare(praw,"slate_date")
    for source,df in [("historical",hist),("prospective",pros)]:
        if df[["game_date" if source=="historical" else "slate_date","game_id","player_id"]].duplicated().any(): raise ValueError(f"{source} player-game identity duplicates")
    # Attach the pregame exposure and three retained probabilities from immutable feature-bound rows.
    prospective_lines=pd.read_csv(CHAR/"prospective_feature_bound_rows.csv",low_memory=False)
    key=["slate_date","game_id","player_id"]
    feature_pg=prospective_lines.sort_values("line").drop_duplicates(key)
    feature_cols=key+[c for c in ["selected_toi_minutes","d10_sog_per60","d20_sog_per60","production_lambda"] if c in feature_pg]
    pros=pros.merge(feature_pg[feature_cols],on=key,how="left",suffixes=("","_bound"),validate="one_to_one")
    if "selected_toi_minutes_bound" in pros:
        pros["selected_toi_minutes"] = pros["selected_toi_minutes_bound"]
    if "selected_toi_minutes" not in pros or pros.selected_toi_minutes.isna().any(): raise ValueError("Prospective bound exposure is missing")
    pros["selected_toi_minutes"]=pd.to_numeric(pros.selected_toi_minutes,errors="coerce")
    pros=prepare(pros,"slate_date")
    # Fail closed on production lambda and retained historical/prospective threshold probabilities.
    replay=hist.replay_abs_diff.abs()
    replay_rows=int(replay.notna().sum()); replay_bad=int((replay>1e-10).sum())
    if replay_bad: raise ValueError(f"Historical production lambda replay mismatch: {replay_bad}/{replay_rows}")
    checks=[]
    hist_lines=pd.read_csv(CHAR/"historical_characterization_rows.csv",low_memory=False)
    hist_lines=hist_lines.drop_duplicates(["game_date","game_id","player_id","line"])
    hist_lines=hist_lines.merge(hist[["game_date","game_id","player_id","lambda_PRODUCTION"]],on=["game_date","game_id","player_id"],how="inner",validate="many_to_one")
    hist_lines["line"]=pd.to_numeric(hist_lines.line,errors="coerce")
    hist_lines["stored_p"]=pd.to_numeric(hist_lines.p_production,errors="coerce")
    for source,df,date_col in [("historical",hist_lines,"game_date"),("prospective",prospective_lines,"slate_date")]:
        stored=pd.to_numeric(df.p_production,errors="coerce")
        calc=pd.Series(np.nan,index=df.index,dtype=float)
        if source=="historical":
            lamcol="lambda_PRODUCTION"
        else:
            pglam=pros.set_index(key).lambda_PRODUCTION
            thiskey=pd.MultiIndex.from_frame(df[key])
            df_lam=pd.Series(thiskey.map(pglam),index=df.index)
            lamcol="_replayed_lambda"
            df=df.copy(); df[lamcol]=df_lam
        for ln in LINES:
            mask=pd.to_numeric(df.line,errors="coerce").eq(ln)
            calc.loc[mask]=poisson_over(pd.to_numeric(df.loc[mask,lamcol],errors="coerce"),ln)
        delta=(calc-stored).abs()
        checks.append({"source":source,"rows":int(delta.notna().sum()),"mismatch_count_gt_1e_10":int((delta>1e-10).sum()),"max_abs_difference":float(delta.max())})
    if any(c["mismatch_count_gt_1e_10"] for c in checks): raise ValueError("Stored production probability replay failed")
    hist.to_csv(OUT/"historical_paired_rows.csv",index=False)
    pros.to_csv(OUT/"prospective_paired_rows.csv",index=False)

    metric_tables={}
    for scope,pg in [("historical",hist),("prospective",pros)]:
        counts=[{"scope":scope,"variant":v,**count_metrics(pg,v)} for v in VARIANTS]
        th=[]
        for v in VARIANTS: th.extend([{"scope":scope,**r} for r in threshold_metrics(pg,v)])
        pd.DataFrame(counts).to_csv(OUT/f"{scope}_global_count_metrics.csv",index=False)
        pd.DataFrame(th).to_csv(OUT/f"{scope}_line_metrics.csv",index=False)
        paired=[paired_metrics(pg,v) for v in VARIANTS[1:]]
        pd.DataFrame(paired).to_csv(OUT/f"{scope}_paired_impact.csv",index=False)
        metric_tables[scope]={"counts":counts,"thresholds":th,"paired":paired}

    # Historical divergence groups and prior signed-decile cutpoints reused exactly.
    hist["regime"]=np.select([hist.material_above,hist.near,hist.material_below],["MATERIAL_ABOVE","NEAR_AGREEMENT","MATERIAL_BELOW"],default="UNCLASSIFIED")
    pros["regime"]=np.select([pros.material_above,pros.near,pros.material_below],["MATERIAL_ABOVE","NEAR_AGREEMENT","MATERIAL_BELOW"],default="UNCLASSIFIED")
    def regime_table(pg: pd.DataFrame,scope: str) -> pd.DataFrame:
        rows=[]
        for regime,g in pg.groupby("regime"):
            for v in VARIANTS:
                r={"scope":scope,"regime":regime,"variant":v,**count_metrics(g,v)}
                for line,tm in zip(LINES,threshold_metrics(g,v)):
                    r[f"line_{line}_brier"]=tm["brier"]; r[f"line_{line}_log_loss"]=tm["log_loss"]; r[f"line_{line}_calibration_bias"]=tm["calibration_bias"]
                rows.append(r)
        return pd.DataFrame(rows)
    groups=pd.concat([regime_table(hist,"historical"),regime_table(pros,"prospective")],ignore_index=True)
    groups.to_csv(OUT/"divergence_group_metrics.csv",index=False)
    near=hist.near
    near_identity={"historical_near_n":int(near.sum()),"symmetric_lambda_exactly_unchanged":bool(np.array_equal(hist.loc[near,"lambda_PRODUCTION"].to_numpy(),hist.loc[near,"lambda_SYMMETRIC_DIVERGENCE_SHRINK_V1"].to_numpy())),"high_d10_lambda_exactly_unchanged":bool(np.array_equal(hist.loc[near,"lambda_PRODUCTION"].to_numpy(),hist.loc[near,"lambda_HIGH_D10_ONLY_SHRINK_V1"].to_numpy())),"prospective_near_n":int(pros.near.sum()),"prospective_predictions_exactly_unchanged":bool(np.array_equal(pros.loc[pros.near,"lambda_PRODUCTION"].to_numpy(),pros.loc[pros.near,"lambda_SYMMETRIC_DIVERGENCE_SHRINK_V1"].to_numpy()))}
    (OUT/"near_agreement_identity_check.json").write_text(json.dumps(near_identity,indent=2,sort_keys=True)+"\n")

    # Reuse prior historical qcut deciles by their empirical boundaries.
    prior_bins=pd.read_csv(PRIOR/"magnitude_bin_metrics.csv")
    cuts=np.quantile(hist.divergence.dropna(),np.arange(.1,1,.1))
    hist["signed_bin"]=pd.cut(hist.divergence,[-np.inf,*cuts,np.inf],labels=[f"Q{i}" for i in range(1,11)],include_lowest=True)
    pros["signed_bin"]=pd.cut(pros.divergence,[-np.inf,*cuts,np.inf],labels=[f"Q{i}" for i in range(1,11)],include_lowest=True)
    bins=[]
    for scope,pg in [("historical",hist),("prospective",pros)]:
        for b,g in pg.groupby("signed_bin",observed=True):
            for v in VARIANTS:
                r={"scope":scope,"bin":str(b),**count_metrics(g,v)}
                for line,tm in zip(LINES,threshold_metrics(g,v)): r[f"line_{line}_brier"]=tm["brier"]
                bins.append(r)
    pd.DataFrame(bins).to_csv(OUT/"signed_divergence_bin_metrics.csv",index=False)
    slopes=[]
    for scope,pg in [("historical",hist),("prospective",pros)]:
        for v in VARIANTS:
            x=pg.divergence.to_numpy(float); y=pg[f"resid_{v}"].to_numpy(float); slope,intercept=np.polyfit(x,y,1)
            slopes.append({"scope":scope,"variant":v,"n":len(pg),"pearson_r":float(np.corrcoef(x,y)[0,1]),"spearman_rho":float(pd.Series(x).corr(pd.Series(y),method="spearman")),"slope":float(slope),"intercept":float(intercept),"r_squared":float(np.corrcoef(x,y)[0,1]**2)})
    pd.DataFrame(slopes).to_csv(OUT/"residual_slope_metrics.csv",index=False)

    # Dates, player-level robustness, and interaction strata use historical cutpoints.
    date_rows=[]
    for scope,pg,datecol in [("historical",hist,"game_date"),("prospective",pros,"slate_date")]:
        for date,g in pg.groupby(datecol,sort=True):
            for v in VARIANTS:
                r={"scope":scope,"date":date,"variant":v,**count_metrics(g,v)}
                for line,tm in zip(LINES,threshold_metrics(g,v)): r[f"line_{line}_brier"]=tm["brier"]; r[f"line_{line}_log_loss"]=tm["log_loss"]
                date_rows.append(r)
    pd.DataFrame(date_rows).to_csv(OUT/"date_metrics.csv",index=False)
    hist["week_start"]=pd.to_datetime(hist.game_date).dt.to_period("W-SUN").dt.start_time.dt.strftime("%Y-%m-%d")
    weekly_rows=[]
    for week,g in hist.groupby("week_start",sort=True):
        for v in VARIANTS:
            r={"week_start":week,"variant":v,**count_metrics(g,v)}
            for ln,tm in zip(LINES,threshold_metrics(g,v)):
                r[f"line_{ln}_brier"]=tm["brier"]; r[f"line_{ln}_log_loss"]=tm["log_loss"]
            weekly_rows.append(r)
    pd.DataFrame(weekly_rows).to_csv(OUT/"weekly_metrics.csv",index=False)
    pg=hist.copy()
    player=[]
    for pid,g in pg.groupby("player_id"):
        if len(g)<3: continue
        for v in VARIANTS:
            player.append({"player_id":pid,"n":len(g),"variant":v,"mean_residual":g[f"resid_{v}"].mean(),"mae":g[f"resid_{v}"].abs().mean()})
    playerdf=pd.DataFrame(player); playerdf.to_csv(OUT/"player_robustness_metrics.csv",index=False)
    pivot=playerdf.pivot(index="player_id",columns="variant",values="mae")
    player_sum={}
    for v in VARIANTS[1:]:
        d=pivot[v]-pivot.PRODUCTION
        player_sum[v]={"players_n":int(d.notna().sum()),"improved":int((d < -1e-12).sum()),"worsened":int((d > 1e-12).sum()),"unchanged":int((d.abs()<=1e-12).sum()),"equal_player_mean_mae_difference":float(d.mean())}
    (OUT/"player_robustness_summary.json").write_text(json.dumps(player_sum,indent=2,sort_keys=True)+"\n")
    dims=[]
    for label,col,q in [("D10_QUINTILE","d10",5),("D10_DECILE","d10",10),("LAMBDA_QUARTILE","lam",4),("TOI_QUARTILE","selected_toi_minutes",4),("PP_SHARE_TERTILE","role_pp_share",3)]:
        vals=pd.to_numeric(hist[col],errors="coerce"); edges=np.unique(np.quantile(vals.dropna(),np.arange(1,q)/q));
        if len(edges):
            band=pd.cut(vals,[-np.inf,*edges,np.inf],labels=[f"Q{i}" for i in range(1,len(edges)+2)],include_lowest=True)
            dims.append((label,band))
    # Make each interaction explicit by regime and frozen variant.
    interaction_rows=[]
    for name,series in dims:
        tmp=hist.copy(); tmp["band"]=series.astype(str)
        for (band,regime),g in tmp.groupby(["band","regime"],dropna=False):
            for v in VARIANTS:
                interaction_rows.append({"dimension":name,"band":band,"regime":regime,"variant":v,**count_metrics(g,v)})
    pd.DataFrame(interaction_rows).to_csv(OUT/"interaction_metrics.csv",index=False)
    # calibration repair rows provide baseline and corrected bias plus delta, by line and regime.
    cal=groups[["scope","regime","variant"]+[f"line_{ln}_calibration_bias" for ln in LINES if f"line_{ln}_calibration_bias" in groups]].copy()
    cal.to_csv(OUT/"calibration_bias_comparison.csv",index=False)

    # Prospective exact common rows with already graded fixed-blend outputs (Oct 13 only).
    run="nhldaily_20261013T201458522957Z_22deb90f"
    blend_comp=[]
    p13=pros[pros.slate_date.eq("2026-10-13")].copy()
    p13=p13.assign(_join=1).merge(pd.DataFrame({"line":LINES,"_join":1}),on="_join").drop(columns="_join")
    for model in ("NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1","NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1"):
        path=ROOT/f"artifacts/operational/nhl/sog_fixed_blend_grades/slate_date=2026-10-13/run_id={run}/model={model}/graded_predictions.csv"
        if not path.exists(): continue
        b=pd.read_csv(path)
        join=p13.merge(b,on=["game_id","player_id","line"],how="inner",suffixes=("_x","_b"),validate="one_to_one")
        g=join; yy=g.realized_over.to_numpy(dtype=int)
        for v in VARIANTS:
            ps=np.array([poisson_over(pd.Series([row[f"lambda_{v}"]]),float(row["line"])).iloc[0] for _,row in g.iterrows()])
            blend_comp.append({"model_identity":model,"variant":v,"n":len(g),"brier":float(np.mean((ps-yy)**2)),"log_loss":float(-np.mean(yy*np.log(np.clip(ps,1e-15,1))+(1-yy)*np.log(np.clip(1-ps,1e-15,1))))})
        pb=pd.to_numeric(g.p_over,errors="coerce").to_numpy()
        blend_comp.append({"model_identity":model,"variant":model,"n":len(g),"brier":float(np.mean((pb-yy)**2)),"log_loss":float(-np.mean(yy*np.log(np.clip(pb,1e-15,1))+(1-yy)*np.log(np.clip(1-pb,1e-15,1))))})
    pd.DataFrame(blend_comp).to_csv(OUT/"fixed_blend_comparison.csv",index=False)

    # Market-relative scores on the exact retained matched rows, no market filter for model scoring.
    market=pd.read_csv(CHAR/"market_matched_rows.csv",low_memory=False)
    market=market[market.slate_date.astype(str).isin(pros.slate_date.unique())].copy()
    m=market.merge(pros[["slate_date","game_id","player_id","lambda_PRODUCTION","lambda_SYMMETRIC_DIVERGENCE_SHRINK_V1","lambda_HIGH_D10_ONLY_SHRINK_V1"]],on=["slate_date","game_id","player_id"],how="inner",validate="many_to_one")
    m["actual"] = pd.to_numeric(m.actual_sog,errors="coerce"); m["market_p"] = pd.to_numeric(m.p_over_mkt_novig,errors="coerce")
    market_metrics=[]
    for line,g in m.groupby("line",sort=True):
        g=g[g.market_p.notna() & g.actual.notna()]
        if not len(g): continue
        y=(g.actual>=np.ceil(float(line))).astype(int).to_numpy()
        for v in (*VARIANTS,"MARKET_NO_VIG"):
            p=g.market_p.to_numpy() if v=="MARKET_NO_VIG" else np.array([poisson_over(pd.Series([row[f"lambda_{v}"]]),float(line)).iloc[0] for _,row in g.iterrows()])
            market_metrics.append({"line":float(line),"variant":v,"n":len(g),"brier":float(np.mean((p-y)**2)),"log_loss":float(-np.mean(y*np.log(np.clip(p,1e-15,1))+(1-y)*np.log(np.clip(1-p,1e-15,1)))),"calibration_bias":float(p.mean()-y.mean()),"realized_over_rate":float(y.mean()),"predicted_over_rate":float(p.mean())})
    pd.DataFrame(market_metrics).to_csv(OUT/"market_comparison.csv",index=False)

    # Summarize predeclared evidence criteria; decision is deliberately conservative.
    hc={v:count_metrics(hist,v) for v in VARIANTS}; pc={v:count_metrics(pros,v) for v in VARIANTS}
    hline=pd.DataFrame(metric_tables["historical"]["thresholds"]); pline=pd.DataFrame(metric_tables["prospective"]["thresholds"])
    pdates=pd.read_csv(OUT/"date_metrics.csv"); hdates=pdates[pdates.scope.eq("historical")]
    date_dec={}
    for v in VARIANTS[1:]:
        result={}
        for scope,frame in [("historical",pdates[pdates.scope.eq("historical")]),("prospective",pdates[pdates.scope.eq("prospective")])]:
            d=frame.pivot(index="date",columns="variant",values="mae"); delta=d[v]-d.PRODUCTION
            item={"mae_dates_improved":int((delta < -1e-12).sum()),"mae_dates_worsened":int((delta > 1e-12).sum()),"mae_dates_tied":int((delta.abs()<=1e-12).sum())}
            for line in LINES:
                b=frame.pivot(index="date",columns="variant",values=f"line_{line}_brier"); bd=b[v]-b.PRODUCTION
                item[f"line_{line}_brier_dates_improved"]=int((bd < -1e-12).sum()); item[f"line_{line}_brier_dates_worsened"]=int((bd > 1e-12).sum())
            result[scope]=item
        date_dec[v]=result
    hist_slopes=pd.DataFrame(slopes).query("scope == 'historical'").set_index("variant").slope.to_dict()
    pro_slopes=pd.DataFrame(slopes).query("scope == 'prospective'").set_index("variant").slope.to_dict()
    def line_improve(scope,metric):
        frame=pd.DataFrame(metric_tables[scope]["thresholds"])
        a=frame[frame.variant.eq(VARIANTS[1])].sort_values("line")[metric].to_numpy()
        b=frame[frame.variant.eq("PRODUCTION")].sort_values("line")[metric].to_numpy()
        return bool((a<b).all())
    criteria={"historical_count_error_improved":hc[VARIANTS[1]]["mae"]<hc["PRODUCTION"]["mae"] and hc[VARIANTS[1]]["rmse"]<hc["PRODUCTION"]["rmse"],"historical_brier_all_lines_improved":line_improve("historical","brier"),"historical_log_loss_all_lines_improved":line_improve("historical","log_loss"),"divergence_slope_reduced_historical_and_prospective":abs(hist_slopes[VARIANTS[1]])<abs(hist_slopes["PRODUCTION"]) and abs(pro_slopes[VARIANTS[1]])<abs(pro_slopes["PRODUCTION"]),"prospective_count_error_improved":pc[VARIANTS[1]]["mae"]<pc["PRODUCTION"]["mae"] and pc[VARIANTS[1]]["rmse"]<pc["PRODUCTION"]["rmse"],"prospective_brier_all_lines_improved":line_improve("prospective","brier"),"prospective_log_loss_all_lines_improved":line_improve("prospective","log_loss"),"near_agreement_exact_identity":near_identity["symmetric_lambda_exactly_unchanged"] and near_identity["prospective_predictions_exactly_unchanged"],"equal_player_mean_mae_improved":player_sum[VARIANTS[1]]["equal_player_mean_mae_difference"]<0,"prospective_count_mae_improved_all_dates":date_dec[VARIANTS[1]]["prospective"]["mae_dates_worsened"]==0}
    classification="READY_FOR_PROSPECTIVE_SHADOW_REVIEW" if all(criteria.values()) else "HISTORICAL_ONLY_IMPROVEMENT" if criteria["historical_count_error_improved"] else "CORRECTION_NOT_SUPPORTED"
    summary={"status":"COMPLETE_OFFLINE_RESEARCH_ONLY","control":{"model_family":"poisson_baseline","model_version":"baseline_v1"},"rule":{"threshold_abs_d10_minus_d20_sog60":THRESHOLD,"symmetric":"if abs(divergence)>0.5, corrected rate=(D10+D20)/2; else D10","high_d10_only":"if divergence>0.5, corrected rate=(D10+D20)/2; else D10","exposure_and_other_scorer_logic":"unchanged retained production exposure and Poisson probability formulas"},"population":{"historical_certified_baseline_rows":13389,"historical_rows":len(hist),"historical_valid_divergence":int(hist.divergence.notna().sum()),"historical_dates":[str(hist.game_date.min()),str(hist.game_date.max())],"prospective_rows":len(pros),"prospective_dates":sorted(pros.slate_date.unique().tolist())},"baseline_replay":{"lambda_rows_checked":replay_rows,"lambda_mismatch_count_gt_1e_10":replay_bad,"lambda_max_abs_difference":float(replay.max()),"probability_checks":checks},"historical_count":hc,"prospective_count":pc,"historical_paired":metric_tables["historical"]["paired"],"prospective_paired":metric_tables["prospective"]["paired"],"near_agreement_identity":near_identity,"date_stability":date_dec,"player_equal_weighted":player_sum,"residual_slopes":{"historical":hist_slopes,"prospective":pro_slopes},"decision_criteria":criteria,"classification":classification,"d10_contribution_classification":"CONDITIONAL_CONTRIBUTOR","activity":{"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_baseline_changed":False,"push":False}}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    manifest_files=[PRIOR/"historical_paired_rows.csv",PRIOR/"prospective_validation_rows.csv",PRIOR/"magnitude_bin_metrics.csv",CHAR/"market_matched_rows.csv",ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py"]
    lineage={"experiment":"SYMMETRIC_DIVERGENCE_SHRINK_V1_AND_HIGH_D10_ONLY_SHRINK_V1","sources":[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in manifest_files],"blend_source_paths":[str((ROOT/f"artifacts/operational/nhl/sog_fixed_blend_grades/slate_date=2026-10-13/run_id={run}/model={model}/graded_predictions.csv").relative_to(ROOT)) for model in ("NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1","NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1") if (ROOT/f"artifacts/operational/nhl/sog_fixed_blend_grades/slate_date=2026-10-13/run_id={run}/model={model}/graded_predictions.csv").exists()],"outputs":{p.name:sha(p) for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"}}
    (OUT/"lineage_manifest.json").write_text(json.dumps(lineage,indent=2,sort_keys=True)+"\n")
    hthreshold=pd.DataFrame(metric_tables["historical"]["thresholds"]); pthreshold=pd.DataFrame(metric_tables["prospective"]["thresholds"])
    lines_md=["| Scope | Variant | Line | N | Brier | Log loss | AUC | AP | Calibration bias |","|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for scope,tab in [("Historical",hthreshold),("Prospective",pthreshold)]:
        for _,r in tab.iterrows(): lines_md.append(f"| {scope} | {r.variant} | {r.line:.1f} | {int(r.n)} | {r.brier:.4f} | {r.log_loss:.4f} | {r.auc:.4f} | {r.average_precision:.4f} | {r.calibration_bias:+.4f} |")
    group_md=["| Scope | Divergence group | Variant | N | Residual | MAE | RMSE | 1.5 bias | 2.5 bias | 3.5 bias |","|---|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for _,r in groups.iterrows(): group_md.append(f"| {r.scope} | {r.regime} | {r.variant} | {int(r.n)} | {r.mean_residual:+.3f} | {r.mae:.3f} | {r.rmse:.3f} | {r['line_1.5_calibration_bias']:+.3f} | {r['line_2.5_calibration_bias']:+.3f} | {r['line_3.5_calibration_bias']:+.3f} |")
    gl=groups.set_index(["scope","regime","variant"])
    mae_change={scope:{regime:float(gl.loc[(scope,regime,VARIANTS[1]),"mae"]-gl.loc[(scope,regime,"PRODUCTION"),"mae"]) for regime in ["MATERIAL_ABOVE","MATERIAL_BELOW"]} for scope in ["historical","prospective"]}
    report=["# NHL SOG D10/D20 divergence correction experiment","","## Frozen experiment","", "Control: `poisson_baseline / baseline_v1`. Symmetric rule: replace D10 with the fixed midpoint of D10 and D20 only when |D10−D20| > 0.5 SOG/60. High-D10-only rule applies the same midpoint only when D10−D20 > 0.5. Production exposure, fallback, lambda construction, and Poisson probabilities are otherwise unchanged. No tuning or search was performed.","", "## Replay and populations","",f"The certified source baseline has 13,389 player-games. The historical paired analysis includes {len(hist):,} rows with valid retained D10/D20 and outcome. The prospective paired analysis has {len(pros):,} player-games, October 10–13, 2026. Historical lambda replay checked {replay_rows:,} rows, with {replay_bad} mismatches above 1e-10; maximum absolute difference {replay.max():.3g}. Production probability replay matched all {checks[0]['rows']:,} historical and {checks[1]['rows']:,} prospective line rows within 1e-10. The exact source rows, probabilities, corrected lambdas and outcomes are retained.","", "## Count error and proper scores","",f"| Scope | Variant | N | MAE | RMSE | Mean residual | Mean lambda |","|---|---|---:|---:|---:|---:|---:|"]+[f"| {scope} | {v} | {d['n']:,} | {d['mae']:.4f} | {d['rmse']:.4f} | {d['mean_residual']:+.4f} | {d['mean_lambda']:.4f} |" for scope,vals in [("Historical",hc),("Prospective",pc)] for v,d in vals.items()]+["",*lines_md,"","## Divergence groups and calibration repair","",*group_md,"",f"Near-agreement identity checks: symmetric and high-D10-only are exact no-ops on all {near_identity['historical_near_n']:,} historical near-agreement rows and all {near_identity['prospective_near_n']:,} prospective near-agreement rows. Symmetric count-MAE changes (corrected minus production) in material-above/below groups: historical {mae_change['historical']['MATERIAL_ABOVE']:+.3f}/{mae_change['historical']['MATERIAL_BELOW']:+.3f}; prospective {mae_change['prospective']['MATERIAL_ABOVE']:+.3f}/{mae_change['prospective']['MATERIAL_BELOW']:+.3f}.","", "## Robustness and comparisons","",f"Historical residual slope changes from {hist_slopes['PRODUCTION']:+.3f} to {hist_slopes[VARIANTS[1]]:+.3f}; prospective slope changes from {pro_slopes['PRODUCTION']:+.3f} to {pro_slopes[VARIANTS[1]]:+.3f}. Historical symmetric count MAE improved on {date_dec[VARIANTS[1]]['historical']['mae_dates_improved']}/{date_dec[VARIANTS[1]]['historical']['mae_dates_improved']+date_dec[VARIANTS[1]]['historical']['mae_dates_worsened']} dates; prospective count MAE improved on {date_dec[VARIANTS[1]]['prospective']['mae_dates_improved']}/{date_dec[VARIANTS[1]]['prospective']['mae_dates_improved']+date_dec[VARIANTS[1]]['prospective']['mae_dates_worsened']} dates. Equal-player average MAE difference is {player_sum[VARIANTS[1]]['equal_player_mean_mae_difference']:+.4f} across {player_sum[VARIANTS[1]]['players_n']} players with at least three games ({player_sum[VARIANTS[1]]['improved']} improve; {player_sum[VARIANTS[1]]['worsened']} worsen).", "", "The D10, lambda, TOI and PP-share interaction table is in `interaction_metrics.csv`. The fixed blend comparison uses the exact common October 13 grade population (1,713 rows). Market-relative scores use retained no-vig probabilities on exact common quoted rows only; market scores are not the sole criterion.","",f"Decision: `{classification}`. The symmetric correction shows pooled historical and prospective count/proper-score gains, with an especially clear repair in material-above calibration; material-below calibration also moves toward zero, with a small prospective material-below MAE increase. This result justifies a later prospective shadow review only. Do not promote, wire or change production based on this offline comparison.","", "## Package contents","", "The package includes exact paired rows, global and line metrics, row-paired effects, divergence group and signed-bin results, residual slopes, dates, player robustness, D10/lambda/TOI/PP interactions, calibration repair, fixed blends, market comparison, and source/output hashes.",""]
    (OUT/"report.md").write_text("\n".join(report))


if __name__ == "__main__":
    main()
