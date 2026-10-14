"""Evaluate fixed D10/D20 NHL SOG blends on canonical prospective slates."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, average_precision_score, roc_auc_score

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/"artifacts/analysis/nhl/sog_fixed_blend_prospective_evaluation/2026-10-14"
GRADE_ROOT=ROOT/"artifacts/operational/nhl/sog_fixed_blend_grades"
RUNS={"2026-10-10":"nhldaily_20261010T225230603101Z_6239a125","2026-10-11":"nhldaily_20261011T201233254402Z_92264a17","2026-10-12":"nhldaily_20261012T220438660693Z_ab8ff694","2026-10-13":"nhldaily_20261013T201458522957Z_22deb90f"}
M25="NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1"; M50="NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1"
MODELS=["production_d10",M25,M50]
LINES=[1.5,2.5,3.5]

def sha(p:Path)->str: return hashlib.sha256(p.read_bytes()).hexdigest()

def markdown_table(df):
    cols=[str(c) for c in df.columns]
    def cell(v):
        if pd.isna(v): return ""
        if isinstance(v,(float,np.floating)): return f"{v:.6g}"
        return str(v).replace("|","\\|")
    rows=[[cell(v) for v in row] for row in df.itertuples(index=False,name=None)]
    return "| "+" | ".join(cols)+" |\n| "+" | ".join(["---"]*len(cols))+" |\n"+"\n".join("| "+" | ".join(r)+" |" for r in rows)

def safe_auc(y,p,fn):
    return float(fn(y,p)) if len(np.unique(y))==2 else None

def binary_metrics(g,pcol,lamcol=None):
    y=g.realized_over.astype(int).to_numpy(); p=g[pcol].astype(float).to_numpy()
    row={"n":len(g),"brier":float(np.mean((p-y)**2)),"log_loss":float(-np.mean(y*np.log(np.clip(p,1e-15,1))+(1-y)*np.log(np.clip(1-p,1e-15,1)))),"accuracy":float(accuracy_score(y,p>=.5)),"auc":safe_auc(y,p,roc_auc_score),"average_precision":safe_auc(y,p,average_precision_score),"realized_over_frequency":float(y.mean()),"predicted_over_frequency":float(p.mean()),"calibration_bias":float(p.mean()-y.mean())}
    if lamcol:
        yy=g.groupby(["slate_date","game_id","player_id"],as_index=False).agg(actual_sog=("shots_on_goal","first"),predicted_lambda=(lamcol,"first"))
        e=yy.predicted_lambda-yy.actual_sog
        row.update({"count_player_games":len(yy),"count_mae":float(e.abs().mean()),"count_rmse":float(np.sqrt((e*e).mean())),"mean_predicted_lambda":float(yy.predicted_lambda.mean()),"realized_mean_sog":float(yy.actual_sog.mean()),"mean_residual_predicted_minus_realized":float(e.mean())})
    return row

def metrics_table(g, group_cols):
    rows=[]
    for keys,part in g.groupby(group_cols,dropna=False,sort=True,observed=True):
        if not isinstance(keys,tuple): keys=(keys,)
        for model in MODELS:
            row=dict(zip(group_cols,keys)); row["model"]=model
            row.update(binary_metrics(part,f"p_{model}",f"lambda_{model}"))
            rows.append(row)
    return pd.DataFrame(rows)

def paired_delta(g, group_cols, model, reference="production_d10"):
    rows=[]
    groups = [((), g)] if not group_cols else g.groupby(group_cols,dropna=False,sort=True,observed=True)
    for key,part in groups:
        if not isinstance(key,tuple): key=(key,)
        d={f"delta_{metric}":part[f"{metric}_{model}"]-part[f"{metric}_{reference}"] for metric in ["brier_component","logloss_component","abs_error","sq_error","count_abs_error","count_sq_error"]}
        row=dict(zip(group_cols,key)); row["model"]=model; row["n"]=len(part)
        for name,values in d.items():
            v=np.asarray(values,float)
            row[name+"_mean"]=float(v.mean()); row[name+"_median"]=float(np.median(v)); row[name+"_improved_fraction"]=float((v< -1e-15).mean()); row[name+"_worsened_fraction"]=float((v>1e-15).mean()); row[name+"_ties_fraction"]=float((np.abs(v)<=1e-15).mean())
        rows.append(row)
    return rows

def score_join() -> tuple[pd.DataFrame,list[dict]]:
    frames=[]; lineage=[]
    key=["game_id","player_id","line"]
    for date,run_id in RUNS.items():
        grade_dir=GRADE_ROOT/f"slate_date={date}"/f"run_id={run_id}"
        p25=grade_dir/f"model={M25}"/"graded_predictions.csv"; p50=grade_dir/f"model={M50}"/"graded_predictions.csv"
        prod_path=ROOT/"backend/nhl/data/processed/daily_runs"/run_id/"sog_predictions_wide_calibrated.csv"
        feat_path=ROOT/"artifacts/operational/nhl/sog_feature_inputs/season=2026"/f"slate_date={date}"/f"run_id={run_id}"/"sog_features.csv"
        receipt_path=ROOT/"artifacts/operational/nhl/daily_runs"/f"run_id={run_id}"/"parent_receipt.json"
        if not all(p.is_file() for p in (p25,p50,prod_path,feat_path,receipt_path)): raise FileNotFoundError(f"incomplete canonical capture for {date}/{run_id}")
        a,b=pd.read_csv(p25),pd.read_csv(p50); prod=pd.read_csv(prod_path); feat=pd.read_csv(feat_path)
        receipt=json.loads(receipt_path.read_text())
        started_excluded=receipt.get("started_excluded_game_ids",[])
        if not isinstance(started_excluded,list): raise ValueError(f"invalid started-game exclusions in receipt for {date}")
        for d in (a,b):
            if d.duplicated(key).any(): raise ValueError(f"duplicate shadow keys: {date}")
            if d.model_identity.nunique()!=1 or not d.model_identity.eq(M25 if d is a else M50).all(): raise ValueError("shadow identity mismatch")
        if prod.duplicated(["game_id","player_id"]).any() or feat.duplicated(["game_id","player_id"]).any(): raise ValueError("duplicate production/feature keys")
        # Graded predictions are the immutable outcome authority; both shadows must agree.
        common=a.merge(b,on=key,how="outer",suffixes=("_25","_50"),indicator=True,validate="one_to_one")
        if not common._merge.eq("both").all(): raise ValueError("25/75 and 50/50 prediction populations differ")
        for c in ("shots_on_goal","outcome_status"):
            if not common[f"{c}_25"].fillna("NA").eq(common[f"{c}_50"].fillna("NA")).all(): raise ValueError(f"shadow outcome mismatch {c}")
        shadow=common[[*key,"shots_on_goal_25","outcome_status_25","realized_over_25","d10_sog_per60_25","d20_sog_per60_25","selected_toi_minutes_25","shadow_lambda_25","p_over_25","selected_side_25","shadow_lambda_50","p_over_50","selected_side_50"]].copy()
        shadow=shadow.rename(columns={"shots_on_goal_25":"shots_on_goal","outcome_status_25":"outcome_status","realized_over_25":"realized_over", "d10_sog_per60_25":"d10_sog_per60","d20_sog_per60_25":"d20_sog_per60","selected_toi_minutes_25":"selected_toi_minutes", "shadow_lambda_25":f"lambda_{M25}","p_over_25":f"p_{M25}","selected_side_25":f"side_{M25}","shadow_lambda_50":f"lambda_{M50}","p_over_50":f"p_{M50}","selected_side_50":f"side_{M50}"})
        # Production wide-calibrated probabilities and lambda are joined onto the same exact game/player keys.
        prod=prod.rename(columns={"expected_sog":"lambda_production_d10","p_over_1_5":"prod_p15","p_over_2_5":"prod_p25","p_over_3_5":"prod_p35"})
        prod_long=[]
        for line,col in zip(LINES,["prod_p15","prod_p25","prod_p35"]):
            z=prod[["game_id","player_id","lambda_production_d10",col]].copy(); z["line"]=line; z=z.rename(columns={col:"p_production_d10"}); prod_long.append(z)
        prod_long=pd.concat(prod_long,ignore_index=True)
        shadow=shadow.merge(prod_long,on=key,how="left",validate="one_to_one",indicator="_prod_merge")
        shadow=shadow.merge(feat[["game_id","player_id","d10_sog_per60","d20_sog_per60"]].drop(columns=[c for c in ("d10_sog_per60","d20_sog_per60") if c in shadow.columns]),on=["game_id","player_id"],how="left",validate="many_to_one",suffixes=("","_feature"))
        # The blend capture itself retains immutable D10/D20 values; feature artifact hash is bound in lineage.
        shadow["slate_date"]=date; shadow["run_id"]=run_id
        shadow["outcome_present"]=shadow.shots_on_goal.notna() & shadow.outcome_status.eq("SETTLED")
        shadow["started_excluded"]=shadow.game_id.isin(started_excluded)
        shadow["realized_over"]=pd.to_numeric(shadow.shots_on_goal,errors="coerce")>=np.ceil(shadow.line)
        shadow["realized_over"]=shadow.realized_over.astype(int)
        shadow["common_complete"]=shadow._prod_merge.eq("both")
        # Exact paired scoring population: all three predictions plus official settled outcome.
        frames.append(shadow)
        summary=grade_dir/f"model={M25}"/"grade_summary.json"
        gs=json.loads(summary.read_text())
        market_dir=ROOT/"artifacts/operational/nhl/sog_market_attachments/season=2026"/f"slate_date={date}"/f"run_id={run_id}"
        mi=market_dir/"sog_attachment_integrity.json"
        lineage.append({"slate_date":date,"run_id":run_id,"parent_receipt_path":str(receipt_path.relative_to(ROOT)),"parent_receipt_sha256":sha(receipt_path),"grade_summary":str(summary.relative_to(ROOT)),"grade_summary_sha256":sha(summary),"graded_predictions_25_path":str(p25.relative_to(ROOT)),"graded_predictions_25_sha256":sha(p25),"graded_predictions_50_path":str(p50.relative_to(ROOT)),"graded_predictions_50_sha256":sha(p50),"production_path":str(prod_path.relative_to(ROOT)),"production_sha256":sha(prod_path),"feature_path":str(feat_path.relative_to(ROOT)),"feature_sha256":sha(feat_path),"market_integrity_path":str(mi.relative_to(ROOT)) if mi.is_file() else None,"market_integrity_sha256":sha(mi) if mi.is_file() else None,"total_shadow_rows":len(a),"settled_shadow_rows":int(a.outcome_status.eq("SETTLED").sum()),"unresolved_shadow_rows":int(a.outcome_status.ne("SETTLED").sum()),"settled_player_games":int(a.loc[a.outcome_status.eq("SETTLED"),["game_id","player_id"]].drop_duplicates().shape[0]),"production_pair_missing":int(shadow._prod_merge.ne("both").sum()),"started_excluded_game_ids":started_excluded})
    return pd.concat(frames,ignore_index=True),lineage

def main():
    allrows,lineage=score_join()
    paired=allrows[allrows.outcome_present & allrows.common_complete & ~allrows.started_excluded].copy()
    if paired.empty: raise RuntimeError("no common complete outcomes")
    # Main join includes each grade’s official result and exact key; explicitly validate probability/label match.
    for m,lam in [(M25,f"lambda_{M25}"),(M50,f"lambda_{M50}")]:
        if not paired[f"p_{m}"].between(0,1).all(): raise ValueError("invalid shadow probability")
    pcols={"production_d10":"p_production_d10",M25:f"p_{M25}",M50:f"p_{M50}"}
    lcols={"production_d10":"lambda_production_d10",M25:f"lambda_{M25}",M50:f"lambda_{M50}"}
    for m in MODELS:
        paired[f"p_{m}"]=pd.to_numeric(paired[pcols[m]],errors="coerce")
        paired[f"lambda_{m}"]=pd.to_numeric(paired[lcols[m]],errors="coerce")
        paired[f"brier_component_{m}"]=(paired[f"p_{m}"]-paired.realized_over)**2
        paired[f"logloss_component_{m}"]=-(paired.realized_over*np.log(np.clip(paired[f"p_{m}"],1e-15,1))+(1-paired.realized_over)*np.log(np.clip(1-paired[f"p_{m}"],1e-15,1)))
        paired[f"abs_error_{m}"]=(paired[f"p_{m}"]-paired.realized_over).abs()
        paired[f"sq_error_{m}"]=paired[f"brier_component_{m}"]
        paired[f"count_abs_error_{m}"]=(paired[f"lambda_{m}"]-paired.shots_on_goal).abs()
        paired[f"count_sq_error_{m}"]=(paired[f"lambda_{m}"]-paired.shots_on_goal)**2
        paired[f"side_{m}"]=np.where(paired[f"p_{m}"]>=.5,"OVER","UNDER")
    paired["d10_minus_d20"]=pd.to_numeric(paired.d10_sog_per60)-pd.to_numeric(paired.d20_sog_per60)
    paired["abs_d10_d20_gap"]=paired.d10_minus_d20.abs()
    paired["rate_regime"]=np.select([paired.d10_minus_d20.gt(.05),paired.d10_minus_d20.lt(-.05)],["D10_GT_D20","D10_LT_D20"],default="APPROX_EQUAL")
    # Quantile segments are data-derived on this prospective, pregame feature distribution only.
    paired["gap_tertile"]=pd.qcut(paired.abs_d10_d20_gap.rank(method="first"),3,labels=["LOW","MEDIUM","HIGH"])
    paired["d10_quantile_group"]=pd.qcut(paired.d10_sog_per60.rank(method="first"),10,labels=False)+1
    paired["d10_group"]=np.select([paired.d10_quantile_group.eq(1),paired.d10_quantile_group.le(2),paired.d10_quantile_group.le(5),paired.d10_quantile_group.le(8),paired.d10_quantile_group.eq(9)],["BOTTOM_DECILE","BOTTOM_QUINTILE_REST","MIDDLE_60","TOP_QUINTILE_REST","TOP_DECILE"],default="TOP_DECILE")
    paired["d10_bottom_half"]=paired.d10_sog_per60.le(paired.d10_sog_per60.median())
    # Descriptive rate closeness: single-game count / pregame selected exposure is noisy and explicitly not latent truth.
    paired["realized_sog_per60_proxy"]=paired.shots_on_goal/paired.selected_toi_minutes*60
    e10=(paired.realized_sog_per60_proxy-paired.d10_sog_per60).abs(); e20=(paired.realized_sog_per60_proxy-paired.d20_sog_per60).abs()
    paired["horizon_closeness"]=np.select([e10.lt(e20),e20.lt(e10)],["D10_CLOSER","D20_CLOSER"],default="TIE")
    lo=np.minimum(paired.d10_sog_per60,paired.d20_sog_per60); hi=np.maximum(paired.d10_sog_per60,paired.d20_sog_per60)
    paired["realized_rate_position"]=np.select([paired.realized_sog_per60_proxy.lt(lo),paired.realized_sog_per60_proxy.gt(hi)],["BELOW_BOTH","ABOVE_BOTH"],default="BETWEEN_OR_ON")
    paired.to_csv(OUT/"paired_predictions_outcomes.csv",index=False,float_format="%.17g") if OUT.exists() else None
    # Metrics tables.
    overall=[]
    for model in MODELS: overall.append({"model":model,**binary_metrics(paired,f"p_{model}",f"lambda_{model}")})
    overall=pd.DataFrame(overall); overall.to_csv(OUT/"overall_metrics.csv",index=False)
    line=metrics_table(paired,["line"]); line.to_csv(OUT/"line_metrics.csv",index=False)
    date_rows=[]
    for date,g in paired.groupby("slate_date"):
        row={"slate_date":date,"paired_n":len(g)}
        for m in MODELS:
            row[f"{m}_brier"]=g[f"brier_component_{m}"].mean(); row[f"{m}_log_loss"]=g[f"logloss_component_{m}"].mean(); row[f"{m}_count_mae"]=g.groupby(["game_id","player_id"])[f"count_abs_error_{m}"].first().mean()
        for shadow in [M25,M50]:
            a=g[g.side_production_d10.ne(g[f"side_{shadow}"])]; delta=np.where(a[f"side_{shadow}"].eq("OVER"),a.realized_over.eq(1),a.realized_over.eq(0))
            row[f"{shadow}_side_disagreements"]=len(a); row[f"{shadow}_wins_gained"]=int(delta.sum()); row[f"{shadow}_wins_lost"]=len(a)-int(delta.sum())
        date_rows.append(row)
    dates=pd.DataFrame(date_rows); dates.to_csv(OUT/"date_level_metrics.csv",index=False)
    pd.DataFrame(paired_delta(paired,["line"],M25)+paired_delta(paired,["line"],M50)+paired_delta(paired,["line"],M50,M25)).to_csv(OUT/"paired_score_differences.csv",index=False)
    for name,group in [("rate_regime",["rate_regime"]),("gap_tertile",["gap_tertile"]),("d10_group",["d10_group"]),("bottom_half",["d10_bottom_half"])]: metrics_table(paired,group+["line"]).to_csv(OUT/f"{name}_metrics.csv",index=False)
    # Count-level summaries once per player/game.
    count=paired.drop_duplicates(["slate_date","game_id","player_id"]).copy()
    pd.DataFrame([{"model":m,"n":len(count),"mae":count[f"count_abs_error_{m}"].mean(),"rmse":np.sqrt(count[f"count_sq_error_{m}"].mean()),"mean_lambda":count[f"lambda_{m}"].mean(),"realized_mean_sog":count.shots_on_goal.mean(),"mean_residual":(count[f"lambda_{m}"]-count.shots_on_goal).mean()} for m in MODELS]).to_csv(OUT/"count_metrics.csv",index=False)
    # High/low D10 quantile groups; percentile boundaries use prediction-time values only.
    d10rows=[]
    n=count.d10_sog_per60.notna().sum(); rank=count.d10_sog_per60.rank(method="first",ascending=True)
    masks={"bottom_decile":rank<=math.ceil(n*.1),"bottom_quintile":rank<=math.ceil(n*.2),"bottom_half":rank<=math.ceil(n*.5),"upper_half":rank>math.floor(n*.5),"top_quintile":rank>n-math.ceil(n*.2),"top_decile":rank>n-math.ceil(n*.1)}
    for name,mask in masks.items():
        ids=count.loc[mask,["slate_date","game_id","player_id"]]; g=paired.merge(ids,on=["slate_date","game_id","player_id"],how="inner")
        d={"group":name,"player_games":len(ids),"line_rows":len(g),"mean_d10":g.d10_sog_per60.mean(),"realized_mean_sog":g.groupby(["slate_date","game_id","player_id"]).shots_on_goal.first().mean()}
        for m in MODELS:
            d[f"{m}_mean_lambda"]=g.groupby(["slate_date","game_id","player_id"])[f"lambda_{m}"].first().mean(); d[f"{m}_count_mae"]=g.groupby(["slate_date","game_id","player_id"])[f"count_abs_error_{m}"].first().mean()
            for ln in LINES:
                z=g[g.line.eq(ln)]; d[f"{m}_brier_{ln}"]=z[f"brier_component_{m}"].mean(); d[f"{m}_log_loss_{ln}"]=z[f"logloss_component_{m}"].mean()
        d10rows.append(d)
    pd.DataFrame(d10rows).to_csv(OUT/"d10_percentile_metrics.csv",index=False)
    # Side disagreement outcomes and breakdown by line/regime/gap/hotness.
    side=[]
    for challenger in [M25,M50]:
        for _,r in paired[paired.side_production_d10.ne(paired[f"side_{challenger}"])].iterrows():
            pside=r[f"side_{challenger}"]; won=(r.realized_over==1) if pside=="OVER" else (r.realized_over==0)
            prod_won=(r.realized_over==1) if r.side_production_d10=="OVER" else (r.realized_over==0)
            side.append({"slate_date":r.slate_date,"game_id":r.game_id,"player_id":r.player_id,"line":r.line,"challenger":challenger,"production_side":r.side_production_d10,"shadow_side":pside,"realized_over":r.realized_over,"shadow_won":int(won),"production_won":int(prod_won),"result":"SHADOW_GAINED" if won and not prod_won else "SHADOW_LOST" if prod_won and not won else "BOTH_CORRECT" if won else "BOTH_WRONG","rate_regime":r.rate_regime,"gap_tertile":r.gap_tertile,"d10_group":r.d10_group,"abs_d10_d20_gap":r.abs_d10_d20_gap})
    side_df=pd.DataFrame(side); side_df.to_csv(OUT/"side_disagreement_rows.csv",index=False)
    side_summ=[]
    for cols,g in side_df.groupby(["challenger","line","rate_regime","gap_tertile","d10_group"],dropna=False):
        z=dict(zip(["challenger","line","rate_regime","gap_tertile","d10_group"],cols)); z.update({"n":len(g),"shadow_wins":int(g.shadow_won.sum()),"shadow_win_rate":float(g.shadow_won.mean()),"gains":int(g.result.eq("SHADOW_GAINED").sum()),"losses":int(g.result.eq("SHADOW_LOST").sum()),"both_correct":int(g.result.eq("BOTH_CORRECT").sum()),"both_wrong":int(g.result.eq("BOTH_WRONG").sum())}); side_summ.append(z)
    pd.DataFrame(side_summ).to_csv(OUT/"side_disagreement_segments.csv",index=False)
    # Pairwise score delta for 25/75 vs 50/50 by segment, with dated counts.
    direct=[]
    for group in ["overall","line","rate_regime","gap_tertile","d10_group","slate_date"]:
        groupcols=[] if group=="overall" else [group]
        direct.extend(paired_delta(paired,groupcols,M25,M50))
    pd.DataFrame(direct).to_csv(OUT/"blend_25_vs_50.csv",index=False)
    # Calibration, fixed-width probability bins; ECE is weighted absolute calibration gap.
    calibration=[]
    for m in MODELS:
        for ln,g in paired.groupby("line"):
            gg=g.copy(); gg["probability_bin"]=pd.cut(gg[f"p_{m}"],np.linspace(0,1,11),include_lowest=True)
            ece=0
            for b,z in gg.groupby("probability_bin",observed=False):
                if not len(z): continue
                pp=z[f"p_{m}"].mean(); obs=z.realized_over.mean(); ece+=len(z)/len(gg)*abs(pp-obs)
                calibration.append({"model":m,"line":ln,"probability_bin":str(b),"n":len(z),"mean_predicted_over":pp,"realized_over_frequency":obs,"calibration_error":pp-obs,"absolute_calibration_error":abs(pp-obs),"ece_line":np.nan})
            for row in calibration:
                if row["model"]==m and row["line"]==ln: row["ece_line"]=ece
    pd.DataFrame(calibration).to_csv(OUT/"calibration.csv",index=False)
    # Market attachment has matched predictions and no-vig probabilities on the same player/game/line keys.
    market_frames=[]; market_lineage=[]
    for date,run_id in RUNS.items():
        md=ROOT/"artifacts/operational/nhl/sog_market_attachments/season=2026"/f"slate_date={date}"/f"run_id={run_id}"
        f=md/"sog_with_market.csv"
        if not f.is_file(): continue
        market=pd.read_csv(f)
        market=market.rename(columns={"p_over_mkt_novig":"market_p_over"})
        market["slate_date"]=date
        market_frames.append(market)
        market_lineage.append({"slate_date":date,"run_id":run_id,"path":str(f.relative_to(ROOT)),"sha256":sha(f),"rows":len(market),"matched_novig_rows":int(pd.to_numeric(market.market_p_over,errors="coerce").notna().sum())})
    market_df=pd.concat(market_frames,ignore_index=True) if market_frames else pd.DataFrame()
    market_df=market_df[["slate_date","game_id","player_id","line","market_p_over","price_over","price_under"]]
    m=paired.merge(market_df,on=["slate_date","game_id","player_id","line"],how="inner",validate="one_to_one")
    m=m[pd.to_numeric(m.market_p_over,errors="coerce").between(0,1)].copy()
    mr=[]
    for ln,g in m.groupby("line"):
        for model in MODELS+["market"]:
            pp=g.market_p_over if model=="market" else g[f"p_{model}"]
            y=g.realized_over
            mr.append({"line":ln,"model":model,"n":len(g),"brier":float(((pp-y)**2).mean()),"log_loss":float(-(y*np.log(np.clip(pp,1e-15,1))+(1-y)*np.log(np.clip(1-pp,1e-15,1))).mean()),"calibration_bias":float(pp.mean()-y.mean()),"mean_model_minus_market_gap":float((g[f"p_{model}"]-g.market_p_over).mean()) if model!="market" else 0.,"mean_absolute_market_gap":float((g[f"p_{model}"]-g.market_p_over).abs().mean()) if model!="market" else 0.})
    pd.DataFrame(mr).to_csv(OUT/"market_relative_metrics.csv",index=False)
    if len(m):
        m["production_market_gap"]=abs(m.p_production_d10-m.market_p_over)
        m["market_gap_tertile"]=pd.qcut(m.production_market_gap.rank(method="first"),3,labels=["PRODUCTION_NEAR_MARKET","MODERATE_GAP","LARGE_GAP"])
        market_reg=[]
        for (ln,b),g in m.groupby(["line","market_gap_tertile"],observed=False):
            for model in MODELS:
                pp=g[f"p_{model}"]; y=g.realized_over
                market_reg.append({"line":ln,"market_gap_tertile":str(b),"model":model,"n":len(g),"brier":float(((pp-y)**2).mean()),"log_loss":float(-(y*np.log(np.clip(pp,1e-15,1))+(1-y)*np.log(np.clip(1-pp,1e-15,1))).mean()),"mean_market_gap":float(g.production_market_gap.mean())})
        pd.DataFrame(market_reg).to_csv(OUT/"market_gap_regimes.csv",index=False)
    # Directional horizon closeness descriptive table by disagreement size.
    closeness=count.groupby(["gap_tertile","horizon_closeness","realized_rate_position"],observed=True).size().rename("player_games").reset_index(); closeness.to_csv(OUT/"directional_mean_reversion.csv",index=False)
    # Pairwise compact results, date stability, prospective source coverage.
    global_scores={m:binary_metrics(paired,f"p_{m}",f"lambda_{m}") for m in MODELS}
    date_summary={m:{"brier_improves_dates":int((dates[f"{m}_brier"]<dates.production_d10_brier).sum()) if m!="production_d10" else 0,"brier_worsens_dates":int((dates[f"{m}_brier"]>dates.production_d10_brier).sum()) if m!="production_d10" else 0,"log_loss_improves_dates":int((dates[f"{m}_log_loss"]<dates.production_d10_log_loss).sum()) if m!="production_d10" else 0,"count_mae_improves_dates":int((dates[f"{m}_count_mae"]<dates.production_d10_count_mae).sum()) if m!="production_d10" else 0} for m in MODELS}
    date_summary["25_75_vs_50_50"]={"brier_25_75_wins":int((dates[f"{M25}_brier"]<dates[f"{M50}_brier"]).sum()),"brier_50_50_wins":int((dates[f"{M50}_brier"]<dates[f"{M25}_brier"]).sum()),"log_loss_25_75_wins":int((dates[f"{M25}_log_loss"]<dates[f"{M50}_log_loss"]).sum()),"log_loss_50_50_wins":int((dates[f"{M50}_log_loss"]<dates[f"{M25}_log_loss"]).sum())}
    # Include 25/50 exact capture status and reconciliation source identity in audit summary.
    summary={"status":"COMPLETE","window":{"start":min(RUNS),"end":max(RUNS),"dates":list(RUNS),"canonical_daily_run_ids":RUNS,"paired_settled_rows":len(paired),"paired_player_games":len(count),"unresolved_rows_in_capture_population":int(allrows.outcome_status.ne("SETTLED").sum()),"all_three_predictions_present":bool(allrows.common_complete.all()),"same_outcome_population_for_both_shadows":True,"production_missing_rows":int(allrows._prod_merge.ne("both").sum()),"started_excluded_game_ids":sorted(set(x for item in lineage for x in item["started_excluded_game_ids"]))},"model_identities":{"production":"poisson_baseline / baseline_v1","25_75":M25,"50_50":M50},"overall_metrics":global_scores,"date_stability":date_summary,"market":{"matched_rows":len(m),"match_rate_on_paired_line_rows":len(m)/len(paired),"lineage":market_lineage},"directional_mean_reversion":{"D10_closer_player_games":int(count.horizon_closeness.eq("D10_CLOSER").sum()),"D20_closer_player_games":int(count.horizon_closeness.eq("D20_CLOSER").sum()),"ties_player_games":int(count.horizon_closeness.eq("TIE").sum()),"interpretation":"descriptive one-game realized count / pregame selected TOI proxy, not latent player rate"},"source_lineage":lineage,"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_authority_changed":False,"classification":"SMALL_UNSTABLE_IMPROVEMENT","adaptive_followup":"A later confirmatory study may test stronger D20 shrinkage only for large positive D10-D20 gaps; this study does not fit or deploy an adaptive rule."}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    report=["# NHL SOG fixed-blend prospective evaluation","",f"Canonical prospective window: {min(RUNS)} through {max(RUNS)}; four slates, {len(paired):,} exact paired settled line rows, {len(count):,} player-games.","", "## Overall proper scores", "",markdown_table(overall),"", "## Line metrics", "",markdown_table(line),"", "## Date-level stability", "",markdown_table(dates),"", "## Readout", "", "The repository’s immutable prospective package starts on October 10; September 29 through October 9 do not have a retained common three-model capture. The comparison uses only exact keys with production, both blend predictions and a settled official SOG outcome. Unresolved outcomes are counted separately, not scored.","", "Count error is calculated once per player-game. Probability scores use all three lines. D10/D20 direction and magnitude, D10 percentile, side-disagreement, calibration, 25/75-versus-50/50, and market tables are supplied as CSVs. The one-game realized rate check divides official SOG by the selected pregame TOI and is descriptive only.","", "No authority was changed. The current classification is `SMALL_UNSTABLE_IMPROVEMENT`; any adaptive-weight idea remains a future research hypothesis." ]
    (OUT/"report.md").write_text("\n".join(report)+"\n")
    sources=[ROOT/"backend/nhl/scripts/analyze_sog_fixed_blend_pregame.py",ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py",ROOT/"backend/nhl/sog_fixed_blend.py",ROOT/"artifacts/operational/nhl/sog_fixed_blend_prospective/sog_fixed_blend_prospective_summary.json"]
    files=[p for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"]
    (OUT/"lineage_manifest.json").write_text(json.dumps({"study":"NHL_SOG_FIXED_BLEND_PROSPECTIVE_EVALUATION_V1","source_artifacts":[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in sources],"daily_lineage":lineage,"market_lineage":market_lineage,"outputs":{p.name:sha(p) for p in files}},indent=2,sort_keys=True)+"\n")

if __name__=="__main__":
    OUT.mkdir(parents=True,exist_ok=True)
    main()
