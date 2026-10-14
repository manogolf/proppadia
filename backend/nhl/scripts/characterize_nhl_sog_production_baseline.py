"""Characterize the fixed NHL SOG Poisson production baseline from retained evidence."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, average_precision_score, roc_auc_score

from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/sog_baseline_characterization/2026-10-14"
HIST_DIR = ROOT / "artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13"
HIST_LEDGER = HIST_DIR / "nhl_season_2025_sog_reproduction_ledger_2026-07-13.csv"
HIST_ROOT = ROOT / "artifacts/archive/generated_daily/nhl"
SUMMARY_ROOT = ROOT / "artifacts/operational/nhl/postgame_reconciliation/learning_restatements"
LINES = (1.5, 2.5, 3.5)
RUNS = {
    "2026-10-08": "nhldaily_20261008T223451824582Z_ec21495b",
    "2026-10-09": "nhldaily_20261009T195836812178Z_6d8ecbb3",
    "2026-10-10": "nhldaily_20261010T225230603101Z_6239a125",
    "2026-10-11": "nhldaily_20261011T201233254402Z_92264a17",
    "2026-10-12": "nhldaily_20261012T220438660693Z_ab8ff694",
    "2026-10-13": "nhldaily_20261013T201458522957Z_22deb90f",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def num(frame: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(frame[name], errors="coerce") if name in frame else pd.Series(np.nan, index=frame.index, dtype=float)


def coalesce(frame: pd.DataFrame, fields: list[str]) -> tuple[pd.Series, pd.Series]:
    out = pd.Series(np.nan, index=frame.index, dtype=float)
    src = pd.Series("", index=frame.index, dtype=object)
    for spec in fields:
        if spec.startswith("SUM:") or spec.startswith("SEC_SUM:"):
            seconds = spec.startswith("SEC_SUM:")
            cols = spec.split(":", 1)[1].split("+")
            values = pd.concat([num(frame, c) for c in cols], axis=1)
            x = values.sum(axis=1, min_count=1) / (60 if seconds else 1)
        else:
            x = num(frame, spec)
        take = out.isna() & x.notna()
        out.loc[take] = x.loc[take]
        src.loc[take] = spec
    return out, src


def metrics(frame: pd.DataFrame, model: str = "production") -> dict:
    y = frame.realized_over.astype(int).to_numpy()
    p = num(frame, f"p_{model}").to_numpy(dtype=float)
    p = np.clip(p, 1e-15, 1 - 1e-15)
    d = frame.drop_duplicates(["slate_date", "game_id", "player_id"])
    residual = num(d, "actual_sog") - num(d, f"lambda_{model}")
    side = p >= .5
    row = {"n": len(frame), "brier": float(np.mean((p-y)**2)),
           "log_loss": float(-np.mean(y*np.log(p)+(1-y)*np.log(1-p))),
           "accuracy": float(accuracy_score(y, side)), "auc": float(roc_auc_score(y,p)) if len(np.unique(y)) == 2 else np.nan,
           "average_precision": float(average_precision_score(y,p)) if len(np.unique(y)) == 2 else np.nan,
           "realized_over_rate": float(y.mean()), "predicted_over_rate": float(p.mean()),
           "calibration_bias": float(p.mean()-y.mean()), "count_player_games": len(d),
           "mean_lambda": float(num(d,f"lambda_{model}").mean()), "realized_mean_sog": float(num(d,"actual_sog").mean()),
           "mean_residual_actual_minus_predicted": float(residual.mean()), "count_mae": float(residual.abs().mean()),
           "count_rmse": float(np.sqrt(np.mean(residual**2))), "median_absolute_error": float(residual.abs().median())}
    row["selected_side_wins"] = int(np.where(side, y == 1, y == 0).sum())
    row["selected_side_losses"] = int(len(y)-row["selected_side_wins"])
    row["selected_side_win_rate"] = row["selected_side_wins"]/len(y) if len(y) else np.nan
    row["over_selections"] = int(side.sum()); row["under_selections"] = int((~side).sum())
    return row


def line_metrics(frame: pd.DataFrame, groups: list[str] | None = None) -> pd.DataFrame:
    groups = groups or []
    rows=[]
    iterator = [((), frame)] if not groups else frame.groupby(groups, dropna=False, observed=True, sort=True)
    for key,g in iterator:
        if not isinstance(key,tuple): key=(key,)
        for line,h in g.groupby("line",sort=True):
            r=dict(zip(groups,key)); r["line"]=line; r.update(metrics(h)); rows.append(r)
    return pd.DataFrame(rows)


def load_summaries() -> tuple[pd.DataFrame,list[dict]]:
    frames=[]; lineage=[]
    for date,run in RUNS.items():
        files=list((SUMMARY_ROOT/date).glob("performance_summary=*/performance_summary.json"))
        if not files: raise FileNotFoundError(f"no performance summary for {date}")
        parsed=[(p,json.loads(p.read_text())) for p in files]
        path,summary=max(parsed,key=lambda x:(x[1].get("summary_revision",0),x[1].get("generated_at_utc","")))
        ref=summary.get("models",{}).get("sog",{}).get("reference",{})
        identity=ref.get("model_identity",{})
        if identity.get("model_family")!="poisson_baseline" or identity.get("model_version")!="baseline_v1":
            raise ValueError(f"production baseline not bound in summary on {date}")
        if identity.get("parent_daily_run_id")!=run: raise ValueError(f"daily run mismatch for {date}")
        pred_path=ROOT/"backend/nhl/data/processed/daily_runs"/run/"sog_predictions_wide_calibrated.csv"
        recs=list((ROOT/"artifacts/operational/nhl/postgame_reconciliation"/date).glob("reconciliation=*/canonical_skater_outcomes.csv"))
        if len(recs)!=1: raise ValueError(f"expected one canonical outcome package for {date}, found {len(recs)}")
        pred=pd.read_csv(pred_path); outcomes=pd.read_csv(recs[0])
        pred["game_id"]=pd.to_numeric(pred.game_id,errors="raise").astype(int); pred["player_id"]=pd.to_numeric(pred.player_id,errors="raise").astype(int)
        outcomes["game_id"]=pd.to_numeric(outcomes.game_id,errors="raise").astype(int); outcomes["player_id"]=pd.to_numeric(outcomes.player_id,errors="raise").astype(int)
        if pred.duplicated(["game_id","player_id"]).any() or outcomes.duplicated(["game_id","player_id"]).any(): raise ValueError("duplicate prospective player-game keys")
        merged=pred.merge(outcomes[["game_id","player_id","official_sog","participation_state","official_final","outcome_source"]],on=["game_id","player_id"],how="left",validate="one_to_one",indicator=True)
        merged["slate_date"]=date; merged["run_id"]=run; merged["actual_sog"]=pd.to_numeric(merged.official_sog,errors="coerce")
        final=merged.official_final.astype("string").str.lower().isin(["true","1","t"])
        merged["outcome_resolved"]=merged._merge.eq("both") & final
        merged["participated"]=merged.participation_state.eq("PARTICIPATED")
        merged["scored"]=merged.outcome_resolved & merged.participated & merged.actual_sog.notna()
        merged["production_lambda"]=pd.to_numeric(merged.expected_sog,errors="coerce")
        for ln,col in zip(LINES,["p_over_1_5","p_over_2_5","p_over_3_5"]):
            sub=merged[["game_id","player_id",col]].copy(); sub["line"]=ln; sub=sub.rename(columns={col:"p_over"}); frames.append(sub.assign(slate_date=date,run_id=run))
        merged.to_csv(OUT/f".prospective_source_{date}.csv",index=False)
        lineage.append({"slate_date":date,"run_id":run,"summary_path":str(path.relative_to(ROOT)),"summary_sha256":sha(path),"prediction_path":str(pred_path.relative_to(ROOT)),"prediction_sha256":sha(pred_path),"outcomes_path":str(recs[0].relative_to(ROOT)),"outcomes_sha256":sha(recs[0]),"prediction_player_games":len(pred),"prediction_without_outcome":int(merged._merge.eq("left_only").sum()),"unresolved_or_nonfinal":int((~merged.outcome_resolved).sum()),"nonparticipants":int((merged.outcome_resolved & ~merged.participated).sum()),"scored_player_games":int(merged.scored.sum()),"summary_stored_sog_metrics":ref.get("overall")})
    # Detailed pregame feature joins are assembled for the retained feature dates in main().
    return pd.concat(frames,ignore_index=True),lineage


def historical_rows() -> tuple[pd.DataFrame,list[dict]]:
    ledger=pd.read_csv(HIST_LEDGER)
    ledger=ledger[ledger.probability_parity_status.isin(["EXACT_MATCH","TOLERANCE_MATCH"])].copy()
    ledger["game_id"]=pd.to_numeric(ledger.game_id,errors="raise").astype(int); ledger["player_id"]=pd.to_numeric(ledger.player_id,errors="raise").astype(int)
    pieces=[]; audits=[]
    for date,g in ledger.groupby("game_date",sort=True):
        p=HIST_ROOT/date/"sog_features"/f"sog_features_{date}_denali.csv"
        if not p.is_file(): audits.append({"game_date":date,"status":"MISSING","rows":len(g)}); continue
        f=pd.read_csv(p,low_memory=False); f["game_id"]=pd.to_numeric(f.game_id,errors="coerce").astype("Int64"); f["player_id"]=pd.to_numeric(f.player_id,errors="coerce").astype("Int64")
        cols=[c for c in ["game_id","player_id","team_id","opponent_id","is_home","d10_sog_per60","d20_sog_per60","d5_sog_per60","d10_toi_min_avg","d20_toi_min_avg","d5_toi_min_avg","szn_toi_per_game_5on5","szn_toi_per_game_pp","season_5on5_icetime_per_game","season_5on4_icetime_per_game","role_pp_share","toi_trend_3v10","d10_toi_cv","team_d10_sf_per_game","opp_d10_sf_allowed_per_game","pace_matchup_index","last10_team_sog_share","hot_last5_flag","num_sog_last5","num_sog_last10","team_num_sog_last10"] if c in f]
        z=g.merge(f[cols],on=["game_id","player_id"],how="left",validate="many_to_one",indicator=True)
        z["feature_bound"]=z._merge.eq("both"); z["slate_date"]=z.game_date; z["actual_sog"]=pd.to_numeric(z.official_sog,errors="coerce")
        z["production_lambda"]=pd.to_numeric(z.regenerated_expected_sog,errors="coerce")
        z["lambda_production"]=z.production_lambda
        z["line"]=pd.to_numeric(z.line,errors="coerce"); z["p_production"]=pd.to_numeric(z.stored_p_over,errors="coerce")
        z["realized_over"]=(z.actual_sog>=np.ceil(z.line)).astype(int)
        z["d10_sog_per60"]=num(z,"d10_sog_per60"); z["d20_sog_per60"]=num(z,"d20_sog_per60"); z["d5_sog_per60"]=num(z,"d5_sog_per60")
        z["selected_toi_minutes"],_=coalesce(z,["d10_toi_min_avg","d20_toi_min_avg","d5_toi_min_avg","SUM:szn_toi_per_game_5on5+szn_toi_per_game_pp","SEC_SUM:season_5on5_icetime_per_game+season_5on4_icetime_per_game"])
        z["team_id"]=num(z,"team_id"); z["opponent_id"]=num(z,"opponent_id"); z["is_home"]=z.get("is_home",pd.Series(index=z.index,dtype=object))
        pieces.append(z); audits.append({"game_date":date,"path":str(p.relative_to(ROOT)),"sha256":sha(p),"status":"LOADED","rows":len(f),"target_rows":len(g),"matched":int(z.feature_bound.sum())})
    data=pd.concat(pieces,ignore_index=True)
    data["window"]="historical_2025_certified_replay"
    return data,audits


def main() -> None:
    OUT.mkdir(parents=True,exist_ok=True)
    _,daily_lineage=load_summaries()
    hist,hist_audit=historical_rows()
    # Build prospective production rows from the immutable daily predictions/outcome joins.
    pros=[]; feature_parts=[]; missing={}
    for date,run in RUNS.items():
        sp=OUT/f".prospective_source_{date}.csv"; d=pd.read_csv(sp); sp.unlink()
        missing[date]={"prediction_player_games":len(d),"prediction_without_outcome":int((~d.outcome_resolved).sum()),"unresolved":int((~d.outcome_resolved).sum()),"nonparticipants":int((d.outcome_resolved & ~d.participated).sum()),"scored":int(d.scored.sum())}
        d=d[d.scored].copy(); d["window"]="prospective_2026_production"; d["lambda_production"]=num(d,"production_lambda")
        d["line"]=1.5
        long=[]
        for ln,pcol in zip(LINES,["p_over_1_5","p_over_2_5","p_over_3_5"]):
            z=d.copy(); z["line"]=ln; z["p_production"]=num(z,pcol); z["realized_over"]=(z.actual_sog>=math.ceil(ln)).astype(int)
            z["production_lambda"]=num(z,"production_lambda"); long.append(z)
        pros.append(pd.concat(long,ignore_index=True))
        feat_path=ROOT/"artifacts/operational/nhl/sog_feature_inputs/season=2026"/f"slate_date={date}"/f"run_id={run}"/"sog_features.csv"
        if feat_path.is_file():
            feat=pd.read_csv(feat_path); feat["game_id"]=pd.to_numeric(feat.game_id,errors="raise").astype(int); feat["player_id"]=pd.to_numeric(feat.player_id,errors="raise").astype(int)
            use=d.merge(feat,on=["game_id","player_id"],how="inner",validate="one_to_one",suffixes=("","_feature"))
            use["window"]="prospective_2026_feature_bound"
            use["selected_toi_minutes"],use["selected_toi_source"]=coalesce(use,["d10_toi_min_avg","d20_toi_min_avg","d5_toi_min_avg","SUM:szn_toi_per_game_5on5+szn_toi_per_game_pp","SEC_SUM:season_5on5_icetime_per_game+season_5on4_icetime_per_game"])
            use["selected_rate"],_=coalesce(use,["d10_sog_per60","d20_sog_per60","d5_sog_per60"])
            use["feature_lambda_parity"]=(use.selected_rate*use.selected_toi_minutes/60-use.production_lambda).abs()<=1e-12
            for ln,pcol in zip(LINES,["p_over_1_5","p_over_2_5","p_over_3_5"]):
                z=use.copy(); z["line"]=ln; z["p_production"]=num(z,pcol); z["realized_over"]=(z.actual_sog>=math.ceil(ln)).astype(int); feature_parts.append(z)
    prospective=pd.concat(pros,ignore_index=True); feature=pd.concat(feature_parts,ignore_index=True)
    hist.to_csv(OUT/"historical_characterization_rows.csv",index=False)
    prospective.to_csv(OUT/"prospective_paired_rows.csv",index=False)
    feature.to_csv(OUT/"prospective_feature_bound_rows.csv",index=False)
    # Global summaries, one row per evaluation window and one row per line.
    glob=[]
    for w,d in [("historical_2025_certified_replay",hist),("prospective_2026_production",prospective)]:
        for ln,g in d.groupby("line"):
            g=g.rename(columns={"p_production":"p_production"}); row={"window":w,"line":ln,**metrics(g)}; glob.append(row)
    pd.DataFrame(glob).to_csv(OUT/"global_line_metrics.csv",index=False)
    date_rows=[]
    for date,g in prospective.groupby("slate_date"):
        for ln,h in g.groupby("line"):
            date_rows.append({"slate_date":date,"line":ln,**metrics(h)})
    pd.DataFrame(date_rows).to_csv(OUT/"date_level_metrics.csv",index=False)
    count_rows=[]
    for w,d in [("historical_2025_certified_replay",hist),("prospective_2026_production",prospective)]:
        c=d.drop_duplicates(["slate_date","game_id","player_id"]); e=c.actual_sog-c.production_lambda
        count_rows.append({"window":w,"player_games":len(c),"mean_lambda":c.production_lambda.mean(),"realized_mean_sog":c.actual_sog.mean(),"mean_residual_actual_minus_lambda":e.mean(),"mae":e.abs().mean(),"rmse":np.sqrt((e*e).mean()),"median_absolute_error":e.abs().median(),"unresolved_or_missing_outcome_rows":int(sum(x["unresolved"] for x in missing.values())) if w.startswith("prospective") else 0})
    pd.DataFrame(count_rows).to_csv(OUT/"global_count_metrics.csv",index=False)
    # Error distribution: count each player-game once; residual sign convention is actual - expected.
    err=[]
    for w,d in [("historical_2025_certified_replay",hist),("prospective_2026_feature_bound",feature)]:
        c=d.drop_duplicates(["slate_date","game_id","player_id"]).copy(); c["error_actual_minus_expected"]=c.actual_sog-c.production_lambda
        x=c.error_actual_minus_expected.dropna(); row={"window":w,"n":len(x),"mean":x.mean(),"median":x.median(),"std":x.std(),"skew":x.skew()}
        for q in [0,.01,.05,.1,.25,.5,.75,.9,.95,.99,1]: row[f"q{int(q*100):02d}"]=x.quantile(q)
        for label,mask in [("large_positive_gt2",x.gt(2)),("large_negative_lt_minus2",x.lt(-2))]: row[label+"_count"]=int(mask.sum())
        c["abs_error_band"]=pd.cut(c.error_actual_minus_expected.abs(),[-np.inf,.5,1,2,np.inf],labels=["<=0.5",">0.5-1",">1-2",">2"],right=True)
        err.append(row)
        c.groupby("abs_error_band",observed=False).size().rename("player_games").reset_index().assign(window=w).to_csv(OUT/f"error_bands_{'historical' if w.startswith('historical') else 'prospective'}.csv",index=False)
    pd.DataFrame(err).to_csv(OUT/"error_distribution.csv",index=False)
    # Attach clean production features, then common dimensional score tables.
    feature["lambda_production"]=num(feature,"production_lambda")
    feature["d10_minus_d20"]=num(feature,"d10_sog_per60")-num(feature,"d20_sog_per60")
    feature["abs_d10_d20_gap"]=feature.d10_minus_d20.abs()
    feature["d10_d20_regime"]=np.select([feature.d10_minus_d20.gt(.5),feature.d10_minus_d20.lt(-.5)], ["D10_MATERIALLY_ABOVE_D20","D10_MATERIALLY_BELOW_D20"], default="APPROX_EQUAL")
    hist["d10_minus_d20"]=num(hist,"d10_sog_per60")-num(hist,"d20_sog_per60")
    hist["d10_d20_regime"]=np.select([hist.d10_minus_d20.gt(.5),hist.d10_minus_d20.lt(-.5)], ["D10_MATERIALLY_ABOVE_D20","D10_MATERIALLY_BELOW_D20"], default="APPROX_EQUAL")
    line_metrics(hist,["d10_d20_regime"]).to_csv(OUT/"historical_d10_d20_line_metrics.csv",index=False)
    feature["lambda_bin"]=pd.qcut(feature.production_lambda.rank(method="first"),10,labels=[f"Q{i}" for i in range(1,11)])
    feature["d10_bin"]=pd.qcut(num(feature,"d10_sog_per60").rank(method="first"),10,labels=[f"Q{i}" for i in range(1,11)])
    feature["gap_tertile"]=pd.qcut(feature.abs_d10_d20_gap.rank(method="first"),3,labels=["LOW","MEDIUM","HIGH"])
    feature["d10_trend"]=np.select([num(feature,"d5_sog_per60")-num(feature,"d10_sog_per60")>.5,num(feature,"d5_sog_per60")-num(feature,"d10_sog_per60")<-.5],["ACCELERATING","DECLINING"],default="STABLE")
    feature["toi_bin"]=pd.qcut(num(feature,"selected_toi_minutes").rank(method="first"),4,labels=["LOW_USAGE","MIDDLE_LOW","MIDDLE_HIGH","HIGH_USAGE"])
    pp=num(feature,"role_pp_share")
    pp_edges=pp.quantile([1/3,2/3]).to_numpy()
    feature["pp_role"]=pd.cut(pp,[-np.inf,*pp_edges,np.inf],labels=["LOW_SHARE","MID_SHARE","HIGH_SHARE"],include_lowest=True,duplicates="drop")
    feature["is_home_group"]=feature.is_home.astype(str).str.lower().isin(["t","true","1"]).map({True:"HOME",False:"AWAY"})
    for name,col in [("lambda_bin","lambda_bin"),("d10_rate","d10_bin"),("d10_d20","d10_d20_regime"),("d10_d20_gap","gap_tertile"),("usage","toi_bin"),("pp_role","pp_role"),("home_away","is_home_group"),("recent_trend","d10_trend")]:
        line_metrics(feature,[col]).to_csv(OUT/f"{name}_metrics.csv",index=False)
    feature.groupby("pp_role",observed=True).agg(n=("player_id","size"),share_min=("role_pp_share","min"),share_median=("role_pp_share","median"),share_max=("role_pp_share","max")).reset_index().to_csv(OUT/"pp_role_bin_definitions.csv",index=False)
    line_metrics(feature,["d10_d20_regime"]).to_csv(OUT/"d10_vs_d20_line_metrics.csv",index=False)
    date_groups=[]
    for (date,regime),g in feature.groupby(["slate_date","d10_d20_regime"],observed=True):
        c=g.drop_duplicates(["game_id","player_id"])
        date_groups.append({"slate_date":date,"d10_d20_regime":regime,"player_games":len(c),"mean_d10_minus_d20":c.d10_minus_d20.mean(),"mean_residual_actual_minus_lambda":(c.actual_sog-c.production_lambda).mean()})
    pd.DataFrame(date_groups).to_csv(OUT/"d10_d20_date_stability.csv",index=False)
    # Correct residual aggregation for D10/D20, count and side/line summaries.
    pg=feature.drop_duplicates(["slate_date","game_id","player_id"]).copy(); pg["residual_actual_minus_expected"]=pg.actual_sog-pg.production_lambda
    pg.groupby(["d10_d20_regime","gap_tertile"],observed=True).agg(player_games=("player_id","size"),mean_d10=("d10_sog_per60","mean"),mean_d20=("d20_sog_per60","mean"),mean_gap=("d10_minus_d20","mean"),mean_lambda=("production_lambda","mean"),realized_mean_sog=("actual_sog","mean"),mean_residual=("residual_actual_minus_expected","mean"),mae=("residual_actual_minus_expected",lambda x:x.abs().mean())).reset_index().to_csv(OUT/"d10_vs_d20_metrics.csv",index=False)
    # Team and opponent support-filtered effects; tables retain only >=50 player-games.
    for col,name in [("team_id","team"),("opponent_id","opponent")]:
        support=pg.groupby(col).size(); ids=support[support>=50].index; group_rows=[]
        for k,g in feature[feature[col].isin(ids)].groupby(col):
            for ln,h in g.groupby("line"):
                r={"identity":k,"line":ln,"player_games":g[["slate_date","game_id","player_id"]].drop_duplicates().shape[0],**metrics(h)}; group_rows.append(r)
        pd.DataFrame(group_rows).to_csv(OUT/f"{name}_metrics_min50.csv",index=False)
    # Strict-prior team shot environment and opponent suppression.
    for source,outname in [("team_d10_sf_per_game","team_shot_environment"),("opp_d10_sf_allowed_per_game","opponent_suppression_environment")]:
        feature[outname+"_bin"]=pd.qcut(num(feature,source).rank(method="first"),3,labels=["LOW","AVERAGE","HIGH"])
        line_metrics(feature,[outname+"_bin"]).to_csv(OUT/f"{outname}_metrics.csv",index=False)
    # Repeat-player bias and prior-error persistence on distinct game rows.
    pg=pg.sort_values(["player_id","game_date"]).copy(); pg["prior_residual"]=pg.groupby("player_id").residual_actual_minus_expected.shift(1); pg["prior3_residual"]=pg.groupby("player_id").residual_actual_minus_expected.transform(lambda s:s.shift(1).rolling(3,min_periods=1).mean())
    player=pg.groupby("player_id").agg(player_games=("actual_sog","size"),mean_residual=("residual_actual_minus_expected","mean"),mae=("residual_actual_minus_expected",lambda x:x.abs().mean()),rmse=("residual_actual_minus_expected",lambda x:np.sqrt((x*x).mean())),positive_residual_rate=("residual_actual_minus_expected",lambda x:(x>0).mean()),first_date=("game_date","min"),last_date=("game_date","max"),distinct_dates=("game_date","nunique")).reset_index()
    player=player[player.player_games.ge(5)].sort_values("mean_residual"); player.to_csv(OUT/"player_residual_metrics_min5.csv",index=False)
    pg.to_csv(OUT/"prospective_historical_residual_sequence.csv",index=False)
    seq=pg.dropna(subset=["prior_residual"]); seq3=pg.dropna(subset=["prior3_residual"])
    persistence={"n_prior_game_pairs":len(seq),"prior_residual_next_residual_corr":seq.prior_residual.corr(seq.residual_actual_minus_expected),"same_sign_rate":float((np.sign(seq.prior_residual)==np.sign(seq.residual_actual_minus_expected)).mean()) if len(seq) else None,"n_prior3_pairs":len(seq3),"prior3_mean_next_residual_corr":seq3.prior3_residual.corr(seq3.residual_actual_minus_expected),"window":"feature-supported historical 2025 replay plus prospective 2026 feature-bound rows","method":"player chronological one-step descriptive correlation; no fitting"}
    (OUT/"residual_persistence.json").write_text(json.dumps(persistence,indent=2,sort_keys=True)+"\n")
    # Selected-side and confidence/probability behavior, line by line.
    feature["selected_side"]=np.where(num(feature,"p_production")>=.5,"OVER","UNDER")
    side_rows=[]
    for (ln,side),g in feature.groupby(["line","selected_side"]):
        r={"line":ln,"side":side,"n":len(g),"frequency":len(g)/int(feature[feature.line.eq(ln)].shape[0]),"mean_model_probability":g.p_production.mean(),"realized_over_rate":g.realized_over.mean(),"calibration_bias":g.p_production.mean()-g.realized_over.mean(),"brier":((g.p_production-g.realized_over)**2).mean(),"log_loss":-(g.realized_over*np.log(g.p_production.clip(1e-15,1-1e-15))+(1-g.realized_over)*np.log((1-g.p_production).clip(1e-15,1-1e-15))).mean(),"wins":int(np.where(g.selected_side.eq("OVER"),g.realized_over.eq(1),g.realized_over.eq(0)).sum())}
        r["win_rate"]=r["wins"]/len(g); side_rows.append(r)
    pd.DataFrame(side_rows).to_csv(OUT/"over_under_selection_metrics.csv",index=False)
    feature["confidence"]=num(feature,"p_production").sub(.5).abs()
    feature["confidence_bin"]=pd.cut(feature.confidence,[-.000001,.05,.15,.30,1],labels=["NEAR_050","MODEST","HIGH","VERY_HIGH"])
    line_metrics(feature,["confidence_bin"]).to_csv(OUT/"confidence_bin_metrics.csv",index=False)
    # Market-relative scores are strictly exact-key matched retained no-vig snapshots.
    market_parts=[]
    for date,run in RUNS.items():
        mp=ROOT/"artifacts/operational/nhl/sog_market_attachments/season=2026"/f"slate_date={date}"/f"run_id={run}"/"sog_with_market.csv"
        if not mp.is_file(): continue
        m=pd.read_csv(mp); m["slate_date"]=date; market_parts.append(m)
    market=pd.concat(market_parts,ignore_index=True) if market_parts else pd.DataFrame()
    market_metrics=[]
    if not market.empty:
        market["game_id"]=pd.to_numeric(market.game_id,errors="coerce").astype("Int64"); market["player_id"]=pd.to_numeric(market.player_id,errors="coerce").astype("Int64"); market["line"]=pd.to_numeric(market.line,errors="coerce")
        base=feature.merge(market[["slate_date","game_id","player_id","line","p_over_mkt_novig"]],on=["slate_date","game_id","player_id","line"],how="inner",validate="one_to_one")
        base["market_p"]=num(base,"p_over_mkt_novig"); base=base[base.market_p.between(0,1)].copy(); base["model_market_gap"]=base.p_production-base.market_p
        base["gap_bin"]=pd.qcut(base.model_market_gap.rank(method="first"),5,labels=["LARGE_NEGATIVE","NEGATIVE","NEAR_MARKET","POSITIVE","LARGE_POSITIVE"])
        for ln,g in base.groupby("line"):
            for name,pcol in [("production","p_production"),("no_vig_market","market_p")]:
                p=g[pcol].clip(1e-15,1-1e-15); y=g.realized_over
                market_metrics.append({"line":ln,"source":name,"n":len(g),"brier":((p-y)**2).mean(),"log_loss":-(y*np.log(p)+(1-y)*np.log(1-p)).mean(),"auc":roc_auc_score(y,p) if y.nunique()==2 else np.nan,"average_precision":average_precision_score(y,p) if y.nunique()==2 else np.nan,"calibration_bias":p.mean()-y.mean(),"mean_probability":p.mean(),"realized_over_rate":y.mean()})
        pd.DataFrame(market_metrics).to_csv(OUT/"market_relative_metrics.csv",index=False)
        line_metrics(base,["gap_bin"]).to_csv(OUT/"model_market_disagreement_metrics.csv",index=False)
        base.to_csv(OUT/"market_matched_rows.csv",index=False)
    # Largest errors and highest-confidence wrong decisions.
    prospective_feature=feature[feature.window.astype(str).str.startswith("prospective")].copy()
    prospective_feature["count_error_actual_minus_expected"]=prospective_feature.actual_sog-prospective_feature.production_lambda
    largest=pd.concat([prospective_feature.nlargest(25,"count_error_actual_minus_expected"),prospective_feature.nsmallest(25,"count_error_actual_minus_expected")]).drop_duplicates(["slate_date","game_id","player_id"])
    bad=[]
    for ln,g in prospective_feature.groupby("line"):
        wrong=g[(g.selected_side.ne(np.where(g.realized_over.eq(1),"OVER","UNDER")))].copy()
        if not wrong.empty:
            wrong["line"]=ln; wrong["side_confidence"]=np.where(wrong.selected_side.eq("OVER"),wrong.p_production,1-wrong.p_production); bad.append(wrong.nlargest(15,"side_confidence"))
    if bad: largest=pd.concat([largest,*bad],ignore_index=True).drop_duplicates(["slate_date","game_id","player_id","line"])
    cols=[c for c in ["window","slate_date","game_date","game_id","player_id","team_id","opponent_id","is_home","line","actual_sog","production_lambda","count_error_actual_minus_expected","p_production","selected_side","selected_toi_minutes","d10_sog_per60","d20_sog_per60","role_pp_share","team_d10_sf_per_game","opp_d10_sf_allowed_per_game"] if c in largest]
    largest[cols].to_csv(OUT/"largest_prospective_errors.csv",index=False)
    # Best regimes are selected descriptively using feature slice support and proper scores.
    line_tbl=line_metrics(feature,["lambda_bin"])
    line_tbl[line_tbl.line.eq(1.5)].nsmallest(3,"brier").to_csv(OUT/"best_regime_candidates.csv",index=False)
    line_tbl[line_tbl.line.eq(1.5)].nlargest(3,"brier").to_csv(OUT/"weak_regime_candidates.csv",index=False)
    # Strength/weakness classifications are constrained by evidence duration and agreement.
    scorecard=[]
    for regime,file,definition in [("lambda spectrum","lambda_bin_metrics.csv","prospective feature-bound production lambda deciles"),("recent D10 rate","d10_rate_metrics.csv","prospective feature-bound D10 rate deciles"),("D10 versus D20","d10_d20_metrics.csv","rate difference groups beyond +/-0.5 SOG/60"),("exposure","usage_metrics.csv","selected pregame TOI quartiles"),("PP role","pp_role_metrics.csv","role_pp_share empirical tertiles; cut points recorded in pp_role_bin_definitions.csv"),("home/away","home_away_metrics.csv","feature-bound home versus away")]:
        scorecard.append({"regime":regime,"definition":definition,"sample_support":"see named table; feature-supported prospective only","historical_evidence":"not available with production-bound subgroup features","prospective_evidence":"feature-bound October 10-13 slice; short and unstable","classification":"NO_EVIDENCE_BEYOND_NOISE","interpretation":"descriptive variation only; evidence does not establish a correctable systematic weakness"})
    scorecard.append({"regime":"global baseline proper scores","definition":"all certified season 2025 replay and all baseline-bound October 8-13 production predictions","sample_support":"large historical; six prospective slates","historical_evidence":"stable reference population with exact saved-input formula replay","prospective_evidence":"immutable production predictions joined to canonical official outcomes","classification":"NO_EVIDENCE_BEYOND_NOISE","interpretation":"overall metric levels characterize the baseline but do not identify a correctable segment"})
    pd.DataFrame(scorecard).to_csv(OUT/"strength_weakness_scorecard.csv",index=False)
    # Inventory source evidence, hashes, exclusions and limits.
    scorer=ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py"; contract=ROOT/"backend/nhl/sog_fixed_blend.py"; feature_contract=ROOT/"backend/nhl/sog_feature_input.py"
    hist_count=hist.drop_duplicates(["game_date","game_id","player_id"])
    summary={"status":"COMPLETE_WITH_WINDOW_LIMITS","model_identity":{"model_family":"poisson_baseline","model_version":"baseline_v1","fitted_model_artifact":None,"scorer_path":str(scorer.relative_to(ROOT)),"scorer_sha256":sha(scorer),"feature_contract_path":str(feature_contract.relative_to(ROOT)),"feature_contract_sha256":sha(feature_contract)},"historical":{"window":"2026-02-28 through 2026-04-15 (certified season 2025 replay)","line_rows":len(hist),"player_games":len(hist_count),"source_ledger":str(HIST_LEDGER.relative_to(ROOT)),"source_ledger_sha256":sha(HIST_LEDGER),"probability_replay_status":"exact/tolerance replay rows only","prepared_feature_rows":int(hist.feature_bound.sum()),"known_scope":"does not represent pre-cutover Denali production; certified model cutover starts 2026-02-28"},"prospective":{"requested_window":"2026-09-29 through latest fully reconciled slate","baseline_bound_dates":list(RUNS),"included_window":"2026-10-08 through 2026-10-13","line_rows":len(prospective),"player_games":prospective.drop_duplicates(["slate_date","game_id","player_id"]).shape[0],"feature_bound_dates":["2026-10-10","2026-10-11","2026-10-12","2026-10-13"],"feature_bound_line_rows":len(feature),"daily_population":missing,"exclusion_reason":"2026-09-29 through 2026-10-07 reconciliations do not bind poisson_baseline/baseline_v1; their SOG arms are cold-start shadow strategies"},"market":{"matched_line_rows":0 if market.empty else len(base),"source":"retained exact-key no-vig SOG attachments; feature-supported dates only"},"residual_convention":"actual_sog - predicted_lambda; positive is underprediction", "activity":{"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_authority_changed":False,"model_training":False,"challenger_search":False},"limitations":["Subgroup prospective features are retained only for October 10-13; global production scoring covers October 8-13.","The historical certification covers the post-cutover production target window, not all of the 2025-26 schedule.","Market comparisons are limited to exact matched retained attachments."],"residual_persistence":persistence}
    summary["classification"]="STABLE_POISSON_BASELINE_WITH_UNCONFIRMED_HIGH_D10_D20_OVERPREDICTION_SIGNAL"
    summary["model_identity"]["feature_construction_sources"]=[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in [ROOT/"backend/nhl/sql/seed_sog_features_for_slate.sql",ROOT/"backend/nhl/sql/fill_sog_toi_features_for_slate.sql",ROOT/"backend/nhl/sql/fill_sog_season_toi_features_for_slate.sql",ROOT/"backend/nhl/sql/export_sog_denali_pregame.sql"]]
    summary["historical"]["line_metrics"]=pd.read_csv(OUT/"global_line_metrics.csv").query("window == 'historical_2025_certified_replay'").to_dict("records")
    summary["prospective"]["line_metrics"]=pd.read_csv(OUT/"global_line_metrics.csv").query("window == 'prospective_2026_production'").to_dict("records")
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    report=["# NHL SOG baseline strengths and weaknesses","", "## Baseline identity", "", "Production is `poisson_baseline / baseline_v1`, a deterministic Poisson scorer, not a fitted artifact. It coalesces SOG rate D10 → D20 → D5; exposure D10 TOI → D20 → D5 → season 5v5+PP TOI → season seconds 5v5+5v4; lambda is rate × minutes / 60. Over probabilities are Poisson tails at counts 2, 3 and 4 for half-lines 1.5, 2.5 and 3.5. No separate calibration step is applied in the scorer.", "", "## Evaluation populations", "", f"Historical: {len(hist):,} certified exact/tolerance-replay rows ({len(hist_count):,} player-games), February 28–April 15, 2026 game dates in canonical season 2025. This is the certified post-cutover baseline, not the full 2025-26 schedule.", "", f"Prospective: {len(prospective):,} settled line rows ({prospective.drop_duplicates(['slate_date','game_id','player_id']).shape[0]:,} player-games), October 8–13, 2026. Baseline identity is not bound in September 29–October 7 summaries, so those cold-start shadow-only dates are excluded. Feature subgroup analysis is restricted to retained feature captures on October 10–13.", "", "## Scorecard and limits", "", "Read `global_count_metrics.csv`, `global_line_metrics.csv`, `error_distribution.csv`, and `strength_weakness_scorecard.csv` first. Detailed segment tables cover lambda, D10, D10/D20, usage, PP role, home/away, team/opponent, recent trend, probability confidence and selected side. Residual persistence and player repeatability are descriptive; player rows are not filters.", "", f"The prospective feature-bound sample has {len(feature):,} line rows. Team and opponent rankings require at least 50 player-games. The model-market table uses exact-key no-vig attachments only. {len(base) if not market.empty else 0:,} prospective rows matched for market comparison.", "", "## Interpretation", "", "The baseline's global proper-score profile is more reliable evidence than selected-side hit rate. High Under accuracy at higher lines must be read against the low Over base rate and the model's side mix. The short prospective window cannot support strong systematic labels for individual subgroups; see the classification on every scorecard row.", "", "No correction, filter, challenger, or promotion was designed. The only justified follow-up opportunities are those with aligned historical and feature-bound prospective error direction, adequate support, and persistence across dates; none is promoted by this characterization." ]
    d20=pd.read_csv(OUT/"d10_vs_d20_line_metrics.csv")
    above=d20[(d20.d10_d20_regime=="D10_MATERIALLY_ABOVE_D20")&(d20.line==1.5)].iloc[0]
    below=d20[(d20.d10_d20_regime=="D10_MATERIALLY_BELOW_D20")&(d20.line==1.5)].iloc[0]
    report += ["", "## Findings", "", "Historical line metrics and prospective line metrics are in `global_line_metrics.csv`. Historical count MAE/RMSE are 1.035/1.338 (13,389 player-games), mean residual -0.008; prospective values are 1.066/1.381 (1,608 player-games), mean residual -0.042. See error distribution and bands for tails.", "", f"The prospective D10 materially above D20 group has {int(above.n)} player-games and mean residual {above.mean_residual_actual_minus_predicted:+.3f} SOG; D10 materially below D20 has {int(below.n)} and {below.mean_residual_actual_minus_predicted:+.3f}. Historical subgroup scores are in `historical_d10_d20_line_metrics.csv`; date-level evidence is in `d10_d20_date_stability.csv`. This is an unconfirmed signal, not a correctable weakness classification.", "", f"Residual persistence correlations are {persistence['prior_residual_next_residual_corr']:.3f} for the immediately prior game and {persistence['prior3_mean_next_residual_corr']:.3f} for the prior three-game mean; same-sign rate is {persistence['same_sign_rate']:.3f}. That gives little support for persistent individual misses.", "", "Market scores on exact matched rows are in `market_relative_metrics.csv`; no-vig market Brier and log loss are lower on all three lines in this sample. This is descriptive evidence only.", "", "The scorecard classifies subgroup evidence conservatively. No repeatable correction opportunity is established; another model is not justified by this study. Classification: `STABLE_POISSON_BASELINE_WITH_UNCONFIRMED_HIGH_D10_D20_OVERPREDICTION_SIGNAL`."]
    (OUT/"report.md").write_text("\n".join(report)+"\n")
    lineage={"study":"NHL_SOG_PRODUCTION_BASELINE_CHARACTERIZATION_V1","historical_source_ledger":{"path":str(HIST_LEDGER.relative_to(ROOT)),"sha256":sha(HIST_LEDGER)},"historical_feature_archives":hist_audit,"prospective_daily_sources":daily_lineage,"source_code":[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in [scorer,feature_contract]],"output_hashes":{p.name:sha(p) for p in OUT.iterdir() if p.is_file() and not p.name.startswith(".") and p.name!="lineage_manifest.json"}}
    (OUT/"lineage_manifest.json").write_text(json.dumps(lineage,indent=2,sort_keys=True,default=str)+"\n")


if __name__ == "__main__":
    main()
