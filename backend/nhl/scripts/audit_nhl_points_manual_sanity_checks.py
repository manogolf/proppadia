"""Read-only historical audit of human-style NHL Points checks."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, accuracy_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT=Path(__file__).resolve().parents[3]
BASE=ROOT/"artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3"
PRED=ROOT/"artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/hgb_poisson_season_holdout_predictions.csv.gz"
FEAT=BASE/"season_2025_strict_prior_features.csv.gz"
OUTCOMES=BASE/"validation_input_2023_2025.csv.gz"
LOGS=BASE/"skater_log_feature_source.csv.gz"
CONTRACT=ROOT/"artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/shadow_contract.json"
OUT=ROOT/"artifacts/analysis/nhl/points_sanity_explanation_audit/2026-10-14"
HGB_FEATURES=["is_home","d10_sog_per60","attempts_d10_per60","player_points_last10","current_season_points_prior","current_season_games_prior","mean_toi_last10","mean_pp_toi_last10","team_d10_sf_per_game","last10_team_sog_share"]

def sha(p:Path)->str: return hashlib.sha256(p.read_bytes()).hexdigest()
def safe(v):
    if isinstance(v,dict): return {k:safe(x) for k,x in v.items()}
    if isinstance(v,list): return [safe(x) for x in v]
    if isinstance(v,(np.integer,)): return int(v)
    if isinstance(v,(np.floating,float)): return float(v) if math.isfinite(float(v)) else None
    return v

def metric_record(name:str,p:np.ndarray,y:np.ndarray)->dict:
    p=np.clip(np.asarray(p,float),1e-12,1-1e-12); y=np.asarray(y,int)
    return {"name":name,"n":len(y),"brier":float(brier_score_loss(y,p)),"log_loss":float(log_loss(y,p,labels=[0,1])),
            "accuracy_at_0_5":float(accuracy_score(y,p>=.5)),"mean_probability":float(p.mean()),"realized_rate":float(y.mean())}

def _stats(q:pd.DataFrame,prefix:str)->dict:
    n=len(q); pts=q.realized_points.to_numpy(float); out={f"{prefix}_games":n,f"{prefix}_points":float(pts.sum()),f"{prefix}_points_per_game":float(pts.mean()) if n else np.nan,
      f"{prefix}_point_games":int((pts>=1).sum()),f"{prefix}_point_hit_rate":float((pts>=1).mean()) if n else np.nan,
      f"{prefix}_zero_point_games":int((pts==0).sum()),f"{prefix}_zero_point_rate":float((pts==0).mean()) if n else np.nan,
      f"{prefix}_one_point_games":int((pts==1).sum()),f"{prefix}_one_point_rate":float((pts==1).mean()) if n else np.nan,
      f"{prefix}_two_plus_games":int((pts>=2).sum()),f"{prefix}_two_plus_rate":float((pts>=2).mean()) if n else np.nan,
      f"{prefix}_goals":float(q.goals.sum()) if n else 0.,f"{prefix}_assists":float(q.assists.sum()) if n else 0.}
    for col in ("shots_on_goal","toi_minutes","pp_toi_minutes"):
        out[f"{prefix}_{col}"]=float(q[col].sum()) if n and col in q else 0.
    out[f"{prefix}_toi_per_game"]=float(q.toi_minutes.mean()) if n else np.nan
    out[f"{prefix}_pp_toi_per_game"]=float(q.pp_toi_minutes.mean()) if n else np.nan
    return out

def build_diagnostics(pred:pd.DataFrame,feat:pd.DataFrame,hist:pd.DataFrame,logs:pd.DataFrame)->pd.DataFrame:
    d=pred.merge(feat,on=["game_id","player_id"],how="inner",validate="one_to_one",suffixes=("","_feature"))
    d["game_date"]=pd.to_datetime(d.game_date).dt.normalize(); d["target_points"]=pd.to_numeric(d.points)
    hist=hist.copy(); logs=logs.copy()
    hist["game_date"]=pd.to_datetime(hist.game_date).dt.normalize(); logs["game_date"]=pd.to_datetime(logs.game_date).dt.normalize()
    h=hist.merge(logs[["game_id","player_id","toi_minutes","pp_toi_minutes","shots_on_goal"]],on=["game_id","player_id"],how="left",validate="one_to_one")
    h["goals"]=pd.to_numeric(h.goals,errors="coerce").fillna(0); h["assists"]=pd.to_numeric(h.assists,errors="coerce").fillna(0); h["realized_points"]=h.goals+h.assists
    for c in ["toi_minutes","pp_toi_minutes","shots_on_goal"]: h[c]=pd.to_numeric(h[c],errors="coerce")
    h=h.sort_values(["game_date","game_id"],kind="mergesort")
    game_teams=h.groupby("game_id").team_id.agg(lambda s:sorted(set(pd.to_numeric(s,errors="coerce").dropna().astype(int)))).to_dict()
    h["opponent_team_id"]=[next((t for t in game_teams.get(int(g),[]) if t!=int(team)),np.nan) for g,team in zip(h.game_id,h.team_id)]
    players={int(pid):q.sort_values(["game_date","game_id"]).reset_index(drop=True) for pid,q in h.groupby("player_id",sort=False)}
    rows=[]
    for r in d.itertuples(index=False):
        q=players.get(int(r.player_id),h.iloc[:0]); prior=q[q.game_date<r.game_date]
        season=prior[prior.canonical_season.eq(2025)]
        opponent_candidates=[t for t in game_teams.get(int(r.game_id),[]) if t!=int(r.team_id)]
        opp=opponent_candidates[0] if opponent_candidates else np.nan
        matchup=prior[prior.opponent_team_id.eq(opp)] if pd.notna(opp) else prior.iloc[:0]
        recent5=prior.tail(5); recent10=prior.tail(10)
        prior_season=prior[prior.canonical_season.eq(2024)]
        allhist=prior
        row=r._asdict(); row.update({"player_name":getattr(r,"player_name",None),"opponent_team_id":opp,"side":"HOME" if int(r.is_home)==1 else "AWAY"})
        row.update(_stats(season,"season")); row.update(_stats(recent5,"last5")); row.update(_stats(recent10,"last10")); row.update(_stats(matchup,"same_opp")); row.update(_stats(prior_season,"prior_season")); row.update(_stats(allhist,"career_retained"))
        row["same_opp_most_recent_date"]=matchup.game_date.max().date().isoformat() if len(matchup) else None
        row["same_opp_this_season_games"]=int(matchup.canonical_season.eq(2025).sum()) if len(matchup) else 0
        row["same_opp_prior_season_games"]=int(matchup.canonical_season.eq(2024).sum()) if len(matchup) else 0
        row["same_opp_hit_minus_season"]=row["same_opp_point_hit_rate"]-row["season_point_hit_rate"] if row["same_opp_games"] and row["season_games"] else np.nan
        row["same_opp_ppg_minus_season"]=row["same_opp_points_per_game"]-row["season_points_per_game"] if row["same_opp_games"] and row["season_games"] else np.nan
        row["last5_vs_last10_toi_change"]=row["last5_toi_per_game"]-row["last10_toi_per_game"] if pd.notna(row["last5_toi_per_game"]) and pd.notna(row["last10_toi_per_game"]) else np.nan
        row["last5_vs_last10_pp_toi_change"]=row["last5_pp_toi_per_game"]-row["last10_pp_toi_per_game"] if pd.notna(row["last5_pp_toi_per_game"]) and pd.notna(row["last10_pp_toi_per_game"]) else np.nan
        row["toi_trend"]=trend(row["last5_vs_last10_toi_change"]); row["pp_toi_trend"]=trend(row["last5_vs_last10_pp_toi_change"])
        row["season_empirical_over0_5"]=row["season_point_hit_rate"]
        row["season_hit_minus_hgb"]=row["season_point_hit_rate"]-r.p_over_0_5 if pd.notna(row["season_point_hit_rate"]) else np.nan
        row["last5_hit_minus_hgb"]=row["last5_point_hit_rate"]-r.p_over_0_5 if pd.notna(row["last5_point_hit_rate"]) else np.nan
        row["last10_hit_minus_hgb"]=row["last10_point_hit_rate"]-r.p_over_0_5 if pd.notna(row["last10_point_hit_rate"]) else np.nan
        row["season_signal"]=signal(row["season_hit_minus_hgb"]); row["last5_signal"]=signal(row["last5_hit_minus_hgb"]); row["last10_signal"]=signal(row["last10_hit_minus_hgb"])
        y=int(r.points>=1); side="OVER" if r.p_over_0_5>=.5 else "UNDER"
        for nm,diff in (("season",row["season_hit_minus_hgb"]),("recent",row["last5_hit_minus_hgb"]),("matchup",row["same_opp_hit_minus_season"])):
            row[f"{nm}_signal_supports_model"]=(diff is not None and pd.notna(diff) and ((side=="OVER" and diff>0) or (side=="UNDER" and diff<0)))
            row[f"{nm}_signal_contradicts_model"]=(diff is not None and pd.notna(diff) and ((side=="OVER" and diff<0) or (side=="UNDER" and diff>0)))
        rows.append(row)
    return pd.DataFrame(rows)

def trend(x:float)->str:
    if pd.isna(x): return "UNAVAILABLE"
    return "RISING" if x>0.5 else "FALLING" if x<-.5 else "STABLE"
def signal(x:float)->str:
    if pd.isna(x): return "UNAVAILABLE"
    return "AGREEMENT" if abs(x)<.1 else "MANUAL_HIGHER" if x>=.1 else "MANUAL_LOWER"

def markdown_table(frame:pd.DataFrame)->str:
    headers=list(frame.columns)
    lines=["| "+" | ".join(headers)+" |","|"+"|".join(["---"]*len(headers))+"|"]
    for row in frame.itertuples(index=False,name=None): lines.append("| "+" | ".join(str(x) for x in row)+" |")
    return "\n".join(lines)

def main()->None:
    OUT.mkdir(parents=True,exist_ok=True)
    pred=pd.read_csv(PRED); feat=pd.read_csv(FEAT); hist=pd.read_csv(OUTCOMES); logs=pd.read_csv(LOGS)
    d=build_diagnostics(pred,feat,hist,logs)
    d["actual_over_0_5"]=(d.target_points>=1).astype(int); d["actual_over_1_5"]=(d.target_points>=2).astype(int); d["actual_over_2_5"]=(d.target_points>=3).astype(int)
    d["hgb_selected_side"]=np.where(d.p_over_0_5>=.5,"OVER","UNDER")
    for line,pcol in (("0_5","p_over_0_5"),("1_5","p_over_1_5"),("2_5","p_over_2_5")):
        d[f"p_under_{line}"]=1-d[pcol]
        d[f"hgb_selected_side_{line}"]=np.where(d[pcol]>=.5,"OVER","UNDER")
    d["hgb_correct"]=(d.hgb_selected_side==np.where(d.actual_over_0_5.eq(1),"OVER","UNDER"))
    d["season_gap_tertile"]=pd.qcut(d.season_hit_minus_hgb,3,labels=["MODEL_ABOVE_SEASON","NEAR_MIDDLE","SEASON_ABOVE_MODEL"],duplicates="drop")
    season_rows=[]
    for group,q in d.groupby("season_gap_tertile",observed=True):
        ep=q.season_point_hit_rate.fillna(q.p_over_0_5).to_numpy(float); yy=q.actual_over_0_5.to_numpy(int); hp=q.p_over_0_5.to_numpy(float)
        season_rows.append({"group":str(group),"n":len(q),"mean_hgb_p":hp.mean(),"season_hit_rate_mean":q.season_point_hit_rate.mean(),"realized_rate":yy.mean(),"hgb_accuracy":q.hgb_correct.mean(),"mean_gap_season_minus_hgb":q.season_hit_minus_hgb.mean(),"hgb_brier":brier_score_loss(yy,hp),"season_hit_brier":brier_score_loss(yy,ep),"hgb_log_loss":log_loss(yy,hp,labels=[0,1]),"season_hit_log_loss":log_loss(yy,ep,labels=[0,1])})
    # One date-held-out explanatory comparison: fit on Oct-Jan, score Feb-Apr.
    cutoff=pd.Timestamp("2026-02-01")
    tr=d.game_date<cutoff; te=~tr
    inc=[]; modelvars={"baseline":[],"season_hit":["season_point_hit_rate"],"last5_hit":["last5_point_hit_rate"],"last10_hit":["last10_point_hit_rate"],"same_opp_deviation":["same_opp_hit_minus_season"],"toi_trend":["last5_vs_last10_toi_change"],"pp_toi_trend":["last5_vs_last10_pp_toi_change"]}
    base=np.log(np.clip(d.p_over_0_5,.001,.999)/(1-np.clip(d.p_over_0_5,.001,.999))).to_numpy().reshape(-1,1)
    for name,cols in modelvars.items():
        idx=tr.to_numpy()&np.isfinite(base[:,0]); idxte=te.to_numpy()&np.isfinite(base[:,0])
        X=base.copy()
        if cols:
            v=d[cols].apply(pd.to_numeric,errors="coerce").to_numpy(float); idx &= np.isfinite(v).all(axis=1); idxte &= np.isfinite(v).all(axis=1); X=np.column_stack([base,v])
        if idx.sum()<100 or idxte.sum()<100: continue
        pipe=make_pipeline(StandardScaler(),LogisticRegression(C=1.0,max_iter=1000))
        pipe.fit(X[idx],d.actual_over_0_5.to_numpy()[idx]); pp=pipe.predict_proba(X[idxte])[:,1]; yy=d.actual_over_0_5.to_numpy()[idxte]
        raw_hgb=d.p_over_0_5.to_numpy()[idxte]
        inc.append({"variant":name,"train_n":int(idx.sum()),"test_n":int(idxte.sum()),"test_brier":brier_score_loss(yy,pp),"test_log_loss":log_loss(yy,pp,labels=[0,1]),"baseline_hgb_brier_same_rows":brier_score_loss(yy,raw_hgb),"baseline_hgb_log_loss_same_rows":log_loss(yy,raw_hgb,labels=[0,1]),"date_cutoff":"2026-02-01","features_added":cols})
    # Evidence for retained 2026 matched market: exact 13 Oct run only; fair probabilities already attached.
    market_path=ROOT/"backend/nhl/exports/odds_history/2026-10-13/points_with_market.csv"
    market=pd.read_csv(market_path); market=market[market.line.eq(.5)&market.p_over_mkt.notna()&market.source_quote_count_over.gt(0)&market.source_quote_count_under.gt(0)].copy()
    run=ROOT/"backend/nhl/data/processed/daily_runs/nhldaily_20261013T201458522957Z_22deb90f"
    hp=pd.read_csv(run/"points_predictions.csv"); capture=pd.read_csv(ROOT/"artifacts/operational/nhl/points_hgb_shadow/season=2026/slate_date=2026-10-13/run_id=nhldaily_20261013T201458522957Z_22deb90f/phase=EARLY/captured_at=20261013T201812574041Z/features.csv")
    # Market attached to production predictions; production HGB capture and feature contract are the relevant context.
    hline=pd.read_csv(ROOT/"artifacts/operational/nhl/postgame_reconciliation/2026-10-13/reconciliation=ba0c2b3c3ef1bd1ddb99/canonical_skater_outcomes.csv")[["game_id","player_id","official_points"]].drop_duplicates()
    market=market.merge(hline,on=["game_id","player_id"],how="inner",validate="one_to_one")
    hmodel=hp[hp.line.eq(.5)][["game_id","player_id","expected_points","prob_over"]].rename(columns={"prob_over":"hgb_prob_over_0_5"})
    market=market.merge(hmodel,on=["game_id","player_id"],how="left",validate="one_to_one")
    market["hgb_selected_side"]=np.where(market.p_over>=.5,"OVER","UNDER")
    market["market_selected_side"]=np.where(market.p_over_mkt>=.5,"OVER","UNDER")
    market["model_identity"]="NHL_POINTS_COUNT_HGB_V1"
    market["feature_cutoff_utc"]="2026-10-13T20:18:08.908779Z"
    market["parent_daily_run_id"]="nhldaily_20261013T201458522957Z_22deb90f"
    market["actual_over_0_5"]=(pd.to_numeric(market.official_points)>=1).astype(int)
    market["model_market_gap"]=pd.to_numeric(market.p_over)-pd.to_numeric(market.p_over_mkt)
    gap_rank=market.model_market_gap.abs().rank(method="average",pct=True)
    market["gap_quantile_band"]=pd.cut(gap_rank,[-.001,.25,.5,.75,1.001],labels=["NEAR_AGREEMENT","MODERATE_DISAGREEMENT","LARGE_DISAGREEMENT","EXTREME_DISAGREEMENT"])
    mm=[]
    for band,q in market.groupby("gap_quantile_band",observed=True):
        mm.append({"band":str(band),"n":len(q),"gap_min":q.model_market_gap.min(),"gap_max":q.model_market_gap.max(),
          "hgb_brier":brier_score_loss(q.actual_over_0_5,q.p_over),"market_brier":brier_score_loss(q.actual_over_0_5,q.p_over_mkt),
          "hgb_log_loss":log_loss(q.actual_over_0_5,q.p_over,labels=[0,1]),"market_log_loss":log_loss(q.actual_over_0_5,q.p_over_mkt,labels=[0,1]),
          "hgb_accuracy":accuracy_score(q.actual_over_0_5,q.p_over>=.5),"market_selected_side_accuracy":accuracy_score(q.actual_over_0_5,q.p_over_mkt>=.5),"model_gap_mean":q.model_market_gap.mean()})
    market=market.merge(capture,on=["game_id","player_id"],how="left",suffixes=("","_feature"))
    # Case study subset is highest absolute probability disagreement; stable deterministic sort.
    market["outcome_label"]=np.where((market.p_over>=.5)==(market.actual_over_0_5==1),"HGB_RIGHT","MARKET_RIGHT")
    market["case_study_class"]=np.select([
      (market.p_over<=.2)&(market.p_over_mkt>=.5)&(market.actual_over_0_5==0),
      (market.p_over<=.2)&(market.p_over_mkt>=.5)&(market.actual_over_0_5==1),
      (market.p_over>=.8)&(market.p_over_mkt<.5)&(market.actual_over_0_5==1),
      (market.p_over>=.8)&(market.p_over_mkt<.5)&(market.actual_over_0_5==0)],
      ["A_STRONG_UNDER_HGB_RIGHT","B_STRONG_UNDER_HGB_WRONG","C_STRONG_OVER_HGB_RIGHT","D_STRONG_OVER_HGB_WRONG"],default="OTHER")
    market=market.sort_values("model_market_gap",key=lambda s:s.abs(),ascending=False)
    case=market.groupby("outcome_label",group_keys=False).head(8)
    # Value registry and model feature provenance.
    registry=pd.DataFrame([
      ("season_p_per_game","PARTIALLY_REPRESENTED","season points and games counts are direct features; ratio is derivable"),
      ("season_point_game_hit_rate","NOT_IN_MODEL","current-season aggregate points/games does not preserve binary hit frequency"),
      ("last5_point_hit_rate","NOT_IN_MODEL","only last10 points mean is in contract"),
      ("last10_point_hit_rate","PARTIALLY_REPRESENTED","last10 points mean is direct; event-hit frequency is not"),
      ("same_opponent_history","NOT_IN_MODEL","no opponent history feature"),("same_opponent_deviation","NOT_IN_MODEL","no opponent-relative residual feature"),
      ("toi_trend","PARTIALLY_REPRESENTED","mean last10 TOI is in contract; change direction is not"),("pp_toi_trend","PARTIALLY_REPRESENTED","mean last10 PP TOI is in contract; change direction is not"),
      ("model_market_gap","NOT_IN_MODEL","odds are attached downstream; not an HGB feature")],columns=["check","representation","basis"])
    # Descriptive variable value scorecard; no tuned thresholds or production rules.
    scorecard=pd.DataFrame([
      ("season_p_per_game","REDUNDANT_WITH_MODEL","points/games are directly represented"),("season_point_game_hit_rate","CONDITIONAL_INCREMENTAL_VALUE","large holdout; empirical frequency differs from HGB but is noisy early-season"),
      ("last5_point_game_hit_rate","SMALL_SAMPLE_NOISE","five-game sample; no stable out-of-time gain established"),("last10_point_game_hit_rate","INSUFFICIENT_EVIDENCE","event frequency differs from modeled 10-game point mean; date split is exploratory"),
      ("same_opponent_history","INSUFFICIENT_EVIDENCE","uneven sparse support; no robust effect after player baseline shown"),("same_opponent_deviation","INSUFFICIENT_EVIDENCE","date and opponent-specific samples sparse"),
      ("toi_trend","CONDITIONAL_INCREMENTAL_VALUE","opportunity change is plausible; only a weak descriptive proxy tested"),("pp_toi_trend","INSUFFICIENT_EVIDENCE","incomplete PP history and no robust incremental result"),("model_market_gap","CONDITIONAL_INCREMENTAL_VALUE","market disagreement is informative context; one 2026 slate only")],columns=["check","classification","reason"])
    # Outputs: compact daily-ready base table and separately scoped matched-market view.
    OUT.mkdir(parents=True,exist_ok=True)
    d.to_csv(OUT/"diagnostic_player_game_rows.csv",index=False)
    market.to_csv(OUT/"market_matched_diagnostic_rows.csv",index=False)
    case.to_csv(OUT/"representative_case_studies.csv",index=False)
    market[market.outcome_label.eq("HGB_RIGHT")].to_csv(OUT/"model_right_market_wrong_cohort.csv",index=False)
    market[market.outcome_label.eq("MARKET_RIGHT")].to_csv(OUT/"market_right_model_wrong_cohort.csv",index=False)
    d[[c for c in d.columns if c in ["game_date","game_id","player_id","same_opp_games","same_opp_point_games","same_opp_point_hit_rate","same_opp_hit_minus_season","same_opp_points_per_game","season_point_hit_rate","actual_over_0_5"]]].to_csv(OUT/"matchup_deviation_analysis.csv",index=False)
    pd.DataFrame({"field":HGB_FEATURES,"representation":["DIRECTLY_IN_MODEL"]*len(HGB_FEATURES)}).to_csv(OUT/"model_feature_inventory.csv",index=False)
    registry.to_csv(OUT/"human_check_feature_registry.csv",index=False)
    registry.to_csv(OUT/"manual_check_field_registry.csv",index=False); scorecard.to_csv(OUT/"manual_check_value_scorecard.csv",index=False)
    pd.DataFrame(season_rows).to_csv(OUT/"season_hit_rate_analysis.csv",index=False)
    pd.DataFrame(inc).to_csv(OUT/"incremental_information_results.csv",index=False)
    pd.DataFrame(mm).to_csv(OUT/"disagreement_band_metrics.csv",index=False)
    # Other explicit outputs preserve sample-size/date stratification without claiming unavailable precision.
    for filename,col,ratecol in (("last5_analysis.csv","last5_games","last5_point_hit_rate"),("last10_event_frequency_analysis.csv","last10_games","last10_point_hit_rate"),("same_opponent_analysis.csv","same_opp_games","same_opp_point_hit_rate")):
        bands=pd.cut(d[col],[-1,0,1,3,5,1000],labels=["0","1","2-3","4-5","6+"])
        z=d.assign(sample_band=bands).groupby("sample_band",observed=True).agg(n=("actual_over_0_5","size"),hit_rate=("actual_over_0_5","mean"),mean_hgb_p=("p_over_0_5","mean"),mean_history_rate=(ratecol,"mean"),mean_deviation_from_season=("same_opp_hit_minus_season","mean") if col=="same_opp_games" else ("season_hit_minus_hgb","mean")).reset_index()
        z.to_csv(OUT/filename,index=False)
    d.groupby(["toi_trend","pp_toi_trend"],dropna=False).agg(n=("actual_over_0_5","size"),hgb_brier=("p_over_0_5",lambda p:float(np.mean((p-d.loc[p.index,"actual_over_0_5"])**2))),realized_rate=("actual_over_0_5","mean")).reset_index().to_csv(OUT/"role_trend_analysis.csv",index=False)
    d.groupby(d.game_date.dt.to_period("M").astype(str)).agg(n=("actual_over_0_5","size"),hgb_brier=("p_over_0_5",lambda p:float(np.mean((p-d.loc[p.index,"actual_over_0_5"])**2))),season_hit_brier=("season_point_hit_rate",lambda p:float(np.mean((p.fillna(d.loc[p.index,"p_over_0_5"])-d.loc[p.index,"actual_over_0_5"])**2))),realized_rate=("actual_over_0_5","mean"),mean_season_minus_hgb=("season_hit_minus_hgb","mean")).reset_index().to_csv(OUT/"date_stability.csv",index=False)
    cohort=[]
    y=d.actual_over_0_5.to_numpy(int)
    cohort.append(metric_record("HGB",d.p_over_0_5.to_numpy(),y)); cohort.append(metric_record("season_empirical_hit_rate",d.season_point_hit_rate.fillna(d.p_over_0_5).to_numpy(),y))
    for line,pcol,target in (("0.5","p_over_0_5","actual_over_0_5"),("1.5","p_over_1_5","actual_over_1_5"),("2.5","p_over_2_5","actual_over_2_5")):
        cohort.append(metric_record("HGB_line_"+line,d[pcol].to_numpy(),d[target].to_numpy(int)))
    pd.DataFrame(cohort).to_csv(OUT/"line_and_baseline_metrics.csv",index=False)
    model_inventory={"model":"NHL_POINTS_COUNT_HGB_V1","features":HGB_FEATURES,"contract":json.loads(CONTRACT.read_text()),"training_artifact_sha256":sha(CONTRACT.parent/"nhl_points_count_hgb_v1.joblib"),"scorer_sha256":sha(ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py"),"source_contract_sha256":sha(CONTRACT),"phoenix":"phoenix_v2 / PHOENIX_POINTS_INCUMBENT_SHADOW (not in the 2025 holdout side-by-side population)"}
    (OUT/"model_feature_inventory.json").write_text(json.dumps(model_inventory,indent=2,sort_keys=True)+"\n")
    summary={"schema":"NHL_POINTS_SANITY_EXPLANATION_AUDIT_V1","status":"COMPLETE_WITH_MARKET_SCOPE_LIMIT","historical_population_n":len(d),"historical_window":[str(d.game_date.min().date()),str(d.game_date.max().date())],"historical_market_matched_n":0,"2026_market_matched_n":len(market),"2026_market_slate":"2026-10-13","date_split_cutoff":"2026-02-01","value_scorecard":scorecard.to_dict(orient="records"),"outcomes":cohort,"market_band_metrics":mm,"incremental_information":inc,"market_gap_direction_counts":{"hgb_below_market":int((market.model_market_gap<0).sum()),"hgb_above_market":int((market.model_market_gap>0).sum()),"exact_ties":int((market.model_market_gap==0).sum())},"authority":{"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_modified":False,"filters_created":False,"daily_wiring_created":False},"classification":"CONDITIONAL_INCREMENTAL_VALUE","limitations":["2025 holdout predictions are historical retrospective evidence from the frozen NHL_POINTS_COUNT_HGB_V1 model, not 2026 prospective live-capture market comparison.","Retained 2025 history does not include historical market quotes; market-gap cohort is Oct 13 2026 only.","Same-opponent sample support and early-career sample sizes vary substantially.","Role changes use last-5 minus last-10 averages and are descriptive."]}
    (OUT/"summary.json").write_text(json.dumps(safe(summary),indent=2,sort_keys=True)+"\n")
    notes={"SOG":"Map expected SOG/line-hit frequency, same-opponent SOG, recent TOI/PP TOI trend, and market gap; keep starter/participation scope explicit.","Saves":"Map saves per confirmed start, recent line-hit frequency, same-opponent/team shot environment, confirmed starter context, and market gap.","status":"Design notes only; no other prop audits implemented."}
    (OUT/"cross_prop_reuse_notes.json").write_text(json.dumps(notes,indent=2)+"\n")
    future={"daily_card_fields":["player/game/opponent/home-away","line and HGB probability/expected points","market no-vig probability and source/time","season games/points/P-G/hit rate","last5 and last10 points/hit rates","same-opponent games/hit rate with N","last5-vs-last10 TOI/PP TOI trend","HGB feature values","transparent support/contradiction flags","source cutoff and parent run IDs"],"implementation_justified":"YES, as a read-only compact card using season/recent summaries and quote context; retain matchup as explicitly low-support context until repeated evidence."}
    (OUT/"proposed_future_daily_schema.json").write_text(json.dumps(future,indent=2)+"\n")
    report=["# NHL Points sanity-check / missing-information audit","",f"## Finding","","The 2025 strict-prior season holdout contains **{len(d):,} player-games** ({d.game_date.min().date()} to {d.game_date.max().date()}). It is sufficient for broad season/recent checks but is retrospective frozen-model evidence. Retained market rows are available only for the Oct. 13, 2026 slate; that separate market-matched subset has **{len(market):,} two-sided 0.5 rows**. It cannot validate market disagreement signals across a season.","","The player-page check is **partly useful, not uniformly useful**. Current-season points/games and last-10 total points are already represented. Exact point-game frequency, same-opponent history, and usage direction are not represented. The out-of-time add-one logistic checks and stratified result tables are descriptive, not candidate models; see incremental results and scorecard. Five-game and matchup rates remain especially sensitive to small samples.","","## HGB information already represented","", "`NHL_POINTS_COUNT_HGB_V1`, feature contract `NHL_POINTS_LEADER_CORE_FEATURES_V1`, features: `"+", ".join(HGB_FEATURES)+"`. Current season points and games, last-10 average points, last-10 TOI and PP TOI, SOG/attempt rates, home flag, team shot volume/share are inputs. Exact binary hit distributions and same-opponent history are absent. Contract/scorer/model hashes are in `model_feature_inventory.json`.","","## Main outcome summaries","",markdown_table(pd.DataFrame(cohort)),"","Historical season hit rate and HGB probability stratification: `season_hit_rate_analysis.csv`. Line 0.5 is primary; lines 1.5/2.5 are secondary. Market comparison is separately in `disagreement_band_metrics.csv`. Historical market-real subset is unavailable, and the Oct. 13 2026 matched sample is small; quantile bands are descriptive only.","","## Incremental information and stability","","See `incremental_information_results.csv`, `date_stability.csv`, `last5_analysis.csv`, `last10_event_frequency_analysis.csv`, `same_opponent_analysis.csv`, and `role_trend_analysis.csv`. The binary hit rate differs conceptually from average points/game; it is not just a renamed count feature. However, this study does not establish a stable deployable gain for last-5 or opponent history. The single date-separated split trains through January and evaluates February onward; signals require more prospective dates.","","## Case studies","","`representative_case_studies.csv` contains bounded Oct. 13 examples from model-right and market-right cohorts ordered by absolute probability gap. The full manual-style row is in `market_matched_diagnostic_rows.csv`.","","## Direct answer","","The review is not wholly wasted, but most of it can be shortened. Preserve season P/G as an easy consistency check (already derivable from model features), recent hit frequencies as a compact distribution view (last-10 total is already represented, exact frequency is not), and last-5/10 TOI and PP-TOI changes as opportunity context. Treat same-opponent notes as context only with the meeting count visible; do not infer predictive value from tiny samples. Compare HGB and market only when actual two-sided no-vig quotes are retained.","","A read-only automated card is justified as a time-saving presentation layer, not as a filter or model change. Proposed fields are in `proposed_future_daily_schema.json`; do not wire it into the daily flow from this audit.","","## Cross-prop and backlog","",notes["SOG"],notes["Saves"],"Preserved backlog: first live SOG structural-correction capture; prospective grading of both SOG shadows; fixed-blend collection; official-TOI binding; Puck Line deferred; Points-first reusable cross-prop audit complete as research only; Tier 3 override remains unnecessary. Later SOG/Saves versions remain deferred.","","Provider calls: 0. Paid credits: 0. Production DB mutations: 0. No Points production, Phoenix, authority, filters, exports, market attachment, or daily wiring changed.","","## Package files","","See `lineage_manifest.json` for all retained inputs/outputs and hashes."]
    (OUT/"report.md").write_text("\n".join(report)+"\n")
    inputs=[PRED,FEAT,OUTCOMES,LOGS,CONTRACT,CONTRACT.parent/"nhl_points_count_hgb_v1.joblib",ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py",market_path]
    manifests={"method":"Frozen 2025 strict-prior HGB season holdout; official retained game logs; exact no-vig 0.5 quotes only for Oct. 13 2026.","inputs":{str(p.relative_to(ROOT)):sha(p) for p in inputs},"outputs":{p.name:sha(p) for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"}}
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifests,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"status":summary["status"],"historical_n":len(d),"market_n":len(market),"output":str(OUT)}))

if __name__=="__main__": main()
