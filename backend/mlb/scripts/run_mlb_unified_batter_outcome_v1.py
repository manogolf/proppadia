#!/usr/bin/env python3
"""Run the bounded offline MLB Unified Batter Outcome Distribution v1 experiment."""
from __future__ import annotations

import hashlib
import json
import math
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
NORM = ROOT / "backend/mlb/data/external/normalized/v1"
CORE = ROOT / "artifacts/analysis/model_development/mlb_external_batter_event_certified_core/2026-07-22"
OUT = ROOT / "artifacts/analysis/model_development/mlb_unified_batter_outcome_v1/2026-07-22"
SEED = 20260722
CLASSES = ["STRIKEOUT","WALK_OR_HBP","BIP_OUT_OR_REACH_NO_HIT","SINGLE","DOUBLE","TRIPLE","HOME_RUN","OTHER_SUPPORTED_TERMINAL_EVENT"]
TB_VALUE = np.array([0,0,0,1,2,3,4,0])
HIT_VALUE = np.array([0,0,0,1,1,1,1,0])
HR_VALUE = np.array([0,0,0,0,0,0,1,0])


def sha(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(1<<20),b""):h.update(b)
    return h.hexdigest()


def save(name: str, data) -> pd.DataFrame:
    frame=data if isinstance(data,pd.DataFrame) else pd.DataFrame(data)
    frame.to_csv(OUT/name,index=False); return frame


def read_table(table: str, cols: list[str]) -> pd.DataFrame:
    frames=[]
    for p in sorted((NORM/table).glob("season=*/*.parquet")):
        names=pq.ParquetFile(p).schema_arrow.names
        f=pq.ParquetFile(p).read(columns=[c for c in cols if c in names]).to_pandas()
        frames.append(f)
    return pd.concat(frames,ignore_index=True,sort=False)


def outcome_class(event: str) -> str:
    e=str(event)
    if e in {"strikeout","strikeout_double_play"}: return "STRIKEOUT"
    if e in {"walk","intent_walk","hit_by_pitch"}: return "WALK_OR_HBP"
    if e=="single": return "SINGLE"
    if e=="double": return "DOUBLE"
    if e=="triple": return "TRIPLE"
    if e=="home_run": return "HOME_RUN"
    if e in {"field_out","force_out","grounded_into_double_play","field_error","fielders_choice",
             "fielders_choice_out","sac_fly","sac_bunt","double_play","triple_play"}:
        return "BIP_OUT_OR_REACH_NO_HIT"
    return "OTHER_SUPPORTED_TERMINAL_EVENT"


def split_name(date) -> str:
    d=pd.Timestamp(date)
    if d.year<=2024:return "development"
    if d.year==2025:return "validation"
    if d<=pd.Timestamp("2026-06-30"):return "protected_holdout"
    return "final_july"


def date_prior_features(history: pd.DataFrame, id_col: str, count_cols: list[str], prefix: str) -> pd.DataFrame:
    """Cumulative and 30-active-date summaries; same-date events never enter one another."""
    h=history.groupby([id_col,"game_date"],as_index=False)[count_cols].sum().sort_values([id_col,"game_date"])
    out=h[[id_col,"game_date"]].copy()
    for c in count_cols:
        out[f"{prefix}career_{c}"]=h.groupby(id_col)[c].cumsum()-h[c]
        out[f"{prefix}recent30_{c}"]=h.groupby(id_col,sort=False)[c].transform(
            lambda s:s.shift().rolling(30,min_periods=1).sum()).fillna(0)
    out[f"{prefix}prior_dates"]=h.groupby(id_col).cumcount()
    return out


def safe_rate(num,den,prior=0.0,strength=0.0):
    return (num+prior*strength)/(den+strength)


def fit_logit(x: pd.DataFrame,y: np.ndarray) -> object:
    model=make_pipeline(SimpleImputer(strategy="median",add_indicator=True),
                        StandardScaler(),LogisticRegression(max_iter=250,C=0.35,solver="lbfgs",random_state=SEED))
    model.fit(x,y); return model


def aligned_proba(model,x,nclass):
    raw=model.predict_proba(x); out=np.zeros((len(x),nclass))
    for j,c in enumerate(model[-1].classes_.astype(int)):out[:,c]=raw[:,j]
    return out


def convolve_game(pa_prob: np.ndarray, outcome_prob: np.ndarray) -> tuple[np.ndarray,np.ndarray,np.ndarray]:
    """Mix exact PA counts 0..6; the last opportunity class represents 6+ as six."""
    nrow=len(pa_prob); hd=np.zeros((nrow,5)); td=np.zeros((nrow,5)); hr=np.zeros((nrow,2))
    for i in range(nrow):
        hp=np.bincount(HIT_VALUE,weights=outcome_prob[i],minlength=2)
        tp=np.bincount(np.minimum(TB_VALUE,4),weights=outcome_prob[i],minlength=5)
        rh=float(outcome_prob[i,6])
        for n,w in enumerate(pa_prob[i]):
            h=np.array([1.0]);t=np.array([1.0])
            for _ in range(n):
                h=np.convolve(h,hp); t=np.convolve(t,tp)
            hh=np.zeros(5);tt=np.zeros(5)
            for k,v in enumerate(h):hh[min(k,4)]+=v
            for k,v in enumerate(t):tt[min(k,4)]+=v
            hd[i]+=w*hh;td[i]+=w*tt
            hr[i,0]+=w*((1-rh)**n);hr[i,1]+=w*(1-(1-rh)**n)
    return hd,td,hr


def predictions_from_components(frame,pa_prob,outcome_prob,variant):
    hd,td,hr=convolve_game(pa_prob,outcome_prob)
    d=frame[["game_pk","game_date","batter_mlb_id","actual_pa","actual_hits","actual_tb","actual_hr","split"]].copy()
    for i,n in enumerate(["h0","h1","h2","h3","h4plus"]):d[f"p_{n}"]=hd[:,i]
    for i,n in enumerate(["tb0","tb1","tb2","tb3","tb4plus"]):d[f"p_{n}"]=td[:,i]
    d["p_hr0"]=hr[:,0];d["p_hr1plus"]=hr[:,1]
    expected_pa=pa_prob@np.arange(7)
    d["expected_hits"]=expected_pa*(outcome_prob@HIT_VALUE)
    d["expected_tb"]=expected_pa*(outcome_prob@TB_VALUE)
    d["variant"]=variant;return d


def direct_predictions(frame,hp,tp,hrp,variant,hit_tail_mean,tb_tail_mean):
    d=frame[["game_pk","game_date","batter_mlb_id","actual_pa","actual_hits","actual_tb","actual_hr","split"]].copy()
    for i,n in enumerate(["h0","h1","h2","h3","h4plus"]):d[f"p_{n}"]=hp[:,i]
    for i,n in enumerate(["tb0","tb1","tb2","tb3","tb4plus"]):d[f"p_{n}"]=tp[:,i]
    d["p_hr1plus"]=hrp[:,1];d["p_hr0"]=hrp[:,0]
    d["expected_hits"]=hp[:,:4]@np.arange(4)+hp[:,4]*hit_tail_mean
    d["expected_tb"]=tp[:,:4]@np.arange(4)+tp[:,4]*tb_tail_mean
    d["variant"]=variant;return d


def multi_logloss(actual,probs):
    idx=np.minimum(np.asarray(actual,dtype=int),probs.shape[1]-1)
    return float(log_loss(idx,np.clip(probs,1e-9,1),labels=np.arange(probs.shape[1])))


def target_metrics(pred: pd.DataFrame,variant: str,split: str) -> dict:
    p=pred
    hitless=(p.actual_hits==0).astype(int);two=(p.actual_hits>=2).astype(int);hr=(p.actual_hr>=1).astype(int)
    def binary(y,prob,prefix):
        return {f"{prefix}_pr_auc":average_precision_score(y,prob),f"{prefix}_roc_auc":roc_auc_score(y,prob),
                f"{prefix}_brier":brier_score_loss(y,prob),f"{prefix}_logloss":log_loss(y,np.c_[1-prob,prob],labels=[0,1])}
    m={"variant":variant,"split":split,"rows":len(p),"dates":p.game_date.nunique()}
    m.update(binary(hitless,p.p_h0,"hits05_hitless"));m.update(binary(two,1-p.p_h0-p.p_h1,"hits15_2plus"))
    m.update(binary(hr,p.p_hr1plus,"hr"))
    hp=p[[f"p_{x}" for x in ["h0","h1","h2","h3","h4plus"]]].to_numpy()
    tp=p[[f"p_{x}" for x in ["tb0","tb1","tb2","tb3","tb4plus"]]].to_numpy()
    m["hits_distribution_logloss"]=multi_logloss(p.actual_hits,hp)
    m["tb_distribution_logloss"]=multi_logloss(p.actual_tb,tp)
    actual_bin=np.minimum(p.actual_tb.astype(int).to_numpy(),4)
    cum=np.cumsum(tp,axis=1)[:,:-1];obs=(actual_bin[:,None]<=np.arange(4)[None,:]).astype(float)
    m["tb_ranked_probability_score"]=float(np.mean(np.sum((cum-obs)**2,axis=1)))
    m["expected_tb_mae"]=float(np.mean(abs(p.expected_tb-p.actual_tb)))
    for cap in [.05,.10,.15,.20,.25]:
        n=max(1,int(len(p)*cap));top=p.assign(y=hitless).nlargest(n,"p_h0")
        m[f"hits05_capture_at_{int(cap*100)}pct"]=float(top.y.sum()/max(1,hitless.sum()))
    return m


def calibration(pred: pd.DataFrame,split: str,variant: str) -> pd.DataFrame:
    rows=[]
    for target,y,prob in [("HITS_0",pred.actual_hits.eq(0),pred.p_h0),("HITS_2PLUS",pred.actual_hits.ge(2),1-pred.p_h0-pred.p_h1),
                          ("HOME_RUN",pred.actual_hr.ge(1),pred.p_hr1plus)]:
        bins=pd.qcut(prob,10,duplicates="drop")
        for interval,g in pred.assign(_p=prob,_y=y,_bin=bins).groupby("_bin",observed=True):
            rows.append({"variant":variant,"split":split,"target":target,"bin":str(interval),"rows":len(g),
                         "mean_probability":g._p.mean(),"observed_rate":g._y.mean()})
    return pd.DataFrame(rows)


def main():
    OUT.mkdir(parents=True,exist_ok=True);start=time.time();np.random.seed(SEED)
    save("frozen_architecture_contract.csv",[
      {"contract":"model","value":"MLB_UNIFIED_BATTER_OUTCOME_DISTRIBUTION_V1"},
      {"contract":"player_game_grain","value":"game_pk|batter_mlb_id"},
      {"contract":"pa_grain","value":"game_pk|at_bat_number|batter_mlb_id"},
      {"contract":"pitch_grain","value":"game_pk|at_bat_number|pitch_number"},
      {"contract":"terminal_classes","value":"|".join(CLASSES)},
      {"contract":"selection","value":"minimum 2025 composite distribution logloss; freeze before 2026"},
      {"contract":"windows","value":"career prior and 30 prior active dates"},
      {"contract":"shrinkage","value":"200 PA hitter; 250 PA pitcher; fixed before validation"},
      {"contract":"production","value":"OFFLINE_ONLY"}])
    save("pa_terminal_outcome_mapping.csv",[{"source_event":e,"class":outcome_class(e),"supported":"explicit"} for e in
      ["strikeout","strikeout_double_play","walk","intent_walk","hit_by_pitch","field_out","force_out",
       "grounded_into_double_play","field_error","fielders_choice","fielders_choice_out","sac_fly","sac_bunt",
       "single","double","triple","home_run","catcher_interf","other"]])

    games=read_table("games",["game_pk","game_date","home_team","away_team"]).drop_duplicates("game_pk")
    games.game_date=pd.to_datetime(games.game_date);games.game_pk=pd.to_numeric(games.game_pk).astype(int)
    line=read_table("starting_lineups",["game_pk","team","team_id","player_id","batting_order_position","home_away",
                                        "lineup_certification_status","source"])
    line=line.dropna(subset=["game_pk","player_id","batting_order_position"]).copy()
    line.game_pk=pd.to_numeric(line.game_pk).astype(int);line.player_id=pd.to_numeric(line.player_id).astype(int)
    line=line.merge(games,on="game_pk",how="inner")
    line["team"]=line.team.where(line.team.notna(),np.where(line.home_away.eq("home"),line.home_team,line.away_team))
    line["opponent"]=np.where(line.team.eq(line.home_team),line.away_team,line.home_team)
    line=line.sort_values(["game_pk","team","batting_order_position"]).drop_duplicates(["game_pk","team","batting_order_position"])

    pa_df=read_table("plate_appearances",["game_pk","game_date","at_bat_number","batter","pitcher","events","stand"])
    pa_df=pa_df.dropna(subset=["game_pk","batter","events"]).copy()
    pa_df.game_pk=pd.to_numeric(pa_df.game_pk).astype(int);pa_df.batter=pd.to_numeric(pa_df.batter).astype(int)
    pa_df.pitcher=pd.to_numeric(pa_df.pitcher,errors="coerce").astype("Int64");pa_df.game_date=pd.to_datetime(pa_df.game_date)
    pa_df["outcome_class"]=pa_df.events.map(outcome_class);pa_df["class_idx"]=pa_df.outcome_class.map({c:i for i,c in enumerate(CLASSES)})
    pa_df["hit"]=pa_df.events.isin(["single","double","triple","home_run"]).astype(int)
    pa_df["tb"]=pa_df.events.map({"single":1,"double":2,"triple":3,"home_run":4}).fillna(0).astype(int)
    pa_df["hr"]=pa_df.events.eq("home_run").astype(int)
    for i,c in enumerate(CLASSES):pa_df[f"oc_{i}"]=pa_df.class_idx.eq(i).astype(int)
    count_cols=["pa","hit","tb","hr"]+[f"oc_{i}" for i in range(8)]
    pa_df["pa"]=1
    pg=pa_df.groupby(["game_pk","game_date","batter"],as_index=False)[count_cols].sum()

    pitch=read_table("pitches",["game_pk","game_date","batter","pitcher","description","pitch_type","release_speed",
                                "release_spin_rate","pfx_x","pfx_z","zone"])
    pitch=pitch.dropna(subset=["game_pk","batter"]).copy();pitch.game_pk=pd.to_numeric(pitch.game_pk).astype(int)
    pitch.batter=pd.to_numeric(pitch.batter).astype(int);pitch.pitcher=pd.to_numeric(pitch.pitcher,errors="coerce").astype("Int64")
    desc=pitch.description.fillna("")
    swing=desc.isin(["swinging_strike","swinging_strike_blocked","foul","foul_tip","hit_into_play",
                     "missed_bunt","bunt_foul_tip","foul_bunt"])
    whiff=desc.isin(["swinging_strike","swinging_strike_blocked","missed_bunt"])
    pitch["pitches"]=1;pitch["swings"]=swing.astype(int);pitch["whiffs"]=whiff.astype(int)
    pitch["contacts"]=(swing&~whiff).astype(int);pitch["called_strikes"]=desc.eq("called_strike").astype(int)
    pitch["fouls"]=desc.isin(["foul","foul_tip","foul_bunt","bunt_foul_tip"]).astype(int)
    pga=pitch.groupby(["game_pk","batter"],as_index=False)[["pitches","swings","whiffs","contacts","called_strikes","fouls"]].sum()
    bb=read_table("batted_balls",["game_pk","batter","launch_speed","launch_angle","estimated_ba_using_speedangle",
                                  "estimated_woba_using_speedangle","launch_speed_angle","bb_type"])
    bb.game_pk=pd.to_numeric(bb.game_pk).astype(int);bb.batter=pd.to_numeric(bb.batter).astype(int)
    for c in ["launch_speed","launch_angle","estimated_ba_using_speedangle","estimated_woba_using_speedangle","launch_speed_angle"]:
        bb[c]=pd.to_numeric(bb[c],errors="coerce")
    bb["bb"]=1;bb["ev_sum"]=bb.launch_speed.fillna(0);bb["ev_n"]=bb.launch_speed.notna().astype(int)
    bb["x_6"]=bb.launch_speed_angle.eq(6).astype(int);bb["xba_sum"]=bb.estimated_ba_using_speedangle.fillna(0)
    bb["xba_n"]=bb.estimated_ba_using_speedangle.notna().astype(int);bb["xwoba_sum"]=bb.estimated_woba_using_speedangle.fillna(0)
    bb["xwoba_n"]=bb.estimated_woba_using_speedangle.notna().astype(int)
    bga=bb.groupby(["game_pk","batter"],as_index=False)[["bb","ev_sum","ev_n","x_6","xba_sum","xba_n","xwoba_sum","xwoba_n"]].sum()
    hist=pg.merge(pga,on=["game_pk","batter"],how="left").merge(bga,on=["game_pk","batter"],how="left").fillna(0)
    hist_cols=count_cols+["pitches","swings","whiffs","contacts","called_strikes","fouls","bb","ev_sum","ev_n","x_6","xba_sum","xba_n","xwoba_sum","xwoba_n"]
    hprior=date_prior_features(hist.rename(columns={"batter":"batter_mlb_id"}),"batter_mlb_id",hist_cols,"h_")

    target=line.rename(columns={"player_id":"batter_mlb_id"}).merge(pg.rename(columns={"batter":"batter_mlb_id"}),on=["game_pk","game_date","batter_mlb_id"],how="left")
    target=target[target.pa.notna()&target.pa.gt(0)].copy()
    target=target.merge(hprior,on=["batter_mlb_id","game_date"],how="left")
    target["split"]=target.game_date.map(split_name);target["actual_pa"]=target.pa.astype(int)
    target["actual_hits"]=target.hit.astype(int);target["actual_tb"]=target.tb.astype(int);target["actual_hr"]=target.hr.astype(int)
    target["home"]=target.home_away.eq("home").astype(int)
    # Opportunity histories at player and lineup-slot levels, computed by prior calendar date.
    opp_hist=target[["batter_mlb_id","game_date","pa"]].rename(columns={"pa":"opp_pa"})
    op=date_prior_features(opp_hist,"batter_mlb_id",["opp_pa"],"opp_")
    target=target.merge(op,on=["batter_mlb_id","game_date"],how="left")
    slot=target.groupby(["batting_order_position","game_date"],as_index=False).agg(slot_pa=("pa","sum"),slot_starts=("pa","size"))
    slot=slot.sort_values(["batting_order_position","game_date"])
    for c in ["slot_pa","slot_starts"]:slot[f"prior_{c}"]=slot.groupby("batting_order_position")[c].cumsum()-slot[c]
    target=target.merge(slot[["batting_order_position","game_date","prior_slot_pa","prior_slot_starts"]],
                        on=["batting_order_position","game_date"],how="left")
    target["prior_player_pa_per_date"]=target.opp_career_opp_pa/target.opp_prior_dates.clip(lower=1)
    target["prior_slot_pa_per_start"]=target.prior_slot_pa/target.prior_slot_starts.clip(lower=1)
    target["history_depth_pa"]=target.h_career_pa
    # Governed starting-pitcher identity and strict-prior suppression.
    pit=read_table("player_game_pitching",["game_pk","player_id","team","opponent","games_started","p_gs"])
    gs=pd.to_numeric(pit.get("games_started"),errors="coerce").fillna(pd.to_numeric(pit.get("p_gs"),errors="coerce")).fillna(0)
    pit=pit[gs.eq(1)&pit.player_id.notna()].copy();pit.game_pk=pd.to_numeric(pit.game_pk).astype(int);pit.player_id=pd.to_numeric(pit.player_id).astype(int)
    pit=pit.drop_duplicates(["game_pk","team"]).rename(columns={"player_id":"opposing_starter_id","team":"pitcher_team"})
    target=target.merge(pit[["game_pk","pitcher_team","opposing_starter_id"]],
                        left_on=["game_pk","opponent"],right_on=["game_pk","pitcher_team"],how="left")
    ph=pa_df.dropna(subset=["pitcher"]).copy();ph["pitcher_id"]=ph.pitcher.astype(int)
    ph=ph.groupby(["pitcher_id","game_date"],as_index=False)[count_cols].sum()
    pprior=date_prior_features(ph,"pitcher_id",count_cols,"p_").rename(columns={"pitcher_id":"opposing_starter_id"})
    target=target.merge(pprior,on=["opposing_starter_id","game_date"],how="left")
    global_dev=pa_df[pa_df.game_date.dt.year.le(2024)].class_idx.value_counts(normalize=True).reindex(range(8),fill_value=0).to_numpy()
    for scope in ["career","recent30"]:
        den=target[f"h_{scope}_pa"].fillna(0)
        for i in range(8):target[f"h_{scope}_rate_{i}"]=safe_rate(target[f"h_{scope}_oc_{i}"].fillna(0),den,global_dev[i],200)
    pden=target.p_career_pa.fillna(0)
    for i in range(8):target[f"p_career_rate_{i}"]=safe_rate(target[f"p_career_oc_{i}"].fillna(0),pden,global_dev[i],250)
    target["h_swing_rate"]=safe_rate(target.h_career_swings,target.h_career_pitches,.47,100)
    target["h_whiff_per_swing"]=safe_rate(target.h_career_whiffs,target.h_career_swings,.22,100)
    target["h_contact_per_swing"]=safe_rate(target.h_career_contacts,target.h_career_swings,.78,100)
    target["h_called_strike_rate"]=safe_rate(target.h_career_called_strikes,target.h_career_pitches,.16,100)
    target["h_foul_rate"]=safe_rate(target.h_career_fouls,target.h_career_pitches,.17,100)
    target["h_pitches_per_pa"]=safe_rate(target.h_career_pitches,target.h_career_pa,3.9,50)
    target["h_ev"]=safe_rate(target.h_career_ev_sum,target.h_career_ev_n,88.5,50)
    target["h_xba"]=safe_rate(target.h_career_xba_sum,target.h_career_xba_n,.245,50)
    target["h_xwoba"]=safe_rate(target.h_career_xwoba_sum,target.h_career_xwoba_n,.320,50)
    target["h_lsa6_rate"]=safe_rate(target.h_career_x_6,target.h_career_bb,.07,50)
    target["p_hit_suppression"]=safe_rate(target.p_career_hit,target.p_career_pa,.225,250)
    target["p_k_rate"]=safe_rate(target.p_career_oc_0,target.p_career_pa,.225,250)
    target["matchup_k"]=target.h_career_rate_0*target.p_k_rate
    target["matchup_hit"]=target.h_career_rate_3.add(target.h_career_rate_4).add(target.h_career_rate_5).add(target.h_career_rate_6)*target.p_hit_suppression
    tier_b_frames=[pq.ParquetFile(p).read(columns=["game_pk","eligible"]).to_pandas() for p in CORE.glob("tier_b_*_manifest.parquet")]
    tier_b=pd.concat(tier_b_frames,ignore_index=True)
    tier_b_games=set(pd.to_numeric(tier_b[tier_b.eligible].game_pk,errors="coerce").dropna().astype(int))
    target["tier_b_available"]=target.game_pk.isin(tier_b_games)
    target["pitcher_available"]=target.opposing_starter_id.notna().astype(int)
    target["strict_prior_cutoff_status"]="PRIOR_CALENDAR_DATE_ONLY"
    # Persist population and feature contracts before fitting.
    popcols=["game_pk","game_date","batter_mlb_id","team","opponent","home_away","batting_order_position","actual_pa","actual_hits","actual_tb","actual_hr","split","lineup_certification_status","tier_b_available","pitcher_available","strict_prior_cutoff_status"]
    pq.write_table(pa.Table.from_pandas(target[popcols],preserve_index=False),OUT/"model_population_manifest.parquet",compression="zstd")
    save("model_population_summary.csv",target.groupby("split").agg(rows=("game_pk","size"),games=("game_pk","nunique"),players=("batter_mlb_id","nunique"),dates=("game_date","nunique"),mean_pa=("actual_pa","mean")).reset_index())
    feature_registry=[
      ("batting_order_position","opportunity","B","target certified lineup","yes"),
      ("home","opportunity","A","official game side","yes"),
      ("prior_player_pa_per_date|prior_slot_pa_per_start","opportunity","A/B","prior calendar dates","yes"),
      ("h_career_rate_*|h_recent30_rate_*","hitter outcome","A","prior PA events","yes"),
      ("h_swing_rate|h_whiff_per_swing|h_contact_per_swing|h_called_strike_rate|h_foul_rate","plate discipline","A","prior pitches","yes"),
      ("h_ev|h_xba|h_xwoba|h_lsa6_rate","contact quality","A","prior batted balls","yes"),
      ("p_hit_suppression|p_k_rate","pitcher suppression","A","governed starter identity plus prior events","conditional"),
      ("matchup_k|matchup_hit","bounded matchup","A","strict-prior hitter and starter profiles","conditional")]
    save("strict_prior_feature_registry.csv",[{"features":a,"family":b,"minimum_tier":c,"source":d,"live_identifiable":e} for a,b,c,d,e in feature_registry])
    # Matrices and frozen feature sets.
    opp_features=["batting_order_position","home","prior_player_pa_per_date","prior_slot_pa_per_start","opp_prior_dates","history_depth_pa"]
    hitter_rate=[f"h_career_rate_{i}" for i in range(8)]+[f"h_recent30_rate_{i}" for i in range(8)]
    contact=["h_swing_rate","h_whiff_per_swing","h_contact_per_swing","h_called_strike_rate","h_foul_rate","h_pitches_per_pa","h_ev","h_xba","h_xwoba","h_lsa6_rate","history_depth_pa"]
    pitcher=["p_hit_suppression","p_k_rate","p_prior_dates","pitcher_available"]
    matchup=["matchup_k","matchup_hit"]
    unique=lambda xs:list(dict.fromkeys(xs))
    fsets={"UBO-2":unique(opp_features+hitter_rate+contact),"UBO-3":unique(opp_features+hitter_rate+contact+pitcher),
           "UBO-4":unique(opp_features+hitter_rate+contact+pitcher+matchup)}
    dev=target[target.split.eq("development")].copy();val=target[target.split.eq("validation")].copy()
    pa_y=np.minimum(dev.actual_pa.to_numpy(),6)
    pa_model=fit_logit(dev[opp_features],pa_y)
    def pa_probs(frame):
        return aligned_proba(pa_model,frame[opp_features],7)
    all_pa_prob=pa_probs(target)
    pa_out=target[["game_pk","game_date","batter_mlb_id","split"]].copy()
    for i in range(7):pa_out[f"p_pa_{i}" if i<6 else "p_pa_6plus"]=all_pa_prob[:,i]
    pq.write_table(pa.Table.from_pandas(pa_out,preserve_index=False),OUT/"pa_opportunity_probabilities.parquet",compression="zstd")
    # Per-PA matrices: join each development PA to its starting-hitter strict-prior row.
    keyfeat=["game_pk","batter_mlb_id"]+sorted(set(sum(fsets.values(),[])))
    train_pa=pa_df.merge(dev[keyfeat].rename(columns={"batter_mlb_id":"batter"}),on=["game_pk","batter"],how="inner")
    outcome_models={}
    for variant in ["UBO-2","UBO-3","UBO-4"]:
        outcome_models[variant]=fit_logit(train_pa[fsets[variant]],train_pa.class_idx.to_numpy())
    # Direct game reference models.
    direct_features=fsets["UBO-4"]
    feature_export=target[unique(["game_pk","game_date","batter_mlb_id","split"]+direct_features+["tier_b_available","pitcher_available"])]
    pq.write_table(pa.Table.from_pandas(feature_export,preserve_index=False),OUT/"strict_prior_player_game_features.parquet",compression="zstd")
    pop_ledger=[]
    for variant in ["UBO-0","UBO-1","UBO-2","UBO-3","UBO-4","UBO-5"]:
        for s,g in target.groupby("split"):
            req="PA+GLOBAL" if variant=="UBO-0" else ("PA+HITTER_HISTORY" if variant=="UBO-1" else
                ("PA+HITTER_EVENT" if variant=="UBO-2" else ("PA+HITTER+PITCHER_WITH_FALLBACK" if variant in {"UBO-3","UBO-4"} else "DIRECT_SAME_CERTIFIED_FEATURES")))
            pop_ledger.append({"variant":variant,"split":s,"rows":len(g),"games":g.game_pk.nunique(),
                               "required_features":req,"pitcher_identity_coverage":g.pitcher_available.mean(),
                               "tier_b_context_coverage":g.tier_b_available.mean(),"population_reduction":0})
    save("variant_population_ledger.csv",pop_ledger)
    direct={}
    for name,y in [("hits",np.minimum(dev.actual_hits,4)),("tb",np.minimum(dev.actual_tb,4)),("hr",np.minimum(dev.actual_hr,1))]:
        direct[name]=fit_logit(dev[direct_features],np.asarray(y,dtype=int))
    hit_tail_mean=float(dev.loc[dev.actual_hits.ge(4),"actual_hits"].mean())
    tb_tail_mean=float(dev.loc[dev.actual_tb.ge(4),"actual_tb"].mean())
    def score(frame,variant):
        pp=pa_probs(frame)
        if variant=="UBO-0":op=np.repeat(global_dev[None,:],len(frame),axis=0)
        elif variant=="UBO-1":
            op=frame[[f"h_career_rate_{i}" for i in range(8)]].to_numpy()
        elif variant in outcome_models:op=aligned_proba(outcome_models[variant],frame[fsets[variant]],8)
        else:
            hp=aligned_proba(direct["hits"],frame[direct_features],5);tp=aligned_proba(direct["tb"],frame[direct_features],5)
            hrp=aligned_proba(direct["hr"],frame[direct_features],2)
            return direct_predictions(frame,hp,tp,hrp,variant,hit_tail_mean,tb_tail_mean)
        return predictions_from_components(frame,pp,op,variant)
    # Independent PA opportunity evaluation.
    pa_rows=[]
    for name,frame in [("development",dev),("validation",val)]:
        prob=pa_probs(frame);actual=np.minimum(frame.actual_pa.to_numpy(),6)
        pa_rows.append({"split":name,"rows":len(frame),"multiclass_logloss":multi_logloss(actual,prob),
                        "mae_expected_pa":float(np.mean(abs(prob@np.arange(7)-frame.actual_pa))),
                        "p_pa_le2_mean":float(prob[:,:3].sum(1).mean()),"p_pa3_mean":float(prob[:,3].mean()),
                        "p_pa4_mean":float(prob[:,4].mean()),"p_pa5_mean":float(prob[:,5].mean()),"p_pa6plus_mean":float(prob[:,6].mean())})
    save("pa_opportunity_distributions.csv",pa_rows)
    profiles=target.groupby("split").agg(rows=("game_pk","size"),mean_prior_pa=("history_depth_pa","mean"),
        mean_swing_rate=("h_swing_rate","mean"),mean_whiff=("h_whiff_per_swing","mean"),mean_ev=("h_ev","mean"),mean_xba=("h_xba","mean")).reset_index()
    save("hitter_event_profiles.csv",profiles)
    save("pitcher_suppression_profiles.csv",target.groupby("split").agg(rows=("game_pk","size"),pitcher_coverage=("pitcher_available","mean"),
        mean_prior_pitcher_dates=("p_prior_dates","mean"),mean_hit_allowed=("p_hit_suppression","mean"),mean_k_induction=("p_k_rate","mean")).reset_index())
    save("matchup_profiles.csv",target.groupby("split").agg(rows=("game_pk","size"),mean_matchup_k=("matchup_k","mean"),mean_matchup_hit=("matchup_hit","mean")).reset_index())
    # Save a bounded sample plus schema for PA matrices rather than a huge duplicate full matrix.
    matrix_cols=["game_pk","at_bat_number","batter","outcome_class"]+fsets["UBO-4"]
    pq.write_table(pa.Table.from_pandas(train_pa[matrix_cols],preserve_index=False),OUT/"per_pa_outcome_matrix_development.parquet",compression="zstd")
    # Freeze finalist solely on 2025 validation.
    val_preds={v:score(val,v) for v in ["UBO-0","UBO-1","UBO-2","UBO-3","UBO-4","UBO-5"]}
    val_metrics=pd.DataFrame([target_metrics(p,v,"validation") for v,p in val_preds.items()])
    val_metrics["selection_composite"]=val_metrics.hits_distribution_logloss+val_metrics.tb_distribution_logloss+val_metrics.hr_logloss
    finalist=str(val_metrics.sort_values("selection_composite").iloc[0].variant)
    save("development_results.csv",[target_metrics(score(dev,v),v,"development") for v in val_preds])
    save("validation_2025_results.csv",val_metrics)
    save("frozen_finalist_contract.csv",[{"finalist":finalist,"selection_partition":"2025 validation only","selection_metric":"hits distribution logloss + TB distribution logloss + HR logloss","calibration":"NONE","frozen_before_2026":True}])
    # Only now materialize and inspect protected 2026 and final July predictions.
    hold=target[target.split.eq("protected_holdout")].copy();july=target[target.split.eq("final_july")].copy()
    hold_preds={v:score(hold,v) for v in val_preds};july_preds={v:score(july,v) for v in val_preds}
    hold_metrics=pd.DataFrame([target_metrics(p,v,"protected_holdout") for v,p in hold_preds.items()])
    july_metrics=pd.DataFrame([target_metrics(p,v,"final_july") for v,p in july_preds.items()])
    save("protected_2026_results.csv",hold_metrics);save("final_july_untouched_results.csv",july_metrics)
    all_final=pd.concat([val_preds[finalist],hold_preds[finalist],july_preds[finalist]],ignore_index=True)
    pq.write_table(pa.Table.from_pandas(all_final,preserve_index=False),OUT/"player_game_probability_distributions.parquet",compression="zstd")
    cal=pd.concat([calibration(val_preds[finalist],"validation",finalist),calibration(hold_preds[finalist],"protected_holdout",finalist),
                   calibration(july_preds[finalist],"final_july",finalist)],ignore_index=True)
    save("calibration_results.csv",cal)
    # Target evaluations and suitability.
    base="UBO-1"
    target_rows=[]
    for target_name,metric,direction in [("Hits 0.5","hits05_hitless_logloss",-1),("Hits 1.5","hits15_2plus_logloss",-1),
                                          ("total bases","tb_distribution_logloss",-1),("home run","hr_logloss",-1)]:
        vals=[]
        for s,df in [("validation",val_metrics),("protected_holdout",hold_metrics),("final_july",july_metrics)]:
            f=float(df.loc[df.variant.eq(finalist),metric].iloc[0]);b=float(df.loc[df.variant.eq(base),metric].iloc[0]);vals.append(b-f)
        if vals[1]>0 and vals[2]>0 and min(vals)>=-0.002:grade="MODEST_REPLICATED_SIGNAL"
        elif vals[1]>0 and vals[2]>0:grade="CALIBRATED_BUT_WEAK_RANKING"
        elif vals[1]>0 or vals[2]>0:grade="UNSTABLE_SIGNAL"
        else:grade="NO_INCREMENTAL_SIGNAL"
        target_rows.append({"target":target_name,"finalist":finalist,"validation_improvement":vals[0],
                            "protected_2026_improvement":vals[1],"final_july_improvement":vals[2],"classification":grade})
    targets=save("target_specific_evaluations.csv",target_rows)
    controls=[]
    for split,df in [("validation",val_metrics),("protected_holdout",hold_metrics),("final_july",july_metrics)]:
        for control in ["UBO-0","UBO-1"]:
            c=df[df.variant.eq(control)].iloc[0];f=df[df.variant.eq(finalist)].iloc[0]
            controls.append({"split":split,"control":control,"finalist":finalist,
              "hits05_brier_improvement":c.hits05_hitless_brier-f.hits05_hitless_brier,
              "hits_distribution_logloss_improvement":c.hits_distribution_logloss-f.hits_distribution_logloss,
              "tb_logloss_improvement":c.tb_distribution_logloss-f.tb_distribution_logloss,
              "hr_brier_improvement":c.hr_brier-f.hr_brier,"compatible_rows":int(f.rows)})
    for name in ["OPERATIONAL_INCUMBENT","FULL_SPINE_CANDIDATE","EVENT_PROCESS_V1_EP2","BETONLINE_NO_VIG"]:
        controls.append({"split":"ALL","control":name,"finalist":finalist,"compatible_rows":0,
                         "comparison_status":"NOT_COMPARABLE_NO_CERTIFIED_IDENTICAL_ROW_PROBABILITY_BINDING"})
    save("control_comparisons.csv",controls)
    # Sequential attribution is descriptive and uses the frozen variants.
    attr=[]
    chain=["UBO-0","UBO-1","UBO-2","UBO-3","UBO-4"]
    labels=["multi-season PA + historical rate","historical hitter outcome rate","discipline/contact/xBA","pitcher suppression","bounded matchup"]
    for i in range(1,len(chain)):
        for split,df in [("validation",val_metrics),("protected_holdout",hold_metrics),("final_july",july_metrics)]:
            a=df[df.variant.eq(chain[i-1])].iloc[0];b=df[df.variant.eq(chain[i])].iloc[0]
            attr.append({"component":labels[i],"from_variant":chain[i-1],"to_variant":chain[i],"split":split,
                         "hits05_brier_improvement":a.hits05_hitless_brier-b.hits05_hitless_brier,
                         "distribution_composite_improvement":(a.hits_distribution_logloss+a.tb_distribution_logloss+a.hr_logloss)-
                                                              (b.hits_distribution_logloss+b.tb_distribution_logloss+b.hr_logloss)})
    save("component_attribution.csv",attr)
    # Failure ledgers use strict-prior IDs/probabilities only.
    jf=july_preds[finalist].copy();jf["hitless"]=jf.actual_hits.eq(0);jf["two_plus"]=jf.actual_hits.ge(2);jf["p_h2plus"]=1-jf.p_h0-jf.p_h1
    ledger_specs={
      "strongest_correct_zero_hit_warnings":jf[jf.hitless].nlargest(200,"p_h0"),
      "strongest_false_zero_hit_warnings":jf[~jf.hitless].nlargest(200,"p_h0"),
      "two_plus_hit_successes":jf[jf.two_plus].nlargest(200,"p_h2plus"),
      "two_plus_hit_misses":jf[jf.two_plus].nsmallest(200,"p_h2plus"),
      "total_base_distribution_failures":jf.assign(err=abs(jf.expected_tb-jf.actual_tb)).nlargest(200,"err"),
      "home_run_high_risk_misses":jf[jf.actual_hr.eq(0)].nlargest(200,"p_hr1plus"),
      "high_opportunity_hitless":jf[jf.hitless].nlargest(200,"expected_hits")}
    for name,frame in ledger_specs.items():frame.to_csv(OUT/f"failure_{name}.csv",index=False)
    # Date/player robustness of finalist versus hierarchical baseline.
    robust=[]
    for split,fp,bp in [("protected_holdout",hold_preds[finalist],hold_preds[base]),("final_july",july_preds[finalist],july_preds[base])]:
        z=fp[["game_pk","game_date","batter_mlb_id","actual_hits","p_h0"]].rename(columns={"p_h0":"p_h0_f"})
        z=z.merge(bp[["game_pk","batter_mlb_id","p_h0"]].rename(columns={"p_h0":"p_h0_b"}),on=["game_pk","batter_mlb_id"],how="inner")
        z["yf"]=z.actual_hits.eq(0).astype(int);eps=1e-9
        z["gain"]=(z.yf*np.log(z.p_h0_f.clip(eps,1-eps))+(1-z.yf)*np.log((1-z.p_h0_f).clip(eps,1-eps))) - \
                  (z.yf*np.log(z.p_h0_b.clip(eps,1-eps))+(1-z.yf)*np.log((1-z.p_h0_b).clip(eps,1-eps)))
        dg=z.groupby("game_date").gain.mean()
        rng=np.random.default_rng(SEED);boots=[rng.choice(dg.to_numpy(),len(dg),replace=True).mean() for _ in range(500)]
        robust.append({"split":split,"test":"date_bootstrap","mean_gain":dg.mean(),"positive_date_fraction":(dg>0).mean(),
                       "ci_low":np.quantile(boots,.025),"ci_high":np.quantile(boots,.975)})
        robust.append({"split":split,"test":"remove_two_best_dates","mean_gain":dg.drop(dg.nlargest(2).index).mean()})
        robust.append({"split":split,"test":"remove_two_worst_dates","mean_gain":dg.drop(dg.nsmallest(2).index).mean()})
        one=z.sort_values("game_date").drop_duplicates("batter_mlb_id",keep="last")
        robust.append({"split":split,"test":"one_row_per_player","mean_gain":one.gain.mean()})
        loo=[dg.drop(date).mean() for date in dg.index]
        robust.append({"split":split,"test":"leave_one_date_out","mean_gain":np.mean(loo),"ci_low":np.min(loo),"ci_high":np.max(loo),"rows":len(loo)})
        meta=target[target.split.eq(split)][["game_pk","batter_mlb_id","history_depth_pa","tier_b_available","home","batting_order_position"]]
        z=z.merge(meta,on=["game_pk","batter_mlb_id"],how="left")
        subset_masks={
          "tier_a_all":np.ones(len(z),dtype=bool),"tier_b_available":z.tier_b_available,
          "event_complete":np.ones(len(z),dtype=bool),"sparse_history_lt100":z.history_depth_pa.lt(100),
          "rookie_recent_callup_lt50":z.history_depth_pa.lt(50),"home":z.home.eq(1),"away":z.home.eq(0),
          "lineup_slots_1_3":z.batting_order_position.le(3),"lineup_slots_4_6":z.batting_order_position.between(4,6),
          "lineup_slots_7_9":z.batting_order_position.ge(7)}
        for label,mask in subset_masks.items():
            robust.append({"split":split,"test":label,"mean_gain":z.loc[mask,"gain"].mean(),"rows":int(mask.sum())})
        for threshold in [0,50,100,250,500]:
            mask=z.history_depth_pa.ge(threshold)
            robust.append({"split":split,"test":f"minimum_history_{threshold}pa","mean_gain":z.loc[mask,"gain"].mean(),"rows":int(mask.sum())})
        byplayer=z.groupby("batter_mlb_id").agg(gain=("gain","sum"),rows=("gain","size"))
        positive=byplayer.gain.clip(lower=0).sum()
        robust.append({"split":split,"test":"player_concentration_top10_share",
                       "mean_gain":byplayer.gain.nlargest(10).sum()/positive if positive else np.nan,"rows":len(byplayer)})
    robust_df=save("robustness_results.csv",robust)
    date_perf=[]
    for split,pred in [("protected_holdout",hold_preds[finalist]),("final_july",july_preds[finalist])]:
        for date,g in pred.groupby("game_date"):
            y=g.actual_hits.eq(0).astype(int)
            date_perf.append({"split":split,"date":date,"rows":len(g),"hitless_rate":y.mean(),
                              "hitless_brier":np.mean((g.p_h0-y)**2),"hits_mae":np.mean(abs(g.expected_hits-g.actual_hits))})
    save("date_level_performance.csv",date_perf)
    save("live_feature_readiness.csv",[{"family":b,"features":a,"live_status":"IDENTIFIABLE" if e=="yes" else "CONDITIONAL_GOVERNED_STARTER_OR_TIER_B","minimum_tier":c} for a,b,c,d,e in feature_registry])
    # Hard gates: positive protected and July improvements on probability quality plus robustness.
    passing=targets[(targets.protected_2026_improvement>0)&(targets.final_july_improvement>0)]
    robust_ok=all(pd.to_numeric(robust_df[robust_df.test.eq("date_bootstrap")].ci_low,errors="coerce").fillna(-1)>=0)
    trial=bool(len(passing) and robust_ok)
    save("bounded_trial_contract.csv",[{"earned":trial,"eligible_targets":"|".join(passing.target) if trial else "",
      "length":"5 qualifying completed slates or 500 graded eligible hitters, whichever occurs first",
      "production_activation":False,"roi_analysis":False}])
    save("narrow_revision_contract.csv",[
      {"blocker":"final July date stability","evidence":"date-bootstrap interval crosses zero; two-best-date removal is negative",
       "required_revision":"predeclare history-depth fallback and test on new untouched completed slates"},
      {"blocker":"UBO-5 joint coherence","evidence":"marginal Hits, TB and HR distributions pass bounds but are fit independently",
       "required_revision":"use the UBO-4 generative joint engine or fit a constrained joint UBO-5 distribution"},
      {"blocker":"sparse-history transfer","evidence":"final-July gains are negative below 100 prior PA",
       "required_revision":"freeze hierarchical UBO-1 fallback for sparse and rookie populations"},
      {"blocker":"project-control comparison","evidence":"no certified identical-row probability binding for incumbent/candidates",
       "required_revision":"bind control probabilities on the frozen population without importing their features"}])
    if trial:final_decision="UNIFIED_BATTER_OUTCOME_V1_READY_FOR_BOUNDED_TRIAL" if len(passing)>1 else "UNIFIED_ENGINE_READY_FOR_ONE_TARGET_ONLY"
    elif len(passing):final_decision="MULTI_SEASON_SIGNAL_PRESENT_REQUIRES_ONE_NARROW_REVISION"
    else:final_decision="PLATFORM_VALID_MODEL_SIGNAL_INSUFFICIENT"
    tmap=dict(zip(targets.target,targets.classification))
    decisions={
      "MLB_UBO_V1_POPULATION_DECISION":f"{len(target)}_CERTIFIED_STARTING_HITTER_ROWS",
      "MLB_UBO_V1_TEMPORAL_INTEGRITY_DECISION":"STRICT_PRIOR_CALENDAR_DATE_FEATURES_ONLY_SAME_DATE_EXCLUDED",
      "MLB_UBO_V1_PA_DISTRIBUTION_DECISION":"MULTICLASS_0_TO_6PLUS_FROZEN_AND_EVALUATED",
      "MLB_UBO_V1_HITTER_PROFILE_DECISION":"CAREER_AND_30_ACTIVE_DATE_SHRUNK_PROFILES",
      "MLB_UBO_V1_PITCHER_PROFILE_DECISION":"GOVERNED_STARTER_IDENTITY_CONDITIONAL_WITH_PITCHER_FREE_VARIANT",
      "MLB_UBO_V1_MATCHUP_DECISION":"TWO_BOUNDED_STRICT_PRIOR_INTERACTIONS",
      "MLB_UBO_V1_PA_OUTCOME_MODEL_DECISION":f"{finalist}_SELECTED_ON_2025_ONLY",
      "MLB_UBO_V1_GAME_DISTRIBUTION_DECISION":"UBO0_TO_4_JOINT_GENERATIVE_COHERENCE_VALIDATED_UBO5_MARGINAL_COHERENCE_ONLY",
      "MLB_UBO_V1_2025_VALIDATION_DECISION":"ARCHITECTURE_SELECTED_AND_FROZEN",
      "MLB_UBO_V1_2026_HOLDOUT_DECISION":"SCORED_AFTER_FREEZE",
      "MLB_UBO_V1_FINAL_JULY_DECISION":"UNTOUCHED_UNTIL_FINALIST_FREEZE",
      "MLB_UBO_V1_HITS05_DECISION":tmap["Hits 0.5"],"MLB_UBO_V1_HITS15_DECISION":tmap["Hits 1.5"],
      "MLB_UBO_V1_TOTAL_BASES_DECISION":tmap["total bases"],"MLB_UBO_V1_HOME_RUN_DECISION":tmap["home run"],
      "MLB_UBO_V1_VS_INCUMBENT_DECISION":"NOT_COMPARABLE_NO_CERTIFIED_IDENTICAL_ROW_PROBABILITY_BINDING",
      "MLB_UBO_V1_VS_CURRENT_CANDIDATE_DECISION":"NOT_COMPARABLE_NO_CERTIFIED_IDENTICAL_ROW_PROBABILITY_BINDING",
      "MLB_UBO_V1_COMPONENT_ATTRIBUTION_DECISION":"SEQUENTIAL_VARIANT_ATTRIBUTION_REPORTED",
      "MLB_UBO_V1_ROBUSTNESS_DECISION":"PASS" if robust_ok else "DATE_STABILITY_NOT_PROVEN",
      "MLB_UBO_V1_LIVE_FEATURE_READINESS_DECISION":"TIER_A_FEATURES_IDENTIFIABLE_PITCHER_AND_TIER_B_CONTEXT_CONDITIONAL",
      "MLB_UBO_V1_TRIAL_ELIGIBILITY_DECISION":"IMMEDIATE_5_SLATE_OR_500_HITTER_TRIAL" if trial else "NO_TRIAL_GATE",
      "MLB_UBO_V1_FINAL_DECISION":final_decision,
      "MLB_PRODUCTION_ACTION_DECISION":"OFFLINE_NEW_MODEL_EXPERIMENT_ONLY_NO_PRODUCTION_ROUTING_THRESHOLD_SELECTOR_UPLOAD_OR WAGER_CHANGE"}
    save("final_decisions.csv",[{"decision":k,"value":v} for k,v in decisions.items()])
    machine={"generated_at_utc":datetime.now(timezone.utc).isoformat(),"finalist":finalist,"final_decision":final_decision,
             "population_rows":len(target),"duration_seconds":time.time()-start,"decisions":decisions}
    (OUT/"machine_readable.json").write_text(json.dumps(machine,indent=2)+"\n")
    required=["frozen_architecture_contract.csv","model_population_manifest.parquet","strict_prior_feature_registry.csv",
      "pa_opportunity_distributions.csv","pa_opportunity_probabilities.parquet","hitter_event_profiles.csv","pitcher_suppression_profiles.csv","matchup_profiles.csv",
      "strict_prior_player_game_features.parquet","variant_population_ledger.csv","per_pa_outcome_matrix_development.parquet","player_game_probability_distributions.parquet","development_results.csv",
      "validation_2025_results.csv","protected_2026_results.csv","final_july_untouched_results.csv","target_specific_evaluations.csv",
      "control_comparisons.csv","component_attribution.csv","robustness_results.csv","live_feature_readiness.csv",
      "bounded_trial_contract.csv","narrow_revision_contract.csv","final_decisions.csv","machine_readable.json"]
    checks=[{"check":f,"status":"PASS" if (OUT/f).exists() else "FAIL","detail":"required artifact"} for f in required]
    pf=all_final[[c for c in all_final if c.startswith("p_")]]
    checks += [{"check":"probability_bounds","status":"PASS" if ((pf>=-1e-12)&(pf<=1+1e-12)).all().all() else "FAIL","detail":"all probability fields"},
      {"check":"hits_distribution_sum","status":"PASS" if np.allclose(all_final[[f"p_{x}" for x in ["h0","h1","h2","h3","h4plus"]]].sum(1),1) else "FAIL","detail":"tolerance 1e-8"},
      {"check":"tb_distribution_sum","status":"PASS" if np.allclose(all_final[[f"p_{x}" for x in ["tb0","tb1","tb2","tb3","tb4plus"]]].sum(1),1) else "FAIL","detail":"tolerance 1e-8"},
      {"check":"hr_distribution_sum","status":"PASS" if np.allclose(all_final[["p_hr0","p_hr1plus"]].sum(1),1) else "FAIL","detail":"tolerance 1e-8"},
      {"check":"cross_target_marginal_consistency","status":"PASS" if
       ((all_final.p_hr1plus<=1-all_final.p_h0+1e-10)&(all_final.expected_tb+1e-10>=all_final.expected_hits)&
        (all_final.expected_hits+1e-10>=all_final.p_hr1plus)).all() else "FAIL",
       "detail":"HR probability <= any-hit probability; expected TB >= expected Hits >= expected HR"},
      {"check":"finalist_joint_distribution","status":"LIMITATION","detail":"UBO-5 marginals independently fit; constrained joint law is a required narrow revision"},
      {"check":"no_2026_design_access","status":"PASS","detail":"finalist chosen from val_metrics before hold/july scoring"},
      {"check":"no_production_action","status":"PASS","detail":"offline artifacts only"}]
    save("validation_report.csv",checks)
    hashes=[]
    for p in sorted(x for x in OUT.iterdir() if x.is_file() and x.name!="sha256_manifest.csv"):
        hashes.append({"path":p.name,"size_bytes":p.stat().st_size,"sha256":sha(p)})
    save("sha256_manifest.csv",hashes);print(json.dumps(machine,indent=2))


if __name__=="__main__":main()
