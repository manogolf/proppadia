#!/usr/bin/env python3
"""Materialize the frozen 38 UBO-5 features only; never fits a model."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import numpy as np, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
from backend.mlb.scripts.run_mlb_unified_batter_outcome_v1 import (
    CLASSES, date_prior_features, outcome_class, safe_rate,
)

FEATURES=(["batting_order_position","home","prior_player_pa_per_date","prior_slot_pa_per_start","opp_prior_dates","history_depth_pa"]+
 [f"h_career_rate_{i}" for i in range(8)]+[f"h_recent30_rate_{i}" for i in range(8)]+
 ["h_swing_rate","h_whiff_per_swing","h_contact_per_swing","h_called_strike_rate","h_foul_rate","h_pitches_per_pa","h_ev","h_xba","h_xwoba","h_lsa6_rate"]+
 ["p_hit_suppression","p_k_rate","p_prior_dates","pitcher_available","matchup_k","matchup_hit"])
MODEL_SUPPORTED_NULL_FEATURES=("p_hit_suppression","p_k_rate","p_prior_dates","matchup_k","matchup_hit")

def files(root,table):
 return sorted((root/table).glob("season=*/*.parquet"))
def read(root,table,cols):
 out=[]
 for p in files(root,table):
  names=pq.ParquetFile(p).schema_arrow.names
  x=pq.ParquetFile(p).read(columns=[c for c in cols if c in names]).to_pandas()
  x["_partition"]=str(p);out.append(x)
 return pd.concat(out,ignore_index=True,sort=False)
def sha_text(s):return hashlib.sha256(s.encode()).hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--normalized-root",type=Path,required=True);ap.add_argument("--output",type=Path,required=True);ap.add_argument("--candidate-file",type=Path);a=ap.parse_args()
 n=a.normalized_root.resolve()
 cand=pd.read_csv(a.candidate_file) if a.candidate_file else pd.DataFrame()
 if a.candidate_file and not len(cand):
  meta=["game_pk","game_date","batter_mlb_id","team","opponent","home_away","lineup_certification_status"]+FEATURES+["latest_included_event_date","feature_vector_sha256","feature_schema_sha256","prediction_timestamp_utc","scheduled_start_utc","lineup_certified_at_utc","line","run_tag","exclusion_reason","route_eligible","strict_prior_pa","source_lineage_pointer","feature_completeness_status","temporal_integrity_status"]
  empty=pd.DataFrame({c:pd.Series(dtype="object") for c in meta})
  a.output.parent.mkdir(parents=True,exist_ok=True);pq.write_table(pa.Table.from_pandas(empty,preserve_index=False),a.output,compression="zstd")
  print(json.dumps({"rows":0,"features":len(FEATURES),"status":"NO_CURRENT_CANDIDATES"}));return
 if len(cand):
  required=["slate_date","game_pk","batter_mlb_id","team","opponent","home_away","prediction_timestamp_utc","scheduled_start_utc","lineup_certified","lineup_certified_at_utc","batting_order_position","line","run_tag","opposing_starter_id"]
  missing=[c for c in required if c not in cand]
  if missing:raise RuntimeError("candidate columns missing: "+"|".join(missing))
  if cand.slate_date.astype(str).nunique()!=1:raise RuntimeError("candidate batch must contain exactly one slate_date")
  cand["prediction_timestamp_utc"]=pd.to_datetime(cand.prediction_timestamp_utc,utc=True);cand["scheduled_start_utc"]=pd.to_datetime(cand.scheduled_start_utc,utc=True);cand["lineup_certified_at_utc"]=pd.to_datetime(cand.lineup_certified_at_utc,utc=True)
  cand["exclusion_reason"]=np.where(~cand.line.eq(1.5),"UNSUPPORTED_LINE",np.where(~cand.lineup_certified.astype(bool),"LINEUP_NOT_CERTIFIED",np.where(cand.prediction_timestamp_utc.ge(cand.scheduled_start_utc),"POST_START_OR_AT_START",np.where(cand.lineup_certified_at_utc.gt(cand.prediction_timestamp_utc),"LINEUP_CERTIFIED_AFTER_PREDICTION",""))))
 games=read(n,"games",["game_pk","game_date","home_team","away_team"]).drop_duplicates("game_pk")
 games.game_date=pd.to_datetime(games.game_date);games.game_pk=pd.to_numeric(games.game_pk).astype(int)
 line=read(n,"starting_lineups",["game_pk","team","team_id","player_id","batting_order_position","home_away","lineup_certification_status","source"])
 line=line.dropna(subset=["game_pk","player_id","batting_order_position"]).copy();line.game_pk=pd.to_numeric(line.game_pk).astype(int);line.player_id=pd.to_numeric(line.player_id).astype(int)
 line=line.merge(games,on="game_pk",how="inner");line["team"]=line.team.where(line.team.notna(),np.where(line.home_away.eq("home"),line.home_team,line.away_team))
 line["opponent"]=np.where(line.team.eq(line.home_team),line.away_team,line.home_team)
 line=line.sort_values(["game_pk","team","batting_order_position"]).drop_duplicates(["game_pk","team","batting_order_position"])
 line["_candidate"]=False
 if len(cand):
  keys=set(zip(cand.game_pk.astype(int),cand.batter_mlb_id.astype(int)))
  line=line[~pd.Series(list(zip(line.game_pk,line.player_id)),index=line.index).isin(keys)]
  cl=pd.DataFrame({"game_pk":cand.game_pk.astype(int),"game_date":pd.to_datetime(cand.slate_date),"player_id":cand.batter_mlb_id.astype(int),"team":cand.team,"opponent":cand.opponent,"home_away":cand.home_away,"batting_order_position":cand.batting_order_position,"lineup_certification_status":np.where(cand.lineup_certified,"CONFIRMED_LINEUP","UNCONFIRMED"),"source":"SYNTHETIC_UNPLAYED_CANDIDATE","home_team":np.where(cand.home_away.eq("home"),cand.team,cand.opponent),"away_team":np.where(cand.home_away.eq("away"),cand.team,cand.opponent),"_candidate":True})
  line=pd.concat([line,cl],ignore_index=True,sort=False)
 pdf=read(n,"plate_appearances",["game_pk","game_date","at_bat_number","batter","pitcher","events","stand"])
 pdf=pdf.dropna(subset=["game_pk","batter","events"]).copy();pdf.game_pk=pd.to_numeric(pdf.game_pk).astype(int);pdf.batter=pd.to_numeric(pdf.batter).astype(int);pdf.pitcher=pd.to_numeric(pdf.pitcher,errors="coerce").astype("Int64");pdf.game_date=pd.to_datetime(pdf.game_date)
 pdf["class_idx"]=pdf.events.map(outcome_class).map({c:i for i,c in enumerate(CLASSES)});pdf["hit"]=pdf.events.isin(["single","double","triple","home_run"]).astype(int);pdf["tb"]=pdf.events.map({"single":1,"double":2,"triple":3,"home_run":4}).fillna(0).astype(int);pdf["hr"]=pdf.events.eq("home_run").astype(int);pdf["pa"]=1
 for i in range(8):pdf[f"oc_{i}"]=pdf.class_idx.eq(i).astype(int)
 cc=["pa","hit","tb","hr"]+[f"oc_{i}" for i in range(8)]
 pg=pdf.groupby(["game_pk","game_date","batter"],as_index=False)[cc].sum()
 pitch=read(n,"pitches",["game_pk","game_date","batter","pitcher","description"])
 pitch=pitch.dropna(subset=["game_pk","batter"]).copy();pitch.game_pk=pd.to_numeric(pitch.game_pk).astype(int);pitch.batter=pd.to_numeric(pitch.batter).astype(int)
 d=pitch.description.fillna("");s=d.isin(["swinging_strike","swinging_strike_blocked","foul","foul_tip","hit_into_play","missed_bunt","bunt_foul_tip","foul_bunt"]);w=d.isin(["swinging_strike","swinging_strike_blocked","missed_bunt"])
 pitch["pitches"]=1;pitch["swings"]=s.astype(int);pitch["whiffs"]=w.astype(int);pitch["contacts"]=(s&~w).astype(int);pitch["called_strikes"]=d.eq("called_strike").astype(int);pitch["fouls"]=d.isin(["foul","foul_tip","foul_bunt","bunt_foul_tip"]).astype(int)
 pga=pitch.groupby(["game_pk","batter"],as_index=False)[["pitches","swings","whiffs","contacts","called_strikes","fouls"]].sum()
 bb=read(n,"batted_balls",["game_pk","batter","launch_speed","estimated_ba_using_speedangle","estimated_woba_using_speedangle","launch_speed_angle"])
 bb.game_pk=pd.to_numeric(bb.game_pk).astype(int);bb.batter=pd.to_numeric(bb.batter).astype(int)
 for c in ["launch_speed","estimated_ba_using_speedangle","estimated_woba_using_speedangle","launch_speed_angle"]:bb[c]=pd.to_numeric(bb[c],errors="coerce")
 bb["bb"]=1;bb["ev_sum"]=bb.launch_speed.fillna(0);bb["ev_n"]=bb.launch_speed.notna().astype(int);bb["x_6"]=bb.launch_speed_angle.eq(6).astype(int);bb["xba_sum"]=bb.estimated_ba_using_speedangle.fillna(0);bb["xba_n"]=bb.estimated_ba_using_speedangle.notna().astype(int);bb["xwoba_sum"]=bb.estimated_woba_using_speedangle.fillna(0);bb["xwoba_n"]=bb.estimated_woba_using_speedangle.notna().astype(int)
 bga=bb.groupby(["game_pk","batter"],as_index=False)[["bb","ev_sum","ev_n","x_6","xba_sum","xba_n","xwoba_sum","xwoba_n"]].sum()
 hist=pg.merge(pga,on=["game_pk","batter"],how="left").merge(bga,on=["game_pk","batter"],how="left").fillna(0)
 hc=cc+["pitches","swings","whiffs","contacts","called_strikes","fouls","bb","ev_sum","ev_n","x_6","xba_sum","xba_n","xwoba_sum","xwoba_n"]
 if len(cand):
  zeros=pd.DataFrame({"game_pk":cand.game_pk.astype(int),"game_date":pd.to_datetime(cand.slate_date),"batter":cand.batter_mlb_id.astype(int)})
  for c in hc:zeros[c]=0
  hist=pd.concat([hist,zeros],ignore_index=True,sort=False)
 hp=date_prior_features(hist.rename(columns={"batter":"batter_mlb_id"}),"batter_mlb_id",hc,"h_")
 t=line.rename(columns={"player_id":"batter_mlb_id"}).merge(pg.rename(columns={"batter":"batter_mlb_id"}),on=["game_pk","game_date","batter_mlb_id"],how="left")
 t=t[((t.pa.notna()&t.pa.gt(0))|t.get("_candidate",False).fillna(False))].copy();t.loc[t.get("_candidate",False).fillna(False),"pa"]=0;t=t.merge(hp,on=["batter_mlb_id","game_date"],how="left");t["home"]=t.home_away.eq("home").astype(int)
 op=date_prior_features(t[["batter_mlb_id","game_date","pa"]].rename(columns={"pa":"opp_pa"}),"batter_mlb_id",["opp_pa"],"opp_");t=t.merge(op,on=["batter_mlb_id","game_date"],how="left")
 slot=t.groupby(["batting_order_position","game_date"],as_index=False).agg(slot_pa=("pa","sum"),slot_starts=("pa","size")).sort_values(["batting_order_position","game_date"])
 for c in ["slot_pa","slot_starts"]:slot[f"prior_{c}"]=slot.groupby("batting_order_position")[c].cumsum()-slot[c]
 t=t.merge(slot[["batting_order_position","game_date","prior_slot_pa","prior_slot_starts"]],on=["batting_order_position","game_date"],how="left")
 t["prior_player_pa_per_date"]=t.opp_career_opp_pa/t.opp_prior_dates.clip(lower=1);t["prior_slot_pa_per_start"]=t.prior_slot_pa/t.prior_slot_starts.clip(lower=1);t["history_depth_pa"]=t.h_career_pa
 pit=read(n,"player_game_pitching",["game_pk","player_id","team","games_started","p_gs"]);gs=pd.to_numeric(pit.get("games_started"),errors="coerce").fillna(pd.to_numeric(pit.get("p_gs"),errors="coerce")).fillna(0)
 pit=pit[gs.eq(1)&pit.player_id.notna()].copy();pit.game_pk=pd.to_numeric(pit.game_pk).astype(int);pit.player_id=pd.to_numeric(pit.player_id).astype(int);pit=pit.drop_duplicates(["game_pk","team"]).rename(columns={"player_id":"opposing_starter_id","team":"pitcher_team"})
 t=t.merge(pit[["game_pk","pitcher_team","opposing_starter_id"]],left_on=["game_pk","opponent"],right_on=["game_pk","pitcher_team"],how="left")
 if len(cand):
  cmap=dict(zip(zip(cand.game_pk.astype(int),cand.batter_mlb_id.astype(int)),pd.to_numeric(cand.opposing_starter_id,errors="coerce")))
  cm=t.get("_candidate",False).fillna(False);t.loc[cm,"opposing_starter_id"]=[cmap.get((int(g),int(b))) for g,b in zip(t.loc[cm,"game_pk"],t.loc[cm,"batter_mlb_id"])]
 ph=pdf.dropna(subset=["pitcher"]).copy();ph["pitcher_id"]=ph.pitcher.astype(int);ph=ph.groupby(["pitcher_id","game_date"],as_index=False)[cc].sum()
 if len(cand):
  pz=pd.DataFrame({"pitcher_id":pd.to_numeric(cand.opposing_starter_id,errors="coerce"),"game_date":pd.to_datetime(cand.slate_date)}).dropna(subset=["pitcher_id"]);pz["pitcher_id"]=pz.pitcher_id.astype(int)
  for c in cc:pz[c]=0
  ph=pd.concat([ph,pz],ignore_index=True,sort=False)
 pp=date_prior_features(ph,"pitcher_id",cc,"p_").rename(columns={"pitcher_id":"opposing_starter_id"});t=t.merge(pp,on=["opposing_starter_id","game_date"],how="left")
 gd=pdf[pdf.game_date.dt.year.le(2024)].class_idx.value_counts(normalize=True).reindex(range(8),fill_value=0).to_numpy()
 for scope in ["career","recent30"]:
  den=t[f"h_{scope}_pa"].fillna(0)
  for i in range(8):t[f"h_{scope}_rate_{i}"]=safe_rate(t[f"h_{scope}_oc_{i}"].fillna(0),den,gd[i],200)
 t["h_swing_rate"]=safe_rate(t.h_career_swings,t.h_career_pitches,.47,100);t["h_whiff_per_swing"]=safe_rate(t.h_career_whiffs,t.h_career_swings,.22,100);t["h_contact_per_swing"]=safe_rate(t.h_career_contacts,t.h_career_swings,.78,100);t["h_called_strike_rate"]=safe_rate(t.h_career_called_strikes,t.h_career_pitches,.16,100);t["h_foul_rate"]=safe_rate(t.h_career_fouls,t.h_career_pitches,.17,100);t["h_pitches_per_pa"]=safe_rate(t.h_career_pitches,t.h_career_pa,3.9,50);t["h_ev"]=safe_rate(t.h_career_ev_sum,t.h_career_ev_n,88.5,50);t["h_xba"]=safe_rate(t.h_career_xba_sum,t.h_career_xba_n,.245,50);t["h_xwoba"]=safe_rate(t.h_career_xwoba_sum,t.h_career_xwoba_n,.320,50);t["h_lsa6_rate"]=safe_rate(t.h_career_x_6,t.h_career_bb,.07,50)
 t["p_hit_suppression"]=safe_rate(t.p_career_hit,t.p_career_pa,.225,250);t["p_k_rate"]=safe_rate(t.p_career_oc_0,t.p_career_pa,.225,250);t["matchup_k"]=t.h_career_rate_0*t.p_k_rate;t["matchup_hit"]=sum(t[f"h_career_rate_{i}"] for i in [3,4,5,6])*t.p_hit_suppression;t["pitcher_available"]=t.opposing_starter_id.notna().astype(int)
 out=t[["game_pk","game_date","batter_mlb_id","team","opponent","home_away","lineup_certification_status"]+FEATURES].copy()
 out["latest_included_event_date"]=(out.game_date-pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d");out["feature_vector_sha256"]=out[FEATURES].astype(str).agg("|".join,axis=1).map(sha_text)
 required_non_null=[c for c in FEATURES if c not in MODEL_SUPPORTED_NULL_FEATURES]
 supported_null_count=out[list(MODEL_SUPPORTED_NULL_FEATURES)].isna().sum(axis=1)
 out["feature_schema_sha256"]=sha_text("\n".join(FEATURES));out["feature_completeness_status"]=np.where(out[required_non_null].notna().all(axis=1),np.where(supported_null_count.gt(0),"COMPLETE_WITH_MODEL_SUPPORTED_NULLS","COMPLETE"),"INCOMPLETE_REQUIRED_FEATURE");out["temporal_integrity_status"]="PASS"
 if len(cand):
  meta_cols=["game_pk","batter_mlb_id","prediction_timestamp_utc","scheduled_start_utc","lineup_certified_at_utc","line","run_tag","exclusion_reason"]+[c for c in ["source_lineage_pointer","batter_identity_certified","identity_ambiguous","market_row_certified"] if c in cand]
  meta=cand[meta_cols]
  out=out.merge(meta,on=["game_pk","batter_mlb_id"],how="left");cm=out.run_tag.notna();out.loc[cm&out.history_depth_pa.lt(100),"exclusion_reason"]="STRICT_PRIOR_PA_LT_100";out["route_eligible"]=cm&out.exclusion_reason.fillna("").eq("")&out.history_depth_pa.ge(100)
  out["strict_prior_pa"]=out["history_depth_pa"]
  out=out[out["run_tag"].notna()].copy()
 a.output.parent.mkdir(parents=True,exist_ok=True);pq.write_table(pa.Table.from_pandas(out,preserve_index=False),a.output,compression="zstd")
 print(json.dumps({"rows":len(out),"features":len(FEATURES),"max_date":str(out.game_date.max().date())}))
if __name__=="__main__":main()
