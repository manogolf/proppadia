#!/usr/bin/env python3
"""WARN-only observer for odds-independent NHL SOG MIDDAY/FINAL predictions."""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg

from backend.nhl.sog_cold_start.core import build_predictions, sha256_file
from backend.nhl.scripts.nhl_observer_provenance import observer_provenance

ROOT=Path(__file__).resolve().parents[3]
DEFAULT_ROOT=ROOT/"artifacts/operational/nhl/sog_prediction_only"

def utc_now()->datetime:return datetime.now(timezone.utc)
def load_env(path:Path)->None:
    if not path.is_file():return
    for raw in path.read_text().splitlines():
        line=raw.strip()
        if line and not line.startswith("#") and "=" in line:
            key,value=line.split("=",1);os.environ.setdefault(key.strip(),value.strip().strip("'\""))
def durable_json(path:Path,payload:dict)->None:
    path.parent.mkdir(parents=True,exist_ok=True);fd,raw=tempfile.mkstemp(prefix=f".{path.name}.",dir=path.parent);tmp=Path(raw)
    try:
        with os.fdopen(fd,"w") as h:h.write(json.dumps(payload,indent=2,sort_keys=True)+"\n");h.flush();os.fsync(h.fileno())
        os.replace(tmp,path)
    finally:tmp.unlink(missing_ok=True)
def phase_for(schedule:pd.DataFrame,now:datetime,requested:str)->tuple[str|None,str]:
    if schedule.empty:return None,"VALID_EMPTY_SLATE"
    starts=pd.to_datetime(schedule.scheduled_start_time_utc,utc=True);future=starts[starts>pd.Timestamp(now)]
    if future.empty:return None,"NO_PRESTART_GAME"
    minutes=(future.min()-pd.Timestamp(now)).total_seconds()/60
    if requested!="AUTO":return requested,"EXPLICIT_PHASE"
    local=now.astimezone(ZoneInfo("America/Los_Angeles"))
    if 20<=minutes<=75:return "FINAL_PREGAME",f"FIRST_START_IN_{minutes:.1f}_MINUTES"
    if local.hour==12 and local.minute<=30:return "MIDDAY","LOCAL_MIDDAY_WINDOW"
    return None,f"OUTSIDE_PREDICTION_WINDOW_FIRST_START_IN_{minutes:.1f}_MINUTES"
@contextlib.contextmanager
def lock(root:Path,slate:str,phase:str):
    p=root/"locks"/f"{slate}_{phase}.lock";p.parent.mkdir(parents=True,exist_ok=True)
    with p.open("a+") as h:
        try:fcntl.flock(h,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as e:raise RuntimeError("SOG_PREDICTION_ONLY_ALREADY_RUNNING") from e
        yield
def _read(cur,query:str,params:tuple=())->pd.DataFrame:
    cur.execute(query,params);return pd.DataFrame(cur.fetchall(),columns=[c.name for c in cur.description])
def export_features(dsn:str,slate:str,cutoff:datetime)->tuple[pd.DataFrame,pd.DataFrame]:
    with psycopg.connect(dsn) as con:
      with con.cursor() as cur:
        cur.execute("BEGIN READ ONLY")
        schedule=_read(cur,"""SELECT season AS canonical_season,game_date::text AS slate_date,game_id,start_time_utc AS scheduled_start_time_utc,home_team_id,away_team_id,game_type,status FROM nhl.games WHERE season=2026 AND game_date=%s::date ORDER BY start_time_utc,game_id""",(slate,))
        if schedule.empty:con.rollback();return schedule,pd.DataFrame()
        games=schedule.game_id.astype(int).tolist();teams=sorted(set(schedule.home_team_id.astype(int))|set(schedule.away_team_id.astype(int)))
        roster=_read(cur,"""WITH latest AS (SELECT DISTINCT ON (r.game_id,r.player_id) r.game_id,r.player_id,r.team_id,r.active_flag,r.asof_ts FROM nhl.roster_status r WHERE r.game_id=ANY(%s) AND r.asof_ts<=%s ORDER BY r.game_id,r.player_id,r.asof_ts DESC) SELECT l.game_id,l.player_id,l.team_id,p.full_name AS player_name,p.position,l.active_flag,l.asof_ts FROM latest l JOIN nhl.players p USING(player_id)""",(games,cutoff))
        if roster.empty:
            players=_read(cur,"""SELECT player_id,current_team_id AS team_id,full_name AS player_name,position,active,updated_at FROM nhl.players WHERE current_team_id=ANY(%s) AND active IS TRUE""",(teams,))
            roster=schedule.assign(_key=1).merge(players.assign(_key=1),on="_key").drop(columns="_key")
            roster=roster[(roster.team_id.eq(roster.home_team_id))|(roster.team_id.eq(roster.away_team_id))]
            roster["active_flag"]=True;roster["asof_ts"]=roster.updated_at;roster_source="ACTIVE_PLAYER_REFERENCE_FALLBACK"
        else:roster_source="GAME_ROSTER_STATUS"
        ids=roster.player_id.astype(int).unique().tolist()
        hist=_read(cur,"""SELECT g.season,g.game_type,g.game_date,g.start_time_utc,g.game_id,l.player_id,l.team_id,l.shots_on_goal,l.toi_minutes FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING(game_id) WHERE l.player_id=ANY(%s) AND g.start_time_utc<%s AND lower(g.status)='final' ORDER BY g.start_time_utc,g.game_id""",(ids,cutoff))
        pos=_read(cur,"""SELECT CASE WHEN upper(position)='D' THEN 'D' ELSE 'F' END AS position_group,sum(l.shots_on_goal)::float8*60/nullif(sum(l.toi_minutes),0) AS position_sog_per60,avg(l.toi_minutes)::float8 AS position_toi_per_game FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING(game_id) JOIN nhl.players p USING(player_id) WHERE g.season=2025 GROUP BY 1 UNION ALL SELECT 'ALL',sum(l.shots_on_goal)::float8*60/nullif(sum(l.toi_minutes),0),avg(l.toi_minutes)::float8 FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING(game_id) WHERE g.season=2025""")
        con.rollback()
    rows=[]
    posmap=pos.set_index("position_group").to_dict("index")
    for rr in roster.itertuples(index=False):
        game=schedule[schedule.game_id.eq(rr.game_id)].iloc[0];opponent=int(game.away_team_id if int(rr.team_id)==int(game.home_team_id) else game.home_team_id)
        ph=hist[hist.player_id.eq(rr.player_id)].copy();prior=ph[ph.season.eq(2025)];older=ph[ph.season.eq(2024)];pre=ph[(ph.season.eq(2026))&ph.game_type.eq(1)];reg=ph[(ph.season.eq(2026))&ph.game_type.eq(2)]
        def stats(x):
            minutes=pd.to_numeric(x.toi_minutes,errors="coerce").sum();shots=pd.to_numeric(x.shots_on_goal,errors="coerce").sum();games=x.game_id.nunique()
            return minutes,(shots*60/minutes if minutes>0 else None),(pd.to_numeric(x.toi_minutes,errors="coerce").mean() if games else None),games
        pm,pr,pt,pg=stats(prior);om,orr,ot,og=stats(older);xm,xr,xt,xg=stats(pre);rm,cr,ct,cg=stats(reg);group="D" if str(rr.position).upper()=="D" else "F";pp=posmap.get(group,{});league=posmap.get("ALL",{})
        recent=prior.tail(20).copy()
        if len(recent):
            weights=pd.Series([.9**i for i in range(len(recent)-1,-1,-1)],index=recent.index);rmin=(pd.to_numeric(recent.toi_minutes,errors="coerce")*weights).sum();rshots=(pd.to_numeric(recent.shots_on_goal,errors="coerce")*weights).sum();recent_rate=rshots*60/rmin if rmin>0 else None;recent_toi=rmin/weights.sum()
        else:recent_rate=recent_toi=None
        rows.append({"canonical_season":int(game.canonical_season),"slate_date":slate,"game_id":int(rr.game_id),"player_id":int(rr.player_id),"player_name":rr.player_name,"team_id":int(rr.team_id),"opponent_id":opponent,"position":rr.position,"roster_status":"ACTIVE_ROSTER" if bool(rr.active_flag) else "INACTIVE","lineup_status":"ACTIVE_ROSTER_UNCONFIRMED","scheduled_start_time_utc":pd.Timestamp(game.scheduled_start_time_utc).isoformat(),"feature_cutoff_utc":cutoff.isoformat(),"roster_source":roster_source,"prior_minutes":pm,"prior_sog_per60":pr,"prior_toi_per_game":pt,"prior_recency_sog_per60":recent_rate,"prior_recency_toi_per_game":recent_toi,"older_minutes":om,"older_sog_per60":orr,"older_toi_per_game":ot,"position_sog_per60":pp.get("position_sog_per60"),"position_toi_per_game":pp.get("position_toi_per_game"),"league_sog_per60":league.get("position_sog_per60"),"league_toi_per_game":league.get("position_toi_per_game"),"team_changed":bool(len(prior) and int(prior.iloc[-1].team_id)!=int(rr.team_id)),"current_preseason_games":xg,"current_preseason_sog_per60":xr,"current_preseason_toi_per_game":xt,"current_regular_games":cg,"current_regular_sog_per60":cr,"current_regular_toi_per_game":ct})
    return schedule,pd.DataFrame(rows)
def write_manifest(directory:Path)->None:
    files=sorted(p for p in directory.iterdir() if p.is_file() and p.name!="SHA256SUMS");(directory/"SHA256SUMS").write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))
def observe(*,slate:str,requested:str,dsn:str,root:Path,now:datetime|None=None)->Path:
    now=now or utc_now();status_dir=root/"status"/slate;status_dir.mkdir(parents=True,exist_ok=True);stamp=now.strftime("%Y%m%dT%H%M%S.%fZ")+"_"+uuid.uuid4().hex[:8];status_path=status_dir/f"prediction_{stamp}.json"
    result={"slate_date":slate,"status":"RUNNING","market_requests":0,"paid_credits":0,"warning_only":True,
            "morning_readiness_dependency":"NONE_PREDICTION_ONLY",
            "provider_event_dependency":"NONE","market_credential_access":False,
            **observer_provenance(Path(__file__),now)}
    if slate<="2026-09-19":
        result.update(status="NOOP_RETROSPECTIVE_FORBIDDEN",gate_reason="SEPTEMBER_19_RETROSPECTIVE_PREDICTION_FORBIDDEN")
        durable_json(status_path,result);return status_path
    try:
        if not dsn:raise RuntimeError("SUPABASE_DB_URL_MISSING")
        schedule,features=export_features(dsn,slate,now);phase,reason=phase_for(schedule,now,requested);result.update(phase=phase,phase_gate=reason)
        if phase is None:result["status"]="NOOP_OUTSIDE_WINDOW";durable_json(status_path,result);return status_path
        with lock(root,slate,phase):
            phase_root=root/"season=2026"/f"slate_date={slate}"/f"phase={phase}"
            existing=list(phase_root.glob("run_id=*"))
            if existing:result.update(status="NOOP_ALREADY_CAPTURED",existing_run=str(existing[0]));durable_json(status_path,result);return status_path
            prestart=pd.to_datetime(features.scheduled_start_time_utc,utc=True,errors="coerce")>pd.Timestamp(now)
            poststart=features.loc[~prestart].copy();features=features.loc[prestart].copy()
            pred,inputs,excluded=build_predictions(features,slate_date=slate,phase=phase,prediction_timestamp_utc=now.isoformat(),input_cutoff_utc=now.isoformat())
            if not poststart.empty:
                poststart=poststart[["canonical_season","slate_date","game_id","player_id","player_name","team_id","opponent_id","position","roster_status","lineup_status","scheduled_start_time_utc","feature_cutoff_utc"]].assign(phase=phase,exclusion_reason="POST_START_NOT_ELIGIBLE",contract_version="NHL_SOG_COLD_START_PREDICTION_V1")
                excluded=pd.concat([excluded,poststart],ignore_index=True)
            run_id=f"nhlsogprediction_s2026_d{slate.replace('-','')}_t{now.strftime('%Y%m%dT%H%M%S%fZ')}_{phase}_v1";dest=phase_root/f"run_id={run_id}";dest.mkdir(parents=True,exist_ok=False)
            schedule.to_csv(dest/"canonical_game_spine.csv",index=False);features.to_csv(dest/"feature_source_snapshot.csv",index=False);inputs.to_csv(dest/"prediction_inputs.csv",index=False);pred.to_csv(dest/"immutable_predictions.csv",index=False);excluded.to_csv(dest/"excluded_players.csv",index=False)
            meta={"run_id":run_id,"slate_date":slate,"phase":phase,"prediction_timestamp_utc":now.isoformat(),"status":"COMPLETE_PREDICTION_ONLY_SHADOW","prediction_rows":len(pred),"admitted_players":int(inputs.player_id.nunique()) if not inputs.empty else 0,"prediction_arms":sorted(inputs.contract_arm.unique().tolist()) if not inputs.empty else [],"excluded_players":len(excluded),"market_attachment":"UNAVAILABLE_NOT_REQUIRED","market_requests":0,"paid_credits":0,"publication":False,"wagering":False};(dest/"run_metadata.json").write_text(json.dumps(meta,indent=2,sort_keys=True)+"\n");write_manifest(dest);result.update(status="CAPTURED",run_dir=str(dest),prediction_rows=len(pred),excluded_players=len(excluded))
    except Exception as error:result.update(status="FAILED_WARN_ONLY",failure=f"{type(error).__name__}:{error}")
    durable_json(status_path,result);return status_path
def main()->int:
    ap=argparse.ArgumentParser();ap.add_argument("--slate-date",default="today");ap.add_argument("--phase",choices=["AUTO","MIDDAY","FINAL_PREGAME"],default="AUTO");ap.add_argument("--env-file",type=Path,default=ROOT/"backend/.env");ap.add_argument("--output-root",type=Path,default=DEFAULT_ROOT);a=ap.parse_args();load_env(a.env_file);now=utc_now();slate=now.astimezone(ZoneInfo("America/New_York")).date().isoformat() if a.slate_date=="today" else a.slate_date;path=observe(slate=slate,requested=a.phase,dsn=os.environ.get("SUPABASE_DB_URL","").strip(),root=a.output_root,now=now);print(path);return 0
if __name__=="__main__":raise SystemExit(main())
