#!/usr/bin/env python3
from __future__ import annotations

import argparse, csv, hashlib, json, math, os, shutil, sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import joblib, numpy as np, pandas as pd

from backend.mlb.scripts.dh_forward_automation_common import (
    ROOT, append_unique_atomic, capture_window_state, date_range, exclusive_lock,
    feed_url, fetch_json, load_config, pacific_today, parse_utc, read_csv,
    schedule_url, sha256_path, update_rolling_status, validate_scorer,
)

FIELDS = "canonical_identity game_date game_pk player_mlb_id team_mlb_id opponent_mlb_id batting_order batter_hand opposing_starter_mlb_id opposing_starter_hand capture_timestamp_utc scheduled_start_utc official_lineup_source official_lineup_source_sha256 bench_certification_status bullpen_certification_status scorer_semantic_id scorer_sha256 feature_schema_sha256 frozen_cutoff_version frozen_cutoff feature_vector_sha256 interval_hazard_0 interval_hazard_1 interval_hazard_2 interval_hazard_3 cumulative_score historical_reference_percentile forward_top20 baseline_expected_pa adjusted_expected_pa expected_pa_delta baseline_probability_lt4_pa adjusted_probability_lt4_pa baseline_probability_lt5_pa adjusted_probability_lt5_pa baseline_o15_probability adjusted_o15_probability o15_probability_delta certification_result fallback_and_data_quality_status".split()
AUDIT_FIELDS = "run_timestamp_utc game_date game_pk team_mlb_id opponent_mlb_id starting_dh_player_id scheduled_start_utc status detail lineup_source_sha256 starter_source retrieval_timestamp_utc".split()


def _person(feed, pid): return feed.get("gameData",{}).get("players",{}).get("ID"+str(pid),{})
def _hand(feed,pid,key): return _person(feed,pid).get(key,{}).get("code","")
def _position(feed,pid): return _person(feed,pid).get("primaryPosition",{}).get("abbreviation","")
def _order(team):
    result=[]
    for value in team.get("battingOrder",[]):
        value=int(value); result.append(value//100 if value%100==0 else value)
    return result[:9]
def _dh(team):
    for pid in _order(team):
        row=team.get("players",{}).get("ID"+str(pid),{}); positions=row.get("allPositions") or []
        if (positions and positions[0].get("abbreviation")=="DH") or row.get("position",{}).get("abbreviation")=="DH": return pid
    return None


def ensure_seed(config):
    ledger=config["prediction_ledger"]
    if ledger.exists(): return
    seed=config["provenance_prediction_seed"]
    if not seed.exists(): raise RuntimeError("PREDICTION_SEED_MISSING")
    ledger.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(seed,ledger)


def cache_prior_feeds(config, day, fetch=fetch_json):
    cache=config["prior_feed_cache"]; cache.mkdir(parents=True,exist_ok=True)
    prior_days=list(date_range(day,8))
    if len(list(cache.glob("*.json")))>=50: prior_days=prior_days[-1:]
    for prior in prior_days:
        try: schedule,_=fetch(schedule_url(prior))
        except Exception: continue
        for block in schedule.get("dates",[]):
            for game in block.get("games",[]):
                pk=int(game["gamePk"]); path=cache/f"{pk}.json"
                if path.exists(): continue
                try:
                    payload,raw=fetch(feed_url(pk))
                    if payload.get("gameData",{}).get("datetime",{}).get("officialDate","")<day and payload.get("gameData",{}).get("status",{}).get("abstractGameState")=="Final":
                        tmp=path.with_suffix(".tmp"); tmp.write_bytes(raw); os.replace(tmp,path)
                except Exception: continue


def build_histories(config, day):
    paths=list((ROOT/"backend/mlb/data/external/statsapi/raw/2026").glob("*/feed_live.json"))+list(config["prior_feed_cache"].glob("*.json"))
    games={}
    for path in paths:
        try:
            feed=json.loads(path.read_text()); d=feed.get("gameData",{}).get("datetime",{}).get("officialDate","")
            if d<day and feed.get("gameData",{}).get("status",{}).get("abstractGameState")=="Final": games[int(feed["gamePk"])]=feed
        except Exception: continue
    pstarts=defaultdict(int); pph=defaultdict(int); pdh=defaultdict(int); prem4=defaultdict(int); teamdh=defaultdict(int); teamph=defaultdict(int); role=defaultdict(lambda:defaultdict(int)); pitching=defaultdict(list)
    for _,feed in sorted(games.items(),key=lambda item:item[1].get("gameData",{}).get("datetime",{}).get("dateTime","")):
        gd=feed["gameData"]["datetime"]["officialDate"]; box=feed.get("liveData",{}).get("boxscore",{}); plays=feed.get("liveData",{}).get("plays",{}).get("allPlays",[])
        subs=defaultdict(list)
        for play in plays:
            for event in play.get("playEvents",[]):
                replaced=event.get("details",{}).get("replacedPlayer",{}).get("id") if event.get("isSubstitution") else None
                if replaced: subs[int(replaced)].append(event)
        for side in ("away","home"):
            team=box.get("teams",{}).get(side,{}); tid=int(feed["gameData"]["teams"][side]["id"]); order=_order(team); dhid=_dh(team)
            for pid in order:
                pstarts[pid]+=1; role[pid]["games"]+=1; role[pid]["starts"]+=1
                if any(e.get("details",{}).get("position",{}).get("abbreviation")=="PH" or "pinch hit" in e.get("details",{}).get("description","").lower() for e in subs.get(pid,[])): pph[pid]+=1
            if dhid:
                pdh[dhid]+=1; teamdh[tid]+=1; stats=team.get("players",{}).get("ID"+str(dhid),{}).get("stats",{}).get("batting",{}); pa=int(stats.get("plateAppearances") or 0)
                pinch=any(e.get("details",{}).get("position",{}).get("abbreviation")=="PH" or "pinch hit" in e.get("details",{}).get("description","").lower() for e in subs.get(dhid,[]))
                if pinch: teamph[tid]+=1
                if subs.get(dhid) and pa<4: prem4[dhid]+=1
            for key,row in team.get("players",{}).items():
                pid=int(key[2:]); bat=row.get("stats",{}).get("batting",{}); pit=row.get("stats",{}).get("pitching",{})
                if (bat.get("plateAppearances") or 0)>0 and pid not in order: role[pid]["ph"]+=1
                if pit and (pit.get("battersFaced") or 0)>0:
                    innings=str(pit.get("inningsPitched") or "0"); whole,_,frac=innings.partition("."); outs=int(whole)*3+int(frac or 0)
                    pitching[pid].append((gd,int(pit.get("numberOfPitches") or 0),outs))
    return dict(games=len(games),pstarts=pstarts,pph=pph,pdh=pdh,prem4=prem4,teamdh=teamdh,teamph=teamph,role=role,pitching=pitching)


def score_feed(feed, raw_sha, now, day, artifact, schema_sha, hist, existing_keys):
    rows=[]; audits=[]; box=feed.get("liveData",{}).get("boxscore",{}); start=parse_utc(feed["gameData"]["datetime"]["dateTime"]); pk=int(feed["gamePk"]); alpha=5; global_rate=float(artifact["global_event_rate"])
    for side in ("away","home"):
        opp="home" if side=="away" else "away"; team=box.get("teams",{}).get(side,{}); opponent=box.get("teams",{}).get(opp,{})
        tid=int(feed["gameData"]["teams"][side]["id"]); oid=int(feed["gameData"]["teams"][opp]["id"]); order=_order(team); dhid=_dh(team)
        baseaudit=dict(run_timestamp_utc=now.isoformat().replace("+00:00","Z"),game_date=day,game_pk=pk,team_mlb_id=tid,opponent_mlb_id=oid,starting_dh_player_id=dhid or "",scheduled_start_utc=start.isoformat().replace("+00:00","Z"),lineup_source_sha256=raw_sha,starter_source="OFFICIAL_MLB_BOXSCORE_PITCHERS_FIRST_ENTRY",retrieval_timestamp_utc=now.isoformat().replace("+00:00","Z"))
        if len(order)<9: audits.append(dict(baseaudit,status="PENDING_OFFICIAL_LINEUP",detail="official batting order not posted")); continue
        if now>=start: audits.append(dict(baseaudit,status="BLOCKED_GAME_ALREADY_STARTED",detail="capture timestamp not before first pitch")); continue
        pitchers=[int(x) for x in opponent.get("pitchers",[])]; starter=pitchers[0] if pitchers else None
        if not starter: audits.append(dict(baseaudit,status="PENDING_CONFIRMED_STARTER",detail="no official opposing starter in boxscore")); continue
        if not dhid: audits.append(dict(baseaudit,status="OTHER_EXPLICIT_REASON",detail="official lineup has no starting DH")); continue
        key=(day,str(pk),str(tid),str(dhid),artifact["artifact_type"])
        if key in existing_keys: audits.append(dict(baseaudit,status="ALREADY_CAPTURED",detail="canonical scorer identity already present")); continue
        bench=[int(x) for x in team.get("bench",[])]; bullpen=[int(x) for x in opponent.get("bullpen",[])]
        if not bench: audits.append(dict(baseaudit,status="BLOCKED_BENCH_NOT_CERTIFIED",detail="official pregame bench empty")); continue
        plausible_L=plausible_R=0
        for pid in bench:
            if hist["role"][pid]["games"]>=10 and pid!=dhid:
                bhand=_hand(feed,pid,"batSide"); plausible_L |= bhand in ("R","S"); plausible_R |= bhand in ("L","S")
        likely=[]
        prev=date.fromisoformat(day)-timedelta(days=1); p1d=prev.isoformat(); p2d=(prev-timedelta(days=1)).isoformat(); p3d=(prev-timedelta(days=2)).isoformat()
        for pid in bullpen:
            games=hist["pitching"][pid]; p1=sum(x[1] for x in games if x[0]==p1d); p2=sum(x[1] for x in games if x[0]>=p2d); recent=[x for x in games if x[0]>=p3d]
            consecutive=p1d in {x[0] for x in games} and p2d in {x[0] for x in games}; multi=any(x[2]>=6 and x[0]>=p2d for x in games)
            if len(games)<3: category="ROLE_OR_AVAILABILITY_UNCERTAIN"
            elif p1>=30 or p2>=45 or len(recent)>=3: category="HEAVILY_USED"
            elif consecutive: category="CONSECUTIVE_DAYS"
            elif multi: category="MULTI_INNING_RECENT"
            elif p1>=15 or p2>=25 or len(recent)>=2: category="AVAILABLE_WITH_RECENT_WORKLOAD"
            else: category="LIKELY_AVAILABLE"
            if category in ("LIKELY_AVAILABLE","AVAILABLE_WITH_RECENT_WORKLOAD","MULTI_INNING_RECENT"): likely.append(pid)
        left=sum(_hand(feed,p,"pitchHand")=="L" for p in likely); right=sum(_hand(feed,p,"pitchHand")=="R" for p in likely); denom=left+right
        if not bullpen or not denom: audits.append(dict(baseaudit,status="BLOCKED_BULLPEN_NOT_CERTIFIED",detail="no certified likely-available handedness pool")); continue
        slot=order.index(dhid)+1; band="SLOT_1_3" if slot<=3 else ("SLOT_4_6" if slot<=6 else "SLOT_7_9"); ps=hist["pstarts"][dhid]; pdh=hist["pdh"][dhid]; th=hist["teamdh"][tid]
        remove_rate=hist["prem4"][dhid]/pdh if pdh else 0
        values=dict(slot_band=band,player_shrunk_ph=(hist["pph"][dhid]+global_rate*alpha)/(ps+alpha),player_shrunk_remove=(remove_rate*ps+global_rate*alpha)/(ps+alpha),team_shrunk_ph=(hist["teamph"][tid]+global_rate*alpha)/(th+alpha),batter_hand=_hand(feed,dhid,"batSide"),opposing_starter_hand=_hand(feed,starter,"pitchHand"),plausible_platoon_alternative_present_vs_L=int(plausible_L),plausible_platoon_alternative_present_vs_R=int(plausible_R),left_handed_share_likely_available=left/denom,right_handed_share_likely_available=right/denom)
        if not values["batter_hand"] or not values["opposing_starter_hand"]: audits.append(dict(baseaudit,status="BLOCKED_MISSING_REQUIRED_FEATURE",detail="missing batter or starter handedness")); continue
        features=pd.DataFrame([dict(values,interval_index=i) for i in range(4)])[artifact["feature_columns"]]; hazards=artifact["model"].predict_proba(features)[:,1]; score=1-float(np.prod(1-hazards)); ref=artifact["reference_scores_sorted"]; percentile=float(np.searchsorted(ref,score,side="right")/len(ref))
        feature_hash=hashlib.sha256(json.dumps([{k:(int(v) if isinstance(v,(np.integer,)) else float(v) if isinstance(v,(np.floating,)) else v) for k,v in row.items()} for row in features.to_dict("records")],sort_keys=True,separators=(",",":")).encode()).hexdigest()
        capture=now.isoformat().replace("+00:00","Z"); identity=f"{day}|{pk}|{tid}|{dhid}"
        row={k:"" for k in FIELDS}; row.update(canonical_identity=identity,game_date=day,game_pk=pk,player_mlb_id=dhid,team_mlb_id=tid,opponent_mlb_id=oid,batting_order=slot,batter_hand=values["batter_hand"],opposing_starter_mlb_id=starter,opposing_starter_hand=values["opposing_starter_hand"],capture_timestamp_utc=capture,scheduled_start_utc=start.isoformat().replace("+00:00","Z"),official_lineup_source="MLB_STATSAPI_FEED_LIVE_OFFICIAL_BOXSCORE",official_lineup_source_sha256=raw_sha,bench_certification_status="CURRENT_BENCH_CERTIFIED",bullpen_certification_status="CURRENT_BULLPEN_CERTIFIED",scorer_semantic_id=artifact["artifact_type"],scorer_sha256=sha256_path(load_config()["scorer_path"]),feature_schema_sha256=schema_sha,frozen_cutoff_version="PASSAGE_2D1_REFERENCE_P80",frozen_cutoff=artifact["reference_cutoff_80"],feature_vector_sha256=feature_hash,interval_hazard_0=hazards[0],interval_hazard_1=hazards[1],interval_hazard_2=hazards[2],interval_hazard_3=hazards[3],cumulative_score=score,historical_reference_percentile=percentile,forward_top20=int(score>=artifact["reference_cutoff_80"]),certification_result="FORWARD_DH_SCORER_CERTIFIED",fallback_and_data_quality_status="PERMANENT_CURRENT_DAY_ADAPTER_CERTIFIED_OPPORTUNITY_OUTPUT_UNAVAILABLE")
        rows.append(row); audits.append(dict(baseaudit,status="CAPTURED_SCORER_CERTIFIED",detail=f"score={score:.12f}"))
    return rows,audits


def run(day, force_window=False, fetch=fetch_json):
    config=load_config(); ensure_seed(config); now=datetime.now(timezone.utc); artifact=joblib.load(config["scorer_path"]); schema_sha=validate_scorer(config,artifact)
    with exclusive_lock(config["lock_dir"]/"capture.lock"):
        schedule,_=fetch(schedule_url(day)); window=capture_window_state(schedule,now,int(config["capture_window_hours_before_first_pitch"]))
        if window!="ACTIVE" and not force_window:
            return {"decision":"NO_MUTATION_"+window,"new_rows":0,"date":day}
        cache_prior_feeds(config,day,fetch); hist=build_histories(config,day); _,old=read_csv(config["prediction_ledger"]); existing={(r["game_date"],r["game_pk"],r["team_mlb_id"],r["player_mlb_id"],r["scorer_semantic_id"]) for r in old}
        new=[]; audits=[]
        for block in schedule.get("dates",[]):
            for game in block.get("games",[]):
                feed,raw=fetch(feed_url(int(game["gamePk"]))); rows,status=score_feed(feed,hashlib.sha256(raw).hexdigest(),now,day,artifact,schema_sha,hist,existing); new.extend(rows); audits.extend(status)
        admitted,duplicates=append_unique_atomic(config["prediction_ledger"],FIELDS,new,"canonical_identity",config["backup_dir"])
        config["capture_audit"].parent.mkdir(parents=True,exist_ok=True)
        with config["capture_audit"].open("w",newline="",encoding="utf-8") as handle: writer=csv.DictWriter(handle,fieldnames=AUDIT_FIELDS); writer.writeheader(); writer.writerows(audits)
        rolling=update_rolling_status(config); _,allrows=read_csv(config["prediction_ledger"])
        counts=defaultdict(int)
        for row in audits: counts[row["status"]]+=1
        return {"decision":"CAPTURE_COMPLETED","date":day,"execution_timestamp_utc":now.isoformat(),"new_rows":admitted,"cumulative_rows":len(allrows),"duplicates":duplicates,"status_counts":dict(counts),"prediction_ledger_sha256":sha256_path(config["prediction_ledger"]),"history_games":hist["games"],"evidence_status":rolling["status"],"outcome_rows_accessed":0}


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--date",default=pacific_today()); parser.add_argument("--force-window",action="store_true"); args=parser.parse_args()
    try: result=run(args.date,args.force_window); print(json.dumps(result,indent=2,sort_keys=True))
    except RuntimeError as exc:
        print(json.dumps({"decision":"CAPTURE_FAILED","error":str(exc)},sort_keys=True),file=sys.stderr); return 2
    return 0
if __name__=="__main__": raise SystemExit(main())
