#!/usr/bin/env python3
from __future__ import annotations

import argparse, hashlib, json, os, sys
from collections import Counter
from datetime import datetime, timedelta, timezone

from backend.mlb.scripts.dh_forward_automation_common import (
    PACIFIC, append_unique_atomic, exclusive_lock, feed_url, fetch_json, load_config,
    read_csv, sha256_path, update_rolling_status,
)

FIELDS = "canonical_identity game_date game_pk player_mlb_id team_mlb_id opponent_mlb_id grading_timestamp_utc grading_status official_source official_source_sha256 original_dh_plate_appearances original_dh_hits reached_fourth_pa reached_fifth_pa hits_o15_outcome completed_as_dh pinch_hit_before_fourth_pa removal_type replacement_player_mlb_id replacement_player_plate_appearances replacement_player_hits detail".split()
LINEAGE_FIELDS = "canonical_identity game_date game_pk grading_timestamp_utc source_lineage_state recorded_source_sha256 retained_source_sha256 retained_source_path detail".split()


def retained_source(config, game_pk, fetch, run_tag):
    """Return one immutable, byte-identical official source for this game/run."""
    cache = config["prior_feed_cache"] / f"{int(game_pk)}.json"
    if cache.exists():
        raw = cache.read_bytes(); feed = json.loads(raw)
        if feed.get("gameData", {}).get("status", {}).get("abstractGameState") == "Final":
            return feed, raw, cache, "CERTIFIED_EXISTING_CACHE"
    feed, raw = fetch(feed_url(game_pk))
    digest = hashlib.sha256(raw).hexdigest()
    target = config["immutable_grade_sources"] / run_tag / f"game_{int(game_pk)}_{digest}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        tmp = target.with_suffix(".tmp"); tmp.write_bytes(raw); os.replace(tmp, target)
    retained = target.read_bytes()
    if hashlib.sha256(retained).hexdigest() != digest: raise RuntimeError("GRADE_SOURCE_RETENTION_HASH_MISMATCH")
    return json.loads(retained), retained, target, "IMMUTABLE_LIVE_RESPONSE_RETAINED"


def repair_existing_lineage_sidecar(config):
    """Document legacy rows without mutating either accepted ledger."""
    _, outcomes = read_csv(config["outcome_ledger"]); rows=[]
    for row in outcomes:
        cache=config["prior_feed_cache"]/f"{int(row['game_pk'])}.json"
        retained_hash=sha256_path(cache) if cache.exists() else ""
        recorded=row.get("official_source_sha256","")
        state="CERTIFIED_EXISTING_CACHE" if retained_hash and retained_hash==recorded else "RAW_RESPONSE_NOT_RETAINED"
        rows.append({"canonical_identity":row["canonical_identity"],"game_date":row["game_date"],"game_pk":row["game_pk"],"grading_timestamp_utc":row["grading_timestamp_utc"],"source_lineage_state":state,"recorded_source_sha256":recorded,"retained_source_sha256":retained_hash,"retained_source_path":str(cache),"detail":"legacy row exact retained bytes verified" if state.startswith("CERTIFIED") else "recorded live-response hash differs from retained cache; original response cannot be recreated"})
    return append_unique_atomic(config["outcome_source_lineage_ledger"],LINEAGE_FIELDS,rows,"canonical_identity",config["backup_dir"])


def classify(feed, prediction):
    pk=int(prediction["game_pk"]); pid=int(prediction["player_mlb_id"]); tid=int(prediction["team_mlb_id"])
    state=feed.get("gameData",{}).get("status",{}); coded=state.get("codedGameState",""); abstract=state.get("abstractGameState","")
    if coded in ("D","P") or "postpon" in state.get("detailedState","").lower(): return "GAME_POSTPONED",{}
    if abstract!="Final": return "OFFICIAL_RESULT_UNAVAILABLE",{}
    box=feed.get("liveData",{}).get("boxscore",{}); team=None
    for side in ("away","home"):
        if int(feed["gameData"]["teams"][side]["id"])==tid: team=box.get("teams",{}).get(side,{})
    if not team: return "IDENTITY_UNRESOLVED",{}
    row=team.get("players",{}).get("ID"+str(pid))
    if not row: return "IDENTITY_UNRESOLVED",{}
    bat=row.get("stats",{}).get("batting",{}); pa=int(bat.get("plateAppearances") or 0); hits=int(bat.get("hits") or 0)
    removal="COMPLETED_AS_DH"; replacement=None
    for play in feed.get("liveData",{}).get("plays",{}).get("allPlays",[]):
        for event in play.get("playEvents",[]):
            details=event.get("details",{})
            if not event.get("isSubstitution") or details.get("replacedPlayer",{}).get("id")!=pid: continue
            replacement=details.get("player",{}).get("id") or event.get("player",{}).get("id")
            pos=details.get("position",{}).get("abbreviation",""); desc=details.get("description","").lower()
            if pos=="PH" or "pinch hit" in desc: removal="PINCH_HIT_REMOVAL"
            elif pos=="PR" or "pinch-run" in desc or "pinch run" in desc: removal="PINCH_RUN_REMOVAL"
            elif "remain" in desc or (pos and pos not in ("PH","PR","DH")): removal="MOVED_TO_FIELD"
            else: removal="OTHER_REMOVAL"
            break
    status={"COMPLETED_AS_DH":"RESOLVED_COMPLETED","PINCH_HIT_REMOVAL":"RESOLVED_PINCH_HIT_REMOVAL","PINCH_RUN_REMOVAL":"RESOLVED_PINCH_RUN_REMOVAL","MOVED_TO_FIELD":"RESOLVED_MOVED_TO_FIELD","OTHER_REMOVAL":"RESOLVED_OTHER_REMOVAL"}[removal]
    repl_pa=repl_hits=""
    if replacement:
        rr=team.get("players",{}).get("ID"+str(replacement),{}).get("stats",{}).get("batting",{}); repl_pa=int(rr.get("plateAppearances") or 0); repl_hits=int(rr.get("hits") or 0)
    return status,{"original_dh_plate_appearances":pa,"original_dh_hits":hits,"reached_fourth_pa":int(pa>=4),"reached_fifth_pa":int(pa>=5),"hits_o15_outcome":"WIN" if hits>=2 else "LOSS","completed_as_dh":int(removal=="COMPLETED_AS_DH"),"pinch_hit_before_fourth_pa":int(removal=="PINCH_HIT_REMOVAL" and pa<4),"removal_type":removal,"replacement_player_mlb_id":replacement or "","replacement_player_plate_appearances":repl_pa,"replacement_player_hits":repl_hits}


def run(day=None, fetch=fetch_json):
    config=load_config(); _,predictions=read_csv(config["prediction_ledger"]); _,graded=read_csv(config["outcome_ledger"]); existing={r["canonical_identity"] for r in graded}
    if day is None: day=(datetime.now(PACIFIC).date()-timedelta(days=1)).isoformat()
    targets=[r for r in predictions if r["game_date"]==day and r["canonical_identity"] not in existing]
    with exclusive_lock(config["lock_dir"]/"grade.lock"):
        now=datetime.now(timezone.utc); run_tag=now.strftime("grade_%Y%m%dT%H%M%S%fZ"); feeds={}; rows=[]; lineage=[]
        for prediction in targets:
            pk=int(prediction["game_pk"])
            if pk not in feeds: feeds[pk]=retained_source(config,pk,fetch,run_tag)
            feed,raw,source_path,lineage_state=feeds[pk]; status,values=classify(feed,prediction)
            if status=="OFFICIAL_RESULT_UNAVAILABLE": continue
            digest=hashlib.sha256(raw).hexdigest()
            row={k:"" for k in FIELDS}; row.update(canonical_identity=prediction["canonical_identity"],game_date=prediction["game_date"],game_pk=pk,player_mlb_id=prediction["player_mlb_id"],team_mlb_id=prediction["team_mlb_id"],opponent_mlb_id=prediction["opponent_mlb_id"],grading_timestamp_utc=now.isoformat(),grading_status=status,official_source="MLB_STATSAPI_FEED_LIVE_FINAL",official_source_sha256=digest,detail="official final only"); row.update(values); rows.append(row)
            lineage.append({"canonical_identity":prediction["canonical_identity"],"game_date":prediction["game_date"],"game_pk":pk,"grading_timestamp_utc":now.isoformat(),"source_lineage_state":lineage_state,"recorded_source_sha256":digest,"retained_source_sha256":digest,"retained_source_path":str(source_path.relative_to(config["outcome_ledger"].parents[5])),"detail":"exact retained bytes used for grading"})
        admitted,duplicates=append_unique_atomic(config["outcome_ledger"],FIELDS,rows,"canonical_identity",config["backup_dir"])
        append_unique_atomic(config["outcome_source_lineage_ledger"],LINEAGE_FIELDS,lineage,"canonical_identity",config["backup_dir"])
        rolling=update_rolling_status(config); _,all_outcomes=read_csv(config["outcome_ledger"])
        return {"decision":"GRADING_COMPLETED","date":day,"new_outcomes":admitted,"cumulative_outcomes":len(all_outcomes),"duplicates":duplicates,"statuses":dict(Counter(r["grading_status"] for r in rows)),"outcome_ledger_sha256":sha256_path(config["outcome_ledger"]),"evidence_status":rolling["status"]}


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--date"); parser.add_argument("--repair-lineage-sidecar",action="store_true"); args=parser.parse_args()
    if args.repair_lineage_sidecar:
        admitted,duplicates=repair_existing_lineage_sidecar(load_config()); print(json.dumps({"decision":"LINEAGE_SIDECAR_REPAIRED","admitted":admitted,"duplicates":duplicates},sort_keys=True)); return 0
    try: print(json.dumps(run(args.date),indent=2,sort_keys=True))
    except RuntimeError as exc: print(json.dumps({"decision":"GRADING_FAILED","error":str(exc)},sort_keys=True),file=sys.stderr); return 2
    return 0
if __name__=="__main__": raise SystemExit(main())
