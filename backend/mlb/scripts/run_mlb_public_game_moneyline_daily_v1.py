#!/usr/bin/env python3
"""Dry-run by default: advance, score, persist, and grade public moneylines."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

from backend.mlb.public_game_predictions.durable_store_v1 import (
    append_official_finals, append_prediction_rows, append_state_snapshot,
    load_official_finals_before,
)
from backend.mlb.public_game_predictions.pythagorean_log5_v1 import score_schedule_payload
from backend.mlb.public_game_predictions.state_v1 import OfficialFinalGame, reconstruct_state

STATSAPI = "https://statsapi.mlb.com/api/v1/schedule"


def _fetch_schedule(start_date: str, end_date: str) -> tuple[dict, bytes]:
    query=urlencode({'sportId':1,'startDate':start_date,'endDate':end_date,'hydrate':'status,linescore,team'})
    with urlopen(f'{STATSAPI}?{query}',timeout=30) as response:  # nosec B310: fixed official MLB host
        raw=response.read()
    return json.loads(raw),raw


def _games(payload: dict):
    for block in payload.get('dates') or []:
        yield from block.get('games') or []


def official_finals_from_schedule(payload: dict, *, observed_at_utc: str,
                                  source_identity: str, source_sha256: str) -> list[OfficialFinalGame]:
    rows=[]
    for game in _games(payload):
        status=str((game.get('status') or {}).get('abstractGameState') or '')
        detailed=str((game.get('status') or {}).get('detailedState') or '')
        if status!='Final' and detailed not in {'Final','Game Over'}:
            continue
        teams=game.get('teams') or {}
        home,away=teams.get('home') or {},teams.get('away') or {}
        rows.append(OfficialFinalGame(
            game_pk=int(game['gamePk']),game_date=str(game['officialDate']),
            scheduled_start_utc=str(game['gameDate']),game_number=int(game.get('gameNumber') or 1),
            home_team_id=int(home['team']['id']),away_team_id=int(away['team']['id']),
            home_runs=int(home['score']),away_runs=int(away['score']),official_status='Final',
            observed_final_at_utc=observed_at_utc,source_identity=source_identity,source_sha256=source_sha256,
        ))
    return rows


def main() -> int:
    parser=argparse.ArgumentParser()
    parser.add_argument('--mlb-date',default=date.today().isoformat())
    parser.add_argument('--prediction-cutoff-utc',required=True)
    parser.add_argument('--schedule-json',type=Path)
    parser.add_argument('--finals-json',type=Path)
    parser.add_argument('--write-durable',action='store_true')
    parser.add_argument('--output-json',type=Path)
    args=parser.parse_args()
    now=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
    if args.schedule_json:
        schedule_raw=args.schedule_json.read_bytes();schedule=json.loads(schedule_raw)
    else:
        schedule,schedule_raw=_fetch_schedule(args.mlb_date,args.mlb_date)
    schedule_hash=hashlib.sha256(schedule_raw).hexdigest()
    if args.finals_json:
        finals_payload=json.loads(args.finals_json.read_text());finals_raw=args.finals_json.read_bytes()
        finals=official_finals_from_schedule(finals_payload,observed_at_utc=now,
                                              source_identity=str(args.finals_json),
                                              source_sha256=hashlib.sha256(finals_raw).hexdigest())
    elif args.write_durable:
        history,history_raw=_fetch_schedule('2026-08-05',args.mlb_date)
        finals=official_finals_from_schedule(history,observed_at_utc=now,
                                              source_identity=f'{STATSAPI}:2026-08-05:{args.mlb_date}',
                                              source_sha256=hashlib.sha256(history_raw).hexdigest())
        append_official_finals(finals)
        finals=load_official_finals_before(args.prediction_cutoff_utc)
    else:
        finals=[]
    snapshot=reconstruct_state(finals,prediction_cutoff_utc=args.prediction_cutoff_utc,
                               state_generated_at_utc=min(now,args.prediction_cutoff_utc))
    rows=score_schedule_payload(schedule,prediction_timestamp_utc=args.prediction_cutoff_utc,
                                source_schedule_hash=schedule_hash,team_state_snapshot=snapshot)
    admitted=[row for row in rows if row['admission_status']=='ADMITTED_SHADOW']
    state_written=predictions_written=0
    if args.write_durable:
        state_written=int(append_state_snapshot(snapshot))
        predictions_written=append_prediction_rows(admitted)
    result={'mode':'DURABLE_WRITE' if args.write_durable else 'DRY_RUN','mlb_date':args.mlb_date,
            'prediction_generated_at_utc':args.prediction_cutoff_utc,
            'prediction_cutoff_utc':args.prediction_cutoff_utc,'source_schedule_hash':schedule_hash,
            'state_hash':snapshot['state_hash'],'state_through_game_date':snapshot['state_through_game_date'],
            'official_finals_considered':len(finals),'games_newly_applied':len(snapshot['applied_game_ids']),
            'unresolved_games':snapshot['unresolved_games'],'rows':rows,'admitted':len(admitted),
            'state_snapshot_written':state_written,'predictions_written':predictions_written,'outcomes_accessed':0}
    text=json.dumps(result,indent=2)
    if args.output_json: args.output_json.write_text(text+'\n')
    print(text)
    return 0


if __name__=='__main__': raise SystemExit(main())
