#!/usr/bin/env python3
"""Dry-run by default: advance, score, persist, and grade public moneylines."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import date, datetime, timedelta, timezone
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
GAME_FEED = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
DEFAULT_RETAINED_SOURCE_DIR = Path("artifacts/ops/mlb_public_game_moneyline_sources")


def _fetch_schedule(start_date: str, end_date: str) -> tuple[dict, bytes]:
    query=urlencode({'sportId':1,'startDate':start_date,'endDate':end_date,'hydrate':'status,linescore,team'})
    with urlopen(f'{STATSAPI}?{query}',timeout=30) as response:  # nosec B310: fixed official MLB host
        raw=response.read()
    return json.loads(raw),raw


def _fetch_game_feed(game_pk: int) -> tuple[dict, bytes]:
    with urlopen(GAME_FEED.format(game_pk=int(game_pk)),timeout=30) as response:  # nosec B310
        raw=response.read()
    return json.loads(raw),raw


def _utc_text(value: str) -> str:
    return datetime.fromisoformat(str(value).replace('Z','+00:00')).astimezone(timezone.utc).isoformat().replace('+00:00','Z')


def _final_effective_utc(feed: dict) -> tuple[str,str]:
    plays=((feed.get('liveData') or {}).get('plays') or {}).get('allPlays') or []
    for play in reversed(plays):
        about=play.get('about') or {}
        for field in ('endTime','startTime'):
            if about.get(field):
                return _utc_text(about[field]),f'OFFICIAL_LAST_PLAY_{field.upper()}'
    start=(((feed.get('gameData') or {}).get('datetime') or {}).get('dateTime'))
    if not start:
        raise ValueError('FINAL_EFFECTIVE_TIME_UNAVAILABLE')
    # Conservative deterministic fallback: no same-day game is admitted until
    # twelve hours after its official scheduled start when play chronology is absent.
    fallback=datetime.fromisoformat(str(start).replace('Z','+00:00')).astimezone(timezone.utc)+timedelta(hours=12)
    return fallback.isoformat().replace('+00:00','Z'),'SCHEDULED_START_PLUS_12H_CONSERVATIVE_FALLBACK'


def _retain_raw(raw: bytes, *, game_pk: int, root: Path) -> Path:
    digest=hashlib.sha256(raw).hexdigest(); target=root/str(int(game_pk))/f'{digest}.json'
    target.parent.mkdir(parents=True,exist_ok=True)
    if not target.exists():
        tmp=target.with_suffix('.tmp');tmp.write_bytes(raw);os.replace(tmp,target)
    if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:
        raise RuntimeError('RETAINED_FINAL_SOURCE_HASH_MISMATCH')
    return target


def _games(payload: dict):
    for block in payload.get('dates') or []:
        yield from block.get('games') or []


def official_final_from_feed(feed: dict, *, observed_at_utc: str,
                             source_identity: str, source_sha256: str) -> OfficialFinalGame:
    game_data=feed.get('gameData') or {}; status=game_data.get('status') or {}
    if status.get('abstractGameState')!='Final' and status.get('detailedState') not in {'Final','Game Over'}:
        raise ValueError('OFFICIAL_GAME_NOT_FINAL')
    lines=((feed.get('liveData') or {}).get('linescore') or {}).get('teams') or {}
    home,away=(game_data.get('teams') or {}).get('home') or {},(game_data.get('teams') or {}).get('away') or {}
    effective,_=_final_effective_utc(feed)
    return OfficialFinalGame(
        game_pk=int(feed['gamePk']),game_date=str((game_data.get('datetime') or {}).get('officialDate')),
        scheduled_start_utc=str((game_data.get('datetime') or {}).get('dateTime')),
        game_number=int((game_data.get('game') or {}).get('gameNumber') or 1),
        home_team_id=int(home['id']),away_team_id=int(away['id']),
        home_runs=int((lines.get('home') or {})['runs']),away_runs=int((lines.get('away') or {})['runs']),
        official_status='Final',official_final_effective_utc=effective,
        observed_final_at_utc=observed_at_utc,source_identity=source_identity,source_sha256=source_sha256,
    )


def official_finals_from_schedule(payload: dict, *, retained_source_dir: Path) -> list[OfficialFinalGame]:
    rows=[]
    for game in _games(payload):
        status=str((game.get('status') or {}).get('abstractGameState') or '')
        detailed=str((game.get('status') or {}).get('detailedState') or '')
        if status!='Final' and detailed not in {'Final','Game Over'}:
            continue
        observed=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
        feed,raw=_fetch_game_feed(int(game['gamePk']))
        retained=_retain_raw(raw,game_pk=int(game['gamePk']),root=retained_source_dir)
        rows.append(official_final_from_feed(feed,observed_at_utc=observed,
                                             source_identity=str(retained),
                                             source_sha256=hashlib.sha256(raw).hexdigest()))
    return rows


def main() -> int:
    parser=argparse.ArgumentParser()
    parser.add_argument('--mlb-date',default=date.today().isoformat())
    parser.add_argument('--prediction-cutoff-utc',default='auto')
    parser.add_argument('--schedule-json',type=Path)
    parser.add_argument('--finals-json',type=Path)
    parser.add_argument('--write-durable',action='store_true')
    parser.add_argument('--retained-source-dir',type=Path,default=DEFAULT_RETAINED_SOURCE_DIR)
    parser.add_argument('--output-json',type=Path)
    args=parser.parse_args()
    if args.schedule_json:
        schedule_raw=args.schedule_json.read_bytes();schedule=json.loads(schedule_raw)
    else:
        schedule,schedule_raw=_fetch_schedule(args.mlb_date,args.mlb_date)
    schedule_hash=hashlib.sha256(schedule_raw).hexdigest()
    inserted_finals=canonical_duplicates=0
    if args.finals_json:
        finals_payload=json.loads(args.finals_json.read_text())
        finals=official_finals_from_schedule(finals_payload,retained_source_dir=args.retained_source_dir)
    elif args.write_durable:
        history,history_raw=_fetch_schedule('2026-08-05',args.mlb_date)
        finals=official_finals_from_schedule(history,retained_source_dir=args.retained_source_dir)
        inserted_finals=append_official_finals(finals);canonical_duplicates=len(finals)-inserted_finals
    else:
        finals=[]
    cutoff=(datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
            if str(args.prediction_cutoff_utc).lower()=='auto' else _utc_text(args.prediction_cutoff_utc))
    generated=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
    if args.write_durable:
        finals=load_official_finals_before(cutoff)
    snapshot=reconstruct_state(finals,prediction_cutoff_utc=cutoff,state_generated_at_utc=generated)
    rows=score_schedule_payload(schedule,prediction_timestamp_utc=cutoff,
                                source_schedule_hash=schedule_hash,team_state_snapshot=snapshot)
    admitted=[row for row in rows if row['admission_status']=='ADMITTED_SHADOW']
    state_written=predictions_written=0
    if args.write_durable:
        state_written=int(append_state_snapshot(snapshot))
        predictions_written=append_prediction_rows(admitted)
    result={'mode':'DURABLE_WRITE' if args.write_durable else 'DRY_RUN','mlb_date':args.mlb_date,
            'prediction_generated_at_utc':generated,
            'prediction_cutoff_utc':cutoff,'source_schedule_hash':schedule_hash,
            'state_hash':snapshot['state_hash'],'state_through_game_date':snapshot['state_through_game_date'],
            'official_finals_considered':len(finals),'games_newly_applied':len(snapshot['applied_game_ids']),
            'official_finals_inserted':inserted_finals,'canonical_final_duplicates':canonical_duplicates,
            'genuine_corrections':0,'unresolved_games':snapshot['unresolved_games'],'rows':rows,'admitted':len(admitted),
            'state_snapshot_written':state_written,'predictions_written':predictions_written,'outcomes_accessed':0}
    text=json.dumps(result,indent=2)
    if args.output_json: args.output_json.write_text(text+'\n')
    print(text)
    return 0


if __name__=='__main__': raise SystemExit(main())
