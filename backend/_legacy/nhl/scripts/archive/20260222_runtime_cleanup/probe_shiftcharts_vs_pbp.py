#!/usr/bin/env python3
# backend/nhl/scripts/probe_shiftcharts_vs_pbp.py
#
# Probe: shiftcharts vs api-web play-by-play for a single game_id.
#
# Usage:
#   python backend/nhl/scripts/probe_shiftcharts_vs_pbp.py --game-id 2025020090 --verbose

import argparse
import json
import time
from typing import Any, Dict, Optional

import requests

PBP_BASE = "https://api-web.nhle.com/v1/gamecenter"
SHIFTCHARTS_URL = "https://api.nhle.com/stats/rest/en/shiftcharts"
UA = {"User-Agent": "proppadia-nhl/1.0"}


def gj(url: str, timeout: int = 20) -> Optional[Dict[str, Any]]:
    for _ in range(3):
        try:
            r = requests.get(url, timeout=timeout, headers=UA)
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(0.5)
    return None


def fetch_shiftcharts(game_id: int) -> Optional[Dict[str, Any]]:
    # NOTE: shift charts come from api.nhle.com stats REST w/ cayenneExp
    url = f"{SHIFTCHARTS_URL}?cayenneExp=gameId={game_id}"
    return gj(url)


def fetch_pbp(game_id: int) -> Optional[Dict[str, Any]]:
    return gj(f"{PBP_BASE}/{game_id}/play-by-play")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--game-id", type=int, required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    gid = args.game_id

    sc = fetch_shiftcharts(gid)
    if not isinstance(sc, dict):
        print(f"[shiftcharts] fetch failed or not-json for {gid}")
        return

    data = sc.get("data")
    if not isinstance(data, list):
        print(f"[shiftcharts] unexpected payload shape for {gid}: top_keys={list(sc.keys())}")
        if args.verbose:
            print(json.dumps(sc, indent=2)[:2000])
        return

    if len(data) == 0:
        print(f"[shiftcharts] 0 rows for {gid} (data empty)")
    else:
        # Common fields in shiftcharts rows (varies slightly):
        # playerId, teamAbbrev, period, startTime, endTime, duration, shiftNumber, etc.
        keys0 = list(data[0].keys())
        player_ids = {row.get("playerId") for row in data if isinstance(row, dict)}
        player_ids.discard(None)
        team_abbrevs = {row.get("teamAbbrev") for row in data if isinstance(row, dict)}
        team_abbrevs.discard(None)

        # If the feed includes strength/state fields, check them (best-effort)
        pp_like = 0
        for row in data:
            if not isinstance(row, dict):
                continue
            # These field names are not guaranteed—this is only a probe.
            strength = row.get("strength") or row.get("eventDescription") or row.get("typeCode")
            if isinstance(strength, str) and ("PP" in strength.upper() or "POWER" in strength.upper()):
                pp_like += 1

        print(f"[shiftcharts] rows={len(data)} players={len(player_ids)} teams={sorted(team_abbrevs)}")
        print(f"[shiftcharts] first_row_keys={keys0}")
        if args.verbose:
            print(f"[shiftcharts] pp_like_rows(best_effort)={pp_like}")
            print("[shiftcharts] sample_row:")
            print(json.dumps(data[0], indent=2))

    pbp = fetch_pbp(gid)
    if not isinstance(pbp, dict):
        print(f"[pbp] fetch failed or not-json for {gid}")
        return

    plays = pbp.get("plays") or []
    if not isinstance(plays, list):
        print(f"[pbp] unexpected payload shape for {gid}: top_keys={list(pbp.keys())}")
        if args.verbose:
            print(json.dumps(pbp, indent=2)[:2000])
        return

    # Quick PBP summary
    situation_codes = set()
    for ev in plays:
        if isinstance(ev, dict):
            scode = ev.get("situationCode")
            if isinstance(scode, str):
                situation_codes.add(scode)

    print(f"[pbp] plays={len(plays)} unique_situationCodes={sorted(situation_codes)}")
    if args.verbose and plays:
        print("[pbp] sample_play_keys:", list(plays[0].keys()))
        d0 = plays[0].get("details")
        if isinstance(d0, dict):
            print("[pbp] sample_play_details_keys:", list(d0.keys()))
        else:
            print("[pbp] sample_play_details_keys: []")


if __name__ == "__main__":
    main()
