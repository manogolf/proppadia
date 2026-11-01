#!/usr/bin/env python3
"""Fetch official NHL regular-season skater (or team) totals for a season (research input).

Source: the NHL stats API skater summary
(``api.nhle.com/stats/rest/en/skater/summary``, gameTypeId 2), paged. Used for
prior-season points per game where the repository has no game-level official
outcomes (2022-23; P2.39 own-rate research). Raw pages are kept with their
SHA-256; the totals CSV holds player_id, games, goals, assists, points and
position. No database access. Retries follow the schedule importer's session.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = "https://api.nhle.com/stats/rest/en/skater/summary"
TEAM_URL = "https://api.nhle.com/stats/rest/en/team/summary"
PAGE = 100


def session() -> requests.Session:
    retry = Retry(total=5, connect=5, read=5, backoff_factor=0.5,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=frozenset({"GET"}), raise_on_status=False)
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-nhl-research"})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def fetch(season_id: str, out: Path) -> pd.DataFrame:
    raw = out / f"raw_season={season_id}"
    raw.mkdir(parents=True, exist_ok=False)
    s, rows, start, total, pages = session(), [], 0, None, []
    while total is None or start < total:
        params = {"isAggregate": "false", "isGame": "false", "start": start, "limit": PAGE,
                  "sort": '[{"property":"playerId","direction":"ASC"}]',
                  "cayenneExp": f"gameTypeId=2 and seasonId={season_id}"}
        r = s.get(URL, params=params, timeout=30)
        r.raise_for_status()
        path = raw / f"page_{start:05d}.json"
        path.write_bytes(r.content)
        body = r.json()
        total = int(body["total"])
        rows.extend(body["data"])
        pages.append({"start": start, "rows": len(body["data"]), "sha256": hashlib.sha256(r.content).hexdigest()})
        if not body["data"]:
            break
        start += PAGE
    frame = pd.DataFrame(rows)
    if len(frame) != total or frame.playerId.duplicated().any():
        raise RuntimeError(f"SKATER_SUMMARY_INCOMPLETE:{len(frame)}/{total}")
    totals = pd.DataFrame({"player_id": frame.playerId.astype("int64"), "season_id": season_id,
                           "games": frame.gamesPlayed.astype(int), "goals": frame.goals.astype(int),
                           "assists": frame.assists.astype(int), "points": frame.points.astype(int),
                           "position": frame.positionCode, "name": frame.skaterFullName})
    path = out / f"skater_totals_season={season_id}.csv"
    totals.to_csv(path, index=False)
    (raw / "manifest.json").write_text(json.dumps({
        "source": URL, "season_id": season_id, "game_type_id": 2, "fetched_utc": datetime.now(timezone.utc).isoformat(),
        "total": total, "pages": pages, "totals_csv_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}, indent=2) + "\n")
    return totals


def fetch_teams(season_id: str, out: Path) -> pd.DataFrame:
    """Team regular-season totals (goals and shots for and against) for one season."""
    raw = out / f"raw_team_season={season_id}"
    raw.mkdir(parents=True, exist_ok=False)
    r = session().get(TEAM_URL, params={"isAggregate": "false", "isGame": "false", "start": 0, "limit": 100,
                                        "cayenneExp": f"gameTypeId=2 and seasonId={season_id}"}, timeout=30)
    r.raise_for_status()
    (raw / "page_00000.json").write_bytes(r.content)
    body = r.json()
    frame = pd.DataFrame(body["data"])
    if len(frame) != int(body["total"]) or frame.teamId.duplicated().any():
        raise RuntimeError("TEAM_SUMMARY_INCOMPLETE")
    games = frame.gamesPlayed.astype(int)
    totals = pd.DataFrame({"team_id": frame.teamId.astype("int64"), "season_id": season_id, "games": games,
                           "goals_for": (frame.goalsForPerGame * games).round().astype(int),
                           "goals_against": (frame.goalsAgainstPerGame * games).round().astype(int),
                           "shots_for": (frame.shotsForPerGame * games).round().astype(int),
                           "shots_against": (frame.shotsAgainstPerGame * games).round().astype(int),
                           "name": frame.teamFullName})
    path = out / f"team_totals_season={season_id}.csv"
    totals.to_csv(path, index=False)
    (raw / "manifest.json").write_text(json.dumps({
        "source": TEAM_URL, "season_id": season_id, "fetched_utc": datetime.now(timezone.utc).isoformat(),
        "total": int(body["total"]), "page_sha256": hashlib.sha256(r.content).hexdigest(),
        "totals_csv_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}, indent=2) + "\n")
    return totals


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season-id", action="append", required=True, help="e.g. 20222023; repeatable")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--teams", action="store_true", help="team totals instead of skater totals")
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for season_id in args.season_id:
        if args.teams:
            t = fetch_teams(season_id, args.out)
            print(season_id, len(t), "teams", int(t.goals_for.sum()), "goals for", int(t.games.sum()), "team games")
            continue
        t = fetch(season_id, args.out)
        print(season_id, len(t), "skaters", int(t.points.sum()), "points", int(t.games.sum()), "games")


if __name__ == "__main__":
    main()
