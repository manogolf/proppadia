#!/usr/bin/env python3
"""Retain official NHL team-level boxscores for the Oct. 2026 SOG lineage audit.

This is a bounded, free NHL Gamecenter acquisition. It reads only the retained
Oct. 7-13 canonical outcomes to discover needed game IDs and never contacts odds
providers or writes to the production database.
"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15"
RAW = OUT / "official_boxscore_reconciliation_raw/season=2026"
OUTCOME_ROOT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
DATES = {f"2026-10-{d:02d}" for d in range(7, 14)}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def retained_game_ids() -> list[int]:
    import pandas as pd

    files = [p for p in OUTCOME_ROOT.glob("2026-10-*/reconciliation=*/canonical_skater_outcomes.csv")
             if p.parent.parent.name in DATES]
    if not files:
        raise ValueError("NO_RETAINED_OPERATIONAL_OUTCOME_SOURCES")
    ids = set()
    for path in files:
        frame = pd.read_csv(path, usecols=["game_id", "official_final"])
        final = frame.official_final.astype(str).str.lower().isin({"true", "1"})
        ids.update(map(int, frame.loc[final, "game_id"].dropna().unique()))
    return sorted(ids)


def parse_boxscore(raw: bytes, expected_game_id: int) -> dict:
    data = json.loads(raw)
    if int(data.get("id", -1)) != expected_game_id:
        raise ValueError("OFFICIAL_BOXSCORE_GAME_ID_MISMATCH")
    if int(data.get("gameType", -1)) != 2:
        raise ValueError("OFFICIAL_BOXSCORE_NOT_REGULAR_SEASON")
    if data.get("gameState") not in {"OFF", "FINAL"}:
        raise ValueError("OFFICIAL_BOXSCORE_NOT_FINAL")
    teams = {}
    for side in ("home", "away"):
        team = data.get(f"{side}Team", {})
        if team.get("id") is None or team.get("sog") is None:
            raise ValueError(f"OFFICIAL_TEAM_SOG_MISSING:{side}")
        teams[side] = {"team_id": int(team["id"]), "team_sog": int(team["sog"])}
    return {"game_id": expected_game_id, "game_date": data.get("gameDate"),
            "start_time_utc": data.get("startTimeUTC"), "game_state": data.get("gameState"),
            "home": teams["home"], "away": teams["away"]}


def fetch_one(game_id: int, retries: int = 4) -> dict:
    endpoint = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
    target = RAW / f"game={game_id}" / "boxscore.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raw = target.read_bytes()
        record = parse_boxscore(raw, game_id)
        return {**record, "endpoint": endpoint, "method": "RETAINED_LOCAL_REUSE",
                "request_timestamp_utc": None, "http_status": None, "bytes": len(raw),
                "sha256": sha(raw), "raw_path": str(target.relative_to(ROOT))}
    last_error = None
    for attempt in range(1, retries + 1):
        requested_at = datetime.now(timezone.utc).isoformat()
        req = Request(endpoint, headers={"User-Agent": "Proppadia-NHL-lineage-audit/1.0"})
        try:
            with urlopen(req, timeout=30) as response:
                raw = response.read()
                status = int(response.status)
            record = parse_boxscore(raw, game_id)
            target.write_bytes(raw)
            return {**record, "endpoint": endpoint, "method": "OFFICIAL_FREE_NHL_GAMECENTER",
                    "request_timestamp_utc": requested_at, "http_status": status,
                    "attempt": attempt, "bytes": len(raw), "sha256": sha(raw),
                    "raw_path": str(target.relative_to(ROOT))}
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            last_error = f"{type(exc).__name__}:{exc}"
            if attempt < retries:
                time.sleep(min(2 ** (attempt - 1), 8))
    raise RuntimeError(f"NHL_GAMECENTER_FETCH_FAILED:{game_id}:{last_error}")


def main() -> None:
    game_ids = retained_game_ids()
    records = [fetch_one(game_id) for game_id in game_ids]
    manifest = {"schema_version": "NHL_POINTS_TEAM_SHOTS_OFFICIAL_BOXSCORE_ACQUISITION_V1",
        "provider": "NHL_OFFICIAL_GAMECENTER", "scope": "completed regular-season games 2026-10-07 through 2026-10-13",
        "paid_credits": 0, "production_database_mutations": 0, "game_count": len(records),
        "game_ids": game_ids, "team_game_rows": len(records) * 2, "responses": records,
        "completed_at_utc": datetime.now(timezone.utc).isoformat()}
    (OUT / "official_boxscore_acquisition_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True)+"\n")
    print(json.dumps({k: manifest[k] for k in ("provider", "game_count", "team_game_rows", "paid_credits")}, sort_keys=True))


if __name__ == "__main__":
    main()
