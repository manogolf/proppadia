#!/usr/bin/env python3
"""Hydrate broad research context onto Expanded O1.5 Universe rows.

Research only. This script rewrites expanded_o15_universe_rows.csv with
additional same-date/no-future context fields and emits coverage/audit files.
It does not change prices, outcomes, grading, uploads, selectors, or identity
keys.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.mlb.scripts import build_mlb_o15_manual_unified_board_universe as manual
from backend.mlb.shared.team_name_map import getFullTeamAbbreviationFromID, normalizeTeamAbbreviation
from backend.mlb.shared.time_utils_backend import get_time_of_day_bucket_et

try:
    from backend.shared.db.pg import pg_fetchall
except Exception:  # pragma: no cover - import can fail in docs-only envs
    pg_fetchall = None  # type: ignore[assignment]


DEFAULT_ROWS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")
DEFAULT_SLATE_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_ALTERNATE_BACKFILL_ROOT = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")

TEAM_ALIASES = {
    "ARIZONA DIAMONDBACKS": "ARI",
    "AZ": "ARI",
    "ARI": "ARI",
    "ATLANTA BRAVES": "ATL",
    "ATL": "ATL",
    "BALTIMORE ORIOLES": "BAL",
    "BAL": "BAL",
    "BOSTON RED SOX": "BOS",
    "BOS": "BOS",
    "CHICAGO CUBS": "CHC",
    "CHC": "CHC",
    "CHICAGO WHITE SOX": "CWS",
    "CHW": "CWS",
    "CWS": "CWS",
    "CINCINNATI REDS": "CIN",
    "CIN": "CIN",
    "CLEVELAND GUARDIANS": "CLE",
    "CLE": "CLE",
    "COLORADO ROCKIES": "COL",
    "COL": "COL",
    "DETROIT TIGERS": "DET",
    "DET": "DET",
    "HOUSTON ASTROS": "HOU",
    "HOU": "HOU",
    "KANSAS CITY ROYALS": "KC",
    "KCR": "KC",
    "KC": "KC",
    "LOS ANGELES ANGELS": "LAA",
    "LAA": "LAA",
    "LOS ANGELES DODGERS": "LAD",
    "LA": "LAD",
    "LAD": "LAD",
    "MIAMI MARLINS": "MIA",
    "MIA": "MIA",
    "MILWAUKEE BREWERS": "MIL",
    "MIL": "MIL",
    "MINNESOTA TWINS": "MIN",
    "MIN": "MIN",
    "NEW YORK METS": "NYM",
    "NYN": "NYM",
    "NYM": "NYM",
    "NEW YORK YANKEES": "NYY",
    "NYA": "NYY",
    "NYY": "NYY",
    "ATHLETICS": "ATH",
    "OAKLAND ATHLETICS": "ATH",
    "OAK": "ATH",
    "ATH": "ATH",
    "PHILADELPHIA PHILLIES": "PHI",
    "PHI": "PHI",
    "PITTSBURGH PIRATES": "PIT",
    "PIT": "PIT",
    "SAN DIEGO PADRES": "SD",
    "SDP": "SD",
    "SD": "SD",
    "SAN FRANCISCO GIANTS": "SF",
    "SFG": "SF",
    "SF": "SF",
    "SEATTLE MARINERS": "SEA",
    "SEA": "SEA",
    "ST. LOUIS CARDINALS": "STL",
    "ST LOUIS CARDINALS": "STL",
    "STL": "STL",
    "TAMPA BAY RAYS": "TB",
    "TBR": "TB",
    "TB": "TB",
    "TEXAS RANGERS": "TEX",
    "TEX": "TEX",
    "TORONTO BLUE JAYS": "TOR",
    "TOR": "TOR",
    "WASHINGTON NATIONALS": "WSH",
    "WSN": "WSH",
    "WSH": "WSH",
}

CONTEXT_FIELDS = [
    "game_id",
    "game_time",
    "time_of_day_bucket",
    "game_day_of_week",
    "park",
    "venue",
    "is_home",
    "home_away",
    "batting_order",
    "lineup_slot",
    "lineup_slot_bucket",
    "team_d7_runs_per_game",
    "team_d7_hits_per_game",
    "team_d7_total_bases_per_game",
    "team_d7_hits_runs_rbis_per_game",
    "team_d15_runs_per_game",
    "team_d15_hits_per_game",
    "team_d15_total_bases_per_game",
    "team_d15_hits_runs_rbis_per_game",
    "same_game_teammate_tier_a_count",
    "same_game_team_o15_candidate_count",
    "same_game_teammate_o15_candidate_count",
    "lineup_heat_cluster",
    "previous_team_game_time",
    "previous_team_time_of_day_bucket",
    "team_time_sequence_bucket",
    "day_after_night",
    "short_turnaround",
    "rest_day_before_game",
    "hours_since_previous_team_game",
    "context_source",
    "team_context_source",
    "rest_context_source",
    "lineup_context_source",
]

COVERAGE_FIELDS = [
    "game_id",
    "game_time",
    "time_of_day_bucket",
    "game_day_of_week",
    "park",
    "venue",
    "is_home",
    "batting_order",
    "lineup_slot",
    "team_d7_runs_per_game",
    "team_d7_hits_per_game",
    "team_d7_total_bases_per_game",
    "team_d15_runs_per_game",
    "team_d15_hits_per_game",
    "team_d15_total_bases_per_game",
    "same_game_teammate_tier_a_count",
    "same_game_team_o15_candidate_count",
    "lineup_heat_cluster",
    "previous_team_game_time",
    "day_after_night",
    "short_turnaround",
    "rest_day_before_game",
]


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _f(value: Any) -> float | None:
    return manual._f(value)


def _i(value: Any) -> int | None:
    number = _f(value)
    return None if number is None else int(number)


def _date(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("board_date") or "")[:10]


def _team(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    alias = TEAM_ALIASES.get(text.upper().replace(".", ""))
    if alias:
        return alias
    if text.isdigit():
        text = getFullTeamAbbreviationFromID(text) or text
    normalized = str(normalizeTeamAbbreviation(text) or manual._team(text) or text).strip()
    return TEAM_ALIASES.get(normalized.upper().replace(".", ""), normalized)


def _norm_name(value: Any) -> str:
    return manual._norm_name(value)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "null"}


def _non_null(value: Any) -> bool:
    if _is_missing(value):
        return False
    if isinstance(value, bool):
        return True
    return True


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(manual.ROOT))
    except Exception:
        return str(path)


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def _time_bucket_from_time(value: Any) -> str:
    parsed = _parse_time(value)
    if parsed is None:
        return ""
    try:
        return get_time_of_day_bucket_et(parsed)
    except Exception:
        return ""


def _canonical_game_key(date_text: str, team_a: Any, team_b: Any) -> str:
    teams = sorted([_team(team_a), _team(team_b)])
    if not date_text or not teams[0] or not teams[1]:
        return ""
    return "|".join((date_text, teams[0], teams[1]))


def _canonical_team_game_key(date_text: str, team: Any, opponent: Any) -> str:
    team_code = _team(team)
    opponent_code = _team(opponent)
    if not date_text or not team_code or not opponent_code:
        return ""
    return "|".join((date_text, team_code, opponent_code))


def _row_lookup_keys(row: dict[str, Any]) -> list[str]:
    date_text = _date(row)
    player_id = _i(row.get("player_id"))
    name = _norm_name(row.get("player_name") or row.get("player"))
    team = _team(row.get("team"))
    opponent = _team(row.get("opponent"))
    keys: list[str] = []
    if player_id is not None:
        keys.append("|".join((date_text, "pid", str(player_id), team, opponent)))
        keys.append("|".join((date_text, "pid", str(player_id))))
    if name:
        keys.append("|".join((date_text, "name", name, team, opponent)))
        keys.append("|".join((date_text, "name", name)))
    return keys


def _slate_candidate(row: dict[str, Any], path: Path) -> dict[str, Any]:
    is_home = str(row.get("is_home") or "").strip().lower()
    home_away = "home" if is_home in {"1", "true", "yes"} else "away" if is_home in {"0", "false", "no"} else ""
    game_time = row.get("game_time")
    return {
        "game_id": row.get("game_id"),
        "game_time": game_time,
        "time_of_day_bucket": _time_bucket_from_time(game_time) or row.get("time_of_day_bucket"),
        "game_day_of_week": row.get("game_day_of_week"),
        "is_home": row.get("is_home"),
        "home_away": home_away,
        "team": _team(row.get("team")),
        "opponent": _team(row.get("opponent")),
        "home_team_code": row.get("home_team_code"),
        "away_team_code": row.get("away_team_code"),
        "team_id": row.get("team_id"),
        "opponent_id": row.get("opponent_id"),
        "park": row.get("park") or row.get("venue") or row.get("venue_name") or row.get("ballpark"),
        "venue": row.get("venue") or row.get("venue_name") or row.get("park") or row.get("ballpark"),
        "context_source": _rel(path),
    }


def _load_slate_context_index(slate_root: Path, dates: set[str]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for date_text in sorted(date for date in dates if date):
        path = slate_root / date_text / "mlb_slate_output.csv"
        for row in _read_csv(path):
            if str(row.get("prop_type") or "").strip().lower() != "hits":
                continue
            slate_date = str(row.get("slate_date") or row.get("game_date") or date_text)[:10]
            player_id = _i(row.get("player_id"))
            name = _norm_name(row.get("player_name"))
            team = _team(row.get("team"))
            opponent = _team(row.get("opponent"))
            candidate = _slate_candidate(row, path)
            keys: list[str] = []
            if player_id is not None:
                keys.append("|".join((slate_date, "pid", str(player_id), team, opponent)))
                keys.append("|".join((slate_date, "pid", str(player_id))))
            if name:
                keys.append("|".join((slate_date, "name", name, team, opponent)))
                keys.append("|".join((slate_date, "name", name)))
            for key in keys:
                index.setdefault(key, candidate)
    return index


def _load_alternate_event_index(backfill_root: Path, dates: set[str]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for date_text in sorted(date for date in dates if date):
        path = backfill_root / date_text / "live_alternate_book_level_rows.csv"
        for row in _read_csv(path):
            if str(row.get("market_key") or "").strip() != "batter_hits_alternate":
                continue
            if str(row.get("side") or "").strip().lower() != "over":
                continue
            line = str(row.get("line") or "").strip()
            if line not in {"1.5", "1.50"}:
                continue
            name = _norm_name(row.get("player_name"))
            player_id = _i(row.get("player_id"))
            event = {
                "event_id": row.get("event_id"),
                "event_game": row.get("game"),
                "event_home_team": _team(row.get("home_team")),
                "event_away_team": _team(row.get("away_team")),
                "game_time": row.get("commence_time"),
                "event_context_source": _rel(path),
            }
            keys: list[str] = []
            if player_id is not None:
                keys.append("|".join((date_text, "pid", str(player_id), line, "over")))
            if name:
                keys.append("|".join((date_text, "name", name, line, "over")))
            for key in keys:
                index.setdefault(key, event)
    return index


def _alternate_lookup_keys(row: dict[str, Any]) -> list[str]:
    date_text = _date(row)
    line = str(row.get("line") or "").strip()
    side = str(row.get("side") or "over").strip().lower() or "over"
    player_id = _i(row.get("player_id"))
    name = _norm_name(row.get("player_name") or row.get("player"))
    keys: list[str] = []
    if player_id is not None:
        keys.append("|".join((date_text, "pid", str(player_id), line, side)))
    if name:
        keys.append("|".join((date_text, "name", name, line, side)))
    return keys


def _hydrate_alternate_event_context(rows: list[dict[str, Any]], backfill_root: Path) -> None:
    dates = {_date(row) for row in rows if _date(row)}
    index = _load_alternate_event_index(backfill_root, dates)
    for row in rows:
        if str(row.get("source_bucket") or "") not in {"alternate_only", "shared"} and not row.get("from_alternate"):
            continue
        match = None
        for key in _alternate_lookup_keys(row):
            if key in index:
                match = index[key]
                break
        if not match:
            continue
        for field in ("event_id", "event_game", "event_home_team", "event_away_team"):
            if _is_missing(row.get(field)) and not _is_missing(match.get(field)):
                row[field] = match.get(field)
        if _is_missing(row.get("game_time")) and not _is_missing(match.get("game_time")):
            row["game_time"] = match.get("game_time")
        row["event_context_source"] = match.get("event_context_source")


def _fetch_game_info(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    if pg_fetchall is None:
        return [], "db_unavailable"
    dates = []
    for row in rows:
        try:
            dates.append(datetime.strptime(_date(row), "%Y-%m-%d").date())
        except Exception:
            pass
    if not dates:
        return [], "no_dates"
    min_date = (min(dates) - timedelta(days=7)).isoformat()
    max_date = (max(dates) + timedelta(days=1)).isoformat()
    try:
        db_rows = pg_fetchall(
            """
SELECT game_id, game_time, game_date::date AS game_date, home_team_id, away_team_id, home_team_abbr, away_team_abbr
FROM mlb.game_info
WHERE game_date BETWEEN %s::date AND %s::date
ORDER BY game_date, game_time, game_id
""",
            (min_date, max_date),
        )
    except Exception as exc:
        return [], f"db_error:{type(exc).__name__}:{exc}"
    rows_out: list[dict[str, Any]] = []
    for item in db_rows or []:
        row = dict(item)
        row["home_team_abbr"] = _team(row.get("home_team_abbr") or row.get("home_team_id"))
        row["away_team_abbr"] = _team(row.get("away_team_abbr") or row.get("away_team_id"))
        rows_out.append(row)
    return rows_out, "mlb.game_info"


def _game_time_close(a: Any, b: Any, minutes: float = 10.0) -> bool:
    ta = _parse_time(a)
    tb = _parse_time(b)
    if ta is None or tb is None:
        return False
    if ta.tzinfo is None:
        ta = ta.replace(tzinfo=timezone.utc)
    if tb.tzinfo is None:
        tb = tb.replace(tzinfo=timezone.utc)
    return abs((ta - tb).total_seconds()) <= minutes * 60.0


def _build_game_indices(game_rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], list[dict[str, Any]]]]:
    by_game_key: dict[str, dict[str, Any]] = {}
    by_date_time: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for game in game_rows:
        date_text = str(game.get("game_date") or "")[:10]
        home = _team(game.get("home_team_abbr"))
        away = _team(game.get("away_team_abbr"))
        key = _canonical_game_key(date_text, home, away)
        if key:
            by_game_key[key] = game
        time_text = str(game.get("game_time") or "")
        if date_text and time_text:
            by_date_time[(date_text, time_text[:16])].append(game)
    return by_game_key, by_date_time


def _game_from_row(row: dict[str, Any], by_game_key: dict[str, dict[str, Any]], game_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    date_text = _date(row)
    event_time = _parse_time(row.get("game_time"))
    date_candidates = [date_text]
    if event_time is not None:
        event_date = event_time.date().isoformat()
        if event_date not in date_candidates:
            date_candidates.append(event_date)
    for a, b in (
        (row.get("team"), row.get("opponent")),
        (row.get("home_team_code"), row.get("away_team_code")),
        (row.get("event_home_team"), row.get("event_away_team")),
    ):
        for candidate_date in date_candidates:
            key = _canonical_game_key(candidate_date, a, b)
            if key and key in by_game_key:
                return by_game_key[key]
    game_time = row.get("game_time")
    if game_time:
        candidates = [
            game
            for game in game_rows
            if str(game.get("game_date") or "")[:10] in set(date_candidates) and _game_time_close(game.get("game_time"), game_time)
        ]
        if len(candidates) == 1:
            return candidates[0]
    return None


def _fetch_player_identity() -> tuple[dict[int, dict[str, Any]], dict[str, list[dict[str, Any]]], str]:
    if pg_fetchall is None:
        return {}, {}, "db_unavailable"
    try:
        db_rows = pg_fetchall(
            """
SELECT player_name, player_id, team_id, team
FROM mlb.player_ids
WHERE COALESCE(is_placeholder, false) = false
""",
            (),
        )
    except Exception as exc:
        return {}, {}, f"db_error:{type(exc).__name__}:{exc}"
    by_id: dict[int, dict[str, Any]] = {}
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in db_rows or []:
        player_id = _i(item.get("player_id"))
        name = _norm_name(item.get("player_name"))
        if player_id is None or not name:
            continue
        row = dict(item)
        row["team_code"] = _team(item.get("team") or item.get("team_id"))
        by_id[int(player_id)] = row
        by_name[name].append(row)
    return by_id, by_name, "mlb.player_ids"


def _hydrate_game_identity(rows: list[dict[str, Any]]) -> str:
    game_rows, game_source = _fetch_game_info(rows)
    by_game_key, _by_date_time = _build_game_indices(game_rows)
    by_player_id, by_player_name, player_source = _fetch_player_identity()
    for row in rows:
        game = _game_from_row(row, by_game_key, game_rows)
        if not game:
            continue
        home = _team(game.get("home_team_abbr"))
        away = _team(game.get("away_team_abbr"))
        if _is_missing(row.get("game_id")):
            row["game_id"] = game.get("game_id")
        if _is_missing(row.get("home_team_code")):
            row["home_team_code"] = home
        if _is_missing(row.get("away_team_code")):
            row["away_team_code"] = away
        if _is_missing(row.get("game_time")):
            row["game_time"] = game.get("game_time")
        if _is_missing(row.get("context_source")):
            row["context_source"] = game_source

        player_team = _team(row.get("team"))
        player_id = _i(row.get("player_id"))
        if not player_team and player_id is not None and player_id in by_player_id:
            player_team = _team(by_player_id[player_id].get("team_code"))
        if not player_team:
            candidates = by_player_name.get(_norm_name(row.get("player_name") or row.get("player")), [])
            in_game = [candidate for candidate in candidates if _team(candidate.get("team_code")) in {home, away}]
            if len(in_game) == 1:
                player_team = _team(in_game[0].get("team_code"))
                if _is_missing(row.get("player_id")):
                    row["player_id"] = in_game[0].get("player_id")
        if player_team in {home, away}:
            opponent = away if player_team == home else home
            row["team"] = player_team
            row["opponent"] = opponent
            row["is_home"] = player_team == home
            row["home_away"] = "home" if player_team == home else "away"
            row["identity_context_source"] = f"{game_source}+{player_source}"
    return f"{game_source}+{player_source}"


def _hydrate_player_team_by_game(rows: list[dict[str, Any]]) -> str:
    if pg_fetchall is None:
        return "db_unavailable"
    pairs = sorted(
        {
            (int(game_id), int(player_id))
            for row in rows
            if (game_id := _i(row.get("game_id"))) is not None and (player_id := _i(row.get("player_id"))) is not None
        }
    )
    if not pairs:
        return "missing_player_or_game_ids"
    game_ids = sorted({game_id for game_id, _player_id in pairs})
    player_ids = sorted({player_id for _game_id, player_id in pairs})
    try:
        db_rows = pg_fetchall(
            """
SELECT player_id, game_id, team_id
FROM mlb.player_team_by_game
WHERE game_id = ANY(%s)
  AND player_id = ANY(%s)
""",
            (game_ids, player_ids),
        )
    except Exception as exc:
        return f"db_error:{type(exc).__name__}:{exc}"
    pteam = {
        (int(_i(item.get("game_id")) or 0), int(_i(item.get("player_id")) or 0)): _team(item.get("team_id"))
        for item in db_rows or []
        if _i(item.get("game_id")) is not None and _i(item.get("player_id")) is not None
    }
    for row in rows:
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        if game_id is None or player_id is None:
            continue
        team = pteam.get((int(game_id), int(player_id)))
        if not team:
            continue
        home = _team(row.get("home_team_code"))
        away = _team(row.get("away_team_code"))
        if team not in {home, away}:
            continue
        row["team"] = team
        row["opponent"] = away if team == home else home
        row["is_home"] = team == home
        row["home_away"] = "home" if team == home else "away"
        row["player_team_context_source"] = "mlb.player_team_by_game"
    return "mlb.player_team_by_game"


def _hydrate_slate_context(rows: list[dict[str, Any]], slate_root: Path) -> None:
    dates = {_date(row) for row in rows if _date(row)}
    index = _load_slate_context_index(slate_root, dates)
    fields = [
        "game_id",
        "game_time",
        "time_of_day_bucket",
        "game_day_of_week",
        "is_home",
        "home_away",
        "home_team_code",
        "away_team_code",
        "team_id",
        "opponent_id",
        "park",
        "venue",
        "team",
        "opponent",
    ]
    for row in rows:
        match = None
        for key in _row_lookup_keys(row):
            if key in index:
                match = index[key]
                break
        if not match:
            continue
        for field in fields:
            if _is_missing(row.get(field)) and not _is_missing(match.get(field)):
                row[field] = match.get(field)
        if not _is_missing(match.get("context_source")):
            row["context_source"] = match.get("context_source")


def _fetch_team_game_stats(rows: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], str]:
    if pg_fetchall is None:
        return {}, "db_unavailable"
    dates = []
    for row in rows:
        try:
            dates.append(datetime.strptime(_date(row), "%Y-%m-%d").date())
        except Exception:
            pass
    if not dates:
        return {}, "no_dates"
    min_date = (min(dates) - timedelta(days=60)).isoformat()
    max_date = max(dates).isoformat()
    try:
        db_rows = pg_fetchall(
            """
SELECT
  game_date::date AS game_date,
  game_id,
  team,
  SUM(COALESCE(hits, 0))::float8 AS team_hits,
  SUM(COALESCE(runs_scored, 0))::float8 AS team_runs,
  SUM(COALESCE(total_bases, 0))::float8 AS team_total_bases,
  SUM(COALESCE(hits, 0) + COALESCE(runs_scored, 0) + COALESCE(rbis, 0))::float8 AS team_hits_runs_rbis
FROM mlb.player_stats
WHERE game_date BETWEEN %s::date AND %s::date
GROUP BY game_date, game_id, team
ORDER BY team, game_date, game_id
""",
            (min_date, max_date),
        )
    except Exception as exc:
        return {}, f"db_error:{type(exc).__name__}:{exc}"
    by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in db_rows or []:
        team = _team(item.get("team"))
        if team:
            by_team[team].append(dict(item))
    for team in by_team:
        by_team[team].sort(key=lambda r: (str(r.get("game_date"))[:10], int(_i(r.get("game_id")) or 0)))
    return by_team, "mlb.player_stats_prior_team_games"


def _avg(values: list[float | None]) -> float | None:
    vals = [value for value in values if value is not None]
    return sum(vals) / len(vals) if vals else None


def _hydrate_team_offense(rows: list[dict[str, Any]]) -> str:
    team_games, source = _fetch_team_game_stats(rows)
    if not team_games:
        return source
    for row in rows:
        team = _team(row.get("team"))
        date_text = _date(row)
        game_id = int(_i(row.get("game_id")) or 0)
        prior = [
            game
            for game in team_games.get(team, [])
            if (str(game.get("game_date"))[:10], int(_i(game.get("game_id")) or 0)) < (date_text, game_id)
        ]
        for n in (7, 15):
            sample = prior[-n:]
            row[f"team_d{n}_games_available"] = len(sample)
            row[f"team_d{n}_hits_per_game"] = _avg([_f(game.get("team_hits")) for game in sample])
            row[f"team_d{n}_runs_per_game"] = _avg([_f(game.get("team_runs")) for game in sample])
            row[f"team_d{n}_total_bases_per_game"] = _avg([_f(game.get("team_total_bases")) for game in sample])
            row[f"team_d{n}_hits_runs_rbis_per_game"] = _avg([_f(game.get("team_hits_runs_rbis")) for game in sample])
        row["team_context_source"] = source
    return source


def _fetch_lineup_slots(rows: list[dict[str, Any]]) -> tuple[dict[tuple[int, int], Any], str]:
    if pg_fetchall is None:
        return {}, "db_unavailable"
    player_ids = sorted({int(pid) for row in rows if (pid := _i(row.get("player_id"))) is not None})
    game_ids = sorted({int(gid) for row in rows if (gid := _i(row.get("game_id"))) is not None})
    if not player_ids or not game_ids:
        return {}, "missing_player_or_game_ids"
    try:
        db_rows = pg_fetchall(
            """
SELECT game_id, player_id, lineup_slot
FROM mlb.prop_features_precomputed
WHERE prop_type = 'hits'
  AND game_id = ANY(%s)
  AND player_id = ANY(%s)
""",
            (game_ids, player_ids),
        )
    except Exception as exc:
        return {}, f"db_error:{type(exc).__name__}:{exc}"
    out: dict[tuple[int, int], Any] = {}
    for item in db_rows or []:
        game_id = _i(item.get("game_id"))
        player_id = _i(item.get("player_id"))
        if game_id is not None and player_id is not None and not _is_missing(item.get("lineup_slot")):
            out[(int(game_id), int(player_id))] = item.get("lineup_slot")
    return out, "mlb.prop_features_precomputed.lineup_slot"


def _lineup_bucket(value: Any) -> str:
    slot = _i(value)
    if slot is None:
        return "missing"
    if slot <= 2:
        return "top_1_2"
    if slot <= 5:
        return "middle_3_5"
    if slot <= 7:
        return "lower_6_7"
    return "bottom_8_9"


def _hydrate_lineup(rows: list[dict[str, Any]]) -> str:
    slots, source = _fetch_lineup_slots(rows)
    for row in rows:
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        value = slots.get((int(game_id), int(player_id))) if game_id is not None and player_id is not None else None
        if value is not None:
            row["lineup_slot"] = value
            row["batting_order"] = value
            row["lineup_slot_bucket"] = _lineup_bucket(value)
            row["lineup_context_source"] = source
        elif _is_missing(row.get("lineup_slot_bucket")):
            row["lineup_slot_bucket"] = _lineup_bucket(row.get("lineup_slot") or row.get("batting_order"))
    return source


def _hydrate_same_game_counts(rows: list[dict[str, Any]]) -> None:
    by_group: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        date_text = _date(row)
        game_id = _i(row.get("game_id"))
        team = _team(row.get("team"))
        if date_text and game_id is not None and team:
            by_group[(date_text, int(game_id), team)].append(row)
    for row in rows:
        date_text = _date(row)
        game_id = _i(row.get("game_id"))
        team = _team(row.get("team"))
        if not date_text or game_id is None or not team:
            row["same_game_team_o15_candidate_count"] = ""
            row["same_game_teammate_o15_candidate_count"] = ""
            row["same_game_team_tier_a_count"] = ""
            row["same_game_teammate_tier_a_count"] = ""
            row["lineup_heat_cluster"] = ""
            continue
        key = (date_text, int(game_id), team)
        group = by_group.get(key, [])
        player_key = str(row.get("player_id") or _norm_name(row.get("player_name") or row.get("player")))
        unique_players = {str(item.get("player_id") or _norm_name(item.get("player_name") or item.get("player"))) for item in group}
        tier_a_players = {
            str(item.get("player_id") or _norm_name(item.get("player_name") or item.get("player")))
            for item in group
            if str(item.get("hitter_tier") or "").strip().upper() == "A"
        }
        row["same_game_team_o15_candidate_count"] = len(unique_players)
        row["same_game_teammate_o15_candidate_count"] = max(0, len(unique_players - {player_key}))
        row["same_game_team_tier_a_count"] = len(tier_a_players)
        row["same_game_teammate_tier_a_count"] = max(0, len(tier_a_players - {player_key}))
        row["lineup_heat_cluster"] = row["same_game_teammate_tier_a_count"] > 0


def _load_schedule(slate_root: Path, min_date: str, max_date: str) -> dict[tuple[str, int], dict[str, Any]]:
    schedule: dict[tuple[str, int], dict[str, Any]] = {}
    for path in sorted(slate_root.glob("20??-??-??/mlb_slate_output.csv")):
        date_text = path.parent.name
        if date_text < min_date or date_text > max_date:
            continue
        for row in _read_csv(path):
            game_id = _i(row.get("game_id"))
            if game_id is None:
                continue
            game_time = row.get("game_time")
            time_bucket = _time_bucket_from_time(game_time) or row.get("time_of_day_bucket")
            home = _team(row.get("home_team_code"))
            away = _team(row.get("away_team_code"))
            if home:
                schedule[(home, int(game_id))] = {
                    "date": date_text,
                    "game_id": int(game_id),
                    "team": home,
                    "game_time": game_time,
                    "time_of_day_bucket": time_bucket,
                }
            if away:
                schedule[(away, int(game_id))] = {
                    "date": date_text,
                    "game_id": int(game_id),
                    "team": away,
                    "game_time": game_time,
                    "time_of_day_bucket": time_bucket,
                }
    return schedule


def _hydrate_rest(rows: list[dict[str, Any]], slate_root: Path) -> None:
    game_rows, source = _fetch_game_info(rows)
    schedule: dict[tuple[str, int], dict[str, Any]] = {}
    for game in game_rows:
        date_text = str(game.get("game_date") or "")[:10]
        game_id = _i(game.get("game_id"))
        if game_id is None:
            continue
        for team in (_team(game.get("home_team_abbr")), _team(game.get("away_team_abbr"))):
            if team:
                schedule[(team, int(game_id))] = {
                    "date": date_text,
                    "game_id": int(game_id),
                    "team": team,
                    "game_time": game.get("game_time"),
                    "time_of_day_bucket": _time_bucket_from_time(game.get("game_time")),
                }
    if not schedule:
        schedule = _load_schedule(slate_root, "1900-01-01", "2999-12-31")
        source = "odds_history_mlb_slate_output_schedule"
    by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in schedule.values():
        by_team[item["team"]].append(item)
    for team in by_team:
        by_team[team].sort(
            key=lambda item: (
                str(item.get("date") or ""),
                _parse_time(item.get("game_time")) or datetime.min.replace(tzinfo=timezone.utc),
                int(item.get("game_id") or 0),
            )
        )

    position: dict[tuple[str, int], int] = {}
    for team, games in by_team.items():
        for idx, game in enumerate(games):
            position[(team, int(game.get("game_id") or 0))] = idx

    for row in rows:
        team = _team(row.get("team"))
        game_id = _i(row.get("game_id"))
        if not team or game_id is None:
            continue
        games = by_team.get(team, [])
        idx = position.get((team, int(game_id)))
        if idx is None or idx <= 0:
            row["team_time_sequence_bucket"] = row.get("team_time_sequence_bucket") or "no_previous_team_game_in_artifacts"
            row["rest_context_source"] = source
            continue
        prev = games[idx - 1]
        current_time = _parse_time(row.get("game_time"))
        prev_time = _parse_time(prev.get("game_time"))
        current_bucket = str(row.get("time_of_day_bucket") or "")
        prev_bucket = str(prev.get("time_of_day_bucket") or "")
        row["previous_team_game_time"] = prev.get("game_time")
        row["previous_team_time_of_day_bucket"] = prev_bucket
        if prev_bucket and current_bucket:
            row["team_time_sequence_bucket"] = f"{current_bucket}_after_{prev_bucket}"
            row["day_after_night"] = current_bucket in {"morning", "afternoon"} and prev_bucket in {"evening", "late"}
        else:
            row["team_time_sequence_bucket"] = "missing_time_bucket"
            row["day_after_night"] = ""
        if current_time and prev_time:
            hours = (current_time - prev_time).total_seconds() / 3600.0
            row["hours_since_previous_team_game"] = hours
            row["short_turnaround"] = hours < 20.0
        else:
            row["short_turnaround"] = ""
        try:
            current_date = datetime.strptime(_date(row), "%Y-%m-%d").date()
            prev_date = datetime.strptime(str(prev.get("date") or "")[:10], "%Y-%m-%d").date()
            row["rest_day_before_game"] = (current_date - prev_date).days > 1
        except Exception:
            row["rest_day_before_game"] = ""
        row["rest_context_source"] = source


def _canonicalize_time_context(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        bucket = _time_bucket_from_time(row.get("game_time"))
        if bucket:
            row["time_of_day_bucket"] = bucket


def _coverage(rows: list[dict[str, Any]]) -> dict[str, tuple[int, float]]:
    total = len(rows)
    out: dict[str, tuple[int, float]] = {}
    for field in COVERAGE_FIELDS:
        count = sum(1 for row in rows if _non_null(row.get(field)))
        out[field] = (count, count / total if total else 0.0)
    return out


def _coverage_rows(before: dict[str, tuple[int, float]], after: dict[str, tuple[int, float]], total: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for field in COVERAGE_FIELDS:
        b_count, b_pct = before.get(field, (0, 0.0))
        a_count, a_pct = after.get(field, (0, 0.0))
        out.append(
            {
                "field": field,
                "rows": total,
                "before_non_null": b_count,
                "before_coverage": b_pct,
                "after_non_null": a_count,
                "after_coverage": a_pct,
                "newly_filled": max(0, a_count - b_count),
            }
        )
    return out


def _add_canonical_keys(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        date_text = _date(row)
        player_id = _i(row.get("player_id"))
        team = _team(row.get("team"))
        opponent = _team(row.get("opponent"))
        player_name = _norm_name(row.get("player_name") or row.get("player"))
        row["canonical_date"] = date_text
        row["canonical_player_id"] = player_id if player_id is not None else ""
        row["canonical_player_name"] = player_name
        row["canonical_team"] = team
        row["canonical_opponent"] = opponent
        row["canonical_game_key"] = _canonical_game_key(date_text, team, opponent)
        row["canonical_player_game_key"] = (
            "|".join((date_text, str(player_id), team, opponent)) if player_id is not None and team and opponent else ""
        )
        row["canonical_team_game_key"] = _canonical_team_game_key(date_text, team, opponent)


def _identity_gap_rows(rows: list[dict[str, Any]], slate_root: Path, backfill_root: Path) -> list[dict[str, Any]]:
    dates = {_date(row) for row in rows if _date(row)}
    slate_index = _load_slate_context_index(slate_root, dates)
    alt_index = _load_alternate_event_index(backfill_root, dates)
    game_rows, game_source = _fetch_game_info(rows)
    by_game_key, _by_date_time = _build_game_indices(game_rows)
    out: list[dict[str, Any]] = []
    field_groups = {
        "game_id": ["game_id"],
        "is_home": ["is_home"],
        "team_offense_context": ["team_d7_runs_per_game", "team_d15_runs_per_game"],
        "same_game_cluster_context": ["same_game_team_o15_candidate_count", "same_game_teammate_tier_a_count"],
        "rest_context": ["previous_team_game_time", "team_time_sequence_bucket"],
        "park_venue": ["park", "venue"],
        "lineup_slot": ["lineup_slot", "batting_order"],
    }
    for row in rows:
        missing_groups = [
            group
            for group, fields in field_groups.items()
            if not any(_non_null(row.get(field)) for field in fields)
        ]
        if not missing_groups:
            continue
        slate_by_pid = any(key in slate_index for key in _row_lookup_keys({**row, "player_name": "", "player": ""}))
        slate_by_any = any(key in slate_index for key in _row_lookup_keys(row))
        alt_by_any = any(key in alt_index for key in _alternate_lookup_keys(row))
        team_game_key = _canonical_game_key(_date(row), row.get("team"), row.get("opponent"))
        event_game_key = _canonical_game_key(_date(row), row.get("event_home_team"), row.get("event_away_team"))
        game_match = bool((team_game_key and team_game_key in by_game_key) or (event_game_key and event_game_key in by_game_key))
        out.append(
            {
                "date": _date(row),
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name") or row.get("player"),
                "team": row.get("team"),
                "opponent": row.get("opponent"),
                "canonical_team": row.get("canonical_team"),
                "canonical_opponent": row.get("canonical_opponent"),
                "source_bucket": row.get("source_bucket"),
                "missing_groups": ",".join(missing_groups),
                "player_id_exists": bool(_i(row.get("player_id")) is not None),
                "team_exists": bool(_team(row.get("team"))),
                "opponent_exists": bool(_team(row.get("opponent"))),
                "canonical_game_key": row.get("canonical_game_key"),
                "canonical_player_game_key": row.get("canonical_player_game_key"),
                "canonical_team_game_key": row.get("canonical_team_game_key"),
                "matching_slate_row_by_player_id": slate_by_pid,
                "matching_slate_row_by_player_or_name": slate_by_any,
                "matching_alternate_event_row": alt_by_any,
                "matching_game_by_team_or_event": game_match,
                "event_home_team": row.get("event_home_team"),
                "event_away_team": row.get("event_away_team"),
                "game_info_source": game_source,
                "first_likely_gap": (
                    "no_player_or_team_identity"
                    if not _team(row.get("team")) and not row.get("event_home_team")
                    else "player_team_unresolved"
                    if not _team(row.get("team"))
                    and "team_offense_context" in missing_groups
                    and (team_game_key or event_game_key)
                    else "game_info_no_match"
                    if not game_match and ("game_id" in missing_groups or "rest_context" in missing_groups)
                    else "source_field_unavailable"
                ),
            }
        )
    return out


def _write_identity_fix_report(path: Path, coverage: list[dict[str, Any]], gap_rows: list[dict[str, Any]]) -> None:
    by_group: dict[str, int] = defaultdict(int)
    by_reason: dict[str, int] = defaultdict(int)
    for row in gap_rows:
        for group in str(row.get("missing_groups") or "").split(","):
            if group:
                by_group[group] += 1
        by_reason[str(row.get("first_likely_gap") or "unknown")] += 1
    lines = [
        "# Expanded O1.5 Context Hydration Identity Fixes",
        "",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        "- Scope: research-layer identity/context hydration only.",
        "- Canonical keys now include date, player id/name, normalized team/opponent, game key, player-game key, and team-game key.",
        "- Join order now uses player-level slate matches, alternate event matches, DB game schedule, then team-game context independent of player matching.",
        "",
        "## Remaining Missing Groups",
        "",
    ]
    if by_group:
        for group, count in sorted(by_group.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"- `{group}`: `{count}` rows")
    else:
        lines.append("- No rows have missing audited context groups.")
    lines.extend(["", "## Remaining Likely Gap Reasons", ""])
    for reason, count in sorted(by_reason.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"- `{reason}`: `{count}` rows")
    lines.extend(["", "## Coverage Snapshot", "", "| field | before | after | newly filled |", "|---|---:|---:|---:|"])
    for row in coverage:
        lines.append(
            f"| {row.get('field')} | {float(row.get('before_coverage') or 0.0) * 100:.2f}% | "
            f"{float(row.get('after_coverage') or 0.0) * 100:.2f}% | {row.get('newly_filled')} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_identity_gap_audit(path: Path, gap_rows: list[dict[str, Any]], coverage: list[dict[str, Any]]) -> None:
    by_missing_group: dict[str, int] = defaultdict(int)
    by_date: dict[str, int] = defaultdict(int)
    by_team: dict[str, int] = defaultdict(int)
    by_source: dict[str, int] = defaultdict(int)
    by_reason: dict[str, int] = defaultdict(int)
    for row in gap_rows:
        for group in str(row.get("missing_groups") or "").split(","):
            if group:
                by_missing_group[group] += 1
        by_date[str(row.get("date") or "missing")] += 1
        by_team[str(row.get("canonical_team") or row.get("team") or "missing")] += 1
        by_source[str(row.get("source_bucket") or "missing")] += 1
        by_reason[str(row.get("first_likely_gap") or "unknown")] += 1

    def top_items(counter: dict[str, int], limit: int = 15) -> list[str]:
        return [f"- `{key}`: `{value}`" for key, value in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]]

    lines = [
        "# Expanded O1.5 Context Identity Gap Audit",
        "",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Rows with at least one audited missing context group: `{len(gap_rows)}`",
        "- This audit separates join-identity misses from true source limitations.",
        "",
        "## Coverage",
        "",
        "| field | after coverage | rows filled |",
        "|---|---:|---:|",
    ]
    for row in coverage:
        lines.append(
            f"| {row.get('field')} | {float(row.get('after_coverage') or 0.0) * 100:.2f}% | {row.get('after_non_null')} |"
        )
    lines.extend(["", "## Missing By Context Group", ""])
    lines.extend(top_items(by_missing_group) or ["- none"])
    lines.extend(["", "## Missing By Likely Reason", ""])
    lines.extend(top_items(by_reason) or ["- none"])
    lines.extend(["", "## Top Dates Affected", ""])
    lines.extend(top_items(by_date) or ["- none"])
    lines.extend(["", "## Top Teams Affected", ""])
    lines.extend(top_items(by_team) or ["- none"])
    lines.extend(["", "## Source Type", ""])
    lines.extend(top_items(by_source) or ["- none"])
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `source_field_unavailable` means identity matched but the requested field is not present in current sources.",
            "- `game_info_no_match` means neither canonical team/opponent nor alternate event teams matched `mlb.game_info`.",
            "- `no_player_or_team_identity` means the row still lacks enough pregame identity to form a team-game key.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_audit(path: Path, coverage: list[dict[str, Any]], sources: dict[str, str], rows: list[dict[str, Any]]) -> None:
    improved = [row for row in coverage if int(row.get("newly_filled") or 0) > 0]
    missing = [row for row in coverage if float(row.get("after_coverage") or 0.0) < 0.05]
    lines = [
        "# Expanded O1.5 Context Hydration Audit",
        "",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Rows: `{len(rows)}`",
        "- Scope: research-layer context hydration only; no production selector/upload/grading/model changes.",
        "- Safety: price, outcome, grade, win/loss, ROI, and dedupe identity fields are not modified.",
        "",
        "## Source Status",
        "",
    ]
    for key, value in sources.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Coverage Before/After", "", "| field | before | after | newly filled |", "|---|---:|---:|---:|"])
    for row in coverage:
        lines.append(
            f"| {row.get('field')} | {float(row.get('before_coverage') or 0.0) * 100:.2f}% | "
            f"{float(row.get('after_coverage') or 0.0) * 100:.2f}% | {row.get('newly_filled')} |"
        )
    lines.extend(["", "## Improved Fields", ""])
    if improved:
        for row in improved:
            lines.append(f"- `{row.get('field')}`: +{row.get('newly_filled')} rows")
    else:
        lines.append("- No additional context cells were filled.")
    lines.extend(["", "## Still Sparse / Unavailable", ""])
    if missing:
        for row in missing:
            lines.append(
                f"- `{row.get('field')}` remains sparse at {float(row.get('after_coverage') or 0.0) * 100:.2f}% coverage."
            )
    else:
        lines.append("- No requested field is below 5% coverage.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _hydration_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "date",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "source_bucket",
        *CONTEXT_FIELDS,
    ]
    return [{field: row.get(field, "") for field in fields} for row in rows]


def run(rows_csv: Path, out_dir: Path, slate_root: Path, backfill_root: Path) -> dict[str, Any]:
    rows = _read_csv(rows_csv)
    before = _coverage(rows)

    _hydrate_slate_context(rows, slate_root)
    _hydrate_alternate_event_context(rows, backfill_root)
    identity_source = _hydrate_game_identity(rows)
    player_team_source = _hydrate_player_team_by_game(rows)
    _add_canonical_keys(rows)
    team_source = _hydrate_team_offense(rows)
    lineup_source = _hydrate_lineup(rows)
    _hydrate_same_game_counts(rows)
    _canonicalize_time_context(rows)
    _hydrate_rest(rows, slate_root)
    _canonicalize_time_context(rows)
    _add_canonical_keys(rows)

    after = _coverage(rows)
    coverage_out = _coverage_rows(before, after, len(rows))
    gap_rows = _identity_gap_rows(rows, slate_root, backfill_root)
    sources = {
        "slate_context_source": "backend/mlb/exports/odds_history/<DATE>/mlb_slate_output.csv",
        "alternate_event_context_source": "artifacts/analysis/mlb/review_aids/alternate_history/backfill/<DATE>/live_alternate_book_level_rows.csv",
        "identity_context_source": identity_source,
        "player_team_context_source": player_team_source,
        "team_context_source": team_source,
        "lineup_context_source": lineup_source,
        "rest_context_source": "mlb.game_info",
    }

    _write_csv(rows_csv, rows)
    _write_csv(out_dir / "expanded_o15_context_coverage_before_after.csv", coverage_out)
    _write_csv(out_dir / "expanded_o15_context_hydration_rows.csv", _hydration_rows(rows))
    _write_csv(out_dir / "expanded_o15_context_identity_gap_rows.csv", gap_rows)
    _write_audit(out_dir / "expanded_o15_context_hydration_audit.md", coverage_out, sources, rows)
    _write_identity_fix_report(out_dir / "expanded_o15_context_hydration_identity_fixes.md", coverage_out, gap_rows)
    _write_identity_gap_audit(out_dir / "expanded_o15_context_identity_gap_audit.md", gap_rows, coverage_out)

    return {
        "rows": len(rows),
        "coverage_report": str(out_dir / "expanded_o15_context_coverage_before_after.csv"),
        "audit": str(out_dir / "expanded_o15_context_hydration_audit.md"),
        "identity_gap_audit": str(out_dir / "expanded_o15_context_identity_gap_audit.md"),
        "team_context_source": team_source,
        "lineup_context_source": lineup_source,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Hydrate broad context for the Expanded O1.5 Universe.")
    ap.add_argument("--rows-csv", type=Path, default=DEFAULT_ROWS_CSV)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--slate-root", type=Path, default=DEFAULT_SLATE_ROOT)
    ap.add_argument("--alternate-backfill-root", type=Path, default=DEFAULT_ALTERNATE_BACKFILL_ROOT)
    args = ap.parse_args()
    print(run(args.rows_csv, args.out_dir, args.slate_root, args.alternate_backfill_root))


if __name__ == "__main__":
    main()
