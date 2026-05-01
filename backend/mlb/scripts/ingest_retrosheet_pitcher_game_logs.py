#!/usr/bin/env python3
"""Build a CSV-only Retrosheet pitcher-game-log sample.

Retrosheet is the historical backbone for pitcher game logs.
MLB Stats API remains the live/current-season source.
Chadwick Register is the ID bridge from Retrosheet IDs to MLBAM IDs.

This script intentionally does not write to the database. The first-run path
writes a normalized sample CSV for inspection:

  make mlb-download-retrosheet-sources

  python -m backend.mlb.scripts.ingest_retrosheet_pitcher_game_logs \
    --retrosheet-gamelogs-dir backend/mlb/data/raw/retrosheet/csv_downloads \
    --chadwick-register-csv backend/mlb/data/raw/retrosheet/chadwick_register/people.csv \
    --season 2024 \
    --limit-games 25 \
    --out-csv tmp/retrosheet_pitcher_game_logs_sample.csv

Expected input shape:
- Preferred: Retrosheet/Chadwick-derived pitcher box-score CSVs with one row
  per (game, pitcher). The script accepts several common column aliases.
- Fallback: raw Retrosheet event files (*.EVN/*.EVA). This is a preview-only
  sample-row backbone parser. It produces common pitcher stats from play rows,
  but earned-runs precision still requires Chadwick/Retrosheet box-score output.
- Chadwick Register CSV with `key_retro` and `key_mlbam` columns.

Out of scope for this foundation:
- pitch-level parsing
- full event-level run reconstruction
- production cron integration
- DB writes
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


OUTPUT_COLUMNS = [
    "game_date",
    "game_id_retrosheet",
    "pitcher_retrosheet_id",
    "pitcher_mlbam_id",
    "player_name",
    "team",
    "opponent",
    "is_starter",
    "innings_pitched",
    "outs_recorded",
    "strikeouts",
    "walks",
    "hits_allowed",
    "earned_runs",
    "runs_allowed",
    "home_runs_allowed",
    "batters_faced",
    "game_finished",
    "source",
    "created_at",
    "updated_at",
]

ALIASES: Mapping[str, Sequence[str]] = {
    "game_date": ("game_date", "date", "game_dt", "yyyymmdd"),
    "game_id_retrosheet": ("game_id_retrosheet", "game_id", "retro_game_id", "gameid", "id"),
    "pitcher_retrosheet_id": (
        "pitcher_retrosheet_id",
        "pitcher_id",
        "retro_id",
        "retrosheet_id",
        "player_id",
        "key_retro",
    ),
    "player_name": ("player_name", "pitcher_name", "name", "player"),
    "team": ("team", "team_id", "team_code", "pitcher_team", "pit_team"),
    "opponent": ("opponent", "opp", "opponent_team", "bat_team"),
    "is_starter": ("is_starter", "starter", "gs", "game_started", "started"),
    "innings_pitched": ("innings_pitched", "ip", "innings", "outs_pitched_baseball"),
    "outs_recorded": ("outs_recorded", "outs", "outs_pitched", "op"),
    "strikeouts": ("strikeouts", "so", "k", "strikeouts_pitching"),
    "walks": ("walks", "bb", "base_on_balls", "walks_allowed"),
    "hits_allowed": ("hits_allowed", "h", "hits", "ha"),
    "earned_runs": ("earned_runs", "er"),
    "runs_allowed": ("runs_allowed", "r", "runs"),
    "home_runs_allowed": ("home_runs_allowed", "hr", "home_runs"),
    "batters_faced": ("batters_faced", "bf"),
    "game_finished": ("game_finished", "gf", "finished"),
}

EVENT_FILE_GLOBS = ("*.EVN", "*.EVA", "*.evn", "*.eva")
CSV_GLOBS = ("*.csv", "*.CSV")
NON_BATTER_EVENTS = ("SB", "CS", "PO", "POCS", "BK", "WP", "PB", "DI", "OA", "NP")


def _clean_text(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def _resolve_col(df: pd.DataFrame, aliases: Iterable[str]) -> str:
    cols = {str(c).strip().lower(): str(c) for c in df.columns}
    for alias in aliases:
        key = str(alias).strip().lower()
        if key in cols:
            return cols[key]
    return ""


def _looks_like_pitcher_boxscore_csv(path: Path) -> bool:
    try:
        header = pd.read_csv(path, nrows=0)
    except Exception:
        return False
    needed = [
        _resolve_col(header, ALIASES["game_date"]),
        _resolve_col(header, ALIASES["game_id_retrosheet"]),
        _resolve_col(header, ALIASES["pitcher_retrosheet_id"]),
    ]
    stats = [
        _resolve_col(header, ALIASES["outs_recorded"]),
        _resolve_col(header, ALIASES["innings_pitched"]),
        _resolve_col(header, ALIASES["strikeouts"]),
        _resolve_col(header, ALIASES["hits_allowed"]),
    ]
    return all(needed) and any(stats)


def _series_text(df: pd.DataFrame, canonical: str) -> pd.Series:
    col = _resolve_col(df, ALIASES[canonical])
    if not col:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[col].map(_clean_text)


def _series_numeric(df: pd.DataFrame, canonical: str) -> pd.Series:
    col = _resolve_col(df, ALIASES[canonical])
    if not col:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _series_bool(df: pd.DataFrame, canonical: str) -> pd.Series:
    col = _resolve_col(df, ALIASES[canonical])
    if not col:
        return pd.Series([False] * len(df), index=df.index, dtype="bool")
    raw = df[col].map(lambda v: str(v).strip().lower())
    return raw.isin({"1", "true", "t", "yes", "y", "start", "starter"})


def _normal_game_date(values: pd.Series) -> pd.Series:
    text = values.map(_clean_text)
    parsed = pd.to_datetime(text, errors="coerce")

    yyyymmdd = text.str.fullmatch(r"\d{8}", na=False)
    if yyyymmdd.any():
        parsed.loc[yyyymmdd] = pd.to_datetime(text.loc[yyyymmdd], format="%Y%m%d", errors="coerce")

    return parsed.dt.date.astype("string")


def _ip_from_outs(outs: pd.Series) -> pd.Series:
    values = pd.to_numeric(outs, errors="coerce")
    whole = np.floor(values / 3)
    rem = values % 3
    return whole + (rem / 10.0)


def _outs_from_ip(ip: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(ip, errors="coerce")
    whole = np.floor(numeric)
    rem = np.rint((numeric - whole) * 10)
    return whole * 3 + rem


def load_chadwick_register(path: Optional[Path]) -> Dict[str, Optional[int]]:
    """Return Retrosheet ID -> MLBAM ID from Chadwick Register CSV."""
    if not path:
        return {}
    if not path.exists():
        raise SystemExit(f"Chadwick Register CSV not found: {path}")
    df = pd.read_csv(path, dtype=str)
    retro_col = _resolve_col(df, ["key_retro", "retro_id", "retrosheet_id"])
    mlbam_col = _resolve_col(df, ["key_mlbam", "mlbam_id", "player_id"])
    if not retro_col or not mlbam_col:
        raise SystemExit(
            f"Chadwick Register must include key_retro and key_mlbam-compatible columns: {path}"
        )

    mapping: Dict[str, Optional[int]] = {}
    for _, row in df.iterrows():
        retro = _clean_text(row.get(retro_col))
        if not retro:
            continue
        raw_mlbam = _clean_text(row.get(mlbam_col))
        try:
            mapping[retro] = int(float(raw_mlbam)) if raw_mlbam else None
        except Exception:
            mapping[retro] = None
    return mapping


def _discover_csv_inputs(gamelogs_dir: Optional[Path], season: Optional[str]) -> List[Path]:
    if not gamelogs_dir:
        return []
    if not gamelogs_dir.exists():
        raise SystemExit(f"Retrosheet gamelogs dir not found: {gamelogs_dir}")
    paths: List[Path] = []
    for pattern in CSV_GLOBS:
        for path in sorted(gamelogs_dir.rglob(pattern)):
            if season and season not in path.name and season not in str(path.parent):
                continue
            if _looks_like_pitcher_boxscore_csv(path):
                paths.append(path)
    return list(dict.fromkeys(paths))


def _discover_event_inputs(events_dir: Optional[Path], season: Optional[str]) -> List[Path]:
    if not events_dir:
        return []
    if not events_dir.exists():
        raise SystemExit(f"Retrosheet events dir not found: {events_dir}")
    paths: List[Path] = []
    for pattern in EVENT_FILE_GLOBS:
        for path in sorted(events_dir.rglob(pattern)):
            if season and season not in path.name and season not in str(path.parent):
                continue
            paths.append(path)
    return list(dict.fromkeys(paths))


def normalize_pitching_rows(df: pd.DataFrame, *, id_map: Mapping[str, Optional[int]]) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["game_date"] = _normal_game_date(_series_text(df, "game_date"))
    out["game_id_retrosheet"] = _series_text(df, "game_id_retrosheet")
    out["pitcher_retrosheet_id"] = _series_text(df, "pitcher_retrosheet_id")
    out["pitcher_mlbam_id"] = out["pitcher_retrosheet_id"].map(lambda v: id_map.get(str(v)) if v else None)
    out["player_name"] = _series_text(df, "player_name")
    out["team"] = _series_text(df, "team").str.upper()
    out["opponent"] = _series_text(df, "opponent").str.upper()
    out["is_starter"] = _series_bool(df, "is_starter")

    outs = _series_numeric(df, "outs_recorded")
    ip = _series_numeric(df, "innings_pitched")
    if outs.notna().any():
        out["outs_recorded"] = outs
        out["innings_pitched"] = np.where(ip.notna(), ip, _ip_from_outs(outs))
    else:
        out["innings_pitched"] = ip
        out["outs_recorded"] = _outs_from_ip(ip)

    for target in [
        "strikeouts",
        "walks",
        "hits_allowed",
        "earned_runs",
        "runs_allowed",
        "home_runs_allowed",
        "batters_faced",
    ]:
        out[target] = _series_numeric(df, target)

    out["game_finished"] = _series_bool(df, "game_finished")
    out["source"] = "retrosheet"
    now = pd.Timestamp.utcnow().isoformat()
    out["created_at"] = now
    out["updated_at"] = now

    # Keep the CSV inspection-friendly: integer-like stat columns use nullable Int64.
    int_cols = [
        "pitcher_mlbam_id",
        "outs_recorded",
        "strikeouts",
        "walks",
        "hits_allowed",
        "earned_runs",
        "runs_allowed",
        "home_runs_allowed",
        "batters_faced",
    ]
    for col in int_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").round().astype("Int64")

    return out[OUTPUT_COLUMNS]


def _csv_rows_from_event_file(path: Path) -> Iterable[List[str]]:
    with path.open("r", encoding="latin-1", newline="") as fh:
        for row in csv.reader(fh):
            if row:
                yield row


def _event_game_date(info: Mapping[str, str], game_id: str) -> str:
    raw = info.get("date") or ""
    if raw:
        parsed = pd.to_datetime(raw, errors="coerce")
        if not pd.isna(parsed):
            return parsed.date().isoformat()
    match = re.search(r"(\d{8})", game_id)
    if match:
        return pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce").date().isoformat()
    return ""


def _new_pitcher_row(
    *,
    game_date: str,
    game_id: str,
    pitcher_id: str,
    pitcher_name: str,
    team: str,
    opponent: str,
    is_starter: bool,
) -> Dict[str, Any]:
    return {
        "game_date": game_date,
        "game_id_retrosheet": game_id,
        "pitcher_retrosheet_id": pitcher_id,
        "pitcher_mlbam_id": None,
        "player_name": pitcher_name,
        "team": team,
        "opponent": opponent,
        "is_starter": bool(is_starter),
        "innings_pitched": np.nan,
        "outs_recorded": 0,
        "strikeouts": 0,
        "walks": 0,
        "hits_allowed": 0,
        # TODO: Use Chadwick/Retrosheet box-score output for exact earned runs.
        # Raw event play text alone is not enough for a safe first-pass ER parser.
        "earned_runs": np.nan,
        "runs_allowed": 0,
        "home_runs_allowed": 0,
        "batters_faced": 0,
        "game_finished": False,
        "source": "retrosheet",
        "created_at": pd.Timestamp.utcnow().isoformat(),
        "updated_at": pd.Timestamp.utcnow().isoformat(),
    }


def _event_main(play_event: str) -> str:
    return str(play_event or "").split("/", 1)[0].split(".", 1)[0].strip()


def _is_plate_appearance(play_event: str) -> bool:
    main = _event_main(play_event)
    if not main:
        return False
    return not main.startswith(NON_BATTER_EVENTS)


def _outs_on_play(play_event: str) -> int:
    main = _event_main(play_event)
    if not main:
        return 0
    # Conservative sample parser. Chadwick box scores should be preferred for exactness.
    outs = 0
    if main.startswith("K") and "+" not in main:
        outs += 1
    elif re.match(r"^\d", main):
        outs += 1
    outs += len(re.findall(r"\([123B]\)", play_event))
    return min(3, outs)


def _runs_on_play(play_event: str) -> int:
    main = _event_main(play_event)
    runs = 1 if main.startswith("HR") else 0
    runs += len(re.findall(r"(?:^|[.;])(?:[123B])-H", play_event))
    return runs


def _apply_play(row: Dict[str, Any], play_event: str) -> None:
    main = _event_main(play_event)
    if _is_plate_appearance(play_event):
        row["batters_faced"] += 1
    row["outs_recorded"] += _outs_on_play(play_event)
    if main.startswith("K"):
        row["strikeouts"] += 1
    if main.startswith(("W", "IW")):
        row["walks"] += 1
    if main.startswith(("S", "D", "T", "HR")):
        row["hits_allowed"] += 1
    if main.startswith("HR"):
        row["home_runs_allowed"] += 1
    row["runs_allowed"] += _runs_on_play(play_event)


def parse_retrosheet_event_files(
    paths: Sequence[Path],
    *,
    id_map: Mapping[str, Optional[int]],
    limit_games: int,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    rows: List[Dict[str, Any]] = []
    counters = {"files_seen": 0, "games_seen": 0}
    current_id = ""
    info: Dict[str, str] = {}
    starters: Dict[str, Tuple[str, str]] = {}
    current_pitcher_by_team: Dict[str, Tuple[str, str]] = {}
    last_pitcher_by_team: Dict[str, str] = {}
    game_rows: Dict[Tuple[str, str], Dict[str, Any]] = {}
    visteam = ""
    hometeam = ""

    def finish_game() -> None:
        nonlocal current_id, info, starters, current_pitcher_by_team, last_pitcher_by_team, game_rows
        if not current_id:
            return
        for (team, pitcher_id), row in game_rows.items():
            row["game_finished"] = last_pitcher_by_team.get(team) == pitcher_id
            row["pitcher_mlbam_id"] = id_map.get(pitcher_id)
            row["innings_pitched"] = _ip_from_outs(pd.Series([row["outs_recorded"]])).iloc[0]
            rows.append(row)
        counters["games_seen"] += 1
        current_id = ""
        info = {}
        starters = {}
        current_pitcher_by_team = {}
        last_pitcher_by_team = {}
        game_rows = {}

    for path in paths:
        counters["files_seen"] += 1
        for record in _csv_rows_from_event_file(path):
            rec_type = record[0].strip().lower()
            if rec_type == "id":
                finish_game()
                if limit_games and counters["games_seen"] >= limit_games:
                    return _finalize_event_rows(rows), counters
                current_id = record[1].strip() if len(record) > 1 else ""
                info = {}
                starters = {}
                current_pitcher_by_team = {}
                last_pitcher_by_team = {}
                game_rows = {}
                visteam = ""
                hometeam = ""
                continue
            if not current_id:
                continue
            if rec_type == "info" and len(record) >= 3:
                info[record[1].strip().lower()] = record[2].strip()
                if record[1].strip().lower() == "visteam":
                    visteam = record[2].strip().upper()
                if record[1].strip().lower() == "hometeam":
                    hometeam = record[2].strip().upper()
                continue
            if rec_type in {"start", "sub"} and len(record) >= 6:
                player_id = record[1].strip()
                player_name = record[2].strip().strip('"')
                team_side = record[3].strip()
                position = record[5].strip()
                team = hometeam if team_side == "1" else visteam
                opponent = visteam if team_side == "1" else hometeam
                if position == "1" and team:
                    current_pitcher_by_team[team] = (player_id, player_name)
                    if rec_type == "start":
                        starters[team] = (player_id, player_name)
                    key = (team, player_id)
                    if key not in game_rows:
                        game_rows[key] = _new_pitcher_row(
                            game_date=_event_game_date(info, current_id),
                            game_id=current_id,
                            pitcher_id=player_id,
                            pitcher_name=player_name,
                            team=team,
                            opponent=opponent,
                            is_starter=starters.get(team, ("", ""))[0] == player_id,
                        )
                continue
            if rec_type == "play" and len(record) >= 7:
                batting_side = record[2].strip()
                fielding_team = hometeam if batting_side == "0" else visteam
                opponent = visteam if batting_side == "0" else hometeam
                pitcher = current_pitcher_by_team.get(fielding_team)
                if not pitcher:
                    continue
                pitcher_id, pitcher_name = pitcher
                key = (fielding_team, pitcher_id)
                if key not in game_rows:
                    game_rows[key] = _new_pitcher_row(
                        game_date=_event_game_date(info, current_id),
                        game_id=current_id,
                        pitcher_id=pitcher_id,
                        pitcher_name=pitcher_name,
                        team=fielding_team,
                        opponent=opponent,
                        is_starter=starters.get(fielding_team, ("", ""))[0] == pitcher_id,
                    )
                _apply_play(game_rows[key], record[6])
                last_pitcher_by_team[fielding_team] = pitcher_id
        finish_game()
        if limit_games and counters["games_seen"] >= limit_games:
            break
    return _finalize_event_rows(rows), counters


def _finalize_event_rows(rows: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    for col in OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = np.nan
    int_cols = [
        "pitcher_mlbam_id",
        "outs_recorded",
        "strikeouts",
        "walks",
        "hits_allowed",
        "earned_runs",
        "runs_allowed",
        "home_runs_allowed",
        "batters_faced",
    ]
    for col in int_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").round().astype("Int64")
    return out[OUTPUT_COLUMNS]


def load_pitcher_boxscore_csvs(paths: Sequence[Path], *, id_map: Mapping[str, Optional[int]], limit_games: int) -> Tuple[pd.DataFrame, Dict[str, int]]:
    frames: List[pd.DataFrame] = []
    files_seen = 0
    for path in paths:
        files_seen += 1
        raw = pd.read_csv(path)
        frames.append(normalize_pitching_rows(raw, id_map=id_map))
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS), {"files_seen": 0, "games_seen": 0}
    out = pd.concat(frames, ignore_index=True)
    if limit_games:
        game_ids = list(dict.fromkeys(out["game_id_retrosheet"].dropna().astype(str).tolist()))[:limit_games]
        out = out[out["game_id_retrosheet"].astype(str).isin(game_ids)].copy()
    return out, {"files_seen": files_seen, "games_seen": int(out["game_id_retrosheet"].nunique())}


def write_empty_sample(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(path, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retrosheet-events-dir", default="", help="Directory containing raw Retrosheet *.EVN/*.EVA files.")
    parser.add_argument(
        "--retrosheet-gamelogs-dir",
        default="",
        help="Directory containing Retrosheet/Chadwick pitcher box-score CSVs.",
    )
    parser.add_argument("--season", default="", help="Season/year filter used when discovering local files.")
    parser.add_argument("--limit-games", type=int, default=25)
    parser.add_argument("--allow-empty", action="store_true", help="Allow writing header-only output when no rows are parsed.")
    parser.add_argument(
        "--allow-event-parser-preview",
        action="store_true",
        help=(
            "Allow preview parsing from raw Retrosheet event files. Exact earned-runs parsing is TODO; "
            "prefer Chadwick/Retrosheet pitcher box-score CSVs for inspection-quality samples."
        ),
    )
    parser.add_argument(
        "--pitching-csv",
        default="",
        help="Direct Retrosheet/Chadwick-derived pitcher game-log CSV. Optional compatibility input.",
    )
    parser.add_argument(
        "--chadwick-register-csv",
        default="",
        help=(
            "Chadwick Register CSV with key_retro -> key_mlbam mapping. "
            "First run: make mlb-download-retrosheet-sources. Then pass "
            "backend/mlb/data/raw/retrosheet/chadwick_register/people.csv."
        ),
    )
    parser.add_argument("--out-csv", default="tmp/retrosheet_pitcher_game_logs_sample.csv")
    args = parser.parse_args()

    out_csv = Path(args.out_csv)
    pitching_csv = Path(args.pitching_csv) if args.pitching_csv else None
    events_dir = Path(args.retrosheet_events_dir) if args.retrosheet_events_dir else None
    gamelogs_dir = Path(args.retrosheet_gamelogs_dir) if args.retrosheet_gamelogs_dir else None
    register_csv = Path(args.chadwick_register_csv) if args.chadwick_register_csv else None

    if not any([pitching_csv, events_dir, gamelogs_dir]):
        if not args.allow_empty:
            raise SystemExit(
                "No Retrosheet input supplied. Provide --pitching-csv, --retrosheet-gamelogs-dir, "
                "or --retrosheet-events-dir. Use --allow-empty to write a header-only sample."
            )
        write_empty_sample(out_csv)
        print(
            "[retrosheet-pitcher-logs] wrote header-only CSV "
            "files_seen=0 games_seen=0 pitcher_rows_written=0 mapped_mlbam_count=0 "
            f"unmapped_retrosheet_count=0 out_csv={out_csv} db_writes=0"
        )
        return 0

    id_map = load_chadwick_register(register_csv)
    files_seen = 0
    games_seen = 0

    if pitching_csv:
        if not pitching_csv.exists():
            raise SystemExit(f"Pitching CSV not found: {pitching_csv}")
        out, counters = load_pitcher_boxscore_csvs([pitching_csv], id_map=id_map, limit_games=args.limit_games)
        files_seen += counters["files_seen"]
        games_seen += counters["games_seen"]
    else:
        gamelog_paths = _discover_csv_inputs(gamelogs_dir, args.season or None)
        if gamelog_paths:
            out, counters = load_pitcher_boxscore_csvs(gamelog_paths, id_map=id_map, limit_games=args.limit_games)
            files_seen += counters["files_seen"]
            games_seen += counters["games_seen"]
        else:
            event_paths = _discover_event_inputs(events_dir, args.season or None)
            if event_paths:
                if not args.allow_event_parser_preview:
                    raise SystemExit(
                        "Raw Retrosheet event parsing is preview-only because exact earned-runs attribution "
                        "is not implemented yet. Expected input for normal sample generation: a "
                        "Retrosheet/Chadwick pitcher box-score CSV directory passed via --retrosheet-gamelogs-dir "
                        "with game/date/pitcher/stat columns. To produce approximate event-derived sample rows "
                        "with earned_runs blank, rerun with --allow-event-parser-preview."
                    )
                out, counters = parse_retrosheet_event_files(event_paths, id_map=id_map, limit_games=args.limit_games)
                files_seen += counters["files_seen"]
                games_seen += counters["games_seen"]
            else:
                out = pd.DataFrame(columns=OUTPUT_COLUMNS)

    if out.empty and not args.allow_empty:
        raise SystemExit(
            "No pitcher rows parsed. Expected either Chadwick/Retrosheet pitcher box-score CSVs "
            "with game/date/pitcher/stat columns, or raw Retrosheet *.EVN/*.EVA files. "
            "Use --allow-empty only for a header-only smoke run."
        )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    mapped = int(out["pitcher_mlbam_id"].notna().sum()) if "pitcher_mlbam_id" in out.columns else 0
    pitcher_ids = set(out.get("pitcher_retrosheet_id", pd.Series(dtype=str)).dropna().astype(str))
    mapped_ids = set(
        out.loc[out["pitcher_mlbam_id"].notna(), "pitcher_retrosheet_id"].dropna().astype(str)
    ) if "pitcher_mlbam_id" in out.columns else set()
    unmapped = len(pitcher_ids - mapped_ids)
    print(
        "[retrosheet-pitcher-logs] "
        f"files_seen={files_seen} games_seen={games_seen} pitcher_rows_written={len(out)} "
        f"mapped_mlbam_count={mapped} unmapped_retrosheet_count={unmapped} "
        f"out_csv={out_csv} db_writes=0"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
