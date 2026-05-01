#!/usr/bin/env python3
"""Build a CSV-only Retrosheet pitcher-game-log sample.

Retrosheet is the historical backbone for pitcher game logs.
MLB Stats API remains the live/current-season source.
Chadwick Register is the ID bridge from Retrosheet IDs to MLBAM IDs.

This script intentionally does not write to the database. The first-run path
writes a normalized sample CSV for inspection:

  python -m backend.mlb.scripts.ingest_retrosheet_pitcher_game_logs \
    --pitching-csv path/to/chadwick_pitching_box.csv \
    --chadwick-register-csv path/to/register.csv \
    --out-csv tmp/retrosheet_pitcher_game_logs_sample.csv

Expected input shape:
- A Retrosheet/Chadwick-derived pitcher box-score CSV with one row per
  (game, pitcher), such as output from Chadwick tooling around Retrosheet
  event files. The script accepts several common column aliases.
- Chadwick Register CSV with `key_retro` and `key_mlbam` columns.

Out of scope for this foundation:
- pitch-level parsing
- full event-level run reconstruction
- production cron integration
- DB writes
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

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


def write_empty_sample(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(path, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pitching-csv",
        default="",
        help="Retrosheet/Chadwick-derived pitcher game-log CSV. If omitted, writes header-only sample.",
    )
    parser.add_argument(
        "--chadwick-register-csv",
        default="",
        help="Chadwick Register CSV with key_retro -> key_mlbam mapping.",
    )
    parser.add_argument("--out-csv", default="tmp/retrosheet_pitcher_game_logs_sample.csv")
    args = parser.parse_args()

    out_csv = Path(args.out_csv)
    pitching_csv = Path(args.pitching_csv) if args.pitching_csv else None
    register_csv = Path(args.chadwick_register_csv) if args.chadwick_register_csv else None

    if not pitching_csv:
        write_empty_sample(out_csv)
        print(
            "[retrosheet-pitcher-logs] wrote header-only CSV "
            f"out_csv={out_csv} rows=0 db_writes=0"
        )
        return 0
    if not pitching_csv.exists():
        raise SystemExit(f"Pitching CSV not found: {pitching_csv}")

    id_map = load_chadwick_register(register_csv)
    raw = pd.read_csv(pitching_csv)
    out = normalize_pitching_rows(raw, id_map=id_map)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    mapped = int(out["pitcher_mlbam_id"].notna().sum())
    print(
        "[retrosheet-pitcher-logs] "
        f"input_rows={len(raw)} output_rows={len(out)} mlbam_mapped={mapped} "
        f"out_csv={out_csv} db_writes=0"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
