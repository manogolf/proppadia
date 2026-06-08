#!/usr/bin/env python3
"""Backfill MLB lineup context for research diagnostics only.

This script does not write production tables, model artifacts, uploads, or lane
rules. It fetches MLB StatsAPI boxscores for selected v2 / Quick Card research
rows and emits a local diagnostics artifact.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/mlb/research_gap_analysis"
V2_ROWS = ROOT / "artifacts/analysis/mlb/v2_health_check/v2_health_check_selected_side_only_rows.csv"
QC_ROWS = ROOT / "artifacts/analysis/mlb/v2_health_check/quick_card_selected_side_deduped_rows.csv"
STATSAPI = "https://statsapi.mlb.com/api/v1"


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(dt) else pd.Timestamp(dt).date().isoformat()


def _id_key(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return str(int(val)) if pd.notna(val) else ""


def _clean(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def _iter_dates(start: date, end: date) -> list[date]:
    days: list[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _lineup_group(slot: Any) -> str:
    val = pd.to_numeric(pd.Series([slot]), errors="coerce").iloc[0]
    if pd.isna(val):
        return "bench_missing"
    spot = int(val)
    if 1 <= spot <= 3:
        return "top_1_3"
    if 4 <= spot <= 6:
        return "middle_4_6"
    if 7 <= spot <= 9:
        return "bottom_7_9"
    return "bench_missing"


def _load_selected_universe(start: str, end: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path, source in [(V2_ROWS, "v2_ranking"), (QC_ROWS, "quick_card")]:
        if not path.exists():
            continue
        df = pd.read_csv(path, low_memory=False)
        if df.empty:
            continue
        out = pd.DataFrame(
            {
                "game_date": df.get("date", pd.Series("", index=df.index)).map(_date_key),
                "game_id": df.get("game_id", pd.Series("", index=df.index)).map(_id_key),
                "player_id": df.get("player_id", pd.Series("", index=df.index)).map(_id_key),
                "player_name": df.get("player_name", pd.Series("", index=df.index)).map(_clean),
                "source_category": source,
            }
        )
        frames.append(out)
    if not frames:
        return pd.DataFrame(columns=["game_date", "game_id", "player_id", "player_name", "source_categories"])
    rows = pd.concat(frames, ignore_index=True, sort=False)
    rows = rows[rows["game_date"].between(start, end) & rows["game_id"].ne("") & rows["player_id"].ne("")].copy()
    rows = rows.groupby(["game_date", "game_id", "player_id"], dropna=False).agg(
        player_name=("player_name", lambda s: next((v for v in s if _clean(v)), "")),
        source_categories=("source_category", lambda s: ",".join(sorted(set(map(str, s))))),
    ).reset_index()
    return rows


def _get_json(url: str, timeout: int, retries: int, sleep_seconds: float) -> tuple[dict[str, Any], str]:
    last_error = ""
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 404:
                return {}, "http_404"
            resp.raise_for_status()
            payload = resp.json()
            return payload if isinstance(payload, dict) else {}, ""
        except Exception as exc:
            last_error = f"{type(exc).__name__}:{exc}"
            if attempt < retries:
                time.sleep(sleep_seconds)
    return {}, last_error or "fetch_failed"


def _team_meta(box: dict[str, Any], side: str) -> dict[str, Any]:
    team = (((box.get("teams") or {}).get(side) or {}).get("team") or {})
    return {
        "id": _id_key(team.get("id")),
        "abbr": _clean(team.get("abbreviation") or team.get("teamCode")).upper(),
        "name": _clean(team.get("name")),
    }


def _parse_batting_order(value: Any) -> tuple[int | None, str]:
    raw = _clean(value)
    if not raw:
        return None, ""
    try:
        parsed = int(raw)
    except Exception:
        return None, f"invalid_battingOrder:{raw}"
    slot = parsed // 100
    if 1 <= slot <= 9:
        return slot, ""
    return None, f"invalid_lineup_slot:{raw}"


def _player_rows_from_boxscore(box: dict[str, Any], game_id: str, game_date: str) -> dict[str, dict[str, Any]]:
    home = _team_meta(box, "home")
    away = _team_meta(box, "away")
    parsed: dict[str, dict[str, Any]] = {}
    teams = box.get("teams") or {}
    for side in ("home", "away"):
        team = home if side == "home" else away
        opp = away if side == "home" else home
        players = ((teams.get(side) or {}).get("players") or {})
        for pdata in players.values():
            person = pdata.get("person") or {}
            player_id = _id_key(person.get("id"))
            if not player_id:
                continue
            slot, slot_error = _parse_batting_order(pdata.get("battingOrder"))
            batting = ((pdata.get("stats") or {}).get("batting") or {})
            actual_ab = pd.to_numeric(pd.Series([batting.get("atBats")]), errors="coerce").iloc[0]
            has_batting_stats = bool(batting)
            parse_ok = slot is not None
            missing_reason = ""
            if not parse_ok:
                missing_reason = slot_error or ("no_batting_order" if has_batting_stats else "no_batting_stats")
            parsed[player_id] = {
                "game_date": game_date,
                "game_id": game_id,
                "player_id": player_id,
                "player_name": _clean(person.get("fullName")),
                "team": team["abbr"] or team["name"],
                "opponent": opp["abbr"] or opp["name"],
                "lineup_slot": slot if slot is not None else pd.NA,
                "lineup_group": _lineup_group(slot),
                "started_flag": bool(slot is not None),
                "actual_ab": actual_ab if pd.notna(actual_ab) else pd.NA,
                "source": "mlb_statsapi_boxscore",
                "parse_ok": bool(parse_ok),
                "missing_reason": missing_reason,
            }
    return parsed


def build_context(args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, Any]]:
    start = args.start.isoformat()
    end = args.end.isoformat()
    selected = _load_selected_universe(start, end)
    if selected.empty:
        return pd.DataFrame(), {
            "start": start,
            "end": end,
            "selected_rows": 0,
            "warnings": ["no selected-side rows found"],
        }

    context_by_game: dict[str, dict[str, dict[str, Any]]] = {}
    fetch_errors: dict[str, str] = {}
    for game_id, group in selected.groupby("game_id"):
        game_date = str(group["game_date"].iloc[0])
        url = f"{STATSAPI}/game/{game_id}/boxscore"
        payload, error = _get_json(url, timeout=args.timeout, retries=args.retries, sleep_seconds=args.sleep_seconds)
        if error or not payload:
            fetch_errors[str(game_id)] = error or "empty_boxscore"
            context_by_game[str(game_id)] = {}
            continue
        context_by_game[str(game_id)] = _player_rows_from_boxscore(payload, str(game_id), game_date)
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    rows: list[dict[str, Any]] = []
    for row in selected.to_dict("records"):
        game_id = str(row["game_id"])
        player_id = str(row["player_id"])
        hit = context_by_game.get(game_id, {}).get(player_id)
        if hit:
            out = dict(hit)
            if not out.get("player_name"):
                out["player_name"] = row.get("player_name", "")
            out["source_categories"] = row.get("source_categories", "")
            rows.append(out)
            continue
        rows.append(
            {
                "game_date": row["game_date"],
                "game_id": game_id,
                "player_id": player_id,
                "player_name": row.get("player_name", ""),
                "team": "",
                "opponent": "",
                "lineup_slot": pd.NA,
                "lineup_group": "bench_missing",
                "started_flag": False,
                "actual_ab": pd.NA,
                "source": "mlb_statsapi_boxscore",
                "parse_ok": False,
                "missing_reason": fetch_errors.get(game_id) or "player_not_found_in_boxscore",
                "source_categories": row.get("source_categories", ""),
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["game_date", "game_id", "player_id"]).reset_index(drop=True)
    summary = {
        "start": start,
        "end": end,
        "selected_rows": int(len(selected)),
        "output_rows": int(len(out)),
        "unique_games": int(selected["game_id"].nunique()),
        "fetch_error_games": len(fetch_errors),
        "parse_ok_rows": int(out["parse_ok"].eq(True).sum()) if not out.empty else 0,
        "lineup_slot_non_null": int(pd.to_numeric(out.get("lineup_slot"), errors="coerce").notna().sum()) if not out.empty else 0,
        "started_rows": int(out["started_flag"].eq(True).sum()) if not out.empty else 0,
        "actual_ab_non_null": int(pd.to_numeric(out.get("actual_ab"), errors="coerce").notna().sum()) if not out.empty else 0,
        "fetch_errors": fetch_errors,
    }
    return out, summary


def _write_summary(path: Path, csv_path: Path, summary: dict[str, Any], rows: pd.DataFrame) -> None:
    group_counts = rows["lineup_group"].value_counts(dropna=False).to_dict() if not rows.empty else {}
    reason_counts = rows["missing_reason"].fillna("").replace("", "parse_ok").value_counts(dropna=False).head(20).to_dict() if not rows.empty else {}
    lines = [
        "# MLB Lineup Context Diagnostics Backfill",
        "",
        "Research diagnostics only. No production tables, uploads, lane rules, thresholds, model artifacts, or retraining changed.",
        "",
        "## Summary",
        "",
        f"- Date range: `{summary.get('start')}` to `{summary.get('end')}`",
        f"- Selected-side input rows: `{summary.get('selected_rows')}`",
        f"- Output rows: `{summary.get('output_rows')}`",
        f"- Unique games fetched: `{summary.get('unique_games')}`",
        f"- Games with fetch errors: `{summary.get('fetch_error_games')}`",
        f"- Parsed lineup rows: `{summary.get('parse_ok_rows')}`",
        f"- Non-null lineup slots: `{summary.get('lineup_slot_non_null')}`",
        f"- Started rows: `{summary.get('started_rows')}`",
        f"- Non-null actual AB rows: `{summary.get('actual_ab_non_null')}`",
        "",
        "## Lineup Group Counts",
        "",
    ]
    if group_counts:
        for key, value in group_counts.items():
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- No rows.")
    lines.extend(["", "## Missing / Parse Reasons", ""])
    if reason_counts:
        for key, value in reason_counts.items():
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- No rows.")
    if summary.get("fetch_errors"):
        lines.extend(["", "## Fetch Errors", ""])
        for game_id, error in list(summary["fetch_errors"].items())[:50]:
            lines.append(f"- `{game_id}`: `{error}`")
    lines.extend(["", "## Outputs", "", f"- `{csv_path.relative_to(ROOT)}`", f"- `{path.relative_to(ROOT)}`"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill MLB lineup context diagnostics from StatsAPI boxscores.")
    parser.add_argument("--start", type=_parse_date, required=True, help="Start date YYYY-MM-DD.")
    parser.add_argument("--end", type=_parse_date, required=True, help="End date YYYY-MM-DD.")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory.")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    args = parser.parse_args()
    if args.start > args.end:
        raise SystemExit(f"invalid date range: {args.start} > {args.end}")
    return args


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    start = args.start.isoformat()
    end = args.end.isoformat()
    csv_path = out_dir / f"lineup_context_diagnostics_{start}_{end}.csv"
    md_path = out_dir / "lineup_context_diagnostics_summary.md"
    rows, summary = build_context(args)
    rows.to_csv(csv_path, index=False)
    (out_dir / f"lineup_context_diagnostics_{start}_{end}_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_summary(md_path, csv_path, summary, rows)
    print(f"Wrote {csv_path}")
    print(f"Wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
