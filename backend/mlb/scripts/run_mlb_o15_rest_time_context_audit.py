#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import run_mlb_hits_15_tier_backtest as tier_base
from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/mlb/review_aids"
WINDOWS = ("full_history", "last_30", "last_14", "last_7")


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    except Exception:
        return None


def _i(value: Any) -> int | None:
    number = _f(value)
    if number is None:
        return None
    return int(number)


def _clean(value: Any) -> str:
    return str(value or "").strip().lower()


def _team(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text == "AZ":
        return "ARI"
    if text in {"ATH", "LV", "VIL"}:
        return "OAK"
    return text


def _line_key(value: Any) -> str:
    line = _f(value)
    return f"{line:.1f}" if line is not None else ""


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100.0:.2f}%"


def _num(value: float | None, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _game_date(row: dict[str, Any]) -> str:
    return str(row.get("game_date") or row.get("slate_date") or "")[:10]


def _date_from_path(path: Path) -> str:
    for part in path.parts:
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            return part
    return ""


def _american_units(price: float | None, won: bool) -> float:
    if price is None:
        return 1.0 if won else -1.0
    if won:
        return price / 100.0 if price > 0 else 100.0 / abs(price)
    return -1.0


def _window_labels(date_text: str, latest: str) -> list[str]:
    out = ["full_history"]
    try:
        d = datetime.strptime(date_text, "%Y-%m-%d").date()
        latest_d = datetime.strptime(latest, "%Y-%m-%d").date()
    except Exception:
        return out
    delta = (latest_d - d).days
    if delta <= 29:
        out.append("last_30")
    if delta <= 13:
        out.append("last_14")
    if delta <= 6:
        out.append("last_7")
    return out


def _load_raw_reconcile(execution_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file in sorted(glob.glob(str(execution_root / "*" / "reconcile_rows.csv"))):
        path = Path(file)
        fallback_date = _date_from_path(path)
        for raw in _read_csv(path):
            row = dict(raw)
            row["date"] = _game_date(row) or fallback_date
            row["source_file"] = _rel(path)
            rows.append(row)
    return rows


def _load_o15_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in raw_rows:
        if _clean(raw.get("prop_type")) != "hits" or _line_key(raw.get("line")) != "1.5":
            continue
        result = _clean(raw.get("actual_over_outcome"))
        if result not in {"win", "loss", "push"}:
            continue
        price = _f(raw.get("price_over_american") or raw.get("market_price_over"))
        units = _f(raw.get("pnl_over_1u"))
        if units is None and result in {"win", "loss"}:
            units = _american_units(price, result == "win")
        actual_hits = _f(raw.get("actual_value"))
        row = {
            "date": str(raw.get("date") or "")[:10],
            "game_id": _i(raw.get("game_id")),
            "player_id": _i(raw.get("player_id")),
            "player_name": raw.get("player_name") or raw.get("market_player_name") or "",
            "team": _team(raw.get("team")),
            "opponent": _team(raw.get("opponent")),
            "line": 1.5,
            "side": "over",
            "result": result,
            "price": price,
            "units": units if units is not None else 0.0,
            "actual_hits": actual_hits,
            "game_time": raw.get("game_time") or "",
            "game_day_of_week": _clean(raw.get("game_day_of_week")),
            "time_of_day_bucket": _clean(raw.get("time_of_day_bucket")),
            "d7_hits_rate": _f(raw.get("d7_hits")),
            "d15_hits_rate": _f(raw.get("d15_hits")),
            "d30_hits_rate": _f(raw.get("d30_hits")),
            "d7_hits_runs_rbis": _f(raw.get("d7_hits_runs_rbis")),
            "d15_hits_runs_rbis": _f(raw.get("d15_hits_runs_rbis")),
            "d30_hits_runs_rbis": _f(raw.get("d30_hits_runs_rbis")),
            "starter_expected_hits_allowed": _f(raw.get("starter_expected_hits_allowed")),
            "team_expected_hits_allowed": _f(raw.get("team_expected_hits_allowed")),
            "model_prob": _f(raw.get("model_prob_over")),
            "source_file": raw.get("source_file") or "",
        }
        out.append(row)
    return out


def _load_qc_flags(lanes_root: Path) -> set[tuple[str, str, str]]:
    flags: set[tuple[str, str, str]] = set()
    for path in sorted((lanes_root / "today").glob("20??-??-??/quick_card_hits_*.csv")):
        date_text = path.parent.name
        for row in _read_csv(path):
            if _clean(row.get("prop_type")) != "hits":
                continue
            if _clean(row.get("side")) != "over" or _line_key(row.get("line")) != "1.5":
                continue
            pid = _i(row.get("player_id"))
            if pid is None:
                continue
            flags.add((date_text, str(pid), "1.5"))
    return flags


def _load_alternate_flags(review_aids_dir: Path) -> set[tuple[str, str, str]]:
    flags: set[tuple[str, str, str]] = set()
    for path in sorted(review_aids_dir.glob("hits_o15_alternate_discovery_*.csv")):
        date_text = _date_from_path(path) or str(path.name).split("_")[-1].replace(".csv", "")
        for row in _read_csv(path):
            pid = _i(row.get("player_id"))
            if pid is None:
                continue
            flags.add((date_text[:10], str(pid), _line_key(row.get("line") or 1.5)))
    return flags


def _component_pa(row: dict[str, Any]) -> float | None:
    explicit = _f(row.get("plate_appearances"))
    if explicit is not None:
        return explicit
    ab = _f(row.get("at_bats"))
    if ab is None:
        return None
    return (
        ab
        + (_f(row.get("walks")) or 0.0)
        + (_f(row.get("hit_by_pitch")) or 0.0)
        + (_f(row.get("sacrifice_flies")) or 0.0)
        + (_f(row.get("sacrifice_hits")) or 0.0)
        + (_f(row.get("catcher_interference")) or 0.0)
    )


def _fetch_actual_pa(rows: list[dict[str, Any]]) -> dict[tuple[str, int, int], dict[str, Any]]:
    player_ids = sorted({int(_i(row.get("player_id")) or 0) for row in rows if _i(row.get("player_id")) is not None})
    game_ids = sorted({int(_i(row.get("game_id")) or 0) for row in rows if _i(row.get("game_id")) is not None})
    if not player_ids or not game_ids:
        return {}
    db_rows = pg_fetchall(
        """
SELECT
  ps.game_date::date AS game_date,
  ps.game_id,
  ps.player_id,
  ps.hits,
  ps.at_bats,
  ps.plate_appearances,
  ps.walks,
  ps.hit_by_pitch,
  ps.sacrifice_flies,
  ps.sacrifice_hits,
  ps.catcher_interference
FROM mlb.player_stats ps
WHERE ps.player_id = ANY(%s)
  AND ps.game_id = ANY(%s)
""",
        (player_ids, game_ids),
    )
    out: dict[tuple[str, int, int], dict[str, Any]] = {}
    for row in db_rows or []:
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        if game_id is None or player_id is None:
            continue
        date_text = str(row.get("game_date"))[:10]
        item = dict(row)
        item["actual_plate_appearances"] = _component_pa(item)
        out[(date_text, int(game_id), int(player_id))] = item
    return out


def _build_team_game_schedule(raw_rows: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, Any]]:
    games: dict[tuple[str, int], dict[str, Any]] = {}
    for row in raw_rows:
        date_text = str(row.get("date") or "")[:10]
        game_id = _i(row.get("game_id"))
        if not date_text or game_id is None:
            continue
        game_time = row.get("game_time") or ""
        game_dt = _parse_dt(game_time)
        bucket = _clean(row.get("time_of_day_bucket")) or _bucket_from_dt(game_dt)
        for team in {_team(row.get("team")), _team(row.get("opponent"))}:
            if not team:
                continue
            key = (team, int(game_id))
            existing = games.get(key)
            if existing and existing.get("game_time"):
                continue
            games[key] = {
                "team": team,
                "game_id": int(game_id),
                "date": date_text,
                "game_time": game_time,
                "game_dt": game_dt,
                "time_of_day_bucket": bucket,
                "opponent": _team(row.get("opponent")) if _team(row.get("team")) == team else _team(row.get("team")),
            }
    return games


def _bucket_from_dt(dt: datetime | None) -> str:
    if dt is None:
        return ""
    hour = dt.hour
    if hour < 16:
        return "afternoon"
    if hour < 20:
        return "evening"
    return "late"


def _is_day_bucket(bucket: str) -> bool:
    return bucket in {"afternoon", "day", "early"}


def _is_night_bucket(bucket: str) -> bool:
    return bucket in {"evening", "late", "night"}


def _annotate_rest_context(rows: list[dict[str, Any]], raw_rows: list[dict[str, Any]]) -> None:
    games = _build_team_game_schedule(raw_rows)
    by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for game in games.values():
        by_team[str(game["team"])].append(game)
    for team in by_team:
        by_team[team].sort(
            key=lambda g: (
                g.get("game_dt") or datetime.strptime(str(g.get("date")), "%Y-%m-%d").replace(tzinfo=timezone.utc),
                int(g.get("game_id") or 0),
            )
        )

    previous_by_team_game: dict[tuple[str, int], dict[str, Any]] = {}
    for team, team_games in by_team.items():
        prev: dict[str, Any] | None = None
        for game in team_games:
            if prev is not None:
                previous_by_team_game[(team, int(game["game_id"]))] = prev
            prev = game

    for row in rows:
        team = _team(row.get("team"))
        game_id = _i(row.get("game_id"))
        current = games.get((team, int(game_id or 0)), {})
        prev = previous_by_team_game.get((team, int(game_id or 0)), {})
        current_bucket = _clean(row.get("time_of_day_bucket")) or _clean(current.get("time_of_day_bucket")) or "missing"
        row["time_of_day_bucket"] = current_bucket
        row["game_time"] = row.get("game_time") or current.get("game_time") or ""
        row["previous_team_game_date"] = prev.get("date") or ""
        row["previous_team_game_time"] = prev.get("game_time") or ""
        row["previous_team_time_of_day_bucket"] = _clean(prev.get("time_of_day_bucket")) or "missing"
        row["previous_team_game_id"] = prev.get("game_id") or ""
        row["previous_team_opponent"] = prev.get("opponent") or ""

        current_dt = _parse_dt(row.get("game_time")) or current.get("game_dt")
        prev_dt = prev.get("game_dt")
        days_between = None
        hours_between = None
        if row.get("date") and prev.get("date"):
            try:
                days_between = (
                    datetime.strptime(str(row.get("date"))[:10], "%Y-%m-%d").date()
                    - datetime.strptime(str(prev.get("date"))[:10], "%Y-%m-%d").date()
                ).days
            except Exception:
                days_between = None
        if current_dt is not None and prev_dt is not None:
            try:
                hours_between = (current_dt - prev_dt).total_seconds() / 3600.0
            except Exception:
                hours_between = None
        row["days_since_previous_team_game"] = days_between
        row["hours_since_previous_team_game"] = hours_between
        row["rest_day_before_game"] = bool(days_between is not None and days_between > 1)
        row["short_turnaround_proxy"] = (
            "yes" if hours_between is not None and hours_between < 22.0 else "no" if hours_between is not None else "missing"
        )
        prev_bucket = str(row.get("previous_team_time_of_day_bucket") or "missing")
        if _is_day_bucket(current_bucket) and _is_night_bucket(prev_bucket):
            sequence = "day_after_night"
        elif _is_day_bucket(current_bucket) and _is_day_bucket(prev_bucket):
            sequence = "day_after_day"
        elif _is_night_bucket(current_bucket) and _is_night_bucket(prev_bucket):
            sequence = "night_after_night"
        elif _is_night_bucket(current_bucket) and _is_day_bucket(prev_bucket):
            sequence = "night_after_day"
        elif not prev:
            sequence = "no_previous_team_game_in_artifacts"
        else:
            sequence = "missing"
        row["team_time_sequence_bucket"] = sequence


def _annotate_rows(rows: list[dict[str, Any]], raw_rows: list[dict[str, Any]], lanes_root: Path, review_aids_dir: Path) -> None:
    pa_context = _fetch_actual_pa(rows)
    qc_flags = _load_qc_flags(lanes_root)
    alt_flags = _load_alternate_flags(review_aids_dir)
    _annotate_rest_context(rows, raw_rows)
    for row in rows:
        key = (str(row.get("date") or "")[:10], int(_i(row.get("game_id")) or 0), int(_i(row.get("player_id")) or 0))
        ctx = pa_context.get(key, {})
        row["actual_plate_appearances"] = _f(ctx.get("actual_plate_appearances"))
        if _f(ctx.get("hits")) is not None:
            row["actual_hits"] = _f(ctx.get("hits"))
        row["two_hit_winner"] = bool(_clean(row.get("result")) == "win" and (_f(row.get("actual_hits")) or 0.0) >= 2.0)
        row["exactly_one_hit_loss"] = bool(_clean(row.get("result")) == "loss" and (_f(row.get("actual_hits")) == 1.0))
        row["zero_hit_loss"] = bool(_clean(row.get("result")) == "loss" and (_f(row.get("actual_hits")) == 0.0))
        row["d7_hot_candidate"] = bool((_f(row.get("d7_hits_rate")) or -999.0) > 1.0)
        row["d15_consistent_candidate"] = bool((_f(row.get("d15_hits_rate")) or -999.0) > 1.0)
        row["favorable_starter_candidate"] = bool((_f(row.get("starter_expected_hits_allowed")) or -999.0) >= 5.0)
        row["qc_candidate"] = (
            str(row.get("date") or "")[:10],
            str(int(_i(row.get("player_id")) or 0)),
            "1.5",
        ) in qc_flags
        row["alternate_discovery_candidate"] = (
            str(row.get("date") or "")[:10],
            str(int(_i(row.get("player_id")) or 0)),
            "1.5",
        ) in alt_flags


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for row in rows if _clean(row.get("result")) == "win")
    losses = sum(1 for row in rows if _clean(row.get("result")) == "loss")
    pushes = sum(1 for row in rows if _clean(row.get("result")) == "push")
    resolved = wins + losses + pushes
    units = sum((_f(row.get("units")) or 0.0) for row in rows if _clean(row.get("result")) in {"win", "loss", "push"})
    two_hit = sum(1 for row in rows if row.get("two_hit_winner") is True)
    one_hit = sum(1 for row in rows if row.get("exactly_one_hit_loss") is True)
    zero_hit = sum(1 for row in rows if row.get("zero_hit_loss") is True)

    def avg(col: str) -> float | None:
        vals = [_f(row.get(col)) for row in rows]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    return {
        "rows": len(rows),
        "resolved": resolved,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / resolved if resolved else None,
        "units": units,
        "avg_odds": avg("price"),
        "two_hit_conversion_rate": two_hit / resolved if resolved else None,
        "exactly_1_hit_loss_rate": one_hit / resolved if resolved else None,
        "zero_hit_loss_rate": zero_hit / resolved if resolved else None,
        "avg_pa": avg("actual_plate_appearances"),
        "avg_d7_hits_rate": avg("d7_hits_rate"),
        "avg_d15_hits_rate": avg("d15_hits_rate"),
        "avg_starter_expected_hits_allowed": avg("starter_expected_hits_allowed"),
        "avg_hours_since_previous_team_game": avg("hours_since_previous_team_game"),
    }


def _summarize(
    rows: list[dict[str, Any]],
    latest: str,
    segment_specs: list[tuple[str, Callable[[dict[str, Any]], bool]]],
    dimensions: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for window in WINDOWS:
        wrows = [row for row in rows if window in _window_labels(str(row.get("date") or "")[:10], latest)]
        for segment, predicate in segment_specs:
            srows = [row for row in wrows if predicate(row)]
            for dimension, field in dimensions:
                values = sorted({str(row.get(field) if row.get(field) not in (None, "") else "missing") for row in srows})
                for value in values:
                    selected = [row for row in srows if str(row.get(field) if row.get(field) not in (None, "") else "missing") == value]
                    item = {
                        "window": window,
                        "segment": segment,
                        "dimension": dimension,
                        "bucket": value,
                    }
                    item.update(_metrics(selected))
                    out.append(item)
    return out


def _write_report(path: Path, rows: list[dict[str, Any]], bucket_rows: list[dict[str, Any]], latest: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    full = [row for row in bucket_rows if row.get("window") == "full_history"]
    last14 = [row for row in bucket_rows if row.get("window") == "last_14"]
    missing_counts = {
        "game_time": sum(1 for row in rows if not row.get("game_time")),
        "time_of_day_bucket": sum(1 for row in rows if not row.get("time_of_day_bucket") or row.get("time_of_day_bucket") == "missing"),
        "game_day_of_week": sum(1 for row in rows if not row.get("game_day_of_week") or row.get("game_day_of_week") == "missing"),
        "previous_team_game": sum(1 for row in rows if not row.get("previous_team_game_date")),
        "actual_pa": sum(1 for row in rows if _f(row.get("actual_plate_appearances")) is None),
    }

    def line(row: dict[str, Any]) -> str:
        return (
            f"| `{row.get('segment')}` | `{row.get('dimension')}` | `{row.get('bucket')}` | `{row.get('rows')}` | "
            f"`{row.get('resolved')}` | `{_pct(_f(row.get('wr')))}` | `{_pct(_f(row.get('roi')))}` | "
            f"`{_num(_f(row.get('units')))}` | `{_num(_f(row.get('avg_odds')))}` | "
            f"`{_pct(_f(row.get('two_hit_conversion_rate')))}` | "
            f"`{_pct(_f(row.get('exactly_1_hit_loss_rate')))}` | "
            f"`{_pct(_f(row.get('zero_hit_loss_rate')))}` | `{_num(_f(row.get('avg_pa')))}` |"
        )

    key_dims = {"time_of_day_bucket", "team_time_sequence", "short_turnaround_proxy", "rest_day_before_game"}
    key_full = [row for row in full if row.get("dimension") in key_dims and row.get("segment") in {"all_o15", "d7+d15", "d7+d15+starter>=5", "QC+d7+d15+starter>=5"}]
    key_last14 = [row for row in last14 if row.get("dimension") in key_dims and row.get("segment") in {"all_o15", "d7+d15+starter>=5", "QC+d7+d15+starter>=5"}]
    strongest = sorted(
        [row for row in key_full if _f(row.get("roi")) is not None and int(row.get("resolved") or 0) >= 20],
        key=lambda r: _f(r.get("roi")) or -999,
        reverse=True,
    )[:8]
    weakest = sorted(
        [row for row in key_full if _f(row.get("roi")) is not None and int(row.get("resolved") or 0) >= 20],
        key=lambda r: _f(r.get("roi")) or 999,
    )[:8]

    lines = [
        "# Hits Over 1.5 Rest / Time Context Audit",
        "",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Latest completed slate: `{latest or 'n/a'}`",
        "- Candidate universe: reconciled `prop_type=hits`, `side=over`, `line=1.5` rows.",
        "- Scope: analysis only; no board logic, production selector, upload, threshold, grading, or matching changes.",
        "",
        "## Available Fields",
        "",
        f"- Rows: `{len(rows)}`",
        f"- `game_time` present: `{len(rows) - missing_counts['game_time']}` / `{len(rows)}`",
        f"- `time_of_day_bucket` present: `{len(rows) - missing_counts['time_of_day_bucket']}` / `{len(rows)}`",
        f"- `game_day_of_week` present: `{len(rows) - missing_counts['game_day_of_week']}` / `{len(rows)}`",
        f"- previous same-team game derivable: `{len(rows) - missing_counts['previous_team_game']}` / `{len(rows)}`",
        f"- actual PA present: `{len(rows) - missing_counts['actual_pa']}` / `{len(rows)}`",
        "- Previous-game/rest buckets are derived from same-team `game_id`/`game_time` rows in execution reconcile artifacts, not from a production rule.",
        "",
        "## Strongest Full-History Buckets (minimum 20 resolved)",
        "",
        "| segment | dimension | bucket | rows | resolved | WR | ROI | units | avg odds | 2-hit conversion | 1-hit loss | 0-hit loss | avg PA |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines.extend(line(row) for row in strongest)
    lines.extend(
        [
            "",
            "## Weakest Full-History Buckets (minimum 20 resolved)",
            "",
            "| segment | dimension | bucket | rows | resolved | WR | ROI | units | avg odds | 2-hit conversion | 1-hit loss | 0-hit loss | avg PA |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    lines.extend(line(row) for row in weakest)
    lines.extend(
        [
            "",
            "## Key Full-History Rest/Time Tables",
            "",
            "| segment | dimension | bucket | rows | resolved | WR | ROI | units | avg odds | 2-hit conversion | 1-hit loss | 0-hit loss | avg PA |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    lines.extend(line(row) for row in key_full)
    lines.extend(
        [
            "",
            "## Key Last-14 Rest/Time Tables",
            "",
            "| segment | dimension | bucket | rows | resolved | WR | ROI | units | avg odds | 2-hit conversion | 1-hit loss | 0-hit loss | avg PA |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    lines.extend(line(row) for row in key_last14)

    d7_starter = [
        row
        for row in full
        if row.get("segment") == "d7+d15+starter>=5" and row.get("dimension") == "team_time_sequence"
    ]
    meaningful = [row for row in d7_starter if int(row.get("resolved") or 0) >= 10]
    spread = None
    if len(meaningful) >= 2:
        rois = [_f(row.get("roi")) for row in meaningful if _f(row.get("roi")) is not None]
        if rois:
            spread = max(rois) - min(rois)
    lines.extend(["", "## Answer", ""])
    if spread is not None:
        lines.append(f"- In the `d7+d15+starter>=5` segment, team-time-sequence ROI spread is `{_pct(spread)}` across buckets with at least 10 resolved rows.")
    lines.append("- Treat small rest/time buckets as warning context until live samples grow; the audit does not change any board threshold.")
    lines.append("- If a bucket repeatedly shows high exactly-1-hit loss rates, it is a candidate board context column or warning flag, not a filter yet.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit rest/time-of-day effects for hits o1.5 two-hit conversion.")
    parser.add_argument("--execution-root", default="artifacts/analysis/mlb/execution_vs_model")
    parser.add_argument("--lanes-root", default="backend/mlb/exports/model_v2/lanes")
    parser.add_argument("--review-aids-dir", default="artifacts/analysis/mlb/review_aids")
    parser.add_argument("--out-dir", default=str(OUT_DIR))
    args = parser.parse_args()

    raw_rows = _load_raw_reconcile(ROOT / args.execution_root)
    rows = _load_o15_rows(raw_rows)
    latest = max([str(row.get("date") or "")[:10] for row in rows], default="")
    tier_base._enrich_rows(rows)
    _annotate_rows(rows, raw_rows, ROOT / args.lanes_root, ROOT / args.review_aids_dir)

    def d7(row: dict[str, Any]) -> bool:
        return bool(row.get("d7_hot_candidate"))

    def d15(row: dict[str, Any]) -> bool:
        return bool(row.get("d15_consistent_candidate"))

    def starter5(row: dict[str, Any]) -> bool:
        return bool(row.get("favorable_starter_candidate"))

    def qc(row: dict[str, Any]) -> bool:
        return bool(row.get("qc_candidate"))

    def alt(row: dict[str, Any]) -> bool:
        return bool(row.get("alternate_discovery_candidate"))

    segment_specs: list[tuple[str, Callable[[dict[str, Any]], bool]]] = [
        ("all_o15", lambda r: True),
        ("d7+d15", lambda r: d7(r) and d15(r)),
        ("d7+d15+starter>=5", lambda r: d7(r) and d15(r) and starter5(r)),
        ("QC+d7+d15+starter>=5", lambda r: qc(r) and d7(r) and d15(r) and starter5(r)),
        ("alternate_discovery", lambda r: alt(r)),
        ("alternate+d7+d15+starter>=5", lambda r: alt(r) and d7(r) and d15(r) and starter5(r)),
    ]
    dimensions = [
        ("time_of_day_bucket", "time_of_day_bucket"),
        ("game_day_of_week", "game_day_of_week"),
        ("team_time_sequence", "team_time_sequence_bucket"),
        ("rest_day_before_game", "rest_day_before_game"),
        ("short_turnaround_proxy", "short_turnaround_proxy"),
        ("previous_team_time_of_day_bucket", "previous_team_time_of_day_bucket"),
    ]
    bucket_rows = _summarize(rows, latest, segment_specs, dimensions)

    out_dir = ROOT / args.out_dir
    rows_path = out_dir / "o15_rest_time_context_rows.csv"
    buckets_path = out_dir / "o15_rest_time_context_buckets.csv"
    report_path = out_dir / "o15_rest_time_context_audit.md"
    _write_csv(rows_path, rows)
    _write_csv(buckets_path, bucket_rows)
    _write_report(report_path, rows, bucket_rows, latest)

    print(f"o15_rest_time_rows={len(rows)}")
    print(f"latest_completed_slate={latest}")
    print(f"rows_with_game_time={sum(1 for row in rows if row.get('game_time'))}")
    print(f"rows_with_previous_team_game={sum(1 for row in rows if row.get('previous_team_game_date'))}")
    print(f"rows_with_actual_pa={sum(1 for row in rows if _f(row.get('actual_plate_appearances')) is not None)}")
    print(f"buckets_csv={_rel(buckets_path)}")
    print(f"rows_csv={_rel(rows_path)}")
    print(f"report_md={_rel(report_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
