#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.scripts.report_mlb_hits_environment import (
    _blend_weighted,
    _canonical_team_code,
    _clamp,
    _fetch_multi_season_starter_baselines,
    _fetch_team_bullpen_hits_allowed_form,
    _fetch_team_hits_form,
)
from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
WINDOWS = ("full_history", "last_30", "last_14", "last_7", "latest_completed_slate")
OUTPUT_DIR = ROOT / "artifacts/analysis/mlb/review_aids"
ENVIRONMENT_COMPONENT_COLUMNS = [
    "pitcher_expected_hits_allowed_weighted",
    "pitcher_base",
    "offense_hits_pg_last7",
    "offense_hits_pg_last15",
    "offense_hits_pg_last30",
    "offense_hits_form_blended",
    "league_offense_hits_form_blended",
    "offense_factor_vs_league",
    "offense_factor_vs_league_clamped",
    "bullpen_hits_allowed_pg_last7",
    "bullpen_hits_allowed_pg_last15",
    "bullpen_hits_allowed_pg_last30",
    "bullpen_hits_allowed_form_blended",
    "starter_expected_hits_allowed",
    "team_expected_hits_allowed",
]

OFFENSE_FACTOR_LINEAGE_COLUMNS = [
    "offense_context_as_of_date",
    "offense_window_excludes_eval_date",
    "offense_window_max_source_game_date",
    "local_team_hits_parity_status",
    "team_hits_mismatch_count",
    "team_hits_rescheduled_outside_window_count",
    "offense_factor_lineage_health_generated_at",
]


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
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _clean(value: Any) -> str:
    return str(value or "").strip().lower()


def _cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _lineage_from_source(row: dict[str, Any]) -> dict[str, Any]:
    out = {field: _cell(row.get(field)) for field in OFFENSE_FACTOR_LINEAGE_COLUMNS}
    if not out.get("local_team_hits_parity_status"):
        out["local_team_hits_parity_status"] = "unknown"
    return out


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100.0:.2f}%"


def _num(value: float | None, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


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


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _american_to_units(price: float | None, won: bool) -> float:
    if price is None:
        return 1.0 if won else -1.0
    if won:
        return price / 100.0 if price > 0 else 100.0 / abs(price)
    return -1.0


def _date_from_path(path: Path) -> str:
    for part in path.parts:
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            return part
    return ""


def _key(date: Any, player_id: Any, line: Any, side: Any) -> tuple[str, str, str, str]:
    pid = _i(player_id)
    line_v = _f(line)
    return (
        str(date or "")[:10],
        str(pid or ""),
        f"{line_v:.1f}" if line_v is not None else "",
        _clean(side),
    )


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
    if delta == 0:
        out.append("latest_completed_slate")
    return out


def _fetch_player_context(rows: list[dict[str, Any]]) -> dict[tuple[str, int, int], dict[str, Any]]:
    player_ids = sorted({int(r["player_id"]) for r in rows if _i(r.get("player_id")) is not None})
    game_ids = sorted({int(r["game_id"]) for r in rows if _i(r.get("game_id")) is not None})
    if not player_ids or not game_ids:
        return {}
    db_rows = pg_fetchall(
        """
SELECT
  ps.game_date::date AS game_date,
  ps.game_id,
  ps.player_id,
  ps.team,
  ps.opponent,
  pds.d7_hits::float8 AS d7_hits,
  pds.d15_hits::float8 AS d15_hits,
  pds.d30_hits::float8 AS d30_hits
FROM mlb.player_stats ps
LEFT JOIN mlb.player_derived_stats pds
  ON pds.player_id = ps.player_id
 AND pds.game_id = ps.game_id
WHERE ps.player_id = ANY(%s)
  AND ps.game_id = ANY(%s)
""",
        (player_ids, game_ids),
    )
    out: dict[tuple[str, int, int], dict[str, Any]] = {}
    for row in db_rows or []:
        date_text = str(row.get("game_date"))[:10]
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        if game_id is None or player_id is None:
            continue
        out[(date_text, int(game_id), int(player_id))] = {
            "player_team": _canonical_team_code(row.get("team")),
            "opponent_team": _canonical_team_code(row.get("opponent")),
            "d7_hits_rate": _f(row.get("d7_hits")),
            "d15_hits_rate": _f(row.get("d15_hits")),
            "d30_hits_rate": _f(row.get("d30_hits")),
        }
    return out


def _fetch_starters(rows: list[dict[str, Any]]) -> dict[tuple[str, int, str, str], dict[str, Any]]:
    game_ids = sorted({int(r["game_id"]) for r in rows if _i(r.get("game_id")) is not None})
    if not game_ids:
        return {}
    db_rows = pg_fetchall(
        """
SELECT
  game_date::date AS game_date,
  game_id,
  player_id,
  team AS pitcher_team,
  opponent AS offense_team,
  hits_allowed,
  outs_recorded
FROM mlb.player_stats
WHERE game_id = ANY(%s)
  AND COALESCE(is_starter, 0) = 1
  AND (position = 'P' OR hits_allowed IS NOT NULL)
  AND COALESCE(outs_recorded, 0) > 0
ORDER BY game_date, game_id, team, outs_recorded DESC
""",
        (game_ids,),
    )
    out: dict[tuple[str, int, str, str], dict[str, Any]] = {}
    for row in db_rows or []:
        date_text = str(row.get("game_date"))[:10]
        game_id = _i(row.get("game_id"))
        pitcher_team = _canonical_team_code(row.get("pitcher_team"))
        offense_team = _canonical_team_code(row.get("offense_team"))
        starter_id = _i(row.get("player_id"))
        if game_id is None or starter_id is None or not pitcher_team or not offense_team:
            continue
        out.setdefault(
            (date_text, int(game_id), pitcher_team, offense_team),
            {"starter_player_id": int(starter_id), "pitcher_team": pitcher_team, "offense_team": offense_team},
        )
    return out


def _enrich_rows(rows: list[dict[str, Any]]) -> None:
    player_ctx = _fetch_player_context(rows)
    starters = _fetch_starters(rows)
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_date[str(row.get("date") or "")].append(row)

    date_cache: dict[str, dict[str, Any]] = {}
    for date_text in sorted(by_date):
        if not date_text:
            continue
        team_form = _fetch_team_hits_form(date_text)
        bullpen_form = _fetch_team_bullpen_hits_allowed_form(date_text)
        starter_baselines, _ = _fetch_multi_season_starter_baselines(
            eval_date=date_text,
            seasons_back=3,
            season_weight_decay=0.70,
            min_starts=5,
        )
        league_offense = _blend_weighted(
            [(f.get("hits_pg_last7"), 0.50) for f in team_form.values()]
            + [(f.get("hits_pg_last15"), 0.30) for f in team_form.values()]
            + [(f.get("hits_pg_last30"), 0.20) for f in team_form.values()]
        )
        date_cache[date_text] = {
            "team_form": team_form,
            "bullpen_form": bullpen_form,
            "starter_baselines": starter_baselines,
            "league_offense": league_offense,
        }

    for row in rows:
        date_text = str(row.get("date") or "")
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        ctx = player_ctx.get((date_text, int(game_id or 0), int(player_id or 0)), {})
        row.update({k: v for k, v in ctx.items() if v is not None and row.get(k) in {None, ""}})
        pitcher_team = _canonical_team_code(ctx.get("opponent_team") or row.get("opponent"))
        offense_team = _canonical_team_code(ctx.get("player_team") or row.get("team"))
        starter = starters.get((date_text, int(game_id or 0), pitcher_team, offense_team), {})
        starter_id = _i(starter.get("starter_player_id"))
        cache = date_cache.get(date_text, {})
        team_form = cache.get("team_form", {})
        bullpen_form = cache.get("bullpen_form", {})
        starter_baselines = cache.get("starter_baselines", {})
        league_offense = _f(cache.get("league_offense"))
        offense_form = team_form.get(offense_team, {})
        offense_hits = _blend_weighted(
            [
                (offense_form.get("hits_pg_last7"), 0.50),
                (offense_form.get("hits_pg_last15"), 0.30),
                (offense_form.get("hits_pg_last30"), 0.20),
            ]
        )
        offense_factor = offense_hits / league_offense if offense_hits is not None and league_offense else None
        offense_factor_clamped = _clamp(offense_factor, 0.70, 1.30)
        baseline = starter_baselines.get(int(starter_id or 0), {})
        starter_expected = _f(baseline.get("expected_hits_allowed_weighted"))
        matchup_expected = (
            starter_expected * offense_factor_clamped
            if starter_expected is not None and offense_factor_clamped is not None
            else None
        )
        bullpen = bullpen_form.get(pitcher_team, {})
        bullpen_hits_allowed_pg_last7 = bullpen.get("bullpen_hits_allowed_pg_last7")
        bullpen_hits_allowed_pg_last15 = bullpen.get("bullpen_hits_allowed_pg_last15")
        bullpen_hits_allowed_pg_last30 = bullpen.get("bullpen_hits_allowed_pg_last30")
        bullpen_expected = _blend_weighted(
            [
                (bullpen_hits_allowed_pg_last7, 0.50),
                (bullpen_hits_allowed_pg_last15, 0.30),
                (bullpen_hits_allowed_pg_last30, 0.20),
            ]
        )
        row.update(
            {
                "opposing_starter_player_id": starter_id,
                "pitcher_expected_hits_allowed_weighted": starter_expected,
                "pitcher_base": starter_expected,
                "offense_hits_pg_last7": _f(offense_form.get("hits_pg_last7")),
                "offense_hits_pg_last15": _f(offense_form.get("hits_pg_last15")),
                "offense_hits_pg_last30": _f(offense_form.get("hits_pg_last30")),
                "offense_hits_form_blended": offense_hits,
                "league_offense_hits_form_blended": league_offense,
                "offense_factor_vs_league": offense_factor,
                "offense_factor_vs_league_clamped": offense_factor_clamped,
                "bullpen_hits_allowed_pg_last7": _f(bullpen_hits_allowed_pg_last7),
                "bullpen_hits_allowed_pg_last15": _f(bullpen_hits_allowed_pg_last15),
                "bullpen_hits_allowed_pg_last30": _f(bullpen_hits_allowed_pg_last30),
                "bullpen_hits_allowed_form_blended": bullpen_expected,
                "starter_expected_hits_allowed": matchup_expected,
                "starter_context_status": "projected" if matchup_expected is not None else "missing",
                "team_expected_hits_allowed": (
                    matchup_expected + bullpen_expected
                    if matchup_expected is not None and bullpen_expected is not None
                    else None
                ),
                **_lineage_from_source(row),
            }
        )


def _o15_hitter_tier(d7: float | None, d15: float | None) -> str:
    if d7 is not None and d15 is not None:
        if d7 > 1.30 and d15 > 1.20:
            return "A"
        if d7 > 1.10 and d15 > 1.10:
            return "B"
    return "C"


def _u15_hitter_tier(d7: float | None, d15: float | None) -> str:
    if d7 is not None and d15 is not None:
        if d7 < 1.0 and d15 < 1.0:
            return "A"
        if d7 < 1.1 and d15 < 1.1:
            return "B"
    return "C"


def _o15_pitcher_tier(expected: float | None) -> str:
    if expected is None:
        return "U"
    if expected >= 5.5:
        return "A"
    if expected >= 5.0:
        return "B"
    if expected >= 4.5:
        return "C"
    return "D"


def _u15_pitcher_tier(expected: float | None) -> str:
    if expected is None:
        return "U"
    if expected < 4.5:
        return "A"
    if expected < 5.0:
        return "B"
    if expected < 5.5:
        return "C"
    return "D"


def _load_reconcile_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file in sorted(glob.glob(str(root / "*" / "reconcile_rows.csv"))):
        path = Path(file)
        for raw in _read_csv(path):
            if _clean(raw.get("prop_type")) != "hits" or _f(raw.get("line")) != 1.5:
                continue
            date_text = str(raw.get("game_date") or raw.get("slate_date") or _date_from_path(path))[:10]
            base = {
                "date": date_text,
                "game_id": _i(raw.get("game_id")),
                "player_id": _i(raw.get("player_id")),
                "player_name": raw.get("player_name"),
                "team": raw.get("team"),
                "opponent": raw.get("opponent"),
                "line": 1.5,
                "d7_hits_rate": _f(raw.get("d7_hits")),
                "d15_hits_rate": _f(raw.get("d15_hits")),
                "d30_hits_rate": _f(raw.get("d30_hits")),
                "model_prob_over": _f(raw.get("model_prob_over")),
                "model_prob_under": _f(raw.get("model_prob_under")),
                "price_over": _f(raw.get("price_over_american") or raw.get("market_price_over")),
                "price_under": _f(raw.get("price_under_american") or raw.get("market_price_under")),
                "actual_over_outcome": _clean(raw.get("actual_over_outcome")),
                "actual_under_outcome": _clean(raw.get("actual_under_outcome")),
                "pnl_over_1u": _f(raw.get("pnl_over_1u")),
                "pnl_under_1u": _f(raw.get("pnl_under_1u")),
                **_lineage_from_source(raw),
            }
            rows.append(base)
    _enrich_rows(rows)
    out: list[dict[str, Any]] = []
    for base in rows:
        for side in ("over", "under"):
            result = _clean(base.get(f"actual_{side}_outcome"))
            price = _f(base.get(f"price_{side}"))
            units = _f(base.get(f"pnl_{side}_1u"))
            if units is None and result in {"win", "loss"}:
                units = _american_to_units(price, result == "win")
            row = dict(base)
            row.update(
                {
                    "side": side,
                    "result": result,
                    "price": price,
                    "units": units if units is not None else 0.0,
                    "model_prob": _f(base.get(f"model_prob_{side}")),
                }
            )
            out.append(row)
    return out


def _load_placed_flags(actual_root: Path) -> set[tuple[str, str, str, str]]:
    placed: set[tuple[str, str, str, str]] = set()
    for file in sorted(glob.glob(str(actual_root / "*" / "actual_wagers_by_source_*.csv"))):
        path = Path(file)
        for row in _read_csv(path):
            if str(row.get("row_type") or "") != "actual_wager":
                continue
            prop = _clean(row.get("prop_type") or row.get("parsed_prop_type"))
            if prop != "hits" or _f(row.get("line") or row.get("parsed_line")) != 1.5:
                continue
            placed.add(_key(row.get("date") or _date_from_path(path), row.get("player_id"), 1.5, row.get("side") or row.get("parsed_side")))
    return placed


def _assign_tiers(row: dict[str, Any], board: str) -> None:
    d7 = _f(row.get("d7_hits_rate"))
    d15 = _f(row.get("d15_hits_rate"))
    expected = _f(row.get("starter_expected_hits_allowed"))
    if board == "u15":
        hitter = _u15_hitter_tier(d7, d15)
        pitcher = _u15_pitcher_tier(expected)
    else:
        hitter = _o15_hitter_tier(d7, d15)
        pitcher = _o15_pitcher_tier(expected)
    row["hitter_tier"] = hitter
    row["pitcher_tier"] = pitcher
    row["combined_tier"] = f"{hitter}/{pitcher}"
    row["ops_proxy_inclusion"] = bool(
        board == "o15" and expected is not None and expected >= 5.0 and d7 is not None and d7 > 1.0
    )


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for r in rows if r.get("result") == "win")
    losses = sum(1 for r in rows if r.get("result") == "loss")
    pushes = sum(1 for r in rows if r.get("result") == "push")
    resolved = wins + losses + pushes
    units = sum(float(r.get("units") or 0.0) for r in rows if r.get("result") in {"win", "loss", "push"})

    def avg(col: str) -> float | None:
        vals = [_f(r.get(col)) for r in rows]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    if resolved < 10:
        sample_warning = "small_sample_lt_10"
    elif resolved < 25:
        sample_warning = "small_sample_lt_25"
    else:
        sample_warning = "ok"

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
        "avg_d7_hits_rate": avg("d7_hits_rate"),
        "avg_d15_hits_rate": avg("d15_hits_rate"),
        "avg_starter_expected_hits_allowed": avg("starter_expected_hits_allowed"),
        "placed_rows": sum(1 for r in rows if r.get("placed")),
        "sample_warning": sample_warning,
    }


def _summarize(rows: list[dict[str, Any]], latest: str, board: str) -> list[dict[str, Any]]:
    dimensions = [
        ("combined_tier", "combined_tier"),
        ("hitter_tier", "hitter_tier"),
        ("pitcher_tier", "pitcher_tier"),
        ("ops_proxy_inclusion", "ops_proxy_inclusion"),
        ("placed_status", "placed_status"),
    ]
    out: list[dict[str, Any]] = []
    for window in WINDOWS:
        wrows = [r for r in rows if window in _window_labels(str(r.get("date") or ""), latest)]
        for dimension, field in dimensions:
            values = sorted({str(r.get(field)) for r in wrows})
            for value in values:
                vals = [r for r in wrows if str(r.get(field)) == value]
                item = {"board": board, "window": window, "dimension": dimension, "tier": value}
                item.update(_metrics(vals))
                out.append(item)
    return out


def _top_recent(rows: list[dict[str, Any]], board: str) -> list[dict[str, Any]]:
    candidates = [
        r
        for r in rows
        if r.get("board") == board
        and r.get("window") in {"last_14", "last_7"}
        and r.get("dimension") == "combined_tier"
        and int(r.get("resolved") or 0) >= 5
        and _f(r.get("roi")) is not None
    ]
    return sorted(candidates, key=lambda r: (_f(r.get("roi")) or -999, int(r.get("resolved") or 0)), reverse=True)[:3]


def _write_report(path: Path, rows: list[dict[str, Any]], board: str, latest: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top = _top_recent(rows, board)
    lines = [
        f"# Reconstructed Hits {'Over' if board == 'o15' else 'Under'} 1.5 All-Market Tier Audit",
        "",
        f"- Latest completed slate: `{latest or 'n/a'}`",
        "- Source: reconstructed all-market population from execution reconcile rows, not actual generated board artifacts.",
        "- Scope: review aid research only; no selector, upload, threshold, grading, or matching changes.",
        "",
        "## Top Recent Combined Tiers",
        "",
        "| window | tier | resolved | WR | ROI | units | avg odds | sample |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in top:
        lines.append(
            f"| {row.get('window')} | `{row.get('tier')}` | `{row.get('resolved')}` | "
            f"`{_pct(_f(row.get('wr')))}` | `{_pct(_f(row.get('roi')))}` | "
            f"`{_num(_f(row.get('units')))}` | `{_num(_f(row.get('avg_odds')))}` | `{row.get('sample_warning')}` |"
        )
    lines.extend(["", "## Full Summary", ""])
    for dimension in ("combined_tier", "hitter_tier", "pitcher_tier", "ops_proxy_inclusion", "placed_status"):
        lines.extend(
            [
                f"### {dimension}",
                "",
                "| window | tier | rows | resolved | WR | ROI | units | avg odds | avg d7 | avg d15 | avg starter exp | sample |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for row in rows:
            if row.get("dimension") != dimension:
                continue
            lines.append(
                f"| {row.get('window')} | `{row.get('tier')}` | `{row.get('rows')}` | `{row.get('resolved')}` | "
                f"`{_pct(_f(row.get('wr')))}` | `{_pct(_f(row.get('roi')))}` | `{_num(_f(row.get('units')))}` | "
                f"`{_num(_f(row.get('avg_odds')))}` | `{_num(_f(row.get('avg_d7_hits_rate')))}` | "
                f"`{_num(_f(row.get('avg_d15_hits_rate')))}` | "
                f"`{_num(_f(row.get('avg_starter_expected_hits_allowed')))}` | `{row.get('sample_warning')}` |"
            )
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Outcome-backed tier backtest for hits o1.5/u1.5 review boards.")
    ap.add_argument("--execution-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--actual-root", default="backend/mlb/exports/model_v2/reconcile")
    ap.add_argument("--out-dir", default=str(OUTPUT_DIR))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    rows = _load_reconcile_rows(ROOT / args.execution_root)
    placed = _load_placed_flags(ROOT / args.actual_root)
    latest = max([str(r.get("date") or "") for r in rows], default="")

    o15_rows: list[dict[str, Any]] = []
    u15_rows: list[dict[str, Any]] = []
    for row in rows:
        row["placed"] = _key(row.get("date"), row.get("player_id"), row.get("line"), row.get("side")) in placed
        row["placed_status"] = "placed" if row["placed"] else "unplaced"
        if row.get("side") == "over":
            item = dict(row)
            _assign_tiers(item, "o15")
            o15_rows.append(item)
        elif row.get("side") == "under":
            item = dict(row)
            _assign_tiers(item, "u15")
            u15_rows.append(item)

    o15_summary = _summarize(o15_rows, latest, "o15")
    u15_summary = _summarize(u15_rows, latest, "u15")

    o15_csv = out_dir / "hits_o15_tier_backtest_summary.csv"
    u15_csv = out_dir / "hits_u15_tier_backtest_summary.csv"
    o15_md = out_dir / "hits_o15_tier_backtest_report.md"
    u15_md = out_dir / "hits_u15_tier_backtest_report.md"
    json_path = out_dir / "hits_15_tier_backtest_summary.json"
    _write_csv(o15_csv, o15_summary)
    _write_csv(u15_csv, u15_summary)
    _write_csv(out_dir / "hits_o15_tier_backtest_rows.csv", o15_rows)
    _write_csv(out_dir / "hits_u15_tier_backtest_rows.csv", u15_rows)
    _write_report(o15_md, o15_summary, "o15", latest)
    _write_report(u15_md, u15_summary, "u15", latest)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_completed_slate": latest,
        "scope": "review_aid_only",
        "o15_top_recent_combined_tiers": _top_recent(o15_summary, "o15"),
        "u15_top_recent_combined_tiers": _top_recent(u15_summary, "u15"),
        "outputs": {
            "o15_csv": _rel(o15_csv),
            "o15_md": _rel(o15_md),
            "u15_csv": _rel(u15_csv),
            "u15_md": _rel(u15_md),
            "summary_json": _rel(json_path),
        },
    }
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
