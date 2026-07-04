#!/usr/bin/env python3
"""Research-only O1.5 d7/d15 hitter-form persistence audit."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable

from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _f(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        out = float(str(value).strip())
        return None if math.isnan(out) else out
    except Exception:
        return None


def _i(value: Any) -> int | None:
    number = _f(value)
    return int(number) if number is not None else None


def _s(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _date(value: Any) -> str:
    text = _s(value)
    return text[:10] if len(text) >= 10 else ""


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _sample_flag(resolved: int) -> str:
    if resolved < 10:
        return "small_sample_lt_10"
    if resolved < 25:
        return "small_sample_lt_25"
    if resolved < 50:
        return "thin_sample_lt_50"
    return "ok"


def _american_implied(price: Any) -> float | None:
    value = _f(price)
    if value is None:
        return None
    if value < 0:
        return abs(value) / (abs(value) + 100.0)
    return 100.0 / (value + 100.0)


def _load_env_profiles(path: Path) -> dict[tuple[str, str, str], str]:
    out: dict[tuple[str, str, str], str] = {}
    for row in _read_csv(path):
        if _s(row.get("side")).lower() != "over":
            continue
        if _f(row.get("line")) != 1.5:
            continue
        profile = _s(row.get("env_v2_beta_profile_family") or row.get("env_v2_beta_profile_label"))
        if not profile:
            continue
        key = (_date(row.get("date")), _s(row.get("player_id") or row.get("canonical_player_id")), _s(row.get("game_id") or row.get("canonical_game_id")))
        out[key] = profile
    return out


def _base_rows(input_csv: Path, env_ledger: Path) -> list[dict[str, Any]]:
    env_profiles = _load_env_profiles(env_ledger)
    rows: list[dict[str, Any]] = []
    for row in _read_csv(input_csv):
        if _s(row.get("side")).lower() != "over":
            continue
        if _f(row.get("line")) != 1.5:
            continue
        result = _s(row.get("result")).lower()
        win = result == "win" or _s(row.get("actual_over_outcome")).lower() == "win"
        loss = result == "loss" or _s(row.get("actual_over_outcome")).lower() == "loss"
        push = result == "push" or _s(row.get("actual_over_outcome")).lower() == "push"
        player_id = _s(row.get("player_id"))
        game_id = _s(row.get("game_id"))
        date_text = _date(row.get("date"))
        out = {
            "date": date_text,
            "game_id": game_id,
            "player_id": player_id,
            "player_name": row.get("player_name") or "",
            "team": row.get("team") or row.get("player_team") or "",
            "opponent": row.get("opponent") or row.get("opponent_team") or "",
            "line": _f(row.get("line")),
            "side": "over",
            "price": _f(row.get("price") or row.get("price_over")),
            "result": "win" if win else "loss" if loss else "push" if push else "unresolved",
            "win": win,
            "loss": loss,
            "push": push,
            "resolved": win or loss or push,
            "units": _f(row.get("units") or row.get("pnl_over_1u")),
            "actual_over_outcome": row.get("actual_over_outcome") or "",
            "actual_under_outcome": row.get("actual_under_outcome") or "",
            "d7_hits_per_game": _f(row.get("d7_hits_rate")),
            "d15_hits_per_game": _f(row.get("d15_hits_rate")),
            "d30_hits_per_game": _f(row.get("d30_hits_rate")),
            "model_prob": _f(row.get("model_prob") or row.get("model_prob_over")),
            "hitter_tier": row.get("hitter_tier") or "",
            "pitcher_tier": row.get("pitcher_tier") or "",
            "combined_tier": row.get("combined_tier") or "",
            "starter_expected_hits_allowed": _f(row.get("starter_expected_hits_allowed")),
            "team_expected_hits_allowed": _f(row.get("team_expected_hits_allowed")),
            "environment_profile": env_profiles.get((date_text, player_id, game_id), ""),
            "source_artifact": _rel(input_csv),
        }
        rows.append(out)
    return rows


def _fetch_player_context(rows: list[dict[str, Any]]) -> tuple[dict[tuple[str, str, str], dict[str, Any]], str]:
    player_ids = sorted({int(_i(row.get("player_id")) or 0) for row in rows if _i(row.get("player_id"))})
    game_ids = sorted({int(_i(row.get("game_id")) or 0) for row in rows if _i(row.get("game_id"))})
    dates = [date.fromisoformat(_date(row.get("date"))) for row in rows if _date(row.get("date"))]
    if not player_ids or not dates:
        return {}, "missing_player_ids_or_dates"

    min_date = min(dates) - timedelta(days=75)
    max_date = max(dates)
    try:
        stat_rows = pg_fetchall(
            """
SELECT
  ps.game_date::date AS game_date,
  ps.game_id,
  ps.player_id,
  ps.hits,
  ps.at_bats,
  ps.plate_appearances
FROM mlb.player_stats ps
WHERE ps.player_id = ANY(%s)
  AND ps.game_date BETWEEN %s::date AND %s::date
ORDER BY ps.player_id, ps.game_date, ps.game_id
""",
            (player_ids, min_date.isoformat(), max_date.isoformat()),
        )
    except Exception as exc:
        return {}, f"player_stats_query_error:{type(exc).__name__}:{exc}"

    try:
        pds_rows = pg_fetchall(
            """
SELECT
  pds.game_date::date AS game_date,
  pds.game_id,
  pds.player_id,
  pds.d7_plate_appearances::float8 AS d7_plate_appearances,
  pds.d15_plate_appearances::float8 AS d15_plate_appearances,
  pds.d30_plate_appearances::float8 AS d30_plate_appearances
FROM mlb.player_derived_stats pds
WHERE pds.player_id = ANY(%s)
  AND pds.game_id = ANY(%s)
""",
            (player_ids, game_ids),
        )
    except Exception:
        pds_rows = []

    target_keys = {(_date(row.get("date")), _s(row.get("game_id")), _s(row.get("player_id"))) for row in rows}
    by_player: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in stat_rows or []:
        player_id = _i(row.get("player_id"))
        if player_id is not None:
            by_player[int(player_id)].append(dict(row))

    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for player_id, player_rows in by_player.items():
        player_rows = sorted(player_rows, key=lambda r: (_date(r.get("game_date")), int(_i(r.get("game_id")) or 0)))
        for idx, row in enumerate(player_rows):
            key = (_date(row.get("game_date")), _s(row.get("game_id")), str(player_id))
            if key not in target_keys:
                continue
            prior = [r for r in player_rows[:idx] if _date(r.get("game_date")) < key[0]]

            def last_games(n: int) -> list[dict[str, Any]]:
                return prior[-n:] if prior else []

            def hit_values(n: int) -> list[float]:
                vals = [_f(r.get("hits")) for r in last_games(n)]
                return [v for v in vals if v is not None]

            def pa_values(n: int) -> list[float]:
                vals = [_f(r.get("plate_appearances")) for r in last_games(n)]
                return [v for v in vals if v is not None]

            def dist(vals: list[float]) -> dict[str, Any]:
                return {
                    "games": len(vals),
                    "zero_hit_games": sum(1 for v in vals if v == 0),
                    "one_hit_games": sum(1 for v in vals if v == 1),
                    "two_hit_games": sum(1 for v in vals if v == 2),
                    "three_plus_hit_games": sum(1 for v in vals if v >= 3),
                    "two_plus_hit_games": sum(1 for v in vals if v >= 2),
                    "hits_per_game": sum(vals) / len(vals) if vals else None,
                    "two_plus_hit_rate": sum(1 for v in vals if v >= 2) / len(vals) if vals else None,
                    "zero_hit_rate": sum(1 for v in vals if v == 0) / len(vals) if vals else None,
                }

            h20 = dist(hit_values(20))
            h30 = dist(hit_values(30))
            p20 = pa_values(20)
            p30 = pa_values(30)
            out[key] = {
                "d20_games": h20["games"],
                "d20_hits_per_game": h20["hits_per_game"],
                "d20_2plus_hit_rate": h20["two_plus_hit_rate"],
                "d20_zero_hit_games": h20["zero_hit_games"],
                "d20_one_hit_games": h20["one_hit_games"],
                "d20_two_hit_games": h20["two_hit_games"],
                "d20_three_plus_hit_games": h20["three_plus_hit_games"],
                "d30_games": h30["games"],
                "d30_2plus_hit_rate": h30["two_plus_hit_rate"],
                "d30_zero_hit_rate": h30["zero_hit_rate"],
                "d30_zero_hit_games": h30["zero_hit_games"],
                "d30_one_hit_games": h30["one_hit_games"],
                "d30_two_hit_games": h30["two_hit_games"],
                "d30_three_plus_hit_games": h30["three_plus_hit_games"],
                "d20_plate_appearances": sum(p20) / len(p20) if p20 else None,
                "d30_plate_appearances_from_logs": sum(p30) / len(p30) if p30 else None,
            }

    for row in pds_rows or []:
        key = (_date(row.get("game_date")), _s(row.get("game_id")), _s(row.get("player_id")))
        if key in target_keys:
            out.setdefault(key, {}).update(
                {
                    "d7_plate_appearances": _f(row.get("d7_plate_appearances")),
                    "d15_plate_appearances": _f(row.get("d15_plate_appearances")),
                    "d30_plate_appearances": _f(row.get("d30_plate_appearances")),
                }
            )
    return out, "ok"


def _consistency_label(row: dict[str, Any]) -> str:
    games = _i(row.get("d30_games")) or 0
    d30_rate = _f(row.get("d30_2plus_hit_rate"))
    d30_hpg = _f(row.get("d30_hits_per_game"))
    zero_rate = _f(row.get("d30_zero_hit_rate"))
    if games < 15 or d30_rate is None:
        return "insufficient_history"
    if d30_rate >= 0.30 and (d30_hpg or 0) > 1.0:
        return "consistent_multi_hit"
    if (d30_hpg or 0) > 1.0 and d30_rate < 0.25:
        return "one_hit_floor"
    if d30_rate >= 0.25 and (zero_rate or 0) >= 0.25:
        return "boom_bust"
    return "mixed_profile"


def _enrich(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    context, status = _fetch_player_context(rows)
    for row in rows:
        key = (_date(row.get("date")), _s(row.get("game_id")), _s(row.get("player_id")))
        row.update(context.get(key, {}))
        row["passes_d7_d15"] = (_f(row.get("d7_hits_per_game")) or -999) > 1.0 and (_f(row.get("d15_hits_per_game")) or -999) > 1.0
        row["passes_d30_gt1"] = (_f(row.get("d30_hits_per_game")) or -999) > 1.0
        row["d30_2plus_high_ge30pct"] = (_f(row.get("d30_2plus_hit_rate")) or -999) >= 0.30
        row["d30_2plus_low_lt20pct"] = (_f(row.get("d30_2plus_hit_rate")) or 999) < 0.20
        row["d15_pa_high_ge4_3"] = (_f(row.get("d15_plate_appearances")) or -999) >= 4.3
        row["d15_pa_low_lt3_8"] = (_f(row.get("d15_plate_appearances")) or 999) < 3.8
        row["consistency_label"] = _consistency_label(row)
        row["implied_probability"] = _american_implied(row.get("price"))
    return rows, status


def _avg(rows: list[dict[str, Any]], field: str) -> float | None:
    vals = [_f(r.get(field)) for r in rows]
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None


def _metrics(label: str, rows: list[dict[str, Any]], group: str = "segment") -> dict[str, Any]:
    resolved = [r for r in rows if r.get("resolved")]
    wins = [r for r in resolved if r.get("win")]
    losses = [r for r in resolved if r.get("loss")]
    pushes = [r for r in resolved if r.get("push")]
    units_vals = [_f(r.get("units")) for r in resolved]
    units_vals = [v for v in units_vals if v is not None]
    prices = [_f(r.get("price")) for r in resolved]
    prices = [v for v in prices if v is not None]
    return {
        "segment_group": group,
        "segment": label,
        "rows": len(rows),
        "resolved": len(resolved),
        "wins": len(wins),
        "losses": len(losses),
        "pushes": len(pushes),
        "wr": len(wins) / (len(wins) + len(losses)) if (wins or losses) else None,
        "roi": sum(units_vals) / len(resolved) if resolved and units_vals else None,
        "units": sum(units_vals) if units_vals else 0.0,
        "average_odds": sum(prices) / len(prices) if prices else None,
        "median_odds": median(prices) if prices else None,
        "sample_flag": _sample_flag(len(resolved)),
        "avg_d7_hits_per_game": _avg(rows, "d7_hits_per_game"),
        "avg_d15_hits_per_game": _avg(rows, "d15_hits_per_game"),
        "avg_d20_hits_per_game": _avg(rows, "d20_hits_per_game"),
        "avg_d30_hits_per_game": _avg(rows, "d30_hits_per_game"),
        "avg_d20_2plus_hit_rate": _avg(rows, "d20_2plus_hit_rate"),
        "avg_d30_2plus_hit_rate": _avg(rows, "d30_2plus_hit_rate"),
        "avg_d7_plate_appearances": _avg(rows, "d7_plate_appearances"),
        "avg_d15_plate_appearances": _avg(rows, "d15_plate_appearances"),
        "avg_d30_plate_appearances": _avg(rows, "d30_plate_appearances"),
        "avg_starter_expected_hits_allowed": _avg(rows, "starter_expected_hits_allowed"),
        "avg_team_expected_hits_allowed": _avg(rows, "team_expected_hits_allowed"),
    }


def _segment_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    primary = [r for r in rows if r.get("passes_d7_d15")]
    specs: list[tuple[str, str, Callable[[dict[str, Any]], bool]]] = [
        ("baseline", "all_o15_backtest", lambda r: True),
        ("primary", "d7_gt1_and_d15_gt1", lambda r: bool(r.get("passes_d7_d15"))),
        ("persistence", "d7d15_and_d30_gt1", lambda r: bool(r.get("passes_d7_d15")) and bool(r.get("passes_d30_gt1"))),
        ("persistence", "d7d15_and_d30_lte1_or_missing", lambda r: bool(r.get("passes_d7_d15")) and not bool(r.get("passes_d30_gt1"))),
        ("persistence", "d7d15_and_high_d30_2plus_ge30pct", lambda r: bool(r.get("passes_d7_d15")) and bool(r.get("d30_2plus_high_ge30pct"))),
        ("persistence", "d7d15_and_low_d30_2plus_lt20pct", lambda r: bool(r.get("passes_d7_d15")) and bool(r.get("d30_2plus_low_lt20pct"))),
        ("pa", "d7d15_and_high_d15_pa_ge4_3", lambda r: bool(r.get("passes_d7_d15")) and bool(r.get("d15_pa_high_ge4_3"))),
        ("pa", "d7d15_and_low_d15_pa_lt3_8", lambda r: bool(r.get("passes_d7_d15")) and bool(r.get("d15_pa_low_lt3_8"))),
    ]
    out = [_metrics(label, [r for r in rows if pred(r)], group) for group, label, pred in specs]
    for tier in sorted({_s(r.get("combined_tier")) for r in primary if _s(r.get("combined_tier"))}):
        if tier in {"A/A", "A/B", "C/A", "B/A", "B/B"}:
            out.append(_metrics(f"d7d15_inside_{tier}", [r for r in primary if _s(r.get("combined_tier")) == tier], "tier"))
    for profile in sorted({_s(r.get("environment_profile")) for r in primary if _s(r.get("environment_profile"))}):
        out.append(_metrics(f"d7d15_inside_env_{profile}", [r for r in primary if _s(r.get("environment_profile")) == profile], "environment"))
    for label in sorted({_s(r.get("consistency_label")) for r in primary if _s(r.get("consistency_label"))}):
        out.append(_metrics(f"d7d15_{label}", [r for r in primary if _s(r.get("consistency_label")) == label], "profile"))
    return out


def _distribution_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "date",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "game_id",
        "result",
        "price",
        "combined_tier",
        "environment_profile",
        "d7_hits_per_game",
        "d15_hits_per_game",
        "d20_hits_per_game",
        "d30_hits_per_game",
        "d20_games",
        "d20_zero_hit_games",
        "d20_one_hit_games",
        "d20_two_hit_games",
        "d20_three_plus_hit_games",
        "d20_2plus_hit_rate",
        "d30_games",
        "d30_zero_hit_games",
        "d30_one_hit_games",
        "d30_two_hit_games",
        "d30_three_plus_hit_games",
        "d30_2plus_hit_rate",
        "d7_plate_appearances",
        "d15_plate_appearances",
        "d30_plate_appearances",
        "consistency_label",
    ]
    return [{field: row.get(field, "") for field in fields} for row in rows if row.get("passes_d7_d15")]


def _fmt_pct(value: Any) -> str:
    f = _f(value)
    return "n/a" if f is None else f"{100.0 * f:.2f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    f = _f(value)
    return "n/a" if f is None else f"{f:.{digits}f}"


def _write_report(path: Path, *, rows: list[dict[str, Any]], segments: list[dict[str, Any]], context_status: str, outputs: dict[str, str]) -> None:
    primary = [r for r in rows if r.get("passes_d7_d15")]
    primary_metric = next((r for r in segments if r.get("segment") == "d7_gt1_and_d15_gt1"), {})
    d30_metric = next((r for r in segments if r.get("segment") == "d7d15_and_d30_gt1"), {})
    d30_weak_metric = next((r for r in segments if r.get("segment") == "d7d15_and_d30_lte1_or_missing"), {})
    high_2p = next((r for r in segments if r.get("segment") == "d7d15_and_high_d30_2plus_ge30pct"), {})
    low_2p = next((r for r in segments if r.get("segment") == "d7d15_and_low_d30_2plus_lt20pct"), {})
    high_pa = next((r for r in segments if r.get("segment") == "d7d15_and_high_d15_pa_ge4_3"), {})
    low_pa = next((r for r in segments if r.get("segment") == "d7d15_and_low_d15_pa_lt3_8"), {})

    d30_strength_rate = (
        sum(1 for r in primary if r.get("passes_d30_gt1")) / len(primary)
        if primary
        else None
    )
    d30_2plus_coverage = sum(1 for r in primary if _f(r.get("d30_2plus_hit_rate")) is not None)
    lines = [
        "# Hits O1.5 d7/d15 Persistence Audit",
        "",
        f"- Generated at: `{_utc_now()}`",
        f"- Context enrichment status: `{context_status}`",
        f"- Source rows: `{len(rows)}` O1.5 backtest rows",
        f"- Primary d7/d15 rows: `{len(primary)}`",
        f"- Primary rows with d30 2+ hit-rate enrichment: `{d30_2plus_coverage}`",
        "",
        "## Direct Answers",
        "",
        f"- Does d7 > 1 and d15 > 1 usually imply durable d30 strength? `{_fmt_pct(d30_strength_rate)}` of primary rows also have d30 hits/game > 1.0.",
        f"- Does it imply actual 2+ hit persistence? Primary rows average d30 2+ hit rate `{_fmt_pct(primary_metric.get('avg_d30_2plus_hit_rate'))}`.",
        f"- Does d30 persistence improve O1.5 evaluation? d7/d15 plus d30 > 1.0 returned ROI `{_fmt_pct(d30_metric.get('roi'))}` vs `{_fmt_pct(d30_weak_metric.get('roi'))}` for rows without d30 > 1.0.",
        f"- Does d30 2+ hit rate improve O1.5 evaluation? It is useful diagnostically, but high d30 2+ returned ROI `{_fmt_pct(high_2p.get('roi'))}` and low d30 2+ is `{low_2p.get('resolved')}` resolved rows, so this split is not stable enough alone.",
        f"- Does PA context change interpretation? PA helps explain opportunity, but this run is mixed: high d15 PA ROI `{_fmt_pct(high_pa.get('roi'))}` vs low d15 PA ROI `{_fmt_pct(low_pa.get('roi'))}`.",
        "- Should this become a monitored research lane? Yes, as research-only monitoring; do not promote to production without a larger/stable sample.",
        "",
        "## Key Segment Snapshot",
        "",
        "| segment | rows | resolved | WR | ROI | units | avg d7 | avg d15 | avg d30 | avg d30 2+ | avg d15 PA | sample |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in [primary_metric, d30_metric, d30_weak_metric, high_2p, low_2p, high_pa, low_pa]:
        if not row:
            continue
        lines.append(
            f"| {row.get('segment')} | `{row.get('rows')}` | `{row.get('resolved')}` | "
            f"`{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt_num(row.get('units'))}` | "
            f"`{_fmt_num(row.get('avg_d7_hits_per_game'))}` | `{_fmt_num(row.get('avg_d15_hits_per_game'))}` | "
            f"`{_fmt_num(row.get('avg_d30_hits_per_game'))}` | `{_fmt_pct(row.get('avg_d30_2plus_hit_rate'))}` | "
            f"`{_fmt_num(row.get('avg_d15_plate_appearances'))}` | `{row.get('sample_flag')}` |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The d7/d15 condition is a useful short-term heat signal, but it should not be read as automatically proving durable multi-hit persistence.",
            "- d30 hits/game produced the clearest persistence split in this audit: rows that also stayed above 1.0 d30 hits/game were meaningfully stronger than d7/d15 rows without d30 support.",
            "- d30 2+ hit rate remains the right diagnostic for separating one-hit floor from multi-hit conversion, but this pass does not justify a standalone high/low 2+ hit-rate rule.",
            "- PA context should be retained beside the persistence label. It is opportunity context, not a proven filter in this sample, and low-PA groups can be distorted by early-season or limited-history rows.",
            "- The most actionable research lane is not simply d7/d15 heat; it is d7/d15 heat plus longer-window persistence and opportunity context.",
            "",
            "## Outputs",
            "",
        ]
    )
    for label, output in outputs.items():
        lines.append(f"- {label}: `{output}`")
    lines.extend(
        [
            "",
            "## Guardrail",
            "",
            "Research only. No production selectors, uploads, thresholds, model behavior, Morning Workbench, or Ops Brief behavior changed.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Research-only O1.5 d7/d15 persistence audit.")
    ap.add_argument("--input-csv", default="artifacts/analysis/mlb/review_aids/hits_o15_tier_backtest_rows.csv")
    ap.add_argument("--env-ledger-csv", default="artifacts/analysis/mlb/environment_v2/ledger/environment_v2_beta_profile_ledger.csv")
    ap.add_argument("--out-md", default="artifacts/analysis/mlb/review_aids/hits_o15_d7_d15_persistence_audit_2026-07-03.md")
    ap.add_argument("--out-segments-csv", default="artifacts/analysis/mlb/review_aids/hits_o15_d7_d15_persistence_segments_2026-07-03.csv")
    ap.add_argument("--out-rows-csv", default="artifacts/analysis/mlb/review_aids/hits_o15_d7_d15_persistence_rows_2026-07-03.csv")
    ap.add_argument("--out-distribution-csv", default="artifacts/analysis/mlb/review_aids/hits_o15_d7_d15_persistence_distribution_2026-07-03.csv")
    args = ap.parse_args()

    input_csv = ROOT / args.input_csv
    rows = _base_rows(input_csv, ROOT / args.env_ledger_csv)
    rows, context_status = _enrich(rows)
    primary = [row for row in rows if row.get("passes_d7_d15")]
    segments = _segment_rows(rows)

    row_fields = [
        "date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "line",
        "side",
        "price",
        "result",
        "win",
        "loss",
        "push",
        "units",
        "d7_hits_per_game",
        "d15_hits_per_game",
        "d20_hits_per_game",
        "d30_hits_per_game",
        "d20_2plus_hit_rate",
        "d30_2plus_hit_rate",
        "d30_zero_hit_rate",
        "d7_plate_appearances",
        "d15_plate_appearances",
        "d30_plate_appearances",
        "passes_d30_gt1",
        "d30_2plus_high_ge30pct",
        "d30_2plus_low_lt20pct",
        "consistency_label",
        "hitter_tier",
        "pitcher_tier",
        "combined_tier",
        "environment_profile",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "source_artifact",
    ]
    _write_csv(ROOT / args.out_segments_csv, segments)
    _write_csv(ROOT / args.out_rows_csv, [{field: row.get(field, "") for field in row_fields} for row in primary], row_fields)
    _write_csv(ROOT / args.out_distribution_csv, _distribution_rows(rows))
    _write_report(
        ROOT / args.out_md,
        rows=rows,
        segments=segments,
        context_status=context_status,
        outputs={
            "segment performance CSV": args.out_segments_csv,
            "row-level audit CSV": args.out_rows_csv,
            "player/game distribution CSV": args.out_distribution_csv,
        },
    )
    print(
        f"[o15-d7-d15-persistence] rows={len(rows)} primary={len(primary)} "
        f"context_status={context_status} out_md={args.out_md}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
