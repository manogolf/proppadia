#!/usr/bin/env python3
"""Audit failure modes for hits over 1.5 Tier A candidates.

Analysis only: reads reconciled history and review-aid context artifacts, then
writes CSV/Markdown diagnostics. It does not change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_EXEC_ROOT = ROOT / "artifacts/analysis/mlb/execution_vs_model"
DEFAULT_REVIEW_DIR = ROOT / "artifacts/analysis/mlb/review_aids"
DEFAULT_LANES_ROOT = ROOT / "backend/mlb/exports/model_v2/lanes/today"


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        out = float(value)
        if math.isnan(out):
            return None
        return out
    except Exception:
        return None


def _i(value: Any) -> int | None:
    value_f = _f(value)
    if value_f is None:
        return None
    return int(value_f)


def _clean(value: Any) -> str:
    return str(value or "").strip().lower()


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


def _american_units(price: float | None, won: bool) -> float:
    if not won:
        return -1.0
    if price is None:
        return 1.0
    return price / 100.0 if price > 0 else 100.0 / abs(price)


def _tier_a(row: dict[str, Any]) -> bool:
    d7 = _f(row.get("d7_hits"))
    d15 = _f(row.get("d15_hits"))
    return d7 is not None and d15 is not None and d7 > 1.30 and d15 > 1.20


def _load_tier_a_rows(exec_root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for file in sorted(exec_root.glob("20??-??-??/reconcile_rows.csv")):
        date_fallback = file.parent.name
        for raw in _read_csv(file):
            if _clean(raw.get("prop_type")) != "hits" or _f(raw.get("line")) != 1.5:
                continue
            if not _tier_a(raw):
                continue
            result = _clean(raw.get("actual_over_outcome"))
            if result not in {"win", "loss", "push"}:
                continue
            actual_hits = _f(raw.get("actual_value"))
            price = _f(raw.get("price_over_american") or raw.get("market_price_over"))
            units = _f(raw.get("pnl_over_1u"))
            if units is None and result in {"win", "loss"}:
                units = _american_units(price, result == "win")
            row = dict(raw)
            row.update(
                {
                    "date": str(raw.get("game_date") or raw.get("slate_date") or date_fallback)[:10],
                    "side": "over",
                    "result": result,
                    "actual_hits": actual_hits,
                    "price": price,
                    "units": units if units is not None else 0.0,
                    "outcome_group": (
                        "winner_2_plus_hits"
                        if result == "win"
                        else "one_hit_loser"
                        if actual_hits == 1
                        else "zero_hit_loser"
                        if actual_hits == 0
                        else "other_loser"
                        if result == "loss"
                        else "push"
                    ),
                    "source_reconcile_file": str(file.relative_to(ROOT)),
                }
            )
            out.append(row)
    return out


def _fetch_actual_context(rows: list[dict[str, Any]]) -> dict[tuple[str, int, int], dict[str, Any]]:
    player_ids = sorted({int(pid) for row in rows if (pid := _i(row.get("player_id"))) is not None})
    game_ids = sorted({int(gid) for row in rows if (gid := _i(row.get("game_id"))) is not None})
    if not player_ids or not game_ids:
        return {}
    stat_rows = pg_fetchall(
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
WHERE ps.game_id = ANY(%s)
  AND ps.player_id = ANY(%s)
""",
        (game_ids, player_ids),
    )
    pfp_rows = pg_fetchall(
        """
SELECT game_date::date AS game_date, game_id, player_id, lineup_slot
FROM mlb.prop_features_precomputed
WHERE game_id = ANY(%s)
  AND player_id = ANY(%s)
  AND prop_type = 'hits'
""",
        (game_ids, player_ids),
    )
    lineup: dict[tuple[str, int, int], Any] = {}
    for row in pfp_rows or []:
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        date_text = str(row.get("game_date") or "")[:10]
        if game_id is not None and player_id is not None and date_text:
            lineup[(date_text, game_id, player_id)] = row.get("lineup_slot")

    out: dict[tuple[str, int, int], dict[str, Any]] = {}
    for row in stat_rows or []:
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        date_text = str(row.get("game_date") or "")[:10]
        if game_id is None or player_id is None or not date_text:
            continue
        pa = _f(row.get("plate_appearances"))
        if pa is None:
            ab = _f(row.get("at_bats"))
            if ab is not None:
                pa = (
                    ab
                    + (_f(row.get("walks")) or 0.0)
                    + (_f(row.get("hit_by_pitch")) or 0.0)
                    + (_f(row.get("sacrifice_flies")) or 0.0)
                    + (_f(row.get("sacrifice_hits")) or 0.0)
                    + (_f(row.get("catcher_interference")) or 0.0)
                )
        key = (date_text, game_id, player_id)
        out[key] = {
            "actual_plate_appearances": pa,
            "actual_at_bats": _f(row.get("at_bats")),
            "lineup_slot": _i(lineup.get(key)),
        }
    return out


def _lineup_bucket(slot: Any) -> str:
    slot_i = _i(slot)
    if slot_i is None:
        return "missing"
    if slot_i <= 3:
        return "top_1_3"
    if slot_i <= 6:
        return "middle_4_6"
    return "bottom_7_9"


def _load_context_from_review_artifacts(review_dir: Path) -> dict[tuple[str, str, str], dict[str, Any]]:
    ctx: dict[tuple[str, str, str], dict[str, Any]] = {}
    patterns = [
        "o15_pa_opportunity_audit_rows.csv",
        "o15_rest_time_context_rows.csv",
        "hits_o15_simple_filter_*.csv",
        "hits_o15_watch_candidates_*.csv",
        "hits_o15_layered_candidates_*.csv",
        "hits_o15_alternate_discovery_*.csv",
    ]
    for pattern in patterns:
        for path in sorted(review_dir.glob(pattern)):
            for row in _read_csv(path):
                player_id = str(row.get("player_id") or "").strip()
                date_text = str(row.get("date") or row.get("game_date") or row.get("slate_date") or "")[:10]
                line = _f(row.get("line"))
                if not player_id or not date_text or line != 1.5:
                    continue
                key = (date_text, player_id, "1.5")
                old = ctx.get(key, {})
                merged = dict(old)
                for col in [
                    "starter_expected_hits_allowed",
                    "team_expected_hits_allowed",
                    "opposing_starter",
                    "starter_context_status",
                    "time_of_day_bucket",
                    "game_day_of_week",
                    "game_time",
                    "team_time_sequence_bucket",
                    "rest_day_before_game",
                    "short_turnaround_proxy",
                    "previous_team_time_of_day_bucket",
                    "hours_since_previous_team_game",
                ]:
                    value = row.get(col)
                    if value not in {None, ""} and merged.get(col) in {None, ""}:
                        merged[col] = value
                ctx[key] = merged
    return ctx


def _load_u15_flags(review_dir: Path) -> set[tuple[str, str]]:
    flags: set[tuple[str, str]] = set()
    for path in sorted(review_dir.glob("hits_u15_favorite_audit_*.csv")):
        for row in _read_csv(path):
            if _f(row.get("line")) != 1.5:
                continue
            player_id = str(row.get("player_id") or "").strip()
            date_text = str(row.get("date") or path.stem[-10:])[:10]
            if player_id and date_text:
                flags.add((date_text, player_id))
    return flags


def _load_overlap_flags(lanes_root: Path) -> set[tuple[str, str]]:
    flags: set[tuple[str, str]] = set()
    for day_dir in sorted(lanes_root.glob("20??-??-??")):
        date_text = day_dir.name
        ranking: set[tuple[str, str, str, str]] = set()
        qc: set[tuple[str, str, str, str]] = set()
        for path in sorted(day_dir.glob(f"hits_lane_selector_{date_text}_ranking_upload_input*.csv")):
            for row in _read_csv(path):
                if _clean(row.get("prop_type")) == "hits" and _f(row.get("line")) == 1.5:
                    pid = str(row.get("player_id") or "").strip()
                    if pid:
                        ranking.add((date_text, pid, _clean(row.get("side")), "1.5"))
        for path in sorted(day_dir.glob(f"quick_card_hits_{date_text}*.csv")):
            for row in _read_csv(path):
                if _clean(row.get("prop_type")) == "hits" and _f(row.get("line")) == 1.5:
                    pid = str(row.get("player_id") or "").strip()
                    if pid:
                        qc.add((date_text, pid, _clean(row.get("side")), "1.5"))
        for key in ranking & qc:
            if key[2] == "over":
                flags.add((key[0], key[1]))
    return flags


def _build_team_rest_context(rows: list[dict[str, Any]]) -> None:
    games: dict[tuple[str, int], dict[str, Any]] = {}
    for row in rows:
        game_id = _i(row.get("game_id"))
        if game_id is None:
            continue
        date_text = str(row.get("date") or "")[:10]
        for team, opponent in [(row.get("team"), row.get("opponent")), (row.get("opponent"), row.get("team"))]:
            team_c = str(team or "").strip()
            if not team_c:
                continue
            games[(team_c, game_id)] = {
                "team": team_c,
                "game_id": game_id,
                "date": date_text,
                "game_time": row.get("game_time") or "",
                "time_of_day_bucket": _clean(row.get("time_of_day_bucket")) or "missing",
                "opponent": str(opponent or "").strip(),
            }
    by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in games.values():
        by_team[item["team"]].append(item)
    for team in by_team:
        by_team[team].sort(key=lambda r: (r["date"], str(r.get("game_time") or ""), int(r["game_id"])))
    prev_by_game: dict[tuple[str, int], dict[str, Any]] = {}
    for team, team_games in by_team.items():
        prev = None
        for game in team_games:
            if prev:
                prev_by_game[(team, int(game["game_id"]))] = prev
            prev = game
    for row in rows:
        team = str(row.get("team") or "").strip()
        game_id = _i(row.get("game_id"))
        prev = prev_by_game.get((team, int(game_id or 0)), {})
        if row.get("team_time_sequence_bucket") not in {None, ""}:
            continue
        cur_bucket = _clean(row.get("time_of_day_bucket")) or "missing"
        prev_bucket = _clean(prev.get("time_of_day_bucket")) or "missing"
        if cur_bucket in {"afternoon", "day", "early"} and prev_bucket in {"evening", "late", "night"}:
            sequence = "day_after_night"
        elif cur_bucket in {"afternoon", "day", "early"} and prev_bucket in {"afternoon", "day", "early"}:
            sequence = "day_after_day"
        elif cur_bucket in {"evening", "late", "night"} and prev_bucket in {"evening", "late", "night"}:
            sequence = "night_after_night"
        elif cur_bucket in {"evening", "late", "night"} and prev_bucket in {"afternoon", "day", "early"}:
            sequence = "night_after_day"
        elif not prev:
            sequence = "no_previous_team_game_in_artifacts"
        else:
            sequence = "missing"
        row["previous_team_time_of_day_bucket"] = prev_bucket
        row["team_time_sequence_bucket"] = sequence


def _enrich_rows(rows: list[dict[str, Any]], review_dir: Path, lanes_root: Path) -> None:
    review_ctx = _load_context_from_review_artifacts(review_dir)
    u15_flags = _load_u15_flags(review_dir)
    overlap_flags = _load_overlap_flags(lanes_root)
    actual_ctx = _fetch_actual_context(rows)
    for row in rows:
        player_id = str(row.get("player_id") or "").strip()
        date_text = str(row.get("date") or "")[:10]
        key = (date_text, player_id, "1.5")
        for col, value in review_ctx.get(key, {}).items():
            if value not in {None, ""} and row.get(col) in {None, ""}:
                row[col] = value
        game_id = _i(row.get("game_id"))
        pid_i = _i(row.get("player_id"))
        actual = actual_ctx.get((date_text, int(game_id or 0), int(pid_i or 0)), {})
        row["actual_plate_appearances"] = actual.get("actual_plate_appearances")
        row["actual_at_bats"] = actual.get("actual_at_bats")
        row["lineup_slot"] = actual.get("lineup_slot")
        row["lineup_bucket"] = _lineup_bucket(actual.get("lineup_slot"))
        row["u15_candidate_same_day"] = (date_text, player_id) in u15_flags
        row["ranking_qc_overlap_same_side"] = (date_text, player_id) in overlap_flags
        starter = _f(row.get("starter_expected_hits_allowed"))
        team = _f(row.get("team_expected_hits_allowed"))
        row["bullpen_expected_hits_allowed_component"] = team - starter if starter is not None and team is not None else None
        row["bullpen_metrics_available"] = row["bullpen_expected_hits_allowed_component"] is not None
    _build_team_rest_context(rows)


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for row in rows if row.get("outcome_group") == "winner_2_plus_hits")
    losses = sum(1 for row in rows if row.get("outcome_group") in {"one_hit_loser", "zero_hit_loser", "other_loser"})
    pushes = sum(1 for row in rows if row.get("outcome_group") == "push")
    resolved = wins + losses + pushes
    units = sum(_f(row.get("units")) or 0.0 for row in rows)

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
        "avg_d7_hits": avg("d7_hits"),
        "avg_d15_hits": avg("d15_hits"),
        "avg_d30_hits": avg("d30_hits"),
        "avg_d7_hits_runs_rbis": avg("d7_hits_runs_rbis"),
        "avg_d15_hits_runs_rbis": avg("d15_hits_runs_rbis"),
        "avg_starter_expected_hits_allowed": avg("starter_expected_hits_allowed"),
        "avg_team_expected_hits_allowed": avg("team_expected_hits_allowed"),
        "avg_actual_plate_appearances": avg("actual_plate_appearances"),
        "avg_bvp_plate_appearances": avg("bvp_plate_appearances"),
        "avg_bvp_avg": avg("bvp_avg"),
        "avg_bvp_slg": avg("bvp_slg"),
    }


def _feature_comparison(rows: list[dict[str, Any]], window: str) -> list[dict[str, Any]]:
    groups = {
        "winners_2_plus_hits": [r for r in rows if r.get("outcome_group") == "winner_2_plus_hits"],
        "all_losers_0_or_1": [r for r in rows if r.get("outcome_group") in {"one_hit_loser", "zero_hit_loser"}],
        "one_hit_losers": [r for r in rows if r.get("outcome_group") == "one_hit_loser"],
        "zero_hit_losers": [r for r in rows if r.get("outcome_group") == "zero_hit_loser"],
    }
    features = [
        "d7_hits",
        "d15_hits",
        "d30_hits",
        "d7_hits_runs_rbis",
        "d15_hits_runs_rbis",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "bullpen_expected_hits_allowed_component",
        "actual_plate_appearances",
        "lineup_slot",
        "price",
        "implied_over",
        "market_no_vig_implied_over",
        "model_prob_over",
        "bvp_plate_appearances",
        "bvp_hits",
        "bvp_avg",
        "bvp_slg",
        "bvp_total_bases",
    ]
    out: list[dict[str, Any]] = []
    for feature in features:
        item = {"window": window, "feature": feature}
        for group, grows in groups.items():
            vals = [_f(r.get(feature)) for r in grows]
            vals = [v for v in vals if v is not None]
            item[f"{group}_rows_with_value"] = len(vals)
            item[f"{group}_avg"] = sum(vals) / len(vals) if vals else None
        win = item.get("winners_2_plus_hits_avg")
        lose = item.get("all_losers_0_or_1_avg")
        item["winner_minus_loser_avg"] = win - lose if win is not None and lose is not None else None
        out.append(item)
    return out


def _bucket(value: float | None, cuts: list[tuple[str, Callable[[float], bool]]]) -> str:
    if value is None:
        return "missing"
    for label, pred in cuts:
        if pred(value):
            return label
    return "other"


def _annotate_buckets(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        row["actual_pa_bucket"] = _bucket(
            _f(row.get("actual_plate_appearances")),
            [("pa_le_3", lambda v: v <= 3), ("pa_eq_4", lambda v: v == 4), ("pa_ge_5", lambda v: v >= 5)],
        )
        row["price_bucket"] = _bucket(
            _f(row.get("price")),
            [("plus_le_125", lambda v: v <= 125), ("plus_126_150", lambda v: v <= 150), ("plus_151_180", lambda v: v <= 180), ("plus_gt_180", lambda v: v > 180)],
        )
        row["starter_bucket"] = _bucket(
            _f(row.get("starter_expected_hits_allowed")),
            [("starter_lt_5", lambda v: v < 5), ("starter_5_5.5", lambda v: v < 5.5), ("starter_ge_5.5", lambda v: v >= 5.5)],
        )
        row["team_expected_bucket"] = _bucket(
            _f(row.get("team_expected_hits_allowed")),
            [("team_lt_8.5", lambda v: v < 8.5), ("team_8.5_9.5", lambda v: v < 9.5), ("team_ge_9.5", lambda v: v >= 9.5)],
        )
        row["bvp_bucket"] = _bucket(
            _f(row.get("bvp_plate_appearances")),
            [("bvp_pa_0", lambda v: v == 0), ("bvp_pa_1_4", lambda v: v < 5), ("bvp_pa_5_plus", lambda v: v >= 5)],
        )


def _reason_summary(rows: list[dict[str, Any]], window: str) -> list[dict[str, Any]]:
    rows_all = list(rows)
    candidates: list[tuple[str, Callable[[dict[str, Any]], bool], str]] = [
        ("postgame_actual_pa_le_3", lambda r: (_f(r.get("actual_plate_appearances")) or 99) <= 3, "diagnostic_only"),
        ("postgame_actual_pa_eq_4", lambda r: _f(r.get("actual_plate_appearances")) == 4, "diagnostic_only"),
        ("pregame_starter_expected_lt_5", lambda r: (_f(r.get("starter_expected_hits_allowed")) is not None and _f(r.get("starter_expected_hits_allowed")) < 5), "veto_candidate"),
        ("pregame_team_expected_lt_8.5", lambda r: (_f(r.get("team_expected_hits_allowed")) is not None and _f(r.get("team_expected_hits_allowed")) < 8.5), "veto_candidate"),
        ("pregame_price_plus_le_125", lambda r: (_f(r.get("price")) is not None and _f(r.get("price")) <= 125), "veto_candidate"),
        ("pregame_price_plus_le_150", lambda r: (_f(r.get("price")) is not None and _f(r.get("price")) <= 150), "veto_candidate"),
        ("pregame_d7_hrr_lt_3", lambda r: (_f(r.get("d7_hits_runs_rbis")) is not None and _f(r.get("d7_hits_runs_rbis")) < 3), "veto_candidate"),
        ("pregame_d15_hrr_lt_2.75", lambda r: (_f(r.get("d15_hits_runs_rbis")) is not None and _f(r.get("d15_hits_runs_rbis")) < 2.75), "veto_candidate"),
        ("pregame_bvp_pa_0", lambda r: _f(r.get("bvp_plate_appearances")) == 0, "veto_candidate"),
        ("pregame_bvp_pa_5_plus_avg_lt_250", lambda r: (_f(r.get("bvp_plate_appearances")) or 0) >= 5 and (_f(r.get("bvp_avg")) or 0) < 0.25, "veto_candidate"),
        ("pregame_bvp_pa_5_plus_slg_lt_350", lambda r: (_f(r.get("bvp_plate_appearances")) or 0) >= 5 and (_f(r.get("bvp_slg")) or 0) < 0.35, "veto_candidate"),
        ("pregame_lineup_bottom_7_9", lambda r: r.get("lineup_bucket") == "bottom_7_9", "veto_candidate"),
        ("pregame_day_after_night", lambda r: r.get("team_time_sequence_bucket") == "day_after_night", "veto_candidate"),
        ("pregame_short_turnaround", lambda r: r.get("short_turnaround_proxy") == "yes", "veto_candidate"),
        ("pregame_u15_candidate_same_day", lambda r: bool(r.get("u15_candidate_same_day")), "veto_candidate"),
        ("pregame_not_ranking_qc_overlap", lambda r: not bool(r.get("ranking_qc_overlap_same_side")), "veto_candidate"),
    ]
    base = _metrics(rows_all)
    out: list[dict[str, Any]] = []
    for name, pred, kind in candidates:
        subset = [r for r in rows_all if pred(r)]
        retained = [r for r in rows_all if not pred(r)]
        sub = _metrics(subset)
        ret = _metrics(retained)
        out.append(
            {
                "window": window,
                "signal": name,
                "signal_type": kind,
                "signal_rows": len(subset),
                "signal_row_share": len(subset) / len(rows_all) if rows_all else None,
                "signal_wr": sub.get("wr"),
                "signal_roi": sub.get("roi"),
                "signal_units": sub.get("units"),
                "signal_losses": sub.get("losses"),
                "signal_loss_rate": sub.get("losses") / (sub.get("wins") + sub.get("losses")) if (sub.get("wins") + sub.get("losses")) else None,
                "retained_rows_after_veto": len(retained),
                "retained_wr_after_veto": ret.get("wr"),
                "retained_roi_after_veto": ret.get("roi"),
                "retained_units_after_veto": ret.get("units"),
                "base_wr": base.get("wr"),
                "base_roi": base.get("roi"),
                "veto_roi_lift": (ret.get("roi") - base.get("roi")) if ret.get("roi") is not None and base.get("roi") is not None else None,
                "veto_units_delta": (ret.get("units") - base.get("units")) if ret.get("units") is not None and base.get("units") is not None else None,
            }
        )
    return out


def _window(rows: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    if label == "season_2026":
        return [r for r in rows if str(r.get("date") or "").startswith("2026-")]
    latest = max(str(r.get("date")) for r in rows)
    latest_d = datetime.strptime(latest, "%Y-%m-%d").date()
    if label == "last_30":
        return [
            r
            for r in rows
            if 0 <= (latest_d - datetime.strptime(str(r.get("date")), "%Y-%m-%d").date()).days <= 29
        ]
    return rows


def _write_report(path: Path, rows: list[dict[str, Any]], reasons: list[dict[str, Any]]) -> None:
    last30 = _window(rows, "last_30")
    season = _window(rows, "season_2026")
    top_veto = sorted(
        [
            r
            for r in reasons
            if r["window"] == "last_30"
            and r["signal_type"] == "veto_candidate"
            and int(r.get("signal_rows") or 0) >= 5
            and r.get("veto_roi_lift") is not None
        ],
        key=lambda r: (float(r.get("veto_roi_lift") or -999), float(r.get("signal_loss_rate") or 0), int(r.get("signal_rows") or 0)),
        reverse=True,
    )[:10]
    lines = [
        "# Tier A Failure Audit",
        "",
        "- Scope: hits over 1.5, Hitter Tier A only.",
        "- Hitter Tier A definition: `d7_hits > 1.30` and `d15_hits > 1.20`.",
        "- This is analysis only; no production, selector, threshold, upload, or grading changes.",
        "",
        "## Population",
        "",
        "| window | rows | wins | losses | WR | ROI | units |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for label, wrows in [("last_30", last30), ("season_2026", season)]:
        m = _metrics(wrows)
        lines.append(
            f"| `{label}` | `{m['rows']}` | `{m['wins']}` | `{m['losses']}` | `{_pct(m['wr'])}` | `{_pct(m['roi'])}` | `{_num(m['units'])}` |"
        )
    lines.extend(
        [
            "",
            "## Last-30 Outcome Split",
            "",
            "| group | rows | avg odds | avg d7 | avg d15 | avg d7 HRR | avg d15 HRR | avg PA |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for group in ["winner_2_plus_hits", "one_hit_loser", "zero_hit_loser"]:
        grows = [r for r in last30 if r.get("outcome_group") == group]
        m = _metrics(grows)
        lines.append(
            f"| `{group}` | `{m['rows']}` | `{_num(m['avg_odds'])}` | `{_num(m['avg_d7_hits'])}` | `{_num(m['avg_d15_hits'])}` | `{_num(m['avg_d7_hits_runs_rbis'])}` | `{_num(m['avg_d15_hits_runs_rbis'])}` | `{_num(m['avg_actual_plate_appearances'])}` |"
        )
    lines.extend(
        [
            "",
            "## Top Veto Candidates",
            "",
            "| rank | signal | rows | signal WR | signal ROI | retained ROI after veto | ROI lift |",
            "|---:|---|---:|---:|---:|---:|---:|",
        ]
    )
    for idx, row in enumerate(top_veto, start=1):
        lines.append(
            f"| `{idx}` | `{row['signal']}` | `{row['signal_rows']}` | `{_pct(row['signal_wr'])}` | `{_pct(row['signal_roi'])}` | `{_pct(row['retained_roi_after_veto'])}` | `{_pct(row['veto_roi_lift'])}` |"
        )
    best = top_veto[0] if top_veto else None
    lines.extend(["", "## Final Review Veto", ""])
    if best:
        lines.append(
            f"If allowed one historically evidenced pregame veto, the current best candidate is `{best['signal']}`. "
            f"It appeared on `{best['signal_rows']}` last-30 Tier A rows; removing it would have moved retained Tier A ROI to `{_pct(best['retained_roi_after_veto'])}` in this sample."
        )
    else:
        lines.append("No pregame veto signal cleared the sample floor.")
    lines.extend(
        [
            "",
            "## Availability Notes",
            "",
            "- Bullpen standalone quality/usage metrics are not present in execution reconcile rows.",
            "- `bullpen_expected_hits_allowed_component` is derived where both `team_expected_hits_allowed` and `starter_expected_hits_allowed` are available.",
            "- BvP strikeout fields were not found in the compact BvP lineage bundle; BvP PA/hits/AVG/SLG/TB are included.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execution-root", default=str(DEFAULT_EXEC_ROOT))
    ap.add_argument("--review-aids-dir", default=str(DEFAULT_REVIEW_DIR))
    ap.add_argument("--lanes-root", default=str(DEFAULT_LANES_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_REVIEW_DIR))
    args = ap.parse_args()

    rows = _load_tier_a_rows(Path(args.execution_root))
    if not rows:
        raise SystemExit("No Tier A rows found")
    _enrich_rows(rows, Path(args.review_aids_dir), Path(args.lanes_root))
    _annotate_buckets(rows)

    out_dir = Path(args.out_dir)
    feature_rows: list[dict[str, Any]] = []
    reason_rows: list[dict[str, Any]] = []
    for label in ["last_30", "season_2026"]:
        wrows = _window(rows, label)
        feature_rows.extend(_feature_comparison(wrows, label))
        reason_rows.extend(_reason_summary(wrows, label))

    output_cols = [
        "date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "line",
        "side",
        "result",
        "outcome_group",
        "actual_hits",
        "actual_plate_appearances",
        "actual_pa_bucket",
        "lineup_slot",
        "lineup_bucket",
        "price",
        "implied_over",
        "market_no_vig_implied_over",
        "model_prob_over",
        "d7_hits",
        "d15_hits",
        "d30_hits",
        "d7_hits_runs_rbis",
        "d15_hits_runs_rbis",
        "d30_hits_runs_rbis",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "bullpen_expected_hits_allowed_component",
        "bullpen_metrics_available",
        "game_time",
        "time_of_day_bucket",
        "game_day_of_week",
        "team_time_sequence_bucket",
        "rest_day_before_game",
        "short_turnaround_proxy",
        "bvp_plate_appearances",
        "bvp_hits",
        "bvp_avg",
        "bvp_slg",
        "bvp_total_bases",
        "u15_candidate_same_day",
        "ranking_qc_overlap_same_side",
        "price_bucket",
        "starter_bucket",
        "team_expected_bucket",
        "bvp_bucket",
        "source_reconcile_file",
    ]
    row_out = [{col: row.get(col) for col in output_cols} for row in rows]
    _write_csv(out_dir / "tier_a_failure_rows.csv", row_out)
    _write_csv(out_dir / "tier_a_failure_feature_comparison.csv", feature_rows)
    _write_csv(out_dir / "tier_a_failure_reason_summary.csv", reason_rows)
    _write_report(out_dir / "tier_a_failure_audit.md", rows, reason_rows)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": len(rows),
        "last_30_rows": len(_window(rows, "last_30")),
        "season_2026_rows": len(_window(rows, "season_2026")),
        "outputs": [
            str(out_dir / "tier_a_failure_audit.md"),
            str(out_dir / "tier_a_failure_feature_comparison.csv"),
            str(out_dir / "tier_a_failure_rows.csv"),
            str(out_dir / "tier_a_failure_reason_summary.csv"),
        ],
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
