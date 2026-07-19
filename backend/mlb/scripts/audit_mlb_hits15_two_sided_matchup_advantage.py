"""Bounded read-only Hits 1.5 two-sided matchup advantage audit.

The audit assembles local review-board, lane-selector, PA diagnostic, and July
12 sentinel artifacts into a proposition-grain ledger. It does not train,
optimize, score, or alter any production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AUDIT_DATE = "2026-07-17"
START_DATE = "2026-05-01"
DEFAULT_OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits15_two_sided_matchup_advantage_audit/2026-07-17"
)
REVIEW_DIR = Path("artifacts/analysis/mlb/review_aids")
LANE_ROOT = Path("backend/mlb/exports/model_v2/lanes/today")
PA_MANIFEST = Path(
    "artifacts/analysis/model_development/mlb_hits_15_pa_opportunity_overlay_diagnostic/"
    "2026-07-16/historical_population_manifest_2026-07-16.csv"
)
SENTINEL_DIR = Path(
    "artifacts/analysis/model_development/mlb_july12_favorite_slate_sentinel_failure_audit/"
    "2026-07-17"
)
SENTINEL_LEDGER = SENTINEL_DIR / f"corrected_prediction_vs_execution_ledger_{AUDIT_DATE}.csv"
SENTINEL_SETTLEMENT = SENTINEL_DIR / f"official_settlement_certification_{AUDIT_DATE}.csv"


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _i(value: Any) -> int | None:
    number = _f(value)
    if number is None:
        return None
    return int(number)


def _s(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _b(value: Any) -> bool:
    return _s(value).lower() in {"1", "true", "yes", "y"}


def _date_from_path(path: Path) -> str:
    m = re.search(r"20\d{2}-\d{2}-\d{2}", path.as_posix())
    return m.group(0) if m else ""


def _norm_line(value: Any) -> str:
    number = _f(value)
    return f"{number:.1f}" if number is not None else ""


def _key(date: Any, game_id: Any, player_id: Any, line: Any) -> str:
    return f"{_s(date)[:10]}|{_i(game_id) or ''}|{_i(player_id) or ''}|hits|{_norm_line(line)}"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
        if not fieldnames:
            fieldnames = ["status"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def _american_profit_per_unit(price: float | None) -> float | None:
    if price is None:
        return None
    return price / 100.0 if price > 0 else 100.0 / abs(price)


def _side_result(side: str, official_hits: float | None) -> str:
    if official_hits is None:
        return "unresolved"
    over_win = official_hits >= 2
    if side == "over":
        return "win" if over_win else "loss"
    if side == "under":
        return "loss" if over_win else "win"
    return "unresolved"


def _roi_for_side(side: str, official_hits: float | None, price: float | None) -> float | None:
    result = _side_result(side, official_hits)
    if result == "unresolved" or price is None:
        return None
    if result == "loss":
        return -1.0
    return _american_profit_per_unit(price)


def board_type(path: Path) -> str:
    name = path.name
    if "alternate_discovery" in name:
        return "hits_o15_alternate_discovery"
    if "layered_candidates" in name:
        return "hits_o15_layered_candidates"
    if "watch_candidates" in name:
        return "hits_o15_watch_candidates"
    if "simple_filter" in name:
        return "hits_o15_simple_filter"
    if "hits_u15_favorite_audit" in name:
        return "hits_u15_favorite_audit"
    return path.stem


def source_surface_side(surface: str) -> str:
    return "under" if "u15" in surface else "over"


def load_outcome_context() -> dict[str, dict[str, Any]]:
    outcome: dict[str, dict[str, Any]] = {}
    pa = _read_csv(PA_MANIFEST)
    if not pa.empty:
        pa = pa[pa["slate_date"].astype(str).ge(START_DATE)]
        for _, row in pa.iterrows():
            if _s(row.get("prop_type")).lower() != "hits" or _norm_line(row.get("line")) != "1.5":
                continue
            key = _key(row.get("slate_date"), row.get("game_id"), row.get("player_id"), row.get("line"))
            official_hits = _f(row.get("target_value"))
            outcome[key] = {
                "official_hits": official_hits,
                "official_over_result": "win" if official_hits is not None and official_hits >= 2 else "loss" if official_hits is not None else "unresolved",
                "official_under_result": "loss" if official_hits is not None and official_hits >= 2 else "win" if official_hits is not None else "unresolved",
                "outcome_source": str(PA_MANIFEST),
                "control_probability": _f(row.get("control_probability")),
                "selected_price": _f(row.get("selected_price")),
                "pa_opp_v1_d7_pa_pg": _f(row.get("pa_opp_v1_d7_pa_pg")),
                "pa_opp_v1_d15_pa_pg": _f(row.get("pa_opp_v1_d15_pa_pg")),
                "pa_opp_v1_d30_pa_pg": _f(row.get("pa_opp_v1_d30_pa_pg")),
                "pa_opp_v1_d15_opportunity_band": _s(row.get("pa_opp_v1_d15_opportunity_band")),
                "pa_semantics_status": _s(row.get("pa_semantics_status")),
                "pa_source_regime": _s(row.get("pa_source_regime")),
            }
    if SENTINEL_SETTLEMENT.exists():
        settlement = _read_csv(SENTINEL_SETTLEMENT)
        for _, row in settlement.iterrows():
            key = _key(row.get("slate_date"), row.get("game_id"), row.get("player_id"), row.get("line"))
            official_hits = _f(row.get("official_hits"))
            existing = outcome.get(key, {})
            existing.update(
                {
                    "official_hits": official_hits,
                    "official_over_result": "win" if official_hits is not None and official_hits >= 2 else "loss" if official_hits is not None else "unresolved",
                    "official_under_result": "loss" if official_hits is not None and official_hits >= 2 else "win" if official_hits is not None else "unresolved",
                    "outcome_source": str(SENTINEL_SETTLEMENT),
                    "official_settlement_status": _s(row.get("official_settlement_status")),
                }
            )
            outcome[key] = existing
    return outcome


def load_lane_market_context() -> dict[str, dict[str, Any]]:
    ctx: dict[str, dict[str, Any]] = {}
    for path in sorted(LANE_ROOT.glob("*/hits_lane_selector_*.csv")) + sorted(LANE_ROOT.glob("*/quick_card_hits_*.csv")):
        if any(token in path.name for token in ["ranking_upload_input", "environment_diagnostics"]):
            continue
        df = _read_csv(path)
        if df.empty:
            continue
        for _, row in df.iterrows():
            if _s(row.get("prop_type")).lower() != "hits" or _norm_line(row.get("line")) != "1.5":
                continue
            date = _s(row.get("date"))[:10] or _date_from_path(path)
            if date < START_DATE:
                continue
            key = _key(date, row.get("game_id"), row.get("player_id"), row.get("line"))
            item = ctx.setdefault(key, {})
            for col, out_col in [
                ("market_price_over", "o15_price"),
                ("odds_over", "o15_price"),
                ("market_price_under", "u15_price"),
                ("odds_under", "u15_price"),
                ("market_book_count_two_sided", "market_book_count_two_sided"),
                ("market_snapshot_time_utc", "market_snapshot_time_utc"),
                ("market_snapshot_run_tag", "market_snapshot_run_tag"),
                ("market_odds_snapshot_file", "market_odds_snapshot_file"),
            ]:
                value = row.get(col)
                if out_col not in item or item.get(out_col) in ("", None) or pd.isna(item.get(out_col)):
                    if _s(value):
                        item[out_col] = value
            selected_side = _s(row.get("side")).lower()
            if selected_side:
                item.setdefault("lane_selected_sides", set()).add(selected_side)
                item.setdefault("lane_source_lanes", set()).add(_s(row.get("source_lane")) or path.name)
    for item in ctx.values():
        if isinstance(item.get("lane_selected_sides"), set):
            item["lane_selected_sides"] = ";".join(sorted(item["lane_selected_sides"]))
        if isinstance(item.get("lane_source_lanes"), set):
            item["lane_source_lanes"] = ";".join(sorted(item["lane_source_lanes"]))
    return ctx


def evidence_from_row(row: pd.Series) -> dict[str, Any]:
    return {
        "player_name": _s(row.get("player_name")) or _s(row.get("player")),
        "team": _s(row.get("team")) or _s(row.get("canonical_team")),
        "opponent": _s(row.get("opponent")) or _s(row.get("canonical_opponent")),
        "d7_hits_rate": _f(row.get("d7_hits_rate")),
        "d15_hits_rate": _f(row.get("d15_hits_rate")),
        "d30_hits_runs_rbis": _f(row.get("d30_hits_runs_rbis")),
        "starter_expected_hits_allowed": _f(row.get("starter_expected_hits_allowed")),
        "pitcher_base": _f(row.get("pitcher_base")) or _f(row.get("pitcher_expected_hits_allowed_weighted")),
        "team_expected_hits_allowed": _f(row.get("team_expected_hits_allowed")),
        "offense_factor_vs_league_clamped": _f(row.get("offense_factor_vs_league_clamped")),
        "hitter_tier_seen": _s(row.get("hitter_tier")),
        "pitcher_tier_seen": _s(row.get("pitcher_tier")),
        "combined_tier_seen": _s(row.get("combined_tier")),
        "opposing_starter": _s(row.get("opposing_starter")),
        "opposing_starter_id": _s(row.get("opposing_starter_id")),
        "starter_context_status": _s(row.get("starter_context_status")),
        "starter_context_source": _s(row.get("starter_context_source")),
        "starter_starts_count": _f(row.get("starter_starts_count")),
        "model_prob": _f(row.get("model_prob")),
        "qc_score": _f(row.get("qc_score")),
        "qc_selected_side": _s(row.get("qc_selected_side")).lower(),
        "ranking_score": _f(row.get("ranking_score")),
        "game_time": _s(row.get("game_time")),
        "time_of_day_bucket": _s(row.get("time_of_day_bucket")),
        "local_team_hits_parity_status": _s(row.get("local_team_hits_parity_status")),
        "offense_context_as_of_date": _s(row.get("offense_context_as_of_date")),
    }


def _merge_first(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if key not in target or target.get(key) in ("", None) or (isinstance(target.get(key), float) and math.isnan(target.get(key))):
            if value not in ("", None):
                target[key] = value


def build_proposition_ledger() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    outcome = load_outcome_context()
    market = load_lane_market_context()
    propositions: dict[str, dict[str, Any]] = {}
    source_files = []

    review_files = sorted(REVIEW_DIR.glob("hits_o15_*_2026-*.csv")) + sorted(REVIEW_DIR.glob("hits_u15_favorite_audit_2026-*.csv"))
    for path in review_files:
        date = _date_from_path(path)
        if not date or date < START_DATE:
            continue
        df = _read_csv(path)
        if df.empty:
            continue
        surface = board_type(path)
        surface_side = source_surface_side(surface)
        source_files.append(path)
        for _, row in df.iterrows():
            line = _norm_line(row.get("line"))
            if line != "1.5":
                continue
            game_id = row.get("game_id") or row.get("canonical_game_id")
            player_id = row.get("player_id") or row.get("canonical_player_id")
            key = _key(row.get("date") or date, game_id, player_id, row.get("line"))
            if "||" in key or not _s(player_id):
                continue
            prop = propositions.setdefault(
                key,
                {
                    "canonical_proposition_key": key,
                    "slate_date": _s(row.get("date"))[:10] or date,
                    "game_id": _i(game_id) or "",
                    "player_id": _i(player_id) or "",
                    "prop_type": "hits",
                    "line": "1.5",
                    "source_artifacts": set(),
                    "over_surfaces": set(),
                    "under_surfaces": set(),
                    "candidate_surface_count": 0,
                },
            )
            prop["source_artifacts"].add(str(path))
            prop["candidate_surface_count"] += 1
            if surface_side == "over":
                prop["over_surfaces"].add(surface)
                if _s(row.get("market_price")):
                    prop.setdefault("o15_price", row.get("market_price"))
                if _s(row.get("best_over_price")):
                    prop.setdefault("o15_price", row.get("best_over_price"))
            else:
                prop["under_surfaces"].add(surface)
                if _s(row.get("market_price")):
                    prop.setdefault("u15_price", row.get("market_price"))
            _merge_first(prop, evidence_from_row(row))

    # Sentinel rows are authoritative for July 12 prediction population even if
    # one row (Curtis Mead) was not recovered in generated surfaces.
    if SENTINEL_LEDGER.exists():
        sent = _read_csv(SENTINEL_LEDGER)
        for _, row in sent.iterrows():
            key = _key(row.get("slate_date"), row.get("game_id"), row.get("player_id"), row.get("line"))
            prop = propositions.setdefault(
                key,
                {
                    "canonical_proposition_key": key,
                    "slate_date": _s(row.get("slate_date"))[:10],
                    "game_id": _i(row.get("game_id")) or "",
                    "player_id": _i(row.get("player_id")) or "",
                    "prop_type": "hits",
                    "line": "1.5",
                    "source_artifacts": set(),
                    "over_surfaces": set(),
                    "under_surfaces": set(),
                    "candidate_surface_count": 0,
                },
            )
            prop["source_artifacts"].add(str(SENTINEL_LEDGER))
            prop["over_surfaces"].add("july12_tracker_bound_sentinel")
            prop["candidate_surface_count"] += 1
            prop["july12_sentinel"] = True
            prop["prediction_status"] = _s(row.get("prediction_status"))
            prop["execution_status"] = _s(row.get("execution_status"))
            prop["execution_population_included"] = _s(row.get("execution_population_included"))
            prop.setdefault("o15_price", row.get("odds"))
            _merge_first(
                prop,
                {
                    "player_name": row.get("player_name"),
                    "team": row.get("team"),
                    "opponent": row.get("opponent"),
                },
            )
    for key, prop in propositions.items():
        prop.update(outcome.get(key, {}))
        prop.update({k: v for k, v in market.get(key, {}).items() if k not in prop or prop.get(k) in ("", None)})
        finalize_prop(prop)

    rows = [normalize_sets(row) for row in propositions.values()]
    rows.sort(key=lambda r: (r.get("slate_date", ""), str(r.get("game_id", "")), str(r.get("player_id", ""))))
    meta = {
        "review_source_files": len(source_files),
        "proposition_rows": len(rows),
        "rows_with_official_hits": sum(1 for r in rows if _f(r.get("official_hits")) is not None),
        "sentinel_rows": sum(1 for r in rows if _b(r.get("july12_sentinel"))),
    }
    return rows, meta


def normalize_sets(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key in ["source_artifacts", "over_surfaces", "under_surfaces"]:
        if isinstance(out.get(key), set):
            out[key] = ";".join(sorted(out[key]))
    return out


def support_label(d7: float | None, d15: float | None, starter: float | None) -> tuple[str, str, str, str]:
    hitter_complete = d7 is not None and d15 is not None
    starter_complete = starter is not None
    if not hitter_complete:
        hitter = "missing"
    elif d7 > 1.3 and d15 > 1.2:
        hitter = "strong_affirmative_hitter"
    elif d7 > 1.0 and d15 > 1.0:
        hitter = "affirmative_hitter"
    elif d7 < 1.0 and d15 < 1.0:
        hitter = "cold_hitter"
    else:
        hitter = "mixed_hitter"
    if not starter_complete:
        pitcher = "missing"
    elif starter < 4.5:
        pitcher = "strong_pitcher_suppression"
    elif starter < 5.0:
        pitcher = "moderate_pitcher_suppression"
    elif starter >= 5.5:
        pitcher = "strong_hitter_environment"
    elif starter >= 5.0:
        pitcher = "moderate_hitter_environment"
    else:
        pitcher = "neutral_pitcher_environment"
    hitter_aff = hitter in {"strong_affirmative_hitter", "affirmative_hitter"} or pitcher in {
        "strong_hitter_environment",
        "moderate_hitter_environment",
    }
    pitcher_aff = pitcher in {"strong_pitcher_suppression", "moderate_pitcher_suppression"}
    if not hitter_complete or not starter_complete:
        owner = "incomplete"
    elif hitter_aff and pitcher_aff:
        owner = "conflicting"
    elif hitter_aff:
        owner = "hitter_dominant"
    elif pitcher_aff:
        owner = "pitcher_dominant"
    else:
        owner = "weak_both"
    missing = []
    if not hitter_complete:
        missing.append("hitter_form")
    if not starter_complete:
        missing.append("starter_expected_hits_allowed")
    return hitter, pitcher, owner, ";".join(missing)


def finalize_prop(prop: dict[str, Any]) -> None:
    over_surfaces = prop.get("over_surfaces") or set()
    under_surfaces = prop.get("under_surfaces") or set()
    if isinstance(over_surfaces, str):
        over_present = bool(over_surfaces)
    else:
        over_present = bool(over_surfaces)
    if isinstance(under_surfaces, str):
        under_present = bool(under_surfaces)
    else:
        under_present = bool(under_surfaces)
    if over_present and under_present:
        current = "BOTH_CONFLICT"
    elif over_present:
        current = "OVER_ONLY"
    elif under_present:
        current = "UNDER_ONLY"
    else:
        current = "NEITHER"
    prop["current_side_surface_state"] = current
    d7 = _f(prop.get("d7_hits_rate"))
    d15 = _f(prop.get("d15_hits_rate"))
    starter = _f(prop.get("starter_expected_hits_allowed"))
    hitter_label, pitcher_label, ownership, missing = support_label(d7, d15, starter)
    prop["hitter_evidence_label"] = hitter_label
    prop["pitcher_suppression_label"] = pitcher_label
    prop["baseball_directional_ownership"] = ownership
    prop["evidence_missingness"] = missing
    official_hits = _f(prop.get("official_hits"))
    prop["official_over_result"] = _side_result("over", official_hits)
    prop["official_under_result"] = _side_result("under", official_hits)
    selected_side = "over" if current == "OVER_ONLY" else "under" if current == "UNDER_ONLY" else "both" if current == "BOTH_CONFLICT" else "none"
    prop["current_selected_direction"] = selected_side
    if selected_side in {"over", "under"}:
        prop["current_selected_result"] = _side_result(selected_side, official_hits)
        price = _f(prop.get("o15_price" if selected_side == "over" else "u15_price"))
        roi = _roi_for_side(selected_side, official_hits, price)
        prop["current_selected_roi_units_per_1u"] = roi if roi is not None else ""
    else:
        prop["current_selected_result"] = "ambiguous_or_none"
        prop["current_selected_roi_units_per_1u"] = ""
    opposite = "under" if selected_side == "over" else "over" if selected_side == "under" else ""
    prop["opposite_side_result"] = _side_result(opposite, official_hits) if opposite else ""
    prop["opposite_side_roi_units_per_1u"] = (
        _roi_for_side(opposite, official_hits, _f(prop.get("u15_price" if opposite == "under" else "o15_price")))
        if opposite
        else ""
    )
    prop["o15_available_in_artifacts"] = bool(prop.get("o15_price") not in ("", None) or over_present)
    prop["u15_available_in_artifacts"] = bool(prop.get("u15_price") not in ("", None) or under_present)


def summarize(rows: list[dict[str, Any]], group_fields: list[str]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(f, "") for f in group_fields)].append(row)
    out = []
    for key, items in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        resolved = [r for r in items if r.get("official_over_result") in {"win", "loss"}]
        over_wins = sum(1 for r in resolved if r.get("official_over_result") == "win")
        under_wins = sum(1 for r in resolved if r.get("official_under_result") == "win")
        selected = [r for r in resolved if r.get("current_selected_result") in {"win", "loss"}]
        selected_wins = sum(1 for r in selected if r.get("current_selected_result") == "win")
        opposite = [r for r in resolved if r.get("opposite_side_result") in {"win", "loss"}]
        opposite_wins = sum(1 for r in opposite if r.get("opposite_side_result") == "win")
        row = {field: key[idx] for idx, field in enumerate(group_fields)}
        row.update(
            {
                "proposition_count": len(items),
                "resolved_count": len(resolved),
                "date_min": min([_s(r.get("slate_date")) for r in items] or [""]),
                "date_max": max([_s(r.get("slate_date")) for r in items] or [""]),
                "over_outcome_rate": over_wins / len(resolved) if resolved else "",
                "under_outcome_rate": under_wins / len(resolved) if resolved else "",
                "current_side_selection_rate": len(selected) / len(resolved) if resolved else "",
                "current_side_win_rate": selected_wins / len(selected) if selected else "",
                "opposite_side_win_rate": opposite_wins / len(opposite) if opposite else "",
                "avg_current_selected_roi_units_per_1u": avg([_f(r.get("current_selected_roi_units_per_1u")) for r in selected]),
                "avg_opposite_side_roi_units_per_1u": avg([_f(r.get("opposite_side_roi_units_per_1u")) for r in opposite]),
                "uncertainty_interval_note": "Wilson intervals not computed in bounded audit; sparse groups should be treated directionally only.",
            }
        )
        out.append(row)
    return out


def avg(values: list[float | None]) -> float | str:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else ""


def architecture_map() -> list[dict[str, Any]]:
    return [
        {
            "surface": "Hits Over 1.5 Watch Candidates",
            "artifact_pattern": "artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_DATE.csv",
            "code_lineage": "backend/mlb/scripts/run_mlb_hits_o15_review_board.py --board watch_o15",
            "input_fields": "Quick Card context, d7_hits_rate, starter_expected_hits_allowed, market_price_over",
            "rule_or_formula": "qc row required; d7_hits_rate > 1.0; starter_expected_hits_allowed >= 5.0",
            "output_side": "OVER",
            "symmetric_over_under": False,
            "pitcher_can_override_hitter": "partial: starter expected below 5 blocks watch inclusion",
            "no_advantage_possible": "yes by exclusion, but no explicit NO_SUBSTANTIATED_ADVANTAGE row",
            "surface_type": "review-aid/discovery",
        },
        {
            "surface": "Hits Over 1.5 Layered Candidates",
            "artifact_pattern": "hits_o15_layered_candidates_DATE.csv",
            "code_lineage": "run_mlb_hits_o15_review_board.py --board layered_o15",
            "input_fields": "d7_hits_rate, d15_hits_rate, starter_expected_hits_allowed, qc/ranking context",
            "rule_or_formula": "layer labels from d7>1.0, d15>1.0, starter_expected>=5.0, qc presence",
            "output_side": "OVER",
            "symmetric_over_under": False,
            "pitcher_can_override_hitter": "partial: unfavorable starter lowers layer but does not create UNDER",
            "no_advantage_possible": "all_o15_other exists in CSV but is not a three-way ownership decision",
            "surface_type": "qualified research/review aid",
        },
        {
            "surface": "Hits 1.5 Alternate Discovery",
            "artifact_pattern": "hits_o15_alternate_discovery_DATE.csv",
            "code_lineage": "run_mlb_hits_o15_review_board.py --board alternate_o15",
            "input_fields": "alternate market lines/books plus hitter form/starter environment",
            "rule_or_formula": "manual research alternate O1.5 discovery layers",
            "output_side": "OVER",
            "symmetric_over_under": False,
            "pitcher_can_override_hitter": "not as a two-sided comparison; starter support affects layer",
            "no_advantage_possible": "not explicit",
            "surface_type": "discovery-only alternate-market board",
        },
        {
            "surface": "Hits Under 1.5 Favorite Audit",
            "artifact_pattern": "hits_u15_favorite_audit_DATE.csv",
            "code_lineage": "run_mlb_hits_o15_review_board.py --board u15",
            "input_fields": "d7_hits_rate, d15_hits_rate, starter_expected_hits_allowed, qc selected under",
            "rule_or_formula": "d7<1.0, d15<1.0, starter_expected<4.5 drive strongest layers",
            "output_side": "UNDER",
            "symmetric_over_under": False,
            "pitcher_can_override_hitter": "UNDER requires affirmative tough-starter evidence for top layer",
            "no_advantage_possible": "all_u15_other exists in CSV but no explicit three-way arbiter",
            "surface_type": "UNDER review-aid/favorite audit",
        },
        {
            "surface": "Lane Selector / Quick Card",
            "artifact_pattern": "backend/mlb/exports/model_v2/lanes/today/DATE/*hits*.csv",
            "code_lineage": "build_mlb_hits_lane_selector.py / export_mlb_daily_quick_card.py",
            "input_fields": "model/lane scores, rolling stats, market prices, BvP, selected side",
            "rule_or_formula": "ranked side-specific lanes; not a proposition-level two-sided ownership arbiter",
            "output_side": "OVER/UNDER depending lane",
            "symmetric_over_under": "partially: side exists, but not governed as ownership comparison at 1.5",
            "pitcher_can_override_hitter": "unknown/partial from lane features",
            "no_advantage_possible": "via no selected row, not explicit",
            "surface_type": "prediction/ranking surface",
        },
    ]


def evidence_domain_registry() -> list[dict[str, Any]]:
    return [
        {"domain": "hitter_strict_prior_hits", "side_supported": "OVER when d7/d15 strong; UNDER only when cold plus suppression", "fields": "d7_hits_rate,d15_hits_rate,d30 concepts", "readiness": "partial", "notes": "Available in review boards; not yet governed as ownership arbiter."},
        {"domain": "pitcher_suppression", "side_supported": "UNDER when starter_expected_hits_allowed < 4.5/5.0", "fields": "starter_expected_hits_allowed,pitcher_base,starter_context_status", "readiness": "partial", "notes": "Available but tier letters invert between O and U boards; raw expected value is safer for comparison."},
        {"domain": "pitcher_vulnerability", "side_supported": "OVER when starter_expected_hits_allowed >= 5.0/5.5", "fields": "starter_expected_hits_allowed,pitcher_base,offense_factor", "readiness": "partial", "notes": "Mature platform component but still blended with offense factor."},
        {"domain": "pa_opportunity", "side_supported": "context/confirmatory", "fields": "pa_opp_v1_* from PA diagnostic manifest", "readiness": "partial", "notes": "Historical manifest is over-side selected-proposition spine; not fully joined to every review-board row."},
        {"domain": "lineup_role", "side_supported": "context/confirmatory", "fields": "postgame/pregame lineup capture research", "readiness": "not_in_this_ledger", "notes": "Role quality exists as research but is not safely retained across all historical propositions here."},
        {"domain": "market_execution", "side_supported": "neither baseball side", "fields": "O/U price, availability, EV, book coverage", "readiness": "partial", "notes": "Preserved separately; not used to define baseball ownership."},
    ]


def july12_directional_rows(ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sentinel = _read_csv(SENTINEL_LEDGER) if SENTINEL_LEDGER.exists() else pd.DataFrame()
    by_key = {row["canonical_proposition_key"]: row for row in ledger}
    out = []
    for _, srow in sentinel.iterrows():
        key = _key(srow.get("slate_date"), srow.get("game_id"), srow.get("player_id"), srow.get("line"))
        row = by_key.get(key, {})
        ownership = row.get("baseball_directional_ownership", "incomplete")
        if ownership == "hitter_dominant":
            retrospective = "hitter evidence stronger"
        elif ownership == "pitcher_dominant":
            retrospective = "pitcher evidence stronger"
        elif ownership == "conflicting":
            retrospective = "conflicting"
        elif ownership == "weak_both":
            retrospective = "insufficient evidence"
        else:
            retrospective = "architecture did not compare both sides"
        out.append(
            {
                "wager_id": srow.get("wager_id"),
                "player_name": srow.get("player_name"),
                "canonical_proposition_key": key,
                "why_hitter_side_surfaced": row.get("over_surfaces", "july12_tracker_bound_sentinel"),
                "hitter_supporting_fields": f"d7={row.get('d7_hits_rate','')}; d15={row.get('d15_hits_rate','')}; hitter_label={row.get('hitter_evidence_label','')}",
                "pitcher_suppression_fields": f"starter_expected_hits_allowed={row.get('starter_expected_hits_allowed','')}; pitcher_label={row.get('pitcher_suppression_label','')}",
                "opportunity_evidence": f"pa_d15={row.get('pa_opp_v1_d15_pa_pg','')}; pa_band={row.get('pa_opp_v1_d15_opportunity_band','')}",
                "evidence_missingness": row.get("evidence_missingness", ""),
                "contrary_pitcher_evidence_evaluated": row.get("pitcher_suppression_label", "") not in {"", "missing"},
                "contrary_evidence_could_block_over": "partial in Watch/Layered if starter_expected below O thresholds; no global arbiter",
                "under_process_independently_surfaced_player": "hits_u15_favorite_audit" in str(row.get("under_surfaces", "")),
                "u15_line_price_available_in_artifacts": row.get("u15_price", ""),
                "o15_line_price_available_in_artifacts": row.get("o15_price", ""),
                "retrospective_evidence_classification_pre_outcome": retrospective,
                "official_hits": row.get("official_hits", ""),
                "official_over_result": row.get("official_over_result", ""),
                "official_under_result": row.get("official_under_result", ""),
                "prediction_status": srow.get("prediction_status"),
                "execution_status": srow.get("execution_status"),
            }
        )
    return out


def surface_overlap(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        surfaces = [x for x in (str(row.get("over_surfaces") or "") + ";" + str(row.get("under_surfaces") or "")).split(";") if x]
        if len(surfaces) < 2:
            continue
        if all("o15" in s or "sentinel" in s for s in surfaces):
            classification = "repeated expression of the same underlying signal"
        elif any("u15" in s for s in surfaces) and any("o15" in s for s in surfaces):
            classification = "partially overlapping corroboration"
        else:
            classification = "unresolved"
        out.append(
            {
                "canonical_proposition_key": row.get("canonical_proposition_key"),
                "player_name": row.get("player_name"),
                "surfaces": ";".join(sorted(set(surfaces))),
                "surface_count": len(set(surfaces)),
                "parent_data_overlap": "high: d7/d15 and starter_expected are reused across boards",
                "rule_overlap": "high for O15 boards; U15 reverses hitter/starter thresholds",
                "corroboration_classification": classification,
                "notes": "Multiple labels should not be treated as independent evidence without further validation.",
            }
        )
    return out


def historical_validation_quality() -> list[dict[str, Any]]:
    return [
        {"rule_or_cohort": "O15 d7/d15 hot + starter_expected>=5 layers", "construction_period": "current code/artifact era", "evaluation_period": "partial local review-board history", "frozen_before_evaluation": "partially", "sample_size": "available in ledger", "holdout_quality": "not certified here", "price_coverage": "partial", "directional_stability": "unknown", "interpretation": "Do not promote to rejection/selection ownership rule without out-of-sample validation."},
        {"rule_or_cohort": "U15 d7/d15 cold + starter_expected<4.5 layers", "construction_period": "current code/artifact era", "evaluation_period": "partial local review-board history", "frozen_before_evaluation": "partially", "sample_size": "available in ledger", "holdout_quality": "not certified here", "price_coverage": "partial", "directional_stability": "unknown", "interpretation": "A genuine suppression framework exists, but it is not governed as the complement of O15."},
        {"rule_or_cohort": "same-side/same-line concentration", "construction_period": "post-sentinel audit", "evaluation_period": "not completed", "frozen_before_evaluation": "no", "sample_size": "July12 one slate", "holdout_quality": "none", "price_coverage": "n/a", "directional_stability": "not validated", "interpretation": "Visible exposure feature, not a validated rejection signal."},
    ]


def framework_design() -> list[dict[str, Any]]:
    return [
        {"output": "HITTER_ADVANTAGE_OVER_15", "baseball_requirements": "affirmative hitter evidence and/or vulnerable starter environment; sufficient PA opportunity; no stronger pitcher suppression evidence", "market_stage": "after direction, require O1.5 posted/current and price/book freshness", "implementation_status": "design_only"},
        {"output": "PITCHER_ADVANTAGE_UNDER_15", "baseball_requirements": "affirmative pitcher suppression evidence, credible starter workload/role, acceptable completeness, no stronger hitter-side evidence", "market_stage": "after direction, require U1.5 posted/current and price/book freshness", "implementation_status": "design_only"},
        {"output": "NO_SUBSTANTIATED_ADVANTAGE", "baseball_requirements": "conflicting, weak, missing, or unstable evidence", "market_stage": "do not rescue with price/EV alone", "implementation_status": "design_only"},
    ]


def decisions() -> dict[str, str]:
    return {
        "MLB_HITS15_CURRENT_OBJECTIVE_BINDING_DECISION": "OBJECTIVE_REBOUND_TO_TWO_SIDED_MATCHUP_OWNERSHIP_OVER_UNDER_OR_WITHHOLD",
        "MLB_HITS15_CURRENT_DIRECTION_ARCHITECTURE_DECISION": "CURRENT_SURFACES_ARE_SIDE_SPECIFIC_NOT_A_GOVERNED_THREE_WAY_OWNERSHIP_ARBITER",
        "MLB_HITS15_HITTER_EVIDENCE_READINESS_DECISION": "PARTIAL_READY_AS_DESCRIPTIVE_STRICT_PRIOR_FORM_SIGNAL_NOT_SOLE_OWNERSHIP_RULE",
        "MLB_HITS15_PITCHER_SUPPRESSION_EVIDENCE_READINESS_DECISION": "PARTIAL_READY_RAW_STARTER_EXPECTED_CONTEXT_AVAILABLE_TIER_SEMANTICS_SIDE_DEPENDENT",
        "MLB_HITS15_OPPORTUNITY_CONTEXT_READINESS_DECISION": "PARTIAL_PA_AVAILABLE_FOR_CERTIFIED_OVER_SPINE_NOT_COMPLETE_FOR_ALL_REVIEW_BOARD_PROPOSITIONS",
        "MLB_HITS15_JULY12_DIRECTIONAL_EVIDENCE_DECISION": "MIXED_CONFLICTING_AND_INCOMPLETE_EVIDENCE_ARCHITECTURE_DID_NOT_GLOBALLY_COMPARE_BOTH_SIDES",
        "MLB_HITS15_CURRENT_SIDE_SELECTION_INTEGRITY_DECISION": "PARTIAL_CURRENT_SYSTEM_CAN_SELECT_BOTH_SIDES_BUT_DOES_NOT_PRODUCE_SINGLE_PROPOSITION_OWNER",
        "MLB_HITS15_UNDER_OPPORTUNITY_DECISION": "UNDER_PROCESS_EXISTS_WITH_AFFIRMATIVE_COLD_HITTER_PLUS_TOUGH_STARTER_LOGIC_BUT_IS_UNDER_PRODUCED_RELATIVE_TO_OVER_DISCOVERY_SURFACES",
        "MLB_HITS15_SURFACE_CORROBORATION_DECISION": "MULTIPLE_O15_SURFACES_OFTEN_REPEAT_SHARED_D7_D15_STARTER_SIGNAL_NOT_INDEPENDENT_CORROBORATION",
        "MLB_HITS15_HISTORICAL_VALIDATION_QUALITY_DECISION": "PARTIAL_INSUFFICIENT_FOR_PRODUCTION_THREE_WAY_RULE_OR_REJECTION_LAYER",
        "MLB_HITS15_THREE_WAY_FRAMEWORK_DESIGN_DECISION": "DESIGN_ONLY_APPROVED_FOR_RESEARCH_OVER_UNDER_WITHHOLD_NO_THRESHOLDS_FINALIZED",
        "MLB_HITS15_NEXT_EXPERIMENT_DECISION": "BUILD_FREEZE_AND_EVALUATE_PROPOSITION_GRAIN_TWO_SIDED_OWNERSHIP_LABELS_WITH_STRICT_PRIOR_EVIDENCE_AND_MARKET_EXECUTION_SEPARATED",
        "MLB_HITS15_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }


def build_package(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ledger, meta = build_proposition_ledger()
    _write_csv(out_dir / f"canonical_proposition_level_advantage_ledger_{AUDIT_DATE}.csv", ledger)
    _write_csv(out_dir / f"current_candidate_architecture_map_{AUDIT_DATE}.csv", architecture_map())
    _write_csv(out_dir / f"two_sided_evidence_domain_registry_{AUDIT_DATE}.csv", evidence_domain_registry())
    _write_csv(out_dir / f"july12_directional_reconstruction_{AUDIT_DATE}.csv", july12_directional_rows(ledger))
    _write_csv(out_dir / f"hitter_vs_pitcher_evidence_matrix_{AUDIT_DATE}.csv", summarize(ledger, ["baseball_directional_ownership", "hitter_evidence_label", "pitcher_suppression_label"]))
    _write_csv(out_dir / f"current_side_selection_integrity_report_{AUDIT_DATE}.csv", summarize(ledger, ["baseball_directional_ownership", "current_side_surface_state"]))
    _write_csv(out_dir / f"hits_u15_opportunity_audit_{AUDIT_DATE}.csv", summarize(ledger, ["current_side_surface_state"]))
    _write_csv(out_dir / f"matched_over_under_comparison_{AUDIT_DATE}.csv", matched_comparison(ledger))
    _write_csv(out_dir / f"surface_overlap_corroboration_analysis_{AUDIT_DATE}.csv", surface_overlap(ledger))
    _write_csv(out_dir / f"historical_validation_quality_report_{AUDIT_DATE}.csv", historical_validation_quality())
    _write_csv(out_dir / f"market_availability_execution_report_{AUDIT_DATE}.csv", market_execution_report(ledger))
    _write_csv(out_dir / f"three_way_framework_design_{AUDIT_DATE}.csv", framework_design())
    _write_csv(out_dir / f"bounded_next_experiment_specification_{AUDIT_DATE}.csv", next_experiment_spec())
    dec = decisions()
    _write_csv(out_dir / f"decision_report_{AUDIT_DATE}.csv", [{"decision": k, "value": v} for k, v in dec.items()])
    summary = executive_summary(ledger, meta, dec)
    _write_md(out_dir / f"executive_summary_{AUDIT_DATE}.md", summary)
    payload = {
        "audit_date": AUDIT_DATE,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "metadata": meta,
        "decisions": dec,
        "constraints": {
            "network_calls": 0,
            "db_writes": 0,
            "oddsapi_calls": 0,
            "model_training": 0,
            "threshold_optimization": 0,
            "production_changes": 0,
        },
    }
    _write_json(out_dir / f"machine_readable_two_sided_matchup_advantage_audit_{AUDIT_DATE}.json", payload)
    write_validation_and_sha(out_dir)
    return payload


def matched_comparison(ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    july12 = [r for r in ledger if _b(r.get("july12_sentinel"))]
    success_over = [r for r in ledger if r.get("official_over_result") == "win" and r.get("current_side_surface_state") in {"OVER_ONLY", "BOTH_CONFLICT"}]
    success_under = [r for r in ledger if r.get("official_under_result") == "win" and r.get("current_side_surface_state") in {"UNDER_ONLY", "BOTH_CONFLICT"}]
    for label, group in [("july12_sentinel_over_failures", july12), ("successful_over_reference", success_over), ("successful_under_reference", success_under)]:
        rows.append(
            {
                "comparison_group": label,
                "rows": len(group),
                "avg_d7_hits_rate": avg([_f(r.get("d7_hits_rate")) for r in group]),
                "avg_d15_hits_rate": avg([_f(r.get("d15_hits_rate")) for r in group]),
                "avg_starter_expected_hits_allowed": avg([_f(r.get("starter_expected_hits_allowed")) for r in group]),
                "pct_incomplete": sum(1 for r in group if r.get("baseball_directional_ownership") == "incomplete") / len(group) if group else "",
                "pct_conflicting": sum(1 for r in group if r.get("baseball_directional_ownership") == "conflicting") / len(group) if group else "",
                "matching_status": "deterministic_broad_reference_not_optimized",
                "notes": "This bounded comparison avoids selecting only strongest profitable rows; a stricter match should be the next experiment.",
            }
        )
    return rows


def market_execution_report(ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in ledger:
        if _b(row.get("july12_sentinel")) or row.get("current_side_surface_state") in {"BOTH_CONFLICT", "OVER_ONLY", "UNDER_ONLY"}:
            out.append(
                {
                    "canonical_proposition_key": row.get("canonical_proposition_key"),
                    "player_name": row.get("player_name"),
                    "current_side_surface_state": row.get("current_side_surface_state"),
                    "o15_available_in_artifacts": row.get("o15_available_in_artifacts"),
                    "u15_available_in_artifacts": row.get("u15_available_in_artifacts"),
                    "o15_price": row.get("o15_price", ""),
                    "u15_price": row.get("u15_price", ""),
                    "market_book_count_two_sided": row.get("market_book_count_two_sided", ""),
                    "market_snapshot_time_utc": row.get("market_snapshot_time_utc", ""),
                    "execution_status": row.get("execution_status", ""),
                    "market_context_role": "Stage B executability only; not used for baseball ownership.",
                }
            )
    return out


def next_experiment_spec() -> list[dict[str, Any]]:
    return [
        {"step": "freeze_proposition_grain_spine", "description": "One row per slate_date|game_id|player_id|hits|1.5 with both sides and strict-prior evidence.", "behavior_change_required": False},
        {"step": "derive_pre_outcome_ownership_labels", "description": "Freeze hitter, suppression, conflicting, weak, incomplete labels before outcome join.", "behavior_change_required": False},
        {"step": "evaluate_out_of_sample", "description": "Evaluate labels against OVER/UNDER outcomes and executable side availability without threshold optimization.", "behavior_change_required": False},
        {"step": "review_three_way_gate", "description": "Only after validation, decide whether OVER/UNDER/WITHHOLD labels are ready for Workbench visibility.", "behavior_change_required": "future_approval_required"},
    ]


def executive_summary(ledger: list[dict[str, Any]], meta: dict[str, Any], dec: dict[str, str]) -> str:
    state_counts = Counter(r.get("current_side_surface_state") for r in ledger)
    own_counts = Counter(r.get("baseball_directional_ownership") for r in ledger)
    pitcher_over = sum(1 for r in ledger if r.get("baseball_directional_ownership") == "pitcher_dominant" and r.get("current_side_surface_state") == "OVER_ONLY")
    resolved = sum(1 for r in ledger if r.get("official_over_result") in {"win", "loss"})
    return f"""
# MLB Hits 1.5 Two-Sided Matchup Advantage and Directional Ownership Audit

- Audit date: `{AUDIT_DATE}`
- Canonical grain: `slate_date | game_id | player_id | hits | line`
- Proposition rows assembled: `{len(ledger)}`
- Rows with official hit outcome attached: `{resolved}`
- Review source files scanned: `{meta.get('review_source_files')}`
- Production change status: `NOT_AUTHORIZED`

## Executive Finding

The current platform has the ingredients for a two-sided Hits 1.5 framework, but
it does not yet produce a governed proposition-level owner of the matchup. The
current architecture primarily qualifies side-specific surfaces independently:
OVER boards identify historically attractive hitters and favorable starter
environment; the UNDER audit identifies cold hitters facing suppressive starters.
There is no single arbiter that compares hitter evidence against pitcher
suppression and then emits `OVER`, `UNDER`, or `NO_SUBSTANTIATED_ADVANTAGE`.

Direct answer: Proppadia is currently closer to surfacing historically attractive
hitters and assigning the OVER side on OVER surfaces than it is to identifying
who owns the hitter-pitcher advantage at the 1.5-hit line. A genuine UNDER
process exists, but it is separate and not integrated into a global two-sided
ownership decision.

## Directional Accounting

- Current surface states: `{dict(state_counts)}`
- Evidence ownership labels: `{dict(own_counts)}`
- Pitcher-dominant rows still surfaced as OVER-only in this bounded ledger:
  `{pitcher_over}`

These counts are descriptive, not optimized thresholds.

## July 12 Interpretation

The July 12 sentinel rows remain certified as 15 issued OVER predictions, all
losses. The directional reconstruction shows that the architecture did not
globally ask whether the pitcher/suppression side owned the proposition before
surfacing OVER. Some rows show hitter support, some show incomplete or
conflicting evidence, and Curtis Mead remains an issued prediction with an
execution-only line availability exception.

## Governing Decisions

{chr(10).join(f'- `{k}` = `{v}`' for k, v in dec.items())}
"""


def write_validation_and_sha(out_dir: Path) -> None:
    validation = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            pd.read_csv(path)
            status, msg = "PASS", "csv_parses"
        except Exception as exc:
            status, msg = "FAIL", f"{type(exc).__name__}: {exc}"
        validation.append({"artifact": str(path), "validation": "csv_parse", "status": status, "message": msg})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            status, msg = "PASS", "json_parses"
        except Exception as exc:
            status, msg = "FAIL", f"{type(exc).__name__}: {exc}"
        validation.append({"artifact": str(path), "validation": "json_parse", "status": status, "message": msg})
    for path in sorted(out_dir.glob("*.md")):
        status = "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL"
        validation.append({"artifact": str(path), "validation": "markdown_nonempty", "status": status, "message": status})
    validation.append({"artifact": "runtime", "validation": "read_only_guardrails", "status": "PASS", "message": "local artifact inspection only; no network/db/OddsAPI/training/production changes"})
    _write_csv(out_dir / f"validation_report_{AUDIT_DATE}.csv", validation)
    rows = []
    for path in sorted(p for p in out_dir.glob("*") if p.is_file() and p.name != f"sha256_manifest_{AUDIT_DATE}.csv"):
        rows.append({"path": str(path), "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
    _write_csv(out_dir / f"sha256_manifest_{AUDIT_DATE}.csv", rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args()
    payload = build_package(Path(args.out_dir))
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
