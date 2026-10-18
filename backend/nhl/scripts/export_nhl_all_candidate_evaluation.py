#!/usr/bin/env python3
"""Build a conservative, reproducible NHL candidate evaluation package.

This exporter reads retained reconciliation CSVs only. It does not access a
database, request odds, or modify operational artifacts. Quotes are emitted
only when the source row proves both observation and quote timestamps precede
the prediction timestamp and scheduled start.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


PREDICTION_FIELDS = [
    "canonical_season", "slate_date", "game_type", "canonical_phase",
    "run_id", "run_type", "scheduled_start_utc", "prediction_timestamp_utc",
    "feature_cutoff_utc", "prediction_identity", "model_role", "model_family",
    "model_version", "feature_contract_sha256", "model_identity_sha256",
    "prediction_artifact_sha256", "odds_manifest_sha256", "market_key",
    "market_line", "selected_side", "probability_over", "probability_under",
    "expected_count", "candidate_policy_status", "game_id", "player_id",
    "goalie_id", "team_id", "opponent_id", "display_name", "outcome_status",
    "outcome_value", "outcome_side", "outcome_source", "outcome_source_timestamp_utc",
    "source_path", "source_sha256", "parent_run_id", "candidate_key",
]

QUOTE_FIELDS = [
    "candidate_key", "canonical_season", "slate_date", "game_id", "player_id",
    "goalie_id", "market_key", "market_line", "selected_side", "model_role",
    "model_family", "model_version", "sportsbook_key", "provider_event_id",
    "provider_market_key", "provider_outcome", "american_price", "decimal_price",
    "source_quote_timestamp_utc", "observation_timestamp_utc", "source_capture_identity", "quote_qualification_status",
    "implied_probability", "expected_value_per_unit", "model_market_edge",
    "ev_status", "edge_status", "source_path", "source_sha256", "odds_manifest_sha256", "parent_run_id", "source_parent_run_id",
]

QUOTE_DIAGNOSTIC_FIELDS = [
    "candidate_key", "market_key", "model_role", "model_family", "model_version",
    "odds_status", "reason", "source_path", "source_sha256",
]


def parse_utc(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def quote_is_eligible(quote: dict[str, Any], prediction: dict[str, Any]) -> bool:
    source_time = parse_utc(quote.get("source_quote_timestamp_utc"))
    observed_time = parse_utc(quote.get("observation_timestamp_utc"))
    prediction_time = parse_utc(prediction.get("prediction_timestamp_utc"))
    start_time = parse_utc(prediction.get("scheduled_start_utc"))
    return bool(
        source_time and observed_time and prediction_time and start_time
        and source_time <= observed_time <= prediction_time < start_time
    )


def american_to_decimal(price: Any) -> float | None:
    try:
        value = float(price)
    except (TypeError, ValueError):
        return None
    if value == 0:
        return None
    return 1 + value / 100 if value > 0 else 1 + 100 / abs(value)


def evaluate_quote(quote: dict[str, Any], probability: Any) -> dict[str, Any]:
    result = dict(quote)
    decimal = result.get("decimal_price")
    if decimal in (None, ""):
        decimal = american_to_decimal(result.get("american_price"))
    try:
        decimal_value = float(decimal)
        prob_value = float(probability)
    except (TypeError, ValueError):
        decimal_value = prob_value = 0.0
        valid = False
    else:
        valid = decimal_value > 1 and 0 <= prob_value <= 1
    result["decimal_price"] = decimal_value if valid else ""
    result["expected_value_per_unit"] = prob_value * decimal_value - 1 if valid else ""
    implied = result.get("implied_probability")
    try:
        implied_value = float(implied)
    except (TypeError, ValueError):
        implied_value = None
    result["model_market_edge"] = prob_value - implied_value if valid and implied_value is not None and 0 <= implied_value <= 1 else ""
    result["ev_status"] = "AVAILABLE" if valid else "UNAVAILABLE_MISSING_OR_INVALID_PRICE_OR_PROBABILITY"
    result["edge_status"] = "AVAILABLE" if result["model_market_edge"] != "" else "UNAVAILABLE_NO_VALID_MARKET_PROBABILITY"
    return result


def candidate_key(row: dict[str, Any]) -> str:
    fields = ("canonical_season", "slate_date", "game_id", "player_id", "goalie_id",
              "market_key", "market_line", "selected_side", "model_family", "model_version",
              "model_role", "prediction_identity")
    return "|".join(str(row.get(field) or "") for field in fields)


def retained_row_identity(row: dict[str, Any]) -> str:
    payload = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "retained-row-sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def deduplicate_candidates(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep one natural candidate per model/market; reject conflicting duplicates."""
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = candidate_key(row)
        if key in result:
            if row != result[key]:
                raise ValueError(f"conflicting duplicate candidate key: {key}")
            continue
        result[key] = row
    return [result[key] for key in sorted(result)]


def missing_required_arms(rows: Iterable[dict[str, Any]], required: set[tuple[str, str]]) -> set[tuple[str, str]]:
    present = {(str(r.get("market_key", "")), str(r.get("model_version", ""))) for r in rows}
    return required - present


_SHA_CACHE: dict[Path, str] = {}


def _sha(path: Path) -> str:
    path = path.resolve()
    if path not in _SHA_CACHE:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        _SHA_CACHE[path] = digest.hexdigest()
    return _SHA_CACHE[path]


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def discover_latest_reconciliations(root: Path) -> list[Path]:
    """Choose latest immutable reconciliation package for each 2026 slate."""
    packages: dict[str, Path] = {}
    for slate_dir in sorted(root.glob("postgame_reconciliation/2026-*")):
        candidates = sorted(slate_dir.glob("reconciliation=*/"), key=lambda p: p.stat().st_mtime_ns)
        if candidates:
            packages[slate_dir.name] = candidates[-1]
    return [packages[key] for key in sorted(packages)]


def _base(package: Path, row: dict[str, str], market: str, family: str,
          version: str, role: str, source: Path) -> dict[str, Any]:
    return {
        "canonical_season": row.get("canonical_season") or 2026,
        "slate_date": row.get("slate_date") or package.parent.name,
        "game_type": row.get("canonical_game_type_code") or row.get("game_type_code") or "",
        "canonical_phase": row.get("canonical_phase") or row.get("phase") or "",
        "run_id": row.get("run_id") or "",
        "run_type": "RECONCILED_RETAINED_CAPTURE",
        "scheduled_start_utc": row.get("canonical_scheduled_start_time_utc") or row.get("scheduled_start_time_utc") or "",
        "prediction_timestamp_utc": row.get("prediction_timestamp_utc") or row.get("prediction_creation_time_utc") or "",
        "feature_cutoff_utc": row.get("feature_cutoff_utc") or row.get("input_cutoff_utc") or row.get("input_cutoff_timestamp_utc") or "",
        "prediction_identity": row.get("prediction_identity") or row.get("substantive_prediction_sha256") or retained_row_identity(row),
        "model_role": role,
        "model_family": family,
        "model_version": version,
        "feature_contract_sha256": row.get("contract_sha256") or row.get("feature_policy_sha256") or "",
        "model_identity_sha256": row.get("model_parameter_sha256") or row.get("substantive_prediction_sha256") or "",
        "prediction_artifact_sha256": "",
        "odds_manifest_sha256": "",
        "market_key": market,
        "market_line": row.get("line") or "",
        "candidate_policy_status": "NOT_APPLIED",
        "game_id": row.get("game_id") or "",
        "player_id": row.get("player_id") or "",
        "goalie_id": row.get("goalie_id") or "",
        "team_id": row.get("team_id") or "",
        "opponent_id": row.get("opponent_id") or "",
        "display_name": row.get("player_name") or row.get("goalie_name") or row.get("home_team") or "",
        "outcome_status": "UNRESOLVED",
        "outcome_value": "",
        "outcome_side": "",
        "outcome_source": row.get("outcome_source") or "",
        "outcome_source_timestamp_utc": row.get("outcome_source_timestamp_utc") or "",
        "source_path": str(source),
        "source_sha256": _sha(source),
        "parent_run_id": row.get("run_id") or "",
        "model_role_tmp": role,
    }


def _emit(rows: list[dict[str, Any]], base: dict[str, Any], p_over: Any,
          p_under: Any, side: str, expected: Any = "", outcome: Any = "",
          outcome_source: str = "") -> None:
    base["probability_over"] = p_over
    base["probability_under"] = p_under
    base["selected_side"] = side
    base["expected_count"] = expected
    if outcome not in (None, ""):
        base["outcome_status"] = "SETTLED"
        base["outcome_value"] = outcome
        base["outcome_side"] = str(outcome)
        base["outcome_source"] = outcome_source or base["outcome_source"]
    base.pop("model_role_tmp", None)
    base["candidate_key"] = candidate_key(base)
    rows.append({field: base.get(field, "") for field in PREDICTION_FIELDS})


def load_candidates(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    predictions: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for package in discover_latest_reconciliations(root):
        for name in ("graded_moneyline.csv", "graded_puck_line.csv", "graded_sog.csv", "graded_saves.csv"):
            path = package / name
            if not path.exists():
                continue
            for source_row in _csv_rows(path):
                r = source_row
                if name == "graded_moneyline.csv":
                    for family, version, role, prob_key in (
                        ("moneyline", "NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3", "PRODUCTION", "challenger_home_win_probability"),
                        ("moneyline", "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2", "SHADOW", "v2_home_win_probability"),
                    ):
                        p = _num(r.get(prob_key))
                        winner = r.get("official_full_game_winner")
                        b = _base(package, r, "moneyline_home", family, version, role, path)
                        _emit(predictions, b, p, 1-p if p is not None else "", "HOME" if p is not None and p >= .5 else "AWAY", outcome=winner, outcome_source=r.get("outcome_source", "official_game_outcome"))
                elif name == "graded_puck_line.csv":
                    for version, role, prefix in (("NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1", "PRODUCTION", "control"), ("NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2", "CHALLENGER", "substantive")):
                        # The retained reconciled table has the complete reference/control
                        # probability distribution. Challenger probabilities are emitted
                        # only when explicit fields are present in the source row.
                        keys = (f"{prefix}_away_by_2_plus_probability", f"{prefix}_one_goal_game_probability", f"{prefix}_home_by_2_plus_probability")
                        if not all(k in r for k in keys):
                            if prefix == "control":
                                keys = ("away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability")
                            else:
                                continue
                        probs = [_num(r.get(k)) for k in keys]
                        if any(v is None for v in probs):
                            continue
                        outcome = r.get("actual_margin_class")
                        b = _base(package, r, "puck_line_full_game", "puck_line", version, role, path)
                        _emit(predictions, b, probs[2], probs[0], r.get("predicted_margin_class") or "MULTICLASS", outcome=outcome, outcome_source="official_final_score")
                elif name == "graded_sog.csv":
                    if r.get("arm_status") == "UNQUALIFIED_SHADOW":
                        continue
                    p = _num(r.get("p_over")); expected = r.get("expected_sog")
                    role = r.get("evaluation_lane") or "PRODUCTION"
                    family = r.get("model_family") or "poisson"
                    version = r.get("model_version") or "baseline_v1"
                    if r.get("contract_arm"):
                        version = f"{version}:{r['contract_arm']}"
                    b = _base(package, r, "shots_on_goal", family, version, role, path)
                    official = r.get("official_sog") if r.get("grading_state") in ("SETTLED", "GRADED") or r.get("official_sog") else ""
                    _emit(predictions, b, p, _num(r.get("p_under")), r.get("selected_side") or "", expected, official, r.get("outcome_source", "official_skater_logs"))
                    if r.get("bookmaker") and r.get("price"):
                        diagnostics.append({"candidate_key": candidate_key({**b, "market_line": r.get("line"), "selected_side": r.get("selected_side")}), "market_key": "shots_on_goal", "model_role": role, "model_family": family, "model_version": version, "odds_status": "UNQUALIFIED_TIMESTAMP_MISSING", "reason": "retained reconciliation attachment has no source quote or observation timestamp", "source_path": str(path), "source_sha256": _sha(path)})
                else:
                    p = _num(r.get("prob_over")); expected = r.get("expected_saves")
                    role = "PRODUCTION"; family = r.get("model") or "phoenix"; version = r.get("model_version") or "phoenix_v2"
                    b = _base(package, r, "goalie_saves", family, version, role, path)
                    _emit(predictions, b, p, _num(r.get("prob_under")), r.get("model_side") or "", expected, r.get("official_saves"), "official_goalie_logs")
                    if r.get("price"):
                        diagnostics.append({"candidate_key": candidate_key({**b, "market_line": r.get("line"), "selected_side": r.get("model_side")}), "market_key": "goalie_saves", "model_role": role, "model_family": family, "model_version": version, "odds_status": "UNQUALIFIED_TIMESTAMP_MISSING", "reason": "retained reconciliation attachment has no source quote or observation timestamp", "source_path": str(path), "source_sha256": _sha(path)})
    # Points has a separate append-only HGB/Phoenix grading package; bind those
    # exact capture rows to its canonical outcome file, retaining unresolved rows.
    point_grades = root / "points_hgb_shadow" / "grades"
    latest_grade: dict[str, Path] = {}
    if point_grades.exists():
        for grade_path in point_grades.glob("season=2026/slate_date=*/capture=*/grade=*/grade.json"):
            slate = grade_path.parents[2].name.split("=", 1)[1]
            current = latest_grade.get(slate)
            if current is None or grade_path.stat().st_mtime_ns > current.stat().st_mtime_ns:
                latest_grade[slate] = grade_path
    for slate, grade_path in sorted(latest_grade.items()):
        grade = json.loads(grade_path.read_text(encoding="utf-8"))
        capture = Path(grade["capture_path"])
        outcome_path = Path(grade["official_outcome_path"]) if grade.get("official_outcome_path") else None
        outcomes = {(r.get("game_id"), r.get("player_id")): r for r in _csv_rows(outcome_path)} if outcome_path and outcome_path.exists() else {}
        # HGB production has one wide row per player/game and three line probabilities.
        hgb_path = Path(grade["production_prediction_path"]) if grade.get("production_prediction_path") else None
        if hgb_path and hgb_path.exists():
            hgb_sha = _sha(hgb_path)
            for r in _csv_rows(hgb_path):
                hgb_lines = [(r.get("line"), _num(r.get("prob_over")))] if r.get("prob_over") not in (None, "") else [(line, _num(r.get(f"prob_over_{line.replace('.', '_')}"))) for line in ("0.5", "1.5", "2.5")]
                for line, p in hgb_lines:
                    if p is None:
                        continue
                    outcome = outcomes.get((r.get("game_id"), r.get("player_id")), {})
                    points = _num(outcome.get("official_points"))
                    b = {
                        "canonical_season": 2026, "slate_date": slate, "game_type": "", "canonical_phase": "REGULAR_SEASON",
                        "run_id": r.get("parent_daily_run_id") or "", "run_type": "FINAL_PREGAME_CAPTURE",
                        "scheduled_start_utc": r.get("game_start_utc") or "", "prediction_timestamp_utc": r.get("feature_input_cutoff_utc") or "",
                        "feature_cutoff_utc": r.get("feature_input_cutoff_utc") or "", "prediction_identity": retained_row_identity(r),
                        "model_role": "PRODUCTION", "model_family": "hist_gradient_boosting", "model_version": "NHL_POINTS_COUNT_HGB_V1",
                        "feature_contract_sha256": grade.get("feature_contract_identity", {}).get("feature_contract_sha256", ""),
                        "model_identity_sha256": grade.get("model_identity", {}).get("model_identity_sha256", ""),
                        "prediction_artifact_sha256": grade.get("production_prediction_sha256", hgb_sha), "odds_manifest_sha256": "",
                        "market_key": "player_points", "market_line": line, "selected_side": "OVER" if p >= .5 else "UNDER",
                        "probability_over": p, "probability_under": 1-p, "expected_count": r.get("expected_points"), "candidate_policy_status": "NOT_APPLIED",
                        "game_id": r.get("game_id"), "player_id": r.get("player_id"), "goalie_id": "", "team_id": "", "opponent_id": "",
                        "display_name": "", "outcome_status": "SETTLED" if points is not None else "UNRESOLVED",
                        "outcome_value": points if points is not None else "", "outcome_side": ("OVER" if points > float(line) else "UNDER") if points is not None else "",
                        "outcome_source": outcome.get("outcome_source", ""), "outcome_source_timestamp_utc": outcome.get("outcome_source_timestamp_utc", ""),
                        "source_path": str(hgb_path), "source_sha256": hgb_sha, "parent_run_id": r.get("parent_daily_run_id") or "",
                    }
                    b["candidate_key"] = candidate_key(b)
                    predictions.append({field: b.get(field, "") for field in PREDICTION_FIELDS})
        # Phoenix is a same-capture shadow; use only the exact retained output.
        phoenix_path = capture / "phoenix_control_predictions.csv"
        if phoenix_path.exists():
            phoenix_sha = _sha(phoenix_path)
            for r in _csv_rows(phoenix_path):
                line = r.get("line") or ""
                p = _num(r.get("prob_over"))
                if p is None:
                    continue
                outcome = outcomes.get((r.get("game_id"), r.get("player_id")), {})
                points = _num(outcome.get("official_points"))
                b = {
                    "canonical_season": 2026, "slate_date": slate, "game_type": "", "canonical_phase": "REGULAR_SEASON",
                    "run_id": r.get("parent_daily_run_id") or grade.get("capture_path", "").split("/run_id=")[-1].split("/")[0], "run_type": "FINAL_PREGAME_CAPTURE",
                    "scheduled_start_utc": r.get("game_start_utc") or "", "prediction_timestamp_utc": r.get("feature_input_cutoff_utc") or "",
                    "feature_cutoff_utc": r.get("feature_input_cutoff_utc") or "", "prediction_identity": retained_row_identity(r),
                    "model_role": "SHADOW", "model_family": "phoenix", "model_version": "phoenix_v2",
                    "feature_contract_sha256": "", "model_identity_sha256": grade.get("phoenix_prediction_sha256", ""),
                    "prediction_artifact_sha256": grade.get("phoenix_prediction_sha256", phoenix_sha), "odds_manifest_sha256": "",
                    "market_key": "player_points", "market_line": line, "selected_side": "OVER" if p >= .5 else "UNDER",
                    "probability_over": p, "probability_under": 1-p, "expected_count": "", "candidate_policy_status": "NOT_APPLIED",
                    "game_id": r.get("game_id"), "player_id": r.get("player_id"), "goalie_id": "", "team_id": "", "opponent_id": "",
                    "display_name": "", "outcome_status": "SETTLED" if points is not None else "UNRESOLVED",
                    "outcome_value": points if points is not None else "", "outcome_side": ("OVER" if points > float(line) else "UNDER") if points is not None else "",
                    "outcome_source": outcome.get("outcome_source", ""), "outcome_source_timestamp_utc": outcome.get("outcome_source_timestamp_utc", ""),
                    "source_path": str(phoenix_path), "source_sha256": phoenix_sha, "parent_run_id": r.get("parent_daily_run_id") or "",
                }
                b["candidate_key"] = candidate_key(b)
                predictions.append({field: b.get(field, "") for field in PREDICTION_FIELDS})
    return deduplicate_candidates(predictions), diagnostics


def _prediction_row(base: dict[str, Any], p_over: float | None, p_under: float | None,
                    selected_side: str, outcome_row: dict[str, str] | None = None) -> dict[str, Any]:
    outcome_row = outcome_row or {}
    observed = _num(outcome_row.get("official_sog"))
    line = _num(base.get("market_line"))
    base.update({
        "probability_over": p_over if p_over is not None else "",
        "probability_under": p_under if p_under is not None else "",
        "selected_side": selected_side,
        "outcome_status": "SETTLED" if observed is not None else "UNRESOLVED",
        "outcome_value": observed if observed is not None else "",
        "outcome_side": ("OVER" if observed > line else "UNDER") if observed is not None and line is not None else "",
        "outcome_source": outcome_row.get("outcome_source", ""),
        "outcome_source_timestamp_utc": outcome_row.get("outcome_source_timestamp_utc", ""),
    })
    base["candidate_key"] = candidate_key(base)
    return {field: base.get(field, "") for field in PREDICTION_FIELDS}


def add_captured_sog_arms(rows: list[dict[str, Any]], operational_root: Path,
                          slate_date: str) -> list[dict[str, Any]]:
    """Bind immutable production and fixed-blend captures to official SOG outcomes."""
    repo = Path.cwd()
    outcomes: dict[tuple[str, str], dict[str, str]] = {}
    reconciliation_root = operational_root / "postgame_reconciliation" / slate_date
    for package in sorted(reconciliation_root.glob("reconciliation=*/")):
        outcome_path = package / "graded_sog.csv"
        if outcome_path.exists():
            for r in _csv_rows(outcome_path):
                outcomes[(r.get("game_id", ""), r.get("player_id", ""))] = r
    blend_root = operational_root / "sog_fixed_blend_shadows" / "season=2026" / f"slate_date={slate_date}"
    manifests = list(blend_root.glob("run_id=*/model=NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1/manifest.json"))
    if not manifests:
        return rows
    manifest_path = max(manifests, key=lambda p: p.stat().st_mtime_ns)
    run_id = manifest_path.parents[1].name.split("=", 1)[1]
    run_root = manifest_path.parents[1]
    cutoff = json.loads(manifest_path.read_text(encoding="utf-8")).get("feature_cutoff_utc", "")
    production_path = repo / "backend/nhl/data/processed/daily_runs" / run_id / "sog_predictions_wide_calibrated.csv"
    for model, path, role, blend_version in (
        ("PRODUCTION", production_path, "PRODUCTION", ""),
        ("NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1", run_root / "model=NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1/predictions.csv", "SHADOW", ""),
        ("NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1", run_root / "model=NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1/predictions.csv", "SHADOW", ""),
    ):
        if not path.exists():
            continue
        sha = _sha(path)
        for r in _csv_rows(path):
            game_id, player_id = r.get("game_id", ""), r.get("player_id", "")
            outcome = outcomes.get((game_id, player_id), {})
            if model == "PRODUCTION":
                family, version = "poisson_baseline", "baseline_v1"
                line_probs = [("1.5", _num(r.get("p_over_1_5"))), ("2.5", _num(r.get("p_over_2_5"))), ("3.5", _num(r.get("p_over_3_5")))]
            else:
                family, version = "poisson_baseline", model
                line_probs = [(r.get("line", ""), _num(r.get("p_over")))]
            for line, p in line_probs:
                if p is None or not line:
                    continue
                b = {
                    "canonical_season": 2026, "slate_date": slate_date, "game_type": "", "canonical_phase": "REGULAR_SEASON",
                    "run_id": run_id, "run_type": "FINAL_PREGAME_CAPTURE", "scheduled_start_utc": outcome.get("scheduled_start_time_utc", ""),
                    "prediction_timestamp_utc": cutoff, "feature_cutoff_utc": cutoff, "prediction_identity": f"{game_id}:{player_id}:{line}:{sha}",
                    "model_role": role, "model_family": family, "model_version": version,
                    "feature_contract_sha256": "", "model_identity_sha256": "", "prediction_artifact_sha256": sha, "odds_manifest_sha256": "",
                    "market_key": "shots_on_goal", "market_line": line, "selected_side": "OVER" if p >= .5 else "UNDER",
                    "probability_over": p, "probability_under": 1-p, "expected_count": r.get("expected_sog") or r.get("shadow_lambda", ""),
                    "candidate_policy_status": "NOT_APPLIED", "game_id": game_id, "player_id": player_id, "goalie_id": "",
                    "team_id": r.get("team_id", ""), "opponent_id": r.get("opponent_id", ""), "display_name": outcome.get("player_name", ""),
                    "outcome_status": "SETTLED" if _num(outcome.get("official_sog")) is not None else "UNRESOLVED",
                    "outcome_value": outcome.get("official_sog", ""), "outcome_side": ("OVER" if _num(outcome.get("official_sog")) > float(line) else "UNDER") if _num(outcome.get("official_sog")) is not None else "",
                    "outcome_source": outcome.get("outcome_source", ""), "outcome_source_timestamp_utc": outcome.get("outcome_source_timestamp_utc", ""),
                    "source_path": str(path), "source_sha256": sha, "parent_run_id": run_id,
                }
                b["candidate_key"] = candidate_key(b)
                rows.append({field: b.get(field, "") for field in PREDICTION_FIELDS})
    return deduplicate_candidates(rows)


def add_puck_line_challenger(rows: list[dict[str, Any]], operational_root: Path,
                             slate_date: str) -> list[dict[str, Any]]:
    captures = operational_root / "cross_market_shadow" / "season=2026" / f"slate_date={slate_date}" / "run_type=FINAL_PREGAME"
    capture_dirs = sorted(captures.glob("state=*"))
    if not capture_dirs:
        return rows
    capture = capture_dirs[-1]
    path = capture / "puck_line_v2_shot_prior_challenger_predictions.csv"
    if not path.exists():
        return rows
    # Bind actual margins from the same retained reconciliation package used by
    # the incumbent control arm.
    outcomes: dict[str, str] = {}
    recon = operational_root / "postgame_reconciliation" / slate_date
    packages = sorted(recon.glob("reconciliation=*/"))
    if packages and (packages[-1] / "graded_puck_line.csv").exists():
        outcomes = {r.get("game_id", ""): r.get("actual_margin_class", "") for r in _csv_rows(packages[-1] / "graded_puck_line.csv")}
    sha = _sha(path)
    run_id = f"cross_market_state_{capture.name.split('=', 1)[-1]}"
    for r in _csv_rows(path):
        if str(r.get("regular_season_evaluation_eligible", "")).lower() != "true":
            continue
        probs = [_num(r.get(k)) for k in ("away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability")]
        if any(p is None for p in probs): continue
        actual = outcomes.get(r.get("game_id", ""), "")
        b = {
            "canonical_season": 2026, "slate_date": slate_date, "game_type": "", "canonical_phase": "REGULAR_SEASON",
            "run_id": run_id, "run_type": "FINAL_PREGAME_CAPTURE", "scheduled_start_utc": r.get("scheduled_start_time_utc", ""),
            "prediction_timestamp_utc": r.get("prediction_creation_time_utc", ""), "feature_cutoff_utc": "", "prediction_identity": r.get("substantive_prediction_sha256", ""),
            "model_role": "CHALLENGER", "model_family": "puck_line", "model_version": "NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2",
            "feature_contract_sha256": r.get("feature_policy_version", ""), "model_identity_sha256": r.get("control_model_sha256", ""),
            "prediction_artifact_sha256": sha, "odds_manifest_sha256": "", "market_key": "puck_line_full_game", "market_line": "-1.5/+1.5",
            "selected_side": r.get("wager_recommendation") or "MULTICLASS", "probability_over": probs[2], "probability_under": probs[0],
            "expected_count": "", "candidate_policy_status": "NOT_APPLIED", "game_id": r.get("game_id", ""), "player_id": "", "goalie_id": "",
            "team_id": r.get("home_team_id", ""), "opponent_id": r.get("away_team_id", ""), "display_name": r.get("home_team", ""),
            "outcome_status": "SETTLED" if actual else "UNRESOLVED", "outcome_value": actual, "outcome_side": actual,
            "outcome_source": "official_final_score" if actual else "", "outcome_source_timestamp_utc": "", "source_path": str(path), "source_sha256": sha,
            "parent_run_id": r.get("parent_daily_run_id", ""),
        }
        b["candidate_key"] = candidate_key(b)
        rows.append({field: b.get(field, "") for field in PREDICTION_FIELDS})
    return deduplicate_candidates(rows)


def load_retained_quotes(operational_root: Path, candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Bind normalized retained quotes; retain only each book's latest eligible quote."""
    by_game_player: dict[tuple[str, str], list[dict[str, Any]]] = {}
    by_game_goalie: dict[tuple[str, str], list[dict[str, Any]]] = {}
    by_game: dict[str, list[dict[str, Any]]] = {}
    for c in candidates:
        by_game_player.setdefault((str(c.get("game_id")), str(c.get("player_id"))), []).append(c)
        if c.get("goalie_id"):
            by_game_goalie.setdefault((str(c.get("game_id")), str(c.get("goalie_id"))), []).append(c)
        by_game.setdefault(str(c.get("game_id")), []).append(c)
    names_path_root = Path.cwd() / "backend/nhl/exports/daily/names"
    output: dict[tuple[str, str], dict[str, Any]] = {}
    diagnostics: list[dict[str, Any]] = []
    odds_root = operational_root / "odds_observations" / "season=2026"
    for obs_dir in sorted(odds_root.glob("slate_date=*/observation=*")):
        summary_path = obs_dir / "observation_summary.json"
        normalized_path = obs_dir / "normalized_odds.jsonl"
        binding_path = obs_dir / "game_binding.jsonl"
        if not all(p.exists() for p in (summary_path, normalized_path, binding_path)):
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        observed = summary.get("observation_timestamp_utc", "")
        if summary.get("phase") not in ("EARLY", "FINAL_PREGAME"):
            continue
        bound_events = {}
        for line in binding_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                binding = json.loads(line)
                if binding.get("binding_status") == "MATCHED":
                    bound_events[binding.get("provider_event_id")] = str(binding.get("canonical_game_id"))
        slate = obs_dir.parent.name.split("=", 1)[1]
        names = {}
        names_path = names_path_root / f"names_{slate}.csv"
        if names_path.exists():
            for r in _csv_rows(names_path):
                for name_col in ("full_name", "full_name_canonical"):
                    if r.get(name_col): names[(str(r.get("game_id")), r[name_col].strip().casefold())] = str(r.get("player_id"))
        obs_sha = _sha(normalized_path)
        manifest_path = obs_dir / "SHA256SUMS"
        odds_manifest_sha = _sha(manifest_path) if manifest_path.exists() else ""
        source_parent_run_id = summary.get("parent_daily_run_id", "")
        source_capture_identity = summary.get("invocation_id", obs_dir.name)
        for line in normalized_path.read_text(encoding="utf-8").splitlines():
            if not line.strip(): continue
            q = json.loads(line)
            game_id = bound_events.get(q.get("provider_event_id"))
            if not game_id: continue
            player_id = names.get((game_id, str(q.get("description") or "").strip().casefold()), "")
            market = {"player_shots_on_goal": "shots_on_goal", "player_shots_on_goal_alternate": "shots_on_goal", "player_points": "player_points", "player_points_alternate": "player_points", "player_total_saves": "goalie_saves", "player_total_saves_alternate": "goalie_saves", "h2h": "moneyline_home"}.get(q.get("market"))
            if not market: continue
            line_val = q.get("point")
            if market == "moneyline_home": line_val = ""
            try: price = int(q.get("price"))
            except (TypeError, ValueError): continue
            decimal = american_to_decimal(price)
            source_time = q.get("market_last_update", "")
            if market == "moneyline_home":
                book_candidates = [c for c in by_game.get(game_id, []) if c.get("market_key") == market]
                for c in book_candidates:
                    outcome_name = str(q.get("outcome") or "").strip().casefold()
                    expected_name = str(q.get("home_team") if c.get("selected_side") == "HOME" else q.get("away_team") or "").strip().casefold()
                    if outcome_name != expected_name: continue
                    quote_player = ""
                    implied = _american_implied(price)
                    candidate = c
                    _consider_quote(output, diagnostics, candidate, q, obs_dir, obs_sha, odds_manifest_sha, observed, source_time, price, decimal, implied, quote_player)
                continue
            if not player_id: continue
            matching_candidates = (by_game_goalie.get((game_id, player_id), []) if market == "goalie_saves"
                                   else by_game_player.get((game_id, player_id), []))
            for c in matching_candidates:
                if c.get("market_key") != market or str(c.get("market_line")) != str(line_val): continue
                side = str(q.get("outcome", "")).upper()
                if side != c.get("selected_side"): continue
                _consider_quote(output, diagnostics, c, q, obs_dir, obs_sha, odds_manifest_sha, observed, source_time, price, decimal, _american_implied(price), player_id)
                key = (c["candidate_key"], str(q.get("bookmaker", "")))
                if key in output:
                    output[key]["source_parent_run_id"] = source_parent_run_id
                    output[key]["source_capture_identity"] = source_capture_identity
    # Moneyline's audited ledger binds its no-vig price, exact model capture,
    # bookmaker, source update and observation time. Use that stronger lineage
    # when it exists; it supersedes the older normalized snapshots per book.
    for slate_dir in sorted((operational_root / "cross_market_shadow" / "season=2026").glob("slate_date=*/run_type=FINAL_PREGAME/state=*")):
        ledger = slate_dir / "moneyline_shot_finishing_challenger_v3_ledger.csv"
        if not ledger.exists():
            continue
        sha = _sha(ledger)
        completion = slate_dir / "RUN_COMPLETE.json"
        manifest_sha = _sha(completion) if completion.exists() else ""
        for qrow in _csv_rows(ledger):
            game_id = str(qrow.get("game_id", ""))
            if qrow.get("qualification_status") != "PREGAME_QUALIFIED":
                continue
            side = str(qrow.get("side_orientation", ""))
            for candidate in by_game.get(game_id, []):
                if candidate.get("market_key") != "moneyline_home" or candidate.get("selected_side") != side:
                    continue
                source_time = qrow.get("source_update_timestamp_utc", "")
                observed = qrow.get("market_observation_timestamp_utc", "")
                price = _num(qrow.get("american_price"))
                decimal = _num(qrow.get("decimal_price"))
                implied = _num(qrow.get("no_vig_probability"))
                if price is None or decimal is None or implied is None:
                    continue
                synthetic = {
                    "bookmaker": qrow.get("sportsbook_key", ""), "market": "h2h",
                    "outcome": side, "provider_event_id": "",
                }
                if not quote_is_eligible({"source_quote_timestamp_utc": source_time, "observation_timestamp_utc": observed}, candidate):
                    continue
                row = {
                    "candidate_key": candidate["candidate_key"], "canonical_season": candidate["canonical_season"], "slate_date": candidate["slate_date"],
                    "game_id": game_id, "player_id": "", "goalie_id": "", "market_key": "moneyline_home", "market_line": "",
                    "selected_side": side, "model_role": candidate["model_role"], "model_family": candidate["model_family"], "model_version": candidate["model_version"],
                    "sportsbook_key": synthetic["bookmaker"], "provider_event_id": "", "provider_market_key": "h2h", "provider_outcome": side,
                    "american_price": price, "decimal_price": decimal, "source_quote_timestamp_utc": source_time,
                    "observation_timestamp_utc": observed, "source_capture_identity": f"cross_market_state_{slate_dir.name.split('=', 1)[-1]}",
                    "quote_qualification_status": "QUALIFIED_AUDITED_MONEYLINE_LEDGER",
                    "implied_probability": implied, "source_path": str(ledger), "source_sha256": sha,
                    "odds_manifest_sha256": manifest_sha, "parent_run_id": candidate.get("parent_run_id", ""),
                    "source_parent_run_id": "",
                }
                probability = candidate.get("probability_over") if side == "HOME" else candidate.get("probability_under")
                row = evaluate_quote(row, probability)
                output[(candidate["candidate_key"], synthetic["bookmaker"])] = row
    return list(output.values()), diagnostics


def _consider_quote(output: dict[tuple[str, str], dict[str, Any]], diagnostics: list[dict[str, Any]],
                    candidate: dict[str, Any], q: dict[str, Any], obs_dir: Path, obs_sha: str,
                    odds_manifest_sha: str, observed: str, source_time: str, price: int,
                    decimal: float | None, implied: float | None, player_id: str) -> None:
    source_path = str(obs_dir / "normalized_odds.jsonl")
    quote = {"source_quote_timestamp_utc": source_time, "observation_timestamp_utc": observed}
    if not quote_is_eligible(quote, candidate):
        return
    row = {
        "candidate_key": candidate["candidate_key"], "canonical_season": candidate["canonical_season"], "slate_date": candidate["slate_date"],
        "game_id": candidate["game_id"], "player_id": candidate.get("player_id", "") if candidate["market_key"] != "goalie_saves" else "",
        "goalie_id": candidate.get("goalie_id", ""), "market_key": candidate["market_key"],
        "market_line": candidate["market_line"], "selected_side": candidate["selected_side"], "model_role": candidate["model_role"],
        "model_family": candidate["model_family"], "model_version": candidate["model_version"], "sportsbook_key": q.get("bookmaker", ""),
        "provider_event_id": q.get("provider_event_id", ""), "provider_market_key": q.get("market", ""),
        "provider_outcome": q.get("outcome", ""), "american_price": price, "decimal_price": decimal or "",
        "source_quote_timestamp_utc": source_time, "observation_timestamp_utc": observed,
        "source_capture_identity": obs_dir.name, "quote_qualification_status": "QUALIFIED_SOURCE_AND_OBSERVATION_PRECEDE_PREDICTION",
        "implied_probability": implied if implied is not None else "", "source_path": source_path, "source_sha256": obs_sha,
        "parent_run_id": candidate.get("parent_run_id", ""), "odds_manifest_sha256": odds_manifest_sha,
    }
    probability = candidate.get("probability_over") if candidate.get("selected_side") == "OVER" else candidate.get("probability_under")
    row = evaluate_quote(row, probability)
    key = (candidate["candidate_key"], str(q.get("bookmaker", "")))
    prior = output.get(key)
    rank = (0 if str(q.get("market", "")).endswith("_alternate") else 1,
            parse_utc(row["observation_timestamp_utc"]))
    prior_rank = ((0 if str(prior.get("provider_market_key", "")).endswith("_alternate") else 1),
                  parse_utc(prior["observation_timestamp_utc"])) if prior else None
    if prior is None or rank > prior_rank:
        output[key] = row


def _american_implied(price: int) -> float | None:
    if not price:
        return None
    return (-price / (-price + 100)) if price < 0 else (100 / (price + 100))


def deduplicate_quotes(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the latest eligible line quote per candidate and sportsbook."""
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not str(row.get("quote_qualification_status", "")).startswith("QUALIFIED_"):
            continue
        if not row.get("candidate_key") or not row.get("sportsbook_key"):
            continue
        if not parse_utc(row.get("source_quote_timestamp_utc")) or not parse_utc(row.get("observation_timestamp_utc")):
            continue
        key = (str(row["candidate_key"]), str(row["sportsbook_key"]))
        rank = (0 if str(row.get("provider_market_key", "")).endswith("_alternate") else 1,
                parse_utc(row.get("observation_timestamp_utc")))
        prior = selected.get(key)
        prior_rank = ((0 if str(prior.get("provider_market_key", "")).endswith("_alternate") else 1),
                      parse_utc(prior.get("observation_timestamp_utc"))) if prior else None
        if prior is None or rank > prior_rank:
            selected[key] = row
    return [selected[k] for k in sorted(selected)]


def _num(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def export(root: Path, out: Path, slate_date: str | None = None) -> dict[str, Any]:
    predictions, diagnostics = load_candidates(root)
    if slate_date:
        predictions = [r for r in predictions if r["slate_date"] == slate_date]
        dates = [slate_date]
    else:
        dates = sorted({r["slate_date"] for r in predictions})
    for date in dates:
        predictions = add_captured_sog_arms(predictions, root, date)
        predictions = add_puck_line_challenger(predictions, root, date)
    if "2026-10-16" in dates:
        required = {
            ("shots_on_goal", "baseline_v1"),
            ("shots_on_goal", "NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1"),
            ("shots_on_goal", "NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1"),
            ("puck_line_full_game", "NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2"),
        }
        missing = missing_required_arms(predictions, required)
        if missing:
            raise ValueError(f"required retained model arms missing for 2026-10-16: {sorted(missing)}")
    if slate_date:
        candidate_keys = {r["candidate_key"] for r in predictions}
        diagnostics = [r for r in diagnostics if r["candidate_key"] in candidate_keys]
    quotes, quote_diagnostics = load_retained_quotes(root, predictions)
    diagnostics.extend(quote_diagnostics)
    out.mkdir(parents=True, exist_ok=True)
    prediction_path = out / "candidate_predictions.csv"
    quote_path = out / "candidate_quotes.csv"
    coverage_path = out / "candidate_quote_coverage.csv"
    _write_csv(prediction_path, PREDICTION_FIELDS, predictions)
    quotes = deduplicate_quotes(quotes)
    _write_csv(quote_path, QUOTE_FIELDS, quotes)
    known_diagnostics = {row["candidate_key"]: row for row in diagnostics}
    for candidate in predictions:
        key = candidate["candidate_key"]
        if key not in known_diagnostics:
            known_diagnostics[key] = {
                "candidate_key": key,
                "market_key": candidate["market_key"],
                "model_role": candidate["model_role"],
                "model_family": candidate["model_family"],
                "model_version": candidate["model_version"],
                "odds_status": "NO_CHRONOLOGY_QUALIFIED_QUOTE",
                "reason": "no retained quote matched this candidate before its prediction cutoff",
                "source_path": candidate["source_path"],
                "source_sha256": candidate["source_sha256"],
            }
    quoted_candidates = {r["candidate_key"] for r in quotes}
    diagnostics = [known_diagnostics[key] for key in sorted(known_diagnostics) if key not in quoted_candidates]
    _write_csv(coverage_path, QUOTE_DIAGNOSTIC_FIELDS, diagnostics)
    market_counts = Counter(row["market_key"] for row in predictions)
    model_counts = Counter((row["market_key"], row["model_family"], row["model_version"], row["model_role"]) for row in predictions)
    quoted_counts = Counter(row["candidate_key"] for row in quotes)
    coverage_summary = {}
    for key, n in sorted(model_counts.items()):
        subset = [r for r in predictions if (r["market_key"], r["model_family"], r["model_version"], r["model_role"]) == key]
        coverage_summary["|".join(key)] = {
            "candidate_rows": n,
            "settled_outcomes": sum(r["outcome_status"] == "SETTLED" for r in subset),
            "unresolved_outcomes": sum(r["outcome_status"] != "SETTLED" for r in subset),
            "timestamp_qualified_quotes": sum(quoted_counts[c["candidate_key"]] for c in subset),
            "candidate_rows_without_qualified_quote": sum(not quoted_counts[c["candidate_key"]] for c in subset),
        }
    quote_counts_by_market = Counter(q["market_key"] for q in quotes)
    manifest = {
        "schema_version": "NHL_ALL_CANDIDATE_EVALUATION_EXPORT_V1",
        "created_from_existing_retained_evidence_only": True,
        "candidate_policy_status": "NOT_APPLIED",
        "predictions_path": prediction_path.name,
        "predictions_sha256": _sha(prediction_path),
        "quotes_path": quote_path.name,
        "quotes_sha256": _sha(quote_path),
        "quote_coverage_path": coverage_path.name,
        "quote_coverage_sha256": _sha(coverage_path),
        "candidate_count": len(predictions),
        "quote_count": len(quotes),
        "quote_counts_by_market": dict(sorted(quote_counts_by_market.items())),
        "ev_available_quote_count": sum(q.get("ev_status") == "AVAILABLE" for q in quotes),
        "edge_available_quote_count": sum(q.get("edge_status") == "AVAILABLE" for q in quotes),
        "candidate_key_unique_count": len({r["candidate_key"] for r in predictions}),
        "quote_candidate_book_unique_count": len({(r["candidate_key"], r["sportsbook_key"]) for r in quotes}),
        "quote_diagnostic_count": len(diagnostics),
        "market_counts": dict(sorted(market_counts.items())),
        "model_counts": [{"market_key": k[0], "model_family": k[1], "model_version": k[2], "model_role": k[3], "rows": n} for k, n in sorted(model_counts.items())],
        "source_evidence_scope": f"latest retained 2026 reconciliation package per slate{'; bounded to ' + slate_date if slate_date else ''}",
        "market_model_coverage": coverage_summary,
        "limitations": [
            "Only the October 16 bounded sample currently joins SOG production/fixed blends and the Puck Line challenger to immutable captures; other dates retain their reconciliation arms only.",
            "The normalized Odds API artifact provides market_last_update plus the immutable observation timestamp. EV uses the quoted price and model side probability; model-market edge uses raw implied probability including bookmaker margin.",
            "Prediction and outcome rows are combined for evaluation convenience. Outcome fields are postgame append-bound fields and must not be used as pregame inputs.",
        ],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("artifacts/operational/nhl"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--slate-date", help="Optional exact slate date for a bounded sample, YYYY-MM-DD")
    args = parser.parse_args()
    print(json.dumps(export(args.root, args.out, args.slate_date), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
