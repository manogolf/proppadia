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
    "provider_market_id", "provider_outcome_id", "american_price", "decimal_price",
    "source_quote_timestamp_utc", "observation_timestamp_utc", "quote_qualification_status",
    "implied_probability", "expected_value_per_unit", "model_market_edge",
    "ev_status", "edge_status", "source_path", "source_sha256", "parent_run_id",
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
        "prediction_identity": row.get("prediction_identity") or "",
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
                        "feature_cutoff_utc": r.get("feature_input_cutoff_utc") or "", "prediction_identity": "",
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
                    "feature_cutoff_utc": r.get("feature_input_cutoff_utc") or "", "prediction_identity": "",
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
        candidate_keys = {r["candidate_key"] for r in predictions}
        diagnostics = [r for r in diagnostics if r["candidate_key"] in candidate_keys]
    out.mkdir(parents=True, exist_ok=True)
    prediction_path = out / "candidate_predictions.csv"
    quote_path = out / "candidate_quotes.csv"
    coverage_path = out / "candidate_quote_coverage.csv"
    _write_csv(prediction_path, PREDICTION_FIELDS, predictions)
    # Reconciliation tables do not retain source quote timestamps consistently.
    # Do not turn attached prices into qualified quote observations without them.
    _write_csv(quote_path, QUOTE_FIELDS, [])
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
                "odds_status": "NO_RETAINED_TIMESTAMPED_QUOTE",
                "reason": "source row has no sportsbook quote with both quote and observation timestamps",
                "source_path": candidate["source_path"],
                "source_sha256": candidate["source_sha256"],
            }
    diagnostics = [known_diagnostics[key] for key in sorted(known_diagnostics)]
    _write_csv(coverage_path, QUOTE_DIAGNOSTIC_FIELDS, diagnostics)
    market_counts = Counter(row["market_key"] for row in predictions)
    model_counts = Counter((row["market_key"], row["model_family"], row["model_version"], row["model_role"]) for row in predictions)
    coverage_summary = {}
    for key, n in sorted(model_counts.items()):
        subset = [r for r in predictions if (r["market_key"], r["model_family"], r["model_version"], r["model_role"]) == key]
        coverage_summary["|".join(key)] = {
            "candidate_rows": n,
            "settled_outcomes": sum(r["outcome_status"] == "SETTLED" for r in subset),
            "unresolved_outcomes": sum(r["outcome_status"] != "SETTLED" for r in subset),
            "timestamp_qualified_quotes": 0,
            "candidate_rows_without_qualified_quote": n,
        }
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
        "quote_count": 0,
        "quote_diagnostic_count": len(diagnostics),
        "market_counts": dict(sorted(market_counts.items())),
        "model_counts": [{"market_key": k[0], "model_family": k[1], "model_version": k[2], "model_role": k[3], "rows": n} for k, n in sorted(model_counts.items())],
        "source_evidence_scope": f"latest retained 2026 reconciliation package per slate{'; bounded to ' + slate_date if slate_date else ''}",
        "market_model_coverage": coverage_summary,
        "limitations": [
            "The row-level retained reconciliation sources do not include the separate prospective SOG production and fixed-blend captures or the puck-line challenger capture; those arms are not inferred from aggregate summaries.",
            "Retained reconciliation attachments do not provide both source quote and observation timestamps. No quote qualifies for candidate_quotes.csv; all candidates have explicit coverage diagnostics.",
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
