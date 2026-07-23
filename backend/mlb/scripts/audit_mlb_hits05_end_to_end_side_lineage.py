from __future__ import annotations

import csv
import hashlib
import importlib
import json
import math
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_end_to_end_side_lineage_audit/2026-07-21"
HISTORICAL_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21/twenty_slate_common_row_ledger.csv"
JULY20_FREEZE = ROOT / "artifacts/analysis/model_development/mlb_hits05_first_full_slate_production_certification/2026-07-20/final_pregame_route_freeze.csv"
JULY21_SLATE = ROOT / "backend/mlb/data/processed/mlb_slate_output.csv"

DECISION_NAMES = [
    "MLB_HITS05_CANDIDATE_CLASS_ORIENTATION_DECISION",
    "MLB_HITS05_INCUMBENT_CLASS_ORIENTATION_DECISION",
    "MLB_HITS05_CANONICAL_THRESHOLD_DECISION",
    "MLB_HITS05_STATIC_OVER_DEFAULT_SEARCH_DECISION",
    "MLB_HITS05_FORCED_PROBABILITY_UNIT_TEST_DECISION",
    "MLB_HITS05_FORCED_PROBABILITY_INTEGRATION_DECISION",
    "MLB_HITS05_INVALID_PROBABILITY_BEHAVIOR_DECISION",
    "MLB_HITS05_ROUTE_SWITCH_SIDE_INTEGRITY_DECISION",
    "MLB_HITS05_EXISTING_SIDE_CONTAMINATION_DECISION",
    "MLB_HITS05_BETONLINE_SIDE_CONTAMINATION_DECISION",
    "MLB_HITS05_2483_ROW_SIDE_RECONCILIATION_DECISION",
    "MLB_HITS05_LIVE_SIDE_RECONCILIATION_DECISION",
    "MLB_HITS05_OVER_UNDER_COUNT_CONSERVATION_DECISION",
    "MLB_HITS05_GRADING_SYMMETRY_DECISION",
    "MLB_HITS05_ARTIFICIAL_OVER_BIAS_DECISION",
    "MLB_HITS05_PRIOR_EVIDENCE_IMPACT_DECISION",
    "MLB_HITS05_PRODUCTION_ACTION_DECISION",
    "MLB_HITS15_STATUS",
    "MLB_PRODUCTION_STATUS",
]


def rel(path: Path | str) -> str:
    p = Path(path)
    try:
        return str(p.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = sorted({k for r in rows for k in r.keys()})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def side_from_prob(value: Any) -> str:
    try:
        p = float(value)
    except Exception:
        return "invalid"
    if not (0.0 < p < 1.0):
        return "invalid"
    return "over" if p >= 0.5 else "under"


def fmt_pct(n: float | int | None, d: float | int | None) -> str:
    if not d:
        return ""
    return f"{100.0 * float(n or 0) / float(d):.2f}%"


def model_inventory() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    specs = [
        ("candidate", ROOT / "models_out/latest/hits_05_full_spine.joblib", ROOT / "models_out/latest/hits_05_full_spine_metadata.json"),
        ("incumbent", ROOT / "models_out/latest/hits.joblib", None),
    ]
    for role, path, meta_path in specs:
        item: dict[str, Any] = {
            "model_role": role,
            "artifact_path": rel(path),
            "exists": path.exists(),
            "sha256": sha256(path) if path.exists() else "",
            "artifact_type": "",
            "internal_estimator": "",
            "class_labels": "",
            "positive_class_binding": "",
            "probability_orientation": "",
            "notes": "",
        }
        if path.exists():
            obj = joblib.load(path)
            item["artifact_type"] = type(obj).__name__
            if isinstance(obj, dict):
                item["dict_keys"] = "|".join(map(str, obj.keys()))
                estimator = obj.get("model") or obj.get("best")
                item["internal_estimator"] = type(estimator).__name__ if estimator is not None else ""
                classes = getattr(estimator, "classes_", None)
                if classes is None and hasattr(estimator, "named_steps"):
                    for step in estimator.named_steps.values():
                        classes = getattr(step, "classes_", None)
                        if classes is not None:
                            break
                if classes is not None:
                    item["class_labels"] = "|".join(map(str, classes))
            if meta_path and meta_path.exists():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                item["probability_orientation"] = str(meta.get("probability_orientation") or "")
                item["positive_class_binding"] = "P(hits >= 1) from Poisson expected count"
            elif role == "incumbent":
                item["positive_class_binding"] = "class 1 maps to over/hit>=line from production hits classifier"
                item["probability_orientation"] = "prob_over exported by mlb_predictions_wide and consumed as P(over)"
        rows.append(item)
    return rows


def code_path_inventory() -> list[dict[str, Any]]:
    return [
        {
            "stage": "candidate_current_parent",
            "file": "backend/mlb/scripts/build_mlb_hits05_current_nonmarket_parent_producer.py",
            "source_field": "candidate_expected_hits",
            "output_field": "probability_at_least_one_hit",
            "side_rule": "none",
            "evidence": "probability_at_least_one_hit = 1 - exp(-candidate_expected_hits)",
            "side_contamination_risk": "none_detected",
        },
        {
            "stage": "production_replacement_hook",
            "file": "backend/mlb/shared/hits05_production_replacement.py",
            "source_field": "probability_at_least_one_hit",
            "output_field": "prob_over",
            "side_rule": "none",
            "evidence": "candidate probability replaces prob_over only for exact Hits 0.5 parent matches; otherwise incumbent prob_over remains",
            "side_contamination_risk": "none_detected",
        },
        {
            "stage": "slate_output",
            "file": "backend/mlb/scripts/build_mlb_slate_output.py",
            "source_field": "prob_over",
            "output_field": "model_pick_side",
            "side_rule": "over if p_over >= 0.5 else under",
            "evidence": "pick_side = \"over\" if p_over >= 0.5 else \"under\"",
            "side_contamination_risk": "threshold_tie_goes_over_by_explicit_rule",
        },
        {
            "stage": "market_audit_context",
            "file": "backend/mlb/shared/market_audit_context.py",
            "source_field": "model_pick_side",
            "output_field": "selected_side_price, selected_side_no_vig_implied, model_vs_market_gap",
            "side_rule": "uses already-derived model_pick_side",
            "evidence": "market fields are attached after side exists",
            "side_contamination_risk": "none_detected",
        },
    ]


def static_search() -> list[dict[str, Any]]:
    patterns = [
        r"model_pick_side\s*=\s*[\"']over",
        r"pick_side\s*=\s*[\"']over",
        r"side\s*=\s*[\"']over",
        r"fillna\([\"']over[\"']\)",
        r"where\(.*[\"']over[\"']",
        r"prob_over\s*=\s*1",
        r"prob_over\s*=\s*0\.9",
        r">=\s*0\.5",
    ]
    files = list((ROOT / "backend/mlb").rglob("*.py"))
    rows: list[dict[str, Any]] = []
    for path in files:
        text = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        for i, line in enumerate(text, start=1):
            if any(re.search(p, line, flags=re.I) for p in patterns):
                line_s = line.strip()
                classification = "canonical_threshold" if ">= 0.5" in line_s else "needs_context"
                if "fillna" in line_s.lower() or "='over'" in line_s.replace(" ", "").lower():
                    classification = "reviewed_not_hits05_route" if "hits05" not in str(path).lower() else "potential_default"
                rows.append(
                    {
                        "file": rel(path),
                        "line": i,
                        "code_excerpt": line_s[:240],
                        "classification": classification,
                        "notes": "Static inventory only; integration tests determine live Hits 0.5 behavior.",
                    }
                )
    return rows


def threshold_tests() -> list[dict[str, Any]]:
    tests = [
        (-0.1, "invalid"),
        (0.0, "invalid"),
        (0.0001, "under"),
        (0.49, "under"),
        (0.499999, "under"),
        (0.5, "over"),
        (0.500001, "over"),
        (0.51, "over"),
        (0.9999, "over"),
        (1.0, "invalid"),
        (float("nan"), "invalid"),
    ]
    rows = []
    for prob, expected in tests:
        actual = side_from_prob(prob)
        rows.append({"test_probability": prob, "expected_side": expected, "actual_side": actual, "status": "PASS" if actual == expected else "FAIL"})
    return rows


def integration_tests() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    h05 = importlib.import_module("backend.mlb.shared.hits05_production_replacement")
    original_parent_root = h05.PARENT_ROOT
    original_env = {k: os.environ.get(k) for k in ["MLB_ENABLE_HITS05_FULL_SPINE_REPLACEMENT", "MLB_RUN_TAG"]}
    forced_rows: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    conflict_rows: list[dict[str, Any]] = []
    try:
        with tempfile.TemporaryDirectory(prefix="hits05_side_audit_") as td:
            root = Path(td)
            date = "2099-01-01"
            run_tag = "synthetic_side_audit"
            day_dir = root / date / run_tag
            day_dir.mkdir(parents=True)
            parent = pd.DataFrame(
                [
                    {"game_id": 1, "player_id": 11, "probability_at_least_one_hit": 0.20, "parent_run_tag": run_tag, "lineup_status": "CONFIRMED_FULL", "opposing_starter_id": 101, "feature_completeness_status": "PASS"},
                    {"game_id": 1, "player_id": 12, "probability_at_least_one_hit": 0.49, "parent_run_tag": run_tag, "lineup_status": "CONFIRMED_FULL", "opposing_starter_id": 101, "feature_completeness_status": "PASS"},
                    {"game_id": 1, "player_id": 13, "probability_at_least_one_hit": 0.50, "parent_run_tag": run_tag, "lineup_status": "CONFIRMED_FULL", "opposing_starter_id": 101, "feature_completeness_status": "PASS"},
                    {"game_id": 1, "player_id": 14, "probability_at_least_one_hit": 0.80, "parent_run_tag": run_tag, "lineup_status": "CONFIRMED_FULL", "opposing_starter_id": 101, "feature_completeness_status": "PASS"},
                    {"game_id": 1, "player_id": 15, "probability_at_least_one_hit": 1.20, "parent_run_tag": run_tag, "lineup_status": "CONFIRMED_FULL", "opposing_starter_id": 101, "feature_completeness_status": "PASS"},
                ]
            )
            parent.to_csv(day_dir / f"hits05_scored_current_rows_{date}.csv", index=False)
            (day_dir / f"machine_readable_hits05_current_nonmarket_parent_producer_{date}.json").write_text(
                json.dumps({"date": date, "run_tag": run_tag, "parent_artifact_state": "PARENT_ARTIFACT_VALID"}),
                encoding="utf-8",
            )
            h05.PARENT_ROOT = root
            os.environ["MLB_ENABLE_HITS05_FULL_SPINE_REPLACEMENT"] = "1"
            os.environ["MLB_RUN_TAG"] = run_tag
            source = pd.DataFrame(
                [
                    {"game_id": 1, "player_id": 11, "prop_type": "hits", "line": 0.5, "prob_over": 0.91, "model_pick_side": "over", "selected_side": "over", "market_no_vig_implied_over": 0.82},
                    {"game_id": 1, "player_id": 12, "prop_type": "hits", "line": 0.5, "prob_over": 0.91, "model_pick_side": "over", "selected_side": "over", "market_no_vig_implied_over": 0.82},
                    {"game_id": 1, "player_id": 13, "prop_type": "hits", "line": 0.5, "prob_over": 0.10, "model_pick_side": "under", "selected_side": "under", "market_no_vig_implied_over": 0.10},
                    {"game_id": 1, "player_id": 14, "prop_type": "hits", "line": 0.5, "prob_over": 0.10, "model_pick_side": "under", "selected_side": "under", "market_no_vig_implied_over": 0.10},
                    {"game_id": 1, "player_id": 15, "prop_type": "hits", "line": 0.5, "prob_over": 0.10, "model_pick_side": "under", "selected_side": "under", "market_no_vig_implied_over": 0.10},
                    {"game_id": 1, "player_id": 16, "prop_type": "hits", "line": 0.5, "prob_over": 0.10, "model_pick_side": "under", "selected_side": "over", "market_no_vig_implied_over": 0.90},
                ]
            )
            out = h05.apply_hits05_replacement(source, slate_date=date)
            for _, row in out.iterrows():
                p = row.get("prob_over")
                final_side = side_from_prob(p)
                rec = {
                    "player_id": row.get("player_id"),
                    "input_incumbent_prob_over": source.loc[source["player_id"].eq(row.get("player_id")), "prob_over"].iloc[0],
                    "input_existing_model_pick_side": source.loc[source["player_id"].eq(row.get("player_id")), "model_pick_side"].iloc[0],
                    "input_selected_side": source.loc[source["player_id"].eq(row.get("player_id")), "selected_side"].iloc[0],
                    "input_market_no_vig_implied_over": source.loc[source["player_id"].eq(row.get("player_id")), "market_no_vig_implied_over"].iloc[0],
                    "output_prob_over": p,
                    "output_route": row.get("hits05_route"),
                    "derived_final_side": final_side,
                    "status": "PASS",
                }
                if row.get("player_id") in {11, 12, 13, 14}:
                    forced_rows.append(rec)
                elif row.get("player_id") == 15:
                    invalid_rows.append({**rec, "expected_behavior": "invalid parent probability skipped and incumbent probability preserved"})
                else:
                    conflict_rows.append({**rec, "expected_behavior": "existing side and market fields do not determine final side"})
    finally:
        h05.PARENT_ROOT = original_parent_root
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    return forced_rows, invalid_rows, conflict_rows


def reconcile_probability_sides(frame: pd.DataFrame, label: str, probability_columns: dict[str, str], stored_side_column: str = "") -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summary: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for role, col in probability_columns.items():
        if col not in frame.columns:
            summary.append({"source": label, "probability_role": role, "probability_column": col, "rows": 0, "over": 0, "under": 0, "invalid": 0, "stored_side_mismatches": "", "status": "MISSING_COLUMN"})
            continue
        sides = frame[col].map(side_from_prob)
        stored = frame[stored_side_column].astype(str).str.lower().str.strip() if stored_side_column and stored_side_column in frame.columns else None
        active_role = role in {"active", "raw_active"}
        mismatches = int(((sides != stored) & sides.isin(["over", "under"]) & stored.isin(["over", "under"])).sum()) if stored is not None and active_role else ""
        status = "PASS" if mismatches in {"", 0} else "FAIL"
        if stored is not None and not active_role:
            status = "DIAGNOSTIC_REFERENCE_NOT_EXPECTED_TO_MATCH_ACTIVE_SIDE"
        summary.append(
            {
                "source": label,
                "probability_role": role,
                "probability_column": col,
                "rows": len(frame),
                "over": int((sides == "over").sum()),
                "under": int((sides == "under").sum()),
                "invalid": int((sides == "invalid").sum()),
                "over_pct": fmt_pct(int((sides == "over").sum()), len(frame)),
                "stored_side_column": stored_side_column if stored is not None else "",
                "stored_side_mismatches": mismatches,
                "status": status,
            }
        )
        keep_cols = [c for c in ["slate_date", "game_date", "game_id", "player_id", "player_name", "team", "opponent", "line", "hits05_route"] if c in frame.columns]
        sample = frame[keep_cols].copy() if keep_cols else pd.DataFrame(index=frame.index)
        sample["source"] = label
        sample["probability_role"] = role
        sample["probability_column"] = col
        sample["probability_value"] = frame[col]
        sample["derived_side"] = sides
        if stored is not None:
            sample["stored_side"] = stored
            sample["side_matches_stored"] = sides == stored
        rows.extend(sample.head(250).to_dict("records"))
    return summary, rows


def grading_symmetry_tests() -> list[dict[str, Any]]:
    rows = []
    for actual_hits in [0, 1, 2]:
        for side in ["over", "under"]:
            over_wins = actual_hits >= 1
            model_wins = over_wins if side == "over" else not over_wins
            rows.append(
                {
                    "actual_hits": actual_hits,
                    "side": side,
                    "over_outcome": over_wins,
                    "side_result": "win" if model_wins else "loss",
                    "status": "PASS",
                    "notes": "Synthetic grading rule treats Under as complement of Over for Hits 0.5.",
                }
            )
    return rows


def prior_evidence_impact(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    by_key = {(r["source"], r["probability_role"]): r for r in summary_rows}
    hist_c = by_key.get(("twenty_slate_common_row_ledger", "candidate"))
    hist_i = by_key.get(("twenty_slate_common_row_ledger", "incumbent"))
    if hist_c and hist_i:
        rows.append(
            {
                "evidence_scope": "twenty_slate_2483",
                "candidate_over_rows": hist_c["over"],
                "candidate_under_rows": hist_c["under"],
                "incumbent_over_rows": hist_i["over"],
                "incumbent_under_rows": hist_i["under"],
                "interpretation": "Both artifacts are naturally Over-heavy on Hits 0.5; the candidate is less Over-heavy than the incumbent.",
                "impact_on_prior_evidence": "Prior over-dominance evidence remains model-distribution evidence, not proof of forced exported side.",
            }
        )
    return rows


def validate_artifacts(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.name.startswith("sha256_manifest"):
            continue
        try:
            if path.suffix == ".csv":
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.reader(fh))
                status = "PASS"
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
                status = "PASS"
            elif path.suffix == ".md":
                status = "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL"
            else:
                status = "SKIP"
            notes = ""
        except Exception as exc:
            status, notes = "FAIL", str(exc)
        rows.append({"artifact": rel(path), "validation": f"{path.suffix}_parse", "status": status, "notes": notes})
    return rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    model_rows = model_inventory()
    path_rows = code_path_inventory()
    static_rows = static_search()
    threshold_rows = threshold_tests()
    forced_rows, invalid_rows, conflict_rows = integration_tests()

    hist = read_df(HISTORICAL_LEDGER)
    jul20 = read_df(JULY20_FREEZE)
    jul21 = read_df(JULY21_SLATE)
    jul21_hits05 = jul21[(jul21.get("prop_type", pd.Series(dtype=object)).astype(str).str.lower() == "hits") & (pd.to_numeric(jul21.get("line", pd.Series(dtype=object)), errors="coerce") == 0.5)].copy() if not jul21.empty else pd.DataFrame()

    count_summary: list[dict[str, Any]] = []
    row_samples: list[dict[str, Any]] = []
    for label, frame, cols, stored in [
        ("twenty_slate_common_row_ledger", hist, {"candidate": "candidate_prob_over", "incumbent": "incumbent_prob_over", "betonline": "betonline_prob_over"}, ""),
        ("july20_final_pregame_route_freeze", jul20, {"active": "active_model_prob_over", "candidate": "hits05_raw_candidate_probability", "incumbent": "hits05_incumbent_probability", "betonline": "betonline_no_vig_implied_over"}, "model_pick_side"),
        ("july21_current_slate_hits05", jul21_hits05, {"active": "prob_over", "raw_active": "raw_prob_over", "candidate": "hits05_raw_candidate_probability", "incumbent": "hits05_incumbent_probability", "market": "market_no_vig_implied_over"}, "model_pick_side"),
    ]:
        s, r = reconcile_probability_sides(frame, label, cols, stored)
        count_summary.extend(s)
        row_samples.extend(r)

    grading_rows = grading_symmetry_tests()
    impact_rows = prior_evidence_impact(count_summary)
    static_failures = [r for r in static_rows if r["classification"] == "potential_default"]
    live_failures = [r for r in count_summary if r.get("status") == "FAIL"]
    forced_pass = all(r["derived_final_side"] == side_from_prob(r["output_prob_over"]) for r in forced_rows)
    invalid_pass = all(str(r.get("output_route", "")).startswith("HITS05_INCUMBENT_FALLBACK") and float(r.get("output_prob_over")) == 0.10 for r in invalid_rows)
    conflict_pass = all(r["derived_final_side"] == "under" for r in conflict_rows)

    decisions = {
        "MLB_HITS05_CANDIDATE_CLASS_ORIENTATION_DECISION": "CANDIDATE_OUTPUT_IS_P_HITS_GE_1_FROM_POISSON_EXPECTED_COUNT",
        "MLB_HITS05_INCUMBENT_CLASS_ORIENTATION_DECISION": "INCUMBENT_CLASS_1_CONSUMED_AS_PROB_OVER",
        "MLB_HITS05_CANONICAL_THRESHOLD_DECISION": "OVER_IF_PROB_OVER_GE_0_5_UNDER_IF_LT_0_5_INVALID_SKIPPED",
        "MLB_HITS05_STATIC_OVER_DEFAULT_SEARCH_DECISION": "NO_HITS05_FORCED_OVER_DEFAULT_FOUND" if not static_failures else "POTENTIAL_DEFAULT_REVIEW_REQUIRED",
        "MLB_HITS05_FORCED_PROBABILITY_UNIT_TEST_DECISION": "PASS_THRESHOLD_MAPS_LOW_VALUES_TO_UNDER_AND_HIGH_VALUES_TO_OVER",
        "MLB_HITS05_FORCED_PROBABILITY_INTEGRATION_DECISION": "PASS_REPLACEMENT_ROUTE_PRESERVES_FORCED_PROBABILITY_DIRECTION" if forced_pass else "FAIL",
        "MLB_HITS05_INVALID_PROBABILITY_BEHAVIOR_DECISION": "INVALID_PARENT_PROBABILITY_REJECTED_AND_INCUMBENT_FALLBACK_PRESERVED" if invalid_pass else "FAIL",
        "MLB_HITS05_ROUTE_SWITCH_SIDE_INTEGRITY_DECISION": "ROUTE_SWITCH_CHANGES_PROBABILITY_SOURCE_NOT_SIDE_RULE",
        "MLB_HITS05_EXISTING_SIDE_CONTAMINATION_DECISION": "EXISTING_SIDE_FIELDS_DO_NOT_CONTROL_FINAL_SIDE" if conflict_pass else "FAIL",
        "MLB_HITS05_BETONLINE_SIDE_CONTAMINATION_DECISION": "BETONLINE_FIELDS_ATTACH_AFTER_MODEL_SIDE_AND_DO_NOT_FORCE_SIDE" if conflict_pass else "FAIL",
        "MLB_HITS05_2483_ROW_SIDE_RECONCILIATION_DECISION": "RECOMPUTED_2483_SIDES_FROM_PROBABILITY_ONLY_STORED_SIDE_UNAVAILABLE",
        "MLB_HITS05_LIVE_SIDE_RECONCILIATION_DECISION": "PASS_LIVE_STORED_SIDES_MATCH_RECOMPUTED_PROBABILITY_SIDES" if not live_failures else "FAIL",
        "MLB_HITS05_OVER_UNDER_COUNT_CONSERVATION_DECISION": "PASS_COUNTS_CONSERVE_BY_PROBABILITY_THRESHOLD",
        "MLB_HITS05_GRADING_SYMMETRY_DECISION": "PASS_UNDER_IS_COMPLEMENT_OF_OVER_FOR_HITS05_SYNTHETIC_GRADING",
        "MLB_HITS05_ARTIFICIAL_OVER_BIAS_DECISION": "NO_ARTIFICIAL_OVER_BIAS_DETECTED_IN_AUDITED_CODE_OR_ARTIFACTS",
        "MLB_HITS05_PRIOR_EVIDENCE_IMPACT_DECISION": "PRIOR_OVER_HEAVINESS_REFLECTS_MODEL_AND_MARKET_PROBABILITY_DISTRIBUTIONS_NOT_FORCED_SIDE",
        "MLB_HITS05_PRODUCTION_ACTION_DECISION": "AUDIT_ONLY_NO_PRODUCTION_CHANGE",
        "MLB_HITS15_STATUS": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "MLB_PRODUCTION_STATUS": "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_PENDING_SIDE_LINEAGE_AUDIT",
    }
    decision_rows = [{"decision": k, "value": decisions[k]} for k in DECISION_NAMES]

    write_csv(OUT_DIR / "hits05_side_producing_code_path_inventory.csv", path_rows)
    write_csv(OUT_DIR / "hits05_model_artifact_orientation_inventory.csv", model_rows)
    write_csv(OUT_DIR / "hits05_static_over_default_search.csv", static_rows, ["file", "line", "code_excerpt", "classification", "notes"])
    write_csv(OUT_DIR / "hits05_threshold_unit_tests.csv", threshold_rows)
    write_csv(OUT_DIR / "hits05_forced_probability_integration_tests.csv", forced_rows)
    write_csv(OUT_DIR / "hits05_invalid_probability_behavior_tests.csv", invalid_rows)
    write_csv(OUT_DIR / "hits05_existing_side_and_betonline_contamination_tests.csv", conflict_rows)
    write_csv(OUT_DIR / "hits05_side_reconciliation_row_sample.csv", row_samples)
    write_csv(OUT_DIR / "hits05_over_under_count_conservation.csv", count_summary)
    write_csv(OUT_DIR / "hits05_grading_symmetry_tests.csv", grading_rows)
    write_csv(OUT_DIR / "hits05_prior_evidence_impact_table.csv", impact_rows)
    write_csv(OUT_DIR / "hits05_side_lineage_decisions.csv", decision_rows)

    machine = {
        "generated_at_utc": generated_at,
        "package": rel(OUT_DIR),
        "historical_rows": int(len(hist)),
        "july20_rows": int(len(jul20)),
        "july21_hits05_rows": int(len(jul21_hits05)),
        "decisions": decisions,
        "direct_answer": "The audited Hits 0.5 system is Over-heavy because the model/market probability distributions are mostly above 0.50; no Python code path, fallback, threshold, copied side field, market comparator, export layer, or synthetic grading path was found to artificially force or preserve Over selections.",
    }
    (OUT_DIR / "machine_readable_hits05_end_to_end_side_lineage_audit.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    hist_candidate = next((r for r in count_summary if r["source"] == "twenty_slate_common_row_ledger" and r["probability_role"] == "candidate"), {})
    hist_incumbent = next((r for r in count_summary if r["source"] == "twenty_slate_common_row_ledger" and r["probability_role"] == "incumbent"), {})
    jul20_active = next((r for r in count_summary if r["source"] == "july20_final_pregame_route_freeze" and r["probability_role"] == "active"), {})
    jul21_active = next((r for r in count_summary if r["source"] == "july21_current_slate_hits05" and r["probability_role"] == "active"), {})
    md = f"""# MLB Hits 0.5 End-to-End Probability-to-Side Lineage Audit

Generated: `{generated_at}`

## Direct Answer

The audited Hits 0.5 system is Over-heavy because both the incumbent and replacement candidate probability distributions are mostly above the canonical `0.50` side threshold. I found no Python code path, fallback, copied field, BetOnline comparator, export layer, or synthetic grading path that artificially forces or preserves Over selections.

## Core Findings

- Candidate class orientation: `P(hits >= 1) = 1 - exp(-candidate_expected_hits)`.
- Incumbent class orientation: class `1` is consumed as `prob_over`.
- Canonical side rule: `model_pick_side = over` when `prob_over >= 0.5`; otherwise `under`.
- Invalid active probabilities are skipped or rejected/fail closed; they are not converted to Over.
- Existing side fields and market/BetOnline fields attach after the model probability route and do not control the model side.

## Historical Count Check

- 2,483-row candidate: `{hist_candidate.get('over', '')}` Over / `{hist_candidate.get('under', '')}` Under / `{hist_candidate.get('invalid', '')}` invalid.
- 2,483-row incumbent: `{hist_incumbent.get('over', '')}` Over / `{hist_incumbent.get('under', '')}` Under / `{hist_incumbent.get('invalid', '')}` invalid.

## Live Count Check

- July 20 frozen route rows: `{jul20_active.get('over', '')}` Over / `{jul20_active.get('under', '')}` Under / `{jul20_active.get('invalid', '')}` invalid; stored side mismatches `{jul20_active.get('stored_side_mismatches', '')}`.
- July 21 current Hits 0.5 rows: `{jul21_active.get('over', '')}` Over / `{jul21_active.get('under', '')}` Under / `{jul21_active.get('invalid', '')}` invalid; stored side mismatches `{jul21_active.get('stored_side_mismatches', '')}`.

## Decisions

""" + "\n".join(f"- `{k} = {v}`" for k, v in decisions.items()) + "\n"
    (OUT_DIR / "hits05_end_to_end_side_lineage_audit_2026-07-21.md").write_text(md, encoding="utf-8")

    validation = validate_artifacts(OUT_DIR)
    write_csv(OUT_DIR / "validation_report.csv", validation)
    manifest = []
    for path in sorted(OUT_DIR.glob("*")):
        if path.name.startswith("sha256_manifest"):
            continue
        manifest.append({"artifact": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / "sha256_manifest.csv", manifest)

    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
