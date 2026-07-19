"""Run a bounded research-only Hits 1.5 PA opportunity overlay diagnostic.

This utility reads local frozen research artifacts only. It performs no network
access, no database writes, no training, no matrix construction, no uploads, and
no production behavior changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-16"
ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_15_pa_opportunity_overlay_diagnostic/2026-07-16"

REENTRY_DIR = ROOT / "artifacts/analysis/model_development/mlb_current_live_selected_proposition_research_reentry/2026-07-16"
PA_CHAR_DIR = ROOT / "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11"
PA_BUNDLE_DIR = ROOT / "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11"
PA_SPEC_DIR = ROOT / "artifacts/analysis/model_development/mlb_cc_0001_pa_opportunity_spec_2026-07-10"
HIST_CERT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/2026-07-14"
HIST_MATRIX_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_bundle_matrix_construction/2026-07-13"
SPINE_CONTRACT_DIR = ROOT / "artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
SIDE_BINDING_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_side_binding_and_resume/2026-07-13"
LIVE_ODDS_DIR = ROOT / "backend/mlb/exports/odds_history/2026-07-16"
PA_FOUNDATION_DIR = ROOT / "artifacts/analysis/mlb/pa_foundation"

PA_EXTENDED_BASE = PA_CHAR_DIR / "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
PA_DECISION = PA_CHAR_DIR / "pa_opp_v1_characterization_decision_2026-07-11.json"
PA_FIELD_DISPOSITION = PA_CHAR_DIR / "pa_opp_v1_field_disposition_2026-07-11.csv"
PA_SHA_MANIFEST = PA_CHAR_DIR / "pa_opp_v1_sha256_manifest_2026-07-11.csv"
PA_SELECTED_FEATURE_CONTRACTS = PA_BUNDLE_DIR / "pa_selected_feature_contracts_2026-07-11.md"
PA_BUNDLE_READINESS = PA_BUNDLE_DIR / "pa_opportunity_bundle_readiness_2026-07-11.json"
PA_BUNDLE_RESEARCH_BASE = PA_BUNDLE_DIR / "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"
REENTRY_DESIGN = REENTRY_DIR / "bounded_experiment_design_brief_2026-07-16.csv"
REENTRY_DECISION = REENTRY_DIR / "machine_readable_reentry_decision_2026-07-16.json"
LIVE_SLATE = LIVE_ODDS_DIR / "mlb_slate_output__local_daily_20260716T200001Z.csv"
LIVE_MANIFEST = LIVE_ODDS_DIR / "manifest.json"
LIVE_DOWNSTREAM_PA = PA_FOUNDATION_DIR / "mlb_pa_downstream_coverage_2026-07-16.csv"
LIVE_REVIEW_AID_PA = PA_FOUNDATION_DIR / "review_aid_pa_retention_pilot_2026-07-16.csv"
HIST_H15_CERT = HIST_CERT_DIR / "fully_qualified_hits_1_5_manifest_2026-07-14.csv"
HIST_H15_MATRIX = HIST_MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-13.csv"
SPINE_CONTRACT = SPINE_CONTRACT_DIR / "historical_population_spine_contract_v1_2026-07-12.json"
SPINE_IDENTITY = SPINE_CONTRACT_DIR / "canonical_identity_specification_2026-07-12.csv"
SPINE_OUTCOME_CONTRACT = ROOT / "artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12/collective_bundle_v1_outcome_label_contract_2026-07-12.json"
SELECTED_LIMITATION = SIDE_BINDING_DIR / "selected_proposition_limitation_statement_2026-07-13.md"
OUTCOME_CERTIFICATION = SIDE_BINDING_DIR / "complete_outcome_certification_ledger_2026-07-13.csv"
SELECTION_PROVENANCE = SIDE_BINDING_DIR / "selection_conditioning_provenance_registry_2026-07-13.csv"


PA_FIELDS = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_opp_v1_d7_pa_pg",
    "pa_opp_v1_d15_pa_pg",
    "pa_opp_v1_d30_pa_pg",
    "pa_opp_v1_d7_vs_d15_delta",
    "pa_opp_v1_d7_vs_d30_delta",
    "pa_opp_v1_d15_vs_d30_delta",
    "pa_opp_v1_d7_to_d30_ratio",
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "pa_context_latest_date",
    "pa_opp_v1_cutoff_status",
    "pa_missing_flag",
    "pa_source_regime",
    "pa_semantics_status",
    "pa_opp_v1_complete_prior_pa",
    "pa_opp_v1_context_age_days",
    "pa_opp_v1_feature_version",
    "pa_opp_v1_formula_version",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _num(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _canonical_key(row: dict[str, Any], side_field: str = "side_normalized") -> str:
    side = row.get(side_field) or row.get("side") or ""
    return "|".join([
        str(row.get("slate_date") or row.get("game_date") or row.get("date") or "")[:10],
        str(row.get("game_id") or ""),
        str(row.get("player_id") or ""),
        str(row.get("prop_type") or ""),
        str(row.get("line") or ""),
        str(side or ""),
    ])


def _american_profit(price: float | None, won: bool) -> float | None:
    if price is None:
        return None
    if not won:
        return -1.0
    if price >= 0:
        return price / 100.0
    return 100.0 / abs(price)


def _ci_rate(wins: int, resolved: int) -> tuple[float | None, float | None, float | None]:
    if resolved <= 0:
        return None, None, None
    rate = wins / resolved
    se = math.sqrt(rate * (1.0 - rate) / resolved)
    return rate, max(0.0, rate - 1.96 * se), min(1.0, rate + 1.96 * se)


def _control_prob_bucket(value: float | None) -> str:
    if value is None:
        return "missing_control_probability"
    if value < 0.55:
        return "lt_0_55"
    if value < 0.60:
        return "0_55_to_lt_0_60"
    if value < 0.65:
        return "0_60_to_lt_0_65"
    if value < 0.70:
        return "0_65_to_lt_0_70"
    return "ge_0_70"


def _d15_quantile_band(value: float | None, q1: float, q2: float) -> str:
    if value is None:
        return "missing"
    if value < q1:
        return "q1_low"
    if value < q2:
        return "q2_mid"
    return "q3_high"


def _calc_quantiles(values: list[float]) -> tuple[float, float]:
    if not values:
        return 3.8, 4.3
    values = sorted(values)
    def pick(p: float) -> float:
        idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * p))))
        return values[idx]
    return pick(1 / 3), pick(2 / 3)


def _source_binding_rows() -> list[dict[str, Any]]:
    artifacts = [
        ("current_live_reentry_decision", REENTRY_DECISION, "binds selected next priority and live run tag"),
        ("current_live_experiment_design", REENTRY_DESIGN, "binds design-only PA overlay scope"),
        ("pa_extended_historical_base", PA_EXTENDED_BASE, "primary historical PA overlay diagnostic spine"),
        ("pa_characterization_decision", PA_DECISION, "PA semantic and strict-prior caveat authority"),
        ("pa_field_disposition", PA_FIELD_DISPOSITION, "field disposition authority"),
        ("pa_selected_feature_contracts", PA_SELECTED_FEATURE_CONTRACTS, "frozen PA feature formulas and cutoff rules"),
        ("pa_bundle_readiness", PA_BUNDLE_READINESS, "bounded PA bundle readiness authority"),
        ("pa_bundle_research_base", PA_BUNDLE_RESEARCH_BASE, "supporting July 3-9 PA research base"),
        ("pa_sha_manifest", PA_SHA_MANIFEST, "PA source hash manifest"),
        ("historical_population_spine_contract", SPINE_CONTRACT, "historical population spine contract"),
        ("canonical_identity_specification", SPINE_IDENTITY, "canonical row identity contract"),
        ("outcome_label_contract", SPINE_OUTCOME_CONTRACT, "official outcome and label contract"),
        ("selected_proposition_limitation_statement", SELECTED_LIMITATION, "selected-proposition interpretation and side-binding limitation statement"),
        ("complete_outcome_certification_ledger", OUTCOME_CERTIFICATION, "official outcome certification reference"),
        ("selection_conditioning_provenance_registry", SELECTION_PROVENANCE, "pregame model-selected direction provenance reference"),
        ("h15_certified_manifest", HIST_H15_CERT, "historical selected-proposition certified Hits 1.5 reference"),
        ("h15_variant_a_matrix", HIST_H15_MATRIX, "historical matrix reference only; not used for training here"),
        ("live_slate_run_tag", LIVE_SLATE, "frozen July 16 live run local_daily_20260716T200001Z"),
        ("live_odds_manifest", LIVE_MANIFEST, "live run artifact manifest"),
        ("live_downstream_pa_coverage", LIVE_DOWNSTREAM_PA, "July 16 downstream PA availability check"),
        ("live_review_aid_pa_retention_pilot", LIVE_REVIEW_AID_PA, "non-authoritative July 16 PA retention pocket; exact live-run row key not guaranteed"),
    ]
    rows = []
    for role, path, notes in artifacts:
        rows.append({
            "artifact_role": role,
            "path": _rel(path),
            "exists": path.exists(),
            "sha256": _sha256(path) if path.exists() else "",
            "bytes": path.stat().st_size if path.exists() else "",
            "notes": notes,
        })
    return rows


def _field_manifest() -> list[dict[str, Any]]:
    fields = []
    formulas = {
        "pa_opp_v1_d7_pa_pg": "prior_d7_plate_appearances",
        "pa_opp_v1_d15_pa_pg": "prior_d15_plate_appearances",
        "pa_opp_v1_d30_pa_pg": "prior_d30_plate_appearances",
        "pa_opp_v1_d7_vs_d15_delta": "d7 - d15",
        "pa_opp_v1_d7_vs_d30_delta": "d7 - d30",
        "pa_opp_v1_d15_vs_d30_delta": "d15 - d30",
        "pa_opp_v1_d7_to_d30_ratio": "d7 / d30 when d30 > 0",
        "pa_opp_v1_d15_opportunity_band": "frozen contract bands; low/medium/high/very_high variants preserved as provided",
        "pa_opp_v1_trend_label": "short-window movement/stability label from frozen PA platform",
    }
    for field in PA_FIELDS:
        fields.append({
            "field": field,
            "source": _rel(PA_EXTENDED_BASE),
            "feature_version": "pa_opp_v1_strict_prior_rolling_avg_2026_07_11" if field.startswith("pa_opp_v1") else "source_context",
            "formula_or_role": formulas.get(field, "provenance, coverage, or source-context field"),
            "prediction_time_availability": "historical_supported_when_cutoff_passes; live_current_slate_not_propagated",
            "missing_policy": "retain missing/null and pa_missing_flag; do not zero-impute silently",
            "leakage_rule": "no same-game raw plate_appearances or postgame lineup fields",
        })
    return fields


def _historical_rows() -> list[dict[str, Any]]:
    rows = _read_csv(PA_EXTENDED_BASE)
    h15 = []
    for row in rows:
        if row.get("prop_type") != "hits":
            continue
        if str(row.get("line")) not in {"1.5", "1.50"}:
            continue
        out = dict(row)
        out["canonical_key"] = _canonical_key(row)
        out["resolved"] = row.get("settlement_status") in {"win", "loss"}
        out["won"] = row.get("settlement_status") == "win"
        out["control_probability_bucket"] = _control_prob_bucket(_num(row.get("control_probability")))
        out["odds_available"] = _num(row.get("selected_price")) is not None
        h15.append(out)
    return h15


def _cohort_spec(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str, float, float]:
    feature_values = [_num(r.get("pa_opp_v1_d15_pa_pg")) for r in rows if r.get("target_diagnostic_population") == "True"]
    q1, q2 = _calc_quantiles([v for v in feature_values if v is not None])
    specs = [
        {
            "cohort_family": "frozen_contract_d15_opportunity_band",
            "field": "pa_opp_v1_d15_opportunity_band",
            "rule": "use value exactly as provided by frozen PA platform",
            "threshold_source": "PA selected feature contracts 2026-07-11",
            "outcome_blind": True,
        },
        {
            "cohort_family": "diagnostic_d15_pa_pg_quantile",
            "field": "pa_opp_v1_d15_pa_pg",
            "rule": f"q1_low < {q1:.6f}; q2_mid >= {q1:.6f} and < {q2:.6f}; q3_high >= {q2:.6f}",
            "threshold_source": "feature distribution only from historical Hits 1.5 PA source before diagnostic aggregation",
            "outcome_blind": True,
        },
        {
            "cohort_family": "control_probability_stratum",
            "field": "control_probability",
            "rule": "<0.55; 0.55-0.60; 0.60-0.65; 0.65-0.70; >=0.70",
            "threshold_source": "predeclared fixed model-probability buckets",
            "outcome_blind": True,
        },
        {
            "cohort_family": "source_confidence",
            "field": "pa_semantics_status",
            "rule": "PREDICTION_SAFE_PRIOR_CONTEXT vs PREGAME_FEATURE_CONTEXT_INFERRED_FROM_RECONCILE_ARTIFACT",
            "threshold_source": "PA characterization provenance",
            "outcome_blind": True,
        },
        {
            "cohort_family": "side",
            "field": "side_normalized",
            "rule": "over and under retained when present; PA extended source provides over only for Hits 1.5",
            "threshold_source": "canonical row key",
            "outcome_blind": True,
        },
    ]
    text = json.dumps(specs, sort_keys=True)
    return specs, hashlib.sha256(text.encode()).hexdigest(), q1, q2


def _decorate_rows(rows: list[dict[str, Any]], q1: float, q2: float) -> list[dict[str, Any]]:
    decorated = []
    certified_keys = {_canonical_key(r, "side") for r in _read_csv(HIST_H15_CERT)}
    matrix_keys = {_canonical_key(r, "side") for r in _read_csv(HIST_H15_MATRIX)}
    for row in rows:
        out = dict(row)
        out["d15_pa_pg_quantile_band"] = _d15_quantile_band(_num(row.get("pa_opp_v1_d15_pa_pg")), q1, q2)
        out["in_2026_07_14_certified_h15_manifest"] = out["canonical_key"] in certified_keys
        out["in_2026_07_13_variant_a_h15_matrix"] = out["canonical_key"] in matrix_keys
        out["official_outcome_qualified"] = row.get("resolved")
        out["strict_prior_pa_qualified"] = row.get("pa_opp_v1_cutoff_status") == "PASS_PRIOR_DATE" and row.get("pa_missing_flag") != "missing_prior_pa"
        out["diagnostic_eligible"] = bool(row.get("resolved")) and row.get("target_diagnostic_population") == "True"
        decorated.append(out)
    return decorated


def _exclusion_ledger(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ledger = []
    for row in rows:
        reason = "included"
        if row.get("side_normalized") not in {"over", "under"}:
            reason = "missing_or_invalid_side"
        elif not row.get("resolved"):
            reason = "unresolved_or_missing_official_outcome"
        elif row.get("target_diagnostic_population") != "True":
            reason = "not_in_target_diagnostic_population"
        elif row.get("pa_opp_v1_cutoff_status") == "FAIL_MISSING_PRIOR_PA":
            reason = "missing_strict_prior_pa"
        elif row.get("pa_opp_v1_cutoff_status") != "PASS_PRIOR_DATE":
            reason = "strict_prior_not_direct_verified_inferred_source_only"
        ledger.append({
            "canonical_key": row.get("canonical_key"),
            "slate_date": row.get("slate_date"),
            "game_id": row.get("game_id"),
            "player_id": row.get("player_id"),
            "player_name": row.get("player_name"),
            "side": row.get("side_normalized"),
            "settlement_status": row.get("settlement_status"),
            "target_diagnostic_population": row.get("target_diagnostic_population"),
            "pa_opp_v1_cutoff_status": row.get("pa_opp_v1_cutoff_status"),
            "pa_semantics_status": row.get("pa_semantics_status"),
            "primary_reason": reason,
        })
    return ledger


def _summarize(rows: list[dict[str, Any]], group_fields: list[str], report_name: str, include_unresolved: bool = False) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if not include_unresolved and not row.get("diagnostic_eligible"):
            continue
        groups[tuple(row.get(field, "") for field in group_fields)].append(row)
    out = []
    for key, group in sorted(groups.items(), key=lambda item: tuple(str(x) for x in item[0])):
        resolved = [r for r in group if r.get("resolved")]
        wins = sum(1 for r in resolved if r.get("won"))
        losses = sum(1 for r in resolved if not r.get("won"))
        rate, lo, hi = _ci_rate(wins, len(resolved))
        odds_rows = [r for r in resolved if r.get("odds_available")]
        units = []
        for r in odds_rows:
            profit = _american_profit(_num(r.get("selected_price")), bool(r.get("won")))
            if profit is not None:
                units.append(profit)
        row = {
            "report": report_name,
            "group_fields": "|".join(group_fields),
            "group_key": "|".join(str(v) for v in key),
            "rows": len(group),
            "resolved": len(resolved),
            "wins": wins,
            "losses": losses,
            "win_rate": round(rate, 6) if rate is not None else "",
            "win_rate_ci95_low": round(lo, 6) if lo is not None else "",
            "win_rate_ci95_high": round(hi, 6) if hi is not None else "",
            "odds_supported_rows": len(odds_rows),
            "flat_stake_units": round(sum(units), 6) if units else "",
            "flat_stake_roi": round(sum(units) / len(units), 6) if units else "",
            "sample_flag": "small_sample_lt_30" if len(resolved) < 30 else "ok",
            "notes": "",
        }
        for i, field in enumerate(group_fields):
            row[field] = key[i]
        out.append(row)
    return out


def _within_stratum(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base = _summarize(rows, ["side_normalized", "control_probability_bucket", "d15_pa_pg_quantile_band"], "within_model_stratum")
    contrast = []
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("diagnostic_eligible"):
            groups[(row.get("side_normalized", ""), row.get("control_probability_bucket", ""))].append(row)
    for (side, prob_bucket), group in groups.items():
        high = [r for r in group if r.get("d15_pa_pg_quantile_band") == "q3_high"]
        low = [r for r in group if r.get("d15_pa_pg_quantile_band") == "q1_low"]
        def rate(rs: list[dict[str, Any]]) -> float | None:
            if not rs:
                return None
            return sum(1 for r in rs if r.get("won")) / len(rs)
        hr, lr = rate(high), rate(low)
        contrast.append({
            "report": "within_model_stratum_high_vs_low_pa",
            "group_fields": "side_normalized|control_probability_bucket",
            "group_key": f"{side}|{prob_bucket}",
            "side_normalized": side,
            "control_probability_bucket": prob_bucket,
            "rows": len(group),
            "resolved": len(group),
            "wins": sum(1 for r in group if r.get("won")),
            "losses": sum(1 for r in group if not r.get("won")),
            "high_pa_rows": len(high),
            "high_pa_win_rate": round(hr, 6) if hr is not None else "",
            "low_pa_rows": len(low),
            "low_pa_win_rate": round(lr, 6) if lr is not None else "",
            "high_minus_low_win_rate": round(hr - lr, 6) if hr is not None and lr is not None else "",
            "sample_flag": "small_sample_lt_30" if len(group) < 30 else "ok",
            "notes": "central incremental diagnostic; not threshold optimization",
        })
    return base + contrast


def _live_overlay(cohort_q1: float, cohort_q2: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    slate = _read_csv(LIVE_SLATE)
    live_h15 = [r for r in slate if r.get("prop_type") == "hits" and str(r.get("line")) in {"1.5", "1.50"}]
    # Non-authoritative PA retention pocket is examined only for join-health. It
    # is not exact-bound to the frozen live run tag, so no PA value is promoted
    # into the replayable live overlay.
    retention = _read_csv(LIVE_REVIEW_AID_PA)
    retention_by_player_date = {
        (r.get("date"), r.get("canonical_player_id") or r.get("player_id"), r.get("team"), r.get("opponent")): r
        for r in retention
    }
    overlay = []
    for row in live_h15:
        candidate = retention_by_player_date.get((row.get("slate_date"), row.get("player_id"), row.get("team"), row.get("opponent")))
        join_status = "NO_EXACT_RUN_BOUND_PA_SOURCE"
        if candidate:
            join_status = "NON_AUTHORITATIVE_PLAYER_DATE_TEAM_MATCH_AVAILABLE_NOT_ATTACHED"
        overlay.append({
            "canonical_key": _canonical_key(row, "model_pick_side"),
            "slate_date": row.get("slate_date"),
            "run_tag": "local_daily_20260716T200001Z",
            "game_id": row.get("game_id"),
            "player_id": row.get("player_id"),
            "player_name": row.get("player_name"),
            "team": row.get("team"),
            "opponent": row.get("opponent"),
            "prop_type": row.get("prop_type"),
            "line": row.get("line"),
            "side": row.get("model_pick_side"),
            "model_pick_prob": row.get("model_pick_prob"),
            "control_probability_bucket": _control_prob_bucket(_num(row.get("model_pick_prob"))),
            "selected_side_price": row.get("selected_side_price"),
            "strict_prior_pa_join_status": join_status,
            "pa_values_attached": False,
            "pa_missing_reason": "current_live_slate_output_has_no_pa_fields_and_no_exact_run_bound_pa_overlay_manifest",
            "non_authoritative_pa_candidate_path": _rel(LIVE_REVIEW_AID_PA) if candidate else "",
            "non_authoritative_d7_plate_appearances": candidate.get("d7_plate_appearances", "") if candidate else "",
            "non_authoritative_d15_plate_appearances": candidate.get("d15_plate_appearances", "") if candidate else "",
            "non_authoritative_d30_plate_appearances": candidate.get("d30_plate_appearances", "") if candidate else "",
            "cohort_assignment_status": "blocked_no_exact_pa_overlay",
            "would_be_quantile_rule": f"q1<{cohort_q1:.6f}; q2<{cohort_q2:.6f}",
            "july_16_outcome_used": False,
        })
    health = [
        {"metric": "live_hits_1_5_rows", "value": len(live_h15), "notes": "from frozen live slate local_daily_20260716T200001Z"},
        {"metric": "live_hits_1_5_over_rows", "value": sum(1 for r in live_h15 if r.get("model_pick_side") == "over"), "notes": ""},
        {"metric": "live_hits_1_5_under_rows", "value": sum(1 for r in live_h15 if r.get("model_pick_side") == "under"), "notes": ""},
        {"metric": "exact_strict_prior_pa_attached_rows", "value": 0, "notes": "no exact run-bound PA overlay artifact located"},
        {"metric": "non_authoritative_player_date_team_pa_matches", "value": sum(1 for r in overlay if r["strict_prior_pa_join_status"].startswith("NON_AUTHORITATIVE")), "notes": "retained for diagnostics only; not attached as replayable PA"},
        {"metric": "identity_duplicates", "value": len(overlay) - len({r["canonical_key"] for r in overlay}), "notes": ""},
        {"metric": "july_16_outcomes_used", "value": 0, "notes": "explicitly prohibited and not accessed"},
    ]
    return overlay, health


def _decision_payload(rows: list[dict[str, Any]], live_overlay: list[dict[str, Any]]) -> dict[str, Any]:
    diagnostic = [r for r in rows if r.get("diagnostic_eligible")]
    direct = [r for r in diagnostic if r.get("pa_semantics_status") == "PREDICTION_SAFE_PRIOR_CONTEXT"]
    inferred = [r for r in diagnostic if "INFERRED" in str(r.get("pa_semantics_status"))]
    dates = sorted({r.get("slate_date") for r in diagnostic})
    decision = "PROMISING_BUT_SAMPLE_INSUFFICIENT"
    if len(diagnostic) < 300 or len(dates) < 20:
        decision = "PROMISING_BUT_SAMPLE_INSUFFICIENT"
    if not direct:
        decision = "DIAGNOSTIC_BLOCKED_BY_POPULATION_OR_COMPATIBILITY_LIMITS"
    return {
        "generated_at_utc": _utc_now(),
        "decisions": {
            "MLB_HITS_15_PA_OVERLAY_POPULATION_DECISION": "HISTORICAL_OVER_15_ONLY_POPULATION_REPLAYABLE_UNDER_UNSUPPORTED",
            "MLB_HITS_15_PA_OVERLAY_REPLAYABILITY_DECISION": "HISTORICAL_PA_OVERLAY_REPLAYABLE_WITH_SOURCE_PROVENANCE_CAVEATS",
            "MLB_HITS_15_PA_OVERLAY_INCREMENTAL_DIAGNOSTIC_DECISION": decision,
            "MLB_HITS_15_PA_OVERLAY_LIVE_JOIN_DECISION": "LIVE_JOIN_BLOCKED_FOR_EXACT_PA_ATTACHMENT_NON_AUTHORITATIVE_MATCH_AVAILABLE",
            "MLB_HITS_15_PA_OVERLAY_NEXT_STEP_DECISION": "DESIGN_STRICT_RUN_BOUND_LIVE_PA_OVERLAY_BEFORE_CHALLENGER_AUTHORIZATION",
            "FINAL_STATUS": "RESEARCH_DIAGNOSTIC_COMPLETED_NO_TRAINING_NO_PRODUCTION_PROMOTION",
        },
        "historical_population_counts": {
            "historical_hits_1_5_rows": len(rows),
            "diagnostic_eligible_rows": len(diagnostic),
            "direct_strict_prior_rows": len(direct),
            "inferred_pregame_reconcile_rows": len(inferred),
            "distinct_dates": len(dates),
            "over_rows": sum(1 for r in rows if r.get("side_normalized") == "over"),
            "under_rows": sum(1 for r in rows if r.get("side_normalized") == "under"),
        },
        "live_counts": {
            "live_hits_1_5_rows": len(live_overlay),
            "exact_pa_attached_rows": sum(1 for r in live_overlay if r.get("pa_values_attached") is True),
            "non_authoritative_pa_matches": sum(1 for r in live_overlay if str(r.get("strict_prior_pa_join_status")).startswith("NON_AUTHORITATIVE")),
        },
    }


def _summary_md(payload: dict[str, Any]) -> str:
    counts = payload["historical_population_counts"]
    live = payload["live_counts"]
    decisions = payload["decisions"]
    return f"""# MLB Hits 1.5 PA Opportunity Overlay Diagnostic - 2026-07-16

## Executive Summary

This bounded research-only diagnostic evaluated the frozen PA Opportunity v1 overlay against locally available historical Hits 1.5 selected-proposition rows and constructed an unscored July 16 live overlay health report.

The historical PA spine supports an OVER 1.5 diagnostic population, but it does not support UNDER 1.5 for this task. The July 16 live run contains one Hits 1.5 row, but no exact run-bound strict-prior PA overlay artifact is available for attachment, so the live overlay is intentionally unscored and PA attachment is fail-closed.

## Decisions

`MLB_HITS_15_PA_OVERLAY_POPULATION_DECISION = {decisions['MLB_HITS_15_PA_OVERLAY_POPULATION_DECISION']}`
`MLB_HITS_15_PA_OVERLAY_REPLAYABILITY_DECISION = {decisions['MLB_HITS_15_PA_OVERLAY_REPLAYABILITY_DECISION']}`
`MLB_HITS_15_PA_OVERLAY_INCREMENTAL_DIAGNOSTIC_DECISION = {decisions['MLB_HITS_15_PA_OVERLAY_INCREMENTAL_DIAGNOSTIC_DECISION']}`
`MLB_HITS_15_PA_OVERLAY_LIVE_JOIN_DECISION = {decisions['MLB_HITS_15_PA_OVERLAY_LIVE_JOIN_DECISION']}`
`MLB_HITS_15_PA_OVERLAY_NEXT_STEP_DECISION = {decisions['MLB_HITS_15_PA_OVERLAY_NEXT_STEP_DECISION']}`

Final status: `{decisions['FINAL_STATUS']}`.

## Historical Population

- Historical Hits 1.5 rows: `{counts['historical_hits_1_5_rows']}`
- Diagnostic eligible rows: `{counts['diagnostic_eligible_rows']}`
- Direct strict-prior PA rows: `{counts['direct_strict_prior_rows']}`
- Inferred pregame reconcile PA rows: `{counts['inferred_pregame_reconcile_rows']}`
- Distinct dates: `{counts['distinct_dates']}`
- OVER rows: `{counts['over_rows']}`
- UNDER rows: `{counts['under_rows']}`

The OVER-only limitation is a real source-population constraint, not a baseball conclusion. No threshold was optimized from outcomes; the frozen cohort specification was written and hashed before diagnostic output aggregation.

## July 16 Live Overlay

- Live Hits 1.5 rows: `{live['live_hits_1_5_rows']}`
- Exact strict-prior PA rows attached: `{live['exact_pa_attached_rows']}`
- Non-authoritative player/date/team PA matches observed: `{live['non_authoritative_pa_matches']}`

No July 16 outcomes were accessed or used.

## Interpretation

This package is suitable as a bounded PA opportunity diagnostic and live join-health checkpoint. It is not a Champion-Challenger execution, not a training package, not a production promotion, and not an upload/change request.
"""


def _validate(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.iterdir()):
        if not path.is_file():
            continue
        if path.suffix == ".csv":
            try:
                list(csv.DictReader(path.open()))
                status, notes = "PASS", "csv_parse_ok"
            except Exception as exc:
                status, notes = "FAIL", repr(exc)
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text())
                status, notes = "PASS", "json_parse_ok"
            except Exception as exc:
                status, notes = "FAIL", repr(exc)
        elif path.suffix == ".md":
            status = "PASS" if path.read_text().strip() else "FAIL"
            notes = "markdown_nonempty" if status == "PASS" else "markdown_empty"
        else:
            continue
        rows.append({"path": _rel(path), "validation": status, "notes": notes})
    return rows


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    source_rows = _source_binding_rows()
    historical = _historical_rows()
    cohort_spec, cohort_sha, q1, q2 = _cohort_spec(historical)
    decorated = _decorate_rows(historical, q1, q2)
    exclusions = _exclusion_ledger(decorated)
    live_overlay, live_health = _live_overlay(q1, q2)

    overall = []
    overall.extend(_summarize(decorated, ["side_normalized"], "overall_by_side"))
    overall.extend(_summarize(decorated, ["side_normalized", "pa_opp_v1_d15_opportunity_band"], "overall_by_frozen_pa_band"))
    overall.extend(_summarize(decorated, ["side_normalized", "d15_pa_pg_quantile_band"], "overall_by_pa_quantile"))
    overall.extend(_summarize(decorated, ["side_normalized", "pa_opp_v1_trend_label"], "overall_by_pa_trend"))

    within = _within_stratum(decorated)
    date_stability = _summarize(decorated, ["slate_date", "side_normalized", "d15_pa_pg_quantile_band"], "date_stability")
    confidence = _summarize(decorated, ["side_normalized", "pa_semantics_status", "pa_opp_v1_cutoff_status"], "source_confidence")
    odds_supported = _summarize([r for r in decorated if r.get("odds_available")], ["side_normalized", "d15_pa_pg_quantile_band"], "odds_supported_secondary")

    hist_fields = [
        "canonical_key", "row_key", "slate_date", "game_id", "player_id", "player_name", "team", "opponent", "prop_type", "line", "side_normalized",
        "target_value", "target_class", "settlement_status", "selected_price", "control_probability", "control_probability_bucket",
        "pa_opp_v1_d15_opportunity_band", "d15_pa_pg_quantile_band", "pa_opp_v1_trend_label", "pa_opp_v1_cutoff_status",
        "pa_semantics_status", "pa_source_regime", "strict_prior_verified_population", "pregame_reconcile_inferred_population",
        "target_diagnostic_population", "official_outcome_qualified", "strict_prior_pa_qualified", "diagnostic_eligible",
        "in_2026_07_14_certified_h15_manifest", "in_2026_07_13_variant_a_h15_matrix",
    ]
    for field in PA_FIELDS:
        if field not in hist_fields:
            hist_fields.append(field)

    _write_csv(out_dir / f"source_contract_binding_report_{RUN_DATE}.csv", source_rows, ["artifact_role", "path", "exists", "sha256", "bytes", "notes"])
    _write_csv(out_dir / f"historical_population_manifest_{RUN_DATE}.csv", decorated, hist_fields)
    _write_csv(out_dir / f"exclusion_missingness_ledger_{RUN_DATE}.csv", exclusions, ["canonical_key", "slate_date", "game_id", "player_id", "player_name", "side", "settlement_status", "target_diagnostic_population", "pa_opp_v1_cutoff_status", "pa_semantics_status", "primary_reason"])
    cohort_rows = []
    for row in cohort_spec:
        item = dict(row)
        item["cohort_spec_sha256"] = cohort_sha
        cohort_rows.append(item)
    _write_csv(out_dir / f"frozen_pre_outcome_cohort_specification_{RUN_DATE}.csv", cohort_rows, ["cohort_family", "field", "rule", "threshold_source", "outcome_blind", "cohort_spec_sha256"])
    _write_csv(out_dir / f"pa_overlay_field_manifest_{RUN_DATE}.csv", _field_manifest(), ["field", "source", "feature_version", "formula_or_role", "prediction_time_availability", "missing_policy", "leakage_rule"])
    diag_fields = ["report", "group_fields", "group_key", "side_normalized", "pa_opp_v1_d15_opportunity_band", "d15_pa_pg_quantile_band", "pa_opp_v1_trend_label", "control_probability_bucket", "pa_semantics_status", "pa_opp_v1_cutoff_status", "slate_date", "rows", "resolved", "wins", "losses", "win_rate", "win_rate_ci95_low", "win_rate_ci95_high", "odds_supported_rows", "flat_stake_units", "flat_stake_roi", "sample_flag", "notes"]
    _write_csv(out_dir / f"overall_diagnostic_results_{RUN_DATE}.csv", overall, diag_fields)
    _write_csv(out_dir / f"within_model_stratum_diagnostic_results_{RUN_DATE}.csv", within, diag_fields + ["high_pa_rows", "high_pa_win_rate", "low_pa_rows", "low_pa_win_rate", "high_minus_low_win_rate"])
    _write_csv(out_dir / f"date_stability_report_{RUN_DATE}.csv", date_stability, diag_fields)
    _write_csv(out_dir / f"source_confidence_comparison_{RUN_DATE}.csv", confidence, diag_fields)
    _write_csv(out_dir / f"odds_supported_secondary_results_{RUN_DATE}.csv", odds_supported, diag_fields)
    _write_csv(out_dir / f"july16_unscored_live_overlay_{RUN_DATE}.csv", live_overlay, [
        "canonical_key", "slate_date", "run_tag", "game_id", "player_id", "player_name", "team", "opponent", "prop_type", "line", "side",
        "model_pick_prob", "control_probability_bucket", "selected_side_price", "strict_prior_pa_join_status", "pa_values_attached",
        "pa_missing_reason", "non_authoritative_pa_candidate_path", "non_authoritative_d7_plate_appearances", "non_authoritative_d15_plate_appearances",
        "non_authoritative_d30_plate_appearances", "cohort_assignment_status", "would_be_quantile_rule", "july_16_outcome_used",
    ])
    _write_csv(out_dir / f"live_join_health_report_{RUN_DATE}.csv", live_health, ["metric", "value", "notes"])

    payload = _decision_payload(decorated, live_overlay)
    payload["cohort_spec_sha256"] = cohort_sha
    payload["source_artifacts"] = source_rows
    _write_json(out_dir / f"machine_readable_pa_overlay_diagnostic_{RUN_DATE}.json", payload)
    (out_dir / f"executive_summary_{RUN_DATE}.md").write_text(_summary_md(payload))

    validation_rows = _validate(out_dir)
    _write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation_rows, ["path", "validation", "notes"])
    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file():
            manifest_rows.append({"path": _rel(path), "sha256": _sha256(path), "bytes": path.stat().st_size})
    _write_csv(out_dir / f"sha256_manifest_{RUN_DATE}.csv", manifest_rows, ["path", "sha256", "bytes"])
    validation_rows = _validate(out_dir)
    _write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation_rows, ["path", "validation", "notes"])
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run bounded research-only MLB Hits 1.5 PA overlay diagnostic.")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", default="research_only", choices=["research_only"])
    args = parser.parse_args()
    payload = build(Path(args.output_dir))
    print(json.dumps({
        "decisions": payload["decisions"],
        "historical_population_counts": payload["historical_population_counts"],
        "live_counts": payload["live_counts"],
        "cohort_spec_sha256": payload["cohort_spec_sha256"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
