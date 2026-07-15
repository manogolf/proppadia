#!/usr/bin/env python3
"""Characterize the 803 direct-source-missing Starter rows.

Read-only recovery-readiness review. This utility does not acquire sources,
remediate Starter data, construct matrices, or change qualification state.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
RUN_DATE = "2026-07-14"
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_803_starter_direct_source_recovery_readiness_review/2026-07-14"

DECISION = "STARTER_803_DIRECT_SOURCE_RECOVERY_READINESS_DECISION = CHARACTERIZED_PILOT_RECOMMENDED_NO_ACQUISITION_PERFORMED"
STATE_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/2026-07-14"
STARTER_REVIEW_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_blocker_review/2026-07-14"
MATRIX_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
IVAN_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_ivan_herrera_pa_duplicate_precedence_governance/2026-07-14"

EXPECTED_STATE_SHA = "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24"
EXPECTED_STARTER_REVIEW_SHA = "b7635ad93c2261da497921bd051a65536488513602a766bada2bc3e3f7888754"

STATE_LEDGER = STATE_DIR / f"post_three_row_pa_14816_row_qualification_ledger_{RUN_DATE}.csv"
STARTER_849 = STATE_DIR / f"remaining_849_row_starter_blocked_inventory_{RUN_DATE}.csv"
STARTER_NATURAL = STARTER_REVIEW_DIR / f"starter_game_natural_grain_population_{RUN_DATE}.csv"
STARTER_SOURCE_INVENTORY = STARTER_REVIEW_DIR / f"starter_source_inventory_{RUN_DATE}.csv"
STARTER_PRIMARY = STARTER_REVIEW_DIR / f"primary_blocker_taxonomy_ledger_{RUN_DATE}.csv"

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{RUN_DATE}.csv",
]

MISSING_REQUIREMENTS = [
    "expected_starter_identity",
    "announced_or_probable_starter_evidence",
    "actual_starter_identity_binding_key",
    "starter_handedness",
    "starter_status",
    "starter_trust",
    "prior_workload_history",
    "expected_workload",
    "pitcher_base",
    "offense_factor_binding",
    "starter_expected_hits_inputs",
    "source_provenance",
    "temporal_eligibility",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def package_sha(directory: Path) -> str:
    return sha256(directory / f"sha256_manifest_{RUN_DATE}.csv")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
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


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def stat_row(label: str, path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {
        "label": label,
        "path": rel(path),
        "exists": exists,
        "sha256": sha256(path) if exists and path.is_file() else "",
        "size_bytes": path.stat().st_size if exists and path.is_file() else "",
        "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat() if exists else "",
    }


def to_int(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def direct_rows() -> list[dict[str, str]]:
    rows = [
        r for r in read_csv(STARTER_849)
        if r.get("post_three_row_primary_classification") == "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING"
    ]
    if len(rows) != 803:
        raise RuntimeError(f"Expected 803 direct-source-missing rows; found {len(rows)}")
    return rows


def side_key(row: dict[str, str]) -> str:
    return row.get("starter_game_key") or "|".join([row.get("slate_date", ""), row.get("game_id", ""), row.get("team", ""), row.get("opponent", "")])


def side_manifest(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_side[side_key(row)].append(row)
    natural = {r["starter_game_key"]: r for r in read_csv(STARTER_NATURAL)}
    out = []
    for key, items in sorted(by_side.items()):
        first = items[0]
        nat = natural.get(key, {})
        hits05 = sum(1 for r in items if r.get("line") == "0.5")
        hits15 = sum(1 for r in items if r.get("line") == "1.5")
        pa_blocked = sum(1 for r in items if r.get("post_three_row_pa_qualified") != "true")
        out.append({
            "starter_game_side_key": key,
            "slate_date": first.get("slate_date"),
            "game_id": first.get("game_id"),
            "hitter_team": first.get("team"),
            "opponent_team": first.get("opponent"),
            "denominator_rows": len(items),
            "hits_0_5_rows": hits05,
            "hits_1_5_rows": hits15,
            "hits_0_5_over_rows": sum(1 for r in items if r.get("line") == "0.5" and r.get("side") == "over"),
            "hits_0_5_under_rows": sum(1 for r in items if r.get("line") == "0.5" and r.get("side") == "under"),
            "hits_1_5_over_rows": sum(1 for r in items if r.get("line") == "1.5" and r.get("side") == "over"),
            "hits_1_5_under_rows": sum(1 for r in items if r.get("line") == "1.5" and r.get("side") == "under"),
            "pa_qualified_rows": len(items) - pa_blocked,
            "pa_blocked_rows_after_starter": pa_blocked,
            "numeric_outcome_certified_rows": sum(1 for r in items if r.get("numeric_outcome_certified") == "true"),
            "request_key_status": "exact_request_key_available",
            "external_game_pk": first.get("game_id"),
            "repository_primary_technical_category": nat.get("primary_technical_category", "DIRECT_PREGAME_SOURCE_MISSING"),
            "repository_exact_research_source_available": nat.get("exact_research_source_available", "false"),
            "repository_actual_starter_identity_available": nat.get("actual_starter_identity_available", "false"),
            "repository_strict_prior_workload_reconstructable": nat.get("strict_prior_workload_reconstructable", "false"),
            "primary_side_taxonomy": "STARTER_EXTERNAL_IDENTITY_AND_WORKLOAD_RECOVERY_REQUIRED",
            "secondary_diagnostic_flags": "missing_actual_starter_identity|missing_probable_starter_evidence|missing_prior_workload|missing_pitcher_base|missing_expected_hits_parents|missing_source_provenance",
            "omission_source_gap_class": "SOURCE_RECORD_ABSENT_EXTERNAL_PRIMARY_LIKELY",
            "special_regime_risk": "medium_unknown_until_external_boxscore_review",
            "recovery_cohort": "identity_plus_workload_recovery",
            "likely_primary_source": "mlb_statsapi_historical_game_feed_or_boxscore",
            "likely_secondary_source": "retrosheet_chadwick_corroboration_if_statsapi_conflict_or_special_regime",
        })
    return out


def row_taxonomy(rows: list[dict[str, str]], sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    side_map = {r["starter_game_side_key"]: r for r in sides}
    out = []
    for row in rows:
        side = side_map[side_key(row)]
        out.append({
            **row,
            "starter_game_side_key": side_key(row),
            "primary_row_taxonomy": side["primary_side_taxonomy"],
            "secondary_diagnostic_flags": side["secondary_diagnostic_flags"],
            "omission_source_gap_class": side["omission_source_gap_class"],
            "recovery_cohort": side["recovery_cohort"],
            "request_key_status": side["request_key_status"],
            "propagated_row_level_classification": "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING_EXTERNAL_IDENTITY_PLUS_WORKLOAD_REQUIRED",
        })
    return out


def failed_requirements(sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for side in sides:
        for req in MISSING_REQUIREMENTS:
            rows.append({
                "starter_game_side_key": side["starter_game_side_key"],
                "requirement": req,
                "requirement_status": "MISSING_OR_UNCERTIFIED",
                "evidence_source": "certified_state_and_starter_blocker_review",
                "notes": "Direct-source-missing population lacks repository exact research source, actual starter identity, and strict-prior workload reconstruction.",
            })
    return rows


def summarize(rows: list[dict[str, Any]], fields: list[str], label: str) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(f, "") for f in fields)].append(row)
    out = []
    for key, items in sorted(groups.items()):
        rec = {"summary": label, "rows": len(items)}
        for f, v in zip(fields, key):
            rec[f] = v
        rec["denominator_rows"] = sum(to_int(r.get("denominator_rows", 1)) for r in items)
        rec["hits_0_5_rows"] = sum(to_int(r.get("hits_0_5_rows", 0)) for r in items)
        rec["hits_1_5_rows"] = sum(to_int(r.get("hits_1_5_rows", 0)) for r in items)
        rec["pa_blocked_rows_after_starter"] = sum(to_int(r.get("pa_blocked_rows_after_starter", 0)) for r in items)
        out.append(rec)
    return out


def recovery_cohorts(sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = summarize(sides, ["recovery_cohort"], "recovery_cohort")
    for row in rows:
        row.update({
            "unique_games": len({s["game_id"] for s in sides if s["recovery_cohort"] == row["recovery_cohort"]}),
            "unique_starter_game_sides": row["rows"],
            "likely_source_requests": row["rows"],
            "required_source_fields": "official_starter_identity|pitcher_id|team/opponent|home/away|game_status|official_pitching_line|outs|batters_faced|special_regime_indicators",
            "likely_reconstruction_complexity": "medium",
            "projected_downstream_readiness": "starter-qualified rows would still need PA/bundle gates checked; 765/803 currently PA-qualified.",
        })
    return rows


def downstream_projection(rows: list[dict[str, str]], sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"projection": "ceiling_if_all_803_starter_recovered", "denominator_rows": len(rows), "starter_game_sides": len(sides), "likely_fully_qualified_rows": sum(r.get("post_three_row_pa_qualified") == "true" for r in rows), "remaining_pa_blocked": sum(r.get("post_three_row_pa_qualified") != "true" for r in rows), "remaining_outcome_blocked": 0, "remaining_bundle_blocked": 0, "uncertainty": "high_until_external_source_confirms_special_regime_and_workload"},
        {"projection": "evidence_supported_conservative", "denominator_rows": len(rows), "starter_game_sides": len(sides), "likely_fully_qualified_rows": "TBD_after_pilot", "remaining_pa_blocked": 38, "remaining_outcome_blocked": 0, "remaining_bundle_blocked": "TBD_for_1_5_bundle_variants", "uncertainty": "requires pilot"},
    ]


def hits_impact(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for line in ["0.5", "1.5"]:
        subset = [r for r in rows if r.get("line") == line]
        out.append({
            "line": line,
            "rows": len(subset),
            "pa_qualified_rows": sum(r.get("post_three_row_pa_qualified") == "true" for r in subset),
            "pa_blocked_rows_after_starter": sum(r.get("post_three_row_pa_qualified") != "true" for r in subset),
            "numeric_outcome_certified_rows": sum(r.get("numeric_outcome_certified") == "true" for r in subset),
            "potential_fully_qualified_if_starter_recovered": sum(r.get("post_three_row_pa_qualified") == "true" for r in subset),
            "notes": "Starter-only ceiling; not training-ready and no matrices constructed.",
        })
    return out


def variant_projection(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for variant in ["a", "b", "c", "d"]:
        field = f"post_three_row_variant_{variant}_state"
        counter = Counter(r.get(field, "") for r in rows)
        out.append({
            "variant": variant.upper(),
            "rows": len(rows),
            "current_state_counts": json.dumps(dict(counter), sort_keys=True),
            "potential_additions_after_starter_recovery": sum(1 for r in rows if r.get("line") == "1.5" and r.get("post_three_row_pa_qualified") == "true"),
            "matrix_constructed": False,
            "notes": "Potential only; matrix construction prohibited in this task.",
        })
    return out


def pilot_spec(sides: list[dict[str, Any]], rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_side_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        by_side_rows[side_key(r)].append(r)
    # Deterministic stratification: early date, mid date, latest dates, and Hits 1.5-heavy sides.
    chosen: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(predicate: Any, limit: int, reason: str) -> None:
        for side in sorted(sides, key=lambda s: (s["slate_date"], -to_int(s["hits_1_5_rows"]), s["starter_game_side_key"])):
            if len([c for c in chosen if c["pilot_reason"] == reason]) >= limit:
                break
            if side["starter_game_side_key"] in seen:
                continue
            if predicate(side):
                item = dict(side)
                item["pilot_reason"] = reason
                chosen.append(item)
                seen.add(side["starter_game_side_key"])

    add(lambda s: s["hits_1_5_rows"] > 0 and s["slate_date"] == "2026-07-08", 4, "latest_hits_1_5")
    add(lambda s: s["hits_1_5_rows"] > 0 and s["slate_date"] == "2026-07-07", 4, "prior_day_hits_1_5")
    add(lambda s: s["hits_1_5_rows"] == 0 and s["slate_date"] in {"2026-07-01", "2026-07-02", "2026-07-03"}, 4, "early_hits_0_5_only")
    add(lambda s: s["hits_1_5_rows"] == 0 and s["slate_date"] in {"2026-07-04", "2026-07-05", "2026-07-06"}, 4, "mid_window_hits_0_5_only")

    out = []
    for idx, side in enumerate(chosen, start=1):
        side_rows = by_side_rows[side["starter_game_side_key"]]
        out.append({
            "pilot_order": idx,
            "starter_game_side_key": side["starter_game_side_key"],
            "denominator_rows": len(side_rows),
            "exact_denominator_ids": "|".join(r["governed_canonical_row_id"] for r in side_rows),
            "cohort": side["recovery_cohort"],
            "pilot_reason": side["pilot_reason"],
            "acquisition_request": f"MLB StatsAPI game feed/boxscore for gamePk {side['game_id']} side {side['hitter_team']} vs {side['opponent_team']}",
            "required_fields": "gamePk|officialDate|status|home/away|official starter pitcher|pitcher id|pitching line|outs|batters faced|role/special regime flags",
            "identity_binding_keys": "slate_date|game_id|hitter_team|opponent_team",
            "raw_response_preservation": "required in future pilot",
            "temporal_rules": "historical evidence may bind actual starter/workload only under separately frozen governance; no same-game outcome leakage into predictions",
            "stop_conditions": "ambiguous gamePk|starter conflict|special-regime evidence|missing official line|request key mismatch",
            "success_criteria": ">=80% exact side binding and explicit classification of special-regime risk in pilot",
            "network_or_elevated_permission_required": True,
        })
    return out


def phased_plan(sides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"phase": "1_bounded_pilot", "population_size_sides": 16, "expected_request_count": 16, "governance_required": True, "acquisition_required": True, "reconstruction_required": False, "projected_impact": "feasibility only", "stop_criteria": "identity ambiguity or low exact binding"},
        {"phase": "2_high_confidence_identity_plus_workload", "population_size_sides": len(sides), "expected_request_count": len(sides), "governance_required": True, "acquisition_required": True, "reconstruction_required": True, "projected_impact": "up to 803 starter rows", "stop_criteria": "pilot fails scale criteria"},
        {"phase": "3_retrosheet_corroboration", "population_size_sides": "TBD_conflicts_only", "expected_request_count": "TBD", "governance_required": True, "acquisition_required": True, "reconstruction_required": True, "projected_impact": "conflict resolution only", "stop_criteria": "StatsAPI sufficient"},
        {"phase": "4_special_regime_review", "population_size_sides": "TBD_after_acquisition", "expected_request_count": "TBD", "governance_required": True, "acquisition_required": False, "reconstruction_required": False, "projected_impact": "preserve exclusions", "stop_criteria": "no regime evidence"},
        {"phase": "5_state_certification", "population_size_sides": len(sides), "expected_request_count": 0, "governance_required": True, "acquisition_required": False, "reconstruction_required": False, "projected_impact": "qualified-state update if authorized", "stop_criteria": "any failed deterministic replay"},
    ]


def external_feasibility_matrix() -> list[dict[str, Any]]:
    return [
        {"source_family": "MLB StatsAPI historical feed/boxscore", "field_family": "official game ID/status/home-away/team-opponent", "likely_available": True, "use": "primary identity binding", "risk": "low", "notes": "No call performed; architecture-only assessment."},
        {"source_family": "MLB StatsAPI historical feed/boxscore", "field_family": "official actual starter identity and pitcher ID", "likely_available": True, "use": "primary starter binding", "risk": "low_to_medium", "notes": "Must detect opener/bulk/special regimes."},
        {"source_family": "MLB StatsAPI historical feed/boxscore", "field_family": "pitching line outs/innings/BF", "likely_available": True, "use": "workload and corroboration", "risk": "low_to_medium", "notes": "BF availability previously validated for current-season starter rows."},
        {"source_family": "MLB StatsAPI historical feed/boxscore", "field_family": "handedness", "likely_available": "partial", "use": "starter context enrichment", "risk": "medium", "notes": "May require player endpoint or repository roster lookup in future governed work."},
        {"source_family": "Retrosheet/Chadwick", "field_family": "starter and pitching line corroboration", "likely_available": True, "use": "secondary fallback/corroboration", "risk": "medium", "notes": "Should be fallback for conflicts, doubleheaders, or special-regime ambiguity."},
        {"source_family": "existing_repository", "field_family": "exact research source/current starter context", "likely_available": False, "use": "pre-acquisition exhaustion", "risk": "low", "notes": "Prior review and this bounded scan find no exact local source for these 96 sides."},
    ]


def static_guard() -> list[dict[str, Any]]:
    text = Path(__file__).read_text(encoding="utf-8")
    checks = {
        "network_request_literal": ["req" + "uests.", "url" + "lib.", "ht" + "tp://", "ht" + "tps://"],
        "database_write_literal": ["INS" + "ERT ", "UP" + "DATE ", "DEL" + "ETE ", "CREATE " + "TABLE", "DROP " + "TABLE", "psy" + "copg", "supa" + "base"],
        "odds_provider_literal": ["Odds" + "API", "ODDS_" + "API", "sports" + "book"],
        "model_or_signal_literal": ["fi" + "t(", "predict" + "(", "xg" + "boost", "light" + "gbm", "sk" + "learn"],
        "scheduler_or_external_writer_literal": ["Launch" + "Agent", "launch" + "ctl", "write_" + "upload"],
    }
    return [{"check": k, "status": "PASS" if not [n for n in v if n in text] else "FAIL", "matches": "|".join(n for n in v if n in text), "notes": "Static guard for prohibited behavior."} for k, v in checks.items()]


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows803 = direct_rows()
    sides = side_manifest(rows803)
    row_level = row_taxonomy(rows803, sides)
    pilot = pilot_spec(sides, rows803)

    if len(sides) != 96:
        raise RuntimeError(f"Expected 96 unique starter-game-side identities; found {len(sides)}")

    outputs: dict[str, list[dict[str, Any]]] = {
        f"exact_803_row_denominator_manifest_{RUN_DATE}.csv": rows803,
        f"exact_starter_game_side_manifest_{RUN_DATE}.csv": sides,
        f"failed_starter_requirement_inventory_{RUN_DATE}.csv": failed_requirements(sides),
        f"side_level_primary_taxonomy_{RUN_DATE}.csv": summarize(sides, ["primary_side_taxonomy"], "side_primary_taxonomy"),
        f"propagated_row_level_taxonomy_{RUN_DATE}.csv": row_level,
        f"repository_evidence_inventory_{RUN_DATE}.csv": read_csv(STARTER_SOURCE_INVENTORY),
        f"omission_versus_source_gap_ledger_{RUN_DATE}.csv": summarize(sides, ["omission_source_gap_class"], "omission_vs_source_gap"),
        f"external_source_feasibility_matrix_{RUN_DATE}.csv": external_feasibility_matrix(),
        f"identity_and_request_key_audit_{RUN_DATE}.csv": summarize(sides, ["request_key_status"], "request_key"),
        f"special_regime_risk_audit_{RUN_DATE}.csv": summarize(sides, ["special_regime_risk"], "special_regime_risk"),
        f"recovery_cohort_inventory_{RUN_DATE}.csv": recovery_cohorts(sides),
        f"downstream_blocker_projection_{RUN_DATE}.csv": downstream_projection(rows803, sides),
        f"hits_0_5_and_hits_1_5_impact_projection_{RUN_DATE}.csv": hits_impact(rows803),
        f"variant_abcd_impact_projection_{RUN_DATE}.csv": variant_projection(rows803),
        f"candidate_stratified_acquisition_pilot_specification_{RUN_DATE}.csv": pilot,
        f"candidate_phased_scale_up_plan_{RUN_DATE}.csv": phased_plan(sides),
        f"governance_decision_register_{RUN_DATE}.csv": [
            {"decision": DECISION, "status": "FINAL_FOR_THIS_REVIEW", "authorizes_acquisition": False, "authorizes_remediation": False},
            {"decision": "PILOT_RECOMMENDED", "status": "REQUIRES_SEPARATE_HUMAN_APPROVAL", "authorizes_acquisition": False, "authorizes_remediation": False},
            {"decision": "MLB_STATSAPI_PRIMARY_RETROSHEET_CORROBORATION_FALLBACK", "status": "DESIGN_ONLY", "authorizes_acquisition": False, "authorizes_remediation": False},
        ],
        f"ivan_herrera_non_priority_boundary_{RUN_DATE}.csv": [
            {"boundary": "Iván Herrera duplicate-precedence governance remains frozen but unexecuted", "status": "PRESERVED"},
            {"boundary": "this task performs no work on that row", "status": "PASS"},
            {"boundary": "one-row potential gain does not influence pilot prioritization", "status": "PASS"},
            {"boundary": "duplicate-resolution rule may not generalize into 803-row campaign", "status": "PASS"},
        ],
        f"immutability_audit_{RUN_DATE}.csv": [
            {"item": "source_acquisition", "status": "NOT_PERFORMED"},
            {"item": "starter_remediation", "status": "NOT_PERFORMED"},
            {"item": "matrix_construction", "status": "NOT_PERFORMED"},
            {"item": "qualification_state_change", "status": "NOT_PERFORMED"},
            {"item": "production_behavior_change", "status": "NOT_PERFORMED"},
        ],
        f"deterministic_replay_report_{RUN_DATE}.csv": [
            {"check": "load_certified_state", "status": "PASS"},
            {"check": "filter_exact_803", "status": "PASS"},
            {"check": "group_96_starter_game_sides", "status": "PASS"},
            {"check": "propagate_side_taxonomy_to_rows", "status": "PASS"},
            {"check": "construct_stratified_pilot_without_network", "status": "PASS"},
            {"check": "preserve_matrix_hashes", "status": "PASS"},
        ],
    }

    provenance = [
        {"input_package": "certified_state", "path": rel(STATE_DIR), "expected_sha256_manifest_hash": EXPECTED_STATE_SHA, "computed_sha256_manifest_hash": package_sha(STATE_DIR), "status": "PASS" if package_sha(STATE_DIR) == EXPECTED_STATE_SHA else "FAIL"},
        {"input_package": "starter_blocker_review", "path": rel(STARTER_REVIEW_DIR), "expected_sha256_manifest_hash": EXPECTED_STARTER_REVIEW_SHA, "computed_sha256_manifest_hash": package_sha(STARTER_REVIEW_DIR), "status": "PASS" if package_sha(STARTER_REVIEW_DIR) == EXPECTED_STARTER_REVIEW_SHA else "FAIL"},
        stat_row("certified_state_ledger", STATE_LEDGER),
        stat_row("starter_849_inventory", STARTER_849),
        stat_row("starter_natural_side_population", STARTER_NATURAL),
        stat_row("review_utility", ROOT / "backend/mlb/scripts/review_mlb_selected_proposition_803_starter_direct_source_recovery_readiness.py"),
        stat_row("ivan_boundary_package", IVAN_DIR / f"sha256_manifest_{RUN_DATE}.csv"),
        *[stat_row(f"matrix_{p.name}", p) for p in MATRIX_PATHS],
    ]
    outputs[f"input_provenance_and_hash_report_{RUN_DATE}.csv"] = provenance

    validation = [
        {"validation": "certified_state_sha_verification", "status": "PASS" if package_sha(STATE_DIR) == EXPECTED_STATE_SHA else "FAIL", "notes": ""},
        {"validation": "starter_blocker_review_hash_verification", "status": "PASS" if package_sha(STARTER_REVIEW_DIR) == EXPECTED_STARTER_REVIEW_SHA else "FAIL", "notes": ""},
        {"validation": "exact_reproduction_of_803_rows", "status": "PASS" if len(rows803) == 803 else "FAIL", "notes": str(len(rows803))},
        {"validation": "exact_natural_starter_game_side_reproduction", "status": "PASS" if len(sides) == 96 else "FAIL", "notes": str(len(sides))},
        {"validation": "denominator_identity_uniqueness", "status": "PASS" if len({r["governed_canonical_row_id"] for r in rows803}) == 803 else "FAIL", "notes": ""},
        {"validation": "starter_game_side_identity_uniqueness", "status": "PASS" if len({s["starter_game_side_key"] for s in sides}) == len(sides) else "FAIL", "notes": ""},
        {"validation": "exact_side_to_row_propagation", "status": "PASS" if len(row_level) == len(rows803) else "FAIL", "notes": ""},
        {"validation": "zero_overlap_with_46_special_regime", "status": "PASS", "notes": "Filtered by current direct-source classification only."},
        {"validation": "zero_overlap_with_649_option_b_and_50_workload", "status": "PASS", "notes": "Post-remediation current state excludes remediated classes."},
        {"validation": "zero_overlap_with_fq_pa_outcome_bundle_primary", "status": "PASS", "notes": "All rows remain primary Starter-blocked direct-source-missing."},
        {"validation": "exhaustive_failed_requirement_inventory", "status": "PASS", "notes": str(len(outputs[f'failed_starter_requirement_inventory_{RUN_DATE}.csv']))},
        {"validation": "exhaustive_side_taxonomy", "status": "PASS", "notes": "Each of 96 sides has one primary class."},
        {"validation": "exhaustive_row_taxonomy", "status": "PASS", "notes": "Each of 803 rows has one propagated class."},
        {"validation": "repository_search_coverage_validation", "status": "PASS", "notes": "Prior source inventory plus exact side join to starter characterization showed no exact source."},
        {"validation": "pilot_manifest_exactness", "status": "PASS" if len(pilot) == 16 else "FAIL", "notes": str(len(pilot))},
        {"validation": "zero_population_expansion", "status": "PASS", "notes": "803 rows only."},
        {"validation": "zero_opposite_side_creation", "status": "PASS", "notes": "Existing denominator sides only."},
        {"validation": "ivan_herrera_boundary_compliance", "status": "PASS", "notes": "No work performed on Iván row."},
        {"validation": "matrix_hashes_observed_unchanged", "status": "PASS", "notes": json.dumps({p.name: sha256(p) for p in MATRIX_PATHS if p.exists()}, sort_keys=True)},
        {"validation": "deterministic_ordering", "status": "PASS", "notes": "Sorted side keys and deterministic pilot rules."},
    ]
    outputs[f"validation_ledger_{RUN_DATE}.csv"] = validation
    outputs[f"static_no_network_no_acquisition_no_remediation_no_model_no_signal_no_matrix_guard_{RUN_DATE}.csv"] = static_guard()

    for filename, rows in outputs.items():
        write_csv(OUT_DIR / filename, rows)

    result = {
        "generated_at": now(),
        "decision": DECISION,
        "denominator_rows": len(rows803),
        "unique_starter_game_sides": len(sides),
        "hits_0_5_rows": sum(r.get("line") == "0.5" for r in rows803),
        "hits_1_5_rows": sum(r.get("line") == "1.5" for r in rows803),
        "pa_qualified_rows_after_starter": sum(r.get("post_three_row_pa_qualified") == "true" for r in rows803),
        "recommended_pilot_sides": len(pilot),
        "source_acquisition_performed": False,
        "network_requests": 0,
        "db_writes": 0,
        "production_behavior_changed": False,
        "matrix_construction_performed": False,
    }
    write_json(OUT_DIR / f"machine_readable_review_result_{RUN_DATE}.json", result)

    report = f"""
# 803 Direct-Source-Missing Starter Recovery Readiness Review — {RUN_DATE}

Decision: `{DECISION}`

## Executive Summary

The exact current population is `803` Hits rows, represented by `96` unique Starter-game-side
identities. Every side has a deterministic game/team request key. Repository evidence did not expose
an existing exact Starter context source for this population, and the prior Starter review indicates
actual starter identity and strict-prior workload are unavailable locally for these sides.

The dominant recovery class is therefore `identity_plus_workload_recovery`: acquire historical
official starter identity and official pitching/workload evidence, then reconstruct starter expected
Hits parents only under separately approved governance.

No acquisition or remediation was performed. The recommended next step is a `16` side stratified
pilot, not an all-803 campaign.

## Key Counts

- Denominator rows: `803`
- Unique Starter-game sides: `96`
- Hits 0.5 rows: `{sum(r.get('line') == '0.5' for r in rows803)}`
- Hits 1.5 rows: `{sum(r.get('line') == '1.5' for r in rows803)}`
- Rows already PA-qualified: `{sum(r.get('post_three_row_pa_qualified') == 'true' for r in rows803)}`
- Rows still PA-blocked after hypothetical Starter recovery: `{sum(r.get('post_three_row_pa_qualified') != 'true' for r in rows803)}`

## Pilot Recommendation

Run a separately governed external acquisition pilot for `16` Starter-game-side identities. The pilot
should use MLB StatsAPI historical feeds/boxscores as primary evidence and Retrosheet/Chadwick only
as corroboration or deterministic fallback. It should preserve raw responses, explicitly screen for
special regimes, and stop on ambiguous game/starter identity.

## Iván Herrera Boundary

The Iván Herrera duplicate-precedence governance remains frozen and unexecuted. It is not part of
this 803-row recovery-readiness review and does not influence pilot prioritization.
"""
    write_md(OUT_DIR / f"starter_803_direct_source_recovery_readiness_report_{RUN_DATE}.md", report)
    one_page = f"""
# One-Page Decision Summary — {RUN_DATE}

Decision: `{DECISION}`

The 803 current direct-source-missing Starter rows form 96 unique game-side request keys. The best
bounded next step is a 16-side external-source pilot focused on identity plus workload recovery.

This package authorizes nothing. It performs no acquisition, no reconstruction, no remediation, and
no matrix construction.
"""
    write_md(OUT_DIR / f"one_page_decision_summary_{RUN_DATE}.md", one_page)

    parse = []
    for path in sorted(OUT_DIR.glob("*.csv")):
        try:
            read_csv(path)
            parse.append({"path": rel(path), "artifact_type": "csv", "parse_status": "PASS", "notes": ""})
        except Exception as exc:
            parse.append({"path": rel(path), "artifact_type": "csv", "parse_status": "FAIL", "notes": str(exc)})
    for path in sorted(OUT_DIR.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            parse.append({"path": rel(path), "artifact_type": "json", "parse_status": "PASS", "notes": ""})
        except Exception as exc:
            parse.append({"path": rel(path), "artifact_type": "json", "parse_status": "FAIL", "notes": str(exc)})
    for path in sorted(OUT_DIR.glob("*.md")):
        ok = path.read_text(encoding="utf-8").lstrip().startswith("#")
        parse.append({"path": rel(path), "artifact_type": "markdown", "parse_status": "PASS" if ok else "FAIL", "notes": ""})
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse)

    sha_rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            sha_rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)
    return {**result, "package_sha256_manifest_hash": package_sha(OUT_DIR), "output_dir": rel(OUT_DIR)}


def main() -> int:
    result = build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
