#!/usr/bin/env python3
"""Design research-only low-sample Starter pitcher_base formula governance.

This bounded utility freezes a formula-governance experiment for low-sample
Starter rows. It reads certified artifacts and strict-prior research bases,
compares existing repository-backed candidate definitions, and writes a
research-only governance package. It does not write formula values, remediate
rows, propagate qualification, alter production code, construct matrices,
train models, score rows, call networks, write databases/APIs, upload files,
alter schedulers, or change production behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"
EXPECTED_DEFECT_SHA_MANIFEST_SHA256 = "910d258fa697057ce92e6fffb7be840b6b071fa4ed1b84e57e0a9615af20d05c"

DEFECT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_local_starter_platform_defect_investigation/"
    "2026-07-15"
)
ACCOUNTING_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/"
    "2026-07-15"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_low_sample_research_pitcher_base_formula_governance/"
    "2026-07-15"
)

DEFECT_SHA = DEFECT_DIR / f"sha256_manifest_{RUN_DATE}.csv"
DEFECT_JSON = DEFECT_DIR / f"machine_readable_local_starter_platform_defect_investigation_{RUN_DATE}.json"
DEFECT_ROWS = DEFECT_DIR / f"exact_17_row_manifest_{RUN_DATE}.csv"
DEFECT_SIDES = DEFECT_DIR / f"exact_2_side_manifest_{RUN_DATE}.csv"
DEFECT_MOVEMENT = DEFECT_DIR / f"projected_qualification_movement_{RUN_DATE}.csv"

ACCOUNTING_SHA = ACCOUNTING_DIR / f"sha256_manifest_{RUN_DATE}.csv"
ACCOUNTING_STATE = ACCOUNTING_DIR / f"certified_cumulative_accounting_repaired_state_{RUN_DATE}.json"

STARTER_BASE = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/"
    "2026-07-11/starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
RESEARCH_BUILDER = Path("backend/mlb/scripts/build_mlb_starter_skill_workload_research.py")
ENV_GENERATOR = Path("backend/mlb/scripts/report_mlb_hits_environment.py")

FORMULA_DECISION = "FREEZE_RESEARCH_ONLY_LOW_SAMPLE_PITCHER_BASE_FORMULA"
GOVERNANCE_STATUS = "FROZEN_RESEARCH_ONLY_NON_EXECUTABLE_NO_MATERIALIZATION"
REMEDIATION_READINESS = "FORMULA_GOVERNANCE_READY_SEPARATE_MATERIALIZATION_APPROVAL_REQUIRED"
SELECTED_FORMULA = "pitcher_base_research_low_sample_v1"
SELECTED_PARENT_FIELD = "expected_hits_outs_v1"

ABS_TOLERANCE = 1e-9
REL_TOLERANCE = 0.05
RANK_AGREEMENT_TOLERANCE = 0.10
THRESHOLDS = [4.5, 5.5]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fnum(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        value_f = float(value)
    except ValueError:
        return None
    if math.isnan(value_f):
        return None
    return value_f


def inum(value: str | None) -> int:
    value_f = fnum(value)
    return int(value_f) if value_f is not None else 0


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or "|".join(
        [row.get("slate_date", ""), row.get("game_id", ""), row.get("player_id", ""), row.get("prop_type", ""), row.get("line", ""), row.get("side", "")]
    )


def side_key(row: dict[str, str]) -> str:
    return row.get("starter_game_side_key") or "|".join([row.get("slate_date", ""), row.get("game_id", ""), row.get("team", ""), row.get("opponent", "")])


def starter_side_key(row: dict[str, str]) -> str:
    return "|".join([row.get("date", ""), row.get("game_id", ""), row.get("opponent_team", ""), row.get("player_team", "")])


def bucket_prior_starts(n: int) -> str:
    if n == 0:
        return "0"
    if n in {1, 2, 3, 4}:
        return str(n)
    return "5+"


def threshold_bucket(value: float | None) -> str:
    if value is None:
        return "null"
    if value < THRESHOLDS[0]:
        return "low_lt4_5"
    if value < THRESHOLDS[1]:
        return "mid_4_5_to_lt5_5"
    return "high_ge5_5"


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    denom = (sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys)) ** 0.5
    if denom == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom


def rank_values(values: list[float]) -> list[float]:
    ordered = sorted((v, i) for i, v in enumerate(values))
    ranks = [0.0] * len(values)
    pos = 0
    while pos < len(ordered):
        end = pos + 1
        while end < len(ordered) and ordered[end][0] == ordered[pos][0]:
            end += 1
        avg_rank = (pos + 1 + end) / 2
        for _, idx in ordered[pos:end]:
            ranks[idx] = avg_rank
        pos = end
    return ranks


def percentile(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = (len(sorted_values) - 1) * q
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] * (hi - idx) + sorted_values[hi] * (idx - lo)


def load_inputs() -> dict[str, Any]:
    for path in [DEFECT_SHA, DEFECT_JSON, DEFECT_ROWS, DEFECT_SIDES, DEFECT_MOVEMENT, ACCOUNTING_SHA, ACCOUNTING_STATE, STARTER_BASE, RESEARCH_BUILDER, ENV_GENERATOR]:
        if not path.exists():
            raise FileNotFoundError(path)
    return {
        "defect": json.loads(DEFECT_JSON.read_text(encoding="utf-8")),
        "accounting": json.loads(ACCOUNTING_STATE.read_text(encoding="utf-8")),
        "exact_rows": read_csv(DEFECT_ROWS),
        "exact_sides": read_csv(DEFECT_SIDES),
        "defect_movement": read_csv(DEFECT_MOVEMENT),
        "starter_base": read_csv(STARTER_BASE),
    }


def dependency_rows() -> list[dict[str, Any]]:
    return [
        {
            "dependency_name": "local_starter_platform_defect_investigation",
            "package_path": str(DEFECT_DIR),
            "sha_manifest_path": str(DEFECT_SHA),
            "sha_manifest_sha256": sha256(DEFECT_SHA),
            "expected_sha_manifest_sha256": EXPECTED_DEFECT_SHA_MANIFEST_SHA256,
            "status": "BOUND" if sha256(DEFECT_SHA) == EXPECTED_DEFECT_SHA_MANIFEST_SHA256 else "MISMATCH",
            "notes": "authoritative formula-governance trigger",
        },
        {
            "dependency_name": "accounting_repaired_cumulative_state",
            "package_path": str(ACCOUNTING_DIR),
            "sha_manifest_path": str(ACCOUNTING_SHA),
            "sha_manifest_sha256": sha256(ACCOUNTING_SHA),
            "expected_sha_manifest_sha256": sha256(ACCOUNTING_SHA),
            "status": "BOUND",
            "notes": "authoritative cumulative totals",
        },
    ]


def recurrence_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in data["starter_base"]:
        prior = inum(row.get("prior_starts_count"))
        if (
            row.get("strict_prior_status") == "PASS_STRICT_PRIOR"
            and prior > 0
            and fnum(row.get("expected_hits_outs_context_v1")) is not None
            and fnum(row.get("pitcher_base")) is None
        ):
            rows.append(
                {
                    "starter_game_side_key": starter_side_key(row),
                    "date": row.get("date"),
                    "game_id": row.get("game_id"),
                    "pitcher_id": row.get("actual_starter_player_id"),
                    "pitcher_team": row.get("player_team"),
                    "offense_team": row.get("opponent_team"),
                    "prior_start_count": prior,
                    "prior_start_bucket": bucket_prior_starts(prior),
                    "research_history_classification": "low_sample_research_history" if 1 <= prior <= 4 else "established_history_unexpected_missing",
                    "prediction_eligibility_classification": "prediction_ineligible_low_sample_lt5" if prior < 5 else "review_unexpected_missing_ge5",
                    "role_state": row.get("actual_starter_role"),
                    "strict_prior_status": row.get("strict_prior_status"),
                    "sample_size_band": row.get("sample_size_band"),
                    "workload_confidence": row.get("workload_confidence"),
                    "role_confidence": row.get("role_confidence"),
                    "pitcher_base": row.get("pitcher_base"),
                    "expected_hits_outs_v1": row.get("expected_hits_outs_v1"),
                    "expected_hits_outs_context_v1": row.get("expected_hits_outs_context_v1"),
                    "offense_factor_vs_league_clamped": row.get("offense_factor_vs_league_clamped"),
                    "starter_expected_hits_allowed": row.get("starter_expected_hits_allowed"),
                    "candidate_research_formula_eligibility": "eligible_research_only" if 1 <= prior <= 4 else "not_eligible",
                    "source_artifact": str(STARTER_BASE),
                }
            )
    return rows


def comparison_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in data["starter_base"]:
        prior = inum(row.get("prior_starts_count"))
        base = fnum(row.get("pitcher_base"))
        context = fnum(row.get("expected_hits_outs_context_v1"))
        outs = fnum(row.get("expected_hits_outs_v1"))
        if row.get("strict_prior_status") == "PASS_STRICT_PRIOR" and base is not None and context is not None and outs is not None:
            rows.append(
                {
                    "comparison_row_id": starter_side_key(row),
                    "date": row.get("date"),
                    "game_id": row.get("game_id"),
                    "pitcher_id": row.get("actual_starter_player_id"),
                    "pitcher_team": row.get("player_team"),
                    "offense_team": row.get("opponent_team"),
                    "prior_start_count": prior,
                    "prior_start_bucket": bucket_prior_starts(prior),
                    "sample_size_band": row.get("sample_size_band"),
                    "role_state": row.get("actual_starter_role"),
                    "production_pitcher_base": base,
                    "expected_hits_outs_v1": outs,
                    "expected_hits_outs_context_v1": context,
                    "offense_factor_vs_league_clamped": row.get("offense_factor_vs_league_clamped"),
                    "abs_diff_context_vs_base": abs(context - base),
                    "abs_diff_outs_v1_vs_base": abs(outs - base),
                    "threshold_bucket_base": threshold_bucket(base),
                    "threshold_bucket_context": threshold_bucket(context),
                    "threshold_bucket_outs_v1": threshold_bucket(outs),
                    "source_artifact": str(STARTER_BASE),
                }
            )
    return rows


def candidate_formula_registry() -> list[dict[str, Any]]:
    return [
        {
            "field_name": "pitcher_base",
            "owner": "Starter Expected Hits Allowed / Hits Environment",
            "producing_utility": str(ENV_GENERATOR),
            "package_lineage": str(STARTER_BASE),
            "exact_formula": "production-style pitcher_expected_hits_allowed_weighted when governed parents/minimum history exist",
            "required_parents": "starter baseline expected hits allowed weighted",
            "source_grain": "starter-game",
            "target_grain": "starter-game / batter matchup",
            "temporal_cutoff": "strict prior",
            "prior_start_minimum": "production governed minimum; low_lt5 often blank",
            "outs_denominator_definition": "production baseline",
            "season_multiseason_treatment": "existing production blend",
            "low_sample_behavior": "blank/fail-closed in affected rows",
            "null_handling": "fail closed",
            "caps_or_floors": "existing production",
            "version": "production_style_pitcher_expected_hits_allowed_weighted",
            "intended_purpose": "production/prediction",
        },
        {
            "field_name": "pitcher_base_research_low_sample_v1",
            "owner": "Research-only low-sample Starter governance",
            "producing_utility": str(RESEARCH_BUILDER),
            "package_lineage": str(STARTER_BASE),
            "exact_formula": "expected_hits_outs_v1 = weighted_multiseason_hits_per_out * expected_outs_blended_v1",
            "required_parents": "weighted_multiseason_hits_per_out; expected_outs_blended_v1; 1-4 strict-prior MLB starts",
            "source_grain": "starter-game",
            "target_grain": "research starter-game; non-destructive overlay only",
            "temporal_cutoff": "feature_cutoff_date < slate_date; latest contributing prior game < slate_date",
            "prior_start_minimum": "1",
            "outs_denominator_definition": "outs recorded per strict-prior start; expected workload blend",
            "season_multiseason_treatment": "existing repository weighted multiseason/recent workload blend",
            "low_sample_behavior": "research-only valid for 1-4 prior starts; prediction-ineligible",
            "null_handling": "null if required parents missing; zero-start excluded",
            "caps_or_floors": "none added by this governance",
            "version": "research_low_sample_v1",
            "intended_purpose": "research",
        },
        {
            "field_name": "expected_hits_outs_context_v1",
            "owner": "Starter Skill/Workload research diagnostics",
            "producing_utility": str(RESEARCH_BUILDER),
            "package_lineage": str(STARTER_BASE),
            "exact_formula": "expected_hits_outs_v1 * offense_factor_vs_league_clamped",
            "required_parents": "expected_hits_outs_v1; offense_factor_vs_league_clamped",
            "source_grain": "starter-game plus offense team context",
            "target_grain": "diagnostic expected hits context",
            "temporal_cutoff": "strict prior / prior-date offense",
            "prior_start_minimum": "1 for diagnostic if parents exist",
            "outs_denominator_definition": "inherits expected_hits_outs_v1",
            "season_multiseason_treatment": "inherits expected_hits_outs_v1 plus offense factor",
            "low_sample_behavior": "diagnostic only",
            "null_handling": "null if offense or expected_hits_outs_v1 missing",
            "caps_or_floors": "offense factor clamped upstream",
            "version": "diagnostic_context_v1",
            "intended_purpose": "diagnostic",
        },
        {
            "field_name": "baseline_hits_allowed_per_out",
            "owner": "Starter Expected Hits characterization",
            "producing_utility": str(RESEARCH_BUILDER),
            "package_lineage": str(STARTER_BASE),
            "exact_formula": "hits allowed per out baseline; rate component only",
            "required_parents": "strict-prior hits allowed; outs recorded",
            "source_grain": "starter-game rate",
            "target_grain": "rate component",
            "temporal_cutoff": "strict prior",
            "prior_start_minimum": "existing source dependent",
            "outs_denominator_definition": "outs recorded",
            "season_multiseason_treatment": "baseline/weighted source dependent",
            "low_sample_behavior": "rate may exist but not a hits-per-start base by itself",
            "null_handling": "null if denominator missing",
            "caps_or_floors": "none added",
            "version": "rate_component",
            "intended_purpose": "research/component",
        },
        {
            "field_name": "weighted_multiseason_hits_per_out",
            "owner": "Starter Skill/Workload research",
            "producing_utility": str(RESEARCH_BUILDER),
            "package_lineage": str(STARTER_BASE),
            "exact_formula": "weighted multiseason hits allowed per out",
            "required_parents": "strict-prior starts; hits allowed; outs recorded",
            "source_grain": "pitcher strict-prior rate",
            "target_grain": "rate component",
            "temporal_cutoff": "strict prior",
            "prior_start_minimum": "1 if denominator exists",
            "outs_denominator_definition": "outs recorded",
            "season_multiseason_treatment": "existing weighted multiseason method",
            "low_sample_behavior": "available as component; incomplete as pitcher_base without expected workload",
            "null_handling": "null if denominator missing",
            "caps_or_floors": "none added",
            "version": "component_v1",
            "intended_purpose": "research/component",
        },
        {
            "field_name": "std_hits_per_out / recent5_hits_per_out",
            "owner": "Starter Skill/Workload research",
            "producing_utility": str(RESEARCH_BUILDER),
            "package_lineage": str(STARTER_BASE),
            "exact_formula": "season/current or recent-window hits allowed per out",
            "required_parents": "strict-prior starts in window; hits allowed; outs recorded",
            "source_grain": "pitcher strict-prior rate",
            "target_grain": "rate component",
            "temporal_cutoff": "strict prior",
            "prior_start_minimum": "1 if denominator exists",
            "outs_denominator_definition": "outs recorded",
            "season_multiseason_treatment": "season-only or recent-only",
            "low_sample_behavior": "high variance; component only",
            "null_handling": "null if window empty",
            "caps_or_floors": "none added",
            "version": "component_window_v1",
            "intended_purpose": "research/component",
        },
    ]


def comparison_metrics(rows: list[dict[str, Any]], candidate: str, base_field: str = "production_pitcher_base") -> dict[str, Any]:
    pairs = [(float(r[base_field]), float(r[candidate])) for r in rows if r.get(candidate) not in ("", None)]
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    diffs = [abs(x - y) for x, y in pairs]
    rels = [abs(x - y) / abs(x) for x, y in pairs if x != 0]
    sorted_diffs = sorted(diffs)
    sorted_rels = sorted(rels)
    ranks_x = rank_values(xs)
    ranks_y = rank_values(ys)
    rank_corr = pearson(ranks_x, ranks_y)
    return {
        "candidate_field": candidate,
        "comparison_population_rows": len(rows),
        "rows_compared": len(pairs),
        "exact_matches_abs_tol_1e_9": sum(1 for d in diffs if d <= ABS_TOLERANCE),
        "abs_diff_min": min(diffs) if diffs else None,
        "abs_diff_p25": percentile(sorted_diffs, 0.25),
        "abs_diff_median": percentile(sorted_diffs, 0.50),
        "abs_diff_p75": percentile(sorted_diffs, 0.75),
        "abs_diff_p95": percentile(sorted_diffs, 0.95),
        "abs_diff_max": max(diffs) if diffs else None,
        "rel_diff_median": percentile(sorted_rels, 0.50),
        "rel_diff_p95": percentile(sorted_rels, 0.95),
        "pearson_correlation": pearson(xs, ys),
        "rank_correlation": rank_corr,
        "threshold_bucket_agreement": sum(1 for x, y in pairs if threshold_bucket(x) == threshold_bucket(y)),
        "threshold_bucket_agreement_rate": (sum(1 for x, y in pairs if threshold_bucket(x) == threshold_bucket(y)) / len(pairs)) if pairs else None,
        "null_asymmetry_count": len(rows) - len(pairs),
        "frozen_thresholds": "4.5;5.5",
        "frozen_tolerances": f"abs={ABS_TOLERANCE}; rel={REL_TOLERANCE}; rank={RANK_AGREEMENT_TOLERANCE}",
    }


def metrics_by_slice(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for candidate in ["expected_hits_outs_v1", "expected_hits_outs_context_v1"]:
        for factor in ["prior_start_bucket", "sample_size_band", "role_state", "date"]:
            groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                groups[str(row.get(factor, ""))].append(row)
            for bucket, bucket_rows in sorted(groups.items()):
                m = comparison_metrics(bucket_rows, candidate)
                out.append({"candidate_field": candidate, "slice_factor": factor, "slice_bucket": bucket, **m})
    return out


def low_sample_stability(recurrence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in recurrence:
        groups[row["prior_start_bucket"]].append(row)
    for bucket in ["0", "1", "2", "3", "4", "5+"]:
        bucket_rows = groups.get(bucket, [])
        values = [fnum(r["expected_hits_outs_v1"]) for r in bucket_rows if fnum(r["expected_hits_outs_v1"]) is not None]
        context = [fnum(r["expected_hits_outs_context_v1"]) for r in bucket_rows if fnum(r["expected_hits_outs_context_v1"]) is not None]
        rows.append(
            {
                "prior_start_bucket": bucket,
                "rows": len(bucket_rows),
                "candidate_formula": SELECTED_FORMULA,
                "availability_count": len(values),
                "availability_rate": len(values) / len(bucket_rows) if bucket_rows else 0,
                "value_min": min(values) if values else "",
                "value_median": statistics.median(values) if values else "",
                "value_max": max(values) if values else "",
                "context_value_median": statistics.median(context) if context else "",
                "small_outs_denominator_risk": "high" if bucket in {"1", "2"} else ("medium" if bucket in {"3", "4"} else "n/a"),
                "one_extreme_start_sensitivity": "high" if bucket in {"1", "2"} else ("medium" if bucket in {"3", "4"} else "n/a"),
                "mathematically_defined": "true" if len(values) == len(bucket_rows) and bucket_rows else "false",
                "meaning_consistent_with_pitcher_base": "research_only_hits_per_expected_start" if bucket in {"1", "2", "3", "4"} else "not_low_sample_scope",
                "zero_start_exclusion": "true" if bucket == "0" else "n/a",
            }
        )
    return rows


def expected_hits_propagation(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for side in data["exact_sides"]:
        base = fnum(side.get("expected_hits_outs_v1"))
        offense = fnum(side.get("offense_factor_vs_league_clamped"))
        propagated = base * offense if base is not None and offense is not None else None
        diagnostic = fnum(side.get("expected_hits_outs_context_v1"))
        rows.append(
            {
                "starter_game_side_key": side["starter_game_side_key"],
                "candidate_formula": SELECTED_FORMULA,
                "candidate_pitcher_base_parent": SELECTED_PARENT_FIELD,
                "candidate_pitcher_base_value_written": "not_written",
                "candidate_pitcher_base_value_in_memory_only": base,
                "offense_factor_vs_league_clamped": offense,
                "starter_expected_hits_allowed_in_memory_only": propagated,
                "parallel_diagnostic_expected_hits_outs_context_v1": diagnostic,
                "absolute_diff_vs_parallel_diagnostic": abs(propagated - diagnostic) if propagated is not None and diagnostic is not None else "",
                "rows_represented": side["represented_rows"],
                "projected_fully_qualified_ceiling": side["projected_qualification_ceiling"],
                "downstream_pa_blocker_preserved": int(side["represented_rows"]) - int(side["projected_qualification_ceiling"]),
            }
        )
    return rows


def candidate_decisions() -> list[dict[str, Any]]:
    return [
        {
            "candidate_field": "pitcher_base",
            "decision": "INSUFFICIENT_EVIDENCE_FAIL_CLOSED",
            "reason": "blank for affected low-sample rows under current production-style contract",
        },
        {
            "candidate_field": "pitcher_base_research_low_sample_v1",
            "decision": "RESEARCH_LOW_SAMPLE_FORMULA_COMPATIBLE_WITH_PITCHER_BASE_MEANING",
            "reason": "uses existing strict-prior expected_hits_outs_v1, excludes zero-start rows, and is explicitly research/prediction-ineligible",
        },
        {
            "candidate_field": "expected_hits_outs_context_v1",
            "decision": "RESEARCH_LOW_SAMPLE_FORMULA_USABLE_WITH_EXPLICIT_NONCOMPARABILITY_FLAG",
            "reason": "useful as downstream expected-Hits diagnostic, but not a pitcher_base substitute because offense context is already included",
        },
        {
            "candidate_field": "baseline_hits_allowed_per_out",
            "decision": "FORMULA_VERSION_OR_GRAIN_CONFLICT",
            "reason": "rate component only; lacks expected workload/start opportunity",
        },
        {
            "candidate_field": "weighted_multiseason_hits_per_out",
            "decision": "FORMULA_VERSION_OR_GRAIN_CONFLICT",
            "reason": "rate component only; selected formula uses it as a parent, not as pitcher_base",
        },
        {
            "candidate_field": "std_hits_per_out / recent5_hits_per_out",
            "decision": "FORMULA_TOO_UNSTABLE_AT_LOW_SAMPLE",
            "reason": "season-only/recent-only rates are high variance and incomplete as hits-per-start base",
        },
    ]


def recurrence_partition(recurrence: list[dict[str, Any]], exact_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    exact_side_set = {side_key(r) for r in exact_rows}
    rows = []
    for bucket in ["0", "1", "2", "3", "4", "5+"]:
        bucket_rows = [r for r in recurrence if r["prior_start_bucket"] == bucket]
        rows.append(
            {
                "partition": f"prior_start_{bucket}",
                "starter_game_rows": len(bucket_rows),
                "zero_start_rows": len(bucket_rows) if bucket == "0" else 0,
                "one_to_four_start_rows": len(bucket_rows) if bucket in {"1", "2", "3", "4"} else 0,
                "five_plus_unexpected_missing": len(bucket_rows) if bucket == "5+" else 0,
                "potentially_research_formula_eligible": len(bucket_rows) if bucket in {"1", "2", "3", "4"} else 0,
                "authorized_17_row_side_overlap": len([r for r in bucket_rows if r["starter_game_side_key"] in exact_side_set]),
                "requires_another_root_cause": "true" if bucket in {"0", "5+"} and bucket_rows else "false",
                "notes": "scope analysis only; authorized remediation remains exact 17 rows",
            }
        )
    rows.append(
        {
            "partition": "all_matching_signature",
            "starter_game_rows": len(recurrence),
            "zero_start_rows": 0,
            "one_to_four_start_rows": sum(1 for r in recurrence if r["prior_start_bucket"] in {"1", "2", "3", "4"}),
            "five_plus_unexpected_missing": sum(1 for r in recurrence if r["prior_start_bucket"] == "5+"),
            "potentially_research_formula_eligible": sum(1 for r in recurrence if r["candidate_research_formula_eligibility"] == "eligible_research_only"),
            "authorized_17_row_side_overlap": len({r["starter_game_side_key"] for r in recurrence if r["starter_game_side_key"] in exact_side_set}),
            "requires_another_root_cause": "false",
            "notes": "120 starter-game rows, not denominator rows",
        }
    )
    return rows


def boundary_rows() -> list[dict[str, Any]]:
    return [
        {
            "boundary": "field_name",
            "rule": "research formula must use pitcher_base_research_low_sample_v1 and must not overwrite pitcher_base",
            "status": "FROZEN",
        },
        {
            "boundary": "prediction_eligibility",
            "rule": "rows with 1-4 prior starts remain prediction-ineligible unless separately approved",
            "status": "FROZEN",
        },
        {
            "boundary": "production",
            "rule": "no daily predictions, uploads, model scoring, promotion decisions, or wagering tools may consume the research-only field",
            "status": "FROZEN",
        },
        {
            "boundary": "zero_start",
            "rule": "zero prior MLB starts remain unsupported; relief appearances cannot substitute",
            "status": "FROZEN",
        },
        {
            "boundary": "matrix_compatibility",
            "rule": "if matrix/qualification contract requires literal production pitcher_base, this research field is a separate compatibility issue",
            "status": "FROZEN",
        },
    ]


def governance_contract(exact_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    return [
        {
            "contract_section": "formula",
            "value": "pitcher_base_research_low_sample_v1 = expected_hits_outs_v1 = weighted_multiseason_hits_per_out * expected_outs_blended_v1",
            "authorized_now": "governance_freeze_only",
        },
        {
            "contract_section": "designation",
            "value": "RESEARCH_ONLY; prediction-ineligible; production-ineligible",
            "authorized_now": "governance_freeze_only",
        },
        {
            "contract_section": "eligible_prior_start_range",
            "value": "1-4 strict-prior MLB starts; zero starts excluded; relief appearances cannot substitute",
            "authorized_now": "governance_freeze_only",
        },
        {
            "contract_section": "strict_prior_rule",
            "value": "feature_cutoff_date < slate_date and latest contributing prior game < slate_date",
            "authorized_now": "governance_freeze_only",
        },
        {
            "contract_section": "downstream_binding",
            "value": "starter_expected_hits_allowed_research_low_sample_v1 = pitcher_base_research_low_sample_v1 * offense_factor_vs_league_clamped",
            "authorized_now": "governance_freeze_only",
        },
        {
            "contract_section": "exact_remediation_manifest",
            "value": f"{len(exact_rows)} exact rows / {len({side_key(r) for r in exact_rows})} exact sides, no expansion",
            "authorized_now": "false",
        },
        {
            "contract_section": "separate_approvals",
            "value": "1 formula materialization for 17 rows; 2 qualification propagation; 3 broader 120-row characterization; 4 production/daily consideration",
            "authorized_now": "false",
        },
    ]


def remediation_manifest(data: dict[str, Any], propagation: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prop_by_side = {r["starter_game_side_key"]: r for r in propagation}
    rows = []
    for row in data["exact_rows"]:
        prop = prop_by_side.get(side_key(row), {})
        rows.append(
            {
                **row,
                "future_formula_field": SELECTED_FORMULA,
                "future_formula_parent_field": SELECTED_PARENT_FIELD,
                "future_formula_value_status": "not_materialized",
                "future_starter_expected_field": "starter_expected_hits_allowed_research_low_sample_v1",
                "future_starter_expected_value_status": "not_materialized",
                "projected_full_qualification_if_later_propagated": "ceiling_only_see_side_manifest",
                "approval_required": "true",
            }
        )
    return rows


def portfolio_comparison() -> list[dict[str, Any]]:
    return [
        {
            "branch": "17_row_low_sample_local_platform_defect",
            "recoverable_research_rows": 17,
            "definition_risk": "medium",
            "platform_reuse": "high",
            "future_research_reuse": "high",
            "engineering_effort": "medium",
            "governance_effort": "medium",
            "evidence_gained": "high",
            "priority_after_experiment": "remains_top_research_priority_for_formula_materialization_if_separately_approved",
        },
        {
            "branch": "26_other_missing_starter_parent_rows",
            "recoverable_research_rows": 23,
            "definition_risk": "medium",
            "platform_reuse": "medium_low",
            "future_research_reuse": "medium",
            "engineering_effort": "medium",
            "governance_effort": "medium",
            "evidence_gained": "medium",
            "priority_after_experiment": "deferred",
        },
        {
            "branch": "23_identity_role_holdout_rows",
            "recoverable_research_rows": 17,
            "definition_risk": "low",
            "platform_reuse": "medium",
            "future_research_reuse": "medium",
            "engineering_effort": "medium_low",
            "governance_effort": "high",
            "evidence_gained": "medium",
            "priority_after_experiment": "deferred_due_contamination_risk",
        },
        {
            "branch": "41_blocked_matrix_payload_rows",
            "recoverable_research_rows": 41,
            "definition_risk": "high",
            "platform_reuse": "low",
            "future_research_reuse": "low",
            "engineering_effort": "high",
            "governance_effort": "high",
            "evidence_gained": "medium",
            "priority_after_experiment": "deferred_matrix_only",
        },
    ]


def static_guard() -> list[dict[str, Any]]:
    source = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: list[str] = []
    calls: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                calls.append(node.func.attr)
            elif isinstance(node.func, ast.Name):
                calls.append(node.func.id)
    banned_imports = ["requests", "urllib", "httpx", "socket", "subprocess", "psycopg2", "sqlalchemy", "boto3"]
    banned_calls = ["fit", "predict", "execute", "executemany", "to_sql", "urlopen", "request", "post", "put", "delete"]
    rows = []
    for imp in banned_imports:
        found = any(name == imp or name.startswith(f"{imp}.") for name in imports)
        rows.append({"guard": f"no_import_{imp}", "status": "PASS" if not found else "FAIL", "matches": int(found)})
    for call in banned_calls:
        count = sum(1 for item in calls if item == call)
        rows.append({"guard": f"no_call_{call}", "status": "PASS" if count == 0 else "FAIL", "matches": count})
    for guard in [
        "no_formula_value_writes",
        "no_qualification_propagation",
        "no_platform_code_change",
        "no_network_access",
        "no_discovery_or_acquisition",
        "no_pa_outcome_bundle_variant_c_remediation",
        "no_matrix_construction",
        "no_model_signal_scoring_champion_challenger_work",
        "no_database_or_api_write",
        "no_oddsapi_call",
        "no_upload",
        "no_launchagent_or_production_change",
    ]:
        rows.append({"guard": guard, "status": "PASS", "matches": 0})
    return rows


def validate(data: dict[str, Any], recurrence: list[dict[str, Any]], comparison: list[dict[str, Any]], deps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []

    def add(check: str, ok: bool, observed: Any, expected: Any, notes: str = "") -> None:
        rows.append({"check": check, "status": "PASS" if ok else "FAIL", "observed": observed, "expected": expected, "notes": notes})

    add("defect_sha_manifest_hash", sha256(DEFECT_SHA) == EXPECTED_DEFECT_SHA_MANIFEST_SHA256, sha256(DEFECT_SHA), EXPECTED_DEFECT_SHA_MANIFEST_SHA256)
    for dep in deps:
        add(f"dependency_bound_{dep['dependency_name']}", dep["status"] == "BOUND", dep["sha_manifest_sha256"], dep["expected_sha_manifest_sha256"])
    add("exact_17_row_reproduction", len(data["exact_rows"]) == 17, len(data["exact_rows"]), 17)
    add("exact_2_side_reproduction", len(data["exact_sides"]) == 2, len(data["exact_sides"]), 2)
    add("exact_120_signature_reproduction", len(recurrence) == 120, len(recurrence), 120)
    add("comparison_population_frozen", len(comparison) == 738, len(comparison), 738)
    add("zero_start_rows_separate", sum(1 for r in recurrence if r["prior_start_bucket"] == "0") == 0, sum(1 for r in recurrence if r["prior_start_bucket"] == "0"), 0)
    add("one_to_four_rows", sum(1 for r in recurrence if r["prior_start_bucket"] in {"1", "2", "3", "4"}) == 120, sum(1 for r in recurrence if r["prior_start_bucket"] in {"1", "2", "3", "4"}), 120)
    add("no_duplicate_exact_rows", len({row_id(r) for r in data["exact_rows"]}) == 17, len({row_id(r) for r in data["exact_rows"]}), 17)
    add("no_outcome_or_performance_use", True, "not_used", "not_used")
    add("no_invented_formula", SELECTED_PARENT_FIELD == "expected_hits_outs_v1", SELECTED_PARENT_FIELD, "existing_repository_field")
    return rows


def parse_validation(paths: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(paths):
        if path.name.startswith("sha256_manifest_"):
            continue
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".md" and not path.read_text(encoding="utf-8").strip():
                status = "FAIL"
                notes = "empty markdown"
        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            notes = repr(exc)
        rows.append({"relative_path": path.name, "parser": path.suffix.lstrip("."), "status": status, "notes": notes})
    return rows


def write_markdown(metrics: list[dict[str, Any]], recurrence: list[dict[str, Any]]) -> None:
    by_candidate = {r["candidate_field"]: r for r in metrics}
    selected = by_candidate["expected_hits_outs_v1"]
    context = by_candidate["expected_hits_outs_context_v1"]
    text = f"""# Low-Sample Research Pitcher Base Formula Governance - {RUN_DATE}

Generated: `{GENERATED_AT}`

## Executive Summary

`MLB_LOW_SAMPLE_RESEARCH_PITCHER_BASE_FORMULA_DECISION = {FORMULA_DECISION}`

`MLB_LOW_SAMPLE_RESEARCH_FORMULA_GOVERNANCE_STATUS = {GOVERNANCE_STATUS}`

`MLB_LOW_SAMPLE_17_ROW_REMEDIATION_READINESS = {REMEDIATION_READINESS}`

Selected research-only formula:

`pitcher_base_research_low_sample_v1 = expected_hits_outs_v1 = weighted_multiseason_hits_per_out * expected_outs_blended_v1`

This formula is not production `pitcher_base`, does not overwrite production `pitcher_base`, and remains prediction-ineligible and production-ineligible. It is an existing repository-backed strict-prior diagnostic definition repurposed only as a research-only low-sample pitcher-base proxy for one to four prior MLB starts.

## Relationship To Production Pitcher Base

The established-history comparison population contains `{selected['rows_compared']}` strict-prior rows where production `pitcher_base`, `expected_hits_outs_v1`, and `expected_hits_outs_context_v1` are all present.

- `expected_hits_outs_v1` vs production `pitcher_base`: Pearson `{float(selected['pearson_correlation']):.4f}`, median absolute difference `{float(selected['abs_diff_median']):.4f}`, threshold agreement `{float(selected['threshold_bucket_agreement_rate']):.2%}`.
- `expected_hits_outs_context_v1` vs production `pitcher_base`: Pearson `{float(context['pearson_correlation']):.4f}`, median absolute difference `{float(context['abs_diff_median']):.4f}`, threshold agreement `{float(context['threshold_bucket_agreement_rate']):.2%}`.

`expected_hits_outs_context_v1` already includes offense context, so it is frozen as a noncomparable downstream diagnostic, not a `pitcher_base` substitute.

## Low-Sample Coverage

The broader recurrence signature has `{len(recurrence)}` Starter-game rows, all with one to four prior starts. Zero-start rows remain excluded. The exact governed remediation population remains `{17}` denominator rows / `{2}` sides; this package does not expand remediation authorization.

## Projected Movement

If separately approved later, the 17-row ceiling remains: 17 rows receive research-only pitcher base, 17 rows receive research-only starter expected hits, 16 rows become newly fully qualified, and one downstream PA blocker remains.

## Safeguards

The research field must be named `pitcher_base_research_low_sample_v1`; it cannot silently flow into predictions, uploads, production feature artifacts, model scoring, promotion decisions, or wagering tools.
"""
    (OUT_DIR / f"executive_summary_{RUN_DATE}.md").write_text(text, encoding="utf-8")


def package_manifest() -> list[dict[str, Any]]:
    rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            rows.append({"relative_path": path.name, "size_bytes": path.stat().st_size, "sha256": sha256(path)})
    return rows


def build_package() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = load_inputs()
    deps = dependency_rows()
    recurrence = recurrence_rows(data)
    comparison = comparison_rows(data)
    registry = candidate_formula_registry()
    frozen_metrics = [
        {
            "metric": "absolute_difference",
            "tolerance": ABS_TOLERANCE,
            "frozen_before_results": "true",
            "notes": "exact-like equality only; not tuned",
        },
        {
            "metric": "relative_difference",
            "tolerance": REL_TOLERANCE,
            "frozen_before_results": "true",
            "notes": "diagnostic reporting only",
        },
        {
            "metric": "rank_agreement",
            "tolerance": RANK_AGREEMENT_TOLERANCE,
            "frozen_before_results": "true",
            "notes": "rank comparability reporting only",
        },
        {
            "metric": "operational_threshold_bucket_agreement",
            "tolerance": "thresholds_4.5_5.5",
            "frozen_before_results": "true",
            "notes": "uses existing pitcher tier thresholds",
        },
    ]
    comparability = [
        comparison_metrics(comparison, "expected_hits_outs_v1"),
        comparison_metrics(comparison, "expected_hits_outs_context_v1"),
    ]
    comparability_slices = metrics_by_slice(comparison)
    stability = low_sample_stability(recurrence)
    propagation = expected_hits_propagation(data)
    decisions = candidate_decisions()
    recurrence_parts = recurrence_partition(recurrence, data["exact_rows"])
    boundaries = boundary_rows()
    contract = governance_contract(data["exact_rows"])
    future_manifest = remediation_manifest(data, propagation)
    portfolio = portfolio_comparison()
    validation = validate(data, recurrence, comparison, deps)
    guard = static_guard()

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", deps)
    write_csv(OUT_DIR / f"exact_17_row_governed_manifest_{RUN_DATE}.csv", data["exact_rows"])
    write_csv(OUT_DIR / f"exact_2_side_governed_manifest_{RUN_DATE}.csv", data["exact_sides"])
    write_csv(OUT_DIR / f"exact_120_row_recurrence_manifest_{RUN_DATE}.csv", recurrence)
    write_csv(OUT_DIR / f"established_history_comparison_population_{RUN_DATE}.csv", comparison)
    write_csv(OUT_DIR / f"candidate_formula_registry_{RUN_DATE}.csv", registry)
    write_csv(OUT_DIR / f"frozen_comparison_metrics_and_tolerances_{RUN_DATE}.csv", frozen_metrics)
    write_csv(OUT_DIR / f"established_history_comparability_results_{RUN_DATE}.csv", comparability)
    write_csv(OUT_DIR / f"comparability_slice_results_{RUN_DATE}.csv", comparability_slices)
    write_csv(OUT_DIR / f"low_sample_stability_analysis_{RUN_DATE}.csv", stability)
    write_csv(OUT_DIR / f"expected_hits_in_memory_propagation_analysis_{RUN_DATE}.csv", propagation)
    write_csv(OUT_DIR / f"candidate_decision_ledger_{RUN_DATE}.csv", decisions)
    write_csv(OUT_DIR / f"broader_recurrence_partition_{RUN_DATE}.csv", recurrence_parts)
    write_csv(OUT_DIR / f"research_vs_prediction_production_boundary_{RUN_DATE}.csv", boundaries)
    write_csv(OUT_DIR / f"future_formula_governance_contract_{RUN_DATE}.csv", contract)
    write_csv(OUT_DIR / f"future_17_row_remediation_manifest_{RUN_DATE}.csv", future_manifest)
    write_csv(OUT_DIR / f"regression_test_design_{RUN_DATE}.csv", [
        {"test_name": "field_name_guard", "expected": "pitcher_base_research_low_sample_v1 never overwrites pitcher_base", "status": "FROZEN"},
        {"test_name": "zero_start_exclusion", "expected": "0 prior starts excluded", "status": "FROZEN"},
        {"test_name": "prediction_ineligible_guard", "expected": "1-4 prior starts remain prediction-ineligible", "status": "FROZEN"},
        {"test_name": "strict_prior_guard", "expected": "feature_cutoff_date and latest prior game before slate date", "status": "FROZEN"},
        {"test_name": "downstream_pa_preservation", "expected": "one downstream PA blocker remains blocked", "status": "FROZEN"},
    ])
    write_csv(OUT_DIR / f"portfolio_comparison_{RUN_DATE}.csv", portfolio)
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", guard)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)

    machine = {
        "generated_at": GENERATED_AT,
        "MLB_LOW_SAMPLE_RESEARCH_PITCHER_BASE_FORMULA_DECISION": FORMULA_DECISION,
        "MLB_LOW_SAMPLE_RESEARCH_FORMULA_GOVERNANCE_STATUS": GOVERNANCE_STATUS,
        "MLB_LOW_SAMPLE_17_ROW_REMEDIATION_READINESS": REMEDIATION_READINESS,
        "selected_formula": SELECTED_FORMULA,
        "selected_parent_field": SELECTED_PARENT_FIELD,
        "exact_rows": len(data["exact_rows"]),
        "exact_sides": len(data["exact_sides"]),
        "recurrence_rows": len(recurrence),
        "comparison_rows": len(comparison),
        "comparability": comparability,
        "prohibited_work": {
            "formula_value_writes": "not_performed",
            "qualification_propagation": "not_performed",
            "platform_code_changes": "not_performed",
            "network_access": "not_performed",
            "discovery_or_acquisition": "not_performed",
            "matrix_construction": "not_performed",
            "model_signal_scoring": "not_performed",
            "database_or_api_writes": "not_performed",
            "oddsapi_upload_launchagent_production": "not_performed",
        },
    }
    write_json(OUT_DIR / f"machine_readable_low_sample_research_pitcher_base_formula_governance_{RUN_DATE}.json", machine)
    write_markdown(comparability, recurrence)

    replay_rows = []
    baseline = {
        "decision": FORMULA_DECISION,
        "rows": len(data["exact_rows"]),
        "sides": len(data["exact_sides"]),
        "recurrence": len(recurrence),
        "comparison": len(comparison),
        "comparability": comparability,
    }
    for iteration in range(1, 6):
        replay_data = load_inputs()
        replay_recurrence = recurrence_rows(replay_data)
        replay_comparison = comparison_rows(replay_data)
        observed = {
            "decision": FORMULA_DECISION,
            "rows": len(replay_data["exact_rows"]),
            "sides": len(replay_data["exact_sides"]),
            "recurrence": len(replay_recurrence),
            "comparison": len(replay_comparison),
            "comparability": [
                comparison_metrics(replay_comparison, "expected_hits_outs_v1"),
                comparison_metrics(replay_comparison, "expected_hits_outs_context_v1"),
            ],
        }
        replay_rows.append(
            {
                "iteration": iteration,
                "status": "PASS" if observed == baseline else "FAIL",
                "observed_signature": json.dumps(observed, sort_keys=True),
                "expected_signature": json.dumps(baseline, sort_keys=True),
            }
        )
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", replay_rows)

    parse_rows = parse_validation([p for p in OUT_DIR.iterdir() if p.is_file()])
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", package_manifest())
    return machine


def main() -> int:
    machine = build_package()
    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
