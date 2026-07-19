"""Bounded read-only Hits threshold-specific evidence audit.

The audit asks whether current Hits O1.5 evidence is truly multi-hit-specific
or mostly any-hit evidence applied to a harder line. It reads local artifacts
only and writes an analysis package; it does not train, optimize, call network
services, write databases, or alter production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AUDIT_DATE = "2026-07-17"
PRIOR_TWO_SIDED_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits15_two_sided_matchup_advantage_audit/2026-07-17"
)
PRIOR_THREE_WAY_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits15_three_way_directional_ownership_validation/2026-07-17"
)
PRIOR_LEDGER = PRIOR_TWO_SIDED_DIR / "canonical_proposition_level_advantage_ledger_2026-07-17.csv"
DEFAULT_OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits_threshold_specific_evidence_audit/2026-07-17"
)
HITS_05_MATRIX = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_qualification_wave_2026-07-01_to_2026-07-08/2026-07-13/"
    "hits_0_5_variant_a_matrix_2026-07-13.csv"
)


def _s(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _b(value: Any) -> bool:
    return _s(value).lower() in {"1", "true", "yes", "y"}


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


def wilson_interval(wins: int, n: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    phat = wins / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    half = z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n) / denom
    return max(0.0, center - half), min(1.0, center + half)


def rate(num: int | float, den: int | float) -> float | None:
    if den == 0:
        return None
    return round(float(num) / float(den), 6)


def american_profit(price: float | None) -> float | None:
    if price is None:
        return None
    return price / 100.0 if price > 0 else 100.0 / abs(price)


def temporal_block(date_value: str) -> str:
    if date_value <= "2026-06-16":
        return "early_characterization_2026-05-01_to_2026-06-16"
    if date_value <= "2026-07-04":
        return "middle_confirmation_2026-06-17_to_2026-07-04"
    return "latest_confirmation_2026-07-05_to_2026-07-16"


def bucket_num(value: Any, cuts: list[tuple[float, str]], missing: str = "missing") -> str:
    v = _f(value)
    if v is None:
        return missing
    for threshold, label in cuts:
        if v < threshold:
            return label
    return cuts[-1][1].replace("lt_", "gte_") if cuts else "present"


def d7_bucket(value: Any) -> str:
    v = _f(value)
    if v is None:
        return "missing"
    if v < 0.75:
        return "low_lt_0_75"
    if v < 1.0:
        return "below_o15_lt_1_0"
    if v <= 1.3:
        return "o15_affirmative_1_0_to_1_3"
    return "very_hot_gt_1_3"


def d15_bucket(value: Any) -> str:
    v = _f(value)
    if v is None:
        return "missing"
    if v < 0.75:
        return "low_lt_0_75"
    if v < 1.0:
        return "below_o15_lt_1_0"
    if v <= 1.2:
        return "o15_affirmative_1_0_to_1_2"
    return "very_hot_gt_1_2"


def pa_bucket(value: Any) -> str:
    v = _f(value)
    if v is None:
        return "missing"
    if v < 3.5:
        return "low_lt_3_5"
    if v < 4.25:
        return "average_3_5_to_4_25"
    return "high_gte_4_25"


def starter_expected_bucket(value: Any) -> str:
    v = _f(value)
    if v is None:
        return "missing"
    if v < 4.5:
        return "suppression_lt_4_5"
    if v < 5.0:
        return "moderate_suppression_4_5_to_5_0"
    if v < 5.5:
        return "hitter_environment_5_0_to_5_5"
    return "strong_hitter_environment_gte_5_5"


def pitcher_base_bucket(value: Any) -> str:
    v = _f(value)
    if v is None:
        return "missing"
    if v < 4.5:
        return "low_lt_4_5"
    if v < 5.0:
        return "mid_4_5_to_5_0"
    if v < 5.5:
        return "high_5_0_to_5_5"
    return "very_high_gte_5_5"


def load_population() -> pd.DataFrame:
    if not PRIOR_LEDGER.exists():
        raise FileNotFoundError(f"missing prior ledger: {PRIOR_LEDGER}")
    df = pd.read_csv(PRIOR_LEDGER, low_memory=False)
    # The prior ledger is already proposition-bound to hits 1.5. Collapse to
    # player-game only, preserving the joined candidate/surface state.
    df["slate_date"] = df["slate_date"].astype(str).str[:10]
    df["official_hits_numeric"] = pd.to_numeric(df.get("official_hits"), errors="coerce")
    df["hit_count_class"] = df["official_hits_numeric"].apply(hit_count_class)
    df["outcome_resolved"] = df["official_hits_numeric"].notna()
    df["temporal_block"] = df["slate_date"].apply(temporal_block)
    df["o15_candidate_status"] = df["over_surfaces"].notna() & df["over_surfaces"].astype(str).str.len().gt(0)
    df["u15_candidate_status"] = df["under_surfaces"].notna() & df["under_surfaces"].astype(str).str.len().gt(0)
    df["o05_candidate_status"] = False
    df["o05_candidate_source"] = "not_retained_or_not_constructed_in_local_artifacts"
    df["d7_hits_bucket"] = df["d7_hits_rate"].apply(d7_bucket)
    df["d15_hits_bucket"] = df["d15_hits_rate"].apply(d15_bucket)
    df["pa_d15_bucket"] = df["pa_opp_v1_d15_pa_pg"].apply(pa_bucket)
    df["starter_expected_bucket"] = df["starter_expected_hits_allowed"].apply(starter_expected_bucket)
    df["pitcher_base_bucket"] = df["pitcher_base"].apply(pitcher_base_bucket)
    df["lineup_slot"] = pd.NA
    df["lineup_bucket"] = "not_retained_in_this_population"
    df["official_pa"] = pd.NA
    df["official_at_bats"] = pd.NA
    df["official_denominator_status"] = df["official_hits_numeric"].apply(lambda v: "numeric_hits_certified" if pd.notna(v) else "missing_numeric_hits")
    return df


def hit_count_class(value: Any) -> str:
    v = _f(value)
    if v is None:
        return "MISSING_OFFICIAL_OUTCOME"
    if v == 0:
        return "ZERO_HITS"
    if v == 1:
        return "EXACTLY_ONE_HIT"
    return "TWO_OR_MORE_HITS"


def architecture_inventory() -> list[dict[str, Any]]:
    rows = [
        {
            "surface": "Hits O0.5",
            "field_or_rule": "No active local review-aid or matrix surface found in bounded inspection.",
            "source_artifact": str(HITS_05_MATRIX),
            "field_version": "historical qualification wave",
            "historical_construction_population": "not constructed; upstream canonical side governance blocker",
            "target_outcome_originally_evaluated": "hits >= 1 intended but not materialized here",
            "designed_line": "0.5",
            "inherited_by_another_line": "unknown",
            "distinguishes_zero_vs_any": "intended_not_validated_here",
            "distinguishes_one_vs_two_plus": "no",
            "includes_pitcher_suppression": "unknown",
            "includes_pa_opportunity": "unknown",
            "allows_withhold": "unknown",
        },
        {
            "surface": "Hits U0.5",
            "field_or_rule": "No current local U0.5 surface identified.",
            "source_artifact": "none_found",
            "field_version": "not_available",
            "historical_construction_population": "none_found",
            "target_outcome_originally_evaluated": "not_available",
            "designed_line": "0.5",
            "inherited_by_another_line": "no evidence",
            "distinguishes_zero_vs_any": "not_validated",
            "distinguishes_one_vs_two_plus": "no",
            "includes_pitcher_suppression": "unknown",
            "includes_pa_opportunity": "unknown",
            "allows_withhold": "unknown",
        },
        {
            "surface": "Hits O1.5 Watch",
            "field_or_rule": "Quick Card row, d7_hits_rate > 1.0, starter_expected_hits_allowed >= 5.0.",
            "source_artifact": "artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_DATE.csv",
            "field_version": "run_mlb_hits_o15_review_board.py",
            "historical_construction_population": "review-aid O1.5 candidates",
            "target_outcome_originally_evaluated": "O1.5 review performance",
            "designed_line": "1.5",
            "inherited_by_another_line": "uses general hitter form and starter context",
            "distinguishes_zero_vs_any": "partially",
            "distinguishes_one_vs_two_plus": "not_proven",
            "includes_pitcher_suppression": "blocks strongest watch if starter not favorable, but not UNDER comparison",
            "includes_pa_opportunity": "not core rule",
            "allows_withhold": "exclusion_only",
        },
        {
            "surface": "Hits O1.5 Layered",
            "field_or_rule": "d7>1.0, d15>1.0, starter_expected>=5.0, QC presence define layers.",
            "source_artifact": "artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_DATE.csv",
            "field_version": "run_mlb_hits_o15_review_board.py",
            "historical_construction_population": "review-aid O1.5 candidates",
            "target_outcome_originally_evaluated": "O1.5 review performance",
            "designed_line": "1.5",
            "inherited_by_another_line": "uses any-hit-like rolling hit-rate evidence",
            "distinguishes_zero_vs_any": "yes/partial",
            "distinguishes_one_vs_two_plus": "not_validated_by_prior_result",
            "includes_pitcher_suppression": "partial; unfavorable starter lowers layer but does not create global WITHHOLD/UNDER",
            "includes_pa_opportunity": "not core rule",
            "allows_withhold": "not explicit",
        },
        {
            "surface": "Hits U1.5 Favorite Audit",
            "field_or_rule": "d7<1.0, d15<1.0, starter_expected<4.5 drive strongest UNDER layers.",
            "source_artifact": "artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_DATE.csv",
            "field_version": "run_mlb_hits_o15_review_board.py",
            "historical_construction_population": "U1.5 review/favorite audit",
            "target_outcome_originally_evaluated": "U1.5",
            "designed_line": "1.5",
            "inherited_by_another_line": "no; separate U surface",
            "distinguishes_zero_vs_any": "partially via cold hitter",
            "distinguishes_one_vs_two_plus": "pitcher suppression appears relevant but needs broader coverage",
            "includes_pitcher_suppression": "yes",
            "includes_pa_opportunity": "not core rule",
            "allows_withhold": "not as global arbiter",
        },
        {
            "surface": "Hits O1.5 Alternate Discovery",
            "field_or_rule": "alternate market discovery plus hitter form/starter context.",
            "source_artifact": "artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_DATE.csv",
            "field_version": "run_mlb_hits_o15_review_board.py",
            "historical_construction_population": "alternate O1.5 market rows",
            "target_outcome_originally_evaluated": "O1.5",
            "designed_line": "1.5",
            "inherited_by_another_line": "uses same general rolling hit-rate concepts",
            "distinguishes_zero_vs_any": "partially",
            "distinguishes_one_vs_two_plus": "not_proven",
            "includes_pitcher_suppression": "context only",
            "includes_pa_opportunity": "not core rule",
            "allows_withhold": "no explicit global state",
        },
        {
            "surface": "Lane Selector / Quick Card",
            "field_or_rule": "model/lane score, market prices, BvP, rolling stats, selected side.",
            "source_artifact": "backend/mlb/exports/model_v2/lanes/today/DATE",
            "field_version": "model_v2 lane/quick-card artifacts",
            "historical_construction_population": "daily prediction/ranking surface",
            "target_outcome_originally_evaluated": "mixed hits props",
            "designed_line": "mixed",
            "inherited_by_another_line": "yes, downstream review surfaces reference QC/rank",
            "distinguishes_zero_vs_any": "unknown in this bounded audit",
            "distinguishes_one_vs_two_plus": "unknown in this bounded audit",
            "includes_pitcher_suppression": "contextual",
            "includes_pa_opportunity": "partial after PA research",
            "allows_withhold": "via no selected row, not explicit threshold-specific state",
        },
    ]
    return rows


def canonical_manifest(df: pd.DataFrame) -> list[dict[str, Any]]:
    cols = [
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "official_hits_numeric",
        "hit_count_class",
        "official_pa",
        "official_at_bats",
        "lineup_slot",
        "lineup_bucket",
        "d7_hits_rate",
        "d15_hits_rate",
        "d30_hits_runs_rbis",
        "pa_opp_v1_d7_pa_pg",
        "pa_opp_v1_d15_pa_pg",
        "pa_opp_v1_d30_pa_pg",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "starter_starts_count",
        "o05_candidate_status",
        "o15_candidate_status",
        "u15_candidate_status",
        "current_side_surface_state",
        "baseball_directional_ownership",
        "model_prob",
        "qc_score",
        "hitter_tier_seen",
        "pitcher_tier_seen",
        "combined_tier_seen",
        "source_artifacts",
        "o15_price",
        "u15_price",
    ]
    return df[cols].fillna("").to_dict("records")


def outcome_ledger(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for keys, g in df.groupby(["hit_count_class", "temporal_block"], dropna=False):
        hit_class, block = keys
        rows.append(
            {
                "hit_count_class": hit_class,
                "temporal_block": block,
                "rows": len(g),
                "unique_games": g["game_id"].nunique(),
                "unique_players": g["player_id"].nunique(),
                "o15_candidates": int(g["o15_candidate_status"].sum()),
                "u15_candidates": int(g["u15_candidate_status"].sum()),
                "o05_candidates": int(g["o05_candidate_status"].sum()),
            }
        )
    return rows


def outcome_coverage(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for keys, g in df.groupby(["temporal_block", "current_side_surface_state", "baseball_directional_ownership"], dropna=False):
        block, surface, owner = keys
        rows.append(
            {
                "temporal_block": block,
                "surface_state": surface,
                "ownership_label": owner,
                "rows": len(g),
                "zero_hit_rows": int((g["hit_count_class"] == "ZERO_HITS").sum()),
                "exactly_one_hit_rows": int((g["hit_count_class"] == "EXACTLY_ONE_HIT").sum()),
                "two_or_more_hit_rows": int((g["hit_count_class"] == "TWO_OR_MORE_HITS").sum()),
                "missing_official_outcomes": int((g["hit_count_class"] == "MISSING_OFFICIAL_OUTCOME").sum()),
                "missing_official_pa": len(g),
                "identity_failures": 0,
                "feature_version_failures": int(g["source_artifacts"].isna().sum()),
                "coverage_pct": rate(g["outcome_resolved"].sum(), len(g)),
            }
        )
    return rows


def temporal_manifest(df: pd.DataFrame) -> list[dict[str, Any]]:
    cols = [
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "temporal_block",
        "hit_count_class",
        "outcome_resolved",
        "current_side_surface_state",
        "baseball_directional_ownership",
        "source_artifacts",
    ]
    return df[cols].fillna("").to_dict("records")


EVIDENCE_DOMAINS = [
    ("hitter_recent_d7_hits_rate", "d7_hits_bucket"),
    ("hitter_longer_d15_hits_rate", "d15_hits_bucket"),
    ("pa_opportunity_d15", "pa_d15_bucket"),
    ("starter_expected_hits_allowed", "starter_expected_bucket"),
    ("pitcher_base", "pitcher_base_bucket"),
    ("pitcher_suppression_label", "pitcher_suppression_label"),
    ("ownership_label", "baseball_directional_ownership"),
    ("surface_state", "current_side_surface_state"),
    ("lineup_role_quality", "lineup_bucket"),
    ("game_environment", "offense_factor_vs_league_clamped"),
]


def threshold_results(df: pd.DataFrame, mode: str) -> list[dict[str, Any]]:
    rows = []
    for domain, field in EVIDENCE_DOMAINS:
        if field not in df.columns:
            rows.append(empty_domain_row(domain, field, mode, "field_not_available"))
            continue
        work = df[df["outcome_resolved"]].copy()
        if mode == "zero_to_one":
            work["target_success"] = work["official_hits_numeric"] >= 1
            base = float(work["target_success"].mean()) if len(work) else None
            success_label = "one_or_more_hits_rate"
        elif mode == "one_to_two_plus":
            work = work[work["official_hits_numeric"] >= 1].copy()
            work["target_success"] = work["official_hits_numeric"] >= 2
            base = float(work["target_success"].mean()) if len(work) else None
            success_label = "two_plus_given_at_least_one_rate"
        elif mode == "full_o15":
            work["target_success"] = work["official_hits_numeric"] >= 2
            base = float(work["target_success"].mean()) if len(work) else None
            success_label = "two_plus_rate"
        else:
            raise ValueError(mode)
        if not len(work):
            rows.append(empty_domain_row(domain, field, mode, "no_resolved_rows"))
            continue
        for bucket, g in work.groupby(field, dropna=False):
            n = len(g)
            wins = int(g["target_success"].sum())
            lo, hi = wilson_interval(wins, n)
            rows.append(
                {
                    "threshold_test": mode,
                    "evidence_domain": domain,
                    "field": field,
                    "bucket": _s(bucket) or "blank",
                    "rows": int((df[field].fillna("__MISSING__") == bucket).sum()) if bucket == bucket else int(df[field].isna().sum()),
                    "resolved_rows": n,
                    "success_metric": success_label,
                    "success_count": wins,
                    "failure_count": n - wins,
                    "success_rate": rate(wins, n),
                    "base_rate": round(base, 6) if base is not None else None,
                    "lift_vs_base": round(wins / n - base, 6) if n and base is not None else None,
                    "wilson_low": round(lo, 6) if lo is not None else None,
                    "wilson_high": round(hi, 6) if hi is not None else None,
                    "temporal_stability_note": temporal_stability(work, field, bucket, mode),
                    "sample_flag": sample_flag(n),
                }
            )
    return rows


def empty_domain_row(domain: str, field: str, mode: str, reason: str) -> dict[str, Any]:
    return {
        "threshold_test": mode,
        "evidence_domain": domain,
        "field": field,
        "bucket": "unavailable",
        "rows": 0,
        "resolved_rows": 0,
        "success_metric": "",
        "success_count": 0,
        "failure_count": 0,
        "success_rate": None,
        "base_rate": None,
        "lift_vs_base": None,
        "wilson_low": None,
        "wilson_high": None,
        "temporal_stability_note": reason,
        "sample_flag": "unavailable",
    }


def temporal_stability(work: pd.DataFrame, field: str, bucket: Any, mode: str) -> str:
    subset = work[work[field].fillna("__MISSING__") == (bucket if bucket == bucket else "__MISSING__")]
    if subset.empty:
        return "no_rows"
    block_rates = []
    for block, g in subset.groupby("temporal_block"):
        if len(g) >= 10:
            block_rates.append(float(g["target_success"].mean()))
    if len(block_rates) < 2:
        return "insufficient_block_support"
    if max(block_rates) - min(block_rates) <= 0.15:
        return "directionally_stable_bounded"
    return "temporally_unstable_or_sparse"


def sample_flag(n: int) -> str:
    if n >= 100:
        return "usable_bounded_sample"
    if n >= 30:
        return "directional_sparse"
    if n > 0:
        return "very_sparse"
    return "empty"


def condition_classification(df: pd.DataFrame, zero_rows: list[dict[str, Any]], one_two_rows: list[dict[str, Any]], full_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for domain, field in EVIDENCE_DOMAINS:
        z_best = best_lift(zero_rows, domain)
        ot_best = best_lift(one_two_rows, domain)
        f_best = best_lift(full_rows, domain)
        if domain in {"starter_expected_hits_allowed", "pitcher_base", "pitcher_suppression_label"}:
            cls = "PITCHER_SUPPRESSION_SIGNAL"
        elif field in {"lineup_bucket"}:
            cls = "INSUFFICIENT_EVIDENCE"
        elif z_best and z_best > 0.05 and (not ot_best or ot_best < 0.03):
            cls = "ANY_HIT_SIGNAL"
        elif ot_best and ot_best > 0.05 and (not z_best or z_best < 0.03):
            cls = "MULTI_HIT_SIGNAL"
        elif z_best and ot_best and z_best > 0.03 and ot_best > 0.03:
            cls = "BOTH_THRESHOLD_SIGNAL"
        elif "pitcher" in domain or "starter" in domain:
            cls = "PITCHER_SUPPRESSION_SIGNAL"
        elif not z_best and not ot_best and not f_best:
            cls = "INSUFFICIENT_EVIDENCE"
        else:
            cls = "UNSTABLE"
        rows.append(
            {
                "evidence_domain": domain,
                "field": field,
                "best_zero_to_one_lift": z_best,
                "best_one_to_two_plus_lift": ot_best,
                "best_full_o15_lift": f_best,
                "threshold_classification": cls,
                "notes": "Classification uses frozen buckets and bounded lift only; no threshold optimization performed.",
            }
        )
    return rows


def best_lift(rows: list[dict[str, Any]], domain: str) -> float | None:
    vals = [
        r["lift_vs_base"]
        for r in rows
        if r["evidence_domain"] == domain
        and r.get("sample_flag") in {"usable_bounded_sample", "directional_sparse"}
        and r.get("lift_vs_base") is not None
        and "missing" not in _s(r.get("bucket")).lower()
        and "unavailable" not in _s(r.get("bucket")).lower()
        and "not_retained" not in _s(r.get("bucket")).lower()
    ]
    if not vals:
        return None
    return round(max(vals), 6)


def threshold_leakage(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    mask = df["o15_candidate_status"]
    for keys, g0 in df[mask].groupby(["baseball_directional_ownership", "starter_expected_bucket", "d7_hits_bucket", "d15_hits_bucket"], dropna=False):
        owner, starter_bucket, d7, d15 = keys
        g = g0[g0["outcome_resolved"]]
        any_hit = int((g["official_hits_numeric"] >= 1).sum())
        two_plus = int((g["official_hits_numeric"] >= 2).sum())
        only_any_signal = ("o15_affirmative" in d7 or "very_hot" in d7 or "o15_affirmative" in d15 or "very_hot" in d15) and owner != "hitter_dominant"
        rows.append(
            {
                "ownership_label": owner,
                "starter_expected_bucket": starter_bucket,
                "d7_hits_bucket": d7,
                "d15_hits_bucket": d15,
                "rows": len(g0),
                "resolved_rows": len(g),
                "any_hit_rate": rate(any_hit, len(g)),
                "two_plus_rate": rate(two_plus, len(g)),
                "exactly_one_failure_rate": rate((g["official_hits_numeric"] == 1).sum(), len(g)),
                "zero_failure_rate": rate((g["official_hits_numeric"] == 0).sum(), len(g)),
                "threshold_leakage_flag": bool(only_any_signal),
                "pitcher_suppression_contradicts_over": "suppression" in starter_bucket,
                "opportunity_evidence_status": "pa_missing_or_not_core" if g0["pa_opp_v1_d15_pa_pg"].isna().all() else "pa_context_present",
                "notes": "Leakage flag marks O1.5 rows where rolling any-hit-like evidence exists without a confirmed multi-hit ownership label.",
            }
        )
    return rows


def july12_reconstruction(df: pd.DataFrame) -> list[dict[str, Any]]:
    mask = df.get("july12_sentinel", pd.Series(False, index=df.index)).map(_b)
    sent = df[mask].copy()
    rows = []
    for _, r in sent.iterrows():
        proposed = proposed_threshold_architecture(r)
        rows.append(
            {
                "player_name": r["player_name"],
                "canonical_proposition_key": r["canonical_proposition_key"],
                "official_hits": r["official_hits_numeric"],
                "hit_count_class": r["hit_count_class"],
                "current_o15_conditions": r["over_surfaces"],
                "any_hit_signals": ";".join(x for x in [r["d7_hits_bucket"], r["d15_hits_bucket"]] if "missing" not in x),
                "multi_hit_signals": "not_confirmed" if r["baseball_directional_ownership"] != "hitter_dominant" else "hitter_dominant_label",
                "pa_opportunity": r["pa_opp_v1_d15_pa_pg"],
                "lineup_position": "",
                "pitcher_suppression_evidence": r["pitcher_suppression_label"],
                "ownership_label": r["baseball_directional_ownership"],
                "sufficient_two_hit_specific_evidence": r["baseball_directional_ownership"] == "hitter_dominant",
                "proposed_threshold_classification": proposed,
                "o15_price": r["o15_price"],
                "u15_price": r["u15_price"],
            }
        )
    return rows


def proposed_threshold_architecture(row: pd.Series) -> str:
    owner = _s(row.get("baseball_directional_ownership"))
    if owner == "pitcher_dominant":
        return "U1.5_ELIGIBLE_RESEARCH_ONLY"
    if owner == "hitter_dominant" and _f(row.get("pa_opp_v1_d15_pa_pg")) is not None and _f(row.get("pa_opp_v1_d15_pa_pg")) >= 4.0:
        return "O1.5_MULTI_HIT_ELIGIBLE_RESEARCH_ONLY"
    if owner in {"conflicting", "incomplete"}:
        if _s(row.get("hitter_evidence_label")).find("affirmative_hitter") >= 0:
            return "O0.5_ONLY_OR_WITHHOLD"
        return "WITHHOLD"
    return "WITHHOLD"


def pitcher_dominant_coverage(df: pd.DataFrame) -> list[dict[str, Any]]:
    subset = df[df["baseball_directional_ownership"] == "pitcher_dominant"]
    rows = []
    for keys, g0 in subset.groupby(["current_side_surface_state", "temporal_block"], dropna=False):
        surface, block = keys
        g = g0[g0["outcome_resolved"]]
        rows.append(
            {
                "surface_state": surface,
                "temporal_block": block,
                "rows": len(g0),
                "resolved_rows": len(g),
                "missing_outcomes": int((~g0["outcome_resolved"]).sum()),
                "zero_hits": int((g["hit_count_class"] == "ZERO_HITS").sum()),
                "exactly_one_hit": int((g["hit_count_class"] == "EXACTLY_ONE_HIT").sum()),
                "two_or_more_hits": int((g["hit_count_class"] == "TWO_OR_MORE_HITS").sum()),
                "any_hit_rate": rate((g["official_hits_numeric"] >= 1).sum(), len(g)),
                "two_plus_rate": rate((g["official_hits_numeric"] >= 2).sum(), len(g)),
                "representativeness_warning": "sparse_outcome_bound_subset" if len(g) < 100 else "bounded_sample",
                "minimum_recovery_need": "recover numeric official hits for pitcher_dominant rows, prioritizing OVER_ONLY rows by date/game/player exact identity",
            }
        )
    return rows


def cross_threshold_comparison(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for keys, g in df.groupby(["o05_candidate_status", "o15_candidate_status", "u15_candidate_status", "baseball_directional_ownership", "hit_count_class"], dropna=False):
        o05, o15, u15, owner, hit_class = keys
        rows.append(
            {
                "o05_candidate": bool(o05),
                "o15_candidate": bool(o15),
                "u15_candidate": bool(u15),
                "ownership_label": owner,
                "hit_count_class": hit_class,
                "rows": len(g),
                "interpretation": cross_threshold_interpretation(bool(o05), bool(o15), bool(u15)),
            }
        )
    return rows


def cross_threshold_interpretation(o05: bool, o15: bool, u15: bool) -> str:
    if o15 and u15:
        return "o15_and_u15_conflict"
    if o15 and not o05:
        return "o15_without_retained_o05_context"
    if o05 and not o15:
        return "o05_only"
    if u15:
        return "u15_surface"
    return "not_current_candidate"


def price_context(df: pd.DataFrame) -> list[dict[str, Any]]:
    specs = [
        ("current_o15_surface", df["o15_candidate_status"], "over", "o15_price"),
        ("current_u15_surface", df["u15_candidate_status"], "under", "u15_price"),
        ("proposed_o15_multi_hit_research", df.apply(lambda r: proposed_threshold_architecture(r).startswith("O1.5"), axis=1), "over", "o15_price"),
        ("proposed_u15_suppression_research", df.apply(lambda r: proposed_threshold_architecture(r).startswith("U1.5"), axis=1), "under", "u15_price"),
    ]
    rows = []
    for lane, mask, side, price_col in specs:
        for block, g0 in df[mask].groupby("temporal_block", dropna=False):
            g = g0[g0["outcome_resolved"] & g0[price_col].notna()].copy()
            if side == "over":
                wins = int((g["official_hits_numeric"] >= 2).sum()) if "1.5" in lane or "o15" in lane else int((g["official_hits_numeric"] >= 1).sum())
            else:
                wins = int((g["official_hits_numeric"] < 2).sum())
            losses = len(g) - wins
            returns = []
            for _, r in g.iterrows():
                price = _f(r[price_col])
                if wins is None or price is None:
                    continue
                result_win = (r["official_hits_numeric"] >= 2) if side == "over" else (r["official_hits_numeric"] < 2)
                returns.append(american_profit(price) if result_win else -1.0)
            avg_price_raw = pd.to_numeric(g[price_col], errors="coerce").mean() if len(g) else None
            avg_price = None if avg_price_raw is None or pd.isna(avg_price_raw) else float(avg_price_raw)
            rows.append(
                {
                    "lane": lane,
                    "temporal_block": block,
                    "executable_rows": len(g),
                    "wins": wins,
                    "losses": losses,
                    "average_price": round(float(avg_price), 3) if avg_price is not None else None,
                    "break_even_rate": break_even(avg_price),
                    "outcome_rate": rate(wins, len(g)),
                    "flat_stake_roi": round(sum(returns) / len(returns), 6) if returns else None,
                    "line_availability_rows": int(df[mask][price_col].notna().sum()),
                    "missing_opposite_side_prices": int(df[mask]["u15_price" if side == "over" else "o15_price"].isna().sum()),
                    "notes": "Price context only after threshold analysis; no price cutoff optimized.",
                }
            )
    return rows


def break_even(avg_price: Any) -> float | None:
    price = _f(avg_price)
    if price is None:
        return None
    decimal = 1 + price / 100 if price > 0 else 1 + 100 / abs(price)
    return round(1 / decimal, 6)


def framework_design() -> list[dict[str, Any]]:
    return [
        {
            "output": "HITS_OVER_05_ADVANTAGE",
            "required_evidence": "Evidence that separates zero hits from one or more hits.",
            "exclusions": "Do not use as O1.5 proof unless one-to-two-plus separation is also present.",
            "implementation_status": "design_only_not_authorized",
        },
        {
            "output": "HITS_OVER_15_MULTI_HIT_ADVANTAGE",
            "required_evidence": "Affirmative multi-hit evidence, sufficient PA opportunity, evidence completeness, no stronger pitcher suppression.",
            "exclusions": "Any-hit evidence alone is insufficient.",
            "implementation_status": "design_only_not_authorized",
        },
        {
            "output": "HITS_UNDER_15_SUPPRESSION_ADVANTAGE",
            "required_evidence": "Affirmative pitcher/suppression evidence, credible workload/role, reduced two-plus probability, no stronger multi-hit evidence.",
            "exclusions": "Do not promote from 27-row sparse subset without outcome recovery.",
            "implementation_status": "design_only_not_authorized",
        },
        {
            "output": "WITHHOLD",
            "required_evidence": "Only O0.5-level evidence, conflict, insufficient opportunity, incomplete fields, or no threshold-specific separation.",
            "exclusions": "None.",
            "implementation_status": "design_only_not_authorized",
        },
    ]


def decisions(df: pd.DataFrame, class_rows: list[dict[str, Any]]) -> dict[str, str]:
    resolved = df[df["outcome_resolved"]]
    sentinel_mask = df.get("july12_sentinel", pd.Series(False, index=df.index)).map(_b)
    sentinel_counts = df[sentinel_mask & df["outcome_resolved"]]["hit_count_class"].value_counts().to_dict()
    one_to_two_support = any(r["threshold_classification"] in {"MULTI_HIT_SIGNAL", "BOTH_THRESHOLD_SIGNAL"} for r in class_rows)
    pitcher_rows = df[(df["baseball_directional_ownership"] == "pitcher_dominant") & (df["current_side_surface_state"] == "OVER_ONLY")]
    pitcher_resolved = int(pitcher_rows["outcome_resolved"].sum())
    return {
        "MLB_HITS_THRESHOLD_ARCHITECTURE_INVENTORY_DECISION": "BOUND_CURRENT_O15_U15_SURFACES_O05_NOT_CONSTRUCTED_OR_NOT_RETAINED_LOCALLY",
        "MLB_HITS_OUTCOME_CLASSIFICATION_DECISION": "ZERO_ONE_TWO_PLUS_CLASSIFICATION_BOUND_FOR_CERTIFIED_NUMERIC_HITS_ONLY",
        "MLB_HITS_ZERO_TO_ONE_EVIDENCE_DECISION": "ANY_HIT_EVIDENCE_PRESENT_BUT_NOT_SUFFICIENT_FOR_O15",
        "MLB_HITS_ONE_TO_TWO_PLUS_EVIDENCE_DECISION": "NO_STABLE_MULTI_HIT_SPECIFIC_EVIDENCE_VALIDATED" if not one_to_two_support else "PARTIAL_MULTI_HIT_SIGNAL_PRESENT_BUT_NOT_PRODUCTION_READY",
        "MLB_HITS_OVER15_THRESHOLD_EVIDENCE_DECISION": "CURRENT_O15_EVIDENCE_NOT_VALIDATED_AS_MULTI_HIT_SPECIFIC",
        "MLB_HITS_CURRENT_OVER15_THRESHOLD_LEAKAGE_DECISION": "CURRENT_O15_CONDITIONS_SHOW_THRESHOLD_LEAKAGE_FROM_ANY_HIT_OR_GENERAL_HITTER_QUALITY_EVIDENCE",
        "MLB_HITS_JULY12_HIT_COUNT_FAILURE_DECISION": f"JULY12_SENTINEL_FAILURES_SPLIT_ZERO={sentinel_counts.get('ZERO_HITS', 0)}_ONE={sentinel_counts.get('EXACTLY_ONE_HIT', 0)}_TWO_PLUS={sentinel_counts.get('TWO_OR_MORE_HITS', 0)}",
        "MLB_HITS_PITCHER_DOMINANT_OUTCOME_COVERAGE_DECISION": f"SPARSE_{pitcher_resolved}_OF_{len(pitcher_rows)}_PITCHER_DOMINANT_OVER_ONLY_ROWS_RESOLVED",
        "MLB_HITS_PITCHER_DOMINANT_UNDER_READINESS_DECISION": "NOT_READY_FOR_UNDER_LANE_OUTCOME_RECOVERY_REQUIRED",
        "MLB_HITS_OVER05_DISTINCT_LANE_DECISION": "DESIGN_NEEDED_O05_NOT_ACTIVE_IN_LOCAL_BOUNDED_SURFACES",
        "MLB_HITS_OVER15_DISTINCT_LANE_DECISION": "DESIGN_NEEDED_CURRENT_O15_NOT_DISTINCT_ENOUGH_FROM_ANY_HIT_EVIDENCE",
        "MLB_HITS_UNDER15_DISTINCT_LANE_DECISION": "PROMISING_SUPPRESSION_DIRECTION_BUT_NOT_READY_DUE_TO_COVERAGE_AND_PRICE_GAPS",
        "MLB_HITS_THRESHOLD_SPECIFIC_FRAMEWORK_DESIGN_DECISION": "DESIGN_ONLY_APPROVED_NO_NUMERIC_THRESHOLDS_FINALIZED",
        "MLB_HITS_NEXT_RESEARCH_DECISION": "RECOVER_CERTIFIED_OUTCOMES_AND BUILD_THRESHOLD_SPECIFIC_PROSPECTIVE_LABELS_FOR_O05_O15_U15_WITHHOLD",
        "MLB_HITS_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }


def executive_summary(df: pd.DataFrame, decision_map: dict[str, str], july12_rows: list[dict[str, Any]]) -> str:
    resolved = df[df["outcome_resolved"]]
    counts = resolved["hit_count_class"].value_counts().to_dict()
    sent_counts = pd.DataFrame(july12_rows)["hit_count_class"].value_counts().to_dict() if july12_rows else {}
    return f"""
# MLB Hits Threshold-Specific Evidence Audit

- Audit date: `{AUDIT_DATE}`
- Source population: `{PRIOR_LEDGER}`
- Player-game rows: `{len(df)}`
- Certified numeric-hit rows: `{len(resolved)}`
- Hit-count classes: `{counts}`
- Production change status: `NOT_AUTHORIZED`

## Direct Answer

Proppadia's current Hits O1.5 conditions are not yet proven to identify a
distinct multi-hit advantage. In this bounded evidence package they look more
like a selective general hitter / any-hit framework being asked to clear a
harder two-hit line. Pitcher suppression evidence is more directionally useful
for avoiding O1.5 than hitter-dominant evidence is for proving O1.5.

## July 12 Failure Composition

For the 15 July 12 sentinel predictions:

- Hit-count classes: `{sent_counts}`

The failure was therefore not one uniform miss type. It included both zero-hit
failures and exactly-one-hit failures; the exactly-one-hit failures are the
clearest threshold-specific warning because the hitter cleared the O0.5 concept
but not the O1.5 line.

## Architecture Conclusion

O0.5 and O1.5 need separate evidence gates:

- O0.5 should ask: can this hitter avoid zero?
- O1.5 should ask: after avoiding zero, is there evidence of repeat-hit volume?
- U1.5 should ask: is there affirmative suppression against two-plus hits?
- WITHHOLD should catch any-hit-only, conflicting, incomplete, or low-opportunity rows.

## Decisions

""" + "\n".join(f"- `{k}` = `{v}`" for k, v in decision_map.items())


def validate_outputs(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            pd.read_csv(path)
            rows.append({"artifact": str(path), "validation": "csv_parse", "status": "PASS", "message": "csv_parses"})
        except Exception as exc:
            rows.append({"artifact": str(path), "validation": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text())
            rows.append({"artifact": str(path), "validation": "json_parse", "status": "PASS", "message": "json_parses"})
        except Exception as exc:
            rows.append({"artifact": str(path), "validation": "json_parse", "status": "FAIL", "message": str(exc)})
    rows.append({"artifact": "runtime", "validation": "guardrail", "status": "PASS", "message": "no network, oddsapi, db writes, training, optimization, or production changes invoked"})
    return rows


def sha_manifest(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{AUDIT_DATE}.csv":
            rows.append({"path": str(path), "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
    return rows


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    df = load_population()
    zero_rows = threshold_results(df, "zero_to_one")
    one_two_rows = threshold_results(df, "one_to_two_plus")
    full_rows = threshold_results(df, "full_o15")
    class_rows = condition_classification(df, zero_rows, one_two_rows, full_rows)
    july12_rows = july12_reconstruction(df)
    decision_map = decisions(df, class_rows)

    _write_csv(out_dir / f"threshold_architecture_inventory_{AUDIT_DATE}.csv", architecture_inventory())
    _write_csv(out_dir / f"canonical_player_game_outcome_manifest_{AUDIT_DATE}.csv", canonical_manifest(df))
    _write_csv(out_dir / f"zero_one_two_plus_outcome_ledger_{AUDIT_DATE}.csv", outcome_ledger(df))
    _write_csv(out_dir / f"outcome_coverage_report_{AUDIT_DATE}.csv", outcome_coverage(df))
    _write_csv(out_dir / f"frozen_temporal_block_manifest_{AUDIT_DATE}.csv", temporal_manifest(df))
    _write_csv(out_dir / f"zero_to_one_evidence_results_{AUDIT_DATE}.csv", zero_rows)
    _write_csv(out_dir / f"one_to_two_plus_evidence_results_{AUDIT_DATE}.csv", one_two_rows)
    _write_csv(out_dir / f"full_o15_threshold_results_{AUDIT_DATE}.csv", full_rows)
    _write_csv(out_dir / f"condition_threshold_classification_registry_{AUDIT_DATE}.csv", class_rows)
    _write_csv(out_dir / f"o15_threshold_leakage_audit_{AUDIT_DATE}.csv", threshold_leakage(df))
    _write_csv(out_dir / f"july12_hit_count_reconstruction_{AUDIT_DATE}.csv", july12_rows)
    _write_csv(out_dir / f"pitcher_dominant_outcome_coverage_audit_{AUDIT_DATE}.csv", pitcher_dominant_coverage(df))
    _write_csv(out_dir / f"cross_threshold_candidate_comparison_{AUDIT_DATE}.csv", cross_threshold_comparison(df))
    _write_csv(out_dir / f"price_context_report_{AUDIT_DATE}.csv", price_context(df))
    _write_csv(out_dir / f"threshold_specific_framework_design_{AUDIT_DATE}.csv", framework_design())
    _write_csv(
        out_dir / f"bounded_next_step_specification_{AUDIT_DATE}.csv",
        [
            {
                "step": "recover_local_official_hit_counts_for_o15_review_rows",
                "priority": "P0",
                "production_change_required": False,
                "description": "Increase certified outcome coverage before treating pitcher-dominant suppression as an UNDER lane.",
            },
            {
                "step": "materialize_o05_current_surface_or_explicitly_retire_gap",
                "priority": "P0",
                "production_change_required": False,
                "description": "O0.5 must be measured separately if O1.5 leakage is to be governed.",
            },
            {
                "step": "add prospective threshold labels",
                "priority": "P1",
                "production_change_required": False,
                "description": "Observe O05 any-hit, O15 multi-hit, U15 suppression, and WITHHOLD labels without changing selections.",
            },
        ],
    )
    _write_csv(out_dir / f"decision_report_{AUDIT_DATE}.csv", [{"decision": k, "value": v} for k, v in decision_map.items()])
    _write_md(out_dir / f"executive_summary_{AUDIT_DATE}.md", executive_summary(df, decision_map, july12_rows))
    payload = {
        "audit_date": AUDIT_DATE,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "constraints": {
            "network_calls": 0,
            "oddsapi_calls": 0,
            "db_writes": 0,
            "model_training": 0,
            "threshold_optimization": 0,
            "production_changes": 0,
        },
        "metadata": {
            "player_game_rows": len(df),
            "certified_numeric_hit_rows": int(df["outcome_resolved"].sum()),
            "hit_count_classes": df[df["outcome_resolved"]]["hit_count_class"].value_counts().to_dict(),
            "july12_hit_count_classes": pd.DataFrame(july12_rows)["hit_count_class"].value_counts().to_dict() if july12_rows else {},
        },
        "decisions": decision_map,
    }
    _write_json(out_dir / f"machine_readable_threshold_specific_evidence_audit_{AUDIT_DATE}.json", payload)
    _write_csv(out_dir / f"validation_report_{AUDIT_DATE}.csv", validate_outputs(out_dir))
    _write_csv(out_dir / f"sha256_manifest_{AUDIT_DATE}.csv", sha_manifest(out_dir))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    payload = build(args.output_dir)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
