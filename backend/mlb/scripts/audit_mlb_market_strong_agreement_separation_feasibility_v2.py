#!/usr/bin/env python3
"""Pre-outcome feasibility amendment for the MLB agreement-separation study."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.power import NormalIndPower
from statsmodels.stats.proportion import proportion_effectsize

from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as v1


ROOT = v1.ROOT
OUT = ROOT / "artifacts/analysis/model_development/mlb_market_strong_agreement_separation_feasibility_v2/2026-09-09"
HISTORY = ROOT / "artifacts/analysis/model_development/mlb_joint_strength_incremental_value_executability_audit_v1/2026-09-09/unique_game_strength_class_ledger.csv"
HISTORY_SUMMARY = ROOT / "artifacts/analysis/model_development/mlb_joint_strength_incremental_value_executability_audit_v1/2026-09-09/summary.json"
TRANSFER = ROOT / "artifacts/analysis/model_development/mlb_oddsapi_historical_joint_strength_transfer_v1/2026-09-09"
CURRENT_MANIFEST_ROOT = ROOT / "backend/mlb/exports/odds_history"
DIAGNOSTIC_REQUESTS = ROOT / ("artifacts/analysis/model_development/mlb_oddsapi_betonline_exhaustive_surface_diagnostic/"
    "2026-07-18/oddsapi_betonline_surface_diag_20260718T234742Z/exhaustive_request_manifest_2026-07-18.csv")
V1_FREEZE = v1.OUT / "pre_outcome_freeze.json"
V2_STUDY = "MLB_MARKET_STRONG_AGREEMENT_SEPARATION_PROSPECTIVE_V2"
REGULAR_START = "2026-09-10"
REGULAR_END = "2026-09-27"
SCHEDULE_URL = ("https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=2026-09-10&"
                "endDate=2026-10-05&gameType=R")
EXPECTED_HISTORICAL_COST = 10
BOUNDED_GATE = {"resolved_comparison_games": 40, "agreement_games": 15,
                "without_agreement_games": 15, "resolved_dates": 10,
                "blocked_out_of_time_games": 20}
BOUNDED_CATEGORIES = {
    "2026_EVIDENCE_CONTRADICTS_INCREMENTAL_AGREEMENT_VALUE",
    "2026_DIRECTIONALLY_FAVORABLE_BUT_UNRESOLVED",
    "2026_NO_OBSERVABLE_SEPARATION",
    "2026_INSUFFICIENT_COMPARISON_SUPPORT",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clean(value: Any) -> Any:
    if isinstance(value, dict): return {str(key): clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)): return [clean(item) for item in value]
    if isinstance(value, (np.bool_,)): return bool(value)
    if isinstance(value, (np.integer,)): return int(value)
    if isinstance(value, (np.floating, float)): return None if not np.isfinite(value) else float(value)
    return value


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(clean(value), indent=2, sort_keys=True, allow_nan=False) + "\n")


def power_derivation() -> dict[str, Any]:
    agreement_prior, comparison_prior = 76, 51
    baseline = 31 / comparison_prior
    lift = .10
    ratio = comparison_prior / agreement_prior
    effect_size = proportion_effectsize(baseline + lift, baseline)
    agreement_exact = NormalIndPower().solve_power(
        effect_size=effect_size, power=.80, alpha=.05, ratio=ratio, alternative="two-sided")
    comparison_exact = agreement_exact * ratio
    return {
        "test": "two-independent-proportion normal approximation using Cohen arcsine effect size",
        "baseline_source": "31 wins / 51 market-strong without-model-agreement games",
        "baseline_win_rate": baseline,
        "minimum_detectable_incremental_effect_absolute": lift,
        "alternative_win_rate": baseline + lift,
        "agreement_to_comparison_allocation": f"{agreement_prior}:{comparison_prior}",
        "agreement_share": agreement_prior / (agreement_prior + comparison_prior),
        "comparison_share": comparison_prior / (agreement_prior + comparison_prior),
        "comparison_to_agreement_ratio": ratio,
        "two_sided_alpha": .05,
        "power": .80,
        "cohen_h": effect_size,
        "agreement_exact": agreement_exact,
        "comparison_exact": comparison_exact,
        "total_exact": agreement_exact + comparison_exact,
        "agreement_ceiling": math.ceil(agreement_exact),
        "comparison_ceiling": math.ceil(comparison_exact),
        "total_after_separate_group_ceilings": math.ceil(agreement_exact) + math.ceil(comparison_exact),
        "date_cluster_design_effect_in_original_732": 1.0,
        "missing_price_or_outcome_inflation_in_original_732": 1.0,
        "interpretation": ("732 is statistically derived and conservatively integer-rounded, but is an unclustered "
                           "eligible-resolved lower bound; its baseline and allocation are inherited from the earlier "
                           "127-game comparison and its 10-point MDE is a predeclared judgmental design choice."),
    }


def historical_feasibility() -> tuple[dict[str, Any], pd.DataFrame]:
    frame = pd.read_csv(HISTORY)
    market = frame[frame.market_strong_side.ne("NONE")].copy()
    market["agreement"] = market.strength_class.eq("JOINT_STRONG_SAME_SIDE").astype(int)
    all_dates = pd.Index(sorted(frame.game_date.unique()), name="game_date")
    daily = market.groupby("game_date").agg(
        market_strong=("game_id", "size"), agreement=("agreement", "sum")).reindex(all_dates, fill_value=0)
    daily["without_agreement"] = daily.market_strong - daily.agreement
    daily["all_pinnacle_priced_games"] = frame.groupby("game_date").size().reindex(all_dates, fill_value=0)
    daily = daily.reset_index()
    x = sm.add_constant(market.agreement)
    fit = sm.OLS(market.evaluated_win, x).fit()
    iid_se = fit.get_robustcov_results(cov_type="HC1").bse[1]
    clustered_se = fit.get_robustcov_results(
        cov_type="cluster", groups=market.game_date, use_correction=True).bse[1]
    summary = json.loads(HISTORY_SUMMARY.read_text())
    coverage = pd.read_csv(TRANSFER / "bookmaker_coverage_and_exclusions.csv")
    valid_cells = int(coverage.VALID_PREGAME_PRICE.sum())
    total_cells = int(coverage[["BOOKMAKER_ABSENT", "VALID_PREGAME_PRICE", "PRICE_UNAVAILABLE",
                                "ONE_SIDED_MARKET", "STALE_OR_POST_START_PRICE",
                                "IDENTITY_MISMATCH"]].sum().sum())
    predictions = int(summary["prediction_rows"])
    pinnacle_priced = int(summary["pinnacle_exact_game_rows"])
    result = {
        "source_path": str(HISTORY.relative_to(ROOT)), "source_sha256": sha(HISTORY),
        "history_start": frame.game_date.min(), "history_end": frame.game_date.max(),
        "ordinary_game_dates": frame.game_date.nunique(), "prediction_rows": predictions,
        "pinnacle_priced_rows": pinnacle_priced,
        "reference_price_availability": pinnacle_priced / predictions,
        "market_strong_rows": len(market),
        "market_strong_frequency_conditional_on_reference_price": len(market) / len(frame),
        "market_strong_frequency_per_prediction_row": len(market) / predictions,
        "agreement_rows": int(market.agreement.sum()),
        "without_agreement_rows": int((1-market.agreement).sum()),
        "agreement_share": float(market.agreement.mean()),
        "without_agreement_share": float(1-market.agreement.mean()),
        "per_ordinary_date": {column: {"mean": float(daily[column].mean()),
            "median": float(daily[column].median()), "p25": float(daily[column].quantile(.25)),
            "p75": float(daily[column].quantile(.75)), "minimum": int(daily[column].min()),
            "maximum": int(daily[column].max())}
            for column in ("market_strong", "agreement", "without_agreement")},
        "empirical_primary_win_difference": float(fit.params["agreement"]),
        "iid_hc1_standard_error": float(iid_se),
        "date_cluster_standard_error": float(clustered_se),
        "empirical_date_cluster_variance_design_effect": float((clustered_se/iid_se)**2),
        "empirical_date_cluster_se_ratio": float(clustered_se/iid_se),
        "cluster_caution": ("The empirical factor is below one on only 30 nonempty dates and is not used to deflate "
                            "the target; 1.0 remains the minimum planning design effect."),
        "ordinary_dates_to_732_at_observed_eligible_rate": math.ceil(732 / daily.market_strong.mean()),
        "historical_ten_book_cells": total_cells,
        "historical_valid_pregame_bookmaker_cells": valid_cells,
        "historical_missing_or_excluded_bookmaker_cells": total_cells-valid_cells,
        "historical_valid_pregame_bookmaker_cell_rate": valid_cells/total_cells,
    }
    return result, daily


def remaining_schedule(path: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    payload = json.loads(path.read_text())
    rows = [{"game_date": block["date"], "scheduled_regular_season_games": int(block["totalGames"])}
            for block in payload.get("dates", []) if REGULAR_START <= block["date"] <= REGULAR_END]
    frame = pd.DataFrame(rows).sort_values("game_date").reset_index(drop=True)
    if len(frame) != 18 or frame.scheduled_regular_season_games.sum() != 235:
        raise RuntimeError("Official remaining regular-season schedule no longer matches the pre-outcome inventory")
    return ({"source_url": SCHEDULE_URL, "source_sha256": sha(path), "dates": len(frame),
             "scheduled_games": int(frame.scheduled_regular_season_games.sum()),
             "first_date": frame.game_date.min(), "last_date": frame.game_date.max()}, frame)


def cost_evidence() -> tuple[dict[str, Any], pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    historical = pd.read_csv(TRANSFER / "append_only_request_ledger.csv", dtype=str).fillna("")
    success = historical[(historical.endpoint_class.eq("historical_sport_odds"))
                         & historical.status.eq("SUCCESS")]
    for row in success.itertuples(index=False):
        rows.append({"evidence_class": "IDENTICAL_TEN_BOOK_HISTORICAL_H2H", "source_path": row.raw_response_path,
                     "bookmaker_count": len(row.bookmakers.split(",")), "market_count": 1,
                     "x_requests_last": int(row.x_requests_last)})
    current_files = sorted((CURRENT_MANIFEST_ROOT / "2026-09-09").glob(
        "odds_mlb_pinnacle_main_markets__*.manifest.json"))
    for path in current_files:
        payload = json.loads(path.read_text())
        headers = payload.get("request_cost_headers", {})
        rows.append({"evidence_class": "CURRENT_PINNACLE_THREE_MARKETS", "source_path": str(path.relative_to(ROOT)),
                     "bookmaker_count": 1, "market_count": 3,
                     "x_requests_last": int(headers["x-requests-last"])})
    diagnostic = pd.read_csv(DIAGNOSTIC_REQUESTS).fillna("")
    wanted = diagnostic[diagnostic.request_id.isin([
        "current_sport_h2h_regions_us", "current_sport_h2h_regions_eu",
        "current_sport_h2h_regions_us_eu", "historical_sport_h2h_july18_target_bookmakers_betonlineag"])]
    for row in wanted.itertuples(index=False):
        rows.append({"evidence_class": row.request_id.upper(),
                     "source_path": str(DIAGNOSTIC_REQUESTS.relative_to(ROOT)),
                     "bookmaker_count": None, "market_count": 1,
                     "x_requests_last": int(row.quota_requests_last)})
    evidence = pd.DataFrame(rows)
    historical_costs = evidence[evidence.evidence_class.eq("IDENTICAL_TEN_BOOK_HISTORICAL_H2H")].x_requests_last
    if len(historical_costs) != 29 or not historical_costs.eq(10).all():
        raise RuntimeError("Historical ten-book cost is not certified by 29 x-requests-last observations")
    current_costs = evidence[evidence.evidence_class.eq("CURRENT_PINNACLE_THREE_MARKETS")].x_requests_last
    if current_costs.empty or not current_costs.eq(3).all():
        raise RuntimeError("Current Pinnacle three-market cost evidence changed")
    summary = {
        "historical_ten_book_h2h_x_requests_last_observations": len(historical_costs),
        "historical_ten_book_h2h_observed_cost_each": 10,
        "current_pinnacle_three_market_observations": len(current_costs),
        "current_pinnacle_three_market_observed_cost_each": 3,
        "current_us_region_h2h_observed_cost": int(wanted.loc[
            wanted.request_id.eq("current_sport_h2h_regions_us"), "quota_requests_last"].iloc[0]),
        "current_eu_region_h2h_observed_cost": int(wanted.loc[
            wanted.request_id.eq("current_sport_h2h_regions_eu"), "quota_requests_last"].iloc[0]),
        "current_us_plus_eu_h2h_observed_cost": int(wanted.loc[
            wanted.request_id.eq("current_sport_h2h_regions_us_eu"), "quota_requests_last"].iloc[0]),
        "historical_single_book_h2h_observed_cost": int(wanted.loc[
            wanted.request_id.eq("historical_sport_h2h_july18_target_bookmakers_betonlineag"),
            "quota_requests_last"].iloc[0]),
        "historical_replacement_conclusion": ("VERIFIED_ZERO_MARGINAL_COST_WITHIN_ONE_HISTORICAL_H2H_CALL: "
            "one explicit book and the frozen ten-book list both recorded x-requests-last=10"),
        "current_capture_expansion_conclusion": ("NOT_VERIFIED_AT_ZERO_MARGINAL_COST: the exact ten-book explicit "
            "current request has no x-requests-last observation; region requests cost 1/1/2, and existing current "
            "captures are not certified at the immutable prediction timestamp"),
        "scheduler_change_authorized": False,
    }
    return summary, evidence


def amendment_payload(frozen_at: str, original_sha: str, schedule_sha: str) -> dict[str, Any]:
    return {
        "study_id": V2_STUDY, "amendment_version": 2, "frozen_at_utc": frozen_at,
        "pre_outcome": True, "prospective_rows_at_amendment": 0,
        "original_v1_freeze_path": str(V1_FREEZE.relative_to(ROOT)),
        "original_v1_freeze_sha256": original_sha, "original_v1_freeze_modified": False,
        "collection_contract": "UNCHANGED_FROM_V1", "long_horizon_confirmatory_target": {
            "eligible_resolved_games": 732, "status": "RETAINED_UNCLUSTERED_LOWER_BOUND",
            "not_a_2026_terminal_gate": True},
        "bounded_2026_regular_season_layer": {
            "game_dates": [REGULAR_START, REGULAR_END], "season_close_date": REGULAR_END,
            "official_schedule_sha256": schedule_sha, "minimum_support_gate": BOUNDED_GATE,
            "minimum_support_gate_rationale": (
                "A descriptive sufficiency screen, not a powered decision threshold: 40 resolved games is "
                "approximately 63% of the 63.4-game schedule projection and 15 per group preserves two-sided "
                "comparison support under moderate allocation drift; 10 dates permits date-clustered uncertainty, "
                "and 20 blocked out-of-time games prevents score comparisons from resting on a negligible holdout."),
            "categories": sorted(BOUNDED_CATEGORIES),
            "contradicts_rule": ("support gate met; agreement-minus-comparison win-rate clustered upper bound < 0; "
                "agreement coefficient clustered upper bound < 0; and both blocked score-difference lower bounds > 0"),
            "directionally_favorable_rule": ("support gate met; positive win-rate difference and agreement coefficient, "
                "negative point differences for both blocked scores, and positive Pinnacle paid-break-even-excess difference; "
                "confirmatory support is explicitly unavailable"),
            "no_observable_separation_rule": ("support gate met but neither the contradiction rule nor the coherent "
                "directional-favorable rule is met; this is not an equivalence claim"),
            "insufficient_rule": "regular-season horizon open or any minimum-support gate unmet",
        },
        "acquisition": {"dates": 18, "calls": 18, "market": "h2h", "bookmaker_count": 10,
                        "verified_cost_per_call": 10, "exact_credit_ceiling": 180,
                        "contingency_calls": 0, "api_called_by_feasibility_review": False},
        "constraints": {"promotion_authorized": False, "wagering": False, "production": False,
                        "scheduler": False, "model_change": False, "threshold_change": False,
                        "prediction_change": False, "statistical_significance_manufactured": False},
    }


def initialize_amendment(output: Path, schedule_sha: str) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    copied = output / "original_v1_freeze.json"
    if copied.exists() and copied.read_bytes() != V1_FREEZE.read_bytes():
        raise RuntimeError("Preserved original v1 freeze changed")
    if not copied.exists(): copied.write_bytes(V1_FREEZE.read_bytes())
    freeze_path = output / "pre_outcome_freeze_v2.json"
    if freeze_path.exists():
        freeze = json.loads(freeze_path.read_text())
        if freeze != amendment_payload(freeze["frozen_at_utc"], sha(V1_FREEZE), schedule_sha):
            raise RuntimeError("V2 feasibility amendment changed")
    else:
        with sqlite3.connect(v1.LEDGER) as conn:
            if conn.execute("SELECT COUNT(*) FROM risk_set").fetchone()[0] != 0:
                raise RuntimeError("Cannot create a pre-outcome amendment after prospective rows exist")
        freeze = amendment_payload(now(), sha(V1_FREEZE), schedule_sha)
        write_json(freeze_path, freeze)
    with sqlite3.connect(v1.LEDGER) as conn:
        row = conn.execute("SELECT freeze_sha256 FROM study_metadata WHERE study_id=?", (V2_STUDY,)).fetchone()
        if row and row[0] != sha(freeze_path): raise RuntimeError("V2 ledger freeze binding changed")
        conn.execute("INSERT OR IGNORE INTO study_metadata VALUES (?,?,?,?,?)",
                     (V2_STUDY, sha(freeze_path), REGULAR_START, REGULAR_END, freeze["frozen_at_utc"]))
        conn.commit()
    return freeze


def bounded_report(output: Path, as_of: str) -> dict[str, Any]:
    freeze = json.loads((output / "pre_outcome_freeze_v2.json").read_text())
    if freeze != amendment_payload(freeze["frozen_at_utc"], sha(V1_FREEZE),
                                   freeze["bounded_2026_regular_season_layer"]["official_schedule_sha256"]):
        raise RuntimeError("V2 feasibility amendment verification failed")
    v1.verify_freeze(v1.OUT, v1.LEDGER)
    with sqlite3.connect(v1.LEDGER) as conn:
        risk = pd.read_sql_query("SELECT * FROM risk_set WHERE game_date BETWEEN ? AND ?", conn,
                                 params=(REGULAR_START, REGULAR_END))
        outcomes = pd.read_sql_query("SELECT * FROM outcomes", conn)
        all_prices = pd.read_sql_query("SELECT * FROM bookmaker_prices", conn)
    prices = all_prices[all_prices.bookmaker_key.eq("pinnacle")]
    resolved = risk[risk.risk_set_eligible.eq(1)].merge(outcomes, on="game_key", how="inner") if len(risk) else pd.DataFrame()
    contrast = {"difference": math.nan, "ci_2_5": math.nan, "ci_97_5": math.nan}
    coefficient = {"coefficient": math.nan, "ci_2_5": math.nan, "ci_97_5": math.nan}
    score_rows = pd.DataFrame()
    pbe = {"difference": math.nan, "ci_2_5": math.nan, "ci_97_5": math.nan}
    group_metrics: list[dict[str, Any]] = []
    bookmaker_metrics: list[dict[str, Any]] = []
    oot = 0
    if len(resolved):
        contrast = v1.clustered_group_difference(resolved, "selected_side_win")
        coefficient = v1.agreement_coefficient_interval(resolved)
        _, score_rows, blocked = v1.blocked_analysis(resolved); oot = int(blocked["out_of_time_games"])
        priced = resolved.merge(prices[prices.price_state.eq("TWO_SIDED")], on="game_key", how="inner")
        if len(priced):
            priced["paid_break_even_excess"] = priced.selected_side_win - priced.selected_paid_break_even
            pbe = v1.clustered_group_difference(priced, "paid_break_even_excess")
        for label, group in (("AGREEMENT", resolved[resolved.agreement_indicator.eq(1)]),
                             ("WITHOUT_AGREEMENT", resolved[resolved.agreement_indicator.eq(0)])):
            y = group.selected_side_win.to_numpy(); probability = group.selected_market_probability.to_numpy()
            win_ci = v1.cluster_interval(group.game_date, y) if len(group) else (math.nan, math.nan)
            brier_values = (probability-y)**2 if len(group) else np.array([])
            clipped = np.clip(probability, 1e-9, 1-1e-9) if len(group) else np.array([])
            log_values = (-y*np.log(clipped)-(1-y)*np.log(1-clipped)) if len(group) else np.array([])
            brier_ci = v1.cluster_interval(group.game_date, brier_values) if len(group) else (math.nan, math.nan)
            log_ci = v1.cluster_interval(group.game_date, log_values) if len(group) else (math.nan, math.nan)
            group_metrics.append({"group": label, "unique_games": len(group),
                "wins": int(group.selected_side_win.sum()) if len(group) else 0,
                "win_rate": float(group.selected_side_win.mean()) if len(group) else math.nan,
                "win_rate_ci_2_5": win_ci[0], "win_rate_ci_97_5": win_ci[1],
                "market_brier": float(brier_values.mean()) if len(group) else math.nan,
                "market_brier_ci_2_5": brier_ci[0], "market_brier_ci_97_5": brier_ci[1],
                "market_log_loss": float(log_values.mean()) if len(group) else math.nan,
                "market_log_loss_ci_2_5": log_ci[0], "market_log_loss_ci_97_5": log_ci[1]})
        all_priced = resolved.merge(all_prices[all_prices.price_state.eq("TWO_SIDED")], on="game_key", how="inner")
        if len(all_priced):
            all_priced["flat_risk_return"] = np.where(all_priced.selected_side_win.eq(1),
                                                       all_priced.selected_decimal_price-1, -1.0)
            all_priced["paid_break_even_excess"] = all_priced.selected_side_win-all_priced.selected_paid_break_even
            for book in v1.BOOKS:
                book_frame = all_priced[all_priced.bookmaker_key.eq(book)]
                for label, group in (("AGREEMENT", book_frame[book_frame.agreement_indicator.eq(1)]),
                                     ("WITHOUT_AGREEMENT", book_frame[book_frame.agreement_indicator.eq(0)])):
                    roi_ci = v1.cluster_interval(group.game_date, group.flat_risk_return) if len(group) else (math.nan, math.nan)
                    pbe_ci = v1.cluster_interval(group.game_date, group.paid_break_even_excess) if len(group) else (math.nan, math.nan)
                    bookmaker_metrics.append({"bookmaker_key": book, "group": label,
                        "unique_games": group.game_key.nunique(),
                        "flat_risk_roi": group.flat_risk_return.mean() if len(group) else math.nan,
                        "flat_risk_roi_ci_2_5": roi_ci[0], "flat_risk_roi_ci_97_5": roi_ci[1],
                        "paid_break_even_excess": group.paid_break_even_excess.mean() if len(group) else math.nan,
                        "paid_break_even_excess_ci_2_5": pbe_ci[0],
                        "paid_break_even_excess_ci_97_5": pbe_ci[1],
                        "effective_outcomes": group.game_key.nunique()})
    agreement = int(resolved.agreement_indicator.eq(1).sum()) if len(resolved) else 0
    comparison = int(resolved.agreement_indicator.eq(0).sum()) if len(resolved) else 0
    gates = {"regular_season_closed": as_of > REGULAR_END,
             "resolved_comparison_games": len(resolved) >= BOUNDED_GATE["resolved_comparison_games"],
             "agreement_games": agreement >= BOUNDED_GATE["agreement_games"],
             "without_agreement_games": comparison >= BOUNDED_GATE["without_agreement_games"],
             "resolved_dates": (resolved.game_date.nunique() if len(resolved) else 0) >= BOUNDED_GATE["resolved_dates"],
             "blocked_out_of_time_games": oot >= BOUNDED_GATE["blocked_out_of_time_games"]}
    support = all(gates.values())
    score_point = dict(zip(score_rows.score, score_rows.difference_combo_minus_market)) if len(score_rows) else {}
    score_low = dict(zip(score_rows.score, score_rows.ci_2_5)) if len(score_rows) else {}
    contradicts = bool(support and contrast["ci_97_5"] < 0 and coefficient["ci_97_5"] < 0
                       and score_low.get("BRIER", -math.inf) > 0 and score_low.get("LOG_LOSS", -math.inf) > 0)
    favorable = bool(support and contrast["difference"] > 0 and coefficient["coefficient"] > 0
                     and score_point.get("BRIER", math.inf) < 0 and score_point.get("LOG_LOSS", math.inf) < 0
                     and pbe["difference"] > 0)
    if not support: category = "2026_INSUFFICIENT_COMPARISON_SUPPORT"
    elif contradicts: category = "2026_EVIDENCE_CONTRADICTS_INCREMENTAL_AGREEMENT_VALUE"
    elif favorable: category = "2026_DIRECTIONALLY_FAVORABLE_BUT_UNRESOLVED"
    else: category = "2026_NO_OBSERVABLE_SEPARATION"
    if category not in BOUNDED_CATEGORIES: raise RuntimeError("Unknown bounded category")
    result = {"study_id": V2_STUDY, "as_of_date": as_of, "category": category,
              "bounded_assessment_made": support, "support_gates": gates,
              "resolved_comparison_games": len(resolved), "agreement_games": agreement,
              "without_agreement_games": comparison, "resolved_dates": resolved.game_date.nunique() if len(resolved) else 0,
              "win_rate_difference_agreement_minus_without": contrast,
              "agreement_coefficient_clustered": coefficient,
              "blocked_score_differences_combo_minus_market": score_rows.to_dict("records") if len(score_rows) else [],
              "group_effect_size_metrics": group_metrics, "bookmaker_effect_size_metrics": bookmaker_metrics,
              "pinnacle_paid_break_even_excess_difference": pbe,
              "long_horizon_732_target_reached": len(resolved) >= 732,
              "confirmatory_incremental_value_decision": "NOT_AUTHORIZED_BY_BOUNDED_LAYER",
              "promotion_authorized": False, "wagering_authorized": False,
              "bookmaker_rows_in_effective_sample_size": False}
    return clean(result)


def render_report(power: dict[str, Any], history: dict[str, Any], schedule: dict[str, Any],
                  costs: dict[str, Any], expected: dict[str, Any], bounded: dict[str, Any]) -> str:
    return f"""# MLB agreement-separation pre-outcome feasibility review v2

## Conclusion

The 732-game floor is **statistically derived and conservatively integer-rounded**, not arbitrary: exact planning sizes are {power['agreement_exact']:.3f} agreement and {power['comparison_exact']:.3f} comparison games, separately rounded to 438 and 294. Its baseline and 76:51 allocation are inherited from the earlier comparison, while the +10 percentage-point MDE is a judgmental predeclared design choice. It is not cluster- or missingness-inflated, so it is properly retained only as a long-horizon eligible-resolved confirmatory lower bound.

## Feasibility

History contains {history['market_strong_rows']} market-strong games across {history['ordinary_game_dates']} ordinary dates: {history['agreement_rows']} agreement and {history['without_agreement_rows']} without agreement. The assumed market-strong frequency is {history['market_strong_frequency_conditional_on_reference_price']:.3%} among reference-priced games and {history['market_strong_frequency_per_prediction_row']:.3%} per immutable prediction after observed reference-price availability. Per ordinary date, means are {history['per_ordinary_date']['market_strong']['mean']:.3f}, {history['per_ordinary_date']['agreement']['mean']:.3f}, and {history['per_ordinary_date']['without_agreement']['mean']:.3f}. At that eligible rate, 732 requires approximately {history['ordinary_dates_to_732_at_observed_eligible_rate']} ordinary game dates.

The official remaining regular-season inventory is {schedule['scheduled_games']} games on {schedule['dates']} dates ({schedule['first_date']} through {schedule['last_date']}). Applying the historical reference-price and market-strength rates yields {expected['comparison_eligible_market_strong_games']:.1f} resolved comparison-eligible games: approximately {expected['agreement_games']:.1f} agreement and {expected['without_agreement_games']:.1f} without agreement. The unadjusted market-strong expectation before reference-price exclusions is {expected['market_strong_before_reference_exclusions']:.1f}.

Historical reference availability was {history['reference_price_availability']:.1%}; this implies {expected['expected_reference_unavailable_all_games']:.1f} unavailable reference rows and about {expected['expected_market_strong_lost_to_reference_unavailability']:.1f} otherwise market-strong rows lost before comparison eligibility. The exact historical ten-book transfer had {history['historical_valid_pregame_bookmaker_cells']}/{history['historical_ten_book_cells']} valid cells, implying about {expected['expected_missing_bookmaker_cells_at_eligible_volume']:.1f} missing or excluded book-game cells at the projected eligible volume. These affect book-specific economics, not the one-outcome-per-game effective sample size.

The empirical date-cluster variance ratio is {history['empirical_date_cluster_variance_design_effect']:.3f} on only 30 nonempty dates. It is too unstable and favorable to justify deflation; the planning floor retains a minimum design effect of 1.0 and all reported inference remains date-clustered.

## 2026 bounded layer

The amendment preserves 732 and adds a regular-season descriptive decision layer with four non-promotional states. The current state is **`{bounded['category']}`** because no prospective rows exist. Once September 27 is closed, the layer requires at least 40 resolved comparison games, 15 per group, 10 dates, and 20 blocked out-of-time scores. The 40-game gate is about 63% of the 63.4 projected comparisons; the remaining gates protect two-sided group support, clustered uncertainty, and a non-negligible blocked holdout. These are descriptive sufficiency screens, not powered significance thresholds. The layer can identify interval-supported contradiction, coherent favorable direction without confirmation, absence of coherent observable separation, or inadequate comparison support. “No observable separation” is explicitly not statistical equivalence, and no bounded state authorizes model promotion.

## Acquisition

The exact ceiling is **180 credits**: {schedule['dates']} additional historical calls × the observed `x-requests-last` cost of {costs['historical_ten_book_h2h_observed_cost_each']}. That cost is verified on {costs['historical_ten_book_h2h_x_requests_last_observations']} prior identical ten-book h2h calls. One-book and ten-book historical h2h requests both recorded 10, so the ten-book list has zero marginal historical cost relative to Pinnacle-only within the same call.

The existing live Pinnacle three-market call recorded `x-requests-last=3`, but an exact ten-book explicit live expansion has not been observed. The US+EU regional h2h request recorded 2. Existing live captures are also not certified as the nearest snapshot at or before the immutable prediction timestamp. Therefore they do not replace the 18 historical calls, no zero-cost live-expansion claim is made, and no scheduler change is authorized.

No Odds API request, model, threshold, prediction, wagering, production, or scheduler change was made by this review.
"""


def render_bounded_report(result: dict[str, Any]) -> str:
    return f"""# 2026 bounded agreement-separation report v2

**{result['category']}**

As of {result['as_of_date']}, the regular-season layer has {result['resolved_comparison_games']} resolved comparison-eligible games: {result['agreement_games']} agreement and {result['without_agreement_games']} without agreement across {result['resolved_dates']} dates.

The bounded layer is descriptive and cannot confirm incremental model value or authorize promotion. All effect sizes, score differences, bookmaker returns, paid break-even excesses, and date-clustered intervals are preserved in `bounded_2026_report.json`. Multiple bookmaker cells do not increase the effective outcome count.
"""


def write_manifest(output: Path) -> None:
    files = sorted(path for path in output.iterdir() if path.is_file() and path.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(path)}  {path.name}\n" for path in files))


def run(schedule_json: Path, output: Path, as_of: str) -> dict[str, Any]:
    schedule_summary, schedule_frame = remaining_schedule(schedule_json)
    freeze = initialize_amendment(output, schedule_summary["source_sha256"])
    power = power_derivation(); history, daily = historical_feasibility(); costs, cost_rows = cost_evidence()
    games = schedule_summary["scheduled_games"]
    expected = {
        "scheduled_games": games,
        "market_strong_before_reference_exclusions": games * history["market_strong_frequency_conditional_on_reference_price"],
        "comparison_eligible_market_strong_games": games * history["market_strong_frequency_per_prediction_row"],
        "agreement_games": games * history["market_strong_frequency_per_prediction_row"] * history["agreement_share"],
        "without_agreement_games": games * history["market_strong_frequency_per_prediction_row"] * history["without_agreement_share"],
        "expected_reference_unavailable_all_games": games * (1-history["reference_price_availability"]),
        "expected_market_strong_lost_to_reference_unavailability": games * (
            history["market_strong_frequency_conditional_on_reference_price"]
            - history["market_strong_frequency_per_prediction_row"]),
        "historical_ten_book_valid_cell_rate": history["historical_valid_pregame_bookmaker_cell_rate"],
        "expected_missing_bookmaker_cells_at_eligible_volume": games * history["market_strong_frequency_per_prediction_row"] * 10 * (
            1-history["historical_valid_pregame_bookmaker_cell_rate"]),
    }
    bounded = bounded_report(output, as_of)
    review_summary = {"audit": V2_STUDY, "floor_determination": {
        "statistically_derived": True, "conservatively_integer_rounded": True,
        "whole_floor_inherited_from_another_study": False, "arbitrary": False,
        "historical_inputs_inherited": ["31/51 baseline", "76:51 allocation"],
        "judgmental_input": "10 percentage-point minimum detectable increment",
        "cluster_adjusted": False, "missingness_adjusted": False,
        "meaning_of_732": "resolved comparison-eligible market-strong games, not all market-strong observations"},
        "long_horizon_target": 732, "expected_regular_season_eligible_games": expected["comparison_eligible_market_strong_games"],
        "expected_dates_to_target": history["ordinary_dates_to_732_at_observed_eligible_rate"],
        "additional_charged_calls": schedule_summary["dates"], "exact_credit_ceiling": 180,
        "bounded_current_category": bounded["category"], "odds_api_requests": 0,
        "amendment_recommended_and_frozen": True, "original_freeze_unchanged": True}
    daily.to_csv(output / "historical_daily_risk_set_rates.csv", index=False, lineterminator="\n")
    schedule_frame.to_csv(output / "remaining_regular_season_schedule.csv", index=False, lineterminator="\n")
    (output / "official_remaining_regular_season_schedule_raw.json").write_bytes(schedule_json.read_bytes())
    cost_rows.to_csv(output / "x_requests_last_cost_evidence.csv", index=False, lineterminator="\n")
    write_json(output / "power_derivation.json", power)
    write_json(output / "historical_feasibility.json", history)
    write_json(output / "schedule_inventory.json", schedule_summary)
    write_json(output / "expected_2026_regular_season_rows.json", expected)
    write_json(output / "acquisition_cost_review.json", costs)
    write_json(output / "bounded_2026_report.json", bounded)
    write_json(output / "review_summary.json", review_summary)
    (output / "bounded_2026_report.md").write_text(render_bounded_report(bounded))
    (output / "feasibility_review.md").write_text(render_report(power, history, schedule_summary, costs, expected, bounded))
    write_manifest(output)
    return {"freeze": freeze, "power": power, "history": history, "schedule": schedule_summary,
            "expected": expected, "costs": costs, "bounded": bounded, "odds_api_requests": 0}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule-json", type=Path)
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args()
    if args.report_only:
        result = bounded_report(args.output, args.as_of)
        write_json(args.output / "bounded_2026_report.json", result)
        (args.output / "bounded_2026_report.md").write_text(render_bounded_report(result))
        write_manifest(args.output)
        print(json.dumps(clean(result), indent=2, sort_keys=True, allow_nan=False))
        return
    if args.schedule_json is None: parser.error("--schedule-json is required unless --report-only is used")
    result = run(args.schedule_json, args.output, args.as_of)
    print(json.dumps(clean(result), indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
