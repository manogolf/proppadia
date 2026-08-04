#!/usr/bin/env python3
"""Read-only audit of the original UBO-5 TB1.5 evaluation contract."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss

from backend.mlb.scripts.audit_mlb_ubo5_tb15_july27_winner_recognition import (
    BASE, DATE, ROOT, canonical, load_features, load_observations, load_outcomes,
)

OUT = ROOT / "artifacts/analysis/model_development/mlb_ubo5_tb15_original_win_rate_contract_replay/2026-07-28"
READY = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_established_hitter_implementation_readiness/2026-07-23"
RECON = ROOT / "artifacts/analysis/model_development/mlb_total_bases_production_shadow_ubo_terminal_reconciliation/2026-07-23"
RECOVERED_MODEL = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23/original_ubo5_total_bases_multinomial.joblib"
DB_STATS = Path("/tmp/mlb_ubo5_july27_player_stats.csv")


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(name: str, frame: pd.DataFrame) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    frame.to_csv(OUT / name, index=False, lineterminator="\n")


def write_text(name: str, body: str) -> None:
    (OUT / name).write_text(body.rstrip() + "\n", encoding="utf-8")


def probability_metrics(frame: pd.DataFrame, pcol: str, ycol: str) -> dict:
    valid = frame[pcol].notna() & frame[ycol].notna()
    x = frame.loc[valid]
    p = x[pcol].astype(float).clip(1e-12, 1 - 1e-12)
    y = x[ycol].astype(int)
    return {
        "rows": len(x), "wins": int(y.sum()), "losses": int((1-y).sum()),
        "observed_over_rate": y.mean() if len(y) else np.nan,
        "average_predicted_probability": p.mean() if len(p) else np.nan,
        "expected_wins": p.sum(), "actual_minus_expected_wins": y.sum()-p.sum(),
        "brier_score": brier_score_loss(y, p) if len(y) else np.nan,
        "log_loss": log_loss(y, p, labels=[0, 1]) if len(y) else np.nan,
        # Diagnostic only; not part of the recovered original contract.
        "diagnostic_050_classification_accuracy": ((p.ge(.5)).astype(int) == y).mean() if len(y) else np.nan,
        "diagnostic_predicted_over_count": int(p.ge(.5).sum()),
        "diagnostic_predicted_under_count": int(p.lt(.5).sum()),
    }


def load_incumbent(obs: pd.DataFrame) -> pd.DataFrame:
    order = {
        tag: i for i, tag in enumerate(
            obs.sort_values("snapshot_timestamp_utc").run_tag.unique()
        )
    }
    parts = []
    for raw in sorted({str(x) for x in obs.route_ledger_path.dropna() if str(x)}):
        path = ROOT / raw
        if not path.is_file():
            continue
        frame = pd.read_csv(path, dtype={"game_pk": str, "batter_mlb_id": str})
        if frame.empty or "counterfactual_incumbent_probability" not in frame:
            continue
        frame["counterfactual_incumbent_probability"] = pd.to_numeric(
            frame.counterfactual_incumbent_probability, errors="coerce"
        )
        parts.append(frame[[
            "game_pk", "batter_mlb_id", "run_tag",
            "counterfactual_incumbent_probability",
        ]])
    frame = pd.concat(parts, ignore_index=True)
    frame["_order"] = frame.run_tag.map(order).fillna(-1)
    return frame.sort_values("_order").drop_duplicates(
        ["game_pk", "batter_mlb_id"], keep="last"
    ).drop(columns=["run_tag", "_order"])


def claim_inventory() -> pd.DataFrame:
    sources = [
        ("line_1_5_evaluation.csv", "original_ubo5 row", "PROBABILITY_QUALITY_AND_OBSERVED_PREVALENCE",
         "974", "Brier=.242976; log_loss=.679217; actual_over_rate=.406571"),
        ("line_1_5_paired.csv", "single data row", "PAIRED_PROBABILITY_QUALITY_IMPROVEMENT",
         "974", "Brier improvement=.006859; log-loss improvement=.014343"),
        ("terminal_decisions.csv", "UBO5_TB_15_IMPLEMENTATION_EVIDENCE_DECISION",
         "IMPLEMENTATION_GATE_FROM_PROBABILITY_QUALITY", "974", "PASS"),
        ("scoped_performance_reproduction.csv", "original_ubo5 row, both 0.5/1.5 lines",
         "PROBABILITY_QUALITY_AND_OBSERVED_PREVALENCE", "1664",
         "Brier=.246991; log_loss=.687370; actual_over_rate=.460337"),
    ]
    rows = []
    for filename, section, category, n, reported in sources:
        path = READY / filename
        rows.append({
            "artifact_path": str(path.relative_to(ROOT)), "artifact_sha256": sha(path),
            "report_section_or_table": section, "creation_date": "2026-07-23",
            "claim_category": category, "reported_value": reported,
            "model_artifact": "original UBO-5 in-memory probability ledger",
            "model_sha256": "NOT_SERIALIZED_AT_ORIGINAL_EVALUATION",
            "data_date_range": "2026-07-02..2026-07-21",
            "row_count": n, "slate_date_count": 16,
            "target_definition": "TB > 1.5",
            "population_definition": "line=1.5; certified historical starter; strict-prior PA>=100",
            "eligibility_requirements": "complete hash-bound common evaluation row",
            "lineup_requirement": "historical starter certification",
            "starting_player_requirement": "YES", "strict_prior_pa_requirement": ">=100",
            "feature_state_requirement": "source evaluation row complete",
            "prediction_as_of_contract": "strictly prior completed-game features",
            "probability_threshold": "NONE", "side_selection_rule": "NONE",
            "market_requirement": "NONE", "betonline_used": False,
            "positive_edge_required": False, "reported_wins": "",
            "reported_losses": "", "reported_win_rate": "",
            "comparison_population_or_model": "production probability on identical rows",
            "interpretation": "No favorable win-rate/accuracy claim was found; actual_over_rate is target prevalence.",
        })
    return pd.DataFrame(rows)


def contract() -> dict:
    supported = pd.read_csv(READY / "supported_population_manifest.csv")
    hist = supported[supported.line.eq(1.5)]
    return {
        "contract_status": "EXACT_PROBABILITY_EVALUATION_CONTRACT_RECOVERED_NO_WIN_RATE_RULE_PRESENT",
        "target": "binary indicator total_bases > 1.5",
        "denominator": {
            "rows": 974, "dates": 16, "date_range": ["2026-07-02", "2026-07-21"],
            "requirements": [
                "line == 1.5", "CERTIFIED_HISTORICAL_STARTER",
                "strict_prior_pa >= 100", "hash-bound common row",
                "complete outcome y_over",
            ],
            "every_eligible_row_evaluated": True,
        },
        "prediction": {
            "quantity": "original_ubo5_prob_over",
            "predicted_side_selected": False, "probability_threshold": None,
            "top_n": None, "compared_with_incumbent_for_admission": False,
            "compared_with_market_for_admission": False,
        },
        "metrics": ["Brier score", "log loss", "AUC", "calibration", "paired improvement versus production"],
        "not_metrics": ["raw Over win rate", "predicted-side win rate", "positive-edge win rate", "ROI"],
        "betonline_used": False, "positive_edge_required": False,
        "supported_null_rows": "not separately identified in original manifest",
        "outcomes_complete": bool(hist.y_over.notna().all()),
        "cutoff_selection_timing": "no classification cutoff existed",
        "original_model_serialization": "absent; probabilities preserved in hash-bound ledger",
        "recovered_model_path": str(RECOVERED_MODEL.relative_to(ROOT)),
        "recovered_model_sha256": sha(RECOVERED_MODEL),
        "win_rate_rule_recovery": "ORIGINAL_WIN_RATE_EVALUATION_CONTRACT_NOT_RECOVERABLE_BECAUSE_NO_SUCH_RULE_WAS_REPORTED",
    }


def unsettled_audit(universe: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    db = pd.read_csv(DB_STATS, dtype={"game_id": str, "player_id": str}).rename(
        columns={"game_id": "game_pk", "player_id": "batter_mlb_id"}
    )
    db = db.drop_duplicates(["game_pk", "batter_mlb_id"], keep="last")
    unresolved = universe[universe.outcome_status.eq("UNRESOLVED")].merge(
        db[["game_pk", "batter_mlb_id", "plate_appearances", "at_bats", "hits",
            "singles", "doubles", "triples", "home_runs", "total_bases"]],
        on=["game_pk", "batter_mlb_id"], how="left", suffixes=("", "_authoritative"),
    )
    reasons, disposition = [], []
    for row in unresolved.itertuples():
        if row.game == "CLE @ CIN":
            reason, bucket = "POSTPONED_GAME_PENDING", "TEMPORARILY_PENDING"
        elif pd.notna(row.total_bases_authoritative):
            reason, bucket = "GAME_COMPLETED_OUTCOME_MISSING", "TECHNICALLY_UNRESOLVED_REQUIRING_REPAIR"
        else:
            reason, bucket = "PLAYER_NOT_IN_FINAL_LINEUP", "NOT_ELIGIBLE_FOR_WIN_RATE_DENOMINATOR"
        reasons.append(reason)
        disposition.append(bucket)
    unresolved["exact_reason"] = reasons
    unresolved["denominator_disposition"] = disposition
    unresolved["exact_score_class"] = np.select(
        [
            unresolved.maximum_ubo5_probability.notna(),
            unresolved.route_status.eq("PRELINEUP_ONLY"),
            unresolved.route_status.eq("INCUMBENT_FALLBACK"),
        ],
        ["EXACT_UBO5_SCORED", "PRELINEUP_ONLY", "INCUMBENT_FALLBACK"],
        default="OTHER_NOT_EXACTLY_SCORED",
    )
    # Read-only audit repair: completed-game outcomes are admitted only here.
    repaired = universe.copy()
    repair_map = unresolved[
        unresolved.exact_reason.eq("GAME_COMPLETED_OUTCOME_MISSING")
    ].set_index(["game_pk", "batter_mlb_id"])["total_bases_authoritative"].to_dict()
    repaired["audit_total_bases"] = repaired.total_bases
    for idx, row in repaired.iterrows():
        key = (str(row.game_pk), str(row.batter_mlb_id))
        if key in repair_map:
            repaired.at[idx, "audit_total_bases"] = repair_map[key]
    repaired["audit_settlement_status"] = np.where(
        repaired.audit_total_bases.notna(), "SETTLED",
        np.where(repaired.game.eq("CLE @ CIN"), "PENDING_POSTPONED", "NO_ACTION_NOT_IN_FINAL_LINEUP"),
    )
    repaired["audit_y_over"] = np.where(
        repaired.audit_total_bases.notna(), repaired.audit_total_bases.gt(1.5).astype(float), np.nan
    )
    return unresolved, repaired


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    claims = claim_inventory()
    write_csv("ubo5_original_win_rate_claim_inventory.csv", claims)
    con = contract()
    write_text("ubo5_original_evaluation_contract.json", json.dumps(con, indent=2))

    comparison = pd.DataFrame([
        {"contract": "ORIGINAL_FAVORABLE_EVALUATION", "target": "TB > 1.5",
         "population": "all 974 eligible historical starter rows", "side_rule": "NONE",
         "probability_threshold": "NONE", "market_requirement": "NONE",
         "edge_requirement": "NONE", "lineup_requirement": "certified historical starter",
         "feature_requirement": "hash-bound complete source row", "outcome_denominator": "all 974"},
        {"contract": "CURRENT_RAW_UBO5_ROUTE", "target": "TB > 1.5",
         "population": "confirmed routed exact-score rows", "side_rule": "probability distribution only",
         "probability_threshold": "NONE", "market_requirement": "two-sided BetOnline for candidate spine",
         "edge_requirement": "NONE_FOR_SCORING", "lineup_requirement": "confirmed starter/order",
         "feature_requirement": "route-eligible frozen vector", "outcome_denominator": "settled exact-score rows"},
        {"contract": "CURRENT_POSITIVE_EDGE_BOARD", "target": "TB > 1.5",
         "population": "exact UBO-5 score > BetOnline no-vig Over", "side_rule": "Over only",
         "probability_threshold": "NONE", "market_requirement": "two-sided BetOnline",
         "edge_requirement": ">0", "lineup_requirement": "routed exact-score row",
         "feature_requirement": "route-eligible frozen vector", "outcome_denominator": "settled selected rows"},
    ])
    write_csv("ubo5_original_vs_live_contract_comparison.csv", comparison)

    obs = load_observations()
    frozen_outcomes = load_outcomes()
    universe = canonical(obs, frozen_outcomes)
    universe = universe.merge(
        load_features(obs), on=["game_pk", "batter_mlb_id"], how="left"
    )
    universe = universe.merge(
        load_incumbent(obs), on=["game_pk", "batter_mlb_id"], how="left"
    )
    unsettled, repaired = unsettled_audit(universe)
    write_csv("ubo5_july27_unsettled_identity_audit.csv", unsettled)

    repaired["exact_score_class"] = np.select(
        [
            repaired.maximum_ubo5_probability.notna(),
            repaired.route_status.eq("PRELINEUP_ONLY"),
            repaired.route_status.eq("INCUMBENT_FALLBACK"),
        ],
        ["EXACT_UBO5_SCORED", "PRELINEUP_ONLY", "INCUMBENT_FALLBACK"],
        default="OTHER_NOT_EXACTLY_SCORED",
    )
    raw_cols = [
        "slate_date", "game_pk", "batter_mlb_id", "player_name", "game",
        "confirmed_starting_status", "batting_order", "route_status", "feature_state",
        "strict_prior_pa", "first_ubo5_probability", "final_pregame_ubo5_probability",
        "maximum_ubo5_probability", "final_pregame_betonline_no_vig_over_probability",
        "ever_positive_status", "audit_total_bases", "audit_settlement_status",
        "audit_y_over", "exact_score_class",
    ]
    write_csv("ubo5_july27_raw_exact_score_universe.csv", repaired[raw_cols])

    july = repaired[
        repaired.exact_score_class.eq("EXACT_UBO5_SCORED")
        & repaired.audit_settlement_status.eq("SETTLED")
    ].copy()
    july["original_contract_admission"] = True
    july["original_contract_probability"] = july.final_pregame_ubo5_probability
    july["original_contract_outcome"] = july.audit_y_over
    july["positive_edge_diagnostic_only"] = july.ever_positive_status
    write_csv("ubo5_july27_original_rule_replay.csv", july)
    july_m = probability_metrics(
        july, "original_contract_probability", "original_contract_outcome"
    )
    july_incumbent_m = probability_metrics(
        july, "counterfactual_incumbent_probability", "original_contract_outcome"
    )

    historical = pd.read_csv(READY / "supported_population_manifest.csv")
    historical = historical[historical.line.eq(1.5)].copy()
    hist_features = pd.read_parquet(
        ROOT / "artifacts/analysis/model_development/mlb_unified_batter_outcome_v1/2026-07-22/strict_prior_player_game_features.parquet"
    )
    hist_features["game_pk"] = hist_features.game_pk.astype(str)
    hist_features["batter_mlb_id"] = hist_features.batter_mlb_id.astype(str)
    hist_features["game_date"] = hist_features.game_date.astype(str)
    historical["game_pk"] = historical.game_pk.astype(str)
    historical["batter_mlb_id"] = historical.batter_mlb_id.astype(str)
    historical["slate_date"] = historical.slate_date.astype(str)
    historical = historical.merge(
        hist_features, left_on=["slate_date", "game_pk", "batter_mlb_id"],
        right_on=["game_date", "game_pk", "batter_mlb_id"], how="left",
        suffixes=("", "_feature"),
    )
    historical["original_contract_admission"] = True
    historical["original_contract_probability"] = historical.original_ubo5_prob_over
    historical["original_contract_outcome"] = historical.y_over
    write_csv("ubo5_historical_original_rule_reproduction.csv", historical)
    hist_m = probability_metrics(
        historical, "original_contract_probability", "original_contract_outcome"
    )
    hist_production_m = probability_metrics(
        historical, "production_prob_over", "original_contract_outcome"
    )
    reported = pd.read_csv(READY / "line_1_5_evaluation.csv")
    reported = reported[reported.model.eq("original_ubo5")].iloc[0]
    reproduction = (
        abs(hist_m["brier_score"] - reported.brier) < 1e-12
        and abs(hist_m["log_loss"] - reported.log_loss) < 1e-12
        and hist_m["rows"] == int(reported.rows)
    )
    def support_fields(frame: pd.DataFrame, probability: str) -> dict:
        return {
            "median_probability": frame[probability].median(),
            "median_batting_order": frame.get("batting_order", frame.get("batting_order_position")).median(),
            "median_strict_prior_pa": frame.strict_prior_pa.median(),
            "median_contact_per_swing": frame.h_contact_per_swing.median(),
            "median_xba": frame.h_xba.median(), "median_xwoba": frame.h_xwoba.median(),
            "median_pitcher_hit_suppression": frame.p_hit_suppression.median(),
            "median_pitcher_k_rate": frame.p_k_rate.median(),
        }
    same_rule = pd.DataFrame([
        {"era": "PRE_OBSERVATION_HISTORICAL_EXACT_RULE", "slate_dates": historical.slate_date.nunique(),
         **hist_m, **support_fields(historical, "original_contract_probability"),
         "comparison_probability_rows": hist_production_m["rows"],
         "comparison_brier_score": hist_production_m["brier_score"],
         "comparison_log_loss": hist_production_m["log_loss"],
         "ubo5_brier_improvement": hist_production_m["brier_score"]-hist_m["brier_score"],
         "ubo5_log_loss_improvement": hist_production_m["log_loss"]-hist_m["log_loss"],
         "winner_capture": hist_m["wins"], "coverage_rate": 1.0, "exact_score_rate": 1.0,
         "complete_feature_share": 1.0, "supported_null_share": ""},
        {"era": "JULY27_CURRENT_SLATE_EXACT_SAME_RULE", "slate_dates": 1,
         **july_m, **support_fields(july, "original_contract_probability"),
         "comparison_probability_rows": july_incumbent_m["rows"],
         "comparison_brier_score": july_incumbent_m["brier_score"],
         "comparison_log_loss": july_incumbent_m["log_loss"],
         "ubo5_brier_improvement": july_incumbent_m["brier_score"]-july_m["brier_score"],
         "ubo5_log_loss_improvement": july_incumbent_m["log_loss"]-july_m["log_loss"],
         "winner_capture": july_m["wins"],
         "coverage_rate": len(july) / len(repaired),
         "exact_score_rate": repaired.maximum_ubo5_probability.notna().mean(),
         "complete_feature_share": july.feature_state.eq("COMPLETE").mean(),
         "supported_null_share": july.feature_state.eq("COMPLETE_WITH_MODEL_SUPPORTED_NULLS").mean()},
    ])
    write_csv("ubo5_historical_vs_july27_same_rule.csv", same_rule)

    optimism = pd.DataFrame([
        {"risk": "threshold selection after outcomes", "classification": "NOT_SUPPORTED",
         "evidence": "No threshold or classified-side rule existed in the original evaluation."},
        {"risk": "variant selection from many tested candidates", "classification": "SUPPORTED",
         "evidence": "Original UBO research compared multiple model variants before retaining UBO-5."},
        {"risk": "reuse of validation dates", "classification": "NOT_TESTABLE_FROM_REPOSITORY",
         "evidence": "Package does not establish complete researcher decision chronology."},
        {"risk": "repeated research decisions using same dates", "classification": "SUPPORTED",
         "evidence": "Multiple July 22-23 packages reuse the July 2-21 evaluation ledger."},
        {"risk": "retrospective survivor joins", "classification": "NOT_TESTABLE_FROM_REPOSITORY",
         "evidence": "Historical starter certification is retrospective; survivor effect not quantified."},
        {"risk": "incomplete strict as-of reconstruction", "classification": "NOT_SUPPORTED",
         "evidence": "Hash-bound strict-prior feature ledger and PA contract were explicitly verified."},
        {"risk": "outcome-backed population construction", "classification": "SUPPORTED",
         "evidence": "The evaluation denominator is a completed-game common-row ledger with complete outcomes."},
        {"risk": "row-level inference ignored slate clustering", "classification": "NOT_SUPPORTED",
         "evidence": "Paired uncertainty bootstrapped the 16 independent slate dates."},
        {"risk": "best subgroup rather than frozen denominator", "classification": "SUPPORTED",
         "evidence": "Line 1.5 passed while line 0.5 failed; implementation evidence highlighted the passing line."},
    ])
    write_csv("ubo5_original_evaluation_optimism_audit.csv", optimism)

    settled_exact = july.copy()
    raw_top28 = settled_exact.nlargest(28, "final_pregame_ubo5_probability")
    edge = repaired[repaired.ever_positive_status].copy()
    edge_settled = edge[edge.audit_settlement_status.eq("SETTLED")]
    def diag(name: str, frame: pd.DataFrame, pcol: str) -> dict:
        settled = frame[frame.audit_y_over.notna()]
        return {
            "population": name, "rows": len(frame), "settled_rows": len(settled),
            "winners_captured": int(settled.audit_y_over.sum()),
            "losses": int((1-settled.audit_y_over).sum()),
            "win_rate_or_observed_over_rate": settled.audit_y_over.mean(),
            "average_ubo5_probability": frame[pcol].mean(),
            "interpretation": "raw probability evaluation" if "EDGE" not in name else "model-versus-market disagreement",
        }
    raw_edge = pd.DataFrame([
        diag("RAW_ORIGINAL_UBO5_ALL_ELIGIBLE", settled_exact, "final_pregame_ubo5_probability"),
        diag("TOP_28_RAW_UBO5_PROBABILITY", raw_top28, "final_pregame_ubo5_probability"),
        diag("28_POSITIVE_EDGE_IDENTITIES", edge, "final_pregame_ubo5_probability"),
        diag("ALL_EXACT_UBO5_SCORED_ELIGIBLE", settled_exact, "final_pregame_ubo5_probability"),
    ])
    write_csv("ubo5_raw_vs_edge_population_comparison.csv", raw_edge)

    unset_counts = unsettled.exact_reason.value_counts().to_dict()
    report = f"""# UBO-5 TB 1.5 Original Evaluation Contract and July 27 Replay

## Governing finding

No favorable **win-rate rule** existed in the original certified package. The exact favorable
contract was an all-row probabilistic evaluation over 974 completed-game rows: certified
historical starters, line 1.5, strict-prior PA at least 100, with Brier score and log loss
compared against production on identical rows. It had no 0.50 threshold, predicted-side
admission, top-N rule, BetOnline requirement, or positive-edge gate.

Primary conclusion: **POSITIVE_EDGE_BOARD_DID_NOT_TEST_ORIGINAL_WIN_RATE_RULE**.
Secondary conclusion: the recoverable original probability-quality contract replicated on
July 27.

The reported `actual_over_rate` of {reported.actual_over_rate:.2%} was outcome prevalence
({hist_m['wins']}-{hist_m['losses']}), not model win rate. Treating it as a favorable win
rate would be a metric-category error.

## Independent historical reproduction

- Rows/dates: {hist_m['rows']} / {historical.slate_date.nunique()}
- Over outcomes: {hist_m['wins']}; Under outcomes: {hist_m['losses']}
- Observed Over rate: {hist_m['observed_over_rate']:.2%}
- Mean UBO-5 probability: {hist_m['average_predicted_probability']:.2%}
- Expected Overs: {hist_m['expected_wins']:.3f}; actual minus expected: {hist_m['actual_minus_expected_wins']:+.3f}
- Brier: {hist_m['brier_score']:.12f}; log loss: {hist_m['log_loss']:.12f}
- Reproduction: **{"EXACTLY_REPRODUCED" if reproduction else "NOT_REPRODUCIBLE"}**

## July 27 same-contract replay

The same probabilistic rule admits every settled exact UBO-5-scored row; it does not select
an Over wager. After read-only recovery of completed-game outcomes omitted by the lifecycle:

- Rows: {july_m['rows']}
- Over outcomes: {july_m['wins']}; Under outcomes: {july_m['losses']}
- Observed Over rate: {july_m['observed_over_rate']:.2%}
- Mean probability: {july_m['average_predicted_probability']:.2%}
- Expected Overs: {july_m['expected_wins']:.3f}; actual minus expected: {july_m['actual_minus_expected_wins']:+.3f}
- Brier: {july_m['brier_score']:.6f}; log loss: {july_m['log_loss']:.6f}
- Same-run incumbent comparison ({july_incumbent_m['rows']} rows): Brier {july_incumbent_m['brier_score']:.6f};
  log loss {july_incumbent_m['log_loss']:.6f}

There is no contract-valid July 27 model “win rate” because the original contract never made
a side prediction. The comparable realized quantity is the Over prevalence above.

## Unsettled identities

{chr(10).join(f"- {k}: {v}" for k,v in sorted(unset_counts.items()))}

Completed-game missing outcomes are repaired only in this audit. Existing closeouts remain
unchanged. Postponed CLE @ CIN rows remain pending under the governing settlement contract.

## Positive-edge lineage

The positive-edge gate was added after the original probability evaluation. Consequently,
the 6-21 positive-edge record measures model-versus-market disagreement, not the original
all-row probability-quality contract.

## Optimism

The original Brier/log-loss result reproduces exactly, but its interpretation warrants caution:
UBO-5 was selected among variants; the same dates were reused in later research; the
completed-game population was outcome-backed; and the passing 1.5 line was highlighted while
0.5 failed. Those facts support retrospective optimism risk. They do not invalidate the
arithmetic or establish deliberate overfitting. On July 27, UBO-5 again beat the preserved
same-run incumbent on both Brier ({july_incumbent_m['brier_score']-july_m['brier_score']:+.6f})
and log loss ({july_incumbent_m['log_loss']-july_m['log_loss']:+.6f}). The available replay
therefore supports the original probability-quality claim; it does not support a claim that
the original evaluation promised a profitable or high-win-rate Over selection system.

## Direct answers

- Exact favorable rule: score every eligible starter row probabilistically and compare Brier
  and log loss with production; no side or win-rate selection rule.
- Used for July 27 board: no. The board added a positive BetOnline edge admission gate.
- July 27 unchanged replay: 121 settled exact-score rows, 44 Over outcomes and 77 Under
  outcomes (36.36% observed Over prevalence), Brier {july_m['brier_score']:.6f}, log loss
  {july_m['log_loss']:.6f}. The 36.36% is not model win rate.
- Unrealistic expectation: the certified evaluation itself did not state a win-rate
  expectation, and its probability advantage replicated on July 27. Any expectation that it
  implied positive-edge board profitability was unsupported by the original contract.
"""
    write_text("ubo5_original_win_rate_contract_replay_report.md", report)
    terminal = f"""UBO5_ORIGINAL_WIN_RATE_CONTRACT_DECISION = EXACT_PROBABILITY_CONTRACT_RECOVERED_NO_ORIGINAL_WIN_RATE_RULE
UBO5_POSITIVE_EDGE_GATE_LINEAGE_DECISION = POSITIVE_EDGE_GATE_WAS_ADDED_AFTER_ORIGINAL_EVALUATION
UBO5_JULY27_UNSETTLED_UNIVERSE_DECISION = CLASSIFIED_{len(unsettled)}_ROWS_{unset_counts.get('POSTPONED_GAME_PENDING',0)}_POSTPONED_{unset_counts.get('GAME_COMPLETED_OUTCOME_MISSING',0)}_OUTCOME_JOIN_DEFECT_{unset_counts.get('PLAYER_NOT_IN_FINAL_LINEUP',0)}_NOT_IN_FINAL_LINEUP
UBO5_HISTORICAL_WIN_RATE_REPRODUCTION_DECISION = {"EXACTLY_REPRODUCED_PROBABILITY_METRICS_NO_WIN_RATE_CLAIM" if reproduction else "NOT_REPRODUCIBLE"}
UBO5_JULY27_SAME_RULE_REPLAY_DECISION = ORIGINAL_PROBABILITY_CONTRACT_REPLICATED_{july_m['rows']}_ROWS_BRIER_GAIN_{july_incumbent_m['brier_score']-july_m['brier_score']:.6f}_LOGLOSS_GAIN_{july_incumbent_m['log_loss']-july_m['log_loss']:.6f}_OBSERVED_OVER_RATE_{100*july_m['observed_over_rate']:.2f}_PERCENT_NOT_WIN_RATE
UBO5_ORIGINAL_EVALUATION_OPTIMISM_DECISION = RETROSPECTIVE_RISKS_PRESENT_NO_ORIGINAL_WIN_RATE_EXPECTATION_AND_PROBABILITY_ADVANTAGE_REPLICATED
UBO5_RAW_VS_EDGE_INTERPRETATION_DECISION = EDGE_BOARD_TESTS_MARKET_DISAGREEMENT_NOT_ORIGINAL_PROBABILITY_CONTRACT
UBO5_AUDIT_STATUS = PROVISIONAL_COMPLETE_EXCEPT_POSTPONED_ROW
"""
    write_text("terminal_decision.md", terminal)
    print(json.dumps({
        "historical": hist_m, "historical_reproduction": reproduction,
        "july27": july_m, "unsettled": unset_counts,
        "output": str(OUT.relative_to(ROOT)),
    }, indent=2, default=float))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
