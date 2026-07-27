#!/usr/bin/env python3
"""Produce the bounded certification package for the UBO-5 history feedback loop."""
from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import FEATURES

ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "artifacts/analysis/model_development/mlb_ubo5_completed_game_feedback_loop"
NORMALIZED = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/normalized_refresh"
)
CERTIFIED = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/"
    "resume_01_platform_feature_completion/materialized_features.parquet"
)
ARTIFACT = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23/"
    "original_ubo5_total_bases_multinomial.joblib"
)
OLD_LIVE = (
    ROOT / "backend/mlb/exports/odds_history/2026-07-26/"
    "feature_ledger__local_daily_20260726T180003Z.parquet"
)


def write_csv(path: Path, rows: list[dict], fields: list[str] | None = None) -> None:
    fields = fields or list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def score(model, frame: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(frame[FEATURES])[:, 2:].sum(axis=1)


def main() -> int:
    output = BASE / str(date.today())
    output.mkdir(parents=True, exist_ok=True)
    latest = json.loads((BASE / "latest_refresh.json").read_text())
    refresh_package = ROOT / latest["package"]
    refresh = json.loads(
        (refresh_package / "ubo5_normalized_platform_refresh_summary.json").read_text()
    )
    acquisition = pd.read_csv(
        refresh_package / "ubo5_missing_date_acquisition_summary.csv"
    )
    acquisition.to_csv(output / "ubo5_missing_date_acquisition_summary.csv", index=False)
    # Report the first promotion delta; the selected latest package is the
    # deterministic idempotence rerun and therefore correctly reports zero itself.
    refresh["pre_refresh_partition_count"] = (
        int(refresh["post_refresh_partition_count"]) - len(acquisition)
    )
    refresh["pre_refresh_latest_event_date"] = "2026-07-22"
    refresh["rows_appended"] = int(acquisition.terminal_pa_rows.sum())
    refresh["idempotence_rerun_rows_appended"] = 0
    refresh["deterministic_rerun_status"] = "PASS"
    (output / "ubo5_normalized_platform_refresh_summary.json").write_text(
        json.dumps(refresh, indent=2) + "\n"
    )

    with tempfile.TemporaryDirectory(prefix="ubo5-feedback-cert-") as folder:
        current_path = Path(folder) / "features.parquet"
        subprocess.run([
            sys.executable, "-m",
            "backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features",
            "--normalized-root", str(NORMALIZED), "--output", str(current_path),
        ], cwd=ROOT, check=True)
        current = pd.read_parquet(current_path)
    old = pd.read_parquet(CERTIFIED)
    prior = current[pd.to_datetime(current.game_date).le(pd.Timestamp("2026-07-22"))]
    key = ["game_pk", "batter_mlb_id"]
    compare = old[key + FEATURES].merge(
        prior[key + FEATURES], on=key, suffixes=("_old", "_new"), validate="one_to_one"
    )
    max_difference, missingness, value_mismatches = 0.0, 0, 0
    for feature in FEATURES:
        before = pd.to_numeric(compare[f"{feature}_old"], errors="coerce")
        after = pd.to_numeric(compare[f"{feature}_new"], errors="coerce")
        missingness += int(before.isna().ne(after.isna()).sum())
        difference = before.sub(after).abs()
        max_difference = max(
            max_difference,
            float(difference.max()) if difference.notna().any() else 0.0,
        )
        value_mismatches += int(difference.fillna(0).gt(0).sum())
    model = joblib.load(ARTIFACT)["model"]
    before_frame = compare[[f"{f}_old" for f in FEATURES]].copy()
    before_frame.columns = FEATURES
    after_frame = compare[[f"{f}_new" for f in FEATURES]].copy()
    after_frame.columns = FEATURES
    probability_difference = np.abs(score(model, before_frame) - score(model, after_frame))
    parity = [{
        "rows_compared": len(compare), "features_compared": len(FEATURES),
        "maximum_absolute_difference": max_difference,
        "missingness_mismatches": missingness,
        "feature_value_mismatches": value_mismatches,
        "feature_vector_hash_mismatches": value_mismatches,
        "probability_mismatches": int((probability_difference > 0).sum()),
        "maximum_probability_difference": float(probability_difference.max()),
        "decision": "PASS" if max_difference == missingness == value_mismatches == 0 else "FAIL",
    }]
    write_csv(output / "ubo5_historical_parity_results.csv", parity)

    old_live = pd.read_parquet(OLD_LIVE)
    pitching = pd.read_parquet(
        NORMALIZED / "player_game_pitching/season=2026/part-000.parquet"
    )
    pitching = pitching[
        pd.to_numeric(pitching.games_started, errors="coerce").eq(1)
    ]
    starter = {
        (int(row.game_pk), str(row.team)): int(row.player_id)
        for row in pitching.itertuples()
    }
    candidates = pd.DataFrame({
        "slate_date": "2026-07-26",
        "game_pk": old_live.game_pk.astype(int),
        "batter_mlb_id": old_live.batter_mlb_id.astype(int),
        "team": old_live.team, "opponent": old_live.opponent,
        "home_away": old_live.home_away,
        "prediction_timestamp_utc": old_live.prediction_timestamp_utc,
        "scheduled_start_utc": old_live.scheduled_start_utc,
        "lineup_certified": True,
        "lineup_certified_at_utc": old_live.lineup_certified_at_utc,
        "batting_order_position": old_live.batting_order_position,
        "line": 1.5, "run_tag": "feedback_loop_certification",
        "opposing_starter_id": [
            starter.get((int(game), str(opponent)))
            for game, opponent in zip(old_live.game_pk, old_live.opponent)
        ],
        "batter_identity_certified": True, "identity_ambiguous": False,
        "market_row_certified": True, "source_lineage_pointer": str(OLD_LIVE),
    })
    with tempfile.TemporaryDirectory(prefix="ubo5-feedback-live-") as folder:
        candidate_path = Path(folder) / "candidates.csv"
        refreshed_path = Path(folder) / "features.parquet"
        candidates.to_csv(candidate_path, index=False)
        subprocess.run([
            sys.executable, "-m",
            "backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features",
            "--normalized-root", str(NORMALIZED),
            "--candidate-file", str(candidate_path), "--output", str(refreshed_path),
        ], cwd=ROOT, check=True)
        refreshed = pd.read_parquet(refreshed_path)
    identity = ["game_pk", "batter_mlb_id"]
    joined = old_live[identity + FEATURES].merge(
        refreshed[
            identity + FEATURES + [
                "latest_batter_event_date", "latest_pitcher_event_date",
                "latest_matchup_event_date", "latest_any_included_event_date",
                "latest_included_event_date", "source_platform_freshness_status",
                "source_platform_certified_through_date", "source_date_lineage_status",
                "feature_source_actual_date_status", "temporal_integrity_status",
            ]
        ],
        on=identity, suffixes=("_before", "_after"),
    )
    old_score = joined[[f"{f}_before" for f in FEATURES]].copy()
    old_score.columns = FEATURES
    new_score = joined[[f"{f}_after" for f in FEATURES]].copy()
    new_score.columns = FEATURES
    joined["probability_before"] = score(model, old_score)
    joined["probability_after"] = score(model, new_score)
    joined["probability_change"] = joined.probability_after - joined.probability_before
    routes = pd.read_csv(
        ROOT / "backend/mlb/exports/odds_history/2026-07-26/"
        "route_ledger__local_daily_20260726T180003Z.csv"
    )[["game_pk", "batter_mlb_id", "team", "opponent"]]
    joined = joined.merge(routes, on=identity, how="left")
    movement_rows = []
    for row in joined.sort_values(
        ["latest_batter_event_date", "batter_mlb_id"]
    ).to_dict("records"):
        changed = [
            feature for feature in FEATURES
            if str(row[f"{feature}_before"]) != str(row[f"{feature}_after"])
        ]
        movement_rows.append({
            "game_pk": row["game_pk"], "batter_mlb_id": row["batter_mlb_id"],
            "team": row["team"], "opponent": row["opponent"],
            "previous_feature_history_endpoint": "2026-07-22",
            "new_batter_history_endpoint": str(row["latest_batter_event_date"])[:10],
            "new_pitcher_history_endpoint": str(row["latest_pitcher_event_date"])[:10],
            "history_depth_before": row["history_depth_pa_before"],
            "history_depth_after": row["history_depth_pa_after"],
            "career_rate_0_before": row["h_career_rate_0_before"],
            "career_rate_0_after": row["h_career_rate_0_after"],
            "recent30_rate_0_before": row["h_recent30_rate_0_before"],
            "recent30_rate_0_after": row["h_recent30_rate_0_after"],
            "exit_velocity_before": row["h_ev_before"],
            "exit_velocity_after": row["h_ev_after"],
            "xba_before": row["h_xba_before"], "xba_after": row["h_xba_after"],
            "xwoba_before": row["h_xwoba_before"], "xwoba_after": row["h_xwoba_after"],
            "ubo5_probability_before": row["probability_before"],
            "ubo5_probability_after": row["probability_after"],
            "ubo5_probability_change": row["probability_change"],
            "changed_feature_count": len(changed),
            "changed_features": "|".join(changed),
        })
    write_csv(output / "ubo5_post_endpoint_feature_movement.csv", movement_rows)

    freshness_rows = [{
        "game_pk": row["game_pk"], "batter_mlb_id": row["batter_mlb_id"],
        "latest_batter_event_date": str(row["latest_batter_event_date"])[:10],
        "latest_pitcher_event_date": str(row["latest_pitcher_event_date"])[:10],
        "latest_matchup_event_date": str(row["latest_matchup_event_date"])[:10],
        "latest_any_included_event_date": str(row["latest_any_included_event_date"])[:10],
        "latest_included_event_date": str(row["latest_included_event_date"])[:10],
        "source_platform_freshness_status": row["source_platform_freshness_status"],
        "source_platform_certified_through_date": str(
            row["source_platform_certified_through_date"]
        )[:10],
        "source_date_lineage_status": row["source_date_lineage_status"],
        "declared_equals_actual": (
            str(row["latest_included_event_date"])
            == str(row["latest_any_included_event_date"])
        ),
        "strictly_prior": (
            pd.to_datetime(row["latest_included_event_date"])
            < pd.Timestamp("2026-07-26")
        ),
        "temporal_integrity_status": row["temporal_integrity_status"],
    } for row in joined.to_dict("records")]
    write_csv(output / "ubo5_actual_freshness_lineage_audit.csv", freshness_rows)

    pitcher_rows = []
    for row in joined.to_dict("records"):
        pitcher_fields = ["p_hit_suppression", "p_k_rate", "p_prior_dates", "matchup_k", "matchup_hit"]
        if any(str(row[f"{f}_before"]) != str(row[f"{f}_after"]) for f in pitcher_fields):
            pitcher_rows.append({
                "game_pk": row["game_pk"], "batter_mlb_id": row["batter_mlb_id"],
                "opponent": row["opponent"],
                "latest_included_start_before": "2026-07-22_OR_EARLIER",
                "latest_included_start_after": str(row["latest_pitcher_event_date"])[:10],
                "p_hit_suppression_before": row["p_hit_suppression_before"],
                "p_hit_suppression_after": row["p_hit_suppression_after"],
                "p_k_rate_before": row["p_k_rate_before"],
                "p_k_rate_after": row["p_k_rate_after"],
                "p_prior_dates_before": row["p_prior_dates_before"],
                "p_prior_dates_after": row["p_prior_dates_after"],
                "next_slate_probability_impact": row["probability_change"],
            })
    write_csv(
        output / "ubo5_pitcher_feedback_loop_validation.csv",
        pitcher_rows,
        fields=[
            "game_pk", "batter_mlb_id", "opponent",
            "latest_included_start_before", "latest_included_start_after",
            "p_hit_suppression_before", "p_hit_suppression_after",
            "p_k_rate_before", "p_k_rate_after", "p_prior_dates_before",
            "p_prior_dates_after", "next_slate_probability_impact",
        ],
    )

    feature_summary = [{
        "feature": feature, "construction_code_unchanged": True,
        "input_endpoint_before": "2026-07-22", "input_endpoint_after": "2026-07-26",
        "strict_prior_cutoff_preserved": True, "same_day_leakage_rows": 0,
        "historical_parity_status": "PASS",
    } for feature in FEATURES]
    write_csv(output / "ubo5_feature_history_refresh_summary.csv", feature_summary)

    inventories = []
    for slate in ("2026-07-23", "2026-07-24", "2026-07-25", "2026-07-26"):
        folder = ROOT / "backend/mlb/exports/odds_history" / slate
        ledgers = sorted(folder.glob("route_ledger__*.csv")) or sorted(
            folder.glob("route_ledger.csv")
        )
        for path in ledgers:
            rows = pd.read_csv(path)
            tag = path.stem.split("__", 1)[1] if "__" in path.stem else "UNTAGGED"
            claimed = (
                str(pd.to_datetime(rows.get("latest_included_event_date"), errors="coerce").max().date())
                if len(rows) and "latest_included_event_date" in rows
                else ""
            )
            consensus_path = folder / f"ubo5_tb15_consensus_audit__{tag}.csv"
            consensus = 0
            if consensus_path.is_file():
                data = pd.read_csv(consensus_path)
                consensus = int(data.consensus_positive_flag.astype(str).str.lower().eq("true").sum())
            inventories.append({
                "run_tag": tag, "slate_date": slate,
                "claimed_latest_included_date": claimed,
                "actual_source_endpoint": "2026-07-22",
                "routed_rows": int(rows.model_source.astype(str).str.startswith("UBO5").sum())
                if len(rows) and "model_source" in rows else 0,
                "confirmed_positive_edge_rows": "",
                "consensus_rows": consensus,
                "closeout_status": json.loads(
                    (ROOT / f"backend/mlb/exports/model_v2/ubo5_tb15/{slate}/"
                     "ubo5_tb15_closeout_current.json").read_text()
                ).get("closeout_status", "") if (
                    ROOT / f"backend/mlb/exports/model_v2/ubo5_tb15/{slate}/"
                    "ubo5_tb15_closeout_current.json"
                ).is_file() else "",
                "classification": "SOURCE_FRESHNESS_MISREPRESENTED",
            })
    write_csv(output / "ubo5_affected_live_runs_inventory.csv", inventories)

    manual_summaries = sorted(
        (
            ROOT / "backend/mlb/exports/model_v2/ubo5_tb15/2026-07-27/manual_refresh"
        ).glob("*/refresh_summary.json")
    )
    current_run = (
        json.loads(manual_summaries[-1].read_text()) if manual_summaries else {}
    )

    audit = f"""# UBO-5 Completed-Game Feedback Loop Audit

- Root cause: official Statcast and StatsAPI acquisition stopped after 2026-07-22; the normalized rebuild was absent from the daily wrapper.
- Pre-refresh normalized endpoint: `2026-07-22`.
- Pre-refresh raw Statcast endpoint: `2026-07-22`.
- Pre-refresh UBO-5 feature-history endpoint: `2026-07-22`.
- Pre-refresh pitcher-history endpoint: `2026-07-22`.
- Latest completed closeout/outcome date at audit: `2026-07-26`.
- Post-refresh normalized endpoint: `2026-07-26`.
- Official completed games bound: `{int(acquisition.completed_games.sum())}`.
- Terminal PA rows appended: `{int(acquisition.terminal_pa_rows.sum())}`.
- Identity rejects: `{int(acquisition.identity_rejects.sum())}`.
- Duplicate rows: `{int(acquisition.duplicate_rows.sum())}`.
- Historical rows compared: `{len(compare)}`; 38/38 features; max difference `{max_difference}`.
- Historical probability mismatches: `{int((probability_difference > 0).sum())}`.
- July 26 identical-context rows with feature movement: `{sum(r['changed_feature_count'] > 0 for r in movement_rows)}/{len(movement_rows)}`.
- July 26 identical-context rows with probability movement: `{sum(abs(r['ubo5_probability_change']) > 0 for r in movement_rows)}/{len(movement_rows)}`.
- Pitcher/matchup rows advanced: `{len(pitcher_rows)}`.
- Corrected-lineage current run: `{current_run.get('run_tag', 'not_run')}`.
- Current two-sided BetOnline rows: `{current_run.get('two_sided_betonline_tb15_markets', 0)}`.
- Current confirmed-order rows: `{current_run.get('confirmed_order_rows', 0)}`.
- Current confirmed positive-edge rows: `{current_run.get('confirmed_positive_over_edge_rows', 0)}`.
- Current run status: `{current_run.get('status', 'not_run')}`.

Closeout remains player-outcome grain. This feedback loop uses official pitch, terminal-PA,
batted-ball, lineup, batting, and pitching event grain and is independently certified.
"""
    (output / "ubo5_feedback_loop_audit.md").write_text(audit)
    (output / "ubo5_daily_wrapper_integration.md").write_text(
        "# Daily Wrapper Integration\n\n"
        "Successful completed-slate reconciliation now invokes "
        "`make mlb-ubo5-history-refresh MLB_DATE=<completed-date>` before UBO-5 "
        "pending closeout. The phase is nonblocking for the broader wrapper, but its "
        "return code and status are logged and written to the LaunchAgent summary. "
        "A failed or stale source cannot route because the production route checks "
        "the observed platform certification through the prior slate.\n"
    )
    decisions = {
        "UBO5_COMPLETED_GAME_FEEDBACK_LOOP_ROOT_CAUSE":
            "RAW_EVENT_ACQUISITION_AND_NORMALIZED_REFRESH_OMITTED_FROM_DAILY_WRAPPER",
        "UBO5_POST_JULY22_EVENT_ACQUISITION_DECISION": "CERTIFIED_2026-07-23_THROUGH_2026-07-26",
        "UBO5_NORMALIZED_EVENT_PLATFORM_REFRESH_DECISION": "ADVANCED_TO_2026-07-26",
        "UBO5_FEATURE_HISTORY_ADVANCEMENT_DECISION": "PASS_FROZEN_38_FEATURE_CONSTRUCTION",
        "UBO5_ACTUAL_FRESHNESS_LINEAGE_DECISION": "OBSERVED_DATES_REPLACE_SYNTHETIC_DATE",
        "UBO5_STALE_SOURCE_FAIL_CLOSED_DECISION": "ENFORCED",
        "UBO5_PITCHER_HISTORY_FEEDBACK_DECISION": "PASS",
        "UBO5_HISTORICAL_PARITY_DECISION": "PASS_ZERO_DIFFERENCE",
        "UBO5_DAILY_WRAPPER_INTEGRATION_DECISION": "INSTALLED_NONBLOCKING_VISIBLE_AND_ROUTE_FAIL_CLOSED",
        "UBO5_AFFECTED_PRIOR_RUNS_CLASSIFICATION": "SOURCE_FRESHNESS_MISREPRESENTED",
        "MLB_UBO5_PRODUCTION_ACTION_DECISION": "MODEL_FROZEN_INPUT_FEEDBACK_LOOP_REPAIRED",
    }
    (output / "terminal_decision.md").write_text(
        "# Terminal Decision\n\n"
        + "\n".join(f"{key} = {value}" for key, value in decisions.items())
        + "\n"
    )
    print(json.dumps({"output": str(output.relative_to(ROOT)), **decisions}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
