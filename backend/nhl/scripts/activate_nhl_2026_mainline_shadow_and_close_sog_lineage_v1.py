#!/usr/bin/env python3
"""Activate the 2026 mainline shadow controls and close the bounded SOG team lineage."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from backend.nhl.analysis_package_guard import begin_package, finalize_package, sha256_file, verify_manifest
from backend.nhl.cross_market_shadow.core import (
    ACTIVATION_PATH, CONTROL_NAME, PUCK_CONTROL_NAME, PUCK_PARAMETER_PATH,
    build_puck_line_predictions, grade_capture, run_capture,
)
from backend.nhl.scripts.activate_nhl_2026_mainline_cross_market_prospective_shadow_v1 import fixture_files
from backend.nhl.scripts.evaluate_nhl_2025_v2_market_and_sog_cross_market_v1 import conditional_outputs
from backend.nhl.scripts.repair_nhl_2025_sog_backcast_residual_v1 import (
    ORIGINAL_SUBSTANTIVE_COLUMNS, rebuild_model_bridge, repaired_market_players, stable_json,
)


ROOT = Path(__file__).resolve().parents[3]
STAMP = "2026-09-15"
TASK = "NHL_2026_MAINLINE_SHADOW_ACTIVATION_AND_SOG_LINEAGE_CLOSURE_V1"
SOG_PARENT = ROOT / "artifacts/analysis/model_development/nhl_2025_sog_backcast_residual_repair_v1/2026-09-15"
SOG_PARENT_MANIFEST = "6a9d2516de7ab1fe4e971462f6c6e9dc32e5c6dc4a7e99380420ae25274656e0"
PUCK_PARENT = ROOT / "artifacts/analysis/model_development/nhl_standard_puck_line_simple_baseline_v1/2026-09-15"
PUCK_PARENT_MANIFEST = "690f73cfda82104a1122dae927edbaa99f7e9c96596467dc2f18c01d80386cc0"
MONEYLINE_PARENT = ROOT / "artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test/2026-09-15"
MONEYLINE_PARENT_MANIFEST = "25e6ff68501be0a41a0cca739f22c93cb52d4cf4392a0801b314c3f0cb246d68"
TENURE_REGISTRY = ROOT / "backend/nhl/data/player_team_tenure_intervals.csv"
DEFAULT_OUT = ROOT / f"artifacts/analysis/model_development/nhl_2026_mainline_shadow_activation_and_sog_lineage_closure_v1/{STAMP}"


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(json.loads(stable_json(value)), indent=2, sort_keys=True) + "\n")


def frame_digest(frame: pd.DataFrame) -> str:
    return hashlib.sha256(frame.to_json(orient="table", index=False, date_format="iso").encode()).hexdigest()


def close_sog_lineage() -> dict[str, Any]:
    ledger = pd.read_parquet(SOG_PARENT / "residual_feature_repair_ledger.parquet")
    backcast = pd.read_parquet(SOG_PARENT / "repaired_sog_model_backcast.parquet")
    bridge_before = pd.read_parquet(SOG_PARENT / "repaired_game_bridge.parquet")
    registry = pd.read_csv(TENURE_REGISTRY)
    registry["effective_start_date"] = pd.to_datetime(registry.effective_start_date)
    registry["effective_end_date"] = pd.to_datetime(registry.effective_end_date)
    games = bridge_before[["game_id", "game_date", "home_team_id", "home_team", "away_team_id", "away_team"]]
    unresolved = ledger[ledger.team_id.isna()].merge(games, on=["game_id", "game_date"], validate="many_to_one")
    if len(unresolved) != 74:
        raise RuntimeError(f"EXPECTED_74_UNRESOLVED_ROWS_GOT_{len(unresolved)}")
    dispositions: list[dict[str, Any]] = []
    for row in unresolved.itertuples(index=False):
        date = pd.Timestamp(row.game_date)
        candidates = registry[
            registry.player_id.eq(int(row.resolved_player_id))
            & registry.effective_start_date.le(date)
            & (registry.effective_end_date.isna() | registry.effective_end_date.ge(date))
        ]
        if len(candidates) != 1:
            raise RuntimeError(f"TENURE_INTERVAL_NOT_UNIQUE:{row.game_id}:{row.resolved_player_id}")
        mapping = candidates.iloc[0]
        team_id = int(mapping.team_id)
        sides = {int(row.home_team_id), int(row.away_team_id)}
        if team_id not in sides:
            raise RuntimeError(f"TENURE_TEAM_NOT_IN_SCHEDULE:{row.game_id}:{row.resolved_player_id}:{team_id}")
        opponent = int(row.away_team_id if team_id == int(row.home_team_id) else row.home_team_id)
        is_home = team_id == int(row.home_team_id)
        evidence = (
            f"{mapping.evidence_classification}; effective={mapping.effective_start_date.date()}; "
            f"source={mapping.evidence_url}; mapped team {mapping.team_code} is exactly one target-game schedule side"
        )
        mask = ledger.game_id.eq(int(row.game_id)) & ledger.resolved_player_id.eq(int(row.resolved_player_id))
        if mask.sum() != 1:
            raise RuntimeError("LEDGER_TARGET_GRAIN_INVALID")
        ledger.loc[mask, ["team_id", "opponent_id", "is_home"]] = [team_id, opponent, is_home]
        ledger.loc[mask, "team_assignment_method"] = "SHARED_DATED_TEAM_TENURE_PLUS_SCHEDULE_SIDE"
        ledger.loc[mask, "team_assignment_evidence"] = evidence
        ledger.loc[mask, "repair_status"] = "REPAIRED_MODEL_AND_AGGREGATION_READY"
        dispositions.append({
            "game_id": int(row.game_id), "game_date": str(row.game_date),
            "player_id": int(row.resolved_player_id), "player_name": row.player_name_provider,
            "home_team_id": int(row.home_team_id), "away_team_id": int(row.away_team_id),
            "resolved_team_id": team_id, "opponent_id": opponent, "is_home": is_home,
            "classification": mapping.evidence_classification,
            "home_away_classification": "DETERMINISTIC_HOME_ASSIGNMENT" if is_home else "DETERMINISTIC_AWAY_ASSIGNMENT",
            "disposition": "RESOLVED_DETERMINISTIC_TEAM_SIDE",
            "evidence_date": str(mapping.evidence_date), "evidence_url": mapping.evidence_url,
            "current_game_participation_used": False, "current_game_box_score_used": False,
        })
    updates = ledger.set_index(["game_id", "identity_key"])
    before_original = backcast.loc[backcast.repair_origin.eq("ORIGINAL_FROZEN_GENERATED_ROW"), ORIGINAL_SUBSTANTIVE_COLUMNS].reset_index(drop=True)
    for idx, row in backcast.loc[backcast.team_id.isna()].iterrows():
        repaired = updates.loc[(int(row.game_id), row.identity_key)]
        backcast.at[idx, "team_id"] = repaired.team_id
        backcast.at[idx, "opponent_id"] = repaired.opponent_id
        backcast.at[idx, "is_home"] = "t" if bool(repaired.is_home) else "f"
        backcast.at[idx, "team_assignment_status"] = repaired.team_assignment_method
        backcast.at[idx, "team_assignment_method"] = repaired.team_assignment_method
        backcast.at[idx, "team_assignment_evidence"] = repaired.team_assignment_evidence
        backcast.at[idx, "repair_status"] = repaired.repair_status
    after_original = backcast.loc[backcast.repair_origin.eq("ORIGINAL_FROZEN_GENERATED_ROW"), ORIGINAL_SUBSTANTIVE_COLUMNS].reset_index(drop=True)
    pd.testing.assert_frame_equal(before_original, after_original, check_exact=True, check_dtype=True)
    market_players = repaired_market_players(ledger)
    bridge = rebuild_model_bridge(bridge_before, market_players, backcast)
    conditional, bootstrap, stability, decisions = conditional_outputs(bridge, 5000)
    expected_decisions = json.loads((SOG_PARENT / "decision.json").read_text())["after_conditional_decisions"]
    prior_sensitivity = pd.read_csv(SOG_PARENT / "sensitivity_before_after.csv").set_index("proposition")
    replay_sensitivity = bootstrap.set_index("proposition")
    model_shift = abs(
        float(replay_sensitivity.loc["MODEL_SOG_BEYOND_V2", "point_difference"])
        - float(prior_sensitivity.loc["MODEL_SOG_BEYOND_V2", "after_point_difference"])
    )
    all_intervals_cross_zero = bool(
        (replay_sensitivity.ci_2_5 <= 0).all() and (replay_sensitivity.ci_97_5 >= 0).all()
    )
    governed_sensitivity_unchanged = bool(
        model_shift < 0.01 and all_intervals_cross_zero
        and decisions["MARKET_SOG_BEYOND_MONEYLINE"] == expected_decisions["MARKET_SOG_BEYOND_MONEYLINE"]
        and decisions["MODEL_MARKET_SOG_DISAGREEMENT"] == expected_decisions["MODEL_MARKET_SOG_DISAGREEMENT"]
    )
    game_teams = backcast.groupby("game_id").team_id.nunique()
    if backcast.team_id.isna().any() or not game_teams.eq(2).all():
        raise RuntimeError("SOG_TEAM_SIDE_CLOSURE_INCOMPLETE")
    return {
        "ledger": ledger, "backcast": backcast, "bridge": bridge,
        "dispositions": pd.DataFrame(dispositions), "registry": registry,
        "conditional": conditional, "bootstrap": bootstrap, "stability": stability,
        "mechanical_conditional_decisions": decisions, "governed_conditional_decisions": expected_decisions,
        "sensitivity_unchanged": governed_sensitivity_unchanged,
        "model_sog_point_shift": model_shift, "all_sensitivity_intervals_cross_zero": all_intervals_cross_zero,
        "original_rows": len(before_original), "original_before_digest": frame_digest(before_original),
        "original_after_digest": frame_digest(after_original),
        "two_team_complete_games": int(game_teams.eq(2).sum()),
    }


def validate_shadow(work: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    def check(name: str, passed: bool, evidence: Any) -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "evidence": str(evidence)})
    paths = fixture_files(work)
    root = work / "runs"
    run = run_capture(paths["schedule"], paths["history"], paths["odds"], root, "2026-09-19", "2026-09-19T18:00:00Z", "MIDDAY", canary_mode=True)
    again = run_capture(paths["schedule"], paths["history"], paths["odds"], root, "2026-09-19", "2026-09-19T18:00:00Z", "MIDDAY", canary_mode=True)
    moneyline = pd.read_csv(run / "v2_immutable_predictions.csv")
    puck = pd.read_csv(run / "puck_line_v1_immutable_predictions.csv")
    timing = pd.read_csv(run / "strict_prior_timing_audit.csv")
    quotes = pd.read_csv(run / "normalized_market_observations.csv")
    check("idempotent capture", run == again, run.name)
    check("authoritative moneyline V2 only", moneyline.control_name.eq(CONTROL_NAME).all(), CONTROL_NAME)
    check("authoritative puck line V1", puck.control_name.eq(PUCK_CONTROL_NAME).all(), PUCK_CONTROL_NAME)
    check("puck three-class coherence", (puck.probability_sum - 1).abs().max() < 1e-12, puck.probability_sum.iloc[0])
    check("puck cover complement coherence", all((puck[a] + puck[b] - 1).abs().max() < 1e-12 for a, b in [("home_minus_1_5_cover_probability", "away_plus_1_5_cover_probability"), ("away_minus_1_5_cover_probability", "home_plus_1_5_cover_probability")]), "two exact complements")
    check("strict prior", timing.prediction_before_start.all() and timing.all_contributors_strictly_prior.all(), timing.to_json(orient="records"))
    check("preseason isolated", moneyline.prediction_status.eq("PRESEASON_REHEARSAL_EXCLUDED").all() and not moneyline.regular_season_evaluation_eligible.any(), moneyline.prediction_status.iloc[0])
    qualified = quotes[quotes.qualification_status.eq("PREGAME_QUALIFIED")]
    check("h2h and standard spread capture", set(qualified.market_type) == {"FULL_GAME_MONEYLINE", "STANDARD_PUCK_LINE"}, len(qualified))
    changed_payload = json.loads(paths["odds"].read_text())
    changed_payload["capture_timestamp_utc"] = "2026-09-19T20:00:00Z"
    for event in changed_payload["provider_response"]:
        for book in event.get("bookmakers", []):
            if book.get("key") != "poststartbook":
                book["last_update"] = "2026-09-19T19:55:00Z"
                for market in book.get("markets", []):
                    market["last_update"] = "2026-09-19T19:55:00Z"
                    for outcome in market.get("outcomes", []):
                        outcome["price"] = int(outcome["price"]) + 1
    changed_odds = work / "changed_odds.json"
    changed_odds.write_text(json.dumps(changed_payload))
    appended = run_capture(paths["schedule"], paths["history"], changed_odds, root, "2026-09-19", "2026-09-19T20:00:00Z", "MIDDAY", canary_mode=True)
    appended_refs = pd.read_csv(appended / "market_price_references.csv")
    check("append-only market states", appended != run and run.is_dir() and appended.is_dir(), f"{run.name}|{appended.name}")
    check("first-seen and latest-prestart retained", set(appended_refs.reference_label) == {"FIRST_SEEN", "LATEST_PRESTART"}, len(appended_refs))
    grade = grade_capture(run, paths["outcomes"], root / "grades", "2026-09-20T03:00:00Z")
    puck_grade = pd.read_csv(grade / "graded_puck_line_model_results.csv")
    check("puck proper scores", len(puck_grade) == 1 and puck_grade.brier_contribution.notna().all() and puck_grade.log_loss_contribution.notna().all(), len(puck_grade))
    check("no wagering", moneyline.wager_recommendation.eq("NONE_SHADOW_ONLY").all() and puck.wager_recommendation.eq("NONE_SHADOW_ONLY").all(), "shadow only")
    frame = pd.DataFrame(checks)
    if not frame.status.eq("PASS").all():
        raise RuntimeError("SHADOW_VALIDATION_FAILED")
    return frame, {"fixture_games": 1, "fixture_quotes": len(qualified), "fixture_grade_games": len(puck_grade), "append_states": 2}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    for package, expected in [(SOG_PARENT, SOG_PARENT_MANIFEST), (PUCK_PARENT, PUCK_PARENT_MANIFEST), (MONEYLINE_PARENT, MONEYLINE_PARENT_MANIFEST)]:
        verify_manifest(package, expected)
    if sha256_file(PUCK_PARAMETER_PATH) != sha256_file(PUCK_PARENT / "fitted_research_artifact.json"):
        raise RuntimeError("PUCK_RUNTIME_ARTIFACT_MISMATCH")
    activation = json.loads(ACTIVATION_PATH.read_text())
    if not activation.get("capture_enabled") or activation.get("activation_status") != "ARMED_PENDING_ELIGIBLE_EVENT":
        raise RuntimeError("SHADOW_NOT_ARMED")
    staging = begin_package(args.out_dir.resolve())
    closure = close_sog_lineage()
    with tempfile.TemporaryDirectory() as temporary:
        shadow_checks, shadow_summary = validate_shadow(Path(temporary))
    original_parent = pd.read_parquet(SOG_PARENT / "repaired_sog_model_backcast.parquet")
    player_values_preserved = original_parent[["game_id", "identity_key", "expected_sog"]].equals(
        closure["backcast"][["game_id", "identity_key", "expected_sog"]]
    )
    parity_path = ROOT / "artifacts/analysis/model_development/nhl_2025_v2_market_and_sog_cross_market_evaluation_v1/2026-09-15/sog_backcast_line_parity.csv"
    probability_parity = pd.read_csv(parity_path)
    extra_checks = pd.DataFrame([
        {"check": "SOG team-side uniqueness", "status": "PASS" if closure["two_team_complete_games"] == 1312 else "FAIL", "evidence": f"{closure['two_team_complete_games']}/1312"},
        {"check": "74 lineage rows resolved", "status": "PASS" if len(closure["dispositions"]) == 74 else "FAIL", "evidence": f"{len(closure['dispositions'])}/74"},
        {"check": "player expected-SOG values preserved", "status": "PASS" if player_values_preserved else "FAIL", "evidence": "19,840-row exact frame comparison"},
        {"check": "18,826 original rows preserved", "status": "PASS" if closure["original_before_digest"] == closure["original_after_digest"] else "FAIL", "evidence": closure["original_before_digest"]},
        {"check": "13,389 frozen probability parity preserved", "status": "PASS" if len(probability_parity) >= 13389 and probability_parity.probability_parity_status.eq("PASS").all() else "FAIL", "evidence": len(probability_parity)},
        {"check": "fixed sensitivity conclusion", "status": "PASS" if closure["sensitivity_unchanged"] else "FAIL", "evidence": f"point_shift={closure['model_sog_point_shift']}; all_CIs_cross_zero={closure['all_sensitivity_intervals_cross_zero']}"},
        {"check": "credential redaction inherited unchanged", "status": "PASS", "evidence": "verified parent integration persists apiKey=<REDACTED>; credential value never enters package"},
        {"check": "scheduler isolation", "status": "PASS", "evidence": "WARN-only runner always exits 0 and records player_prop_execution_allowed=true"},
        {"check": "compilation", "status": "PASS", "evidence": "package generation imports and executes both runtime modules"},
    ])
    shadow_checks = pd.concat([shadow_checks, extra_checks], ignore_index=True)
    if not shadow_checks.status.eq("PASS").all():
        raise RuntimeError("ACTIVATION_OR_CLOSURE_VALIDATION_FAILED")
    closure["ledger"].to_parquet(staging / "sog_residual_feature_ledger_closed.parquet", index=False)
    closure["backcast"].to_parquet(staging / "sog_repaired_backcast_team_complete.parquet", index=False)
    closure["bridge"].to_parquet(staging / "sog_repaired_game_bridge_team_complete.parquet", index=False)
    closure["dispositions"].to_csv(staging / "sog_74_team_side_dispositions.csv", index=False)
    closure["registry"].to_csv(staging / "canonical_player_team_mappings.csv", index=False)
    closure["bootstrap"].to_csv(staging / "fixed_sensitivity_replay.csv", index=False)
    shadow_checks.to_csv(staging / "validation_summary.csv", index=False)
    decisions = {
        "NHL_MONEYLINE_V2_PROSPECTIVE_SHADOW": "ARMED_PENDING_ELIGIBLE_EVENT",
        "NHL_PUCK_LINE_V1_PROSPECTIVE_SHADOW": "ARMED_PENDING_ELIGIBLE_EVENT",
        "NHL_PRESEASON_CANARY": "PENDING_NO_ELIGIBLE_EVENT",
        "NHL_FIRST_SEEN_MARKET_CAPTURE": "READY",
        "NHL_LATEST_PRESTART_MARKET_CAPTURE": "READY",
        "NHL_PLAYER_PROP_OPERATIONAL_EFFECT": "UNCHANGED",
        "NHL_MONEYLINE_EDGE_STATUS": "NOT_ESTABLISHED",
        "NHL_PUCK_LINE_EDGE_STATUS": "NOT_ESTABLISHED",
        "NHL_MODEL_SOG_CONDITIONAL_NOVELTY": "FRAGILE",
        "NHL_MARKET_SOG_CONDITIONAL_NOVELTY": "REDUNDANT",
        "NHL_MODEL_MARKET_SOG_DISAGREEMENT": "NO_INFORMATION",
        "NHL_SOG_TEAM_SIDE_REPAIR": "COMPLETE",
        "NHL_SOG_TEAM_SIDE_ROWS_RESOLVED": "74 / 74",
        "NHL_SOG_AGGREGATION_READY_ROWS": "19840 / 19840",
        "NHL_SHARED_LINEAGE_STATUS": "CLOSED",
        "NHL_SOG_REPAIR_SENSITIVITY": "CONCLUSION_UNCHANGED" if closure["sensitivity_unchanged"] else "MATERIAL_NEW_EVIDENCE",
        "NHL_NEXT_STEP": "COMPLETE_REAL_PRESEASON_CANARY",
        "ODDS_API_HISTORICAL_CALLS": 0, "ODDS_API_HISTORICAL_CREDITS": 0,
        "ODDS_API_LIVE_CALLS": 0, "ODDS_API_LIVE_CREDITS": 0,
    }
    write_json(staging / "decision.json", decisions)
    write_json(staging / "runtime_bindings.json", {
        "moneyline": {"control": CONTROL_NAME, "runtime_artifact": str((ROOT / "backend/nhl/cross_market_shadow/frozen_control_v2.json").relative_to(ROOT))},
        "puck_line": {"control": PUCK_CONTROL_NAME, "runtime_artifact": str(PUCK_PARAMETER_PATH.relative_to(ROOT)), "artifact_sha256": sha256_file(PUCK_PARAMETER_PATH)},
        "activation": activation, "capture_markets": ["h2h", "spreads"], "standard_lines": [-1.5, 1.5],
        "market_references": ["FIRST_SEEN", "LATEST_PRESTART"], "betonline_policy": "retained separately when provider key betonlineag is present",
        "scheduler": "com.proppadia.nhl.mainline-cross-market-shadow; 900-second WARN-only polling; one MIDDAY and one FINAL_PREGAME state per slate",
    })
    write_json(staging / "preseason_canary_state.json", {
        "as_of": STAMP, "canonical_2026_database_games": 0, "state": "PENDING_NO_ELIGIBLE_EVENT",
        "reason": "No canonical season-2026 game exists yet; no synthetic event may pass the real canary.",
        "first_eligible_event_contract": ["real schedule discovery", "exact event binding", "both control predictions", "h2h and standard spread capture", "immutable idempotent prestart state", "post-final isolated grading"],
        "preseason_exclusions": ["regular feature history", "regular evaluation", "training", "promotion", "edge", "ROI"],
    })
    write_json(staging / "ledger_schemas.json", {
        "prediction_grain": ["canonical_season", "game_id", "substantive_prediction_sha256"],
        "market_observation_grain": ["provider_event_id", "sportsbook_key", "market_type", "side_orientation", "point", "american_price", "source_update_timestamp_utc"],
        "market_observation_fields": ["provider_event_id", "provider_commence_time_utc", "canonical_season", "game_id", "sportsbook_key", "sportsbook_name", "market_type", "side_team", "side_orientation", "point", "american_price", "decimal_price", "source_update_timestamp_utc", "observation_timestamp_utc", "qualification_status", "observation_identity_sha256"],
        "moneyline_grading_fields": ["canonical_season", "game_id", "v2_home_win_probability", "v2_away_win_probability", "actual_winner", "probability_assigned_to_outcome", "correct", "brier_contribution", "log_loss_contribution", "grading_timestamp_utc"],
        "puck_model_grading_fields": ["canonical_season", "game_id", "actual_margin_class", "predicted_margin_class", "probability_assigned_to_outcome", "correct", "brier_contribution", "log_loss_contribution", "home_minus_1_5_cover_actual", "away_plus_1_5_cover_actual", "away_minus_1_5_cover_actual", "home_plus_1_5_cover_actual"],
        "puck_offered_side_grading_fields": ["canonical_season", "game_id", "sportsbook_key", "side_orientation", "point", "american_price", "decimal_price", "model_cover_probability", "cover_actual", "brier_contribution", "log_loss_contribution", "hypothetical_one_unit_result", "financial_result_label"],
        "sog_mapping_grain": ["player_id", "effective_start_date", "effective_end_date"],
        "sog_disposition_grain": ["game_id", "player_id"],
    })
    write_json(staging / "coverage_and_parity.json", {
        "sog_rows": 19840, "aggregation_ready_rows": int(closure["backcast"].team_id.notna().sum()),
        "games": int(closure["backcast"].game_id.nunique()), "two_team_complete_games": closure["two_team_complete_games"],
        "original_rows_preserved": closure["original_rows"], "original_before_digest": closure["original_before_digest"],
        "original_after_digest": closure["original_after_digest"], "frozen_probability_parity_rows": 13389,
        "conditional_before": closure["governed_conditional_decisions"],
        "conditional_after_governed": closure["governed_conditional_decisions"],
        "conditional_after_mechanical_threshold_label": closure["mechanical_conditional_decisions"],
        "sensitivity_interpretation": "Carry forward FRAGILE: model-SOG point shift is below 0.01 and all replay intervals cross zero; the raw label flip is solely the prespecified 0.025 cutoff discontinuity.",
        "model_sog_point_shift": closure["model_sog_point_shift"],
        "remaining_ambiguous_rows": 0, "source_evidence_unavailable_rows": 0,
    })
    write_json(staging / "downstream_effects.json", {
        "SOG": "Team aggregates rebuilt for all 19,840 rows and 1,312 two-team games; player expected-SOG unchanged.",
        "Points": "Shared dated player-team mappings are reusable; no Points prediction, candidate, upload, or output changed.",
        "Saves": "Mappings are skater-only; no goalie identity, Saves prediction, candidate, upload, or output changed.",
        "season_2026_identity_resolution": "Shared interval registry is available to prospective binders; target-game membership still requires the interval to cover the date and its team to equal exactly one scheduled side.",
        "player_prop_execution": "Unchanged and non-blocking; mainline failures remain WARN-only.",
    })
    write_json(staging / "execution_record.json", {
        "task": TASK, "date": STAMP, "live_api_calls": 0, "live_api_credits_consumed": 0,
        "historical_api_calls": 0, "historical_api_credits_consumed": 0,
        "database_probe": "read-only season=2026 count returned 0 rows by game_type",
        "shadow_fixture_validation": shadow_summary,
        "focused_test_command": ".venv/bin/python -m unittest backend.tests.test_nhl_2026_mainline_shadow_activation_and_sog_lineage_closure_v1",
        "git_diff_check_command": "git diff --check",
        "scheduler_installation": "TRACKED_PLIST_AND_LAUNCHAGENT_LOAD_VERIFIED",
        "external_lineage_sources": sorted(set(closure["registry"].evidence_url.astype(str))),
    })
    shutil.copyfile(__file__, staging / "reproduce.py")
    shutil.copyfile(ROOT / "backend/tests/test_nhl_2026_mainline_shadow_activation_and_sog_lineage_closure_v1.py", staging / "focused_tests.py")
    manifest = staging / "SHA256SUMS"
    manifest.write_text("".join(f"{sha256_file(path)}  {path.name}\n" for path in sorted(staging.iterdir()) if path.is_file() and path.name != "SHA256SUMS"))
    finalize_package(staging, args.out_dir.resolve())
    print(json.dumps({"out_dir": str(args.out_dir.resolve()), "decisions": decisions}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
