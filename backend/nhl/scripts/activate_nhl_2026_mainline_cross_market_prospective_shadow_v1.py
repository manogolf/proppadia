#!/usr/bin/env python3
"""Build the governed readiness package for the NHL 2026 V2 cross-market shadow."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.analysis_package_guard import begin_package, finalize_package, verify_manifest
from backend.nhl.cross_market_shadow.core import (
    CONTROL_NAME,
    FEATURES,
    PARAMETER_PATH,
    V1_DISPOSITION,
    build_v2_predictions,
    daily_status,
    fetch_markets,
    grade_capture,
    quota_estimate,
    run_capture,
    sha256,
)


ROOT = Path(__file__).resolve().parents[3]
DATE = "2026-09-15"
SLATE = "2026-09-19"
GAME_ID = 2026010001
START = "2026-09-19T23:00:00Z"
RUN_TIME = "2026-09-19T18:00:00Z"
PARENT = ROOT / "artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test/2026-09-15"
PARENT_MANIFEST_SHA256 = "25e6ff68501be0a41a0cca739f22c93cb52d4cf4392a0801b314c3f0cb246d68"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/nhl_2026_mainline_cross_market_prospective_shadow_v1/2026-09-15"


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def tree_hashes(path: Path) -> dict[str, str]:
    return {
        str(item.relative_to(path)): sha256(item)
        for item in sorted(path.rglob("*")) if item.is_file()
    }


def credential_configured() -> bool:
    """Return presence only; never return, print, or persist a credential value."""
    env_path = ROOT / "backend/.env"
    if not env_path.is_file():
        return False
    for raw in env_path.read_text().splitlines():
        stripped = raw.strip()
        if stripped.startswith("ODDS_API_KEY="):
            return bool(stripped.partition("=")[2].strip().strip("'\""))
    return False


def fixture_files(directory: Path) -> dict[str, Path]:
    schedule_columns = [
        "canonical_season", "slate_date", "game_id", "game_date", "scheduled_start_time_utc",
        "home_team_id", "home_team", "away_team_id", "away_team", "game_status",
        "game_type_code",
    ]
    schedule = pd.DataFrame([{
        "canonical_season": 2026, "slate_date": SLATE, "game_id": GAME_ID,
        "game_date": SLATE, "scheduled_start_time_utc": START,
        "home_team_id": 10, "home_team": "MTL", "away_team_id": 20,
        "away_team": "OTT", "game_status": "FUT", "game_type_code": 1,
    }], columns=schedule_columns)
    history = pd.DataFrame(columns=schedule_columns + [
        "final_home_goals", "final_away_goals", "final_home_shots", "final_away_shots",
    ])
    odds = {
        "capture_timestamp_utc": RUN_TIME,
        "provider": "SYNTHETIC_LOCAL_REHEARSAL_NO_NETWORK",
        "quota": {**quota_estimate(), "credits_consumed": 0, "requests_remaining": 999},
        "provider_response": [{
            "id": "synthetic-event-2026010001", "home_team": "Montreal Canadiens",
            "away_team": "Ottawa Senators", "commence_time": START,
            "bookmakers": [
                {
                    "key": "betonlineag", "title": "BetOnline", "last_update": "2026-09-19T17:55:00Z",
                    "markets": [
                        {"key": "h2h", "last_update": "2026-09-19T17:55:00Z", "outcomes": [
                            {"name": "Montreal Canadiens", "price": -120},
                            {"name": "Ottawa Senators", "price": 105},
                        ]},
                        {"key": "spreads", "last_update": "2026-09-19T17:55:00Z", "outcomes": [
                            {"name": "Montreal Canadiens", "point": -1.5, "price": 190},
                            {"name": "Ottawa Senators", "point": 1.5, "price": -220},
                            {"name": "Montreal Canadiens", "point": -2.5, "price": 300},
                        ]},
                    ],
                },
                {
                    "key": "draftkings", "title": "DraftKings", "last_update": "2026-09-19T17:58:00Z",
                    "markets": [
                        {"key": "h2h", "outcomes": [
                            {"name": "Montreal Canadiens", "price": -118},
                            {"name": "Ottawa Senators", "price": 102},
                        ]},
                        {"key": "spreads", "outcomes": [
                            {"name": "Montreal Canadiens", "point": -1.5, "price": 185},
                            {"name": "Ottawa Senators", "point": 1.5, "price": -215},
                        ]},
                    ],
                },
                {
                    "key": "poststartbook", "title": "Post Start Fixture", "last_update": "2026-09-19T23:01:00Z",
                    "markets": [{"key": "h2h", "outcomes": [
                        {"name": "Montreal Canadiens", "price": -130},
                        {"name": "Ottawa Senators", "price": 110},
                    ]}],
                },
            ],
        }],
    }
    sog = pd.DataFrame([{
        "game_id": GAME_ID, "sog_home_model_covered_player_count": 9,
        "sog_away_model_covered_player_count": 10,
        "sog_home_continuous_expectation_sum": 27.5,
        "sog_away_continuous_expectation_sum": 29.0,
        "sog_continuous_expectation_sum_diff": -1.5,
        "snapshot_timestamp_utc": "2026-09-19T17:45:00Z",
        "coverage_quality_state": "SYNTHETIC_COMPLETE",
        "sog_model_market_disagreement_identical_line": 0.03,
    }])
    points = pd.DataFrame([{
        "game_id": GAME_ID, "points_covered_player_count": 12,
        "snapshot_timestamp_utc": "2026-09-19T17:44:00Z",
        "coverage_quality_state": "PARTIAL_NO_STABLE_ORDERED_RELATIONSHIP",
    }])
    saves = pd.DataFrame([{
        "game_id": GAME_ID, "saves_market_listed_goalie_count": 2,
        "saves_starter_state": "MARKET_LISTED_STARTER_UNCONFIRMED",
        "snapshot_timestamp_utc": "2026-09-19T17:43:00Z",
        "coverage_quality_state": "PARTIAL_FRAGILE_STARTER_UNCONFIRMED",
    }])
    paths = {}
    for name, frame in [("schedule", schedule), ("history", history), ("sog", sog), ("points", points), ("saves", saves)]:
        paths[name] = directory / f"{name}.csv"
        frame.to_csv(paths[name], index=False)
    paths["odds"] = directory / "odds.json"
    write_json(paths["odds"], odds)
    outcomes = pd.DataFrame([{
        "canonical_season": 2026, "game_id": GAME_ID, "final_home_goals": 4,
        "final_away_goals": 2, "game_status": "FINAL", "outcome_source": "SYNTHETIC_CERTIFIED_FIXTURE",
        "outcome_source_timestamp_utc": "2026-09-20T02:30:00Z",
    }])
    paths["outcomes"] = directory / "outcomes.csv"
    outcomes.to_csv(paths["outcomes"], index=False)
    return paths


class FakeResponse:
    status = 200
    headers = {"x-requests-last": "4", "x-requests-remaining": "996", "x-requests-used": "4"}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return b"[]"


def run_validation(work: Path) -> tuple[list[dict], dict]:
    checks: list[dict] = []

    def check(name: str, passed: bool, evidence: object, severity: str = "CRITICAL") -> None:
        checks.append({
            "check": name, "severity": severity, "status": "PASS" if passed else "FAIL",
            "evidence": str(evidence),
        })

    verify_manifest(PARENT, PARENT_MANIFEST_SHA256)
    parent_control = json.loads((PARENT / "v2_fitted_control.json").read_text())
    runtime_control = json.loads(PARAMETER_PATH.read_text())
    check("verified V2 parent manifest", True, PARENT_MANIFEST_SHA256)
    check("runtime V2 semantic binding", parent_control == runtime_control, sha256(PARAMETER_PATH))
    check("V1 forward replay prohibited", V1_DISPOSITION == "HISTORICAL_REFERENCE_NOT_FORWARD_REPLAYABLE", V1_DISPOSITION)

    paths = fixture_files(work)
    run_root = work / "runs"
    first = run_capture(
        paths["schedule"], paths["history"], paths["odds"], run_root, SLATE, RUN_TIME,
        "MIDDAY", paths["sog"], paths["points"], paths["saves"], canary_mode=True,
    )
    first_tree = tree_hashes(first)
    second = run_capture(
        paths["schedule"], paths["history"], paths["odds"], run_root, SLATE, RUN_TIME,
        "MIDDAY", paths["sog"], paths["points"], paths["saves"], canary_mode=True,
    )
    check("idempotent unchanged capture", first == second and first_tree == tree_hashes(first), first.name)

    predictions = pd.read_csv(first / "v2_immutable_predictions.csv")
    timing = pd.read_csv(first / "strict_prior_timing_audit.csv")
    quotes = pd.read_csv(first / "normalized_market_observations.csv")
    references = pd.read_csv(first / "market_price_references.csv")
    coverage = pd.read_csv(first / "cross_market_game_state_observations.csv")
    qualified = quotes[quotes.qualification_status.eq("PREGAME_QUALIFIED")]
    check("deterministic V2 prediction", predictions.substantive_prediction_sha256.nunique() == 1, predictions.substantive_prediction_sha256.iloc[0])
    check("prediction uses V2 only", predictions.control_name.eq(CONTROL_NAME).all(), CONTROL_NAME)
    check("strict-prior timestamps", timing.prediction_before_start.all() and timing.all_contributors_strictly_prior.all(), timing.to_json(orient="records"))
    check("preseason tagged and excluded", predictions.prediction_status.eq("PRESEASON_REHEARSAL_EXCLUDED").all() and not predictions.regular_season_evaluation_eligible.any(), predictions.prediction_status.iloc[0])
    check("market event exact binding", pd.read_csv(first / "market_event_binding.csv").binding_status.eq("BOUND").all(), GAME_ID)
    check("h2h and standard puck line captured", set(qualified.market_type) == {"FULL_GAME_MONEYLINE", "STANDARD_PUCK_LINE"} and set(qualified.loc[qualified.market_type.eq("STANDARD_PUCK_LINE"), "point"]) == {-1.5, 1.5}, len(qualified))
    check("post-start observations cannot replace pregame", quotes.loc[quotes.sportsbook_key.eq("poststartbook"), "qualification_status"].eq("POST_START_INVALID").all() and not references.sportsbook_key.eq("poststartbook").any(), "poststartbook quarantined")
    check("BetOnline represented without substitution", qualified.loc[qualified.sportsbook_key.eq("betonlineag"), "betonline_available"].all(), "betonlineag")
    check("price reference labels truthful", set(references.reference_label) == {"FIRST_SEEN", "LATEST_PRESTART"}, len(references))
    check("normalized observation identities unique", not quotes.observation_identity_sha256.duplicated().any(), quotes.observation_identity_sha256.nunique())
    check("all cross-market coverage present", all(bool(coverage[column].iloc[0]) for column in ["sog_strict_prior_available", "points_strict_prior_available", "saves_strict_prior_available", "moneyline_market_available", "puck_line_market_available", "v2_prediction_available"]), coverage.to_json(orient="records"))
    check("goalie remains unconfirmed", not coverage.confirmed_goalie_inferred.any() and coverage.saves_classification.eq("PARTIAL_FRAGILE_STARTER_UNCONFIRMED").all(), coverage.saves_classification.iloc[0])

    grade_root = run_root / "grades"
    grade = grade_capture(first, paths["outcomes"], grade_root, "2026-09-20T03:00:00Z")
    grade_tree = tree_hashes(grade)
    grade_again = grade_capture(first, paths["outcomes"], grade_root, "2026-09-20T03:00:00Z")
    moneyline = pd.read_csv(grade / "graded_moneyline_shadow_results.csv")
    comparison = pd.read_csv(grade / "graded_moneyline_market_comparison.csv")
    puck = pd.read_csv(grade / "graded_puck_line_market_results.csv")
    check("idempotent append-only grading", grade == grade_again and grade_tree == tree_hashes(grade), grade.name)
    check("preseason Moneyline retained as non-evaluation", len(moneyline) == 1 and moneyline.evaluation_status.eq("PRESEASON_NON_EVALUATION").all() and moneyline.correct.isna().all() and moneyline.brier_contribution.isna().all() and moneyline.log_loss_contribution.isna().all(), moneyline[["evaluation_status", "correct", "brier_contribution", "log_loss_contribution"]].to_json(orient="records"))
    check("preseason market quotes retained outside evaluation", len(comparison) == 8 and comparison.no_vig_probability.notna().all() and comparison.evaluation_status.eq("PRESEASON_NON_EVALUATION").all() and comparison.v2_minus_market_probability_gap.isna().all(), len(comparison))
    check("preseason Puck Line retained as non-evaluation", len(puck) == 4 and puck.evaluation_status.eq("PRESEASON_NON_EVALUATION").all() and puck.standard_puck_line_result.isna().all() and puck.financial_result_label.eq("HYPOTHETICAL_NO_WAGER_PLACED").all(), len(puck))
    check("pregame capture immutable after grading", first_tree == tree_hashes(first), sha256(first / "SHA256SUMS"))

    # A regular-season target can use prior regular-season games, but never preseason.
    regular_target = pd.DataFrame([{
        "canonical_season": 2026, "slate_date": "2027-01-02", "game_id": 2026020600,
        "game_date": "2027-01-02", "scheduled_start_time_utc": "2027-01-03T00:00:00Z",
        "home_team_id": 10, "home_team": "MTL", "away_team_id": 20, "away_team": "OTT",
        "game_status": "FUT", "game_type_code": 2,
    }])
    prior_rows = [
        {"canonical_season": 2026, "slate_date": "2026-09-20", "game_id": 2026010002, "game_date": "2026-09-20", "scheduled_start_time_utc": "2026-09-20T23:00:00Z", "home_team_id": 10, "home_team": "MTL", "away_team_id": 20, "away_team": "OTT", "game_status": "FINAL", "game_type_code": 1, "final_home_goals": 8, "final_away_goals": 0, "final_home_shots": 50, "final_away_shots": 10},
        {"canonical_season": 2026, "slate_date": "2026-12-30", "game_id": 2026020590, "game_date": "2026-12-30", "scheduled_start_time_utc": "2026-12-31T00:00:00Z", "home_team_id": 10, "home_team": "MTL", "away_team_id": 20, "away_team": "OTT", "game_status": "FINAL", "game_type_code": 2, "final_home_goals": 3, "final_away_goals": 2, "final_home_shots": 31, "final_away_shots": 29},
    ]
    reg_a, reg_timing = build_v2_predictions(regular_target, pd.DataFrame(prior_rows), "2027-01-02T18:00:00Z")
    reg_b, _ = build_v2_predictions(regular_target, pd.DataFrame(prior_rows), "2027-01-02T18:00:00Z")
    check("regular feature state excludes preseason", int(reg_a.home_prior_games.iloc[0]) == 1 and int(reg_a.away_prior_games.iloc[0]) == 1 and reg_timing.preseason_history_rows.eq(0).all(), reg_a[["home_prior_games", "away_prior_games"]].to_json(orient="records"))
    check("January 2027 retains season 2026", reg_a.canonical_season.eq(2026).all(), reg_a.slate_date.iloc[0])
    check("deterministic replay", reg_a.substantive_prediction_sha256.equals(reg_b.substantive_prediction_sha256) and reg_a.v2_home_win_probability.equals(reg_b.v2_home_win_probability), reg_a.substantive_prediction_sha256.iloc[0])
    regular_schedule_path = work / "regular_schedule.csv"
    regular_history_path = work / "regular_history.csv"
    regular_target.to_csv(regular_schedule_path, index=False)
    pd.DataFrame(prior_rows).to_csv(regular_history_path, index=False)
    try:
        run_capture(
            regular_schedule_path, regular_history_path, paths["odds"], work / "regular_canary",
            "2027-01-02", "2027-01-02T18:00:00Z", "MIDDAY", canary_mode=True,
        )
    except RuntimeError as error:
        canary_scope_failure = str(error)
    else:
        canary_scope_failure = "NO_FAILURE"
    check("canary bypass limited to preseason window", canary_scope_failure == "PRESEASON_CANARY_SCOPE_VIOLATION", canary_scope_failure)

    missing_output = work / "missing_credential.json"
    try:
        fetch_markets("", missing_output)
    except RuntimeError as error:
        missing_failure = str(error)
    else:
        missing_failure = "NO_FAILURE"
    check("missing credential fails closed", missing_failure == "ODDS_API_CREDENTIAL_MISSING_FAIL_CLOSED" and not missing_output.exists(), missing_failure)
    mock_output = work / "mock_transport.json"
    sentinel = "NEVER_PERSIST_THIS_TEST_CREDENTIAL"
    with patch("backend.nhl.cross_market_shadow.core.urllib.request.urlopen", return_value=FakeResponse()):
        fetch_markets(sentinel, mock_output)
    mock_text = mock_output.read_text()
    mock_payload = json.loads(mock_text)
    check("credential redaction", sentinel not in mock_text and mock_payload["request_metadata"]["credential_persisted"] is False, "sentinel absent")
    check("quota headers and credits recorded", mock_payload["quota"]["credits_consumed"] == 4 and mock_payload["quota"]["requests_remaining"] == "996", mock_payload["quota"])
    bounded = quota_estimate()
    over = quota_estimate(("us", "us2", "us_ex", "eu"))
    check("quota request and credit bounds", bounded["within_request_bound"] and bounded["within_credit_bound"] and not over["within_credit_bound"], {"normal": bounded, "over": over})

    morning_root = work / "morning"
    command = [
        str(ROOT / ".venv/bin/python"), str(ROOT / "backend/nhl/scripts/run_nhl_morning_orchestration.py"),
        "--slate-date", SLATE, "--fixture-scenario", "shadow_failure", "--output-root", str(morning_root),
    ]
    failed_shadow = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
    health_path = Path(failed_shadow.stdout.strip().splitlines()[-1])
    health = json.loads(health_path.read_text())
    shadow_stage = next(row for row in health["stages"] if row["stage_id"] == "12_cross_market_shadow_status")
    check("scheduler shadow failure is WARN-only", failed_shadow.returncode == 0 and health["overall_status"] == "READY" and shadow_stage["state"] == "FAILED_WARN_ONLY", shadow_stage["state"])
    check("prop pipeline remains allowed", all(health["downstream"][key] for key in ["SOG_MORNING_PREREQUISITES_READY", "POINTS_MORNING_PREREQUISITES_READY", "SAVES_MORNING_PREREQUISITES_READY"]), health["downstream"])
    retry = subprocess.run(command[:-4] + ["--fixture-scenario", "valid_empty", "--output-root", str(morning_root)], cwd=ROOT, text=True, capture_output=True)
    check("scheduler lock released after every exit", retry.returncode == 0, retry.stderr.strip() or "second invocation exit 0")

    status = daily_status(run_root, SLATE)
    check("daily status command surface", status["graded_moneyline_record"] == "0-0" and status["final_games_awaiting_grading"] == 0 and status["graded_puck_line_market_observations"] == 0, status)

    failures = [row for row in checks if row["status"] != "PASS"]
    summary = {
        "checks": len(checks), "failures": len(failures),
        "synthetic_rehearsal_only": True, "real_preseason_canary": "PENDING_NO_ELIGIBLE_EVENT",
        "live_api_calls": 0, "live_api_credits_consumed": 0,
        "configured_odds_api_key_present": credential_configured(),
        "fixture_games": 1, "fixture_qualified_quotes": len(qualified),
        "fixture_books": int(qualified.sportsbook_key.nunique()),
        "fixture_grade_rows": {"moneyline": len(moneyline), "market_comparison": len(comparison), "puck_line": len(puck)},
        "daily_status_fixture": status,
    }
    if failures:
        raise RuntimeError(f"CROSS_MARKET_SHADOW_VALIDATION_FAILED:{failures}")
    return checks, summary


def contracts(staging: Path, summary: dict) -> None:
    write_json(staging / "shadow_contract.json", {
        "task": "NHL_2026_MAINLINE_CROSS_MARKET_PROSPECTIVE_SHADOW_V1",
        "mode": "SHADOW_RESEARCH_ONLY", "season": 2026,
        "season_convention": "2026 season includes January-April 2027; no multi-year label",
        "control": CONTROL_NAME, "v1_disposition": V1_DISPOSITION,
        "wagers": "PROHIBITED", "recommendations": "PROHIBITED",
        "uploads_and_published_picks": "UNCHANGED", "puck_line_prediction": "NOT_YET_DEVELOPED",
        "capture_phases": ["MIDDAY", "FINAL_PREGAME"],
    })
    write_json(staging / "v2_runtime_binding.json", {
        "control_name": CONTROL_NAME, "feature_order": FEATURES, "refit": False,
        "source_parent": str(PARENT.relative_to(ROOT)),
        "source_parent_manifest_sha256": PARENT_MANIFEST_SHA256,
        "source_control_sha256": sha256(PARENT / "v2_fitted_control.json"),
        "runtime_control": str(PARAMETER_PATH.relative_to(ROOT)),
        "runtime_control_sha256": sha256(PARAMETER_PATH), "semantic_equality": True,
        "chronology": "scheduled_start_time_utc strict prior",
        "rest": "idle dates between team games; days_rest=max(date_delta-1,0)",
        "back_to_back": "calendar date_delta equals 1",
        "regular_history_game_types": [2], "postseason_history_game_types": [2, 3],
        "preseason_history_game_types": [],
    })
    write_json(staging / "source_market_contract.json", {
        "provider": "The Odds API", "sport_key": "icehockey_nhl",
        "markets": {"h2h": "full-game winner including overtime/shootout", "spreads": "standard home -1.5 / away +1.5 only"},
        "regions": ["us", "us2", "us_ex"], "book_policy": "retain all returned books; flag BetOnline only when provider key is betonlineag",
        "binding": "exact normalized home/away plus commence time within 15 minutes; ambiguity retained and unqualified",
        "pregame": "source update and observation timestamps both strictly before scheduled start",
        "labels": {"FIRST_SEEN": "earliest observed qualified price; not opening", "LATEST_PRESTART": "latest observed qualified price before start; not close"},
        "raw": "immutable provider envelope", "normalized": "content-identified append-only observations",
        "configured_credential_present": summary["configured_odds_api_key_present"],
        "credential_value_persisted": False, "live_calls_during_activation": 0,
    })
    write_json(staging / "ledger_schemas.json", {
        "grain_and_files": {
            "schedule_event_identity": ["canonical_season", "game_id"],
            "v2_immutable_predictions": ["canonical_season", "game_id", "substantive_prediction_sha256"],
            "raw_market_observations": ["provider_event_id", "sportsbook_key", "provider_market_key", "provider_last_update_utc"],
            "normalized_market_observations": ["observation_identity_sha256"],
            "cross_market_game_state_observations": ["canonical_season", "game_id", "observation_timestamp_utc"],
            "canonical_outcomes": ["canonical_season", "game_id"],
            "graded_moneyline_shadow_results": ["canonical_season", "game_id"],
            "graded_puck_line_market_results": ["canonical_season", "game_id", "sportsbook_key", "side_orientation", "point"],
            "daily_execution_status": ["slate_date", "run_type", "substantive_state_sha256"],
            "coverage_history": ["canonical_season", "game_id", "observation_timestamp_utc"],
        },
        "persistence": "content-addressed create-only states; identical substantive state resolves to prior manifest-complete path",
        "metadata_excluded_from_state": ["filesystem timestamps", "prediction creation time", "report generation metadata"],
    })
    write_json(staging / "grading_contract.json", {
        "outcome": "certified final score; full-game including overtime/shootout",
        "moneyline": ["predicted side", "actual winner", "correct", "probability assigned to outcome", "Brier contribution", "log-loss contribution"],
        "market_comparison": ["two-sided no-vig probability", "V2-minus-market probability gap", "FIRST_SEEN", "LATEST_PRESTART"],
        "puck_line": ["home -1.5 result", "away +1.5 result", "captured price", "hypothetical one-unit result"],
        "puck_line_pushes": False, "financial_label": "HYPOTHETICAL_NO_WAGER_PLACED",
        "unsupported": ["puck-line probability", "puck-line edge", "puck-line EV", "puck-line side selection"],
    })
    write_json(staging / "quota_policy.json", {
        "maximum_http_requests_per_capture": 1, "markets_per_request": 2,
        "regions": 3, "estimated_credit_upper_bound": 6,
        "response_headers": ["x-requests-last", "x-requests-remaining", "x-requests-used"],
        "missing_credential": "FAIL_CLOSED", "unbounded_polling": "PROHIBITED",
        "activation_live_calls": 0, "activation_live_credits_consumed": 0,
    })
    write_json(staging / "preseason_isolation_policy.json", {
        "preseason_start": "2026-09-19", "regular_season_start": "2026-09-29",
        "preseason_use": "OPERATIONAL_REHEARSAL_ONLY", "preseason_label": "PRESEASON_REHEARSAL_EXCLUDED",
        "excluded_from": ["regular feature state", "regular performance", "training", "promotion", "ROI", "edge claims"],
        "canary_status_as_of_2026_09_15": "PENDING_NO_ELIGIBLE_EVENT",
    })
    write_json(staging / "automation_binding.json", {
        "scheduler": "com.proppadia.nhl.morning-orchestration", "cadence": "07:30 America/Los_Angeles",
        "scheduler_reused": True, "duplicate_scheduler_created": False,
        "independent_stage": "12_cross_market_shadow_status", "failure_semantics": "WARN_ONLY",
        "runtime_hook": "backend/nhl/scripts/run_nhl_cross_market_shadow_warn_only.py",
        "capture_enabled": False, "reason": "successful real preseason canary required",
        "midday_final_pregame": "manual create-only commands until real preseason canary and explicit activation flip",
        "lock": "per-slate nonblocking fcntl lock released by context manager/process exit",
        "retry_policy": "no new retry loop; one request maximum per capture",
        "prop_artifacts": "never written by cross-market shadow",
    })


def build_package(output: Path) -> dict:
    verify_manifest(PARENT, PARENT_MANIFEST_SHA256)
    staging = begin_package(output)
    with tempfile.TemporaryDirectory(prefix="nhl_cross_market_shadow_v1_") as raw:
        checks, summary = run_validation(Path(raw))
    write_csv(staging / "validation_summary.csv", checks)
    write_json(staging / "dry_run_canary_results.json", summary)
    write_csv(staging / "dry_run_canary_results.csv", [{
        "evidence_type": "SYNTHETIC_LOCAL_REHEARSAL", "status": "PASSED",
        "games": summary["fixture_games"], "qualified_quotes": summary["fixture_qualified_quotes"],
        "books": summary["fixture_books"], "network_calls": 0, "credits_consumed": 0,
        "promotion_evidence": "NO",
    }, {
        "evidence_type": "REAL_PRESEASON_CANARY", "status": "PENDING_NO_ELIGIBLE_EVENT",
        "games": 0, "qualified_quotes": 0, "books": 0, "network_calls": 0,
        "credits_consumed": 0, "promotion_evidence": "NO",
    }])
    contracts(staging, summary)
    decisions = {
        "NHL_MONEYLINE_V2_SHADOW_BINDING": "READY_NOT_ACTIVATED",
        "NHL_PRESEASON_CANARY": "PENDING_NO_ELIGIBLE_EVENT",
        "NHL_MONEYLINE_MARKET_CAPTURE": "READY",
        "NHL_PUCK_LINE_MARKET_CAPTURE": "READY",
        "NHL_PUCK_LINE_PREDICTION": "NOT_YET_DEVELOPED",
        "NHL_CROSS_MARKET_PROSPECTIVE_LEDGER": "READY",
        "NHL_PLAYER_PROP_OPERATIONAL_EFFECT": "UNCHANGED",
        "NHL_2026_REGULAR_SEASON_EVALUATION_START": "2026-09-29",
        "NHL_NEXT_STEP": "COMPLETE_PRESEASON_OPERATIONAL_REHEARSAL",
    }
    write_json(staging / "decision.json", decisions)
    report = f"""# NHL 2026 V2 cross-market prospective shadow activation

The V2 runtime and cross-market ledger are implemented and passed {summary['checks']} local checks with zero failures. The existing 07:30 NHL scheduler now contains an independent WARN-only status stage; its simulated failure left all player-prop readiness flags allowed and the next invocation acquired the released lock. No competing scheduler was created.

Binding remains `READY_NOT_ACTIVATED`. On {DATE}, the first eligible preseason event is still future-dated ({SLATE}), so the required real canary is `PENDING_NO_ELIGIBLE_EVENT`; capture stays disabled. The local rehearsal was synthetic plumbing evidence only and is excluded from performance, ROI, edge, or promotion claims. No live Odds API call was made and no credit was consumed. Repository configuration contains an existing credential: `{summary['configured_odds_api_key_present']}`; its value was never printed or persisted.

The runtime preserves V2 raw features, imputation/scaling state, probabilities and hashes; captures `h2h` plus only standard ±1.5 spreads; keeps raw and normalized content-identified states; derives `FIRST_SEEN` and `LATEST_PRESTART`; grades certified full-game winners and hypothetical puck-line results; and reports SOG/Points/Saves coverage without treating a listed goalie as confirmed. V1 is `{V1_DISPOSITION}` and cannot be a fallback. There is no puck-line model, wager selection, upload, or pick-publication path.

## Decisions

""" + "\n".join(f"- `{key}` = `{value}`" for key, value in decisions.items()) + "\n"
    (staging / "report.md").write_text(report)
    commands = """# Operator commands

Status (safe while capture is gated):

```bash
.venv/bin/python -m backend.nhl.cross_market_shadow.cli status --slate-date YYYY-MM-DD
```

After an eligible preseason slate exists, use one new raw path per phase, then run the canary explicitly. The API key is read from the environment and must never be placed on the command line:

```bash
.venv/bin/python -m backend.nhl.cross_market_shadow.cli fetch-markets --output /absolute/create-only/raw.json
.venv/bin/python -m backend.nhl.cross_market_shadow.cli run --schedule-csv /absolute/schedule.csv --history-csv /absolute/history.csv --odds-json /absolute/create-only/raw.json --output-root artifacts/operational/nhl/cross_market_shadow --slate-date YYYY-MM-DD --run-timestamp-utc RFC3339_UTC --run-type MIDDAY --preseason-canary
```

Repeat with a distinct raw path and `FINAL_PREGAME`. Review all timing, binding, market, coverage, quota, and grading evidence before changing `capture_enabled`; the synthetic rehearsal does not authorize that change.
"""
    (staging / "operator_commands.md").write_text(commands)
    write_json(staging / "package_identity.json", {
        "task": "NHL_2026_MAINLINE_CROSS_MARKET_PROSPECTIVE_SHADOW_V1", "as_of_date": DATE,
        "parent_manifest_sha256": PARENT_MANIFEST_SHA256,
        "activation_state": "READY_NOT_ACTIVATED", "artifact_count_excluding_manifest": len(list(staging.iterdir())) + 1,
        "source_utility": str(Path(__file__).relative_to(ROOT)),
        "source_utility_sha256": sha256(Path(__file__)),
    })
    files = sorted(item for item in staging.iterdir() if item.is_file() and item.name != "SHA256SUMS")
    (staging / "SHA256SUMS").write_text("".join(f"{sha256(item)}  {item.name}\n" for item in files))
    finalize_package(staging, output)
    return {"output": str(output), "manifest_sha256": sha256(output / "SHA256SUMS"), "decisions": decisions, "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(build_package(args.output_dir.resolve()), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
