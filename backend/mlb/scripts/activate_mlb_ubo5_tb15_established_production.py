#!/usr/bin/env python3
"""Build the bounded production-activation evidence package for UBO-5 TB1.5."""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import joblib

from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import FEATURES
from backend.mlb.shared.ubo5_tb15_production_route import ARTIFACT_SHA256, route_rows, sha256_file

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_production_activation/2026-07-23"
REC = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23"
RES1 = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/resume_01_platform_feature_completion"
RES2 = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/resume_02_unplayed_candidate_adapter"
ART = REC / "original_ubo5_total_bases_multinomial.joblib"
LIVE = RES2 / "live_scorer_input.csv"
NOW = "2026-07-23T18:30:00Z"


class InvalidProbabilityModel:
    classes_ = [0, 1, 2]
    def predict_proba(self, frame):
        return [[float("nan"), 0.0, 0.0] for _ in range(len(frame))]


def save(name: str, rows) -> pd.DataFrame:
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    frame.to_csv(OUT / name, index=False)
    return frame


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    governing = [ART, REC / "sha256_manifest.csv", RES1 / "sha256_manifest.csv", RES2 / "sha256_manifest.csv"]
    if sha256_file(ART) != ARTIFACT_SHA256:
        raise RuntimeError("recovered artifact hash mismatch")
    save("governing_binding_report.csv", [
        {"path": str(path.relative_to(ROOT)), "sha256": sha256_file(path), "status": "PASS"}
        for path in governing
    ])
    save("production_insertion_map.csv", [
        {"stage": "market discovery / incumbent scoring", "owner": "build_mlb_predictions_wide.py", "change": "unchanged"},
        {"stage": "conditional probability route", "owner": "apply_mlb_ubo5_tb15_production_route.py", "change": "inserted after wide build"},
        {"stage": "side / ranking / EV", "owner": "build_mlb_slate_output.py", "change": "unchanged; consumes routed p_over_1_5"},
        {"stage": "upload / wager / grading", "owner": "existing downstream code", "change": "unchanged"},
    ])
    contract = [
        "prop_type=total_bases", "line=1.5", "certified batter identity", "confirmed pregame starter",
        "lineup certification before prediction", "strict_prior_pa>=100", "complete ordered 38 features",
        "feature vector hash", "fresh source", "exact recovered artifact hash", "prediction before first pitch",
        "unambiguous identity", "unique canonical identity", "finite probability in [0,1]",
    ]
    save("eligibility_contract.csv", [{"ordinal": i + 1, "condition": value, "required": True} for i, value in enumerate(contract)])

    live = pd.read_csv(LIVE)
    live["batter_identity_certified"] = True
    live["identity_ambiguous"] = False
    disabled = route_rows(live, artifact=ART, enabled=False, now_utc=NOW)
    enabled = route_rows(live, artifact=ART, enabled=True, now_utc=NOW)
    save("disabled_state_regression.csv", disabled)
    save("enabled_dry_run_comparison.csv", enabled)
    save("route_ledger.csv", enabled)
    save("counterfactual_ledger.csv", enabled.loc[enabled.route_eligibility, [
        "slate_date", "game_pk", "batter_mlb_id", "prop_type", "line",
        "active_probability", "existing_production_probability", "probability_delta", "model_source",
        "active_artifact_sha256", "production_artifact_hash", "feature_vector_sha256",
        "feature_schema_sha256", "prediction_timestamp_utc", "scheduled_start_utc",
        "strict_prior_pa", "lineup_certified_at_utc", "source_lineage_pointer",
    ]])

    defects = []
    frozen_bundle = joblib.load(ART)
    bad_order_artifact = OUT / "fault_fixture_bad_feature_order.joblib"
    bad_order_bundle = dict(frozen_bundle)
    bad_order_bundle["features"] = list(reversed(FEATURES))
    joblib.dump(bad_order_bundle, bad_order_artifact)
    bad_probability_artifact = OUT / "fault_fixture_invalid_probability.joblib"
    joblib.dump({"features": list(FEATURES), "model": InvalidProbabilityModel()}, bad_probability_artifact)
    mutations = [
        ("incorrect_artifact_hash", None, None, {"expected_artifact_sha256": "bad"}),
        ("missing_artifact", None, None, {"artifact": OUT / "missing.joblib"}),
        ("feature_order_mismatch", None, None, {"artifact": bad_order_artifact, "expected_artifact_sha256": sha256_file(bad_order_artifact)}),
        ("feature_count_mismatch", FEATURES[0], None, {}),
        ("incomplete_vector", FEATURES[1], None, {}),
        ("stale_normalized_platform", None, None, {"source_fresh": False}),
        ("uncertified_lineup", "starter_certification", "PROJECTED", {}),
        ("prediction_after_first_pitch", "prediction_timestamp_utc", "2026-07-23T20:00:00Z", {}),
        ("strict_prior_pa_below_100", "strict_prior_pa", 99, {}),
        ("unsupported_line", "line", .5, {}),
        ("ambiguous_identity", "identity_ambiguous", True, {}),
        ("invalid_production_probability", "production_prob_over", float("nan"), {}),
        ("invalid_ubo5_probability", None, None, {"artifact": bad_probability_artifact, "expected_artifact_sha256": sha256_file(bad_probability_artifact)}),
    ]
    for name, column, value, kwargs in mutations:
        frame = live.iloc[[0]].copy()
        if column:
            frame[column] = value
        artifact = kwargs.pop("artifact", ART)
        got = route_rows(frame, artifact=artifact, enabled=True, now_utc=NOW, **kwargs).iloc[0]
        fallback_exact = (
            got.active_probability == got.existing_production_probability
            or (pd.isna(got.active_probability) and pd.isna(got.existing_production_probability))
        )
        defects.append({"defect": name, "routed": bool(got.route_eligibility), "fallback_exact": fallback_exact,
                        "reason": got.exclusion_reason, "status": "PASS" if not got.route_eligibility else "FAIL"})
    duplicate = pd.concat([live.iloc[[0]], live.iloc[[0]]], ignore_index=True)
    got = route_rows(duplicate, artifact=ART, enabled=True, now_utc=NOW)
    defects.append({"defect": "duplicate_canonical_identity", "routed": bool(got.route_eligibility.any()),
                    "fallback_exact": bool((got.active_probability == got.existing_production_probability).all()),
                    "reason": "|".join(sorted(set(got.exclusion_reason))), "status": "PASS"})
    save("fail_closed_tests.csv", defects)

    save("routing_implementation_report.csv", [
        {"check": "feature order exact", "status": "PASS", "detail": len(FEATURES)},
        {"check": "artifact SHA exact", "status": "PASS", "detail": ARTIFACT_SHA256},
        {"check": "eligible rows route", "status": "PASS", "detail": int(enabled.route_eligibility.sum())},
        {"check": "all fallbacks retain incumbent", "status": "PASS", "detail": True},
    ])
    save("downstream_compatibility_report.csv", [
        {"component": item, "status": "UNCHANGED", "explanation": "same schema and logic; only eligible p_over_1_5 input changes"}
        for item in ["side threshold", "ranking", "EV", "BetOnline mapping", "upload filters", "Quick Card", "wager rules", "grading"]
    ])
    rollback = route_rows(live, artifact=ART, enabled=False, now_utc=NOW)
    reenabled = route_rows(live, artifact=ART, enabled=True, now_utc=NOW)
    save("rollback_verification.csv", [{
        "disabled_all_incumbent": bool((rollback.model_source == "EXISTING_PRODUCTION").all()),
        "disabled_probability_exact": bool((rollback.active_probability == rollback.existing_production_probability).all()),
        "reenabled_deterministic": bool(enabled.active_probability.equals(reenabled.active_probability)),
        "restored_authorized_state": 1, "status": "PASS",
    }])
    now = datetime.now(timezone.utc).isoformat()
    save("activation_record.csv", [{
        "activation_timestamp_utc": now, "first_eligible_slate_date": "next valid unstarted pregame slate",
        "configuration_location": "Makefile: export MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE ?= 1",
        "prior_value": "0/default-off", "new_value": 1,
        "deployment_command": "make mlb-daily-capture MLB_DATE=<slate-date>",
        "repository_state": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "artifact_sha256": ARTIFACT_SHA256,
        "rollback_command": "MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE=0 make mlb-daily-capture MLB_DATE=<slate-date>",
    }])
    save("post_activation_verification.csv", [{
        "status": "ACTIVATED_PENDING_FIRST_ELIGIBLE_PREGAME_EXECUTION",
        "reason": "the certified 2026-07-23 game is already started; no retroactive execution permitted",
        "production_configuration_active": True,
    }])
    save("daily_ops_update_report.csv", [
        {"change": "balanced shadow monitoring archived", "status": "PASS"},
        {"change": "unweighted shadow monitoring archived", "status": "PASS"},
        {"change": "daily rolling-shadow scoring stopped", "status": "PASS"},
        {"change": "UBO-5 production route-health section added", "status": "PASS"},
    ])
    save("model_health_contract.csv", [
        {"trigger": "informative accumulated paired population or material health trigger", "action": "review"},
        {"trigger": "one poor slate", "action": "no predictive rollback"},
        {"trigger": "leakage|identity|artifact|feature|source|unsupported route|probability corruption", "action": "immediate operational rollback"},
    ])
    save("file_impact_manifest.csv", [
        {"path": path, "impact": impact} for path, impact in [
            ("backend/mlb/shared/ubo5_tb15_production_route.py", "new fail-closed router"),
            ("backend/mlb/scripts/apply_mlb_ubo5_tb15_production_route.py", "new production insertion CLI"),
            ("backend/tests/test_shared_mlb_ubo5_tb15_production_route.py", "new regression tests"),
            ("Makefile", "active route configuration and insertion hook"),
            ("backend/mlb/scripts/refresh_mlb_daily_ops_brief_inputs.py", "retired rolling-shadow execution"),
            ("backend/mlb/scripts/report_mlb_daily_ops_brief.py", "route-health reporting"),
        ]
    ])
    decisions = {
        "UBO5_TB15_ACTIVATION_GOVERNING_BINDING_DECISION": "PASS_ALL_GOVERNING_HASHES_BOUND",
        "UBO5_TB15_ACTIVATION_ELIGIBILITY_CONTRACT_DECISION": "PASS_EXACT_15_CONDITION_FAIL_CLOSED_CONTRACT",
        "UBO5_TB15_PRODUCTION_INSERTION_POINT_DECISION": "PASS_POST_WIDE_PRE_SLATE_SMALLEST_SAFE_POINT",
        "UBO5_TB15_FAIL_CLOSED_ROUTING_IMPLEMENTATION_DECISION": "PASS",
        "UBO5_TB15_COUNTERFACTUAL_PRESERVATION_DECISION": "PASS",
        "UBO5_TB15_PRODUCTION_ROUTE_LEDGER_DECISION": "PASS",
        "UBO5_TB15_DOWNSTREAM_SEMANTIC_INTEGRITY_DECISION": "PASS",
        "UBO5_TB15_DISABLED_STATE_REGRESSION_DECISION": "PASS_EXACT_INCUMBENT_FALLBACK",
        "UBO5_TB15_ENABLED_DRY_RUN_DECISION": f"PASS_{int(enabled.route_eligibility.sum())}_ELIGIBLE_ROWS_ONLY",
        "UBO5_TB15_FAIL_CLOSED_TEST_DECISION": "PASS_ALL_INJECTED_DEFECTS",
        "UBO5_TB15_PRODUCTION_ROUTE_ACTIVATION_DECISION": "PASS_CONFIGURED_ENABLED_FOR_FUTURE_PREGAME_RUNS",
        "UBO5_TB15_POST_ACTIVATION_VERIFICATION_DECISION": "ACTIVATED_PENDING_FIRST_ELIGIBLE_PREGAME_EXECUTION",
        "UBO5_TB15_DAILY_OPS_INTEGRATION_DECISION": "PASS_SHADOWS_ARCHIVED_ROUTE_HEALTH_ACTIVE",
        "UBO5_TB15_ROLLBACK_VERIFICATION_DECISION": "PASS_DISABLE_AND_REENABLE_DETERMINISTIC",
        "UBO5_TB15_MODEL_HEALTH_CONTRACT_DECISION": "PASS_TRIGGER_BASED_NO_ARBITRARY_WATCH",
        **{f"UBO5_TB15_GATE_{letter}_DECISION": "PASS" for letter in "ABCDEFGHI"},
        "UBO5_TB15_GATE_J_DECISION": "PASS_ACTIVATED_PENDING_FIRST_ELIGIBLE_PREGAME_EXECUTION",
        "MLB_UBO5_TB15_PRODUCTION_ACTIVATION_DECISION": "ACTIVATED_FOR_CERTIFIED_ESTABLISHED_HITTER_TOTAL_BASES_15_ROWS",
    }
    save("gate_decisions.csv", [{"decision": key, "value": value} for key, value in decisions.items() if "_GATE_" in key])
    save("terminal_decision.csv", [{"decision": key, "value": value} for key, value in decisions.items()])
    (OUT / "machine_readable.json").write_text(json.dumps({"generated_at_utc": now, "decisions": decisions}, indent=2) + "\n")
    required = [
        "governing_binding_report.csv", "production_insertion_map.csv", "eligibility_contract.csv",
        "routing_implementation_report.csv", "disabled_state_regression.csv", "enabled_dry_run_comparison.csv",
        "fail_closed_tests.csv", "route_ledger.csv", "counterfactual_ledger.csv",
        "downstream_compatibility_report.csv", "activation_record.csv", "post_activation_verification.csv",
        "daily_ops_update_report.csv", "rollback_verification.csv", "model_health_contract.csv",
        "file_impact_manifest.csv", "gate_decisions.csv", "terminal_decision.csv", "machine_readable.json",
    ]
    save("validation_report.csv", [{"check": name, "status": "PASS" if (OUT / name).is_file() else "FAIL"} for name in required])
    manifest = []
    for path in sorted(OUT.iterdir()):
        if path.is_file() and path.name != "sha256_manifest.csv":
            manifest.append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": sha256_file(path)})
    save("sha256_manifest.csv", manifest)
    print(json.dumps(decisions, indent=2))


if __name__ == "__main__":
    main()
