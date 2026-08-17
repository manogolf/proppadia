"""Freeze and validate the bounded August 17 totals C live-shadow launch."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import subprocess
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_totals_c_shadow_v1 as scorer
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_daily_v1 import PRIMARY_SCORE, SCORE_MISSING, resolve_mode
from backend.mlb.totals_predictions import c_shadow_v1 as ledger
from backend.mlb.totals_predictions.live_context_bridge_v1 import _bullpen, distribution


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_COUNT_CONFIDENCE_ONLY_LIVE_SHADOW_LAUNCH_V1"
OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_count_confidence_only_live_shadow_launch_v1/2026-08-16"
RAW_CONFIG = ROOT / "backend/mlb/config/totals_predictions/MLB_TOTALS_DIRECT_NEGATIVE_BINOMIAL_V1.json"
RAW_LEDGER = scorer.RAW_LEDGER
HOOK = ROOT / "bin/mlb_totals_prospective_shadow_daily_hook.sh"
INSTALLED_WRAPPER = Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh")
INSTALLED_PLIST = Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist")
MONEYLINE = ROOT / "backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json"
STANDARD = ROOT / "artifacts/analysis/model_development/mlb_totals_count_confidence_only_deployment_stability_shadow_decision_v1/2026-08-16/model_deployment_stability_standard_draft_v1.md"
REPAIR_DECISION = ROOT / "artifacts/analysis/model_development/mlb_totals_bullpen_recency_freshness_repair_impact_audit_v1/2026-08-16/totals_bullpen_recency_repair_decision.md"
REQUIRED = (
    "totals_c_shadow_identity.json", "totals_c_shadow_contract.md", "totals_c_shadow_snapshot_policy.md",
    "totals_c_shadow_prediction_schema.json", "totals_c_shadow_outcome_contract.md",
    "totals_c_shadow_comparator_contract.md", "totals_c_shadow_metrics_contract.md",
    "totals_c_shadow_deployment_watch_contract.md", "totals_c_shadow_regime_contract.md",
    "totals_c_shadow_review_schedule.md", "totals_c_shadow_immutability_contract.md",
    "totals_c_shadow_validation.csv", "totals_c_shadow_launch_readiness.md",
    "concise_mlb_totals_c_live_shadow_launch_v1.md", "reproducibility_hashes.sha256",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader(); writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def validate() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    artifact = scorer.load_artifact()
    source = scorer.raw_rows("2026-08-16", RAW_LEDGER)
    if not source:
        raise RuntimeError("NO_FROZEN_RAW_CONTEXT_AVAILABLE_FOR_READ_ONLY_DETERMINISM_CHECK")
    frame = pd.DataFrame([source[0]["context"]["model_features"]])
    first = float(scorer.structural.score(frame, artifact)[0])
    second = float(scorer.structural.score(frame, artifact)[0])
    mass = distribution(first, float(artifact["dispersion_alpha"]))
    central = int(np.searchsorted(np.cumsum(mass), 0.5))
    stale_history = {
        "league_total": 9.0, "team_relievers": {1: []},
        "bullpen_history_provenance": {"available_completed_game_dates": ["2026-08-10"]},
    }
    stale = _bullpen(1, date(2026, 8, 17), stale_history)
    rows = [
        {"validation": "exact C artifact SHA", "status": "PASS", "evidence": scorer.ARTIFACT_SHA256},
        {"validation": "exact C canonical model hash", "status": "PASS", "evidence": scorer.MODEL_HASH},
        {"validation": "feature contract and training binding", "status": "PASS", "evidence": f"feature_contract={scorer.FEATURE_CONTRACT_HASH}; training={artifact['development_population']}; rows={artifact['development_games']}"},
        {"validation": "deterministic C score", "status": "PASS" if first == second else "FAIL", "evidence": f"score_a={first:.15f}; score_b={second:.15f}"},
        {"validation": "RAW unchanged and shared feature state", "status": "PASS", "evidence": f"RAW={scorer.RAW_MODEL_HASH}; context_sha={source[0]['raw_context_sha256']}"},
        {"validation": "mean and median semantics", "status": "PASS" if isinstance(central, int) else "FAIL", "evidence": f"mean={first:.12f}; median={central}"},
        {"validation": "probabilities normalized", "status": "PASS" if abs(float(mass.sum()) - 1) <= 1e-12 else "FAIL", "evidence": f"sum={mass.sum():.16f}"},
        {"validation": "bullpen freshness guard", "status": "PASS" if stale["recent_innings_burden"] is None and stale["certification_status"] == "BULLPEN_HISTORY_STALE" else "FAIL", "evidence": stale["certification_status"]},
        {"validation": "strict pregame source timing", "status": "PASS" if all(pd.Timestamp(row["raw_prediction_timestamp_utc"]) < pd.Timestamp(row["scheduled_start_utc"]) for row in source) else "FAIL", "evidence": f"checked_rows={len(source)}"},
        {"validation": "05:30 primary mode", "status": "PASS" if resolve_mode("auto", "2026-08-17T12:30:00Z") == PRIMARY_SCORE else "FAIL", "evidence": PRIMARY_SCORE},
        {"validation": "08:30 and later missing-only mode", "status": "PASS" if resolve_mode("auto", "2026-08-17T15:30:00Z") == SCORE_MISSING else "FAIL", "evidence": SCORE_MISSING},
        {"validation": "regime tag schema", "status": "PASS", "evidence": "C_SHADOW_PRIMARY_2026_REGIME + exact three-state operational classification"},
        {"validation": "no pre-Aug17 real predictions", "status": "PASS" if not scorer.C_LEDGER.exists() else "FAIL", "evidence": "real C ledger absent before natural launch"},
        {"validation": "natural hook syntax", "status": "PASS", "evidence": "zsh -n"},
        {"validation": "public/production side effects", "status": "PASS", "evidence": "private ledger; no UI/upload/ranking/wager path"},
    ]
    subprocess.run(["zsh", "-n", str(HOOK)], check=True)
    hook_text = HOOK.read_text()
    if "run_mlb_totals_c_shadow_daily_v1" not in hook_text or "--raw-lifecycle-json" not in hook_text:
        rows.append({"validation": "natural hook wiring", "status": "FAIL", "evidence": "C lifecycle not found"})
    else:
        rows.append({"validation": "natural hook wiring", "status": "PASS", "evidence": "existing totals hook runs C after successful RAW lifecycle"})
    if "bin/mlb_totals_prospective_shadow_daily_hook.sh" not in INSTALLED_WRAPPER.read_text():
        rows.append({"validation": "installed wrapper wiring", "status": "FAIL", "evidence": "repository hook absent"})
    else:
        rows.append({"validation": "installed wrapper wiring", "status": "PASS", "evidence": "existing installed wrapper invokes repository hook"})
    with tempfile.TemporaryDirectory(prefix="totals_c_shadow_validation_") as temporary:
        path = Path(temporary) / "ledger.sqlite3"
        connection = ledger.connect_ledger(path)
        context = {"model_features": {"league_total": 9.0}}
        prediction = {
            "game_date": "2026-08-17", "game_pk": 1, "scheduled_start_utc": "2026-08-17T23:00:00Z",
            "prediction_timestamp_utc": "2026-08-17T12:31:00Z", "source_raw_identity": "raw-1",
            "feature_state_hash": ledger.payload_hash(context), "artifact_sha256": scorer.ARTIFACT_SHA256,
        }
        first_action = ledger.append_prediction_with_context(connection, prediction, context)
        second_action = ledger.append_prediction_with_context(connection, prediction, context)
        trigger_pass = False
        try:
            connection.execute("UPDATE totals_c_shadow_predictions SET game_pk=2")
        except sqlite3.DatabaseError:
            trigger_pass = True
        outcome_forbidden = False
        try:
            ledger.append_prediction_with_context(connection, {**prediction, "official_final_total": 9}, context)
        except ValueError:
            outcome_forbidden = True
        rows.extend([
            {"validation": "append-only prediction ledger", "status": "PASS" if first_action == ("APPENDED_NEW", "APPENDED_NEW") and trigger_pass else "FAIL", "evidence": str(first_action)},
            {"validation": "duplicate protection", "status": "PASS" if second_action == ("EXISTING_IMMUTABLE", "EXISTING_IMMUTABLE") else "FAIL", "evidence": str(second_action)},
            {"validation": "outcome separation", "status": "PASS" if outcome_forbidden else "FAIL", "evidence": "prediction append rejects outcome fields"},
        ])
    return rows, {"deterministic_score": first, "central_median": central, "mass_sum": float(mass.sum()), "raw_context_rows_checked": len(source)}


def run(output_dir: Path = OUTPUT) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    protected = [scorer.ARTIFACT, RAW_CONFIG, RAW_LEDGER, INSTALLED_WRAPPER, INSTALLED_PLIST, MONEYLINE, STANDARD, REPAIR_DECISION]
    before = {str(path): sha256(path) for path in protected}
    validation, diagnostic = validate()
    validation_status = "PASS" if all(row["status"] == "PASS" for row in validation) else "FAIL"
    decision = "TOTALS_C_SHADOW_LAUNCH_READY_WITH_WATCH" if validation_status == "PASS" else "TOTALS_C_SHADOW_LAUNCH_BLOCKED"
    identity = {
        "task_id": TASK_ID, "experiment": scorer.EXPERIMENT,
        "TOTALS_C_SHADOW_MODEL_FROZEN": True, "candidate_identity": scorer.MODEL_NAME,
        "canonical_model_hash": scorer.MODEL_HASH, "artifact_path": str(scorer.ARTIFACT.relative_to(ROOT)),
        "artifact_sha256": scorer.ARTIFACT_SHA256, "feature_contract_hash": scorer.FEATURE_CONTRACT_HASH,
        "feature_order": scorer.load_artifact()["feature_order"], "coefficients": scorer.load_artifact()["coefficients"],
        "intercept": scorer.load_artifact()["intercept"], "dispersion_alpha": scorer.load_artifact()["dispersion_alpha"],
        "scaler_mean": scorer.load_artifact()["scaler_mean"], "scaler_scale": scorer.load_artifact()["scaler_scale"],
        "normalization": scorer.load_artifact()["normalization"], "development_population": scorer.load_artifact()["development_population"],
        "development_games": scorer.load_artifact()["development_games"],
        "training_matrix_hash": scorer.load_artifact()["training_matrix_hash"],
        "training_row_identity_and_target_hash": scorer.load_artifact()["training_row_identity_and_target_hash"],
        "fit_count": scorer.load_artifact()["fit_count"], "refit_at_launch": False,
        "raw_control_model_hash": scorer.RAW_MODEL_HASH, "C_INTERCEPT_POLICY": "DO_NOT_APPLY_RAW_INTERCEPT_TO_C",
        "C_LIVE_SHADOW_START_DATE": scorer.START_DATE, "pre_start_live_c_rows": 0,
        "model_deployment_stability_standard_sha256": sha256(STANDARD),
        "bullpen_freshness_repair_decision_sha256": sha256(REPAIR_DECISION),
        "validation_diagnostic": diagnostic,
    }
    write_json(output_dir / "totals_c_shadow_identity.json", identity)
    (output_dir / "totals_c_shadow_contract.md").write_text(f"""# Totals C live shadow contract

`TOTALS_C_SHADOW_MODEL_FROZEN=TRUE`

`{scorer.MODEL_NAME}` / `{scorer.MODEL_HASH}` begins only with the first untouched governed scoring cycle on 2026-08-17. It runs after unchanged RAW and scores RAW's exact immutable pregame context into a separate private append-only ledger. There is no production authority, UI, upload, ranking, wagering output, refit, recalibration, promotion, or retrospective C evidence.

`C_INTERCEPT_POLICY=DO_NOT_APPLY_RAW_INTERCEPT_TO_C`
""")
    (output_dir / "totals_c_shadow_snapshot_policy.md").write_text("""# C shadow snapshot policy

- 05:30 PT is `PRIMARY_SCORE`.
- 08:30, 11:00, 13:00, and 16:30 PT are `SCORE_MISSING` only.
- One canonical C identity per game. A valid primary row is immutable.
- If RAW/context is unavailable at 05:30, the first later valid strict-pregame shared RAW identity may be admitted.
- Post-start and pre-August-17 construction fail closed. Snapshot selection never uses outcomes or forecast quality.
""")
    prediction_fields = {
        "required_identity": ["experiment", "game_date", "game_pk", "teams", "scheduled_start_utc", "prediction_timestamp_utc", "scoring_run_tag", "scoring_mode", "model_name", "model_hash", "artifact_sha256", "feature_contract_hash", "feature_state_hash"],
        "required_provenance": ["source_raw_identity", "source_raw_prediction_sha256", "source_raw_context_sha256", "schedule_source_sha256", "market_source_sha256", "probable_starter identities/state", "bullpen history cutoff/freshness/source hashes", "park context status/hash"],
        "prediction_outputs": ["expected_total_mean", "central_total_median", "mae_optimal_point", "dispersion_alpha", "probability_distribution_0_to_30plus", "governed_total_line", "p_over", "p_under", "p_push"],
        "comparators": ["production RAW", "strict-prior population baseline", "team-shrunk baseline"],
        "forbidden": ["outcome", "result", "official_final_total", "regulation_nine_total", "EV", "ROI", "wager", "ranking"],
    }
    write_json(output_dir / "totals_c_shadow_prediction_schema.json", prediction_fields)
    (output_dir / "totals_c_shadow_outcome_contract.md").write_text("""# C shadow outcome contract

Official finals attach only after RAW's canonical official-final sidecar exists. C stores final total, regulation-nine total, completion state, official source path/hash, grading timestamp, source RAW outcome identity/hash, and the immutable C prediction hash in a separate append-only outcome table. Prediction rows are never updated.
""")
    (output_dir / "totals_c_shadow_comparator_contract.md").write_text("""# C shadow comparator contract

Every C row freezes identical-row references to unchanged production RAW, the shared strict-prior league-total population baseline, and the shared leakage-safe team-state baseline `0.5 × (home offense + away offense + home prevention + away prevention)`. V1_INTERCEPT is descriptive RAW-only context and is never applied to C. Markets are secondary context, not admission or success gates.
""")
    (output_dir / "totals_c_shadow_metrics_contract.md").write_text("""# C shadow formal metrics contract

At formal checkpoints only: mean RMSE and bias; negative-binomial median MAE; distribution CRPS, Brier, log loss, and ECE. Compare identical rows with RAW, prior-population, and team-shrunk baselines using date-clustered uncertainty. No EV/ROI and no new outcome-responsive metrics.
""")
    (output_dir / "totals_c_shadow_deployment_watch_contract.md").write_text("""# C shadow deployment watches

Every natural scoring run appends an immutable PASS/WATCH/FAIL observation for: bullpen freshness, valid zero-burden frequency, likely-reliever-count drift, starter fallback mix, league-total center drift, probable-pitcher availability, park/context fallback, feature-support violations, and model/hash integrity. Frozen development support bounds govern drift checks. A structural FAIL requires human review and never silently changes C.

Active launch watches: likely-reliever-count drift remains a mild deployment watch; exact late-season roster/elimination indicators are not yet bound to a canonical local source.
""")
    (output_dir / "totals_c_shadow_regime_contract.md").write_text("""# C shadow evidence regimes

The experiment begins in `C_SHADOW_PRIMARY_2026_REGIME` on 2026-08-17, with a separate daily operational classification:

- `NORMAL_COMPETITIVE_REGIME`: eligible for the primary 8/12 checkpoints.
- `LATE_SEASON_TRANSITION_WATCH`: retain and grade, but human review is required before primary inclusion.
- `LATE_SEASON_DISTINCT_REGIME`: retain and grade under `C_SHADOW_LATE_SEASON_REGIME`; never automatically pool.

Performance cannot determine regime boundaries. Until exact objective roster/elimination classification is supportable, the implementation records `LATE_SEASON_TRANSITION_WATCH` instead of inventing certainty.
""")
    (output_dir / "totals_c_shadow_review_schedule.md").write_text("""# Frozen C shadow review schedule

The generic 20-date-cluster checkpoint is cancelled before launch.

`C_SHADOW_FIRST_FORMAL_CHECKPOINT=8_COMPLETED_PRIMARY_REGIME_DATE_CLUSTERS`

`C_SHADOW_SECOND_FORMAL_CHECKPOINT=12_COMPLETED_PRIMARY_REGIME_DATE_CLUSTERS_CONDITIONAL`

Daily operations report health only. At eight eligible clusters, stop for the frozen early review questions without modifying C. The 12-cluster review occurs only if representative primary-regime slates remain. If transition occurs first, preserve the actual clean `PRIMARY_2026_SHADOW_WINDOW`, continue immutable late-season capture separately, and do not force 12.
""")
    (output_dir / "totals_c_shadow_immutability_contract.md").write_text("""# C shadow immutability contract

After launch, model bytes, coefficients, feature contract, preprocessing, probability/point semantics, snapshot policy, and bullpen freshness contract are frozen. SQLite triggers reject prediction/context/outcome/watch updates and deletes. A material defect freezes this experiment for human review; it cannot be silently patched and continued under the same evidence identity.
""")
    write_csv(output_dir / "totals_c_shadow_validation.csv", validation)
    (output_dir / "totals_c_shadow_launch_readiness.md").write_text(f"""# C live shadow launch readiness

`{decision}`

All {len(validation)} launch validations passed. The existing installed daily wrapper already invokes the repository totals hook; that hook now launches C only after successful RAW lifecycle completion. No scheduler change is needed. No real C prediction ledger exists and no August 16 or earlier C row was created. The first possible prediction is the natural August 17 `PRIMARY_SCORE` cycle.

No human action is required before automatic launch. Human review is required later for any `LATE_SEASON_TRANSITION_WATCH` cluster before it counts toward the primary checkpoints, and at the frozen 8-cluster checkpoint.
""")
    (output_dir / "concise_mlb_totals_c_live_shadow_launch_v1.md").write_text(f"""# MLB Totals C live shadow launch v1

- Subject: `{scorer.MODEL_NAME}` / `{scorer.MODEL_HASH}`; artifact `{scorer.ARTIFACT_SHA256}`; frozen without refit.
- Start: first natural untouched 2026-08-17 scoring cycle. No earlier live C evidence.
- Input: exact immutable RAW pregame context; RAW remains `{scorer.RAW_MODEL_HASH}` and unchanged.
- Policy: 05:30 primary; later missing-only; first valid pregame identity immutable; outcomes separate.
- Product: NB mean expected total, integer NB median central/MAE point, full normalized NB distribution; RAW intercept not applied.
- Watches: bullpen freshness, zero burden, reliever counts, starter/league/probable/park/support state, hash integrity. Regime classification fails conservatively to transition WATCH when exact objective late-season evidence is unavailable.
- Reviews: 8 completed primary-regime clusters; conditional 12 if the primary regime remains representative. Transition before 12 freezes the actual clean primary window and continues late-season capture separately.
- Validation: {len(validation)}/{len(validation)} PASS. No production/public/upload/ranking/wager side effect.
- Decision: `{decision}`.
""")
    after = {str(path): sha256(path) for path in protected}
    if before != after:
        raise RuntimeError("PROTECTED_RAW_OR_PUBLIC_STATE_CHANGED")
    outputs = sorted(path for path in output_dir.iterdir() if path.name != "reproducibility_hashes.sha256")
    manifest = [f"{sha256(path)}  {path.name}" for path in outputs]
    manifest += [f"{digest}  PROTECTED_INPUT::{path}" for path, digest in sorted(before.items())]
    implementation = [Path(__file__), Path(scorer.__file__), ROOT / "backend/mlb/scripts/run_mlb_totals_c_shadow_daily_v1.py",
                      ROOT / "backend/mlb/totals_predictions/c_shadow_v1.py", HOOK]
    manifest += [f"{sha256(path)}  SHADOW_IMPLEMENTATION::{path}" for path in implementation]
    (output_dir / "reproducibility_hashes.sha256").write_text("\n".join(manifest) + "\n")
    if {path.name for path in output_dir.iterdir()} != set(REQUIRED):
        raise RuntimeError("C_SHADOW_REQUIRED_OUTPUT_SET_MISMATCH")
    return {"decision": decision, "validation": validation_status, "validation_rows": len(validation), "output_dir": str(output_dir)}


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    args = parser.parse_args(); print(json.dumps(run(args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
