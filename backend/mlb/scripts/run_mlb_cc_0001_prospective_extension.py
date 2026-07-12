#!/usr/bin/env python3
"""MLB-CC-0001 prospective extension runner.

This runner is intentionally artifact-only:
- no retraining
- no production model writes
- no uploads
- no scheduler changes
- no OddsAPI calls
- no DB writes

It scores only complete, frozen, prediction-time-safe input manifests.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score


ROOT = Path(__file__).resolve().parents[3]
EXPERIMENT_ID = "MLB-CC-0001"
EVIDENCE_PHASE = "prospective_extension"
EXTENSION_START_EXCLUSIVE = "2026-07-09"
MAX_ENDPOINT_DATE = "2026-08-16"
EVIDENCE_FLOOR_SLATES = 12
EVIDENCE_FLOOR_ROWS = 3000
EVIDENCE_FLOOR_CLUSTERS = 1500
MAX_GRADED_SLATES = 20

BASE_DIR = ROOT / "artifacts/analysis/model_development/mlb_cc_0001_prospective_extension"
RUNS_DIR = BASE_DIR / "runs"
PREGAME_MANIFEST_ROOT = BASE_DIR / "pregame_manifests"
EXEC_DIR = ROOT / "artifacts/analysis/model_development/mlb_cc_0001_execution_2026-07-10"
GOV_DIR = ROOT / "artifacts/analysis/model_development/mlb_cc_0001_post_execution_governance_2026-07-10"

CONTROL_MODEL = EXEC_DIR / "model_artifacts/mlb_cc_0001_control.joblib"
CHALLENGER_MODEL = EXEC_DIR / "model_artifacts/mlb_cc_0001_pa_challenger.joblib"
CONTROL_FEATURE_MANIFEST = EXEC_DIR / "mlb_cc_0001_control_feature_manifest_2026-07-10.csv"
CHALLENGER_FEATURE_MANIFEST = EXEC_DIR / "mlb_cc_0001_challenger_feature_manifest_2026-07-10.csv"
CONTROL_SHA = "1acbc1ee25372cd7779752674998a85e1ae856539bb00ff74d727d5049c072ef"
CHALLENGER_SHA = "d79f57cce162ecefb014f6bfa30d408b78e295eb66054f4203dd8d5b4934dc77"
PA_FEATURES = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_missing_flag",
]
PREGAME_MANIFEST_SCHEMA_VERSION = "mlb_cc_0001_pregame_manifest.v1"

CONTRACT_JSON = BASE_DIR / "mlb_cc_0001_prospective_extension_contract.json"
CONTRACT_MD = BASE_DIR / "mlb_cc_0001_prospective_extension_contract.md"
CONTRACT_SHA = BASE_DIR / "mlb_cc_0001_prospective_extension_contract.sha256"
FROZEN_MODEL_INVENTORY = BASE_DIR / "mlb_cc_0001_frozen_model_inventory.csv"
PREDICTION_LEDGER = BASE_DIR / "mlb_cc_0001_prospective_prediction_ledger.csv"
GRADED_LEDGER = BASE_DIR / "mlb_cc_0001_prospective_graded_ledger.csv"
RUN_INDEX = BASE_DIR / "mlb_cc_0001_prospective_run_index.csv"
PROGRESS_JSON = BASE_DIR / "mlb_cc_0001_prospective_progress.json"
PROGRESS_MD = BASE_DIR / "mlb_cc_0001_prospective_progress.md"
METRIC_HISTORY = BASE_DIR / "mlb_cc_0001_prospective_metric_history.csv"
SLATE_SUMMARY = BASE_DIR / "mlb_cc_0001_prospective_slate_summary.csv"
INTEGRITY_STATUS = BASE_DIR / "mlb_cc_0001_prospective_integrity_status.csv"
DECISION_STATUS = BASE_DIR / "mlb_cc_0001_prospective_decision_status.md"

PREDICTION_COLUMNS = [
    "experiment_id",
    "evidence_phase",
    "run_tag",
    "slate_date",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "line",
    "side",
    "control_probability",
    "challenger_probability",
    "probability_difference",
    "control_rank",
    "challenger_rank",
    "rank_difference",
    "market_price",
    "bookmaker",
    "feature_cutoff_verified",
    "pa_context_latest_date",
    "row_hash",
    "cluster_id",
    "control_model_sha256",
    "challenger_model_sha256",
    "input_manifest_sha256",
    "scored_timestamp_utc",
]

GRADED_EXTRA_COLUMNS = [
    "official_outcome",
    "target",
    "settlement",
    "control_log_loss",
    "challenger_log_loss",
    "paired_log_loss_diff",
    "control_brier",
    "challenger_brier",
    "paired_brier_diff",
    "control_win",
    "challenger_win",
    "market_result",
    "graded_timestamp_utc",
    "outcome_source_reference",
    "outcome_source_sha256",
    "grading_status",
]
GRADED_COLUMNS = PREDICTION_COLUMNS + GRADED_EXTRA_COLUMNS


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def append_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_csv_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if path.exists() and path.stat().st_size > 0:
        return pd.read_csv(path)
    return pd.DataFrame(columns=columns)


def stable_row_hash(row: pd.Series) -> str:
    fields = [
        EXPERIMENT_ID,
        EVIDENCE_PHASE,
        str(row.get("slate_date", "")),
        str(row.get("game_id", "")),
        str(row.get("player_id", "")),
        str(row.get("prop_type", "")),
        str(row.get("line", "")),
        str(row.get("side", "")),
        str(row.get("market_price", "")),
        str(row.get("bookmaker", "")),
    ]
    return hashlib.sha256("|".join(fields).encode()).hexdigest()


def cluster_id(row: pd.Series) -> str:
    fields = [
        str(row.get("slate_date", "")),
        str(row.get("game_id", "")),
        str(row.get("player_id", "")),
        str(row.get("prop_type", "")),
        str(row.get("side", "")),
        str(row.get("line", "")),
    ]
    return hashlib.sha256("|".join(fields).encode()).hexdigest()[:16]


def contract_payload() -> dict[str, Any]:
    return {
        "experiment_id": EXPERIMENT_ID,
        "evidence_phase": EVIDENCE_PHASE,
        "extension_start_boundary": {"exclusive_after_slate_date": EXTENSION_START_EXCLUSIVE},
        "evidence_floor": {
            "fully_graded_slates": EVIDENCE_FLOOR_SLATES,
            "paired_rows": EVIDENCE_FLOOR_ROWS,
            "independent_clusters": EVIDENCE_FLOOR_CLUSTERS,
        },
        "maximum_endpoint": {
            "fully_graded_slates": MAX_GRADED_SLATES,
            "calendar_date": MAX_ENDPOINT_DATE,
            "rule": "whichever_occurs_first",
        },
        "frozen_models": {
            "control": {"path": rel(CONTROL_MODEL), "sha256": CONTROL_SHA, "label": "MLB-CC-0001-CONTROL-FROZEN"},
            "challenger": {"path": rel(CHALLENGER_MODEL), "sha256": CHALLENGER_SHA, "label": "MLB-CC-0001-PA-CHALLENGER-FROZEN"},
        },
        "feature_manifests": {
            "control": {"path": rel(CONTROL_FEATURE_MANIFEST), "sha256": sha256_path(CONTROL_FEATURE_MANIFEST)},
            "challenger": {"path": rel(CHALLENGER_FEATURE_MANIFEST), "sha256": sha256_path(CHALLENGER_FEATURE_MANIFEST)},
        },
        "repaired_pa_semantics": {
            "features": PA_FEATURES,
            "cutoff": "pa_context_latest_date must be strictly earlier than slate_date",
            "raw_plate_appearances_allowed": False,
        },
        "target_definition": "target_class=1 means official over-target success for the frozen row grain",
        "row_grain": "one paired model row per eligible player/game/prop/side/line/market observation in the frozen prospective input manifest",
        "eligible_line_side_policy": "same as governed execution; broad Hits rows only when both frozen models can score identical rows",
        "outcome_source": "approved official player-outcome/reconcile source; supplied as immutable outcome manifest for grading",
        "settlement_policy": "win/loss/push/void follows champion evaluation policy; missing joins are classified, not silently dropped",
        "duplicate_policy": "reject duplicate row_hash; reject duplicate logical keys unless byte-identical replay is explicitly audited",
        "feature_cutoff_policy": "prediction-time snapshot required; no same-game outcomes or postgame-mutated feature reconstruction",
        "prediction_time_snapshot_policy": "score only a complete immutable pregame input manifest; otherwise MISSED_PREGAME_FREEZE or SOURCE_INCOMPLETE",
        "cluster_definition": "player/game/prop/side/line cluster_id",
        "primary_metric": "paired log-loss difference, challenger minus control",
        "guardrails": ["Brier", "AUC/ranking", "calibration", "coverage", "data defects", "market degradation"],
        "uncertainty_method": {"method": "paired cluster bootstrap", "confidence_level": 0.95, "seed": 42},
        "interim_schedule": [
            {"after_fully_graded_slates": 6, "allowed_actions": ["informational", "futility_only"]},
            {"after_fully_graded_slates": 12, "allowed_actions": ["informational", "futility_only"]},
        ],
        "futility_policy": "may close only for adverse/effectively-zero cumulative effect under frozen stopping rules; no early promotion",
        "final_decision_rules": {
            "promotion_strength_support": "material thresholds plus 95% bootstrap upper CI < 0 and no guardrail degradation; authorizes only later explicit promotion review",
            "valid_no_improvement": "adequate evidence shows no material benefit or adverse/zero effect",
            "insufficient_evidence": "favorable direction remains but endpoint reached without promotion-strength evidence",
        },
    }


def initialize_ledgers() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    if not PREDICTION_LEDGER.exists():
        write_csv(PREDICTION_LEDGER, [], PREDICTION_COLUMNS)
    if not GRADED_LEDGER.exists():
        write_csv(GRADED_LEDGER, [], GRADED_COLUMNS)
    if not RUN_INDEX.exists():
        write_csv(
            RUN_INDEX,
            [],
            [
                "run_tag",
                "mode",
                "slate_date",
                "started_at_utc",
                "finished_at_utc",
                "status",
                "rows_written",
                "run_dir",
                "validation_report",
                "notes",
            ],
        )
    for path, columns in [
        (METRIC_HISTORY, ["generated_at_utc", "view", "fully_graded_slates", "paired_rows", "clusters", "control_log_loss", "challenger_log_loss", "paired_log_loss_diff", "control_brier", "challenger_brier", "paired_brier_diff", "bootstrap_ci_lower", "bootstrap_ci_upper", "interim_status"]),
        (SLATE_SUMMARY, ["slate_date", "scored_rows", "graded_rows", "clusters", "games", "players", "status", "notes"]),
        (INTEGRITY_STATUS, ["generated_at_utc", "check_name", "status", "detail", "run_tag"]),
    ]:
        if not path.exists():
            write_csv(path, [], columns)


def freeze_contract() -> str:
    initialize_ledgers()
    payload = contract_payload()
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    CONTRACT_JSON.write_text(text)
    contract_sha = sha256_text(text)
    CONTRACT_SHA.write_text(contract_sha + "\n")
    lines = [
        "# MLB-CC-0001 Prospective Extension Contract",
        "",
        f"- Experiment ID: `{EXPERIMENT_ID}`",
        f"- Contract SHA256: `{contract_sha}`",
        f"- Extension start: strictly after `{EXTENSION_START_EXCLUSIVE}`",
        f"- Evidence floor: `{EVIDENCE_FLOOR_SLATES}` slates, `{EVIDENCE_FLOOR_ROWS}` rows, `{EVIDENCE_FLOOR_CLUSTERS}` clusters",
        f"- Maximum endpoint: `{MAX_GRADED_SLATES}` fully graded slates or `{MAX_ENDPOINT_DATE}`, whichever occurs first",
        f"- Control model: `{rel(CONTROL_MODEL)}` / `{CONTROL_SHA}`",
        f"- PA challenger: `{rel(CHALLENGER_MODEL)}` / `{CHALLENGER_SHA}`",
        "- Early promotion: `NOT_ALLOWED`",
        "- Production promotion: `NOT_AUTHORIZED`",
        "",
        "The machine-readable contract is the controlling artifact for all runner modes.",
    ]
    CONTRACT_MD.write_text("\n".join(lines) + "\n")
    write_frozen_model_inventory()
    update_status("freeze-contract")
    return contract_sha


def load_contract_and_verify() -> dict[str, Any]:
    if not CONTRACT_JSON.exists() or not CONTRACT_SHA.exists():
        raise SystemExit("Prospective contract is not frozen. Run --mode freeze-contract first.")
    actual = sha256_path(CONTRACT_JSON)
    expected = CONTRACT_SHA.read_text().strip()
    if actual != expected:
        raise SystemExit(f"Contract SHA mismatch: actual={actual} expected={expected}")
    if sha256_path(CONTROL_MODEL) != CONTROL_SHA:
        raise SystemExit("Control model SHA mismatch")
    if sha256_path(CHALLENGER_MODEL) != CHALLENGER_SHA:
        raise SystemExit("Challenger model SHA mismatch")
    payload = json.loads(CONTRACT_JSON.read_text())
    if sha256_path(CONTROL_FEATURE_MANIFEST) != payload["feature_manifests"]["control"]["sha256"]:
        raise SystemExit("Control feature manifest SHA mismatch")
    if sha256_path(CHALLENGER_FEATURE_MANIFEST) != payload["feature_manifests"]["challenger"]["sha256"]:
        raise SystemExit("Challenger feature manifest SHA mismatch")
    return payload


def write_frozen_model_inventory() -> None:
    rows = []
    for model_id, role, path, feature_path, feature_count, sha in [
        ("MLB-CC-0001-CONTROL-FROZEN", "control", CONTROL_MODEL, CONTROL_FEATURE_MANIFEST, 73, CONTROL_SHA),
        ("MLB-CC-0001-PA-CHALLENGER-FROZEN", "pa_challenger", CHALLENGER_MODEL, CHALLENGER_FEATURE_MANIFEST, 77, CHALLENGER_SHA),
    ]:
        rows.append(
            {
                "experiment_model_id": model_id,
                "role": role,
                "artifact_path": rel(path),
                "artifact_sha256": sha256_path(path),
                "expected_sha256": sha,
                "feature_manifest_path": rel(feature_path),
                "feature_manifest_sha256": sha256_path(feature_path),
                "feature_count": feature_count,
                "freeze_status": "FROZEN_EXPERIMENT_LOCAL",
                "production_usage": "NO",
            }
        )
    write_csv(
        FROZEN_MODEL_INVENTORY,
        rows,
        ["experiment_model_id", "role", "artifact_path", "artifact_sha256", "expected_sha256", "feature_manifest_path", "feature_manifest_sha256", "feature_count", "freeze_status", "production_usage"],
    )


def feature_names() -> tuple[list[str], list[str]]:
    control = pd.read_csv(CONTROL_FEATURE_MANIFEST)["feature_name"].astype(str).tolist()
    challenger = pd.read_csv(CHALLENGER_FEATURE_MANIFEST)["feature_name"].astype(str).tolist()
    return control, challenger


def canonical_manifest_metadata_path(path: Path) -> Path:
    return path.parent / "mlb_cc_0001_pregame_manifest_metadata.json"


def load_canonical_manifest_metadata(path: Path) -> dict[str, Any] | None:
    metadata_path = canonical_manifest_metadata_path(path)
    if not metadata_path.exists():
        return None
    try:
        return json.loads(metadata_path.read_text())
    except Exception:
        return None


def find_latest_valid_pregame_manifest(slate_date: str) -> Path | None:
    root = PREGAME_MANIFEST_ROOT / slate_date
    if not root.exists():
        return None
    candidates = sorted(root.glob(f"*/mlb_cc_0001_pregame_manifest_{slate_date}__*.csv"), reverse=True)
    for path in candidates:
        metadata = load_canonical_manifest_metadata(path)
        if not metadata:
            continue
        if metadata.get("generator_schema_version") != PREGAME_MANIFEST_SCHEMA_VERSION:
            continue
        if metadata.get("slate_date") != slate_date:
            continue
        if metadata.get("pregame_freeze_status") != "VALID_PREGAME_FREEZE":
            continue
        if metadata.get("manifest_sha256") != sha256_path(path):
            continue
        return path
    return None


def make_run_dir(mode: str, slate_date: str | None) -> tuple[str, Path]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tag = f"{mode}_{slate_date or 'all'}_{stamp}"
    path = RUNS_DIR / tag
    path.mkdir(parents=True, exist_ok=False)
    return tag, path


def append_run_index(run_tag: str, mode: str, slate_date: str | None, started: str, status: str, rows_written: int, run_dir: Path, validation_path: Path, notes: str) -> None:
    append_csv(
        RUN_INDEX,
        [
            {
                "run_tag": run_tag,
                "mode": mode,
                "slate_date": slate_date or "",
                "started_at_utc": started,
                "finished_at_utc": utc_now(),
                "status": status,
                "rows_written": rows_written,
                "run_dir": rel(run_dir),
                "validation_report": rel(validation_path),
                "notes": notes,
            }
        ],
        ["run_tag", "mode", "slate_date", "started_at_utc", "finished_at_utc", "status", "rows_written", "run_dir", "validation_report", "notes"],
    )


def validate_input_manifest(path: Path, slate_date: str) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    control, challenger = feature_names()
    df = pd.read_csv(path)
    checks = []
    def add(name: str, status: str, detail: str) -> None:
        checks.append({"check_name": name, "status": status, "detail": detail})
    metadata = load_canonical_manifest_metadata(path)
    add("canonical_manifest_metadata_present", "PASS" if metadata else "FAIL", rel(canonical_manifest_metadata_path(path)))
    if metadata:
        add("generator_schema_version", "PASS" if metadata.get("generator_schema_version") == PREGAME_MANIFEST_SCHEMA_VERSION else "FAIL", str(metadata.get("generator_schema_version")))
        add("metadata_slate_date_match", "PASS" if metadata.get("slate_date") == slate_date else "FAIL", str(metadata.get("slate_date")))
        add("metadata_manifest_sha256", "PASS" if metadata.get("manifest_sha256") == sha256_path(path) else "FAIL", str(metadata.get("manifest_sha256")))
        add("metadata_contract_sha256", "PASS" if metadata.get("contract_sha256") == CONTRACT_SHA.read_text().strip() else "FAIL", str(metadata.get("contract_sha256")))
        add("metadata_control_model_sha256", "PASS" if metadata.get("control_model_sha256") == CONTROL_SHA else "FAIL", str(metadata.get("control_model_sha256")))
        add("metadata_challenger_model_sha256", "PASS" if metadata.get("challenger_model_sha256") == CHALLENGER_SHA else "FAIL", str(metadata.get("challenger_model_sha256")))
        add("metadata_freeze_status", "PASS" if metadata.get("pregame_freeze_status") == "VALID_PREGAME_FREEZE" else "FAIL", str(metadata.get("pregame_freeze_status")))
    if "generator_schema_version" in df.columns:
        versions = set(df["generator_schema_version"].astype(str))
        add("row_generator_schema_version", "PASS" if versions == {PREGAME_MANIFEST_SCHEMA_VERSION} else "FAIL", str(sorted(versions)))
    else:
        add("row_generator_schema_version", "FAIL", "missing generator_schema_version")
    if "pregame_freeze_status" in df.columns:
        statuses = set(df["pregame_freeze_status"].astype(str))
        add("row_pregame_freeze_status", "PASS" if statuses == {"VALID_PREGAME_FREEZE"} else "FAIL", str(sorted(statuses)))
    else:
        add("row_pregame_freeze_status", "FAIL", "missing pregame_freeze_status")
    required_identity = ["slate_date", "game_id", "player_id", "player_name", "team", "opponent", "prop_type", "line", "side"]
    missing_identity = [c for c in required_identity if c not in df.columns]
    add("identity_columns_present", "PASS" if not missing_identity else "FAIL", ",".join(missing_identity))
    missing_control = [c for c in control if c not in df.columns]
    missing_challenger = [c for c in challenger if c not in df.columns]
    add("control_feature_columns_present", "PASS" if not missing_control else "FAIL", f"missing={len(missing_control)}")
    add("challenger_feature_columns_present", "PASS" if not missing_challenger else "FAIL", f"missing={len(missing_challenger)}")
    add("raw_plate_appearances_absent", "PASS" if "plate_appearances" not in df.columns else "FAIL", "raw plate_appearances column forbidden")
    forbidden_outcomes = [c for c in ["target", "official_outcome", "settlement", "target_class", "target_value"] if c in df.columns and df[c].notna().any()]
    add("pregame_outcomes_absent", "PASS" if not forbidden_outcomes else "FAIL", ",".join(forbidden_outcomes))
    if "slate_date" in df.columns:
        add("requested_date_match", "PASS" if set(df["slate_date"].astype(str)) == {slate_date} else "FAIL", str(sorted(set(df["slate_date"].astype(str)))))
        add("post_holdout_date", "PASS" if slate_date > EXTENSION_START_EXCLUSIVE else "FAIL", f"{slate_date} must be > {EXTENSION_START_EXCLUSIVE}")
    if "pa_context_latest_date" in df.columns and "slate_date" in df.columns:
        pa = pd.to_datetime(df["pa_context_latest_date"], errors="coerce")
        sd = pd.to_datetime(df["slate_date"], errors="coerce")
        bad = int((pa >= sd).sum())
        add("pa_context_strict_prior", "PASS" if bad == 0 else "FAIL", f"bad_rows={bad}")
    else:
        add("pa_context_strict_prior", "FAIL", "pa_context_latest_date required")
    if "snapshot_frozen_before_first_pitch" in df.columns:
        ok = df["snapshot_frozen_before_first_pitch"].astype(str).str.lower().isin(["true", "1", "yes"]).all()
        add("pregame_snapshot_assertion", "PASS" if ok else "FAIL", "snapshot_frozen_before_first_pitch must be true for every row")
    else:
        add("pregame_snapshot_assertion", "FAIL", "missing snapshot_frozen_before_first_pitch")
    return df, checks


def fail_if_checks_fail(checks: list[dict[str, Any]]) -> None:
    failures = [c for c in checks if c["status"] == "FAIL"]
    if failures:
        raise SystemExit("Validation failed: " + "; ".join(f"{c['check_name']}={c['detail']}" for c in failures[:5]))


def score(args: argparse.Namespace) -> None:
    load_contract_and_verify()
    started = utc_now()
    run_tag, run_dir = make_run_dir("score", args.date)
    validation_path = run_dir / "validation_report.csv"
    if not args.input_manifest:
        auto_manifest = find_latest_valid_pregame_manifest(args.date)
        if not auto_manifest:
            rows = inspect_date(args.date)
            write_csv(run_dir / "date_inspection.csv", rows, ["slate_date", "classification", "source_path", "detail", "eligible_for_score"])
            write_csv(validation_path, [{"check_name": "input_manifest_supplied", "status": "FAIL", "detail": "no same-date valid canonical pregame manifest found"}], ["check_name", "status", "detail"])
            append_run_index(run_tag, "score", args.date, started, "SOURCE_INCOMPLETE", 0, run_dir, validation_path, "No valid canonical manifest supplied or auto-located; no ledger append")
            update_status(run_tag)
            return
        input_path = auto_manifest
    else:
        input_path = Path(args.input_manifest)
    df, checks = validate_input_manifest(input_path, args.date)
    shutil.copy2(input_path, run_dir / input_path.name)
    input_sha = sha256_path(input_path)
    checks.append({"check_name": "input_manifest_sha256", "status": "PASS", "detail": input_sha})
    write_csv(validation_path, checks, ["check_name", "status", "detail"])
    fail_if_checks_fail(checks)
    control_features, challenger_features = feature_names()
    control_payload = joblib.load(CONTROL_MODEL)
    challenger_payload = joblib.load(CHALLENGER_MODEL)
    control_model = control_payload.get("model") or control_payload.get("best")
    challenger_model = challenger_payload.get("model") or challenger_payload.get("best")
    control_prob = control_model.predict_proba(df[control_features])[:, 1]
    challenger_prob = challenger_model.predict_proba(df[challenger_features])[:, 1]
    out = df.copy()
    out["control_probability"] = control_prob
    out["challenger_probability"] = challenger_prob
    out["probability_difference"] = out["challenger_probability"] - out["control_probability"]
    out["control_rank"] = out["control_probability"].rank(method="first", ascending=False).astype(int)
    out["challenger_rank"] = out["challenger_probability"].rank(method="first", ascending=False).astype(int)
    out["rank_difference"] = out["challenger_rank"] - out["control_rank"]
    if "market_price" not in out.columns:
        out["market_price"] = ""
    if "bookmaker" not in out.columns:
        out["bookmaker"] = ""
    out["feature_cutoff_verified"] = True
    out["row_hash"] = out.apply(stable_row_hash, axis=1)
    out["cluster_id"] = out.apply(cluster_id, axis=1)
    out["experiment_id"] = EXPERIMENT_ID
    out["evidence_phase"] = EVIDENCE_PHASE
    out["run_tag"] = run_tag
    out["control_model_sha256"] = CONTROL_SHA
    out["challenger_model_sha256"] = CHALLENGER_SHA
    out["input_manifest_sha256"] = input_sha
    out["scored_timestamp_utc"] = utc_now()
    pred = out[PREDICTION_COLUMNS].copy()
    if pred["row_hash"].duplicated().any():
        raise SystemExit("Duplicate row_hash within scoring run")
    existing = read_csv_or_empty(PREDICTION_LEDGER, PREDICTION_COLUMNS)
    dupes = set(pred["row_hash"]) & set(existing["row_hash"].astype(str))
    if dupes:
        raise SystemExit(f"Duplicate row_hash already in prediction ledger: {len(dupes)}")
    pred.to_csv(run_dir / "paired_pregame_predictions.csv", index=False)
    append_csv(PREDICTION_LEDGER, pred.to_dict("records"), PREDICTION_COLUMNS)
    (run_dir / "prediction_ledger_after.sha256").write_text(sha256_path(PREDICTION_LEDGER) + "\n")
    append_run_index(run_tag, "score", args.date, started, "PASS", len(pred), run_dir, validation_path, "prediction ledger appended")
    update_status(run_tag)


def row_log_loss(prob: float, target: int) -> float:
    p = min(max(float(prob), 1e-15), 1 - 1e-15)
    return float(-(target * math.log(p) + (1 - target) * math.log(1 - p)))


def grade(args: argparse.Namespace) -> None:
    load_contract_and_verify()
    started = utc_now()
    run_tag, run_dir = make_run_dir("grade", args.date)
    validation_path = run_dir / "validation_report.csv"
    preds = read_csv_or_empty(PREDICTION_LEDGER, PREDICTION_COLUMNS)
    preds = preds[preds["slate_date"].astype(str).eq(args.date)].copy()
    if preds.empty:
        write_csv(validation_path, [{"check_name": "predictions_exist_for_date", "status": "FAIL", "detail": "no prediction rows for slate"}], ["check_name", "status", "detail"])
        append_run_index(run_tag, "grade", args.date, started, "NOT_FULLY_GRADED", 0, run_dir, validation_path, "no prediction rows for date")
        update_status(run_tag)
        return
    if not args.outcome_manifest:
        write_csv(validation_path, [{"check_name": "outcome_manifest_supplied", "status": "FAIL", "detail": "immutable outcome manifest required"}], ["check_name", "status", "detail"])
        append_run_index(run_tag, "grade", args.date, started, "NOT_FULLY_GRADED", 0, run_dir, validation_path, "no outcome manifest supplied")
        update_status(run_tag)
        return
    outcome_path = Path(args.outcome_manifest)
    outcomes = pd.read_csv(outcome_path)
    required = {"row_hash", "official_outcome", "target", "settlement", "outcome_source_reference"}
    missing = required - set(outcomes.columns)
    checks = [{"check_name": "outcome_columns_present", "status": "PASS" if not missing else "FAIL", "detail": ",".join(sorted(missing))}]
    checks.append({"check_name": "outcome_source_sha256", "status": "PASS", "detail": sha256_path(outcome_path)})
    write_csv(validation_path, checks, ["check_name", "status", "detail"])
    fail_if_checks_fail(checks)
    merged = preds.merge(outcomes, on="row_hash", how="left", suffixes=("", "_outcome"))
    graded_rows = []
    for _, row in merged.iterrows():
        status = "GRADED" if pd.notna(row.get("target")) else "OUTCOME_JOIN_MISSING"
        target = int(row["target"]) if status == "GRADED" else ""
        control_ll = row_log_loss(row["control_probability"], target) if status == "GRADED" else ""
        challenger_ll = row_log_loss(row["challenger_probability"], target) if status == "GRADED" else ""
        control_brier = (float(row["control_probability"]) - target) ** 2 if status == "GRADED" else ""
        challenger_brier = (float(row["challenger_probability"]) - target) ** 2 if status == "GRADED" else ""
        rec = {c: row.get(c, "") for c in PREDICTION_COLUMNS}
        rec.update(
            {
                "official_outcome": row.get("official_outcome", ""),
                "target": target,
                "settlement": row.get("settlement", ""),
                "control_log_loss": control_ll,
                "challenger_log_loss": challenger_ll,
                "paired_log_loss_diff": challenger_ll - control_ll if status == "GRADED" else "",
                "control_brier": control_brier,
                "challenger_brier": challenger_brier,
                "paired_brier_diff": challenger_brier - control_brier if status == "GRADED" else "",
                "control_win": "",
                "challenger_win": "",
                "market_result": row.get("settlement", ""),
                "graded_timestamp_utc": utc_now(),
                "outcome_source_reference": row.get("outcome_source_reference", ""),
                "outcome_source_sha256": sha256_path(outcome_path),
                "grading_status": status,
            }
        )
        graded_rows.append(rec)
    existing = read_csv_or_empty(GRADED_LEDGER, GRADED_COLUMNS)
    dupes = set(r["row_hash"] for r in graded_rows) & set(existing["row_hash"].astype(str))
    if dupes:
        raise SystemExit(f"Duplicate row_hash already in graded ledger: {len(dupes)}")
    write_csv(run_dir / "graded_rows.csv", graded_rows, GRADED_COLUMNS)
    append_csv(GRADED_LEDGER, graded_rows, GRADED_COLUMNS)
    append_run_index(run_tag, "grade", args.date, started, "PASS", len(graded_rows), run_dir, validation_path, "graded ledger appended")
    update_status(run_tag)


def bootstrap_ci(df: pd.DataFrame, reps: int = 1000, seed: int = 42) -> tuple[float, float]:
    if df.empty:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    grouped = df.groupby("cluster_id", dropna=False).agg(rows=("cluster_id", "size"), diff_sum=("paired_log_loss_diff", "sum")).reset_index()
    counts = grouped["rows"].astype(float).to_numpy()
    diffs = grouped["diff_sum"].astype(float).to_numpy()
    n = len(grouped)
    vals = []
    for _ in range(reps):
        idx = rng.integers(0, n, size=n)
        vals.append(float(diffs[idx].sum() / max(counts[idx].sum(), 1.0)))
    return float(np.percentile(vals, 2.5)), float(np.percentile(vals, 97.5))


def metrics_from_graded() -> dict[str, Any]:
    graded = read_csv_or_empty(GRADED_LEDGER, GRADED_COLUMNS)
    valid = graded[graded.get("grading_status", pd.Series(dtype=str)).astype(str).eq("GRADED")].copy() if not graded.empty else graded
    if valid.empty:
        return {
            "fully_graded_slates": 0,
            "paired_rows": 0,
            "clusters": 0,
            "games": 0,
            "players": 0,
            "control_log_loss": None,
            "challenger_log_loss": None,
            "paired_log_loss_diff": None,
            "control_brier": None,
            "challenger_brier": None,
            "paired_brier_diff": None,
            "bootstrap_ci_lower": None,
            "bootstrap_ci_upper": None,
        }
    for col in ["control_log_loss", "challenger_log_loss", "paired_log_loss_diff", "control_brier", "challenger_brier", "paired_brier_diff"]:
        valid[col] = pd.to_numeric(valid[col], errors="coerce")
    ci_lo, ci_hi = bootstrap_ci(valid)
    return {
        "fully_graded_slates": int(valid["slate_date"].nunique()),
        "paired_rows": int(len(valid)),
        "clusters": int(valid["cluster_id"].nunique()),
        "games": int(valid["game_id"].nunique()),
        "players": int(valid["player_id"].nunique()),
        "control_log_loss": float(valid["control_log_loss"].mean()),
        "challenger_log_loss": float(valid["challenger_log_loss"].mean()),
        "paired_log_loss_diff": float(valid["paired_log_loss_diff"].mean()),
        "control_brier": float(valid["control_brier"].mean()),
        "challenger_brier": float(valid["challenger_brier"].mean()),
        "paired_brier_diff": float(valid["paired_brier_diff"].mean()),
        "bootstrap_ci_lower": ci_lo,
        "bootstrap_ci_upper": ci_hi,
    }


def interim_status(metrics: dict[str, Any]) -> str:
    if metrics["fully_graded_slates"] >= MAX_GRADED_SLATES or date.today().isoformat() >= MAX_ENDPOINT_DATE:
        return "MAX_ENDPOINT_REACHED"
    if metrics["fully_graded_slates"] < EVIDENCE_FLOOR_SLATES or metrics["paired_rows"] < EVIDENCE_FLOOR_ROWS or metrics["clusters"] < EVIDENCE_FLOOR_CLUSTERS:
        return "EVIDENCE_FLOOR_NOT_REACHED"
    diff = metrics["paired_log_loss_diff"]
    if diff is None:
        return "EVIDENCE_FLOOR_NOT_REACHED"
    if diff < -0.0005:
        return "DIRECTIONALLY_FAVORABLE"
    if diff > 0.002 and metrics.get("paired_brier_diff", 0) and metrics["paired_brier_diff"] > 0:
        return "FUTILITY_BOUNDARY_MET"
    if abs(diff) <= 0.0005:
        return "DIRECTIONALLY_NEUTRAL"
    return "DIRECTIONALLY_ADVERSE" if diff > 0 else "DIRECTIONALLY_FAVORABLE"


def update_status(run_tag: str) -> None:
    initialize_ledgers()
    metrics = metrics_from_graded()
    preds = read_csv_or_empty(PREDICTION_LEDGER, PREDICTION_COLUMNS)
    graded = read_csv_or_empty(GRADED_LEDGER, GRADED_COLUMNS)
    scored_slates = sorted(preds["slate_date"].astype(str).unique().tolist()) if not preds.empty else []
    graded_slates = sorted(graded[graded.get("grading_status", pd.Series(dtype=str)).astype(str).eq("GRADED")]["slate_date"].astype(str).unique().tolist()) if not graded.empty else []
    status = interim_status(metrics)
    today = date.today()
    max_date = date.fromisoformat(MAX_ENDPOINT_DATE)
    progress = {
        "experiment_id": EXPERIMENT_ID,
        "generated_at_utc": utc_now(),
        "extension_start_after": EXTENSION_START_EXCLUSIVE,
        "latest_scored_slate": scored_slates[-1] if scored_slates else None,
        "latest_graded_slate": graded_slates[-1] if graded_slates else None,
        "eligible_slates_scored": len(scored_slates),
        "fully_graded_slates": metrics["fully_graded_slates"],
        "missed_pregame_slates": count_run_status("MISSED_PREGAME_FREEZE"),
        "excluded_slates": count_excluded_runs(),
        "cumulative_rows": metrics["paired_rows"],
        "cumulative_clusters": metrics["clusters"],
        "progress_to_12_slate_floor_pct": round(metrics["fully_graded_slates"] / EVIDENCE_FLOOR_SLATES * 100, 2),
        "progress_to_3000_row_floor_pct": round(metrics["paired_rows"] / EVIDENCE_FLOOR_ROWS * 100, 2),
        "progress_to_1500_cluster_floor_pct": round(metrics["clusters"] / EVIDENCE_FLOOR_CLUSTERS * 100, 2),
        "progress_to_20_slate_max_pct": round(metrics["fully_graded_slates"] / MAX_GRADED_SLATES * 100, 2),
        "days_remaining_until_2026_08_16": max((max_date - today).days, 0),
        "current_interim_status": status,
        "finalization_permitted": status in {"MAX_ENDPOINT_REACHED", "FUTILITY_BOUNDARY_MET"} or (metrics["fully_graded_slates"] >= EVIDENCE_FLOOR_SLATES and metrics["paired_rows"] >= EVIDENCE_FLOOR_ROWS and metrics["clusters"] >= EVIDENCE_FLOOR_CLUSTERS),
        "finalization_required": status == "MAX_ENDPOINT_REACHED",
        **metrics,
    }
    PROGRESS_JSON.write_text(json.dumps(progress, indent=2, sort_keys=True) + "\n")
    PROGRESS_MD.write_text(progress_markdown(progress))
    append_csv(
        METRIC_HISTORY,
        [
            {
                "generated_at_utc": progress["generated_at_utc"],
                "view": "prospective_only",
                "fully_graded_slates": metrics["fully_graded_slates"],
                "paired_rows": metrics["paired_rows"],
                "clusters": metrics["clusters"],
                "control_log_loss": metrics["control_log_loss"],
                "challenger_log_loss": metrics["challenger_log_loss"],
                "paired_log_loss_diff": metrics["paired_log_loss_diff"],
                "control_brier": metrics["control_brier"],
                "challenger_brier": metrics["challenger_brier"],
                "paired_brier_diff": metrics["paired_brier_diff"],
                "bootstrap_ci_lower": metrics["bootstrap_ci_lower"],
                "bootstrap_ci_upper": metrics["bootstrap_ci_upper"],
                "interim_status": status,
            }
        ],
        ["generated_at_utc", "view", "fully_graded_slates", "paired_rows", "clusters", "control_log_loss", "challenger_log_loss", "paired_log_loss_diff", "control_brier", "challenger_brier", "paired_brier_diff", "bootstrap_ci_lower", "bootstrap_ci_upper", "interim_status"],
    )
    checks = integrity_checks(run_tag)
    write_csv(INTEGRITY_STATUS, checks, ["generated_at_utc", "check_name", "status", "detail", "run_tag"])
    DECISION_STATUS.write_text(decision_markdown(progress) + "\n")


def progress_markdown(progress: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# MLB-CC-0001 Prospective Progress",
            "",
            f"- Extension start: strictly after `{EXTENSION_START_EXCLUSIVE}`",
            f"- Latest scored slate: `{progress['latest_scored_slate']}`",
            f"- Latest graded slate: `{progress['latest_graded_slate']}`",
            f"- Fully graded slates: `{progress['fully_graded_slates']}` / `{EVIDENCE_FLOOR_SLATES}` floor / `{MAX_GRADED_SLATES}` max",
            f"- Paired rows: `{progress['cumulative_rows']}` / `{EVIDENCE_FLOOR_ROWS}`",
            f"- Independent clusters: `{progress['cumulative_clusters']}` / `{EVIDENCE_FLOOR_CLUSTERS}`",
            f"- Current interim status: `{progress['current_interim_status']}`",
            f"- Finalization permitted: `{progress['finalization_permitted']}`",
            f"- Finalization required: `{progress['finalization_required']}`",
            f"- Days remaining until `{MAX_ENDPOINT_DATE}`: `{progress['days_remaining_until_2026_08_16']}`",
            "",
            "No production promotion is authorized by this progress report.",
        ]
    ) + "\n"


def decision_markdown(progress: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# MLB-CC-0001 Prospective Decision Status",
            "",
            f"- Current status: `{progress['current_interim_status']}`",
            "- Early promotion: `NOT_ALLOWED`",
            f"- Finalization permitted: `{progress['finalization_permitted']}`",
            f"- Finalization required: `{progress['finalization_required']}`",
            "",
            "A final approved classification can be issued only by `finalize` mode when the frozen stopping contract allows it.",
        ]
    )


def integrity_checks(run_tag: str) -> list[dict[str, Any]]:
    checks = []
    def add(name: str, status: str, detail: str) -> None:
        checks.append({"generated_at_utc": utc_now(), "check_name": name, "status": status, "detail": detail, "run_tag": run_tag})
    try:
        load_contract_and_verify()
        add("contract_and_model_hashes", "PASS", "contract, model, and feature manifest hashes verified")
    except SystemExit as exc:
        add("contract_and_model_hashes", "FAIL", str(exc))
    preds = read_csv_or_empty(PREDICTION_LEDGER, PREDICTION_COLUMNS)
    graded = read_csv_or_empty(GRADED_LEDGER, GRADED_COLUMNS)
    add("prediction_row_hash_unique", "PASS" if preds.empty or preds["row_hash"].is_unique else "FAIL", f"rows={len(preds)}")
    add("graded_row_hash_unique", "PASS" if graded.empty or graded["row_hash"].is_unique else "FAIL", f"rows={len(graded)}")
    if not preds.empty:
        add("raw_plate_appearances_absent", "PASS", "prediction ledger schema excludes raw plate_appearances")
    else:
        add("raw_plate_appearances_absent", "NOT_APPLICABLE", "empty ledger")
    add("no_production_path_writes", "PASS", "runner writes only prospective extension artifact directory")
    return checks


def count_run_status(status: str) -> int:
    if not RUN_INDEX.exists():
        return 0
    df = pd.read_csv(RUN_INDEX)
    return int(df["status"].astype(str).eq(status).sum()) if "status" in df.columns else 0


def count_excluded_runs() -> int:
    if not RUN_INDEX.exists():
        return 0
    df = pd.read_csv(RUN_INDEX)
    if "status" not in df.columns:
        return 0
    return int(df["status"].astype(str).isin(["SOURCE_INCOMPLETE", "MISSED_PREGAME_FREEZE", "NOT_FULLY_GRADED"]).sum())


def inspect_date(slate_date: str) -> list[dict[str, Any]]:
    rows = []
    if slate_date <= EXTENSION_START_EXCLUSIVE:
        return [{"slate_date": slate_date, "classification": "SOURCE_INCOMPLETE", "source_path": "", "detail": "date is not strictly after holdout endpoint", "eligible_for_score": False}]
    odds_dir = ROOT / f"backend/mlb/exports/odds_history/{slate_date}"
    prepared = ROOT / f"backend/mlb/exports/model_diagnostics/prepared_feature_vectors/{slate_date}/hits_features.csv"
    if not odds_dir.exists() and not prepared.exists():
        return [{"slate_date": slate_date, "classification": "SOURCE_INCOMPLETE", "source_path": "", "detail": "no local odds/prepared feature artifacts found", "eligible_for_score": False}]
    if prepared.exists():
        df = pd.read_csv(prepared, nrows=1)
        control, challenger = feature_names()
        missing_control = [c for c in control if c not in df.columns]
        missing_challenger = [c for c in challenger if c not in df.columns]
        if missing_control or missing_challenger:
            rows.append(
                {
                    "slate_date": slate_date,
                    "classification": "SOURCE_INCOMPLETE",
                    "source_path": rel(prepared),
                    "detail": f"prepared feature vector is not complete frozen input manifest; missing_control={len(missing_control)} missing_challenger={len(missing_challenger)}",
                    "eligible_for_score": False,
                }
            )
        else:
            rows.append({"slate_date": slate_date, "classification": "ELIGIBLE_FOR_SCORE_NOW", "source_path": rel(prepared), "detail": "complete feature columns present; still requires pregame snapshot assertion", "eligible_for_score": True})
    if odds_dir.exists():
        rows.append({"slate_date": slate_date, "classification": "SOURCE_INCOMPLETE", "source_path": rel(odds_dir), "detail": "odds-history snapshots exist but are not complete frozen feature manifests for this experiment", "eligible_for_score": False})
    return rows


def dry_run(args: argparse.Namespace) -> None:
    load_contract_and_verify()
    started = utc_now()
    run_tag, run_dir = make_run_dir("dry-run", args.date or "scan")
    validation_path = run_dir / "validation_report.csv"
    dates = [args.date] if args.date else discover_post_holdout_dates()
    all_rows = []
    for slate_date in dates:
        all_rows.extend(inspect_date(slate_date))
    write_csv(run_dir / "post_holdout_date_inspection.csv", all_rows, ["slate_date", "classification", "source_path", "detail", "eligible_for_score"])
    write_csv(validation_path, [{"check_name": "dry_run_no_ledger_append", "status": "PASS", "detail": f"dates_inspected={len(dates)}"}], ["check_name", "status", "detail"])
    append_run_index(run_tag, "dry-run", args.date or "", started, "PASS", 0, run_dir, validation_path, "inspection only")
    update_status(run_tag)


def discover_post_holdout_dates() -> list[str]:
    dates = set()
    for base in [ROOT / "backend/mlb/exports/odds_history", ROOT / "backend/mlb/exports/model_diagnostics/prepared_feature_vectors"]:
        if base.exists():
            for p in base.iterdir():
                if p.is_dir() and re_date(p.name) and p.name > EXTENSION_START_EXCLUSIVE:
                    dates.add(p.name)
    return sorted(dates)


def re_date(text: str) -> bool:
    try:
        date.fromisoformat(text)
        return True
    except Exception:
        return False


def status_mode(_: argparse.Namespace) -> None:
    load_contract_and_verify()
    update_status("status")
    print(PROGRESS_JSON.read_text())


def finalize(_: argparse.Namespace) -> None:
    load_contract_and_verify()
    update_status("finalize")
    progress = json.loads(PROGRESS_JSON.read_text())
    if not progress["finalization_permitted"]:
        raise SystemExit("Finalization not permitted by frozen stopping rules")
    classification = "INSUFFICIENT_EVIDENCE"
    if progress["current_interim_status"] == "FUTILITY_BOUNDARY_MET":
        classification = "VALID_NO_IMPROVEMENT"
    elif progress["fully_graded_slates"] >= EVIDENCE_FLOOR_SLATES and progress.get("paired_log_loss_diff") is not None:
        if progress["paired_log_loss_diff"] >= -0.0005:
            classification = "VALID_NO_IMPROVEMENT"
        elif progress.get("bootstrap_ci_upper") is not None and progress["bootstrap_ci_upper"] < 0 and abs(progress["paired_log_loss_diff"]) >= 0.010:
            classification = "PROMOTED"
    final_md = BASE_DIR / "mlb_cc_0001_prospective_final_decision.md"
    final_md.write_text(
        "\n".join(
            [
                "# MLB-CC-0001 Prospective Final Decision",
                "",
                f"- Classification: `{classification}`",
                "- Production promotion authorized: `NO`",
                "",
                "`PROMOTED`, if reached, means eligible for a later explicit shadow/production-promotion review only.",
            ]
        )
        + "\n"
    )
    print(f"classification={classification}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["freeze-contract", "score", "grade", "status", "finalize", "dry-run"], required=True)
    parser.add_argument("--date", help="Slate date YYYY-MM-DD")
    parser.add_argument("--input-manifest", help="Complete immutable pregame input manifest for score mode")
    parser.add_argument("--outcome-manifest", help="Immutable official outcome manifest for grade mode")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.mode == "freeze-contract":
        sha = freeze_contract()
        print(json.dumps({"contract_path": rel(CONTRACT_JSON), "contract_sha256": sha}, indent=2))
    elif args.mode == "score":
        if not args.date:
            raise SystemExit("--date is required for score")
        score(args)
    elif args.mode == "grade":
        if not args.date:
            raise SystemExit("--date is required for grade")
        grade(args)
    elif args.mode == "status":
        status_mode(args)
    elif args.mode == "finalize":
        finalize(args)
    elif args.mode == "dry-run":
        dry_run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
