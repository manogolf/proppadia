"""Promotion evidence and reversible Points authority for NHL HGB v1."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
GATE_PATH = ROOT / "artifacts/analysis/nhl/points_hgb_operational_bridge/2026-10-09/operational_promotion_gates.json"
AUTHORITY_PATH = ROOT / "artifacts/operational/nhl/points_model_authority/authority_v1.json"
INTEGRATION_EVIDENCE = ROOT / "artifacts/analysis/nhl/points_hgb_operational_bridge/2026-10-09/routine_daily_integration_evidence.json"
VALID_AUTHORITY = {"phoenix_v2", "NHL_POINTS_COUNT_HGB_V1"}
HGB_AUTHORITY = "NHL_POINTS_COUNT_HGB_V1"
PHOENIX_AUTHORITY = "phoenix_v2"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def selected_production_authority(path: Path = AUTHORITY_PATH) -> dict[str, Any]:
    state = json.loads(Path(path).read_text())
    if state.get("schema_version") != "NHL_POINTS_MODEL_AUTHORITY_V1":
        raise ValueError("POINTS_AUTHORITY_SCHEMA_INVALID")
    selected = state.get("production_authority")
    if selected not in VALID_AUTHORITY:
        raise ValueError("POINTS_AUTHORITY_UNKNOWN")
    return state


def next_authority_state(state: dict[str, Any], selected: str) -> dict[str, Any]:
    """Return an explicit next version without mutating the source state."""
    if selected not in VALID_AUTHORITY:
        raise ValueError("POINTS_AUTHORITY_UNKNOWN")
    if state.get("schema_version") != "NHL_POINTS_MODEL_AUTHORITY_V1":
        raise ValueError("POINTS_AUTHORITY_SCHEMA_INVALID")
    updated = dict(state)
    updated["authority_version"] = int(state.get("authority_version", 0)) + 1
    updated["production_authority"] = selected
    updated["shadow_authorities"] = [
        PHOENIX_AUTHORITY if selected == HGB_AUTHORITY else HGB_AUTHORITY
    ]
    updated["production_changed"] = selected != state.get("production_authority")
    return updated


def assert_hgb_promotion_ready(*, evaluator=None) -> dict[str, Any]:
    """Fail closed when HGB is selected before all operational gates are proven."""
    if evaluator is None:
        evaluator = evaluate_promotion()
    if evaluator.get("classification") != "READY_FOR_HGB_PRODUCTION_PROMOTION":
        raise RuntimeError("HGB_AUTHORITY_SELECTED_BEFORE_PROMOTION_GATES_PASS")
    return evaluator


def evaluate_promotion(*, gate_path: Path = GATE_PATH,
                       integration_evidence_path: Path = INTEGRATION_EVIDENCE,
                       shadow_root: Path = ROOT / "artifacts/operational/nhl/points_hgb_shadow") -> dict[str, Any]:
    gates = json.loads(Path(gate_path).read_text())
    statuses = {key: value.get("status", "PENDING") for key, value in gates.get("gates", {}).items()}
    for key in "ABCDEHI":
        entry = gates.get("gates", {}).get(key, {})
        if statuses.get(key) != "PASS" or not str(entry.get("evidence", "")).strip():
            statuses[key] = "PENDING"
    if set(statuses) != set("ABCDEFGHIJ"):
        statuses = {key: statuses.get(key, "PENDING") for key in "ABCDEFGHIJ"}
    proof = None
    if Path(integration_evidence_path).is_file():
        proof = json.loads(Path(integration_evidence_path).read_text())
        helper_sha = sha(ROOT / "backend/nhl/points_hgb_shadow.py")
        if (proof.get("classification") == "HGB_ROUTINE_DAILY_INTEGRATION_PASS"
                and proof.get("deterministic_replay") == "DETERMINISTIC_REPLAY_PASS"
                and proof.get("coherence_crossing_count") == 0
                and proof.get("normal_daily_entrypoint") == "python -m backend.nhl.cli daily --with-odds"
                and proof.get("production_orchestration_helper_sha256") == helper_sha
                and proof.get("daily_cli_integration_sha256") == sha(ROOT / "backend/nhl/cli.py")
                and proof.get("feature_exporter_sha256") == sha(ROOT / "backend/nhl/scripts/export_nhl_points_hgb_features.py")
                and proof.get("scorer_sha256") == sha(ROOT / "backend/nhl/scripts/score_nhl_points_hgb_shadow.py")
                and proof.get("model_artifact_sha256") == sha(ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/nhl_points_count_hgb_v1.joblib")):
            statuses["J"] = "PASS"
    grade_evidence = []
    for path in Path(shadow_root).glob("grades/season=*/slate_date=*/capture=*/grade=*/grade.json"):
        try:
            grade = json.loads(path.read_text())
            from backend.nhl.daily_capture import verify_package
            grade_package = path.parent
            verify_package(grade_package)
            from backend.nhl.postgame_learning import verify_reconciliation_package
            reconciliation_path = Path(grade["reconciliation_package"])
            verify_reconciliation_package(reconciliation_path, str(grade["slate_date"]))
            if sha(reconciliation_path / "SHA256SUMS") != grade.get("reconciliation_manifest_sha256"):
                continue
            if (grade.get("status") == "COMPLETE" and grade.get("official_outcomes_status") == "FINAL"
                    and grade.get("official_outcome_sha256") and grade.get("reconciliation_manifest_sha256")
                    and grade.get("hgb_prediction_sha256") and grade.get("official_outcome_identity")):
                grade_evidence.append((str(grade.get("slate_date", "")), path, grade))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    if grade_evidence:
        latest = max(grade_evidence, key=lambda item: item[0])
        statuses["F"] = "PASS"
        statuses["G"] = "PASS"
    else:
        # The ledger's old text alone cannot stand in for prospective outcome
        # reconciliation and immutable HGB grading artifacts.
        statuses["F"] = "PENDING"
        statuses["G"] = "PENDING"
    ready = all(statuses[k] == "PASS" for k in "ABCDEFGHIJ")
    return {"schema_version": "NHL_POINTS_HGB_PROMOTION_EVALUATION_V1",
            "classification": "READY_FOR_HGB_PRODUCTION_PROMOTION" if ready else "NOT_READY_FOR_HGB_PRODUCTION_PROMOTION",
            "gates": {k: {"status": statuses[k]} for k in "ABCDEFGHIJ"},
            "evidence": {"promotion_gate_ledger_sha256": sha(gate_path),
                         "routine_integration_evidence_sha256": sha(integration_evidence_path)
                         if Path(integration_evidence_path).is_file() else None,
                         "latest_hgb_grade_path": str(latest[1]) if grade_evidence else None,
                         "latest_hgb_grade_sha256": sha(latest[1]) if grade_evidence else None},
            "promotion_authorized": bool(ready), "promotion_performed": False}
