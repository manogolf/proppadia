"""Prediction-time, append-only MLB semantic lineage support."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

CONTRACT_VERSION = "mlb_prediction_semantic_lineage_v1"
FEATURE_CONTRACT_VERSION = "mlb_prepare_prop_exact_vector_v1"
PROP_CONTRACT_VERSION = "mlb_two_sided_player_prop_v1"
ORIENTATION_CONTRACT = "probability_over=P(actual_value>line); selected_probability=P(selected_side wins)"

MANDATORY = ("run_tag","prediction_timestamp","scheduled_game_start","normal_decision_window","producing_script_path",
 "producing_code_git_commit","dirty_working_tree_status","model_artifact_path","model_artifact_sha256","model_semantic_name",
 "model_semantic_version","feature_schema_sha256","feature_vector_sha256","feature_construction_contract_version",
 "calibration_method","calibration_artifact_path","calibration_artifact_sha256","configuration_sha256",
 "probability_orientation_contract","proposition_contract_version","odds_snapshot_path","odds_snapshot_sha256",
 "odds_snapshot_timestamp","bookmaker_key","price_over_american","price_under_american","selected_side",
 "selected_side_executable_price","selected_side_no_vig_probability","model_selected_side_probability","canonical_row_identity")


def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, bool)): return v
    if isinstance(v, float): return None if not math.isfinite(v) else v
    if isinstance(v, dict): return {str(k): _jsonable(v[k]) for k in sorted(v)}
    if isinstance(v, (list, tuple)): return [_jsonable(x) for x in v]
    return str(v)


def canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def hash_bytes(data: bytes) -> str: return hashlib.sha256(data).hexdigest()
def hash_file(path: Path) -> str: return hash_bytes(path.read_bytes())
def hash_value(value: Any) -> str: return hash_bytes(canonical_json(value).encode())


def git_identity(repo: Path) -> tuple[str, str]:
    try:
        commit = subprocess.run(["git","rev-parse","HEAD"], cwd=repo, check=True, capture_output=True, text=True).stdout.strip()
        dirty = subprocess.run(["git","status","--porcelain"], cwd=repo, check=True, capture_output=True, text=True).stdout
        return commit, "DIRTY" if dirty else "CLEAN"
    except Exception:
        return "", "UNKNOWN"


def model_identity(prop_type: str) -> dict[str,str]:
    path = Path(os.getenv("MODEL_DIR", "/var/data/proppadia/models")) / "latest" / f"{prop_type}.joblib"
    try:
        from backend.mlb.shared.semantic_model_registry import certify_loaded
        ok,status,doc=certify_loaded(prop_type); payload=(doc or {}).get("registration_payload",{})
        if not ok: raise ValueError(status)
        return {"model_artifact_path":str(path.resolve()),"model_artifact_sha256":hash_file(path),
          "model_semantic_name":str(payload["semantic_model_id"]),"model_semantic_version":str(payload["effective_from_timestamp"]),
          "registered_feature_schema_sha256":str(payload["feature_schema_sha256"]),"registered_configuration_sha256":str(payload["configuration_sha256"]),
          "calibration_method":str(payload["calibration_mode"]),"calibration_artifact_path":str(payload.get("calibration_artifact_path") or "NOT_APPLICABLE_DETERMINISTIC_CODE_CALIBRATION"),
          "calibration_artifact_sha256":str((payload.get("calibration_identity") or {}).get("identity_sha256") or "")}
    except Exception:
        return {"model_artifact_path":str(path.resolve()),"model_artifact_sha256":hash_file(path) if path.is_file() else "",
          "model_semantic_name":"UNRECORDED","model_semantic_version":"UNRECORDED"}


def validate(row: dict[str,Any]) -> tuple[str,str]:
    forbidden={"actual_value","outcome","outcome_status","selected_side_outcome","pnl_1u","result","grading_timestamp"}
    present=sorted(k for k in forbidden if k in row)
    if present: return "LINEAGE_BLOCKED_OUTCOME_FIELD","outcome fields forbidden at prediction time: "+",".join(present)
    if not row.get("model_artifact_sha256"): return "LINEAGE_BLOCKED_MISSING_MODEL_HASH","model_artifact_sha256 absent"
    if not row.get("feature_vector_sha256") or not row.get("feature_schema_sha256"): return "LINEAGE_BLOCKED_MISSING_FEATURE_HASH","feature hash absent"
    if not row.get("calibration_method") or not row.get("calibration_artifact_path") or not row.get("calibration_artifact_sha256"): return "LINEAGE_BLOCKED_MISSING_CALIBRATION_IDENTITY","calibration identity absent"
    if not row.get("configuration_sha256"): return "LINEAGE_BLOCKED_MISSING_CONFIG_HASH","configuration_sha256 absent"
    if row.get("registered_feature_schema_sha256") and row.get("feature_schema_sha256") != row.get("registered_feature_schema_sha256"): return "LINEAGE_BLOCKED_FEATURE_SCHEMA_MISMATCH","feature schema differs from active registration"
    if row.get("registered_configuration_sha256") and row.get("configuration_sha256") != row.get("registered_configuration_sha256"): return "LINEAGE_BLOCKED_CONFIGURATION_MISMATCH","configuration differs from active registration"
    if row.get("model_semantic_name") == "UNRECORDED" or row.get("model_semantic_version") == "UNRECORDED": return "LINEAGE_BLOCKED_OTHER_MODEL_SEMANTIC_IDENTITY","semantic model name/version unrecorded"
    if row.get("price_over_american") in (None,"") or row.get("price_under_american") in (None,""): return "LINEAGE_BLOCKED_ODDS_PAIR","same-snapshot two-sided odds absent"
    if not row.get("prediction_timestamp") or not row.get("odds_snapshot_timestamp") or not row.get("scheduled_game_start"): return "LINEAGE_BLOCKED_TIMESTAMP","required timestamp absent"
    try:
        prediction=datetime.fromisoformat(str(row["prediction_timestamp"]).replace("Z","+00:00"))
        start=datetime.fromisoformat(str(row["scheduled_game_start"]).replace("Z","+00:00"))
        if prediction >= start: return "LINEAGE_BLOCKED_EVENT_STARTED","prediction timestamp is not before scheduled start"
    except Exception: return "LINEAGE_BLOCKED_TIMESTAMP","required timestamp is not ISO-8601"
    identity=row.get("canonical_row_identity")
    try: ident=json.loads(identity) if isinstance(identity,str) else identity
    except Exception: ident={}
    if not all(ident.get(k) not in (None,"") for k in ("game_date","game_id","player_id","prop_type","line","selected_side","bookmaker_key","snapshot_run_tag")):
        return "LINEAGE_BLOCKED_IDENTITY","canonical identity incomplete"
    missing=[k for k in MANDATORY if row.get(k) in (None,"")]
    if missing: return "LINEAGE_BLOCKED_OTHER_MANDATORY_FIELD",",".join(missing)
    if row.get("selected_side") not in ("over","under"): return "LINEAGE_BLOCKED_OTHER_SELECTED_SIDE","selected_side invalid"
    return "LINEAGE_CERTIFIED",""


def annotate_distribution_coherence(rows: list[dict[str,Any]]) -> None:
    """Annotate fixed same-snapshot adjacent-line monotonicity without selecting rows."""
    groups: dict[tuple, list[dict[str,Any]]] = {}
    for row in rows:
        ident=json.loads(str(row["canonical_row_identity"]))
        key=(ident["game_date"],ident["game_id"],ident["player_id"],ident["prop_type"],ident["bookmaker_key"],ident["snapshot_run_tag"])
        groups.setdefault(key,[]).append(row)
    for group in groups.values():
        group.sort(key=lambda x: json.loads(str(x["canonical_row_identity"]))["line"])
        if len(group)<2:
            group[0]["distribution_coherence_status"]="NOT_EVALUABLE_SINGLE_LINE"
            continue
        coherent=all(float(group[i]["model_probability_over"]) >= float(group[i+1]["model_probability_over"]) for i in range(len(group)-1))
        for row in group: row["distribution_coherence_status"]="COHERENT" if coherent else "INCOHERENT"


def append_rows(path: Path, rows: Iterable[dict[str,Any]]) -> int:
    rows=list(rows)
    if not rows: return 0
    forbidden={"actual_value","outcome","outcome_status","selected_side_outcome","pnl_1u","result","grading_timestamp"}
    if any(forbidden.intersection(r) for r in rows): raise ValueError("outcome fields forbidden from prediction-time ledger")
    fields=list(rows[0])
    path.parent.mkdir(parents=True,exist_ok=True)
    exists=path.exists() and path.stat().st_size>0
    incoming=[str(r.get("canonical_row_identity") or "") for r in rows]
    if len(set(incoming)) != len(incoming): raise ValueError("duplicate canonical row identity in append batch")
    if exists:
        with path.open(newline="",encoding="utf-8") as f:
            reader=csv.DictReader(f); old=list(reader.fieldnames or []); prior={str(r.get("canonical_row_identity") or "") for r in reader}
        if old != fields: raise ValueError("append-only lineage schema mismatch")
        if prior.intersection(incoming): raise ValueError("append-only lineage duplicate canonical row identity")
    with path.open("a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields)
        if not exists: w.writeheader()
        w.writerows(rows)
    return len(rows)


def utc_now() -> str: return datetime.now(timezone.utc).isoformat()
