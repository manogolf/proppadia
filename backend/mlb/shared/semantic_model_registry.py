"""Forward-only immutable semantic identities for current MLB model artifacts."""
from __future__ import annotations
import hashlib, json, os
from pathlib import Path
from typing import Any

REPO_ROOT=Path(__file__).resolve().parents[3]
ACTIVE_POINTER=REPO_ROOT/"backend/mlb/config/semantic_models/active_registry.json"

def canonical_json(v: Any)->str: return json.dumps(v,sort_keys=True,separators=(",",":"),ensure_ascii=False)
def hash_value(v: Any)->str: return hashlib.sha256(canonical_json(v).encode()).hexdigest()
def hash_file(p: Path)->str: return hashlib.sha256(p.read_bytes()).hexdigest()
def effective_inference_config()->dict[str,Any]:
    def flag(name,default): return str(os.getenv(name,default)).strip().lower() in {"1","true","yes","on"}
    return {
      "model_dir":str(Path(os.getenv("MODEL_DIR","/var/data/proppadia/models")).resolve()),
      "blend_strategy":"auc_weighted_artifact_meta_auc_minus_0.5_nonnegative_equal_mean_if_zero",
      "line_sensitivity_enabled":flag("MLB_LINE_SENSITIVITY_CORRECTION_ENABLED","1"),
      "line_sensitivity_alpha":float(os.getenv("MLB_LINE_SENSITIVITY_ALPHA") or 0.90),
      "forced_invert_props":sorted(x.strip().lower() for x in str(os.getenv("MLB_FORCE_INVERT_PROPS","")).split(",") if x.strip()),
      "upload_external_calibration_enabled":flag("MLB_APPLY_PROBABILITY_CALIBRATION_TO_UPLOAD","0"),
      "require_two_sided":flag("MLB_PREDICT_REQUIRE_TWO_SIDED","1"),
      "two_sided_bookmaker":str(os.getenv("MLB_PREDICT_TWO_SIDED_BOOKMAKER","")).strip(),
      "pitcher_min_starter_games":int(os.getenv("MLB_PITCHER_STARTER_MIN_GAMES") or 5),
      "pitcher_min_starter_outs":int(os.getenv("MLB_PITCHER_STARTER_MIN_OUTS") or 1),
      "pitcher_require_probable_starter":flag("MLB_PITCHER_REQUIRE_PROBABLE_STARTER","1"),
    }
def load_active_registry(pointer: Path=ACTIVE_POINTER)->dict:
    p=json.loads(pointer.read_text()); registry=(pointer.parent/p["active_registry_path"]).resolve()
    if hash_file(registry)!=p["active_registry_sha256"]: raise ValueError("active semantic registry hash mismatch")
    return json.loads(registry.read_text())
def active_manifest(prop: str, pointer: Path=ACTIVE_POINTER)->dict:
    reg=load_active_registry(pointer); entry=next((x for x in reg["entries"] if x["proposition"]==prop),None)
    if not entry: raise KeyError(f"unregistered proposition: {prop}")
    path=(pointer.parent/entry["semantic_manifest_path"]).resolve()
    if hash_file(path)!=entry["semantic_manifest_sha256"]: raise ValueError("semantic manifest hash mismatch")
    doc=json.loads(path.read_text()); payload=doc["registration_payload"]
    if hash_value(payload)!=doc["semantic_registration_manifest_sha256"]: raise ValueError("semantic registration payload hash mismatch")
    return doc
def certify_loaded(prop: str, pointer: Path=ACTIVE_POINTER)->tuple[bool,str,dict]:
    try: doc=active_manifest(prop,pointer)
    except Exception as e: return False,f"UNREGISTERED_OR_INVALID:{e}",{}
    payload=doc["registration_payload"]; loaded=Path(payload["loaded_model_artifact_path"])
    if not loaded.is_file() or hash_file(loaded)!=payload["loaded_artifact_sha256"]: return False,"ARTIFACT_HASH_MISMATCH",doc
    if hash_value(payload["required_feature_order"])!=payload["feature_schema_sha256"]: return False,"FEATURE_SCHEMA_MISMATCH",doc
    if hash_value(effective_inference_config())!=payload["configuration_sha256"]: return False,"CONFIGURATION_MISMATCH",doc
    cal=payload.get("calibration_identity") or {}
    if payload.get("calibration_mode") not in {"EXTERNAL_CALIBRATION_ARTIFACT","EMBEDDED_IN_MODEL_ARTIFACT","DETERMINISTIC_CODE_CALIBRATION","IDENTITY_NO_CALIBRATION"} or not cal.get("identity_sha256"): return False,"MISSING_CALIBRATION_IDENTITY",doc
    return True,"LINEAGE_CERTIFIED",doc
