from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


MODEL_PATH = Path("models_out/latest/hits_05_full_spine.joblib")
FEATURE_MANIFEST_PATH = Path("models_out/latest/hits_05_full_spine_feature_manifest.json")
METADATA_PATH = Path("models_out/latest/hits_05_full_spine_metadata.json")
PARENT_ROOT = Path("artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer")

EXPECTED_MODEL_SHA256 = "4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b"

PROVENANCE_COLUMNS = [
    "hits05_route",
    "hits05_artifact",
    "hits05_artifact_sha256",
    "hits05_feature_manifest_sha256",
    "hits05_parent_run_tag",
    "hits05_lineup_status",
    "hits05_starter_status",
    "hits05_feature_completeness",
    "hits05_fallback_reason",
    "hits05_raw_candidate_probability",
    "hits05_incumbent_probability",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _falsey(value: object) -> bool:
    return str(value or "").strip().lower() in {"0", "false", "no", "n", "off"}


def _load_metadata() -> Dict[str, object]:
    if not METADATA_PATH.exists():
        return {}
    try:
        with METADATA_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def replacement_enabled() -> bool:
    explicit = os.environ.get("MLB_ENABLE_HITS05_FULL_SPINE_REPLACEMENT")
    if explicit is not None:
        return not _falsey(explicit)
    if _truthy(os.environ.get("MLB_DISABLE_HITS05_FULL_SPINE_REPLACEMENT")):
        return False
    meta = _load_metadata()
    if "enabled" in meta:
        return bool(meta.get("enabled"))
    return False


def _latest_parent_scores_path(slate_date: str) -> Optional[Path]:
    day_dir = PARENT_ROOT / str(slate_date)
    run_tag = str(os.environ.get("MLB_RUN_TAG") or "").strip()
    candidates = []
    if run_tag:
        candidates.extend(day_dir.glob(f"{run_tag}/hits05_scored_current_rows_{slate_date}.csv"))
    candidates.extend(day_dir.glob(f"hits05_scored_current_rows_{slate_date}.csv"))
    candidates.extend(day_dir.glob(f"**/hits05_scored_current_rows_{slate_date}.csv"))
    candidates = [p for p in candidates if p.exists()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: (p.stat().st_mtime, str(p)))[-1]


def _latest_parent_machine_path(slate_date: str) -> Optional[Path]:
    day_dir = PARENT_ROOT / str(slate_date)
    run_tag = str(os.environ.get("MLB_RUN_TAG") or "").strip()
    candidates = []
    if run_tag:
        candidates.extend(day_dir.glob(f"{run_tag}/machine_readable_hits05_current_nonmarket_parent_producer_{slate_date}.json"))
    candidates.extend(day_dir.glob(f"machine_readable_hits05_current_nonmarket_parent_producer_{slate_date}.json"))
    candidates.extend(day_dir.glob(f"**/machine_readable_hits05_current_nonmarket_parent_producer_{slate_date}.json"))
    candidates = [p for p in candidates if p.exists()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: (p.stat().st_mtime, str(p)))[-1]


def _read_parent_machine(slate_date: str) -> tuple[Optional[Path], Dict[str, Any]]:
    path = _latest_parent_machine_path(slate_date)
    if path is None:
        return None, {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return path, {"parent_artifact_state": "PARENT_ARTIFACT_MALFORMED"}
    if not isinstance(data, dict):
        return path, {"parent_artifact_state": "PARENT_ARTIFACT_MALFORMED"}
    if str(data.get("date") or "").strip() != str(slate_date):
        data["parent_artifact_state"] = "PARENT_SLATE_DATE_MISMATCH"
    run_tag = str(os.environ.get("MLB_RUN_TAG") or "").strip()
    if run_tag and str(data.get("run_tag") or "").strip() not in {"", run_tag}:
        data["parent_artifact_state"] = "PARENT_RUN_TAG_MISMATCH"
    return path, data


def _paired_artifact_path(scores_path: Path, *, slate_date: str, stem: str) -> Optional[Path]:
    candidate = scores_path.with_name(f"{stem}_{slate_date}.csv")
    if candidate.exists():
        return candidate
    day_dir = PARENT_ROOT / str(slate_date)
    matches = [p for p in day_dir.glob(f"**/{stem}_{slate_date}.csv") if p.exists()]
    if not matches:
        return None
    return sorted(matches, key=lambda p: (p.stat().st_mtime, str(p)))[-1]


def _load_parent_scores(slate_date: str) -> tuple[Optional[Path], Dict[tuple[int, int], Dict[str, object]]]:
    path = _latest_parent_scores_path(slate_date)
    if path is None:
        return None, {}
    try:
        frame = pd.read_csv(path)
    except Exception:
        return path, {}
    needed = {"game_id", "player_id", "probability_at_least_one_hit"}
    if not needed.issubset(set(frame.columns)):
        return path, {}
    rows: Dict[tuple[int, int], Dict[str, object]] = {}
    for _, row in frame.iterrows():
        try:
            key = (int(row["game_id"]), int(row["player_id"]))
            prob = float(row["probability_at_least_one_hit"])
        except Exception:
            continue
        if not (0.0 < prob < 1.0):
            continue
        rows[key] = row.to_dict()
    return path, rows


def _load_withheld_reasons(scores_path: Optional[Path], slate_date: str) -> Dict[tuple[int, int], str]:
    if scores_path is None:
        return {}
    path = _paired_artifact_path(scores_path, slate_date=slate_date, stem="hits05_withheld_ledger")
    if path is None:
        return {}
    try:
        frame = pd.read_csv(path)
    except Exception:
        return {}
    out: Dict[tuple[int, int], str] = {}
    for _, row in frame.iterrows():
        try:
            key = (int(row["game_id"]), int(row["player_id"]))
        except Exception:
            continue
        reason = str(row.get("withheld_reason") or "").strip()
        if reason:
            out[key] = reason
    return out


def _fallback_route_from_reason(reason: object) -> str:
    text = str(reason or "").strip().upper()
    if "STARTER" in text:
        return "HITS05_INCUMBENT_FALLBACK_NO_STARTER"
    if "FEATURE" in text:
        return "HITS05_INCUMBENT_FALLBACK_FEATURE_INCOMPLETE"
    if "LINEUP" in text or "NOT_ELIGIBLE" in text:
        return "HITS05_INCUMBENT_FALLBACK_NO_LINEUP"
    if "IDENTITY" in text or "DUPLICATE" in text:
        return "HITS05_INCUMBENT_FALLBACK_PARENT_IDENTITY_FAILED"
    if "MODEL" in text or "ARTIFACT" in text or "SHA" in text:
        return "HITS05_INCUMBENT_FALLBACK_MODEL_LOAD_FAILED"
    return "HITS05_INCUMBENT_FALLBACK_PARENT_NOT_YET_GENERATED"


def _fallback_reason_from_parent_state(state: str) -> str:
    if state == "PARENT_ARTIFACT_ZERO_VALID_NO_LINEUPS":
        return "HITS05_INCUMBENT_FALLBACK_NO_LINEUP"
    if state == "PARENT_ARTIFACT_ZERO_VALID_NO_STARTERS":
        return "HITS05_INCUMBENT_FALLBACK_NO_STARTER"
    if state == "PARENT_ARTIFACT_ZERO_VALID_NO_ELIGIBLE_GAMES":
        return "HITS05_INCUMBENT_FALLBACK_NO_ELIGIBLE_GAMES"
    if state == "PARENT_RUN_TAG_MISMATCH":
        return "HITS05_CURRENT_PARENT_RUN_TAG_MISMATCH"
    if state == "PARENT_SLATE_DATE_MISMATCH":
        return "HITS05_CURRENT_PARENT_SLATE_DATE_MISMATCH"
    if state == "PARENT_ARTIFACT_MALFORMED":
        return "HITS05_CURRENT_PARENT_ARTIFACT_MALFORMED"
    return "HITS05_CURRENT_PARENT_SCORE_MISSING"


def apply_hits05_replacement(df_long: pd.DataFrame, *, slate_date: str) -> pd.DataFrame:
    out = df_long.copy()
    for col in PROVENANCE_COLUMNS:
        if col not in out.columns:
            out[col] = None

    is_hits05 = (
        out.get("prop_type", pd.Series(index=out.index, dtype=object)).astype(str).str.strip().str.lower().eq("hits")
        & (pd.to_numeric(out.get("line", pd.Series(index=out.index, dtype=object)), errors="coerce") == 0.5)
    )
    if not bool(is_hits05.any()):
        return out

    out.loc[~is_hits05, "hits05_route"] = "INCUMBENT_PRESERVED_NOT_HITS_0_5"
    out.loc[is_hits05, "hits05_incumbent_probability"] = pd.to_numeric(out.loc[is_hits05, "prob_over"], errors="coerce")

    if not replacement_enabled():
        out.loc[is_hits05, "hits05_route"] = "HITS05_INCUMBENT_FALLBACK_PARENT_NOT_YET_GENERATED"
        out.loc[is_hits05, "hits05_fallback_reason"] = "HITS05_REPLACEMENT_DISABLED"
        return out

    if not MODEL_PATH.exists() or not FEATURE_MANIFEST_PATH.exists() or not METADATA_PATH.exists():
        out.loc[is_hits05, "hits05_route"] = "HITS05_INCUMBENT_FALLBACK_MODEL_LOAD_FAILED"
        out.loc[is_hits05, "hits05_fallback_reason"] = "HITS05_PRODUCTION_ARTIFACT_MISSING"
        return out

    try:
        artifact_sha = sha256_file(MODEL_PATH)
        manifest_sha = sha256_file(FEATURE_MANIFEST_PATH)
    except Exception:
        out.loc[is_hits05, "hits05_route"] = "HITS05_INCUMBENT_FALLBACK_MODEL_LOAD_FAILED"
        out.loc[is_hits05, "hits05_fallback_reason"] = "HITS05_ARTIFACT_HASH_UNAVAILABLE"
        return out

    if artifact_sha != EXPECTED_MODEL_SHA256:
        out.loc[is_hits05, "hits05_route"] = "HITS05_INCUMBENT_FALLBACK_MODEL_LOAD_FAILED"
        out.loc[is_hits05, "hits05_fallback_reason"] = "HITS05_ARTIFACT_SHA_MISMATCH"
        return out

    parent_machine_path, parent_machine = _read_parent_machine(slate_date)
    parent_path, parent_rows = _load_parent_scores(slate_date)
    withheld_reasons = _load_withheld_reasons(parent_path, slate_date)
    if not parent_rows:
        parent_state = str(parent_machine.get("parent_artifact_state") or "").strip()
        if not parent_state and parent_path:
            parent_state = "PARENT_ARTIFACT_ZERO_VALID_NO_ELIGIBLE_GAMES"
        if parent_state == "PARENT_ARTIFACT_ZERO_VALID_NO_LINEUPS":
            route = "HITS05_INCUMBENT_FALLBACK_NO_LINEUP"
        elif parent_state == "PARENT_ARTIFACT_ZERO_VALID_NO_STARTERS":
            route = "HITS05_INCUMBENT_FALLBACK_NO_STARTER"
        elif parent_path or parent_machine_path:
            route = "HITS05_INCUMBENT_FALLBACK_PARENT_NOT_YET_GENERATED"
        else:
            route = "HITS05_INCUMBENT_FALLBACK_PARENT_NOT_YET_GENERATED"
        out.loc[is_hits05, "hits05_route"] = route
        out.loc[is_hits05, "hits05_artifact"] = str(MODEL_PATH)
        out.loc[is_hits05, "hits05_artifact_sha256"] = artifact_sha
        out.loc[is_hits05, "hits05_feature_manifest_sha256"] = manifest_sha
        out.loc[is_hits05, "hits05_parent_run_tag"] = parent_machine.get("run_tag")
        out.loc[is_hits05, "hits05_lineup_status"] = parent_state
        out.loc[is_hits05, "hits05_fallback_reason"] = (
            _fallback_reason_from_parent_state(parent_state) if (parent_path or parent_machine_path)
            else "HITS05_CURRENT_PARENT_ARTIFACT_MISSING"
        )
        return out

    for idx, row in out.loc[is_hits05].iterrows():
        try:
            key = (int(row["game_id"]), int(row["player_id"]))
        except Exception:
            out.at[idx, "hits05_route"] = "HITS05_INCUMBENT_FALLBACK_PARENT_IDENTITY_FAILED"
            out.at[idx, "hits05_fallback_reason"] = "HITS05_ROW_KEY_INVALID"
            continue
        parent = parent_rows.get(key)
        if not parent:
            withheld_reason = withheld_reasons.get(key) or "HITS05_NO_CERTIFIED_CURRENT_PARENT_SCORE"
            out.at[idx, "hits05_route"] = _fallback_route_from_reason(withheld_reason)
            out.at[idx, "hits05_artifact"] = str(MODEL_PATH)
            out.at[idx, "hits05_artifact_sha256"] = artifact_sha
            out.at[idx, "hits05_feature_manifest_sha256"] = manifest_sha
            out.at[idx, "hits05_fallback_reason"] = withheld_reason
            continue
        candidate_prob = float(parent["probability_at_least_one_hit"])
        out.at[idx, "prob_over"] = candidate_prob
        out.at[idx, "hits05_route"] = "HITS05_FULL_SPINE_CANDIDATE"
        out.at[idx, "hits05_artifact"] = str(MODEL_PATH)
        out.at[idx, "hits05_artifact_sha256"] = artifact_sha
        out.at[idx, "hits05_feature_manifest_sha256"] = manifest_sha
        out.at[idx, "hits05_parent_run_tag"] = parent.get("parent_run_tag")
        out.at[idx, "hits05_lineup_status"] = parent.get("lineup_status")
        out.at[idx, "hits05_starter_status"] = "RESOLVED" if parent.get("opposing_starter_id") else "UNRESOLVED"
        out.at[idx, "hits05_feature_completeness"] = parent.get("feature_completeness_status")
        out.at[idx, "hits05_fallback_reason"] = ""
        out.at[idx, "hits05_raw_candidate_probability"] = candidate_prob

    return out
