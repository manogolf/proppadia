"""Fail-closed production routing for the certified original UBO-5 TB 1.5 model."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import FEATURES, MODEL_SUPPORTED_NULL_FEATURES

ENABLE_FLAG = "MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE"
ARTIFACT_SHA256 = "505bbd44fee7ba5b4331e81692efd0da24afc1ae1e22e2081f6c65e0804d844d"
MODEL_SOURCE = "UBO5_TB15_ESTABLISHED"
FALLBACK_SOURCE = "EXISTING_PRODUCTION"
IDENTITY = ["slate_date", "game_pk", "batter_mlb_id", "prop_type", "line"]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _vector_hash(row: pd.Series) -> str:
    value = "|".join(f"{name}={row[name]!r}" for name in FEATURES)
    return hashlib.sha256(value.encode()).hexdigest()


def route_rows(
    rows: pd.DataFrame,
    *,
    artifact: Path,
    enabled: bool,
    expected_artifact_sha256: str = ARTIFACT_SHA256,
    now_utc: Any = None,
    source_fresh: bool = True,
) -> pd.DataFrame:
    """Return a route ledger; every integrity failure retains production probability."""
    out = rows.copy()
    now = pd.Timestamp(now_utc or pd.Timestamp.now(tz="UTC"))
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    defaults = {
        "prop_type": "", "line": np.nan, "strict_prior_pa": np.nan,
        "starter_certification": "", "lineup_certified_at_utc": pd.NaT,
        "prediction_timestamp_utc": pd.NaT, "scheduled_start_utc": pd.NaT,
        "production_prob_over": np.nan, "batter_identity_certified": False,
        "identity_ambiguous": True, "source_lineage_pointer": "",
        "production_artifact_hash": "", "latest_included_event_date": pd.NaT,
    }
    for name, value in defaults.items():
        if name not in out:
            out[name] = value
    for name in IDENTITY:
        if name not in out:
            out[name] = np.nan
    for name in ("prediction_timestamp_utc", "scheduled_start_utc", "lineup_certified_at_utc"):
        out[name] = pd.to_datetime(out[name], utc=True, errors="coerce")

    out["route_flag_enabled"] = bool(enabled)
    out["route_eligibility"] = False
    out["exclusion_reason"] = ""
    out["ubo5_probability_over"] = np.nan
    out["existing_production_probability"] = pd.to_numeric(out["production_prob_over"], errors="coerce")
    out["active_probability"] = out["existing_production_probability"]
    out["probability_delta"] = 0.0
    out["model_source"] = FALLBACK_SOURCE
    out["active_artifact_sha256"] = ""
    out["feature_schema_sha256"] = hashlib.sha256("\n".join(FEATURES).encode()).hexdigest()
    out["feature_vector_sha256"] = ""
    out["artifact_hash_status"] = "NOT_CHECKED"
    out["feature_completeness_status"] = "INCOMPLETE"
    out["temporal_integrity_status"] = "FAIL"
    out["missing_feature_count"] = 0
    out["exact_missing_features"] = ""
    out["primary_fallback_category"] = ""
    out["secondary_fallback_details"] = ""
    out["source_state"] = "CERTIFIED_FRESH"
    out["builder_stage"] = "production_route"
    out["repair_possible"] = False
    out["legitimate_permanent_fallback"] = False

    reasons = pd.Series("", index=out.index, dtype=object)
    def reject(mask: pd.Series, reason: str) -> None:
        target = mask & reasons.eq("")
        reasons.loc[target] = reason

    reject(~out["prop_type"].eq("total_bases"), "UNSUPPORTED_PROP_TYPE")
    reject(~pd.to_numeric(out["line"], errors="coerce").eq(1.5), "UNSUPPORTED_LINE")
    reject(~out["batter_identity_certified"].fillna(False).astype(bool), "IDENTITY_NOT_CERTIFIED")
    reject(out["identity_ambiguous"].fillna(True).astype(bool), "AMBIGUOUS_IDENTITY")
    reject(~out["starter_certification"].eq("CERTIFIED_PREGAME_STARTER"), "LINEUP_NOT_CERTIFIED")
    reject(out["lineup_certified_at_utc"].isna() | (out["lineup_certified_at_utc"] > out["prediction_timestamp_utc"]), "LINEUP_CERTIFICATION_NOT_PREGAME")
    reject(pd.to_numeric(out["strict_prior_pa"], errors="coerce").lt(100) | out["strict_prior_pa"].isna(), "STRICT_PRIOR_PA_BELOW_100")
    missing_required = pd.Series(False, index=out.index)
    for feature in FEATURES:
        if feature not in out:
            missing_required[:] = True
    if all(feature in out for feature in FEATURES):
        missing_by_row = out[FEATURES].isna()
        out["missing_feature_count"] = missing_by_row.sum(axis=1)
        out["exact_missing_features"] = missing_by_row.apply(
            lambda row: "|".join(row.index[row].tolist()), axis=1
        )
        required = [feature for feature in FEATURES if feature not in MODEL_SUPPORTED_NULL_FEATURES]
        missing_required |= out[required].isna().any(axis=1)
    reject(missing_required, "MISSING_REQUIRED_NON_NULL_FEATURES")
    reject(pd.Series(not source_fresh, index=out.index), "STALE_SOURCE")
    latest_event = pd.to_datetime(out.get("latest_included_event_date"), utc=True, errors="coerce")
    slate_day = pd.to_datetime(out["slate_date"], utc=True, errors="coerce")
    reject(
        latest_event.isna() | slate_day.isna() | (latest_event >= slate_day) | ((slate_day - latest_event).dt.days > 3),
        "STALE_OR_NON_PRIOR_SOURCE_EVENTS",
    )
    reject(out["prediction_timestamp_utc"].isna() | out["scheduled_start_utc"].isna() | (out["prediction_timestamp_utc"] >= out["scheduled_start_utc"]), "PREDICTION_NOT_BEFORE_FIRST_PITCH")
    reject(out["scheduled_start_utc"] <= now, "GAME_ALREADY_STARTED")
    duplicates = out.duplicated(IDENTITY, keep=False)
    reject(duplicates, "DUPLICATE_CANONICAL_IDENTITY")
    reject(~np.isfinite(out["existing_production_probability"]), "INVALID_EXISTING_PRODUCTION_PROBABILITY")
    if not enabled:
        reject(reasons.eq(""), "ROUTE_DISABLED")

    artifact_ok = artifact.is_file() and sha256_file(artifact) == expected_artifact_sha256
    out["artifact_hash_status"] = "PASS" if artifact_ok else "FAIL"
    if not artifact_ok:
        reject(reasons.eq(""), "ARTIFACT_MISSING_OR_HASH_MISMATCH")

    eligible = reasons.eq("")
    if eligible.any():
        try:
            bundle = joblib.load(artifact)
            if list(bundle.get("features", [])) != list(FEATURES):
                reject(eligible, "FEATURE_ORDER_MISMATCH")
            else:
                indicator_features = list(bundle["model"].named_steps["simpleimputer"].indicator_.features_)
                expected_indicators = [FEATURES.index(name) for name in MODEL_SUPPORTED_NULL_FEATURES]
                if indicator_features != expected_indicators:
                    reject(eligible, "FROZEN_NULL_INDICATOR_CONTRACT_MISMATCH")
                    raise ValueError("frozen null indicator contract mismatch")
                probs = bundle["model"].predict_proba(out.loc[eligible, FEATURES])
                classes = list(bundle["model"].classes_)
                p = pd.Series(
                    [1.0 - dict(zip(classes, row)).get(0, 0.0) - dict(zip(classes, row)).get(1, 0.0) for row in probs],
                    index=out.index[eligible],
                )
                invalid = ~np.isfinite(p) | ~p.between(0.0, 1.0)
                reject(eligible & out.index.to_series().isin(p.index[invalid]), "INVALID_UBO5_PROBABILITY")
                routed = eligible & reasons.eq("")
                out.loc[routed, "ubo5_probability_over"] = p.loc[routed]
                out.loc[routed, "active_probability"] = p.loc[routed]
                out.loc[routed, "probability_delta"] = p.loc[routed] - out.loc[routed, "existing_production_probability"]
                out.loc[routed, "model_source"] = MODEL_SOURCE
                out.loc[routed, "active_artifact_sha256"] = expected_artifact_sha256
                out.loc[routed, "feature_vector_sha256"] = out.loc[routed].apply(_vector_hash, axis=1)
                supported_nulls = routed & out["missing_feature_count"].gt(0)
                out.loc[routed, "feature_completeness_status"] = "COMPLETE"
                out.loc[supported_nulls, "feature_completeness_status"] = "COMPLETE_WITH_MODEL_SUPPORTED_NULLS"
                out.loc[routed, "temporal_integrity_status"] = "PASS"
                out.loc[routed, "route_eligibility"] = True
        except Exception:
            reject(eligible, "ARTIFACT_LOAD_OR_SCORING_FAILURE")
    out["exclusion_reason"] = reasons
    out.loc[out["exclusion_reason"].eq("MISSING_REQUIRED_NON_NULL_FEATURES"), "primary_fallback_category"] = "G_BUILDER_OR_REQUIRED_FEATURE_DEFECT"
    out.loc[out["exclusion_reason"].eq("MISSING_REQUIRED_NON_NULL_FEATURES"), "secondary_fallback_details"] = out["exact_missing_features"]
    out.loc[out["exclusion_reason"].eq("MISSING_REQUIRED_NON_NULL_FEATURES"), "repair_possible"] = True
    out.loc[out["exclusion_reason"].eq("STALE_OR_NON_PRIOR_SOURCE_EVENTS"), "primary_fallback_category"] = "F_STALE_CERTIFIED_SOURCE"
    out.loc[out["exclusion_reason"].eq("STALE_SOURCE"), "primary_fallback_category"] = "F_STALE_CERTIFIED_SOURCE"
    out.loc[out["exclusion_reason"].eq("STRICT_PRIOR_PA_BELOW_100"), "primary_fallback_category"] = "B_INSUFFICIENT_STRICT_PRIOR_HISTORY"
    out.loc[out["exclusion_reason"].eq("STRICT_PRIOR_PA_BELOW_100"), "legitimate_permanent_fallback"] = True
    out.loc[out["exclusion_reason"].str.contains("IDENTITY", na=False), "primary_fallback_category"] = "D_JOIN_OR_IDENTITY_DEFECT"
    out.loc[out["exclusion_reason"].str.contains("LINEUP", na=False), "primary_fallback_category"] = "C_CANDIDATE_SHELL_PROPAGATION_DEFECT"
    out.loc[out["exclusion_reason"].str.contains("ARTIFACT|FEATURE_ORDER|PROBABILITY", na=False), "primary_fallback_category"] = "G_BUILDER_IMPLEMENTATION_DEFECT"
    out.loc[out["exclusion_reason"].ne("") & out["primary_fallback_category"].eq(""), "primary_fallback_category"] = "H_TRULY_UNAVAILABLE_OR_UNSUPPORTED"
    out.loc[out["route_eligibility"], "exclusion_reason"] = ""
    return out
