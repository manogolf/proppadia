#!/usr/bin/env python3
"""Build canonical MLB-CC-0001 pregame input manifests.

This utility is artifact-only:
- no training
- no model writes
- no uploads
- no OddsAPI calls
- no database writes

It reuses the production Hits feature vectorizer to materialize the frozen
control feature list, then appends only the four approved PA challenger fields.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.mlb.prediction.make_prediction import _vectorize


ROOT = Path(__file__).resolve().parents[3]
SCHEMA_VERSION = "mlb_cc_0001_pregame_manifest.v1"
EXPERIMENT_ID = "MLB-CC-0001"
EXEC_DIR = ROOT / "artifacts/analysis/model_development/mlb_cc_0001_execution_2026-07-10"
EXT_DIR = ROOT / "artifacts/analysis/model_development/mlb_cc_0001_prospective_extension"
AUDIT_DIR = ROOT / "artifacts/analysis/model_development/mlb_cc_0001_pregame_manifest_generator_2026-07-10"
DEFAULT_MANIFEST_ROOT = EXT_DIR / "pregame_manifests"

CONTROL_FEATURE_MANIFEST = EXEC_DIR / "mlb_cc_0001_control_feature_manifest_2026-07-10.csv"
CHALLENGER_FEATURE_MANIFEST = EXEC_DIR / "mlb_cc_0001_challenger_feature_manifest_2026-07-10.csv"
CONTROL_MODEL = EXEC_DIR / "model_artifacts/mlb_cc_0001_control.joblib"
CHALLENGER_MODEL = EXEC_DIR / "model_artifacts/mlb_cc_0001_pa_challenger.joblib"
CONTRACT_JSON = EXT_DIR / "mlb_cc_0001_prospective_extension_contract.json"
CONTRACT_SHA = EXT_DIR / "mlb_cc_0001_prospective_extension_contract.sha256"

CONTROL_SHA = "1acbc1ee25372cd7779752674998a85e1ae856539bb00ff74d727d5049c072ef"
CHALLENGER_SHA = "d79f57cce162ecefb014f6bfa30d408b78e295eb66054f4203dd8d5b4934dc77"
PA_FEATURES = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_missing_flag",
]
IDENTITY_COLUMNS = [
    "slate_date",
    "game_date",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "line",
    "side",
    "market_price",
    "bookmaker",
]
METADATA_COLUMNS = [
    "experiment_id",
    "generator_schema_version",
    "manifest_run_tag",
    "manifest_frozen_timestamp_utc",
    "pregame_freeze_status",
    "snapshot_frozen_before_first_pitch",
    "earliest_first_pitch_utc",
    "pa_context_latest_date",
    "pa_feature_source_status",
    "feature_cutoff_verified",
    "canonical_vectorizer",
    "source_feature_path",
    "pa_context_source_path",
    "row_hash",
]
READINESS_VALUES = {
    "GENERATOR_READY_FOR_MANUAL_PREGAME_USE",
    "BLOCKED_BY_CANONICAL_FEATURE_ASSEMBLY",
    "BLOCKED_BY_MISSING_CONTROL_FEATURES",
    "BLOCKED_BY_FEATURE_PARITY",
    "BLOCKED_BY_PREDICTION_PARITY",
    "BLOCKED_BY_PREGAME_TIMING_SOURCE",
    "BLOCKED_BY_ROW_POPULATION_MISMATCH",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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


def feature_names() -> tuple[list[str], list[str]]:
    control = pd.read_csv(CONTROL_FEATURE_MANIFEST)["feature_name"].astype(str).tolist()
    challenger = pd.read_csv(CHALLENGER_FEATURE_MANIFEST)["feature_name"].astype(str).tolist()
    return control, challenger


def default_source_feature_path(slate_date: str) -> Path:
    return ROOT / f"backend/mlb/exports/model_diagnostics/prepared_feature_vectors/{slate_date}/hits_features.csv"


def default_pa_context_path(slate_date: str) -> Path:
    return ROOT / f"artifacts/analysis/mlb/pa_foundation/examples/current_slate_pa_context_{slate_date}.csv"


def parse_time(value: Any) -> pd.Timestamp | None:
    if value is None or (isinstance(value, float) and math.isnan(value)) or str(value).strip() == "":
        return None
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts


def first_pitch_utc(source: pd.DataFrame, pa_context: pd.DataFrame | None) -> str:
    values: list[pd.Timestamp] = []
    for df in [source, pa_context]:
        if df is None or "game_time" not in df.columns:
            continue
        for value in df["game_time"].dropna().unique():
            ts = parse_time(value)
            if ts is not None:
                values.append(ts)
    if not values:
        return ""
    return min(values).isoformat().replace("+00:00", "Z")


def stable_row_hash(row: pd.Series) -> str:
    fields = [
        EXPERIMENT_ID,
        str(row.get("slate_date", "")),
        str(row.get("game_id", "")),
        str(row.get("player_id", "")),
        str(row.get("prop_type", "")),
        str(row.get("line", "")),
        str(row.get("side", "")),
        str(row.get("market_price", "")),
        str(row.get("bookmaker", "")),
        str(row.get("manifest_run_tag", "")),
    ]
    return hashlib.sha256("|".join(fields).encode()).hexdigest()


def load_pa_context(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    df = pd.read_csv(path)
    required = ["game_id", "player_id", "prop_type", "line", "d7_plate_appearances", "d15_plate_appearances", "d30_plate_appearances"]
    if any(c not in df.columns for c in required):
        return None
    keep = required + [c for c in ["pa_context_date", "pa_retention_status", "game_time"] if c in df.columns]
    return df[keep].copy()


def attach_pa(source: pd.DataFrame, pa_context: pd.DataFrame | None) -> tuple[pd.DataFrame, str, str]:
    df = source.copy()
    if pa_context is None:
        df["prior_d7_plate_appearances"] = 0.0
        df["prior_d15_plate_appearances"] = 0.0
        df["prior_d30_plate_appearances"] = 0.0
        df["pa_missing_flag"] = 1.0
        df["pa_context_latest_date"] = ""
        return df, "SOURCE_MISSING", ""
    keys = ["game_id", "player_id", "prop_type", "line"]
    pa = pa_context.copy()
    merged = df.merge(pa, on=keys, how="left", suffixes=("", "_pa"))
    for src, dst in [
        ("d7_plate_appearances", "prior_d7_plate_appearances"),
        ("d15_plate_appearances", "prior_d15_plate_appearances"),
        ("d30_plate_appearances", "prior_d30_plate_appearances"),
    ]:
        merged[dst] = pd.to_numeric(merged[src], errors="coerce")
    missing = merged[PA_FEATURES[:3]].isna().any(axis=1)
    merged.loc[missing, PA_FEATURES[:3]] = 0.0
    merged["pa_missing_flag"] = missing.astype(float)
    latest = ""
    if "pa_context_date" in merged.columns:
        dates = pd.to_datetime(merged["pa_context_date"], errors="coerce")
        if dates.notna().any():
            latest = dates.max().date().isoformat()
    merged["pa_context_latest_date"] = latest
    status = "PASS" if not missing.any() and latest else "PARTIAL"
    return merged, status, latest


def materialize_features(source: pd.DataFrame, control_features: list[str]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for _, row in source.iterrows():
        rows.append(_vectorize(row.to_dict(), control_features))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=control_features)


def market_price(row: pd.Series) -> Any:
    side = str(row.get("side", "")).lower()
    if side == "over":
        return row.get("price_over_american", row.get("market_price_over", ""))
    if side == "under":
        return row.get("price_under_american", row.get("market_price_under", ""))
    return row.get("market_price", "")


def build_manifest(args: argparse.Namespace) -> dict[str, Any]:
    slate_date = args.date
    source_path = Path(args.source_feature_csv) if args.source_feature_csv else default_source_feature_path(slate_date)
    pa_path = Path(args.pa_context_csv) if args.pa_context_csv else default_pa_context_path(slate_date)
    control_features, challenger_features = feature_names()
    run_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = Path(args.output_root or DEFAULT_MANIFEST_ROOT) / slate_date / run_tag
    run_dir.mkdir(parents=True, exist_ok=False)

    if not source_path.exists():
        raise FileNotFoundError(f"source feature CSV not found: {source_path}")
    source = pd.read_csv(source_path)
    if "prop_type" in source.columns:
        source = source[source["prop_type"].astype(str).eq("hits")].copy()
    for col in ["slate_date", "game_date"]:
        if col not in source.columns:
            source[col] = source.get("for_date", source.get("date", slate_date))
    source["slate_date"] = slate_date
    if "bookmaker" not in source.columns:
        source["bookmaker"] = source.get("market_bookmaker_key", "")
    source["market_price"] = source.apply(market_price, axis=1)

    pa_context = load_pa_context(pa_path)
    enriched, pa_status, pa_latest = attach_pa(source, pa_context)
    features = materialize_features(enriched, control_features)
    for pa_feature in PA_FEATURES:
        features[pa_feature] = pd.to_numeric(enriched[pa_feature], errors="coerce").fillna(0.0)

    first_pitch = first_pitch_utc(source, pa_context)
    frozen_at = utc_now()
    frozen_ts = parse_time(frozen_at)
    first_pitch_ts = parse_time(first_pitch)
    before_first_pitch = bool(first_pitch_ts is not None and frozen_ts is not None and frozen_ts < first_pitch_ts)
    timing_status = "VALID_PREGAME_FREEZE" if before_first_pitch else ("GAME_TIME_UNKNOWN" if first_pitch_ts is None else "LATE_FREEZE")
    pa_cutoff_ok = bool(pa_latest and pa_latest < slate_date)
    pregame_status = timing_status if timing_status != "VALID_PREGAME_FREEZE" else ("VALID_PREGAME_FREEZE" if pa_cutoff_ok and pa_status == "PASS" else "SOURCE_INCOMPLETE")

    out = pd.DataFrame()
    for col in IDENTITY_COLUMNS:
        out[col] = enriched[col] if col in enriched.columns else ""
    out["game_id"] = out["game_id"].astype(str)
    out["player_id"] = out["player_id"].astype(str)
    out["line"] = out["line"].astype(str)
    for col in METADATA_COLUMNS:
        out[col] = ""
    out["experiment_id"] = EXPERIMENT_ID
    out["generator_schema_version"] = SCHEMA_VERSION
    out["manifest_run_tag"] = run_tag
    out["manifest_frozen_timestamp_utc"] = frozen_at
    out["pregame_freeze_status"] = pregame_status
    out["snapshot_frozen_before_first_pitch"] = before_first_pitch
    out["earliest_first_pitch_utc"] = first_pitch
    out["pa_context_latest_date"] = pa_latest
    out["pa_feature_source_status"] = pa_status
    out["feature_cutoff_verified"] = bool(pa_cutoff_ok and before_first_pitch)
    out["canonical_vectorizer"] = "backend.mlb.prediction.make_prediction._vectorize"
    out["source_feature_path"] = rel(source_path)
    out["pa_context_source_path"] = rel(pa_path) if pa_path.exists() else ""
    for col in control_features + PA_FEATURES:
        out[col] = features[col] if col in features.columns else ""
    out["row_hash"] = out.apply(stable_row_hash, axis=1)

    manifest_path = run_dir / f"mlb_cc_0001_pregame_manifest_{slate_date}__{run_tag}.csv"
    ordered_cols = IDENTITY_COLUMNS + METADATA_COLUMNS + challenger_features
    out.to_csv(manifest_path, index=False, columns=ordered_cols)
    manifest_sha = sha256_path(manifest_path)

    source_sha = sha256_path(source_path)
    pa_sha = sha256_path(pa_path) if pa_path.exists() else ""
    metadata = {
        "experiment_id": EXPERIMENT_ID,
        "generator_schema_version": SCHEMA_VERSION,
        "slate_date": slate_date,
        "manifest_run_tag": run_tag,
        "manifest_path": rel(manifest_path),
        "manifest_sha256": manifest_sha,
        "manifest_frozen_timestamp_utc": frozen_at,
        "pregame_freeze_status": pregame_status,
        "snapshot_frozen_before_first_pitch": before_first_pitch,
        "earliest_first_pitch_utc": first_pitch,
        "source_feature_path": rel(source_path),
        "source_feature_sha256": source_sha,
        "pa_context_source_path": rel(pa_path) if pa_path.exists() else "",
        "pa_context_sha256": pa_sha,
        "pa_context_latest_date": pa_latest,
        "pa_feature_source_status": pa_status,
        "control_feature_manifest": rel(CONTROL_FEATURE_MANIFEST),
        "control_feature_manifest_sha256": sha256_path(CONTROL_FEATURE_MANIFEST),
        "challenger_feature_manifest": rel(CHALLENGER_FEATURE_MANIFEST),
        "challenger_feature_manifest_sha256": sha256_path(CHALLENGER_FEATURE_MANIFEST),
        "control_model_sha256": sha256_path(CONTROL_MODEL) if CONTROL_MODEL.exists() else "",
        "challenger_model_sha256": sha256_path(CHALLENGER_MODEL) if CHALLENGER_MODEL.exists() else "",
        "contract_sha256": CONTRACT_SHA.read_text().strip() if CONTRACT_SHA.exists() else "",
        "rows": int(len(out)),
        "unique_row_hashes": int(out["row_hash"].nunique()),
    }
    metadata_path = run_dir / "mlb_cc_0001_pregame_manifest_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")

    validation = validation_rows(out, source, control_features, challenger_features, metadata)
    validation_path = run_dir / "mlb_cc_0001_pregame_manifest_validation.csv"
    write_csv(validation_path, validation, ["check_name", "status", "detail"])
    write_audit_artifacts(slate_date, source, control_features, challenger_features, metadata, validation)
    return {
        "manifest_path": manifest_path,
        "metadata_path": metadata_path,
        "validation_path": validation_path,
        "metadata": metadata,
        "validation": validation,
    }


def validation_rows(out: pd.DataFrame, source: pd.DataFrame, control_features: list[str], challenger_features: list[str], metadata: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    def add(name: str, status: str, detail: str) -> None:
        rows.append({"check_name": name, "status": status, "detail": detail})

    add("generator_schema_version", "PASS" if metadata["generator_schema_version"] == SCHEMA_VERSION else "FAIL", metadata["generator_schema_version"])
    add("manifest_sha256_recorded", "PASS" if len(metadata["manifest_sha256"]) == 64 else "FAIL", metadata["manifest_sha256"])
    add("control_model_hash", "PASS" if metadata["control_model_sha256"] == CONTROL_SHA else "FAIL", metadata["control_model_sha256"])
    add("challenger_model_hash", "PASS" if metadata["challenger_model_sha256"] == CHALLENGER_SHA else "FAIL", metadata["challenger_model_sha256"])
    add("control_feature_presence", "PASS" if all(c in out.columns for c in control_features) else "FAIL", f"missing={len([c for c in control_features if c not in out.columns])}")
    add("challenger_feature_presence", "PASS" if all(c in out.columns for c in challenger_features) else "FAIL", f"missing={len([c for c in challenger_features if c not in out.columns])}")
    actual_order = [c for c in out.columns if c in challenger_features]
    add("challenger_feature_order", "PASS" if actual_order == challenger_features else "FAIL", f"expected={len(challenger_features)} actual={len(actual_order)}")
    add("raw_plate_appearances_absent", "PASS" if "plate_appearances" not in out.columns else "FAIL", "raw plate_appearances forbidden")
    add("pa_cutoff_strict_prior", "PASS" if metadata["pa_context_latest_date"] and metadata["pa_context_latest_date"] < metadata["slate_date"] else "FAIL", str(metadata["pa_context_latest_date"]))
    add("pregame_freeze_status", "PASS" if metadata["pregame_freeze_status"] == "VALID_PREGAME_FREEZE" else "FAIL", metadata["pregame_freeze_status"])
    add("row_hash_unique", "PASS" if metadata["rows"] == metadata["unique_row_hashes"] else "FAIL", f"rows={metadata['rows']} unique={metadata['unique_row_hashes']}")
    dupes = int(source.duplicated(["game_id", "player_id", "prop_type", "line", "side"]).sum()) if all(c in source.columns for c in ["game_id", "player_id", "prop_type", "line", "side"]) else -1
    add("logical_key_duplicates", "PASS" if dupes == 0 else "FAIL", f"duplicates={dupes}")
    add("historical_feature_parity", "FAIL", "not yet proven against frozen historical prediction matrix")
    add("historical_prediction_parity", "FAIL", "not yet proven against frozen production prediction output")
    return rows


def write_audit_artifacts(slate_date: str, source: pd.DataFrame, control_features: list[str], challenger_features: list[str], metadata: dict[str, Any], validation: list[dict[str, str]]) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    source_cols = set(source.columns)
    feature_rows = []
    gap_rows = []
    for idx, feature in enumerate(challenger_features, start=1):
        if feature in PA_FEATURES:
            status = "PA_CONTEXT_JOIN"
            parent = feature.replace("prior_", "").replace("pa_missing_flag", "d7/d15/d30_plate_appearances")
            source_ref = "artifacts/analysis/mlb/pa_foundation/examples/current_slate_pa_context_<DATE>.csv"
            generator = "PA foundation passive retention artifacts"
        elif feature.startswith("isna__"):
            status = "CANONICAL_VECTORIZER_DERIVED"
            parent = feature.split("__", 1)[1]
            source_ref = "backend.mlb.prediction.make_prediction._vectorize"
            generator = "production model vectorizer"
        elif feature in source_cols:
            status = "DIRECT_SOURCE_COLUMN"
            parent = feature
            source_ref = "backend/mlb/exports/model_diagnostics/prepared_feature_vectors/<DATE>/hits_features.csv"
            generator = "canonical MLB Hits prepared-feature export"
        else:
            status = "PRODUCTION_NUMERIC_ZERO_FALLBACK"
            parent = feature
            source_ref = "backend.mlb.prediction.make_prediction._vectorize"
            generator = "production model vectorizer numeric fallback"
        feature_rows.append({
            "feature_name": feature,
            "feature_order": idx,
            "role": "control_and_challenger" if feature in control_features else "challenger_only",
            "source_table_or_file": source_ref,
            "source_column": parent,
            "generator": generator,
            "prediction_time_availability": status,
            "historical_availability": "UNKNOWN_PENDING_PARITY",
            "missing_policy": "canonical_vectorizer" if status != "PA_CONTEXT_JOIN" else "prior-window PA or pa_missing_flag",
            "notes": "derived/fallback uses production vectorizer; not manually fabricated",
        })
        if feature not in source_cols:
            gap_rows.append({
                "feature_name": feature,
                "gap_class": status,
                "source_column_present_in_prepared_features": False,
                "blocks_manual_pregame_use": status == "PRODUCTION_NUMERIC_ZERO_FALLBACK",
                "resolution": "prove historical feature/prediction parity before enabling scoreable freeze",
                "notes": "Missingness indicators are expected runtime features; raw numeric fallbacks require parity evidence.",
            })
    write_csv(
        AUDIT_DIR / "mlb_cc_0001_feature_source_map_2026-07-10.csv",
        feature_rows,
        ["feature_name", "feature_order", "role", "source_table_or_file", "source_column", "generator", "prediction_time_availability", "historical_availability", "missing_policy", "notes"],
    )
    write_csv(
        AUDIT_DIR / "mlb_cc_0001_missing_feature_gap_audit_2026-07-10.csv",
        gap_rows,
        ["feature_name", "gap_class", "source_column_present_in_prepared_features", "blocks_manual_pregame_use", "resolution", "notes"],
    )
    parity_rows = [
        {"parity_check": "row_population_parity", "status": "NOT_PROVEN", "evidence_path": rel(metadata_path(metadata)), "notes": "current-day diagnostic manifest only; no historical replay accepted"},
        {"parity_check": "feature_value_parity", "status": "BLOCKED", "evidence_path": "", "notes": "needs frozen historical production feature matrix or byte-comparable prediction rows"},
        {"parity_check": "prediction_probability_parity", "status": "BLOCKED", "evidence_path": "", "notes": "must compare generated manifest model scores to production predictions before manual pregame scoring"},
    ]
    write_csv(AUDIT_DIR / "mlb_cc_0001_canonical_feature_parity_2026-07-10.csv", parity_rows, ["parity_check", "status", "evidence_path", "notes"])
    schema_rows = [{"column_name": c, "column_order": i + 1, "column_role": "identity" if c in IDENTITY_COLUMNS else ("metadata" if c in METADATA_COLUMNS else "model_feature"), "required": True, "notes": ""} for i, c in enumerate(IDENTITY_COLUMNS + METADATA_COLUMNS + challenger_features)]
    write_csv(AUDIT_DIR / "mlb_cc_0001_pregame_manifest_schema_2026-07-10.csv", schema_rows, ["column_name", "column_order", "column_role", "required", "notes"])
    write_csv(AUDIT_DIR / "mlb_cc_0001_generator_validation_2026-07-10.csv", validation, ["check_name", "status", "detail"])
    health_rows = [
        {"health_check": "complete frozen manifest exists", "failure_action": "do not score slate", "current_status": metadata["pregame_freeze_status"], "notes": "fail closed"},
        {"health_check": "PA context date strictly prior", "failure_action": "do not score slate", "current_status": str(metadata["pa_context_latest_date"]), "notes": "raw plate_appearances excluded"},
        {"health_check": "historical feature/prediction parity", "failure_action": "do not authorize manual use", "current_status": "BLOCKED", "notes": "required before readiness"},
    ]
    write_csv(AUDIT_DIR / "mlb_cc_0001_generator_health_checks_2026-07-10.csv", health_rows, ["health_check", "failure_action", "current_status", "notes"])
    write_markdown_docs(metadata, validation, feature_rows, gap_rows)


def metadata_path(metadata: dict[str, Any]) -> Path:
    p = ROOT / metadata["manifest_path"]
    return p.parent / "mlb_cc_0001_pregame_manifest_metadata.json"


def write_markdown_docs(metadata: dict[str, Any], validation: list[dict[str, str]], feature_rows: list[dict[str, Any]], gap_rows: list[dict[str, Any]]) -> None:
    failures = [r for r in validation if r["status"] == "FAIL"]
    readiness = "BLOCKED_BY_FEATURE_PARITY"
    if any(r["check_name"] == "pregame_freeze_status" and r["status"] == "FAIL" for r in validation):
        readiness = "BLOCKED_BY_PREGAME_TIMING_SOURCE"
    assert readiness in READINESS_VALUES
    summary = [
        "# MLB-CC-0001 Pregame Manifest Generator Architecture",
        "",
        "This generator creates an immutable, prediction-time-safe input manifest for MLB-CC-0001 without training models, changing formulas, writing to the database, or touching production uploads.",
        "",
        "The generator reuses `backend.mlb.prediction.make_prediction._vectorize` to materialize the 73 frozen control inputs in the same order as the frozen control manifest, then appends only the four approved PA challenger features.",
        "",
        "## Current Dry Run",
        "",
        f"- Slate date: `{metadata['slate_date']}`",
        f"- Manifest: `{metadata['manifest_path']}`",
        f"- Manifest SHA256: `{metadata['manifest_sha256']}`",
        f"- Rows: `{metadata['rows']}`",
        f"- Pregame freeze status: `{metadata['pregame_freeze_status']}`",
        f"- PA context latest date: `{metadata['pa_context_latest_date']}`",
        f"- Readiness decision: `{readiness}`",
        "",
        "## Architecture",
        "",
        "1. Load the frozen control and challenger feature manifests from the approved MLB-CC-0001 execution package.",
        "2. Load the canonical daily Hits prepared-feature export for the slate date.",
        "3. Join PA rolling context from the PA foundation passive-retention artifact by exact game/player/prop/line key.",
        "4. Materialize the control feature matrix through the production vectorizer, including `isna__` indicators.",
        "5. Append `prior_d7_plate_appearances`, `prior_d15_plate_appearances`, `prior_d30_plate_appearances`, and `pa_missing_flag` only.",
        "6. Freeze row hashes, source hashes, feature-manifest hashes, model hashes, and metadata sidecar.",
        "7. Fail closed unless pregame timing, PA cutoff, feature order, row uniqueness, and parity gates pass.",
    ]
    (AUDIT_DIR / "mlb_cc_0001_pregame_manifest_architecture_2026-07-10.md").write_text("\n".join(summary) + "\n")
    integration = [
        "# MLB-CC-0001 Daily Integration Design",
        "",
        "The generator is intended to run after the normal prediction/context artifacts exist and before first pitch.",
        "",
        "Suggested manual command:",
        "",
        "```bash",
        "python -m backend.mlb.scripts.build_mlb_cc_0001_pregame_manifest --date YYYY-MM-DD --mode freeze",
        "```",
        "",
        "The prospective runner can consume a generated canonical manifest with `--input-manifest`, and it can also auto-locate the latest same-date valid freeze when no explicit manifest is supplied.",
        "",
        "The integration remains artifact-only. It does not write to the database, call OddsAPI, alter LaunchAgents, or change production behavior.",
    ]
    (AUDIT_DIR / "mlb_cc_0001_daily_integration_design_2026-07-10.md").write_text("\n".join(integration) + "\n")
    readiness_doc = [
        "# MLB-CC-0001 Pregame Manifest Generator Readiness",
        "",
        f"- Readiness decision: `{readiness}`",
        f"- Validation failures: `{len(failures)}`",
        f"- Feature source-map rows: `{len(feature_rows)}`",
        f"- Gap rows: `{len(gap_rows)}`",
        "",
        "## Decision",
        "",
        "The generator can assemble a governed diagnostic manifest, but it is not yet ready for manual pregame scoring because historical feature-value and prediction-probability parity have not been proven.",
        "",
        "Current-day output is also not countable evidence unless it was frozen before first pitch.",
        "",
        "## Blockers",
        "",
    ]
    for row in failures:
        readiness_doc.append(f"- `{row['check_name']}`: {row['detail']}")
    (AUDIT_DIR / "mlb_cc_0001_generator_readiness_2026-07-10.md").write_text("\n".join(readiness_doc) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Slate date YYYY-MM-DD")
    parser.add_argument("--mode", choices=["diagnostic", "freeze"], default="diagnostic")
    parser.add_argument("--source-feature-csv")
    parser.add_argument("--pa-context-csv")
    parser.add_argument("--output-root")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = build_manifest(args)
    print(json.dumps({k: rel(v) if isinstance(v, Path) else v for k, v in result.items() if k.endswith("_path")}, indent=2))
    if args.mode == "freeze" and result["metadata"].get("pregame_freeze_status") != "VALID_PREGAME_FREEZE":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
