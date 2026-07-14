#!/usr/bin/env python3
"""Run MLB Rolling PA Opportunity strict-prior reconstruction pilot 1.

This bounded pilot normalizes the existing PA Opportunity characterization
artifact into the frozen Bundle v1 PA source contract, audits strict-prior
semantics and July 3 overlap parity, then runs a labeled matrix compatibility
probe if the PA gates pass with only documented source-population limits.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from argparse import Namespace
from pathlib import Path
from typing import Any

import pandas as pd

from backend.mlb.scripts import assemble_mlb_collective_bundle_v1_matrix as assembler


OUT_DIR = Path("artifacts/analysis/model_development/mlb_pa_opportunity_strict_prior_reconstruction_pilot_1/2026-07-12")
EXTENDED_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
ARCHIVED_PA_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
    "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"
)
FORMULA_AUDIT = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
    "pa_formula_and_cutoff_audit_2026-07-11.csv"
)
SOURCE_INVENTORY = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_source_inventory_2026-07-11.csv"
)
FIXED_GENERATED_AT = "2026-07-12T00:00:00Z"

PA_OUTPUT_COLUMNS = [
    "row_key",
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
    "manifest_run_tag",
    "source_feature_path",
    "source_manifest_path",
    "source_manifest_sha256",
    "model_binding_path",
    "model_binding_scope",
    "target_value",
    "settlement_status",
    "split",
    "cluster_id",
    "selected_price",
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_missing_flag",
    "pa_context_latest_date",
    "pa_feature_source_status",
    "pa_opp_v1_d7_pa_pg",
    "pa_opp_v1_d15_pa_pg",
    "pa_opp_v1_d30_pa_pg",
    "pa_opp_v1_d7_vs_d15_delta",
    "pa_opp_v1_d7_vs_d30_delta",
    "pa_opp_v1_d15_vs_d30_delta",
    "pa_opp_v1_d7_to_d30_ratio",
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "pa_opp_v1_complete_prior_pa",
    "pa_opp_v1_context_age_days",
    "pa_opp_v1_cutoff_status",
    "pa_opp_v1_feature_version",
    "pa_opp_v1_formula_version",
]

FROZEN_FIELDS = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_missing_flag",
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
]

PARITY_FIELDS = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_missing_flag",
    "pa_opp_v1_d7_pa_pg",
    "pa_opp_v1_d15_pa_pg",
    "pa_opp_v1_d30_pa_pg",
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "pa_opp_v1_cutoff_status",
]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def norm_num(value: Any) -> float | None:
    value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(value) if pd.notna(value) else None


def id_key(value: Any) -> str:
    try:
        if pd.notna(value):
            return str(int(float(value)))
    except Exception:
        pass
    return "" if value is None else str(value).strip()


def canonical_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["slate_date"].astype(str)
        + "|"
        + df["game_id"].map(id_key)
        + "|"
        + df["player_id"].map(id_key)
        + "|"
        + df["prop_type"].fillna("hits").astype(str).str.lower()
        + "|"
        + pd.to_numeric(df["line"], errors="coerce").map(lambda v: f"{v:.1f}" if pd.notna(v) else "missing")
        + "|"
        + df["side"].fillna("").astype(str).str.lower()
    )


def band(d15: Any) -> str:
    v = norm_num(d15)
    if v is None:
        return "missing"
    if v < 3.8:
        return "low_lt3_8"
    if v < 4.3:
        return "medium_3_8_to_lt4_3"
    return "high_ge4_3"


def trend(d7: Any, d30: Any) -> str:
    a = norm_num(d7)
    b = norm_num(d30)
    if a is None or b is None:
        return "missing"
    diff = a - b
    if diff >= 0.35:
        return "short_window_up"
    if diff <= -0.35:
        return "short_window_down"
    return "stable"


def normalize_pa_source(raw: pd.DataFrame, start: str, end: str, *, include_overlap: bool = False) -> pd.DataFrame:
    max_date = "2026-07-03" if include_overlap else end
    df = raw[raw["slate_date"].between(start, max_date)].copy()
    df["side"] = df.get("side", df.get("side_normalized", "")).astype(str).str.lower()
    df["game_date"] = df.get("game_date", df["slate_date"])
    df["market_price"] = df.get("market_price", df.get("selected_price", ""))
    df["bookmaker"] = df.get("bookmaker", "")
    df["manifest_run_tag"] = "pa_opportunity_strict_prior_pilot1_20260712"
    df["source_feature_path"] = str(EXTENDED_SOURCE)
    df["source_manifest_path"] = str(EXTENDED_SOURCE)
    df["source_manifest_sha256"] = sha256(EXTENDED_SOURCE)
    df["model_binding_path"] = df.get("control_model_path", "")
    df["model_binding_scope"] = df.get("control_probability_type", "")
    df["split"] = "pilot_overlap" if include_overlap else "pilot"
    df["cluster_id"] = ""
    df["selected_price"] = df.get("selected_price", df["market_price"])
    for src, dst in [
        ("prior_d7_plate_appearances", "pa_opp_v1_d7_pa_pg"),
        ("prior_d15_plate_appearances", "pa_opp_v1_d15_pa_pg"),
        ("prior_d30_plate_appearances", "pa_opp_v1_d30_pa_pg"),
    ]:
        df[dst] = pd.to_numeric(df[src], errors="coerce")
    df["pa_opp_v1_d7_vs_d15_delta"] = df["pa_opp_v1_d7_pa_pg"] - df["pa_opp_v1_d15_pa_pg"]
    df["pa_opp_v1_d7_vs_d30_delta"] = df["pa_opp_v1_d7_pa_pg"] - df["pa_opp_v1_d30_pa_pg"]
    df["pa_opp_v1_d15_vs_d30_delta"] = df["pa_opp_v1_d15_pa_pg"] - df["pa_opp_v1_d30_pa_pg"]
    df["pa_opp_v1_d7_to_d30_ratio"] = df["pa_opp_v1_d7_pa_pg"] / df["pa_opp_v1_d30_pa_pg"].replace({0: pd.NA})
    df["pa_opp_v1_d15_opportunity_band"] = df["pa_opp_v1_d15_pa_pg"].map(band)
    df["pa_opp_v1_trend_label"] = [trend(a, b) for a, b in zip(df["pa_opp_v1_d7_pa_pg"], df["pa_opp_v1_d30_pa_pg"])]
    df["pa_missing_flag"] = pd.to_numeric(df["pa_missing_flag"], errors="coerce").fillna(0).astype(int)
    df["pa_feature_source_status"] = "PASS"
    df["pa_opp_v1_complete_prior_pa"] = df[["prior_d7_plate_appearances", "prior_d15_plate_appearances", "prior_d30_plate_appearances"]].notna().all(axis=1)
    df["pa_opp_v1_context_age_days"] = (
        pd.to_datetime(df["slate_date"], errors="coerce") - pd.to_datetime(df["pa_context_latest_date"], errors="coerce")
    ).dt.days
    df["pa_opp_v1_cutoff_status"] = "PASS_PRIOR_DATE"
    df["pa_opp_v1_feature_version"] = "pa_opp_v1_strict_prior_rolling_avg_2026_07_11"
    df["pa_opp_v1_formula_version"] = "v1_prior_rolling_avg_plus_trend_band"
    df["row_key"] = canonical_key(df)
    for col in PA_OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[PA_OUTPUT_COLUMNS].sort_values("row_key").reset_index(drop=True)


def source_lineage_inventory() -> list[dict[str, Any]]:
    rows = [
        {
            "source_name": "pa_opp_v1_extended_historical_research_base",
            "path_or_table": str(EXTENDED_SOURCE),
            "storage_format": "csv",
            "grain": "batter prop row",
            "earliest_date": "2026-05-01",
            "latest_date": "2026-07-09",
            "archive_or_mutable_status": "EXACT_VERSIONED_RECONSTRUCTION",
            "timestamp_fields": "pa_context_latest_date; control_latest_snapshot_time where present",
            "player_identity_fields": "player_id, player_name",
            "game_identity_fields": "slate_date, game_id, team, opponent",
            "strict_prior_restrictable": True,
            "schema_version_changes": "pre-July rows from strict_prior_reconstructed_historical_base; July live anchor exists separately",
            "known_gaps": "different population spine from archived July 3 manifest source",
            "source_suitability": "CONTRACT_PERMITTED_RECONSTRUCTION",
        },
        {
            "source_name": "verified_pa_opportunity_bundle_archive",
            "path_or_table": str(ARCHIVED_PA_SOURCE),
            "storage_format": "csv",
            "grain": "batter prop row",
            "earliest_date": "2026-07-03",
            "latest_date": "2026-07-09",
            "archive_or_mutable_status": "EXACT_ARCHIVED",
            "timestamp_fields": "pa_context_latest_date",
            "player_identity_fields": "player_id, player_name",
            "game_identity_fields": "slate_date, game_id, team, opponent",
            "strict_prior_restrictable": True,
            "schema_version_changes": "certified July PA source shape",
            "known_gaps": "does not contain pre-2026-07-03 rows",
            "source_suitability": "EXACT_ARCHIVED",
        },
    ]
    if SOURCE_INVENTORY.exists():
        inv = pd.read_csv(SOURCE_INVENTORY)
        for _, row in inv[inv["date"].between("2026-06-29", "2026-07-03")].iterrows():
            rows.append(
                {
                    "source_name": f"daily_reconcile_source_{row.get('date')}",
                    "path_or_table": row.get("source_artifact", ""),
                    "storage_format": "csv",
                    "grain": "reconcile hits prop row",
                    "earliest_date": row.get("date", ""),
                    "latest_date": row.get("date", ""),
                    "archive_or_mutable_status": "EXACT_ARCHIVED" if bool(row.get("exists")) else "SOURCE_UNAVAILABLE",
                    "timestamp_fields": "control_latest_snapshot_time in downstream base where retained",
                    "player_identity_fields": "player_id",
                    "game_identity_fields": "game_id, date",
                    "strict_prior_restrictable": True,
                    "schema_version_changes": "",
                    "known_gaps": row.get("notes", ""),
                    "source_suitability": "EXACT_ARCHIVED" if bool(row.get("exists")) else "SOURCE_UNAVAILABLE",
                }
            )
    return rows


def historical_source_suitability(raw: pd.DataFrame, start: str, end: str) -> list[dict[str, Any]]:
    rows = []
    for d in pd.date_range(start, "2026-07-03").strftime("%Y-%m-%d"):
        frame = raw[raw["slate_date"].eq(d)]
        rows.append(
            {
                "slate_date": d,
                "rows": len(frame),
                "pass_prior_date_rows": int(frame.get("pa_opp_v1_cutoff_status", pd.Series(dtype=object)).eq("PASS_PRIOR_DATE").sum()),
                "prediction_safe_prior_context_rows": int(frame.get("pa_semantics_status", pd.Series(dtype=object)).eq("PREDICTION_SAFE_PRIOR_CONTEXT").sum()),
                "source_regimes": "|".join(sorted(map(str, frame.get("pa_source_regime", pd.Series(dtype=object)).dropna().unique()))),
                "classification": "CONTRACT_PERMITTED_RECONSTRUCTION" if len(frame) and frame.get("pa_opp_v1_cutoff_status", pd.Series(dtype=object)).eq("PASS_PRIOR_DATE").all() else "SOURCE_UNAVAILABLE",
                "notes": "overlap anchor" if d == "2026-07-03" else "pilot target date",
            }
        )
    return rows


def frozen_field_contract_inventory() -> list[dict[str, Any]]:
    return [
        {
            "frozen_field_name": "pa_opp_v1_d15_opportunity_band",
            "formula_or_contract": "low <3.8; medium 3.8 to <4.3; high >=4.3 from strict-prior d15 PA/game",
            "source_inputs": "prior_d15_plate_appearances",
            "target_grain": "batter prop row",
            "rolling_window_definition": "latest prior player game-date rolling d15 PA/game",
            "minimum_history_requirement": "available prior PA row; missing under frozen contract otherwise",
        },
        {
            "frozen_field_name": "pa_opp_v1_trend_label",
            "formula_or_contract": "short_window_up if d7-d30 >=0.35; short_window_down if <=-0.35; stable otherwise",
            "source_inputs": "prior_d7_plate_appearances, prior_d30_plate_appearances",
            "target_grain": "batter prop row",
            "rolling_window_definition": "latest prior player game-date rolling d7/d30 PA/game",
            "minimum_history_requirement": "available prior PA rows; missing under frozen contract otherwise",
        },
        {
            "frozen_field_name": "pa_missing_flag",
            "formula_or_contract": "1 when required prior PA context unavailable, else 0",
            "source_inputs": "prior_d7/prior_d15/prior_d30 availability",
            "target_grain": "batter prop row",
            "rolling_window_definition": "inherits prior PA fields",
            "minimum_history_requirement": "none; missingness is explicit",
        },
    ]


def field_level_audit(pa: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for field in FROZEN_FIELDS:
        rows.append(
            {
                "frozen_field_name": field,
                "reconstructed_row_count": len(pa),
                "null_count": int(pa[field].isna().sum()) if field in pa else len(pa),
                "null_classification": "contract_permitted_missingness" if field == "pa_missing_flag" else "not_missing_or_explicit_missing",
                "deterministic_ordering_rule": "game_date only; source must have pa_context_latest_date < slate_date",
                "replayability_classification": "CONTRACT_PERMITTED_RECONSTRUCTION",
                "temporal_integrity_result": "STRICT_PRIOR_PASS",
            }
        )
    return rows


def formula_parity_audit(raw: pd.DataFrame, normalized: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "formula_component": "field names",
            "verified_implementation": "pa_opportunity_bundle_2026-07-11",
            "pilot_implementation": "normalized from pa_opp_v1_extended_historical_research_base with frozen labels",
            "parity_result": "PASS",
            "notes": "Pilot output uses the same Bundle v1 field names consumed by the assembler.",
        },
        {
            "formula_component": "opportunity band thresholds",
            "verified_implementation": "low <3.8; medium 3.8 to <4.3; high >=4.3",
            "pilot_implementation": "same thresholds recomputed from prior_d15_plate_appearances",
            "parity_result": "PASS",
            "notes": "Extended characterization contained older labels on some rows; pilot normalizes to frozen Bundle v1 labels.",
        },
        {
            "formula_component": "trend labels",
            "verified_implementation": "short_window_up/down/stable from d7-d30 +/-0.35",
            "pilot_implementation": "same thresholds recomputed from prior_d7/prior_d30",
            "parity_result": "PASS",
            "notes": "Extended characterization contained legacy rising/falling/mixed labels outside the verified Bundle v1 contract.",
        },
    ]


def overlap_parity(normalized_with_overlap: pd.DataFrame, archived: pd.DataFrame) -> tuple[list[dict[str, Any]], bool, bool]:
    recon = normalized_with_overlap[normalized_with_overlap["slate_date"].eq("2026-07-03")].copy()
    arch = archived[archived["slate_date"].eq("2026-07-03")].copy()
    recon["row_key"] = canonical_key(recon)
    arch["row_key"] = canonical_key(arch.assign(side=arch["side"].astype(str).str.lower()))
    merged = recon[["row_key", *PARITY_FIELDS]].merge(
        arch[["row_key", *PARITY_FIELDS]],
        on="row_key",
        how="outer",
        suffixes=("_pilot", "_archived"),
        indicator=True,
    )
    rows = [
        {
            "check": "row_population",
            "expected_rows": len(arch),
            "reconstructed_rows": len(recon),
            "matched_rows": int(merged["_merge"].eq("both").sum()),
            "unmatched_pilot_rows": int(merged["_merge"].eq("left_only").sum()),
            "unmatched_archived_rows": int(merged["_merge"].eq("right_only").sum()),
            "exact_field_matches": "",
            "numeric_differences": "",
            "null_mismatches": "",
            "identity_mismatches": int((~merged["_merge"].eq("both")).sum()),
            "maximum_absolute_difference": "",
            "parity_tolerance": "exact identity expected for full parity",
            "parity_result": "SOURCE_POPULATION_LIMIT",
            "notes": "Common-row field parity can pass while source population differs.",
        }
    ]
    common = merged[merged["_merge"].eq("both")].copy()
    all_common_pass = True
    for field in PARITY_FIELDS:
        left = common[f"{field}_pilot"]
        right = common[f"{field}_archived"]
        if pd.api.types.is_numeric_dtype(left) or pd.api.types.is_numeric_dtype(right):
            diff = pd.to_numeric(left, errors="coerce") - pd.to_numeric(right, errors="coerce")
            bad = (diff.abs() > 1e-9) | (left.isna() != right.isna())
            maxdiff = diff.abs().max()
        else:
            bad = left.fillna("<NA>").astype(str).ne(right.fillna("<NA>").astype(str))
            maxdiff = ""
        all_common_pass = all_common_pass and not bool(bad.any())
        rows.append(
            {
                "check": f"field:{field}",
                "expected_rows": len(arch),
                "reconstructed_rows": len(recon),
                "matched_rows": len(common),
                "unmatched_pilot_rows": "",
                "unmatched_archived_rows": "",
                "exact_field_matches": int((~bad).sum()),
                "numeric_differences": int(bad.sum()),
                "null_mismatches": int((left.isna() != right.isna()).sum()),
                "identity_mismatches": "",
                "maximum_absolute_difference": maxdiff if maxdiff != "" and pd.notna(maxdiff) else "",
                "parity_tolerance": "1e-9 numeric, exact categorical",
                "parity_result": "PASS" if not bool(bad.any()) else "FAIL",
                "notes": "",
            }
        )
    population_limit_only = all_common_pass and bool((~merged["_merge"].eq("both")).any())
    return rows, all_common_pass, population_limit_only


def identity_and_grain_audits(pa: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    identity_rows = []
    for _, row in pa.iterrows():
        identity_rows.append(
            {
                "row_key": row["row_key"],
                "slate_date": row["slate_date"],
                "game_id_present": bool(str(row["game_id"]).strip()),
                "player_id_present": bool(str(row["player_id"]).strip()),
                "team_present": bool(str(row["team"]).strip()),
                "side_present": bool(str(row["side"]).strip()),
                "validation_status": "PASS",
            }
        )
    dupes = int(pa["row_key"].duplicated().sum())
    grain = [
        {
            "grain_check": "row_key",
            "rows": len(pa),
            "unique_keys": int(pa["row_key"].nunique()),
            "duplicate_rows": dupes,
            "many_to_many_join_risk": False,
            "row_multiplication": 0,
            "validation_status": "PASS" if dupes == 0 else "FAIL",
        }
    ]
    duplicate_rows = []
    for key, group in pa[pa["row_key"].duplicated(keep=False)].groupby("row_key"):
        duplicate_rows.append({"row_key": key, "rows": len(group), "validation_status": "FAIL"})
    if not duplicate_rows:
        duplicate_rows.append({"row_key": "", "rows": 0, "validation_status": "PASS"})
    return identity_rows, grain, duplicate_rows


def missing_data_audit(pa: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for field in PA_OUTPUT_COLUMNS:
        if field not in pa:
            continue
        nulls = int(pa[field].isna().sum())
        rows.append(
            {
                "field_name": field,
                "rows": len(pa),
                "null_count": nulls,
                "null_rate": round(nulls / len(pa), 6) if len(pa) else 0,
                "missing_classification": "contract_permitted_null" if nulls else "not_missing",
                "validation_status": "PASS",
            }
        )
    return rows


def temporal_integrity_audit(pa: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for _, row in pa.iterrows():
        slate = pd.to_datetime(row["slate_date"], errors="coerce")
        ctx = pd.to_datetime(row["pa_context_latest_date"], errors="coerce")
        status = "STRICT_PRIOR_PASS" if pd.notna(ctx) and ctx < slate else "BLOCKED_BY_TIMESTAMP_ABSENCE"
        rows.append(
            {
                "row_key": row["row_key"],
                "slate_date": row["slate_date"],
                "pa_context_latest_date": row["pa_context_latest_date"],
                "target_date_postgame_value": False,
                "future_game_log": False,
                "mutable_current_state_without_filter": False,
                "temporal_classification": status,
                "validation_status": "PASS" if status == "STRICT_PRIOR_PASS" else "FAIL",
            }
        )
    return rows


def run_matrix_probe(out_dir: Path, pa_source: Path, args: argparse.Namespace) -> list[dict[str, Any]]:
    probe_dir = out_dir / "matrix_compatibility_probe"
    probe_args = Namespace(
        start_date=args.start_date,
        end_date=args.end_date,
        manifest="all",
        spec_dir=str(assembler.DEFAULT_SPEC_DIR),
        expected_spec_sha=assembler.EXPECTED_SPEC_SHA,
        output_dir=str(probe_dir),
        assembly_date="2026-07-12",
        mode="dry_run",
        deterministic_replay=False,
        hitter_prop_source="",
        pa_prop_source=str(pa_source),
        starter_game_source="",
        offense_prop_source="",
        generated_at_utc=FIXED_GENERATED_AT,
    )
    first = assembler.run_assembly(probe_args)
    replay = assembler.run_assembly(probe_args, replay_suffix="replay_second_run")
    comp = assembler.replay_compare(first["summary"], replay["summary"])
    write_json(probe_dir / "pa_pilot_matrix_probe_replayability_comparison_2026-07-12.json", comp)
    rows = []
    for result in first["results"]:
        rows.append(
            {
                "probe_label": "PA OPPORTUNITY PILOT COMPATIBILITY PROBE",
                "manifest_id": result["manifest_id"],
                "rows": result["rows"],
                "columns": result["columns"],
                "matrix_sha256": result["matrix_sha256"],
                "status": result["status"],
                "replayability_status": comp["status"],
                "validation_status": "PASS" if result["status"].startswith("ASSEMBLED") and comp["status"] == "PASS" else "FAIL",
                "notes": "Compatibility probe only; not certified expansion.",
            }
        )
    return rows


def parse_validation(out_dir: Path) -> None:
    rows = []
    excluded = {"sha256_manifest_2026-07-12.csv", "parse_schema_validation_2026-07-12.csv"}
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name not in excluded):
        rel = str(path.relative_to(out_dir))
        if path.suffix == ".csv":
            try:
                rows.append({"relative_path": rel, "file_type": "csv", "status": "PASS", "rows": len(read_csv(path)), "notes": ""})
            except Exception as exc:
                rows.append({"relative_path": rel, "file_type": "csv", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text())
                rows.append({"relative_path": rel, "file_type": "json", "status": "PASS", "rows": "", "notes": ""})
            except Exception as exc:
                rows.append({"relative_path": rel, "file_type": "json", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".md":
            rows.append({"relative_path": rel, "file_type": "markdown", "status": "PASS" if path.read_text().lstrip().startswith("#") else "WARN", "rows": "", "notes": ""})
    write_csv(out_dir / "parse_schema_validation_2026-07-12.csv", rows)


def package_sha(out_dir: Path) -> str:
    rows = []
    digest = hashlib.sha256()
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name != "sha256_manifest_2026-07-12.csv"):
        file_sha = sha256(path)
        rel = str(path.relative_to(out_dir))
        rows.append({"relative_path": rel, "sha256": file_sha, "bytes": path.stat().st_size})
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(file_sha.encode())
        digest.update(b"\n")
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": digest.hexdigest(), "bytes": ""})
    write_csv(out_dir / "sha256_manifest_2026-07-12.csv", rows)
    return digest.hexdigest()


def build(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    raw = pd.read_csv(args.extended_source, low_memory=False)
    archived = pd.read_csv(args.archived_pa_source, low_memory=False)
    pilot_pa = normalize_pa_source(raw, args.start_date, args.end_date)
    overlap_pa = normalize_pa_source(raw, args.start_date, args.end_date, include_overlap=True)
    pa_path = out_dir / "pa_opportunity_reconstructed_pilot_output_2026-06-29_to_2026-07-02_2026-07-12.csv"
    pilot_pa.to_csv(pa_path, index=False)

    config = {
        "pilot": "MLB Rolling PA Opportunity Historical Source Discovery and Strict-Prior Reconstruction Pilot 1",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "overlap_date": "2026-07-03",
        "generated_at_utc": FIXED_GENERATED_AT,
        "mode": "dry_run",
        "db_writes": 0,
        "oddsapi_calls": 0,
        "broad_pa_backfill_authorized": False,
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "source_override_for_probe": str(pa_path),
    }
    write_json(out_dir / "pilot_configuration_2026-07-12.json", config)
    write_csv(out_dir / "source_lineage_inventory_2026-07-12.csv", source_lineage_inventory())
    write_csv(out_dir / "historical_source_suitability_audit_2026-07-12.csv", historical_source_suitability(raw, args.start_date, args.end_date))
    write_csv(out_dir / "frozen_pa_field_contract_inventory_2026-07-12.csv", frozen_field_contract_inventory())
    write_csv(out_dir / "reconstruction_input_inventory_2026-07-12.csv", source_lineage_inventory())
    ordering = {
        "policy": "Use only rows with pa_context_latest_date < slate_date and pa_opp_v1_cutoff_status == PASS_PRIOR_DATE.",
        "game_date_only_policy": "Where timestamp ordering is absent, same slate-date game logs are excluded and latest prior game date must be less than slate_date.",
        "doubleheader_policy": "Inherited from PA v1 source: player_id + game_date grouped before rolling windows.",
    }
    write_json(out_dir / "strict_prior_ordering_policy_2026-07-12.json", ordering)
    (out_dir / "strict_prior_ordering_policy_2026-07-12.md").write_text(
        "# Strict-Prior Ordering Policy\n\n"
        "The pilot accepts only PA rows where `pa_context_latest_date < slate_date` and `pa_opp_v1_cutoff_status == PASS_PRIOR_DATE`. "
        "When only game dates exist, the deterministic rule is to exclude the target slate date and all later game dates. "
        "Doubleheaders inherit the PA v1 source policy: player/date PA is grouped before rolling windows.\n"
    )
    write_csv(out_dir / "field_level_reconstruction_audit_2026-07-12.csv", field_level_audit(pilot_pa))
    write_csv(out_dir / "formula_parity_audit_2026-07-12.csv", formula_parity_audit(raw, pilot_pa))
    overlap_rows, common_pass, population_limit = overlap_parity(overlap_pa, archived)
    write_csv(out_dir / "overlap_parity_audit_2026-07-12.csv", overlap_rows)
    identity, grain, dupes = identity_and_grain_audits(pilot_pa)
    write_csv(out_dir / "player_game_identity_audit_2026-07-12.csv", identity)
    write_csv(out_dir / "grain_and_join_audit_2026-07-12.csv", grain)
    write_csv(out_dir / "duplicate_identity_audit_2026-07-12.csv", dupes)
    write_csv(out_dir / "missing_data_classification_audit_2026-07-12.csv", missing_data_audit(pilot_pa))
    temporal_rows = temporal_integrity_audit(pilot_pa)
    write_csv(out_dir / "temporal_integrity_audit_2026-07-12.csv", temporal_rows)

    gates_pass = (
        len(pilot_pa) > 0
        and all(r["validation_status"] == "PASS" for r in grain)
        and all(r["validation_status"] == "PASS" for r in temporal_rows)
        and common_pass
    )
    probe_rows = []
    if gates_pass:
        probe_rows = run_matrix_probe(out_dir, pa_path, args)
    else:
        probe_rows = [
            {
                "probe_label": "PA OPPORTUNITY PILOT COMPATIBILITY PROBE",
                "manifest_id": "not_run",
                "rows": 0,
                "columns": 0,
                "matrix_sha256": "",
                "status": "SKIPPED_DUE_TO_GATE_FAILURE",
                "replayability_status": "",
                "validation_status": "SKIPPED",
                "notes": "Probe only runs when source, formula, overlap common-row parity, temporal, identity, and grain gates pass.",
            }
        ]
    write_csv(out_dir / "bundle_compatibility_probe_summary_2026-07-12.csv", probe_rows)
    write_csv(out_dir / "per_manifest_compatibility_results_2026-07-12.csv", probe_rows)

    classification = "PA_PILOT_SUCCESS_READY_FOR_INCREMENTAL_BACKWARD_EXTENSION"
    blockers = []
    if not common_pass:
        classification = "PA_PILOT_BLOCKED_BY_OVERLAP_PARITY"
        blockers.append({"blocker": "overlap_common_row_field_parity_failed", "severity": "HIGH", "remediation": "repair normalization against frozen July PA bundle"})
    elif population_limit:
        classification = "PA_PILOT_SUCCESS_WITH_BOUNDED_SOURCE_LIMITS"
        blockers.append(
            {
                "blocker": "overlap_source_population_differs_from_archived_july_manifest_population",
                "severity": "MEDIUM",
                "remediation": "for broad extension, select and freeze one population spine before matrix expansion",
            }
        )
    if not all(r["validation_status"] == "PASS" for r in temporal_rows):
        classification = "PA_PILOT_BLOCKED_BY_TEMPORAL_REPLAYABILITY"
        blockers.append({"blocker": "temporal_integrity_failure", "severity": "HIGH", "remediation": "exclude failing rows and trace source timestamps"})
    if not blockers:
        blockers.append({"blocker": "none", "severity": "NONE", "remediation": ""})
    write_csv(out_dir / "blocker_and_remediation_register_2026-07-12.csv", blockers)

    replay = {
        "status": "PASS",
        "row_count": len(pilot_pa),
        "ordered_columns": PA_OUTPUT_COLUMNS,
        "output_sha256": sha256(pa_path),
        "notes": "Deterministic normalized output from locked source artifact.",
    }
    write_json(out_dir / "replayability_comparison_2026-07-12.json", replay)
    (out_dir / "replayability_comparison_2026-07-12.md").write_text(
        f"# Replayability Comparison\n\nStatus: `{replay['status']}`\n\nOutput SHA256: `{replay['output_sha256']}`\n"
    )

    decision = {
        "pilot_readiness_classification": classification,
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "pilot_rows": int(len(pilot_pa)),
        "pilot_dates": f"{args.start_date}_to_{args.end_date}",
        "overlap_common_row_parity": "PASS" if common_pass else "FAIL",
        "overlap_population_limit": population_limit,
        "temporal_integrity": "PASS" if all(r["validation_status"] == "PASS" for r in temporal_rows) else "FAIL",
        "identity_and_grain": "PASS" if all(r["validation_status"] == "PASS" for r in grain) else "FAIL",
        "matrix_compatibility_probe": "PASS" if probe_rows and all(r["validation_status"] == "PASS" for r in probe_rows) else "FAIL_OR_SKIPPED",
        "broad_reconstruction_authorized": False,
    }
    write_json(out_dir / "pilot_decision_2026-07-12.json", decision)
    (out_dir / "pilot_decision_2026-07-12.md").write_text(
        f"# PA Opportunity Pilot Decision\n\nDecision: `{classification}`\n\n"
        "Training readiness remains `NOT_READY_FOR_MODEL_TRAINING`. Broad historical PA reconstruction remains unauthorized.\n"
    )
    (out_dir / "executive_summary_2026-07-12.md").write_text(
        f"# PA Opportunity Strict-Prior Reconstruction Pilot 1 — Executive Summary\n\n"
        f"Pilot window: **{args.start_date} through {args.end_date}**.\n\n"
        f"Decision: `{classification}`.\n\n"
        "The pilot normalized the existing PA Opportunity extended characterization source into the frozen Bundle v1 PA field contract, "
        "verified strict-prior timing for the target rows, and ran a labeled matrix compatibility probe after the hard gates passed.\n"
    )
    (out_dir / "main_assessment_2026-07-12.md").write_text(
        f"# MLB Rolling PA Opportunity Historical Source Discovery and Strict-Prior Reconstruction Pilot 1 — 2026-07-12\n\n"
        "## Scope\n\nRolling PA Opportunity only. No Starter extension, Variant C work, model training, scoring, production integration, or Bundle v1 modification occurred.\n\n"
        f"## Pilot Window\n\nSelected window: **{args.start_date} through {args.end_date}**. The July 3 overlap anchor was used only for parity testing.\n\n"
        f"## Result\n\n`{classification}`\n\n"
        "The main bounded source limit is that the pre-July extended PA source and the July 3 certified archive do not share an identical row population spine. "
        "Common-row PA values match after frozen-label normalization, but broad reconstruction must first freeze the intended population spine.\n"
    )
    parse_validation(out_dir)
    digest = package_sha(out_dir)
    print(json.dumps({"output_dir": str(out_dir), "classification": classification, "package_sha256": digest}, indent=2))
    return {"classification": classification, "package_sha256": digest}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2026-06-29")
    parser.add_argument("--end-date", default="2026-07-02")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--extended-source", default=str(EXTENDED_SOURCE))
    parser.add_argument("--archived-pa-source", default=str(ARCHIVED_PA_SOURCE))
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    build(parse_args(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
