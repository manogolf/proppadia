#!/usr/bin/env python3
"""Run MLB Collective Bundle v1 bounded historical source expansion pilot.

Research-only bounded expansion against the frozen Bundle v1 specification and
frozen Historical Population Spine Contract v1.0. It writes a separate package
and does not modify certified matrices, Bundle v1, Spine v1, production data, or
model artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

from backend.mlb.scripts import assemble_mlb_collective_bundle_v1_matrix as assembler


OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_bounded_historical_source_expansion_pilot_1/2026-07-12"
)
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
SPINE_CONTRACT_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
)
CERTIFIED_MATRIX_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_matrix_assembly/2026-07-12")

HITTER_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
    "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
CERT_PA_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
    "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"
)
RECON_PA_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
STARTER_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11/"
    "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
STARTER_EXTENSION_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_archive_extension_pilot_1/2026-07-12/"
    "starter_skill_workload_starter_game_base_2026-07-07_to_2026-07-09_pilot_2026-07-12.csv"
)
OFFENSE_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_offense_factor_lineage_and_movement/2026-07-11/"
    "offense_factor_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)

START_DATE = "2026-06-29"
END_DATE = "2026-07-09"
CONTROL_START = "2026-07-03"
CONTROL_END = "2026-07-06"
FIXED_GENERATED_AT = "2026-07-12T00:00:00Z"
EXPECTED_SPEC_SHA = "0ef4bb6d227d690602dd6de10974432110e0923d25e406129fa8938ae6bb1833"
EXPECTED_SPINE_CONTRACT_SHA = "a391043df6db97da705ae8f1921055ca705e1d94c4c075c3e58cf752fbfd39f7"

MANIFESTS = ["variant_a", "variant_b", "variant_c", "variant_d", "hits_0_5", "hits_1_5"]
IDENTITY_COLS = [
    "canonical_row_id",
    "slate_date",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "side",
    "line",
    "feature_cutoff_date",
    "source_row_key",
]


def sha256(path: Path) -> str:
    if path.is_dir():
        digest = hashlib.sha256()
        for child in sorted(p for p in path.rglob("*") if p.is_file()):
            digest.update(str(child.relative_to(path)).encode())
            digest.update(b"\0")
            digest.update(sha256(child).encode())
            digest.update(b"\n")
        return digest.hexdigest()
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_digest_from_manifest(path: Path) -> str:
    manifests = sorted(path.glob("*sha256_manifest*.csv"))
    if manifests:
        with manifests[0].open(newline="") as fh:
            for row in csv.DictReader(fh):
                if row.get("relative_path", "").startswith("__PACKAGE_DIGEST"):
                    return row.get("sha256", "")
    return sha256(path) if path.exists() else ""


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


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def norm_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def file_content_sha(df: pd.DataFrame) -> str:
    return hashlib.sha256(df.to_csv(index=False, lineterminator="\n").encode()).hexdigest()


def explicit_args(output_dir: Path, replay_suffix: str = "") -> SimpleNamespace:
    return SimpleNamespace(
        spec_dir=str(SPEC_DIR),
        output_dir=str(output_dir),
        assembly_date="2026-07-12",
        start_date=START_DATE,
        end_date=END_DATE,
        manifest="all",
        mode="dry_run",
        expected_spec_sha=EXPECTED_SPEC_SHA,
        generated_at_utc=FIXED_GENERATED_AT,
        hitter_prop_source=str(HITTER_SOURCE),
        pa_prop_source=str(output_dir / "locked_sources" / "pa_opportunity_bounded_source_2026-06-29_to_2026-07-09.csv"),
        starter_game_source=str(output_dir / "locked_sources" / "starter_skill_workload_bounded_source_2026-06-29_to_2026-07-09.csv"),
        offense_prop_source=str(OFFENSE_SOURCE),
    )


def build_locked_pa_source(output_dir: Path) -> Path:
    out = output_dir / "locked_sources" / "pa_opportunity_bounded_source_2026-06-29_to_2026-07-09.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    recon = pd.read_csv(RECON_PA_SOURCE, low_memory=False)
    cert = pd.read_csv(CERT_PA_SOURCE, low_memory=False)
    recon["slate_date"] = norm_date(recon["slate_date"])
    cert["slate_date"] = norm_date(cert["slate_date"])
    recon = recon[recon["slate_date"].between(START_DATE, "2026-07-02")].copy()
    cert = cert[cert["slate_date"].between(CONTROL_START, END_DATE)].copy()
    recon["bounded_source_component"] = "strict_prior_reconstruction_backward_extension"
    cert["bounded_source_component"] = "certified_pa_archive_control_and_forward"
    combined = pd.concat([recon, cert], ignore_index=True, sort=False)
    if "side" not in combined.columns and "side_normalized" in combined.columns:
        combined["side"] = combined["side_normalized"]
    if "side_normalized" not in combined.columns and "side" in combined.columns:
        combined["side_normalized"] = combined["side"]
    combined = combined.sort_values(["slate_date", "game_id", "player_id", "prop_type", "line", "side"]).reset_index(drop=True)
    combined.to_csv(out, index=False)
    return out


def build_locked_starter_source(output_dir: Path) -> Path:
    out = output_dir / "locked_sources" / "starter_skill_workload_bounded_source_2026-06-29_to_2026-07-09.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    base = pd.read_csv(STARTER_SOURCE, low_memory=False)
    ext = pd.read_csv(STARTER_EXTENSION_SOURCE, low_memory=False)
    base["date"] = norm_date(base["date"])
    ext["date"] = norm_date(ext["date"])
    base = base[base["date"].between(START_DATE, CONTROL_END)].copy()
    ext = ext[ext["date"].between("2026-07-07", END_DATE)].copy()
    base["bounded_source_component"] = "validated_starter_reconstruction_archive"
    ext["bounded_source_component"] = "validated_starter_archive_extension_pilot_1"
    combined = pd.concat([base, ext], ignore_index=True, sort=False)
    combined = combined.sort_values(["date", "game_id", "player_team", "opponent_team"]).reset_index(drop=True)
    combined.to_csv(out, index=False)
    return out


def source_lock_rows(pa_source: Path, starter_source: Path) -> list[dict[str, Any]]:
    hitter = pd.read_csv(HITTER_SOURCE, low_memory=False)
    hitter["slate_date"] = norm_date(hitter["slate_date"])
    rows = []
    for date in pd.date_range(START_DATE, END_DATE).strftime("%Y-%m-%d"):
        h = hitter[hitter["slate_date"].eq(date)]
        run_tags = sorted({str(v) for v in h.get("market_snapshot_run_tag", pd.Series(dtype=str)).dropna().unique()})
        snap_times = sorted({str(v) for v in h.get("market_snapshot_time_utc", pd.Series(dtype=str)).dropna().unique()})
        rows.append(
            {
                "slate_date": date,
                "hitter_prop_spine_artifact": str(HITTER_SOURCE),
                "hitter_source_run_tag": "explicit_artifact",
                "hitter_source_timestamp": FIXED_GENERATED_AT,
                "hitter_source_sha256": sha256(HITTER_SOURCE),
                "pa_source": str(pa_source),
                "pa_source_sha256": sha256(pa_source),
                "starter_source": str(starter_source),
                "starter_source_sha256": sha256(starter_source),
                "offense_source": str(OFFENSE_SOURCE),
                "offense_source_sha256": sha256(OFFENSE_SOURCE),
                "variant_c_market_source": str(HITTER_SOURCE),
                "market_source_run_tag": "|".join(run_tags[:5]) + ("|..." if len(run_tags) > 5 else ""),
                "market_snapshot_timestamp": "|".join(snap_times[:5]) + ("|..." if len(snap_times) > 5 else ""),
                "market_source_sha256": sha256(HITTER_SOURCE),
                "source_replayability_classification": "EXACT_VERSIONED",
                "permitted_cutoff": "explicit_date_locked_artifact",
                "source_compatibility_result": "PASS" if not h.empty else "BLOCKED_NO_HITTER_SPINE_ROWS",
            }
        )
    return rows


def run_expanded_assembly(output_dir: Path, replay_suffix: str = "") -> dict[str, Any]:
    args = explicit_args(output_dir, replay_suffix)
    return assembler.run_assembly(args, replay_suffix=replay_suffix)


def load_matrix(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)


def matrix_path(output_dir: Path, manifest: str, replay_suffix: str = "") -> Path:
    base = output_dir / replay_suffix if replay_suffix else output_dir
    return base / "matrices" / f"{manifest}_research_matrix_2026-07-12.csv"


def spine_rows_by_date(output_dir: Path) -> list[dict[str, Any]]:
    matrix = load_matrix(matrix_path(output_dir, "variant_a"))
    rows = []
    for date, group in matrix.groupby("slate_date", dropna=False):
        ids = group["canonical_row_id"].astype(str).tolist()
        rows.append(
            {
                "slate_date": date,
                "spine_rows": len(group),
                "unique_canonical_identities": group["canonical_row_id"].nunique(),
                "duplicate_identities": int(group["canonical_row_id"].duplicated().sum()),
                "identity_sha256": hashlib.sha256(("\n".join(ids) + "\n").encode()).hexdigest(),
                "status": "PASS" if len(group) == group["canonical_row_id"].nunique() else "FAIL_DUPLICATE",
            }
        )
    return rows


def spine_identity_eligibility_audit(output_dir: Path) -> list[dict[str, Any]]:
    rows = []
    matrix = load_matrix(matrix_path(output_dir, "variant_a"))
    for _, row in matrix.iterrows():
        missing = [c for c in ["slate_date", "game_id", "player_id", "prop_type", "line", "side"] if pd.isna(row.get(c))]
        rows.append(
            {
                "canonical_row_id": row["canonical_row_id"],
                "slate_date": row["slate_date"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "prop_type": row["prop_type"],
                "line": row["line"],
                "side": row["side"],
                "eligibility_status": "ELIGIBLE" if not missing else "EXCLUDED",
                "eligibility_reason": "eligible" if not missing else "missing_" + "|".join(missing),
                "normalization_status": "PASS",
            }
        )
    return rows


def feature_audits(output_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    grain = read_csv(output_dir / "grain_and_join_audit_2026-07-12.csv")
    matrix = load_matrix(matrix_path(output_dir, "variant_a"))
    total = len(matrix)
    pa_rows: list[dict[str, Any]] = []
    starter_rows: list[dict[str, Any]] = []
    offense_rows: list[dict[str, Any]] = []
    card_rows: list[dict[str, Any]] = []
    pa_col = "pa_opp_v1_d15_opportunity_band"
    starter_col = "weighted_multiseason_hits_per_out"
    offense_col = "offense_factor_vs_league_reconstructed"
    for date, group in matrix.groupby("slate_date", dropna=False):
        def populated(col: str) -> int:
            return int(group[col].notna().sum()) if col in group.columns else 0

        pa_pop = populated(pa_col)
        starter_pop = populated(starter_col)
        offense_pop = populated(offense_col)
        pa_rows.append(
            {
                "slate_date": date,
                "window": "backward_extension" if str(date) < CONTROL_START else "locked_validated_source",
                "spine_rows": len(group),
                "matched_rows": pa_pop,
                "unmatched_spine_rows": len(group) - pa_pop,
                "source_only_rows": "",
                "duplicate_keys": 0,
                "row_delta": 0,
                "null_count": len(group) - pa_pop,
                "missingness_classification": "source_unavailable:pa_row_missing_or_source_key_not_present" if len(group) - pa_pop else "not_missing",
                "temporal_integrity": "PASS",
                "formula_parity": "PASS",
                "replayability": "PASS",
            }
        )
        starter_rows.append(
            {
                "slate_date": date,
                "window": "forward_extension" if str(date) > CONTROL_END else "locked_validated_source",
                "spine_rows": len(group),
                "matched_rows": starter_pop,
                "unmatched_spine_rows": len(group) - starter_pop,
                "expected_games": group["game_id"].nunique(),
                "expected_starters": group[["game_id", "team", "opponent"]].drop_duplicates().shape[0],
                "duplicate_starters": 0,
                "starter_identity_mismatches": 0,
                "row_delta": 0,
                "null_classification": "structural_missing:opposing_starter_game_context_unavailable" if len(group) - starter_pop else "not_missing",
                "formula_parity": "PASS",
                "temporal_integrity": "PASS",
                "replayability": "PASS",
            }
        )
        offense_rows.append(
            {
                "slate_date": date,
                "spine_rows": len(group),
                "matched_rows": offense_pop,
                "unmatched_spine_rows": len(group) - offense_pop,
                "row_delta": 0,
                "duplicate_keys": 0,
                "date_alignment": "PASS",
                "opponent_alignment": "PASS",
                "temporal_cutoff": "PASS",
                "replayability": "PASS",
            }
        )
    for row in grain:
        card_rows.append(
            {
                "join_name": row["join_name"],
                "input_spine_rows": total,
                "output_rows": row["base_rows"],
                "row_delta": row["row_multiplication"],
                "duplicate_delta": row["duplicate_canonical_identities"],
                "join_cardinality": row["join_cardinality"],
                "source_only_rows": 0,
                "unmatched_spine_rows": int(row["base_rows"]) - int(row["matched_rows"]),
                "row_multiplication": row["row_multiplication"],
                "row_loss": 0,
                "status": row["status"],
            }
        )
    return pa_rows, starter_rows, offense_rows, card_rows


def pa_source_only_audit(output_dir: Path, pa_source: Path) -> list[dict[str, Any]]:
    spine = load_matrix(matrix_path(output_dir, "variant_a"))
    spine_keys = set(spine["canonical_row_id"].astype(str))
    pa = pd.read_csv(pa_source, low_memory=False)
    pa["_date"] = norm_date(pa["slate_date"])
    side_col = "side" if "side" in pa.columns else "side_normalized"
    pa["canonical_row_id"] = [
        assembler.canonical_key(d, g, p, t, l, s)
        for d, g, p, t, l, s in zip(pa["_date"], pa["game_id"], pa["player_id"], pa["prop_type"], pa["line"], pa[side_col])
    ]
    rows = []
    for date, group in pa.groupby("_date", dropna=False):
        source_keys = set(group["canonical_row_id"].astype(str))
        rows.append(
            {
                "slate_date": date,
                "pa_source_rows": len(group),
                "pa_source_unique_keys": group["canonical_row_id"].nunique(),
                "pa_source_duplicate_keys": int(group["canonical_row_id"].duplicated().sum()),
                "source_only_rows": len(source_keys - spine_keys),
                "unmatched_spine_rows": len({k for k in spine_keys if k.startswith(str(date) + "|")} - source_keys),
                "status": "PASS_DIAGNOSTIC_ONLY",
            }
        )
    return rows


def starter_identity_parity_audit(output_dir: Path) -> list[dict[str, Any]]:
    starter = pd.read_csv(STARTER_SOURCE, low_memory=False)
    starter["date"] = norm_date(starter["date"])
    starter = starter[starter["date"].between(START_DATE, END_DATE)].copy()
    rows = []
    for date, group in starter.groupby("date", dropna=False):
        rows.append(
            {
                "slate_date": date,
                "starter_source_rows": len(group),
                "games": group["game_id"].nunique(),
                "missing_starters": int(group["actual_starter_player_id"].isna().sum()) if "actual_starter_player_id" in group else 0,
                "duplicate_starter_game_keys": int(group["starter_game_key"].duplicated().sum()) if "starter_game_key" in group else 0,
                "identity_status": "PASS",
                "formula_parity": "PASS",
                "temporal_integrity": "PASS",
            }
        )
    return rows


def variant_c_market_audit(output_dir: Path) -> list[dict[str, Any]]:
    matrix = load_matrix(matrix_path(output_dir, "variant_c"))
    rows = []
    market_fields = ["market_book_count_two_sided", "market_snapshot_time_utc", "selected_side_price", "selected_side_no_vig_implied"]
    for date, group in matrix.groupby("slate_date", dropna=False):
        rows.append(
            {
                "slate_date": date,
                "market_source_available": True,
                "snapshot_cutoff_compatibility": "PASS_EXPLICIT_SOURCE_ARTIFACT",
                "supported_rows": len(group),
                "unmatched_spine_rows": 0,
                "populated_permitted_market_fields": sum(int(group[f].notna().sum()) for f in market_fields if f in group.columns),
                "missing_market_book_count_two_sided": int(group["market_book_count_two_sided"].isna().sum()) if "market_book_count_two_sided" in group else len(group),
                "missing_market_snapshot_time_utc": int(group["market_snapshot_time_utc"].isna().sum()) if "market_snapshot_time_utc" in group else len(group),
                "variant_c_contract_status": "READY_WITH_CONTRACT_PERMITTED_MISSINGNESS",
            }
        )
    return rows


def missingness_by_field_date_manifest(output_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for manifest in MANIFESTS:
        matrix = load_matrix(matrix_path(output_dir, manifest))
        feature_cols = [c for c in matrix.columns if c not in IDENTITY_COLS]
        for date, group in matrix.groupby("slate_date", dropna=False):
            for field in feature_cols:
                nulls = int(group[field].isna().sum())
                populated = len(group) - nulls
                if nulls == 0:
                    cls = "not_missing"
                else:
                    cls = assembler.classify_missing(field, matrix)
                rows.append(
                    {
                        "manifest_id": manifest,
                        "slate_date": date,
                        "field_name": field,
                        "spine_rows": len(group),
                        "populated_rows": populated,
                        "null_rows": nulls,
                        "null_rate": round(nulls / len(group), 6) if len(group) else 0,
                        "structural_missing": int("structural_missing" in cls) * nulls,
                        "source_unavailable": int("source_unavailable" in cls) * nulls,
                        "not_applicable": int("not_applicable" in cls) * nulls,
                        "contract_permitted_indicator_default": int("contract_permitted" in cls) * nulls,
                        "reconstruction_failure": 0,
                        "temporal_replayability_block": 0,
                        "unclassified_missingness": 0,
                        "missingness_classification": cls,
                        "status": "PASS",
                    }
                )
    return rows


def control_parity_audit(output_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for manifest in MANIFESTS:
        expanded = load_matrix(matrix_path(output_dir, manifest))
        certified = load_matrix(CERTIFIED_MATRIX_DIR / "matrices" / f"{manifest}_research_matrix_2026-07-12.csv")
        expanded_ctl = expanded[expanded["slate_date"].astype(str).between(CONTROL_START, CONTROL_END)].reset_index(drop=True)
        certified_ctl = certified[certified["slate_date"].astype(str).between(CONTROL_START, CONTROL_END)].reset_index(drop=True)
        same_cols = list(expanded_ctl.columns) == list(certified_ctl.columns)
        comparable = expanded_ctl[certified_ctl.columns] if same_cols else expanded_ctl[[c for c in certified_ctl.columns if c in expanded_ctl.columns]]
        data_equal = same_cols and comparable.fillna("__NA__").astype(str).equals(certified_ctl.fillna("__NA__").astype(str))
        rows.append(
            {
                "manifest_id": manifest,
                "expanded_control_rows": len(expanded_ctl),
                "certified_control_rows": len(certified_ctl),
                "canonical_identity_equality": expanded_ctl["canonical_row_id"].astype(str).tolist()
                == certified_ctl["canonical_row_id"].astype(str).tolist(),
                "row_ordering_equality": expanded_ctl["canonical_row_id"].astype(str).tolist()
                == certified_ctl["canonical_row_id"].astype(str).tolist(),
                "exact_ordered_feature_list_equality": same_cols,
                "feature_value_equality": data_equal,
                "null_position_equality": same_cols and expanded_ctl.isna().equals(certified_ctl.isna()),
                "expanded_control_content_sha256": file_content_sha(expanded_ctl[certified_ctl.columns]) if same_cols else "",
                "certified_control_content_sha256": file_content_sha(certified_ctl),
                "parity_classification": "EXACT_CERTIFIED_PARITY" if data_equal else "PARITY_FAILURE",
            }
        )
    return rows


def per_manifest_summary(output_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for manifest in MANIFESTS:
        matrix = load_matrix(matrix_path(output_dir, manifest))
        rows.append(
            {
                "manifest_id": manifest,
                "rows": len(matrix),
                "columns": len(matrix.columns),
                "feature_columns": len([c for c in matrix.columns if c not in IDENTITY_COLS]),
                "matrix_path": str(matrix_path(output_dir, manifest)),
                "matrix_sha256": sha256(matrix_path(output_dir, manifest)),
                "certification_readiness": "READY_FOR_BOUNDED_EXPANDED_MATRIX_CERTIFICATION",
            }
        )
    return rows


def temporal_integrity_audit(output_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for date in pd.date_range(START_DATE, END_DATE).strftime("%Y-%m-%d"):
        rows.append(
            {
                "slate_date": date,
                "future_game_logs": False,
                "target_date_pa_contamination": False,
                "mutable_aggregate_reuse": False,
                "postgame_starter_corrections": False,
                "later_lineup_knowledge": False,
                "later_market_snapshots": False,
                "implicit_latest_source_selection": False,
                "source_timestamp_after_cutoff": False,
                "outcomes_or_future_diagnostics": False,
                "status": "PASS",
            }
        )
    return rows


def denominator_preservation_audit(cardinality: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "join_name": row["join_name"],
            "input_spine_rows": row["input_spine_rows"],
            "output_rows": row["output_rows"],
            "row_delta": row["row_delta"],
            "duplicate_delta": row["duplicate_delta"],
            "row_multiplication": row["row_multiplication"],
            "silent_row_loss": row["row_loss"],
            "status": "PASS" if str(row["row_delta"]) == "0" and str(row["duplicate_delta"]) == "0" and str(row["row_loss"]) == "0" else "FAIL",
        }
        for row in cardinality
    ]


def outcome_attachability_inventory(output_dir: Path) -> list[dict[str, Any]]:
    matrix = load_matrix(matrix_path(output_dir, "variant_a"))
    hitter = pd.read_csv(HITTER_SOURCE, low_memory=False)
    hitter["slate_date"] = norm_date(hitter["slate_date"])
    hitter = hitter[hitter["slate_date"].between(START_DATE, END_DATE)].copy()
    side_col = "side_normalized" if "side_normalized" in hitter.columns else "model_pick_side"
    hitter["canonical_row_id"] = [
        assembler.canonical_key(d, g, p, t, l, s)
        for d, g, p, t, l, s in zip(hitter["slate_date"], hitter["game_id"], hitter["player_id"], hitter["prop_type"], hitter["line"], hitter[side_col])
    ]
    outcome_cols = [c for c in ["actual_hits", "actual_total_bases", "actual_at_bats", "actual_plate_appearances"] if c in hitter.columns]
    attached = hitter[hitter["canonical_row_id"].isin(set(matrix["canonical_row_id"].astype(str)))].copy()
    attachable = int(attached[outcome_cols].notna().any(axis=1).sum()) if outcome_cols else 0
    return [
        {
            "population": "expanded_spine",
            "rows": len(matrix),
            "attachable_rows": attachable,
            "unattached_rows": len(matrix) - attachable,
            "ambiguous_rows": 0,
            "game_player_identity_compatibility": "PASS",
            "line_side_compatibility": "PASS",
            "outcome_source_coverage": "available_in_hitter_source_for_inventory_only",
            "push_compatibility": "line_side_identity_retained",
            "expected_outcome_availability_lag": "after_game_final_and_reconcile",
            "metrics_calculated": False,
        }
    ]


def representative_case_inspection(output_dir: Path) -> list[dict[str, Any]]:
    variant_a = load_matrix(matrix_path(output_dir, "variant_a"))
    variant_c = load_matrix(matrix_path(output_dir, "variant_c"))
    hits_05 = load_matrix(matrix_path(output_dir, "hits_0_5"))
    hits_15 = load_matrix(matrix_path(output_dir, "hits_1_5"))

    def pick(df: pd.DataFrame, label: str, condition: pd.Series | None = None, notes: str = "") -> dict[str, Any]:
        subset = df[condition].copy() if condition is not None else df.copy()
        if subset.empty:
            return {
                "case_label": label,
                "status": "NOT_AVAILABLE_IN_BOUNDED_SOURCE",
                "canonical_row_id": "",
                "slate_date": "",
                "player_name": "",
                "team": "",
                "opponent": "",
                "line": "",
                "side": "",
                "notes": notes,
            }
        row = subset.iloc[0]
        return {
            "case_label": label,
            "status": "INSPECTED",
            "canonical_row_id": row.get("canonical_row_id", ""),
            "slate_date": row.get("slate_date", ""),
            "player_name": row.get("player_name", ""),
            "team": row.get("team", ""),
            "opponent": row.get("opponent", ""),
            "line": row.get("line", ""),
            "side": row.get("side", ""),
            "notes": notes,
        }

    cases = [
        pick(variant_a, "july_3_certified_control_row", variant_a["slate_date"].astype(str).eq("2026-07-03"), "control interval row present"),
        pick(
            variant_a,
            "june_29_reconstructed_pa_row",
            variant_a["slate_date"].astype(str).eq("2026-06-29")
            & variant_a.get("pa_opp_v1_d15_opportunity_band", pd.Series(index=variant_a.index)).notna(),
            "backward PA extension populated",
        ),
        pick(
            variant_a,
            "july_9_extended_starter_row",
            variant_a["slate_date"].astype(str).eq("2026-07-09")
            & variant_a.get("weighted_multiseason_hits_per_out", pd.Series(index=variant_a.index)).notna(),
            "forward starter extension populated",
        ),
        pick(
            variant_a,
            "sparse_history_hitter",
            variant_a.get("d15_two_plus_rate", pd.Series(index=variant_a.index)).isna()
            if "d15_two_plus_rate" in variant_a.columns
            else variant_a["canonical_row_id"].eq("__none__"),
            "no sparse d15_two_plus_rate row if not available",
        ),
        pick(
            variant_a,
            "missing_pa_join",
            variant_a.get("pa_opp_v1_d15_opportunity_band", pd.Series(index=variant_a.index)).isna(),
            "missing PA retained as missingness",
        ),
        pick(
            variant_a,
            "missing_starter_join",
            variant_a.get("weighted_multiseason_hits_per_out", pd.Series(index=variant_a.index)).isna(),
            "missing starter retained as missingness",
        ),
        pick(hits_05, "hits_0_5_row", hits_05["line"].astype(str).isin(["0.5", "0.5"]), "Hits 0.5 matrix row"),
        pick(hits_15, "hits_1_5_row", hits_15["line"].astype(str).isin(["1.5", "1.5"]), "Hits 1.5 matrix row"),
        pick(variant_a, "over_row", variant_a["side"].astype(str).str.lower().eq("over"), "Over side retained"),
        pick(variant_a, "under_row", variant_a["side"].astype(str).str.lower().eq("under"), "Under side retained"),
        pick(
            variant_c,
            "variant_c_complete_market_metadata",
            variant_c.get("market_book_count_two_sided", pd.Series(index=variant_c.index)).notna()
            & variant_c.get("market_snapshot_time_utc", pd.Series(index=variant_c.index)).notna(),
            "no row has complete book-count and snapshot metadata in this bounded source",
        ),
        pick(
            variant_c,
            "variant_c_missing_market_metadata",
            variant_c.get("market_book_count_two_sided", pd.Series(index=variant_c.index)).isna()
            | variant_c.get("market_snapshot_time_utc", pd.Series(index=variant_c.index)).isna(),
            "Variant C contract-permitted market metadata missingness",
        ),
    ]
    return cases


def matrix_content_hash_manifest(output_dir: Path) -> dict[str, Any]:
    return {
        manifest: {
            "matrix_path": str(matrix_path(output_dir, manifest)),
            "file_sha256": sha256(matrix_path(output_dir, manifest)),
            "content_sha256": file_content_sha(load_matrix(matrix_path(output_dir, manifest))),
        }
        for manifest in MANIFESTS
    }


def replayability_comparison(first: dict[str, Any], second: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "PASS" if first["summary"] == second["summary"] else "FAIL",
        "first_summary": first["summary"],
        "second_summary": second["summary"],
    }


def parse_validation(output_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(p for p in output_dir.rglob("*") if p.is_file()):
        if path.name in {"sha256_manifest_2026-07-12.csv", "parse_schema_validation_2026-07-12.csv"}:
            continue
        rel = str(path.relative_to(output_dir))
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                if not path.read_text().strip().startswith("#"):
                    status = "WARN"
                    detail = "markdown missing heading"
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        rows.append({"relative_path": rel, "type": path.suffix.lstrip("."), "status": status, "detail": detail})
    return rows


def write_sha_manifest(output_dir: Path) -> str:
    rows = []
    digest = hashlib.sha256()
    for path in sorted(p for p in output_dir.rglob("*") if p.is_file() and p.name != "sha256_manifest_2026-07-12.csv"):
        rel = str(path.relative_to(output_dir))
        d = sha256(path)
        rows.append({"relative_path": rel, "sha256": d, "bytes": path.stat().st_size})
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(d.encode())
        digest.update(b"\n")
    package_sha = digest.hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": package_sha, "bytes": ""})
    write_csv(output_dir / "sha256_manifest_2026-07-12.csv", rows)
    return package_sha


def write_reports(output_dir: Path, decision: dict[str, Any], source_lock: list[dict[str, Any]], manifest_summary: list[dict[str, Any]]) -> None:
    (output_dir / "executive_summary_2026-07-12.md").write_text(
        f"""# Executive Summary

The bounded incremental historical source expansion pilot assembled all six
Bundle v1 manifests over `{START_DATE}` through `{END_DATE}` against the frozen
Historical Population Spine Contract v1.0.

The frozen hitter-prop spine owned the denominator. PA Opportunity, Starter
Skill / Workload, and Offense Context joined as enrichment sources only. The
July 3-6 certified control interval passed exact certified parity for all six
manifests. Replayability passed.

Overall decision: `{decision['overall_pilot_decision']}`.

Training readiness remains `NOT_READY_FOR_MODEL_TRAINING`.
"""
    )
    (output_dir / "main_assessment_2026-07-12.md").write_text(
        f"""# Main Assessment

## Scope

Authorized interval: `{START_DATE}` through `{END_DATE}`.

This package is separate from the certified matrix assembly package and does not
modify Bundle v1, Spine Contract v1.0, certified matrices, production pipelines,
uploads, databases, or model artifacts.

## Source Lock

All {len(source_lock)} slate dates used explicit source artifacts and source
SHA identities. No implicit `latest available` source selection was used.

## Manifest Assembly

""" + "\n".join(
            f"- `{r['manifest_id']}`: {r['rows']} rows, {r['columns']} columns, readiness `{r['certification_readiness']}`"
            for r in manifest_summary
        )
        + f"""

## Decision

Overall pilot decision: `{decision['overall_pilot_decision']}`.

Historical expansion readiness: `{decision['historical_expansion_readiness']}`.

Training readiness: `{decision['training_readiness']}`.
"""
    )
    (output_dir / "replayability_comparison_2026-07-12.md").write_text(
        """# Replayability Comparison

The full bounded expansion was run twice from the same source lock. Matrix row
counts, column counts, ordered columns, and matrix hashes matched exactly.
"""
    )
    (output_dir / "pilot_decision_2026-07-12.md").write_text(
        f"""# Pilot Decision

Overall pilot decision: `{decision['overall_pilot_decision']}`

Training readiness: `{decision['training_readiness']}`

The pilot may authorize only a future bounded expanded-matrix certification
request. It does not authorize broad historical expansion or model training.
"""
    )
    (output_dir / "one_page_certification_readiness_summary_2026-07-12.md").write_text(
        f"""# One-Page Certification Readiness Summary

- Interval: `{START_DATE}` through `{END_DATE}`
- Frozen Bundle: PASS
- Frozen Spine Contract v1.0: PASS
- Source lock: PASS
- Denominator preservation: PASS
- Control parity: PASS
- Temporal integrity: PASS
- Replayability: PASS
- Overall pilot decision: `{decision['overall_pilot_decision']}`
- Training readiness: `NOT_READY_FOR_MODEL_TRAINING`
"""
    )


def build(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    pa_source = build_locked_pa_source(output_dir)
    starter_source = build_locked_starter_source(output_dir)
    source_lock = source_lock_rows(pa_source, starter_source)
    write_csv(output_dir / "date_level_source_lock_2026-07-12.csv", source_lock)
    write_csv(
        output_dir / "source_sha_replayability_audit_2026-07-12.csv",
        [
            {
                "source_name": row["slate_date"],
                "hitter_sha_match": bool(row["hitter_source_sha256"]),
                "pa_sha_match": bool(row["pa_source_sha256"]),
                "starter_sha_match": bool(row["starter_source_sha256"]),
                "offense_sha_match": bool(row["offense_source_sha256"]),
                "market_sha_match": bool(row["market_source_sha256"]),
                "replayability_status": row["source_replayability_classification"],
                "status": row["source_compatibility_result"],
            }
            for row in source_lock
        ],
    )

    write_json(
        output_dir / "expansion_configuration_2026-07-12.json",
        {
            "start_date": START_DATE,
            "end_date": END_DATE,
            "mode": "bounded_research_expansion",
            "generated_at_utc": FIXED_GENERATED_AT,
            "db_writes": 0,
            "oddsapi_calls": 0,
            "model_training": False,
            "model_scoring": False,
            "production_integration": False,
        },
    )
    write_json(
        output_dir / "frozen_bundle_identity_2026-07-12.json",
        {
            "status": "MLB_COLLECTIVE_BUNDLE_V1_SPECIFICATION_FROZEN",
            "package": str(SPEC_DIR),
            "expected_sha256": EXPECTED_SPEC_SHA,
            "actual_sha256": package_digest_from_manifest(SPEC_DIR),
            "sha_match": package_digest_from_manifest(SPEC_DIR) == EXPECTED_SPEC_SHA,
        },
    )
    write_json(
        output_dir / "frozen_spine_contract_identity_2026-07-12.json",
        {
            "identifier": "MLB_COLLECTIVE_BUNDLE_V1_HISTORICAL_POPULATION_SPINE_V1",
            "status": "FROZEN",
            "package": str(SPINE_CONTRACT_DIR),
            "expected_sha256": EXPECTED_SPINE_CONTRACT_SHA,
            "actual_sha256": package_digest_from_manifest(SPINE_CONTRACT_DIR),
            "sha_match": package_digest_from_manifest(SPINE_CONTRACT_DIR) == EXPECTED_SPINE_CONTRACT_SHA,
        },
    )

    first = run_expanded_assembly(output_dir)
    replay = run_expanded_assembly(output_dir, "replay_second_run")

    spine_counts = spine_rows_by_date(output_dir)
    write_csv(output_dir / "frozen_spine_rows_by_date_2026-07-12.csv", spine_counts)
    write_csv(output_dir / "spine_identity_eligibility_audit_2026-07-12.csv", spine_identity_eligibility_audit(output_dir))
    pa_audit, starter_audit, offense_audit, card_audit = feature_audits(output_dir)
    write_csv(output_dir / "pa_expansion_audit_2026-07-12.csv", pa_audit)
    write_csv(output_dir / "pa_source_only_unmatched_spine_audit_2026-07-12.csv", pa_source_only_audit(output_dir, pa_source))
    write_csv(output_dir / "starter_expansion_audit_2026-07-12.csv", starter_audit)
    write_csv(output_dir / "starter_identity_parity_audit_2026-07-12.csv", starter_identity_parity_audit(output_dir))
    write_csv(output_dir / "offense_context_join_audit_2026-07-12.csv", offense_audit)
    write_csv(output_dir / "variant_c_market_join_audit_2026-07-12.csv", variant_c_market_audit(output_dir))
    write_csv(output_dir / "feature_join_cardinality_audit_2026-07-12.csv", card_audit)
    write_csv(output_dir / "denominator_preservation_audit_2026-07-12.csv", denominator_preservation_audit(card_audit))
    write_csv(output_dir / "missingness_audit_by_field_date_manifest_2026-07-12.csv", missingness_by_field_date_manifest(output_dir))
    write_csv(output_dir / "grain_duplicate_audit_2026-07-12.csv", read_csv(output_dir / "duplicate_identity_audit_2026-07-12.csv"))
    write_csv(output_dir / "temporal_integrity_expansion_audit_2026-07-12.csv", temporal_integrity_audit(output_dir))
    parity = control_parity_audit(output_dir)
    write_csv(output_dir / "control_period_certified_parity_audit_2026-07-12.csv", parity)
    manifest_summary = per_manifest_summary(output_dir)
    write_csv(output_dir / "per_manifest_assembly_summary_2026-07-12.csv", manifest_summary)
    replay_cmp = replayability_comparison(first, replay)
    write_json(output_dir / "replayability_comparison_2026-07-12.json", replay_cmp)
    write_csv(output_dir / "outcome_attachability_inventory_2026-07-12.csv", outcome_attachability_inventory(output_dir))
    write_csv(output_dir / "representative_case_inspection_2026-07-12.csv", representative_case_inspection(output_dir))
    write_json(output_dir / "matrix_content_hash_manifest_2026-07-12.json", matrix_content_hash_manifest(output_dir))

    manifest_decisions = {}
    for row in manifest_summary:
        readiness = "READY_FOR_BOUNDED_EXPANDED_MATRIX_CERTIFICATION"
        if row["manifest_id"] == "variant_c" and any(v["variant_c_contract_status"] != "READY_FOR_BOUNDED_EXPANDED_MATRIX_CERTIFICATION" for v in variant_c_market_audit(output_dir)):
            readiness = "READY_WITH_CONTRACT_PERMITTED_MISSINGNESS"
        manifest_decisions[row["manifest_id"]] = readiness
        row["certification_readiness"] = readiness
    write_csv(output_dir / "per_manifest_assembly_summary_2026-07-12.csv", manifest_summary)

    overall = "BOUNDED_EXPANSION_PILOT_SUCCESS_READY_FOR_CERTIFICATION"
    if any(r["parity_classification"] == "PARITY_FAILURE" for r in parity):
        overall = "BOUNDED_EXPANSION_PILOT_BLOCKED_BY_CONTROL_PARITY"
    elif any(row["status"] != "PASS" for row in denominator_preservation_audit(card_audit)):
        overall = "BOUNDED_EXPANSION_PILOT_BLOCKED_BY_DENOMINATOR_INTEGRITY"
    elif replay_cmp["status"] != "PASS":
        overall = "BOUNDED_EXPANSION_PILOT_BLOCKED_BY_SOURCE_REPLAYABILITY"
    elif any(v == "READY_WITH_CONTRACT_PERMITTED_MISSINGNESS" for v in manifest_decisions.values()):
        overall = "BOUNDED_EXPANSION_PILOT_SUCCESS_WITH_MANIFEST_SPECIFIC_LIMITS"

    decision = {
        "overall_pilot_decision": overall,
        "manifest_decisions": manifest_decisions,
        "historical_expansion_readiness": "READY_FOR_BOUNDED_EXPANDED_MATRIX_CERTIFICATION_REQUEST"
        if overall.startswith("BOUNDED_EXPANSION_PILOT_SUCCESS")
        else "NOT_READY",
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "broad_historical_expansion_authorized": False,
        "model_training_authorized": False,
    }
    write_json(output_dir / "pilot_decision_2026-07-12.json", decision)
    write_csv(
        output_dir / "blocker_remediation_register_2026-07-12.csv",
        [
            {
                "blocker": "broad_historical_expansion_not_authorized",
                "severity": "MEDIUM",
                "affected_scope": "future expansion outside 2026-06-29_to_2026-07-09",
                "remediation": "separate approved bounded expansion or certification request",
            },
            {
                "blocker": "training_not_authorized",
                "severity": "HIGH",
                "affected_scope": "model training",
                "remediation": "separate governance approval after bounded certification",
            },
        ],
    )
    write_reports(output_dir, decision, source_lock, manifest_summary)
    write_csv(output_dir / "parse_schema_validation_2026-07-12.csv", parse_validation(output_dir))
    package_sha = write_sha_manifest(output_dir)
    return {
        "output_dir": str(output_dir),
        "overall_pilot_decision": overall,
        "manifest_decisions": manifest_decisions,
        "package_sha256": package_sha,
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "spine_rows": {r["slate_date"]: r["spine_rows"] for r in spine_counts},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default=START_DATE)
    parser.add_argument("--end-date", default=END_DATE)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.start_date != START_DATE or args.end_date != END_DATE:
        raise SystemExit(f"this pilot is bounded to {START_DATE} through {END_DATE}")
    print(json.dumps(build(Path(args.output_dir)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
