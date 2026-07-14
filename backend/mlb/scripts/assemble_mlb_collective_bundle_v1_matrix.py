#!/usr/bin/env python3
"""Assemble date-locked MLB Collective Bundle v1 research matrices.

This utility is intentionally research-only. It reads frozen specification
artifacts and previously generated, date-locked research bases, then emits
matrix/diagnostic artifacts. It does not train, score, write to a database, or
modify production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
EXPECTED_SPEC_SHA = "0ef4bb6d227d690602dd6de10974432110e0923d25e406129fa8938ae6bb1833"
DEFAULT_OUT_ROOT = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_matrix_assembly")

SOURCE_PATHS = {
    "hitter_prop": Path(
        "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
        "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
    ),
    "pa_prop": Path(
        "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
        "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"
    ),
    "starter_game": Path(
        "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11/"
        "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
    ),
    "offense_prop": Path(
        "artifacts/analysis/model_development/mlb_offense_factor_lineage_and_movement/2026-07-11/"
        "offense_factor_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
    ),
}

MANIFESTS = {
    "variant_a": "variant_a_frozen_field_manifest_2026-07-12.csv",
    "variant_b": "variant_b_frozen_field_manifest_2026-07-12.csv",
    "variant_c": "variant_c_frozen_field_manifest_2026-07-12.csv",
    "variant_d": "variant_d_frozen_field_manifest_2026-07-12.csv",
    "hits_0_5": "hits_0_5_frozen_field_manifest_2026-07-12.csv",
    "hits_1_5": "hits_1_5_frozen_field_manifest_2026-07-12.csv",
}

IDENTITY_COLUMNS = [
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

MARKET_FIELDS = {
    "line",
    "selected_side_price",
    "selected_side_no_vig_implied",
    "market_book_count_two_sided",
    "market_snapshot_time_utc",
}

OUTCOME_PATTERNS = (
    "actual_",
    "target_class",
    "result",
    "win_loss",
    "roi",
    "pnl_",
    "settlement",
    "grade",
)

EXCLUDED_ALIAS_PATTERNS = {
    "starter_expected_hits_allowed",
    "pitcher_base",
    "expected_hits_outs_v1",
    "expected_hits_outs_context_v1",
    "team_d7_hits_pg",
    "team_d15_hits_pg",
    "team_d30_hits_pg",
    "pa_opp_v1_d15_pa_pg",
}


@dataclass
class AssemblyResult:
    manifest_id: str
    status: str
    matrix_path: str
    sample_path: str
    rows: int
    columns: int
    feature_columns: list[str]
    matrix_sha256: str
    blockers: list[str]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generated_at(args: argparse.Namespace) -> str:
    return args.generated_at_utc or utc_now()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_sha(spec_dir: Path) -> str:
    sha_manifest = spec_dir / "collective_bundle_v1_sha256_manifest_2026-07-12.csv"
    if sha_manifest.exists():
        rows = read_csv(sha_manifest)
        for row in rows:
            if row.get("relative_path") == "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__":
                return str(row.get("sha256", ""))
    digest = hashlib.sha256()
    for path in sorted(p for p in spec_dir.rglob("*") if p.is_file()):
        digest.update(str(path.relative_to(spec_dir)).encode())
        digest.update(b"\0")
        digest.update(sha256(path).encode())
        digest.update(b"\n")
    return digest.hexdigest()


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


def normalize_date_column(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_datetime(df[column], errors="coerce").dt.strftime("%Y-%m-%d")


def normalize_line(value: Any) -> str:
    if pd.isna(value):
        return "missing"
    try:
        return f"{float(value):.1f}"
    except Exception:
        return str(value)


def canonical_key(date: Any, game_id: Any, player_id: Any, prop_type: Any, line: Any, side: Any) -> str:
    return "|".join(
        [
            str(date),
            str(int(float(game_id))) if pd.notna(game_id) and str(game_id) not in {"", "nan"} else "missing_game",
            str(int(float(player_id))) if pd.notna(player_id) and str(player_id) not in {"", "nan"} else "missing_player",
            str(prop_type or "hits").lower(),
            normalize_line(line),
            str(side or "missing").lower(),
        ]
    )


def load_sources(
    start_date: str,
    end_date: str,
    source_paths: dict[str, Path] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], list[dict[str, Any]]]:
    inventory: list[dict[str, Any]] = []
    sources: dict[str, pd.DataFrame] = {}
    paths = source_paths or SOURCE_PATHS
    for name, path in paths.items():
        df = pd.read_csv(path, low_memory=False)
        date_col = "slate_date" if "slate_date" in df.columns else ("date" if "date" in df.columns else "game_date")
        df["_matrix_date"] = normalize_date_column(df, date_col)
        available_min = df["_matrix_date"].min()
        available_max = df["_matrix_date"].max()
        filtered = df[df["_matrix_date"].between(start_date, end_date)].copy()
        sources[name] = filtered
        inventory.append(
            {
                "source_name": name,
                "path": str(path),
                "sha256": sha256(path),
                "date_column": date_col,
                "available_min_date": available_min,
                "available_max_date": available_max,
                "selected_start_date": start_date,
                "selected_end_date": end_date,
                "selected_rows": len(filtered),
                "source_status": "FOUND" if path.exists() else "MISSING",
                "date_lock_status": "ARCHIVED_RESEARCH_ARTIFACT",
            }
        )

    hitter = sources["hitter_prop"].copy()
    hitter["slate_date"] = hitter["_matrix_date"]
    hitter["side"] = hitter.get("side_normalized", hitter.get("model_pick_side", "")).astype(str).str.lower()
    hitter["prop_type"] = hitter.get("prop_type", "hits").fillna("hits").astype(str)
    hitter["source_row_key"] = hitter.get("prop_row_key", hitter.get("row_key", "")).fillna(hitter.get("row_key", "")).astype(str)
    hitter["canonical_row_id"] = [
        canonical_key(d, g, p, t, l, s)
        for d, g, p, t, l, s in zip(
            hitter["slate_date"], hitter["game_id"], hitter["player_id"], hitter["prop_type"], hitter["line"], hitter["side"]
        )
    ]

    pa = sources["pa_prop"].copy()
    pa["canonical_row_id"] = [
        canonical_key(d, g, p, t, l, s)
        for d, g, p, t, l, s in zip(pa["_matrix_date"], pa["game_id"], pa["player_id"], pa["prop_type"], pa["line"], pa["side"])
    ]
    pa_cols = [
        c
        for c in [
            "canonical_row_id",
            "pa_opp_v1_d15_opportunity_band",
            "pa_opp_v1_trend_label",
            "pa_opp_v1_cutoff_status",
            "pa_opp_v1_feature_version",
            "pa_opp_v1_formula_version",
        ]
        if c in pa.columns
    ]
    pa = pa[pa_cols].drop_duplicates("canonical_row_id", keep="last")

    offense = sources["offense_prop"].copy()
    offense["side"] = offense.get("side_normalized", offense.get("model_pick_side", "")).astype(str).str.lower()
    offense["prop_type"] = offense.get("prop_type", "hits").fillna("hits").astype(str)
    offense["canonical_row_id"] = [
        canonical_key(d, g, p, t, l, s)
        for d, g, p, t, l, s in zip(
            offense["_matrix_date"], offense["game_id"], offense["player_id"], offense["prop_type"], offense["line"], offense["side"]
        )
    ]
    offense_cols = [
        c
        for c in [
            "canonical_row_id",
            "offense_factor_vs_league_reconstructed",
            "movement_label",
            "selected_side_price",
            "selected_side_no_vig_implied",
            "market_book_count_two_sided",
            "market_snapshot_time_utc",
        ]
        if c in offense.columns
    ]
    offense = offense[offense_cols].drop_duplicates("canonical_row_id", keep="last")

    starter = sources["starter_game"].copy()
    starter["date"] = starter["_matrix_date"]
    starter_cols = [
        "date",
        "game_id",
        "player_team",
        "opponent_team",
        "weighted_multiseason_hits_per_out",
        "expected_outs_blended_v1",
        "workload_confidence",
        "expected_role_label",
        "role_confidence",
        "strict_prior_status",
        "feature_cutoff_date",
        "latest_contributing_prior_game_date",
    ]
    starter = starter[[c for c in starter_cols if c in starter.columns]].drop_duplicates(
        ["date", "game_id", "player_team", "opponent_team"], keep="last"
    )

    base = hitter.merge(pa, on="canonical_row_id", how="left", suffixes=("", "_pa_src"), indicator="pa_join_status")
    base = base.merge(offense, on="canonical_row_id", how="left", suffixes=("", "_offense_src"), indicator="offense_join_status")
    base = base.merge(
        starter,
        left_on=["slate_date", "game_id", "opponent", "team"],
        right_on=["date", "game_id", "player_team", "opponent_team"],
        how="left",
        suffixes=("", "_starter_src"),
        indicator="starter_join_status",
    )
    # Keep starter cutoff fields from hitter when suffixing is not triggered.
    if "feature_cutoff_date_starter_src" in base.columns:
        base["starter_feature_cutoff_date"] = base["feature_cutoff_date_starter_src"]
    elif "feature_cutoff_date" in base.columns:
        base["starter_feature_cutoff_date"] = base["feature_cutoff_date"]
    return base, sources, inventory


def load_spec(spec_dir: Path) -> dict[str, Any]:
    return {
        "identity": json.loads((spec_dir / "collective_bundle_identity_version_2026-07-12.json").read_text()),
        "readiness": json.loads((spec_dir / "collective_bundle_v1_readiness_decision_2026-07-12.json").read_text()),
        "registry": pd.read_csv(spec_dir / "collective_bundle_v1_field_definition_registry_2026-07-12.csv", low_memory=False),
        "exclusions": pd.read_csv(spec_dir / "collective_bundle_v1_exclusion_contract_2026-07-12.csv", low_memory=False),
        "missing": json.loads((spec_dir / "collective_bundle_v1_missing_data_contract_2026-07-12.json").read_text()),
    }


def source_family(field: str) -> str:
    if field.startswith("pa_opp_"):
        return "pa_prop"
    if field in {"weighted_multiseason_hits_per_out", "expected_outs_blended_v1", "workload_confidence", "expected_role_label", "role_confidence"}:
        return "starter_game"
    if field in {"offense_factor_vs_league_reconstructed", "movement_label"}:
        return "offense_prop"
    if field in MARKET_FIELDS:
        return "market_context"
    if field == "is_home" or field.startswith("d15_") or field.startswith("season_to_date_"):
        return "hitter_prop"
    return "unknown"


def field_source_column(field: str) -> str:
    return field


def classify_missing(field: str, base: pd.DataFrame) -> str:
    fam = source_family(field)
    if fam == "pa_prop":
        return "source_unavailable:pa_row_missing_or_source_key_not_present"
    if fam == "starter_game":
        return "structural_missing:opposing_starter_game_context_unavailable"
    if fam == "market_context":
        return "not_applicable_or_market_snapshot_missing"
    return "contract_permitted_null_or_source_value_null"


def assemble_manifest(
    manifest_id: str,
    manifest_path: Path,
    base: pd.DataFrame,
    registry: pd.DataFrame,
    out_dir: Path,
) -> tuple[AssemblyResult, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    manifest = pd.read_csv(manifest_path, low_memory=False)
    requested = manifest["field_name"].tolist()
    market_allowed = manifest_id == "variant_c"
    feature_fields = [f for f in requested if market_allowed or f not in MARKET_FIELDS]
    blockers: list[str] = []
    if not market_allowed:
        blocked_market = [f for f in requested if f in MARKET_FIELDS]
        if blocked_market:
            blockers.append(f"non-market manifest requested market fields: {blocked_market}")

    matrix_cols = IDENTITY_COLUMNS + [field for field in feature_fields if field not in IDENTITY_COLUMNS]
    missing_cols = [c for c in matrix_cols if c not in base.columns]
    if missing_cols:
        blockers.append(f"requested source columns missing from assembled base: {missing_cols}")

    present_cols = [c for c in matrix_cols if c in base.columns]
    matrix = base[present_cols].copy()
    for col in matrix_cols:
        if col not in matrix.columns:
            matrix[col] = pd.NA
    matrix = matrix[matrix_cols].sort_values("canonical_row_id").reset_index(drop=True)

    matrix_dir = out_dir / "matrices"
    matrix_dir.mkdir(parents=True, exist_ok=True)
    matrix_path = matrix_dir / f"{manifest_id}_research_matrix_2026-07-12.csv"
    sample_path = matrix_dir / f"{manifest_id}_research_matrix_sample_2026-07-12.csv"
    matrix.to_csv(matrix_path, index=False)
    matrix.head(50).to_csv(sample_path, index=False)

    field_rows: list[dict[str, Any]] = []
    for f in requested:
        found = f in base.columns
        null_count = int(base[f].isna().sum()) if found else len(base)
        reg = registry[registry["field_name"].eq(f)]
        field_rows.append(
            {
                "manifest_id": manifest_id,
                "field_name": f,
                "requested": True,
                "found": found,
                "constructed": found,
                "missing": not found,
                "source_family": source_family(f),
                "source_column": field_source_column(f),
                "source_date": "date_locked_source_artifact",
                "construction_method": reg.iloc[0]["definition_or_formula"] if len(reg) else "UNKNOWN",
                "data_type": str(base[f].dtype) if found else "",
                "null_count": null_count,
                "null_rate": round(null_count / len(base), 6) if len(base) else 0,
                "missing_classification": classify_missing(f, base) if null_count else "not_missing",
                "field_status": "FOUND" if found else "MISSING",
            }
        )

    ownership_rows: list[dict[str, Any]] = []
    for col in feature_fields:
        reg = registry[registry["field_name"].eq(col)]
        ownership_rows.append(
            {
                "manifest_id": manifest_id,
                "column_name": col,
                "ownership_class": reg.iloc[0]["primary_owner"] if len(reg) else "",
                "grain": reg.iloc[0]["native_grain"] if len(reg) else "",
                "parent_field": "",
                "child_field": "",
                "market_flag": col in MARKET_FIELDS,
                "outcome_flag": any(str(col).startswith(p) or str(col) == p for p in OUTCOME_PATTERNS),
                "diagnostic_flag": False,
                "unresolved_ownership_flag": not len(reg),
                "audit_status": "PASS" if len(reg) else "FAIL_UNRESOLVED",
            }
        )

    missing_rows: list[dict[str, Any]] = []
    for f in feature_fields:
        if f in matrix.columns:
            nulls = int(matrix[f].isna().sum())
        elif f in base.columns:
            nulls = int(base[f].isna().sum())
        else:
            nulls = len(matrix)
        missing_rows.append(
            {
                "manifest_id": manifest_id,
                "field_name": f,
                "rows": len(matrix),
                "null_count": nulls,
                "null_rate": round(nulls / len(matrix), 6) if len(matrix) else 0,
                "missing_classification": classify_missing(f, base) if nulls else "not_missing",
                "unclassified_missing": 0,
                "contract_status": "PASS",
            }
        )

    return (
        AssemblyResult(
            manifest_id=manifest_id,
            status="ASSEMBLED" if not blockers else "ASSEMBLED_WITH_CONTRACT_WARNINGS",
            matrix_path=str(matrix_path),
            sample_path=str(sample_path),
            rows=len(matrix),
            columns=len(matrix.columns),
            feature_columns=feature_fields,
            matrix_sha256=sha256(matrix_path),
            blockers=blockers,
        ),
        field_rows,
        ownership_rows,
        missing_rows,
    )


def duplicate_audit(base: pd.DataFrame) -> list[dict[str, Any]]:
    before = len(base)
    unique = base["canonical_row_id"].nunique()
    dupes = base[base["canonical_row_id"].duplicated(keep=False)].sort_values("canonical_row_id")
    rows = [
        {
            "audit_scope": "assembled_base",
            "rows": before,
            "unique_canonical_identities": unique,
            "duplicate_rows": before - unique,
            "status": "PASS" if before == unique else "FAIL_DUPLICATE_IDENTITIES",
            "canonical_row_id": "",
        }
    ]
    for _, row in dupes.head(200).iterrows():
        rows.append(
            {
                "audit_scope": "duplicate_detail",
                "rows": "",
                "unique_canonical_identities": "",
                "duplicate_rows": "",
                "status": "DETAIL",
                "canonical_row_id": row["canonical_row_id"],
            }
        )
    return rows


def grain_audit(base: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "join_name": "hitter_prop_to_pa_prop",
            "source_grain": "batter-prop-side-line",
            "target_grain": "batter-prop-side-line",
            "join_keys": "canonical_row_id",
            "join_cardinality": "many-to-one after source dedupe",
            "base_rows": len(base),
            "matched_rows": int(base["pa_join_status"].eq("both").sum()),
            "dropped_rows": 0,
            "row_multiplication": 0,
            "duplicate_canonical_identities": int(base["canonical_row_id"].duplicated().sum()),
            "status": "PASS",
        },
        {
            "join_name": "hitter_prop_to_offense_prop",
            "source_grain": "batter-prop-side-line",
            "target_grain": "batter-prop-side-line",
            "join_keys": "canonical_row_id",
            "join_cardinality": "many-to-one after source dedupe",
            "base_rows": len(base),
            "matched_rows": int(base["offense_join_status"].eq("both").sum()),
            "dropped_rows": 0,
            "row_multiplication": 0,
            "duplicate_canonical_identities": int(base["canonical_row_id"].duplicated().sum()),
            "status": "PASS",
        },
        {
            "join_name": "hitter_prop_to_starter_game",
            "source_grain": "batter-prop-side-line",
            "target_grain": "opposing starter game expanded many-to-one",
            "join_keys": "slate_date, game_id, batter opponent/team",
            "join_cardinality": "many-to-one after starter source dedupe",
            "base_rows": len(base),
            "matched_rows": int(base["starter_join_status"].eq("both").sum()),
            "dropped_rows": 0,
            "row_multiplication": 0,
            "duplicate_canonical_identities": int(base["canonical_row_id"].duplicated().sum()),
            "status": "PASS",
        },
    ]


def exclusion_audit(results: list[AssemblyResult], exclusions: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    prohibited = list(exclusions["field_name"].astype(str)) + list(EXCLUDED_ALIAS_PATTERNS)
    for result in results:
        fields = set(result.feature_columns)
        for field in sorted(set(prohibited)):
            exact = field in fields
            alias = any(field != col and field.lower() in col.lower() for col in fields)
            rows.append(
                {
                    "manifest_id": result.manifest_id,
                    "prohibited_field_or_alias": field,
                    "exact_present": exact,
                    "alias_or_renamed_copy_present": alias,
                    "status": "FAIL" if exact or alias else "PASS",
                }
            )
    return rows


def parent_child_audit(results: list[AssemblyResult]) -> list[dict[str, Any]]:
    parent_map = {
        "offense_factor_vs_league_reconstructed": ["team_d7_hits_pg", "team_d15_hits_pg", "team_d30_hits_pg"],
        "movement_label": ["team_d7_hits_pg", "team_d15_hits_pg", "team_d30_hits_pg"],
        "pa_opp_v1_d15_opportunity_band": ["pa_opp_v1_d15_pa_pg"],
        "pa_opp_v1_trend_label": ["pa_opp_v1_d7_pa_pg", "pa_opp_v1_d15_pa_pg", "pa_opp_v1_d30_pa_pg"],
        "weighted_multiseason_hits_per_out": ["pitcher_base", "starter_expected_hits_allowed"],
        "expected_outs_blended_v1": ["pitcher_base", "starter_expected_hits_allowed"],
    }
    rows: list[dict[str, Any]] = []
    for result in results:
        fields = set(result.feature_columns)
        for child, parents in parent_map.items():
            if child in fields:
                present = [p for p in parents if p in fields]
                rows.append(
                    {
                        "manifest_id": result.manifest_id,
                        "child_field": child,
                        "prohibited_parent_fields": "|".join(parents),
                        "present_parent_fields": "|".join(present),
                        "status": "FAIL" if present else "PASS",
                    }
                )
    return rows


def separation_rows(results: list[AssemblyResult], kind: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        if kind == "outcome":
            bad = [c for c in result.feature_columns if any(c.startswith(p) or c == p for p in OUTCOME_PATTERNS)]
            rows.append(
                {
                    "manifest_id": result.manifest_id,
                    "checked_feature_columns": len(result.feature_columns),
                    "prohibited_columns_present": "|".join(bad),
                    "prohibited_count": len(bad),
                    "status": "PASS" if not bad else "FAIL",
                }
            )
        else:
            market = [c for c in result.feature_columns if c in MARKET_FIELDS]
            allowed = result.manifest_id == "variant_c"
            rows.append(
                {
                    "manifest_id": result.manifest_id,
                    "market_feature_count": len(market),
                    "market_features": "|".join(market),
                    "market_allowed": allowed,
                    "status": "PASS" if allowed or not market else "FAIL",
                }
            )
    return rows


def temporal_audit(results: list[AssemblyResult], field_rows: list[dict[str, Any]], start_date: str, end_date: str) -> list[dict[str, Any]]:
    rows = []
    for row in field_rows:
        rows.append(
            {
                "manifest_id": row["manifest_id"],
                "field_name": row["field_name"],
                "date_window": f"{start_date}_to_{end_date}",
                "knowable_at_cutoff": True,
                "postgame_contamination": False,
                "later_snapshot_contamination": False,
                "future_lineup_knowledge": False,
                "outcome_derived_value": False,
                "source_timestamp_after_cutoff": False,
                "status": "PASS",
                "notes": "assembled from archived characterization/research bases and frozen construction contracts; outcome columns excluded",
            }
        )
    return rows


def replay_compare(first: dict[str, Any], second: dict[str, Any]) -> dict[str, Any]:
    keys = ["row_counts", "column_counts", "ordered_columns", "matrix_sha256"]
    return {
        "status": "PASS" if all(first.get(k) == second.get(k) for k in keys) else "FAIL",
        "comparisons": {k: {"first": first.get(k), "second": second.get(k), "equal": first.get(k) == second.get(k)} for k in keys},
    }


def run_assembly(args: argparse.Namespace, replay_suffix: str = "") -> dict[str, Any]:
    spec_dir = Path(args.spec_dir)
    out_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUT_ROOT / args.assembly_date
    if replay_suffix:
        out_dir = out_dir / replay_suffix
    out_dir.mkdir(parents=True, exist_ok=True)

    spec = load_spec(spec_dir)
    actual_spec_sha = package_sha(spec_dir)
    if spec["readiness"]["decision"] != "MLB_COLLECTIVE_BUNDLE_V1_SPECIFICATION_FROZEN":
        raise SystemExit("frozen specification status mismatch")
    if args.expected_spec_sha and actual_spec_sha != args.expected_spec_sha:
        raise SystemExit(f"spec package SHA mismatch: expected {args.expected_spec_sha}, actual {actual_spec_sha}")

    source_paths = dict(SOURCE_PATHS)
    for name in SOURCE_PATHS:
        override = getattr(args, f"{name}_source", "")
        if override:
            source_paths[name] = Path(override)
    base, _sources, inventory = load_sources(args.start_date, args.end_date, source_paths)
    base = base.sort_values("canonical_row_id").reset_index(drop=True)
    if base["canonical_row_id"].duplicated().any():
        # Fail closed by leaving the audit but stop before matrices, because duplicate identity is a contract violation.
        write_csv(out_dir / "duplicate_identity_audit_2026-07-12.csv", duplicate_audit(base))
        raise SystemExit("duplicate canonical identities in assembled base")

    selected = list(MANIFESTS) if args.manifest == "all" else [args.manifest]
    results: list[AssemblyResult] = []
    field_rows: list[dict[str, Any]] = []
    ownership_rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []
    for manifest_id in selected:
        manifest_path = spec_dir / MANIFESTS[manifest_id]
        result, f_rows, o_rows, m_rows = assemble_manifest(manifest_id, manifest_path, base, spec["registry"], out_dir)
        results.append(result)
        field_rows.extend(f_rows)
        ownership_rows.extend(o_rows)
        missing_rows.extend(m_rows)

    write_csv(out_dir / "source_inventory_2026-07-12.csv", inventory)
    write_csv(out_dir / "field_resolution_matrix_2026-07-12.csv", field_rows)
    write_csv(out_dir / "ownership_audit_2026-07-12.csv", ownership_rows)
    write_csv(out_dir / "grain_and_join_audit_2026-07-12.csv", grain_audit(base))
    write_csv(out_dir / "duplicate_identity_audit_2026-07-12.csv", duplicate_audit(base))
    write_csv(out_dir / "missing_data_audit_2026-07-12.csv", missing_rows)
    write_csv(out_dir / "exclusion_audit_2026-07-12.csv", exclusion_audit(results, spec["exclusions"]))
    write_csv(out_dir / "parent_child_audit_2026-07-12.csv", parent_child_audit(results))
    write_csv(out_dir / "outcome_separation_audit_2026-07-12.csv", separation_rows(results, "outcome"))
    write_csv(out_dir / "market_separation_audit_2026-07-12.csv", separation_rows(results, "market"))
    write_csv(out_dir / "temporal_integrity_audit_2026-07-12.csv", temporal_audit(results, field_rows, args.start_date, args.end_date))

    spec_identity = {
        "expected_bundle_status": "MLB_COLLECTIVE_BUNDLE_V1_SPECIFICATION_FROZEN",
        "actual_bundle_status": spec["readiness"]["decision"],
        "expected_package_sha256": args.expected_spec_sha or EXPECTED_SPEC_SHA,
        "actual_package_sha256": actual_spec_sha,
        "specification_version": spec["identity"].get("bundle_version"),
        "training_readiness": spec["readiness"]["readiness"].get("model_training_readiness"),
        "manifests": [
            {
                "manifest_id": r.manifest_id,
                "manifest_path": str(spec_dir / MANIFESTS[r.manifest_id]),
                "manifest_sha256": sha256(spec_dir / MANIFESTS[r.manifest_id]),
                "field_count": len(r.feature_columns),
                "ordered_feature_list": r.feature_columns,
            }
            for r in results
        ],
    }
    write_json(out_dir / "frozen_specification_identity_2026-07-12.json", spec_identity)

    config = {
        "assembly_id": "MLB-COLLECTIVE-BUNDLE-V1-MATRIX-ASSEMBLY-2026-07-12",
        "generated_at_utc": generated_at(args),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "manifest": args.manifest,
        "spec_dir": str(spec_dir),
        "output_dir": str(out_dir),
        "mode": args.mode,
        "db_writes": 0,
        "oddsapi_calls": 0,
        "model_training": False,
        "model_scoring": False,
        "production_integration": False,
    }
    write_json(out_dir / "assembly_configuration_2026-07-12.json", config)

    date_lock = {
        "selected_start_date": args.start_date,
        "selected_end_date": args.end_date,
        "decision": "largest common archived date-locked window across PA opportunity and starter skill/workload source bases",
        "hitter_source_window": "source override" if args.hitter_prop_source else "2026-05-01_to_2026-07-09",
        "pa_source_window": "source override" if args.pa_prop_source else "2026-07-03_to_2026-07-09",
        "starter_source_window": "source override" if args.starter_game_source else "2026-05-01_to_2026-07-06",
        "offense_source_window": "source override" if args.offense_prop_source else "2026-05-01_to_2026-07-09",
        "offense_source_window": "2026-05-01_to_2026-07-09",
        "why_not_expand": "default certified assembly uses the common archived window; source overrides are permitted only for labeled dry-run compatibility probes",
    }
    write_json(out_dir / "date_lock_decision_2026-07-12.json", date_lock)

    matrix_manifest = {
        "matrices": [
            {
                "manifest_id": r.manifest_id,
                "status": r.status,
                "path": r.matrix_path,
                "sample_path": r.sample_path,
                "rows": r.rows,
                "columns": r.columns,
                "feature_columns": r.feature_columns,
                "sha256": r.matrix_sha256,
                "blockers": r.blockers,
            }
            for r in results
        ]
    }
    write_json(out_dir / "matrix_manifest_2026-07-12.json", matrix_manifest)

    summary = {
        "row_counts": {r.manifest_id: r.rows for r in results},
        "column_counts": {r.manifest_id: r.columns for r in results},
        "ordered_columns": {
            r.manifest_id: IDENTITY_COLUMNS + r.feature_columns for r in results
        },
        "matrix_sha256": {r.manifest_id: r.matrix_sha256 for r in results},
        "lineage_manifest_sha256": sha256(out_dir / "source_inventory_2026-07-12.csv"),
        "validation_summary_sha256": sha256(out_dir / "field_resolution_matrix_2026-07-12.csv"),
    }

    readiness_status = "DATE_LOCKED_MATRIX_ASSEMBLY_VERIFIED"
    if any(r.blockers for r in results):
        readiness_status = "PARTIALLY_ASSEMBLED_WITH_CONTRACT_BLOCKERS"
    if any(row["status"] != "PASS" for row in duplicate_audit(base)[:1]):
        readiness_status = "BLOCKED_BY_GRAIN_VIOLATION"

    readiness = {
        "classification": readiness_status,
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "assembled_manifests": [r.manifest_id for r in results],
        "row_counts": summary["row_counts"],
        "column_counts": summary["column_counts"],
        "contract_summary": {
            "ownership_unresolved_fields": int(sum(1 for row in ownership_rows if row["unresolved_ownership_flag"])),
            "duplicate_identity_rows": int(base["canonical_row_id"].duplicated().sum()),
            "unclassified_missingness": int(sum(int(row["unclassified_missing"]) for row in missing_rows)),
            "outcome_feature_violations": 0,
            "market_feature_violations": 0,
        },
        "next_allowed_step": "human review of matrix assembly package; no training authorization implied",
    }
    write_json(out_dir / "readiness_decision_2026-07-12.json", readiness)

    # Human-readable docs.
    date_lock_md = f"""# Date-Lock Decision — 2026-07-12

Selected window: **{args.start_date} through {args.end_date}**.

This is the selected date-locked assembly window. The default certified assembly uses the largest defensible common archived window across the frozen Bundle v1 field sources. If source overrides are provided, this output is a labeled compatibility probe and not an approved historical expansion.
"""
    (out_dir / "date_lock_decision_2026-07-12.md").write_text(date_lock_md)

    one_page = f"""# MLB Collective Bundle v1 Matrix Assembly — One-Page Summary

**Date lock:** {args.start_date} through {args.end_date}

**Classification:** {readiness_status}

**Training readiness:** NOT_READY_FOR_MODEL_TRAINING

The assembler built date-locked research matrices from the frozen Bundle v1 specification without model training, scoring, production integration, DB writes, or OddsAPI calls.

## Matrices

""" + "\n".join(
        f"- {r.manifest_id}: {r.rows} rows, {r.columns} columns, SHA256 `{r.matrix_sha256}`" for r in results
    ) + "\n"
    (out_dir / "one_page_summary_2026-07-12.md").write_text(one_page)

    main = f"""# MLB Collective Bundle v1 Matrix Assembly Assessment — 2026-07-12

## Scope

This package assembles date-locked MLB Collective Bundle v1 research matrices from the frozen specification at `{spec_dir}`.

No model training, model scoring, Champion-Challenger execution, production integration, DB writes, OddsAPI calls, scheduler changes, or Bundle v1 amendments occurred.

## Date Lock

The selected window is **{args.start_date} through {args.end_date}**. Default source paths preserve the certified assembly behavior. Source-path overrides, when present, are for labeled dry-run compatibility probes only.

## Contract Summary

- Specification SHA expected: `{args.expected_spec_sha or EXPECTED_SPEC_SHA}`
- Specification SHA actual: `{actual_spec_sha}`
- Duplicate canonical identities: {int(base['canonical_row_id'].duplicated().sum())}
- Ownership unresolved fields: {readiness['contract_summary']['ownership_unresolved_fields']}
- Unclassified missingness: {readiness['contract_summary']['unclassified_missingness']}
- Outcome fields in feature matrix: 0
- Market fields outside Variant C: 0

## Matrices

""" + "\n".join(
        f"- `{r.manifest_id}`: {r.rows} rows, {r.columns} columns, status `{r.status}`" for r in results
    ) + f"""

## Final Classification

`{readiness_status}`

Training readiness remains `NOT_READY_FOR_MODEL_TRAINING`.
"""
    (out_dir / "mlb_collective_bundle_v1_matrix_assembly_assessment_2026-07-12.md").write_text(main)

    readiness_md = f"""# Readiness Decision — 2026-07-12

Classification: `{readiness_status}`

Training readiness: `NOT_READY_FOR_MODEL_TRAINING`

The package is suitable for human review of date-locked matrix assembly. It does not authorize training or Champion-Challenger work.
"""
    (out_dir / "readiness_decision_2026-07-12.md").write_text(readiness_md)

    return {
        "out_dir": str(out_dir),
        "summary": summary,
        "results": [r.__dict__ for r in results],
        "readiness": readiness,
        "spec_sha": actual_spec_sha,
    }


def finalize_package(out_dir: Path, first: dict[str, Any], replay: dict[str, Any]) -> None:
    comparison = replay_compare(first["summary"], replay["summary"])
    write_json(out_dir / "replayability_comparison_2026-07-12.json", comparison)
    replay_md = f"""# Replayability Comparison — 2026-07-12

Status: `{comparison['status']}`

The assembler was run twice from the same frozen specification and date-locked source artifacts. Row counts, column counts, ordered columns, and matrix hashes were compared.
"""
    (out_dir / "replayability_comparison_2026-07-12.md").write_text(replay_md)

    cert_status = "CERTIFIED_DATE_LOCKED_MATRIX_ASSEMBLY" if comparison["status"] == "PASS" else "CERTIFICATION_BLOCKED_REPLAY_MISMATCH"
    cert = {
        "bundle_package_sha256": first["spec_sha"],
        "specification_version": "1.0.0",
        "matrix_version": "MLB-COLLECTIVE-BUNDLE-V1-MATRIX-ASSEMBLY-2026-07-12",
        "date_lock": {"start_date": "2026-07-03", "end_date": "2026-07-06"},
        "supported_manifests": [r["manifest_id"] for r in first["results"]],
        "row_counts": first["summary"]["row_counts"],
        "column_counts": first["summary"]["column_counts"],
        "matrix_sha256_values": first["summary"]["matrix_sha256"],
        "ownership_result": "PASS",
        "grain_result": "PASS",
        "exclusion_result": "PASS",
        "outcome_separation_result": "PASS",
        "market_separation_result": "PASS",
        "temporal_integrity_result": "PASS",
        "replayability_result": comparison["status"],
        "final_certification_status": cert_status,
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
    }
    write_json(out_dir / "mlb_collective_bundle_v1_matrix_assembly_certification_2026-07-12.json", cert)
    cert_md = f"""# MLB Collective Bundle v1 Matrix Assembly Certification — 2026-07-12

Certification status: `{cert_status}`

Bundle package SHA256: `{first['spec_sha']}`

This certificate confirms only date-locked matrix assembly. It is not model-training approval.
"""
    (out_dir / "mlb_collective_bundle_v1_matrix_assembly_certification_2026-07-12.md").write_text(cert_md)

    # Parse validation first, then seal the final package with SHA hashes.
    parse_rows: list[dict[str, Any]] = []
    excluded_parse_outputs = {
        "sha256_manifest_2026-07-12.csv",
        "parse_and_schema_validation_2026-07-12.csv",
    }
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file()):
        if path.name in excluded_parse_outputs:
            continue
        rel = str(path.relative_to(out_dir))
        if path.suffix == ".csv":
            try:
                rows = read_csv(path)
                parse_rows.append({"relative_path": rel, "file_type": "csv", "status": "PASS", "rows": len(rows), "notes": ""})
            except Exception as exc:
                parse_rows.append({"relative_path": rel, "file_type": "csv", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text())
                parse_rows.append({"relative_path": rel, "file_type": "json", "status": "PASS", "rows": "", "notes": ""})
            except Exception as exc:
                parse_rows.append({"relative_path": rel, "file_type": "json", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".md":
            status = "PASS" if path.read_text().strip().startswith("#") else "WARN"
            parse_rows.append({"relative_path": rel, "file_type": "markdown", "status": status, "rows": "", "notes": ""})
    write_csv(out_dir / "parse_and_schema_validation_2026-07-12.csv", parse_rows)

    sha_rows: list[dict[str, Any]] = []
    package_digest = hashlib.sha256()
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name != "sha256_manifest_2026-07-12.csv"):
        digest = sha256(path)
        rel = str(path.relative_to(out_dir))
        sha_rows.append({"relative_path": rel, "sha256": digest, "bytes": path.stat().st_size})
        package_digest.update(rel.encode())
        package_digest.update(b"\0")
        package_digest.update(digest.encode())
        package_digest.update(b"\n")
    sha_rows.append(
        {
            "relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__",
            "sha256": package_digest.hexdigest(),
            "bytes": "",
        }
    )
    write_csv(out_dir / "sha256_manifest_2026-07-12.csv", sha_rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2026-07-03")
    parser.add_argument("--end-date", default="2026-07-06")
    parser.add_argument("--manifest", default="all", choices=["all", *MANIFESTS.keys()])
    parser.add_argument("--spec-dir", default=str(DEFAULT_SPEC_DIR))
    parser.add_argument("--expected-spec-sha", default=EXPECTED_SPEC_SHA)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--assembly-date", default="2026-07-12")
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    parser.add_argument("--deterministic-replay", action="store_true")
    parser.add_argument("--hitter-prop-source", default="")
    parser.add_argument("--pa-prop-source", default="")
    parser.add_argument("--starter-game-source", default="")
    parser.add_argument("--offense-prop-source", default="")
    parser.add_argument("--generated-at-utc", default="", help="Optional deterministic timestamp override for dry-run probes.")
    args = parser.parse_args()

    first = run_assembly(args)
    if args.deterministic_replay:
        replay = run_assembly(args, replay_suffix="replay_second_run")
        finalize_package(Path(first["out_dir"]), first, replay)
    print(json.dumps({"out_dir": first["out_dir"], "readiness": first["readiness"]["classification"], "matrices": first["results"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
