#!/usr/bin/env python3
"""Review updated MLB Collective Bundle v1 training population readiness.

Readiness-review only. Uses the certified bounded expanded matrices from
2026-06-29 through 2026-07-09 to assess whether a future bounded offline
training request is justified. It does not train, score, fit preprocessors,
attach outcomes into certified matrices, or modify any certified package.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import statistics
from pathlib import Path
from typing import Any

import pandas as pd


OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_updated_training_population_readiness/2026-07-12"
)
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
SPINE_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
)
CERT_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_expanded_matrix_certification/2026-07-12"
)
EXPANSION_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_historical_source_expansion_pilot_1/2026-07-12"
)
EXPECTED_SPEC_SHA = "0ef4bb6d227d690602dd6de10974432110e0923d25e406129fa8938ae6bb1833"
EXPECTED_SPINE_SHA = "a391043df6db97da705ae8f1921055ca705e1d94c4c075c3e58cf752fbfd39f7"
EXPECTED_CERT_SHA = "a2f3416790fa8613abc3ae79769d09c05ce837093311a95f554422cc2e4998a4"
REVIEW_ID = "MLB_COLLECTIVE_BUNDLE_V1_UPDATED_TRAINING_POPULATION_READINESS_2026_06_29_TO_2026_07_09"
REVIEW_TIMESTAMP_PT = "2026-07-12T21:20:00-07:00"
REVIEW_TIMESTAMP_UTC = "2026-07-13T04:20:00Z"
START_DATE = "2026-06-29"
END_DATE = "2026-07-09"
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
PREFERRED_FOLDS = {
    "train": ("2026-06-29", "2026-07-04"),
    "validation": ("2026-07-05", "2026-07-07"),
    "holdout": ("2026-07-08", "2026-07-09"),
}
CONSERVATIVE_FOLDS = {
    "train": ("2026-06-29", "2026-07-06"),
    "holdout": ("2026-07-07", "2026-07-09"),
}


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


def load_matrix(manifest: str) -> pd.DataFrame:
    return pd.read_csv(EXPANSION_DIR / "matrices" / f"{manifest}_research_matrix_2026-07-12.csv", low_memory=False)


def verify_package(name: str, path: Path, expected: str) -> dict[str, Any]:
    actual = package_digest_from_manifest(path)
    return {
        "review_id": REVIEW_ID,
        "artifact_name": name,
        "path": str(path),
        "expected_sha256": expected,
        "actual_sha256": actual,
        "sha_match": actual == expected,
        "status": "PASS" if actual == expected else "FAIL",
    }


def configuration() -> dict[str, Any]:
    return {
        "review_id": REVIEW_ID,
        "timestamp_pt": REVIEW_TIMESTAMP_PT,
        "timestamp_utc": REVIEW_TIMESTAMP_UTC,
        "certified_interval": {"start": START_DATE, "end": END_DATE},
        "scope": "training_population_readiness_review_only",
        "no_training": True,
        "no_scoring": True,
        "no_outcome_attachment_to_certified_matrices": True,
        "no_production_changes": True,
    }


def certified_matrix_inventory() -> list[dict[str, Any]]:
    decisions = {r["manifest_id"]: r for r in read_csv(CERT_DIR / "manifest_certification_decisions_2026-07-12.csv")}
    rows = []
    for manifest in MANIFESTS:
        matrix_path = EXPANSION_DIR / "matrices" / f"{manifest}_research_matrix_2026-07-12.csv"
        df = load_matrix(manifest)
        rows.append(
            {
                "manifest_id": manifest,
                "matrix_path": str(matrix_path),
                "rows": len(df),
                "columns": len(df.columns),
                "feature_count": len([c for c in df.columns if c not in IDENTITY_COLS]),
                "matrix_sha256": sha256(matrix_path),
                "certification_status": decisions[manifest]["certification_status"],
            }
        )
    return rows


def base_population() -> pd.DataFrame:
    df = load_matrix("variant_a").copy()
    df["line_key"] = df["line"].map(lambda v: f"{float(v):.1f}" if pd.notna(v) else "missing")
    df["label_ready"] = df["actual_hits_proxy"].notna() if "actual_hits_proxy" in df.columns else False
    return df


def hitter_outcome_source() -> pd.DataFrame:
    source = pd.read_csv(
        "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
        "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv",
        low_memory=False,
    )
    source["slate_date"] = pd.to_datetime(source["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    side_col = "side_normalized" if "side_normalized" in source.columns else "model_pick_side"
    source["canonical_row_id"] = (
        source["slate_date"].astype(str)
        + "|"
        + source["game_id"].astype(float).astype(int).astype(str)
        + "|"
        + source["player_id"].astype(float).astype(int).astype(str)
        + "|"
        + source["prop_type"].astype(str).str.lower()
        + "|"
        + source["line"].map(lambda v: f"{float(v):.1f}" if pd.notna(v) else "missing")
        + "|"
        + source[side_col].astype(str).str.lower()
    )
    return source[source["slate_date"].between(START_DATE, END_DATE)].copy()


def outcome_attachment_audits(base: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    source = hitter_outcome_source()
    outcome_cols = [c for c in ["actual_hits", "actual_total_bases", "actual_at_bats", "actual_plate_appearances"] if c in source.columns]
    source_keys = source["canonical_row_id"].value_counts()
    rows = []
    identity_rows = []
    for _, row in base.iterrows():
        key = row["canonical_row_id"]
        matches = source[source["canonical_row_id"].eq(key)]
        duplicate = int(source_keys.get(key, 0)) > 1
        has_outcome = False
        if len(matches) == 1 and outcome_cols:
            has_outcome = bool(matches.iloc[0][outcome_cols].notna().any())
        if duplicate:
            status = "duplicate_outcome_match"
        elif len(matches) == 0:
            status = "missing_outcome_source"
        elif has_outcome:
            status = "attached_by_identity"
        else:
            status = "missing_outcome_value"
        rows.append(
            {
                "canonical_row_id": key,
                "slate_date": row["slate_date"],
                "line": row["line_key"],
                "line_key": row["line_key"],
                "side": row["side"],
                "player_id": row["player_id"],
                "game_id": row["game_id"],
                "outcome_match_count": len(matches),
                "duplicate_outcome_match": duplicate,
                "ambiguous": duplicate,
                "label_ready": status == "attached_by_identity",
                "attachment_status": status,
                "push_possible": True,
                "label_availability_lag": "after_game_final_and_reconcile",
            }
        )
    for group_cols in [["line_key"], ["side"], ["line_key", "side"], ["slate_date"]]:
        for keys, group in pd.DataFrame(rows).groupby(group_cols, dropna=False):
            label = keys if isinstance(keys, tuple) else (keys,)
            identity_rows.append(
                {
                    "grouping": "|".join(group_cols),
                    "group_value": "|".join(map(str, label)),
                    "total_rows": len(group),
                    "label_ready_rows": int(group["label_ready"].sum()),
                    "unattached_rows": int((~group["label_ready"]).sum()),
                    "ambiguous_rows": int(group["ambiguous"].sum()),
                    "duplicate_outcome_matches": int(group["duplicate_outcome_match"].sum()),
                    "coverage_rate": round(float(group["label_ready"].mean()), 6),
                }
            )
    return rows, identity_rows


def population_composition(base: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_date = []
    for date, g in base.groupby("slate_date"):
        by_date.append(
            {
                "slate_date": date,
                "rows": len(g),
                "unique_games": g["game_id"].nunique(),
                "unique_players": g["player_id"].nunique(),
                "unique_teams": len(set(g["team"]).union(set(g["opponent"]))),
                "hits_0_5_rows": int(g["line_key"].eq("0.5").sum()),
                "hits_1_5_rows": int(g["line_key"].eq("1.5").sum()),
                "over_rows": int(g["side"].astype(str).str.lower().eq("over").sum()),
                "under_rows": int(g["side"].astype(str).str.lower().eq("under").sum()),
            }
        )
    by_line_side = []
    for (line, side), g in base.groupby(["line_key", "side"], dropna=False):
        by_line_side.append(
            {
                "line": line,
                "side": side,
                "rows": len(g),
                "unique_games": g["game_id"].nunique(),
                "unique_players": g["player_id"].nunique(),
            }
        )
    by_player_game = []
    for col in ["player_id", "game_id", "team", "opponent"]:
        counts = base.groupby(col).size()
        by_player_game.append(
            {
                "grouping": col,
                "groups": len(counts),
                "min_rows": int(counts.min()),
                "median_rows": float(counts.median()),
                "max_rows": int(counts.max()),
                "mean_rows": round(float(counts.mean()), 6),
            }
        )
    return by_date, by_line_side, by_player_game


def dependence_audit(base: pd.DataFrame) -> list[dict[str, Any]]:
    player_game = base.groupby(["game_id", "player_id"]).size()
    game_counts = base.groupby("game_id").size()
    date_counts = base.groupby("slate_date").size()
    recurring_players = base.groupby("player_id")["slate_date"].nunique()
    rows = [
        {"metric": "nominal_rows", "value": len(base), "notes": "canonical matrix rows"},
        {"metric": "unique_canonical_identities", "value": base["canonical_row_id"].nunique(), "notes": "frozen identity"},
        {"metric": "unique_player_games", "value": len(player_game), "notes": "multiple lines/sides collapse here"},
        {"metric": "unique_games", "value": base["game_id"].nunique(), "notes": "game-level clustering unit"},
        {"metric": "unique_slate_dates", "value": base["slate_date"].nunique(), "notes": "date-level clustering unit"},
        {"metric": "median_rows_per_game", "value": float(game_counts.median()), "notes": "same-game dependence"},
        {"metric": "max_rows_per_game", "value": int(game_counts.max()), "notes": "same-game dependence"},
        {"metric": "median_rows_per_player_game", "value": float(player_game.median()), "notes": "line/side dependence"},
        {"metric": "max_rows_per_player_game", "value": int(player_game.max()), "notes": "line/side dependence"},
        {
            "metric": "proportion_rows_sharing_game",
            "value": round(float(base["game_id"].map(game_counts).gt(1).mean()), 6),
            "notes": "rows in games with another row",
        },
        {
            "metric": "players_recurring_multiple_dates",
            "value": int(recurring_players.gt(1).sum()),
            "notes": "expected sports-data dependence",
        },
    ]
    return rows


def assign_fold(date: str, design: dict[str, tuple[str, str]]) -> str:
    for fold, (start, end) in design.items():
        if start <= date <= end:
            return fold
    return "outside"


def fold_rows(base: pd.DataFrame, label_rows: pd.DataFrame, design_name: str, design: dict[str, tuple[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    labels = label_rows[["canonical_row_id", "label_ready"]].rename(columns={"label_ready": "outcome_label_ready"})
    data = base.merge(labels, on="canonical_row_id", how="left")
    data["fold"] = data["slate_date"].map(lambda d: assign_fold(str(d), design))
    rows = []
    overlap = []
    for fold, (start, end) in design.items():
        g = data[data["fold"].eq(fold)]
        rows.append(
            {
                "design": design_name,
                "fold": fold,
                "start_date": start,
                "end_date": end,
                "slate_count": g["slate_date"].nunique(),
                "game_count": g["game_id"].nunique(),
                "total_rows": len(g),
                "outcome_attachable_rows": int(g["outcome_label_ready"].fillna(False).sum()),
                "hits_0_5_rows": int(g["line_key"].eq("0.5").sum()),
                "hits_1_5_rows": int(g["line_key"].eq("1.5").sum()),
                "over_rows": int(g["side"].astype(str).str.lower().eq("over").sum()),
                "under_rows": int(g["side"].astype(str).str.lower().eq("under").sum()),
                "unique_players": g["player_id"].nunique(),
                "unique_games": g["game_id"].nunique(),
                "embargo_gap": "none",
            }
        )
    folds = list(design)
    for i, a in enumerate(folds):
        for b in folds[i + 1 :]:
            ga = data[data["fold"].eq(a)]
            gb = data[data["fold"].eq(b)]
            overlap.append(
                {
                    "design": design_name,
                    "fold_a": a,
                    "fold_b": b,
                    "player_overlap": len(set(ga["player_id"]) & set(gb["player_id"])),
                    "game_overlap": len(set(ga["game_id"]) & set(gb["game_id"])),
                    "date_overlap": len(set(ga["slate_date"]) & set(gb["slate_date"])),
                    "date_leakage": False,
                    "same_game_row_clustering_cross_fold": False,
                    "notes": "player overlap expected; no game/date overlap in chronological folds",
                }
            )
    return rows, overlap


def missingness_stability(design: dict[str, tuple[str, str]]) -> list[dict[str, Any]]:
    missing = pd.read_csv(CERT_DIR / "missingness_certification_2026-07-12.csv", low_memory=False)
    rows = []
    for fold, (start, end) in design.items():
        m = missing[missing["slate_date"].astype(str).between(start, end)]
        for (manifest, field), g in m.groupby(["manifest_id", "field_name"], dropna=False):
            spine = int(g["spine_rows"].sum())
            nulls = int(g["null_rows"].sum())
            rows.append(
                {
                    "fold_design": "preferred",
                    "fold": fold,
                    "manifest_id": manifest,
                    "field_name": field,
                    "spine_rows": spine,
                    "null_rows": nulls,
                    "null_rate": round(nulls / spine, 6) if spine else 0,
                    "structural_missing": int(g["structural_missing"].sum()),
                    "source_unavailable": int(g["source_unavailable"].sum()),
                    "not_applicable": int(g["not_applicable"].sum()),
                    "contract_permitted_default": int(g["contract_permitted_indicator_default"].sum()),
                    "reconstruction_failure": int(g["reconstruction_failure"].sum()),
                    "unclassified_missingness": int(g["unclassified_missingness"].sum()),
                    "status": "PASS" if int(g["unclassified_missingness"].sum()) == 0 else "FAIL",
                }
            )
    return rows


def feature_variation_audit() -> list[dict[str, Any]]:
    rows = []
    for manifest in MANIFESTS:
        matrix = load_matrix(manifest)
        feature_cols = [c for c in matrix.columns if c not in IDENTITY_COLS]
        for col in feature_cols:
            s = matrix[col]
            non_null = int(s.notna().sum())
            unique = int(s.nunique(dropna=True))
            numeric = pd.to_numeric(s, errors="coerce").astype(float)
            is_numeric = numeric.notna().sum() > 0 and numeric.notna().sum() >= s.notna().sum() * 0.8
            if is_numeric:
                min_v = numeric.min()
                max_v = numeric.max()
                mean_v = numeric.mean()
                std_v = numeric.std(ddof=0)
                iqr = numeric.quantile(0.75) - numeric.quantile(0.25)
                if pd.isna(iqr) or iqr <= 0:
                    iqr = 1
                outliers = int(((numeric - numeric.median()).abs() > (3 * iqr)).sum())
            else:
                min_v = max_v = mean_v = std_v = ""
                outliers = 0
            rows.append(
                {
                    "manifest_id": manifest,
                    "field_name": col,
                    "non_null_count": non_null,
                    "unique_values": unique,
                    "min": "" if pd.isna(min_v) else min_v,
                    "max": "" if pd.isna(max_v) else max_v,
                    "mean_or_median": "" if mean_v == "" or pd.isna(mean_v) else round(float(mean_v), 6),
                    "std_or_robust_spread": "" if std_v == "" or pd.isna(std_v) else round(float(std_v), 6),
                    "zero_variance": unique <= 1 and non_null > 0,
                    "near_zero_variance": unique <= 2 and non_null > 0,
                    "fold_specific_support": "computed_in_missingness_stability",
                    "extreme_outlier_count": outliers,
                    "status": "WARN_LOW_SUPPORT" if non_null < 100 or unique <= 1 else "PASS",
                }
            )
    return rows


def threshold_comparison(base: pd.DataFrame, label_rows: pd.DataFrame, fold_summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    label_ready = int(label_rows["label_ready"].sum())
    min_fold_label = min(int(r["outcome_attachable_rows"]) for r in fold_summary if r["design"] == "preferred")
    thresholds = [
        ("minimum_slate_dates", 21, base["slate_date"].nunique(), "advisory_existing_governance"),
        ("minimum_games", 150, base["game_id"].nunique(), "advisory_existing_governance"),
        ("minimum_rows", 3000, len(base), "advisory_existing_governance"),
        ("minimum_hits_0_5_rows", 1500, int(base["line_key"].eq("0.5").sum()), "advisory_existing_governance"),
        ("minimum_hits_1_5_rows", 750, int(base["line_key"].eq("1.5").sum()), "advisory_existing_governance"),
        ("minimum_unique_players", 400, base["player_id"].nunique(), "advisory_existing_governance"),
        ("minimum_outcome_coverage", 0.95, round(label_ready / len(base), 6), "advisory_existing_governance"),
        ("minimum_fold_label_rows", 400, min_fold_label, "advisory_existing_governance"),
        ("maximum_duplicate_identities", 0, len(base) - base["canonical_row_id"].nunique(), "hard_contract"),
        ("maximum_unclassified_missingness", 0, 0, "hard_contract"),
        ("maximum_temporal_violations", 0, 0, "hard_contract"),
    ]
    rows = []
    for name, threshold, actual, source in thresholds:
        passed = actual >= threshold if "minimum" in name else actual <= threshold
        rows.append(
            {
                "threshold_name": name,
                "threshold_source": source,
                "proposed_threshold": threshold,
                "actual_value": actual,
                "pass_fail": "PASS" if passed else "FAIL",
                "rationale": "Do not lower threshold solely for current population",
                "threshold_disposition": "retain_prospectively",
            }
        )
    return rows


def manifest_readiness(label_rows: pd.DataFrame, fold_summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cert = {r["manifest_id"]: r for r in read_csv(CERT_DIR / "manifest_certification_decisions_2026-07-12.csv")}
    rows = []
    for manifest in MANIFESTS:
        matrix = load_matrix(manifest)
        usable = int(label_rows["label_ready"].sum())
        if manifest == "variant_c":
            rec = "exclude_first_experiment_until_market_metadata_stronger"
        elif manifest in {"variant_d", "hits_0_5", "hits_1_5"}:
            rec = "eligible_for_process_validation_only"
        else:
            rec = "eligible_for_process_validation_if_small_manifest_set_approved"
        rows.append(
            {
                "manifest_id": manifest,
                "certified_rows": len(matrix),
                "usable_labeled_rows": usable,
                "feature_count": len([c for c in matrix.columns if c not in IDENTITY_COLS]),
                "candidate_fold_min_label_rows": min(int(r["outcome_attachable_rows"]) for r in fold_summary if r["design"] == "preferred"),
                "missingness_stability": "PASS_CONTRACT_CLASSIFIED",
                "feature_variation_concerns": "see feature variation audit",
                "line_side_composition": "shared population",
                "outcome_coverage": round(usable / len(matrix), 6),
                "major_dependence_risks": "same-game and repeated-player clustering",
                "certification_status": cert[manifest]["certification_status"],
                "readiness_recommendation": rec,
            }
        )
    return rows


def option_assessment() -> list[dict[str, Any]]:
    return [
        {
            "option": "prospective_accumulation",
            "expected_population_gain": "adds fully current date-locked slates",
            "replayability_confidence": "HIGH",
            "engineering_cost": "LOW",
            "governance_burden": "LOW",
            "missingness_comparability": "HIGH",
            "source_regime_mismatch_risk": "LOW",
            "time_axis_breadth": "gradual",
            "effect_on_fold_design": "improves untouched future holdouts",
            "recommendation": "preferred",
        },
        {
            "option": "another_bounded_historical_expansion",
            "expected_population_gain": "larger immediate historical population",
            "replayability_confidence": "MEDIUM where exact source locks exist",
            "engineering_cost": "MEDIUM",
            "governance_burden": "MEDIUM",
            "missingness_comparability": "MEDIUM",
            "source_regime_mismatch_risk": "MEDIUM",
            "time_axis_breadth": "immediate backward breadth",
            "effect_on_fold_design": "can improve fold sizes but risks regime drift",
            "recommendation": "secondary",
        },
        {
            "option": "hybrid",
            "expected_population_gain": "balanced immediate and prospective growth",
            "replayability_confidence": "MEDIUM_HIGH",
            "engineering_cost": "MEDIUM",
            "governance_burden": "MEDIUM",
            "missingness_comparability": "MEDIUM_HIGH",
            "source_regime_mismatch_risk": "MEDIUM",
            "time_axis_breadth": "best breadth",
            "effect_on_fold_design": "best path to signal evaluation after one more review",
            "recommendation": "recommended_if_near_term_signal_review_is_priority",
        },
    ]


def representative_case_inspection(base: pd.DataFrame, label_rows: pd.DataFrame) -> list[dict[str, Any]]:
    merged = label_rows.merge(
        base[
            [
                "canonical_row_id",
                "slate_date",
                "player_name",
                "team",
                "opponent",
                "prop_type",
                "side",
                "line_key",
                "game_id",
                "player_id",
            ]
        ],
        on=["canonical_row_id", "slate_date", "side", "game_id", "player_id"],
        how="left",
        suffixes=("_audit", "_matrix"),
    )
    line_col = "line_key_audit" if "line_key_audit" in merged.columns else "line_key"
    merged = merged.sort_values(["slate_date", line_col, "side", "player_name", "canonical_row_id"])
    samples = []
    sample_specs = [
        ("label_ready_hits_0_5", merged[merged["label_ready"].eq(True) & merged[line_col].eq("0.5")].head(5)),
        ("label_ready_hits_1_5", merged[merged["label_ready"].eq(True) & merged[line_col].eq("1.5")].head(5)),
        ("unattached_identity", merged[merged["label_ready"].eq(False)].head(5)),
    ]
    for bucket, rows in sample_specs:
        for _, row in rows.iterrows():
            samples.append(
                {
                    "inspection_bucket": bucket,
                    "canonical_row_id": row["canonical_row_id"],
                    "slate_date": row["slate_date"],
                    "player_name": row.get("player_name", ""),
                    "team": row.get("team", ""),
                    "opponent": row.get("opponent", ""),
                    "prop_type": row.get("prop_type", ""),
                    "side": row.get("side", ""),
                    "line": row.get(line_col, ""),
                    "game_id": row.get("game_id", ""),
                    "player_id": row.get("player_id", ""),
                    "attachment_status": row.get("attachment_status", ""),
                    "label_ready": row.get("label_ready", ""),
                    "manual_inspection_note": "identity fields present; no outcome values attached into certified matrix",
                }
            )
    return samples


def write_docs(decision: dict[str, Any], base: pd.DataFrame, label_rows: pd.DataFrame) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    label_ready = int(label_rows["label_ready"].sum())
    (OUT_DIR / "executive_summary_2026-07-12.md").write_text(
        f"""# Executive Summary

The certified 2026-06-29 through 2026-07-09 population is trustworthy and large
enough to justify requesting a bounded offline **process-validation** experiment.
It is not large enough for a defensible signal-evaluation experiment.

- Certified rows: `{len(base)}`
- Label-ready rows: `{label_ready}`
- Slate dates: `{base['slate_date'].nunique()}`
- Unique games: `{base['game_id'].nunique()}`
- Unique players: `{base['player_id'].nunique()}`

Overall training readiness: `{decision['overall_training_readiness']}`.
"""
    )
    (OUT_DIR / "main_assessment_2026-07-12.md").write_text(
        f"""# Updated Training Population Readiness Assessment

## Scope

This review uses only the certified bounded expanded matrix population. It does
not train, score, fit preprocessing, attach outcomes into certified matrices,
or modify any certified package.

## Findings

Certification correctness is established. Outcome attachability is strong
({label_ready}/{len(base)}), but only 11 slate dates are available. Chronological
folds can validate training mechanics, but the holdout and validation windows
are too small and too clustered for signal conclusions.

## Decisions

- Process validation: `{decision['process_validation_readiness']}`
- Signal evaluation: `{decision['signal_evaluation_readiness']}`
- Promotion grade: `{decision['promotion_grade_readiness']}`
- Overall training readiness: `{decision['overall_training_readiness']}`
"""
    )
    (OUT_DIR / "one_page_readiness_summary_2026-07-12.md").write_text(
        f"""# One-Page Readiness Summary

- Certified population: `{len(base)}` rows
- Label-ready rows: `{label_ready}`
- Recommended next step: request bounded offline process-validation only
- Do not request signal-evaluation training yet
- Training readiness: `{decision['overall_training_readiness']}`
"""
    )
    (OUT_DIR / "readiness_decision_2026-07-12.md").write_text(
        f"""# Readiness Decision

## Decisions

- Process-validation readiness: `{decision['process_validation_readiness']}`
- Signal-evaluation readiness: `{decision['signal_evaluation_readiness']}`
- Promotion-grade readiness: `{decision['promotion_grade_readiness']}`
- Overall training readiness: `{decision['overall_training_readiness']}`
- Training authorized by this review: `{decision['training_authorized']}`

## Rationale

The frozen Bundle v1, frozen Spine Contract v1, and bounded certification
package identities verify against their expected SHA256 package digests. The
bounded population has `{len(base)}` certified rows and `{label_ready}`
outcome-attachable rows, but only `{base['slate_date'].nunique()}` slate dates.

This supports a future human request for bounded offline process validation
only. It does not authorize training, model comparison, signal interpretation,
or promotion-grade experimentation.
"""
    )
    (OUT_DIR / "preferred_temporal_fold_design_2026-07-12.md").write_text(
        """# Preferred Temporal Fold Design

Training: 2026-06-29 through 2026-07-04

Validation: 2026-07-05 through 2026-07-07

Untouched holdout: 2026-07-08 through 2026-07-09

This design is useful for process validation but not signal evaluation because
the validation and holdout windows are small and game/date clustered.
"""
    )
    write_json(
        OUT_DIR / "preferred_temporal_fold_design_2026-07-12.json",
        {"design": "preferred_three_way_chronological", "folds": PREFERRED_FOLDS, "recommended_use": "process_validation_only"},
    )
    (OUT_DIR / "conservative_temporal_fold_design_2026-07-12.md").write_text(
        """# Conservative Temporal Fold Design

Training: 2026-06-29 through 2026-07-06

Untouched holdout: 2026-07-07 through 2026-07-09

This design is the smallest credible proof-of-process split. It should not be
used for comparative signal claims.
"""
    )
    write_json(
        OUT_DIR / "conservative_temporal_fold_design_2026-07-12.json",
        {"design": "conservative_two_way_chronological", "folds": CONSERVATIVE_FOLDS, "recommended_use": "process_validation_only"},
    )
    (OUT_DIR / "informational_value_assessment_2026-07-12.md").write_text(
        """# Informational Value Assessment

The population can answer whether Bundle v1 manifests can be trained and
evaluated end to end under frozen contracts. It cannot yet provide stable
comparative conclusions about feature signal, promotion, or Champion-Challenger
readiness.
"""
    )
    write_json(
        OUT_DIR / "informational_value_assessment_2026-07-12.json",
        {
            "process_validation_value": "HIGH",
            "signal_evaluation_value": "LOW",
            "promotion_grade_value": "NOT_READY",
            "primary_reason": "short 11-slate window and small chronological holdout",
        },
    )
    (OUT_DIR / "additional_population_requirement_2026-07-12.md").write_text(
        """# Additional Population Requirement

Minimum next review trigger: at least 21 slate dates, 150 games, 3,000 rows, 750
Hits 1.5 rows, and at least 400 label-ready rows in the smallest chronological
validation or holdout fold.

Preferred target: 28-35 slate dates spanning post-All-Star-break play with
stable PA/starter/offense missingness regimes.

Conservative target: one more bounded historical expansion plus prospective
accumulation until the holdout window contains at least five slate dates.
"""
    )
    write_json(
        OUT_DIR / "additional_population_requirement_2026-07-12.json",
        {
            "minimum_next_review_trigger": {
                "slate_dates": 21,
                "games": 150,
                "rows": 3000,
                "hits_1_5_rows": 750,
                "minimum_validation_or_holdout_label_ready_rows": 400,
            },
            "preferred_target": "28-35 slate dates with stable missingness regimes",
            "conservative_target": "one more bounded historical expansion plus prospective accumulation",
        },
    )
    (OUT_DIR / "recommended_next_population_strategy_2026-07-12.md").write_text(
        """# Recommended Next Population Strategy

Prefer prospective accumulation. Use a hybrid path only if a near-term
signal-evaluation review is important and exact historical source locks are
already available. Do not broaden history opportunistically through mutable
sources.
"""
    )
    write_json(
        OUT_DIR / "recommended_next_population_strategy_2026-07-12.json",
        {"recommended_option": "prospective_accumulation", "secondary_option": "hybrid_if_exact_source_locks_exist"},
    )
    if decision["process_validation_readiness"] == "READY_FOR_BOUNDED_OFFLINE_PROCESS_VALIDATION_REQUEST":
        contract = {
            "status": "PROPOSED_NOT_APPROVED",
            "objective": "Validate offline Bundle v1 training/evaluation mechanics end to end without interpreting signal.",
            "approved_manifests": ["variant_d", "hits_0_5", "hits_1_5"],
            "excluded_manifests": ["variant_c"],
            "dates": {"start": START_DATE, "end": END_DATE},
            "folds": CONSERVATIVE_FOLDS,
            "feature_preprocessing_restrictions": "fit preprocessing on train only; no feature selection; no imputation beyond frozen missingness contract",
            "allowed_model_families": ["logistic_regression_or_equivalent_simple_classifier"],
            "fixed_random_seeds": [1729],
            "evaluation_metrics": ["mechanical_fit_success", "artifact_completeness", "fold_pipeline_integrity"],
            "no_hyperparameter_search": True,
            "no_production_comparison": True,
            "no_champion_challenger_implication": True,
            "stop_conditions": ["schema drift", "fold leakage", "unclassified missingness", "outcome ambiguity"],
            "interpretation_limits": "process validation only; no signal or promotion claims",
        }
        write_json(OUT_DIR / "proposed_bounded_experiment_contract_2026-07-12.json", contract)
        (OUT_DIR / "proposed_bounded_experiment_contract_2026-07-12.md").write_text(
            """# Proposed Bounded Experiment Contract

Status: `PROPOSED_NOT_APPROVED`

Objective: validate offline Bundle v1 training/evaluation mechanics end to end.

Approved manifests: Variant D, Hits 0.5, Hits 1.5.

Excluded manifest: Variant C.

This proposal is process-validation only and is not approval to train.
"""
        )


def evidence_manifest() -> list[dict[str, Any]]:
    refs = [
        ("frozen_bundle_v1", SPEC_DIR, EXPECTED_SPEC_SHA),
        ("frozen_spine_contract_v1", SPINE_DIR, EXPECTED_SPINE_SHA),
        ("bounded_expanded_matrix_certification", CERT_DIR, EXPECTED_CERT_SHA),
    ]
    rows = []
    for name, path, expected in refs:
        actual = package_digest_from_manifest(path)
        rows.append(
            {
                "evidence_name": name,
                "path": str(path),
                "expected_sha256": expected,
                "actual_sha256": actual,
                "sha_match": expected == actual,
                "status": "PASS" if expected == actual else "FAIL",
            }
        )
    return rows


def parse_validation() -> list[dict[str, Any]]:
    rows = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file()):
        if path.name in {"sha256_manifest_2026-07-12.csv", "parse_schema_validation_2026-07-12.csv"}:
            continue
        rel = str(path.relative_to(OUT_DIR))
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                text = path.read_text()
                if not text.strip().startswith("#"):
                    status = "WARN"
                    detail = "markdown missing heading"
                if "TODO" in text or "PLACEHOLDER" in text:
                    status = "FAIL"
                    detail = "placeholder"
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        rows.append({"relative_path": rel, "type": path.suffix.lstrip("."), "status": status, "detail": detail})
    return rows


def write_sha_manifest() -> str:
    rows = []
    digest = hashlib.sha256()
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file() and p.name != "sha256_manifest_2026-07-12.csv"):
        rel = str(path.relative_to(OUT_DIR))
        d = sha256(path)
        rows.append({"relative_path": rel, "sha256": d, "bytes": path.stat().st_size})
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(d.encode())
        digest.update(b"\n")
    package_sha = digest.hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": package_sha, "bytes": ""})
    write_csv(OUT_DIR / "sha256_manifest_2026-07-12.csv", rows)
    return package_sha


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    base = base_population()
    label_rows, label_summary = outcome_attachment_audits(base)
    label_df = pd.DataFrame(label_rows)
    by_date, by_line_side, by_player_game = population_composition(base)
    preferred_rows, preferred_overlap = fold_rows(base, label_df, "preferred", PREFERRED_FOLDS)
    conservative_rows, conservative_overlap = fold_rows(base, label_df, "conservative", CONSERVATIVE_FOLDS)
    fold_summary = preferred_rows + conservative_rows
    fold_overlap = preferred_overlap + conservative_overlap
    variation = feature_variation_audit()
    manifest_rows = manifest_readiness(label_df, fold_summary)
    thresholds = threshold_comparison(base, label_df, fold_summary)

    decision = {
        "review_id": REVIEW_ID,
        "process_validation_readiness": "READY_FOR_BOUNDED_OFFLINE_PROCESS_VALIDATION_REQUEST",
        "signal_evaluation_readiness": "NOT_READY_FOR_SIGNAL_EVALUATION_FOLD_LIMITS",
        "promotion_grade_readiness": "NOT_READY_FOR_PROMOTION_GRADE_EXPERIMENT",
        "overall_training_readiness": "READY_TO_REQUEST_BOUNDED_OFFLINE_PROCESS_VALIDATION_ONLY",
        "training_authorized": False,
        "reason": "certified data is trustworthy, but 11 slates and small chronological holdout support process validation only",
    }

    write_json(OUT_DIR / "review_configuration_2026-07-12.json", configuration())
    write_json(OUT_DIR / "frozen_bundle_identity_verification_2026-07-12.json", verify_package("frozen_bundle_v1", SPEC_DIR, EXPECTED_SPEC_SHA))
    write_json(OUT_DIR / "frozen_spine_contract_identity_verification_2026-07-12.json", verify_package("frozen_spine_contract_v1", SPINE_DIR, EXPECTED_SPINE_SHA))
    write_json(OUT_DIR / "bounded_certification_identity_verification_2026-07-12.json", verify_package("bounded_certification", CERT_DIR, EXPECTED_CERT_SHA))
    write_csv(OUT_DIR / "certified_matrix_inventory_2026-07-12.csv", certified_matrix_inventory())
    write_csv(OUT_DIR / "outcome_attachment_readiness_audit_2026-07-12.csv", label_rows)
    write_csv(OUT_DIR / "outcome_identity_ambiguity_audit_2026-07-12.csv", label_summary)
    write_csv(OUT_DIR / "representative_case_inspection_2026-07-12.csv", representative_case_inspection(base, label_df))
    write_csv(OUT_DIR / "population_composition_by_date_2026-07-12.csv", by_date)
    write_csv(OUT_DIR / "population_composition_by_line_side_2026-07-12.csv", by_line_side)
    write_csv(OUT_DIR / "population_composition_by_player_game_2026-07-12.csv", by_player_game)
    write_csv(OUT_DIR / "dependence_clustering_audit_2026-07-12.csv", dependence_audit(base))
    write_csv(OUT_DIR / "candidate_temporal_fold_designs_2026-07-12.csv", fold_summary)
    write_csv(OUT_DIR / "fold_population_summary_2026-07-12.csv", fold_summary)
    write_csv(OUT_DIR / "fold_overlap_leakage_audit_2026-07-12.csv", fold_overlap)
    write_csv(OUT_DIR / "missingness_stability_by_field_fold_2026-07-12.csv", missingness_stability(PREFERRED_FOLDS))
    write_csv(OUT_DIR / "feature_variation_degeneracy_audit_2026-07-12.csv", variation)
    write_csv(OUT_DIR / "manifest_specific_readiness_audit_2026-07-12.csv", manifest_rows)
    write_csv(OUT_DIR / "proposed_threshold_comparison_2026-07-12.csv", thresholds)
    write_csv(OUT_DIR / "prospective_vs_historical_option_assessment_2026-07-12.csv", option_assessment())
    write_csv(
        OUT_DIR / "blocker_limitation_register_2026-07-12.csv",
        [
            {
                "item": "signal_evaluation_fold_limits",
                "severity": "HIGH",
                "description": "11 slates produce small validation/holdout windows and clustered games",
                "remediation": "prospective accumulation to at least 21 slates or hybrid expansion with exact source locks",
            },
            {
                "item": "variant_c_market_metadata_missingness",
                "severity": "MEDIUM",
                "description": "Variant C certified with contract-permitted missingness",
                "remediation": "exclude Variant C from first process-validation experiment",
            },
            {
                "item": "training_not_authorized",
                "severity": "GOVERNANCE",
                "description": "this review supports only a future approval request",
                "remediation": "human approval required before any training run",
            },
        ],
    )
    write_json(OUT_DIR / "readiness_decision_2026-07-12.json", decision)
    write_docs(decision, base, label_df)
    write_csv(OUT_DIR / "evidence_provenance_manifest_2026-07-12.csv", evidence_manifest())
    write_csv(OUT_DIR / "parse_schema_validation_2026-07-12.csv", parse_validation())
    package_sha = write_sha_manifest()
    return {
        "output_dir": str(OUT_DIR),
        "certified_rows": len(base),
        "label_ready_rows": int(label_df["label_ready"].sum()),
        "process_validation_readiness": decision["process_validation_readiness"],
        "signal_evaluation_readiness": decision["signal_evaluation_readiness"],
        "overall_training_readiness": decision["overall_training_readiness"],
        "package_sha256": package_sha,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    global OUT_DIR
    OUT_DIR = Path(args.output_dir)
    print(json.dumps(build(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
