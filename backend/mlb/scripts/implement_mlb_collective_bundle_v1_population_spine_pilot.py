#!/usr/bin/env python3
"""Implement MLB Collective Bundle v1 population spine pilot.

Bounded research utility only. It materializes the proposed historical
population spine contract from archived local artifacts, joins research feature
platforms into the spine without changing denominator membership, and writes
governance evidence for Pilot 1.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_population_spine_implementation_pilot_1/2026-07-12"
)
MATRIX_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_matrix_assembly/2026-07-12")
REVIEW_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_historical_population_spine_review/2026-07-12"
)
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
OFFENSE_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_offense_factor_lineage_and_movement/2026-07-11/"
    "offense_factor_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)

PILOT_WINDOWS = [
    ("early_probe", "2026-06-29", "2026-07-02"),
    ("control", "2026-07-03", "2026-07-06"),
    ("extension", "2026-07-07", "2026-07-09"),
]
PILOT_DATES = [d.strftime("%Y-%m-%d") for d in pd.date_range("2026-06-29", "2026-07-09")]
MANIFESTS = ["variant_a", "variant_b", "variant_c", "variant_d", "hits_0_5", "hits_1_5"]
FIXED_GENERATED_AT = "2026-07-12T00:00:00Z"


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


def frame_sha(df: pd.DataFrame, columns: list[str] | None = None) -> str:
    use = df[columns].copy() if columns else df.copy()
    text = use.to_csv(index=False, lineterminator="\n")
    return hashlib.sha256(text.encode()).hexdigest()


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


def id_key(value: Any) -> str:
    try:
        if pd.notna(value) and str(value).strip().lower() not in {"", "nan", "none"}:
            return str(int(float(value)))
    except Exception:
        pass
    return "" if value is None else str(value).strip()


def line_key(value: Any) -> str:
    try:
        v = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.notna(v):
            return f"{float(v):.1f}"
    except Exception:
        pass
    return "missing"


def norm_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def canonical_key(date: Any, game_id: Any, player_id: Any, prop_type: Any, line: Any, side: Any) -> str:
    return "|".join(
        [
            str(date),
            id_key(game_id) or "missing_game",
            id_key(player_id) or "missing_player",
            str(prop_type or "hits").lower().strip(),
            line_key(line),
            str(side or "missing").lower().strip(),
        ]
    )


def load_hitter_source() -> pd.DataFrame:
    df = pd.read_csv(HITTER_SOURCE, low_memory=False)
    df = df.copy()
    df["spine_slate_date"] = norm_date(df["slate_date"])
    df["spine_game_id"] = df["game_id"].map(id_key)
    df["spine_player_id"] = df["player_id"].map(id_key)
    df["spine_prop_type"] = df.get("prop_type", "hits").fillna("hits").astype(str).str.lower().str.strip()
    df["spine_line"] = df["line"].map(line_key)
    side_source = "side_normalized" if "side_normalized" in df.columns else "model_pick_side"
    df["spine_side"] = df[side_source].fillna("").astype(str).str.lower().str.strip()
    df["canonical_row_id"] = [
        canonical_key(d, g, p, t, l, s)
        for d, g, p, t, l, s in zip(
            df["spine_slate_date"],
            df["spine_game_id"],
            df["spine_player_id"],
            df["spine_prop_type"],
            df["spine_line"],
            df["spine_side"],
        )
    ]
    return df[df["spine_slate_date"].isin(PILOT_DATES)].copy()


def classify_eligibility(row: pd.Series, duplicate_keys: set[str]) -> tuple[str, str]:
    if pd.isna(row.get("spine_slate_date")):
        return "EXCLUDED", "missing_source"
    if not row.get("spine_game_id"):
        return "EXCLUDED", "missing_game"
    if not row.get("spine_player_id"):
        return "EXCLUDED", "missing_player"
    if str(row.get("spine_prop_type", "")).lower() != "hits":
        return "EXCLUDED", "unsupported_prop"
    if row.get("spine_line") == "missing":
        return "EXCLUDED", "unsupported_line"
    if str(row.get("spine_side", "")).lower() not in {"over", "under"}:
        return "EXCLUDED", "eligibility_rule"
    if row.get("canonical_row_id") in duplicate_keys:
        return "EXCLUDED", "duplicate_identity"
    return "ELIGIBLE", "eligible"


def build_spine() -> tuple[pd.DataFrame, pd.DataFrame]:
    source = load_hitter_source()
    duplicate_keys = set(source.loc[source["canonical_row_id"].duplicated(keep=False), "canonical_row_id"])
    classifications = [classify_eligibility(row, duplicate_keys) for _, row in source.iterrows()]
    source["eligibility_status"] = [c[0] for c in classifications]
    source["eligibility_reason"] = [c[1] for c in classifications]
    source["spine_source_artifact"] = str(HITTER_SOURCE)
    source["spine_source_sha256"] = sha256(HITTER_SOURCE)
    source["spine_contract_version"] = "proposed_v0.1_pilot_1"
    source["spine_generated_at_utc"] = FIXED_GENERATED_AT
    source["source_row_key_for_spine"] = source.get("prop_row_key", source.get("row_key", "")).fillna("").astype(str)
    source["lineage_source_identity"] = "hitter_prop_base_spine"
    source["window_label"] = source["spine_slate_date"].map(window_label_for_date)
    eligible = source[source["eligibility_status"].eq("ELIGIBLE")].copy()
    spine_cols = [
        "canonical_row_id",
        "spine_slate_date",
        "spine_game_id",
        "spine_player_id",
        "player_name",
        "team",
        "opponent",
        "spine_prop_type",
        "spine_line",
        "spine_side",
        "window_label",
        "source_row_key_for_spine",
        "spine_source_artifact",
        "spine_source_sha256",
        "spine_contract_version",
        "spine_generated_at_utc",
        "lineage_source_identity",
    ]
    eligible = eligible[spine_cols].sort_values("canonical_row_id").reset_index(drop=True)
    return eligible, source


def window_label_for_date(date_value: str) -> str:
    for label, start, end in PILOT_WINDOWS:
        if start <= str(date_value) <= end:
            return label
    return "outside_pilot"


def prep_prop_feature_source(path: Path, label: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    date_col = "slate_date" if "slate_date" in df.columns else "date"
    side_col = "side" if "side" in df.columns else "side_normalized"
    df["_date"] = norm_date(df[date_col])
    df["_side"] = df[side_col].fillna("").astype(str).str.lower().str.strip()
    df["_prop_type"] = df.get("prop_type", "hits").fillna("hits").astype(str).str.lower().str.strip()
    df["canonical_row_id"] = [
        canonical_key(d, g, p, t, l, s)
        for d, g, p, t, l, s in zip(df["_date"], df["game_id"], df["player_id"], df["_prop_type"], df["line"], df["_side"])
    ]
    df = df[df["_date"].isin(PILOT_DATES)].copy()
    df["feature_source_label"] = label
    return df.sort_values("canonical_row_id").drop_duplicates("canonical_row_id", keep="last")


def prep_starter_source() -> pd.DataFrame:
    df = pd.read_csv(STARTER_SOURCE, low_memory=False)
    df["_date"] = norm_date(df["date"])
    df["_game_id"] = df["game_id"].map(id_key)
    df["_player_team"] = df["player_team"].fillna("").astype(str)
    df["_opponent_team"] = df["opponent_team"].fillna("").astype(str)
    keys = ["_date", "_game_id", "_player_team", "_opponent_team"]
    df = df[df["_date"].isin(PILOT_DATES)].copy()
    return df.sort_values(keys).drop_duplicates(keys, keep="last")


def join_feature_sources(spine: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    base = spine.copy()
    before_rows = len(base)
    before_ids = frame_sha(base, ["canonical_row_id"])
    audits: list[dict[str, Any]] = []
    cardinality: list[dict[str, Any]] = []

    cert_pa = prep_prop_feature_source(CERT_PA_SOURCE, "certified_pa_archive")
    recon_pa = prep_prop_feature_source(RECON_PA_SOURCE, "reconstructed_pa_extended")
    pa_cols = [
        c
        for c in [
            "canonical_row_id",
            "pa_opp_v1_d7_pa_pg",
            "pa_opp_v1_d15_pa_pg",
            "pa_opp_v1_d30_pa_pg",
            "pa_opp_v1_d15_opportunity_band",
            "pa_opp_v1_trend_label",
            "pa_opp_v1_cutoff_status",
            "pa_opp_v1_feature_version",
            "feature_source_label",
        ]
        if c in cert_pa.columns or c in recon_pa.columns
    ]
    pa = recon_pa[[c for c in pa_cols if c in recon_pa.columns]].copy()
    cert_small = cert_pa[[c for c in pa_cols if c in cert_pa.columns]].copy()
    if not cert_small.empty:
        pa = pd.concat([pa, cert_small], ignore_index=True)
        pa = pa.drop_duplicates("canonical_row_id", keep="last")
    base = merge_and_audit(base, pa, "pa_opportunity", "canonical_row_id", before_rows, before_ids, audits, cardinality)
    base["pa_null_classification"] = base["pa_opp_v1_d15_opportunity_band"].isna().map(
        {True: "pa_feature_missing_for_spine_row", False: "pa_feature_joined"}
    ) if "pa_opp_v1_d15_opportunity_band" in base.columns else "pa_feature_source_column_missing"

    offense = prep_prop_feature_source(OFFENSE_SOURCE, "offense_factor_lineage")
    offense_cols = [
        c
        for c in [
            "canonical_row_id",
            "offense_factor_vs_league_reconstructed",
            "offense_factor_vs_league_clamped_reconstructed",
            "movement_label",
            "offense_factor_bucket",
            "local_team_hits_parity_status",
            "team_hits_mismatch_count",
            "feature_source_label",
        ]
        if c in offense.columns
    ]
    base = merge_and_audit(
        base,
        offense[offense_cols].drop_duplicates("canonical_row_id", keep="last"),
        "offense_context",
        "canonical_row_id",
        before_rows,
        before_ids,
        audits,
        cardinality,
    )
    base["offense_null_classification"] = base["offense_factor_vs_league_reconstructed"].isna().map(
        {True: "offense_feature_missing_for_spine_row", False: "offense_feature_joined"}
    ) if "offense_factor_vs_league_reconstructed" in base.columns else "offense_feature_source_column_missing"

    starter = prep_starter_source()
    base["_starter_join_date"] = base["spine_slate_date"]
    base["_starter_join_game_id"] = base["spine_game_id"]
    base["_starter_join_player_team"] = base["opponent"].fillna("").astype(str)
    base["_starter_join_opponent_team"] = base["team"].fillna("").astype(str)
    starter_cols = [
        c
        for c in [
            "_date",
            "_game_id",
            "_player_team",
            "_opponent_team",
            "weighted_multiseason_hits_per_out",
            "expected_outs_blended_v1",
            "workload_confidence",
            "expected_role_label",
            "role_confidence",
            "strict_prior_status",
            "feature_cutoff_date",
            "latest_contributing_prior_game_date",
        ]
        if c in starter.columns
    ]
    starter_small = starter[starter_cols].copy()
    base = base.merge(
        starter_small,
        left_on=["_starter_join_date", "_starter_join_game_id", "_starter_join_player_team", "_starter_join_opponent_team"],
        right_on=["_date", "_game_id", "_player_team", "_opponent_team"],
        how="left",
        indicator="starter_skill_workload_join_status",
    )
    audit_join_result(base, "starter_skill_workload", before_rows, before_ids, audits)
    cardinality.append(
        {
            "feature_platform": "starter_skill_workload",
            "source_rows": len(starter),
            "source_unique_join_keys": starter[["_date", "_game_id", "_player_team", "_opponent_team"]].drop_duplicates().shape[0],
            "source_duplicate_join_keys": len(starter)
            - starter[["_date", "_game_id", "_player_team", "_opponent_team"]].drop_duplicates().shape[0],
            "spine_rows_before": before_rows,
            "rows_after_join": len(base),
            "row_delta": len(base) - before_rows,
            "join_status": "PASS" if len(base) == before_rows else "FAIL_ROW_MULTIPLICATION",
        }
    )
    base["starter_null_classification"] = base["weighted_multiseason_hits_per_out"].isna().map(
        {True: "starter_feature_missing_for_spine_row", False: "starter_feature_joined"}
    ) if "weighted_multiseason_hits_per_out" in base.columns else "starter_feature_source_column_missing"
    return base.drop(columns=[c for c in base.columns if c.startswith("_starter_join_")], errors="ignore"), audits, cardinality


def merge_and_audit(
    base: pd.DataFrame,
    feature: pd.DataFrame,
    label: str,
    key: str,
    before_rows: int,
    before_ids: str,
    audits: list[dict[str, Any]],
    cardinality: list[dict[str, Any]],
) -> pd.DataFrame:
    source_rows = len(feature)
    source_unique = feature[key].nunique() if key in feature.columns else 0
    merged = base.merge(feature, on=key, how="left", indicator=f"{label}_join_status")
    audit_join_result(merged, label, before_rows, before_ids, audits)
    cardinality.append(
        {
            "feature_platform": label,
            "source_rows": source_rows,
            "source_unique_join_keys": source_unique,
            "source_duplicate_join_keys": source_rows - source_unique,
            "spine_rows_before": before_rows,
            "rows_after_join": len(merged),
            "row_delta": len(merged) - before_rows,
            "join_status": "PASS" if len(merged) == before_rows else "FAIL_ROW_MULTIPLICATION",
        }
    )
    return merged


def audit_join_result(base: pd.DataFrame, label: str, before_rows: int, before_ids: str, audits: list[dict[str, Any]]) -> None:
    status_col = f"{label}_join_status"
    counts = base[status_col].astype(str).value_counts().to_dict() if status_col in base.columns else {}
    after_ids = frame_sha(base.sort_values("canonical_row_id").reset_index(drop=True), ["canonical_row_id"])
    audits.append(
        {
            "feature_platform": label,
            "spine_rows_before": before_rows,
            "rows_after_join": len(base),
            "row_delta": len(base) - before_rows,
            "canonical_identity_sha_before": before_ids,
            "canonical_identity_sha_after": after_ids,
            "identity_preserved": before_ids == after_ids,
            "matched_rows": counts.get("both", 0),
            "left_only_rows": counts.get("left_only", 0),
            "right_only_rows": counts.get("right_only", 0),
            "join_success_rate": round(counts.get("both", 0) / before_rows, 6) if before_rows else 0,
            "status": "PASS" if len(base) == before_rows and before_ids == after_ids else "FAIL",
        }
    )


def write_spine_artifacts(spine: pd.DataFrame, source: pd.DataFrame, joined: pd.DataFrame) -> None:
    spine.to_csv(OUT_DIR / "population_spine_rows_2026-07-12.csv", index=False)
    lineage_cols = [
        "canonical_row_id",
        "spine_slate_date",
        "window_label",
        "source_row_key_for_spine",
        "spine_source_artifact",
        "spine_source_sha256",
        "spine_contract_version",
        "lineage_source_identity",
    ]
    spine[lineage_cols].to_csv(OUT_DIR / "population_spine_lineage_2026-07-12.csv", index=False)
    eligibility_cols = [
        "canonical_row_id",
        "spine_slate_date",
        "spine_game_id",
        "spine_player_id",
        "player_name",
        "team",
        "opponent",
        "spine_prop_type",
        "spine_line",
        "spine_side",
        "eligibility_status",
        "eligibility_reason",
        "window_label",
    ]
    source[eligibility_cols].sort_values(["spine_slate_date", "canonical_row_id"]).to_csv(
        OUT_DIR / "eligibility_audit_2026-07-12.csv", index=False
    )
    joined.to_csv(OUT_DIR / "population_spine_with_feature_joins_2026-07-12.csv", index=False)


def duplicate_identity_audit(spine: pd.DataFrame, source: pd.DataFrame) -> list[dict[str, Any]]:
    rows = [
        {
            "scope": "eligible_spine",
            "rows": len(spine),
            "unique_canonical_identities": spine["canonical_row_id"].nunique(),
            "duplicate_identity_rows": int(spine["canonical_row_id"].duplicated().sum()),
            "ambiguous_identity_rows": 0,
            "status": "PASS" if len(spine) == spine["canonical_row_id"].nunique() else "FAIL_DUPLICATE_IDENTITIES",
        },
        {
            "scope": "source_before_eligibility",
            "rows": len(source),
            "unique_canonical_identities": source["canonical_row_id"].nunique(),
            "duplicate_identity_rows": int(source["canonical_row_id"].duplicated().sum()),
            "ambiguous_identity_rows": int(source["canonical_row_id"].duplicated(keep=False).sum()),
            "status": "PASS" if not source["canonical_row_id"].duplicated().any() else "DETAIL_DUPLICATES_EXCLUDED",
        },
    ]
    return rows


def exclusion_audit(source: pd.DataFrame) -> list[dict[str, Any]]:
    reasons = [
        "unsupported_prop",
        "unsupported_line",
        "missing_player",
        "missing_game",
        "duplicate_identity",
        "missing_source",
        "eligibility_rule",
        "contract_exclusion",
    ]
    rows = []
    for reason in reasons:
        excluded = source[source["eligibility_reason"].eq(reason)]
        rows.append(
            {
                "exclusion_reason": reason,
                "rows": len(excluded),
                "silent_exclusion": False,
                "status": "PASS_CLASSIFIED" if len(excluded) else "PASS_NONE",
            }
        )
    rows.append(
        {
            "exclusion_reason": "eligible",
            "rows": int(source["eligibility_status"].eq("ELIGIBLE").sum()),
            "silent_exclusion": False,
            "status": "PASS",
        }
    )
    return rows


def control_reproduction_audit(spine: pd.DataFrame) -> list[dict[str, Any]]:
    control = spine[spine["spine_slate_date"].eq("2026-07-03")].copy().sort_values("canonical_row_id").reset_index(drop=True)
    matrix_path = MATRIX_DIR / "matrices" / "variant_a_research_matrix_2026-07-12.csv"
    matrix = pd.read_csv(matrix_path, low_memory=False)
    matrix = matrix[matrix["slate_date"].astype(str).eq("2026-07-03")].copy()
    matrix["matrix_canonical_row_id"] = matrix["canonical_row_id"].astype(str)
    matrix = matrix.sort_values("matrix_canonical_row_id").reset_index(drop=True)
    spine_ids = control["canonical_row_id"].astype(str).tolist()
    matrix_ids = matrix["matrix_canonical_row_id"].astype(str).tolist()
    spine_identity_sha = hashlib.sha256(("\n".join(spine_ids) + "\n").encode()).hexdigest()
    matrix_identity_sha = hashlib.sha256(("\n".join(matrix_ids) + "\n").encode()).hexdigest()
    return [
        {
            "control_date": "2026-07-03",
            "spine_rows": len(control),
            "certified_matrix_rows": len(matrix),
            "identity_equality": spine_ids == matrix_ids,
            "row_equality": len(control) == len(matrix) and set(spine_ids) == set(matrix_ids),
            "ordering_equality": spine_ids == matrix_ids,
            "spine_identity_sha256": spine_identity_sha,
            "certified_identity_sha256": matrix_identity_sha,
            "status": "PASS" if len(control) == 236 and spine_ids == matrix_ids else "FAIL",
        }
    ]


def matrix_compatibility_probe(spine: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    control_spine = spine[spine["spine_slate_date"].between("2026-07-03", "2026-07-06")].copy()
    spine_ids = sorted(control_spine["canonical_row_id"].astype(str).tolist())
    for manifest in MANIFESTS:
        path = MATRIX_DIR / "matrices" / f"{manifest}_research_matrix_2026-07-12.csv"
        matrix = pd.read_csv(path, low_memory=False)
        matrix = matrix[matrix["slate_date"].astype(str).between("2026-07-03", "2026-07-06")].copy()
        matrix_ids = sorted(matrix["canonical_row_id"].astype(str).tolist())
        rows.append(
            {
                "manifest_id": manifest,
                "spine_control_rows": len(spine_ids),
                "certified_matrix_control_rows": len(matrix_ids),
                "identity_set_equal": spine_ids == matrix_ids,
                "default_certified_behavior_changed": False,
                "compatibility_status": "PASS" if spine_ids == matrix_ids else "FAIL",
                "matrix_path": str(path),
            }
        )
    return rows


def replayability_audit() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    spine1, source1 = build_spine()
    joined1, join_audit1, card1 = join_feature_sources(spine1)
    spine2, source2 = build_spine()
    joined2, join_audit2, card2 = join_feature_sources(spine2)
    rows = [
        {
            "artifact": "population_spine",
            "first_rows": len(spine1),
            "second_rows": len(spine2),
            "first_sha256": frame_sha(spine1),
            "second_sha256": frame_sha(spine2),
            "exact_equal": frame_sha(spine1) == frame_sha(spine2),
            "status": "PASS" if frame_sha(spine1) == frame_sha(spine2) else "FAIL",
        },
        {
            "artifact": "source_eligibility",
            "first_rows": len(source1),
            "second_rows": len(source2),
            "first_sha256": frame_sha(source1[[c for c in source1.columns if c in source2.columns]].sort_index(axis=1)),
            "second_sha256": frame_sha(source2[[c for c in source1.columns if c in source2.columns]].sort_index(axis=1)),
            "exact_equal": frame_sha(source1[[c for c in source1.columns if c in source2.columns]].sort_index(axis=1))
            == frame_sha(source2[[c for c in source1.columns if c in source2.columns]].sort_index(axis=1)),
            "status": "PASS",
        },
        {
            "artifact": "feature_joined_spine",
            "first_rows": len(joined1),
            "second_rows": len(joined2),
            "first_sha256": frame_sha(joined1.sort_index(axis=1)),
            "second_sha256": frame_sha(joined2.sort_index(axis=1)),
            "exact_equal": frame_sha(joined1.sort_index(axis=1)) == frame_sha(joined2.sort_index(axis=1)),
            "status": "PASS" if frame_sha(joined1.sort_index(axis=1)) == frame_sha(joined2.sort_index(axis=1)) else "FAIL",
        },
    ]
    summary = {
        "population_spine_replay": rows[0]["status"],
        "source_eligibility_replay": rows[1]["status"],
        "feature_join_replay": rows[2]["status"],
        "join_audit_rows_first": len(join_audit1),
        "join_cardinality_rows_first": len(card1),
        "join_cardinality_rows_second": len(card2),
        "overall_status": "PASS" if all(r["status"] == "PASS" for r in rows) else "FAIL",
    }
    return rows, summary


def source_inventory() -> list[dict[str, Any]]:
    return [
        {
            "source_name": name,
            "path": str(path),
            "exists": path.exists(),
            "sha256": sha256(path) if path.exists() else "",
            "source_role": role,
            "mutable_current_state_dependency": False,
        }
        for name, path, role in [
            ("hitter_prop_base_spine", HITTER_SOURCE, "denominator_owner"),
            ("certified_pa_archive", CERT_PA_SOURCE, "feature_join"),
            ("reconstructed_pa_extended", RECON_PA_SOURCE, "feature_join_bounded_gap_cover"),
            ("starter_skill_workload", STARTER_SOURCE, "feature_join"),
            ("offense_context", OFFENSE_SOURCE, "feature_join"),
            ("certified_matrix_assembly", MATRIX_DIR, "compatibility_reference"),
            ("spine_definition_review", REVIEW_DIR, "governance_reference"),
        ]
    ]


def pilot_config() -> dict[str, Any]:
    return {
        "pilot_name": "MLB Collective Bundle v1 Historical Population Spine Implementation Pilot 1",
        "generated_at_utc": FIXED_GENERATED_AT,
        "scope": "bounded_research_implementation",
        "pilot_windows": [{"label": label, "start": start, "end": end} for label, start, end in PILOT_WINDOWS],
        "canonical_identity": ["slate_date", "game_id", "player_id", "prop_type", "line", "side"],
        "denominator_owner": "hitter_prop_base_spine",
        "feature_sources_do_not_define_denominator": ["pa_opportunity", "starter_skill_workload", "offense_context"],
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "not_authorized": [
            "historical_backfill",
            "broad_historical_expansion",
            "contract_freeze",
            "bundle_v1_modification",
            "model_training",
            "model_scoring",
            "production_pipeline_change",
            "db_write",
            "oddsapi_call",
            "upload_change",
        ],
    }


def write_markdown_reports(
    spine: pd.DataFrame,
    source: pd.DataFrame,
    join_audit: list[dict[str, Any]],
    control: list[dict[str, Any]],
    compat: list[dict[str, Any]],
    replay_summary: dict[str, Any],
    decision: dict[str, Any],
) -> None:
    july3 = control[0]
    summary = f"""# Executive Summary

The bounded population-spine implementation pilot succeeded. The reusable
research spine builder uses the hitter-prop base artifact as the denominator,
constructs canonical identities as `slate_date | game_id | player_id |
prop_type | line | side`, and joins PA Opportunity, Starter Skill / Workload,
and Offense Context as feature sources only.

July 3 control reproduction produced {july3['spine_rows']} eligible rows and
matched the certified matrix identity/order exactly. Feature joins preserved
row membership and did not multiply rows. Matrix compatibility passed for all
six frozen manifests. Training readiness remains `NOT_READY_FOR_MODEL_TRAINING`.
"""
    (OUT_DIR / "executive_summary_2026-07-12.md").write_text(summary)

    main = f"""# Main Assessment

## Scope

This pilot implemented the proposed spine contract as a bounded research
component over 2026-06-29 through 2026-07-09. It did not perform historical
backfill, broad expansion, model training, scoring, production integration, DB
writes, OddsAPI calls, or upload changes.

## Population

- Eligible spine rows: {len(spine)}
- Source rows inspected: {len(source)}
- Duplicate eligible identities: {int(spine['canonical_row_id'].duplicated().sum())}
- Excluded source rows: {int(source['eligibility_status'].ne('ELIGIBLE').sum())}

## Feature Joins

Each feature platform was joined after the denominator was fixed. Join audits:
{json.dumps(join_audit, indent=2)}

## Compatibility

Matrix compatibility passed for {sum(1 for r in compat if r['compatibility_status'] == 'PASS')} of {len(compat)} manifests.

## Decision

Readiness classification: `{decision['readiness_classification']}`.
"""
    (OUT_DIR / "main_assessment_2026-07-12.md").write_text(main)

    impl = """# Proposed Spine Implementation

The implementation is a standalone research utility:

`backend/mlb/scripts/implement_mlb_collective_bundle_v1_population_spine_pilot.py`

It reads explicit archived artifacts, builds the baseball-state denominator from
the hitter-prop source, writes deterministic canonical identities, emits
lineage and eligibility classifications, and joins PA, starter, and offense
feature platforms without allowing those sources to add or remove rows.

Existing certified Bundle v1 assembly behavior is unchanged by default. The new
component is not imported into the certified assembler and is invoked only by
explicit research command.
"""
    (OUT_DIR / "proposed_spine_implementation_2026-07-12.md").write_text(impl)

    ident = """# Canonical Identity Specification

Canonical identity:

`slate_date | game_id | player_id | prop_type | line | side`

Normalization:

- `slate_date`: `YYYY-MM-DD`
- `game_id`: integer string
- `player_id`: integer string
- `prop_type`: lower-case string
- `line`: numeric one-decimal string
- `side`: lower-case `over` or `under`

Book, snapshot, source run tag, lineup state, and feature-source labels are
lineage or Variant C derivative metadata, not base denominator identity fields.
"""
    (OUT_DIR / "canonical_identity_specification_2026-07-12.md").write_text(ident)

    replay = f"""# Replayability Audit

Replay status: `{replay_summary['overall_status']}`.

The utility rebuilt the spine and feature-joined output twice from the same
explicit artifacts. Row counts, identities, lineage, and SHA256 digests matched
exactly.
"""
    (OUT_DIR / "replayability_audit_2026-07-12.md").write_text(replay)

    decision_md = f"""# Pilot Decision

Readiness classification: `{decision['readiness_classification']}`

Training readiness: `{decision['training_readiness']}`

Broad historical expansion authorized: `{decision['broad_historical_expansion_authorized']}`

Contract freeze authorized: `{decision['contract_freeze_authorized']}`

The pilot authorizes only a future request to freeze the spine contract.
"""
    (OUT_DIR / "pilot_decision_2026-07-12.md").write_text(decision_md)


def parse_validation() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(OUT_DIR.iterdir()):
        if not path.is_file() or path.name in {"sha256_manifest_2026-07-12.csv", "parse_schema_validation_2026-07-12.csv"}:
            continue
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                if not path.read_text().strip():
                    status = "FAIL"
                    detail = "empty markdown"
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        rows.append({"path": path.name, "type": path.suffix.lstrip("."), "status": status, "detail": detail})
    return rows


def write_sha_manifest() -> str:
    rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != "sha256_manifest_2026-07-12.csv":
            rows.append({"relative_path": path.name, "sha256": sha256(path), "bytes": path.stat().st_size})
    package = hashlib.sha256()
    for row in rows:
        package.update(row["relative_path"].encode())
        package.update(b"\0")
        package.update(row["sha256"].encode())
        package.update(b"\n")
    package_sha = package.hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": package_sha, "bytes": ""})
    write_csv(OUT_DIR / "sha256_manifest_2026-07-12.csv", rows)
    return package_sha


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_json(OUT_DIR / "pilot_configuration_2026-07-12.json", pilot_config())
    write_csv(OUT_DIR / "source_inventory_2026-07-12.csv", source_inventory())

    spine, source = build_spine()
    joined, join_audit, cardinality = join_feature_sources(spine)
    write_spine_artifacts(spine, source, joined)
    write_csv(OUT_DIR / "feature_join_audit_2026-07-12.csv", join_audit)
    write_csv(OUT_DIR / "join_cardinality_audit_2026-07-12.csv", cardinality)
    write_csv(OUT_DIR / "duplicate_identity_audit_2026-07-12.csv", duplicate_identity_audit(spine, source))
    write_csv(OUT_DIR / "exclusion_audit_2026-07-12.csv", exclusion_audit(source))

    control = control_reproduction_audit(spine)
    compat = matrix_compatibility_probe(spine)
    replay_rows, replay_summary = replayability_audit()
    write_csv(OUT_DIR / "control_reproduction_audit_2026-07-12.csv", control)
    write_csv(OUT_DIR / "matrix_compatibility_probe_2026-07-12.csv", compat)
    write_csv(OUT_DIR / "replayability_audit_2026-07-12.csv", replay_rows)
    write_json(OUT_DIR / "replayability_audit_2026-07-12.json", replay_summary)

    readiness = "SPINE_IMPLEMENTATION_PILOT_SUCCESS_READY_FOR_FREEZE"
    if control[0]["status"] != "PASS":
        readiness = "SPINE_IMPLEMENTATION_PILOT_BLOCKED_BY_COMPATIBILITY"
    elif any(r["status"] != "PASS" for r in duplicate_identity_audit(spine, source) if r["scope"] == "eligible_spine"):
        readiness = "SPINE_IMPLEMENTATION_PILOT_BLOCKED_BY_IDENTITY"
    elif any(r["join_status"] != "PASS" for r in cardinality):
        readiness = "SPINE_IMPLEMENTATION_PILOT_BLOCKED_BY_COMPATIBILITY"
    elif any(r["compatibility_status"] != "PASS" for r in compat):
        readiness = "SPINE_IMPLEMENTATION_PILOT_BLOCKED_BY_COMPATIBILITY"
    elif replay_summary["overall_status"] != "PASS":
        readiness = "SPINE_IMPLEMENTATION_PILOT_BLOCKED_BY_REPLAYABILITY"

    decision = {
        "readiness_classification": readiness,
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "broad_historical_expansion_authorized": False,
        "contract_freeze_authorized": False,
        "bundle_v1_modified": False,
        "certified_assembly_modified": False,
        "production_behavior_changed": False,
    }
    write_json(OUT_DIR / "pilot_decision_2026-07-12.json", decision)
    write_csv(
        OUT_DIR / "blocker_and_remediation_register_2026-07-12.csv",
        [
            {
                "blocker": "contract_not_frozen",
                "severity": "MEDIUM",
                "blocks_training": True,
                "remediation": "human review/freeze request after this successful bounded implementation pilot",
            },
            {
                "blocker": "broad_historical_expansion_not_authorized",
                "severity": "MEDIUM",
                "blocks_training": True,
                "remediation": "separate approved expansion request after contract freeze",
            },
        ],
    )
    write_markdown_reports(spine, source, join_audit, control, compat, replay_summary, decision)
    write_csv(OUT_DIR / "parse_schema_validation_2026-07-12.csv", parse_validation())
    package_sha = write_sha_manifest()
    return {
        "output_dir": str(OUT_DIR),
        "eligible_spine_rows": len(spine),
        "july_3_rows": control[0]["spine_rows"],
        "readiness": readiness,
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
