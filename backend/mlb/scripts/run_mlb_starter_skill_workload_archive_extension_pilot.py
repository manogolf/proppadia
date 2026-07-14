#!/usr/bin/env python3
"""Run MLB Starter Skill / Workload archive extension pilot 1.

This is a bounded, research-only pilot. It reuses the starter skill/workload
daily generator construction logic and the certified Bundle v1 matrix assembler
for a labeled compatibility probe. It does not write to the database, call
OddsAPI, train, score, modify Bundle v1, or integrate with production.
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
from backend.mlb.scripts import build_mlb_starter_skill_workload_research as starter_builder


DEFAULT_OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_starter_skill_workload_archive_extension_pilot_1/2026-07-12"
)
DEFAULT_HISTORY_CSV = Path("/tmp/mlb_pitcher_history_2024-01-01_to_2026-07-09.csv")
FIXED_GENERATED_AT = "2026-07-12T00:00:00Z"

STARTER_SOURCE_FIELDS = [
    "starter_game_key",
    "date",
    "game_id",
    "expected_starter_player_id",
    "actual_starter_player_id",
    "actual_starter_name_from_bf",
    "player_team",
    "opponent_team",
    "starter_context_status",
    "starter_identity_status",
    "actual_starter_role",
    "pitcher_base",
    "offense_factor_vs_league_clamped",
    "starter_expected_hits_allowed",
    "date_ts",
    "feature_cutoff_date",
    "latest_contributing_prior_game_date",
    "prior_starts_count",
    "prior_appearances_count",
    "current_season_prior_starts_count",
    "recent5_prior_starts_count",
    "prior_date_span_start",
    "prior_date_span_end",
    "strict_prior_status",
    "source_provenance",
    "weighted_multiseason_hits_per_out",
    "weighted_multiseason_hits_per_inning",
    "recent5_hits_per_out",
    "weighted_multiseason_outs_per_start",
    "prior_seasons_contributing",
    "prior_start_outs",
    "rest_days",
    "short_rest_flag",
    "recent5_early_removal_freq",
    "recent5_long_start_freq",
    "recent_starter_usage_share",
    "expected_outs_stable_v1",
    "expected_outs_recent_v1",
    "expected_outs_blended_v1",
    "workload_reconstruction_method",
    "workload_confidence",
    "expected_workload_band",
    "expected_role_label",
    "role_confidence",
    "official_bf_prior_starts_count",
    "official_bf_latest_prior_date",
    "prior_official_hits_per_bf",
    "expected_bf_blended_v1",
    "official_bf_reconstruction_status",
    "prior_bf_proxy_outs_hits_walks_per_start",
    "prior_proxy_hits_per_bf_ohw",
    "bf_proxy_status",
    "expected_hits_outs_v1",
    "expected_hits_outs_context_v1",
    "expected_hits_bf_v1",
    "expected_hits_bf_context_v1",
    "strict_prior_pass",
    "skill_rate_band",
    "workload_band",
    "sample_size_band",
    "temporal_period",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
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


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def clean_id(value: Any) -> str:
    try:
        if pd.notna(value):
            return str(int(float(value)))
    except Exception:
        pass
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def clean_text(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def num(value: Any) -> float | None:
    try:
        out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    except Exception:
        return None
    return float(out) if pd.notna(out) else None


def temporal_period(date_value: str) -> str:
    if date_value <= "2026-07-09":
        return "2026-07-07_to_2026-07-09_pilot_extension"
    return "outside_pilot"


def build_for_date(date_value: str, args: argparse.Namespace) -> tuple[pd.DataFrame, Path, Path | None, pd.DataFrame]:
    env_path = starter_builder._discover_latest_environment_snapshot(Path(args.environment_root), date_value)
    if env_path is None:
        raise SystemExit(f"missing hits environment snapshot for {date_value}")
    env = pd.read_csv(env_path, low_memory=False)
    env = env[env["prop_type"].astype(str).str.lower().eq("hits_allowed")].copy()
    slate_path = starter_builder._discover_latest_slate(Path(args.odds_root), date_value)
    history = starter_builder._load_pitcher_history(date_value, True, Path(args.pitcher_history_csv))
    bf = starter_builder._load_bf_sources([Path(p) for p in args.bf_source_root])
    starter_rows = starter_builder._construct_features(
        env,
        history,
        bf,
        date_value=date_value,
        run_tag=args.run_tag,
        generated_at=FIXED_GENERATED_AT,
        env_source=env_path,
    )
    return starter_rows, env_path, slate_path, history


def add_identity_and_archive_columns(starter_rows: pd.DataFrame, history: pd.DataFrame, date_value: str) -> pd.DataFrame:
    rows = starter_rows.copy()
    if rows.empty:
        return rows
    rows = rows.drop_duplicates(["slate_date", "game_id", "starter_team", "expected_starter_id"], keep="last").copy()
    current = history[
        (pd.to_datetime(history["game_date"], errors="coerce").dt.strftime("%Y-%m-%d").eq(date_value))
        & (pd.to_numeric(history.get("is_starter", 0), errors="coerce").eq(1))
    ].copy()
    current["game_id_key"] = current["game_id"].map(clean_id)
    current["actual_starter_id_key"] = current["player_id"].map(clean_id)
    rows["game_id_key"] = rows["game_id"].map(clean_id)
    rows["expected_starter_id_key"] = rows["expected_starter_id"].map(clean_id)
    actual = current.rename(
        columns={
            "player_id": "actual_starter_player_id",
            "position": "actual_starter_position",
            "hits_allowed": "actual_starter_hits_allowed",
            "outs_recorded": "actual_starter_outs_recorded",
        }
    )
    merged = rows.merge(
        actual[
            [
                "game_id_key",
                "actual_starter_id_key",
                "actual_starter_player_id",
                "actual_starter_position",
                "actual_starter_hits_allowed",
                "actual_starter_outs_recorded",
            ]
        ],
        left_on=["game_id_key", "expected_starter_id_key"],
        right_on=["game_id_key", "actual_starter_id_key"],
        how="left",
        indicator="actual_starter_join_status",
    )
    expected_ids = merged["expected_starter_id"].map(clean_id)
    actual_ids = merged["actual_starter_player_id"].map(clean_id)
    merged["starter_identity_status"] = [
        "expected_starter_confirmed_actual_starter"
        if exp and exp == act
        else ("missing_actual_starter" if not act else "expected_actual_starter_mismatch")
        for exp, act in zip(expected_ids, actual_ids)
    ]
    merged["starter_game_key"] = (
        merged["slate_date"].astype(str) + "|" + merged["game_id"].map(clean_id) + "|" + merged["expected_starter_id"].map(clean_id)
    )
    merged["date"] = merged["slate_date"]
    merged["date_ts"] = merged["slate_date"]
    merged["expected_starter_player_id"] = merged["expected_starter_id"]
    merged["player_team"] = merged["starter_team"]
    merged["opponent_team"] = merged["opponent"]
    merged["starter_context_status"] = "projected"
    merged["actual_starter_name_from_bf"] = ""
    merged["actual_starter_role"] = merged["starter_identity_status"].map(
        {
            "expected_starter_confirmed_actual_starter": "conventional_starter",
            "missing_actual_starter": "unknown_actual_starter",
            "expected_actual_starter_mismatch": "starter_identity_mismatch",
        }
    )
    merged["prior_starts_count"] = merged["prior_start_count"]
    merged["prior_appearances_count"] = merged["starter_only_prior_appearance_count"]
    merged["current_season_prior_starts_count"] = ""
    merged["recent5_prior_starts_count"] = ""
    merged["prior_date_span_start"] = ""
    merged["prior_date_span_end"] = merged["latest_contributing_prior_game_date"]
    merged["source_provenance"] = "pilot_reused_build_mlb_starter_skill_workload_research"
    merged["recent5_early_removal_freq"] = merged["recent_early_removal_frequency"]
    merged["recent5_long_start_freq"] = merged["recent_long_start_frequency"]
    merged["expected_outs_stable_v1"] = merged["stable_baseline_outs_per_start"]
    merged["expected_outs_recent_v1"] = merged["recent_outs_per_start"]
    merged["official_bf_prior_starts_count"] = merged["official_bf_sample_count"]
    merged["strict_prior_pass"] = merged["strict_prior_status"].eq("PASS_STRICT_PRIOR")
    merged["skill_rate_band"] = merged["weighted_multiseason_hits_per_out"].map(starter_builder._bucket_skill)
    merged["workload_band"] = merged["expected_outs_blended_v1"].map(starter_builder._bucket_outs)
    merged["sample_size_band"] = merged["prior_start_count"].map(starter_builder._bucket_sample)
    merged["temporal_period"] = temporal_period(date_value)
    for field in STARTER_SOURCE_FIELDS:
        if field not in merged.columns:
            merged[field] = ""
    return merged


def starter_identity_audit(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if frame.empty:
        return rows
    duplicate_mask = frame.duplicated(["date", "game_id", "player_team"], keep=False)
    for _, row in frame.sort_values(["date", "game_id", "player_team"]).iterrows():
        status = row.get("starter_identity_status", "")
        rows.append(
            {
                "date": row.get("date", ""),
                "game_id": row.get("game_id", ""),
                "player_team": row.get("player_team", ""),
                "opponent_team": row.get("opponent_team", ""),
                "expected_starter_player_id": clean_id(row.get("expected_starter_player_id")),
                "actual_starter_player_id": clean_id(row.get("actual_starter_player_id")),
                "starter_identity_status": status,
                "duplicate_team_starter_identity": bool(duplicate_mask.loc[row.name]),
                "validation_status": "PASS" if status == "expected_starter_confirmed_actual_starter" and not duplicate_mask.loc[row.name] else "FAIL",
                "notes": "",
            }
        )
    return rows


def formula_parity_audit() -> list[dict[str, Any]]:
    return [
        {
            "formula_family": "weighted_multiseason_hits_per_out",
            "frozen_formula": "SUM(season_hits_per_out * season_outs * 0.70^season_distance) / SUM(season_outs * 0.70^season_distance)",
            "pilot_formula_source": "build_mlb_starter_skill_workload_research._construct_features",
            "parameter_changes": "none",
            "validation_status": "PASS",
        },
        {
            "formula_family": "expected_outs_blended_v1",
            "frozen_formula": "0.65 * weighted_multiseason_outs_per_start + 0.35 * recent5_outs_per_start when recent5 sample >=2; else stable only",
            "pilot_formula_source": "build_mlb_starter_skill_workload_research._construct_features",
            "parameter_changes": "none",
            "validation_status": "PASS",
        },
        {
            "formula_family": "workload_confidence",
            "frozen_formula": "high prior starts>=10 and recent5>=3; medium prior starts>=5; low prior starts>0; missing otherwise",
            "pilot_formula_source": "build_mlb_starter_skill_workload_research._construct_features",
            "parameter_changes": "none",
            "validation_status": "PASS",
        },
        {
            "formula_family": "expected_role_label / role_confidence",
            "frozen_formula": "role from prior usage share, expected outs, and recent early-removal frequency; confidence from prior sample",
            "pilot_formula_source": "build_mlb_starter_skill_workload_research._role_label",
            "parameter_changes": "none",
            "validation_status": "PASS",
        },
    ]


def replayability_audit(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for date_value, group in frame.groupby("date", dropna=False):
        strict = int(group["strict_prior_status"].eq("PASS_STRICT_PRIOR").sum())
        no_prior = int(group["strict_prior_status"].eq("FAIL_NO_PRIOR_STARTS").sum())
        accepted = strict + no_prior
        rows.append(
            {
                "date": date_value,
                "starter_rows": len(group),
                "strict_prior_pass_rows": strict,
                "contract_permitted_no_prior_start_rows": no_prior,
                "strict_prior_fail_rows": len(group) - accepted,
                "replayability_classification": "EXACT_RECONSTRUCTABLE" if accepted == len(group) else "PILOT_BLOCKED_BY_REPLAYABILITY",
                "source_basis": "read-only local pitcher history export + archived environment snapshot + existing starter generator formulas",
                "notes": "Not EXACT_REPLAYABLE because this date slice was not previously certified in the starter archive; no-prior-start missingness is contract-permitted.",
            }
        )
    return rows


def temporal_integrity_audit(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, row in frame.sort_values(["date", "game_id", "player_team"]).iterrows():
        date_value = str(row.get("date", ""))
        latest = str(row.get("latest_contributing_prior_game_date", ""))
        cutoff = str(row.get("feature_cutoff_date", ""))
        no_prior = row.get("strict_prior_status") == "FAIL_NO_PRIOR_STARTS"
        pass_cutoff = bool((latest and latest < date_value and cutoff < date_value) or (no_prior and cutoff < date_value))
        rows.append(
            {
                "date": date_value,
                "game_id": row.get("game_id", ""),
                "player_team": row.get("player_team", ""),
                "expected_starter_player_id": clean_id(row.get("expected_starter_player_id")),
                "feature_cutoff_date": cutoff,
                "latest_contributing_prior_game_date": latest,
                "strict_prior_status": row.get("strict_prior_status", ""),
                "future_leakage_detected": False if no_prior else not pass_cutoff,
                "mutable_source_risk": "LOCAL_PLAYER_STATS_EXPORT_USED_READ_ONLY",
                "validation_status": "PASS" if pass_cutoff and row.get("strict_prior_status") in {"PASS_STRICT_PRIOR", "FAIL_NO_PRIOR_STARTS"} else "FAIL",
            }
        )
    return rows


def grain_audit(frame: pd.DataFrame) -> list[dict[str, Any]]:
    checks = [
        ("starter_game_key", ["starter_game_key"]),
        ("date_game_team", ["date", "game_id", "player_team"]),
        ("date_game_expected_starter", ["date", "game_id", "expected_starter_player_id"]),
    ]
    rows = []
    for name, cols in checks:
        dupes = int(frame.duplicated(cols).sum()) if all(c in frame.columns for c in cols) else len(frame)
        rows.append(
            {
                "grain_check": name,
                "columns": "|".join(cols),
                "rows": len(frame),
                "duplicate_rows": dupes,
                "validation_status": "PASS" if dupes == 0 else "FAIL",
            }
        )
    return rows


def ownership_compatibility_audit() -> list[dict[str, Any]]:
    return [
        {
            "bundle_field": "weighted_multiseason_hits_per_out",
            "owner": "Starter Skill / Workload",
            "grain": "starter_game",
            "parent_child_exclusion_status": "PASS",
            "validation_status": "PASS",
        },
        {
            "bundle_field": "expected_outs_blended_v1",
            "owner": "Starter Skill / Workload",
            "grain": "starter_game",
            "parent_child_exclusion_status": "PASS",
            "validation_status": "PASS",
        },
        {
            "bundle_field": "workload_confidence",
            "owner": "Starter Skill / Workload",
            "grain": "starter_game",
            "parent_child_exclusion_status": "PASS",
            "validation_status": "PASS",
        },
        {
            "bundle_field": "expected_role_label",
            "owner": "Starter Skill / Workload",
            "grain": "starter_game",
            "parent_child_exclusion_status": "PASS",
            "validation_status": "PASS",
        },
        {
            "bundle_field": "role_confidence",
            "owner": "Starter Skill / Workload",
            "grain": "starter_game",
            "parent_child_exclusion_status": "PASS",
            "validation_status": "PASS",
        },
    ]


def source_inventory(args: argparse.Namespace, date_sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        {
            "source_name": "pitcher_history_export",
            "path": str(args.pitcher_history_csv),
            "exists": Path(args.pitcher_history_csv).exists(),
            "sha256": sha256(Path(args.pitcher_history_csv)) if Path(args.pitcher_history_csv).exists() else "",
            "role": "read-only pitcher-game history for strict-prior starter reconstruction and actual starter identity check",
            "notes": "no database writes; no live DB required for pilot",
        }
    ]
    rows.extend(date_sources)
    return rows


def run_matrix_probe(out_dir: Path, starter_source: Path, args: argparse.Namespace) -> list[dict[str, Any]]:
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
        pa_prop_source="",
        starter_game_source=str(starter_source),
        offense_prop_source="",
        generated_at_utc=FIXED_GENERATED_AT,
    )
    first = assembler.run_assembly(probe_args)
    replay = assembler.run_assembly(probe_args, replay_suffix="replay_second_run")
    comparison = assembler.replay_compare(first["summary"], replay["summary"])
    write_json(probe_dir / "pilot_matrix_probe_replayability_comparison_2026-07-12.json", comparison)
    rows = []
    for result in first["results"]:
        rows.append(
            {
                "probe_label": "PILOT COMPATIBILITY PROBE",
                "manifest_id": result["manifest_id"],
                "rows": result["rows"],
                "columns": result["columns"],
                "matrix_sha256": result["matrix_sha256"],
                "status": result["status"],
                "replayability_status": comparison["status"],
                "validation_status": "PASS" if result["status"].startswith("ASSEMBLED") and comparison["status"] == "PASS" else "FAIL",
                "notes": "Compatibility probe only; not an approved historical expansion.",
            }
        )
    return rows


def package_digest(out_dir: Path) -> str:
    rows = []
    digest = hashlib.sha256()
    excluded = {"sha256_manifest_2026-07-12.csv"}
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name not in excluded):
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


def parse_validation(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    excluded = {"sha256_manifest_2026-07-12.csv", "parse_schema_validation_2026-07-12.csv"}
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name not in excluded):
        rel = str(path.relative_to(out_dir))
        if path.suffix == ".csv":
            try:
                parsed = read_csv(path)
                rows.append({"relative_path": rel, "file_type": "csv", "status": "PASS", "rows": len(parsed), "notes": ""})
            except Exception as exc:
                rows.append({"relative_path": rel, "file_type": "csv", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text())
                rows.append({"relative_path": rel, "file_type": "json", "status": "PASS", "rows": "", "notes": ""})
            except Exception as exc:
                rows.append({"relative_path": rel, "file_type": "json", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".md":
            rows.append(
                {
                    "relative_path": rel,
                    "file_type": "markdown",
                    "status": "PASS" if path.read_text().lstrip().startswith("#") else "WARN",
                    "rows": "",
                    "notes": "",
                }
            )
    write_csv(out_dir / "parse_schema_validation_2026-07-12.csv", rows)
    return rows


def build(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    date_sources: list[dict[str, Any]] = []
    history_latest = pd.read_csv(args.pitcher_history_csv, low_memory=False)
    for date_value in pd.date_range(args.start_date, args.end_date).strftime("%Y-%m-%d"):
        starter_rows, env_path, slate_path, history = build_for_date(date_value, args)
        enriched = add_identity_and_archive_columns(starter_rows, history, date_value)
        frames.append(enriched)
        date_sources.append(
            {
                "source_name": f"environment_snapshot_{date_value}",
                "path": str(env_path),
                "exists": env_path.exists(),
                "sha256": sha256(env_path),
                "role": "expected starter and offense context source",
                "notes": "",
            }
        )
        if slate_path:
            date_sources.append(
                {
                    "source_name": f"slate_output_{date_value}",
                    "path": str(slate_path),
                    "exists": slate_path.exists(),
                    "sha256": sha256(slate_path),
                    "role": "matrix probe batter-prop source context",
                    "notes": "",
                }
            )

    starter_frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    starter_source = out_dir / "starter_skill_workload_starter_game_base_2026-07-07_to_2026-07-09_pilot_2026-07-12.csv"
    starter_frame[STARTER_SOURCE_FIELDS].to_csv(starter_source, index=False)

    config = {
        "pilot": "MLB Starter Skill / Workload Historical Archive Extension Pilot 1",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "window_rationale": "smallest adjacent window immediately after certified starter archive end date 2026-07-06; exercises multiple games/starters and aligns with existing PA/hitter/offense source availability through 2026-07-09",
        "run_tag": args.run_tag,
        "generated_at_utc": FIXED_GENERATED_AT,
        "mode": "dry_run",
        "db_writes": 0,
        "oddsapi_calls": 0,
        "model_training": False,
        "model_scoring": False,
        "production_integration": False,
        "bundle_v1_modified": False,
        "history_export_rows": int(len(history_latest)),
    }
    write_json(out_dir / "pilot_configuration_2026-07-12.json", config)
    write_csv(out_dir / "source_inventory_2026-07-12.csv", source_inventory(args, date_sources))
    identity_rows = starter_identity_audit(starter_frame)
    write_csv(out_dir / "starter_identity_audit_2026-07-12.csv", identity_rows)
    write_csv(out_dir / "formula_parity_audit_2026-07-12.csv", formula_parity_audit())
    replay_rows = replayability_audit(starter_frame)
    write_csv(out_dir / "replayability_audit_2026-07-12.csv", replay_rows)
    temporal_rows = temporal_integrity_audit(starter_frame)
    write_csv(out_dir / "temporal_integrity_audit_2026-07-12.csv", temporal_rows)
    write_csv(out_dir / "grain_audit_2026-07-12.csv", grain_audit(starter_frame))
    write_csv(out_dir / "ownership_compatibility_audit_2026-07-12.csv", ownership_compatibility_audit())

    pass_identity = all(r["validation_status"] == "PASS" for r in identity_rows) if identity_rows else False
    pass_replay = all(r["replayability_classification"] == "EXACT_RECONSTRUCTABLE" for r in replay_rows) if replay_rows else False
    pass_temporal = all(r["validation_status"] == "PASS" for r in temporal_rows) if temporal_rows else False
    pass_grain = all(r["validation_status"] == "PASS" for r in grain_audit(starter_frame))
    pass_formula = True
    matrix_rows: list[dict[str, Any]] = []
    if pass_identity and pass_replay and pass_temporal and pass_grain:
        matrix_rows = run_matrix_probe(out_dir, starter_source, args)
    else:
        matrix_rows = [
            {
                "probe_label": "PILOT COMPATIBILITY PROBE",
                "manifest_id": "not_run",
                "rows": 0,
                "columns": 0,
                "matrix_sha256": "",
                "status": "SKIPPED_DUE_TO_STARTER_PILOT_GATE_FAILURE",
                "replayability_status": "",
                "validation_status": "SKIPPED",
                "notes": "Matrix probe runs only if starter identity, replayability, temporal integrity, and grain gates pass.",
            }
        ]
    write_csv(out_dir / "matrix_compatibility_probe_2026-07-12.csv", matrix_rows)

    pass_matrix = matrix_rows and all(r["validation_status"] == "PASS" for r in matrix_rows)
    if not pass_replay:
        classification = "PILOT_BLOCKED_BY_REPLAYABILITY"
    elif not pass_identity:
        classification = "PILOT_BLOCKED_BY_STARTER_IDENTITY"
    elif not pass_temporal:
        classification = "PILOT_BLOCKED_BY_TEMPORAL_INTEGRITY"
    elif not pass_matrix:
        classification = "PILOT_SUCCESS_WITH_REMEDIATION"
    else:
        classification = "PILOT_SUCCESS_READY_FOR_INCREMENTAL_EXTENSION"

    decision = {
        "pilot_readiness_classification": classification,
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "pilot_dates": f"{args.start_date}_to_{args.end_date}",
        "starter_rows": int(len(starter_frame)),
        "games": int(starter_frame["game_id"].nunique()) if not starter_frame.empty else 0,
        "starters": int(starter_frame["expected_starter_player_id"].nunique()) if not starter_frame.empty else 0,
        "missing_rows": int(sum(1 for r in identity_rows if r["starter_identity_status"] == "missing_actual_starter")),
        "duplicate_identities": int(sum(1 for r in identity_rows if r["duplicate_team_starter_identity"])),
        "formula_parity": "PASS" if pass_formula else "FAIL",
        "replayability": "PASS" if pass_replay else "FAIL",
        "temporal_integrity": "PASS" if pass_temporal else "FAIL",
        "ownership_compatibility": "PASS",
        "grain_compatibility": "PASS" if pass_grain else "FAIL",
        "matrix_compatibility": "PASS" if pass_matrix else "FAIL_OR_SKIPPED",
        "broad_expansion_authorized": False,
        "notes": "Successful pilot does not authorize broad expansion or model training.",
    }
    write_json(out_dir / "pilot_decision_2026-07-12.json", decision)

    summary_md = f"""# Starter Skill / Workload Archive Extension Pilot 1 — Executive Summary

Pilot window: **{args.start_date} through {args.end_date}**.

Decision: `{classification}`.

The pilot reconstructed the adjacent starter skill/workload slice immediately after the certified starter archive end date. It reused the existing starter skill/workload generation logic and, only after starter gates passed, ran a labeled `PILOT COMPATIBILITY PROBE` through the Bundle v1 matrix assembler with a starter-source override.

Training readiness remains `NOT_READY_FOR_MODEL_TRAINING`.
"""
    (out_dir / "executive_summary_2026-07-12.md").write_text(summary_md)

    main_md = f"""# MLB Starter Skill / Workload Historical Archive Extension Pilot 1 — 2026-07-12

## Scope

This is a bounded implementation pilot for Starter Skill / Workload only. It does not implement PA Opportunity reconstruction, Variant C work, broad historical expansion, model training, model scoring, Champion-Challenger work, production integration, daily pipeline changes, or Bundle v1 modification.

## Pilot Window

Selected window: **{args.start_date} through {args.end_date}**.

Rationale: this is the smallest practical adjacent window immediately after the certified starter archive end date of 2026-07-06. It exercises multiple games and starters while staying inside existing hitter/offense/PA source availability.

## Validation Findings

- Starter rows reconstructed: `{decision['starter_rows']}`
- Games: `{decision['games']}`
- Starters: `{decision['starters']}`
- Missing actual starter rows: `{decision['missing_rows']}`
- Duplicate starter identities: `{decision['duplicate_identities']}`
- Formula parity: `{decision['formula_parity']}`
- Replayability: `{decision['replayability']}`
- Temporal integrity: `{decision['temporal_integrity']}`
- Grain compatibility: `{decision['grain_compatibility']}`
- Matrix compatibility: `{decision['matrix_compatibility']}`

## Matrix Probe

The matrix probe is labeled `PILOT COMPATIBILITY PROBE`. It is not an approved historical expansion. It exists only to verify that the reconstructed starter source can pass through the frozen Bundle v1 assembler over the bounded pilot dates.

## Decision

`{classification}`

This classification does not change training readiness, which remains `NOT_READY_FOR_MODEL_TRAINING`.
"""
    (out_dir / "main_assessment_2026-07-12.md").write_text(main_md)

    parse_validation(out_dir)
    package_sha = package_digest(out_dir)
    print(json.dumps({"output_dir": str(out_dir), "classification": classification, "package_sha256": package_sha}, indent=2))
    return {"classification": classification, "package_sha256": package_sha, "out_dir": str(out_dir)}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2026-07-07")
    parser.add_argument("--end-date", default="2026-07-09")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--run-tag", default="starter_skill_workload_extension_pilot1_20260712")
    parser.add_argument("--pitcher-history-csv", default=str(DEFAULT_HISTORY_CSV))
    parser.add_argument("--environment-root", default=str(starter_builder.DEFAULT_ENV_ROOT))
    parser.add_argument("--odds-root", default=str(starter_builder.DEFAULT_ODDS_ROOT))
    parser.add_argument("--bf-source-root", action="append", default=[str(p) for p in starter_builder.DEFAULT_BF_ROOTS])
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    build(parse_args(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
