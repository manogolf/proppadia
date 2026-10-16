#!/usr/bin/env python3
"""Bounded PP-TOI feature replay for frozen 2025 HGB evaluation identities.

This read-only analysis applies only independently reconciled 2023/24 values
from the PP-TOI recovery candidate. It uses the frozen HGB 120-day, strict-
prior, latest-ten player-game mean contract and does not score or fit a model.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RAW = ROOT / "artifacts/analysis/nhl/raw_foundation_integrity_2026-10-16/current_raw_snapshot.csv"
DEFAULT_CANDIDATE = ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/pp_toi_recovery_candidates.csv"
DEFAULT_TARGETS = ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/hgb_pp_toi_feature_exposure_training.csv"

VERIFIED = {"VERIFIED_CORRECT", "VERIFIED_INCORRECT"}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def replay(raw_path: Path, candidate_path: Path, target_path: Path,
           seasons: set[int] | None = None,
           history_identities_path: Path | None = None) -> pd.DataFrame:
    raw = pd.read_csv(raw_path)
    candidates = pd.read_csv(candidate_path)
    targets = pd.read_csv(target_path)
    required_targets = {"game_date", "game_id", "player_id"}
    if not required_targets.issubset(targets.columns):
        raise ValueError("TARGET_IDENTITY_SCHEMA_MISSING")
    keep = ["game_date", "game_id", "player_id"]
    for optional in ("canonical_season", "mean_pp_toi_last10"):
        if optional in targets:
            keep.append(optional)
    targets = targets[keep]
    if seasons is not None:
        if "canonical_season" not in targets:
            raise ValueError("TARGET_SEASON_COLUMN_MISSING")
        targets = targets.loc[pd.to_numeric(targets.canonical_season).isin(seasons)].copy()
    raw["game_id"] = pd.to_numeric(raw.game_id, errors="raise").astype("int64")
    raw["player_id"] = pd.to_numeric(raw.player_id, errors="raise").astype("int64")
    raw["game_date"] = pd.to_datetime(raw.game_date).dt.normalize()
    raw["pp_toi_minutes"] = pd.to_numeric(raw.pp_toi_minutes, errors="coerce")
    targets["game_id"] = pd.to_numeric(targets.game_id, errors="raise").astype("int64")
    targets["player_id"] = pd.to_numeric(targets.player_id, errors="raise").astype("int64")
    targets["game_date"] = pd.to_datetime(targets.game_date).dt.normalize()
    if raw.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_RAW_PLAYER_GAME")
    if targets.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_HGB_EVALUATION_IDENTITY")
    identity_source = pd.read_csv(history_identities_path or target_path,
                                  usecols=["game_id", "player_id"])
    identity_source["game_id"] = pd.to_numeric(identity_source.game_id, errors="raise").astype("int64")
    identity_source["player_id"] = pd.to_numeric(identity_source.player_id, errors="raise").astype("int64")
    history_keys = set(zip(identity_source.game_id, identity_source.player_id))
    raw = raw.loc[[key in history_keys for key in zip(raw.game_id, raw.player_id)]].copy()

    candidate_values = candidates.loc[:, ["game_id", "player_id", "reconstructed_pp_toi_minutes",
        "checkpoint_pp_toi_minutes", "classification"]].copy()
    candidate_values["game_id"] = pd.to_numeric(candidate_values.game_id, errors="raise").astype("int64")
    candidate_values["player_id"] = pd.to_numeric(candidate_values.player_id, errors="raise").astype("int64")
    for col in ("reconstructed_pp_toi_minutes", "checkpoint_pp_toi_minutes"):
        candidate_values[col] = pd.to_numeric(candidate_values[col], errors="coerce")
    if candidate_values.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_CANDIDATE_IDENTITY")
    candidate_map = {
        (int(r.game_id), int(r.player_id)): (float(r.reconstructed_pp_toi_minutes)
            if pd.notna(r.reconstructed_pp_toi_minutes) else None, str(r.classification))
        for r in candidate_values.itertuples(index=False)
    }
    by_player = {int(pid): g.sort_values(["game_date", "game_id"], kind="mergesort")
                 for pid, g in raw.groupby("player_id", sort=False)}

    rows = []
    for target in targets.itertuples(index=False):
        group = by_player.get(int(target.player_id))
        if group is None:
            rows.append({"game_date": target.game_date.date().isoformat(), "game_id": int(target.game_id),
                         "player_id": int(target.player_id), "status": "TARGET_RAW_IDENTITY_MISSING"})
            continue
        lower = target.game_date - pd.Timedelta(days=120)
        hist = group.loc[group.game_date.lt(target.game_date) & group.game_date.ge(lower)].tail(10)
        before = hist.pp_toi_minutes.copy()
        after = before.copy()
        used_verified = 0
        for idx, hrow in enumerate(hist.itertuples(index=False)):
            key = (int(hrow.game_id), int(hrow.player_id))
            candidate = candidate_map.get(key)
            if candidate and candidate[1] in VERIFIED and candidate[0] is not None:
                after.iloc[idx] = candidate[0]
                used_verified += 1
        before_mean = float(before.mean()) if before.notna().any() else None
        after_mean = float(after.mean()) if after.notna().any() else None
        rows.append({
            "game_date": target.game_date.date().isoformat(), "game_id": int(target.game_id),
            "player_id": int(target.player_id), "status": "REPLAYED",
            "prior_player_games_in_120d": int(len(hist)),
            "verified_reconstruction_rows_in_window": used_verified,
            "mean_pp_toi_last10_before": before_mean,
            "mean_pp_toi_last10_after_verified_source_values": after_mean,
            "frozen_feature_mean_pp_toi_last10": getattr(target, "mean_pp_toi_last10", None),
            "current_raw_replay_matches_frozen_feature": (
                abs(before_mean - float(target.mean_pp_toi_last10)) <= 1e-6
                if before_mean is not None and pd.notna(getattr(target, "mean_pp_toi_last10", None))
                else None
            ),
            "delta_minutes": (after_mean - before_mean) if before_mean is not None and after_mean is not None else None,
        })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, default=DEFAULT_RAW)
    parser.add_argument("--candidate", type=Path, default=DEFAULT_CANDIDATE)
    parser.add_argument("--targets", type=Path, default=DEFAULT_TARGETS)
    parser.add_argument("--history-identities", type=Path)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--season", type=int, action="append", default=[])
    args = parser.parse_args()
    result = replay(args.raw, args.candidate, args.targets,
                    set(args.season) if args.season else None, args.history_identities)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.out, index=False, float_format="%.17g")
    replayed = result.loc[result.status.eq("REPLAYED")]
    changed = replayed.loc[replayed.delta_minutes.fillna(0).ne(0)]
    def optional_mean(series: pd.Series):
        value = series.mean()
        return None if pd.isna(value) else float(value)

    frozen_match_count = None
    frozen_comparable_count = 0
    if "current_raw_replay_matches_frozen_feature" in replayed:
        comparisons = replayed.current_raw_replay_matches_frozen_feature
        frozen_comparable_count = int(comparisons.notna().sum())
        frozen_match_count = int(sum(bool(value) for value in comparisons if pd.notna(value)))
    summary = {
        "schema_version": "NHL_HGB_PP_TOI_FEATURE_EXPOSURE_V1",
        "frozen_hgb_contract": "NHL_POINTS_COUNT_HGB_V1_HISTORY_120_DAY_VALIDATED",
        "window": "strict prior game_date, 120-day lower bound, last 10 player games, arithmetic mean",
        "historical_hgb_feature_construction": "backend/nhl/scripts/export_nhl_points_hgb_features.py:build_features",
        "target_evaluation_artifact": str(args.targets), "target_evaluation_sha256": sha(args.targets),
        "history_identity_source": str(args.history_identities or args.targets),
        "raw_snapshot": str(args.raw), "raw_snapshot_sha256": sha(args.raw),
        "candidate_file": str(args.candidate), "candidate_sha256": sha(args.candidate),
        "output_path": str(args.out), "output_sha256": sha(args.out),
        "target_rows": int(len(result)), "replayed_rows": int(len(replayed)),
        "missing_target_raw_identity_rows": int(result.status.ne("REPLAYED").sum()),
        "targets_with_verified_source_rows_in_window": int(replayed.verified_reconstruction_rows_in_window.gt(0).sum()),
        "targets_with_changed_mean_pp_toi_last10": int(len(changed)),
        "targets_matching_frozen_feature_baseline": frozen_match_count,
        "targets_with_comparable_frozen_feature_baseline": frozen_comparable_count,
        "mean_absolute_change_minutes_among_changed_targets": float(changed.delta_minutes.abs().mean()) if len(changed) else 0.0,
        "maximum_absolute_change_minutes": float(changed.delta_minutes.abs().max()) if len(changed) else 0.0,
        "mean_before_all_replayed_targets": optional_mean(replayed.mean_pp_toi_last10_before),
        "mean_after_verified_substitutions_all_replayed_targets": optional_mean(replayed.mean_pp_toi_last10_after_verified_source_values),
        "baseline": "current raw PP-TOI values over the frozen outcome identity set",
        "after": "official reconstruction substituted only for independently verified games; unresolved identities retain current raw value",
        "predictions_modified": False,
    }
    summary_path = args.out.with_name(args.out.stem + "_summary.json")
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
