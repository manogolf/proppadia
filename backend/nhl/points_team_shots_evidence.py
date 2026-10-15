"""Deterministic, offline builder/verifier for NHL Points team-shot evidence.

This package format is deliberately separate from the daily exporter. A future
integration must obtain the feature export and raw rows in the same database
snapshot before calling ``write_evidence_package``.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from collections.abc import Mapping

import numpy as np
import pandas as pd

RAW_COLUMNS = ("player_id", "game_id", "game_date", "team_id", "shots_on_goal", "shot_attempts", "toi_minutes")
TARGET_COLUMNS = ("player_id", "game_id", "game_date", "team_id")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def reconstruct_from_raw_rows(
    raw_rows: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    team_aliases: Mapping[int, int] | None = None,
    expected_team_game_rows: Mapping[tuple[int, int], int] | None = None,
    source_coverage_complete: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return target features, aggregated history games, and lossless raw rows.

    Expected roster counts, when available, make incomplete groups explicit.
    Missing SOG values retain their nulls in raw evidence and are SQL-compatible
    zero contributions to the aggregate; a missing player/game record is never
    created as a zero-SOG observation.
    """
    if not set(RAW_COLUMNS).issubset(raw_rows.columns):
        raise ValueError("RAW_TEAM_SHOT_EVIDENCE_SCHEMA_MISSING")
    if not set(TARGET_COLUMNS).issubset(targets.columns):
        raise ValueError("TARGET_TEAM_SHOT_EVIDENCE_SCHEMA_MISSING")
    raw = raw_rows[list(RAW_COLUMNS)].copy()
    target = targets.copy()
    for frame, label in ((raw, "RAW"), (target, "TARGET")):
        for column in ("player_id", "game_id", "team_id"):
            frame[column] = pd.to_numeric(frame[column], errors="raise").astype("int64")
        frame["game_date"] = pd.to_datetime(frame.game_date, errors="raise").dt.normalize()
    if raw.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_RAW_PLAYER_GAME_ROW")
    if target.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_TARGET_PLAYER_GAME_ROW")
    aliases = {int(k): int(v) for k, v in (team_aliases or {}).items()}
    raw["raw_team_id"] = raw.team_id
    raw["canonical_team_id"] = raw.team_id.map(lambda value: aliases.get(int(value), int(value)))
    target["raw_team_id"] = target.team_id
    target["canonical_team_id"] = target.team_id.map(lambda value: aliases.get(int(value), int(value)))
    raw["shots_on_goal_numeric"] = pd.to_numeric(raw.shots_on_goal, errors="coerce")
    raw["shot_attempts_numeric"] = pd.to_numeric(raw.shot_attempts, errors="coerce")
    expected = {(int(game), int(team)): int(count) for (game, team), count in (expected_team_game_rows or {}).items()}

    team_games = (raw.groupby(["canonical_team_id", "game_id", "game_date"], as_index=False, sort=True)
                  .agg(source_player_rows=("player_id", "size"), unique_players=("player_id", "nunique"),
                       missing_sog_rows=("shots_on_goal_numeric", lambda values: int(values.isna().sum())),
                       missing_attempts_rows=("shot_attempts_numeric", lambda values: int(values.isna().sum())),
                       team_sog=("shots_on_goal_numeric", lambda values: float(values.fillna(0).sum())),
                       team_attempts=("shot_attempts_numeric", lambda values: float(values.fillna(0).sum()))))
    team_games["expected_player_rows"] = [expected.get((int(game), int(team))) for game, team in zip(team_games.game_id, team_games.canonical_team_id)]
    team_games["source_coverage_complete"] = [
        bool(source_coverage_complete and missing == 0 and (exp is None or int(rows) >= exp))
        for rows, missing, exp in zip(team_games.source_player_rows, team_games.missing_sog_rows, team_games.expected_player_rows)
    ]

    target_rows = []
    history_rows = []
    for target_row in target.itertuples(index=False):
        eligible = team_games.loc[
            team_games.canonical_team_id.eq(int(target_row.canonical_team_id))
            & team_games.game_date.lt(target_row.game_date)
            & team_games.game_date.ge(target_row.game_date - pd.Timedelta(days=120))
        ].sort_values(["game_date", "game_id"], kind="mergesort").tail(10)
        complete = bool(source_coverage_complete and eligible.source_coverage_complete.all())
        valid_count = int(len(eligible))
        feature = float(eligible.team_sog.mean()) if valid_count else 0.0
        target_rows.append({
            "player_id": int(target_row.player_id), "game_id": int(target_row.game_id),
            "game_date": target_row.game_date.date().isoformat(),
            "raw_target_team_id": int(target_row.raw_team_id),
            "canonical_team_id": int(target_row.canonical_team_id),
            "prior_game_ids": ";".join(map(str, eligible.game_id.astype(int))),
            "valid_prior_game_count": valid_count,
            "source_coverage_complete": complete,
            "team_d10_sf_per_game": feature,
        })
        for ordinal, game in enumerate(eligible.itertuples(index=False), start=1):
            history_rows.append({
                "target_player_id": int(target_row.player_id), "target_game_id": int(target_row.game_id),
                "target_game_date": target_row.game_date.date().isoformat(),
                "raw_target_team_id": int(target_row.raw_team_id), "canonical_team_id": int(target_row.canonical_team_id),
                "history_ordinal_oldest_first": ordinal, "history_game_id": int(game.game_id),
                "history_game_date": game.game_date.date().isoformat(), "source_player_rows": int(game.source_player_rows),
                "unique_players": int(game.unique_players), "expected_player_rows": game.expected_player_rows,
                "missing_sog_rows": int(game.missing_sog_rows), "missing_attempts_rows": int(game.missing_attempts_rows),
                "official_or_raw_team_sog_sum": float(game.team_sog),
                "source_coverage_complete": bool(game.source_coverage_complete),
            })
    return pd.DataFrame(target_rows), pd.DataFrame(history_rows), raw


def validate_reconstruction_against_export(target_features: pd.DataFrame, feature_export: pd.DataFrame) -> dict:
    """Bind reconstructed values to the exact exporter player/game rows."""
    keys = ["game_id", "player_id"]
    required = {*keys, "team_d10_sf_per_game"}
    if not required.issubset(feature_export.columns):
        raise ValueError("FEATURE_EXPORT_TEAM_SHOT_BINDING_COLUMNS_MISSING")
    if feature_export.duplicated(keys).any() or target_features.duplicated(keys).any():
        raise ValueError("FEATURE_EXPORT_OR_EVIDENCE_IDENTITY_DUPLICATE")
    joined = feature_export[keys + ["team_d10_sf_per_game"]].merge(
        target_features[keys + ["team_d10_sf_per_game", "source_coverage_complete"]],
        on=keys, how="outer", validate="one_to_one", suffixes=("_export", "_evidence"), indicator=True)
    if not joined._merge.eq("both").all():
        raise ValueError("FEATURE_EXPORT_EVIDENCE_IDENTITY_SET_MISMATCH")
    errors = (pd.to_numeric(joined.team_d10_sf_per_game_export, errors="raise")
              - pd.to_numeric(joined.team_d10_sf_per_game_evidence, errors="raise")).abs()
    if errors.gt(1e-12).any():
        raise ValueError("FEATURE_EXPORT_EVIDENCE_VALUE_MISMATCH")
    return {"status": "PASS", "target_rows": int(len(joined)), "exact_feature_rows": int(errors.le(1e-12).sum()),
            "incomplete_source_rows": int((~joined.source_coverage_complete.astype(bool)).sum()),
            "maximum_absolute_difference": float(errors.max()) if len(errors) else 0.0}


def write_evidence_package(
    *, root: Path, run_id: str, slate_date: str, feature_export_bytes: bytes,
    targets: pd.DataFrame, raw_rows: pd.DataFrame, acquired_at_utc: str,
    team_aliases: Mapping[int, int] | None = None,
    expected_team_game_rows: Mapping[tuple[int, int], int] | None = None,
    source_coverage_complete: bool = True,
) -> Path:
    """Write a create-only run package with deterministic CSVs and SHA-256s."""
    import io

    directory = Path(root) / f"slate_date={slate_date}" / f"run_id={run_id}"
    directory.mkdir(parents=True, exist_ok=False)
    target_features, history, raw = reconstruct_from_raw_rows(
        raw_rows, targets, team_aliases=team_aliases,
        expected_team_game_rows=expected_team_game_rows,
        source_coverage_complete=source_coverage_complete,
    )
    feature_export = pd.read_csv(io.BytesIO(feature_export_bytes))
    validation = validate_reconstruction_against_export(target_features, feature_export)
    files = {
        "feature_export.csv": feature_export_bytes,
        "raw_prior_player_games.csv": raw.to_csv(index=False, float_format="%.17g", lineterminator="\n").encode(),
        "prior_team_game_aggregation.csv": history.to_csv(index=False, float_format="%.17g", lineterminator="\n").encode(),
        "target_team_shots.csv": target_features.to_csv(index=False, float_format="%.17g", lineterminator="\n").encode(),
    }
    for filename, content in files.items():
        (directory / filename).write_bytes(content)
    manifest = {
        "schema_version": "NHL_POINTS_PREGAME_TEAM_SHOT_EVIDENCE_V1",
        "parent_daily_run_id": str(run_id), "slate_date": str(slate_date),
        "acquired_at_utc": str(acquired_at_utc),
        "same_snapshot_as_feature_export": False,
        "feature_export_sha256": sha256_bytes(feature_export_bytes),
        "source_raw_player_game_rows": int(len(raw)),
        "target_player_game_rows": int(len(target_features)),
        "source_completeness_validation": validation,
        "files": {name: {"sha256": sha256_bytes(content), "bytes": len(content)} for name, content in sorted(files.items())},
        "run_id_directory_is_create_only": True,
    }
    manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    (directory / "manifest.json").write_bytes(manifest_bytes)
    sums = "".join(f"{sha256_bytes(content)}  {name}\n" for name, content in sorted(files.items()))
    sums += f"{sha256_bytes(manifest_bytes)}  manifest.json\n"
    (directory / "SHA256SUMS").write_text(sums)
    return directory


def verify_evidence_package(directory: Path) -> dict:
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    for name, identity in manifest["files"].items():
        path = directory / name
        if path.stat().st_size != int(identity["bytes"]) or sha256_file(path) != identity["sha256"]:
            raise ValueError(f"EVIDENCE_FILE_HASH_MISMATCH:{name}")
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        expected, name = line.split("  ", 1)
        if sha256_file(directory / name) != expected:
            raise ValueError(f"EVIDENCE_MANIFEST_HASH_MISMATCH:{name}")
    if manifest.get("parent_daily_run_id") != directory.name.removeprefix("run_id="):
        raise ValueError("EVIDENCE_PACKAGE_RUN_ID_MISMATCH")
    return manifest
