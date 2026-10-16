"""Deterministic, offline builder/verifier for NHL Points team-shot evidence.

This package format is deliberately separate from the daily exporter. A future
integration must obtain the feature export and raw rows in the same database
snapshot before calling ``write_evidence_package``.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime, timezone
from backend.nhl.daily_orchestration import redact_sensitive_text

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


def _refresh_package_manifest(directory: Path, updates: Mapping[str, object]) -> str:
    manifest_path = Path(directory) / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.update(dict(updates))
    encoded = (json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    manifest_path.write_bytes(encoded)
    (Path(directory) / "SHA256SUMS").write_text("".join(
        f"{sha256_file(path)}  {path.name}\n" for path in sorted(Path(directory).iterdir())
        if path.is_file() and path.name != "SHA256SUMS"))
    return sha256_file(Path(directory) / "SHA256SUMS")


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
    optional_raw = [name for name in ("game_start_utc", "canonical_team_id", "home_team_id", "away_team_id")
                    if name in raw_rows]
    raw = raw_rows[list(RAW_COLUMNS) + optional_raw].copy()
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
    raw["feature_team_id"] = raw.team_id.map(lambda value: aliases.get(int(value), int(value)))
    if "canonical_team_id" not in raw:
        raw["canonical_team_id"] = raw.feature_team_id
    else:
        raw["canonical_team_id"] = pd.to_numeric(raw.canonical_team_id, errors="coerce")
    target["raw_team_id"] = target.team_id
    target["feature_team_id"] = target.team_id.map(lambda value: aliases.get(int(value), int(value)))
    if "canonical_team_id" not in target:
        target["canonical_team_id"] = target.feature_team_id
    else:
        target["canonical_team_id"] = pd.to_numeric(target.canonical_team_id, errors="coerce")
    raw["shots_on_goal_numeric"] = pd.to_numeric(raw.shots_on_goal, errors="coerce")
    raw["shot_attempts_numeric"] = pd.to_numeric(raw.shot_attempts, errors="coerce")
    expected = {(int(game), int(team)): int(count) for (game, team), count in (expected_team_game_rows or {}).items()}

    team_games = (raw.groupby(["feature_team_id", "game_id", "game_date"], as_index=False, sort=True)
                  .agg(source_player_rows=("player_id", "size"), unique_players=("player_id", "nunique"),
                       missing_sog_rows=("shots_on_goal_numeric", lambda values: int(values.isna().sum())),
                       missing_attempts_rows=("shot_attempts_numeric", lambda values: int(values.isna().sum())),
                       team_sog=("shots_on_goal_numeric", lambda values: float(values.fillna(0).sum())),
                       team_attempts=("shot_attempts_numeric", lambda values: float(values.fillna(0).sum()))))
    team_games["expected_player_rows"] = [expected.get((int(game), int(team))) for game, team in zip(team_games.game_id, team_games.feature_team_id)]
    team_games["source_coverage_complete"] = [
        bool(source_coverage_complete and missing == 0 and (exp is None or int(rows) >= exp))
        for rows, missing, exp in zip(team_games.source_player_rows, team_games.missing_sog_rows, team_games.expected_player_rows)
    ]

    target_rows = []
    history_rows = []
    for target_row in target.itertuples(index=False):
        eligible = team_games.loc[
            team_games.feature_team_id.eq(int(target_row.feature_team_id))
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
                "feature_team_id": int(target_row.feature_team_id),
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
                "feature_team_id": int(target_row.feature_team_id),
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
    same_snapshot_as_feature_export: bool = False,
    snapshot_metadata: Mapping[str, object] | None = None,
) -> Path:
    """Write a create-only run package with deterministic CSVs and SHA-256s."""
    import io

    directory = Path(root) / f"slate_date={slate_date}" / f"run_id={run_id}"
    directory.mkdir(parents=True, exist_ok=False)
    raw_bytes = raw_rows.to_csv(index=False, float_format="%.17g", lineterminator="\n").encode()
    target_bytes = targets.to_csv(index=False, float_format="%.17g", lineterminator="\n").encode()
    files = {
        "feature_export.csv": feature_export_bytes,
        "raw_prior_player_games.csv": raw_bytes,
        "target_bindings.csv": target_bytes,
    }
    capture_status = "COMPLETE"
    capture_error = None
    validation: dict[str, object] = {"status": "NOT_RUN"}
    raw: pd.DataFrame = raw_rows
    target_features = pd.DataFrame()
    try:
        target_features, history, raw = reconstruct_from_raw_rows(
            raw_rows, targets, team_aliases=team_aliases,
            expected_team_game_rows=expected_team_game_rows,
            source_coverage_complete=source_coverage_complete,
        )
        feature_export = pd.read_csv(io.BytesIO(feature_export_bytes))
        validation = validate_reconstruction_against_export(target_features, feature_export)
        files["prior_team_game_aggregation.csv"] = history.to_csv(
            index=False, float_format="%.17g", lineterminator="\n").encode()
        files["target_team_shots.csv"] = target_features.to_csv(
            index=False, float_format="%.17g", lineterminator="\n").encode()
    except Exception as error:
        capture_status = "PARTIAL"
        capture_error = f"{type(error).__name__}:{error}"
    for filename, content in files.items():
        (directory / filename).write_bytes(content)
    manifest = {
        "schema_version": "NHL_POINTS_PREGAME_TEAM_SHOT_EVIDENCE_V1",
        "capture_status": capture_status,
        "capture_error": capture_error,
        "parent_daily_run_id": str(run_id), "slate_date": str(slate_date),
        "acquired_at_utc": str(acquired_at_utc),
        "same_snapshot_as_feature_export": bool(same_snapshot_as_feature_export),
        "snapshot_metadata": dict(snapshot_metadata or {}),
        "feature_export_sha256": sha256_bytes(feature_export_bytes),
        "source_raw_player_game_rows": int(len(raw)),
        "target_player_game_rows": int(len(target_features) if not target_features.empty else len(targets)),
        "raw_sog_null_rows": int(pd.to_numeric(raw_rows.shots_on_goal, errors="coerce").isna().sum()),
        "raw_shot_attempts_null_rows": int(pd.to_numeric(raw_rows.shot_attempts, errors="coerce").isna().sum()),
        "raw_duplicate_player_game_rows": int(raw_rows.duplicated(["game_id", "player_id"]).sum()),
        "raw_source_coverage_assessment": "ROW_COUNTS_RETAINED_EXPECTED_ROSTER_DENOMINATOR_NOT_CAPTURED",
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


def _psql_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def capture_points_export_snapshot(
    *, db_url: str, slate_date: str, season: int, run_id: str,
    export_sql: Path, output_root: Path, psql_environment: Mapping[str, str],
) -> dict[str, object]:
    """Capture the Phoenix export and its exact SQL source rows in one RO snapshot.

    If evidence fails after the feature COPY succeeds, its bytes are still
    returned for the unchanged production scoring path and a checksummed
    PARTIAL/FAILED package records the reason.
    """
    started = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    season_root = Path(output_root) / f"season={season}"
    package = season_root / f"slate_date={slate_date}" / f"run_id={run_id}"
    feature_bytes: bytes | None = None
    status = "EVIDENCE_CAPTURE_FAILED"
    reason: str | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="nhl_points_export_evidence_") as temp_name:
            temp = Path(temp_name)
            feature_path, raw_path, target_path, snapshot_path = (
                temp / "exact_feature_export.csv", temp / "raw_prior_player_game_rows.csv",
                temp / "target_bindings.csv", temp / "database_snapshot.csv")
            script = temp / "export_and_capture.sql"
            raw_query = f"""COPY (
  SELECT l.player_id, l.game_id, g.game_date, g.start_time_utc AS game_start_utc,
         l.team_id, CASE WHEN l.team_id = g.home_team_id THEN g.home_team_id
                         WHEN l.team_id = g.away_team_id THEN g.away_team_id END AS canonical_team_id,
         g.home_team_id, g.away_team_id, l.shots_on_goal, l.shot_attempts, l.toi_minutes
  FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING (game_id)
  WHERE substring(g.game_id::text, 5, 2) = '02'
    AND l.team_id IN (
      SELECT DISTINCT rs.team_id FROM nhl.roster_status rs JOIN nhl.games gs USING (game_id)
      WHERE gs.game_date = DATE :'slate_date' AND rs.team_id IS NOT NULL
    )
    AND g.game_date < DATE :'slate_date'
    AND g.game_date >= DATE :'slate_date' - INTERVAL '120 days'
  ORDER BY g.game_date, g.start_time_utc, l.game_id, l.player_id
) TO STDOUT WITH CSV HEADER;"""
            target_query = f"""COPY (
  SELECT rs.player_id, rs.game_id, g.game_date, g.start_time_utc AS game_start_utc,
         rs.team_id, CASE WHEN rs.team_id = g.home_team_id THEN g.home_team_id
                          WHEN rs.team_id = g.away_team_id THEN g.away_team_id END AS canonical_team_id,
         g.home_team_id, g.away_team_id,
         CASE WHEN rs.team_id = g.home_team_id THEN 1
              WHEN rs.team_id = g.away_team_id THEN 0 END AS is_home
  FROM nhl.roster_status rs JOIN nhl.games g USING (game_id)
  WHERE g.game_date = DATE :'slate_date'
  ORDER BY rs.player_id, rs.game_id
) TO STDOUT WITH CSV HEADER;"""
            snapshot_query = """COPY (SELECT transaction_timestamp() AS transaction_timestamp_utc,
                current_setting('transaction_isolation') AS transaction_isolation,
                current_setting('transaction_read_only') AS transaction_read_only,
                pg_current_snapshot()::text AS postgres_snapshot) TO STDOUT WITH CSV HEADER;"""
            script.write_text("\n".join([
                "SET statement_timeout=0;",
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;",
                f"\\o {_psql_literal(feature_path)}",
                f"\\set slate_date {slate_date}",
                f"\\i {_psql_literal(Path(export_sql).resolve())}",
                f"\\o {_psql_literal(raw_path)}", raw_query,
                f"\\o {_psql_literal(target_path)}", target_query,
                f"\\o {_psql_literal(snapshot_path)}", snapshot_query,
                "\\o", "COMMIT;", "",
            ]))
            command = ["psql", "--no-psqlrc", "--pset", "pager=off", "-q",
                       "-v", "ON_ERROR_STOP=1", db_url,
                       "-v", f"slate_date={slate_date}", "-f", str(script)]
            process = subprocess.run(command, cwd=str(Path(export_sql).resolve().parents[3]),
                                     env=dict(psql_environment), capture_output=True, check=False)
            if feature_path.is_file():
                observed_feature_bytes = feature_path.read_bytes()
                feature_bytes = observed_feature_bytes or None
            if process.returncode != 0:
                detail = process.stderr.decode("utf-8", errors="replace")[-1200:]
                reason = f"PSQL_CAPTURE_EXIT_{process.returncode}:{redact_sensitive_text(detail)}"
                status = "EVIDENCE_CAPTURE_PARTIAL" if feature_bytes is not None else "EVIDENCE_CAPTURE_FAILED"
            elif not all(path.is_file() for path in (feature_path, raw_path, target_path, snapshot_path)):
                missing = [path.name for path in (feature_path, raw_path, target_path, snapshot_path) if not path.is_file()]
                reason = "SNAPSHOT_OUTPUT_MISSING:" + ",".join(missing)
                status = "EVIDENCE_CAPTURE_PARTIAL" if feature_bytes is not None else "EVIDENCE_CAPTURE_FAILED"
            else:
                raw_rows = pd.read_csv(raw_path)
                targets = pd.read_csv(target_path)
                snapshot = pd.read_csv(snapshot_path).iloc[0].to_dict()
                snapshot = {str(key): (value.item() if hasattr(value, "item") else value)
                            for key, value in snapshot.items()}
                package_path = write_evidence_package(
                    root=season_root, run_id=run_id, slate_date=slate_date,
                    feature_export_bytes=feature_bytes or b"", targets=targets,
                    raw_rows=raw_rows, acquired_at_utc=started,
                    source_coverage_complete=True,
                    same_snapshot_as_feature_export=True,
                    snapshot_metadata=snapshot,
                )
                package_manifest_sha = _refresh_package_manifest(package_path, {
                    "feature_export_sql_path": str(Path(export_sql).resolve()),
                    "feature_export_sql_sha256": sha256_file(Path(export_sql)),
                    "evidence_capture_code_sha256": sha256_file(Path(__file__).resolve()),
                    "feature_exported_at_utc": snapshot.get("transaction_timestamp_utc"),
                })
                manifest = json.loads((package_path / "manifest.json").read_text())
                status = ("EVIDENCE_CAPTURE_COMPLETE" if manifest.get("capture_status") == "COMPLETE"
                          else "EVIDENCE_CAPTURE_PARTIAL")
                reason = manifest.get("capture_error")
                return {
                    "status": status, "reason": reason, "path": str(package_path.resolve()),
                    "manifest_sha256": package_manifest_sha,
                    "feature_export_sha256": sha256_bytes(feature_bytes or b""),
                    "feature_export_bytes": feature_bytes,
                    "same_snapshot_as_feature_export": True,
                    "transaction_metadata": snapshot,
                    "acquired_at_utc": started,
                }
            # Persist whatever was produced under this run ID; never overwrite.
            package.parent.mkdir(parents=True, exist_ok=True)
            package.mkdir(parents=False, exist_ok=False)
            files: dict[str, bytes] = {}
            for source, name in ((feature_path, "exact_feature_export.csv"),
                                 (raw_path, "raw_prior_player_game_rows.csv"),
                                 (target_path, "target_bindings.csv"),
                                 (snapshot_path, "database_snapshot.csv")):
                if source.is_file():
                    files[name] = source.read_bytes()
            for name, content in files.items():
                (package / name).write_bytes(content)
            manifest = {"schema_version": "NHL_POINTS_PREGAME_TEAM_SHOT_EVIDENCE_V1",
                        "capture_status": status, "parent_daily_run_id": run_id,
                        "slate_date": slate_date, "acquired_at_utc": started,
                        "same_snapshot_as_feature_export": True,
                        "failure_reason": reason,
                        "files": {name: {"bytes": len(data), "sha256": sha256_bytes(data)}
                                  for name, data in sorted(files.items())}}
            manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
            (package / "manifest.json").write_bytes(manifest_bytes)
            sums = {**files, "manifest.json": manifest_bytes}
            (package / "SHA256SUMS").write_text("".join(
                f"{sha256_bytes(content)}  {name}\n" for name, content in sorted(sums.items())))
    except Exception as error:
        reason = redact_sensitive_text(f"{type(error).__name__}:{error}")
        status = "EVIDENCE_CAPTURE_PARTIAL" if feature_bytes is not None else "EVIDENCE_CAPTURE_FAILED"
        if not package.exists():
            package.parent.mkdir(parents=True, exist_ok=True)
            try:
                package.mkdir(parents=False, exist_ok=False)
            except FileExistsError:
                pass
        if package.exists() and not (package / "manifest.json").exists():
            if feature_bytes is not None and not (package / "exact_feature_export.csv").exists():
                (package / "exact_feature_export.csv").write_bytes(feature_bytes)
            manifest = {"schema_version": "NHL_POINTS_PREGAME_TEAM_SHOT_EVIDENCE_V1",
                        "capture_status": status, "parent_daily_run_id": run_id,
                        "slate_date": slate_date, "acquired_at_utc": started,
                        "same_snapshot_as_feature_export": feature_bytes is not None,
                        "failure_reason": reason}
            manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
            (package / "manifest.json").write_bytes(manifest_bytes)
            (package / "SHA256SUMS").write_text("".join(
                f"{sha256_file(path)}  {path.name}\n" for path in sorted(package.iterdir())
                if path.is_file() and path.name != "SHA256SUMS"))
    return {
        "status": status, "reason": reason,
        "path": str(package.resolve()) if package.exists() else None,
        "manifest_sha256": sha256_file(package / "SHA256SUMS") if (package / "SHA256SUMS").is_file() else None,
        "feature_export_sha256": sha256_bytes(feature_bytes) if feature_bytes is not None else None,
        "feature_export_bytes": feature_bytes,
        "same_snapshot_as_feature_export": feature_bytes is not None,
        "acquired_at_utc": started,
    }


def verify_evidence_package(directory: Path) -> dict:
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    for name, identity in manifest["files"].items():
        path = directory / name
        if path.stat().st_size != int(identity["bytes"]) or sha256_file(path) != identity["sha256"]:
            raise ValueError(f"EVIDENCE_FILE_HASH_MISMATCH:{name}")
    checksum_names: set[str] = set()
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        expected, name = line.split("  ", 1)
        if name in checksum_names:
            raise ValueError(f"EVIDENCE_DUPLICATE_CHECKSUM_ENTRY:{name}")
        checksum_names.add(name)
        if sha256_file(directory / name) != expected:
            raise ValueError(f"EVIDENCE_MANIFEST_HASH_MISMATCH:{name}")
    actual_names = {path.name for path in directory.iterdir() if path.is_file() and path.name != "SHA256SUMS"}
    if checksum_names != actual_names:
        raise ValueError("EVIDENCE_PACKAGE_FILE_SET_MISMATCH")
    if manifest.get("parent_daily_run_id") != directory.name.removeprefix("run_id="):
        raise ValueError("EVIDENCE_PACKAGE_RUN_ID_MISMATCH")
    return manifest


def bind_scorer_input(directory: Path, scorer_input_path: Path) -> dict[str, object]:
    """Retain and bind the final Phoenix scorer input in its run evidence package."""
    directory, scorer_input_path = Path(directory), Path(scorer_input_path)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    filename = "phoenix_scorer_input.csv"
    content = scorer_input_path.read_bytes()
    (directory / filename).write_bytes(content)
    manifest.setdefault("files", {})[filename] = {"bytes": len(content), "sha256": sha256_bytes(content)}
    manifest["phoenix_scorer_input_sha256"] = sha256_bytes(content)
    manifest["phoenix_scorer_input_bound_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    manifest_path.write_bytes(manifest_bytes)
    rows = []
    for path in sorted(directory.iterdir()):
        if path.is_file() and path.name != "SHA256SUMS":
            rows.append(f"{sha256_file(path)}  {path.name}\n")
    (directory / "SHA256SUMS").write_text("".join(rows))
    return {"path": str((directory / filename).resolve()), "sha256": sha256_bytes(content),
            "manifest_sha256": sha256_file(directory / "SHA256SUMS")}


def mark_capture_partial(directory: Path, reason: str) -> dict[str, object]:
    directory = Path(directory)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["capture_status"] = "PARTIAL"
    manifest["capture_error"] = redact_sensitive_text(reason)
    manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    manifest_path.write_bytes(manifest_bytes)
    (directory / "SHA256SUMS").write_text("".join(
        f"{sha256_file(path)}  {path.name}\n" for path in sorted(directory.iterdir())
        if path.is_file() and path.name != "SHA256SUMS"))
    return {"path": str(directory.resolve()), "manifest_sha256": sha256_file(directory / "SHA256SUMS")}


def attach_fallback_feature_export(directory: Path, feature_export_bytes: bytes, reason: str) -> dict[str, object]:
    """Retain the ordinary exporter result if the same-session wrapper failed.

    The manifest remains PARTIAL and explicitly disclaims a shared snapshot.
    """
    directory = Path(directory)
    filename = "exact_feature_export.csv"
    (directory / filename).write_bytes(feature_export_bytes)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["capture_status"] = "PARTIAL"
    manifest["capture_error"] = str(reason)
    manifest["same_snapshot_as_feature_export"] = False
    manifest["feature_export_sha256"] = sha256_bytes(feature_export_bytes)
    manifest.setdefault("files", {})[filename] = {
        "bytes": len(feature_export_bytes), "sha256": sha256_bytes(feature_export_bytes)}
    manifest_hash = _refresh_package_manifest(directory, manifest)
    return {"status": "EVIDENCE_CAPTURE_PARTIAL", "path": str(directory.resolve()),
            "manifest_sha256": manifest_hash,
            "feature_export_sha256": sha256_bytes(feature_export_bytes),
            "same_snapshot_as_feature_export": False}
