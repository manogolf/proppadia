"""Routine, nonblocking operational capture for the frozen Points HGB shadow."""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import psycopg
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score

from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.scripts import export_nhl_points_hgb_features as exporter
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = scorer.CONTRACT_PATH
MODEL = scorer.MODEL_PATH
FEATURE_CONTRACT = "NHL_POINTS_LEADER_CORE_FEATURES_V1"
HISTORY_CONTRACT = "120_DAY_LEGACY_BOUND"
HGB_AUTHORITY_LOCAL = "NHL_POINTS_COUNT_HGB_V1"
FEATURES = tuple(scorer.FEATURES)


def sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_hgb_capture(capture: Path, *, daily_run_root: Path | None = None) -> dict[str, Any]:
    """Validate immutable HGB capture files and their receipt bindings."""
    from backend.nhl.daily_capture import verify_package

    capture = Path(capture)
    verify_package(capture)
    receipt = json.loads((capture / "receipt.json").read_text())
    marker = json.loads((capture / "RUN_COMPLETE.json").read_text())
    identity = json.loads((capture / "model_identity.json").read_text())
    feature_contract = json.loads((capture / "feature_contract_identity.json").read_text())
    expected_parent = capture.parent.parent.name.removeprefix("run_id=")
    expected_slate = capture.parent.parent.parent.name.removeprefix("slate_date=")
    if receipt.get("status") != "COMPLETE" or marker.get("status") != "COMPLETE":
        raise RuntimeError("HGB_CAPTURE_NOT_COMPLETE")
    if (receipt.get("parent_run_id") != expected_parent
            or marker.get("parent_run_id") != expected_parent):
        raise RuntimeError("HGB_CAPTURE_PARENT_RUN_MISMATCH")
    if receipt.get("slate_date") != expected_slate:
        raise RuntimeError("HGB_CAPTURE_SLATE_DATE_MISMATCH")
    if receipt.get("research_model_id") != HGB_AUTHORITY_LOCAL or identity.get("research_model_id") != HGB_AUTHORITY_LOCAL:
        raise RuntimeError("HGB_CAPTURE_MODEL_IDENTITY_MISMATCH")
    if receipt.get("model_identity") != identity:
        raise RuntimeError("HGB_CAPTURE_MODEL_IDENTITY_BINDING_MISMATCH")
    if receipt.get("feature_sha256") != sha(capture / "features.csv"):
        raise RuntimeError("HGB_CAPTURE_FEATURE_HASH_MISMATCH")
    if receipt.get("prediction_sha256") != sha(capture / "predictions.csv"):
        raise RuntimeError("HGB_CAPTURE_PREDICTION_HASH_MISMATCH")
    if feature_contract.get("feature_contract") != identity.get("feature_contract"):
        raise RuntimeError("HGB_CAPTURE_FEATURE_CONTRACT_MISMATCH")
    if feature_contract.get("research_history_contract") != identity.get("history_contract"):
        raise RuntimeError("HGB_CAPTURE_HISTORY_CONTRACT_MISMATCH")
    if receipt.get("phoenix_control", {}).get("status") == "BOUND":
        phoenix = receipt["phoenix_control"]
        if (phoenix.get("parent_run_id") != expected_parent
                or phoenix.get("prediction_sha256") != sha(capture / "phoenix_control_predictions.csv")):
            raise RuntimeError("HGB_CAPTURE_PHOENIX_CONTROL_BINDING_MISMATCH")
    if daily_run_root is not None:
        parent_dir = Path(daily_run_root) / f"run_id={expected_parent}"
        verify_package(parent_dir)
        parent = json.loads((parent_dir / "parent_receipt.json").read_text())
        lane = (parent.get("lanes") or {}).get("points_hgb_shadow") or {}
        retained = next((item for item in lane.get("outputs", [])
                         if Path(str(item.get("capture_path", ""))).resolve() == capture.resolve()), None)
        capture_manifest_sha = sha(capture / "SHA256SUMS")
        if (parent.get("parent_daily_run_id") != expected_parent
                or parent.get("slate_date") != expected_slate
                or lane.get("status") != "COMPLETE"
                or retained is None
                or retained.get("manifest_sha256") != capture_manifest_sha
                or retained.get("prediction_sha256") != receipt.get("prediction_sha256")
                or retained.get("feature_sha256") != receipt.get("feature_sha256")
                or retained.get("model_identity") != identity
                or retained.get("deterministic_replay") != "DETERMINISTIC_REPLAY_PASS"
                or retained.get("coherence_crossing_count") != 0):
            raise RuntimeError("HGB_CAPTURE_PARENT_RECEIPT_BINDING_MISMATCH")
    return receipt


def prepare_authoritative_inputs(*, db_url: str, canonical_games: list[Any],
                                slate_date: str, phoenix_predictions: Path | None,
                                asof_utc: str, work_dir: Path) -> tuple[Path, list[Path], Path, dict[str, Any]]:
    """Build the HGB exporter inputs from official reconciliation and read-only logs."""
    from backend.nhl.postgame_learning import verify_reconciliation_package

    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    target_day = pd.Timestamp(slate_date)
    lower = target_day - pd.Timedelta(days=120)
    outcomes_root = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
    outcome_frames = []
    for outcome_path in sorted(outcomes_root.glob("????-??-??/reconciliation=*/canonical_skater_outcomes.csv")):
        try:
            package_date = pd.Timestamp(outcome_path.parts[-3])
        except (ValueError, IndexError):
            continue
        if not lower.date() <= package_date.date() < target_day.date():
            continue
        verify_reconciliation_package(outcome_path.parent, package_date.date().isoformat())
        frame = pd.read_csv(outcome_path)
        if "game_date" not in frame and "slate_date" not in frame:
            frame["slate_date"] = package_date.date().isoformat()
        outcome_frames.append(frame)
    if not outcome_frames:
        raise RuntimeError("HGB_OFFICIAL_HISTORY_UNAVAILABLE")
    outcomes = pd.concat(outcome_frames, ignore_index=True)
    date_col = "game_date" if "game_date" in outcomes else "slate_date"
    outcomes[date_col] = pd.to_datetime(outcomes[date_col]).dt.date.astype(str)
    outcomes = outcomes.loc[(outcomes[date_col] >= lower.date().isoformat()) &
                            (outcomes[date_col] < slate_date)].copy()
    if "official_final" in outcomes:
        outcomes = outcomes.loc[outcomes.official_final.astype(str).str.lower().isin({"true", "t", "1"})]
    if outcomes.duplicated(["game_id", "player_id"]).any():
        raise ValueError("HGB_OFFICIAL_OUTCOME_DUPLICATE_IDENTITY")
    outcomes_path = work_dir / "official_outcomes.csv"
    outcomes.to_csv(outcomes_path, index=False)
    game_ids = sorted(map(int, outcomes.game_id.unique()))
    if not game_ids:
        raise RuntimeError("HGB_OFFICIAL_HISTORY_EMPTY")

    # Confirm that every completed regular-season game in the bounded history has
    # an immutable outcome package before constructing features.
    with psycopg.connect(db_url, prepare_threshold=None) as conn, conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT g.game_id FROM nhl.games g
            WHERE substring(g.game_id::text,5,2) = '02' AND lower(g.status) = 'final'
              AND g.game_date >= %s AND g.game_date < %s
              AND g.start_time_utc < %s ORDER BY g.game_id""",
            (lower.date(), target_day.date(), asof_utc))
        expected_ids = {int(row[0]) for row in cur.fetchall()}
        if expected_ids - set(game_ids):
            raise RuntimeError("HGB_OFFICIAL_HISTORY_PACKAGE_GAP")
        cur.execute("""SELECT g.season AS canonical_season,g.game_date,l.game_id,l.player_id,
                   l.team_id,l.is_home,l.toi_minutes,l.pp_toi_minutes,l.shots_on_goal,l.shot_attempts
            FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING(game_id)
            WHERE l.game_id = ANY(%s) AND substring(g.game_id::text,5,2) = '02'
            ORDER BY g.game_date,g.start_time_utc,l.game_id,l.player_id""", (game_ids,))
        log_rows = cur.fetchall()
        log_columns = [column.name for column in cur.description]
        cur.execute("""SELECT DISTINCT ON (r.game_id,r.player_id)
                   r.game_id,r.player_id,r.team_id
            FROM nhl.roster_status r WHERE r.game_id = ANY(%s)
            ORDER BY r.game_id,r.player_id,r.asof_ts DESC""", (list(map(int, [g.game_id for g in canonical_games])),))
        roster_rows = cur.fetchall()
        roster_columns = [column.name for column in cur.description]
    logs = pd.DataFrame(log_rows, columns=log_columns)
    if logs.empty:
        raise RuntimeError("HGB_OFFICIAL_SKATER_LOGS_UNAVAILABLE")
    logs_path = work_dir / "official_skater_logs.csv"
    logs.to_csv(logs_path, index=False)

    game_frame = pd.DataFrame([{"game_id": int(g.game_id), "game_date": slate_date,
        "game_start_utc": g.start_time_utc, "home_team_id": int(g.home_team_id),
        "away_team_id": int(g.away_team_id)} for g in canonical_games])
    canonical_ids = sorted(map(int, game_frame.game_id.unique()))
    eligible_mask = pd.to_datetime(game_frame.game_start_utc, utc=True).gt(pd.Timestamp(asof_utc))
    eligible_game_ids = sorted(map(int, game_frame.loc[eligible_mask, "game_id"]))
    started_games = sorted(set(canonical_ids) - set(eligible_game_ids))
    if not eligible_game_ids:
        raise RuntimeError("HGB_ALL_CANONICAL_GAMES_ALREADY_STARTED")
    roster = pd.DataFrame(roster_rows, columns=roster_columns)
    if phoenix_predictions is not None and Path(phoenix_predictions).is_file():
        phoenix = pd.read_csv(phoenix_predictions)
        slate_identities = phoenix[["game_id", "player_id"]].drop_duplicates()
    else:
        # Production HGB can score from the canonical roster even if the
        # incumbent shadow scorer is unavailable. The shadow failure is
        # recorded independently by the caller.
        slate_identities = roster[["game_id", "player_id"]].drop_duplicates()
    slate = slate_identities.merge(roster, on=["game_id", "player_id"], how="left", validate="one_to_one")
    slate = slate.merge(game_frame, on="game_id", how="left", validate="many_to_one")
    eligible = pd.to_datetime(slate.game_start_utc, utc=True).gt(pd.Timestamp(asof_utc))
    slate = slate.loc[eligible].copy()
    if slate.empty:
        raise RuntimeError("HGB_ALL_CANONICAL_GAMES_ALREADY_STARTED")
    if slate.team_id.isna().any() or slate.game_start_utc.isna().any():
        raise RuntimeError("HGB_CANONICAL_SLATE_IDENTITY_UNAVAILABLE")
    slate["is_home"] = slate.team_id.astype(int).eq(slate.home_team_id.astype(int))
    slate_path = work_dir / "canonical_slate.csv"
    slate.to_csv(slate_path, index=False)
    return logs_path, [outcomes_path], slate_path, {
        "history_game_count": len(game_ids), "history_player_game_count": len(outcomes),
        "source_outcome_package_count": len(outcome_frames),
        "slate_identity_count": len(slate), "source_history_cutoff_exclusive": slate_date,
        "canonical_game_ids": canonical_ids,
        "canonical_game_count": len(canonical_ids),
        "canonical_game_set_hash": canonical_game_set_hash(canonical_ids),
        "eligible_pregame_game_ids": eligible_game_ids,
        "started_excluded_game_ids": started_games,
        "excluded_started_game_ids": started_games,
    }


def capture_from_files(
    *, logs_path: Path, outcomes_paths: list[Path], slate_path: Path,
    phoenix_predictions_path: Path | None, phoenix_prediction_sha256: str | None,
    phoenix_model_identity_sha256: str | None, phoenix_feature_contract_sha256: str | None,
    phoenix_feature_cutoff_utc: str | None, slate_date: str, season: int,
    capture_phase: str, parent_run_id: str, feature_cutoff_utc: str,
    output_root: Path, excluded_started_game_ids: list[int] | None = None,
    canonical_game_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Exercise exporter and scorer as production does, then retain a sealed capture."""
    phoenix_available = (phoenix_predictions_path is not None
                         and Path(phoenix_predictions_path).is_file()
                         and phoenix_prediction_sha256 is not None)
    if phoenix_available and sha(phoenix_predictions_path) != phoenix_prediction_sha256:
        raise ValueError("PHOENIX_CONTROL_HASH_MISMATCH")
    contract = json.loads(CONTRACT.read_text())
    if contract.get("research_model_id") != "NHL_POINTS_COUNT_HGB_V1":
        raise ValueError("HGB_MODEL_IDENTITY_MISMATCH")
    if contract.get("history_contract") != HISTORY_CONTRACT or contract.get("feature_columns") != list(FEATURES):
        raise ValueError("HGB_FROZEN_CONTRACT_MISMATCH")
    cutoff = pd.Timestamp(feature_cutoff_utc)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    output_root = Path(output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="nhl_points_hgb_export_") as temp:
        export_dir = Path(temp) / "export"
        command = [sys.executable, str(ROOT / "backend/nhl/scripts/export_nhl_points_hgb_features.py"),
                   "--logs", str(Path(logs_path).resolve())]
        command.append("--outcomes")
        command.extend(str(Path(outcome).resolve()) for outcome in outcomes_paths)
        command.extend(["--slate", str(Path(slate_path).resolve()), "--season", str(season),
                        "--cutoff-utc", cutoff.isoformat(), "--parent-run-id", parent_run_id,
                        "--out", str(export_dir)])
        try:
            subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as error:
            detail = (error.stdout or "") + (error.stderr or "")
            raise RuntimeError("HGB_FEATURE_EXPORT_FAILED:" + detail[-1200:]) from None
        source_features = export_dir / "features.csv"
        source_manifest = export_dir / "manifest.json"
        features = pd.read_csv(source_features)
        manifest = json.loads(source_manifest.read_text())
        if manifest.get("ordered_model_features") != list(FEATURES):
            raise ValueError("HGB_FEATURE_ORDER_MISMATCH")
        if len(features.columns.intersection(FEATURES)) != len(FEATURES):
            raise ValueError("HGB_FEATURE_SCHEMA_MISSING")
        if manifest.get("history_contract") != "NHL_POINTS_COUNT_HGB_V1_HISTORY_120_DAY_VALIDATED":
            raise ValueError("HGB_HISTORY_CONTRACT_IDENTITY_MISMATCH")
        if pd.to_datetime(features.game_start_utc, utc=True).le(cutoff).any():
            raise ValueError("NOT_STRICTLY_PREGAME")
        if not (pd.to_datetime(features.history_cutoff_exclusive).dt.date
                == pd.to_datetime(features.game_date).dt.date).all():
            raise ValueError("HGB_STRICT_PRIOR_CUTOFF_MISMATCH")
        if manifest.get("same_day_history_rows") != 0 or manifest.get("history_rows_after_cutoff") != 0:
            raise ValueError("HGB_HISTORY_LEAKAGE")
        keycols = ["game_id", "player_id"]
        if features.duplicated(keycols).any():
            raise ValueError("HGB_IDENTITY_VALIDATION_FAILED:DUPLICATE")
        hgb_keys = set(map(tuple, features[keycols].astype("int64").to_numpy()))
        phoenix = pd.read_csv(phoenix_predictions_path) if phoenix_available else pd.DataFrame()
        if phoenix_available:
            phoenix_keys = set(map(tuple, phoenix[keycols].drop_duplicates().astype("int64").to_numpy()))
            if hgb_keys != phoenix_keys:
                raise ValueError("HGB_PHOENIX_IDENTITY_SET_MISMATCH")
        game_hash = canonical_game_set_hash(features.game_id.unique())
        if game_hash != manifest.get("canonical_game_set_hash"):
            raise ValueError("HGB_CANONICAL_GAME_SET_MISMATCH")
        eligible_game_ids = sorted(map(int, features.game_id.unique()))
        excluded_ids = sorted(set(map(int, excluded_started_game_ids or [])))
        canonical_ids = sorted(set(map(int, canonical_game_ids or [])) or
                               (set(eligible_game_ids) | set(excluded_ids)))
        if (set(eligible_game_ids) & set(excluded_ids)
                or set(eligible_game_ids) | set(excluded_ids) != set(canonical_ids)):
            raise ValueError("HGB_CANONICAL_ELIGIBILITY_PARTITION_INVALID")
        full_game_hash = canonical_game_set_hash(canonical_ids)
        fitted = joblib.load(MODEL)
        scaler_sha = joblib.hash(fitted["scaler"]) if isinstance(fitted, dict) and "scaler" in fitted else None
        if not scaler_sha:
            raise ValueError("HGB_FROZEN_SCALER_IDENTITY_MISSING")
        first = scorer.score(features, fitted)
        crossings = int(((first.prob_over_0_5 < first.prob_over_1_5) |
                         (first.prob_over_1_5 < first.prob_over_2_5)).sum())
        if crossings:
            raise ValueError("COHERENCE_FAILED")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        capture = output_root / f"season={season}" / f"slate_date={slate_date}" / f"run_id={parent_run_id}" / f"phase={capture_phase}" / f"captured_at={stamp}"
        capture.mkdir(parents=True, exist_ok=False)
        feature_out = capture / "features.csv"
        prediction_out = capture / "predictions.csv"
        phoenix_out = capture / "phoenix_control_predictions.csv"
        shutil.copyfile(source_features, feature_out)
        first.to_csv(prediction_out, index=False, float_format="%.17g")
        if phoenix_available:
            shutil.copyfile(phoenix_predictions_path, phoenix_out)
        # Deterministic replay uses only the retained feature artifact and fitted model.
        replay = scorer.score(pd.read_csv(feature_out), fitted)
        with tempfile.TemporaryDirectory(prefix="nhl_points_hgb_replay_") as replay_dir:
            replay_path = Path(replay_dir) / "predictions.csv"
            replay.to_csv(replay_path, index=False, float_format="%.17g")
            replay_sha = sha(replay_path)
            deterministic = replay_sha == sha(prediction_out)
        if not deterministic:
            raise ValueError("DETERMINISTIC_REPLAY_FAILED")
        identity = {
            "research_model_id": contract["research_model_id"],
            "model_identity_sha256": contract["model_identity_sha256"],
            "model_artifact_sha256": sha(MODEL),
            "scaler_identity": {"type":"sklearn.preprocessing.StandardScaler", "joblib_hash":scaler_sha},
            "hgb_artifact_identity": contract.get("fitted_artifact_sha256"),
            "scorer_sha256": sha(ROOT / "backend/nhl/scripts/score_nhl_points_hgb_shadow.py"),
            "feature_exporter_sha256": sha(ROOT / "backend/nhl/scripts/export_nhl_points_hgb_features.py"),
            "model_contract_sha256": sha(CONTRACT),
            "feature_contract": FEATURE_CONTRACT,
            "feature_contract_sha256": sha(ROOT / "artifacts/analysis/nhl/points_hgb_operational_bridge/2026-10-09/operational_feature_contract_v2.json"),
            "feature_columns": list(FEATURES), "history_contract": HISTORY_CONTRACT,
            "history_contract_identity_sha256": hashlib.sha256(
                json.dumps({"contract": HISTORY_CONTRACT, "strict_prior": True,
                            "lower_bound_days": 120, "last_player_games": 10}, sort_keys=True).encode()).hexdigest(),
            "poisson_threshold_derivation": contract["thresholds"],
            "poisson_threshold_derivation_identity": hashlib.sha256(json.dumps(contract["thresholds"], sort_keys=True).encode()).hexdigest(),
        }
        phoenix_control = ({"status": "BOUND", "parent_run_id": parent_run_id,
                "prediction_sha256": phoenix_prediction_sha256,
                "fitted_model_identity_sha256": phoenix_model_identity_sha256,
                "feature_contract_sha256": phoenix_feature_contract_sha256,
                "prediction_row_count": int(len(phoenix)), "feature_cutoff_utc": phoenix_feature_cutoff_utc,
                "prediction_path": "phoenix_control_predictions.csv", "file_sha256": sha(phoenix_out)}
            if phoenix_available else {"status": "UNAVAILABLE_PHOENIX_SHADOW_FAILED"})
        body = {
            "schema_version": "NHL_POINTS_HGB_ROUTINE_SHADOW_CAPTURE_V1",
            "status": "COMPLETE", "research_model_id": contract["research_model_id"],
            "production_authority": False, "blocking": False,
            "slate_date": slate_date, "season": season, "capture_phase": capture_phase,
            "parent_run_id": parent_run_id, "canonical_game_set_hash": full_game_hash,
            "canonical_game_ids": canonical_ids, "canonical_game_count": len(canonical_ids),
            "eligible_pregame_game_ids": eligible_game_ids,
            "eligible_pregame_game_count": len(eligible_game_ids),
            "eligible_game_set_hash": game_hash,
            "started_excluded_game_ids": excluded_ids,
            "started_excluded_game_count": len(excluded_ids),
            "excluded_started_game_ids": excluded_ids,
            "feature_cutoff_utc": cutoff.isoformat(), "capture_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "feature_path": "features.csv", "feature_sha256": sha(feature_out),
            "prediction_path": "predictions.csv", "prediction_sha256": sha(prediction_out),
            "identity_count": int(features[keycols].drop_duplicates().shape[0]),
            "prediction_count": int(len(first)), "coherence_crossing_count": crossings,
            "deterministic_replay": "DETERMINISTIC_REPLAY_PASS", "replay_prediction_sha256": replay_sha,
            "model_identity": identity,
            "phoenix_control": phoenix_control,
        }
        (capture / "model_identity.json").write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")
        contract_identity = {
            "feature_contract": FEATURE_CONTRACT,
            "feature_contract_sha256": identity["feature_contract_sha256"],
            "feature_columns_in_order": list(FEATURES),
            "research_history_contract": HISTORY_CONTRACT,
            "operational_history_contract": "NHL_POINTS_COUNT_HGB_V1_HISTORY_120_DAY_VALIDATED",
            "operational_feature_contract_sha256": sha(ROOT / "artifacts/analysis/nhl/points_hgb_operational_bridge/2026-10-09/operational_feature_contract_v2.json"),
        }
        (capture / "feature_contract_identity.json").write_text(json.dumps(contract_identity, indent=2, sort_keys=True) + "\n")
        (capture / "receipt.json").write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
        marker = {"status": "COMPLETE", "parent_run_id": parent_run_id,
                  "prediction_sha256": body["prediction_sha256"]}
        (capture / "RUN_COMPLETE.json").write_text(json.dumps(marker, indent=2, sort_keys=True) + "\n")
        names = ["features.csv", "predictions.csv", "model_identity.json",
                 "feature_contract_identity.json", "receipt.json", "RUN_COMPLETE.json"]
        if phoenix_available:
            names.append("phoenix_control_predictions.csv")
        (capture / "SHA256SUMS").write_text("".join(f"{sha(capture / name)}  {name}\n" for name in names))
        body["capture_path"] = str(capture)
        body["manifest_sha256"] = sha(capture / "SHA256SUMS")
        body["receipt_sha256"] = sha(capture / "receipt.json")
        body["feature_artifact_sha256"] = body["feature_sha256"]
        body["prediction_artifact_sha256"] = body["prediction_sha256"]
        return body


def build_production_prediction_artifact(*, capture_path: Path, output_path: Path,
                                         canonical_games: list[Any], slate_date: str,
                                         parent_run_id: str, feature_cutoff_utc: str,
                                         canonical_game_set_sha256: str) -> dict[str, Any]:
    """Bind retained HGB probabilities to the normal Points line-grain contract."""
    from backend.nhl.prediction_lineage import build_canonical_game_map, pregame_game_eligibility

    capture_path = Path(capture_path)
    capture_receipt = json.loads((capture_path / "receipt.json").read_text())
    verify_features = pd.read_csv(capture_path / "features.csv")
    predictions = pd.read_csv(capture_path / "predictions.csv")
    key = ["game_id", "player_id"]
    if verify_features.duplicated(key).any() or predictions.duplicated(key).any():
        raise ValueError("HGB_PRODUCTION_IDENTITY_DUPLICATE")
    features = verify_features.merge(predictions, on=key, how="outer", validate="one_to_one",
                                     suffixes=("", "_prediction"), indicator=True)
    if not features._merge.eq("both").all():
        raise ValueError("HGB_PRODUCTION_FEATURE_PREDICTION_IDENTITY_MISMATCH")
    canonical = build_canonical_game_map(canonical_games, slate=slate_date,
                                         expected_game_set_hash=canonical_game_set_sha256)
    eligibility = pregame_game_eligibility(canonical_games, feature_cutoff_utc)
    eligible_ids = set(eligibility["eligible_pregame_game_ids"])
    excluded_ids = set(eligibility["started_excluded_game_ids"])
    feature_game_ids = set(features.game_id.astype(int))
    if feature_game_ids - set(canonical):
        raise ValueError("HGB_PRODUCTION_NONCANONICAL_GAME")
    if feature_game_ids & excluded_ids:
        raise ValueError("STARTED_GAME_PRESENT_IN_HGB_PRODUCTION_OUTPUT")
    if feature_game_ids != eligible_ids:
        raise ValueError("HGB_PRODUCTION_ELIGIBLE_GAME_COVERAGE_MISMATCH")
    if capture_receipt.get("parent_run_id") != parent_run_id:
        raise ValueError("HGB_PRODUCTION_CAPTURE_PARENT_MISMATCH")
    if capture_receipt.get("canonical_game_ids") is not None and sorted(map(
            int, capture_receipt["canonical_game_ids"])) != sorted(canonical):
        raise ValueError("HGB_PRODUCTION_CAPTURE_CANONICAL_GAME_MISMATCH")
    if capture_receipt.get("canonical_game_set_hash") not in (None, canonical_game_set_sha256):
        raise ValueError("HGB_PRODUCTION_CAPTURE_CANONICAL_HASH_MISMATCH")
    if capture_receipt.get("eligible_pregame_game_ids") is not None and sorted(map(
            int, capture_receipt["eligible_pregame_game_ids"])) != sorted(eligible_ids):
        raise ValueError("HGB_PRODUCTION_CAPTURE_ELIGIBLE_GAME_MISMATCH")
    if capture_receipt.get("started_excluded_game_ids") is not None and sorted(map(
            int, capture_receipt["started_excluded_game_ids"])) != sorted(excluded_ids):
        raise ValueError("HGB_PRODUCTION_CAPTURE_EXCLUDED_GAME_MISMATCH")
    if int(capture_receipt.get("identity_count", len(features))) != len(features):
        raise ValueError("HGB_PRODUCTION_CAPTURE_IDENTITY_COUNT_MISMATCH")
    if (capture_receipt.get("feature_sha256") is not None
            and capture_receipt["feature_sha256"] != sha(capture_path / "features.csv")):
        raise ValueError("HGB_PRODUCTION_CAPTURE_FEATURE_HASH_MISMATCH")
    if (capture_receipt.get("prediction_sha256") is not None
            and capture_receipt["prediction_sha256"] != sha(capture_path / "predictions.csv")):
        raise ValueError("HGB_PRODUCTION_CAPTURE_PREDICTION_HASH_MISMATCH")
    if int(capture_receipt.get("prediction_count", len(predictions))) != len(predictions):
        raise ValueError("HGB_PRODUCTION_CAPTURE_PREDICTION_COUNT_MISMATCH")
    starts = pd.to_datetime(features.game_start_utc, utc=True, errors="coerce")
    cutoff = pd.Timestamp(feature_cutoff_utc)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    if starts.isna().any() or not (starts > cutoff).all():
        raise ValueError("HGB_PRODUCTION_NOT_STRICTLY_PREGAME")
    if not features.game_date.astype(str).eq(slate_date).all():
        raise ValueError("HGB_PRODUCTION_SLATE_DATE_MISMATCH")

    phoenix_shadow = {"status": "UNAVAILABLE_PHOENIX_SHADOW_FAILED"}
    if capture_receipt.get("phoenix_control", {}).get("status") == "BOUND":
        phoenix_path = capture_path / "phoenix_control_predictions.csv"
        if not phoenix_path.is_file():
            raise ValueError("HGB_PRODUCTION_PHOENIX_CONTROL_MISSING")
        phoenix = pd.read_csv(phoenix_path)
        key = ["game_id", "player_id"]
        if not set(key + ["line", "prob_over"]).issubset(phoenix.columns):
            raise ValueError("HGB_PRODUCTION_PHOENIX_CONTROL_SCHEMA_MISSING")
        if phoenix.duplicated(key + ["line"]).any():
            raise ValueError("HGB_PRODUCTION_PHOENIX_CONTROL_DUPLICATE_IDENTITY")
        phoenix_game_ids = set(pd.to_numeric(phoenix.game_id, errors="raise").astype(int))
        if phoenix_game_ids & excluded_ids:
            raise ValueError("STARTED_GAME_PRESENT_IN_PHOENIX_CONTROL_OUTPUT")
        phoenix_keys = set(map(tuple, phoenix[key].drop_duplicates().astype("int64").to_numpy()))
        hgb_keys = set(map(tuple, features[key].astype("int64").to_numpy()))
        if phoenix_keys != hgb_keys or phoenix_game_ids != eligible_ids:
            raise ValueError("HGB_PRODUCTION_PHOENIX_ELIGIBLE_IDENTITY_MISMATCH")
        line_counts = phoenix.groupby(key).line.agg(list)
        expected_lines = {0.5, 1.5, 2.5}
        if any(set(map(float, values)) != expected_lines or len(values) != 3
               for values in line_counts):
            raise ValueError("HGB_PRODUCTION_PHOENIX_LINE_POPULATION_MISMATCH")
        control = capture_receipt["phoenix_control"]
        if control.get("parent_run_id") != parent_run_id:
            raise ValueError("HGB_PRODUCTION_PHOENIX_PARENT_MISMATCH")
        if control.get("prediction_sha256") != sha(phoenix_path):
            raise ValueError("HGB_PRODUCTION_PHOENIX_HASH_MISMATCH")
        phoenix_shadow = {
            "status": "COMPLETE", "model": "PHOENIX_POINTS_INCUMBENT_SHADOW",
            "model_family": "phoenix", "model_version": "phoenix_v2",
            "path": str(phoenix_path.resolve()), "sha256": sha(phoenix_path),
            "prediction_count": len(phoenix), "identity_count": len(phoenix_keys),
            "eligible_pregame_game_ids": sorted(phoenix_game_ids),
            "started_excluded_game_ids": sorted(excluded_ids),
            "fitted_model_identity_sha256": control.get("fitted_model_identity_sha256"),
            "feature_contract_sha256": control.get("feature_contract_sha256"),
        }
    rows = []
    for row in features.itertuples(index=False):
        game_id, player_id = int(row.game_id), int(row.player_id)
        game = canonical[game_id]
        for line, probability in ((0.5, row.prob_over_0_5),
                                  (1.5, row.prob_over_1_5),
                                  (2.5, row.prob_over_2_5)):
            rows.append({
                "game_id": game_id, "player_id": player_id, "line": line,
                "prob_over": float(probability), "expected_points": float(row.expected_points),
                "game_date": slate_date, "game_start_utc": game.game_start_utc,
                "home_team_id": game.home_team_id, "away_team_id": game.away_team_id,
                "parent_daily_run_id": parent_run_id,
                "feature_input_cutoff_utc": cutoff.isoformat().replace("+00:00", "Z"),
                "canonical_game_set_hash": canonical_game_set_sha256,
            })
    output = pd.DataFrame(rows)
    if output.empty or output.duplicated(["game_date", "game_id", "player_id", "line"]).any():
        raise ValueError("HGB_PRODUCTION_NATURAL_IDENTITY_INVALID")
    if set(output.line.astype(float)) != {0.5, 1.5, 2.5}:
        raise ValueError("HGB_PRODUCTION_LINE_SET_INVALID")
    for _, ladder in output.groupby(["game_id", "player_id"], sort=False):
        ladder = ladder.sort_values("line")
        values = ladder.prob_over.astype(float).to_numpy()
        if len(values) != 3 or not (values[0] >= values[1] >= values[2]):
            raise ValueError("HGB_PRODUCTION_COHERENCE_FAILED")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_path, index=False, float_format="%.17g")
    return {
        "path": str(output_path.resolve()), "sha256": sha(output_path),
        "bytes": output_path.stat().st_size, "row_count": len(output),
        "identity_count": int(output[key].drop_duplicates().shape[0]),
        "canonical_game_count": len(canonical),
        "canonical_game_set_hash": canonical_game_set_sha256,
        "canonical_game_ids": sorted(canonical),
        "eligible_pregame_game_count": len(eligible_ids),
        "eligible_pregame_game_ids": sorted(eligible_ids),
        "started_excluded_game_count": len(excluded_ids),
        "started_excluded_game_ids": sorted(excluded_ids),
        "started_exclusion_reason": "GAME_ALREADY_STARTED" if excluded_ids else None,
        "line_count": 3,
        "parent_daily_run_id": parent_run_id,
        "feature_input_cutoff_utc": cutoff.isoformat().replace("+00:00", "Z"),
        "source_hgb_capture_path": str(capture_path.resolve()),
        "source_hgb_capture_prediction_sha256": capture_receipt["prediction_sha256"],
        "phoenix_shadow": phoenix_shadow,
        "coherence_crossing_count": 0,
        "validated_prediction_identity": True,
    }


def discover_daily_points_authority(*, slate_date: str,
                                    daily_run_root: Path | None = None) -> dict[str, Any] | None:
    """Read the exact production/shadow Points identities from a daily receipt."""
    from backend.nhl.daily_capture import verify_package

    root = Path(daily_run_root or ROOT / "artifacts/operational/nhl/daily_runs")
    matches = []
    for receipt_path in root.glob("run_id=*/parent_receipt.json"):
        try:
            receipt = json.loads(receipt_path.read_text())
            if receipt.get("slate_date") != slate_date:
                continue
            verify_package(receipt_path.parent)
            points_lane = (receipt.get("lanes") or {}).get("points") or {}
            if points_lane.get("status") != "COMPLETE":
                continue
            selection = next((item for item in points_lane.get("inputs", [])
                              if item.get("production_authority")), {})
            production = str(selection.get("production_authority") or "phoenix_v2")
            identity = next((item for item in points_lane.get("outputs", [])
                             if str(item.get("path", "")).endswith("points_predictions.csv")), None)
            if not identity or not Path(identity.get("path", "")).is_file():
                continue
            if sha(Path(identity["path"])) != identity.get("sha256"):
                continue
            evidence = identity.get("fitted_model_evidence") or {}
            if evidence.get("prediction_artifact_sha256") != identity.get("sha256"):
                continue
            shadow = identity.get("phoenix_shadow")
            if production == "phoenix_v2":
                hgb_lane = (receipt.get("lanes") or {}).get("points_hgb_shadow") or {}
                hgb_output = next((item for item in hgb_lane.get("outputs", [])
                                  if item.get("capture_path")), None)
                shadow = ({"status": "COMPLETE", "model": HGB_AUTHORITY_LOCAL,
                           "capture_path": hgb_output.get("capture_path"),
                           "prediction_sha256": hgb_output.get("prediction_sha256")}
                          if hgb_output else {"status": hgb_lane.get("status", "UNAVAILABLE"),
                                             "model": "NHL_POINTS_COUNT_HGB_V1"})
            matches.append((str(receipt.get("ended_at_utc") or ""), {
                "status": "BOUND", "parent_daily_run_id": receipt.get("parent_daily_run_id"),
                "daily_receipt_path": str(receipt_path.resolve()),
                "daily_receipt_manifest_sha256": sha(receipt_path.parent / "SHA256SUMS"),
                "authority_version": selection.get("authority_version"),
                "production": {"model": production,
                    "model_family": evidence.get("model_family", identity.get("model_family")),
                    "model_version": evidence.get("model_version", identity.get("model_version")),
                    "prediction_path": str(Path(identity["path"]).resolve()),
                    "prediction_sha256": identity["sha256"],
                    "fitted_model_identity_sha256": evidence.get("fitted_model_identity_sha256"),
                    "feature_contract_sha256": (evidence.get("scoring_configuration") or {}).get(
                        "feature_contract_sha256"),
                    "history_contract": (evidence.get("scoring_configuration") or {}).get(
                        "history_contract"),
                    "source_hgb_capture_path": identity.get("source_hgb_capture_path"),
                    "source_hgb_capture_prediction_sha256": identity.get(
                        "source_hgb_capture_prediction_sha256")},
                "incumbent_shadow": shadow,
            }))
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue
    return max(matches, key=lambda item: item[0])[1] if matches else None


def grade_prior_hgb_capture(*, slate_date: str, reconciliation_package: Path,
                            shadow_root: Path | None = None,
                            daily_run_root: Path | None = None) -> dict[str, Any]:
    """Grade the highest available pregame phase without rebuilding predictions."""
    from backend.nhl.daily_capture import verify_package
    from backend.nhl.scripts.grade_nhl_points_hgb_shadow import grade
    from backend.nhl.postgame_learning import verify_reconciliation_package

    shadow_root = Path(shadow_root or ROOT / "artifacts/operational/nhl/points_hgb_shadow")
    daily_run_root = Path(daily_run_root or ROOT / "artifacts/operational/nhl/daily_runs")
    year, month = int(slate_date[:4]), int(slate_date[5:7])
    season = year if month >= 9 else year - 1
    root = shadow_root / f"season={season}" / f"slate_date={slate_date}"
    candidates = []
    phase_order = {"EARLY": 0, "REFRESH": 1, "FINAL_PREGAME": 2}
    for receipt_path in root.glob("run_id=*/phase=*/captured_at=*/receipt.json"):
        capture = receipt_path.parent
        try:
            receipt = validate_hgb_capture(capture, daily_run_root=daily_run_root)
            if receipt.get("slate_date") != slate_date:
                continue
            candidates.append((phase_order.get(str(receipt.get("capture_phase", "")).upper(), -1),
                               str(receipt.get("capture_timestamp_utc", "")), capture, receipt))
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            continue
    if not candidates:
        return {"status": "NOT_AVAILABLE", "reason": "NO_VALID_IMMUTABLE_HGB_CAPTURE"}
    _, _, capture, receipt = max(candidates, key=lambda row: (row[0], row[1]))
    daily_authority = discover_daily_points_authority(
        slate_date=slate_date, daily_run_root=daily_run_root)
    if (daily_authority and daily_authority.get("parent_daily_run_id")
            != receipt.get("parent_run_id")):
        daily_authority = None
    outcomes_path = Path(reconciliation_package) / "canonical_skater_outcomes.csv"
    verify_reconciliation_package(Path(reconciliation_package), slate_date)
    outcomes = pd.read_csv(outcomes_path)
    pred = pd.read_csv(capture / "predictions.csv")
    production_prediction_sha256 = None
    production_prediction_path = None
    hgb_is_production = bool(daily_authority and
                             daily_authority["production"].get("model") == "NHL_POINTS_COUNT_HGB_V1")
    if hgb_is_production:
        if (Path(daily_authority["production"].get("source_hgb_capture_path", "")).resolve()
                != capture.resolve()
                or daily_authority["production"].get("source_hgb_capture_prediction_sha256")
                != receipt.get("prediction_sha256")):
            raise RuntimeError("HGB_PRODUCTION_CAPTURE_AUTHORITY_MISMATCH")
        production_prediction_path = Path(daily_authority["production"]["prediction_path"])
        production_prediction_sha256 = daily_authority["production"]["prediction_sha256"]
        if sha(production_prediction_path) != production_prediction_sha256:
            raise RuntimeError("HGB_PRODUCTION_PREDICTION_HASH_MISMATCH")
        production_rows = pd.read_csv(production_prediction_path)
        required = {"game_id", "player_id", "line", "prob_over", "expected_points"}
        if not required.issubset(production_rows.columns):
            raise RuntimeError("HGB_PRODUCTION_PREDICTION_SCHEMA_MISSING")
        if production_rows.duplicated(["game_id", "player_id", "line"]).any():
            raise RuntimeError("HGB_PRODUCTION_PREDICTION_DUPLICATE_IDENTITY")
        prod_wide = production_rows.pivot(index=["game_id", "player_id"],
                                          columns="line", values="prob_over").reset_index()
        means = production_rows.groupby(["game_id", "player_id"], as_index=False).expected_points.nunique()
        if means.expected_points.gt(1).any():
            raise RuntimeError("HGB_PRODUCTION_EXPECTED_COUNT_LINE_MISMATCH")
        expected_count = production_rows.groupby(["game_id", "player_id"], as_index=False).expected_points.first()
        pred = prod_wide.merge(expected_count, on=["game_id", "player_id"], validate="one_to_one")
        pred = pred.rename(columns={0.5: "prob_over_0_5", 1.5: "prob_over_1_5", 2.5: "prob_over_2_5"})
        expected_columns = {"prob_over_0_5", "prob_over_1_5", "prob_over_2_5"}
        if not expected_columns.issubset(pred.columns):
            raise RuntimeError("HGB_PRODUCTION_PREDICTION_LINE_SET_INVALID")
    game_ids = set(map(int, receipt["canonical_game_ids"]))
    outcomes = outcomes.loc[outcomes.game_id.astype(int).isin(game_ids)].copy()
    if outcomes.empty or not outcomes.get("official_final", pd.Series(True, index=outcomes.index)).astype(str).str.lower().isin({"true", "t", "1"}).all():
        return {"status": "OUTCOMES_NOT_FINAL", "capture_path": str(capture)}
    report = grade(pred, outcomes)
    phoenix_thresholds = {}
    if receipt.get("phoenix_control", {}).get("status") == "BOUND":
        phoenix = pd.read_csv(capture / "phoenix_control_predictions.csv")
        phoenix = phoenix.loc[phoenix.game_id.astype(int).isin(game_ids)]
        phoenix_lines = phoenix.pivot(index=["game_id", "player_id"], columns="line", values="prob_over").reset_index()
        phoenix_lines = phoenix_lines.rename(columns={0.5: "prob_over_0_5", 1.5: "prob_over_1_5", 2.5: "prob_over_2_5"})
        scored_outcomes = outcomes.merge(phoenix_lines, on=["game_id", "player_id"], how="inner", validate="one_to_one")
        y_source = (pd.to_numeric(scored_outcomes.official_points, errors="coerce")
                    if "official_points" in scored_outcomes else
                    pd.to_numeric(scored_outcomes.official_goals, errors="coerce") +
                    pd.to_numeric(scored_outcomes.official_assists, errors="coerce"))
        final = scored_outcomes.official_final.astype(str).str.lower().isin({"true", "t", "1"}) if "official_final" in scored_outcomes else pd.Series(True,index=scored_outcomes.index)
        participated = scored_outcomes.participation_state.eq("PARTICIPATED") if "participation_state" in scored_outcomes else pd.Series(True,index=scored_outcomes.index)
        valid = final & participated & y_source.notna()
        for threshold, col in zip((1, 2, 3), ("prob_over_0_5", "prob_over_1_5", "prob_over_2_5")):
            y = (y_source[valid].astype(int) >= threshold).astype(int).to_numpy()
            p = np.clip(scored_outcomes.loc[valid, col].astype(float).to_numpy(), 1e-12, 1 - 1e-12)
            phoenix_thresholds[col] = {"n": len(y), "log_loss": float(log_loss(y, p, labels=[0, 1])),
                "brier": float(brier_score_loss(y, p)),
                "auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else None,
                "average_precision": float(average_precision_score(y, p)) if y.sum() else None,
                "base_rate": float(y.mean()) if len(y) else None}
    outcome_sha = sha(outcomes_path)
    capture_manifest_sha = verify_package(capture)
    input_identity = hashlib.sha256(json.dumps({"capture_manifest_sha256":capture_manifest_sha,
        "prediction_sha256":production_prediction_sha256 or receipt["prediction_sha256"],
        "outcomes_sha256":outcome_sha,
        "game_ids":sorted(game_ids)}, sort_keys=True).encode()).hexdigest()
    grade_root = shadow_root / "grades" / f"season={season}" / f"slate_date={slate_date}" / f"capture={capture.name}" / f"grade={input_identity[:20]}"
    # A prior failed grade may have created its destination before finishing.
    # Recover only a truly empty directory; populated packages remain create-only.
    if grade_root.is_dir() and not any(grade_root.iterdir()):
        grade_root.rmdir()
    elif grade_root.is_dir():
        verify_package(grade_root)
        existing = json.loads((grade_root / "grade.json").read_text())
        if (existing.get("status") != "COMPLETE"
                or existing.get("official_outcome_identity") != input_identity
                or existing.get("hgb_prediction_sha256") != receipt["prediction_sha256"]):
            raise RuntimeError("HGB_GRADE_PACKAGE_IDENTITY_MISMATCH")
        existing["grade_path"] = str(grade_root)
        existing["grade_sha256"] = sha(grade_root / "grade.json")
        return existing
    grade_root.mkdir(parents=True, exist_ok=False)
    result = {"schema_version":"NHL_POINTS_HGB_SHADOW_GRADE_V1", "status":"COMPLETE",
        "slate_date":slate_date, "capture_path":str(capture), "capture_manifest_sha256":capture_manifest_sha,
        "hgb_prediction_sha256":receipt["prediction_sha256"], "model_identity":receipt["model_identity"],
        "evaluation_role":"PRODUCTION" if hgb_is_production else "SHADOW",
        "production_authority_context":daily_authority,
        "production_prediction_path":str(production_prediction_path.resolve()) if production_prediction_path else None,
        "production_prediction_sha256":production_prediction_sha256,
        "feature_contract_identity":json.loads((capture/"feature_contract_identity.json").read_text()),
        "official_outcomes_status":"FINAL", "reconciliation_package":str(Path(reconciliation_package).resolve()),
        # Reconciliation packages deliberately keep RUN_COMPLETE.json outside
        # SHA256SUMS; validate them with their own contract, then bind the
        # manifest bytes. The daily observation verifier requires exact file
        # set equality and is only appropriate for HGB capture packages.
        "reconciliation_manifest_sha256":sha(Path(reconciliation_package) / "SHA256SUMS"),
        "official_outcome_path":str(outcomes_path), "official_outcome_sha256":outcome_sha,
        "official_outcome_identity":input_identity, "hgb_metrics":report,
        "phoenix_same_capture_threshold_metrics":phoenix_thresholds,
        "phoenix_prediction_sha256":receipt.get("phoenix_control", {}).get("prediction_sha256"),
        "comparison_type":"DESCRIPTIVE_SAME_CAPTURE_CONTROL", "promotion_decision_from_single_slate":False}
    (grade_root / "grade.json").write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
    (grade_root / "RUN_COMPLETE.json").write_text(json.dumps({"status":"COMPLETE",
        "official_outcome_identity":input_identity}, indent=2, sort_keys=True)+"\n")
    sums = f"{sha(grade_root/'grade.json')}  grade.json\n{sha(grade_root/'RUN_COMPLETE.json')}  RUN_COMPLETE.json\n"
    (grade_root / "SHA256SUMS").write_text(sums)
    result["grade_path"] = str(grade_root)
    result["grade_sha256"] = sha(grade_root/"grade.json")
    return result
