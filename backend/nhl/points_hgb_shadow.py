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
FEATURES = tuple(scorer.FEATURES)


def sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prepare_authoritative_inputs(*, db_url: str, canonical_games: list[Any],
                                slate_date: str, phoenix_predictions: Path,
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

    phoenix = pd.read_csv(phoenix_predictions)
    game_frame = pd.DataFrame([{"game_id": int(g.game_id), "game_date": slate_date,
        "game_start_utc": g.start_time_utc, "home_team_id": int(g.home_team_id),
        "away_team_id": int(g.away_team_id)} for g in canonical_games])
    roster = pd.DataFrame(roster_rows, columns=roster_columns)
    phoenix_identities = phoenix[["game_id", "player_id"]].drop_duplicates()
    slate = phoenix_identities.merge(roster, on=["game_id", "player_id"], how="left", validate="one_to_one")
    slate = slate.merge(game_frame, on="game_id", how="left", validate="many_to_one")
    eligible = pd.to_datetime(slate.game_start_utc, utc=True).gt(pd.Timestamp(asof_utc))
    started_games = sorted(map(int, slate.loc[~eligible, "game_id"].unique()))
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
        "excluded_started_game_ids": started_games,
    }


def capture_from_files(
    *, logs_path: Path, outcomes_paths: list[Path], slate_path: Path,
    phoenix_predictions_path: Path, phoenix_prediction_sha256: str,
    phoenix_model_identity_sha256: str, phoenix_feature_contract_sha256: str,
    phoenix_feature_cutoff_utc: str, slate_date: str, season: int,
    capture_phase: str, parent_run_id: str, feature_cutoff_utc: str,
    output_root: Path, excluded_started_game_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Exercise exporter and scorer as production does, then retain a sealed capture."""
    if sha(phoenix_predictions_path) != phoenix_prediction_sha256:
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
        phoenix = pd.read_csv(phoenix_predictions_path)
        keycols = ["game_id", "player_id"]
        if features.duplicated(keycols).any():
            raise ValueError("HGB_IDENTITY_VALIDATION_FAILED:DUPLICATE")
        hgb_keys = set(map(tuple, features[keycols].astype("int64").to_numpy()))
        phoenix_keys = set(map(tuple, phoenix[keycols].drop_duplicates().astype("int64").to_numpy()))
        if not hgb_keys.issubset(phoenix_keys):
            raise ValueError("HGB_PHOENIX_IDENTITY_SET_MISMATCH")
        game_hash = canonical_game_set_hash(features.game_id.unique())
        if game_hash != manifest.get("canonical_game_set_hash"):
            raise ValueError("HGB_CANONICAL_GAME_SET_MISMATCH")
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
        body = {
            "schema_version": "NHL_POINTS_HGB_ROUTINE_SHADOW_CAPTURE_V1",
            "status": "COMPLETE", "research_model_id": contract["research_model_id"],
            "production_authority": False, "blocking": False,
            "slate_date": slate_date, "season": season, "capture_phase": capture_phase,
            "parent_run_id": parent_run_id, "canonical_game_set_hash": game_hash,
            "canonical_game_ids": sorted(map(int, features.game_id.unique())),
            "excluded_started_game_ids": sorted(map(int, excluded_started_game_ids or [])),
            "feature_cutoff_utc": cutoff.isoformat(), "capture_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "feature_path": "features.csv", "feature_sha256": sha(feature_out),
            "prediction_path": "predictions.csv", "prediction_sha256": sha(prediction_out),
            "identity_count": int(features[keycols].drop_duplicates().shape[0]),
            "prediction_count": int(len(first)), "coherence_crossing_count": crossings,
            "deterministic_replay": "DETERMINISTIC_REPLAY_PASS", "replay_prediction_sha256": replay_sha,
            "model_identity": identity,
            "phoenix_control": {"parent_run_id": parent_run_id,
                "prediction_sha256": phoenix_prediction_sha256,
                "fitted_model_identity_sha256": phoenix_model_identity_sha256,
                "feature_contract_sha256": phoenix_feature_contract_sha256,
                "prediction_row_count": int(len(phoenix)), "feature_cutoff_utc": phoenix_feature_cutoff_utc,
                "prediction_path": "phoenix_control_predictions.csv", "file_sha256": sha(phoenix_out)},
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
        names = ("features.csv", "predictions.csv", "phoenix_control_predictions.csv",
                 "model_identity.json", "feature_contract_identity.json", "receipt.json", "RUN_COMPLETE.json")
        (capture / "SHA256SUMS").write_text("".join(f"{sha(capture / name)}  {name}\n" for name in names))
        body["capture_path"] = str(capture)
        body["manifest_sha256"] = sha(capture / "SHA256SUMS")
        body["receipt_sha256"] = sha(capture / "receipt.json")
        body["feature_artifact_sha256"] = body["feature_sha256"]
        body["prediction_artifact_sha256"] = body["prediction_sha256"]
        return body


def grade_prior_hgb_capture(*, slate_date: str, reconciliation_package: Path,
                            shadow_root: Path | None = None) -> dict[str, Any]:
    """Grade the highest available pregame phase without rebuilding predictions."""
    from backend.nhl.daily_capture import verify_package
    from backend.nhl.scripts.grade_nhl_points_hgb_shadow import grade
    from backend.nhl.postgame_learning import verify_reconciliation_package

    shadow_root = Path(shadow_root or ROOT / "artifacts/operational/nhl/points_hgb_shadow")
    year, month = int(slate_date[:4]), int(slate_date[5:7])
    season = year if month >= 9 else year - 1
    root = shadow_root / f"season={season}" / f"slate_date={slate_date}"
    candidates = []
    phase_order = {"EARLY": 0, "REFRESH": 1, "FINAL_PREGAME": 2}
    for receipt_path in root.glob("run_id=*/phase=*/captured_at=*/receipt.json"):
        capture = receipt_path.parent
        try:
            verify_package(capture)
            receipt = json.loads(receipt_path.read_text())
            marker = json.loads((capture / "RUN_COMPLETE.json").read_text())
            if receipt.get("status") != "COMPLETE" or marker.get("status") != "COMPLETE":
                continue
            if receipt.get("slate_date") != slate_date:
                continue
            candidates.append((phase_order.get(str(receipt.get("capture_phase", "")).upper(), -1),
                               str(receipt.get("capture_timestamp_utc", "")), capture, receipt))
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            continue
    if not candidates:
        return {"status": "NOT_AVAILABLE", "reason": "NO_VALID_IMMUTABLE_HGB_CAPTURE"}
    _, _, capture, receipt = max(candidates, key=lambda row: (row[0], row[1]))
    outcomes_path = Path(reconciliation_package) / "canonical_skater_outcomes.csv"
    verify_reconciliation_package(Path(reconciliation_package), slate_date)
    outcomes = pd.read_csv(outcomes_path)
    pred = pd.read_csv(capture / "predictions.csv")
    game_ids = set(map(int, receipt["canonical_game_ids"]))
    outcomes = outcomes.loc[outcomes.game_id.astype(int).isin(game_ids)].copy()
    if outcomes.empty or not outcomes.get("official_final", pd.Series(True, index=outcomes.index)).astype(str).str.lower().isin({"true", "t", "1"}).all():
        return {"status": "OUTCOMES_NOT_FINAL", "capture_path": str(capture)}
    report = grade(pred, outcomes)
    phoenix = pd.read_csv(capture / "phoenix_control_predictions.csv")
    phoenix = phoenix.loc[phoenix.game_id.astype(int).isin(game_ids)]
    phoenix_lines = phoenix.pivot(index=["game_id", "player_id"], columns="line", values="prob_over").reset_index()
    phoenix_lines = phoenix_lines.rename(columns={0.5: "prob_over_0_5", 1.5: "prob_over_1_5", 2.5: "prob_over_2_5"})
    scored_outcomes = outcomes.merge(phoenix_lines, on=["game_id", "player_id"], how="inner", validate="one_to_one")
    phoenix_thresholds = {}
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
        "prediction_sha256":receipt["prediction_sha256"], "outcomes_sha256":outcome_sha,
        "game_ids":sorted(game_ids)}, sort_keys=True).encode()).hexdigest()
    grade_root = shadow_root / "grades" / f"season={season}" / f"slate_date={slate_date}" / f"capture={capture.name}" / f"grade={input_identity[:20]}"
    grade_root.mkdir(parents=True, exist_ok=False)
    result = {"schema_version":"NHL_POINTS_HGB_SHADOW_GRADE_V1", "status":"COMPLETE",
        "slate_date":slate_date, "capture_path":str(capture), "capture_manifest_sha256":capture_manifest_sha,
        "hgb_prediction_sha256":receipt["prediction_sha256"], "model_identity":receipt["model_identity"],
        "feature_contract_identity":json.loads((capture/"feature_contract_identity.json").read_text()),
        "official_outcomes_status":"FINAL", "reconciliation_package":str(Path(reconciliation_package).resolve()),
        "reconciliation_manifest_sha256":verify_package(Path(reconciliation_package)),
        "official_outcome_path":str(outcomes_path), "official_outcome_sha256":outcome_sha,
        "official_outcome_identity":input_identity, "hgb_metrics":report,
        "phoenix_same_capture_threshold_metrics":phoenix_thresholds,
        "phoenix_prediction_sha256":receipt["phoenix_control"]["prediction_sha256"],
        "comparison_type":"DESCRIPTIVE_SAME_CAPTURE_CONTROL", "promotion_decision_from_single_slate":False}
    (grade_root / "grade.json").write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
    (grade_root / "RUN_COMPLETE.json").write_text(json.dumps({"status":"COMPLETE",
        "official_outcome_identity":input_identity}, indent=2, sort_keys=True)+"\n")
    sums = f"{sha(grade_root/'grade.json')}  grade.json\n{sha(grade_root/'RUN_COMPLETE.json')}  RUN_COMPLETE.json\n"
    (grade_root / "SHA256SUMS").write_text(sums)
    result["grade_path"] = str(grade_root)
    result["grade_sha256"] = sha(grade_root/"grade.json")
    return result
