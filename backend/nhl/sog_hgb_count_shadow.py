"""Immutable prospective SOG shadow: frozen HGB Poisson count model (P2.33).

Scores the exact production-scored player-games with the frozen
`NHL_SOG_COUNT_HGB_V1` artifact. Features come from
`backend/nhl/sog_hgb_features.py` over strict-prior skater logs read
read-only from the database. A player without qualifying history (no recent
TOI or no prior-season league rate) keeps the production expected shots.
Production SOG is unchanged; this module only writes its own capture package.
"""
from __future__ import annotations

import hashlib
import json
from functools import partial
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, verify_package
from backend.nhl.sog_feature_input import sha256_file
from backend.nhl.sog_hgb_features import FEATURES, HISTORY_SQL, build, normalize_logs, position_groups
from backend.nhl.sog_shrunk_rate_shadow import _write_sums
from backend.nhl.sog_shrunk_rate_shadow import grade_slate as _grade_count_shadow
from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail

ROOT = Path(__file__).resolve().parents[2]
MODEL_IDENTITY = "NHL_SOG_COUNT_HGB_V1"
PRIMARY_SOURCE = "HGB_COUNT"
CONTRACT_PATH = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/frozen_2026-10-25/shadow_contract.json"
EXPECTED_ARTIFACT_SHA256 = "f7bf625dd3a32030a6003879f7e62380e674859f8c7f9f299e4bdae2245ae392"
LINES = (1.5, 2.5, 3.5)

grade_slate = partial(_grade_count_shadow, model_identity=MODEL_IDENTITY, primary_source=PRIMARY_SOURCE)


def load_model(contract_path: Path = CONTRACT_PATH):
    contract = json.loads(Path(contract_path).read_text())
    artifact = Path(contract_path).parent / contract["fitted_artifact"]
    digest = sha256_file(artifact)
    if digest != contract["fitted_artifact_sha256"] or digest != EXPECTED_ARTIFACT_SHA256:
        raise ValueError("SOG_HGB_ARTIFACT_SHA_MISMATCH")
    if contract["feature_columns"] != FEATURES:
        raise ValueError("SOG_HGB_FEATURE_CONTRACT_MISMATCH")
    return joblib.load(artifact), contract, digest


def load_history(dsn: str, *, slate_date: str, season: int, game_ids: list[int]
                 ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read-only pull of strict-prior logs, positions and the slate's games."""
    import psycopg
    with psycopg.connect(dsn, prepare_threshold=None) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cursor.execute(HISTORY_SQL, {"first_season": season - 2, "season": season, "slate_date": slate_date})
        history = pd.DataFrame(cursor.fetchall(), columns=[d.name for d in cursor.description])
        cursor.execute("SELECT player_id::bigint, position FROM nhl.players")
        positions = pd.DataFrame(cursor.fetchall(), columns=["player_id", "position"])
        cursor.execute("SELECT game_id::bigint, home_team_id::bigint, away_team_id::bigint FROM nhl.games "
                       "WHERE game_id = ANY(%s)", (list(map(int, game_ids)),))
        games = pd.DataFrame(cursor.fetchall(), columns=["game_id", "home_team_id", "away_team_id"])
    return history, positions, games


def history_sha256(history: pd.DataFrame) -> str:
    frame = normalize_logs(history).sort_values(["player_id", "game_date", "game_id"])
    return hashlib.sha256(frame.to_csv(index=False, float_format="%.17g").encode()).hexdigest()


def build_predictions(history: pd.DataFrame, positions: pd.DataFrame, games: pd.DataFrame, production: pd.DataFrame,
                      model, *, slate_date: str, season: int, run_id: str, feature_cutoff_utc: str,
                      canonical_game_ids: list[int], started_excluded_game_ids: list[int] | None = None) -> pd.DataFrame:
    keys = ["game_id", "player_id"]
    if {"game_id", "player_id", "expected_sog", "is_home"} - set(production.columns):
        raise ValueError("SOG_HGB_PRODUCTION_SCHEMA_INVALID")
    if production.duplicated(keys).any():
        raise ValueError("SOG_HGB_DUPLICATE_PLAYER_GAME")
    prod = production.copy()
    prod["game_id"] = pd.to_numeric(prod.game_id).astype("int64")
    prod["player_id"] = pd.to_numeric(prod.player_id).astype("int64")
    if not set(prod.game_id).issubset(set(map(int, canonical_game_ids))):
        raise ValueError("SOG_HGB_NONCANONICAL_GAME")
    logs = normalize_logs(history)
    if len(logs) and logs.game_date.max() >= pd.Timestamp(slate_date):
        raise ValueError("SOG_HGB_HISTORY_NOT_STRICTLY_PRIOR")
    g = games.copy()
    g["game_id"] = pd.to_numeric(g.game_id).astype("int64")
    targets = prod[keys + ["is_home"]].merge(g, on="game_id", how="left", validate="many_to_one")
    if targets[["home_team_id", "away_team_id"]].isna().any().any():
        raise ValueError("SOG_HGB_SLATE_GAME_MISSING")
    home = targets.is_home.astype(str).str.strip().str.lower()
    if not home.isin(["true", "t", "1", "1.0", "false", "f", "0", "0.0"]).all():
        raise ValueError("SOG_HGB_IS_HOME_UNPARSEABLE")
    targets["is_home"] = home.isin(["true", "t", "1", "1.0"])
    targets["game_date"] = pd.Timestamp(slate_date)
    targets["season"] = int(season)
    frame = build(logs, position_groups(positions), targets)
    fcols = [f"feature_{c}" for c in FEATURES]
    feats = frame[frame.is_target][keys + FEATURES].rename(columns=dict(zip(FEATURES, fcols)))
    joined = prod.merge(feats, on=keys, how="left", validate="one_to_one")
    production_lambda = pd.to_numeric(joined.expected_sog, errors="coerce")
    if production_lambda.isna().any() or production_lambda.lt(0).any():
        raise ValueError("SOG_HGB_PRODUCTION_LAMBDA_INVALID")
    eligible = joined.feature_toi_avg_10.notna() & joined.feature_shrunk_lambda_180.notna()
    model_lambda = pd.Series(np.nan, index=joined.index)
    if eligible.any():
        model_lambda.loc[eligible] = model.predict(joined.loc[eligible, fcols].astype(float).to_numpy())
    shadow_lambda = model_lambda.where(eligible, production_lambda)
    if not np.isfinite(shadow_lambda).all() or shadow_lambda.lt(0).any():
        raise ValueError("SOG_HGB_SHADOW_LAMBDA_INVALID")
    out = joined[[c for c in ("game_id", "player_id", "team_id", "opponent_id", "is_home", "game_date") if c in joined]].copy()
    out.insert(0, "slate_date", slate_date)
    out["season"] = season
    out["parent_daily_run_id"] = run_id
    out["started_excluded_game_ids"] = json.dumps(sorted(set(map(int, started_excluded_game_ids or []))))
    out["feature_cutoff_utc"] = feature_cutoff_utc
    out["production_model_family"] = "poisson_baseline"
    out["production_model_version"] = "baseline_v1"
    for col in fcols:
        out[col] = joined[col]
    out["shadow_source"] = np.where(eligible, PRIMARY_SOURCE, "NO_CLEAN_HISTORY_PRODUCTION_FALLBACK")
    out["production_lambda"] = production_lambda
    out["shadow_lambda"] = shadow_lambda
    out["lambda_delta"] = shadow_lambda - production_lambda
    parts = []
    for line, threshold, pcol in zip(LINES, (2, 3, 4), ("p_over_1_5", "p_over_2_5", "p_over_3_5")):
        p_prod = (pd.to_numeric(joined[pcol], errors="coerce") if pcol in joined
                  else production_lambda.map(lambda x: _poisson_tail(float(x), threshold)))
        p_shadow = shadow_lambda.map(lambda x: _poisson_tail(float(x), threshold))
        out[f"production_{pcol}"] = p_prod
        out[f"shadow_{pcol}"] = p_shadow
        part = out.copy()
        part["line"] = line
        part["production_p_over"] = p_prod
        part["shadow_p_over"] = p_shadow
        part["probability_delta"] = p_shadow - p_prod
        part["p_under"] = 1 - p_shadow
        part["selected_side"] = np.where(p_shadow.ge(.5), "OVER", "UNDER")
        parts.append(part)
    if not ((out.shadow_p_over_1_5 >= out.shadow_p_over_2_5) & (out.shadow_p_over_2_5 >= out.shadow_p_over_3_5)).all():
        raise ValueError("SOG_HGB_THRESHOLD_COHERENCE_FAILED")
    return pd.concat(parts, ignore_index=True)


def capture(*, history: pd.DataFrame, positions: pd.DataFrame, games: pd.DataFrame, production_path: Path,
            output_root: Path, run_id: str, slate_date: str, season: int, feature_cutoff_utc: str,
            canonical_game_ids: list[int], started_excluded_game_ids: list[int] | None, scorer_path: Path,
            contract_path: Path = CONTRACT_PATH) -> dict[str, Any]:
    model, contract, artifact_sha = load_model(contract_path)
    production = pd.read_csv(production_path)
    kwargs = dict(slate_date=slate_date, season=season, run_id=run_id, feature_cutoff_utc=feature_cutoff_utc,
                  canonical_game_ids=canonical_game_ids, started_excluded_game_ids=started_excluded_game_ids)
    frame = build_predictions(history, positions, games, production, model, **kwargs)
    replay = build_predictions(history, positions, games, production, model, **kwargs)
    out_root = Path(output_root) / f"season={season}" / f"slate_date={slate_date}" / f"run_id={run_id}"
    final = out_root / f"model={MODEL_IDENTITY}"
    stage = out_root / f".{MODEL_IDENTITY}.incomplete"
    out_root.mkdir(parents=True, exist_ok=True)
    if final.exists():
        raise FileExistsError(f"SOG_HGB_CAPTURE_ALREADY_EXISTS:{final}")
    stage.mkdir(exist_ok=False)
    prediction_path = stage / "predictions.csv"
    frame.to_csv(prediction_path, index=False, float_format="%.17g")
    replay_path = stage / "replay_predictions.csv"
    replay.to_csv(replay_path, index=False, float_format="%.17g")
    if sha256_file(prediction_path) != sha256_file(replay_path):
        raise ValueError("SOG_HGB_DETERMINISTIC_REPLAY_FAILED")
    replay_path.unlink()
    players = frame.drop_duplicates(["game_id", "player_id"])
    manifest = {
        "contract": "NHL_SOG_COUNT_HGB_SHADOW_CONTRACT_V1", "model_identity": MODEL_IDENTITY,
        "research_role": "PROSPECTIVE_SHADOW", "season": season, "slate_date": slate_date, "parent_daily_run_id": run_id,
        "fitted_artifact_sha256": artifact_sha, "shadow_contract_sha256": sha256_file(contract_path),
        "training_frame_sha256": contract["training_frame_sha256"], "feature_columns": FEATURES,
        "history_source": "nhl.skater_game_logs_raw JOIN nhl.games (read-only)",
        "history_row_count": int(len(history)), "history_sha256": history_sha256(history),
        "production_model_identity": {"family": "poisson_baseline", "version": "baseline_v1"},
        "production_scorer_path": str(Path(scorer_path).resolve()), "production_scorer_sha256": sha256_file(scorer_path),
        "production_prediction_path": str(Path(production_path).resolve()),
        "production_prediction_sha256": sha256_file(production_path),
        "canonical_game_ids": sorted(set(map(int, canonical_game_ids))),
        "canonical_game_set_hash": canonical_game_set_hash(canonical_game_ids),
        "started_excluded_game_ids": sorted(set(map(int, started_excluded_game_ids or []))),
        "feature_cutoff_utc": feature_cutoff_utc, "prediction_row_count": len(frame),
        "player_game_count": len(players), "line_count": len(LINES),
        "model_player_game_count": int(players.shadow_source.eq(PRIMARY_SOURCE).sum()),
        "production_fallback_player_game_count": int(players.shadow_source.ne(PRIMARY_SOURCE).sum()),
        "deterministic_replay": "PASS", "prediction_sha256": sha256_file(prediction_path), "status": "COMPLETE",
    }
    (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    readback = pd.read_csv(prediction_path)
    if (len(readback) != len(frame)
            or not np.array_equal(readback[["game_id", "player_id", "line"]].to_numpy(float),
                                  frame[["game_id", "player_id", "line"]].to_numpy(float))
            or not np.allclose(readback.shadow_lambda, frame.shadow_lambda, rtol=0, atol=1e-15)):
        raise ValueError("SOG_HGB_CAPTURE_READBACK_FAILED")
    (stage / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE", "parent_daily_run_id": run_id}) + "\n")
    _write_sums(stage)
    stage.rename(final)
    verify_package(final)
    manifest["path"] = str(final.resolve())
    manifest["manifest_sha256"] = sha256_file(final / "manifest.json")
    return manifest
