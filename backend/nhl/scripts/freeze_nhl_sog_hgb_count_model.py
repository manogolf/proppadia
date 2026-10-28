#!/usr/bin/env python3
"""Freeze the NHL SOG HGB count model for the live shadow (P2.33).

Read-only on the database (research login). Fits the validated recipe
(`backend/nhl/sog_hgb_features.py`, BASE_KW) on every eligible regular-season
skater-game of seasons 2024 and 2025 and writes, under OUT:
- nhl_sog_count_hgb_v1.joblib (the fitted HistGradientBoostingRegressor);
- shadow_contract.json (features, hyperparameters, training rows and frame
  SHA-256, library versions, artifact SHA-256).
Refuses to overwrite an existing artifact.

``--rebuild-training-frame`` (2026-10-28) re-reads the same history read-only,
rebuilds the training frame with the same code, compares it with the
contract's ``training_frame_sha256`` and keeps it locally beside the model as
``training_frame_rebuilt.csv.gz`` with ``training_frame_rebuild_receipt.json``.
It never changes the model or the contract.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import psycopg
import sklearn
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.sog_hgb_features import (  # noqa: E402
    BASE_KW, FEATURES, HISTORY_SQL, build, fit, normalize_logs, position_groups)

OUT = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/frozen_2026-10-25"
ARTIFACT = "nhl_sog_count_hgb_v1.joblib"
MODEL_ID = "NHL_SOG_COUNT_HGB_V1"
TRAINING_SEASONS = (2024, 2025)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def training_frame(logs: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    frame = build(normalize_logs(logs), position_groups(positions))
    keep = (frame.season.isin(TRAINING_SEASONS) & frame.toi_avg_10.notna()
            & frame.shrunk_lambda_180.notna() & ~frame.is_target)
    return frame[keep].sort_values(["game_date", "game_id", "player_id"]).reset_index(drop=True)


def _read_history() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Training history and positions in one read-only snapshot (research login)."""
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute(HISTORY_SQL, {"first_season": 2023, "season": 2025, "slate_date": "2026-07-01"})
        logs = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        cur.execute("SELECT player_id::bigint, position FROM nhl.players")
        positions = pd.DataFrame(cur.fetchall(), columns=["player_id", "position"])
    return logs, positions


def _frame_text(train: pd.DataFrame) -> str:
    return train[["game_id", "player_id", "shots_on_goal"] + FEATURES].to_csv(index=False, float_format="%.17g")


def rebuild_training_frame() -> int:
    """Rebuild the frozen model's training frame and check it against the contract."""
    import gzip
    from datetime import datetime, timezone
    contract = json.loads((OUT / "shadow_contract.json").read_text())
    if sha(OUT / ARTIFACT) != contract["fitted_artifact_sha256"]:
        raise SystemExit("SOG_HGB_ARTIFACT_SHA_MISMATCH")
    train = training_frame(*_read_history())
    frame_text = _frame_text(train)
    rebuilt_sha = hashlib.sha256(frame_text.encode()).hexdigest()
    frame_path = OUT / "training_frame_rebuilt.csv.gz"
    with gzip.open(frame_path, "wt", encoding="utf-8", newline="") as stream:
        stream.write(frame_text)
    model = joblib.load(OUT / ARTIFACT)
    receipt = {
        "schema_version": "NHL_SOG_HGB_TRAINING_FRAME_REBUILD_V1",
        "rebuilt_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "database_identity": "research (REPEATABLE READ READ ONLY)", "database_writes": 0,
        "recorded_training_frame_sha256": contract["training_frame_sha256"],
        "rebuilt_training_frame_sha256": rebuilt_sha,
        "matches_recorded_frame": rebuilt_sha == contract["training_frame_sha256"],
        "recorded_rows": contract["training_rows"], "rebuilt_rows": int(len(train)),
        "recorded_mean_prediction": contract["training_mean_prediction"],
        "rebuilt_mean_prediction": float(model.predict(train[FEATURES].astype(float).to_numpy()).mean()),
        "frame_file": frame_path.name, "frame_file_sha256": sha(frame_path),
        "frame_columns": ["game_id", "player_id", "shots_on_goal"] + FEATURES,
    }
    (OUT / "training_frame_rebuild_receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=1))
    return 0


def main() -> int:
    if "--rebuild-training-frame" in sys.argv[1:]:
        return rebuild_training_frame()
    if (OUT / ARTIFACT).exists():
        raise SystemExit(f"SOG_HGB_ARTIFACT_ALREADY_FROZEN:{OUT / ARTIFACT}")
    train = training_frame(*_read_history())
    frame_text = _frame_text(train)
    model = fit(train, BASE_KW)
    OUT.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, OUT / ARTIFACT)
    reloaded = joblib.load(OUT / ARTIFACT)
    x = train[FEATURES].astype(float).to_numpy()
    if not np.array_equal(reloaded.predict(x), model.predict(x)):
        raise SystemExit("SOG_HGB_ARTIFACT_RELOAD_MISMATCH")
    contract = {
        "schema_version": "NHL_SOG_COUNT_HGB_SHADOW_CONTRACT_V1", "model_id": MODEL_ID,
        "authority": "PROSPECTIVE_SHADOW", "fitted_artifact": ARTIFACT, "fitted_artifact_sha256": sha(OUT / ARTIFACT),
        "training_seasons": list(TRAINING_SEASONS), "training_rows": int(len(train)),
        "training_window": [str(train.game_date.min().date()), str(train.game_date.max().date())],
        "training_frame_sha256": hashlib.sha256(frame_text.encode()).hexdigest(),
        "training_mean_target": float(train.shots_on_goal.mean()),
        "training_mean_prediction": float(model.predict(x).mean()),
        "feature_module": "backend/nhl/sog_hgb_features.py", "feature_columns": FEATURES,
        "history_semantics": "strict prior game_date; cleaned regular-season games (TOI > 0, SOG recorded); seasons s-2..s",
        "target": "skater_game_logs_raw.shots_on_goal", "distribution": "Poisson",
        "thresholds": {"over_1_5": "P(X >= 2)", "over_2_5": "P(X >= 3)", "over_3_5": "P(X >= 4)"},
        "hyperparameters": BASE_KW, "sklearn_version": sklearn.__version__, "joblib_version": joblib.__version__,
        "validation": "NHL Backlog P2.33 (out of sample 2026-01..04; 2026 slates 10-08..10-24)",
        "known_limitation": "season-level shot-rate drift: about -2% to -4% low early in a new season; no tested fix improved log loss",
    }
    (OUT / "shadow_contract.json").write_text(json.dumps(contract, indent=2) + "\n")
    print(json.dumps({k: contract[k] for k in ("model_id", "fitted_artifact_sha256", "training_rows",
                                               "training_window", "training_frame_sha256")}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
