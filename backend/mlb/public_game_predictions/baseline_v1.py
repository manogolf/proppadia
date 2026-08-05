"""Immutable scorer and append-only ledgers for MLB_GAME_PUBLIC_BASELINE_V1.

This module intentionally has no imports from retired MLB model, prop-model,
ranking, EV, upload, routing, or wagering packages.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from scipy.stats import poisson

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "backend/mlb/config/public_game_predictions/MLB_GAME_PUBLIC_BASELINE_V1.json"
RUNTIME_ROOT = REPO_ROOT / "backend/mlb/data/runtime/public_game_predictions/v1"
MODEL_VERSION = "MLB_GAME_PUBLIC_BASELINE_V1"
SNAPSHOT_CLASS = "DESIGNATED_DAILY_PUBLIC_SNAPSHOT"
PUBLIC_PENDING = "PUBLIC_GAME_PREDICTION_CANDIDATE_PENDING_ENABLEMENT"
PUBLIC_ACTIVE = "PUBLIC_GAME_PREDICTION_MODEL_ACTIVE"
BETTING_AUTHORITY = "NO_QUALIFIED_MLB_BETTING_MODEL"
PROP_AUTHORITY = "NO_QUALIFIED_MLB_PROP_MODEL"


class PublicGamePredictionError(RuntimeError):
    pass


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def scorer_sha256() -> str:
    return sha256_bytes(Path(__file__).read_bytes())


def load_candidate(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    identity = payload.get("model_identity")
    expected = sha256_bytes(_canonical_bytes(identity))
    if payload.get("model_hash") != expected:
        raise PublicGamePredictionError("PUBLIC_GAME_BASELINE_MODEL_HASH_MISMATCH")
    if identity.get("model_version") != MODEL_VERSION:
        raise PublicGamePredictionError("PUBLIC_GAME_BASELINE_VERSION_MISMATCH")
    return payload


def feature_enabled(env: dict[str, str] | None = None) -> bool:
    source = os.environ if env is None else env
    return str(source.get("MLB_PUBLIC_GAME_PREDICTIONS_ENABLED", "0")).strip().lower() in {
        "1", "true", "yes", "on"
    }


def authority_status(env: dict[str, str] | None = None) -> dict[str, str]:
    enabled = feature_enabled(env)
    return {
        "public_game_prediction_authority": PUBLIC_ACTIVE if enabled else PUBLIC_PENDING,
        "betting_authority": BETTING_AUTHORITY,
        "player_prop_authority": PROP_AUTHORITY,
        "model_version": MODEL_VERSION,
    }


def _joint(mu_home: float, mu_away: float) -> tuple[list[int], Any]:
    import numpy as np
    grid = list(range(24))
    joint = np.outer(poisson.pmf(grid, mu_home), poisson.pmf(grid, mu_away))
    joint /= joint.sum()
    return grid, joint


def _probability_contract(mu_home: float, mu_away: float) -> dict[str, float]:
    grid, joint = _joint(mu_home, mu_away)
    home = float(sum(joint[h, a] for h in grid for a in grid if h > a))
    away = float(sum(joint[h, a] for h in grid for a in grid if a > h))
    tie = float(sum(joint[x, x] for x in grid))
    decided = home + away
    prob = lambda fn: float(sum(joint[h, a] for h in grid for a in grid if fn(h, a)))
    return {
        "home_win_probability": home / decided,
        "away_win_probability": away / decided,
        "tie_after_nine_probability": tie,
        "home_minus_1_5_probability": prob(lambda h, a: h - a > 1.5),
        "away_plus_1_5_probability": prob(lambda h, a: h - a < 1.5),
        "away_minus_1_5_probability": prob(lambda h, a: a - h > 1.5),
        "home_plus_1_5_probability": prob(lambda h, a: a - h < 1.5),
    }


def _confidence(home_probability: float) -> str:
    separation = abs(home_probability - 0.5)
    if separation <= 0.025:
        return "NEAR_EVEN"
    if separation <= 0.075:
        return "LEAN"
    if separation <= 0.125:
        return "MODERATE"
    return "STRONG"


def _schedule_games(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for date_block in payload.get("dates") or []:
        for game in date_block.get("games") or []:
            yield game


def _team_name(game: dict[str, Any], side: str) -> str | None:
    return (((game.get("teams") or {}).get(side) or {}).get("team") or {}).get("name")


def score_schedule_payload(
    payload: dict[str, Any], *, prediction_timestamp_utc: str, source_schedule_hash: str,
    snapshot_class: str = SNAPSHOT_CLASS,
) -> list[dict[str, Any]]:
    """Score schedule identity only; never reads scores, outcomes, odds, or props."""
    config = load_candidate()
    identity = config["model_identity"]
    now = datetime.fromisoformat(prediction_timestamp_utc.replace("Z", "+00:00")).astimezone(timezone.utc)
    mu_home = float(identity["expected_home_runs"])
    mu_away = float(identity["expected_away_runs"])
    probs = _probability_contract(mu_home, mu_away)
    rows: list[dict[str, Any]] = []
    for game in _schedule_games(payload):
        game_id = game.get("gamePk")
        start_raw = game.get("gameDate")
        away_team = _team_name(game, "away")
        home_team = _team_name(game, "home")
        reason = None
        start = None
        try:
            start = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            reason = "SCHEDULED_START_TIME_UNRESOLVED"
        if not game_id or not away_team or not home_team:
            reason = reason or "CORE_GAME_IDENTITY_UNRESOLVED"
        if start is not None and now >= start:
            reason = reason or "PREGAME_CUTOFF_FAILED"
        game_date = str(game.get("officialDate") or (start.date().isoformat() if start else payload.get("date") or ""))
        common = {
            "league": "MLB", "game_date": game_date, "game_id": game_id,
            "scheduled_start_utc": start.isoformat().replace("+00:00", "Z") if start else start_raw,
            "prediction_timestamp_utc": now.isoformat().replace("+00:00", "Z"),
            "away_team": away_team, "home_team": home_team,
            "prediction_snapshot_class": snapshot_class,
            "model_name": identity["model_name"], "model_version": identity["model_version"],
            "model_hash": config["model_hash"], "scorer_hash": scorer_sha256(),
            "source_schedule_hash": source_schedule_hash,
            "historical_population": identity["historical_population"],
            "historical_moneyline_accuracy": config["historical_evaluation"]["moneyline_accuracy"],
            "historical_moneyline_brier": config["historical_evaluation"]["moneyline_brier"],
            "historical_total_mae": config["historical_evaluation"]["total_runs_mae"],
            "betting_edge_status": config["betting_edge_status"], "disclosure": config["disclosure"],
            "generated_at_utc": now.isoformat().replace("+00:00", "Z"),
            "admission_status": "REJECTED_FAIL_CLOSED" if reason else "ADMITTED_SHADOW",
            "failure_reason": reason,
            "lead_time_minutes": (start-now).total_seconds()/60 if start else None,
        }
        if reason:
            rows.append(common)
            continue
        home_p = probs["home_win_probability"]
        common.update({
            "predicted_winner": home_team if home_p >= 0.5 else away_team,
            "expected_home_runs": mu_home, "expected_away_runs": mu_away,
            "expected_total_runs": mu_home + mu_away, "expected_home_margin": mu_home - mu_away,
            "confidence_band": _confidence(home_p), "data_quality_status": "SCHEDULE_IDENTITY_CERTIFIED_BASELINE",
            **probs,
        })
        rows.append(common)
    return rows


def _identity(row: dict[str, Any]) -> str:
    return "|".join(str(row.get(k) or "") for k in (
        "game_date", "game_id", "model_version", "prediction_snapshot_class"
    ))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n")
    return len(rows)


def append_prediction_rows(rows: list[dict[str, Any]], path: Path) -> int:
    existing = {_identity(row) for row in _read_jsonl(path)}
    admitted: list[dict[str, Any]] = []
    seen = set(existing)
    for row in rows:
        key = _identity(row)
        if not all(key.split("|")):
            raise PublicGamePredictionError("PREDICTION_LEDGER_IDENTITY_INCOMPLETE")
        if key in seen:
            continue
        seen.add(key); admitted.append(row)
    return _append_jsonl(path, admitted)


def append_grading_rows(rows: list[dict[str, Any]], path: Path) -> int:
    existing = {_identity(row) for row in _read_jsonl(path)}
    admitted = []
    for row in rows:
        if row.get("official_status") != "Final":
            raise PublicGamePredictionError("GRADING_REQUIRES_OFFICIAL_FINAL")
        key = _identity(row)
        if key in existing:
            continue
        existing.add(key);admitted.append(row)
    return _append_jsonl(path, admitted)
