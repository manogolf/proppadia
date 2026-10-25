"""Immutable prospective SOG shadow: shrunk, cleaned 20-game shot rate (P2.32).

Recipe (validated read-only in `scripts/research_nhl_sog_shrunk_rate.py`):
- history: the player's regular-season games with TOI > 0 and a recorded SOG,
  strictly before the slate date;
- rate: (SOG over the last 20 such games + prior rate * K / 60)
        / (TOI minutes over those games + K) * 60, with K = 180 minutes;
- prior: the previous season's regular-season league shots per 60 for the
  player's position group (D or F);
- exposure: mean TOI of the last 10 such games;
- probabilities: the production Poisson tail for 1.5/2.5/3.5.
A player with no qualifying history keeps the production expected shots.
Production SOG is unchanged; this module only writes its own capture package.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, verify_package
from backend.nhl.sog_feature_input import sha256_file
from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail

MODEL_IDENTITY = "NHL_SOG_SHRUNK_RATE_K180_V1"
CONTRACT = "NHL_SOG_SHRUNK_RATE_K180_V1"
PRIOR_MINUTES = 180
RATE_WINDOW = 20
TOI_WINDOW = 10
LINES = (1.5, 2.5, 3.5)
HISTORY_COLUMNS = ["player_id", "game_id", "game_date", "season", "game_type", "shots_on_goal", "toi_minutes"]

HISTORY_SQL = """
SELECT l.player_id::bigint, l.game_id::bigint, g.game_date, g.season::int,
       substring(g.game_id::text, 5, 2) AS game_type,
       l.shots_on_goal, l.toi_minutes
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
 WHERE g.season BETWEEN %(first_season)s AND %(season)s
   AND g.game_date < %(slate_date)s::date
 ORDER BY l.player_id, g.game_date, l.game_id
"""


def load_history(dsn: str, *, slate_date: str, season: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read-only pull of strict-prior skater logs and position groups."""
    import psycopg
    with psycopg.connect(dsn, prepare_threshold=None) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cursor.execute(HISTORY_SQL, {"first_season": season - 2, "season": season, "slate_date": slate_date})
        history = pd.DataFrame(cursor.fetchall(), columns=HISTORY_COLUMNS)
        cursor.execute("SELECT player_id::bigint, position FROM nhl.players")
        positions = pd.DataFrame(cursor.fetchall(), columns=["player_id", "position"])
    return history, positions


def _normalize_history(history: pd.DataFrame, slate_date: str) -> pd.DataFrame:
    frame = history[HISTORY_COLUMNS].copy()
    frame["player_id"] = pd.to_numeric(frame.player_id).astype("int64")
    frame["game_id"] = pd.to_numeric(frame.game_id).astype("int64")
    frame["season"] = pd.to_numeric(frame.season).astype("int64")
    frame["game_date"] = pd.to_datetime(frame.game_date).dt.strftime("%Y-%m-%d")
    frame["game_type"] = frame.game_type.astype(str).str.zfill(2)
    for column in ("shots_on_goal", "toi_minutes"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(float)
    if frame.game_date.ge(slate_date).any():
        raise ValueError("SOG_SHRUNK_RATE_HISTORY_NOT_STRICTLY_PRIOR")
    if frame.duplicated(["player_id", "game_id"]).any():
        raise ValueError("SOG_SHRUNK_RATE_DUPLICATE_HISTORY_ROW")
    return frame.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)


def history_sha256(history: pd.DataFrame, slate_date: str) -> str:
    """Content hash of the normalized history used for the shadow inputs."""
    text = _normalize_history(history, slate_date).to_csv(index=False, float_format="%.17g")
    return hashlib.sha256(text.encode()).hexdigest()


def position_groups(positions: pd.DataFrame) -> pd.DataFrame:
    frame = positions[["player_id", "position"]].copy()
    frame["player_id"] = pd.to_numeric(frame.player_id).astype("int64")
    frame["position_group"] = np.where(frame.position.astype(str).str.upper().str.startswith("D"), "D", "F")
    return frame.drop_duplicates("player_id")[["player_id", "position_group"]]


def league_prior(history: pd.DataFrame, groups: pd.DataFrame, prior_season: int) -> dict[str, float]:
    """Prior-season regular-season shots per 60 by position group."""
    rows = history[(history.season == prior_season) & (history.game_type == "02")
                   & (history.toi_minutes > 0) & history.shots_on_goal.notna()]
    rows = rows.merge(groups, on="player_id", how="left")
    rows["position_group"] = rows.position_group.fillna("F")
    grouped = rows.groupby("position_group")
    rates = (grouped.shots_on_goal.sum() / grouped.toi_minutes.sum() * 60).to_dict()
    if set(rates) != {"D", "F"} or not all(math.isfinite(v) and v > 0 for v in rates.values()):
        raise ValueError("SOG_SHRUNK_RATE_LEAGUE_PRIOR_UNAVAILABLE")
    return {key: float(rates[key]) for key in ("D", "F")}


def player_windows(history: pd.DataFrame) -> pd.DataFrame:
    """Last-20 SOG/TOI sums and last-10 mean TOI over cleaned prior games."""
    clean = history[(history.game_type == "02") & (history.toi_minutes > 0) & history.shots_on_goal.notna()]
    clean = clean.sort_values(["player_id", "game_date", "game_id"])
    last20 = clean.groupby("player_id").tail(RATE_WINDOW).groupby("player_id")
    last10 = clean.groupby("player_id").tail(TOI_WINDOW).groupby("player_id")
    return pd.DataFrame({
        "clean_games_20": last20.size(), "clean_sog_20": last20.shots_on_goal.sum(),
        "clean_toi_20": last20.toi_minutes.sum(), "clean_toi_avg_10": last10.toi_minutes.mean(),
    }).reset_index()


def build_predictions(history: pd.DataFrame, positions: pd.DataFrame, production: pd.DataFrame, *,
                      slate_date: str, season: int, run_id: str, feature_cutoff_utc: str,
                      canonical_game_ids: list[int], started_excluded_game_ids: list[int] | None = None,
                      production_identity: tuple[str, str] = ("poisson_baseline", "baseline_v1"),
                      ) -> tuple[pd.DataFrame, dict[str, float]]:
    """Build shadow rows for the exact production-scored player-game set."""
    keys = ["game_id", "player_id"]
    if {"game_id", "player_id", "expected_sog"} - set(production.columns):
        raise ValueError("SOG_SHRUNK_RATE_PRODUCTION_SCHEMA_INVALID")
    if production.duplicated(keys).any():
        raise ValueError("SOG_SHRUNK_RATE_DUPLICATE_PLAYER_GAME")
    prod = production.copy()
    prod["game_id"] = pd.to_numeric(prod.game_id).astype("int64")
    prod["player_id"] = pd.to_numeric(prod.player_id).astype("int64")
    if not set(prod.game_id).issubset(set(map(int, canonical_game_ids))):
        raise ValueError("SOG_SHRUNK_RATE_NONCANONICAL_GAME")
    hist = _normalize_history(history, slate_date)
    groups = position_groups(positions)
    prior = league_prior(hist, groups, season - 1)
    windows = player_windows(hist)
    joined = prod.merge(windows, on="player_id", how="left", validate="many_to_one")
    joined = joined.merge(groups, on="player_id", how="left", validate="many_to_one")
    joined["position_group"] = joined.position_group.fillna("F")
    production_lambda = pd.to_numeric(joined.expected_sog, errors="coerce")
    if production_lambda.isna().any() or production_lambda.lt(0).any():
        raise ValueError("SOG_SHRUNK_RATE_PRODUCTION_LAMBDA_INVALID")
    prior_rate = joined.position_group.map(prior)
    sog20 = joined.clean_sog_20.fillna(0.0)
    toi20 = joined.clean_toi_20.fillna(0.0)
    shrunk_rate = (sog20 + prior_rate * PRIOR_MINUTES / 60) / (toi20 + PRIOR_MINUTES) * 60
    exposure = pd.to_numeric(joined.clean_toi_avg_10, errors="coerce")
    has_history = exposure.notna() & exposure.gt(0)
    shadow_lambda = (shrunk_rate * exposure / 60).where(has_history, production_lambda)
    out_cols = [c for c in ("game_id", "player_id", "team_id", "opponent_id", "is_home", "game_date") if c in joined]
    out = joined[out_cols].copy()
    out.insert(0, "slate_date", slate_date)
    out["season"] = season
    out["parent_daily_run_id"] = run_id
    out["started_excluded_game_ids"] = json.dumps(sorted(set(map(int, started_excluded_game_ids or []))))
    out["feature_cutoff_utc"] = feature_cutoff_utc
    out["production_model_family"], out["production_model_version"] = production_identity
    out["position_group"] = joined.position_group
    out["league_prior_sog_per60"] = prior_rate
    out["prior_minutes"] = PRIOR_MINUTES
    out["clean_games_20"] = joined.clean_games_20.fillna(0).astype(int)
    out["clean_sog_20"] = joined.clean_sog_20
    out["clean_toi_20"] = joined.clean_toi_20
    out["clean_toi_avg_10"] = exposure
    out["shrunk_rate_per60"] = shrunk_rate.where(has_history)
    out["shadow_source"] = np.where(has_history, "SHRUNK_RATE", "NO_CLEAN_HISTORY_PRODUCTION_FALLBACK")
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
        raise ValueError("SOG_SHRUNK_RATE_THRESHOLD_COHERENCE_FAILED")
    if not np.isfinite(out.shadow_lambda).all():
        raise ValueError("SOG_SHRUNK_RATE_SHADOW_LAMBDA_INVALID")
    return pd.concat(parts, ignore_index=True), prior


def _write_sums(path: Path) -> None:
    (path / "SHA256SUMS").write_text("".join(
        f"{sha256_file(item)}  {item.name}\n" for item in sorted(path.iterdir())
        if item.is_file() and item.name != "SHA256SUMS"))


def capture(*, history: pd.DataFrame, positions: pd.DataFrame, production_path: Path, output_root: Path,
            run_id: str, slate_date: str, season: int, feature_cutoff_utc: str,
            canonical_game_ids: list[int], started_excluded_game_ids: list[int] | None,
            scorer_path: Path, production_identity: tuple[str, str] = ("poisson_baseline", "baseline_v1"),
            ) -> dict[str, Any]:
    production = pd.read_csv(production_path)
    kwargs = dict(slate_date=slate_date, season=season, run_id=run_id, feature_cutoff_utc=feature_cutoff_utc,
                  canonical_game_ids=canonical_game_ids, started_excluded_game_ids=started_excluded_game_ids,
                  production_identity=production_identity)
    frame, prior = build_predictions(history, positions, production, **kwargs)
    replay, _ = build_predictions(history, positions, production, **kwargs)
    out_root = Path(output_root) / f"season={season}" / f"slate_date={slate_date}" / f"run_id={run_id}"
    final = out_root / f"model={MODEL_IDENTITY}"
    stage = out_root / f".{MODEL_IDENTITY}.incomplete"
    out_root.mkdir(parents=True, exist_ok=True)
    if final.exists():
        raise FileExistsError(f"SOG_SHRUNK_RATE_CAPTURE_ALREADY_EXISTS:{final}")
    stage.mkdir(exist_ok=False)
    prediction_path = stage / "predictions.csv"
    frame.to_csv(prediction_path, index=False, float_format="%.17g")
    replay_path = stage / "replay_predictions.csv"
    replay.to_csv(replay_path, index=False, float_format="%.17g")
    if sha256_file(prediction_path) != sha256_file(replay_path):
        raise ValueError("SOG_SHRUNK_RATE_DETERMINISTIC_REPLAY_FAILED")
    replay_path.unlink()
    players = frame.drop_duplicates(["game_id", "player_id"])
    manifest = {
        "contract": CONTRACT, "model_identity": MODEL_IDENTITY, "research_role": "PROSPECTIVE_SHADOW",
        "season": season, "slate_date": slate_date, "parent_daily_run_id": run_id,
        "recipe": {"rate_window_games": RATE_WINDOW, "toi_window_games": TOI_WINDOW,
                   "prior_minutes": PRIOR_MINUTES, "prior_season": season - 1,
                   "history_filter": "regular season, toi_minutes > 0, shots_on_goal not null, game_date < slate_date",
                   "fallback": "production expected_sog when the player has no qualifying history"},
        "league_prior_sog_per60": prior,
        "history_source": "nhl.skater_game_logs_raw JOIN nhl.games (read-only)",
        "history_row_count": int(len(history)), "history_sha256": history_sha256(history, slate_date),
        "production_model_identity": {"family": production_identity[0], "version": production_identity[1]},
        "production_scorer_path": str(Path(scorer_path).resolve()), "production_scorer_sha256": sha256_file(scorer_path),
        "production_prediction_path": str(Path(production_path).resolve()),
        "production_prediction_sha256": sha256_file(production_path),
        "canonical_game_ids": sorted(set(map(int, canonical_game_ids))),
        "canonical_game_set_hash": canonical_game_set_hash(canonical_game_ids),
        "started_excluded_game_ids": sorted(set(map(int, started_excluded_game_ids or []))),
        "feature_cutoff_utc": feature_cutoff_utc, "prediction_row_count": len(frame),
        "player_game_count": len(players), "line_count": len(LINES),
        "shrunk_player_game_count": int(players.shadow_source.eq("SHRUNK_RATE").sum()),
        "production_fallback_player_game_count": int(players.shadow_source.ne("SHRUNK_RATE").sum()),
        "deterministic_replay": "PASS", "prediction_sha256": sha256_file(prediction_path), "status": "COMPLETE",
    }
    (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    readback = pd.read_csv(prediction_path)
    if (len(readback) != len(frame)
            or not np.array_equal(readback[["game_id", "player_id", "line"]].to_numpy(float),
                                  frame[["game_id", "player_id", "line"]].to_numpy(float))
            or not np.allclose(readback.shadow_lambda, frame.shadow_lambda, rtol=0, atol=1e-15)
            or not np.allclose(readback.shadow_p_over, frame.shadow_p_over, rtol=0, atol=1e-15)):
        raise ValueError("SOG_SHRUNK_RATE_CAPTURE_READBACK_FAILED")
    (stage / "RUN_COMPLETE.json").write_text(json.dumps({"status": "COMPLETE", "parent_daily_run_id": run_id}) + "\n")
    _write_sums(stage)
    stage.rename(final)
    verify_package(final)
    manifest["path"] = str(final.resolve())
    manifest["manifest_sha256"] = sha256_file(final / "manifest.json")
    return manifest


def _probability_metrics(y: pd.Series, p: pd.Series) -> dict[str, float]:
    p = p.clip(1e-15, 1 - 1e-15)
    return {"brier": float(((p - y) ** 2).mean()),
            "log_loss": float((-(y * np.log(p) + (1 - y) * np.log(1 - p))).mean())}


def _poisson_log_loss(y: pd.Series, lam: pd.Series) -> float:
    from scipy.stats import poisson
    return float(-poisson.logpmf(y.astype(int), lam.clip(lower=0.02)).mean())


def _grade_rows(rows: pd.DataFrame, value: str, primary_source: str = "SHRUNK_RATE") -> dict[str, Any]:
    """Count and per-line metrics for settled line rows, paired with production."""
    players = rows.drop_duplicates(["game_id", "player_id"])
    y_player = pd.to_numeric(players[value])
    lam, prod = pd.to_numeric(players.shadow_lambda), pd.to_numeric(players.production_lambda)
    item: dict[str, Any] = {"settled_player_games": int(len(players)), "settled_line_rows": int(len(rows)),
                            "production_fallback_player_games": int(players.shadow_source.ne(primary_source).sum())}
    if len(players):
        item.update({
            "mean_lambda": float(lam.mean()), "mean_actual": float(y_player.mean()),
            "count_mae": float((lam - y_player).abs().mean()),
            "paired_count_mae_difference_vs_production": float((lam - y_player).abs().mean() - (prod - y_player).abs().mean()),
            "poisson_log_loss": _poisson_log_loss(y_player, lam),
            "paired_poisson_log_loss_difference_vs_production": _poisson_log_loss(y_player, lam) - _poisson_log_loss(y_player, prod),
        })
    by_line = {}
    wins = losses = prod_wins = prod_losses = 0
    for line in LINES:
        part = rows[rows.line.eq(line)]
        entry: dict[str, Any] = {"n": int(len(part))}
        if len(part):
            y = (pd.to_numeric(part[value]) > line).astype(float)
            ps, pp = pd.to_numeric(part.shadow_p_over), pd.to_numeric(part.production_p_over)
            shadow_hit, prod_hit = ps.ge(.5).eq(y.eq(1)), pp.ge(.5).eq(y.eq(1))
            sm, pm = _probability_metrics(y, ps), _probability_metrics(y, pp)
            entry.update({**sm, "wins": int(shadow_hit.sum()), "losses": int((~shadow_hit).sum()),
                          "production_wins": int(prod_hit.sum()), "production_losses": int((~prod_hit).sum()),
                          "paired_brier_difference_vs_production": sm["brier"] - pm["brier"],
                          "paired_log_loss_difference_vs_production": sm["log_loss"] - pm["log_loss"],
                          "side_disagreements": int(ps.ge(.5).ne(pp.ge(.5)).sum()),
                          "wins_gained": int((shadow_hit & ~prod_hit).sum()),
                          "wins_lost": int((~shadow_hit & prod_hit).sum())})
            wins += entry["wins"]; losses += entry["losses"]
            prod_wins += entry["production_wins"]; prod_losses += entry["production_losses"]
        by_line[str(line)] = entry
    item["by_line"] = by_line
    item["overall"] = {"wins": wins, "losses": losses}
    item["production_overall"] = {"wins": prod_wins, "losses": prod_losses}
    return item


def grade_slate(*, slate_date: str, reconciliation_package: Path, capture_root: Path,
                model_identity: str = MODEL_IDENTITY, primary_source: str = "SHRUNK_RATE") -> dict[str, Any]:
    """Grade retained immutable captures against finalized canonical outcomes.

    Shared with other count-model SOG shadows through `model_identity` and
    `primary_source` (the `shadow_source` value of non-fallback rows).

    `grades` holds one entry per daily-run capture. `latest_pregame` grades each
    player-game once, from the latest run that captured it (runs only capture
    games that have not started), paired with production on the same rows.
    """
    outcome_path = Path(reconciliation_package) / "canonical_skater_outcomes.csv"
    if not outcome_path.is_file():
        return {"status": "NO_FINAL_OUTCOMES"}
    outcomes = pd.read_csv(outcome_path)
    outcomes = outcomes[outcomes.slate_date.astype(str).eq(slate_date)]
    if "participation_state" in outcomes:
        outcomes = outcomes[outcomes.participation_state.eq("PARTICIPATED")]
    if "official_final" in outcomes:
        outcomes = outcomes[outcomes.official_final.astype(str).str.lower().eq("true")]
    value = "shots_on_goal" if "shots_on_goal" in outcomes else "official_sog"
    outcome_sha = sha256_file(outcome_path)
    grades: list[dict[str, Any]] = []
    settled_parts: list[pd.DataFrame] = []
    for package in sorted(Path(capture_root).glob(f"season=*/slate_date={slate_date}/run_id=*/model={model_identity}")):
        verify_package(package)
        manifest = json.loads((package / "manifest.json").read_text())
        pred = pd.read_csv(package / "predictions.csv")
        joined = pred.merge(outcomes[["game_id", "player_id", value]], on=["game_id", "player_id"],
                            how="left", validate="many_to_one")
        y_count = pd.to_numeric(joined[value], errors="coerce")
        settled = y_count.notna() & y_count.ge(0)
        rows = joined[settled].copy()
        rows["capture_run_id"] = manifest["parent_daily_run_id"]
        settled_parts.append(rows)
        grades.append({"model_identity": model_identity, "research_role": "PROSPECTIVE_SHADOW",
                       "slate_date": slate_date, "run_id": manifest["parent_daily_run_id"],
                       "prediction_sha256": manifest["prediction_sha256"], "outcome_sha256": outcome_sha,
                       "unsettled_line_rows": int((~settled).sum()), **_grade_rows(rows, value, primary_source)})
    result: dict[str, Any] = {"status": "COMPLETE" if grades else "NO_IMMUTABLE_SHADOW_CAPTURE",
                              "role": "PROSPECTIVE_SHADOW", "model_identity": model_identity, "grades": grades}
    if settled_parts:
        combined = pd.concat(settled_parts, ignore_index=True)
        latest = combined.groupby(["game_id", "player_id"]).capture_run_id.transform("max")
        selected = combined[combined.capture_run_id.eq(latest)]
        result["latest_pregame"] = {"selection": "LATEST_CAPTURE_RUN_PER_PLAYER_GAME",
                                    "capture_runs": sorted(set(selected.capture_run_id)),
                                    **_grade_rows(selected, value, primary_source)}
    return result
