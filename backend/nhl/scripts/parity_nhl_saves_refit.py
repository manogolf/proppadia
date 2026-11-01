#!/usr/bin/env python3
"""Live parity for the frozen Saves refit (P2.41): score a slate's production goalies.

Reads history strictly before the slate from the database (research login,
read-only, one snapshot): regular-season skater logs and goalie logs for the
slate's season and the one before. Scores the same goalie population as the
production Saves input of the given daily run and compares with its
predictions. ``--replay-date`` also re-scores a completed slate's starters and
compares with the freeze-path holdout predictions (same model, history built
by the freeze path), which checks that live and training features agree.
Writes only under ``--out``. Production is unchanged.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import saves_refit_model as srm  # noqa: E402
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.scripts.run_nhl_points_hgb_pp_corrected_refit import research_env  # noqa: E402

FROZEN = ROOT / "artifacts/analysis/nhl/saves_refit_2026-11-01/frozen_2026-11-01"


def history(slate_date: str, season: int) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    with psycopg.connect(database_url("research", environ=research_env()), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("SELECT transaction_timestamp()")
        snap = cur.fetchone()[0].isoformat()
        cur.execute("""SELECT g.season, g.game_date, l.game_id::bigint, l.team_id, g.home_team_id, g.away_team_id,
                              l.shots_on_goal
                       FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING(game_id)
                       WHERE g.season IN (%s, %s) AND substring(g.game_id::text,5,2) = '02' AND g.game_date < %s""",
                    (season - 1, season, slate_date))
        logs = pd.DataFrame(cur.fetchall(), columns=["season", "game_date", "game_id", "team_id", "home_team_id",
                                                     "away_team_id", "shots_on_goal"])
        cur.execute("""SELECT g.season, l.game_id::bigint, l.player_id::bigint, l.team_id, g.game_date,
                              l.toi_minutes, l.saves, l.shots_faced, l.goals_allowed, l.start_flag
                       FROM nhl.goalie_game_logs_raw l JOIN nhl.games g USING(game_id)
                       WHERE g.season IN (%s, %s) AND substring(g.game_id::text,5,2) = '02' AND g.game_date < %s""",
                    (season - 1, season, slate_date))
        goalies = pd.DataFrame(cur.fetchall(), columns=["season", "game_id", "player_id", "team_id", "game_date", "toi",
                                                        "saves", "shots", "goals_allowed", "start_flag"])
    logs["shots_on_goal"] = pd.to_numeric(logs.shots_on_goal, errors="coerce")
    for col in ("toi", "saves", "shots", "goals_allowed"):
        goalies[col] = pd.to_numeric(goalies[col], errors="coerce")
    # As in the research loader: shots faced from saves + goals when only those are stored.
    goalies["shots"] = goalies.shots.fillna(goalies.saves + goalies.goals_allowed).where(goalies.toi > 0)
    return logs, goalies, snap


def score(model: dict, slate_date: str, season: int, targets: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """targets: player_id, game_id, team_id, home_team_id, away_team_id."""
    logs, goalies, snap = history(slate_date, season)
    games = targets[["game_id", "home_team_id", "away_team_id"]].drop_duplicates().assign(game_date=slate_date)
    g = srm.team_features(srm.add_targets(srm.team_games(logs), games, season))
    target_goalies = targets[["player_id", "game_id"]].assign(season=season, game_date=slate_date, toi=np.nan,
                                                              saves=np.nan, shots=np.nan)
    rates = srm.save_rates(pd.concat([goalies, target_goalies], ignore_index=True))
    rows = targets.merge(rates[["player_id", "game_id", "save_rate", "goalie_shots_prior"]], on=["player_id", "game_id"],
                         how="left").merge(g.drop(columns=["season", "game_date"]), on=["game_id", "team_id"], how="left")
    rows["expected_saves"] = srm.expected_saves(model, rows)
    return pd.concat([rows.reset_index(drop=True), srm.prob_over(model, rows.expected_saves.to_numpy())], axis=1), snap


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--slate-date", required=True)
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--run-dir", type=Path, required=True, help="production daily run folder (saves_scoring_input.csv, saves_predictions.csv)")
    ap.add_argument("--replay-date", help="completed slate to replay against the freeze-path holdout")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    model = joblib.load(FROZEN / "nhl_saves_shots_x_save_rate_v1.joblib")
    contract = json.loads((FROZEN / "contract.json").read_text())
    inp = pd.read_csv(a.run_dir / "saves_scoring_input.csv")
    targets = inp[["player_id", "game_id", "team_id", "home_team_id", "away_team_id"]].astype("int64")
    scored, snap = score(model, a.slate_date, a.season, targets)
    prod = pd.read_csv(a.run_dir / "saves_predictions.csv")
    cols = [c for c in scored.columns if c.startswith("p_over_")]
    m = scored.merge(prod[["player_id", "game_id", *cols]], on=["player_id", "game_id"], suffixes=("_refit", "_prod"))
    ladder_ok = bool((np.diff(scored[cols].to_numpy(), axis=1) <= 1e-12).all())
    flips = {c.replace("p_over_", ""): int(((m[f"{c}_refit"] >= .5) != (m[f"{c}_prod"] >= .5)).sum()) for c in cols}
    report = {"slate_date": a.slate_date, "model_identity_sha256": contract["model_identity_sha256"],
              "database_snapshot_utc": snap, "goalies": int(len(scored)), "matched_to_production": int(len(m)),
              "invalid_expected": int((~np.isfinite(scored.expected_saves)).sum()),
              "missing_team_features": int(scored.sa_shr.isna().sum()), "missing_save_rate": int(scored.save_rate.isna().sum()),
              "ladder_monotone": ladder_ok, "side_flips_vs_production": flips,
              "expected_saves": {"mean": float(scored.expected_saves.mean()), "min": float(scored.expected_saves.min()),
                                 "max": float(scored.expected_saves.max())},
              "goalies_with_no_prior_shots": int((scored.goalie_shots_prior == 0).sum())}
    if a.replay_date:
        hold = pd.read_csv(FROZEN / "frozen_holdout_expected_saves.csv")
        st = pd.read_csv(FROZEN / "holdout_starters_2025_2026.csv.gz")
        day = st[pd.to_datetime(st.game_date).dt.strftime("%Y-%m-%d").eq(a.replay_date)]
        tg = day[["player_id", "game_id", "team_id"]].merge(
            targets.iloc[:0], how="left").astype({"player_id": "int64", "game_id": "int64", "team_id": "int64"})
        with psycopg.connect(database_url("research", environ=research_env())) as c, c.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute("SELECT game_id::bigint, home_team_id, away_team_id FROM nhl.games WHERE game_id = ANY(%s)",
                        (sorted(map(int, tg.game_id.unique())),))
            gm = pd.DataFrame(cur.fetchall(), columns=["game_id", "home_team_id", "away_team_id"])
        tg = tg[["player_id", "game_id", "team_id"]].merge(gm, on="game_id")
        replay, _ = score(model, a.replay_date, a.season, tg)
        cmp_ = replay.merge(hold, on=["game_id", "player_id"])
        report["replay"] = {"date": a.replay_date, "starters": int(len(cmp_)),
                            "max_abs_diff_expected_saves": float((cmp_.expected_saves_x - cmp_.expected_saves_y).abs().max()),
                            "mean_abs_diff": float((cmp_.expected_saves_x - cmp_.expected_saves_y).abs().mean())}
    m.to_csv(a.out / f"parity_{a.slate_date}.csv", index=False)
    (a.out / f"parity_{a.slate_date}.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=1))
    show = m[["player_id", "game_id", "expected_saves", "save_rate", "p_over_24_5_refit", "p_over_24_5_prod"]]
    print(show.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
