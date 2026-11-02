#!/usr/bin/env python3
"""Live parity for the frozen Points goals + assists model (P2.39).

Runs the production Points input step (``points_hgb_shadow.prepare_authoritative_inputs``,
research login, read-only) for a slate, builds features with the production
exporter code (``export_nhl_points_hgb_features``) with the frozen contract
selected in this process only, and scores the frozen model and production.

- ``--replay-date``: a completed slate; component inputs and expected points
  from the live path must equal the freeze path (official outcomes through
  ``points_component_features``) for the same player-games.
- ``--slate-date``: today's slate; validity, ladder, side flips vs the PP-corrected
  refit it replaced (production until 2026-11-01, now the rollback),
  levels for stars and defensemen.
Writes only under ``--out``; read-only.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import joblib
import numpy as np
import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_component_features as pcf  # noqa: E402
from backend.nhl import points_hgb_shadow as hgb  # noqa: E402
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.scripts import export_nhl_points_hgb_features as exporter  # noqa: E402
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402
from backend.nhl.scripts.run_nhl_points_hgb_pp_corrected_refit import research_env  # noqa: E402

FROZEN_CONTRACT = freeze.OUT / "shadow_contract.json"
KEYS = ["game_id", "player_id"]


def live_features(slate_date: str, asof_utc: str, work: Path) -> pd.DataFrame:
    url = database_url("research", environ=research_env())
    with psycopg.connect(url, prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("""SELECT game_id, start_time_utc, home_team_id, away_team_id FROM nhl.games
                       WHERE game_date = %s AND substring(game_id::text,5,2)='02'""", (slate_date,))
        games = [SimpleNamespace(game_id=g, start_time_utc=s.isoformat(), home_team_id=h, away_team_id=a)
                 for g, s, h, a in cur.fetchall()]
    logs, outcomes, slate, _ = hgb.prepare_authoritative_inputs(
        db_url=url, canonical_games=games, slate_date=slate_date, phoenix_predictions=None,
        asof_utc=asof_utc, work_dir=work)
    history = exporter.normalize_history(pd.read_csv(logs), pd.concat([pd.read_csv(p) for p in outcomes]))
    season = int(slate_date[:4]) if int(slate_date[5:7]) >= 7 else int(slate_date[:4]) - 1
    frame = exporter.normalize_slate(pd.read_csv(slate), season)
    with mock.patch.object(scorer, "CONTRACT_PATH", FROZEN_CONTRACT), \
            mock.patch.object(exporter, "FEATURES", list(freeze.FEATURES)):
        return exporter.build_features(history, frame)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--slate-date", required=True)
    ap.add_argument("--asof-utc", required=True)
    ap.add_argument("--replay-date")
    ap.add_argument("--replay-asof-utc")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    model = joblib.load(freeze.OUT / freeze.ARTIFACT)
    # The model it replaced (now the rollback), so the comparison holds after cutover.
    previous = json.loads(scorer.PP_CORRECTED_CONTRACT_PATH.read_text())
    production = joblib.load(scorer.PP_CORRECTED_CONTRACT_PATH.parent / previous["fitted_artifact"])
    report = {"model_identity_sha256": json.loads(FROZEN_CONTRACT.read_text())["model_identity_sha256"]}
    if a.replay_date:
        live = live_features(a.replay_date, a.replay_asof_utc, a.out / f"work_{a.replay_date}")
        outcomes, prior, league = freeze.official()
        targets = live[KEYS + ["game_date"]].assign(season=int(a.replay_date[:4]))
        ref = pcf.add_features(targets, outcomes[outcomes.game_date.astype(str) < a.replay_date], prior, league)
        cmp_ = live.merge(ref[KEYS + pcf.FEATURES], on=KEYS, suffixes=("_live", "_freeze"))
        diffs = {f: float((cmp_[f"{f}_live"] - cmp_[f"{f}_freeze"]).abs().max()) for f in pcf.FEATURES}
        report["replay"] = {"date": a.replay_date, "player_games": int(len(cmp_)), "max_abs_diff_component_inputs": diffs}
    today = live_features(a.slate_date, a.asof_utc, a.out / f"work_{a.slate_date}")
    new, old = scorer.score(today, model), scorer.score(today, production)
    m = new.merge(old, on=KEYS, suffixes=("_new", "_old")).merge(
        today[KEYS + ["current_season_points_prior", "current_season_games_prior", "own_rate_k30"]], on=KEYS)
    ladder = bool(((m.prob_over_0_5_new >= m.prob_over_1_5_new) & (m.prob_over_1_5_new >= m.prob_over_2_5_new)).all())
    flips = {line: int(((m[f"prob_over_{line}_new"] >= .5) != (m[f"prob_over_{line}_old"] >= .5)).sum())
             for line in ("0_5", "1_5", "2_5")}
    m["ppg"] = m.current_season_points_prior / m.current_season_games_prior.replace(0, np.nan)
    levels = m.groupby(pd.cut(m.ppg, [-1, .5, .8, 1.2, 5]), observed=True)[["expected_points_old", "expected_points_new"]].mean()
    report["today"] = {"slate_date": a.slate_date, "player_games": int(len(m)),
                       "invalid": int((~np.isfinite(m.expected_points_new)).sum()),
                       "missing_component_inputs": int(today[pcf.FEATURES].isna().any(axis=1).sum()),
                       "ladder_monotone": ladder, "side_flips_vs_production": flips,
                       "mean_expected": {"production": float(m.expected_points_old.mean()), "new": float(m.expected_points_new.mean())},
                       "by_points_per_game": {str(k): {c: round(float(v), 3) for c, v in r.items()} for k, r in levels.iterrows()}}
    m.to_csv(a.out / f"parity_{a.slate_date}.csv", index=False)
    (a.out / f"parity_{a.slate_date}.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=1))
    top = m.assign(delta=m.expected_points_new - m.expected_points_old).sort_values("delta")
    print(top[KEYS + ["ppg", "expected_points_old", "expected_points_new"]].head(5).round(3).to_string(index=False))
    print(top[KEYS + ["ppg", "expected_points_old", "expected_points_new"]].tail(5).round(3).to_string(index=False))


if __name__ == "__main__":
    main()
