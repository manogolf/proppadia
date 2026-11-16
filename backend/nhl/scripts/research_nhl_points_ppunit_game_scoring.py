#!/usr/bin/env python3
"""Points: power-play unit and the game's expected scoring as inputs (NHL Backlog P2.46 / P2.55).

Research only, read-only. Both are public before the first daily run, used by the books and not
by the model (operator 2026-11-16: no information advantage at the first run; bring that
information into the model's perspective).

- PP unit, from the stored per-player PP TOI (corrected, P2.45): each team-game, players ranked
  by PP minutes; PP1 = the top five with PP time. Inputs, strict prior: pp1_last_game,
  pp1_share_last5 (share of his last five games on PP1), pp_rank_last_game.
- The game's expected scoring, from public team results (no historical game totals are stored):
  team goals for and the opponent's goals against per game over each team's last 10 games
  (strict prior), and exp_team_goals = their mean.

Candidates (goals + assists, production recipe, rebuilt frames with Utah): PRODUCTION
(2f0824cf), +PPU, +GAME, +BOTH. Scored on the 2025-26 holdout (all rows and listed rows),
live 2026-11-01..11-15, two-sided Points lines vs the market, and the qualified searches.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import rebuild_nhl_points_training_frames as rb  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_listed_training as lt  # noqa: E402
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms  # noqa: E402
from backend.nhl.scripts import research_nhl_points_per_minute_corrected as pmc  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_ppunit_game_scoring_2026-11-16"
REBUILT = ROOT / "artifacts/analysis/nhl/points_training_rebuild_2026-11-16"
QV = ROOT / "artifacts/analysis/nhl"
KEYS = ["game_id", "player_id"]
F = list(freeze.FEATURES)
PPU = ["pp1_last_game", "pp1_share_last5", "pp_rank_last_game"]
GAME = ["team_gf_l10", "opp_ga_l10", "exp_team_goals"]


def team_games() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Player-game PP unit and team-game goals, every stored regular-season game 2023-2026."""
    logs, _ = refit.pull_history()
    frozen = pd.read_csv(refit.FROZEN_FRAME, low_memory=False)
    frozen["game_id"] = frozen.game_id.astype("int64"); frozen["player_id"] = frozen.player_id.astype("int64")
    frozen = pd.concat([frozen, rb.utah_labels()], ignore_index=True)
    outcomes, _ = refit.official_outcomes(frozen)
    outcomes["game_id"] = outcomes.game_id.astype("int64"); outcomes["player_id"] = outcomes.player_id.astype("int64")
    pg = logs[KEYS + ["game_date", "team_id", "home_team_id", "away_team_id", "pp_toi_minutes"]].merge(
        outcomes[KEYS + ["goals"]], on=KEYS, how="left")
    pg["game_date"] = pd.to_datetime(pg.game_date)
    pp = pg.pp_toi_minutes.fillna(0)
    pg["pp_rank"] = pp.groupby([pg.game_id, pg.team_id]).rank(ascending=False, method="first")
    pg["pp1"] = ((pg.pp_rank <= 5) & (pp > 0)).astype(float)
    pg.loc[pp.eq(0), "pp_rank"] = 10.0
    tg = pg.groupby(["game_id", "team_id"]).agg(game_date=("game_date", "first"), gf=("goals", "sum"),
                                                home=("home_team_id", "first"), away=("away_team_id", "first")).reset_index()
    tg["opp"] = np.where(tg.team_id.eq(tg.home), tg.away, tg.home)
    ga = tg[["game_id", "team_id", "gf"]].rename(columns={"team_id": "opp", "gf": "ga"})
    tg = tg.merge(ga, on=["game_id", "opp"], how="left")
    return pg, tg


def rolling_team(tg: pd.DataFrame) -> pd.DataFrame:
    tg = tg.sort_values(["team_id", "game_date", "game_id"]).copy()
    g = tg.groupby("team_id")
    tg["gf_l10"] = g.gf.transform(lambda s: s.shift(1).rolling(10, min_periods=3).mean())
    tg["ga_l10"] = g.ga.transform(lambda s: s.shift(1).rolling(10, min_periods=3).mean())
    return tg


def inputs(targets: pd.DataFrame, pg: pd.DataFrame, tg: pd.DataFrame) -> pd.DataFrame:
    pg = pg.sort_values(["player_id", "game_date", "game_id"]).copy()
    g = pg.groupby("player_id")
    pg["pp1_last_game"] = g.pp1.shift(1)
    pg["pp_rank_last_game"] = g.pp_rank.shift(1)
    pg["pp1_share_last5"] = g.pp1.transform(lambda s: s.shift(1).rolling(5, min_periods=1).mean())
    tr = rolling_team(tg)
    me = tr[["game_id", "team_id", "gf_l10", "opp"]].rename(columns={"gf_l10": "team_gf_l10"})
    opp = tr[["game_id", "team_id", "ga_l10"]].rename(columns={"team_id": "opp", "ga_l10": "opp_ga_l10"})
    x = pg[KEYS + ["team_id"] + PPU].merge(me, on=["game_id", "team_id"], how="left").merge(opp, on=["game_id", "opp"], how="left")
    x["exp_team_goals"] = x[["team_gf_l10", "opp_ga_l10"]].mean(axis=1)
    return targets[KEYS].merge(x[KEYS + PPU + GAME], on=KEYS, how="left")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    cache = OUT / "inputs.csv.gz"
    train = pd.read_csv(REBUILT / "training_frame_2023_2024.csv.gz", float_precision="round_trip")
    s25 = lt.listed_2025()   # 2025-26 rows on dates with a 7 pm snapshot, with the listed flag
    full25 = pd.read_csv(REBUILT / "season_frame_2025.csv.gz", float_precision="round_trip")
    live = ms.frames()["live"].copy()
    live["slate_date"] = live.game_date.astype(str)
    if cache.is_file():
        x = pd.read_csv(cache)
    else:
        pg, tg = team_games()
        keys = pd.concat([d[KEYS] for d in (train, full25, live)]).drop_duplicates()
        x = inputs(keys, pg, tg)
        x.to_csv(cache, index=False, float_format="%.6g")
    train, full25, s25, live = (d.merge(x, on=KEYS, how="left") for d in (train, full25, s25, live))
    cov = {c: {"train": float(train[c].notna().mean()), "live": float(live[c].notna().mean())} for c in PPU + GAME}
    specs = {"PPU": F + PPU, "GAME": F + GAME, "BOTH": F + PPU + GAME}
    prod = joblib.load(scorer.MODEL_PATH)
    models = {}
    for k, feats in specs.items():
        g = freeze.research.fit_count(train, feats, "goals")
        a = freeze.research.fit_count(train, feats, "assists")
        models[k] = {"structure": "GOALS_PLUS_ASSISTS", "feature_columns": feats, "fill_zero": True,
                     "scaler": g["scaler"], "goals": g, "assists": a}
    names = ["PRODUCTION", *specs]
    for d in (full25, s25, live):
        d["PRODUCTION"] = scorer.score(d, prod).expected_points.to_numpy()
        for k, m in models.items():
            d[k] = scorer.score(d, m).expected_points.to_numpy()
    summary = {"coverage": cov, "rows": {"train": int(len(train)), "holdout": int(len(full25)), "live": int(len(live))}}
    tabs = []
    full25["role_change"] = full25["role_up"] = full25["role_down"] = False
    for part, d, masks in (("holdout_all", full25, {"all": np.ones(len(full25), bool)}),
                           ("holdout_listed", s25, {"listed": s25.listed.to_numpy(bool), "not_listed": ~s25.listed.to_numpy(bool)}),
                           ("live", live, {"all": np.ones(len(live), bool)})):
        for k in specs:
            tabs.append(pmc.paired(d, k, "PRODUCTION", masks).assign(part=part, model=k))
    paired = pd.concat(tabs, ignore_index=True)[["part", "model", "group", "line", "n", "diff_x1000", "ci_x1000"]]
    paired.to_csv(OUT / "paired_vs_production.csv", index=False)
    lv = s25[s25.listed]
    summary["listed_2025_p1"] = {"actual": float((lv.realized_points >= 1).mean()),
                                 **{k: float(poisson.sf(0, lv[k]).mean()) for k in names}}
    priced = pd.read_csv(QV / "qualified_plays_vs_market_2026-11-16_close" / "priced_rows.csv.gz")
    p05 = priced[priced.lane.eq("points") & priced.line.eq(0.5) & priced.side.eq("Over")][KEYS + ["mkt_over"]]
    mk = live.merge(p05, on=KEYS, how="inner")
    y = (mk.realized_points >= 1).astype(float).to_numpy()
    summary["live_two_sided_0_5"] = {"rows": int(len(mk)), "actual": float(y.mean()), "market": float(mk.mkt_over.mean()),
                                     "log_loss": {"market": float(base.log_loss_rows(mk.mkt_over.to_numpy(), y).mean()),
                                                  **{k: float(base.log_loss_rows(poisson.sf(0, mk[k].to_numpy()), y).mean()) for k in names}},
                                     "p1": {k: float(poisson.sf(0, mk[k]).mean()) for k in names}}
    pr = pd.read_csv(QV / "qualified_plays_vs_market_2026-11-16" / "priced_rows.csv.gz")
    pr = pr[pr.lane.eq("points") & pr.line.eq(0.5)][KEYS + ["side", "decimal", "mkt_p", "won"]].merge(
        live[KEYS + names], on=KEYS, how="inner")
    q = []
    for k in names:
        for side, lo, hi, plo, phi in (("Under", 0.04, 0.10, 0.5714, 0.75), ("Over", 0.05, 1.0, 0.5714, 0.8333)):
            s = pr[pr.side.eq(side)]
            p_over = poisson.sf(0, s[k].to_numpy()); p = p_over if side == "Over" else 1 - p_over
            e = p - 1 / s.decimal.to_numpy(); keep = (e >= lo) & (e <= hi) & (p >= plo) & (p <= phi); sel = s[keep]
            q.append({"model": k, "search": f"Points {side} 0.5", "plays": int(keep.sum()),
                      "won": float(sel.won.mean()) if len(sel) else None, "market": float(sel.mkt_p.mean()) if len(sel) else None,
                      "roi": float(np.where(sel.won, sel.decimal - 1, -1.0).mean()) if len(sel) else None})
    pd.DataFrame(q).to_csv(OUT / "qualified_searches_1pm.csv", index=False)
    joblib.dump(models, OUT / "candidates.joblib")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary, indent=1))
    print(paired.to_string(index=False))
    print(pd.DataFrame(q).round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
