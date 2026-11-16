#!/usr/bin/env python3
"""Points: stronger pull of thin-history players toward the league rate (NHL Backlog P2.55, 2026-11-16).

Research only, read-only. Production 2f0824cf blends each player's points, goals and assists
rates with K = 30 games of the previous season's league rate
(`points_component_features.add_features`). On depth players the model is 4-5 points below the
market and the market matches the result (P2.55). K = 10 was no better (P2.46). This rebuilds
the three rates with K = 60 and K = 120 from the stored K = 30 values
(total = rate x (games + 30) - 30 x league; rate_K = (total + K x league) / (games + K)),
refits the production recipe on the rebuilt frames (Utah included), and compares with
production on the 2025-26 holdout, live 2026-11-01..11-15, Points 0.5 against the morning and
closing market by scoring-rate quintile, and the qualified Points Under 0.5 plays at 1 PM.
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
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms  # noqa: E402
from backend.nhl.scripts import research_nhl_points_per_minute_corrected as pmc  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_rate_shrinkage_2026-11-16"
REBUILT = ROOT / "artifacts/analysis/nhl/points_training_rebuild_2026-11-16"
QV = ROOT / "artifacts/analysis/nhl"
KEYS = ["game_id", "player_id"]
RATES = {"own_rate_k30": "points", "goal_rate": "goals", "assist_rate": "assists"}
K_GRID = (60, 120)


def reshrink(frame: pd.DataFrame, league: pd.DataFrame, k: int) -> pd.DataFrame:
    f = frame.copy()
    season = pd.to_numeric(f["season"] if "season" in f else pmc.season_of(f.game_date), errors="coerce").astype(int) - 1
    n = pd.to_numeric(f.own_games, errors="coerce").astype(float)
    for col, rate in RATES.items():
        mu = season.map(league[rate])
        total = f[col] * (n + 30) - 30 * mu
        f[col] = (total + k * mu) / (n + k)
    return f


def market(kind: str) -> pd.DataFrame:
    folder = "qualified_plays_vs_market_2026-11-16" + ("" if kind == "1pm" else f"_{kind}")
    return pd.read_csv(QV / folder / "priced_rows.csv.gz")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    contract = json.loads(scorer.CONTRACT_PATH.read_text())
    league = pd.read_csv(scorer.CONTRACT_PATH.parent / contract["league_rates"]).set_index("season")
    prod = joblib.load(scorer.MODEL_PATH)
    train = pd.read_csv(REBUILT / "training_frame_2023_2024.csv.gz", float_precision="round_trip")
    hold = pd.read_csv(REBUILT / "season_frame_2025.csv.gz", float_precision="round_trip")
    live = ms.frames()["live"].copy()
    live["slate_date"] = live.game_date.astype(str)
    live["season"] = 2026
    # Check the inversion: K = 30 reproduces the stored rates exactly.
    check = float(np.nanmax(np.abs(reshrink(hold, league, 30)[list(RATES)].to_numpy() - hold[list(RATES)].to_numpy())))
    names = ["PRODUCTION"] + [f"K{k}" for k in K_GRID]
    hold["PRODUCTION"] = scorer.score(hold, prod).expected_points.to_numpy()
    live["PRODUCTION"] = scorer.score(live, prod).expected_points.to_numpy()
    for k in K_GRID:
        m = freeze.fit_pair(reshrink(train, league, k))
        hold[f"K{k}"] = scorer.score(reshrink(hold, league, k), m).expected_points.to_numpy()
        live[f"K{k}"] = scorer.score(reshrink(live, league, k), m).expected_points.to_numpy()
    summary = {"inversion_check_max_abs": check,
               "mean": {"holdout": {"actual": float(hold.realized_points.mean()), **{c: float(hold[c].mean()) for c in names}},
                        "live": {"actual": float(live.realized_points.mean()), **{c: float(live[c].mean()) for c in names}}}}
    hold["role_change"] = hold["role_up"] = hold["role_down"] = False
    tabs = []
    for part, frame in (("holdout_2025", hold), ("live", live)):
        frame["own_q"] = pd.qcut(frame.own_rate_k30.rank(method="first"), 5, labels=False)
        masks = {"all": np.ones(len(frame), bool), "own_rate_Q1_low": frame.own_q.eq(0).to_numpy(),
                 "own_rate_Q5_high": frame.own_q.eq(4).to_numpy()}
        for c in names[1:]:
            tabs.append(pmc.paired(frame, c, "PRODUCTION", masks).assign(part=part, model=c))
    paired = pd.concat(tabs, ignore_index=True)[["part", "model", "group", "line", "n", "diff_x1000", "ci_x1000"]]
    paired.to_csv(OUT / "paired_vs_production.csv", index=False)
    # Points 0.5 against the morning and closing market, by scoring-rate quintile.
    rows = []
    for kind in ("morning", "close"):
        mk = market(kind)
        mk = mk[mk.lane.eq("points") & mk.line.eq(0.5) & mk.side.eq("Over")][KEYS + ["mkt_over"]]
        m = live.merge(mk, on=KEYS, how="inner")
        y = (m.realized_points >= 1).astype(float).to_numpy()
        m["q"] = pd.qcut(m.own_rate_k30.rank(method="first"), 5, labels=["Q1 low", "Q2", "Q3", "Q4", "Q5 high"])
        for q, g in [("all", m)] + list(m.groupby("q", observed=True)):
            yy = (g.realized_points >= 1).astype(float).to_numpy()
            row = {"market_time": kind, "scoring_rate_band": q, "n": len(g), "actual": float(yy.mean()),
                   "market": float(g.mkt_over.mean()), "market_ll": float(base.log_loss_rows(g.mkt_over.to_numpy(), yy).mean())}
            for c in names:
                p = poisson.sf(0, g[c].to_numpy())
                row[c] = float(p.mean()); row[f"{c}_ll"] = float(base.log_loss_rows(p, yy).mean())
            rows.append(row)
    vs_market = pd.DataFrame(rows)
    vs_market.to_csv(OUT / "points_0_5_vs_market_by_scoring_rate.csv", index=False)
    # Qualified Points Under 0.5 plays at 1 PM, re-qualified with each model's own probability.
    pr = market("1pm")
    pr = pr[pr.lane.eq("points") & pr.line.eq(0.5) & pr.side.eq("Under")][KEYS + ["decimal", "mkt_p", "won"]]
    pr = pr.merge(live[KEYS + names], on=KEYS, how="inner")
    q_rows = []
    for c in names:
        p_under = poisson.pmf(0, pr[c].to_numpy())
        edge = p_under - 1 / pr.decimal
        sel = pr[(edge >= 0.04) & (edge <= 0.10) & (p_under >= 0.5714) & (p_under <= 0.75)]
        pu = poisson.pmf(0, sel[c].to_numpy())
        q_rows.append({"model": c, "plays": len(sel), "per_slate": len(sel) / live.slate_date.nunique(),
                       "won": float(sel.won.mean()), "model_p": float(pu.mean()), "market": float(sel.mkt_p.mean()),
                       "price": float((1 / sel.decimal).mean()),
                       "roi": float(np.where(sel.won, sel.decimal - 1, -1.0).mean())})
    qual = pd.DataFrame(q_rows)
    qual.to_csv(OUT / "qualified_points_under_1pm.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary, indent=1))
    print(paired.to_string(index=False))
    print(vs_market.round(4).to_string(index=False))
    print(qual.round(4).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
