#!/usr/bin/env python3
"""Points trained on listed player-games (NHL Backlog P2.55, 2026-11-16).

Research only, read-only. Production 2f0824cf is calibrated on every player-game but about 3
points low on the ones the books list (2025-26: model 46.8%, actual 49.7%), the only ones that
can be bet. Listing history exists from 2025-10 (7 pm ET snapshots), so the listed training
population is 2025-26. Same recipe (goals + assists, production features and hyperparameters).

1. Time split within 2025-26: fit on player-games before 2026-02-01, score listed player-games
   from 2026-02-01 on. LISTED_EARLY (listed rows only) vs ALL25_EARLY (every 2025-26 row, to
   separate "listed population" from "recent season") vs MIXED (2023-24 + 2024-25 all rows plus
   2025-26 listed rows before the split) vs production.
2. Live: LISTED_2025 (all 2025-26 listed rows) and MIXED_2025 scored on 2026-27 player-games
   with a two-sided Points market (11-01..11-15), against production and the market; and the
   qualified Points Under 0.5 plays at 1 PM re-qualified with each model.
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
from backend.nhl.scripts import research_nhl_points_listed_players as lp  # noqa: E402
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_listed_training_2026-11-16"
REBUILT = ROOT / "artifacts/analysis/nhl/points_training_rebuild_2026-11-16"
QV = ROOT / "artifacts/analysis/nhl"
KEYS = ["game_id", "player_id"]
SPLIT = "2026-02-01"
LINES = (0.5, 1.5, 2.5)


def paired(frame: pd.DataFrame, a: str, b: str) -> dict:
    out = {}
    for line in LINES:
        y = (frame.realized_points > line).astype(float).to_numpy()
        r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), frame[a].to_numpy()), y),
                                  base.log_loss_rows(poisson.sf(int(line), frame[b].to_numpy()), y),
                                  frame.slate_date.astype(str).to_numpy())
        out[str(line)] = {"diff_x1000": round(r["mean_diff"] * 1e3, 2), "ci": [round(c * 1e3, 2) for c in r["ci95"]]}
    return out


def listed_2025() -> pd.DataFrame:
    import os
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    s25 = pd.read_csv(REBUILT / "season_frame_2025.csv.gz", float_precision="round_trip")
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SELECT player_id::bigint, full_name FROM nhl.players")
        names = dict(cur.fetchall())
    s25["who"] = s25.player_id.map(names).map(lp.norm)
    lst = lp.listed()
    s25 = s25[s25.slate_date.astype(str).isin(set(lst.slate_date))].copy()
    s25["slate_date"] = s25.slate_date.astype(str)
    s25 = s25.merge(lst[["slate_date", "who", "books"]], on=["slate_date", "who"], how="left")
    s25["listed"] = s25.books.notna()
    return s25.drop(columns=["who", "books"])


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    prod = joblib.load(scorer.MODEL_PATH)
    train2 = pd.read_csv(REBUILT / "training_frame_2023_2024.csv.gz", float_precision="round_trip")
    s25 = listed_2025()
    early, late = s25[s25.slate_date < SPLIT], s25[s25.slate_date >= SPLIT]
    test = late[late.listed].copy()
    specs = {"LISTED_EARLY": early[early.listed], "ALL25_EARLY": early,
             "MIXED_EARLY": pd.concat([train2, early[early.listed]], ignore_index=True)}
    test["PRODUCTION"] = scorer.score(test, prod).expected_points.to_numpy()
    for k, tr in specs.items():
        test[k] = scorer.score(test, freeze.fit_pair(tr.reset_index(drop=True))).expected_points.to_numpy()
    split = {"split": SPLIT, "train_rows": {k: int(len(v)) for k, v in specs.items()},
             "test_listed_rows": int(len(test)), "test_slates": int(test.slate_date.nunique()),
             "level": {"actual": float(test.realized_points.mean()), **{k: float(test[k].mean()) for k in ["PRODUCTION", *specs]}},
             "p1_level": {"actual": float((test.realized_points >= 1).mean()),
                          **{k: float(poisson.sf(0, test[k]).mean()) for k in ["PRODUCTION", *specs]}},
             "paired_vs_production_x1000": {k: paired(test, k, "PRODUCTION") for k in specs}}
    # Live 2026-27: player-games with a two-sided Points market are the listed ones.
    full = {"LISTED_2025": s25[s25.listed], "MIXED_2025": pd.concat([train2, s25[s25.listed]], ignore_index=True)}
    models = {k: freeze.fit_pair(v.reset_index(drop=True)) for k, v in full.items()}
    live = ms.frames()["live"].copy()
    live["slate_date"] = live.game_date.astype(str)
    priced = pd.read_csv(QV / "qualified_plays_vs_market_2026-11-16_close" / "priced_rows.csv.gz")
    p05 = priced[priced.lane.eq("points") & priced.line.eq(0.5) & priced.side.eq("Over")][KEYS + ["mkt_over"]]
    lv = live.merge(p05, on=KEYS, how="inner")
    lv["PRODUCTION"] = scorer.score(lv, prod).expected_points.to_numpy()
    for k, m in models.items():
        lv[k] = scorer.score(lv, m).expected_points.to_numpy()
    y = (lv.realized_points >= 1).astype(float).to_numpy()
    names = ["PRODUCTION", *models]
    live_out = {"rows": int(len(lv)), "slates": int(lv.slate_date.nunique()),
                "p1": {"actual": float(y.mean()), "market": float(lv.mkt_over.mean()),
                       **{k: float(poisson.sf(0, lv[k]).mean()) for k in names}},
                "log_loss_0_5": {"market": float(base.log_loss_rows(lv.mkt_over.to_numpy(), y).mean()),
                                 **{k: float(base.log_loss_rows(poisson.sf(0, lv[k].to_numpy()), y).mean()) for k in names}},
                "paired_vs_production_x1000": {k: paired(lv, k, "PRODUCTION") for k in models}}
    lv["band"] = pd.qcut(lv.own_rate_k30.rank(method="first"), 5, labels=["Q1 low", "Q2", "Q3", "Q4", "Q5 high"])
    bands = lv.groupby("band", observed=True).apply(
        lambda g: pd.Series({"n": len(g), "actual": (g.realized_points >= 1).mean(), "market": g.mkt_over.mean(),
                             **{k: poisson.sf(0, g[k]).mean() for k in names}}), include_groups=False).round(3)
    bands.to_csv(OUT / "live_by_scoring_band.csv")
    pr = pd.read_csv(QV / "qualified_plays_vs_market_2026-11-16" / "priced_rows.csv.gz")
    pr = pr[pr.lane.eq("points") & pr.line.eq(0.5)][KEYS + ["side", "decimal", "mkt_p", "won"]]
    pr = pr.merge(lv[KEYS + names], on=KEYS, how="inner")
    q = []
    for k in names:
        for side, lo, hi, plo, phi in (("Under", 0.04, 0.10, 0.5714, 0.75), ("Over", 0.05, 1.0, 0.5714, 0.8333)):
            s = pr[pr.side.eq(side)]
            p_over = poisson.sf(0, s[k].to_numpy())
            p = p_over if side == "Over" else 1 - p_over
            e = p - 1 / s.decimal
            sel = s[(e >= lo) & (e <= hi) & (p >= plo) & (p <= phi)]
            ps = p[(e >= lo) & (e <= hi) & (p >= plo) & (p <= phi)]
            q.append({"model": k, "search": f"Points {side} 0.5", "plays": len(sel), "won": float(sel.won.mean()) if len(sel) else None,
                      "model_p": float(ps.mean()) if len(sel) else None, "market": float(sel.mkt_p.mean()) if len(sel) else None,
                      "roi": float(np.where(sel.won, sel.decimal - 1, -1.0).mean()) if len(sel) else None})
    qual = pd.DataFrame(q)
    qual.to_csv(OUT / "qualified_searches_1pm.csv", index=False)
    joblib.dump(models, OUT / "candidates.joblib")
    summary = {"time_split_2025": split, "live_2026": live_out}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    print(json.dumps(summary, indent=1, default=str))
    print(bands.to_string())
    print(qual.round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
