#!/usr/bin/env python3
"""Points: a lift for listed player-games (NHL Backlog P2.55, 2026-11-16). MEASUREMENT ONLY.

This is a calibration layer (promotion rules route C): it may not enter any model without the
operator's approval. Production 2f0824cf is about 3 points low on player-games the books list.
The lift multiplies expected points of listed player-games by one factor c (keeps the Poisson
ladder coherent at every line), fitted by Poisson log loss on graded listed rows strictly
before the evaluation window:

  1. 2025-26 time split: c fitted on listed rows before 2026-02-01, scored on listed rows after.
  2. Live 2026-27 (11-01..11-15): c fitted on every 2025-26 listed row, scored on player-games
     with a two-sided Points market, against production and the market, by scoring-rate band,
     and on the qualified Points 0.5 searches at 1 PM.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_listed_training as lt  # noqa: E402
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_listed_lift_2026-11-16"
QV = ROOT / "artifacts/analysis/nhl"
KEYS = ["game_id", "player_id"]


def fit_c(mu: np.ndarray, y: np.ndarray) -> float:
    r = minimize_scalar(lambda c: -poisson.logpmf(y, mu * c).mean(), bounds=(0.8, 1.3), method="bounded")
    return float(r.x)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    prod = joblib.load(scorer.MODEL_PATH)
    s25 = lt.listed_2025()
    s25["PRODUCTION"] = scorer.score(s25, prod).expected_points.to_numpy()
    L = s25[s25.listed]
    early, late = L[L.slate_date < lt.SPLIT].copy(), L[L.slate_date >= lt.SPLIT].copy()
    c_early = fit_c(early.PRODUCTION.to_numpy(), early.realized_points.to_numpy().astype(int))
    late["LIFT"] = late.PRODUCTION * c_early
    split = {"c_fitted_before_split": c_early, "train_rows": int(len(early)), "test_rows": int(len(late)),
             "p1": {"actual": float((late.realized_points >= 1).mean()),
                    "PRODUCTION": float(poisson.sf(0, late.PRODUCTION).mean()), "LIFT": float(poisson.sf(0, late.LIFT).mean())},
             "paired_vs_production_x1000": lt.paired(late, "LIFT", "PRODUCTION")}
    c_all = fit_c(L.PRODUCTION.to_numpy(), L.realized_points.to_numpy().astype(int))
    live = ms.frames()["live"].copy()
    live["slate_date"] = live.game_date.astype(str)
    priced = pd.read_csv(QV / "qualified_plays_vs_market_2026-11-16_close" / "priced_rows.csv.gz")
    p05 = priced[priced.lane.eq("points") & priced.line.eq(0.5) & priced.side.eq("Over")][KEYS + ["mkt_over"]]
    lv = live.merge(p05, on=KEYS, how="inner")
    lv["PRODUCTION"] = scorer.score(lv, prod).expected_points.to_numpy()
    lv["LIFT"] = lv.PRODUCTION * c_all
    y = (lv.realized_points >= 1).astype(float).to_numpy()
    live_out = {"c_fitted_on_2025_26_listed": c_all, "rows": int(len(lv)), "slates": int(lv.slate_date.nunique()),
                "p1": {"actual": float(y.mean()), "market": float(lv.mkt_over.mean()),
                       "PRODUCTION": float(poisson.sf(0, lv.PRODUCTION).mean()), "LIFT": float(poisson.sf(0, lv.LIFT).mean())},
                "log_loss_0_5": {"market": float(base.log_loss_rows(lv.mkt_over.to_numpy(), y).mean()),
                                 "PRODUCTION": float(base.log_loss_rows(poisson.sf(0, lv.PRODUCTION.to_numpy()), y).mean()),
                                 "LIFT": float(base.log_loss_rows(poisson.sf(0, lv.LIFT.to_numpy()), y).mean())},
                "paired_vs_production_x1000": lt.paired(lv, "LIFT", "PRODUCTION")}
    lv["band"] = pd.qcut(lv.own_rate_k30.rank(method="first"), 5, labels=["Q1 low", "Q2", "Q3", "Q4", "Q5 high"])
    bands = lv.groupby("band", observed=True).apply(
        lambda g: pd.Series({"n": len(g), "actual": (g.realized_points >= 1).mean(), "market": g.mkt_over.mean(),
                             "PRODUCTION": poisson.sf(0, g.PRODUCTION).mean(), "LIFT": poisson.sf(0, g.LIFT).mean()}),
        include_groups=False).round(3)
    bands.to_csv(OUT / "live_by_scoring_band.csv")
    pr = pd.read_csv(QV / "qualified_plays_vs_market_2026-11-16" / "priced_rows.csv.gz")
    pr = pr[pr.lane.eq("points") & pr.line.eq(0.5)][KEYS + ["side", "decimal", "mkt_p", "won"]]
    pr = pr.merge(lv[KEYS + ["PRODUCTION", "LIFT"]], on=KEYS, how="inner")
    q = []
    for k in ("PRODUCTION", "LIFT"):
        for side, lo, hi, plo, phi in (("Under", 0.04, 0.10, 0.5714, 0.75), ("Over", 0.05, 1.0, 0.5714, 0.8333)):
            s = pr[pr.side.eq(side)]
            p_over = poisson.sf(0, s[k].to_numpy())
            p = p_over if side == "Over" else 1 - p_over
            e = p - 1 / s.decimal.to_numpy()
            keep = (e >= lo) & (e <= hi) & (p >= plo) & (p <= phi)
            sel = s[keep]
            q.append({"model": k, "search": f"Points {side} 0.5", "plays": int(keep.sum()),
                      "won": float(sel.won.mean()) if len(sel) else None, "model_p": float(p[keep].mean()) if len(sel) else None,
                      "market": float(sel.mkt_p.mean()) if len(sel) else None,
                      "roi": float(np.where(sel.won, sel.decimal - 1, -1.0).mean()) if len(sel) else None})
    qual = pd.DataFrame(q)
    qual.to_csv(OUT / "qualified_searches_1pm.csv", index=False)
    summary = {"status": "MEASUREMENT_ONLY_CALIBRATION_NOT_APPROVED", "time_split_2025": split, "live_2026": live_out}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps(summary, indent=1))
    print(bands.to_string())
    print(qual.round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
