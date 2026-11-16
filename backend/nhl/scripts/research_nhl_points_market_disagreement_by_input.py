#!/usr/bin/env python3
"""What the Points market weighs differently from the model, by input (NHL Backlog P2.55, 2026-11-16).

Research only, read-only. Every Points player-game the production model (c940ae2f, the model
live 11-01..11-15; its live inputs from the latest pregame capture) scored with a two-sided
market at Points 0.5. Model and market are P(1+ point); the market is the other books' median
no-vig price, taken in the morning (quotes up to 06:30 PT, as the 05:45 run saw them) and, for
more rows, near the close (the market barely moves between them, P2.55).

For each input: quintiles with the mean model, market and actual rate, the model-minus-market
difference and who was closer (log loss). A difference that grows across an input's range is
something the books weigh differently; whether the market or the model is right there says
whether it is a model shortcoming. Also by position.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_market_disagreement_by_input_2026-11-16"
QV = ROOT / "artifacts/analysis/nhl"
KEYS = ["game_id", "player_id"]


def market_rows(kind: str) -> pd.DataFrame:
    folder = "qualified_plays_vs_market_2026-11-16" + ("" if kind == "1pm" else f"_{kind}")
    r = pd.read_csv(QV / folder / "priced_rows.csv.gz")
    r = r[r.lane.eq("points") & r.line.eq(0.5) & r.side.eq("Over")]
    return r[KEYS + ["game_date", "mkt_over"]].rename(columns={"mkt_over": f"market_{kind}"})


def ll(p, y) -> float:
    return float(base.log_loss_rows(np.clip(p, 1e-6, 1 - 1e-6), y).mean())


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    live = ms.frames()["live"].copy()
    from scipy.stats import poisson
    from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
    prod = __import__("joblib").load(scorer.PP_STATS_API_CONTRACT_PATH.parent
                                     / json.loads(scorer.PP_STATS_API_CONTRACT_PATH.read_text())["fitted_artifact"])
    live["model"] = poisson.sf(0, scorer.score(live, prod).expected_points.to_numpy())
    live["actual"] = (live.realized_points >= 1).astype(float)
    pos = base.positions() if hasattr(base, "positions") else None
    if pos is not None:
        live = live.drop(columns=["is_defense"], errors="ignore").merge(pos, on="player_id", how="left")
    out = {}
    summaries = []
    for kind in ("morning", "close"):
        m = live.merge(market_rows(kind), on=KEYS, how="inner").rename(columns={f"market_{kind}": "market"})
        m["diff"] = m.model - m.market
        out[kind] = {"rows": int(len(m)), "slates": int(m.game_date_x.nunique() if "game_date_x" in m else m.slate_date.nunique()),
                     "model": float(m.model.mean()), "market": float(m.market.mean()), "actual": float(m.actual.mean()),
                     "ll_model": ll(m.model, m.actual), "ll_market": ll(m.market, m.actual)}
        feats = list(freeze.FEATURES) + (["is_defense"] if "is_defense" in m else [])
        for f in feats:
            v = pd.to_numeric(m[f], errors="coerce")
            if v.nunique() <= 2:
                bands = v.fillna(-1).astype(int).astype(str)
            else:
                bands = pd.qcut(v.rank(method="first"), 5, labels=["Q1 low", "Q2", "Q3", "Q4", "Q5 high"])
            for b, g in m.groupby(bands, observed=True):
                summaries.append({"market_time": kind, "input": f, "band": str(b), "n": len(g),
                                  "input_mean": float(pd.to_numeric(g[f], errors="coerce").mean()),
                                  "model": g.model.mean(), "market": g.market.mean(), "actual": g.actual.mean(),
                                  "model_minus_market": g["diff"].mean(),
                                  "ll_model": ll(g.model, g.actual), "ll_market": ll(g.market, g.actual)})
        # Which inputs move the difference most: slope of diff across the input's quintiles.
        m.to_csv(OUT / f"rows_{kind}.csv.gz", index=False)
    s = pd.DataFrame(summaries)
    s["closer"] = np.where(s.ll_market < s.ll_model, "market", "model")
    s.round(4).to_csv(OUT / "by_input_quintile.csv", index=False)
    spread = (s[s.market_time.eq("close")].groupby("input").model_minus_market.agg(lambda x: x.max() - x.min())
              .sort_values(ascending=False).round(4))
    spread.to_csv(OUT / "difference_spread_by_input.csv")
    (OUT / "summary.json").write_text(json.dumps(out, indent=1) + "\n")
    pd.set_option("display.width", 250); pd.set_option("display.max_rows", 400)
    print(json.dumps(out, indent=1))
    print(spread.to_string())
    top = list(spread.index[:7])
    t = s[s.market_time.eq("close") & s.input.isin(top)].copy()
    t["input"] = pd.Categorical(t.input, top)
    print(t.sort_values(["input", "band"])[["input", "band", "n", "input_mean", "model", "market", "actual",
                                            "model_minus_market", "closer"]].round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
