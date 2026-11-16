#!/usr/bin/env python3
"""Do the current models' qualified plays beat the market consensus? Research only (NHL Backlog P2.55).

Read-only. The operator's betting history (2024-01..2026-11) won about what the other
books' no-vig price said, while the model's fair odds showed 10-13 point edges. This asks
the same of the current production models: SOG `NHL_SOG_COUNT_HGB_V2` (rescored
2026-10-25..11-14, P2.51 live_rows.csv) and Points `c940ae2f` (2026-11-01..11-14, P2.53).

Market: every book quote captured in artifacts/operational/nhl/odds_observations
(timestamped). For each game, the last observation at least 30 minutes before the start.
Play price: best of the operator's books in the capture (BetOnline, Novig, Pinnacle;
Kalshi is not in the odds feed). Market fair: median proportional no-vig of every other
book quoting both sides of the same player, line (excluding the play's book).

Qualified = the operator's 8rain searches (2026-11-16 screenshots): edge is model minus
the play price's implied probability (points, as 8rain shows it).
  Points Over 0.5   edge >= 5,      model 57.1-83.3% (fair -133..-500)
  Points Under 0.5  edge 4-10,      model 57.1-75.0% (fair -133..-300)
  SOG Over 1.5      edge >= 6,      model 57.1-83.3%
  SOG Under 1.5     edge >= 10,     model 57.1-75.0%
Also reported: every line and side with a positive edge, by model-minus-market gap.
"""
from __future__ import annotations

import glob
import json
import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/qualified_plays_vs_market_2026-11-16"
SOG_ROWS = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/pp_corrected_refit_2026-11-15/live_rows.csv"
PTS_ROWS = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/pp_stats_api_refit_2026-11-15/live_rows.csv"
OBS = ROOT / "artifacts/operational/nhl/odds_observations/season=2026"
ATTACH = ROOT / "artifacts/operational/nhl/{lane}_market_attachments/season=2026"
MY_BOOKS = ("betonlineag", "novig", "pinnacle")
MARKETS = {"sog": "player_shots_on_goal", "points": "player_points"}
LINES = {"sog": (1.5, 2.5, 3.5), "points": (0.5, 1.5)}
FILTERS = {("points", 0.5, "Over"): (0.05, 1.0, 0.5714, 0.8333), ("points", 0.5, "Under"): (0.04, 0.10, 0.5714, 0.75),
           ("sog", 1.5, "Over"): (0.06, 1.0, 0.5714, 0.8333), ("sog", 1.5, "Under"): (0.10, 1.0, 0.5714, 0.75)}
LEAD = pd.Timedelta(minutes=30)


def norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return " ".join(re.sub(r"[^a-z ]", " ", s.replace("'", "").replace(".", "")).split())


def dec(american) -> np.ndarray:
    a = np.asarray(american, float)
    return np.where(a > 0, 1 + a / 100, 1 + 100 / -a)


def quotes() -> pd.DataFrame:
    parts = []
    for f in glob.glob(str(OBS / "slate_date=*/observation=*/normalized_odds.jsonl")):
        t = pd.read_json(f, lines=True)
        if "market" not in t:
            continue
        t = t[t.market.isin(MARKETS.values()) & t.outcome.isin(["Over", "Under"])]
        if t.empty:
            continue
        t["obs"] = pd.Timestamp(re.search(r"observation=(\d{8}T\d{6})", f).group(1), tz="UTC")
        parts.append(t[["obs", "bookmaker", "market", "description", "outcome", "point", "price", "commence_time"]])
    q = pd.concat(parts, ignore_index=True)
    q["start"] = pd.to_datetime(q.commence_time, utc=True)
    q = q[q.obs <= q.start - LEAD]
    q["who"] = q.description.map(norm)
    q["game_date"] = q.start.dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")
    # last pregame observation per game (all books in it)
    last = q.groupby(["game_date", "start"]).obs.transform("max")
    return q[q.obs.eq(last)]


def names() -> dict[int, str]:
    out: dict[int, str] = {}
    for lane in ("sog", "points"):
        for f in glob.glob(str(ATTACH).format(lane=lane) + f"/slate_date=*/run_id=*/{lane}_with_market.csv"):
            t = pd.read_csv(f, usecols=["player_id", "full_name"])
            out.update(dict(zip(t.player_id.astype(int), t.full_name)))
    return out


def model_rows() -> pd.DataFrame:
    sog = pd.read_csv(SOG_ROWS).assign(lane="sog", lam=lambda x: x.V2, actual=lambda x: x.shots_on_goal)
    pts = pd.read_csv(PTS_ROWS).assign(lane="points", lam=lambda x: x.CORRECTED_FIT, actual=lambda x: x.realized_points)
    rows = []
    for t in (sog, pts):
        for line in LINES[t.lane.iloc[0]]:
            p_over = poisson.sf(int(line), t.lam.clip(lower=1e-6))
            for side in ("Over", "Under"):
                rows.append(pd.DataFrame({"lane": t.lane, "game_id": t.game_id, "player_id": t.player_id,
                                          "game_date": t.game_date.astype(str), "line": line, "side": side,
                                          "model_p": p_over if side == "Over" else 1 - p_over,
                                          "won": (t.actual > line) if side == "Over" else (t.actual < line)}))
    return pd.concat(rows, ignore_index=True)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    q = quotes()
    two = q.pivot_table(index=["game_date", "market", "who", "point", "bookmaker"], columns="outcome",
                        values="price", aggfunc="last").dropna().reset_index()
    two["p_over"] = (1 / dec(two.Over)) / (1 / dec(two.Over) + 1 / dec(two.Under))
    side_q = q[q.bookmaker.isin(MY_BOOKS)].assign(decimal=lambda x: dec(x.price))
    best = side_q.sort_values("decimal").groupby(["game_date", "market", "who", "point", "outcome"]).tail(1)
    m = model_rows()
    nm = names()
    m["who"] = m.player_id.map(nm).map(norm)
    m["market"] = m.lane.map(MARKETS)
    m = m.merge(best[["game_date", "market", "who", "point", "outcome", "decimal", "bookmaker"]]
                .rename(columns={"point": "line", "outcome": "side"}), on=["game_date", "market", "who", "line", "side"])
    # market fair excluding the play's own book
    fair = []
    for r in m[["game_date", "market", "who", "line", "bookmaker"]].drop_duplicates().itertuples(index=False):
        x = two[(two.game_date == r.game_date) & (two.market == r.market) & (two.who == r.who)
                & (two.point == r.line) & (two.bookmaker != r.bookmaker)]
        fair.append((*r, x.p_over.median() if len(x) else np.nan, x.bookmaker.nunique()))
    fair = pd.DataFrame(fair, columns=["game_date", "market", "who", "line", "bookmaker", "mkt_over", "n_books"])
    m = m.merge(fair, on=["game_date", "market", "who", "line", "bookmaker"])
    m = m[m.n_books >= 2].copy()
    m["mkt_p"] = np.where(m.side == "Over", m.mkt_over, 1 - m.mkt_over)
    m["implied"] = 1 / m.decimal
    m["edge"] = m.model_p - m.implied
    m["mkt_ev"] = m.mkt_p * m.decimal - 1          # value of the price by the market's own number
    m["profit"] = np.where(m.won, m.decimal - 1, -1.0)
    m["qualified"] = False
    for (lane, line, side), (lo, hi, plo, phi) in FILTERS.items():
        k = (m.lane == lane) & (m.line == line) & (m.side == side)
        m.loc[k, "qualified"] = m.loc[k].edge.between(lo, hi) & m.loc[k].model_p.between(plo, phi)
    days = m.groupby("lane").game_date.nunique().to_dict()
    m.to_csv(OUT / "priced_rows.csv.gz", index=False)

    def summ(g):
        n = len(g)
        se = np.sqrt((g.mkt_p * (1 - g.mkt_p)).sum()) / n if n else np.nan
        return pd.Series({"plays": n, "per_slate": n / days[g.lane.iloc[0]], "won_pct": 100 * g.won.mean(),
                          "model_pct": 100 * g.model_p.mean(), "market_pct": 100 * g.mkt_p.mean(),
                          "price_pct": 100 * g.implied.mean(), "z_vs_market": (g.won.mean() - g.mkt_p.mean()) / se,
                          "roi_pct": 100 * g.profit.mean(), "beat_market_price_share": 100 * (g.mkt_ev > 0).mean()})

    qual = m[m.qualified]
    t1 = qual.groupby(["lane", "line", "side"])[list(m.columns)].apply(summ).round(2)
    t2 = qual.assign(price_vs_market=np.where(qual.mkt_ev > 0, "price better than market fair",
                                              "price at or worse than market fair")) \
        .groupby(["lane", "price_vs_market"])[list(m.columns)].apply(summ).round(2)
    pos = m[m.edge > 0].copy()
    pos["gap_band"] = pd.cut(pos.model_p - pos.mkt_p, [-1, 0, 0.03, 0.06, 0.10, 1],
                             labels=["model<=market", "0-3", "3-6", "6-10", "10+"])
    t3 = pos.groupby(["lane", "gap_band"], observed=True)[list(m.columns)].apply(summ).round(2)
    t1.to_csv(OUT / "qualified_by_search.csv")
    t2.to_csv(OUT / "qualified_by_price_vs_market.csv")
    t3.to_csv(OUT / "positive_edge_by_model_market_gap.csv")
    (OUT / "summary.json").write_text(json.dumps({"slates": days, "priced_rows": int(len(m)),
                                                   "qualified": int(len(qual)), "my_books": MY_BOOKS,
                                                   "observation_lead_minutes": 30}, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps({"slates": days, "priced_rows": len(m), "qualified": len(qual)}))
    print(t1.to_string()); print(t2.to_string()); print(t3.to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
