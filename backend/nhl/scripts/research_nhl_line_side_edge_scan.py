#!/usr/bin/env python3
"""Where the current SOG and Points models beat the price, by line, side and edge. Research only.

Read-only. Uses the current production models' own predictions, not retired
records: SOG `NHL_SOG_COUNT_HGB_V2` rescored on 2026-10-25..11-14 (P2.51
`live_rows.csv`, the same fit as the frozen V2) and Points `c940ae2f` rescored
on 2026-11-01..11-14 (P2.53 `live_rows.csv`, identical to the frozen model).
Prices are the best decimal price at the operator's books (BetOnline, Novig,
Kalshi, Pinnacle) quoted before the prediction (the weekly price view's cache).

For every player-game, line and side with a price: model probability of the
side (Poisson ladder), implied probability 1/price, edge = model - implied
(points, as 8rain shows it), result and profit per unit staked. Reported inside
the operator's Fair Odds range (model 57.1-83.3%, -133 to -500) by edge band,
with volume per slate.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
OUT = ROOT / "artifacts/analysis/nhl/line_side_edge_scan_2026-11-15"
SOG_ROWS = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/pp_corrected_refit_2026-11-15/live_rows.csv"
PTS_ROWS = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/pp_stats_api_refit_2026-11-15/live_rows.csv"
PRICES = ROOT / "artifacts/analysis/nhl/model_evaluation/row_cache/prices"
LINES = {"sog": (1.5, 2.5, 3.5), "points": (0.5, 1.5, 2.5)}
FAIR_LO, FAIR_HI = 0.5714, 0.8333
EDGE_BANDS = [(-1.0, 0.0, "<0"), (0.0, 0.04, "0-4"), (0.04, 0.06, "4-6"), (0.06, 0.10, "6-10"), (0.10, 1.0, "10+")]


def prices() -> pd.DataFrame:
    files = {}
    for path in sorted(PRICES.glob("*_v1.csv")):
        day, snaps, _ = path.name.split("_")
        if day not in files or int(snaps) > files[day][0]:
            files[day] = (int(snaps), path)
    parts = [pd.read_csv(p).assign(slate_date=d) for d, (_, p) in files.items()]
    t = pd.concat(parts, ignore_index=True)
    t = t[t.price_scope.eq("my_books") & t.decimal_price.between(1.05, 25)]
    return t.drop_duplicates(["lane", "game_id", "player_id", "line", "quote_side"], keep="last")


def candidates(lane: str, rows: pd.DataFrame, lam_col: str, count_col: str) -> pd.DataFrame:
    out = []
    for line in LINES[lane]:
        k = int(line)
        p_over = poisson.sf(k, rows[lam_col].clip(1e-6).to_numpy())
        over_won = rows[count_col].to_numpy() > line
        for side in ("OVER", "UNDER"):
            out.append(pd.DataFrame({"lane": lane, "game_id": rows.game_id.astype("int64"),
                                     "player_id": rows.player_id.astype("int64"), "slate_date": rows.game_date.astype(str),
                                     "line": line, "quote_side": side,
                                     "model_p": p_over if side == "OVER" else 1 - p_over,
                                     "won": over_won if side == "OVER" else ~over_won}))
    return pd.concat(out, ignore_index=True)


def summarize(c: pd.DataFrame, days: dict[str, int]) -> pd.DataFrame:
    rows = []
    for (lane, line, side, band), g in c.groupby(["lane", "line", "quote_side", "edge_band"], observed=True):
        rows.append({"lane": lane, "line": line, "side": side, "edge_band": band, "n": len(g),
                     "per_slate": round(len(g) / days[lane], 2), "wins": int(g.won.sum()),
                     "win_pct": round(100 * g.won.mean(), 1), "model_pct": round(100 * g.model_p.mean(), 1),
                     "implied_pct": round(100 * g.implied.mean(), 1),
                     "roi_pct": round(100 * g.profit.mean(), 1), "units": round(g.profit.sum(), 2)})
    return pd.DataFrame(rows)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    sog = pd.read_csv(SOG_ROWS)
    pts = pd.read_csv(PTS_ROWS)
    c = pd.concat([candidates("sog", sog, "V2", "shots_on_goal"),
                   candidates("points", pts, "CORRECTED_FIT", "realized_points")], ignore_index=True)
    c = c.merge(prices()[["lane", "game_id", "player_id", "line", "quote_side", "decimal_price", "sportsbook_key"]],
                on=["lane", "game_id", "player_id", "line", "quote_side"], how="inner")
    c["implied"] = 1 / c.decimal_price
    c["edge"] = c.model_p - c.implied
    c["profit"] = np.where(c.won, c.decimal_price - 1, -1.0)
    c["edge_band"] = pd.cut(c.edge, [b[0] for b in EDGE_BANDS] + [1.0], labels=[b[2] for b in EDGE_BANDS], right=False)
    days = {"sog": sog.game_date.nunique(), "points": pts.game_date.nunique()}
    inside = c[c.model_p.between(FAIR_LO, FAIR_HI)]
    table = summarize(inside, days)
    table.to_csv(OUT / "fair_range_by_line_side_edge.csv", index=False)
    pos = inside[inside.edge > 0]
    overall = summarize(pos.assign(edge_band="all positive"), days)
    overall.to_csv(OUT / "fair_range_positive_edge_by_line_side.csv", index=False)
    books = pos.groupby("sportsbook_key").agg(n=("profit", "size"), roi=("profit", "mean")).round(3)
    books.to_csv(OUT / "best_price_book_share.csv")
    (OUT / "summary.json").write_text(json.dumps({"slates": days, "priced_candidates": int(len(c)),
                                                  "inside_fair_range": int(len(inside))}, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps({"slates": days, "priced": len(c), "inside_fair_range": len(inside)}))
    print(overall.to_string(index=False))
    print(table[table.edge_band.isin(["4-6", "6-10", "10+"])].to_string(index=False))
    print(books.to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
