#!/usr/bin/env python3
"""SOG: does the model miss the road top line's shot shortfall? (NHL Backlog P2.46 test 4, 2026-11-16)

Research only, read-only. Line matchups (first version) found road top lines take about 5%
fewer shots than their own last-10 rate x TOI, 2-3 points more than other road units (the home
coach's last change). The SOG model has is_home but no line rank. Here the V2 recipe
(sog_hgb_features.FEATURES, the P2.52 ablation fit) is fitted on rows before 2026-01-01 and
scored on the 2026-01..04 holdout; live uses V2's own predictions 2026-10-25..11-14 (P2.51
live_rows.csv). Pregame units from last-10 TOI within each team-game (forwards F1-3, F4-6,
F7-9, F10+; defense D1-2, D3-4, D5+). By home/road and unit: actual vs predicted SOG and
P(Over 1.5), and for live the SOG 1.5 Over plays against the price and the market.
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
from backend.nhl.scripts import research_nhl_sog_hgb_count_model as research  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_situation_ablation as abl  # noqa: E402
from backend.nhl.sog_hgb_features import FEATURES  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/sog_road_top_line_2026-11-16"
LIVE_ROWS = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/pp_corrected_refit_2026-11-15/live_rows.csv"
QV = ROOT / "artifacts/analysis/nhl/qualified_plays_vs_market_2026-11-16/priced_rows.csv.gz"
KEYS = ["game_id", "player_id"]


def units(f: pd.DataFrame) -> pd.Series:
    team = np.where(f.is_home.astype(bool), f.home_team_id, f.away_team_id)
    r = f.assign(team=team).groupby(["game_id", "team", "is_defense"]).toi_avg_10.rank(ascending=False, method="first")
    fw = np.select([r <= 3, r <= 6, r <= 9], ["F1-3 top line", "F4-6", "F7-9"], "F10+")
    dz = np.select([r <= 2, r <= 4], ["D1-2 top pair", "D3-4"], "D5+")
    return pd.Series(np.where(f.is_defense.astype(int).eq(1), dz, fw), index=f.index)


def table(f: pd.DataFrame, pred: str) -> pd.DataFrame:
    rows = []
    for (u, home), g in f.groupby(["unit", "is_home"]):
        y = g.shots_on_goal.to_numpy(float)
        p = g[pred].to_numpy(float)
        po = poisson.sf(1, p)
        rows.append({"unit": u, "side": "home" if home else "road", "n": len(g), "sog_actual": y.mean(), "sog_pred": p.mean(),
                     "ratio": y.sum() / p.sum(), "over15_actual": float((y >= 2).mean()), "over15_pred": float(po.mean()),
                     "over15_gap": float((y >= 2).mean() - po.mean()), "se": float(np.sqrt(po.mean() * (1 - po.mean()) / len(g)))})
    return pd.DataFrame(rows).sort_values(["unit", "side"])


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    logs, pos = research.pull()
    frame = research.build(logs, pos)
    frame["game_id"], frame["player_id"] = frame.game_id.astype("int64"), frame.player_id.astype("int64")
    f = abl.add_features(frame, abl.situation_table())
    ok = f.toi_avg_10.notna() & f.shrunk_lambda_180.notna()
    train_all = f[f.season.isin([2024, 2025]) & ok]
    cut = pd.Timestamp(abl.HOLDOUT_START)
    early = train_all[train_all.game_date < cut]
    test = train_all[(train_all.game_date >= cut) & (train_all.game_date < pd.Timestamp(abl.HOLDOUT_END))].copy()
    test["pred"] = abl._fit(early, FEATURES).predict(test[FEATURES].astype(float).to_numpy())
    test["unit"] = units(test)
    hold = table(test, "pred")
    hold.round(4).to_csv(OUT / "holdout_2026_jan_apr.csv", index=False)
    # Live: V2's own predictions.
    lr = pd.read_csv(LIVE_ROWS)[KEYS + ["V2"]]
    live = f[f.season.eq(2026)].merge(lr, on=KEYS, how="inner").copy()
    live["unit"] = units(live)
    lt = table(live, "V2")
    lt.round(4).to_csv(OUT / "live_2026.csv", index=False)
    # Live SOG 1.5 Over plays (priced at the operator's books, 1 PM market) by home/road for top lines.
    pr = pd.read_csv(QV)
    pr = pr[pr.lane.eq("sog") & pr.line.eq(1.5) & pr.side.eq("Over")].merge(live[KEYS + ["unit", "is_home"]], on=KEYS)
    rows = []
    for (u, home), g in pr.groupby(["unit", "is_home"]):
        q = g[g.edge.between(0.06, 1) & g.model_p.between(0.5714, 0.8333)]
        rows.append({"unit": u, "side": "home" if home else "road", "priced": len(g), "qualified": len(q),
                     "won": float(q.won.mean()) if len(q) else None, "model": float(q.model_p.mean()) if len(q) else None,
                     "market": float(q.mkt_p.mean()) if len(q) else None,
                     "roi": float(q.profit.mean()) if len(q) else None,
                     "all_won": float(g.won.mean()), "all_model": float(g.model_p.mean()), "all_market": float(g.mkt_p.mean())})
    plays = pd.DataFrame(rows).sort_values(["unit", "side"])
    plays.round(3).to_csv(OUT / "live_sog_over_1_5_plays.csv", index=False)
    summary = {"holdout_rows": int(len(test)), "live_rows": int(len(live))}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary))
    print(hold.round(3).to_string(index=False)); print(lt.round(3).to_string(index=False)); print(plays.round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
