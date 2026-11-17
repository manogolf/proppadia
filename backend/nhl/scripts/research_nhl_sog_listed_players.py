#!/usr/bin/env python3
"""SOG: is the model low on player-games the books list? (P2.59 follow-on, 2026-11-16)

Research only, read-only. Points was calibrated over every player-game but about 3 points low on
the ones the books list (P2.55). Same question for SOG:
- Holdout 2026-01..04 (2025-26 season): the V2 recipe fitted on rows before 2026-01-01 (as in the
  P2.52 ablation and the road-top-line check); listed = any book quoted the player's SOG in the
  7 pm ET snapshot (`backend/nhl/exports/odds_history`).
- Live 2026-10-25..11-14: V2's own predictions; listed = a two-sided SOG market in the captures.
By listed / not listed and by shot-rate quintile: mean SOG and P(Over 1.5), actual vs model.
"""
from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import research_nhl_points_listed_players as lp  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_hgb_count_model as research  # noqa: E402
from backend.nhl.scripts import research_nhl_sog_situation_ablation as abl  # noqa: E402
from backend.nhl.sog_hgb_features import FEATURES  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/sog_listed_players_2026-11-16"
LIVE_ROWS = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/pp_corrected_refit_2026-11-15/live_rows.csv"
PRICED = ROOT / "artifacts/analysis/nhl/qualified_plays_vs_market_2026-11-16_close/priced_rows.csv.gz"
KEYS = ["game_id", "player_id"]


def listed_sog() -> pd.DataFrame:
    rows = []
    for d in sorted(glob.glob(str(lp.ODDS / "2026-0[1-4]*"))):
        p = Path(d) / "odds_event_wrappers.json"
        if not p.is_file():
            continue
        for w in json.loads(p.read_text()):
            for b in (w.get("data") or {}).get("bookmakers", []):
                for m in b.get("markets", []):
                    if m["key"] in ("player_shots_on_goal", "player_shots_on_goal_alternate"):
                        for o in m["outcomes"]:
                            rows.append((Path(d).name, lp.norm(o.get("description"))))
    return pd.DataFrame(rows, columns=["slate_date", "who"]).drop_duplicates()


def table(f: pd.DataFrame, pred: str) -> pd.DataFrame:
    f = f.assign(p15=poisson.sf(1, f[pred].to_numpy()), y15=(f.shots_on_goal >= 2).astype(float),
                 band=pd.qcut(f[pred].rank(method="first"), 5, labels=["Q1 low", "Q2", "Q3", "Q4", "Q5 high"]))
    out = []
    for key in ("all", "band"):
        groups = [("all", f)] if key == "all" else list(f.groupby("band", observed=True))
        for b, g0 in groups:
            for lst, g in g0.groupby("listed"):
                n = len(g)
                out.append({"by": key, "band": str(b), "listed": bool(lst), "n": n, "sog_actual": g.shots_on_goal.mean(),
                            "sog_model": g[pred].mean(), "over15_actual": g.y15.mean(), "over15_model": g.p15.mean(),
                            "over15_gap": g.y15.mean() - g.p15.mean(),
                            "se": float(np.sqrt(g.p15.mean() * (1 - g.p15.mean()) / n))})
    return pd.DataFrame(out)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    logs, pos = research.pull()
    frame = research.build(logs, pos)
    frame["game_id"], frame["player_id"] = frame.game_id.astype("int64"), frame.player_id.astype("int64")
    f = abl.add_features(frame, abl.situation_table())
    ok = f.toi_avg_10.notna() & f.shrunk_lambda_180.notna()
    tr = f[f.season.isin([2024, 2025]) & ok]
    early = tr[tr.game_date < pd.Timestamp(abl.HOLDOUT_START)]
    hold = tr[(tr.game_date >= pd.Timestamp(abl.HOLDOUT_START)) & (tr.game_date < pd.Timestamp(abl.HOLDOUT_END))].copy()
    hold["pred"] = abl._fit(early, FEATURES).predict(hold[FEATURES].astype(float).to_numpy())
    import os
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SELECT player_id::bigint, full_name FROM nhl.players")
        names = dict(cur.fetchall())
    hold["slate_date"] = hold.game_date.dt.strftime("%Y-%m-%d")
    hold["who"] = hold.player_id.map(names).map(lp.norm)
    lst = listed_sog()
    hold = hold[hold.slate_date.isin(set(lst.slate_date))]
    hold = hold.merge(lst.assign(listed=True), on=["slate_date", "who"], how="left")
    hold["listed"] = hold.listed.fillna(False).astype(bool)
    th = table(hold, "pred")
    th.round(4).to_csv(OUT / "holdout_2026_jan_apr.csv", index=False)
    live = pd.read_csv(LIVE_ROWS)[KEYS + ["game_date", "shots_on_goal", "V2"]]
    pr = pd.read_csv(PRICED)
    priced = pr[pr.lane.eq("sog")][KEYS].drop_duplicates().assign(listed=True)
    live = live.merge(priced, on=KEYS, how="left")
    live["listed"] = live.listed.fillna(False).astype(bool)
    tl = table(live, "V2")
    tl.round(4).to_csv(OUT / "live_2026.csv", index=False)
    summary = {"holdout_rows": int(len(hold)), "holdout_slates": int(hold.slate_date.nunique()),
               "holdout_listed_share": float(hold.listed.mean()), "live_rows": int(len(live)),
               "live_listed_share": float(live.listed.mean())}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary))
    print(th.round(3).to_string(index=False)); print(tl.round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
