#!/usr/bin/env python3
"""Do depth players the books list for Points outscore the model? 2025-26 (NHL Backlog P2.55).

Research only, read-only. Live 2026-27: over all player-games the Points model is calibrated
on depth players, but the depth players with a posted Points line scored more than it
expected and about what the market said. On 2025-26 the archived 7 pm ET snapshots
(`backend/nhl/exports/odds_history/<date>/odds_event_wrappers.json`, market player_points,
US books) say which player-games were listed. Production 2f0824cf scores the rebuilt 2025-26
frame (its holdout season). Compared: model vs actual P(1+ point), listed vs not, by scoring
rate quintile (training cut-offs) and ice time; and the listed share by band.
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
import unicodedata
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_listed_players_2025_2026-11-16"
REBUILT = ROOT / "artifacts/analysis/nhl/points_training_rebuild_2026-11-16"
ODDS = ROOT / "backend/nhl/exports/odds_history"


def norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return " ".join(re.sub(r"[^a-z ]", " ", s.replace("'", "").replace(".", "")).split())


def listed() -> pd.DataFrame:
    rows = []
    for d in sorted(glob.glob(str(ODDS / "2025-1*")) + glob.glob(str(ODDS / "2026-0[1-4]*"))):
        p = Path(d) / "odds_event_wrappers.json"
        if not p.is_file():
            continue
        day = Path(d).name
        for w in json.loads(p.read_text()):
            ev = w.get("data") or {}
            for b in ev.get("bookmakers", []):
                for m in b.get("markets", []):
                    if m["key"] != "player_points":
                        continue
                    for o in m["outcomes"]:
                        rows.append((day, norm(o.get("description")), b["key"], o.get("point")))
    t = pd.DataFrame(rows, columns=["slate_date", "who", "book", "point"])
    return t.groupby(["slate_date", "who"]).agg(books=("book", "nunique"),
                                                has_0_5=("point", lambda s: bool((s == 0.5).any()))).reset_index()


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    s25 = pd.read_csv(REBUILT / "season_frame_2025.csv.gz", float_precision="round_trip")
    train = pd.read_csv(REBUILT / "training_frame_2023_2024.csv.gz", usecols=["own_rate_k30"])
    prod = joblib.load(scorer.MODEL_PATH)
    s25["model"] = poisson.sf(0, scorer.score(s25, prod).expected_points.to_numpy())
    s25["actual"] = (s25.realized_points >= 1).astype(float)
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SELECT player_id::bigint, full_name FROM nhl.players")
        names = dict(cur.fetchall())
    s25["who"] = s25.player_id.map(names).map(norm)
    lst = listed()
    days = set(lst.slate_date)
    s25 = s25[s25.slate_date.astype(str).isin(days)].copy()
    s25 = s25.merge(lst, left_on=[s25.slate_date.astype(str), "who"], right_on=["slate_date", "who"], how="left",
                    suffixes=("", "_l"))
    s25["listed"] = s25.books.notna()
    cuts = train.own_rate_k30.quantile([0.2, 0.4, 0.6, 0.8]).to_list()
    s25["rate_band"] = pd.cut(s25.own_rate_k30, [-1] + cuts + [9], labels=["Q1 low", "Q2", "Q3", "Q4", "Q5 high"])
    s25["toi_band"] = pd.cut(s25.mean_toi_last10, [0, 13, 15, 17, 20, 40], labels=["<13", "13-15", "15-17", "17-20", "20+"])
    out = []
    for key in ("rate_band", "toi_band"):
        for (band, lst_flag), g in s25.groupby([key, "listed"], observed=True):
            se = float(np.sqrt(g.actual.mean() * (1 - g.actual.mean()) / len(g)))
            out.append({"by": key, "band": str(band), "listed": bool(lst_flag), "n": len(g),
                        "model": g.model.mean(), "actual": g.actual.mean(), "actual_minus_model": g.actual.mean() - g.model.mean(),
                        "se": se, "log_loss": float(base.log_loss_rows(g.model.to_numpy(), g.actual.to_numpy()).mean())})
    t = pd.DataFrame(out)
    share = s25.groupby("rate_band", observed=True).listed.mean().round(3)
    t.round(4).to_csv(OUT / "listed_vs_unlisted.csv", index=False)
    summary = {"slates": int(s25.slate_date.nunique()), "player_games": int(len(s25)), "listed_share": float(s25.listed.mean()),
               "listed_share_by_rate_band": share.to_dict(),
               "overall": {str(k): {"n": int(len(g)), "model": float(g.model.mean()), "actual": float(g.actual.mean())}
                           for k, g in s25.groupby("listed")}}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    pd.set_option("display.width", 220)
    print(json.dumps(summary, indent=1))
    print(t.round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
