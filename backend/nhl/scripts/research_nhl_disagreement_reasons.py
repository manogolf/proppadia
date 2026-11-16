#!/usr/bin/env python3
"""When the model disagrees most with the market, which side is right, by reason? Research only (P2.56).

Read-only. Every slate's input watch lists the biggest disagreements per lane (model above
the market on its side, two-sided lines) with the reasons the watch found in the inputs
(`input_watch.json` -> disagreements.lanes). Each is graded on the official postgame
outcome (`canonical_skater_outcomes.csv`, participants only). Model = the production
model's probability that day, as the watch recorded it (retired models before 2026-11-15,
labelled by period); market = the watch's no-vig market probability. Log loss compares
the two forecasts on the same plays; win rate vs each shows which was closer.
"""
from __future__ import annotations

import glob
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
WATCH = ROOT / "artifacts/operational/nhl/input_watch"
POSTGAME = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
OUT = ROOT / "artifacts/analysis/nhl/disagreement_reasons_2026-11-16"
CURRENT_FROM = "2026-11-15"  # SOG V2 and Points c940ae2f


def category(reason: str) -> str:
    r = reason.lower()
    for key, label in (("role change", "role change vs last season"), ("toi low", "TOI low vs market peers"),
                       ("toi high", "TOI high vs market peers"), ("team shot share", "team shot share vs peers"),
                       ("games played low", "few games vs market peers"), ("only", "few games played"),
                       ("defenseman", "defenseman"), ("no input stands out", "no input stands out")):
        if key in r:
            return label
    return re.sub(r"\(.*?\)|[\d.]+", "", reason).strip() or "other"


def disagreements() -> pd.DataFrame:
    rows = []
    for day_dir in sorted(WATCH.glob("slate_date=*")):
        runs = sorted(day_dir.glob("run_id=*/input_watch.json"))
        if not runs:
            continue
        j = json.loads(runs[-1].read_text())  # latest run of the slate
        for lane, items in ((j.get("disagreements") or {}).get("lanes") or {}).items():
            for d in items:
                reasons = d.get("reasons") or ["no input stands out"]
                rows.append({"slate_date": j["slate_date"], "lane": lane, "game_id": int(d["game_id"]),
                             "player_id": int(d["player_id"]), "player": d.get("player"), "line": float(d["line"]),
                             "side": d["side"], "model_p": float(d["model_p"]), "market_p": float(d["market_p"]),
                             "books": d.get("books"), "reasons": reasons,
                             "categories": sorted({category(x) for x in reasons})})
    return pd.DataFrame(rows)


def outcomes() -> pd.DataFrame:
    parts = []
    for day in sorted(POSTGAME.glob("20*")):
        files = sorted(day.glob("*/canonical_skater_outcomes.csv"))
        if files:
            parts.append(pd.read_csv(files[-1]))
    o = pd.concat(parts, ignore_index=True)
    o = o[o.participation_state.astype(str).str.upper().str.startswith("PARTICIPA")]
    return o[["game_id", "player_id", "official_sog", "official_points"]].astype("int64")


def summ(g: pd.DataFrame) -> pd.Series:
    y = g.won.to_numpy(float)
    ll = lambda p: float(-np.mean(y * np.log(np.clip(p, 1e-6, 1 - 1e-6)) + (1 - y) * np.log(np.clip(1 - p, 1e-6, 1 - 1e-6))))  # noqa: E731
    n = len(g)
    se = np.sqrt((g.market_p * (1 - g.market_p)).sum()) / n
    return pd.Series({"plays": n, "won_pct": 100 * g.won.mean(), "model_pct": 100 * g.model_p.mean(),
                      "market_pct": 100 * g.market_p.mean(), "z_vs_market": (g.won.mean() - g.market_p.mean()) / se,
                      "log_loss_model": ll(g.model_p.to_numpy()), "log_loss_market": ll(g.market_p.to_numpy()),
                      "closer": "market" if ll(g.market_p.to_numpy()) < ll(g.model_p.to_numpy()) else "model"})


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    d = disagreements().merge(outcomes(), on=["game_id", "player_id"], how="inner")
    actual = np.where(d.lane.eq("sog"), d.official_sog, d.official_points)
    d["won"] = np.where(d.side.eq("OVER"), actual > d.line, actual < d.line)
    d["period"] = np.where(d.slate_date >= CURRENT_FROM, "current models", "retired models")
    d["gap"] = d.model_p - d.market_p
    d.to_csv(OUT / "graded_disagreements.csv", index=False)
    ex = d.explode("categories")
    t_all = d.groupby(["lane", "side"])[list(d.columns)].apply(summ)
    t_reason = ex.groupby(["lane", "categories"])[list(ex.columns)].apply(summ)
    t_reason = t_reason[t_reason.plays >= 10]
    t_gap = d.assign(gap_band=pd.cut(d.gap, [0, 0.12, 0.15, 0.20, 1], labels=["10-12", "12-15", "15-20", "20+"])) \
        .groupby(["lane", "gap_band"], observed=True)[list(d.columns) + ["gap_band"]].apply(summ)
    t_period = d.groupby(["period", "lane"])[list(d.columns)].apply(summ)
    for name, t in (("by_lane_side", t_all), ("by_reason", t_reason), ("by_gap", t_gap), ("by_period", t_period)):
        t.round(4).to_csv(OUT / f"{name}.csv")
    (OUT / "summary.json").write_text(json.dumps({"graded": int(len(d)), "slates": int(d.slate_date.nunique()),
                                                   "first": d.slate_date.min(), "last": d.slate_date.max()}, indent=1) + "\n")
    pd.set_option("display.width", 250)
    print(len(d), d.slate_date.min(), d.slate_date.max(), d.slate_date.nunique())
    for t in (t_period, t_all, t_gap, t_reason):
        print(t.round(3).to_string()); print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
