#!/usr/bin/env python3
"""Points with the real game total: implied team goals at the first daily run (NHL Backlog P2.55).

Research only, read-only. From the historical snapshots (fetch_nhl_historical_game_lines.py,
14:00 UTC, about the 05:45 PT first run) and, for 2026-27, the timestamped captures (last
observation by 06:30 PT on game day, else the earliest of the day): per game the median total
line and no-vig Over probability across books, and the median no-vig home win probability.
Implied team goals: total split by win probability, team share = 0.5 + k x (p_win - 0.5) with
k = 0.6 (a team that wins 60% of the time gets about 53% of the goals); the expected total is
the line moved by the Over lean (0.5 goals per 10 points of no-vig Over probability above 50%).

Candidates on the rebuilt frames (Utah included; goals + assists, production recipe):
PRODUCTION (2f0824cf), +PPU (power-play unit, P2.55), +TOTALS (implied team goals, game total,
win probability), +PPU+TOTALS. Scored on the 2025-26 holdout (all and listed), live
2026-11-01..11-15, two-sided Points vs the market, and the qualified searches at 1 PM.
"""
from __future__ import annotations

import glob
import gzip
import json
import re
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
from backend.nhl.scripts import research_nhl_points_listed_training as lt  # noqa: E402
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms  # noqa: E402
from backend.nhl.scripts import research_nhl_points_per_minute_corrected as pmc  # noqa: E402
from backend.nhl.scripts import research_nhl_points_ppunit_game_scoring as pu  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_game_totals_2026-11-16"
HIST = ROOT / "artifacts/analysis/nhl/game_lines_history_2026-11-16/raw"
OBS = ROOT / "artifacts/operational/nhl/odds_observations/season=2026"
REBUILT = ROOT / "artifacts/analysis/nhl/points_training_rebuild_2026-11-16"
QV = ROOT / "artifacts/analysis/nhl"
KEYS = ["game_id", "player_id"]
F = list(freeze.FEATURES)
TOT = ["team_implied_goals", "game_total", "team_win_p"]
K_SHARE = 0.6


def dec(a):
    a = np.asarray(a, float)
    return np.where(a > 0, 1 + a / 100, 1 + 100 / -a)


def summarize(rows: pd.DataFrame) -> pd.DataFrame:
    """rows: game_date, home, away, book, market, outcome, point, price -> one row per game."""
    out = []
    for (day, home, away), g in rows.groupby(["game_date", "home", "away"]):
        h2h = g[g.market.eq("h2h")].pivot_table(index="book", columns="outcome", values="price", aggfunc="last")
        p_home = np.nan
        if home in h2h and away in h2h:
            h = h2h[[home, away]].dropna()
            ph, pa = 1 / dec(h[home]), 1 / dec(h[away])
            p_home = float(np.median(ph / (ph + pa))) if len(h) else np.nan
        tot = g[g.market.eq("totals")]
        line, p_over = np.nan, np.nan
        if len(tot):
            line = float(tot.point.median())
            t = tot[tot.point.eq(line)].pivot_table(index="book", columns="outcome", values="price", aggfunc="last")
            if {"Over", "Under"} <= set(t.columns):
                t = t[["Over", "Under"]].dropna()
                po, pu_ = 1 / dec(t.Over), 1 / dec(t.Under)
                p_over = float(np.median(po / (po + pu_))) if len(t) else np.nan
        out.append({"game_date": day, "home": home, "away": away, "p_home": p_home, "total_line": line, "p_over": p_over})
    return pd.DataFrame(out)


def historical() -> pd.DataFrame:
    rows = []
    for f in sorted(HIST.glob("date=*.json.gz")):
        day = re.search(r"date=([\d-]+)", f.name).group(1)
        for ev in json.loads(gzip.decompress(f.read_bytes())).get("data") or []:
            et = pd.Timestamp(ev["commence_time"]).tz_convert("America/New_York").strftime("%Y-%m-%d")
            if et != day:
                continue
            for b in ev.get("bookmakers", []):
                for m in b.get("markets", []):
                    for o in m.get("outcomes", []):
                        rows.append((day, ev["home_team"], ev["away_team"], b["key"], m["key"], o["name"], o.get("point"), o["price"]))
    return pd.DataFrame(rows, columns=["game_date", "home", "away", "book", "market", "outcome", "point", "price"])


def live_lines() -> pd.DataFrame:
    """2026-27 game lines from the observation the live shadow uses (points_candidate_shadows.morning_observation)."""
    from backend.nhl import points_candidate_shadows as live
    rows = []
    for d in sorted(OBS.glob("slate_date=2026-1[01]-*")):
        day = d.name.split("=")[1]
        p = live.morning_observation(day)
        if p is None:
            continue
        t = pd.read_json(p, lines=True)
        if "market" not in t:
            continue
        t = t[t.market.isin(["h2h", "totals"])]
        for r in t.itertuples():
            et = pd.Timestamp(r.commence_time).tz_convert("America/New_York").strftime("%Y-%m-%d")
            if et == day:
                rows.append((day, r.home_team, r.away_team, r.bookmaker, r.market, r.outcome, r.point, r.price))
    return pd.DataFrame(rows, columns=["game_date", "home", "away", "book", "market", "outcome", "point", "price"])


def team_names() -> dict:
    import os
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SELECT game_id::bigint, game_date::text, home_team_code, away_team_code, home_team_id, away_team_id FROM nhl.games WHERE season BETWEEN 2023 AND 2026 AND game_type = 2")
        return pd.DataFrame(cur.fetchall(), columns=["game_id", "game_date", "home_code", "away_code", "home_id", "away_id"])


CODES = {"Anaheim Ducks": "ANA", "Arizona Coyotes": "ARI", "Boston Bruins": "BOS", "Buffalo Sabres": "BUF", "Calgary Flames": "CGY",
         "Carolina Hurricanes": "CAR", "Chicago Blackhawks": "CHI", "Colorado Avalanche": "COL", "Columbus Blue Jackets": "CBJ",
         "Dallas Stars": "DAL", "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM", "Florida Panthers": "FLA",
         "Los Angeles Kings": "LAK", "Minnesota Wild": "MIN", "Montréal Canadiens": "MTL", "Montreal Canadiens": "MTL",
         "Nashville Predators": "NSH", "New Jersey Devils": "NJD", "New York Islanders": "NYI", "New York Rangers": "NYR",
         "Ottawa Senators": "OTT", "Philadelphia Flyers": "PHI", "Pittsburgh Penguins": "PIT", "San Jose Sharks": "SJS",
         "Seattle Kraken": "SEA", "St Louis Blues": "STL", "St. Louis Blues": "STL", "Tampa Bay Lightning": "TBL",
         "Toronto Maple Leafs": "TOR", "Utah Hockey Club": "UTA", "Utah Mammoth": "UTA", "Vancouver Canucks": "VAN",
         "Vegas Golden Knights": "VGK", "Washington Capitals": "WSH", "Winnipeg Jets": "WPG"}


def game_inputs() -> pd.DataFrame:
    lines = pd.concat([historical(), live_lines()], ignore_index=True)
    g = summarize(lines)
    g["home_code"], g["away_code"] = g.home.map(CODES), g.away.map(CODES)
    games = team_names()
    m = games.merge(g, on=["game_date", "home_code", "away_code"], how="inner")
    lean = (m.p_over - 0.5).fillna(0) * 5.0          # 0.5 goals per 10 points of no-vig Over lean
    m["game_total"] = m.total_line + lean
    rows = []
    for side, team, pwin in (("home", m.home_id, m.p_home), ("away", m.away_id, 1 - m.p_home)):
        share = 0.5 + K_SHARE * (pwin - 0.5)
        rows.append(pd.DataFrame({"game_id": m.game_id, "team_id": team, "team_implied_goals": m.game_total * share,
                                  "game_total": m.game_total, "team_win_p": pwin}))
    return pd.concat(rows, ignore_index=True), {"games_matched": int(len(m)), "games_total": int(len(games)),
                                                "unmapped_names": sorted(set(g.home[g.home_code.isna()]) | set(g.away[g.away_code.isna()]))}


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    gi, cov = game_inputs()
    gi.to_csv(OUT / "game_inputs.csv.gz", index=False)
    pp = pd.read_csv(pu.OUT / "inputs.csv.gz")
    team = pu.team_games()[0][KEYS + ["team_id"]] if not (OUT / "player_team.csv.gz").is_file() else None
    if team is not None:
        team.to_csv(OUT / "player_team.csv.gz", index=False)
    team = pd.read_csv(OUT / "player_team.csv.gz")
    x = pp.merge(team, on=KEYS, how="left").merge(gi, on=["game_id", "team_id"], how="left")
    train = pd.read_csv(REBUILT / "training_frame_2023_2024.csv.gz", float_precision="round_trip")
    full25 = pd.read_csv(REBUILT / "season_frame_2025.csv.gz", float_precision="round_trip")
    s25 = lt.listed_2025()
    live = ms.frames()["live"].copy(); live["slate_date"] = live.game_date.astype(str)
    train, full25, s25, live = (d.merge(x[KEYS + pu.PPU + TOT], on=KEYS, how="left") for d in (train, full25, s25, live))
    cov["coverage"] = {c: {"train": float(train[c].notna().mean()), "holdout": float(full25[c].notna().mean()),
                           "live": float(live[c].notna().mean())} for c in TOT}
    specs = {"PPU": F + pu.PPU, "TOTALS": F + TOT, "PPU_TOTALS": F + pu.PPU + TOT}
    models = {}
    for k, feats in specs.items():
        g = freeze.research.fit_count(train, feats, "goals"); a = freeze.research.fit_count(train, feats, "assists")
        models[k] = {"structure": "GOALS_PLUS_ASSISTS", "feature_columns": feats, "fill_zero": True,
                     "scaler": g["scaler"], "goals": g, "assists": a}
    prod = joblib.load(scorer.MODEL_PATH)
    names = ["PRODUCTION", *specs]
    for d in (full25, s25, live):
        d["PRODUCTION"] = scorer.score(d, prod).expected_points.to_numpy()
        for k, m in models.items():
            d[k] = scorer.score(d, m).expected_points.to_numpy()
    full25["role_change"] = full25["role_up"] = full25["role_down"] = False
    tabs = []
    for part, d, masks in (("holdout_all", full25, {"all": np.ones(len(full25), bool)}),
                           ("holdout_listed", s25, {"listed": s25.listed.to_numpy(bool)}),
                           ("live", live, {"all": np.ones(len(live), bool)})):
        for k in specs:
            tabs.append(pmc.paired(d, k, "PRODUCTION", masks).assign(part=part, model=k))
    paired = pd.concat(tabs, ignore_index=True)[["part", "model", "group", "line", "n", "diff_x1000", "ci_x1000"]]
    paired.to_csv(OUT / "paired_vs_production.csv", index=False)
    lv = s25[s25.listed]
    summary = {"games": cov, "listed_2025_p1": {"actual": float((lv.realized_points >= 1).mean()),
                                                **{k: float(poisson.sf(0, lv[k]).mean()) for k in names}}}
    priced = pd.read_csv(QV / "qualified_plays_vs_market_2026-11-16_close" / "priced_rows.csv.gz")
    p05 = priced[priced.lane.eq("points") & priced.line.eq(0.5) & priced.side.eq("Over")][KEYS + ["mkt_over"]]
    mk = live.merge(p05, on=KEYS, how="inner"); y = (mk.realized_points >= 1).astype(float).to_numpy()
    summary["live_two_sided_0_5"] = {"rows": int(len(mk)), "log_loss": {
        "market": float(base.log_loss_rows(mk.mkt_over.to_numpy(), y).mean()),
        **{k: float(base.log_loss_rows(poisson.sf(0, mk[k].to_numpy()), y).mean()) for k in names}}}
    pr = pd.read_csv(QV / "qualified_plays_vs_market_2026-11-16" / "priced_rows.csv.gz")
    pr = pr[pr.lane.eq("points") & pr.line.eq(0.5)][KEYS + ["side", "decimal", "mkt_p", "won"]].merge(live[KEYS + names], on=KEYS)
    q = []
    for k in names:
        for side, lo, hi, plo, phi in (("Under", 0.04, 0.10, 0.5714, 0.75), ("Over", 0.05, 1.0, 0.5714, 0.8333)):
            s = pr[pr.side.eq(side)]
            p_over = poisson.sf(0, s[k].to_numpy()); p = p_over if side == "Over" else 1 - p_over
            e = p - 1 / s.decimal.to_numpy(); keep = (e >= lo) & (e <= hi) & (p >= plo) & (p <= phi); sel = s[keep]
            q.append({"model": k, "search": f"Points {side} 0.5", "plays": int(keep.sum()),
                      "won": float(sel.won.mean()) if len(sel) else None, "market": float(sel.mkt_p.mean()) if len(sel) else None,
                      "roi": float(np.where(sel.won, sel.decimal - 1, -1.0).mean()) if len(sel) else None})
    pd.DataFrame(q).to_csv(OUT / "qualified_searches_1pm.csv", index=False)
    joblib.dump(models, OUT / "candidates.joblib")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary, indent=1, default=str))
    print(paired.to_string(index=False))
    print(pd.DataFrame(q).round(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
