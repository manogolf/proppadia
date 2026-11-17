#!/usr/bin/env python3
"""Line matchups, first version: home vs road top lines (NHL Backlog P2.46 test 4, 2026-11-16).

Research only, read-only. The home coach has the last change and can put his checking line
against the visitors' top line. Season 2025-26 (production 2f0824cf's holdout; shift charts
complete after the P2.46 fill):

A. Pregame line ranks: within each team-game, forwards ranked by last-10 TOI (F1-F3 = top
   line, F4-F6, F7-F9, F10+) and defensemen (D1-D2 = top pair, D3-D4, D5+). By home/road and
   rank: actual points and P(1+ point) vs the model's expectation (the model already has
   is_home), and shots on goal vs the player's own last-10 rate x last-10 TOI.
B. Who faced whom, from shift charts (periods 1-3, all strengths): for each team's pregame top
   line, the share of its ice time against the opponent's pregame F1-F3, F4-F6, F7-F9, F10+ and
   top D pair, home vs road; and how predictable a team's top-line opponent is from its
   previous games (repeat rate of the most-faced opposing line rank).
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
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/line_matchups_2026-11-16"
REBUILT = ROOT / "artifacts/analysis/nhl/points_training_rebuild_2026-11-16"
EVENTS = ROOT / "artifacts/analysis/nhl/points_onice_xg_2026-11-16/events.joblib"
TEAM = ROOT / "artifacts/analysis/nhl/points_game_totals_2026-11-16/player_team.csv.gz"
KEYS = ["game_id", "player_id"]


def ranks(frame: pd.DataFrame) -> pd.DataFrame:
    f = frame.copy()
    f["r"] = f.groupby(["game_id", "team_id", "is_defense"]).mean_toi_last10.rank(ascending=False, method="first")
    fw = np.select([f.r <= 3, f.r <= 6, f.r <= 9], ["F1-3 top line", "F4-6", "F7-9"], "F10+")
    dz = np.select([f.r <= 2, f.r <= 4], ["D1-2 top pair", "D3-4"], "D5+")
    f["unit"] = np.where(f.is_defense.eq(1), dz, fw)
    return f


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    s25 = pd.read_csv(REBUILT / "season_frame_2025.csv.gz", float_precision="round_trip")
    s25 = s25.merge(pd.read_csv(TEAM), on=KEYS, how="left").merge(base.positions(), on="player_id", how="left")
    s25["is_defense"] = s25.is_defense.fillna(0).astype(int)
    prod = joblib.load(scorer.MODEL_PATH)
    s25["exp_pts"] = scorer.score(s25, prod).expected_points.to_numpy()
    s25["p1"] = poisson.sf(0, s25.exp_pts)
    s25 = ranks(s25[s25.mean_toi_last10.notna()])
    shots, _, shifts = joblib.load(EVENTS)
    from backend.nhl.scripts import research_nhl_points_onice_xg as ox
    logs = ox.db_logs_with_sog()[KEYS + ["shots_on_goal", "toi_minutes"]]
    s25 = s25.merge(logs, on=KEYS, how="left")
    s25["exp_sog"] = s25.d10_sog_per60 * s25.mean_toi_last10 / 60
    # A. results vs expectation by home/road and pregame unit.
    rows = []
    for (unit, home), g in s25.groupby(["unit", "is_home"]):
        n = len(g)
        rows.append({"unit": unit, "side": "home" if home else "road", "n": n,
                     "pts_actual": g.realized_points.mean(), "pts_model": g.exp_pts.mean(),
                     "pts_ratio": g.realized_points.sum() / g.exp_pts.sum(),
                     "p1_actual": (g.realized_points >= 1).mean(), "p1_model": g.p1.mean(),
                     "p1_gap": (g.realized_points >= 1).mean() - g.p1.mean(),
                     "p1_se": float(np.sqrt(g.p1.mean() * (1 - g.p1.mean()) / n)),
                     "sog_actual": g.shots_on_goal.mean(), "sog_expected": g.exp_sog.mean(),
                     "sog_ratio": g.shots_on_goal.sum() / g.exp_sog.sum()})
    a = pd.DataFrame(rows).sort_values(["unit", "side"])
    a.round(4).to_csv(OUT / "A_results_vs_expectation_by_unit.csv", index=False)
    # B. who faced whom.
    unit = s25[KEYS + ["is_home", "unit"]]
    sh = shifts[(shifts.game_id // 1_000_000 == 2025) & shifts.start.lt(3600)].merge(unit, on=KEYS, how="inner")
    out = []
    for gid, g in sh.groupby("game_id"):
        players = g.player_id.unique()
        idx = {p: i for i, p in enumerate(players)}
        on = np.zeros((len(players), 3600), dtype=np.uint8)
        for r in g.itertuples():
            on[idx[r.player_id], int(r.start):int(min(r.end, 3600))] = 1
        meta = g.drop_duplicates("player_id").set_index("player_id")
        teams = meta.team_id.unique()
        if len(teams) != 2:
            continue
        for t in teams:
            mine = [p for p in players if meta.loc[p, "team_id"] == t and meta.loc[p, "unit"] == "F1-3 top line"]
            if not mine:
                continue
            my_on = on[[idx[p] for p in mine]].max(axis=0).astype(bool)   # seconds any top-line forward is on
            total = my_on.sum()
            if total == 0:
                continue
            for ou in ("F1-3 top line", "F4-6", "F7-9", "F10+", "D1-2 top pair"):
                opp = [p for p in players if meta.loc[p, "team_id"] != t and meta.loc[p, "unit"] == ou]
                share = (my_on & on[[idx[p] for p in opp]].max(axis=0).astype(bool)).sum() / total if opp else np.nan
                out.append({"game_id": gid, "team_id": t, "is_home": bool(meta.loc[mine[0], "is_home"]),
                            "opponent_unit": ou, "share_of_top_line_time": share, "top_line_seconds": int(total)})
    b = pd.DataFrame(out)
    b.to_csv(OUT / "B_top_line_opponents_per_game.csv.gz", index=False)
    bs = b.groupby(["is_home", "opponent_unit"]).share_of_top_line_time.agg(["mean", "count"]).round(3).reset_index()
    bs["side"] = np.where(bs.is_home, "home", "road")
    bs.to_csv(OUT / "B_top_line_opponents_summary.csv", index=False)
    # Predictability: most-faced opposing forward unit per team-game; repeat rate vs the team's previous game.
    fw = b[b.opponent_unit.str.startswith("F")]
    top = fw.loc[fw.groupby(["game_id", "team_id"]).share_of_top_line_time.idxmax()][["game_id", "team_id", "is_home", "opponent_unit"]]
    top = top.sort_values(["team_id", "game_id"])
    top["prev"] = top.groupby("team_id").opponent_unit.shift(1)
    pred = {"repeat_rate_vs_previous_game": float((top.opponent_unit == top.prev).mean()),
            "most_faced_distribution": {str(k): v for k, v in top.groupby("is_home").opponent_unit.value_counts(normalize=True).round(3).items()}}
    summary = {"rows_A": int(len(s25)), "games_B": int(b.game_id.nunique()), "predictability": pred}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary, indent=1, default=str))
    print(a.round(3).to_string(index=False))
    print(bs.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
