#!/usr/bin/env python3
"""Points refit candidates on the rebuilt frames (Utah 2024-25 included): two vs three seasons,
production recipe vs xG + relative shots (NHL Backlog P2.46 / P2.57).

Research only, read-only. Frames: rebuild_nhl_points_training_frames.py (2023-24 + 2024-25
training with the 82 Utah games; the 2025-26 season). Inputs added for the input-change
candidates: ixg_d10_per60 and sh_pct_shrunk (research_nhl_points_onice_xg, same xG fit on
2023-24), and the three shot-volume inputs relative to the league mean of earlier slates
(research_nhl_points_minutes_scaling.add_relative). Candidates, goals + assists pairs:

  ROUTE_B_2S   production recipe on 2023-24 + 2024-25 with Utah (the data-correction refit)
  XG_REL_2S    + ixG and shooting %, shot inputs relative, same two seasons
  ROUTE_B_3S   production recipe on 2023-24 .. 2025-26
  XG_REL_3S    xG + relative on three seasons

Scored against production c940ae2f: 2025-26 holdout (two-season candidates only) and live
2026-11-01..11-15 (all), paired log loss at 0.5/1.5/2.5, two-sided lines vs the market,
and the input watch's biggest Points disagreements.
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
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_minutes_scaling as ms  # noqa: E402
from backend.nhl.scripts import research_nhl_points_onice_xg as ox  # noqa: E402
from backend.nhl.scripts import research_nhl_points_per_minute_corrected as pmc  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

REBUILT = ROOT / "artifacts/analysis/nhl/points_training_rebuild_2026-11-16"
OUT = ROOT / "artifacts/analysis/nhl/points_refit_candidates_2026-11-16"
KEYS = ["game_id", "player_id"]
F = list(freeze.FEATURES)
XG = F + ["ixg_d10_per60", "sh_pct_shrunk"]


def shot_fallback(frames: list[pd.DataFrame]) -> dict:
    means = {}
    for x in frames:
        d = pd.to_datetime(x.slate_date if "slate_date" in x else x.game_date)
        s = pmc.season_of(d)
        for season in set(s):
            for c in ms.SHOTS:
                means[(season, c)] = float(pd.to_numeric(x[c], errors="coerce")[s == season].mean())
    fb = {(s + 1, c): v for (s, c), v in means.items()}
    for c in ms.SHOTS:
        fb.setdefault((2023, c), means[(2023, c)])
    return fb


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    train2 = pd.read_csv(REBUILT / "training_frame_2023_2024.csv.gz", float_precision="round_trip")
    s25 = pd.read_csv(REBUILT / "season_frame_2025.csv.gz", float_precision="round_trip")
    live = ms.frames()["live"].copy()
    live["slate_date"] = live.game_date.astype(str)
    market = ms.frames()["market"]
    # xG inputs (events cache from research_nhl_points_onice_xg, Utah and fills included).
    shots, goals, shifts = ox.build_events()
    s26, g26 = [], []
    for f in sorted(ox.PBP_2026.glob("game=*.play-by-play.json")):
        a, b = ox.parse_pbp(json.load(open(f)))
        s26 += a; g26 += b
    shots = pd.concat([shots, pd.DataFrame(s26)], ignore_index=True)
    goals = pd.concat([goals, pd.DataFrame(g26)], ignore_index=True)
    shifts = pd.concat([shifts, ox.shifts_2025(2026)], ignore_index=True)
    pg, info = ox.per_game(shots, goals, shifts)
    logs = ox.db_logs_with_sog()
    cache = OUT / "rolling_inputs.csv.gz"
    if cache.is_file():
        roll = pd.read_csv(cache)
    else:
        keys = pd.concat([x[KEYS + ["slate_date"]] for x in (train2, s25, live)]).drop_duplicates(KEYS)
        roll = ox.rolling(keys, pg, logs)
        roll.to_csv(cache, index=False, float_format="%.10g")
    train2, s25, live = (x.merge(roll, on=KEYS, how="left") for x in (train2, s25, live))
    for x in (train2, s25, live):
        x["attempts_d10_per60"] = scorer.impute_unknown_attempts(x)
    fb = shot_fallback([train2, s25, live])
    train2, s25, live = (ms.add_relative(x, fb) for x in (train2, s25, live))
    rel = lambda x: x.assign(**{c: x[ms.REL[c]] for c in ms.SHOTS})  # noqa: E731
    train3 = pd.concat([train2, s25], ignore_index=True)
    specs = {"ROUTE_B_2S": (train2, F, False), "XG_REL_2S": (train2, XG, True),
             "ROUTE_B_3S": (train3, F, False), "XG_REL_3S": (train3, XG, True)}
    models = {k: ms.fit(rel(t) if r else t, f) for k, (t, f, r) in specs.items()}
    again = ms.fit(train2, F)
    prod = joblib.load(scorer.CONTRACT_PATH.parent / json.loads(scorer.CONTRACT_PATH.read_text())["fitted_artifact"])
    s25["PRODUCTION"] = scorer.score(s25, prod).expected_points.to_numpy()
    live["PRODUCTION"] = scorer.score(live, prod).expected_points.to_numpy()
    for frame in (s25, live):
        for k, (t, f, r) in specs.items():
            frame[k] = ms.predict(models[k], rel(frame) if r else frame)
    summary = {"training_rows": {"2S": int(len(train2)), "3S": int(len(train3))}, "holdout_rows": int(len(s25)),
               "live_rows": int(len(live)), "xg_by_season": info["xg_by_season"],
               "route_b_2s_reproducible": bool(np.array_equal(ms.predict(again, s25), s25.ROUTE_B_2S.to_numpy())),
               "mean": {"holdout": {"actual": float(s25.realized_points.mean()),
                                    **{k: float(s25[k].mean()) for k in ["PRODUCTION", "ROUTE_B_2S", "XG_REL_2S"]}},
                        "live": {"actual": float(live.realized_points.mean()),
                                 **{k: float(live[k].mean()) for k in ["PRODUCTION", *specs]}}}}
    tabs = []
    s25["role_change"] = s25["role_up"] = s25["role_down"] = False
    hold_masks = {k: v for k, v in pmc.groups(s25).items() if k in ("all", "toi_17_20", "toi_20_plus", "toi_under_13")}
    for k in ("ROUTE_B_2S", "XG_REL_2S"):
        tabs.append(pmc.paired(s25, k, "PRODUCTION", hold_masks).assign(part="holdout_2025", model=k))
    live_masks = {k: v for k, v in pmc.groups(live).items() if k in ("all", "role_change", "role_up", "toi_20_plus")}
    for k in specs:
        tabs.append(pmc.paired(live, k, "PRODUCTION", live_masks).assign(part="live", model=k))
    paired = pd.concat(tabs, ignore_index=True)[["part", "model", "group", "line", "n", "diff_x1000", "ci_x1000"]]
    paired.to_csv(OUT / "paired_vs_production.csv", index=False)
    mk = live.merge(market, on=KEYS, how="inner")
    y = (mk.realized_points > mk.line).astype(float).to_numpy()
    kk = mk.line.astype(int).to_numpy()
    rows = []
    for name, m in {"all": np.ones(len(mk), bool), "line_0.5": mk.line.eq(0.5).to_numpy(),
                    "role_change": mk.role_change.to_numpy(bool)}.items():
        row = {"group": name, "n": int(m.sum()), "MARKET": float(base.log_loss_rows(mk.market_p_over.to_numpy()[m], y[m]).mean())}
        for k in ["PRODUCTION", *specs]:
            row[k] = float(base.log_loss_rows(poisson.sf(kk, mk[k].to_numpy())[m], y[m]).mean())
        rows.append(row)
    vs_market = pd.DataFrame(rows)
    vs_market.to_csv(OUT / "live_two_sided_vs_market.csv", index=False)
    if pmc.DISAGREE.is_file():
        d = pd.read_csv(pmc.DISAGREE)
        d = d[d.lane.eq("points")].merge(live[KEYS + ["PRODUCTION", *specs]], on=KEYS, how="inner")
        over = d.side.eq("OVER").to_numpy()
        row = {"plays": len(d), "won": float(d.won.mean()), "market": float(d.market_p.mean())}
        for k in ["PRODUCTION", *specs]:
            p = poisson.sf(d.line.astype(int).to_numpy(), d[k].to_numpy())
            row[k] = float(np.where(over, p, 1 - p).mean())
        pd.DataFrame([row]).to_csv(OUT / "live_biggest_points_disagreements.csv", index=False)
        summary["biggest_disagreements"] = row
    joblib.dump(models, OUT / "candidates.joblib")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary, indent=1, default=str))
    print(paired.to_string(index=False))
    print(vs_market.round(4).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
