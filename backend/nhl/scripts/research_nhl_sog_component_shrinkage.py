#!/usr/bin/env python3
"""SOG component model: choose the rate shrinkage, then compare with V2 (NHL Backlog P2.52 order, step 2).

Research only, read-only. The first component-only SOG model (P2.52 ablation,
model D) was 0.006 log loss behind V2 with predictions too compressed (slope
1.05-1.07): its EV and PP shot rates were pulled toward the league rate with
fixed weights (180 EV minutes, 30 PP minutes). Here the two weights are chosen
on training rows only (seasons 2024-2025 before 2026-01-01) by Poisson log
loss of the component total, then the reassembled component model is scored
against V2's recipe on the 2026-01..04 holdout and the live 2026-10-25..11-14
graded player-games, with the same features and data as the ablation.
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
from backend.nhl.scripts.research_nhl_sog_shrunk_rate import score  # noqa: E402
from backend.nhl.sog_hgb_features import FEATURES, _roll  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/sog_component_shrinkage_2026-11-15"
EV_GRID = (30.0, 60.0, 120.0, 180.0, 360.0, 720.0)
PP_GRID = (5.0, 10.0, 20.0, 30.0, 60.0, 120.0)


def component_total(f: pd.DataFrame, k_ev: float, k_pp: float, own_prior: bool = False) -> np.ndarray:
    """Expected shots from the components. own_prior: shrink toward the player's own
    prior-season situation rate (itself shrunk to the league rate) instead of the league rate."""
    ev_target, pp_target = (f.own_ev60_prior, f.own_pp60_prior) if own_prior else (f.lg_ev60, f.lg_pp60)
    ev = (f.ev_s20 + ev_target * k_ev / 60) / (f.ev_t20 + k_ev) * 60
    pp = (f.pp_s40 + pp_target * k_pp / 60) / (f.pp_t40 + k_pp) * 60
    lam = (ev * f.ev_toi_avg_10 + pp * f.exp_pp_min.fillna(f.pp_toi_avg_10_off)) / 60
    return lam.fillna(f.shrunk_lambda_180).clip(lower=0.02).to_numpy()


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    logs, pos = research.pull()
    frame = research.build(logs, pos)
    frame["game_id"], frame["player_id"] = frame.game_id.astype("int64"), frame.player_id.astype("int64")
    f = abl.add_features(frame, abl.situation_table())
    f = f.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    f["ev_s20"] = _roll(f, "player_id", "ev_shots", 20, "sum").fillna(0)
    f["ev_t20"] = _roll(f, "player_id", "ev_min", 20, "sum").fillna(0)
    f["pp_s40"] = _roll(f, "player_id", "pp_shots", 40, "sum").fillna(0)
    f["pp_t40"] = _roll(f, "player_id", "pp_min", 40, "sum").fillna(0)
    # Own prior-season situation rates, shrunk to the league rate by 300 EV / 60 PP minutes.
    season = f.groupby(["player_id", "season"]).agg(es=("ev_shots", "sum"), et=("ev_min", "sum"),
                                                     ps=("pp_shots", "sum"), pt=("pp_min", "sum")).reset_index()
    season["season"] += 1
    f = f.merge(season, on=["player_id", "season"], how="left")
    for c in ("es", "et", "ps", "pt"):
        f[c] = f[c].fillna(0)
    f["own_ev60_prior"] = (f.es + f.lg_ev60 * 300 / 60) / (f.et + 300) * 60
    f["own_pp60_prior"] = (f.ps + f.lg_pp60 * 60 / 60) / (f.pt + 60) * 60
    f = f.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    ok = f.toi_avg_10.notna() & f.shrunk_lambda_180.notna()
    train_all = f[f.season.isin([2024, 2025]) & ok]
    cut = pd.Timestamp(abl.HOLDOUT_START)
    early = train_all[train_all.game_date < cut]
    test = train_all[(train_all.game_date >= cut) & (train_all.game_date < pd.Timestamp(abl.HOLDOUT_END))].copy()
    y_tr = early.shots_on_goal.to_numpy(int)
    grid = {(k_ev, k_pp): float(-poisson.logpmf(y_tr, component_total(early, k_ev, k_pp)).mean())
            for k_ev in EV_GRID for k_pp in PP_GRID}
    best = min(grid, key=grid.get)
    grid_own = {(k_ev, k_pp): float(-poisson.logpmf(y_tr, component_total(early, k_ev, k_pp, True)).mean())
                for k_ev in EV_GRID for k_pp in PP_GRID}
    best_own = min(grid_own, key=grid_own.get)
    summary: dict = {"chosen_on": "training rows before 2026-01-01 (seasons 2024-2025)",
                     "grid_best": {"k_ev": best[0], "k_pp": best[1], "train_log_loss": grid[best]},
                     "grid_ablation_setting": {"k_ev": 180.0, "k_pp": 30.0, "train_log_loss": grid[(180.0, 30.0)]},
                     "grid": {f"{a}/{b}": round(v, 6) for (a, b), v in grid.items()},
                     "own_prior_best": {"k_ev": best_own[0], "k_pp": best_own[1], "train_log_loss": grid_own[best_own]}}
    a_model = abl._fit(early, FEATURES)
    test["A_V2_recipe"] = a_model.predict(test[FEATURES].astype(float).to_numpy())
    test["D_components_old"] = component_total(test, 180.0, 30.0)
    test["D_components_tuned"] = component_total(test, *best)
    test["D_components_own_prior"] = component_total(test, *best_own, True)
    cols = {"A_production": "A_V2_recipe", "D_old": "D_components_old", "D_tuned": "D_components_tuned",
            "D_own_prior": "D_components_own_prior"}
    y = test.shots_on_goal.to_numpy(int)
    summary["holdout"] = {"rows": int(len(test)), "scores": {k: score(y, test[c].to_numpy()) for k, c in cols.items()},
                          "paired_vs_A": {k: research.paired_ci(test, c, "A_V2_recipe") for k, c in cols.items()
                                          if c != "A_V2_recipe"}}
    live = f[(f.season == 2026) & (f.game_date >= pd.Timestamp(abl.LIVE_START))
             & (f.game_date <= pd.Timestamp(abl.LIVE_END)) & ok].copy()
    from backend.nhl.scripts import build_nhl_weekly_model_evaluation as weekly
    graded = []
    for day in weekly._days(abl.LIVE_START, abl.LIVE_END):
        s = weekly._summary(day)
        if s:
            r = weekly._cached_rows("sog", day, s["summary_identity"])
            if not r.empty:
                graded.append(r[r.line.eq(1.5)][["game_id", "player_id"]])
    live = live.merge(pd.concat(graded).astype("int64").drop_duplicates(), on=["game_id", "player_id"], how="inner")
    live["A_V2_recipe"] = abl._fit(train_all, FEATURES).predict(live[FEATURES].astype(float).to_numpy())
    live["D_components_old"] = component_total(live, 180.0, 30.0)
    live["D_components_tuned"] = component_total(live, *best)
    live["D_components_own_prior"] = component_total(live, *best_own, True)
    y = live.shots_on_goal.to_numpy(int)
    summary["live"] = {"rows": int(len(live)), "scores": {k: score(y, live[c].to_numpy()) for k, c in cols.items()},
                       "paired_vs_A": {k: research.paired_ci(live, c, "A_V2_recipe") for k, c in cols.items()
                                       if c != "A_V2_recipe"}}
    for part, fr in (("holdout", test), ("live", live)):
        b = fr.assign(pp_band=pd.cut(fr.pp_toi_avg_10.fillna(0), [-0.01, 0.25, 1, 2, 3, 99],
                                     labels=["0-0.25", "0.25-1", "1-2", "2-3", "3+"]))
        rows = []
        for band, g in b.groupby("pp_band", observed=True):
            row = {"pp_band": band, "n": len(g), "actual": float(g.shots_on_goal.mean())}
            for k, c in cols.items():
                row[f"{k}_mean"] = float(g[c].mean())
                row[f"{k}_ll"] = float(-poisson.logpmf(g.shots_on_goal.to_numpy(int), g[c].clip(0.02)).mean())
            rows.append(row)
        pd.DataFrame(rows).to_csv(OUT / f"{part}_by_pp_band.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    print(json.dumps(summary["grid_best"]), json.dumps(summary["grid_ablation_setting"]), json.dumps(summary["own_prior_best"]))
    for part in ("holdout", "live"):
        s = summary[part]
        print(part, s["rows"], {k: (round(v["poisson_log_loss"], 5), round(v["slope"], 3)) for k, v in s["scores"].items()},
              {k: (round(v["mean"], 5), [round(x, 5) for x in v["ci95"]]) for k, v in s["paired_vs_A"].items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
