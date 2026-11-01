#!/usr/bin/env python3
"""Research: goal and assist component models feeding Points (P2.39, 2026-11-01).

Operator idea: goals and assists are their own bet markets and have different
drivers (shots and finishing vs linemates and the power play), so separate
models summed into expected points may estimate the total better.

- G and A: the production HGB recipe fitted to official goals and official
  assists, each on the ten core inputs, the own points rate (``own_rate_k30``,
  games) and the player's own goal and assist rates (previous season plus this
  season so far, shrunk to last season's league rate by 30 games; 2022-23 from
  the NHL stats API). Expected points = G + A (Poisson sum).
- CONTROL: one points model with the same added inputs, to separate the value
  of splitting from the value of the inputs.

Labels: the restored 2023-25 official outcomes and the 2026 reconciliation
packages (the P2.31 refit's ``official_outcomes``). Judged by the per-tier gate
on the 2025 holdout (low-usage forwards excluded) and on 2026 two-sided lines.
Read-only; nothing changes production.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/goal_assist_2026-11-01"
KEYS = ["game_id", "player_id"]
K = 30
EXCLUDED = {"low_usage_forwards (F, TOI < 13)"}


def component_rates(frame: pd.DataFrame, outcomes: pd.DataFrame, api: pd.DataFrame) -> pd.DataFrame:
    """Strict-prior shrunk goal and assist rates per target row."""
    o = outcomes[KEYS + ["game_date", "canonical_season", "goals", "assists"]].copy()
    o["game_date"] = pd.to_datetime(o.game_date)
    o[["goals", "assists"]] = o[["goals", "assists"]].apply(pd.to_numeric, errors="coerce").fillna(0)
    season = o.groupby(["player_id", "canonical_season"]).agg(g=("game_id", "size"), go=("goals", "sum"), a=("assists", "sum")).reset_index()
    season = pd.concat([season, pd.DataFrame({"player_id": api.player_id, "canonical_season": 2022, "g": api.games,
                                              "go": api.goals, "a": api.assists})], ignore_index=True)
    league = season.groupby("canonical_season").apply(lambda t: pd.Series({"go": t.go.sum() / t.g.sum(), "a": t.a.sum() / t.g.sum()}),
                                                      include_groups=False)
    first = league.iloc[0]
    prev = season.assign(canonical_season=season.canonical_season + 1).rename(columns={"g": "pg", "go": "pgo", "a": "pa"})
    o = o.sort_values(["player_id", "game_date", "game_id"])
    by = o.groupby(["player_id", "canonical_season"], sort=False)
    o["cg"] = by.cumcount()
    o["cgo"] = by.goals.transform(lambda v: v.shift(1).cumsum()).fillna(0)
    o["ca"] = by.assists.transform(lambda v: v.shift(1).cumsum()).fillna(0)
    f = frame.merge(o[KEYS + ["cg", "cgo", "ca", "goals", "assists"]], on=KEYS, how="left")
    f = f.merge(prev, on=["player_id", "canonical_season"], how="left").fillna({"pg": 0, "pgo": 0, "pa": 0})
    # Targets outside the outcome table (none expected) keep this season's counts from the frame.
    f["cg"] = f.cg.fillna(f.current_season_games_prior)
    f[["cgo", "ca"]] = f[["cgo", "ca"]].fillna(0)
    lg_go = f.canonical_season.map(lambda s: league.go.get(s - 1, first.go))
    lg_a = f.canonical_season.map(lambda s: league.a.get(s - 1, first.a))
    f["goal_rate"] = (f.pgo + f.cgo + K * lg_go) / (f.pg + f.cg + K)
    f["assist_rate"] = (f.pa + f.ca + K * lg_a) / (f.pg + f.cg + K)
    return f


def fit_count(train: pd.DataFrame, features: list[str], target: str) -> dict:
    x = base.x_matrix(train, features)
    scaler = StandardScaler().fit(x)
    model = HistGradientBoostingRegressor(**base.HGB_KW).fit(scaler.transform(x), train[target].to_numpy(float))
    return {"feature_columns": features, "scaler": scaler, "model": model}


def predict_count(m: dict, frame: pd.DataFrame) -> np.ndarray:
    return np.maximum(m["model"].predict(m["scaler"].transform(base.x_matrix(frame, m["feature_columns"]))), 1e-8)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data = joblib.load(base.OUT / "matrices.joblib")
    api_pts = pd.read_csv(own.SEASON_2022)
    totals = pd.concat([data["season_totals"], pd.DataFrame({"player_id": api_pts.player_id, "canonical_season": 2022,
                                                              "season_points": api_pts.points, "season_games": api_pts.games})],
                       ignore_index=True)
    frozen = pd.read_csv(refit.FROZEN_FRAME, low_memory=False)
    frozen["game_id"] = frozen.game_id.astype("int64"); frozen["player_id"] = frozen.player_id.astype("int64")
    outcomes, audit = refit.official_outcomes(frozen)
    outcomes["game_id"] = outcomes.game_id.astype("int64"); outcomes["player_id"] = outcomes.player_id.astype("int64")
    hist = component_rates(own.add_own_rate(data["historical"], totals), outcomes, api_pts)
    f26 = component_rates(own.add_own_rate(data["2026"], totals), outcomes, api_pts).reset_index(drop=True)
    rows = data["market_2026"].merge(f26[KEYS], on=KEYS, how="inner").reset_index(drop=True)
    train = hist[hist.canonical_season.isin([2023, 2024]) & hist.goals.notna()].reset_index(drop=True)
    hold = hist[hist.canonical_season.eq(2025)].reset_index(drop=True)
    label_check = float((train.goals + train.assists - train.realized_points).abs().max())
    k30 = own.CORE + ["own_rate_k30", "own_games"]
    comp = k30 + ["goal_rate", "assist_rate"]
    g_model, a_model = fit_count(train, comp, "goals"), fit_count(train, comp, "assists")
    control = own.fit_hgb(train, comp)
    base_k30 = own.fit_hgb(train, k30)
    joblib.dump({"goals": g_model, "assists": a_model, "control": control}, OUT / "candidates.joblib")
    production = joblib.load(own.scorer.MODEL_PATH)

    def preds(frame):
        g, a = predict_count(g_model, frame), predict_count(a_model, frame)
        return {"PRODUCTION": own.predict(production, frame), "HGB+OWN_k30": own.predict(base_k30, frame),
                "GOALS+ASSISTS": g + a, "CONTROL_points_with_rates": own.predict(control, frame)}, g, a

    hp, hg, ha = preds(hold)
    p26, _, _ = preds(f26)
    hold_rows, levels = own.holdout_tables(hp, hold)
    gates = own.gate(hold_rows[~hold_rows.tier.isin(EXCLUDED)])
    market = own.market_tables(p26, rows, f26)
    step = []
    pts, d = hold.realized_points.to_numpy(float), hold.slate_date.astype(str).to_numpy()
    for tier, mask in own.tiers(hold).items():
        for label in ("GOALS+ASSISTS", "CONTROL_points_with_rates"):
            for line in own.LINES:
                y = (pts > line).astype(float)
                r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), hp[label]), y)[mask],
                                          base.log_loss_rows(poisson.sf(int(line), hp["HGB+OWN_k30"]), y)[mask], d[mask])
                step.append({"tier": tier, "line": line, "model": label, "n": r["n"],
                             "diff_vs_own_k30_x1000": round(r["mean_diff"] * 1e3, 2), "ci_x1000": [round(c * 1e3, 2) for c in r["ci95"]]})
    step = pd.DataFrame(step)
    # Component quality on their own targets (goals, assists): Poisson log loss vs the points model split by league shares.
    gl, al = hold.goals.to_numpy(float), hold.assists.to_numpy(float)
    share_g = float(train.goals.sum() / train.realized_points.sum())
    comp_eval = {"goals_ll_component": float(-poisson.logpmf(gl, hg).mean()),
                 "goals_ll_points_x_league_share": float(-poisson.logpmf(gl, np.maximum(hp["HGB+OWN_k30"] * share_g, 1e-9)).mean()),
                 "assists_ll_component": float(-poisson.logpmf(al, ha).mean()),
                 "assists_ll_points_x_league_share": float(-poisson.logpmf(al, np.maximum(hp["HGB+OWN_k30"] * (1 - share_g), 1e-9)).mean()),
                 "goal_share_of_points_train": share_g}
    for name, frame in (("holdout_paired_by_tier", hold_rows), ("holdout_levels_by_tier", levels),
                        ("tier_gate_excluding_low_usage_forwards", gates), ("two_sided_2026_by_tier", market),
                        ("step_vs_own_k30", step)):
        frame.to_csv(OUT / f"{name}.csv", index=False)
    summary = {"training_rows": int(len(train)), "label_check_max_abs(goals+assists-points)": label_check,
               "component_eval_holdout_2025": comp_eval, "excluded_tiers": sorted(EXCLUDED), "labels": audit}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    pd.set_option("display.width", 250); pd.set_option("display.max_rows", 1000); pd.set_option("display.max_colwidth", 60)
    print(json.dumps(summary, indent=1, default=str))
    print(gates.to_string(index=False))
    print(step[step.n >= own.MIN_TIER].pivot_table(index=["tier", "line"], columns="model", values="diff_vs_own_k30_x1000").to_string())
    print(market.pivot_table(index="tier", columns="model", values="diff_vs_production_x1000").to_string())


if __name__ == "__main__":
    main()
