#!/usr/bin/env python3
"""Research: expected team goals per game, then as a Points input (P2.39, 2026-11-01).

Points are a player's share of his team's goals times how many goals the team
scores. The Points model had only the share side; this builds the team side.

Team-game table: goals from the official outcomes the Points labels use
(restored 2023-25, 2026 reconciliation packages; skater goals summed, so
shootout goals are excluded as they are from points), shots on goal from the
skater logs; read-only research login. Strict-prior features for the team and
its opponent: season-to-date goals for/against and shots for/against per
game, shrunk toward the team's previous season (itself half-shrunk to the
league; 2022-23 from the NHL stats API team summary), the last 10 games, rest
days, home, and the league scoring level so far this season. Team-goals models
(Poisson GLM, production-recipe HGB) are fitted on 2023-24 and tested on 2025
and 2026 against a league-and-home baseline.

Points: expected team goals (and the player's share of his team's goals) are
added to the own-rate candidate HGB+OWN_k30 and judged with the operator's
per-tier gate (research_nhl_points_hgb_own_rate.py).
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
from sklearn.linear_model import PoissonRegressor
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/team_goals_2026-11-01"
TEAM_2022 = (ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-11-01_skater_season_totals"
             / "team_totals_season=20222023.csv")
K_TEAM = 20  # games of prior weight on a team's previous-season rate
STATS = ("gf", "ga", "sf", "sa")
TEAM_FEATURES = ["is_home", "rest_days", "opp_rest_days", "league_gpg_to_date",
                 *[f"{side}{s}_{w}" for side in ("", "opp_") for s in STATS for w in ("shr", "l10")]]


def team_games() -> tuple[pd.DataFrame, dict]:
    logs, audit = refit.pull_history()
    frozen = pd.read_csv(refit.FROZEN_FRAME, low_memory=False)
    frozen["game_id"] = frozen.game_id.astype("int64"); frozen["player_id"] = frozen.player_id.astype("int64")
    outcomes, _ = refit.official_outcomes(frozen)
    x = logs.drop(columns=["goals", "assists"], errors="ignore").merge(
        outcomes[["game_id", "player_id", "goals"]], on=["game_id", "player_id"], how="inner")
    x["goals"] = pd.to_numeric(x.goals, errors="coerce")
    g = (x.groupby(["canonical_season", "game_date", "game_id", "team_id", "home_team_id", "away_team_id"], as_index=False)
         .agg(gf=("goals", "sum"), sf=("shots_on_goal", "sum"), skaters=("player_id", "size")))
    g["opp_id"] = np.where(g.team_id == g.home_team_id, g.away_team_id, g.team_id * 0 + g.home_team_id)
    g = g.merge(g[["game_id", "team_id", "gf", "sf"]].rename(columns={"team_id": "opp_id", "gf": "ga", "sf": "sa"}),
                on=["game_id", "opp_id"], how="inner")
    g["is_home"] = (g.team_id == g.home_team_id).astype(int)
    g["game_date"] = pd.to_datetime(g.game_date)
    side = logs[["game_id", "player_id", "team_id"]].drop_duplicates(["game_id", "player_id"])
    return g.sort_values(["team_id", "game_date", "game_id"]).reset_index(drop=True), {
        "login": audit["login"], "snapshot_utc": audit["transaction_timestamp_utc"], "player_team": side}


def features(g: pd.DataFrame) -> pd.DataFrame:
    g = g.copy()
    api = pd.read_csv(TEAM_2022)
    season_tot = g.groupby(["team_id", "canonical_season"]).agg(
        games=("gf", "size"), **{s: (s, "sum") for s in STATS}).reset_index()
    season_tot = pd.concat([season_tot, pd.DataFrame({
        "team_id": api.team_id, "canonical_season": 2022, "games": api.games, "gf": api.goals_for,
        "ga": api.goals_against, "sf": api.shots_for, "sa": api.shots_against})], ignore_index=True)
    league = season_tot.groupby("canonical_season").apply(
        lambda t: pd.Series({s: t[s].sum() / t.games.sum() for s in STATS}), include_groups=False)
    prev = season_tot.assign(canonical_season=season_tot.canonical_season + 1)
    for s in STATS:
        prev[f"prev_{s}"] = prev[s] / prev.games
    g = g.merge(prev[["team_id", "canonical_season", *[f"prev_{s}" for s in STATS]]],
                on=["team_id", "canonical_season"], how="left")
    by = g.groupby(["team_id", "canonical_season"], sort=False)
    g["games_prior"] = by.cumcount()
    for s in STATS:
        lg_prev = g.canonical_season.map(lambda y: league.loc[y - 1, s] if (y - 1) in league.index else league.iloc[0][s])
        prior = 0.5 * g[f"prev_{s}"].fillna(lg_prev) + 0.5 * lg_prev  # a new or relocated team keeps the league rate
        cum = by[s].transform(lambda v: v.shift(1).cumsum()).fillna(0)
        g[f"{s}_shr"] = (cum + K_TEAM * prior) / (g.games_prior + K_TEAM)
        g[f"{s}_l10"] = by[s].transform(lambda v: v.shift(1).rolling(10, min_periods=1).mean()).fillna(prior)
    g["rest_days"] = by.game_date.transform(lambda d: d.diff().dt.days).fillna(7).clip(upper=7)
    # League goals per team-game so far this season (all games on earlier dates).
    daily = g.groupby(["canonical_season", "game_date"]).agg(goals=("gf", "sum"), n=("gf", "size")).reset_index()
    daily = daily.sort_values(["canonical_season", "game_date"])
    sb = daily.groupby("canonical_season")
    daily["league_gpg_to_date"] = (sb.goals.transform(lambda v: v.shift(1).cumsum())
                                   / sb.n.transform(lambda v: v.shift(1).cumsum()))
    daily["league_gpg_to_date"] = daily.league_gpg_to_date.fillna(
        daily.canonical_season.map(lambda y: league.loc[y - 1, "gf"] if (y - 1) in league.index else league.iloc[0]["gf"]))
    g = g.merge(daily[["canonical_season", "game_date", "league_gpg_to_date"]], on=["canonical_season", "game_date"])
    opp = g[["game_id", "team_id", "rest_days", *[f"{s}_{w}" for s in STATS for w in ("shr", "l10")]]]
    opp = opp.rename(columns={"team_id": "opp_id", "rest_days": "opp_rest_days",
                              **{f"{s}_{w}": f"opp_{s}_{w}" for s in STATS for w in ("shr", "l10")}})
    return g.merge(opp, on=["game_id", "opp_id"], how="left")


def fit_team(train: pd.DataFrame) -> dict:
    x = train[TEAM_FEATURES].astype(float).to_numpy()
    y = train.gf.to_numpy(float)
    scaler = StandardScaler().fit(x)
    xs = scaler.transform(x)
    glm = PoissonRegressor(alpha=1e-4, max_iter=1000).fit(xs, y)
    hgb = HistGradientBoostingRegressor(**base.HGB_KW).fit(xs, y)
    return {"scaler": scaler, "glm": glm, "hgb": hgb}


def predict_team(m: dict, frame: pd.DataFrame) -> dict[str, np.ndarray]:
    xs = m["scaler"].transform(frame[TEAM_FEATURES].astype(float).to_numpy())
    return {"glm": m["glm"].predict(xs), "hgb": np.maximum(m["hgb"].predict(xs), 1e-6)}


def poisson_ll(y: np.ndarray, mu: np.ndarray) -> float:
    return float(-poisson.logpmf(y, np.maximum(mu, 1e-9)).mean())


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    cache = OUT / "team_games.joblib"
    if not cache.exists():
        joblib.dump(team_games(), cache)
    g, meta = joblib.load(cache)
    g = features(g)
    train = g[g.canonical_season.isin([2023, 2024])].reset_index(drop=True)
    tests = {"2025": g[g.canonical_season.eq(2025)].reset_index(drop=True),
             "2026": g[g.canonical_season.eq(2026)].reset_index(drop=True)}
    m = fit_team(train)
    joblib.dump(m, OUT / "team_goals_model.joblib")
    team_eval = {}
    for name, t in tests.items():
        p = predict_team(m, t)
        base_mu = t.league_gpg_to_date * np.where(t.is_home == 1, 1.04, 0.96)
        y = t.gf.to_numpy()
        team_eval[name] = {"team_games": len(t), "mean_actual": float(y.mean()),
                           "baseline_ll": poisson_ll(y, base_mu.to_numpy()),
                           "glm_ll": poisson_ll(y, p["glm"]), "hgb_ll": poisson_ll(y, p["hgb"]),
                           "glm_mean": float(p["glm"].mean()), "hgb_mean": float(p["hgb"].mean()),
                           "glm_corr": float(np.corrcoef(p["glm"], y)[0, 1]),
                           "glm_top_vs_bottom_quintile_actual": [
                               float(y[p["glm"] >= np.quantile(p["glm"], .8)].mean()),
                               float(y[p["glm"] <= np.quantile(p["glm"], .2)].mean())]}
    print(json.dumps(team_eval, indent=1))
    g["exp_team_goals"] = predict_team(m, g)["glm"]
    # Points: attach expected team goals to every player target.
    data = joblib.load(base.OUT / "matrices.joblib")
    totals = data["season_totals"]
    api = pd.read_csv(own.SEASON_2022)
    totals = pd.concat([totals, pd.DataFrame({"player_id": api.player_id, "canonical_season": 2022,
                                               "season_points": api.points, "season_games": api.games})], ignore_index=True)
    pt = meta["player_team"]
    team_cols = g[["game_id", "team_id", "exp_team_goals", "gf_shr", "opp_ga_shr"]]

    def attach(frame: pd.DataFrame) -> pd.DataFrame:
        frame = own.add_own_rate(frame, totals).merge(pt, on=["game_id", "player_id"], how="left")
        frame = frame.merge(team_cols, on=["game_id", "team_id"], how="left")
        frame["goal_share"] = frame.own_rate_k30 / frame.gf_shr
        return frame

    hist = attach(data["historical"])
    f26 = attach(data["2026"]).reset_index(drop=True)
    rows = data["market_2026"].merge(f26[own.KEYS], on=own.KEYS, how="inner").reset_index(drop=True)
    cover = {"historical": float(hist.exp_team_goals.notna().mean()), "2026": float(f26.exp_team_goals.notna().mean())}
    trainp = hist[hist.canonical_season.isin([2023, 2024])].reset_index(drop=True)
    hold = hist[hist.canonical_season.eq(2025)].reset_index(drop=True)
    k30 = own.CORE + ["own_rate_k30", "own_games"]
    specs = {"HGB+OWN_k30": (own.fit_hgb, k30),
             "HGB+OWN_k30+TEAMGOALS": (own.fit_hgb, k30 + ["exp_team_goals"]),
             "HGB+OWN_k30+TEAMGOALS+SHARE": (own.fit_hgb, k30 + ["exp_team_goals", "goal_share"]),
             "OFFSET+OWN_k30+TEAMGOALS": (own.fit_offset, k30 + ["exp_team_goals"])}
    models = {name: fn(trainp, f) for name, (fn, f) in specs.items()}
    joblib.dump(models, OUT / "points_candidates.joblib")
    production = joblib.load(own.scorer.MODEL_PATH)
    hp = {"PRODUCTION": own.predict(production, hold), **{k: own.predict(v, hold) for k, v in models.items()}}
    p26 = {"PRODUCTION": own.predict(production, f26), **{k: own.predict(v, f26) for k, v in models.items()}}
    hold_rows, levels = own.holdout_tables(hp, hold)
    market = own.market_tables(p26, rows, f26)
    gates = own.gate(hold_rows)
    # The step that matters: team goals on top of own rate, same rows.
    step = []
    pts = hold.realized_points.to_numpy(float)
    dates = hold.slate_date.astype(str).to_numpy()
    for tier, mask in own.tiers(hold).items():
        for label in [k for k in models if k != "HGB+OWN_k30"]:
            for line in own.LINES:
                y = (pts > line).astype(float)
                r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), hp[label]), y)[mask],
                                          base.log_loss_rows(poisson.sf(int(line), hp["HGB+OWN_k30"]), y)[mask], dates[mask])
                step.append({"tier": tier, "line": line, "model": label, "n": r["n"],
                             "diff_vs_own_k30_x1000": round(r["mean_diff"] * 1e3, 2),
                             "ci_x1000": [round(c * 1e3, 2) for c in r["ci95"]]})
    step = pd.DataFrame(step)
    for name, frame in (("team_goals_eval", pd.DataFrame(team_eval).T.reset_index()),
                        ("points_holdout_paired_by_tier", hold_rows), ("points_holdout_levels_by_tier", levels),
                        ("points_two_sided_2026_by_tier", market), ("points_tier_gate", gates),
                        ("points_step_vs_own_k30", step)):
        frame.to_csv(OUT / f"{name}.csv", index=False)
    summary = {"team_eval": team_eval, "team_features": TEAM_FEATURES, "k_team": K_TEAM,
               "team_goals_coverage_on_points_rows": cover, "database_login": meta["login"],
               "snapshot_utc": meta["snapshot_utc"], "points_candidates": {k: v[1] for k, v in specs.items()}}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    pd.set_option("display.width", 250); pd.set_option("display.max_rows", 1000); pd.set_option("display.max_colwidth", 60)
    print(json.dumps(cover))
    print(gates.to_string(index=False))
    print(step[step.n >= own.MIN_TIER].pivot_table(index=["tier", "line"], columns="model",
                                                   values="diff_vs_own_k30_x1000").to_string())
    print(market[market.model.isin(list(models) + ["MARKET"])].pivot_table(
        index="tier", columns="model", values="diff_vs_production_x1000").to_string())


if __name__ == "__main__":
    main()
