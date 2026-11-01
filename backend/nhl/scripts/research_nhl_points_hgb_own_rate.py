#!/usr/bin/env python3
"""Research: a player's own scoring rate as a Points input (P2.39, 2026-11-01).

Replaces group rules (the withdrawn global ``is_defense``) with the player's
own record, shrunk toward a league rate while he has few games (as the SOG
SHRUNK_180 rate does for shots):

    own_rate = (prior-season points + current-season points so far + k * mu)
             / (prior-season games  + current-season games so far  + k)

``mu`` is the previous season's league points per game, all skaters (or by
position group in the POSPRIOR variant; the pull toward it fades with games).
Prior-season totals are full regular seasons strictly before the target
season. 2023 rows have no 2022 season (restored outcomes start in 2023), so
their prior comes from the NHL stats API season summary (validated against
our 2023-24 outcomes); the TRAIN2024 variant trains on 2024 only.

Gate (operator, 2026-11-01): no player tier with n >= 200 worse than
production at any line on the 2025 holdout (paired log loss point estimate),
plus the 2026 two-sided lines reported by tier. Inputs: the cached month-end
matrices (``research_nhl_points_hgb_month_end_refit.py``). Local files only.
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
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/own_rate_2026-11-01"
CORE = list(scorer.CORE_FEATURES)
KEYS = ["game_id", "player_id"]
LINES = (0.5, 1.5, 2.5)
MIN_TIER = 200
SEASON_2022 = (ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-11-01_skater_season_totals"
               / "skater_totals_season=20222023.csv")


def add_own_rate(frame: pd.DataFrame, totals: pd.DataFrame, ks=(10, 30, 60)) -> pd.DataFrame:
    frame = frame.copy()
    prior = totals.assign(canonical_season=totals.canonical_season + 1).rename(
        columns={"season_points": "prior_points", "season_games": "prior_games"})
    frame = frame.drop(columns=["prior_points", "prior_games"], errors="ignore").merge(
        prior, on=["player_id", "canonical_season"], how="left")
    frame[["prior_points", "prior_games"]] = frame[["prior_points", "prior_games"]].fillna(0)
    pos = totals.merge(frame[["player_id", "is_defense"]].drop_duplicates("player_id"), on="player_id", how="left")
    pos["is_defense"] = pos.is_defense.fillna(0)
    league = totals.groupby("canonical_season").apply(lambda g: g.season_points.sum() / g.season_games.sum())
    by_pos = pos.groupby(["canonical_season", "is_defense"]).apply(lambda g: g.season_points.sum() / g.season_games.sum())
    first = int(league.index.min())
    # Previous season's league rate; the first season has none, so it uses its own (one constant).
    frame["mu_all"] = frame.canonical_season.map(lambda s: league.get(s - 1, league[first]))
    frame["mu_pos"] = [by_pos.get((s - 1, d), by_pos[(first, d)]) for s, d in zip(frame.canonical_season, frame.is_defense)]
    games = frame.prior_games + frame.current_season_games_prior
    points = frame.prior_points + frame.current_season_points_prior
    for k in ks:
        frame[f"own_rate_k{k}"] = (points + k * frame.mu_all) / (games + k)
        frame[f"own_rate_pos_k{k}"] = (points + k * frame.mu_pos) / (games + k)
    frame["prior_ppg"] = (frame.prior_points / frame.prior_games.replace(0, np.nan))
    frame["own_games"] = games
    return frame


def x_matrix(frame: pd.DataFrame, features: list[str]) -> np.ndarray:
    return base.x_matrix(frame, features)


def fit_hgb(train: pd.DataFrame, features: list[str]) -> dict:
    """Production recipe: fill zero, StandardScaler, HGB Poisson."""
    x = x_matrix(train, features)
    scaler = StandardScaler().fit(x)
    model = HistGradientBoostingRegressor(**base.HGB_KW).fit(scaler.transform(x), train.realized_points.to_numpy(float))
    return {"feature_columns": features, "scaler": scaler, "model": model}


def fit_offset(train: pd.DataFrame, features: list[str]) -> dict:
    m = base.fit_offset(train, features)
    return {"structure": "GLM_TIMES_HGB_OFFSET", "feature_columns": features,
            "scaler": m["scaler"], "glm": m["glm"], "model": m["offset"]}


def predict(model: dict, frame: pd.DataFrame) -> np.ndarray:
    return scorer.score(frame.assign(game_id=0, player_id=range(len(frame))), model).expected_points.to_numpy()


def tiers(frame: pd.DataFrame) -> dict[str, np.ndarray]:
    g = frame.current_season_games_prior.to_numpy()
    ppg = (frame.current_season_points_prior / frame.current_season_games_prior.replace(0, np.nan)).to_numpy()
    prior = frame.prior_ppg.to_numpy()
    d = frame.is_defense.to_numpy() == 1
    f = ~d
    pps = (frame.current_season_points_prior / frame.season_sog_prior.replace(0, np.nan)).to_numpy()
    f10 = f & (g >= 10) & np.isfinite(pps)
    cut = np.nanquantile(pps[f10], 2 / 3) if f10.any() else np.inf
    toi = frame.mean_toi_last10.to_numpy()
    return {
        "all": np.ones(len(frame), bool),
        "star_forwards (F, 10+ g, 1.0+ ppg)": f & (g >= 10) & (ppg >= 1.0),
        "star_forwards_early (F, <10 g, prior 1.0+ ppg)": f & (g < 10) & (prior >= 1.0),
        "high_scoring_defense (D, 10+ g, 0.8+ ppg)": d & (g >= 10) & (ppg >= 0.8),
        "top_defense (D, 10+ g, 1.0+ ppg)": d & (g >= 10) & (ppg >= 1.0),
        "high_scoring_defense_early (D, <10 g, prior 0.6+ ppg)": d & (g < 10) & (prior >= 0.6),
        "playmakers (F, 10+ g, top third points/SOG)": f10 & (pps >= cut),
        "low_usage_defense (D, TOI < 18)": d & (toi < 18),
        "low_usage_forwards (F, TOI < 13)": f & (toi < 13),
        "defense_all": d,
        "early_season (<10 g)": g < 10,
    }


def paired(loss_a, loss_b, dates):
    return base.paired_bootstrap(loss_a, loss_b, dates)


def holdout_tables(preds: dict, hold: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    pts = hold.realized_points.to_numpy(float)
    dates = hold.slate_date.astype(str).to_numpy()
    rows, levels = [], []
    for tier, mask in tiers(hold).items():
        for label, mu in preds.items():
            levels.append({"tier": tier, "model": label, "n": int(mask.sum()),
                           "mean_expected": float(mu[mask].mean()) if mask.any() else np.nan,
                           "mean_actual": float(pts[mask].mean()) if mask.any() else np.nan})
        if mask.sum() < 30:
            continue
        for line in LINES:
            y = (pts > line).astype(float)
            prod = base.log_loss_rows(poisson.sf(int(line), preds["PRODUCTION"]), y)
            for label, mu in preds.items():
                if label == "PRODUCTION":
                    continue
                r = paired(base.log_loss_rows(poisson.sf(int(line), mu), y)[mask], prod[mask], dates[mask])
                rows.append({"tier": tier, "line": line, "model": label, "n": r["n"],
                             "diff_x1000": round(r["mean_diff"] * 1e3, 2),
                             "ci_x1000": [round(c * 1e3, 2) for c in r["ci95"]]})
    return pd.DataFrame(rows), pd.DataFrame(levels)


def market_tables(preds: dict, rows: pd.DataFrame, f26: pd.DataFrame) -> pd.DataFrame:
    idx = rows[KEYS].merge(f26[KEYS].reset_index(), on=KEYS, how="left")["index"].to_numpy()
    y = (rows.points > rows.line).astype(float).to_numpy()
    thr = rows.line.astype(int).to_numpy()
    probs = {k: poisson.sf(thr, mu[idx]) for k, mu in preds.items()}
    probs["MARKET"] = rows.market_p_over.to_numpy()
    dates = rows.slate_date.astype(str).to_numpy()
    out = []
    for tier, mask in tiers(f26).items():
        m = mask[idx]
        if m.sum() < 30:
            continue
        prod = base.log_loss_rows(probs["PRODUCTION"][m], y[m])
        for label, p in probs.items():
            if label == "PRODUCTION":
                continue
            r = paired(base.log_loss_rows(p[m], y[m]), prod, dates[m])
            out.append({"tier": tier, "model": label, "n": r["n"], "diff_vs_production_x1000": round(r["mean_diff"] * 1e3, 2),
                        "ci_x1000": [round(c * 1e3, 2) for c in r["ci95"]]})
    return pd.DataFrame(out)


def gate(hold_rows: pd.DataFrame) -> pd.DataFrame:
    big = hold_rows[hold_rows.n >= MIN_TIER]
    g = big.groupby("model").apply(lambda t: pd.Series({
        "tiers_checked": t.tier.nunique(),
        "worst_tier": t.loc[t.diff_x1000.idxmax(), "tier"], "worst_line": t.loc[t.diff_x1000.idxmax(), "line"],
        "worst_diff_x1000": t.diff_x1000.max(),
        "tier_lines_worse": int((t.diff_x1000 > 0).sum()),
        "all_0.5": float(t[(t.tier == "all") & (t.line == 0.5)].diff_x1000.iloc[0]),
        "all_1.5": float(t[(t.tier == "all") & (t.line == 1.5)].diff_x1000.iloc[0]),
        "all_2.5": float(t[(t.tier == "all") & (t.line == 2.5)].diff_x1000.iloc[0]),
    }), include_groups=False)
    g["gate"] = np.where(g.tier_lines_worse == 0, "PASS", "FAIL")
    return g.reset_index()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data = joblib.load(base.OUT / "matrices.joblib")
    totals = data["season_totals"]
    # 2022-23 totals from the NHL stats API (fetch_nhl_skater_season_totals.py), the
    # prior season for 2023 rows; validated against our 2023-24 outcomes first.
    api = pd.read_csv(SEASON_2022)
    totals = pd.concat([totals, pd.DataFrame({"player_id": api.player_id, "canonical_season": 2022,
                                               "season_points": api.points, "season_games": api.games})],
                       ignore_index=True)
    hist = add_own_rate(data["historical"], totals)
    f26 = add_own_rate(data["2026"], totals).reset_index(drop=True)
    rows = data["market_2026"].merge(f26[KEYS], on=KEYS, how="inner").reset_index(drop=True)
    train = hist[hist.canonical_season.isin([2023, 2024])].reset_index(drop=True)
    train24 = hist[hist.canonical_season.eq(2024)].reset_index(drop=True)
    hold = hist[hist.canonical_season.eq(2025)].reset_index(drop=True)
    production = joblib.load(scorer.MODEL_PATH)
    specs = {}
    for k in (10, 30, 60):
        feats = CORE + [f"own_rate_k{k}", "own_games"]
        specs[f"HGB+OWN_k{k}"] = (fit_hgb, train, feats)
        specs[f"OFFSET+OWN_k{k}"] = (fit_offset, train, feats)
    specs["OFFSET"] = (fit_offset, train, CORE)
    specs["HGB+OWN_POSPRIOR_k30"] = (fit_hgb, train, CORE + ["own_rate_pos_k30", "own_games"])
    specs["OFFSET+OWN_POSPRIOR_k30"] = (fit_offset, train, CORE + ["own_rate_pos_k30", "own_games"])
    specs["HGB+OWN_k30_TRAIN2024"] = (fit_hgb, train24, CORE + ["own_rate_k30", "own_games"])
    specs["OFFSET+OWN_k30_TRAIN2024"] = (fit_offset, train24, CORE + ["own_rate_k30", "own_games"])
    models = {name: fn(frame, feats) for name, (fn, frame, feats) in specs.items()}
    joblib.dump(models, OUT / "candidates.joblib")
    hp = {"PRODUCTION": predict(production, hold), **{k: predict(m, hold) for k, m in models.items()}}
    p26 = {"PRODUCTION": predict(production, f26), **{k: predict(m, f26) for k, m in models.items()}}
    hold_rows, levels = holdout_tables(hp, hold)
    market = market_tables(p26, rows, f26)
    gates = gate(hold_rows)
    hold_rows.to_csv(OUT / "holdout_2025_paired_by_tier.csv", index=False)
    levels.to_csv(OUT / "holdout_2025_levels_by_tier.csv", index=False)
    market.to_csv(OUT / "two_sided_2026_paired_by_tier.csv", index=False)
    gates.to_csv(OUT / "tier_gate.csv", index=False)
    examples = {}
    for pid in (8478402, 8480800, 8479318, 8478403):  # McDavid, Q. Hughes, M. Tkachuk, Eichel
        r = f26[f26.player_id == pid].sort_values("game_date").tail(1)
        if len(r):
            i = r.index[0]
            examples[str(pid)] = {"game_date": str(r.game_date.iloc[0]), **{k: round(float(v[i]), 3) for k, v in p26.items()},
                                  "season_ppg": round(float(r.current_season_points_prior.iloc[0] / max(r.current_season_games_prior.iloc[0], 1)), 3),
                                  "prior_ppg": round(float(r.prior_ppg.iloc[0]), 3) if pd.notna(r.prior_ppg.iloc[0]) else None}
    summary = {"training_rows": len(train), "training_rows_2024_only": len(train24), "holdout_rows": len(hold),
               "two_sided_rows": len(rows), "two_sided_dates": int(rows.slate_date.nunique()),
               "rows_with_prior_season": {"train": int((train.prior_games > 0).sum()), "holdout": int((hold.prior_games > 0).sum()),
                                          "2026": int((f26.prior_games > 0).sum())},
               "league_mu_by_season": {int(s): round(float(v), 4) for s, v in hist.groupby("canonical_season").mu_all.first().items()},
               "candidates": {k: v[2] for k, v in specs.items()}, "examples_2026_latest": examples}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    pd.set_option("display.width", 250); pd.set_option("display.max_rows", 1000); pd.set_option("display.max_colwidth", 60)
    print(json.dumps(summary, indent=1, default=str))
    print(gates.to_string(index=False))
    print(levels.pivot_table(index="tier", columns="model", values="mean_expected").round(3).to_string())
    print(levels.groupby("tier")[["n", "mean_actual"]].first().round(3).to_string())
    print(hold_rows.to_string(index=False))
    print(market.to_string(index=False))


if __name__ == "__main__":
    main()
