#!/usr/bin/env python3
"""Research: Points inputs for role changes (NHL Backlog P2.46, 2026-11-06).

The current Points model (goals + assists, P2.39) reads ``own_rate_k30``,
``goal_rate`` and ``assist_rate``: last season's and this season's totals per
game played, so a player whose ice time changed keeps last season's rate. The
SOG model reads per-minute rates and recent TOI and follows role changes
(P2.44). Tests, each as a goals + assists pair (production recipe) and scored
against the current production artifact:

1. PER_MINUTE: the same shrunk rates per minute of ice time, times the
   player's TOI over his last 10 games (``*_rate_toi``); added beside the
   per-game rates (ADD) or in place of them (REPLACE). Shrinkage: 30 games of
   league-average minutes at last season's league rate per minute. 2022-23 has
   no stored TOI, so 2023-24 rows estimate last season's minutes as prior
   games x this season's TOI (their per-minute rate equals the per-game one).
2. MATES: previous-game top-two linemates' own rate and the top linemate's
   share of ice time (P2.39 inputs), plus NEW_MATES (neither top linemate was
   among his three most frequent top linemates last season).
3. LINE: games in a row with the same top-two linemates (capped at 10).

Scored on the 2025 holdout (all rows, tiers, and role-change rows: last-10
TOI 2.5+ minutes or PP TOI 1.0+ minutes from last season's per-game average,
PP scaled by the league's season change, 5+ games this season and 20+ last)
and on the cached 2026 two-sided lines against the market's no-vig price.
Paired log loss differences x1000 (negative = better than production).
Read-only (research database login); nothing changes production.
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
from backend.nhl.scripts import research_nhl_points_goal_assist as ga  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_linemates as lm  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/role_change_tests_2026-11-06"
KEYS = ["game_id", "player_id"]
K = 30
LINES = (0.5, 1.5, 2.5)
ROLE_TOI, ROLE_PP, ROLE_GAMES, ROLE_PRIOR_GAMES = 2.5, 1.0, 5, 20
PROD_RATES = ["own_rate_k30", "goal_rate", "assist_rate"]


def usage_tables(logs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-season usage by player, and strict-prior minutes this season per player-game."""
    x = logs[["canonical_season", "game_date", "game_id", "player_id", "toi_minutes", "pp_toi_minutes"]].copy()
    x[["toi", "pp"]] = x[["toi_minutes", "pp_toi_minutes"]].apply(pd.to_numeric, errors="coerce")
    season = x.groupby(["player_id", "canonical_season"]).agg(
        s_games=("game_id", "size"), s_min=("toi", "sum"), s_toi_pg=("toi", "mean"), s_pp_pg=("pp", "mean")).reset_index()
    x = x.sort_values(["player_id", "game_date", "game_id"])
    x["cur_min"] = x.groupby(["player_id", "canonical_season"], sort=False).toi.transform(
        lambda v: v.shift(1).fillna(0).cumsum())
    return season, x[KEYS + ["cur_min"]]


def add_role_inputs(frame: pd.DataFrame, season: pd.DataFrame, cur: pd.DataFrame, totals: pd.DataFrame) -> pd.DataFrame:
    f = frame.merge(cur, on=KEYS, how="left")
    prev = season.assign(canonical_season=season.canonical_season + 1).rename(
        columns={"s_games": "p_games_toi", "s_min": "p_min", "s_toi_pg": "p_toi_pg", "s_pp_pg": "p_pp_pg"})
    f = f.merge(prev, on=["player_id", "canonical_season"], how="left")
    cur_toi = (f.cur_min / f.current_season_games_prior.replace(0, np.nan)).fillna(f.mean_toi_last10)
    # No stored TOI for the prior season (2022-23): assume this season's minutes per game.
    f["p_min_est"] = f.p_min.fillna(f.pg * cur_toi)
    f["cur_min"] = f.cur_min.fillna(f.current_season_games_prior * f.mean_toi_last10)
    league_min = season.groupby("canonical_season").apply(lambda t: t.s_min.sum() / t.s_games.sum(), include_groups=False)
    lg_min = f.canonical_season.map(lambda s: league_min.get(s - 1, league_min.iloc[0]))
    # League points/goals/assists per minute last season = per game / league minutes per game.
    rates = {"own_rate_toi": ("pp_points", "cur_points", "mu_all"), "goal_rate_toi": ("pgo", "cgo", "lg_go"),
             "assist_rate_toi": ("pa", "ca", "lg_a")}
    f["pp_points"] = f.prior_points
    f["cur_points"] = f.current_season_points_prior
    for name, (prior, current, mu) in rates.items():
        per_min_mu = f[mu] / lg_min
        per_min = (f[prior] + f[current] + K * lg_min * per_min_mu) / (f.p_min_est + f.cur_min + K * lg_min)
        f[name] = per_min * f.mean_toi_last10
    # Role change, as in the input watch (PP scaled by the league's season change).
    pp_scale = f.groupby("canonical_season").apply(
        lambda t: t.mean_pp_toi_last10.mean() / t.p_pp_pg.mean() if t.p_pp_pg.mean() > 0 else 1.0, include_groups=False)
    f["role_change"] = ((f.current_season_games_prior >= ROLE_GAMES) & (f.p_games_toi >= ROLE_PRIOR_GAMES)
                        & (((f.mean_toi_last10 - f.p_toi_pg).abs() >= ROLE_TOI)
                           | ((f.mean_pp_toi_last10 - f.p_pp_pg * f.canonical_season.map(pp_scale)).abs() >= ROLE_PP)))
    return f


def line_inputs(mates: pd.DataFrame, dates: pd.DataFrame) -> pd.DataFrame:
    """Previous game's top-two mates, games in a row with them, and whether they are new vs last season."""
    m = mates.merge(dates, on="game_id", how="left").sort_values(["player_id", "game_date", "game_id"])
    m["pair"] = [tuple(sorted((a, b))) for a, b in zip(m.mate1, m.mate2)]
    by = m.groupby(["player_id", "canonical_season"], sort=False)
    same = (m.pair == by.pair.shift(1)).astype(int)
    run = same.groupby((same == 0).cumsum()).cumsum() + 1
    m["run"] = run.where(by.cumcount() > 0, 1)
    common = (m.groupby(["player_id", "canonical_season"]).mate1.agg(lambda v: set(v.value_counts().head(3).index))
              .rename("last_common").reset_index().assign(canonical_season=lambda t: t.canonical_season + 1))
    m = m.merge(common, on=["player_id", "canonical_season"], how="left")
    for c in ("mate1", "mate2", "mate1_share", "run"):
        m[f"prev_{c}"] = by[c].shift(1).values if c != "run" else m.groupby(["player_id", "canonical_season"]).run.shift(1)
    known = [isinstance(s, set) for s in m.last_common]
    m["new_mates"] = [float(not ({a, b} & s)) if k and pd.notna(a) else np.nan
                      for a, b, s, k in zip(m.prev_mate1, m.prev_mate2, m.last_common, known)]
    m["line_run"] = m.prev_run.clip(upper=10)
    return m[KEYS + ["prev_mate1", "prev_mate2", "prev_mate1_share", "new_mates", "line_run"]]


def attach_mates(frame: pd.DataFrame, lines: pd.DataFrame, rate: pd.DataFrame) -> pd.DataFrame:
    f = frame.merge(lines, on=KEYS, how="left")
    for k in ("1", "2"):
        f = f.merge(rate.rename(columns={"player_id": f"prev_mate{k}", "own_rate_k30": f"mate{k}_rate"}),
                    on=["game_id", f"prev_mate{k}"], how="left")
    f["mate_rate_top2"] = f[["mate1_rate", "mate2_rate"]].mean(axis=1)
    f["mate_share"] = f.prev_mate1_share
    return f


def fit_pair(train: pd.DataFrame, features: list[str]) -> dict:
    return {"goals": ga.fit_count(train, features, "goals"), "assists": ga.fit_count(train, features, "assists")}


def predict_pair(model: dict, frame: pd.DataFrame) -> np.ndarray:
    return ga.predict_count(model["goals"], frame) + ga.predict_count(model["assists"], frame)


def paired_rows(preds: dict, frame: pd.DataFrame, masks: dict, points: np.ndarray) -> pd.DataFrame:
    dates = frame.slate_date.astype(str).to_numpy()
    out = []
    for group, mask in masks.items():
        if mask.sum() < 30:
            continue
        for line in LINES:
            y = (points > line).astype(float)
            prod = base.log_loss_rows(poisson.sf(int(line), preds["PRODUCTION"]), y)
            for label, mu in preds.items():
                if label == "PRODUCTION":
                    continue
                r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), mu), y)[mask], prod[mask], dates[mask])
                out.append({"group": group, "line": line, "model": label, "n": r["n"],
                            "diff_x1000": round(r["mean_diff"] * 1e3, 2), "ci_x1000": [round(c * 1e3, 2) for c in r["ci95"]]})
    return pd.DataFrame(out)


def market_rows(preds: dict, rows: pd.DataFrame, f26: pd.DataFrame, masks: dict) -> pd.DataFrame:
    idx = rows[KEYS].merge(f26[KEYS].reset_index(), on=KEYS, how="left")["index"].to_numpy()
    y = (rows.points > rows.line).astype(float).to_numpy()
    thr = rows.line.astype(int).to_numpy()
    probs = {k: poisson.sf(thr, mu[idx]) for k, mu in preds.items()}
    probs["MARKET"] = rows.market_p_over.to_numpy()
    dates = rows.slate_date.astype(str).to_numpy()
    out = []
    for group, mask in masks.items():
        m = mask[idx]
        if m.sum() < 30:
            continue
        prod = base.log_loss_rows(probs["PRODUCTION"][m], y[m])
        for label, p in probs.items():
            if label == "PRODUCTION":
                continue
            r = base.paired_bootstrap(base.log_loss_rows(p[m], y[m]), prod, dates[m])
            out.append({"group": group, "model": label, "n": r["n"], "diff_vs_production_x1000": round(r["mean_diff"] * 1e3, 2),
                        "ci_x1000": [round(c * 1e3, 2) for c in r["ci95"]]})
    return pd.DataFrame(out)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    cache = OUT / "inputs.joblib"
    if not cache.exists():
        logs, audit = refit.pull_history()
        joblib.dump({"logs": logs[["canonical_season", "game_date", "game_id", "player_id", "toi_minutes", "pp_toi_minutes"]],
                     "login": audit["login"], "snapshot_utc": audit["transaction_timestamp_utc"]}, cache)
    inputs = joblib.load(cache)
    season, cur = usage_tables(inputs["logs"])
    data = joblib.load(base.OUT / "matrices.joblib")
    api = pd.read_csv(own.SEASON_2022)
    totals = pd.concat([data["season_totals"], pd.DataFrame({"player_id": api.player_id, "canonical_season": 2022,
                                                              "season_points": api.points, "season_games": api.games})],
                       ignore_index=True)
    frozen = pd.read_csv(refit.FROZEN_FRAME, low_memory=False)
    frozen["game_id"] = frozen.game_id.astype("int64"); frozen["player_id"] = frozen.player_id.astype("int64")
    outcomes, _ = refit.official_outcomes(frozen)
    outcomes["game_id"] = outcomes.game_id.astype("int64"); outcomes["player_id"] = outcomes.player_id.astype("int64")

    def build(frame):
        f = ga.component_rates(own.add_own_rate(frame, totals), outcomes, api)
        # League per-game goal/assist rates last season, as component_rates uses (for the per-minute prior).
        f["lg_go"] = (f.goal_rate * (f.pg + f.cg + K) - f.pgo - f.cgo) / K
        f["lg_a"] = (f.assist_rate * (f.pg + f.cg + K) - f.pa - f.ca) / K
        return add_role_inputs(f, season, cur, totals)

    hist, f26 = build(data["historical"]), build(data["2026"]).reset_index(drop=True)
    lmi = joblib.load(lm.OUT / "inputs.joblib")
    rate = pd.concat([hist[KEYS + ["own_rate_k30"]], f26[KEYS + ["own_rate_k30"]]]).drop_duplicates(KEYS)
    lines = line_inputs(lmi["mates"], lmi["dates"])
    hist, f26 = attach_mates(hist, lines, rate), attach_mates(f26, lines, rate).reset_index(drop=True)
    rows = data["market_2026"].merge(f26[KEYS], on=KEYS, how="inner").reset_index(drop=True)
    train = hist[hist.canonical_season.isin([2023, 2024]) & hist.goals.notna()].reset_index(drop=True)
    hold = hist[hist.canonical_season.eq(2025)].reset_index(drop=True)

    core = list(own.CORE)
    prod_feats = core + ["own_rate_k30", "own_games", "goal_rate", "assist_rate"]
    toi_rates = ["own_rate_toi", "goal_rate_toi", "assist_rate_toi"]
    specs = {
        "REFIT_PRODUCTION_FEATURES": prod_feats,
        "PER_MINUTE_ADD": prod_feats + toi_rates,
        "PER_MINUTE_REPLACE": core + ["own_games"] + toi_rates,
        "MATES": prod_feats + ["mate_rate_top2", "mate_share", "new_mates"],
        "LINE": prod_feats + ["line_run"],
        "PER_MINUTE_ADD+MATES+LINE": prod_feats + toi_rates + ["mate_rate_top2", "mate_share", "new_mates", "line_run"],
    }
    models = {name: fit_pair(train, feats) for name, feats in specs.items()}
    joblib.dump(models, OUT / "candidates.joblib")
    production = joblib.load(own.scorer.MODEL_PATH)
    hp = {"PRODUCTION": own.predict(production, hold), **{k: predict_pair(m, hold) for k, m in models.items()}}
    p26 = {"PRODUCTION": own.predict(production, f26), **{k: predict_pair(m, f26) for k, m in models.items()}}

    def masks(frame):
        t = own.tiers(frame)
        return {"all": t["all"], "role_change": frame.role_change.to_numpy(bool),
                "role_up_toi": (frame.role_change & (frame.mean_toi_last10 > frame.p_toi_pg)).to_numpy(bool),
                "role_down_toi": (frame.role_change & (frame.mean_toi_last10 < frame.p_toi_pg)).to_numpy(bool),
                "no_role_change": ~frame.role_change.to_numpy(bool),
                **{k: v for k, v in t.items() if k != "all"}}

    hold_tab = paired_rows(hp, hold, masks(hold), hold.realized_points.to_numpy(float))
    market_tab = market_rows(p26, rows, f26, {k: v for k, v in masks(f26).items()
                                               if k in ("all", "role_change", "role_up_toi", "no_role_change")})
    levels = pd.DataFrame([{"group": g, "model": k, "n": int(m.sum()), "mean_expected": float(v[m].mean()),
                            "mean_actual": float(hold.realized_points.to_numpy()[m].mean())}
                           for g, m in masks(hold).items() if m.sum() for k, v in hp.items()])
    hold_tab.to_csv(OUT / "holdout_2025_paired.csv", index=False)
    market_tab.to_csv(OUT / "two_sided_2026_vs_production.csv", index=False)
    levels.to_csv(OUT / "holdout_2025_levels.csv", index=False)
    summary = {
        "training_rows": len(train), "holdout_rows": len(hold), "two_sided_rows": len(rows),
        "two_sided_dates": [str(rows.slate_date.min()), str(rows.slate_date.max())],
        "role_change_rows": {"holdout": int(hold.role_change.sum()), "2026": int(f26.role_change.sum()),
                             "two_sided": int(f26.role_change.to_numpy()[rows[KEYS].merge(f26[KEYS].reset_index(), on=KEYS)["index"]].sum())},
        "coverage": {c: {"holdout": float(hold[c].notna().mean()), "2026": float(f26[c].notna().mean())}
                     for c in ("own_rate_toi", "mate_rate_top2", "new_mates", "line_run")},
        "prior_minutes_estimated_share": {"train": float(train.p_min.isna().mean()), "holdout": float(hold.p_min.isna().mean())},
        "candidates": specs, "database_login": inputs["login"], "snapshot_utc": inputs["snapshot_utc"],
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    pd.set_option("display.width", 250); pd.set_option("display.max_rows", 1000); pd.set_option("display.max_colwidth", 40)
    print(json.dumps(summary, indent=1, default=str))
    key = hold_tab[hold_tab.group.isin(["all", "role_change", "role_up_toi", "role_down_toi", "no_role_change"])]
    print(key.pivot_table(index=["group", "line"], columns="model", values="diff_x1000").to_string())
    print(market_tab.pivot_table(index="group", columns="model", values="diff_vs_production_x1000").to_string())


if __name__ == "__main__":
    main()
