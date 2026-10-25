#!/usr/bin/env python3
"""Research (P2.33): does a fitted SOG count model beat production and SHRUNK_180?

Read-only. Builds strict-prior features for every regular-season skater-game
(TOI > 0, SOG recorded) from `nhl.skater_game_logs_raw`, fits a
HistGradientBoosting Poisson count model (the Points HGB family) and scores
it out of sample.
- Inner selection: fit on season 2024, choose the config on 2025-10..12.
- Test: refit on season 2024 + season 2025 games before 2026-01-01; score
  2026-01-01..2026-04-30 once, paired with PROD and SHRUNK_180 on the same rows.
- 2026 live check: refit on all of seasons 2024-2025; score 2026 slates paired
  with graded production rows, SHRUNK_180 and arms C, D, G.
Seasons are NHL start years (season 2025 = Oct 2025..Apr 2026).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg
from dotenv import dotenv_values
from scipy.stats import poisson
from sklearn.ensemble import HistGradientBoostingRegressor

from backend.nhl.database_access import database_url
from backend.nhl.scripts.research_nhl_sog_shrunk_rate import score

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/2026-10-25"
PRIOR_MINUTES = 180
TEST_START, TEST_END = "2026-01-01", "2026-05-01"
BASE_KW = dict(loss="poisson", max_iter=100, max_leaf_nodes=15, l2_regularization=1.0,
               early_stopping=False, random_state=42)
CONFIGS = {
    "points_kw": BASE_KW,
    "slow_300": {**BASE_KW, "max_iter": 300, "learning_rate": 0.05},
    "slow_300_leaf100": {**BASE_KW, "max_iter": 300, "learning_rate": 0.05, "min_samples_leaf": 100},
    "deep_31": {**BASE_KW, "max_iter": 200, "learning_rate": 0.05, "max_leaf_nodes": 31, "min_samples_leaf": 100},
}
FEATURES = [
    "is_home", "is_defense", "sog60_5", "sog60_10", "sog60_20", "sog60_season", "sog60_prior_season",
    "shrunk_rate_180", "shrunk_lambda_180", "toi_avg_5", "toi_avg_10", "toi_season", "pp_toi_avg_10",
    "att60_10", "att60_20", "games_season", "rest_days", "team_sf_10", "opp_sa_10", "league_prior",
]


def pull() -> tuple[pd.DataFrame, pd.DataFrame]:
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT l.player_id, l.game_id, g.game_date, g.season, substring(g.game_id::text,5,2) AS game_type,
                              l.is_home, g.home_team_id, g.away_team_id,
                              l.shots_on_goal, l.toi_minutes, l.pp_toi_minutes, l.shot_attempts
                       FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING (game_id)
                       WHERE g.season BETWEEN 2023 AND 2026 AND g.game_date < CURRENT_DATE""")
        logs = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        cur.execute("SELECT player_id, position FROM nhl.players")
        pos = pd.DataFrame(cur.fetchall(), columns=["player_id", "position"])
    for col in ("shots_on_goal", "toi_minutes", "pp_toi_minutes", "shot_attempts"):
        logs[col] = pd.to_numeric(logs[col], errors="coerce").astype(float)
    logs["game_date"] = pd.to_datetime(logs.game_date)
    pos["group"] = np.where(pos.position.astype(str).str.upper().str.startswith("D"), "D", "F")
    return logs, pos[["player_id", "group"]].drop_duplicates("player_id")


def _roll(frame: pd.DataFrame, keys, value: str, n: int | None, how: str) -> pd.Series:
    """Strict-prior rolling (or expanding, n=None) aggregate within key groups."""
    def f(s):
        s = s.shift(1)
        r = s.expanding(min_periods=1) if n is None else s.rolling(n, min_periods=1)
        return getattr(r, how)()
    return frame.groupby(keys, sort=False)[value].transform(f)


def build(logs: pd.DataFrame, pos: pd.DataFrame) -> pd.DataFrame:
    """One row per clean regular-season skater-game with strict-prior features."""
    clean = logs[(logs.game_type == "02") & (logs.toi_minutes > 0) & logs.shots_on_goal.notna()].copy()
    clean = clean.merge(pos, on="player_id", how="left")
    clean["group"] = clean.group.fillna("F")
    clean = clean.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    clean["is_home"] = clean.is_home.astype(bool).astype(int)
    clean["is_defense"] = clean.group.eq("D").astype(int)
    clean["team_id_bound"] = np.where(clean.is_home == 1, clean.home_team_id, clean.away_team_id)
    clean["opp_id_bound"] = np.where(clean.is_home == 1, clean.away_team_id, clean.home_team_id)
    p = "player_id"
    sums = {}
    for n in (5, 10, 20):
        sums[f"sog{n}"] = _roll(clean, p, "shots_on_goal", n, "sum")
        sums[f"toi{n}"] = _roll(clean, p, "toi_minutes", n, "sum")
    for n in (5, 10, 20):
        clean[f"sog60_{n}"] = (sums[f"sog{n}"] / sums[f"toi{n}"] * 60).where(sums[f"toi{n}"] > 0)
    clean["toi_avg_5"] = _roll(clean, p, "toi_minutes", 5, "mean")
    clean["toi_avg_10"] = _roll(clean, p, "toi_minutes", 10, "mean")
    clean["pp_toi_avg_10"] = _roll(clean, p, "pp_toi_minutes", 10, "mean")
    clean["toi_att"] = clean.toi_minutes.where(clean.shot_attempts.notna())
    for n in (10, 20):
        a, t = _roll(clean, p, "shot_attempts", n, "sum"), _roll(clean, p, "toi_att", n, "sum")
        clean[f"att60_{n}"] = (a / t * 60).where(t > 0)
    ps = [p, "season"]
    s_sog, s_toi = _roll(clean, ps, "shots_on_goal", None, "sum"), _roll(clean, ps, "toi_minutes", None, "sum")
    clean["sog60_season"] = (s_sog / s_toi * 60).where(s_toi > 0)
    clean["toi_season"] = _roll(clean, ps, "toi_minutes", None, "mean")
    clean["games_season"] = clean.groupby(ps).cumcount()
    season_tot = clean.groupby(ps).agg(s=("shots_on_goal", "sum"), t=("toi_minutes", "sum")).reset_index()
    season_tot["sog60_prior_season"] = season_tot.s / season_tot.t * 60
    season_tot["season"] += 1
    clean = clean.merge(season_tot[[p, "season", "sog60_prior_season"]], on=ps, how="left")
    clean["rest_days"] = clean.groupby(p).game_date.diff().dt.days.clip(upper=10)
    league = (clean.groupby(["season", "group"]).agg(s=("shots_on_goal", "sum"), t=("toi_minutes", "sum"))
              .assign(r=lambda d: d.s / d.t * 60).r.rename("league_prior").reset_index())
    league["season"] += 1
    clean = clean.merge(league, on=["season", "group"], how="left")
    # window sums recomputed after the merges above
    sog20 = _roll(clean, p, "shots_on_goal", 20, "sum").fillna(0)
    toi20 = _roll(clean, p, "toi_minutes", 20, "sum").fillna(0)
    clean["shrunk_rate_180"] = (sog20 + clean.league_prior * PRIOR_MINUTES / 60) / (toi20 + PRIOR_MINUTES) * 60
    clean["shrunk_lambda_180"] = clean.shrunk_rate_180 * clean.toi_avg_10 / 60
    # team shots for / opponent shots against, strict prior per team over its last 10 games
    team = clean.groupby(["team_id_bound", "game_id", "game_date"], as_index=False).shots_on_goal.sum()
    team = team.rename(columns={"team_id_bound": "team", "shots_on_goal": "sf"})
    opp = clean.groupby(["opp_id_bound", "game_id"], as_index=False).shots_on_goal.sum()
    opp = opp.rename(columns={"opp_id_bound": "team", "shots_on_goal": "sa"})
    team = team.merge(opp, on=["team", "game_id"], how="left").sort_values(["team", "game_date", "game_id"])
    team["team_sf_10"] = _roll(team, "team", "sf", 10, "mean")
    team["opp_sa_10"] = _roll(team, "team", "sa", 10, "mean")
    clean = clean.merge(team[["team", "game_id", "team_sf_10"]].rename(columns={"team": "team_id_bound"}),
                        on=["team_id_bound", "game_id"], how="left")
    clean = clean.merge(team[["team", "game_id", "opp_sa_10"]].rename(columns={"team": "opp_id_bound"}),
                        on=["opp_id_bound", "game_id"], how="left")
    # production formula (as in the shrunk-rate research): rate from last 10 logged games of any type
    allrows = logs.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    allrows["sog0"] = allrows.shots_on_goal.fillna(0)
    allrows["toi_pos"] = allrows.toi_minutes.where(allrows.toi_minutes > 0)
    for n in (5, 10, 20):
        allrows[f"p_sog{n}"] = _roll(allrows, p, "sog0", n, "sum")
        allrows[f"p_toi{n}"] = _roll(allrows, p, "toi_pos", n, "sum")
    toi_rows = allrows[allrows.toi_pos.notna()].copy()
    for n in (5, 10, 20):
        toi_rows[f"p_toiavg{n}"] = _roll(toi_rows, p, "toi_pos", n, "mean")
    allrows = allrows.merge(toi_rows[[p, "game_id", "p_toiavg5", "p_toiavg10", "p_toiavg20"]], on=[p, "game_id"], how="left")
    rate = lambda s, t: (s / t * 60).where(t > 0)
    allrows["PROD"] = (rate(allrows.p_sog10, allrows.p_toi10).fillna(rate(allrows.p_sog20, allrows.p_toi20))
                       .fillna(rate(allrows.p_sog5, allrows.p_toi5))
                       * allrows.p_toiavg10.fillna(allrows.p_toiavg20).fillna(allrows.p_toiavg5) / 60)
    clean = clean.merge(allrows[[p, "game_id", "PROD"]], on=[p, "game_id"], how="left")
    return clean


def fit(frame: pd.DataFrame, kw: dict) -> HistGradientBoostingRegressor:
    x = frame[FEATURES].astype(float).to_numpy()
    y = frame.shots_on_goal.to_numpy(int)
    model = HistGradientBoostingRegressor(**kw).fit(x, y)
    repeat = HistGradientBoostingRegressor(**kw).fit(x.copy(), y.copy())
    if not np.array_equal(model.predict(x), repeat.predict(x)):
        raise ValueError("SOG_HGB_REFIT_REPRODUCIBILITY_FAIL")
    return model


def predict(model, frame: pd.DataFrame) -> np.ndarray:
    return model.predict(frame[FEATURES].astype(float).to_numpy())


def paired_ci(frame: pd.DataFrame, a: str, b: str, seed: int = 20261025) -> dict:
    """Date-clustered bootstrap of mean Poisson log loss difference a - b."""
    y = frame.shots_on_goal.to_numpy(int)
    d = -poisson.logpmf(y, frame[a].clip(0.02)) + poisson.logpmf(y, frame[b].clip(0.02))
    by = pd.DataFrame({"date": frame.game_date.to_numpy(), "d": d}).groupby("date").d.agg(["sum", "size"])
    rng = np.random.default_rng(seed)
    idx = np.arange(len(by))
    s, n = by["sum"].to_numpy(), by["size"].to_numpy()
    boots = [s[pick].sum() / n[pick].sum() for pick in (rng.choice(idx, len(idx)) for _ in range(5000))]
    return {"mean": float(d.mean()), "ci95": [float(np.percentile(boots, 2.5)), float(np.percentile(boots, 97.5))]}


def calibration_by_decile(frame: pd.DataFrame, col: str) -> list[dict]:
    q = pd.qcut(frame[col], 10, labels=False, duplicates="drop")
    g = frame.groupby(q).agg(pred=(col, "mean"), actual=("shots_on_goal", "mean"), n=(col, "size"))
    return [{"decile": int(k), **{c: float(v) if c != "n" else int(v) for c, v in r.items()}} for k, r in g.iterrows()]


def permutation_importance(model, frame: pd.DataFrame, seed: int = 7) -> list[dict]:
    rng = np.random.default_rng(seed)
    y = frame.shots_on_goal.to_numpy(int)
    base = float(-poisson.logpmf(y, np.clip(predict(model, frame), 0.02, None)).mean())
    out = []
    for col in FEATURES:
        shuffled = frame.copy()
        shuffled[col] = rng.permutation(shuffled[col].to_numpy())
        ll = float(-poisson.logpmf(y, np.clip(predict(model, shuffled), 0.02, None)).mean())
        out.append({"feature": col, "log_loss_increase": ll - base})
    return sorted(out, key=lambda r: -r["log_loss_increase"])


def lambda_from_p_over_1_5(p: float) -> float:
    """Invert P(X >= 2) = 1 - e^-lam (1 + lam) for the Poisson mean."""
    from scipy.optimize import brentq
    if p <= 1e-12:
        return 0.0
    return float(brentq(lambda lam: 1 - np.exp(-lam) * (1 + lam) - p, 1e-9, 50.0, xtol=1e-12))


def live_2026(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """Pair 2026 slates: governed production rows, arms C, D, G, SHRUNK_180 and HGB."""
    from backend.nhl.reconciliation_grades import restated_grade_path
    from backend.nhl.scripts import freeze_nhl_sog_points_wl_baseline as baseline
    prod_parts, arm_parts, official_parts = [], [], []
    for pkg in sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation").glob("2026-1*/reconciliation=*")):
        day = pkg.parent.name
        if day < "2026-10-08":
            continue
        path = restated_grade_path(pkg, "graded_resilient_sog.csv")
        if path.is_file():
            g = pd.read_csv(path, low_memory=False)
            g = g[(g.line == 1.5) & g.official_sog.notna()]
            arm_parts.append(g[["slate_date", "game_id", "player_id", "contract_arm", "expected_sog", "official_sog"]])
        prod, _ = baseline.sog_rows(day)
        _, summary = baseline._latest_summary(day)
        outcome_path = baseline._package(day, summary["reconciliation_package"]) / "canonical_skater_outcomes.csv"
        outcomes = pd.read_csv(outcome_path)
        outcomes = outcomes[outcomes.slate_date.astype(str).eq(day) & outcomes.official_sog.notna()]
        if "participation_state" in outcomes:
            outcomes = outcomes[outcomes.participation_state.eq("PARTICIPATED")]
        official_parts.append(outcomes[["slate_date", "game_id", "player_id", "official_sog"]])
        if len(prod):
            prod = prod[(prod.line == 1.5) & prod.grading_state.isin(["WIN", "LOSS"])]
            prod_parts.append(prod[["slate_date", "game_id", "player_id", "p_over"]])
    prod = pd.concat(prod_parts).drop_duplicates(["slate_date", "game_id", "player_id"], keep="last")
    prod["PRODUCTION"] = prod.p_over.astype(float).map(lambda_from_p_over_1_5)
    arms = pd.concat(arm_parts).drop_duplicates(["slate_date", "game_id", "player_id", "contract_arm"], keep="last")
    wide = arms.pivot_table(index=["slate_date", "game_id", "player_id"], columns="contract_arm",
                            values="expected_sog").reset_index()
    r = rows.assign(slate_date=rows.game_date.dt.strftime("%Y-%m-%d"))
    keys = ["slate_date", "game_id", "player_id"]
    official = pd.concat(official_parts).drop_duplicates(keys, keep="last")
    official["game_id"] = official.game_id.astype("int64"); official["player_id"] = official.player_id.astype("int64")
    # Grade against official outcomes (the governed source); log SOG is only a model input.
    base = (prod[keys + ["PRODUCTION"]]
            .merge(r[keys + ["game_date", "shrunk_lambda_180", "HGB"]], on=keys, how="inner")
            .merge(official.rename(columns={"official_sog": "shots_on_goal"}), on=keys, how="inner")
            .dropna(subset=["PRODUCTION", "shrunk_lambda_180", "HGB", "shots_on_goal"]))
    arm_cols = ["C_MULTISEASON_SHRUNK_PLAYER", "D_PLAYER_ROLE_HIERARCHICAL", "G_COLD_START_TO_CURRENT_SEASON_BLEND"]
    with_arms = base.merge(wide, on=keys, how="inner").dropna(subset=arm_cols)
    return base, with_arms, arm_cols


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    logs, pos = pull()
    frame = build(logs, pos)
    eligible = frame[frame.season.ge(2024) & frame.toi_avg_10.notna() & frame.shrunk_lambda_180.notna() & frame.PROD.notna()].copy()
    # inner selection: season 2024 -> 2025-10..12
    inner_train = eligible[eligible.season.eq(2024)]
    inner_valid = eligible[eligible.season.eq(2025) & (eligible.game_date < TEST_START)]
    yv = inner_valid.shots_on_goal.to_numpy(int)
    inner = {name: score(yv, np.asarray(predict(fit(inner_train, kw), inner_valid)))["poisson_log_loss"] for name, kw in CONFIGS.items()}
    best = min(inner, key=inner.get)
    # test: fit on everything before 2026-01-01, score Jan..Apr 2026 once
    train = eligible[eligible.game_date < TEST_START]
    test = eligible[(eligible.game_date >= TEST_START) & (eligible.game_date < TEST_END) & eligible.season.eq(2025)].copy()
    model = fit(train, CONFIGS[best])
    test["HGB"] = predict(model, test)
    y = test.shots_on_goal.to_numpy(int)
    rows = [{"recipe": c, "window": "test 2026-01..04", **score(y, test[col].to_numpy())}
            for c, col in (("PROD", "PROD"), ("SHRUNK_180", "shrunk_lambda_180"), ("HGB", "HGB"))]
    result = {
        "inner_selection_log_loss": inner, "selected_config": best, "selected_kw": CONFIGS[best],
        "features": FEATURES, "train_rows": int(len(train)), "test_rows": int(len(test)),
        "train_window": [str(train.game_date.min().date()), str(train.game_date.max().date())],
        "test_window": [str(test.game_date.min().date()), str(test.game_date.max().date())],
        "paired_hgb_minus_prod": paired_ci(test, "HGB", "PROD"),
        "paired_hgb_minus_shrunk180": paired_ci(test, "HGB", "shrunk_lambda_180"),
        "calibration_deciles_hgb": calibration_by_decile(test, "HGB"),
        "permutation_importance_test": permutation_importance(model, test),
    }
    # 2026 live check with a model fitted on all of seasons 2024-2025
    full = fit(eligible[eligible.season.isin([2024, 2025])], CONFIGS[best])
    cur = frame[frame.season.eq(2026)].copy()
    cur = cur[cur.toi_avg_10.notna() & cur.shrunk_lambda_180.notna()]
    cur["HGB"] = predict(full, cur)
    paired, with_arms, arm_cols = live_2026(cur)
    main_cols = ["PRODUCTION", "shrunk_lambda_180", "HGB"]
    y26 = paired.shots_on_goal.to_numpy(int)
    window = f"2026 {paired.slate_date.min()}..{paired.slate_date.max()} paired"
    rows += [{"recipe": c, "window": window, **score(y26, paired[c].to_numpy())} for c in main_cols]
    ya = with_arms.shots_on_goal.to_numpy(int)
    window = f"2026 {with_arms.slate_date.min()}..{with_arms.slate_date.max()} paired with arms"
    rows += [{"recipe": c, "window": window, **score(ya, with_arms[c].to_numpy())} for c in main_cols + arm_cols]
    result["live_2026_rows"] = int(len(paired))
    result["live_2026_rows_with_arms"] = int(len(with_arms))
    result["live_2026_paired_hgb_minus_production"] = paired_ci(paired, "HGB", "PRODUCTION")
    result["live_2026_paired_hgb_minus_shrunk180"] = paired_ci(paired, "HGB", "shrunk_lambda_180")
    table = pd.DataFrame(rows)
    table.to_csv(OUT / "comparison.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps(result, indent=2, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(table.round(4).to_string(index=False))
    print(json.dumps({k: result[k] for k in ("inner_selection_log_loss", "selected_config", "train_rows", "test_rows",
                                             "paired_hgb_minus_prod", "paired_hgb_minus_shrunk180", "live_2026_rows",
                                             "live_2026_paired_hgb_minus_production", "live_2026_paired_hgb_minus_shrunk180")}, indent=1))
    print(json.dumps(result["permutation_importance_test"][:10], indent=0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
