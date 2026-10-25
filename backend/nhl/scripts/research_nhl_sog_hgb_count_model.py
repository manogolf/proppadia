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
from backend.nhl.sog_hgb_features import (  # noqa: F401  (re-exported for tests)
    BASE_KW, FEATURES, PRIOR_MINUTES, _roll, build, fit, predict)
from backend.nhl.scripts.research_nhl_sog_shrunk_rate import score

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/sog_hgb_count_model/2026-10-25"
TEST_START, TEST_END = "2026-01-01", "2026-05-01"
CONFIGS = {
    "points_kw": BASE_KW,
    "slow_300": {**BASE_KW, "max_iter": 300, "learning_rate": 0.05},
    "slow_300_leaf100": {**BASE_KW, "max_iter": 300, "learning_rate": 0.05, "min_samples_leaf": 100},
    "deep_31": {**BASE_KW, "max_iter": 200, "learning_rate": 0.05, "max_leaf_nodes": 31, "min_samples_leaf": 100},
}


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


def level_bias_check(eligible: pd.DataFrame, league_k: float = 20000) -> pd.DataFrame:
    """Early-season / season-level bias: base model vs candidate fixes on three splits.

    Candidates: league level to date (strict prior, shrunk toward the prior
    season with `league_k` minutes) as an offset on SHRUNK_180 plus features;
    and doubling the weight of the latest training season.
    """
    day = eligible.groupby(["season", "group", "game_date"], as_index=False).agg(
        s=("shots_on_goal", "sum"), t=("toi_minutes", "sum")).sort_values(["season", "group", "game_date"])
    day["cs"] = day.groupby(["season", "group"]).s.cumsum() - day.s
    day["ct"] = day.groupby(["season", "group"]).t.cumsum() - day.t
    e = eligible.merge(day[["season", "group", "game_date", "cs", "ct"]], on=["season", "group", "game_date"], how="left")
    e["league_td"] = (e.cs + e.league_prior * league_k / 60) / (e.ct + league_k) * 60
    e["level_ratio"] = e.league_td / e.league_prior
    e["shrunk_adj"] = e.shrunk_lambda_180 * e.level_ratio
    splits = {
        "replay 2025-10..11-15 (fit season 2024)": (e[e.season.eq(2024)], e[e.season.eq(2025) & (e.game_date < "2025-11-16")]),
        "test 2026-01..04 (fit before 2026)": (e[e.game_date < TEST_START], e[e.season.eq(2025) & (e.game_date >= TEST_START)]),
        "2026 slates (fit seasons 2024-2025)": (e[e.season.isin([2024, 2025])], e[e.season.eq(2026)]),
    }
    rows = []
    for name, (tr, te) in splits.items():
        y = te.shots_on_goal.to_numpy(int)
        cands = {"SHRUNK_180": te.shrunk_lambda_180.to_numpy()}
        cands["HGB base"] = predict(fit(tr, BASE_KW), te)
        sw = np.where(tr.season.eq(tr.season.max()), 2.0, 1.0)
        cands["HGB latest season x2"] = HistGradientBoostingRegressor(**BASE_KW).fit(
            tr[FEATURES].astype(float), tr.shots_on_goal, sample_weight=sw).predict(te[FEATURES].astype(float))
        feats = FEATURES + ["league_td", "level_ratio"]
        off = tr.shrunk_adj.to_numpy()
        cands["HGB league-to-date offset"] = HistGradientBoostingRegressor(**BASE_KW).fit(
            tr[feats].astype(float), tr.shots_on_goal / off, sample_weight=off).predict(
            te[feats].astype(float)) * te.shrunk_adj.to_numpy()
        early = te.games_season.le(9).to_numpy()
        for label, lam in cands.items():
            rows.append({"split": name, "candidate": label, "n": len(te), "mean_pred": float(lam.mean()),
                         "mean_actual": float(y.mean()), "bias_pct": float(lam.mean() / y.mean() - 1) * 100,
                         "early_bias_pct": float(lam[early].mean() / y[early].mean() - 1) * 100 if early.any() else None,
                         "poisson_log_loss": float(-poisson.logpmf(y, np.clip(lam, 0.02, None)).mean())})
    return pd.DataFrame(rows)


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
    bias = level_bias_check(eligible)
    bias.to_csv(OUT / "level_bias_check.csv", index=False)
    (OUT / "summary.json").write_text(json.dumps(result, indent=2, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(table.round(4).to_string(index=False))
    print(json.dumps({k: result[k] for k in ("inner_selection_log_loss", "selected_config", "train_rows", "test_rows",
                                             "paired_hgb_minus_prod", "paired_hgb_minus_shrunk180", "live_2026_rows",
                                             "live_2026_paired_hgb_minus_production", "live_2026_paired_hgb_minus_shrunk180")}, indent=1))
    print(json.dumps(result["permutation_importance_test"][:10], indent=0))
    print(bias.round(4).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
