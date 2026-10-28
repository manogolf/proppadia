#!/usr/bin/env python3
"""Research: let top scorers go above the HGB Points ceiling (2026-10-28).

The production HGB Points model (PP-corrected refit, P2.31) cannot predict
above about 1.5 expected points: trees cannot extrapolate past the training
range (player_points_last10 p99 = 1.6), so 2+ points-per-game players are
capped (McDavid 10-28: 34 points in 12 games, model 1.14). Candidates, fitted
on the same 2023-24 rows and features as production:

- GLM: Poisson regression (log link) on the standardized production features.
  Linear in log space, so it extrapolates.
- OFFSET: the production HGB recipe fitted as a multiplier on the GLM
  (Poisson with offset == fit y/m with sample weight m). Trees bend the
  shape; the level for stars comes from the GLM.

Evaluation: the 2025 holdout (all players and a top-scorer slice), then 2026
two-sided lines since 2026-10-16 (clean attempts inputs) against production and
the market (median same-book no-vig). Inputs are local files only: the saved
2023-24/2025 matrices from the P2.31 refit, the retained Points HGB captures and
an all-candidate export made with ``--price-both-sides``. No database access,
nothing changes in production.
"""
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import PoissonRegressor
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
import sys  # noqa: E402
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

REFIT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/2026-10-24"
OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/star_ceiling_2026-10-28"
FEATURES = scorer.FEATURES
HGB_KW = json.loads(scorer.CONTRACT_PATH.read_text())["hyperparameters"]
LINES = (0.5, 1.5, 2.5)
STAR_PPG = 1.2


def x_matrix(frame: pd.DataFrame) -> np.ndarray:
    """Production preprocessing: numeric, unknown attempts from SOG, zero fill."""
    x = frame[FEATURES].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)
    x["attempts_d10_per60"] = scorer.impute_unknown_attempts(x)
    return x.fillna(0).astype(float).to_numpy()


def fit_candidates(train: pd.DataFrame) -> dict:
    x = x_matrix(train)
    y = train.realized_points.to_numpy(dtype=float)
    scaler = StandardScaler().fit(x)
    xs = scaler.transform(x)
    glm = PoissonRegressor(alpha=1e-4, max_iter=1000).fit(xs, y)
    m = glm.predict(xs)
    offset = HistGradientBoostingRegressor(**HGB_KW).fit(xs, y / m, sample_weight=m)
    return {"scaler": scaler, "glm": glm, "offset": offset}


def predict_all(models: dict, production, frame: pd.DataFrame) -> dict[str, np.ndarray]:
    x = x_matrix(frame)
    xs = models["scaler"].transform(x)
    glm = models["glm"].predict(xs)
    return {
        "production": scorer.score(frame.assign(game_id=0, player_id=range(len(frame))),
                                   production).expected_points.to_numpy(),
        "glm": glm,
        "offset": glm * np.maximum(models["offset"].predict(xs), 1e-8),
    }


def line_metrics(mu: np.ndarray, points: np.ndarray, label: str, slice_name: str) -> list[dict]:
    rows = []
    for line in LINES:
        p = np.clip(poisson.sf(int(line), mu), 1e-12, 1 - 1e-12)
        y = (points > line).astype(float)
        rows.append({"model": label, "slice": slice_name, "line": line, "n": int(len(p)),
                     "log_loss": float(-np.mean(y * np.log(p) + (1 - y) * np.log1p(-p))),
                     "brier": float(np.mean((p - y) ** 2)),
                     "mean_p_over": float(p.mean()), "over_rate": float(y.mean())})
    rows.append({"model": label, "slice": slice_name, "line": "COUNT", "n": int(len(mu)),
                 "mean_expected": float(mu.mean()), "mean_realized": float(points.mean()),
                 "max_expected": float(mu.max())})
    return rows


def holdout(models: dict, production) -> pd.DataFrame:
    hold = pd.read_csv(REFIT / "rebuilt_holdout_matrix_2025.csv.gz")
    hold = hold[hold.realized_points.notna()].reset_index(drop=True)
    points = hold.realized_points.to_numpy(dtype=float)
    preds = predict_all(models, production, hold)
    ppg = hold.current_season_points_prior / hold.current_season_games_prior.replace(0, np.nan)
    star = ((ppg >= STAR_PPG) & (hold.current_season_games_prior >= 10)).to_numpy()
    rows = []
    for label, mu in preds.items():
        rows += line_metrics(mu, points, label, "all")
        rows += line_metrics(mu[star], points[star], label, f"season_ppg>={STAR_PPG}")
    return pd.DataFrame(rows)


def two_sided_rows(export: Path, since: str) -> pd.DataFrame:
    """2026 Points production rows with a same-book two-sided line, plus capture features."""
    p = pd.read_csv(export / "candidate_predictions.csv", low_memory=False)
    q = pd.read_csv(export / "candidate_quotes.csv", low_memory=False)
    p = p[(p.market_key == "player_points") & (p.model_role == "PRODUCTION") & (p.slate_date >= since)].copy()
    p["line"] = pd.to_numeric(p.market_line, errors="coerce")
    p["points"] = pd.to_numeric(p.outcome_value, errors="coerce")
    p = p[p.points.notna()].copy()
    q = q[q.candidate_key.isin(p.candidate_key)].copy()
    q["imp"] = pd.to_numeric(q.implied_probability, errors="coerce")
    q["is_sel"] = q.quote_side == q.selected_side
    wide = q.pivot_table(index=["candidate_key", "sportsbook_key"], columns="is_sel", values="imp",
                         aggfunc="first").dropna()
    wide["nv_sel"] = wide[True] / (wide[True] + wide[False])
    market = wide.groupby("candidate_key").nv_sel.median().rename("market_p_sel").reset_index()
    p = p.merge(market, on="candidate_key", how="inner")
    p["market_p_over"] = np.where(p.selected_side == "OVER", p.market_p_sel, 1 - p.market_p_sel)
    p["run_id"] = p.source_path.str.extract(r"(nhldaily_[^/]+)/")[0]
    p["game_id"] = pd.to_numeric(p.game_id).astype("int64")
    p["player_id"] = pd.to_numeric(p.player_id).astype("int64")
    features = []
    for run in p.run_id.dropna().unique():
        files = sorted(glob.glob(str(ROOT / "artifacts/operational/nhl/points_hgb_shadow/season=2026/"
                                     f"slate_date=*/run_id={run}/phase=*/captured_at=*/features.csv")))
        if files:
            features.append(pd.read_csv(files[-1])[["game_id", "player_id", *FEATURES]].assign(run_id=run))
    feats = pd.concat(features).drop_duplicates(["run_id", "game_id", "player_id"])
    return p.merge(feats, on=["run_id", "game_id", "player_id"], how="inner").reset_index(drop=True)


def paired_bootstrap(loss_a: np.ndarray, loss_b: np.ndarray, dates: np.ndarray,
                     resamples: int = 5000, seed: int = 7) -> dict:
    """Mean loss difference (a - b) with a 95% interval, resampling slate dates."""
    frame = pd.DataFrame({"d": dates, "diff": loss_a - loss_b})
    by_date = frame.groupby("d")["diff"].agg(["sum", "size"])
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(by_date), size=(resamples, len(by_date)))
    sums, sizes = by_date["sum"].to_numpy()[idx].sum(1), by_date["size"].to_numpy()[idx].sum(1)
    draws = sums / sizes
    return {"mean_diff": float(frame["diff"].mean()), "ci95": [float(np.quantile(draws, .025)),
                                                               float(np.quantile(draws, .975))],
            "dates": int(len(by_date)), "n": int(len(frame))}


def log_loss_rows(p_over: np.ndarray, y: np.ndarray) -> np.ndarray:
    p = np.clip(p_over, 1e-12, 1 - 1e-12)
    return -(y * np.log(p) + (1 - y) * np.log1p(-p))


def market_comparison(models: dict, production, rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    preds = predict_all(models, production, rows)
    y = (rows.points > rows.line).astype(float).to_numpy()
    ppg = rows.current_season_points_prior / rows.current_season_games_prior.replace(0, np.nan)
    slices = {"all": np.ones(len(rows), bool),
              f"season_ppg>={STAR_PPG}": (ppg >= STAR_PPG).to_numpy()}
    probs = {label: poisson.sf(rows.line.astype(int).to_numpy(), mu) for label, mu in preds.items()}
    probs["market"] = rows.market_p_over.to_numpy()
    out, cohort = [], []
    for slice_name, mask in slices.items():
        for label, p_over in probs.items():
            p = np.clip(p_over[mask], 1e-12, 1 - 1e-12)
            yy = y[mask]
            pick_over = p >= 0.5
            wins = int((pick_over == (yy == 1)).sum())
            out.append({"slice": slice_name, "model": label, "n": int(mask.sum()),
                        "log_loss": float(-np.mean(yy * np.log(p) + (1 - yy) * np.log1p(-p))),
                        "brier": float(np.mean((p - yy) ** 2)),
                        "W-L": f"{wins}-{int(mask.sum()) - wins}", "win_rate": wins / max(int(mask.sum()), 1)})
    market_over = rows.market_p_over.to_numpy()
    for label in ("production", "glm", "offset"):
        p_over = probs[label]
        sel_over = p_over >= 0.5
        model_sel = np.where(sel_over, p_over, 1 - p_over)
        market_sel = np.where(sel_over, market_over, 1 - market_over)
        dog = market_sel < 0.5
        won = (sel_over == (y == 1))
        cohort.append({"model": label, "market_dog_picks": int(dog.sum()),
                       "W-L": f"{int(won[dog].sum())}-{int((~won[dog]).sum())}",
                       "win_rate": float(won[dog].mean()) if dog.any() else None,
                       "mean_model_p": float(model_sel[dog].mean()) if dog.any() else None,
                       "mean_market_p": float(market_sel[dog].mean()) if dog.any() else None})
    dates = rows.slate_date.astype(str).to_numpy()
    paired = []
    for slice_name, mask in slices.items():
        for a, b in (("offset", "production"), ("glm", "production"), ("offset", "market"), ("production", "market")):
            result = paired_bootstrap(log_loss_rows(probs[a][mask], y[mask]),
                                      log_loss_rows(probs[b][mask], y[mask]), dates[mask])
            paired.append({"slice": slice_name, "comparison": f"{a} - {b}", **result})
    pd.DataFrame(paired).to_csv(OUT / "two_sided_2026_paired_log_loss.csv", index=False)
    print(pd.DataFrame(paired).to_string(index=False))
    return pd.DataFrame(out), pd.DataFrame(cohort)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--export", type=Path, required=True,
                    help="export_nhl_all_candidate_evaluation --price-both-sides output folder")
    ap.add_argument("--since", default="2026-10-16")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    train = pd.read_csv(REFIT / "rebuilt_training_matrix_2023_2024.csv.gz")
    train = train[train.realized_points.notna()].reset_index(drop=True)
    models = fit_candidates(train)
    joblib.dump(models, OUT / "candidates.joblib")
    production = joblib.load(scorer.MODEL_PATH)
    hold = holdout(models, production)
    held = pd.read_csv(REFIT / "rebuilt_holdout_matrix_2025.csv.gz")
    held = held[held.realized_points.notna()].reset_index(drop=True)
    hp = predict_all(models, production, held)
    pts = held.realized_points.to_numpy(dtype=float)
    hd = held.slate_date.astype(str).to_numpy()
    ppg = (held.current_season_points_prior / held.current_season_games_prior.replace(0, np.nan)).to_numpy()
    star = (ppg >= STAR_PPG) & (held.current_season_games_prior.to_numpy() >= 10)
    hold_paired = []
    for slice_name, mask in (("all", np.ones(len(held), bool)), (f"season_ppg>={STAR_PPG}", star)):
        for line in LINES:
            yy = (pts > line).astype(float)
            for a in ("offset", "glm"):
                hold_paired.append({"slice": slice_name, "line": line, "comparison": f"{a} - production",
                                    **paired_bootstrap(log_loss_rows(poisson.sf(int(line), hp[a])[mask], yy[mask]),
                                                       log_loss_rows(poisson.sf(int(line), hp["production"])[mask], yy[mask]),
                                                       hd[mask])})
    pd.DataFrame(hold_paired).to_csv(OUT / "holdout_2025_paired_log_loss.csv", index=False)
    print(pd.DataFrame(hold_paired).to_string(index=False))
    hold.to_csv(OUT / "holdout_2025_metrics.csv", index=False)
    rows = two_sided_rows(args.export, args.since)
    market, cohort = market_comparison(models, production, rows)
    market.to_csv(OUT / "two_sided_2026_metrics.csv", index=False)
    cohort.to_csv(OUT / "two_sided_2026_market_dog_cohort.csv", index=False)
    mcdavid = pd.DataFrame([{"current_season_points_prior": 34, "current_season_games_prior": 12,
                             "player_points_last10": 2.7, "is_home": 1, "d10_sog_per60": 6.693,
                             "attempts_d10_per60": 14.6, "mean_toi_last10": 24.168,
                             "mean_pp_toi_last10": 3.505, "team_d10_sf_per_game": 23.9,
                             "last10_team_sog_share": 0.113}])
    example = {label: float(mu[0]) for label, mu in predict_all(models, production, mcdavid).items()}
    summary = {"training_rows": int(len(train)), "two_sided_rows": int(len(rows)), "since": args.since,
               "export": str(args.export), "hgb_hyperparameters": HGB_KW,
               "mcdavid_2026_10_28_expected_points": example}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    pd.set_option("display.width", 200)
    print(hold.to_string(index=False))
    print(market.to_string(index=False))
    print(cohort.to_string(index=False))
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
