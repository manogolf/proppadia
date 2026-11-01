#!/usr/bin/env python3
"""Research: month-end Points refit candidates (P2.39 items 1-4, 2026-11-01).

One refit carries OFFSET (the 10-28 star-ceiling fix: production HGB recipe
fitted as a multiplier on a Poisson GLM) plus three input changes, each added
on its own so it is kept only if it helps:

- ASSISTS (item 2): assists per game over the last 10 games and the season,
  and involvement (points / team goals over the last 10 games played).
- DEFENSE (item 3): ``is_defense`` from ``nhl.players`` (as the SOG HGB).
- SHARE (item 4): ``last10_team_sog_share`` over the games the player played,
  instead of the team's last 10 games.

Reuses the P2.31 refit pull and labels (``run_nhl_points_hgb_pp_corrected_refit``),
the live feature code (``export_nhl_points_hgb_features.build_features``) and the
10-28 OFFSET recipe. Read-only database access through the restricted research
login; nothing changes production.

Evaluation: 2025 holdout (paired log loss by line, slate-date bootstrap) and
2026 two-sided lines (latest pregame production prediction per player-game and
line, median same-book no-vig market) from a ``--price-both-sides`` export.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import psycopg
from scipy.stats import poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import PoissonRegressor
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402
from backend.nhl.scripts.research_nhl_points_hgb_star_ceiling import (  # noqa: E402
    log_loss_rows, paired_bootstrap)
from backend.nhl.sog_hgb_features import position_groups  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/month_end_2026-11-01"
BASE = list(scorer.CORE_FEATURES)
HGB_KW = json.loads(scorer.CONTRACT_PATH.read_text())["hyperparameters"]
KEYS = ["game_id", "player_id"]
LINES = (0.5, 1.5, 2.5)
ASSISTS = ["assists_last10", "assists_season_per_game", "involvement_last10"]
CANDIDATES = {
    "OFFSET": BASE,
    "OFFSET+ASSISTS": BASE + ASSISTS,
    "OFFSET+DEFENSE": BASE + ["is_defense"],
    "OFFSET+SHARE": [f if f != "last10_team_sog_share" else "share_played" for f in BASE],
}


def extra_features(history: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    """New inputs over the same strict-prior window as build_features.

    Window: the player's games before the target date, within 120 days, last 10.
    Team goals and team SOG are summed only over those games (games he played).
    """
    team = (history.groupby(["canonical_team_id", "game_id"], as_index=False)
            .agg(team_goals=("goals", "sum"), team_sog=("shots_on_goal", "sum")))
    h = history.merge(team, on=["canonical_team_id", "game_id"], how="left")
    groups = {int(pid): g for pid, g in h.groupby("player_id", sort=False)}
    rows = []
    for t in targets.itertuples(index=False):
        d = pd.Timestamp(t.game_date)
        g = groups.get(int(t.player_id), h.iloc[:0])
        prior = g.loc[g.game_date < d]
        w = prior.loc[prior.game_date >= d - pd.Timedelta(days=120)].tail(10)
        season = prior.loc[prior.canonical_season.eq(int(t.canonical_season))]
        tg, ts = float(w.team_goals.sum()), float(w.team_sog.sum())
        rows.append({"game_id": int(t.game_id), "player_id": int(t.player_id),
                     "assists_last10": w.assists.mean() if len(w) else np.nan,
                     "assists_season_per_game": season.assists.sum() / len(season) if len(season) else np.nan,
                     "involvement_last10": w.realized_points.sum() / tg if tg > 0 else np.nan,
                     "share_played": float(w.shots_on_goal.fillna(0).sum()) / ts if ts > 0 else 0.0,
                     "season_sog_prior": float(season.shots_on_goal.fillna(0).sum())})
    return pd.DataFrame(rows)


def positions() -> pd.DataFrame:
    with psycopg.connect(database_url("research", environ=refit.research_env()), prepare_threshold=None) as conn, \
            conn.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("SELECT player_id::bigint, position FROM nhl.players")
        frame = pd.DataFrame(cur.fetchall(), columns=["player_id", "position"])
    groups = position_groups(frame)
    return groups.assign(is_defense=groups.group.eq("D").astype(int))[["player_id", "is_defense"]]


def market_rows(export: Path, since: str) -> pd.DataFrame:
    """Latest pregame production Points prediction per player-game and line, with a two-sided market."""
    p = pd.read_csv(export / "candidate_predictions.csv", low_memory=False)
    p = p[(p.market_key == "player_points") & (p.model_role == "PRODUCTION") & (p.slate_date >= since)
          & (p.outcome_status == "SETTLED")].copy()
    p = p[pd.to_datetime(p.prediction_timestamp_utc, format="ISO8601", utc=True)
          < pd.to_datetime(p.scheduled_start_utc, format="ISO8601", utc=True)]
    p["line"] = pd.to_numeric(p.market_line, errors="coerce")
    p["points"] = pd.to_numeric(p.outcome_value, errors="coerce")
    p["game_id"] = pd.to_numeric(p.game_id).astype("int64")
    p["player_id"] = pd.to_numeric(p.player_id).astype("int64")
    p = p.dropna(subset=["points", "line"]).sort_values("prediction_timestamp_utc")
    p = p.groupby(KEYS + ["line"], as_index=False).tail(1)
    q = pd.concat(c[c.candidate_key.isin(set(p.candidate_key))] for c in pd.read_csv(
        export / "candidate_quotes.csv", chunksize=500000, low_memory=False))
    q["imp"] = pd.to_numeric(q.implied_probability, errors="coerce")
    q = q.sort_values("observation_timestamp_utc").groupby(["candidate_key", "sportsbook_key", "quote_side"]).tail(1)
    w = q.pivot_table(index=["candidate_key", "sportsbook_key"], columns="quote_side", values="imp").dropna()
    w["nv_over"] = w["OVER"] / (w["OVER"] + w["UNDER"])
    market = w.groupby("candidate_key").nv_over.median().rename("market_p_over").reset_index()
    p = p.merge(market, on="candidate_key", how="inner")
    return p[["candidate_key", "slate_date", "game_id", "player_id", "line", "selected_side",
              "probability_over", "points", "market_p_over"]].reset_index(drop=True)


def build_matrices(export: Path, since: str) -> dict:
    logs, db_audit = refit.pull_history()
    logs, pp_audit = refit.apply_pp_zero_rule(logs)
    frozen = pd.read_csv(refit.FROZEN_FRAME, low_memory=False)
    frozen["game_id"] = frozen.game_id.astype("int64"); frozen["player_id"] = frozen.player_id.astype("int64")
    outcomes, outcome_audit = refit.official_outcomes(frozen)
    history, history_audit = refit.history_inputs(logs, outcomes)
    games = logs[["game_id", "game_date", "start_time_utc", "home_team_id", "away_team_id"]].drop_duplicates("game_id")
    frozen = frozen.loc[frozen.game_id.isin(games.game_id)].reset_index(drop=True)
    hist_targets = frozen[KEYS + ["canonical_season", "is_home"]]
    rebuilt = refit.build_targets(history, games, hist_targets)
    rebuilt = rebuilt.merge(frozen[KEYS + ["canonical_season", "realized_points"]], on=KEYS, validate="one_to_one")
    rows = market_rows(export, since)
    side = logs[KEYS + ["team_id", "home_team_id"]].drop_duplicates(KEYS)
    t26 = rows[KEYS].drop_duplicates().merge(side, on=KEYS, how="inner")
    t26["is_home"] = t26.team_id.astype(int).eq(t26.home_team_id.astype(int))
    t26["canonical_season"] = 2026
    f26 = refit.build_targets(history, games, t26[KEYS + ["canonical_season", "is_home"]])
    f26["canonical_season"] = 2026
    pos = positions()
    out = {}
    for name, frame in (("historical", rebuilt), ("2026", f26)):
        targets = frame[KEYS + ["game_date", "canonical_season"]]
        frame = frame.merge(extra_features(history, targets), on=KEYS, validate="one_to_one")
        frame = frame.drop(columns=["is_defense"], errors="ignore").merge(pos, on="player_id", how="left")
        frame["is_defense"] = frame.is_defense.fillna(0).astype(int)
        out[name] = frame
    out["market_2026"] = rows
    out["audit"] = {"database": db_audit, "pp_zero_rule": pp_audit, "labels": outcome_audit, "history": history_audit,
                    "defense_share_historical": float(out["historical"].is_defense.mean())}
    return out


def x_matrix(frame: pd.DataFrame, features: list[str]) -> np.ndarray:
    x = frame[features].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)
    x["attempts_d10_per60"] = scorer.impute_unknown_attempts(x)
    return x.fillna(0).astype(float).to_numpy()


def fit_offset(train: pd.DataFrame, features: list[str]) -> dict:
    x = x_matrix(train, features)
    y = train.realized_points.to_numpy(dtype=float)
    scaler = StandardScaler().fit(x)
    xs = scaler.transform(x)
    glm = PoissonRegressor(alpha=1e-4, max_iter=1000).fit(xs, y)
    m = glm.predict(xs)
    hgb = HistGradientBoostingRegressor(**HGB_KW).fit(xs, y / m, sample_weight=m)
    return {"features": features, "scaler": scaler, "glm": glm, "offset": hgb}


def predict_offset(model: dict, frame: pd.DataFrame) -> np.ndarray:
    xs = model["scaler"].transform(x_matrix(frame, model["features"]))
    return model["glm"].predict(xs) * np.maximum(model["offset"].predict(xs), 1e-8)


def predict_production(production, frame: pd.DataFrame) -> np.ndarray:
    return scorer.score(frame.assign(game_id=0, player_id=range(len(frame))), production).expected_points.to_numpy()


def slices(frame: pd.DataFrame) -> dict[str, np.ndarray]:
    games = frame.current_season_games_prior.to_numpy()
    ppg = (frame.current_season_points_prior / frame.current_season_games_prior.replace(0, np.nan)).to_numpy()
    pps = (frame.current_season_points_prior / frame.season_sog_prior.replace(0, np.nan)).to_numpy()
    fwd10 = (frame.is_defense.to_numpy() == 0) & (games >= 10) & np.isfinite(pps)
    cut = np.nanquantile(pps[fwd10], 2 / 3) if fwd10.any() else np.inf
    return {"all": np.ones(len(frame), bool),
            "stars_ppg_1.2+": (ppg >= 1.2) & (games >= 10),
            "playmakers_top_third_points_per_sog": fwd10 & (pps >= cut),
            "defense": frame.is_defense.to_numpy() == 1}


def holdout_eval(preds: dict, hold: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    pts = hold.realized_points.to_numpy(dtype=float)
    dates = hold.slate_date.astype(str).to_numpy()
    paired, level = [], []
    for s, mask in slices(hold).items():
        for label, mu in preds.items():
            level.append({"slice": s, "model": label, "n": int(mask.sum()), "mean_expected": float(mu[mask].mean()),
                          "mean_actual": float(pts[mask].mean())})
        for line in LINES:
            y = (pts > line).astype(float)
            loss = {k: log_loss_rows(poisson.sf(int(line), mu), y) for k, mu in preds.items()}
            for a, b in [(k, "PRODUCTION") for k in preds if k != "PRODUCTION"] + \
                        [(k, "OFFSET") for k in preds if k not in ("PRODUCTION", "OFFSET")]:
                paired.append({"slice": s, "line": line, "comparison": f"{a} - {b}",
                               **paired_bootstrap(loss[a][mask], loss[b][mask], dates[mask])})
    return pd.DataFrame(paired), pd.DataFrame(level)


def market_eval(preds: dict, rows: pd.DataFrame, f26: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    idx = rows[KEYS].merge(f26[KEYS].reset_index(), on=KEYS, how="left")["index"].to_numpy()
    y = (rows.points > rows.line).astype(float).to_numpy()
    thr = rows.line.astype(int).to_numpy()
    probs = {k: poisson.sf(thr, mu[idx]) for k, mu in preds.items()}
    probs["MARKET"] = rows.market_p_over.to_numpy()
    sl = {k: v[idx] for k, v in slices(f26).items()}
    dates = rows.slate_date.astype(str).to_numpy()
    paired, record = [], []
    for s, mask in sl.items():
        for line in (None, *LINES):
            m = mask if line is None else mask & (rows.line.to_numpy() == line)
            if m.sum() < 30:
                continue
            for label, p in probs.items():
                pick = p[m] >= 0.5
                won = pick == (y[m] == 1)
                record.append({"slice": s, "line": line or "all", "model": label, "n": int(m.sum()),
                               "log_loss": float(log_loss_rows(p[m], y[m]).mean()),
                               "W-L": f"{int(won.sum())}-{int((~won).sum())}"})
            for a in [k for k in preds if k != "PRODUCTION"]:
                for b in ("PRODUCTION", "MARKET") + (("OFFSET",) if a != "OFFSET" else ()):
                    paired.append({"slice": s, "line": line or "all", "comparison": f"{a} - {b}",
                                   **paired_bootstrap(log_loss_rows(probs[a][m], y[m]),
                                                      log_loss_rows(probs[b][m], y[m]), dates[m])})
    return pd.DataFrame(paired), pd.DataFrame(record)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--export", type=Path, required=True)
    ap.add_argument("--since", default="2026-10-16", help="first 2026 slate (clean attempts inputs)")
    ap.add_argument("--rebuild", action="store_true", help="pull and rebuild matrices even if cached")
    ap.add_argument("--combined", default="", help="comma-separated additions to fit together, e.g. ASSISTS,DEFENSE")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    cache = OUT / "matrices.joblib"
    if args.rebuild or not cache.exists():
        joblib.dump(build_matrices(args.export, args.since), cache)
    data = joblib.load(cache)
    hist, f26, rows = data["historical"], data["2026"], data["market_2026"]
    f26 = f26.reset_index(drop=True)
    rows = rows.merge(f26[KEYS], on=KEYS, how="inner").reset_index(drop=True)
    train = hist[hist.canonical_season.isin([2023, 2024])].reset_index(drop=True)
    hold = hist[hist.canonical_season.eq(2025)].reset_index(drop=True)
    saved = pd.read_csv(refit.OUT / "rebuilt_holdout_matrix_2025.csv.gz")
    par = hold[KEYS + BASE].merge(saved[KEYS + BASE], on=KEYS, suffixes=("", "_saved"))
    parity = {f: int((~np.isclose(par[f].astype(float), par[f + "_saved"].astype(float), equal_nan=True)).sum())
              for f in BASE}
    candidates = dict(CANDIDATES)
    if args.combined:
        feats = list(BASE)
        for add in args.combined.split(","):
            if add == "SHARE":
                feats = [f if f != "last10_team_sog_share" else "share_played" for f in feats]
            else:
                feats += {"ASSISTS": ASSISTS, "DEFENSE": ["is_defense"]}[add]
        candidates["OFFSET+" + "+".join(args.combined.split(","))] = feats
    production = joblib.load(scorer.MODEL_PATH)
    models = {k: fit_offset(train, f) for k, f in candidates.items()}
    joblib.dump(models, OUT / "candidates.joblib")
    hp = {"PRODUCTION": predict_production(production, hold), **{k: predict_offset(m, hold) for k, m in models.items()}}
    p26 = {"PRODUCTION": predict_production(production, f26), **{k: predict_offset(m, f26) for k, m in models.items()}}
    hold_paired, hold_level = holdout_eval(hp, hold)
    mk_paired, mk_record = market_eval(p26, rows, f26)
    captured = poisson.sf(rows.line.astype(int).to_numpy(),
                          p26["PRODUCTION"][rows[KEYS].merge(f26[KEYS].reset_index(), on=KEYS)["index"].to_numpy()])
    for name, frame in (("holdout_2025_paired_log_loss", hold_paired), ("holdout_2025_levels", hold_level),
                        ("two_sided_2026_paired_log_loss", mk_paired), ("two_sided_2026_records", mk_record)):
        frame.to_csv(OUT / f"{name}.csv", index=False)
    summary = {"export": str(args.export), "since": args.since, "training_rows": len(train), "holdout_rows": len(hold),
               "two_sided_rows": len(rows), "two_sided_dates": int(rows.slate_date.nunique()),
               "last_slate": str(rows.slate_date.max()), "candidates": candidates,
               "holdout_feature_parity_vs_2026_10_24_mismatches": parity,
               "production_rebuilt_vs_captured_p_over_max_abs_diff":
                   float(np.abs(captured - rows.probability_over.astype(float)).max()),
               "production_rebuilt_vs_captured_p_over_mean_abs_diff":
                   float(np.abs(captured - rows.probability_over.astype(float)).mean()),
               "audit": {k: v for k, v in data["audit"].items() if k != "database"} | {
                   "database_login": data["audit"]["database"]["login"],
                   "snapshot_utc": data["audit"]["database"]["transaction_timestamp_utc"]}}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    pd.set_option("display.width", 220); pd.set_option("display.max_rows", 500)
    print(json.dumps(summary, indent=1, default=str))
    print(hold_level.to_string(index=False))
    print(hold_paired.to_string(index=False))
    print(mk_paired.to_string(index=False))
    print(mk_record.to_string(index=False))


if __name__ == "__main__":
    main()
