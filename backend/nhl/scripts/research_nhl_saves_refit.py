#!/usr/bin/env python3
"""Research: Saves refit = expected shots against x the goalie's save rate (P2.41, 2026-11-01).

Production Saves predicts worse than a flat league mean (P2.41 review). Saves
are mostly shots faced, which the fitted model never sees. This candidate:

1. Expected shots against for the goalie's team: Poisson GLM on strict-prior
   team inputs (the team's shots-against and the opponent's shots-for rates,
   shrunk to last season, last 10, the team's own shots-for, the opponent's
   shots-against, home, rest, league shot level so far), from the team-game
   table of ``research_nhl_team_goals_model.py``.
2. The goalie's save rate: his saves over shots faced, previous season plus
   this season so far, shrunk toward last season's league rate by K shots.
3. Expected saves for a starter = shots against x save rate x one fitted
   scale (absorbs pulled starts); counts are negative binomial with the
   training variance ratio. No calibration layer (operator 2026-11-01).

Train 2023-24 starters, test 2025 (goalie logs complete since the P2.41
backfill) and 2026. Starters: the boxscore ``starter`` flag (retained 2023-24
boxscores; ``start_flag`` for 2025-26). 2026 is also compared on the same
graded starters and lines with production (published and raw probabilities)
and the market (median same-book no-vig). Read-only; nothing changes.
"""
from __future__ import annotations

import glob
import gzip
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import psycopg
from scipy.stats import nbinom, poisson
from sklearn.linear_model import PoissonRegressor
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_team_goals_model as team  # noqa: E402
from backend.nhl.scripts.run_nhl_points_hgb_pp_corrected_refit import research_env  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/saves_refit_2026-11-01"
BOX_2023_24 = ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources"
EXPORT = ROOT / "artifacts/analysis/nhl/all_candidate_evaluation_export/full_window_2026-11-01_both_sides"
LINES = [18.5 + i for i in range(13)]
SHOT_FEATURES = ["sa_shr", "sa_l10", "opp_sf_shr", "opp_sf_l10", "sf_shr", "opp_sa_shr",
                 "is_home", "rest_days", "opp_rest_days", "league_spg_to_date"]
KS = (500, 1000, 2000)


def goalie_logs() -> pd.DataFrame:
    with psycopg.connect(database_url("research", environ=research_env()), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT g.season, l.game_id::bigint, l.player_id::bigint, l.team_id, g.game_date,
                              l.toi_minutes, l.saves, l.shots_faced, l.goals_allowed, l.start_flag
                       FROM nhl.goalie_game_logs_raw l JOIN nhl.games g USING(game_id)
                       WHERE g.season IN (2023, 2024, 2025, 2026) AND substring(g.game_id::text,5,2) = '02'""")
        d = pd.DataFrame(cur.fetchall(), columns=["season", "game_id", "player_id", "team_id", "game_date", "toi",
                                                   "saves", "shots", "goals_allowed", "start_flag"])
    for col in ("toi", "saves", "shots", "goals_allowed"):
        d[col] = pd.to_numeric(d[col], errors="coerce")
    d["shots"] = d.shots.fillna(d.saves + d.goals_allowed).where(d.toi > 0)
    starters, teams = {}, {}
    for path in BOX_2023_24.glob("game=*/boxscore.json.gz"):
        box = json.loads(gzip.open(path).read())
        for side in ("homeTeam", "awayTeam"):
            for g in (box.get("playerByGameStats") or {}).get(side, {}).get("goalies") or []:
                starters[(int(box["id"]), int(g["playerId"]))] = bool(g.get("starter"))
                teams[(int(box["id"]), int(g["playerId"]))] = int(box[side]["id"])
    old = d.season.isin([2023, 2024])
    keys = list(zip(d.game_id[old], d.player_id[old]))
    d.loc[old, "start_flag"] = [starters.get(k) for k in keys]
    # 2023-24 logs carry legacy raw team IDs; bind each goalie to his boxscore side.
    d.loc[old, "team_id"] = [teams.get(k, np.nan) for k in keys]
    d["game_date"] = pd.to_datetime(d.game_date)
    return d


def save_rates(d: pd.DataFrame) -> pd.DataFrame:
    """Strict-prior shrunk save rate per goalie-game (previous season + this season so far)."""
    played = d[d.toi > 0].copy()
    season = played.groupby(["player_id", "season"]).agg(s_saves=("saves", "sum"), s_shots=("shots", "sum")).reset_index()
    league = played.groupby("season").apply(lambda t: t.saves.sum() / t.shots.sum(), include_groups=False)
    prev = season.assign(season=season.season + 1).rename(columns={"s_saves": "prev_saves", "s_shots": "prev_shots"})
    d = d.merge(prev, on=["player_id", "season"], how="left")
    d[["prev_saves", "prev_shots"]] = d[["prev_saves", "prev_shots"]].fillna(0)
    d = d.sort_values(["player_id", "game_date", "game_id"])
    by = d.groupby(["player_id", "season"], sort=False)
    d["cur_saves"] = by.saves.transform(lambda v: v.where(d.loc[v.index, "toi"] > 0).fillna(0).shift(1).cumsum()).fillna(0)
    d["cur_shots"] = by.shots.transform(lambda v: v.fillna(0).shift(1).cumsum()).fillna(0)
    d["league_sv_prev"] = d.season.map(lambda s: league.get(s - 1, league.iloc[0]))
    for k in KS:
        d[f"sv_k{k}"] = ((d.prev_saves + d.cur_saves + k * d.league_sv_prev)
                         / (d.prev_shots + d.cur_shots + k))
    d["goalie_shots_prior"] = d.prev_shots + d.cur_shots
    return d


def team_shot_frame() -> pd.DataFrame:
    g, _ = joblib.load(team.OUT / "team_games.joblib")
    f = team.features(g)
    daily = f.groupby(["canonical_season", "game_date"]).agg(shots=("sf", "sum"), n=("sf", "size")).reset_index()
    daily = daily.sort_values(["canonical_season", "game_date"])
    sb = daily.groupby("canonical_season")
    daily["league_spg_to_date"] = sb.shots.transform(lambda v: v.shift(1).cumsum()) / sb.n.transform(lambda v: v.shift(1).cumsum())
    first = daily.groupby("canonical_season").apply(lambda t: t.shots.sum() / t.n.sum(), include_groups=False)
    daily["league_spg_to_date"] = daily.league_spg_to_date.fillna(daily.canonical_season.map(lambda s: first.get(s - 1, first.iloc[0])))
    return f.merge(daily[["canonical_season", "game_date", "league_spg_to_date"]], on=["canonical_season", "game_date"])


def nb_sf(line: float, mu: np.ndarray, ratio: float) -> np.ndarray:
    p = 1.0 / ratio
    n = mu * p / (1 - p)
    return nbinom.sf(int(line), n, p)


def nb_ll(y: np.ndarray, mu: np.ndarray, ratio: float) -> float:
    p = 1.0 / ratio
    return float(-nbinom.logpmf(y, mu * p / (1 - p), p).mean())


def market_saves() -> pd.DataFrame:
    p = pd.read_csv(EXPORT / "candidate_predictions.csv", low_memory=False)
    p = p[(p.market_key == "goalie_saves") & (p.model_role == "PRODUCTION")].copy()
    p["line"] = pd.to_numeric(p.market_line, errors="coerce")
    p["goalie_id"] = pd.to_numeric(p.goalie_id, errors="coerce")
    p = p.dropna(subset=["line", "goalie_id"])
    q = pd.concat(c[c.candidate_key.isin(set(p.candidate_key))] for c in pd.read_csv(
        EXPORT / "candidate_quotes.csv", chunksize=500000, low_memory=False))
    q["imp"] = pd.to_numeric(q.implied_probability, errors="coerce")
    q = q.sort_values("observation_timestamp_utc").groupby(["candidate_key", "sportsbook_key", "quote_side"]).tail(1)
    w = q.pivot_table(index=["candidate_key", "sportsbook_key"], columns="quote_side", values="imp").dropna()
    w["nv_over"] = w["OVER"] / (w["OVER"] + w["UNDER"])
    m = w.groupby("candidate_key").nv_over.median().rename("market_p_over").reset_index()
    p = p.merge(m, on="candidate_key", how="inner").sort_values("prediction_timestamp_utc")
    return p.groupby(["game_id", "goalie_id", "line"], as_index=False).tail(1)[
        ["game_id", "goalie_id", "line", "market_p_over"]].astype({"game_id": "int64", "goalie_id": "int64"})


def production_2026() -> pd.DataFrame:
    frames = []
    for d in sorted(glob.glob(str(ROOT / "artifacts/operational/nhl/postgame_reconciliation/2026-1*"))):
        pk = sorted(glob.glob(d + "/reconciliation=*/graded_saves_settled_starters.csv"), key=lambda x: Path(x).stat().st_mtime)
        if pk:
            frames.append(pd.read_csv(pk[-1]))
    s = pd.concat(frames, ignore_index=True)
    s = s[s.grading_status.eq("SETTLED")].sort_values("prediction_timestamp_utc")
    s = s.groupby(["game_id", "goalie_id", "line"], as_index=False).tail(1)
    return s[["game_id", "goalie_id", "line", "prob_over", "raw_prob_over", "expected_saves", "official_saves",
              "prediction_timestamp_utc"]].astype({"game_id": "int64", "goalie_id": "int64"})


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    logs = save_rates(goalie_logs())
    tf = team_shot_frame()
    st = logs[logs.start_flag.eq(True)].merge(
        tf[["game_id", "team_id", "sa", *SHOT_FEATURES]], on=["game_id", "team_id"], how="inner")
    tg_train = tf[tf.canonical_season.isin([2023, 2024])]
    # Shots relative to the league's rate so far this season (log offset), so a
    # league-wide level change passes through one-to-one; team rates enter as
    # ratios to that level. Fitted as y / level with weight level.
    def rel(frame):
        x = frame[SHOT_FEATURES].astype(float).copy()
        for c in ("sa_shr", "sa_l10", "opp_sf_shr", "opp_sf_l10", "sf_shr", "opp_sa_shr"):
            x[c] = x[c] / frame.league_spg_to_date
        return x.drop(columns=["league_spg_to_date"]).to_numpy()
    x = rel(tg_train)
    scaler = StandardScaler().fit(x)
    level = tg_train.league_spg_to_date.to_numpy(float)
    shots_glm = PoissonRegressor(alpha=1e-4, max_iter=1000).fit(scaler.transform(x), tg_train.sa.to_numpy(float) / level,
                                                               sample_weight=level)
    st["exp_sa"] = shots_glm.predict(scaler.transform(rel(st))) * st.league_spg_to_date.to_numpy(float)
    shot_eval = {}
    for s in (2025, 2026):
        t = tf[tf.canonical_season.eq(s)]
        mu = shots_glm.predict(scaler.transform(rel(t))) * t.league_spg_to_date.to_numpy(float)
        y = t.sa.to_numpy(float)
        shot_eval[s] = {"team_games": len(t), "corr": float(np.corrcoef(mu, y)[0, 1]),
                        "poisson_ll": float(-poisson.logpmf(y, mu).mean()),
                        "flat_ll": float(-poisson.logpmf(y, np.full(len(y), y.mean())).mean()),
                        "mean_pred": float(mu.mean()), "mean_actual": float(y.mean())}
    train = st[st.season.isin([2023, 2024])]
    results, models = {}, {}
    # Save rate relative to last season's league rate, applied to this season's
    # league rate so far (same pass-through idea).
    for k in KS:
        raw = train.exp_sa * train[f"sv_k{k}"]
        scale = float(train.saves.sum() / raw.sum())
        resid = train.saves - scale * raw
        ratio = float((resid ** 2).mean() / (scale * raw).mean())
        models[k] = {"scale": scale, "ratio": ratio}
        for s in (2025, 2026):
            t = st[st.season.eq(s)]
            mu = (scale * t.exp_sa * t[f"sv_k{k}"]).to_numpy()
            y = t.saves.to_numpy(float)
            base_mu = np.full(len(y), train.saves.mean())
            results[f"k{k}_{s}"] = {"starters": len(t), "corr": float(np.corrcoef(mu, y)[0, 1]),
                                    "mae": float(np.abs(mu - y).mean()), "negbin_ll": nb_ll(y, mu, ratio),
                                    "flat_train_mean_negbin_ll": nb_ll(y, base_mu, ratio),
                                    "mean_pred": float(mu.mean()), "mean_actual": float(y.mean())}
    best = min(KS, key=lambda k: results[f"k{k}_2025"]["negbin_ll"])
    scale, ratio = models[best]["scale"], models[best]["ratio"]
    st["mu"] = scale * st.exp_sa * st[f"sv_k{best}"]
    # 2026: same graded starters and lines as production, plus the market.
    prod = production_2026()
    s26 = st[st.season.eq(2026)][["game_id", "player_id", "mu"]].rename(columns={"player_id": "goalie_id"})
    cmp_ = prod.merge(s26, on=["game_id", "goalie_id"], how="inner").merge(market_saves(), on=["game_id", "goalie_id", "line"], how="left")
    cmp_["y"] = (cmp_.official_saves > cmp_.line).astype(float)
    cmp_["p_refit"] = [float(nb_sf(l, np.array([m]), ratio)[0]) for l, m in zip(cmp_.line, cmp_.mu)]
    cmp_["slate"] = cmp_.prediction_timestamp_utc.str[:10]

    def ll(p, y):
        p = np.clip(p, 1e-6, 1 - 1e-6)
        return -(y * np.log(p) + (1 - y) * np.log(1 - p))

    rows = []
    for line, g in [("all", cmp_), *cmp_.groupby("line")]:
        r = {"line": line, "n": len(g)}
        for name in ("p_refit", "prob_over", "raw_prob_over"):
            r[f"ll_{name}"] = float(ll(g[name].to_numpy(), g.y.to_numpy()).mean())
        mk = g[g.market_p_over.notna()]
        r["n_market"] = len(mk)
        if len(mk):
            r["ll_market"] = float(ll(mk.market_p_over.to_numpy(), mk.y.to_numpy()).mean())
            r["ll_refit_on_market_rows"] = float(ll(mk.p_refit.to_numpy(), mk.y.to_numpy()).mean())
            r["ll_published_on_market_rows"] = float(ll(mk.prob_over.to_numpy(), mk.y.to_numpy()).mean())
        r["over_rate"] = float(g.y.mean()); r["refit_p"] = float(g.p_refit.mean()); r["published_p"] = float(g.prob_over.mean())
        rows.append(r)
    by_line = pd.DataFrame(rows)
    paired = {}
    d = cmp_.slate.to_numpy()
    for a, b in (("p_refit", "prob_over"), ("p_refit", "raw_prob_over")):
        paired[f"{a} - {b}"] = base.paired_bootstrap(ll(cmp_[a].to_numpy(), cmp_.y.to_numpy()),
                                                     ll(cmp_[b].to_numpy(), cmp_.y.to_numpy()), d)
    mk = cmp_[cmp_.market_p_over.notna()]
    paired["p_refit - market"] = base.paired_bootstrap(ll(mk.p_refit.to_numpy(), mk.y.to_numpy()),
                                                       ll(mk.market_p_over.to_numpy(), mk.y.to_numpy()), mk.slate.to_numpy())
    reliab = cmp_.assign(bin=pd.cut(cmp_.p_refit, [0, .2, .4, .6, .8, 1])).groupby("bin", observed=True).agg(
        n=("y", "size"), refit_p=("p_refit", "mean"), actual=("y", "mean")).reset_index()
    summary = {"shots_model_eval": shot_eval, "saves_eval_by_k": results, "best_k": best, "scale": scale,
               "variance_ratio": ratio, "train_starters": int(len(train)),
               "starters_by_season": st.groupby("season").size().to_dict(),
               "starter_flag_missing_2023_24": int(logs[logs.season.isin([2023, 2024])].start_flag.isna().sum()),
               "2026_compare_rows": int(len(cmp_)), "2026_compare_goalie_games": int(cmp_[["game_id", "goalie_id"]].drop_duplicates().shape[0]),
               "paired_2026_x1000": {k: {"diff": round(v["mean_diff"] * 1e3, 2), "ci": [round(c * 1e3, 2) for c in v["ci95"]], "n": v["n"]}
                                     for k, v in paired.items()},
               "shot_features": SHOT_FEATURES, "calibration_layer": "NONE"}
    by_line.to_csv(OUT / "compare_2026_by_line.csv", index=False)
    reliab.to_csv(OUT / "reliability_2026_refit.csv", index=False)
    joblib.dump({"scaler": scaler, "shots_glm": shots_glm, "models": models, "best_k": best}, OUT / "candidate.joblib")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    pd.set_option("display.width", 250)
    print(json.dumps(summary, indent=1, default=str))
    print(by_line.round(4).to_string(index=False))
    print(reliab.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
