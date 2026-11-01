"""NHL Saves refit: expected shots against x the goalie's save rate (P2.41).

One module for training, freezing and live scoring, so the same code builds
features in all three. Validated read-only in
``scripts/research_nhl_saves_refit.py`` (2025 holdout and 2026 lines).

- Team-game shots: shots on goal summed over the team's skaters (official
  skater logs, canonical team), shots against = the opponent's shots.
- Team inputs (strict prior, regular season): season-to-date shots for and
  against per game shrunk toward the team's previous season (itself half
  shrunk to the league) by ``K_TEAM`` games, last 10, rest days, home, and the
  league's shots per team-game so far this season. Rates enter relative to
  that league level and the shots model has it as a log offset, so a
  league-wide change passes through one-to-one.
- Goalie save rate: saves over shots faced, previous season plus this season
  so far (appearances with ice time), shrunk toward last season's league rate
  by ``K_SAVE`` shots.
- Expected saves (for the named goalie starting) = shots against x save rate
  x ``scale``; counts are negative binomial with variance ``ratio`` x mean.
  No calibration layer (operator, 2026-11-01).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import nbinom
from sklearn.linear_model import PoissonRegressor
from sklearn.preprocessing import StandardScaler

MODEL_ID = "NHL_SAVES_SHOTS_X_SAVE_RATE_V1"
K_TEAM = 20
K_SAVE = 1000
LINES = tuple(18.5 + i for i in range(13))
RATE_COLS = ("sa_shr", "sa_l10", "opp_sf_shr", "opp_sf_l10", "sf_shr", "opp_sa_shr")
SHOT_FEATURES = [*RATE_COLS, "is_home", "rest_days", "opp_rest_days"]


def team_games(logs: pd.DataFrame) -> pd.DataFrame:
    """One row per team-game from skater logs (columns: season, game_date, game_id,
    team_id, home_team_id, away_team_id, shots_on_goal). Target games may be
    appended with NaN shots; they get strict-prior features only."""
    g = (logs.groupby(["season", "game_date", "game_id", "team_id", "home_team_id", "away_team_id"], as_index=False)
         .agg(sf=("shots_on_goal", lambda v: v.sum(min_count=1))))
    g["opp_id"] = np.where(g.team_id == g.home_team_id, g.away_team_id, g.home_team_id)
    g = g.merge(g[["game_id", "team_id", "sf"]].rename(columns={"team_id": "opp_id", "sf": "sa"}),
                on=["game_id", "opp_id"], how="left")
    g["is_home"] = (g.team_id == g.home_team_id).astype(int)
    g["game_date"] = pd.to_datetime(g.game_date)
    return g.sort_values(["team_id", "game_date", "game_id"]).reset_index(drop=True)


def add_targets(g: pd.DataFrame, slate: pd.DataFrame, season: int) -> pd.DataFrame:
    """Append upcoming team-games (slate: game_id, game_date, home_team_id, away_team_id)."""
    rows = []
    for r in slate.drop_duplicates("game_id").itertuples(index=False):
        for team, opp in ((r.home_team_id, r.away_team_id), (r.away_team_id, r.home_team_id)):
            rows.append({"season": season, "game_date": pd.Timestamp(r.game_date), "game_id": int(r.game_id),
                         "team_id": int(team), "home_team_id": int(r.home_team_id), "away_team_id": int(r.away_team_id),
                         "opp_id": int(opp), "is_home": int(team == r.home_team_id), "sf": np.nan, "sa": np.nan})
    return pd.concat([g, pd.DataFrame(rows)], ignore_index=True).sort_values(
        ["team_id", "game_date", "game_id"]).reset_index(drop=True)


def team_features(g: pd.DataFrame, extra_prev: pd.DataFrame | None = None) -> pd.DataFrame:
    """Strict-prior team inputs. ``extra_prev``: team_id, season, games, sf, sa season
    totals for a season without game rows (2022-23 from the NHL stats API)."""
    g = g.copy()
    done = g[g.sf.notna() & g.sa.notna()]
    tot = done.groupby(["team_id", "season"]).agg(games=("sf", "size"), sf=("sf", "sum"), sa=("sa", "sum")).reset_index()
    if extra_prev is not None:
        tot = pd.concat([tot, extra_prev[["team_id", "season", "games", "sf", "sa"]]], ignore_index=True)
    league = tot.groupby("season").apply(lambda t: t.sf.sum() / t.games.sum(), include_groups=False)
    first = float(league.iloc[0])
    lg_prev = g.season.map(lambda s: float(league.get(s - 1, first)))
    prev = tot.assign(season=tot.season + 1)
    prev["prev_sf"], prev["prev_sa"] = prev.sf / prev.games, prev.sa / prev.games
    g = g.merge(prev[["team_id", "season", "prev_sf", "prev_sa"]], on=["team_id", "season"], how="left")
    g = g.sort_values(["team_id", "game_date", "game_id"]).reset_index(drop=True)
    by = g.groupby(["team_id", "season"], sort=False)
    g["games_prior"] = by.sf.transform(lambda v: v.notna().astype(int).shift(1).cumsum()).fillna(0)
    lg_prev = g.season.map(lambda s: float(league.get(s - 1, first)))
    for s in ("sf", "sa"):
        prior = 0.5 * g[f"prev_{s}"].fillna(lg_prev) + 0.5 * lg_prev
        cum = by[s].transform(lambda v: v.fillna(0).shift(1).cumsum()).fillna(0)
        g[f"{s}_shr"] = (cum + K_TEAM * prior) / (g.games_prior + K_TEAM)
        g[f"{s}_l10"] = by[s].transform(lambda v: v.shift(1).rolling(10, min_periods=1).mean())
        g[f"{s}_l10"] = g[f"{s}_l10"].fillna(prior)
    g["rest_days"] = by.game_date.transform(lambda d: d.diff().dt.days).fillna(7).clip(upper=7)
    daily = (g[g.sf.notna()].groupby(["season", "game_date"]).agg(shots=("sf", "sum"), n=("sf", "size")).reset_index())
    dates = g[["season", "game_date"]].drop_duplicates().merge(daily, on=["season", "game_date"], how="left").fillna(
        {"shots": 0, "n": 0}).sort_values(["season", "game_date"])
    sb = dates.groupby("season")
    dates["league_spg_to_date"] = (sb.shots.transform(lambda v: v.shift(1).cumsum())
                                   / sb.n.transform(lambda v: v.shift(1).cumsum()).replace(0, np.nan))
    dates["league_spg_to_date"] = dates.league_spg_to_date.fillna(dates.season.map(lambda s: float(league.get(s - 1, first))))
    g = g.merge(dates[["season", "game_date", "league_spg_to_date"]], on=["season", "game_date"], how="left")
    opp = g[["game_id", "team_id", "rest_days", "sf_shr", "sf_l10", "sa_shr", "sa_l10"]].rename(columns={
        "team_id": "opp_id", "rest_days": "opp_rest_days", "sf_shr": "opp_sf_shr", "sf_l10": "opp_sf_l10",
        "sa_shr": "opp_sa_shr", "sa_l10": "opp_sa_l10"})
    return g.merge(opp, on=["game_id", "opp_id"], how="left")


def save_rates(goalies: pd.DataFrame) -> pd.DataFrame:
    """Strict-prior shrunk save rate per goalie row (columns: season, game_id,
    player_id, game_date, toi, saves, shots). Target rows may have NaN toi."""
    d = goalies.copy()
    d["game_date"] = pd.to_datetime(d.game_date)
    played = d.toi.fillna(0) > 0
    d["_sv"], d["_sh"] = d.saves.where(played, 0).fillna(0), d.shots.where(played, 0).fillna(0)
    season = d.groupby(["player_id", "season"]).agg(prev_saves=("_sv", "sum"), prev_shots=("_sh", "sum")).reset_index()
    tot = d[played].groupby("season").agg(sv=("_sv", "sum"), sh=("_sh", "sum"))
    league = tot.sv / tot.sh
    first = float(league.iloc[0])
    d = d.merge(season.assign(season=season.season + 1), on=["player_id", "season"], how="left")
    d[["prev_saves", "prev_shots"]] = d[["prev_saves", "prev_shots"]].fillna(0)
    d = d.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)
    by = d.groupby(["player_id", "season"], sort=False)
    d["cur_saves"] = by._sv.transform(lambda v: v.shift(1).cumsum()).fillna(0)
    d["cur_shots"] = by._sh.transform(lambda v: v.shift(1).cumsum()).fillna(0)
    d["league_sv_prev"] = d.season.map(lambda s: float(league.get(s - 1, first)))
    d["save_rate"] = (d.prev_saves + d.cur_saves + K_SAVE * d.league_sv_prev) / (d.prev_shots + d.cur_shots + K_SAVE)
    d["goalie_shots_prior"] = d.prev_shots + d.cur_shots
    return d.drop(columns=["_sv", "_sh"])


def _relative(frame: pd.DataFrame) -> np.ndarray:
    x = frame[SHOT_FEATURES].astype(float).copy()
    for c in RATE_COLS:
        x[c] = x[c] / frame.league_spg_to_date.astype(float)
    return x.to_numpy()


def fit(team_rows: pd.DataFrame, starters: pd.DataFrame) -> dict:
    """team_rows: completed team-games with team features; starters: starter rows with
    save_rate, saves and the team's features (exp_sa is computed here)."""
    x = _relative(team_rows)
    scaler = StandardScaler().fit(x)
    level = team_rows.league_spg_to_date.to_numpy(float)
    glm = PoissonRegressor(alpha=1e-4, max_iter=1000).fit(
        scaler.transform(x), team_rows.sa.to_numpy(float) / level, sample_weight=level)
    exp_sa = glm.predict(scaler.transform(_relative(starters))) * starters.league_spg_to_date.to_numpy(float)
    raw = exp_sa * starters.save_rate.to_numpy(float)
    scale = float(starters.saves.sum() / raw.sum())
    mu = scale * raw
    ratio = float(((starters.saves.to_numpy(float) - mu) ** 2).mean() / mu.mean())
    return {"model_id": MODEL_ID, "shot_features": SHOT_FEATURES, "rate_columns": list(RATE_COLS),
            "scaler": scaler, "shots_glm": glm, "scale": scale, "variance_ratio": ratio,
            "k_team": K_TEAM, "k_save": K_SAVE, "lines": list(LINES), "calibration_layer": "NONE"}


def expected_saves(model: dict, rows: pd.DataFrame) -> np.ndarray:
    exp_sa = model["shots_glm"].predict(model["scaler"].transform(_relative(rows))) * rows.league_spg_to_date.to_numpy(float)
    return model["scale"] * exp_sa * rows.save_rate.to_numpy(float)


def prob_over(model: dict, mu: np.ndarray) -> pd.DataFrame:
    """P(saves > line) at every line from the negative binomial; monotone by construction."""
    p = 1.0 / model["variance_ratio"]
    n = np.asarray(mu, float) * p / (1 - p)
    return pd.DataFrame({f"p_over_{str(line).replace('.', '_')}": nbinom.sf(int(line), n, p) for line in model["lines"]})
