"""Strict-prior SOG count-model features and fit helpers (P2.33).

Shared by the research script, the freeze script and the live HGB SOG shadow so
all three compute identical features. History is every skater log before the
slate; features use only each player's earlier cleaned regular-season games
(TOI > 0, SOG recorded), team and opponent shot history, and the prior
season's league rate by position.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

PRIOR_MINUTES = 180
BASE_KW = dict(loss="poisson", max_iter=100, max_leaf_nodes=15, l2_regularization=1.0,
               early_stopping=False, random_state=42)
FEATURES = [
    "is_home", "is_defense", "sog60_5", "sog60_10", "sog60_20", "sog60_season", "sog60_prior_season",
    "shrunk_rate_180", "shrunk_lambda_180", "toi_avg_5", "toi_avg_10", "toi_season", "pp_toi_avg_10",
    "att60_10", "att60_20", "games_season", "rest_days", "team_sf_10", "opp_sa_10", "league_prior",
]
LOG_COLUMNS = ["player_id", "game_id", "game_date", "season", "game_type", "is_home", "home_team_id",
               "away_team_id", "shots_on_goal", "toi_minutes", "pp_toi_minutes", "shot_attempts"]
HISTORY_SQL = """
SELECT l.player_id::bigint, l.game_id::bigint, g.game_date, g.season::int,
       substring(g.game_id::text, 5, 2) AS game_type, l.is_home,
       g.home_team_id::bigint, g.away_team_id::bigint,
       l.shots_on_goal, l.toi_minutes, l.pp_toi_minutes, l.shot_attempts
  FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING (game_id)
 WHERE g.season BETWEEN %(first_season)s AND %(season)s
   AND g.game_date < %(slate_date)s::date
"""


def normalize_logs(logs: pd.DataFrame) -> pd.DataFrame:
    logs = logs[LOG_COLUMNS].copy()
    for col in ("shots_on_goal", "toi_minutes", "pp_toi_minutes", "shot_attempts"):
        logs[col] = pd.to_numeric(logs[col], errors="coerce").astype(float)
    for col in ("player_id", "game_id", "season", "home_team_id", "away_team_id"):
        logs[col] = pd.to_numeric(logs[col]).astype("int64")
    logs["game_type"] = logs.game_type.astype(str).str.zfill(2)
    logs["game_date"] = pd.to_datetime(logs.game_date)
    return logs


def position_groups(positions: pd.DataFrame) -> pd.DataFrame:
    pos = positions[["player_id", "position"]].copy()
    pos["player_id"] = pd.to_numeric(pos.player_id).astype("int64")
    pos["group"] = np.where(pos.position.astype(str).str.upper().str.startswith("D"), "D", "F")
    return pos[["player_id", "group"]].drop_duplicates("player_id")


def _roll(frame: pd.DataFrame, keys, value: str, n: int | None, how: str) -> pd.Series:
    """Strict-prior rolling (or expanding, n=None) aggregate within key groups."""
    def f(s):
        s = s.shift(1)
        r = s.expanding(min_periods=1) if n is None else s.rolling(n, min_periods=1)
        return getattr(r, how)()
    return frame.groupby(keys, sort=False)[value].transform(f)


def build(logs: pd.DataFrame, pos: pd.DataFrame, targets: pd.DataFrame | None = None) -> pd.DataFrame:
    """One row per clean regular-season skater-game with strict-prior features.

    `targets` (optional) are upcoming player-games (player_id, game_id, game_date,
    season, is_home, home_team_id, away_team_id) dated after every history row;
    they get the same strict-prior features and come back with is_target = True.
    """
    logs = logs.assign(is_target=False)
    if targets is not None and len(targets):
        if pd.to_datetime(targets.game_date).min() <= pd.to_datetime(logs.game_date).max():
            raise ValueError("SOG_HGB_TARGET_NOT_AFTER_HISTORY")
        extra = targets.assign(game_type="02", shots_on_goal=np.nan, toi_minutes=np.nan,
                               pp_toi_minutes=np.nan, shot_attempts=np.nan, is_target=True)
        extra["game_date"] = pd.to_datetime(extra.game_date)
        logs = pd.concat([logs, extra[logs.columns]], ignore_index=True)
    clean = logs[logs.is_target | ((logs.game_type == "02") & (logs.toi_minutes > 0) & logs.shots_on_goal.notna())].copy()
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


