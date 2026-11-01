"""Own scoring-rate inputs for the NHL Points goals + assists model (P2.39).

One definition for training, freezing and live scoring. Every count comes from
official outcomes (the labels' source): the restored 2023-25 outcome frame,
the reconciliation packages for 2026 onward, and the NHL stats API season
summary for 2022-23.

For a target player-game in season s on date d:
- prior: the player's full season s-1 points, goals, assists and games
  (``prior_season_totals``), and season s-1 league per-game rates;
- current: his official points, goals, assists and games in season s before d;
- rate = (prior + current + K x league rate) / (prior games + current games + K),
  K = 30 games, for points (``own_rate_k30``), goals (``goal_rate``) and
  assists (``assist_rate``); ``own_games`` = prior games + current games.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

K = 30
FEATURES = ["own_rate_k30", "own_games", "goal_rate", "assist_rate"]
TOTAL_COLUMNS = ["player_id", "season", "games", "points", "goals", "assists"]


def season_totals(outcomes: pd.DataFrame) -> pd.DataFrame:
    """Full-season official totals per player (outcomes: player_id, game_id, season, goals, assists)."""
    o = outcomes.copy()
    o[["goals", "assists"]] = o[["goals", "assists"]].apply(pd.to_numeric, errors="coerce").fillna(0)
    t = o.groupby(["player_id", "season"]).agg(games=("game_id", "nunique"), goals=("goals", "sum"),
                                                assists=("assists", "sum")).reset_index()
    t["points"] = t.goals + t.assists
    return t[TOTAL_COLUMNS]


def league_rates(totals: pd.DataFrame) -> pd.DataFrame:
    """Per-game league rates by season (rows: season; columns points, goals, assists)."""
    g = totals.groupby("season")[["games", "points", "goals", "assists"]].sum()
    return pd.DataFrame({c: g[c] / g.games for c in ("points", "goals", "assists")})


def current_season(history: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    """Official counts in the target's season strictly before its date.

    history: player_id, game_id, season, game_date, goals, assists (official outcomes).
    targets: player_id, game_id, season, game_date."""
    h = history[["player_id", "game_id", "season", "game_date", "goals", "assists"]].copy()
    h["game_date"] = pd.to_datetime(h.game_date).dt.normalize()
    h[["goals", "assists"]] = h[["goals", "assists"]].apply(pd.to_numeric, errors="coerce").fillna(0)
    daily = (h.groupby(["player_id", "season", "game_date"])
             .agg(g=("game_id", "nunique"), go=("goals", "sum"), a=("assists", "sum")).reset_index()
             .sort_values(["player_id", "season", "game_date"]))
    by = daily.groupby(["player_id", "season"], sort=False)
    for c in ("g", "go", "a"):
        daily[f"cum_{c}"] = by[c].cumsum()
    t = targets[["player_id", "game_id", "season", "game_date"]].copy()
    t["game_date"] = pd.to_datetime(t.game_date).dt.normalize()
    # Last cumulative row on a date strictly before the target date (same player and season).
    t["_d"] = t.game_date - pd.Timedelta(days=1)
    t = t.sort_values("_d")
    daily = daily.sort_values("game_date")
    m = pd.merge_asof(t, daily[["player_id", "season", "game_date", "cum_g", "cum_go", "cum_a"]].rename(
        columns={"game_date": "_d"}), on="_d", by=["player_id", "season"], direction="backward")
    m[["cum_g", "cum_go", "cum_a"]] = m[["cum_g", "cum_go", "cum_a"]].fillna(0)
    return m.rename(columns={"cum_g": "cur_games", "cum_go": "cur_goals", "cum_a": "cur_assists"})[
        ["player_id", "game_id", "cur_games", "cur_goals", "cur_assists"]]


def add_features(targets: pd.DataFrame, history: pd.DataFrame, prior_totals: pd.DataFrame,
                 league: pd.DataFrame) -> pd.DataFrame:
    """targets: player_id, game_id, season, game_date (other columns kept).
    prior_totals: TOTAL_COLUMNS for the seasons before the targets'; league: ``league_rates``."""
    if targets.duplicated(["player_id", "game_id"]).any():
        raise ValueError("POINTS_COMPONENT_TARGET_DUPLICATE")
    cur = current_season(history, targets)
    prior = prior_totals.assign(season=prior_totals.season + 1).rename(
        columns={"games": "prior_games", "points": "prior_points", "goals": "prior_goals", "assists": "prior_assists"})
    f = targets.merge(cur, on=["player_id", "game_id"], how="left", validate="one_to_one").merge(
        prior[["player_id", "season", "prior_games", "prior_points", "prior_goals", "prior_assists"]],
        on=["player_id", "season"], how="left", validate="many_to_one")
    f[["prior_games", "prior_points", "prior_goals", "prior_assists"]] = f[
        ["prior_games", "prior_points", "prior_goals", "prior_assists"]].fillna(0)
    seasons = f.season.astype(int) - 1
    if not set(seasons).issubset(set(league.index.astype(int))):
        raise ValueError(f"POINTS_COMPONENT_LEAGUE_RATE_MISSING:{sorted(set(seasons) - set(league.index.astype(int)))}")
    games = f.prior_games + f.cur_games
    f["own_games"] = games
    for name, prior_col, cur_col, rate in (("own_rate_k30", "prior_points", None, "points"),
                                           ("goal_rate", "prior_goals", "cur_goals", "goals"),
                                           ("assist_rate", "prior_assists", "cur_assists", "assists")):
        current = (f.cur_goals + f.cur_assists) if cur_col is None else f[cur_col]
        mu = seasons.map(league[rate])
        f[name] = (f[prior_col] + current + K * mu) / (games + K)
    return f.drop(columns=["cur_games", "cur_goals", "cur_assists", "prior_games", "prior_points",
                           "prior_goals", "prior_assists"])
