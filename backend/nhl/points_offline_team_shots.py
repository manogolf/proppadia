"""Small offline reference builder for NHL Points team-shot history.

This code is isolated from the production feature exporter and daily wiring.
Input rows must be independently sourced official team-game SOG records.
"""
from __future__ import annotations

from bisect import bisect_left
from collections.abc import Mapping

import pandas as pd


REQUIRED_TEAM_GAME_COLUMNS = {"team_id", "game_id", "game_date", "team_sog"}
REQUIRED_SLATE_COLUMNS = {"team_id", "game_id", "player_id", "game_date"}


def reconstruct_team_shots(
    team_games: pd.DataFrame,
    slate: pd.DataFrame,
    *,
    team_aliases: Mapping[int, int] | None = None,
    source_complete: bool = True,
) -> pd.DataFrame:
    """Reconstruct strict-prior, 120-day, latest-ten team SOG averages.

    ``source_complete`` is evidence supplied by the caller about schedule/source
    coverage; a missing row is never inferred to be a zero-SOG game. Genuine
    zero-SOG observations remain valid denominator rows. Alias mapping is only
    applied when explicitly supplied and maps source team IDs to canonical IDs.
    """
    if not REQUIRED_TEAM_GAME_COLUMNS.issubset(team_games.columns):
        raise ValueError("TEAM_GAME_SCHEMA_MISSING")
    if not REQUIRED_SLATE_COLUMNS.issubset(slate.columns):
        raise ValueError("SLATE_SCHEMA_MISSING")

    games = team_games[list(REQUIRED_TEAM_GAME_COLUMNS)].copy()
    targets = slate[list(REQUIRED_SLATE_COLUMNS)].copy()
    for frame in (games, targets):
        for col in ("team_id", "game_id"):
            frame[col] = pd.to_numeric(frame[col], errors="raise").astype("int64")
        frame["game_date"] = pd.to_datetime(frame.game_date, errors="raise").dt.normalize()
    if games.duplicated(["team_id", "game_id"]).any():
        raise ValueError("DUPLICATE_TEAM_GAME_SOURCE")
    if targets.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_TARGET_PLAYER_GAME")
    games["team_sog"] = pd.to_numeric(games.team_sog, errors="coerce")
    if games.team_sog.isna().any():
        raise ValueError("MISSING_OFFICIAL_TEAM_SOG")
    if (games.team_sog < 0).any():
        raise ValueError("NEGATIVE_OFFICIAL_TEAM_SOG")
    aliases = {int(k): int(v) for k, v in (team_aliases or {}).items()}
    games["canonical_team_id"] = games.team_id.map(lambda value: aliases.get(int(value), int(value)))
    # Many players share a physical team and target date. Build the team/date
    # reference once and map it back, avoiding a full source scan per player.
    unique_targets = targets[["team_id", "game_date"]].drop_duplicates()
    reference: dict[tuple[int, pd.Timestamp], dict] = {}
    sources = {
        int(team_id): group.sort_values(["game_date", "game_id"], kind="mergesort").reset_index(drop=True)
        for team_id, group in games.groupby("canonical_team_id", sort=False)
    }
    for team_id, group in unique_targets.groupby("team_id", sort=False):
        source = sources.get(int(team_id))
        if source is None:
            source = games.iloc[0:0]
        source_dates = source.game_date.tolist()
        for day in group.game_date:
            upper = bisect_left(source_dates, day)  # strict-prior: excludes target date
            lower = bisect_left(source_dates, day - pd.Timedelta(days=120))
            prior = source.iloc[max(lower, upper - 10):upper]
            reference[(int(team_id), day)] = {
                "prior_game_ids": ";".join(str(int(v)) for v in prior.game_id),
                "prior_game_dates": ";".join(v.date().isoformat() for v in prior.game_date),
                "prior_team_sog_values": ";".join(format(float(v), ".17g") for v in prior.team_sog),
                "valid_prior_game_count": int(len(prior)),
                "team_d10_sf_per_game": float(prior.team_sog.mean()) if len(prior) else 0.0,
            }
    rows = []
    for target in targets.itertuples(index=False):
        values = reference[(int(target.team_id), target.game_date)]
        rows.append({
            "game_id": int(target.game_id),
            "player_id": int(target.player_id),
            "team_id": int(target.team_id),
            "game_date": target.game_date.date().isoformat(),
            **values,
            "source_supported": bool(source_complete),
            "evidence_source": "NHL_OFFICIAL_GAMECENTER_TEAM_BOXSCORE",
        })
    return pd.DataFrame(rows)
