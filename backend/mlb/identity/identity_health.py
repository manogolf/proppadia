from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


PLAYER_ID_COLS = ("player_id", "canonical_player_id", "mlb_player_id")
GAME_ID_COLS = ("game_id", "canonical_game_id", "mlb_game_id")
TEAM_COLS = ("team", "canonical_team", "team_code", "home_team_code", "away_team_code", "pitcher_team", "offense_team")
EVENT_ID_COLS = ("event_id", "provider_event_id", "oddsapi_event_id")
FALLBACK_COLS = ("fallback_used", "identity_fallback_used", "identity_method", "identity_status", "join_method", "match_method")
AMBIGUOUS_COLS = ("identity_status", "ambiguity_reason", "ambiguous_reason", "unavailable_reason")


@dataclass(frozen=True)
class IdentityHealthSummary:
    artifact: str
    path: str
    rows: int
    rows_using_ids: int
    rows_using_fallback: int
    rows_ambiguous: int
    rows_unresolved: int
    player_id_coverage_pct: float
    game_id_coverage_pct: float
    canonical_team_coverage_pct: float
    provider_event_id_coverage_pct: float
    identity_status: str


def _matching_cols(df: pd.DataFrame, options: tuple[str, ...]) -> list[str]:
    lower = {str(c).lower(): str(c) for c in df.columns}
    return [lower[item.lower()] for item in options if item.lower() in lower]


def _present_any(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    if df.empty or not cols:
        return pd.Series(False, index=df.index)
    flags = pd.Series(False, index=df.index)
    for col in cols:
        vals = df[col]
        flags = flags | vals.notna() & (vals.astype(str).str.strip() != "")
    return flags


def _text_contains_any(df: pd.DataFrame, cols: list[str], patterns: tuple[str, ...]) -> pd.Series:
    if df.empty or not cols:
        return pd.Series(False, index=df.index)
    flags = pd.Series(False, index=df.index)
    pattern = "|".join(patterns)
    for col in cols:
        vals = df[col].astype(str).str.lower()
        flags = flags | vals.str.contains(pattern, regex=True, na=False)
    return flags


def summarize_dataframe(artifact: str, path: str, df: pd.DataFrame) -> IdentityHealthSummary:
    rows = len(df)
    player_present = _present_any(df, _matching_cols(df, PLAYER_ID_COLS))
    game_present = _present_any(df, _matching_cols(df, GAME_ID_COLS))
    team_present = _present_any(df, _matching_cols(df, TEAM_COLS))
    event_present = _present_any(df, _matching_cols(df, EVENT_ID_COLS))
    fallback = _text_contains_any(df, _matching_cols(df, FALLBACK_COLS), ("fallback", "name", "alias", "normalized"))
    ambiguous = _text_contains_any(df, _matching_cols(df, AMBIGUOUS_COLS), ("ambiguous", "multiple", "collision"))
    unresolved = _text_contains_any(df, _matching_cols(df, AMBIGUOUS_COLS), ("unresolved", "missing", "not_found", "unmapped", "unknown"))
    rows_using_ids = int((player_present & game_present).sum())
    status = "pass"
    if rows and rows_using_ids == 0 and int(event_present.sum()) == 0:
        status = "warn"
    if rows and int(unresolved.sum()) > 0:
        status = "warn"
    return IdentityHealthSummary(
        artifact=artifact,
        path=path,
        rows=rows,
        rows_using_ids=rows_using_ids,
        rows_using_fallback=int(fallback.sum()),
        rows_ambiguous=int(ambiguous.sum()),
        rows_unresolved=int(unresolved.sum()),
        player_id_coverage_pct=round(float(player_present.mean() * 100) if rows else 0.0, 2),
        game_id_coverage_pct=round(float(game_present.mean() * 100) if rows else 0.0, 2),
        canonical_team_coverage_pct=round(float(team_present.mean() * 100) if rows else 0.0, 2),
        provider_event_id_coverage_pct=round(float(event_present.mean() * 100) if rows else 0.0, 2),
        identity_status=status,
    )


def summary_to_dict(summary: IdentityHealthSummary) -> dict[str, Any]:
    return {
        "artifact": summary.artifact,
        "path": summary.path,
        "rows": summary.rows,
        "rows_using_ids": summary.rows_using_ids,
        "rows_using_fallback": summary.rows_using_fallback,
        "rows_ambiguous": summary.rows_ambiguous,
        "rows_unresolved": summary.rows_unresolved,
        "player_id_coverage_pct": summary.player_id_coverage_pct,
        "game_id_coverage_pct": summary.game_id_coverage_pct,
        "canonical_team_coverage_pct": summary.canonical_team_coverage_pct,
        "provider_event_id_coverage_pct": summary.provider_event_id_coverage_pct,
        "identity_status": summary.identity_status,
    }
