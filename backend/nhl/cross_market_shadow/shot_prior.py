"""Frozen early-season shot-differential prior for the Puck Line V2 shadow."""
from __future__ import annotations

import pandas as pd

POLICY_VERSION = "NHL_PUCK_LINE_SHOT_PRIOR_POLICY_V1"
CHALLENGER_NAME = "NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2"
FRANCHISE = {"ARI": "UTA", "UTA": "UTA"}


def prior_weight(games: int) -> float:
    if games <= 3:
        return 1.0
    if games <= 5:
        return 0.75
    if games <= 10:
        return 0.50
    return 0.0


def _code(value: object) -> str:
    value = str(value).strip().upper()
    return FRANCHISE.get(value, value)


def build_shot_prior_challenger(
    schedule: pd.DataFrame,
    history: pd.DataFrame,
    v1: pd.DataFrame,
    prior_games: pd.DataFrame,
    *,
    prior_source_sha256: str,
    prior_outcome_sha256: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return V2 scoring rows and auditable per-game/per-side prior provenance.

    `prior_games` must already be loaded from the repository's hash-pinned,
    qualified preceding-season source. The routine independently limits it to
    regular-season final records and requires complete shot totals.
    """
    prior = prior_games.copy()
    prior["game_type_code"] = pd.to_numeric(prior.game_type, errors="coerce")
    prior = prior[prior.game_type_code.eq(2) & prior.database_game_status.astype(str).str.lower().isin({"final", "off", "completed"})]
    prior["home_shots"] = pd.to_numeric(prior.home_shots, errors="coerce")
    prior["away_shots"] = pd.to_numeric(prior.away_shots, errors="coerce")
    prior = prior.dropna(subset=["home_shots", "away_shots"])
    states: dict[str, float] = {}
    for code, group in prior.groupby(prior.home_team_code.map(_code)):
        states[code] = float((group.home_shots - group.away_shots).mean())
    for code, group in prior.groupby(prior.away_team_code.map(_code)):
        away_state = float((group.away_shots - group.home_shots).mean())
        n = int((prior.home_team_code.map(_code).eq(code)).sum()) + int((prior.away_team_code.map(_code).eq(code)).sum())
        # Combine side means by game count (each completed game is one observation).
        home_n = int(prior.home_team_code.map(_code).eq(code).sum())
        away_n = int(prior.away_team_code.map(_code).eq(code).sum())
        states[code] = (states.get(code, 0.0) * home_n + away_state * away_n) / max(n, 1)

    hist = history.copy()
    hist["game_type_code"] = pd.to_numeric(hist.game_type_code, errors="coerce")
    hist["scheduled_start_time_utc"] = pd.to_datetime(hist.scheduled_start_time_utc, utc=True, errors="coerce")
    hist["final_home_shots"] = pd.to_numeric(hist.final_home_shots, errors="coerce")
    hist["final_away_shots"] = pd.to_numeric(hist.final_away_shots, errors="coerce")
    eligible = hist[
        hist.canonical_season.eq(2026) & hist.game_type_code.eq(2)
        & hist.game_status.astype(str).str.upper().isin({"FINAL", "OFF", "COMPLETED"})
        & hist.final_home_shots.notna() & hist.final_away_shots.notna()
    ]
    v1_rows = v1.set_index("game_id", drop=False)
    candidates, audits = [], []
    for target in schedule.itertuples(index=False):
        target_start = pd.to_datetime(target.scheduled_start_time_utc, utc=True)
        side_records = {}
        for side in ("home", "away"):
            team_id = int(getattr(target, f"{side}_team_id"))
            team_code = _code(getattr(target, f"{side}_team"))
            subset = eligible[
                (eligible.home_team_id.eq(team_id) | eligible.away_team_id.eq(team_id))
                & eligible.scheduled_start_time_utc.lt(target_start)
            ].copy().sort_values(["scheduled_start_time_utc", "game_id"])
            team_shots = subset.apply(
                lambda r: (r.final_home_shots - r.final_away_shots)
                if int(r.home_team_id) == team_id else (r.final_away_shots - r.final_home_shots), axis=1
            )
            current_n = len(subset)
            current = float(team_shots.mean()) if current_n else None
            prior_state = states.get(team_code)
            weight = prior_weight(current_n)
            v1_value = float(v1_rows.loc[int(target.game_id), "diff_std_shot_diff_pg"])
            if prior_state is None:
                blended = v1_value
                reason = "PRIOR_UNAVAILABLE_V1_PRESERVED"
            elif current is None:
                blended, reason = prior_state, "CURRENT_UNAVAILABLE_PRIOR_USED"
            else:
                blended = weight * prior_state + (1 - weight) * current
                reason = "PRIOR_WEIGHTED" if weight else "PRIOR_WEIGHT_ZERO"
            side_records[side] = {"team_id": team_id, "team_code": team_code,
                "prior": prior_state, "current": current, "count": current_n,
                "weight": weight, "blended": blended, "reason": reason}
        home, away = side_records["home"], side_records["away"]
        v1_diff = float(v1_rows.loc[int(target.game_id), "diff_std_shot_diff_pg"])
        candidate_diff = (
            v1_diff if home["prior"] is None or away["prior"] is None
            else home["blended"] - away["blended"]
        )
        source_row = v1_rows.loc[int(target.game_id)].copy()
        source_row["diff_std_shot_diff_pg"] = candidate_diff
        candidates.append(source_row)
        audits.append({
            "canonical_season": 2026, "slate_date": target.slate_date, "game_id": int(target.game_id),
            "scheduled_start_time_utc": target_start.isoformat(),
            "home_team_id": home["team_id"], "away_team_id": away["team_id"],
            "prior_season": 2025,
            "prior_home_full_season_shot_diff_pg": home["prior"],
            "prior_away_full_season_shot_diff_pg": away["prior"],
            "current_home_shot_diff_pg": home["current"], "current_away_shot_diff_pg": away["current"],
            "current_home_games": home["count"], "current_away_games": away["count"],
            "home_prior_weight": home["weight"], "away_prior_weight": away["weight"],
            "blended_home_shot_diff_pg": home["blended"], "blended_away_shot_diff_pg": away["blended"],
            "v1_diff_std_shot_diff_pg": v1_diff,
            "diff_std_shot_diff_pg": candidate_diff,
            "fallback_reason": ";".join(sorted({home["reason"], away["reason"]})),
            "feature_policy_version": POLICY_VERSION, "prior_source_sha256": prior_source_sha256,
            "prior_outcome_source_sha256": prior_outcome_sha256,
            "prior_source_artifact": "season_2025_team_game_source.csv",
            "prior_outcome_source_artifact": "season_2025_outcome_spine.parquet",
            "prior_qualified_regular_season_games": len(prior),
        })
    return pd.DataFrame(candidates).reset_index(drop=True), pd.DataFrame(audits)
