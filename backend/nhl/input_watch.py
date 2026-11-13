"""Daily input watch: surface model-input problems before anyone asks (2026-10-28).

Runs at the end of each daily run, after production SOG, Points HGB and the
odds observation. Read-only: no database access, no provider requests, no
change to any prediction. It writes
``artifacts/operational/nhl/input_watch/slate_date=<d>/run_id=<r>/`` with
``input_watch.json`` and ``input_watch.md``.

1. Input sanity: impossible or substituted values in each player's inputs,
   and Points inputs outside the range the frozen model saw in training
   (``backend/nhl/config/points_hgb_training_feature_ranges.json``).
2. Disagreement explainer: the day's largest gaps between our probability on
   the selected side and the median same-book no-vig market probability, on
   two-sided lines quoted before the watch ran. Quote matching reuses
   ``export_nhl_all_candidate_evaluation.load_retained_quotes``. SOG inputs are
   checked against ``backend/nhl/config/sog_hgb_training_feature_ranges.json``
   (from the frozen model's rebuilt training frame, hash-verified). Each pick
   lists the inputs that explain it: rule reasons from the 2026-10-28 study
   (NHL Backlog P2.29, P2.37) and inputs unusual against players the market
   prices the same.
3. Source health: the prior date's shot-attempt source diagnostic.
4. Role changes (2026-11-06): tonight's players whose ice time or power-play
   time over the last 10 games differs from last season's by at least
   ``ROLE_TOI_DELTA`` / ``ROLE_PP_DELTA`` minutes, with points per game for
   this season and the two before and the model's Points 0.5 and SOG 1.5
   probabilities next to the market's. The Points rate input weighs last
   season's games like this season's, so a player whose role changed can sit
   far from a market that has already reacted. Last season's usage comes from
   ``PRIOR_USAGE_PATH`` (built once per season by
   ``scripts/build_nhl_prior_season_usage.py``); points per game from the
   production Points model's frozen ``prior_season_totals``.
5. Hot streaks (2026-11-12, watch-only, NHL Backlog P2.49): tonight's players who
   cleared SOG 1.5 or Points 0.5 in each of their last ``HOT_GAMES`` played
   games this season (from saved reconciliation outcomes), with the model's and
   the market's P(over). On 10-26..11-11 these players beat both the market and
   the models; each watch grades the prior slate's tags to keep measuring it.
6. Goalies (2026-11-13, NHL Backlog P2.7): each game's projected starters (G1 in
   the newest NHL.com lineup capture, P2.47) against each team's starts leader
   this season (official starts in saved reconciliations), next to the
   production moneyline favorite from the newest complete cross-market capture
   and back-to-back flags. The moneyline model has no goalie input; a backup
   on its favorite reads "pass", on its underdog "model side stronger". The
   watch runs before that run's lineup and cross-market steps, so it reads the
   previous captures.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.nhl.scripts import export_nhl_all_candidate_evaluation as candidate_export

ROOT = Path(__file__).resolve().parents[2]
POINTS_RANGES_PATH = ROOT / "backend/nhl/config/points_hgb_training_feature_ranges.json"
SOG_RANGES_PATH = ROOT / "backend/nhl/config/sog_hgb_training_feature_ranges.json"
OUTPUT_ROOT = ROOT / "artifacts/operational/nhl/input_watch"
SCHEMA = "NHL_INPUT_WATCH_V1"
TOP_N = 10
DRIFT_SHARE = 0.10       # flag a feature when this share of players is outside training p01-p99
IMPUTED_SHARE = 0.02     # flag substituted attempts above this share of players
PEER_WINDOW = 0.05       # peers: same line, market P(over) within +/- this
PEER_MIN = 8
PEER_Z = 1.5
PRIOR_USAGE_PATH = OUTPUT_ROOT / "prior_season_usage.csv"
ROLE_TOI_DELTA = 2.5     # minutes per game, last 10 games vs last season
ROLE_PP_DELTA = 1.0      # power-play minutes per game
ROLE_MIN_GAMES = 5       # games this season before a role change is read
ROLE_MIN_PRIOR_GAMES = 20
ROLE_MARKET_GAP = 0.05   # model vs market P(over) beyond this is read as a gap
ROLE_LINES = {"points": 0.5, "sog": 1.5}
POINTS_FEATURES = [
    "is_home", "d10_sog_per60", "attempts_d10_per60", "player_points_last10",
    "current_season_points_prior", "current_season_games_prior", "mean_toi_last10",
    "mean_pp_toi_last10", "team_d10_sf_per_game", "last10_team_sog_share",
]
POINTS_LINES = ((0.5, "prob_over_0_5"), (1.5, "prob_over_1_5"), (2.5, "prob_over_2_5"))
SOG_LINES = ((1.5, "p_over_1_5"), (2.5, "p_over_2_5"), (3.5, "p_over_3_5"))
FEATURE_LABELS = {
    "d10_sog_per60": "shots/60", "attempts_d10_per60": "attempts/60",
    "player_points_last10": "points/game last 10", "current_season_games_prior": "games played",
    "current_season_points_prior": "season points", "mean_toi_last10": "TOI",
    "mean_pp_toi_last10": "PP TOI", "team_d10_sf_per_game": "team shots/game",
    "last10_team_sog_share": "team shot share", "season_ppg": "season points/game",
    "feature_sog60_10": "shots/60 (10)", "feature_sog60_season": "shots/60 season",
    "feature_toi_avg_10": "TOI (10)", "feature_pp_toi_avg_10": "PP TOI (10)",
    "feature_att60_10": "attempts/60 (10)", "feature_games_season": "games played",
    "feature_team_sf_10": "team shots/game", "feature_opp_sa_10": "opponent shots against",
}


def _check(name: str, mask: pd.Series, frame: pd.DataFrame, *, flag: bool, detail: str) -> dict[str, Any]:
    hits = frame[mask.fillna(False)]
    examples = hits.get("player_name", hits.player_id.astype(str)).astype(str).head(5).tolist()
    return {"check": name, "count": int(len(hits)), "flag": bool(flag and len(hits)),
            "detail": detail, "examples": examples}


def points_sanity(features: pd.DataFrame, ranges: dict[str, Any]) -> list[dict[str, Any]]:
    f = features.copy()
    for column in POINTS_FEATURES:
        f[column] = pd.to_numeric(f[column], errors="coerce")
    n = max(len(f), 1)
    rates = ["d10_sog_per60", "attempts_d10_per60", "player_points_last10", "mean_toi_last10"]
    per_game_share = (f.d10_sog_per60 * f.mean_toi_last10 / 60) / f.team_d10_sf_per_game.replace(0, np.nan)
    imputed = f.attempts_d10_per60.isna() & f.d10_sog_per60.notna()
    checks = [
        _check("ATTEMPTS_BELOW_SOG", f.attempts_d10_per60 < f.d10_sog_per60, f, flag=True,
               detail="attempt rate below shot rate (impossible)"),
        _check("TEAM_SHOTS_IMPLAUSIBLE", ~f.team_d10_sf_per_game.between(15, 45), f, flag=True,
               detail="team shots per game outside 15-45"),
        _check("SHOT_SHARE_INVALID", ~f.last10_team_sog_share.between(0, 0.5), f, flag=True,
               detail="team shot share outside 0-0.5"),
        _check("PP_TOI_ABOVE_TOI", f.mean_pp_toi_last10 > f.mean_toi_last10, f, flag=True,
               detail="power-play TOI above total TOI"),
        _check("ATTEMPTS_UNKNOWN_IMPUTED", imputed, f, flag=imputed.sum() / n > IMPUTED_SHARE,
               detail="attempt rate unknown; scored from shot rate x 1.993 (P2.37)"),
        _check("NO_HISTORY_ZERO_FILLED", f[rates].isna().all(axis=1), f, flag=False,
               detail="no recent games; frozen recipe fills zero (as in training)"),
        _check("SHOT_SHARE_DEFLATED_BY_MISSED_GAMES",
               f.last10_team_sog_share < 0.75 * per_game_share, f, flag=False,
               detail="share counts team games the player missed"),
    ]
    for column in POINTS_FEATURES:
        bounds = (ranges.get("features") or {}).get(column)
        if not bounds or column == "is_home":
            continue
        outside = f[column].notna() & ~f[column].between(bounds["p01"], bounds["p99"])
        checks.append(_check(f"OUT_OF_TRAINING_RANGE:{column}", outside, f,
                             flag=outside.sum() / n > DRIFT_SHARE,
                             detail=f"outside training p01-p99 [{bounds['p01']:g}, {bounds['p99']:g}]"))
    return checks


# Constant for a whole season (last season's league rate by position): a new
# season sits outside the two training values by construction, so it is
# reported, not flagged. Trees score it as the nearest training value.
SEASON_CONSTANT_INPUTS = {"feature_league_prior"}


def _sog_out_of_range(ranges: dict[str, Any] | None) -> dict[str, dict[str, float]]:
    """Live SOG column -> training p01/p99; binary inputs are skipped."""
    return {f"feature_{name}": bounds for name, bounds in ((ranges or {}).get("features") or {}).items()
            if name not in {"is_home", "is_defense"}}


def sog_sanity(features: pd.DataFrame, ranges: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    f = features.copy()
    n = max(len(f), 1)
    num = lambda c: pd.to_numeric(f[c], errors="coerce")  # noqa: E731
    checks = [
        _check("NEGATIVE_RATE", (num("feature_sog60_10") < 0) | (num("feature_att60_10") < 0), f, flag=True,
               detail="negative shot or attempt rate"),
        _check("ATTEMPTS_BELOW_SOG", num("feature_att60_10") < num("feature_sog60_10"), f, flag=True,
               detail="attempt rate below shot rate (impossible)"),
        _check("TEAM_SHOTS_IMPLAUSIBLE", ~num("feature_team_sf_10").between(15, 45), f, flag=True,
               detail="team shots per game outside 15-45"),
        _check("PP_TOI_ABOVE_TOI", num("feature_pp_toi_avg_10") > num("feature_toi_avg_10"), f, flag=True,
               detail="power-play TOI above total TOI"),
    ]
    for column in [c for c in f.columns if c.startswith("feature_")]:
        missing = num(column).isna()
        checks.append(_check(f"MISSING:{column}", missing, f, flag=missing.sum() / n > DRIFT_SHARE,
                             detail="input missing"))
    for column, bounds in _sog_out_of_range(ranges).items():
        if column not in f:
            continue
        outside = num(column).notna() & ~num(column).between(bounds["p01"], bounds["p99"])
        constant = column in SEASON_CONSTANT_INPUTS
        checks.append(_check(f"OUT_OF_TRAINING_RANGE:{column}", outside, f,
                             flag=not constant and outside.sum() / n > DRIFT_SHARE,
                             detail=(f"outside training p01-p99 [{bounds['p01']:g}, {bounds['p99']:g}]"
                                     + ("; season-level constant, reported not flagged" if constant else ""))))
    return checks


def _market(quotes: list[dict[str, Any]]) -> pd.DataFrame:
    """Median same-book no-vig P(selected) per candidate on two-sided lines."""
    if not quotes:
        return pd.DataFrame(columns=["candidate_key", "market_p", "books", "selected_price"])
    q = pd.DataFrame(quotes)
    q["imp"] = pd.to_numeric(q.implied_probability, errors="coerce")
    q["is_selected"] = q.quote_side.eq(q.selected_side)
    wide = q.pivot_table(index=["candidate_key", "sportsbook_key"], columns="is_selected",
                         values=["imp", "american_price"], aggfunc="first").dropna()
    if wide.empty:
        return pd.DataFrame(columns=["candidate_key", "market_p", "books", "selected_price"])
    wide.columns = [f"{a}_{'sel' if b else 'opp'}" for a, b in wide.columns]
    wide["nv"] = wide.imp_sel / (wide.imp_sel + wide.imp_opp)
    return wide.groupby("candidate_key").agg(
        market_p=("nv", "median"), books=("nv", "size"),
        selected_price=("american_price_sel", "median")).reset_index()


def _candidates(rows: pd.DataFrame, *, market_key: str, lines, slate_date: str,
                starts: dict[int, str], now_utc: str, run_id: str) -> list[dict[str, Any]]:
    out = []
    for row in rows.itertuples(index=False):
        for line, column in lines:
            p_over = float(getattr(row, column))
            side = "OVER" if p_over >= 0.5 else "UNDER"
            candidate = {
                "canonical_season": 2026, "slate_date": slate_date, "game_id": str(int(row.game_id)),
                "player_id": str(int(row.player_id)), "goalie_id": "", "market_key": market_key,
                "market_line": str(float(line)), "selected_side": side, "model_role": "PRODUCTION",
                "model_family": market_key, "model_version": "input_watch",
                "prediction_identity": f"{run_id}:{int(row.game_id)}:{int(row.player_id)}:{line}",
                "probability_over": p_over, "probability_under": 1 - p_over,
                # Diagnostic: quotes observed before the watch ran count.
                "prediction_timestamp_utc": now_utc,
                "scheduled_start_utc": starts.get(int(row.game_id), ""),
                "parent_run_id": run_id,
            }
            candidate["candidate_key"] = candidate_export.candidate_key(candidate)
            out.append(candidate)
    return out


def _peer_reasons(row: pd.Series, frame: pd.DataFrame, columns: list[str]) -> list[str]:
    peers = frame[(frame.line == row.line) & (frame.market_p_over - row.market_p_over).abs().le(PEER_WINDOW)
                  & frame.player_id.ne(row.player_id)]
    if len(peers) < PEER_MIN:
        return []
    found = []
    for column in columns:
        value, values = row.get(column), pd.to_numeric(peers[column], errors="coerce").dropna()
        if pd.isna(value) or len(values) < PEER_MIN or values.std() == 0:
            continue
        z = (float(value) - values.mean()) / values.std()
        if abs(z) >= PEER_Z:
            found.append((abs(z), f"{FEATURE_LABELS.get(column, column)} {'high' if z > 0 else 'low'} "
                                  f"vs market peers ({float(value):.3g} vs {values.mean():.3g})"))
    return [text for _, text in sorted(found, reverse=True)[:2]]


def _points_reasons(row: pd.Series, slate: pd.DataFrame, ranges: dict[str, Any]) -> list[str]:
    reasons = []
    if pd.isna(row.attempts_d10_per60) and pd.notna(row.d10_sog_per60):
        reasons.append("attempts unknown (scored from shot rate)")
    if row.current_season_games_prior < 5:
        reasons.append(f"only {int(row.current_season_games_prior)} games played")
    per_game = (row.d10_sog_per60 * row.mean_toi_last10 / 60) / row.team_d10_sf_per_game \
        if row.team_d10_sf_per_game else np.nan
    if pd.notna(per_game) and row.last10_team_sog_share < 0.75 * per_game:
        reasons.append("shot share understated by missed games")
    if row.get("is_defense") == 1:
        reasons.append("defenseman (position is not a model input)")
    if row.season_ppg >= 1.5:
        reasons.append(f"{row.season_ppg:.2f} points/game: near the model's ceiling")
    if row.season_ppg >= 0.7 and row.mean_toi_last10 < slate.mean_toi_last10.median() - 3:
        reasons.append(f"low TOI ({row.mean_toi_last10:.1f}) for his scoring")
    if row.season_ppg >= 0.6 and row.last10_team_sog_share < slate.last10_team_sog_share.quantile(0.33):
        reasons.append("scores without shooting (low shot share)")
    if row.selected_side == "OVER" and row.last10_team_sog_share >= 0.10:
        reasons.append("high shot share (model rewards shooters)")
    for column in POINTS_FEATURES:
        bounds = (ranges.get("features") or {}).get(column)
        value = row.get(column)
        if bounds and column != "is_home" and pd.notna(value) and not bounds["p01"] <= value <= bounds["p99"]:
            reasons.append(f"{FEATURE_LABELS.get(column, column)} {value:.3g} outside training range")
    return reasons


def season_of(slate_date: str) -> int:
    """NHL season by start year (September onward belongs to that year's season)."""
    year, month = int(slate_date[:4]), int(slate_date[5:7])
    return year if month >= 9 else year - 1


def prior_points_per_game() -> pd.DataFrame | None:
    """Points per game by player and season from the production Points model's frozen totals."""
    from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer
    contract = json.loads(scorer.CONTRACT_PATH.read_text())
    path = scorer.CONTRACT_PATH.parent / contract["prior_season_totals"]
    if not path.is_file():
        return None
    totals = pd.read_csv(path)
    totals["ppg"] = totals.points / totals.games.replace(0, np.nan)
    return totals[["player_id", "season", "games", "ppg"]]


def role_direction(toi_delta, pp_delta) -> np.ndarray:
    """UP/DOWN from the moves that pass their threshold; MIXED only when those disagree."""
    toi, pp = np.asarray(toi_delta, dtype=float), np.asarray(pp_delta, dtype=float)
    toi_move = np.where(np.abs(toi) >= ROLE_TOI_DELTA, np.sign(toi), 0)
    pp_move = np.where(np.abs(pp) >= ROLE_PP_DELTA, np.sign(pp), 0)
    up = (toi_move >= 0) & (pp_move >= 0)
    down = (toi_move <= 0) & (pp_move <= 0)
    return np.select([up, down], ["UP", "DOWN"], "MIXED")


def role_changes(points: pd.DataFrame, usage: pd.DataFrame, season: int,
                 totals: pd.DataFrame | None = None) -> pd.DataFrame:
    """Tonight's players whose last-10 ice time or PP time moved from last season's."""
    prior = usage[usage.season.eq(season - 1)].set_index("player_id")
    before = usage[usage.season.eq(season - 2)].set_index("player_id")
    f = points.copy()
    f["player_id"] = f.player_id.astype("int64")
    f = f[pd.to_numeric(f.current_season_games_prior, errors="coerce").ge(ROLE_MIN_GAMES)]
    f = f[f.player_id.isin(prior.index[prior.games.ge(ROLE_MIN_PRIOR_GAMES)])]
    if f.empty:
        return pd.DataFrame()
    f["toi_prior"] = f.player_id.map(prior.toi_per_game)
    f["toi_before"] = f.player_id.map(before.toi_per_game) if len(before) else np.nan
    f["toi_now"] = pd.to_numeric(f.mean_toi_last10, errors="coerce")
    f["pp_now"] = pd.to_numeric(f.mean_pp_toi_last10, errors="coerce").fillna(0.0)
    # League power-play time shifts between seasons (2026 runs ~30% above 2025);
    # scale last season's PP time by tonight's players' overall change so only
    # player-specific moves count.
    raw_pp_prior = f.player_id.map(prior.pp_toi_per_game).fillna(0.0)
    pp_scale = float(f.pp_now.mean() / raw_pp_prior.mean()) if raw_pp_prior.mean() > 0 else 1.0
    f["pp_prior"] = raw_pp_prior * pp_scale
    f["toi_delta"] = f.toi_now - f.toi_prior
    f["pp_delta"] = f.pp_now - f.pp_prior
    f = f[f.toi_delta.abs().ge(ROLE_TOI_DELTA) | f.pp_delta.abs().ge(ROLE_PP_DELTA)].copy()
    if f.empty:
        empty = pd.DataFrame()
        empty.attrs["pp_scale"] = pp_scale
        return empty
    f["direction"] = role_direction(f.toi_delta, f.pp_delta)
    f["ppg_now"] = f.current_season_points_prior / f.current_season_games_prior.replace(0, np.nan)
    for label, offset in (("ppg_prior", 1), ("ppg_before", 2)):
        f[label] = np.nan
        if totals is not None:
            rates = totals[totals.season.eq(season - offset)].set_index("player_id").ppg
            f[label] = f.player_id.map(rates)
    f["role_size"] = f.toi_delta.abs() / ROLE_TOI_DELTA + f.pp_delta.abs() / ROLE_PP_DELTA
    keep = ["game_id", "player_id", "player_name", "direction", "toi_before", "toi_prior", "toi_now",
            "toi_delta", "pp_prior", "pp_now", "pp_delta", "ppg_before", "ppg_prior", "ppg_now",
            "current_season_games_prior", "role_size"]
    if "player_name" not in f:
        f["player_name"] = f.player_id.astype(str)
    out = f[keep].sort_values("role_size", ascending=False).reset_index(drop=True)
    out.attrs["pp_scale"] = pp_scale
    return out


def role_read(direction: str, model_p_over: float | None, market_p_over: float | None) -> str:
    """Plain reading of a role change against the model-market gap on the Over."""
    if model_p_over is None or market_p_over is None or pd.isna(model_p_over) or pd.isna(market_p_over):
        return "no two-sided market"
    gap = model_p_over - market_p_over
    if abs(gap) < ROLE_MARKET_GAP:
        return "model and market agree"
    if direction == "MIXED":
        return f"model {'above' if gap > 0 else 'below'} market; mixed role change, check lines/PP unit"
    behind = (direction == "UP" and gap < 0) or (direction == "DOWN" and gap > 0)
    if behind:
        return "model likely behind the role change (market already moved)"
    return "model further along the role change than the market; check news"


def _role_reason(row: pd.Series, roles: pd.DataFrame | None) -> str | None:
    if roles is None or roles.empty:
        return None
    hit = roles[roles.player_id.eq(int(row.player_id))]
    if hit.empty:
        return None
    r = hit.iloc[0]
    return (f"role change vs last season: TOI {r.toi_prior:.1f} to {r.toi_now:.1f}, "
            f"PP {r.pp_prior:.1f} to {r.pp_now:.1f} (rate input leans on last season)")


def explain_disagreements(*, points: pd.DataFrame | None, sog: pd.DataFrame | None, slate_date: str,
                          run_id: str, operational_root: Path, now_utc: str,
                          ranges: dict[str, Any], sog_ranges: dict[str, Any] | None = None,
                          roles: pd.DataFrame | None = None) -> dict[str, Any]:
    starts: dict[int, str] = {}
    if points is not None and "game_start_utc" in points:
        starts = {int(g): str(s).replace(" ", "T") for g, s in zip(points.game_id, points.game_start_utc)}
    lanes: dict[str, tuple] = {}
    if points is not None and len(points):
        lanes["points"] = (points, "player_points", POINTS_LINES)
    if sog is not None and len(sog):
        lanes["sog"] = (sog, "shots_on_goal", SOG_LINES)
    candidates = []
    for frame, market_key, lines in lanes.values():
        candidates += _candidates(frame, market_key=market_key, lines=lines, slate_date=slate_date,
                                  starts=starts, now_utc=now_utc, run_id=run_id)
    quotes, _ = candidate_export.load_retained_quotes(operational_root, candidates, price_both_sides=True)
    market = _market(candidate_export.deduplicate_quotes(quotes))
    cand = pd.DataFrame(candidates).merge(market, on="candidate_key", how="inner")
    out: dict[str, Any] = {"two_sided_candidates": int(len(cand)), "lanes": {}, "role_market": [], "line_market": []}
    for lane, (frame, market_key, _lines) in lanes.items():
        part = cand[cand.market_key.eq(market_key)].copy()
        if part.empty:
            out["lanes"][lane] = []
            continue
        part["game_id"] = part.game_id.astype("int64")
        part["player_id"] = part.player_id.astype("int64")
        part["line"] = part.market_line.astype(float)
        part["model_p"] = np.where(part.selected_side.eq("OVER"), part.probability_over, part.probability_under)
        part["gap"] = part.model_p - part.market_p
        part["market_p_over"] = np.where(part.selected_side.eq("OVER"), part.market_p, 1 - part.market_p)
        frame = frame.copy()
        frame["game_id"] = frame.game_id.astype("int64")
        frame["player_id"] = frame.player_id.astype("int64")
        part = part.merge(frame, on=["game_id", "player_id"], how="left", suffixes=("", "_feature"))
        out["line_market"] += [{"lane": lane, "player_id": int(r.player_id),
                                "market_p_over": round(float(r.market_p_over), 4)}
                               for r in part[part.line.eq(ROLE_LINES[lane])].itertuples(index=False)]
        if roles is not None and not roles.empty:
            mine = part[part.player_id.isin(roles.player_id) & part.line.eq(ROLE_LINES[lane])]
            out["role_market"] += [{"lane": lane, "player_id": int(r.player_id), "line": float(r.line),
                                    "model_p_over": round(float(r.probability_over), 4),
                                    "market_p_over": round(float(r.market_p_over), 4)}
                                   for r in mine.itertuples(index=False)]
        if lane == "points":
            part["season_ppg"] = part.current_season_points_prior / part.current_season_games_prior.replace(0, np.nan)
            peer_columns = ["season_ppg", "player_points_last10", "mean_toi_last10", "mean_pp_toi_last10",
                            "d10_sog_per60", "attempts_d10_per60", "last10_team_sog_share",
                            "current_season_games_prior", "team_d10_sf_per_game"]
        else:
            peer_columns = ["feature_sog60_10", "feature_sog60_season", "feature_toi_avg_10",
                            "feature_pp_toi_avg_10", "feature_att60_10", "feature_games_season",
                            "feature_team_sf_10", "feature_opp_sa_10"]
        top = part.sort_values("gap", ascending=False).head(TOP_N)
        picks = []
        for _, row in top.iterrows():
            reasons = (_points_reasons(row, frame.assign(
                season_ppg=frame.current_season_points_prior / frame.current_season_games_prior.replace(0, np.nan)),
                ranges) if lane == "points" else [])
            if lane == "sog":
                if row.get("feature_games_season", 99) < 5:
                    reasons.append(f"only {int(row.feature_games_season)} games played")
                if row.get("feature_is_defense") == 1:
                    reasons.append("defenseman")
                missing = [FEATURE_LABELS.get(c, c) for c in peer_columns if pd.isna(row.get(c))]
                if missing:
                    reasons.append("missing: " + ", ".join(missing))
                for column, bounds in _sog_out_of_range(sog_ranges).items():
                    if column in SEASON_CONSTANT_INPUTS:
                        continue
                    value = pd.to_numeric(row.get(column), errors="coerce")
                    if pd.notna(value) and not bounds["p01"] <= value <= bounds["p99"]:
                        reasons.append(f"{FEATURE_LABELS.get(column, column.removeprefix('feature_'))} "
                                       f"{value:.3g} outside training range")
            role = _role_reason(row, roles)
            if role:
                reasons.insert(0, role)
            reasons += _peer_reasons(row, part, peer_columns)
            picks.append({
                "player": str(row.get("player_name") or row.player_id), "player_id": int(row.player_id),
                "game_id": int(row.game_id), "line": float(row.line), "side": row.selected_side,
                "model_p": round(float(row.model_p), 4), "market_p": round(float(row.market_p), 4),
                "gap": round(float(row.gap), 4), "books": int(row.books),
                "book_price": None if pd.isna(row.selected_price) else int(row.selected_price),
                "reasons": reasons or ["no input stands out; check the market (news, lineup)"],
            })
        out["lanes"][lane] = picks
    return out


def attempt_source_health(operational_root: Path, prior_date: str | None, run_id: str) -> dict[str, Any]:
    if not prior_date:
        return {"status": "NOT_APPLICABLE"}
    path = (operational_root / "shot_attempts_source_diagnostics" / f"slate_date={prior_date}"
            / f"run_id={run_id}" / "attempt_source_diagnostic.json")
    if not path.is_file():
        return {"status": "NO_DIAGNOSTIC_FOR_THIS_RUN", "prior_date": prior_date}
    diagnostic = json.loads(path.read_text())
    summary = diagnostic.get("source_summary") or {}
    failed = diagnostic.get("failed_source_responses") or {}
    return {"status": "READ", "prior_date": prior_date,
            "games_without_attempts": sorted(failed),
            "game_failure_reasons": {game: value.get("reason") for game, value in failed.items()},
            "unresolved_skaters": int(summary.get("pbp_unresolved_skaters", 0) or 0),
            "conflicts": int(summary.get("confirmed_discrepancy_count", 0) or 0),
            "flag": bool(failed)}


def _role_rows(roles: pd.DataFrame | None, market: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if roles is None or roles.empty:
        return []
    by_key = {(m["lane"], m["player_id"]): m for m in market}
    rows = []
    for r in roles.itertuples(index=False):
        row = {key: (None if pd.isna(value) else (round(float(value), 3) if isinstance(value, float) else value))
               for key, value in r._asdict().items()}
        row["player_id"], row["game_id"] = int(r.player_id), int(r.game_id)
        for lane in ("points", "sog"):
            m = by_key.get((lane, int(r.player_id))) or {}
            row[f"{lane}_model_p_over"] = m.get("model_p_over")
            row[f"{lane}_market_p_over"] = m.get("market_p_over")
        lead = "points" if row["points_market_p_over"] is not None else "sog"
        row["read"] = role_read(r.direction, row[f"{lead}_model_p_over"], row[f"{lead}_market_p_over"])
        row["read_from"] = lead
        rows.append(row)
    # Players the model looks behind on come first; no-market players last.
    order = {"model likely behind": 0, "model further": 1, "model below": 2, "model above": 2,
             "model and market": 3, "no two-sided": 4}
    rank = lambda read: next((v for k, v in order.items() if read.startswith(k)), 3)
    return sorted(rows, key=lambda row: rank(row["read"]))


def _pct(value: float | None) -> str:
    return "–" if value is None else f"{100 * value:.0f}%"


def _num(value: float | None, digits: int = 1) -> str:
    return "–" if value is None else f"{value:.{digits}f}"


def render_role_changes(section: dict[str, Any]) -> list[str]:
    lines = ["## Role changes vs last season", "",
             f"Tonight's players whose ice time over the last 10 games moved {ROLE_TOI_DELTA:g}+ minutes, or "
             f"power-play time {ROLE_PP_DELTA:g}+ minutes, from last season ({ROLE_MIN_GAMES}+ games this "
             f"season, {ROLE_MIN_PRIOR_GAMES}+ last season). The Points rate input counts last season's games "
             "like this season's, so these are the players the model is slowest on. Model and market are "
             "P(Over) at Points 0.5 and SOG 1.5 (market: median no-vig, two-sided lines only); the read uses "
             f"Points 0.5 when quoted and treats a gap under {100 * ROLE_MARKET_GAP:.0f} points as agreement.", ""]
    if not str(section.get("status", "")).startswith("COMPLETE"):
        return lines + [f"Unavailable: {section.get('status')}", ""]
    if section.get("pp_scale"):
        lines[-1:-1] = ["", f"Last season's PP time is scaled by {section['pp_scale']:.2f} (tonight's players' "
                        "power-play time now vs last season) so a league-wide shift is not read as a role change."]
    every = section.get("players") or []
    quoted = [p for p in every if not p["read"].startswith("no two-sided")]
    agree = [p["player_name"] for p in quoted if p["read"].startswith("model and market agree")]
    players = [p for p in quoted if not p["read"].startswith("model and market agree")]
    unquoted = len(every) - len(quoted)
    tail = []
    if agree:
        tail.append(f"Role changed, model and market within {100 * ROLE_MARKET_GAP:.0f} points: {', '.join(agree)}.")
    if unquoted:
        tail.append(f"{unquoted} more role-change player(s) without a two-sided market (in the JSON).")
    if not players:
        return lines + ["No role-change player tonight where the model and market disagree.", "", *tail, ""]
    lines += ["| Player | Role | TOI 2 seasons ago / last / now | PP TOI last (adjusted) / now | Points/game 2 seasons ago / "
              "last / now | Points 0.5 model / market | SOG 1.5 model / market | Read |",
              "|---|---|---|---|---|---|---|---|"]
    for p in players:
        lines.append(
            f"| {p['player_name']} | {p['direction']} | {_num(p['toi_before'])} / {_num(p['toi_prior'])} / "
            f"{_num(p['toi_now'])} | {_num(p['pp_prior'])} / {_num(p['pp_now'])} | {_num(p['ppg_before'], 2)} / "
            f"{_num(p['ppg_prior'], 2)} / {_num(p['ppg_now'], 2)} | {_pct(p['points_model_p_over'])} / "
            f"{_pct(p['points_market_p_over'])} | {_pct(p['sog_model_p_over'])} / {_pct(p['sog_market_p_over'])} "
            f"| {p['read']} |")
    return lines + ["", *tail, ""]


HIGHLIGHT_MAX = 6
REMOVAL_STAKE = 10.0   # dollars per skipped play when grading removal candidates
HOT_GAMES = 3          # hit the line in each of this many most recent played games
HOT_RULES = {"sog": ("official_sog", 2), "points": ("official_points", 1)}  # SOG 1.5, Points 0.5
GOALIE_MIN_STARTS = 3  # starts this season before a team's usual starter is named
GOALIE_BACKUP_SHARE = 0.40  # a non-leader below this share of team starts is a backup; at or above, a tandem
LINEUP_ROOT = ROOT / "artifacts/operational/nhl/projected_lineups"
CROSS_MARKET_ROOT = ROOT / "artifacts/operational/nhl/cross_market_shadow"


def removal_candidates(prior_watches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A slate's Points 0.5 plays the watch says to skip: the model's side on a role change it is behind on.

    Each player's last pregame read that day is used (later runs drop started games). The
    direction is recomputed with the current rule, so older reports grade the same way.
    """
    latest: dict[tuple[int, int], dict[str, Any]] = {}
    for watch in sorted(prior_watches, key=lambda w: w.get("generated_at_utc") or ""):
        for p in (watch.get("role_changes") or {}).get("players") or []:
            latest[(int(p["game_id"]), int(p["player_id"]))] = p
    picks = []
    for (game_id, player_id), p in sorted(latest.items()):
        model, market = p.get("points_model_p_over"), p.get("points_market_p_over")
        if model is None or market is None or "toi_delta" not in p:
            continue
        direction = str(role_direction([p["toi_delta"]], [p["pp_delta"]])[0])
        if not role_read(direction, model, market).startswith("model likely behind"):
            continue
        side = "UNDER" if direction == "UP" else "OVER"
        picks.append({"game_id": game_id, "player_id": player_id, "player_name": p.get("player_name"),
                      "direction": direction, "side": side, "model_p": model if side == "OVER" else 1 - model,
                      "market_p": market if side == "OVER" else 1 - market})
    return picks


def grade_removals(picks: list[dict[str, Any]], outcomes: pd.DataFrame) -> list[dict[str, Any]]:
    """Grade each skipped play at $REMOVAL_STAKE and the market's no-vig price; no game is VOID."""
    o = outcomes.drop_duplicates(["game_id", "player_id"]).set_index(["game_id", "player_id"])
    graded = []
    for p in picks:
        key = (p["game_id"], p["player_id"])
        row = o.loc[key] if key in o.index else None
        if row is None or row.participation_state != "PARTICIPATED" or not bool(row.official_final):
            graded.append({**p, "points": None, "result": "VOID", "dollars": 0.0})
            continue
        points = int(row.official_points)
        won = (points >= 1) == (p["side"] == "OVER")
        dollars = REMOVAL_STAKE * (1 - p["market_p"]) / p["market_p"] if won else -REMOVAL_STAKE
        graded.append({**p, "points": points, "result": "W" if won else "L", "dollars": round(dollars, 2)})
    return graded


def prior_outcomes(operational_root: Path, prior_date: str) -> pd.DataFrame | None:
    """Skater outcomes from the newest complete reconciliation package for the date."""
    packages = [p for p in (Path(operational_root) / "postgame_reconciliation" / prior_date).glob("reconciliation=*")
                if (p / "RUN_COMPLETE.json").is_file() and (p / "canonical_skater_outcomes.csv").is_file()]
    if not packages:
        return None
    newest = max(packages, key=lambda p: (p / "RUN_COMPLETE.json").stat().st_mtime)
    return pd.read_csv(newest / "canonical_skater_outcomes.csv")


def _graded_date(day: str, operational_root: Path, output_root: Path) -> tuple[str, list[dict[str, Any]] | None]:
    watches = [json.loads(p.read_text()) for p in
               (Path(output_root) / f"slate_date={day}").glob("run_id=*/input_watch.json")]
    if not watches:
        return "NO_WATCH_FOR_PRIOR_DATE", None
    if not any((w.get("role_changes") or {}).get("status") == "COMPLETE" for w in watches):
        return "NO_ROLE_READS_FOR_PRIOR_DATE", None  # role reads began 2026-11-06
    outcomes = prior_outcomes(operational_root, day)
    if outcomes is None:
        return "NO_OUTCOMES_YET", None
    return "GRADED", grade_removals(removal_candidates(watches), outcomes)


def removal_section(*, prior_date: str | None, operational_root: Path, output_root: Path) -> dict[str, Any]:
    """The prior slate's removal candidates graded, plus every earlier slate this season."""
    if not prior_date:
        return {"status": "NOT_APPLICABLE"}
    status, picks = _graded_date(prior_date, operational_root, output_root)
    if picks is None:
        return {"status": status, "prior_date": prior_date}
    season = {prior_date: picks}
    for folder in sorted(Path(output_root).glob("slate_date=*")):
        day = folder.name.split("=", 1)[1]
        if day < prior_date and season_of(day) == season_of(prior_date):
            _, earlier = _graded_date(day, operational_root, output_root)
            if earlier is not None:
                season[day] = earlier
    every = [p for rows in season.values() for p in rows]
    tally = lambda rows: {"won": sum(p["result"] == "W" for p in rows), "lost": sum(p["result"] == "L" for p in rows),
                          "void": sum(p["result"] == "VOID" for p in rows),
                          "dollars": round(sum(p["dollars"] for p in rows), 2)}
    return {"status": "GRADED", "prior_date": prior_date, "stake": REMOVAL_STAKE, "picks": picks,
            "totals": tally(picks), "season": {"slates": len(season), "first_slate": min(season), **tally(every)}}


def _removal_summary(totals: dict[str, Any]) -> str:
    saved = -totals["dollars"]
    return (f"skipped plays would have gone {totals['won']}-{totals['lost']}"
            + (f" ({totals['void']} void)" if totals["void"] else "")
            + f", {'saving' if saved >= 0 else 'costing'} ${abs(saved):.2f}")


def render_removals(section: dict[str, Any]) -> list[str]:
    lines = [f"## Removal candidates graded ({section.get('prior_date', 'prior date')})", ""]
    if section.get("status") != "GRADED":
        return lines + [f"Not graded: {section.get('status')}.", ""]
    lines.append(f"Points 0.5 plays the watch said to skip (role change, model likely behind, so the "
                 f"market is the better guide). Each graded at ${section['stake']:g} and the market's "
                 f"no-vig price, so book prices would pay a little less. This slate: "
                 f"{_removal_summary(section['totals'])}. Season so far ({section['season']['slates']} "
                 f"slates from {section['season']['first_slate']}): {_removal_summary(section['season'])}.")
    if section["picks"]:
        lines += ["", "| Player | Role | Skipped side | Market | Model | Points | Would have | $ |",
                  "|---|---|---|---:|---:|---:|---|---:|"]
        lines += [f"| {p['player_name']} | {p['direction']} | {p['side']} 0.5 | {_pct(p['market_p'])} | "
                  f"{_pct(p['model_p'])} | {'–' if p['points'] is None else p['points']} | "
                  f"{ {'W': 'won', 'L': 'lost', 'VOID': 'void'}[p['result']]} | {p['dollars']:+.2f} |"
                  for p in section["picks"]]
    return lines + [""]


def season_outcomes(operational_root: Path, slate_date: str) -> pd.DataFrame | None:
    """Played games this season before the slate, one row per player-game, from saved reconciliations."""
    frames = []
    for folder in sorted((Path(operational_root) / "postgame_reconciliation").glob("20*")):
        day = folder.name
        if day < slate_date and season_of(day) == season_of(slate_date):
            outcomes = prior_outcomes(operational_root, day)
            if outcomes is not None:
                frames.append(outcomes.assign(slate_date=day))
    if not frames:
        return None
    played = pd.concat(frames)
    played = played[played.participation_state.eq("PARTICIPATED") & played.official_final.astype(bool)]
    return played.drop_duplicates(["game_id", "player_id"]).sort_values(["player_id", "slate_date"])


def hot_streaks(*, points: pd.DataFrame | None, sog: pd.DataFrame | None, history: pd.DataFrame | None,
                market: dict[tuple[str, int], float]) -> list[dict[str, Any]]:
    """Tonight's players who cleared the line in each of their last HOT_GAMES played games."""
    if history is None or history.empty:
        return []
    out = []
    for lane, frame, model_column in (("sog", sog, "p_over_1_5"), ("points", points, "prob_over_0_5")):
        if frame is None or model_column not in frame:
            continue
        column, threshold = HOT_RULES[lane]
        recent = history.groupby("player_id").tail(HOT_GAMES)
        hits = recent.assign(hit=recent[column].ge(threshold)).groupby("player_id").hit.agg(["sum", "size"])
        hot = set(hits[(hits["size"] == HOT_GAMES) & (hits["sum"] == HOT_GAMES)].index.astype("int64"))
        for row in frame.drop_duplicates(["game_id", "player_id"]).itertuples(index=False):
            if int(row.player_id) in hot:
                out.append({"lane": lane, "line": ROLE_LINES[lane], "game_id": int(row.game_id),
                            "player_id": int(row.player_id),
                            "player_name": str(getattr(row, "player_name", row.player_id)),
                            "model_p_over": round(float(getattr(row, model_column)), 4),
                            "market_p_over": market.get((lane, int(row.player_id)))})
    # Model's Over lean over the market first: the tags that back a search play.
    return sorted(out, key=lambda p: (p["lane"], p["market_p_over"] is None,
                                      -(p["model_p_over"] - (p["market_p_over"] or 0))))


def grade_hot(tags: list[dict[str, Any]], outcomes: pd.DataFrame) -> list[dict[str, Any]]:
    """Each tag graded as an Over at $REMOVAL_STAKE and the market's no-vig price (VOID without one)."""
    o = outcomes.drop_duplicates(["game_id", "player_id"]).set_index(["game_id", "player_id"])
    graded = []
    for t in tags:
        key = (t["game_id"], t["player_id"])
        row = o.loc[key] if key in o.index else None
        market = t.get("market_p_over")
        if row is None or row.participation_state != "PARTICIPATED" or not bool(row.official_final):
            graded.append({**t, "result": "VOID", "dollars": 0.0})
            continue
        column, threshold = HOT_RULES[t["lane"]]
        won = int(getattr(row, column)) >= threshold
        dollars = (0.0 if not market else REMOVAL_STAKE * (1 - market) / market if won else -REMOVAL_STAKE)
        graded.append({**t, "result": "W" if won else "L", "dollars": round(dollars, 2)})
    return graded


def _hot_date(day: str, operational_root: Path, output_root: Path) -> list[dict[str, Any]] | None:
    latest: dict[tuple[str, int, int], dict[str, Any]] = {}
    watches = [json.loads(p.read_text()) for p in (Path(output_root) / f"slate_date={day}").glob("run_id=*/input_watch.json")]
    for w in sorted(watches, key=lambda w: w.get("generated_at_utc") or ""):
        for t in (w.get("hot_streaks") or {}).get("players") or []:
            latest[(t["lane"], t["game_id"], t["player_id"])] = t
    outcomes = prior_outcomes(operational_root, day) if latest else None
    return None if outcomes is None else grade_hot(list(latest.values()), outcomes)


def hot_section(*, slate_date: str, prior_date: str | None, points, sog, market,
                operational_root: Path, output_root: Path) -> dict[str, Any]:
    players = hot_streaks(points=points, sog=sog, history=season_outcomes(operational_root, slate_date), market=market)
    section: dict[str, Any] = {"status": "COMPLETE", "games": HOT_GAMES, "players": players, "season": {}}
    days = sorted(f.name.split("=", 1)[1] for f in Path(output_root).glob("slate_date=*"))
    for day in days:
        if day < slate_date and season_of(day) == season_of(slate_date):
            graded = _hot_date(day, operational_root, output_root)
            if graded:
                section["season"][day] = graded
                if day == prior_date:
                    section["prior"] = {"date": day, "picks": graded}
    return section


def _hot_tally(rows: list[dict[str, Any]]) -> str:
    settled = [r for r in rows if r["result"] != "VOID"]
    if not settled:
        return "none settled"
    priced = [r for r in settled if r.get("market_p_over")]
    won = sum(r["result"] == "W" for r in settled)
    text = f"{won}-{len(settled) - won} ({100 * won / len(settled):.0f}%)"
    if priced:
        text += (f"; priced {len(priced)}: hit {100 * sum(r['result'] == 'W' for r in priced) / len(priced):.0f}% vs "
                 f"market {100 * sum(r['market_p_over'] for r in priced) / len(priced):.0f}% and model "
                 f"{100 * sum(r['model_p_over'] for r in priced) / len(priced):.0f}%, "
                 f"${sum(r['dollars'] for r in priced):+.2f} at ${REMOVAL_STAKE:g} and no-vig")
    return text


def render_hot(section: dict[str, Any]) -> list[str]:
    lines = [f"## Hot streaks: cleared the line in each of the last {section.get('games', HOT_GAMES)} games", ""]
    if section.get("status") != "COMPLETE":
        return lines + [f"Unavailable: {section.get('status')}.", ""]
    lines.append("Watch-only (NHL Backlog P2.49): from 10-26 to 11-11 these players cleared the line more often "
                 "than the market and the models priced. Tonight's model and market P(over):")
    lines += ["", "| Player | Line | Model | Market |", "|---|---|---:|---:|"]
    lines += [f"| {p['player_name']} | {'SOG 1.5' if p['lane'] == 'sog' else 'Points 0.5'} Over | "
              f"{_pct(p['model_p_over'])} | {_pct(p['market_p_over'])} |" for p in section["players"]]
    if not section["players"]:
        lines.append("| none | | | |")
    for lane, label in (("sog", "SOG 1.5"), ("points", "Points 0.5")):
        prior = [r for r in (section.get("prior") or {}).get("picks", []) if r["lane"] == lane]
        every = [r for rows in section["season"].values() for r in rows if r["lane"] == lane]
        if every:
            lines.append(f"- {label} Over tags graded: prior slate {_hot_tally(prior)}; "
                         f"since {min(section['season'])} {_hot_tally(every)}.")
    return lines + [""]


def season_starts(operational_root: Path, slate_date: str) -> pd.DataFrame | None:
    """Official goalie starts this season before the slate, one row per team-game."""
    frames = []
    for folder in sorted((Path(operational_root) / "postgame_reconciliation").glob("20*")):
        if not (folder.name < slate_date and season_of(folder.name) == season_of(slate_date)):
            continue
        packages = [p for p in folder.glob("reconciliation=*")
                    if (p / "RUN_COMPLETE.json").is_file() and (p / "canonical_goalie_outcomes.csv").is_file()]
        if packages:
            newest = max(packages, key=lambda p: (p / "RUN_COMPLETE.json").stat().st_mtime)
            frames.append(pd.read_csv(newest / "canonical_goalie_outcomes.csv"))
    if not frames:
        return None
    starts = pd.concat(frames)
    starts = starts[starts.actual_start_flag.astype(str).eq("True")]
    return starts.drop_duplicates(["game_id", "team_id"])


def usual_starters(starts: pd.DataFrame | None) -> dict[int, dict[str, Any]]:
    """Each team's starts leader this season, once he has GOALIE_MIN_STARTS starts."""
    if starts is None or starts.empty:
        return {}
    out = {}
    for team_id, team in starts.groupby("team_id"):
        counts = team.goalie_id.value_counts()
        if counts.iloc[0] >= GOALIE_MIN_STARTS:
            out[int(team_id)] = {"goalie_id": int(counts.index[0]), "starts": int(counts.iloc[0]),
                                 "team_games": int(len(team)),
                                 "by_goalie": {int(g): int(n) for g, n in counts.items()}}
    return out


def projected_starters(slate_date: str, lineup_root: Path = LINEUP_ROOT) -> dict[str, dict[str, Any]]:
    """G1 per team from the newest NHL.com lineup capture that covers the slate (P2.47).

    The article labels the day it was published "TODAY" and later days by weekday, so a
    game posted a day early is read from the previous day's capture. Pages are re-parsed
    with the current parser (fixed 2026-11-12)."""
    from backend.nhl.scripts import collect_nhl_projected_lineups as lineups

    slate = datetime.fromisoformat(slate_date).date()
    weekday = slate.strftime("%A").upper()
    folders = [Path(lineup_root) / f"season={season_of(slate_date)}" / f"slate_date={d.isoformat()}"
               for d in (slate, slate - timedelta(days=1))]
    captures = sorted((o for f in folders for o in f.glob("observation=*") if (o / "page.html").is_file()),
                      key=lambda o: o.name, reverse=True)
    found: dict[str, dict[str, Any]] = {}
    for capture in captures:
        html = (capture / "page.html").read_text()
        published = lineups.article_times(html).get("date_published")
        article_day = (datetime.fromisoformat(published.replace("Z", "+00:00")).astimezone(
            ZoneInfo("America/New_York")).date() if published else
            datetime.fromisoformat(capture.parent.name.split("=", 1)[1]).date())
        wanted = "TODAY" if article_day == slate else weekday if article_day < slate else None
        if wanted is None:
            continue
        for row in lineups.parse(html):
            if row["unit"] == "G1" and row["day"] == wanted and row["team_code"] not in found:
                found[row["team_code"]] = {"name": row["name"], "observation": capture.name.split("=", 1)[1]}
    return found


def latest_moneyline(slate_date: str, cross_market_root: Path = CROSS_MARKET_ROOT) -> pd.DataFrame | None:
    """Production moneyline predictions from the newest complete cross-market capture for the slate."""
    files = [p / "moneyline_production_predictions.csv" for p in
             (Path(cross_market_root) / f"season={season_of(slate_date)}" / f"slate_date={slate_date}").glob(
                 "run_type=*/state=*") if (p / "RUN_COMPLETE.json").is_file()]
    files = [f for f in files if f.is_file()]
    if not files:
        return None
    return pd.read_csv(max(files, key=lambda f: f.stat().st_mtime))


def goalie_read(home: dict[str, Any], away: dict[str, Any], favorite: str | None) -> str:
    backups = [t for t in (home, away) if t["status"] == "BACKUP"]
    if any(t["status"] == "NOT_POSTED" for t in (home, away)):
        return "starter not posted yet"
    if not backups:
        return "usual starters"
    if len(backups) == 2:
        return "both teams start a backup"
    side = backups[0]
    if favorite is None:
        return f"backup for {side['team']}"
    return (f"backup on the model's favorite ({side['team']}): pass" if side["team"] == favorite else
            f"backup on the model's underdog ({side['team']}): model side stronger")


def goalie_section(*, slate_date: str, operational_root: Path, names: pd.DataFrame | None,
                   lineup_root: Path = LINEUP_ROOT, cross_market_root: Path = CROSS_MARKET_ROOT) -> dict[str, Any]:
    """Projected starter vs each team's usual starter, next to the moneyline model's favorite.

    The moneyline model has no goalie input (NHL Backlog P2.7), so it prices each team with
    its usual goaltending; a projected backup is a matchup it cannot see."""
    from backend.nhl.scripts import collect_nhl_projected_lineups as lineups

    if names is None or names.empty:
        return {"status": "UNAVAILABLE:no names file", "games": []}
    usual = usual_starters(season_starts(operational_root, slate_date))
    projected = projected_starters(slate_date, lineup_root)
    rows = pd.DataFrame([{"team_code": t, "name": v["name"]} for t, v in projected.items()],
                        columns=["team_code", "name"])
    matched = lineups.match_names(rows, names) if len(rows) else rows.assign(player_id=pd.NA)
    projected_id = dict(zip(matched.team_code, matched.player_id))
    name_of = dict(zip(names.player_id.astype("int64"), names.full_name))
    code_of = dict(zip(names.team_id.astype("int64"), names.team_code))
    moneyline = latest_moneyline(slate_date, cross_market_root)
    if moneyline is not None:
        games = [{"game_id": int(r.game_id), "home": r.home_team, "away": r.away_team,
                  "favorite": r.challenger_favored_team,
                  "favorite_p": round(float(max(r.challenger_home_win_probability,
                                                r.challenger_away_win_probability)), 4),
                  "back_to_back": [t for t, b in ((r.home_team, r.home_back_to_back),
                                                  (r.away_team, r.away_back_to_back)) if int(b)]}
                 for r in moneyline.drop_duplicates("game_id").itertuples(index=False)]
    else:
        teams = names.drop_duplicates(["game_id", "team_code"])
        games = [{"game_id": int(g), "home": None, "away": None, "favorite": None, "favorite_p": None,
                  "back_to_back": [], "teams": sorted(t.team_code)} for g, t in teams.groupby("game_id")]
    team_id_of = {c: t for t, c in code_of.items()}
    out = []
    for game in games:
        sides = []
        for code in ([game["away"], game["home"]] if game["home"] else game.pop("teams")):
            entry = {"team": code, "projected": (projected.get(code) or {}).get("name")}
            regular = usual.get(team_id_of.get(code, -1))
            if regular:
                entry.update(usual=name_of.get(regular["goalie_id"], str(regular["goalie_id"])),
                             usual_starts=regular["starts"], team_games=regular["team_games"])
            pid = projected_id.get(code)
            if entry["projected"] is None:
                entry["status"] = "NOT_POSTED"
            elif not regular:
                entry["status"] = "NO_USUAL_STARTER"
            elif pd.isna(pid):
                entry["status"] = "UNMATCHED"
            else:
                own = regular["by_goalie"].get(int(pid), 0)
                entry["projected_starts"] = own
                entry["status"] = ("USUAL" if int(pid) == regular["goalie_id"] else
                                   "BACKUP" if own < GOALIE_BACKUP_SHARE * regular["team_games"] else "TANDEM")
            sides.append(entry)
        game["away_goalie"], game["home_goalie"] = sides[0], sides[1]
        game["read"] = goalie_read(sides[1], sides[0], game["favorite"])
        out.append(game)
    order = {"pass": 0, "stronger": 1, "both": 2, "backup": 3}
    out.sort(key=lambda g: next((v for k, v in order.items() if k in g["read"]), 9))
    return {"status": "COMPLETE" if moneyline is not None else "NO_MONEYLINE_CAPTURE_YET",
            "posted_teams": len(projected), "games": out}


def _goalie_cell(side: dict[str, Any]) -> str:
    if side["status"] == "NOT_POSTED":
        return f"{side['team']}: not posted"
    text = f"{side['team']}: {side['projected']}"
    if side["status"] == "BACKUP":
        text += (f" (**backup**, {side['projected_starts']} of {side['team_games']} starts; usual {side['usual']}, "
                 f"{side['usual_starts']})")
    elif side["status"] == "TANDEM":
        text += (f" (tandem, {side['projected_starts']} of {side['team_games']} starts; leader {side['usual']}, "
                 f"{side['usual_starts']})")
    elif side["status"] == "USUAL":
        text += f" (usual, {side['usual_starts']} of {side['team_games']})"
    elif side["status"] == "UNMATCHED":
        text += " (name not matched)"
    return text


def render_goalies(section: dict[str, Any]) -> list[str]:
    lines = ["## Goalies vs the moneyline model's favorite", ""]
    if not section.get("games"):
        return lines + [f"Unavailable: {section.get('status')}.", ""]
    lines.append("The moneyline model has no goalie input (P2.7). Projected starter (NHL.com G1) against each "
                 "team's starts leader this season; a goalie with under "
                 f"{100 * GOALIE_BACKUP_SHARE:.0f}% of his team's starts is a backup, more is a tandem. "
                 "A backup on the model's favorite: pass; on its underdog: the model's side is stronger." +(" No moneyline capture for the slate yet."
                                                    if section["status"] != "COMPLETE" else ""))
    lines += ["", "| Game | Model favorite | Away goalie | Home goalie | Rest | Read |", "|---|---|---|---|---|---|"]
    for g in section["games"]:
        fav = f"{g['favorite']} {_pct(g['favorite_p'])}" if g["favorite"] else "—"
        rest = ", ".join(f"{t} back-to-back" for t in g["back_to_back"]) or "—"
        title = f"{g['away']} at {g['home']}" if g["home"] else f"{g['away_goalie']['team']}-{g['home_goalie']['team']}"
        lines.append(f"| {title} | {fav} | {_goalie_cell(g['away_goalie'])} | {_goalie_cell(g['home_goalie'])} | "
                     f"{rest} | {g['read']} |")
    return lines + [""]


def render_highlights(result: dict[str, Any]) -> list[str]:
    """A few lines to read first: who the model is likely behind on, unexplained gaps, data flags."""
    lines = ["## Highlights", ""]
    players = (result.get("role_changes") or {}).get("players") or []
    line_of = lambda p: (f"Points 0.5 model {_pct(p['points_model_p_over'])} vs market "
                         f"{_pct(p['points_market_p_over'])}" if p["read_from"] == "points" else
                         f"SOG 1.5 model {_pct(p['sog_model_p_over'])} vs market {_pct(p['sog_market_p_over'])}")
    behind = [p for p in players if p["read"].startswith("model likely behind")]
    ahead = [p for p in players if p["read"].startswith("model further")]
    if behind:
        lines.append("- **Model likely behind a role change** (Over chance; role "
                     + ("up" if all(p["direction"] == "UP" for p in behind) else "changed") + "):")
        lines += [f"  - {p['player_name']} ({p['direction'].lower()}): {line_of(p)}; TOI "
                  f"{_num(p['toi_prior'])} to {_num(p['toi_now'])}, PP {_num(p['pp_prior'])} to "
                  f"{_num(p['pp_now'])}" for p in behind[:HIGHLIGHT_MAX]]
        if len(behind) > HIGHLIGHT_MAX:
            lines.append(f"  - and {len(behind) - HIGHLIGHT_MAX} more: "
                         + ", ".join(p["player_name"] for p in behind[HIGHLIGHT_MAX:]))
    if ahead:
        lines.append("- **Model ahead of the market on a role change** (check news): "
                     + "; ".join(f"{p['player_name']} {line_of(p)}" for p in ahead[:HIGHLIGHT_MAX]))
    shots = sorted((p for p in players if p.get("sog_market_p_over") is not None
                    and abs(p["sog_model_p_over"] - p["sog_market_p_over"]) >= ROLE_MARKET_GAP),
                   key=lambda p: -abs(p["sog_model_p_over"] - p["sog_market_p_over"]))
    if shots:
        # The SOG model reads recent TOI and per-60 rates, so it tracks role changes; on these
        # players it has been closer to results than the market (NHL Backlog P2.44).
        lines.append(f"- **Shots: SOG 1.5 model vs market {100 * ROLE_MARKET_GAP:.0f}+ apart on a role change** "
                     "(the SOG model has been the better guide on these): "
                     + "; ".join(f"{p['player_name']} model {_pct(p['sog_model_p_over'])} vs market "
                                 f"{_pct(p['sog_market_p_over'])} (role {p['direction'].lower()})"
                                 for p in shots[:HIGHLIGHT_MAX])
                     + (f"; and {len(shots) - HIGHLIGHT_MAX} more: "
                        + ", ".join(p["player_name"] for p in shots[HIGHLIGHT_MAX:]) if len(shots) > HIGHLIGHT_MAX else ""))
    unexplained = [(lane, p) for lane, picks in ((result.get("disagreements") or {}).get("lanes") or {}).items()
                   for p in picks if p["reasons"][0].startswith("no input stands out")]
    if unexplained:
        lines.append("- **Big gaps no input explains** (likely news or lineup the model can't see): "
                     + "; ".join(f"{p['player']} {lane.upper()} {p['line']:g} {p['side']} model "
                                 f"{100 * p['model_p']:.0f}% vs market {100 * p['market_p']:.0f}%"
                                 for lane, p in unexplained[:HIGHLIGHT_MAX]))
    hot = [p for p in (result.get("hot_streaks") or {}).get("players") or []]
    for lane, label in (("sog", "SOG 1.5"), ("points", "Points 0.5")):
        mine = [p for p in hot if p["lane"] == lane]
        if mine:
            lines.append(f"- **Hot: {label} Over cleared in each of the last {HOT_GAMES} games** (watch-only; "
                         f"{len(mine)} players; market and models have underrated these; model's Over lean first): "
                         + "; ".join(f"{p['player_name']} model {_pct(p['model_p_over'])} vs market "
                                     f"{_pct(p['market_p_over'])}" for p in mine[:HIGHLIGHT_MAX])
                         + (f"; and {len(mine) - HIGHLIGHT_MAX} more" if len(mine) > HIGHLIGHT_MAX else ""))
    flagged = [g for g in (result.get("goalies") or {}).get("games") or []
               if "backup" in g["read"]]
    if flagged:
        lines.append("- **Goalies: projected backup** (moneyline model has no goalie input): "
                     + "; ".join(f"{g['away'] or g['away_goalie']['team']} at {g['home'] or g['home_goalie']['team']} "
                                 f"— {g['read']}" for g in flagged))
    removals = result.get("removal_grades") or {}
    if removals.get("status") == "GRADED":
        lines.append(f"- **Removal candidates {removals['prior_date']}**: {_removal_summary(removals['totals'])}; "
                     f"season so far {_removal_summary(removals['season'])}")
    flags = result.get("flags") or []
    lines.append(f"- Data flags: {', '.join(flags)}" if flags else "- Data flags: none")
    if not (behind or ahead or unexplained or shots):
        lines.insert(2, "- Nothing stands out tonight between role changes and the market.")
    return lines + [""]


def render(result: dict[str, Any]) -> str:
    lines = [f"# NHL input watch — {result['slate_date']}", "",
             f"Run `{result['run_id']}`; {result['flag_count']} flag(s). Read-only diagnostic; "
             "predictions are unchanged.", ""]
    lines += render_highlights(result)
    if result.get("role_changes"):
        lines += render_role_changes(result["role_changes"])
    if result.get("removal_grades", {}).get("status") not in (None, "NOT_APPLICABLE"):
        lines += render_removals(result["removal_grades"])
    if result.get("goalies"):
        lines += render_goalies(result["goalies"])
    if result.get("hot_streaks"):
        lines += render_hot(result["hot_streaks"])
    for lane in ("points", "sog"):
        checks = result["sanity"].get(lane) or []
        flagged = [c for c in checks if c["flag"]]
        notes = [c for c in checks if c["count"] and not c["flag"]]
        lines.append(f"## {lane.upper()} input sanity ({result['players'].get(lane, 0)} players)")
        lines += [f"- **FLAG {c['check']}**: {c['count']} ({c['detail']}); e.g. {', '.join(c['examples'])}"
                  for c in flagged] or ["- No flags."]
        lines += [f"- {c['check']}: {c['count']} ({c['detail']})" for c in notes]
        lines.append("")
    explainer = result.get("disagreements") or {}
    if explainer.get("status") == "UNAVAILABLE":
        lines += ["## Biggest model-market disagreements", f"Unavailable: {explainer.get('reason')}", ""]
    for lane, picks in (explainer.get("lanes") or {}).items():
        lines += [f"## Biggest {lane.upper()} disagreements (model above the market on its side)", "",
                  "| Player | Line | Side | Model | Market | Gap | Why |", "|---|---:|---|---:|---:|---:|---|"]
        for p in picks:
            lines.append(f"| {p['player']} | {p['line']:g} | {p['side']} | {100 * p['model_p']:.1f}% | "
                         f"{100 * p['market_p']:.1f}% | {100 * p['gap']:+.1f} | {'; '.join(p['reasons'])} |")
        lines.append("")
    health = result.get("source_health") or {}
    if health.get("status") == "READ":
        lines += ["## Source health (prior date shot attempts)",
                  f"Games without attempts: {', '.join(map(str, health['games_without_attempts'])) or 'none'}; "
                  f"unresolved skaters {health['unresolved_skaters']}; conflicts {health['conflicts']}.", ""]
    return "\n".join(lines)


def run_input_watch(*, slate_date: str, run_id: str, prediction_run_dir: Path,
                    points_capture_dir: Path | None, operational_root: Path,
                    prior_date: str | None = None, output_root: Path = OUTPUT_ROOT,
                    now_utc: str | None = None, prior_usage_path: Path = PRIOR_USAGE_PATH) -> dict[str, Any]:
    now_utc = now_utc or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    ranges = json.loads(POINTS_RANGES_PATH.read_text())
    sog_ranges = json.loads(SOG_RANGES_PATH.read_text()) if SOG_RANGES_PATH.is_file() else None
    points = sog = None
    sog_features_path = Path(prediction_run_dir) / "sog_hgb_features.csv"
    sog_predictions_path = Path(prediction_run_dir) / "sog_predictions_wide_calibrated.csv"
    if sog_features_path.is_file() and sog_predictions_path.is_file():
        predictions = pd.read_csv(sog_predictions_path)
        keep = [c for c in ("game_id", "player_id", "player_name", "p_over_1_5", "p_over_2_5", "p_over_3_5")
                if c in predictions]
        sog = predictions[keep].merge(pd.read_csv(sog_features_path).drop(columns=["poisson_source"], errors="ignore"),
                                      on=["game_id", "player_id"], how="left")
    if points_capture_dir and (Path(points_capture_dir) / "features.csv").is_file():
        features = pd.read_csv(Path(points_capture_dir) / "features.csv")
        predictions = pd.read_csv(Path(points_capture_dir) / "predictions.csv")
        keys = ["game_id", "player_id"]
        points = features.merge(
            predictions[keys + [c for c in predictions.columns if c not in features.columns]],
            on=keys, how="inner")
        if sog is not None and "feature_is_defense" in sog:
            points = points.merge(sog[["game_id", "player_id", "feature_is_defense"]].rename(
                columns={"feature_is_defense": "is_defense"}), on=["game_id", "player_id"], how="left")
    names_path = ROOT / "backend/nhl/exports/daily/names" / f"names_{slate_date}.csv"
    if names_path.is_file():
        names = pd.read_csv(names_path, usecols=["player_id", "full_name"]).drop_duplicates("player_id")
        named = dict(zip(names.player_id.astype("int64"), names.full_name))
        for frame in (points, sog):
            if frame is not None:
                frame["player_name"] = frame.player_id.astype("int64").map(named).fillna(
                    frame["player_name"] if "player_name" in frame else frame.player_id.astype(str))
    sanity = {"points": points_sanity(points, ranges) if points is not None else [],
              "sog": sog_sanity(sog, sog_ranges) if sog is not None else []}
    roles, role_status = None, "UNAVAILABLE:no Points capture"
    if points is not None:
        try:
            if not Path(prior_usage_path).is_file():
                role_status = f"UNAVAILABLE:missing {Path(prior_usage_path).name}; run build_nhl_prior_season_usage.py"
            else:
                roles = role_changes(points, pd.read_csv(prior_usage_path), season_of(slate_date),
                                     prior_points_per_game())
                role_status = "COMPLETE"
        except Exception as error:  # the watch never blocks the run
            role_status = f"UNAVAILABLE:{type(error).__name__}:{error}"
    try:
        disagreements = explain_disagreements(points=points, sog=sog, slate_date=slate_date, run_id=run_id,
                                              operational_root=Path(operational_root), now_utc=now_utc,
                                              ranges=ranges, sog_ranges=sog_ranges, roles=roles)
        disagreements["status"] = "COMPLETE"
    except Exception as error:  # the watch never blocks the run
        disagreements = {"status": "UNAVAILABLE", "reason": f"{type(error).__name__}:{error}"}
    role_section = {"status": role_status, "players": _role_rows(roles, disagreements.get("role_market") or []),
                    "pp_scale": None if roles is None else round(float(roles.attrs.get("pp_scale", 1.0)), 3)}
    health = attempt_source_health(Path(operational_root), prior_date, run_id)
    try:
        removals = removal_section(prior_date=prior_date, operational_root=Path(operational_root),
                                   output_root=Path(output_root))
    except Exception as error:  # the watch never blocks the run
        removals = {"status": f"UNAVAILABLE:{type(error).__name__}:{error}", "prior_date": prior_date}
    try:
        line_market = {(m["lane"], m["player_id"]): m["market_p_over"]
                       for m in disagreements.get("line_market") or []}
        hot = hot_section(slate_date=slate_date, prior_date=prior_date, points=points, sog=sog, market=line_market,
                          operational_root=Path(operational_root), output_root=Path(output_root))
    except Exception as error:  # the watch never blocks the run
        hot = {"status": f"UNAVAILABLE:{type(error).__name__}:{error}", "players": [], "season": {}}
    try:
        goalies = goalie_section(slate_date=slate_date, operational_root=Path(operational_root),
                                 names=pd.read_csv(names_path) if names_path.is_file() else None,
                                 lineup_root=Path(operational_root) / "projected_lineups",
                                 cross_market_root=Path(operational_root) / "cross_market_shadow")
    except Exception as error:  # the watch never blocks the run
        goalies = {"status": f"UNAVAILABLE:{type(error).__name__}:{error}", "games": []}
    flags = [f"{lane}:{c['check']}" for lane, checks in sanity.items() for c in checks if c["flag"]]
    if health.get("flag"):
        flags.append("source:SHOT_ATTEMPTS_GAMES_DROPPED")
    result = {"schema_version": SCHEMA, "slate_date": slate_date, "run_id": run_id, "generated_at_utc": now_utc,
              "players": {"points": 0 if points is None else int(len(points)),
                          "sog": 0 if sog is None else int(len(sog))},
              "sanity": sanity, "role_changes": role_section, "disagreements": disagreements,
              "source_health": health, "removal_grades": removals, "hot_streaks": hot, "goalies": goalies,
              "flags": flags, "flag_count": len(flags), "database_writes": 0, "provider_requests": 0}
    out = Path(output_root) / f"slate_date={slate_date}" / f"run_id={run_id}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "input_watch.json").write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")
    (out / "input_watch.md").write_text(render(result) + "\n")
    sums = "".join(f"{hashlib.sha256((out / name).read_bytes()).hexdigest()}  {name}\n"
                   for name in ("input_watch.json", "input_watch.md"))
    (out / "SHA256SUMS").write_text(sums)
    result["path"] = str(out)
    return result
