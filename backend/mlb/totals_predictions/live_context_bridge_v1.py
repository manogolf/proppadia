"""Outcome-blind live starter/venue bridge for the frozen totals candidate."""
from __future__ import annotations

import gzip
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
from scipy.stats import nbinom

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = REPO_ROOT / "backend/mlb/config/totals_predictions/MLB_TOTALS_DIRECT_NEGATIVE_BINOMIAL_V1.json"
SPINE_DIR = REPO_ROOT / "artifacts/analysis/model_development/mlb_totals_feature_spine_v1/2026-08-06"
RAW_BOXSCORES = REPO_ROOT / "artifacts/raw/mlb/totals_feature_spine_v1/boxscore"
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
SCHEDULE_HYDRATE = "probablePitcher,venue,team"
SCHEDULE_FIELDS = "dates,date,games,gamePk,gameDate,officialDate,status,abstractGameState,detailedState,teams,away,home,team,id,name,probablePitcher,fullName,venue,gameNumber,doubleHeader"
MODEL_VERSION = "DIRECT_NEGATIVE_BINOMIAL"
GOVERNED_STARTER_HISTORY_TIERS = frozenset({
    "DIRECT_STARTER_HISTORY",
    "PITCHER_ROLE_COHORT",
    "TEAM_STARTER_HISTORY",
    "LEAGUE_STARTER_HISTORY",
})


class TotalsLiveContextError(RuntimeError):
    pass


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def fetch_hydrated_schedule(game_date: str) -> tuple[dict[str, Any], str, str]:
    observed = datetime.now(timezone.utc).isoformat()
    query = urlencode({"sportId": 1, "date": game_date, "hydrate": SCHEDULE_HYDRATE, "fields": SCHEDULE_FIELDS})
    with urlopen(Request(f"{SCHEDULE_URL}?{query}", headers={"User-Agent": "proppadia-totals-live-context-v1"}), timeout=45) as response:
        raw = response.read()
    # Field restriction is a safety contract: outcome values must never arrive.
    lowered = raw.lower()
    if b'"score"' in lowered or b'"runs"' in lowered or b'"iswinner"' in lowered:
        raise TotalsLiveContextError("OUTCOME_FIELD_PRESENT_IN_LIVE_SCHEDULE")
    return json.loads(raw), observed, hashlib.sha256(raw).hexdigest()


def normalize_schedule(payload: dict[str, Any], observed_at_utc: str, source_sha256: str) -> list[dict[str, Any]]:
    rows = []
    for day in payload.get("dates", []):
        for game in day.get("games", []):
            sides = {}
            for side in ("away", "home"):
                entry = game.get("teams", {}).get(side, {})
                pitcher = entry.get("probablePitcher")
                if not pitcher:
                    status = "PROBABLE_PITCHER_UNAVAILABLE"
                elif not pitcher.get("id"):
                    status = "PROBABLE_PITCHER_IDENTITY_UNRESOLVED"
                else:
                    status = "PROBABLE_PITCHER_CERTIFIED"
                sides[side] = {
                    "team_id": entry.get("team", {}).get("id"),
                    "team_name": entry.get("team", {}).get("name"),
                    "probable_pitcher_id": pitcher.get("id") if pitcher else None,
                    "probable_pitcher_name": pitcher.get("fullName") if pitcher else None,
                    "probable_pitcher_status": status,
                }
            venue = game.get("venue") or {}
            rows.append({
                "game_pk": int(game["gamePk"]), "game_date": game.get("officialDate", day.get("date")),
                "scheduled_start_utc": game["gameDate"], "game_number": int(game.get("gameNumber", 1)),
                "doubleheader_state": game.get("doubleHeader", "N"),
                "official_game_status": game.get("status", {}).get("detailedState", "UNKNOWN"),
                "away_team_id": sides["away"]["team_id"], "away_team_name": sides["away"]["team_name"],
                "home_team_id": sides["home"]["team_id"], "home_team_name": sides["home"]["team_name"],
                "away_probable_pitcher_id": sides["away"]["probable_pitcher_id"],
                "away_probable_pitcher_name": sides["away"]["probable_pitcher_name"],
                "away_probable_pitcher_status": sides["away"]["probable_pitcher_status"],
                "home_probable_pitcher_id": sides["home"]["probable_pitcher_id"],
                "home_probable_pitcher_name": sides["home"]["probable_pitcher_name"],
                "home_probable_pitcher_status": sides["home"]["probable_pitcher_status"],
                "venue_id": venue.get("id"), "venue_name": venue.get("name"),
                "source_observed_at_utc": observed_at_utc, "source_sha256": source_sha256,
            })
    identities = [(r["game_pk"], r["game_number"]) for r in rows]
    if len(identities) != len(set(identities)):
        raise TotalsLiveContextError("DUPLICATE_GAME_NUMBER_IDENTITY")
    return rows


def load_candidate() -> dict[str, Any]:
    candidate = json.loads(CONFIG_PATH.read_text())
    stable = {k: candidate[k] for k in candidate if k != "canonical_model_hash"}
    if canonical_hash(stable) != candidate["canonical_model_hash"]:
        raise TotalsLiveContextError("TOTALS_CANDIDATE_HASH_MISMATCH")
    return candidate


def score_mean(features: dict[str, float], candidate: dict[str, Any]) -> float:
    order = candidate["feature_order"]
    values = np.array([float(features[name]) for name in order])
    scaled = (values - np.array(candidate["scaler_mean"])) / np.array(candidate["scaler_scale"])
    eta = float(candidate["intercept"] + np.dot(scaled, np.array(candidate["coefficients"])))
    return math.exp(eta)


def distribution(mu: float, alpha: float, max_total: int = 30) -> np.ndarray:
    k = np.arange(max_total + 1); size = 1 / alpha; prob = size / (size + mu)
    mass = nbinom.pmf(k, size, prob); mass[-1] += max(0.0, 1 - mass.sum())
    return mass


def _historical_context() -> dict[str, Any]:
    core = pd.read_csv(SPINE_DIR / "totals_core_feature_spine.csv")
    core["game_date"] = pd.to_datetime(core["game_date"]); core["scheduled_start_utc"] = pd.to_datetime(core["scheduled_start_utc"], utc=True)
    league_total = float(core.final_total.mean()); league_home = float(core.final_home_runs.mean()); league_away = float(core.final_away_runs.mean())
    teams = {}
    for team in set(core.home_team_id) | set(core.away_team_id):
        scored = pd.concat([core.loc[core.home_team_id == team, "final_home_runs"], core.loc[core.away_team_id == team, "final_away_runs"]])
        allowed = pd.concat([core.loc[core.home_team_id == team, "final_away_runs"], core.loc[core.away_team_id == team, "final_home_runs"]])
        teams[int(team)] = {"offense": float(scored.mean()), "prevention": float(allowed.mean())}
    appearances, team_starts, league_starts, team_relievers = {}, {}, [], {}
    for path in sorted(RAW_BOXSCORES.glob("*.json.gz")):
        game = int(path.name.split(".")[0]); match = core[core.game_pk == game]
        if match.empty:
            continue
        row = match.iloc[0]; box = json.loads(gzip.decompress(path.read_bytes()))
        for side in ("home", "away"):
            team_id = int(row[f"{side}_team_id"])
            for pitcher_id in box["teams"][side].get("pitchers", []):
                stats = box["teams"][side].get("players", {}).get(f"ID{pitcher_id}", {}).get("stats", {}).get("pitching", {})
                if not stats:
                    continue
                rec = {"game_pk": game, "date": row.game_date.date(), "pitcher_id": int(pitcher_id), "team_id": team_id,
                       "is_starter": bool(int(stats.get("gamesStarted", 0))), "outs": int(stats.get("outs", 0)),
                       "batters_faced": int(stats.get("battersFaced", 0)), "pitches": int(stats.get("pitchesThrown", stats.get("numberOfPitches", 0))),
                       "runs": int(stats.get("runs", 0)), "earned_runs": int(stats.get("earnedRuns", 0)),
                       "hits": int(stats.get("hits", 0)), "walks": int(stats.get("baseOnBalls", 0)),
                       "strikeouts": int(stats.get("strikeOuts", 0)), "home_runs": int(stats.get("homeRuns", 0))}
                appearances.setdefault(int(pitcher_id), []).append(rec)
                if rec["is_starter"]:
                    team_starts.setdefault(team_id, []).append(rec); league_starts.append(rec)
                else:
                    team_relievers.setdefault(team_id, []).append(rec)
    for collection in (appearances, team_starts, team_relievers):
        for values in collection.values():
            values.sort(key=lambda x: (x["date"], x["game_pk"]))
    league_starts.sort(key=lambda x: (x["date"], x["game_pk"]))
    # Reconstruct the exact frozen regressed-park state through the last prior
    # completed game. Reading the last historical feature row would omit that
    # row's outcome, because every spine row is itself strict-prior.
    parks, park_history, league_games, team_scored = {}, {}, [], {}
    venue_meta = pd.read_csv(SPINE_DIR / "strict_prior_park_factor.csv").sort_values("feature_cutoff_utc")
    venue_meta = {int(venue): group.iloc[-1].to_dict() for venue, group in venue_meta.groupby("venue_id")}
    for row in core.sort_values(["game_date", "scheduled_start_utc", "game_pk"]).itertuples():
        venue = int(row.venue_id); prior = park_history.setdefault(venue, [])
        league_mean = float(np.mean([x["total"] for x in league_games])) if league_games else 8.6
        home_prior = team_scored.get(int(row.home_team_id), []); away_prior = team_scored.get(int(row.away_team_id), [])
        expected_home = float(np.mean(home_prior)) if home_prior else league_mean / 2
        expected_away = float(np.mean(away_prior)) if away_prior else league_mean / 2
        prior.append({"game": int(row.game_pk), "source_sha256": row.source_sha256, "total": float(row.final_total),
                      "adjusted_total_ratio": float(row.final_total) / max(expected_home + expected_away, .5)})
        league_games.append({"total": float(row.final_total)})
        team_scored.setdefault(int(row.home_team_id), []).append(float(row.final_home_runs))
        team_scored.setdefault(int(row.away_team_id), []).append(float(row.final_away_runs))
    for venue, prior in park_history.items():
        n = len(prior); weight = n / (n + 50); direct = float(np.mean([x["adjusted_total_ratio"] for x in prior]))
        meta = venue_meta.get(venue, {})
        parks[venue] = {"venue_id": venue, "park_name": meta.get("park_name", ""), "park_history_depth": n,
                        "strict_prior_total_run_factor": weight * direct + (1 - weight),
                        "fallback_status": "DIRECT_REGRESSED_PARK_HISTORY" if n >= 20 else "LEAGUE_REGRESSED_SPARSE_PARK",
                        "roof_type": meta.get("roof_type", "UNAVAILABLE"), "elevation": meta.get("elevation"),
                        "latest_included_game_id": prior[-1]["game"],
                        "source_sha256": canonical_hash([x["source_sha256"] for x in prior])}
    return {"core": core, "league_total": league_total, "league_home": league_home, "league_away": league_away,
            "teams": teams, "appearances": appearances, "team_starts": team_starts, "league_starts": league_starts,
            "team_relievers": team_relievers, "parks": parks}


def _starter(pid: int | None, team_id: int, cutoff_date, feature_cutoff_utc: str, history: dict[str, Any]) -> dict[str, Any]:
    prior = [x for x in history["appearances"].get(int(pid), []) if x["is_starter"] and x["date"] < cutoff_date] if pid else []
    if len(prior) >= 3:
        base, tier = prior[-3:], "DIRECT_STARTER_HISTORY"
    elif prior:
        base, tier = prior, "PITCHER_ROLE_COHORT"
    else:
        base = [x for x in history["team_starts"].get(team_id, []) if x["date"] < cutoff_date][-20:]
        tier = "TEAM_STARTER_HISTORY" if base else "LEAGUE_STARTER_HISTORY"
        if not base:
            base = [x for x in history["league_starts"] if x["date"] < cutoff_date][-200:]
    if not base:
        base = [{"outs": 15, "batters_faced": 21, "pitches": 85, "runs": history["league_total"] / 2}]
    outs = sum(x["outs"] for x in prior); runs = sum(x["runs"] for x in prior)
    state = {"probable_pitcher_id": pid, "prior_starts": len(prior), "history_depth": len(prior),
             "starter_ra9": 27 * runs / outs if outs else history["league_total"] / 2,
             "expected_outs": float(np.clip(np.mean([x["outs"] for x in base]), 3, 27)),
             "workload_uncertainty_outs": float(np.std([x["outs"] for x in base])) if len(base) > 1 else 4.5,
             "fallback_tier": tier, "sparse_history_status": "DIRECT" if len(prior) >= 3 else "GOVERNED_SPARSE_FALLBACK",
             "feature_cutoff_utc": feature_cutoff_utc,
             "latest_included_game_id": prior[-1]["game_pk"] if prior else None,
             "latest_included_game_date": prior[-1]["date"].isoformat() if prior else None,
             "certification_status": "STRICT_PRIOR_STARTER_STATE" if pid else "GOVERNED_STARTER_FALLBACK"}
    state["state_hash"] = canonical_hash({**state, "pitcher_id": pid, "team_id": team_id, "cutoff_date": str(cutoff_date)})
    return state


def _bullpen(team_id: int, cutoff_date, history: dict[str, Any]) -> dict[str, Any]:
    prior = [x for x in history["team_relievers"].get(team_id, []) if x["date"] < cutoff_date]
    recent30 = [x for x in prior if (cutoff_date - x["date"]).days <= 30]
    recent1 = [x for x in prior if (cutoff_date - x["date"]).days <= 1]
    recent3 = [x for x in prior if (cutoff_date - x["date"]).days <= 3]
    outs = sum(x["outs"] for x in prior); runs = sum(x["runs"] for x in prior)
    return {"bullpen_ra9": 27 * runs / outs if outs else history["league_total"] / 2,
            "recent_innings_burden": sum(x["outs"] for x in recent3) / 3,
            "likely_available_reliever_count": max(0, len({x["pitcher_id"] for x in recent30}) - len({x["pitcher_id"] for x in recent1})),
            "certification_status": "GOVERNED_TEAM_RELIEVER_HISTORY" if prior else "LEAGUE_BULLPEN_FALLBACK"}


def attach_context(schedule_row: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
    cutoff_date = pd.Timestamp(schedule_row["game_date"]).date()
    away = _starter(schedule_row["away_probable_pitcher_id"], int(schedule_row["away_team_id"]), cutoff_date, schedule_row["scheduled_start_utc"], history)
    home = _starter(schedule_row["home_probable_pitcher_id"], int(schedule_row["home_team_id"]), cutoff_date, schedule_row["scheduled_start_utc"], history)
    away_bullpen = _bullpen(int(schedule_row["away_team_id"]), cutoff_date, history); home_bullpen = _bullpen(int(schedule_row["home_team_id"]), cutoff_date, history)
    park = history["parks"].get(int(schedule_row["venue_id"])) if schedule_row.get("venue_id") else None
    park_state = {"park_factor": float(park["strict_prior_total_run_factor"]), "park_history_depth": int(park["park_history_depth"]),
                  "fallback_status": park["fallback_status"], "roof_type": park["roof_type"], "elevation": park["elevation"],
                  "feature_cutoff_utc": schedule_row["scheduled_start_utc"], "latest_included_game_id": park.get("latest_included_game_id"),
                  "state_hash": canonical_hash({k: park[k] for k in ("venue_id", "strict_prior_total_run_factor", "park_history_depth", "fallback_status")})} if park else {
                  "park_factor": 1.0, "park_history_depth": 0, "fallback_status": "LEAGUE_PARK_FALLBACK", "roof_type": "UNAVAILABLE", "elevation": None,
                  "feature_cutoff_utc": schedule_row["scheduled_start_utc"], "latest_included_game_id": None,
                  "state_hash": canonical_hash({"venue_id": schedule_row.get("venue_id"), "fallback": "LEAGUE_PARK_FALLBACK"})}
    probable_certified = all(
        schedule_row.get(f"{side}_probable_pitcher_status") == "PROBABLE_PITCHER_CERTIFIED"
        for side in ("away", "home")
    )
    starter_features_certified = all(
        state["certification_status"] == "STRICT_PRIOR_STARTER_STATE"
        and state["fallback_tier"] in GOVERNED_STARTER_HISTORY_TIERS
        for state in (away, home)
    )
    complete = (probable_certified and starter_features_certified
                and park is not None and park["fallback_status"] == "DIRECT_REGRESSED_PARK_HISTORY")
    unresolved = any(schedule_row.get(f"{side}_probable_pitcher_status") == "PROBABLE_PITCHER_IDENTITY_UNRESOLVED" for side in ("away", "home"))
    quality = "TOTALS_CONTEXT_UNRESOLVED" if unresolved else ("TOTALS_CONTEXT_COMPLETE" if complete else "TOTALS_CONTEXT_PARTIAL_FALLBACK")
    starter_history_quality = ("DIRECT_STARTER_HISTORY_BOTH" if all(
        state["fallback_tier"] == "DIRECT_STARTER_HISTORY" for state in (away, home)
    ) else "GOVERNED_SPARSE_STARTER_HISTORY")
    return {**schedule_row, "away_starter_state": away, "home_starter_state": home, "away_bullpen_state": away_bullpen,
            "home_bullpen_state": home_bullpen, "park_state": park_state,
            "away_starter_history_fallback_tier": away["fallback_tier"],
            "home_starter_history_fallback_tier": home["fallback_tier"],
            "starter_history_fallback_tier": f"away={away['fallback_tier']}|home={home['fallback_tier']}",
            "starter_history_quality_state": starter_history_quality,
            "data_quality_status": quality}


def feature_row(context: dict[str, Any], history: dict[str, Any], candidate: dict[str, Any]) -> dict[str, float]:
    home_team = history["teams"].get(int(context["home_team_id"]), {"offense": history["league_home"], "prevention": history["league_away"]})
    away_team = history["teams"].get(int(context["away_team_id"]), {"offense": history["league_away"], "prevention": history["league_home"]})
    hs, aws = context["home_starter_state"], context["away_starter_state"]; hb, ab = context["home_bullpen_state"], context["away_bullpen_state"]
    values = {"league_total": history["league_total"], "home_offense": home_team["offense"], "home_prevention": home_team["prevention"],
              "away_offense": away_team["offense"], "away_prevention": away_team["prevention"], "home_starter_ra9": hs["starter_ra9"],
              "away_starter_ra9": aws["starter_ra9"], "home_starter_prior_starts": hs["prior_starts"], "away_starter_prior_starts": aws["prior_starts"],
              "home_expected_outs": hs["expected_outs"], "away_expected_outs": aws["expected_outs"],
              "home_workload_uncertainty_outs": hs["workload_uncertainty_outs"], "away_workload_uncertainty_outs": aws["workload_uncertainty_outs"],
              "home_bullpen_ra9": hb["bullpen_ra9"], "away_bullpen_ra9": ab["bullpen_ra9"],
              "home_bullpen_likely_available_reliever_count": hb["likely_available_reliever_count"],
              "away_bullpen_likely_available_reliever_count": ab["likely_available_reliever_count"],
              "home_bullpen_recent_innings_burden": hb["recent_innings_burden"], "away_bullpen_recent_innings_burden": ab["recent_innings_burden"],
              "strict_prior_total_run_factor": context["park_state"]["park_factor"], "park_history_depth": context["park_state"]["park_history_depth"],
              "game_number": context["game_number"]}
    return {name: float(values[name]) for name in candidate["feature_order"]}


def score_context(context: dict[str, Any], history: dict[str, Any], candidate: dict[str, Any], prediction_timestamp_utc: str) -> dict[str, Any]:
    start = datetime.fromisoformat(context["scheduled_start_utc"].replace("Z", "+00:00"))
    prediction_time = datetime.fromisoformat(prediction_timestamp_utc.replace("Z", "+00:00"))
    if start <= prediction_time:
        raise TotalsLiveContextError("POST_START_GAME_NOT_ELIGIBLE")
    features = feature_row(context, history, candidate); mu = score_mean(features, candidate); mass = distribution(mu, candidate["dispersion_alpha"]); cdf = np.cumsum(mass)
    return {"expected_total": mu, "interval_80_low": int(np.searchsorted(cdf, .1)), "interval_80_high": int(np.searchsorted(cdf, .9)),
            "p_over_7_5": float(mass[8:].sum()), "p_over_8_5": float(mass[9:].sum()), "p_over_9_5": float(mass[10:].sum()),
            "p_over_10_5": float(mass[11:].sum()), "prediction_timestamp_utc": prediction_timestamp_utc,
            "model_version": MODEL_VERSION, "model_hash": candidate["canonical_model_hash"], "feature_vector_hash": canonical_hash(features)}


def build_history() -> dict[str, Any]:
    return _historical_context()
