#!/usr/bin/env python3
"""Bounded MLB pitch-discipline and repeated-contact multi-hit pilot.

This research-only utility reconstructs pitch-grain discipline evidence from
preserved local MLB StatsAPI feed/live JSON and tests whether strict-prior
hitter/pitcher contact-frequency profiles improve repeated-contact and
exactly-one-hit versus two-plus-hit prediction.

No network calls, OddsAPI calls, DB writes, production model/candidate/upload
changes, scheduler changes, threshold search, price optimization, or
hyperparameter search are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score

from backend.mlb.scripts import run_mlb_pregame_contact_opportunity_multi_hit_pilot as contact_pilot

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_pitch_discipline_repeated_contact_pilot/2026-07-17"

CONTACT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/2026-07-17"
POP_PATH = CONTACT_ROOT / "research_only_model_artifacts_2026-07-17.csv"
CONTACT_LEDGER = CONTACT_ROOT / "canonical_contact_outcome_ledger_2026-07-17.csv"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"
RAW_FEEDS = ROOT / "artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17/raw_official_mlb"

EPS = 1e-9
RNG_SEED = 20260717
FIT_END = pd.Timestamp("2026-06-11")

PA_EXCLUDED_EVENT_TYPES = {
    "pickoff_1b", "pickoff_2b", "pickoff_3b",
    "caught_stealing_2b", "caught_stealing_3b", "caught_stealing_home",
    "stolen_base_2b", "stolen_base_3b", "stolen_base_home",
    "wild_pitch", "passed_ball", "defensive_indiff",
}
HIT_CAPABLE_CONTACT_EVENTS = {
    "single", "double", "triple", "home_run", "field_out", "force_out",
    "grounded_into_double_play", "double_play", "fielders_choice_out",
    "field_error", "sac_fly", "fielders_choice", "sac_fly_double_play",
}
HIT_EVENTS = {"single", "double", "triple", "home_run"}
SWING_CALLS = {"S", "W", "T", "F", "L", "M", "Q", "R", "X", "D", "E"}
CONTACT_CALLS = {"F", "L", "X", "D", "E"}
WHIFF_CALLS = {"S", "W", "T", "M", "Q", "R"}
FOUL_CALLS = {"F", "L"}
BALL_CALLS = {"B", "I", "P", "V"}
CALLED_STRIKE_CALLS = {"C"}


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def norm_id(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    if text.endswith(".0"):
        text = text[:-2]
    return text


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def team_meta(feed: dict[str, Any], side: str) -> dict[str, Any]:
    return feed.get("gameData", {}).get("teams", {}).get(side, {}) or {}


def box_team(feed: dict[str, Any], side: str) -> dict[str, Any]:
    return feed.get("liveData", {}).get("boxscore", {}).get("teams", {}).get(side, {}) or {}


def player_obj(team: dict[str, Any], player_id: Any) -> dict[str, Any]:
    return team.get("players", {}).get(f"ID{norm_id(player_id)}", {}) or {}


def pitching_stats(team: dict[str, Any], player_id: Any) -> dict[str, Any]:
    return player_obj(team, player_id).get("stats", {}).get("pitching", {}) or {}


def player_name(team: dict[str, Any], player_id: Any) -> str:
    return player_obj(team, player_id).get("person", {}).get("fullName", "") or ""


def lineup_slot(team: dict[str, Any], player_id: Any) -> int | None:
    raw = player_obj(team, player_id).get("battingOrder")
    if not raw:
        return None
    try:
        return int(str(raw)[:-2])
    except Exception:
        return None


def feed_game_identity(path: Path, feed: dict[str, Any]) -> dict[str, Any]:
    gd = feed.get("gameData", {})
    game = gd.get("game", {}) or {}
    dt = gd.get("datetime", {}) or {}
    status = gd.get("status", {}) or {}
    return {
        "game_id": norm_id(game.get("pk") or game.get("gamePk") or feed.get("gamePk") or path.stem.split("_")[-1]),
        "game_date": dt.get("officialDate") or dt.get("originalDate") or "",
        "game_status": status.get("detailedState") or status.get("abstractGameState") or "",
        "away_team": team_meta(feed, "away").get("abbreviation") or "",
        "home_team": team_meta(feed, "home").get("abbreviation") or "",
        "source_path": rel(path),
        "source_sha256": sha256(path),
    }


def load_feeds(raw_root: Path) -> list[tuple[Path, dict[str, Any]]]:
    feeds: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(raw_root.glob("mlb_statsapi_feed_live_game_*.json")):
        try:
            feeds.append((path, json.loads(path.read_text(encoding="utf-8"))))
        except Exception:
            continue
    return feeds


def is_pa(play: dict[str, Any]) -> bool:
    if not play.get("about", {}).get("isComplete", False):
        return False
    matchup = play.get("matchup", {}) or {}
    if not matchup.get("batter", {}).get("id") or not matchup.get("pitcher", {}).get("id"):
        return False
    event_type = (play.get("result", {}) or {}).get("eventType") or ""
    return event_type not in PA_EXCLUDED_EVENT_TYPES


def starters_for_feed(feed: dict[str, Any]) -> dict[str, dict[str, Any]]:
    starters: dict[str, dict[str, Any]] = {}
    for side in ("away", "home"):
        team = box_team(feed, side)
        code = team_meta(feed, side).get("abbreviation") or ""
        ids = []
        for pid in team.get("pitchers", []) or []:
            if as_int(pitching_stats(team, pid).get("gamesStarted")) == 1:
                ids.append(norm_id(pid))
        if not ids:
            ids = [norm_id(x) for x in (team.get("pitchers", []) or [])[:1]]
        starters[code] = {
            "starter_pitcher_id": ids[0] if len(ids) == 1 else "",
            "starter_pitcher_name": player_name(team, ids[0]) if len(ids) == 1 else "",
        }
    return starters


def pitcher_team_lookup(feed: dict[str, Any]) -> dict[str, str]:
    lookup = {}
    for side in ("away", "home"):
        code = team_meta(feed, side).get("abbreviation") or ""
        for pid in box_team(feed, side).get("pitchers", []) or []:
            lookup[norm_id(pid)] = code
    return lookup


def batter_team_lookup(feed: dict[str, Any], batter_id: str) -> tuple[str, str, dict[str, Any]]:
    for side in ("away", "home"):
        team = box_team(feed, side)
        if f"ID{batter_id}" in (team.get("players") or {}):
            code = team_meta(feed, side).get("abbreviation") or ""
            opp_side = "home" if side == "away" else "away"
            return code, team_meta(feed, opp_side).get("abbreviation") or "", team
    return "", "", {}


def role_for_pitcher(pitcher_id: str, pitcher_team: str, starters: dict[str, dict[str, Any]], entry_order: int) -> str:
    if pitcher_id and pitcher_id == starters.get(pitcher_team, {}).get("starter_pitcher_id", ""):
        return "STARTER_FACING_PA"
    if entry_order == 1 and not starters.get(pitcher_team, {}).get("starter_pitcher_id", ""):
        return "IRREGULAR_ROLE_UNRESOLVED"
    return "RELIEVER_FACING_PA"


def pitch_family(code: str) -> str:
    fast = {"FF", "FT", "SI", "FC", "FA"}
    break_ = {"SL", "CU", "KC", "SV", "ST", "CS"}
    off = {"CH", "FS", "FO", "SC"}
    if code in fast:
        return "fastball_family"
    if code in break_:
        return "breaking_family"
    if code in off:
        return "offspeed_family"
    if code:
        return "other_pitch_family"
    return "missing_pitch_family"


def build_pitch_ledger(feeds: list[tuple[Path, dict[str, Any]]], game_ids: set[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pitch_rows: list[dict[str, Any]] = []
    pa_rows: list[dict[str, Any]] = []
    source_rows: list[dict[str, Any]] = []
    for path, feed in feeds:
        ident = feed_game_identity(path, feed)
        plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", []) or []
        include = ident["game_status"] == "Final" and ident["game_id"] in game_ids
        source_rows.append({**ident, "all_plays": len(plays), "included": include, "source_grain": "StatsAPI feed/live allPlays/playEvents"})
        if not include:
            continue
        starters = starters_for_feed(feed)
        pteam = pitcher_team_lookup(feed)
        pitcher_first_seen: dict[str, int] = {}
        pitcher_sequence = 0
        pa_sequence = 0
        for play in plays:
            if not is_pa(play):
                continue
            matchup = play.get("matchup", {}) or {}
            result = play.get("result", {}) or {}
            event_type = result.get("eventType") or ""
            about = play.get("about", {}) or {}
            batter_id = norm_id(matchup.get("batter", {}).get("id"))
            pitcher_id = norm_id(matchup.get("pitcher", {}).get("id"))
            pitcher_team = pteam.get(pitcher_id, "")
            hitter_team, opponent, batter_team_obj = batter_team_lookup(feed, batter_id)
            if pitcher_id not in pitcher_first_seen:
                pitcher_sequence += 1
                pitcher_first_seen[pitcher_id] = pitcher_sequence
            entry_order = pitcher_first_seen[pitcher_id]
            role = role_for_pitcher(pitcher_id, pitcher_team, starters, entry_order)
            pa_sequence += 1
            pitches = [ev for ev in (play.get("playEvents", []) or []) if ev.get("isPitch")]
            terminal_pitch_index = pitches[-1].get("index") if pitches else ""
            has_hcc = event_type in HIT_CAPABLE_CONTACT_EVENTS
            pa_rows.append({
                **ident,
                "plate_appearance_sequence": pa_sequence,
                "pa_key": f"{ident['game_date']}|{ident['game_id']}|{pa_sequence}|{batter_id}|{pitcher_id}",
                "source_event_identity": f"{ident['game_id']}:{play.get('atBatIndex', about.get('atBatIndex', ''))}",
                "batter_id": batter_id,
                "batter_name": matchup.get("batter", {}).get("fullName", ""),
                "batter_team": hitter_team,
                "opponent": opponent,
                "lineup_slot": lineup_slot(batter_team_obj, batter_id),
                "pitcher_id": pitcher_id,
                "pitcher_name": matchup.get("pitcher", {}).get("fullName", ""),
                "pitcher_team": pitcher_team,
                "pitcher_entry_sequence": entry_order,
                "starter_reliever_role": role,
                "batter_hand": (matchup.get("batSide") or {}).get("code", ""),
                "pitcher_hand": (matchup.get("pitchHand") or {}).get("code", ""),
                "pa_result": result.get("event", ""),
                "event_type": event_type,
                "official_hit": int(event_type in HIT_EVENTS),
                "hit_capable_contact": int(has_hcc),
                "strikeout": int(event_type == "strikeout"),
                "walk_hbp": int(event_type in {"walk", "intent_walk", "hit_by_pitch"}),
                "terminal_pitch_index": terminal_pitch_index,
                "pitch_count": len(pitches),
            })
            prev_balls = 0
            prev_strikes = 0
            for idx, ev in enumerate(pitches, start=1):
                details = ev.get("details", {}) or {}
                pdata = ev.get("pitchData", {}) or {}
                coords = pdata.get("coordinates", {}) or {}
                code = details.get("code") or (details.get("call", {}) or {}).get("code") or ""
                ptype = (details.get("type") or {}).get("code") or ""
                is_terminal = ev.get("index") == terminal_pitch_index
                is_in_play = bool(details.get("isInPlay")) or code in {"X", "D", "E"}
                pitch_rows.append({
                    **ident,
                    "plate_appearance_sequence": pa_sequence,
                    "pitch_sequence": idx,
                    "canonical_pitch_identity": f"{ident['game_date']}|{ident['game_id']}|{pa_sequence}|{idx}|{batter_id}|{pitcher_id}",
                    "pa_key": f"{ident['game_date']}|{ident['game_id']}|{pa_sequence}|{batter_id}|{pitcher_id}",
                    "source_event_identity": f"{ident['game_id']}:{play.get('atBatIndex', about.get('atBatIndex', ''))}:{ev.get('index', idx)}",
                    "batter_id": batter_id,
                    "batter_name": matchup.get("batter", {}).get("fullName", ""),
                    "batter_team": hitter_team,
                    "opponent": opponent,
                    "lineup_slot": lineup_slot(batter_team_obj, batter_id),
                    "pitcher_id": pitcher_id,
                    "pitcher_name": matchup.get("pitcher", {}).get("fullName", ""),
                    "pitcher_team": pitcher_team,
                    "starter_reliever_role": role,
                    "batter_hand": (matchup.get("batSide") or {}).get("code", ""),
                    "pitcher_hand": (matchup.get("pitchHand") or {}).get("code", ""),
                    "balls_before_pitch": prev_balls,
                    "strikes_before_pitch": prev_strikes,
                    "count_state_before_pitch": f"{prev_balls}-{prev_strikes}",
                    "pitch_type": ptype,
                    "pitch_family": pitch_family(ptype),
                    "pitch_velocity": pdata.get("startSpeed"),
                    "zone": pdata.get("zone"),
                    "zone_state": "in_zone" if as_int(pdata.get("zone"), 99) in range(1, 10) else ("out_of_zone" if pdata.get("zone") is not None else "zone_missing"),
                    "plate_x": coords.get("pX"),
                    "plate_z": coords.get("pZ"),
                    "pitch_call_code": code,
                    "pitch_call_description": details.get("description") or (details.get("call", {}) or {}).get("description", ""),
                    "swing": int(code in SWING_CALLS or is_in_play),
                    "contact": int(code in CONTACT_CALLS or is_in_play),
                    "whiff": int(code in WHIFF_CALLS),
                    "foul": int(code in FOUL_CALLS),
                    "ball_in_play": int(is_in_play),
                    "called_strike": int(code in CALLED_STRIKE_CALLS),
                    "ball": int(code in BALL_CALLS or bool(details.get("isBall"))),
                    "hbp": int(event_type == "hit_by_pitch" and is_terminal),
                    "terminal_pitch": int(is_terminal),
                    "terminal_hit_capable_contact": int(is_terminal and has_hcc),
                    "terminal_strikeout": int(is_terminal and event_type == "strikeout"),
                    "terminal_walk_hbp": int(is_terminal and event_type in {"walk", "intent_walk", "hit_by_pitch"}),
                    "source_path": ident["source_path"],
                    "source_sha256": ident["source_sha256"],
                })
                count = ev.get("count", {}) or {}
                prev_balls = as_int(count.get("balls"), prev_balls)
                prev_strikes = as_int(count.get("strikes"), prev_strikes)
    return pd.DataFrame(pitch_rows), pd.DataFrame(pa_rows), pd.DataFrame(source_rows)


def support_class(n: int) -> str:
    if n >= 250:
        return "HIGH_PERSONAL_SUPPORT"
    if n >= 125:
        return "MODERATE_PERSONAL_SUPPORT"
    if n >= 40:
        return "LOW_PERSONAL_SUPPORT"
    if n > 0:
        return "PRIOR_DOMINATED"
    return "MISSING"


def shrink(raw: float, n: int, prior: float, k: int = 80) -> tuple[float, float, float]:
    if not math.isfinite(raw):
        raw = prior
    w = n / (n + k) if n + k else 0.0
    return float(raw * w + prior * (1 - w)), float(w), float(1 - w)


def poisson_two_plus(lam: float) -> float:
    lam = max(float(lam), 0.0001)
    p0 = math.exp(-lam)
    p1 = lam * p0
    return float(max(0.0, 1 - p0 - p1))


def poisson_bins(lam: float) -> tuple[float, float, float, float, float]:
    lam = max(float(lam), 0.0001)
    p0 = math.exp(-lam)
    p1 = lam * p0
    p2 = lam**2 / 2 * p0
    p3 = lam**3 / 6 * p0
    p4 = max(0.0, 1 - p0 - p1 - p2 - p3)
    s = p0 + p1 + p2 + p3 + p4
    return p0 / s, p1 / s, p2 / s, p3 / s, p4 / s


def build_pa_targets(pa: pd.DataFrame) -> pd.DataFrame:
    if pa.empty:
        return pd.DataFrame()
    g = pa.groupby(["game_date", "game_id", "batter_id"], dropna=False).agg(
        terminal_contact_count=("hit_capable_contact", "count"),
        hit_capable_contact_count=("hit_capable_contact", "sum"),
        starter_facing_contact_count=("hit_capable_contact", lambda s: int(s[pa.loc[s.index, "starter_reliever_role"].eq("STARTER_FACING_PA")].sum())),
        bullpen_facing_contact_count=("hit_capable_contact", lambda s: int(s[pa.loc[s.index, "starter_reliever_role"].eq("RELIEVER_FACING_PA")].sum())),
        pa_ending_in_contact=("hit_capable_contact", "sum"),
        pa_ending_in_strikeout=("strikeout", "sum"),
        pa_ending_in_walk_hbp=("walk_hbp", "sum"),
        total_pa=("hit_capable_contact", "count"),
    ).reset_index().rename(columns={"batter_id": "player_id"})
    for n in [2, 3, 4]:
        g[f"contact_count_ge{n}"] = (g["hit_capable_contact_count"] >= n).astype(int)
    g["player_game_key"] = g["game_date"].astype(str) + "|" + g["game_id"].astype(str) + "|" + g["player_id"].astype(str)
    return g


def rate(numer: pd.Series, denom: pd.Series) -> pd.Series:
    return (numer / denom.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


def summarize_group(pitches: pd.DataFrame, pa: pd.DataFrame, group_col: str, prior: dict[str, float]) -> dict[str, dict[str, Any]]:
    p = pitches.copy()
    pa_g = pa.copy()
    out: dict[str, dict[str, Any]] = {}
    if group_col not in p.columns:
        p[group_col] = ""
    if group_col not in pa_g.columns:
        pa_g[group_col] = ""
    if not p.empty:
        p["_group_key"] = p[group_col].astype(str)
        p["_zone_pitch"] = p["zone_state"].eq("in_zone").astype(int)
        p["_out_zone_pitch"] = p["zone_state"].eq("out_of_zone").astype(int)
        p["_zone_swing"] = (p["zone_state"].eq("in_zone") & p["swing"].eq(1)).astype(int)
        p["_out_zone_swing"] = (p["zone_state"].eq("out_of_zone") & p["swing"].eq(1)).astype(int)
        p["_zone_contact"] = (p["zone_state"].eq("in_zone") & p["contact"].eq(1)).astype(int)
        p["_out_zone_contact"] = (p["zone_state"].eq("out_of_zone") & p["contact"].eq(1)).astype(int)
        p["_two_strike_pitch"] = p["strikes_before_pitch"].ge(2).astype(int)
        p["_two_strike_contact"] = (p["strikes_before_pitch"].ge(2) & p["contact"].eq(1)).astype(int)
        pitch_agg = p.groupby("_group_key", dropna=False).agg(
            pitch_sample=("swing", "count"),
            swing=("swing", "sum"),
            zone_pitch=("_zone_pitch", "sum"),
            out_zone_pitch=("_out_zone_pitch", "sum"),
            zone_swing=("_zone_swing", "sum"),
            out_zone_swing=("_out_zone_swing", "sum"),
            contact=("contact", "sum"),
            zone_contact=("_zone_contact", "sum"),
            out_zone_contact=("_out_zone_contact", "sum"),
            whiff=("whiff", "sum"),
            foul=("foul", "sum"),
            ball_in_play=("ball_in_play", "sum"),
            two_strike_pitch=("_two_strike_pitch", "sum"),
            two_strike_contact=("_two_strike_contact", "sum"),
        )
    else:
        pitch_agg = pd.DataFrame()
    if not pa_g.empty:
        pa_g["_group_key"] = pa_g[group_col].astype(str)
        pa_agg = pa_g.groupby("_group_key", dropna=False).agg(
            pa_sample=("hit_capable_contact", "count"),
            hit_capable_contact=("hit_capable_contact", "sum"),
            strikeout=("strikeout", "sum"),
            walk_hbp=("walk_hbp", "sum"),
        )
    else:
        pa_agg = pd.DataFrame()
    keys = sorted(set(pitch_agg.index.astype(str)).union(set(pa_agg.index.astype(str))))
    for key in keys:
        pp = pitch_agg.loc[key].to_dict() if key in pitch_agg.index else {}
        aa = pa_agg.loc[key].to_dict() if key in pa_agg.index else {}
        pitch_n = int(pp.get("pitch_sample", 0) or 0)
        swings = int(pp.get("swing", 0) or 0)
        zone_n = int(pp.get("zone_pitch", 0) or 0)
        out_zone_n = int(pp.get("out_zone_pitch", 0) or 0)
        zone_swings = int(pp.get("zone_swing", 0) or 0)
        out_swings = int(pp.get("out_zone_swing", 0) or 0)
        contacts = int(pp.get("contact", 0) or 0)
        hcc = int(aa.get("hit_capable_contact", 0) or 0)
        pa_n = int(aa.get("pa_sample", 0) or 0)
        raw = {
            "swing_rate": swings / max(pitch_n, 1) if pitch_n else np.nan,
            "zone_swing_rate": zone_swings / max(zone_n, 1) if pitch_n else np.nan,
            "chase_rate": out_swings / max(out_zone_n, 1) if pitch_n else np.nan,
            "contact_rate": contacts / max(swings, 1) if pitch_n else np.nan,
            "zone_contact_rate": int(pp.get("zone_contact", 0) or 0) / max(zone_swings, 1) if pitch_n else np.nan,
            "out_zone_contact_rate": int(pp.get("out_zone_contact", 0) or 0) / max(out_swings, 1) if pitch_n else np.nan,
            "whiff_rate": int(pp.get("whiff", 0) or 0) / max(swings, 1) if pitch_n else np.nan,
            "foul_rate": int(pp.get("foul", 0) or 0) / max(swings, 1) if pitch_n else np.nan,
            "ball_in_play_rate_per_swing": int(pp.get("ball_in_play", 0) or 0) / max(swings, 1) if pitch_n else np.nan,
            "ball_in_play_rate_per_pa": int(pp.get("ball_in_play", 0) or 0) / max(pa_n, 1) if pa_n else np.nan,
            "hit_capable_contact_per_pa": hcc / max(pa_n, 1) if pa_n else np.nan,
            "strikeout_rate": int(aa.get("strikeout", 0) or 0) / max(pa_n, 1) if pa_n else np.nan,
            "walk_hbp_rate": int(aa.get("walk_hbp", 0) or 0) / max(pa_n, 1) if pa_n else np.nan,
            "pitches_per_pa": pitch_n / max(pa_n, 1) if pa_n else np.nan,
            "two_strike_contact_rate": int(pp.get("two_strike_contact", 0) or 0) / max(int(pp.get("two_strike_pitch", 0) or 0), 1) if pitch_n else np.nan,
        }
        row: dict[str, Any] = {"entity_id": key, "pitch_sample": pitch_n, "pa_sample": pa_n, "evidence_class": support_class(pitch_n)}
        for name, val in raw.items():
            sample = pa_n if name in {"ball_in_play_rate_per_pa", "hit_capable_contact_per_pa", "strikeout_rate", "walk_hbp_rate", "pitches_per_pa"} else pitch_n
            shr, w, prior_w = shrink(float(val) if pd.notna(val) else np.nan, sample, prior.get(name, 0.0))
            row[f"{name}_raw"] = val
            row[f"{name}_shrunk"] = shr
            row[f"{name}_sample"] = sample
            row[f"{name}_personal_weight"] = w
            row[f"{name}_prior_contribution"] = prior_w
        out[key] = row
    return out


def global_priors(pitches: pd.DataFrame, pa: pd.DataFrame) -> dict[str, float]:
    return summarize_group(pitches.assign(_all="all"), pa.assign(_all="all"), "_all", {})["all"] | {}


def prior_rates(pitches: pd.DataFrame, pa: pd.DataFrame) -> dict[str, float]:
    swings = max(int(pitches["swing"].sum()), 1)
    zone = pitches["zone_state"].eq("in_zone")
    out_zone = pitches["zone_state"].eq("out_of_zone")
    pa_n = max(len(pa), 1)
    return {
        "swing_rate": float(pitches["swing"].mean()) if len(pitches) else 0.45,
        "zone_swing_rate": int(pitches.loc[zone, "swing"].sum()) / max(int(zone.sum()), 1),
        "chase_rate": int(pitches.loc[out_zone, "swing"].sum()) / max(int(out_zone.sum()), 1),
        "contact_rate": int(pitches["contact"].sum()) / swings,
        "zone_contact_rate": int(pitches.loc[zone, "contact"].sum()) / max(int(pitches.loc[zone, "swing"].sum()), 1),
        "out_zone_contact_rate": int(pitches.loc[out_zone, "contact"].sum()) / max(int(pitches.loc[out_zone, "swing"].sum()), 1),
        "whiff_rate": int(pitches["whiff"].sum()) / swings,
        "foul_rate": int(pitches["foul"].sum()) / swings,
        "ball_in_play_rate_per_swing": int(pitches["ball_in_play"].sum()) / swings,
        "ball_in_play_rate_per_pa": int(pitches["ball_in_play"].sum()) / pa_n,
        "hit_capable_contact_per_pa": int(pa["hit_capable_contact"].sum()) / pa_n,
        "strikeout_rate": int(pa["strikeout"].sum()) / pa_n,
        "walk_hbp_rate": int(pa["walk_hbp"].sum()) / pa_n,
        "pitches_per_pa": len(pitches) / pa_n,
        "two_strike_contact_rate": int(pitches.loc[pitches["strikes_before_pitch"].ge(2), "contact"].sum()) / max(int(pitches["strikes_before_pitch"].ge(2).sum()), 1),
    }


def build_profiles_for_pop(pop: pd.DataFrame, pitches: pd.DataFrame, pa: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows = []
    hitter_rows = []
    pitcher_rows = []
    bullpen_rows = []
    pop = pop.copy()
    pop["slate_date_dt"] = pd.to_datetime(pop["slate_date"], errors="coerce")
    pop["player_key"] = pd.to_numeric(pop["player_id"], errors="coerce").astype("Int64").astype(str)
    pop["starter_key"] = pd.to_numeric(pop["opposing_starter_id"], errors="coerce").astype("Int64").astype(str)
    pitches["game_date_dt"] = pd.to_datetime(pitches["game_date"], errors="coerce")
    pa["game_date_dt"] = pd.to_datetime(pa["game_date"], errors="coerce")
    for date, day in pop.groupby("slate_date_dt", dropna=False):
        prior_p = pitches[pitches["game_date_dt"] < date].copy()
        prior_pa = pa[pa["game_date_dt"] < date].copy()
        pri = prior_rates(prior_p, prior_pa) if len(prior_pa) else prior_rates(pitches.iloc[0:0], pa.iloc[0:0])
        hitter = summarize_group(prior_p.rename(columns={"batter_id": "entity"}), prior_pa.rename(columns={"batter_id": "entity"}), "entity", pri)
        starter_p = prior_p[prior_p["starter_reliever_role"].eq("STARTER_FACING_PA")].rename(columns={"pitcher_id": "entity"})
        starter_pa = prior_pa[prior_pa["starter_reliever_role"].eq("STARTER_FACING_PA")].rename(columns={"pitcher_id": "entity"})
        pitcher = summarize_group(starter_p, starter_pa, "entity", pri)
        bullpen_p = prior_p[prior_p["starter_reliever_role"].eq("RELIEVER_FACING_PA")].copy()
        bullpen_pa = prior_pa[prior_pa["starter_reliever_role"].eq("RELIEVER_FACING_PA")].copy()
        bullpen = summarize_group(bullpen_p.assign(entity=bullpen_p["pitcher_team"]), bullpen_pa.assign(entity=bullpen_pa["pitcher_team"]), "entity", pri)
        for _, r in day.iterrows():
            hk = str(r["player_key"])
            pk = str(r["starter_key"])
            bteam = str(r.get("opposing_starter_team", ""))
            hp = hitter.get(hk, {})
            sp = pitcher.get(pk, {})
            bp = bullpen.get(bteam, {})
            h_rate = float(hp.get("hit_capable_contact_per_pa_shrunk", pri["hit_capable_contact_per_pa"]))
            s_rate = float(sp.get("hit_capable_contact_per_pa_shrunk", pri["hit_capable_contact_per_pa"]))
            b_rate = float(bp.get("hit_capable_contact_per_pa_shrunk", pri["hit_capable_contact_per_pa"]))
            h_w = float(hp.get("hit_capable_contact_per_pa_personal_weight", 0))
            s_w = float(sp.get("hit_capable_contact_per_pa_personal_weight", 0))
            b_w = float(bp.get("hit_capable_contact_per_pa_personal_weight", 0))
            hs_rate = (h_rate * (h_w + .001) + s_rate * (s_w + .001)) / (h_w + s_w + .002)
            hb_rate = (h_rate * (h_w + .001) + b_rate * (b_w + .001)) / (h_w + b_w + .002)
            rows.append({
                "player_game_key": r["player_game_key"],
                "discipline_global_hcc_per_pa_prior": pri["hit_capable_contact_per_pa"],
                "hitter_pitch_sample": hp.get("pitch_sample", 0),
                "starter_pitch_sample": sp.get("pitch_sample", 0),
                "bullpen_pitch_sample": bp.get("pitch_sample", 0),
                "hitter_discipline_evidence_class": hp.get("evidence_class", "MISSING"),
                "starter_discipline_evidence_class": sp.get("evidence_class", "MISSING"),
                "bullpen_discipline_evidence_class": bp.get("evidence_class", "MISSING"),
                "hitter_hcc_per_pa": h_rate,
                "starter_hcc_allowed_per_pa": s_rate,
                "bullpen_hcc_allowed_per_pa": b_rate,
                "hitter_x_starter_hcc_per_pa": hs_rate,
                "hitter_x_bullpen_hcc_per_pa": hb_rate,
                "hitter_swing_rate": hp.get("swing_rate_shrunk", pri["swing_rate"]),
                "hitter_chase_rate": hp.get("chase_rate_shrunk", pri["chase_rate"]),
                "hitter_contact_rate": hp.get("contact_rate_shrunk", pri["contact_rate"]),
                "hitter_whiff_rate": hp.get("whiff_rate_shrunk", pri["whiff_rate"]),
                "hitter_strikeout_rate": hp.get("strikeout_rate_shrunk", pri["strikeout_rate"]),
                "hitter_walk_hbp_rate": hp.get("walk_hbp_rate_shrunk", pri["walk_hbp_rate"]),
                "starter_whiff_rate_allowed": sp.get("whiff_rate_shrunk", pri["whiff_rate"]),
                "starter_contact_allowed_rate": sp.get("contact_rate_shrunk", pri["contact_rate"]),
                "starter_pitches_per_pa": sp.get("pitches_per_pa_shrunk", pri["pitches_per_pa"]),
            })
            hitter_rows.append({"player_game_key": r["player_game_key"], **hp})
            pitcher_rows.append({"player_game_key": r["player_game_key"], **sp})
            bullpen_rows.append({"player_game_key": r["player_game_key"], "entity_id": bteam, **bp})
    return (
        pop.merge(pd.DataFrame(rows), on="player_game_key", how="left"),
        pd.DataFrame(hitter_rows),
        pd.DataFrame(pitcher_rows),
        pd.DataFrame(bullpen_rows),
    )


def apply_instruments(df: pd.DataFrame, hit_conv: float) -> pd.DataFrame:
    out = df.copy()
    pred_total_pa = pd.to_numeric(out["prior_pred_total_pa"], errors="coerce").fillna(pd.to_numeric(out["pred_total_pa"], errors="coerce")).fillna(4.0)
    starter_pa = pd.to_numeric(out["turnover_starter_pa"], errors="coerce").fillna(pd.to_numeric(out["prior_pred_starter_pa"], errors="coerce")).fillna(2.4)
    bullpen_pa = pd.to_numeric(out["turnover_bullpen_pa"], errors="coerce").fillna(pd.to_numeric(out["prior_pred_bullpen_pa"], errors="coerce")).fillna(1.6)
    out["discipline_hitter_pred_hcc_count"] = pred_total_pa * out["hitter_hcc_per_pa"]
    out["discipline_hitter_starter_pred_hcc_count"] = starter_pa * out["hitter_x_starter_hcc_per_pa"] + bullpen_pa * out["hitter_hcc_per_pa"]
    out["discipline_source_aware_pred_hcc_count"] = starter_pa * out["hitter_x_starter_hcc_per_pa"] + bullpen_pa * out["hitter_x_bullpen_hcc_per_pa"]
    out["discipline_pa_state_pred_hcc_count"] = out["discipline_source_aware_pred_hcc_count"] * (1 + 0.08 * (out["hitter_contact_rate"] - out["discipline_global_hcc_per_pa_prior"])).clip(0.85, 1.15)
    out["discipline_unified_pred_hcc_count"] = out["discipline_pa_state_pred_hcc_count"]
    out["oracle_source_pred_hcc_count"] = pd.to_numeric(out["actual_starter_facing_pa"], errors="coerce").fillna(0) * out["hitter_x_starter_hcc_per_pa"] + pd.to_numeric(out["actual_bullpen_facing_pa"], errors="coerce").fillna(0) * out["hitter_x_bullpen_hcc_per_pa"]
    out["oracle_contact_count_pred_hcc_count"] = pd.to_numeric(out["hit_capable_contact_count"], errors="coerce").fillna(0)
    for name in [
        "discipline_hitter", "discipline_hitter_starter", "discipline_source_aware",
        "discipline_pa_state", "discipline_unified", "oracle_source", "oracle_contact_count",
    ]:
        count_col = f"{name}_pred_hcc_count"
        for n in [2, 3, 4]:
            out[f"{name}_contact_count_ge{n}"] = 1 - sum(math.exp(-lam) * lam**k / math.factorial(k) for k in range(n) for lam in [max(float(x), .0001)] for _ in [0]) if False else 0
        vals = [poisson_bins(v) for v in out[count_col]]
        out[f"{name}_contact_p0"] = [v[0] for v in vals]
        out[f"{name}_contact_p1"] = [v[1] for v in vals]
        out[f"{name}_contact_p2"] = [v[2] for v in vals]
        out[f"{name}_contact_p3"] = [v[3] for v in vals]
        out[f"{name}_contact_p4p"] = [v[4] for v in vals]
        out[f"{name}_contact_count_ge2"] = 1 - out[f"{name}_contact_p0"] - out[f"{name}_contact_p1"]
        out[f"{name}_contact_count_ge3"] = out[f"{name}_contact_p3"] + out[f"{name}_contact_p4p"]
        out[f"{name}_contact_count_ge4"] = out[f"{name}_contact_p4p"]
        out[f"{name}_p_two_plus_hits"] = [poisson_two_plus(v * hit_conv) for v in out[count_col]]
    return out


def binary_metric(y: pd.Series, p: pd.Series, split: str, instrument: str, target: str) -> dict[str, Any]:
    yy = y.astype(int).to_numpy()
    pp = np.clip(pd.to_numeric(p, errors="coerce").fillna(float(pd.to_numeric(p, errors="coerce").mean())).to_numpy(), EPS, 1 - EPS)
    out = {
        "temporal_split": split, "instrument": instrument, "target": target,
        "rows": int(len(yy)), "positives": int(yy.sum()),
        "observed_rate": float(yy.mean()) if len(yy) else "",
        "avg_predicted": float(pp.mean()) if len(pp) else "",
        "brier": float(np.mean((pp - yy) ** 2)) if len(yy) else "",
        "log_loss": float(log_loss(yy, pp, labels=[0, 1])) if len(yy) else "",
        "auc": float(roc_auc_score(yy, pp)) if len(set(yy)) > 1 else "",
    }
    try:
        x = np.log(pp / (1 - pp))
        slope, intercept = np.polyfit(x, yy, 1)
        out["calibration_slope"] = float(slope)
        out["calibration_intercept"] = float(intercept)
    except Exception:
        out["calibration_slope"] = ""
        out["calibration_intercept"] = ""
    return out


def expected_calibration_error(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0, 1, bins + 1)
    ece = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (p >= lo) & (p < hi if hi < 1 else p <= hi)
        if mask.any():
            ece += float(mask.mean()) * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(ece)


def one_to_two_metric(df: pd.DataFrame, prob_col: str, instrument: str, split: str) -> dict[str, Any]:
    g = df[(df["temporal_split"].eq(split)) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
    out = binary_metric(g["two_plus_binary"], g[prob_col], split, instrument, "two_plus_among_exactly_one_or_two_plus")
    out["wins_two_plus"] = int(g["two_plus_binary"].sum())
    out["losses_exactly_one"] = int(len(g) - g["two_plus_binary"].sum())
    out["ece"] = expected_calibration_error(g["two_plus_binary"].astype(int).to_numpy(), np.clip(pd.to_numeric(g[prob_col], errors="coerce").fillna(pd.to_numeric(g[prob_col], errors="coerce").mean()).to_numpy(), EPS, 1 - EPS)) if len(g) else ""
    return out


def count_validation(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    instruments = {
        "prior_contact_count_control": "pred_hit_capable_contact_count_c",
        "discipline_hitter": "discipline_hitter_pred_hcc_count",
        "discipline_hitter_starter": "discipline_hitter_starter_pred_hcc_count",
        "discipline_source_aware": "discipline_source_aware_pred_hcc_count",
        "discipline_unified": "discipline_unified_pred_hcc_count",
        "oracle_source": "oracle_source_pred_hcc_count",
    }
    for split in ["validation", "holdout"]:
        g = df[(df["temporal_split"].eq(split)) & (df["confirmatory_contact_eval"] == True)].copy()
        actual = pd.to_numeric(g["hit_capable_contact_count"], errors="coerce")
        for name, col in instruments.items():
            err = pd.to_numeric(g[col], errors="coerce") - actual
            rows.append({"temporal_split": split, "instrument": name, "target": "hit_capable_contact_count", "rows": int(err.notna().sum()), "mae": float(err.abs().mean()), "rmse": float(np.sqrt((err**2).mean())), "bias": float(err.mean()), "median_absolute_error": float(err.abs().median())})
            for n in [2, 3, 4]:
                prob_col = f"{name}_contact_count_ge{n}"
                if prob_col in g.columns:
                    pred_prob = g[prob_col]
                else:
                    pred_prob = pd.to_numeric(g[col], errors="coerce").fillna(0).map(
                        lambda lam, threshold=n: 1 - sum(math.exp(-max(float(lam), .0001)) * max(float(lam), .0001) ** k / math.factorial(k) for k in range(threshold))
                    )
                rows.append(binary_metric((actual >= n).astype(int), pred_prob, split, name, f"contact_count_ge{n}"))
        for name, actual_col, pred_col in [
            ("starter_source_count", "actual_starter_facing_pa", "discipline_source_aware_pred_hcc_count"),
            ("bullpen_source_count", "actual_bullpen_facing_pa", "discipline_source_aware_pred_hcc_count"),
        ]:
            rows.append({"temporal_split": split, "instrument": name, "target": actual_col, "rows": len(g), "mae": "", "rmse": "", "bias": "", "median_absolute_error": "", "notes": "source-specific PA counts retained from frozen exposure; contact count is not separately observed by source in this pilot package"})
    return pd.DataFrame(rows)


def one_to_two_results(df: pd.DataFrame) -> pd.DataFrame:
    instruments = {
        "exposure_control": "prior_predicted_exposure_p_two_plus_hits",
        "prior_contact_count_control": "source_aware_contact_challenger_p_two_plus_hits",
        "discipline_hitter": "discipline_hitter_p_two_plus_hits",
        "discipline_hitter_starter": "discipline_hitter_starter_p_two_plus_hits",
        "discipline_source_aware": "discipline_source_aware_p_two_plus_hits",
        "discipline_pa_state": "discipline_pa_state_p_two_plus_hits",
        "discipline_unified": "discipline_unified_p_two_plus_hits",
        "oracle_source": "oracle_source_p_two_plus_hits",
        "oracle_contact_count": "oracle_contact_count_p_two_plus_hits",
    }
    rows = []
    for split in ["validation", "holdout"]:
        for name, col in instruments.items():
            rows.append(one_to_two_metric(df, col, name, split))
    return pd.DataFrame(rows)


def probability_bands(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    fit = df[(df["temporal_split"].eq("fit")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)]["discipline_unified_p_two_plus_hits"].dropna()
    if fit.empty:
        return pd.DataFrame()
    edges = sorted(set([float("-inf"), *np.quantile(fit, [.25, .5, .75]).tolist(), float("inf")]))
    labels = [f"fit_q{i+1}" for i in range(len(edges) - 1)]
    for split in ["validation", "holdout"]:
        g = df[(df["temporal_split"].eq(split)) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
        g["band"] = pd.cut(g["discipline_unified_p_two_plus_hits"], edges, labels=labels, include_lowest=True)
        for band, b in g.groupby("band", observed=True):
            rows.append({"temporal_split": split, "instrument": "discipline_unified", "frozen_probability_band": str(band), "rows": len(b), "observed_two_plus_rate": float(b["two_plus_binary"].mean()), "avg_predicted_two_plus": float(b["discipline_unified_p_two_plus_hits"].mean())})
    return pd.DataFrame(rows)


def bootstrap(df: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(RNG_SEED)
    hold = df[(df["temporal_split"].eq("holdout")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
    rows = []
    for name, col in {"exposure_control": "prior_predicted_exposure_p_two_plus_hits", "discipline_unified": "discipline_unified_p_two_plus_hits"}.items():
        briers, aucs = [], []
        for _ in range(250):
            s = hold.sample(len(hold), replace=True, random_state=int(rng.integers(0, 2**31 - 1)))
            y = s["two_plus_binary"].astype(int).to_numpy()
            p = np.clip(s[col].astype(float).to_numpy(), EPS, 1 - EPS)
            briers.append(float(np.mean((p - y) ** 2)))
            aucs.append(float(roc_auc_score(y, p)) if len(set(y)) > 1 else np.nan)
        rows.append({"instrument": name, "brier_p05": float(np.nanquantile(briers, .05)), "brier_p50": float(np.nanquantile(briers, .5)), "brier_p95": float(np.nanquantile(briers, .95)), "auc_p05": float(np.nanquantile(aucs, .05)), "auc_p50": float(np.nanquantile(aucs, .5)), "auc_p95": float(np.nanquantile(aucs, .95))})
    return pd.DataFrame(rows)


def suppression(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["validation", "holdout"]:
        g = df[(df["temporal_split"].eq(split)) & df["suppression_subtype"].notna() & (df["confirmatory_contact_eval"] == True)]
        rows.append({"temporal_split": split, "rows": len(g), "avg_pred_pa": float(pd.to_numeric(g["prior_pred_total_pa"], errors="coerce").mean()) if len(g) else "", "avg_pred_contact_count": float(g["discipline_unified_pred_hcc_count"].mean()) if len(g) else "", "avg_pred_strikeout_rate": float(g["hitter_strikeout_rate"].mean()) if len(g) else "", "avg_non_contact_rate": float(1 - g["hitter_hcc_per_pa"].mean()) if len(g) else "", "avg_pred_two_plus": float(g["discipline_unified_p_two_plus_hits"].mean()) if len(g) else "", "observed_two_plus_rate": float(g["two_plus_binary"].mean()) if len(g) else "", "suppression_preserved": bool(g["discipline_unified_p_two_plus_hits"].mean() < .30) if len(g) else ""})
    return pd.DataFrame(rows)


def roster_relative(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    hold = df[(df["temporal_split"].eq("holdout")) & (df["confirmatory_contact_eval"] == True)].copy()
    for instrument, col in [
        ("exposure_control", "prior_predicted_exposure_p_two_plus_hits"),
        ("prior_contact_count_control", "source_aware_contact_challenger_p_two_plus_hits"),
        ("discipline_unified", "discipline_unified_p_two_plus_hits"),
        ("expected_pa", "prior_pred_total_pa"),
    ]:
        for (game_id, starter), g in hold.groupby(["game_id", "opposing_starter_id"], dropna=False):
            if len(g) < 4:
                continue
            pred = g.sort_values(col, ascending=False).iloc[0]
            actual = g.sort_values("hit_capable_contact_count", ascending=False).iloc[0]
            pairs = correct_contact = ot_pairs = ot_correct = 0
            gg = g[[col, "hit_capable_contact_count", "two_plus_binary", "one_to_two_population"]].dropna().reset_index(drop=True)
            for i in range(len(gg)):
                for j in range(i + 1, len(gg)):
                    if gg.loc[i, "hit_capable_contact_count"] != gg.loc[j, "hit_capable_contact_count"]:
                        pairs += 1
                        correct_contact += int((gg.loc[i, col] > gg.loc[j, col]) == (gg.loc[i, "hit_capable_contact_count"] > gg.loc[j, "hit_capable_contact_count"]))
                    if bool(gg.loc[i, "one_to_two_population"]) and bool(gg.loc[j, "one_to_two_population"]) and gg.loc[i, "two_plus_binary"] != gg.loc[j, "two_plus_binary"]:
                        ot_pairs += 1
                        ot_correct += int((gg.loc[i, col] > gg.loc[j, col]) == (gg.loc[i, "two_plus_binary"] > gg.loc[j, "two_plus_binary"]))
            rows.append({"instrument": instrument, "game_id": game_id, "opposing_starter_id": starter, "hitters": len(g), "top_predicted_vs_top_actual_contact_agreement": pred["player_game_key"] == actual["player_game_key"], "pairwise_contact_ordering_accuracy": correct_contact / pairs if pairs else "", "one_to_two_pairwise_accuracy": ot_correct / ot_pairs if ot_pairs else "", "pairwise_contact_pairs": pairs, "one_to_two_pairs": ot_pairs})
    return pd.DataFrame(rows)


def second_hit_source(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    two = df[df["outcome_class"].eq("TWO_OR_MORE_HITS") & df["two_plus_hit_source_class"].notna() & (df["confirmatory_contact_eval"] == True)]
    for split in ["validation", "holdout"]:
        for cls, g in two[two["temporal_split"].eq(split)].groupby("two_plus_hit_source_class"):
            rows.append({"temporal_split": split, "second_hit_source": cls, "rows": len(g), "avg_pred_contact_count": float(g["discipline_unified_pred_hcc_count"].mean()), "avg_pred_two_plus": float(g["discipline_unified_p_two_plus_hits"].mean()), "observed_two_plus_rate": 1.0})
    return pd.DataFrame(rows)


def plus200(df: pd.DataFrame) -> pd.DataFrame:
    price = read_csv(LONG_PRICE)
    target = price[price["price_band"].eq("+200_through_+249")].copy() if not price.empty else pd.DataFrame()
    if target.empty:
        return pd.DataFrame()
    m = target.merge(df, on="player_game_key", how="left", suffixes=("_price", ""))
    rows = []
    for split, g in m.groupby("temporal_split", dropna=False):
        rows.append({"temporal_split": split, "rows": len(g), "avg_pred_repeated_contact_ge2": float(g["discipline_unified_contact_count_ge2"].mean()), "avg_pred_contact_count": float(g["discipline_unified_pred_hcc_count"].mean()), "avg_pred_two_plus": float(g["discipline_unified_p_two_plus_hits"].mean()), "observed_two_plus_rate": float(g["outcome_class"].eq("TWO_OR_MORE_HITS").mean()), "avg_implied_break_even": float(pd.to_numeric(g.get("market_implied_break_even_probability", pd.Series(np.nan, index=g.index)), errors="coerce").mean()), "diagnostic_roi": float(pd.to_numeric(g.get("profit_1u_diagnostic", pd.Series(np.nan, index=g.index)), errors="coerce").mean()), "timing_certification": "|".join(sorted(g.get("selection_time_timing_certification", pd.Series(dtype=object)).dropna().astype(str).unique()))})
    return pd.DataFrame(rows)


def date_stability(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for date, g in df[(df["temporal_split"].eq("holdout")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].groupby("slate_date"):
        rows.append({"slate_date": date, "rows": len(g), "observed_two_plus_rate": float(g["two_plus_binary"].mean()), "avg_predicted": float(g["discipline_unified_p_two_plus_hits"].mean()), "brier": float(((g["discipline_unified_p_two_plus_hits"] - g["two_plus_binary"]) ** 2).mean()), "sample_flag": "SPARSE" if len(g) < 20 else "OK"})
    return pd.DataFrame(rows)


def concentration(df: pd.DataFrame) -> pd.DataFrame:
    hold = df[(df["temporal_split"].eq("holdout")) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)]
    if hold.empty:
        return pd.DataFrame()
    player = hold.groupby(["player_id", "player_name"]).size().reset_index(name="rows").sort_values("rows", ascending=False)
    pitcher = hold.groupby(["opposing_starter_id", "opposing_starter_name"]).size().reset_index(name="rows").sort_values("rows", ascending=False)
    return pd.DataFrame([
        {"dimension": "player", "top_identity": player.iloc[0]["player_name"], "top_rows": int(player.iloc[0]["rows"]), "top_share": float(player.iloc[0]["rows"] / len(hold))},
        {"dimension": "pitcher", "top_identity": pitcher.iloc[0]["opposing_starter_name"], "top_rows": int(pitcher.iloc[0]["rows"]), "top_share": float(pitcher.iloc[0]["rows"] / len(hold))},
    ])


def validation_report(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pop = read_csv(POP_PATH)
    game_ids = set(pop["game_id"].map(norm_id))
    feeds = load_feeds(RAW_FEEDS)
    pitch, pa, source_manifest = build_pitch_ledger(feeds, game_ids)
    pitch["game_date_dt"] = pd.to_datetime(pitch["game_date"], errors="coerce")
    pa["game_date_dt"] = pd.to_datetime(pa["game_date"], errors="coerce")
    targets = build_pa_targets(pa)
    hit_conv = float(read_csv(CONTACT_LEDGER).query("game_date <= '2026-06-11'")["official_hit"].mean())
    model, hitter_profiles, starter_profiles, bullpen_profiles = build_profiles_for_pop(pop, pitch, pa)
    model = model.merge(targets[["player_game_key", "hit_capable_contact_count", "contact_count_ge2", "contact_count_ge3", "contact_count_ge4"]], on="player_game_key", how="left", suffixes=("", "_pitch_ledger"))
    # Preserve the frozen contact pilot target if present; use pitch-ledger fill only for rows without it.
    if "hit_capable_contact_count_pitch_ledger" in model.columns:
        model["hit_capable_contact_count"] = pd.to_numeric(model["hit_capable_contact_count"], errors="coerce").fillna(pd.to_numeric(model["hit_capable_contact_count_pitch_ledger"], errors="coerce"))
    model = apply_instruments(model, hit_conv)

    contact_val = count_validation(model)
    one_two = one_to_two_results(model)
    bands = probability_bands(model)
    boot = bootstrap(model)
    suppress = suppression(model)
    roster = roster_relative(model)
    source = second_hit_source(model)
    plus = plus200(model)
    stability = date_stability(model)
    conc = concentration(model)
    semantics = pd.DataFrame([
        {"field": "pitch_type", "source": "playEvents.details.type.code/description", "status": "AVAILABLE", "notes": "used only as frozen pitch-family profile input"},
        {"field": "pitch_velocity", "source": "playEvents.pitchData.startSpeed", "status": "AVAILABLE", "notes": "retained in pitch ledger"},
        {"field": "zone", "source": "playEvents.pitchData.zone", "status": "AVAILABLE", "notes": "MLB zone 1-9 treated as in_zone; others out_of_zone; no custom coordinate chase inference"},
        {"field": "swing/contact/whiff/foul/BIP", "source": "playEvents.details.code and isInPlay", "status": "AVAILABLE_LOCAL_CONTRACT", "notes": "frozen code mapping documented in utility"},
        {"field": "terminal pitch / PA result", "source": "playEvents terminal index and play.result.eventType", "status": "AVAILABLE", "notes": "targets only, not current-game pregame features"},
    ])
    instruments = pd.DataFrame([
        {"instrument": "control_a_exposure", "definition": "prior_predicted_exposure_p_two_plus_hits unchanged"},
        {"instrument": "control_b_prior_contact_count", "definition": "source_aware_contact_challenger_p_two_plus_hits unchanged"},
        {"instrument": "instrument_a_hitter_discipline", "definition": "predicted PA * strict-prior hitter hit-capable-contact per PA"},
        {"instrument": "instrument_b_hitter_x_starter", "definition": "starter PA blend of hitter and starter contact-allowed plus hitter bullpen fallback"},
        {"instrument": "instrument_c_source_aware", "definition": "starter and bullpen predicted PA with strict-prior source rates"},
        {"instrument": "instrument_d_pa_state", "definition": "source-aware count with historical contact-rate discipline adjustment; no current-game count states"},
        {"instrument": "instrument_e_unified", "definition": "Poisson repeated-contact distribution translated through constant strict-prior contact-hit conversion"},
        {"instrument": "oracle_source", "definition": "actual starter/bullpen PA source with strict-prior profiles; nondeployable"},
        {"instrument": "oracle_contact_count", "definition": "actual hit-capable contact count; nondeployable"},
    ])
    support = model[["player_game_key", "hitter_pitch_sample", "starter_pitch_sample", "bullpen_pitch_sample", "hitter_discipline_evidence_class", "starter_discipline_evidence_class", "bullpen_discipline_evidence_class"]].copy()
    hold = one_two[one_two["temporal_split"].eq("holdout")].set_index("instrument")
    control_auc = float(hold.loc["exposure_control", "auc"])
    control_brier = float(hold.loc["exposure_control", "brier"])
    unified_auc = float(hold.loc["discipline_unified", "auc"])
    unified_brier = float(hold.loc["discipline_unified", "brier"])
    prior_auc = float(hold.loc["prior_contact_count_control", "auc"])
    oracle_auc = float(hold.loc["oracle_contact_count", "auc"])
    suppression_ok = bool(suppress[suppress["temporal_split"].eq("holdout")]["suppression_preserved"].iloc[0])
    if unified_brier < control_brier and unified_auc > control_auc:
        final_decision = "PITCH_DISCIPLINE_ADDS_REPEATED_CONTACT_VALUE"
    elif unified_auc > prior_auc and unified_brier < float(hold.loc["prior_contact_count_control", "brier"]):
        final_decision = "SOURCE_AWARE_CONTACT_FREQUENCY_ADDS_VALUE"
    elif oracle_auc > unified_auc + 0.08:
        final_decision = "REALIZED_SWING_EXECUTION_IS_PRIMARY_LIMITER"
    else:
        final_decision = "PREGAME_REPEATED_CONTACT_NOT_FORECASTABLE_WITH_LOCAL_DATA"
    if not suppression_ok:
        final_decision = "STOP_HITTER_OWNED_O15_CURRENT_SEASON_BRANCH"
    if final_decision not in {"PITCH_DISCIPLINE_ADDS_REPEATED_CONTACT_VALUE", "SOURCE_AWARE_CONTACT_FREQUENCY_ADDS_VALUE", "PITCHER_CONTACT_ALLOWED_ADDS_VALUE"}:
        next_decision = "STOP_HITTER_OWNED_O15_CURRENT_SEASON_BRANCH"
    else:
        next_decision = final_decision
    decisions = pd.DataFrame([
        ("MLB_DISCIPLINE_PITCH_LEDGER_DECISION", "CANONICAL_PITCH_DISCIPLINE_LEDGER_BUILT_FROM_LOCAL_STATSAPI_FEEDS"),
        ("MLB_DISCIPLINE_HITTER_PROFILE_DECISION", "STRICT_PRIOR_HITTER_DISCIPLINE_PROFILES_BUILT"),
        ("MLB_DISCIPLINE_PITCHER_PROFILE_DECISION", "STRICT_PRIOR_STARTER_AND_TEAM_BULLPEN_PROFILES_BUILT"),
        ("MLB_DISCIPLINE_CONTACT_TARGET_DECISION", "CONTACT_FREQUENCY_TARGETS_FROZEN_FROM_CANONICAL_PA_LEDGER"),
        ("MLB_DISCIPLINE_CONTACT_FORECAST_DECISION", "PREGAME_CONTACT_FREQUENCY_INSTRUMENTS_EVALUATED"),
        ("MLB_DISCIPLINE_SOURCE_AWARE_DECISION", "SOURCE_AWARE_STARTER_BULLPEN_CONTACT_FORECAST_EVALUATED"),
        ("MLB_DISCIPLINE_ORACLE_DIAGNOSTIC_DECISION", "ORACLE_SOURCE_AND_ACTUAL_CONTACT_COUNT_RETAINED_NONDEPLOYABLE"),
        ("MLB_DISCIPLINE_CONTACT_COUNT_HOLDOUT_DECISION", "CONTACT_COUNT_HOLDOUT_MEASURED"),
        ("MLB_DISCIPLINE_ONE_TO_TWO_PLUS_HOLDOUT_DECISION", final_decision),
        ("MLB_DISCIPLINE_SUPPRESSION_PRESERVATION_DECISION", "SUPPRESSION_PRESERVED" if suppression_ok else "SUPPRESSION_NOT_PRESERVED"),
        ("MLB_DISCIPLINE_ROSTER_RELATIVE_DECISION", "ROSTER_RELATIVE_DISCIPLINE_DIAGNOSTIC_RETAINED"),
        ("MLB_DISCIPLINE_SECOND_HIT_SOURCE_DECISION", "SECOND_HIT_SOURCE_DIAGNOSTIC_RETAINED_NO_SUBGROUP_SELECTED"),
        ("MLB_DISCIPLINE_PLUS200_DECISION", "PLUS200_REVALIDATED_DIAGNOSTIC_ONLY_NO_OPTIMIZATION"),
        ("MLB_DISCIPLINE_FINAL_LOCAL_BRANCH_DECISION", next_decision),
        ("MLB_DISCIPLINE_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ], columns=["decision", "value"])
    outputs = {
        "pitch_event_semantic_binding_2026-07-17.csv": semantics,
        "canonical_pitch_discipline_ledger_2026-07-17.csv": pitch,
        "canonical_pa_discipline_targets_2026-07-17.csv": pa,
        "hitter_discipline_profiles_2026-07-17.csv": hitter_profiles,
        "starter_discipline_profiles_2026-07-17.csv": starter_profiles,
        "bullpen_discipline_profiles_2026-07-17.csv": bullpen_profiles,
        "support_and_shrinkage_report_2026-07-17.csv": support,
        "frozen_contact_frequency_instruments_2026-07-17.csv": instruments,
        "oracle_diagnostics_2026-07-17.csv": one_two[one_two["instrument"].astype(str).str.startswith("oracle")],
        "contact_count_validation_2026-07-17.csv": contact_val,
        "one_to_two_plus_validation_holdout_metrics_2026-07-17.csv": one_two,
        "frozen_probability_band_progression_2026-07-17.csv": bands,
        "bootstrap_uncertainty_2026-07-17.csv": boot,
        "date_stability_2026-07-17.csv": stability,
        "hitter_pitcher_concentration_2026-07-17.csv": conc,
        "suppression_preservation_2026-07-17.csv": suppress,
        "roster_relative_results_2026-07-17.csv": roster,
        "second_hit_source_analysis_2026-07-17.csv": source,
        "frozen_plus200_evaluation_2026-07-17.csv": plus,
        "research_only_model_artifacts_2026-07-17.csv": model,
        "source_manifest_2026-07-17.csv": source_manifest,
        "required_decisions_2026-07-17.csv": decisions,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    direct = "No. The local pitch-discipline branch did not improve the frozen one-to-two-plus holdout comparison; local-data hitter-owned O1.5 current-season development has reached its current limit." if next_decision == "STOP_HITTER_OWNED_O15_CURRENT_SEASON_BRANCH" else "Yes. Strict-prior discipline/contact-frequency evidence improved the frozen holdout comparison, while remaining research-only."
    machine = {
        "generated_at_utc": now_utc(),
        "pitch_events": int(len(pitch)),
        "pa_rows": int(len(pa)),
        "hitter_profile_rows": int(len(hitter_profiles)),
        "starter_profile_rows": int(len(starter_profiles)),
        "bullpen_profile_rows": int(len(bullpen_profiles)),
        "holdout_control_brier": control_brier,
        "holdout_control_auc": control_auc,
        "holdout_prior_contact_count_auc": prior_auc,
        "holdout_discipline_unified_brier": unified_brier,
        "holdout_discipline_unified_auc": unified_auc,
        "holdout_oracle_contact_count_auc": oracle_auc,
        "suppression_preserved": suppression_ok,
        "final_branch_decision": next_decision,
        "direct_answer": direct,
        "production_status": "NOT_AUTHORIZED",
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_pitch_discipline_repeated_contact_pilot_2026-07-17.json")
    decision_lines = "\n".join(f"- `{r.decision} = {r.value}`" for r in decisions.itertuples(index=False))
    write_md(f"""# MLB Pregame Pitch-Discipline and Repeated-Contact Multi-Hit Pilot

Generated: `{machine['generated_at_utc']}`

## Executive Summary

This bounded pilot reconstructed a pitch-grain ledger from preserved local
StatsAPI feed/live JSON and built strict-prior hitter, Starter, and team
bullpen pitch-discipline/contact-frequency profiles.

The pilot did not create a new contact-quality surface and did not use
current-game pitch mix, count sequence, contact count, or outcomes as
legitimate pregame features.

## Coverage

| item | rows |
|---|---:|
| pitch events | {machine['pitch_events']} |
| PA rows | {machine['pa_rows']} |
| hitter profile rows | {machine['hitter_profile_rows']} |
| Starter profile rows | {machine['starter_profile_rows']} |
| bullpen profile rows | {machine['bullpen_profile_rows']} |

## One-to-Two-Plus Holdout

| instrument | brier | auc |
|---|---:|---:|
| exposure control | {machine['holdout_control_brier']:.6f} | {machine['holdout_control_auc']:.6f} |
| discipline unified | {machine['holdout_discipline_unified_brier']:.6f} | {machine['holdout_discipline_unified_auc']:.6f} |
| oracle actual contact count |  | {machine['holdout_oracle_contact_count_auc']:.6f} |

## Direct Answer

{direct}

## Decisions

{decision_lines}

## Production Status

`MLB_DISCIPLINE_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, candidate, selector, upload, Quick Card, workspace,
LaunchAgent, database, network, or OddsAPI behavior changed.
""", out_dir / "executive_summary_2026-07-17.md")
    manifest = []
    for path in [POP_PATH, CONTACT_LEDGER, LONG_PRICE, ROOT / "backend/mlb/scripts/run_mlb_pitch_discipline_repeated_contact_pilot.py"]:
        if path.exists():
            manifest.append({"artifact_role": "input", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    validation_report(out_dir)
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
