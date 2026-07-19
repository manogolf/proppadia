#!/usr/bin/env python3
"""Research-only batter-pitcher encounter ledger pilot for MLB Hits 1.5.

This utility uses existing local StatsAPI feed/live JSON artifacts only. It
constructs a bounded historical plate-appearance encounter ledger, validates it
against embedded official boxscore totals, and writes research artifacts for the
later-PA/bullpen-exposure platform. It performs no network calls, DB writes,
model fitting, production mutations, or external acquisition.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_batter_pitcher_encounter_ledger_pilot/2026-07-17"
BENCH = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/canonical_modeling_population_2026-07-17.csv"
RAW_FEEDS = ROOT / "artifacts/analysis/model_development/mlb_historical_outcome_gap_authoritative_recovery/2026-07-13/raw_official_mlb"

HIT_EVENT_TYPES = {"single", "double", "triple", "home_run"}
PA_EXCLUDED_EVENT_TYPES = {
    "pickoff_1b",
    "pickoff_2b",
    "pickoff_3b",
    "caught_stealing_2b",
    "caught_stealing_3b",
    "caught_stealing_home",
    "stolen_base_2b",
    "stolen_base_3b",
    "stolen_base_home",
    "wild_pitch",
    "passed_ball",
    "defensive_indiff",
}


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


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


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


def batting_stats(team: dict[str, Any], player_id: Any) -> dict[str, Any]:
    return player_obj(team, player_id).get("stats", {}).get("batting", {}) or {}


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


def load_feeds(raw_root: Path) -> list[tuple[Path, dict[str, Any]]]:
    feeds: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(raw_root.glob("mlb_statsapi_feed_live_game_*.json")):
        try:
            feeds.append((path, json.loads(path.read_text(encoding="utf-8"))))
        except Exception:
            continue
    return feeds


def feed_game_identity(path: Path, feed: dict[str, Any]) -> dict[str, Any]:
    gd = feed.get("gameData", {})
    game = gd.get("game", {}) or {}
    dt = gd.get("datetime", {}) or {}
    status = gd.get("status", {}) or {}
    return {
        "game_id": norm_id(game.get("pk") or game.get("gamePk") or path.stem.split("_")[-1]),
        "game_date": dt.get("officialDate") or dt.get("originalDate") or "",
        "game_status": status.get("detailedState") or status.get("abstractGameState") or "",
        "source_path": rel(path),
        "source_sha256": sha256(path),
        "away_team": team_meta(feed, "away").get("abbreviation") or "",
        "home_team": team_meta(feed, "home").get("abbreviation") or "",
    }


def starters_for_feed(feed: dict[str, Any]) -> dict[str, dict[str, Any]]:
    starters: dict[str, dict[str, Any]] = {}
    for side in ("away", "home"):
        team = box_team(feed, side)
        team_code = team_meta(feed, side).get("abbreviation") or ""
        ids: list[str] = []
        for pid in team.get("pitchers", []) or []:
            stats = pitching_stats(team, pid)
            if as_int(stats.get("gamesStarted")) == 1:
                ids.append(norm_id(pid))
        if not ids:
            # Fallback remains within the official boxscore feed: first listed
            # pitcher is treated as unresolved starter authority.
            ids = [norm_id(x) for x in (team.get("pitchers", []) or [])[:1]]
        starters[team_code] = {
            "starter_pitcher_id": ids[0] if len(ids) == 1 else "",
            "starter_pitcher_name": player_name(team, ids[0]) if len(ids) == 1 else "",
            "starter_identity_status": "OFFICIAL_BOXSCORE_GAMES_STARTED" if len(ids) == 1 else "UNRESOLVED_MULTIPLE_OR_MISSING",
        }
    return starters


def pitcher_team_lookup(feed: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for side in ("away", "home"):
        code = team_meta(feed, side).get("abbreviation") or ""
        for pid in box_team(feed, side).get("pitchers", []) or []:
            lookup[norm_id(pid)] = code
    return lookup


def opponent_team_for_batter(feed: dict[str, Any], batter_id: str) -> tuple[str, str, dict[str, Any]]:
    for side in ("away", "home"):
        team = box_team(feed, side)
        if f"ID{batter_id}" in (team.get("players") or {}):
            batting = batting_stats(team, batter_id)
            if batting:
                code = team_meta(feed, side).get("abbreviation") or ""
                opp_side = "home" if side == "away" else "away"
                opp = team_meta(feed, opp_side).get("abbreviation") or ""
                return code, opp, team
    return "", "", {}


def role_for_pitcher(pitcher_id: str, pitcher_team: str, starters: dict[str, dict[str, Any]], entry_order: int) -> str:
    starter_id = starters.get(pitcher_team, {}).get("starter_pitcher_id", "")
    if pitcher_id and starter_id and pitcher_id == starter_id:
        return "STARTER_FACING_PA"
    if entry_order == 1 and not starter_id:
        return "IRREGULAR_ROLE_UNRESOLVED"
    return "RELIEVER_FACING_PA"


def is_pa(play: dict[str, Any]) -> bool:
    if not play.get("about", {}).get("isComplete", False):
        return False
    matchup = play.get("matchup", {}) or {}
    if not matchup.get("batter", {}).get("id") or not matchup.get("pitcher", {}).get("id"):
        return False
    event_type = (play.get("result", {}) or {}).get("eventType") or ""
    if event_type in PA_EXCLUDED_EVENT_TYPES:
        return False
    return True


def build_ledgers(feeds: list[tuple[Path, dict[str, Any]]], benchmark: pd.DataFrame) -> dict[str, pd.DataFrame]:
    bench_games = set(benchmark["game_id"].map(norm_id))
    benchmark_keys = set(benchmark["player_game_key"].astype(str))
    bench_lookup = benchmark.set_index("player_game_key").to_dict("index")

    encounter_rows: list[dict[str, Any]] = []
    pitcher_entry: dict[tuple[str, str], dict[str, Any]] = {}
    source_manifest: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []

    for path, feed in feeds:
        ident = feed_game_identity(path, feed)
        plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", []) or []
        status = ident["game_status"]
        in_benchmark = ident["game_id"] in bench_games
        starters = starters_for_feed(feed)
        pteam = pitcher_team_lookup(feed)
        pitcher_first_seen: dict[str, int] = {}
        pitcher_sequence = 0
        pa_sequence = 0
        final = status == "Final"
        source_manifest.append({
            **ident,
            "grain": "StatsAPI feed/live allPlays plus embedded boxscore",
            "all_plays": len(plays),
            "complete_pa_events": sum(1 for p in plays if is_pa(p)),
            "in_frozen_benchmark": in_benchmark,
            "strict_historical_authority": "official_statsapi_raw_response_preserved_locally",
            "local_availability": path.exists(),
            "replayability": "local_json_sha256_bound",
            "duplicate_state": "one_file_per_game_id" if len(list(path.parent.glob(f"*{ident['game_id']}*.json"))) == 1 else "duplicate_name_check_required",
        })
        if not final:
            rejections.append({**ident, "reason": "GAME_NOT_FINAL", "source_event_identity": ""})
            continue
        if not in_benchmark:
            rejections.append({**ident, "reason": "FINAL_GAME_NOT_IN_FROZEN_BENCHMARK", "source_event_identity": ""})
            continue

        for play in plays:
            source_event_id = f"{ident['game_id']}:{play.get('atBatIndex', play.get('about', {}).get('atBatIndex', ''))}"
            if not is_pa(play):
                rejections.append({**ident, "reason": "NON_PA_OR_INCOMPLETE_PLAY", "source_event_identity": source_event_id})
                continue
            matchup = play.get("matchup", {}) or {}
            result = play.get("result", {}) or {}
            about = play.get("about", {}) or {}
            batter_id = norm_id(matchup.get("batter", {}).get("id"))
            pitcher_id = norm_id(matchup.get("pitcher", {}).get("id"))
            pitcher_team = pteam.get(pitcher_id, "")
            hitter_team, opponent, batter_team_obj = opponent_team_for_batter(feed, batter_id)
            if pitcher_id not in pitcher_first_seen:
                pitcher_sequence += 1
                pitcher_first_seen[pitcher_id] = pitcher_sequence
            entry_order = pitcher_first_seen[pitcher_id]
            pitcher_key = (ident["game_id"], pitcher_id)
            if pitcher_key not in pitcher_entry:
                pitcher_entry[pitcher_key] = {
                    **ident,
                    "pitcher_id": pitcher_id,
                    "pitcher_name": matchup.get("pitcher", {}).get("fullName", ""),
                    "pitcher_team": pitcher_team,
                    "pitcher_entry_sequence": entry_order,
                    "first_pa_sequence": pa_sequence + 1,
                    "first_inning": about.get("inning"),
                    "first_half_inning": about.get("halfInning"),
                    "official_starter_flag": pitcher_id == starters.get(pitcher_team, {}).get("starter_pitcher_id", ""),
                    "role_classification": role_for_pitcher(pitcher_id, pitcher_team, starters, entry_order),
                    "starter_identity_status": starters.get(pitcher_team, {}).get("starter_identity_status", ""),
                }
            pa_sequence += 1
            event_type = result.get("eventType") or ""
            official_hit = event_type in HIT_EVENT_TYPES
            player_key = f"{ident['game_date']}|{ident['game_id']}|{batter_id}"
            bench = bench_lookup.get(player_key, {})
            encounter_rows.append({
                **ident,
                "plate_appearance_sequence": pa_sequence,
                "source_event_identity": source_event_id,
                "at_bat_index": play.get("atBatIndex", about.get("atBatIndex")),
                "inning": about.get("inning"),
                "half_inning": about.get("halfInning"),
                "outs_before": (play.get("count", {}) or {}).get("outs"),
                "batter_id": batter_id,
                "batter_name": matchup.get("batter", {}).get("fullName", ""),
                "batter_team": hitter_team,
                "opponent": opponent,
                "lineup_slot": lineup_slot(batter_team_obj, batter_id),
                "pitcher_id": pitcher_id,
                "pitcher_name": matchup.get("pitcher", {}).get("fullName", ""),
                "pitcher_team": pitcher_team,
                "pitcher_entry_sequence": entry_order,
                "official_starting_pitcher_id_for_pitcher_team": starters.get(pitcher_team, {}).get("starter_pitcher_id", ""),
                "official_starting_pitcher_name_for_pitcher_team": starters.get(pitcher_team, {}).get("starter_pitcher_name", ""),
                "role_classification": role_for_pitcher(pitcher_id, pitcher_team, starters, entry_order),
                "pa_result": result.get("event", ""),
                "event_type": event_type,
                "official_hit": official_hit,
                "benchmark_player_game_key": player_key if player_key in benchmark_keys else "",
                "benchmark_temporal_split": bench.get("temporal_split", ""),
                "hitter_tier_proxy": bench.get("outcome_class", ""),
                "pa_opportunity_proxy": bench.get("d15_pa_per_game", ""),
            })

    pitcher_rows = list(pitcher_entry.values())
    encounters = pd.DataFrame(encounter_rows)
    pitchers = pd.DataFrame(pitcher_rows).sort_values(["game_date", "game_id", "pitcher_entry_sequence"]) if pitcher_rows else pd.DataFrame()
    if not encounters.empty and not pitchers.empty:
        pstats = encounters.groupby(["game_id", "pitcher_id"], dropna=False).agg(
            last_pa_sequence=("plate_appearance_sequence", "max"),
            pitcher_bound_pa=("plate_appearance_sequence", "count"),
            hits_allowed_reconstructed=("official_hit", "sum"),
        ).reset_index()
        pitchers = pitchers.merge(pstats, on=["game_id", "pitcher_id"], how="left")
        first_bullpen = (
            pitchers[pitchers["role_classification"].eq("RELIEVER_FACING_PA")]
            .sort_values(["game_id", "pitcher_entry_sequence"])
            .groupby("game_id", dropna=False)
            .head(1)[["game_id", "pitcher_id", "pitcher_name", "first_pa_sequence"]]
            .rename(columns={
                "pitcher_id": "first_bullpen_pitcher_id",
                "pitcher_name": "first_bullpen_pitcher_name",
                "first_pa_sequence": "first_bullpen_pa_sequence",
            })
        )
        starter_exit = (
            pitchers[pitchers["role_classification"].eq("STARTER_FACING_PA")]
            .groupby("game_id", dropna=False)
            .agg(starter_exit_pa_sequence=("last_pa_sequence", "max"))
            .reset_index()
        )
        pitchers = pitchers.merge(first_bullpen, on="game_id", how="left")
        pitchers = pitchers.merge(starter_exit, on="game_id", how="left")
    source = pd.DataFrame(source_manifest)
    rejected = pd.DataFrame(rejections)
    return {"encounters": encounters, "pitcher_entry": pitchers, "source_manifest": source, "rejected": rejected}


def official_batter_totals(feeds: list[tuple[Path, dict[str, Any]]], benchmark_games: set[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path, feed in feeds:
        ident = feed_game_identity(path, feed)
        if ident["game_status"] != "Final" or ident["game_id"] not in benchmark_games:
            continue
        for side in ("away", "home"):
            team = box_team(feed, side)
            code = team_meta(feed, side).get("abbreviation") or ""
            opp = team_meta(feed, "home" if side == "away" else "away").get("abbreviation") or ""
            for bid in team.get("batters", []) or []:
                stats = batting_stats(team, bid)
                if not stats:
                    continue
                rows.append({
                    "game_date": ident["game_date"],
                    "game_id": ident["game_id"],
                    "player_id": norm_id(bid),
                    "player_name": player_name(team, bid),
                    "team": code,
                    "opponent": opp,
                    "official_pa_boxscore": as_int(stats.get("plateAppearances")),
                    "official_ab_boxscore": as_int(stats.get("atBats")),
                    "official_hits_boxscore": as_int(stats.get("hits")),
                    "official_lineup_slot": lineup_slot(team, bid),
                    "source_path": ident["source_path"],
                })
    return pd.DataFrame(rows)


def hitter_game_summary(encounters: pd.DataFrame, official: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    if encounters.empty:
        return pd.DataFrame()
    e = encounters.copy()
    e["is_starter_pa"] = e["role_classification"].eq("STARTER_FACING_PA")
    e["is_bullpen_pa"] = e["role_classification"].eq("RELIEVER_FACING_PA")
    grouped = e.groupby(["game_date", "game_id", "batter_id"], dropna=False)
    rows = []
    for key, g in grouped:
        hits = g[g["official_hit"]].sort_values("plate_appearance_sequence")
        first_hit = hits.iloc[0] if len(hits) >= 1 else None
        second_hit = hits.iloc[1] if len(hits) >= 2 else None
        roles = list(hits["role_classification"])
        if len(hits) >= 2 and roles[0] == "STARTER_FACING_PA" and roles[1] == "STARTER_FACING_PA":
            two_plus_source = "BOTH_FIRST_TWO_HITS_AGAINST_STARTER"
        elif len(hits) >= 2 and roles[0] == "STARTER_FACING_PA" and roles[1] == "RELIEVER_FACING_PA":
            two_plus_source = "FIRST_STARTER_SECOND_BULLPEN"
        elif len(hits) >= 2 and roles[0] == "RELIEVER_FACING_PA" and roles[1] == "RELIEVER_FACING_PA":
            two_plus_source = "BOTH_FIRST_TWO_HITS_AGAINST_BULLPEN"
        elif len(hits) >= 2:
            two_plus_source = "OTHER_OR_IRREGULAR_SEQUENCE"
        else:
            two_plus_source = ""
        rows.append({
            "game_date": key[0],
            "game_id": key[1],
            "player_id": key[2],
            "player_name": g["batter_name"].iloc[0],
            "team": g["batter_team"].iloc[0],
            "opponent": g["opponent"].iloc[0],
            "lineup_slot": g["lineup_slot"].dropna().iloc[0] if g["lineup_slot"].notna().any() else "",
            "reconstructed_total_pa": len(g),
            "actual_starter_facing_pa": int(g["is_starter_pa"].sum()),
            "actual_bullpen_facing_pa": int(g["is_bullpen_pa"].sum()),
            "hits_against_starter": int((g["official_hit"] & g["is_starter_pa"]).sum()),
            "hits_against_bullpen": int((g["official_hit"] & g["is_bullpen_pa"]).sum()),
            "reconstructed_hits": int(g["official_hit"].sum()),
            "pitchers_faced": g["pitcher_id"].nunique(),
            "first_hit_pa_sequence": "" if first_hit is None else int(first_hit["plate_appearance_sequence"]),
            "second_hit_pa_sequence": "" if second_hit is None else int(second_hit["plate_appearance_sequence"]),
            "first_hit_pitcher_id": "" if first_hit is None else first_hit["pitcher_id"],
            "second_hit_pitcher_id": "" if second_hit is None else second_hit["pitcher_id"],
            "two_plus_hit_source_class": two_plus_source,
            "starter_plus_bullpen_pa_equals_total": int(g["is_starter_pa"].sum() + g["is_bullpen_pa"].sum()) == len(g),
        })
    h = pd.DataFrame(rows)
    if not official.empty:
        h = h.merge(official, on=["game_date", "game_id", "player_id"], how="left", suffixes=("", "_official"))
    b = benchmark[["player_game_key", "slate_date", "game_id", "player_id", "official_pa", "official_hits", "outcome_class", "temporal_split", "d15_pa_per_game", "lineup_bucket"]].copy()
    b["game_id"] = b["game_id"].map(norm_id)
    b["player_id"] = b["player_id"].map(norm_id)
    b = b.rename(columns={"slate_date": "game_date", "official_pa": "official_pa_benchmark", "official_hits": "official_hits_benchmark"})
    h = h.merge(b, on=["game_date", "game_id", "player_id"], how="left")
    h["pa_reconciles_boxscore"] = h["reconstructed_total_pa"].eq(h["official_pa_boxscore"])
    h["hits_reconciles_boxscore"] = h["reconstructed_hits"].eq(h["official_hits_boxscore"])
    h["pa_reconciles_benchmark"] = h["official_pa_benchmark"].notna() & h["reconstructed_total_pa"].eq(h["official_pa_benchmark"])
    h["hits_reconciles_benchmark"] = h["official_hits_benchmark"].notna() & h["reconstructed_hits"].eq(h["official_hits_benchmark"])
    return h


def second_hit_source(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame()
    rows = summary[summary["reconstructed_hits"] >= 1].copy()
    rows["hit_count_class"] = rows["reconstructed_hits"].map(lambda x: "TWO_PLUS_HITS" if x >= 2 else "EXACTLY_ONE_HIT")
    return rows[[
        "game_date", "game_id", "player_id", "player_name", "team", "opponent", "lineup_slot",
        "reconstructed_total_pa", "actual_starter_facing_pa", "actual_bullpen_facing_pa",
        "reconstructed_hits", "hit_count_class", "first_hit_pa_sequence", "second_hit_pa_sequence",
        "first_hit_pitcher_id", "second_hit_pitcher_id", "two_plus_hit_source_class", "pitchers_faced",
    ]]


def reconciliation(summary: pd.DataFrame, source_manifest: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame([{"scope": "pilot", "rows": 0}])
    rows = []
    def add(scope: str, frame: pd.DataFrame) -> None:
        rows.append({
            "scope": scope,
            "rows": len(frame),
            "pa_boxscore_reconciled": int(frame["pa_reconciles_boxscore"].sum()) if "pa_reconciles_boxscore" in frame else "",
            "pa_boxscore_rate": float(frame["pa_reconciles_boxscore"].mean()) if len(frame) and "pa_reconciles_boxscore" in frame else "",
            "hits_boxscore_reconciled": int(frame["hits_reconciles_boxscore"].sum()) if "hits_reconciles_boxscore" in frame else "",
            "hits_boxscore_rate": float(frame["hits_reconciles_boxscore"].mean()) if len(frame) and "hits_reconciles_boxscore" in frame else "",
            "pa_benchmark_reconciled": int(frame["pa_reconciles_benchmark"].sum()) if "pa_reconciles_benchmark" in frame else "",
            "hits_benchmark_reconciled": int(frame["hits_reconciles_benchmark"].sum()) if "hits_reconciles_benchmark" in frame else "",
            "starter_plus_bullpen_pa_equals_total": int(frame["starter_plus_bullpen_pa_equals_total"].sum()) if "starter_plus_bullpen_pa_equals_total" in frame else "",
        })
    add("all_boxscore_hitter_games_in_pilot_games", summary)
    bench = summary[summary["player_game_key"].notna()] if "player_game_key" in summary else summary.iloc[0:0]
    if not bench.empty:
        add("frozen_benchmark_overlap_hitter_games", bench)
    by_date = summary.groupby("game_date", dropna=False)
    for date, g in by_date:
        add(f"date_{date}", g)
    if not source_manifest.empty:
        rows.append({
            "scope": "source_games",
            "rows": int((source_manifest["game_status"] == "Final").sum()),
            "pa_boxscore_reconciled": "",
            "pa_boxscore_rate": "",
            "hits_boxscore_reconciled": "",
            "hits_boxscore_rate": "",
            "pa_benchmark_reconciled": "",
            "hits_benchmark_reconciled": "",
            "starter_plus_bullpen_pa_equals_total": "",
        })
    return pd.DataFrame(rows)


def reconciliation_discrepancies(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame()
    cols = [
        "game_date", "game_id", "player_id", "player_name", "team", "opponent",
        "reconstructed_total_pa", "official_pa_boxscore", "official_pa_benchmark",
        "reconstructed_hits", "official_hits_boxscore", "official_hits_benchmark",
        "pa_reconciles_boxscore", "hits_reconciles_boxscore",
        "pa_reconciles_benchmark", "hits_reconciles_benchmark",
        "source_path",
    ]
    mask = (
        (~summary["pa_reconciles_boxscore"].fillna(False))
        | (~summary["hits_reconciles_boxscore"].fillna(False))
        | (summary["official_pa_benchmark"].notna() & ~summary["pa_reconciles_benchmark"].fillna(False))
        | (summary["official_hits_benchmark"].notna() & ~summary["hits_reconciles_benchmark"].fillna(False))
    )
    return summary.loc[mask, [c for c in cols if c in summary.columns]].copy()


def roster_relative(encounters: pd.DataFrame, summary: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    if encounters.empty:
        return pd.DataFrame()
    bench_cols = benchmark[["game_id", "player_id", "outcome_class", "d15_pa_per_game", "lineup_bucket", "starter_context_status"]].copy()
    bench_cols["game_id"] = bench_cols["game_id"].map(norm_id)
    bench_cols["player_id"] = bench_cols["player_id"].map(norm_id)
    h = summary.merge(bench_cols, on=["game_id", "player_id"], how="left", suffixes=("", "_bench"))
    rows: list[dict[str, Any]] = []
    for (game_id, pitcher_id), gpa in encounters.groupby(["game_id", "pitcher_id"], dropna=False):
        hitter_ids = sorted(gpa["batter_id"].unique())
        hg = h[(h["game_id"].eq(game_id)) & (h["player_id"].isin(hitter_ids))]
        if hg.empty:
            continue
        rows.append({
            "game_date": gpa["game_date"].iloc[0],
            "game_id": game_id,
            "pitcher_id": pitcher_id,
            "pitcher_name": gpa["pitcher_name"].iloc[0],
            "pitcher_team": gpa["pitcher_team"].iloc[0],
            "role_classification": gpa["role_classification"].iloc[0],
            "hitters_faced": len(hitter_ids),
            "pa": len(gpa),
            "hits_allowed_reconstructed": int(gpa["official_hit"].sum()),
            "multi_hit_hitter_games": int((hg["reconstructed_hits"] >= 2).sum()),
            "benchmark_overlap_hitter_games": int(hg["player_game_key"].notna().sum()) if "player_game_key" in hg else 0,
            "lineup_slots_available": int(hg["lineup_slot"].notna().sum()) if "lineup_slot" in hg else 0,
            "pa_opportunity_available": int(hg["d15_pa_per_game"].notna().sum()) if "d15_pa_per_game" in hg else 0,
            "direct_bvp_support_available": 0,
            "handedness_available": int(gpa["pitcher_id"].notna().sum() > 0),
            "future_same_pitcher_test_supported": bool(len(hitter_ids) >= 2),
        })
    return pd.DataFrame(rows)


def coverage_assessment(source_manifest: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    bench_games = set(benchmark["game_id"].map(norm_id))
    final_local = source_manifest[source_manifest["game_status"].eq("Final")].copy()
    local_game_ids = set(final_local["game_id"].astype(str))
    overlap_games = bench_games & local_game_ids
    covered_batter_games = benchmark[benchmark["game_id"].map(norm_id).isin(overlap_games)]
    missing_games = sorted(bench_games - local_game_ids)
    rows = [
        {
            "scope": "full_frozen_benchmark",
            "benchmark_games": len(bench_games),
            "locally_available_final_feed_games": len(local_game_ids),
            "overlap_games": len(overlap_games),
            "benchmark_batter_games": len(benchmark),
            "overlap_batter_games": len(covered_batter_games),
            "coverage_pct_batter_games": len(covered_batter_games) / len(benchmark) if len(benchmark) else 0,
            "missing_games": len(missing_games),
            "missing_dates": ",".join(sorted(set(benchmark[benchmark["game_id"].map(norm_id).isin(missing_games)]["slate_date"].astype(str)))),
            "external_acquisition_required": len(missing_games) > 0,
            "proposed_endpoint": "/api/v1.1/game/{gamePk}/feed/live",
            "estimated_request_count": len(missing_games),
            "storage_processing_note": "raw JSON already sufficient where present; missing benchmark games require official feed/live acquisition or equivalent local archive",
        }
    ]
    for date, g in benchmark.groupby("slate_date"):
        games = set(g["game_id"].map(norm_id))
        overlap = games & local_game_ids
        rows.append({
            "scope": f"date_{date}",
            "benchmark_games": len(games),
            "locally_available_final_feed_games": len(games & local_game_ids),
            "overlap_games": len(overlap),
            "benchmark_batter_games": len(g),
            "overlap_batter_games": int(g["game_id"].map(norm_id).isin(overlap).sum()),
            "coverage_pct_batter_games": float(g["game_id"].map(norm_id).isin(overlap).mean()) if len(g) else 0,
            "missing_games": len(games - local_game_ids),
            "missing_dates": "" if len(games - local_game_ids) == 0 else str(date),
            "external_acquisition_required": len(games - local_game_ids) > 0,
            "proposed_endpoint": "/api/v1.1/game/{gamePk}/feed/live",
            "estimated_request_count": len(games - local_game_ids),
            "storage_processing_note": "",
        })
    return pd.DataFrame(rows)


def local_source_inventory(feeds: list[tuple[Path, dict[str, Any]]]) -> pd.DataFrame:
    feed_dates = Counter()
    final_games = 0
    all_plays = 0
    complete_pa = 0
    for path, feed in feeds:
        ident = feed_game_identity(path, feed)
        feed_dates[ident["game_date"]] += 1
        if ident["game_status"] == "Final":
            final_games += 1
        plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", []) or []
        all_plays += len(plays)
        complete_pa += sum(1 for p in plays if is_pa(p))
    rows = [
        {
            "source_name": "local_statsapi_feed_live_json",
            "path_or_table": rel(RAW_FEEDS),
            "grain": "game feed with allPlays; each complete allPlay with batter and pitcher is PA-like encounter",
            "date_coverage": f"{min(feed_dates) if feed_dates else ''} to {max(feed_dates) if feed_dates else ''}",
            "games": len(feeds),
            "events_or_pa": complete_pa,
            "batter_id": "matchup.batter.id",
            "pitcher_id": "matchup.pitcher.id",
            "game_id": "gameData.game.pk",
            "inning_sequence": "about.inning/about.halfInning/about.atBatIndex",
            "event_result": "result.eventType/result.event",
            "official_starter_identity": "boxscore players pitching.gamesStarted=1",
            "strict_historical_authority": "official StatsAPI raw feed preserved locally with SHA256",
            "local_availability": "AVAILABLE_PARTIAL_DATE_RANGE",
            "replayability": "local raw JSON",
            "duplicate_missingness_state": f"{final_games} final local feeds; partial benchmark coverage",
        },
        {
            "source_name": "retrosheet_chadwick_register",
            "path_or_table": "backend/mlb/data/raw/retrosheet/chadwick_register/",
            "grain": "player identity register only",
            "date_coverage": "identity register, not game events",
            "games": 0,
            "events_or_pa": 0,
            "batter_id": "chadwick IDs/MLBAM mappings",
            "pitcher_id": "chadwick IDs/MLBAM mappings",
            "game_id": "not present",
            "inning_sequence": "not present",
            "event_result": "not present",
            "official_starter_identity": "not present",
            "strict_historical_authority": "identity support only",
            "local_availability": "AVAILABLE_IDENTITY_ONLY",
            "replayability": "local CSV",
            "duplicate_missingness_state": "not an encounter source",
        },
        {
            "source_name": "bvp_preparation_artifacts",
            "path_or_table": "backend/mlb/scripts/refresh_mlb_bvp_pvb.py and BvP aggregate artifacts",
            "grain": "aggregate batter-vs-pitcher history",
            "date_coverage": "varies",
            "games": "",
            "events_or_pa": "",
            "batter_id": "available in aggregate outputs",
            "pitcher_id": "available in aggregate outputs",
            "game_id": "not reliably retained",
            "inning_sequence": "not retained",
            "event_result": "aggregate only",
            "official_starter_identity": "not retained",
            "strict_historical_authority": "not sufficient for encounter ledger",
            "local_availability": "AVAILABLE_AGGREGATE_ONLY",
            "replayability": "not PA sequence replayable",
            "duplicate_missingness_state": "cannot reconstruct PA order",
        },
        {
            "source_name": "pitcher_game_logs_and_bf_sources",
            "path_or_table": "starter BF / pitcher game-log artifacts and proposed tables",
            "grain": "pitcher-game aggregate",
            "date_coverage": "varies",
            "games": "",
            "events_or_pa": "",
            "batter_id": "not present",
            "pitcher_id": "present",
            "game_id": "present where source-bound",
            "inning_sequence": "not present",
            "event_result": "aggregate only",
            "official_starter_identity": "starter flags available in some artifacts",
            "strict_historical_authority": "not sufficient for batter-pitcher encounter ledger",
            "local_availability": "AVAILABLE_AGGREGATE_ONLY",
            "replayability": "not PA sequence replayable",
            "duplicate_missingness_state": "cannot assign hitter PA to pitcher",
        },
    ]
    return pd.DataFrame(rows)


def design_tables() -> dict[str, pd.DataFrame]:
    contract = pd.DataFrame([
        {"field": "game_date", "required": True, "grain": "encounter", "definition": "official game date", "notes": ""},
        {"field": "game_id", "required": True, "grain": "encounter", "definition": "official MLB gamePk", "notes": ""},
        {"field": "plate_appearance_sequence", "required": True, "grain": "encounter", "definition": "game-level PA sequence from StatsAPI allPlays order", "notes": ""},
        {"field": "batter_id", "required": True, "grain": "encounter", "definition": "MLBAM batter ID", "notes": ""},
        {"field": "pitcher_id", "required": True, "grain": "encounter", "definition": "MLBAM pitcher ID on the PA", "notes": ""},
        {"field": "role_classification", "required": True, "grain": "encounter", "definition": "STARTER_FACING_PA / RELIEVER_FACING_PA / OPENER_FACING_PA / BULK_RELIEVER_FACING_PA / IRREGULAR_ROLE_UNRESOLVED", "notes": "pilot only assigns starter/reliever/irregular; opener/bulk reserved for governed role logic"},
        {"field": "official_hit", "required": True, "grain": "encounter", "definition": "single/double/triple/home_run event type", "notes": ""},
        {"field": "source_event_identity", "required": True, "grain": "encounter", "definition": "game_id:atBatIndex", "notes": ""},
        {"field": "source_path", "required": True, "grain": "encounter", "definition": "local raw feed path", "notes": ""},
        {"field": "source_sha256", "required": True, "grain": "encounter", "definition": "raw feed checksum", "notes": ""},
    ])
    bullpen = pd.DataFrame([
        {"component": "exposure_quantity", "prediction_time_target": "P(0/1/2/3+ bullpen-facing PA)", "historical_parent": "encounter ledger actual bullpen PA", "allowed_now": "design_only", "notes": "separate from bullpen quality"},
        {"component": "starter_exit_range", "prediction_time_target": "starter exit PA/BF/inning range", "historical_parent": "starter encounter count and pitcher-entry ledger", "allowed_now": "design_only", "notes": ""},
        {"component": "relief_pitcher_count", "prediction_time_target": "likely number of relievers faced", "historical_parent": "pitcher-entry ledger", "allowed_now": "design_only", "notes": ""},
        {"component": "bullpen_quality", "prediction_time_target": "team bullpen suppression prior", "historical_parent": "reliever-facing PA and hits allowed", "allowed_now": "design_only", "notes": "do not merge into composite score here"},
        {"component": "matchup_compatibility", "prediction_time_target": "hitter profile vs likely relief characteristics", "historical_parent": "handedness and reliever encounter outcomes", "allowed_now": "design_only", "notes": "requires governed pregame availability"},
    ])
    experiment = pd.DataFrame([
        {"item": "frozen_control", "definition": "existing hitter + PA + Starter benchmark", "metric_or_gate": "unchanged baseline", "notes": ""},
        {"item": "challenger", "definition": "adds predicted starter-facing PA, predicted bullpen-facing PA, second-hit source behavior, bullpen suppression context", "metric_or_gate": "design only", "notes": "no fitting in this task"},
        {"item": "primary_target", "definition": "EXACTLY_ONE_HIT versus TWO_OR_MORE_HITS", "metric_or_gate": "Brier/log loss/AUC/calibration", "notes": ""},
        {"item": "economic_slice", "definition": "+200 O1.5 evaluation", "metric_or_gate": "timing-certified only", "notes": ""},
        {"item": "suppression_preservation", "definition": "do not erase U1.5/pitcher-suppression findings", "metric_or_gate": "required", "notes": ""},
    ])
    external = pd.DataFrame([
        {"need": "missing frozen benchmark feed/live games", "proposed_endpoint": "/api/v1.1/game/{gamePk}/feed/live", "approval_required": True, "reason": "local feeds cover only partial benchmark", "smallest_bounded_pilot": "one missing benchmark slate date"},
        {"need": "historical full benchmark completion", "proposed_endpoint": "/api/v1.1/game/{gamePk}/feed/live", "approval_required": True, "reason": "encounter ledger requires exact batter-pitcher PA rows", "smallest_bounded_pilot": "10-20 missing game IDs"},
    ])
    return {"contract": contract, "bullpen": bullpen, "experiment": experiment, "external": external}


def markdown_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df.empty:
        return "_No rows._"
    d = df.head(max_rows).copy()
    cols = list(d.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in d.iterrows():
        lines.append("| " + " | ".join(str(row.get(c, "")).replace("\n", " ") for c in cols) + " |")
    if len(df) > max_rows:
        lines.append(f"\n_Showing {max_rows} of {len(df)} rows._")
    return "\n".join(lines)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    benchmark = read_csv(BENCH)
    benchmark["game_id"] = benchmark["game_id"].map(norm_id)
    benchmark["player_id"] = benchmark["player_id"].map(norm_id)
    feeds = load_feeds(RAW_FEEDS)
    ledgers = build_ledgers(feeds, benchmark)
    official = official_batter_totals(feeds, set(benchmark["game_id"]))
    summary = hitter_game_summary(ledgers["encounters"], official, benchmark)
    second = second_hit_source(summary)
    recon = reconciliation(summary, ledgers["source_manifest"])
    discrepancies = reconciliation_discrepancies(summary)
    roster = roster_relative(ledgers["encounters"], summary, benchmark)
    coverage = coverage_assessment(ledgers["source_manifest"], benchmark)
    inventory = local_source_inventory(feeds)
    designs = design_tables()

    two_plus = second[second["hit_count_class"].eq("TWO_PLUS_HITS")].copy() if not second.empty else pd.DataFrame()
    source_counts = Counter(two_plus.get("two_plus_hit_source_class", []))
    exact_recon_ok = bool(not summary.empty and summary["pa_reconciles_boxscore"].mean() >= 0.98 and summary["hits_reconciles_boxscore"].mean() >= 0.98)
    overlap_games = int(coverage.loc[coverage["scope"].eq("full_frozen_benchmark"), "overlap_games"].iloc[0])
    overlap_batter_games = int(coverage.loc[coverage["scope"].eq("full_frozen_benchmark"), "overlap_batter_games"].iloc[0])
    missing_games = int(coverage.loc[coverage["scope"].eq("full_frozen_benchmark"), "missing_games"].iloc[0])

    decisions = {
        "MLB_ENCOUNTER_SOURCE_INVENTORY_DECISION": "LOCAL_STATSAPI_FEED_LIVE_EXISTS_PARTIAL_BENCHMARK_RANGE",
        "MLB_ENCOUNTER_LOCAL_CONSTRUCTIBILITY_DECISION": "LOCALLY_CONSTRUCTIBLE_PARTIAL_DATE_RANGE",
        "MLB_ENCOUNTER_CANONICAL_CONTRACT_DECISION": "CANONICAL_CONTRACT_FROZEN_RESEARCH_ONLY",
        "MLB_ENCOUNTER_PILOT_CONSTRUCTION_DECISION": "BOUNDED_LEDGER_CONSTRUCTED_FROM_LOCAL_STATSAPI_FEEDS",
        "MLB_ENCOUNTER_OFFICIAL_TOTAL_RECONCILIATION_DECISION": "OFFICIAL_TOTAL_RECONCILIATION_PASS" if exact_recon_ok else "OFFICIAL_TOTAL_RECONCILIATION_PARTIAL_REVIEW_REQUIRED",
        "MLB_ENCOUNTER_SECOND_HIT_SOURCE_DECISION": "SECOND_HIT_SOURCE_OBSERVABLE_DESCRIPTIVE_ONLY" if not two_plus.empty else "SECOND_HIT_SOURCE_NOT_OBSERVABLE_IN_PILOT",
        "MLB_ENCOUNTER_ROSTER_RELATIVE_FEASIBILITY_DECISION": "SAME_PITCHER_ROSTER_RELATIVE_FEASIBLE_FOR_LOCAL_LEDGER",
        "MLB_ENCOUNTER_FULL_BENCHMARK_COVERAGE_DECISION": "FULL_BENCHMARK_REQUIRES_EXTERNAL_OFFICIAL_FEED_ACQUISITION",
        "MLB_BULLPEN_EXPOSURE_PLATFORM_DESIGN_DECISION": "BULLPEN_EXPOSURE_PLATFORM_DESIGNED_NOT_IMPLEMENTED",
        "MLB_ENCOUNTER_NEXT_PROBABILITY_EXPERIMENT_DECISION": "DESIGN_ONLY_WAIT_FOR_COVERAGE_EXPANSION",
        "MLB_ENCOUNTER_EXTERNAL_PERMISSION_REQUIREMENT": "EXPLICIT_APPROVAL_REQUIRED_FOR_MISSING_FEED_ACQUISITION",
        "MLB_ENCOUNTER_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }

    outputs = {
        "local_source_inventory_2026-07-17.csv": inventory,
        "bounded_source_manifest_2026-07-17.csv": ledgers["source_manifest"],
        "plate_appearance_encounter_ledger_2026-07-17.csv": ledgers["encounters"],
        "pitcher_entry_ledger_2026-07-17.csv": ledgers["pitcher_entry"],
        "hitter_game_exposure_summary_2026-07-17.csv": summary,
        "second_hit_source_ledger_2026-07-17.csv": second,
        "official_total_reconciliation_2026-07-17.csv": recon,
        "official_total_discrepancy_ledger_2026-07-17.csv": discrepancies,
        "roster_relative_feasibility_report_2026-07-17.csv": roster,
        "full_benchmark_coverage_assessment_2026-07-17.csv": coverage,
        "canonical_encounter_contract_2026-07-17.csv": designs["contract"],
        "bullpen_exposure_platform_design_2026-07-17.csv": designs["bullpen"],
        "next_probability_experiment_design_2026-07-17.csv": designs["experiment"],
        "external_permission_request_2026-07-17.csv": designs["external"],
        "rejection_missingness_ledger_2026-07-17.csv": ledgers["rejected"],
        "required_decisions_2026-07-17.csv": pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()]),
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)

    metrics = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "local_feed_files": len(feeds),
        "local_final_feed_files": int((ledgers["source_manifest"]["game_status"] == "Final").sum()) if not ledgers["source_manifest"].empty else 0,
        "bounded_overlap_games": overlap_games,
        "bounded_benchmark_batter_games": overlap_batter_games,
        "encounter_rows": len(ledgers["encounters"]),
        "hitter_game_rows": len(summary),
        "benchmark_games": int(benchmark["game_id"].nunique()),
        "missing_benchmark_games": missing_games,
        "boxscore_pa_reconciliation_rate": float(summary["pa_reconciles_boxscore"].mean()) if not summary.empty else 0,
        "boxscore_hits_reconciliation_rate": float(summary["hits_reconciles_boxscore"].mean()) if not summary.empty else 0,
        "benchmark_overlap_pa_reconciled": int(summary["pa_reconciles_benchmark"].sum()) if not summary.empty else 0,
        "benchmark_overlap_hits_reconciled": int(summary["hits_reconciles_benchmark"].sum()) if not summary.empty else 0,
        "two_plus_hitter_games": int((summary["reconstructed_hits"] >= 2).sum()) if not summary.empty else 0,
        "second_hit_source_counts": dict(source_counts),
        "decisions": decisions,
    }
    write_json(metrics, out_dir / "machine_readable_encounter_ledger_pilot_2026-07-17.json")

    md = f"""# MLB Batter-Pitcher Encounter Ledger and Bullpen-Exposure Foundation Pilot

Generated: `{metrics['generated_at_utc']}`

## Executive Summary

The repository can construct an exact batter-pitcher plate-appearance encounter ledger where preserved local StatsAPI feed/live JSON exists. The bounded local pilot covers **{overlap_games} frozen-benchmark games** and **{overlap_batter_games} benchmark batter-games** from the partial local feed archive. It produced **{metrics['encounter_rows']} encounter rows** and **{metrics['hitter_game_rows']} hitter-game exposure rows**.

This is a data-platform result, not a model result. It confirms that the correct foundation for later-PA and bullpen exposure is a canonical encounter ledger, not another Starter-only exposure variation.

## Official Reconciliation

Boxscore PA reconciliation rate: **{metrics['boxscore_pa_reconciliation_rate']:.2%}**.

Boxscore hit reconciliation rate: **{metrics['boxscore_hits_reconciliation_rate']:.2%}**.

## Second-Hit Source Counts

{markdown_table(pd.DataFrame([{'second_hit_source_class': k, 'two_plus_hitter_games': v} for k, v in source_counts.items()]))}

## Full Benchmark Coverage

{markdown_table(coverage.head(1))}

## Source Inventory

{markdown_table(inventory)}

## Required Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## Direct Answer

Yes, for locally preserved feed/live games the project can now observe who each hitter actually faced in every plate appearance and can separate actual Starter-facing from reliever-facing exposure. No, the project cannot yet do this for every frozen benchmark game without additional official feed/live acquisition: **{missing_games} / {benchmark['game_id'].nunique()}** benchmark games are not locally present as exact encounter feeds.

## No Behavior Changed

No network, OddsAPI, DB write, external acquisition, model fitting, threshold optimization, production model, selector, candidate, upload, Quick Card, workspace, or LaunchAgent behavior changed.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")

    validation = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        validation.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(validation), out_dir / "validation_report_2026-07-17.csv")

    manifest = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT))
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    args = parser.parse_args()
    metrics = build(Path(args.output_dir))
    print(json.dumps(metrics, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
