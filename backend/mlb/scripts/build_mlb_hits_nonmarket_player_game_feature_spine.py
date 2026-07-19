"""Build a broad MLB hitter player-game feature spine without market inputs.

This utility is research-only. It reads local/database-backed baseball tables,
constructs strict-prior hitter and starter features at player-game grain, and
writes a certification package. It does not fit models, call sportsbook APIs,
write databases, alter production artifacts, or use odds/line/book fields.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.shared.db.pg import pg_fetchall


RUN_DATE = "2026-07-19"
START_DATE = "2026-05-01"
END_DATE = "2026-07-18"
CURRENT_REPLAY_DATE = "2026-07-19"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_nonmarket_player_game_feature_spine/2026-07-19"
PRIOR_MARKET_POP = ROOT / "artifacts/analysis/model_development/mlb_hits_market_independent_reconstruction/2026-07-18/recovered_baseball_population_2026-07-18.csv"
PRIOR_MISSING_53 = ROOT / "artifacts/analysis/model_development/mlb_hits_market_independent_reconstruction/2026-07-18/missing_53_date_recovery_inventory_2026-07-18.csv"
LINEUP_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_pregame_lineup_turnover_exposure_pilot/2026-07-17/canonical_pregame_lineup_ledger_2026-07-17.csv"
LIVE_PARENT_0718 = ROOT / "artifacts/analysis/model_development/mlb_live_hitter_parent_daily_integration/2026-07-18/live_hitter_parent_artifact_2026-07-18.csv"

MARKET_BANNED = {
    "odds",
    "line",
    "price",
    "book",
    "bookmaker",
    "market",
    "implied",
    "vig",
    "consensus",
    "fanduel",
    "betonline",
    "draftkings",
    "bovada",
    "upload",
    "candidate",
    "selector",
}

IDENTITY_COLUMNS = [
    "slate_date",
    "game_id",
    "player_id",
    "team",
    "opponent",
    "player_name",
    "game_start_time",
    "is_home",
    "batting_side",
    "opposing_starter_id",
    "opposing_starter_name",
    "opposing_starter_identity_semantics",
    "opposing_starter_source",
    "position",
    "lineup_status",
    "lineup_semantics_source",
    "lineup_source_timestamp",
    "batting_order_position",
    "player_appearance_status",
]

OUTCOME_COLUMNS = [
    "actual_hits",
    "actual_plate_appearances",
    "actual_at_bats",
    "actual_lineup_position",
    "started_game",
    "appeared_in_game",
    "zero_pa_status",
    "actual_hits_class",
]


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    cols = list(df.columns)
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in df.iterrows():
        vals = [str(row.get(c, "")).replace("|", "\\|") for c in cols]
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def daterange(start: str, end: str) -> list[str]:
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    out = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def safe_num(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def hit_class(v: Any) -> str:
    n = safe_num(v)
    if n <= 0:
        return "0"
    if n == 1:
        return "1"
    if n == 2:
        return "2"
    return "3+"


def bucket_slot(v: Any) -> str:
    n = safe_num(v)
    if 1 <= n <= 3:
        return "top_order"
    if 4 <= n <= 6:
        return "middle_order"
    if 7 <= n <= 9:
        return "bottom_order"
    return "unknown"


@dataclass
class Sources:
    player_stats: pd.DataFrame
    game_info: pd.DataFrame


def fetch_sources(start: str, end: str, history_start: str, current_date: str) -> Sources:
    ps_rows = pg_fetchall(
        """
        SELECT
          ps.player_id, ps.game_id, ps.game_date::text AS game_date, ps.team,
          ps.opponent, ps.is_home, ps.position, ps.hits, ps.total_bases,
          ps.rbis, ps.runs_scored, ps.strikeouts_batting, ps.walks,
          ps.singles, ps.doubles, ps.triples, ps.home_runs, ps.stolen_bases,
          ps.strikeouts_pitching, ps.walks_allowed, ps.hits_allowed,
          ps.outs_recorded, ps.earned_runs, ps.is_starter, ps.at_bats,
          ps.plate_appearances, ps.pa_source, pid.player_name AS player_name
        FROM mlb.player_stats ps
        LEFT JOIN mlb.player_ids pid ON pid.player_id = ps.player_id
        WHERE ps.game_date BETWEEN %s::date AND %s::date
        """,
        (history_start, current_date),
    )
    gi_rows = pg_fetchall(
        """
        SELECT
          game_id, game_time::text AS game_time, game_date::text AS game_date,
          home_team_id, away_team_id, home_team_abbr, away_team_abbr,
          starting_pitcher_id_home, starting_pitcher_id_away
        FROM mlb.game_info
        WHERE game_date BETWEEN %s::date AND %s::date
        """,
        (start, current_date),
    )
    return Sources(pd.DataFrame(ps_rows), pd.DataFrame(gi_rows))


def build_denominator(ps: pd.DataFrame, gi: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if ps.empty:
        return pd.DataFrame()
    df = ps.copy()
    df["slate_date"] = df["game_date"].astype(str).str[:10]
    for c in ["hits", "total_bases", "at_bats", "plate_appearances", "walks", "singles", "doubles", "triples", "home_runs"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "is_starter" in df:
        df["is_starter"] = pd.to_numeric(df["is_starter"], errors="coerce")
    df["position"] = df["position"].fillna("")
    batting_stat_sum = df[["hits", "total_bases", "at_bats", "plate_appearances", "walks", "singles", "doubles", "triples", "home_runs"]].fillna(0).sum(axis=1)
    hitter_positions = ~df["position"].isin(["P"])
    pitcher_batted = df["position"].eq("P") & (df["plate_appearances"].fillna(0) > 0)
    candidate = df[(hitter_positions | pitcher_batted) & (batting_stat_sum >= 0)].copy()
    candidate = candidate[(candidate["slate_date"] >= start) & (candidate["slate_date"] <= end)].copy()
    gi2 = gi.copy()
    if not gi2.empty:
        gi2["game_id"] = pd.to_numeric(gi2["game_id"], errors="coerce")
        gi2 = gi2.drop_duplicates("game_id")
        candidate = candidate.merge(
            gi2[
                [
                    "game_id",
                    "game_time",
                    "home_team_abbr",
                    "away_team_abbr",
                    "starting_pitcher_id_home",
                    "starting_pitcher_id_away",
                ]
            ],
            on="game_id",
            how="left",
        )
    else:
        for c in ["game_time", "home_team_abbr", "away_team_abbr", "starting_pitcher_id_home", "starting_pitcher_id_away"]:
            candidate[c] = np.nan
    candidate["opposing_starter_id"] = np.where(
        candidate["is_home"].astype(str).str.lower().isin(["true", "1"]),
        candidate["starting_pitcher_id_away"],
        candidate["starting_pitcher_id_home"],
    )
    starter_identity = df[(df["position"].eq("P")) & (df["is_starter"].fillna(0).eq(1))].copy()
    if not starter_identity.empty:
        starter_identity["starter_id_unique_count"] = starter_identity.groupby(["game_id", "team"])["player_id"].transform("nunique")
        starter_identity = starter_identity[starter_identity["starter_id_unique_count"].eq(1)].copy()
        starter_identity = starter_identity.drop_duplicates(["game_id", "team"])[
            ["game_id", "team", "player_id", "player_name"]
        ].rename(
            columns={
                "team": "opponent",
                "player_id": "actual_opposing_starter_id",
                "player_name": "actual_opposing_starter_name",
            }
        )
        candidate = candidate.merge(starter_identity, on=["game_id", "opponent"], how="left")
    else:
        candidate["actual_opposing_starter_id"] = np.nan
        candidate["actual_opposing_starter_name"] = ""
    candidate["opposing_starter_id_gameinfo"] = candidate["opposing_starter_id"]
    candidate["opposing_starter_id"] = candidate["opposing_starter_id"].combine_first(candidate["actual_opposing_starter_id"])
    candidate["opposing_starter_name"] = candidate["actual_opposing_starter_name"].fillna("")
    candidate["opposing_starter_identity_semantics"] = np.select(
        [
            candidate["opposing_starter_id_gameinfo"].notna(),
            candidate["actual_opposing_starter_id"].notna(),
        ],
        [
            "PREGAME_GAME_INFO_STARTER_ID",
            "POSTGAME_ACTUAL_STARTER_IDENTITY_FOR_RETROSPECTIVE_BINDING",
        ],
        default="OPPOSING_STARTER_IDENTITY_UNAVAILABLE",
    )
    candidate["opposing_starter_source"] = np.select(
        [
            candidate["opposing_starter_id_gameinfo"].notna(),
            candidate["actual_opposing_starter_id"].notna(),
        ],
        [
            "mlb.game_info.starting_pitcher_id_home_away",
            "mlb.player_stats.same_game_pitcher_is_starter",
        ],
        default="",
    )
    candidate["player_game_key"] = candidate["slate_date"].astype(str) + "|" + candidate["game_id"].astype(str) + "|" + candidate["player_id"].astype(str)
    candidate["appeared_in_game"] = True
    candidate["zero_pa_status"] = np.where(candidate["plate_appearances"].fillna(0) <= 0, "ZERO_OFFICIAL_PA", "HAS_OFFICIAL_PA")
    candidate["player_appearance_status"] = np.where(candidate["plate_appearances"].fillna(0) > 0, "APPEARED_WITH_PA", "APPEARED_ZERO_PA")
    candidate["started_game"] = np.where(candidate["position"].isin(["PH", "PR"]), False, np.nan)
    candidate["actual_hits"] = candidate["hits"]
    candidate["actual_plate_appearances"] = candidate["plate_appearances"]
    candidate["actual_at_bats"] = candidate["at_bats"]
    candidate["actual_lineup_position"] = np.nan
    candidate["actual_hits_class"] = candidate["actual_hits"].map(hit_class)
    candidate["batting_side"] = ""
    candidate["game_start_time"] = candidate["game_time"]
    return candidate.sort_values(["slate_date", "game_id", "team", "player_id"]).reset_index(drop=True)


def attach_lineup_semantics(spine: pd.DataFrame) -> pd.DataFrame:
    out = spine.copy()
    out["lineup_status"] = "LINEUP_STATUS_UNAVAILABLE"
    out["lineup_semantics_source"] = ""
    out["lineup_source_timestamp"] = ""
    out["batting_order_position"] = np.nan
    out["lineup_bucket"] = "unknown"
    ledger = read_csv(LINEUP_LEDGER)
    if not ledger.empty:
        cols = [
            "player_game_key",
            "certainty_state",
            "canonical_pregame_lineup_slot",
            "confirmed_pregame_lineup_slot",
            "projected_batting_position",
            "lineup_source",
            "source_timestamp",
        ]
        for c in cols:
            if c not in ledger:
                ledger[c] = np.nan
        ledger = ledger[cols].drop_duplicates("player_game_key")
        out = out.merge(ledger, on="player_game_key", how="left")
        confirmed = out["certainty_state"].eq("CONFIRMED_PREGAME_LINEUP")
        projected = out["certainty_state"].eq("LINEUP_POSITION_FALLBACK") & out["projected_batting_position"].notna()
        out.loc[confirmed, "lineup_status"] = "CONFIRMED_PREGAME_STARTER"
        out.loc[projected, "lineup_status"] = "PROJECTED_PREGAME_STARTER"
        out.loc[out["certainty_state"].eq("LINEUP_UNKNOWN"), "lineup_status"] = "LINEUP_STATUS_UNAVAILABLE"
        out["batting_order_position"] = pd.to_numeric(out["canonical_pregame_lineup_slot"], errors="coerce")
        out["lineup_semantics_source"] = out["lineup_source"].fillna("")
        out["lineup_source_timestamp"] = out["source_timestamp"].fillna("")
        out = out.drop(columns=[c for c in ["certainty_state", "canonical_pregame_lineup_slot", "confirmed_pregame_lineup_slot", "projected_batting_position", "lineup_source", "source_timestamp"] if c in out])

    live = read_csv(LIVE_PARENT_0718)
    if not live.empty:
        live = live.copy()
        live["player_game_key"] = live["slate_date"].astype(str) + "|" + live["game_id"].astype(str) + "|" + live["player_id"].astype(str)
        live = live[["player_game_key", "lineup_slot", "lineup_source_path", "lineup_source_timestamp", "parent_row_status"]].drop_duplicates("player_game_key")
        out = out.merge(live, on="player_game_key", how="left", suffixes=("", "_live0718"))
        mask = out["lineup_slot"].notna()
        out.loc[mask, "lineup_status"] = "CONFIRMED_PREGAME_STARTER"
        out.loc[mask, "batting_order_position"] = pd.to_numeric(out.loc[mask, "lineup_slot"], errors="coerce")
        out.loc[mask, "lineup_semantics_source"] = out.loc[mask, "lineup_source_path"].fillna("")
        out.loc[mask, "lineup_source_timestamp"] = out.loc[mask, "lineup_source_timestamp_live0718"].fillna("")
        out = out.drop(columns=[c for c in ["lineup_slot", "lineup_source_path", "lineup_source_timestamp_live0718", "parent_row_status"] if c in out])
    out["lineup_bucket"] = out["batting_order_position"].map(bucket_slot)
    return out


def add_strict_prior_features(spine: pd.DataFrame, all_ps: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = spine.copy()
    hist = all_ps.copy()
    hist["game_date"] = hist["game_date"].astype(str).str[:10]
    for c in [
        "hits",
        "plate_appearances",
        "at_bats",
        "singles",
        "doubles",
        "triples",
        "home_runs",
        "total_bases",
        "hits_allowed",
        "outs_recorded",
        "earned_runs",
        "is_starter",
    ]:
        if c in hist:
            hist[c] = pd.to_numeric(hist[c], errors="coerce")
    # Batter prior features.
    hist_bat = hist[(hist["position"].fillna("") != "P") | (hist["plate_appearances"].fillna(0) > 0)].copy()
    hist_bat = hist_bat.sort_values(["player_id", "game_date", "game_id"])
    feature_rows = []
    for _, r in out.iterrows():
        d = str(r["slate_date"])
        pid = r["player_id"]
        prev = hist_bat[(hist_bat["player_id"].eq(pid)) & (hist_bat["game_date"] < d)]
        row: dict[str, Any] = {
            "player_game_key": r["player_game_key"],
            "feature_cutoff_date": (date.fromisoformat(d) - timedelta(days=1)).isoformat(),
            "prior_game_count": int(len(prev)),
            "latest_contributing_prior_game_date": prev["game_date"].max() if len(prev) else "",
        }
        for w in [7, 15, 30]:
            tail = prev.tail(w)
            pa = tail["plate_appearances"].fillna(0).sum()
            hits = tail["hits"].fillna(0).sum()
            games = len(tail)
            row[f"d{w}_games"] = games
            row[f"d{w}_hits"] = float(hits / games) if games else np.nan
            row[f"d{w}_plate_appearances"] = float(pa / games) if games else np.nan
            row[f"d{w}_at_bats"] = float(tail["at_bats"].fillna(0).sum() / games) if games else np.nan
            row[f"d{w}_hits_per_pa"] = float(hits / pa) if pa else np.nan
            row[f"d{w}_singles"] = float(tail["singles"].fillna(0).sum() / games) if games else np.nan
            row[f"d{w}_doubles"] = float(tail["doubles"].fillna(0).sum() / games) if games else np.nan
            row[f"d{w}_triples"] = float(tail["triples"].fillna(0).sum() / games) if games else np.nan
            row[f"d{w}_home_runs"] = float(tail["home_runs"].fillna(0).sum() / games) if games else np.nan
            row[f"d{w}_total_bases"] = float(tail["total_bases"].fillna(0).sum() / games) if games else np.nan
            row[f"d{w}_two_plus_rate"] = float((tail["hits"].fillna(0) >= 2).mean()) if games else np.nan
        row["season_to_date_games"] = int(len(prev))
        pa_all = prev["plate_appearances"].fillna(0).sum()
        hits_all = prev["hits"].fillna(0).sum()
        row["season_to_date_hits_per_pa"] = float(hits_all / pa_all) if pa_all else np.nan
        row["season_to_date_pa_per_game"] = float(pa_all / len(prev)) if len(prev) else np.nan
        row["strict_prior_status"] = "PASS_STRICT_PRIOR" if len(prev) else "SPARSE_OR_NO_PRIOR_HISTORY"
        feature_rows.append(row)
    feats = pd.DataFrame(feature_rows)
    out = out.merge(feats, on="player_game_key", how="left")

    # Opposing starter prior features.
    pitch = hist[(hist["is_starter"].fillna(0) == 1) & hist["player_id"].notna()].copy()
    pitch["player_id_norm"] = pd.to_numeric(pitch["player_id"], errors="coerce").astype("Int64").astype(str)
    starter_rows = []
    for _, r in out.iterrows():
        d = str(r["slate_date"])
        spid = r.get("opposing_starter_id")
        spid_norm = ""
        if pd.notna(spid):
            parsed_spid = pd.to_numeric(pd.Series([spid]), errors="coerce").iloc[0]
            if pd.notna(parsed_spid):
                spid_norm = str(int(parsed_spid))
        prev = pitch[(pitch["player_id_norm"].eq(spid_norm)) & (pitch["game_date"] < d)] if spid_norm else pd.DataFrame()
        row = {"player_game_key": r["player_game_key"], "starter_prior_start_count": int(len(prev))}
        for w in [7, 15, 30]:
            tail = prev.tail(w)
            outs = tail["outs_recorded"].fillna(0).sum() if len(tail) else 0
            ha = tail["hits_allowed"].fillna(0).sum() if len(tail) else 0
            row[f"starter_d{w}_starts"] = int(len(tail))
            row[f"starter_d{w}_outs_per_start"] = float(outs / len(tail)) if len(tail) else np.nan
            row[f"starter_d{w}_hits_allowed_per_out"] = float(ha / outs) if outs else np.nan
            row[f"starter_d{w}_earned_runs_per_start"] = float(tail["earned_runs"].fillna(0).sum() / len(tail)) if len(tail) else np.nan
        starter_rows.append(row)
    out = out.merge(pd.DataFrame(starter_rows), on="player_game_key", how="left")

    team_game = (
        hist_bat.groupby(["game_date", "team", "game_id"], dropna=False)
        .agg(team_hits=("hits", "sum"), team_pa=("plate_appearances", "sum"))
        .reset_index()
        .sort_values(["team", "game_date", "game_id"])
    )
    team_rows = []
    for _, r in out.iterrows():
        d = str(r["slate_date"])
        team = r["team"]
        prev = team_game[(team_game["team"].eq(team)) & (team_game["game_date"] < d)]
        row = {"player_game_key": r["player_game_key"]}
        for w in [7, 15, 30]:
            tail = prev.tail(w)
            row[f"team_offense_d{w}_hits_per_game"] = float(tail["team_hits"].mean()) if len(tail) else np.nan
        team_rows.append(row)
    out = out.merge(pd.DataFrame(team_rows), on="player_game_key", how="left")

    audit = pd.DataFrame(
        [
            {"feature_family": "batter_history", "rows": len(out), "complete_rows": int(out["d30_hits_per_pa"].notna().sum()), "strict_prior_rule": "source game_date < slate_date", "status": "PASS_WITH_SPARSE_HISTORY_LABELS"},
            {"feature_family": "opportunity", "rows": len(out), "complete_rows": int(out["d15_plate_appearances"].notna().sum()), "strict_prior_rule": "source game_date < slate_date; lineup role only when replayable pregame/projection source exists", "status": "PASS_WITH_LINEUP_LIMITATIONS"},
            {"feature_family": "opposing_starter", "rows": len(out), "complete_rows": int(out["starter_d30_hits_allowed_per_out"].notna().sum()), "strict_prior_rule": "starter source game_date < slate_date", "status": "PARTIAL"},
            {"feature_family": "team_offense_context", "rows": len(out), "complete_rows": int(out["team_offense_d30_hits_per_game"].notna().sum()), "strict_prior_rule": "team game_date < slate_date", "status": "PASS_WITH_SPARSE_HISTORY_LABELS"},
            {"feature_family": "bullpen_environment_bvp", "rows": len(out), "complete_rows": 0, "strict_prior_rule": "not materialized in this spine", "status": "NOT_INCLUDED_REQUIRES_SEPARATE_GOVERNED_SOURCE"},
        ]
    )
    return out, audit


def build_date_coverage(spine: pd.DataFrame, gi: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    rows = []
    all_dates = daterange(start, end)
    gi = gi.copy()
    if not gi.empty:
        gi["game_date"] = gi["game_date"].astype(str).str[:10]
    for d in all_dates:
        games = gi[gi["game_date"].eq(d)] if not gi.empty else pd.DataFrame()
        s = spine[spine["slate_date"].eq(d)]
        team_game_counts = Counter()
        if not games.empty:
            for _, g in games.iterrows():
                for t in [g.get("home_team_abbr"), g.get("away_team_abbr")]:
                    if pd.notna(t):
                        team_game_counts[str(t)] += 1
        slate_class = "REGULAR_SEASON_SLATE" if len(games) else ("ALL_STAR_BREAK_OR_NO_SLATE" if "2026-07-13" <= d <= "2026-07-16" else "NO_SLATE_DATE")
        rows.append(
            {
                "slate_date": d,
                "slate_classification": slate_class,
                "scheduled_games": int(len(games)),
                "doubleheader_team_count": sum(1 for v in team_game_counts.values() if v > 1),
                "eligible_hitter_rows": int(len(s)),
                "confirmed_lineup_rows": int(s["lineup_status"].eq("CONFIRMED_PREGAME_STARTER").sum()) if not s.empty else 0,
                "projected_lineup_rows": int(s["lineup_status"].eq("PROJECTED_PREGAME_STARTER").sum()) if not s.empty else 0,
                "outcome_qualified_rows": int((s["actual_plate_appearances"].fillna(0) > 0).sum()) if not s.empty else 0,
                "feature_complete_rows": int(s["model_ready_feature_status"].eq("FEATURE_COMPLETE_CORE").sum()) if not s.empty else 0,
                "feature_partial_rows": int(s["model_ready_feature_status"].eq("FEATURE_PARTIAL").sum()) if not s.empty else 0,
                "blocked_rows": int(s["training_admissibility"].str.contains("BLOCKED", na=False).sum()) if not s.empty else 0,
            }
        )
    return pd.DataFrame(rows)


def compare_market_population(spine: pd.DataFrame) -> pd.DataFrame:
    prior = read_csv(PRIOR_MARKET_POP)
    if prior.empty:
        return pd.DataFrame([{"comparison": "prior_market_population_missing", "rows": 0}])
    prior = prior.copy()
    prior_date = prior["slate_date"] if "slate_date" in prior else prior.get("date", "")
    prior["player_game_key"] = prior_date.astype(str).str[:10] + "|" + prior["game_id"].astype(str) + "|" + prior["player_id"].astype(str)
    keys_prior = set(prior["player_game_key"])
    keys_spine = set(spine["player_game_key"])
    both = spine[spine["player_game_key"].isin(keys_prior)]
    nonmarket_only = spine[~spine["player_game_key"].isin(keys_prior)]
    market_only = prior[~prior["player_game_key"].isin(keys_spine)]
    rows = [
        {"slice": "new_nonmarket_spine", "rows": len(spine), "avg_actual_hits": spine["actual_hits"].mean(), "avg_actual_pa": spine["actual_plate_appearances"].mean(), "two_plus_rate": (spine["actual_hits"].fillna(0) >= 2).mean(), "avg_d15_pa_pg": spine["d15_plate_appearances"].mean(), "selection_rate": np.nan, "notes": "all official baseball player-game hitter rows in period"},
        {"slice": "prior_market_conditioned_population", "rows": len(prior), "avg_actual_hits": pd.to_numeric(prior.get("actual_hits_uncapped", prior.get("official_hits", np.nan)), errors="coerce").mean(), "avg_actual_pa": np.nan, "two_plus_rate": (pd.to_numeric(prior.get("actual_hits_uncapped", prior.get("official_hits", np.nan)), errors="coerce").fillna(0) >= 2).mean(), "avg_d15_pa_pg": np.nan, "selection_rate": np.nan, "notes": "prior reconstruction population"},
        {"slice": "rows_in_both", "rows": len(both), "avg_actual_hits": both["actual_hits"].mean(), "avg_actual_pa": both["actual_plate_appearances"].mean(), "two_plus_rate": (both["actual_hits"].fillna(0) >= 2).mean(), "avg_d15_pa_pg": both["d15_plate_appearances"].mean(), "selection_rate": len(both) / max(1, len(spine)), "notes": "same player-game keys"},
        {"slice": "nonmarket_only", "rows": len(nonmarket_only), "avg_actual_hits": nonmarket_only["actual_hits"].mean(), "avg_actual_pa": nonmarket_only["actual_plate_appearances"].mean(), "two_plus_rate": (nonmarket_only["actual_hits"].fillna(0) >= 2).mean(), "avg_d15_pa_pg": nonmarket_only["d15_plate_appearances"].mean(), "selection_rate": len(nonmarket_only) / max(1, len(spine)), "notes": "not present in prior market-conditioned reconstruction"},
        {"slice": "market_conditioned_only_anomalies", "rows": len(market_only), "avg_actual_hits": np.nan, "avg_actual_pa": np.nan, "two_plus_rate": np.nan, "avg_d15_pa_pg": np.nan, "selection_rate": np.nan, "notes": "prior keys absent from new official player-game spine"},
        {"slice": "market_selection_rate", "rows": len(both), "avg_actual_hits": np.nan, "avg_actual_pa": np.nan, "two_plus_rate": np.nan, "avg_d15_pa_pg": np.nan, "selection_rate": len(both) / max(1, len(spine)), "notes": "fraction of broad nonmarket spine selected by prior market-conditioned population"},
    ]
    return pd.DataFrame(rows)


def recover_53(spine: pd.DataFrame) -> pd.DataFrame:
    old = read_csv(PRIOR_MISSING_53)
    if old.empty:
        return pd.DataFrame()
    rows = []
    for _, r in old.iterrows():
        d = str(r["slate_date"])
        s = spine[spine["slate_date"].eq(d)]
        full = int(s["model_ready_feature_status"].eq("FEATURE_COMPLETE_CORE").sum()) if not s.empty else 0
        partial = int(s["model_ready_feature_status"].eq("FEATURE_PARTIAL").sum()) if not s.empty else 0
        unresolved = int(s["model_ready_feature_status"].eq("FEATURE_BLOCKED").sum()) if not s.empty else 0
        if len(s) == 0:
            status = "NO_ELIGIBLE_SLATE"
        elif full > 0 and unresolved == 0:
            status = "FULL_NONMARKET_SPINE_RECOVERED"
        elif full > 0 or partial > 0:
            status = "PARTIAL_NONMARKET_SPINE_RECOVERED"
        else:
            status = "DENOMINATOR_RECOVERED_FEATURES_BLOCKED"
        unresolved_fams = []
        if full + partial < len(s):
            unresolved_fams.append("core_strict_prior")
        if s["lineup_status"].eq("LINEUP_STATUS_UNAVAILABLE").any():
            unresolved_fams.append("pregame_lineup")
        rows.append(
            {
                "slate_date": d,
                "prior_market_manifest_hit_rows": r.get("manifest_hit_rows", ""),
                "nonmarket_denominator_rows": len(s),
                "feature_complete_rows": full,
                "partial_rows": partial,
                "unresolved_rows": unresolved,
                "recovery_status": status,
                "unresolved_feature_families": "|".join(sorted(set(unresolved_fams))),
                "notes": "Old prepared-feature requirement removed; recovery judged on nonmarket player-game spine.",
            }
        )
    return pd.DataFrame(rows)


def model_manifest(spine: pd.DataFrame) -> pd.DataFrame:
    prohibited = sorted(MARKET_BANNED)
    feature_cols = [
        c for c in spine.columns
        if c not in IDENTITY_COLUMNS
        and c not in OUTCOME_COLUMNS
        and c not in {"player_game_key", "model_ready_feature_status", "training_admissibility", "admission_status"}
        and not any(tok in c.lower() for tok in prohibited)
    ]
    rows = []
    for c in feature_cols:
        if c.startswith("d") or c.startswith("season") or c.startswith("prior") or c.startswith("current"):
            fam = "batter_history"
        elif c.startswith("starter"):
            fam = "opposing_starter"
        elif c.startswith("team_offense"):
            fam = "team_offense_context"
        elif c in {"batting_order_position", "lineup_bucket"}:
            fam = "opportunity_lineup"
        else:
            fam = "context_or_identity_nonmarket"
        rows.append(
            {
                "feature_name": c,
                "feature_family": fam,
                "source_lineage": "mlb.player_stats/game_info strict-prior construction",
                "temporal_semantics": "strict-prior: source rows before slate_date unless lineage explicitly states pregame capture",
                "missing_value_policy": "retain null and expose feature status; no current/latest substitution",
                "current_replay_availability": "requires pregame lineup/source population; not market dependent",
                "historical_coverage_pct": round(100 * spine[c].notna().mean(), 2) if c in spine else 0,
                "prohibited_field_check": "PASS",
                "notes": "",
            }
        )
    for c in OUTCOME_COLUMNS:
        rows.append(
            {
                "feature_name": c,
                "feature_family": "outcome_field_excluded_from_features",
                "source_lineage": "mlb.player_stats official postgame row",
                "temporal_semantics": "postgame/evaluation only",
                "missing_value_policy": "required for supervised training target when applicable",
                "current_replay_availability": "not available pregame",
                "historical_coverage_pct": round(100 * spine[c].notna().mean(), 2) if c in spine else 0,
                "prohibited_field_check": "EXCLUDED",
                "notes": "Do not feed into feature matrix.",
            }
        )
    return pd.DataFrame(rows)


def validate_outputs(out_dir: Path) -> pd.DataFrame:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.suffix == ".csv":
            try:
                with path.open(newline="", encoding="utf-8") as f:
                    list(csv.reader(f))
                status = "PASS"
                notes = ""
            except Exception as exc:
                status = "FAIL"
                notes = f"{type(exc).__name__}: {exc}"
            rows.append({"artifact": rel(path), "validation": "csv_parse", "status": status, "notes": notes})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text(encoding="utf-8"))
                status = "PASS"
                notes = ""
            except Exception as exc:
                status = "FAIL"
                notes = f"{type(exc).__name__}: {exc}"
            rows.append({"artifact": rel(path), "validation": "json_parse", "status": status, "notes": notes})
        elif path.suffix == ".md":
            rows.append({"artifact": rel(path), "validation": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "notes": ""})
    return pd.DataFrame(rows)


def build(out_dir: Path, start: str, end: str, current_date: str) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    history_start = (date.fromisoformat(start) - timedelta(days=60)).isoformat()
    sources = fetch_sources(start, end, history_start, current_date)
    denominator = build_denominator(sources.player_stats, sources.game_info, start, end)
    denominator = attach_lineup_semantics(denominator)
    spine, strict_audit = add_strict_prior_features(denominator, sources.player_stats)
    core_cols = ["d15_hits_per_pa", "d15_plate_appearances", "season_to_date_hits_per_pa", "season_to_date_pa_per_game"]
    spine["model_ready_feature_status"] = np.where(spine[core_cols].notna().all(axis=1), "FEATURE_COMPLETE_CORE", "FEATURE_PARTIAL")
    spine.loc[spine["actual_plate_appearances"].fillna(0) <= 0, "model_ready_feature_status"] = "FEATURE_BLOCKED"
    spine["admission_status"] = np.where(spine["actual_plate_appearances"].fillna(0) > 0, "OUTCOME_QUALIFIED_APPEARED_HITTER", "ZERO_PA_OR_NON_BATTING_APPEARANCE")
    spine["training_admissibility"] = np.where(
        (spine["actual_plate_appearances"].fillna(0) > 0) & spine["model_ready_feature_status"].ne("FEATURE_BLOCKED"),
        "ADMISSIBLE_FOR_HISTORICAL_TRAINING_WITH_APPEARANCE_DENOMINATOR_DISCLOSURE",
        "BLOCKED_ZERO_PA_OR_FEATURE_INCOMPLETE",
    )

    player_game_cols = [
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "game_start_time",
        "is_home",
        "position",
        "batting_side",
        "opposing_starter_id",
        "opposing_starter_name",
        "opposing_starter_identity_semantics",
        "opposing_starter_source",
        "lineup_status",
        "lineup_semantics_source",
        "lineup_source_timestamp",
        "batting_order_position",
        "lineup_bucket",
        "player_appearance_status",
        "admission_status",
        "training_admissibility",
        "model_ready_feature_status",
    ] + [c for c in spine.columns if c.startswith(("d7_", "d15_", "d30_", "season_", "starter_", "team_offense_", "feature_cutoff", "latest_contributing", "prior_game", "strict_prior"))] + OUTCOME_COLUMNS
    player_game_cols = [c for c in dict.fromkeys(player_game_cols) if c in spine.columns]
    player_game = spine[player_game_cols].copy()

    lineup_ledger = player_game[[
        "player_game_key", "slate_date", "game_id", "player_id", "player_name", "team", "opponent",
        "lineup_status", "batting_order_position", "lineup_bucket", "lineup_semantics_source", "lineup_source_timestamp",
        "actual_lineup_position", "player_appearance_status", "training_admissibility",
    ]].copy()
    outcome_ledger = player_game[[
        "player_game_key", "slate_date", "game_id", "player_id", "player_name", "team", "opponent",
        "actual_hits", "actual_plate_appearances", "actual_at_bats", "actual_lineup_position",
        "appeared_in_game", "zero_pa_status", "actual_hits_class",
    ]].copy()
    date_cov = build_date_coverage(player_game, sources.game_info, start, end)
    compare = compare_market_population(player_game)
    missing53 = recover_53(player_game)
    manifest = model_manifest(player_game)

    source_inventory = pd.DataFrame(
        [
            {"source": "mlb.player_stats", "source_type": "database_read", "role": "official batting outcomes, historical strict-prior source, and postgame actual starter identity fallback", "rows_read": len(sources.player_stats), "date_range": f"{history_start}..{current_date}", "market_independent": True, "notes": "Read-only. Same-game starter identity is labeled as retrospective binding lineage, not a pregame confirmation source."},
            {"source": "mlb.game_info", "source_type": "database_read", "role": "game identity, start time, home/away teams, probable/starting pitcher IDs when populated", "rows_read": len(sources.game_info), "date_range": f"{start}..{current_date}", "market_independent": True, "notes": "Read-only. Starting pitcher ID columns were not populated for the target period in this local source."},
            {"source": rel(LINEUP_LEDGER), "source_type": "artifact", "role": "historical lineup semantics ledger where available", "rows_read": len(read_csv(LINEUP_LEDGER)), "date_range": "2026-05-01..2026-07-09", "market_independent": True, "notes": "Mostly projected/unknown semantics; not treated as confirmed unless ledger says confirmed."},
            {"source": rel(LIVE_PARENT_0718), "source_type": "artifact", "role": "governed July 18 confirmed pregame lineup/live parent rows", "rows_read": len(read_csv(LIVE_PARENT_0718)), "date_range": "2026-07-18", "market_independent": True, "notes": "Used only as pregame lineage for matching player-games."},
            {"source": rel(PRIOR_MARKET_POP), "source_type": "artifact", "role": "comparison only; not denominator or feature source", "rows_read": len(read_csv(PRIOR_MARKET_POP)), "date_range": "prior reconstruction", "market_independent": False, "notes": "Used only after spine construction for selection comparison."},
        ]
    )

    feature_registry = pd.DataFrame(
        [
            {"feature_family": "batter_history", "fields": "d7/d15/d30/season hit, PA, hit-per-PA, component-hit rates", "source": "mlb.player_stats", "temporal_rule": "source game_date < slate_date", "status": "INCLUDED"},
            {"feature_family": "opportunity", "fields": "rolling PA/G, lineup status/bucket when replayable", "source": "mlb.player_stats + governed lineup artifacts", "temporal_rule": "rolling source game_date < slate_date; lineup only if pregame/projection lineage exists", "status": "INCLUDED_WITH_LINEUP_LIMITATIONS"},
            {"feature_family": "opposing_starter", "fields": "starter prior starts, outs/start, hits allowed/out, earned runs/start", "source": "mlb.player_stats + mlb.game_info", "temporal_rule": "pitcher source game_date < slate_date", "status": "PARTIAL"},
            {"feature_family": "team_offense_context", "fields": "team rolling hits/game", "source": "mlb.player_stats", "temporal_rule": "team source game_date < slate_date", "status": "INCLUDED"},
            {"feature_family": "bullpen_weather_park_bvp", "fields": "not frozen here", "source": "requires separate governed source", "temporal_rule": "not applicable", "status": "EXCLUDED_PENDING_GOVERNANCE"},
        ]
    )

    current_rows = []
    live_current = ROOT / f"artifacts/analysis/model_development/mlb_live_hitter_parent_daily_integration/{current_date}/live_hitter_parent_artifact_{current_date}.csv"
    live_df = read_csv(live_current)
    if not live_df.empty:
        current_rows = live_df.to_dict("records")
        current_replay = live_df.copy()
        current_replay["current_replay_status"] = np.where(current_replay.get("parent_row_status", "").astype(str).eq("COMPLETE"), "FEATURE_READY_NO_MARKET_REQUIRED", "WITHHELD")
    else:
        games_current = sources.game_info[sources.game_info["game_date"].astype(str).str[:10].eq(current_date)].copy()
        current_replay = pd.DataFrame(
            [
                {
                    "slate_date": current_date,
                    "game_id": g.get("game_id"),
                    "current_replay_status": "WITHHELD_NO_GOVERNED_CURRENT_NONMARKET_LINEUP_SOURCE",
                    "withheld_reason": "No live hitter parent/current pregame lineup artifact found for current replay date.",
                }
                for _, g in games_current.iterrows()
            ]
        )

    decisions = {
        "MLB_HITS_NONMARKET_SPINE_DENOMINATOR_DECISION": "BROAD_OFFICIAL_PLAYER_GAME_HITTER_DENOMINATOR_CONSTRUCTED_FROM_BASEBALL_TABLES",
        "MLB_HITS_NONMARKET_SPINE_LINEUP_SEMANTICS_DECISION": "PREGAME_CONFIRMED_ONLY_WHEN_GOVERNED_SOURCE_EXISTS_OTHERWISE_PROJECTED_OR_UNAVAILABLE",
        "MLB_HITS_NONMARKET_SPINE_FEATURE_RECOVERY_DECISION": "STRICT_PRIOR_CORE_FEATURES_RECOVERED_FOR_BROAD_SPINE_WITH_SPARSE_HISTORY_LABELS",
        "MLB_HITS_NONMARKET_SPINE_TEMPORAL_SAFETY_DECISION": "PASS_NO_MARKET_FIELDS_STRICT_PRIOR_FEATURE_DATES_WITH_STARTER_IDENTITY_SEMANTICS_DISCLOSED",
        "MLB_HITS_NONMARKET_SPINE_OUTCOME_ATTACHMENT_DECISION": "OUTCOMES_ATTACHED_AFTER_FEATURE_CONSTRUCTION_AS_EXCLUDED_FIELDS",
        "MLB_HITS_NONMARKET_SPINE_POPULATION_QUALITY_DECISION": "BROAD_SPINE_CERTIFIED_WITH_LINEUP_AND_STARTER_PARTIALITY_DISCLOSED",
        "MLB_HITS_NONMARKET_SPINE_MARKET_SELECTION_COMPARISON_DECISION": "MARKET_CONDITIONED_POPULATION_IS_SMALLER_AND_SELECTED_SUBSET",
        "MLB_HITS_NONMARKET_SPINE_53_DATE_RECOVERY_DECISION": "MISSING_MARKET_FEATURE_DATES_REASSESSED_USING_NONMARKET_SPINE",
        "MLB_HITS_NONMARKET_SPINE_CURRENT_REPLAY_DECISION": "CURRENT_REPLAY_REQUIRES_GOVERNED_CURRENT_NONMARKET_LINEUP_SOURCE",
        "MLB_HITS_NONMARKET_SPINE_MODEL_READY_CONTRACT_DECISION": "FROZEN_CONTRACT_CREATED_NO_MODEL_FIT_AUTHORIZED",
        "MLB_HITS_NONMARKET_SPINE_NEXT_MODELING_DECISION": "NEXT_STEP_TRAIN_MARKET_INDEPENDENT_HITS_MODEL_FROM_FROZEN_SPINE_AFTER_HUMAN_APPROVAL",
        "MLB_PRODUCTION_STATUS": "UNCHANGED",
    }
    decisions_df = pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()])

    pop_quality = pd.DataFrame(
        [
            {"metric": "date_start", "value": start, "notes": ""},
            {"metric": "date_end", "value": end, "notes": ""},
            {"metric": "dates", "value": player_game["slate_date"].nunique(), "notes": ""},
            {"metric": "games", "value": player_game["game_id"].nunique(), "notes": ""},
            {"metric": "unique_hitters", "value": player_game["player_id"].nunique(), "notes": ""},
            {"metric": "player_game_rows", "value": len(player_game), "notes": ""},
            {"metric": "feature_complete_core_rows", "value": int(player_game["model_ready_feature_status"].eq("FEATURE_COMPLETE_CORE").sum()), "notes": ""},
            {"metric": "feature_partial_rows", "value": int(player_game["model_ready_feature_status"].eq("FEATURE_PARTIAL").sum()), "notes": ""},
            {"metric": "feature_blocked_rows", "value": int(player_game["model_ready_feature_status"].eq("FEATURE_BLOCKED").sum()), "notes": ""},
            {"metric": "confirmed_pregame_lineup_rows", "value": int(player_game["lineup_status"].eq("CONFIRMED_PREGAME_STARTER").sum()), "notes": ""},
            {"metric": "projected_pregame_lineup_rows", "value": int(player_game["lineup_status"].eq("PROJECTED_PREGAME_STARTER").sum()), "notes": ""},
            {"metric": "lineup_unavailable_rows", "value": int(player_game["lineup_status"].eq("LINEUP_STATUS_UNAVAILABLE").sum()), "notes": ""},
            {"metric": "zero_hit_rows", "value": int(player_game["actual_hits"].fillna(0).eq(0).sum()), "notes": ""},
            {"metric": "one_hit_rows", "value": int(player_game["actual_hits"].fillna(0).eq(1).sum()), "notes": ""},
            {"metric": "two_hit_rows", "value": int(player_game["actual_hits"].fillna(0).eq(2).sum()), "notes": ""},
            {"metric": "three_plus_hit_rows", "value": int(player_game["actual_hits"].fillna(0).ge(3).sum()), "notes": ""},
            {"metric": "prior_market_conditioned_rows", "value": 2887, "notes": "From governing result."},
            {"metric": "size_multiple_vs_prior_market_population", "value": round(len(player_game) / 2887, 3), "notes": ""},
        ]
    )

    paths = {
        "denominator_source_inventory": out_dir / f"denominator_source_inventory_{RUN_DATE}.csv",
        "player_game_denominator": out_dir / f"player_game_denominator_{RUN_DATE}.csv",
        "lineup_semantics_ledger": out_dir / f"lineup_semantics_ledger_{RUN_DATE}.csv",
        "feature_source_lineage_registry": out_dir / f"feature_source_lineage_registry_{RUN_DATE}.csv",
        "strict_prior_audit": out_dir / f"strict_prior_audit_{RUN_DATE}.csv",
        "outcome_attachment_ledger": out_dir / f"outcome_attachment_ledger_{RUN_DATE}.csv",
        "date_level_coverage_matrix": out_dir / f"date_level_coverage_matrix_{RUN_DATE}.csv",
        "population_quality_summary": out_dir / f"population_quality_summary_{RUN_DATE}.csv",
        "market_selection_comparison": out_dir / f"market_selection_comparison_{RUN_DATE}.csv",
        "corrected_53_date_recovery_inventory": out_dir / f"corrected_53_date_recovery_inventory_{RUN_DATE}.csv",
        "current_replay_spine": out_dir / f"current_replay_spine_{RUN_DATE}.csv",
        "frozen_model_ready_manifest": out_dir / f"frozen_model_ready_manifest_{RUN_DATE}.csv",
        "required_decisions": out_dir / f"required_decisions_{RUN_DATE}.csv",
    }
    for df, key in [
        (source_inventory, "denominator_source_inventory"),
        (player_game, "player_game_denominator"),
        (lineup_ledger, "lineup_semantics_ledger"),
        (feature_registry, "feature_source_lineage_registry"),
        (strict_audit, "strict_prior_audit"),
        (outcome_ledger, "outcome_attachment_ledger"),
        (date_cov, "date_level_coverage_matrix"),
        (pop_quality, "population_quality_summary"),
        (compare, "market_selection_comparison"),
        (missing53, "corrected_53_date_recovery_inventory"),
        (current_replay, "current_replay_spine"),
        (manifest, "frozen_model_ready_manifest"),
        (decisions_df, "required_decisions"),
    ]:
        write_csv(df, paths[key])

    machine = {
        "generated_at_utc": now_utc(),
        "date_start": start,
        "date_end": end,
        "player_game_rows": int(len(player_game)),
        "dates": int(player_game["slate_date"].nunique()),
        "games": int(player_game["game_id"].nunique()),
        "unique_hitters": int(player_game["player_id"].nunique()),
        "feature_complete_core_rows": int(player_game["model_ready_feature_status"].eq("FEATURE_COMPLETE_CORE").sum()),
        "feature_partial_rows": int(player_game["model_ready_feature_status"].eq("FEATURE_PARTIAL").sum()),
        "confirmed_pregame_lineup_rows": int(player_game["lineup_status"].eq("CONFIRMED_PREGAME_STARTER").sum()),
        "projected_pregame_lineup_rows": int(player_game["lineup_status"].eq("PROJECTED_PREGAME_STARTER").sum()),
        "prior_market_conditioned_rows": 2887,
        "size_multiple_vs_prior_market_population": round(len(player_game) / 2887, 3),
        "current_replay_rows": int(len(current_replay)),
        "decisions": decisions,
    }
    write_json(machine, out_dir / f"machine_readable_hits_nonmarket_spine_{RUN_DATE}.json")

    md = f"""# MLB Hits Nonmarket Player-Game Feature Spine

Generated: `{machine['generated_at_utc']}`

## Executive Summary

This package builds a broad player-game hitter spine for `{start}` through `{end}` from baseball sources only. It does not use sportsbook availability, proposition lines, odds, bookmaker identity, candidate selection, upload state, or prior model predictions.

Rows: **{machine['player_game_rows']}** player-games across **{machine['dates']}** dates, **{machine['games']}** games, and **{machine['unique_hitters']}** hitters.

Compared with the prior market-conditioned reconstruction population of `2,887` rows, this spine is **{machine['size_multiple_vs_prior_market_population']}x** larger.

## Lineup Semantics

Confirmed pregame lineup rows are retained only where governed pregame lineage exists. Projected lineup rows are labeled separately. Historical player-game appearance and actual batting facts are preserved as outcome/evaluation fields, not feature inputs.

## Feature Contract

Strict-prior feature families included here are batter history, PA opportunity from prior games, opposing starter history, and team offense context. In this local historical range, `mlb.game_info` did not populate starter IDs, so opposing starter identity is recovered from official same-game starter rows and labeled as retrospective binding lineage; the starter profile values themselves are still computed only from games before `slate_date`. Bullpen, weather, park, and generalized matchup fields remain excluded pending separate governed source certification.

## Current Replay

Current replay does not require market data, but it does require a governed current nonmarket player population such as confirmed/projected pregame lineup or live hitter parent output. The current replay artifact reports availability or withheld reasons without generating odds-dependent outputs.

## Decisions

{markdown_table(decisions_df)}

## No Behavior Changed

No model was fit, no production artifacts were modified, no sportsbook data was used, and no database writes were performed.
"""
    (out_dir / f"hits_nonmarket_player_game_feature_spine_{RUN_DATE}.md").write_text(md, encoding="utf-8")

    sha_rows = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            sha_rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(pd.DataFrame(sha_rows), out_dir / f"sha256_manifest_{RUN_DATE}.csv")
    validation = validate_outputs(out_dir)
    write_csv(validation, out_dir / f"validation_report_{RUN_DATE}.csv")
    return machine


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default=START_DATE)
    parser.add_argument("--end-date", default=END_DATE)
    parser.add_argument("--current-date", default=CURRENT_REPLAY_DATE)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--mode", choices=["research_only"], default="research_only")
    args = parser.parse_args()
    result = build(Path(args.output_dir), args.start_date, args.end_date, args.current_date)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
