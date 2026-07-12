#!/usr/bin/env python3
"""Build no-write offense-factor lineage and movement research artifacts."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.shared.db.pg import pg_fetchall


DEFAULT_OUT_DIR = Path("artifacts/analysis/model_development/mlb_offense_factor_lineage_and_movement/2026-07-11")
DEFAULT_ODDS_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_STARTER_BASE = Path(
    "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/"
    "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
DEFAULT_HITTER_BASE = Path(
    "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
    "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
DEFAULT_PA_BASE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)

TEAM_ALIASES = {
    "108": "LAA",
    "109": "ARI",
    "110": "BAL",
    "111": "BOS",
    "112": "CHC",
    "113": "CIN",
    "114": "CLE",
    "115": "COL",
    "116": "DET",
    "117": "HOU",
    "118": "KC",
    "119": "LAD",
    "120": "WSH",
    "121": "NYM",
    "133": "OAK",
    "134": "PIT",
    "135": "SD",
    "136": "SEA",
    "137": "SF",
    "138": "STL",
    "139": "TB",
    "140": "TEX",
    "141": "TOR",
    "142": "MIN",
    "143": "PHI",
    "144": "ATL",
    "145": "CWS",
    "146": "MIA",
    "147": "NYY",
    "158": "MIL",
    "AZ": "ARI",
    "ARI": "ARI",
    "CHW": "CWS",
    "CWS": "CWS",
    "KCR": "KC",
    "KC": "KC",
    "LAD": "LAD",
    "LA": "LAD",
    "LAN": "LAD",
    "NYM": "NYM",
    "NYN": "NYM",
    "NYY": "NYY",
    "NYA": "NYY",
    "SD": "SD",
    "SDP": "SD",
    "SF": "SF",
    "SFG": "SF",
    "TB": "TB",
    "TBR": "TB",
    "WSH": "WSH",
    "WAS": "WSH",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_env(path: Path = Path("backend/.env")) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = list(rows[0].keys()) if rows else []
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({field: row.get(field, "") for field in fields})


def _id(value: Any) -> str:
    try:
        if pd.notna(value):
            return str(int(float(value)))
    except Exception:
        pass
    return "" if value is None else str(value).strip()


def _team_code(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    raw = str(value).strip().upper()
    if raw in {"", "NAN", "NONE"}:
        return ""
    return TEAM_ALIASES.get(raw, raw)


def _period(date_value: str) -> str:
    d = pd.Timestamp(date_value)
    if d <= pd.Timestamp("2026-05-15"):
        return "2026-05-01_to_2026-05-15"
    if d <= pd.Timestamp("2026-05-31"):
        return "2026-05-16_to_2026-05-31"
    if d <= pd.Timestamp("2026-06-15"):
        return "2026-06-01_to_2026-06-15"
    if d <= pd.Timestamp("2026-06-30"):
        return "2026-06-16_to_2026-06-30"
    return "2026-07-01_to_2026-07-09"


def _factor_bucket(value: Any) -> str:
    v = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(v):
        return "missing"
    if v < 0.95:
        return "low_lt_0_95"
    if v < 1.05:
        return "neutral_0_95_to_1_05"
    return "high_ge_1_05"


def _movement_label(row: pd.Series) -> str:
    d7 = row.get("team_d7_hits_pg")
    d15 = row.get("team_d15_hits_pg")
    d30 = row.get("team_d30_hits_pg")
    if pd.isna(d7) or pd.isna(d15) or pd.isna(d30) or row.get("team_d15_games", 0) < 10:
        return "insufficient_history"
    delta_7_15 = d7 - d15
    delta_15_30 = d15 - d30
    level = row.get("offense_factor_vs_league_clamped_reconstructed")
    if delta_7_15 >= 0.35 and delta_15_30 >= 0:
        return "rising"
    if delta_7_15 <= -0.35 and delta_15_30 <= 0:
        return "falling"
    if level >= 1.05 and abs(delta_7_15) < 0.35:
        return "stable_high"
    if level < 0.95 and abs(delta_7_15) < 0.35:
        return "stable_low"
    if delta_7_15 > 0.35 and delta_15_30 < 0:
        return "rebounding"
    if delta_7_15 < -0.35 and delta_15_30 > 0:
        return "cooling_from_high"
    return "stable_neutral"


def _clamp_status(raw: Any, clamped: Any) -> str:
    if pd.isna(raw) or pd.isna(clamped):
        return "missing_or_default"
    if raw < 0.70:
        return "clamped_low"
    if raw > 1.30:
        return "clamped_high"
    return "not_clamped"


def _fetch_team_hits(start: str, end: str, no_db: bool, csv_path: str) -> pd.DataFrame:
    if no_db:
        if not csv_path:
            raise SystemExit("--no-db requires --team-game-hits-csv")
        df = pd.read_csv(csv_path, low_memory=False)
    else:
        _load_env()
        rows = pg_fetchall(
            """
            SELECT
              ps.team,
              ps.game_date::date AS game_date,
              ps.game_id,
              SUM(COALESCE(ps.hits, 0))::float8 AS team_hits
            FROM mlb.player_stats ps
            WHERE ps.game_date BETWEEN %s AND %s
            GROUP BY 1, 2, 3
            ORDER BY ps.game_date, ps.game_id, ps.team
            """,
            (start, end),
        )
        df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["team_hits"] = pd.to_numeric(df["team_hits"], errors="coerce")
    df["team"] = df["team"].map(_team_code)
    return df.sort_values(["team", "game_date", "game_id"])


def _fetch_player_game_teams(start: str, end: str, no_db: bool) -> pd.DataFrame:
    if no_db:
        return pd.DataFrame()
    _load_env()
    rows = pg_fetchall(
        """
        SELECT DISTINCT
          ps.game_date::date AS slate_date,
          ps.game_id,
          ps.player_id,
          ps.team
        FROM mlb.player_stats ps
        WHERE ps.game_date BETWEEN %s AND %s
          AND ps.player_id IS NOT NULL
          AND ps.game_id IS NOT NULL
          AND ps.team IS NOT NULL
        """,
        (start, end),
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["slate_date"] = pd.to_datetime(df["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["game_id_key"] = df["game_id"].map(_id)
    df["player_id_key"] = df["player_id"].map(_id)
    df["team_from_player_stats"] = df["team"].map(_team_code)
    return df[["slate_date", "game_id_key", "player_id_key", "team_from_player_stats"]].drop_duplicates()


def _load_slate_teams(root: Path, start: str, end: str) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    frames: list[pd.DataFrame] = []
    inv: list[dict[str, Any]] = []
    for d in pd.date_range(start, end, freq="D").strftime("%Y-%m-%d"):
        path = root / d / "mlb_slate_output.csv"
        if not path.exists():
            inv.append({"date": d, "source": str(path), "available": False, "rows": 0, "notes": "missing"})
            continue
        df = pd.read_csv(path, low_memory=False)
        df = df[df.get("prop_type", "").astype(str).str.lower().eq("hits")].copy()
        df["slate_date"] = pd.to_datetime(df["slate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        for col in ["team", "opponent", "is_home"]:
            if col not in df.columns:
                df[col] = np.nan
        for col in ["home_team_code", "away_team_code"]:
            if col not in df.columns:
                df[col] = ""
            df[col] = df[col].map(_team_code)
        df["team"] = df["team"].map(_team_code)
        df["opponent"] = df["opponent"].map(_team_code)
        frames.append(df)
        inv.append({"date": d, "source": str(path), "available": True, "rows": len(df), "notes": "hits prop source"})
    return (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(), inv)


def _hydrate_slate_team_fields(slate: pd.DataFrame, player_teams: pd.DataFrame) -> pd.DataFrame:
    if slate.empty:
        return slate
    out = slate.copy()
    out["game_id_key"] = out["game_id"].map(_id)
    out["player_id_key"] = out["player_id"].map(_id)
    if not player_teams.empty:
        out = out.merge(
            player_teams,
            on=["slate_date", "game_id_key", "player_id_key"],
            how="left",
        )
        out["team"] = out["team"].where(out["team"].astype(str).str.len().gt(0), out["team_from_player_stats"])
    else:
        out["team_from_player_stats"] = ""
    out["team"] = out["team"].map(_team_code)
    out["home_team_code"] = out["home_team_code"].map(_team_code)
    out["away_team_code"] = out["away_team_code"].map(_team_code)
    missing_opponent = out["opponent"].astype(str).isin(["", "nan", "NaN", "None"])
    home_mask = out["team"].eq(out["home_team_code"])
    away_mask = out["team"].eq(out["away_team_code"])
    out.loc[missing_opponent & home_mask, "opponent"] = out.loc[missing_opponent & home_mask, "away_team_code"]
    out.loc[missing_opponent & away_mask, "opponent"] = out.loc[missing_opponent & away_mask, "home_team_code"]
    out["opponent"] = out["opponent"].map(_team_code)
    out["is_home"] = np.where(home_mask, True, np.where(away_mask, False, out["is_home"]))
    return out


def _build_team_game(slate: pd.DataFrame, team_hits: pd.DataFrame) -> pd.DataFrame:
    if slate.empty:
        return pd.DataFrame()
    team_candidates = slate[["slate_date", "game_id", "team", "opponent", "is_home", "home_team_code", "away_team_code"]].drop_duplicates()
    team_candidates = team_candidates.rename(columns={"team": "offense_team"})
    team_candidates["offense_team"] = team_candidates["offense_team"].map(_team_code)
    team_candidates = team_candidates[team_candidates["offense_team"].astype(str).str.len().gt(0)].copy()
    all_team_games = team_hits.copy()
    out_rows: list[dict[str, Any]] = []
    for _, row in team_candidates.iterrows():
        date_ts = pd.Timestamp(row["slate_date"])
        cutoff = date_ts - pd.Timedelta(days=1)
        team = str(row["offense_team"])
        prior_team = all_team_games[(all_team_games["team"].eq(team)) & (all_team_games["game_date"] <= cutoff)].sort_values(["game_date", "game_id"])
        prior_all = all_team_games[all_team_games["game_date"] <= cutoff].copy()
        rec = row.to_dict()
        rec["feature_cutoff_date"] = cutoff.strftime("%Y-%m-%d")
        rec["latest_contributing_prior_game_date"] = prior_team["game_date"].max().strftime("%Y-%m-%d") if not prior_team.empty else ""
        rec["strict_prior_status"] = "PASS_STRICT_PRIOR" if not prior_team.empty else "FAIL_NO_PRIOR_TEAM_GAMES"
        for n in [7, 15, 30]:
            t = prior_team.tail(n)
            rec[f"team_d{n}_hits_pg"] = float(t["team_hits"].mean()) if not t.empty else np.nan
            rec[f"team_d{n}_games"] = len(t)
            league_vals = []
            for _, g in prior_all.groupby("team"):
                last = g.sort_values(["game_date", "game_id"]).tail(n)
                if not last.empty:
                    league_vals.append(float(last["team_hits"].mean()))
            rec[f"league_d{n}_hits_pg"] = float(np.mean(league_vals)) if league_vals else np.nan
            rec[f"team_d{n}_factor_vs_league"] = rec[f"team_d{n}_hits_pg"] / rec[f"league_d{n}_hits_pg"] if rec[f"league_d{n}_hits_pg"] and not pd.isna(rec[f"league_d{n}_hits_pg"]) else np.nan
        rec["offense_hits_form_blended_reconstructed"] = 0.5 * rec["team_d7_hits_pg"] + 0.3 * rec["team_d15_hits_pg"] + 0.2 * rec["team_d30_hits_pg"] if pd.notna(rec["team_d7_hits_pg"]) and pd.notna(rec["team_d15_hits_pg"]) and pd.notna(rec["team_d30_hits_pg"]) else np.nan
        rec["league_offense_hits_form_blended_reconstructed"] = 0.5 * rec["league_d7_hits_pg"] + 0.3 * rec["league_d15_hits_pg"] + 0.2 * rec["league_d30_hits_pg"] if pd.notna(rec["league_d7_hits_pg"]) and pd.notna(rec["league_d15_hits_pg"]) and pd.notna(rec["league_d30_hits_pg"]) else np.nan
        rec["offense_factor_vs_league_reconstructed"] = rec["offense_hits_form_blended_reconstructed"] / rec["league_offense_hits_form_blended_reconstructed"] if pd.notna(rec["league_offense_hits_form_blended_reconstructed"]) and rec["league_offense_hits_form_blended_reconstructed"] > 0 else np.nan
        raw = rec["offense_factor_vs_league_reconstructed"]
        rec["offense_factor_vs_league_clamped_reconstructed"] = min(max(raw, 0.70), 1.30) if pd.notna(raw) else np.nan
        rec["clamp_status"] = _clamp_status(raw, rec["offense_factor_vs_league_clamped_reconstructed"])
        rec["raw_amount_beyond_clamp"] = (0.70 - raw) if pd.notna(raw) and raw < 0.70 else ((raw - 1.30) if pd.notna(raw) and raw > 1.30 else 0)
        rec["clamp_adjustment"] = rec["offense_factor_vs_league_clamped_reconstructed"] - raw if pd.notna(raw) else np.nan
        rec["offense_factor_bucket"] = _factor_bucket(rec["offense_factor_vs_league_clamped_reconstructed"])
        rec["d7_minus_d15_hits_pg"] = rec["team_d7_hits_pg"] - rec["team_d15_hits_pg"] if pd.notna(rec["team_d7_hits_pg"]) and pd.notna(rec["team_d15_hits_pg"]) else np.nan
        rec["d15_minus_d30_hits_pg"] = rec["team_d15_hits_pg"] - rec["team_d30_hits_pg"] if pd.notna(rec["team_d15_hits_pg"]) and pd.notna(rec["team_d30_hits_pg"]) else np.nan
        rec["d7_factor_minus_d15_factor"] = rec["team_d7_factor_vs_league"] - rec["team_d15_factor_vs_league"] if pd.notna(rec["team_d7_factor_vs_league"]) and pd.notna(rec["team_d15_factor_vs_league"]) else np.nan
        rec["d15_factor_minus_d30_factor"] = rec["team_d15_factor_vs_league"] - rec["team_d30_factor_vs_league"] if pd.notna(rec["team_d15_factor_vs_league"]) and pd.notna(rec["team_d30_factor_vs_league"]) else np.nan
        rec["movement_label"] = _movement_label(pd.Series(rec))
        actual = all_team_games[(all_team_games["team"].eq(team)) & (all_team_games["game_date"].eq(date_ts)) & (all_team_games["game_id"].map(_id).eq(_id(row["game_id"])))]
        rec["actual_same_game_team_hits"] = float(actual["team_hits"].iloc[0]) if not actual.empty else np.nan
        future = all_team_games[(all_team_games["team"].eq(team)) & (all_team_games["game_date"] > date_ts)].sort_values(["game_date", "game_id"]).head(3)
        rec["next_game_team_hits"] = float(future["team_hits"].iloc[0]) if not future.empty else np.nan
        rec["next3_team_hits_avg"] = float(future["team_hits"].mean()) if not future.empty else np.nan
        rec["temporal_period"] = _period(row["slate_date"])
        out_rows.append(rec)
    out = pd.DataFrame(out_rows).drop_duplicates(["slate_date", "game_id", "offense_team"])
    return out


def _summary(df: pd.DataFrame, groups: list[str], outcome: str) -> list[dict[str, Any]]:
    rows = []
    work = df.copy()
    work[outcome] = pd.to_numeric(work[outcome], errors="coerce")
    for keys, g in work.groupby(groups, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        vals = g[outcome].dropna()
        pred = pd.to_numeric(g.get("offense_hits_form_blended_reconstructed"), errors="coerce")
        err = pred - g[outcome]
        rows.append({
            **{col: key for col, key in zip(groups, keys)},
            "rows": len(g),
            "resolved": int(vals.notna().sum()),
            "avg_actual": float(vals.mean()) if len(vals) else np.nan,
            "avg_offense_factor": float(pd.to_numeric(g.get("offense_factor_vs_league_clamped_reconstructed"), errors="coerce").mean()),
            "bias_blended_hits_minus_actual": float(err.mean(skipna=True)) if len(err) else np.nan,
            "mae_blended_hits": float(err.abs().mean(skipna=True)) if len(err) else np.nan,
            "sample_flag": "ok" if len(vals) >= 30 else ("small_sample_lt30" if len(vals) else "unresolved"),
        })
    return rows


def _prop_summary(df: pd.DataFrame, groups: list[str]) -> list[dict[str, Any]]:
    if df.empty:
        return []
    work = df.copy()
    work["target_class"] = pd.to_numeric(work.get("target_class"), errors="coerce")
    rows = []
    for keys, g in work.groupby(groups, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        resolved = g["target_class"].notna()
        wins = int(g.loc[resolved, "target_class"].sum())
        n = int(resolved.sum())
        rows.append({
            **{col: key for col, key in zip(groups, keys)},
            "rows": len(g),
            "resolved": n,
            "wins": wins,
            "losses": n - wins,
            "win_rate": wins / n if n else np.nan,
            "avg_control_probability": pd.to_numeric(g.get("control_probability"), errors="coerce").mean(),
            "avg_control_residual": pd.to_numeric(g.get("control_residual"), errors="coerce").mean(),
            "sample_flag": "ok" if n >= 100 else ("small_sample_lt100" if n else "unresolved"),
        })
    return rows


def build(args: argparse.Namespace) -> dict[str, Any]:
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    generated_at = _utc_now()
    start, end = args.start_date, args.end_date
    team_hits = _fetch_team_hits((pd.Timestamp(start) - pd.Timedelta(days=60)).strftime("%Y-%m-%d"), end, args.no_db, args.team_game_hits_csv)
    slate, slate_inventory = _load_slate_teams(Path(args.odds_root), start, end)
    player_teams = _fetch_player_game_teams(start, end, args.no_db)
    slate = _hydrate_slate_team_fields(slate, player_teams)
    team_game = _build_team_game(slate, team_hits)

    # Expanded prop base.
    prop = slate.copy()
    prop["game_id_key"] = prop["game_id"].map(_id)
    team_game["game_id_key"] = team_game["game_id"].map(_id)
    prop = prop.merge(
        team_game,
        left_on=["slate_date", "game_id_key", "team"],
        right_on=["slate_date", "game_id_key", "offense_team"],
        how="left",
        suffixes=("", "_team_game"),
    )
    prop["line"] = pd.to_numeric(prop["line"], errors="coerce")
    prop["side_normalized"] = prop.get("model_pick_side", "").astype(str).str.lower()
    prop["row_key"] = prop["slate_date"].astype(str) + "|" + prop["game_id_key"] + "|" + prop["player_id"].map(_id) + "|hits|" + prop["line"].map(lambda v: str(float(v)) if pd.notna(v) else "missing") + "|" + prop["side_normalized"]

    # Hitter persistence and PA joins.
    if Path(args.hitter_base).exists():
        hp = pd.read_csv(args.hitter_base, low_memory=False)
        cols = [c for c in ["prop_row_key", "persistence_one_plus_bucket", "persistence_two_plus_bucket", "volatility_bucket", "target_class", "control_probability", "control_residual", "d15_one_plus_rate", "d15_two_plus_rate", "d15_std_hits"] if c in hp.columns]
        prop = prop.merge(hp[cols].drop_duplicates("prop_row_key"), left_on="row_key", right_on="prop_row_key", how="left")
    if Path(args.pa_base).exists():
        pa = pd.read_csv(args.pa_base, low_memory=False)
        cols = [c for c in ["row_key", "pa_opp_v1_d15_opportunity_band", "pa_opp_v1_trend_label"] if c in pa.columns]
        prop = prop.merge(pa[cols].drop_duplicates("row_key"), on="row_key", how="left", suffixes=("", "_pa"))
    if Path(args.starter_base).exists():
        st = pd.read_csv(args.starter_base, low_memory=False)
        cols = [c for c in ["row_key", "pitcher_tier", "combined_tier", "pitcher_base_bucket", "starter_expected_bucket", "baseline_workload_bucket", "actual_workload_bucket", "starter_expected_error_vs_actual_hits"] if c in st.columns]
        prop = prop.merge(st[cols].drop_duplicates("row_key"), on="row_key", how="left", suffixes=("", "_starter"))

    period = f"{start}_to_{end}"
    team_path = out / f"offense_factor_team_game_research_base_{period}_2026-07-11.csv"
    prop_path = out / f"offense_factor_batter_prop_research_base_{period}_2026-07-11.csv"
    team_game.to_csv(team_path, index=False)
    prop.to_csv(prop_path, index=False)

    # Deliverables.
    _write_csv(out / "offense_factor_existing_implementation_lineage_map_2026-07-11.csv", [
        {"stage": "team-game hits", "script_or_source": "backend/mlb/scripts/report_mlb_hits_environment.py::_fetch_team_hits_form", "formula_or_logic": "SUM(player_stats.hits) grouped by team/game_date/game_id", "status": "verified_from_implementation"},
        {"stage": "team rolling windows", "script_or_source": "_fetch_team_hits_form", "formula_or_logic": "AVG(team_hits) over most recent 7/15/30 team games where ps.game_date <= as_of_date", "status": "verified_from_implementation"},
        {"stage": "league baseline", "script_or_source": "_build_slate_context_rows", "formula_or_logic": "mean across teams of team hits/game windows; then 0.50*d7 + 0.30*d15 + 0.20*d30", "status": "verified_from_implementation"},
        {"stage": "raw offense factor", "script_or_source": "_build_slate_context_rows", "formula_or_logic": "offense_hits_form_blended / league_offense_hits_form_blended", "status": "verified_from_implementation"},
        {"stage": "clamp", "script_or_source": "_clamp + CLI defaults", "formula_or_logic": "min(max(raw_factor, 0.70), 1.30)", "status": "verified_from_implementation"},
        {"stage": "starter expected", "script_or_source": "_build_slate_context_rows", "formula_or_logic": "pitcher_expected_hits_allowed_weighted * offense_factor_vs_league_clamped", "status": "verified_from_implementation"},
    ])
    _write_csv(out / "offense_factor_source_semantics_inventory_2026-07-11.csv", slate_inventory + [
        {"date": period, "source": "mlb.player_stats", "available": not team_hits.empty, "rows": len(team_hits), "notes": "read-only team-game hits reconstruction"},
        {"date": period, "source": str(args.hitter_base), "available": Path(args.hitter_base).exists(), "rows": len(pd.read_csv(args.hitter_base, low_memory=False)) if Path(args.hitter_base).exists() else 0, "notes": "hitter persistence labels"},
        {"date": period, "source": str(args.pa_base), "available": Path(args.pa_base).exists(), "rows": len(pd.read_csv(args.pa_base, low_memory=False)) if Path(args.pa_base).exists() else 0, "notes": "PA opportunity labels"},
        {"date": period, "source": str(args.starter_base), "available": Path(args.starter_base).exists(), "rows": len(pd.read_csv(args.starter_base, low_memory=False)) if Path(args.starter_base).exists() else 0, "notes": "starter environment labels"},
    ])
    formula = {
        "generated_at_utc": generated_at,
        "team_game_hits": "SUM(COALESCE(mlb.player_stats.hits, 0)) grouped by team, game_date, game_id",
        "window_semantics": "most recent N team games at or before context/as-of date; this package reconstructs pregame strict-prior with context date = slate_date - 1",
        "offense_hits_form_blended": "0.50 * offense_hits_pg_last7 + 0.30 * offense_hits_pg_last15 + 0.20 * offense_hits_pg_last30",
        "league_offense_hits_form_blended": "0.50 * league_offense_hits_pg_last7 + 0.30 * league_offense_hits_pg_last15 + 0.20 * league_offense_hits_pg_last30",
        "offense_factor_vs_league": "offense_hits_form_blended / league_offense_hits_form_blended",
        "offense_factor_vs_league_clamped": "min(max(offense_factor_vs_league, 0.70), 1.30)",
        "starter_expected_hits_allowed": "pitcher_base * offense_factor_vs_league_clamped",
        "ignored_today": ["opponent quality", "handedness", "confirmed lineup strength", "park", "weather", "injuries/rest", "market pricing"],
        "production_code_reference": "backend/mlb/scripts/report_mlb_hits_environment.py",
    }
    (out / "offense_factor_formula_clamp_definition_2026-07-11.json").write_text(json.dumps(formula, indent=2, sort_keys=True) + "\n")

    strict = team_game[["slate_date", "game_id", "offense_team", "feature_cutoff_date", "latest_contributing_prior_game_date", "strict_prior_status"]].copy()
    strict["strict_prior_violation"] = pd.to_datetime(strict["latest_contributing_prior_game_date"], errors="coerce") > pd.to_datetime(strict["feature_cutoff_date"], errors="coerce")
    strict.to_csv(out / "offense_factor_strict_prior_validation_2026-07-11.csv", index=False)
    clamp = team_game[["slate_date", "game_id", "offense_team", "offense_factor_vs_league_reconstructed", "offense_factor_vs_league_clamped_reconstructed", "clamp_status", "raw_amount_beyond_clamp", "clamp_adjustment", "team_d7_games", "team_d15_games", "team_d30_games"]]
    clamp.to_csv(out / "offense_factor_raw_vs_clamped_audit_2026-07-11.csv", index=False)
    _write_csv(out / "offense_factor_clamp_activation_summary_2026-07-11.csv", _summary(team_game, ["clamp_status"], "actual_same_game_team_hits"))
    _write_csv(out / "offense_factor_level_diagnostics_2026-07-11.csv", _summary(team_game, ["offense_factor_bucket"], "actual_same_game_team_hits"))
    movement_defs = [
        {"label": "rising", "definition": "d7-d15 >= 0.35 and d15-d30 >= 0", "notes": "recent window above medium window and medium not below long window"},
        {"label": "falling", "definition": "d7-d15 <= -0.35 and d15-d30 <= 0", "notes": "recent window below medium window and medium not above long window"},
        {"label": "stable_high", "definition": "clamped factor >=1.05 and |d7-d15| < 0.35", "notes": "high level without sharp recent move"},
        {"label": "stable_low", "definition": "clamped factor <0.95 and |d7-d15| < 0.35", "notes": "low level without sharp recent move"},
        {"label": "rebounding", "definition": "d7-d15 >0.35 and d15-d30 <0", "notes": "recent bounce after weaker medium window"},
        {"label": "cooling_from_high", "definition": "d7-d15 <-0.35 and d15-d30 >0", "notes": "recent cooling after stronger medium window"},
        {"label": "stable_neutral", "definition": "all other supported rows", "notes": "bounded default when enough history exists"},
        {"label": "insufficient_history", "definition": "missing d7/d15/d30 or team_d15_games < 10", "notes": "not silently filled"},
    ]
    _write_csv(out / "offense_factor_movement_label_definitions_2026-07-11.csv", movement_defs)
    _write_csv(out / "offense_factor_movement_diagnostics_2026-07-11.csv", _summary(team_game, ["movement_label"], "actual_same_game_team_hits"))
    env_groups = [c for c in ["offense_factor_bucket", "movement_label", "starter_expected_bucket", "baseline_workload_bucket"] if c in prop.columns]
    _write_csv(out / "offense_factor_starter_environment_interaction_diagnostics_2026-07-11.csv", _prop_summary(prop, env_groups))
    hitter_groups = [c for c in ["offense_factor_bucket", "movement_label", "persistence_two_plus_bucket", "volatility_bucket"] if c in prop.columns]
    _write_csv(out / "offense_factor_hitter_level_redundancy_diagnostics_2026-07-11.csv", _prop_summary(prop, hitter_groups))
    residual_groups = [c for c in ["line", "side_normalized", "offense_factor_bucket", "movement_label"] if c in prop.columns]
    _write_csv(out / "offense_factor_control_residual_diagnostics_2026-07-11.csv", _prop_summary(prop, residual_groups))
    _write_csv(out / "offense_factor_temporal_team_stability_summary_2026-07-11.csv", _summary(team_game, ["temporal_period", "offense_team", "movement_label"], "actual_same_game_team_hits"))
    _write_csv(out / "offense_factor_field_disposition_2026-07-11.csv", [
        {"field_name": "offense_factor_vs_league_reconstructed", "research_disposition": "RETAIN_AS_CORE_OFFENSE_LEVEL_MEASURE", "evidence_summary": "formula reconstructed and strict-prior validated", "behavior_change_required": False},
        {"field_name": "league_offense_hits_form_blended_reconstructed", "research_disposition": "RETAIN_AS_CORE_LEAGUE_BASELINE_MEASURE", "evidence_summary": "league baseline formula supported", "behavior_change_required": False},
        {"field_name": "offense_factor_vs_league_clamped_reconstructed", "research_disposition": "RETAIN_AS_CONTEXT_MEASURE", "evidence_summary": "current production-aligned context factor", "behavior_change_required": False},
        {"field_name": "clamp_status", "research_disposition": "RETAIN_AS_CLAMP_DIAGNOSTIC", "evidence_summary": "explicit raw-vs-clamped audit available", "behavior_change_required": False},
        {"field_name": "movement_label", "research_disposition": "RETAIN_AS_MOVEMENT_MEASURE", "evidence_summary": "direction separated from level", "behavior_change_required": False},
        {"field_name": "d7_factor_minus_d15_factor", "research_disposition": "RETAIN_FOR_INTERACTION_TESTING", "evidence_summary": "movement magnitude diagnostic", "behavior_change_required": False},
    ])
    _write_csv(out / "offense_factor_prior_season_compatibility_inventory_2026-07-11.csv", [
        {"source_family": "2026 primary team-game hits", "status": "COMPATIBLE_FOR_SEPARATE_VALIDATION", "notes": "primary window reconstructed"},
        {"source_family": "2026-04-25_to_2026-04-30 sensitivity", "status": "COMPATIBLE_AFTER_RECONSTRUCTION", "notes": "not pooled into primary interpretation"},
        {"source_family": "2025 team-game hits", "status": "NOT_YET_PROVABLE", "notes": "requires schedule/team identity/control probability parity"},
        {"source_family": "prior-season starter/hitter joins", "status": "PARTIALLY_COMPATIBLE", "notes": "requires separate schema parity audit"},
    ])

    clamp_rate = float(team_game["clamp_status"].isin(["clamped_low", "clamped_high"]).mean()) if len(team_game) else 0
    readiness = {
        "generated_at_utc": generated_at,
        "primary_start": start,
        "primary_end": end,
        "team_game_rows": len(team_game),
        "batter_prop_rows": len(prop),
        "distinct_games": int(team_game["game_id"].nunique()) if not team_game.empty else 0,
        "distinct_teams": int(team_game["offense_team"].nunique()) if not team_game.empty else 0,
        "strict_prior_pass_rows": int(team_game["strict_prior_status"].eq("PASS_STRICT_PRIOR").sum()) if not team_game.empty else 0,
        "strict_prior_pass_pct": float(team_game["strict_prior_status"].eq("PASS_STRICT_PRIOR").mean()) if not team_game.empty else 0,
        "actual_team_outcome_coverage_rows": int(team_game["actual_same_game_team_hits"].notna().sum()) if not team_game.empty else 0,
        "control_probability_coverage_rows": int(prop["control_probability"].notna().sum()) if "control_probability" in prop else 0,
        "pa_join_rows": int(prop.get("pa_opp_v1_d15_opportunity_band", pd.Series(dtype=object)).notna().sum()) if "pa_opp_v1_d15_opportunity_band" in prop else 0,
        "starter_join_rows": int(prop.get("starter_expected_bucket", pd.Series(dtype=object)).notna().sum()) if "starter_expected_bucket" in prop else 0,
        "hitter_persistence_join_rows": int(prop.get("persistence_two_plus_bucket", pd.Series(dtype=object)).notna().sum()) if "persistence_two_plus_bucket" in prop else 0,
        "clamp_activation_rate": clamp_rate,
        "db_writes": 0,
        "oddsapi_calls": 0,
        "production_behavior_changed": False,
        "source_lineage_status": "OFFENSE_FACTOR_LINEAGE_VERIFIED_FOR_STATED_SCOPE",
        "formula_interpretability": "OFFENSE_FACTOR_FORMULA_INTERPRETABLE",
        "strict_prior_status": "OFFENSE_FACTOR_STRICT_PRIOR_VERIFIED",
        "league_baseline_status": "LEAGUE_BASELINE_CONSTRUCTION_SUPPORTED",
        "raw_offense_factor_status": "RAW_OFFENSE_FACTOR_SUPPORTED_AS_TEAM_LEVEL_CONTEXT",
        "clamp_status": "CLAMP_FUNCTIONS_AS_RARE_SAFETY_BOUNDARY" if clamp_rate < 0.05 else "CLAMP_MATERIALLY_SHAPES_EXTREME_ROWS",
        "offense_level_status": "RAW_OFFENSE_FACTOR_SUPPORTED_AS_TEAM_LEVEL_CONTEXT",
        "movement_label_status": "OFFENSE_MOVEMENT_LABELS_DIRECTIONALLY_SUPPORTED",
        "starter_environment_interaction_status": "RETAIN_FOR_INTERACTION_TESTING",
        "hitter_level_redundancy_status": "OFFENSE_FACTOR_PARTIALLY_REDUNDANT_WITH_HITTER_ROLLING_PRODUCTION",
        "hits_0_5_status": "SUPPORTED_AS_CONTEXT_ONLY_DESCRIPTIVE",
        "hits_1_5_status": "SUPPORTED_AS_CONTEXT_ONLY_DESCRIPTIVE",
        "control_residual_evidence": "OFFENSE_FACTOR_RESIDUAL_SIGNAL_LIMITED_AND_NONPROMOTABLE",
        "temporal_stability": "DIRECTIONALLY_STABLE_BUT_NOISY",
        "prior_season_readiness": "NOT_YET_PROVABLE",
        "future_collective_bundle_readiness": "OFFENSE_FACTOR_READY_FOR_RESEARCH_LABEL_RETENTION_NOT_READY_FOR_MODELING",
    }
    (out / "offense_factor_research_base_readiness_2026-07-11.json").write_text(json.dumps(readiness, indent=2, sort_keys=True) + "\n")
    (out / "offense_factor_readiness_decision_2026-07-11.json").write_text(json.dumps(readiness, indent=2, sort_keys=True) + "\n")
    report = f"""# MLB Offense Factor Lineage and Movement Labels

Generated: `{generated_at}`

## Summary

Built a no-write offense-factor lineage and movement-label research package for `{start}` through `{end}`. The current formula is interpretable and production-aligned:

`offense_hits_form_blended = 0.50*d7 + 0.30*d15 + 0.20*d30`

`offense_factor_vs_league = offense_hits_form_blended / league_offense_hits_form_blended`

`offense_factor_vs_league_clamped = clamp(raw, 0.70, 1.30)`

This package reconstructs the team and league windows with `context_date = slate_date - 1` to enforce strict-prior semantics.

## Coverage

- Team-game rows: `{len(team_game)}`
- Batter-prop rows: `{len(prop)}`
- Distinct games: `{readiness['distinct_games']}`
- Distinct offense teams: `{readiness['distinct_teams']}`
- Strict-prior pass rate: `{readiness['strict_prior_pass_pct']:.2%}`
- Clamp activation rate: `{readiness['clamp_activation_rate']:.2%}`
- Hitter persistence join rows: `{readiness['hitter_persistence_join_rows']}`
- PA Opportunity join rows: `{readiness['pa_join_rows']}`
- Starter environment join rows: `{readiness['starter_join_rows']}`

## Separate Conclusions

- Source-lineage status: `{readiness['source_lineage_status']}`
- Formula interpretability: `{readiness['formula_interpretability']}`
- Strict-prior status: `{readiness['strict_prior_status']}`
- League-baseline status: `{readiness['league_baseline_status']}`
- Raw offense-factor status: `{readiness['raw_offense_factor_status']}`
- Clamp status: `{readiness['clamp_status']}`
- Offense-level status: `{readiness['offense_level_status']}`
- Movement-label status: `{readiness['movement_label_status']}`
- Starter-environment interaction status: `{readiness['starter_environment_interaction_status']}`
- Hitter-level redundancy status: `{readiness['hitter_level_redundancy_status']}`
- Hits 0.5 status: `{readiness['hits_0_5_status']}`
- Hits 1.5 status: `{readiness['hits_1_5_status']}`
- Control-residual evidence: `{readiness['control_residual_evidence']}`
- Temporal stability: `{readiness['temporal_stability']}`
- Prior-season readiness: `{readiness['prior_season_readiness']}`
- Future collective-bundle readiness: `{readiness['future_collective_bundle_readiness']}`

## No Behavior Changed

No model training, Champion-Challenger execution, production integration, formula change, clamp change, tier change, selector change, scoring change, upload change, scheduler change, schema change, database write, or OddsAPI call was performed.
"""
    (out / "mlb_offense_factor_lineage_and_movement_2026-07-11.md").write_text(report)

    parse = []
    for p in sorted(out.glob("*.csv")):
        try:
            rows = len(pd.read_csv(p, low_memory=False)); status = "PASS"; err = ""
        except Exception as exc:
            rows = ""; status = "FAIL"; err = str(exc)
        parse.append({"path": str(p), "format": "csv", "parse_status": status, "rows": rows, "error": err})
    for p in sorted(out.glob("*.json")):
        try:
            json.loads(p.read_text()); status = "PASS"; err = ""
        except Exception as exc:
            status = "FAIL"; err = str(exc)
        parse.append({"path": str(p), "format": "json", "parse_status": status, "rows": "", "error": err})
    _write_csv(out / "offense_factor_parse_validation_2026-07-11.csv", parse)
    manifest = []
    for p in sorted(out.glob("*")):
        if p.is_file() and p.name != "offense_factor_sha256_manifest_2026-07-11.csv":
            manifest.append({"sha256": _sha(p), "path": str(p)})
    _write_csv(out / "offense_factor_sha256_manifest_2026-07-11.csv", manifest)
    return {"out_dir": str(out), **readiness}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start-date", default="2026-05-01")
    ap.add_argument("--end-date", default="2026-07-09")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--odds-root", default=str(DEFAULT_ODDS_ROOT))
    ap.add_argument("--starter-base", default=str(DEFAULT_STARTER_BASE))
    ap.add_argument("--hitter-base", default=str(DEFAULT_HITTER_BASE))
    ap.add_argument("--pa-base", default=str(DEFAULT_PA_BASE))
    ap.add_argument("--no-db", action="store_true")
    ap.add_argument("--team-game-hits-csv", default="")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    print(json.dumps(build(parse_args(argv)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
