#!/usr/bin/env python3
"""
python backend/nhl/scripts/sog_integrity_report.py
python backend/nhl/scripts/sog_integrity_report.py --slate-date 2025-12-23 --feature-key shots_on_goal_denali
python backend/nhl/scripts/sog_integrity_report.py --slate-date 2025-12-23 --feature-key shots_on_goal_denali --db-rolling-check

Adds optional TOI integrity checks against skater logs.

Usage:
  export SLATE_DATE=2025-12-09
  export SUPABASE_DB_URL='postgresql://...'
  python backend/nhl/scripts/sog_integrity_report.py --slate-date 2025-12-23

If SUPABASE_DB_URL (or DATABASE_URL) is not set, DB checks are skipped.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any
from datetime import date, datetime
from datetime import date, timedelta
import pandas as pd


# ---------- helpers ----------
def die(msg: str, code: int = 2) -> None:
    print(f"[sog_integrity] FATAL: {msg}")
    raise SystemExit(code)


def pct(a: float) -> str:
    try:
        return f"{a*100:.1f}%"
    except Exception:
        return "n/a"


def read_csv_if_exists(p: Path) -> pd.DataFrame | None:
    if not p.exists():
        return None
    try:
        return pd.read_csv(p)
    except Exception as e:
        print(f"[sog_integrity] ⚠️  failed to read CSV {p}: {e}")
        return None


def read_json_if_exists(p: Path) -> Any | None:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        print(f"[sog_integrity] ⚠️  failed to read JSON {p}: {e}")
        return None


def file_stat_line(p: Path) -> str:
    if not p.exists():
        return f"❌ missing: {p}"
    try:
        sz = p.stat().st_size
        return f"✅ {p}  ({sz/1024/1024:.2f} MB)"
    except Exception:
        return f"✅ {p}"


def print_df_summary(
    title: str,
    df: pd.DataFrame,
    key_cols: list[str] | None = None,
    focus_cols: list[str] | None = None,
) -> None:
    print(f"\n=== {title} ===")
    print(f"rows={len(df):,}  cols={len(df.columns):,}")

    if key_cols:
        present = [c for c in key_cols if c in df.columns]
        if len(present) == len(key_cols):
            uniq = df.drop_duplicates(subset=key_cols)
            print(f"unique keys {tuple(key_cols)} = {len(uniq):,}")
        else:
            print(f"keys missing in df: {[c for c in key_cols if c not in df.columns]}")

    if focus_cols:
        cols = [c for c in focus_cols if c in df.columns]
        if not cols:
            print("focus cols: (none present)")
            return

        # numeric view only
        num = df[cols].apply(pd.to_numeric, errors="coerce")
        for c in cols:
            s = num[c]
            nonnull = s.notna().sum()
            nulls = s.isna().sum()
            nunique = s.dropna().nunique()
            if nonnull > 0:
                print(
                    f"  {c:24s} nonnull={nonnull:6d} null={nulls:6d} null%={pct(nulls/len(df))} "
                    f"distinct={nunique:6d}  min={s.min():.4g} max={s.max():.4g} mean={s.mean():.4g}"
                )
            else:
                print(
                    f"  {c:24s} nonnull={nonnull:6d} null={nulls:6d} null%={pct(nulls/len(df))} distinct={nunique:6d}"
                )


def psql_available() -> bool:
    return shutil.which("psql") is not None


def run_psql(db_url: str, sql: str) -> str | None:
    if not psql_available():
        print("[sog_integrity] ⚠️  psql not found on PATH; skipping DB checks.")
        return None
    try:
        cmd = ["psql", "--no-psqlrc", "-v", "ON_ERROR_STOP=1", db_url, "-At", "-c", sql]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return out.strip()
    except subprocess.CalledProcessError as e:
        print("[sog_integrity] ⚠️  DB check failed:")
        print(e.output)
        return None


def _coerce_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def db_toi_integrity_check(
    db_url: str,
    slate: str,
    days_back: int = 30,
    source: str = "nhl.skater_game_logs_raw",
) -> None:
    """
    TOI integrity checks (skaters):
      - slate-day null/zero/outlier TOI
      - slate-day pp_toi > toi violations
      - last N days rollup (regression detector)

    `source` should have columns:
      player_id, game_id, toi_minutes, pp_toi_minutes (pp optional but preferred)
    """
    print("\n=== DB TOI integrity ===")
    print(f"[sog_integrity] toi_source={source}  days_back={days_back}")

    try:
        toi_date = (date.fromisoformat(slate) - timedelta(days=1)).isoformat()
    except Exception:
        toi_date = None

    # ---- YDAY checks (TOI source should exist for completed games) ----
    check_date = toi_date  # derived above

    slate_sql = f"""
    WITH g AS (
    SELECT game_id
    FROM nhl.games
    WHERE game_date = DATE '{check_date}'
    ),
    s AS (
    SELECT
        player_id,
        game_id,
        toi_minutes,
        pp_toi_minutes
    FROM {source}
    WHERE game_id IN (SELECT game_id FROM g)
    )
    SELECT
    COUNT(*)::int                                                      AS rows,
    COUNT(DISTINCT player_id)::int                                     AS players,
    SUM(CASE WHEN toi_minutes IS NULL THEN 1 ELSE 0 END)::int          AS toi_null,
    SUM(CASE WHEN toi_minutes = 0 THEN 1 ELSE 0 END)::int              AS toi_zero,
    SUM(CASE WHEN toi_minutes < 1 THEN 1 ELSE 0 END)::int              AS toi_lt1,
    SUM(CASE WHEN toi_minutes > 35 THEN 1 ELSE 0 END)::int             AS toi_gt35,
    SUM(CASE WHEN toi_minutes < 0 THEN 1 ELSE 0 END)::int              AS toi_lt0,
    SUM(CASE WHEN pp_toi_minutes IS NULL THEN 1 ELSE 0 END)::int       AS pp_toi_null,
    SUM(CASE WHEN pp_toi_minutes > toi_minutes THEN 1 ELSE 0 END)::int AS pp_gt_toi,
    ROUND(MIN(toi_minutes)::numeric, 2)                                AS toi_min,
    ROUND(MAX(toi_minutes)::numeric, 2)                                AS toi_max,
    ROUND(AVG(toi_minutes)::numeric, 2)                                AS toi_avg
    FROM s;
    """
    out = run_psql(db_url, slate_sql)
    if out:
        print(f"[sog_integrity] yday_toi({check_date})| " + out)

        # Cheap regression detector for "nothing came back"
        if out.startswith("0|0|"):
            print(f"[sog_integrity] ⚠️ yday has 0 rows in TOI source for {check_date}; seed/ingest may be missing.")

        # ---- Last N days checks ----
        window_sql = f"""
    WITH w AS (
    SELECT game_id
    FROM nhl.games
    WHERE game_date >= (CURRENT_DATE - INTERVAL '{days_back} days')
    ),
    s AS (
    SELECT
        player_id,
        game_id,
        toi_minutes,
        pp_toi_minutes
    FROM {source}
    WHERE game_id IN (SELECT game_id FROM w)
    )
    SELECT
    COUNT(*)::int                                                      AS rows,
    COUNT(DISTINCT player_id)::int                                     AS players,
    SUM(CASE WHEN toi_minutes IS NULL THEN 1 ELSE 0 END)::int          AS toi_null,
    SUM(CASE WHEN toi_minutes = 0 THEN 1 ELSE 0 END)::int              AS toi_zero,
    SUM(CASE WHEN toi_minutes > 35 THEN 1 ELSE 0 END)::int             AS toi_gt35,
    SUM(CASE WHEN toi_minutes < 0 THEN 1 ELSE 0 END)::int              AS toi_lt0,
    SUM(CASE WHEN pp_toi_minutes IS NULL THEN 1 ELSE 0 END)::int       AS pp_toi_null,
    SUM(CASE WHEN pp_toi_minutes > toi_minutes THEN 1 ELSE 0 END)::int AS pp_gt_toi,
    ROUND(MIN(toi_minutes)::numeric, 2)                                AS toi_min,
    ROUND(MAX(toi_minutes)::numeric, 2)                                AS toi_max,
    ROUND(AVG(toi_minutes)::numeric, 2)                                AS toi_avg,
    ROUND(STDDEV_SAMP(toi_minutes)::numeric, 4)                        AS toi_sd
    FROM s;
    """
        out2 = run_psql(db_url, window_sql)
        if out2:
            print("[sog_integrity] window_toi| " + out2)

def _as_date(s: str) -> date:
    # Accept "YYYY-MM-DD" (what your pipeline uses)
    return datetime.fromisoformat(s).date()


def report_slate_toi_feature_coverage(conn, slate_date: date):
    """
    Diagnoses TOI feature fill quality on the SOG pregame table for a given slate day.

    Buckets missing d10_toi_min_avg into:
    - no_history_before_slate: player has 0 prior games in skater_game_logs_raw for same season
    - has_history_missing_any: player has prior games, but still no d10_toi_min_avg (unexpected)
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH p AS (
            SELECT %s::date AS slate_date
            ),
            slate AS (
            SELECT
                t.player_id::bigint AS player_id,
                t.season::int       AS season,
                t.d10_toi_min_avg   AS d10_toi_min_avg
            FROM nhl.training_features_nhl_sog_enriched_pregame_v2 t
            JOIN p ON TRUE
            WHERE t.game_date = p.slate_date
            ),
            hist AS (
            SELECT
                l.player_id::bigint AS player_id,
                g.season::int       AS season,
                COUNT(*)::int       AS prior_games
            FROM nhl.skater_game_logs_raw l
            JOIN nhl.games g USING (game_id)
            JOIN p ON TRUE
            WHERE g.game_date < p.slate_date
            GROUP BY 1,2
            ),
            joined AS (
            SELECT
                s.player_id,
                s.season,
                s.d10_toi_min_avg,
                COALESCE(h.prior_games, 0) AS prior_games
            FROM slate s
            LEFT JOIN hist h
                ON h.player_id = s.player_id
            AND h.season    = s.season
            )
            SELECT
            COUNT(*)::int                                              AS rows_slate,
            COUNT(*) FILTER (WHERE d10_toi_min_avg IS NOT NULL)::int    AS nn_d10_toi,
            COUNT(*) FILTER (WHERE d10_toi_min_avg IS NULL)::int        AS missing_d10_toi,
            COUNT(*) FILTER (
                WHERE d10_toi_min_avg IS NULL AND prior_games = 0
            )::int                                                     AS missing_no_history_before_slate,
            COUNT(*) FILTER (
                WHERE d10_toi_min_avg IS NULL AND prior_games > 0
            )::int                                                     AS missing_has_history_unexpected
            FROM joined;
            """,
            (slate_date.isoformat(),),
        )
        row = cur.fetchone() or {}

    print("\n=== SOG TOI Feature Coverage (slate-day) ===")
    print(
        f"slate_date={slate_date} | rows_slate={row.get('rows_slate', 0)} | "
        f"nn_d10_toi={row.get('nn_d10_toi', 0)} | missing_d10_toi={row.get('missing_d10_toi', 0)}"
    )
    print(
        f"missing buckets: no_history_before_slate={row.get('missing_no_history_before_slate', 0)} | "
        f"has_history_unexpected={row.get('missing_has_history_unexpected', 0)}"
    )

# ---------- new: feature metadata / schema checks ----------
def expected_features_from_metadata(meta: Any, key: str | None = None) -> list[str]:
    """
    Supports a few shapes:
      - {"shots_on_goal": ["f1","f2",...], ...}
      - {"features_by_model": {"shots_on_goal": [...]}}  (if you ever wrap)
      - {"shots_on_goal": {"features":[...]}}            (nested)
    If key is None and only one list-like value exists, uses that.
    """
    if meta is None:
        return []

    if isinstance(meta, dict):
        # direct key
        if key and key in meta:
            v = meta[key]
            if isinstance(v, list):
                return [str(x) for x in v]
            if isinstance(v, dict) and isinstance(v.get("features"), list):
                return [str(x) for x in v["features"]]

        # common wrapper
        fbm = meta.get("features_by_model")
        if isinstance(fbm, dict):
            if key and key in fbm and isinstance(fbm[key], list):
                return [str(x) for x in fbm[key]]
            if key and key in fbm and isinstance(fbm[key], dict) and isinstance(fbm[key].get("features"), list):
                return [str(x) for x in fbm[key]["features"]]

        # fallback: single list-like entry
        list_candidates: list[list[str]] = []
        for _, v in meta.items():
            if isinstance(v, list):
                list_candidates.append([str(x) for x in v])
            elif isinstance(v, dict) and isinstance(v.get("features"), list):
                list_candidates.append([str(x) for x in v["features"]])
        if len(list_candidates) == 1:
            return list_candidates[0]

    return []


def schema_mismatch_report(expected: list[str], actual_cols: list[str], ignore: set[str]) -> tuple[list[str], list[str]]:
    exp = [c for c in expected if c not in ignore]
    act = [c for c in actual_cols if c not in ignore]
    missing = sorted([c for c in exp if c not in act])
    extra = sorted([c for c in act if c not in exp])
    return missing, extra


# ---------- new: rolling / toi / join checks ----------
def low_variance_flags(
    df: pd.DataFrame,
    key_cols: list[str],
    feature_cols: list[str],
    distinct_threshold: int,
    std_threshold: float,
    min_rows_for_flag: int = 25,
) -> dict[str, Any]:
    """
    Slate-local “stuck” detector. Not perfect, but catches regressions where
    a feature collapses to a few repeated values across the slate.
    """
    out: dict[str, Any] = {"features_checked": [], "flags": {}}
    if df is None or len(df) == 0:
        return out

    for c in feature_cols:
        if c not in df.columns:
            continue
        s = _coerce_num(df[c])
        s2 = s.dropna()
        out["features_checked"].append(c)

        if len(s2) < min_rows_for_flag:
            continue

        nun = int(s2.nunique())
        sd = float(s2.std()) if len(s2) > 1 else 0.0

        if nun <= distinct_threshold or sd <= std_threshold:
            out["flags"][c] = {
                "n": int(len(s2)),
                "distinct": nun,
                "std": sd,
                "min": float(s2.min()),
                "max": float(s2.max()),
            }

    # optional: include a small sample of keys for a flagged feature
    if out["flags"]:
        sample_keys = []
        present_keys = [k for k in key_cols if k in df.columns]
        if present_keys:
            sample_keys = (
                df[present_keys]
                .drop_duplicates()
                .head(10)
                .to_dict(orient="records")
            )
        out["sample_keys"] = sample_keys

    return out


def identical_d5_d10_d20_flags(df: pd.DataFrame) -> dict[str, Any]:
    cols = ["d5_sog_per60", "d10_sog_per60", "d20_sog_per60"]
    if not all(c in df.columns for c in cols):
        return {"checked": False}

    a = _coerce_num(df[cols[0]])
    b = _coerce_num(df[cols[1]])
    c = _coerce_num(df[cols[2]])

    m = a.notna() & b.notna() & c.notna()
    if m.sum() == 0:
        return {"checked": True, "rows_compared": 0}

    eq = (a[m] == b[m]) & (b[m] == c[m])
    return {
        "checked": True,
        "rows_compared": int(m.sum()),
        "rows_all_equal": int(eq.sum()),
        "pct_all_equal": float(eq.mean()),
    }


def toi_integrity(df: pd.DataFrame) -> dict[str, Any]:
    if "toi_min" not in df.columns:
        return {"checked": False}

    s = _coerce_num(df["toi_min"])
    n = len(df)
    nulls = int(s.isna().sum())
    le0 = int((s <= 0).sum(skipna=True))
    gt40 = int((s > 40).sum(skipna=True))
    lt1 = int((s < 1).sum(skipna=True))

    desc = s.dropna().describe(percentiles=[0.05, 0.5, 0.95]).to_dict() if s.notna().any() else {}

    return {
        "checked": True,
        "rows": n,
        "nulls": nulls,
        "null_pct": float(nulls / n) if n else 0.0,
        "toi_le_0": le0,
        "toi_lt_1": lt1,
        "toi_gt_40": gt40,
        "describe": {
            "min": float(desc.get("min", 0.0)) if desc else None,
            "p05": float(desc.get("5%", 0.0)) if desc else None,
            "p50": float(desc.get("50%", 0.0)) if desc else None,
            "p95": float(desc.get("95%", 0.0)) if desc else None,
            "max": float(desc.get("max", 0.0)) if desc else None,
            "mean": float(desc.get("mean", 0.0)) if desc else None,
        },
    }


def join_explosion_guard(df: pd.DataFrame, key_cols: list[str]) -> dict[str, Any]:
    present = [c for c in key_cols if c in df.columns]
    if len(present) != len(key_cols):
        return {"checked": False, "missing_key_cols": [c for c in key_cols if c not in df.columns]}

    total = len(df)
    uniq = int(df.drop_duplicates(subset=key_cols).shape[0])
    dup_rows = total - uniq

    # show a tiny sample of offending keys if any
    sample = []
    if dup_rows > 0:
        g = df.groupby(key_cols, dropna=False).size().reset_index(name="c")
        offenders = g[g["c"] > 1].sort_values("c", ascending=False).head(10)
        sample = offenders.to_dict(orient="records")

    return {
        "checked": True,
        "total_rows": int(total),
        "unique_keys": int(uniq),
        "dup_row_count": int(dup_rows),
        "dup_key_groups": int(len(sample)) if dup_rows > 0 else 0,
        "sample_offenders": sample,
    }
def db_pp_role_coverage_check(db_url: str, slate: str) -> None:
    """
    Integrity-only: report PP role coverage for the slate in the pregame SOG table.
    Uses the already-filled column: role_pp_share
    """
    print("\n=== DB PP role coverage (slate-day) ===")

    sql = f"""
WITH p AS (
  SELECT
    player_id, game_id, team_id, game_date,
    role_pp_share
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2
  WHERE game_date = DATE '{slate}'
),
pl AS (
  -- latest prior log per player (for reason-bucketing)
  SELECT DISTINCT ON (l.player_id)
    l.player_id,
    g.game_date,
    NULLIF(l.pp_toi_minutes, 0)::numeric AS pp_toi_min
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  WHERE g.game_date < DATE '{slate}'
  ORDER BY l.player_id, g.game_date DESC
),
team_pp AS (
  -- team PP total for the same prior date chosen per player (keeps denominator consistent)
  SELECT
    pl.player_id,
    SUM(COALESCE(l2.pp_toi_minutes,0))::numeric AS team_pp_toi_min
  FROM pl
  JOIN nhl.skater_game_logs_raw l2
    ON l2.game_id IN (SELECT game_id FROM nhl.games WHERE game_date = pl.game_date)
  GROUP BY pl.player_id
),
diag AS (
  SELECT
    p.*,
    pl.player_id AS has_player_log,
    team_pp.team_pp_toi_min
  FROM p
  LEFT JOIN pl      ON pl.player_id = p.player_id
  LEFT JOIN team_pp ON team_pp.player_id = p.player_id
)
SELECT
  COUNT(*)::int                                                AS rows_total,
  COUNT(*) FILTER (WHERE role_pp_share IS NOT NULL)::int        AS rows_with_role_pp_share,
  COUNT(*) FILTER (WHERE role_pp_share IS NULL)::int            AS rows_missing_role_pp_share,
  ROUND(MIN(role_pp_share)::numeric, 6)                         AS min_share,
  ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY role_pp_share)::numeric, 6) AS p50_share,
  ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY role_pp_share)::numeric, 6) AS p90_share,
  ROUND(MAX(role_pp_share)::numeric, 6)                         AS max_share
FROM diag;
"""
    out = run_psql(db_url, sql)
    if out:
        print(f"[sog_integrity] pp_role_coverage({slate})| " + out)

    sql2 = f"""
WITH p AS (
  SELECT player_id, game_id, team_id, game_date, role_pp_share
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2
  WHERE game_date = DATE '{slate}'
),
pl AS (
  SELECT DISTINCT ON (l.player_id)
    l.player_id,
    g.game_date
  FROM nhl.skater_game_logs_raw l
  JOIN nhl.games g USING (game_id)
  WHERE g.game_date < DATE '{slate}'
  ORDER BY l.player_id, g.game_date DESC
),
team_pp AS (
  SELECT
    pl.player_id,
    SUM(COALESCE(l2.pp_toi_minutes,0))::numeric AS team_pp_toi_min
  FROM pl
  JOIN nhl.skater_game_logs_raw l2
    ON l2.game_id IN (SELECT game_id FROM nhl.games WHERE game_date = pl.game_date)
  GROUP BY pl.player_id
),
diag AS (
  SELECT
    p.player_id,
    p.role_pp_share,
    pl.player_id AS has_player_log,
    COALESCE(team_pp.team_pp_toi_min, 0) AS team_pp_toi_min
  FROM p
  LEFT JOIN pl      ON pl.player_id = p.player_id
  LEFT JOIN team_pp ON team_pp.player_id = p.player_id
)
SELECT
  COUNT(*) FILTER (WHERE role_pp_share IS NULL)::int                                           AS n_null,
  COUNT(*) FILTER (WHERE role_pp_share IS NULL AND has_player_log IS NULL)::int                AS n_missing_player_log,
  COUNT(*) FILTER (WHERE role_pp_share IS NULL AND has_player_log IS NOT NULL AND team_pp_toi_min = 0)::int AS n_team_total_zero
FROM diag;
"""
    out2 = run_psql(db_url, sql2)
    if out2:
        print(f"[sog_integrity] pp_role_missing_reasons({slate})| " + out2)


# ---------- existing DB checks (plus optional rolling DB check) ----------
def db_checks(db_url: str, slate: str) -> None:
    print("\n=== DB checks (nhl.*) ===")

    counts_sql = f"""
WITH g AS (SELECT game_id FROM nhl.games WHERE game_date = DATE '{slate}')
SELECT 'games' || '|' || COUNT(*) FROM nhl.games WHERE game_date = DATE '{slate}'
UNION ALL SELECT 'roster_rows' || '|' || COUNT(*) FROM nhl.roster_status r WHERE r.game_id IN (SELECT game_id FROM g)
UNION ALL SELECT 'preds_sog' || '|' || COUNT(*) FROM nhl.predictions p WHERE p.game_id IN (SELECT game_id FROM g) AND p.prop = 'shots_on_goal'
UNION ALL SELECT 'preds_total' || '|' || COUNT(*) FROM nhl.predictions p WHERE p.game_id IN (SELECT game_id FROM g);
"""
    out = run_psql(db_url, counts_sql)
    if out:
        for line in out.splitlines():
            k, v = (line.split("|", 1) + [""])[:2]
            print(f"  {k:12s}: {v}")

    dup_sql = f"""
WITH g AS (SELECT game_id FROM nhl.games WHERE game_date = DATE '{slate}')
SELECT
  COUNT(*)::int
FROM (
  SELECT player_id, game_id, prop, line, COUNT(*) AS c
  FROM nhl.predictions
  WHERE game_id IN (SELECT game_id FROM g) AND prop = 'shots_on_goal'
  GROUP BY 1,2,3,4
  HAVING COUNT(*) > 1
) d;
"""
    out2 = run_psql(db_url, dup_sql)
    if out2 is not None:
        print(f"  dup_groups (player_id,game_id,prop,line): {out2}")

    players_sql = f"""
WITH g AS (SELECT game_id FROM nhl.games WHERE game_date = DATE '{slate}')
SELECT
  COUNT(DISTINCT player_id)::int
FROM nhl.predictions
WHERE game_id IN (SELECT game_id FROM g) AND prop = 'shots_on_goal';
"""
    out3 = run_psql(db_url, players_sql)
    if out3 is not None:
        print(f"  distinct_players_in_sog_preds: {out3}")


def db_rolling_check(db_url: str, view_name: str, days_back: int) -> None:
    """
    Optional DB rolling sanity: checks stddev over last N days for a few key fields
    IF they exist in the given view/table.

    Conservative: won't fail if columns don't exist.
    """
    print("\n=== DB rolling check ===")
    print(f"[sog_integrity] db_rolling_view={view_name}  days_back={days_back}")

    # Check columns present
    col_sql = f"""
SELECT column_name
FROM information_schema.columns
WHERE table_schema = split_part('{view_name}', '.', 1)
  AND table_name   = split_part('{view_name}', '.', 2);
"""
    out = run_psql(db_url, col_sql)
    if not out:
        print(f"[sog_integrity] ⚠️  could not introspect columns for {view_name}")
        return

    cols = set(x.strip() for x in out.splitlines() if x.strip())

    # NOTE: this is still SOG-feature oriented; TOI lives elsewhere (see notes below)
    want = [
        "d10_sog_per60",
        "attempts_d10_per60",
        "team_d10_sf_per_game",
        "opp_d10_sf_allowed_per_game",
        "toi_minutes",
    ]
    have = [c for c in want if c in cols]
    print(f"[sog_integrity] db_rolling_have_cols={have if have else 'none'}")

    if not have:
        print(f"[sog_integrity] ℹ️  none of {want} found in {view_name}; skipping.")
        return

    select_std = ",\n       ".join([f"STDDEV({c}) AS sd_{c}" for c in have])
    sql = f"""
WITH w AS (
  SELECT game_id
  FROM nhl.games
  WHERE game_date >= (CURRENT_DATE - INTERVAL '{days_back} days')
),
v AS (
  SELECT x.*
  FROM {view_name} x
  JOIN w USING (game_id)
)
SELECT
  COUNT(*)::int AS rows,
  COUNT(DISTINCT player_id)::int AS players,
  {select_std}
FROM v;
"""
    out2 = run_psql(db_url, sql)
    if not out2:
        return

    # pretty-print the single-line output
    parts = out2.split("|")
    if len(parts) >= 2:
        print(f"[sog_integrity] rows={parts[0]} players={parts[1]}")
        for i, c in enumerate(have):
            val = parts[2 + i] if 2 + i < len(parts) else ""
            print(f"[sog_integrity]   sd_{c}={val}")
    else:
        print(out2)


# ---------- main ----------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate-date", required=False, default=None, help="YYYY-MM-DD (ET)")
    ap.add_argument("--repo-root", default=".", help="repo root (default: .)")
    ap.add_argument("--db-url", default=None, help="override DB url (else SUPABASE_DB_URL or DATABASE_URL)")

    # NEW: schema check inputs
    ap.add_argument(
        "--feature-metadata",
        default="backend/nhl/features/feature_metadata_nhl.json",
        help="path to feature metadata json (default: backend/nhl/features/feature_metadata_nhl.json)",
    )
    ap.add_argument(
        "--feature-key",
        default="shots_on_goal",
        help="which key/model name inside feature metadata to compare (default: shots_on_goal)",
    )
    ap.add_argument(
        "--schema-ignore",
        default="player_id,game_id,team_id,opponent_id,is_home,game_date,season,shots_on_goal,full_name,team_code",
        help="comma-separated columns to ignore in schema mismatch (ids/targets/context)",
    )

    # NEW: variance thresholds
    ap.add_argument("--variance-distinct-threshold", type=int, default=5, help="flag feature if distinct <= this")
    ap.add_argument("--variance-std-threshold", type=float, default=1e-6, help="flag feature if stddev <= this")
    ap.add_argument("--variance-min-rows", type=int, default=25, help="min non-null rows to consider for variance flags")

    # NEW: optional DB rolling check
    ap.add_argument("--db-rolling-check", action="store_true", help="run a lightweight DB rolling check (optional)")
    ap.add_argument(
        "--db-rolling-view",
        default="nhl.skater_game_logs_raw",
        help="view/table to use for DB rolling check (default: nhl.skater_game_logs_raw)",
    )
    ap.add_argument("--db-rolling-days-back", type=int, default=30, help="days back for DB rolling check")

    ap.add_argument("--db-toi-check", action="store_true", help="run TOI integrity checks (optional)")
    ap.add_argument("--db-toi-source", default="nhl.skater_game_logs_raw", help="table/view to use for TOI checks")
    ap.add_argument("--db-toi-days-back", type=int, default=30, help="days back for TOI window rollup")

    args = ap.parse_args()

    slate = args.slate_date or os.environ.get("SLATE_DATE")
    if not slate:
        die("Provide --slate-date YYYY-MM-DD or set SLATE_DATE env (ET).")

    root = Path(args.repo_root).resolve()

    # expected artifacts (based on your pipeline logs)
    names_csv = root / "backend/nhl/exports/daily/names" / f"names_{slate}.csv"
    feats_csv = root / "backend/nhl/exports/daily/sog_features" / f"sog_features_{slate}_denali.csv"
    pred_csv = root / "backend/nhl/data/processed/sog_predictions.csv"
    pred_cal_csv = root / "backend/nhl/data/processed/sog_predictions_wide_calibrated.csv"
    curves_json = root / "backend/nhl/data/processed/sog_denali_calibration_curves.json"
    site_with_market = root / "nhl/site/data/sog_with_market.csv"  # optional

    feature_meta_path = (root / args.feature_metadata).resolve()
    ignore = set([c.strip() for c in str(args.schema_ignore).split(",") if c.strip()])

    print(f"[sog_integrity] slate={slate}")
    print("\n=== Artifacts ===")
    for p in [names_csv, feats_csv, pred_csv, pred_cal_csv, curves_json, site_with_market, feature_meta_path]:
        print(file_stat_line(p))

    # ---- Names ----
    names = read_csv_if_exists(names_csv)
    if names is not None:
        print_df_summary(
            "names CSV",
            names,
            key_cols=["player_id", "game_id"],
            focus_cols=[],
        )
        if "full_name" in names.columns:
            nn = names["full_name"].notna().sum()
            print(f"full_name non-null: {nn:,}/{len(names):,}")
        if "game_date" in names.columns:
            gd = names["game_date"].astype(str)
            on_slate = (gd == slate).sum()
            print(f"game_date == slate: {on_slate:,}/{len(names):,}")

    # ---- Features ----
    feats = read_csv_if_exists(feats_csv)
    if feats is not None:
        roll_cols = [
            "d5_sog_per60",
            "d10_sog_per60",
            "d20_sog_per60",
            "attempts_d10_per60",
            "pace_matchup_index",
            "pace_index",
            "team_d10_sf_per_game",
            "opp_d10_sf_allowed_per_game",
            "toi_min",  # may or may not exist
        ]
        print_df_summary(
            "sog features (denali) CSV",
            feats,
            key_cols=["player_id", "game_id"],
            focus_cols=roll_cols,
        )

        # NEW: join explosion guard
        jg = join_explosion_guard(feats, ["player_id", "game_id"])
        if jg.get("checked"):
            if jg["dup_row_count"] > 0:
                print(f"[sog_integrity] ⚠️  JOIN EXPLOSION? dup_rows={jg['dup_row_count']} unique_keys={jg['unique_keys']:,} total_rows={jg['total_rows']:,}")
                if jg.get("sample_offenders"):
                    print("[sog_integrity] sample duplicate keys (player_id,game_id,count):")
                    for r in jg["sample_offenders"]:
                        print(f"  {r}")
            else:
                print("[sog_integrity] ✅ join explosion guard: OK (1 row per player_id,game_id)")

        # NEW: TOI integrity (only if toi_min exists)
        ti = toi_integrity(feats)
        if ti.get("checked"):
            if ti["nulls"] > 0 or ti["toi_le_0"] > 0 or ti["toi_gt_40"] > 0:
                print("[sog_integrity] ⚠️  TOI integrity issues detected:")
                print(f"  nulls={ti['nulls']} ({pct(ti['null_pct'])})  toi<=0={ti['toi_le_0']}  toi<1={ti['toi_lt_1']}  toi>40={ti['toi_gt_40']}")
                print(f"  describe={ti['describe']}")
            else:
                print("[sog_integrity] ✅ TOI integrity: OK")

        # NEW: schema / column mismatch (metadata vs export cols)
        meta = read_json_if_exists(feature_meta_path)
        expected = expected_features_from_metadata(meta, args.feature_key)
        if expected:
            missing, extra = schema_mismatch_report(expected, list(feats.columns), ignore)
            print("\n=== schema mismatch (expected model features vs export columns) ===")
            print(f"feature_key={args.feature_key}  expected={len(expected)}  export_cols={len(feats.columns)}  ignore={len(ignore)}")
            if missing:
                print(f"[sog_integrity] ❌ missing_in_export ({len(missing)}): {missing[:50]}{' ...' if len(missing) > 50 else ''}")
            else:
                print("[sog_integrity] ✅ missing_in_export: none")
            if extra:
                # extra can be fine (context columns), but still useful to see
                print(f"[sog_integrity] ⚠️  extra_in_export ({len(extra)}): {extra[:50]}{' ...' if len(extra) > 50 else ''}")
            else:
                print("[sog_integrity] ✅ extra_in_export: none")
        else:
            print("\n[sog_integrity] ⚠️  feature metadata not found / unreadable / key missing; schema mismatch check skipped.")

        # NEW: rolling sanity checks (slate-local)
        # (1) low variance / collapse detector across slate
        var_report = low_variance_flags(
            feats,
            key_cols=["player_id", "game_id"],
            feature_cols=[
                "d10_sog_per60",
                "attempts_d10_per60",
                "team_d10_sf_per_game",
                "opp_d10_sf_allowed_per_game",
                "pace_matchup_index",
                "pace_index",
            ],
            distinct_threshold=args.variance_distinct_threshold,
            std_threshold=args.variance_std_threshold,
            min_rows_for_flag=args.variance_min_rows,
        )
        if var_report.get("flags"):
            print("\n=== rolling sanity (slate-local variance) ===")
            print("[sog_integrity] ⚠️  LOW VARIANCE FEATURES flagged:")
            for k, v in var_report["flags"].items():
                print(f"  {k}: n={v['n']} distinct={v['distinct']} std={v['std']:.4g} min={v['min']:.4g} max={v['max']:.4g}")
        else:
            print("\n[sog_integrity] ✅ rolling sanity (slate-local variance): OK (no collapse flags)")

        # (2) d5/d10/d20 identical detector
        eq = identical_d5_d10_d20_flags(feats)
        if eq.get("checked"):
            if eq["rows_compared"] > 0 and eq["pct_all_equal"] > 0.5:
                print("\n[sog_integrity] ⚠️  d5/d10/d20 frequently identical (possible bug):")
                print(f"  rows_compared={eq['rows_compared']} rows_all_equal={eq['rows_all_equal']} pct_all_equal={pct(eq['pct_all_equal'])}")
            else:
                print("\n[sog_integrity] ✅ d5/d10/d20 identical check: OK")

    # ---- Predictions (raw/merged) ----
    pred = read_csv_if_exists(pred_csv)
    if pred is not None:
        pcols = [c for c in pred.columns if c.startswith("p_over_")]
        if pcols:
            print_df_summary(
                "sog predictions (wide) CSV",
                pred,
                key_cols=[c for c in ["player_id", "game_id"] if c in pred.columns],
                focus_cols=pcols,
            )
            for c in pcols:
                s = pd.to_numeric(pred[c], errors="coerce").dropna()
                if len(s) > 0:
                    nun = s.nunique()
                    if nun <= 10:
                        print(f"[sog_integrity] ⚠️  COLLAPSE RISK: {c} distinct_probs={nun} (too bucketed?)")
        else:
            focus = [c for c in ["p_over", "line"] if c in pred.columns]
            print_df_summary(
                "sog predictions (long) CSV",
                pred,
                key_cols=[c for c in ["player_id", "game_id", "line"] if c in pred.columns],
                focus_cols=focus,
            )
            if "p_over" in pred.columns and "line" in pred.columns:
                tmp = pred.copy()
                tmp["p_over"] = pd.to_numeric(tmp["p_over"], errors="coerce")
                tmp["line"] = pd.to_numeric(tmp["line"], errors="coerce")
                print("\nper-line p_over distinct-count:")
                for ln, g in tmp.dropna(subset=["line"]).groupby("line"):
                    s = g["p_over"].dropna()
                    if len(s) == 0:
                        continue
                    print(f"  line={ln:>4.1f}  n={len(s):4d}  distinct={s.nunique():4d}  min={s.min():.4g} max={s.max():.4g} mean={s.mean():.4g}")

    # ---- Predictions (calibrated wide) ----
    pred_cal = read_csv_if_exists(pred_cal_csv)
    if pred_cal is not None:
        cal_cols = [c for c in pred_cal.columns if c.startswith("p_over_")]
        if cal_cols:
            print_df_summary(
                "sog predictions (wide calibrated) CSV",
                pred_cal,
                key_cols=[c for c in ["player_id", "game_id"] if c in pred_cal.columns],
                focus_cols=cal_cols,
            )
            for c in cal_cols:
                s = pd.to_numeric(pred_cal[c], errors="coerce").dropna()
                if len(s) > 0:
                    nun = s.nunique()
                    if nun <= 10:
                        print(f"[sog_integrity] ⚠️  CALIB COLLAPSE? {c} distinct_probs={nun}")

    # ---- Calibration curves file ----
    if curves_json.exists():
        curves = read_json_if_exists(curves_json)
        if isinstance(curves, dict):
            keys = sorted(curves.keys())
            print("\n=== calibration curves ===")
            print(f"keys: {keys[:12]}{' ...' if len(keys) > 12 else ''}")

    # ---- Optional site output check ----
    site = read_csv_if_exists(site_with_market)
    if site is not None:
        print_df_summary(
            "site sog_with_market.csv (optional)",
            site,
            key_cols=[c for c in ["player_id", "game_id", "line"] if c in site.columns],
            focus_cols=[c for c in ["p_over", "price_over", "p_over_mkt", "edge_over"] if c in site.columns],
        )

    # ---- DB checks ----
    db_url = args.db_url or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if db_url:
        db_checks(db_url, slate)
        db_pp_role_coverage_check(db_url, slate)
        if args.db_rolling_check:
            db_rolling_check(db_url, args.db_rolling_view, args.db_rolling_days_back)
        if getattr(args, "db_toi_check", False):
            db_toi_integrity_check(
                db_url=db_url,
                slate=slate,
                days_back=args.db_toi_days_back,
                source=args.db_toi_source,
            )
    else:
        print("\n[sog_integrity] (DB checks skipped) Set SUPABASE_DB_URL or pass --db-url to enable.")

    print("\n[sog_integrity] done.")


if __name__ == "__main__":
    main()
