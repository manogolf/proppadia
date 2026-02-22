#!/usr/bin/env python3
"""
NHL pipeline CLI

Commands:
  daily        Run the full daily pipeline (schedule/roster → features → score → site files).
  fetch-odds   Fetch NHL player props odds (SOG + Saves + Points) to nhl/site/data JSON.
  build-sog    Build nhl/site/data/sog_with_market.csv from latest predictions + odds.
  build-saves  Build nhl/site/data/saves_with_market.csv from latest predictions + odds.
  build-points Build nhl/site/data/points_with_market.csv from latest predictions + odds.

Conventions:
  - All dates are Eastern Time (ET).
  - Artifacts:
      exports/                             (SQL exports consumed by models)
      backend/nhl/data/processed/          (model outputs)
      nhl/site/data/                       (site-consumed CSV/JSON)
  - Models live under:
      backend/nhl/models/sog
      backend/nhl/models/saves
      backend/nhl/models/points

  - Daily Run: python -m backend.nhl.cli daily --with-odds    
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import subprocess
import subprocess as sp
from pathlib import Path
import pandas as pd
from typing import Optional, Sequence, Union
from datetime import datetime, timedelta, timezone
import requests
import re


# ---------- bootstrap env ----------

BASE = Path(__file__).resolve().parent

def _load_dotenv_multi():
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    here = Path(__file__).resolve()
    root = here.parents[2]
    for p in (
        root / ".env.local",
        root / ".env",
        root / "backend" / ".env",
        root / "nhl" / ".env",
    ):
        if p.exists():
            load_dotenv(p, override=False)

_load_dotenv_multi()

ROOT = Path(__file__).resolve().parents[2]  # repo root
PY   = os.environ.get("PYTHON", sys.executable)

# Canonical NHL module root
NHL_DIR = ROOT / "backend" / "nhl"

SITE_DIR    = ROOT / "nhl" / "site" / "data"
EXPORTS_DIR = NHL_DIR / "exports"
PROC_DIR    = NHL_DIR / "data" / "processed"
SQL_DIR     = NHL_DIR / "sql"
SCRIPTS_DIR = NHL_DIR / "scripts"
MODELS_DIR  = NHL_DIR / "models"

# Daily artifact organization
EXPORTS_DAILY_NAMES_DIR = EXPORTS_DIR / "daily" / "names"
EXPORTS_DAILY_SOG_DIR   = EXPORTS_DIR / "daily" / "sog_features"

for d in (
    SITE_DIR,
    EXPORTS_DIR,
    PROC_DIR,
    EXPORTS_DAILY_NAMES_DIR,
    EXPORTS_DAILY_SOG_DIR,
):
    d.mkdir(parents=True, exist_ok=True)

# ---------- time helpers (ET) ----------

def et_today() -> str:
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except Exception:
        et = timezone(timedelta(hours=-5))
    return datetime.now(et).strftime("%Y-%m-%d")

def et_yesterday() -> str:
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        return (datetime.now(et) - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        return (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    
def infer_nhl_season_from_date_yyyy_mm_dd(date_str: str) -> int:
    # NHL season naming: season is the year the season starts (e.g., 2025-26 => 2025)
    y, m, d = (int(x) for x in date_str.split("-"))
    return y if m >= 9 else (y - 1)

# ---------- guardrails: prevent repeating the same fix/step ----------
def _guard_dir() -> Path:
    d = PROC_DIR / "_guard"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _guard_path(key: str, slate: str | None = None) -> Path:
    safe_key = re.sub(r"[^a-zA-Z0-9_.-]+", "_", key).strip("_")
    safe_slate = re.sub(r"[^0-9-]+", "_", (slate or "global"))
    return _guard_dir() / f"{safe_key}__{safe_slate}.done"

def guard_mark_done(key: str, slate: str | None = None, details: str = "") -> None:
    p = _guard_path(key, slate)
    payload = (details.strip() + "\n") if details else "done\n"
    p.write_text(payload, encoding="utf-8")

def guard_already_done(key: str, slate: str | None = None) -> bool:
    return _guard_path(key, slate).exists()

def guard_require_not_done(key: str, slate: str | None = None) -> None:
    """
    Hard stop if we try to repeat a step that was already marked done.
    Use this for 'assistant-suggested edits' so we don't churn.
    """
    p = _guard_path(key, slate)
    if p.exists():
        msg = p.read_text(encoding="utf-8").strip()
        raise AssertionError(f"[guard] step already done: {key} (slate={slate}). Notes: {msg}")

def guard_clear(key: str, slate: str | None = None) -> None:
    p = _guard_path(key, slate)
    if p.exists():
        p.unlink()

def guard_list(slate: str | None = None) -> list[tuple[str, str, str]]:
    """
    Return [(key, slate, notes)] for .done files under PROC_DIR/_guard.
    If slate is provided, filters to that slate (or 'global').
    """
    out = []
    d = _guard_dir()
    for p in sorted(d.glob("*.done")):
        name = p.name  # key__slate.done
        if "__" not in name:
            continue
        key, rest = name.split("__", 1)
        slate_part = rest.replace(".done", "")
        if slate is not None and slate_part not in {slate, "global"}:
            continue
        notes = p.read_text(encoding="utf-8").strip()
        out.append((key, slate_part, notes))
    return out

def guard_print(slate: str | None = None) -> None:
    rows = guard_list(slate)
    if not rows:
        print("[guard] no recorded steps.")
        return
    print("[guard] recorded steps:")
    for key, sl, notes in rows:
        msg = f"  - {key} (slate={sl})"
        if notes and notes != "done":
            msg += f": {notes}"
        print(msg)

# ---------- shell helpers ----------

def _psql_env() -> dict:
    """
    Build a safe environment for any psql subprocess spawned by cli.py.

    Why:
      - Supabase often enforces a low default statement_timeout (you saw 2min),
        which kills long-running \COPY exports.
      - We also want a consistent schema search_path for all sessions.

    Behavior:
      - Ensures search_path=nhl,public
      - Ensures statement_timeout=0 (unlimited) so exports don't get canceled
      - Keeps any existing PGOPTIONS flags (appends ours)
    """
    env = dict(os.environ)

    # Our required session settings
    required = [
        "-c search_path=nhl,public",
        "-c statement_timeout=0",
        # Optional: fail fast on lock waits instead of "hanging"
        "-c lock_timeout=5000",
    ]
    required_str = " ".join(required)

    # Preserve any existing PGOPTIONS and append ours
    existing = (env.get("PGOPTIONS") or "").strip()
    env["PGOPTIONS"] = (existing + " " + required_str).strip() if existing else required_str

    return env

def run_psql_bytes(db_url: str, sql: str, *, timeout: Optional[int] = None) -> bytes:
    """
    Runs a single SQL command via psql and returns stdout bytes.
    Raises if psql exits non-zero.
    """
    return subprocess.check_output(
        ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql],
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )

def is_testing() -> bool:
    return os.environ.get("TESTING") == "1"

def guard_testing_only_slate(slate: str | None, *, cmd_name: str) -> None:
    # Passing an explicit slate is considered "testing mode" behavior.
    if slate and not is_testing():
        raise AssertionError(
            f"[guard] {cmd_name} with an explicit slate is only allowed when TESTING=1. "
            f"Refusing slate={slate} in non-testing runs."
        )

def assert_sog_rollups_present(db_url: str, slate_date: str, *, min_ok_frac: float = 0.25) -> None:
    sql = f"""
    COPY (
      SELECT
        COUNT(*)::int AS n,
        COUNT(*) FILTER (WHERE d10_sog_per60 IS NOT NULL)::int AS n_d10_ok,
        COUNT(*) FILTER (WHERE attempts_d10_per60 IS NOT NULL)::int AS n_att_ok,
        CASE WHEN COUNT(*) = 0 THEN 0
             ELSE (COUNT(*) FILTER (WHERE d10_sog_per60 IS NOT NULL)::numeric / COUNT(*)::numeric)
        END AS frac_ok
      FROM nhl.training_features_nhl_sog_enriched_pregame_v2
      WHERE game_date = DATE '{slate_date}'
    ) TO STDOUT WITH CSV HEADER;
    """
    csv_bytes = run_psql_bytes(db_url, sql)
    lines = csv_bytes.decode("utf-8", errors="replace").splitlines()
    if len(lines) < 2:
        raise AssertionError(f"[sog_rollups_check] empty result for slate_date={slate_date}")

    header = lines[0].split(",")
    vals = lines[1].split(",")
    row = dict(zip(header, vals))

    n = int(row["n"])
    n_d10_ok = int(row["n_d10_ok"])
    frac_ok = float(row["frac_ok"])

    print(f"[sog_rollups_check] slate={slate_date} n={n} n_d10_ok={n_d10_ok} frac_ok={frac_ok:.3f}")

    if n == 0:
        raise AssertionError(f"[sog_rollups_check] no rows for slate_date={slate_date}")
    if frac_ok < min_ok_frac:
        raise AssertionError(
            f"[sog_rollups_check] rollups missing: slate_date={slate_date} "
            f"frac_ok={frac_ok:.3f} (n={n}, n_d10_ok={n_d10_ok})"
        )

def run(cmd, *, cwd: Path = ROOT, env: dict | None = None, check: bool = True):
    cmd = [str(c) for c in cmd]
    print("▶", " ".join(cmd))
    e = os.environ.copy()
    if env:
        e.update(env)
    try:
        return sp.run(cmd, cwd=str(cwd), env=e, check=check, text=True, capture_output=True)
    except sp.CalledProcessError as exc:
        print(f"[run] COMMAND FAILED: {' '.join(map(str, cmd))}", file=sys.stderr)
        if exc.stdout:
            print("[run] --- stdout ---", file=sys.stderr)
            print(exc.stdout, file=sys.stderr)
        if exc.stderr:
            print("[run] --- stderr ---", file=sys.stderr)
            print(exc.stderr, file=sys.stderr)
        raise

def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = raw.strip().lower()
    if not val:
        return default
    return val in {"1", "true", "t", "yes", "y", "on"}

def _append_cli_arg(cmd: list[str], flag: str, value: str | None) -> None:
    if value is None:
        return
    s = str(value).strip()
    if not s:
        return
    cmd.extend([flag, s])

def require_db_url() -> str:
    db = os.environ.get("SUPABASE_DB_URL")
    if not db:
        print("FATAL: SUPABASE_DB_URL missing", file=sys.stderr)
        sys.exit(2)
    return db

def run_psql_file(sql_file: Path, *, vars: dict[str, str] | None = None):
    db = require_db_url()
    cmd = ["psql", "--no-psqlrc", "--pset", "pager=off", "-v", "ON_ERROR_STOP=1", db]

    if vars:
        for k, v in vars.items():
            cmd += ["-v", f"{k}={v}"]

    cmd += ["-c", "SET statement_timeout=0;"]
    cmd += ["-f", str(sql_file)]

    run(cmd, env=_psql_env())

def run_psql_file_to_path(
    sql_file: Path,
    out_path: Path,
    *,
    vars: dict[str, str] | None = None,
) -> None:
    db = require_db_url()
    cmd = ["psql", "--no-psqlrc", "--pset", "pager=off", "-v", "ON_ERROR_STOP=1", db]

    if vars:
        for k, v in vars.items():
            cmd += ["-v", f"{k}={v}"]

    cmd += ["-c", "SET statement_timeout=0;"]
    cmd += ["-f", str(sql_file)]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        sp.run(cmd, env=_psql_env(), check=True, stdout=f)

def run_psql(sql: str) -> str:
    import os
    import subprocess

    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL (or DATABASE_URL)")

    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-A", "-F", ",", "-t", "-c", sql]
    p = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return p.stdout

def psql_one_row(db_url: str, sql: str) -> dict:
    wrapped = f"SELECT row_to_json(t) FROM ({sql}) t;"
    res = sp.run(
        ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-t", "-A", "-c", wrapped],
        capture_output=True, text=True
    )
    if res.returncode != 0:
        raise RuntimeError(
            "psql_one_row failed.\n"
            f"SQL:\n{wrapped}\n\n"
            f"STDOUT:\n{res.stdout}\n\n"
            f"STDERR:\n{res.stderr}\n"
        )

    out = (res.stdout or "").strip()
    if not out:
        raise RuntimeError(f"psql_one_row: expected 1 row, got 0.\nSQL:\n{wrapped}")

    try:
        return json.loads(out)
    except Exception as e:
        raise RuntimeError(f"psql_one_row: failed to parse JSON:\n{out}") from e

def _pred_game_dates(pred_csv: Path) -> list[str]:
    """Return sorted unique game_date strings found in sog_predictions.csv (if column exists)."""
    if not pred_csv.exists():
        return []
    try:
      
        df = pd.read_csv(pred_csv, usecols=lambda c: c in {"game_date"}, dtype={"game_date": "string"})
        if "game_date" not in df.columns:
            return []
        vals = df["game_date"].dropna().astype(str).unique().tolist()
        vals = sorted(set(v.strip() for v in vals if v and v.strip()))
        return vals
    except Exception:
        return []
    
def require_single_game_date_csv(path: Path, slate: str, *, col: str = "game_date", label: str = "") -> None:
    """
    Guardrail: ensure a CSV contains exactly one unique game_date and it matches slate.
    Fails early so we don't chase downstream join errors.
    """

    if not path.exists() or path.stat().st_size == 0:
        raise AssertionError(f"[guard] missing/empty CSV: {label or path}")

    df = pd.read_csv(path, usecols=lambda c: c == col, dtype={col: "string"})
    if col not in df.columns:
        raise AssertionError(f"[guard] {label or path} missing required column: {col}")

    vals = df[col].dropna().astype(str).map(lambda s: s.strip()).tolist()
    uniq = sorted(set(v for v in vals if v))
    if uniq != [slate]:
        show = uniq[:5]
        raise AssertionError(
            f"[guard] {label or path} {col} mismatch: expected [{slate}] got {show}"
        )

def _run_sog_evaluator() -> None:
    subprocess.check_call([sys.executable, "backend/nhl/scripts/evaluate_sog_predictions.py"])

def psql_stdout(sql_file: Path, *, vars: dict[str, str] | None = None) -> bytes:
    """Run psql on a file that COPY/SELECTs TO STDOUT and return stdout bytes."""
    db = require_db_url()
    cmd = ["psql", "--no-psqlrc", "--pset", "pager=off", "-q", "-v", "ON_ERROR_STOP=1", db]

    if vars:
        for k, v in vars.items():
            cmd += ["-v", f"{k}={v}"]

    cmd += ["-c", "SET statement_timeout=0;"]
    cmd += ["-f", str(sql_file)]

    res = sp.run(
        cmd,
        cwd=str(ROOT),
        env=_psql_env(),
        check=False,
        capture_output=True,
        text=False,  # IMPORTANT: keep stdout as bytes
    )

    if res.returncode != 0:
        print("psql FAILED:", " ".join(cmd), file=sys.stderr)
        if res.stderr:
            try:
                print(res.stderr.decode("utf-8", errors="replace").strip(), file=sys.stderr)
            except Exception:
                print(str(res.stderr)[:2000], file=sys.stderr)

        # show tail of stdout too (COPY can emit partial output)
        if res.stdout:
            try:
                tail = b"\n".join(res.stdout.splitlines()[-30:]).decode("utf-8", errors="replace")
                print("psql stdout tail:\n" + tail, file=sys.stderr)
            except Exception:
                pass

        raise sp.CalledProcessError(res.returncode, cmd, output=res.stdout, stderr=res.stderr)

    return res.stdout

def refresh_sog_denali_rollups_window(db: str, *, start_date: str, end_date: str) -> None:
    """
    Recompute + persist rolling SOG features into:
      nhl.training_features_nhl_sog_enriched_pregame_v2

    This prevents 'frozen' rollups by making the daily runner actively refresh them.

    Window behavior:
      - Updates target rows where game_date in [start_date, end_date]
      - Computes rolling sums from nhl.skater_game_logs_raw joined to nhl.games
      - STRICT: uses only realized games strictly BEFORE end_date (end_date - 1 day)
      - Converts to per60 using TOI minutes
    """
    sql = f"""
    BEGIN;

    WITH params AS (
      SELECT DATE '{start_date}' AS start_date, DATE '{end_date}' AS end_date
    ),
    realized AS (
      SELECT
        l.player_id::bigint AS player_id,
        g.game_id::bigint   AS game_id,
        g.game_date::date   AS game_date,

        -- safe numeric casts (handle '' and NULL)
        COALESCE(NULLIF(BTRIM(l.shots_on_goal::text), ''), '0')::numeric AS sog,
        COALESCE(NULLIF(BTRIM(l.shot_attempts::text), ''), '0')::numeric AS attempts,

        NULLIF(COALESCE(NULLIF(BTRIM(l.toi_minutes::text), ''), '0')::numeric, 0) AS toi_min
      FROM nhl.skater_game_logs_raw l
      JOIN nhl.games g USING (game_id)
      JOIN params p ON TRUE
      -- include enough history before start_date so rolling windows at start_date aren't empty
      -- and exclude end_date itself to avoid any same-day leakage
      WHERE g.game_date BETWEEN (p.start_date - INTERVAL '260 days') AND (p.end_date - INTERVAL '1 day')
    ),
    rolls AS (
      SELECT
        r.player_id,
        r.game_id,
        r.game_date,

        SUM(r.sog)      OVER w5  AS sog_5,
        SUM(r.toi_min)  OVER w5  AS toi_5,

        SUM(r.sog)      OVER w10 AS sog_10,
        SUM(r.attempts) OVER w10 AS att_10,
        SUM(r.toi_min)  OVER w10 AS toi_10,

        SUM(r.sog)      OVER w20 AS sog_20,
        SUM(r.toi_min)  OVER w20 AS toi_20,

        ROW_NUMBER() OVER (
          PARTITION BY r.player_id
          ORDER BY r.game_date DESC, r.game_id DESC
        ) AS rn
      FROM realized r
      WINDOW
        w5  AS (PARTITION BY r.player_id ORDER BY r.game_date, r.game_id ROWS BETWEEN 4  PRECEDING AND CURRENT ROW),
        w10 AS (PARTITION BY r.player_id ORDER BY r.game_date, r.game_id ROWS BETWEEN 9  PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY r.player_id ORDER BY r.game_date, r.game_id ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    ),
    rollups AS (
      SELECT
        player_id,
        CASE WHEN toi_5  IS NULL OR toi_5  <= 0 THEN NULL ELSE (sog_5  / toi_5 ) * 60 END AS d5_sog_per60,
        CASE WHEN toi_10 IS NULL OR toi_10 <= 0 THEN NULL ELSE (sog_10 / toi_10) * 60 END AS d10_sog_per60,
        CASE WHEN toi_20 IS NULL OR toi_20 <= 0 THEN NULL ELSE (sog_20 / toi_20) * 60 END AS d20_sog_per60,
        CASE WHEN toi_10 IS NULL OR toi_10 <= 0 THEN NULL ELSE (att_10 / toi_10) * 60 END AS attempts_d10_per60
      FROM rolls
      WHERE rn = 1
    )
    UPDATE nhl.training_features_nhl_sog_enriched_pregame_v2 t
    SET
      d5_sog_per60       = r.d5_sog_per60,
      d10_sog_per60      = r.d10_sog_per60,
      d20_sog_per60      = r.d20_sog_per60,
      attempts_d10_per60 = r.attempts_d10_per60
    FROM rollups r, params p
    WHERE
      t.player_id = r.player_id
      AND t.game_date BETWEEN p.start_date AND p.end_date;

    COMMIT;
    """

    print(f"↻ Refreshing SOG rollups in-table for {start_date}..{end_date} ...")
    run(["psql", db, "--no-psqlrc", "-v", "ON_ERROR_STOP=1", "-c", sql])
    print("✅ SOG rollups refreshed.")

def safe_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2))


def export_sog_denali_features(db_url: str, slate_date: str, out_path: Path) -> None:
    """
    Export Denali SOG features for a given slate_date into a CSV used by the SOG scorer.

    Behavior:
      - (Upstream guard) Fails early if pairings/coverage substrate is missing on the slate rows.
      - Runs backend/nhl/sql/export_sog_denali_pregame.sql via psql.
      - Passes slate_date as a psql variable: -v slate_date=YYYY-MM-DD
      - Script itself does COPY ... TO STDOUT WITH CSV HEADER.
      - (Post guard) Ensures CSV is non-empty and has basic ID columns.
    """
    sql_path = BASE / "sql" / "export_sog_denali_pregame.sql"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # UPSTREAM GUARD: fail export if pairings/coverage substrate is missing
    # (This is intentionally BEFORE we write any CSV.)
    # ------------------------------------------------------------------
    guard_sql = f"""
    WITH s AS (
      SELECT COUNT(*)::int AS n_rows
      FROM nhl.training_features_nhl_sog_enriched_pregame_v2
      WHERE game_date = DATE '{slate_date}'
    ),
    nn AS (
      SELECT
        COUNT(d10_shiftcharts_coverage_rate)::int AS nn_d10_cov,
        COUNT(d20_shiftcharts_coverage_rate)::int AS nn_d20_cov,
        COUNT(d10_pairings_available)::int        AS nn_d10_avail,
        COUNT(d20_pairings_available)::int        AS nn_d20_avail
      FROM nhl.training_features_nhl_sog_enriched_pregame_v2
      WHERE game_date = DATE '{slate_date}'
    )
    SELECT
      s.n_rows,
      nn.nn_d10_cov, nn.nn_d20_cov,
      nn.nn_d10_avail, nn.nn_d20_avail
    FROM s, nn;
    """

    proc = sp.run(
        ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-t", "-A", "-c", guard_sql],
        check=True,
        capture_output=True,
        text=True,
        env=_psql_env(),
    )

    # output format: n_rows|nn_d10_cov|nn_d20_cov|nn_d10_avail|nn_d20_avail
    parts = proc.stdout.strip().split("|")
    if len(parts) != 5:
        raise RuntimeError(f"[guard] unexpected guard output: {proc.stdout!r}")

    n_rows, nn_d10_cov, nn_d20_cov, nn_d10_avail, nn_d20_avail = map(int, parts)

    if n_rows == 0:
        raise RuntimeError(
            f"[guard] no rows in nhl.training_features_nhl_sog_enriched_pregame_v2 for slate_date={slate_date}"
        )

    if nn_d10_cov == 0 or nn_d20_cov == 0 or nn_d10_avail == 0 or nn_d20_avail == 0:
        raise RuntimeError(
            f"[guard] missing pairings/coverage substrate for slate_date={slate_date}: "
            f"n_rows={n_rows} nn_d10_cov={nn_d10_cov} nn_d20_cov={nn_d20_cov} "
            f"nn_d10_avail={nn_d10_avail} nn_d20_avail={nn_d20_avail}. "
            "This should be fixed upstream (fill_sog_pairings_rolling_for_slate.sql / fill_sog_pairings_for_slate.sql)."
        )

    # ------------------------------------------------------------------
    # EXPORT
    # ------------------------------------------------------------------
    with out_path.open("w", encoding="utf-8", newline="") as f:
        sp.run(
            [
                "psql",
                db_url,
                "-v", "ON_ERROR_STOP=1",
                "-v", f"slate_date={slate_date}",
                "-f", str(sql_path),
            ],
            check=True,
            stdout=f,
            env=_psql_env(),
        )

    # ------------------------------------------------------------------
    # POST-EXPORT GUARD: ensure artifact is real + has ID columns
    # ------------------------------------------------------------------
    if (not out_path.exists()) or out_path.stat().st_size < 200:
        raise AssertionError(
            f"[guard] export produced empty/small CSV: {out_path} (slate_date={slate_date})"
        )

    hdr = pd.read_csv(out_path, nrows=1)
    required_cols = ["player_id", "game_id", "game_date"]
    missing = [c for c in required_cols if c not in hdr.columns]
    if missing:
        raise AssertionError(
            f"[guard] {out_path.name} missing columns {missing} (slate_date={slate_date}). "
            f"got={list(hdr.columns)}"
        )

    print(f"✅ Exported SOG Denali features for {slate_date} → {out_path}")

# ---------- names export ----------

def export_names_csv(slate: str) -> Path:
    """
    exports/names_{slate}.csv with columns:
      player_id,full_name,team_id,team_code,game_id,game_date

    Uses backend/nhl/sql/_export_names.sql, which expects:
      -v slate_date=YYYY-MM-DD
    """
    out_path = EXPORTS_DAILY_NAMES_DIR / f"names_{slate}.csv"

    # Base directory for this NHL backend module (backend/nhl)
    nhl_base = Path(__file__).resolve().parent
    sql_path = nhl_base / "sql" / "_export_names.sql"

    # Run the static SQL with a bound slate_date variable and capture CSV bytes
    csv_bytes = psql_stdout(sql_path, vars={"slate_date": slate})
    if not isinstance(csv_bytes, (bytes, bytearray)):
        raise AssertionError(
            f"[guard] psql_stdout must return bytes, got {type(csv_bytes).__name__}"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(csv_bytes)
    require_single_game_date_csv(out_path, slate, label="names export")

    # Guardrail: verify header/shape so we don't proceed with a broken names export
    try:
        df_head = pd.read_csv(out_path, nrows=5)
        need = {"player_id", "game_id", "full_name", "team_id", "team_code", "game_date"}
        miss = sorted(need - set(df_head.columns))
        if miss:
            raise AssertionError(
                f"[export_names_csv] BAD CSV header (missing={miss}). "
                f"got={list(df_head.columns)} file={out_path}"
            )
        if df_head.empty:
            raise AssertionError(f"[export_names_csv] names CSV has no rows: {out_path}")
    except Exception as e:
        raise AssertionError(f"[export_names_csv] names CSV validation failed: {e}") from e

    print(f"[export_names_csv] wrote names CSV → {out_path}")
    return out_path

# ---------- odds fetch ----------

def fetch_odds(
    days_from: int = 1,
    markets: str = "player_shots_on_goal,player_shots_on_goal_alternate,player_total_saves,player_points",
    regions: str = "us,us2",
    odds_format: str = "american",
    out_latest: Path = SITE_DIR / "odds_latest.json",
    out_today: Path = SITE_DIR / "odds_nhl_playerprops_today.json",
):
    key = os.environ.get("ODDS_API_KEY", "").strip()
    print(f"[fetch_odds] Using ODDS_API_KEY starting with: {key[:8]!r}")
    if not key:
        print("⚠️  ODDS_API_KEY not set — writing empty odds files.")
        safe_json([], out_today)
        try:
            out_latest.write_text(out_today.read_text())
        except Exception:
            pass
        return

    base = "https://api.the-odds-api.com/v4/sports/icehockey_nhl"

    # 1) Fetch events
    ev_url = f"{base}/events?dateFormat=iso&daysFrom={days_from}&apiKey={key}"
    print(f"→ Fetching events (daysFrom={days_from}) … {ev_url}")
    try:
        r = requests.get(ev_url, timeout=30)
        print(f"   events status={r.status_code}")
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to fetch events from The Odds API: {e}")
        safe_json([], out_today)
        try:
            out_latest.write_text(out_today.read_text())
        except Exception:
            pass
        return

    try:
        events = r.json()
    except Exception as e:
        print(f"❌ Failed to parse events JSON: {e}")
        safe_json([], out_today)
        try:
            out_latest.write_text(out_today.read_text())
        except Exception:
            pass
        return

    if not isinstance(events, list):
        print(f"❌ Unexpected events payload type: {type(events)}; writing empty odds.")
        safe_json([], out_today)
        try:
            out_latest.write_text(out_today.read_text())
        except Exception:
            pass
        return

    (SITE_DIR / "events_today.json").write_text(json.dumps(events))
    print(f"   events_today.json → {len(events)} events")

    # 2) Fetch player props per event
    print(f"→ Fetching player props (markets={markets}, regions={regions}) …")
    all_event_odds: list[dict] = []
    ok_count = 0
    fail_no_id = 0
    fail_http = 0

    for ev in events:
        eid = ev.get("id")
        home = ev.get("home_team") or ev.get("homeTeam")
        away = ev.get("away_team") or ev.get("awayTeam")

        if not eid:
            fail_no_id += 1
            print(f"   ⚠️  Event missing id; home={home}, away={away} → appending empty dict")
            all_event_odds.append({})
            continue

        url = (
            f"{base}/events/{eid}/odds"
            f"?regions={regions}&markets={markets}&oddsFormat={odds_format}&apiKey={key}"
        )

        last_status = None
        last_text = None
        success = False

        for attempt in (1, 2, 3):
            try:
                rr = requests.get(url, timeout=30)
                last_status = rr.status_code
                if rr.ok:
                    try:
                        j = rr.json()
                        all_event_odds.append(j)
                        ok_count += 1
                        success = True
                        break
                    except Exception as e:
                        print(f"   ❌ JSON parse error for event {eid} (attempt {attempt}): {e}")
                else:
                    last_text = rr.text[:200]
                    print(
                        f"   ⚠️  Odds request failed for event {eid} (attempt {attempt}) "
                        f"status={rr.status_code}"
                    )
            except Exception as e:
                print(f"   ❌ Exception fetching odds for event {eid} (attempt {attempt}): {e}")

        if not success:
            fail_http += 1
            print(
                f"   ⚠️  Giving up on event {eid} after retries; "
                f"last_status={last_status}, snippet={last_text!r}"
            )
            all_event_odds.append({})

    # 3) Summary + write files
    print(
        f"✅ fetch_odds summary: events={len(events)}, "
        f"success={ok_count}, missing_id={fail_no_id}, http_fail={fail_http}"
    )

    # If literally everything failed, prefer an empty array over [ {}, {}, ... ]
    if ok_count == 0:
        print("⚠️  No odds succeeded — writing [] instead of list of empty dicts.")
        safe_json([], out_today)
    else:
        safe_json(all_event_odds, out_today)

    try:
        out_latest.write_text(out_today.read_text())
    except Exception as e:
        print(f"⚠️  Failed to mirror odds to odds_latest.json: {e}")

# ---------- builders (CSV for site) ----------

def build_sog(slate: str):
    # Always regenerate (or overwrite) names for this slate and use the returned path
    names_csv = export_names_csv(slate)

    pred_path = PROC_DIR / "sog_predictions_wide_calibrated.csv"

    if not pred_path.exists() or pred_path.stat().st_size < 200:
        raise AssertionError(f"[build-sog] missing/empty calibrated predictions: {pred_path}")

    dates = _pred_game_dates(pred_path)
    if not dates:
        raise AssertionError(f"[build-sog] predictions CSV has no game_date values: {pred_path}")

    if len(dates) != 1 or dates[0] != slate:
        raise AssertionError(
            f"[build-sog] slate mismatch: expected {slate}, got {dates} in {pred_path}. "
            f"Run daily again; refusing to auto-regenerate."
        )
    
    if not pred_path.exists() or pred_path.stat().st_size == 0:
        raise AssertionError(f"[build-sog] expected artifact missing/empty: {pred_path}")

    if not names_csv.exists() or names_csv.stat().st_size == 0:
        raise AssertionError(f"[build-sog] expected artifact missing/empty: {names_csv}")

    run(
        [
            PY,
            SCRIPTS_DIR / "build_sog_with_market.py",
            "--pred",       str(pred_path),
            "--names",      str(names_csv),
            "--odds-json",  "nhl/site/data/odds_latest.json",
            "--out",        "nhl/site/data/sog_with_market.csv",
            "--unmatched",  "nhl/site/data/unmatched_sog.csv",
            "--slate-date", slate,
        ],
    )

    # Postcondition: market merge must produce a non-empty output artifact
    out_csv = SITE_DIR / "sog_with_market.csv"
    if not out_csv.exists() or out_csv.stat().st_size == 0:
        raise AssertionError(f"[build-sog] expected artifact missing/empty: {out_csv}")
    
    unmatched_csv = SITE_DIR / "unmatched_sog.csv"
    if not unmatched_csv.exists() or unmatched_csv.stat().st_size == 0:
        raise AssertionError(f"[build-sog] expected artifact missing/empty: {unmatched_csv}")


def build_saves(slate: str):
    # Ensure names exist (build_saves can be called standalone)
    names_csv = export_names_csv(slate)

    pred_path = PROC_DIR / "saves_predictions.csv"
    run(
        [
            PY,
            SCRIPTS_DIR / "build_saves_with_market.py",
            "--pred", pred_path,
            "--names", names_csv,
            "--odds-json", SITE_DIR / "odds_latest.json",
            "--out", SITE_DIR / "saves_with_market.csv",
            "--unmatched", SITE_DIR / "unmatched_saves.csv",
        ],
        env={"SLATE_DATE": slate},
    )


def build_points(slate: str):
    args = [
        PY,
        SCRIPTS_DIR / "build_points_with_market.py",
        "--odds-json",   SITE_DIR / "odds_latest.json",
        "--events-json", SITE_DIR / "events_today.json",
        "--out",         SITE_DIR / "points_with_market.csv",
        "--unmatched",   SITE_DIR / "unmatched_points.csv",
    ]

    pred_path = PROC_DIR / "points_predictions.csv"
    if pred_path.exists():
        args += ["--pred", pred_path]

    # Only include names if we actually have them (and don't assume location)
    names_path = export_names_csv(slate)
    if names_path.exists():
        args += ["--names", names_path]

    run(args)

# ---------- daily pipeline ----------

# --- REPLACE the very top of cmd_daily(with_odds: bool) down through the two print() lines ---
def cmd_daily(with_odds: bool):
    db = require_db_url()

    # DAILY SHOULD MEAN "TODAY" BY DEFAULT.
    #
    # We only honor SLATE_DATE / YDAY from env if you explicitly opt in by setting:
    #   HONOR_ENV_DATES=1
    #
    # This prevents stale SLATE_DATE from old debugging sessions from silently
    # forcing yesterday (or any prior date) into today's processed outputs.
    honor_env = os.environ.get("HONOR_ENV_DATES") == "1"

    if honor_env:
        # Explicit opt-in behavior (historical/replay runs)
        slate = os.environ.get("SLATE_DATE") or et_today()
        yday = os.environ.get("YDAY")
        if not yday:
            yday = slate if os.environ.get("SLATE_DATE") else et_yesterday()
    else:
        # Default behavior for real daily runs: ignore any stale env values
        os.environ.pop("SLATE_DATE", None)
        os.environ.pop("YDAY", None)
        slate = et_today()
        yday = et_yesterday()

    os.environ["SLATE_DATE"] = slate
    os.environ["YDAY"] = yday

    season = infer_nhl_season_from_date_yyyy_mm_dd(yday)

    print(f"SLATE_DATE (ET): {slate}" + (" (honor env)" if honor_env else ""))
    print(f"YDAY       (ET): {yday}" + (" (honor env)" if honor_env else ""))

    # --- Daily artifact dirs (your weekly cleanup automation) ---
    DAILY_EXPORTS_DIR = ROOT / "backend" / "nhl" / "exports" / "daily"
    DAILY_NAMES_DIR = DAILY_EXPORTS_DIR / "names"
    DAILY_SOG_FEATURES_DIR = DAILY_EXPORTS_DIR / "sog_features"
    for d in (DAILY_NAMES_DIR, DAILY_SOG_FEATURES_DIR):
        d.mkdir(parents=True, exist_ok=True)

    # 0) DB sanity
    run(["psql", db, "-v", "ON_ERROR_STOP=1", "-c", "SELECT now();"])

    # 0b) Full-league roster/player refresh (all teams, not slate-limited)
    # Non-fatal: if upstream NHL API is flaky, keep daily slate pipeline running.
    try:
        run([PY, SCRIPTS_DIR / "refresh_all_team_rosters.py"], env={"SLATE_DATE": slate})
    except Exception as e:
        print(f"⚠️ full-team roster refresh failed/skipped (continuing): {e}")

    # ============================================================
    # PHASE A: Finalize YDAY into raw/history FIRST (the guardrail)
    # ============================================================

    # A1) Pull yesterday logs + shiftcharts into stage (safe even if yday had no games)
    run([PY, SCRIPTS_DIR / "seed_goalie_logs_for_date.py"],        env={"SLATE_DATE": yday})
    run([PY, SCRIPTS_DIR / "refresh_players_and_roster_today.py"], env={"SLATE_DATE": yday})
    run([PY, SCRIPTS_DIR / "seed_skater_logs_for_date.py"],        env={"SLATE_DATE": yday})
    run([PY, SCRIPTS_DIR / "ingest_shiftcharts_for_date.py"],      env={"SLATE_DATE": yday})

    # A2) Promote stage → raw for yday (idempotent upsert)
    promote_sql = f"""
    WITH src AS (
      SELECT DISTINCT
        s.player_id, s.game_id, s.game_date,
        s.shots_on_goal, s.shot_attempts, s.toi_minutes, s.pp_toi_minutes
      FROM nhl.import_skater_logs_stage s
      WHERE s.game_date = DATE '{yday}'
    ),
    rs AS (
      SELECT DISTINCT ON (game_id, player_id)
        game_id,
        team_id,
        player_id
      FROM nhl.roster_status
      WHERE game_id IN (SELECT game_id FROM nhl.games WHERE game_date = DATE '{yday}')
      ORDER BY game_id, player_id, asof_ts DESC
    ),
    g AS (
      SELECT game_id, home_team_id, away_team_id
      FROM nhl.games
      WHERE game_date = DATE '{yday}'
    ),
    joined AS (
      SELECT
        src.player_id,
        src.game_id,
        rs.team_id,
        CASE
          WHEN rs.team_id = g.home_team_id THEN g.away_team_id
          WHEN rs.team_id = g.away_team_id THEN g.home_team_id
          ELSE NULL
        END AS opponent_id,
        (rs.team_id = g.home_team_id) AS is_home,
        src.game_date,
        src.shots_on_goal,
        src.shot_attempts,
        src.toi_minutes,
        src.pp_toi_minutes
      FROM src
      JOIN rs ON rs.game_id = src.game_id AND rs.player_id = src.player_id
      JOIN g  ON g.game_id  = src.game_id
    )
    INSERT INTO nhl.skater_game_logs_raw
      (player_id, game_id, team_id, opponent_id, is_home, game_date,
       shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes)
    SELECT
      player_id, game_id, team_id, opponent_id, is_home, game_date,
      shots_on_goal, shot_attempts, toi_minutes, NULLIF(pp_toi_minutes, 0) AS pp_toi_minutes
    FROM joined
    WHERE opponent_id IS NOT NULL
    ON CONFLICT (player_id, game_id) DO UPDATE SET
      team_id        = EXCLUDED.team_id,
      opponent_id    = EXCLUDED.opponent_id,
      is_home        = EXCLUDED.is_home,
      game_date      = EXCLUDED.game_date,
      shots_on_goal  = EXCLUDED.shots_on_goal,
      shot_attempts  = COALESCE(EXCLUDED.shot_attempts, nhl.skater_game_logs_raw.shot_attempts),
      toi_minutes    = EXCLUDED.toi_minutes,
      pp_toi_minutes = COALESCE(NULLIF(EXCLUDED.pp_toi_minutes, 0), nhl.skater_game_logs_raw.pp_toi_minutes);
    """
    run(["psql", db, "-v", "ON_ERROR_STOP=1", "-c", promote_sql])

    # A3a) Build manpower segments (PP/PK windows) for this slate date (required for PP TOI)
    run([PY, SCRIPTS_DIR / "backfill_game_manpower_segments.py", "--start-date", yday, "--end-date", yday, "--season", str(season)])

    # A3b) Fill PP TOI minutes (depends on manpower segments)
    run([PY, SCRIPTS_DIR / "fill_pp_toi_minutes_for_date.py", "--date", yday, "--commit"])

    # A4) Pairings artifacts for yday (built ONCE)
    run_psql_file(SQL_DIR / "shiftcharts_pairings_for_date.sql", vars={"game_date": yday})

    # A5) Refresh views/materializations now that yday raw/pp_toi/pairings are updated
    refresh_sql = SCRIPTS_DIR / "refresh.sql"
    if refresh_sql.exists():
        run(["psql", db, "-v", "ON_ERROR_STOP=1", "-f", refresh_sql])

    # A6) Evaluate SOG for YDAY (only if actuals exist)
    try:
        run(
            [
                PY,
                SCRIPTS_DIR / "evaluate_sog_predictions.py",
                "--game-date", yday,
            ]
        )
    except Exception as e:
        print(f"⚠️ evaluate_sog_predictions failed/skipped (continuing): {e}")

    # ============================================================
    # PHASE B: Build SLATE pregame features (history through yday)
    # ============================================================

    # 1) Today: schedule & roster
    run([PY, SCRIPTS_DIR / "import_schedule_today.py"], env={"SLATE_DATE": slate})

    # --- EARLY EXIT: no NHL games on this slate date ---
    no_games_sql = f"SELECT COUNT(*) FROM nhl.games WHERE game_date = DATE '{slate}';"
    res = sp.run(
        ["psql", db, "-v", "ON_ERROR_STOP=1", "-t", "-A", "-c", no_games_sql],
        capture_output=True, text=True, check=True
    )
    game_count = int((res.stdout or "").strip() or "0")
    if game_count == 0:
        print(f"ℹ️ No NHL games for {slate} (ET) — skipping scoring/export steps (yday finalization already done).")
        return
    # --- end early exit ---

    run(
        [PY, SCRIPTS_DIR / "import_roster_today.py"],
        env={"SLATE_DATE": slate, "SKIP_ROSTER_STATUS": "1", "SKIP_PLAYERS": "1"},
    )
    run(
        [PY, SCRIPTS_DIR / "refresh_players_and_roster_today.py"],
        env={"SLATE_DATE": slate},
    )

    # 2) Seed features for today (SOG + Saves).
    run_psql_file(SQL_DIR / "seed_sog_features_for_slate.sql",    vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "fill_sog_counts_for_slate.sql",      vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "seed_goalie_features_for_slate.sql", vars={"slate_date": slate})

    # 2b) Refresh rolling SOG features into the pregame table
    # Guardrail: rollups may only use realized stats through YDAY.
    rollup_start = os.environ.get("ROLLUP_START_DATE") or (
        datetime.fromisoformat(slate) - timedelta(days=260)
    ).strftime("%Y-%m-%d")
    refresh_sog_denali_rollups_window(db, start_date=rollup_start, end_date=slate)

    # 2c) Fill PP role + TOI-derived features for THIS slate (computed from history < slate)
    run_psql_file(SQL_DIR / "fill_sog_pp_role_for_slate.sql",              vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "fill_sog_toi_features_for_slate.sql",         vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "fill_sog_season_toi_features_for_slate.sql",  vars={"slate_date": slate})

    # 2d) Pairings fill into TODAY's pregame table using the yday-built pairings history
    run_psql_file(SQL_DIR / "fill_sog_pairings_for_slate.sql",             vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "fill_sog_pairings_rolling_for_slate.sql",     vars={"slate_date": slate})

    row = psql_one_row(
        db,
        f"""
        SELECT
        COUNT(*)::int AS n,
        COUNT(*) FILTER (WHERE szn_toi_per_game_5on5 IS NULL)::int AS null_5v5,
        COUNT(*) FILTER (WHERE season_5on5_icetime_per_game IS NULL)::int AS null_season_5v5
        FROM nhl.training_features_nhl_sog_enriched_pregame_v2
        WHERE game_date = DATE '{slate}'
        """
    )

    n = int(row["n"])
    null_5v5 = int(row["null_5v5"])
    null_season_5v5 = int(row["null_season_5v5"])

    # allow a couple misses (callups), but not systemic failure
    if n > 0 and (null_5v5 > 0.20 * n or null_season_5v5 > 0.20 * n):
        raise AssertionError(f"[SOG] season TOI features missing too often for {slate}: {row}")

    # After seed_sog_features_for_slate + pairings fills

    # 3) Export names (single source of truth)
    try:
        names_path = export_names_csv(slate)
    except Exception as e:
        names_path = None
        print(f"⚠️ names export failed; downstream builders may degrade: {e}")
    # 3.9) Team context rollups for slate
    run_psql_file(SQL_DIR / "upsert_team_context_for_slate.sql", vars={"slate_date": slate})

    # 4) Export feature CSVs for this slate

    # 4a) SOG Denali features → backend/nhl/exports/daily/sog_features/
    sog_feat_path = DAILY_SOG_FEATURES_DIR / f"sog_features_{slate}_denali.csv"
    # --- ensure TOI/shift “season” features are populated before exporting Denali SOG slate features ---
    export_sog_denali_features(db, slate, sog_feat_path)

    # 4b) Saves / Points exporters
    saves_csv  = psql_stdout(SQL_DIR / "export_saves_from_denali.sql", vars={"slate_date": slate})
    points_csv = psql_stdout(SQL_DIR / "export_points.sql",            vars={"slate_date": slate})
    (EXPORTS_DIR / "train_goalie_saves_v2.csv").write_bytes(saves_csv)
    (EXPORTS_DIR / "train_nhl_points_v2.csv").write_bytes(points_csv)
    print("exports → sog_features_{slate}_denali.csv, train_goalie_saves_v2.csv, train_nhl_points_v2.csv")

    # 5) Score SOG (ORDINAL LGBM) — single source of truth
    ordinal_root = (
        ROOT
        / "backend" / "nhl" / "models" / "latest" / "shots_on_goal"
        / "sog_player_denali_pairings_ordinal_v1__no_shiftcounts"
    )
    ordinal_meta = ordinal_root / "ge_2" / "metadata.json"  # canonical feature list lives here

    if not ordinal_root.exists():
        raise SystemExit(f"Missing ORDINAL SOG models at {ordinal_root}")

    if not ordinal_meta.exists():
        raise SystemExit(f"Missing ORDINAL feature metadata at {ordinal_meta}")

    calibrated_pred_path = PROC_DIR / "sog_predictions_wide_calibrated.csv"
    run(
        [
            PY,
            SCRIPTS_DIR / "score_sog_denali_pairings_ordinal_lgbm.py",
            "--in",           str(sog_feat_path),
            "--out",          str(calibrated_pred_path),
            "--model-root",   str(ordinal_root),
            "--feature-meta", str(ordinal_meta),
        ]
    )

    # ---- GUARD: ordinal predictions must exist + match slate ----
    calib_path = calibrated_pred_path
    if not calib_path.exists() or calib_path.stat().st_size < 200:
        raise AssertionError(f"[daily] missing/empty ordinal predictions: {calib_path}")

    dates = _pred_game_dates(calib_path)
    if not dates:
        raise AssertionError(f"[daily] ordinal CSV has no game_date column or no values: {calib_path}")

    if len(dates) != 1 or dates[0] != slate:
        raise AssertionError(
            f"[daily] ordinal predictions slate mismatch: expected {slate}, got {dates}. "
            f"Refusing to continue."
        )

    # 5a.1) Optional segmented recency calibration (feature-flagged).
    # Default ON for daily runs; set NHL_SOG_SEGMENTED_CALIBRATION_ENABLED=0 to disable quickly.
    seg_cal_enabled = _env_bool("NHL_SOG_SEGMENTED_CALIBRATION_ENABLED", default=True)
    seg_cal_required = _env_bool("NHL_SOG_SEGMENTED_CALIBRATION_REQUIRED", default=False)
    if seg_cal_enabled:
        seg_cal_cmd = [
            PY,
            SCRIPTS_DIR / "calibrate_sog_segmented_recency.py",
            "--pred-csv", str(calibrated_pred_path),
            "--out-csv", str(calibrated_pred_path),
        ]
        _append_cli_arg(
            seg_cal_cmd,
            "--model-family",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_MODEL_FAMILY")
            or os.environ.get("NHL_SOG_MODEL_FAMILY"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--model-version",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_MODEL_VERSION")
            or os.environ.get("NHL_SOG_MODEL_VERSION"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--lines",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_LINES")
            or os.environ.get("NHL_SOG_LINES"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--lookback-days",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_LOOKBACK_DAYS")
            or os.environ.get("NHL_SOG_CAL_LOOKBACK_DAYS"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--segment-min-rows",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_SEGMENT_MIN_ROWS")
            or os.environ.get("NHL_SOG_CAL_SEGMENT_MIN_ROWS"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--blend-alpha",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_BLEND_ALPHA")
            or os.environ.get("NHL_SOG_CAL_BLEND_ALPHA"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--decay-half-life-days",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_DECAY_HALF_LIFE_DAYS")
            or os.environ.get("NHL_SOG_CAL_DECAY_HALF_LIFE_DAYS"),
        )
        _append_cli_arg(
            seg_cal_cmd,
            "--asof-date",
            os.environ.get("NHL_SOG_SEGMENTED_CALIBRATION_ASOF_DATE"),
        )
        if _env_bool("NHL_SOG_SEGMENTED_CALIBRATION_STRICT", default=False):
            seg_cal_cmd.append("--strict")

        try:
            run(seg_cal_cmd)
            print("✅ segmented SOG calibration applied.")
        except Exception as exc:
            if seg_cal_required:
                raise RuntimeError(
                    "Segmented SOG calibration failed with NHL_SOG_SEGMENTED_CALIBRATION_REQUIRED=1."
                ) from exc
            print(f"⚠️ segmented SOG calibration failed; continuing with ordinal output: {exc}")

    # 5b) Load ordinal SOG into nhl.predictions
    run(
        [
            PY,
            SCRIPTS_DIR / "load_sog_predictions_denali.py",
            "--pred-csv",   str(calibrated_pred_path),
            "--project",    "nhl",
            "--prop-type",  "shots_on_goal",
            "--slate-date", slate,
        ]
    )
    # 6) Score + load Saves
    saves_model_dir = MODELS_DIR / "latest" / "goalie_saves"
    if saves_model_dir.exists():
        run(
            [
                PY,
                SCRIPTS_DIR / "score_nhl_props.py",
                "--model-dir",    saves_model_dir,
                "--csv",          EXPORTS_DIR / "train_goalie_saves_v2.csv",
                "--feature-json", "backend/nhl/features/feature_metadata_nhl.json",
                "--feature-key",  "goalie_saves",
                "--line",         "18.5,19.5,20.5,21.5,22.5,23.5,24.5,25.5,26.5,27.5,28.5,29.5,30.5",
                "--out",          PROC_DIR / "saves_predictions.csv",
            ]
        )
        saves_pred_csv = PROC_DIR / "saves_predictions.csv"
        if saves_pred_csv.exists():
            run(
            [
                PY,
                SCRIPTS_DIR / "load_nhl_predictions_generic.py",
                "--pred-csv", str(saves_pred_csv),
                "--project",  "nhl",
                "--prop",     "goalie_saves",
                "--model-family", "phoenix",
                "--model-version", "phoenix_v2",
                "--feature-hash", "phoenix_v2",
            ]
        )

        else:
            print(f"⚠️ saves_predictions.csv not found at {saves_pred_csv} — skipping saves load.")
    else:
        print(f"⚠️  No saves models at {saves_model_dir} — skipping saves scoring.")

    # 7) Score points (Phoenix) + load
    run(
        [
            PY,
            SCRIPTS_DIR / "score_points_phoenix.py",
            "--features-csv", EXPORTS_DIR / "train_nhl_points_v2.csv",
            "--model-root",   MODELS_DIR / "latest" / "points",
            "--out",          PROC_DIR / "points_predictions.csv",
        ]
    )

    points_pred_csv = PROC_DIR / "points_predictions.csv"
    if points_pred_csv.exists():
        run(
        [
            PY,
            SCRIPTS_DIR / "load_nhl_predictions_generic.py",
            "--pred-csv", str(points_pred_csv),
            "--project",  "nhl",
            "--prop",     "player_points",
            "--model-family", "phoenix",
            "--model-version", "phoenix_v2",
            "--feature-hash", "phoenix_v2",
        ]
    )

    else:
        print(f"⚠️ points predictions CSV not found at {points_pred_csv} — skipping points load.")

    # 8) Odds
    if with_odds:
        fetch_odds()

    # 9) Build site CSVs
    build_sog(slate)
    build_saves(slate)
    build_points(slate)

    # 9b) SOG integrity report (warn-only, except guard-fatal)
    try:
        run(
            [
                PY,
                SCRIPTS_DIR / "sog_integrity_report.py",
                "--slate-date", slate,
                "--feature-key", "shots_on_goal_denali",
                "--db-toi-check",
                "--db-toi-source", "nhl.skater_game_logs_raw",
                "--db-toi-days-back", "30",
            ]
        )
    except SystemExit as e:
        # Preserve guard semantics: die(..., code=2) should stop the pipeline
        if getattr(e, "code", None) == 2:
            raise
        print(f"⚠️ sog_integrity_report exited (continuing): {e}")
    except Exception as e:
        # Non-guard failures remain warn-only
        print(f"⚠️ sog_integrity_report failed (continuing): {e}")

    # 11) Sanity counts (for slate games)
    sanity = f"""
    WITH g AS (SELECT game_id FROM nhl.games WHERE game_date = DATE '{slate}')
    SELECT 'games_today'            AS which, COUNT(*) FROM nhl.games         WHERE game_date = DATE '{slate}'
    UNION ALL SELECT 'roster_rows_today', COUNT(*) FROM nhl.roster_status r   WHERE r.game_id IN (SELECT game_id FROM g)
    UNION ALL SELECT 'preds_sog',         COUNT(*) FROM nhl.predictions p     WHERE p.game_id IN (SELECT game_id FROM g) AND p.prop = 'shots_on_goal'
    UNION ALL SELECT 'preds_saves',       COUNT(*) FROM nhl.predictions p     WHERE p.game_id IN (SELECT game_id FROM g) AND p.prop = 'goalie_saves'
    UNION ALL SELECT 'preds_points',      COUNT(*) FROM nhl.predictions p     WHERE p.game_id IN (SELECT game_id FROM g) AND p.prop = 'player_points'
    UNION ALL SELECT 'predictions_total', COUNT(*) FROM nhl.predictions p     WHERE p.game_id IN (SELECT game_id FROM g);
    """
    run(["psql", db, "-v", "ON_ERROR_STOP=1", "-c", sanity])

    print("\n✅ Daily pipeline complete. Site data in nhl/site/data/.")

# ---------- entrypoint ----------

def main():
    ap = argparse.ArgumentParser(prog="nhl-cli", description="NHL pipelines")
    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("daily", help="Run full daily pipeline")
    d.add_argument("--with-odds", action="store_true", help="Fetch odds inline")

    fo = sub.add_parser("fetch-odds", help="Fetch odds JSON into nhl/site/data")
    fo.add_argument("--days-from", type=int, default=1)

    rr = sub.add_parser("refresh-rosters-all", help="Refresh NHL players/rosters for all teams")
    rr.add_argument("--date", default=os.environ.get("SLATE_DATE") or et_today(), help="YYYY-MM-DD ET context date")

    bsog = sub.add_parser("build-sog", help="Build sog_with_market.csv")
    bsog.add_argument("--slate", default=os.environ.get("SLATE_DATE") or et_today())

    bsv = sub.add_parser("build-saves", help="Build saves_with_market.csv")
    bsv.add_argument("--slate", default=os.environ.get("SLATE_DATE") or et_today())

    bpts = sub.add_parser("build-points", help="Build points_with_market.csv")
    bpts.add_argument("--slate", default=os.environ.get("SLATE_DATE") or et_today())

    g = sub.add_parser("guard", help="List/clear one-off guardrail steps")
    gsub = g.add_subparsers(dest="guard_cmd", required=True)

    gl = gsub.add_parser("list", help="List recorded guard steps")
    gl.add_argument("--slate", default=None, help="Optional slate YYYY-MM-DD; filters to slate + global")

    gc = gsub.add_parser("clear", help="Clear a recorded guard step")
    gc.add_argument("key", help="Guard key to clear (e.g., fix_psql_stdout_bytes_vs_str)")
    gc.add_argument("--slate", default="global", help="Slate to clear (default: global)")

    args = ap.parse_args()

    if args.cmd == "daily":
        cmd_daily(with_odds=args.with_odds)
    elif args.cmd == "fetch-odds":
        fetch_odds(days_from=args.days_from)
    elif args.cmd == "refresh-rosters-all":
        run([PY, SCRIPTS_DIR / "refresh_all_team_rosters.py"], env={"SLATE_DATE": args.date})
    elif args.cmd == "guard":
        if args.guard_cmd == "list":
            guard_print(args.slate)
        elif args.guard_cmd == "clear":
            guard_clear(args.key, args.slate)
            print(f"[guard] cleared: {args.key} (slate={args.slate})")
        else:
            ap.print_help()
    elif args.cmd == "build-sog":
        guard_testing_only_slate(args.slate, cmd_name="build-sog")
        build_sog(args.slate)
    elif args.cmd == "build-saves":
        guard_testing_only_slate(args.slate, cmd_name="build-saves")
        build_saves(args.slate)
    elif args.cmd == "build-points":
        guard_testing_only_slate(args.slate, cmd_name="build-points")
        build_points(args.slate)
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
