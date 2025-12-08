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
from datetime import datetime, timedelta, timezone
import psycopg2  # or psycopg2-binary, whichever you're using
import requests

# ---------- bootstrap env ----------

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

ROOT        = Path(__file__).resolve().parents[2]  # repo root
PY          = os.environ.get("PYTHON", sys.executable)

SITE_DIR    = ROOT / "nhl" / "site" / "data"
EXPORTS_DIR = ROOT / "exports"
PROC_DIR    = ROOT / "backend" / "nhl" / "data" / "processed"
SQL_DIR     = ROOT / "backend" / "nhl" / "sql"
SCRIPTS_DIR = ROOT / "backend" / "nhl" / "scripts"
MODELS_DIR  = ROOT / "backend" / "nhl" / "models"

for d in (SITE_DIR, EXPORTS_DIR, PROC_DIR):
    d.mkdir(parents=True, exist_ok=True)

PY = sys.executable
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"
EXPORTS_DIR = BASE_DIR / "exports"
MODELS_DIR = BASE_DIR / "models"
PROC_DIR = BASE_DIR / "data" / "processed"

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

# ---------- shell helpers ----------

def run(cmd, *, cwd: Path = ROOT, env: dict | None = None, check: bool = True):
    cmd = [str(c) for c in cmd]
    print("▶", " ".join(cmd))
    e = os.environ.copy()
    if env:
        e.update(env)
    return sp.run(cmd, cwd=str(cwd), env=e, check=check)

def require_db_url() -> str:
    db = os.environ.get("SUPABASE_DB_URL")
    if not db:
        print("FATAL: SUPABASE_DB_URL missing", file=sys.stderr)
        sys.exit(2)
    return db

def run_psql_file(sql_file: Path, *, vars: dict[str, str] | None = None):
    db = require_db_url()
    cmd = ["psql", "--no-psqlrc", "-v", "ON_ERROR_STOP=1", db]
    if vars:
        for k, v in vars.items():
            cmd += ["-v", f"{k}={v}"]
    cmd += ["-f", str(sql_file)]
    run(cmd)

def psql_stdout(sql_file: Path, *, vars: dict[str, str] | None = None) -> bytes:
    """Run psql on a file that COPY/SELECTs TO STDOUT and return stdout bytes."""
    db = require_db_url()
    cmd = ["psql", "--no-psqlrc", "-q", "-v", "ON_ERROR_STOP=1", db]
    if vars:
        for k, v in vars.items():
            cmd += ["-v", f"{k}={v}"]
    cmd += ["-f", str(sql_file)]
    res = sp.run(cmd, cwd=str(ROOT), env=os.environ, check=True, capture_output=True)
    return res.stdout

def safe_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2))


def export_sog_denali_features(db_url: str, slate_date: str, out_path: Path) -> None:
    """
    Export Denali SOG features for a given slate_date into a CSV used by score_sog_denali.py.

    New behavior (wired to SQL script):
      - Runs backend/nhl/sql/export_sog_denali_pregame.sql via psql.
      - Passes slate_date as a psql variable: -v slate_date=YYYY-MM-DD
      - Script itself does COPY ... TO STDOUT WITH CSV HEADER.
    """
    # Resolve SQL file relative to this cli.py
    BASE = Path(__file__).resolve().parent  # backend/nhl
    sql_path = BASE / "sql" / "export_sog_denali_pregame.sql"

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Let psql do COPY TO STDOUT → out_path
    with out_path.open("w", encoding="utf-8", newline="") as f:
        subprocess.run(
            [
                "psql",
                db_url,
                "-v",
                "ON_ERROR_STOP=1",
                "-v",
                f"slate_date={slate_date}",
                "-f",
                str(sql_path),
            ],
            check=True,
            stdout=f,
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
    out_path = EXPORTS_DIR / f"names_{slate}.csv"

    # Base directory for this NHL backend module (backend/nhl)
    nhl_base = Path(__file__).resolve().parent
    sql_path = nhl_base / "sql" / "_export_names.sql"

    # Run the static SQL with a bound slate_date variable and capture CSV bytes
    csv_bytes = psql_stdout(sql_path, vars={"slate_date": slate})
    out_path.write_bytes(csv_bytes)

    print(f"[export_names_csv] wrote names CSV → {out_path}")
    return out_path

# ---------- odds fetch ----------

def fetch_odds(
    days_from: int = 1,
    markets: str = "player_shots_on_goal,player_total_saves,player_points",
    regions: str = "us",
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
    export_names_csv(slate)
    names_csv = EXPORTS_DIR / f"names_{slate}.csv"
    pred_path = PROC_DIR / "sog_predictions.csv"
    run(
        [
            PY,
            SCRIPTS_DIR / "build_sog_with_market.py",
            "--pred", pred_path,
            "--names", names_csv,
            "--odds-json", SITE_DIR / "odds_latest.json",
            "--out", SITE_DIR / "sog_with_market.csv",
            "--unmatched", SITE_DIR / "unmatched_sog.csv",
        ],
        env={"SLATE_DATE": slate},
    )

def build_saves(slate: str):
    names_csv = EXPORTS_DIR / f"names_{slate}.csv"
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

    names_path = EXPORTS_DIR / f"names_{slate}.csv"
    if names_path.exists():
        args += ["--names", names_path]

    run(args)

# ---------- daily pipeline ----------

def cmd_daily(with_odds: bool):
    db = require_db_url()

    slate = os.environ.get("SLATE_DATE") or et_today()
    yday  = os.environ.get("YDAY")       or et_yesterday()
    os.environ["SLATE_DATE"] = slate
    os.environ["YDAY"] = yday

    print(f"SLATE_DATE (ET): {slate}")
    print(f"YDAY       (ET): {yday}")

    # 0 DB sanity
    run(["psql", db, "-v", "ON_ERROR_STOP=1", "-c", "SELECT now();"])

    # 1 Today: schedule & roster
    run([PY, SCRIPTS_DIR / "import_schedule_today.py"], env={"SLATE_DATE": slate})
    run(
        [PY, SCRIPTS_DIR / "import_roster_today.py"],
        env={"SLATE_DATE": slate, "SKIP_ROSTER_STATUS": "1", "SKIP_PLAYERS": "1"},
    )
    run(
        [PY, SCRIPTS_DIR / "refresh_players_and_roster_today.py"],
        env={"SLATE_DATE": slate},
    )

    # 2 Seed features for today (SOG + Saves). Points features SQL can be added here when ready.
    run_psql_file(SQL_DIR / "seed_sog_features_for_slate.sql",   vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "seed_goalie_features_for_slate.sql", vars={"slate_date": slate})

    # 3 Export names (used by all builders)
    try:
        export_names_csv(slate)
    except Exception as e:
        print(f"⚠️ names export failed; downstream builders will fall back if possible: {e}")

    # 4 Export feature CSVs for this slate

    # 4a) SOG (Denali view → slate-only features)
    sog_feat_path = EXPORTS_DIR / f"sog_features_{slate}_denali.csv"
    export_sog_denali_features(db, slate, sog_feat_path)

    # 4b) Saves / Points via existing SQL exporters
    saves_csv  = psql_stdout(SQL_DIR / "export_saves_from_denali.sql", vars={"slate_date": slate})
    points_csv = psql_stdout(SQL_DIR / "export_points.sql",           vars={"slate_date": slate})

    (EXPORTS_DIR / "train_goalie_saves_v2.csv").write_bytes(saves_csv)
    (EXPORTS_DIR / "train_nhl_points_v2.csv").write_bytes(points_csv)

    print("exports → sog_features_{slate}_denali.csv, train_goalie_saves_v2.csv, train_nhl_points_v2.csv")

    # 5) Score SOG (Denali LR+RF models under backend/nhl/models/latest/shots_on_goal/sog_player_denali)
    sog_model_root = MODELS_DIR / "latest" / "shots_on_goal" / "sog_player_denali"
    if not sog_model_root.exists():
        raise SystemExit(f"Missing SOG models at {sog_model_root}; train sog_player_denali first.")

    # Score each line separately, then merge into a single sog_predictions.csv
    line_list = [0.5, 1.5, 2.5, 3.5]
    per_line_paths: list[tuple[float, Path]] = []

    for ln in line_list:
        suffix = str(ln).replace(".", "_")
        out_csv = PROC_DIR / f"sog_predictions_{suffix}.csv"
        per_line_paths.append((ln, out_csv))

        run(
            [
                PY,
                SCRIPTS_DIR / "score_sog_player_denali.py",
                "--features-csv", str(sog_feat_path),
                "--line",         str(ln),
                "--models-root",  str(sog_model_root),
                "--out-csv",      str(out_csv),
            ]
        )

    # Merge per-line predictions into one sog_predictions.csv (uncalibrated blend)
    base_df = None
    key_cols = ["player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date"]

    for ln, path in per_line_paths:
        df_line = pd.read_csv(path)

        # Keep keys + all prediction columns from this line (p_over_lr_*, p_over_rf_*, p_over_*)
        keep_cols = key_cols + [c for c in df_line.columns if c.startswith("p_over_")]
        df_line = df_line[keep_cols]

        if base_df is None:
            base_df = df_line
        else:
            base_df = base_df.merge(df_line, on=key_cols, how="left")

    final_pred_path = PROC_DIR / "sog_predictions.csv"
    final_pred_path.parent.mkdir(parents=True, exist_ok=True)
    base_df.to_csv(final_pred_path, index=False)
    print(f"✅ Wrote merged SOG predictions → {final_pred_path}")

    # 5b) Calibrate SOG probabilities (Denali-wide calibration)
    calib_train_path = PROC_DIR / "sog_calibration_training_denali.csv"
    calibrated_pred_path = PROC_DIR / "sog_predictions_wide_calibrated.csv"

    run(
        [
            PY,
            SCRIPTS_DIR / "calibrate_sog_denali.py",
            "--train",    str(calib_train_path),
            "--wide-in",  str(final_pred_path),
            "--wide-out", str(calibrated_pred_path),
        ]
    )

    # 5c) Load *calibrated* SOG predictions into nhl.predictions (Denali)
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

    # 6) Score Saves using generic scorer + latest goalie_saves models
    saves_model_dir = MODELS_DIR / "latest" / "goalie_saves"
    if saves_model_dir.exists():
        run(
            [
                PY,
                SCRIPTS_DIR / "score_nhl_props.py",
                "--model-dir",   saves_model_dir,
                "--csv",         EXPORTS_DIR / "train_goalie_saves_v2.csv",
                "--feature-json","backend/nhl/features/feature_metadata_nhl.json",
                "--feature-key", "goalie_saves",
                "--line",        "18.5,19.5,20.5,21.5,22.5,23.5,24.5,25.5,26.5,27.5,28.5,29.5,30.5",
                "--out",         PROC_DIR / "saves_predictions.csv",
            ]
        )
    else:
        print(f"⚠️  No saves models at {saves_model_dir} — skipping saves scoring.")

    # 7) Phoenix points scoring (raw model probabilities)
    run(
        [
            PY,
            SCRIPTS_DIR / "score_points_phoenix.py",
            "--features-csv",
            EXPORTS_DIR / "train_nhl_points_v2.csv",
            "--model-root",
            MODELS_DIR / "latest" / "points",
            "--out",
            PROC_DIR / "points_predictions.csv",
        ]
    )

    # 7b) Calibrate Phoenix points to sane probabilities by line
    #     This will overwrite backend/nhl/data/processed/points_predictions.csv
    #     with calibrated probabilities (prob_over), while preserving
    #     prob_over_raw and prob_over_calibrated columns for inspection.
    run(
        [
            PY,
            SCRIPTS_DIR / "calibrate_points_phoenix.py",
        ]
    )

    # 8) Odds
    if with_odds:
        fetch_odds()

    # 9) Build site CSVs (points now see calibrated prob_over)
    build_sog(slate)
    build_saves(slate)
    build_points(slate)
    
    # 10) Yesterday logs → promote to raw
    run([PY, SCRIPTS_DIR / "seed_goalie_logs_for_date.py"],        env={"SLATE_DATE": yday})
    run([PY, SCRIPTS_DIR / "refresh_players_and_roster_today.py"], env={"SLATE_DATE": yday})
    run([PY, SCRIPTS_DIR / "seed_skater_logs_for_date.py"],        env={"SLATE_DATE": yday})

    promote_sql = f"""
    WITH src AS (
      SELECT DISTINCT
        s.player_id, s.game_id, s.game_date,
        s.shots_on_goal, s.shot_attempts, s.toi_minutes, s.pp_toi_minutes
      FROM nhl.import_skater_logs_stage s
      WHERE s.game_date = DATE '{yday}'
    ),
    rs AS (
      SELECT DISTINCT game_id, team_id, player_id
      FROM nhl.roster_status
      WHERE game_id IN (SELECT game_id FROM nhl.games WHERE game_date = DATE '{yday}')
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
      shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes
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
      pp_toi_minutes = EXCLUDED.pp_toi_minutes;
    """
    run(["psql", db, "-v", "ON_ERROR_STOP=1", "-c", promote_sql])

    # 11) Refresh views/materializations + sanity counts
    refresh_sql = SCRIPTS_DIR / "refresh.sql"
    if refresh_sql.exists():
        run(["psql", db, "-v", "ON_ERROR_STOP=1", "-f", refresh_sql])

    sanity = f"""
      WITH g AS (SELECT game_id FROM nhl.games WHERE game_date = DATE '{slate}')
      SELECT 'games_today'            AS which, COUNT(*) FROM nhl.games                 WHERE game_date = DATE '{slate}'
      UNION ALL SELECT 'roster_rows_today', COUNT(*) FROM nhl.roster_status r           WHERE r.game_id IN (SELECT game_id FROM g)
      UNION ALL SELECT 'sog_stage',         COUNT(*) FROM nhl.predictions_sog_stage s   WHERE s.game_id IN (SELECT game_id FROM g)
      UNION ALL SELECT 'saves_stage',       COUNT(*) FROM nhl.predictions_saves_stage s WHERE s.game_id IN (SELECT game_id FROM g)
      UNION ALL SELECT 'predictions',       COUNT(*) FROM nhl.predictions p             WHERE p.game_id IN (SELECT game_id FROM g);
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

    bsog = sub.add_parser("build-sog", help="Build sog_with_market.csv")
    bsog.add_argument("--slate", default=os.environ.get("SLATE_DATE") or et_today())

    bsv = sub.add_parser("build-saves", help="Build saves_with_market.csv")
    bsv.add_argument("--slate", default=os.environ.get("SLATE_DATE") or et_today())

    bpts = sub.add_parser("build-points", help="Build points_with_market.csv")
    bpts.add_argument("--slate", default=os.environ.get("SLATE_DATE") or et_today())

    args = ap.parse_args()

    if args.cmd == "daily":
        cmd_daily(with_odds=args.with_odds)
    elif args.cmd == "fetch-odds":
        fetch_odds(days_from=args.days_from)
    elif args.cmd == "build-sog":
        build_sog(args.slate)
    elif args.cmd == "build-saves":
        build_saves(args.slate)
    elif args.cmd == "build-points":
        build_points(args.slate)
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
