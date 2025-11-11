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
import subprocess as sp
from pathlib import Path
from datetime import datetime, timedelta, timezone

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
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))

# ---------- names export ----------

def export_names_csv(slate: str) -> Path:
    """
    exports/names_{slate}.csv:
      full_name, player_id, team_id
    from nhl.roster_status + nhl.players for that game_date.
    """
    out_path = EXPORTS_DIR / f"names_{slate}.csv"
    tmp_sql = EXPORTS_DIR / "_export_names.sql"
    tmp_sql.write_text(f"""
    COPY (
      SELECT DISTINCT
        COALESCE(NULLIF(btrim(p.full_name), ''), 'Player '||rs.player_id::text) AS full_name,
        rs.player_id,
        rs.team_id
      FROM nhl.roster_status rs
      JOIN nhl.games g   ON g.game_id = rs.game_id
      JOIN nhl.players p ON p.player_id = rs.player_id
      WHERE g.game_date = DATE '{slate}'
      ORDER BY full_name, rs.player_id
    ) TO STDOUT WITH CSV HEADER;
    """)
    csv_bytes = psql_stdout(tmp_sql)
    out_path.write_bytes(csv_bytes)
    try:
        tmp_sql.unlink()
    except Exception:
        pass
    print(f"[cli] names CSV → {out_path}")
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
    if not key:
        print("⚠️  ODDS_API_KEY not set — writing empty odds files.")
        safe_json([], out_today)
        try:
            out_latest.write_text(out_today.read_text())
        except Exception:
            pass
        return

    base = "https://api.the-odds-api.com/v4/sports/icehockey_nhl"

    # Events
    ev_url = f"{base}/events?dateFormat=iso&daysFrom={days_from}&apiKey={key}"
    print(f"→ Fetching events (daysFrom={days_from}) …")
    r = requests.get(ev_url, timeout=30)
    r.raise_for_status()
    events = r.json()
    (SITE_DIR / "events_today.json").write_text(json.dumps(events))
    print(f"   events_today.json → {len(events)} events")

    # Player props
    print(f"→ Fetching player props (markets={markets}, regions={regions}) …")
    all_event_odds = []
    for ev in events:
        eid = ev.get("id")
        if not eid:
            all_event_odds.append({})
            continue
        url = (
            f"{base}/events/{eid}/odds"
            f"?regions={regions}&markets={markets}&oddsFormat={odds_format}&apiKey={key}"
        )
        ok = False
        for _ in (1, 2, 3):
            try:
                rr = requests.get(url, timeout=30)
                if rr.ok:
                    all_event_odds.append(rr.json())
                    ok = True
                    break
            except Exception:
                pass
        if not ok:
            all_event_odds.append({})

    safe_json(all_event_odds, out_today)
    try:
        out_latest.write_text(out_today.read_text())
    except Exception:
        pass

    total_bm = 0
    for ev_obj in all_event_odds:
        if isinstance(ev_obj, dict) and isinstance(ev_obj.get("bookmakers"), list):
            total_bm += len(ev_obj["bookmakers"])
    print(f"✅ Wrote {out_today} | events={len(events)} | bookmaker_arrays={total_bm}")

# ---------- builders (CSV for site) ----------

def build_sog(slate: str):
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

    # 0) DB sanity
    run(["psql", db, "-v", "ON_ERROR_STOP=1", "-c", "SELECT now();"])

    # 1) Today: schedule & roster
    run([PY, SCRIPTS_DIR / "import_schedule_today.py"], env={"SLATE_DATE": slate})
    run(
        [PY, SCRIPTS_DIR / "import_roster_today.py"],
        env={"SLATE_DATE": slate, "SKIP_ROSTER_STATUS": "1", "SKIP_PLAYERS": "1"},
    )
    run(
        [PY, SCRIPTS_DIR / "refresh_players_and_roster_today.py"],
        env={"SLATE_DATE": slate},
    )

    # 2) Seed features for today (SOG + Saves). Points features SQL can be added here when ready.
    run_psql_file(SQL_DIR / "seed_sog_features_for_slate.sql",   vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "seed_goalie_features_for_slate.sql", vars={"slate_date": slate})

    # 3) Export names (used by all builders)
    try:
        export_names_csv(slate)
    except Exception as e:
        print(f"⚠️ names export failed; downstream builders will fall back if possible: {e}")

    # 4) Export feature CSVs for this slate
    sog_csv    = psql_stdout(SQL_DIR / "export_sog.sql",    vars={"slate_date": slate})
    saves_csv  = psql_stdout(SQL_DIR / "export_saves.sql",  vars={"slate_date": slate})
    points_csv = psql_stdout(SQL_DIR / "export_points.sql", vars={"slate_date": slate})

    (EXPORTS_DIR / "train_nhl_sog_v2.csv").write_bytes(sog_csv)
    (EXPORTS_DIR / "train_goalie_saves_v2.csv").write_bytes(saves_csv)
    (EXPORTS_DIR / "train_nhl_points_v2.csv").write_bytes(points_csv)

    print("exports → train_nhl_sog_v2.csv, train_goalie_saves_v2.csv, train_nhl_points_v2.csv")

    # 5) Score SOG (phoenix logistic models under backend/nhl/models/sog)
    run(
        [
            PY,
            SCRIPTS_DIR / "score_sog_phoenix.py",
            "--features-csv", EXPORTS_DIR / "train_nhl_sog_v2.csv",
            "--model-root",   MODELS_DIR / "sog",
            "--out",          PROC_DIR / "sog_predictions.csv",
        ]
    )

    # 6) Score Saves (keep existing pipeline / models under backend/nhl/models/saves)
    # Adjust this call to whatever your current goalie-saves scorer is.
    saves_model_dir = MODELS_DIR / "saves"
    if saves_model_dir.exists():
        run(
            [
                PY,
                SCRIPTS_DIR / "score_goalie_saves.py",
                "--features-csv", EXPORTS_DIR / "train_goalie_saves_v2.csv",
                "--model-root",   saves_model_dir,
                "--out",          PROC_DIR / "saves_predictions.csv",
            ]
        )
    else:
        print("⚠️  No saves models at backend/nhl/models/saves — skipping saves scoring.")

    # 7) Score Points (phoenix logistic models under backend/nhl/models/points)
    points_model_root = MODELS_DIR / "points"
    if points_model_root.exists():
        run(
            [
                PY,
                SCRIPTS_DIR / "score_points_phoenix.py",
                "--features-csv", EXPORTS_DIR / "train_nhl_points_v2.csv",
                "--model-root",   points_model_root,
                "--out",          PROC_DIR / "points_predictions.csv",
            ]
        )
    else:
        print("⚠️  No points models at backend/nhl/models/points — skipping points scoring (odds-only points file).")

    # 8) Odds
    if with_odds:
        fetch_odds()

    # 9) Build site CSVs
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
