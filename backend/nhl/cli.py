#!/usr/bin/env python3
"""
NHL pipeline CLI (Python is the boss).

Commands:
  daily        Run the full daily pipeline (optionally fetch odds).
  fetch-odds   Fetch NHL player props odds (SOG + Saves + Points) to site/data JSON.
  build-sog    Build nhl/site/data/sog_with_market.csv.
  build-saves  Build nhl/site/data/saves_with_market.csv.
  build-points Build nhl/site/data/points_with_market.csv.

Conventions:
  - All dates are Eastern Time (ET).
  - Artifacts:
      exports/                             (SQL exports consumed by models)
      backend/nhl/data/processed/          (model outputs)
      nhl/site/data/                       (site-consumed CSV/JSON)
"""

from __future__ import annotations
import argparse
import os
import sys
import json
import requests
import subprocess as sp
from pathlib import Path
from datetime import datetime, timedelta, timezone
from glob import glob

# --- Load .env early (supports multiple locations; CI/env vars still win) ---
def _load_dotenv_multi():
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    here = Path(__file__).resolve()
    root = here.parents[2]  # repo root
    for p in (root / ".env.local", root / ".env", root / "backend" / ".env", root / "nhl" / ".env"):
        if p.exists():
            load_dotenv(p, override=False)

_load_dotenv_multi()

ROOT = Path(__file__).resolve().parents[2]  # repo root
PY = os.environ.get("PYTHON", sys.executable)

SITE_DIR    = ROOT / "nhl" / "site" / "data"
EXPORTS_DIR = ROOT / "exports"
PROC_DIR    = ROOT / "backend" / "nhl" / "data" / "processed"
SQL_DIR     = ROOT / "backend" / "nhl" / "sql"
SCRIPTS_DIR = ROOT / "backend" / "nhl" / "scripts"

SITE_DIR.mkdir(parents=True, exist_ok=True)
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

def et_today() -> str:
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except Exception:
        et = timezone(timedelta(hours=-5))  # best-effort
    return datetime.now(et).strftime("%Y-%m-%d")

def et_yesterday() -> str:
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        return (datetime.now(et) - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        return (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

def run(cmd: list[str], *, cwd: Path = ROOT, env: dict | None = None, check=True):
    print("▶", " ".join(map(str, cmd)))
    e = os.environ.copy()
    if env:
        e.update(env)
    return sp.run(cmd, cwd=str(cwd), env=e, capture_output=False, check=check)

def run_psql_file(sql_file: Path, *, vars: dict[str, str] | None = None):
    db = os.environ.get("SUPABASE_DB_URL")
    if not db:
        print("FATAL: SUPABASE_DB_URL missing", file=sys.stderr)
        sys.exit(2)
    cmd = ["psql", "--no-psqlrc", "-v", "ON_ERROR_STOP=1", db]
    if vars:
        for k, v in vars.items():
            cmd += ["-v", f"{k}={v}"]
    cmd += ["-f", str(sql_file)]
    run(cmd)

def psql_stdout(sql_file: Path, *, vars: dict[str, str] | None = None) -> bytes:
    """Run psql and return STDOUT (for SQL that \\copy TO STDOUT)."""
    db = os.environ.get("SUPABASE_DB_URL")
    if not db:
        print("FATAL: SUPABASE_DB_URL missing", file=sys.stderr)
        sys.exit(2)
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

# --- Names export for the slate (DB-backed) ---
def export_names_csv(slate: str) -> Path:
    """
    Writes exports/names_{slate}.csv with columns:
      full_name, player_id, team_id
    Sourced from nhl.players + nhl.roster_status for games on the slate date.
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
    ) TO STDOUT WITH CSV HEADER
    """)
    csv_bytes = psql_stdout(tmp_sql)
    out_path.write_bytes(csv_bytes)
    try:
        tmp_sql.unlink()
    except Exception:
        pass
    print(f"[cli] names CSV → {out_path}")
    return out_path

# ---------------- odds (requests) ----------------
def fetch_odds(days_from: int = 1,
               markets: str = "player_shots_on_goal,player_total_saves,player_points",
               regions: str = "us",
               odds_format: str = "american",
               out_latest: Path = SITE_DIR / "odds_latest.json",
               out_today: Path = SITE_DIR / "odds_nhl_playerprops_today.json"):
    """
    Fetch events and per-event odds for the requested markets.
    Default markets include player_points.
    """
    key = os.environ.get("ODDS_API_KEY", "").strip()
    if not key:
        print("⚠️  ODDS_API_KEY not set — writing empty odds files.")
        safe_json([], out_today); out_latest.write_text(out_today.read_text())
        return

    base = "https://api.the-odds-api.com/v4/sports/icehockey_nhl"
    ev_url = f"{base}/events?dateFormat=iso&daysFrom={days_from}&apiKey={key}"
    print(f"→ Fetching events (daysFrom={days_from}) …")
    r = requests.get(ev_url, timeout=30)
    r.raise_for_status()
    events = r.json()
    (SITE_DIR / "events_today.json").write_text(json.dumps(events))
    print(f"   events_today.json → {len(events)} events")

    all_event_odds = []
    print(f"→ Fetching player props (markets={markets}, regions={regions}) …")
    for ev in events:
        eid = ev.get("id")
        if not eid:
            all_event_odds.append({})
            continue
        url = (f"{base}/events/{eid}/odds?regions={regions}&markets={markets}"
               f"&oddsFormat={odds_format}&apiKey={key}")
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
    try:
        for ev_obj in all_event_odds:
            if isinstance(ev_obj, dict) and isinstance(ev_obj.get("bookmakers"), list):
                total_bm += len(ev_obj["bookmakers"])
    except Exception:
        pass
    print(f"✅ Wrote {out_today}  | events: {len(events)} | bookmaker arrays total: {total_bm}")

# ---------------- builders ----------------
def build_sog(slate: str):
    # Merge predictions + names + odds (names come from DB export)
    names_csv = EXPORTS_DIR / f"names_{slate}.csv"
    run([PY, str(SCRIPTS_DIR / "build_sog_with_market.py"),
         "--pred",       str(PROC_DIR / "sog_predictions.csv"),
         "--names",      str(names_csv),
         "--odds-json",  str(SITE_DIR / "odds_latest.json"),
         "--out",        str(SITE_DIR / "sog_with_market.csv"),
         "--unmatched",  str(SITE_DIR / "unmatched_sog.csv")],
        env={"SLATE_DATE": slate})

def build_saves(slate: str):
    names_csv = EXPORTS_DIR / f"names_{slate}.csv"
    run([PY, str(SCRIPTS_DIR / "build_saves_with_market.py"),
         "--pred",       str(PROC_DIR / "saves_predictions.csv"),
         "--names",      str(names_csv),
         "--odds-json",  str(SITE_DIR / "odds_latest.json"),
         "--out",        str(SITE_DIR / "saves_with_market.csv"),
         "--unmatched",  str(SITE_DIR / "unmatched_saves.csv")],
        env={"SLATE_DATE": slate})

def build_points(slate: str):
    """
    Points builder supports two modes automatically:
      - Odds-only (no preds/names) → minimal CSV for UI.
      - Full merge when both preds + names exist → adds edge/pricing columns.
    """
    args = [
        PY, str(SCRIPTS_DIR / "build_points_with_market.py"),
        "--odds-json",   str(SITE_DIR / "odds_latest.json"),
        "--events-json", str(SITE_DIR / "events_today.json"),
        "--out",         str(SITE_DIR / "points_with_market.csv"),
        "--unmatched",   str(SITE_DIR / "unmatched_points.csv"),
    ]

    pred_path  = PROC_DIR / "points_predictions.csv"
    if pred_path.exists():
        args += ["--pred", str(pred_path)]

    names_path = EXPORTS_DIR / f"names_{slate}.csv"
    if names_path.exists():
        args += ["--names", str(names_path)]

    run(args)

# ---------------- daily pipeline ----------------
def cmd_daily(with_odds: bool):
    slate = os.environ.get("SLATE_DATE") or et_today()
    yday  = os.environ.get("YDAY")       or et_yesterday()
    os.environ["SLATE_DATE"] = slate
    os.environ["YDAY"] = yday
    print(f"SLATE_DATE (ET): {slate}    YDAY (ET): {yday}")

    # 0) DB sanity
    run(["psql", os.environ["SUPABASE_DB_URL"], "-v", "ON_ERROR_STOP=1", "-c", "select now();"])

    # 1) today: schedule & roster
    run([PY, str(SCRIPTS_DIR / "import_schedule_today.py")], env={"SLATE_DATE": slate})
    run([PY, str(SCRIPTS_DIR / "import_roster_today.py")],
        env={"SLATE_DATE": slate, "SKIP_ROSTER_STATUS": "1", "SKIP_PLAYERS": "1"})
    run([PY, str(SCRIPTS_DIR / "refresh_players_and_roster_today.py")],
        env={"SLATE_DATE": slate})

    # Points ingest (best-effort: generates stage CSV and/or seeds DB if present)
    run([PY, str(SCRIPTS_DIR / "ingest_points_from_boxscores.py")], env={"SLATE_DATE": slate})
    points_stage_csv = EXPORTS_DIR / f"points_stage_{slate}.csv"
    if points_stage_csv.exists():
        run(["psql", os.environ["SUPABASE_DB_URL"], "-v", "ON_ERROR_STOP=1",
             "-v", f"slate_date={slate}",
             "-v", f"csv_path={points_stage_csv}",
             "-f", str(SQL_DIR / "seed_points_from_csv.sql")])

    # 2) features (view-independent seeding)
    run_psql_file(SQL_DIR / "seed_sog_features_for_slate.sql",   vars={"slate_date": slate})
    run_psql_file(SQL_DIR / "seed_goalie_features_for_slate.sql", vars={"slate_date": slate})
    # (When points feature seeding SQL exists, call it here.)

    # Export names (used by all builders)
    try:
        export_names_csv(slate)
    except Exception as e:
        print(f"⚠️ names export failed (builders will fall back to odds-only merges if needed): {e}")

    # 3) exports (CSV)
    sog_csv   = psql_stdout(SQL_DIR / "export_sog.sql",    vars={"slate_date": slate})
    saves_csv = psql_stdout(SQL_DIR / "export_saves.sql",  vars={"slate_date": slate})
    points_csv= psql_stdout(SQL_DIR / "export_points.sql", vars={"slate_date": slate})

    (EXPORTS_DIR / "train_nhl_sog_v2.csv").write_bytes(sog_csv)
    (EXPORTS_DIR / "train_goalie_saves_v2.csv").write_bytes(saves_csv)
    (EXPORTS_DIR / "train_nhl_points_v2.csv").write_bytes(points_csv)
    print("   export_* → exports/train_*.csv (sog, saves, points)")

    # 4) scoring (SOG/Saves via unified scorer; Points optional Poisson scorer if model present)
    run([PY, str(SCRIPTS_DIR / "run_daily_slate.py"),
         "--project", "nhl",
         "--sog-csv",   str(EXPORTS_DIR / "train_nhl_sog_v2.csv"),
         "--saves-csv", str(EXPORTS_DIR / "train_goalie_saves_v2.csv"),
         "--db-url", os.environ["SUPABASE_DB_URL"],
         "--scorer", str(SCRIPTS_DIR / "score_nhl_props.py")])

    points_model_dirs = sorted(glob(str(ROOT / "models_out" / "nhl" / "points" / "v*")))
    if points_model_dirs:
        mdl_dir = points_model_dirs[-1]
        run([PY, str(SCRIPTS_DIR / "score_points_poisson.py"),
             "--features-csv", str(EXPORTS_DIR / "train_nhl_points_v2.csv"),
             "--model-dir",    mdl_dir,
             "--odds-json",    str(SITE_DIR / "odds_nhl_playerprops_today.json"),
             "--out",          str(PROC_DIR / "points_predictions.csv")])
    else:
        print("⚠️  No points model found under models_out/nhl/points/. Skipping points scoring (odds-only site CSV).")

    # 5) odds (optional but recommended)
    if with_odds:
        fetch_odds()

    # 6) site CSVs
    build_sog(slate)
    build_saves(slate)
    build_points(slate)

    # 7) yesterday logs (goalies + skaters), then promote to raw
    run([PY, str(SCRIPTS_DIR / "seed_goalie_logs_for_date.py")],        env={"SLATE_DATE": yday})
    run([PY, str(SCRIPTS_DIR / "refresh_players_and_roster_today.py")], env={"SLATE_DATE": yday})
    run([PY, str(SCRIPTS_DIR / "seed_skater_logs_for_date.py")],        env={"SLATE_DATE": yday})

    promote_sql = f"""
    WITH src AS (
      SELECT DISTINCT s.player_id, s.game_id, s.game_date,
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
    run(["psql", os.environ["SUPABASE_DB_URL"], "-v", "ON_ERROR_STOP=1", "-c", promote_sql])

    # Refresh views/materializations and sanity counts
    run(["psql", os.environ["SUPABASE_DB_URL"], "-v", "ON_ERROR_STOP=1",
         "-f", str(SCRIPTS_DIR / "refresh.sql")])

    sanity = f"""
      WITH g AS (SELECT game_id FROM nhl.games WHERE game_date = DATE '{slate}')
      SELECT 'games_today'            AS which, COUNT(*) FROM nhl.games                 WHERE game_date = DATE '{slate}'
      UNION ALL SELECT 'roster_rows_today', COUNT(*) FROM nhl.roster_status r           WHERE r.game_id IN (SELECT game_id FROM g)
      UNION ALL SELECT 'sog_stage',         COUNT(*) FROM nhl.predictions_sog_stage s   WHERE s.game_id IN (SELECT game_id FROM g)
      UNION ALL SELECT 'saves_stage',       COUNT(*) FROM nhl.predictions_saves_stage s WHERE s.game_id IN (SELECT game_id FROM g)
      UNION ALL SELECT 'predictions',       COUNT(*) FROM nhl.predictions p             WHERE p.game_id IN (SELECT game_id FROM g);
    """
    run(["psql", os.environ["SUPABASE_DB_URL"], "-v", "ON_ERROR_STOP=1", "-c", sanity])
    print("\n✅ Done. Site files under nhl/site/data/. Enjoy the coffee ☕")

# ---------------- argparse ----------------
def main():
    ap = argparse.ArgumentParser(prog="nhl-cli", description="NHL pipelines (Python boss)")
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
