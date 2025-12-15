#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

# ---------------- config ----------------

REPO_ROOT = Path(__file__).resolve().parents[2]
CLI_PATH  = REPO_ROOT / "backend" / "nhl" / "cli.py"

DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or os.environ.get("PGURL")
if not DB_URL:
    raise SystemExit("Missing SUPABASE_DB_URL (or DATABASE_URL/PGURL).")

CHECKPOINT_DEFAULT = REPO_ROOT / "backend" / "nhl" / ".range_runner_checkpoint.txt"

# ---------------- helpers ----------------

def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts}  {msg}", flush=True)

def psql_scalar(sql: str) -> str:
    r = subprocess.run(
        ["psql", DB_URL, "--no-psqlrc", "-v", "ON_ERROR_STOP=1", "-q", "-At", "-c", sql],
        check=True,
        text=True,
        capture_output=True,
    )
    return (r.stdout or "").strip()

def day_has_games(ds: str) -> int:
    return int(psql_scalar(f"SELECT COUNT(*) FROM nhl.games WHERE game_date = DATE '{ds}';") or "0")

@dataclass
class DayCounts:
    games: int
    sk_toi: int
    sk_sog: int
    gk_toi: int

def day_counts(ds: str) -> DayCounts:
    games = int(psql_scalar(f"SELECT COUNT(*) FROM nhl.games WHERE game_date = DATE '{ds}';") or "0")

    # Always key by the set of game_ids on this slate date, NOT logs.game_date
    sk_toi = int(psql_scalar(f"""
        WITH g AS (
          SELECT game_id FROM nhl.games WHERE game_date = DATE '{ds}'
        )
        SELECT COUNT(*)
        FROM nhl.skater_game_logs_raw l
        JOIN g USING (game_id)
        WHERE COALESCE(NULLIF(BTRIM(l.toi_minutes::text), ''), '0')::numeric > 0;
    """) or "0")

    sk_sog = int(psql_scalar(f"""
        WITH g AS (
          SELECT game_id FROM nhl.games WHERE game_date = DATE '{ds}'
        )
        SELECT COUNT(*)
        FROM nhl.skater_game_logs_raw l
        JOIN g USING (game_id)
        WHERE COALESCE(l.shots_on_goal,0) > 0;
    """) or "0")

    gk_toi = int(psql_scalar(f"""
        WITH g AS (
          SELECT game_id FROM nhl.games WHERE game_date = DATE '{ds}'
        )
        SELECT COUNT(*)
        FROM nhl.goalie_game_logs_raw l
        JOIN g USING (game_id)
        WHERE COALESCE(NULLIF(BTRIM(l.toi_minutes::text), ''), '0')::numeric > 0;
    """) or "0")

    return DayCounts(games=games, sk_toi=sk_toi, sk_sog=sk_sog, gk_toi=gk_toi)

def write_checkpoint(path: Path, ds: str) -> None:
    path.write_text(ds + "\n")

def read_checkpoint(path: Path) -> str | None:
    try:
        s = path.read_text().strip()
        return s or None
    except Exception:
        return None

def run_cli_for_day(ds: str, *, attempts: int = 3, sleep_s: int = 15) -> None:
    # Make rollups window deterministic per slate
    rollup_start = (date.fromisoformat(ds) - timedelta(days=260)).isoformat()

    env = os.environ.copy()
    env["SLATE_DATE"] = ds
    env["YDAY"] = ds                    # ✅ CRITICAL: make logs ingestion/promotion match slate
    env["ROLLUP_START_DATE"] = rollup_start

    for i in range(1, attempts + 1):
        log(f"[run] {ds} attempt {i}/{attempts}")
        try:
            subprocess.run([sys.executable, str(CLI_PATH), "daily"], check=True, cwd=str(REPO_ROOT), env=env)
            return
        except subprocess.CalledProcessError as e:
            log(f"[warn] {ds} attempt {i} failed: {type(e).__name__}: {e}")
            if i < attempts:
                time.sleep(sleep_s)
            else:
                raise

# ---------------- main ----------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end",   required=True, help="YYYY-MM-DD")
    ap.add_argument("--resume", default=None, help="YYYY-MM-DD to resume from (overrides checkpoint)")
    ap.add_argument("--checkpoint", default=str(CHECKPOINT_DEFAULT))
    ap.add_argument("--attempts", type=int, default=3)
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    ckpt_path = Path(args.checkpoint)

    resume = args.resume or read_checkpoint(ckpt_path)
    if resume:
        start = date.fromisoformat(resume)

    log(f"[start] range {args.start}..{args.end}  resume={resume!r}  checkpoint={ckpt_path}")

    d = start
    while d <= end:
        ds = d.isoformat()
        log(f"[day] {ds} begin")

        # If logs already exist (meaning we’ve already processed this day), skip safely.
        pre = day_counts(ds)

        # Option A (SOG-first): only require skater TOI to exist for this slate's games.
        if pre.games > 0 and pre.sk_toi > 0:
            log(f"[skip] {ds} already present: games={pre.games} sk_toi={pre.sk_toi} sk_sog={pre.sk_sog} gk_toi={pre.gk_toi}")
            write_checkpoint(ckpt_path, (d + timedelta(days=1)).isoformat())
            d += timedelta(days=1)
            continue

        # Run full pipeline for this slate day (date-pure due to env overrides)
        # IMPORTANT: do NOT require nhl.games to already be seeded; CLI will seed schedule/roster/features.
        run_cli_for_day(ds, attempts=args.attempts)

        post = day_counts(ds)
        log(f"[post] {ds} games={post.games} sk_toi={post.sk_toi} sk_sog={post.sk_sog} gk_toi={post.gk_toi}")

        # If there truly were no games, accept and move on.
        if post.games == 0:
            log(f"[skip] {ds} no games (after schedule seed)")
            write_checkpoint(ckpt_path, (d + timedelta(days=1)).isoformat())
            d += timedelta(days=1)
            continue

        # Guardrail (SOG-first): if the day has games, we expect skater TOI rows after the run.
        if post.games > 0 and post.sk_toi == 0:
            raise RuntimeError(
                f"GUARDRAIL: {ds} has games={post.games} but skater TOI rows missing "
                f"(sk_toi={post.sk_toi}, sk_sog={post.sk_sog}). Stopping to avoid poisoning."
            )

        write_checkpoint(ckpt_path, (d + timedelta(days=1)).isoformat())
        d += timedelta(days=1)

    log("[done] ✅ Range complete.")

if __name__ == "__main__":
    main()
