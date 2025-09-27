#!/usr/bin/env python3
"""
Backfill two NHL seasons (2023-24, 2024-25) from api-web into CSVs.

- Calls scripts/fetch_nhl_to_csv.py once per month
- Appends rows into season-wide aggregate files (no header duplication)
- Leaves you with:
    data/import_bulk/skater_game_logs_raw_2seasons.csv
    data/import_bulk/goalie_game_logs_raw_2seasons.csv
"""

import csv
import subprocess
import sys
from pathlib import Path
from datetime import date, timedelta

# --- project paths ---
PROJ = Path.home() / "Projects" / "Proppadia" / "nhl-props"
FETCH = PROJ / "scripts" / "fetch_nhl_to_csv.py"
TMPDIR = PROJ / "data" / "import_templates"
OUTDIR = PROJ / "data" / "import_bulk"
SK_TMP = TMPDIR / "skater_game_logs_raw.csv"
GK_TMP = TMPDIR / "goalie_game_logs_raw.csv"
SK_OUT = OUTDIR / "skater_game_logs_raw_2seasons.csv"
GK_OUT = OUTDIR / "goalie_game_logs_raw_2seasons.csv"

# --- helpers ---
def month_range(y: int, m: int) -> tuple[date, date]:
    """Return (first_day, last_day) of month."""
    first = date(y, m, 1)
    # next month first day:
    if m == 12:
        nxt = date(y + 1, 1, 1)
    else:
        nxt = date(y, m + 1, 1)
    last = nxt - timedelta(days=1)
    return first, last

def safe_append(src_csv: Path, dst_csv: Path, header: list[str]) -> int:
    """Append rows from src_csv to dst_csv, skipping header. Returns rows appended."""
    if not src_csv.exists() or src_csv.stat().st_size == 0:
        return 0
    added = 0
    OUTDIR.mkdir(parents=True, exist_ok=True)
    # Ensure destination exists with header
    new_file = not dst_csv.exists()
    with dst_csv.open("a", newline="") as fout:
        w = csv.writer(fout)
        if new_file:
            w.writerow(header)
        with src_csv.open("r", newline="") as fin:
            r = csv.reader(fin)
            try:
                src_header = next(r, None)
            except Exception:
                src_header = None
            for row in r:
                if not row or all(c == "" for c in row):
                    continue
                w.writerow(row)
                added += 1
    return added

def run_fetch(start: date, end: date) -> None:
    cmd = [
        sys.executable, str(FETCH),
        "--start", start.isoformat(),
        "--end", end.isoformat(),
    ]
    print(f"⏳ Fetch {start} → {end}")
    res = subprocess.run(cmd, cwd=str(PROJ))
    if res.returncode != 0:
        raise SystemExit(f"fetch_nhl_to_csv failed for {start}→{end} (code {res.returncode})")

def main():
    # sanity checks
    if not FETCH.exists():
        raise SystemExit(f"Missing fetch script: {FETCH}")
    (PROJ / ".venv").exists()  # optional check; you already activate outside

    # target seasons (regular season windows)
    months_2324 = [(2023, m) for m in range(10, 13)] + [(2024, m) for m in (1,2,3,4,5,6)]
    months_2425 = [(2024, m) for m in range(10, 13)] + [(2025, m) for m in (1,2,3,4,5,6)]
    months = months_2324 + months_2425

    # reset outputs
    if SK_OUT.exists(): SK_OUT.unlink()
    if GK_OUT.exists(): GK_OUT.unlink()

    total_sk = total_gk = 0

    # canonical headers expected by your import tables
    SK_HDR = ["player_id","game_id","team_id","opponent_id","is_home","shots_on_goal","shot_attempts","toi_minutes","pp_toi_minutes","game_date"]
    GK_HDR = ["player_id","game_id","team_id","opponent_id","is_home","shots_faced","saves","goals_allowed","toi_minutes","start_prob","game_date"]

    for y, m in months:
        start, last = month_range(y, m)
        try:
            run_fetch(start, last)
        except SystemExit as e:
            print(f"⚠️  Skip {y}-{m:02d}: {e}")
            continue

        sk_added = safe_append(SK_TMP, SK_OUT, SK_HDR)
        gk_added = safe_append(GK_TMP, GK_OUT, GK_HDR)
        total_sk += sk_added
        total_gk += gk_added
        print(f"   ↳ appended skaters={sk_added} goalies={gk_added}")

    print("\n✅ Done.")
    print(f"   {SK_OUT}  (rows: {total_sk})")
    print(f"   {GK_OUT}  (rows: {total_gk})")
    print("→ Import these into nhl.import_skaters_stage / nhl.import_goalies_stage, then run your saved upsert.")

if __name__ == "__main__":
    main()
