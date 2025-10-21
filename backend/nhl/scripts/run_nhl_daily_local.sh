#!/usr/bin/env bash
set -euo pipefail

# --- Bootstrap Python env (venv + deps) ---
# repo root (three levels up from backend/nhl/scripts/)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

# venv
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

# Try to import deps; if it fails, install them.
if ! python3 - <<'PY'; then
try:
    import requests, psycopg, dotenv  # noqa: F401
    print("deps_ok")
except Exception:
    raise SystemExit(1)
PY
  python3 -m pip install -U pip wheel >/dev/null
  python3 -m pip install "psycopg[binary]>=3.1" requests python-dotenv >/dev/null
fi
# --- end bootstrap ---

# --- Config ---
SLATE_DATE="${1:-$(TZ=America/New_York date +%F)}"
echo "SLATE_DATE=${SLATE_DATE}"

# Check deps
command -v psql >/dev/null || { echo "psql not found"; exit 1; }
python3 -c 'import sys' >/dev/null || { echo "python not found"; exit 1; }

# 0) Ensure players & roster_status are up-to-date for the slate
SLATE_DATE="${SLATE_DATE}" python3 backend/nhl/scripts/refresh_players_and_roster_today.py

# 1) Import schedule & rosters
SLATE_DATE="${SLATE_DATE}" python3 backend/nhl/scripts/import_schedule_today.py
SLATE_DATE="${SLATE_DATE}" python3 backend/nhl/scripts/import_roster_today.py

# 2) Refresh features/views
if [[ -f backend/nhl/scripts/refresh.sql ]]; then
  PGOPTIONS='-c statement_timeout=0' \
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/scripts/refresh.sql
fi

# 3) Export SOG / SAVES features to CSV (header-only if no rows)
mkdir -p exports
psql --no-psqlrc -q "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/export_sog.sql > exports/train_nhl_sog_v2.csv

psql --no-psqlrc -q "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/export_saves.sql > exports/train_goalie_saves_v2.csv

echo "Exported:"
wc -l exports/train_nhl_sog_v2.csv exports/train_goalie_saves_v2.csv || true

# 4) Score & load predictions (writes sog/saves_predictions.csv)
python3 backend/nhl/scripts/run_daily_slate.py \
  --project nhl \
  --sog-csv exports/train_nhl_sog_v2.csv \
  --saves-csv exports/train_goalie_saves_v2.csv \
  --scorer backend/nhl/scripts/score_nhl_props.py \
  --db-url "$SUPABASE_DB_URL"

# 5) Attach names → sog_with_names.csv & saves_with_names.csv
python3 backend/nhl/scripts/attach_names.py

# 6) Optionally copy into the static site folder
mkdir -p nhl/site/data
cp backend/nhl/data/processed/sog_with_names.csv   nhl/site/data/ || true
cp backend/nhl/data/processed/saves_with_names.csv nhl/site/data/ || true

echo "Done. Open your page at http://localhost:8080 (if serving nhl/site)"
