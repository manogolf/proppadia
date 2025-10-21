#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

# (venv bootstrap unchanged) …

SLATE_DATE="${1:-$(TZ=America/New_York date +%F)}"
echo "SLATE_DATE=${SLATE_DATE}"

# 1) Import schedule & rosters
SLATE_DATE="${SLATE_DATE}" python3 backend/nhl/scripts/import_schedule_today.py
SLATE_DATE="${SLATE_DATE}" SKIP_ROSTER_STATUS=1 python3 backend/nhl/scripts/import_roster_today.py

# 2) Refresh views (so offline fallback has fresh v_slate_*)
if [[ -f backend/nhl/scripts/refresh.sql ]]; then
  PGOPTIONS='-c statement_timeout=0' \
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/scripts/refresh.sql
fi

# 3) Single canonical writer for players + roster_status
SLATE_DATE="${SLATE_DATE}" python3 backend/nhl/scripts/refresh_players_and_roster_today.py

# 4) Export CSVs
mkdir -p exports
psql --no-psqlrc -q "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/export_sog.sql > exports/train_nhl_sog_v2.csv

psql --no-psqlrc -q "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/export_saves.sql > exports/train_goalie_saves_v2.csv

echo "Exported:"
wc -l exports/train_nhl_sog_v2.csv exports/train_goalie_saves_v2.csv || true

# 5) Score & load predictions
python3 backend/nhl/scripts/run_daily_slate.py \
  --project nhl \
  --sog-csv exports/train_nhl_sog_v2.csv \
  --saves-csv exports/train_goalie_saves_v2.csv \
  --scorer backend/nhl/scripts/score_nhl_props.py \
  --db-url "$SUPABASE_DB_URL"

# 6) Attach names
python3 backend/nhl/scripts/attach_names.py

# 7) Copy for site (optional)
mkdir -p nhl/site/data
cp backend/nhl/data/processed/sog_with_names.csv   nhl/site/data/ || true
cp backend/nhl/data/processed/saves_with_names.csv nhl/site/data/ || true

echo "Done." Open your page at http://localhost:8080 (if serving nhl/site)"
