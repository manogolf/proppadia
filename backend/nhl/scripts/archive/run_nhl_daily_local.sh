#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

# (venv bootstrap unchanged) …

SLATE_DATE="${1:-$(TZ=America/New_York date +%F)}"
export SLATE_DATE
echo "SLATE_DATE=${SLATE_DATE}"

command -v psql >/dev/null || { echo "psql not found"; exit 1; }

# 1) Import schedule & rosters
SLATE_DATE="${SLATE_DATE}" python3 backend/nhl/scripts/import_schedule_today.py
SLATE_DATE="$SLATE_DATE" SKIP_ROSTER_STATUS=1 SKIP_PLAYERS=1 \
  python3 backend/nhl/scripts/import_roster_today.py

# 2) Refresh views (so offline fallback has fresh v_slate_*)
if [[ -f backend/nhl/scripts/refresh.sql ]]; then
  PGOPTIONS='-c statement_timeout=0' \
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/scripts/refresh.sql
fi

# 3) Single canonical writer for players + roster_status
SLATE_DATE="${SLATE_DATE}" python3 backend/nhl/scripts/refresh_players_and_roster_today.py

# -- Seed TODAY’s features so v_slate_* has rows for exports --
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/seed_sog_features_for_slate.sql

psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v slate_date="$SLATE_DATE" \
  -f backend/nhl/sql/seed_goalie_features_for_slate.sql

# Refresh views/materializations after seeding
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/scripts/refresh.sql

# --- NEW: fail fast if v_slate_* is empty (prevents empty exports/scoring) ---
V_SOG=$(psql "$SUPABASE_DB_URL" -Atqc \
  "select count(*) from nhl.v_slate_sog_features where game_date = date '$SLATE_DATE';")
V_SAV=$(psql "$SUPABASE_DB_URL" -Atqc \
  "select count(*) from nhl.v_slate_saves_features where game_date = date '$SLATE_DATE';")
echo "== v_slate counts ($SLATE_DATE) == sog=$V_SOG saves=$V_SAV"
if [[ "${V_SOG:-0}" -eq 0 || "${V_SAV:-0}" -eq 0 ]]; then
  echo "FATAL: v_slate_* views are empty for $SLATE_DATE (sog=$V_SOG saves=$V_SAV)"
  exit 4
fi

# --- NEW: last-mile name safety (use first+last if full_name is placeholder) ---
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -c "
  UPDATE nhl.players
     SET full_name = btrim(concat_ws(' ', first_name, last_name)),
         updated_at = now()
   WHERE (full_name IS NULL OR btrim(full_name)='' OR full_name ~* '^(player|unknown)\\s+\\d+$')
     AND btrim(concat_ws(' ', first_name, last_name)) <> '';
"

# 4) Export CSVs (names now come from players via COALESCE in export_*.sql)
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

echo "Done. Open your page at http://localhost:8080 (if serving nhl/site)"
