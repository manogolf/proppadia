#!/usr/bin/env bash
set -euo pipefail

ROOT="$(pwd)"
PY="${ROOT}/.venv/bin/python3"
SQL_DIR="${ROOT}/backend/nhl/sql"
SCRIPTS_DIR="${ROOT}/backend/nhl/scripts"

for i in {0..29}; do
  # Compute SLATE_DATE in ET (GNU date if available, else macOS BSD date)
  if command -v gdate >/dev/null 2>&1; then
    SLATE_DATE="$(TZ=America/New_York gdate -d "-${i} days" +%Y-%m-%d)"
  else
    SLATE_DATE="$(TZ=America/New_York date -v-"${i}"d +%Y-%m-%d)"
  fi

  echo "=== ${SLATE_DATE} ==="

  # 1) Boxscore → CSV (best-effort)
  SLATE_DATE="${SLATE_DATE}" "${PY}" "${SCRIPTS_DIR}/ingest_points_from_boxscores.py" || true

  CSV="exports/points_stage_${SLATE_DATE}.csv"
  if [ -f "${CSV}" ]; then
    # 2) CSV → DB (stage/raw upsert)
    psql --no-psqlrc -q -v ON_ERROR_STOP=1 \
      "$SUPABASE_DB_URL" \
      -v slate_date="${SLATE_DATE}" \
      -v csv_path="${CSV}" \
      -f "${SQL_DIR}/seed_points_from_csv.sql"
  else
    echo "   (no CSV for ${SLATE_DATE}, skipping seed)"
  fi
done

# Sanity: any non-zero points in last 30 days?
psql "$SUPABASE_DB_URL" -Atqc "
SELECT COUNT(*)
FROM nhl.skater_game_logs_raw l
JOIN nhl.games g USING(game_id)
WHERE g.game_date >= CURRENT_DATE - INTERVAL '30 days'
  AND COALESCE(l.points, COALESCE(l.goals,0)+COALESCE(l.assists,0)) > 0;
"
