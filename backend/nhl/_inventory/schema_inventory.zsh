#!/usr/bin/env zsh
set -euo pipefail

# Require env DSN
: "${SUPABASE_DB_URL:?SUPABASE_DB_URL must be set in your environment}"

ROOT_DIR="backend/nhl/_inventory"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${ROOT_DIR}/nhl_schema_inventory_${TS}"
mkdir -p "$OUT_DIR"

echo "Using SUPABASE_DB_URL (redacted): ${SUPABASE_DB_URL%%\?*}?…"
echo "Writing outputs to: $OUT_DIR"
echo ""

run_sql_csv() {
  local out="$1"
  shift
  local sql="$*"
  psql "$SUPABASE_DB_URL" --no-psqlrc -v ON_ERROR_STOP=1 \
    -c "\pset format csv \pset footer off \pset tuples_only on" \
    -c "$sql" > "$out"
}

# 1) Objects (tables/views/matviews/etc) with size + est_rows
run_sql_csv "${OUT_DIR}/objects.csv" "
SELECT
  n.nspname                     AS schema,
  c.relname                     AS name,
  CASE c.relkind
    WHEN 'r' THEN 'table'
    WHEN 'v' THEN 'view'
    WHEN 'm' THEN 'matview'
    WHEN 'p' THEN 'partitioned_table'
    WHEN 'S' THEN 'sequence'
    WHEN 'f' THEN 'foreign_table'
    ELSE c.relkind::text
  END                           AS kind,
  pg_total_relation_size(c.oid) AS bytes_total,
  pg_relation_size(c.oid)       AS bytes_heap,
  pg_indexes_size(c.oid)        AS bytes_indexes,
  c.reltuples::bigint           AS est_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'nhl'
  AND c.relkind IN ('r','v','m','p','S','f')
ORDER BY kind, bytes_total DESC, name;
"

# 2) Columns inventory (searchable via ripgrep)
run_sql_csv "${OUT_DIR}/columns.csv" "
SELECT
  table_schema,
  table_name,
  column_name,
  data_type,
  is_nullable,
  COALESCE(column_default,'') AS column_default
FROM information_schema.columns
WHERE table_schema = 'nhl'
ORDER BY table_name, ordinal_position;
"

# 3) View definitions (often explains “why does this exist?”)
run_sql_csv "${OUT_DIR}/views.csv" "
SELECT
  n.nspname AS schema,
  c.relname AS view_name,
  CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'matview' ELSE c.relkind::text END AS kind,
  pg_get_viewdef(c.oid, true) AS viewdef
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'nhl'
  AND c.relkind IN ('v','m')
ORDER BY view_name;
"

# 4) Dependencies (critical for safe deletions)
run_sql_csv "${OUT_DIR}/deps.csv" "
WITH dep AS (
  SELECT
    n1.nspname AS referenced_schema,
    c1.relname AS referenced_object,
    c1.relkind AS referenced_relkind,
    n2.nspname AS dependent_schema,
    c2.relname AS dependent_object,
    c2.relkind AS dependent_relkind
  FROM pg_depend d
  JOIN pg_rewrite r ON r.oid = d.objid
  JOIN pg_class c2  ON c2.oid = r.ev_class
  JOIN pg_class c1  ON c1.oid = d.refobjid
  JOIN pg_namespace n1 ON n1.oid = c1.relnamespace
  JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
  WHERE n1.nspname = 'nhl'
    AND n2.nspname = 'nhl'
)
SELECT
  referenced_schema || '.' || referenced_object AS referenced_object,
  CASE referenced_relkind
    WHEN 'r' THEN 'table'
    WHEN 'v' THEN 'view'
    WHEN 'm' THEN 'matview'
    WHEN 'p' THEN 'partitioned_table'
    ELSE referenced_relkind::text
  END AS referenced_type,
  dependent_schema || '.' || dependent_object AS dependent_object,
  CASE dependent_relkind
    WHEN 'r' THEN 'table'
    WHEN 'v' THEN 'view'
    WHEN 'm' THEN 'matview'
    WHEN 'p' THEN 'partitioned_table'
    ELSE dependent_relkind::text
  END AS dependent_type
FROM dep
ORDER BY referenced_object, dependent_object;
"

echo "✅ Done."
echo "Directory: $OUT_DIR"
echo "Files:"
echo "  - $OUT_DIR/objects.csv"
echo "  - $OUT_DIR/columns.csv"
echo "  - $OUT_DIR/views.csv"
echo "  - $OUT_DIR/deps.csv"
echo ""
echo "Tip: search columns quickly:"
echo "  rg -n \"szn_toi_per_game_pk|pp_toi_minutes|game_manpower_segments\" $OUT_DIR/columns.csv"
