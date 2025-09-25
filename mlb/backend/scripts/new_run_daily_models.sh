#  backend/scripts/run_daily_models.sh

#!/usr/bin/env bash
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO"

# env
export MODELS_DIR="$REPO/models_out"
export MODEL_DIR="$MODELS_DIR"
export MIN_CLASS_COUNT=50

# 1) Refresh synthetic outs for yesterday (starters only; per-pitcher prior-starts line)
psql "$PGURL" <<'SQL'
DO $$
DECLARE
  s date := current_date - 1;
  t date := current_date;
  clamp_low numeric := 14.5;
  clamp_high numeric := 19.5;
  start_min_outs int := 12;
  fallback_line numeric := 16.5;
BEGIN
  WITH base AS (
    SELECT
      ps.player_id::text AS player_id,
      ps.game_id::text   AS game_id,
      ps.game_date::date AS game_date,
      COALESCE(ps.is_home,false) AS is_home,
      ps.outs_recorded::numeric  AS outs_val,
      (ps.outs_recorded >= start_min_outs) AS is_start
    FROM public.player_stats ps
    JOIN public.starting_pitchers_ref spr
      ON spr.game_id = ps.game_id
     AND ps.player_id::text IN (spr.home_starter_id, spr.away_starter_id)
    WHERE ps.outs_recorded IS NOT NULL
      AND ps.game_date >= s
      AND ps.game_date <  t
  ),
  enriched AS (
    SELECT
      b.*,
      AVG(CASE WHEN b.is_start THEN b.outs_val END) OVER (
        PARTITION BY b.player_id
        ORDER BY b.game_date, b.game_id
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
      ) AS avg_prior_starts
    FROM base b
  ),
  with_line AS (
    SELECT
      e.player_id, e.game_id, e.game_date, e.is_home, e.outs_val,
      CASE
        WHEN e.avg_prior_starts IS NOT NULL
          THEN (ROUND(GREATEST(clamp_low, LEAST(clamp_high, e.avg_prior_starts)) * 2) / 2.0)
        ELSE fallback_line
      END AS synth_line
    FROM enriched e
  )
  INSERT INTO public.training_features_store (
    player_id, game_id, prop_type, prop_source, game_date,
    result, outcome,
    time_of_day_bucket, game_day_of_week,
    is_home, is_pitcher, over_under,
    line, prop_value, line_diff,
    streak_type, streak_count, rolling_result_avg_7
  )
  SELECT DISTINCT ON (wl.player_id, wl.game_id)
    wl.player_id, wl.game_id,
    'outs_recorded', 'synth', wl.game_date,
    CASE WHEN wl.outs_val >= wl.synth_line THEN 1 ELSE 0 END AS result,
    CASE WHEN wl.outs_val >= wl.synth_line THEN 'win' ELSE 'loss' END AS outcome,
    'night'::text, to_char(wl.game_date,'Dy'),
    wl.is_home, TRUE, 'over',
    wl.synth_line, wl.outs_val, (wl.outs_val - wl.synth_line),
    t.streak_type, t.streak_count, t.rolling_result_avg_7
  FROM with_line wl
  LEFT JOIN public.training_features_streaks_text t
    ON t.player_id = wl.player_id
   AND t.game_id   = wl.game_id
   AND t.prop_type = 'outs_recorded'
  ON CONFLICT (player_id, game_id, prop_type, prop_source) DO UPDATE
  SET line        = EXCLUDED.line,
      prop_value  = EXCLUDED.prop_value,
      result      = EXCLUDED.result,
      outcome     = EXCLUDED.outcome,
      over_under  = EXCLUDED.over_under,
      line_diff   = EXCLUDED.line_diff,
      is_home     = EXCLUDED.is_home,
      is_pitcher  = TRUE,
      time_of_day_bucket   = EXCLUDED.time_of_day_bucket,
      game_day_of_week     = EXCLUDED.game_day_of_week,
      streak_type          = COALESCE(EXCLUDED.streak_type, public.training_features_store.streak_type),
      streak_count         = COALESCE(EXCLUDED.streak_count, public.training_features_store.streak_count),
      rolling_result_avg_7 = COALESCE(EXCLUDED.rolling_result_avg_7, public.training_features_store.rolling_result_avg_7);
END $$;
SQL

# 2) Train all props against the right feature view
PROP_LIST_PITCH_OUTS=("outs_recorded")
PROP_LIST_PITCH=("earned_runs" "hits_allowed" "walks_allowed" "strikeouts_pitching")
PROP_LIST_BAT=("hits" "hits_runs_rbis" "rbis" "runs_scored" "doubles" "singles" "total_bases" "strikeouts_batting" "walks" "stolen_bases" "triples" "home_runs")

VENV="./.venv/bin/python"

# outs_recorded
export FEATURE_VIEW="training_features_pitching_outs_enriched"
$VENV -m backend.scripts.retrain_all_models --prop outs_recorded --days-back 1095 --limit 100000 || true

# pitching (starters-only view)
export FEATURE_VIEW="training_features_pitching_enriched_mlb"
for p in "${PROP_LIST_PITCH[@]}"; do
  $VENV -m backend.scripts.retrain_all_models --prop "$p" --days-back 1095 --limit 100000 || true
done

# batting
export FEATURE_VIEW="training_features_hits_enriched"
for p in "${PROP_LIST_BAT[@]}"; do
  $VENV -m backend.scripts.retrain_all_models --prop "$p" --days-back 1095 --limit 100000 || true
done

# daily 8:10am
10 8 * * * cd /Users/jerrystrain/Projects/baseball-streaks && \
  export MODELS_DIR="$PWD/models_out" MODEL_DIR="$PWD/models_out" FEATURE_VIEW=training_features_runs_enriched_v11 MIN_CLASS_COUNT=50 && \
  ./.venv/bin/python -m backend.scripts.retrain_all_models --prop runs_scored --days-back 1095 --limit 50000 >> logs/train_runs_scored.log 2>&1

# daily 8:12am
12 8 * * * cd /Users/jerrystrain/Projects/baseball-streaks && \
  export MODELS_DIR="$PWD/models_out" MODEL_DIR="$PWD/models_out" \
         FEATURE_VIEW=training_features_hrr_enriched_v1 MIN_CLASS_COUNT=50 && \
  ./.venv/bin/python -m backend.scripts.retrain_all_models \
    --prop hits_runs_rbis --days-back 1095 --limit 50000 >> logs/train_hrr.log 2>&1


#!/usr/bin/env bash
set -euo pipefail

# --- config ---
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$REPO_DIR/.venv/bin/python"
export MODELS_DIR="$REPO_DIR/models_out"
export MODEL_DIR="$MODELS_DIR"
export MIN_CLASS_COUNT=50

# trainer knobs
DAYS_BACK=1095
LIMIT_DEFAULT=50000        # safe for base views; bump if using per-prop MVs

# map: prop -> feature view to read
declare -A VIEW=(
  # batting (use per-prop MV if you created it; else the big batting view)
  [hits]=public.training_features_batting_enriched_v1
  [home_runs]=public.training_features_batting_enriched_v1
  [rbis]=public.training_features_batting_enriched_v1
  [runs_rbis]=public.training_features_batting_enriched_v1
  [runs_scored]=public.training_features_runs_enriched_v11
  [singles]=public.training_features_batting_enriched_v1
  [total_bases]=public.training_features_batting_enriched_v1
  [walks]=public.training_features_batting_enriched_v1
  [strikeouts_batting]=public.training_features_batting_enriched_v1
  [doubles]=public.training_features_batting_enriched_v1
  [triples]=public.training_features_batting_enriched_v1
  [stolen_bases]=public.training_features_batting_enriched_v1

  # pitching (train only outs_recorded daily; hold others until enriched)
  [outs_recorded]=public.training_features_pitching_outs_enriched
)

# if you built per-prop MVs, override VIEW here, e.g.:
# VIEW[home_runs]=public.training_features_batting_home_runs_mv
# VIEW[rbis]=public.training_features_batting_rbis_mv
# ...etc.

# lock to avoid overlap
LOCKDIR="/tmp/bs_retrain.lock"
if ! mkdir "${LOCKDIR}" 2>/dev/null; then
  echo "another run is in progress; exiting"
  exit 0
fi
trap 'rmdir "${LOCKDIR}" || true' EXIT

cd "$REPO_DIR"

# 1) refresh fast MVs that feed features (ignore if missing)
psql "${DATABASE_URL:-}" -v ON_ERROR_STOP=1 <<'SQL' || true
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname='public' AND matviewname='team_rolling_agg_v1') THEN
    EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY public.team_rolling_agg_v1';
  END IF;
END$$;
SQL

# 2) regenerate synthetic outs_recorded for recent window (last 14 days)
psql "${DATABASE_URL:-}" -v ON_ERROR_STOP=1 <<'SQL' || true
WITH params AS (
  SELECT CURRENT_DATE - INTERVAL '14 days' AS s, CURRENT_DATE + INTERVAL '1 day' AS e,
         15.5::numeric AS fallback_line, 14.5::numeric AS clamp_low, 19.5::numeric AS clamp_high
),
base AS (
  SELECT ps.player_id::text AS player_id,
         ps.game_id::text   AS game_id,
         ps.game_date::date AS game_date,
         COALESCE(ps.is_home,false) AS is_home,
         ps.outs_recorded::numeric  AS outs_val,
         (ps.outs_recorded >= 12)   AS is_start
  FROM public.player_stats ps
  JOIN params p ON ps.game_date >= p.s::date AND ps.game_date < p.e::date
  WHERE ps.outs_recorded IS NOT NULL
),
enriched AS (
  SELECT b.*,
         AVG(CASE WHEN b.is_start THEN b.outs_val END) OVER (
           PARTITION BY b.player_id
           ORDER BY b.game_date, b.game_id
           ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
         ) AS avg_prior_starts
  FROM base b
),
with_line AS (
  SELECT e.player_id, e.game_id, e.game_date, e.is_home, e.outs_val,
         CASE WHEN e.avg_prior_starts IS NOT NULL
              THEN (ROUND(GREATEST(p.clamp_low, LEAST(p.clamp_high, e.avg_prior_starts)) * 2) / 2.0)
              ELSE p.fallback_line END AS synth_line
  FROM enriched e CROSS JOIN params p
)
INSERT INTO public.training_features_store (
  player_id, game_id, prop_type, prop_source, game_date,
  result, outcome, time_of_day_bucket, game_day_of_week,
  is_home, is_pitcher, over_under, line, prop_value, line_diff,
  streak_type, streak_count, rolling_result_avg_7
)
SELECT wl.player_id, wl.game_id, 'outs_recorded', 'synth', wl.game_date,
       CASE WHEN wl.outs_val >= wl.synth_line THEN 1 ELSE 0 END,
       CASE WHEN wl.outs_val >= wl.synth_line THEN 'win' ELSE 'loss' END,
       'night', to_char(wl.game_date,'Dy'), wl.is_home, TRUE, 'over',
       wl.synth_line, wl.outs_val, (wl.outs_val - wl.synth_line),
       t.streak_type, t.streak_count, t.rolling_result_avg_7
FROM with_line wl
LEFT JOIN public.training_features_streaks_text t
  ON t.player_id = wl.player_id AND t.game_id = wl.game_id AND t.prop_type='outs_recorded'
ON CONFLICT (player_id, game_id, prop_type, prop_source) DO UPDATE
SET line=EXCLUDED.line, prop_value=EXCLUDED.prop_value, result=EXCLUDED.result, outcome=EXCLUDED.outcome,
    over_under=EXCLUDED.over_under, line_diff=EXCLUDED.line_diff,
    is_home=EXCLUDED.is_home, is_pitcher=TRUE,
    time_of_day_bucket=EXCLUDED.time_of_day_bucket, game_day_of_week=EXCLUDED.game_day_of_week,
    streak_type=COALESCE(EXCLUDED.streak_type, training_features_store.streak_type),
    streak_count=COALESCE(EXCLUDED.streak_count, training_features_store.streak_count),
    rolling_result_avg_7=COALESCE(EXCLUDED.rolling_result_avg_7, training_features_store.rolling_result_avg_7);
SQL

# 3) refresh prop-specific MVs if you created them
psql "${DATABASE_URL:-}" -v ON_ERROR_STOP=1 <<'SQL' || true
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT schemaname, matviewname
    FROM pg_matviews
    WHERE schemaname='public'
      AND matviewname LIKE 'training_features_batting\_%\_mv' ESCAPE '\'
  LOOP
    EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %I.%I', r.schemaname, r.matviewname);
  END LOOP;
END$$;
SQL

# 4) train props
LOGDIR="$MODELS_DIR/logs"
mkdir -p "$LOGDIR"
ts="$(date +%Y%m%dT%H%M%S)"

for PROP in "${!VIEW[@]}"; do
  export FEATURE_VIEW="${VIEW[$PROP]}"
  LIMIT="$LIMIT_DEFAULT"

  echo "==> Training $PROP from $FEATURE_VIEW"
  "$PY" -m backend.scripts.retrain_all_models \
    --prop "$PROP" --days-back "$DAYS_BACK" --limit "$LIMIT" \
    2>&1 | tee -a "$LOGDIR/train-$PROP-$ts.log"
done

echo "done."

-- adjust the list to your actual MV names
SET statement_timeout = '15min';

REFRESH MATERIALIZED VIEW CONCURRENTLY public.team_rolling_agg_v1;

-- if you created per-prop MVs, add them here, e.g.:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY public.training_features_batting_enriched_mv;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY public.training_features_runs_enriched_mv;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY public.training_features_pitching_outs_enriched_mv;

-- replace view names as needed
SELECT 'runs'  AS v, MIN(game_date), MAX(game_date), COUNT(*) FROM public.training_features_runs_enriched_v11;
SELECT 'bat'   AS v, MIN(game_date), MAX(game_date), COUNT(*) FROM public.training_features_batting_enriched_v1;
SELECT 'p_out' AS v, MIN(game_date), MAX(game_date), COUNT(*) FROM public.training_features_pitching_outs_enriched;

#!/usr/bin/env bash
set -euo pipefail

REPO="/Users/jerrystrain/Projects/baseball-streaks"
PY="$REPO/.venv/bin/python"

export MODELS_DIR="$REPO/models_out"
export MODEL_DIR="$MODELS_DIR"
export MIN_CLASS_COUNT=50

# Map each prop to the feature view that worked best in your runs
declare -A VIEW=(
  [runs_scored]="training_features_runs_enriched_v11"
  # unified enriched batting view for the rest of batting props
  [hits_runs_rbis]="training_features_batting_enriched_v1"
  [rbis]="training_features_batting_enriched_v1"
  [runs_rbis]="training_features_batting_enriched_v1"
  [home_runs]="training_features_batting_enriched_v1"
  [singles]="training_features_batting_enriched_v1"
  [total_bases]="training_features_batting_enriched_v1"
  [walks]="training_features_batting_enriched_v1"
  [strikeouts_batting]="training_features_batting_enriched_v1"
  [hits]="training_features_batting_enriched_v1"

  # pitching (only outs for daily; hold others until enriched)
  [outs_recorded]="training_features_pitching_outs_enriched"
)

# Default sampling + per-prop tweaks
DEFAULT_LIMIT=50000
DEFAULT_DAYS=1095

declare -A LIMIT=(
  [outs_recorded]=200000    # small enough corpus; use (nearly) all
  # add [triples]=30000 if you decide to include it here
)

props=(
  runs_scored
  hits_runs_rbis
  rbis
  runs_rbis
  home_runs
  singles
  total_bases
  walks
  strikeouts_batting
  hits
  outs_recorded
)

for p in "${props[@]}"; do
  export FEATURE_VIEW="${VIEW[$p]}"
  lim="${LIMIT[$p]:-$DEFAULT_LIMIT}"
  echo "[train] $p via $FEATURE_VIEW limit=$lim days=$DEFAULT_DAYS"
  "$PY" -m backend.scripts.retrain_all_models \
    --prop "$p" \
    --days-back "$DEFAULT_DAYS" \
    --limit "$lim" \
  || echo "⚠️  $p failed; continuing"
done

chmod +x backend/scripts/cron/train_daily.sh

# crontab -e
30 05 * * * cd /Users/jerrystrain/Projects/baseball-streaks && psql "$DATABASE_URL" -f backend/scripts/cron/refresh_mvs.sql >> logs/refresh_mvs.$(date +\%Y\%m\%d).log 2>&1
50 05 * * * cd /Users/jerrystrain/Projects/baseball-streaks && backend/scripts/cron/train_daily.sh >> logs/train.$(date +\%Y\%m\%d).log 2>&1

# Sun 6:10am — push limits up a bit
10 06 * * 0 cd /Users/jerrystrain/Projects/baseball-streaks && FEATURE_VIEW=training_features_batting_enriched_v1 \
  ./.venv/bin/python -m backend.scripts.retrain_all_models --prop hits_runs_rbis --days-back 1095 --limit 150000 >> logs/train_deep.$(date +\%Y\%m\%d).log 2>&1
# repeat for hits / rbis as desired

-- runs view: ensure key features are present
SELECT
  COUNT(*) AS n,
  SUM((d7_runs_scored         IS NOT NULL)::int) AS nn_rs7,
  SUM((d7_team_runs_pg        IS NOT NULL)::int) AS nn_team7,
  SUM((d7_opp_runs_pg         IS NOT NULL)::int) AS nn_opp7,
  SUM((bvp_at_bats            IS NOT NULL)::int) AS nn_bvp_ab
FROM public.training_features_runs_enriched_v11;

-- batting view: same idea
SELECT
  COUNT(*) AS n,
  SUM((d7_hits IS NOT NULL)::int) AS nn_hits7,
  SUM((bvp_hits IS NOT NULL)::int) AS nn_bvp_hits
FROM public.training_features_batting_enriched_v1;

-- outs view
SELECT COUNT(*) AS n FROM public.training_features_pitching_outs_enriched;
