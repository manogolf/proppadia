-- MVP backend datasets for /mlb/today decision workspace.
-- Inputs must be loaded first into:
--   mlb.today_odds_book_rows
--   mlb.today_slate_rows
--   mlb.today_wide_rows
--
-- Notes:
-- - No picks/recommendations/ranking logic here.
-- - Descriptive market + timing + player context only.

BEGIN;

CREATE SCHEMA IF NOT EXISTS mlb;

DROP MATERIALIZED VIEW IF EXISTS mlb.today_workspace_mlb;
DROP MATERIALIZED VIEW IF EXISTS mlb.today_market_timing_signal;
DROP MATERIALIZED VIEW IF EXISTS mlb.today_market_snapshot;
DROP MATERIALIZED VIEW IF EXISTS mlb.today_player_context;

-- ---------------------------------------------------------------------------
-- STAGE 1: Current market snapshot
-- One row per (player_id, game_id, prop_type, line) at latest snapshot.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mlb.today_market_snapshot AS
WITH active_slate AS (
  SELECT max(slate_date) AS slate_date
  FROM mlb.today_slate_rows
),
base AS (
  SELECT
    o.*,
    CASE
      WHEN o.price_over_american IS NOT NULL AND abs(o.price_over_american) BETWEEN 100 AND 500
      THEN o.price_over_american
      ELSE NULL
    END AS price_over_american_clean,
    CASE
      WHEN o.price_under_american IS NOT NULL AND abs(o.price_under_american) BETWEEN 100 AND 500
      THEN o.price_under_american
      ELSE NULL
    END AS price_under_american_clean
  FROM mlb.today_odds_book_rows o
  JOIN active_slate a
    ON o.slate_date = a.slate_date
),
latest AS (
  SELECT
    b.*
  FROM base b
  JOIN (
    SELECT
      player_id,
      game_id,
      prop_type,
      line,
      max(snapshot_ts) AS last_snapshot_ts
    FROM base
    WHERE price_over_american_clean IS NOT NULL
       OR price_under_american_clean IS NOT NULL
    GROUP BY 1,2,3,4
  ) l
    ON b.player_id = l.player_id
   AND b.game_id = l.game_id
   AND b.prop_type = l.prop_type
   AND b.line = l.line
   AND b.snapshot_ts = l.last_snapshot_ts
  WHERE b.price_over_american_clean IS NOT NULL
     OR b.price_under_american_clean IS NOT NULL
),
best_over AS (
  SELECT DISTINCT ON (player_id, game_id, prop_type, line)
    player_id,
    game_id,
    prop_type,
    line,
    bookmaker_key AS best_over_book,
    price_over_american_clean AS best_over_price
  FROM latest
  WHERE price_over_american_clean IS NOT NULL
  ORDER BY player_id, game_id, prop_type, line, price_over_american_clean DESC, bookmaker_key
),
best_under AS (
  SELECT DISTINCT ON (player_id, game_id, prop_type, line)
    player_id,
    game_id,
    prop_type,
    line,
    bookmaker_key AS best_under_book,
    price_under_american_clean AS best_under_price
  FROM latest
  WHERE price_under_american_clean IS NOT NULL
  ORDER BY player_id, game_id, prop_type, line, price_under_american_clean DESC, bookmaker_key
),
agg AS (
  SELECT
    game_date,
    game_id,
    player_id,
    max(player_name) AS player_name,
    max(team) AS team,
    max(opponent) AS opponent,
    bool_or(is_home) AS is_home,
    prop_type,
    line,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY price_over_american_clean) AS market_median_over_price_raw,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY price_under_american_clean) AS market_median_under_price_raw,
    max(price_over_american_clean) AS market_max_over_price,
    min(price_over_american_clean) AS market_min_over_price,
    max(price_under_american_clean) AS market_max_under_price,
    min(price_under_american_clean) AS market_min_under_price,
    count(*) FILTER (WHERE price_over_american_clean IS NOT NULL) AS book_count_over,
    count(*) FILTER (WHERE price_under_american_clean IS NOT NULL) AS book_count_under,
    stddev_pop(price_over_american_clean) AS price_dispersion_over,
    stddev_pop(price_under_american_clean) AS price_dispersion_under,
    max(snapshot_ts) AS last_snapshot_ts
  FROM latest
  GROUP BY 1,2,3,8,9
  HAVING count(*) FILTER (WHERE price_over_american_clean IS NOT NULL) > 0
     AND count(*) FILTER (WHERE price_under_american_clean IS NOT NULL) > 0
)
SELECT
  a.game_date,
  a.game_id,
  a.player_id,
  a.player_name,
  a.team,
  a.opponent,
  a.is_home,
  a.prop_type,
  a.line,
  bo.best_over_price,
  bu.best_under_price,
  bo.best_over_book,
  bu.best_under_book,
  a.market_median_over_price_raw AS market_median_over_price,
  a.market_median_under_price_raw AS market_median_under_price,
  (a.market_max_over_price - a.market_min_over_price) AS market_range_over,
  (a.market_max_under_price - a.market_min_under_price) AS market_range_under,
  a.book_count_over,
  a.book_count_under,
  a.price_dispersion_over,
  a.price_dispersion_under,
  a.last_snapshot_ts
FROM agg a
LEFT JOIN best_over bo
  ON a.player_id = bo.player_id
 AND a.game_id = bo.game_id
 AND a.prop_type = bo.prop_type
 AND a.line = bo.line
LEFT JOIN best_under bu
  ON a.player_id = bu.player_id
 AND a.game_id = bu.game_id
 AND a.prop_type = bu.prop_type
 AND a.line = bu.line;

CREATE UNIQUE INDEX idx_today_market_snapshot_key
  ON mlb.today_market_snapshot (player_id, game_id, prop_type, line);

-- ---------------------------------------------------------------------------
-- STAGE 2: Intraday timing signal
-- Descriptive label only (EARLY/LOW CONFIDENCE/VOLATILE/STABLE).
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mlb.today_market_timing_signal AS
WITH active_slate AS (
  SELECT max(slate_date) AS slate_date
  FROM mlb.today_slate_rows
),
base AS (
  SELECT
    o.*,
    CASE
      WHEN o.price_over_american IS NOT NULL AND abs(o.price_over_american) BETWEEN 100 AND 500
      THEN o.price_over_american
      ELSE NULL
    END AS price_over_american_clean,
    CASE
      WHEN o.price_under_american IS NOT NULL AND abs(o.price_under_american) BETWEEN 100 AND 500
      THEN o.price_under_american
      ELSE NULL
    END AS price_under_american_clean
  FROM mlb.today_odds_book_rows o
  JOIN active_slate a
    ON o.slate_date = a.slate_date
),
snap AS (
  SELECT
    player_id,
    game_id,
    prop_type,
    line,
    snapshot_ts,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY price_over_american_clean) AS snap_over_median_raw,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY price_under_american_clean) AS snap_under_median_raw,
    max(price_over_american_clean) AS snap_best_over,
    max(price_under_american_clean) AS snap_best_under
  FROM base
  GROUP BY 1,2,3,4,5
  HAVING count(*) FILTER (WHERE price_over_american_clean IS NOT NULL) > 0
     AND count(*) FILTER (WHERE price_under_american_clean IS NOT NULL) > 0
),
ranked AS (
  SELECT
    s.*,
    row_number() OVER (PARTITION BY player_id, game_id, prop_type, line ORDER BY snapshot_ts ASC) AS rn_open,
    row_number() OVER (PARTITION BY player_id, game_id, prop_type, line ORDER BY snapshot_ts DESC) AS rn_latest,
    count(*) OVER (PARTITION BY player_id, game_id, prop_type, line) AS num_snapshots
  FROM snap s
),
open_rows AS (
  SELECT * FROM ranked WHERE rn_open = 1
),
latest_rows AS (
  SELECT * FROM ranked WHERE rn_latest = 1
),
vol AS (
  SELECT
    player_id,
    game_id,
    prop_type,
    line,
    max(snap_over_median_raw) - min(snap_over_median_raw) AS over_span,
    max(snap_under_median_raw) - min(snap_under_median_raw) AS under_span
  FROM snap
  GROUP BY 1,2,3,4
)
SELECT
  l.player_id,
  l.game_id,
  l.prop_type,
  l.line,
  CASE
    WHEN o.snap_over_median_raw IS NOT NULL AND abs(o.snap_over_median_raw) BETWEEN 100 AND 500
    THEN o.snap_over_median_raw
    ELSE NULL
  END AS open_over_price,
  CASE
    WHEN o.snap_under_median_raw IS NOT NULL AND abs(o.snap_under_median_raw) BETWEEN 100 AND 500
    THEN o.snap_under_median_raw
    ELSE NULL
  END AS open_under_price,
  CASE
    WHEN l.snap_over_median_raw IS NOT NULL AND abs(l.snap_over_median_raw) BETWEEN 100 AND 500
    THEN l.snap_over_median_raw
    ELSE NULL
  END AS latest_over_price,
  CASE
    WHEN l.snap_under_median_raw IS NOT NULL AND abs(l.snap_under_median_raw) BETWEEN 100 AND 500
    THEN l.snap_under_median_raw
    ELSE NULL
  END AS latest_under_price,
  l.snap_best_over AS best_over_price_now,
  l.snap_best_under AS best_under_price_now,
  extract(epoch FROM (l.snapshot_ts - o.snapshot_ts)) / 60.0 AS minutes_since_open,
  l.num_snapshots,
  (
    CASE
      WHEN l.snap_over_median_raw IS NOT NULL AND abs(l.snap_over_median_raw) BETWEEN 100 AND 500
      THEN l.snap_over_median_raw
      ELSE NULL
    END
    -
    CASE
      WHEN o.snap_over_median_raw IS NOT NULL AND abs(o.snap_over_median_raw) BETWEEN 100 AND 500
      THEN o.snap_over_median_raw
      ELSE NULL
    END
  ) AS over_price_change_from_open,
  (
    CASE
      WHEN l.snap_under_median_raw IS NOT NULL AND abs(l.snap_under_median_raw) BETWEEN 100 AND 500
      THEN l.snap_under_median_raw
      ELSE NULL
    END
    -
    CASE
      WHEN o.snap_under_median_raw IS NOT NULL AND abs(o.snap_under_median_raw) BETWEEN 100 AND 500
      THEN o.snap_under_median_raw
      ELSE NULL
    END
  ) AS under_price_change_from_open,
  v.over_span,
  v.under_span,
  (coalesce(l.snap_best_over, o.snap_best_over) > o.snap_best_over) AS best_price_improved_since_open,
  (coalesce(l.snap_best_over, o.snap_best_over) < o.snap_best_over) AS best_price_worsened_since_open,
  CASE
    WHEN greatest(coalesce(v.over_span, 0), coalesce(v.under_span, 0)) >= 25 THEN 'VOLATILE'
    WHEN coalesce(
      (
        CASE
          WHEN l.snap_over_median_raw IS NOT NULL AND abs(l.snap_over_median_raw) BETWEEN 100 AND 500
          THEN l.snap_over_median_raw
          ELSE NULL
        END
        -
        CASE
          WHEN o.snap_over_median_raw IS NOT NULL AND abs(o.snap_over_median_raw) BETWEEN 100 AND 500
          THEN o.snap_over_median_raw
          ELSE NULL
        END
      ),
      0
    ) >= 10 THEN 'LOW CONFIDENCE'
    WHEN coalesce(
      (
        CASE
          WHEN l.snap_over_median_raw IS NOT NULL AND abs(l.snap_over_median_raw) BETWEEN 100 AND 500
          THEN l.snap_over_median_raw
          ELSE NULL
        END
        -
        CASE
          WHEN o.snap_over_median_raw IS NOT NULL AND abs(o.snap_over_median_raw) BETWEEN 100 AND 500
          THEN o.snap_over_median_raw
          ELSE NULL
        END
      ),
      0
    ) <= -10 THEN 'EARLY'
    ELSE 'STABLE'
  END AS timing_signal,
  CASE
    WHEN greatest(coalesce(v.over_span, 0), coalesce(v.under_span, 0)) >= 25 THEN 'Large intraday movement'
    WHEN coalesce(
      (
        CASE
          WHEN l.snap_over_median_raw IS NOT NULL AND abs(l.snap_over_median_raw) BETWEEN 100 AND 500
          THEN l.snap_over_median_raw
          ELSE NULL
        END
        -
        CASE
          WHEN o.snap_over_median_raw IS NOT NULL AND abs(o.snap_over_median_raw) BETWEEN 100 AND 500
          THEN o.snap_over_median_raw
          ELSE NULL
        END
      ),
      0
    ) >= 10 THEN 'Market has moved materially from the open'
    WHEN coalesce(
      (
        CASE
          WHEN l.snap_over_median_raw IS NOT NULL AND abs(l.snap_over_median_raw) BETWEEN 100 AND 500
          THEN l.snap_over_median_raw
          ELSE NULL
        END
        -
        CASE
          WHEN o.snap_over_median_raw IS NOT NULL AND abs(o.snap_over_median_raw) BETWEEN 100 AND 500
          THEN o.snap_over_median_raw
          ELSE NULL
        END
      ),
      0
    ) <= -10 THEN 'Market is still forming'
    ELSE 'Little intraday movement'
  END AS timing_reason
FROM latest_rows l
JOIN open_rows o
  ON l.player_id = o.player_id
 AND l.game_id = o.game_id
 AND l.prop_type = o.prop_type
 AND l.line = o.line
LEFT JOIN vol v
  ON l.player_id = v.player_id
 AND l.game_id = v.game_id
 AND l.prop_type = v.prop_type
 AND l.line = v.line;

CREATE UNIQUE INDEX idx_today_market_timing_signal_key
  ON mlb.today_market_timing_signal (player_id, game_id, prop_type, line);

-- ---------------------------------------------------------------------------
-- STAGE 3: Player context (streak + baseline + consistency)
-- One row per (player_id, prop_type), sourced from graded model_training_props.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mlb.today_player_context AS
WITH active_slate AS (
  SELECT max(slate_date) AS slate_date
  FROM mlb.today_slate_rows
),
hist AS (
  SELECT
    m.player_id::bigint AS player_id,
    max(m.player_name) AS player_name,
    lower(trim(m.prop_type)) AS prop_type,
    m.game_id::bigint AS game_id,
    m.game_date::date AS game_date,
    m.prop_value::numeric AS prop_value,
    m.line::numeric AS line,
    CASE
      WHEN lower(trim(m.over_under)) = 'over' AND lower(trim(m.outcome)) = 'win' THEN 1
      WHEN lower(trim(m.over_under)) = 'over' AND lower(trim(m.outcome)) = 'loss' THEN 0
      WHEN lower(trim(m.over_under)) = 'under' AND lower(trim(m.outcome)) = 'win' THEN 0
      WHEN lower(trim(m.over_under)) = 'under' AND lower(trim(m.outcome)) = 'loss' THEN 1
      ELSE NULL
    END::numeric AS over_hit_flag
  FROM mlb.model_training_props m
  JOIN active_slate a
    ON m.game_date::date < a.slate_date
   AND m.game_date::date >= date_trunc('year', a.slate_date::timestamp)::date
  WHERE m.player_id IS NOT NULL
    AND m.game_id IS NOT NULL
    AND m.prop_type IS NOT NULL
    AND m.prop_value IS NOT NULL
    AND m.line IS NOT NULL
    AND lower(trim(coalesce(m.prop_source, ''))) = 'mlb_api'
    AND lower(trim(coalesce(m.outcome, ''))) IN ('win', 'loss', 'push')
    AND lower(trim(coalesce(m.over_under, ''))) IN ('over', 'under')
  GROUP BY 1,3,4,5,6,7,8
),
ranked AS (
  SELECT
    h.*,
    row_number() OVER (
      PARTITION BY h.player_id, h.prop_type
      ORDER BY h.game_date DESC, h.game_id DESC
    ) AS rn
  FROM hist h
),
agg AS (
  SELECT
    player_id,
    max(player_name) AS player_name,
    prop_type,
    avg(prop_value) FILTER (WHERE rn <= 5) AS last_5_avg,
    avg(prop_value) FILTER (WHERE rn <= 10) AS last_10_avg,
    avg(prop_value) AS season_avg,
    avg(over_hit_flag) FILTER (WHERE rn <= 5 AND over_hit_flag IS NOT NULL) AS hit_rate_last_5,
    avg(over_hit_flag) FILTER (WHERE rn <= 10 AND over_hit_flag IS NOT NULL) AS hit_rate_last_10,
    avg(over_hit_flag) FILTER (WHERE over_hit_flag IS NOT NULL) AS hit_rate_season,
    stddev_pop(prop_value) FILTER (WHERE rn <= 10) AS stddev_last_10
  FROM ranked
  GROUP BY 1,3
),
hit_ordered AS (
  SELECT
    r.player_id,
    r.prop_type,
    r.game_id,
    r.game_date,
    r.over_hit_flag,
    lag(r.over_hit_flag) OVER (
      PARTITION BY r.player_id, r.prop_type
      ORDER BY r.game_date DESC, r.game_id DESC
    ) AS prev_hit
  FROM ranked r
  WHERE r.over_hit_flag IS NOT NULL
),
hit_grouped AS (
  SELECT
    h.*,
    sum(
      CASE WHEN h.prev_hit IS DISTINCT FROM h.over_hit_flag THEN 1 ELSE 0 END
    ) OVER (
      PARTITION BY h.player_id, h.prop_type
      ORDER BY h.game_date DESC, h.game_id DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS streak_group
  FROM hit_ordered h
),
streak AS (
  SELECT
    player_id,
    prop_type,
    CASE
      WHEN max(over_hit_flag) FILTER (WHERE streak_group = 1) = 1 THEN 'HOT'
      WHEN max(over_hit_flag) FILTER (WHERE streak_group = 1) = 0 THEN 'COLD'
      ELSE 'NEUTRAL'
    END AS streak_type,
    count(*) FILTER (WHERE streak_group = 1) AS streak_count
  FROM hit_grouped
  GROUP BY 1,2
),
scored AS (
  SELECT
    a.*,
    (a.stddev_last_10 / NULLIF(abs(a.last_10_avg), 0)) AS cv_last_10,
    (a.hit_rate_last_10 - a.hit_rate_season) AS baseline_delta
  FROM agg a
)
SELECT
  s.player_id,
  s.player_name,
  s.prop_type,
  s.last_5_avg,
  s.last_10_avg,
  s.season_avg,
  s.hit_rate_last_5,
  s.hit_rate_last_10,
  s.hit_rate_season,
  s.stddev_last_10,
  s.cv_last_10,
  round(
    (
      100.0 * (
        1.0 - percent_rank() OVER (
          PARTITION BY s.prop_type
          ORDER BY s.cv_last_10 NULLS LAST
        )
      )
    )::numeric,
    1
  ) AS consistency_score,
  coalesce(st.streak_type, 'NEUTRAL') AS streak_type,
  coalesce(st.streak_count, 0) AS streak_count,
  s.baseline_delta,
  CASE
    WHEN coalesce(st.streak_type, 'NEUTRAL') = 'HOT' AND coalesce(st.streak_count, 0) >= 3 THEN 'HOT'
    WHEN coalesce(st.streak_type, 'NEUTRAL') = 'COLD' AND coalesce(st.streak_count, 0) >= 3 THEN 'COLD'
    WHEN s.baseline_delta >= 0.10 THEN 'ABOVE_BASELINE'
    WHEN s.baseline_delta <= -0.10 THEN 'BELOW_BASELINE'
    ELSE 'NEUTRAL'
  END AS streak_context_label
FROM scored s
LEFT JOIN streak st
  ON s.player_id = st.player_id
 AND s.prop_type = st.prop_type;

CREATE UNIQUE INDEX idx_today_player_context_key
  ON mlb.today_player_context (player_id, prop_type);

-- ---------------------------------------------------------------------------
-- STAGE 4: Frontend-serving workspace
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mlb.today_workspace_mlb AS
WITH joined AS (
  SELECT
    ms.game_date,
    ms.game_id,
    ms.player_id,
    ms.player_name,
    ms.team,
    ms.opponent,
    ms.is_home,
    ms.prop_type,
    ms.line,
    ms.best_over_price,
    ms.best_under_price,
    ms.best_over_book,
    ms.best_under_book,
    ms.market_median_over_price,
    ms.market_median_under_price,
    ms.market_range_over,
    ms.market_range_under,
    ms.book_count_over,
    ms.book_count_under,
    ms.price_dispersion_over,
    ms.price_dispersion_under,
    ts.open_over_price,
    ts.open_under_price,
    ts.latest_over_price,
    ts.latest_under_price,
    ts.minutes_since_open,
    ts.num_snapshots,
    ts.over_price_change_from_open,
    ts.under_price_change_from_open,
    ts.over_span,
    ts.under_span,
    pc.streak_context_label,
    pc.consistency_score,
    pc.last_5_avg,
    pc.last_10_avg,
    pc.season_avg,
    pc.hit_rate_last_5,
    pc.hit_rate_last_10,
    pc.hit_rate_season,
    pc.streak_type,
    pc.streak_count,
    pc.baseline_delta
  FROM mlb.today_market_snapshot ms
  LEFT JOIN mlb.today_market_timing_signal ts
    ON ms.player_id = ts.player_id
   AND ms.game_id = ts.game_id
   AND ms.prop_type = ts.prop_type
   AND ms.line = ts.line
  LEFT JOIN mlb.today_player_context pc
    ON ms.player_id = pc.player_id
   AND ms.prop_type = pc.prop_type
),
side_rows AS (
  SELECT
    j.game_date,
    j.game_id,
    j.player_id,
    j.player_name,
    j.team,
    j.opponent,
    j.is_home,
    j.prop_type,
    j.line,
    'OVER'::text AS side,
    j.best_over_price AS best_price,
    j.best_over_book AS best_price_book,
    j.market_median_over_price AS market_median,
    j.market_range_over AS market_range,
    j.open_over_price AS open_price,
    j.latest_over_price AS latest_price,
    j.over_price_change_from_open AS price_change_from_open,
    j.book_count_over AS book_count,
    j.price_dispersion_over AS price_dispersion,
    j.over_span AS intraday_span,
    j.minutes_since_open,
    j.num_snapshots,
    j.streak_context_label,
    j.consistency_score,
    j.last_5_avg,
    j.last_10_avg,
    j.season_avg,
    j.hit_rate_last_5,
    j.hit_rate_last_10,
    j.hit_rate_season,
    j.streak_type,
    j.streak_count,
    j.baseline_delta
  FROM joined j
  UNION ALL
  SELECT
    j.game_date,
    j.game_id,
    j.player_id,
    j.player_name,
    j.team,
    j.opponent,
    j.is_home,
    j.prop_type,
    j.line,
    'UNDER'::text AS side,
    j.best_under_price AS best_price,
    j.best_under_book AS best_price_book,
    j.market_median_under_price AS market_median,
    j.market_range_under AS market_range,
    j.open_under_price AS open_price,
    j.latest_under_price AS latest_price,
    j.under_price_change_from_open AS price_change_from_open,
    j.book_count_under AS book_count,
    j.price_dispersion_under AS price_dispersion,
    j.under_span AS intraday_span,
    j.minutes_since_open,
    j.num_snapshots,
    j.streak_context_label,
    j.consistency_score,
    j.last_5_avg,
    j.last_10_avg,
    j.season_avg,
    j.hit_rate_last_5,
    j.hit_rate_last_10,
    j.hit_rate_season,
    j.streak_type,
    j.streak_count,
    j.baseline_delta
  FROM joined j
),
scored AS (
  SELECT
    s.game_date,
    s.game_id,
    s.player_id,
    s.player_name,
    s.team,
    s.opponent,
    s.is_home,
    s.prop_type,
    s.line,
    s.side,
    s.best_price,
    s.best_price_book,
    s.market_median,
    s.market_range,
    (s.best_price - s.market_median) AS value_vs_market,
    s.open_price,
    s.latest_price,
    s.minutes_since_open,
    s.num_snapshots,
    s.price_change_from_open,
    s.book_count,
    s.price_dispersion,
    CASE
      WHEN s.best_price IS NULL OR s.market_median IS NULL THEN 'UNRELIABLE'
      WHEN coalesce(s.book_count, 0) < 2 THEN 'THIN'
      WHEN coalesce(s.num_snapshots, 0) <= 1 THEN 'LIMITED'
      WHEN s.market_range IS NULL THEN 'LIMITED'
      WHEN s.market_range >= 120 THEN 'LIMITED'
      WHEN coalesce(s.book_count, 0) >= 4
        AND coalesce(s.num_snapshots, 0) >= 3
        AND s.market_range <= 40
        THEN 'STRONG'
      WHEN coalesce(s.book_count, 0) >= 3
        AND coalesce(s.num_snapshots, 0) >= 2
        AND s.market_range <= 80
        THEN 'GOOD'
      ELSE 'LIMITED'
    END AS coverage_quality_label,
    CASE
      WHEN s.best_price IS NULL OR s.market_median IS NULL THEN 'No reliable median'
      WHEN coalesce(s.book_count, 0) < 2 THEN 'Few books available'
      WHEN coalesce(s.num_snapshots, 0) <= 1 THEN 'Sparse snapshot coverage'
      WHEN s.market_range IS NULL THEN 'Incomplete market range'
      WHEN s.market_range >= 120 THEN 'Wide market spread'
      WHEN coalesce(s.book_count, 0) >= 4
        AND coalesce(s.num_snapshots, 0) >= 3
        AND s.market_range <= 40
        THEN 'Median available across multiple books with tight range'
      WHEN coalesce(s.book_count, 0) >= 3
        AND coalesce(s.num_snapshots, 0) >= 2
        AND s.market_range <= 80
        THEN 'Median available with solid book and snapshot coverage'
      ELSE 'Partial market coverage'
    END AS coverage_quality_reason,
    CASE
      WHEN coalesce(s.intraday_span, 0) >= 25 THEN 'VOLATILE'
      WHEN coalesce(s.price_change_from_open, 0) >= 10 THEN 'LOW CONFIDENCE'
      WHEN coalesce(s.price_change_from_open, 0) <= -10 THEN 'EARLY'
      ELSE 'STABLE'
    END AS timing_signal,
    CASE
      WHEN coalesce(s.intraday_span, 0) >= 25 THEN 'Large intraday movement'
      WHEN coalesce(s.price_change_from_open, 0) >= 10 THEN 'Market has moved materially from the open'
      WHEN coalesce(s.price_change_from_open, 0) <= -10 THEN 'Market is still forming'
      ELSE 'Little intraday movement'
    END AS timing_reason,
    s.streak_context_label,
    s.consistency_score,
    s.last_5_avg,
    s.last_10_avg,
    s.season_avg,
    s.hit_rate_last_5,
    s.hit_rate_last_10,
    s.hit_rate_season,
    s.streak_type,
    s.streak_count,
    s.baseline_delta
  FROM side_rows s
)
SELECT
  sc.*,
  CASE
    WHEN sc.value_vs_market IS NULL THEN 'Neutral'
    WHEN abs(sc.value_vs_market) >= 40
      AND sc.coverage_quality_label IN ('GOOD', 'STRONG')
      AND sc.timing_signal IN ('STABLE', 'EARLY')
      THEN 'Strong signal'
    WHEN abs(sc.value_vs_market) >= 25
      AND (
        sc.timing_signal = 'VOLATILE'
        OR sc.coverage_quality_label IN ('LIMITED', 'THIN')
      )
      THEN 'Monitor'
    ELSE 'Neutral'
  END AS decision_label,
  CASE
    WHEN sc.value_vs_market IS NULL THEN 'Neutral: no clear market signal'
    WHEN abs(sc.value_vs_market) >= 40
      AND sc.coverage_quality_label IN ('GOOD', 'STRONG')
      AND sc.timing_signal IN ('STABLE', 'EARLY')
      THEN 'Strong market signal: notable price gap with reliable/stable coverage'
    WHEN abs(sc.value_vs_market) >= 25
      AND sc.timing_signal = 'VOLATILE'
      THEN 'Monitor: notable price gap, but market is volatile'
    WHEN abs(sc.value_vs_market) >= 25
      AND sc.coverage_quality_label IN ('LIMITED', 'THIN')
      THEN 'Monitor: notable price gap, but market coverage is limited'
    ELSE 'Neutral: no clear market signal'
  END AS decision_reason
FROM scored sc;

CREATE UNIQUE INDEX idx_today_workspace_mlb_key
  ON mlb.today_workspace_mlb (player_id, game_id, prop_type, line, side);

COMMIT;
