# Points player-history challenger

This isolated shadow path exports the same slate population under three player-history contracts and scores all three with the same frozen Phoenix line models.

| Contract identity | SQL value | Player rolling history |
|---|---|---|
| `POINTS_PLAYER_HISTORY_CROSS_SEASON_V2` | `CROSS_SEASON` | Latest qualifying regular-season games across seasons, strictly before target game |
| `POINTS_PLAYER_HISTORY_120_DAY_LEGACY_SHADOW` | `LEGACY_120_DAY` | Previous 120-day bounded player-log population, strict-prior game filter, then last N rows |
| `POINTS_PLAYER_HISTORY_CURRENT_SEASON_ONLY_SHADOW` | `CURRENT_SEASON_ONLY` | Latest qualifying regular-season games in the target game's canonical season only |

All arms share target player/game identities, `is_home`, team history, current-season-to-date totals, strict-prior dates, model files, scorer, and lines. The legacy arm uses the former slate-date minus 120-day cutoff. `last10_team_sog_share` is considered a player-history-dependent field because its numerator comes from the player rolling window; its team denominator remains unchanged.

## Capture a future slate

For each arm, execute the read-only exporter once. `COPY TO STDOUT` writes the result to a local CSV; it does not mutate database state.

```bash
psql "$SUPABASE_DB_URL" --no-psqlrc -v ON_ERROR_STOP=1 \
  -v slate_date=YYYY-MM-DD -v points_history_contract=CROSS_SEASON \
  -f backend/nhl/sql/export_points_history_challenger.sql > /tmp/points_cross_season.csv

psql "$SUPABASE_DB_URL" --no-psqlrc -v ON_ERROR_STOP=1 \
  -v slate_date=YYYY-MM-DD -v points_history_contract=LEGACY_120_DAY \
  -f backend/nhl/sql/export_points_history_challenger.sql > /tmp/points_legacy_120_day.csv

psql "$SUPABASE_DB_URL" --no-psqlrc -v ON_ERROR_STOP=1 \
  -v slate_date=YYYY-MM-DD -v points_history_contract=CURRENT_SEASON_ONLY \
  -f backend/nhl/sql/export_points_history_challenger.sql > /tmp/points_current_season_only.csv

.venv/bin/python backend/nhl/scripts/score_nhl_points_history_challenger.py \
  --as-of-date YYYY-MM-DD \
  --cross-season-csv /tmp/points_cross_season.csv \
  --legacy-120-day-csv /tmp/points_legacy_120_day.csv \
  --current-season-only-csv /tmp/points_current_season_only.csv \
  --output-dir artifacts/analysis/nhl/points_history_challenger/YYYY-MM-DD
```

The scorer checks exact game/player identity parity and equality of every shared non-player-history column. It writes immutable per-arm raw predictions, model/scorer/input hashes, and no PAVA or market attachment. Its output directory is create-only. The first production cycle can be captured with these commands; no slate has been run by this setup task.

## Optional fourth arm

`CROSS_SEASON_RECENCY_WEIGHTED_PLAYER_HISTORY` is design-only. A deterministic decay weight can be applied to prior-season observations while retaining current-season observations at unit weight, but this changes feature construction semantics and needs a frozen decay schedule and prospective evidence before scoring. The current SQL architecture could express it cleanly, but it is not included because selecting the decay function without evidence would add an arbitrary model input transformation.
