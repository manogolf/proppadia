# MLB Retrosheet Pitcher Ingest Plan

This is an additive historical data foundation for pitcher prop modeling and reporting.

Source roles:

- Retrosheet is the historical backbone for pitcher game logs.
- Chadwick Register is the ID bridge from Retrosheet player IDs to MLBAM IDs.
- MLB Stats API remains the live/current-season source, but it should not be the only historical backfill source.

Initial scope:

- Build one row per pitcher per game.
- Focus on pitcher prop backbone fields: outs, strikeouts, walks, hits allowed, earned runs, runs, home runs, batters faced, starter flag, and game-finished flag.
- Use Retrosheet/Chadwick-derived pitcher box-score CSVs as input.
- Write only `tmp/retrosheet_pitcher_game_logs_sample.csv` until the CSV is inspected.

Out of scope for the first pass:

- Pitch-level modeling.
- Full event-level reconstruction.
- Production cron integration.
- Direct DB writes.

Proposed target table:

- `mlb.pitcher_game_logs_historical`
- Draft DDL: `backend/mlb/sql/migrations/20260501_proposed_pitcher_game_logs_historical.sql`

Sample command:

```bash
python -m backend.mlb.scripts.ingest_retrosheet_pitcher_game_logs \
  --pitching-csv path/to/chadwick_pitching_box.csv \
  --chadwick-register-csv path/to/chadwick-register.csv \
  --out-csv tmp/retrosheet_pitcher_game_logs_sample.csv
```
