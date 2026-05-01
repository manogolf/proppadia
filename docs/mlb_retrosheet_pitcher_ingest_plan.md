# MLB Retrosheet Pitcher Ingest Plan

This is an additive historical data foundation for pitcher prop modeling and reporting.

Source roles:

- Retrosheet is the historical backbone for pitcher game logs.
- Chadwick Register is the ID bridge from Retrosheet player IDs to MLBAM IDs.
- MLB Stats API remains the live/current-season source, but it should not be the only historical backfill source.

Initial scope:

- Build one row per pitcher per game.
- Focus on pitcher prop backbone fields: outs, strikeouts, walks, hits allowed, earned runs, runs, home runs, batters faced, starter flag, and game-finished flag.
- Prefer Retrosheet/Chadwick-derived pitcher box-score CSVs as input.
- Raw Retrosheet event files (`*.EVN` / `*.EVA`) are preview-only for now. Exact earned-runs attribution should come from Chadwick/Retrosheet box-score output before DB writes.
- Write only `tmp/retrosheet_pitcher_game_logs_sample.csv` until the CSV is inspected.
- Header-only output requires `--allow-empty`; missing inputs should fail clearly.

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
make mlb-download-retrosheet-sources

python -m backend.mlb.scripts.ingest_retrosheet_pitcher_game_logs \
  --retrosheet-gamelogs-dir backend/mlb/data/raw/retrosheet/csv_downloads \
  --chadwick-register-csv backend/mlb/data/raw/retrosheet/chadwick_register/people.csv \
  --season 2024 \
  --limit-games 25 \
  --out-csv tmp/retrosheet_pitcher_game_logs_sample.csv
```

If only raw event files are available, the script fails clearly by default. For a rough parser preview only:

```bash
python -m backend.mlb.scripts.ingest_retrosheet_pitcher_game_logs \
  --retrosheet-events-dir backend/mlb/data/raw/retrosheet/event_files \
  --chadwick-register-csv backend/mlb/data/raw/retrosheet/chadwick_register/people.csv \
  --season 2024 \
  --limit-games 25 \
  --allow-event-parser-preview \
  --out-csv tmp/retrosheet_pitcher_game_logs_sample.csv
```

Validation counters printed by the script:

- `files_seen`
- `games_seen`
- `pitcher_rows_written`
- `mapped_mlbam_count`
- `unmapped_retrosheet_count`
