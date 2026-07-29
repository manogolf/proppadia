# Clean-room Table Contracts

Status: **IMPLEMENTED**

Seven append-only tables were created: `teams`, `players`, `games`,
`lineup_snapshots`, `odds_snapshots`, `player_game_results`, and `ingestion_runs`.

All source rows carry a source payload SHA-256. Snapshot rows carry an ingestion-run
UUID. Exact keys use only MLB team, player, and game identifiers. Update and delete
operations are rejected by a trigger on every source table.

Four source-only views were created with deterministic latest-observation ordering:
`current_games`, `latest_lineups`, `latest_bol_tb15`, and
`official_completed_player_games`.
