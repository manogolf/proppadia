# Explicit Non-Goals

This design does not authorize or attempt to:

- create executable SQL or a database migration;
- create `mlb.game_phase_authority_v1` or any view;
- modify, move, rename, delete, quarantine, or rewrite the rejected migration;
- edit historical migration, rehearsal, installation, or preflight evidence;
- update `mlb.game_info`;
- update `mlb_cleanroom_v1.games`;
- disable any append-only trigger;
- backfill any database;
- change a production consumer;
- change a writer or scheduler;
- acquire schedule/feed data;
- call StatsAPI or a paid provider;
- install PostgreSQL or any other software;
- change models, features, artifacts, probabilities, thresholds, predictions, outcomes, or wagers;
- close the regular season;
- activate postseason collection or publication;
- define a 2027 feature-use policy;
- create a database observation ledger without new evidence that one is required;
- treat file-backed authority as perpetually current;
- infer phase from calendar date, status, league expectations, market presence, or model participation;
- grant implementation or deployment approval.

The package is ready only for architecture and cutover-plan review.
