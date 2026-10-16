# NHL Points pregame evidence capture activation

## Status

- Capture installed: **INSTALLED_AWAITING_LIVE_VERIFICATION**
- Corrected historical challenger inputs: **READY**
- Daily execution: not run for this task.

## Installation

The existing `backend/nhl/cli.py` daily export boundary now invokes a single `psql` session containing the unchanged `backend/nhl/sql/export_points.sql` plus read-only evidence queries. The session uses `REPEATABLE READ READ ONLY`. It writes the exact SQL export, slate target bindings, and bounded 120-day raw player-game rows from the same PostgreSQL snapshot. The D10 values are rebuilt from those rows, compared on exact `(game_id, player_id)` keys against the export at `1e-12`, and written with contributing team-game IDs, source counts, missingness, denominator, team identity, snapshot ID, and checksums. After `prepare_scoring_input`, the exact Phoenix scorer input is copied into the same run package and bound by hash.

The production export SQL and scoring calculations remain unchanged. If capture fails, the existing exporter call is used as a fallback; its bytes are retained if possible and the evidence status becomes `EVIDENCE_CAPTURE_PARTIAL` or `EVIDENCE_CAPTURE_FAILED`. Capture status, reason, path, and hashes are recorded in the Points lane receipt. The research capture does not add a daily blocking condition.

Operational Phoenix evidence packages will be written to:

`artifacts/operational/nhl/points_feature_evidence/season={season}/slate_date={slate_date}/run_id={parent_daily_run_id}/`

They contain `exact_feature_export.csv`, `phoenix_scorer_input.csv`, `target_bindings.csv`, `raw_prior_player_game_rows.csv`, `prior_team_game_aggregation.csv`, `target_team_shots.csv`, `manifest.json`, and `SHA256SUMS`.

## Phoenix and HGB lineage

Phoenix is the production SQL path: `skater_game_logs_raw` → `export_points.sql` → `train_nhl_points_v2.csv` → `prepare_scoring_input` → `score_nhl_points_with_lineage.py` / `score_points_phoenix.py`.

HGB uses a separate feature path: read-only official skater logs plus canonical slate and immutable official-outcome packages → `export_nhl_points_hgb_features.py` → HGB scorer. Its existing immutable HGB capture now retains the exact source logs, canonical slate, outcome inputs, team-game aggregation, per-target contributing-game evidence, generated feature file, source manifest, and checksums. Logs and slate are acquired inside a separate repeatable-read, read-only transaction. Official outcomes are immutable file inputs rather than rows from that database snapshot; their exact files and hashes are retained. This records that boundary instead of claiming one shared snapshot across both sources.

## Bounded integration check

A direct, bounded export check ran for the existing 2026-10-15 slate. It did not invoke the daily command, load predictions, request odds, or write to PostgreSQL. The transaction reported `repeatable read`, `read only = on`, and snapshot `17209071:17209071:`. Evidence capture returned `EVIDENCE_CAPTURE_COMPLETE`, with D10 reconstruction matching the exporter. The exact export SHA-256 was `dc41c8500f1db93738d25e113ffd71ebeb700ebe7e2c366158fc7a7612e7432e`, matching the existing daily run's retained Phoenix feature export hash. The bounded test package was temporary and removed after the check; this is not a normal-run receipt-bound capture.

Synthetic integration tests also exercise package reconstruction against the Phoenix scorer input and independently execute the HGB feature builder, confirming the two paths are distinct. All 21 focused tests pass. The next ordinary daily execution must still verify that both packages appear in the actual parent receipt and that the HGB source-input manifest validates end to end.

## Historical challenger readiness

The corrected 2023–24 training matrix (90,486 rows) and corrected 2025 holdout (47,239 rows) are ready. Exact row identities, labels, all non-team-shot features, and matrix row counts match their originals. Only `team_d10_sf_per_game` differs. The frozen incumbent identity is `a53f43d5caea9a64ee8e6d48daf8072e0332b629998242639b2ba15be743e5c2`; model SHA is `e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9`; scaler identity is `611685ae22c0c0a5883c6deeee1897cad9de268d796071b57f1695376995562d`. The preserved evaluation uses expanding prior-date monthly origins, with 2025-10-07 through 2026-04-16 as the 2025 holdout and monthly folds from 2025-10 through 2026-04. No model was trained.

Readiness details and matrix/column hashes are in `artifacts/analysis/nhl/points_historical_team_shots_reconstruction/2026-10-15/challenger_readiness.json`.

## Findings retained

- The historical team-shot feature defect remains material: 30,547 of 90,486 training rows changed under independent reconstruction.
- The controlled HGB challenger is justified, but has not been trained.
- The original 2026 team-shot discrepancy remains 14.907 captured mean versus 25.972 independently reconstructed mean; its first incorrect production transformation is still unconfirmed.
- Current `shot_attempts` NULLs remain a separate backlog item; no repair was performed.
- Production database writes: 0. Provider requests: 0. Paid credits: 0.
