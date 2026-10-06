# MLB Totals runtime phase overlay and wrapper exit receipt V1

## Totals schedule authority

The natural Totals lifecycle consumes the exact schedule response retained by
the current run's wide builder at
`backend/mlb/exports/provider_event_game_bindings/schedule_sources/<slate>/statsapi_schedule__<run-tag>.json`.
It does not issue a second schedule request. The bytes are hashed and loaded
through `RuntimeScheduleAuthority` over the active pinned authority. A game
outside the pinned date horizon is considered for admission only if that exact
gamePk occurs in the retained response with the requested `officialDate`; the
subsequent exact-game authority lookup and source-game-type consistency check
remain mandatory. Date or gameType alone never admits a game.

Prediction scoring, market attachment, grading, and Totals C reuse this
source-bound authority. Missing retained schedule evidence, hash mismatch,
ambiguous identity, or phase conflict remains fail-closed. Totals persistence
and grading continue to partition and validate exact game identities before
writing.

## Natural wrapper exit receipt

Each installed natural wrapper invocation writes one create-only artifact:

`artifacts/ops/mlb_wrapper_run_receipts/<start-date>/<run-identity>.json`

Schema `MLB_NATURAL_WRAPPER_RUN_RECEIPT_V1` records exact run identity,
start/end timestamps, `wrapper_rc`, status, and a payload SHA-256. Publication
uses a same-directory temporary file and atomic hard-link creation; a repeated
identity cannot overwrite its first receipt. Receipt failure is logged but does
not replace the wrapper's original exit status. The existing
`launchagent_daily_summary_latest.json` continues to be written unchanged.

The production wrapper is installed outside the repository at
`/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh`. Installation creates
the exact pre-change backup
`/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh.pre_wrapper_run_receipt_v1`.
Rollback is to restore that backup over the installed wrapper, then run
`zsh -n` on the restored file. The receipt artifacts already written are
append-only evidence and are not removed by rollback.
