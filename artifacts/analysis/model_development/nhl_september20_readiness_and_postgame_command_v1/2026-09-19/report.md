# NHL September 20 readiness and postgame command

## SOG prediction-only boundary

The parent 900-second observer already invoked the SOG prediction-only hook before the morning/market gate. The hook itself duplicated the morning-readiness gate, so a failed morning receipt could still suppress SOG. That internal gate is removed. The resulting order is canonical database slate and feature export, pregame phase validation, request-free prediction construction, and only then the parent observer's separate morning/paid-market evaluation.

The SOG path does not read provider event IDs, market responses, prices, paid claims, or market credentials. The offline September 20 fixture generated D plus A/B/C/G rows with zero current-season TOI. `E_TEAM_CHANGE_AWARE` remains a historical diagnostic arm and is not emitted by the frozen operational contract; it was not silently added. The D selection, retrospective prohibition, canonical identity, roster/position, feature cutoff, and pregame timing gates are unchanged.

## Five September 19 observer receipts

All five receipts are resolved as `CODEX_VALIDATION_SMOKE_TEST`. The retained Codex session is `/Users/jerrystrain/.codex/sessions/2026/09/02/rollout-2026-09-02T07-27-52-01a06284-e9c3-7ef1-8897-ebecf48f0b49.jsonl`. At each matching time, a focused test launch executed `test_main_lock_failure_keeps_exit_zero`. That test supplied neither an isolated `--output-root` nor a successful morning receipt, so `record_morning_not_ready()` wrote a zero-call status into the production default directory. No recovery, manual capture, scheduler, or paid request caused the five receipts.

The test now supplies a temporary output root and an explicit validation identity. New observer receipts safely record origin, classification, validation/run identifier, PID/PPID, parent executable basename, invocation timestamp, and executable/script hashes. They never record command arguments or environment values.

## Governed postgame command

Read-only readiness:

```text
bin/nhl_postgame_reconcile.sh --preflight --date 2026-09-19
```

Later, only after every admitted game is an official final:

```text
bin/nhl_postgame_reconcile.sh --execute --date 2026-09-19
```

Omitting a mode defaults safely to preflight. Exit 0 means ready/complete, 2 means unfinished, 3 means identity or game-type refusal, 4 means lock busy, 5 means collection/integrity failure, and 6 means a conflicting retained outcome. Execute validates the complete official slate before mutation, invokes existing schedule, roster, box-score, shift-chart, manpower, TOI, and pairing collectors, promotes all retained skater and goalie outcome fields, and publishes a content-addressed append-only package. An identical second pass performs zero publication inserts and skips recollection. A conflicting outcome fails closed.

Moneyline and puck line remain `PRESEASON_NON_EVALUATION`; Points retains the run-summary timestamp qualification; Saves uses actual postgame participation and the frozen conditional-starter denominator; September 19 SOG is outcome-only with `NO_SEPTEMBER_19_PREDICTION_GRADE`.

At 2026-09-19 16:57:53 PT / 23:57:53 UTC, the live database-only preflight found seven canonical games, all retained as `SCHEDULED`, and returned exit 2. It made zero external requests and zero writes. Postgame execution was not started.
