# NHL Historical PP-TOI Recovery Execution — 2026-10-16

## Result

The operator-authorized prepared transaction committed **71,627** PP-TOI
corrections for the `202302` and `202402` game-ID prefixes. An independent
post-commit export confirms that exactly those rows and only the
`pp_toi_minutes` field changed. The guarded retry committed with **zero**
additional updates. The rollback was validated but not executed.

| Verification | Result |
| --- | ---: |
| Changed PP-TOI values | 71,627 |
| Positive-to-positive corrections | 50,724 |
| Positive-to-verified-zero corrections | 20,903 |
| Previously correct rows unchanged | 1,605 |
| Unresolved raw rows unchanged | 18,016 |
| Missing raw identities excluded | 195 |
| Checkpoint-only identities excluded | 51 |
| Other raw fields or keys changed | 0 |
| Protected 2025 rows unchanged | 46,713 |
| Protected 2026 rows unchanged | 4,248 |
| Guarded retry updates | 0 |

The 18,016 unresolved raw rows cover all 502 rejected games. Their values were
not changed. The 195 missing raw identities and 51 checkpoint-only identities
were not inserted.

## Preflight and transaction evidence

The preflight reviewed `docs/PROJECT_DOCTRINE.md`,
`docs/CANONICAL_IDENTITY_DOCTRINE.md`, and
`docs/contracts/missingness_and_zero_semantics.md`. The candidate, prepared SQL,
and official source evidence were validated before execution:

- Candidate SHA-256: `0bd6953c4152e3b29b0bc1af324816f5916d86066d494559a58f492eed4dcfef`.
- Forward SQL SHA-256: `6b7eea80d7347869ee9b9f46d2948c653c60c61b7c9f0a53a3e800bda976177c`.
- Rollback SQL SHA-256: `be5781ebd992b29343becd547bcbf79e90bce94c59b67f2300fda44ba0c90064`.
- Official acquisition manifest SHA-256: `625f59f6d29fd7a54a2b4a5df7a630c2968f6e8bab36ad2f0b6bf88c6b7e22f2`; its self-excluding digest is `e7f9117810e6d40e0a9b791a3936f04cd827c57e6fcb54c11c0b1d3a1e694034`.
- All 6,117 retained PBP, boxscore, and shiftchart responses used by the 71,627 targets were hash-checked against the reconciled game receipts. The 2,039 target games were final and reconciled by both shift-overlap methods.
- Candidate target player and game identities matched the retained official reconstructions. Historical internal team IDs were decoded through the documented legacy ID-to-abbreviation map and compared with official NHL team identities; all target team abbreviations and home/away sides agreed.
- Immediate pre-write checks found no competing NHL daily, reconciliation, fill, or repair process and no active NHL-related database session. The raw table had no non-internal triggers.

The forward SQL inserted 71,627 guarded candidate rows, changed 71,627 database
rows, verified every post-update value against the official candidate, and
committed. The retry inserted the same temporary candidates, passed its
original-or-already-correct value guard, changed zero rows, and committed.

## Before/after evidence

Run-bound files are under
`artifacts/operational/nhl/pp_toi_recovery_execution/run_id=pp_toi_recovery_20261016T152648Z/`.

- `before_raw.csv`: full raw rows for the 2023/24 repair prefixes plus protected 2025/26 prefixes; 142,209 unique player-game keys; SHA-256 `732129de985e27033bd8106d963b3a447ffd371b384cef0b88d09f2c95be7100`.
- `after_raw.csv`: same 142,209 keys and fields; SHA-256 `a01c8521af57e1ff16b1dada1619dc7dcc75185dc59fb6710f1c1c28762a15bb`.
- `after_retry_raw.csv`: byte-identical to `after_raw.csv`; SHA-256 `a01c8521af57e1ff16b1dada1619dc7dcc75185dc59fb6710f1c1c28762a15bb`.
- `changed_rows.csv`: the 71,627 actual before/official/after values with source hashes; SHA-256 `f3c915cac682bd8e82cf08efee37e883b32623e874f62aa8080aeb906ef24ac3`.
- `transaction_receipt.json`: observed forward and retry psql output, exit statuses, candidate/source/SQL hashes, and transaction dispositions; SHA-256 `f08e9f66e4058909f5a20e441738b5a8c9729e077a5562ed20d581ef2fe647d5`.
- `postcommit_verification.json`: row-level comparison summary; SHA-256 `f9ef3687a9715cae9bc4f70879b63bbecba398e4226666326dc2acc5e608a9bf`.
- `SHA256SUMS`: checksums for the run-bound candidate, SQL, manifest, snapshots, row delta, and transaction logs; SHA-256 `0ce64e607eb62e3683a21dc2fba3da989ec20dd4389c9215ec61d6232bf9a566`.

The independent comparison found the exact target key set and exactly 71,627
field changes. Every changed value equals the candidate's independently
reconstructed official PP-TOI value. The 20,903 zero replacements are stored as
numeric zero. All other columns and identities in the snapshots are unchanged.
The complete 2025 and 2026 prefix rows match before and after.

The SQL touches only `nhl.skater_game_logs_raw.pp_toi_minutes`, names only the
exact candidate keys, and no triggers exist on that table. It does not update
prediction tables or model files; immutable predictions and fitted models were
not rewritten, rescored, or retrained. The rollback SQL contains the same exact
71,627-key set and current-value guards; rollback was not executed.

## Durability check

Normal current-slate daily operation is date-scoped and cannot target these
2023/24 game IDs. A historical date-specific rerun remains a potential overwrite
path:

- `seed_skater_logs_for_date.py` refreshes staged PP-TOI from its source response,
  preserving explicit staged zero and clearing a stale staged value for NULL.
- The postgame promoter uses
  `preserve_existing_pp_toi_conflict_assignment`, which keeps raw PP-TOI when
  staged input is NULL but accepts any non-NULL staged value. It does not compare
  that value with this recovery's official shift reconstruction. A historical
  postgame replay could therefore replace a recovered positive or zero if its
  staged value differs.
- `fill_pp_toi_minutes_for_date.py` reads manpower-segment/shiftchart overlap
  and updates raw only where PP-TOI is NULL or zero. It cannot replace a
  recovered positive, but a historical rerun can replace a recovered verified
  zero with a positive overlap value.

The regular daily command passes its current slate date to these operations, so
the identified historical risk requires a date-specific replay/backfill. This
task made no code or permission changes. Historical recovery values are verified
and preserved by this transaction, but they are **not immutable against every
explicit historical rerun**. That overwrite path remains a follow-up prevention
item.

## Validation and limits

`./bin/python -m unittest backend.nhl.tests.test_nhl_historical_pp_toi_recovery`
passed all 5 focused tests. The source-validation preflight checked the complete
candidate, the official source hashes for every correction target, identity
bindings, and exact counts. No daily pipeline, odds acquisition, model
operation, or rollback ran. The broader NHL Points foundation investigation
remains open.
