# NHL 2025 Shot-Attempt Recovery Execution

Status: **COMMITTED: 34,108 verified attempts corrected; all protected checks passed.**

On 2026-10-16 UTC, the operator-authorized prepared forward SQL committed the 2025 `202502` correction. It changed only `nhl.skater_game_logs_raw.shot_attempts` for the exact candidate identities. The transaction reported 34,108 updates. A guarded repeat committed with zero updates and an empty returned change set. The rollback was validated against the candidate and was not executed.

| Verification | Result |
| --- | ---: |
| Corrected targets | 34,108 |
| Official positive counts | 31,381 |
| Verified zeros | 2,727 |
| Unresolved rows unchanged | 6,368 |
| Other raw fields changed | 0 |
| Historical prediction rows changed | 0 |
| 2026 prefix rows changed | 0 |
| Idempotency retry updates | 0 |

The pre-update full 2025 export had 46,713 rows and exactly matched the retained candidate keys and captured source fields. The update SQL's embedded target set and unresolved guard set were independently compared row-for-row to the candidate. The 2026 prefix snapshot retained all 3,852 rows and its hash is identical before and after both transactions. The full 2025 prediction-row JSON export has 442,917 rows and is byte-identical before and after. In the 2025 raw snapshots, the only changed field was `shot_attempts` on exactly the 34,108 target rows. All target values now equal the official PBP counts; the 6,368 unresolved values remain unchanged.

## Run evidence

Run-bound evidence is under `artifacts/operational/nhl/shot_attempts_recovery/nhl_shot_attempts_recovery_20261016_86b259fe/`:

- `recovery_transaction_receipt.json` — machine-readable transaction and verification receipt; SHA-256 `3e4f9238f93ab0817c36eccf68ec246e54c291abd1bd257942c2cb63b96df3ff`.
- `before_202502.csv`, `after_202502.csv`, `after_retry_202502.csv` — full raw before/after exports; before SHA-256 `d59c4dd405b53bce124706a7068b90de4633bfa8e3e3b5fe2c451129062cd690`, after and retry SHA-256 `55fb3af304c1ba575e6ce828a5d9c642daa6486ece6b7b658f1d12a7183e298d`.
- `before_202602.csv`, `after_202602.csv`, `after_retry_202602.csv` — 2026 scope-protection exports; all SHA-256 `3a47ef30b71fad166673a813cbd026dfe1cbe8eaef137eec1596aaf7fa327889`.
- `predictions_before_202502.csv`, `predictions_after_202502.csv`, `predictions_after_retry_202502.csv` — 442,917 historical prediction rows; all SHA-256 `25a28ab42ee5e7ca32fe7686120c825f5d201d5d4bb8860f498f2a1571e8c106`.
- `changed_rows.csv` — actual transaction-returned before/after values and per-game source hashes for all 34,108 updates; SHA-256 `bfc99fa55b5e3a55b4bcd8bcb4c036a4559163926e54dba413c3184174d9f849`.
- `forward_transaction.log` and `idempotency_retry.log` — transaction receipts; they report 34,108 and zero updated rows respectively.
- `preflight_verification.json` and `SHA256SUMS` — independent checks and file checksums. All entries in `SHA256SUMS` verified successfully.

The Oct. 10, 2025 checkpoint, original recovery snapshot, candidate and official-source responses were not modified. No SOG, PP TOI, model artifacts, authority, prediction history, or unresolved observations were changed. No daily scoring or paid provider calls occurred. The broader raw-foundation and Points investigations remain open.
