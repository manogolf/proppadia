# MLB canonical outcome summary conflict lineage

- Task ID: `MLB_CANONICAL_OUTCOME_SUMMARY_IDEMPOTENCY_REPAIR_V1`
- Review date: `2026-08-20`
- Scope: canonical prospective player-prop outcome reconciliation only
- Final decision: `CANONICAL_OUTCOME_SUMMARY_IDEMPOTENCY_REPAIR_VALIDATED`

## Pre-repair reproduction

The unmodified reconciler was run against the retained August 18 and August 19 inputs. Both commands passed the exact-completeness gate and the immutable CSV comparison, then exited 1 at `IMMUTABLE_OUTCOME_SUMMARY_CONFLICT`.

| Slate | Frozen identities | Resolved | Unresolved | Outcome CSV SHA-256 | Stored completeness SHA-256 | Current completeness SHA-256 |
|---|---:|---:|---:|---|---|---|
| 2026-08-18 | 742 | 694 | 48 | `c1f7a13401a82289780042eecd85f93387a91d273c7fbda510ef550fab2c62f6` | `b5cdf139c60e37eb459c4539d51f2fd110f7ecddac511d7a82af658aa7d0eb96` | `bfa928ac8de7e49eff99c3e5ff16483fad7ea011a7958b0d6ad855863a38df11` |
| 2026-08-19 | 752 | 710 | 42 | `62840632ff743ff8f2d7fce61a514e16077d1d25699472173bb5710bab278b1a` | `44564c212cb50ff2a648c3d367ba35d75f433db9827a097d35603c9a3fdd34c8` | `7f58fa5e48962d211b7c9f7aeed6447b1228086b35a893f327d9efd1932ad0c6` |

For both dates, the frozen summary's prediction-ledger hash still matched the current ledger and its outcome-CSV hash still matched the current immutable CSV. The sole proposed-summary difference was the byte hash of the regenerated exact-completeness artifact. The canonical outcome CSV bytes already compared equal before the old summary check raised.

## Root cause

The reconciler treated the entire summary JSON byte stream as the immutable outcome identity. A later exact-completeness refresh can preserve the same completed games and canonical outcomes while changing the completeness artifact's file hash. That incidental input-file change made the full JSON encoding different and incorrectly escalated an already-current outcome set into a summary conflict.

## Repair contract

The immutable CSV comparator now builds a deterministic canonical outcome set:

- strict schema admission with explicit incidental timestamp-field allowance only;
- required canonical identity validation from game, player, prop, and normalized line;
- deterministic row sorting by canonical identity;
- stable decimal, integer, and null normalization;
- duplicate identities fail closed;
- a stable canonical JSON SHA-256 covers identity, date, linked prediction/model provenance, resolved status, official value, sample/conflict counts, selected-side result, and outcome contract.

Existing files are never rewritten. A logically identical set returns the existing file hash. Missing/extra identities, changed official values/statuses, changed linked prediction/model provenance, changed source contract, malformed schema, and duplicates remain fatal immutable-sidecar conflicts.

Summary reconciliation now compares outcome-derived semantic fields: slate and ledger identity, frozen/resolved/unresolved counts, duplicate count, prop/lane aggregates, outcome path, and the immutable outcome CSV hash. Run decision and regenerated input byte hashes remain recorded as provenance but no longer define canonical outcome equality. Material aggregate or outcome-file differences remain `IMMUTABLE_OUTCOME_SUMMARY_CONFLICT`.

## Post-repair production-date comparison

| Slate | Decision | Write action | Canonical set SHA-256 | CSV/summary bytes | CSV/summary mtimes |
|---|---|---|---|---|---|
| 2026-08-18 | `IMMUTABLE_OUTCOME_SUMMARY_ALREADY_CURRENT` | `NO_OP` | `3c5060c0094f64d358150420c6e65b89108d3706bb75e92ea40d8c47a79e0e1a` | unchanged | unchanged |
| 2026-08-19 | `IMMUTABLE_OUTCOME_SUMMARY_ALREADY_CURRENT` | `NO_OP` | `78c70c5f145d5dcc2555bf42f19248d8be5b660d725e5aa07d60e45308c7536a` | unchanged | unchanged |

Both comparisons exited 0. The daily wrapper treats exit 0 as `DONE`, so already-current reruns no longer emit the canonical reconciliation warning.

## Validation and isolation

- Focused and adjacent reconciliation/completeness suites: 66 passed.
- Required cases covered: first write; identical second pass; reordered rows; incidental metadata; true official-outcome change; missing/extra identity; duplicate identity; null/numeric normalization; stable semantic hash; zero mutation on no-op and conflict.
- A broader adjacent invocation had 69 passes and one unrelated existing date-sensitive failure in `test_prospective_lineage.py`, whose 2026-08-04 placeholder timestamp now fails its strict-current-pregame validator. The repair-specific and directly adjacent suites are green.
- The reconciler imports/reads only the player-prop prediction ledger, exact player-stat completeness evidence, and canonical player-stat outcome sources. It does not import or open moneyline lifecycle state, RAW totals shadow, or Totals C shadow.
- Hits fitted artifact remains `MLB_HITS_SEMANTIC_V1_2e7377b2cdcb` / `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`.
- Totals C candidate artifact remains `ea496a7a65d6ffad306238a46dd1279cf0cc81675c07f7447e9a48b511b4abfc` with its governed model/hash contract unchanged.
- No model fitting, shadow-regime transition, ledger rewrite, backfill, workflow run, or Git push occurred.
