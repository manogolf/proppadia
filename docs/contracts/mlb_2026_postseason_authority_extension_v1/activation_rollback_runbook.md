# Activation and rollback runbook

The active selector may name only a fully verified, governed child descriptor.
The offline script implements the documented compare-and-swap path:

1. Build the candidate with the existing versioned authority builder and the
   hash-pinned `source_set.json`.
2. Run the focused extension tests and verify that only the four expected
   gamePks were appended and all parent bytes are preserved.
3. Promote the candidate using `--promote`; this changes only the descriptor
   status and the child SHA manifest. Proposal and source-manifest bytes do not
   change.
4. Re-run focused tests and check the retained close artifact hash.
5. Activate once using `--activate`. The script requires the exact original V1
   selector bytes/hash, writes `activation_rollback.json` first, and atomically
   compare-and-swaps only `active_selection.json` to the reviewed V2 descriptor.
6. Run `--validate`, the versioned authority tests, the regular-season close
   inventory validator, and `git diff --check`.
7. Do not rerun the failed natural wrapper or Moneyline. Validate operational
   behavior only at the next naturally eligible window.

Rollback, if a later reviewed finding requires it, is:

```sh
.venv/bin/python -m backend.mlb.scripts.activate_mlb_postseason_phase_authority_v2 --rollback
```

Rollback restores the exact saved selector bytes only if the current selector
still equals the exact V2 child selector recorded at activation. A selector
changed by another owner causes a fail-closed compare-and-swap error. Neither
the child snapshot nor the regular-season close artifact is deleted or rewritten.
