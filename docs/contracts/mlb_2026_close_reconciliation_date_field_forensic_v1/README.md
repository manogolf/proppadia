# MLB close-reconciliation date-field forensic V1

This offline report investigates only the four date conflicts and sixteen feed
gaps named in close inventory V2. It reads schedule artifacts through the
V1-pinned source manifests plus the exact schedule-history artifacts listed in
the V2 relationship-evidence manifest. Feed searches are bounded to V2's
documented roots:

- `artifacts/analysis/mlb/player_stats_completeness`
- `artifacts/ops/mlb_stat_derived_natural_run_evidence_v1`

The report records exact source-file SHA-256 values and bounded-root feed-file
inventory hashes. The 824785 relationship is resolved by retained postponed
and final schedule appearances; 823489, 824703, and 824705 remain relationship
gaps. UTC-to-Eastern conversion is diagnostic only; StatsAPI
`officialDate` remains the authoritative date field. No close inventory or
season-close state is modified.

Validate offline with:

```sh
.venv/bin/python -m backend.mlb.scripts.validate_mlb_2026_close_reconciliation_date_field_forensic_v1
```
