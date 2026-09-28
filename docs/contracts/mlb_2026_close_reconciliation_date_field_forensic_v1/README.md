# MLB close-reconciliation date-field forensic V1

This offline report investigates only the four date conflicts and sixteen feed
gaps named in close inventory V2. It reads schedule artifacts through the
V1-pinned source manifests and searches only V2's documented feed roots:

- `artifacts/analysis/mlb/player_stats_completeness`
- `artifacts/ops/mlb_stat_derived_natural_run_evidence_v1`

The report records exact source-file SHA-256 values and bounded-root feed-file
inventory hashes. UTC-to-Eastern conversion is diagnostic only; StatsAPI
`officialDate` remains the authoritative date field. No close inventory or
season-close state is modified.

Validate offline with:

```sh
.venv/bin/python -m backend.mlb.scripts.validate_mlb_2026_close_reconciliation_date_field_forensic_v1
```
