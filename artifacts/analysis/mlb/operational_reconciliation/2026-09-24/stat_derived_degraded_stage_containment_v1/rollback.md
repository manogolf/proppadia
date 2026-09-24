# Rollback

Pre-change installed wrapper SHA-256:

`10ee0b9f85a5529f51c13237aef8d6c6325c5cfab2ae51093e6b3c977015059f`

Authorized post-change SHA-256:

`3229ba332c391a7ad019d1c020377dae5539d914fe79227a62ef1cd28456e74c`

After first confirming no MLB wrapper is active, reverse only the recorded patch:

```sh
cd /Users/jerrystrain/bin
patch -R -p1 < /Users/jerrystrain/Projects/proppadia/artifacts/analysis/mlb/operational_reconciliation/2026-09-24/stat_derived_degraded_stage_containment_v1/installed_wrapper.patch
zsh -n /Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh
shasum -a 256 /Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh
```

The resulting hash must equal the pre-change hash above. Removing the tracked coordinator/contract should occur only in the same reviewed rollback commit. Retained degraded-run receipts are operational evidence and must not be deleted.
