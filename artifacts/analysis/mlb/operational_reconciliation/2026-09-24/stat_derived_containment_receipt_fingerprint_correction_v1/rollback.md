# Rollback

Installed-wrapper pre-change SHA-256:

`3229ba332c391a7ad019d1c020377dae5539d914fe79227a62ef1cd28456e74c`

Authorized post-change SHA-256:

`09d236d57eeff4576c97eac06c9fb8ad60a24f41e1091e1c3dccea6ead73faef`

After confirming that no MLB wrapper or stat-derived process is active, reverse only this correction:

```sh
cd /Users/jerrystrain/bin
patch -R -p1 < /Users/jerrystrain/Projects/proppadia/artifacts/analysis/mlb/operational_reconciliation/2026-09-24/stat_derived_containment_receipt_fingerprint_correction_v1/installed_wrapper.patch
zsh -n /Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh
shasum -a 256 /Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh
```

The resulting hash must be the pre-change hash above. Revert the bounded repository commit separately. Do not delete retained stage streams or historical receipts. The original September 24 receipt remains immutable evidence and is not part of rollback.
