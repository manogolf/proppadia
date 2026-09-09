# Manual prospective runbook

This study is intentionally not attached to a scheduler. Set credentials outside command text. Never pass a key in a URL or command-line argument.

For a game date after its immutable daily prediction snapshot exists:

```bash
.venv/bin/python -m backend.mlb.scripts.run_mlb_market_strong_agreement_separation_prospective_v1 \
  --ingest-predictions --through-date YYYY-MM-DD --report
```

Before a charged request, obtain separate authorization for a total study ceiling. One successful date request is expected to cost 10 credits. The first ceiling supplied becomes immutable in the append-only study ledger:

```bash
.venv/bin/python -m backend.mlb.scripts.run_mlb_market_strong_agreement_separation_prospective_v1 \
  --acquire-date YYYY-MM-DD --authorized-credit-ceiling AUTHORIZED_TOTAL --report
```

If parsing fails after a successful response was preserved, reconcile it without requesting it again:

```bash
.venv/bin/python -m backend.mlb.scripts.run_mlb_market_strong_agreement_separation_prospective_v1 \
  --reconcile-date YYYY-MM-DD --report
```

After official grading exists:

```bash
.venv/bin/python -m backend.mlb.scripts.run_mlb_market_strong_agreement_separation_prospective_v1 \
  --grade --through-date YYYY-MM-DD --report
```

Validate the package without network access or credential access:

```bash
.venv/bin/python -m backend.mlb.scripts.validate_mlb_market_strong_agreement_separation_prospective_v1
```

The date request must not be repeated after success. A transport interruption with unknown charge is terminal pending manual quota reconciliation. An HTTP retry is allowed only when the preserved `x-requests-last` value is exactly zero and the frozen total ceiling still permits the next expected request.
