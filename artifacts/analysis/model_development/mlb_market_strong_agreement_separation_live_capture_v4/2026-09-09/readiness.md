# Next-slate readiness

Status: **READY_FOR_NEXT_ELIGIBLE_SLATE (2026-09-10)**.

The existing 05:30 orchestration now launches the isolated capture immediately after the durable moneyline lifecycle returns successfully. The client independently re-verifies the committed row count and commit barrier, then fails closed unless request start is within 300 seconds. The live request is date-idempotent and the parent workflow does not wait for it. Tests use fixtures only; no credential value was read or tested and no Odds API request was made. Runtime credential presence remains an operational prerequisite and will be handled without persistence.
