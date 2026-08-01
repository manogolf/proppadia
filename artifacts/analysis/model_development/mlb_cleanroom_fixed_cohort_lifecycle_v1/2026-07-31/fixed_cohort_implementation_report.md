# Fixed-cohort lifecycle v1 implementation

`MLB_CLEANROOM_BOL_TB15_FIXED_COHORT_V1` is ready. The operator needs one atomic capture-and-freeze command in the 12:45–1:15 PM Pacific window and one closeout command after games finish. Games must begin 15–180 minutes after the governing capture.

The successful cohort freezes baseline positions 1–9, rejected positions 1–3, and retained positions 4–9. Unconfirmed and unknown-order rows are exclusions. A zero-row cohort freezes immutably and counts as an attempt. A second successful invocation is refused before network access.

The closeout loads only frozen identities, uses exact official outcome joins and recomputed TB, settles authentic frozen Under odds at $5 risk, and is revisioned and idempotent. Missing results fail closed.

The research block stops at the first qualifying closed cohort after 100 baseline and 30 rejected actionable wagers, or after five valid attempted dates. Its terminal H1 rule is frozen. July 29–31 are excluded.

The installed 1:00 PM pipeline contains a default-disabled, nonblocking hook. No schedule was added. The first cohort was not run because implementation completed outside the allowed window. August 1 remains `POST_HARDENING_FIXED_COHORT_ATTEMPT_001`.
