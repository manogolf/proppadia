# Failure and recovery contract

Fail closed for model/state/source hash mismatch, unavailable official schedule, ambiguous game/team identity, failed state advancement, missing cutoff, post-start scoring, durable-write failure, or inconsistent authority. Never fall back to the archived baseline, retired prop model, stale August 4 state, unversioned files, or filesystem-only predictions.

- Missed run: retry before first pitch; never reconstruct after start.
- Duplicate invocation: identical payload is no-op; conflict stops.
- Delayed final: retain unresolved and retry grading later.
- Official correction: append correction evidence and rebuild a new state snapshot from initialization; preserve old snapshots/grades.
- Database outage: publish nothing; retry after recovery.
- Schedule change/postponement: use new exact gamePk/start only before first pitch; preserve rejection reason.
- Doubleheader reschedule: distinguish by gamePk, game number, and scheduled start.
