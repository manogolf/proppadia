# MLB Supabase Estate Triage

This was a read-only catalog and repository-caller pass. No DDL or DML was executed.

- Objects inventoried and classified: 1039
- Trusted source: 2
- Active operational: 40
- Shared certified infrastructure: 0
- Derived research quarantine: 99
- Dead orphan: 0
- Unknown fail closed: 898
- Excluded from clean-room access: 99.81%

## Decision

The source layer is partially trustworthy, but clean-room activation is blocked by the
listed immutable-odds, official-lineup, team-dimension, and backup-verification gaps.
The generated schema and role SQL are plans only and were not executed.

Ten active pg_cron entries target absent retired `public.*` materialized views. They
are stale scheduled callers, not proof of an active source. No cron changes were made.
