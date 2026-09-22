# MLB 2026 Canonical Phase Local PostgreSQL 15 Rehearsal V1

This package records the authorized 2026-09-22 attempt to install keg-only Homebrew `postgresql@15` and execute the canonical-phase migration rehearsal locally.

The repository, architecture, disk, formula-major-version, keg-only, non-sudo, and Homebrew-prefix checks passed. Homebrew resolved `postgresql@15` 15.19, but the single authorized command failed because no bottle is available for the host's macOS 27 configuration. Homebrew suggested `--build-from-source`; that different command was not authorized and was not executed or retried.

The failed attempt installed no formula or dependency, created no PostgreSQL cluster, registered or started no service, and left `/opt/homebrew` and its Cellar at the same measured sizes. No operational database connection, provider request, or Python-environment change occurred.

Because no PostgreSQL 15 server became available, every intended runtime scenario remains an unexecuted blocking failure. The existing dependency-free tests and validators were re-executed and passed, but they do not substitute for the required database rehearsal.

The smallest next action is a separate authorization decision: either authorize one Homebrew source-build attempt for the same keg-only `postgresql@15` formula and its declared build dependencies, or provide a compatible already-installed PostgreSQL 15 server. Do not use the operational Supabase target.

See `installation_report.json` and `rehearsal_report.json` for exact results.
