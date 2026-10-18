# Repository operating instructions

- Before work, read applicable `AGENTS.md` files and the relevant project doctrine, contracts, and runbooks. This checklist points to those sources; their detailed policies remain authoritative.
- At task start, review backlog items germane to authorized work; resolve related items that fit naturally within scope and close them with evidence, record newly deferred items before finishing, and do not pursue unrelated backlog work or expand production authorization.
- Use `backend/.env` as the authoritative environment configuration. Never print, log, or include credentials or connection strings in output or artifacts.
- When an authorized external request fails on DNS or transport inside the sandbox, immediately request elevated network permission for that request and retry. Check elevation before diagnosing a Mac or provider outage.
- Preserve ordinary provider retry behavior; do not add arbitrary one-attempt limits.
- Protect populated historical source values. Routine operations must use their configured restricted database identity; never switch to owner-level access to bypass a failure.
- Before changing a database caller, inspect the relevant SQL schema, returned column aliases, actual Python result shape, and connection routing.
- Prefer narrow corrections to demonstrated defects. Avoid repeating completed historical audits, adding defensive frameworks, or building speculative infrastructure.
- Preserve unrelated worktree changes. Commit coherent completed corrections locally; never push without explicit authorization.
- Commit source code, tests, documentation, and small reproducibility manifests. Do not stage generated research datasets, especially CSV exports, unless explicitly authorized. Respect existing `artifacts/**` ignore rules; do not use `git add -f` to bypass ignored generated data without explicit authorization. Before committing, inspect staged paths and sizes. Keep large generated outputs local with regeneration instructions and checksums, and do not delete unrelated research artifacts to reduce commit size.
- Distinguish routine production acquisition from unusually large historical backfills. Do not inhibit ordinary odds collection; inspect and control spending for unusually large backfills before execution.
- Respect established MLB/NHL workflow boundaries and current operational priorities; do not broaden a task into another sport or workflow.

# Python environment

- Always run repository Python commands with `./bin/python`.
- Prefer `./bin/python -m pytest` when pytest is installed; for unittest-compatible tests, use `./bin/python -m unittest` if pytest is unavailable.
- Do not use ambient `python`, `python3`, or `pytest` for repository work.
- Pytest being unavailable is not a reason to change interpreters or troubleshoot NumPy/SciPy. Do not install packages without explicit authorization.
- For Python import failures, first run `./bin/check-python-env`.
