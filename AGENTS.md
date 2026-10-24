# Repository operating instructions

- Before work, read applicable `AGENTS.md` files and the relevant project doctrine, contracts, and runbooks. This checklist points to those sources; their detailed policies remain authoritative.
- At task start, review backlog items germane to authorized work; resolve related items that fit naturally within scope and close them with evidence, record newly deferred items before finishing, and do not pursue unrelated backlog work or expand production authorization.
- When a change alters an operating contract — gates, statuses, exclusion or grading rules, artifact layout, or which files readers consume — update the governing document in `docs/` (the relevant contract, command guide, and `docs/README.md` index) in the same change, and correct any statement it makes stale. The backlog records evidence and open work; it does not replace the contract.
- Use `backend/.env` as the authoritative environment configuration. Never print, log, or include credentials or connection strings in output or artifacts.
- When an authorized external request fails on DNS or transport inside the sandbox, immediately request elevated network permission for that request and retry. Check elevation before diagnosing a Mac or provider outage.
- Preserve ordinary provider retry behavior; do not add arbitrary one-attempt limits.
- Protect populated historical source values. Routine operations must use their configured restricted database identity; never switch to owner-level access to bypass a failure.
- Before changing a database caller, inspect the relevant SQL schema, returned column aliases, actual Python result shape, and connection routing.
- Before creating a new directory (in code or artifacts), scan the repository for an existing directory that serves the same or a similar purpose, and use it if it fits. Avoid repository bloat.
- Before writing new logic or a new function, search the code, the `docs/` contracts and `git log -S` for an existing implementation of the same rule. In the plan or report, name what you found and whether you are reusing it, extending it, or why neither fits. A change that replaces existing behavior must say so.
- Prefer narrow corrections to demonstrated defects. Avoid repeating completed historical audits, adding defensive frameworks, or building speculative infrastructure.
- Repo only: create, copy or write files only inside this repository. Never write anywhere else on the Mac — no temporary or scratch folders outside the repo, no files in the home directory, no Python environments, caches or other hidden folders outside the repo — and do not ask for an exception; it will not be granted. Use the repo's git-ignored `tmp/` for scratch output, and run tests with `TMPDIR` and `--basetemp` pointed inside `tmp/`. Work directly in this checkout; do not create git worktrees.
- Preserve unrelated worktree changes. Commit coherent completed corrections locally; never push without explicit authorization.
- Commit source code, tests, documentation, and small reproducibility manifests. Do not stage generated research datasets, especially CSV exports, unless explicitly authorized. Respect existing `artifacts/**` ignore rules; do not use `git add -f` to bypass ignored generated data without explicit authorization. Before committing, inspect staged paths and sizes. Keep large generated outputs local with regeneration instructions and checksums, and do not delete unrelated research artifacts to reduce commit size.
- Distinguish routine production acquisition from unusually large historical backfills. Do not inhibit ordinary odds collection; inspect and control spending for unusually large backfills before execution.
- Respect established MLB/NHL workflow boundaries and current operational priorities; do not broaden a task into another sport or workflow.

## Git workflow
- After completing each step, commit the changes with a short, clear message describing what changed.
- Keep each commit small and focused on one step.
- Never force-push or rewrite history unless I ask.

# Python environment

- Always run repository Python commands with `./bin/python`.
- Prefer `./bin/python -m pytest` when pytest is installed; for unittest-compatible tests, use `./bin/python -m unittest` if pytest is unavailable.
- Do not use ambient `python`, `python3`, or `pytest` for repository work.
- Pytest being unavailable is not a reason to change interpreters or troubleshoot NumPy/SciPy. Do not install packages without explicit authorization.
- For Python import failures, first run `./bin/check-python-env`.
- `./bin/python` runs the repo's own `.venv` (Python 3.11.13, Homebrew). To rebuild it identically and offline, from the repo root:
  `rm -rf .venv && /opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv && TMPDIR=$PWD/tmp PIP_NO_CACHE_DIR=1 .venv/bin/python -m pip install --no-index --no-cache-dir --find-links vendor/python-wheels --require-hashes -r vendor/requirements-lock.txt && printf '%s\n' "$PWD" > .venv/lib/python3.11/site-packages/proppadia_root.pth`
  The wheels in `vendor/python-wheels/` are git-ignored; `vendor/requirements-lock.txt` pins every package version and checksum.
