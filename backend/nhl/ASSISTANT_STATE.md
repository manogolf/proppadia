# Assistant State (NHL pipeline guardrail)

This file is the source of truth for what has already been completed.
Before suggesting changes, check this file and DO NOT re-suggest completed items.

## Completed

- [ ] (fill in as you go)

## In progress

- [ ] (fill in)

## Do not suggest again

- [ ] remove duplicate run_psql_file_to_path definition
- [ ] replace psql_stdout to return bytes/text consistently (resolved)
- [ ] add import re when NameError occurs (resolved when added)

## Notes

- Daily run must remain minimal: `python -m backend.nhl.cli daily --with-odds`
- Any testing-only flags must be gated behind TESTING=1 and never required for daily.
