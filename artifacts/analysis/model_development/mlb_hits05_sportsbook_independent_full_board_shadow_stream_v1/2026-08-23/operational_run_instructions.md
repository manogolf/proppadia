# Operations

The installed MLB daily wrapper calls `bin/mlb_hits05_full_board_shadow_daily_hook.sh` immediately after the governed current nonmarket lineup/starter parent finishes. It uses the existing five MLB daily invocations and adds no scheduler or provider request. The hook is nonblocking to the normal refresh and defaults on beginning 2026-08-24; set `MLB_ENABLE_HITS05_FULL_BOARD_SHADOW=0` for an emergency shadow-only disable.

Manual process validation (never prospective evidence):

```bash
.venv/bin/python -m backend.mlb.scripts.score_mlb_hits05_full_board_shadow_v1 --fixture /path/to/process_fixture.json --run-tag process_only --capture-timestamp 2099-01-01T00:00:00Z --ledger /tmp/hits05_fixture.sqlite3
```

Normal scoring is orchestration-owned. Do not manually backfill missed slates. Independent utilities:

```bash
.venv/bin/python -m backend.mlb.scripts.attach_mlb_hits05_full_board_markets_v1 --date YYYY-MM-DD
.venv/bin/python -m backend.mlb.scripts.grade_mlb_hits05_full_board_shadow_v1 --date YYYY-MM-DD
.venv/bin/python -m backend.mlb.scripts.report_mlb_hits05_full_board_shadow_v1
.venv/bin/python -m backend.mlb.scripts.validate_mlb_hits05_full_board_shadow_v1
```

Primary ledger: `backend/mlb/exports/model_v2/hits05_full_board_shadow_v1/hits05_full_board_shadow_v1.sqlite3`. Daily integrity summaries: `artifacts/analysis/mlb/hits05_full_board_shadow/<date>/`. Progress: `artifacts/analysis/mlb/hits05_full_board_shadow/progress_latest.json`.

Market observation classes are derived without mutation: first observed, nearest valid observation at no more than 30 minutes pre-start, and latest pre-start. They are Proppadia-observed prices, never asserted to be true opening or closing prices unless the source independently proves that status.
