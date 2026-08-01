# Optional pipeline hook

The installed `/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh` invokes `bin/mlb_cleanroom_fixed_cohort_optional_hook.sh` immediately after acquiring its daily and shared pipeline locks, before the normal production work begins.

The hook defaults off through `${MLB_CLEANROOM_FIXED_COHORT_ENABLED:-0}`. To opt in later, set `MLB_CLEANROOM_FIXED_COHORT_ENABLED=1` in `backend/.env`. It was not enabled during implementation.

The helper invokes the standalone Make target once for `MLB_DATE_ET`, preserves a dedicated log, and returns nonblocking status to the wrapper. The wrapper captures a nonzero return, emits a warning, and continues. No LaunchAgent or schedule was added.
