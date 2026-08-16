# Bullpen recency root cause

`BULLPEN_HISTORY_ROOT_CAUSE=STALE_ARTIFACT_DEFECT`

Acquisition did not stop: retained official final feeds exist for every completed game from August 6 through August 15, and the installed daily wrapper runs completed-slate recovery before totals scoring. Parsing those retained feeds succeeds, duplicate copies normalize identically, and no append job failed. The totals live bridge simply continued reading a fixed August 6 feature-spine artifact whose latest game was August 5; it had no supplement/read-through path to the retained later finals. The empty rolling lookback naturally summed to numeric zero—zero was not explicitly written by acquisition.

`BULLPEN_FEATURE_DEFECT_START=2026-08-07` because August 7 is the first target date whose expected prior-date foundation (August 6) was absent.
