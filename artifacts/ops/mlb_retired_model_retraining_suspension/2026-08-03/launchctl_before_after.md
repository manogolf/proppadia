# launchctl before/after

Before: service existed at `gui/501/com.proppadia.mlb.retrain.weekly`, state `not running`, active count 0, runs 3, last exit code 2, calendar trigger Tuesday 23:05.

After: `launchctl print gui/501/com.proppadia.mlb.retrain.weekly` returns service not found. `launchctl print-disabled gui/501` reports `com.proppadia.mlb.retrain.weekly => disabled`.

No retired trainer process was active. User crontab does not exist. Unrelated MLB LaunchAgents remained loaded.
