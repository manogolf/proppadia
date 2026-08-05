# DH forward-validation automation installation and rollback

The two jobs are research-only. They do not authorize a qualified production MLB model.

Install or refresh:

```bash
mkdir -p "$HOME/Library/LaunchAgents" artifacts/ops
cp backend/mlb/launchagents/com.proppadia.mlb.dh-forward-capture.plist "$HOME/Library/LaunchAgents/"
cp backend/mlb/launchagents/com.proppadia.mlb.dh-forward-grade.plist "$HOME/Library/LaunchAgents/"
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.proppadia.mlb.dh-forward-capture.plist"
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.proppadia.mlb.dh-forward-grade.plist"
```

Capture runs every 600 seconds but exits before doing reconstruction outside the dynamic window beginning four hours before the earliest first pitch and ending when the final scheduled game starts. Grading runs at 8:15 AM Pacific for the prior date. Both paths use exclusive locks, atomic replacement, and recoverable pre-mutation backups.

Manual research invocations:

```bash
.venv/bin/python -m backend.mlb.scripts.run_mlb_dh_forward_capture
.venv/bin/python -m backend.mlb.scripts.run_mlb_dh_forward_grade
```

Rollback (preserves ledgers, logs, scorer, and research evidence):

```bash
launchctl bootout "gui/$(id -u)/com.proppadia.mlb.dh-forward-capture"
launchctl bootout "gui/$(id -u)/com.proppadia.mlb.dh-forward-grade"
launchctl disable "gui/$(id -u)/com.proppadia.mlb.dh-forward-capture"
launchctl disable "gui/$(id -u)/com.proppadia.mlb.dh-forward-grade"
```

The retired `com.proppadia.mlb.retrain.weekly` job is not referenced or modified by either automation.
