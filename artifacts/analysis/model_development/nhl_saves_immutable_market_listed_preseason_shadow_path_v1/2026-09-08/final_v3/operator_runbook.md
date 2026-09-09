# NHL Saves immutable shadow operator runbook

This is a manual, non-wagering P/M/G path. It never represents market listing as starter confirmation.

1. Use the completed morning run's canonical game spine and complete scorer-eligible goalie feature/roster identity export. Seal both in a SHA256 parent manifest.
2. Fetch explicitly: `.venv/bin/python -m backend.nhl.saves_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output /create-only/raw_saves.json`.
3. Archive MIDDAY or FINAL_PREGAME with `python -m backend.nhl.saves_quote_capture.cli archive` and all required spine, manifest, slate, timestamp, run-type, and output-root arguments.
4. Run `python -m backend.nhl.saves_shadow.cli run` with the immutable game/goalie parents and the exact quote-run directory. Do not filter the goalie input by market presence.
5. Inspect `saves_live_failure_sentinel.json`, `policy_c_team_game_decisions.csv`, P, and M. `C/U/E` must remain empty.
6. After games, use `python -m backend.nhl.saves_shadow.cli grade`; actual starter and participation belong only in the outcomes file. Preseason remains non-evaluative.

Recovery is a new run identity or grading revision. Never edit a completed run. Zero usable markets is a healthy P run with M=0.
