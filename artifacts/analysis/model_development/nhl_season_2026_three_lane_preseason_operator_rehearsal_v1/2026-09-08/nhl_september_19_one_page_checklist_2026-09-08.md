# September 19 NHL preseason checklist

1. Confirm the 07:30 LaunchAgent exit is 0 and `morning_health.json` is manifest-complete. If `VALID_EMPTY_SLATE`, stop successfully.
2. Confirm canonical season 2026, game type 1, unique game IDs, start times, home/away orientation, and exact same game IDs across Moneyline/SOG/Points.
3. Confirm DB credential availability, `ODDS_API_KEY`, strict-prior history/roster timestamps, Points input manifest, SOG parity hash, and explicit authorized SOG policy JSON.
4. Create new MIDDAY timestamps and paths. Run Moneyline, SOG quote→score→candidate with `--no-upload-shaped-output`, and Points input→quote→P/M. Run nothing for Goalie Saves.
5. Verify MIDDAY manifests and populations: Moneyline P/M shadow only; SOG E=0 and no upload-shaped file; Points C/U/E=0 with `RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG`, blocked ladders visible in P and absent from M.
6. Read the MIDDAY sentinel. RED blocks downstream action. YELLOW requires reason review; missing/partial markets may be healthy reduced coverage.
7. Before each game starts, create entirely new FINAL_PREGAME raw, quote, effective-config, and shadow identities. Never overwrite MIDDAY. Confirm every qualified quote is pre-start.
8. Read FINAL_PREGAME sentinel and compare MIDDAY/FINAL coverage, identities, disappearances, and late postings. Preserve both phases.
9. After official completion, grade append-only. Confirm preseason non-evaluation, nonparticipant ungraded, execution rows zero, and pregame hashes unchanged.
10. Reconcile cumulative artifacts and preserve all incomplete paths. Retry only with a new run identity. Upload never implies execution; no upload or execution is authorized here.

Decision boundary: Moneyline stays frozen-control prediction/market shadow-only; SOG stays certified P/M/C-lineage shadow-only with no upload/execution; Points stays P/M shadow-only; Goalie Saves stays disabled and `NOT_READY_FOR_PRESEASON_BURN_IN`.
