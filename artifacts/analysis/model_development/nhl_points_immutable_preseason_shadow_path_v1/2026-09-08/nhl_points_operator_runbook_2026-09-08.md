# NHL Points preseason shadow operator runbook

This path is manual observation only. Do not upload or execute from it.

1. Confirm the 07:30 NHL morning run is complete and `POINTS_MORNING_PREREQUISITES_READY=true`. Record its run directory; do not alter the LaunchAgent schedule.
2. Create the canonical Points input snapshot after the strict-prior feature export:

   `.venv/bin/python -m backend.nhl.scripts.create_points_shadow_input_snapshot --slate-date YYYY-MM-DD --game-spine-csv <morning-run>/canonical_game_spine.csv --game-spine-manifest <morning-run>/SHA256SUMS --features-csv backend/nhl/exports/train_nhl_points_v2.csv`

3. Fetch a raw Points response to a new path (never a latest/today path):

   `.venv/bin/python -m backend.nhl.points_quote_capture.cli fetch --api-key "$THE_ODDS_API_KEY" --output <new-raw-envelope.json>`

4. Normalize one immutable MIDDAY or FINAL_PREGAME quote run, using the game spine and player inputs inside the input snapshot and that snapshot's manifest:

   `.venv/bin/python -m backend.nhl.points_quote_capture.cli run --payload-json <raw-envelope.json> --games-csv <input-snapshot>/canonical_game_spine.csv --players-csv <input-snapshot>/points_player_inputs.csv --parent-manifest <input-snapshot>/SHA256SUMS --slate-date YYYY-MM-DD --run-timestamp-utc <UTC> --run-type MIDDAY`

5. Run the frozen shadow after quote capture, with the same run type. Do not pass an effective policy:

   `.venv/bin/python -m backend.nhl.points_shadow.cli run --game-spine-csv <input-snapshot>/canonical_game_spine.csv --game-spine-manifest <input-snapshot>/SHA256SUMS --player-inputs-csv <input-snapshot>/points_player_inputs.csv --player-inputs-manifest <input-snapshot>/SHA256SUMS --quote-run-dir <quote-run> --slate-date YYYY-MM-DD --run-timestamp-utc <later-UTC> --run-type MIDDAY`

6. Require `overall_status=PASS_WITH_REDUCED_COVERAGE`, no critical Points sentinel failures, C/U/E all zero, and no blocked player-game identity in M. Preserve the entire final directory.
7. For FINAL_PREGAME, repeat steps 3–6 with a new raw envelope and `FINAL_PREGAME`; do not carry missing MIDDAY markets forward.
8. Grade only from a canonical official outcome CSV. Preseason must remain `PRESEASON_NON_EVALUATION`; scratched/nonparticipants remain ungraded. Corrections use a new grading timestamp plus `--correction-of-grade-id`.

Immediate stop conditions: any parent/scorer/model hash drift, identity or orientation mismatch, unknown game type, post-start contamination, incomplete ladder marked coherent, blocked ladder in M/C/U/E, any candidate/upload/execution row, or missing completion manifest.

