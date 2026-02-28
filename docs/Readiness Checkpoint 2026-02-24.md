# Readiness Checkpoint 2026-02-24

Purpose: record current MLB preseason / NHL restart pipeline readiness before NHL resumes on 2026-02-25.

Context:
- NHL resumes after break on 2026-02-25 (ET)
- MLB Opening Day is still approximately 30 days out

## Summary

- NHL restart readiness: `good`, with one known cron-runtime dependency caveat (`.venv` bootstrap in Render cron run command)
- MLB preseason readiness: `good`, with live MLB wide/slate/book validation still blocked by spring prop odds availability

## NHL Restart Readiness (2026-02-25)

Checked / completed:

- [x] `backend.nhl.cli daily --with-odds` runs successfully in Render shell through no-slate path
- [x] Offseason/break behavior is correct (`No NHL games ... skipping scoring/export steps`)
- [x] Render cron build/runtime blockers addressed:
  - Python 3.11 pin
  - `psql` available
  - active NHL daily scripts migrated/hardened for `psycopg` v3
- [x] NHL CLI command log redaction patch deployed
- [x] NHL UX route alignment in place (`/nhl/props` workspace modes)
- [x] NHL streaks dashboard now uses real SOG prediction/result data (`nhl.predictions` + `nhl.skater_game_logs_raw`)

Known caveat:

- [ ] Render NHL cron trigger runtime still uses conditional dependency bootstrap in the run command when `.venv` dependencies are missing
  - Current mitigation is operationally acceptable
  - Future cleanup: remove runtime bootstrap once cron runtime dependency behavior is made deterministic

First live-slate checks to run on/after 2026-02-25:

- `make nhl-post-deploy-strict BASE_URL=<backend_url> NHL_DATE=2026-02-25`
- Observe one Render NHL cron run on a live slate
- Verify NHL UI:
  - Today’s NHL Games page
  - NHL streaks dashboard values populate
  - NHL Picks workspace loads cleanly

## MLB Preseason / Season Transition Readiness

Checked / completed:

- [x] MLB prod12 daily path includes stat-derived refresh stage
- [x] Stat-derived refresh observed running in Render logs
- [x] Skip-existing-date behavior confirmed (safe reruns with lookback)
- [x] MLB preseason handling intent documented (`Season Activation Runbook`)
- [x] MLB slate output + book upload pipeline code exists (wide -> slate -> book upload)
- [x] MLB frontend browser DB reads removed from direct Supabase table access (privacy path)

Pending / timing-dependent:

- [ ] Live validation of MLB wide/slate/book path on a date with posted MLB player prop odds (OddsAPI availability)
- [ ] Season activation / cutover commands at appropriate time:
  - `make mlb-season-kickoff-check ...`
  - `make season-baseline-lock ...`
  - preseason cleanup decision (`make mlb-preseason-cleanup ...` dry-run first)
  - `make mlb-season-mode-lock`
  - `make season-cutover-ready`

## Follow-Up Notes

- Keep NHL cron runtime bootstrap in place until a deterministic Render cron dependency strategy is chosen.
- Revisit Supabase optimization planning as live MLB/NHL read/write volume increases.
- Continue NHL streak dashboard refinement after live games resume (ranking and presentation tuning).
