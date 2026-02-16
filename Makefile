.PHONY: help mlb-help mlb-runbook mlb-cron-preview nhl-help ops-help ops-status ops-operator-summary ops-operator-summary-json ops-operator-summary-json-compact ops-operator-log ops-operator-last ops-operator-incident ops-operator-incident-strict ops-daily-check phase-status phase-status-json season-activation-status season-activation-status-strict season-activation-log season-activation-last season-activation-report season-activation-report-strict season-baseline-check season-cutover-ready season-activation-check cron-governance-check cron-governance-snapshot cron-fast-check cron-fast-check-json cron-current-state cron-scheduled-state cron-summary cron-summary-json cron-path-summary cron-path-summary-json nhl-workflow-compat-summary nhl-workflow-compat-summary-json assistant-handoff-bundle workflow-inventory workflow-inventory-strict workflow-path-audit workflow-path-audit-strict docs-make-target-audit ops-shortlist-check mlb-season-kickoff-check season-baseline-capture frontend-route-smoke diagnose ci-offline-checks shared-checks-offline mlb-checks-offline mlb-checks-offline-core mlb-checks mlb-checks-full mlb-checks-auto mlb-checks-golden mlb-checks-props-contract mlb-checks-profile-contract mlb-market-cache-refresh mlb-roster-refresh-all mlb-show-config mlb-readiness-snapshot mlb-readiness-log mlb-readiness-last mlb-prediction-readiness mlb-prediction-quality mlb-prediction-gate mlb-pipeline-check mlb-pipeline-check-json mlb-pipeline-log mlb-pipeline-last mlb-pipeline-daily-check mlb-prop-coverage mlb-prediction-flow-audit mlb-insert-stat-derived mlb-check-stat-derived mlb-check-stat-derived-json mlb-stat-derived-refresh mlb-stat-derived-smoke mlb-stat-derived-backfill mlb-daily-refresh mlb-daily-refresh-strict mlb-daily-refresh-smoke mlb-ops-check mlb-post-deploy mlb-post-deploy-strict mlb-post-deploy-strict-offseason mlb-release-check nhl-checks-offline nhl-checks-offline-core nhl-workflow-compat-check nhl-prediction-quality nhl-openapi-contract nhl-post-deploy nhl-post-deploy-strict nhl-post-deploy-strict-offseason nhl-release-check nhl-roster-refresh-all roster-refresh-all cross-sport-post-deploy runtime-boundaries

VENV_PY ?= .venv/bin/python
BASE_URL ?= http://127.0.0.1:8001
MLB_BASE_URL ?=
MLB_DATE ?= 2025-08-15
NHL_DATE ?= 2025-11-20
MLB_MARKET_DAYS ?= 1
MLB_ROSTER_DATE ?= $(shell date +%F)
NHL_ROSTER_DATE ?= $(shell date +%F)
MLB_STAT_DERIVED_DAYS ?= 7
MLB_STAT_DERIVED_MIN ?= 0
MLB_PREDICT_SAMPLE ?= 10
MLB_PREDICT_MIN_SUCCESS ?= 1
MLB_PREDICT_PROP_TYPES ?= hits
MLB_QUALITY_WINDOW_DAYS ?= 120
MLB_QUALITY_WINDOW_MODE ?= days
MLB_QUALITY_GAMES_BACK ?= 30
MLB_QUALITY_MIN_TOTAL ?= 1
MLB_QUALITY_MIN_ACCURACY ?= 0
MLB_PROP_COVERAGE_WINDOW_DAYS ?= 30
MLB_PROP_COVERAGE_WINDOW_MODE ?= days
MLB_PROP_COVERAGE_GAMES_BACK ?= 30
MLB_PROP_COVERAGE_REQUIRED ?=
MLB_PROP_COVERAGE_MIN_GRADED ?= 0
NHL_QUALITY_FROM_DATE ?=
NHL_QUALITY_TO_DATE ?=
NHL_QUALITY_MIN_TOTAL ?= 1
MLB_STAT_FROM_DATE ?=
MLB_STAT_TO_DATE ?=
MLB_STAT_DAYS_AGO ?= 2
MLB_STAT_MAX_GAMES ?= 0
MLB_STAT_SKIP_EXISTING_DATES ?= 1

help:
	@echo "Proppadia checks"
	@echo "  make diagnose"
	@echo "  make ci-offline-checks"
	@echo "  make shared-checks-offline"
	@echo "  make mlb-release-check BASE_URL=<url> [MLB_DATE=YYYY-MM-DD]"
	@echo "  make nhl-release-check BASE_URL=<url> [NHL_DATE=YYYY-MM-DD]"
	@echo "  make mlb-checks-full"
	@echo "  make mlb-market-cache-refresh [MLB_MARKET_DAYS=1]"
	@echo "  make mlb-roster-refresh-all [MLB_ROSTER_DATE=YYYY-MM-DD]"
	@echo "  make frontend-route-smoke [verify critical nav/route surface in AppRouter]"
	@echo "  make workflow-inventory [report scheduled workflow files]"
	@echo "  make workflow-inventory-strict [fail if scheduled files differ from allowlist]"
	@echo "  make workflow-path-audit [report missing python refs in scheduled workflows]"
	@echo "  make workflow-path-audit-strict [fail on missing python refs in scheduled workflows]"
	@echo "  make docs-make-target-audit [fail if docs reference missing make targets]"
	@echo "  make phase-status [print current phase tracker snapshot]"
	@echo "  make phase-status-json [machine-readable phase tracker summary]"
	@echo "  make season-activation-status [phase 6 status + baseline artifact presence]"
	@echo "  make season-activation-status-strict [non-zero exit until phase 6 readiness is complete]"
	@echo "  make season-activation-log [append season activation snapshot to artifacts jsonl]"
	@echo "  make season-activation-last [show latest season activation history rows]"
	@echo "  make season-activation-report [single JSON report: phase + activation + baseline + history]"
	@echo "  make season-activation-report-strict [same report, but exits non-zero when not ready]"
	@echo "  make season-baseline-check [validate MLB/NHL baseline artifacts exist]"
	@echo "  make season-cutover-ready [strict phase 6 readiness + governance gate]"
	@echo "  make season-activation-check [run phase 6.1 + 6.3 bundle]"
	@echo "  make ops-shortlist-check [high-signal ops bundle; optional NHL quality + post-deploy]"
	@echo "  make mlb-season-kickoff-check [opening-day readiness bundle; optional deployed check]"
	@echo "  make season-baseline-capture [write MLB/NHL day-0 quality JSON to artifacts]"
	@echo "  make nhl-workflow-compat-check [verify NHL workflow compatibility scripts]"
	@echo "  make cron-governance-check [inventory + path audit + NHL workflow compat]"
	@echo "  make cron-governance-snapshot [single combined JSON governance payload]"
	@echo "  make ops-operator-summary [compact daily ops summary]"
	@echo "  make ops-operator-summary-json [machine-readable daily ops summary]"
	@echo "  make ops-operator-summary-json-compact [machine-readable minimal ops summary]"
	@echo "  make ops-operator-log [append compact ops summary to history jsonl]"
	@echo "  make ops-operator-last [show recent compact ops history rows]"
	@echo "  make ops-operator-incident [current compact summary + history tail for incident triage]"
	@echo "  make ops-operator-incident-strict [same incident snapshot, exits non-zero on fail]"
	@echo "  make ops-daily-check [log compact summary + strict incident gate]"
	@echo "  make assistant-handoff-bundle [single JSON payload for support handoff]"
	@echo "  make cron-fast-check [quiet inventory/path summaries + NHL workflow compat]"
	@echo "  make cron-current-state [print current scheduled/manual workflow state]"
	@echo "  make cron-scheduled-state [print only currently scheduled workflows]"
	@echo "  make cron-summary [quiet strict cron inventory summary]"
	@echo "  make cron-summary-json [json strict cron inventory summary]"
	@echo "  make cron-path-summary [quiet strict scheduled-path audit summary]"
	@echo "  make cron-path-summary-json [json strict scheduled-path audit summary]"
	@echo "  make nhl-workflow-compat-summary [quiet NHL workflow compat summary]"
	@echo "  make nhl-workflow-compat-summary-json [json NHL workflow compat summary]"
	@echo "  make cron-fast-check-json [json summaries + NHL compat json]"
	@echo "  make mlb-show-config [prints effective MLB make/runtime values]"
	@echo "  make mlb-readiness-snapshot [json readiness for stat-derived + roster freshness]"
	@echo "  make mlb-readiness-log [append readiness snapshot to artifacts history jsonl]"
	@echo "  make mlb-readiness-last [show latest readiness history rows]"
	@echo "  make mlb-prediction-readiness [prepare->predict readiness sample for MLB_DATE]"
	@echo "  make mlb-prediction-quality [historical model quality summary json]"
	@echo "  make mlb-prediction-gate [combined operability + quality pass/fail]"
	@echo "  make mlb-pipeline-check [prediction gate + flow audit + prop coverage]"
	@echo "  make mlb-pipeline-check-json [single JSON payload for gate + flow + coverage]"
	@echo "  make mlb-pipeline-log [append pipeline check JSON snapshot to history]"
	@echo "  make mlb-pipeline-last [show recent pipeline history snapshots]"
	@echo "  make mlb-pipeline-daily-check [append latest pipeline snapshot, then show history tail]"
	@echo "  make mlb-prediction-flow-audit [date/game binding + duplicate/idempotency checks]"
	@echo "  make mlb-prop-coverage [recent prop-type coverage and graded volume]"
	@echo "  make mlb-daily-refresh [daily baseline; cache+roster+stat-derived]"
	@echo "  make mlb-daily-refresh-strict [daily baseline + require stat-derived min=1]"
	@echo "  make mlb-daily-refresh-smoke [daily baseline smoke; forces MLB_STAT_MAX_GAMES=1]"
	@echo "  make mlb-ops-check BASE_URL=<url> [ops confidence loop: config+daily-smoke+post-deploy]"
	@echo "  make mlb-stat-derived-refresh [insert+check; supports MLB_STAT_DAYS_AGO/MLB_STAT_SKIP_EXISTING_DATES]"
	@echo "  make mlb-stat-derived-backfill MLB_STAT_FROM_DATE=YYYY-MM-DD MLB_STAT_TO_DATE=YYYY-MM-DD [MLB_STAT_DERIVED_DAYS=400 MLB_STAT_DERIVED_MIN=1]"
	@echo "  make mlb-stat-derived-smoke [quick wiring check; forces MLB_STAT_MAX_GAMES=1]"
	@echo "  make mlb-insert-stat-derived [advanced: direct insert flags]"
	@echo "  make mlb-check-stat-derived [advanced: direct volume guard flags]"
	@echo "  make mlb-check-stat-derived-json [advanced: direct volume guard json]"
	@echo "  make roster-refresh-all [MLB_ROSTER_DATE=YYYY-MM-DD] [NHL_ROSTER_DATE=YYYY-MM-DD]"
	@echo "  make mlb-post-deploy BASE_URL=<url>"
	@echo "  make nhl-post-deploy BASE_URL=<url>"
	@echo "  make nhl-prediction-quality NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD [NHL_QUALITY_MIN_TOTAL=1]"
	@echo "  make nhl-roster-refresh-all [NHL_ROSTER_DATE=YYYY-MM-DD]"
	@echo "  make cross-sport-post-deploy BASE_URL=<url> [MLB_DATE=YYYY-MM-DD] [NHL_DATE=YYYY-MM-DD]"

mlb-help:
	@if command -v rg >/dev/null 2>&1; then \
		$(MAKE) help | rg "mlb-|MLB_"; \
	else \
		$(MAKE) help | grep -E "mlb-|MLB_"; \
	fi

mlb-runbook:
	@echo "1) make mlb-show-config"
	@echo "2) make mlb-daily-refresh-strict MLB_MARKET_DAYS=$(MLB_MARKET_DAYS) MLB_ROSTER_DATE=$(MLB_ROSTER_DATE) MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS)"
	@echo "3) make mlb-post-deploy BASE_URL=$(BASE_URL) MLB_DATE=$(MLB_DATE)"
	@echo "4) make mlb-ops-check BASE_URL=$(BASE_URL)"

mlb-cron-preview:
	@echo "Recommended MLB cron commands:"
	@echo "1) make mlb-daily-refresh-strict MLB_MARKET_DAYS=$(MLB_MARKET_DAYS) MLB_ROSTER_DATE=\$$(date +%F) MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS)"
	@echo "2) make mlb-market-cache-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS)   # optional extra warm schedule"

nhl-help:
	@if command -v rg >/dev/null 2>&1; then \
		$(MAKE) help | rg "nhl-|NHL_"; \
	else \
		$(MAKE) help | grep -E "nhl-|NHL_"; \
	fi

ops-help:
	@echo "MLB commands:"
	@$(MAKE) mlb-help
	@echo ""
	@echo "NHL commands:"
	@$(MAKE) nhl-help

ops-status:
	@echo "BASE_URL=$(BASE_URL)"
	@echo "MLB_DATE=$(MLB_DATE)"
	@echo "NHL_DATE=$(NHL_DATE)"
	@echo "MLB_ROSTER_DATE=$(MLB_ROSTER_DATE)"
	@echo "NHL_ROSTER_DATE=$(NHL_ROSTER_DATE)"
	@echo ""
	@$(MAKE) ops-operator-summary

ops-operator-summary:
	$(VENV_PY) backend/scripts/ops_operator_summary.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

ops-operator-summary-json:
	$(VENV_PY) backend/scripts/ops_operator_summary.py --json --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

ops-operator-summary-json-compact:
	$(VENV_PY) backend/scripts/ops_operator_summary.py --compact --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

ops-operator-log:
	$(VENV_PY) backend/scripts/ops_operator_log.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

ops-operator-last:
	$(VENV_PY) backend/scripts/ops_operator_last.py --json --limit 10

ops-operator-incident:
	$(VENV_PY) backend/scripts/ops_operator_incident.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

ops-operator-incident-strict:
	$(VENV_PY) backend/scripts/ops_operator_incident.py --strict --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

ops-daily-check:
	$(MAKE) ops-operator-log
	$(MAKE) ops-operator-incident-strict

phase-status:
	@awk '/^## Phase Status Tracker/{flag=1; print; next} /^## / && flag{exit} flag{print}' docs/Execution\ Plan.md

phase-status-json:
	$(VENV_PY) backend/scripts/phase_status_snapshot.py

season-activation-status:
	$(VENV_PY) backend/scripts/season_activation_status.py

season-activation-status-strict:
	$(VENV_PY) backend/scripts/season_activation_status.py --strict

season-activation-log:
	$(VENV_PY) backend/scripts/season_activation_log.py

season-activation-last:
	$(VENV_PY) backend/scripts/season_activation_last.py --json --limit 10

season-activation-report:
	$(VENV_PY) backend/scripts/season_activation_report.py

season-activation-report-strict:
	$(VENV_PY) backend/scripts/season_activation_report.py --strict

season-baseline-check:
	$(VENV_PY) backend/scripts/check_season_baseline_artifacts.py

season-cutover-ready:
	$(MAKE) season-activation-status-strict
	$(MAKE) season-baseline-check
	$(MAKE) cron-governance-check

season-activation-check:
	$(MAKE) mlb-season-kickoff-check BASE_URL="$(BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_MARKET_DAYS="$(MLB_MARKET_DAYS)" MLB_ROSTER_DATE="$(MLB_ROSTER_DATE)" MLB_STAT_DAYS_AGO="$(MLB_STAT_DAYS_AGO)" MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_DERIVED_DAYS="$(MLB_STAT_DERIVED_DAYS)" MLB_STAT_DERIVED_MIN="$(MLB_STAT_DERIVED_MIN)"
	@if [ -z "$(NHL_QUALITY_FROM_DATE)" ] || [ -z "$(NHL_QUALITY_TO_DATE)" ]; then \
		echo "season-activation-check requires NHL_QUALITY_FROM_DATE and NHL_QUALITY_TO_DATE for baseline capture"; \
		exit 2; \
	fi
	$(MAKE) season-baseline-capture MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" NHL_QUALITY_FROM_DATE="$(NHL_QUALITY_FROM_DATE)" NHL_QUALITY_TO_DATE="$(NHL_QUALITY_TO_DATE)" NHL_QUALITY_MIN_TOTAL="$(NHL_QUALITY_MIN_TOTAL)"

workflow-inventory:
	$(VENV_PY) backend/scripts/check_workflow_schedule_inventory.py

workflow-inventory-strict:
	$(VENV_PY) backend/scripts/check_workflow_schedule_inventory.py --strict

workflow-path-audit:
	$(VENV_PY) backend/scripts/check_workflow_command_paths.py

workflow-path-audit-strict:
	$(VENV_PY) backend/scripts/check_workflow_command_paths.py --strict

docs-make-target-audit:
	$(VENV_PY) backend/scripts/check_docs_make_targets.py

ops-shortlist-check:
	$(MAKE) phase-status-json
	$(MAKE) cron-governance-check
	$(MAKE) mlb-pipeline-daily-check MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PREDICT_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_PROP_COVERAGE_WINDOW_MODE="$(MLB_PROP_COVERAGE_WINDOW_MODE)" MLB_PROP_COVERAGE_WINDOW_DAYS="$(MLB_PROP_COVERAGE_WINDOW_DAYS)" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROP_COVERAGE_REQUIRED)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_PROP_COVERAGE_MIN_GRADED)"
	@if [ -n "$(NHL_QUALITY_FROM_DATE)" ] && [ -n "$(NHL_QUALITY_TO_DATE)" ]; then \
		$(MAKE) nhl-prediction-quality NHL_QUALITY_FROM_DATE="$(NHL_QUALITY_FROM_DATE)" NHL_QUALITY_TO_DATE="$(NHL_QUALITY_TO_DATE)" NHL_QUALITY_MIN_TOTAL="$(NHL_QUALITY_MIN_TOTAL)"; \
	else \
		echo "ops-shortlist-check: skipping nhl-prediction-quality (set NHL_QUALITY_FROM_DATE and NHL_QUALITY_TO_DATE to enable)"; \
	fi
	@if [ -n "$(BASE_URL)" ] && [ "$(BASE_URL)" != "http://127.0.0.1:8001" ]; then \
		$(MAKE) cross-sport-post-deploy BASE_URL="$(BASE_URL)" MLB_DATE="$(MLB_DATE)" NHL_DATE="$(NHL_DATE)"; \
	else \
		echo "ops-shortlist-check: skipping cross-sport-post-deploy (set BASE_URL to deployed URL to enable)"; \
	fi

mlb-season-kickoff-check:
	$(MAKE) cron-governance-check
	$(MAKE) mlb-show-config
	$(MAKE) mlb-daily-refresh-smoke MLB_MARKET_DAYS=$(MLB_MARKET_DAYS) MLB_ROSTER_DATE=$(MLB_ROSTER_DATE) MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)
	$(MAKE) mlb-pipeline-check-json MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PREDICT_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_PROP_COVERAGE_WINDOW_MODE="$(MLB_PROP_COVERAGE_WINDOW_MODE)" MLB_PROP_COVERAGE_WINDOW_DAYS="$(MLB_PROP_COVERAGE_WINDOW_DAYS)" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROP_COVERAGE_REQUIRED)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_PROP_COVERAGE_MIN_GRADED)"
	@if [ -n "$(BASE_URL)" ] && [ "$(BASE_URL)" != "http://127.0.0.1:8001" ]; then \
		$(MAKE) mlb-post-deploy-strict-offseason BASE_URL="$(BASE_URL)" MLB_DATE="$(MLB_DATE)"; \
	else \
		echo "mlb-season-kickoff-check: skipping post-deploy (set BASE_URL to deployed URL to enable)"; \
	fi

season-baseline-capture:
	@mkdir -p artifacts/season_baselines
	$(VENV_PY) backend/scripts/analyze_mlb_prediction_quality.py --window-mode $(MLB_QUALITY_WINDOW_MODE) --window-days $(MLB_QUALITY_WINDOW_DAYS) --games-back $(MLB_QUALITY_GAMES_BACK) --min-total $(MLB_QUALITY_MIN_TOTAL) > artifacts/season_baselines/mlb_quality_$(MLB_QUALITY_WINDOW_MODE)_$(MLB_QUALITY_GAMES_BACK)_$(MLB_QUALITY_WINDOW_DAYS).json
	@if [ -z "$(NHL_QUALITY_FROM_DATE)" ] || [ -z "$(NHL_QUALITY_TO_DATE)" ]; then \
		echo "season-baseline-capture requires NHL_QUALITY_FROM_DATE and NHL_QUALITY_TO_DATE"; \
		exit 2; \
	fi
	$(VENV_PY) backend/scripts/analyze_nhl_prediction_quality.py --from-date $(NHL_QUALITY_FROM_DATE) --to-date $(NHL_QUALITY_TO_DATE) --min-total $(NHL_QUALITY_MIN_TOTAL) > artifacts/season_baselines/nhl_quality_$(NHL_QUALITY_FROM_DATE)_$(NHL_QUALITY_TO_DATE).json
	@echo "Wrote artifacts/season_baselines/mlb_quality_$(MLB_QUALITY_WINDOW_MODE)_$(MLB_QUALITY_GAMES_BACK)_$(MLB_QUALITY_WINDOW_DAYS).json"
	@echo "Wrote artifacts/season_baselines/nhl_quality_$(NHL_QUALITY_FROM_DATE)_$(NHL_QUALITY_TO_DATE).json"

cron-governance-check:
	$(MAKE) workflow-inventory-strict
	$(MAKE) workflow-path-audit-strict
	$(MAKE) nhl-workflow-compat-check
	$(MAKE) docs-make-target-audit

cron-governance-snapshot:
	$(VENV_PY) backend/scripts/cron_governance_snapshot.py

assistant-handoff-bundle:
	$(VENV_PY) backend/scripts/assistant_handoff_bundle.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

cron-fast-check:
	$(MAKE) cron-summary
	$(MAKE) cron-path-summary
	$(MAKE) nhl-workflow-compat-summary

cron-fast-check-json:
	$(MAKE) cron-summary-json
	$(MAKE) cron-path-summary-json
	$(MAKE) nhl-workflow-compat-summary-json

cron-current-state:
	$(MAKE) workflow-inventory

cron-scheduled-state:
	$(VENV_PY) backend/scripts/check_workflow_schedule_inventory.py --scheduled-only

cron-summary:
	$(VENV_PY) backend/scripts/check_workflow_schedule_inventory.py --strict --quiet

cron-summary-json:
	$(VENV_PY) backend/scripts/check_workflow_schedule_inventory.py --strict --json

cron-path-summary:
	$(VENV_PY) backend/scripts/check_workflow_command_paths.py --strict --quiet

cron-path-summary-json:
	$(VENV_PY) backend/scripts/check_workflow_command_paths.py --strict --json

nhl-workflow-compat-summary:
	$(VENV_PY) backend/scripts/check_nhl_workflow_compat.py --quiet

nhl-workflow-compat-summary-json:
	$(VENV_PY) backend/scripts/check_nhl_workflow_compat.py --json

# One-command local diagnostic baseline for support/debug sessions.
diagnose:
	$(MAKE) runtime-boundaries
	$(MAKE) frontend-route-smoke
	$(MAKE) cron-fast-check
	$(MAKE) shared-checks-offline
	$(MAKE) mlb-checks-offline-core
	$(MAKE) nhl-checks-offline-core

# One-command offline CI baseline (same composition as diagnose).
ci-offline-checks: diagnose

runtime-boundaries:
	$(VENV_PY) backend/scripts/check_runtime_import_boundaries.py

frontend-route-smoke:
	$(VENV_PY) backend/scripts/check_frontend_route_smoke.py

# Shared backend checks not tied to one sport.
shared-checks-offline:
	$(VENV_PY) -m unittest discover -s backend/tests -p 'test_shared_*.py' -v

# Fast local verification (no external MLB API required).
mlb-checks-offline:
	$(MAKE) runtime-boundaries
	$(MAKE) shared-checks-offline
	$(MAKE) mlb-checks-offline-core

mlb-checks-offline-core:
	$(VENV_PY) -m unittest discover -s backend/tests -p 'test_mlb_*.py' -v
	$(VENV_PY) backend/scripts/smoke_mlb_api.py --mode offline
	$(VENV_PY) backend/scripts/check_mlb_openapi_contract.py
	$(MAKE) mlb-checks-profile-contract

# Default day-to-day MLB verification.
# Includes metrics endpoint shape checks (requires DB connectivity from backend).
mlb-checks: mlb-checks-offline
	$(VENV_PY) backend/scripts/validate_mlb_metrics.py --api-only

# Auto mode for local environments where DB may be unavailable.
# Runs strict DB-dependent checks when possible; otherwise keeps offline checks green.
mlb-checks-auto: mlb-checks-offline
	@if $(VENV_PY) backend/scripts/validate_mlb_metrics.py --api-only; then \
		echo "mlb-checks-auto: metrics api-only passed"; \
	else \
		echo "mlb-checks-auto: DB-dependent metrics unavailable, kept offline checks only"; \
	fi

# Full verification pass (historical DB + schedule/context checks).
# Requires DB connectivity and outbound MLB StatsAPI access.
mlb-checks-full: mlb-checks
	$(VENV_PY) backend/scripts/smoke_mlb_api.py --mode full --date 2025-08-15
	$(VENV_PY) backend/scripts/validate_mlb_metrics.py
	$(MAKE) mlb-checks-props-contract
	$(MAKE) mlb-checks-golden

# Golden-path write-aware smoke (prepare -> predict -> add -> duplicate replay).
# Requires DB connectivity and a resolvable historical game context.
mlb-checks-golden:
	$(VENV_PY) backend/scripts/smoke_mlb_prop_flow.py --date 2025-08-15 --team-id 119 --player-id 660271

# DB contract check for fields consumed by frontend PlayerPropsTable.
mlb-checks-props-contract:
	$(VENV_PY) backend/scripts/validate_mlb_props_contract.py

# Warm MLB OddsAPI cache snapshot for ET date window (cron-friendly).
mlb-market-cache-refresh:
	$(VENV_PY) -m backend.scripts.refresh_mlb_market_cache --days $(MLB_MARKET_DAYS)

# Full-team MLB player/roster refresh (all teams; not slate-limited).
mlb-roster-refresh-all:
	$(VENV_PY) -m backend.scripts.refresh_mlb_players_rosters --date $(MLB_ROSTER_DATE)

# Show effective MLB make/runtime values before execution.
mlb-show-config:
	@echo "MLB_DATE=$(MLB_DATE)"
	@echo "MLB_MARKET_DAYS=$(MLB_MARKET_DAYS)"
	@echo "MLB_ROSTER_DATE=$(MLB_ROSTER_DATE)"
	@echo "MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO)"
	@echo "MLB_STAT_FROM_DATE=$(MLB_STAT_FROM_DATE)"
	@echo "MLB_STAT_TO_DATE=$(MLB_STAT_TO_DATE)"
	@echo "MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES)"
	@echo "MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES)"
	@echo "MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS)"
	@echo "MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)"
	@echo "MLB_PREDICT_SAMPLE=$(MLB_PREDICT_SAMPLE)"
	@echo "MLB_PREDICT_MIN_SUCCESS=$(MLB_PREDICT_MIN_SUCCESS)"
	@echo "MLB_PREDICT_PROP_TYPES=$(MLB_PREDICT_PROP_TYPES)"
	@echo "MLB_QUALITY_WINDOW_DAYS=$(MLB_QUALITY_WINDOW_DAYS)"
	@echo "MLB_QUALITY_WINDOW_MODE=$(MLB_QUALITY_WINDOW_MODE)"
	@echo "MLB_QUALITY_GAMES_BACK=$(MLB_QUALITY_GAMES_BACK)"
	@echo "MLB_QUALITY_MIN_TOTAL=$(MLB_QUALITY_MIN_TOTAL)"
	@echo "MLB_QUALITY_MIN_ACCURACY=$(MLB_QUALITY_MIN_ACCURACY)"
	@echo "MLB_PROP_COVERAGE_WINDOW_DAYS=$(MLB_PROP_COVERAGE_WINDOW_DAYS)"
	@echo "MLB_PROP_COVERAGE_WINDOW_MODE=$(MLB_PROP_COVERAGE_WINDOW_MODE)"
	@echo "MLB_PROP_COVERAGE_GAMES_BACK=$(MLB_PROP_COVERAGE_GAMES_BACK)"
	@echo "MLB_PROP_COVERAGE_REQUIRED=$(MLB_PROP_COVERAGE_REQUIRED)"
	@echo "MLB_PROP_COVERAGE_MIN_GRADED=$(MLB_PROP_COVERAGE_MIN_GRADED)"

# JSON snapshot for MLB readiness signals (stat-derived + roster freshness).
mlb-readiness-snapshot:
	$(VENV_PY) backend/scripts/mlb_readiness_snapshot.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

mlb-readiness-log:
	$(VENV_PY) backend/scripts/mlb_readiness_log.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

mlb-readiness-last:
	$(VENV_PY) backend/scripts/mlb_readiness_last.py --limit 10

mlb-prediction-readiness:
	$(VENV_PY) backend/scripts/probe_mlb_prediction_readiness.py --date $(MLB_DATE) --sample-size $(MLB_PREDICT_SAMPLE) --require-min-success $(MLB_PREDICT_MIN_SUCCESS) --prop-types "$(MLB_PREDICT_PROP_TYPES)"

mlb-prediction-quality:
	$(VENV_PY) backend/scripts/analyze_mlb_prediction_quality.py --window-mode $(MLB_QUALITY_WINDOW_MODE) --window-days $(MLB_QUALITY_WINDOW_DAYS) --games-back $(MLB_QUALITY_GAMES_BACK) --min-total $(MLB_QUALITY_MIN_TOTAL)

mlb-prediction-gate:
	$(VENV_PY) backend/scripts/mlb_prediction_gate.py --date $(MLB_DATE) --sample-size $(MLB_PREDICT_SAMPLE) --require-min-success $(MLB_PREDICT_MIN_SUCCESS) --prop-types "$(MLB_PREDICT_PROP_TYPES)" --quality-window-mode $(MLB_QUALITY_WINDOW_MODE) --quality-window-days $(MLB_QUALITY_WINDOW_DAYS) --quality-games-back $(MLB_QUALITY_GAMES_BACK) --quality-min-total $(MLB_QUALITY_MIN_TOTAL) --quality-min-accuracy $(MLB_QUALITY_MIN_ACCURACY)

mlb-pipeline-check:
	$(MAKE) mlb-prediction-gate MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PREDICT_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)"
	$(MAKE) mlb-prediction-flow-audit MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)"
	$(MAKE) mlb-prop-coverage MLB_PROP_COVERAGE_WINDOW_MODE="$(MLB_PROP_COVERAGE_WINDOW_MODE)" MLB_PROP_COVERAGE_WINDOW_DAYS="$(MLB_PROP_COVERAGE_WINDOW_DAYS)" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROP_COVERAGE_REQUIRED)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_PROP_COVERAGE_MIN_GRADED)"

mlb-pipeline-check-json:
	$(VENV_PY) backend/scripts/mlb_pipeline_check.py $(if $(MLB_BASE_URL),--base-url $(MLB_BASE_URL),) --date $(MLB_DATE) --sample-size $(MLB_PREDICT_SAMPLE) --require-min-success $(MLB_PREDICT_MIN_SUCCESS) --prop-types "$(MLB_PREDICT_PROP_TYPES)" --quality-window-mode $(MLB_QUALITY_WINDOW_MODE) --quality-window-days $(MLB_QUALITY_WINDOW_DAYS) --quality-games-back $(MLB_QUALITY_GAMES_BACK) --quality-min-total $(MLB_QUALITY_MIN_TOTAL) --quality-min-accuracy $(MLB_QUALITY_MIN_ACCURACY) --coverage-window-mode $(MLB_PROP_COVERAGE_WINDOW_MODE) --coverage-window-days $(MLB_PROP_COVERAGE_WINDOW_DAYS) --coverage-games-back $(MLB_PROP_COVERAGE_GAMES_BACK) --coverage-required-props "$(MLB_PROP_COVERAGE_REQUIRED)" --coverage-min-graded-per-prop $(MLB_PROP_COVERAGE_MIN_GRADED)

mlb-pipeline-log:
	$(VENV_PY) backend/scripts/mlb_pipeline_log.py --output artifacts/mlb_pipeline_history.jsonl $(if $(MLB_BASE_URL),--base-url $(MLB_BASE_URL),) --date $(MLB_DATE) --sample-size $(MLB_PREDICT_SAMPLE) --require-min-success $(MLB_PREDICT_MIN_SUCCESS) --prop-types "$(MLB_PREDICT_PROP_TYPES)" --quality-window-mode $(MLB_QUALITY_WINDOW_MODE) --quality-window-days $(MLB_QUALITY_WINDOW_DAYS) --quality-games-back $(MLB_QUALITY_GAMES_BACK) --quality-min-total $(MLB_QUALITY_MIN_TOTAL) --quality-min-accuracy $(MLB_QUALITY_MIN_ACCURACY) --coverage-window-mode $(MLB_PROP_COVERAGE_WINDOW_MODE) --coverage-window-days $(MLB_PROP_COVERAGE_WINDOW_DAYS) --coverage-games-back $(MLB_PROP_COVERAGE_GAMES_BACK) --coverage-required-props "$(MLB_PROP_COVERAGE_REQUIRED)" --coverage-min-graded-per-prop $(MLB_PROP_COVERAGE_MIN_GRADED)

mlb-pipeline-last:
	$(VENV_PY) backend/scripts/mlb_pipeline_last.py --input artifacts/mlb_pipeline_history.jsonl --limit 10 --json

mlb-pipeline-daily-check:
	$(MAKE) mlb-pipeline-log MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PREDICT_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_PROP_COVERAGE_WINDOW_MODE="$(MLB_PROP_COVERAGE_WINDOW_MODE)" MLB_PROP_COVERAGE_WINDOW_DAYS="$(MLB_PROP_COVERAGE_WINDOW_DAYS)" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROP_COVERAGE_REQUIRED)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_PROP_COVERAGE_MIN_GRADED)"
	$(MAKE) mlb-pipeline-last

mlb-prop-coverage:
	$(VENV_PY) backend/scripts/report_mlb_prop_coverage.py --window-mode $(MLB_PROP_COVERAGE_WINDOW_MODE) --window-days $(MLB_PROP_COVERAGE_WINDOW_DAYS) --games-back $(MLB_PROP_COVERAGE_GAMES_BACK) --required-props "$(MLB_PROP_COVERAGE_REQUIRED)" --min-graded-per-prop $(MLB_PROP_COVERAGE_MIN_GRADED)

mlb-prediction-flow-audit:
	$(VENV_PY) backend/scripts/audit_mlb_prediction_flow.py --window-mode $(MLB_QUALITY_WINDOW_MODE) --window-days $(MLB_QUALITY_WINDOW_DAYS) --games-back $(MLB_QUALITY_GAMES_BACK)

# Generate historical stat-derived MLB rows (legacy workhorse script).
mlb-insert-stat-derived:
	$(VENV_PY) backend/scripts/insert_mlb_stat_derived.py --quiet --days-ago $(MLB_STAT_DAYS_AGO) --max-games-per-date $(MLB_STAT_MAX_GAMES) $(if $(filter 1,$(MLB_STAT_SKIP_EXISTING_DATES)),--skip-existing-dates,) $(if $(MLB_STAT_FROM_DATE),--from-date $(MLB_STAT_FROM_DATE),) $(if $(MLB_STAT_TO_DATE),--to-date $(MLB_STAT_TO_DATE),)

# Validate recent stat-derived row volume in model_training_props.
mlb-check-stat-derived:
	$(VENV_PY) backend/scripts/validate_mlb_stat_derived_recent.py --days $(MLB_STAT_DERIVED_DAYS) --require-min $(MLB_STAT_DERIVED_MIN)

mlb-check-stat-derived-json:
	$(VENV_PY) backend/scripts/validate_mlb_stat_derived_recent.py --days $(MLB_STAT_DERIVED_DAYS) --require-min $(MLB_STAT_DERIVED_MIN) --json

# One-command stat-derived refresh + guard (cron-friendly).
mlb-stat-derived-refresh:
	$(MAKE) mlb-insert-stat-derived MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES)
	$(MAKE) mlb-check-stat-derived MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)

# Quick smoke for stat-derived wiring (limits game load).
mlb-stat-derived-smoke:
	$(MAKE) mlb-insert-stat-derived MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=1 MLB_STAT_SKIP_EXISTING_DATES=0

# Historical window backfill + guard in one command.
mlb-stat-derived-backfill:
	@if [ -z "$(MLB_STAT_FROM_DATE)" ] || [ -z "$(MLB_STAT_TO_DATE)" ]; then \
		echo "mlb-stat-derived-backfill requires MLB_STAT_FROM_DATE and MLB_STAT_TO_DATE"; \
		exit 2; \
	fi
	$(MAKE) mlb-insert-stat-derived MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES)
	$(MAKE) mlb-check-stat-derived MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)

# One-command MLB daily refresh baseline (cache + rosters + stat-derived + guard).
mlb-daily-refresh:
	$(MAKE) mlb-show-config
	$(MAKE) mlb-market-cache-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS)
	$(MAKE) mlb-roster-refresh-all MLB_ROSTER_DATE=$(MLB_ROSTER_DATE)
	$(MAKE) mlb-stat-derived-refresh MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)

# Strict daily baseline: enforces stat-derived volume guard.
mlb-daily-refresh-strict:
	$(MAKE) mlb-daily-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS) MLB_ROSTER_DATE=$(MLB_ROSTER_DATE) MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=1

# Daily baseline smoke mode: quick end-to-end validation with max one game/date.
mlb-daily-refresh-smoke:
	$(MAKE) mlb-daily-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS) MLB_ROSTER_DATE=$(MLB_ROSTER_DATE) MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=1 MLB_STAT_SKIP_EXISTING_DATES=0 MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)

# Ops confidence loop for MLB: config snapshot + quick daily smoke + deployed API smoke.
mlb-ops-check:
	$(MAKE) mlb-show-config
	$(MAKE) mlb-market-cache-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS)
	$(MAKE) mlb-roster-refresh-all MLB_ROSTER_DATE=$(MLB_ROSTER_DATE)
	$(MAKE) mlb-stat-derived-smoke MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)
	$(MAKE) mlb-post-deploy BASE_URL=$(BASE_URL) MLB_DATE=$(MLB_DATE)

# API contract check for /api/player-profile payload consumed by frontend.
mlb-checks-profile-contract:
	$(VENV_PY) backend/scripts/validate_mlb_profile_contract.py

# Fast deployed-environment health check (safe, no write operations).
mlb-post-deploy:
	$(VENV_PY) backend/scripts/post_deploy_mlb_check.py --base-url $(BASE_URL) --date $(MLB_DATE)

# Post-deploy check that also requires non-sparse probe data.
mlb-post-deploy-strict:
	$(VENV_PY) backend/scripts/post_deploy_mlb_check.py --base-url $(BASE_URL) --date $(MLB_DATE) --require-data

# Post-deploy strict transport/DB checks, but tolerate sparse probe data (offseason-safe).
mlb-post-deploy-strict-offseason:
	$(VENV_PY) backend/scripts/post_deploy_mlb_check.py --base-url $(BASE_URL) --date $(MLB_DATE) --require-data --allow-sparse

# One-command MLB release confidence gate (offseason-safe strict deploy check).
mlb-release-check: mlb-checks-offline
	$(MAKE) mlb-post-deploy-strict-offseason BASE_URL=$(BASE_URL) MLB_DATE=$(MLB_DATE)

# Fast NHL deployed-environment health check (safe, no write operations).
nhl-post-deploy:
	$(VENV_PY) backend/scripts/post_deploy_nhl_check.py --base-url $(BASE_URL) --date $(NHL_DATE)

# NHL post-deploy check requiring non-sparse probe data.
nhl-post-deploy-strict:
	$(VENV_PY) backend/scripts/post_deploy_nhl_check.py --base-url $(BASE_URL) --date $(NHL_DATE) --require-data

# NHL post-deploy strict transport/DB checks, but tolerate sparse probe data.
nhl-post-deploy-strict-offseason:
	$(VENV_PY) backend/scripts/post_deploy_nhl_check.py --base-url $(BASE_URL) --date $(NHL_DATE) --require-data --allow-sparse

# NHL OpenAPI contract drift check.
nhl-openapi-contract:
	$(VENV_PY) backend/scripts/check_nhl_openapi_contract.py

nhl-prediction-quality:
	@if [ -z "$(NHL_QUALITY_FROM_DATE)" ] || [ -z "$(NHL_QUALITY_TO_DATE)" ]; then \
		echo "nhl-prediction-quality requires NHL_QUALITY_FROM_DATE and NHL_QUALITY_TO_DATE"; \
		exit 2; \
	fi
	$(VENV_PY) backend/scripts/analyze_nhl_prediction_quality.py --from-date $(NHL_QUALITY_FROM_DATE) --to-date $(NHL_QUALITY_TO_DATE) --min-total $(NHL_QUALITY_MIN_TOTAL)

# Fast local NHL verification (no external NHL API required).
nhl-checks-offline:
	$(MAKE) runtime-boundaries
	$(MAKE) shared-checks-offline
	$(MAKE) nhl-checks-offline-core

nhl-checks-offline-core:
	$(MAKE) nhl-workflow-compat-check
	$(VENV_PY) -m unittest discover -s backend/tests -p 'test_nhl_*.py' -v
	$(MAKE) nhl-openapi-contract

nhl-workflow-compat-check:
	$(VENV_PY) backend/scripts/check_nhl_workflow_compat.py

# Full-team NHL player/roster refresh (all teams; not slate-limited).
# Override date with NHL_ROSTER_DATE=YYYY-MM-DD when needed.
nhl-roster-refresh-all:
	$(VENV_PY) -m backend.nhl.cli refresh-rosters-all --date $(NHL_ROSTER_DATE)

# Convenience umbrella target for both sports.
roster-refresh-all:
	$(MAKE) mlb-roster-refresh-all MLB_ROSTER_DATE=$(MLB_ROSTER_DATE)
	$(MAKE) nhl-roster-refresh-all NHL_ROSTER_DATE=$(NHL_ROSTER_DATE)

# One-command NHL release confidence gate (offseason-safe strict deploy check).
nhl-release-check: nhl-checks-offline
	$(MAKE) nhl-post-deploy-strict-offseason BASE_URL=$(BASE_URL) NHL_DATE=$(NHL_DATE)

# One-command cross-sport deploy confidence gate (offseason-safe strict checks).
cross-sport-post-deploy:
	$(MAKE) mlb-post-deploy-strict-offseason BASE_URL=$(BASE_URL) MLB_DATE=$(MLB_DATE)
	$(MAKE) nhl-post-deploy-strict-offseason BASE_URL=$(BASE_URL) NHL_DATE=$(NHL_DATE)
