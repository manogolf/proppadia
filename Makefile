.PHONY: help diagnose ci-offline-checks shared-checks-offline mlb-checks-offline mlb-checks-offline-core mlb-checks mlb-checks-full mlb-checks-auto mlb-checks-golden mlb-checks-props-contract mlb-checks-profile-contract mlb-market-cache-refresh mlb-roster-refresh-all mlb-insert-stat-derived mlb-check-stat-derived mlb-stat-derived-refresh mlb-stat-derived-smoke mlb-stat-derived-backfill mlb-daily-refresh mlb-daily-refresh-strict mlb-post-deploy mlb-post-deploy-strict mlb-post-deploy-strict-offseason mlb-release-check nhl-checks-offline nhl-checks-offline-core nhl-openapi-contract nhl-post-deploy nhl-post-deploy-strict nhl-post-deploy-strict-offseason nhl-release-check nhl-roster-refresh-all roster-refresh-all cross-sport-post-deploy runtime-boundaries

VENV_PY ?= .venv/bin/python
BASE_URL ?= http://127.0.0.1:8001
MLB_DATE ?= 2025-08-15
NHL_DATE ?= 2025-11-20
MLB_MARKET_DAYS ?= 1
MLB_ROSTER_DATE ?= $(shell date +%F)
NHL_ROSTER_DATE ?= $(shell date +%F)
MLB_STAT_DERIVED_DAYS ?= 7
MLB_STAT_DERIVED_MIN ?= 0
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
	@echo "  make mlb-insert-stat-derived [MLB_STAT_FROM_DATE=YYYY-MM-DD MLB_STAT_TO_DATE=YYYY-MM-DD MLB_STAT_MAX_GAMES=0 MLB_STAT_SKIP_EXISTING_DATES=1]"
	@echo "  make mlb-check-stat-derived [MLB_STAT_DERIVED_DAYS=7] [MLB_STAT_DERIVED_MIN=0]"
	@echo "  make mlb-stat-derived-refresh [MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7 MLB_STAT_DERIVED_MIN=1]"
	@echo "  make mlb-stat-derived-smoke [MLB_STAT_FROM_DATE=YYYY-MM-DD MLB_STAT_TO_DATE=YYYY-MM-DD MLB_STAT_MAX_GAMES=1]"
	@echo "  make mlb-stat-derived-backfill MLB_STAT_FROM_DATE=YYYY-MM-DD MLB_STAT_TO_DATE=YYYY-MM-DD [MLB_STAT_DERIVED_DAYS=400 MLB_STAT_DERIVED_MIN=1]"
	@echo "  make mlb-daily-refresh [MLB_ROSTER_DATE=YYYY-MM-DD MLB_MARKET_DAYS=1 MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1]"
	@echo "  make mlb-daily-refresh-strict [MLB_ROSTER_DATE=YYYY-MM-DD MLB_MARKET_DAYS=1 MLB_STAT_DAYS_AGO=2]"
	@echo "  make roster-refresh-all [MLB_ROSTER_DATE=YYYY-MM-DD] [NHL_ROSTER_DATE=YYYY-MM-DD]"
	@echo "  make mlb-post-deploy BASE_URL=<url>"
	@echo "  make nhl-post-deploy BASE_URL=<url>"
	@echo "  make nhl-roster-refresh-all [NHL_ROSTER_DATE=YYYY-MM-DD]"
	@echo "  make cross-sport-post-deploy BASE_URL=<url> [MLB_DATE=YYYY-MM-DD] [NHL_DATE=YYYY-MM-DD]"

# One-command local diagnostic baseline for support/debug sessions.
diagnose:
	$(MAKE) runtime-boundaries
	$(MAKE) shared-checks-offline
	$(MAKE) mlb-checks-offline-core
	$(MAKE) nhl-checks-offline-core

# One-command offline CI baseline (same composition as diagnose).
ci-offline-checks: diagnose

runtime-boundaries:
	$(VENV_PY) backend/scripts/check_runtime_import_boundaries.py

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

# Generate historical stat-derived MLB rows (legacy workhorse script).
mlb-insert-stat-derived:
	$(VENV_PY) backend/scripts/insert_mlb_stat_derived.py --quiet --days-ago $(MLB_STAT_DAYS_AGO) --max-games-per-date $(MLB_STAT_MAX_GAMES) $(if $(filter 1,$(MLB_STAT_SKIP_EXISTING_DATES)),--skip-existing-dates,) $(if $(MLB_STAT_FROM_DATE),--from-date $(MLB_STAT_FROM_DATE),) $(if $(MLB_STAT_TO_DATE),--to-date $(MLB_STAT_TO_DATE),)

# Validate recent stat-derived row volume in model_training_props.
mlb-check-stat-derived:
	$(VENV_PY) backend/scripts/validate_mlb_stat_derived_recent.py --days $(MLB_STAT_DERIVED_DAYS) --require-min $(MLB_STAT_DERIVED_MIN)

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
	$(MAKE) mlb-market-cache-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS)
	$(MAKE) mlb-roster-refresh-all MLB_ROSTER_DATE=$(MLB_ROSTER_DATE)
	$(MAKE) mlb-stat-derived-refresh MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)

# Strict daily baseline: enforces stat-derived volume guard.
mlb-daily-refresh-strict:
	$(MAKE) mlb-daily-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS) MLB_ROSTER_DATE=$(MLB_ROSTER_DATE) MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=1

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

# Fast local NHL verification (no external NHL API required).
nhl-checks-offline:
	$(MAKE) runtime-boundaries
	$(MAKE) shared-checks-offline
	$(MAKE) nhl-checks-offline-core

nhl-checks-offline-core:
	$(VENV_PY) -m unittest discover -s backend/tests -p 'test_nhl_*.py' -v
	$(MAKE) nhl-openapi-contract

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
