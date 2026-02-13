.PHONY: help mlb-checks-offline mlb-checks mlb-checks-full mlb-checks-auto mlb-checks-golden mlb-checks-props-contract mlb-checks-profile-contract mlb-post-deploy mlb-post-deploy-strict mlb-post-deploy-strict-offseason mlb-release-check nhl-checks-offline nhl-openapi-contract nhl-post-deploy nhl-post-deploy-strict nhl-post-deploy-strict-offseason nhl-release-check runtime-boundaries

VENV_PY ?= .venv/bin/python
BASE_URL ?= http://127.0.0.1:8001
MLB_DATE ?= 2025-08-15
NHL_DATE ?= 2025-11-20

help:
	@echo "Proppadia checks"
	@echo "  make mlb-release-check BASE_URL=<url> [MLB_DATE=YYYY-MM-DD]"
	@echo "  make nhl-release-check BASE_URL=<url> [NHL_DATE=YYYY-MM-DD]"
	@echo "  make mlb-checks-full"
	@echo "  make mlb-post-deploy BASE_URL=<url>"
	@echo "  make nhl-post-deploy BASE_URL=<url>"

runtime-boundaries:
	$(VENV_PY) backend/scripts/check_runtime_import_boundaries.py

# Fast local verification (no external MLB API required).
mlb-checks-offline:
	$(MAKE) runtime-boundaries
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
	$(MAKE) nhl-openapi-contract

# One-command NHL release confidence gate (offseason-safe strict deploy check).
nhl-release-check: nhl-checks-offline
	$(MAKE) nhl-post-deploy-strict-offseason BASE_URL=$(BASE_URL) NHL_DATE=$(NHL_DATE)
