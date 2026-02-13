.PHONY: mlb-checks-offline mlb-checks mlb-checks-full mlb-checks-auto mlb-checks-golden mlb-checks-props-contract mlb-checks-profile-contract mlb-post-deploy mlb-post-deploy-strict runtime-boundaries

VENV_PY ?= .venv/bin/python
BASE_URL ?= http://127.0.0.1:8001

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
	$(VENV_PY) backend/scripts/post_deploy_mlb_check.py --base-url $(BASE_URL)

# Post-deploy check that also requires non-sparse probe data.
mlb-post-deploy-strict:
	$(VENV_PY) backend/scripts/post_deploy_mlb_check.py --base-url $(BASE_URL) --require-data
