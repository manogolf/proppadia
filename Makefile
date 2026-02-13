.PHONY: mlb-checks-offline mlb-checks mlb-checks-full mlb-checks-auto mlb-checks-golden mlb-checks-props-contract runtime-boundaries

VENV_PY ?= .venv/bin/python

runtime-boundaries:
	$(VENV_PY) backend/scripts/check_runtime_import_boundaries.py

# Fast local verification (no external MLB API required).
mlb-checks-offline:
	$(MAKE) runtime-boundaries
	$(VENV_PY) -m unittest discover -s backend/tests -p 'test_mlb_*.py' -v
	$(VENV_PY) backend/scripts/smoke_mlb_api.py --mode offline
	$(VENV_PY) backend/scripts/check_mlb_openapi_contract.py

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
