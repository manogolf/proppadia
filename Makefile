.PHONY: help mlb-help mlb-runbook mlb-cron-preview mlb-prod12-cron-preview mlb-prod12-script-preview mlb-prod12-bootstrap-preview mlb-prod12-scheduler-smoke mlb-prod12-bootstrap mlb-prod12-bootstrap-strict nhl-help ops-help ops-show-config ops-status ops-operator-summary ops-operator-summary-json ops-operator-summary-json-compact ops-operator-log ops-operator-last ops-operator-incident ops-operator-incident-strict ops-daily-check phase-status phase-status-json season-activation-status season-activation-status-strict season-activation-log season-activation-last season-activation-report season-activation-report-strict season-baseline-check season-baseline-last season-baseline-lock season-cutover-cadence season-cutover-log season-cutover-last season-cutover-ready season-activation-check cron-governance-check cron-governance-snapshot cron-fast-check cron-fast-check-json cron-current-state cron-scheduled-state cron-summary cron-summary-json cron-path-summary cron-path-summary-json nhl-workflow-compat-summary nhl-workflow-compat-summary-json assistant-handoff-bundle workflow-inventory workflow-inventory-strict workflow-path-audit workflow-path-audit-strict docs-make-target-audit ops-shortlist-check mlb-season-kickoff-check season-baseline-capture mlb-season-baseline-capture mlb-prod8-baseline-capture frontend-route-smoke diagnose ci-offline-checks shared-checks-offline mlb-checks-offline mlb-checks-offline-core mlb-checks mlb-checks-full mlb-checks-auto mlb-checks-golden mlb-checks-props-contract mlb-checks-profile-contract mlb-player-surface-checks mlb-market-cache-refresh mlb-roster-refresh-all mlb-predictions-wide mlb-slate-output mlb-book-upload mlb-slate-archive mlb-reconcile-rows mlb-model-vs-fade mlb-post-grade-fade-check mlb-show-config mlb-readiness-snapshot mlb-readiness-log mlb-readiness-last mlb-prediction-readiness mlb-prediction-quality mlb-prediction-quality-core mlb-prediction-quality-prod8 mlb-prediction-quality-prod12 mlb-recompute-training-predictions mlb-corrected-props-recompute mlb-model-artifact-validate mlb-model-artifact-validate-prod12 mlb-pre-cron-check mlb-model-snapshot mlb-model-publish mlb-model-prune mlb-model-rollback mlb-feature-health mlb-feature-health-prod12 mlb-pfp-overlap-audit mlb-pfp-overlap-backfill mlb-prediction-quality-user-added mlb-prediction-quality-segmented mlb-degenerate-lane-report mlb-underserved-historical-report mlb-high-value-historical-report mlb-retrain-prereq-check mlb-candidate-eval mlb-candidate-eval-prod12 mlb-prod12-status mlb-prod12-status-strict mlb-prod12-health-report mlb-prod12-incident mlb-prod12-incident-strict mlb-prod12-ops-check mlb-prod12-ops-log mlb-prod12-ops-last mlb-prod12-track-daily mlb-prod12-daily-gate mlb-prod12-daily-gate-incident mlb-prod12-daily-cycle mlb-prod12-track-weekly mlb-prod12-release-manifest mlb-prod12-replay-latency mlb-prod12-phase2-log mlb-prod12-phase2-last mlb-prod12-phase2-last-strict mlb-prod12-phase2-weekly-gate mlb-prod12-phase2-weekly-gate-incident mlb-prod12-phase2-weekly-cycle mlb-prod12-phase2-readiness mlb-prediction-gate mlb-pipeline-check mlb-pipeline-check-json mlb-pipeline-check-core mlb-pipeline-check-prod8 mlb-pipeline-check-prod12 mlb-pipeline-check-ops mlb-pipeline-log mlb-pipeline-log-prod12 mlb-pipeline-log-ops mlb-pipeline-last mlb-pipeline-daily-check mlb-prop-coverage mlb-prop-coverage-core mlb-prediction-flow-audit mlb-hits-expectation-sources mlb-insert-stat-derived mlb-check-stat-derived mlb-check-stat-derived-json mlb-check-rolling-integrity mlb-stat-derived-refresh mlb-stat-derived-smoke mlb-stat-derived-backfill mlb-preseason-cleanup mlb-season-mode-lock mlb-daily-refresh mlb-daily-refresh-strict mlb-daily-refresh-smoke mlb-ops-check mlb-post-deploy mlb-post-deploy-strict mlb-post-deploy-strict-offseason mlb-release-check nhl-checks-offline nhl-checks-offline-core nhl-workflow-compat-check nhl-prediction-quality nhl-prediction-quality-auto nhl-openapi-contract nhl-post-deploy nhl-post-deploy-strict nhl-post-deploy-strict-offseason nhl-release-check nhl-roster-refresh-all roster-refresh-all cross-sport-post-deploy runtime-boundaries
.PHONY: mlb-tmp-focus mlb-build-early-steam-movement mlb-cleanroom-bol-tb15-capture mlb-cleanroom-bol-tb15-closeout mlb-cleanroom-bol-tb15-status mlb-cleanroom-lineup-temporal-audit mlb-cleanroom-bol-tb15-under-freeze mlb-cleanroom-bol-tb15-under-closeout mlb-cleanroom-bol-tb15-under-status mlb-cleanroom-bol-tb15-under-toporder-freeze mlb-cleanroom-bol-tb15-under-toporder-closeout mlb-cleanroom-bol-tb15-under-toporder-status tmp-audit tmp-prune-bulky tmp-prune-age tmp-prune-fat-csv tmp-prune mlb-odds-history-audit mlb-odds-history-prune-intermediate mlb-odds-history-prune-old-dates mlb-odds-history-offload-status mlb-odds-history-offload-sync mlb-odds-history-offload-prune-local mlb-odds-history-offload-cycle artifacts-audit artifacts-prune-safe artifacts-prune-experiments artifacts-prune
.PHONY: mlb-cleanroom-prospective-lineage-status
.PHONY: mlb-cleanroom-outcome-closeout-lineage-status

VENV_PY ?= .venv/bin/python
# Canonical model root (same default local + render); cron/runtime can still override MODEL_DIR.
export MODEL_DIR ?= /var/data/proppadia/models
BASE_URL ?= http://127.0.0.1:8001
MLB_BASE_URL ?=
MLB_DATE ?= $(shell date -u +%F)
MLB_DATE_ET ?= $(shell TZ=America/New_York date +%F)
MLB_DAILY_RECONCILE_DATE ?= $(shell TZ=America/New_York date -v-1d +%F 2>/dev/null || TZ=America/New_York date -d "yesterday" +%F)

# Bounded source-only capture. Does not run models, incumbent predictions,
# ranking, Quick Card, uploads, Ops Brief, or historical reconciliation.
# Usage: make mlb-cleanroom-bol-tb15-capture MLB_DATE=YYYY-MM-DD
mlb-cleanroom-bol-tb15-capture:
	@set -a; . backend/.env; set +a; \
	$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.run_cleanroom_bol_tb15_capture \
		--date "$(MLB_DATE)"

mlb-cleanroom-bol-tb15-closeout:
	@$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.closeout_cleanroom_bol_tb15 \
		--date "$(MLB_DATE)"

mlb-cleanroom-bol-tb15-status:
	@$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.closeout_cleanroom_bol_tb15 \
		--date "$(MLB_DATE)" --status-only

mlb-cleanroom-lineup-temporal-audit:
	@set -a; . backend/.env; set +a; \
	$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.audit_lineup_temporal_admissibility \
		--date "$(MLB_DATE)"

mlb-cleanroom-prospective-lineage-status:
	@$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.prospective_lineage_status

mlb-cleanroom-outcome-closeout-lineage-status:
	@$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.outcome_closeout_lineage_status

mlb-cleanroom-bol-tb15-under-freeze:
	@$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.manage_cleanroom_bol_tb15_under_hypotheses \
		--date "$(MLB_DATE)" --mode freeze

mlb-cleanroom-bol-tb15-under-closeout:
	@set -a; . backend/.env; set +a; \
	$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.manage_cleanroom_bol_tb15_under_hypotheses \
		--date "$(MLB_DATE)" --mode closeout

mlb-cleanroom-bol-tb15-under-status:
	@$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.manage_cleanroom_bol_tb15_under_hypotheses \
		--date "$(MLB_DATE)" --mode status

mlb-cleanroom-bol-tb15-under-toporder-freeze:
	@$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.manage_cleanroom_bol_tb15_under_toporder \
		--date "$(MLB_DATE)" --mode freeze

mlb-cleanroom-bol-tb15-under-toporder-closeout:
	@set -a; . backend/.env; set +a; \
	$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.manage_cleanroom_bol_tb15_under_toporder \
		--date "$(MLB_DATE)" --mode closeout

mlb-cleanroom-bol-tb15-under-toporder-status:
	@$(VENV_PY) -u -m backend.mlb.scripts.cleanroom_v1.manage_cleanroom_bol_tb15_under_toporder \
		--date "$(MLB_DATE)" --mode status
MLB_UPLOAD_PREP_DATE ?= $(MLB_DATE_ET)
MLB_REBUILD_UPLOADS ?= 0
# 8rain public catalog fetch is opt-in and limited to documented /public/api/catalog endpoints.
# Intentional use: make mlb-daily-upload-prep MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH=1
MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH ?= 0
MLB_OVERLAP_SNAPSHOT_DATE ?= $(MLB_DAILY_RECONCILE_DATE)
MLB_OVERLAP_SNAPSHOT_CSV ?= artifacts/analysis/mlb/research_gap_analysis/overlap_daily_snapshot.csv
MLB_OVERLAP_OPS_DATE ?= $(MLB_DAILY_RECONCILE_DATE)
MLB_V2_REGISTRY_DATE ?= $(MLB_DAILY_RECONCILE_DATE)
MLB_V2_REGISTRY_CSV ?= artifacts/analysis/mlb/research_gap_analysis/mlb_v2_daily_candidate_registry.csv
MLB_POST_GRADE_DATE ?= $(MLB_DATE_ET)
NHL_DATE ?= 2025-11-20
NHL_MODEL_VS_FADE_GRADED_GLOB ?= tmp/graded/nhl_sog_graded_*.csv
NHL_MODEL_VS_FADE_CARDS_DIR ?= tmp/cards
NHL_MODEL_VS_FADE_MIN_BETS_ALERT ?= 20
NHL_MODEL_VS_FADE_OUT_JSON ?= tmp/analysis/nhl_model_vs_fade_summary.json
NHL_MODEL_VS_FADE_OUT_SEGMENTS_CSV ?= tmp/analysis/nhl_model_vs_fade_by_segment.csv
NHL_MODEL_VS_FADE_OUT_ROWS_CSV ?= tmp/analysis/nhl_model_vs_fade_rows.csv
CROSS_SPORT_MODEL_VS_FADE_OUT_JSON ?= tmp/analysis/cross_sport_model_vs_fade_summary.json
CROSS_SPORT_MODEL_VS_FADE_MAX_DELTA ?= 0
CROSS_SPORT_MODEL_VS_FADE_NHL_MIN_BETS ?= $(NHL_MODEL_VS_FADE_MIN_BETS_ALERT)
CROSS_SPORT_MODEL_VS_FADE_MLB_MIN_BETS ?= $(MLB_MODEL_VS_FADE_MIN_BETS_ALERT)
CROSS_SPORT_MODEL_VS_FADE_REQUIRE_NHL ?= 1
CROSS_SPORT_MODEL_VS_FADE_REQUIRE_MLB ?= 1
MLB_MARKET_DAYS ?= 1
MLB_SLATE_PRED_CSV ?= backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv
# Retired compatibility flag. Active code ignores it; retained only so old
# environments receive a visible tombstone rather than reactivating UBO-5.
export MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE ?= 0
MLB_UBO5_TB15_ARTIFACT ?= artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23/original_ubo5_total_bases_multinomial.joblib
MLB_UBO5_TB15_NORMALIZED_ROOT ?= artifacts/analysis/model_development/mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/normalized_refresh
MLB_UBO5_TB15_CANDIDATE_LEDGER ?= artifacts/analysis/mlb/production_routes/ubo5_tb15/$(MLB_DATE)/candidate_ledger.csv
MLB_UBO5_TB15_LINEUP_DIR ?= artifacts/analysis/mlb/production_routes/ubo5_tb15/$(MLB_DATE)/governed_lineup_capture
MLB_UBO5_TB15_LINEUP_LEDGER ?= $(MLB_UBO5_TB15_LINEUP_DIR)/pregame_lineup_player_rows_$(MLB_DATE)_ubo5_tb15_daily.csv
MLB_UBO5_TB15_PRODUCER_STATUS ?= artifacts/analysis/mlb/production_routes/ubo5_tb15/$(MLB_DATE)/feature_ledger_producer_status.json
MLB_UBO5_TB15_FEATURE_LEDGER ?= artifacts/analysis/mlb/production_routes/ubo5_tb15/$(MLB_DATE)/feature_ledger.parquet
MLB_UBO5_TB15_ROUTE_LEDGER ?= artifacts/analysis/mlb/production_routes/ubo5_tb15/$(MLB_DATE)/route_ledger.csv
MLB_UBO5_TB15_HEALTH_JSON ?= artifacts/analysis/mlb/production_routes/ubo5_tb15/$(MLB_DATE)/route_health.json
MLB_UBO5_HISTORY_NORMALIZED_ROOT ?= artifacts/analysis/model_development/mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/normalized_refresh
MLB_UBO5_TB15_BOARD_ROOT ?= backend/mlb/exports/model_v2/ubo5_tb15
MLB_UBO5_TB15_BOARD_MD ?= $(MLB_UBO5_TB15_BOARD_ROOT)/$(MLB_DATE)/ubo5_tb15_board_$(MLB_DATE).md
MLB_UBO5_TB15_BOARD_CSV ?= $(MLB_UBO5_TB15_BOARD_ROOT)/$(MLB_DATE)/ubo5_tb15_board_$(MLB_DATE).csv
MLB_UBO5_TB15_BOARD_RUN_TAG ?=
MLB_UBO5_TB15_PROVISIONAL_RUN_TAG ?=
MLB_UBO5_TB15_PRELINEUP_CONFIRMATION_RUN_TAG ?=
MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV ?= artifacts/analysis/mlb/execution_vs_model/$(MLB_DATE)/reconcile_rows.csv
MLB_SLATE_OUTPUT_CSV ?= backend/mlb/data/processed/mlb_slate_output.csv
MLB_SLATE_PROP_TYPE ?=
MLB_BOOK_UPLOAD_OUT_CSV ?= backend/mlb/data/processed/mlb_book_upload.csv
MLB_BOOK_UPLOAD_WEIGHTED_OUT_CSV ?= backend/mlb/data/processed/mlb_book_upload_weighted.csv
MLB_BOOK_UPLOAD_HYBRID_OUT_CSV ?= backend/mlb/data/processed/mlb_book_upload_hybrid.csv
MLB_UPLOAD_COMPARE_BASE_CSV ?= backend/mlb/data/processed/mlb_uploads/$(MLB_DATE)/05_book_upload_base.csv
MLB_UPLOAD_COMPARE_WEIGHTED_CSV ?= backend/mlb/data/processed/mlb_uploads/$(MLB_DATE)/05_book_upload_weighted.csv
MLB_UPLOAD_COMPARE_OUT_DIR ?= artifacts/analysis/mlb/upload_variant_compare/$(MLB_DATE)
MLB_UPLOAD_COMPARE_GRADED_ROWS_CSV ?= tmp/mlb_base_vs_market_rows_anybook.csv
MLB_SINGLES_SHADOW_BASE_CSV ?= backend/mlb/data/processed/mlb_uploads/$(MLB_DATE)/05_book_upload_base.csv
MLB_SINGLES_SHADOW_OUT_CSV ?= backend/mlb/data/processed/mlb_uploads/$(MLB_DATE)/05_book_upload_singles_shadow.csv
MLB_SINGLES_SHADOW_OUT_DIR ?= tmp/experiments/singles_shadow/$(MLB_DATE)
MLB_SINGLES_SHADOW_ODDS_SNAPSHOT ?= backend/mlb/exports/odds_history/$(MLB_DATE)/odds_latest_compatible.json
MLB_SINGLES_SHADOW_GRADED_ROWS_CSV ?= tmp/mlb_base_vs_market_rows_anybook.csv
MLB_SINGLES_SHADOW_THRESHOLD ?= 0.55
MLB_SINGLES_SHADOW_TOP_N ?= 25
MLB_SINGLES_SHADOW_MAX_PER_PLAYER ?= 2
MLB_SINGLES_SHADOW_MAX_ABS_WIN_PCT ?= 500
MLB_SINGLES_SHADOW_MODEL_PATH ?=
MLB_TOTAL_BASES_SHADOW_DATE ?= $(MLB_DATE_ET)
MLB_TOTAL_BASES_SHADOW_TRAIN_THROUGH ?= 2026-06-14
MLB_TOTAL_BASES_SHADOW_TRAINING_DATASET ?= artifacts/analysis/mlb/model_quality/total_bases_canonical_spine_rolling_hydrated/2026-04-01_2026-06-14/total_bases_canonical_spine_dry_run_dataset.csv
MLB_TOTAL_BASES_SHADOW_SLATE_OUTPUT_CSV ?= $(MLB_SLATE_OUTPUT_CSV)
MLB_TOTAL_BASES_SHADOW_OUT_ROOT ?= artifacts/analysis/mlb/model_quality/total_bases_shadow
MLB_TOTAL_BASES_SHADOW_OUTCOMES_CSV ?=
MLB_TOTAL_BASES_SHADOW_EVAL_OUT_DIR ?= artifacts/analysis/mlb/model_quality/total_bases_shadow/evaluation
MLB_TOTAL_BASES_SHADOW_RECONCILE_ROOT ?= artifacts/analysis/mlb/execution_vs_model
MLB_HITS_O15_SIMPLE_FILTER_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_HITS_O15_SIMPLE_FILTER_OUT_DIR ?= artifacts/analysis/mlb/review_aids
MLB_HITS_O15_WATCH_CANDIDATES_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_HITS_O15_WATCH_CANDIDATES_OUT_DIR ?= artifacts/analysis/mlb/review_aids
MLB_HITS_O15_LAYERED_CANDIDATES_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_HITS_O15_LAYERED_CANDIDATES_OUT_DIR ?= artifacts/analysis/mlb/review_aids
MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_HITS_O15_ALTERNATE_DISCOVERY_OUT_DIR ?= artifacts/analysis/mlb/review_aids
MLB_ODDSAPI_BATTER_HITS_ALTERNATE_LIVE_DISCOVERY_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE))
MLB_ODDSAPI_BATTER_HITS_ALTERNATE_LIVE_DISCOVERY_OUT_ROOT ?= artifacts/analysis/mlb/review_aids/oddsapi_batter_hits_alternate_live_discovery
MLB_HITS_O15_ALTERNATE_BOOK_LEVEL_CSV ?= artifacts/analysis/mlb/review_aids/oddsapi_batter_hits_alternate_live_discovery/$(MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE)/live_alternate_book_level_rows.csv
MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_FROM ?= $(if $(strip $(DATE_FROM)),$(DATE_FROM),$(MLB_DATE_ET))
MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_TO ?= $(if $(strip $(DATE_TO)),$(DATE_TO),$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_FROM))
MLB_ODDSAPI_ALTERNATE_HISTORY_SNAPSHOT_TIME_ET ?= 13:00
MLB_ODDSAPI_ALTERNATE_HISTORY_DRY_RUN ?= $(if $(strip $(DRY_RUN)),$(DRY_RUN),1)
MLB_ODDSAPI_ALTERNATE_HISTORY_RUN_PROBE ?= 0
MLB_ODDSAPI_ALTERNATE_HISTORY_CONFIRM ?=
MLB_ODDSAPI_ALTERNATE_HISTORY_OUT_DIR ?= artifacts/analysis/mlb/review_aids/alternate_history
MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT ?= artifacts/analysis/mlb/review_aids/alternate_history/backfill
MLB_HITS_O15_ALTERNATE_HISTORY_BOOK_LEVEL_CSV ?= $(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)/$(MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE)/live_alternate_book_level_rows.csv
MLB_HITS_O15_ALTERNATE_HISTORY_SLATE_OUTPUT_CSV ?= backend/mlb/exports/odds_history/$(MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE)/mlb_slate_output.csv
MLB_HITS_O15_ALTERNATE_HISTORY_BUILD_SUMMARY_CSV ?= artifacts/analysis/mlb/review_aids/alternate_history/o15_alternate_history_build_summary.csv
MLB_HITS_O15_ALTERNATE_HISTORY_RECHECK_MD ?= artifacts/analysis/mlb/review_aids/alternate_history/o15_alternate_history_7day_recheck.md
MLB_EXPANDED_O15_UNIVERSE_DATE_FROM ?= $(DATE_FROM)
MLB_EXPANDED_O15_UNIVERSE_DATE_TO ?= $(DATE_TO)
MLB_EXPANDED_O15_UNIVERSE_DATE ?= $(DATE)
MLB_RESEARCH_SNAPSHOT_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_RESEARCH_SNAPSHOT_OUT_ROOT ?= artifacts/analysis/mlb
MLB_EXPANDED_O15_UNIVERSE_OUT_DIR ?= artifacts/analysis/mlb/expanded_o15_universe
MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV ?= $(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)/expanded_o15_universe_rows.csv
MLB_EXPANDED_O15_CONTEXT_HEALTH_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_EXPANDED_O15_CONTEXT_HEALTH_JSON ?= $(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)/expanded_o15_context_health_$(MLB_EXPANDED_O15_CONTEXT_HEALTH_DATE).json
MLB_O15_ONTOLOGY_HEALTH_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_O15_ONTOLOGY_HEALTH_OUT_DIR ?= artifacts/analysis/mlb/ontology
MLB_PROJECT_INVARIANTS_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_PROJECT_INVARIANTS_OUT_DIR ?= artifacts/analysis/mlb/invariants
MLB_INVARIANT_BACKLOG_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_INVARIANT_BACKLOG_OUT_DIR ?= artifacts/analysis/mlb/invariants
MLB_MORNING_WORKFLOW_AUDIT_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_MORNING_WORKFLOW_AUDIT_OUT_ROOT ?= artifacts/analysis/mlb
MAX_ODDSAPI_CALLS ?= 0
MAX_ODDSAPI_CREDITS ?= 0
MIN_ODDSAPI_REMAINING_CREDITS ?= 0
REQUIRE_CONFIRM ?= 1
MLB_HITS_U15_FAVORITE_AUDIT_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_HITS_U15_FAVORITE_AUDIT_OUT_DIR ?= artifacts/analysis/mlb/review_aids
MLB_DAILY_REVIEW_BOARDS_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_UPLOAD_PREP_DATE))
MLB_HITS_15_TIER_BACKTEST_OUT_DIR ?= artifacts/analysis/mlb/review_aids
MLB_HITS_15_TIER_BACKTEST_JSON ?= $(MLB_HITS_15_TIER_BACKTEST_OUT_DIR)/hits_15_tier_backtest_summary.json
MLB_REVIEW_AID_PERFORMANCE_OUT_DIR ?= artifacts/analysis/mlb/review_aids/performance
MLB_REVIEW_AID_PERFORMANCE_JSON ?= $(MLB_REVIEW_AID_PERFORMANCE_OUT_DIR)/review_aid_performance_summary.json
MLB_REHYDRATE_RECONCILE_ROLLING_DATE_FROM ?= $(if $(strip $(DATE_FROM)),$(DATE_FROM),2026-04-01)
MLB_REHYDRATE_RECONCILE_ROLLING_DATE_TO ?= $(if $(strip $(DATE_TO)),$(DATE_TO),2026-06-14)
MLB_REHYDRATE_RECONCILE_ROLLING_DRY_RUN ?= $(if $(strip $(DRY_RUN)),$(DRY_RUN),1)
MLB_REHYDRATE_RECONCILE_ROLLING_OUT_DIR ?= artifacts/analysis/mlb/feature_lineage
MLB_EXEC_TOOL_RESULTS_CSV ?=
MLB_EXEC_OUT_DIR ?= artifacts/analysis/mlb/execution_vs_model/$(MLB_DATE)
MLB_EXEC_RECONCILE_CSV ?= $(MLB_EXEC_OUT_DIR)/execution_reconcile_rows.csv
MLB_EXEC_RECONCILE_SUMMARY_JSON ?= $(MLB_EXEC_OUT_DIR)/execution_reconcile_summary.json
MLB_EXEC_RECONCILE_BOOKMAKER ?=
MLB_EXEC_RECONCILE_SLATE_FILENAME_MODE ?= all
MLB_EXEC_RECONCILE_SLATE_FILENAME_GLOB ?= mlb_slate_output*.csv
MLB_EXEC_RECONCILE_SNAPSHOT_POLICY ?= all
MLB_EXEC_RECONCILE_ODDS_FILENAME_MODE ?= all
MLB_EXEC_RECONCILE_ODDS_FILENAME_GLOB ?= odds_mlb_playerprops*.json
MLB_EXEC_RECONCILE_REQUIRE_TWO_SIDED ?= 1
MLB_EXEC_RECONCILE_REQUIRE_OUTCOMES ?= 1
MLB_EXEC_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN ?= 1
MLB_EXEC_OUT_CSV ?= $(MLB_EXEC_OUT_DIR)/execution_vs_model.csv
MLB_EXEC_OUT_JSON ?= $(MLB_EXEC_OUT_DIR)/summary.json
MLB_EXEC_OUT_MD ?= $(MLB_EXEC_OUT_DIR)/summary.md
MLB_EXEC_EXPECTED_RAW_TOOL_ROWS ?=
MLB_EXEC_EXPECTED_MLB_BETONLINE_ROWS ?=
MLB_EXEC_EXPECTED_MLB_BETONLINE_NON_PUSH_ROWS ?=
MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS ?= 1
MLB_RECONCILE_INCLUDE_ONE_SIDED ?= 0
MLB_RECONCILE_INCLUDE_SINGLE_BOOK ?= 0
MLB_FULL_SLATE_RECONCILE_CSV ?= $(MLB_EXEC_OUT_DIR)/reconcile_rows.csv
MLB_FULL_SLATE_RECONCILE_SUMMARY_JSON ?= $(MLB_EXEC_OUT_DIR)/reconcile_summary.json
MLB_FULL_SLATE_SUMMARY_MD ?= $(MLB_EXEC_OUT_DIR)/full_slate_summary.md
MLB_FULL_SLATE_BY_PROP_CSV ?= $(MLB_EXEC_OUT_DIR)/full_slate_by_prop.csv
MLB_FULL_SLATE_SNAPSHOT_POLICY ?= deduped_union
MLB_FULL_SLATE_SNAPSHOT_RUN_TAG ?=
MLB_FULL_SLATE_MIN_RESOLVED_ROWS ?= 0
MLB_ENABLE_O15_PROSPECTIVE_GRADER ?= 1
MLB_O15_PROSPECTIVE_GRADER_DATE ?= $(MLB_DAILY_RECONCILE_DATE)
MLB_O15_PROSPECTIVE_GRADER_RUN_DATE ?= 2026-07-17
MLB_O15_PROSPECTIVE_GRADER_RECONCILE_CSV ?= artifacts/analysis/mlb/execution_vs_model/$(MLB_O15_PROSPECTIVE_GRADER_DATE)/reconcile_rows.csv
MLB_O15_PROSPECTIVE_GRADER_OUT_DIR ?= artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_prospective
MLB_PROBABILITY_CALIBRATION_JSON ?= artifacts/analysis/mlb/calibration/mlb_probability_calibrator.json
MLB_APPLY_PROBABILITY_CALIBRATION_TO_UPLOAD ?= 0
MLB_CALIBRATION_TRAIN_CSV ?= tmp/mlb_base_vs_market_rows_anybook_window.csv
MLB_CALIBRATION_PROP_TYPES ?= hits,singles,total_bases,hits_runs_rbis,strikeouts_pitching,outs_recorded
MLB_CALIBRATION_FROM_DATE ?=
MLB_CALIBRATION_TO_DATE ?=
MLB_CALIBRATION_MIN_PROP_SAMPLES ?= 200
MLB_CALIBRATION_TRAINING_SCOPE ?= model_picks
MLB_CALIBRATION_OUT_DIR ?= artifacts/analysis/mlb/calibration
MLB_CALIBRATION_COMPARISON_CSV ?= $(MLB_CALIBRATION_OUT_DIR)/raw_vs_calibrated.csv
MLB_CALIBRATION_CURVE_CSV ?= $(MLB_CALIBRATION_OUT_DIR)/calibration_curve.csv
MLB_WEIGHTED_MODEL_DIR ?= $(CURDIR)/models_out/overlays/weighted540_hl90_full
MLB_WEIGHTED_SLATE_PRED_CSV ?= backend/mlb/data/processed/mlb_predictions_wide_calibrated_weighted.csv
MLB_WEIGHTED_SLATE_OUTPUT_CSV ?= backend/mlb/data/processed/mlb_slate_output_weighted.csv
MLB_UPLOAD_VARIANTS_BUILD_BASE ?= 1
MLB_BOOK_UPLOAD_FILTER_OUT_CSV ?= backend/mlb/data/processed/mlb_book_upload_top40_recommended.csv
MLB_BOOK_UPLOAD_FILTER_OUT_JSON ?= tmp/analysis/mlb_book_upload_filter_recommendation.json
MLB_BOOK_UPLOAD_FILTER_LOOKBACK_DAYS ?= 5
MLB_BOOK_UPLOAD_FILTER_WINDOWS_DAYS ?= 7,14
MLB_BOOK_UPLOAD_FILTER_TARGET_ROWS ?= 40
MLB_BOOK_UPLOAD_FILTER_MIN_MODEL_ROWS ?= 60
MLB_BOOK_UPLOAD_FILTER_MIN_MODEL_WIN_RATE_PCT ?= 52
MLB_BOOK_UPLOAD_FILTER_MIN_GRADED_ROWS ?= 8
MLB_BOOK_UPLOAD_FILTER_GRADED_ROI_FLOOR_PCT ?= -8
MLB_BOOK_UPLOAD_FILTER_MIN_OVERS ?= 4
MLB_BOOK_UPLOAD_MIN_SIDE_PROB ?= 0
MLB_PROP_REGIME_DAILY_RECONCILE_CSVS ?= $(wildcard artifacts/analysis/mlb/execution_vs_model/20??-??-??/reconcile_rows.csv)
MLB_PROP_REGIME_RECONCILE_CSVS ?= tmp/mlb_reconcile_rows_historical_bestbook_2024.csv tmp/mlb_reconcile_rows_historical_bestbook_2025.csv tmp/mlb_base_vs_market_rows_anybook_full.csv $(MLB_PROP_REGIME_DAILY_RECONCILE_CSVS)
MLB_PROP_REGIME_EXECUTION_CSV ?= artifacts/analysis/mlb/execution_vs_model/extended_clean/execution_vs_model.csv
MLB_PROP_REGIME_OUT_DIR ?= artifacts/analysis/mlb/prop_regime_validation
MLB_PROP_REGIME_DEPLOY_CSV ?= backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv
MLB_MODEL_PERFORMANCE_TO_DATE ?= $(MLB_POST_GRADE_DATE)
MLB_MODEL_PERFORMANCE_FROM_DATE ?= $(shell python3 -c 'from datetime import date,timedelta; print((date.fromisoformat("$(MLB_MODEL_PERFORMANCE_TO_DATE)")-timedelta(days=13)).isoformat())')
MLB_MODEL_PERFORMANCE_DAILY_CSV ?= backend/mlb/exports/model_performance/prop_daily_performance.csv
MLB_MODEL_PERFORMANCE_SUMMARY_CSV ?= backend/mlb/exports/model_performance/prop_rolling_summary.csv
MLB_MODEL_PERFORMANCE_SOURCE_TYPE ?= full_slate_model_pick
MLB_REPORTING_ALIGNMENT_DATE ?= $(MLB_MODEL_PERFORMANCE_TO_DATE)
MLB_REPORTING_ALIGNMENT_OUT_CSV ?= backend/mlb/exports/reporting_alignment/reporting_alignment_$(MLB_REPORTING_ALIGNMENT_DATE).csv
MLB_REPORTING_ALIGNMENT_OUT_MD ?= backend/mlb/exports/reporting_alignment/reporting_alignment_$(MLB_REPORTING_ALIGNMENT_DATE).md
MLB_BOOK_UPLOAD_SELECTION_MODE ?= policy
MLB_ODDS_HISTORY_ROOT ?= backend/mlb/exports/odds_history
MLB_ODDS_SNAPSHOT_JSON ?= $(MLB_ODDS_HISTORY_ROOT)/$(MLB_DATE)/odds_mlb_playerprops.json
MLB_ARCHIVE_RUN_TAG ?=
MLB_ODDS_SNAPSHOT_IN ?=
MLB_POLICY_PLAN_ENABLED ?= 0
MLB_POLICY_PLAN_CSV ?=
MLB_POLICY_PLAN_ALLOW_ONE_SIDED ?= 0
MLB_POLICY_PLAN_ALLOW_EMPTY ?= 1
MLB_PREDICT_REQUIRE_TWO_SIDED ?= 1
MLB_PREDICT_TWO_SIDED_BOOKMAKER ?=
# Props allowed to fall back to any two-sided bookmaker when MLB_PREDICT_TWO_SIDED_BOOKMAKER
# is explicitly set and missing/one-sided for that prop. Wide production output defaults
# to any two-sided book so full market-backed prop coverage is preserved.
MLB_PREDICT_TWO_SIDED_OPTIONAL_TARGET_BOOK_PROPS ?= singles
MLB_ODDS_BACKFILL_SEASON ?= 2025
MLB_ODDS_BACKFILL_FROM_DATE ?=
MLB_ODDS_BACKFILL_TO_DATE ?=
MLB_ODDS_BACKFILL_SNAPSHOT_TIME_ET ?= 13:00
MLB_ODDS_BACKFILL_MARKETS ?=
MLB_ODDS_BACKFILL_REGIONS ?= us,us2
MLB_ODDS_BACKFILL_MAX_MARKETS_PER_CALL ?= 6
MLB_ODDS_BACKFILL_MAX_DAYS ?=
MLB_ODDS_BACKFILL_SLEEP_MS ?= 150
MLB_ODDS_BACKFILL_OVERWRITE ?= 0
MLB_ODDS_BACKFILL_DRY_RUN ?= 0
MLB_RECONCILE_FROM_DATE ?= $(MLB_DATE)
MLB_RECONCILE_TO_DATE ?= $(MLB_DATE)
MLB_RECONCILE_BOOKMAKER ?= betonlineag
MLB_RECONCILE_SLATE_FILENAME ?= mlb_slate_output.csv
MLB_RECONCILE_SLATE_FILENAME_MODE ?= single
MLB_RECONCILE_SLATE_FILENAME_GLOB ?= mlb_slate_output*.csv
MLB_RECONCILE_SNAPSHOT_POLICY ?= all
MLB_RECONCILE_SNAPSHOT_RUN_TAG ?=
MLB_RECONCILE_ODDS_FILENAME ?= odds_latest_compatible.json
MLB_RECONCILE_ODDS_FILENAME_MODE ?= single
MLB_RECONCILE_ODDS_FILENAME_GLOB ?= odds_mlb_playerprops*.json
MLB_RECONCILE_ROWS_OUT_CSV ?= tmp/mlb_base_vs_market_rows.csv
MLB_RECONCILE_SUMMARY_OUT_JSON ?= tmp/mlb_base_vs_market_summary.json
MLB_RECONCILE_REQUIRE_TWO_SIDED ?= 1
MLB_RECONCILE_REQUIRE_OUTCOMES ?= 0
MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN ?= 0
MLB_POST_GRADE_REQUIRE_OUTCOMES ?= 1
MLB_POST_GRADE_REQUIRE_OUTCOME_ROWS_MIN ?= 1
MLB_MODEL_VS_FADE_ROWS_CSV ?= $(MLB_RECONCILE_ROWS_OUT_CSV)
MLB_MODEL_VS_FADE_OUT_JSON ?= tmp/analysis/mlb_model_vs_fade_summary.json
MLB_MODEL_VS_FADE_OUT_CSV ?= tmp/analysis/mlb_model_vs_fade_by_prop.csv
MLB_MODEL_VS_FADE_MIN_BETS_ALERT ?= 30
MLB_ALL_AVAILABLE_ROWS_CSV ?= $(MLB_RECONCILE_ROWS_OUT_CSV)
MLB_ALL_AVAILABLE_OUT_JSON ?= tmp/analysis/mlb_all_available_summary.json
MLB_ALL_AVAILABLE_OUT_CSV ?= tmp/analysis/mlb_all_available_by_prop.csv
MLB_ODDS_BUCKET_REQUIRE_TWO_SIDED ?= 1
MLB_QUALITY_RECONCILE_REQUIRE_TWO_SIDED ?= 1
MLB_RED_BUCKET_FROM_DATE ?= 2026-03-25
MLB_RED_BUCKET_TO_DATE ?= $(MLB_DATE_ET)
MLB_RED_BUCKET_BOOKMAKER ?= betonlineag
MLB_RED_BUCKET_ODDS_FILENAME ?= odds_latest_compatible.json
MLB_RED_BUCKET_ROWS_CSV ?= tmp/mlb_red_mode_rows.csv
MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON ?= tmp/mlb_red_mode_reconcile_summary.json
MLB_RED_BUCKET_SUMMARY_OUT_JSON ?= tmp/analysis/mlb_red_mode_odds_bucket_summary.json
MLB_RED_BUCKET_BY_BUCKET_OUT_CSV ?= tmp/analysis/mlb_red_mode_odds_bucket_by_bucket.csv
MLB_RED_BUCKET_FOCUS_OUT_CSV ?= tmp/analysis/mlb_red_mode_odds_bucket_focus.csv
MLB_RED_BUCKET_LAYOUT ?= ten
MLB_RED_BUCKET_FOCUS_BUCKETS ?=
MLB_RED_BUCKET_PRINT_BOTH_CONTRIBUTORS ?= 0
MLB_RED_BUCKET_OUTPUT_POSITIVE_ONLY ?= 0
MLB_RED_BUCKET_FADE_SUMMARY_OUT_JSON ?= tmp/analysis/mlb_red_mode_fade_odds_bucket_summary.json
MLB_RED_BUCKET_FADE_BY_BUCKET_OUT_CSV ?= tmp/analysis/mlb_red_mode_fade_odds_bucket_by_bucket.csv
MLB_RED_BUCKET_FADE_FOCUS_OUT_CSV ?= tmp/analysis/mlb_red_mode_fade_odds_bucket_focus.csv
MLB_RED_BUCKET_FADE_MIN_PRINT_ROI_PCT ?= 0
MLB_RED_SIDE_MATRIX_OUT_CSV ?= tmp/analysis/mlb_red_mode_side_matrix.csv
MLB_EARLY_STEAM_MOVEMENT_CSV ?= tmp/mlb_line_movement_*_imp.csv
MLB_EARLY_STEAM_MOVEMENT_OUT_CSV ?= tmp/mlb_line_movement_$(MLB_DATE)_mixedbook_imp.csv
MLB_EARLY_STEAM_MOVEMENT_SNAPSHOT_GLOB ?=
MLB_EARLY_STEAM_RECONCILE_CSV ?= tmp/mlb_reconcile_rows_*_full_slate_mixedbook.csv
MLB_EARLY_STEAM_ROWS_CSV ?= tmp/mlb_early_steam_multiday_results.csv
MLB_EARLY_STEAM_PITCHER_CANDIDATES_OUT_CSV ?= tmp/mlb_early_steam_pitcher_candidates_$(MLB_DATE).csv
MLB_EARLY_STEAM_PITCHER_CANDIDATES_SUMMARY_OUT_CSV ?= tmp/mlb_early_steam_pitcher_candidates_$(MLB_DATE)_summary.csv
MLB_EARLY_STEAM_PITCHER_MIN_IMP_MOVE ?= 0.02
MLB_EARLY_STEAM_PITCHER_MAX_IMP_MOVE ?= 0.05
MLB_EARLY_STEAM_PITCHER_PROFILE_LOGS_CSV ?= tmp/pitcher_game_logs_pybaseball_2026-04-16_to_2026-05-01.csv
MLB_EARLY_STEAM_PITCHER_PROFILE_OUT_CSV ?= tmp/mlb_early_steam_pitcher_profile_analysis.csv
MLB_EARLY_STEAM_PITCHER_PROFILE_SUMMARY_OUT_CSV ?= tmp/mlb_early_steam_pitcher_profile_summary.csv
MLB_EARLY_STEAM_PITCHER_PROFILE_STABLE_SUMMARY_OUT_CSV ?= tmp/mlb_early_steam_pitcher_profile_stable_summary.csv
MLB_EARLY_STEAM_PITCHER_PROFILE_STABLE_MIN_BETS ?= 5
MLB_EARLY_STEAM_V1_PITCHING_LOGS_CSV ?= tmp/pitcher_game_logs_pybaseball_2026-03-15_to_2026-05-01.csv
MLB_EARLY_STEAM_V1_PITCHING_OUT_CSV ?= tmp/mlb_early_steam_v1_pitching_candidates_$(MLB_DATE).csv
MLB_EARLY_STEAM_V1_MIN_OUTS_STD ?= 2.0
MLB_RETROSHEET_RAW_DIR ?= backend/mlb/data/raw/retrosheet
MLB_RETROSHEET_SEASON ?=
MLB_RETROSHEET_FORCE_DOWNLOAD ?= 0
MLB_CHADWICK_REGISTER_CSV ?= $(MLB_RETROSHEET_RAW_DIR)/chadwick_register/people.csv
MLB_CHADWICK_AUDIT_OUT_CSV ?= tmp/chadwick_register_mapping_audit.csv
START_DATE ?=
END_DATE ?=
MLB_PITCHER_GAME_LOGS_PYBASEBALL_OUT_CSV ?= tmp/pitcher_game_logs_pybaseball.csv
MLB_PITCHER_GAME_LOGS_PYBASEBALL_AUDIT_OUT_CSV ?= tmp/pitcher_game_logs_pybaseball_audit.csv
MLB_PITCHER_GAME_LOGS_PYBASEBALL_CHUNK_DAYS ?= 7
MLB_ONE_SIDED_CLEANUP_SCHEMA ?= mlb
MLB_ONE_SIDED_CLEANUP_TABLES ?=
MLB_ONE_SIDED_CLEANUP_OUT_JSON ?= artifacts/ops/mlb_one_sided_cleanup_latest.json
MLB_ONE_SIDED_CLEANUP_APPLY ?= 0
MLB_BOOK_UPLOAD_SIDE_MATRIX_BOOKMAKER ?= $(MLB_RED_BUCKET_BOOKMAKER)
MLB_BOOK_UPLOAD_SIDE_MATRIX_BUCKET_LAYOUT ?= legacy
MLB_BOOK_UPLOAD_SIDE_MATRIX_ALLOWED_STATUSES ?= play
MLB_BOOK_UPLOAD_SIDE_MATRIX_SELECTION_MODE ?= all-qualified
MLB_BOOK_UPLOAD_SIDE_MATRIX_OUT_CSV ?= backend/mlb/data/processed/mlb_book_upload_side_matrix.csv
MLB_BOOK_UPLOAD_SIDE_MATRIX_DATED_OUT_CSV ?= tmp/analysis/mlb_book_upload_side_matrix_$(subst -,,$(MLB_DATE)).csv
MLB_BOOK_UPLOAD_SIDE_MATRIX_DETAILS_OUT_CSV ?= tmp/analysis/mlb_book_upload_side_matrix_details_$(subst -,,$(MLB_DATE)).csv
MLB_BOOK_UPLOAD_SIDE_MATRIX_REFRESH_REPORTS ?= 0
MLB_BET_SHEET_HISTORY_ROWS_CSV ?= tmp/mlb_red_mode_rows.csv
MLB_BET_SHEET_DETAILS_CSV ?= tmp/analysis/mlb_book_upload_side_matrix_details_$(subst -,,$(MLB_DATE)).csv
MLB_BET_SHEET_UPLOAD_CSV ?= tmp/analysis/mlb_book_upload_side_matrix_$(subst -,,$(MLB_DATE)).csv
MLB_BET_SHEET_BOOKMAKER ?= betonlineag
MLB_BET_SHEET_SELECTION ?= fade
MLB_BET_SHEET_PROP_TYPES ?= total_bases
MLB_BET_SHEET_REQUIRED_SIDE ?= over
MLB_BET_SHEET_REQUIRED_PICK_TYPE ?= fade
MLB_BET_SHEET_MIN_LANE_ROWS ?= 20
MLB_BET_SHEET_MIN_LANE_ROI_PCT ?= 6
MLB_BET_SHEET_FAIL_IF_EMPTY ?= 0
MLB_BET_SHEET_OUT_UPLOAD_CSV ?= tmp/analysis/mlb_bet_sheet_$(subst -,,$(MLB_DATE))_tb_fade_20_6.csv
MLB_BET_SHEET_OUT_DETAILS_CSV ?= tmp/analysis/mlb_bet_sheet_$(subst -,,$(MLB_DATE))_tb_fade_20_6.details.csv
MLB_BET_SHEET_OUT_SUMMARY_JSON ?= tmp/analysis/mlb_bet_sheet_$(subst -,,$(MLB_DATE))_tb_fade_20_6.summary.json
MLB_REBUILD_TEST_ROWS_CSV ?= tmp/mlb_base_vs_market_rows.csv
MLB_REBUILD_TEST_FROM_DATE ?= 2026-03-25
MLB_REBUILD_TEST_TO_DATE ?= $(MLB_DATE_ET)
MLB_REBUILD_TEST_BOOKMAKER ?= betonlineag
MLB_REBUILD_TEST_REQUIRE_TWO_SIDED ?= 1
MLB_REBUILD_TEST_WARMUP_DAYS ?= 7
MLB_REBUILD_TEST_MIN_PROP_BETS ?= 30
MLB_REBUILD_TEST_MIN_POSITIVE_PROPS ?= 4
MLB_REBUILD_TEST_MAX_PROP_PNL_SHARE_PCT ?= 50
MLB_REBUILD_TEST_OUT_SUMMARY_CSV ?= tmp/analysis/mlb_rebuild_test_summary.csv
MLB_REBUILD_TEST_OUT_DAILY_CSV ?= tmp/analysis/mlb_rebuild_test_daily.csv
MLB_REBUILD_TEST_OUT_BY_PROP_CSV ?= tmp/analysis/mlb_rebuild_test_by_prop.csv
MLB_REBUILD_TEST_OUT_JSON ?= tmp/analysis/mlb_rebuild_test_latest.json
MLB_GRADED_IN_CSV ?=
MLB_GRADED_ROWS_OUT_CSV ?= tmp/analysis/mlb_graded_wagers_rows.csv
MLB_GRADED_SUMMARY_OUT_JSON ?= tmp/analysis/mlb_graded_wagers_summary.json
MLB_GRADED_BY_PROP_OUT_CSV ?= tmp/analysis/mlb_graded_wagers_by_prop.csv
MLB_GRADED_REPORT_REQUIRED ?= 0
MLB_POSTGRADE_INCLUDE_GRADED ?= 1
MLB_GRADER_IN_CSV ?=
MLB_POSTGRADE_TRACKER_DATE ?=
MLB_POSTGRADE_TRACKER_OUT_CSV ?= artifacts/mlb_postgrade_daily_tracker.csv
MLB_POSTGRADE_TRACKER_OUT_BY_PROP_CSV ?= artifacts/mlb_postgrade_by_prop_daily_tracker.csv
MLB_POSTGRADE_TRACKER_CHARTS_DIR ?= artifacts/analysis/mlb
MLB_POSTGRADE_TRACKER_SKIP_CHARTS ?= 0
MLB_POSTGRADE_ALLOW_GRADED_DATE_MISMATCH ?= 0
MLB_POSTGRADE_ALERTS_OUT_JSON ?= artifacts/analysis/mlb/mlb_postgrade_alerts_latest.json
MLB_POSTGRADE_ALERTS_HISTORY_JSONL ?= artifacts/analysis/mlb/mlb_postgrade_alerts_history.jsonl
MLB_POSTGRADE_ALERT_FADE_MIN_PAIRED_BETS ?= 30
MLB_POSTGRADE_ALERT_ROI_MIN_PAIRED_BETS ?= 30
MLB_POSTGRADE_ALERT_ROI_BREACH_THRESHOLD ?= -0.08
MLB_POSTGRADE_ALERT_OVERALL_DROP_WINDOW_DAYS ?= 3
MLB_POSTGRADE_ALERT_OVERALL_DROP_THRESHOLD_PCT ?= 5
MLB_POSTGRADE_ALERT_PROP_DROP_WINDOW_DAYS ?= 3
MLB_POSTGRADE_ALERT_PROP_DROP_THRESHOLD_PCT ?= 8
MLB_POSTGRADE_ALERT_PROP_DROP_MIN_MODEL_ROWS ?= 20
MLB_POSTGRADE_ALERTS_STRICT ?= 0
MLB_POLICY_REPLAY_ROWS_CSV ?= tmp/mlb_reconcile_rows_2024_2025_prod11_allbooks_noncollapsed.csv
MLB_POLICY_REPLAY_OUT_DIR ?= tmp/analysis/mlb_baseline_readiness_pack/pass4_execution_replay
MLB_POLICY_MONITOR_PROPS ?= doubles,walks_allowed
MLB_POLICY_MONITOR_MIN_BETS_ALERT ?= 30
MLB_WIDE_PROP_TYPES ?= $(MLB_PROD12_PROP_TYPES)
MLB_WIDE_REQUIRE_MIN_ROWS ?= 1
MLB_DAILY_INCLUDE_CAPTURE ?= 1
MLB_DAILY_BVP_IMPACT_ENABLED ?= 1
MLB_DAILY_BVP_IMPACT_REQUIRED ?= 0
MLB_DAILY_HITS_ENV_ENABLED ?= 1
MLB_DAILY_HITS_ENV_REQUIRED ?= 0
MLB_DAILY_OPS_BRIEF_ENABLED ?= 1
MLB_DAILY_OPS_BRIEF_REQUIRED ?= 0
MLB_HITS_ENV_AS_OF_DATE ?= $(MLB_DATE_ET)
MLB_HITS_ENV_LOOKBACK_DAYS ?= 30
MLB_HITS_ENV_RECENT_DAYS ?= 7
MLB_HITS_ENV_STARTER_BASELINE_SEASONS ?= 3
MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS ?= 5
MLB_HITS_ENV_STARTER_BASELINE_DECAY ?= 0.70
MLB_HITS_ENV_SLATE_WEIGHT_LAST7 ?= 0.50
MLB_HITS_ENV_SLATE_WEIGHT_LAST15 ?= 0.30
MLB_HITS_ENV_SLATE_WEIGHT_LAST30 ?= 0.20
MLB_HITS_ENV_SLATE_FACTOR_MIN ?= 0.70
MLB_HITS_ENV_SLATE_FACTOR_MAX ?= 1.30
MLB_HITS_ENV_SLATE_DATE ?= $(MLB_DATE)
MLB_HITS_ENV_SLATE_CSV ?= $(MLB_SLATE_OUTPUT_CSV)
MLB_HITS_ENV_WIDE_CSV ?= $(MLB_SLATE_PRED_CSV)
MLB_HITS_ENV_ODDS_SNAPSHOT ?= backend/mlb/exports/odds_history/$(MLB_HITS_ENV_SLATE_DATE)/odds_latest_compatible.json
MLB_HITS_ENV_OUT_JSON ?= artifacts/analysis/mlb/mlb_hits_environment_latest.json
MLB_HITS_ENV_OUT_CSV ?= tmp/analysis/mlb_hits_environment_hits_allowed_rows.csv
MLB_HITS_ENV_SNAPSHOT_DIR ?= artifacts/analysis/mlb/hits_environment_snapshots
MLB_HITS_ENV_HISTORY_JSONL ?= artifacts/analysis/mlb/mlb_hits_environment_history.jsonl
MLB_HITS_ENV_EVAL_TRACKER_CSV ?= artifacts/analysis/mlb/mlb_hits_environment_team_eval_daily_tracker.csv
MLB_HITS15_ENV_LINEAGE_DATE ?= $(if $(strip $(DATE)),$(DATE),$(shell date +%F))
MLB_HITS15_ENV_LINEAGE_OUT_DIR ?= artifacts/analysis/mlb/review_aids
MLB_HITS15_ENV_V2_ALPHA_OUT_DIR ?= artifacts/analysis/mlb/review_aids
MLB_ENVIRONMENT_V2_BETA_DAILY_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DATE_ET))
MLB_ENVIRONMENT_V2_BETA_DAILY_OUT_ROOT ?= artifacts/analysis/mlb/environment_v2/daily
MLB_ENVIRONMENT_V2_BETA_DAILY_WRAPPER_MODE ?= manual_target_only
MLB_ENVIRONMENT_V2_BETA_RECONCILE_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DAILY_RECONCILE_DATE))
MLB_ENVIRONMENT_V2_BETA_RECONCILE_ROOT ?= artifacts/analysis/mlb/execution_vs_model
MLB_ENVIRONMENT_V2_BETA_LEDGER_CSV ?= artifacts/analysis/mlb/environment_v2/ledger/environment_v2_beta_profile_ledger.csv
MLB_DAILY_BRIEF_REPORT_DATE ?= $(MLB_DATE_ET)
MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE ?= $(MLB_DAILY_RECONCILE_DATE)
MLB_DAILY_BRIEF_CURRENT_SLATE_DATE ?= $(MLB_DATE_ET)
MLB_DAILY_BRIEF_POSTGRADE_ALERTS_JSON ?= $(MLB_POSTGRADE_ALERTS_OUT_JSON)
MLB_DAILY_BRIEF_MODEL_VS_FADE_JSON ?= $(MLB_MODEL_VS_FADE_OUT_JSON)
MLB_DAILY_BRIEF_BVP_IMPACT_JSON ?= $(MLB_BVP_IMPACT_OUT_JSON)
MLB_DAILY_BRIEF_HITS_ENV_JSON ?= $(MLB_HITS_ENV_OUT_JSON)
MLB_DAILY_BRIEF_USER_OVER_15_WATCH_JSON ?= artifacts/analysis/mlb/user_over_15_filter_watch.json
MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_SUMMARY_JSON ?= artifacts/analysis/mlb/model_quality/total_bases_shadow/$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)/total_bases_shadow_summary_$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE).json
MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_EVALUATION_JSON ?= artifacts/analysis/mlb/model_quality/total_bases_shadow/evaluation/total_bases_shadow_evaluation_summary.json
MLB_ENABLE_ROLLING_CANDIDATE_OBS ?= auto
MLB_ROLLING_CANDIDATE_OBS_JSON ?= artifacts/analysis/mlb/market_late_candidate_discovery/rolling_observation_$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)/rolling_candidate_ops_brief_input_$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE).json
MLB_ROLLING_OBSERVATION_DATE ?= $(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)
MLB_ROLLING_OBSERVATION_EFFECTIVE_DATE = $(if $(filter YYYY-MM-DD yyyy-mm-dd <DATE> DATE,$(strip $(MLB_ROLLING_OBSERVATION_DATE))),$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE),$(MLB_ROLLING_OBSERVATION_DATE))
MLB_ROLLING_OBSERVATION_OUT_DIR ?= artifacts/analysis/mlb/market_late_candidate_discovery/rolling_observation_$(MLB_ROLLING_OBSERVATION_EFFECTIVE_DATE)
MLB_ROLLING_OBSERVATION_INDEX_DATE ?= $(MLB_ROLLING_OBSERVATION_EFFECTIVE_DATE)
MLB_ROLLING_OBSERVATION_ROOT ?= artifacts/analysis/mlb/market_late_candidate_discovery
MLB_DAILY_ROLLING_OBS_REFRESH_BRIEF_INPUTS ?= 0
MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE ?= 0
MLB_LIVE_HITTER_PARENT_DATE ?= $(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)
MLB_LIVE_HITTER_PARENT_RUN_TAG ?=
MLB_LIVE_HITTER_PARENT_CUTOFF ?=
MLB_LIVE_HITTER_PARENT_SLATE_ARTIFACT ?=
MLB_LIVE_HITTER_PARENT_LINEUP_PLAYER_ROWS ?=
MLB_LIVE_HITTER_PARENT_OPPORTUNITY_PROFILE_PARENT ?=
MLB_LIVE_HITTER_PARENT_OUT_DIR ?= artifacts/analysis/model_development/mlb_live_hitter_parent_daily_integration/$(MLB_LIVE_HITTER_PARENT_DATE)
MLB_ENABLE_HITS05_LIVE_PA_SHADOW ?= 0
MLB_HITS05_LIVE_PA_SHADOW_DATE ?= $(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)
MLB_HITS05_LIVE_PA_SHADOW_RUN_TAG ?=
MLB_HITS05_LIVE_PA_SHADOW_PREDICTION_TIMESTAMP ?=
MLB_HITS05_LIVE_PA_SHADOW_CURRENT_PARENT_DIR ?=
MLB_HITS05_LIVE_PA_SHADOW_OUTPUT_ROOT ?= artifacts/analysis/model_development/mlb_hits05_live_expected_pa_parent_pilot/2026-07-21
MLB_GOVERNED_LINEUP_CAPTURE_OUT_DIR ?= artifacts/analysis/model_development/mlb_governed_pregame_lineup_capture/$(MLB_LIVE_HITTER_PARENT_DATE)
MLB_GOVERNED_LINEUP_CAPTURE_RUN_TAG ?= $(MLB_LIVE_HITTER_PARENT_RUN_TAG)
MLB_GOVERNED_LINEUP_CAPTURE_CUTOFF ?= $(MLB_LIVE_HITTER_PARENT_CUTOFF)
MLB_GOVERNED_LINEUP_CAPTURE_PARSED ?= $(MLB_GOVERNED_LINEUP_CAPTURE_OUT_DIR)/parsed_lineup_artifact_$(MLB_LIVE_HITTER_PARENT_DATE).csv
MLB_USER_OVER_15_FILTER_WATCH_CSV ?= artifacts/analysis/mlb/user_over_15_filter_watch.csv
MLB_USER_OVER_15_FILTER_WATCH_MD ?= artifacts/analysis/mlb/user_over_15_filter_watch.md
MLB_DAILY_BRIEF_PIPELINE_HISTORY_JSONL ?= $(MLB_PIPELINE_HISTORY_INPUT)
MLB_DAILY_BRIEF_OPS_HISTORY_JSONL ?= $(MLB_PROD12_OPS_HISTORY_INPUT)
MLB_DAILY_BRIEF_OUT_MD ?= artifacts/analysis/mlb/mlb_daily_ops_brief_latest.md
MLB_DAILY_BRIEF_DATED_OUT_MD ?= artifacts/analysis/mlb/mlb_daily_ops_brief_$(MLB_DAILY_BRIEF_REPORT_DATE).md
MLB_DAILY_BRIEF_OUT_JSON ?= artifacts/analysis/mlb/mlb_daily_ops_brief_latest.json
MLB_DAILY_BRIEF_HISTORY_JSONL ?= artifacts/analysis/mlb/mlb_daily_ops_brief_history.jsonl
MLB_DAILY_BRIEF_INPUT_REFRESH_STATUS_JSON ?= artifacts/analysis/mlb/mlb_daily_ops_brief_input_refresh_latest.json
MLB_BETONLINE_CAPTURE_INTEGRITY_JSON ?= artifacts/analysis/mlb/betonline_capture_integrity/$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)/betonline_capture_integrity_daily_summary_$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE).json
MLB_DAILY_BRIEF_RECONCILE_ROWS_CSV ?= artifacts/analysis/mlb/execution_vs_model/$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)/reconcile_rows.csv
MLB_DAILY_FEATURE_LINEAGE_HEALTH_JSON ?= artifacts/analysis/mlb/feature_lineage/daily_feature_lineage_health_latest.json
MLB_DAILY_FEATURE_LINEAGE_HEALTH_DATED_JSON ?= artifacts/analysis/mlb/feature_lineage/daily_feature_lineage_health_$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE).json
MLB_DAILY_FEATURE_LINEAGE_HEALTH_MD ?= artifacts/analysis/mlb/feature_lineage/daily_feature_lineage_health_$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE).md
MLB_DAILY_FEATURE_LINEAGE_HEALTH_NULL_WARN_THRESHOLD ?= 0.05
MLB_PA_FOUNDATION_HEALTH_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE))
MLB_PA_FOUNDATION_HEALTH_OUT_DIR ?= artifacts/analysis/mlb/pa_foundation
MLB_PA_FOUNDATION_PROPAGATION_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE))
MLB_PA_FOUNDATION_PROPAGATION_COMPLETED_DATE ?= $(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)
MLB_PA_FOUNDATION_PROPAGATION_OUT_DIR ?= artifacts/analysis/mlb/pa_foundation
MLB_PA_OPPORTUNITY_SHADOW_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE))
MLB_PA_OPPORTUNITY_SHADOW_OUT_DIR ?= artifacts/analysis/mlb/pa_foundation
MLB_DAILY_BRIEF_REFRESH_INPUTS ?= 1
MLB_DAILY_BRIEF_ALLOW_GRADED_DATE_MISMATCH ?= 1
MLB_DAILY_BRIEF_REFRESH_BVP_IMPACT ?= 0
MLB_DAILY_BRIEF_BVP_IMPACT_TIMEOUT_SEC ?= 180
MLB_DAILY_BRIEF_REQUIRE_FRESH_BVP_IMPACT ?= 1
MLB_DAILY_PREFLIGHT_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE))
MLB_DAILY_PREFLIGHT_OUT_JSON ?= artifacts/analysis/mlb/orchestration/mlb_daily_preflight_$(MLB_DAILY_PREFLIGHT_DATE).json
MLB_DAILY_PREFLIGHT_OUT_MD ?= artifacts/analysis/mlb/orchestration/mlb_daily_preflight_$(MLB_DAILY_PREFLIGHT_DATE).md
MLB_DAILY_INDEX_DATE ?= $(if $(strip $(DATE)),$(DATE),$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE))
MLB_DAILY_INDEX_COMPLETED_SLATE_DATE ?= $(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)
MLB_DAILY_INDEX_OUT_ROOT ?= artifacts/analysis/mlb
MLB_MORNING_OPERATING_SYSTEM_DATE ?= $(MLB_DAILY_INDEX_DATE)
MLB_MORNING_OPERATING_SYSTEM_OUT_ROOT ?= artifacts/analysis/mlb
MLB_O15_MORNING_WORKBENCH_DATE ?= $(MLB_DAILY_INDEX_DATE)
MLB_O15_MORNING_WORKBENCH_OUT_DIR ?= artifacts/analysis/mlb/review_aids/performance
MLB_ROSTER_DATE ?= $(shell date +%F)
NHL_ROSTER_DATE ?= $(shell date +%F)
MLB_STAT_DERIVED_DAYS ?= 7
MLB_STAT_DERIVED_MIN ?= 0
MLB_ROLLING_CHECK_DAYS ?= 10
MLB_ROLLING_CHECK_FROM_DATE ?=
MLB_ROLLING_CHECK_TO_DATE ?=
MLB_ROLLING_CHECK_MIN_COVERAGE_PCT ?= 99
MLB_ROLLING_CHECK_MIN_COMPARABLE ?= 100
MLB_PA_BATCH_SIZE ?= 500
MLB_PA_DRY_RUN ?= 0
MLB_PA_LIMIT_GAMES ?=
MLB_PA_DATE_FROM ?= $(MLB_DATE)
MLB_PA_DATE_TO ?= $(MLB_DATE)
MLB_PA_ONLY_MISSING ?= 1
TMP_RETENTION_DAYS ?= 7
TMP_FAT_CSV_MIN_MB ?= 10
TMP_FAT_CSV_MIN_AGE_DAYS ?= 2
MLB_TMP_FOCUS_DATE ?= $(MLB_DATE_ET)
MLB_TMP_FOCUS_ROOT ?= backend/mlb/data/processed/mlb_uploads
MLB_ODDS_HISTORY_RETENTION_DAYS ?= 365
MLB_ODDS_HISTORY_ARCHIVE_ROOT ?=
MLB_ODDS_HISTORY_LOCAL_RETENTION_DAYS ?= 180
ARTIFACTS_RETENTION_DAYS ?= 30
MLB_PREDICT_SAMPLE ?= 10
MLB_PREDICT_MIN_SUCCESS ?= 3
MLB_PREDICT_PROP_TYPES ?= $(MLB_PROD12_PROP_TYPES)
MLB_QUALITY_WINDOW_DAYS ?= 120
MLB_QUALITY_WINDOW_MODE ?= days
MLB_QUALITY_GAMES_BACK ?= 30
MLB_QUALITY_MIN_TOTAL ?= 1000
MLB_QUALITY_MIN_ACCURACY ?= 48
MLB_QUALITY_SOURCE_TABLE ?= model_training_props
MLB_QUALITY_ROWS_CSV ?=
MLB_QUALITY_PROP_SOURCES ?= mlb_api
MLB_QUALITY_RECONCILE_TWO_SIDED_ARG = $(if $(filter 1 true TRUE yes YES,$(MLB_QUALITY_RECONCILE_REQUIRE_TWO_SIDED)),--reconcile-require-two-sided,)
MLB_RECOMPUTE_PROP_TYPES ?= runs_scored,runs_rbis,hits_runs_rbis
MLB_RECOMPUTE_PROP_SOURCE ?= mlb_api
MLB_RECOMPUTE_DAYS_BACK ?= 35
MLB_RECOMPUTE_FROM_DATE ?=
MLB_RECOMPUTE_TO_DATE ?=
MLB_RECOMPUTE_LIMIT ?= 0
MLB_RECOMPUTE_REQUIRE_REGULAR ?= 1
MLB_RECOMPUTE_FORCE_INVERT_PROPS ?=
MLB_RECOMPUTE_GATE_MIN_TOTAL_PER_PROP ?= 200
MLB_RECOMPUTE_GATE_MIN_ACCURACY_PCT ?= 45
# Per-prop gate override (used by looped recompute targets).
MLB_RECOMPUTE_GATE_MIN_ACCURACY_HITS_RUNS_RBIS_PCT ?= 36
# Non-blocking recompute lanes (typically derived-only props not consistently market-backed).
MLB_RECOMPUTE_NON_BLOCKING_PROPS ?= runs_rbis
MLB_RECOMPUTE_BATCH_PROP_TYPES ?= $(MLB_CORRECTED_PROP_TYPES)
MLB_RECOMPUTE_REQUIRE_REGULAR_ARG = $(if $(filter 1,$(MLB_RECOMPUTE_REQUIRE_REGULAR)),--require-regular-season,)
MLB_CORRECTED_PROP_TYPES ?= runs_scored,runs_rbis,hits_runs_rbis
MLB_HYBRID_PROP_WINDOWS ?= hits_runs_rbis:540,runs_rbis:540,runs_scored:540,walks_allowed:730
MLB_HYBRID_TRAIN_LIMIT ?= 150000
MLB_HYBRID_RECOMPUTE_DAYS_BACK ?= 30
MLB_HYBRID_RECOMPUTE_LIMIT ?= 8000
MLB_TRAIN_FEATURE_SOURCE ?= reconcile_csv
MLB_TRAIN_PROFILE ?= legacy
MLB_TRAIN_MARKET_ONLY ?= 0
MLB_TRAIN_RECONCILE_ROWS_CSV ?= $(MLB_RECONCILE_ROWS_OUT_CSV)
MLB_TRAIN_RECONCILE_BOOKMAKER ?=
MLB_TRAIN_RECONCILE_REQUIRE_TWO_SIDED ?= 1
MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE ?= 0
MLB_TRAIN_MIN_CLASS_COUNT ?= 100
MLB_TRAIN_MIN_MINORITY_PCT ?= 0.10
MLB_RETRAIN_BOL_PROP_TYPES ?= $(MLB_PROD12_PROP_TYPES)
MLB_RETRAIN_BOL_DAYS_BACK ?= 540
MLB_RETRAIN_BOL_TRAIN_LIMIT ?= 150000
MLB_RETRAIN_BROAD_PROP_TYPES ?= $(MLB_PROD12_PROP_TYPES)
# Reconcile_csv + two-sided lane uses market-backed active props. Keep runs_rbis
# out of the active lane unless a compatible market-backed source returns.
MLB_PROD12_RECONCILE_PROP_TYPES ?= hits,singles,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,rbis
MLB_RETRAIN_BROAD_RECONCILE_PROP_TYPES ?= $(MLB_PROD12_RECONCILE_PROP_TYPES)
MLB_RETRAIN_BROAD_DAYS_BACK ?= 540
MLB_RETRAIN_BROAD_TRAIN_LIMIT ?= 150000
MLB_RETRAIN_BROAD_RECOMPUTE_DAYS_BACK ?= 30
# Weekly local retrain defaults to full-window recompute; set >0 only if you intentionally cap runtime.
MLB_RETRAIN_BROAD_RECOMPUTE_LIMIT ?= 0
MLB_RETRAIN_QUALITY_MIN_TOTAL ?= $(MLB_QUALITY_MIN_TOTAL)
MLB_FEATURE_WINDOW_MODE ?= games
MLB_FEATURE_WINDOW_DAYS ?= 120
MLB_FEATURE_GAMES_BACK ?= 30
MLB_FEATURE_PROP_TYPES ?= $(MLB_PROD12_PROP_TYPES)
MLB_FEATURE_PROP_SOURCES ?= mlb_api
MLB_FEATURE_WARN_DEFAULT_PCT ?= 35
MLB_FEATURE_WARN_MIN_ROWS ?= 200
MLB_FEATURE_FAIL_ON_WARN ?= 0
MLB_PFP_OVERLAP_PROP_TYPES ?= $(MLB_PROD12_PROP_TYPES)
MLB_PFP_OVERLAP_PROP_SOURCE ?= mlb_api
MLB_PFP_OVERLAP_FEATURE_SET_TAG ?= v1
MLB_PFP_OVERLAP_MODEL_TAG ?= mtp_overlap_backfill_v1
MLB_PFP_OVERLAP_WINDOW_MODE ?= games
MLB_PFP_OVERLAP_GAMES_BACK ?= 30
MLB_PFP_OVERLAP_WINDOW_DAYS ?= 120
MLB_PFP_OVERLAP_FROM_DATE ?=
MLB_PFP_OVERLAP_TO_DATE ?=
MLB_PFP_OVERLAP_LIMIT ?= 0
MLB_PFP_OVERLAP_BATCH_SIZE ?= 1000
MLB_BVP_DATE ?= $(MLB_DATE_ET)
MLB_BVP_FROM_DATE ?=
MLB_BVP_TO_DATE ?=
MLB_BVP_FEATURE_SET_TAG ?= $(MLB_PFP_OVERLAP_FEATURE_SET_TAG)
MLB_BVP_MODEL_TAG ?= bvp_pvb_refresh_v1
MLB_BVP_BATCH_SIZE ?= 1000
MLB_BVP_REQUEST_TIMEOUT_SEC ?= 20
MLB_BVP_REQUEST_RETRIES ?= 3
MLB_BVP_DRY_RUN ?= 0
MLB_DAILY_BVP_PVB_ENABLED ?= 1
MLB_BVP_IMPACT_SLATE_CSV ?= $(MLB_SLATE_OUTPUT_CSV)
MLB_BVP_IMPACT_WIDE_CSV ?= $(MLB_SLATE_PRED_CSV)
MLB_BVP_IMPACT_OUT_JSON ?= artifacts/analysis/mlb/mlb_bvp_impact_latest.json
MLB_BVP_IMPACT_OUT_CSV ?= tmp/analysis/mlb_bvp_impact_rows.csv
MLB_BVP_IMPACT_HISTORY_JSONL ?= artifacts/analysis/mlb/mlb_bvp_impact_history.jsonl
MLB_BVP_IMPACT_LABEL_DATE ?= $(MLB_DATE_ET)
MLB_BVP_IMPACT_MAX_ROWS ?= 0
MLB_BVP_IMPACT_REQUIRE_DB ?= 1
MLB_BVP_IMPACT_PREFLIGHT_MEDIUM_ROWS ?= 700
MLB_BVP_IMPACT_PREFLIGHT_HIGH_ROWS ?= 1500
MLB_BVP_IMPACT_PREFLIGHT_FAIL_HIGH ?= 0
MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT ?= 60
MLB_MODEL_ROOT ?= $(if $(MODEL_DIR),$(MODEL_DIR),/var/data/proppadia/models)
MLB_MODEL_LATEST_DIR ?= $(MLB_MODEL_ROOT)/latest
MLB_MODEL_ARCHIVE_DIR ?= $(MLB_MODEL_ROOT)/archive
MLB_MODEL_SNAPSHOT_SOURCE ?= $(MLB_MODEL_LATEST_DIR)
MLB_MODEL_SNAPSHOT_ID ?= $(shell date -u +%Y%m%dT%H%M%SZ)
MLB_MODEL_MANIFEST_OUTPUT ?= artifacts/releases/mlb_model_snapshot_$(MLB_MODEL_SNAPSHOT_ID).json
MLB_MODEL_PRUNE_KEEP ?= 3
MLB_MODEL_PRUNE_DRY_RUN ?= 1
MLB_MODEL_PUBLISH_SNAPSHOT ?=
MLB_MODEL_ROLLBACK_SNAPSHOT ?=
MLB_BALANCE_GUARD_PROP_TYPE ?= runs_scored
MLB_BALANCE_GUARD_PROP_SOURCES ?= mlb_api
MLB_BALANCE_GUARD_WINDOW_MODE ?= games
MLB_BALANCE_GUARD_WINDOW_DAYS ?= 30
MLB_BALANCE_GUARD_GAMES_BACK ?= 30
MLB_BALANCE_GUARD_MIN_TOTAL ?= 1000
MLB_BALANCE_GUARD_MIN_ACCURACY ?= 48
MLB_BALANCE_GUARD_MIN_OVER_PCT ?= 10
MLB_QUALITY_SEGMENT_PRESEASON_FROM_DATE ?=
MLB_QUALITY_SEGMENT_PRESEASON_TO_DATE ?=
MLB_QUALITY_SEGMENT_REGULAR_FROM_DATE ?=
MLB_QUALITY_SEGMENT_REGULAR_TO_DATE ?=
MLB_QUALITY_SEGMENT_MIN_PRESEASON_TOTAL ?= 1
MLB_QUALITY_SEGMENT_MIN_REGULAR_TOTAL ?= 1
MLB_RETRAIN_FRESHNESS_DAYS ?= 7
MLB_RETRAIN_FRESHNESS_MIN_ROWS ?= 1
MLB_RETRAIN_COVERAGE_WINDOW_MODE ?= games
MLB_RETRAIN_COVERAGE_WINDOW_DAYS ?= 30
MLB_RETRAIN_COVERAGE_GAMES_BACK ?= 30
MLB_RETRAIN_REQUIRED_PROPS ?= $(MLB_CORE_PROP_TYPES)
MLB_RETRAIN_MIN_TRAINING_SOURCE_PER_PROP ?= $(MLB_CORE_MIN_GRADED)
MLB_RETRAIN_TRAINING_PROP_SOURCES ?= mlb_api,user_added
MLB_RETRAIN_GRADING_WINDOW_MODE ?= games
MLB_RETRAIN_GRADING_WINDOW_DAYS ?= 30
MLB_RETRAIN_GRADING_GAMES_BACK ?= 30
MLB_RETRAIN_GRADING_PROP_TYPES ?= $(MLB_CORE_PROP_TYPES)
MLB_RETRAIN_GRADING_MIN_TOTAL ?= 1000
MLB_RETRAIN_BASELINE_MAX_AGE_HOURS ?= 0
MLB_CANDIDATE_BASELINE_PATH ?=
MLB_CANDIDATE_BASELINE_DIR ?= artifacts/season_baselines
MLB_PROD8_BASELINE_DIR ?= artifacts/season_baselines
MLB_CANDIDATE_SOURCE_TABLE ?= model_training_props
MLB_CANDIDATE_ROWS_CSV ?=
MLB_CANDIDATE_WINDOW_MODE ?=
MLB_CANDIDATE_WINDOW_DAYS ?= 120
MLB_CANDIDATE_GAMES_BACK ?= 30
MLB_CANDIDATE_PROP_TYPES ?= $(MLB_CORE_PROP_TYPES)
MLB_CANDIDATE_REQUIRED_PROPS ?= $(MLB_CORE_PROP_TYPES)
MLB_PROD12_CANDIDATE_PROP_TYPES ?= $(MLB_PROD12_PROP_TYPES)
# Reconcile candidate eval should mirror the reconcile broad train scope above.
MLB_PROD12_CANDIDATE_PROP_TYPES_RECONCILE ?= $(MLB_PROD12_RECONCILE_PROP_TYPES)
# Keep reconcile-required stability set scoped to active market-backed props.
MLB_PROD12_CANDIDATE_REQUIRED_PROPS ?= hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,rbis
MLB_PROD12_CANDIDATE_SOURCE_TABLE ?= reconcile_rows
MLB_PROD12_CANDIDATE_ROWS_CSV ?= tmp/mlb_base_vs_market_rows_anybook.csv
MLB_CANDIDATE_MIN_TOTAL ?= -1
MLB_CANDIDATE_MIN_LIFT_PCT ?= 0.25
# Broad candidate gate stays looser than phase2: tighten in stages (8 now, consider 6 after 2-3 clean runs).
MLB_CANDIDATE_MAX_PROP_DROP_PCT ?= 8.0
MLB_CANDIDATE_MIN_BASELINE_PROP_TOTAL_FOR_DROP ?= 300
MLB_CANDIDATE_MIN_COVERAGE_RATIO_FOR_DROP ?= 0.5
MLB_CANDIDATE_PROP_TIER_CONFIG ?=
MLB_PROD12_MAX_PROP_DROP_PCT ?= 3.5
MLB_PROD12_MIN_LIFT_PCT ?= -0.25
MLB_PROD12_MIN_BASELINE_PROP_TOTAL_FOR_DROP ?= $(MLB_CANDIDATE_MIN_BASELINE_PROP_TOTAL_FOR_DROP)
MLB_PROD12_MIN_COVERAGE_RATIO_FOR_DROP ?= $(MLB_CANDIDATE_MIN_COVERAGE_RATIO_FOR_DROP)
MLB_PROD12_PROP_TIER_CONFIG ?= backend/mlb/config/prod12_phase2_prop_tiers.json
MLB_PROD12_RELEASE_OUTPUT ?= artifacts/releases/mlb_prod12_release_manifest.json
MLB_PROD12_ARTIFACT_DIRS ?= models_out
MLB_PROD12_ARTIFACT_PATTERNS ?= *.joblib,*.pkl,*.onnx,*.bin
MLB_PROD12_REPLAY_OUTPUT ?= artifacts/releases/mlb_prod12_replay_latency.json
MLB_PROD12_PHASE2_HISTORY_INPUT ?= artifacts/mlb_prod12_phase2_history.jsonl
MLB_PROD12_PHASE2_BASELINE_PATH ?= artifacts/season_baselines/mlb_prod12_phase2_quality_games_$(MLB_CANDIDATE_GAMES_BACK)_$(MLB_CANDIDATE_WINDOW_DAYS)_reconcile_rows_anybook.json
MLB_PROD12_DAILY_MAX_AGE_HOURS ?= 30
MLB_PROD12_WEEKLY_MAX_AGE_HOURS ?= 240
MLB_PROD12_BOOTSTRAP_MAX_AGE_HOURS ?= 2
MLB_PROD12_HEALTH_DAILY_WINDOW ?= 14
MLB_PROD12_HEALTH_WEEKLY_WINDOW ?= 8
MLB_PROD12_OPS_HISTORY_INPUT ?= artifacts/mlb_prod12_ops_history.jsonl
# Ops snapshot logging should capture current state without freshness gating.
# Freshness enforcement is handled by mlb-prod12-status-strict.
MLB_PROD12_OPS_DAILY_MAX_AGE_HOURS ?= 0
MLB_PROD12_OPS_WEEKLY_MAX_AGE_HOURS ?= 0
MLB_REPLAY_SAMPLE ?= $(MLB_PREDICT_SAMPLE)
MLB_REPLAY_MIN_SUCCESS ?= $(MLB_PREDICT_MIN_SUCCESS)
MLB_REPLAY_MAX_PREDICT_P95_MS ?= 4000
MLB_REPLAY_ALLOW_SPARSE ?= 1
MLB_REPLAY_RETRY_ATTEMPTS ?= 2
MLB_REPLAY_RETRY_BACKOFF_MS ?= 350
MLB_PROP_COVERAGE_WINDOW_DAYS ?= 30
MLB_PROP_COVERAGE_WINDOW_MODE ?= days
MLB_PROP_COVERAGE_GAMES_BACK ?= 30
MLB_PROP_COVERAGE_REQUIRED ?=
MLB_PROP_COVERAGE_MIN_GRADED ?= 0
MLB_PROP_COVERAGE_GATE_METRIC ?= graded
MLB_PROP_COVERAGE_TRAINING_SOURCES ?= mlb_api
MLB_INCLUDE_COVERAGE ?= 0
MLB_CORE_PROP_TYPES ?= hits,total_bases,hits_runs_rbis,runs_rbis,rbis,runs_scored,strikeouts_batting,walks,singles,doubles,strikeouts_pitching,outs_recorded
MLB_PROD8_PROP_TYPES ?= hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks
MLB_UNDERSERVED_PROMOTED_PROP_TYPES ?= runs_scored,walks_allowed,rbis
MLB_UNDERSERVED_WATCHLIST_PROP_TYPES ?= outs_recorded,home_runs
# Keep prod12 default lane aligned with cron/runbook plus pitcher outs for upload coverage.
MLB_PROD12_PROP_TYPES ?= $(MLB_PROD8_PROP_TYPES),hits_runs_rbis,runs_scored,walks_allowed,rbis,outs_recorded
# Full daily prod12 eval is always all-12. Use MLB_PROD12_WATERLINE_PROP_TYPES for optional narrowed experiments.
MLB_PROD12_DAILY_PROP_TYPES ?= $(MLB_PROD12_PROP_TYPES)
MLB_PROD12_WATERLINE_PROP_TYPES ?= hits,total_bases,strikeouts_batting
MLB_PROD12_PIPELINE_PROP_TYPES ?= $(MLB_PROD12_PROP_TYPES)
MLB_DEGENERATE_PROP_TYPES ?= $(MLB_UNDERSERVED_WATCHLIST_PROP_TYPES)
MLB_UNDERSERVED_PROP_TYPES ?= $(MLB_UNDERSERVED_PROMOTED_PROP_TYPES),$(MLB_UNDERSERVED_WATCHLIST_PROP_TYPES)
MLB_UNDERSERVED_PROP_SOURCES ?= mlb_api
MLB_UNDERSERVED_SEASONS ?=
MLB_UNDERSERVED_SEASON_COUNT ?= 3
MLB_UNDERSERVED_BALANCE_FLOOR_PCT ?= 20
MLB_HIGH_VALUE_PROP_TYPES ?= $(MLB_UNDERSERVED_PROP_TYPES)
MLB_HIGH_VALUE_PROP_SOURCES ?= $(MLB_UNDERSERVED_PROP_SOURCES)
MLB_HIGH_VALUE_SEASONS ?= $(MLB_UNDERSERVED_SEASONS)
MLB_HIGH_VALUE_SEASON_COUNT ?= $(MLB_UNDERSERVED_SEASON_COUNT)
MLB_HIGH_VALUE_BALANCE_FLOOR_PCT ?= $(MLB_UNDERSERVED_BALANCE_FLOOR_PCT)
MLB_CORE_MIN_GRADED ?= 20
MLB_CORE_TRAINING_SOURCES ?= mlb_api
NHL_QUALITY_FROM_DATE ?=
NHL_QUALITY_TO_DATE ?=
NHL_QUALITY_MIN_TOTAL ?= 0
NHL_QUALITY_ACTIVE_MIN_TOTAL ?= 1
NHL_SOG_MODEL_FAMILY ?= denali_blend
NHL_SOG_MODEL_VERSION ?= phoenix_v2
NHL_SOG_LINES ?= 1.5,2.5,3.5
NHL_SOG_FROM_DATE ?=
NHL_SOG_TO_DATE ?=
NHL_SOG_LOOKBACK_DAYS ?= 120
NHL_SOG_RECENT_DAYS ?= 14
NHL_SOG_SEGMENT_MIN_N ?= 80
NHL_SOG_DECILE_MIN_N ?= 25
NHL_SOG_PLAYER_MIN_N ?= 4
NHL_SOG_PLAYER_TOP_N ?= 10
NHL_SOG_WORST_LIMIT ?= 8
NHL_SOG_OUTPUT ?=
NHL_SOG_CAL_MODEL_FAMILY ?= denali_blend
NHL_SOG_CAL_MODEL_VERSION ?= phoenix_v2
NHL_SOG_CAL_LINES ?= 1.5,2.5,3.5
NHL_SOG_CAL_FROM_DATE ?=
NHL_SOG_CAL_TO_DATE ?=
NHL_SOG_CAL_LOOKBACK_DAYS ?= 120
NHL_SOG_CAL_HOLDOUT_DAYS ?= 14
NHL_SOG_CAL_SEGMENT_MIN_ROWS ?= 120
NHL_SOG_CAL_BLEND_ALPHA ?= 0.65
NHL_SOG_CAL_DECAY_HALF_LIFE_DAYS ?= 21
NHL_SOG_CAL_OUTPUT ?=
NHL_SOG_MONITOR_HISTORY_INPUT ?= artifacts/nhl_sog_calibration_history.jsonl
NHL_SOG_MONITOR_HISTORY_LIMIT ?= 10
NHL_SOG_MONITOR_REQUIRED_LINES ?= 1.5,2.5,3.5
NHL_SOG_MONITOR_MAX_DELTA_BRIER ?= 0.0
NHL_SOG_MONITOR_MAX_DELTA_LOGLOSS ?= 0.0
NHL_SOG_MONITOR_HISTORY_CLEAN_BACKUP ?= 1
NHL_SOG_BASELINE_FROM_DATE ?=
NHL_SOG_BASELINE_TO_DATE ?=
NHL_SOG_BASELINE_OUTPUT ?= artifacts/season_baselines/nhl_sog_segmented_calibration_baseline.json
NHL_SOG_SEG_CAL_PRED_CSV ?= backend/nhl/data/processed/sog_predictions_wide_calibrated.csv
NHL_SOG_SEG_CAL_OUT_CSV ?=
NHL_SOG_SEG_CAL_ASOF_DATE ?=
NHL_SOG_SEG_CAL_STRICT ?= 0
MLB_STAT_FROM_DATE ?=
MLB_STAT_TO_DATE ?=
MLB_STAT_DAYS_AGO ?= 2
MLB_STAT_MAX_GAMES ?= 0
MLB_STAT_SKIP_EXISTING_DATES ?= 1
MLB_STAT_BATTER_SAMPLE_RATIO ?= 1.0
MLB_SEASON_REQUIRE_REGULAR ?= 0
MLB_PRESEASON_FROM_DATE ?=
MLB_PRESEASON_TO_DATE ?=
MLB_PRESEASON_INCLUDE_USER_ADDED ?= 0
MLB_PRESEASON_GAME_TYPES ?= S
OPS_HISTORY_INPUT ?= artifacts/ops_operator_history.jsonl
OPS_HISTORY_LIMIT ?= 10
SEASON_HISTORY_INPUT ?= artifacts/season_activation_history.jsonl
SEASON_HISTORY_LIMIT ?= 10

# Ensure repo-root package imports (e.g., `from backend.scripts import ...`) work
# in shells where the project is not installed as a package.
export PYTHONPATH := $(CURDIR):$(PYTHONPATH)
SEASON_HISTORY_MAX_AGE_HOURS ?= 0
SEASON_MAX_AGE_HOURS ?= 0
SEASON_CUTOVER_HISTORY_LIMIT ?= 10
MLB_PIPELINE_HISTORY_INPUT ?= artifacts/mlb_pipeline_history.jsonl
MLB_PIPELINE_HISTORY_LIMIT ?= 10
SEASON_CUTOVER_HISTORY_INPUT ?= artifacts/season_cutover_history.jsonl

.PHONY: mlb-hybrid-window-refresh
.PHONY: mlb-retrain-broad-reconcile
.PHONY: mlb-retrain-bol-market-only
.PHONY: mlb-prod12-model-bundle-publish
.PHONY: mlb-odds-backfill-history
.PHONY: mlb-db-env-check
.PHONY: mlb-red-mode-bucket-report mlb-red-mode-bucket-report-positive mlb-red-mode-fade-bucket-report mlb-red-mode-bucket-report-combined mlb-book-upload-side-matrix mlb-daily-bet-sheet mlb-rebuild-lane-test mlb-bvp-impact-preflight mlb-bvp-impact-report mlb-hits-environment-report mlb-daily-feature-lineage-health mlb-daily-health-gates mlb-refresh-daily-ops-brief-inputs mlb-daily-preflight mlb-daily-index mlb-daily-index-check mlb-daily-ops-brief mlb-cleanup-one-sided-price-rows mlb-project-invariants mlb-invariant-backlog

mlb-db-env-check:
	$(VENV_PY) backend/scripts/check_db_env.py --check-connection

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
	@echo "  make mlb-predictions-wide MLB_DATE=YYYY-MM-DD [writes per-slate odds snapshot json]"
	@echo "  make mlb-slate-archive MLB_DATE=YYYY-MM-DD [archive wide/slate/book/odds artifacts]"
	@echo "  make mlb-reconcile-rows MLB_RECONCILE_FROM_DATE=YYYY-MM-DD MLB_RECONCILE_TO_DATE=YYYY-MM-DD [row-level model+market(+outcome) csv]"
	@echo "  make mlb-cleanup-one-sided-price-rows [preview DB one-sided price rows; set MLB_ONE_SIDED_CLEANUP_APPLY=1 to delete]"
	@echo "  make mlb-all-available-report [resolved all-available summary + by-prop rates from reconcile rows]"
	@echo "  make mlb-graded-wagers-report MLB_GRADED_IN_CSV=tmp/graded/8rainstation_daily_YYYY-MM-DD_mlb_player_props.csv [placed graded-wager summary + by-prop]"
	@echo "  make mlb-graded-wagers-report-latest [auto-pick latest split MLB player-props grader csv from tmp/graded]"
	@echo "  make mlb-book-upload [full base upload CSV; policy filtering disabled by default]"
	@echo "  make mlb-book-upload-variants [build base + weighted upload CSVs and package both into dated mlb_uploads folder]"
	@echo "  make mlb-compare-upload-variants-postgame MLB_DATE=YYYY-MM-DD [postgame base-vs-weighted comparison report]"
	@echo "  make mlb-singles-shadow MLB_DATE=YYYY-MM-DD [build isolated singles-threshold shadow CSV and summary artifacts]"
	@echo "  make mlb-total-bases-shadow-candidate MLB_TOTAL_BASES_SHADOW_DATE=YYYY-MM-DD [shadow-score dedicated total_bases + rolling candidate]"
	@echo "  make mlb-total-bases-shadow-evaluation [read-only cumulative evaluation of existing total_bases shadow scores]"
	@echo "  make mlb-book-upload-policy MLB_POLICY_PLAN_CSV=<path> [optional policy-filtered upload rows]"
	@echo "  make mlb-book-upload-top-recommended [adaptive top-N from current book upload + recent post-grade tracker]"
	@echo "  make mlb-book-upload-side-matrix [one-command side-matrix upload; no EV/gap policy filters]"
	@echo "  make mlb-daily-bet-sheet [build tool-ready daily sheet from historical lane stats + side-matrix details]"
	@echo "  make mlb-rebuild-lane-test [walk-forward deterministic lane testing with strict robustness gates]"
	@echo "  make mlb-post-grade-step7 [split latest grader download + run full MLB post-grade tracking for that slate date]"
	@echo "  make mlb-post-grade-next-day [one-step post-grade bundle after next-day cron]"
	@echo "  make mlb-post-grade-report-and-track-latest [all-available + model-vs-fade + graded wagers merged into tracker/charts]"
	@echo "  make mlb-red-mode-bucket-report [cumulative model ROI by odds bucket (default 10-point layout)]"
	@echo "  make mlb-red-mode-bucket-report-positive [same as above, but output files keep only positive-ROI buckets]"
	@echo "  make mlb-red-mode-fade-bucket-report [cumulative fade ROI by odds bucket; prints positive buckets compact]"
	@echo "  make mlb-red-mode-bucket-report-combined [run model + fade bucket reports together]"
	@echo "  make mlb-odds-backfill-history [historical OddsAPI pull; defaults to season 2025 regular-season start]"
	@echo "  make frontend-route-smoke [verify critical nav/route surface in AppRouter]"
	@echo "  make workflow-inventory [report scheduled workflow files]"
	@echo "  make workflow-inventory-strict [fail if scheduled files differ from allowlist]"
	@echo "  make workflow-path-audit [report missing python refs in scheduled workflows]"
	@echo "  make workflow-path-audit-strict [fail on missing python refs in scheduled workflows]"
	@echo "  make docs-make-target-audit [fail if docs reference missing make targets]"
	@echo "  make phase-status [print current phase tracker snapshot]"
	@echo "  make phase-status-json [machine-readable phase tracker summary]"
	@echo "  make season-activation-status [phase 6 status + baseline/cutover/history presence]"
	@echo "  make season-activation-status-strict [non-zero exit until phase 6 readiness is complete; set SEASON_HISTORY_MAX_AGE_HOURS to enforce recency]"
	@echo "  make season-activation-log [append season activation snapshot to artifacts jsonl]"
	@echo "  make season-activation-last [show latest season activation history rows]"
	@echo "  make season-activation-report [single JSON report: phase + activation + baseline + history]"
	@echo "  make season-activation-report-strict [same report, but exits non-zero when not ready; honors SEASON_HISTORY_MAX_AGE_HOURS]"
	@echo "  make season-baseline-check [validate MLB/NHL baseline artifacts exist]"
	@echo "  make season-baseline-last [show latest MLB/NHL baseline artifact summary]"
	@echo "  make season-baseline-lock [capture+validate+log day-0 baseline artifacts]"
	@echo "  make season-cutover-cadence [show intended in-season cron cadence/commands]"
	@echo "  make season-cutover-log [append in-season cadence plan snapshot to history jsonl]"
	@echo "  make season-cutover-last [show recent cadence snapshots + regression hints]"
	@echo "  make season-cutover-ready [strict phase 6 readiness + governance gate]"
	@echo "  make season-activation-check [run kickoff + baseline lock + cadence plan]"
	@echo "  make ops-shortlist-check [high-signal ops bundle; optional NHL quality + post-deploy]"
	@echo "  make mlb-season-kickoff-check [opening-day readiness bundle; optional deployed check]"
	@echo "  make season-baseline-capture [write MLB/NHL day-0 quality JSON to artifacts]"
	@echo "  make mlb-season-baseline-capture [write MLB quality baseline JSON only]"
	@echo "  make mlb-prod8-baseline-capture [write production-8 quality and pipeline JSON artifacts]"
	@echo "  make nhl-workflow-compat-check [verify NHL workflow compatibility scripts]"
	@echo "  make cron-governance-check [inventory + path audit + NHL workflow compat]"
	@echo "  make cron-governance-snapshot [single combined JSON governance payload]"
	@echo "  make ops-operator-summary [compact daily ops summary; honors SEASON_HISTORY_MAX_AGE_HOURS]"
	@echo "  make ops-show-config [print effective ops history/pipeline/season inputs]"
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
	@echo "  make mlb-prediction-quality-core [core 12 quality summary over games window]"
	@echo "  make mlb-prediction-quality-prod8 [production-8 quality summary over games window]"
	@echo "  make mlb-prediction-quality-prod12 [production-12 quality summary over games window]"
	@echo "  make mlb-recompute-training-predictions [re-score model_training_props rows with current feature/prediction logic]"
	@echo "  make mlb-corrected-props-recompute [safe model-based recompute for corrected combo/runs props + quality snapshot]"
	@echo "  make mlb-hybrid-window-refresh [hybrid per-prop retrain windows + gated recompute + quality/candidate snapshots]"
	@echo "  make mlb-retrain-broad-reconcile [broad prop retrain from reconcile rows + gated recompute + quality/candidate snapshots]"
	@echo "  make mlb-model-artifact-validate [validate MLB model artifacts are loadable, fitted, and schema-compatible]"
	@echo "  make mlb-model-artifact-validate-prod12 [same validation scoped to prod12 props]"
	@echo "  make mlb-pre-cron-check [report-only Prod12 pre-cron GO/NO-GO check]"
	@echo "  make mlb-model-snapshot [snapshot latest model dir to archive + manifest]"
	@echo "  make mlb-model-publish [promote archived snapshot to active latest]"
	@echo "  make mlb-model-prune [prune old archive snapshots; dry-run by default]"
	@echo "  make mlb-model-rollback [rollback latest to a prior snapshot id]"
	@echo "  make mlb-prod12-model-bundle-publish [upload prod12 bundle to versioned key + stable latest.tgz alias]"
	@echo "  make mlb-feature-health [feature-source mix + fallback/default rates by prop lane]"
	@echo "  make mlb-feature-health-prod12 [same feature-health report scoped to production-12 lanes]"
	@echo "  make mlb-pfp-overlap-audit [report missing prop_features_precomputed overlap for selected props/window]"
	@echo "  make mlb-pfp-overlap-backfill [upsert missing prop_features_precomputed rows from reconciled model_training_props]"
	@echo "  make mlb-bvp-pvb-refresh [refresh daily BvP/PvB feature payloads into prop_features_precomputed]"
	@echo "  make mlb-bvp-impact-preflight [estimate BvP impact runtime cost before running the heavy report]"
	@echo "  make mlb-bvp-impact-report [compare slate probabilities with BvP on vs off; writes latest+history impact artifacts]"
	@echo "  make mlb-balance-guard [single-prop one-sided drift guard (default runs_scored)]"
	@echo "  make mlb-prediction-quality-user-added [user_added-only quality summary json]"
	@echo "  make mlb-prediction-quality-segmented [preseason vs regular-season date-window quality report]"
	@echo "  make mlb-degenerate-lane-report [balance-vs-accuracy diagnostics for degenerate lanes]"
	@echo "  make mlb-underserved-historical-report [3-season historical diagnostics for underserved lanes]"
	@echo "  make mlb-retrain-prereq-check [freshness+coverage+grading+baseline checklist json]"
	@echo "  make mlb-candidate-eval [candidate-vs-baseline quality comparison + promotion recommendation]"
	@echo "  make mlb-candidate-eval-prod12 [candidate eval scoped to production-12 props]"
	@echo "  make mlb-retrain-bol-market-only [brand-new market-native retrain: BetOnline two-sided reconcile rows only]"
	@echo "  make mlb-prod12-status [single JSON status for latest prod12 daily + weekly lanes]"
	@echo "  make mlb-prod12-status-strict [non-zero when prod12 daily/weekly status failed or stale]"
	@echo "  make mlb-prod12-health-report [pass-rate trend report for recent prod12 daily+weekly runs]"
	@echo "  make mlb-prod12-incident [compact incident summary with next actions]"
	@echo "  make mlb-prod12-incident-strict [same incident summary, exits non-zero on fail]"
	@echo "  make mlb-prod12-ops-check [strict status + health report; prints incident summary on failure]"
	@echo "  make mlb-prod12-ops-log [append prod12 operator snapshot to history jsonl]"
	@echo "  make mlb-prod12-ops-last [show recent prod12 operator snapshot history]"
	@echo "  make mlb-prod12-track-daily [run production-12 FULL check+log pair (always all 12 props)]"
	@echo "  make mlb-prod12-track-daily-waterline [optional narrowed experiment run; does NOT replace full daily]"
	@echo "  make mlb-prod12-daily-gate [run daily prod12 track, then enforce strict status freshness]"
	@echo "  make mlb-prod12-daily-gate-incident [daily strict gate; prints prod12 incident summary on failure]"
	@echo "  make mlb-prod12-daily-cycle [daily gate with incident-on-fail; always appends ops snapshot history]"
	@echo "  make mlb-prod12-track-weekly [run production-12 candidate eval with tracking tolerance]"
	@echo "  make mlb-prod12-cron-preview [print copy/paste-ready daily+weekly prod12 scheduler commands]"
	@echo "  make mlb-prod12-script-preview [print scheduler-ready wrapper script commands]"
	@echo "  make mlb-prod12-bootstrap-preview [print one copy/paste-ready post-redeploy bootstrap command]"
	@echo "  make mlb-prod12-scheduler-smoke [run daily+weekly wrapper scripts, then print status/history summaries]"
	@echo "  make mlb-prod12-bootstrap [one-time post-redeploy bootstrap: weekly cycle, daily cycle, strict status + history]"
	@echo "  make mlb-prod12-bootstrap-strict [bootstrap plus tight-freshness validation for daily+weekly histories]"
	@echo "  make mlb-prod12-release-manifest [write prod12 release manifest with artifact checksums]"
	@echo "  make mlb-prod12-replay-latency [historical replay latency report for prod12 props; allow sparse by default]"
	@echo "  make mlb-prod12-phase2-log [append prod12 phase-2 weekly summary to history jsonl]"
	@echo "  make mlb-prod12-phase2-last [show recent prod12 phase-2 history snapshots]"
	@echo "  make mlb-prod12-phase2-last-strict [non-zero exit when latest prod12 phase-2 snapshot failed]"
	@echo "  make mlb-prod12-phase2-weekly-gate [run phase-2 readiness, then enforce latest strict status]"
	@echo "  make mlb-prod12-phase2-weekly-gate-incident [weekly strict gate; prints prod12 incident summary on failure]"
	@echo "  make mlb-prod12-phase2-weekly-cycle [weekly gate with incident-on-fail; always appends ops snapshot history]"
	@echo "  make mlb-prod12-phase2-readiness [manifest + replay latency + weekly candidate eval]"
	@echo "  make mlb-prediction-gate [combined operability + quality pass/fail]"
	@echo "  make mlb-pipeline-check [prediction gate + flow audit + expectation-source guard]"
	@echo "  make mlb-pipeline-check-json [single JSON payload for gate + flow (+ optional coverage)]"
	@echo "  make mlb-pipeline-check-ops [same as pipeline-check-json, with coverage enabled]"
	@echo "  make mlb-pipeline-check-core [JSON pipeline bundle with core-12 coverage thresholds]"
	@echo "  make mlb-pipeline-check-prod8 [JSON pipeline bundle scoped to production-8 prop lanes]"
	@echo "  make mlb-pipeline-check-prod12 [JSON pipeline bundle scoped to production-12 prop lanes]"
	@echo "  make mlb-hits-expectation-sources [guard forbidden expectation source + report source mix]"
	@echo "  make mlb-pipeline-log [append pipeline check JSON snapshot to history]"
	@echo "  make mlb-pipeline-log-prod12 [append production-12 pipeline snapshot to history]"
	@echo "  make mlb-pipeline-log-ops [append pipeline snapshot with coverage diagnostics]"
	@echo "  make mlb-pipeline-last [show recent pipeline history snapshots]"
	@echo "  make mlb-pipeline-daily-check [append latest pipeline snapshot, then show history tail]"
	@echo "  make mlb-prediction-flow-audit [date/game binding + duplicate/idempotency checks]"
	@echo "  make mlb-prop-coverage [recent prop-type coverage and graded volume]"
	@echo "  make mlb-prop-coverage-core [core 12 prop coverage guard]"
	@echo "  make mlb-player-surface-checks [focused player lookup/search/profile regression suite]"
	@echo "  make mlb-hits-environment-report [league hits/game regime + hits_allowed opponent-form monitor]"
	@echo "  make mlb-hits-o15-simple-filter DATE=YYYY-MM-DD [review aid: hits over 1.5 hot-hitter + starter hits-allowed filter]"
	@echo "  make mlb-hits-o15-watch-candidates DATE=YYYY-MM-DD [review aid: outcome-backed QC + hot hitter + starter hits o1.5 subset]"
	@echo "  make mlb-hits-o15-layered-candidates DATE=YYYY-MM-DD [review aid: layered hits over 1.5 candidate board]"
	@echo "  make mlb-oddsapi-batter-hits-alternate-live-discovery DATE=YYYY-MM-DD [review aid: pull live OddsAPI batter_hits_alternate discovery source]"
	@echo "  make mlb-hits-o15-alternate-discovery DATE=YYYY-MM-DD [review aid: build OddsAPI batter_hits_alternate board from existing source CSV]"
	@echo "  make mlb-hits-o15-alternate-discovery-full DATE=YYYY-MM-DD [review aid: pull live alternate source, then build board]"
	@echo "  make mlb-oddsapi-alternate-history-cost-estimate DATE_FROM=YYYY-MM-DD DATE_TO=YYYY-MM-DD [review aid: estimate historical batter_hits_alternate cost; dry-run default]"
	@echo "  make mlb-oddsapi-alternate-history-backfill DATE_FROM=YYYY-MM-DD DATE_TO=YYYY-MM-DD [review aid: historical batter_hits_alternate source pull; dry-run default]"
	@echo "  make mlb-hits-o15-alternate-discovery-from-history DATE=YYYY-MM-DD [review aid: build alternate board from historical backfill source]"
	@echo "  make mlb-hits-o15-alternate-discovery-from-history-range DATE_FROM=YYYY-MM-DD DATE_TO=YYYY-MM-DD [review aid: build alternate boards from historical sources]"
	@echo "  make mlb-o15-alternate-history-backfill-and-build DATE_FROM=YYYY-MM-DD DATE_TO=YYYY-MM-DD [review aid: capped historical source pull + board build]"
	@echo "  make mlb-expanded-o15-universe [DATE=YYYY-MM-DD or DATE_FROM=YYYY-MM-DD DATE_TO=YYYY-MM-DD] [research: canonical expanded o1.5 universe]"
	@echo "  make mlb-expanded-o15-context-health DATE=YYYY-MM-DD [research: health gate for expanded o1.5 context hydration]"
	@echo "  make mlb-expanded-o15-universe-slice-analysis [research: explain which expanded o1.5 slices carry ROI]"
	@echo "  make mlb-expanded-o15-universe-betonline-audit [research: audit expanded o1.5 under BetOnline pricing]"
	@echo "  make mlb-expanded-o15-hidden-matchup-support-audit [research: audit hidden pitcher/team support inside expanded o1.5]"
	@echo "  make mlb-expanded-o15-agreement-score-audit [research: audit multi-signal agreement inside expanded o1.5]"
	@echo "  make mlb-expanded-o15-variable-importance-survey [research: broad variable/pairwise survey over expanded o1.5]"
	@echo "  make mlb-expanded-o15-feature-centrality-audit [research: rank recurring feature building blocks across expanded o1.5 pairwise interactions]"
	@echo "  make mlb-time-of-day-bucket-audit [research: audit canonical MLB time-of-day bucket definitions and artifact consistency]"
	@echo "  make mlb-expanded-o15-late-game-proxy-audit [research: decompose late-game signal into region/team/market composition]"
	@echo "  make mlb-expanded-o15-low-attention-signpost-audit [research: audit low-attention +200s O1.5 signpost profile]"
	@echo "  make mlb-research-snapshot DATE=YYYY-MM-DD [weekly immutable MLB research checkpoint]"
	@echo "  make mlb-identity-health [canonical identity coverage/fallback/ambiguity diagnostics]"
	@echo "  make mlb-o15-ontology-health DATE=YYYY-MM-DD [O1.5 ontology metadata coverage diagnostics]"
	@echo "  make mlb-project-invariants DATE=YYYY-MM-DD [project doctrine invariant audit]"
	@echo "  make mlb-invariant-backlog DATE=YYYY-MM-DD [invariant intake backlog summary]"
	@echo "  make mlb-hits-u15-favorite-audit DATE=YYYY-MM-DD [review aid: hits under 1.5 favorite audit board]"
	@echo "  make mlb-daily-review-boards DATE=YYYY-MM-DD [routine current-slate review board bundle]"
	@echo "  make mlb-review-aid-performance [review aid: outcome-backed board/layer/tier performance tracker]"
	@echo "  make mlb-daily-index DATE=YYYY-MM-DD [daily MLB artifact index + cleanup manifest]"
	@echo "  make mlb-daily-index-check DATE=YYYY-MM-DD [validate daily MLB artifact index links]"
	@echo "  make mlb-db-env-check [safe DB env/DNS/connection diagnostics]"
	@echo "  make mlb-daily-ops-brief [daily human-readable MLB ops brief from key artifacts]"
	@echo "  make mlb-daily-refresh [daily baseline; cache+roster+bvp/pvb+stat-derived+capture+bvp-impact+hits-env monitor]"
	@echo "  make mlb-daily-refresh-strict [daily baseline + require stat-derived min=1]"
	@echo "  make mlb-daily-refresh-smoke [daily baseline smoke; forces MLB_STAT_MAX_GAMES=1]"
	@echo "  make mlb-ops-check BASE_URL=<url> [ops confidence loop: config+daily-smoke+post-deploy]"
	@echo "  make mlb-stat-derived-refresh [insert+check; supports MLB_STAT_DAYS_AGO/MLB_STAT_SKIP_EXISTING_DATES/MLB_STAT_BATTER_SAMPLE_RATIO]"
	@echo "  make mlb-stat-derived-backfill MLB_STAT_FROM_DATE=YYYY-MM-DD MLB_STAT_TO_DATE=YYYY-MM-DD [MLB_STAT_DERIVED_DAYS=400 MLB_STAT_DERIVED_MIN=1]"
	@echo "  make mlb-cleanup-one-sided-price-rows [preview one-sided rows in mlb.* price tables; set MLB_ONE_SIDED_CLEANUP_APPLY=1 to delete]"
	@echo "  make mlb-preseason-cleanup MLB_PRESEASON_FROM_DATE=YYYY-MM-DD MLB_PRESEASON_TO_DATE=YYYY-MM-DD [MLB_PRESEASON_INCLUDE_USER_ADDED=0] [MLB_PRESEASON_GAME_TYPES=S]"
	@echo "  make mlb-season-mode-lock [smoke stat-derived with MLB_SEASON_REQUIRE_REGULAR=1]"
	@echo "  make mlb-stat-derived-smoke [quick wiring check; forces MLB_STAT_MAX_GAMES=1]"
	@echo "  make tmp-audit [show tmp footprint and largest files]"
	@echo "  make mlb-tmp-focus [copy key upload CSVs into backend/mlb/data/processed/upload_hub with stable names]"
	@echo "  make tmp-prune-bulky [remove bulky generated reconcile CSVs in tmp]"
	@echo "  make tmp-prune-age [remove tmp files older than TMP_RETENTION_DAYS]"
	@echo "  make tmp-prune-fat-csv [remove tmp CSV files >= TMP_FAT_CSV_MIN_MB older than TMP_FAT_CSV_MIN_AGE_DAYS]"
	@echo "  make tmp-prune [run bulky prune then age-based prune]"
	@echo "  make mlb-odds-history-audit [show MLB odds_history footprint and largest files]"
	@echo "  make mlb-odds-history-prune-intermediate [remove raw intermediate files when odds_latest_compatible exists]"
	@echo "  make mlb-odds-history-prune-old-dates [remove odds_history date dirs older than MLB_ODDS_HISTORY_RETENTION_DAYS]"
	@echo "  make mlb-odds-history-offload-status MLB_ODDS_HISTORY_ARCHIVE_ROOT=/Volumes/<Drive>/... [show local vs external odds_history footprint]"
	@echo "  make mlb-odds-history-offload-sync MLB_ODDS_HISTORY_ARCHIVE_ROOT=/Volumes/<Drive>/... [rsync local odds_history to external archive]"
	@echo "  make mlb-odds-history-offload-prune-local MLB_ODDS_HISTORY_ARCHIVE_ROOT=/Volumes/<Drive>/... [prune local old dates only when archived copy exists]"
	@echo "  make mlb-odds-history-offload-cycle MLB_ODDS_HISTORY_ARCHIVE_ROOT=/Volumes/<Drive>/... [status -> sync -> prune-local -> status]"
	@echo "  make artifacts-audit [show artifacts footprint and largest files]"
	@echo "  make artifacts-prune-safe [remove stale experiments/log/cache files older than ARTIFACTS_RETENTION_DAYS]"
	@echo "  make artifacts-prune-experiments [remove all files under artifacts/experiments]"
	@echo "  make artifacts-prune [run safe prune using ARTIFACTS_RETENTION_DAYS]"
	@echo "  make mlb-insert-stat-derived [advanced: direct insert flags]"
	@echo "  make mlb-check-stat-derived [advanced: direct volume guard flags]"
	@echo "  make mlb-check-stat-derived-json [advanced: direct volume guard json]"
	@echo "  make mlb-check-rolling-integrity [rolling d7/d15/d30 coverage + movement PASS/FAIL]"
	@echo "  make roster-refresh-all [MLB_ROSTER_DATE=YYYY-MM-DD] [NHL_ROSTER_DATE=YYYY-MM-DD]"
	@echo "  make mlb-post-deploy BASE_URL=<url>"
	@echo "  make nhl-post-deploy BASE_URL=<url>"
	@echo "  make nhl-prediction-quality NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD [NHL_QUALITY_MIN_TOTAL=0]"
	@echo "  make nhl-prediction-quality-auto NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD [NHL_QUALITY_ACTIVE_MIN_TOTAL=1]"
	@echo "  make nhl-sog-quality-layers [NHL_SOG_FROM_DATE=YYYY-MM-DD NHL_SOG_TO_DATE=YYYY-MM-DD NHL_SOG_OUTPUT=artifacts/nhl_sog_layers.json]"
	@echo "  make nhl-sog-segmented-calibration-experiment [NHL_SOG_CAL_OUTPUT=artifacts/nhl_sog_cal_experiment.json]"
	@echo "  make nhl-sog-calibration-baseline NHL_SOG_BASELINE_FROM_DATE=YYYY-MM-DD NHL_SOG_BASELINE_TO_DATE=YYYY-MM-DD"
	@echo "  make nhl-sog-calibration-log [NHL_SOG_MONITOR_HISTORY_INPUT=artifacts/nhl_sog_calibration_history.jsonl]"
	@echo "  make nhl-sog-calibration-last [NHL_SOG_MONITOR_HISTORY_LIMIT=10]"
	@echo "  make nhl-sog-calibration-history-clean [NHL_SOG_MONITOR_HISTORY_CLEAN_BACKUP=1]"
	@echo "  make nhl-sog-segmented-calibrate-file [NHL_SOG_SEG_CAL_PRED_CSV=backend/nhl/data/processed/sog_predictions_wide_calibrated.csv]"
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

mlb-prod12-cron-preview:
	@echo "Recommended prod12 automation commands:"
	@echo "1) One-time bootstrap after redeploy: make mlb-prod12-bootstrap MLB_BASE_URL=$(if $(MLB_BASE_URL),$(MLB_BASE_URL),https://baseball-streaks-sq44.onrender.com) MLB_DATE=$(MLB_DATE) MLB_PREDICT_SAMPLE=$(MLB_PREDICT_SAMPLE) MLB_PREDICT_MIN_SUCCESS=$(MLB_PREDICT_MIN_SUCCESS) MLB_REPLAY_SAMPLE=$(MLB_REPLAY_SAMPLE) MLB_REPLAY_MIN_SUCCESS=$(MLB_REPLAY_MIN_SUCCESS) MLB_REPLAY_MAX_PREDICT_P95_MS=$(MLB_REPLAY_MAX_PREDICT_P95_MS) MLB_REPLAY_RETRY_ATTEMPTS=$(MLB_REPLAY_RETRY_ATTEMPTS) MLB_REPLAY_RETRY_BACKOFF_MS=$(MLB_REPLAY_RETRY_BACKOFF_MS)"
	@echo "2) Daily: make mlb-prod12-daily-cycle MLB_BASE_URL=$(if $(MLB_BASE_URL),$(MLB_BASE_URL),https://baseball-streaks-sq44.onrender.com) MLB_DATE=\$$(date -u +%F) MLB_PREDICT_SAMPLE=$(MLB_PREDICT_SAMPLE) MLB_PREDICT_MIN_SUCCESS=$(MLB_PREDICT_MIN_SUCCESS)"
	@echo "3) Weekly: make mlb-prod12-phase2-weekly-cycle MLB_BASE_URL=$(if $(MLB_BASE_URL),$(MLB_BASE_URL),https://baseball-streaks-sq44.onrender.com) MLB_DATE=$(MLB_DATE) MLB_REPLAY_SAMPLE=$(MLB_REPLAY_SAMPLE) MLB_REPLAY_MIN_SUCCESS=$(MLB_REPLAY_MIN_SUCCESS) MLB_REPLAY_MAX_PREDICT_P95_MS=$(MLB_REPLAY_MAX_PREDICT_P95_MS) MLB_REPLAY_RETRY_ATTEMPTS=$(MLB_REPLAY_RETRY_ATTEMPTS) MLB_REPLAY_RETRY_BACKOFF_MS=$(MLB_REPLAY_RETRY_BACKOFF_MS)"

mlb-prod12-script-preview:
	@echo "Recommended prod12 scheduler script commands:"
	@echo "1) One-time bootstrap after redeploy: make mlb-prod12-bootstrap MLB_BASE_URL=$(if $(MLB_BASE_URL),$(MLB_BASE_URL),https://baseball-streaks-sq44.onrender.com) MLB_DATE=$(MLB_DATE)"
	@echo "2) Daily trigger cron command: bin/mlb_prod12_remote_trigger_daily.sh"
	@echo "3) Weekly trigger cron command (one-prop sequence): bin/mlb_prod12_remote_trigger_weekly.sh"

mlb-prod12-bootstrap-preview:
	@echo "Run after each deploy:"
	@echo "make mlb-prod12-bootstrap-strict MLB_BASE_URL=$(if $(MLB_BASE_URL),$(MLB_BASE_URL),https://baseball-streaks-sq44.onrender.com) MLB_DATE=$(MLB_DATE) MLB_PREDICT_SAMPLE=$(MLB_PREDICT_SAMPLE) MLB_PREDICT_MIN_SUCCESS=$(MLB_PREDICT_MIN_SUCCESS) MLB_REPLAY_SAMPLE=$(MLB_REPLAY_SAMPLE) MLB_REPLAY_MIN_SUCCESS=$(MLB_REPLAY_MIN_SUCCESS) MLB_REPLAY_MAX_PREDICT_P95_MS=$(MLB_REPLAY_MAX_PREDICT_P95_MS) MLB_REPLAY_RETRY_ATTEMPTS=$(MLB_REPLAY_RETRY_ATTEMPTS) MLB_REPLAY_RETRY_BACKOFF_MS=$(MLB_REPLAY_RETRY_BACKOFF_MS)"

mlb-prod12-scheduler-smoke:
	@set -e; \
	MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" bin/mlb_prod12_daily_cycle.sh; \
	MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_REPLAY_SAMPLE="$(MLB_REPLAY_SAMPLE)" MLB_REPLAY_MIN_SUCCESS="$(MLB_REPLAY_MIN_SUCCESS)" MLB_REPLAY_MAX_PREDICT_P95_MS="$(MLB_REPLAY_MAX_PREDICT_P95_MS)" MLB_REPLAY_RETRY_ATTEMPTS="$(MLB_REPLAY_RETRY_ATTEMPTS)" MLB_REPLAY_RETRY_BACKOFF_MS="$(MLB_REPLAY_RETRY_BACKOFF_MS)" bin/mlb_prod12_weekly_cycle.sh; \
	$(MAKE) mlb-prod12-status; \
	$(MAKE) mlb-prod12-ops-last; \
	$(MAKE) mlb-prod12-phase2-last

mlb-prod12-bootstrap:
	@set -e; \
	MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_REPLAY_SAMPLE="$(MLB_REPLAY_SAMPLE)" MLB_REPLAY_MIN_SUCCESS="$(MLB_REPLAY_MIN_SUCCESS)" MLB_REPLAY_MAX_PREDICT_P95_MS="$(MLB_REPLAY_MAX_PREDICT_P95_MS)" MLB_REPLAY_RETRY_ATTEMPTS="$(MLB_REPLAY_RETRY_ATTEMPTS)" MLB_REPLAY_RETRY_BACKOFF_MS="$(MLB_REPLAY_RETRY_BACKOFF_MS)" bin/mlb_prod12_weekly_cycle.sh; \
	MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" bin/mlb_prod12_daily_cycle.sh; \
	$(MAKE) mlb-prod12-status-strict; \
	$(MAKE) mlb-prod12-ops-last; \
	$(MAKE) mlb-prod12-phase2-last

mlb-prod12-bootstrap-strict:
	@set -e; \
	$(MAKE) mlb-prod12-bootstrap MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_REPLAY_SAMPLE="$(MLB_REPLAY_SAMPLE)" MLB_REPLAY_MIN_SUCCESS="$(MLB_REPLAY_MIN_SUCCESS)" MLB_REPLAY_MAX_PREDICT_P95_MS="$(MLB_REPLAY_MAX_PREDICT_P95_MS)" MLB_REPLAY_RETRY_ATTEMPTS="$(MLB_REPLAY_RETRY_ATTEMPTS)" MLB_REPLAY_RETRY_BACKOFF_MS="$(MLB_REPLAY_RETRY_BACKOFF_MS)"; \
	$(MAKE) mlb-prod12-phase2-last-strict; \
	$(MAKE) mlb-prod12-status-strict MLB_PROD12_DAILY_MAX_AGE_HOURS="$(MLB_PROD12_BOOTSTRAP_MAX_AGE_HOURS)" MLB_PROD12_WEEKLY_MAX_AGE_HOURS="$(MLB_PROD12_BOOTSTRAP_MAX_AGE_HOURS)"

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
	@echo "OPS_HISTORY_INPUT=$(OPS_HISTORY_INPUT)"
	@echo "OPS_HISTORY_LIMIT=$(OPS_HISTORY_LIMIT)"
	@echo "SEASON_HISTORY_INPUT=$(SEASON_HISTORY_INPUT)"
	@echo "SEASON_HISTORY_LIMIT=$(SEASON_HISTORY_LIMIT)"
	@echo "SEASON_HISTORY_MAX_AGE_HOURS=$(SEASON_HISTORY_MAX_AGE_HOURS)"
	@echo "SEASON_MAX_AGE_HOURS=$(SEASON_MAX_AGE_HOURS)"
	@echo "SEASON_CUTOVER_HISTORY_INPUT=$(SEASON_CUTOVER_HISTORY_INPUT)"
	@echo "SEASON_CUTOVER_HISTORY_LIMIT=$(SEASON_CUTOVER_HISTORY_LIMIT)"
	@echo "MLB_PIPELINE_HISTORY_INPUT=$(MLB_PIPELINE_HISTORY_INPUT)"
	@echo "MLB_PIPELINE_HISTORY_LIMIT=$(MLB_PIPELINE_HISTORY_LIMIT)"
	@echo ""
	@$(MAKE) ops-operator-summary

ops-show-config:
	@echo "OPS_HISTORY_INPUT=$(OPS_HISTORY_INPUT)"
	@echo "OPS_HISTORY_LIMIT=$(OPS_HISTORY_LIMIT)"
	@echo "SEASON_HISTORY_INPUT=$(SEASON_HISTORY_INPUT)"
	@echo "SEASON_HISTORY_LIMIT=$(SEASON_HISTORY_LIMIT)"
	@echo "SEASON_HISTORY_MAX_AGE_HOURS=$(SEASON_HISTORY_MAX_AGE_HOURS)"
	@echo "SEASON_MAX_AGE_HOURS=$(SEASON_MAX_AGE_HOURS)"
	@echo "SEASON_CUTOVER_HISTORY_INPUT=$(SEASON_CUTOVER_HISTORY_INPUT)"
	@echo "SEASON_CUTOVER_HISTORY_LIMIT=$(SEASON_CUTOVER_HISTORY_LIMIT)"
	@echo "MLB_PIPELINE_HISTORY_INPUT=$(MLB_PIPELINE_HISTORY_INPUT)"
	@echo "MLB_PIPELINE_HISTORY_LIMIT=$(MLB_PIPELINE_HISTORY_LIMIT)"

ops-operator-summary:
	$(VENV_PY) backend/scripts/ops_operator_summary.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN) --season-history-input $(SEASON_HISTORY_INPUT) --season-history-limit $(SEASON_HISTORY_LIMIT) --season-history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS) --season-max-age-hours $(SEASON_MAX_AGE_HOURS) --season-cutover-history-input $(SEASON_CUTOVER_HISTORY_INPUT) --season-cutover-history-limit $(SEASON_CUTOVER_HISTORY_LIMIT) --pipeline-history-input $(MLB_PIPELINE_HISTORY_INPUT) --pipeline-history-limit $(MLB_PIPELINE_HISTORY_LIMIT)

ops-operator-summary-json:
	$(VENV_PY) backend/scripts/ops_operator_summary.py --json --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN) --season-history-input $(SEASON_HISTORY_INPUT) --season-history-limit $(SEASON_HISTORY_LIMIT) --season-history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS) --season-max-age-hours $(SEASON_MAX_AGE_HOURS) --season-cutover-history-input $(SEASON_CUTOVER_HISTORY_INPUT) --season-cutover-history-limit $(SEASON_CUTOVER_HISTORY_LIMIT) --pipeline-history-input $(MLB_PIPELINE_HISTORY_INPUT) --pipeline-history-limit $(MLB_PIPELINE_HISTORY_LIMIT)

ops-operator-summary-json-compact:
	$(VENV_PY) backend/scripts/ops_operator_summary.py --compact --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN) --season-history-input $(SEASON_HISTORY_INPUT) --season-history-limit $(SEASON_HISTORY_LIMIT) --season-history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS) --season-max-age-hours $(SEASON_MAX_AGE_HOURS) --season-cutover-history-input $(SEASON_CUTOVER_HISTORY_INPUT) --season-cutover-history-limit $(SEASON_CUTOVER_HISTORY_LIMIT) --pipeline-history-input $(MLB_PIPELINE_HISTORY_INPUT) --pipeline-history-limit $(MLB_PIPELINE_HISTORY_LIMIT)

ops-operator-log:
	$(VENV_PY) backend/scripts/ops_operator_log.py --output $(OPS_HISTORY_INPUT) --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN) --season-history-input $(SEASON_HISTORY_INPUT) --season-history-limit $(SEASON_HISTORY_LIMIT) --season-history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS) --season-max-age-hours $(SEASON_MAX_AGE_HOURS) --season-cutover-history-input $(SEASON_CUTOVER_HISTORY_INPUT) --season-cutover-history-limit $(SEASON_CUTOVER_HISTORY_LIMIT) --pipeline-history-input $(MLB_PIPELINE_HISTORY_INPUT) --pipeline-history-limit $(MLB_PIPELINE_HISTORY_LIMIT)

ops-operator-last:
	$(VENV_PY) backend/scripts/ops_operator_last.py --json --input $(OPS_HISTORY_INPUT) --limit $(OPS_HISTORY_LIMIT)

ops-operator-incident:
	$(VENV_PY) backend/scripts/ops_operator_incident.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN) --season-history-input $(SEASON_HISTORY_INPUT) --season-history-limit $(SEASON_HISTORY_LIMIT) --season-history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS) --season-max-age-hours $(SEASON_MAX_AGE_HOURS) --season-cutover-history-input $(SEASON_CUTOVER_HISTORY_INPUT) --season-cutover-history-limit $(SEASON_CUTOVER_HISTORY_LIMIT) --ops-history-input $(OPS_HISTORY_INPUT) --ops-history-limit $(OPS_HISTORY_LIMIT) --pipeline-history-input $(MLB_PIPELINE_HISTORY_INPUT) --pipeline-history-limit $(MLB_PIPELINE_HISTORY_LIMIT)

ops-operator-incident-strict:
	$(VENV_PY) backend/scripts/ops_operator_incident.py --strict --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN) --season-history-input $(SEASON_HISTORY_INPUT) --season-history-limit $(SEASON_HISTORY_LIMIT) --season-history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS) --season-max-age-hours $(SEASON_MAX_AGE_HOURS) --season-cutover-history-input $(SEASON_CUTOVER_HISTORY_INPUT) --season-cutover-history-limit $(SEASON_CUTOVER_HISTORY_LIMIT) --ops-history-input $(OPS_HISTORY_INPUT) --ops-history-limit $(OPS_HISTORY_LIMIT) --pipeline-history-input $(MLB_PIPELINE_HISTORY_INPUT) --pipeline-history-limit $(MLB_PIPELINE_HISTORY_LIMIT)

ops-daily-check:
	$(MAKE) ops-operator-log
	$(MAKE) ops-operator-incident-strict

phase-status:
	@awk '/^## Phase Status Tracker/{flag=1; print; next} /^## / && flag{exit} flag{print}' docs/Execution\ Plan.md

phase-status-json:
	$(VENV_PY) backend/scripts/phase_status_snapshot.py

season-activation-status:
	$(VENV_PY) backend/scripts/season_activation_status.py --history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS)

season-activation-status-strict:
	$(VENV_PY) backend/scripts/season_activation_status.py --strict --history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS)

season-activation-log:
	$(VENV_PY) backend/scripts/season_activation_log.py

season-activation-last:
	$(VENV_PY) backend/scripts/season_activation_last.py --json --limit 10

season-activation-report:
	$(VENV_PY) backend/scripts/season_activation_report.py --history-input $(SEASON_HISTORY_INPUT) --history-limit $(SEASON_HISTORY_LIMIT) --history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS) --max-age-hours $(SEASON_MAX_AGE_HOURS) --cutover-history-input $(SEASON_CUTOVER_HISTORY_INPUT) --cutover-history-limit $(SEASON_CUTOVER_HISTORY_LIMIT)

season-activation-report-strict:
	$(VENV_PY) backend/scripts/season_activation_report.py --strict --history-input $(SEASON_HISTORY_INPUT) --history-limit $(SEASON_HISTORY_LIMIT) --history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS) --max-age-hours $(SEASON_MAX_AGE_HOURS) --cutover-history-input $(SEASON_CUTOVER_HISTORY_INPUT) --cutover-history-limit $(SEASON_CUTOVER_HISTORY_LIMIT)

season-baseline-check:
	$(VENV_PY) backend/scripts/check_season_baseline_artifacts.py

season-baseline-last:
	$(VENV_PY) backend/scripts/season_baseline_last.py

season-baseline-lock:
	$(MAKE) season-baseline-capture MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" NHL_QUALITY_FROM_DATE="$(NHL_QUALITY_FROM_DATE)" NHL_QUALITY_TO_DATE="$(NHL_QUALITY_TO_DATE)" NHL_QUALITY_MIN_TOTAL="$(NHL_QUALITY_MIN_TOTAL)"
	$(MAKE) season-baseline-check
	$(MAKE) season-baseline-last
	$(MAKE) season-activation-log
	$(MAKE) season-activation-status

season-cutover-cadence:
	$(VENV_PY) backend/scripts/season_cutover_cadence.py

season-cutover-log:
	$(VENV_PY) backend/scripts/season_cutover_log.py --output $(SEASON_CUTOVER_HISTORY_INPUT)

season-cutover-last:
	$(VENV_PY) backend/scripts/season_cutover_last.py --input $(SEASON_CUTOVER_HISTORY_INPUT) --json --limit 10

season-cutover-ready:
	@set -e; \
	if ! $(MAKE) season-activation-report-strict; then \
		echo "season-cutover-ready: season activation strict check failed; latest snapshots:"; \
		$(MAKE) season-activation-log || true; \
		$(MAKE) season-activation-last || true; \
		$(MAKE) season-cutover-last || true; \
		exit 2; \
	fi; \
	if ! $(MAKE) cron-governance-check; then \
		echo "season-cutover-ready: cron governance failed; current summary:"; \
		$(MAKE) season-activation-log || true; \
		$(MAKE) cron-summary-json || true; \
		exit 2; \
	fi; \
	$(MAKE) season-activation-log || true; \
	echo "season-cutover-ready: pass"

season-activation-check:
	$(MAKE) mlb-season-kickoff-check BASE_URL="$(BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_MARKET_DAYS="$(MLB_MARKET_DAYS)" MLB_ROSTER_DATE="$(MLB_ROSTER_DATE)" MLB_STAT_DAYS_AGO="$(MLB_STAT_DAYS_AGO)" MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_DERIVED_DAYS="$(MLB_STAT_DERIVED_DAYS)" MLB_STAT_DERIVED_MIN="$(MLB_STAT_DERIVED_MIN)"
	@if [ -z "$(NHL_QUALITY_FROM_DATE)" ] || [ -z "$(NHL_QUALITY_TO_DATE)" ]; then \
		echo "season-activation-check requires NHL_QUALITY_FROM_DATE and NHL_QUALITY_TO_DATE for baseline lock"; \
		exit 2; \
	fi
	$(MAKE) season-baseline-lock MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" NHL_QUALITY_FROM_DATE="$(NHL_QUALITY_FROM_DATE)" NHL_QUALITY_TO_DATE="$(NHL_QUALITY_TO_DATE)" NHL_QUALITY_MIN_TOTAL="$(NHL_QUALITY_MIN_TOTAL)"
	$(MAKE) season-cutover-cadence
	$(MAKE) season-cutover-log
	$(MAKE) season-cutover-ready

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
	$(MAKE) mlb-pipeline-daily-check MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PREDICT_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_PROP_COVERAGE_WINDOW_MODE="$(MLB_PROP_COVERAGE_WINDOW_MODE)" MLB_PROP_COVERAGE_WINDOW_DAYS="$(MLB_PROP_COVERAGE_WINDOW_DAYS)" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROP_COVERAGE_REQUIRED)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_PROP_COVERAGE_MIN_GRADED)" MLB_PROP_COVERAGE_GATE_METRIC="$(MLB_PROP_COVERAGE_GATE_METRIC)" MLB_PROP_COVERAGE_TRAINING_SOURCES="$(MLB_PROP_COVERAGE_TRAINING_SOURCES)"
	@if [ -n "$(NHL_QUALITY_FROM_DATE)" ] && [ -n "$(NHL_QUALITY_TO_DATE)" ]; then \
		$(MAKE) nhl-prediction-quality-auto NHL_QUALITY_FROM_DATE="$(NHL_QUALITY_FROM_DATE)" NHL_QUALITY_TO_DATE="$(NHL_QUALITY_TO_DATE)" NHL_QUALITY_ACTIVE_MIN_TOTAL="$(NHL_QUALITY_ACTIVE_MIN_TOTAL)"; \
	else \
		echo "ops-shortlist-check: skipping nhl-prediction-quality-auto (set NHL_QUALITY_FROM_DATE and NHL_QUALITY_TO_DATE to enable)"; \
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
	$(MAKE) mlb-pipeline-check-json MLB_BASE_URL="$(if $(MLB_BASE_URL),$(MLB_BASE_URL),$(BASE_URL))" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PREDICT_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_PROP_COVERAGE_WINDOW_MODE="$(MLB_PROP_COVERAGE_WINDOW_MODE)" MLB_PROP_COVERAGE_WINDOW_DAYS="$(MLB_PROP_COVERAGE_WINDOW_DAYS)" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROP_COVERAGE_REQUIRED)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_PROP_COVERAGE_MIN_GRADED)" MLB_PROP_COVERAGE_GATE_METRIC="$(MLB_PROP_COVERAGE_GATE_METRIC)" MLB_PROP_COVERAGE_TRAINING_SOURCES="$(MLB_PROP_COVERAGE_TRAINING_SOURCES)"
	@if [ -n "$(BASE_URL)" ] && [ "$(BASE_URL)" != "http://127.0.0.1:8001" ]; then \
		$(MAKE) mlb-post-deploy-strict-offseason BASE_URL="$(BASE_URL)" MLB_DATE="$(MLB_DATE)"; \
	else \
		echo "mlb-season-kickoff-check: skipping post-deploy (set BASE_URL to deployed URL to enable)"; \
	fi

season-baseline-capture:
	@set -e; \
	mkdir -p artifacts/season_baselines; \
	if [ -z "$(NHL_QUALITY_FROM_DATE)" ] || [ -z "$(NHL_QUALITY_TO_DATE)" ]; then \
		echo "season-baseline-capture requires NHL_QUALITY_FROM_DATE and NHL_QUALITY_TO_DATE"; \
		exit 2; \
	fi; \
	mlb_out="artifacts/season_baselines/mlb_quality_$(MLB_QUALITY_WINDOW_MODE)_$(MLB_QUALITY_GAMES_BACK)_$(MLB_QUALITY_WINDOW_DAYS).json"; \
	nhl_out="artifacts/season_baselines/nhl_quality_$(NHL_QUALITY_FROM_DATE)_$(NHL_QUALITY_TO_DATE).json"; \
	mlb_tmp="$$mlb_out.tmp"; \
	nhl_tmp="$$nhl_out.tmp"; \
	rm -f "$$mlb_tmp" "$$nhl_tmp"; \
	if ! $(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality.py --window-mode $(MLB_QUALITY_WINDOW_MODE) --window-days $(MLB_QUALITY_WINDOW_DAYS) --games-back $(MLB_QUALITY_GAMES_BACK) --source-table "$(MLB_QUALITY_SOURCE_TABLE)" $(if $(MLB_QUALITY_ROWS_CSV),--rows-csv "$(MLB_QUALITY_ROWS_CSV)",) $(MLB_QUALITY_RECONCILE_TWO_SIDED_ARG) --min-total $(MLB_QUALITY_MIN_TOTAL) > "$$mlb_tmp"; then \
		echo "season-baseline-capture: MLB baseline generation failed"; \
		if [ -s "$$mlb_tmp" ]; then cat "$$mlb_tmp"; fi; \
		exit 1; \
	fi; \
	if ! $(VENV_PY) backend/nhl/scripts/analyze_nhl_prediction_quality.py --from-date $(NHL_QUALITY_FROM_DATE) --to-date $(NHL_QUALITY_TO_DATE) --min-total $(NHL_QUALITY_MIN_TOTAL) > "$$nhl_tmp"; then \
		echo "season-baseline-capture: NHL baseline generation failed"; \
		if [ -s "$$nhl_tmp" ]; then cat "$$nhl_tmp"; fi; \
		exit 1; \
	fi; \
	mv "$$mlb_tmp" "$$mlb_out"; \
	mv "$$nhl_tmp" "$$nhl_out"; \
	echo "Wrote $$mlb_out"; \
	echo "Wrote $$nhl_out"

mlb-season-baseline-capture:
	@set -e; \
	mkdir -p artifacts/season_baselines; \
	mlb_out="artifacts/season_baselines/mlb_quality_$(MLB_QUALITY_WINDOW_MODE)_$(MLB_QUALITY_GAMES_BACK)_$(MLB_QUALITY_WINDOW_DAYS).json"; \
	mlb_tmp="$$mlb_out.tmp"; \
	rm -f "$$mlb_tmp"; \
	if ! $(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality.py --window-mode $(MLB_QUALITY_WINDOW_MODE) --window-days $(MLB_QUALITY_WINDOW_DAYS) --games-back $(MLB_QUALITY_GAMES_BACK) --source-table "$(MLB_QUALITY_SOURCE_TABLE)" $(if $(MLB_QUALITY_ROWS_CSV),--rows-csv "$(MLB_QUALITY_ROWS_CSV)",) $(MLB_QUALITY_RECONCILE_TWO_SIDED_ARG) --prop-types "$(MLB_PROD12_PROP_TYPES)" --prop-sources "$(MLB_QUALITY_PROP_SOURCES)" --min-total $(MLB_QUALITY_MIN_TOTAL) > "$$mlb_tmp"; then \
		echo "mlb-season-baseline-capture: MLB baseline generation failed"; \
		if [ -s "$$mlb_tmp" ]; then cat "$$mlb_tmp"; fi; \
		exit 1; \
	fi; \
	mv "$$mlb_tmp" "$$mlb_out"; \
	echo "Wrote $$mlb_out"

mlb-prod12-phase2-baseline-capture:
	@set -e; \
	mode="$(MLB_CANDIDATE_WINDOW_MODE)"; \
	if [ -z "$$mode" ]; then mode="games"; fi; \
	out_path="$(MLB_PROD12_PHASE2_BASELINE_PATH)"; \
	out_tmp="$$out_path.tmp"; \
	mkdir -p "$$(dirname "$$out_path")"; \
	rm -f "$$out_tmp"; \
	if ! $(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality.py --window-mode "$$mode" --window-days $(MLB_CANDIDATE_WINDOW_DAYS) --games-back $(MLB_CANDIDATE_GAMES_BACK) --source-table "$(MLB_PROD12_CANDIDATE_SOURCE_TABLE)" $(if $(MLB_PROD12_CANDIDATE_ROWS_CSV),--rows-csv "$(MLB_PROD12_CANDIDATE_ROWS_CSV)",) $(MLB_QUALITY_RECONCILE_TWO_SIDED_ARG) --prop-types "$(MLB_PROD12_CANDIDATE_PROP_TYPES)" --min-total 1 > "$$out_tmp"; then \
		echo "mlb-prod12-phase2-baseline-capture: baseline generation failed"; \
		if [ -s "$$out_tmp" ]; then cat "$$out_tmp"; fi; \
		exit 1; \
	fi; \
	mv "$$out_tmp" "$$out_path"; \
	echo "Wrote $$out_path"

mlb-prod8-baseline-capture:
	@set -e; \
	mkdir -p "$(MLB_PROD8_BASELINE_DIR)"; \
	quality_out="$(MLB_PROD8_BASELINE_DIR)/mlb_prod8_quality_games_$(MLB_QUALITY_GAMES_BACK).json"; \
	pipeline_out="$(MLB_PROD8_BASELINE_DIR)/mlb_prod8_pipeline_games_$(MLB_QUALITY_GAMES_BACK).json"; \
	quality_tmp="$$quality_out.tmp"; \
	pipeline_tmp="$$pipeline_out.tmp"; \
	rm -f "$$quality_tmp" "$$pipeline_tmp"; \
	if ! $(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality.py --window-mode games --games-back $(MLB_QUALITY_GAMES_BACK) --source-table "$(MLB_QUALITY_SOURCE_TABLE)" $(if $(MLB_QUALITY_ROWS_CSV),--rows-csv "$(MLB_QUALITY_ROWS_CSV)",) $(MLB_QUALITY_RECONCILE_TWO_SIDED_ARG) --prop-types "$(MLB_PROD8_PROP_TYPES)" --prop-sources "$(MLB_QUALITY_PROP_SOURCES)" --min-total $(MLB_QUALITY_MIN_TOTAL) > "$$quality_tmp"; then \
		echo "mlb-prod8-baseline-capture: quality generation failed"; \
		if [ -s "$$quality_tmp" ]; then cat "$$quality_tmp"; fi; \
		exit 1; \
	fi; \
	if ! $(VENV_PY) backend/mlb/scripts/mlb_pipeline_check.py $(if $(MLB_BASE_URL),--base-url $(MLB_BASE_URL),) --date $(MLB_DATE) --sample-size $(MLB_PREDICT_SAMPLE) --require-min-success $(MLB_PREDICT_MIN_SUCCESS) --prop-types "$(MLB_PROD8_PROP_TYPES)" --quality-window-mode games --quality-window-days $(MLB_QUALITY_WINDOW_DAYS) --quality-games-back $(MLB_QUALITY_GAMES_BACK) --quality-min-total $(MLB_QUALITY_MIN_TOTAL) --quality-min-accuracy $(MLB_QUALITY_MIN_ACCURACY) --quality-prop-sources "$(MLB_QUALITY_PROP_SOURCES)" --coverage-window-mode games --coverage-window-days $(MLB_PROP_COVERAGE_WINDOW_DAYS) --coverage-games-back $(MLB_PROP_COVERAGE_GAMES_BACK) --coverage-required-props "$(MLB_PROD8_PROP_TYPES)" --coverage-min-graded-per-prop $(MLB_CORE_MIN_GRADED) --coverage-gate-metric training_source --coverage-training-prop-sources "$(MLB_CORE_TRAINING_SOURCES)" > "$$pipeline_tmp"; then \
		echo "mlb-prod8-baseline-capture: pipeline generation failed"; \
		if [ -s "$$pipeline_tmp" ]; then cat "$$pipeline_tmp"; fi; \
		exit 1; \
	fi; \
	mv "$$quality_tmp" "$$quality_out"; \
	mv "$$pipeline_tmp" "$$pipeline_out"; \
	echo "Wrote $$quality_out"; \
	echo "Wrote $$pipeline_out"

cron-governance-check:
	$(MAKE) workflow-inventory-strict
	$(MAKE) workflow-path-audit-strict
	$(MAKE) nhl-workflow-compat-check
	$(MAKE) docs-make-target-audit

cron-governance-snapshot:
	$(VENV_PY) backend/scripts/cron_governance_snapshot.py

assistant-handoff-bundle:
	$(VENV_PY) backend/scripts/assistant_handoff_bundle.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN) --history-input $(OPS_HISTORY_INPUT) --history-limit $(OPS_HISTORY_LIMIT) --pipeline-history-input $(MLB_PIPELINE_HISTORY_INPUT) --pipeline-history-limit $(MLB_PIPELINE_HISTORY_LIMIT) --season-activation-input $(SEASON_HISTORY_INPUT) --season-history-max-age-hours $(SEASON_HISTORY_MAX_AGE_HOURS) --season-cutover-input $(SEASON_CUTOVER_HISTORY_INPUT) --season-cutover-limit $(SEASON_CUTOVER_HISTORY_LIMIT)

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
	$(VENV_PY) backend/nhl/scripts/check_nhl_workflow_compat.py --quiet

nhl-workflow-compat-summary-json:
	$(VENV_PY) backend/nhl/scripts/check_nhl_workflow_compat.py --json

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
	$(VENV_PY) backend/_legacy/scripts/check_frontend_route_smoke.py

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
	$(VENV_PY) backend/_legacy/scripts/smoke_mlb_api.py --mode offline
	$(VENV_PY) backend/_legacy/scripts/check_mlb_openapi_contract.py
	$(MAKE) mlb-checks-profile-contract

# Default day-to-day MLB verification.
# Includes metrics endpoint shape checks (requires DB connectivity from backend).
mlb-checks: mlb-checks-offline
	$(VENV_PY) backend/mlb/scripts/validate_mlb_metrics.py --api-only

# Auto mode for local environments where DB may be unavailable.
# Runs strict DB-dependent checks when possible; otherwise keeps offline checks green.
mlb-checks-auto: mlb-checks-offline
	@if $(VENV_PY) backend/mlb/scripts/validate_mlb_metrics.py --api-only; then \
		echo "mlb-checks-auto: metrics api-only passed"; \
	else \
		echo "mlb-checks-auto: DB-dependent metrics unavailable, kept offline checks only"; \
	fi

# Full verification pass (historical DB + schedule/context checks).
# Requires DB connectivity and outbound MLB StatsAPI access.
mlb-checks-full: mlb-checks
	$(VENV_PY) backend/_legacy/scripts/smoke_mlb_api.py --mode full --date 2025-08-15
	$(VENV_PY) backend/mlb/scripts/validate_mlb_metrics.py
	$(MAKE) mlb-checks-props-contract
	$(MAKE) mlb-checks-golden

# Golden-path write-aware smoke (prepare -> predict -> add -> duplicate replay).
# Requires DB connectivity and a resolvable historical game context.
mlb-checks-golden:
	$(VENV_PY) backend/_legacy/scripts/smoke_mlb_prop_flow.py --date 2025-08-15 --team-id 119 --player-id 660271

# DB contract check for fields consumed by frontend PlayerPropsTable.
mlb-checks-props-contract:
	$(VENV_PY) backend/mlb/scripts/validate_mlb_props_contract.py

# Focused MLB player-surface regression lane (repository/domain/service/router).
mlb-player-surface-checks:
	$(VENV_PY) -m unittest backend/tests/test_mlb_player_repository.py backend/tests/test_mlb_player_directory.py backend/tests/test_mlb_player_service.py backend/tests/test_mlb_player_resolver.py backend/tests/test_mlb_players_endpoint.py -v

# Warm MLB OddsAPI cache snapshot for ET date window (cron-friendly).
mlb-market-cache-refresh:
	$(VENV_PY) -m backend.mlb.scripts.refresh_mlb_market_cache --days $(MLB_MARKET_DAYS)

# Full-team MLB player/roster refresh (all teams; not slate-limited).
mlb-roster-refresh-all:
	$(VENV_PY) -m backend.mlb.scripts.refresh_mlb_players_rosters --date $(MLB_ROSTER_DATE)

.PHONY: mlb-bvp-pvb-refresh

# Refresh BvP/PvB features and merge into mlb.prop_features_precomputed.
mlb-bvp-pvb-refresh:
	@if [ -n "$(MLB_BVP_FROM_DATE)" ] || [ -n "$(MLB_BVP_TO_DATE)" ]; then \
		if [ -z "$(MLB_BVP_FROM_DATE)" ] || [ -z "$(MLB_BVP_TO_DATE)" ]; then \
			echo "mlb-bvp-pvb-refresh requires both MLB_BVP_FROM_DATE and MLB_BVP_TO_DATE when range mode is used"; \
			exit 2; \
		fi; \
		$(VENV_PY) backend/mlb/scripts/refresh_mlb_bvp_pvb.py --from-date "$(MLB_BVP_FROM_DATE)" --to-date "$(MLB_BVP_TO_DATE)" --feature-set-tag "$(MLB_BVP_FEATURE_SET_TAG)" --model-tag "$(MLB_BVP_MODEL_TAG)" --batch-size "$(MLB_BVP_BATCH_SIZE)" --request-timeout-sec "$(MLB_BVP_REQUEST_TIMEOUT_SEC)" --request-retries "$(MLB_BVP_REQUEST_RETRIES)" $(if $(filter 1,$(MLB_BVP_DRY_RUN)),--dry-run,); \
	else \
		$(VENV_PY) backend/mlb/scripts/refresh_mlb_bvp_pvb.py --date "$(MLB_BVP_DATE)" --feature-set-tag "$(MLB_BVP_FEATURE_SET_TAG)" --model-tag "$(MLB_BVP_MODEL_TAG)" --batch-size "$(MLB_BVP_BATCH_SIZE)" --request-timeout-sec "$(MLB_BVP_REQUEST_TIMEOUT_SEC)" --request-retries "$(MLB_BVP_REQUEST_RETRIES)" $(if $(filter 1,$(MLB_BVP_DRY_RUN)),--dry-run,); \
	fi

.PHONY: mlb-bvp-impact-preflight mlb-bvp-impact-report mlb-pa-foundation-health mlb-pa-foundation-propagate mlb-morning-workflow-audit mlb-live-hitter-parent-daily-integration mlb-hits05-live-expected-pa-shadow

# Estimate BvP/PvB impact runtime before launching the expensive row-by-row comparison.
mlb-bvp-impact-preflight:
	$(VENV_PY) backend/mlb/scripts/preflight_mlb_bvp_impact.py --slate-csv "$(MLB_BVP_IMPACT_SLATE_CSV)" --wide-csv "$(MLB_BVP_IMPACT_WIDE_CSV)" --impact-json "$(MLB_BVP_IMPACT_OUT_JSON)" --label-date "$(MLB_BVP_IMPACT_LABEL_DATE)" --medium-rows "$(MLB_BVP_IMPACT_PREFLIGHT_MEDIUM_ROWS)" --high-rows "$(MLB_BVP_IMPACT_PREFLIGHT_HIGH_ROWS)" --fail-high "$(MLB_BVP_IMPACT_PREFLIGHT_FAIL_HIGH)"

# Compare prediction probabilities with BvP/PvB hydration enabled vs disabled.
mlb-bvp-impact-report:
	$(VENV_PY) backend/mlb/scripts/report_mlb_bvp_impact.py --slate-csv "$(MLB_BVP_IMPACT_SLATE_CSV)" --wide-csv "$(MLB_BVP_IMPACT_WIDE_CSV)" --out-json "$(MLB_BVP_IMPACT_OUT_JSON)" --out-csv "$(MLB_BVP_IMPACT_OUT_CSV)" --history-jsonl "$(MLB_BVP_IMPACT_HISTORY_JSONL)" --label-date "$(MLB_BVP_IMPACT_LABEL_DATE)" --max-rows "$(MLB_BVP_IMPACT_MAX_ROWS)" --require-db "$(MLB_BVP_IMPACT_REQUIRE_DB)"

# Track league hits/game environment and annotate today's hits_allowed slate rows.
mlb-hits-environment-report:
	$(VENV_PY) backend/mlb/scripts/report_mlb_hits_environment.py --as-of-date "$(MLB_HITS_ENV_AS_OF_DATE)" --lookback-days "$(MLB_HITS_ENV_LOOKBACK_DAYS)" --recent-days "$(MLB_HITS_ENV_RECENT_DAYS)" --starter-baseline-seasons "$(MLB_HITS_ENV_STARTER_BASELINE_SEASONS)" --starter-baseline-min-starts "$(MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS)" --starter-baseline-season-weight-decay "$(MLB_HITS_ENV_STARTER_BASELINE_DECAY)" --slate-offense-weight-last7 "$(MLB_HITS_ENV_SLATE_WEIGHT_LAST7)" --slate-offense-weight-last15 "$(MLB_HITS_ENV_SLATE_WEIGHT_LAST15)" --slate-offense-weight-last30 "$(MLB_HITS_ENV_SLATE_WEIGHT_LAST30)" --slate-offense-factor-min "$(MLB_HITS_ENV_SLATE_FACTOR_MIN)" --slate-offense-factor-max "$(MLB_HITS_ENV_SLATE_FACTOR_MAX)" --slate-date "$(MLB_HITS_ENV_SLATE_DATE)" --slate-csv "$(MLB_HITS_ENV_SLATE_CSV)" --wide-csv "$(MLB_HITS_ENV_WIDE_CSV)" --odds-snapshot "$(MLB_HITS_ENV_ODDS_SNAPSHOT)" --out-json "$(MLB_HITS_ENV_OUT_JSON)" --out-csv "$(MLB_HITS_ENV_OUT_CSV)" --snapshot-dir "$(MLB_HITS_ENV_SNAPSHOT_DIR)" --history-jsonl "$(MLB_HITS_ENV_HISTORY_JSONL)" --eval-tracker-csv "$(MLB_HITS_ENV_EVAL_TRACKER_CSV)"

# Check passive model-context lineage in current-slate artifacts.
mlb-daily-feature-lineage-health:
	$(VENV_PY) backend/mlb/scripts/check_mlb_daily_feature_lineage_health.py --date "$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)" --slate-output-csv "$(MLB_SLATE_OUTPUT_CSV)" --warn-null-threshold "$(MLB_DAILY_FEATURE_LINEAGE_HEALTH_NULL_WARN_THRESHOLD)" --out-json "$(MLB_DAILY_FEATURE_LINEAGE_HEALTH_DATED_JSON)" --latest-json "$(MLB_DAILY_FEATURE_LINEAGE_HEALTH_JSON)" --out-md "$(MLB_DAILY_FEATURE_LINEAGE_HEALTH_MD)"

# Report PA source health and passive downstream propagation gaps.
mlb-pa-foundation-health:
	$(VENV_PY) backend/mlb/scripts/check_mlb_pa_foundation_health.py --date "$(MLB_PA_FOUNDATION_HEALTH_DATE)" --out-dir "$(MLB_PA_FOUNDATION_HEALTH_OUT_DIR)" --write-pilot

# Passively retain PA fields in research/diagnostic artifacts only.
mlb-pa-foundation-propagate:
	$(VENV_PY) backend/mlb/scripts/hydrate_mlb_pa_foundation_context.py --date "$(MLB_PA_FOUNDATION_PROPAGATION_DATE)" --completed-date "$(MLB_PA_FOUNDATION_PROPAGATION_COMPLETED_DATE)" --out-dir "$(MLB_PA_FOUNDATION_PROPAGATION_OUT_DIR)" --write

# Research-only PA opportunity shadow test for Hits O1.5/U1.5.
mlb-pa-opportunity-shadow-test:
	$(VENV_PY) backend/mlb/scripts/run_mlb_pa_opportunity_shadow_test.py --date "$(MLB_PA_OPPORTUNITY_SHADOW_DATE)" --out-dir "$(MLB_PA_OPPORTUNITY_SHADOW_OUT_DIR)"

mlb-live-hitter-parent-daily-integration:
	@if [ "$(MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE)" = "1" ]; then \
		if [ -z "$(MLB_LIVE_HITTER_PARENT_LINEUP_PLAYER_ROWS)" ]; then \
			$(VENV_PY) -m backend.mlb.scripts.capture_mlb_governed_pregame_lineups --date "$(MLB_LIVE_HITTER_PARENT_DATE)" --output-dir "$(MLB_GOVERNED_LINEUP_CAPTURE_OUT_DIR)" --mode dry_run $(if $(strip $(MLB_GOVERNED_LINEUP_CAPTURE_RUN_TAG)),--run-tag "$(MLB_GOVERNED_LINEUP_CAPTURE_RUN_TAG)",) $(if $(strip $(MLB_GOVERNED_LINEUP_CAPTURE_CUTOFF)),--cutoff "$(MLB_GOVERNED_LINEUP_CAPTURE_CUTOFF)",); \
			lineup_rows="$(MLB_GOVERNED_LINEUP_CAPTURE_PARSED)"; \
		else \
			lineup_rows="$(MLB_LIVE_HITTER_PARENT_LINEUP_PLAYER_ROWS)"; \
		fi; \
		$(VENV_PY) -m backend.mlb.scripts.run_mlb_live_hitter_parent_daily_integration --date "$(MLB_LIVE_HITTER_PARENT_DATE)" --mode dry_run --enabled --output-dir "$(MLB_LIVE_HITTER_PARENT_OUT_DIR)" $(if $(strip $(MLB_LIVE_HITTER_PARENT_RUN_TAG)),--run-tag "$(MLB_LIVE_HITTER_PARENT_RUN_TAG)",) $(if $(strip $(MLB_LIVE_HITTER_PARENT_CUTOFF)),--cutoff "$(MLB_LIVE_HITTER_PARENT_CUTOFF)",) $(if $(strip $(MLB_LIVE_HITTER_PARENT_SLATE_ARTIFACT)),--slate-artifact "$(MLB_LIVE_HITTER_PARENT_SLATE_ARTIFACT)",) --lineup-player-rows "$$lineup_rows" $(if $(strip $(MLB_LIVE_HITTER_PARENT_OPPORTUNITY_PROFILE_PARENT)),--opportunity-profile-parent "$(MLB_LIVE_HITTER_PARENT_OPPORTUNITY_PROFILE_PARENT)",); \
	else \
		echo "mlb-live-hitter-parent-daily-integration: disabled (MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE=$(MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE))"; \
	fi

mlb-hits05-live-expected-pa-shadow:
	@if [ "$(MLB_ENABLE_HITS05_LIVE_PA_SHADOW)" = "1" ]; then \
		$(VENV_PY) -m backend.mlb.scripts.build_mlb_hits05_live_expected_pa_parent --date "$(MLB_HITS05_LIVE_PA_SHADOW_DATE)" --mode dry_run --output-root "$(MLB_HITS05_LIVE_PA_SHADOW_OUTPUT_ROOT)" $(if $(strip $(MLB_HITS05_LIVE_PA_SHADOW_RUN_TAG)),--run-tag "$(MLB_HITS05_LIVE_PA_SHADOW_RUN_TAG)",) $(if $(strip $(MLB_HITS05_LIVE_PA_SHADOW_PREDICTION_TIMESTAMP)),--prediction-timestamp "$(MLB_HITS05_LIVE_PA_SHADOW_PREDICTION_TIMESTAMP)",) $(if $(strip $(MLB_HITS05_LIVE_PA_SHADOW_CURRENT_PARENT_DIR)),--current-parent-dir "$(MLB_HITS05_LIVE_PA_SHADOW_CURRENT_PARENT_DIR)",); \
	else \
		echo "mlb-hits05-live-expected-pa-shadow: disabled (MLB_ENABLE_HITS05_LIVE_PA_SHADOW=$(MLB_ENABLE_HITS05_LIVE_PA_SHADOW))"; \
	fi

mlb-daily-health-gates:
	$(MAKE) mlb-expanded-o15-universe MLB_EXPANDED_O15_UNIVERSE_DATE="" MLB_EXPANDED_O15_UNIVERSE_DATE_FROM="" MLB_EXPANDED_O15_UNIVERSE_DATE_TO=""
	$(MAKE) mlb-expanded-o15-context-health MLB_EXPANDED_O15_CONTEXT_HEALTH_DATE="$(MLB_DAILY_PREFLIGHT_DATE)"
	$(MAKE) mlb-identity-health
	$(MAKE) mlb-o15-ontology-health MLB_O15_ONTOLOGY_HEALTH_DATE="$(MLB_DAILY_PREFLIGHT_DATE)"
	$(MAKE) mlb-project-invariants MLB_PROJECT_INVARIANTS_DATE="$(MLB_DAILY_PREFLIGHT_DATE)"

# Verify current-slate artifacts that must exist before the Ops Brief renders review-board counts.
mlb-daily-preflight:
	$(VENV_PY) backend/mlb/scripts/check_mlb_daily_orchestration_preflight.py --date "$(MLB_DAILY_PREFLIGHT_DATE)" --slate-output-csv "$(MLB_SLATE_OUTPUT_CSV)" --lane-selector-csv "backend/mlb/exports/model_v2/lanes/today/$(MLB_DAILY_PREFLIGHT_DATE)/hits_lane_selector_$(MLB_DAILY_PREFLIGHT_DATE).csv" --quick-card-csv "backend/mlb/exports/model_v2/lanes/today/$(MLB_DAILY_PREFLIGHT_DATE)/quick_card_hits_$(MLB_DAILY_PREFLIGHT_DATE).csv" --hits-o15-simple-csv "$(MLB_HITS_O15_SIMPLE_FILTER_OUT_DIR)/hits_o15_simple_filter_$(MLB_DAILY_PREFLIGHT_DATE).csv" --hits-o15-watch-csv "$(MLB_HITS_O15_WATCH_CANDIDATES_OUT_DIR)/hits_o15_watch_candidates_$(MLB_DAILY_PREFLIGHT_DATE).csv" --hits-o15-layered-csv "$(MLB_HITS_O15_LAYERED_CANDIDATES_OUT_DIR)/hits_o15_layered_candidates_$(MLB_DAILY_PREFLIGHT_DATE).csv" --hits-u15-favorite-csv "$(MLB_HITS_U15_FAVORITE_AUDIT_OUT_DIR)/hits_u15_favorite_audit_$(MLB_DAILY_PREFLIGHT_DATE).csv" --hits-o15-alternate-csv "$(MLB_HITS_O15_ALTERNATE_DISCOVERY_OUT_DIR)/hits_o15_alternate_discovery_$(MLB_DAILY_PREFLIGHT_DATE).csv" --expanded-o15-context-health-json "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)/expanded_o15_context_health_$(MLB_DAILY_PREFLIGHT_DATE).json" --project-invariants-json "$(MLB_PROJECT_INVARIANTS_OUT_DIR)/mlb_project_invariants_$(MLB_DAILY_PREFLIGHT_DATE).json" --out-json "$(MLB_DAILY_PREFLIGHT_OUT_JSON)" --out-md "$(MLB_DAILY_PREFLIGHT_OUT_MD)"

mlb-morning-operating-system:
	$(VENV_PY) backend/mlb/scripts/build_mlb_morning_operating_system.py --date "$(MLB_MORNING_OPERATING_SYSTEM_DATE)" --out-root "$(MLB_MORNING_OPERATING_SYSTEM_OUT_ROOT)"

mlb-o15-morning-workbench:
	$(VENV_PY) backend/mlb/scripts/build_o15_morning_workbench.py --date "$(MLB_O15_MORNING_WORKBENCH_DATE)" --out-dir "$(MLB_O15_MORNING_WORKBENCH_OUT_DIR)"

mlb-daily-index:
	$(MAKE) mlb-morning-operating-system MLB_MORNING_OPERATING_SYSTEM_DATE="$(MLB_DAILY_INDEX_DATE)"
	$(MAKE) mlb-o15-morning-workbench MLB_O15_MORNING_WORKBENCH_DATE="$(MLB_DAILY_INDEX_DATE)"
	$(VENV_PY) backend/mlb/scripts/build_mlb_artifact_index.py --date "$(MLB_DAILY_INDEX_DATE)" --completed-slate-date "$(MLB_DAILY_INDEX_COMPLETED_SLATE_DATE)" --out-root "$(MLB_DAILY_INDEX_OUT_ROOT)"

mlb-daily-index-check:
	$(VENV_PY) backend/mlb/scripts/build_mlb_artifact_index.py --date "$(MLB_DAILY_INDEX_DATE)" --out-root "$(MLB_DAILY_INDEX_OUT_ROOT)" --check-only

mlb-morning-workflow-audit:
	$(VENV_PY) backend/mlb/scripts/audit_mlb_morning_workflow.py --date "$(MLB_MORNING_WORKFLOW_AUDIT_DATE)" --out-root "$(MLB_MORNING_WORKFLOW_AUDIT_OUT_ROOT)"

# Refresh all local/date-owned artifacts consumed by the daily ops brief.
mlb-refresh-daily-ops-brief-inputs:
	$(VENV_PY) backend/mlb/scripts/refresh_mlb_daily_ops_brief_inputs.py --completed-slate-date "$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)" --current-slate-date "$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)" --reconcile-rows-csv "$(MLB_DAILY_BRIEF_RECONCILE_ROWS_CSV)" --model-vs-fade-json "$(MLB_DAILY_BRIEF_MODEL_VS_FADE_JSON)" --model-vs-fade-csv "$(MLB_MODEL_VS_FADE_OUT_CSV)" --all-available-json "$(MLB_ALL_AVAILABLE_OUT_JSON)" --all-available-csv "$(MLB_ALL_AVAILABLE_OUT_CSV)" --postgrade-alerts-json "$(MLB_DAILY_BRIEF_POSTGRADE_ALERTS_JSON)" --postgrade-alerts-history-jsonl "$(MLB_POSTGRADE_ALERTS_HISTORY_JSONL)" --postgrade-tracker-csv "$(MLB_POSTGRADE_TRACKER_OUT_CSV)" --postgrade-by-prop-tracker-csv "$(MLB_POSTGRADE_TRACKER_OUT_BY_PROP_CSV)" --graded-summary-json "$(MLB_GRADED_SUMMARY_OUT_JSON)" --graded-by-prop-csv "$(MLB_GRADED_BY_PROP_OUT_CSV)" --book-upload-csv "$(MLB_BOOK_UPLOAD_OUT_CSV)" --model-performance-summary-csv "$(MLB_MODEL_PERFORMANCE_SUMMARY_CSV)" --model-performance-daily-csv "$(MLB_MODEL_PERFORMANCE_DAILY_CSV)" --reporting-alignment-csv "backend/mlb/exports/reporting_alignment/reporting_alignment_$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE).csv" --reporting-alignment-md "backend/mlb/exports/reporting_alignment/reporting_alignment_$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE).md" --prop-regime-csv "$(MLB_PROP_REGIME_DEPLOY_CSV)" --hits-environment-json "$(MLB_DAILY_BRIEF_HITS_ENV_JSON)" --bvp-impact-json "$(MLB_DAILY_BRIEF_BVP_IMPACT_JSON)" --hits-15-tier-backtest-json "$(MLB_HITS_15_TIER_BACKTEST_JSON)" --review-aid-performance-json "$(MLB_REVIEW_AID_PERFORMANCE_JSON)" --total-bases-shadow-summary-json "$(MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_SUMMARY_JSON)" --total-bases-shadow-evaluation-json "$(MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_EVALUATION_JSON)" --brief-output-md "$(MLB_DAILY_BRIEF_OUT_MD)" --status-json "$(MLB_DAILY_BRIEF_INPUT_REFRESH_STATUS_JSON)" --refresh-bvp-impact "$(MLB_DAILY_BRIEF_REFRESH_BVP_IMPACT)" --bvp-impact-timeout-sec "$(MLB_DAILY_BRIEF_BVP_IMPACT_TIMEOUT_SEC)" --allow-graded-date-mismatch "$(MLB_DAILY_BRIEF_ALLOW_GRADED_DATE_MISMATCH)"

# Build a daily human-readable MLB ops brief from current artifacts.
mlb-daily-ops-brief:
	@if [ "$(MLB_DAILY_BRIEF_REFRESH_INPUTS)" = "1" ]; then \
		set +e; \
		$(MAKE) mlb-refresh-daily-ops-brief-inputs MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE="$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)" MLB_DAILY_BRIEF_CURRENT_SLATE_DATE="$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)" MLB_DAILY_BRIEF_RECONCILE_ROWS_CSV="$(MLB_DAILY_BRIEF_RECONCILE_ROWS_CSV)" MLB_DAILY_BRIEF_MODEL_VS_FADE_JSON="$(MLB_DAILY_BRIEF_MODEL_VS_FADE_JSON)" MLB_DAILY_BRIEF_POSTGRADE_ALERTS_JSON="$(MLB_DAILY_BRIEF_POSTGRADE_ALERTS_JSON)" MLB_DAILY_BRIEF_HITS_ENV_JSON="$(MLB_DAILY_BRIEF_HITS_ENV_JSON)" MLB_DAILY_BRIEF_BVP_IMPACT_JSON="$(MLB_DAILY_BRIEF_BVP_IMPACT_JSON)" MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_SUMMARY_JSON="$(MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_SUMMARY_JSON)" MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_EVALUATION_JSON="$(MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_EVALUATION_JSON)" MLB_DAILY_BRIEF_INPUT_REFRESH_STATUS_JSON="$(MLB_DAILY_BRIEF_INPUT_REFRESH_STATUS_JSON)" MLB_DAILY_BRIEF_REFRESH_BVP_IMPACT="$(MLB_DAILY_BRIEF_REFRESH_BVP_IMPACT)" MLB_DAILY_BRIEF_ALLOW_GRADED_DATE_MISMATCH="$(MLB_DAILY_BRIEF_ALLOW_GRADED_DATE_MISMATCH)" MLB_DAILY_BRIEF_OUT_MD="$(MLB_DAILY_BRIEF_OUT_MD)"; \
		refresh_rc=$$?; \
		set -e; \
		if [ "$$refresh_rc" -ne 0 ]; then \
			echo "mlb-daily-ops-brief: WARN input refresh failed rc=$$refresh_rc; continuing to generate brief with source diagnostics"; \
		fi; \
	else \
		echo "mlb-daily-ops-brief: skipping input refresh (MLB_DAILY_BRIEF_REFRESH_INPUTS=$(MLB_DAILY_BRIEF_REFRESH_INPUTS))"; \
	fi
	@set +e; \
	$(MAKE) mlb-daily-feature-lineage-health MLB_DAILY_BRIEF_CURRENT_SLATE_DATE="$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)" MLB_DAILY_FEATURE_LINEAGE_HEALTH_JSON="$(MLB_DAILY_FEATURE_LINEAGE_HEALTH_JSON)" MLB_DAILY_FEATURE_LINEAGE_HEALTH_DATED_JSON="$(MLB_DAILY_FEATURE_LINEAGE_HEALTH_DATED_JSON)" MLB_DAILY_FEATURE_LINEAGE_HEALTH_MD="$(MLB_DAILY_FEATURE_LINEAGE_HEALTH_MD)"; \
	lineage_rc=$$?; \
	set -e; \
	if [ "$$lineage_rc" -ne 0 ]; then \
		echo "mlb-daily-ops-brief: WARN feature lineage health returned rc=$$lineage_rc; continuing to generate brief with source diagnostics"; \
	fi
	@if [ "$(MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE)" = "1" ]; then \
		$(MAKE) mlb-live-hitter-parent-daily-integration MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE=1 MLB_LIVE_HITTER_PARENT_DATE="$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)" MLB_LIVE_HITTER_PARENT_RUN_TAG="$(MLB_LIVE_HITTER_PARENT_RUN_TAG)" MLB_LIVE_HITTER_PARENT_CUTOFF="$(MLB_LIVE_HITTER_PARENT_CUTOFF)" MLB_LIVE_HITTER_PARENT_SLATE_ARTIFACT="$(MLB_LIVE_HITTER_PARENT_SLATE_ARTIFACT)" MLB_LIVE_HITTER_PARENT_LINEUP_PLAYER_ROWS="$(MLB_LIVE_HITTER_PARENT_LINEUP_PLAYER_ROWS)" MLB_LIVE_HITTER_PARENT_OPPORTUNITY_PROFILE_PARENT="$(MLB_LIVE_HITTER_PARENT_OPPORTUNITY_PROFILE_PARENT)" MLB_LIVE_HITTER_PARENT_OUT_DIR="$(MLB_LIVE_HITTER_PARENT_OUT_DIR)"; \
	else \
		:; \
	fi
	@if [ "$(MLB_ENABLE_HITS05_LIVE_PA_SHADOW)" = "1" ]; then \
		set +e; \
		$(MAKE) mlb-hits05-live-expected-pa-shadow MLB_ENABLE_HITS05_LIVE_PA_SHADOW=1 MLB_HITS05_LIVE_PA_SHADOW_DATE="$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)" MLB_HITS05_LIVE_PA_SHADOW_RUN_TAG="$(MLB_HITS05_LIVE_PA_SHADOW_RUN_TAG)" MLB_HITS05_LIVE_PA_SHADOW_PREDICTION_TIMESTAMP="$(MLB_HITS05_LIVE_PA_SHADOW_PREDICTION_TIMESTAMP)" MLB_HITS05_LIVE_PA_SHADOW_CURRENT_PARENT_DIR="$(MLB_HITS05_LIVE_PA_SHADOW_CURRENT_PARENT_DIR)" MLB_HITS05_LIVE_PA_SHADOW_OUTPUT_ROOT="$(MLB_HITS05_LIVE_PA_SHADOW_OUTPUT_ROOT)"; \
		hits05_pa_rc=$$?; \
		set -e; \
		if [ "$$hits05_pa_rc" -ne 0 ]; then \
			echo "mlb-daily-ops-brief: WARN Hits 0.5 live expected-PA shadow returned rc=$$hits05_pa_rc; continuing"; \
		fi; \
	else \
		:; \
	fi
	$(VENV_PY) backend/mlb/scripts/report_mlb_daily_ops_brief.py --report-date "$(MLB_DAILY_BRIEF_REPORT_DATE)" --completed-slate-date "$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)" --current-slate-date "$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)" --postgrade-alerts-json "$(MLB_DAILY_BRIEF_POSTGRADE_ALERTS_JSON)" --model-vs-fade-json "$(MLB_DAILY_BRIEF_MODEL_VS_FADE_JSON)" --prop-regime-csv "$(MLB_PROP_REGIME_DEPLOY_CSV)" --model-performance-summary-csv "$(MLB_MODEL_PERFORMANCE_SUMMARY_CSV)" --model-performance-daily-csv "$(MLB_MODEL_PERFORMANCE_DAILY_CSV)" --reporting-alignment-csv "backend/mlb/exports/reporting_alignment/reporting_alignment_{completed_slate_date}.csv" --bvp-impact-json "$(MLB_DAILY_BRIEF_BVP_IMPACT_JSON)" --require-fresh-bvp-impact "$(MLB_DAILY_BRIEF_REQUIRE_FRESH_BVP_IMPACT)" --hits-environment-json "$(MLB_DAILY_BRIEF_HITS_ENV_JSON)" --hits-15-tier-backtest-json "$(MLB_HITS_15_TIER_BACKTEST_JSON)" --review-aid-performance-json "$(MLB_REVIEW_AID_PERFORMANCE_JSON)" --total-bases-shadow-summary-json "$(MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_SUMMARY_JSON)" --total-bases-shadow-evaluation-json "$(MLB_DAILY_BRIEF_TOTAL_BASES_SHADOW_EVALUATION_JSON)" --feature-lineage-health-json "$(MLB_DAILY_FEATURE_LINEAGE_HEALTH_JSON)" --input-refresh-status-json "$(MLB_DAILY_BRIEF_INPUT_REFRESH_STATUS_JSON)" --pipeline-history-jsonl "$(MLB_DAILY_BRIEF_PIPELINE_HISTORY_JSONL)" --ops-history-jsonl "$(MLB_DAILY_BRIEF_OPS_HISTORY_JSONL)" --rolling-candidate-obs-json "$(MLB_ROLLING_CANDIDATE_OBS_JSON)" --rolling-candidate-obs-mode "$(MLB_ENABLE_ROLLING_CANDIDATE_OBS)" --betonline-capture-integrity-json "$(MLB_BETONLINE_CAPTURE_INTEGRITY_JSON)" --out-md "$(MLB_DAILY_BRIEF_OUT_MD)" --dated-out-md "$(MLB_DAILY_BRIEF_DATED_OUT_MD)" --out-json "$(MLB_DAILY_BRIEF_OUT_JSON)" --history-jsonl "$(MLB_DAILY_BRIEF_HISTORY_JSONL)" $(if $(filter 1 true TRUE yes YES,$(MLB_ENABLE_ROLLING_CANDIDATE_OBS)),--enable-rolling-candidate-obs,) $(if $(filter 0 false FALSE no NO,$(MLB_DAILY_BRIEF_REFRESH_INPUTS)),--skip-today-workspace-fetch,)

.PHONY: mlb-daily-rolling-observation
mlb-daily-rolling-observation:
	@if [ "$(MLB_ROLLING_OBSERVATION_DATE)" != "$(MLB_ROLLING_OBSERVATION_EFFECTIVE_DATE)" ]; then echo "mlb-daily-rolling-observation: resolving placeholder date '$(MLB_ROLLING_OBSERVATION_DATE)' to today's slate '$(MLB_ROLLING_OBSERVATION_EFFECTIVE_DATE)'"; fi
	$(VENV_PY) -m backend.mlb.scripts.build_mlb_rolling_market_late_candidates --date "$(MLB_ROLLING_OBSERVATION_EFFECTIVE_DATE)" --mode dry_run --output-dir "$(MLB_ROLLING_OBSERVATION_OUT_DIR)"
	$(VENV_PY) -c 'import csv,json; from pathlib import Path; d="$(MLB_ROLLING_OBSERVATION_EFFECTIVE_DATE)"; b=Path("$(MLB_ROLLING_OBSERVATION_OUT_DIR)"); csvs=["rolling_candidate_ledger_"+d+".csv","rolling_candidate_current_projection_"+d+".csv","rolling_candidate_growth_summary_"+d+".csv","rolling_candidate_delta_summary_"+d+".csv","rolling_candidate_pivot_source_"+d+".csv"]; [sum(1 for _ in csv.DictReader((b / p).open(newline="", encoding="utf-8"))) for p in csvs]; json.loads((b / ("rolling_candidate_ops_brief_input_"+d+".json")).read_text(encoding="utf-8")); print("rolling_observation_parse_check=pass")'
	$(MAKE) mlb-daily-ops-brief MLB_DAILY_BRIEF_REPORT_DATE="$(MLB_DAILY_BRIEF_REPORT_DATE)" MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE="$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)" MLB_DAILY_BRIEF_CURRENT_SLATE_DATE="$(MLB_ROLLING_OBSERVATION_EFFECTIVE_DATE)" MLB_DAILY_BRIEF_REFRESH_INPUTS="$(MLB_DAILY_ROLLING_OBS_REFRESH_BRIEF_INPUTS)" MLB_ENABLE_ROLLING_CANDIDATE_OBS=1 MLB_ROLLING_CANDIDATE_OBS_JSON="$(MLB_ROLLING_OBSERVATION_OUT_DIR)/rolling_candidate_ops_brief_input_$(MLB_ROLLING_OBSERVATION_EFFECTIVE_DATE).json" MLB_DAILY_BRIEF_OUT_MD="$(MLB_DAILY_BRIEF_OUT_MD)" MLB_DAILY_BRIEF_DATED_OUT_MD="$(MLB_DAILY_BRIEF_DATED_OUT_MD)" MLB_DAILY_BRIEF_OUT_JSON="$(MLB_DAILY_BRIEF_OUT_JSON)" MLB_DAILY_BRIEF_HISTORY_JSONL="$(MLB_DAILY_BRIEF_HISTORY_JSONL)"

.PHONY: mlb-rolling-observation-index
mlb-rolling-observation-index:
	$(VENV_PY) -m backend.mlb.scripts.build_mlb_rolling_observation_index --index-date "$(MLB_ROLLING_OBSERVATION_INDEX_DATE)" --root "$(MLB_ROLLING_OBSERVATION_ROOT)"
	$(VENV_PY) -c 'import csv; from pathlib import Path; d="$(MLB_ROLLING_OBSERVATION_INDEX_DATE)"; r=Path("$(MLB_ROLLING_OBSERVATION_ROOT)"); csv_path=r / ("rolling_observation_multi_day_index_"+d+".csv"); md_path=r / ("rolling_observation_multi_day_index_"+d+".md"); rows=sum(1 for _ in csv.DictReader(csv_path.open(newline="", encoding="utf-8"))); assert md_path.exists(), md_path; print(f"rolling_observation_index_parse_check=pass rows={rows}")'

.PHONY: mlb-compare-upload-variants-postgame
mlb-compare-upload-variants-postgame:
	$(VENV_PY) backend/mlb/scripts/compare_upload_variants_postgame.py --date "$(MLB_DATE)" --base-csv "$(MLB_UPLOAD_COMPARE_BASE_CSV)" --weighted-csv "$(MLB_UPLOAD_COMPARE_WEIGHTED_CSV)" --out-dir "$(MLB_UPLOAD_COMPARE_OUT_DIR)" --graded-rows-csv "$(MLB_UPLOAD_COMPARE_GRADED_ROWS_CSV)"

.PHONY: mlb-singles-shadow
mlb-singles-shadow:
	$(VENV_PY) backend/mlb/scripts/generate_singles_shadow_upload.py --date "$(MLB_DATE)" --base-csv "$(MLB_SINGLES_SHADOW_BASE_CSV)" --shadow-csv "$(MLB_SINGLES_SHADOW_OUT_CSV)" --out-dir "$(MLB_SINGLES_SHADOW_OUT_DIR)" --odds-snapshot-in "$(MLB_SINGLES_SHADOW_ODDS_SNAPSHOT)" --graded-rows-csv "$(MLB_SINGLES_SHADOW_GRADED_ROWS_CSV)" --threshold "$(MLB_SINGLES_SHADOW_THRESHOLD)" --top-n "$(MLB_SINGLES_SHADOW_TOP_N)" --max-rows-per-player "$(MLB_SINGLES_SHADOW_MAX_PER_PLAYER)" --max-abs-win-pct "$(MLB_SINGLES_SHADOW_MAX_ABS_WIN_PCT)" $(if $(strip $(MLB_SINGLES_SHADOW_MODEL_PATH)),--singles-model-path "$(MLB_SINGLES_SHADOW_MODEL_PATH)",)

.PHONY: mlb-total-bases-shadow-candidate
mlb-total-bases-shadow-candidate:
	$(VENV_PY) backend/mlb/scripts/run_total_bases_shadow_candidate.py --slate-date "$(MLB_TOTAL_BASES_SHADOW_DATE)" --training-dataset "$(MLB_TOTAL_BASES_SHADOW_TRAINING_DATASET)" --slate-output-csv "$(MLB_TOTAL_BASES_SHADOW_SLATE_OUTPUT_CSV)" --train-through "$(MLB_TOTAL_BASES_SHADOW_TRAIN_THROUGH)" --out-root "$(MLB_TOTAL_BASES_SHADOW_OUT_ROOT)" $(if $(strip $(MLB_TOTAL_BASES_SHADOW_OUTCOMES_CSV)),--outcomes-csv "$(MLB_TOTAL_BASES_SHADOW_OUTCOMES_CSV)",)

.PHONY: mlb-total-bases-shadow-evaluation
mlb-total-bases-shadow-evaluation:
	$(VENV_PY) backend/mlb/scripts/run_total_bases_shadow_evaluation_tracker.py --shadow-root "$(MLB_TOTAL_BASES_SHADOW_OUT_ROOT)" --reconcile-root "$(MLB_TOTAL_BASES_SHADOW_RECONCILE_ROOT)" --out-dir "$(MLB_TOTAL_BASES_SHADOW_EVAL_OUT_DIR)"

.PHONY: mlb-hits-o15-simple-filter
mlb-hits-o15-simple-filter:
	$(VENV_PY) backend/mlb/scripts/run_mlb_hits_o15_review_board.py --date "$(MLB_HITS_O15_SIMPLE_FILTER_DATE)" --slate-output-csv "$(MLB_SLATE_OUTPUT_CSV)" --hits-environment-json "$(MLB_HITS_ENV_OUT_JSON)" --hits-environment-history-jsonl "$(MLB_HITS_ENV_HISTORY_JSONL)" --hits-environment-snapshot-dir "$(MLB_HITS_ENV_SNAPSHOT_DIR)" --starter-required-min-starts "$(MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS)" --out-dir "$(MLB_HITS_O15_SIMPLE_FILTER_OUT_DIR)"

.PHONY: mlb-hits-o15-watch-candidates
mlb-hits-o15-watch-candidates:
	$(VENV_PY) backend/mlb/scripts/run_mlb_hits_o15_review_board.py --board watch_o15 --date "$(MLB_HITS_O15_WATCH_CANDIDATES_DATE)" --slate-output-csv "$(MLB_SLATE_OUTPUT_CSV)" --hits-environment-json "$(MLB_HITS_ENV_OUT_JSON)" --hits-environment-history-jsonl "$(MLB_HITS_ENV_HISTORY_JSONL)" --hits-environment-snapshot-dir "$(MLB_HITS_ENV_SNAPSHOT_DIR)" --starter-required-min-starts "$(MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS)" --out-dir "$(MLB_HITS_O15_WATCH_CANDIDATES_OUT_DIR)"

.PHONY: mlb-hits-o15-layered-candidates
mlb-hits-o15-layered-candidates:
	$(VENV_PY) backend/mlb/scripts/run_mlb_hits_o15_review_board.py --board layered_o15 --date "$(MLB_HITS_O15_LAYERED_CANDIDATES_DATE)" --slate-output-csv "$(MLB_SLATE_OUTPUT_CSV)" --hits-environment-json "$(MLB_HITS_ENV_OUT_JSON)" --hits-environment-history-jsonl "$(MLB_HITS_ENV_HISTORY_JSONL)" --hits-environment-snapshot-dir "$(MLB_HITS_ENV_SNAPSHOT_DIR)" --starter-required-min-starts "$(MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS)" --out-dir "$(MLB_HITS_O15_LAYERED_CANDIDATES_OUT_DIR)"

.PHONY: mlb-hits-o15-alternate-discovery
mlb-hits-o15-alternate-discovery:
	$(VENV_PY) backend/mlb/scripts/run_mlb_hits_o15_review_board.py --board alternate_o15 --date "$(MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE)" --slate-output-csv "$(MLB_SLATE_OUTPUT_CSV)" --hits-environment-json "$(MLB_HITS_ENV_OUT_JSON)" --hits-environment-history-jsonl "$(MLB_HITS_ENV_HISTORY_JSONL)" --hits-environment-snapshot-dir "$(MLB_HITS_ENV_SNAPSHOT_DIR)" --starter-required-min-starts "$(MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS)" --alternate-book-level-csv "$(MLB_HITS_O15_ALTERNATE_BOOK_LEVEL_CSV)" --out-dir "$(MLB_HITS_O15_ALTERNATE_DISCOVERY_OUT_DIR)"

.PHONY: mlb-oddsapi-batter-hits-alternate-live-discovery
mlb-oddsapi-batter-hits-alternate-live-discovery:
	$(VENV_PY) backend/mlb/scripts/run_oddsapi_batter_hits_alternate_live_discovery.py --date "$(MLB_ODDSAPI_BATTER_HITS_ALTERNATE_LIVE_DISCOVERY_DATE)" --out-root "$(MLB_ODDSAPI_BATTER_HITS_ALTERNATE_LIVE_DISCOVERY_OUT_ROOT)"

.PHONY: mlb-hits-o15-alternate-discovery-full
mlb-hits-o15-alternate-discovery-full:
	$(MAKE) mlb-oddsapi-batter-hits-alternate-live-discovery MLB_ODDSAPI_BATTER_HITS_ALTERNATE_LIVE_DISCOVERY_DATE="$(MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE)"
	$(MAKE) mlb-hits-o15-alternate-discovery MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE="$(MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE)" MLB_HITS_O15_ALTERNATE_BOOK_LEVEL_CSV="$(MLB_ODDSAPI_BATTER_HITS_ALTERNATE_LIVE_DISCOVERY_OUT_ROOT)/$(MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE)/live_alternate_book_level_rows.csv"

.PHONY: mlb-oddsapi-alternate-history-cost-estimate
mlb-oddsapi-alternate-history-cost-estimate:
	$(VENV_PY) backend/mlb/scripts/audit_mlb_oddsapi_alternate_history_cost.py --date-from "$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_FROM)" --date-to "$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_TO)" --snapshot-time-et "$(MLB_ODDSAPI_ALTERNATE_HISTORY_SNAPSHOT_TIME_ET)" --out-dir "$(MLB_ODDSAPI_ALTERNATE_HISTORY_OUT_DIR)" $(if $(filter 0,$(MLB_ODDSAPI_ALTERNATE_HISTORY_DRY_RUN)),--no-dry-run,--dry-run) $(if $(filter 1,$(MLB_ODDSAPI_ALTERNATE_HISTORY_RUN_PROBE)),--run-probe,) --max-oddsapi-calls "$(MAX_ODDSAPI_CALLS)" --max-oddsapi-credits "$(MAX_ODDSAPI_CREDITS)" --min-remaining-credits "$(MIN_ODDSAPI_REMAINING_CREDITS)" --require-confirm "$(REQUIRE_CONFIRM)" --confirm "$(MLB_ODDSAPI_ALTERNATE_HISTORY_CONFIRM)"

.PHONY: mlb-oddsapi-alternate-history-backfill
mlb-oddsapi-alternate-history-backfill:
	$(VENV_PY) backend/mlb/scripts/backfill_mlb_oddsapi_alternate_history.py --date-from "$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_FROM)" --date-to "$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_TO)" --snapshot-time-et "$(MLB_ODDSAPI_ALTERNATE_HISTORY_SNAPSHOT_TIME_ET)" --out-root "$(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)" $(if $(filter 0,$(MLB_ODDSAPI_ALTERNATE_HISTORY_DRY_RUN)),--no-dry-run,--dry-run) --max-oddsapi-calls "$(MAX_ODDSAPI_CALLS)" --max-oddsapi-credits "$(MAX_ODDSAPI_CREDITS)" --min-remaining-credits "$(MIN_ODDSAPI_REMAINING_CREDITS)" --require-confirm "$(REQUIRE_CONFIRM)" --confirm "$(MLB_ODDSAPI_ALTERNATE_HISTORY_CONFIRM)"

.PHONY: mlb-hits-o15-alternate-discovery-from-history
mlb-hits-o15-alternate-discovery-from-history:
	$(VENV_PY) backend/mlb/scripts/run_mlb_hits_o15_review_board.py --board alternate_o15 --date "$(MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE)" --slate-output-csv "$(MLB_HITS_O15_ALTERNATE_HISTORY_SLATE_OUTPUT_CSV)" --hits-environment-json "$(MLB_HITS_ENV_OUT_JSON)" --hits-environment-history-jsonl "$(MLB_HITS_ENV_HISTORY_JSONL)" --hits-environment-snapshot-dir "$(MLB_HITS_ENV_SNAPSHOT_DIR)" --starter-required-min-starts "$(MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS)" --alternate-book-level-csv "$(MLB_HITS_O15_ALTERNATE_HISTORY_BOOK_LEVEL_CSV)" --out-dir "$(MLB_HITS_O15_ALTERNATE_DISCOVERY_OUT_DIR)"

.PHONY: mlb-hits-o15-alternate-discovery-from-history-range
mlb-hits-o15-alternate-discovery-from-history-range:
	$(VENV_PY) backend/mlb/scripts/build_mlb_o15_alternate_discovery_from_history_range.py --date-from "$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_FROM)" --date-to "$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_TO)" --backfill-root "$(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)" --summary-csv "$(MLB_HITS_O15_ALTERNATE_HISTORY_BUILD_SUMMARY_CSV)" --report-md "$(MLB_HITS_O15_ALTERNATE_HISTORY_RECHECK_MD)"

.PHONY: mlb-o15-alternate-history-backfill-and-build
mlb-o15-alternate-history-backfill-and-build:
	$(MAKE) mlb-oddsapi-alternate-history-backfill DATE_FROM="$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_FROM)" DATE_TO="$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_TO)" MLB_ODDSAPI_ALTERNATE_HISTORY_DRY_RUN="$(MLB_ODDSAPI_ALTERNATE_HISTORY_DRY_RUN)" MLB_ODDSAPI_ALTERNATE_HISTORY_CONFIRM="$(MLB_ODDSAPI_ALTERNATE_HISTORY_CONFIRM)" MAX_ODDSAPI_CALLS="$(MAX_ODDSAPI_CALLS)" MAX_ODDSAPI_CREDITS="$(MAX_ODDSAPI_CREDITS)" MIN_ODDSAPI_REMAINING_CREDITS="$(MIN_ODDSAPI_REMAINING_CREDITS)"
	$(MAKE) mlb-hits-o15-alternate-discovery-from-history-range DATE_FROM="$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_FROM)" DATE_TO="$(MLB_ODDSAPI_ALTERNATE_HISTORY_DATE_TO)"

.PHONY: mlb-hits-u15-favorite-audit
mlb-hits-u15-favorite-audit:
	$(VENV_PY) backend/mlb/scripts/run_mlb_hits_o15_review_board.py --board u15 --date "$(MLB_HITS_U15_FAVORITE_AUDIT_DATE)" --slate-output-csv "$(MLB_SLATE_OUTPUT_CSV)" --hits-environment-json "$(MLB_HITS_ENV_OUT_JSON)" --hits-environment-history-jsonl "$(MLB_HITS_ENV_HISTORY_JSONL)" --hits-environment-snapshot-dir "$(MLB_HITS_ENV_SNAPSHOT_DIR)" --starter-required-min-starts "$(MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS)" --out-dir "$(MLB_HITS_U15_FAVORITE_AUDIT_OUT_DIR)"

.PHONY: mlb-daily-review-boards mlb-train-probability-calibration mlb-check-finalized-training-data mlb-ensure-finalized-training-data mlb-execution-vs-model mlb-full-slate-performance mlb-daily-reconcile mlb-daily-upload-prep mlb-current-upload-prep mlb-daily-review-and-upload mlb-refresh-v2-environment-interactions mlb-refresh-v2-qc-candidate-watch mlb-refresh-overlap-role-profile-watch mlb-refresh-user-over-15-filter-watch mlb-capture-overlap-snapshot mlb-v2-candidate-registry mlb-overlap-monitor mlb-review-aid-performance mlb-expanded-o15-universe mlb-expanded-o15-context-health mlb-expanded-o15-universe-slice-analysis mlb-expanded-o15-universe-betonline-audit mlb-expanded-o15-hidden-matchup-support-audit mlb-expanded-o15-agreement-score-audit mlb-expanded-o15-variable-importance-survey mlb-expanded-o15-feature-centrality-audit mlb-time-of-day-bucket-audit mlb-expanded-o15-late-game-proxy-audit mlb-expanded-o15-low-attention-signpost-audit mlb-research-snapshot mlb-weekly-research-snapshot mlb-identity-health mlb-o15-ontology-health ontology-health mlb-hits15-environment-lineage-health mlb-hits15-environment-v2-alpha-dashboard mlb-environment-v2-beta-daily mlb-environment-v2-beta-reconcile mlb-rehydrate-reconcile-rolling-context
mlb-daily-review-boards:
	$(MAKE) mlb-hits-o15-simple-filter MLB_HITS_O15_SIMPLE_FILTER_DATE="$(MLB_DAILY_REVIEW_BOARDS_DATE)"
	$(MAKE) mlb-hits-o15-watch-candidates MLB_HITS_O15_WATCH_CANDIDATES_DATE="$(MLB_DAILY_REVIEW_BOARDS_DATE)"
	$(MAKE) mlb-hits-o15-layered-candidates MLB_HITS_O15_LAYERED_CANDIDATES_DATE="$(MLB_DAILY_REVIEW_BOARDS_DATE)"
	-$(MAKE) mlb-hits-o15-alternate-discovery-full MLB_HITS_O15_ALTERNATE_DISCOVERY_DATE="$(MLB_DAILY_REVIEW_BOARDS_DATE)"
	$(MAKE) mlb-hits-u15-favorite-audit MLB_HITS_U15_FAVORITE_AUDIT_DATE="$(MLB_DAILY_REVIEW_BOARDS_DATE)"

mlb-train-probability-calibration:
	$(VENV_PY) backend/mlb/scripts/train_mlb_probability_calibration.py --rows-csv "$(MLB_CALIBRATION_TRAIN_CSV)" --out-json "$(MLB_PROBABILITY_CALIBRATION_JSON)" --comparison-csv "$(MLB_CALIBRATION_COMPARISON_CSV)" --curve-csv "$(MLB_CALIBRATION_CURVE_CSV)" --prop-types "$(MLB_CALIBRATION_PROP_TYPES)" --min-prop-samples "$(MLB_CALIBRATION_MIN_PROP_SAMPLES)" --training-scope "$(MLB_CALIBRATION_TRAINING_SCOPE)" $(if $(strip $(MLB_CALIBRATION_FROM_DATE)),--from-date "$(MLB_CALIBRATION_FROM_DATE)",) $(if $(strip $(MLB_CALIBRATION_TO_DATE)),--to-date "$(MLB_CALIBRATION_TO_DATE)",)

.PHONY: mlb-prop-regime-validation mlb-model-performance-by-prop mlb-reporting-alignment-audit
mlb-prop-regime-validation:
	$(VENV_PY) backend/mlb/scripts/build_prop_regime_validation.py $(foreach csv,$(MLB_PROP_REGIME_RECONCILE_CSVS),--reconcile-csv "$(csv)") --execution-csv "$(MLB_PROP_REGIME_EXECUTION_CSV)" --out-dir "$(MLB_PROP_REGIME_OUT_DIR)" --deploy-csv "$(MLB_PROP_REGIME_DEPLOY_CSV)"

mlb-model-performance-by-prop:
	$(VENV_PY) backend/mlb/scripts/report_mlb_model_performance_by_prop_daily.py --from-date "$(MLB_MODEL_PERFORMANCE_FROM_DATE)" --to-date "$(MLB_MODEL_PERFORMANCE_TO_DATE)" --out-csv "$(MLB_MODEL_PERFORMANCE_DAILY_CSV)" --summary-csv "$(MLB_MODEL_PERFORMANCE_SUMMARY_CSV)" --active-props-csv "$(MLB_PROP_REGIME_DEPLOY_CSV)" --source-type "$(MLB_MODEL_PERFORMANCE_SOURCE_TYPE)"

mlb-reporting-alignment-audit:
	$(VENV_PY) backend/mlb/scripts/audit_mlb_reporting_alignment.py --date "$(MLB_REPORTING_ALIGNMENT_DATE)" --out-csv "$(MLB_REPORTING_ALIGNMENT_OUT_CSV)" --out-md "$(MLB_REPORTING_ALIGNMENT_OUT_MD)"

mlb-check-finalized-training-data:
	$(VENV_PY) backend/mlb/scripts/check_mlb_finalized_training_data.py --date "$(MLB_DATE)" $(if $(filter 1 true TRUE yes YES,$(MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS)),--check-player-stats,)

mlb-ensure-finalized-training-data:
	@echo "Ensuring finalized MLB stat/training data for $(MLB_DATE)"
	@if $(MAKE) --no-print-directory mlb-check-finalized-training-data MLB_DATE="$(MLB_DATE)" MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS="$(MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS)"; then \
		echo "Finalized MLB data already present for $(MLB_DATE)"; \
	else \
		echo "Finalized MLB data missing for $(MLB_DATE); running exact-date stat-derived ingestion"; \
		$(MAKE) mlb-stat-derived-backfill MLB_STAT_FROM_DATE="$(MLB_DATE)" MLB_STAT_TO_DATE="$(MLB_DATE)" MLB_STAT_SKIP_EXISTING_DATES=0 MLB_STAT_DERIVED_DAYS=2 MLB_STAT_DERIVED_MIN=1 MLB_SEASON_REQUIRE_REGULAR=1; \
		$(MAKE) --no-print-directory mlb-check-finalized-training-data MLB_DATE="$(MLB_DATE)" MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS="$(MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS)"; \
	fi

mlb-execution-vs-model:
	@if [ -z "$(strip $(MLB_EXEC_TOOL_RESULTS_CSV))" ]; then \
		echo "mlb-execution-vs-model requires MLB_EXEC_TOOL_RESULTS_CSV=<daily_tool_results.csv>"; \
		exit 2; \
	fi
	$(MAKE) mlb-check-finalized-training-data MLB_DATE="$(MLB_DATE)" MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS="$(MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS)"
	$(MAKE) mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="$(MLB_DATE)" MLB_RECONCILE_TO_DATE="$(MLB_DATE)" MLB_RECONCILE_BOOKMAKER="$(MLB_EXEC_RECONCILE_BOOKMAKER)" MLB_RECONCILE_SLATE_FILENAME_MODE="$(MLB_EXEC_RECONCILE_SLATE_FILENAME_MODE)" MLB_RECONCILE_SLATE_FILENAME_GLOB="$(MLB_EXEC_RECONCILE_SLATE_FILENAME_GLOB)" MLB_RECONCILE_SNAPSHOT_POLICY="$(MLB_EXEC_RECONCILE_SNAPSHOT_POLICY)" MLB_RECONCILE_ODDS_FILENAME="$(MLB_RECONCILE_ODDS_FILENAME)" MLB_RECONCILE_ODDS_FILENAME_MODE="$(MLB_EXEC_RECONCILE_ODDS_FILENAME_MODE)" MLB_RECONCILE_ODDS_FILENAME_GLOB="$(MLB_EXEC_RECONCILE_ODDS_FILENAME_GLOB)" MLB_RECONCILE_ROWS_OUT_CSV="$(MLB_EXEC_RECONCILE_CSV)" MLB_RECONCILE_SUMMARY_OUT_JSON="$(MLB_EXEC_RECONCILE_SUMMARY_JSON)" MLB_RECONCILE_REQUIRE_TWO_SIDED="$(MLB_EXEC_RECONCILE_REQUIRE_TWO_SIDED)" MLB_RECONCILE_REQUIRE_OUTCOMES="$(MLB_EXEC_RECONCILE_REQUIRE_OUTCOMES)" MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN="$(MLB_EXEC_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN)"
	$(VENV_PY) backend/mlb/scripts/compare_execution_vs_model.py --date "$(MLB_DATE)" --tool-results-csv "$(MLB_EXEC_TOOL_RESULTS_CSV)" --reconcile-csv "$(MLB_EXEC_RECONCILE_CSV)" --out-csv "$(MLB_EXEC_OUT_CSV)" --out-json "$(MLB_EXEC_OUT_JSON)" --out-md "$(MLB_EXEC_OUT_MD)" $(if $(wildcard $(MLB_PROBABILITY_CALIBRATION_JSON)),--calibration-json "$(MLB_PROBABILITY_CALIBRATION_JSON)",) $(if $(strip $(MLB_EXEC_EXPECTED_RAW_TOOL_ROWS)),--expected-raw-tool-rows "$(MLB_EXEC_EXPECTED_RAW_TOOL_ROWS)",) $(if $(strip $(MLB_EXEC_EXPECTED_MLB_BETONLINE_ROWS)),--expected-mlb-betonline-rows "$(MLB_EXEC_EXPECTED_MLB_BETONLINE_ROWS)",) $(if $(strip $(MLB_EXEC_EXPECTED_MLB_BETONLINE_NON_PUSH_ROWS)),--expected-mlb-betonline-non-push-rows "$(MLB_EXEC_EXPECTED_MLB_BETONLINE_NON_PUSH_ROWS)",)

mlb-full-slate-performance:
	$(MAKE) mlb-ensure-finalized-training-data MLB_DATE="$(MLB_DATE)" MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS="$(MLB_RECONCILE_FINALIZED_CHECK_PLAYER_STATS)"
	$(MAKE) mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="$(MLB_DATE)" MLB_RECONCILE_TO_DATE="$(MLB_DATE)" MLB_RECONCILE_BOOKMAKER="" MLB_RECONCILE_SLATE_FILENAME="$(MLB_RECONCILE_SLATE_FILENAME)" MLB_RECONCILE_SLATE_FILENAME_MODE="all" MLB_RECONCILE_SLATE_FILENAME_GLOB="$(MLB_RECONCILE_SLATE_FILENAME_GLOB)" MLB_RECONCILE_SNAPSHOT_POLICY="$(MLB_FULL_SLATE_SNAPSHOT_POLICY)" MLB_RECONCILE_SNAPSHOT_RUN_TAG="$(MLB_FULL_SLATE_SNAPSHOT_RUN_TAG)" MLB_RECONCILE_ODDS_FILENAME="$(MLB_RECONCILE_ODDS_FILENAME)" MLB_RECONCILE_ODDS_FILENAME_MODE="all" MLB_RECONCILE_ODDS_FILENAME_GLOB="$(MLB_RECONCILE_ODDS_FILENAME_GLOB)" MLB_RECONCILE_ROWS_OUT_CSV="$(MLB_FULL_SLATE_RECONCILE_CSV)" MLB_RECONCILE_SUMMARY_OUT_JSON="$(MLB_FULL_SLATE_RECONCILE_SUMMARY_JSON)" MLB_RECONCILE_REQUIRE_TWO_SIDED="$(MLB_RECONCILE_REQUIRE_TWO_SIDED)" MLB_RECONCILE_REQUIRE_OUTCOMES=1 MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN=1
	$(VENV_PY) backend/mlb/scripts/report_mlb_full_slate_performance.py --rows-csv "$(MLB_FULL_SLATE_RECONCILE_CSV)" --out-md "$(MLB_FULL_SLATE_SUMMARY_MD)" --out-by-prop-csv "$(MLB_FULL_SLATE_BY_PROP_CSV)" --min-resolved-rows "$(MLB_FULL_SLATE_MIN_RESOLVED_ROWS)"

mlb-refresh-v2-environment-interactions:
	@echo "Refreshing v2 environment interactions"
	$(VENV_PY) backend/mlb/scripts/analyze_mlb_v2_environment_interactions.py
	@echo "Refreshing v2 favorites environment breakdown"
	$(VENV_PY) backend/mlb/scripts/analyze_mlb_v2_favorites_environment_breakdown.py

mlb-refresh-v2-qc-candidate-watch:
	@echo "Refreshing v2 / Quick Card candidate watch"
	$(VENV_PY) backend/mlb/scripts/build_mlb_v2_qc_candidate_watch.py

mlb-refresh-overlap-role-profile-watch:
	@echo "Refreshing overlap role profile watch"
	$(VENV_PY) backend/mlb/scripts/build_mlb_overlap_role_profile_watch.py

mlb-refresh-user-over-15-filter-watch:
	@echo "Refreshing retired research-only user over 1.5 proxy watch"
	$(VENV_PY) tmp/analysis/run_mlb_user_over_15_filter_watch.py --out-json "$(MLB_DAILY_BRIEF_USER_OVER_15_WATCH_JSON)" --out-csv "$(MLB_USER_OVER_15_FILTER_WATCH_CSV)" --out-md "$(MLB_USER_OVER_15_FILTER_WATCH_MD)"

.PHONY: mlb-refresh-hits-15-tier-backtest
mlb-refresh-hits-15-tier-backtest:
	@echo "Refreshing hits 1.5 tier backtest review aid"
	$(VENV_PY) backend/mlb/scripts/run_mlb_hits_15_tier_backtest.py --out-dir "$(MLB_HITS_15_TIER_BACKTEST_OUT_DIR)"

mlb-o15-manual-unified-board-universe:
	@echo "Building manual o1.5 unified board universe"
	$(VENV_PY) backend/mlb/scripts/build_mlb_o15_manual_unified_board_universe.py --out-dir "$(MLB_REVIEW_AID_PERFORMANCE_OUT_DIR)"

mlb-expanded-o15-universe:
	@echo "Building expanded o1.5 universe"
	$(VENV_PY) backend/mlb/scripts/expanded_o15_universe_builder.py $(if $(strip $(MLB_EXPANDED_O15_UNIVERSE_DATE)),--date "$(MLB_EXPANDED_O15_UNIVERSE_DATE)",) $(if $(strip $(MLB_EXPANDED_O15_UNIVERSE_DATE_FROM)),--date-from "$(MLB_EXPANDED_O15_UNIVERSE_DATE_FROM)",) $(if $(strip $(MLB_EXPANDED_O15_UNIVERSE_DATE_TO)),--date-to "$(MLB_EXPANDED_O15_UNIVERSE_DATE_TO)",) --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)"
	$(VENV_PY) backend/mlb/scripts/hydrate_expanded_o15_context.py --rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)" --alternate-backfill-root "$(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)"

mlb-expanded-o15-context-health:
	@echo "Checking expanded o1.5 context health for $(MLB_EXPANDED_O15_CONTEXT_HEALTH_DATE)"
	$(VENV_PY) backend/mlb/scripts/check_expanded_o15_context_health.py --date "$(MLB_EXPANDED_O15_CONTEXT_HEALTH_DATE)" --rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)"

mlb-hits15-environment-lineage-health:
	@echo "Checking hits 1.5 environment v1.1 lineage retention for $(MLB_HITS15_ENV_LINEAGE_DATE)"
	$(VENV_PY) backend/mlb/scripts/check_mlb_hits15_environment_lineage_health.py --date "$(MLB_HITS15_ENV_LINEAGE_DATE)" --out-dir "$(MLB_HITS15_ENV_LINEAGE_OUT_DIR)"

mlb-hits15-environment-v2-alpha-dashboard:
	@echo "Building hits 1.5 Environment v2-alpha component dashboard"
	$(VENV_PY) backend/mlb/scripts/build_mlb_hits15_environment_v2_alpha_dashboard.py --out-dir "$(MLB_HITS15_ENV_V2_ALPHA_OUT_DIR)"

mlb-environment-v2-beta-daily:
	@echo "Capturing Environment v2-beta daily research profiles for $(MLB_ENVIRONMENT_V2_BETA_DAILY_DATE)"
	$(VENV_PY) backend/mlb/scripts/build_mlb_environment_v2_beta_daily_lane.py --date "$(MLB_ENVIRONMENT_V2_BETA_DAILY_DATE)" --out-root "$(MLB_ENVIRONMENT_V2_BETA_DAILY_OUT_ROOT)" --wrapper-mode "$(MLB_ENVIRONMENT_V2_BETA_DAILY_WRAPPER_MODE)"

mlb-environment-v2-beta-reconcile:
	@echo "Reconciling Environment v2-beta daily research profiles for $(MLB_ENVIRONMENT_V2_BETA_RECONCILE_DATE)"
	$(VENV_PY) backend/mlb/scripts/reconcile_mlb_environment_v2_beta_daily.py --date "$(MLB_ENVIRONMENT_V2_BETA_RECONCILE_DATE)" --daily-root "$(MLB_ENVIRONMENT_V2_BETA_DAILY_OUT_ROOT)" --reconcile-root "$(MLB_ENVIRONMENT_V2_BETA_RECONCILE_ROOT)" --ledger-csv "$(MLB_ENVIRONMENT_V2_BETA_LEDGER_CSV)"

mlb-expanded-o15-universe-slice-analysis:
	@echo "Analyzing expanded o1.5 universe slices"
	$(VENV_PY) backend/mlb/scripts/analyze_expanded_o15_universe_slices.py --rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)" --backfill-root "$(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)"

mlb-expanded-o15-universe-betonline-audit:
	@echo "Auditing expanded o1.5 universe BetOnline pricing"
	$(VENV_PY) backend/mlb/scripts/audit_expanded_o15_betonline.py --rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)" --backfill-root "$(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)"

mlb-expanded-o15-hidden-matchup-support-audit:
	@echo "Auditing expanded o1.5 hidden matchup support"
	$(VENV_PY) backend/mlb/scripts/audit_expanded_o15_hidden_matchup_support.py --rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)" --backfill-root "$(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)"

mlb-expanded-o15-agreement-score-audit:
	@echo "Auditing expanded o1.5 agreement score"
	$(VENV_PY) backend/mlb/scripts/audit_expanded_o15_agreement_score.py --rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)" --backfill-root "$(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)"

mlb-expanded-o15-variable-importance-survey:
	@echo "Surveying expanded o1.5 variable importance"
	$(VENV_PY) backend/mlb/scripts/audit_expanded_o15_variable_importance.py --rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)" --backfill-root "$(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)"

mlb-expanded-o15-feature-centrality-audit:
	@echo "Auditing expanded o1.5 feature centrality"
	$(VENV_PY) backend/mlb/scripts/audit_expanded_o15_feature_centrality.py --interactions-csv "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)/expanded_o15_pairwise_interactions.csv" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)"

mlb-time-of-day-bucket-audit:
	@echo "Auditing MLB time-of-day bucket definitions"
	$(VENV_PY) backend/mlb/scripts/audit_mlb_time_of_day_buckets.py --expanded-rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)"

mlb-expanded-o15-late-game-proxy-audit:
	@echo "Auditing expanded o1.5 late-game proxy effects"
	$(VENV_PY) backend/mlb/scripts/audit_expanded_o15_late_game_proxy.py --rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)"

mlb-expanded-o15-low-attention-signpost-audit:
	@echo "Auditing expanded o1.5 low-attention +200s signpost"
	$(VENV_PY) backend/mlb/scripts/audit_expanded_o15_low_attention_signpost.py --rows-csv "$(MLB_EXPANDED_O15_UNIVERSE_ROWS_CSV)" --out-dir "$(MLB_EXPANDED_O15_UNIVERSE_OUT_DIR)" --backfill-root "$(MLB_ODDSAPI_ALTERNATE_HISTORY_BACKFILL_OUT_ROOT)"

mlb-research-snapshot:
	@echo "Building immutable MLB weekly research snapshot for $(MLB_RESEARCH_SNAPSHOT_DATE)"
	$(VENV_PY) backend/mlb/scripts/build_mlb_research_snapshot.py --date "$(MLB_RESEARCH_SNAPSHOT_DATE)" --out-root "$(MLB_RESEARCH_SNAPSHOT_OUT_ROOT)"

mlb-weekly-research-snapshot:
	$(MAKE) mlb-daily-reconcile MLB_DAILY_RECONCILE_DATE="$(MLB_DAILY_RECONCILE_DATE)" MLB_REBUILD_UPLOADS=0
	$(MAKE) mlb-expanded-o15-universe MLB_EXPANDED_O15_UNIVERSE_DATE="" MLB_EXPANDED_O15_UNIVERSE_DATE_FROM="" MLB_EXPANDED_O15_UNIVERSE_DATE_TO=""
	$(MAKE) mlb-expanded-o15-variable-importance-survey
	$(MAKE) mlb-expanded-o15-feature-centrality-audit
	$(MAKE) mlb-research-snapshot MLB_RESEARCH_SNAPSHOT_DATE="$(MLB_RESEARCH_SNAPSHOT_DATE)"
	$(MAKE) mlb-daily-index MLB_DAILY_INDEX_DATE="$(MLB_UPLOAD_PREP_DATE)" MLB_DAILY_INDEX_COMPLETED_SLATE_DATE="$(MLB_DAILY_RECONCILE_DATE)"

mlb-identity-health:
	@echo "Running MLB canonical identity health diagnostics"
	$(VENV_PY) backend/mlb/scripts/run_mlb_identity_health.py

mlb-o15-ontology-health:
	@echo "Checking O1.5 ontology metadata health for $(MLB_O15_ONTOLOGY_HEALTH_DATE)"
	$(VENV_PY) backend/mlb/scripts/check_mlb_o15_ontology_health.py --date "$(MLB_O15_ONTOLOGY_HEALTH_DATE)" --out-dir "$(MLB_O15_ONTOLOGY_HEALTH_OUT_DIR)"

ontology-health: mlb-o15-ontology-health

mlb-project-invariants:
	@echo "Auditing MLB project invariants for $(MLB_PROJECT_INVARIANTS_DATE)"
	$(VENV_PY) backend/mlb/scripts/audit_mlb_project_invariants.py --date "$(MLB_PROJECT_INVARIANTS_DATE)" --out-dir "$(MLB_PROJECT_INVARIANTS_OUT_DIR)"

mlb-invariant-backlog:
	@echo "Summarizing MLB invariant backlog for $(MLB_INVARIANT_BACKLOG_DATE)"
	$(VENV_PY) backend/mlb/scripts/audit_mlb_invariant_backlog.py --date "$(MLB_INVARIANT_BACKLOG_DATE)" --out-dir "$(MLB_INVARIANT_BACKLOG_OUT_DIR)"

mlb-review-aid-performance:
	@echo "Refreshing review-aid performance tracker"
	$(VENV_PY) backend/mlb/scripts/run_mlb_review_aid_performance_tracker.py --completed-slate-date "$(MLB_DAILY_RECONCILE_DATE)" --out-dir "$(MLB_REVIEW_AID_PERFORMANCE_OUT_DIR)"

mlb-rehydrate-reconcile-rolling-context:
	@echo "Rehydrating execution reconcile rolling context from $(MLB_REHYDRATE_RECONCILE_ROLLING_DATE_FROM) to $(MLB_REHYDRATE_RECONCILE_ROLLING_DATE_TO) (DRY_RUN=$(MLB_REHYDRATE_RECONCILE_ROLLING_DRY_RUN))"
	$(VENV_PY) backend/mlb/scripts/rehydrate_mlb_reconcile_rolling_context.py --start-date "$(MLB_REHYDRATE_RECONCILE_ROLLING_DATE_FROM)" --end-date "$(MLB_REHYDRATE_RECONCILE_ROLLING_DATE_TO)" --out-dir "$(MLB_REHYDRATE_RECONCILE_ROLLING_OUT_DIR)" $(if $(filter 0 false FALSE no NO,$(MLB_REHYDRATE_RECONCILE_ROLLING_DRY_RUN)),--write,)

mlb-capture-overlap-snapshot:
	@echo "Capturing overlap daily snapshot for $(MLB_OVERLAP_SNAPSHOT_DATE)"
	$(VENV_PY) tmp/analysis/capture_overlap_daily_snapshot.py --date "$(MLB_OVERLAP_SNAPSHOT_DATE)" --out-csv "$(MLB_OVERLAP_SNAPSHOT_CSV)"

mlb-v2-candidate-registry:
	@echo "Building MLB V2/QC daily candidate registry for $(MLB_V2_REGISTRY_DATE)"
	$(VENV_PY) tmp/analysis/build_mlb_v2_daily_candidate_registry.py --date "$(MLB_V2_REGISTRY_DATE)" --out-csv "$(MLB_V2_REGISTRY_CSV)"

mlb-overlap-monitor:
	@echo "Refreshing overlap feature monitor"
	$(VENV_PY) tmp/analysis/run_overlap_feature_monitor.py
	@echo "Refreshing overlap forward tracker"
	$(VENV_PY) tmp/analysis/run_overlap_forward_tracker.py
	$(VENV_PY) tmp/analysis/print_overlap_ops_block.py --date "$(MLB_OVERLAP_OPS_DATE)"
	@echo "Daily review: check drift flags, 7-day/14-day WR and ROI, and sample collapse"
	@echo "Weekly review: compare overlap vs ranking-only vs QC-only, loser market spread, and low-opportunity separation"
	@echo "Decision annotations: 7-day deterioration=monitor; 7-day+14-day deterioration=investigate; 30-day deterioration or sample collapse=pause operational expansion"

mlb-daily-reconcile:
	@echo "Building full-slate reconcile for $(MLB_DAILY_RECONCILE_DATE)"
	$(MAKE) mlb-full-slate-performance MLB_DATE="$(MLB_DAILY_RECONCILE_DATE)" MLB_FULL_SLATE_MIN_RESOLVED_ROWS=1
	@echo "Running lane selector reconcile for $(MLB_DAILY_RECONCILE_DATE)"
	MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH=0 $(VENV_PY) -m backend.mlb.scripts.run_mlb_hits_lane_selector_report --date "$(MLB_DAILY_RECONCILE_DATE)" $(if $(filter 1 true TRUE yes YES,$(MLB_REBUILD_UPLOADS)),,--skip-upload-prep)
	$(MAKE) mlb-capture-overlap-snapshot MLB_OVERLAP_SNAPSHOT_DATE="$(MLB_DAILY_RECONCILE_DATE)"
	@echo "Running actual wagers by source reconcile for $(MLB_DAILY_RECONCILE_DATE)"
	$(VENV_PY) -m backend.mlb.scripts.compare_mlb_model_v2_upload_vs_actual --date "$(MLB_DAILY_RECONCILE_DATE)"
	$(MAKE) mlb-v2-candidate-registry MLB_V2_REGISTRY_DATE="$(MLB_DAILY_RECONCILE_DATE)"
	$(MAKE) mlb-refresh-v2-environment-interactions
	$(MAKE) mlb-refresh-v2-qc-candidate-watch
	$(MAKE) mlb-refresh-overlap-role-profile-watch
	$(MAKE) mlb-refresh-user-over-15-filter-watch
	$(MAKE) mlb-review-aid-performance MLB_DAILY_RECONCILE_DATE="$(MLB_DAILY_RECONCILE_DATE)"
	$(MAKE) mlb-capture-overlap-snapshot MLB_OVERLAP_SNAPSHOT_DATE="$(MLB_DAILY_RECONCILE_DATE)"
	$(MAKE) mlb-overlap-monitor MLB_OVERLAP_OPS_DATE="$(MLB_DAILY_RECONCILE_DATE)"
	-$(MAKE) mlb-finalize-frozen-tb15-populations

mlb-daily-upload-prep:
	@echo "Building current upload prep for $(MLB_UPLOAD_PREP_DATE)"
	@echo "public_catalog_fetch_allowed=$(MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH) cache_only_mode=$(if $(filter 1 true TRUE yes YES,$(MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH)),false,true)"
	MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH="$(MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH)" $(VENV_PY) -m backend.mlb.scripts.run_mlb_hits_lane_selector_report --date "$(MLB_UPLOAD_PREP_DATE)"
	$(MAKE) mlb-daily-review-boards MLB_DAILY_REVIEW_BOARDS_DATE="$(MLB_UPLOAD_PREP_DATE)"

mlb-current-upload-prep: mlb-daily-upload-prep

mlb-daily-review-and-upload:
	$(MAKE) mlb-daily-reconcile MLB_DAILY_RECONCILE_DATE="$(MLB_DAILY_RECONCILE_DATE)" MLB_REBUILD_UPLOADS=0
	$(MAKE) mlb-daily-upload-prep MLB_UPLOAD_PREP_DATE="$(MLB_UPLOAD_PREP_DATE)" MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH="$(MLB_ALLOW_8RAIN_PUBLIC_CATALOG_FETCH)"
	$(MAKE) mlb-expanded-o15-universe
	$(MAKE) mlb-expanded-o15-context-health MLB_EXPANDED_O15_CONTEXT_HEALTH_DATE="$(MLB_UPLOAD_PREP_DATE)"
	$(MAKE) mlb-expanded-o15-variable-importance-survey
	$(MAKE) mlb-identity-health
	$(MAKE) mlb-o15-ontology-health MLB_O15_ONTOLOGY_HEALTH_DATE="$(MLB_UPLOAD_PREP_DATE)"
	$(MAKE) mlb-project-invariants MLB_PROJECT_INVARIANTS_DATE="$(MLB_UPLOAD_PREP_DATE)"
	$(MAKE) mlb-invariant-backlog MLB_INVARIANT_BACKLOG_DATE="$(MLB_UPLOAD_PREP_DATE)"
	$(MAKE) mlb-daily-preflight MLB_DAILY_PREFLIGHT_DATE="$(MLB_UPLOAD_PREP_DATE)"
	$(MAKE) mlb-daily-ops-brief MLB_DAILY_BRIEF_REPORT_DATE="$(MLB_DAILY_BRIEF_REPORT_DATE)" MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE="$(MLB_DAILY_RECONCILE_DATE)" MLB_DAILY_BRIEF_CURRENT_SLATE_DATE="$(MLB_UPLOAD_PREP_DATE)"
	$(MAKE) mlb-daily-index MLB_DAILY_INDEX_DATE="$(MLB_UPLOAD_PREP_DATE)" MLB_DAILY_INDEX_COMPLETED_SLATE_DATE="$(MLB_DAILY_RECONCILE_DATE)"

.PHONY: mlb-o15-prospective-grade
mlb-o15-prospective-grade:
	@if [ "$(MLB_ENABLE_O15_PROSPECTIVE_GRADER)" != "1" ]; then \
		echo "mlb-o15-prospective-grade: disabled MLB_ENABLE_O15_PROSPECTIVE_GRADER=$(MLB_ENABLE_O15_PROSPECTIVE_GRADER)"; \
	elif [ "$(MLB_O15_PROSPECTIVE_GRADER_DATE)" != "$(MLB_O15_PROSPECTIVE_GRADER_RUN_DATE)" ]; then \
		echo "mlb-o15-prospective-grade: no frozen O1.5 prospective run for date=$(MLB_O15_PROSPECTIVE_GRADER_DATE); expected_run_date=$(MLB_O15_PROSPECTIVE_GRADER_RUN_DATE); skipping"; \
	elif [ ! -s "$(MLB_O15_PROSPECTIVE_GRADER_RECONCILE_CSV)" ]; then \
		echo "mlb-o15-prospective-grade: WARN missing or empty reconcile source $(MLB_O15_PROSPECTIVE_GRADER_RECONCILE_CSV); skipping"; \
	else \
		echo "mlb-o15-prospective-grade: START date=$(MLB_O15_PROSPECTIVE_GRADER_DATE) reconcile=$(MLB_O15_PROSPECTIVE_GRADER_RECONCILE_CSV)"; \
		$(VENV_PY) -m backend.mlb.scripts.run_mlb_o15_market_anchored_ranking_prospective_grader --mode dry_run --run-date "$(MLB_O15_PROSPECTIVE_GRADER_DATE)" --output-dir "$(MLB_O15_PROSPECTIVE_GRADER_OUT_DIR)"; \
		echo "mlb-o15-prospective-grade: DONE date=$(MLB_O15_PROSPECTIVE_GRADER_DATE)"; \
	fi

# Build MLB daily WIDE predictions from market snapshot + model workflow.
mlb-predictions-wide:
	MLB_PREDICT_TWO_SIDED_OPTIONAL_TARGET_BOOK_PROPS="$(MLB_PREDICT_TWO_SIDED_OPTIONAL_TARGET_BOOK_PROPS)" $(VENV_PY) backend/mlb/scripts/build_mlb_predictions_wide.py --slate-date $(MLB_DATE) --output "$(MLB_SLATE_PRED_CSV)" --odds-snapshot-out "$(MLB_ODDS_SNAPSHOT_JSON)" --require-min-rows "$(MLB_WIDE_REQUIRE_MIN_ROWS)" $(if $(strip $(MLB_WIDE_PROP_TYPES)),--prop-types "$(MLB_WIDE_PROP_TYPES)",) $(if $(strip $(MLB_ODDS_SNAPSHOT_IN)),--odds-snapshot-in "$(MLB_ODDS_SNAPSHOT_IN)",) $(if $(filter 1 true TRUE yes YES,$(MLB_PREDICT_REQUIRE_TWO_SIDED)),--require-two-sided,) $(if $(strip $(MLB_PREDICT_TWO_SIDED_BOOKMAKER)),--two-sided-bookmaker "$(MLB_PREDICT_TWO_SIDED_BOOKMAKER)",)
	@if [ "$${MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE:-0}" != "0" ]; then echo "UBO5_DECOMMISSIONED: old enable flag ignored; incumbent remains active"; fi
	$(VENV_PY) -m backend.mlb.scripts.build_mlb_bol_tb15_market_board --date "$(MLB_DATE)" --wide-csv "$(MLB_SLATE_PRED_CSV)" --odds-json "$(MLB_ODDS_SNAPSHOT_JSON)" --run-tag "$(MLB_RUN_TAG)"

# Render the authoritative route as a presentation-only operator board.
.PHONY: mlb-ubo5-tb15-board mlb-ubo5-tb15-refresh
mlb-ubo5-tb15-board:
	@echo "UBO-5 TB 1.5 is decommissioned and is no longer part of daily operations."

# Fresh manual-only UBO-5 TB 1.5 prices, lineups, scoring, and boards.
mlb-ubo5-tb15-refresh:
	@echo "UBO-5 TB 1.5 is decommissioned and is no longer part of daily operations."

.PHONY: mlb-ubo5-history-refresh mlb-ubo5-history-backfill
mlb-ubo5-history-refresh:
	@echo "UBO-5 TB 1.5 is decommissioned and is no longer part of daily operations."

mlb-ubo5-history-backfill:
	@echo "UBO-5 TB 1.5 is decommissioned and is no longer part of daily operations."

# Append a presentation-only observation and render the provisional watchlist.
.PHONY: mlb-ubo5-tb15-provisional-tracker
mlb-ubo5-tb15-provisional-tracker:
	@echo "UBO-5 TB 1.5 is decommissioned and is no longer part of daily operations."

# Render the presentation-only nine-position pre-lineup confirmation envelope.
.PHONY: mlb-ubo5-tb15-prelineup-confirmation
mlb-ubo5-tb15-prelineup-confirmation:
	@echo "UBO-5 TB 1.5 is decommissioned and is no longer part of daily operations."

# Rebuild the broader run-snapshot spine only from exact surviving run-tagged
# sources. This never reconstructs or modifies consensus selections.
.PHONY: mlb-ubo5-tb15-run-spine-backfill
mlb-ubo5-tb15-run-spine-backfill:
	@echo "UBO-5 TB 1.5 is decommissioned; new or reconstructed populations are prohibited."

.PHONY: mlb-ubo5-tb15-ever-positive-closeout
mlb-ubo5-tb15-ever-positive-closeout:
	$(VENV_PY) -m backend.mlb.scripts.build_mlb_ubo5_tb15_broad_population_closeout --date "$(MLB_DATE)" --population ever_positive --output-root "$(MLB_UBO5_TB15_BOARD_ROOT)" --reconcile-csv "$(MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV)"

.PHONY: mlb-ubo5-tb15-final-pregame-closeout
mlb-ubo5-tb15-final-pregame-closeout:
	$(VENV_PY) -m backend.mlb.scripts.build_mlb_ubo5_tb15_broad_population_closeout --date "$(MLB_DATE)" --population final_pregame --output-root "$(MLB_UBO5_TB15_BOARD_ROOT)" --reconcile-csv "$(MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV)"

.PHONY: mlb-ubo5-tb15-complete-outcome-audit
mlb-ubo5-tb15-complete-outcome-audit:
	$(VENV_PY) -m backend.mlb.scripts.build_mlb_ubo5_tb15_complete_outcome_audit --date "$(MLB_DATE)" --output-root "$(MLB_UBO5_TB15_BOARD_ROOT)" --reconcile-csv "$(MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV)"

.PHONY: mlb-ubo5-tb15-broad-records
mlb-ubo5-tb15-broad-records:
	@rc=0; \
	$(MAKE) mlb-ubo5-tb15-complete-outcome-audit MLB_DATE="$(MLB_DATE)" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="artifacts/analysis/mlb/execution_vs_model/$(MLB_DATE)/reconcile_rows.csv" || rc=$$?; \
	$(MAKE) mlb-ubo5-tb15-ever-positive-closeout MLB_DATE="$(MLB_DATE)" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="artifacts/analysis/mlb/execution_vs_model/$(MLB_DATE)/reconcile_rows.csv" || rc=$$?; \
	$(MAKE) mlb-ubo5-tb15-final-pregame-closeout MLB_DATE="$(MLB_DATE)" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="artifacts/analysis/mlb/execution_vs_model/$(MLB_DATE)/reconcile_rows.csv" || rc=$$?; \
	for manifest in $(MLB_UBO5_TB15_BOARD_ROOT)/????-??-??/ubo5_tb15_run_population_manifest_*.json; do \
		[ -f "$$manifest" ] || continue; pending_date=$$(basename "$$(dirname "$$manifest")"); \
		[ "$$pending_date" != "$(MLB_DATE)" ] || continue; \
		[ -s "artifacts/analysis/mlb/execution_vs_model/$$pending_date/reconcile_rows.csv" ] || continue; \
		current_audit="$(MLB_UBO5_TB15_BOARD_ROOT)/$$pending_date/ubo5_tb15_complete_outcome_audit_current.json"; \
		audit_status=""; [ ! -f "$$current_audit" ] || audit_status=$$($(VENV_PY) -c 'import json,sys; print(json.load(open(sys.argv[1])).get("closeout_status",""))' "$$current_audit"); \
		[ "$$audit_status" = "FINAL" ] || $(MAKE) mlb-ubo5-tb15-complete-outcome-audit MLB_DATE="$$pending_date" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="artifacts/analysis/mlb/execution_vs_model/$$pending_date/reconcile_rows.csv" || rc=$$?; \
		for population in ever_positive final_pregame; do \
			current="$(MLB_UBO5_TB15_BOARD_ROOT)/$$pending_date/ubo5_tb15_$${population}_closeout_current.json"; \
			status=""; [ ! -f "$$current" ] || status=$$($(VENV_PY) -c 'import json,sys; print(json.load(open(sys.argv[1])).get("closeout_status",""))' "$$current"); \
			[ "$$status" != "FINAL" ] || continue; \
			if [ "$$population" = "ever_positive" ]; then target=mlb-ubo5-tb15-ever-positive-closeout; else target=mlb-ubo5-tb15-final-pregame-closeout; fi; \
			$(MAKE) $$target MLB_DATE="$$pending_date" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="artifacts/analysis/mlb/execution_vs_model/$$pending_date/reconcile_rows.csv" || rc=$$?; \
		done; \
	done; exit $$rc

# Build a revisioned observation closeout from immutable morning/run-tagged artifacts.
.PHONY: mlb-ubo5-tb15-closeout
mlb-ubo5-tb15-closeout:
	$(VENV_PY) -m backend.mlb.scripts.build_mlb_ubo5_tb15_daily_closeout --date "$(MLB_DATE)" --output-root "$(MLB_UBO5_TB15_BOARD_ROOT)" --odds-root "$(MLB_ODDS_HISTORY_ROOT)" --reconcile-csv "$(MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV)"

# Retry the just-reconciled slate plus every locally pending closeout. FINAL
# manifests are skipped; unchanged sources remain revision-stable.
.PHONY: mlb-ubo5-tb15-retry-pending-closeouts
mlb-ubo5-tb15-retry-pending-closeouts:
	@rc=0; \
	$(MAKE) mlb-ubo5-tb15-closeout MLB_DATE="$(MLB_DATE)" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="artifacts/analysis/mlb/execution_vs_model/$(MLB_DATE)/reconcile_rows.csv" || rc=$$?; \
	for manifest in $(MLB_UBO5_TB15_BOARD_ROOT)/????-??-??/ubo5_tb15_closeout_current.json; do \
		[ -f "$$manifest" ] || continue; \
		status=$$($(VENV_PY) -c 'import json,sys; print(json.load(open(sys.argv[1])).get("closeout_status",""))' "$$manifest"); \
		[ "$$status" != "FINAL" ] || continue; \
		pending_date=$$(basename "$$(dirname "$$manifest")"); \
		[ "$$pending_date" != "$(MLB_DATE)" ] || continue; \
		echo "Retrying pending UBO-5 TB1.5 closeout for $$pending_date"; \
		$(MAKE) mlb-ubo5-tb15-closeout MLB_DATE="$$pending_date" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="artifacts/analysis/mlb/execution_vs_model/$$pending_date/reconcile_rows.csv" || rc=$$?; \
	done; \
	exit $$rc

# Grade only the immutable certified UBO-5 + incumbent consensus-board
# population. This record is intentionally separate from the broad closeout.
.PHONY: mlb-ubo5-tb15-consensus-closeout
mlb-ubo5-tb15-consensus-closeout:
	$(VENV_PY) -m backend.mlb.scripts.build_mlb_ubo5_tb15_consensus_closeout --date "$(MLB_DATE)" --output-root "$(MLB_UBO5_TB15_BOARD_ROOT)" --reconcile-csv "$(MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV)"

# Shared reconciliation hook for already-frozen historical TB1.5 populations.
# It never creates a population: only an existing non-FINAL current manifest
# with an existing exact-ID reconcile ledger can reach a retained finalizer.
.PHONY: mlb-finalize-frozen-tb15-populations
mlb-finalize-frozen-tb15-populations:
	@rc=0; \
	for current in $(MLB_UBO5_TB15_BOARD_ROOT)/????-??-??/*_current.json; do \
		[ -f "$$current" ] || continue; \
		status=$$($(VENV_PY) -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d.get("closeout_status") or d.get("status") or "")' "$$current" 2>/dev/null); \
		[ "$$status" != "FINAL" ] && [ "$$status" != "FINAL_ARCHIVED" ] || continue; \
		date=$$(basename "$$(dirname "$$current")"); reconcile="artifacts/analysis/mlb/execution_vs_model/$$date/reconcile_rows.csv"; \
		[ -s "$$reconcile" ] || continue; \
		case "$$(basename "$$current")" in \
		  ubo5_tb15_closeout_current.json) target=mlb-ubo5-tb15-closeout ;; \
		  ubo5_tb15_consensus_closeout_current.json) target=mlb-ubo5-tb15-consensus-closeout ;; \
		  ubo5_tb15_complete_outcome_audit_current.json) target=mlb-ubo5-tb15-complete-outcome-audit ;; \
		  ubo5_tb15_ever_positive_closeout_current.json) target=mlb-ubo5-tb15-ever-positive-closeout ;; \
		  ubo5_tb15_final_pregame_closeout_current.json) target=mlb-ubo5-tb15-final-pregame-closeout ;; \
		  *) continue ;; \
		esac; \
		echo "Finalizing existing frozen TB1.5 population date=$$date target=$$target"; \
		$(MAKE) $$target MLB_DATE="$$date" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="$$reconcile" || rc=$$?; \
	done; exit $$rc

.PHONY: mlb-ubo5-tb15-retry-pending-consensus-closeouts
mlb-ubo5-tb15-retry-pending-consensus-closeouts:
	@rc=0; \
	$(MAKE) mlb-ubo5-tb15-consensus-closeout MLB_DATE="$(MLB_DATE)" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="artifacts/analysis/mlb/execution_vs_model/$(MLB_DATE)/reconcile_rows.csv" || rc=$$?; \
	for manifest in $(MLB_UBO5_TB15_BOARD_ROOT)/????-??-??/ubo5_tb15_consensus_population_manifest_*.json; do \
		[ -f "$$manifest" ] || continue; \
		pending_date=$$(basename "$$(dirname "$$manifest")"); \
		[ "$$pending_date" != "$(MLB_DATE)" ] || continue; \
		current="$(MLB_UBO5_TB15_BOARD_ROOT)/$$pending_date/ubo5_tb15_consensus_closeout_current.json"; \
		status=""; [ ! -f "$$current" ] || status=$$($(VENV_PY) -c 'import json,sys; print(json.load(open(sys.argv[1])).get("closeout_status",""))' "$$current"); \
		[ "$$status" != "FINAL" ] || continue; \
		echo "Retrying pending UBO-5 + Incumbent consensus closeout for $$pending_date"; \
		$(MAKE) mlb-ubo5-tb15-consensus-closeout MLB_DATE="$$pending_date" MLB_UBO5_TB15_CLOSEOUT_RECONCILE_CSV="artifacts/analysis/mlb/execution_vs_model/$$pending_date/reconcile_rows.csv" || rc=$$?; \
	done; \
	exit $$rc

# Build canonical MLB slate output (model-only) from calibrated wide predictions.
mlb-slate-output:
	MLB_APPLY_PROBABILITY_CALIBRATION_TO_UPLOAD="$(MLB_APPLY_PROBABILITY_CALIBRATION_TO_UPLOAD)" $(VENV_PY) backend/mlb/scripts/build_mlb_slate_output.py --slate-date $(MLB_DATE) --pred-csv "$(MLB_SLATE_PRED_CSV)" --out-csv "$(MLB_SLATE_OUTPUT_CSV)" $(if $(strip $(MLB_SLATE_PROP_TYPE)),--prop-type "$(MLB_SLATE_PROP_TYPE)") $(if $(and $(filter 1 true TRUE yes YES,$(MLB_APPLY_PROBABILITY_CALIBRATION_TO_UPLOAD)),$(wildcard $(MLB_PROBABILITY_CALIBRATION_JSON))),--calibration-json "$(MLB_PROBABILITY_CALIBRATION_JSON)",)

# Export MLB book-upload CSV from canonical MLB slate output.
.PHONY: mlb-book-upload mlb-book-upload-variants mlb-book-upload-policy mlb-book-upload-top-recommended

mlb-book-upload:
	MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" $(VENV_PY) backend/mlb/scripts/export_mlb_book_upload.py --slate-date $(MLB_DATE) --use-slate-output --slate-csv "$(MLB_SLATE_OUTPUT_CSV)" --min-side-prob "$(MLB_BOOK_UPLOAD_MIN_SIDE_PROB)" --selection-mode "policy" --policy-plan-csv ""
	@if [ "$${MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST:-0}" = "1" ] && [ "$${MLB_BOOK_UPLOAD_REMOTE_FETCH_KIND:-book_upload}" = "book_upload" ]; then \
		echo "mlb-book-upload: remote kind=book_upload sync mode; skipping local mlb-slate-archive"; \
	else \
		$(MAKE) mlb-slate-archive MLB_DATE="$(MLB_DATE)" MLB_ODDS_HISTORY_ROOT="$(MLB_ODDS_HISTORY_ROOT)" MLB_SLATE_PRED_CSV="$(MLB_SLATE_PRED_CSV)" MLB_SLATE_OUTPUT_CSV="$(MLB_SLATE_OUTPUT_CSV)" MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" MLB_ODDS_SNAPSHOT_JSON="$(MLB_ODDS_SNAPSHOT_JSON)" MLB_ARCHIVE_RUN_TAG="$(MLB_ARCHIVE_RUN_TAG)"; \
	fi

mlb-book-upload-variants:
	MLB_DATE="$(MLB_DATE)" \
	MLB_TMP_FOCUS_ROOT="$(MLB_TMP_FOCUS_ROOT)" \
	MLB_ODDS_SNAPSHOT_JSON="$(MLB_ODDS_SNAPSHOT_JSON)" \
	MLB_SLATE_PRED_CSV="$(MLB_SLATE_PRED_CSV)" \
	MLB_SLATE_OUTPUT_CSV="$(MLB_SLATE_OUTPUT_CSV)" \
	MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" \
	MLB_WEIGHTED_MODEL_DIR="$(MLB_WEIGHTED_MODEL_DIR)" \
	MLB_BOOK_UPLOAD_WEIGHTED_OUT_CSV="$(MLB_BOOK_UPLOAD_WEIGHTED_OUT_CSV)" \
	MLB_BOOK_UPLOAD_HYBRID_OUT_CSV="$(MLB_BOOK_UPLOAD_HYBRID_OUT_CSV)" \
	MLB_WEIGHTED_SLATE_PRED_CSV="$(MLB_WEIGHTED_SLATE_PRED_CSV)" \
	MLB_WEIGHTED_SLATE_OUTPUT_CSV="$(MLB_WEIGHTED_SLATE_OUTPUT_CSV)" \
	MLB_UPLOAD_VARIANTS_BUILD_BASE="$(MLB_UPLOAD_VARIANTS_BUILD_BASE)" \
	bin/mlb_build_upload_variants.sh build "$(MLB_DATE)"

mlb-book-upload-policy:
	@if [ -z "$(strip $(MLB_POLICY_PLAN_CSV))" ]; then \
		echo "mlb-book-upload-policy requires MLB_POLICY_PLAN_CSV=<path/to/policy_plan.csv>"; \
		exit 2; \
	fi
	MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" $(VENV_PY) backend/mlb/scripts/export_mlb_book_upload.py --slate-date $(MLB_DATE) --use-slate-output --slate-csv "$(MLB_SLATE_OUTPUT_CSV)" --min-side-prob "$(MLB_BOOK_UPLOAD_MIN_SIDE_PROB)" --selection-mode "$(MLB_BOOK_UPLOAD_SELECTION_MODE)" --policy-plan-csv "$(MLB_POLICY_PLAN_CSV)" --odds-snapshot-json "$(MLB_ODDS_SNAPSHOT_JSON)" $(if $(filter 1,$(MLB_POLICY_PLAN_ALLOW_ONE_SIDED)),--policy-allow-one-sided,) $(if $(filter 1,$(MLB_POLICY_PLAN_ALLOW_EMPTY)),--policy-allow-empty,)
	@if [ "$${MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST:-0}" = "1" ] && [ "$${MLB_BOOK_UPLOAD_REMOTE_FETCH_KIND:-book_upload}" = "book_upload" ]; then \
		echo "mlb-book-upload-policy: remote kind=book_upload sync mode; skipping local mlb-slate-archive"; \
	else \
		$(MAKE) mlb-slate-archive MLB_DATE="$(MLB_DATE)" MLB_ODDS_HISTORY_ROOT="$(MLB_ODDS_HISTORY_ROOT)" MLB_SLATE_PRED_CSV="$(MLB_SLATE_PRED_CSV)" MLB_SLATE_OUTPUT_CSV="$(MLB_SLATE_OUTPUT_CSV)" MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" MLB_ODDS_SNAPSHOT_JSON="$(MLB_ODDS_SNAPSHOT_JSON)" MLB_ARCHIVE_RUN_TAG="$(MLB_ARCHIVE_RUN_TAG)"; \
	fi

# One-command side-matrix upload build (no EV/gap policy filters).
# 1) (optional) refresh cumulative model+fade bucket reports
# 2) build side matrix (preferred model/fade side by odds bucket)
# 3) emit tool-ready upload CSV for today's slate
mlb-book-upload-side-matrix:
	@if [ "$(MLB_BOOK_UPLOAD_SIDE_MATRIX_REFRESH_REPORTS)" = "1" ]; then \
		$(MAKE) mlb-red-mode-bucket-report-combined MLB_RED_BUCKET_FROM_DATE="$(MLB_RED_BUCKET_FROM_DATE)" MLB_RED_BUCKET_TO_DATE="$(MLB_RED_BUCKET_TO_DATE)" MLB_RED_BUCKET_BOOKMAKER="$(MLB_RED_BUCKET_BOOKMAKER)" MLB_RED_BUCKET_ODDS_FILENAME="$(MLB_RED_BUCKET_ODDS_FILENAME)" MLB_RED_BUCKET_ROWS_CSV="$(MLB_RED_BUCKET_ROWS_CSV)" MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON)" MLB_RED_BUCKET_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_SUMMARY_OUT_JSON)" MLB_RED_BUCKET_BY_BUCKET_OUT_CSV="$(MLB_RED_BUCKET_BY_BUCKET_OUT_CSV)" MLB_RED_BUCKET_FOCUS_OUT_CSV="$(MLB_RED_BUCKET_FOCUS_OUT_CSV)" MLB_RED_BUCKET_LAYOUT="$(MLB_BOOK_UPLOAD_SIDE_MATRIX_BUCKET_LAYOUT)" MLB_RED_BUCKET_FOCUS_BUCKETS="$(MLB_RED_BUCKET_FOCUS_BUCKETS)" MLB_RED_BUCKET_FADE_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_FADE_SUMMARY_OUT_JSON)" MLB_RED_BUCKET_FADE_BY_BUCKET_OUT_CSV="$(MLB_RED_BUCKET_FADE_BY_BUCKET_OUT_CSV)" MLB_RED_BUCKET_FADE_FOCUS_OUT_CSV="$(MLB_RED_BUCKET_FADE_FOCUS_OUT_CSV)" MLB_RED_BUCKET_FADE_MIN_PRINT_ROI_PCT="$(MLB_RED_BUCKET_FADE_MIN_PRINT_ROI_PCT)"; \
	fi
	$(VENV_PY) backend/mlb/scripts/export_mlb_book_upload_side_matrix.py --model-buckets-csv "$(MLB_RED_BUCKET_BY_BUCKET_OUT_CSV)" --fade-buckets-csv "$(MLB_RED_BUCKET_FADE_BY_BUCKET_OUT_CSV)" --side-matrix-out-csv "$(MLB_RED_SIDE_MATRIX_OUT_CSV)" --slate-csv "$(MLB_SLATE_OUTPUT_CSV)" --slate-date "$(MLB_DATE)" --odds-snapshot-json "$(MLB_ODDS_SNAPSHOT_JSON)" --odds-history-root "$(MLB_ODDS_HISTORY_ROOT)" --bookmaker "$(MLB_BOOK_UPLOAD_SIDE_MATRIX_BOOKMAKER)" --allowed-statuses "$(MLB_BOOK_UPLOAD_SIDE_MATRIX_ALLOWED_STATUSES)" --selection-mode "$(MLB_BOOK_UPLOAD_SIDE_MATRIX_SELECTION_MODE)" --out-csv "$(MLB_BOOK_UPLOAD_SIDE_MATRIX_OUT_CSV)" --dated-out-csv "$(MLB_BOOK_UPLOAD_SIDE_MATRIX_DATED_OUT_CSV)" --details-out-csv "$(MLB_BOOK_UPLOAD_SIDE_MATRIX_DETAILS_OUT_CSV)"

# Build daily tool-ready sheet from historical lane stats + side-matrix rows.
mlb-daily-bet-sheet:
	$(VENV_PY) backend/mlb/scripts/build_mlb_daily_bet_sheet.py --slate-date "$(MLB_DATE)" --history-rows-csv "$(MLB_BET_SHEET_HISTORY_ROWS_CSV)" --details-csv "$(MLB_BET_SHEET_DETAILS_CSV)" --upload-csv "$(MLB_BET_SHEET_UPLOAD_CSV)" --bookmaker "$(MLB_BET_SHEET_BOOKMAKER)" --selection "$(MLB_BET_SHEET_SELECTION)" --prop-types "$(MLB_BET_SHEET_PROP_TYPES)" --required-side "$(MLB_BET_SHEET_REQUIRED_SIDE)" --required-pick-type "$(MLB_BET_SHEET_REQUIRED_PICK_TYPE)" --min-lane-rows "$(MLB_BET_SHEET_MIN_LANE_ROWS)" --min-lane-roi-pct "$(MLB_BET_SHEET_MIN_LANE_ROI_PCT)" --out-upload-csv "$(MLB_BET_SHEET_OUT_UPLOAD_CSV)" --out-details-csv "$(MLB_BET_SHEET_OUT_DETAILS_CSV)" --out-summary-json "$(MLB_BET_SHEET_OUT_SUMMARY_JSON)" $(if $(filter 1 true TRUE yes YES,$(MLB_BET_SHEET_FAIL_IF_EMPTY)),--fail-if-empty,)

# Deterministic walk-forward lane test with strict pass/fail criteria.
mlb-rebuild-lane-test:
	$(VENV_PY) backend/mlb/scripts/test_mlb_rebuild_lanes.py --rows-csv "$(MLB_REBUILD_TEST_ROWS_CSV)" --from-date "$(MLB_REBUILD_TEST_FROM_DATE)" --to-date "$(MLB_REBUILD_TEST_TO_DATE)" --bookmaker "$(MLB_REBUILD_TEST_BOOKMAKER)" $(if $(filter 1 true TRUE yes YES,$(MLB_REBUILD_TEST_REQUIRE_TWO_SIDED)),--require-two-sided,) --warmup-days "$(MLB_REBUILD_TEST_WARMUP_DAYS)" --min-prop-bets "$(MLB_REBUILD_TEST_MIN_PROP_BETS)" --min-positive-props "$(MLB_REBUILD_TEST_MIN_POSITIVE_PROPS)" --max-prop-pnl-share-pct "$(MLB_REBUILD_TEST_MAX_PROP_PNL_SHARE_PCT)" --out-summary-csv "$(MLB_REBUILD_TEST_OUT_SUMMARY_CSV)" --out-daily-csv "$(MLB_REBUILD_TEST_OUT_DAILY_CSV)" --out-by-prop-csv "$(MLB_REBUILD_TEST_OUT_BY_PROP_CSV)" --out-json "$(MLB_REBUILD_TEST_OUT_JSON)"

# Build adaptive top-N recommendation from current book upload + recent post-grade history.
mlb-book-upload-top-recommended:
	$(VENV_PY) backend/mlb/scripts/recommend_mlb_book_upload_filters.py --book-upload-csv "$(MLB_BOOK_UPLOAD_OUT_CSV)" --by-prop-tracker-csv "$(MLB_POSTGRADE_TRACKER_OUT_BY_PROP_CSV)" --lookback-days "$(MLB_BOOK_UPLOAD_FILTER_LOOKBACK_DAYS)" --windows-days "$(MLB_BOOK_UPLOAD_FILTER_WINDOWS_DAYS)" --target-rows "$(MLB_BOOK_UPLOAD_FILTER_TARGET_ROWS)" --min-model-rows "$(MLB_BOOK_UPLOAD_FILTER_MIN_MODEL_ROWS)" --min-model-win-rate-pct "$(MLB_BOOK_UPLOAD_FILTER_MIN_MODEL_WIN_RATE_PCT)" --min-graded-rows "$(MLB_BOOK_UPLOAD_FILTER_MIN_GRADED_ROWS)" --graded-roi-floor-pct "$(MLB_BOOK_UPLOAD_FILTER_GRADED_ROI_FLOOR_PCT)" --min-overs "$(MLB_BOOK_UPLOAD_FILTER_MIN_OVERS)" --out-csv "$(MLB_BOOK_UPLOAD_FILTER_OUT_CSV)" --out-json "$(MLB_BOOK_UPLOAD_FILTER_OUT_JSON)"

# Full daily capture smoke path using an existing odds snapshot (no live OddsAPI fetch).
mlb-daily-capture-from-snapshot:
	@if [ -z "$(MLB_ODDS_SNAPSHOT_IN)" ]; then \
		echo "mlb-daily-capture-from-snapshot requires MLB_ODDS_SNAPSHOT_IN=<path/to/odds_snapshot.json>"; \
		exit 2; \
	fi
	$(MAKE) mlb-predictions-wide MLB_DATE="$(MLB_DATE)" MLB_SLATE_PRED_CSV="$(MLB_SLATE_PRED_CSV)" MLB_ODDS_SNAPSHOT_JSON="$(MLB_ODDS_SNAPSHOT_JSON)" MLB_ODDS_SNAPSHOT_IN="$(MLB_ODDS_SNAPSHOT_IN)" MLB_WIDE_PROP_TYPES="$(MLB_WIDE_PROP_TYPES)" MLB_WIDE_REQUIRE_MIN_ROWS="$(MLB_WIDE_REQUIRE_MIN_ROWS)"
	$(MAKE) mlb-slate-output MLB_DATE="$(MLB_DATE)" MLB_SLATE_PRED_CSV="$(MLB_SLATE_PRED_CSV)" MLB_SLATE_OUTPUT_CSV="$(MLB_SLATE_OUTPUT_CSV)" MLB_SLATE_PROP_TYPE="$(MLB_SLATE_PROP_TYPE)"
	$(MAKE) mlb-book-upload MLB_DATE="$(MLB_DATE)" MLB_SLATE_OUTPUT_CSV="$(MLB_SLATE_OUTPUT_CSV)" MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" MLB_ODDS_HISTORY_ROOT="$(MLB_ODDS_HISTORY_ROOT)" MLB_ODDS_SNAPSHOT_JSON="$(MLB_ODDS_SNAPSHOT_JSON)"

# Archive one MLB slate's reproducibility artifacts under backend/mlb/exports/odds_history/YYYY-MM-DD.
mlb-slate-archive:
	$(VENV_PY) backend/mlb/scripts/archive_mlb_slate_artifacts.py --slate-date $(MLB_DATE) --odds-root "$(MLB_ODDS_HISTORY_ROOT)" --pred-csv "$(MLB_SLATE_PRED_CSV)" --slate-csv "$(MLB_SLATE_OUTPUT_CSV)" --book-upload-csv "$(MLB_BOOK_UPLOAD_OUT_CSV)" --odds-snapshot-json "$(MLB_ODDS_SNAPSHOT_JSON)" $(if $(strip $(MLB_ARCHIVE_RUN_TAG)),--run-tag "$(MLB_ARCHIVE_RUN_TAG)",)

# Build row-level MLB reconcile dataset from archived odds history artifacts.
mlb-reconcile-rows:
	$(VENV_PY) backend/mlb/scripts/build_mlb_reconcile_rows.py --odds-root "$(MLB_ODDS_HISTORY_ROOT)" --from-date "$(MLB_RECONCILE_FROM_DATE)" --to-date "$(MLB_RECONCILE_TO_DATE)" --bookmaker "$(MLB_RECONCILE_BOOKMAKER)" --slate-filename "$(MLB_RECONCILE_SLATE_FILENAME)" --slate-filename-mode "$(MLB_RECONCILE_SLATE_FILENAME_MODE)" --slate-filename-glob "$(MLB_RECONCILE_SLATE_FILENAME_GLOB)" --snapshot-policy "$(MLB_RECONCILE_SNAPSHOT_POLICY)" --snapshot-run-tag "$(MLB_RECONCILE_SNAPSHOT_RUN_TAG)" --odds-filename "$(MLB_RECONCILE_ODDS_FILENAME)" --odds-filename-mode "$(MLB_RECONCILE_ODDS_FILENAME_MODE)" --odds-filename-glob "$(MLB_RECONCILE_ODDS_FILENAME_GLOB)" --out-csv "$(MLB_RECONCILE_ROWS_OUT_CSV)" --out-summary-json "$(MLB_RECONCILE_SUMMARY_OUT_JSON)" $(if $(filter 1 true TRUE yes YES,$(MLB_RECONCILE_REQUIRE_TWO_SIDED)),--require-two-sided,) $(if $(filter 1 true TRUE yes YES,$(MLB_RECONCILE_INCLUDE_ONE_SIDED)),--include-one-sided,) $(if $(filter 1 true TRUE yes YES,$(MLB_RECONCILE_INCLUDE_SINGLE_BOOK)),--include-single-book,) $(if $(filter 1,$(MLB_RECONCILE_REQUIRE_OUTCOMES)),--require-outcomes,) --require-outcome-rows-min "$(MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN)"

# Compare model-picked side performance vs opposite-side fade from reconcile rows.
mlb-model-vs-fade:
	$(VENV_PY) backend/mlb/scripts/report_mlb_model_vs_fade.py --rows-csv "$(MLB_MODEL_VS_FADE_ROWS_CSV)" --out-json "$(MLB_MODEL_VS_FADE_OUT_JSON)" --out-csv "$(MLB_MODEL_VS_FADE_OUT_CSV)" --min-bets-alert "$(MLB_MODEL_VS_FADE_MIN_BETS_ALERT)"

# Report all-available resolved outcomes from reconcile rows.
mlb-all-available-report:
	$(VENV_PY) backend/mlb/scripts/report_mlb_all_available.py --rows-csv "$(MLB_ALL_AVAILABLE_ROWS_CSV)" --out-json "$(MLB_ALL_AVAILABLE_OUT_JSON)" --out-csv "$(MLB_ALL_AVAILABLE_OUT_CSV)"

# RED-mode cumulative report: model-picked ROI by American-odds bucket.
mlb-red-mode-bucket-report:
	$(MAKE) mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="$(MLB_RED_BUCKET_FROM_DATE)" MLB_RECONCILE_TO_DATE="$(MLB_RED_BUCKET_TO_DATE)" MLB_RECONCILE_BOOKMAKER="$(MLB_RED_BUCKET_BOOKMAKER)" MLB_RECONCILE_ODDS_FILENAME="$(MLB_RED_BUCKET_ODDS_FILENAME)" MLB_RECONCILE_ROWS_OUT_CSV="$(MLB_RED_BUCKET_ROWS_CSV)" MLB_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON)" MLB_RECONCILE_REQUIRE_OUTCOMES=1 MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN=1
	$(VENV_PY) backend/mlb/scripts/report_mlb_odds_bucket_roi.py --rows-csv "$(MLB_RED_BUCKET_ROWS_CSV)" --out-json "$(MLB_RED_BUCKET_SUMMARY_OUT_JSON)" --out-csv "$(MLB_RED_BUCKET_BY_BUCKET_OUT_CSV)" --out-focus-csv "$(MLB_RED_BUCKET_FOCUS_OUT_CSV)" --bucket-layout "$(MLB_RED_BUCKET_LAYOUT)" --focus-buckets "$(MLB_RED_BUCKET_FOCUS_BUCKETS)" --label-from-date "$(MLB_RED_BUCKET_FROM_DATE)" --label-to-date "$(MLB_RED_BUCKET_TO_DATE)" $(if $(filter 1 true TRUE yes YES,$(MLB_ODDS_BUCKET_REQUIRE_TWO_SIDED)),--require-two-sided,) $(if $(filter 1 true TRUE yes YES,$(MLB_RED_BUCKET_PRINT_BOTH_CONTRIBUTORS)),--print-both-contributors,) $(if $(filter 1 true TRUE yes YES,$(MLB_RED_BUCKET_OUTPUT_POSITIVE_ONLY)),--output-positive-only,)

# RED-mode cumulative report: model-picked ROI, output files filtered to positive ROI buckets only.
mlb-red-mode-bucket-report-positive:
	$(MAKE) mlb-red-mode-bucket-report MLB_RED_BUCKET_FROM_DATE="$(MLB_RED_BUCKET_FROM_DATE)" MLB_RED_BUCKET_TO_DATE="$(MLB_RED_BUCKET_TO_DATE)" MLB_RED_BUCKET_BOOKMAKER="$(MLB_RED_BUCKET_BOOKMAKER)" MLB_RED_BUCKET_ODDS_FILENAME="$(MLB_RED_BUCKET_ODDS_FILENAME)" MLB_RED_BUCKET_ROWS_CSV="$(MLB_RED_BUCKET_ROWS_CSV)" MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON)" MLB_RED_BUCKET_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_SUMMARY_OUT_JSON)" MLB_RED_BUCKET_BY_BUCKET_OUT_CSV="$(MLB_RED_BUCKET_BY_BUCKET_OUT_CSV)" MLB_RED_BUCKET_FOCUS_OUT_CSV="$(MLB_RED_BUCKET_FOCUS_OUT_CSV)" MLB_RED_BUCKET_LAYOUT="$(MLB_RED_BUCKET_LAYOUT)" MLB_RED_BUCKET_FOCUS_BUCKETS="$(MLB_RED_BUCKET_FOCUS_BUCKETS)" MLB_RED_BUCKET_PRINT_BOTH_CONTRIBUTORS="$(MLB_RED_BUCKET_PRINT_BOTH_CONTRIBUTORS)" MLB_RED_BUCKET_OUTPUT_POSITIVE_ONLY=1

# RED-mode cumulative report: fade ROI by American-odds bucket (compact positive-bucket printout).
mlb-red-mode-fade-bucket-report:
	$(MAKE) mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="$(MLB_RED_BUCKET_FROM_DATE)" MLB_RECONCILE_TO_DATE="$(MLB_RED_BUCKET_TO_DATE)" MLB_RECONCILE_BOOKMAKER="$(MLB_RED_BUCKET_BOOKMAKER)" MLB_RECONCILE_ODDS_FILENAME="$(MLB_RED_BUCKET_ODDS_FILENAME)" MLB_RECONCILE_ROWS_OUT_CSV="$(MLB_RED_BUCKET_ROWS_CSV)" MLB_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON)" MLB_RECONCILE_REQUIRE_OUTCOMES=1 MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN=1
	$(VENV_PY) backend/mlb/scripts/report_mlb_odds_bucket_roi.py --selection fade --rows-csv "$(MLB_RED_BUCKET_ROWS_CSV)" --out-json "$(MLB_RED_BUCKET_FADE_SUMMARY_OUT_JSON)" --out-csv "$(MLB_RED_BUCKET_FADE_BY_BUCKET_OUT_CSV)" --out-focus-csv "$(MLB_RED_BUCKET_FADE_FOCUS_OUT_CSV)" --bucket-layout "$(MLB_RED_BUCKET_LAYOUT)" --focus-buckets "" --label-from-date "$(MLB_RED_BUCKET_FROM_DATE)" --label-to-date "$(MLB_RED_BUCKET_TO_DATE)" $(if $(filter 1 true TRUE yes YES,$(MLB_ODDS_BUCKET_REQUIRE_TWO_SIDED)),--require-two-sided,) --print-positive-only --compact-print --min-print-roi-pct "$(MLB_RED_BUCKET_FADE_MIN_PRINT_ROI_PCT)"

# RED-mode combined report: model + fade in one command.
mlb-red-mode-bucket-report-combined:
	$(MAKE) mlb-red-mode-bucket-report MLB_RED_BUCKET_FROM_DATE="$(MLB_RED_BUCKET_FROM_DATE)" MLB_RED_BUCKET_TO_DATE="$(MLB_RED_BUCKET_TO_DATE)" MLB_RED_BUCKET_BOOKMAKER="$(MLB_RED_BUCKET_BOOKMAKER)" MLB_RED_BUCKET_ODDS_FILENAME="$(MLB_RED_BUCKET_ODDS_FILENAME)" MLB_RED_BUCKET_ROWS_CSV="$(MLB_RED_BUCKET_ROWS_CSV)" MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON)" MLB_RED_BUCKET_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_SUMMARY_OUT_JSON)" MLB_RED_BUCKET_BY_BUCKET_OUT_CSV="$(MLB_RED_BUCKET_BY_BUCKET_OUT_CSV)" MLB_RED_BUCKET_FOCUS_OUT_CSV="$(MLB_RED_BUCKET_FOCUS_OUT_CSV)" MLB_RED_BUCKET_LAYOUT="$(MLB_RED_BUCKET_LAYOUT)" MLB_RED_BUCKET_FOCUS_BUCKETS="$(MLB_RED_BUCKET_FOCUS_BUCKETS)"
	$(MAKE) mlb-red-mode-fade-bucket-report MLB_RED_BUCKET_FROM_DATE="$(MLB_RED_BUCKET_FROM_DATE)" MLB_RED_BUCKET_TO_DATE="$(MLB_RED_BUCKET_TO_DATE)" MLB_RED_BUCKET_BOOKMAKER="$(MLB_RED_BUCKET_BOOKMAKER)" MLB_RED_BUCKET_ODDS_FILENAME="$(MLB_RED_BUCKET_ODDS_FILENAME)" MLB_RED_BUCKET_ROWS_CSV="$(MLB_RED_BUCKET_ROWS_CSV)" MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_RECONCILE_SUMMARY_OUT_JSON)" MLB_RED_BUCKET_FADE_SUMMARY_OUT_JSON="$(MLB_RED_BUCKET_FADE_SUMMARY_OUT_JSON)" MLB_RED_BUCKET_FADE_BY_BUCKET_OUT_CSV="$(MLB_RED_BUCKET_FADE_BY_BUCKET_OUT_CSV)" MLB_RED_BUCKET_FADE_FOCUS_OUT_CSV="$(MLB_RED_BUCKET_FADE_FOCUS_OUT_CSV)" MLB_RED_BUCKET_LAYOUT="$(MLB_RED_BUCKET_LAYOUT)" MLB_RED_BUCKET_FADE_MIN_PRINT_ROI_PCT="$(MLB_RED_BUCKET_FADE_MIN_PRINT_ROI_PCT)"

# Build early movement rows from local daily OddsAPI snapshots.
mlb-build-early-steam-movement:
	$(VENV_PY) backend/mlb/scripts/build_mlb_early_steam_movement.py --date "$(MLB_DATE)" --odds-history-root "$(MLB_ODDS_HISTORY_ROOT)" --out-csv "$(MLB_EARLY_STEAM_MOVEMENT_OUT_CSV)" $(if $(strip $(MLB_EARLY_STEAM_MOVEMENT_SNAPSHOT_GLOB)),--snapshot-glob "$(MLB_EARLY_STEAM_MOVEMENT_SNAPSHOT_GLOB)",)

# Join daily/multiday early movement rows to reconcile outcomes while preserving movement snapshots.
mlb-early-steam-results:
	$(VENV_PY) backend/mlb/scripts/build_mlb_early_steam_results.py --movement-csv "$(MLB_EARLY_STEAM_MOVEMENT_CSV)" --reconcile-csv "$(MLB_EARLY_STEAM_RECONCILE_CSV)" --out-csv "$(MLB_EARLY_STEAM_ROWS_CSV)" --min-imp-move "$(MLB_EARLY_STEAM_PITCHER_MIN_IMP_MOVE)" --max-imp-move "$(MLB_EARLY_STEAM_PITCHER_MAX_IMP_MOVE)"

# Export daily early-steam pitcher-market candidates from joined movement results.
mlb-early-steam-pitcher-candidates:
	$(VENV_PY) backend/mlb/scripts/export_mlb_early_steam_pitcher_candidates.py --rows-csv "$(MLB_EARLY_STEAM_ROWS_CSV)" --date "$(MLB_DATE)" --out-csv "$(MLB_EARLY_STEAM_PITCHER_CANDIDATES_OUT_CSV)" --out-summary-csv "$(MLB_EARLY_STEAM_PITCHER_CANDIDATES_SUMMARY_OUT_CSV)" --min-imp-move "$(MLB_EARLY_STEAM_PITCHER_MIN_IMP_MOVE)" --max-imp-move "$(MLB_EARLY_STEAM_PITCHER_MAX_IMP_MOVE)"

# Join early-steam pitcher candidates to recent pitcher profiles and emit coarse stability segments.
mlb-early-steam-pitcher-profiles:
	$(VENV_PY) backend/mlb/scripts/analyze_early_steam_pitcher_profiles.py --early-steam-csv "$(MLB_EARLY_STEAM_ROWS_CSV)" --pitcher-logs-csv "$(MLB_EARLY_STEAM_PITCHER_PROFILE_LOGS_CSV)" --out-csv "$(MLB_EARLY_STEAM_PITCHER_PROFILE_OUT_CSV)" --summary-csv "$(MLB_EARLY_STEAM_PITCHER_PROFILE_SUMMARY_OUT_CSV)" --stable-summary-csv "$(MLB_EARLY_STEAM_PITCHER_PROFILE_STABLE_SUMMARY_OUT_CSV)" --stable-min-bets "$(MLB_EARLY_STEAM_PITCHER_PROFILE_STABLE_MIN_BETS)" --min-imp-move "$(MLB_EARLY_STEAM_PITCHER_MIN_IMP_MOVE)" --max-imp-move "$(MLB_EARLY_STEAM_PITCHER_MAX_IMP_MOVE)"

# Export V1 early-steam pitching candidates using workload-volatility threshold.
mlb-early-steam-v1-pitching-candidates:
	$(VENV_PY) backend/mlb/scripts/export_mlb_early_steam_v1_pitching_candidates.py --rows-csv "$(MLB_EARLY_STEAM_ROWS_CSV)" --pitcher-logs-csv "$(MLB_EARLY_STEAM_V1_PITCHING_LOGS_CSV)" --date "$(MLB_DATE)" --out-csv "$(MLB_EARLY_STEAM_V1_PITCHING_OUT_CSV)" --min-imp-move "$(MLB_EARLY_STEAM_PITCHER_MIN_IMP_MOVE)" --max-imp-move "$(MLB_EARLY_STEAM_PITCHER_MAX_IMP_MOVE)" --min-outs-std "$(MLB_EARLY_STEAM_V1_MIN_OUTS_STD)"

# Download local source files for Retrosheet/Chadwick pitcher historical backfill.
mlb-download-retrosheet-sources:
	$(VENV_PY) backend/mlb/scripts/download_retrosheet_pitcher_sources.py --out-dir "$(MLB_RETROSHEET_RAW_DIR)" --download-chadwick-register $(if $(strip $(MLB_RETROSHEET_SEASON)),--season "$(MLB_RETROSHEET_SEASON)",) $(if $(filter 1 true TRUE yes YES,$(MLB_RETROSHEET_FORCE_DOWNLOAD)),--force,)

# Audit Chadwick Register Retrosheet -> MLBAM mapping coverage.
mlb-audit-chadwick-register:
	$(VENV_PY) backend/mlb/scripts/audit_chadwick_register_mapping.py --register-csv "$(MLB_CHADWICK_REGISTER_CSV)" --out-csv "$(MLB_CHADWICK_AUDIT_OUT_CSV)"

# Build CSV-only pybaseball pitcher game logs for a date range.
mlb-build-pitcher-game-logs-pybaseball:
	@if [ -z "$(START_DATE)" ] || [ -z "$(END_DATE)" ]; then \
		echo "mlb-build-pitcher-game-logs-pybaseball requires START_DATE=YYYY-MM-DD END_DATE=YYYY-MM-DD"; \
		exit 2; \
	fi
	$(VENV_PY) backend/mlb/scripts/build_pitcher_game_logs_pybaseball.py --start-date "$(START_DATE)" --end-date "$(END_DATE)" --chunk-days "$(MLB_PITCHER_GAME_LOGS_PYBASEBALL_CHUNK_DAYS)" --chadwick-register-csv "$(MLB_CHADWICK_REGISTER_CSV)" --out-csv "$(MLB_PITCHER_GAME_LOGS_PYBASEBALL_OUT_CSV)"

# Audit CSV-only pybaseball pitcher game logs.
mlb-audit-pitcher-game-logs-pybaseball:
	$(VENV_PY) backend/mlb/scripts/audit_pitcher_game_logs_pybaseball.py --csv "$(MLB_PITCHER_GAME_LOGS_PYBASEBALL_OUT_CSV)" --out-csv "$(MLB_PITCHER_GAME_LOGS_PYBASEBALL_AUDIT_OUT_CSV)"

# Post-grade routine: rebuild reconcile rows then report model-vs-fade for that window.
mlb-post-grade-fade-check:
	$(MAKE) mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="$(MLB_RECONCILE_FROM_DATE)" MLB_RECONCILE_TO_DATE="$(MLB_RECONCILE_TO_DATE)" MLB_RECONCILE_BOOKMAKER="$(MLB_RECONCILE_BOOKMAKER)" MLB_RECONCILE_ODDS_FILENAME="$(MLB_RECONCILE_ODDS_FILENAME)" MLB_RECONCILE_ROWS_OUT_CSV="$(MLB_RECONCILE_ROWS_OUT_CSV)" MLB_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RECONCILE_SUMMARY_OUT_JSON)" MLB_RECONCILE_REQUIRE_OUTCOMES="$(MLB_POST_GRADE_REQUIRE_OUTCOMES)" MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN="$(MLB_POST_GRADE_REQUIRE_OUTCOME_ROWS_MIN)"
	$(MAKE) mlb-model-vs-fade MLB_MODEL_VS_FADE_ROWS_CSV="$(MLB_RECONCILE_ROWS_OUT_CSV)" MLB_MODEL_VS_FADE_OUT_JSON="$(MLB_MODEL_VS_FADE_OUT_JSON)" MLB_MODEL_VS_FADE_OUT_CSV="$(MLB_MODEL_VS_FADE_OUT_CSV)" MLB_MODEL_VS_FADE_MIN_BETS_ALERT="$(MLB_MODEL_VS_FADE_MIN_BETS_ALERT)"

# Post-grade routine: rebuild reconcile rows then report all-available resolved outcomes.
mlb-post-grade-all-available-check:
	$(MAKE) mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="$(MLB_RECONCILE_FROM_DATE)" MLB_RECONCILE_TO_DATE="$(MLB_RECONCILE_TO_DATE)" MLB_RECONCILE_BOOKMAKER="$(MLB_RECONCILE_BOOKMAKER)" MLB_RECONCILE_ODDS_FILENAME="$(MLB_RECONCILE_ODDS_FILENAME)" MLB_RECONCILE_ROWS_OUT_CSV="$(MLB_RECONCILE_ROWS_OUT_CSV)" MLB_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RECONCILE_SUMMARY_OUT_JSON)" MLB_RECONCILE_REQUIRE_OUTCOMES="$(MLB_POST_GRADE_REQUIRE_OUTCOMES)" MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN="$(MLB_POST_GRADE_REQUIRE_OUTCOME_ROWS_MIN)"
	$(MAKE) mlb-all-available-report MLB_ALL_AVAILABLE_ROWS_CSV="$(MLB_RECONCILE_ROWS_OUT_CSV)" MLB_ALL_AVAILABLE_OUT_JSON="$(MLB_ALL_AVAILABLE_OUT_JSON)" MLB_ALL_AVAILABLE_OUT_CSV="$(MLB_ALL_AVAILABLE_OUT_CSV)"

# Summarize posted MLB graded wagers (priority placed-wager analysis source).
mlb-graded-wagers-report:
	@if [ -z "$(MLB_GRADED_IN_CSV)" ]; then \
		echo "mlb-graded-wagers-report requires MLB_GRADED_IN_CSV=<path/to/*_mlb_player_props.csv>"; \
		exit 2; \
	fi
	$(VENV_PY) backend/mlb/scripts/report_mlb_graded_wagers.py --in-csv "$(MLB_GRADED_IN_CSV)" --out-rows-csv "$(MLB_GRADED_ROWS_OUT_CSV)" --out-summary-json "$(MLB_GRADED_SUMMARY_OUT_JSON)" --out-by-prop-csv "$(MLB_GRADED_BY_PROP_OUT_CSV)"

# Convenience alias: use latest split MLB player-props grader CSV from tmp/graded.
mlb-graded-wagers-report-latest:
	@latest=$$(ls -1t tmp/graded/8rainstation_daily_*_mlb_player_props.csv 2>/dev/null | head -n 1); \
	if [ -z "$$latest" ]; then \
		if [ "$(MLB_GRADED_REPORT_REQUIRED)" = "1" ]; then \
			echo "mlb-graded-wagers-report-latest: no split MLB player-props grader CSV found under tmp/graded"; \
			exit 2; \
		fi; \
		echo "mlb-graded-wagers-report-latest: no split MLB player-props grader CSV found; skipping (MLB_GRADED_REPORT_REQUIRED=$(MLB_GRADED_REPORT_REQUIRED))"; \
		rm -f "$(MLB_GRADED_SUMMARY_OUT_JSON)" "$(MLB_GRADED_BY_PROP_OUT_CSV)" "$(MLB_GRADED_ROWS_OUT_CSV)"; \
	else \
		echo "mlb-graded-wagers-report-latest: using $$latest"; \
		$(MAKE) mlb-graded-wagers-report MLB_GRADED_IN_CSV="$$latest" MLB_GRADED_ROWS_OUT_CSV="$(MLB_GRADED_ROWS_OUT_CSV)" MLB_GRADED_SUMMARY_OUT_JSON="$(MLB_GRADED_SUMMARY_OUT_JSON)" MLB_GRADED_BY_PROP_OUT_CSV="$(MLB_GRADED_BY_PROP_OUT_CSV)"; \
	fi

# Append one post-grade tracker row and render trend charts.
mlb-post-grade-tracker:
	$(VENV_PY) backend/mlb/scripts/mlb_postgrade_tracker.py $(if $(strip $(MLB_POSTGRADE_TRACKER_DATE)),--date "$(MLB_POSTGRADE_TRACKER_DATE)",) --model-vs-fade-summary-json "$(MLB_MODEL_VS_FADE_OUT_JSON)" --all-available-summary-json "$(MLB_ALL_AVAILABLE_OUT_JSON)" --all-available-by-prop-csv "$(MLB_ALL_AVAILABLE_OUT_CSV)" --graded-summary-json "$(MLB_GRADED_SUMMARY_OUT_JSON)" --graded-by-prop-csv "$(MLB_GRADED_BY_PROP_OUT_CSV)" --book-upload-csv "$(MLB_BOOK_UPLOAD_OUT_CSV)" --out-csv "$(MLB_POSTGRADE_TRACKER_OUT_CSV)" --out-by-prop-csv "$(MLB_POSTGRADE_TRACKER_OUT_BY_PROP_CSV)" --charts-dir "$(MLB_POSTGRADE_TRACKER_CHARTS_DIR)" --alerts-out-json "$(MLB_POSTGRADE_ALERTS_OUT_JSON)" --alerts-history-jsonl "$(MLB_POSTGRADE_ALERTS_HISTORY_JSONL)" --alert-fade-min-paired-bets "$(MLB_POSTGRADE_ALERT_FADE_MIN_PAIRED_BETS)" --alert-roi-min-paired-bets "$(MLB_POSTGRADE_ALERT_ROI_MIN_PAIRED_BETS)" --alert-roi-breach-threshold "$(MLB_POSTGRADE_ALERT_ROI_BREACH_THRESHOLD)" --alert-overall-drop-window-days "$(MLB_POSTGRADE_ALERT_OVERALL_DROP_WINDOW_DAYS)" --alert-overall-drop-threshold-pct "$(MLB_POSTGRADE_ALERT_OVERALL_DROP_THRESHOLD_PCT)" --alert-prop-drop-window-days "$(MLB_POSTGRADE_ALERT_PROP_DROP_WINDOW_DAYS)" --alert-prop-drop-threshold-pct "$(MLB_POSTGRADE_ALERT_PROP_DROP_THRESHOLD_PCT)" --alert-prop-drop-min-model-rows "$(MLB_POSTGRADE_ALERT_PROP_DROP_MIN_MODEL_ROWS)" $(if $(filter 1,$(MLB_POSTGRADE_ALERTS_STRICT)),--alerts-strict,) $(if $(filter 1,$(MLB_POSTGRADE_TRACKER_SKIP_CHARTS)),--skip-charts,) $(if $(filter 1,$(MLB_POSTGRADE_ALLOW_GRADED_DATE_MISMATCH)),--allow-graded-date-mismatch,)

# One-command post-grade routine: reconcile rows -> reports -> tracker row + charts.
mlb-post-grade-report-and-track:
	$(MAKE) mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="$(MLB_RECONCILE_FROM_DATE)" MLB_RECONCILE_TO_DATE="$(MLB_RECONCILE_TO_DATE)" MLB_RECONCILE_BOOKMAKER="$(MLB_RECONCILE_BOOKMAKER)" MLB_RECONCILE_ODDS_FILENAME="$(MLB_RECONCILE_ODDS_FILENAME)" MLB_RECONCILE_ROWS_OUT_CSV="$(MLB_RECONCILE_ROWS_OUT_CSV)" MLB_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RECONCILE_SUMMARY_OUT_JSON)" MLB_RECONCILE_REQUIRE_OUTCOMES="$(MLB_POST_GRADE_REQUIRE_OUTCOMES)" MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN="$(MLB_POST_GRADE_REQUIRE_OUTCOME_ROWS_MIN)"
	$(MAKE) mlb-model-vs-fade MLB_MODEL_VS_FADE_ROWS_CSV="$(MLB_RECONCILE_ROWS_OUT_CSV)" MLB_MODEL_VS_FADE_OUT_JSON="$(MLB_MODEL_VS_FADE_OUT_JSON)" MLB_MODEL_VS_FADE_OUT_CSV="$(MLB_MODEL_VS_FADE_OUT_CSV)" MLB_MODEL_VS_FADE_MIN_BETS_ALERT="$(MLB_MODEL_VS_FADE_MIN_BETS_ALERT)"
	$(MAKE) mlb-all-available-report MLB_ALL_AVAILABLE_ROWS_CSV="$(MLB_RECONCILE_ROWS_OUT_CSV)" MLB_ALL_AVAILABLE_OUT_JSON="$(MLB_ALL_AVAILABLE_OUT_JSON)" MLB_ALL_AVAILABLE_OUT_CSV="$(MLB_ALL_AVAILABLE_OUT_CSV)"
	@if [ "$(MLB_POSTGRADE_INCLUDE_GRADED)" = "1" ]; then \
		$(MAKE) mlb-graded-wagers-report-latest MLB_GRADED_REPORT_REQUIRED="$(MLB_GRADED_REPORT_REQUIRED)" MLB_GRADED_ROWS_OUT_CSV="$(MLB_GRADED_ROWS_OUT_CSV)" MLB_GRADED_SUMMARY_OUT_JSON="$(MLB_GRADED_SUMMARY_OUT_JSON)" MLB_GRADED_BY_PROP_OUT_CSV="$(MLB_GRADED_BY_PROP_OUT_CSV)"; \
	else \
		echo "mlb-post-grade-report-and-track: skipping graded-wagers report (MLB_POSTGRADE_INCLUDE_GRADED=$(MLB_POSTGRADE_INCLUDE_GRADED))"; \
	fi
	$(MAKE) mlb-post-grade-tracker MLB_POSTGRADE_TRACKER_DATE="$(MLB_POSTGRADE_TRACKER_DATE)" MLB_POSTGRADE_TRACKER_OUT_CSV="$(MLB_POSTGRADE_TRACKER_OUT_CSV)" MLB_POSTGRADE_TRACKER_OUT_BY_PROP_CSV="$(MLB_POSTGRADE_TRACKER_OUT_BY_PROP_CSV)" MLB_POSTGRADE_TRACKER_CHARTS_DIR="$(MLB_POSTGRADE_TRACKER_CHARTS_DIR)" MLB_POSTGRADE_TRACKER_SKIP_CHARTS="$(MLB_POSTGRADE_TRACKER_SKIP_CHARTS)" MLB_POSTGRADE_ALLOW_GRADED_DATE_MISMATCH="$(MLB_POSTGRADE_ALLOW_GRADED_DATE_MISMATCH)" MLB_POSTGRADE_ALERTS_OUT_JSON="$(MLB_POSTGRADE_ALERTS_OUT_JSON)" MLB_POSTGRADE_ALERTS_HISTORY_JSONL="$(MLB_POSTGRADE_ALERTS_HISTORY_JSONL)" MLB_POSTGRADE_ALERT_FADE_MIN_PAIRED_BETS="$(MLB_POSTGRADE_ALERT_FADE_MIN_PAIRED_BETS)" MLB_POSTGRADE_ALERT_ROI_MIN_PAIRED_BETS="$(MLB_POSTGRADE_ALERT_ROI_MIN_PAIRED_BETS)" MLB_POSTGRADE_ALERT_ROI_BREACH_THRESHOLD="$(MLB_POSTGRADE_ALERT_ROI_BREACH_THRESHOLD)" MLB_POSTGRADE_ALERT_OVERALL_DROP_WINDOW_DAYS="$(MLB_POSTGRADE_ALERT_OVERALL_DROP_WINDOW_DAYS)" MLB_POSTGRADE_ALERT_OVERALL_DROP_THRESHOLD_PCT="$(MLB_POSTGRADE_ALERT_OVERALL_DROP_THRESHOLD_PCT)" MLB_POSTGRADE_ALERT_PROP_DROP_WINDOW_DAYS="$(MLB_POSTGRADE_ALERT_PROP_DROP_WINDOW_DAYS)" MLB_POSTGRADE_ALERT_PROP_DROP_THRESHOLD_PCT="$(MLB_POSTGRADE_ALERT_PROP_DROP_THRESHOLD_PCT)" MLB_POSTGRADE_ALERT_PROP_DROP_MIN_MODEL_ROWS="$(MLB_POSTGRADE_ALERT_PROP_DROP_MIN_MODEL_ROWS)" MLB_POSTGRADE_ALERTS_STRICT="$(MLB_POSTGRADE_ALERTS_STRICT)" MLB_MODEL_VS_FADE_OUT_JSON="$(MLB_MODEL_VS_FADE_OUT_JSON)" MLB_ALL_AVAILABLE_OUT_JSON="$(MLB_ALL_AVAILABLE_OUT_JSON)" MLB_ALL_AVAILABLE_OUT_CSV="$(MLB_ALL_AVAILABLE_OUT_CSV)" MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" MLB_GRADED_SUMMARY_OUT_JSON="$(MLB_GRADED_SUMMARY_OUT_JSON)" MLB_GRADED_BY_PROP_OUT_CSV="$(MLB_GRADED_BY_PROP_OUT_CSV)"

# Convenience alias: post-grade run pinned to ET date.
mlb-post-grade-report-and-track-et:
	$(MAKE) mlb-post-grade-report-and-track MLB_RECONCILE_FROM_DATE="$(MLB_POST_GRADE_DATE)" MLB_RECONCILE_TO_DATE="$(MLB_POST_GRADE_DATE)"

# Convenience alias: post-grade run pinned to latest archived slate date.
mlb-post-grade-report-and-track-latest:
	@latest=$$(ls -1 "$(MLB_ODDS_HISTORY_ROOT)" 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}$$' | sort | tail -n 1); \
	if [ -z "$$latest" ]; then \
		echo "mlb-post-grade-report-and-track-latest: no dated dirs found under $(MLB_ODDS_HISTORY_ROOT)"; \
		exit 2; \
	fi; \
	echo "mlb-post-grade-report-and-track-latest: using $$latest"; \
	$(MAKE) mlb-post-grade-report-and-track MLB_RECONCILE_FROM_DATE="$$latest" MLB_RECONCILE_TO_DATE="$$latest"

# Step 7 (MLB): split current grader CSV and run full post-grade tracking for that grader date.
mlb-post-grade-step7:
	@grader="$(MLB_GRADER_IN_CSV)"; \
	if [ -z "$$grader" ]; then \
		grader=$$(ls -1t ~/Downloads/8rainstation_daily_*.csv 2>/dev/null | head -n 1); \
	fi; \
	if [ -z "$$grader" ]; then \
		echo "mlb-post-grade-step7: no grader CSV found. Set MLB_GRADER_IN_CSV or place 8rainstation_daily_*.csv in ~/Downloads"; \
		exit 2; \
	fi; \
	echo "mlb-post-grade-step7: grader=$$grader"; \
	$(VENV_PY) backend/scripts/split_grader_csv_by_sport.py --in-csv "$$grader"; \
	date_tag=$$(basename "$$grader" .csv | sed -E 's/^8rainstation_daily_([0-9]{4})[_-]([0-9]{2})[_-]([0-9]{2}).*/\1-\2-\3/'); \
	if ! echo "$$date_tag" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}$$'; then \
		echo "mlb-post-grade-step7: could not infer date from $$grader"; \
		exit 2; \
	fi; \
	echo "mlb-post-grade-step7: date=$$date_tag"; \
	$(MAKE) mlb-post-grade-report-and-track \
		MLB_RECONCILE_FROM_DATE="$$date_tag" \
		MLB_RECONCILE_TO_DATE="$$date_tag" \
		MLB_RECONCILE_BOOKMAKER="$(MLB_RECONCILE_BOOKMAKER)" \
		MLB_GRADED_REPORT_REQUIRED=1

# One-step alias for post-grade processing after next-day cron settles outcomes.
mlb-post-grade-next-day:
	$(MAKE) mlb-post-grade-step7 MLB_GRADER_IN_CSV="$(MLB_GRADER_IN_CSV)" MLB_RECONCILE_BOOKMAKER="$(MLB_RECONCILE_BOOKMAKER)"

# Replay locked MLB policy plan across historical reconcile rows (includes fragile-lane monitor output).
mlb-policy-plan-replay:
	$(VENV_PY) backend/mlb/scripts/replay_mlb_policy_plan.py --rows-csv "$(MLB_POLICY_REPLAY_ROWS_CSV)" --policy-plan-csv "$(MLB_POLICY_PLAN_CSV)" --out-dir "$(MLB_POLICY_REPLAY_OUT_DIR)" --monitor-props "$(MLB_POLICY_MONITOR_PROPS)" --monitor-min-bets-alert "$(MLB_POLICY_MONITOR_MIN_BETS_ALERT)"

.PHONY: mlb-all-available-report mlb-post-grade-all-available-check mlb-graded-wagers-report mlb-graded-wagers-report-latest mlb-post-grade-tracker mlb-post-grade-report-and-track mlb-post-grade-report-and-track-et mlb-post-grade-report-and-track-latest mlb-post-grade-step7 mlb-post-grade-next-day

.PHONY: nhl-model-vs-fade nhl-post-grade-fade-check

# Compare NHL SOG model-picked performance vs opposite-side fade using card prices.
nhl-model-vs-fade:
	$(VENV_PY) backend/nhl/scripts/report_nhl_model_vs_fade.py --graded-glob "$(NHL_MODEL_VS_FADE_GRADED_GLOB)" --cards-dir "$(NHL_MODEL_VS_FADE_CARDS_DIR)" --min-bets-alert "$(NHL_MODEL_VS_FADE_MIN_BETS_ALERT)" --out-json "$(NHL_MODEL_VS_FADE_OUT_JSON)" --out-segments-csv "$(NHL_MODEL_VS_FADE_OUT_SEGMENTS_CSV)" --out-rows-csv "$(NHL_MODEL_VS_FADE_OUT_ROWS_CSV)"

# Post-grade NHL check alias (keeps this check explicit in daily routine).
nhl-post-grade-fade-check: nhl-model-vs-fade

.PHONY: cross-sport-model-vs-fade cross-sport-model-vs-fade-strict cross-sport-post-grade-fade-check

cross-sport-model-vs-fade:
	$(VENV_PY) backend/scripts/report_cross_sport_model_vs_fade.py --nhl-json "$(NHL_MODEL_VS_FADE_OUT_JSON)" --mlb-json "$(MLB_MODEL_VS_FADE_OUT_JSON)" --nhl-min-bets "$(CROSS_SPORT_MODEL_VS_FADE_NHL_MIN_BETS)" --mlb-min-bets "$(CROSS_SPORT_MODEL_VS_FADE_MLB_MIN_BETS)" --max-fade-minus-model-delta "$(CROSS_SPORT_MODEL_VS_FADE_MAX_DELTA)" $(if $(filter 1,$(CROSS_SPORT_MODEL_VS_FADE_REQUIRE_NHL)),--require-nhl,) $(if $(filter 1,$(CROSS_SPORT_MODEL_VS_FADE_REQUIRE_MLB)),--require-mlb,) --out-json "$(CROSS_SPORT_MODEL_VS_FADE_OUT_JSON)"

cross-sport-model-vs-fade-strict:
	$(VENV_PY) backend/scripts/report_cross_sport_model_vs_fade.py --nhl-json "$(NHL_MODEL_VS_FADE_OUT_JSON)" --mlb-json "$(MLB_MODEL_VS_FADE_OUT_JSON)" --nhl-min-bets "$(CROSS_SPORT_MODEL_VS_FADE_NHL_MIN_BETS)" --mlb-min-bets "$(CROSS_SPORT_MODEL_VS_FADE_MLB_MIN_BETS)" --max-fade-minus-model-delta "$(CROSS_SPORT_MODEL_VS_FADE_MAX_DELTA)" $(if $(filter 1,$(CROSS_SPORT_MODEL_VS_FADE_REQUIRE_NHL)),--require-nhl,) $(if $(filter 1,$(CROSS_SPORT_MODEL_VS_FADE_REQUIRE_MLB)),--require-mlb,) --strict --out-json "$(CROSS_SPORT_MODEL_VS_FADE_OUT_JSON)"

cross-sport-post-grade-fade-check:
	$(MAKE) nhl-post-grade-fade-check NHL_MODEL_VS_FADE_GRADED_GLOB="$(NHL_MODEL_VS_FADE_GRADED_GLOB)" NHL_MODEL_VS_FADE_CARDS_DIR="$(NHL_MODEL_VS_FADE_CARDS_DIR)" NHL_MODEL_VS_FADE_MIN_BETS_ALERT="$(NHL_MODEL_VS_FADE_MIN_BETS_ALERT)" NHL_MODEL_VS_FADE_OUT_JSON="$(NHL_MODEL_VS_FADE_OUT_JSON)" NHL_MODEL_VS_FADE_OUT_SEGMENTS_CSV="$(NHL_MODEL_VS_FADE_OUT_SEGMENTS_CSV)" NHL_MODEL_VS_FADE_OUT_ROWS_CSV="$(NHL_MODEL_VS_FADE_OUT_ROWS_CSV)"
	$(MAKE) mlb-post-grade-fade-check MLB_RECONCILE_FROM_DATE="$(MLB_RECONCILE_FROM_DATE)" MLB_RECONCILE_TO_DATE="$(MLB_RECONCILE_TO_DATE)" MLB_RECONCILE_BOOKMAKER="$(MLB_RECONCILE_BOOKMAKER)" MLB_RECONCILE_ODDS_FILENAME="$(MLB_RECONCILE_ODDS_FILENAME)" MLB_RECONCILE_ROWS_OUT_CSV="$(MLB_RECONCILE_ROWS_OUT_CSV)" MLB_RECONCILE_SUMMARY_OUT_JSON="$(MLB_RECONCILE_SUMMARY_OUT_JSON)" MLB_MODEL_VS_FADE_OUT_JSON="$(MLB_MODEL_VS_FADE_OUT_JSON)" MLB_MODEL_VS_FADE_OUT_CSV="$(MLB_MODEL_VS_FADE_OUT_CSV)" MLB_MODEL_VS_FADE_MIN_BETS_ALERT="$(MLB_MODEL_VS_FADE_MIN_BETS_ALERT)"
	$(MAKE) cross-sport-model-vs-fade-strict CROSS_SPORT_MODEL_VS_FADE_OUT_JSON="$(CROSS_SPORT_MODEL_VS_FADE_OUT_JSON)" CROSS_SPORT_MODEL_VS_FADE_MAX_DELTA="$(CROSS_SPORT_MODEL_VS_FADE_MAX_DELTA)" CROSS_SPORT_MODEL_VS_FADE_NHL_MIN_BETS="$(CROSS_SPORT_MODEL_VS_FADE_NHL_MIN_BETS)" CROSS_SPORT_MODEL_VS_FADE_MLB_MIN_BETS="$(CROSS_SPORT_MODEL_VS_FADE_MLB_MIN_BETS)" CROSS_SPORT_MODEL_VS_FADE_REQUIRE_NHL="$(CROSS_SPORT_MODEL_VS_FADE_REQUIRE_NHL)" CROSS_SPORT_MODEL_VS_FADE_REQUIRE_MLB="$(CROSS_SPORT_MODEL_VS_FADE_REQUIRE_MLB)"

# Backfill MLB historical odds snapshots from OddsAPI into odds_history root.
mlb-odds-backfill-history:
	$(VENV_PY) backend/mlb/scripts/backfill_mlb_oddsapi_history.py \
	  $(if $(strip $(MLB_ODDS_BACKFILL_FROM_DATE)),--from-date "$(MLB_ODDS_BACKFILL_FROM_DATE)",--season $(MLB_ODDS_BACKFILL_SEASON)) \
	  $(if $(strip $(MLB_ODDS_BACKFILL_TO_DATE)),--to-date "$(MLB_ODDS_BACKFILL_TO_DATE)",) \
	  --snapshot-time-et "$(MLB_ODDS_BACKFILL_SNAPSHOT_TIME_ET)" \
	  $(if $(strip $(MLB_ODDS_BACKFILL_MARKETS)),--markets "$(MLB_ODDS_BACKFILL_MARKETS)",) \
	  --regions "$(MLB_ODDS_BACKFILL_REGIONS)" \
	  --max-markets-per-call "$(MLB_ODDS_BACKFILL_MAX_MARKETS_PER_CALL)" \
	  $(if $(strip $(MLB_ODDS_BACKFILL_MAX_DAYS)),--max-days "$(MLB_ODDS_BACKFILL_MAX_DAYS)",) \
	  --sleep-ms "$(MLB_ODDS_BACKFILL_SLEEP_MS)" \
	  $(if $(filter 1,$(MLB_ODDS_BACKFILL_OVERWRITE)),--overwrite,) \
	  $(if $(filter 1,$(MLB_ODDS_BACKFILL_DRY_RUN)),--dry-run,) \
	  --out-root "$(MLB_ODDS_HISTORY_ROOT)"

# Show effective MLB make/runtime values before execution.
mlb-show-config:
	@echo "MLB_DATE=$(MLB_DATE)"
	@echo "MLB_DATE_ET=$(MLB_DATE_ET)"
	@echo "MLB_POST_GRADE_DATE=$(MLB_POST_GRADE_DATE)"
	@echo "MLB_MARKET_DAYS=$(MLB_MARKET_DAYS)"
	@echo "MLB_SLATE_PRED_CSV=$(MLB_SLATE_PRED_CSV)"
	@echo "MLB_SLATE_OUTPUT_CSV=$(MLB_SLATE_OUTPUT_CSV)"
	@echo "MLB_SLATE_PROP_TYPE=$(MLB_SLATE_PROP_TYPE)"
	@echo "MLB_BOOK_UPLOAD_OUT_CSV=$(MLB_BOOK_UPLOAD_OUT_CSV)"
	@echo "MLB_ODDS_HISTORY_ROOT=$(MLB_ODDS_HISTORY_ROOT)"
	@echo "MLB_ODDS_SNAPSHOT_JSON=$(MLB_ODDS_SNAPSHOT_JSON)"
	@echo "MLB_ARCHIVE_RUN_TAG=$(MLB_ARCHIVE_RUN_TAG)"
	@echo "MLB_ODDS_SNAPSHOT_IN=$(MLB_ODDS_SNAPSHOT_IN)"
	@echo "MLB_POLICY_PLAN_ENABLED=$(MLB_POLICY_PLAN_ENABLED)"
	@echo "MLB_POLICY_PLAN_CSV=$(MLB_POLICY_PLAN_CSV)"
	@echo "MLB_POLICY_PLAN_ALLOW_ONE_SIDED=$(MLB_POLICY_PLAN_ALLOW_ONE_SIDED)"
	@echo "MLB_POLICY_PLAN_ALLOW_EMPTY=$(MLB_POLICY_PLAN_ALLOW_EMPTY)"
	@echo "MLB_PREDICT_REQUIRE_TWO_SIDED=$(MLB_PREDICT_REQUIRE_TWO_SIDED)"
	@echo "MLB_PREDICT_TWO_SIDED_BOOKMAKER=$(MLB_PREDICT_TWO_SIDED_BOOKMAKER)"
	@echo "MLB_PREDICT_TWO_SIDED_OPTIONAL_TARGET_BOOK_PROPS=$(MLB_PREDICT_TWO_SIDED_OPTIONAL_TARGET_BOOK_PROPS)"
	@echo "MLB_RECONCILE_FROM_DATE=$(MLB_RECONCILE_FROM_DATE)"
	@echo "MLB_RECONCILE_TO_DATE=$(MLB_RECONCILE_TO_DATE)"
	@echo "MLB_RECONCILE_BOOKMAKER=$(MLB_RECONCILE_BOOKMAKER)"
	@echo "MLB_RECONCILE_ODDS_FILENAME=$(MLB_RECONCILE_ODDS_FILENAME)"
	@echo "MLB_RECONCILE_ROWS_OUT_CSV=$(MLB_RECONCILE_ROWS_OUT_CSV)"
	@echo "MLB_RECONCILE_SUMMARY_OUT_JSON=$(MLB_RECONCILE_SUMMARY_OUT_JSON)"
	@echo "MLB_RECONCILE_REQUIRE_TWO_SIDED=$(MLB_RECONCILE_REQUIRE_TWO_SIDED)"
	@echo "MLB_RECONCILE_REQUIRE_OUTCOMES=$(MLB_RECONCILE_REQUIRE_OUTCOMES)"
	@echo "MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN=$(MLB_RECONCILE_REQUIRE_OUTCOME_ROWS_MIN)"
	@echo "MLB_POST_GRADE_REQUIRE_OUTCOMES=$(MLB_POST_GRADE_REQUIRE_OUTCOMES)"
	@echo "MLB_POST_GRADE_REQUIRE_OUTCOME_ROWS_MIN=$(MLB_POST_GRADE_REQUIRE_OUTCOME_ROWS_MIN)"
	@echo "MLB_MODEL_VS_FADE_ROWS_CSV=$(MLB_MODEL_VS_FADE_ROWS_CSV)"
	@echo "MLB_MODEL_VS_FADE_OUT_JSON=$(MLB_MODEL_VS_FADE_OUT_JSON)"
	@echo "MLB_MODEL_VS_FADE_OUT_CSV=$(MLB_MODEL_VS_FADE_OUT_CSV)"
	@echo "MLB_MODEL_VS_FADE_MIN_BETS_ALERT=$(MLB_MODEL_VS_FADE_MIN_BETS_ALERT)"
	@echo "MLB_ALL_AVAILABLE_ROWS_CSV=$(MLB_ALL_AVAILABLE_ROWS_CSV)"
	@echo "MLB_ALL_AVAILABLE_OUT_JSON=$(MLB_ALL_AVAILABLE_OUT_JSON)"
	@echo "MLB_ALL_AVAILABLE_OUT_CSV=$(MLB_ALL_AVAILABLE_OUT_CSV)"
	@echo "MLB_POSTGRADE_TRACKER_DATE=$(MLB_POSTGRADE_TRACKER_DATE)"
	@echo "MLB_POSTGRADE_TRACKER_OUT_CSV=$(MLB_POSTGRADE_TRACKER_OUT_CSV)"
	@echo "MLB_POSTGRADE_TRACKER_OUT_BY_PROP_CSV=$(MLB_POSTGRADE_TRACKER_OUT_BY_PROP_CSV)"
	@echo "MLB_POSTGRADE_TRACKER_CHARTS_DIR=$(MLB_POSTGRADE_TRACKER_CHARTS_DIR)"
	@echo "MLB_POSTGRADE_TRACKER_SKIP_CHARTS=$(MLB_POSTGRADE_TRACKER_SKIP_CHARTS)"
	@echo "MLB_POSTGRADE_ALLOW_GRADED_DATE_MISMATCH=$(MLB_POSTGRADE_ALLOW_GRADED_DATE_MISMATCH)"
	@echo "MLB_POSTGRADE_ALERTS_OUT_JSON=$(MLB_POSTGRADE_ALERTS_OUT_JSON)"
	@echo "MLB_POSTGRADE_ALERTS_HISTORY_JSONL=$(MLB_POSTGRADE_ALERTS_HISTORY_JSONL)"
	@echo "MLB_POSTGRADE_ALERT_FADE_MIN_PAIRED_BETS=$(MLB_POSTGRADE_ALERT_FADE_MIN_PAIRED_BETS)"
	@echo "MLB_POSTGRADE_ALERT_ROI_MIN_PAIRED_BETS=$(MLB_POSTGRADE_ALERT_ROI_MIN_PAIRED_BETS)"
	@echo "MLB_POSTGRADE_ALERT_ROI_BREACH_THRESHOLD=$(MLB_POSTGRADE_ALERT_ROI_BREACH_THRESHOLD)"
	@echo "MLB_POSTGRADE_ALERT_OVERALL_DROP_WINDOW_DAYS=$(MLB_POSTGRADE_ALERT_OVERALL_DROP_WINDOW_DAYS)"
	@echo "MLB_POSTGRADE_ALERT_OVERALL_DROP_THRESHOLD_PCT=$(MLB_POSTGRADE_ALERT_OVERALL_DROP_THRESHOLD_PCT)"
	@echo "MLB_POSTGRADE_ALERT_PROP_DROP_WINDOW_DAYS=$(MLB_POSTGRADE_ALERT_PROP_DROP_WINDOW_DAYS)"
	@echo "MLB_POSTGRADE_ALERT_PROP_DROP_THRESHOLD_PCT=$(MLB_POSTGRADE_ALERT_PROP_DROP_THRESHOLD_PCT)"
	@echo "MLB_POSTGRADE_ALERT_PROP_DROP_MIN_MODEL_ROWS=$(MLB_POSTGRADE_ALERT_PROP_DROP_MIN_MODEL_ROWS)"
	@echo "MLB_POSTGRADE_ALERTS_STRICT=$(MLB_POSTGRADE_ALERTS_STRICT)"
	@echo "MLB_POLICY_REPLAY_ROWS_CSV=$(MLB_POLICY_REPLAY_ROWS_CSV)"
	@echo "MLB_POLICY_REPLAY_OUT_DIR=$(MLB_POLICY_REPLAY_OUT_DIR)"
	@echo "MLB_POLICY_MONITOR_PROPS=$(MLB_POLICY_MONITOR_PROPS)"
	@echo "MLB_POLICY_MONITOR_MIN_BETS_ALERT=$(MLB_POLICY_MONITOR_MIN_BETS_ALERT)"
	@echo "MLB_ODDS_BACKFILL_SEASON=$(MLB_ODDS_BACKFILL_SEASON)"
	@echo "MLB_ODDS_BACKFILL_FROM_DATE=$(MLB_ODDS_BACKFILL_FROM_DATE)"
	@echo "MLB_ODDS_BACKFILL_TO_DATE=$(MLB_ODDS_BACKFILL_TO_DATE)"
	@echo "MLB_ODDS_BACKFILL_SNAPSHOT_TIME_ET=$(MLB_ODDS_BACKFILL_SNAPSHOT_TIME_ET)"
	@echo "MLB_ODDS_BACKFILL_MARKETS=$(MLB_ODDS_BACKFILL_MARKETS)"
	@echo "MLB_ODDS_BACKFILL_REGIONS=$(MLB_ODDS_BACKFILL_REGIONS)"
	@echo "MLB_ODDS_BACKFILL_MAX_MARKETS_PER_CALL=$(MLB_ODDS_BACKFILL_MAX_MARKETS_PER_CALL)"
	@echo "MLB_ODDS_BACKFILL_MAX_DAYS=$(MLB_ODDS_BACKFILL_MAX_DAYS)"
	@echo "MLB_ODDS_BACKFILL_SLEEP_MS=$(MLB_ODDS_BACKFILL_SLEEP_MS)"
	@echo "MLB_ODDS_BACKFILL_OVERWRITE=$(MLB_ODDS_BACKFILL_OVERWRITE)"
	@echo "MLB_ODDS_BACKFILL_DRY_RUN=$(MLB_ODDS_BACKFILL_DRY_RUN)"
	@echo "MLB_WIDE_PROP_TYPES=$(MLB_WIDE_PROP_TYPES)"
	@echo "MLB_WIDE_REQUIRE_MIN_ROWS=$(MLB_WIDE_REQUIRE_MIN_ROWS)"
	@echo "MLB_DAILY_INCLUDE_CAPTURE=$(MLB_DAILY_INCLUDE_CAPTURE)"
	@echo "MLB_DAILY_BVP_IMPACT_ENABLED=$(MLB_DAILY_BVP_IMPACT_ENABLED)"
	@echo "MLB_DAILY_BVP_IMPACT_REQUIRED=$(MLB_DAILY_BVP_IMPACT_REQUIRED)"
	@echo "MLB_DAILY_BVP_PVB_ENABLED=$(MLB_DAILY_BVP_PVB_ENABLED)"
	@echo "MLB_BVP_DATE=$(MLB_BVP_DATE)"
	@echo "MLB_BVP_FROM_DATE=$(MLB_BVP_FROM_DATE)"
	@echo "MLB_BVP_TO_DATE=$(MLB_BVP_TO_DATE)"
	@echo "MLB_BVP_FEATURE_SET_TAG=$(MLB_BVP_FEATURE_SET_TAG)"
	@echo "MLB_BVP_MODEL_TAG=$(MLB_BVP_MODEL_TAG)"
	@echo "MLB_BVP_BATCH_SIZE=$(MLB_BVP_BATCH_SIZE)"
	@echo "MLB_BVP_REQUEST_TIMEOUT_SEC=$(MLB_BVP_REQUEST_TIMEOUT_SEC)"
	@echo "MLB_BVP_REQUEST_RETRIES=$(MLB_BVP_REQUEST_RETRIES)"
	@echo "MLB_BVP_DRY_RUN=$(MLB_BVP_DRY_RUN)"
	@echo "MLB_BVP_IMPACT_SLATE_CSV=$(MLB_BVP_IMPACT_SLATE_CSV)"
	@echo "MLB_BVP_IMPACT_WIDE_CSV=$(MLB_BVP_IMPACT_WIDE_CSV)"
	@echo "MLB_BVP_IMPACT_OUT_JSON=$(MLB_BVP_IMPACT_OUT_JSON)"
	@echo "MLB_BVP_IMPACT_OUT_CSV=$(MLB_BVP_IMPACT_OUT_CSV)"
	@echo "MLB_BVP_IMPACT_HISTORY_JSONL=$(MLB_BVP_IMPACT_HISTORY_JSONL)"
	@echo "MLB_BVP_IMPACT_LABEL_DATE=$(MLB_BVP_IMPACT_LABEL_DATE)"
	@echo "MLB_BVP_IMPACT_MAX_ROWS=$(MLB_BVP_IMPACT_MAX_ROWS)"
	@echo "MLB_BVP_IMPACT_REQUIRE_DB=$(MLB_BVP_IMPACT_REQUIRE_DB)"
	@echo "MLB_ROSTER_DATE=$(MLB_ROSTER_DATE)"
	@echo "MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO)"
	@echo "MLB_STAT_FROM_DATE=$(MLB_STAT_FROM_DATE)"
	@echo "MLB_STAT_TO_DATE=$(MLB_STAT_TO_DATE)"
	@echo "MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES)"
	@echo "MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES)"
	@echo "MLB_STAT_BATTER_SAMPLE_RATIO=$(MLB_STAT_BATTER_SAMPLE_RATIO)"
	@echo "MLB_SEASON_REQUIRE_REGULAR=$(MLB_SEASON_REQUIRE_REGULAR)"
	@echo "MLB_PRESEASON_FROM_DATE=$(MLB_PRESEASON_FROM_DATE)"
	@echo "MLB_PRESEASON_TO_DATE=$(MLB_PRESEASON_TO_DATE)"
	@echo "MLB_PRESEASON_INCLUDE_USER_ADDED=$(MLB_PRESEASON_INCLUDE_USER_ADDED)"
	@echo "MLB_PRESEASON_GAME_TYPES=$(MLB_PRESEASON_GAME_TYPES)"
	@echo "MLB_ONE_SIDED_CLEANUP_SCHEMA=$(MLB_ONE_SIDED_CLEANUP_SCHEMA)"
	@echo "MLB_ONE_SIDED_CLEANUP_TABLES=$(MLB_ONE_SIDED_CLEANUP_TABLES)"
	@echo "MLB_ONE_SIDED_CLEANUP_OUT_JSON=$(MLB_ONE_SIDED_CLEANUP_OUT_JSON)"
	@echo "MLB_ONE_SIDED_CLEANUP_APPLY=$(MLB_ONE_SIDED_CLEANUP_APPLY)"
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
	@echo "MLB_QUALITY_SOURCE_TABLE=$(MLB_QUALITY_SOURCE_TABLE)"
	@echo "MLB_QUALITY_ROWS_CSV=$(MLB_QUALITY_ROWS_CSV)"
	@echo "MLB_QUALITY_PROP_SOURCES=$(MLB_QUALITY_PROP_SOURCES)"
	@echo "MLB_QUALITY_RECONCILE_REQUIRE_TWO_SIDED=$(MLB_QUALITY_RECONCILE_REQUIRE_TWO_SIDED)"
	@echo "MLB_ODDS_BUCKET_REQUIRE_TWO_SIDED=$(MLB_ODDS_BUCKET_REQUIRE_TWO_SIDED)"
	@echo "MLB_PROP_COVERAGE_WINDOW_DAYS=$(MLB_PROP_COVERAGE_WINDOW_DAYS)"
	@echo "MLB_PROP_COVERAGE_WINDOW_MODE=$(MLB_PROP_COVERAGE_WINDOW_MODE)"
	@echo "MLB_PROP_COVERAGE_GAMES_BACK=$(MLB_PROP_COVERAGE_GAMES_BACK)"
	@echo "MLB_PROP_COVERAGE_REQUIRED=$(MLB_PROP_COVERAGE_REQUIRED)"
	@echo "MLB_PROP_COVERAGE_MIN_GRADED=$(MLB_PROP_COVERAGE_MIN_GRADED)"
	@echo "MLB_PROP_COVERAGE_GATE_METRIC=$(MLB_PROP_COVERAGE_GATE_METRIC)"
	@echo "MLB_PROP_COVERAGE_TRAINING_SOURCES=$(MLB_PROP_COVERAGE_TRAINING_SOURCES)"
	@echo "MLB_CORE_PROP_TYPES=$(MLB_CORE_PROP_TYPES)"
	@echo "MLB_PROD8_PROP_TYPES=$(MLB_PROD8_PROP_TYPES)"
	@echo "MLB_PROD12_PROP_TYPES=$(MLB_PROD12_PROP_TYPES)"
	@echo "MLB_DEGENERATE_PROP_TYPES=$(MLB_DEGENERATE_PROP_TYPES)"
	@echo "MLB_CORE_MIN_GRADED=$(MLB_CORE_MIN_GRADED)"
	@echo "MLB_CORE_TRAINING_SOURCES=$(MLB_CORE_TRAINING_SOURCES)"
	@echo "MLB_TRAIN_FEATURE_SOURCE=$(MLB_TRAIN_FEATURE_SOURCE)"
	@echo "MLB_TRAIN_PROFILE=$(MLB_TRAIN_PROFILE)"
	@echo "MLB_TRAIN_MARKET_ONLY=$(MLB_TRAIN_MARKET_ONLY)"
	@echo "MLB_TRAIN_RECONCILE_ROWS_CSV=$(MLB_TRAIN_RECONCILE_ROWS_CSV)"
	@echo "MLB_TRAIN_RECONCILE_BOOKMAKER=$(MLB_TRAIN_RECONCILE_BOOKMAKER)"
	@echo "MLB_TRAIN_RECONCILE_REQUIRE_TWO_SIDED=$(MLB_TRAIN_RECONCILE_REQUIRE_TWO_SIDED)"
	@echo "MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE=$(MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE)"
	@echo "MLB_TRAIN_MIN_CLASS_COUNT=$(MLB_TRAIN_MIN_CLASS_COUNT)"
	@echo "MLB_TRAIN_MIN_MINORITY_PCT=$(MLB_TRAIN_MIN_MINORITY_PCT)"
	@echo "MLB_RETRAIN_BOL_PROP_TYPES=$(MLB_RETRAIN_BOL_PROP_TYPES)"
	@echo "MLB_RETRAIN_BOL_DAYS_BACK=$(MLB_RETRAIN_BOL_DAYS_BACK)"
	@echo "MLB_RETRAIN_BOL_TRAIN_LIMIT=$(MLB_RETRAIN_BOL_TRAIN_LIMIT)"
	@echo "MLB_RETRAIN_BROAD_PROP_TYPES=$(MLB_RETRAIN_BROAD_PROP_TYPES)"
	@echo "MLB_RETRAIN_BROAD_DAYS_BACK=$(MLB_RETRAIN_BROAD_DAYS_BACK)"
	@echo "MLB_RETRAIN_BROAD_TRAIN_LIMIT=$(MLB_RETRAIN_BROAD_TRAIN_LIMIT)"
	@echo "MLB_RETRAIN_BROAD_RECOMPUTE_DAYS_BACK=$(MLB_RETRAIN_BROAD_RECOMPUTE_DAYS_BACK)"
	@echo "MLB_RETRAIN_BROAD_RECOMPUTE_LIMIT=$(MLB_RETRAIN_BROAD_RECOMPUTE_LIMIT)"
	@echo "MLB_RECOMPUTE_REQUIRE_REGULAR=$(MLB_RECOMPUTE_REQUIRE_REGULAR)"
	@echo "MLB_RECOMPUTE_FORCE_INVERT_PROPS=$(MLB_RECOMPUTE_FORCE_INVERT_PROPS)"
	@echo "MLB_CANDIDATE_SOURCE_TABLE=$(MLB_CANDIDATE_SOURCE_TABLE)"
	@echo "MLB_CANDIDATE_ROWS_CSV=$(MLB_CANDIDATE_ROWS_CSV)"
	@echo "MLB_PROD12_CANDIDATE_PROP_TYPES=$(MLB_PROD12_CANDIDATE_PROP_TYPES)"
	@echo "MLB_PROD12_CANDIDATE_REQUIRED_PROPS=$(MLB_PROD12_CANDIDATE_REQUIRED_PROPS)"
	@echo "MLB_PROD12_CANDIDATE_SOURCE_TABLE=$(MLB_PROD12_CANDIDATE_SOURCE_TABLE)"
	@echo "MLB_PROD12_CANDIDATE_ROWS_CSV=$(MLB_PROD12_CANDIDATE_ROWS_CSV)"

# JSON snapshot for MLB readiness signals (stat-derived + roster freshness).
mlb-readiness-snapshot:
	$(VENV_PY) backend/mlb/scripts/mlb_readiness_snapshot.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

mlb-readiness-log:
	$(VENV_PY) backend/mlb/scripts/mlb_readiness_log.py --stat-days $(MLB_STAT_DERIVED_DAYS) --stat-require-min $(MLB_STAT_DERIVED_MIN)

mlb-readiness-last:
	$(VENV_PY) backend/mlb/scripts/mlb_readiness_last.py --limit 10

mlb-prediction-readiness:
	$(VENV_PY) backend/mlb/scripts/probe_mlb_prediction_readiness.py --date $(MLB_DATE) --sample-size $(MLB_PREDICT_SAMPLE) --require-min-success $(MLB_PREDICT_MIN_SUCCESS) --prop-types "$(MLB_PREDICT_PROP_TYPES)"

mlb-prediction-quality:
	$(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality.py --window-mode $(MLB_QUALITY_WINDOW_MODE) --window-days $(MLB_QUALITY_WINDOW_DAYS) --games-back $(MLB_QUALITY_GAMES_BACK) --source-table "$(MLB_QUALITY_SOURCE_TABLE)" $(if $(MLB_QUALITY_ROWS_CSV),--rows-csv "$(MLB_QUALITY_ROWS_CSV)",) $(MLB_QUALITY_RECONCILE_TWO_SIDED_ARG) --prop-types "$(MLB_PREDICT_PROP_TYPES)" --prop-sources "$(MLB_QUALITY_PROP_SOURCES)" --min-total $(MLB_QUALITY_MIN_TOTAL)

mlb-prediction-quality-core:
	$(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality.py --window-mode games --games-back $(MLB_QUALITY_GAMES_BACK) --source-table "$(MLB_QUALITY_SOURCE_TABLE)" $(if $(MLB_QUALITY_ROWS_CSV),--rows-csv "$(MLB_QUALITY_ROWS_CSV)",) $(MLB_QUALITY_RECONCILE_TWO_SIDED_ARG) --prop-types "$(MLB_CORE_PROP_TYPES)" --prop-sources "$(MLB_QUALITY_PROP_SOURCES)" --min-total $(MLB_QUALITY_MIN_TOTAL)

mlb-prediction-quality-prod8:
	$(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality.py --window-mode games --games-back $(MLB_QUALITY_GAMES_BACK) --source-table "$(MLB_QUALITY_SOURCE_TABLE)" $(if $(MLB_QUALITY_ROWS_CSV),--rows-csv "$(MLB_QUALITY_ROWS_CSV)",) $(MLB_QUALITY_RECONCILE_TWO_SIDED_ARG) --prop-types "$(MLB_PROD8_PROP_TYPES)" --prop-sources "$(MLB_QUALITY_PROP_SOURCES)" --min-total $(MLB_QUALITY_MIN_TOTAL)

mlb-prediction-quality-prod12:
	$(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality.py --window-mode games --games-back $(MLB_QUALITY_GAMES_BACK) --source-table "$(MLB_QUALITY_SOURCE_TABLE)" $(if $(MLB_QUALITY_ROWS_CSV),--rows-csv "$(MLB_QUALITY_ROWS_CSV)",) $(MLB_QUALITY_RECONCILE_TWO_SIDED_ARG) --prop-types "$(MLB_PROD12_PROP_TYPES)" --prop-sources "$(MLB_QUALITY_PROP_SOURCES)" --min-total $(MLB_QUALITY_MIN_TOTAL)

mlb-recompute-training-predictions:
	MLB_FORCE_INVERT_PROPS="$(MLB_RECOMPUTE_FORCE_INVERT_PROPS)" $(VENV_PY) backend/_legacy/scripts/recompute_mlb_training_predictions.py --days-back $(MLB_RECOMPUTE_DAYS_BACK) --prop-types "$(MLB_RECOMPUTE_PROP_TYPES)" --prop-source "$(MLB_RECOMPUTE_PROP_SOURCE)" --from-date "$(MLB_RECOMPUTE_FROM_DATE)" --to-date "$(MLB_RECOMPUTE_TO_DATE)" --limit $(MLB_RECOMPUTE_LIMIT) $(MLB_RECOMPUTE_REQUIRE_REGULAR_ARG)

mlb-corrected-props-recompute:
	@set -e; \
	if [ -z "$$DATABASE_URL" ] && [ -z "$$SUPABASE_DB_URL" ]; then \
		echo "mlb-corrected-props-recompute requires DATABASE_URL or SUPABASE_DB_URL"; \
		exit 2; \
	fi; \
	if [ -z "$$MODEL_DIR" ]; then \
		echo "mlb-corrected-props-recompute requires MODEL_DIR (directory containing feature_metadata.json and prop model artifacts)"; \
		exit 2; \
	fi; \
	MLB_FORCE_INVERT_PROPS="$(MLB_RECOMPUTE_FORCE_INVERT_PROPS)" $(VENV_PY) backend/_legacy/scripts/recompute_mlb_training_predictions.py --days-back "$(MLB_RECOMPUTE_DAYS_BACK)" --prop-types "$(MLB_CORRECTED_PROP_TYPES)" --prop-source "$(MLB_RECOMPUTE_PROP_SOURCE)" --from-date "$(MLB_RECOMPUTE_FROM_DATE)" --to-date "$(MLB_RECOMPUTE_TO_DATE)" --limit "$(MLB_RECOMPUTE_LIMIT)" $(MLB_RECOMPUTE_REQUIRE_REGULAR_ARG); \
	$(MAKE) mlb-prediction-quality-prod12 MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)"

mlb-corrected-props-recompute-gated:
	@set -e; \
	if [ -z "$$DATABASE_URL" ] && [ -z "$$SUPABASE_DB_URL" ]; then \
		echo "mlb-corrected-props-recompute-gated requires DATABASE_URL or SUPABASE_DB_URL"; \
		exit 2; \
	fi; \
	if [ -z "$$MODEL_DIR" ]; then \
		echo "mlb-corrected-props-recompute-gated requires MODEL_DIR (directory containing feature_metadata.json and prop model artifacts)"; \
		exit 2; \
	fi; \
	MLB_FORCE_INVERT_PROPS="$(MLB_RECOMPUTE_FORCE_INVERT_PROPS)" $(VENV_PY) backend/_legacy/scripts/recompute_mlb_training_predictions.py --days-back "$(MLB_RECOMPUTE_DAYS_BACK)" --prop-types "$(MLB_CORRECTED_PROP_TYPES)" --prop-source "$(MLB_RECOMPUTE_PROP_SOURCE)" --from-date "$(MLB_RECOMPUTE_FROM_DATE)" --to-date "$(MLB_RECOMPUTE_TO_DATE)" --limit "$(MLB_RECOMPUTE_LIMIT)" --gate-min-total-per-prop "$(MLB_RECOMPUTE_GATE_MIN_TOTAL_PER_PROP)" --gate-min-accuracy-pct "$(MLB_RECOMPUTE_GATE_MIN_ACCURACY_PCT)" $(MLB_RECOMPUTE_REQUIRE_REGULAR_ARG); \
	$(MAKE) mlb-prediction-quality-prod12 MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)"

mlb-corrected-props-recompute-gated-batched:
	@set -e; \
	if [ -z "$$DATABASE_URL" ] && [ -z "$$SUPABASE_DB_URL" ]; then \
		echo "mlb-corrected-props-recompute-gated-batched requires DATABASE_URL or SUPABASE_DB_URL"; \
		exit 2; \
	fi; \
	if [ -z "$$MODEL_DIR" ]; then \
		echo "mlb-corrected-props-recompute-gated-batched requires MODEL_DIR (directory containing feature_metadata.json and prop model artifacts)"; \
		exit 2; \
	fi; \
	batch_props="$(MLB_RECOMPUTE_BATCH_PROP_TYPES)"; \
	OLD_IFS="$$IFS"; IFS=','; \
		for prop in $$batch_props; do \
			prop=$$(echo "$$prop" | xargs); \
			if [ -z "$$prop" ]; then continue; fi; \
			echo "==> recompute gated batch prop=$$prop"; \
				gate_min_acc="$(MLB_RECOMPUTE_GATE_MIN_ACCURACY_PCT)"; \
				case "$$prop" in \
					hits_runs_rbis) gate_min_acc="$(MLB_RECOMPUTE_GATE_MIN_ACCURACY_HITS_RUNS_RBIS_PCT)" ;; \
				esac; \
				case ",$(MLB_RECOMPUTE_NON_BLOCKING_PROPS)," in *,"$$prop",*) gate_min_acc="-1" ;; esac; \
				MLB_FORCE_INVERT_PROPS="$(MLB_RECOMPUTE_FORCE_INVERT_PROPS)" $(VENV_PY) backend/_legacy/scripts/recompute_mlb_training_predictions.py --days-back "$(MLB_RECOMPUTE_DAYS_BACK)" --prop-types "$$prop" --prop-source "$(MLB_RECOMPUTE_PROP_SOURCE)" --from-date "$(MLB_RECOMPUTE_FROM_DATE)" --to-date "$(MLB_RECOMPUTE_TO_DATE)" --limit "$(MLB_RECOMPUTE_LIMIT)" --gate-min-total-per-prop "$(MLB_RECOMPUTE_GATE_MIN_TOTAL_PER_PROP)" --gate-min-accuracy-pct "$$gate_min_acc" $(MLB_RECOMPUTE_REQUIRE_REGULAR_ARG) || exit $$?; \
		done; \
	IFS="$$OLD_IFS"; \
	$(MAKE) mlb-prediction-quality-prod12 MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)"

mlb-hybrid-window-refresh:
	@set -e; \
	if [ -z "$$DATABASE_URL" ] && [ -z "$$SUPABASE_DB_URL" ]; then \
		echo "mlb-hybrid-window-refresh requires DATABASE_URL or SUPABASE_DB_URL"; \
		exit 2; \
	fi; \
	if [ -z "$$MODEL_DIR" ]; then \
		echo "mlb-hybrid-window-refresh requires MODEL_DIR (directory containing feature_metadata.json and prop model artifacts)"; \
		exit 2; \
	fi; \
	if [ -z "$$SUPABASE_URL" ]; then \
		echo "mlb-hybrid-window-refresh requires SUPABASE_URL for model trainer"; \
		exit 2; \
	fi; \
	if [ -z "$$SUPABASE_SERVICE_ROLE_KEY" ] && [ -z "$$SUPABASE_ANON_KEY" ]; then \
		echo "mlb-hybrid-window-refresh requires SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) for model trainer"; \
		exit 2; \
	fi; \
	if [ ! -f "$(MLB_TRAIN_RECONCILE_ROWS_CSV)" ]; then \
		echo "mlb-hybrid-window-refresh missing reconcile rows csv: $(MLB_TRAIN_RECONCILE_ROWS_CSV)"; \
		echo "build it first with make mlb-reconcile-rows MLB_RECONCILE_BOOKMAKER=betonlineag MLB_RECONCILE_REQUIRE_TWO_SIDED=1"; \
		exit 2; \
	fi; \
	hybrid_pairs="$(MLB_HYBRID_PROP_WINDOWS)"; \
	OLD_IFS="$$IFS"; IFS=','; \
	for pair in $$hybrid_pairs; do \
		pair=$$(echo "$$pair" | xargs); \
		if [ -z "$$pair" ]; then continue; fi; \
		prop="$${pair%%:*}"; \
		days_back="$${pair##*:}"; \
		if [ "$$prop" = "$$days_back" ]; then \
			echo "invalid MLB_HYBRID_PROP_WINDOWS item: $$pair (expected prop:days)"; \
			exit 2; \
		fi; \
		echo "==> hybrid train prop=$$prop days_back=$$days_back"; \
		$(VENV_PY) backend/mlb/model_trainer.py --prop "$$prop" --days-back "$$days_back" --limit "$(MLB_HYBRID_TRAIN_LIMIT)" || exit $$?; \
			if [ ! -f "$$MODEL_DIR/latest/$$prop.joblib" ]; then \
				echo "==> hybrid recompute skipped prop=$$prop (no model artifact at $$MODEL_DIR/latest/$$prop.joblib)"; \
				continue; \
			fi; \
			echo "==> hybrid recompute prop=$$prop"; \
			gate_min_acc="$(MLB_RECOMPUTE_GATE_MIN_ACCURACY_PCT)"; \
			case "$$prop" in \
				hits_runs_rbis) gate_min_acc="$(MLB_RECOMPUTE_GATE_MIN_ACCURACY_HITS_RUNS_RBIS_PCT)" ;; \
			esac; \
			case ",$(MLB_RECOMPUTE_NON_BLOCKING_PROPS)," in *,"$$prop",*) gate_min_acc="-1" ;; esac; \
			MLB_FORCE_INVERT_PROPS="$(MLB_RECOMPUTE_FORCE_INVERT_PROPS)" $(VENV_PY) backend/_legacy/scripts/recompute_mlb_training_predictions.py --days-back "$(MLB_HYBRID_RECOMPUTE_DAYS_BACK)" --prop-types "$$prop" --prop-source "$(MLB_RECOMPUTE_PROP_SOURCE)" --from-date "$(MLB_RECOMPUTE_FROM_DATE)" --to-date "$(MLB_RECOMPUTE_TO_DATE)" --limit "$(MLB_HYBRID_RECOMPUTE_LIMIT)" --gate-min-total-per-prop "$(MLB_RECOMPUTE_GATE_MIN_TOTAL_PER_PROP)" --gate-min-accuracy-pct "$$gate_min_acc" $(MLB_RECOMPUTE_REQUIRE_REGULAR_ARG) || exit $$?; \
		done; \
	IFS="$$OLD_IFS"; \
	$(MAKE) mlb-prediction-quality-prod12 MLB_QUALITY_SOURCE_TABLE="reconcile_rows" MLB_QUALITY_ROWS_CSV="$(MLB_TRAIN_RECONCILE_ROWS_CSV)" MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_PROP_SOURCES="" MLB_QUALITY_MIN_TOTAL="$(MLB_RETRAIN_QUALITY_MIN_TOTAL)"; \
	$(MAKE) mlb-candidate-eval-prod12 MLB_PROD12_CANDIDATE_SOURCE_TABLE="reconcile_rows" MLB_PROD12_CANDIDATE_ROWS_CSV="$(MLB_TRAIN_RECONCILE_ROWS_CSV)" MLB_PROD12_MIN_LIFT_PCT="$(MLB_PROD12_MIN_LIFT_PCT)" MLB_PROD12_MAX_PROP_DROP_PCT="$(MLB_PROD12_MAX_PROP_DROP_PCT)"

# Brand-new market-native retrain lane:
# - BetOnline-only
# - two-sided prices required
# - no fallback to legacy base merge
# - no derived/BvP feature hydration during training
mlb-retrain-bol-market-only:
	@set -e; \
	model_root="$(MLB_MODEL_ROOT)"; \
	if [ -z "$$MODEL_DIR" ] && [ "$$model_root" = "/var/data/proppadia/models" ]; then \
		echo "mlb-retrain-bol-market-only: MODEL_DIR unset; local fallback to $(CURDIR)/models_out"; \
		model_root="$(CURDIR)/models_out"; \
	fi; \
	mkdir -p "$$model_root" "$$model_root/latest" "$$model_root/archive"; \
	if [ ! -f "$(MLB_TRAIN_RECONCILE_ROWS_CSV)" ]; then \
		echo "mlb-retrain-bol-market-only missing reconcile rows csv: $(MLB_TRAIN_RECONCILE_ROWS_CSV)"; \
		echo "build it first with make mlb-reconcile-rows MLB_RECONCILE_BOOKMAKER=betonlineag MLB_RECONCILE_REQUIRE_TWO_SIDED=1"; \
		exit 2; \
	fi; \
	props="$(MLB_RETRAIN_BOL_PROP_TYPES)"; \
	OLD_IFS="$$IFS"; IFS=','; \
	for prop in $$props; do \
		prop=$$(echo "$$prop" | xargs); \
		if [ -z "$$prop" ]; then continue; fi; \
		echo "==> bol-market-only train prop=$$prop rows=$(MLB_TRAIN_RECONCILE_ROWS_CSV) bookmaker=betonlineag model_root=$$model_root"; \
		MODEL_DIR="$$model_root" MODELS_DIR="$$model_root" TRAIN_FEATURE_SOURCE="reconcile_csv" MLB_TRAIN_PROFILE="market_only" MLB_TRAIN_MARKET_ONLY="1" MLB_TRAIN_RECONCILE_ROWS_CSV="$(MLB_TRAIN_RECONCILE_ROWS_CSV)" MLB_TRAIN_RECONCILE_BOOKMAKER="betonlineag" MLB_TRAIN_RECONCILE_REQUIRE_TWO_SIDED="1" MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE="0" MIN_CLASS_COUNT="$(MLB_TRAIN_MIN_CLASS_COUNT)" MIN_MINORITY_PCT="$(MLB_TRAIN_MIN_MINORITY_PCT)" $(VENV_PY) backend/mlb/model_trainer.py --prop "$$prop" --days-back "$(MLB_RETRAIN_BOL_DAYS_BACK)" --limit "$(MLB_RETRAIN_BOL_TRAIN_LIMIT)" || exit $$?; \
	done; \
	IFS="$$OLD_IFS"; \
	$(MAKE) mlb-model-artifact-validate MODEL_DIR="$$model_root" MODELS_DIR="$$model_root" MLB_PREDICT_PROP_TYPES="$(MLB_RETRAIN_BOL_PROP_TYPES)"

mlb-retrain-broad-reconcile:
	@set -e; \
	model_root="$(MLB_MODEL_ROOT)"; \
	if [ -z "$$MODEL_DIR" ] && [ "$$model_root" = "/var/data/proppadia/models" ]; then \
		echo "mlb-retrain-broad-reconcile: MODEL_DIR unset; local fallback to $(CURDIR)/models_out"; \
		model_root="$(CURDIR)/models_out"; \
	fi; \
	if [ -z "$$DATABASE_URL" ] && [ -z "$$SUPABASE_DB_URL" ]; then \
		echo "mlb-retrain-broad-reconcile requires DATABASE_URL or SUPABASE_DB_URL"; \
		exit 2; \
	fi; \
	mkdir -p "$$model_root" "$$model_root/latest" "$$model_root/archive"; \
	if [ ! -f "$(MLB_TRAIN_RECONCILE_ROWS_CSV)" ]; then \
		echo "mlb-retrain-broad-reconcile missing reconcile rows csv: $(MLB_TRAIN_RECONCILE_ROWS_CSV)"; \
		echo "build it first with make mlb-reconcile-rows MLB_RECONCILE_BOOKMAKER=betonlineag MLB_RECONCILE_REQUIRE_TWO_SIDED=1"; \
		exit 2; \
	fi; \
	props="$(MLB_RETRAIN_BROAD_RECONCILE_PROP_TYPES)"; \
	failed_props=""; \
	OLD_IFS="$$IFS"; IFS=','; \
	for prop in $$props; do \
		prop=$$(echo "$$prop" | xargs); \
		if [ -z "$$prop" ]; then continue; fi; \
		echo "==> broad train prop=$$prop source=$(MLB_TRAIN_FEATURE_SOURCE) rows=$(MLB_TRAIN_RECONCILE_ROWS_CSV) model_root=$$model_root"; \
		if ! MODEL_DIR="$$model_root" MODELS_DIR="$$model_root" TRAIN_FEATURE_SOURCE="$(MLB_TRAIN_FEATURE_SOURCE)" MLB_TRAIN_PROFILE="$(MLB_TRAIN_PROFILE)" MLB_TRAIN_MARKET_ONLY="$(MLB_TRAIN_MARKET_ONLY)" MLB_TRAIN_RECONCILE_ROWS_CSV="$(MLB_TRAIN_RECONCILE_ROWS_CSV)" MLB_TRAIN_RECONCILE_BOOKMAKER="$(MLB_TRAIN_RECONCILE_BOOKMAKER)" MLB_TRAIN_RECONCILE_REQUIRE_TWO_SIDED="$(MLB_TRAIN_RECONCILE_REQUIRE_TWO_SIDED)" MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE="$(MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE)" MIN_CLASS_COUNT="$(MLB_TRAIN_MIN_CLASS_COUNT)" MIN_MINORITY_PCT="$(MLB_TRAIN_MIN_MINORITY_PCT)" $(VENV_PY) backend/mlb/model_trainer.py --prop "$$prop" --days-back "$(MLB_RETRAIN_BROAD_DAYS_BACK)" --limit "$(MLB_RETRAIN_BROAD_TRAIN_LIMIT)"; then \
			echo "==> broad train failed prop=$$prop"; \
			failed_props="$$failed_props $$prop:train"; \
			continue; \
		fi; \
		if [ ! -f "$$model_root/latest/$$prop.joblib" ]; then \
			echo "==> broad recompute skipped prop=$$prop (no model artifact at $$model_root/latest/$$prop.joblib)"; \
			continue; \
		fi; \
		echo "==> broad recompute prop=$$prop"; \
		gate_min_acc="$(MLB_RECOMPUTE_GATE_MIN_ACCURACY_PCT)"; \
		case "$$prop" in \
			hits_runs_rbis) gate_min_acc="$(MLB_RECOMPUTE_GATE_MIN_ACCURACY_HITS_RUNS_RBIS_PCT)" ;; \
		esac; \
		case ",$(MLB_RECOMPUTE_NON_BLOCKING_PROPS)," in *,"$$prop",*) gate_min_acc="-1" ;; esac; \
		if ! MLB_FORCE_INVERT_PROPS="$(MLB_RECOMPUTE_FORCE_INVERT_PROPS)" MODEL_DIR="$$model_root" MODELS_DIR="$$model_root" $(VENV_PY) backend/_legacy/scripts/recompute_mlb_training_predictions.py --days-back "$(MLB_RETRAIN_BROAD_RECOMPUTE_DAYS_BACK)" --prop-types "$$prop" --prop-source "$(MLB_RECOMPUTE_PROP_SOURCE)" --from-date "$(MLB_RECOMPUTE_FROM_DATE)" --to-date "$(MLB_RECOMPUTE_TO_DATE)" --limit "$(MLB_RETRAIN_BROAD_RECOMPUTE_LIMIT)" --gate-min-total-per-prop "$(MLB_RECOMPUTE_GATE_MIN_TOTAL_PER_PROP)" --gate-min-accuracy-pct "$$gate_min_acc" $(MLB_RECOMPUTE_REQUIRE_REGULAR_ARG); then \
			echo "==> broad recompute failed prop=$$prop"; \
			failed_props="$$failed_props $$prop:recompute"; \
			continue; \
		fi; \
	done; \
	IFS="$$OLD_IFS"; \
	$(MAKE) mlb-prediction-quality-prod12 MLB_QUALITY_SOURCE_TABLE="reconcile_rows" MLB_QUALITY_ROWS_CSV="$(MLB_TRAIN_RECONCILE_ROWS_CSV)" MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_PROP_SOURCES="" MLB_QUALITY_MIN_TOTAL="$(MLB_RETRAIN_QUALITY_MIN_TOTAL)"; \
	$(MAKE) mlb-candidate-eval-prod12 MLB_PROD12_CANDIDATE_SOURCE_TABLE="reconcile_rows" MLB_PROD12_CANDIDATE_ROWS_CSV="$(MLB_TRAIN_RECONCILE_ROWS_CSV)" MLB_PROD12_CANDIDATE_PROP_TYPES="$(MLB_PROD12_CANDIDATE_PROP_TYPES_RECONCILE)" MLB_PROD12_MIN_LIFT_PCT="$(MLB_PROD12_MIN_LIFT_PCT)" MLB_PROD12_MAX_PROP_DROP_PCT="$(MLB_PROD12_MAX_PROP_DROP_PCT)"; \
	if [ -n "$$failed_props" ]; then \
		echo "mlb-retrain-broad-reconcile finished with prop failures:$$failed_props"; \
		exit 1; \
	fi

mlb-model-artifact-validate:
	$(VENV_PY) backend/mlb/scripts/validate_mlb_model_artifacts.py --prop-types "$(MLB_PREDICT_PROP_TYPES)" --min-feature-overlap-pct "$(MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT)"

mlb-model-artifact-validate-prod12:
	$(MAKE) mlb-model-artifact-validate MLB_PREDICT_PROP_TYPES="$(MLB_PROD12_PROP_TYPES)"

mlb-pre-cron-check:
	$(VENV_PY) backend/mlb/scripts/pre_cron_check.py --repo-root "$(CURDIR)" --mlb-date "$(MLB_DATE)" --prod12-prop-types "$(MLB_PROD12_PROP_TYPES)"

mlb-model-snapshot:
	$(VENV_PY) backend/mlb/scripts/mlb_model_snapshot.py --source "$(MLB_MODEL_SNAPSHOT_SOURCE)" --archive-dir "$(MLB_MODEL_ARCHIVE_DIR)" --snapshot-id "$(MLB_MODEL_SNAPSHOT_ID)" --manifest-output "$(MLB_MODEL_MANIFEST_OUTPUT)" --copy

mlb-model-publish:
	@if [ -z "$(MLB_MODEL_PUBLISH_SNAPSHOT)" ]; then \
		echo "mlb-model-publish requires MLB_MODEL_PUBLISH_SNAPSHOT=<snapshot_id>"; \
		exit 2; \
	fi
	$(VENV_PY) backend/mlb/scripts/mlb_model_publish.py --archive-dir "$(MLB_MODEL_ARCHIVE_DIR)" --snapshot-id "$(MLB_MODEL_PUBLISH_SNAPSHOT)" --latest-dir "$(MLB_MODEL_LATEST_DIR)"

mlb-model-prune:
	$(VENV_PY) backend/mlb/scripts/mlb_model_prune.py --archive-dir "$(MLB_MODEL_ARCHIVE_DIR)" --keep "$(MLB_MODEL_PRUNE_KEEP)" $(if $(filter 1,$(MLB_MODEL_PRUNE_DRY_RUN)),--dry-run,)

mlb-model-rollback:
	@if [ -z "$(MLB_MODEL_ROLLBACK_SNAPSHOT)" ]; then \
		echo "mlb-model-rollback requires MLB_MODEL_ROLLBACK_SNAPSHOT=<snapshot_id>"; \
		exit 2; \
	fi
	$(VENV_PY) backend/mlb/scripts/mlb_model_rollback.py --archive-dir "$(MLB_MODEL_ARCHIVE_DIR)" --snapshot-id "$(MLB_MODEL_ROLLBACK_SNAPSHOT)" --latest-dir "$(MLB_MODEL_LATEST_DIR)"

mlb-prod12-model-bundle-publish:
	bin/mlb_prod12_model_bundle_publish.sh

mlb-feature-health:
	$(VENV_PY) backend/mlb/scripts/report_mlb_feature_health.py --window-mode $(MLB_FEATURE_WINDOW_MODE) --window-days $(MLB_FEATURE_WINDOW_DAYS) --games-back $(MLB_FEATURE_GAMES_BACK) --prop-types "$(MLB_FEATURE_PROP_TYPES)" --prop-sources "$(MLB_FEATURE_PROP_SOURCES)" --warn-default-pct $(MLB_FEATURE_WARN_DEFAULT_PCT) --warn-min-rows $(MLB_FEATURE_WARN_MIN_ROWS) $(if $(filter 1,$(MLB_FEATURE_FAIL_ON_WARN)),--fail-on-warn,)

mlb-feature-health-prod12:
	$(MAKE) mlb-feature-health MLB_FEATURE_WINDOW_MODE=games MLB_FEATURE_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_FEATURE_PROP_TYPES="$(MLB_PROD12_PROP_TYPES)" MLB_FEATURE_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)"

mlb-pfp-overlap-audit:
	$(VENV_PY) backend/_legacy/scripts/backfill_mlb_pfp_overlap_from_mtp.py --prop-types "$(MLB_PFP_OVERLAP_PROP_TYPES)" --prop-source "$(MLB_PFP_OVERLAP_PROP_SOURCE)" --feature-set-tag "$(MLB_PFP_OVERLAP_FEATURE_SET_TAG)" --model-tag "$(MLB_PFP_OVERLAP_MODEL_TAG)" --window-mode "$(MLB_PFP_OVERLAP_WINDOW_MODE)" --games-back "$(MLB_PFP_OVERLAP_GAMES_BACK)" --window-days "$(MLB_PFP_OVERLAP_WINDOW_DAYS)" --from-date "$(MLB_PFP_OVERLAP_FROM_DATE)" --to-date "$(MLB_PFP_OVERLAP_TO_DATE)" --limit "$(MLB_PFP_OVERLAP_LIMIT)" --batch-size "$(MLB_PFP_OVERLAP_BATCH_SIZE)"

mlb-pfp-overlap-backfill:
	$(VENV_PY) backend/_legacy/scripts/backfill_mlb_pfp_overlap_from_mtp.py --apply --prop-types "$(MLB_PFP_OVERLAP_PROP_TYPES)" --prop-source "$(MLB_PFP_OVERLAP_PROP_SOURCE)" --feature-set-tag "$(MLB_PFP_OVERLAP_FEATURE_SET_TAG)" --model-tag "$(MLB_PFP_OVERLAP_MODEL_TAG)" --window-mode "$(MLB_PFP_OVERLAP_WINDOW_MODE)" --games-back "$(MLB_PFP_OVERLAP_GAMES_BACK)" --window-days "$(MLB_PFP_OVERLAP_WINDOW_DAYS)" --from-date "$(MLB_PFP_OVERLAP_FROM_DATE)" --to-date "$(MLB_PFP_OVERLAP_TO_DATE)" --limit "$(MLB_PFP_OVERLAP_LIMIT)" --batch-size "$(MLB_PFP_OVERLAP_BATCH_SIZE)"

mlb-balance-guard:
	$(VENV_PY) backend/mlb/scripts/check_mlb_prop_balance_guard.py --prop-type "$(MLB_BALANCE_GUARD_PROP_TYPE)" --prop-sources "$(MLB_BALANCE_GUARD_PROP_SOURCES)" --window-mode "$(MLB_BALANCE_GUARD_WINDOW_MODE)" --window-days $(MLB_BALANCE_GUARD_WINDOW_DAYS) --games-back $(MLB_BALANCE_GUARD_GAMES_BACK) --min-total $(MLB_BALANCE_GUARD_MIN_TOTAL) --min-accuracy-pct $(MLB_BALANCE_GUARD_MIN_ACCURACY) --min-over-pct $(MLB_BALANCE_GUARD_MIN_OVER_PCT)

mlb-prediction-quality-user-added:
	$(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality.py --window-mode $(MLB_QUALITY_WINDOW_MODE) --window-days $(MLB_QUALITY_WINDOW_DAYS) --games-back $(MLB_QUALITY_GAMES_BACK) --prop-types "$(MLB_PREDICT_PROP_TYPES)" --prop-sources "user_added" --min-total 1

mlb-prediction-quality-segmented:
	@if [ -z "$(MLB_QUALITY_SEGMENT_PRESEASON_FROM_DATE)" ] || [ -z "$(MLB_QUALITY_SEGMENT_PRESEASON_TO_DATE)" ] || [ -z "$(MLB_QUALITY_SEGMENT_REGULAR_FROM_DATE)" ] || [ -z "$(MLB_QUALITY_SEGMENT_REGULAR_TO_DATE)" ]; then \
		echo "mlb-prediction-quality-segmented requires MLB_QUALITY_SEGMENT_PRESEASON_FROM_DATE, MLB_QUALITY_SEGMENT_PRESEASON_TO_DATE, MLB_QUALITY_SEGMENT_REGULAR_FROM_DATE, MLB_QUALITY_SEGMENT_REGULAR_TO_DATE"; \
		exit 2; \
	fi
	$(VENV_PY) backend/mlb/scripts/analyze_mlb_prediction_quality_segmented.py --preseason-from-date $(MLB_QUALITY_SEGMENT_PRESEASON_FROM_DATE) --preseason-to-date $(MLB_QUALITY_SEGMENT_PRESEASON_TO_DATE) --regular-from-date $(MLB_QUALITY_SEGMENT_REGULAR_FROM_DATE) --regular-to-date $(MLB_QUALITY_SEGMENT_REGULAR_TO_DATE) --prop-types "$(MLB_PREDICT_PROP_TYPES)" --min-preseason-total $(MLB_QUALITY_SEGMENT_MIN_PRESEASON_TOTAL) --min-regular-total $(MLB_QUALITY_SEGMENT_MIN_REGULAR_TOTAL)

mlb-degenerate-lane-report:
	$(VENV_PY) backend/mlb/scripts/report_mlb_degenerate_lanes.py --window-mode games --games-back $(MLB_QUALITY_GAMES_BACK) --prop-types "$(MLB_DEGENERATE_PROP_TYPES)"

mlb-underserved-historical-report:
	$(VENV_PY) backend/mlb/scripts/report_mlb_high_value_historical.py --prop-types "$(MLB_UNDERSERVED_PROP_TYPES)" --prop-sources "$(MLB_UNDERSERVED_PROP_SOURCES)" --seasons "$(MLB_UNDERSERVED_SEASONS)" --season-count $(MLB_UNDERSERVED_SEASON_COUNT) --balance-floor-pct $(MLB_UNDERSERVED_BALANCE_FLOOR_PCT)

mlb-high-value-historical-report:
	$(MAKE) mlb-underserved-historical-report MLB_UNDERSERVED_PROP_TYPES="$(MLB_HIGH_VALUE_PROP_TYPES)" MLB_UNDERSERVED_PROP_SOURCES="$(MLB_HIGH_VALUE_PROP_SOURCES)" MLB_UNDERSERVED_SEASONS="$(MLB_HIGH_VALUE_SEASONS)" MLB_UNDERSERVED_SEASON_COUNT="$(MLB_HIGH_VALUE_SEASON_COUNT)" MLB_UNDERSERVED_BALANCE_FLOOR_PCT="$(MLB_HIGH_VALUE_BALANCE_FLOOR_PCT)"

mlb-retrain-prereq-check:
	$(VENV_PY) backend/mlb/scripts/mlb_retrain_prereq_check.py --freshness-days $(MLB_RETRAIN_FRESHNESS_DAYS) --freshness-min-rows $(MLB_RETRAIN_FRESHNESS_MIN_ROWS) --coverage-window-mode $(MLB_RETRAIN_COVERAGE_WINDOW_MODE) --coverage-window-days $(MLB_RETRAIN_COVERAGE_WINDOW_DAYS) --coverage-games-back $(MLB_RETRAIN_COVERAGE_GAMES_BACK) --coverage-required-props "$(MLB_RETRAIN_REQUIRED_PROPS)" --coverage-min-training-source-per-prop $(MLB_RETRAIN_MIN_TRAINING_SOURCE_PER_PROP) --coverage-training-prop-sources "$(MLB_RETRAIN_TRAINING_PROP_SOURCES)" --grading-window-mode $(MLB_RETRAIN_GRADING_WINDOW_MODE) --grading-window-days $(MLB_RETRAIN_GRADING_WINDOW_DAYS) --grading-games-back $(MLB_RETRAIN_GRADING_GAMES_BACK) --grading-prop-types "$(MLB_RETRAIN_GRADING_PROP_TYPES)" --grading-min-total $(MLB_RETRAIN_GRADING_MIN_TOTAL) --baseline-max-age-hours $(MLB_RETRAIN_BASELINE_MAX_AGE_HOURS)

mlb-candidate-eval:
	$(VENV_PY) backend/mlb/scripts/mlb_candidate_eval.py --baseline-path "$(MLB_CANDIDATE_BASELINE_PATH)" --baseline-dir "$(MLB_CANDIDATE_BASELINE_DIR)" --source-table "$(MLB_CANDIDATE_SOURCE_TABLE)" $(if $(MLB_CANDIDATE_ROWS_CSV),--rows-csv "$(MLB_CANDIDATE_ROWS_CSV)",) $(MLB_QUALITY_RECONCILE_TWO_SIDED_ARG) $(if $(MLB_CANDIDATE_WINDOW_MODE),--window-mode $(MLB_CANDIDATE_WINDOW_MODE),) --window-days $(MLB_CANDIDATE_WINDOW_DAYS) --games-back $(MLB_CANDIDATE_GAMES_BACK) --prop-types "$(MLB_CANDIDATE_PROP_TYPES)" --required-props "$(MLB_CANDIDATE_REQUIRED_PROPS)" --min-candidate-total $(MLB_CANDIDATE_MIN_TOTAL) --min-overall-lift-pct $(MLB_CANDIDATE_MIN_LIFT_PCT) --max-prop-drop-pct $(MLB_CANDIDATE_MAX_PROP_DROP_PCT) --min-baseline-prop-total-for-drop $(MLB_CANDIDATE_MIN_BASELINE_PROP_TOTAL_FOR_DROP) --min-coverage-ratio-for-drop $(MLB_CANDIDATE_MIN_COVERAGE_RATIO_FOR_DROP) $(if $(MLB_CANDIDATE_PROP_TIER_CONFIG),--prop-tier-config "$(MLB_CANDIDATE_PROP_TIER_CONFIG)",)

mlb-candidate-eval-prod12:
	$(MAKE) mlb-candidate-eval MLB_CANDIDATE_SOURCE_TABLE="$(MLB_PROD12_CANDIDATE_SOURCE_TABLE)" MLB_CANDIDATE_ROWS_CSV="$(MLB_PROD12_CANDIDATE_ROWS_CSV)" MLB_CANDIDATE_PROP_TYPES="$(MLB_PROD12_CANDIDATE_PROP_TYPES)" MLB_CANDIDATE_REQUIRED_PROPS="$(MLB_PROD12_CANDIDATE_REQUIRED_PROPS)" MLB_CANDIDATE_MIN_LIFT_PCT="$(MLB_PROD12_MIN_LIFT_PCT)" MLB_CANDIDATE_MAX_PROP_DROP_PCT="$(MLB_PROD12_MAX_PROP_DROP_PCT)" MLB_CANDIDATE_MIN_BASELINE_PROP_TOTAL_FOR_DROP="$(MLB_PROD12_MIN_BASELINE_PROP_TOTAL_FOR_DROP)" MLB_CANDIDATE_MIN_COVERAGE_RATIO_FOR_DROP="$(MLB_PROD12_MIN_COVERAGE_RATIO_FOR_DROP)" MLB_CANDIDATE_PROP_TIER_CONFIG="$(MLB_PROD12_PROP_TIER_CONFIG)"

mlb-prod12-status:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_status.py --pipeline-history "$(MLB_PIPELINE_HISTORY_INPUT)" --phase2-history "$(MLB_PROD12_PHASE2_HISTORY_INPUT)"

mlb-prod12-status-strict:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_status.py --pipeline-history "$(MLB_PIPELINE_HISTORY_INPUT)" --phase2-history "$(MLB_PROD12_PHASE2_HISTORY_INPUT)" --daily-max-age-hours "$(MLB_PROD12_DAILY_MAX_AGE_HOURS)" --weekly-max-age-hours "$(MLB_PROD12_WEEKLY_MAX_AGE_HOURS)" --strict

mlb-prod12-status-daily-strict:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_status.py --pipeline-history "$(MLB_PIPELINE_HISTORY_INPUT)" --phase2-history "$(MLB_PROD12_PHASE2_HISTORY_INPUT)" --daily-max-age-hours "$(MLB_PROD12_DAILY_MAX_AGE_HOURS)" --scope daily --strict

mlb-prod12-health-report:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_health_report.py --pipeline-history "$(MLB_PIPELINE_HISTORY_INPUT)" --phase2-history "$(MLB_PROD12_PHASE2_HISTORY_INPUT)" --daily-window "$(MLB_PROD12_HEALTH_DAILY_WINDOW)" --weekly-window "$(MLB_PROD12_HEALTH_WEEKLY_WINDOW)"

mlb-prod12-incident:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_incident.py --pipeline-history "$(MLB_PIPELINE_HISTORY_INPUT)" --phase2-history "$(MLB_PROD12_PHASE2_HISTORY_INPUT)"

mlb-prod12-incident-strict:
	$(MAKE) mlb-prod12-incident

mlb-prod12-ops-check:
	@set -e; \
	if ! $(MAKE) mlb-prod12-status-strict; then \
		$(MAKE) mlb-prod12-health-report || true; \
		$(MAKE) mlb-prod12-incident || true; \
		exit 1; \
	fi; \
	$(MAKE) mlb-prod12-health-report

mlb-prod12-ops-log:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_ops_log.py --output "$(MLB_PROD12_OPS_HISTORY_INPUT)" --pipeline-history "$(MLB_PIPELINE_HISTORY_INPUT)" --phase2-history "$(MLB_PROD12_PHASE2_HISTORY_INPUT)" --daily-max-age-hours "$(MLB_PROD12_OPS_DAILY_MAX_AGE_HOURS)" --weekly-max-age-hours "$(MLB_PROD12_OPS_WEEKLY_MAX_AGE_HOURS)" --daily-window "$(MLB_PROD12_HEALTH_DAILY_WINDOW)" --weekly-window "$(MLB_PROD12_HEALTH_WEEKLY_WINDOW)"

mlb-prod12-ops-last:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_ops_last.py --input "$(MLB_PROD12_OPS_HISTORY_INPUT)" --limit 10 --json

mlb-prod12-track-daily:
	$(MAKE) mlb-pipeline-log-prod12 MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PROD12_PIPELINE_PROP_TYPES="$(MLB_PROD12_PROP_TYPES)"

mlb-prod12-track-daily-waterline:
	$(MAKE) mlb-pipeline-log-prod12 MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PROD12_PIPELINE_PROP_TYPES="$(MLB_PROD12_WATERLINE_PROP_TYPES)"

mlb-prod12-daily-gate:
	$(MAKE) mlb-prod12-track-daily MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)"
	$(MAKE) mlb-prod12-status-daily-strict

mlb-prod12-daily-gate-incident:
	@set -e; \
	if ! $(MAKE) mlb-prod12-daily-gate MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)"; then \
		$(MAKE) mlb-prod12-incident || true; \
		exit 1; \
	fi

mlb-prod12-daily-cycle:
	@set -e; \
	rc=0; \
	$(MAKE) mlb-prod12-daily-gate-incident MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" || rc=$$?; \
	$(MAKE) mlb-prod12-ops-log || true; \
	exit $$rc

mlb-prod12-track-weekly:
	$(MAKE) mlb-candidate-eval MLB_CANDIDATE_SOURCE_TABLE="$(MLB_PROD12_CANDIDATE_SOURCE_TABLE)" MLB_CANDIDATE_ROWS_CSV="$(MLB_PROD12_CANDIDATE_ROWS_CSV)" MLB_CANDIDATE_PROP_TYPES="$(MLB_PROD12_CANDIDATE_PROP_TYPES)" MLB_CANDIDATE_REQUIRED_PROPS="$(MLB_PROD12_CANDIDATE_REQUIRED_PROPS)" MLB_CANDIDATE_MIN_LIFT_PCT="$(MLB_PROD12_MIN_LIFT_PCT)" MLB_CANDIDATE_MAX_PROP_DROP_PCT="$(MLB_PROD12_MAX_PROP_DROP_PCT)" MLB_CANDIDATE_MIN_BASELINE_PROP_TOTAL_FOR_DROP="$(MLB_PROD12_MIN_BASELINE_PROP_TOTAL_FOR_DROP)" MLB_CANDIDATE_MIN_COVERAGE_RATIO_FOR_DROP="$(MLB_PROD12_MIN_COVERAGE_RATIO_FOR_DROP)" MLB_CANDIDATE_PROP_TIER_CONFIG="$(MLB_PROD12_PROP_TIER_CONFIG)"

mlb-prod12-release-manifest:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_release_manifest.py --output "$(MLB_PROD12_RELEASE_OUTPUT)" --artifact-dirs "$(MLB_PROD12_ARTIFACT_DIRS)" --artifact-patterns "$(MLB_PROD12_ARTIFACT_PATTERNS)" --baseline-dir "$(MLB_CANDIDATE_BASELINE_DIR)" --pipeline-history "$(MLB_PIPELINE_HISTORY_INPUT)" --prop-types "$(MLB_PROD12_PROP_TYPES)" --quality-games-back $(MLB_QUALITY_GAMES_BACK) --quality-min-total $(MLB_QUALITY_MIN_TOTAL) --quality-min-accuracy $(MLB_QUALITY_MIN_ACCURACY) --max-prop-drop-pct $(MLB_PROD12_MAX_PROP_DROP_PCT)

mlb-prod12-replay-latency:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_replay_latency.py $(if $(MLB_BASE_URL),--base-url $(MLB_BASE_URL),) --date $(MLB_DATE) --sample-size $(MLB_REPLAY_SAMPLE) --require-min-success $(MLB_REPLAY_MIN_SUCCESS) --prop-types "$(MLB_PROD12_PROP_TYPES)" --max-predict-p95-ms $(MLB_REPLAY_MAX_PREDICT_P95_MS) --retry-attempts $(MLB_REPLAY_RETRY_ATTEMPTS) --retry-backoff-ms $(MLB_REPLAY_RETRY_BACKOFF_MS) $(if $(filter 1,$(MLB_REPLAY_ALLOW_SPARSE)),--allow-sparse,) --output "$(MLB_PROD12_REPLAY_OUTPUT)"

mlb-prod12-phase2-log:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_phase2_log.py --output "$(MLB_PROD12_PHASE2_HISTORY_INPUT)" --manifest-path "$(MLB_PROD12_RELEASE_OUTPUT)" --replay-path "$(MLB_PROD12_REPLAY_OUTPUT)" --baseline-path "$(MLB_CANDIDATE_BASELINE_PATH)" --baseline-dir "$(MLB_CANDIDATE_BASELINE_DIR)" --source-table "$(MLB_PROD12_CANDIDATE_SOURCE_TABLE)" $(if $(MLB_PROD12_CANDIDATE_ROWS_CSV),--rows-csv "$(MLB_PROD12_CANDIDATE_ROWS_CSV)",) $(if $(MLB_CANDIDATE_WINDOW_MODE),--window-mode $(MLB_CANDIDATE_WINDOW_MODE),) --window-days $(MLB_CANDIDATE_WINDOW_DAYS) --games-back $(MLB_CANDIDATE_GAMES_BACK) --prop-types "$(MLB_PROD12_CANDIDATE_PROP_TYPES)" --required-props "$(MLB_PROD12_CANDIDATE_REQUIRED_PROPS)" --min-candidate-total $(MLB_CANDIDATE_MIN_TOTAL) --min-overall-lift-pct $(MLB_PROD12_MIN_LIFT_PCT) --max-prop-drop-pct $(MLB_PROD12_MAX_PROP_DROP_PCT) --min-baseline-prop-total-for-drop $(MLB_PROD12_MIN_BASELINE_PROP_TOTAL_FOR_DROP) --min-coverage-ratio-for-drop $(MLB_PROD12_MIN_COVERAGE_RATIO_FOR_DROP) $(if $(MLB_PROD12_PROP_TIER_CONFIG),--prop-tier-config "$(MLB_PROD12_PROP_TIER_CONFIG)",)

mlb-prod12-phase2-last:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_phase2_last.py --input "$(MLB_PROD12_PHASE2_HISTORY_INPUT)" --limit 10 --json

mlb-prod12-phase2-last-strict:
	$(VENV_PY) backend/mlb/scripts/mlb_prod12_phase2_last.py --input "$(MLB_PROD12_PHASE2_HISTORY_INPUT)" --limit 10 --json --strict

mlb-prod12-phase2-weekly-gate:
	$(MAKE) mlb-prod12-phase2-readiness MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_REPLAY_SAMPLE="$(MLB_REPLAY_SAMPLE)" MLB_REPLAY_MIN_SUCCESS="$(MLB_REPLAY_MIN_SUCCESS)" MLB_REPLAY_MAX_PREDICT_P95_MS="$(MLB_REPLAY_MAX_PREDICT_P95_MS)" MLB_REPLAY_RETRY_ATTEMPTS="$(MLB_REPLAY_RETRY_ATTEMPTS)" MLB_REPLAY_RETRY_BACKOFF_MS="$(MLB_REPLAY_RETRY_BACKOFF_MS)"
	$(MAKE) mlb-prod12-phase2-last-strict

mlb-prod12-phase2-weekly-gate-incident:
	@set -e; \
	if ! $(MAKE) mlb-prod12-phase2-weekly-gate MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_REPLAY_SAMPLE="$(MLB_REPLAY_SAMPLE)" MLB_REPLAY_MIN_SUCCESS="$(MLB_REPLAY_MIN_SUCCESS)" MLB_REPLAY_MAX_PREDICT_P95_MS="$(MLB_REPLAY_MAX_PREDICT_P95_MS)" MLB_REPLAY_RETRY_ATTEMPTS="$(MLB_REPLAY_RETRY_ATTEMPTS)" MLB_REPLAY_RETRY_BACKOFF_MS="$(MLB_REPLAY_RETRY_BACKOFF_MS)"; then \
		$(MAKE) mlb-prod12-incident || true; \
		exit 1; \
	fi

mlb-prod12-phase2-weekly-cycle:
	@set -e; \
	rc=0; \
	$(MAKE) mlb-prod12-phase2-weekly-gate-incident MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_REPLAY_SAMPLE="$(MLB_REPLAY_SAMPLE)" MLB_REPLAY_MIN_SUCCESS="$(MLB_REPLAY_MIN_SUCCESS)" MLB_REPLAY_MAX_PREDICT_P95_MS="$(MLB_REPLAY_MAX_PREDICT_P95_MS)" MLB_REPLAY_RETRY_ATTEMPTS="$(MLB_REPLAY_RETRY_ATTEMPTS)" MLB_REPLAY_RETRY_BACKOFF_MS="$(MLB_REPLAY_RETRY_BACKOFF_MS)" || rc=$$?; \
	$(MAKE) mlb-prod12-ops-log || true; \
	exit $$rc

mlb-prod12-phase2-readiness:
	@set -e; \
	mlb_baseline="$(MLB_CANDIDATE_BASELINE_PATH)"; \
	if [ "$(MLB_PROD12_CANDIDATE_SOURCE_TABLE)" = "reconcile_rows" ]; then \
		if [ -z "$$mlb_baseline" ]; then mlb_baseline="$(MLB_PROD12_PHASE2_BASELINE_PATH)"; fi; \
		echo "mlb-prod12-phase2-readiness: capturing source-matched baseline $$mlb_baseline"; \
		$(MAKE) mlb-prod12-phase2-baseline-capture MLB_PROD12_PHASE2_BASELINE_PATH="$$mlb_baseline" MLB_PROD12_CANDIDATE_SOURCE_TABLE="$(MLB_PROD12_CANDIDATE_SOURCE_TABLE)" MLB_PROD12_CANDIDATE_ROWS_CSV="$(MLB_PROD12_CANDIDATE_ROWS_CSV)" MLB_PROD12_CANDIDATE_PROP_TYPES="$(MLB_PROD12_CANDIDATE_PROP_TYPES)" MLB_CANDIDATE_WINDOW_MODE="$(MLB_CANDIDATE_WINDOW_MODE)" MLB_CANDIDATE_WINDOW_DAYS="$(MLB_CANDIDATE_WINDOW_DAYS)" MLB_CANDIDATE_GAMES_BACK="$(MLB_CANDIDATE_GAMES_BACK)"; \
	else \
		if [ -z "$$mlb_baseline" ]; then mlb_baseline="artifacts/season_baselines/mlb_quality_games_$(MLB_QUALITY_GAMES_BACK)_$(MLB_QUALITY_WINDOW_DAYS).json"; fi; \
		if [ ! -f "$$mlb_baseline" ]; then \
			echo "mlb-prod12-phase2-readiness: baseline missing, capturing $$mlb_baseline"; \
			$(MAKE) mlb-season-baseline-capture MLB_QUALITY_WINDOW_MODE="games"; \
		fi; \
		fi; \
		$(MAKE) mlb-model-artifact-validate-prod12; \
		$(MAKE) mlb-prod12-release-manifest; \
		$(MAKE) mlb-prod12-replay-latency MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_REPLAY_SAMPLE="$(MLB_REPLAY_SAMPLE)" MLB_REPLAY_MIN_SUCCESS="$(MLB_REPLAY_MIN_SUCCESS)" MLB_REPLAY_MAX_PREDICT_P95_MS="$(MLB_REPLAY_MAX_PREDICT_P95_MS)"; \
		$(MAKE) mlb-prod12-track-weekly MLB_CANDIDATE_BASELINE_PATH="$$mlb_baseline" MLB_PROD12_MIN_BASELINE_PROP_TOTAL_FOR_DROP="$(MLB_PROD12_MIN_BASELINE_PROP_TOTAL_FOR_DROP)" MLB_PROD12_MIN_COVERAGE_RATIO_FOR_DROP="$(MLB_PROD12_MIN_COVERAGE_RATIO_FOR_DROP)" MLB_PROD12_PROP_TIER_CONFIG="$(MLB_PROD12_PROP_TIER_CONFIG)"; \
		$(MAKE) mlb-prod12-phase2-log MLB_CANDIDATE_BASELINE_PATH="$$mlb_baseline" MLB_PROD12_MIN_BASELINE_PROP_TOTAL_FOR_DROP="$(MLB_PROD12_MIN_BASELINE_PROP_TOTAL_FOR_DROP)" MLB_PROD12_MIN_COVERAGE_RATIO_FOR_DROP="$(MLB_PROD12_MIN_COVERAGE_RATIO_FOR_DROP)" MLB_PROD12_PROP_TIER_CONFIG="$(MLB_PROD12_PROP_TIER_CONFIG)"

mlb-prediction-gate:
	$(VENV_PY) backend/mlb/scripts/mlb_prediction_gate.py --date $(MLB_DATE) --sample-size $(MLB_PREDICT_SAMPLE) --require-min-success $(MLB_PREDICT_MIN_SUCCESS) --prop-types "$(MLB_PREDICT_PROP_TYPES)" --quality-window-mode $(MLB_QUALITY_WINDOW_MODE) --quality-window-days $(MLB_QUALITY_WINDOW_DAYS) --quality-games-back $(MLB_QUALITY_GAMES_BACK) --quality-min-total $(MLB_QUALITY_MIN_TOTAL) --quality-min-accuracy $(MLB_QUALITY_MIN_ACCURACY) --quality-prop-sources "$(MLB_QUALITY_PROP_SOURCES)"

mlb-pipeline-check:
	$(MAKE) mlb-prediction-gate MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PREDICT_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_QUALITY_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)"
	$(MAKE) mlb-prediction-flow-audit MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)"
	$(MAKE) mlb-hits-expectation-sources MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)"

mlb-pipeline-check-json:
	$(VENV_PY) backend/mlb/scripts/mlb_pipeline_check.py $(if $(MLB_BASE_URL),--base-url $(MLB_BASE_URL),) --date $(MLB_DATE) --sample-size $(MLB_PREDICT_SAMPLE) --require-min-success $(MLB_PREDICT_MIN_SUCCESS) --prop-types "$(MLB_PREDICT_PROP_TYPES)" --quality-window-mode $(MLB_QUALITY_WINDOW_MODE) --quality-window-days $(MLB_QUALITY_WINDOW_DAYS) --quality-games-back $(MLB_QUALITY_GAMES_BACK) --quality-min-total $(MLB_QUALITY_MIN_TOTAL) --quality-min-accuracy $(MLB_QUALITY_MIN_ACCURACY) --quality-prop-sources "$(MLB_QUALITY_PROP_SOURCES)" --coverage-window-mode $(MLB_PROP_COVERAGE_WINDOW_MODE) --coverage-window-days $(MLB_PROP_COVERAGE_WINDOW_DAYS) --coverage-games-back $(MLB_PROP_COVERAGE_GAMES_BACK) --coverage-required-props "$(MLB_PROP_COVERAGE_REQUIRED)" --coverage-min-graded-per-prop $(MLB_PROP_COVERAGE_MIN_GRADED) --coverage-gate-metric $(MLB_PROP_COVERAGE_GATE_METRIC) --coverage-training-prop-sources "$(MLB_PROP_COVERAGE_TRAINING_SOURCES)" $(if $(filter 1,$(MLB_INCLUDE_COVERAGE)),--include-coverage,)

mlb-pipeline-check-ops:
	$(MAKE) mlb-pipeline-check-json MLB_INCLUDE_COVERAGE=1

mlb-pipeline-check-core:
	$(MAKE) mlb-pipeline-check-json MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PREDICT_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="games" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_QUALITY_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)" MLB_PROP_COVERAGE_WINDOW_MODE="games" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_CORE_PROP_TYPES)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_CORE_MIN_GRADED)" MLB_PROP_COVERAGE_GATE_METRIC="row_source" MLB_PROP_COVERAGE_TRAINING_SOURCES="$(MLB_CORE_TRAINING_SOURCES)"

mlb-pipeline-check-prod8:
	$(MAKE) mlb-pipeline-check-json MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PROD8_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="games" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_QUALITY_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)" MLB_PROP_COVERAGE_WINDOW_MODE="games" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROD8_PROP_TYPES)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_CORE_MIN_GRADED)" MLB_PROP_COVERAGE_GATE_METRIC="row_source" MLB_PROP_COVERAGE_TRAINING_SOURCES="$(MLB_CORE_TRAINING_SOURCES)"

mlb-pipeline-check-prod12:
	$(MAKE) mlb-pipeline-check-json MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PROD12_PIPELINE_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="games" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_QUALITY_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)" MLB_PROP_COVERAGE_WINDOW_MODE="games" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROD12_PROP_TYPES)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_CORE_MIN_GRADED)" MLB_PROP_COVERAGE_GATE_METRIC="row_source" MLB_PROP_COVERAGE_TRAINING_SOURCES="$(MLB_CORE_TRAINING_SOURCES)"

mlb-pipeline-log:
	$(VENV_PY) backend/mlb/scripts/mlb_pipeline_log.py --output artifacts/mlb_pipeline_history.jsonl $(if $(MLB_BASE_URL),--base-url $(MLB_BASE_URL),) --date $(MLB_DATE) --sample-size $(MLB_PREDICT_SAMPLE) --require-min-success $(MLB_PREDICT_MIN_SUCCESS) --prop-types "$(MLB_PREDICT_PROP_TYPES)" --quality-window-mode $(MLB_QUALITY_WINDOW_MODE) --quality-window-days $(MLB_QUALITY_WINDOW_DAYS) --quality-games-back $(MLB_QUALITY_GAMES_BACK) --quality-min-total $(MLB_QUALITY_MIN_TOTAL) --quality-min-accuracy $(MLB_QUALITY_MIN_ACCURACY) --quality-prop-sources "$(MLB_QUALITY_PROP_SOURCES)" --coverage-window-mode $(MLB_PROP_COVERAGE_WINDOW_MODE) --coverage-window-days $(MLB_PROP_COVERAGE_WINDOW_DAYS) --coverage-games-back $(MLB_PROP_COVERAGE_GAMES_BACK) --coverage-required-props "$(MLB_PROP_COVERAGE_REQUIRED)" --coverage-min-graded-per-prop $(MLB_PROP_COVERAGE_MIN_GRADED) --coverage-gate-metric $(MLB_PROP_COVERAGE_GATE_METRIC) --coverage-training-prop-sources "$(MLB_PROP_COVERAGE_TRAINING_SOURCES)" $(if $(filter 1,$(MLB_INCLUDE_COVERAGE)),--include-coverage,)

mlb-pipeline-log-prod12:
	$(MAKE) mlb-pipeline-log MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PROD12_PIPELINE_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="games" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_QUALITY_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)" MLB_PROP_COVERAGE_WINDOW_MODE="games" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROD12_PROP_TYPES)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_CORE_MIN_GRADED)" MLB_PROP_COVERAGE_GATE_METRIC="row_source" MLB_PROP_COVERAGE_TRAINING_SOURCES="$(MLB_CORE_TRAINING_SOURCES)"

mlb-pipeline-log-ops:
	$(MAKE) mlb-pipeline-log MLB_INCLUDE_COVERAGE=1

mlb-pipeline-last:
	$(VENV_PY) backend/mlb/scripts/mlb_pipeline_last.py --input artifacts/mlb_pipeline_history.jsonl --limit 10 --json

mlb-pipeline-daily-check:
	$(MAKE) mlb-pipeline-log MLB_BASE_URL="$(MLB_BASE_URL)" MLB_DATE="$(MLB_DATE)" MLB_PREDICT_SAMPLE="$(MLB_PREDICT_SAMPLE)" MLB_PREDICT_MIN_SUCCESS="$(MLB_PREDICT_MIN_SUCCESS)" MLB_PREDICT_PROP_TYPES="$(MLB_PREDICT_PROP_TYPES)" MLB_QUALITY_WINDOW_MODE="$(MLB_QUALITY_WINDOW_MODE)" MLB_QUALITY_WINDOW_DAYS="$(MLB_QUALITY_WINDOW_DAYS)" MLB_QUALITY_GAMES_BACK="$(MLB_QUALITY_GAMES_BACK)" MLB_QUALITY_MIN_TOTAL="$(MLB_QUALITY_MIN_TOTAL)" MLB_QUALITY_MIN_ACCURACY="$(MLB_QUALITY_MIN_ACCURACY)" MLB_QUALITY_PROP_SOURCES="$(MLB_QUALITY_PROP_SOURCES)" MLB_PROP_COVERAGE_WINDOW_MODE="$(MLB_PROP_COVERAGE_WINDOW_MODE)" MLB_PROP_COVERAGE_WINDOW_DAYS="$(MLB_PROP_COVERAGE_WINDOW_DAYS)" MLB_PROP_COVERAGE_GAMES_BACK="$(MLB_PROP_COVERAGE_GAMES_BACK)" MLB_PROP_COVERAGE_REQUIRED="$(MLB_PROP_COVERAGE_REQUIRED)" MLB_PROP_COVERAGE_MIN_GRADED="$(MLB_PROP_COVERAGE_MIN_GRADED)" MLB_PROP_COVERAGE_GATE_METRIC="$(MLB_PROP_COVERAGE_GATE_METRIC)" MLB_PROP_COVERAGE_TRAINING_SOURCES="$(MLB_PROP_COVERAGE_TRAINING_SOURCES)"
	$(MAKE) mlb-pipeline-last

mlb-prop-coverage:
	$(VENV_PY) backend/mlb/scripts/report_mlb_prop_coverage.py --window-mode $(MLB_PROP_COVERAGE_WINDOW_MODE) --window-days $(MLB_PROP_COVERAGE_WINDOW_DAYS) --games-back $(MLB_PROP_COVERAGE_GAMES_BACK) --required-props "$(MLB_PROP_COVERAGE_REQUIRED)" --min-graded-per-prop $(MLB_PROP_COVERAGE_MIN_GRADED) --gate-metric $(MLB_PROP_COVERAGE_GATE_METRIC) --row-sources "$(MLB_PROP_COVERAGE_TRAINING_SOURCES)"

mlb-prop-coverage-core:
	$(VENV_PY) backend/mlb/scripts/report_mlb_prop_coverage.py --window-mode games --games-back $(MLB_PROP_COVERAGE_GAMES_BACK) --required-props "$(MLB_CORE_PROP_TYPES)" --min-graded-per-prop $(MLB_CORE_MIN_GRADED) --gate-metric row_source --row-sources "$(MLB_CORE_TRAINING_SOURCES)"

mlb-prediction-flow-audit:
	$(VENV_PY) backend/mlb/scripts/audit_mlb_prediction_flow.py --window-mode $(MLB_QUALITY_WINDOW_MODE) --window-days $(MLB_QUALITY_WINDOW_DAYS) --games-back $(MLB_QUALITY_GAMES_BACK)

mlb-hits-expectation-sources:
	$(VENV_PY) backend/mlb/scripts/check_mlb_hits_expectation_sources.py --window-mode $(MLB_QUALITY_WINDOW_MODE) --window-days $(MLB_QUALITY_WINDOW_DAYS) --games-back $(MLB_QUALITY_GAMES_BACK)

# Generate historical stat-derived MLB rows (legacy workhorse script).
mlb-insert-stat-derived:
	$(VENV_PY) backend/mlb/scripts/insert_mlb_stat_derived.py --quiet --days-ago $(MLB_STAT_DAYS_AGO) --max-games-per-date $(MLB_STAT_MAX_GAMES) --batter-sample-ratio $(MLB_STAT_BATTER_SAMPLE_RATIO) $(if $(filter 1,$(MLB_STAT_SKIP_EXISTING_DATES)),--skip-existing-dates,) $(if $(filter 1,$(MLB_SEASON_REQUIRE_REGULAR)),--require-regular-season,) $(if $(MLB_STAT_FROM_DATE),--from-date $(MLB_STAT_FROM_DATE),) $(if $(MLB_STAT_TO_DATE),--to-date $(MLB_STAT_TO_DATE),)

# Validate recent stat-derived row volume in model_training_props.
mlb-check-stat-derived:
	$(VENV_PY) backend/mlb/scripts/validate_mlb_stat_derived_recent.py --days $(MLB_STAT_DERIVED_DAYS) --require-min $(MLB_STAT_DERIVED_MIN)

mlb-check-stat-derived-json:
	$(VENV_PY) backend/mlb/scripts/validate_mlb_stat_derived_recent.py --days $(MLB_STAT_DERIVED_DAYS) --require-min $(MLB_STAT_DERIVED_MIN) --json

mlb-check-rolling-integrity:
	$(VENV_PY) backend/mlb/scripts/check_mlb_rolling_integrity.py --days $(MLB_ROLLING_CHECK_DAYS) --min-coverage-pct $(MLB_ROLLING_CHECK_MIN_COVERAGE_PCT) --min-comparable $(MLB_ROLLING_CHECK_MIN_COMPARABLE) $(if $(MLB_ROLLING_CHECK_FROM_DATE),--from-date $(MLB_ROLLING_CHECK_FROM_DATE),) $(if $(MLB_ROLLING_CHECK_TO_DATE),--to-date $(MLB_ROLLING_CHECK_TO_DATE),)

mlb-refresh-player-pa:
	$(VENV_PY) backend/mlb/scripts/backfill_mlb_player_pa.py --start-date "$(MLB_PA_DATE_FROM)" --end-date "$(MLB_PA_DATE_TO)" --batch-size "$(MLB_PA_BATCH_SIZE)" $(if $(filter 1 true TRUE yes YES,$(MLB_PA_DRY_RUN)),--dry-run,) $(if $(filter 1 true TRUE yes YES,$(MLB_PA_ONLY_MISSING)),--only-missing-pa,) $(if $(strip $(MLB_PA_LIMIT_GAMES)),--limit-games "$(MLB_PA_LIMIT_GAMES)",) $(if $(filter 1 true TRUE yes YES,$(MLB_SEASON_REQUIRE_REGULAR)),--require-regular-season,)

# One-command stat-derived refresh + guard (cron-friendly).
mlb-stat-derived-refresh:
	$(MAKE) mlb-insert-stat-derived MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_BATTER_SAMPLE_RATIO=$(MLB_STAT_BATTER_SAMPLE_RATIO)
	$(MAKE) mlb-check-stat-derived MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)

# Quick smoke for stat-derived wiring (limits game load).
mlb-stat-derived-smoke:
	$(MAKE) mlb-insert-stat-derived MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=1 MLB_STAT_SKIP_EXISTING_DATES=0 MLB_STAT_BATTER_SAMPLE_RATIO=$(MLB_STAT_BATTER_SAMPLE_RATIO)

# Historical window backfill + guard in one command.
mlb-stat-derived-backfill:
	@if [ -z "$(MLB_STAT_FROM_DATE)" ] || [ -z "$(MLB_STAT_TO_DATE)" ]; then \
		echo "mlb-stat-derived-backfill requires MLB_STAT_FROM_DATE and MLB_STAT_TO_DATE"; \
		exit 2; \
	fi
	$(MAKE) mlb-insert-stat-derived MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_BATTER_SAMPLE_RATIO=$(MLB_STAT_BATTER_SAMPLE_RATIO)
	$(MAKE) mlb-check-stat-derived MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)

mlb-cleanup-one-sided-price-rows:
	$(VENV_PY) backend/mlb/scripts/cleanup_mlb_one_sided_price_rows.py --schema "$(MLB_ONE_SIDED_CLEANUP_SCHEMA)" $(if $(strip $(MLB_ONE_SIDED_CLEANUP_TABLES)),--tables "$(MLB_ONE_SIDED_CLEANUP_TABLES)",) $(if $(strip $(MLB_ONE_SIDED_CLEANUP_OUT_JSON)),--out-json "$(MLB_ONE_SIDED_CLEANUP_OUT_JSON)",) $(if $(filter 1 true TRUE yes YES,$(MLB_ONE_SIDED_CLEANUP_APPLY)),--apply,)

mlb-preseason-cleanup:
	@if [ -z "$(MLB_PRESEASON_FROM_DATE)" ] || [ -z "$(MLB_PRESEASON_TO_DATE)" ]; then \
		echo "mlb-preseason-cleanup requires MLB_PRESEASON_FROM_DATE and MLB_PRESEASON_TO_DATE"; \
		exit 2; \
	fi
	$(VENV_PY) backend/mlb/scripts/cleanup_mlb_preseason_rows.py --from-date $(MLB_PRESEASON_FROM_DATE) --to-date $(MLB_PRESEASON_TO_DATE) $(if $(filter 1,$(MLB_PRESEASON_INCLUDE_USER_ADDED)),--include-user-added,) $(if $(strip $(MLB_PRESEASON_GAME_TYPES)),--game-types "$(MLB_PRESEASON_GAME_TYPES)",)
	@echo "Dry-run complete. Re-run with:"
	@echo "  $(VENV_PY) backend/mlb/scripts/cleanup_mlb_preseason_rows.py --from-date $(MLB_PRESEASON_FROM_DATE) --to-date $(MLB_PRESEASON_TO_DATE) --apply $(if $(filter 1,$(MLB_PRESEASON_INCLUDE_USER_ADDED)),--include-user-added,) $(if $(strip $(MLB_PRESEASON_GAME_TYPES)),--game-types \"$(MLB_PRESEASON_GAME_TYPES)\",)"

mlb-season-mode-lock:
	$(MAKE) mlb-show-config MLB_SEASON_REQUIRE_REGULAR=1
	$(MAKE) mlb-stat-derived-smoke MLB_SEASON_REQUIRE_REGULAR=1 MLB_STAT_SKIP_EXISTING_DATES=0

# Local tmp housekeeping helpers (tmp is git-ignored; safe to prune aggressively).
mlb-tmp-focus:
	MLB_TMP_FOCUS_ROOT="$(MLB_TMP_FOCUS_ROOT)" bin/mlb_tmp_focus.sh build "$(MLB_TMP_FOCUS_DATE)"

tmp-audit:
	bin/tmp_housekeeping.sh audit

tmp-prune-bulky:
	bin/tmp_housekeeping.sh prune-bulky

tmp-prune-age:
	bin/tmp_housekeeping.sh prune-age $(TMP_RETENTION_DAYS)

tmp-prune-fat-csv:
	bin/tmp_housekeeping.sh prune-fat-csv $(TMP_FAT_CSV_MIN_MB) $(TMP_FAT_CSV_MIN_AGE_DAYS)

tmp-prune:
	$(MAKE) tmp-prune-bulky
	$(MAKE) tmp-prune-age TMP_RETENTION_DAYS=$(TMP_RETENTION_DAYS)

# Local MLB odds_history housekeeping helpers (generated, untracked runtime snapshots).
mlb-odds-history-audit:
	bin/mlb_odds_history_housekeeping.sh audit

mlb-odds-history-prune-intermediate:
	bin/mlb_odds_history_housekeeping.sh prune-intermediate

mlb-odds-history-prune-old-dates:
	bin/mlb_odds_history_housekeeping.sh prune-old-dates $(MLB_ODDS_HISTORY_RETENTION_DAYS)

# Offload MLB odds_history to external storage while retaining recoverability.
mlb-odds-history-offload-status:
	bin/mlb_odds_history_offload.sh status "$(MLB_ODDS_HISTORY_ARCHIVE_ROOT)"

mlb-odds-history-offload-sync:
	bin/mlb_odds_history_offload.sh sync "$(MLB_ODDS_HISTORY_ARCHIVE_ROOT)"

mlb-odds-history-offload-prune-local:
	bin/mlb_odds_history_offload.sh prune-local-synced "$(MLB_ODDS_HISTORY_ARCHIVE_ROOT)" "$(MLB_ODDS_HISTORY_LOCAL_RETENTION_DAYS)"

mlb-odds-history-offload-cycle:
	$(MAKE) mlb-odds-history-offload-status MLB_ODDS_HISTORY_ARCHIVE_ROOT="$(MLB_ODDS_HISTORY_ARCHIVE_ROOT)"
	$(MAKE) mlb-odds-history-offload-sync MLB_ODDS_HISTORY_ARCHIVE_ROOT="$(MLB_ODDS_HISTORY_ARCHIVE_ROOT)"
	$(MAKE) mlb-odds-history-offload-prune-local MLB_ODDS_HISTORY_ARCHIVE_ROOT="$(MLB_ODDS_HISTORY_ARCHIVE_ROOT)" MLB_ODDS_HISTORY_LOCAL_RETENTION_DAYS="$(MLB_ODDS_HISTORY_LOCAL_RETENTION_DAYS)"
	$(MAKE) mlb-odds-history-offload-status MLB_ODDS_HISTORY_ARCHIVE_ROOT="$(MLB_ODDS_HISTORY_ARCHIVE_ROOT)"

# Local artifacts housekeeping helpers (keep history/baselines, prune stale generated bulk).
artifacts-audit:
	bin/artifacts_housekeeping.sh audit

artifacts-prune-safe:
	bin/artifacts_housekeeping.sh prune-safe $(ARTIFACTS_RETENTION_DAYS)

artifacts-prune-experiments:
	bin/artifacts_housekeeping.sh prune-experiments-all

artifacts-prune:
	$(MAKE) artifacts-prune-safe ARTIFACTS_RETENTION_DAYS=$(ARTIFACTS_RETENTION_DAYS)

# Daily capture lane: persist MLB odds snapshot + slate artifacts into odds_history.
mlb-daily-capture:
	$(MAKE) mlb-predictions-wide MLB_DATE="$(MLB_DATE)" MLB_SLATE_PRED_CSV="$(MLB_SLATE_PRED_CSV)" MLB_ODDS_SNAPSHOT_JSON="$(MLB_ODDS_SNAPSHOT_JSON)" MLB_WIDE_PROP_TYPES="$(MLB_WIDE_PROP_TYPES)" MLB_WIDE_REQUIRE_MIN_ROWS="$(MLB_WIDE_REQUIRE_MIN_ROWS)"
	$(MAKE) mlb-slate-output MLB_DATE="$(MLB_DATE)" MLB_SLATE_PRED_CSV="$(MLB_SLATE_PRED_CSV)" MLB_SLATE_OUTPUT_CSV="$(MLB_SLATE_OUTPUT_CSV)" MLB_SLATE_PROP_TYPE="$(MLB_SLATE_PROP_TYPE)"
	$(MAKE) mlb-book-upload MLB_DATE="$(MLB_DATE)" MLB_SLATE_OUTPUT_CSV="$(MLB_SLATE_OUTPUT_CSV)" MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" MLB_ODDS_HISTORY_ROOT="$(MLB_ODDS_HISTORY_ROOT)" MLB_ODDS_SNAPSHOT_JSON="$(MLB_ODDS_SNAPSHOT_JSON)"

mlb-build-upload-only:
	$(MAKE) mlb-daily-capture MLB_DATE="$(MLB_DATE)" MLB_SLATE_PRED_CSV="$(MLB_SLATE_PRED_CSV)" MLB_ODDS_SNAPSHOT_JSON="$(MLB_ODDS_SNAPSHOT_JSON)" MLB_WIDE_PROP_TYPES="$(MLB_WIDE_PROP_TYPES)" MLB_WIDE_REQUIRE_MIN_ROWS="$(MLB_WIDE_REQUIRE_MIN_ROWS)" MLB_SLATE_OUTPUT_CSV="$(MLB_SLATE_OUTPUT_CSV)" MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" MLB_ODDS_HISTORY_ROOT="$(MLB_ODDS_HISTORY_ROOT)"

# One-command MLB daily refresh baseline (cache + rosters + bvp/pvb + stat-derived + guard + optional capture).
mlb-daily-refresh:
	$(MAKE) mlb-show-config
	$(MAKE) mlb-market-cache-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS)
	$(MAKE) mlb-roster-refresh-all MLB_ROSTER_DATE=$(MLB_ROSTER_DATE)
	@if [ "$(MLB_DAILY_BVP_PVB_ENABLED)" = "1" ]; then \
		echo "mlb-daily-refresh: running bvp/pvb refresh"; \
		$(MAKE) mlb-bvp-pvb-refresh MLB_BVP_DATE="$(MLB_DATE_ET)" MLB_BVP_FEATURE_SET_TAG="$(MLB_BVP_FEATURE_SET_TAG)" MLB_BVP_MODEL_TAG="$(MLB_BVP_MODEL_TAG)" MLB_BVP_BATCH_SIZE="$(MLB_BVP_BATCH_SIZE)" MLB_BVP_REQUEST_TIMEOUT_SEC="$(MLB_BVP_REQUEST_TIMEOUT_SEC)"; \
	else \
		echo "mlb-daily-refresh: skipping bvp/pvb refresh (MLB_DAILY_BVP_PVB_ENABLED=$(MLB_DAILY_BVP_PVB_ENABLED))"; \
	fi
	$(MAKE) mlb-stat-derived-refresh MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_BATTER_SAMPLE_RATIO=$(MLB_STAT_BATTER_SAMPLE_RATIO) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)
	@if [ "$(MLB_DAILY_INCLUDE_CAPTURE)" = "1" ]; then \
		echo "mlb-daily-refresh: running daily capture lane"; \
		$(MAKE) mlb-daily-capture MLB_DATE="$(MLB_DATE)" MLB_SLATE_PRED_CSV="$(MLB_SLATE_PRED_CSV)" MLB_SLATE_OUTPUT_CSV="$(MLB_SLATE_OUTPUT_CSV)" MLB_BOOK_UPLOAD_OUT_CSV="$(MLB_BOOK_UPLOAD_OUT_CSV)" MLB_ODDS_HISTORY_ROOT="$(MLB_ODDS_HISTORY_ROOT)" MLB_ODDS_SNAPSHOT_JSON="$(MLB_ODDS_SNAPSHOT_JSON)" MLB_WIDE_PROP_TYPES="$(MLB_WIDE_PROP_TYPES)" MLB_WIDE_REQUIRE_MIN_ROWS="$(MLB_WIDE_REQUIRE_MIN_ROWS)" MLB_SLATE_PROP_TYPE="$(MLB_SLATE_PROP_TYPE)"; \
	else \
		echo "mlb-daily-refresh: skipping capture (MLB_DAILY_INCLUDE_CAPTURE=$(MLB_DAILY_INCLUDE_CAPTURE))"; \
	fi
	@if [ "$(MLB_DAILY_BVP_IMPACT_ENABLED)" = "1" ]; then \
		echo "mlb-daily-refresh: running bvp/pvb impact monitor"; \
		set +e; \
		$(MAKE) mlb-bvp-impact-report MLB_BVP_IMPACT_LABEL_DATE="$(MLB_DATE)" MLB_BVP_IMPACT_SLATE_CSV="$(MLB_SLATE_OUTPUT_CSV)" MLB_BVP_IMPACT_WIDE_CSV="$(MLB_SLATE_PRED_CSV)"; \
		impact_rc=$$?; \
		set -e; \
		if [ "$$impact_rc" -ne 0 ]; then \
			if [ "$(MLB_DAILY_BVP_IMPACT_REQUIRED)" = "1" ]; then \
				echo "mlb-daily-refresh: bvp/pvb impact monitor failed rc=$$impact_rc"; \
				exit "$$impact_rc"; \
			fi; \
			echo "mlb-daily-refresh: WARN bvp/pvb impact monitor failed rc=$$impact_rc; continuing"; \
		fi; \
	else \
		echo "mlb-daily-refresh: skipping bvp/pvb impact monitor (MLB_DAILY_BVP_IMPACT_ENABLED=$(MLB_DAILY_BVP_IMPACT_ENABLED))"; \
	fi
	@if [ "$(MLB_DAILY_HITS_ENV_ENABLED)" = "1" ]; then \
		echo "mlb-daily-refresh: running hits-environment monitor"; \
		set +e; \
		$(MAKE) mlb-hits-environment-report MLB_HITS_ENV_AS_OF_DATE="$(MLB_HITS_ENV_AS_OF_DATE)" MLB_HITS_ENV_LOOKBACK_DAYS="$(MLB_HITS_ENV_LOOKBACK_DAYS)" MLB_HITS_ENV_RECENT_DAYS="$(MLB_HITS_ENV_RECENT_DAYS)" MLB_HITS_ENV_STARTER_BASELINE_SEASONS="$(MLB_HITS_ENV_STARTER_BASELINE_SEASONS)" MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS="$(MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS)" MLB_HITS_ENV_STARTER_BASELINE_DECAY="$(MLB_HITS_ENV_STARTER_BASELINE_DECAY)" MLB_HITS_ENV_SLATE_WEIGHT_LAST7="$(MLB_HITS_ENV_SLATE_WEIGHT_LAST7)" MLB_HITS_ENV_SLATE_WEIGHT_LAST15="$(MLB_HITS_ENV_SLATE_WEIGHT_LAST15)" MLB_HITS_ENV_SLATE_WEIGHT_LAST30="$(MLB_HITS_ENV_SLATE_WEIGHT_LAST30)" MLB_HITS_ENV_SLATE_FACTOR_MIN="$(MLB_HITS_ENV_SLATE_FACTOR_MIN)" MLB_HITS_ENV_SLATE_FACTOR_MAX="$(MLB_HITS_ENV_SLATE_FACTOR_MAX)" MLB_HITS_ENV_SLATE_DATE="$(MLB_HITS_ENV_SLATE_DATE)" MLB_HITS_ENV_SLATE_CSV="$(MLB_HITS_ENV_SLATE_CSV)" MLB_HITS_ENV_WIDE_CSV="$(MLB_HITS_ENV_WIDE_CSV)" MLB_HITS_ENV_OUT_JSON="$(MLB_HITS_ENV_OUT_JSON)" MLB_HITS_ENV_OUT_CSV="$(MLB_HITS_ENV_OUT_CSV)" MLB_HITS_ENV_HISTORY_JSONL="$(MLB_HITS_ENV_HISTORY_JSONL)" MLB_HITS_ENV_EVAL_TRACKER_CSV="$(MLB_HITS_ENV_EVAL_TRACKER_CSV)"; \
		hits_env_rc=$$?; \
		set -e; \
		if [ "$$hits_env_rc" -ne 0 ]; then \
			if [ "$(MLB_DAILY_HITS_ENV_REQUIRED)" = "1" ]; then \
				echo "mlb-daily-refresh: hits-environment monitor failed rc=$$hits_env_rc"; \
				exit "$$hits_env_rc"; \
			fi; \
			echo "mlb-daily-refresh: WARN hits-environment monitor failed rc=$$hits_env_rc; continuing"; \
		fi; \
	else \
		echo "mlb-daily-refresh: skipping hits-environment monitor (MLB_DAILY_HITS_ENV_ENABLED=$(MLB_DAILY_HITS_ENV_ENABLED))"; \
	fi
	echo "mlb-daily-refresh: refreshing prop regime validation";
	$(MAKE) mlb-prop-regime-validation
	echo "mlb-daily-refresh: building model performance by prop";
	$(MAKE) mlb-model-performance-by-prop MLB_MODEL_PERFORMANCE_TO_DATE="$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)" MLB_MODEL_PERFORMANCE_SOURCE_TYPE="full_slate_model_pick"
	echo "mlb-daily-refresh: building reporting alignment audit";
	$(MAKE) mlb-reporting-alignment-audit MLB_REPORTING_ALIGNMENT_DATE="$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)"
	@if [ "$(MLB_DAILY_OPS_BRIEF_ENABLED)" = "1" ]; then \
		echo "mlb-daily-refresh: building daily ops brief"; \
		set +e; \
		$(MAKE) mlb-daily-ops-brief MLB_DAILY_BRIEF_REFRESH_BVP_IMPACT=0 MLB_DAILY_BRIEF_REQUIRE_FRESH_BVP_IMPACT="$(MLB_DAILY_BRIEF_REQUIRE_FRESH_BVP_IMPACT)" MLB_DAILY_BRIEF_REPORT_DATE="$(MLB_DAILY_BRIEF_REPORT_DATE)" MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE="$(MLB_DAILY_BRIEF_COMPLETED_SLATE_DATE)" MLB_DAILY_BRIEF_CURRENT_SLATE_DATE="$(MLB_DAILY_BRIEF_CURRENT_SLATE_DATE)" MLB_DAILY_BRIEF_POSTGRADE_ALERTS_JSON="$(MLB_DAILY_BRIEF_POSTGRADE_ALERTS_JSON)" MLB_DAILY_BRIEF_MODEL_VS_FADE_JSON="$(MLB_DAILY_BRIEF_MODEL_VS_FADE_JSON)" MLB_DAILY_BRIEF_BVP_IMPACT_JSON="$(MLB_DAILY_BRIEF_BVP_IMPACT_JSON)" MLB_DAILY_BRIEF_HITS_ENV_JSON="$(MLB_DAILY_BRIEF_HITS_ENV_JSON)" MLB_DAILY_BRIEF_PIPELINE_HISTORY_JSONL="$(MLB_DAILY_BRIEF_PIPELINE_HISTORY_JSONL)" MLB_DAILY_BRIEF_OPS_HISTORY_JSONL="$(MLB_DAILY_BRIEF_OPS_HISTORY_JSONL)" MLB_DAILY_BRIEF_OUT_MD="$(MLB_DAILY_BRIEF_OUT_MD)" MLB_DAILY_BRIEF_DATED_OUT_MD="$(MLB_DAILY_BRIEF_DATED_OUT_MD)" MLB_DAILY_BRIEF_OUT_JSON="$(MLB_DAILY_BRIEF_OUT_JSON)" MLB_DAILY_BRIEF_HISTORY_JSONL="$(MLB_DAILY_BRIEF_HISTORY_JSONL)"; \
		brief_rc=$$?; \
		set -e; \
		if [ "$$brief_rc" -ne 0 ]; then \
			if [ "$(MLB_DAILY_OPS_BRIEF_REQUIRED)" = "1" ]; then \
				echo "mlb-daily-refresh: daily ops brief failed rc=$$brief_rc"; \
				exit "$$brief_rc"; \
			fi; \
			echo "mlb-daily-refresh: WARN daily ops brief failed rc=$$brief_rc; continuing"; \
		fi; \
	else \
		echo "mlb-daily-refresh: skipping daily ops brief (MLB_DAILY_OPS_BRIEF_ENABLED=$(MLB_DAILY_OPS_BRIEF_ENABLED))"; \
	fi

# Strict daily baseline: enforces stat-derived volume guard.
mlb-daily-refresh-strict:
	$(MAKE) mlb-daily-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS) MLB_ROSTER_DATE=$(MLB_ROSTER_DATE) MLB_DAILY_BVP_PVB_ENABLED=$(MLB_DAILY_BVP_PVB_ENABLED) MLB_BVP_FEATURE_SET_TAG="$(MLB_BVP_FEATURE_SET_TAG)" MLB_BVP_MODEL_TAG="$(MLB_BVP_MODEL_TAG)" MLB_BVP_BATCH_SIZE="$(MLB_BVP_BATCH_SIZE)" MLB_BVP_REQUEST_TIMEOUT_SEC="$(MLB_BVP_REQUEST_TIMEOUT_SEC)" MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=$(MLB_STAT_MAX_GAMES) MLB_STAT_SKIP_EXISTING_DATES=$(MLB_STAT_SKIP_EXISTING_DATES) MLB_STAT_BATTER_SAMPLE_RATIO=$(MLB_STAT_BATTER_SAMPLE_RATIO) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=1 MLB_DAILY_INCLUDE_CAPTURE=$(MLB_DAILY_INCLUDE_CAPTURE) MLB_DAILY_BVP_IMPACT_ENABLED=$(MLB_DAILY_BVP_IMPACT_ENABLED) MLB_DAILY_BVP_IMPACT_REQUIRED=$(MLB_DAILY_BVP_IMPACT_REQUIRED) MLB_DAILY_HITS_ENV_ENABLED=$(MLB_DAILY_HITS_ENV_ENABLED) MLB_DAILY_HITS_ENV_REQUIRED=$(MLB_DAILY_HITS_ENV_REQUIRED) MLB_DAILY_OPS_BRIEF_ENABLED=$(MLB_DAILY_OPS_BRIEF_ENABLED) MLB_DAILY_OPS_BRIEF_REQUIRED=$(MLB_DAILY_OPS_BRIEF_REQUIRED) MLB_HITS_ENV_AS_OF_DATE="$(MLB_HITS_ENV_AS_OF_DATE)" MLB_HITS_ENV_LOOKBACK_DAYS="$(MLB_HITS_ENV_LOOKBACK_DAYS)" MLB_HITS_ENV_RECENT_DAYS="$(MLB_HITS_ENV_RECENT_DAYS)" MLB_HITS_ENV_STARTER_BASELINE_SEASONS="$(MLB_HITS_ENV_STARTER_BASELINE_SEASONS)" MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS="$(MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS)" MLB_HITS_ENV_STARTER_BASELINE_DECAY="$(MLB_HITS_ENV_STARTER_BASELINE_DECAY)" MLB_HITS_ENV_SLATE_WEIGHT_LAST7="$(MLB_HITS_ENV_SLATE_WEIGHT_LAST7)" MLB_HITS_ENV_SLATE_WEIGHT_LAST15="$(MLB_HITS_ENV_SLATE_WEIGHT_LAST15)" MLB_HITS_ENV_SLATE_WEIGHT_LAST30="$(MLB_HITS_ENV_SLATE_WEIGHT_LAST30)" MLB_HITS_ENV_SLATE_FACTOR_MIN="$(MLB_HITS_ENV_SLATE_FACTOR_MIN)" MLB_HITS_ENV_SLATE_FACTOR_MAX="$(MLB_HITS_ENV_SLATE_FACTOR_MAX)" MLB_HITS_ENV_SLATE_DATE="$(MLB_HITS_ENV_SLATE_DATE)" MLB_HITS_ENV_SLATE_CSV="$(MLB_HITS_ENV_SLATE_CSV)" MLB_HITS_ENV_WIDE_CSV="$(MLB_HITS_ENV_WIDE_CSV)" MLB_HITS_ENV_OUT_JSON="$(MLB_HITS_ENV_OUT_JSON)" MLB_HITS_ENV_OUT_CSV="$(MLB_HITS_ENV_OUT_CSV)" MLB_HITS_ENV_HISTORY_JSONL="$(MLB_HITS_ENV_HISTORY_JSONL)" MLB_HITS_ENV_EVAL_TRACKER_CSV="$(MLB_HITS_ENV_EVAL_TRACKER_CSV)" MLB_DAILY_BRIEF_REPORT_DATE="$(MLB_DAILY_BRIEF_REPORT_DATE)" MLB_DAILY_BRIEF_POSTGRADE_ALERTS_JSON="$(MLB_DAILY_BRIEF_POSTGRADE_ALERTS_JSON)" MLB_DAILY_BRIEF_MODEL_VS_FADE_JSON="$(MLB_DAILY_BRIEF_MODEL_VS_FADE_JSON)" MLB_DAILY_BRIEF_BVP_IMPACT_JSON="$(MLB_DAILY_BRIEF_BVP_IMPACT_JSON)" MLB_DAILY_BRIEF_HITS_ENV_JSON="$(MLB_DAILY_BRIEF_HITS_ENV_JSON)" MLB_DAILY_BRIEF_PIPELINE_HISTORY_JSONL="$(MLB_DAILY_BRIEF_PIPELINE_HISTORY_JSONL)" MLB_DAILY_BRIEF_OPS_HISTORY_JSONL="$(MLB_DAILY_BRIEF_OPS_HISTORY_JSONL)" MLB_DAILY_BRIEF_OUT_MD="$(MLB_DAILY_BRIEF_OUT_MD)" MLB_DAILY_BRIEF_DATED_OUT_MD="$(MLB_DAILY_BRIEF_DATED_OUT_MD)" MLB_DAILY_BRIEF_OUT_JSON="$(MLB_DAILY_BRIEF_OUT_JSON)" MLB_DAILY_BRIEF_HISTORY_JSONL="$(MLB_DAILY_BRIEF_HISTORY_JSONL)"

# Daily baseline smoke mode: quick end-to-end validation with max one game/date.
mlb-daily-refresh-smoke:
	$(MAKE) mlb-daily-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS) MLB_ROSTER_DATE=$(MLB_ROSTER_DATE) MLB_DAILY_BVP_PVB_ENABLED=0 MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_MAX_GAMES=1 MLB_STAT_SKIP_EXISTING_DATES=0 MLB_STAT_BATTER_SAMPLE_RATIO=$(MLB_STAT_BATTER_SAMPLE_RATIO) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN) MLB_DAILY_INCLUDE_CAPTURE=0 MLB_DAILY_BVP_IMPACT_ENABLED=0 MLB_DAILY_HITS_ENV_ENABLED=0

# Ops confidence loop for MLB: config snapshot + quick daily smoke + deployed API smoke.
mlb-ops-check:
	$(MAKE) mlb-show-config
	$(MAKE) mlb-market-cache-refresh MLB_MARKET_DAYS=$(MLB_MARKET_DAYS)
	$(MAKE) mlb-roster-refresh-all MLB_ROSTER_DATE=$(MLB_ROSTER_DATE)
	$(MAKE) mlb-stat-derived-smoke MLB_STAT_DAYS_AGO=$(MLB_STAT_DAYS_AGO) MLB_STAT_FROM_DATE="$(MLB_STAT_FROM_DATE)" MLB_STAT_TO_DATE="$(MLB_STAT_TO_DATE)" MLB_STAT_BATTER_SAMPLE_RATIO=$(MLB_STAT_BATTER_SAMPLE_RATIO) MLB_STAT_DERIVED_DAYS=$(MLB_STAT_DERIVED_DAYS) MLB_STAT_DERIVED_MIN=$(MLB_STAT_DERIVED_MIN)
	$(MAKE) mlb-post-deploy BASE_URL=$(BASE_URL) MLB_DATE=$(MLB_DATE)

# API contract check for /api/player-profile payload consumed by frontend.
mlb-checks-profile-contract:
	$(VENV_PY) backend/mlb/scripts/validate_mlb_profile_contract.py

# Fast deployed-environment health check (safe, no write operations).
mlb-post-deploy:
	$(VENV_PY) backend/_legacy/scripts/post_deploy_mlb_check.py --base-url $(BASE_URL) --date $(MLB_DATE)

# Post-deploy check that also requires non-sparse probe data.
mlb-post-deploy-strict:
	$(VENV_PY) backend/_legacy/scripts/post_deploy_mlb_check.py --base-url $(BASE_URL) --date $(MLB_DATE) --require-data

# Post-deploy strict transport/DB checks, but tolerate sparse probe data (offseason-safe).
mlb-post-deploy-strict-offseason:
	$(VENV_PY) backend/_legacy/scripts/post_deploy_mlb_check.py --base-url $(BASE_URL) --date $(MLB_DATE) --require-data --allow-sparse

# One-command MLB release confidence gate (offseason-safe strict deploy check).
mlb-release-check: mlb-checks-offline
	$(MAKE) mlb-post-deploy-strict-offseason BASE_URL=$(BASE_URL) MLB_DATE=$(MLB_DATE)

# Fast NHL deployed-environment health check (safe, no write operations).
nhl-post-deploy:
	$(VENV_PY) backend/_legacy/scripts/post_deploy_nhl_check.py --base-url $(BASE_URL) --date $(NHL_DATE)

# NHL post-deploy check requiring non-sparse probe data.
nhl-post-deploy-strict:
	$(VENV_PY) backend/_legacy/scripts/post_deploy_nhl_check.py --base-url $(BASE_URL) --date $(NHL_DATE) --require-data

# NHL post-deploy strict transport/DB checks, but tolerate sparse probe data.
nhl-post-deploy-strict-offseason:
	$(VENV_PY) backend/_legacy/scripts/post_deploy_nhl_check.py --base-url $(BASE_URL) --date $(NHL_DATE) --require-data --allow-sparse

# NHL OpenAPI contract drift check.
nhl-openapi-contract:
	$(VENV_PY) backend/_legacy/scripts/check_nhl_openapi_contract.py

nhl-prediction-quality:
	@if [ -z "$(NHL_QUALITY_FROM_DATE)" ] || [ -z "$(NHL_QUALITY_TO_DATE)" ]; then \
		echo "nhl-prediction-quality requires NHL_QUALITY_FROM_DATE and NHL_QUALITY_TO_DATE"; \
		exit 2; \
	fi
	$(VENV_PY) backend/nhl/scripts/analyze_nhl_prediction_quality.py --from-date $(NHL_QUALITY_FROM_DATE) --to-date $(NHL_QUALITY_TO_DATE) --min-total $(NHL_QUALITY_MIN_TOTAL)

nhl-prediction-quality-auto:
	@if [ -z "$(NHL_QUALITY_FROM_DATE)" ] || [ -z "$(NHL_QUALITY_TO_DATE)" ]; then \
		echo "nhl-prediction-quality-auto requires NHL_QUALITY_FROM_DATE and NHL_QUALITY_TO_DATE"; \
		exit 2; \
	fi
	$(VENV_PY) backend/nhl/scripts/analyze_nhl_prediction_quality.py --from-date $(NHL_QUALITY_FROM_DATE) --to-date $(NHL_QUALITY_TO_DATE) --min-total $(NHL_QUALITY_ACTIVE_MIN_TOTAL) --auto-min-total

nhl-sog-quality-layers:
	$(VENV_PY) backend/nhl/scripts/analyze_nhl_sog_quality_layers.py \
		$(if $(NHL_SOG_MODEL_FAMILY),--model-family $(NHL_SOG_MODEL_FAMILY),) \
		$(if $(NHL_SOG_MODEL_VERSION),--model-version $(NHL_SOG_MODEL_VERSION),) \
		$(if $(NHL_SOG_LINES),--lines $(NHL_SOG_LINES),) \
		$(if $(NHL_SOG_FROM_DATE),--from-date $(NHL_SOG_FROM_DATE),) \
		$(if $(NHL_SOG_TO_DATE),--to-date $(NHL_SOG_TO_DATE),) \
		--lookback-days $(NHL_SOG_LOOKBACK_DAYS) \
		--recent-days $(NHL_SOG_RECENT_DAYS) \
		--segment-min-n $(NHL_SOG_SEGMENT_MIN_N) \
		--decile-min-n $(NHL_SOG_DECILE_MIN_N) \
		--player-min-n $(NHL_SOG_PLAYER_MIN_N) \
		--player-top-n $(NHL_SOG_PLAYER_TOP_N) \
		--worst-limit $(NHL_SOG_WORST_LIMIT) \
		$(if $(NHL_SOG_OUTPUT),--output $(NHL_SOG_OUTPUT),)

nhl-sog-segmented-calibration-experiment:
	$(VENV_PY) backend/nhl/scripts/experiment_nhl_sog_segmented_calibration.py \
		--model-family $(NHL_SOG_CAL_MODEL_FAMILY) \
		--model-version $(NHL_SOG_CAL_MODEL_VERSION) \
		--lines $(NHL_SOG_CAL_LINES) \
		$(if $(NHL_SOG_CAL_FROM_DATE),--from-date $(NHL_SOG_CAL_FROM_DATE),) \
		$(if $(NHL_SOG_CAL_TO_DATE),--to-date $(NHL_SOG_CAL_TO_DATE),) \
		--lookback-days $(NHL_SOG_CAL_LOOKBACK_DAYS) \
		--holdout-days $(NHL_SOG_CAL_HOLDOUT_DAYS) \
		--segment-min-rows $(NHL_SOG_CAL_SEGMENT_MIN_ROWS) \
		--blend-alpha $(NHL_SOG_CAL_BLEND_ALPHA) \
		--decay-half-life-days $(NHL_SOG_CAL_DECAY_HALF_LIFE_DAYS) \
		$(if $(NHL_SOG_CAL_OUTPUT),--output $(NHL_SOG_CAL_OUTPUT),)

nhl-sog-calibration-baseline:
	@if [ -z "$(NHL_SOG_BASELINE_FROM_DATE)" ] || [ -z "$(NHL_SOG_BASELINE_TO_DATE)" ]; then \
		echo "nhl-sog-calibration-baseline requires NHL_SOG_BASELINE_FROM_DATE and NHL_SOG_BASELINE_TO_DATE"; \
		exit 2; \
	fi
	$(VENV_PY) backend/nhl/scripts/experiment_nhl_sog_segmented_calibration.py \
		--model-family $(NHL_SOG_CAL_MODEL_FAMILY) \
		--model-version $(NHL_SOG_CAL_MODEL_VERSION) \
		--lines $(NHL_SOG_CAL_LINES) \
		--from-date $(NHL_SOG_BASELINE_FROM_DATE) \
		--to-date $(NHL_SOG_BASELINE_TO_DATE) \
		--lookback-days $(NHL_SOG_CAL_LOOKBACK_DAYS) \
		--holdout-days $(NHL_SOG_CAL_HOLDOUT_DAYS) \
		--segment-min-rows $(NHL_SOG_CAL_SEGMENT_MIN_ROWS) \
		--blend-alpha $(NHL_SOG_CAL_BLEND_ALPHA) \
		--decay-half-life-days $(NHL_SOG_CAL_DECAY_HALF_LIFE_DAYS) \
		--output $(NHL_SOG_BASELINE_OUTPUT)

nhl-sog-calibration-log:
	$(VENV_PY) backend/nhl/scripts/nhl_sog_calibration_log.py \
		--output $(NHL_SOG_MONITOR_HISTORY_INPUT) \
		--model-family $(NHL_SOG_CAL_MODEL_FAMILY) \
		--model-version $(NHL_SOG_CAL_MODEL_VERSION) \
		--lines $(NHL_SOG_CAL_LINES) \
		$(if $(NHL_SOG_CAL_FROM_DATE),--from-date $(NHL_SOG_CAL_FROM_DATE),) \
		$(if $(NHL_SOG_CAL_TO_DATE),--to-date $(NHL_SOG_CAL_TO_DATE),) \
		--lookback-days $(NHL_SOG_CAL_LOOKBACK_DAYS) \
		--holdout-days $(NHL_SOG_CAL_HOLDOUT_DAYS) \
		--segment-min-rows $(NHL_SOG_CAL_SEGMENT_MIN_ROWS) \
		--blend-alpha $(NHL_SOG_CAL_BLEND_ALPHA) \
		--decay-half-life-days $(NHL_SOG_CAL_DECAY_HALF_LIFE_DAYS) \
		--required-lines $(NHL_SOG_MONITOR_REQUIRED_LINES) \
		--max-delta-brier-vs-raw $(NHL_SOG_MONITOR_MAX_DELTA_BRIER) \
		--max-delta-logloss-vs-raw $(NHL_SOG_MONITOR_MAX_DELTA_LOGLOSS)

nhl-sog-calibration-last:
	@if [ ! -f "$(NHL_SOG_MONITOR_HISTORY_INPUT)" ]; then \
		echo "history file missing: $(NHL_SOG_MONITOR_HISTORY_INPUT)"; \
		exit 2; \
	fi
	@tail -n $(NHL_SOG_MONITOR_HISTORY_LIMIT) $(NHL_SOG_MONITOR_HISTORY_INPUT)

nhl-sog-calibration-history-clean:
	$(VENV_PY) backend/_legacy/scripts/clean_nhl_sog_calibration_history.py \
		--input $(NHL_SOG_MONITOR_HISTORY_INPUT) \
		--in-place \
		$(if $(filter 1,$(NHL_SOG_MONITOR_HISTORY_CLEAN_BACKUP)),--backup,)

nhl-sog-segmented-calibrate-file:
	@if [ -z "$(NHL_SOG_SEG_CAL_PRED_CSV)" ]; then \
		echo "nhl-sog-segmented-calibrate-file requires NHL_SOG_SEG_CAL_PRED_CSV"; \
		exit 2; \
	fi
	$(VENV_PY) backend/nhl/scripts/calibrate_sog_segmented_recency.py \
		--pred-csv $(NHL_SOG_SEG_CAL_PRED_CSV) \
		$(if $(NHL_SOG_SEG_CAL_OUT_CSV),--out-csv $(NHL_SOG_SEG_CAL_OUT_CSV),) \
		--model-family $(NHL_SOG_CAL_MODEL_FAMILY) \
		--model-version $(NHL_SOG_CAL_MODEL_VERSION) \
		--lines $(NHL_SOG_CAL_LINES) \
		--lookback-days $(NHL_SOG_CAL_LOOKBACK_DAYS) \
		--segment-min-rows $(NHL_SOG_CAL_SEGMENT_MIN_ROWS) \
		--blend-alpha $(NHL_SOG_CAL_BLEND_ALPHA) \
		--decay-half-life-days $(NHL_SOG_CAL_DECAY_HALF_LIFE_DAYS) \
		$(if $(NHL_SOG_SEG_CAL_ASOF_DATE),--asof-date $(NHL_SOG_SEG_CAL_ASOF_DATE),) \
		$(if $(filter 1,$(NHL_SOG_SEG_CAL_STRICT)),--strict,)

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
	$(VENV_PY) backend/nhl/scripts/check_nhl_workflow_compat.py

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
