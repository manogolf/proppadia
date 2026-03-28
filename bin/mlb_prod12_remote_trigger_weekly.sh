#!/usr/bin/env bash
set -euo pipefail

# Convenience wrapper for schedulers: weekly phase2 in low-memory sequence mode
# (one prop at a time with pause between props).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export MLB_CRON_RUN_MODE="${MLB_CRON_RUN_MODE:-weekly}"
export MLB_WEEKLY_PHASE2_ENABLED="${MLB_WEEKLY_PHASE2_ENABLED:-1}"
export MLB_WEEKLY_PROP_SEQUENCE_ENABLED="${MLB_WEEKLY_PROP_SEQUENCE_ENABLED:-1}"
export MLB_WEEKLY_PROP_SEQUENCE_CONTINUE_ON_ERROR="${MLB_WEEKLY_PROP_SEQUENCE_CONTINUE_ON_ERROR:-1}"
export MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC="${MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC:-8}"
export MLB_WEEKLY_PROP_SEQUENCE="${MLB_WEEKLY_PROP_SEQUENCE:-hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis}"
# Keep retrain/recompute off by default on Render weekly triggers.
# Preferred workflow: run cadence locally, then publish bundle.
export MLB_WEEKLY_RETRAIN_CADENCE_ENABLED="${MLB_WEEKLY_RETRAIN_CADENCE_ENABLED:-0}"
export MLB_WEEKLY_RETRAIN_CADENCE_REQUIRED="${MLB_WEEKLY_RETRAIN_CADENCE_REQUIRED:-0}"

# Safer weekly defaults for constrained instances.
export MLB_REPLAY_SAMPLE="${MLB_REPLAY_SAMPLE:-1}"
export MLB_REPLAY_MIN_SUCCESS="${MLB_REPLAY_MIN_SUCCESS:-1}"
export MLB_REPLAY_RETRY_ATTEMPTS="${MLB_REPLAY_RETRY_ATTEMPTS:-2}"
export MLB_REPLAY_RETRY_BACKOFF_MS="${MLB_REPLAY_RETRY_BACKOFF_MS:-350}"
export MLB_REPLAY_MAX_PREDICT_P95_MS="${MLB_REPLAY_MAX_PREDICT_P95_MS:-12000}"
export MLB_CANDIDATE_MIN_TOTAL="${MLB_CANDIDATE_MIN_TOTAL:-0}"
export MLB_PROD12_MIN_LIFT_PCT="${MLB_PROD12_MIN_LIFT_PCT:--5}"
export MLB_PROD12_MAX_PROP_DROP_PCT="${MLB_PROD12_MAX_PROP_DROP_PCT:-3.5}"

exec "${SCRIPT_DIR}/mlb_prod12_remote_trigger.sh"
