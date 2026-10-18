#!/bin/zsh
status_args=()
if [[ -n "${MLB_SLATE_OUTPUT_GATE_STATUS_FILE:-}" ]]; then
  status_args=(--status-file "${MLB_SLATE_OUTPUT_GATE_STATUS_FILE}")
fi
exec "${0:A:h}/mlb_predictive_command_guarded.sh" \
  --stage "MLB slate output" \
  --operation production_slate_generation \
  "${status_args[@]}" \
  -- make mlb-slate-output "$@"
