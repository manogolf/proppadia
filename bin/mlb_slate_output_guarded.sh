#!/bin/zsh
exec "${0:A:h}/mlb_predictive_command_guarded.sh" \
  --stage "MLB slate output" \
  --operation production_slate_generation \
  -- make mlb-slate-output "$@"
