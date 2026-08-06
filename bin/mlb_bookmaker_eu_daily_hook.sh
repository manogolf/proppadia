#!/bin/zsh
# Compatibility entry point retained for the accepted BookMaker.eu adapter.
# Acquisition is provider-wide; there is no separate book-specific request.
exec bin/mlb_sportsgameodds_main_market_trial_daily_hook.sh "$@"
