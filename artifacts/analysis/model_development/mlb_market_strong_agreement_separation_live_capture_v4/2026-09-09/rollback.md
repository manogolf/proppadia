# Isolated v4 rollback

Disable only this stage by setting `MLB_AGREEMENT_SEPARATION_CAPTURE_V4_ENABLED=0` in the environment loaded by the existing wrapper. No LaunchAgent calendar change is needed.

For full removal, delete only the conditional v4 launch block immediately after `mlb_public_game_moneyline_daily_hook.sh` in `/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh`. The existing moneyline lifecycle and later Pinnacle h2h/totals/spreads hook stay in place. After disabling/removing the launch, the two v4 shell hooks and v4 client may be removed in a separate commit. Preserve the freeze, SQLite ledger, raw responses, request metadata, quota headers, and logs for audit.
