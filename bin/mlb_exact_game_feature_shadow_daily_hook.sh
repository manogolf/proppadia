#!/bin/zsh
set -euo pipefail

slate_date="${1:?slate_date required}"
run_identity="${2:?run_identity required}"
wrapper_started_at_utc="${3:?wrapper_started_at_utc required}"

echo "[$(date -u +%FT%TZ)] START exact-game feature shadow writer slate_date=${slate_date} run_identity=${run_identity} mode=READ_ONLY_SHADOW"
set +e
.venv/bin/python -m backend.mlb.scripts.run_mlb_exact_game_feature_shadow_v1 \
  --slate-date "$slate_date" \
  --run-identity "$run_identity" \
  --wrapper-started-at-utc "$wrapper_started_at_utc"
rc=$?
set -e
if [[ "$rc" -eq 0 ]]; then
  echo "[$(date -u +%FT%TZ)] DONE exact-game feature shadow writer slate_date=${slate_date} run_identity=${run_identity}"
else
  echo "[$(date -u +%FT%TZ)] WARN exact-game feature shadow writer failed rc=${rc} slate_date=${slate_date} run_identity=${run_identity}; production consumers remain isolated" >&2
fi
exit "$rc"
