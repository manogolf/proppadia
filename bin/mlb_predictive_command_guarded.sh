#!/bin/zsh
# Exact fail-closed adapter for retired MLB predictive-output commands.
set -u

stage=""
operation=""
status_file=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --stage) stage="${2:-}"; shift 2 ;;
    --operation) operation="${2:-}"; shift 2 ;;
    --status-file) status_file="${2:-}"; shift 2 ;;
    --) shift; break ;;
    *) echo "usage: $0 --stage LABEL --operation OP [--status-file PATH] -- COMMAND..." >&2; exit 64 ;;
  esac
done

case "${operation}" in
  production_slate_generation|production_upload_generation|production_ranking_and_routing) ;;
  *) echo "UNLISTED_MLB_PREDICTIVE_OPERATION operation=${operation}" >&2; exit 64 ;;
esac
if [[ -z "${stage}" || "$#" -eq 0 ]]; then
  echo "MISSING_MLB_PREDICTIVE_GUARD_ARGUMENT" >&2
  exit 64
fi

stdout_capture="$(mktemp -t mlb-predictive.stdout.XXXXXX)"
stderr_capture="$(mktemp -t mlb-predictive.stderr.XXXXXX)"
cleanup() { rm -f "${stdout_capture}" "${stderr_capture}"; }
trap cleanup EXIT

set +e
"$@" >"${stdout_capture}" 2>"${stderr_capture}"
command_rc=$?
set -e
cat "${stdout_capture}"
cat "${stderr_capture}" >&2

write_status() {
  [[ -z "${status_file}" ]] && return 0
  mkdir -p "${status_file:h}"
  print -r -- "$1" >"${status_file}"
}

if [[ "${command_rc}" -eq 0 ]]; then
  write_status "SUCCESS operation=${operation}"
  exit 0
fi

if grep -Fq "MLBPredictiveModelBlocked" "${stderr_capture}" \
  && grep -Fq "MLB_PREDICTIVE_MODEL_BLOCKED_NO_QUALIFIED_MODEL" "${stderr_capture}" \
  && grep -Fq "operation=${operation}" "${stderr_capture}"; then
  echo "[$(date -u +%FT%TZ)] SKIP ${stage}: NO_QUALIFIED_MLB_MODEL operation=${operation}"
  write_status "SKIPPED operation=${operation}"
  exit 0
fi

write_status "FAILED operation=${operation} rc=${command_rc}"
exit "${command_rc}"
