#!/bin/zsh
# Run slate output fail-closed, except for the one governed retired-model state.
set -u

stdout_capture="$(mktemp -t mlb-slate-output.stdout.XXXXXX)"
stderr_capture="$(mktemp -t mlb-slate-output.stderr.XXXXXX)"
cleanup() {
  rm -f "${stdout_capture}" "${stderr_capture}"
}
trap cleanup EXIT

set +e
make mlb-slate-output "$@" >"${stdout_capture}" 2>"${stderr_capture}"
slate_output_rc=$?
set -e

cat "${stdout_capture}"
cat "${stderr_capture}" >&2

if [[ "${slate_output_rc}" -eq 0 ]]; then
  exit 0
fi

if grep -Fq "MLBPredictiveModelBlocked" "${stderr_capture}" \
  && grep -Fq "MLB_PREDICTIVE_MODEL_BLOCKED_NO_QUALIFIED_MODEL" "${stderr_capture}" \
  && grep -Fq "operation=production_slate_generation" "${stderr_capture}"; then
  echo "[$(date -u +%FT%TZ)] SKIP MLB slate output: NO_QUALIFIED_MLB_MODEL"
  exit 0
fi

exit "${slate_output_rc}"
