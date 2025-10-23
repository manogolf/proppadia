#!/usr/bin/env bash
set -euo pipefail
CMD="$*"
for n in 1 2 3; do
  echo "→ Attempt $n: $CMD"
  if bash -lc "$CMD"; then exit 0; fi
  sleep $((n*10))
done
exit 1
