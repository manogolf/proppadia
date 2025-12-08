#!/usr/bin/env bash
set -euo pipefail

START=2025-10-07
END=2025-12-05       # <- end of season, adjust as needed
CHUNK=7

CURRENT=$START

while [[ "$CURRENT" < "$END" ]]; do
  NEXT=$(date -j -v+"${CHUNK}"d -f "%Y-%m-%d" "$CURRENT" +"%Y-%m-%d")
  if [[ "$NEXT" > "$END" ]]; then
    NEXT=$END
  fi

  echo ""
  echo "=== Backfilling $CURRENT .. $NEXT ==="
  python backend/nhl/scripts/backfill_denali_predictions.py \
      --start "$CURRENT" --end "$NEXT" --chunk-days "$CHUNK"

  CURRENT=$NEXT
done

echo ""
echo "=== All chunks complete ==="
