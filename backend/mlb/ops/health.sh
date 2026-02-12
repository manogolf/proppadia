#backend/scripts/ops/health.sh

# === 1) promote_models.sh — point latest → version and sanity-check files ===
mkdir -p backend/scripts/ops
cat > backend/scripts/ops/promote_models.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
BASE="${MODEL_DIR:-/var/data/models/props}"
VER="${1:-}"
if [[ -z "$VER" ]]; then
  echo "usage: $0 vYYYYMMDD" >&2; exit 2
fi
if [[ ! -d "$BASE/$VER" ]]; then
  echo "[ERR] missing $BASE/$VER" >&2; exit 1
fi

# Promote
ln -sfn "$BASE/$VER" "$BASE/latest"

# Create flat *.joblib links expected by the loader
shopt -s nullglob
for d in "$BASE/latest"/*/; do
  prop="${d%/}"; prop="${prop##*/}"
  cand=( "$d"/*poisson*.joblib "$d"/*zip_lambda*.joblib "$d"/*.joblib )
  [[ ${#cand[@]} -gt 0 ]] && ln -sfn "${cand[0]}" "$BASE/latest/$prop.joblib"
done
shopt -u nullglob

# Feature JSON sanity: must contain {"features": [...]}
bad=0
while IFS= read -r -d '' f; do
  if ! jq -e 'has("features") and (.features|type=="array")' "$f" >/dev/null; then
    echo "[WARN] $f missing .features array" >&2; bad=1
  fi
done < <(find "$BASE/latest" -maxdepth 2 -type f -name '*features*_v*.json' -print0)

# Summary
echo "latest -> $(readlink -f "$BASE/latest")"
find -L "$BASE/latest" -maxdepth 1 -name '*.joblib' -printf '%f\n' | sort
exit $bad
SH
chmod +x backend/scripts/ops/promote_models.sh

# === 2) smoke_api.sh — prepareProp then score-prop round-trip ===
cat > backend/scripts/ops/smoke_api.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
BASE="${BASE_URL:-https://baseball-streaks-sq44.onrender.com}"
pid="${1:-444482}"
gid="${2:-744996}"
ptype="${3:-total_bases}"
line="${4:-1.5}"
gdate="${5:-2024-07-04}"

echo "# prepareProp"
curl -sS -X POST "$BASE/api/prepareProp" -H 'Content-Type: application/json' \
  -d "{\"prop_type\":\"$ptype\",\"player_id\":$pid,\"game_id\":$gid,\"line\":$line}" | jq .

echo "# score-prop"
curl -sS -X POST "$BASE/api/score-prop" -H 'Content-Type: application/json' \
  -d "{\"prop_type\":\"$ptype\",\"player_id\":$pid,\"game_date\":\"$gdate\",\"line\":$line}" | jq .
SH
chmod +x backend/scripts/ops/smoke_api.sh

# === 3) score_prop_cli.sh — simple CLI for the site scoring endpoint ===
cat > backend/scripts/ops/score_prop_cli.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
BASE="${BASE_URL:-https://baseball-streaks-sq44.onrender.com}"
if [[ $# -lt 4 ]]; then
  echo "usage: $0 PROP_TYPE PLAYER_ID GAME_DATE LINE" >&2
  exit 2
fi
ptype="$1"; pid="$2"; gdt="$3"; line="$4"
curl -sS -X POST "$BASE/api/score-prop" -H 'Content-Type: application/json' \
  -d "{\"prop_type\":\"$ptype\",\"player_id\":$pid,\"game_date\":\"$gdt\",\"line\":$line}" | jq -c .
SH
chmod +x backend/scripts/ops/score_prop_cli.sh

# === 4) health.sh — cheap liveness + dependency check ===
cat > backend/scripts/ops/health.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
BASE="${BASE_URL:-https://baseball-streaks-sq44.onrender.com}"

curl -fsS "$BASE/openapi.json" >/dev/null && echo "openapi ✓" || { echo "openapi ✗"; exit 1; }

# Known good canary (padres @ rangers 2024-07-04)
curl -fsS -X POST "$BASE/api/prepareProp" -H 'Content-Type: application/json' \
  -d '{"prop_type":"total_bases","player_id":444482,"game_id":744996,"line":1.5}' >/dev/null && echo "prepareProp ✓" || { echo "prepareProp ✗"; exit 1; }

curl -fsS -X POST "$BASE/api/score-prop" -H 'Content-Type: application/json' \
  -d '{"prop_type":"total_bases","player_id":444482,"game_date":"2024-07-04","line":1.5}' >/dev/null && echo "score-prop ✓" || { echo "score-prop ✗"; exit 1; }
SH
chmod +x backend/scripts/ops/health.sh
