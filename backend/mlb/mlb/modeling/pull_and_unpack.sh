#!/usr/bin/env bash
set -euo pipefail

: "${MODELS_REPO:?set MODELS_REPO like owner/repo}"
DEST="${MODELS_DIR:-/var/data/models}"
TAG="${MODELS_TAG:-models-latest}"      # or a timestamp tag
USE_LATEST="${USE_LATEST_API:-0}"

# Optional auth header (fine if missing on public repos)
AUTH=()
[ -n "${GH_TOKEN:-}" ]      && AUTH=(-H "Authorization: token $GH_TOKEN")
[ ${#AUTH[@]} -eq 0 ] && [ -n "${GITHUB_TOKEN:-}" ] && AUTH=(-H "Authorization: token $GITHUB_TOKEN")

TMPROOT="$(mktemp -d)"
trap 'rm -rf "$TMPROOT"' EXIT
JSON="$TMPROOT/release.json"
WORK="$TMPROOT/extract"
TAR="$TMPROOT/models.tar.gz"
mkdir -p "$WORK" "$DEST"

# Pick endpoint
if [ "$USE_LATEST" = "1" ]; then
  API_URL="https://api.github.com/repos/$MODELS_REPO/releases/latest"
else
  API_URL="https://api.github.com/repos/$MODELS_REPO/releases/tags/$TAG"
fi

echo "[pull_and_unpack] Fetching release metadata: $API_URL"
curl -fsSL "${AUTH[@]}" "$API_URL" -o "$JSON"

# Extract the first *.tar.gz (prefer .tar.gz over .tgz)
ASSET_URL="$(python3 - "$JSON" <<'PY'
import sys, json
path = sys.argv[1]
d = json.load(open(path))
assets = d.get("assets", [])
best = None
for a in assets:
    n = (a.get("name") or "").lower()
    if n.endswith(".tar.gz") or n.endswith(".tgz"):
        best = a.get("browser_download_url")
        if n.endswith(".tar.gz"):
            break
print(best or "")
PY
)"
if [ -z "$ASSET_URL" ]; then
  echo "[pull_and_unpack] ERROR: no .tar.gz asset found in release JSON"
  exit 1
fi

echo "[pull_and_unpack] Downloading asset: $ASSET_URL"
curl -fsSL "${AUTH[@]}" "$ASSET_URL" -o "$TAR"

echo "[pull_and_unpack] Unpacking to temp dir…"
tar -xzf "$TAR" -C "$WORK" --strip-components=1 || {
  echo "[pull_and_unpack] NOTE: retrying without strip-components=1"
  rm -rf "$WORK" && mkdir -p "$WORK"
  tar -xzf "$TAR" -C "$WORK"
}

# If tarball contains a top-level "latest" folder, flatten it
if [ -d "$WORK/latest" ] && [ -f "$WORK/latest/MODEL_INDEX.json" ]; then
  mv "$WORK/latest" "$WORK.tmp" && rm -rf "$WORK" && mv "$WORK.tmp" "$WORK"
fi

echo "${TAG}" > "$WORK/.version" || true

# Atomic swap into DEST/latest
if [ -d "$DEST/latest" ]; then
  rm -rf "$DEST/latest.prev"
  mv "$DEST/latest" "$DEST/latest.prev"
fi
mv "$WORK" "$DEST/latest"

echo "✅ Installed tag: ${TAG} → ${DEST}/latest"
ls -1 "$DEST/latest"/*.joblib 2>/dev/null | sed 's#^.*/# - #'
