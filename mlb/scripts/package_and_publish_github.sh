#   scripts/package_and_publish_github.sh

#!/usr/bin/env bash
set -euo pipefail

# ---- CONFIG (edit these) ----
OWNER="YOUR_GH_OWNER"              # e.g., jerrystrain
REPO="YOUR_REPO"                   # e.g., baseball-streaks
MODELS_DIR="${MODELS_DIR:-$PWD/models_out}"
TAG="models-latest"
ASSET="latest.tar.gz"
NOTES="Automated model bundle"
# -----------------------------

echo "[1/4] Ensure feature metadata is included"
cp backend/scripts/modeling/feature_metadata.json "$MODELS_DIR/feature_metadata.json"

echo "[2/4] Build tarball"
rm -f "$ASSET" "$ASSET.sha256"
tar -C "$MODELS_DIR" -czf "$ASSET" feature_metadata.json latest archive
shasum -a 256 "$ASSET" | awk '{print $1}' > "$ASSET.sha256"

echo "[3/4] Create or update release '$TAG'"
# If release doesn't exist, create it; if it exists, continue
if ! gh release view "$TAG" -R "$OWNER/$REPO" >/dev/null 2>&1; then
  gh release create "$TAG" -R "$OWNER/$REPO" -t "$TAG" -n "$NOTES" || true
fi

echo "[4/4] Upload assets (clobber)"
gh release upload "$TAG" "$ASSET" "$ASSET.sha256" -R "$OWNER/$REPO" --clobber

echo
echo "Published:"
echo "  bundle: https://github.com/$OWNER/$REPO/releases/download/$TAG/$ASSET"
echo "  sha256: https://github.com/$OWNER/$REPO/releases/download/$TAG/$ASSET.sha256"
