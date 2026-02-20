#!/usr/bin/env bash
set -euo pipefail

# Download the latest prod12 model bundle and persist it on the service disk.
# This script is intended for weekly execution (or manual repair), not daily.

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

: "${SUPABASE_URL:?mlb_prod12_model_bundle_sync requires SUPABASE_URL}"
: "${SUPABASE_SECRET_KEY:?mlb_prod12_model_bundle_sync requires SUPABASE_SECRET_KEY}"

MLB_MODELS_OBJECT_PATH="${MLB_MODELS_OBJECT_PATH:-mlb/prod12/latest.tgz}"
MODEL_DIR="${MODEL_DIR:-/var/data/proppadia/models}"
MODEL_STAGING_DIR="${MODEL_STAGING_DIR:-/tmp/mlb_models_unpack}"
MODEL_TARBALL="${MODEL_TARBALL:-/tmp/mlb_latest.tgz}"

echo "[prod12-model-sync] object path: ${MLB_MODELS_OBJECT_PATH}"
echo "[prod12-model-sync] target MODEL_DIR: ${MODEL_DIR}"

rm -rf "${MODEL_STAGING_DIR}" "${MODEL_TARBALL}"
mkdir -p "${MODEL_STAGING_DIR}" "${MODEL_DIR}/latest"

if ! curl -fsSL \
  -H "Authorization: Bearer ${SUPABASE_SECRET_KEY}" \
  -H "apikey: ${SUPABASE_SECRET_KEY}" \
  "${SUPABASE_URL}/storage/v1/object/models/${MLB_MODELS_OBJECT_PATH}" \
  -o "${MODEL_TARBALL}"; then
  echo "[prod12-model-sync] ERROR: unable to download models/${MLB_MODELS_OBJECT_PATH}" >&2
  echo "[prod12-model-sync] Hint: publish bundle with a stable alias (mlb/prod12/latest.tgz) or update MLB_MODELS_OBJECT_PATH" >&2
  exit 22
fi

tar -xzf "${MODEL_TARBALL}" -C "${MODEL_STAGING_DIR}"

if [[ ! -d "${MODEL_STAGING_DIR}/latest" ]]; then
  echo "[prod12-model-sync] ERROR: unpacked bundle missing latest/" >&2
  find "${MODEL_STAGING_DIR}" -maxdepth 3 -type f | sed "s#^#[prod12-model-sync] unpack file: #"
  exit 3
fi

# Remove macOS sidecar metadata files if present.
find "${MODEL_STAGING_DIR}/latest" -maxdepth 1 -type f -name '._*' -delete

rsync -a --delete "${MODEL_STAGING_DIR}/latest/" "${MODEL_DIR}/latest/"

# Weekly readiness manifest expects models_out/latest to exist.
mkdir -p models_out
rsync -a --delete "${MODEL_DIR}/latest/" models_out/latest/

printf '{"synced_at":"%s","object_path":"%s"}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "${MLB_MODELS_OBJECT_PATH}" > "${MODEL_DIR}/latest/SYNC_INFO.json"

echo "[prod12-model-sync] sync complete"
