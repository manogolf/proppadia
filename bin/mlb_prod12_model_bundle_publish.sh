#!/usr/bin/env bash
set -euo pipefail

# Package and publish prod12 models to Supabase Storage.
# Writes both:
#   1) versioned object: mlb/prod12/mlb_latest_<timestamp>.tgz
#   2) stable alias:     mlb/prod12/latest.tgz

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

: "${SUPABASE_URL:?mlb_prod12_model_bundle_publish requires SUPABASE_URL}"
if [[ -z "${SUPABASE_SERVICE_ROLE_KEY:-}" && -z "${SUPABASE_SECRET_KEY:-}" ]]; then
  echo "mlb_prod12_model_bundle_publish requires SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SECRET_KEY" >&2
  exit 2
fi
SUPABASE_API_KEY="${SUPABASE_SERVICE_ROLE_KEY:-${SUPABASE_SECRET_KEY:-}}"

MODELS_DIR="${MODELS_DIR:-/var/data/proppadia/models}"
MODELS_BUCKET="${MODELS_BUCKET:-models}"

if [[ ! -d "${MODELS_DIR}/latest" ]]; then
  echo "mlb_prod12_model_bundle_publish: missing ${MODELS_DIR}/latest" >&2
  exit 2
fi

ts="$(date -u +%Y%m%dT%H%M%SZ)"
BUNDLE_OBJECT="${BUNDLE_OBJECT:-mlb/prod12/mlb_latest_${ts}.tgz}"
BUNDLE_LATEST_OBJECT="${BUNDLE_LATEST_OBJECT:-mlb/prod12/latest.tgz}"
MODEL_TARBALL="${MODEL_TARBALL:-/tmp/mlb_prod12_bundle_${ts}.tgz}"
MODEL_TAR_SOURCE="${MODEL_TAR_SOURCE:-latest}"
MANIFEST_OBJECT="${MANIFEST_OBJECT:-mlb/prod12/publish_manifest.json}"

upload_object() {
  local object_path="$1"
  curl -fsS -X POST \
    "${SUPABASE_URL}/storage/v1/object/${MODELS_BUCKET}/${object_path}" \
    -H "Authorization: Bearer ${SUPABASE_API_KEY}" \
    -H "apikey: ${SUPABASE_API_KEY}" \
    -H "x-upsert: true" \
    -H "Content-Type: application/gzip" \
    --data-binary @"${MODEL_TARBALL}" >/dev/null
}

upload_manifest() {
  local uploaded_at="$1"
  local size_bytes="$2"
  local sha256="$3"
  local payload
  payload="$(printf '{"uploaded_at":"%s","bucket":"%s","size_bytes":%s,"sha256":"%s","objects":["%s","%s"]}' \
    "${uploaded_at}" \
    "${MODELS_BUCKET}" \
    "${size_bytes}" \
    "${sha256}" \
    "${BUNDLE_OBJECT}" \
    "${BUNDLE_LATEST_OBJECT}")"
  curl -fsS -X POST \
    "${SUPABASE_URL}/storage/v1/object/${MODELS_BUCKET}/${MANIFEST_OBJECT}" \
    -H "Authorization: Bearer ${SUPABASE_API_KEY}" \
    -H "apikey: ${SUPABASE_API_KEY}" \
    -H "x-upsert: true" \
    -H "Content-Type: application/json" \
    --data-binary "${payload}" >/dev/null
}

echo "[prod12-model-publish] source MODELS_DIR=${MODELS_DIR}"
echo "[prod12-model-publish] source subdir=${MODEL_TAR_SOURCE}"
echo "[prod12-model-publish] target object=${BUNDLE_OBJECT}"
echo "[prod12-model-publish] alias object=${BUNDLE_LATEST_OBJECT}"
echo "[prod12-model-publish] tarball path=${MODEL_TARBALL}"

if [[ "${MODEL_TAR_SOURCE}" == "." ]]; then
  tar -czf "${MODEL_TARBALL}" -C "${MODELS_DIR}" .
else
  tar -czf "${MODEL_TARBALL}" -C "${MODELS_DIR}" "${MODEL_TAR_SOURCE}"
fi

size_bytes="$(wc -c < "${MODEL_TARBALL}" | tr -d ' ')"
sha256="$(sha256sum "${MODEL_TARBALL}" | awk '{print $1}')"
uploaded_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "[prod12-model-publish] tarball bytes=${size_bytes}"
echo "[prod12-model-publish] tarball sha256=${sha256}"

upload_object "${BUNDLE_OBJECT}"
echo "[prod12-model-publish] uploaded ${MODELS_BUCKET}/${BUNDLE_OBJECT}"

upload_object "${BUNDLE_LATEST_OBJECT}"
echo "[prod12-model-publish] uploaded ${MODELS_BUCKET}/${BUNDLE_LATEST_OBJECT}"

upload_manifest "${uploaded_at}" "${size_bytes}" "${sha256}"
echo "[prod12-model-publish] uploaded ${MODELS_BUCKET}/${MANIFEST_OBJECT}"

echo "[prod12-model-publish] publish complete"
