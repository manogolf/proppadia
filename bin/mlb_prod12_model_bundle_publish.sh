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

MODELS_DIR="${MODELS_DIR:-/var/data/proppadia/models}"
MODELS_BUCKET="${MODELS_BUCKET:-models}"

if [[ ! -d "${MODELS_DIR}/latest" ]]; then
  echo "mlb_prod12_model_bundle_publish: missing ${MODELS_DIR}/latest" >&2
  exit 2
fi

# Keep package step compatible with uploader assumptions.
mkdir -p "${MODELS_DIR}/archive"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
BUNDLE_OBJECT="${BUNDLE_OBJECT:-mlb/prod12/mlb_latest_${ts}.tgz}"
BUNDLE_LATEST_OBJECT="${BUNDLE_LATEST_OBJECT:-mlb/prod12/latest.tgz}"

if [[ -x ".venv/bin/python3" ]]; then
  PY=".venv/bin/python3"
elif [[ -x ".venv/bin/python" ]]; then
  PY=".venv/bin/python"
else
  PY="python3"
fi

echo "[prod12-model-publish] source MODELS_DIR=${MODELS_DIR}"
echo "[prod12-model-publish] target object=${BUNDLE_OBJECT}"
echo "[prod12-model-publish] alias object=${BUNDLE_LATEST_OBJECT}"

MODELS_DIR="${MODELS_DIR}" \
MODELS_BUCKET="${MODELS_BUCKET}" \
BUNDLE_OBJECT="${BUNDLE_OBJECT}" \
BUNDLE_LATEST_OBJECT="${BUNDLE_LATEST_OBJECT}" \
BUNDLE_AUTO_PROD12_LATEST_ALIAS="0" \
"${PY}" backend/mlb/modeling/package_and_upload.py

echo "[prod12-model-publish] publish complete"
