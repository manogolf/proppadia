#!/usr/bin/env bash
set -euo pipefail

# Convenience wrapper for schedulers: always triggers the lightweight daily mode.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/mlb_prod12_remote_trigger.sh" '{"run_mode":"daily"}'
