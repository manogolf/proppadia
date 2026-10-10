#!/bin/zsh
set -eu

repo_root="${0:A:h:h}"
cd "$repo_root"
exec "$repo_root/bin/python" -m backend.nhl.scripts.run_nhl_postgame_reconciliation "$@"
