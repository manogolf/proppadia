#!/usr/bin/env python3
"""Build deterministic, offline review evidence for the 2026 close inventory."""

from __future__ import annotations

import json

from backend.mlb.season_transition.regular_season_close_inventory_v1 import (
    DEFAULT_PACKAGE_PATH,
    build_authoritative_inventory,
    write_inventory_core,
)


def main() -> int:
    rows, summary = build_authoritative_inventory()
    manifest = write_inventory_core(rows, summary)
    print(
        json.dumps(
            {
                "package_path": str(DEFAULT_PACKAGE_PATH),
                "inventory_rows": len(rows),
                "disposition_counts": summary["disposition_counts"],
                "scheduled_not_final_count": len(
                    summary["scheduled_not_final_game_pks"]
                ),
                "unresolved_count": len(summary["unresolved_game_pks"]),
                "manifest_sha256": manifest["manifest_sha256"],
                "close_executed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
