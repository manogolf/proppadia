"""Guard MLB reconciliation on finalized upstream training data."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from typing import Any, Dict

from backend.shared.db.pg import pg_fetchone
from backend.mlb.scripts.player_stats_game_completeness import inspect_date


def _parse_date(value: str) -> str:
    try:
        return date.fromisoformat(str(value).strip()).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD date, got {value!r}") from exc


def _counts(game_date: str) -> Dict[str, Any]:
    return pg_fetchone(
        """
        SELECT
          (
            SELECT COUNT(*)::int
            FROM mlb.model_training_props
            WHERE game_date::date = %s::date
          ) AS model_training_props_rows,
          (
            SELECT COUNT(*)::int
            FROM mlb.player_stats
            WHERE game_date::date = %s::date
          ) AS player_stats_rows
        """,
        (game_date, game_date),
    ) or {}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", required=True, type=_parse_date, help="Finalized slate date to check.")
    ap.add_argument(
        "--check-player-stats",
        action="store_true",
        help="Also require mlb.player_stats rows for the date.",
    )
    args = ap.parse_args()

    try:
        counts = _counts(args.date)
    except Exception as exc:
        print(
            f"Reconcile skipped: could not verify finalized training data for {args.date}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2

    mtp_rows = int(counts.get("model_training_props_rows") or 0)
    player_stats_rows = int(counts.get("player_stats_rows") or 0)
    print(
        "[mlb-finalized-data-check] "
        f"date={args.date} "
        f"model_training_props_rows={mtp_rows} "
        f"player_stats_rows={player_stats_rows}"
    )

    if mtp_rows <= 0:
        print(f"Reconcile skipped: no finalized training data for {args.date}", file=sys.stderr)
        return 2

    if args.check_player_stats and player_stats_rows <= 0:
        print(f"Reconcile skipped: no finalized player_stats data for {args.date}", file=sys.stderr)
        return 2

    if args.check_player_stats:
        try:
            completeness = inspect_date(
                args.date,
                __import__("pathlib").Path("artifacts/analysis/mlb/player_stats_completeness") / args.date,
            )
        except Exception as exc:
            print(f"COMPLETED_GAME_PLAYER_STATS_INCOMPLETE date={args.date} audit_error={type(exc).__name__}: {exc}", file=sys.stderr)
            return 2
        incomplete = [r for r in completeness if r.get("classification") != "COMPLETE_EXACT"]
        if incomplete:
            print(
                "COMPLETED_GAME_PLAYER_STATS_INCOMPLETE "
                f"date={args.date} games="
                + ",".join(f"{r.get('game_pk')}:{r.get('classification')}" for r in incomplete),
                file=sys.stderr,
            )
            return 2

    print(f"[mlb-finalized-data-check] finalized upstream data present for {args.date}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
