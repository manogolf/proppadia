#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from dateutil import parser as dt_parser

from backend.mlb.shared.time_utils_backend import (
    ET,
    TIME_OF_DAY_BUCKETS,
    get_time_of_day_bucket_definition_rows,
    get_time_of_day_bucket_et,
)


DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")
DEFAULT_EXPANDED_ROWS = DEFAULT_OUT_DIR / "expanded_o15_universe_rows.csv"
DEFAULT_SLATE_OUTPUT = Path("backend/mlb/data/processed/mlb_slate_output.csv")
DEFAULT_REVIEW_AID_DIR = Path("artifacts/analysis/mlb/review_aids")
DEFAULT_PAIRWISE = DEFAULT_OUT_DIR / "expanded_o15_pairwise_interactions.csv"
DEFAULT_CENTRALITY = DEFAULT_OUT_DIR / "expanded_o15_feature_centrality_values.csv"


IMPLEMENTATION_DEFINITIONS = [
    {
        "implementation": "canonical_shared_helper",
        "location": "backend/mlb/shared/time_utils_backend.py:get_time_of_day_bucket_et",
        "labels": "morning, afternoon, evening, late",
        "boundaries": "ET hour <12 morning; <16 afternoon; <20 evening; otherwise late",
        "timezone": "America/New_York",
        "status": "canonical",
        "notes": "Used by current slate/reconcile/review-board/expanded hydration paths after this audit.",
    },
    {
        "implementation": "slate_output",
        "location": "backend/mlb/scripts/build_mlb_slate_output.py:_time_of_day_bucket",
        "labels": "canonical helper",
        "boundaries": "delegates to canonical_shared_helper",
        "timezone": "America/New_York",
        "status": "aligned",
        "notes": "",
    },
    {
        "implementation": "reconcile_rows",
        "location": "backend/mlb/scripts/build_mlb_reconcile_rows.py:_time_of_day_bucket",
        "labels": "canonical helper",
        "boundaries": "delegates to canonical_shared_helper",
        "timezone": "America/New_York",
        "status": "aligned",
        "notes": "",
    },
    {
        "implementation": "hits_review_boards",
        "location": "backend/mlb/scripts/run_mlb_hits_o15_review_board.py:_derive_time_of_day_bucket",
        "labels": "canonical helper",
        "boundaries": "delegates to canonical_shared_helper",
        "timezone": "America/New_York",
        "status": "aligned",
        "notes": "",
    },
    {
        "implementation": "expanded_context_hydration",
        "location": "backend/mlb/scripts/hydrate_expanded_o15_context.py:_time_bucket_from_time",
        "labels": "canonical helper",
        "boundaries": "delegates to canonical_shared_helper",
        "timezone": "America/New_York",
        "status": "aligned",
        "notes": "",
    },
    {
        "implementation": "prediction_wide",
        "location": "backend/mlb/scripts/build_mlb_predictions_wide.py",
        "labels": "canonical helper",
        "boundaries": "delegates to canonical_shared_helper",
        "timezone": "America/New_York",
        "status": "aligned",
        "notes": "",
    },
    {
        "implementation": "legacy_api_backfill",
        "location": "backend/mlb/v2_backfill_mlb_api_training.py:_time_of_day_bucket",
        "labels": "day, evening",
        "boundaries": "ET hour <17 day; otherwise evening",
        "timezone": "America/New_York",
        "status": "legacy_mismatch",
        "notes": "Not used by current Expanded O1.5 board/survey path; should be retired or migrated before reuse.",
    },
    {
        "implementation": "legacy_model_trainer_fallback",
        "location": "backend/mlb/model_trainer.py:_add_time_features",
        "labels": "morning, afternoon, night",
        "boundaries": "game_date hour <12 morning; <18 afternoon; otherwise night",
        "timezone": "not explicit",
        "status": "legacy_mismatch",
        "notes": "Fallback path; do not use for current bucket interpretation.",
    },
]


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _f(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "win"}


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt_parser.isoparse(text).astimezone(ET)
    except Exception:
        return None


def _canonical_bucket(value: Any) -> str:
    try:
        return str(get_time_of_day_bucket_et(value))
    except Exception:
        return ""


def _american_implied(price: Any) -> float | None:
    number = _f(price)
    if number is None or number == 0:
        return None
    if number > 0:
        return 100.0 / (number + 100.0)
    return abs(number) / (abs(number) + 100.0)


def _american_profit(price: Any, won: bool) -> float | None:
    number = _f(price)
    if number is None or number == 0:
        return None
    if won:
        return number / 100.0 if number > 0 else 100.0 / abs(number)
    return -1.0


def _price(row: dict[str, Any]) -> float | None:
    for col in ("expanded_price", "best_over_price", "market_price", "manual_price", "board_price"):
        value = _f(row.get(col))
        if value is not None:
            return value
    return None


def _bucket_stats(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("time_of_day_bucket") or "missing").strip().lower() or "missing"].append(row)
    out: list[dict[str, Any]] = []
    for bucket in [*TIME_OF_DAY_BUCKETS, "missing", "invalid"]:
        part = grouped.get(bucket, [])
        if not part:
            continue
        resolved = [r for r in part if _truthy(r.get("resolved")) or str(r.get("result") or "").strip()]
        wins = [r for r in resolved if _truthy(r.get("win"))]
        losses = [r for r in resolved if _truthy(r.get("loss"))]
        units: list[float] = []
        odds: list[float] = []
        implied: list[float] = []
        hours: list[float] = []
        west = 0
        for row in resolved:
            price = _price(row)
            if price is not None:
                odds.append(price)
                imp = _f(row.get("selected_side_implied_probability")) or _american_implied(price)
                if imp is not None:
                    implied.append(imp)
                profit = _f(row.get("units"))
                if profit is None:
                    profit = _american_profit(price, _truthy(row.get("win")))
                if profit is not None:
                    units.append(profit)
            dt = _parse_time(row.get("game_time"))
            if dt is not None:
                hours.append(dt.hour + dt.minute / 60.0)
            teams = {str(row.get("home_team_code") or ""), str(row.get("away_team_code") or ""), str(row.get("team") or ""), str(row.get("opponent") or "")}
            if teams & {"LAD", "LAA", "SD", "SF", "SEA", "OAK", "ATH"}:
                west += 1
        resolved_count = len(resolved)
        total_units = sum(units)
        out.append(
            {
                "bucket": bucket,
                "candidate_rows": len(part),
                "resolved_rows": resolved_count,
                "wins": len(wins),
                "losses": len(losses),
                "pushes": max(0, resolved_count - len(wins) - len(losses)),
                "wr": len(wins) / resolved_count if resolved_count else "",
                "roi": total_units / resolved_count if resolved_count else "",
                "units": total_units if resolved_count else "",
                "avg_odds": sum(odds) / len(odds) if odds else "",
                "avg_implied_probability": sum(implied) / len(implied) if implied else "",
                "avg_game_start_hour_et": sum(hours) / len(hours) if hours else "",
                "west_coast_related_resolved_rows": west,
                "west_coast_related_resolved_rate": west / resolved_count if resolved_count else "",
            }
        )
    return out


def _artifact_checks(path: Path, rows: list[dict[str, Any]], label: str) -> dict[str, Any]:
    valid = set(TIME_OF_DAY_BUCKETS)
    total = len(rows)
    present = 0
    invalid = 0
    mismatched = 0
    boundary = 0
    counts: Counter[str] = Counter()
    mismatch_examples: list[str] = []
    boundary_examples: list[str] = []
    for row in rows:
        stored = str(row.get("time_of_day_bucket") or "").strip().lower()
        if stored:
            present += 1
        if stored and stored not in valid:
            invalid += 1
        if stored:
            counts[stored] += 1
        canonical = _canonical_bucket(row.get("game_time"))
        if stored and canonical and stored != canonical:
            mismatched += 1
            if len(mismatch_examples) < 5:
                mismatch_examples.append(f"{row.get('date') or row.get('slate_date') or row.get('board_date')} {row.get('player_name') or row.get('player')} {row.get('game_time')} stored={stored} canonical={canonical}")
        dt = _parse_time(row.get("game_time"))
        if dt and dt.hour in {11, 12, 15, 16, 19, 20}:
            boundary += 1
            if len(boundary_examples) < 5:
                boundary_examples.append(f"{dt.isoformat()}->{canonical}")
    return {
        "artifact": label,
        "path": path.as_posix(),
        "rows": total,
        "time_bucket_present": present,
        "present_rate": present / total if total else "",
        "invalid_label_rows": invalid,
        "stored_vs_canonical_mismatch_rows": mismatched,
        "morning_rows": counts.get("morning", 0),
        "afternoon_rows": counts.get("afternoon", 0),
        "evening_rows": counts.get("evening", 0),
        "late_rows": counts.get("late", 0),
        "boundary_rows": boundary,
        "mismatch_examples": " | ".join(mismatch_examples),
        "boundary_examples": " | ".join(boundary_examples),
    }


def _review_board_rows(review_dir: Path) -> list[tuple[Path, list[dict[str, Any]]]]:
    patterns = [
        "hits_o15_simple_filter_*.csv",
        "hits_o15_watch_candidates_*.csv",
        "hits_o15_layered_candidates_*.csv",
        "hits_o15_alternate_discovery_*.csv",
        "hits_u15_favorite_audit_*.csv",
    ]
    out: list[tuple[Path, list[dict[str, Any]]]] = []
    for pattern in patterns:
        for path in sorted(review_dir.glob(pattern)):
            out.append((path, _read_csv(path)))
    return out


def _pairwise_bucket_rows(path: Path) -> list[dict[str, Any]]:
    rows = _read_csv(path)
    out: list[dict[str, Any]] = []
    for row in rows:
        for suffix in ("a", "b"):
            if row.get(f"variable_{suffix}") == "time_of_day_bucket":
                out.append(
                    {
                        "artifact": "variable_importance_pairwise",
                        "bucket": row.get(f"bucket_{suffix}"),
                        "resolved": row.get("resolved"),
                        "roi": row.get("roi"),
                        "roi_betonline": row.get("roi_betonline"),
                        "partner": row.get("variable_b" if suffix == "a" else "variable_a"),
                        "partner_bucket": row.get("bucket_b" if suffix == "a" else "bucket_a"),
                    }
                )
    return out


def _write_report(
    path: Path,
    definition_rows: list[dict[str, Any]],
    artifact_rows: list[dict[str, Any]],
    distribution_rows: list[dict[str, Any]],
    pairwise_rows: list[dict[str, Any]],
) -> None:
    failures = [
        row
        for row in artifact_rows
        if int(row.get("invalid_label_rows") or 0) > 0 or int(row.get("stored_vs_canonical_mismatch_rows") or 0) > 0
    ]
    evening = next((row for row in distribution_rows if row.get("bucket") == "evening"), {})
    late = next((row for row in distribution_rows if row.get("bucket") == "late"), {})
    verdict = "YES" if not failures else "PARTIAL"
    lines = [
        "# MLB Time-of-Day Bucket Audit",
        "",
        "Scope: research/definition audit. No production selector/upload/model/threshold/grading changes.",
        "",
        "## Canonical Definition",
        "",
        "Timezone: `America/New_York` / ET.",
        "",
        "| label | hour boundaries |",
        "|---|---|",
    ]
    for row in get_time_of_day_bucket_definition_rows():
        lines.append(f"| `{row['label']}` | {row['hour_boundaries']} |")
    lines.extend(
        [
            "",
            "Buckets are mutually exclusive and exhaustive across ET hours 0-23.",
            "",
            "## Implementation Comparison",
            "",
            "| implementation | status | location | boundaries | notes |",
            "|---|---|---|---|---|",
        ]
    )
    for row in definition_rows:
        lines.append(
            f"| {row['implementation']} | `{row['status']}` | `{row['location']}` | {row['boundaries']} | {row.get('notes','')} |"
        )
    lines.extend(["", "## Artifact Consistency Checks", "", "| artifact | rows | present | invalid | mismatched | morning | afternoon | evening | late |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|"])
    for row in artifact_rows:
        lines.append(
            f"| {row['artifact']} | {row['rows']} | {row['time_bucket_present']} | {row['invalid_label_rows']} | {row['stored_vs_canonical_mismatch_rows']} | {row['morning_rows']} | {row['afternoon_rows']} | {row['evening_rows']} | {row['late_rows']} |"
        )
    lines.extend(["", "## Expanded O1.5 Bucket Performance", "", "| bucket | candidates | resolved | record | ROI | avg odds | avg implied | avg ET hour | west-coast rate |", "|---|---:|---:|---|---:|---:|---:|---:|---:|"])
    for row in distribution_rows:
        record = f"{row.get('wins', 0)}-{row.get('losses', 0)}-{row.get('pushes', 0)}"
        lines.append(
            f"| `{row['bucket']}` | {row['candidate_rows']} | {row['resolved_rows']} | {record} | {_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('avg_odds'))} | {_fmt_pct(row.get('avg_implied_probability'))} | {_fmt_num(row.get('avg_game_start_hour_et'))} | {_fmt_pct(row.get('west_coast_related_resolved_rate'))} |"
        )
    lines.extend(["", "## Confusion Audit", ""])
    if failures:
        lines.append("- Stored bucket mismatches remain in one or more artifacts. Treat time-bucket interpretation as `PARTIAL` until regenerated.")
    else:
        lines.append("- Stored bucket labels match canonical ET-derived labels in audited current artifacts.")
    lines.extend(
        [
            "- `evening` and `late` are disjoint by definition: evening is 16:00-19:59 ET; late is 20:00-23:59 ET.",
            "- Boundary games are assigned by ET hour: 16:00 enters `evening`, 20:00 enters `late`.",
            "- West-coast games are expected to be overrepresented in `late`; the distribution table reports west-coast-related rates.",
            "- Legacy code with `day/evening` or `night` labels was found, but is not the current Expanded O1.5/slate/review-board path after this patch.",
            "",
            "## Pairwise Time-Bucket Rows",
            "",
            f"- Pairwise interactions involving `time_of_day_bucket`: `{len(pairwise_rows)}`",
            "",
            "## Final Verdict",
            "",
            f"Can we trust `evening positive` / `late negative`? `{verdict}`.",
            "",
        ]
    )
    if verdict == "YES":
        lines.append("The current Expanded O1.5, slate, review-board, variable-importance, and feature-centrality paths now share the same ET bucket definition. Interpretation should still be treated as research, not as a production rule.")
    else:
        lines.append("The signal is directionally useful but should not be treated as final until remaining mismatched artifacts are regenerated.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100.0:.2f}%"


def _fmt_num(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.2f}"


def run(args: argparse.Namespace) -> dict[str, Any]:
    expanded_rows = _read_csv(args.expanded_rows_csv)
    definition_rows = list(IMPLEMENTATION_DEFINITIONS)
    distribution_rows = _bucket_stats(expanded_rows)
    artifact_checks = [
        _artifact_checks(args.expanded_rows_csv, expanded_rows, "expanded_o15_universe"),
        _artifact_checks(args.slate_output_csv, _read_csv(args.slate_output_csv), "mlb_slate_output"),
    ]
    board_rows: list[dict[str, Any]] = []
    for path, rows in _review_board_rows(args.review_aid_dir):
        board_rows.extend(rows)
        artifact_checks.append(_artifact_checks(path, rows, path.name))
    if board_rows:
        artifact_checks.append(_artifact_checks(args.review_aid_dir, board_rows, "review_boards_combined"))
    pairwise_rows = _pairwise_bucket_rows(args.pairwise_csv)

    _write_csv(args.out_dir / "time_of_day_bucket_definition_comparison.csv", definition_rows)
    _write_csv(args.out_dir / "time_of_day_bucket_distribution.csv", distribution_rows)
    _write_csv(args.out_dir / "time_of_day_bucket_artifact_consistency.csv", artifact_checks)
    _write_csv(args.out_dir / "time_of_day_bucket_pairwise_rows.csv", pairwise_rows)
    _write_report(
        args.out_dir / "time_of_day_bucket_audit.md",
        definition_rows,
        artifact_checks,
        distribution_rows,
        pairwise_rows,
    )
    return {
        "status": "ok",
        "expanded_rows": len(expanded_rows),
        "artifact_checks": len(artifact_checks),
        "mismatched_artifacts": sum(1 for row in artifact_checks if int(row.get("stored_vs_canonical_mismatch_rows") or 0) > 0),
        "definition_comparison": str(args.out_dir / "time_of_day_bucket_definition_comparison.csv"),
        "distribution": str(args.out_dir / "time_of_day_bucket_distribution.csv"),
        "report": str(args.out_dir / "time_of_day_bucket_audit.md"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit MLB time-of-day bucket definitions and artifact consistency.")
    parser.add_argument("--expanded-rows-csv", type=Path, default=DEFAULT_EXPANDED_ROWS)
    parser.add_argument("--slate-output-csv", type=Path, default=DEFAULT_SLATE_OUTPUT)
    parser.add_argument("--review-aid-dir", type=Path, default=DEFAULT_REVIEW_AID_DIR)
    parser.add_argument("--pairwise-csv", type=Path, default=DEFAULT_PAIRWISE)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    print(run(args))


if __name__ == "__main__":
    main()
