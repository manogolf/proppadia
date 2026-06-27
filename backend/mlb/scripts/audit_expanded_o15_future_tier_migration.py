#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from backend.mlb.scripts import analyze_expanded_o15_universe_slices as slices
from backend.mlb.scripts import audit_expanded_o15_betonline as bol_audit


DEFAULT_ROWS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
DEFAULT_BACKFILL_ROOT = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")
HORIZONS = (3, 5, 7, 10)
TIER_RANK = {"C": 1, "B": 2, "A": 3}


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
    return slices._f(value)


def _b(value: Any) -> bool:
    return slices._b(value)


def _norm_name(value: Any) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^0-9A-Za-z ]+", " ", text).lower()
    return re.sub(r"\s+", " ", text).strip()


def _date_text(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("board_date") or "")[:10]


def _date(row: dict[str, Any]) -> datetime | None:
    try:
        return datetime.strptime(_date_text(row), "%Y-%m-%d")
    except Exception:
        return None


def _player_key(row: dict[str, Any]) -> str:
    player_id = _f(row.get("player_id"))
    if player_id is not None:
        return f"id:{int(player_id)}"
    return f"name:{_norm_name(row.get('player_name') or row.get('player'))}"


def _tier(row: dict[str, Any]) -> str:
    tier = str(row.get("hitter_tier") or "C").strip().upper()
    return tier if tier in TIER_RANK else "C"


def _max_tier(rows: Iterable[dict[str, Any]]) -> str:
    best = ""
    best_rank = 0
    for row in rows:
        tier = _tier(row)
        rank = TIER_RANK.get(tier, 0)
        if rank > best_rank:
            best = tier
            best_rank = rank
    return best


def _avg(values: Iterable[Any]) -> float | None:
    nums = [_f(value) for value in values]
    nums = [value for value in nums if value is not None]
    return sum(nums) / len(nums) if nums else None


def _units(row: dict[str, Any], price_col: str = "expanded_price") -> float:
    return slices._american_units(_f(row.get(price_col)), _b(row.get("win")), _b(row.get("loss")), _b(row.get("push")))


def _metrics(rows: list[dict[str, Any]], price_col: str = "expanded_price") -> dict[str, Any]:
    resolved = [row for row in rows if _b(row.get("resolved"))]
    wins = sum(1 for row in resolved if _b(row.get("win")))
    losses = sum(1 for row in resolved if _b(row.get("loss")))
    pushes = sum(1 for row in resolved if _b(row.get("push")))
    units = sum(_units(row, price_col) for row in resolved)
    return {
        "rows": len(rows),
        "resolved": len(resolved),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / len(resolved) if resolved else None,
        "units": units,
        "avg_price": _avg(row.get(price_col) for row in rows),
        "avg_d7_hits_rate": _avg(row.get("d7_hits_rate") for row in rows),
        "avg_d15_hits_rate": _avg(row.get("d15_hits_rate") for row in rows),
        "avg_d7_hrr": _avg(row.get("d7_hits_runs_rbis") for row in rows),
        "avg_d15_hrr": _avg(row.get("d15_hits_runs_rbis") for row in rows),
        "avg_starter_expected_hits_allowed": _avg(row.get("starter_expected_hits_allowed") for row in rows),
        "avg_team_expected_hits_allowed": _avg(row.get("team_expected_hits_allowed") for row in rows),
    }


def _future_rows(row: dict[str, Any], by_player: dict[str, list[dict[str, Any]]], horizon: int) -> list[dict[str, Any]]:
    start = _date(row)
    if start is None:
        return []
    end = start + timedelta(days=horizon)
    out: list[dict[str, Any]] = []
    for item in by_player.get(_player_key(row), []):
        d = _date(item)
        if d is not None and start < d <= end:
            out.append(item)
    return out


def _latest_future(row: dict[str, Any], by_player: dict[str, list[dict[str, Any]]], horizon: int) -> dict[str, Any] | None:
    future = _future_rows(row, by_player, horizon)
    return future[-1] if future else None


def _days_until_tier(row: dict[str, Any], by_player: dict[str, list[dict[str, Any]]], target: str = "A", max_days: int = 10) -> int | None:
    start = _date(row)
    if start is None:
        return None
    for item in by_player.get(_player_key(row), []):
        d = _date(item)
        if d is None or d <= start or d > start + timedelta(days=max_days):
            continue
        if _tier(item) == target:
            return (d - start).days
    return None


def _migration_cohort(row: dict[str, Any], future10: list[dict[str, Any]]) -> str:
    current = _tier(row)
    if not future10:
        return "no_future_observation"
    max_future = _max_tier(future10)
    if current == "C" and TIER_RANK.get(max_future, 0) >= TIER_RANK["B"]:
        return "current_C_to_future_B_or_A"
    if current == "B" and max_future == "A":
        return "current_B_to_future_A"
    if current == "A" and max_future == "A":
        return "current_A_stays_A"
    if current == "A" and max_future in {"B", "C"}:
        return "current_A_falls"
    return "no_upward_migration"


def _price_bucket(row: dict[str, Any]) -> str:
    return slices._price_bucket(row.get("betonline_over_price") or row.get("best_available_over_price") or row.get("expanded_price"))


def _row_record(row: dict[str, Any], by_player: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    future10 = _future_rows(row, by_player, 10)
    out: dict[str, Any] = {
        "date": _date_text(row),
        "player_id": row.get("player_id"),
        "player_name": row.get("player_name") or row.get("player"),
        "team": row.get("team"),
        "opponent": row.get("opponent"),
        "line": row.get("line"),
        "side": row.get("side"),
        "source_bucket": row.get("source_bucket"),
        "hitter_tier": _tier(row),
        "pitcher_tier": row.get("pitcher_tier"),
        "combined_tier": row.get("combined_tier"),
        "alternate_layer": row.get("alternate_layer"),
        "d7_hits_rate": row.get("d7_hits_rate"),
        "d15_hits_rate": row.get("d15_hits_rate"),
        "d7_hits_runs_rbis": row.get("d7_hits_runs_rbis"),
        "d15_hits_runs_rbis": row.get("d15_hits_runs_rbis"),
        "starter_expected_hits_allowed": row.get("starter_expected_hits_allowed"),
        "team_expected_hits_allowed": row.get("team_expected_hits_allowed"),
        "market_price": row.get("market_price") or row.get("expanded_price"),
        "best_price": row.get("best_available_over_price"),
        "betonline_price": row.get("betonline_over_price"),
        "median_price": row.get("median_available_over_price"),
        "price_bucket": _price_bucket(row),
        "resolved": row.get("resolved"),
        "outcome": "win" if _b(row.get("win")) else "loss" if _b(row.get("loss")) else "push" if _b(row.get("push")) else "",
        "actual_hits": row.get("actual_value"),
        "migration_cohort_10d": _migration_cohort(row, future10),
        "days_until_tier_a_10d": _days_until_tier(row, by_player, "A", 10),
    }
    for horizon in HORIZONS:
        future = _future_rows(row, by_player, horizon)
        latest = future[-1] if future else None
        max_tier = _max_tier(future)
        future_resolved = [item for item in future if _b(item.get("resolved"))]
        out.update(
            {
                f"future_{horizon}d_observations": len(future),
                f"future_{horizon}d_max_hitter_tier": max_tier,
                f"future_{horizon}d_becomes_tier_b": str(TIER_RANK.get(max_tier, 0) >= TIER_RANK["B"]).lower(),
                f"future_{horizon}d_becomes_tier_a": str(max_tier == "A").lower(),
                f"future_{horizon}d_latest_date": _date_text(latest or {}),
                f"future_{horizon}d_latest_d7_hits_rate": (latest or {}).get("d7_hits_rate"),
                f"future_{horizon}d_latest_d15_hits_rate": (latest or {}).get("d15_hits_rate"),
                f"future_{horizon}d_latest_d7_hrr": (latest or {}).get("d7_hits_runs_rbis"),
                f"future_{horizon}d_latest_d15_hrr": (latest or {}).get("d15_hits_runs_rbis"),
                f"future_{horizon}d_latest_hitter_tier": _tier(latest or {}) if latest else "",
                f"future_{horizon}d_resolved_rows": len(future_resolved),
                f"future_{horizon}d_wins": sum(1 for item in future_resolved if _b(item.get("win"))),
                f"future_{horizon}d_losses": sum(1 for item in future_resolved if _b(item.get("loss"))),
                f"future_{horizon}d_units": sum(_units(item) for item in future_resolved),
            }
        )
    return out


def _summary_group(label: str, value: str, rows: list[dict[str, Any]], by_player: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    base = {"group_type": label, "group_value": value}
    base.update(_metrics(rows))
    for horizon in HORIZONS:
        observed = [row for row in rows if _future_rows(row, by_player, horizon)]
        future_all = [item for row in rows for item in _future_rows(row, by_player, horizon)]
        resolved_future = [item for item in future_all if _b(item.get("resolved"))]
        base[f"future_{horizon}d_observed_rows"] = len(observed)
        base[f"future_{horizon}d_observation_rate"] = len(observed) / len(rows) if rows else None
        base[f"future_{horizon}d_tier_a_rate"] = (
            sum(1 for row in rows if _max_tier(_future_rows(row, by_player, horizon)) == "A") / len(rows) if rows else None
        )
        base[f"future_{horizon}d_tier_b_or_a_rate"] = (
            sum(1 for row in rows if TIER_RANK.get(_max_tier(_future_rows(row, by_player, horizon)), 0) >= TIER_RANK["B"]) / len(rows)
            if rows
            else None
        )
        base[f"future_{horizon}d_resolved"] = len(resolved_future)
        base[f"future_{horizon}d_wins"] = sum(1 for item in resolved_future if _b(item.get("win")))
        base[f"future_{horizon}d_losses"] = sum(1 for item in resolved_future if _b(item.get("loss")))
        base[f"future_{horizon}d_units"] = sum(_units(item) for item in resolved_future)
        base[f"future_{horizon}d_roi"] = (
            base[f"future_{horizon}d_units"] / len(resolved_future) if resolved_future else None
        )
    return base


def _build_summary(rows: list[dict[str, Any]], by_player: dict[str, list[dict[str, Any]]], row_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[tuple[str, str, list[dict[str, Any]]]] = [
        ("population", "alternate_only_all", rows),
        ("population", "alternate_only_non_tier_a", [row for row in rows if _tier(row) != "A"]),
        (
            "population",
            "alternate_only_non_tier_a_current_win",
            [row for row in rows if _tier(row) != "A" and _b(row.get("resolved")) and _b(row.get("win"))],
        ),
        (
            "population",
            "alternate_only_non_tier_a_current_loss",
            [row for row in rows if _tier(row) != "A" and _b(row.get("resolved")) and _b(row.get("loss"))],
        ),
        ("population", "alternate_only_tier_a", [row for row in rows if _tier(row) == "A"]),
        ("population", "combined_tier_C_A", [row for row in rows if str(row.get("combined_tier") or "") == "C/A"]),
        ("population", "combined_tier_B_A", [row for row in rows if str(row.get("combined_tier") or "") == "B/A"]),
        ("population", "d7_hits_rate_lte_1_0", [row for row in rows if (_f(row.get("d7_hits_rate")) is not None and (_f(row.get("d7_hits_rate")) or 0) <= 1.0)]),
        ("population", "price_201_300", [row for row in rows if _price_bucket(row) == "201-300"]),
        ("population", "non_tier_a_price_201_300", [row for row in rows if _tier(row) != "A" and _price_bucket(row) == "201-300"]),
        ("population", "c_a_price_201_300", [row for row in rows if str(row.get("combined_tier") or "") == "C/A" and _price_bucket(row) == "201-300"]),
        ("population", "betonline_available", [row for row in rows if _f(row.get("betonline_over_price")) is not None]),
    ]
    for tier in ("A", "B", "C"):
        groups.append(("current_hitter_tier", tier, [row for row in rows if _tier(row) == tier]))
    for cohort in sorted({row.get("migration_cohort_10d") or "" for row in row_records}):
        keys = {
            (str(rec.get("date")), str(rec.get("player_id") or rec.get("player_name")), str(rec.get("line")), str(rec.get("side")))
            for rec in row_records
            if rec.get("migration_cohort_10d") == cohort
        }
        groups.append(
            (
                "migration_cohort_10d",
                cohort,
                [
                    row
                    for row in rows
                    if (_date_text(row), str(row.get("player_id") or row.get("player_name")), str(row.get("line")), str(row.get("side"))) in keys
                ],
            )
        )
    return [_summary_group(group_type, value, group_rows, by_player) for group_type, value, group_rows in groups]


def _trajectory_rows(rows: list[dict[str, Any]], by_player: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    specs = [
        ("population", "alternate_only_all", rows),
        ("population", "alternate_only_non_tier_a", [row for row in rows if _tier(row) != "A"]),
        ("population", "combined_tier_C_A", [row for row in rows if str(row.get("combined_tier") or "") == "C/A"]),
        ("population", "price_201_300", [row for row in rows if _price_bucket(row) == "201-300"]),
    ]
    out: list[dict[str, Any]] = []
    for group_type, group_value, group_rows in specs:
        for horizon in (0, *HORIZONS):
            observed: list[dict[str, Any]] = []
            for row in group_rows:
                if horizon == 0:
                    observed.append(row)
                else:
                    latest = _latest_future(row, by_player, horizon)
                    if latest:
                        observed.append(latest)
            out.append(
                {
                    "group_type": group_type,
                    "group_value": group_value,
                    "horizon_days": horizon,
                    "base_rows": len(group_rows),
                    "observed_rows": len(observed),
                    "avg_d7_hits_rate": _avg(row.get("d7_hits_rate") for row in observed),
                    "avg_d15_hits_rate": _avg(row.get("d15_hits_rate") for row in observed),
                    "avg_d7_hits_runs_rbis": _avg(row.get("d7_hits_runs_rbis") for row in observed),
                    "avg_d15_hits_runs_rbis": _avg(row.get("d15_hits_runs_rbis") for row in observed),
                    "tier_a_rate": sum(1 for row in observed if _tier(row) == "A") / len(observed) if observed else None,
                    "tier_b_or_a_rate": sum(1 for row in observed if TIER_RANK.get(_tier(row), 0) >= 2) / len(observed) if observed else None,
                }
            )
    return out


def _price_bucket_rows(rows: list[dict[str, Any]], by_player: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    buckets = sorted({_price_bucket(row) for row in rows})
    for bucket in buckets:
        group = [row for row in rows if _price_bucket(row) == bucket]
        item = _summary_group("price_bucket", bucket, group, by_player)
        out.append(item)
    return out


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100:.2f}%"


def _fmt_num(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.2f}"


def _write_report(path: Path, summary: list[dict[str, Any]], trajectories: list[dict[str, Any]], price_rows: list[dict[str, Any]]) -> None:
    by_key = {(row.get("group_type"), row.get("group_value")): row for row in summary}
    all_row = by_key.get(("population", "alternate_only_all"), {})
    non_a = by_key.get(("population", "alternate_only_non_tier_a"), {})
    non_a_win = by_key.get(("population", "alternate_only_non_tier_a_current_win"), {})
    non_a_loss = by_key.get(("population", "alternate_only_non_tier_a_current_loss"), {})
    tier_a = by_key.get(("population", "alternate_only_tier_a"), {})
    c_a = by_key.get(("population", "combined_tier_C_A"), {})
    price = by_key.get(("population", "price_201_300"), {})
    non_a_price = by_key.get(("population", "non_tier_a_price_201_300"), {})
    lines = [
        "# Expanded O1.5 Future Tier Migration Audit",
        "",
        "Scope: alternate-only Expanded O1.5 candidate appearances. Future observations are later rows for the same player inside the Expanded O1.5 universe, not a complete player-day panel.",
        "",
        "## Headline",
        "",
        f"- Alternate-only candidates: `{all_row.get('rows', 0)}`; current-day ROI `{_fmt_pct(all_row.get('roi'))}`.",
        f"- Non-Tier-A alternate-only candidates: `{non_a.get('rows', 0)}`; current-day ROI `{_fmt_pct(non_a.get('roi'))}`; +10d Tier A migration rate `{_fmt_pct(non_a.get('future_10d_tier_a_rate'))}`.",
        f"- Current Tier A alternate-only candidates: `{tier_a.get('rows', 0)}`; current-day ROI `{_fmt_pct(tier_a.get('roi'))}`; +10d Tier A rate `{_fmt_pct(tier_a.get('future_10d_tier_a_rate'))}`.",
        f"- C/A candidates: `{c_a.get('rows', 0)}`; current-day ROI `{_fmt_pct(c_a.get('roi'))}`; +10d Tier A migration rate `{_fmt_pct(c_a.get('future_10d_tier_a_rate'))}`.",
        f"- Price 201-300 candidates: `{price.get('rows', 0)}`; current-day ROI `{_fmt_pct(price.get('roi'))}`; +10d Tier A migration rate `{_fmt_pct(price.get('future_10d_tier_a_rate'))}`.",
        f"- Non-Tier-A winners vs losers future Tier A rates: winners `{_fmt_pct(non_a_win.get('future_10d_tier_a_rate'))}`, losers `{_fmt_pct(non_a_loss.get('future_10d_tier_a_rate'))}`.",
        f"- Non-Tier-A price 201-300 candidates: `{non_a_price.get('rows', 0)}`; current-day ROI `{_fmt_pct(non_a_price.get('roi'))}`; +10d Tier A migration rate `{_fmt_pct(non_a_price.get('future_10d_tier_a_rate'))}`.",
        "",
        "## Key Slice Checks",
        "",
        "| slice | rows | resolved | W-L-P | ROI | +10d Tier A rate | +10d B/A rate |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for key in (
        ("population", "alternate_only_non_tier_a"),
        ("population", "alternate_only_non_tier_a_current_win"),
        ("population", "alternate_only_non_tier_a_current_loss"),
        ("population", "combined_tier_C_A"),
        ("population", "combined_tier_B_A"),
        ("population", "price_201_300"),
        ("population", "non_tier_a_price_201_300"),
        ("population", "c_a_price_201_300"),
        ("population", "d7_hits_rate_lte_1_0"),
    ):
        row = by_key.get(key, {})
        lines.append(
            f"| {key[1]} | {row.get('rows', 0)} | {row.get('resolved', 0)} | "
            f"{row.get('wins', 0)}-{row.get('losses', 0)}-{row.get('pushes', 0)} | {_fmt_pct(row.get('roi'))} | "
            f"{_fmt_pct(row.get('future_10d_tier_a_rate'))} | {_fmt_pct(row.get('future_10d_tier_b_or_a_rate'))} |"
        )
    lines.extend(
        [
            "",
            "## Migration Cohorts",
            "",
            "| cohort | rows | resolved | W-L-P | ROI | +10d obs rate | +10d Tier A rate | +10d B/A rate |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary:
        if row.get("group_type") != "migration_cohort_10d":
            continue
        lines.append(
            f"| {row.get('group_value')} | {row.get('rows')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | "
            f"{_fmt_pct(row.get('future_10d_observation_rate'))} | {_fmt_pct(row.get('future_10d_tier_a_rate'))} | {_fmt_pct(row.get('future_10d_tier_b_or_a_rate'))} |"
        )
    lines.extend(
        [
            "",
            "## Price Buckets",
            "",
            "| bucket | rows | resolved | W-L-P | ROI | +10d Tier A rate | +10d B/A rate |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in price_rows:
        lines.append(
            f"| {row.get('group_value')} | {row.get('rows')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | "
            f"{_fmt_pct(row.get('future_10d_tier_a_rate'))} | {_fmt_pct(row.get('future_10d_tier_b_or_a_rate'))} |"
        )
    lines.extend(
        [
            "",
            "## Trajectory Notes",
            "",
            "See `expanded_o15_future_tier_migration_trajectories.csv` for d7/d15/HRR trajectory averages from day 0 through +10 days.",
            "",
            "## Interpretation",
            "",
            "- This audit tests whether alternate discovery identifies soon-to-be-hot players by checking future tier migration after the first candidate appearance.",
            "- If positive ROI is concentrated in rows that do not migrate upward, the signal is more likely matchup/price/universe selection than a pure heat-preview signal.",
            "- If C/A or price 201-300 rows migrate upward at elevated rates, those slices deserve further review as early-form candidates.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit future hitter-tier migration in the Expanded O1.5 Universe.")
    ap.add_argument("--rows-csv", default=str(DEFAULT_ROWS_CSV))
    ap.add_argument("--backfill-root", default=str(DEFAULT_BACKFILL_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    rows = _read_csv(Path(args.rows_csv))
    bol_audit._enrich(rows, Path(args.backfill_root))
    dated = [row for row in rows if _date(row) is not None]
    by_player: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(dated, key=lambda item: (_player_key(item), _date_text(item))):
        by_player[_player_key(row)].append(row)

    alternate_only = [row for row in dated if _b(row.get("from_alternate")) and not _b(row.get("from_both"))]
    row_records = [_row_record(row, by_player) for row in alternate_only]
    summary = _build_summary(alternate_only, by_player, row_records)
    trajectories = _trajectory_rows(alternate_only, by_player)
    price_rows = _price_bucket_rows(alternate_only, by_player)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "expanded_o15_future_tier_migration_rows.csv", row_records)
    _write_csv(out_dir / "expanded_o15_future_tier_migration_summary.csv", summary)
    _write_csv(out_dir / "expanded_o15_future_tier_migration_trajectories.csv", trajectories)
    _write_csv(out_dir / "expanded_o15_future_tier_migration_price_buckets.csv", price_rows)
    _write_report(out_dir / "expanded_o15_future_tier_migration_audit.md", summary, trajectories, price_rows)
    print(
        {
            "alternate_only_rows": len(alternate_only),
            "row_output": str(out_dir / "expanded_o15_future_tier_migration_rows.csv"),
            "report": str(out_dir / "expanded_o15_future_tier_migration_audit.md"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
