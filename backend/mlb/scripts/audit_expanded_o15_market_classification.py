#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import analyze_expanded_o15_universe_slices as slices
from backend.mlb.scripts import audit_expanded_o15_betonline as bol_audit


DEFAULT_ROWS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
DEFAULT_BACKFILL_ROOT = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")


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


def _avg(values: list[Any]) -> float | None:
    nums = [_f(value) for value in values]
    nums = [value for value in nums if value is not None]
    return sum(nums) / len(nums) if nums else None


def _price(row: dict[str, Any]) -> float | None:
    return _f(row.get("best_available_over_price") or row.get("expanded_price") or row.get("market_price"))


def _price_bucket(value: Any) -> str:
    price = _f(value)
    if price is None:
        return "missing"
    if price <= 150:
        return "+100_to_+150"
    if price <= 200:
        return "+151_to_+200"
    if price <= 250:
        return "+201_to_+250"
    if price <= 300:
        return "+251_to_+300"
    if price <= 400:
        return "+301_to_+400"
    return "+401_plus"


def _rate_bucket(value: Any) -> str:
    return slices._rate_bucket(value)


def _hrr_bucket(value: Any) -> str:
    return slices._hrr_bucket(value)


def _starter_bucket(value: Any) -> str:
    return slices._expected_bucket(value)


def _team_bucket(value: Any) -> str:
    return slices._team_expected_bucket(value)


def _book_count(row: dict[str, Any]) -> int:
    source = _f(row.get("book_count"))
    if source is not None:
        return int(source)
    return len([x for x in str(row.get("bookmaker_list") or "").split(",") if x.strip()])


def _units(row: dict[str, Any], price_col: str = "best_available_over_price") -> float | None:
    price = _f(row.get(price_col))
    if price is None:
        return None
    return slices._american_units(price, _b(row.get("win")), _b(row.get("loss")), _b(row.get("push")))


def _metrics(rows: list[dict[str, Any]], price_col: str = "best_available_over_price") -> dict[str, Any]:
    resolved = [row for row in rows if _b(row.get("resolved"))]
    priced = [row for row in resolved if _f(row.get(price_col)) is not None]
    wins = sum(1 for row in resolved if _b(row.get("win")))
    losses = sum(1 for row in resolved if _b(row.get("loss")))
    pushes = sum(1 for row in resolved if _b(row.get("push")))
    units = sum((_units(row, price_col) or 0.0) for row in priced)
    return {
        "rows": len(rows),
        "resolved": len(resolved),
        "priced_resolved": len(priced),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / len(priced) if priced else None,
        "units": units if priced else None,
        "avg_price": _avg([row.get(price_col) for row in rows]),
        "avg_implied": _avg([bol_audit._american_implied(_f(row.get(price_col))) for row in rows]),
        "avg_d7_hits_rate": _avg([row.get("d7_hits_rate") for row in rows]),
        "avg_d15_hits_rate": _avg([row.get("d15_hits_rate") for row in rows]),
        "avg_d7_hits_runs_rbis": _avg([row.get("d7_hits_runs_rbis") for row in rows]),
        "avg_d15_hits_runs_rbis": _avg([row.get("d15_hits_runs_rbis") for row in rows]),
        "avg_starter_expected_hits_allowed": _avg([row.get("starter_expected_hits_allowed") for row in rows]),
        "avg_team_expected_hits_allowed": _avg([row.get("team_expected_hits_allowed") for row in rows]),
        "avg_same_game_teammate_tier_a_count": _avg([row.get("same_game_teammate_tier_a_count") for row in rows]),
        "avg_book_count": _avg([_book_count(row) for row in rows]),
        "betonline_availability_rate": sum(1 for row in rows if _f(row.get("betonline_over_price")) is not None) / len(rows) if rows else None,
        "avg_best_to_betonline_gap": _avg([row.get("price_gap_best_minus_bol") for row in rows]),
        "avg_best_to_median_gap": _avg(
            [
                (_f(row.get("best_available_over_price")) or 0) - (_f(row.get("median_available_over_price")) or 0)
                for row in rows
                if _f(row.get("best_available_over_price")) is not None and _f(row.get("median_available_over_price")) is not None
            ]
        ),
    }


def _profile_label(row: dict[str, Any]) -> str:
    hitter = str(row.get("hitter_tier") or "C")
    pitcher = str(row.get("pitcher_tier") or "U")
    d7 = _f(row.get("d7_hits_rate"))
    d15 = _f(row.get("d15_hits_rate"))
    d7_hrr = _f(row.get("d7_hits_runs_rbis"))
    starter = _f(row.get("starter_expected_hits_allowed"))
    team = _f(row.get("team_expected_hits_allowed"))
    price = _price(row)
    if hitter == "A" or (d7 is not None and d7 > 1.3):
        if price is not None and price <= 300:
            return "overpriced_hot_profile"
        return "obvious_hot_public_profile"
    if hitter == "C" and pitcher in {"A", "B"} and (d7 is None or d7 <= 1.0):
        return "favorable_pitcher_hidden_profile"
    if hitter == "C" and pitcher in {"A", "B"}:
        return "quiet_matchup_profile"
    if price is not None and price >= 401 and pitcher not in {"A", "B"} and (d7_hrr is None or d7_hrr < 2.5):
        return "weak_context_longshot"
    if price is not None and 201 <= price <= 300 and hitter == "C" and pitcher not in {"A", "B"} and (team is None or team < 8.0):
        return "price_only_no_baseball_support"
    if starter is not None and starter >= 5.5 and (team is not None and team >= 9.0):
        return "quiet_matchup_profile"
    if d15 is not None and d15 <= 1.0 and pitcher in {"A", "B"}:
        return "favorable_pitcher_hidden_profile"
    return "unclassified_middle"


def _annotate(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        row["classification_price_bucket"] = _price_bucket(_price(row))
        row["market_classification_label"] = _profile_label(row)
        row["book_count"] = _book_count(row)
        best = _f(row.get("best_available_over_price"))
        median = _f(row.get("median_available_over_price"))
        row["best_to_median_gap"] = best - median if best is not None and median is not None else None


def _metric_row(group_type: str, group_value: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = {"group_type": group_type, "group_value": group_value}
    out.update(_metrics(rows, "best_available_over_price"))
    bol = _metrics(rows, "betonline_over_price")
    out["roi_betonline_price"] = bol["roi"]
    out["units_betonline_price"] = bol["units"]
    out["avg_betonline_price"] = bol["avg_price"]
    return out


def _group(rows: list[dict[str, Any]], group_type: str, func: Callable[[dict[str, Any]], str]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[func(row)].append(row)
    return [_metric_row(group_type, key, groups[key]) for key in sorted(groups)]


def _price_bucket_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for bucket_row in _group(rows, "price_bucket", lambda r: str(r.get("classification_price_bucket") or "missing")):
        out.append(bucket_row)
    return out


def _winner_loser(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "d7_hits_rate",
        "d15_hits_rate",
        "d7_hits_runs_rbis",
        "d15_hits_runs_rbis",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "same_game_teammate_tier_a_count",
        "book_count",
        "price_gap_best_minus_bol",
        "best_to_median_gap",
    ]
    out: list[dict[str, Any]] = []
    for bucket in sorted({str(row.get("classification_price_bucket") or "missing") for row in rows}):
        bucket_rows = [row for row in rows if str(row.get("classification_price_bucket") or "missing") == bucket and _b(row.get("resolved"))]
        winners = [row for row in bucket_rows if _b(row.get("win"))]
        losers = [row for row in bucket_rows if _b(row.get("loss"))]
        for field in fields:
            win_avg = _avg([row.get(field) for row in winners])
            loss_avg = _avg([row.get(field) for row in losers])
            out.append(
                {
                    "price_bucket": bucket,
                    "feature": field,
                    "resolved": len(bucket_rows),
                    "wins": len(winners),
                    "losses": len(losers),
                    "winner_avg": win_avg,
                    "loser_avg": loss_avg,
                    "winner_minus_loser": win_avg - loss_avg if win_avg is not None and loss_avg is not None else None,
                }
            )
        for field in ("hitter_tier", "pitcher_tier", "combined_tier", "market_classification_label"):
            values = sorted({str(row.get(field) or "missing") for row in bucket_rows})
            for value in values:
                sub = [row for row in bucket_rows if str(row.get(field) or "missing") == value]
                wins = sum(1 for row in sub if _b(row.get("win")))
                losses = sum(1 for row in sub if _b(row.get("loss")))
                out.append(
                    {
                        "price_bucket": bucket,
                        "feature": field,
                        "feature_value": value,
                        "resolved": len(sub),
                        "wins": wins,
                        "losses": losses,
                        "win_rate": wins / (wins + losses) if wins + losses else None,
                    }
                )
    return out


def _profiles(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = _group(rows, "market_classification_label", lambda r: str(r.get("market_classification_label") or "missing"))
    for row in out:
        label_rows = [r for r in rows if str(r.get("market_classification_label") or "missing") == row["group_value"]]
        traits = []
        if _avg([r.get("d7_hits_rate") for r in label_rows]) is not None:
            traits.append(f"avg d7={_avg([r.get('d7_hits_rate') for r in label_rows]):.2f}")
        if _avg([r.get("starter_expected_hits_allowed") for r in label_rows]) is not None:
            traits.append(f"starter={_avg([r.get('starter_expected_hits_allowed') for r in label_rows]):.2f}")
        if _avg([r.get("team_expected_hits_allowed") for r in label_rows]) is not None:
            traits.append(f"team={_avg([r.get('team_expected_hits_allowed') for r in label_rows]):.2f}")
        row["key_baseball_traits"] = "; ".join(traits)
    return out


def _slice_candidates(rows: list[dict[str, Any]], bucket: str) -> list[tuple[str, str, list[dict[str, Any]]]]:
    bucket_rows = [row for row in rows if str(row.get("classification_price_bucket") or "") == bucket]
    specs: list[tuple[str, Callable[[dict[str, Any]], str]]] = [
        ("classification", lambda r: str(r.get("market_classification_label") or "missing")),
        ("combined_tier", lambda r: str(r.get("combined_tier") or "missing")),
        ("hitter_tier", lambda r: str(r.get("hitter_tier") or "missing")),
        ("pitcher_tier", lambda r: str(r.get("pitcher_tier") or "missing")),
        ("d7_rate", lambda r: _rate_bucket(r.get("d7_hits_rate"))),
        ("d15_rate", lambda r: _rate_bucket(r.get("d15_hits_rate"))),
        ("d7_hrr", lambda r: _hrr_bucket(r.get("d7_hits_runs_rbis"))),
        ("d15_hrr", lambda r: _hrr_bucket(r.get("d15_hits_runs_rbis"))),
        ("starter_expected", lambda r: _starter_bucket(r.get("starter_expected_hits_allowed"))),
        ("team_expected", lambda r: _team_bucket(r.get("team_expected_hits_allowed"))),
        ("book_count", lambda r: str(r.get("book_count") or 0)),
        ("betonline_available", lambda r: "yes" if _f(r.get("betonline_over_price")) is not None else "no"),
    ]
    out: list[tuple[str, str, list[dict[str, Any]]]] = []
    for slice_type, func in specs:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in bucket_rows:
            grouped[func(row)].append(row)
        for value, group in grouped.items():
            out.append((slice_type, value, group))
    return out


def _top_bottom_by_bucket(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for bucket in sorted({str(row.get("classification_price_bucket") or "missing") for row in rows}):
        bucket_rows = [row for row in rows if str(row.get("classification_price_bucket") or "missing") == bucket]
        base = _metrics(bucket_rows)
        candidates = []
        for slice_type, slice_value, group in _slice_candidates(rows, bucket):
            m = _metrics(group)
            if int(m["resolved"] or 0) >= 10:
                candidates.append((slice_type, slice_value, group, m))
        positive = sorted(candidates, key=lambda item: (_f(item[3].get("roi")) if _f(item[3].get("roi")) is not None else -999), reverse=True)
        negative = sorted(candidates, key=lambda item: (_f(item[3].get("roi")) if _f(item[3].get("roi")) is not None else 999))
        for kind, selected in (("best_positive", positive[:5]), ("worst_negative", negative[:5])):
            for rank, (slice_type, slice_value, group, m) in enumerate(selected, start=1):
                remaining = [row for row in bucket_rows if row not in group] if kind == "worst_negative" else []
                retained = _metrics(remaining) if remaining else {}
                out.append(
                    {
                        "price_bucket": bucket,
                        "kind": kind,
                        "rank": rank,
                        "slice_type": slice_type,
                        "slice_value": slice_value,
                        **m,
                        "bucket_resolved": base.get("resolved"),
                        "bucket_roi": base.get("roi"),
                        "roi_excluding_this_slice": retained.get("roi"),
                        "units_excluding_this_slice": retained.get("units"),
                    }
                )
    return out


def _row_output(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "date",
        "player_name",
        "player_id",
        "team",
        "opponent",
        "line",
        "side",
        "classification_price_bucket",
        "market_classification_label",
        "hitter_tier",
        "pitcher_tier",
        "combined_tier",
        "d7_hits_rate",
        "d15_hits_rate",
        "d7_hits_runs_rbis",
        "d15_hits_runs_rbis",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "same_game_teammate_tier_a_count",
        "book_count",
        "best_available_over_price",
        "betonline_over_price",
        "median_available_over_price",
        "price_gap_best_minus_bol",
        "best_to_median_gap",
        "resolved",
        "win",
        "loss",
        "push",
        "actual_value",
    ]
    return [{field: row.get(field) for field in fields} for row in rows]


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100:.2f}%"


def _fmt_num(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.2f}"


def _write_report(path: Path, bucket_rows: list[dict[str, Any]], profiles: list[dict[str, Any]], winner_loser: list[dict[str, Any]], top_bottom: list[dict[str, Any]]) -> None:
    lines = [
        "# Expanded O1.5 Market Classification Audit",
        "",
        "Scope: alternate-only Expanded O1.5 rows. Price bucket uses best available over price unless otherwise noted.",
        "",
        "## Price Buckets",
        "",
        "| bucket | rows | resolved | W-L-P | ROI best | ROI BOL | avg price | avg d7 | avg d15 | avg starter |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in bucket_rows:
        lines.append(
            f"| {row.get('group_value')} | {row.get('rows')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | "
            f"{_fmt_pct(row.get('roi_betonline_price'))} | {_fmt_num(row.get('avg_price'))} | "
            f"{_fmt_num(row.get('avg_d7_hits_rate'))} | {_fmt_num(row.get('avg_d15_hits_rate'))} | {_fmt_num(row.get('avg_starter_expected_hits_allowed'))} |"
        )
    lines.extend(
        [
            "",
            "## Market Classification Labels",
            "",
            "| label | rows | resolved | W-L-P | ROI best | ROI BOL | avg price | key traits |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in profiles:
        lines.append(
            f"| {row.get('group_value')} | {row.get('rows')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | "
            f"{_fmt_pct(row.get('roi_betonline_price'))} | {_fmt_num(row.get('avg_price'))} | {row.get('key_baseball_traits') or ''} |"
        )
    lines.extend(
        [
            "",
            "## Same Price, Different Baseball",
            "",
            "| bucket | kind | slice | resolved | W-L-P | ROI | bucket ROI | ROI excluding worst slice |",
            "|---|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in top_bottom[:60]:
        lines.append(
            f"| {row.get('price_bucket')} | {row.get('kind')} | {row.get('slice_type')}={row.get('slice_value')} | "
            f"{row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | "
            f"{_fmt_pct(row.get('roi'))} | {_fmt_pct(row.get('bucket_roi'))} | {_fmt_pct(row.get('roi_excluding_this_slice'))} |"
        )
    notable = [row for row in winner_loser if row.get("price_bucket") in {"+201_to_+250", "+251_to_+300"} and row.get("feature") in {"d7_hits_rate", "d15_hits_rate", "d7_hits_runs_rbis", "starter_expected_hits_allowed", "team_expected_hits_allowed"}]
    lines.extend(
        [
            "",
            "## Winner vs Loser Feature Deltas: +201 to +300",
            "",
            "| bucket | feature | winner avg | loser avg | delta |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for row in notable:
        lines.append(
            f"| {row.get('price_bucket')} | {row.get('feature')} | {_fmt_num(row.get('winner_avg'))} | "
            f"{_fmt_num(row.get('loser_avg'))} | {_fmt_num(row.get('winner_minus_loser'))} |"
        )
    lines.extend(
        [
            "",
            "## Classification Rules",
            "",
            "- `overpriced_hot_profile`: Hitter Tier A or d7 > 1.3 at price <= +300.",
            "- `obvious_hot_public_profile`: Hitter Tier A or d7 > 1.3 outside the shorter +300-or-better range.",
            "- `favorable_pitcher_hidden_profile`: Hitter Tier C with pitcher tier A/B and d7 <= 1.0, or d15 <= 1.0 with pitcher tier A/B.",
            "- `quiet_matchup_profile`: Hitter Tier C with pitcher tier A/B, or very high starter/team expected hits allowed.",
            "- `weak_context_longshot`: price >= +401 without A/B pitcher support and d7 HRR < 2.5.",
            "- `price_only_no_baseball_support`: +201 to +300, Hitter Tier C, no A/B pitcher support, and team expected hits < 8 or missing.",
            "",
            "## Field Availability Caveat",
            "",
            "- `team_d7_runs_per_game` and BvP fields are not currently present in `expanded_o15_universe_rows.csv`, so this audit cannot yet test those dimensions inside the expanded universe.",
            "- `same_game_teammate_tier_a_count` is present only where source boards supplied it; blank averages mean the field was not populated for that slice.",
            "",
            "## Readout",
            "",
            "- This audit supports the idea that alternate O1.5 is a market-classification problem, not a simple hot-hitter problem.",
            "- The useful question is not whether a player is obviously hot, but whether a similarly priced player has hidden matchup/support traits.",
            "- Price +201 to +300 remains useful, but too broad by itself; it needs baseball-profile slicing.",
            "- `overpriced_hot_profile` is the clearest EV-bait candidate; `favorable_pitcher_hidden_profile` is the cleaner baseball-supported value candidate.",
            "- The broad `quiet_matchup_profile` label is not yet good enough; it needs refinement because it was negative while the stricter hidden-pitcher profile was positive.",
            "- The next research thread should focus on favorable hidden pitcher/team support versus overpriced hot profiles inside +201 to +300.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build market-classification audit for Expanded O1.5 alternate-only rows.")
    ap.add_argument("--rows-csv", default=str(DEFAULT_ROWS_CSV))
    ap.add_argument("--backfill-root", default=str(DEFAULT_BACKFILL_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    rows = [row for row in _read_csv(Path(args.rows_csv)) if _b(row.get("from_alternate")) and not _b(row.get("from_both"))]
    bol_audit._enrich(rows, Path(args.backfill_root))
    _annotate(rows)

    bucket_rows = _price_bucket_summary(rows)
    profile_rows = _profiles(rows)
    winner_loser_rows = _winner_loser(rows)
    top_bottom_rows = _top_bottom_by_bucket(rows)
    row_rows = _row_output(rows)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "expanded_o15_market_classification_price_buckets.csv", bucket_rows)
    _write_csv(out_dir / "expanded_o15_market_classification_profiles.csv", profile_rows)
    _write_csv(out_dir / "expanded_o15_market_classification_winner_loser.csv", winner_loser_rows)
    _write_csv(out_dir / "expanded_o15_market_classification_rows.csv", row_rows)
    _write_report(out_dir / "expanded_o15_market_classification_audit.md", bucket_rows, profile_rows, winner_loser_rows, top_bottom_rows)
    print(
        {
            "alternate_only_rows": len(rows),
            "report": str(out_dir / "expanded_o15_market_classification_audit.md"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
