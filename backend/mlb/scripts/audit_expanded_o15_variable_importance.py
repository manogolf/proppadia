#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import analyze_expanded_o15_universe_slices as slices
from backend.mlb.scripts import audit_expanded_o15_betonline as bol_audit
from backend.mlb.scripts import audit_expanded_o15_hidden_matchup_support as hidden
from backend.mlb.scripts import audit_expanded_o15_market_classification as market


DEFAULT_ROWS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
DEFAULT_BACKFILL_ROOT = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")
MIN_RESOLVED = 50
MISSING_BUCKETS = {"missing", "0_or_missing", "other"}


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


def _date(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("board_date") or "")[:10]


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100.0:.2f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.{digits}f}"


def _bucket_number(value: Any, cuts: list[tuple[str, float | None, float | None]], missing: str = "missing") -> str:
    number = _f(value)
    if number is None:
        return missing
    for label, low, high in cuts:
        if low is not None and number < low:
            continue
        if high is not None and number >= high:
            continue
        return label
    return "other"


def _clean(value: Any, missing: str = "missing") -> str:
    text = str(value or "").strip()
    return text if text else missing


def _bool_bucket(value: Any, true_label: str = "yes", false_label: str = "no") -> str:
    return true_label if _b(value) else false_label


def _american_implied(price: Any) -> float | None:
    return bol_audit._american_implied(_f(price))


def _price_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<=150", None, 151),
            ("151-200", 151, 201),
            ("201-250", 201, 251),
            ("251-300", 251, 301),
            ("301-400", 301, 401),
            ("401-600", 401, 601),
            (">600", 601, None),
        ],
    )


def _implied_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<20%", None, 0.20),
            ("20-25%", 0.20, 0.25),
            ("25-30%", 0.25, 0.30),
            ("30-35%", 0.30, 0.35),
            (">=35%", 0.35, None),
        ],
    )


def _rate_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<=0.7", None, 0.7000001),
            ("0.7-1.0", 0.7000001, 1.0000001),
            ("1.0-1.1", 1.0000001, 1.1000001),
            ("1.1-1.3", 1.1000001, 1.3000001),
            (">1.3", 1.3000001, None),
        ],
    )


def _hrr_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<1.5", None, 1.5),
            ("1.5-2.5", 1.5, 2.5),
            ("2.5-3.0", 2.5, 3.0),
            ("3.0-3.5", 3.0, 3.5),
            (">=3.5", 3.5, None),
        ],
    )


def _expected_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<4.5", None, 4.5),
            ("4.5-5.0", 4.5, 5.0),
            ("5.0-5.5", 5.0, 5.5),
            (">=5.5", 5.5, None),
        ],
    )


def _team_expected_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<7", None, 7),
            ("7-8", 7, 8),
            ("8-9", 8, 9),
            (">=9", 9, None),
        ],
    )


def _count_bucket(value: Any) -> str:
    number = _f(value)
    if number is None:
        return "missing"
    if number <= 0:
        return "0"
    if number == 1:
        return "1"
    if number == 2:
        return "2"
    return "3+"


def _book_count(row: dict[str, Any]) -> int:
    source = _f(row.get("book_count"))
    if source is not None:
        return int(source)
    source = _f(row.get("book_count_from_source"))
    if source is not None:
        return int(source)
    return len([x for x in str(row.get("bookmaker_list") or "").split(",") if x.strip()])


def _book_count_bucket(row: dict[str, Any]) -> str:
    count = _book_count(row)
    if count <= 1:
        return "1"
    if count <= 3:
        return "2-3"
    if count <= 5:
        return "4-5"
    return "6+"


def _gap_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("no_gap_or_bol_best", None, 0.0000001),
            ("1-25", 0.0000001, 26),
            ("26-75", 26, 76),
            (">75", 76, None),
        ],
        missing="no_betonline",
    )


def _bvp_pa_bucket(row: dict[str, Any]) -> str:
    pa = _f(row.get("bvp_plate_appearances"))
    if pa is None:
        return "no_payload"
    if pa <= 0:
        return "0"
    if pa < 3:
        return "1-2"
    if pa < 5:
        return "3-4"
    return ">=5"


def _avg_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<.200", None, 0.200),
            (".200-.249", 0.200, 0.250),
            (".250-.299", 0.250, 0.300),
            (">=.300", 0.300, None),
        ],
    )


def _slg_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<.300", None, 0.300),
            (".300-.349", 0.300, 0.350),
            (".350-.499", 0.350, 0.500),
            (">=.500", 0.500, None),
        ],
    )


def _trend_bucket(row: dict[str, Any]) -> str:
    d7 = _f(row.get("d7_hits_rate"))
    d15 = _f(row.get("d15_hits_rate"))
    if d7 is None or d15 is None:
        return "missing"
    diff = d7 - d15
    if diff >= 0.15:
        return "d7_rising_vs_d15"
    if diff <= -0.15:
        return "d7_falling_vs_d15"
    return "stable"


def _team_stat_bucket(row: dict[str, Any], cols: tuple[str, ...]) -> str:
    for col in cols:
        if col in row and _f(row.get(col)) is not None:
            return _bucket_number(
                row.get(col),
                [
                    ("low", None, 4.0),
                    ("mid", 4.0, 5.5),
                    ("high", 5.5, None),
                ],
            )
    return "missing"


def _team_total_bases_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<12", None, 12.0),
            ("12-15", 12.0, 15.0),
            (">=15", 15.0, None),
        ],
    )


def _home_away(row: dict[str, Any]) -> str:
    value = _clean(row.get("home_away"), "")
    if value:
        return value
    is_home = str(row.get("is_home") or "").strip().lower()
    if is_home in {"1", "true", "yes", "home"}:
        return "home"
    if is_home in {"0", "false", "no", "away"}:
        return "away"
    return "missing"


def _rest_bucket(row: dict[str, Any]) -> str:
    for col in ("team_time_sequence_bucket", "rest_context", "rest_bucket"):
        value = _clean(row.get(col), "")
        if value:
            return value
    return "missing"


def _lineup_slot_bucket(row: dict[str, Any]) -> str:
    value = _clean(row.get("lineup_slot_bucket"), "")
    if value:
        return value
    slot = _f(row.get("lineup_slot") or row.get("batting_order"))
    if slot is None:
        return "missing"
    if slot <= 2:
        return "top_1_2"
    if slot <= 5:
        return "middle_3_5"
    if slot <= 7:
        return "lower_6_7"
    return "bottom_8_9"


def _boolean_field_bucket(row: dict[str, Any], field: str) -> str:
    if field not in row or str(row.get(field) or "").strip() == "":
        return "missing"
    return "yes" if _b(row.get(field)) else "no"


def _units(row: dict[str, Any], price_col: str) -> float | None:
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
        "avg_odds": _avg([row.get(price_col) for row in rows]),
        "avg_implied": _avg([_american_implied(row.get(price_col)) for row in rows]),
        "avg_d7_hits_rate": _avg([row.get("d7_hits_rate") for row in rows]),
        "avg_d15_hits_rate": _avg([row.get("d15_hits_rate") for row in rows]),
        "avg_d7_hits_runs_rbis": _avg([row.get("d7_hits_runs_rbis") for row in rows]),
        "avg_d15_hits_runs_rbis": _avg([row.get("d15_hits_runs_rbis") for row in rows]),
        "avg_starter_expected_hits_allowed": _avg([row.get("starter_expected_hits_allowed") for row in rows]),
        "avg_team_expected_hits_allowed": _avg([row.get("team_expected_hits_allowed") for row in rows]),
        "avg_book_count": _avg([_book_count(row) for row in rows]),
        "avg_best_to_betonline_gap": _avg([row.get("price_gap_best_minus_bol") for row in rows]),
    }


def _date_stability(rows: list[dict[str, Any]], price_col: str = "best_available_over_price") -> dict[str, Any]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if _b(row.get("resolved")):
            by_date[_date(row)].append(row)
    daily = []
    for date_text, date_rows in by_date.items():
        metric = _metrics(date_rows, price_col)
        if int(metric.get("priced_resolved") or 0) > 0:
            daily.append({"date": date_text, **metric})
    positive = [row for row in daily if (_f(row.get("roi")) or 0.0) > 0]
    return {
        "resolved_date_count": len(daily),
        "positive_roi_date_count": len(positive),
        "positive_roi_date_rate": len(positive) / len(daily) if daily else None,
        "stability_score": (len(positive) / len(daily)) * min(1.0, len(daily) / 7.0) if daily else None,
    }


def _metric_row(variable: str, bucket: str, rows: list[dict[str, Any]], *, price_col: str = "best_available_over_price") -> dict[str, Any]:
    out = {"analysis_type": "single_variable", "variable": variable, "bucket": bucket}
    out.update(_metrics(rows, price_col))
    out.update(_date_stability(rows, price_col))
    bol = _metrics(rows, "betonline_over_price")
    out["roi_betonline"] = bol["roi"]
    out["units_betonline"] = bol["units"]
    out["priced_resolved_betonline"] = bol["priced_resolved"]
    out["avg_betonline_odds"] = bol["avg_odds"]
    return out


BucketFunc = Callable[[dict[str, Any]], str]


def _variable_specs() -> list[tuple[str, str, BucketFunc]]:
    return [
        ("player", "hitter_tier", lambda r: _clean(r.get("hitter_tier"))),
        ("player", "combined_tier", lambda r: _clean(r.get("combined_tier"))),
        ("player", "d7_hits_rate_bucket", lambda r: _rate_bucket(r.get("d7_hits_rate"))),
        ("player", "d15_hits_rate_bucket", lambda r: _rate_bucket(r.get("d15_hits_rate"))),
        ("player", "d7_hrr_bucket", lambda r: _hrr_bucket(r.get("d7_hits_runs_rbis"))),
        ("player", "d15_hrr_bucket", lambda r: _hrr_bucket(r.get("d15_hits_runs_rbis"))),
        ("player", "recent_hit_trend", _trend_bucket),
        ("pitcher", "starter_expected_bucket", lambda r: _expected_bucket(r.get("starter_expected_hits_allowed"))),
        ("pitcher", "pitcher_tier", lambda r: _clean(r.get("pitcher_tier"))),
        ("team", "team_expected_bucket", lambda r: _team_expected_bucket(r.get("team_expected_hits_allowed"))),
        ("team", "team_d7_runs_bucket", lambda r: _team_stat_bucket(r, ("team_d7_runs_per_game", "team_d7_runs"))),
        ("team", "team_d7_hits_bucket", lambda r: _team_stat_bucket(r, ("team_d7_hits_per_game", "team_d7_hits"))),
        ("team", "team_d7_total_bases_bucket", lambda r: _team_total_bases_bucket(r.get("team_d7_total_bases_per_game"))),
        ("team", "team_d15_runs_bucket", lambda r: _team_stat_bucket(r, ("team_d15_runs_per_game", "team_d15_runs"))),
        ("team", "team_d15_hits_bucket", lambda r: _team_stat_bucket(r, ("team_d15_hits_per_game", "team_d15_hits"))),
        ("team", "team_d15_total_bases_bucket", lambda r: _team_total_bases_bucket(r.get("team_d15_total_bases_per_game"))),
        ("team", "lineup_heat_cluster", lambda r: ">0" if (_f(r.get("same_game_teammate_tier_a_count")) or 0) > 0 else "0_or_missing"),
        ("team", "same_game_teammate_tier_a_count", lambda r: _count_bucket(r.get("same_game_teammate_tier_a_count"))),
        ("team", "same_game_team_o15_candidate_count", lambda r: _count_bucket(r.get("same_game_team_o15_candidate_count"))),
        ("team", "same_game_teammate_o15_candidate_count", lambda r: _count_bucket(r.get("same_game_teammate_o15_candidate_count"))),
        ("market", "price_bucket_best", lambda r: _price_bucket(r.get("best_available_over_price") or r.get("expanded_price"))),
        ("market", "implied_probability_bucket", lambda r: _implied_bucket(_american_implied(r.get("best_available_over_price") or r.get("expanded_price")))),
        ("market", "betonline_available", lambda r: _bool_bucket(r.get("betonline_available")) if "betonline_available" in r else ("yes" if _f(r.get("betonline_over_price")) is not None else "no")),
        ("market", "betonline_price_bucket", lambda r: _price_bucket(r.get("betonline_over_price"))),
        ("market", "betonline_price_gap_bucket", lambda r: _gap_bucket(r.get("price_gap_best_minus_bol"))),
        ("market", "bookmaker_count_bucket", _book_count_bucket),
        ("market", "source_bucket", lambda r: _clean(r.get("source_bucket"))),
        ("bvp", "bvp_payload_present", lambda r: _bool_bucket(r.get("bvp_payload_present"))),
        ("bvp", "bvp_pa_bucket", _bvp_pa_bucket),
        ("bvp", "bvp_avg_bucket", lambda r: _avg_bucket(r.get("bvp_avg"))),
        ("bvp", "bvp_slg_bucket", lambda r: _slg_bucket(r.get("bvp_slg"))),
        ("game", "home_away", _home_away),
        ("game", "time_of_day_bucket", lambda r: _clean(r.get("time_of_day_bucket"))),
        ("game", "game_day_of_week", lambda r: _clean(r.get("game_day_of_week"))),
        ("game", "day_after_night", lambda r: "day_after_night" if _rest_bucket(r) == "day_after_night" else ("missing" if _rest_bucket(r) == "missing" else "not_day_after_night")),
        ("game", "rest_context", _rest_bucket),
        ("game", "short_turnaround", lambda r: _boolean_field_bucket(r, "short_turnaround")),
        ("game", "rest_day_before_game", lambda r: _boolean_field_bucket(r, "rest_day_before_game")),
        ("game", "lineup_slot_bucket", _lineup_slot_bucket),
        ("game", "park", lambda r: _clean(r.get("park") or r.get("venue") or r.get("ballpark"))),
    ]


def _annotate(rows: list[dict[str, Any]], backfill_root: Path) -> None:
    bol_audit._enrich(rows, backfill_root)
    market._annotate(rows)
    hidden._annotate_bvp(rows)
    for row in rows:
        best = _f(row.get("best_available_over_price"))
        if best is None:
            row["best_available_over_price"] = _f(row.get("expanded_price") or row.get("market_price") or row.get("best_over_price"))
        if _f(row.get("median_available_over_price")) is None:
            row["median_available_over_price"] = row.get("best_available_over_price")


def _single_variable_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for category, variable, func in _variable_specs():
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[func(row)].append(row)
        for bucket, group_rows in sorted(groups.items()):
            item = {"category": category, **_metric_row(variable, bucket, group_rows)}
            out.append(item)
    return out


def _variable_bucket_inventory(rows: list[dict[str, Any]]) -> dict[str, set[str]]:
    inventory: dict[str, set[str]] = defaultdict(set)
    for _category, variable, func in _variable_specs():
        for row in rows:
            inventory[variable].add(func(row))
    return inventory


def _variable_has_signal_coverage(variable: str, inventory: dict[str, set[str]]) -> bool:
    buckets = inventory.get(variable, set())
    return any(bucket not in MISSING_BUCKETS for bucket in buckets)


def _rankable_bucket(variable: str, bucket: str, inventory: dict[str, set[str]]) -> bool:
    if not _variable_has_signal_coverage(variable, inventory):
        return False
    if bucket in MISSING_BUCKETS:
        return False
    return True


def _rank_variable_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    inventory: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        inventory[str(row.get("variable") or "")].add(str(row.get("bucket") or ""))
    eligible = [
        row
        for row in rows
        if int(row.get("resolved") or 0) >= MIN_RESOLVED
        and _f(row.get("roi")) is not None
        and _rankable_bucket(str(row.get("variable") or ""), str(row.get("bucket") or ""), inventory)
    ]
    by_roi = sorted(eligible, key=lambda r: _f(r.get("roi")) or -999, reverse=True)
    by_stability = sorted(eligible, key=lambda r: _f(r.get("stability_score")) or -999, reverse=True)
    by_sample = sorted(eligible, key=lambda r: int(r.get("resolved") or 0), reverse=True)
    roi_rank = {id(row): idx for idx, row in enumerate(by_roi, start=1)}
    stability_rank = {id(row): idx for idx, row in enumerate(by_stability, start=1)}
    sample_rank = {id(row): idx for idx, row in enumerate(by_sample, start=1)}
    eligible_ids = {id(row) for row in eligible}
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        is_eligible = id(row) in eligible_ids
        item["rank_eligible"] = is_eligible
        if is_eligible:
            item["rank_roi"] = roi_rank[id(row)]
            item["rank_stability"] = stability_rank[id(row)]
            item["rank_sample_size"] = sample_rank[id(row)]
            item["composite_rank"] = item["rank_roi"] + item["rank_stability"] + item["rank_sample_size"]
        else:
            item["rank_exclusion_reason"] = (
                "minimum_resolved"
                if int(row.get("resolved") or 0) < MIN_RESOLVED
                else "missing_or_noninformative_bucket"
                if not _rankable_bucket(str(row.get("variable") or ""), str(row.get("bucket") or ""), inventory)
                else "missing_roi"
            )
        out.append(item)
    return sorted(
        out,
        key=lambda r: (
            0 if _b(r.get("rank_eligible")) else 1,
            int(r.get("composite_rank") or 999999),
            int(r.get("rank_roi") or 999999),
            str(r.get("variable") or ""),
            str(r.get("bucket") or ""),
        ),
    )


def _pairwise_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    specs = _variable_specs()
    inventory = _variable_bucket_inventory(rows)
    specs = [spec for spec in specs if _variable_has_signal_coverage(spec[1], inventory)]
    out: list[dict[str, Any]] = []
    for (_cat_a, var_a, func_a), (_cat_b, var_b, func_b) in combinations(specs, 2):
        groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            bucket_a = func_a(row)
            bucket_b = func_b(row)
            if not _rankable_bucket(var_a, bucket_a, inventory) or not _rankable_bucket(var_b, bucket_b, inventory):
                continue
            groups[(bucket_a, bucket_b)].append(row)
        for (bucket_a, bucket_b), group_rows in sorted(groups.items()):
            metric = _metrics(group_rows, "best_available_over_price")
            if int(metric.get("resolved") or 0) < MIN_RESOLVED:
                continue
            bol = _metrics(group_rows, "betonline_over_price")
            item = {
                "variable_a": var_a,
                "bucket_a": bucket_a,
                "variable_b": var_b,
                "bucket_b": bucket_b,
                **metric,
                "roi_betonline": bol["roi"],
                "units_betonline": bol["units"],
                "priced_resolved_betonline": bol["priced_resolved"],
                "avg_betonline_odds": bol["avg_odds"],
            }
            item.update(_date_stability(group_rows, "best_available_over_price"))
            out.append(item)
    ranked = sorted(out, key=lambda r: _f(r.get("roi")) or -999, reverse=True)
    for idx, row in enumerate(ranked, start=1):
        row["rank_roi_desc"] = idx
    ranked_neg = sorted(out, key=lambda r: _f(r.get("roi")) if _f(r.get("roi")) is not None else 999)
    for idx, row in enumerate(ranked_neg, start=1):
        row["rank_roi_asc"] = idx
    return sorted(out, key=lambda r: (_f(r.get("roi")) or -999), reverse=True)


def _archetypes(variable_rankings: list[dict[str, Any]], interactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    positives = [row for row in interactions if (_f(row.get("roi")) or -999) > 0]
    negatives = [row for row in interactions if (_f(row.get("roi")) or 999) < 0]
    positives = sorted(positives, key=lambda r: (_f(r.get("roi")) or -999, int(r.get("resolved") or 0)), reverse=True)
    negatives = sorted(negatives, key=lambda r: (_f(r.get("roi")) if _f(r.get("roi")) is not None else 999, -int(r.get("resolved") or 0)))
    archetype_rows: list[dict[str, Any]] = []
    for label, source_rows, kind in (
        ("positive_candidate", positives[:12], "support"),
        ("negative_candidate", negatives[:12], "avoid"),
    ):
        for rank, row in enumerate(source_rows, start=1):
            archetype_rows.append(
                {
                    "archetype_type": label,
                    "rank": rank,
                    "kind": kind,
                    "definition": f"{row.get('variable_a')}={row.get('bucket_a')} AND {row.get('variable_b')}={row.get('bucket_b')}",
                    "resolved": row.get("resolved"),
                    "wins": row.get("wins"),
                    "losses": row.get("losses"),
                    "wr": row.get("wr"),
                    "roi": row.get("roi"),
                    "roi_betonline": row.get("roi_betonline"),
                    "units": row.get("units"),
                    "avg_odds": row.get("avg_odds"),
                    "stability_score": row.get("stability_score"),
                    "note": "Candidate archetype from pairwise survey; not a rule.",
                }
            )
    ranked_variables = [row for row in variable_rankings if _b(row.get("rank_eligible"))]
    for rank, row in enumerate(ranked_variables[:12], start=1):
        archetype_rows.append(
            {
                "archetype_type": "single_variable_signal",
                "rank": rank,
                "kind": "support" if (_f(row.get("roi")) or 0) > 0 else "avoid",
                "definition": f"{row.get('variable')}={row.get('bucket')}",
                "resolved": row.get("resolved"),
                "wins": row.get("wins"),
                "losses": row.get("losses"),
                "wr": row.get("wr"),
                "roi": row.get("roi"),
                "roi_betonline": row.get("roi_betonline"),
                "units": row.get("units"),
                "avg_odds": row.get("avg_odds"),
                "stability_score": row.get("stability_score"),
                "note": "Broad single-variable signal with at least 50 resolved.",
            }
        )
    return archetype_rows


def _write_report(
    path: Path,
    rows: list[dict[str, Any]],
    variable_rows: list[dict[str, Any]],
    rankings: list[dict[str, Any]],
    interactions: list[dict[str, Any]],
    archetypes: list[dict[str, Any]],
) -> None:
    base = _metrics(rows, "best_available_over_price")
    top_vars = [row for row in rankings if _b(row.get("rank_eligible"))][:15]
    top_interactions = sorted(interactions, key=lambda r: _f(r.get("roi")) or -999, reverse=True)[:25]
    bottom_interactions = sorted(interactions, key=lambda r: _f(r.get("roi")) if _f(r.get("roi")) is not None else 999)[:25]
    lines = [
        "# Expanded O1.5 Variable Importance Survey",
        "",
        "Scope: all resolved rows in the Expanded O1.5 Universe. No pre-filtering, no funnel optimization, no production changes.",
        "",
        "## Baseline",
        "",
        f"- Candidate rows: `{len(rows)}`",
        f"- Resolved rows: `{base.get('resolved')}`",
        f"- Record: `{base.get('wins')}-{base.get('losses')}-{base.get('pushes')}`",
        f"- ROI at best available price: `{_fmt_pct(base.get('roi'))}`",
        f"- Units: `{_fmt_num(base.get('units'))}`",
        "",
        "## Ranking Method",
        "",
        f"- Variable rankings only include buckets with at least `{MIN_RESOLVED}` resolved rows.",
        "- Stability is the share of resolved dates with positive ROI, scaled down when a bucket appears on fewer than 7 resolved dates.",
        "- Pairwise interactions are not optimized funnels; they are simple two-variable bucket intersections with the same minimum resolved floor.",
        "",
        "## Top Single-Variable Buckets",
        "",
        "| variable | bucket | resolved | W-L-P | ROI | BOL ROI | stability | avg odds |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in top_vars:
        lines.append(
            f"| {row.get('variable')} | {row.get('bucket')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | "
            f"{_fmt_pct(row.get('roi_betonline'))} | {_fmt_pct(row.get('stability_score'))} | {_fmt_num(row.get('avg_odds'))} |"
        )
    lines.extend(
        [
            "",
            "## Top Positive Pairwise Interactions",
            "",
            "| interaction | resolved | W-L-P | ROI | BOL ROI | stability | avg odds |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in top_interactions:
        label = f"{row.get('variable_a')}={row.get('bucket_a')} x {row.get('variable_b')}={row.get('bucket_b')}"
        lines.append(
            f"| {label} | {row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | "
            f"{_fmt_pct(row.get('roi'))} | {_fmt_pct(row.get('roi_betonline'))} | {_fmt_pct(row.get('stability_score'))} | {_fmt_num(row.get('avg_odds'))} |"
        )
    lines.extend(
        [
            "",
            "## Top Negative Pairwise Interactions",
            "",
            "| interaction | resolved | W-L-P | ROI | BOL ROI | stability | avg odds |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in bottom_interactions:
        label = f"{row.get('variable_a')}={row.get('bucket_a')} x {row.get('variable_b')}={row.get('bucket_b')}"
        lines.append(
            f"| {label} | {row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | "
            f"{_fmt_pct(row.get('roi'))} | {_fmt_pct(row.get('roi_betonline'))} | {_fmt_pct(row.get('stability_score'))} | {_fmt_num(row.get('avg_odds'))} |"
        )
    lines.extend(
        [
            "",
            "## Broad Factor Read",
            "",
            "- Positive BvP quality is the clearest baseball context signal with enough sample: AVG `>= .300` and SLG `>= .500` both rank well independently and recur in the strongest pairwise interactions.",
            "- Time context matters in the current sample: `evening` is the strongest broad single-variable bucket and repeatedly strengthens BvP/recent-production buckets.",
            "- The alternate universe still carries most of the opportunity count, but BetOnline-only economics are weaker unless a specific market-structure bucket is present.",
            "- Price structure matters more than raw hitter heat: `201-250` is positive, while shorter `151-200` / implied `>=35%` repeatedly appears in negative interactions.",
            "- Quiet hitter / Tier C context is not bad in this universe; Tier C plus positive BvP or high team expected support is one of the more repeatable positive themes.",
            "",
            "## Coverage Caveats",
            "",
        "- Team offense, same-game clustering, home/away, and rest context are now partially hydrated from same-date artifacts and prior-game sources; rankings only include buckets that clear the 50-resolved floor.",
        "- Park/venue and batting-order/lineup-slot context remain sparse or unavailable in current expanded rows and are excluded from ranked signal tables when missing-only.",
            "- BvP is useful when present, but coverage is still partial; treat BvP-positive as confirmation and BvP-negative as an avoid candidate only when supported by price/context.",
            "- These rankings use best available price as the primary ROI view and include BetOnline ROI separately. Any actionability discussion should check the BetOnline columns first.",
        ]
    )
    lines.extend(
        [
            "",
            "## Candidate Archetypes",
            "",
            "These are broad patterns emerging from single-variable and pairwise rankings. They are not rules.",
            "",
            "| type | definition | resolved | W-L | ROI | BOL ROI | note |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in archetypes[:20]:
        lines.append(
            f"| {row.get('archetype_type')} | {row.get('definition')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')} | {_fmt_pct(row.get('roi'))} | {_fmt_pct(row.get('roi_betonline'))} | {row.get('note')} |"
        )
    lines.extend(
        [
            "",
            "## Answer",
            "",
            "The variable survey should be treated as the next research baseline: start with broad, repeatable factors with at least 50 resolved rows, then form archetypes from repeated positive or negative interactions. Tiny high-ROI funnels remain hypothesis generators only.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_archetype_report(path: Path, archetypes: list[dict[str, Any]]) -> None:
    lines = [
        "# Expanded O1.5 Candidate Archetypes",
        "",
        "These archetypes are derived from broad variable rankings and pairwise interactions with at least 50 resolved rows. They are research candidates, not filters.",
        "",
        "| type | rank | definition | resolved | W-L | ROI | BOL ROI | stability |",
        "|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for row in archetypes:
        lines.append(
            f"| {row.get('archetype_type')} | {row.get('rank')} | {row.get('definition')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')} | {_fmt_pct(row.get('roi'))} | {_fmt_pct(row.get('roi_betonline'))} | {_fmt_pct(row.get('stability_score'))} |"
        )
    lines.extend(
        [
            "",
            "## Use",
            "",
            "Use this file to decide which broad factors deserve deeper study. Do not convert these rows into production thresholds without separate validation.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(rows_csv: Path, out_dir: Path, backfill_root: Path) -> dict[str, Any]:
    rows = _read_csv(rows_csv)
    _annotate(rows, backfill_root)
    variable_rows = _single_variable_rows(rows)
    rankings = _rank_variable_rows(variable_rows)
    interactions = _pairwise_rows(rows)
    archetypes = _archetypes(rankings, interactions)
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "expanded_o15_variable_rankings.csv", rankings)
    _write_csv(out_dir / "expanded_o15_pairwise_interactions.csv", interactions)
    _write_csv(out_dir / "expanded_o15_candidate_archetypes.csv", archetypes)
    _write_report(out_dir / "expanded_o15_variable_importance.md", rows, variable_rows, rankings, interactions, archetypes)
    _write_report(
        out_dir / "expanded_o15_variable_importance_after_context_hydration.md",
        rows,
        variable_rows,
        rankings,
        interactions,
        archetypes,
    )
    _write_archetype_report(out_dir / "expanded_o15_candidate_archetypes.md", archetypes)
    return {
        "rows": len(rows),
        "resolved": sum(1 for row in rows if _b(row.get("resolved"))),
        "variable_rankings": len(rankings),
        "pairwise_interactions": len(interactions),
        "report": str(out_dir / "expanded_o15_variable_importance.md"),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Survey broad variable importance in the Expanded O1.5 Universe.")
    ap.add_argument("--rows-csv", type=Path, default=DEFAULT_ROWS_CSV)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--backfill-root", type=Path, default=DEFAULT_BACKFILL_ROOT)
    args = ap.parse_args()
    print(run(args.rows_csv, args.out_dir, args.backfill_root))


if __name__ == "__main__":
    main()
