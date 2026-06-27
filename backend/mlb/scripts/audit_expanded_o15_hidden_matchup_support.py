#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import audit_expanded_o15_market_classification as market


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
    return market._f(value)


def _b(value: Any) -> bool:
    return market._b(value)


def _avg(values: list[Any]) -> float | None:
    return market._avg(values)


def _metric_triplet(rows: list[dict[str, Any]]) -> dict[str, Any]:
    best = market._metrics(rows, "best_available_over_price")
    bol = market._metrics(rows, "betonline_over_price")
    median = market._metrics(rows, "median_available_over_price")
    out = {
        "candidates": len(rows),
        "resolved": best["resolved"],
        "wins": best["wins"],
        "losses": best["losses"],
        "pushes": best["pushes"],
        "wr": best["wr"],
        "roi_best_price": best["roi"],
        "units_best_price": best["units"],
        "roi_betonline_price": bol["roi"],
        "units_betonline_price": bol["units"],
        "roi_median_price": median["roi"],
        "units_median_price": median["units"],
        "avg_best_price": best["avg_price"],
        "avg_betonline_price": bol["avg_price"],
        "avg_median_price": median["avg_price"],
        "avg_d7_hits_rate": best["avg_d7_hits_rate"],
        "avg_d15_hits_rate": best["avg_d15_hits_rate"],
        "avg_d7_hits_runs_rbis": best["avg_d7_hits_runs_rbis"],
        "avg_d15_hits_runs_rbis": best["avg_d15_hits_runs_rbis"],
        "avg_starter_expected_hits_allowed": best["avg_starter_expected_hits_allowed"],
        "avg_team_expected_hits_allowed": best["avg_team_expected_hits_allowed"],
        "avg_same_game_teammate_tier_a_count": best["avg_same_game_teammate_tier_a_count"],
        "avg_book_count": best["avg_book_count"],
        "betonline_availability_rate": best["betonline_availability_rate"],
    }
    return out


def _metric_row(row_type: str, name: str, rows: list[dict[str, Any]], *, note: str = "") -> dict[str, Any]:
    out = {"row_type": row_type, "name": name, "note": note}
    out.update(_metric_triplet(rows))
    return out


def _pa(row: dict[str, Any]) -> float:
    return _f(row.get("bvp_plate_appearances")) or 0.0


def _bvp_payload(row: dict[str, Any]) -> bool:
    return _b(row.get("bvp_payload_present")) or any(
        _f(row.get(col)) is not None for col in ("bvp_plate_appearances", "bvp_at_bats", "bvp_hits", "bvp_total_bases")
    )


def _annotate_bvp(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        pa = _pa(row)
        avg = _f(row.get("bvp_avg"))
        slg = _f(row.get("bvp_slg"))
        row["bvp_none_or_low_sample"] = pa < 5
        row["bvp_positive_pa5_avg_250"] = pa >= 5 and avg is not None and avg >= 0.250
        row["bvp_negative_pa5_avg_lt_250"] = pa >= 5 and avg is not None and avg < 0.250
        row["bvp_positive_pa5_slg_350"] = pa >= 5 and slg is not None and slg >= 0.350
        row["bvp_negative_pa5_slg_lt_350"] = pa >= 5 and slg is not None and slg < 0.350
        row["bvp_strong_positive_pa5_avg_300_or_slg_500"] = pa >= 5 and (
            (avg is not None and avg >= 0.300) or (slg is not None and slg >= 0.500)
        )
        row["bvp_strong_negative_pa5_avg_200_and_slg_300"] = pa >= 5 and (
            avg is not None and avg < 0.200 and slg is not None and slg < 0.300
        )
        row["bvp_positive_pa3_avg_250"] = pa >= 3 and avg is not None and avg >= 0.250
        row["bvp_negative_pa3_avg_lt_250"] = pa >= 3 and avg is not None and avg < 0.250
        row["bvp_positive_pa3_slg_350"] = pa >= 3 and slg is not None and slg >= 0.350
        row["bvp_negative_pa3_slg_lt_350"] = pa >= 3 and slg is not None and slg < 0.350


def _bvp_positive(row: dict[str, Any], min_pa: int = 5) -> bool:
    return _pa(row) >= min_pa and (
        (_f(row.get("bvp_avg")) is not None and (_f(row.get("bvp_avg")) or 0) >= 0.250)
        or (_f(row.get("bvp_slg")) is not None and (_f(row.get("bvp_slg")) or 0) >= 0.350)
    )


def _bvp_negative(row: dict[str, Any], min_pa: int = 5) -> bool:
    avg = _f(row.get("bvp_avg"))
    slg = _f(row.get("bvp_slg"))
    return _pa(row) >= min_pa and avg is not None and slg is not None and avg < 0.250 and slg < 0.350


def _coverage_row(name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if _b(row.get("resolved"))]

    def count(group: list[dict[str, Any]], pred: Callable[[dict[str, Any]], bool]) -> int:
        return sum(1 for row in group if pred(row))

    out = {
        "population": name,
        "rows": len(rows),
        "resolved_rows": len(resolved),
        "bvp_payload_present_rows": count(rows, _bvp_payload),
        "bvp_pa_gt_0_rows": count(rows, lambda r: _pa(r) > 0),
        "bvp_pa_gte_3_rows": count(rows, lambda r: _pa(r) >= 3),
        "bvp_pa_gte_5_rows": count(rows, lambda r: _pa(r) >= 5),
        "resolved_bvp_payload_present_rows": count(resolved, _bvp_payload),
        "resolved_bvp_pa_gt_0_rows": count(resolved, lambda r: _pa(r) > 0),
        "resolved_bvp_pa_gte_3_rows": count(resolved, lambda r: _pa(r) >= 3),
        "resolved_bvp_pa_gte_5_rows": count(resolved, lambda r: _pa(r) >= 5),
    }
    for key in list(out):
        if key.endswith("_rows") and key not in {"rows", "resolved_rows"}:
            denom = out["resolved_rows"] if key.startswith("resolved_") else out["rows"]
            out[key.replace("_rows", "_rate")] = (out[key] / denom) if denom else None
    return out


def _price_201_300(row: dict[str, Any]) -> bool:
    price = market._price(row)
    return price is not None and 201 <= price <= 300


def _base_rows(rows: list[dict[str, Any]], *, alternate_only: bool = True) -> list[dict[str, Any]]:
    out = [row for row in rows if _b(row.get("from_alternate")) and _price_201_300(row)]
    if alternate_only:
        out = [row for row in out if not _b(row.get("from_both"))]
    return out


def _slice_specs() -> list[tuple[str, Callable[[dict[str, Any]], bool], str]]:
    return [
        ("all_201_300", lambda r: True, "baseline +201 to +300"),
        ("exclude_hitter_tier_a", lambda r: str(r.get("hitter_tier") or "") != "A", "not Hitter Tier A"),
        ("d7_lte_1_3", lambda r: (_f(r.get("d7_hits_rate")) is None or (_f(r.get("d7_hits_rate")) or 0) <= 1.3), "not public-hot by d7"),
        ("d7_lte_1_0", lambda r: _f(r.get("d7_hits_rate")) is not None and (_f(r.get("d7_hits_rate")) or 0) <= 1.0, "quiet/low d7"),
        ("d7_hrr_gte_2_0", lambda r: _f(r.get("d7_hits_runs_rbis")) is not None and (_f(r.get("d7_hits_runs_rbis")) or 0) >= 2.0, "some recent offensive involvement"),
        ("d7_hrr_gte_2_5", lambda r: _f(r.get("d7_hits_runs_rbis")) is not None and (_f(r.get("d7_hits_runs_rbis")) or 0) >= 2.5, "stronger recent offensive involvement"),
        ("starter_gte_5_0", lambda r: _f(r.get("starter_expected_hits_allowed")) is not None and (_f(r.get("starter_expected_hits_allowed")) or 0) >= 5.0, "starter support"),
        ("starter_gte_5_5", lambda r: _f(r.get("starter_expected_hits_allowed")) is not None and (_f(r.get("starter_expected_hits_allowed")) or 0) >= 5.5, "strong starter support"),
        ("team_gte_8_0", lambda r: _f(r.get("team_expected_hits_allowed")) is not None and (_f(r.get("team_expected_hits_allowed")) or 0) >= 8.0, "team support"),
        ("team_gte_9_0", lambda r: _f(r.get("team_expected_hits_allowed")) is not None and (_f(r.get("team_expected_hits_allowed")) or 0) >= 9.0, "strong team support"),
        ("pitcher_tier_a_b", lambda r: str(r.get("pitcher_tier") or "") in {"A", "B"}, "pitcher Tier A/B"),
        ("combined_c_a_or_b_a", lambda r: str(r.get("combined_tier") or "") in {"C/A", "B/A"}, "C/A or B/A"),
        ("combined_c_a", lambda r: str(r.get("combined_tier") or "") == "C/A", "C/A only"),
        ("favorable_hidden_profile", lambda r: str(r.get("market_classification_label") or "") == "favorable_pitcher_hidden_profile", "previous favorable hidden label"),
        ("not_overpriced_hot", lambda r: str(r.get("market_classification_label") or "") != "overpriced_hot_profile", "exclude public-hot EV bait"),
        ("not_layer_a", lambda r: str(r.get("alternate_layer") or "") != "alternate_layer_a_d7_d15_starter", "exclude alternate Layer A"),
        ("hidden_core_1", lambda r: str(r.get("hitter_tier") or "") != "A" and str(r.get("pitcher_tier") or "") in {"A", "B"}, "non-A hitter plus pitcher A/B"),
        ("hidden_core_2", lambda r: str(r.get("hitter_tier") or "") != "A" and (_f(r.get("d7_hits_rate")) is None or (_f(r.get("d7_hits_rate")) or 0) <= 1.3) and str(r.get("pitcher_tier") or "") in {"A", "B"}, "non-hot plus pitcher A/B"),
        ("hidden_core_3", lambda r: str(r.get("hitter_tier") or "") != "A" and (_f(r.get("d7_hits_rate")) is not None and (_f(r.get("d7_hits_rate")) or 0) <= 1.0) and str(r.get("pitcher_tier") or "") in {"A", "B"}, "quiet d7 plus pitcher A/B"),
        ("team_hidden_1", lambda r: str(r.get("hitter_tier") or "") != "A" and (_f(r.get("team_expected_hits_allowed")) is not None and (_f(r.get("team_expected_hits_allowed")) or 0) >= 8.0), "non-A hitter plus team >=8"),
        ("team_hidden_2", lambda r: str(r.get("hitter_tier") or "") != "A" and (_f(r.get("team_expected_hits_allowed")) is not None and (_f(r.get("team_expected_hits_allowed")) or 0) >= 9.0), "non-A hitter plus team >=9"),
        ("quiet_team_hidden", lambda r: str(r.get("hitter_tier") or "") != "A" and (_f(r.get("d7_hits_rate")) is not None and (_f(r.get("d7_hits_rate")) or 0) <= 1.0) and (_f(r.get("team_expected_hits_allowed")) is not None and (_f(r.get("team_expected_hits_allowed")) or 0) >= 8.0), "quiet d7 plus team >=8"),
        ("cluster_hidden", lambda r: str(r.get("hitter_tier") or "") != "A" and (_f(r.get("same_game_teammate_tier_a_count")) or 0) > 0, "non-A with same-game Tier A teammate"),
    ]


def _build_slices(base: list[dict[str, Any]], source_total: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = [
        _metric_row("baseline", "alternate_source_total_201_300", source_total, note="includes overlap/from_both"),
        _metric_row("baseline", "alternate_only_201_300", base, note="primary research population"),
    ]
    specs = _slice_specs()
    selected: dict[str, list[dict[str, Any]]] = {}
    for name, predicate, note in specs:
        group = [row for row in base if predicate(row)]
        selected[name] = group
        rows.append(_metric_row("definition", name, group, note=note))
    combinations = [
        ("not_hot_pitcher_ab", ["exclude_hitter_tier_a", "d7_lte_1_3", "pitcher_tier_a_b"]),
        ("not_hot_team_8", ["exclude_hitter_tier_a", "d7_lte_1_3", "team_gte_8_0"]),
        ("quiet_pitcher_ab", ["d7_lte_1_0", "pitcher_tier_a_b"]),
        ("quiet_team_8", ["d7_lte_1_0", "team_gte_8_0"]),
        ("quiet_pitcher_ab_team_8", ["d7_lte_1_0", "pitcher_tier_a_b", "team_gte_8_0"]),
        ("not_hot_pitcher_ab_not_layer_a", ["exclude_hitter_tier_a", "d7_lte_1_3", "pitcher_tier_a_b", "not_layer_a"]),
        ("not_hot_team_8_not_layer_a", ["exclude_hitter_tier_a", "d7_lte_1_3", "team_gte_8_0", "not_layer_a"]),
        ("not_hot_team_9", ["exclude_hitter_tier_a", "d7_lte_1_3", "team_gte_9_0"]),
        ("not_overpriced_plus_team_8", ["not_overpriced_hot", "team_gte_8_0"]),
        ("not_overpriced_plus_pitcher_ab", ["not_overpriced_hot", "pitcher_tier_a_b"]),
        ("hrr_2_0_team_8", ["d7_hrr_gte_2_0", "team_gte_8_0", "not_overpriced_hot"]),
        ("hrr_2_5_team_8", ["d7_hrr_gte_2_5", "team_gte_8_0", "not_overpriced_hot"]),
    ]
    predicates = {name: pred for name, pred, _note in specs}
    for name, parts in combinations:
        group = [row for row in base if all(predicates[part](row) for part in parts)]
        rows.append(_metric_row("combination", name, group, note=" + ".join(parts)))
    return rows


def _veto_specs() -> list[tuple[str, Callable[[dict[str, Any]], bool], str]]:
    return [
        ("remove_hitter_tier_a", lambda r: str(r.get("hitter_tier") or "") == "A", "Hitter Tier A"),
        ("remove_d7_gt_1_3", lambda r: _f(r.get("d7_hits_rate")) is not None and (_f(r.get("d7_hits_rate")) or 0) > 1.3, "d7 > 1.3"),
        ("remove_layer_a", lambda r: str(r.get("alternate_layer") or "") == "alternate_layer_a_d7_d15_starter", "Alternate Layer A"),
        ("remove_price_201_220", lambda r: (_f(r.get("best_available_over_price")) or 999) <= 220, "shorter +201 to +220"),
        ("remove_team_lt_8", lambda r: _f(r.get("team_expected_hits_allowed")) is None or (_f(r.get("team_expected_hits_allowed")) or 0) < 8.0, "team expected <8 or missing"),
        ("remove_team_lt_9", lambda r: _f(r.get("team_expected_hits_allowed")) is None or (_f(r.get("team_expected_hits_allowed")) or 0) < 9.0, "team expected <9 or missing"),
        ("remove_starter_lt_5", lambda r: _f(r.get("starter_expected_hits_allowed")) is None or (_f(r.get("starter_expected_hits_allowed")) or 0) < 5.0, "starter expected <5 or missing"),
        ("remove_d7_hrr_lt_2", lambda r: _f(r.get("d7_hits_runs_rbis")) is None or (_f(r.get("d7_hits_runs_rbis")) or 0) < 2.0, "d7 HRR <2 or missing"),
        ("remove_d7_hrr_lt_2_5", lambda r: _f(r.get("d7_hits_runs_rbis")) is None or (_f(r.get("d7_hits_runs_rbis")) or 0) < 2.5, "d7 HRR <2.5 or missing"),
        ("remove_overpriced_hot_profile", lambda r: str(r.get("market_classification_label") or "") == "overpriced_hot_profile", "overpriced hot profile"),
    ]


def _build_vetos(base: list[dict[str, Any]]) -> list[dict[str, Any]]:
    baseline = _metric_triplet(base)
    out: list[dict[str, Any]] = []
    for name, predicate, note in _veto_specs():
        removed = [row for row in base if predicate(row)]
        retained = [row for row in base if not predicate(row)]
        removed_row = _metric_row("removed", name, removed, note=note)
        retained_row = _metric_row("retained", name, retained, note=note)
        retained_row["roi_lift_best_vs_baseline"] = (
            (_f(retained_row.get("roi_best_price")) or 0) - (_f(baseline.get("roi_best_price")) or 0)
            if retained_row.get("roi_best_price") is not None and baseline.get("roi_best_price") is not None
            else None
        )
        retained_row["roi_lift_betonline_vs_baseline"] = (
            (_f(retained_row.get("roi_betonline_price")) or 0) - (_f(baseline.get("roi_betonline_price")) or 0)
            if retained_row.get("roi_betonline_price") is not None and baseline.get("roi_betonline_price") is not None
            else None
        )
        out.extend([removed_row, retained_row])
    return out


def _bvp_population_specs() -> list[tuple[str, Callable[[dict[str, Any]], bool], str]]:
    return [
        ("alternate_only_201_300", lambda r: True, "baseline +201 to +300 alternate-only"),
        (
            "alternate_only_201_300_d7_lte_1_0",
            lambda r: _f(r.get("d7_hits_rate")) is not None and (_f(r.get("d7_hits_rate")) or 0) <= 1.0,
            "quiet d7 slice",
        ),
        (
            "alternate_only_201_300_team_gte_9",
            lambda r: _f(r.get("team_expected_hits_allowed")) is not None
            and (_f(r.get("team_expected_hits_allowed")) or 0) >= 9.0,
            "strong team expected support",
        ),
        (
            "favorable_pitcher_hidden_profile",
            lambda r: str(r.get("market_classification_label") or "") == "favorable_pitcher_hidden_profile",
            "prior market-classification hidden profile",
        ),
        ("combined_c_a", lambda r: str(r.get("combined_tier") or "") == "C/A", "C/A profile"),
        ("non_tier_a", lambda r: str(r.get("hitter_tier") or "") != "A", "non-Hitter-Tier-A"),
    ]


def _bvp_test_rows(base: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for population, predicate, note in _bvp_population_specs():
        rows = [row for row in base if predicate(row)]
        tests: list[tuple[str, str, list[dict[str, Any]]]] = [
            ("all_rows", "all rows in population", rows),
            ("bvp_positive_pa5", "PA>=5 and AVG>=.250 or SLG>=.350", [row for row in rows if _bvp_positive(row, 5)]),
            ("bvp_negative_pa5", "PA>=5 and AVG<.250 or SLG<.350", [row for row in rows if _bvp_negative(row, 5)]),
            ("bvp_low_or_no_sample_pa5", "PA<5 or no BvP sample", [row for row in rows if not _bvp_positive(row, 5) and not _bvp_negative(row, 5)]),
            (
                "retained_remove_negative_pa5",
                "remove PA>=5 negative BvP",
                [row for row in rows if not _bvp_negative(row, 5)],
            ),
            (
                "retained_positive_or_neutral_pa5",
                "require positive BvP or low/no sample",
                [row for row in rows if _bvp_positive(row, 5) or not _bvp_negative(row, 5)],
            ),
            ("bvp_positive_pa3", "PA>=3 and AVG>=.250 or SLG>=.350", [row for row in rows if _bvp_positive(row, 3)]),
            ("bvp_negative_pa3", "PA>=3 and AVG<.250 or SLG<.350", [row for row in rows if _bvp_negative(row, 3)]),
            (
                "retained_remove_negative_pa3",
                "PA>=3 sensitivity: remove negative BvP",
                [row for row in rows if not _bvp_negative(row, 3)],
            ),
        ]
        for test_name, test_note, test_group in tests:
            row = {
                "population": population,
                "test": test_name,
                "population_note": note,
                "test_note": test_note,
                "bvp_payload_coverage": (
                    sum(1 for item in rows if _bvp_payload(item)) / len(rows) if rows else None
                ),
                "bvp_pa5_coverage": sum(1 for item in rows if _pa(item) >= 5) / len(rows) if rows else None,
                "bvp_pa3_coverage": sum(1 for item in rows if _pa(item) >= 3) / len(rows) if rows else None,
                "avg_bvp_pa": _avg([item.get("bvp_plate_appearances") for item in test_group]),
                "avg_bvp_avg": _avg([item.get("bvp_avg") for item in test_group]),
                "avg_bvp_slg": _avg([item.get("bvp_slg") for item in test_group]),
            }
            row.update(_metric_triplet(test_group))
            out.append(row)
    return out


def _summary_rows(base: list[dict[str, Any]], source_total: list[dict[str, Any]], slices: list[dict[str, Any]], vetos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best20 = sorted(
        [row for row in slices if int(row.get("resolved") or 0) >= 20],
        key=lambda row: _f(row.get("roi_best_price")) if _f(row.get("roi_best_price")) is not None else -999,
        reverse=True,
    )
    best40 = sorted(
        [row for row in slices if int(row.get("resolved") or 0) >= 40],
        key=lambda row: _f(row.get("roi_best_price")) if _f(row.get("roi_best_price")) is not None else -999,
        reverse=True,
    )
    best60 = sorted(
        [row for row in slices if int(row.get("resolved") or 0) >= 60],
        key=lambda row: _f(row.get("roi_best_price")) if _f(row.get("roi_best_price")) is not None else -999,
        reverse=True,
    )
    veto_best = sorted(
        [row for row in vetos if row.get("row_type") == "retained"],
        key=lambda row: _f(row.get("roi_lift_best_vs_baseline")) if _f(row.get("roi_lift_best_vs_baseline")) is not None else -999,
        reverse=True,
    )
    out = [
        {"summary_type": "baseline", "name": "alternate_source_total_201_300", **_metric_triplet(source_total)},
        {"summary_type": "baseline", "name": "alternate_only_201_300", **_metric_triplet(base)},
    ]
    for min_resolved, rows in ((20, best20[:10]), (40, best40[:10]), (60, best60[:10])):
        for rank, row in enumerate(rows, start=1):
            out.append({"summary_type": f"best_slice_min_{min_resolved}", "rank": rank, **row})
    for rank, row in enumerate(veto_best[:10], start=1):
        out.append({"summary_type": "best_veto_lift", "rank": rank, **row})
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
        "alternate_layer",
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
        "best_available_over_price",
        "betonline_over_price",
        "median_available_over_price",
        "resolved",
        "win",
        "loss",
        "push",
        "actual_value",
        "bvp_plate_appearances",
        "bvp_at_bats",
        "bvp_hits",
        "bvp_total_bases",
        "bvp_avg",
        "bvp_slg",
        "bvp_payload_present",
        "bvp_source",
        "bvp_join_mode",
        "bvp_source_path",
        "bvp_source_date",
        "bvp_none_or_low_sample",
        "bvp_positive_pa5_avg_250",
        "bvp_negative_pa5_avg_lt_250",
        "bvp_positive_pa5_slg_350",
        "bvp_negative_pa5_slg_lt_350",
        "bvp_strong_positive_pa5_avg_300_or_slg_500",
        "bvp_strong_negative_pa5_avg_200_and_slg_300",
        "bvp_positive_pa3_avg_250",
        "bvp_negative_pa3_avg_lt_250",
        "bvp_positive_pa3_slg_350",
        "bvp_negative_pa3_slg_lt_350",
    ]
    return [{field: row.get(field) for field in fields} for row in rows]


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100:.2f}%"


def _fmt_num(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.2f}"


def _line(row: dict[str, Any]) -> str:
    return (
        f"| {row.get('name')} | {row.get('candidates')} | {row.get('resolved')} | "
        f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | "
        f"{_fmt_pct(row.get('roi_best_price'))} | {_fmt_pct(row.get('roi_betonline_price'))} | {_fmt_pct(row.get('roi_median_price'))} | "
        f"{_fmt_num(row.get('avg_best_price'))} | {_fmt_num(row.get('avg_d7_hits_rate'))} | {_fmt_num(row.get('avg_starter_expected_hits_allowed'))} | {_fmt_num(row.get('avg_team_expected_hits_allowed'))} |"
    )


def _write_report(
    path: Path,
    slices: list[dict[str, Any]],
    vetos: list[dict[str, Any]],
    summary: list[dict[str, Any]],
    coverage: list[dict[str, Any]],
    bvp_tests: list[dict[str, Any]],
) -> None:
    baselines = [row for row in summary if row.get("summary_type") == "baseline"]
    best20 = [row for row in summary if row.get("summary_type") == "best_slice_min_20"][:8]
    best40 = [row for row in summary if row.get("summary_type") == "best_slice_min_40"][:8]
    best_vetos = [row for row in summary if row.get("summary_type") == "best_veto_lift"][:8]
    fav = next((row for row in slices if row.get("name") == "favorable_hidden_profile"), {})
    hidden = next((row for row in slices if row.get("name") == "hidden_core_3"), {})
    team = next((row for row in slices if row.get("name") == "team_hidden_1"), {})
    veto = next((row for row in best_vetos), {})
    bvp_cov = next((row for row in coverage if row.get("population") == "alternate_only_201_300"), {})
    bvp_display = [
        row
        for row in bvp_tests
        if row.get("population") in {"alternate_only_201_300", "alternate_only_201_300_team_gte_9"}
        and row.get("test")
        in {"all_rows", "bvp_positive_pa5", "bvp_negative_pa5", "bvp_low_or_no_sample_pa5", "retained_remove_negative_pa5"}
    ]

    def bvp_line(row: dict[str, Any]) -> str:
        return (
            f"| {row.get('population')} | {row.get('test')} | {row.get('candidates')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi_best_price'))} | "
            f"{_fmt_pct(row.get('roi_betonline_price'))} | {_fmt_pct(row.get('roi_median_price'))} | "
            f"{_fmt_num(row.get('avg_bvp_pa'))} | {_fmt_num(row.get('avg_bvp_avg'))} | {_fmt_num(row.get('avg_bvp_slg'))} |"
        )

    lines = [
        "# Expanded O1.5 Hidden Matchup Support Audit",
        "",
        "Scope: Expanded O1.5 alternate source, price +201 to +300. Primary population is alternate-only; alternate-source total is reported for comparison.",
        "",
        "## BvP Source",
        "",
        "- Expanded rows hydrate compact BvP from same-date `backend/mlb/exports/odds_history/<date>/mlb_slate_output.csv`.",
        "- Join priority: `date + player_id + team + opponent`, then `date + player_id`, then normalized-name fallbacks.",
        "- No future-date BvP fallback is used.",
        "- The slate BvP columns originate from production slate output, which carries compact `prop_features_precomputed` BvP payloads.",
        "",
        "## Baselines",
        "",
        "| name | candidates | resolved | W-L-P | ROI best | ROI BOL | ROI median | avg best | avg d7 | avg starter | avg team |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in baselines:
        lines.append(_line(row))
    lines.extend(
        [
            "",
            "## Best Retained Slices: Min 20 Resolved",
            "",
            "| name | candidates | resolved | W-L-P | ROI best | ROI BOL | ROI median | avg best | avg d7 | avg starter | avg team |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in best20:
        lines.append(_line(row))
    lines.extend(
        [
            "",
            "## Best Retained Slices: Min 40 Resolved",
            "",
            "| name | candidates | resolved | W-L-P | ROI best | ROI BOL | ROI median | avg best | avg d7 | avg starter | avg team |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in best40:
        lines.append(_line(row))
    lines.extend(
        [
            "",
            "## Veto Lift",
            "",
            "| veto | candidates retained | resolved | W-L-P | ROI best | ROI BOL | ROI lift best | ROI lift BOL |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in best_vetos:
        lines.append(
            f"| {row.get('name')} | {row.get('candidates')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi_best_price'))} | "
            f"{_fmt_pct(row.get('roi_betonline_price'))} | {_fmt_pct(row.get('roi_lift_best_vs_baseline'))} | {_fmt_pct(row.get('roi_lift_betonline_vs_baseline'))} |"
        )
    lines.extend(
        [
            "",
            "## Readout",
            "",
            f"- Previous `favorable_pitcher_hidden_profile`: `{fav.get('wins', 0)}-{fav.get('losses', 0)}` at `{_fmt_pct(fav.get('roi_best_price'))}` best-price ROI and `{_fmt_pct(fav.get('roi_betonline_price'))}` BetOnline ROI.",
            f"- Strict quiet d7 + pitcher A/B (`hidden_core_3`): `{hidden.get('wins', 0)}-{hidden.get('losses', 0)}` at `{_fmt_pct(hidden.get('roi_best_price'))}` best-price ROI.",
            f"- Non-A hitter + team expected >=8 (`team_hidden_1`): `{team.get('wins', 0)}-{team.get('losses', 0)}` at `{_fmt_pct(team.get('roi_best_price'))}` best-price ROI.",
            f"- Best veto by retained best-price lift: `{veto.get('name', 'n/a')}`.",
            f"- BvP coverage in alternate-only +201 to +300: `{bvp_cov.get('bvp_payload_present_rows', 0)}/{bvp_cov.get('rows', 0)}` payload rows; `{bvp_cov.get('bvp_pa_gte_5_rows', 0)}` rows with PA>=5.",
            "",
            "## BvP Confirm / Veto Tests",
            "",
            "Default BvP tests use PA>=5. PA>=3 sensitivity rows are in `expanded_o15_bvp_hidden_support_slices.csv`.",
            "",
            "| population | test | candidates | resolved | W-L-P | ROI best | ROI BOL | ROI median | avg BvP PA | avg BvP AVG | avg BvP SLG |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in bvp_display:
        lines.append(bvp_line(row))
    lines.extend(
        [
            "",
            "## Answer",
            "",
            "Favorable hidden support remains directionally real, but BvP is coverage-limited. Positive BvP is the cleaner confirmation signal so far. Negative BvP is not a reliable veto yet because several negative-BvP samples remain small and context-dependent, especially inside strong team-support slices.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit hidden matchup support inside Expanded O1.5 +201 to +300.")
    ap.add_argument("--rows-csv", default=str(DEFAULT_ROWS_CSV))
    ap.add_argument("--backfill-root", default=str(DEFAULT_BACKFILL_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    rows = _read_csv(Path(args.rows_csv))
    market.bol_audit._enrich(rows, Path(args.backfill_root))
    market._annotate(rows)
    _annotate_bvp(rows)
    base = _base_rows(rows, alternate_only=True)
    source_total = _base_rows(rows, alternate_only=False)
    slices = _build_slices(base, source_total)
    vetos = _build_vetos(base)
    summary = _summary_rows(base, source_total, slices, vetos)
    coverage = [
        _coverage_row("expanded_total", rows),
        _coverage_row("resolved_expanded_total", [row for row in rows if _b(row.get("resolved"))]),
        _coverage_row("alternate_only_201_300", base),
        _coverage_row(
            "alternate_only_201_300_d7_lte_1_0",
            [row for row in base if _f(row.get("d7_hits_rate")) is not None and (_f(row.get("d7_hits_rate")) or 0) <= 1.0],
        ),
        _coverage_row(
            "alternate_only_201_300_team_gte_9",
            [
                row
                for row in base
                if _f(row.get("team_expected_hits_allowed")) is not None
                and (_f(row.get("team_expected_hits_allowed")) or 0) >= 9.0
            ],
        ),
    ]
    bvp_tests = _bvp_test_rows(base)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "expanded_o15_hidden_matchup_support_slices.csv", slices)
    _write_csv(out_dir / "expanded_o15_hidden_matchup_support_vetos.csv", vetos)
    _write_csv(out_dir / "expanded_o15_hidden_matchup_support_rows.csv", _row_output(base))
    _write_csv(out_dir / "expanded_o15_hidden_matchup_support_summary.csv", summary)
    _write_csv(out_dir / "expanded_o15_bvp_coverage.csv", coverage)
    _write_csv(out_dir / "expanded_o15_bvp_hidden_support_slices.csv", bvp_tests)
    _write_csv(out_dir / "expanded_o15_bvp_rows.csv", _row_output(base))
    _write_report(out_dir / "expanded_o15_hidden_matchup_support_audit.md", slices, vetos, summary, coverage, bvp_tests)
    _write_report(out_dir / "expanded_o15_bvp_integration_audit.md", slices, vetos, summary, coverage, bvp_tests)
    print(
        {
            "alternate_only_201_300": len(base),
            "alternate_source_201_300": len(source_total),
            "report": str(out_dir / "expanded_o15_hidden_matchup_support_audit.md"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
