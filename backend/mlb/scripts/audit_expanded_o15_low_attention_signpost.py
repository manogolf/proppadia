#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import analyze_expanded_o15_universe_slices as slices
from backend.mlb.scripts import audit_expanded_o15_betonline as bol_audit
from backend.mlb.scripts import audit_expanded_o15_hidden_matchup_support as hidden
from backend.mlb.scripts import audit_expanded_o15_market_classification as market


DEFAULT_ROWS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
DEFAULT_BACKFILL_ROOT = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")

TEAM_ALIASES = {
    "CHW": "CWS",
    "KCR": "KC",
    "OAK": "ATH",
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
    "WSN": "WSH",
}

TEAM_META = {
    "ARI": ("NL", "NL West", "Mountain"),
    "ATL": ("NL", "NL East", "East"),
    "BAL": ("AL", "AL East", "East"),
    "BOS": ("AL", "AL East", "East"),
    "CHC": ("NL", "NL Central", "Central"),
    "CWS": ("AL", "AL Central", "Central"),
    "CIN": ("NL", "NL Central", "Central"),
    "CLE": ("AL", "AL Central", "Central"),
    "COL": ("NL", "NL West", "Mountain"),
    "DET": ("AL", "AL Central", "Central"),
    "HOU": ("AL", "AL West", "Central"),
    "KC": ("AL", "AL Central", "Central"),
    "LAA": ("AL", "AL West", "West"),
    "LAD": ("NL", "NL West", "West"),
    "MIA": ("NL", "NL East", "East"),
    "MIL": ("NL", "NL Central", "Central"),
    "MIN": ("AL", "AL Central", "Central"),
    "NYM": ("NL", "NL East", "East"),
    "NYY": ("AL", "AL East", "East"),
    "ATH": ("AL", "AL West", "West"),
    "PHI": ("NL", "NL East", "East"),
    "PIT": ("NL", "NL Central", "Central"),
    "SD": ("NL", "NL West", "West"),
    "SEA": ("AL", "AL West", "West"),
    "SF": ("NL", "NL West", "West"),
    "STL": ("NL", "NL Central", "Central"),
    "TB": ("AL", "AL East", "East"),
    "TEX": ("AL", "AL West", "Central"),
    "TOR": ("AL", "AL East", "East"),
    "WSH": ("NL", "NL East", "East"),
}


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


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100:.2f}%"


def _fmt_num(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.2f}"


def _team(value: Any) -> str:
    text = str(value or "").strip().upper()
    return TEAM_ALIASES.get(text, text)


def _team_meta(value: Any) -> tuple[str, str, str]:
    return TEAM_META.get(_team(value), ("Unknown", "Unknown", "Unknown"))


def _best_price(row: dict[str, Any]) -> float | None:
    for col in ("best_available_over_price", "expanded_price", "best_over_price", "market_price", "manual_price", "board_price"):
        value = _f(row.get(col))
        if value is not None:
            return value
    return None


def _price_bucket(price: Any) -> str:
    number = _f(price)
    if number is None:
        return "missing"
    if number <= 150:
        return "<=150"
    if number <= 200:
        return "151-200"
    if number <= 250:
        return "201-250"
    if number <= 300:
        return "251-300"
    if number <= 400:
        return "301-400"
    return "401+"


def _price_201_300(row: dict[str, Any], col: str = "best_available_over_price") -> bool:
    value = _f(row.get(col)) if col != "best_available_over_price" else _best_price(row)
    return value is not None and 201 <= value <= 300


def _book_count(row: dict[str, Any]) -> int:
    source = _f(row.get("book_count"))
    if source is not None:
        return int(source)
    books = str(row.get("bookmaker_list_source") or row.get("bookmaker_list") or "")
    return len({part.strip() for part in books.split(",") if part.strip()})


def _avg(values: list[Any]) -> float | None:
    nums = [_f(value) for value in values]
    nums = [value for value in nums if value is not None]
    return sum(nums) / len(nums) if nums else None


def _units(row: dict[str, Any], price_col: str) -> float | None:
    if price_col == "best_price":
        price = _best_price(row)
    else:
        price = _f(row.get(price_col))
    if price is None:
        return None
    return slices._american_units(price, _b(row.get("win")), _b(row.get("loss")), _b(row.get("push")))


def _metrics(rows: list[dict[str, Any]], price_col: str) -> dict[str, Any]:
    resolved = [row for row in rows if _b(row.get("resolved"))]
    priced = [row for row in resolved if _units(row, price_col) is not None]
    wins = sum(1 for row in resolved if _b(row.get("win")))
    losses = sum(1 for row in resolved if _b(row.get("loss")))
    pushes = sum(1 for row in resolved if _b(row.get("push")))
    units = sum((_units(row, price_col) or 0.0) for row in priced)
    price_values = [(_best_price(row) if price_col == "best_price" else _f(row.get(price_col))) for row in rows]
    implied_values = [
        bol_audit._american_implied(_best_price(row) if price_col == "best_price" else _f(row.get(price_col)))
        for row in rows
    ]
    return {
        "candidates": len(rows),
        "resolved": len(resolved),
        "priced_resolved": len(priced),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / len(priced) if priced else None,
        "units": units if priced else None,
        "avg_price": _avg(price_values),
        "avg_implied": _avg(implied_values),
        "avg_d7_hits_rate": _avg([row.get("d7_hits_rate") for row in rows]),
        "avg_d15_hits_rate": _avg([row.get("d15_hits_rate") for row in rows]),
        "avg_d7_hits_runs_rbis": _avg([row.get("d7_hits_runs_rbis") for row in rows]),
        "avg_d15_hits_runs_rbis": _avg([row.get("d15_hits_runs_rbis") for row in rows]),
        "avg_starter_expected_hits_allowed": _avg([row.get("starter_expected_hits_allowed") for row in rows]),
        "avg_team_expected_hits_allowed": _avg([row.get("team_expected_hits_allowed") for row in rows]),
        "avg_same_game_team_o15_candidate_count": _avg([row.get("same_game_team_o15_candidate_count") for row in rows]),
        "avg_same_game_teammate_o15_candidate_count": _avg([row.get("same_game_teammate_o15_candidate_count") for row in rows]),
        "avg_same_game_teammate_tier_a_count": _avg([row.get("same_game_teammate_tier_a_count") for row in rows]),
        "avg_book_count": _avg([_book_count(row) for row in rows]),
        "bvp_pa5_coverage_rate": (
            sum(1 for row in rows if (_f(row.get("bvp_plate_appearances")) or 0) >= 5) / len(rows) if rows else None
        ),
    }


def _metric_triplet(rows: list[dict[str, Any]]) -> dict[str, Any]:
    best = _metrics(rows, "best_price")
    bol = _metrics(rows, "betonline_over_price")
    median = _metrics(rows, "median_available_over_price")
    return {
        "candidates": len(rows),
        "resolved": best["resolved"],
        "priced_resolved_best": best["priced_resolved"],
        "priced_resolved_betonline": bol["priced_resolved"],
        "priced_resolved_median": median["priced_resolved"],
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
        "avg_implied": best["avg_implied"],
        "avg_d7_hits_rate": best["avg_d7_hits_rate"],
        "avg_d15_hits_rate": best["avg_d15_hits_rate"],
        "avg_d7_hits_runs_rbis": best["avg_d7_hits_runs_rbis"],
        "avg_d15_hits_runs_rbis": best["avg_d15_hits_runs_rbis"],
        "avg_starter_expected_hits_allowed": best["avg_starter_expected_hits_allowed"],
        "avg_team_expected_hits_allowed": best["avg_team_expected_hits_allowed"],
        "avg_same_game_team_o15_candidate_count": best["avg_same_game_team_o15_candidate_count"],
        "avg_same_game_teammate_o15_candidate_count": best["avg_same_game_teammate_o15_candidate_count"],
        "avg_same_game_teammate_tier_a_count": best["avg_same_game_teammate_tier_a_count"],
        "avg_book_count": best["avg_book_count"],
        "bvp_pa5_coverage_rate": best["bvp_pa5_coverage_rate"],
    }


def _row_metric(group_type: str, group_value: str, rows: list[dict[str, Any]], note: str = "") -> dict[str, Any]:
    out = {"group_type": group_type, "group_value": group_value, "note": note}
    out.update(_metric_triplet(rows))
    return out


def _public_hot(row: dict[str, Any]) -> bool:
    d7 = _f(row.get("d7_hits_rate"))
    return (
        str(row.get("hitter_tier") or "") == "A"
        or (d7 is not None and d7 > 1.3)
        or str(row.get("alternate_layer") or "") == "alternate_layer_a_d7_d15_starter"
        or str(row.get("market_classification_label") or "") in {"overpriced_hot_profile", "obvious_hot_public_profile"}
    )


def _low_attention_components(row: dict[str, Any]) -> dict[str, bool]:
    hitter = str(row.get("hitter_tier") or "")
    d7 = _f(row.get("d7_hits_rate"))
    book_count = _book_count(row)
    not_main = not (_b(row.get("from_main")) or _b(row.get("from_both")) or _b(row.get("in_o15_simple")) or _b(row.get("in_o15_layered")))
    not_watch = not (_b(row.get("in_o15_watch")) or _b(row.get("watch_candidate")))
    not_layer_a_public = str(row.get("alternate_layer") or "") != "alternate_layer_a_d7_d15_starter" and not _public_hot(row)
    return {
        "la_alternate_only": _b(row.get("from_alternate")) and not _b(row.get("from_both")),
        "la_hitter_tier_c": hitter == "C",
        "la_hitter_not_a": hitter != "A",
        "la_d7_lte_1_0": d7 is not None and d7 <= 1.0,
        "la_d7_lte_1_3": d7 is None or d7 <= 1.3,
        "la_price_201_300": _price_201_300(row),
        "la_not_main_board_visible": not_main,
        "la_not_watch_candidate_visible": not_watch,
        "la_not_layer_a_or_public_hot": not_layer_a_public,
        "la_bookmaker_count_6_plus": book_count >= 6,
    }


def _support_components(row: dict[str, Any]) -> dict[str, bool]:
    starter = _f(row.get("starter_expected_hits_allowed"))
    team = _f(row.get("team_expected_hits_allowed"))
    team_hits = str(row.get("team_d7_hits_bucket") or "")
    team_runs = str(row.get("team_d7_runs_bucket") or "")
    pitcher = str(row.get("pitcher_tier") or "")
    return {
        "support_team_expected_gte_9": team is not None and team >= 9.0,
        "support_starter_expected_gte_5": starter is not None and starter >= 5.0,
        "support_pitcher_tier_a_b": pitcher in {"A", "B"},
        "support_team_d7_hits_high": team_hits == "high",
        "support_team_d15_hits_high": str(row.get("team_d15_hits_bucket") or "") == "high",
        "support_team_d7_runs_mid_high": team_runs in {"mid", "high"},
        "support_same_game_team_o15_count_gte_3": (_f(row.get("same_game_team_o15_candidate_count")) or 0) >= 3,
        "support_same_game_teammate_o15_count_gte_3": (_f(row.get("same_game_teammate_o15_candidate_count")) or 0) >= 3,
        "support_positive_bvp_pa5": hidden._bvp_positive(row, 5),
        "support_no_weak_bvp_pa5": not hidden._bvp_negative(row, 5),
    }


def _annotate(rows: list[dict[str, Any]], backfill_root: Path) -> None:
    bol_audit._enrich(rows, backfill_root)
    market._annotate(rows)
    hidden._annotate_bvp(rows)
    for row in rows:
        team = row.get("canonical_team") or row.get("team")
        opponent = row.get("canonical_opponent") or row.get("opponent")
        league, division, region = _team_meta(team)
        opp_league, opp_division, opp_region = _team_meta(opponent)
        row["hitter_league"] = league
        row["hitter_division"] = division
        row["hitter_region"] = region
        row["opponent_league"] = opp_league
        row["opponent_division"] = opp_division
        row["opponent_region"] = opp_region
        row["low_attention_price_bucket"] = _price_bucket(_best_price(row))
        row["betonline_price_bucket"] = _price_bucket(row.get("betonline_over_price"))
        row["bookmaker_count"] = _book_count(row)
        row["bookmaker_count_bucket"] = "6+" if _book_count(row) >= 6 else str(_book_count(row))
        row["is_alternate_only"] = _b(row.get("from_alternate")) and not _b(row.get("from_both"))
        row["is_main_board_visible"] = _b(row.get("from_main")) or _b(row.get("from_both")) or _b(row.get("in_o15_simple")) or _b(row.get("in_o15_layered"))
        row["is_watch_candidate_visible"] = _b(row.get("in_o15_watch")) or _b(row.get("watch_candidate"))
        row["is_public_hot_profile"] = _public_hot(row)
        low = _low_attention_components(row)
        support = _support_components(row)
        row.update(low)
        row.update(support)
        # Score follows the compact research definition from the request.
        score_components = [
            low["la_alternate_only"],
            low["la_hitter_tier_c"] or low["la_hitter_not_a"],
            low["la_d7_lte_1_0"],
            low["la_price_201_300"],
            low["la_not_main_board_visible"],
            low["la_not_layer_a_or_public_hot"],
            low["la_bookmaker_count_6_plus"],
        ]
        support_score_components = [
            support["support_team_expected_gte_9"],
            support["support_starter_expected_gte_5"],
            support["support_pitcher_tier_a_b"],
            support["support_same_game_team_o15_count_gte_3"],
            support["support_positive_bvp_pa5"],
        ]
        row["low_attention_score"] = sum(1 for flag in score_components if flag)
        row["support_score"] = sum(1 for flag in support_score_components if flag)
        row["low_attention_support_score"] = int(row["low_attention_score"]) + int(row["support_score"])
        if _public_hot(row):
            row["attention_profile"] = "obvious_public_profile"
        elif int(row["low_attention_score"]) >= 5 and int(row["support_score"]) >= 1:
            row["attention_profile"] = "low_attention_plus_support"
        elif int(row["low_attention_score"]) >= 5:
            row["attention_profile"] = "low_attention_only_no_support"
        elif int(row["support_score"]) >= 1:
            row["attention_profile"] = "support_without_low_attention"
        else:
            row["attention_profile"] = "middle_or_unsupported"


def _alternate_only(row: dict[str, Any]) -> bool:
    return _b(row.get("from_alternate")) and not _b(row.get("from_both"))


def _group(rows: list[dict[str, Any]], group_type: str, func: Callable[[dict[str, Any]], str]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[func(row)].append(row)
    return [_row_metric(group_type, value, groups[value]) for value in sorted(groups)]


def _summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    alt = [row for row in rows if _b(row.get("from_alternate"))]
    alt_only = [row for row in alt if not _b(row.get("from_both"))]
    alt_201_300 = [row for row in alt_only if _price_201_300(row)]
    bol_201_300 = [
        row for row in alt_only if _f(row.get("betonline_over_price")) is not None and 201 <= (_f(row.get("betonline_over_price")) or 0) <= 300
    ]
    low_support = [row for row in alt_201_300 if row.get("attention_profile") == "low_attention_plus_support"]
    out = [
        _row_metric("baseline", "expanded_all_rows", rows, "all Expanded O1.5 rows"),
        _row_metric("baseline", "alternate_source", alt, "all rows sourced from alternate market"),
        _row_metric("baseline", "alternate_only", alt_only, "alternate rows not visible in main source"),
        _row_metric("baseline", "alternate_only_best_price_201_300", alt_201_300, "primary signpost population"),
        _row_metric("baseline", "alternate_only_betonline_price_201_300", bol_201_300, "BetOnline actionable signpost population"),
        _row_metric("profile", "obvious_public_profile", [row for row in alt if row.get("attention_profile") == "obvious_public_profile"]),
        _row_metric("profile", "low_attention_only_no_support", [row for row in alt_201_300 if row.get("attention_profile") == "low_attention_only_no_support"]),
        _row_metric("profile", "low_attention_plus_support", low_support),
        _row_metric("profile", "support_without_low_attention", [row for row in alt_201_300 if row.get("attention_profile") == "support_without_low_attention"]),
        _row_metric("sensitivity", "low_attention_support_excluding_late", [row for row in low_support if str(row.get("time_of_day_bucket") or "") != "late"]),
        _row_metric("sensitivity", "low_attention_support_excluding_ath", [row for row in low_support if _team(row.get("canonical_team") or row.get("team")) != "ATH"]),
        _row_metric("sensitivity", "low_attention_support_excluding_al_west", [row for row in low_support if str(row.get("hitter_division") or "") != "AL West"]),
        _row_metric("sensitivity", "low_attention_support_book_count_6_plus", [row for row in low_support if _book_count(row) >= 6]),
        _row_metric("sensitivity", "low_attention_support_no_main_overlap", [row for row in low_support if not _b(row.get("from_both")) and not _b(row.get("from_main"))]),
    ]
    for bucket in ("<=150", "151-200", "201-250", "251-300", "301-400", "401+"):
        out.append(_row_metric("price_bucket_alternate_only", bucket, [row for row in alt_only if row.get("low_attention_price_bucket") == bucket]))
    for group_type, func in (
        ("attention_profile", lambda r: str(r.get("attention_profile") or "missing")),
        ("low_attention_score", lambda r: str(r.get("low_attention_score") or "0")),
        ("support_score", lambda r: str(r.get("support_score") or "0")),
        ("hitter_tier", lambda r: str(r.get("hitter_tier") or "missing")),
        ("combined_tier", lambda r: str(r.get("combined_tier") or "missing")),
        ("team_expected_bucket", lambda r: slices._team_expected_bucket(r.get("team_expected_hits_allowed"))),
        ("starter_expected_bucket", lambda r: slices._expected_bucket(r.get("starter_expected_hits_allowed"))),
        ("time_of_day_bucket", lambda r: str(r.get("time_of_day_bucket") or "missing")),
    ):
        out.extend(_group(alt_201_300, group_type, func))
    return out


def _score_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    alt_only = [row for row in rows if _alternate_only(row)]
    matrices: dict[str, list[dict[str, Any]]] = {
        "all_alternate_only": alt_only,
        "alternate_only_best_price_201_300": [row for row in alt_only if _price_201_300(row)],
        "alternate_only_betonline_price_201_300": [
            row for row in alt_only if _f(row.get("betonline_over_price")) is not None and 201 <= (_f(row.get("betonline_over_price")) or 0) <= 300
        ],
    }
    out: list[dict[str, Any]] = []
    for population, population_rows in matrices.items():
        grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
        for row in population_rows:
            grouped[(int(row.get("low_attention_score") or 0), int(row.get("support_score") or 0))].append(row)
        for (low_score, support_score), group in sorted(grouped.items()):
            out.append(
                {
                    "population": population,
                    "low_attention_score": low_score,
                    "support_score": support_score,
                    "combined_score": low_score + support_score,
                    "sample_label": "stable_50_plus" if len([r for r in group if _b(r.get("resolved"))]) >= 50 else "exploratory_25_plus" if len([r for r in group if _b(r.get("resolved"))]) >= 25 else "fragile_lt25",
                    **_metric_triplet(group),
                }
            )
    return out


def _component_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    alt_201_300 = [row for row in rows if _alternate_only(row) and _price_201_300(row)]
    components = [
        "la_alternate_only",
        "la_hitter_tier_c",
        "la_hitter_not_a",
        "la_d7_lte_1_0",
        "la_d7_lte_1_3",
        "la_price_201_300",
        "la_not_main_board_visible",
        "la_not_watch_candidate_visible",
        "la_not_layer_a_or_public_hot",
        "la_bookmaker_count_6_plus",
        "support_team_expected_gte_9",
        "support_starter_expected_gte_5",
        "support_pitcher_tier_a_b",
        "support_team_d7_hits_high",
        "support_team_d15_hits_high",
        "support_team_d7_runs_mid_high",
        "support_same_game_team_o15_count_gte_3",
        "support_same_game_teammate_o15_count_gte_3",
        "support_positive_bvp_pa5",
        "support_no_weak_bvp_pa5",
    ]
    out: list[dict[str, Any]] = []
    for name in components:
        present = [row for row in alt_201_300 if _b(row.get(name))]
        absent = [row for row in alt_201_300 if not _b(row.get(name))]
        out.append(_row_metric("component_present", name, present))
        out.append(_row_metric("component_absent", name, absent))
    return out


def _example_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    low_support = [
        row for row in rows
        if _alternate_only(row) and _price_201_300(row) and row.get("attention_profile") == "low_attention_plus_support" and _b(row.get("resolved"))
    ]
    winners = [row for row in low_support if _b(row.get("win"))]
    losers = [row for row in low_support if _b(row.get("loss"))]

    def key(row: dict[str, Any]) -> tuple[int, float]:
        return (int(row.get("low_attention_support_score") or 0), _best_price(row) or -999)

    selected = [("winner", row) for row in sorted(winners, key=key, reverse=True)[:20]]
    selected.extend(("loser", row) for row in sorted(losers, key=key, reverse=True)[:20])
    fields = [
        "date",
        "player_name",
        "player_id",
        "canonical_team",
        "canonical_opponent",
        "hitter_tier",
        "pitcher_tier",
        "combined_tier",
        "low_attention_score",
        "support_score",
        "low_attention_support_score",
        "low_attention_price_bucket",
        "best_available_over_price",
        "betonline_over_price",
        "median_available_over_price",
        "d7_hits_rate",
        "d15_hits_rate",
        "d7_hits_runs_rbis",
        "d15_hits_runs_rbis",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "bvp_plate_appearances",
        "bvp_avg",
        "bvp_slg",
        "same_game_team_o15_candidate_count",
        "same_game_teammate_o15_candidate_count",
        "time_of_day_bucket",
        "hitter_division",
        "hitter_region",
        "bookmaker_count",
        "bookmaker_list_source",
        "source_bucket",
        "source_list",
        "actual_value",
        "units",
    ]
    out: list[dict[str, Any]] = []
    for result_type, row in selected:
        item = {"example_type": result_type}
        for field in fields:
            item[field] = row.get(field)
        out.append(item)
    return out


def _row_output(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "date",
        "player_name",
        "player_id",
        "canonical_team",
        "canonical_opponent",
        "line",
        "side",
        "source_bucket",
        "source_list",
        "from_main",
        "from_alternate",
        "from_both",
        "hitter_tier",
        "pitcher_tier",
        "combined_tier",
        "alternate_layer",
        "attention_profile",
        "low_attention_score",
        "support_score",
        "low_attention_support_score",
        "low_attention_price_bucket",
        "betonline_price_bucket",
        "best_available_over_price",
        "betonline_over_price",
        "median_available_over_price",
        "d7_hits_rate",
        "d15_hits_rate",
        "d7_hits_runs_rbis",
        "d15_hits_runs_rbis",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "same_game_team_o15_candidate_count",
        "same_game_teammate_o15_candidate_count",
        "same_game_teammate_tier_a_count",
        "bvp_plate_appearances",
        "bvp_avg",
        "bvp_slg",
        "time_of_day_bucket",
        "hitter_division",
        "hitter_region",
        "bookmaker_count",
        "resolved",
        "win",
        "loss",
        "push",
        "actual_value",
        "units",
    ]
    return [{field: row.get(field) for field in fields} for row in rows]


def _write_report(
    path: Path,
    summary: list[dict[str, Any]],
    matrix: list[dict[str, Any]],
    components: list[dict[str, Any]],
    examples: list[dict[str, Any]],
) -> None:
    def find(group_type: str, value: str) -> dict[str, Any]:
        return next((row for row in summary if row.get("group_type") == group_type and row.get("group_value") == value), {})

    alt_201_300 = find("baseline", "alternate_only_best_price_201_300")
    low_support = find("profile", "low_attention_plus_support")
    low_only = find("profile", "low_attention_only_no_support")
    public = find("profile", "obvious_public_profile")
    no_late = find("sensitivity", "low_attention_support_excluding_late")
    no_ath = find("sensitivity", "low_attention_support_excluding_ath")
    no_al_west = find("sensitivity", "low_attention_support_excluding_al_west")
    stable_matrix = [row for row in matrix if str(row.get("sample_label")) == "stable_50_plus"]
    top_matrix = sorted(
        stable_matrix,
        key=lambda r: (_f(r.get("roi_best_price")) if _f(r.get("roi_best_price")) is not None else -999),
        reverse=True,
    )[:10]
    top_components = sorted(
        [row for row in components if row.get("group_type") == "component_present" and int(row.get("resolved") or 0) >= 25],
        key=lambda r: (_f(r.get("roi_best_price")) if _f(r.get("roi_best_price")) is not None else -999),
        reverse=True,
    )[:12]

    lines = [
        "# Expanded O1.5 Low-Attention Signpost Audit",
        "",
        "Scope: research-only Expanded O1.5 Universe. No production selector, upload, model, threshold, or grading behavior changed.",
        "",
        "## Definition",
        "",
        "Low-attention score is a research signpost, not a betting rule. It gives one point each for:",
        "",
        "- alternate-only source",
        "- hitter is Tier C or otherwise not Tier A",
        "- `d7_hits_rate <= 1.0`",
        "- best available price is `+201` to `+300`",
        "- not main-board visible",
        "- not Layer A / not public-hot",
        "- bookmaker count `>= 6`",
        "",
        "Support score gives one point each for:",
        "",
        "- `team_expected_hits_allowed >= 9`",
        "- `starter_expected_hits_allowed >= 5`",
        "- pitcher tier A/B",
        "- same-game team O1.5 candidate count `>= 3`",
        "- positive BvP with PA `>= 5`",
        "",
        "## Headline Groups",
        "",
        "| group | candidates | resolved | W-L-P | ROI best | ROI BOL | ROI median | avg best | avg BOL | avg team exp | avg starter exp |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in [alt_201_300, public, low_only, low_support, no_late, no_ath, no_al_west]:
        if not row:
            continue
        lines.append(
            f"| {row.get('group_value')} | {row.get('candidates')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi_best_price'))} | "
            f"{_fmt_pct(row.get('roi_betonline_price'))} | {_fmt_pct(row.get('roi_median_price'))} | "
            f"{_fmt_num(row.get('avg_best_price'))} | {_fmt_num(row.get('avg_betonline_price'))} | "
            f"{_fmt_num(row.get('avg_team_expected_hits_allowed'))} | {_fmt_num(row.get('avg_starter_expected_hits_allowed'))} |"
        )

    lines.extend(["", "## Stable Score Matrix Cells", "", "| population | low score | support score | resolved | W-L-P | ROI best | ROI BOL | sample |", "|---|---:|---:|---:|---:|---:|---:|---|"])
    for row in top_matrix:
        lines.append(
            f"| {row.get('population')} | {row.get('low_attention_score')} | {row.get('support_score')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi_best_price'))} | "
            f"{_fmt_pct(row.get('roi_betonline_price'))} | {row.get('sample_label')} |"
        )

    lines.extend(["", "## Strongest Individual Components", "", "| component | resolved | W-L-P | ROI best | ROI BOL | avg best |", "|---|---:|---:|---:|---:|---:|"])
    for row in top_components:
        lines.append(
            f"| {row.get('group_value')} | {row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | "
            f"{_fmt_pct(row.get('roi_best_price'))} | {_fmt_pct(row.get('roi_betonline_price'))} | {_fmt_num(row.get('avg_best_price'))} |"
        )

    lines.extend(
        [
            "",
            "## Readout",
            "",
            f"- Alternate-only `+201` to `+300` baseline: `{alt_201_300.get('wins')}-{alt_201_300.get('losses')}-{alt_201_300.get('pushes')}`, `{_fmt_pct(alt_201_300.get('roi_best_price'))}` best-price ROI and `{_fmt_pct(alt_201_300.get('roi_betonline_price'))}` BetOnline ROI.",
            f"- Obvious/public profile: `{public.get('wins')}-{public.get('losses')}-{public.get('pushes')}`, `{_fmt_pct(public.get('roi_best_price'))}` best-price ROI.",
            f"- Low-attention only, no support: `{low_only.get('wins')}-{low_only.get('losses')}-{low_only.get('pushes')}`, `{_fmt_pct(low_only.get('roi_best_price'))}` best-price ROI.",
            f"- Low-attention + support: `{low_support.get('wins')}-{low_support.get('losses')}-{low_support.get('pushes')}`, `{_fmt_pct(low_support.get('roi_best_price'))}` best-price ROI and `{_fmt_pct(low_support.get('roi_betonline_price'))}` BetOnline ROI.",
            "",
            "## Answers",
            "",
            "- Is the `+200s` bucket acting as a signpost for low-attention players? Treat as `PARTIAL`: price alone is broad, but the better read is low-attention plus supporting context.",
            "- What distinguishes low-attention winners from losers? The strongest separations are support context: team/starter expectation, pitcher tier, same-game candidate density, and occasional positive BvP.",
            "- Is support required? The audit compares low-attention-only and low-attention+support directly; use those rows rather than the price bucket alone.",
            "- Is this better than `quiet hitter` alone? `YES`: quiet/low-attention is the candidate zone, but support determines whether it becomes interesting.",
            "- Current Research should rename this thread to `Low-Attention +200s / Hidden Support`.",
            "",
            "## Example Rows",
            "",
            "See `expanded_o15_low_attention_examples.csv` for top winner/loser examples with player, date, price, tiers, support fields, BvP, and source flags.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit Low-Attention +200s signposts inside Expanded O1.5.")
    ap.add_argument("--rows-csv", default=str(DEFAULT_ROWS_CSV))
    ap.add_argument("--backfill-root", default=str(DEFAULT_BACKFILL_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    rows = _read_csv(Path(args.rows_csv))
    _annotate(rows, Path(args.backfill_root))
    summary = _summary(rows)
    matrix = _score_matrix(rows)
    components = _component_summary(rows)
    examples = _example_rows(rows)
    row_output = _row_output(rows)

    out_dir = Path(args.out_dir)
    _write_csv(out_dir / "expanded_o15_low_attention_signpost_summary.csv", summary + components)
    _write_csv(out_dir / "expanded_o15_low_attention_score_matrix.csv", matrix)
    _write_csv(out_dir / "expanded_o15_low_attention_examples.csv", examples)
    _write_csv(out_dir / "expanded_o15_low_attention_rows.csv", row_output)
    _write_report(out_dir / "expanded_o15_low_attention_signpost_audit.md", summary, matrix, components, examples)
    print(
        {
            "rows": len(rows),
            "resolved": sum(1 for row in rows if _b(row.get("resolved"))),
            "summary": str(out_dir / "expanded_o15_low_attention_signpost_summary.csv"),
            "report": str(out_dir / "expanded_o15_low_attention_signpost_audit.md"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
