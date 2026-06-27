#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable


DEFAULT_ROWS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")

TEAM_META = {
    "ARI": ("NL", "NL West", "Mountain"),
    "ATL": ("NL", "NL East", "East"),
    "BAL": ("AL", "AL East", "East"),
    "BOS": ("AL", "AL East", "East"),
    "CHC": ("NL", "NL Central", "Central"),
    "CWS": ("AL", "AL Central", "Central"),
    "CHW": ("AL", "AL Central", "Central"),
    "CIN": ("NL", "NL Central", "Central"),
    "CLE": ("AL", "AL Central", "Central"),
    "COL": ("NL", "NL West", "Mountain"),
    "DET": ("AL", "AL Central", "Central"),
    "HOU": ("AL", "AL West", "Central"),
    "KC": ("AL", "AL Central", "Central"),
    "KCR": ("AL", "AL Central", "Central"),
    "LAA": ("AL", "AL West", "West"),
    "LAD": ("NL", "NL West", "West"),
    "MIA": ("NL", "NL East", "East"),
    "MIL": ("NL", "NL Central", "Central"),
    "MIN": ("AL", "AL Central", "Central"),
    "NYM": ("NL", "NL East", "East"),
    "NYY": ("AL", "AL East", "East"),
    "OAK": ("AL", "AL West", "West"),
    "ATH": ("AL", "AL West", "West"),
    "PHI": ("NL", "NL East", "East"),
    "PIT": ("NL", "NL Central", "Central"),
    "SD": ("NL", "NL West", "West"),
    "SDP": ("NL", "NL West", "West"),
    "SEA": ("AL", "AL West", "West"),
    "SF": ("NL", "NL West", "West"),
    "SFG": ("NL", "NL West", "West"),
    "STL": ("NL", "NL Central", "Central"),
    "TB": ("AL", "AL East", "East"),
    "TBR": ("AL", "AL East", "East"),
    "TEX": ("AL", "AL West", "Central"),
    "TOR": ("AL", "AL East", "East"),
    "WSH": ("NL", "NL East", "East"),
    "WSN": ("NL", "NL East", "East"),
}

TEAM_ALIASES = {
    "CHW": "CWS",
    "KCR": "KC",
    "OAK": "ATH",
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
    "WSN": "WSH",
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


def _team(value: Any) -> str:
    text = str(value or "").strip().upper()
    return TEAM_ALIASES.get(text, text)


def _meta(team: Any) -> tuple[str, str, str]:
    return TEAM_META.get(_team(team), ("Unknown", "Unknown", "Unknown"))


def _price(row: dict[str, Any]) -> float | None:
    for col in ("expanded_price", "best_over_price", "market_price", "manual_price", "board_price"):
        value = _f(row.get(col))
        if value is not None:
            return value
    return None


def _american_implied(price: Any) -> float | None:
    number = _f(price)
    if number is None or number == 0:
        return None
    if number > 0:
        return 100.0 / (number + 100.0)
    return abs(number) / (abs(number) + 100.0)


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


def _implied_bucket(value: Any, price: Any = None) -> str:
    number = _f(value)
    if number is None:
        number = _american_implied(price)
    if number is None:
        return "missing"
    if number < 0.20:
        return "<20%"
    if number < 0.25:
        return "20-25%"
    if number < 0.30:
        return "25-30%"
    if number < 0.35:
        return "30-35%"
    return ">=35%"


def _book_count(row: dict[str, Any]) -> int:
    books = [x.strip() for x in str(row.get("bookmaker_list") or "").split(",") if x.strip()]
    return len(set(books))


def _book_bucket(row: dict[str, Any]) -> str:
    count = _book_count(row)
    if count <= 1:
        return "1"
    if count <= 3:
        return "2-3"
    if count <= 5:
        return "4-5"
    return "6+"


def _betonline_available(row: dict[str, Any]) -> bool:
    books = str(row.get("bookmaker_list") or "").lower()
    return "betonline" in books or str(row.get("book") or "").lower() == "betonline"


def _betonline_selected(row: dict[str, Any]) -> bool:
    return str(row.get("book") or "").strip().lower() == "betonline"


def _avg(values: list[Any]) -> float | None:
    nums = [_f(v) for v in values]
    nums = [v for v in nums if v is not None]
    return sum(nums) / len(nums) if nums else None


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [r for r in rows if _truthy(r.get("resolved"))]
    wins = [r for r in resolved if _truthy(r.get("win"))]
    losses = [r for r in resolved if _truthy(r.get("loss"))]
    units = [_f(r.get("units")) for r in resolved]
    units = [u for u in units if u is not None]
    prices = [_price(r) for r in resolved]
    prices = [p for p in prices if p is not None]
    implied = [
        _f(r.get("selected_side_implied_probability")) or _american_implied(_price(r))
        for r in resolved
    ]
    implied = [x for x in implied if x is not None]
    return {
        "candidate_rows": len(rows),
        "resolved_rows": len(resolved),
        "wins": len(wins),
        "losses": len(losses),
        "pushes": max(0, len(resolved) - len(wins) - len(losses)),
        "wr": len(wins) / len(resolved) if resolved else "",
        "roi": sum(units) / len(resolved) if resolved else "",
        "units": sum(units) if resolved else "",
        "avg_best_price": sum(prices) / len(prices) if prices else "",
        "avg_implied_probability": sum(implied) / len(implied) if implied else "",
        "avg_book_count": _avg([_book_count(r) for r in resolved]) or "",
        "betonline_available_rows": sum(1 for r in resolved if _betonline_available(r)),
        "betonline_available_rate": sum(1 for r in resolved if _betonline_available(r)) / len(resolved) if resolved else "",
        "betonline_selected_rows": sum(1 for r in resolved if _betonline_selected(r)),
        "avg_starter_expected_hits_allowed": _avg([r.get("starter_expected_hits_allowed") for r in resolved]) or "",
        "avg_team_expected_hits_allowed": _avg([r.get("team_expected_hits_allowed") for r in resolved]) or "",
        "avg_team_d7_runs_per_game": _avg([r.get("team_d7_runs_per_game") for r in resolved]) or "",
        "avg_team_d15_runs_per_game": _avg([r.get("team_d15_runs_per_game") for r in resolved]) or "",
        "bvp_payload_present_rate": sum(1 for r in resolved if _truthy(r.get("bvp_payload_present"))) / len(resolved) if resolved else "",
        "bvp_pa5_rows": sum(1 for r in resolved if (_f(r.get("bvp_plate_appearances")) or 0) >= 5),
    }


def _group(rows: list[dict[str, Any]], key_fn: Callable[[dict[str, Any]], str], label: str) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[key_fn(row) or "Unknown"].append(row)
    out = []
    for key, part in groups.items():
        item = {"group_type": label, "group": key}
        item.update(_metrics(part))
        out.append(item)
    out.sort(key=lambda r: (int(r.get("resolved_rows") or 0), int(r.get("candidate_rows") or 0)), reverse=True)
    return out


def _annotate(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    team = _team(row.get("team") or row.get("canonical_team"))
    opp = _team(row.get("opponent") or row.get("canonical_opponent"))
    home = _team(row.get("home_team_code"))
    away = _team(row.get("away_team_code"))
    league, division, region = _meta(team)
    opp_league, opp_division, opp_region = _meta(opp)
    home_league, home_division, home_region = _meta(home)
    away_league, away_division, away_region = _meta(away)
    price = _price(row)
    out.update(
        {
            "hitter_team": team,
            "opponent_team": opp,
            "hitter_league": league,
            "hitter_division": division,
            "hitter_region": region,
            "opponent_league": opp_league,
            "opponent_division": opp_division,
            "opponent_region": opp_region,
            "home_region": home_region,
            "away_region": away_region,
            "home_division": home_division,
            "away_division": away_division,
            "price_bucket": _price_bucket(price),
            "implied_probability_bucket": _implied_bucket(row.get("selected_side_implied_probability"), price),
            "bookmaker_count": _book_count(row),
            "bookmaker_count_bucket": _book_bucket(row),
            "betonline_available": _betonline_available(row),
            "betonline_selected": _betonline_selected(row),
            "region_game_bucket": "+".join(sorted({home_region, away_region} - {"Unknown"})) or "Unknown",
        }
    )
    return out


def _leave_one(rows: list[dict[str, Any]], key_fn: Callable[[dict[str, Any]], str], sensitivity_type: str) -> list[dict[str, Any]]:
    late = [r for r in rows if str(r.get("time_of_day_bucket") or "").lower() == "late" and _truthy(r.get("resolved"))]
    base = _metrics(late)
    keys = sorted({key_fn(r) for r in late if key_fn(r)})
    out = []
    for key in keys:
        retained = [r for r in late if key_fn(r) != key]
        item = {
            "sensitivity_type": sensitivity_type,
            "excluded": key,
            "baseline_late_resolved": base.get("resolved_rows"),
            "baseline_late_roi": base.get("roi"),
            "baseline_late_units": base.get("units"),
        }
        item.update({f"retained_{k}": v for k, v in _metrics(retained).items()})
        out.append(item)
    out.sort(key=lambda r: _f(r.get("retained_roi")) or -999, reverse=True)
    return out


def _price_book_breakdown(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    late = [r for r in rows if str(r.get("time_of_day_bucket") or "").lower() == "late"]
    breakdowns: list[dict[str, Any]] = []
    for label, fn in [
        ("source_bucket", lambda r: str(r.get("source_bucket") or "missing")),
        ("price_bucket", lambda r: str(r.get("price_bucket") or "missing")),
        ("implied_probability_bucket", lambda r: str(r.get("implied_probability_bucket") or "missing")),
        ("bookmaker_count_bucket", lambda r: str(r.get("bookmaker_count_bucket") or "missing")),
        ("betonline_available", lambda r: str(r.get("betonline_available"))),
        ("hitter_tier", lambda r: str(r.get("hitter_tier") or "missing")),
        ("combined_tier", lambda r: str(r.get("combined_tier") or "missing")),
        ("hitter_region", lambda r: str(r.get("hitter_region") or "missing")),
    ]:
        breakdowns.extend(_group(late, fn, label))
    return breakdowns


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100.0:.2f}%"


def _fmt_num(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.2f}"


def _write_report(path: Path, rows: list[dict[str, Any]], by_team: list[dict[str, Any]], by_region: list[dict[str, Any]], sensitivity: list[dict[str, Any]], price_book: list[dict[str, Any]]) -> None:
    all_metrics = _metrics([r for r in rows if _truthy(r.get("resolved"))])
    late_rows = [r for r in rows if str(r.get("time_of_day_bucket") or "").lower() == "late"]
    late_metrics = _metrics(late_rows)
    non_late_west = [
        r for r in rows
        if str(r.get("time_of_day_bucket") or "").lower() != "late" and str(r.get("hitter_region")) == "West"
    ]
    late_west = [
        r for r in rows
        if str(r.get("time_of_day_bucket") or "").lower() == "late" and str(r.get("hitter_region")) == "West"
    ]
    late_non_west = [
        r for r in rows
        if str(r.get("time_of_day_bucket") or "").lower() == "late" and str(r.get("hitter_region")) != "West"
    ]
    region_summary = {
        "late_west": _metrics(late_west),
        "late_non_west": _metrics(late_non_west),
        "non_late_west": _metrics(non_late_west),
    }
    leave_west = [r for r in sensitivity if r.get("sensitivity_type") == "leave_one_hitter_region" and r.get("excluded") == "West"]
    west_retained = leave_west[0] if leave_west else {}

    lines = [
        "# Expanded O1.5 Late Game Proxy Audit",
        "",
        "Scope: research only; no production selector/upload/model/threshold/grading changes.",
        "",
        "## Region Mapping",
        "",
        "- West: `LAD`, `LAA`, `SD`, `SF`, `SEA`, `OAK/ATH`",
        "- Mountain/West-ish: `ARI`, `COL`",
        "- Central: `CHC`, `CWS/CHW`, `CIN`, `CLE`, `DET`, `HOU`, `KC/KCR`, `MIL`, `MIN`, `PIT`, `STL`, `TEX`",
        "- East: `ATL`, `BAL`, `BOS`, `MIA`, `NYM`, `NYY`, `PHI`, `TB/TBR`, `TOR`, `WSH/WSN`",
        "",
        "## Headline",
        "",
        f"- Expanded resolved baseline: `{all_metrics['wins']}-{all_metrics['losses']}-{all_metrics['pushes']}`, ROI `{_fmt_pct(all_metrics['roi'])}`.",
        f"- Late resolved: `{late_metrics['wins']}-{late_metrics['losses']}-{late_metrics['pushes']}`, ROI `{_fmt_pct(late_metrics['roi'])}`.",
        f"- Late BetOnline availability rate: `{_fmt_pct(late_metrics['betonline_available_rate'])}`.",
        f"- Late average price: `{_fmt_num(late_metrics['avg_best_price'])}`.",
        f"- Late average implied probability: `{_fmt_pct(late_metrics['avg_implied_probability'])}`.",
        "",
        "## West / Non-West Check",
        "",
        "| slice | resolved | record | ROI | avg price | BetOnline available |",
        "|---|---:|---|---:|---:|---:|",
    ]
    for label, met in region_summary.items():
        lines.append(
            f"| `{label}` | {met['resolved_rows']} | {met['wins']}-{met['losses']}-{met['pushes']} | {_fmt_pct(met['roi'])} | {_fmt_num(met['avg_best_price'])} | {_fmt_pct(met['betonline_available_rate'])} |"
        )
    lines.extend(["", "## Leave-West Sensitivity", ""])
    if west_retained:
        lines.append(
            f"- Late excluding hitter-region West: `{west_retained.get('retained_wins')}-{west_retained.get('retained_losses')}-{west_retained.get('retained_pushes')}`, ROI `{_fmt_pct(west_retained.get('retained_roi'))}`."
        )
    else:
        lines.append("- No West-region sensitivity row available.")
    lines.extend(["", "## Top Late Teams By Resolved Rows", "", "| team | resolved | record | ROI | avg price | avg implied |", "|---|---:|---|---:|---:|---:|"])
    for row in [r for r in by_team if r.get("group_type") == "hitter_team"][:12]:
        lines.append(
            f"| `{row['group']}` | {row['resolved_rows']} | {row['wins']}-{row['losses']}-{row['pushes']} | {_fmt_pct(row['roi'])} | {_fmt_num(row['avg_best_price'])} | {_fmt_pct(row['avg_implied_probability'])} |"
        )
    lines.extend(["", "## Region / Division Summary", "", "| group type | group | resolved | record | ROI | avg price |", "|---|---|---:|---|---:|---:|"])
    for row in by_region[:20]:
        lines.append(
            f"| {row['group_type']} | `{row['group']}` | {row['resolved_rows']} | {row['wins']}-{row['losses']}-{row['pushes']} | {_fmt_pct(row['roi'])} | {_fmt_num(row['avg_best_price'])} |"
        )
    lines.extend(["", "## Price / Book Breakdown", "", "| group type | group | resolved | record | ROI | avg price | BetOnline available |", "|---|---|---:|---|---:|---:|---:|"])
    for row in price_book[:30]:
        lines.append(
            f"| {row['group_type']} | `{row['group']}` | {row['resolved_rows']} | {row['wins']}-{row['losses']}-{row['pushes']} | {_fmt_pct(row['roi'])} | {_fmt_num(row['avg_best_price'])} | {_fmt_pct(row['betonline_available_rate'])} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This audit treats `late` as a possible proxy rather than a causal claim. The most important checks are whether late remains negative after removing top West teams/regions and whether the loss is concentrated by price/book/source composition.",
            "",
            "Use the CSV outputs for full team, region, price/book, and leave-one sensitivity detail.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(rows_csv: Path, out_dir: Path) -> dict[str, Any]:
    rows = [_annotate(r) for r in _read_csv(rows_csv)]
    resolved = [r for r in rows if _truthy(r.get("resolved"))]
    late = [r for r in rows if str(r.get("time_of_day_bucket") or "").lower() == "late"]
    by_team: list[dict[str, Any]] = []
    by_team.extend(_group(late, lambda r: str(r.get("hitter_team") or "Unknown"), "hitter_team"))
    by_team.extend(_group(late, lambda r: str(r.get("opponent_team") or "Unknown"), "opponent_team"))
    by_team.extend(_group(late, lambda r: str(r.get("home_team_code") or "Unknown"), "home_team"))
    by_team.extend(_group(late, lambda r: str(r.get("away_team_code") or "Unknown"), "away_team"))

    by_region: list[dict[str, Any]] = []
    for label, fn in [
        ("time_of_day_bucket", lambda r: str(r.get("time_of_day_bucket") or "missing")),
        ("hitter_region", lambda r: str(r.get("hitter_region") or "Unknown")),
        ("hitter_division", lambda r: str(r.get("hitter_division") or "Unknown")),
        ("hitter_league", lambda r: str(r.get("hitter_league") or "Unknown")),
        ("region_game_bucket", lambda r: str(r.get("region_game_bucket") or "Unknown")),
        ("late_hitter_region", lambda r: str(r.get("hitter_region") or "Unknown") if str(r.get("time_of_day_bucket") or "").lower() == "late" else "non_late"),
    ]:
        source = rows if label in {"time_of_day_bucket", "hitter_region", "hitter_division", "hitter_league", "region_game_bucket"} else late
        by_region.extend(_group(source, fn, label))

    sensitivity: list[dict[str, Any]] = []
    sensitivity.extend(_leave_one(rows, lambda r: str(r.get("hitter_team") or ""), "leave_one_hitter_team"))
    sensitivity.extend(_leave_one(rows, lambda r: str(r.get("hitter_division") or ""), "leave_one_hitter_division"))
    sensitivity.extend(_leave_one(rows, lambda r: str(r.get("hitter_region") or ""), "leave_one_hitter_region"))
    sensitivity.extend(_leave_one(rows, lambda r: str(r.get("source_bucket") or ""), "leave_one_source_bucket"))

    price_book = _price_book_breakdown(rows)
    late_rows = [r for r in rows if str(r.get("time_of_day_bucket") or "").lower() == "late"]

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "expanded_o15_late_game_rows.csv", late_rows)
    _write_csv(out_dir / "expanded_o15_late_game_by_team.csv", by_team)
    _write_csv(out_dir / "expanded_o15_late_game_by_region.csv", by_region)
    _write_csv(out_dir / "expanded_o15_late_game_sensitivity.csv", sensitivity)
    _write_csv(out_dir / "expanded_o15_late_game_price_book_breakdown.csv", price_book)
    _write_report(
        out_dir / "expanded_o15_late_game_proxy_audit.md",
        rows,
        by_team,
        by_region,
        sensitivity,
        price_book,
    )
    return {
        "status": "ok",
        "rows": len(rows),
        "resolved": len(resolved),
        "late_rows": len(late_rows),
        "late_resolved": sum(1 for r in late_rows if _truthy(r.get("resolved"))),
        "report": str(out_dir / "expanded_o15_late_game_proxy_audit.md"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit whether Expanded O1.5 late games proxy for West/market-composition effects.")
    parser.add_argument("--rows-csv", type=Path, default=DEFAULT_ROWS_CSV)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    print(run(args.rows_csv, args.out_dir))


if __name__ == "__main__":
    main()
