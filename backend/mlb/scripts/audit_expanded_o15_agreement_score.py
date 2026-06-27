#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import audit_expanded_o15_hidden_matchup_support as hidden
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


def _price_201_300(row: dict[str, Any], col: str = "best_available_over_price") -> bool:
    price = _f(row.get(col))
    return price is not None and 201 <= price <= 300


def _alternate_only(row: dict[str, Any]) -> bool:
    return _b(row.get("from_alternate")) and not _b(row.get("from_both"))


def _component_defs() -> list[dict[str, Any]]:
    return [
        {
            "component": "price_201_300",
            "score_family": "best_price",
            "definition": "best_available_over_price between +201 and +300",
            "uses_bvp": False,
        },
        {
            "component": "quiet_hitter",
            "score_family": "baseball",
            "definition": "d7_hits_rate <= 1.0 OR hitter_tier != A",
            "uses_bvp": False,
        },
        {
            "component": "not_public_hot",
            "score_family": "baseball",
            "definition": "d7_hits_rate <= 1.3 AND hitter_tier != A",
            "uses_bvp": False,
        },
        {
            "component": "team_support",
            "score_family": "baseball",
            "definition": "team_expected_hits_allowed >= 9.0",
            "uses_bvp": False,
        },
        {
            "component": "pitcher_support",
            "score_family": "baseball",
            "definition": "starter_expected_hits_allowed >= 5.0 OR pitcher_tier in A/B",
            "uses_bvp": False,
        },
        {
            "component": "combined_tier_support",
            "score_family": "baseball",
            "definition": "combined_tier in C/A, B/A, C/B, B/B",
            "uses_bvp": False,
        },
        {
            "component": "lineup_heat_cluster",
            "score_family": "baseball",
            "definition": "same_game_teammate_tier_a_count > 0",
            "uses_bvp": False,
        },
        {
            "component": "positive_bvp",
            "score_family": "bvp",
            "definition": "BvP PA >= 5 and AVG >= .250 OR SLG >= .350",
            "uses_bvp": True,
        },
        {
            "component": "no_negative_bvp",
            "score_family": "bvp",
            "definition": "NOT (BvP PA >= 5 and AVG < .250 and SLG < .350)",
            "uses_bvp": True,
        },
        {
            "component": "betonline_price_available_201_300",
            "score_family": "betonline",
            "definition": "BetOnline O1.5 price exists and is between +201 and +300",
            "uses_bvp": False,
        },
    ]


def _flag_rows(rows: list[dict[str, Any]]) -> None:
    hidden._annotate_bvp(rows)
    for row in rows:
        d7 = _f(row.get("d7_hits_rate"))
        starter = _f(row.get("starter_expected_hits_allowed"))
        hitter = str(row.get("hitter_tier") or "")
        pitcher = str(row.get("pitcher_tier") or "")
        combined = str(row.get("combined_tier") or "")
        row["flag_price_201_300"] = _price_201_300(row, "best_available_over_price")
        row["flag_quiet_hitter"] = (d7 is not None and d7 <= 1.0) or hitter != "A"
        row["flag_not_public_hot"] = (d7 is None or d7 <= 1.3) and hitter != "A"
        row["flag_team_support"] = (_f(row.get("team_expected_hits_allowed")) or -999) >= 9.0
        row["flag_pitcher_support"] = (starter is not None and starter >= 5.0) or pitcher in {"A", "B"}
        row["flag_combined_tier_support"] = combined in {"C/A", "B/A", "C/B", "B/B"}
        row["flag_positive_bvp"] = hidden._bvp_positive(row, 5)
        row["flag_no_negative_bvp"] = not hidden._bvp_negative(row, 5)
        row["flag_lineup_heat_cluster"] = (_f(row.get("same_game_teammate_tier_a_count")) or 0.0) > 0
        row["flag_betonline_price_available_201_300"] = _price_201_300(row, "betonline_over_price")
        no_bvp = [
            "flag_price_201_300",
            "flag_quiet_hitter",
            "flag_not_public_hot",
            "flag_team_support",
            "flag_pitcher_support",
            "flag_combined_tier_support",
            "flag_lineup_heat_cluster",
        ]
        with_bvp = no_bvp + ["flag_positive_bvp", "flag_no_negative_bvp"]
        bol_no_bvp = [
            "flag_quiet_hitter",
            "flag_not_public_hot",
            "flag_team_support",
            "flag_pitcher_support",
            "flag_combined_tier_support",
            "flag_lineup_heat_cluster",
            "flag_betonline_price_available_201_300",
        ]
        bol_with_bvp = bol_no_bvp + ["flag_positive_bvp", "flag_no_negative_bvp"]
        row["agreement_score_best_price_no_bvp"] = sum(1 for col in no_bvp if _b(row.get(col)))
        row["agreement_score_best_price_context"] = sum(1 for col in with_bvp if _b(row.get(col)))
        row["agreement_score_betonline_no_bvp"] = sum(1 for col in bol_no_bvp if _b(row.get(col)))
        row["agreement_score_betonline_context"] = sum(1 for col in bol_with_bvp if _b(row.get(col)))


def _metric_triplet(rows: list[dict[str, Any]]) -> dict[str, Any]:
    best = market._metrics(rows, "best_available_over_price")
    bol = market._metrics(rows, "betonline_over_price")
    median = market._metrics(rows, "median_available_over_price")
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
        "avg_d7_hits_rate": best["avg_d7_hits_rate"],
        "avg_d15_hits_rate": best["avg_d15_hits_rate"],
        "avg_d7_hits_runs_rbis": best["avg_d7_hits_runs_rbis"],
        "avg_d15_hits_runs_rbis": best["avg_d15_hits_runs_rbis"],
        "avg_starter_expected_hits_allowed": best["avg_starter_expected_hits_allowed"],
        "avg_team_expected_hits_allowed": best["avg_team_expected_hits_allowed"],
        "bvp_payload_coverage": sum(1 for row in rows if hidden._bvp_payload(row)) / len(rows) if rows else None,
        "bvp_pa5_coverage": sum(1 for row in rows if hidden._pa(row) >= 5) / len(rows) if rows else None,
        "avg_bvp_pa": _avg([row.get("bvp_plate_appearances") for row in rows]),
        "avg_bvp_avg": _avg([row.get("bvp_avg") for row in rows]),
        "avg_bvp_slg": _avg([row.get("bvp_slg") for row in rows]),
    }


def _score_bucket(value: Any) -> str:
    score = int(_f(value) or 0)
    if score >= 5:
        return "5+"
    return str(score)


def _score_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    score_cols = [
        "agreement_score_best_price_no_bvp",
        "agreement_score_best_price_context",
        "agreement_score_betonline_no_bvp",
        "agreement_score_betonline_context",
    ]
    out: list[dict[str, Any]] = []
    for population_name, population_rows in (
        ("alternate_only", [row for row in rows if _alternate_only(row)]),
        ("alternate_only_price_201_300", [row for row in rows if _alternate_only(row) and _b(row.get("flag_price_201_300"))]),
    ):
        for score_col in score_cols:
            for bucket in ("0", "1", "2", "3", "4", "5+"):
                group = [row for row in population_rows if _score_bucket(row.get(score_col)) == bucket]
                item = {"population": population_name, "score_name": score_col, "score_bucket": bucket}
                item.update(_metric_triplet(group))
                out.append(item)
    return out


def _funnel_specs() -> list[tuple[str, Callable[[dict[str, Any]], bool], str]]:
    return [
        ("A_price_201_300_only", lambda r: _b(r.get("flag_price_201_300")), "price +201 to +300"),
        (
            "B_price_plus_quiet_hitter",
            lambda r: _b(r.get("flag_price_201_300")) and _b(r.get("flag_quiet_hitter")),
            "price + quiet hitter",
        ),
        (
            "C_price_quiet_team_support",
            lambda r: _b(r.get("flag_price_201_300")) and _b(r.get("flag_quiet_hitter")) and _b(r.get("flag_team_support")),
            "price + quiet hitter + team support",
        ),
        (
            "D_price_quiet_team_pitcher_support",
            lambda r: _b(r.get("flag_price_201_300"))
            and _b(r.get("flag_quiet_hitter"))
            and _b(r.get("flag_team_support"))
            and _b(r.get("flag_pitcher_support")),
            "price + quiet hitter + team support + pitcher support",
        ),
        (
            "E_D_plus_positive_bvp",
            lambda r: _b(r.get("flag_price_201_300"))
            and _b(r.get("flag_quiet_hitter"))
            and _b(r.get("flag_team_support"))
            and _b(r.get("flag_pitcher_support"))
            and _b(r.get("flag_positive_bvp")),
            "D + positive BvP",
        ),
        (
            "F_D_plus_no_negative_bvp",
            lambda r: _b(r.get("flag_price_201_300"))
            and _b(r.get("flag_quiet_hitter"))
            and _b(r.get("flag_team_support"))
            and _b(r.get("flag_pitcher_support"))
            and _b(r.get("flag_no_negative_bvp")),
            "D + no negative BvP",
        ),
        (
            "G_D_exclude_public_hot",
            lambda r: _b(r.get("flag_price_201_300"))
            and _b(r.get("flag_quiet_hitter"))
            and _b(r.get("flag_team_support"))
            and _b(r.get("flag_pitcher_support"))
            and _b(r.get("flag_not_public_hot")),
            "D + exclude public-hot",
        ),
        (
            "H_betonline_D_survivor",
            lambda r: _b(r.get("flag_betonline_price_available_201_300"))
            and _b(r.get("flag_quiet_hitter"))
            and _b(r.get("flag_team_support"))
            and _b(r.get("flag_pitcher_support"))
            and _b(r.get("flag_not_public_hot")),
            "BetOnline +201 to +300 + quiet/team/pitcher/not-public-hot",
        ),
    ]


def _funnel_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base = [row for row in rows if _alternate_only(row)]
    out: list[dict[str, Any]] = []
    for name, predicate, note in _funnel_specs():
        group = [row for row in base if predicate(row)]
        item = {"funnel": name, "definition": note}
        item.update(_metric_triplet(group))
        out.append(item)
    return out


def _component_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base = [row for row in rows if _alternate_only(row)]
    out: list[dict[str, Any]] = []
    for comp in _component_defs():
        col = f"flag_{comp['component']}"
        yes = [row for row in base if _b(row.get(col))]
        no = [row for row in base if not _b(row.get(col))]
        for value, group in (("true", yes), ("false", no)):
            item = {**comp, "component_value": value}
            item.update(_metric_triplet(group))
            out.append(item)
    return out


def _row_output(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "date",
        "player_name",
        "player_id",
        "team",
        "opponent",
        "source_bucket",
        "classification_price_bucket",
        "market_classification_label",
        "best_available_over_price",
        "betonline_over_price",
        "median_available_over_price",
        "d7_hits_rate",
        "d15_hits_rate",
        "d7_hits_runs_rbis",
        "d15_hits_runs_rbis",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "hitter_tier",
        "pitcher_tier",
        "combined_tier",
        "same_game_teammate_tier_a_count",
        "bvp_plate_appearances",
        "bvp_avg",
        "bvp_slg",
        "resolved",
        "win",
        "loss",
        "push",
        "actual_value",
    ]
    fields += [f"flag_{comp['component']}" for comp in _component_defs()]
    fields += [
        "agreement_score_best_price_no_bvp",
        "agreement_score_best_price_context",
        "agreement_score_betonline_no_bvp",
        "agreement_score_betonline_context",
    ]
    return [{field: row.get(field) for field in fields} for row in rows]


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100:.2f}%"


def _fmt_num(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.2f}"


def _line(row: dict[str, Any], name_key: str = "score_bucket") -> str:
    return (
        f"| {row.get(name_key)} | {row.get('candidates')} | {row.get('resolved')} | "
        f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | "
        f"{_fmt_pct(row.get('roi_best_price'))} | {_fmt_pct(row.get('roi_betonline_price'))} | "
        f"{_fmt_pct(row.get('roi_median_price'))} | {_fmt_num(row.get('avg_best_price'))} | "
        f"{_fmt_num(row.get('avg_team_expected_hits_allowed'))} | {_fmt_pct(row.get('bvp_pa5_coverage'))} |"
    )


def _write_report(path: Path, summary: list[dict[str, Any]], funnels: list[dict[str, Any]], components: list[dict[str, Any]]) -> None:
    score_focus = [
        row
        for row in summary
        if row.get("population") == "alternate_only_price_201_300"
        and row.get("score_name") in {"agreement_score_best_price_no_bvp", "agreement_score_best_price_context"}
    ]
    funnel_rank_20 = sorted(
        [row for row in funnels if int(row.get("resolved") or 0) >= 20],
        key=lambda r: _f(r.get("roi_best_price")) if _f(r.get("roi_best_price")) is not None else -999,
        reverse=True,
    )
    funnel_rank_40 = sorted(
        [row for row in funnels if int(row.get("resolved") or 0) >= 40],
        key=lambda r: _f(r.get("roi_best_price")) if _f(r.get("roi_best_price")) is not None else -999,
        reverse=True,
    )
    pos_bvp = next((row for row in components if row.get("component") == "positive_bvp" and row.get("component_value") == "true"), {})
    no_neg = next((row for row in components if row.get("component") == "no_negative_bvp" and row.get("component_value") == "true"), {})
    lines = [
        "# Expanded O1.5 Agreement Score Audit",
        "",
        "Scope: Expanded O1.5 Universe, with emphasis on alternate-only rows and the +201 to +300 price pocket.",
        "",
        "## Agreement Scores",
        "",
        "- `agreement_score_best_price_no_bvp`: best-price + baseball evidence, excluding BvP.",
        "- `agreement_score_best_price_context`: best-price + baseball evidence + positive/no-negative BvP flags.",
        "- `agreement_score_betonline_no_bvp`: BetOnline price availability + baseball evidence, excluding BvP.",
        "- `agreement_score_betonline_context`: BetOnline price availability + baseball evidence + BvP flags.",
        "",
        "## Score Buckets: Alternate-Only +201 To +300",
        "",
        "| score | candidates | resolved | W-L-P | ROI best | ROI BOL | ROI median | avg best | avg team exp | BvP PA>=5 cov |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for score_name in ("agreement_score_best_price_no_bvp", "agreement_score_best_price_context"):
        lines.extend(["", f"### {score_name}"])
        for row in [r for r in score_focus if r.get("score_name") == score_name]:
            lines.append(_line(row))
    lines.extend(
        [
            "",
            "## Specific Funnels",
            "",
            "| funnel | candidates | resolved | W-L-P | ROI best | ROI BOL | ROI median | avg best | avg team exp | BvP PA>=5 cov |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in funnels:
        lines.append(_line(row, "funnel"))
    lines.extend(
        [
            "",
            "## Best Funnels",
            "",
            f"- Best with >=20 resolved: `{funnel_rank_20[0].get('funnel') if funnel_rank_20 else 'n/a'}` at `{_fmt_pct(funnel_rank_20[0].get('roi_best_price') if funnel_rank_20 else None)}` best-price ROI.",
            f"- Best with >=40 resolved: `{funnel_rank_40[0].get('funnel') if funnel_rank_40 else 'n/a'}` at `{_fmt_pct(funnel_rank_40[0].get('roi_best_price') if funnel_rank_40 else None)}` best-price ROI.",
            f"- Positive BvP component: `{pos_bvp.get('wins', 0)}-{pos_bvp.get('losses', 0)}` at `{_fmt_pct(pos_bvp.get('roi_best_price'))}` best-price ROI and `{_fmt_pct(pos_bvp.get('roi_betonline_price'))}` BetOnline ROI.",
            f"- No-negative-BvP component: `{no_neg.get('wins', 0)}-{no_neg.get('losses', 0)}` at `{_fmt_pct(no_neg.get('roi_best_price'))}` best-price ROI.",
            "",
            "## Answer",
            "",
            "Agreement count is useful as research context, but this is not ready to become a board rule. The strongest practical read is still the middle-price pocket with quiet/non-public-hot hitter context and team/pitcher support; positive BvP is a confirmation boost when available, while no-negative-BvP is weaker because most rows have no usable BvP sample.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit agreement-score behavior inside Expanded O1.5.")
    ap.add_argument("--rows-csv", default=str(DEFAULT_ROWS_CSV))
    ap.add_argument("--backfill-root", default=str(DEFAULT_BACKFILL_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    rows = _read_csv(Path(args.rows_csv))
    market.bol_audit._enrich(rows, Path(args.backfill_root))
    market._annotate(rows)
    _flag_rows(rows)
    summary = _score_summary(rows)
    funnels = _funnel_rows(rows)
    components = _component_rows(rows)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "expanded_o15_agreement_score_rows.csv", _row_output(rows))
    _write_csv(out_dir / "expanded_o15_agreement_score_summary.csv", summary)
    _write_csv(out_dir / "expanded_o15_agreement_score_funnels.csv", funnels)
    _write_csv(out_dir / "expanded_o15_agreement_score_components.csv", components)
    _write_report(out_dir / "expanded_o15_agreement_score_audit.md", summary, funnels, components)
    print(
        {
            "rows": len(rows),
            "alternate_only_rows": sum(1 for row in rows if _alternate_only(row)),
            "report": str(out_dir / "expanded_o15_agreement_score_audit.md"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
