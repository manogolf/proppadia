#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any


DEFAULT_INTERACTIONS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_pairwise_interactions.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")
MIN_RESOLVED = 50
MIN_RESOLVED_STRICT = 75
MIN_UNIT_GAIN = 5.0


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


def _i(value: Any) -> int:
    number = _f(value)
    return int(number) if number is not None else 0


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100.0:.2f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.{digits}f}"


def _avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _med(values: list[float]) -> float | None:
    return float(median(values)) if values else None


def _component(row: dict[str, Any], suffix: str) -> tuple[str, str]:
    variable = str(row.get(f"variable_{suffix}") or "").strip() or "unknown"
    bucket = str(row.get(f"bucket_{suffix}") or "").strip() or "missing"
    return variable, bucket


def _blank_stats() -> dict[str, Any]:
    return {
        "eligible_interactions": 0,
        "positive_50_appearances": 0,
        "positive_75_appearances": 0,
        "top25_appearances": 0,
        "top50_appearances": 0,
        "positive_betonline_appearances": 0,
        "positive_unit_gain_appearances": 0,
        "negative_50_appearances": 0,
        "partner_families": set(),
        "partner_values": set(),
        "roi_values": [],
        "roi_positive_values": [],
        "roi_negative_values": [],
        "roi_betonline_values": [],
        "resolved_values": [],
        "units_values": [],
        "units_betonline_values": [],
    }


def _add_interaction(
    stats: dict[tuple[str, str], dict[str, Any]],
    key: tuple[str, str],
    partner_key: tuple[str, str],
    row: dict[str, Any],
    flags: dict[str, bool],
) -> None:
    data = stats[key]
    data["eligible_interactions"] += 1
    data["partner_families"].add(partner_key[0])
    data["partner_values"].add(f"{partner_key[0]}={partner_key[1]}")

    roi = _f(row.get("roi"))
    roi_bol = _f(row.get("roi_betonline"))
    units = _f(row.get("units"))
    units_bol = _f(row.get("units_betonline"))
    resolved = _i(row.get("resolved"))

    if roi is not None:
        data["roi_values"].append(roi)
        if roi > 0:
            data["roi_positive_values"].append(roi)
        if roi < 0:
            data["roi_negative_values"].append(roi)
    if roi_bol is not None:
        data["roi_betonline_values"].append(roi_bol)
    if units is not None:
        data["units_values"].append(units)
    if units_bol is not None:
        data["units_betonline_values"].append(units_bol)
    data["resolved_values"].append(resolved)

    for flag_name, enabled in flags.items():
        if enabled:
            data[flag_name] += 1


def _materialize(
    stats: dict[tuple[str, str], dict[str, Any]],
    entity_type: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (family, value), data in stats.items():
        roi_values = list(data["roi_values"])
        roi_bol_values = list(data["roi_betonline_values"])
        units_values = list(data["units_values"])
        resolved_values = list(data["resolved_values"])
        partner_family_count = len(data["partner_families"])
        partner_value_count = len(data["partner_values"])
        positive = int(data["positive_50_appearances"])
        top25 = int(data["top25_appearances"])
        top50 = int(data["top50_appearances"])
        bol = int(data["positive_betonline_appearances"])
        unit_gain = int(data["positive_unit_gain_appearances"])
        negative = int(data["negative_50_appearances"])

        avg_roi = _avg(roi_values)
        median_roi = _med(roi_values)
        worst_roi = min(roi_values) if roi_values else None
        best_roi = max(roi_values) if roi_values else None
        avg_bol = _avg(roi_bol_values)
        median_bol = _med(roi_bol_values)
        avg_units = _avg(units_values)
        total_resolved = sum(resolved_values)

        # This is intentionally simple and auditable: frequency, partner diversity,
        # top-interaction recurrence, and BetOnline survival push a variable up;
        # recurring negative appearances and a bad worst-case pull it down.
        consistency_score = (
            positive
            + (0.5 * int(data["positive_75_appearances"]))
            + (2.0 * top25)
            + top50
            + (1.5 * bol)
            + unit_gain
            + (0.5 * min(partner_family_count, 10))
        )
        if median_roi is not None:
            consistency_score += median_roi * 5.0
        if worst_roi is not None and worst_roi < 0:
            consistency_score += worst_roi * 2.0
        consistency_score -= negative * 0.75

        risk_score = negative + max(0, -float(avg_roi or 0)) * 5.0 + max(0, -float(median_roi or 0)) * 5.0

        label = "neutral"
        if positive >= 3 and partner_family_count >= 3 and (avg_roi or 0) > 0 and (median_roi or 0) > 0:
            label = "building_block"
        if (top25 or top50) and (positive <= 2 or partner_family_count <= 2):
            label = "fragile_hypothesis"
        if data["eligible_interactions"] >= 5 and (avg_roi or 0) < 0 and (median_roi or 0) < 0:
            label = "frequent_but_harmful"

        rows.append(
            {
                "entity_type": entity_type,
                "variable_family": family,
                "variable_value": "" if entity_type == "family" else value,
                "specific_value": f"{family}={value}" if entity_type == "value" else family,
                "eligible_interactions": data["eligible_interactions"],
                "positive_50_appearances": positive,
                "positive_75_appearances": data["positive_75_appearances"],
                "top25_appearances": top25,
                "top50_appearances": top50,
                "positive_betonline_appearances": bol,
                "positive_unit_gain_appearances": unit_gain,
                "negative_50_appearances": negative,
                "partner_family_count": partner_family_count,
                "partner_value_count": partner_value_count,
                "avg_roi": avg_roi,
                "median_roi": median_roi,
                "best_interaction_roi": best_roi,
                "worst_interaction_roi": worst_roi,
                "avg_betonline_roi": avg_bol,
                "median_betonline_roi": median_bol,
                "total_resolved_across_interactions": total_resolved,
                "avg_resolved": _avg(resolved_values),
                "avg_units": avg_units,
                "best_units": max(units_values) if units_values else None,
                "worst_units": min(units_values) if units_values else None,
                "consistency_score": consistency_score,
                "risk_score": risk_score,
                "classification": label,
            }
        )
    rows.sort(key=lambda r: (_f(r.get("consistency_score")) or 0, _f(r.get("median_roi")) or -999), reverse=True)
    return rows


def _top(rows: list[dict[str, Any]], n: int = 10) -> list[dict[str, Any]]:
    return rows[:n]


def _markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    if not rows:
        return ["_No rows._"]
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    for row in rows:
        values: list[str] = []
        for col in columns:
            value = row.get(col, "")
            if col.endswith("roi") or col in {"avg_roi", "median_roi", "worst_interaction_roi", "best_interaction_roi"}:
                values.append(_fmt_pct(value))
            elif col.endswith("score") or col.startswith("avg_") or col.startswith("median_"):
                values.append(_fmt_num(value))
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return lines


def _write_report(
    path: Path,
    source: Path,
    eligible: list[dict[str, Any]],
    positive_50: list[dict[str, Any]],
    positive_75: list[dict[str, Any]],
    top25: list[dict[str, Any]],
    top50: list[dict[str, Any]],
    bol_positive: list[dict[str, Any]],
    family_rows: list[dict[str, Any]],
    value_rows: list[dict[str, Any]],
    building_blocks: list[dict[str, Any]],
    risks: list[dict[str, Any]],
    fragile: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [
        "# Expanded O1.5 Feature Centrality Audit",
        "",
        "Scope: research only; no production selector/upload/model/threshold/grading changes.",
        "",
        "## Inputs",
        "",
        f"- Source pairwise interactions: `{source.as_posix()}`",
        f"- Eligible interactions with at least {MIN_RESOLVED} resolved: `{len(eligible)}`",
        f"- Positive ROI interactions with at least {MIN_RESOLVED} resolved: `{len(positive_50)}`",
        f"- Positive ROI interactions with at least {MIN_RESOLVED_STRICT} resolved: `{len(positive_75)}`",
        f"- Positive best-price and BetOnline interactions: `{len(bol_positive)}`",
        "",
        "## How Centrality Is Scored",
        "",
        "The audit parses every pairwise interaction into two components. It tracks both the variable family, such as `price_bucket`, and the specific value, such as `price_bucket=201-250`.",
        "",
        "A useful building block is not merely the single highest-ROI interaction. It appears repeatedly, pairs with different partners, has positive average and median ROI, and survives BetOnline view when that field is available.",
        "",
        "Fragile signals are high-ROI but low-support items that appear in only one or two successful interactions or depend on very few partner variables.",
        "",
        "## Top 10 Likely Building Blocks",
        "",
    ]
    lines.extend(
        _markdown_table(
            _top(building_blocks, 10),
            [
                "specific_value",
                "positive_50_appearances",
                "partner_family_count",
                "median_roi",
                "avg_roi",
                "positive_betonline_appearances",
                "consistency_score",
            ],
        )
    )
    lines.extend(["", "## Top 10 Recurring Risk Factors", ""])
    lines.extend(
        _markdown_table(
            _top(risks, 10),
            [
                "specific_value",
                "negative_50_appearances",
                "eligible_interactions",
                "median_roi",
                "avg_roi",
                "worst_interaction_roi",
                "risk_score",
            ],
        )
    )
    lines.extend(["", "## Top 10 Fragile / Hypothesis-Only Signals", ""])
    lines.extend(
        _markdown_table(
            _top(fragile, 10),
            [
                "specific_value",
                "positive_50_appearances",
                "top25_appearances",
                "partner_family_count",
                "median_roi",
                "best_interaction_roi",
                "classification",
            ],
        )
    )
    lines.extend(["", "## Strongest Variable Families", ""])
    lines.extend(
        _markdown_table(
            _top(family_rows, 10),
            [
                "specific_value",
                "positive_50_appearances",
                "partner_family_count",
                "median_roi",
                "avg_roi",
                "positive_betonline_appearances",
                "classification",
            ],
        )
    )
    lines.extend(["", "## BetOnline-Surviving Building Blocks", ""])
    bol_blocks = [
        row
        for row in building_blocks
        if _i(row.get("positive_betonline_appearances")) > 0 and (_f(row.get("avg_betonline_roi")) or 0) > 0
    ]
    lines.extend(
        _markdown_table(
            _top(bol_blocks, 10),
            [
                "specific_value",
                "positive_betonline_appearances",
                "avg_betonline_roi",
                "median_betonline_roi",
                "positive_50_appearances",
                "partner_family_count",
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Use `expanded_o15_feature_centrality_building_blocks.csv` for broad repeatable signals.",
            "- Use `expanded_o15_feature_centrality_risk_factors.csv` for recurring avoid/veto candidates.",
            "- Use `expanded_o15_feature_centrality_fragile.csv` as hypothesis fuel only; those rows are not yet stable enough for decision rules.",
            "- This audit ranks variables by recurrence across successful interactions, not by tiny optimized funnels.",
            "",
            "## Outputs",
            "",
            "- `expanded_o15_feature_centrality_family.csv`",
            "- `expanded_o15_feature_centrality_values.csv`",
            "- `expanded_o15_feature_centrality_building_blocks.csv`",
            "- `expanded_o15_feature_centrality_risk_factors.csv`",
            "- `expanded_o15_feature_centrality_fragile.csv`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(interactions_csv: Path, out_dir: Path) -> dict[str, Any]:
    rows = _read_csv(interactions_csv)
    eligible = [row for row in rows if _i(row.get("resolved")) >= MIN_RESOLVED]
    eligible.sort(key=lambda r: _f(r.get("roi")) or -999, reverse=True)
    positive_50 = [row for row in eligible if (_f(row.get("roi")) or 0) > 0]
    positive_75 = [row for row in eligible if _i(row.get("resolved")) >= MIN_RESOLVED_STRICT and (_f(row.get("roi")) or 0) > 0]
    top25 = eligible[:25]
    top50 = eligible[:50]
    bol_positive = [
        row
        for row in eligible
        if (_f(row.get("roi")) or 0) > 0
        and (_f(row.get("roi_betonline")) is not None)
        and (_f(row.get("roi_betonline")) or 0) > 0
    ]

    top25_ids = {id(row) for row in top25}
    top50_ids = {id(row) for row in top50}
    bol_ids = {id(row) for row in bol_positive}
    positive75_ids = {id(row) for row in positive_75}

    family_stats: dict[tuple[str, str], dict[str, Any]] = defaultdict(_blank_stats)
    value_stats: dict[tuple[str, str], dict[str, Any]] = defaultdict(_blank_stats)

    for row in eligible:
        roi = _f(row.get("roi")) or 0.0
        units = _f(row.get("units")) or 0.0
        is_positive = roi > 0
        flags = {
            "positive_50_appearances": is_positive,
            "positive_75_appearances": id(row) in positive75_ids,
            "top25_appearances": id(row) in top25_ids,
            "top50_appearances": id(row) in top50_ids,
            "positive_betonline_appearances": id(row) in bol_ids,
            "positive_unit_gain_appearances": is_positive and units >= MIN_UNIT_GAIN,
            "negative_50_appearances": roi < 0,
        }
        comp_a = _component(row, "a")
        comp_b = _component(row, "b")
        _add_interaction(family_stats, (comp_a[0], ""), (comp_b[0], ""), row, flags)
        _add_interaction(family_stats, (comp_b[0], ""), (comp_a[0], ""), row, flags)
        _add_interaction(value_stats, comp_a, comp_b, row, flags)
        _add_interaction(value_stats, comp_b, comp_a, row, flags)

    family_rows = _materialize(family_stats, "family")
    value_rows = _materialize(value_stats, "value")
    building_blocks = [
        row
        for row in value_rows
        if row.get("classification") == "building_block"
        and _i(row.get("positive_50_appearances")) >= 3
        and _i(row.get("partner_family_count")) >= 3
    ]
    building_blocks.sort(key=lambda r: (_f(r.get("consistency_score")) or 0, _f(r.get("median_roi")) or -999), reverse=True)

    risks = [
        row
        for row in value_rows
        if (
            row.get("classification") == "frequent_but_harmful"
            or (_i(row.get("negative_50_appearances")) >= 3 and (_f(row.get("median_roi")) or 0) < 0)
        )
    ]
    risks.sort(key=lambda r: (_f(r.get("risk_score")) or 0, _i(r.get("negative_50_appearances"))), reverse=True)

    fragile = [row for row in value_rows if row.get("classification") == "fragile_hypothesis"]
    fragile.sort(key=lambda r: (_f(r.get("best_interaction_roi")) or -999, _i(r.get("top25_appearances"))), reverse=True)

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "expanded_o15_feature_centrality_family.csv", family_rows)
    _write_csv(out_dir / "expanded_o15_feature_centrality_values.csv", value_rows)
    _write_csv(out_dir / "expanded_o15_feature_centrality_building_blocks.csv", building_blocks)
    _write_csv(out_dir / "expanded_o15_feature_centrality_risk_factors.csv", risks)
    _write_csv(out_dir / "expanded_o15_feature_centrality_fragile.csv", fragile)
    _write_report(
        out_dir / "expanded_o15_feature_centrality_audit.md",
        interactions_csv,
        eligible,
        positive_50,
        positive_75,
        top25,
        top50,
        bol_positive,
        family_rows,
        value_rows,
        building_blocks,
        risks,
        fragile,
    )
    return {
        "status": "ok",
        "interactions": len(rows),
        "eligible_min50": len(eligible),
        "positive_min50": len(positive_50),
        "positive_min75": len(positive_75),
        "positive_betonline": len(bol_positive),
        "family_rows": len(family_rows),
        "value_rows": len(value_rows),
        "building_blocks": len(building_blocks),
        "risk_factors": len(risks),
        "fragile": len(fragile),
        "report": str(out_dir / "expanded_o15_feature_centrality_audit.md"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit feature centrality across expanded O1.5 pairwise interactions.")
    parser.add_argument("--interactions-csv", type=Path, default=DEFAULT_INTERACTIONS_CSV)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    print(run(args.interactions_csv, args.out_dir))


if __name__ == "__main__":
    main()
