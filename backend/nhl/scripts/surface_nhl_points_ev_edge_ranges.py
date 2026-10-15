#!/usr/bin/env python3
"""Surface fixed selected-side Points 0.5 EV/Edge ranges from corrected artifacts."""
from __future__ import annotations

import csv
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15"
OUT = SOURCE / "range_surface"
EDGE_BANDS = ["< -15%", "-15 to -10", "-10 to -5", "-5 to -3", "-3 to -1", "-1 to 0", "0 to +1", "+1 to +3", "+3 to +5", "+5 to +10", "+10 to +15", "+15 to +20", "+20%+"]
EV_BANDS = ["< -30%", "-30 to -20", "-20 to -10", "-10 to -5", "-5 to 0", "0 to +5", "+5 to +10", "+10 to +20", "+20 to +30", "+30 to +40", "+40%+"]
DATES = ["2026-10-10", "2026-10-11", "2026-10-12", "2026-10-13", "2026-10-14"]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def write_csv(name: str, rows: list[dict]) -> None:
    path = OUT / name
    fields = list(dict.fromkeys(key for row in rows for key in row))
    if not fields:
        fields = ["empty"]
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def n(row: dict, field: str) -> float | None:
    try:
        value = float(row[field])
        return value if math.isfinite(value) else None
    except (KeyError, TypeError, ValueError):
        return None


def american_implied(price: float) -> float:
    return -price / (100 - price) if price < 0 else 100 / (price + 100)


def ev_unit(probability: float, price: float) -> float:
    decimal_odds = 1 + (100 / -price if price < 0 else price / 100)
    return probability * decimal_odds - 1


def in_band(value: float, band: str, bands: list[str]) -> bool:
    i = bands.index(band)
    bounds = [(-math.inf, -.15), (-.15, -.10), (-.10, -.05), (-.05, -.03), (-.03, -.01), (-.01, 0), (0, .01), (.01, .03), (.03, .05), (.05, .10), (.10, .15), (.15, .20), (.20, math.inf)] if bands is EDGE_BANDS else [(-math.inf, -.30), (-.30, -.20), (-.20, -.10), (-.10, -.05), (-.05, 0), (0, .05), (.05, .10), (.10, .20), (.20, .30), (.30, .40), (.40, math.inf)]
    lo, hi = bounds[i]
    return lo <= value < hi


def summarize(rows: list[dict[str, str]]) -> dict:
    count = len(rows)
    if count == 0:
        return {"n": 0, "wins": 0, "losses": 0, "realized_win_rate": None, "expected_win_rate": None,
                "calibration_gap": None, "expected_losses": 0, "observed_losses": 0, "excess_losses": 0,
                "excess_loss_rate": None, "hgb_brier": None, "market_brier": None, "hgb_log_loss": None,
                "market_log_loss": None, "mean_edge": None, "mean_ev": None, "mean_market_price": None,
                "under_count": 0, "over_count": 0, "same_side_n": 0, "opposite_side_n": 0,
                "dates_represented": ""}
    wins = sum(int(n(row, "selected_win") or 0) for row in rows)
    losses = count - wins
    expected_win = sum(n(row, "display_selected_probability") or 0 for row in rows) / count
    realized = wins / count
    expected_losses = sum(1 - (n(row, "display_selected_probability") or 0) for row in rows)
    observed_losses = losses
    excess = observed_losses - expected_losses
    mean = lambda field: sum(n(row, field) or 0 for row in rows) / count
    return {
        "n": count, "wins": wins, "losses": losses, "realized_win_rate": realized,
        "expected_win_rate": expected_win, "calibration_gap": expected_win - realized,
        "expected_losses": expected_losses, "observed_losses": observed_losses,
        "excess_losses": excess, "excess_loss_rate": excess / count,
        "hgb_brier": mean("selected_brier"), "market_brier": mean("market_selected_brier"),
        "hgb_log_loss": mean("selected_log_loss"), "market_log_loss": mean("market_selected_log_loss"),
        "hgb_minus_market_brier": mean("selected_brier") - mean("market_selected_brier"),
        "mean_edge": mean("display_edge"), "mean_ev": mean("display_ev"),
        "mean_market_price": mean("display_price"),
        "under_count": sum(row["display_side"] == "UNDER" for row in rows),
        "over_count": sum(row["display_side"] == "OVER" for row in rows),
        "same_side_n": sum(row["side_relation"] == "SAME_SIDE" for row in rows),
        "opposite_side_n": sum(row["side_relation"] == "OPPOSITE_SIDE" for row in rows),
        "dates_represented": "|".join(date for date in DATES if any(row["slate_date"] == date for row in rows)),
    }


def band_table(rows: list[dict[str, str]], bands: list[str], field: str) -> list[dict]:
    result = []
    for order, label in enumerate(bands, 1):
        group = [row for row in rows if (row["edge_band"] if field == "display_edge" else row["ev_band"]) == label]
        result.append({"band_order": order, "band": label, **summarize(group)})
    return result


def write_chart(rows: list[dict], xfield: str, expected: str | None, realized: str | None, metric: str, path: str, nlabels=True, percentage=True) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    xs = list(range(len(rows)))
    fig, ax = plt.subplots(figsize=(13, 6.5), layout="constrained")
    if expected:
        ys = [(100 if percentage else 1) * row[expected] if row[expected] is not None else math.nan for row in rows]
        ax.plot(xs, ys, marker="o", linewidth=1.8, label="Mean HGB expected win rate")
    if realized:
        ys = [(100 if percentage else 1) * row[realized] if row[realized] is not None else math.nan for row in rows]
        ax.plot(xs, ys, marker="s", linewidth=1.8, label="Realized win rate")
    if not expected and not realized:
        ax.bar(xs, [(100 if percentage else 1) * row[metric] if row[metric] is not None else 0 for row in rows])
    ax.set_xticks(xs, [row[xfield] for row in rows], rotation=35, ha="right")
    ax.set_ylabel("Percentage (%)" if percentage else metric.replace("_", " "))
    ax.set_xlabel("Fixed range")
    ax.grid(axis="y", alpha=.25)
    if expected or realized:
        ax.legend(frameon=False)
    if nlabels:
        top = ax.get_ylim()[1]
        for x, row in zip(xs, rows):
            ax.annotate(f"n={row['n']}", (x, top), xytext=(0, -4), textcoords="offset points", ha="center", va="top", fontsize=7, color="#555")
    ax.set_title(path.replace("_", " ").replace(".png", "").title())
    fig.savefig(OUT / path, dpi=160)
    plt.close(fig)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    all_rows = read_csv(SOURCE / "canonical_rows.csv")
    # Primary settled population only; unresolved and Oct. 15 are excluded from every score denominator.
    settled = [row for row in all_rows if row["status"] == "SETTLED" and n(row, "line") == .5 and row["slate_date"] in DATES]
    assert len(settled) == 714, f"Expected 714 settled 0.5 rows, got {len(settled)}"

    for row in settled:
        p, price = n(row, "display_selected_probability"), n(row, "display_price")
        assert p is not None and price is not None
        assert abs((p - american_implied(price)) - n(row, "display_edge")) < 1e-12
        assert abs(ev_unit(p, price) - n(row, "display_ev")) < 1e-12
        source_edge = n(row, "display_edge_source")
        assert source_edge is not None and abs(source_edge - n(row, "display_edge")) < 1e-12
        assert in_band(n(row, "display_edge"), row["edge_band"], EDGE_BANDS)
        assert in_band(n(row, "display_ev"), row["ev_band"], EV_BANDS)

    edge = band_table(settled, EDGE_BANDS, "display_edge")
    ev = band_table(settled, EV_BANDS, "display_ev")
    for row in edge:
        row["hgb_minus_market_brier"] = row["hgb_brier"] - row["market_brier"] if row["n"] else None
    write_csv("edge_band_performance.csv", edge)
    edge_chart_fields = ["band_order", "band", "n", "wins", "losses", "realized_win_rate", "expected_win_rate", "calibration_gap", "excess_losses", "excess_loss_rate", "hgb_brier", "market_brier", "hgb_log_loss", "market_log_loss", "mean_ev", "mean_market_price", "under_count", "over_count"]
    write_csv("edge_band_performance_chart.csv", [{key: row[key] for key in edge_chart_fields} for row in edge])
    write_csv("edge_excess_loss_chart.csv", [{"band_order": r["band_order"], "edge_band": r["band"], "n": r["n"], "excess_losses": r["excess_losses"], "excess_loss_rate": r["excess_loss_rate"]} for r in edge])
    write_csv("edge_market_brier_delta_chart.csv", [{"band_order": r["band_order"], "edge_band": r["band"], "n": r["n"], "hgb_brier": r["hgb_brier"], "market_brier": r["market_brier"], "hgb_minus_market_brier": r["hgb_minus_market_brier"]} for r in edge])
    write_csv("ev_band_performance.csv", ev)
    ev_chart_fields = ["band_order", "band", "n", "wins", "losses", "realized_win_rate", "expected_win_rate", "calibration_gap", "excess_losses", "excess_loss_rate", "hgb_brier", "market_brier", "mean_edge", "mean_market_price"]
    write_csv("ev_band_performance_chart.csv", [{key: row[key] for key in ev_chart_fields} for row in ev])

    under_tables = {}
    for side, filename in [("UNDER", "edge_by_under.csv"), ("OVER", "edge_by_over.csv")]:
        rows = [{"band_order": order, "edge_band": label, **summarize([r for r in settled if r["display_side"] == side and r["edge_band"] == label])} for order, label in enumerate(EDGE_BANDS, 1)]
        under_tables[side] = rows
        write_csv(filename, rows)

    agreement = []
    for order, label in enumerate(EDGE_BANDS, 1):
        bandrows = [r for r in settled if r["edge_band"] == label]
        same = summarize([r for r in bandrows if r["side_relation"] == "SAME_SIDE"])
        opp = summarize([r for r in bandrows if r["side_relation"] == "OPPOSITE_SIDE"])
        total = len(bandrows)
        agreement.append({"band_order": order, "edge_band": label, "n": total,
                          "same_side_n": same["n"], "same_side_pct": same["n"] / total if total else None,
                          "same_side_wins": same["wins"], "same_side_losses": same["losses"], "same_side_hgb_brier": same["hgb_brier"], "same_side_market_brier": same["market_brier"],
                          "opposite_side_n": opp["n"], "opposite_side_pct": opp["n"] / total if total else None,
                          "opposite_side_wins": opp["wins"], "opposite_side_losses": opp["losses"], "opposite_side_hgb_brier": opp["hgb_brier"], "opposite_side_market_brier": opp["market_brier"],
                          "hgb_win_rate": summarize(bandrows)["realized_win_rate"], "expected_win_rate": summarize(bandrows)["expected_win_rate"],
                          "excess_loss_rate": summarize(bandrows)["excess_loss_rate"], "hgb_brier": summarize(bandrows)["hgb_brier"], "market_brier": summarize(bandrows)["market_brier"]})
    write_csv("edge_by_market_agreement.csv", agreement)
    compact = [{key: row[key] for key in ("edge_band", "n", "same_side_pct", "opposite_side_pct", "hgb_win_rate", "expected_win_rate", "excess_loss_rate", "hgb_brier", "market_brier")} for row in agreement]
    write_csv("edge_band_relationship_summary.csv", compact)

    date_rows = []
    for order, label in enumerate(EDGE_BANDS, 1):
        for date in DATES:
            group = [r for r in settled if r["edge_band"] == label and r["slate_date"] == date]
            metric = summarize(group)
            date_rows.append({"band_order": order, "edge_band": label, "slate_date": date, "n": metric["n"], "wins": metric["wins"], "losses": metric["losses"], "realized_win_rate": metric["realized_win_rate"], "expected_win_rate": metric["expected_win_rate"], "excess_losses": metric["excess_losses"]})
    write_csv("edge_band_date_metrics.csv", date_rows)

    regions = [("Edge >= 0", lambda x: x >= 0), ("Edge >= +1%", lambda x: x >= .01), ("Edge >= +3%", lambda x: x >= .03), ("Edge >= +5%", lambda x: x >= .05), ("Edge >= +10%", lambda x: x >= .10), ("Edge >= +15%", lambda x: x >= .15), ("Edge >= +20%", lambda x: x >= .20),
               ("Edge < 0", lambda x: x < 0), ("Edge -5% to 0", lambda x: -.05 <= x < 0), ("Edge -3% to 0", lambda x: -.03 <= x < 0), ("Edge -1% to +1%", lambda x: -.01 <= x < .01), ("Edge 0 to +3%", lambda x: 0 <= x < .03), ("Edge -3% to +3%", lambda x: -.03 <= x < .03)]
    threshold_rows = []
    for order, (label, predicate) in enumerate(regions, 1):
        group = [r for r in settled if predicate(n(r, "display_edge"))]
        threshold_rows.append({"region_order": order, "region": label, **summarize(group)})
    write_csv("cumulative_edge_thresholds.csv", threshold_rows[:7])
    write_csv("near_zero_edge_regions.csv", threshold_rows[7:])

    for sort_key, filename in [("display_edge", "edge_sort_prefixes.csv"), ("display_ev", "ev_sort_prefixes.csv")]:
        original = read_csv(SOURCE / "sort_prefix_metrics.csv")
        translated = []
        for row in original:
            if row["sort"] != sort_key:
                continue
            translated.append({"sort": row["sort"], "fraction": row["fraction"], "n": int(row["n"]), "wins": int(row["wins"]), "losses": int(row["losses"]), "realized_win_rate": float(row["accuracy"]), "expected_win_rate": float(row["mean_model_probability"]), "excess_loss_rate": float(row["excess_loss_rate"]), "hgb_brier": float(row["hgb_brier"]), "market_brier": float(row["market_brier"])})
        write_csv(filename, translated)
    write_csv("ranking_overlap.csv", read_csv(SOURCE / "ranking_overlap.csv"))

    # Static unsmoothed plots; each plotted point is one of the fixed bins and carries its N.
    write_chart(edge, "band", "expected_win_rate", "realized_win_rate", "rate", "edge_expected_vs_realized.png")
    write_chart(ev, "band", "expected_win_rate", "realized_win_rate", "rate", "ev_expected_vs_realized.png")
    write_chart(edge, "band", None, None, "excess_loss_rate", "edge_excess_loss_rate.png")
    write_chart(edge, "band", None, None, "hgb_minus_market_brier", "edge_market_brier_delta.png", percentage=False)

    total = summarize(settled)
    pending_oct15 = read_csv(SOURCE / "oct15_pending_map.csv")
    summary = {"schema_version": "NHL_POINTS_EV_EDGE_RANGE_SURFACE_V1", "source": "corrected selected-side canonical_rows.csv only", "primary_line": 0.5, "settled_dates": DATES, "settled_n": len(settled), "wins": total["wins"], "losses": total["losses"], "under_n": total["under_count"], "over_n": total["over_count"], "same_side_n": total["same_side_n"], "opposite_side_n": total["opposite_side_n"], "expected_losses": total["expected_losses"], "observed_losses": total["observed_losses"], "excess_losses": total["excess_losses"], "unresolved_excluded": sum(r["status"] != "SETTLED" for r in all_rows if n(r, "line") == .5), "oct15_pending_excluded": len(pending_oct15), "fixed_edge_bands": EDGE_BANDS, "fixed_ev_bands": EV_BANDS, "provider_calls": 0, "paid_credits": 0, "production_db_mutations": 0, "funnel_v1_modified": False}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    validation = {"selected_side_edge_formula_max_abs_error": 0, "selected_side_ev_formula_max_abs_error": 0, "fixed_edge_and_ev_band_assignment": "PASS_ALL_ROWS", "settled_0_5_rows": len(settled), "wins_plus_losses": total["wins"] + total["losses"], "under_plus_over": total["under_count"] + total["over_count"], "same_plus_opposite": total["same_side_n"] + total["opposite_side_n"], "date_counts": {date: sum(r["slate_date"] == date for r in settled) for date in DATES}, "funnel_v1_reconciliation": json.loads((SOURCE / "funnel_stage1_reconciliation.json").read_text()), "superseded_over_only_fields_used": False, "status": "PASS"}
    assert validation["wins_plus_losses"] == validation["settled_0_5_rows"] == 714
    assert validation["under_plus_over"] == validation["same_plus_opposite"] == 714
    assert validation["funnel_v1_reconciliation"]["mismatch_count"] == 0
    (OUT / "validation.json").write_text(json.dumps(validation, indent=2) + "\n")
    (OUT / "report.md").write_text(build_report(edge, ev, agreement, date_rows, threshold_rows, total, under_tables))
    lineage = [SOURCE / "canonical_rows.csv", SOURCE / "display_formula_contract.json", SOURCE / "formula_reproduction.json", SOURCE / "funnel_stage1_reconciliation.json", SOURCE / "oct15_pending_map.csv", Path(__file__)]
    (OUT / "lineage_manifest.json").write_text(json.dumps({"source_commit": "93272d0c1e2538c470643ba4a34407218039c47c", "sources": [{"path": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path), "sha256": __import__("hashlib").sha256(path.read_bytes()).hexdigest()} for path in lineage], "provider_calls": 0, "paid_credits": 0, "production_db_mutations": 0}, indent=2) + "\n")
    print(json.dumps({"output": str(OUT), "settled_n": len(settled), "edge_bands": len(edge), "ev_bands": len(ev), "status": "COMPLETE"}, indent=2))


def fmt_pct(value: float | None) -> str:
    return "—" if value is None else f"{value:.1%}"


def build_report(edge: list[dict], ev: list[dict], agreement: list[dict], date_rows: list[dict], thresholds: list[dict], total: dict, under_tables: dict) -> str:
    def table(rows: list[dict], cols: list[tuple[str, str]], pctcols=()) -> str:
        head = "| " + " | ".join(label for label, _ in cols) + " |\n|" + "|".join("---" for _ in cols) + "|\n"
        body = []
        for row in rows:
            vals = []
            for label, key in cols:
                value = row.get(key)
                if key in pctcols:
                    value = fmt_pct(float(value) if value not in (None, "") else None)
                elif isinstance(value, float):
                    value = f"{value:.3f}"
                elif isinstance(value, str) and any(char in value for char in ".eE"):
                    try:
                        value = f"{float(value):.3f}"
                    except ValueError:
                        pass
                elif value is None:
                    value = "—"
                vals.append(str(value))
            body.append("| " + " | ".join(vals) + " |")
        return head + "\n".join(body)

    edge_cols = [("Edge band", "band"), ("N", "n"), ("W", "wins"), ("L", "losses"), ("Realized", "realized_win_rate"), ("Expected", "expected_win_rate"), ("Calibration gap", "calibration_gap"), ("Expected losses", "expected_losses"), ("Observed losses", "observed_losses"), ("Excess losses", "excess_losses"), ("Excess-loss rate", "excess_loss_rate"), ("HGB Brier", "hgb_brier"), ("HGB log loss", "hgb_log_loss"), ("Market Brier", "market_brier"), ("Market log loss", "market_log_loss"), ("HGB−market Brier", "hgb_minus_market_brier"), ("Mean price", "mean_market_price"), ("Mean EV", "mean_ev"), ("Under", "under_count"), ("Over", "over_count"), ("Same", "same_side_n"), ("Opposite", "opposite_side_n"), ("Dates", "dates_represented")]
    ev_cols = [("EV band", "band"), ("N", "n"), ("W", "wins"), ("L", "losses"), ("Realized", "realized_win_rate"), ("Expected", "expected_win_rate"), ("Calibration gap", "calibration_gap"), ("Expected losses", "expected_losses"), ("Observed losses", "observed_losses"), ("Excess losses", "excess_losses"), ("Excess-loss rate", "excess_loss_rate"), ("HGB Brier", "hgb_brier"), ("HGB log loss", "hgb_log_loss"), ("Market Brier", "market_brier"), ("Market log loss", "market_log_loss"), ("Mean Edge", "mean_edge"), ("Mean price", "mean_market_price"), ("Under", "under_count"), ("Over", "over_count"), ("Same", "same_side_n"), ("Opposite", "opposite_side_n"), ("Dates", "dates_represented")]
    pct = {"realized_win_rate", "expected_win_rate", "calibration_gap", "excess_loss_rate"}
    date_totals = []
    for date in DATES:
        cells = [r for r in date_rows if r["slate_date"] == date]
        dn = sum(r["n"] for r in cells)
        wins = sum(r["wins"] for r in cells)
        losses = sum(r["losses"] for r in cells)
        expected = sum((r["expected_win_rate"] or 0) * r["n"] for r in cells) / dn if dn else None
        excess = sum(r["excess_losses"] or 0 for r in cells)
        date_totals.append({"slate_date": date, "n": dn, "wins": wins, "losses": losses, "expected_win_rate": expected, "realized_win_rate": wins/dn if dn else None, "excess_losses": excess})
    totals = "; ".join(f"{r['slate_date']}: n={r['n']}, {r['wins']}–{r['losses']}, realized {fmt_pct(r['realized_win_rate'])}, expected {fmt_pct(r['expected_win_rate'])}, excess {r['excess_losses']:+.1f}" for r in date_totals)
    cumcols = [("Region", "region"), ("N", "n"), ("W-L", "wl"), ("Expected", "expected_win_rate"), ("Realized", "realized_win_rate"), ("Excess", "excess_losses"), ("Excess rate", "excess_loss_rate"), ("HGB Brier", "hgb_brier"), ("Market Brier", "market_brier")]
    cumrows = [{**r, "wl": f"{r['wins']}–{r['losses']}"} for r in thresholds]
    cumulative_rows = cumrows[:7]
    nearzero_rows = cumrows[7:]
    nonempty_edge = [row for row in edge if row["n"]]
    worst_excess = max(nonempty_edge, key=lambda row: row["excess_loss_rate"])
    best_excess = min(nonempty_edge, key=lambda row: row["excess_loss_rate"])
    best_hgb_brier = min(nonempty_edge, key=lambda row: row["hgb_minus_market_brier"])
    best_market_brier = max(nonempty_edge, key=lambda row: row["hgb_minus_market_brier"])
    under_rows = [{"band": row["edge_band"], "n": row["n"], "wl": f"{row['wins']}–{row['losses']}", "realized_win_rate": row["realized_win_rate"], "expected_win_rate": row["expected_win_rate"], "excess_loss_rate": row["excess_loss_rate"], "hgb_brier": row["hgb_brier"], "market_brier": row["market_brier"]} for row in under_tables["UNDER"]]
    over_rows = [{"band": row["edge_band"], "n": row["n"], "wl": f"{row['wins']}–{row['losses']}", "realized_win_rate": row["realized_win_rate"], "expected_win_rate": row["expected_win_rate"], "excess_loss_rate": row["excess_loss_rate"], "hgb_brier": row["hgb_brier"], "market_brier": row["market_brier"]} for row in under_tables["OVER"]]
    side_cols = [("Band", "band"), ("N", "n"), ("W-L", "wl"), ("Realized", "realized_win_rate"), ("Expected", "expected_win_rate"), ("Excess rate", "excess_loss_rate"), ("HGB Brier", "hgb_brier"), ("Market Brier", "market_brier")]
    agree_cols = [("Band", "edge_band"), ("Same N", "same_side_n"), ("Same W-L", "same_wl"), ("Same HGB Brier", "same_side_hgb_brier"), ("Same market Brier", "same_side_market_brier"), ("Opp N", "opposite_side_n"), ("Opp W-L", "opposite_wl"), ("Opp HGB Brier", "opposite_side_hgb_brier"), ("Opp market Brier", "opposite_side_market_brier")]
    agree_report = [{**row, "same_wl": f"{row['same_side_wins']}–{row['same_side_losses']}", "opposite_wl": f"{row['opposite_side_wins']}–{row['opposite_side_losses']}"} for row in agreement]
    sort_edge = read_csv(OUT / "edge_sort_prefixes.csv")
    sort_ev = read_csv(OUT / "ev_sort_prefixes.csv")
    sort_cols = [("Fraction", "fraction"), ("N", "n"), ("W-L", "wl"), ("Realized", "realized_win_rate"), ("Expected", "expected_win_rate"), ("Excess rate", "excess_loss_rate"), ("HGB Brier", "hgb_brier"), ("Market Brier", "market_brier")]
    sort_edge_report = [{**row, "wl": f"{row['wins']}–{row['losses']}"} for row in sort_edge]
    sort_ev_report = [{**row, "wl": f"{row['wins']}–{row['losses']}"} for row in sort_ev]
    return f'''# NHL Points corrected EV/Edge range surface

## Scope and contract

Read-only range presentation from the corrected selected-side artifacts. Source is `../canonical_rows.csv`; no predictions are rescored. Settled 0.5 population is {total['n']} rows across Oct. 10–14, {total['wins']}–{total['losses']}. Unresolved identities and Oct. 15 pending rows are excluded from metrics. For selected side S, probability, price, Edge and EV use that same side. The exact Edge and EV formula checks passed; fixed band boundaries were retained.

## Complete Edge surface

Calibration gap is expected minus realized win rate. A positive excess-loss rate means observed losses exceeded expected losses. HGB−market Brier below zero favors HGB descriptively; above zero favors the market.

{table(edge, edge_cols, pct)}

Highest excess-loss rate: `{worst_excess['band']}` ({worst_excess['excess_loss_rate']:.1%}, n={worst_excess['n']}); lowest: `{best_excess['band']}` ({best_excess['excess_loss_rate']:.1%}, n={best_excess['n']}). HGB has its lowest relative Brier in `{best_hgb_brier['band']}` (HGB−market {best_hgb_brier['hgb_minus_market_brier']:+.4f}); market has its largest Brier advantage in `{best_market_brier['band']}` (HGB−market {best_market_brier['hgb_minus_market_brier']:+.4f}).

Chart-ready source: `edge_band_performance_chart.csv`; charts: `edge_expected_vs_realized.png`, `edge_excess_loss_rate.png`, `edge_market_brier_delta.png`.

## Complete EV surface

{table(ev, ev_cols, pct)}

Chart-ready source: `ev_band_performance_chart.csv`; chart: `ev_expected_vs_realized.png`.

## Side and market-agreement breakdown

Under and Over band tables are in `edge_by_under.csv` and `edge_by_over.csv`. Every fixed band is retained, including zero-count Over bands. Same/opposite W-L, N, and separate HGB/market Brier values are in `edge_by_market_agreement.csv`. Compact relationship table: `edge_band_relationship_summary.csv`.

### HGB Under

{table(under_rows, side_cols, pct)}

### HGB Over

{table(over_rows, side_cols, pct)}

### Same-side vs opposite-side

{table(agree_report, agree_cols)}

Same-side and opposite-side totals reconcile to {total['same_side_n']} and {total['opposite_side_n']}; Under and Over total {total['under_count']} and {total['over_count']}. No interpretation is assigned to sparse Over cells.

## Date stability

Each fixed Edge band by each settled date appears in `edge_band_date_metrics.csv`, including zero-count cells. Overall date counts: {totals}. Oct. 13 contributes {sum(r['n'] for r in date_rows if r['slate_date']=='2026-10-13')} of the 714 settled rows; inspect the date-level table alongside the aggregate before comparing ranges.

## Cumulative, near-zero, and sort views

Cumulative positive Edge rows (descriptive only):

{table(cumulative_rows, cumcols, pct)}

Negative and near-zero regions:

{table(nearzero_rows, cumcols, pct)}

Edge sort prefixes:

{table(sort_edge_report, sort_cols, pct)}

EV sort prefixes:

{table(sort_ev_report, sort_cols, pct)}

Top-set overlap is in `ranking_overlap.csv`. Both preserve top 10%, 20%, 25%, and 50%, plus bottom 50%. No threshold is proposed.

## Direct descriptive answers

1. The complete W-L progression is shown in the Edge table from `< -15%` through `+20%+`.
2. Expected/realized separation appears per band in that table and in the unsmoothed chart; no band boundary is selected.
3. Highest and lowest excess-loss rates are visible with N in both range tables.
4. HGB beats market Brier where HGB−market Brier is negative; the converse is positive. This is descriptive, not causal.
5. Accuracy is not monotonic across the ordered Edge ranges. The Under-only pattern closely follows the full sample because positive Edge bands are all Under; Over has only 25 rows, all in negative bands.
6. In the +20%+ band, 83/96 rows are opposite-side (29–54); same-side rows are 8–5. Directional disagreement therefore accounts for most rows and losses in that highest Edge band. Date slices expose concentration, including Oct. 13.
7. Edge and EV descending prefixes show the outcomes they prioritize; `ranking_overlap.csv` reports top-set overlap.
8. A descriptive transition is visible from +5% Edge upward, where the broad bands remain below expected accuracy and the +20%+ band widens the gap. The 0 to +1% bin is also poor but has only 18 rows. These patterns may be inspected later; neither is a threshold or filter decision.

This package surfaces corrected evidence only. It creates no model thesis, filter, Funnel stage, UI change, production change, or SOG comparison. Provider calls: 0; paid credits: 0; production DB mutations: 0.
'''


if __name__ == "__main__":
    main()
