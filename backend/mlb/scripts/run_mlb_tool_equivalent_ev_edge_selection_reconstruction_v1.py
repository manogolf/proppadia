#!/usr/bin/env python3
"""No-refit, read-only reconstruction of the owner's external EV/Edge selector."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_tool_equivalent_ev_edge_selection_reconstruction_v1/2026-08-11"
JOIN = ROOT / "artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10/totals_pinnacle_join.csv"
GAME_POP = ROOT / "artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06/certified_totals_game_population.csv"
EXPERIMENT = "MLB_TOOL_EQUIVALENT_EV_EDGE_SELECTION_RECONSTRUCTION_V1"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def implied(american):
    x = np.asarray(american, dtype=float)
    out = np.empty_like(x, dtype=float); positive = x > 0
    out[positive] = 100 / (x[positive] + 100)
    out[~positive] = -x[~positive] / (-x[~positive] + 100)
    return out


def decimal(american):
    x = np.asarray(american, dtype=float)
    out = np.empty_like(x, dtype=float); positive = x > 0
    out[positive] = 1 + x[positive] / 100
    out[~positive] = 1 + 100 / -x[~positive]
    return out


def fair_american(probability):
    p = np.clip(np.asarray(probability, dtype=float), 1e-9, 1 - 1e-9)
    return np.where(p >= .5, -100 * p / (1 - p), 100 * (1 - p) / p)


def example_validation():
    examples = [
        (-103, 105, 4.1, 2.0, "Cleveland at Detroit Under 8"),
        (-159, -148, 2.8, 1.7, "visible row 2"),
        (-156, -147, 2.5, 1.5, "visible row 3"),
        (-130, -125, 1.8, 1.0, "visible row 4"),
        (-185, -178, 1.5, .9, "visible row 5"),
        (-111, -111, .1, .1, "visible row 6"),
    ]
    rows = []
    for fair, book, shown_ev, shown_edge, label in examples:
        mp, bp = float(implied(fair)), float(implied(book))
        ev, edge = 100 * (mp * float(decimal(book)) - 1), 100 * (mp - bp)
        rows.append({"example": label, "model_fair_odds": fair, "book_odds": book,
                     "model_probability": mp, "raw_book_implied_probability": bp,
                     "calculated_ev_pct": ev, "displayed_ev_pct": shown_ev,
                     "ev_display_difference_pp": ev - shown_ev,
                     "calculated_edge_pct": edge, "displayed_edge_pct": shown_edge,
                     "edge_display_difference_pp": edge - shown_edge,
                     "within_0_15pp_display_tolerance": abs(ev - shown_ev) <= .15 and abs(edge - shown_edge) <= .15,
                     "formula_support_status": "SUPPORTS_FORMULA_WITH_DISPLAY_OR_HIDDEN_PRECISION" if fair != book
                     else "REQUIRES_HIDDEN_UNROUNDED_INPUT_IDENTICAL_DISPLAYED_ODDS_YIELD_ZERO",
                     "interpretation": "formula reproduced to displayed precision; minor difference is consistent with hidden pre-display precision" if fair != book
                     else "identical displayed integer odds mathematically yield 0.0%; at least one underlying input must differ before display"})
    frame = pd.DataFrame(rows)
    frame.to_csv(OUT / "tool_example_formula_validation.csv", index=False)
    matched = int((frame.model_fair_odds != frame.book_odds).sum())
    (OUT / "tool_math_reconstruction.md").write_text(f"""# Tool math reconstruction

The supplied formulas reproduce **{matched} of {len(frame)}** visible rows at one-decimal display precision.

- `model_probability = american_implied(model_fair_odds)`
- `raw_book_implied_probability = american_implied(book_odds)`
- `Edge % = 100 × (model_probability - raw_book_implied_probability)`
- `EV % = 100 × (model_probability × book_decimal_odds - 1)`

The -103/+105 example calculates EV {frame.iloc[0].calculated_ev_pct:.3f}% and Edge {frame.iloc[0].calculated_edge_pct:.3f}%, displayed as 4.1%/2.0% under ordinary UI rounding or hidden precision. The -111/-111 example cannot produce 0.1%/0.1% from the displayed integer prices: identical prices give exactly 0.0%/0.0%. The smallest required difference is hidden unrounded fair probability or odds before integer display. No alternate formula is supported by the other examples.

Edge is based on the raw offered price, not paired no-vig probability. Both are retained in replay, but raw Edge governs selection.
""")


def write_contract():
    contract = {
        "experiment": EXPERIMENT, "max_days": 1, "min_weight": 10,
        "weight_gate_status": "NOT_APPLIED_EXTERNAL_TOOL_WEIGHT_NOT_REPRODUCIBLE",
        "ev_pct": {"minimum_inclusive": 0.0, "maximum_inclusive": 8.0},
        "edge_pct": {"minimum_inclusive": 0.01, "maximum_inclusive": 6.0},
        "venue_book_american_odds": {"minimum_inclusive": -400, "maximum": None},
        "model_fair_american_odds": {"minimum": None, "maximum_inclusive": 100,
                                     "equivalent_probability_minimum_inclusive": .5},
        "sportsbooks": ["Pinnacle", "NoVig"],
        "market_families": ["FULL_GAME_TOTAL", "FIRST_5_TOTAL", "FULL_GAME_TEAM_TOTAL", "FIRST_5_TEAM_TOTAL"],
        "primary_edge_formula": "100*(model_probability-raw_book_implied_probability)",
        "ev_formula": "100*(model_probability*book_decimal_odds-1)",
        "interpretation": "blank bound means unbounded; American odds ordering is numeric",
    }
    (OUT / "owner_filter_contract.json").write_text(json.dumps(contract, indent=2) + "\n")
    (OUT / "weight_semantics_audit.md").write_text("""# Weight semantics audit

`EXTERNAL_TOOL_WEIGHT_NOT_REPRODUCIBLE`

Repository and retained-data searches found no external-tool export field, schema, or example defining `Weight`. Internal Proppadia uses of “weight” refer to unrelated model coefficients, sample blends, or calibration weights and cannot be substituted. It is therefore unknown whether the tool means model count, source agreement, confidence/sample weight, or another quantity.

The replay applies every reproducible owner filter and labels the Weight gate `NOT_APPLIED_EXTERNAL_TOOL_WEIGHT_NOT_REPRODUCIBLE`. Results are partial and are not claimed to be the exact final external-tool selection set.
""")


def availability():
    j = pd.read_csv(JOIN)
    rows = []
    for market in ["FULL_GAME_TOTAL", "FIRST_5_TOTAL", "FULL_GAME_TEAM_TOTAL", "FIRST_5_TEAM_TOTAL"]:
        for book in ["Pinnacle", "NoVig"]:
            ready = market == "FULL_GAME_TOTAL" and book == "Pinnacle"
            rows.append({"market_family": market, "sportsbook": book,
                "historical_start_date": j.game_date.min() if ready else "",
                "historical_end_date": j.game_date.max() if ready else "",
                "market_rows_exact_lines": len(j) if ready else 0,
                "unique_games": j.game_pk.nunique() if ready else 0,
                "model_probability_rows_exact_lines": int(j.model_over_probability.notna().sum()) if ready else 0,
                "paired_over_under_rows": int((j.pinnacle_over_price.notna() & j.pinnacle_under_price.notna()).sum()) if ready else 0,
                "source_timestamp_rows": int(j.provider_snapshot_utc.notna().sum()) if ready else 0,
                "outcome_rows": int(j.final_total.notna().sum()) if ready else 0,
                "availability": "COMPLETE_EXISTING_EXACT_LINE_CHAIN" if ready else "MISSING_RETAINED_MARKET_AND_OR_MATCHED_MODEL_CHAIN",
                "missing_requirement": "" if ready else (
                    "historical pregame NoVig paired prices with timestamps and exact-line identity" if book == "NoVig" and market == "FULL_GAME_TOTAL" else
                    f"historical pregame {book} paired prices plus temporally valid Proppadia {market} probabilities at identical lines")})
    pd.DataFrame(rows).to_csv(OUT / "target_market_historical_availability.csv", index=False)
    readiness = []
    for market in ["FULL_GAME_TOTAL", "FIRST_5_TOTAL", "FULL_GAME_TEAM_TOTAL", "FIRST_5_TEAM_TOTAL"]:
        status = "HISTORICAL_SELECTION_REPLAY_PARTIAL" if market == "FULL_GAME_TOTAL" else "HISTORICAL_SELECTION_REPLAY_NOT_READY"
        reason = ("Pinnacle exact-line chain exists for 764 games, but NoVig and external Weight are unavailable" if market == "FULL_GAME_TOTAL" else
                  "No retained exact-line historical price plus temporally valid model-probability chain")
        readiness.append({"market_family": market, "decision": status, "reason": reason})
    pd.DataFrame(readiness).to_csv(OUT / "market_replay_readiness.csv", index=False)


def population():
    j = pd.read_csv(JOIN)
    schedule = pd.read_csv(GAME_POP, usecols=["game_pk", "scheduled_start_utc"])
    j = j.merge(schedule, on="game_pk", how="left", validate="one_to_one")
    j["scheduled_start_utc"] = pd.to_datetime(j.scheduled_start_utc, utc=True)
    j["provider_snapshot_utc"] = pd.to_datetime(j.provider_snapshot_utc, utc=True)
    rows = []
    for _, r in j.iterrows():
        for side in ["OVER", "UNDER"]:
            mp = float(r.model_over_probability if side == "OVER" else 1 - r.model_over_probability)
            price = float(r.pinnacle_over_price if side == "OVER" else r.pinnacle_under_price)
            raw = float(implied(price))
            paired_over, paired_under = float(implied(r.pinnacle_over_price)), float(implied(r.pinnacle_under_price))
            novig = (paired_over if side == "OVER" else paired_under) / (paired_over + paired_under)
            actual = "PUSH" if r.final_total == r.pinnacle_total_line else (
                "WIN" if (side == "OVER") == (r.final_total > r.pinnacle_total_line) else "LOSS")
            fair = float(fair_american(mp)); edge = 100 * (mp - raw); ev = 100 * (mp * float(decimal(price)) - 1)
            lead_days = (r.scheduled_start_utc - r.provider_snapshot_utc).total_seconds() / 86400
            reproducible = (0 <= lead_days <= 1 and 0 <= ev <= 8 and .01 <= edge <= 6 and price >= -400 and fair <= 100)
            rows.append({"game_pk": int(r.game_pk), "game_date": r.game_date, "market_family": "FULL_GAME_TOTAL",
                "side": side, "line": r.pinnacle_total_line, "sportsbook": "Pinnacle",
                "model_probability": mp, "model_fair_american_odds": fair, "book_american_odds": price,
                "raw_book_implied_probability": raw, "paired_no_vig_probability": novig,
                "edge_pct_raw": edge, "edge_pct_no_vig": 100 * (mp - novig), "ev_pct": ev,
                "weight": np.nan, "weight_status": "EXTERNAL_TOOL_WEIGHT_NOT_REPRODUCIBLE",
                "prediction_timestamp": r.requested_snapshot_utc, "sportsbook_timestamp": r.provider_snapshot_utc,
                "game_start_timestamp": r.scheduled_start_utc, "snapshot_lead_days": lead_days,
                "final_total": r.final_total, "outcome": actual,
                "passes_all_reproducible_filters": reproducible,
                "passes_exact_owner_contract": False,
                "exact_owner_contract_blocker": "WEIGHT_GATE_UNREPRODUCIBLE",
                "source_sha256": r.source_sha256})
    p = pd.DataFrame(rows).sort_values(["game_date", "game_pk", "side"]).reset_index(drop=True)
    p.to_csv(OUT / "tool_equivalent_selection_population.csv", index=False)
    selected = p[p.passes_all_reproducible_filters].copy()
    selected.to_csv(OUT / "tool_equivalent_selected_rows.csv", index=False)
    return p, selected


def pnl(row):
    if row.outcome == "PUSH": return 0.0
    if row.outcome == "LOSS": return -1.0
    return row.book_american_odds / 100 if row.book_american_odds > 0 else 100 / -row.book_american_odds


def summarize(g, candidate_count=None):
    settled = g[g.outcome.isin(["WIN", "LOSS"])]
    pnl_values = g.apply(pnl, axis=1) if len(g) else pd.Series(dtype=float)
    return {"candidate_propositions_before_filters": candidate_count if candidate_count is not None else len(g),
        "selected_propositions": len(g), "selection_rate": len(g) / candidate_count if candidate_count else np.nan,
        "wins": int((g.outcome == "WIN").sum()), "losses": int((g.outcome == "LOSS").sum()),
        "pushes": int((g.outcome == "PUSH").sum()),
        "win_rate_excluding_pushes": float((settled.outcome == "WIN").mean()) if len(settled) else np.nan,
        "flat_stake_roi": float(pnl_values.sum() / len(g)) if len(g) else np.nan,
        "mean_ev_pct": g.ev_pct.mean(), "median_ev_pct": g.ev_pct.median(),
        "mean_edge_pct": g.edge_pct_raw.mean(), "median_edge_pct": g.edge_pct_raw.median(),
        "average_book_price": g.book_american_odds.mean(), "average_model_fair_probability": g.model_probability.mean()}


def outcome_outputs(pop, selected):
    pd.DataFrame([{"sportsbook": "Pinnacle", "market_family": "FULL_GAME_TOTAL",
                   "selection_contract_status": "ALL_REPRODUCIBLE_FILTERS_EXCEPT_WEIGHT", **summarize(selected, len(pop))}]
                 ).to_csv(OUT / "selected_outcome_summary.csv", index=False)
    selected["ev_band"] = pd.cut(selected.ev_pct, [0, 1, 2, 3, 4, 6, 8],
        labels=[">0-1%", ">1-2%", ">2-3%", ">3-4%", ">4-6%", ">6-8%"], include_lowest=False)
    selected["edge_band"] = pd.cut(selected.edge_pct_raw, [.01, 1, 2, 3, 4, 6],
        labels=["0.01-1%", ">1-2%", ">2-3%", ">3-4%", ">4-6%"], include_lowest=True)
    ev_rows, edge_rows = [], []
    for band, g in selected.groupby("ev_band", observed=False): ev_rows.append({"ev_band": band, **summarize(g)})
    for band, g in selected.groupby("edge_band", observed=False): edge_rows.append({"edge_band": band, **summarize(g)})
    pd.DataFrame(ev_rows).to_csv(OUT / "selected_ev_bands.csv", index=False)
    pd.DataFrame(edge_rows).to_csv(OUT / "selected_edge_bands.csv", index=False)
    raw_qual = pop.edge_pct_raw.between(.01, 6, inclusive="both")
    novig_qual = pop.edge_pct_no_vig.between(.01, 6, inclusive="both")
    pd.DataFrame([{"population": "ALL_PINNACLE_FULL_GAME_TOTAL_PROPOSITIONS", "rows": len(pop),
        "raw_edge_qualifies": int(raw_qual.sum()), "novig_edge_qualifies": int(novig_qual.sum()),
        "both_qualify": int((raw_qual & novig_qual).sum()), "raw_only": int((raw_qual & ~novig_qual).sum()),
        "novig_only": int((~raw_qual & novig_qual).sum()), "neither": int((~raw_qual & ~novig_qual).sum()),
        "mean_raw_minus_novig_edge_pp": float((pop.edge_pct_raw - pop.edge_pct_no_vig).mean())}]
        ).to_csv(OUT / "raw_vs_novig_edge_comparison.csv", index=False)
    comparisons = []
    all_predictions = pop[(pop.model_fair_american_odds <= 100) & (pop.snapshot_lead_days.between(0, 1)) & (pop.book_american_odds >= -400)].copy()
    for label, g in [("ALL_MODEL_PREDICTIONS", all_predictions), ("OWNER_FILTERED_EXCEPT_UNREPRODUCIBLE_WEIGHT", selected)]:
        settled = g[g.outcome.isin(["WIN", "LOSS"])]
        y = (settled.outcome == "WIN").astype(int)
        comparisons.append({"sportsbook": "Pinnacle", "market_family": "FULL_GAME_TOTAL", "population": label,
            "rows": len(g), "wins": int((g.outcome == "WIN").sum()), "losses": int((g.outcome == "LOSS").sum()),
            "pushes": int((g.outcome == "PUSH").sum()), "win_rate_excluding_pushes": y.mean() if len(y) else np.nan,
            "flat_stake_roi": g.apply(pnl, axis=1).sum() / len(g) if len(g) else np.nan,
            "brier": brier_score_loss(y, settled.model_probability) if len(y) else np.nan,
            "mean_model_minus_raw_book_probability": (g.model_probability - g.raw_book_implied_probability).mean()})
    pd.DataFrame(comparisons).to_csv(OUT / "all_vs_selected_comparison.csv", index=False)
    stability = []
    selected["month"] = selected.game_date.str[:7]
    selected = selected.sort_values(["game_date", "game_pk", "side"]); selected["rolling_25_block"] = np.arange(len(selected)) // 25
    for typ, groups in [("month", selected.groupby("month")), ("rolling_25", selected.groupby("rolling_25_block")),
                        ("sportsbook", selected.groupby("sportsbook")), ("market_family", selected.groupby("market_family"))]:
        for value, g in groups: stability.append({"slice_type": typ, "slice_value": value, **summarize(g)})
    pd.DataFrame(stability).to_csv(OUT / "selected_temporal_stability.csv", index=False)


def report(pop, selected):
    summary = pd.read_csv(OUT / "selected_outcome_summary.csv").iloc[0]
    comp = pd.read_csv(OUT / "all_vs_selected_comparison.csv")
    allrow, selrow = comp.iloc[0], comp.iloc[1]
    edge = pd.read_csv(OUT / "raw_vs_novig_edge_comparison.csv").iloc[0]
    text = f"""# MLB Tool-Equivalent EV/Edge Selection Reconstruction v1

Experiment: `{EXPERIMENT}`

## Result

- EV and Edge formulas are supported by five of six supplied examples within 0.15 percentage points, consistent with hidden pre-display precision. The -111/-111 row cannot validate them without hidden precision; equal integer odds yield exactly zero.
- Exact formulas: `EV%=100*(model_probability*book_decimal_odds-1)` and `Edge%=100*(model_probability-raw_book_implied_probability)`.
- Owner filters were frozen exactly in `owner_filter_contract.json`. Fair odds <= +100 is equivalent to model probability >=50%; book odds >=-400 has no positive upper bound.
- Weight: `EXTERNAL_TOOL_WEIGHT_NOT_REPRODUCIBLE`. No internal substitute was invented, so the replay is explicitly partial.
- Replayable chain: Pinnacle full-game totals, {len(pop)//2} games/{len(pop)} side propositions, {pop.game_date.min()} through {pop.game_date.max()}. NoVig, F5 totals, full-game team totals, and F5 team totals are not replay-ready from retained exact-line data.
- Passing every reproducible filter except Weight: {len(selected)} of {len(pop)} propositions ({len(selected)/len(pop):.2%}); {summary.wins:.0f}-{summary.losses:.0f}-{summary.pushes:.0f}, win rate {summary.win_rate_excluding_pushes:.2%}, flat-stake ROI {summary.flat_stake_roi:.2%}.
- All {allrow.rows:.0f} model-selected sides: win rate {allrow.win_rate_excluding_pushes:.2%}, ROI {allrow.flat_stake_roi:.2%}, Brier {allrow.brier:.6f}. Partial selected subset: win rate {selrow.win_rate_excluding_pushes:.2%}, ROI {selrow.flat_stake_roi:.2%}, Brier {selrow.brier:.6f}.
- Raw-vs-no-vig Edge qualification: raw-only {edge.raw_only:.0f}, no-vig-only {edge.novig_only:.0f}, both {edge.both_qualify:.0f}; raw Edge averages {edge.mean_raw_minus_novig_edge_pp:.3f} pp lower because it retains vig.
- Month and rolling-25 results are reported separately. Any realized ROI is price-and-selection evidence, not proof that the model broadly beats Pinnacle.

## Conclusion

Prior every-prediction evaluation materially answered a different question from the owner's filtered interface. This reconstruction measures the reproducible portion of that selection process, but cannot establish the exact external-tool result until Weight semantics and matching NoVig/F5/team-total histories are available. No model was refit, no thresholds were tuned, no live selector or wager recommendation was created, and no ledger was mutated.
"""
    (OUT / "concise_mlb_tool_equivalent_selection_reconstruction_v1.md").write_text(text)


def hashes():
    files = sorted(p for p in OUT.iterdir() if p.is_file() and p.name != "reproducibility_hashes.sha256")
    (OUT / "reproducibility_hashes.sha256").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    example_validation(); write_contract(); availability()
    pop, selected = population(); outcome_outputs(pop, selected); report(pop, selected); hashes()
    print(json.dumps({"experiment": EXPERIMENT, "candidate_propositions": len(pop),
                      "selected_except_weight": len(selected), "exact_owner_selections": None,
                      "weight_status": "EXTERNAL_TOOL_WEIGHT_NOT_REPRODUCIBLE", "output": str(OUT.relative_to(ROOT))}, indent=2))


if __name__ == "__main__":
    main()
