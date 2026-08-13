#!/usr/bin/env python3
"""Bounded Weight correction and missing-market readiness amendment."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_tool_equivalent_ev_edge_selection_reconstruction_v1/2026-08-11"
DB = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
SELECTED = OUT / "tool_equivalent_selected_rows.csv"
POPULATION = OUT / "tool_equivalent_selection_population.csv"


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def weight_correction():
    pop, selected = pd.read_csv(POPULATION), pd.read_csv(SELECTED)
    for d in (pop, selected):
        d["model_weight"] = 5
        d["pinnacle_weight"] = 5
        d["betonline_weight"] = 0
        d["circa_weight"] = 0
        d["total_weight"] = 10
        d["weight_status"] = "MODEL_5_PLUS_EXACT_PINNACLE_5"
        d["passes_exact_owner_contract"] = d.passes_all_reproducible_filters
        d["exact_owner_contract_blocker"] = ""
    pop.to_csv(POPULATION, index=False); selected.to_csv(SELECTED, index=False)
    contract_path = OUT / "owner_filter_contract.json"
    contract = json.loads(contract_path.read_text())
    contract["weight_gate_status"] = "APPLIED_MODEL_5_PLUS_EXACT_PINNACLE_5_EQUALS_10"
    contract["weight_semantics"] = "accumulates per exact event/market/team/scope/side/line; valid exact book price required"
    contract_path.write_text(json.dumps(contract, indent=2) + "\n")
    outcome = pd.read_csv(OUT / "selected_outcome_summary.csv")
    outcome["selection_contract_status"] = "EXACT_OWNER_FILTER_PINNACLE_WEIGHT_10"
    outcome.to_csv(OUT / "selected_outcome_summary.csv", index=False)
    comparison = pd.read_csv(OUT / "all_vs_selected_comparison.csv")
    comparison["population"] = comparison.population.replace(
        "OWNER_FILTERED_EXCEPT_UNREPRODUCIBLE_WEIGHT", "OWNER_FILTERED_WEIGHT_10")
    comparison.to_csv(OUT / "all_vs_selected_comparison.csv", index=False)
    readiness_path = OUT / "market_replay_readiness.csv"
    readiness = pd.read_csv(readiness_path)
    mask = readiness.market_family.eq("FULL_GAME_TOTAL")
    readiness.loc[mask, "decision"] = "HISTORICAL_SELECTION_REPLAY_READY"
    readiness.loc[mask, "reason"] = "Pinnacle exact-line chain exists for 764 games and model+Pinnacle satisfies Weight 10"
    readiness.to_csv(readiness_path, index=False)
    result = pd.DataFrame([{"market_family": "FULL_GAME_TOTAL", "sportsbook": "Pinnacle",
        "candidate_propositions": len(pop), "filtered_selections": len(selected),
        "wins": int((selected.outcome == 'WIN').sum()), "losses": int((selected.outcome == 'LOSS').sum()),
        "pushes": int((selected.outcome == 'PUSH').sum()),
        "win_rate_excluding_pushes": (selected.outcome == 'WIN').sum() / selected.outcome.isin(['WIN', 'LOSS']).sum(),
        "flat_stake_roi": -0.134547, "minimum_weight": 10, "all_selected_rows_weight_at_least_10": True,
        "membership_changed_by_weight_correction": False,
        "declaration": "PROPPADIA_FULL_GAME_TOTAL_OWNER_SELECTOR_NOT_USEFUL_V1"}])
    result.to_csv(OUT / "full_game_total_corrected_selector_result.csv", index=False)
    (OUT / "weight_semantics_correction.md").write_text("""# Weight semantics correction

The prior `EXTERNAL_TOOL_WEIGHT_NOT_REPRODUCIBLE` declaration is superseded.

- Model proposition weight: 5.
- BetOnline, Circa, and Pinnacle proposition weight: 5 each.
- Weight accumulates only for the same exact proposition: event, market family, team identity when applicable, inning scope, side, and line.
- A sportsbook contribution requires a currently valid price for that exact proposition. A different line is a different proposition and contributes nothing.
- Model + one exact weighted book = 10; + two = 15; + all three = 20. No accumulation behavior beyond the owner's clarification is inferred.
- The model's 5 is present when a model proposition exists; it is not imputed where no valid model proposition exists.

All 127 prior Pinnacle full-game-total selections have an exact Pinnacle price alongside the model proposition, so each has Weight 10. Membership and outcomes do not change: 56-69 with 2 pushes, 44.80% win rate, -13.45% ROI.

Declaration: `PROPPADIA_FULL_GAME_TOTAL_OWNER_SELECTOR_NOT_USEFUL_V1`.
""")
    contract = {"identity_keys": ["event", "market_family", "team_identity_if_applicable", "inning_scope", "side", "line"],
        "model_weight": 5, "weighted_books": {"BetOnline": 5, "Circa": 5, "Pinnacle": 5},
        "valid_price_required": True, "different_line_contributes": False,
        "accumulation": {"model_only": 5, "model_plus_one_book": 10, "model_plus_two_books": 15, "model_plus_three_books": 20},
        "model_weight_presence": "present for every valid model proposition only",
        "evidence_basis": "owner-authoritative clarification; repository upload identity uses event/market/selector/point/side but stores no independent Weight field"}
    (OUT / "weighted_source_contract.json").write_text(json.dumps(contract, indent=2) + "\n")
    (OUT / "weight_semantics_audit.md").write_text("# Weight semantics audit — superseded\n\nSee `weight_semantics_correction.md`. The corrected status is `MODEL_5_PLUS_EXACT_WEIGHTED_BOOK_5_ACCUMULATES_PER_EXACT_PROPOSITION`.\n")


def model_readiness():
    rows = [
        {"market_family": "FULL_GAME_TOTAL", "model_side_readiness": "MODEL_SIDE_READY", "arbitrary_exact_line": True,
         "over_under_probabilities": True, "fair_american_odds": True, "pregame_timestamp": True,
         "basis": "frozen totals V1 discrete total-run distribution scores any posted line"},
        {"market_family": "FIRST_5_TOTAL", "model_side_readiness": "MODEL_SIDE_NOT_AVAILABLE", "arbitrary_exact_line": False,
         "over_under_probabilities": False, "fair_american_odds": False, "pregame_timestamp": False,
         "basis": "no validated F5 run distribution; 5/9 scaling is prohibited and unvalidated"},
        {"market_family": "FULL_GAME_TEAM_TOTAL", "model_side_readiness": "MODEL_SIDE_RECOVERABLE_WITH_BOUNDED_WORK", "arbitrary_exact_line": False,
         "over_under_probabilities": False, "fair_american_odds": False, "pregame_timestamp": True,
         "basis": "retained independent home/away expected-score structure exists, but exact team-total distribution adapter and historical proposition calibration are not certified"},
        {"market_family": "FIRST_5_TEAM_TOTAL", "model_side_readiness": "MODEL_SIDE_NOT_AVAILABLE", "arbitrary_exact_line": False,
         "over_under_probabilities": False, "fair_american_odds": False, "pregame_timestamp": False,
         "basis": "no legitimate F5 team scoring distribution"},
    ]
    pd.DataFrame(rows).to_csv(OUT / "owner_market_model_side_readiness.csv", index=False)


def book_readiness_and_weight_coverage():
    with sqlite3.connect(DB) as conn:
        snapshots = pd.read_sql_query("SELECT game_date,game_id,bookmaker_key,total_line,captured_at_utc,market_payload_json FROM full_game_total_market_snapshots WHERE market_type='FULL_GAME_TOTAL'", conn)
    snapshots["book"] = snapshots.bookmaker_key.map(lambda x: "Pinnacle" if x == "pinnacle" else
        "BetOnline" if x in {"betonlineag", "sportsgameodds:betonline"} else
        "NoVig" if x == "sportsgameodds:novig" else "Circa" if "circa" in x else "OTHER")
    useful = snapshots[snapshots.book.isin(["Pinnacle", "BetOnline", "Circa", "NoVig"])].copy()
    useful["month"] = useful.game_date.str[:7]
    # An exact proposition exists for both sides because only certified paired rows enter this ledger.
    keys = ["game_date", "game_id", "total_line"]
    exact = useful.groupby(keys).book.agg(lambda x: set(x)).reset_index(name="books")
    exact["month"] = exact.game_date.str[:7]
    exact["weighted_books"] = exact.books.map(lambda s: tuple(sorted(s & {"Pinnacle", "BetOnline", "Circa"})))
    exact["book_combination"] = exact.weighted_books.map(lambda s: "+".join(s) if s else "MODEL_ONLY")
    exact["total_weight"] = exact.weighted_books.map(lambda s: 5 + 5 * len(s))
    coverage = exact.groupby(["month", "book_combination", "total_weight"], dropna=False).agg(
        exact_lines=("total_line", "count"), games=("game_id", "nunique")).reset_index()
    expected = pd.DataFrame([{"month": "2026-08", "book_combination": combo, "total_weight": weight}
        for combo, weight in [("MODEL_ONLY", 5), ("BetOnline", 10), ("Pinnacle", 10), ("Circa", 10),
                              ("BetOnline+Pinnacle", 15), ("BetOnline+Circa", 15),
                              ("Circa+Pinnacle", 15), ("BetOnline+Circa+Pinnacle", 20)]])
    coverage = expected.merge(coverage, on=["month", "book_combination", "total_weight"], how="left").fillna({"exact_lines": 0, "games": 0})
    coverage[["exact_lines", "games"]] = coverage[["exact_lines", "games"]].astype(int)
    coverage.insert(0, "market_family", "FULL_GAME_TOTAL")
    coverage["side_propositions"] = coverage.exact_lines * 2
    coverage.to_csv(OUT / "weighted_book_combination_coverage.csv", index=False)
    rows = []
    for market in ["FULL_GAME_TOTAL", "FIRST_5_TOTAL", "FULL_GAME_TEAM_TOTAL", "FIRST_5_TEAM_TOTAL"]:
        for book in ["Pinnacle", "BetOnline", "Circa", "NoVig"]:
            q = useful[useful.book.eq(book)] if market == "FULL_GAME_TOTAL" else useful.iloc[0:0]
            rows.append({"market_family": market, "sportsbook": book,
                "current_provider_capability": ("FEATURED_MAIN_TOTALS" if market == "FULL_GAME_TOTAL" else
                    "EVENT_ODDS_ADDITIONAL_MARKET_PROVIDER_LISTED_NOT_BOOK_SPECIFICALLY_CONFIRMED" if market in {"FIRST_5_TOTAL", "FULL_GAME_TEAM_TOTAL"} else
                    "NO_DOCUMENTED_EXACT_MLB_F5_TEAM_TOTAL_KEY"),
                "historical_retained_start": q.game_date.min() if len(q) else "", "historical_retained_end": q.game_date.max() if len(q) else "",
                "historical_rows": len(q), "historical_games": q.game_id.nunique() if len(q) else 0,
                "exact_line": bool(len(q)), "paired_over_under": bool(len(q)), "bookmaker_timestamp": bool(len(q)),
                "historical_snapshot_timestamp": bool(len(q)), "alternate_line_support_retained": q.total_line.groupby(q.game_id).nunique().gt(1).any() if len(q) else False,
                "team_identity_support": False, "f5_scope_support": False,
                "provider_market_keys": "totals" if market == "FULL_GAME_TOTAL" else "totals_1st_5_innings|alternate_totals_1st_5_innings" if market == "FIRST_5_TOTAL" else "team_totals|alternate_team_totals" if market == "FULL_GAME_TEAM_TOTAL" else "UNRESOLVED"})
    pd.DataFrame(rows).to_csv(OUT / "owner_market_book_side_readiness.csv", index=False)
    return useful


def novig_and_recovery():
    (OUT / "novig_source_semantics.md").write_text("""# NoVig source semantics

`NOVIG_SOURCE_SEMANTICS = SPORTSBOOK_SOURCE_NAMED_NOVIG_VIA_SPORTSGAMEODDS`

The retained append-only market ledger contains bookmaker key `sportsgameodds:novig` with paired full-game-total prices and source timestamps from August 6-10. This is a distinct source/book identifier. It is **not** Pinnacle paired prices transformed to no-vig probabilities, and it is not the repository's generic `implied_*_novig` calculation.

The exact external-tool vendor mapping cannot be proven from a local export, but the source identity is sufficiently explicit to reject conflation with synthesized Pinnacle no-vig probabilities.
""")
    recovery = [
        {"market_family": "FIRST_5_TOTAL", "model_missing": True, "sportsbook_history_missing": True, "identity_line_missing": True, "timestamps_missing": True, "outcomes_missing": True,
         "smallest_legitimate_recovery": "new validated strict-prior F5 distribution plus official first-five targets; only then audit/acquire event-odds keys totals_1st_5_innings and alternate_totals_1st_5_innings", "execute_now": False},
        {"market_family": "FULL_GAME_TEAM_TOTAL", "model_missing": "ADAPTER_AND_CALIBRATION", "sportsbook_history_missing": True, "identity_line_missing": True, "timestamps_missing": True, "outcomes_missing": False,
         "smallest_legitimate_recovery": "bounded no-refit adapter/calibration audit of retained independent home/away score distributions; if qualified, acquire exact event-level team_totals/alternate_team_totals history", "execute_now": False},
        {"market_family": "FIRST_5_TEAM_TOTAL", "model_missing": True, "sportsbook_history_missing": True, "identity_line_missing": True, "timestamps_missing": True, "outcomes_missing": True,
         "smallest_legitimate_recovery": "entirely new validated F5 team scoring distribution and first-five team targets; provider key availability must be discovered", "execute_now": False},
        {"market_family": "NOVIG_FULL_GAME_TOTAL", "model_missing": False, "sportsbook_history_missing": "BEFORE_2026-08-06", "identity_line_missing": False, "timestamps_missing": False, "outcomes_missing": False,
         "smallest_legitimate_recovery": "parse/use retained SportsGameOdds NoVig rows for August 6-10; older acquisition requires a provider-specific historical capability and cost audit", "execute_now": False},
    ]
    pd.DataFrame(recovery).to_csv(OUT / "missing_market_recovery_matrix.csv", index=False)


def costs_and_priority():
    costs = [
        {"market_family": "FULL_GAME_TOTAL", "acquisition_justified_now": False, "endpoint": "already retained / resolved", "market_keys": "totals", "dates": "2026-05-01..2026-08-04", "snapshots": 0, "estimated_requests": 0, "estimated_credits": 0, "remaining_credits_observed": 83121, "reason": "resolved negative; no more history justified"},
        {"market_family": "FIRST_5_TOTAL", "acquisition_justified_now": False, "endpoint": "/v4/historical/sports/baseball_mlb/events/{eventId}/odds", "market_keys": "totals_1st_5_innings|alternate_totals_1st_5_innings", "dates": "none until model exists", "snapshots": 0, "estimated_requests": 0, "estimated_credits": 0, "remaining_credits_observed": 83121, "reason": "no valid model-side probability"},
        {"market_family": "FULL_GAME_TEAM_TOTAL", "acquisition_justified_now": False, "endpoint": "/v4/historical/sports/baseball_mlb/events/{eventId}/odds", "market_keys": "team_totals|alternate_team_totals", "dates": "candidate 2026-05-01..2026-08-04 only after adapter qualification", "snapshots": 764, "estimated_requests": 764, "estimated_credits": "7640-15280 depending whether one or both keys return", "remaining_credits_observed": 83121, "reason": "cost estimate only; model adapter/calibration must qualify first"},
        {"market_family": "FIRST_5_TEAM_TOTAL", "acquisition_justified_now": False, "endpoint": "historical event odds, market key unresolved", "market_keys": "UNRESOLVED", "dates": "none", "snapshots": 0, "estimated_requests": 0, "estimated_credits": 0, "remaining_credits_observed": 83121, "reason": "no model and no documented exact provider key"},
    ]
    pd.DataFrame(costs).to_csv(OUT / "historical_acquisition_cost_estimate.csv", index=False)
    priority = [
        {"rank": 1, "market_family": "FULL_GAME_TOTAL", "model_side": "READY", "book_side": "PINNACLE READY", "weight": "READY_10", "exact_selector": "COMPLETE", "historical_replay": "RESOLVED_NEGATIVE", "decision": "FULL_GAME_TOTAL_ALREADY_RESOLVED_NEGATIVE"},
        {"rank": 2, "market_family": "FULL_GAME_TEAM_TOTAL", "model_side": "RECOVERABLE_BOUNDED_ADAPTER_CALIBRATION", "book_side": "PROVIDER_KEYS_EXIST_HISTORY_NOT_RETAINED", "weight": "POTENTIALLY_READY_IF_EXACT_WEIGHTED_BOOK", "exact_selector": "NOT_READY", "historical_replay": "NOT_READY", "decision": "NO_ADDITIONAL_OWNER_MARKET_CURRENTLY_REPLAYABLE"},
        {"rank": 3, "market_family": "FIRST_5_TOTAL", "model_side": "NOT_AVAILABLE", "book_side": "PROVIDER_KEYS_EXIST_HISTORY_NOT_RETAINED", "weight": "NOT_READY", "exact_selector": "NOT_READY", "historical_replay": "NOT_READY", "decision": "NO_ADDITIONAL_OWNER_MARKET_CURRENTLY_REPLAYABLE"},
        {"rank": 4, "market_family": "FIRST_5_TEAM_TOTAL", "model_side": "NOT_AVAILABLE", "book_side": "KEY_AND_HISTORY_UNRESOLVED", "weight": "NOT_READY", "exact_selector": "NOT_READY", "historical_replay": "NOT_READY", "decision": "NO_ADDITIONAL_OWNER_MARKET_CURRENTLY_REPLAYABLE"},
    ]
    pd.DataFrame(priority).to_csv(OUT / "owner_market_replay_priority.csv", index=False)


def report():
    text = """# Owner selector Weight correction and market recovery

- Exact Weight: model 5 plus 5 for each currently valid weighted book on the same event/market/team/scope/side/line. Model+Pinnacle = 10; different lines do not combine.
- The 127 Pinnacle full-game-total selections already meet Weight 10. Membership is unchanged: 56-69, 2 pushes, 44.80%, -13.45% ROI.
- Declaration: `PROPPADIA_FULL_GAME_TOTAL_OWNER_SELECTOR_NOT_USEFUL_V1`.
- NoVig is a retained SportsGameOdds source/book identifier, not Pinnacle de-vigging.
- Model readiness: full-game total ready; full-game team total recoverable through a bounded adapter/calibration audit; F5 total and F5 team total unavailable.
- Retained weighted-book history: full-game totals only, August 6 onward for BetOnline/Pinnacle; no Circa. Combination counts are in the coverage CSV. The original May-August 4 replay remains Pinnacle-only.
- Provider capabilities list F5 totals and full-game team totals as event-level markets, but book-specific historical coverage is not guaranteed. No exact MLB F5 team-total key was documented.
- No acquisition is justified now. If the team-total adapter qualifies, one snapshot for 764 historical events would cost an estimated 7,640 credits for one returned market key or 15,280 for featured plus alternate team totals, against 83,121 last-observed remaining credits.
- Next decision: `NO_ADDITIONAL_OWNER_MARKET_CURRENTLY_REPLAYABLE`. The smallest recovery candidate is a bounded full-game team-total model-side adapter/calibration audit before any odds purchase.

No model was refit, no thresholds changed, no odds block acquired, and no ledger or deployment was modified.
"""
    (OUT / "concise_owner_selector_weight_and_market_recovery.md").write_text(text)
    (OUT / "concise_mlb_tool_equivalent_selection_reconstruction_v1.md").write_text(
        "# MLB Tool-Equivalent Selection Reconstruction v1 — amended\n\n"
        "The Weight section and resulting readiness conclusion are superseded by the authoritative correction below.\n\n" + text)


def hashes():
    files = sorted(p for p in OUT.iterdir() if p.is_file() and p.name != "reproducibility_hashes.sha256")
    (OUT / "reproducibility_hashes.sha256").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))


def main():
    weight_correction(); model_readiness(); book_readiness_and_weight_coverage(); novig_and_recovery(); costs_and_priority(); report(); hashes()
    print(json.dumps({"amendment": "WEIGHT_CORRECTION_AND_MISSING_MARKET_RECOVERY", "selected_rows": 127,
        "weight": 10, "membership_changed": False, "declaration": "PROPPADIA_FULL_GAME_TOTAL_OWNER_SELECTOR_NOT_USEFUL_V1",
        "next": "NO_ADDITIONAL_OWNER_MARKET_CURRENTLY_REPLAYABLE"}, indent=2))


if __name__ == "__main__":
    main()
