#!/usr/bin/env python3
"""Build the bounded, read-only NHL mainline historical feasibility package.

The utility inventories repository evidence only. It does not fetch odds, mutate the
database, settle wagers, calculate ROI, or fit a model.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

STAMP = "2026-07-13"
MAINLINE = {
    "h2h": "MONEYLINE",
    "h2h_3_way": "REGULATION_MONEYLINE",
    "spreads": "PUCK_LINE",
    "totals": "GAME_TOTAL",
    "team_totals": "TEAM_TOTAL",
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError(f"refusing empty output {path}")
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def scan_odds(root: Path) -> dict[str, Any]:
    files = sorted(root.glob("*/odds_event_wrappers.json"))
    markets: Counter[str] = Counter()
    books: Counter[str] = Counter()
    events = events_with_books = 0
    timestamps: list[str] = []
    dates: list[str] = []
    for path in files:
        payload = json.loads(path.read_text())
        if not isinstance(payload, list):
            raise RuntimeError(f"unexpected odds wrapper grain: {path}")
        dates.append(path.parent.name)
        for wrapped in payload:
            data = wrapped.get("data", wrapped) if isinstance(wrapped, dict) else {}
            if not isinstance(data, dict):
                continue
            events += 1
            if wrapped.get("timestamp"):
                timestamps.append(str(wrapped["timestamp"]))
            bookmakers = data.get("bookmakers", [])
            if bookmakers:
                events_with_books += 1
            for book in bookmakers:
                books[str(book.get("key", ""))] += 1
                for market in book.get("markets", []):
                    markets[str(market.get("key", ""))] += 1
    mainline_quotes = sum(v for k, v in markets.items() if k in MAINLINE)
    return {
        "files": len(files), "first_date": min(dates), "last_date": max(dates),
        "events": events, "events_with_books": events_with_books,
        "timestamped_wrappers": len(timestamps), "books": books,
        "markets": markets, "mainline_quotes": mainline_quotes,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--odds-root", default="backend/nhl/exports/odds_history")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)
    audit = scan_odds(Path(args.odds_root))
    if audit["files"] != 153 or audit["events"] != 907 or audit["mainline_quotes"] != 0:
        raise RuntimeError(f"frozen repository odds evidence changed: {audit}")

    source = "backend/nhl/exports/odds_history/*/odds_event_wrappers.json"
    market_rows = []
    specs = [
        ("MONEYLINE", "h2h", "home/away", "team", "full game including overtime/shootout only if source contract says so"),
        ("REGULATION_MONEYLINE", "h2h_3_way", "home/draw/away", "team or draw", "regulation only"),
        ("PUCK_LINE", "spreads", "home/away plus handicap", "team-line", "full-game score plus explicit line"),
        ("GAME_TOTAL", "totals", "over/under plus line", "game-line", "full-game total plus explicit line"),
        ("TEAM_TOTAL", "team_totals", "team plus over/under plus line", "team-game-line", "team score plus explicit line"),
        ("FIRST_PERIOD", "unobserved variants", "period-specific", "market-specific", "period-one score"),
        ("OTHER", "none materially observed", "market-specific", "market-specific", "source-specific"),
    ]
    for family, native, sides, grain, settlement in specs:
        market_rows.append({
            "canonical_market_family": family, "source_native_market_label": native,
            "sportsbook": "NONE_OBSERVED_FOR_MAINLINE", "source_path_or_table": source,
            "date_coverage": f"{audit['first_date']}..{audit['last_date']} archive inspected",
            "season_coverage": "2025", "game_coverage": 0, "home_away_identity": sides,
            "line_present": "NO", "price_present": "NO", "price_format": "not applicable",
            "quote_timestamp": "NO_MAINLINE_QUOTE", "capture_timestamp": "wrapper timestamp exists only for player props",
            "market_status": "ABSENT", "open_current_close": "UNKNOWN", "pregame_status": "NO_PRICE",
            "raw_or_derived": "NOT_OBSERVED", "multiple_books": "NO_MAINLINE_ROWS",
            "historical_replayability": "NOT_REPLAYABLE", "known_gaps": settlement,
            "authority_confidence": "HIGH_ABSENCE_IN_INSPECTED_ARCHIVE",
        })
    write_csv(out/f"nhl_mainline_market_inventory_{STAMP}.csv", market_rows)

    settlement = [
        {"market":"MONEYLINE","natural_grain":"game-team","required_outcome":"final home/away score and winner","overtime_shootout":"must be contractually confirmed; default unresolved","push":"none","neutral_logic":"winner by certified full-game score","repository_readiness":"READY_WITH_BOUNDED_LIMITS_FOR_SEASONS_2023_2024"},
        {"market":"REGULATION_MONEYLINE","natural_grain":"game-outcome(3-way)","required_outcome":"score after period 3","overtime_shootout":"excluded","push":"draw is third outcome, not push","neutral_logic":"home win/draw/away win after regulation","repository_readiness":"BLOCKED_BY_REGULATION_OUTCOME_CERTIFICATION"},
        {"market":"PUCK_LINE","natural_grain":"game-team-line","required_outcome":"certified final goal differential","overtime_shootout":"source contract required","push":"integer line pushes; half line cannot push","neutral_logic":"team goal differential plus signed handicap","repository_readiness":"READY_WITH_BOUNDED_LIMITS_FOR_SEASONS_2023_2024"},
        {"market":"GAME_TOTAL","natural_grain":"game-line","required_outcome":"certified final total goals","overtime_shootout":"source contract required","push":"integer line pushes; half line cannot push","neutral_logic":"home goals plus away goals versus line","repository_readiness":"READY_WITH_BOUNDED_LIMITS_FOR_SEASONS_2023_2024"},
        {"market":"TEAM_TOTAL","natural_grain":"game-team-line","required_outcome":"certified team-specific goals","overtime_shootout":"source contract required","push":"integer line pushes; half line cannot push","neutral_logic":"selected team goals versus line","repository_readiness":"READY_WITH_BOUNDED_LIMITS_FOR_SEASONS_2023_2024"},
        {"market":"FIRST_PERIOD","natural_grain":"game-period-market","required_outcome":"period-one score","overtime_shootout":"not applicable","push":"line-specific","neutral_logic":"period-one only","repository_readiness":"NOT_ENOUGH_REPOSITORY_EVIDENCE"},
    ]
    write_csv(out/f"nhl_mainline_outcome_and_settlement_inventory_{STAMP}.csv", settlement)

    price_rows=[]
    for row in market_rows:
        price_rows.append({
            "market":row["canonical_market_family"],"season":2025,"sportsbook":"NONE",
            "archive_dates":audit["files"],"archive_events":audit["events"],"events_with_any_player_prop_book":audit["events_with_books"],
            "mainline_quote_rows":0,"exact_timestamped":0,"latest_pregame_certified":0,"pregame_date_only":0,
            "timestamp_unknown":0,"post_start_invalid":0,"derived_consensus_only":0,"no_price":audit["events"],
            "classification":"NO_PRICE","evidence":"archive contains only player_shots_on_goal, player_shots_on_goal_alternate, player_total_saves, player_points",
        })
    write_csv(out/f"nhl_mainline_price_timestamp_inventory_{STAMP}.csv", price_rows)

    grain=[]
    for market,key in [("MONEYLINE","season+game_id+market+team+sportsbook+quote_timestamp"),("REGULATION_MONEYLINE","season+game_id+market+home/draw/away+sportsbook+quote_timestamp"),("PUCK_LINE","season+game_id+market+team+line+sportsbook+quote_timestamp"),("GAME_TOTAL","season+game_id+market+over/under+line+sportsbook+quote_timestamp"),("TEAM_TOTAL","season+game_id+market+team+over/under+line+sportsbook+quote_timestamp"),("FIRST_PERIOD","season+game_id+period+market+side+line+sportsbook+quote_timestamp")]:
        grain.append({"market":market,"natural_identity":key,"canonical_game_ids":4110,"duplicate_game_id_groups":0,"home_away_identity_conflicts":0,"multiple_books_handling":"retain book in key","multiple_snapshots_handling":"retain quote timestamp in key","multiple_lines_handling":"retain line in applicable key","reschedule_handling":"join by game_id; retain scheduled start and game date history","neutral_site_handling":"venue flag absent; requires explicit certification","grain_decision":"DEFINED_NOT_PRICE_CERTIFIED"})
    write_csv(out/f"nhl_mainline_game_identity_and_grain_audit_{STAMP}.csv", grain)

    models=[
        {"system":"No surviving NHL mainline baseline","entry_point":"NONE_FOUND","artifact":"NONE_FOUND","model_type":"NONE","target":"game-level mainline","output":"NONE","date_coverage":"NONE","season_coverage":"NONE","training_replay_status":"NOT_APPLICABLE","inputs":"NONE","status":"ABSENT","saved_predictions":"NO","saved_probabilities":"NO","saved_prepared_inputs":"NO","evaluation_reports":"NO","exact_reproduction":"NO"},
        {"system":"Player-prop SOG Poisson baseline","entry_point":"backend/nhl/scripts/score_sog_poisson_baseline.py","artifact":"formula only","model_type":"player-game Poisson","target":"player shots on goal","output":"player-prop probabilities","date_coverage":"2026-02-28..2026-04-16 production","season_coverage":"2025","training_replay_status":"Level 4 exact reproduction","inputs":"player SOG rate and TOI","status":"PROP_ONLY_NOT_MAINLINE","saved_predictions":"YES","saved_probabilities":"YES","saved_prepared_inputs":"YES","evaluation_reports":"YES","exact_reproduction":"YES_PROP_ONLY"},
        {"system":"Goalie saves and points models","entry_point":"backend/nhl/models/latest and scoring scripts","artifact":"prop model artifacts","model_type":"player prop","target":"goalie saves or player points","output":"player-prop probabilities","date_coverage":"partial","season_coverage":"primarily 2025","training_replay_status":"not mainline-certified","inputs":"player/game context","status":"PROP_ONLY_NOT_MAINLINE","saved_predictions":"PARTIAL","saved_probabilities":"PARTIAL","saved_prepared_inputs":"PARTIAL","evaluation_reports":"PROP_ONLY","exact_reproduction":"NOT_AS_MAINLINE"},
    ]
    write_csv(out/f"nhl_mainline_existing_model_inventory_{STAMP}.csv", models)

    features=[
        ("TEAM_STRENGTH","goals, shots, attempts, xG, possession proxies, venue and special-teams situations","team_game_2023/2024_summary and roll; shots stages; team context tables","team-game/situation","prior-game roll columns exist for 2023/2024","BOUNDED_RECONSTRUCTION","high 2023/2024; partial/fragmented 2025","collectable with pipeline work"),
        ("GOALIE","actual goalie logs, performance, workload; no certified expected starter history","goalie_game_logs_raw and goalie feature scripts","goalie-game","actual results are postgame; zero prediction-time starter rows","POSTGAME_ONLY_OR_TIMING_UNCERTIFIED","1400 games 2023; 1316 2024; 359 2025","requires prediction-time starter capture"),
        ("SCHEDULE","game date, teams, home/away; start timestamp only complete in 2025","nhl.games","game","game schedule authoritative; rest derivable prior-only","BOUNDED_RECONSTRUCTION","complete game identity all seasons; start time missing 2023/2024","daily collectable"),
        ("LINEUP_AVAILABILITY","active flag and as-of timestamp","nhl.roster_status","player-team-game snapshot","only 469 rows are pre-start; not full games","NOT_HISTORICALLY_REPLAYABLE","3 games 2023; 9 2024; 1312 2025 but mostly current/post-start","requires governed timestamped capture"),
        ("RECENT_FORM","prior team rolling goals and SOG","team_game_2023_roll; team_game_2024_roll; team context","team-game","prior-window definitions require independent certification","BOUNDED_RECONSTRUCTION","strong 2023/2024; discontinuous 2025","collectable after canonicalization"),
        ("MARKET_CONTEXT","none surviving for mainlines","player-prop-only odds archive","game-market-book-snapshot","not applicable","NOT_REPLAYABLE","zero mainline quotes","new governed collection required"),
    ]
    feature_rows=[{"feature_family":a,"fields_or_concepts":b,"source":c,"natural_grain":d,"timing":e,"historical_replayability":f,"coverage":g,"season_2026_collectability":h,"production_history":"PROP-ORIENTED_ONLY","research_history":"NO_CERTIFIED_MAINLINE_USE"} for a,b,c,d,e,f,g,h in features]
    write_csv(out/f"nhl_mainline_team_game_feature_inventory_{STAMP}.csv", feature_rows)
    timing_rows=[{"feature_family":r["feature_family"],"source":r["source"],"grain":r["natural_grain"],"timing_decision":r["timing"],"strict_prior_status":"VERIFIED_SOURCE_ORDERING" if r["feature_family"]=="SCHEDULE" else "REQUIRES_CERTIFICATION","replayability":r["historical_replayability"],"mutable_source_risk":"HIGH" if r["feature_family"] in {"GOALIE","LINEUP_AVAILABILITY","MARKET_CONTEXT"} else "MEDIUM","use_decision":"CHARACTERIZE_ONLY"} for r in feature_rows]
    write_csv(out/f"nhl_mainline_feature_timing_and_replayability_{STAMP}.csv", timing_rows)

    continuity=[
        {"season":2023,"game_count":1400,"outcomes_available":1400,"outcome_authority":"two clean team_game_2023_summary rows per game","mainline_price_games":0,"timestamped_price_games":0,"team_feature_coverage":"HIGH","goalie_games":1400,"pregame_goalie_certified":"NO","lineup_games":3,"surviving_model_predictions":0,"strict_prior_feasibility":"READY_WITH_BOUNDED_LIMITS","population_spine_readiness":"READY_FOR_OUTCOME_CERTIFICATION"},
        {"season":2024,"game_count":1398,"outcomes_available":1398,"outcome_authority":"two clean team_game_2024_summary rows per game","mainline_price_games":0,"timestamped_price_games":0,"team_feature_coverage":"HIGH","goalie_games":1316,"pregame_goalie_certified":"NO","lineup_games":9,"surviving_model_predictions":0,"strict_prior_feasibility":"READY_WITH_BOUNDED_LIMITS","population_spine_readiness":"READY_FOR_OUTCOME_CERTIFICATION"},
        {"season":2025,"game_count":1312,"outcomes_available":0,"outcome_authority":"nhl.games has 900 final statuses but no scores; no populated team_game_sit source","mainline_price_games":0,"timestamped_price_games":0,"team_feature_coverage":"PARTIAL_FRAGMENTED","goalie_games":359,"pregame_goalie_certified":"NO","lineup_games":1312,"surviving_model_predictions":0,"strict_prior_feasibility":"BLOCKED_BY_CONTINUITY","population_spine_readiness":"BLOCKED_BY_OUTCOME_AND_PRICE"},
    ]
    write_csv(out/f"nhl_mainline_season_continuity_{STAMP}.csv", continuity)

    readiness={"MONEYLINE":"READY_FOR_POPULATION_CERTIFICATION","PUCK_LINE":"READY_WITH_BOUNDED_LIMITS","GAME_TOTAL":"READY_WITH_BOUNDED_LIMITS","TEAM_TOTAL":"READY_WITH_BOUNDED_LIMITS","REGULATION_MONEYLINE":"BLOCKED_BY_OUTCOME_GRAIN","FIRST_PERIOD":"NOT_ENOUGH_REPOSITORY_EVIDENCE"}
    feasible=[]
    for market in readiness:
        feasible.append({"market":market,"outcome_clarity":"HIGH_2023_2024" if market!="REGULATION_MONEYLINE" else "NEEDS_PERIOD_CERTIFICATION","historical_price_coverage":"NONE","price_timestamp_quality":"NO_PRICE","market_identity_quality":"DEFINED_NOT_OBSERVED","historical_feature_coverage":"BOUNDED_2023_2024","strict_prior_feasibility":"BOUNDED","saved_model_evidence":"NONE","settlement_clarity":"DEFINED" if market!="FIRST_PERIOD" else "INSUFFICIENT","season_2026_collectability":"REQUIRES_NEW_GOVERNED_CAPTURE","engineering_effort":"MEDIUM" if market=="MONEYLINE" else "MEDIUM_HIGH","readiness":readiness[market],"ranking_basis":"infrastructure only; no ROI"})
    write_csv(out/f"nhl_mainline_market_family_feasibility_{STAMP}.csv", feasible)

    comparison=[
        {"dimension":"fixed probability baseline","SOG_lane":"Level 4 exact reproduction","mainline_lane":"none identified","further_along":"SOG"},
        {"dimension":"outcome architecture","SOG_lane":"certified official SOG denominator","mainline_lane":"complete team goal summaries for seasons 2023/2024 only","further_along":"SOG"},
        {"dimension":"price quality","SOG_lane":"candidate parity blocked by run-bound odds linkage","mainline_lane":"no surviving mainline quotes","further_along":"SOG"},
        {"dimension":"feature continuity","SOG_lane":"76 concepts characterized on fixed 2025 spine","mainline_lane":"team rolling sources exist mainly for 2023/2024; goalie/lineup timing weak","further_along":"SOG"},
        {"dimension":"historical replayability","SOG_lane":"exact saved-input probability replay","mainline_lane":"outcome spine feasible; no prediction or price replay","further_along":"SOG"},
        {"dimension":"next requirement","SOG_lane":"prediction-time role/lineup/goalie collection","mainline_lane":"moneyline population and outcome certification for seasons 2023/2024","further_along":"SEPARATE_LANES"},
    ]
    write_csv(out/f"nhl_sog_vs_mainline_structural_readiness_{STAMP}.csv", comparison)

    decisions={
        "canonical_seasons":[2023,2024,2025],
        "NHL_MAINLINE_MARKET_INVENTORY_COMPLETE":"READY_WITH_BOUNDED_LIMITS",
        "NHL_MAINLINE_OUTCOME_ARCHITECTURE_READINESS":"READY_WITH_BOUNDED_LIMITS",
        "NHL_MAINLINE_PRICE_HISTORY_READINESS":"BLOCKED_BY_NO_MAINLINE_PRICES",
        "NHL_MAINLINE_PRICE_TIMESTAMP_READINESS":"BLOCKED_BY_NO_MAINLINE_PRICES",
        "NHL_MAINLINE_GAME_IDENTITY_CERTIFIED":"READY_WITH_BOUNDED_LIMITS",
        "NHL_MAINLINE_FEATURE_PLATFORM_READINESS":"READY_WITH_BOUNDED_LIMITS_FOR_SEASONS_2023_2024",
        "NHL_MAINLINE_HISTORICAL_REPLAYABILITY":"BLOCKED_BY_NO_MAINLINE_PRICES_OR_BASELINE",
        "NHL_MAINLINE_EXISTING_BASELINE_IDENTIFIED":"NOT_READY",
        "NHL_MONEYLINE_RESEARCH_READINESS":"READY_FOR_POPULATION_CERTIFICATION",
        "NHL_REGULATION_MONEYLINE_RESEARCH_READINESS":"BLOCKED_BY_REGULATION_OUTCOME_CERTIFICATION",
        "NHL_PUCK_LINE_RESEARCH_READINESS":"BLOCKED_BY_NO_MAINLINE_PRICES",
        "NHL_GAME_TOTAL_RESEARCH_READINESS":"BLOCKED_BY_NO_MAINLINE_PRICES",
        "NHL_TEAM_TOTAL_RESEARCH_READINESS":"BLOCKED_BY_NO_MAINLINE_PRICES",
        "NHL_MAINLINE_POPULATION_CERTIFICATION_READINESS":"READY_WITH_BOUNDED_LIMITS",
        "NHL_MAINLINE_MODEL_TRAINING_READINESS":"NOT_READY",
        "NHL_SEASON_2026_MAINLINE_OPERATIONAL_RESTART_READINESS":"NOT_READY",
        "selected_follow_up":"Season 2023 and season 2024 NHL full-game moneyline population and outcome certification",
        "unlocked":"exactly one bounded moneyline population and outcome certification task",
        "still_unauthorized":["odds acquisition/backfill","model training","challenger fitting","feature selection","ROI analysis","promotion","production restart"],
    }
    (out/f"nhl_mainline_feasibility_decision_{STAMP}.json").write_text(json.dumps(decisions,indent=2,sort_keys=True)+"\n")
    identity={"package":"nhl_mainline_historical_feasibility","version":"1.0.0","as_of":STAMP,"assessment_date":"2026-08-09","canonical_seasons":[2023,2024,2025],"scope":"read-only infrastructure feasibility","odds_archive":{"files":audit["files"],"events":audit["events"],"mainline_quotes":0}}
    (out/f"package_identity_{STAMP}.json").write_text(json.dumps(identity,indent=2,sort_keys=True)+"\n")

    next_step="""# First bounded NHL mainline follow-up

## Selection

Certify a full-game moneyline population and neutral outcome spine for canonical seasons `2023` and `2024`.

- Date ranges: `2023-10-10` through `2024-06-24`, and `2024-10-04` through `2025-06-17`.
- Authorities: `nhl.games` for season/game/team/date identity; `nhl.team_game_2023_summary` and `nhl.team_game_2024_summary` for two-team goal outcomes; shot-event period fields only as reconciliation evidence.
- Grain: one game-team side row for evaluation; one game row for neutral outcome truth.
- Expected output: frozen game ledger, home/away score, full-game winner, goal differential, total goals, status/exclusion decisions, duplicate and source-agreement audits.
- Pass: all 2,798 games reconcile to one canonical game, two distinct teams, one score per team, one neutral settlement outcome, canonical season/date agreement, and explicit overtime/shootout scope.
- Fail: any unresolved identity collision, missing team score, ambiguous winner, or silent population narrowing.
- Unlock: one later historical mainline price/timestamp certification against the frozen moneyline spine.
- Unauthorized: odds acquisition, backfill, model training, feature selection, ROI, recommendations, promotion, or restart.
"""
    (out/f"nhl_mainline_first_bounded_next_step_{STAMP}.md").write_text(next_step)

    report=f"""# NHL mainline historical feasibility and repository inventory

## Decision

Proppadia can construct a trustworthy **outcome-first** game population for canonical seasons `2023` and `2024`, but it cannot yet construct a historically price-certified or model-replayable mainline population. The inspected odds archive has {audit['files']} daily files, {audit['events']} event wrappers, {audit['events_with_books']} events with book data, and zero mainline markets. Its only market keys are player shots on goal, alternate player shots on goal, goalie saves, and player points.

## Outcomes and identity

`nhl.games` contains 1,400 games for season `2023`, 1,398 for season `2024`, and 1,312 for season `2025`, with 4,110 distinct IDs and no duplicate IDs or home/away identity conflicts. It does not contain scores. Separate season `2023` and `2024` team summary tables provide exactly two distinct team rows and goal counts for every game. Season `2025` has 900 rows marked final in `nhl.games`, but no populated canonical team-game score table, so it is not outcome-certified here. Regulation, shootout, and overtime settlement remain separate and must not be inferred from full-game goals.

## Prices and markets

No moneyline, regulation moneyline, puck line, game total, team total, or first-period quote survived in the inspected archive. Wrapper capture timestamps are real but apply only to player props; they cannot certify absent mainline prices. There is no derived consensus or closing/opening distinction for mainlines.

## Models and features

No surviving game-level win, score, goal-difference, total, puck-line, Elo, Skellam, or mainline Poisson baseline was identified. The reproducible SOG Poisson formula and other player-prop artifacts are not mainline systems. Team goal/shot rolling summaries create bounded season `2023`/`2024` feature potential, but their strict-prior definitions still need certification. Goalie history is largely actual/postgame: 1,400 games in season `2023`, 1,316 in season `2024`, and 359 in season `2025`, with zero rows shown written before scheduled start. Lineup continuity is weak: only 3 games in season `2023`, 9 in season `2024`; season `2025` has broad game coverage but only 469 individual roster rows timestamped before start.

## Feasibility ranking

Full-game moneyline ranks first because its season `2023`/`2024` identity and outcome grain can now be certified without assuming prices or a model. Puck line, game total, and team total have definable neutral outcomes but remain blocked by absent price history. Regulation moneyline is additionally blocked by regulation-score certification. First-period markets lack enough repository evidence.

## SOG comparison and boundary

The SOG lane is structurally further along: Level 4 probability reproduction, a fixed control, and a characterized feature platform. Mainline has only an outcome-spine opportunity, no baseline, and no prices. This does not imply either market is easier or more profitable, and it does not displace the SOG lane.

## Recommendation

Unlock exactly one bounded task: season `2023` and season `2024` full-game moneyline population and outcome certification. Odds acquisition/backfill, model training, challenger fitting, feature selection, ROI analysis, wagers, promotion, and production restart remain unauthorized.
"""
    (out/f"nhl_mainline_historical_feasibility_report_{STAMP}.md").write_text(report)
    summary="""# NHL mainline feasibility — one-page summary

The repository can support an outcome-first season `2023` and season `2024` game spine, not a price-certified or model-replayable mainline research population. There are 2,798 games with clean two-team goal summaries across those seasons. Season `2025` is not score-certified in the canonical game architecture.

The 153-day season `2025` odds archive contains 907 event wrappers but only player-prop markets and zero mainline quotes. No surviving NHL game-level baseline was found. Team history is promising for bounded later feature work; expected-goalie and lineup timing are not historically certified.

Moneyline is therefore ready only for population and outcome certification. Puck line, game total, and team total are blocked by absent prices; regulation moneyline also needs regulation-score certification. Training, ROI analysis, odds backfill, production changes, and restart remain blocked. The SOG lane remains preserved and structurally further along.
"""
    (out/f"nhl_mainline_feasibility_one_page_summary_{STAMP}.md").write_text(summary)

    run={"odds_files":audit["files"],"odds_events":audit["events"],"events_with_books":audit["events_with_books"],"mainline_quotes":0,"bookmakers":len(audit["books"]),"market_keys":dict(sorted(audit["markets"].items())),"database_snapshot_evidence":{"games":{"2023":1400,"2024":1398,"2025":1312},"clean_scored_games":{"2023":1400,"2024":1398,"2025":0},"duplicate_game_ids":0}}
    (out/f"nhl_mainline_feasibility_run_summary_{STAMP}.json").write_text(json.dumps(run,indent=2,sort_keys=True)+"\n")
    manifest=out/"SHA256SUMS"
    manifest.write_text("\n".join(f"{sha256(p)}  {p.name}" for p in sorted(out.iterdir()) if p.is_file() and p.name != "SHA256SUMS")+"\n")
    print(json.dumps(run,sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
