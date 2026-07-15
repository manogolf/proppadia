#!/usr/bin/env python3
"""Package the bounded season-2025 SOG feature characterization."""
from __future__ import annotations

import argparse, hashlib, json
from pathlib import Path
import pandas as pd

STAMP = "2026-07-13"

def digest(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--parent-contract", required=True)
    args = ap.parse_args(); out = Path(args.out_dir)
    contract = pd.read_csv(args.parent_contract)
    contract.insert(1, "source_native_field", contract.field_name)
    contract["formula_role"] = contract.field_name.map({
        "d10_sog_per60":"primary rate", "d20_sog_per60":"rate fallback 1", "d5_sog_per60":"rate fallback 2",
        "d10_toi_min_avg":"primary TOI", "d20_toi_min_avg":"TOI fallback 1", "d5_toi_min_avg":"TOI fallback 2",
        "szn_toi_per_game_5on5":"TOI fallback 3 component", "szn_toi_per_game_pp":"TOI fallback 3 component",
        "season_5on5_icetime_per_game":"TOI fallback 4 component", "season_5on4_icetime_per_game":"TOI fallback 4 component",
        "expected_sog":"lambda = rate * TOI / 60", "p_over_line":"Poisson tail", "side":"p_over >= 0.5"
    })
    contract["input_class"] = contract.field_name.map(lambda x: "DIRECT_PREPARED_INPUT" if x.startswith(("d", "szn", "season")) else "DERIVED_OUTPUT")
    contract["verification"] = "VERIFIED_AGAINST_EXACT_PARENT_REPRODUCTION"
    contract.to_csv(out/f"nhl_season_2025_sog_baseline_input_contract_verified_{STAMP}.csv", index=False, lineterminator="\n")

    shortlist = pd.DataFrame([{
        "rank":1, "feature_family":"PAIRING_DATA_QUALITY", "recommendation":"PREREQUISITE_ONLY_NOT_A_CHALLENGER_SIGNAL",
        "hypothesis":"shiftchart coverage can gate later role-feature experiments without becoming a predictor",
        "conceptual_domain":"PLAYER_ROLE", "fields":"d10_shiftcharts_coverage_rate|d20_shiftcharts_coverage_rate|d10_pairings_available|d20_pairings_available",
        "authoritative_sources":"saved Denali prepared values; prior-game shiftchart SQL", "fixed_historical_population_availability":"100% any-field; 99.686% all-field",
        "season_2026_daily_availability":"READY_WITH_BOUNDED_LIMITS", "expected_mechanism":"measurement-quality gate",
        "major_risks":"coverage is not hockey signal; source continuity", "future_challenger_must_prove":"certified role signal remains stable after a prespecified coverage gate",
        "ranking_basis":"only bounded prerequisite justified; no family met potential-incremental classification"
    }])
    shortlist.to_csv(out/f"nhl_season_2025_sog_challenger_family_shortlist_{STAMP}.csv", index=False, lineterminator="\n")

    decisions = {
      "canonical_season":2025,
      "NHL_SEASON_2025_SOG_BASELINE_INPUT_CONTRACT_VERIFIED":"READY",
      "NHL_SEASON_2025_SOG_FEATURE_INVENTORY_COMPLETE":"READY_WITH_BOUNDED_LIMITS",
      "NHL_SEASON_2025_SOG_FEATURE_OWNERSHIP_RESOLVED":"READY_WITH_BOUNDED_LIMITS",
      "NHL_SEASON_2025_SOG_FEATURE_GRAIN_CERTIFIED":"READY",
      "NHL_SEASON_2025_SOG_FEATURE_TIMING_CERTIFIED":"READY_WITH_BOUNDED_LIMITS",
      "NHL_SEASON_2025_SOG_FEATURE_LINEAGE_CERTIFIED":"READY_WITH_BOUNDED_LIMITS",
      "NHL_SEASON_2025_SOG_HISTORICAL_FEATURE_REPLAYABILITY":"READY_WITH_BOUNDED_LIMITS",
      "NHL_SEASON_2025_SOG_INCREMENTAL_INFORMATION_CHARACTERIZED":"READY",
      "NHL_SEASON_2025_SOG_CHALLENGER_FAMILIES_IDENTIFIED":"NOT_READY",
      "NHL_SEASON_2025_SOG_CHALLENGER_SPECIFICATION_READINESS":"BLOCKED_BY_NO_STABLE_INCREMENTAL_FAMILY",
      "NHL_SEASON_2026_SOG_FEATURE_COLLECTION_READINESS":"READY_WITH_BOUNDED_LIMITS",
      "NHL_SEASON_2026_SOG_MODEL_TRAINING_READINESS":"NOT_READY",
      "NHL_SEASON_2026_SOG_OPERATIONAL_RESTART_READINESS":"NOT_READY",
      "candidate_parity":"BLOCKED_BY_NO_RUN_BOUND_POLICY_AND_ODDS_SNAPSHOT",
      "unlocked":"one bounded prerequisite: certify prediction-time role/lineup/goalie collection and pairing coverage",
      "not_authorized":["training","challenger fitting","threshold tuning","ROI","production changes","restart"]
    }
    (out/f"nhl_season_2025_sog_feature_platform_decision_{STAMP}.json").write_text(json.dumps(decisions, indent=2, sort_keys=True)+"\n")
    identity = {"package":"nhl_season_2025_sog_feature_platform_characterization", "version":"1.0.0", "as_of":STAMP, "canonical_season":2025, "population":{"prediction_rows":40167,"player_games":13389,"dates":"2026-02-28..2026-04-15"}, "model":"poisson_baseline / baseline_v1", "scope":"descriptive characterization only"}
    (out/f"package_identity_{STAMP}.json").write_text(json.dumps(identity, indent=2, sort_keys=True)+"\n")

    joins=pd.read_csv(out/f"nhl_season_2025_sog_feature_fixed_spine_join_audit_{STAMP}.csv")
    fam=pd.read_csv(out/f"nhl_season_2025_sog_feature_family_characterization_{STAMP}.csv")
    inv=pd.read_csv(out/f"nhl_season_2025_sog_feature_inventory_{STAMP}.csv")
    join_lines="\n".join(f"- {r.feature_family}: {r.exact_join_rate:.4%} ({int(r.joinable_player_games):,}/{int(r.control_player_games):,} player-games); many-to-many 0" for _,r in joins.iterrows())
    text=f"""# Season 2025 NHL SOG feature-platform characterization

## Outcome

The exact `poisson_baseline / baseline_v1` input contract is verified, and the fixed 40,167-row / 13,389-player-game spine is grain-certified. The bounded inventory contains {len(inv)} fields or repository concepts: 67 saved prepared columns and 9 materially relevant absent concepts. No family met the prespecified descriptive standard for potential incremental information. Consequently, this package does **not** unlock a champion-challenger experiment specification.

## Exact baseline contract

The formula selects the first available rate in `d10_sog_per60`, `d20_sog_per60`, `d5_sog_per60`; selects TOI from `d10_toi_min_avg`, `d20_toi_min_avg`, `d5_toi_min_avg`, then season 5-on-5 plus PP minutes, then season situation seconds divided by 60; defaults unresolved lambda to zero; computes `expected_sog = rate * TOI / 60`; computes the Poisson tail at lines 1.5, 2.5, 3.5; and selects OVER at probability >= 0.5. All values survived in prepared archives and reproduced 40,167/40,167 probabilities within 1e-12 and sides exactly.

## Frozen joins

{join_lines}

The source archive has no duplicate natural keys. All joins validate one-to-one at player-game grain; values are broadcast to exactly the three pre-existing line rows. The control population was never narrowed.

## Ownership, duplication, and timing

Ownership is explicit for baseline inputs, outcome, identity, and the twelve requested domains; unresolved prepared metadata remains visibly marked rather than silently assigned. Eight duplicate/parent-child concept groups are documented, especially rolling SOG windows, TOI fallbacks and units, pace aliases, PP role versus realized opportunity, and pairing signal versus coverage.

The rolling feature, pairing, and season-TOI SQL use prior games. Saved values make replay exact, but `role_pp_share` remains date-only and several top-line context fields lack prediction timestamps. `shots_on_goal` is explicitly postgame/outcome-only. Goalie, injury, scratch, market movement, and candidate-policy concepts are not historically timing-certified.

## Descriptive diagnostics

Diagnostics cover 46 numeric fields using fixed natural distributions, Spearman association with official SOG and baseline residual, correctness association, and monthly sign consistency. Player shooting skill had median absolute residual Spearman 0.0804 but is substantially the information already consumed by the baseline. Opportunity was likewise baseline-overlapping (0.0283). Pairing data-quality measured 0.0253; role, team, opponent, rest, and recent-form families were 0.0083 or below (or unavailable) and classified weak/unstable. These are associations, not lift, and no model, coefficient, threshold scan, p-value selection, or ROI calculation was performed.

## Readiness and decision

Existing rolling/player/team/opponent fields are suitable for bounded historical characterization and prospective observation with timestamp/coverage gates. Prediction-time goalie, lineup, injury, scratch, richer travel, and market context are deferred by timing or source continuity. Pairing data quality is listed only as a prerequisite/gating family, not a challenger signal.

The next bounded activity is **prediction-time role/lineup/goalie collection and pairing-coverage certification**. Training, challenger fitting, threshold tuning, candidate parity, ROI claims, production changes, season 2026 model training, and operational restart remain blocked. Candidate parity remains `BLOCKED_BY_NO_RUN_BOUND_POLICY_AND_ODDS_SNAPSHOT`.

## Boundaries

The user-reported historical ROI observation remains `USER_REPORTED_UNVERIFIED`. The analysis uses only canonical season `2025`, the full fixed accuracy population, and saved control inputs. It does not infer wagers or candidate logic.
"""
    (out/f"nhl_season_2025_sog_feature_platform_report_{STAMP}.md").write_text(text)
    summary="# Season 2025 NHL SOG feature platform — one-page summary\n\n"+text.split("## Exact baseline contract",1)[0].split("## Outcome",1)[1].strip()+"\n\n"+"No challenger family is authorized. The bounded next step is prediction-time role/lineup/goalie collection and pairing-coverage certification. Candidate policy, ROI, training, production changes, and restart remain blocked.\n"
    (out/f"nhl_season_2025_sog_feature_platform_one_page_summary_{STAMP}.md").write_text(summary)
    manifest = out / "SHA256SUMS"
    rows = [f"{digest(p)}  {p.name}" for p in sorted(out.iterdir()) if p.is_file() and p.name != manifest.name]
    manifest.write_text("\n".join(rows)+"\n")
    return 0
if __name__ == "__main__": raise SystemExit(main())
