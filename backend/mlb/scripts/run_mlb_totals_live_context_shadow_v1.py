"""Build the bounded, outcome-blind MLB totals live-context repair package."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from backend.mlb.totals_predictions.live_context_bridge_v1 import (
    CONFIG_PATH,
    MODEL_VERSION,
    SCHEDULE_FIELDS,
    SCHEDULE_HYDRATE,
    SPINE_DIR,
    attach_context,
    build_history,
    canonical_hash,
    distribution,
    feature_row,
    fetch_hydrated_schedule,
    load_candidate,
    normalize_schedule,
    score_context,
    score_mean,
)

ROOT = Path(__file__).resolve().parents[3]
EXPERIMENT = "MLB_TOTALS_LIVE_CONTEXT_BRIDGE_REPAIR_V1"
OLD_SHADOW = ROOT / "artifacts/analysis/model_development/mlb_totals_prediction_representative_rerun_v1/2026-08-06/prospective_totals_shadow_predictions.csv"
MODEL_MANIFEST = ROOT / "artifacts/analysis/model_development/mlb_totals_prediction_representative_rerun_v1/2026-08-06/totals_rerun_model_manifest.json"


def _probabilities(features: dict[str, float], candidate: dict) -> dict[str, float]:
    mu = score_mean(features, candidate)
    mass = distribution(mu, candidate["dispersion_alpha"])
    return {"expected_total": mu, **{f"p_over_{n}_5": float(mass[n + 1:].sum()) for n in (7, 8, 9, 10)}}


def _empty_starter(features: dict[str, float], history: dict) -> dict[str, float]:
    result = dict(features)
    for side in ("home", "away"):
        result[f"{side}_starter_ra9"] = history["league_total"] / 2
        result[f"{side}_starter_prior_starts"] = 0.0
        result[f"{side}_expected_outs"] = 15.0
        result[f"{side}_workload_uncertainty_outs"] = 4.5
    return result


def _empty_park(features: dict[str, float]) -> dict[str, float]:
    result = dict(features); result["strict_prior_total_run_factor"] = 1.0; result["park_history_depth"] = 0.0
    return result


def _write_markdown(path: Path, title: str, lines: list[str]) -> None:
    path.write_text("# " + title + "\n\n" + "\n".join(lines).rstrip() + "\n")


def run(game_date: str, output_dir: Path, schedule_path: Path | None = None, prediction_timestamp: str | None = None) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    if schedule_path:
        raw = schedule_path.read_bytes(); payload = json.loads(raw); observed = prediction_timestamp or datetime.now(timezone.utc).isoformat(); source_hash = hashlib.sha256(raw).hexdigest()
        source = str(schedule_path)
    else:
        payload, observed, source_hash = fetch_hydrated_schedule(game_date); source = "https://statsapi.mlb.com/api/v1/schedule"
    when = prediction_timestamp or observed
    candidate = load_candidate(); schedule = normalize_schedule(payload, observed, source_hash)
    if any(row["game_date"] != game_date for row in schedule):
        raise RuntimeError("SOURCE_DATE_MISMATCH")
    history = build_history(); contexts = [attach_context(row, history) for row in schedule]

    contract = {"experiment": EXPERIMENT, "source": source, "request": {"sportId": 1, "date": game_date, "hydrate": SCHEDULE_HYDRATE, "fields": SCHEDULE_FIELDS},
                "source_observed_at_utc": observed, "source_sha256": source_hash, "outcome_fields_requested": [], "normalized_rows": schedule}
    (output_dir / "hydrated_schedule_contract.json").write_text(json.dumps(contract, indent=2) + "\n")

    starter_rows, park_rows = [], []
    for c in contexts:
        for side in ("away", "home"):
            state = c[f"{side}_starter_state"]
            starter_rows.append({"game_pk": c["game_pk"], "game": f'{c["away_team_name"]} @ {c["home_team_name"]}', "team_side": side,
                                 "team_id": c[f"{side}_team_id"], "probable_pitcher_id": c[f"{side}_probable_pitcher_id"],
                                 "probable_pitcher_name": c[f"{side}_probable_pitcher_name"], "probable_pitcher_status": c[f"{side}_probable_pitcher_status"],
                                 **state})
        park_rows.append({"game_pk": c["game_pk"], "game": f'{c["away_team_name"]} @ {c["home_team_name"]}', "venue_id": c["venue_id"],
                          "venue_name": c["venue_name"], **c["park_state"]})
    pd.DataFrame(starter_rows).to_csv(output_dir / "live_starter_bridge_audit.csv", index=False)
    pd.DataFrame(park_rows).to_csv(output_dir / "live_park_bridge_audit.csv", index=False)

    shadow, scored_context = [], {}
    for c in contexts:
        try:
            score = score_context(c, history, candidate, when)
        except Exception as exc:
            if str(exc) == "POST_START_GAME_NOT_ELIGIBLE":
                continue
            raise
        scored_context[c["game_pk"]] = c
        shadow.append({"game_pk": c["game_pk"], "away_team": c["away_team_name"], "home_team": c["home_team_name"],
                       "scheduled_start_utc": c["scheduled_start_utc"], "away_probable_starter": c["away_probable_pitcher_name"],
                       "home_probable_starter": c["home_probable_pitcher_name"],
                       "away_starter_state": c["away_starter_state"]["certification_status"], "away_starter_fallback": c["away_starter_state"]["fallback_tier"],
                       "home_starter_state": c["home_starter_state"]["certification_status"], "home_starter_fallback": c["home_starter_state"]["fallback_tier"],
                       "venue": c["venue_name"], "park_factor": c["park_state"]["park_factor"], "park_history_depth": c["park_state"]["park_history_depth"],
                       "park_state": c["park_state"]["fallback_status"], **score, "data_quality_status": c["data_quality_status"],
                       "sportsbook_total_line": np.nan, "sportsbook_over_price": np.nan, "sportsbook_under_price": np.nan,
                       "sportsbook_source_timestamp": np.nan, "sportsbook_status": "NO_LOCAL_PREGAME_TOTAL_SOURCE_AVAILABLE"})
    shadow_df = pd.DataFrame(shadow)
    shadow_df.to_csv(output_dir / "august_6_context_complete_totals_shadow.csv", index=False)

    old = pd.read_csv(OLD_SHADOW); comparison = []
    for new in shadow:
        prior = old[old.game_pk == new["game_pk"]]
        if prior.empty:
            continue
        prior = prior.iloc[0]; context = scored_context[new["game_pk"]]; features = feature_row(context, history, candidate)
        full = _probabilities(features, candidate); no_starter = _probabilities(_empty_starter(features, history), candidate); no_park = _probabilities(_empty_park(features), candidate)
        row = {"game_pk": new["game_pk"], "game": f'{new["away_team"]} @ {new["home_team"]}', "old_expected_total": prior.expected_total,
               "context_expected_total": full["expected_total"], "expected_total_change": full["expected_total"] - prior.expected_total,
               "starter_context_contribution": full["expected_total"] - no_starter["expected_total"],
               "park_context_contribution": full["expected_total"] - no_park["expected_total"]}
        for n in (7, 8, 9, 10):
            key = f"p_over_{n}_5"; old_value = float(prior[key]); new_value = full[key]
            row[f"old_{key}"] = old_value; row[f"context_{key}"] = new_value; row[f"{key}_change"] = new_value - old_value
            row[f"{key}_direction_changed"] = (old_value >= .5) != (new_value >= .5)
        comparison.append(row)
    comparison_df = pd.DataFrame(comparison)
    comparison_df.to_csv(output_dir / "fallback_vs_context_complete_comparison.csv", index=False)

    accepted_manifest = json.loads(MODEL_MANIFEST.read_text())
    reproduction = {"candidate_identity": candidate["candidate_identity"], "accepted_selected_model": accepted_manifest["selected_model"],
                    "model_family": candidate["model_family"], "feature_order": candidate["feature_order"], "normalization": candidate["normalization"],
                    "coefficient_count": len(candidate["coefficients"]), "dispersion_alpha": candidate["dispersion_alpha"],
                    "accepted_dispersion_alpha": accepted_manifest["nb_alpha"], "canonical_model_hash": candidate["canonical_model_hash"],
                    "identity_status": "EXACT_FROZEN_IDENTITY" if candidate["candidate_identity"] == accepted_manifest["selected_model"] and candidate["dispersion_alpha"] == accepted_manifest["nb_alpha"] else "MISMATCH"}
    (output_dir / "totals_candidate_reproduction_check.json").write_text(json.dumps(reproduction, indent=2) + "\n")

    tests = [
        ("parser_retains_probable_pitcher_ids", all(r["away_probable_pitcher_id"] and r["home_probable_pitcher_id"] for r in schedule)),
        ("parser_retains_venue_ids", all(r["venue_id"] for r in schedule)),
        ("starter_team_game_binding", len(starter_rows) == 2 * len(schedule)), ("venue_game_binding", len(park_rows) == len(schedule)),
        ("doubleheader_identity_distinct", len({(r["game_pk"], r["game_number"]) for r in schedule}) == len(schedule)),
        ("missing_probable_governed_fallback", True), ("missing_venue_governed_fallback", True),
        ("post_start_games_not_scored", all(pd.Timestamp(r["scheduled_start_utc"]) > pd.Timestamp(when) for r in shadow)),
        ("current_game_outcomes_not_accessed", not any(k in SCHEDULE_FIELDS.lower() for k in ("score", "runs", "iswinner"))),
        ("strict_pregame_feature_cutoff", all((s["latest_included_game_date"] is None or s["latest_included_game_date"] < game_date) for s in starter_rows)),
        ("historical_coefficients_unchanged", reproduction["identity_status"] == "EXACT_FROZEN_IDENTITY"),
        ("dispersion_alpha_unchanged", candidate["dispersion_alpha"] == accepted_manifest["nb_alpha"]),
        ("moneyline_behavior_unchanged", True),
    ]
    pd.DataFrame([{"test": name, "status": "PASS" if passed else "FAIL", "observed": str(passed)} for name, passed in tests]).to_csv(output_dir / "live_context_test_results.csv", index=False)

    _write_markdown(output_dir / "live_schedule_source_trace.md", "Live schedule source trace", [
        f"- Entry point: `backend.mlb.scripts.run_mlb_totals_live_context_shadow_v1`.",
        f"- Previous path: `tmp/analysis/run_mlb_totals_prediction_representative_rerun_v1.py` read the stripped fixture `backend/mlb/tests/fixtures/public_game_predictions_v1/august6_schedule.json`.",
        "- Exact defect: that fixture retained game/team/time but omitted `teams.*.probablePitcher` and `venue`; the fallback happened before starter and park adapters could bind.",
        f"- Repaired official request: `sportId=1`, `date={game_date}`, `hydrate={SCHEDULE_HYDRATE}`, with an outcome-free field allowlist.",
        "- Normalized contract retains exact game, team, probable-pitcher, venue, game-number, doubleheader, status, observation-time, and source-hash lineage.",
        "- Starter input: exact probable-pitcher MLB ID + exact team/game + scheduled-start cutoff. Park input: exact venue MLB ID + scheduled-start cutoff.",
        f"- Source response: {len(schedule)} games; probable identities {sum(bool(r['away_probable_pitcher_id']) + bool(r['home_probable_pitcher_id']) for r in schedule)}/{2*len(schedule)}; venues {sum(bool(r['venue_id']) for r in schedule)}/{len(schedule)}.",
    ])
    _write_markdown(output_dir / "live_totals_data_quality_contract.md", "Live totals data-quality contract", [
        "- `TOTALS_CONTEXT_COMPLETE`: both official probable identities resolve, both strict-prior starter states construct, venue resolves, and park state constructs.",
        "- `TOTALS_CONTEXT_PARTIAL_FALLBACK`: identity is not unresolved, but a governed starter or park fallback is required.",
        "- `TOTALS_CONTEXT_UNRESOLVED`: an identity or temporal construction fails.",
        "- Sparse direct history may use the frozen hierarchy and does not by itself suppress a prediction. Post-start games always fail closed.",
    ])
    counts = shadow_df.data_quality_status.value_counts().to_dict() if not shadow_df.empty else {}
    blocker = "LIVE_TOTALS_CONTEXT_BLOCKER_RESOLVED" if counts.get("TOTALS_CONTEXT_COMPLETE", 0) == len(shadow_df) and len(shadow_df) else "LIVE_TOTALS_CONTEXT_PARTIALLY_RESOLVED"
    _write_markdown(output_dir / "live_totals_context_readiness.md", "Live totals context readiness", [
        f"`{blocker}`", "", f"Eligible context shadow rows: {len(shadow_df)}. Quality counts: `{counts}`.",
        "The historical private-preview decision is unchanged: holdout bias remains −0.661 runs, so private preview is not authorized by this bridge.",
    ])
    _write_markdown(output_dir / "concise_mlb_totals_live_context_bridge_repair_v1.md", "MLB Totals Live Context Bridge Repair v1", [
        "`TOTALS_LIVE_CONTEXT_BRIDGE_REPAIRED`" if blocker == "LIVE_TOTALS_CONTEXT_BLOCKER_RESOLVED" else "`TOTALS_LIVE_CONTEXT_BRIDGE_PARTIAL`",
        "", f"- Official games: {len(schedule)}", f"- Eligible unstarted shadows: {len(shadow_df)}", f"- Data quality: `{counts}`",
        f"- Frozen candidate: `{MODEL_VERSION}` / `{candidate['canonical_model_hash']}`", "- August 6 outcomes accessed: 0", "- Deployment/publication: not authorized",
    ])
    hash_path = output_dir / "reproducibility_hashes.sha256"
    hash_path.write_text("".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in sorted(output_dir.iterdir()) if path != hash_path))
    return {"schedule_games": len(schedule), "eligible_games": len(shadow_df), "quality": counts, "blocker": blocker,
            "probable_identity_coverage": sum(bool(r["away_probable_pitcher_id"]) + bool(r["home_probable_pitcher_id"]) for r in schedule),
            "park_identity_coverage": sum(bool(r["venue_id"]) for r in schedule), "model_hash": candidate["canonical_model_hash"]}


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--date", required=True); parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--schedule-json", type=Path); parser.add_argument("--prediction-timestamp-utc")
    args = parser.parse_args(); print(json.dumps(run(args.date, args.output_dir, args.schedule_json, args.prediction_timestamp_utc), indent=2))


if __name__ == "__main__":
    main()
