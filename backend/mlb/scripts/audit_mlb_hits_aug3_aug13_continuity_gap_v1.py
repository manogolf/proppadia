"""Inventory the August 3-13 MLB Hits continuity gap without replay or grading."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits_aug3_aug13_continuity_gap_audit_v1/2026-08-14"
DATES = [f"2026-08-{day:02d}" for day in range(3, 14)]
CURRENT_ID = "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb"
CURRENT_HASH = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"
OUTCOME_COUNTS = {
    "2026-08-03": (8, 139, 19), "2026-08-04": (15, 219, 39),
    "2026-08-05": (15, 43, 10), "2026-08-06": (11, 71, 15),
    "2026-08-07": (15, 239, 25), "2026-08-08": (15, 180, 29),
    "2026-08-09": (15, 16, 2), "2026-08-10": (10, 163, 21),
    "2026-08-11": (15, 225, 38), "2026-08-12": (15, 132, 28),
    "2026-08-13": (9, 54, 2),
}


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(name: str, rows: list[dict]) -> None:
    with (OUT / name).open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def bol_inventory(date: str) -> dict:
    path = ROOT / f"backend/mlb/exports/odds_history/{date}/odds_mlb_playerprops_earliest.json"
    payload = json.loads(path.read_text())
    rows = {0.5: set(), 1.5: set()}; paired = {0.5: set(), 1.5: set()}; updates = []
    for event in payload["events"]:
        for book in event.get("bookmakers", []):
            if book.get("key") != "betonlineag": continue
            for market in book.get("markets", []):
                if market.get("key") != "batter_hits": continue
                updates.append(market.get("last_update", "")); grouped = {}
                for outcome in market.get("outcomes", []):
                    point = float(outcome.get("point")); key = (event["id"], outcome.get("description"), point)
                    grouped.setdefault(key, set()).add(outcome.get("name"))
                for key, sides in grouped.items():
                    if key[2] in rows:
                        rows[key[2]].add(key)
                        if {"Over", "Under"} <= sides: paired[key[2]].add(key)
    return dict(source=str(path.relative_to(ROOT)), source_sha256=digest(path), captured_at=payload["captured_at_utc"],
                h05=len(rows[0.5]), h05_paired=len(paired[0.5]), h15=len(rows[1.5]), h15_paired=len(paired[1.5]),
                first_update=min(updates) if updates else "")


def aug3_originals() -> pd.DataFrame:
    frames = []
    for path in sorted((ROOT / "backend/mlb/exports/odds_history/2026-08-03").glob("mlb_slate_output*.csv")):
        frame = pd.read_csv(path, low_memory=False)
        frame = frame[frame.prop_type.eq("hits")].copy(); frame["source_file"] = str(path.relative_to(ROOT)); frames.append(frame)
    all_rows = pd.concat(frames, ignore_index=True)
    all_rows["prediction_time"] = pd.to_datetime(all_rows.generated_at_utc, utc=True)
    all_rows["scheduled_start"] = pd.to_datetime(all_rows.game_time, utc=True)
    return all_rows.sort_values("prediction_time").drop_duplicates(["game_id", "player_id", "line"])


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    aug3 = aug3_originals()
    bol = {d: bol_inventory(d) for d in DATES + ["2026-08-14"]}
    daily, h05_rows, h15_rows, timing, provenance, replay, outcomes, markets, classes = [], [], [], [], [], [], [], [], []
    for date in DATES:
        feature_path = ROOT / f"backend/mlb/exports/model_diagnostics/prepared_feature_vectors/{date}/hits_features.csv"
        feature = pd.read_csv(feature_path, dtype={"game_id": str, "player_id": str}, low_memory=False)
        counts = {line: feature[feature.line.eq(line)][["game_id", "player_id"]].drop_duplicates() for line in (0.5, 1.5)}
        source_hash = digest(feature_path)
        is_aug3 = date == "2026-08-03"
        originals = {}
        for line in (0.5, 1.5):
            q = aug3[aug3.line.eq(line)] if is_aug3 else aug3.iloc[0:0]
            q = q[q.prediction_time < q.scheduled_start]
            originals[line] = q
        daily.append(dict(date=date, hits05_prediction_rows=len(originals[0.5]), hits15_prediction_rows=len(originals[1.5]),
            model_probability="YES" if is_aug3 else "NO", selected_side="YES" if is_aug3 else "NO", line="YES",
            player_game_identity="YES", prediction_timestamp="YES" if is_aug3 else "NO", semantic_model_id="NOT_EMBEDDED",
            model_hash="MIXED_ROUTE_CANDIDATE_HASH_ONLY" if is_aug3 else "NO_PREDICTION",
            fitted_artifact_reference="CANDIDATE_ONLY" if is_aug3 else "CURRENT_ARTIFACT_RETAINED_SEPARATELY",
            feature_contract_hash="NOT_EMBEDDED", run_tag="YES" if is_aug3 else "INFERRED_FROM_DAILY_CAPTURE_ONLY",
            strict_prior_state="RETAINED", lineup_starter_state="NOT_REQUIRED_BY_INCUMBENT_ARTIFACT_AND_NOT_IN_PREPARED_VECTOR",
            parent_feature_artifact=str(feature_path.relative_to(ROOT)), source_hash=source_hash, exact_state_timestamp="ABSENT",
            betonline_hits05=bol[date]["h05_paired"], betonline_hits15=bol[date]["h15_paired"],
            official_completed_games=OUTCOME_COUNTS[date][0], certified_outcome_artifact="mlb.player_stats durable official-derived rows; no gap-specific certification package"))
        for line, lane, rows_out in ((0.5, "HITS05", h05_rows), (1.5, "HITS15_UNDER", h15_rows)):
            original = len(originals[line]); candidates = len(counts[line]); strict = original
            recovery = "ORIGINAL_PREGAME_PROBABILITY_RETAINED" if original else "INSUFFICIENT_STATE_FOR_REPLAY"
            rows_out.append(dict(date=date, lane=lane, original_pregame_probabilities=original, retained_feature_player_games=candidates,
                recovery_state=recovery, probability_semantics="direct p_over at line; Under is complement", selected_side_complement_valid="YES",
                feature_artifact=str(feature_path.relative_to(ROOT)), feature_sha256=source_hash,
                caveat="August 3 rows mix incumbent fallback and research candidate routing" if is_aug3 else "state timestamp absent; replay not performed"))
            timing.append(dict(date=date, lane=lane, retained_prediction_candidates=original,
                prediction_timestamp=originals[line].prediction_time.min().isoformat() if original else "",
                latest_prediction_timestamp=originals[line].prediction_time.max().isoformat() if original else "",
                scheduled_start_present=bool(original), strict_pregame_rows=strict,
                timing_class="STRICT_PREGAME_PROVEN" if original else "TIMING_UNRESOLVED",
                proof="generated_at_utc < game_time row-by-row" if original else "feature artifact has date and run contract but no exact state timestamp"))
            provenance.append(dict(date=date, lane=lane, retained_prediction_rows=original, semantic_model_id="ABSENT_FROM_ROW",
                exact_model_hash="ABSENT_OR_NONCURRENT_CANDIDATE_ONLY", feature_contract_hash="ABSENT_FROM_ROW", run_tag="PRESENT" if original else "NO_PREDICTION_RUN",
                timestamp="PRESENT" if original else "ABSENT", quality="PRODUCER_PROVENANCE_ONLY" if original else "PROVENANCE_UNRESOLVED",
                current_model_canonical_rows=0))
            replay.append(dict(date=date, lane=lane, no_original_probability=0 if original else candidates,
                fitted_current_artifact="RETAINED", exact_feature_order="RETAINED_IN_MANIFEST", feature_values="RETAINED",
                missing_fallback_state="DERIVABLE_FROM_VALUES", exact_state_timestamp="ABSENT", postgame_contamination_excluded="NOT_CRYPTOGRAPHICALLY_PROVEN",
                feasibility="NOT_APPLICABLE_ORIGINAL_RETAINED" if original else "PARTIAL_REPLAY_ONLY", replay_performed="NO"))
            outcome_available = OUTCOME_COUNTS[date][1 if line == 0.5 else 2]
            population = original if original else candidates
            outcomes.append(dict(date=date, lane=lane, retained_candidate_player_games=population, completed_games=OUTCOME_COUNTS[date][0],
                hit_outcomes_available=outcome_available, certified_outcomes_available=0,
                unresolved_outcomes=max(population-outcome_available, 0), source="read-only mlb.player_stats identity join on 2026-08-14",
                grading_block="gap-specific certified outcome artifact absent; no grading performed"))
            primary = "GENUINE_PROSPECTIVE_ROWS_RECOVERABLE_PARTIAL_PROVENANCE" if original else "PARTIAL_BRIDGE_ONLY"
            classes.append(dict(date=date, lane=lane, primary_classification=primary, original_rows=original,
                replay_only_candidates=0 if original else candidates, unrecoverable_observed_candidates=0,
                evidence_tier="TIER_B" if original else "TIER_C_NOT_YET_CREATED", notes="Tier C requires later authorization and exact timing review"))
        markets.append(dict(date=date, source=bol[date]["source"], source_sha256=bol[date]["source_sha256"], capture_timestamp=bol[date]["captured_at"],
            first_book_update=bol[date]["first_update"], hits05_player_games=bol[date]["h05"], hits05_paired=bol[date]["h05_paired"],
            hits05_novig_possible=bol[date]["h05_paired"], hits15_player_games=bol[date]["h15"], hits15_paired=bol[date]["h15_paired"],
            hits15_novig_possible=bol[date]["h15_paired"], evidence="MODEL_PLUS_BETONLINE_PARITY_AVAILABLE_SEPARATELY"))
    write_csv("hits_gap_daily_artifact_inventory.csv", daily)
    write_csv("hits05_gap_recoverability.csv", h05_rows); write_csv("hits15_under_gap_recoverability.csv", h15_rows)
    write_csv("hits_gap_timing_proof.csv", timing); write_csv("hits_gap_provenance_quality.csv", provenance)
    write_csv("hits_gap_replay_feasibility.csv", replay); write_csv("hits_gap_outcome_availability.csv", outcomes)
    write_csv("hits_gap_betonline_availability.csv", markets); write_csv("hits_gap_daily_classification.csv", classes)

    activation = [
        dict(event="artifact_trained", timestamp="2026-07-09T06:11:24.788077", date="2026-07-09", evidence="models_out/archive/hits/hits-20260709T061129Z.joblib", detail="byte-identical to current latest artifact"),
        dict(event="first_possible_live_scoring", timestamp="2026-07-09T06:11:29Z", date="2026-07-09", evidence="dated archive chronology", detail="artifact available after training"),
        dict(event="first_retained_output_using_byte_identical_artifact", timestamp="2026-07-09T12:57:52.649855Z", date="2026-07-09", evidence="odds_history/2026-07-09/mlb_slate_output__local_daily_20260709T123644Z.csv", detail="execution lineage inference; hash not embedded in row"),
        dict(event="semantic_registration_effective", timestamp="2026-08-03T21:46:49.912141Z", date="2026-08-03", evidence="MLB_HITS_SEMANTIC_V1_2e7377b2cdcb.json", detail=CURRENT_ID),
        dict(event="first_retained_post_registration_daily_run", timestamp="2026-08-03T23:39:52.021126Z", date="2026-08-03", evidence="mlb_slate_output__local_daily_20260803T233005Z.csv", detail="prediction rows retained; exact current semantic binding absent; candidate/downstream route mixed"),
        dict(event="aug4_aug13_daily_workflows", timestamp="2026-08-04/2026-08-13", date="2026-08-04/2026-08-13", evidence="daily raw markets and prepared_feature_vectors", detail="prediction attempted upstream; downstream authority prevented retained canonical output"),
    ]
    write_csv("hits_gap_model_activation_timeline.csv", activation)
    surfaces = [
        dict(surface="odds_history slate/wide", dates="2026-08-03", prediction_state="PREDICTION_GENERATED_BUT_DOWNSTREAM_MIXED_OR_BLOCKED", evidence="six run-tagged slate/wide snapshots"),
        dict(surface="odds_history raw player props", dates="2026-08-03/2026-08-14", prediction_state="MARKET_ONLY", evidence="pregame Odds API snapshots"),
        dict(surface="prepared_feature_vectors/hits_features.csv", dates="2026-08-03/2026-08-14", prediction_state="FEATURE_STATE_BEFORE_PREDICTION_CALL; PREDICTION_NOT_GENERATED ON AUG4-14", evidence="daily diagnostic contract; exact state timestamp absent"),
        dict(surface="model_v2 lane/review/upload", dates="2026-08-03", prediction_state="DERIVED_REVIEW_SURFACE", evidence="August 3 run-tagged quick-card/ranking outputs; not canonical semantic evidence"),
        dict(surface="semantic manifest", dates="effective 2026-08-03T21:46:49Z", prediction_state="IDENTITY_ONLY", evidence=CURRENT_ID),
        dict(surface="database/lifecycle", dates="2026-08-03/2026-08-13", prediction_state="NO HITS PREDICTION TABLE FOUND", evidence="information_schema inspection; moneyline table unrelated"),
        dict(surface="current nonmarket parent research package", dates="2026-08-14", prediction_state="27 SCORED RESEARCH CANDIDATE ROWS; NOT CURRENT SEMANTIC MODEL", evidence="model hash 4959109c...; production unchanged"),
        dict(surface="logs/reporting alignment", dates="2026-08-04/2026-08-13", prediction_state="AUTHORITY SKIP EVIDENCE; NO RETAINED PROBABILITY", evidence="daily reporting alignment artifacts"),
    ]
    write_csv("hits_gap_prediction_surface_inventory.csv", surfaces)

    bridge = []
    for lane in ("HITS05", "HITS15_UNDER"):
        relevant = [r for r in classes if r["lane"] == lane]
        bridge.append(dict(lane=lane, original_pregame_probabilities=sum(r["original_rows"] for r in relevant), exact_current_model_provenance_rows=0,
            partial_provenance_original_rows=sum(r["original_rows"] for r in relevant), replay_only_candidate_rows=sum(r["replay_only_candidates"] for r in relevant),
            unrecoverable_observed_player_games=0, unique_games="8 original; replay counts date-scoped in recoverability CSV",
            unique_players="158 original H05 / 20 original H15; replay identities date-scoped", dates_represented=11,
            denominator_note="Counts cover retained original/feature candidates only; absence from a retained candidate surface is not silently counted as a player-game."))
    write_csv("hits_gap_potential_bridge_counts.csv", bridge)

    tier_contract = {
        "TIER_A": {"label":"ORIGINAL_PROSPECTIVE_CURRENT_MODEL","requirements":["actual pregame prediction","exact semantic ID/hash","feature-contract hash","run tag","prediction timestamp"]},
        "TIER_B": {"label":"ORIGINAL_PROSPECTIVE_PARTIAL_PROVENANCE","requirements":["actual pregame prediction retained","timing proven","exact current-model binding incomplete"],"may_count_as_tier_a":False},
        "TIER_C": {"label":"RECONSTRUCTED_PREGAME_BRIDGE","requirements":["authorized later replay","frozen pregame state","exact model/feature contract","reconstruction timestamp and original-state timestamp"],"may_count_as_tier_a":False},
        "TIER_D": {"label":"UNRECOVERABLE","requirements":["no legitimate original or replayable state"]},
    }
    (OUT / "hits_gap_evidence_tier_contract.json").write_text(json.dumps(tier_contract, indent=2) + "\n")

    chronology = """# MLB Hits continuity map

## Historical evidence — March 25 through August 2

Valid historical prediction evidence. The producer and fitted generations are reproducible, but per-row model identity is partial.

## August 3

Hits 0.5: 158 strict-pregame original rows. Hits 1.5 Under: 20 strict-pregame original rows; eight identities first appeared post-start and are excluded. Rows are Tier B because the slate mixes incumbent fallback/research-candidate routing and lacks exact current semantic binding.

## August 4–13

No original probability artifacts were found. Daily prepared feature vectors and BetOnline markets survive. Each date is `PARTIAL_BRIDGE_ONLY`: a later replay may be technically useful, but exact pregame-state timestamps are absent and no replay is authorized here.

## August 14 onward

`AUG14_CANONICAL_BASELINE_NOT_READY`. August 14 has raw markets, diagnostic feature vectors, and a separate research-only Poisson candidate (`4959109c...`) with 27 scored rows. It has zero full-board predictions bound to `MLB_HITS_SEMANTIC_V1_2e7377b2cdcb`; Hits 1.5 canonical current-semantic rows are also absent.

Historical, original prospective Tier B, possible reconstructed Tier C, and future Tier A evidence must remain separate.
"""
    (OUT / "hits_gap_continuity_map.md").write_text(chronology)
    h05_orig = bridge[0]["original_pregame_probabilities"]; h05_replay = bridge[0]["replay_only_candidate_rows"]
    h15_orig = bridge[1]["original_pregame_probabilities"]; h15_replay = bridge[1]["replay_only_candidate_rows"]
    total_outcomes = sum(r["hit_outcomes_available"] for r in outcomes)
    total_candidates = sum(r["retained_candidate_player_games"] for r in outcomes)
    total_b05 = sum(r["hits05_paired"] for r in markets); total_b15 = sum(r["hits15_paired"] for r in markets)
    concise = f"""# MLB Hits August 3–13 continuity-gap audit v1

- Current artifact trained July 9; semantic registration effective `2026-08-03T21:46:49.912141Z`.
- First retained byte-identical-artifact run: July 9 at `12:57:52Z` (execution-lineage binding). First post-registration retained daily output: August 3 at `23:39:52Z`, without exact current-semantic row binding.
- August 3: 158 strict-pregame Hits 0.5 and 20 strict-pregame Hits 1.5 rows; `GENUINE_PROSPECTIVE_ROWS_RECOVERABLE_PARTIAL_PROVENANCE` (Tier B).
- August 4–13: no original probabilities; every date is `PARTIAL_BRIDGE_ONLY`. Retained feature candidates: Hits 0.5={h05_replay}; Hits 1.5={h15_replay}. No replay was performed.
- Exact current-model provenance rows: 0. Observed-candidate rows declared unrecoverable: 0; the audit does not invent a denominator for players absent from all retained candidate surfaces.
- Outcome availability: {total_outcomes}/{total_candidates} retained candidate identities have durable hit outcomes; no gap-specific certified outcome package was found.
- Earliest retained BetOnline paired coverage totals: Hits 0.5={total_b05}; Hits 1.5={total_b15}. Markets remain separate evidence.
- August 14: `AUG14_CANONICAL_BASELINE_NOT_READY`; current-semantic full-board predictions are absent. The 27 scored rows found use separate research model `4959109c...`.
- Overall: `AUG3_AUG13_GAP_PRIMARILY_REPLAY_ONLY`.
- `HITS05_GAP_RECOVERY = MIXED_TIER_B_ORIGINAL_AND_PARTIAL_REPLAY_BRIDGE`
- `HITS15_UNDER_GAP_RECOVERY = MIXED_TIER_B_ORIGINAL_AND_PARTIAL_REPLAY_BRIDGE`
- `AUG14_CANONICAL_BASELINE = NOT_READY`

Human decision required next: either authorize a strictly labeled Tier C replay feasibility/identity-resolution phase for August 4–13, or leave those dates outside continuity evidence and begin Tier A capture only after the current semantic model is bound at freeze time.
"""
    (OUT / "concise_mlb_hits_aug3_aug13_continuity_gap_audit_v1.md").write_text(concise)
    hash_path = OUT / "reproducibility_hashes.sha256"
    hash_path.write_text("\n".join(f"{digest(p)}  {p.name}" for p in sorted(OUT.iterdir()) if p.is_file() and p != hash_path) + "\n")
    print(json.dumps({"h05_original":h05_orig,"h05_replay":h05_replay,"h15_original":h15_orig,"h15_replay":h15_replay,
                      "aug14":"NOT_READY","decision":"AUG3_AUG13_GAP_PRIMARILY_REPLAY_ONLY"}, indent=2))


if __name__ == "__main__": main()
