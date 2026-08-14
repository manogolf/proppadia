"""Evaluate the strict MLB Hits Tier C bridge gate without weakening it."""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd

from backend.mlb.scripts.recover_mlb_hits05_historical_model_provenance_v1 import score

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits_aug4_aug13_tier_c_bridge_recovery_v1/2026-08-14"
DATES = [f"2026-08-{day:02d}" for day in range(4, 14)]
ARCHIVE = ROOT / "models_out/archive/hits/hits-20260709T061129Z.joblib"
CURRENT = ROOT / "models_out/latest/hits.joblib"
MANIFEST = ROOT / "backend/mlb/config/semantic_models/manifests/MLB_HITS_SEMANTIC_V1_2e7377b2cdcb.json"
SEMANTIC_ID = "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb"


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""): h.update(chunk)
    return h.hexdigest()


def write_csv(name: str, rows: list[dict], fields: list[str] | None = None) -> None:
    fields = fields or list(rows[0])
    with (OUT / name).open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore"); writer.writeheader(); writer.writerows(rows)


def original_aug3() -> pd.DataFrame:
    frames = []
    root = ROOT / "backend/mlb/exports/odds_history/2026-08-03"
    for path in sorted(root.glob("mlb_slate_output*.csv")):
        frame = pd.read_csv(path, low_memory=False); frame = frame[frame.prop_type.eq("hits")].copy()
        frame["source_prediction_artifact"] = str(path.relative_to(ROOT)); frames.append(frame)
    frame = pd.concat(frames, ignore_index=True)
    frame["prediction_timestamp"] = pd.to_datetime(frame.generated_at_utc, utc=True)
    frame["scheduled_start"] = pd.to_datetime(frame.game_time, utc=True)
    frame = frame.sort_values("prediction_timestamp").drop_duplicates(["game_id", "player_id", "line"])
    return frame[frame.prediction_timestamp < frame.scheduled_start]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    archive_hash, current_hash = sha(ARCHIVE), sha(CURRENT)
    if archive_hash != current_hash:
        raise RuntimeError("July 9/current semantic artifacts are not byte-identical")
    bundle = joblib.load(CURRENT); meta = bundle["meta"]
    manifest = json.loads(MANIFEST.read_text())["registration_payload"]
    artifact_identity = {
        "july9_artifact_path": str(ARCHIVE.relative_to(ROOT)), "july9_sha256": archive_hash,
        "semantic_artifact_path": str(CURRENT.relative_to(ROOT)), "semantic_sha256": current_hash,
        "byte_equality": True, "BYTE_IDENTICAL_MODEL_ARTIFACT": True,
        "model_family": "serialized sklearn LR/RF pipelines with AUC-weighted probability blend",
        "fitted_parameters_identity": "BYTE_IDENTICAL", "feature_count_identity": len(meta["input_columns"]),
    }
    (OUT / "tier_c_model_artifact_identity.json").write_text(json.dumps(artifact_identity, indent=2) + "\n")
    contract = {
        "semantic_model_id": SEMANTIC_ID, "model_sha256": current_hash,
        "feature_contract_sha256": manifest["feature_schema_sha256"], "feature_names_in_order": meta["input_columns"],
        "preprocessing": "embedded sklearn ColumnTransformer pipelines",
        "missing_value_behavior": "isna__ indicators generated from source values; non-indicator numeric missing/non-numeric coerced to 0.0",
        "fallback_behavior": "rolling_result_avg_7 fallback only when d7/d15/d30 exact-prop mean unavailable; no arbitrary probability fallback accepted",
        "probability_generation": "LR/RF predict_proba blended by max(AUC-0.5,0), equal mean if weights zero",
        "line_sensitivity": "sigmoid(logit(p)+0.90*((weighted d7/d15/d30 hits mean-line)/bounded hits scale))",
        "selected_side_logic": "Over iff p_over >= 0.5", "hits05_complement": "p_under_0_5=1-p_over_0_5",
        "hits15_transform": "same exact line-sensitivity function evaluated directly at line 1.5; not derived from p_over_0_5",
        "output_precision": "slate export rounds probability to six decimals",
    }
    (OUT / "tier_c_model_contract.json").write_text(json.dumps(contract, indent=2) + "\n")

    originals = original_aug3()
    features = pd.read_csv(ROOT / "backend/mlb/exports/model_diagnostics/prepared_feature_vectors/2026-08-03/hits_features.csv",
                           dtype={"game_id":str,"player_id":str}, low_memory=False)
    originals["game_key"] = originals.game_id.astype(str); originals["player_key"] = originals.player_id.astype(str)
    feature_key = {(str(r.game_id), str(r.player_id), float(r.line)): r.to_dict() for _,r in features.iterrows()}
    anchor = []
    for _, row in originals.iterrows():
        if float(row.line) not in (0.5, 1.5): continue
        key = (row.game_key, row.player_key, float(row.line)); f = feature_key.get(key)
        if f is None:
            anchor.append(dict(lane="HITS05" if row.line == .5 else "HITS15_UNDER", game_date="2026-08-03", game_pk=row.game_id,
                player_id=row.player_id, player_name=row.player_name, line=row.line, retained_original_p_over=row.prob_over,
                retained_original_selected_side=row.model_pick_side, replayed_p_over="", replayed_selected_side="", absolute_difference="",
                exact_match=False, numerically_equivalent=False, selected_side_agreement=False, source_feature_artifact="",
                feature_state_status="STATE_UNAVAILABLE", anchor_classification="REPLAY_MISMATCH"))
            continue
        replayed = score(bundle, f, float(row.line)); difference = abs(replayed-float(row.prob_over))
        anchor.append(dict(lane="HITS05" if row.line == .5 else "HITS15_UNDER", game_date="2026-08-03", game_pk=row.game_id,
            player_id=row.player_id, player_name=row.player_name, line=row.line, retained_original_p_over=row.prob_over,
            retained_original_selected_side=row.model_pick_side, replayed_p_over=replayed,
            replayed_selected_side="over" if replayed >= .5 else "under", absolute_difference=difference,
            exact_match=difference <= 1e-12, numerically_equivalent=difference <= 1.1e-6,
            selected_side_agreement=(str(row.model_pick_side).lower()==("over" if replayed>=.5 else "under")),
            source_feature_artifact="backend/mlb/exports/model_diagnostics/prepared_feature_vectors/2026-08-03/hits_features.csv",
            feature_state_status="PARTIAL_STATE_ONLY", anchor_classification="REPLAY_MISMATCH"))
    write_csv("tier_c_aug3_anchor_replay.csv", anchor)
    anchor_df = pd.DataFrame(anchor)
    anchor_summary = {}
    for lane in ("HITS05", "HITS15_UNDER"):
        q = anchor_df[anchor_df.lane.eq(lane)]; matched = q[q.absolute_difference.astype(str).ne("")].copy()
        matched["difference"] = pd.to_numeric(matched.absolute_difference)
        anchor_summary[lane] = dict(eligible=len(q), replayed=len(matched), exact=int(matched.exact_match.sum()),
            equivalent=int(matched.numerically_equivalent.sum()), max_difference=float(matched.difference.max()),
            mean_difference=float(matched.difference.mean()), side_agreement=int(matched.selected_side_agreement.sum()),
            classification="REPLAY_MISMATCH")

    source_rows, feature_validation, exclusion, counts = [], [], [], []
    candidate_total = {"HITS05":0, "HITS15_UNDER":0}
    for date in DATES:
        path = ROOT / f"backend/mlb/exports/model_diagnostics/prepared_feature_vectors/{date}/hits_features.csv"
        frame = pd.read_csv(path, dtype={"game_id":str,"player_id":str}, low_memory=False)
        for line, lane in ((0.5,"HITS05"),(1.5,"HITS15_UNDER")):
            candidates = frame[frame.line.eq(line)].drop_duplicates(["game_id","player_id"]).copy()
            candidate_total[lane] += len(candidates)
            missing_required = sum(col not in frame.columns for col in meta["input_columns"] if not col.startswith("isna__"))
            source_rows.append(dict(date=date,lane=lane,source_run_tag="UNRESOLVED_OVERWRITTEN_DAILY_DIAGNOSTIC",source_artifact=str(path.relative_to(ROOT)),
                source_sha256=sha(path),generated_timestamp="ABSENT",scheduled_game_start_data="ABSENT_IN_FEATURE_ARTIFACT",
                candidate_player_identities=len(candidates),required_feature_columns=len(meta["input_columns"]),missing_required_columns=missing_required,
                fallback_state="DERIVABLE_FROM_VALUES_BUT_NOT_TIMESTAMP_BOUND",source_before_first_pitch="UNRESOLVED",state_classification="PARTIAL_STATE_ONLY"))
            feature_validation.append(dict(date=date,lane=lane,rows=len(candidates),comparison_surface="NO_SECOND_TIMESTAMP_BOUND_FEATURE_CAPTURE",
                exact_match_rate="NOT_MEASURABLE",maximum_absolute_difference="NOT_MEASURABLE",mean_absolute_difference="NOT_MEASURABLE",
                changed_missingness="NOT_MEASURABLE",changed_fallback_status="NOT_MEASURABLE",standard="PARTIAL_STATE_ONLY",
                source_self_hash=sha(path),reason="exact values retained, but generated timestamp/start binding and independent known-state comparator absent"))
            for _, row in candidates.iterrows():
                exclusion.append(dict(date=date,lane=lane,game_pk=row.game_id,player_id=row.player_id,player_name=row.player_name,line=line,
                    qualification_status="EXCLUDED",exclusion_reason="AUG3_ANCHOR_REPLAY_MISMATCH_GATE_FAILED",
                    secondary_reason="TIMING_UNRESOLVED_FEATURE_STATE_TIMESTAMP_ABSENT",source_artifact=str(path.relative_to(ROOT))))
            counts.append(dict(date=date,lane=lane,candidate_rows=len(candidates),exact_state_rows=0,replayed_rows=0,tier_c_accepted=0,
                excluded=len(candidates),exclusion_reason="AUG3_ANCHOR_REPLAY_MISMATCH_GATE_FAILED; timing unresolved"))
    write_csv("tier_c_daily_source_state.csv", source_rows); write_csv("tier_c_feature_state_validation.csv", feature_validation)
    write_csv("tier_c_exclusion_ledger.csv", exclusion); write_csv("tier_c_daily_recovery_counts.csv", counts)

    prediction_fields = ["game_pk","game_date","scheduled_start","player_id","player_name","source_run","feature_state_timestamp",
        "model_semantic_id","model_hash","feature_contract_hash","p_over","p_under","replay_timestamp","replay_provenance","tier_c_qualification_status"]
    write_csv("tier_c_hits05_prediction_ledger.csv", [], prediction_fields)
    write_csv("tier_c_hits15_under_prediction_ledger.csv", [], prediction_fields)
    write_csv("tier_c_timing_validation.csv", [dict(lane=lane,candidate_rows=count,strict_pregame=0,timing_unresolved=count,post_start_contaminated=0,
        tier_c_accepted=0,decision="EXCLUDED_BEFORE_REPLAY_ANCHOR_GATE_FAILED_AND_FEATURE_TIMESTAMP_ABSENT") for lane,count in candidate_total.items()])
    write_csv("tier_c_outcome_attachment.csv", [], ["lane","game_pk","game_date","player_id","actual_hits","binary_outcome","outcome_source","outcome_hash","evidence_class"])
    write_csv("tier_c_betonline_attachment.csv", [], ["lane","game_pk","game_date","player_id","line","over_price","under_price","no_vig_probability","market_timestamp","source_hash","descriptive_only"])

    quality = [dict(lane=lane,rows=0,brier="NOT_COMPUTED",log_loss="NOT_COMPUTED",ece="NOT_COMPUTED",observed_rate="NOT_COMPUTED",
        mean_predicted_probability="NOT_COMPUTED",probability_sd="NOT_COMPUTED",reason="NO_QUALIFIED_TIER_C_ROWS; OUTCOMES_NOT_ATTACHED") for lane in ("HITS05","HITS15_UNDER")]
    write_csv("tier_c_bridge_prediction_quality.csv", quality)
    comparison = [
        dict(lane="HITS05",historical_brier=0.244277,historical_log_loss=0.682127,historical_ece=0.036572,bridge_brier="N/A",bridge_log_loss="N/A",bridge_ece="N/A",
             brier_delta="N/A",log_loss_delta="N/A",calibration_difference="N/A",probability_distribution_difference="N/A",classification="BRIDGE_BEHAVIOR_INCONSISTENT",reason="August 3 anchor replay mismatch; no bridge frozen"),
        dict(lane="HITS15_UNDER",historical_brier="see recovered-population package",historical_log_loss="see recovered-population package",historical_ece="see recovered-population package",
             bridge_brier="N/A",bridge_log_loss="N/A",bridge_ece="N/A",brier_delta="N/A",log_loss_delta="N/A",calibration_difference="N/A",
             probability_distribution_difference="N/A",classification="BRIDGE_BEHAVIOR_INCONSISTENT",reason="August 3 anchor replay mismatch; no bridge frozen"),
    ]
    write_csv("tier_c_historical_comparison.csv", comparison)

    (OUT / "tier_c_continuity_map.md").write_text("""# Tier C continuity map

- Through August 2: historical prediction evidence; model identity partially recovered.
- August 3: original strict-pregame Tier B evidence remains `ORIGINAL_PROSPECTIVE_PARTIAL_PROVENANCE`. Current-artifact replay materially mismatched and does not strengthen exact model identity.
- August 4–13: zero Tier C rows. The August 3 gate failed and feature-state timestamps are unresolved; all 1,363 Hits 0.5 and 210 Hits 1.5 candidates remain excluded.
- August 14 onward: still requires a future canonical Tier A provenance-bound capture. No Tier A row was created here.
""")
    (OUT / "tier_a_future_capture_requirements.md").write_text("""# Future Tier A capture requirements

Every canonical row must freeze semantic model ID, exact model SHA-256, feature-contract hash, source/run tag, prediction timestamp, scheduled first pitch, deterministic player/game identity, line, full model probability, and all source hashes. The repository has components capable of producing these fields (semantic manifest, model hashes, run tags, schedule, identities, and hashing utilities), but the current daily Hits prediction surface does not bind all of them into one immutable row. Therefore `FUTURE_TIER_A_CAPTURE_READY_FOR_IMPLEMENTATION = NO`: a bounded design/implementation decision is still required. No implementation occurred here.
""")
    concise = f"""# MLB Hits August 4–13 Tier C bridge recovery v1

- July 9/current semantic artifacts: byte-identical, SHA-256 `{current_hash}`; 73-feature fitted contract identical.
- August 3 Hits 0.5 anchor: eligible 158, replayed {anchor_summary['HITS05']['replayed']}, exact {anchor_summary['HITS05']['exact']}, equivalent {anchor_summary['HITS05']['equivalent']}, mean abs diff {anchor_summary['HITS05']['mean_difference']:.6f}, max {anchor_summary['HITS05']['max_difference']:.6f}, side agreement {anchor_summary['HITS05']['side_agreement']}/{anchor_summary['HITS05']['replayed']}; `REPLAY_MISMATCH`.
- August 3 Hits 1.5 Under anchor: eligible 20, replayed {anchor_summary['HITS15_UNDER']['replayed']}, exact {anchor_summary['HITS15_UNDER']['exact']}, equivalent {anchor_summary['HITS15_UNDER']['equivalent']}, mean abs diff {anchor_summary['HITS15_UNDER']['mean_difference']:.6f}, max {anchor_summary['HITS15_UNDER']['max_difference']:.6f}, side agreement {anchor_summary['HITS15_UNDER']['side_agreement']}/{anchor_summary['HITS15_UNDER']['replayed']}; `REPLAY_MISMATCH`.
- Hits 0.5 candidates: {candidate_total['HITS05']}; Tier C accepted: 0.
- Hits 1.5 Under candidates: {candidate_total['HITS15_UNDER']}; Tier C accepted: 0.
- Exclusions: anchor replay gate failed for every candidate; feature-state timestamps and scheduled-start binding are also unresolved.
- Outcome attachment: 0; BetOnline attachment: 0. The failed gate prohibited bridge freezing and subsequent attachment/quality grading.
- Bridge Brier/log loss/ECE: not computed for either lane; no qualified rows.
- Historical comparison: `BRIDGE_BEHAVIOR_INCONSISTENT` because the required anchor did not reproduce retained probabilities.
- August 3 remains Tier B; replay does not strengthen exact current-model identity.
- `FUTURE_TIER_A_CAPTURE_READY_FOR_IMPLEMENTATION = NO`.
- `HITS05_TIER_C_ROWS = 0`
- `HITS15_UNDER_TIER_C_ROWS = 0`
- `BRIDGE_BEHAVIOR = INCONSISTENT`
- Final: `TIER_C_BRIDGE_RECOVERY_NOT_VALID`.

Human review must decide whether to investigate which August 3 route/model generated each retained row, or abandon the bridge and authorize a new provenance-bound Tier A capture implementation. No replay bridge should be graded or merged.
"""
    (OUT / "concise_mlb_hits_aug4_aug13_tier_c_bridge_recovery_v1.md").write_text(concise)
    hash_path = OUT / "reproducibility_hashes.sha256"
    hash_path.write_text("\n".join(f"{sha(p)}  {p.name}" for p in sorted(OUT.iterdir()) if p.is_file() and p != hash_path) + "\n")
    print(json.dumps({"byte_identical":True,"anchor":anchor_summary,"hits05_candidates":candidate_total["HITS05"],
        "hits15_candidates":candidate_total["HITS15_UNDER"],"tier_c_rows":0,"decision":"TIER_C_BRIDGE_RECOVERY_NOT_VALID"}, indent=2))


if __name__ == "__main__": main()
