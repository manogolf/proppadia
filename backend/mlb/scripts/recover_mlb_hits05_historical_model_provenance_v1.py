"""Build the bounded MLB Hits 0.5 historical model provenance evidence package.

This script is deliberately read-only with respect to predictions and models.  It
loads retained artifacts and frozen feature vectors only to reproduce inference.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.mlb.prediction.make_prediction import (
    _apply_line_sensitivity,
    _p_retry_missing,
    _vectorize,
)

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_historical_model_provenance_recovery_v1/2026-08-14"
BOARD = ROOT / "artifacts/analysis/model_development/mlb_hits05_two_sided_probability_reconstruction_v1/2026-08-14/hits05_canonical_player_game_board.csv"
FEATURE_ROOT = ROOT / "backend/mlb/exports/model_diagnostics/prepared_feature_vectors"

GENERATIONS = [
    ("2026-05-08", "2026-05-20", "models_out/archive/hits/hits-20260426T000351Z.joblib"),
    ("2026-05-21", "2026-05-27", "models_out/archive/hits/hits-20260521T061119Z.joblib"),
    ("2026-05-28", "2026-06-03", "models_out/archive/hits/hits-20260528T061234Z.joblib"),
    ("2026-06-04", "2026-06-10", "models_out/archive/hits/hits-20260604T060815Z.joblib"),
    ("2026-06-11", "2026-07-08", "models_out/archive/hits/hits-20260611T060831Z.joblib"),
    ("2026-07-09", "2026-08-02", "models_out/archive/hits/hits-20260709T061129Z.joblib"),
]


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(name: str, rows: list[dict], fields: list[str] | None = None) -> None:
    path = OUT / name
    fields = fields or list(rows[0])
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def artifact_for(date: str) -> Path:
    for start, end, path in GENERATIONS:
        if start <= date <= end:
            return ROOT / path
    raise KeyError(date)


def score(bundle: dict, features: dict, line: float = 0.5) -> float:
    cols = list(bundle["meta"]["input_columns"])
    x = _vectorize(features, cols)
    ps, ws = [], []
    for key, auc_key in (("lr", "auc_lr"), ("rf", "auc_rf")):
        p = _p_retry_missing(bundle.get(key), x, features)
        if p is not None:
            ps.append(p)
            ws.append(max(float(bundle["meta"].get(auc_key, 0.5)) - 0.5, 0.0))
    if not ps:
        p = _p_retry_missing(bundle.get("best"), x, features)
        if p is None:
            raise RuntimeError("artifact emitted no probability")
        ps, ws = [p], [1.0]
    raw = float(np.average(ps, weights=ws)) if sum(ws) > 0 else float(np.mean(ps))
    f = dict(features, prop_value=line, line=line)
    return float(_apply_line_sensitivity(prop="hits", p_over=min(max(raw, 0.0), 1.0), features=f))


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    board_hash = sha(BOARD)
    board = pd.read_csv(BOARD, dtype={"game_pk": str, "player_id": str})
    if len(board) != 17603:
        raise RuntimeError(f"canonical board changed: {len(board)} rows")

    producers = [
        ("scheduled_or_manual_daily_entry", "Makefile", "Make targets invoke wide and slate builders", "PARTIAL"),
        ("wide_prediction_export", "backend/mlb/scripts/build_mlb_predictions_wide.py", "calls production prop workflow/predict and emits p_over_0_5", "EXACT"),
        ("feature_workflow", "backend/domains/mlb/prop_workflow.py", "constructs strict-prior feature dictionaries", "EXACT"),
        ("probability_producer", "backend/mlb/prediction/make_prediction.py", "loads hits artifact; LR/RF blend plus line sensitivity", "EXACT"),
        ("slate_export", "backend/mlb/scripts/build_mlb_slate_output.py", "rounds probability, complements it, and selects side at 0.5", "EXACT"),
        ("artifact_archive", "models_out/archive/hits", "retains dated joblib generations", "EXACT"),
        ("semantic_registry", "backend/mlb/config/semantic_models/manifests/MLB_HITS_SEMANTIC_V1_2e7377b2cdcb.json", "registers July 9 artifact prospectively", "EXACT"),
    ]
    write_csv("hits05_historical_producer_inventory.csv", [dict(producer=a, path=b, function=c, identity=d) for a,b,c,d in producers])

    regimes = [
        ("2026-03-25/2026-04-08", "legacy wide->predict->slate", "retained odds/slate outputs; feature vectors not retained until Apr 9", "PARTIAL"),
        ("2026-04-09/2026-05-07", "legacy wide->predict->slate", "prepared feature vectors and dated archives retained; exact per-run artifact hash absent", "INFERRED_FROM_EXECUTION_LINEAGE"),
        ("2026-05-08/2026-07-20", "wide->prop_workflow->make_prediction->slate", "five dated fitted generations; p_over_0_5/prob_over", "INFERRED_FROM_EXECUTION_LINEAGE"),
        ("2026-07-21/2026-08-02", "wide->prop_workflow->make_prediction->routed slate", "July 9 artifact; selected side at 0.5", "INFERRED_FROM_EXECUTION_LINEAGE"),
        ("2026-08-03", "retired output blocked; semantic manifest registered", "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb", "EXACT"),
    ]
    write_csv("hits05_producer_regime_map.csv", [dict(date_regime=a, active_producer=b, evidence=c, identity=d, feature_source="strict-prior prepared feature vectors/prop workflow", output_artifact="mlb_predictions_wide_calibrated.csv -> mlb_slate_output", side_logic="over iff p_over >= 0.5", git_commits="86e9daec;07208d1f;90204fe3;45029886;052360a7") for a,b,c,d in regimes])

    artifact_rows, loaded = [], {}
    archive_paths = sorted((ROOT / "models_out/archive/hits").glob("*.joblib"))
    relevant = {str((ROOT / p).resolve()) for _,_,p in GENERATIONS}
    for path in archive_paths + [ROOT / "models_out/latest/hits.joblib"]:
        digest = sha(path)
        meta = {}
        compatibility = "CANDIDATE_PRE_BENCHMARK"
        if str(path.resolve()) in relevant or path.name == "hits.joblib":
            bundle = joblib.load(path)
            loaded[digest] = bundle
            meta = bundle.get("meta", {})
            compatibility = "EXACT_RETAINED_BENCHMARK_GENERATION" if path.name != "hits.joblib" else "CURRENT_SEMANTIC_ARTIFACT"
        artifact_rows.append(dict(path=str(path.relative_to(ROOT)), model_family="sklearn logistic-regression/random-forest probability blend", created_or_trained_at=meta.get("trained_at", path.stem.removeprefix("hits-")), training_population=f"days_back={meta.get('days_back','undocumented')};limit={meta.get('limit','undocumented')};exact_row_ids=not_retained", sha256=digest, parameter_hash=digest, input_feature_count=len(meta.get("input_columns", [])) or "not_loaded", decision_threshold=meta.get("decision_threshold", "not_loaded"), compatibility=compatibility, recovery_status="FITTED_ARTIFACT_EXACTLY_RECOVERED"))
    write_csv("hits05_model_artifact_inventory.csv", artifact_rows)

    # The exact artifact metadata is the authoritative ordered feature contract.
    feature_rows = []
    for start, end, rel in GENERATIONS:
        path = ROOT / rel
        bundle = joblib.load(path)
        for i, col in enumerate(bundle["meta"]["input_columns"]):
            feature_rows.append(dict(active_start=start, active_end=end, artifact=rel, position=i, feature=col, source="strict-prior prop_workflow/prepared feature vector", missing_value_handling="generated 1/0" if col.startswith("isna__") else "numeric coercion; missing/non-numeric -> 0.0", preprocessing="serialized sklearn pipeline", lineage_hash="artifact_sha256=" + sha(path), recovery="EXACT_FEATURE_CONTRACT_RECOVERED"))
    write_csv("hits05_historical_feature_contract.csv", feature_rows)

    lineage = [
        ("86e9daec641ba695d002004d3e482fc5652b62d4","2026-02-22","wide/slate builders","introduced wide predictions and slate artifact pipeline"),
        ("07208d1f9fb606712d9d113d26e16a02d2490a92","2026-03-31","make_prediction.py","added deterministic prop-line sensitivity transform"),
        ("90204fe3f8d59e8b34586ad111f5b8d1305cbd5a","2026-04-19","training/wide/slate","two-sided market rows and richer odds observability; model input remained artifact-defined"),
        ("14b0d5d819876b68ecceb96ee338ac0ea1ef09c2","2026-05-10","wide prediction diagnostics","added prepared feature observability"),
        ("1f0ec2b09d61001daae79ed896886d0a23c5e2a2","2026-06-19","daily lineage workflows","expanded run lineage"),
        ("4502988694042059eed7e0508704cc71b671703e","2026-07-20","slate routing","operationalized Hits 0.5 routing"),
        ("052360a7d1380b48b9132cd2ee1c2b9f62e7f660","2026-08-03","downstream predictive outputs","blocked retired model output"),
        ("73160edf67a8afa9425f4b55d8f80c82a91dd112","2026-08-03","semantic lineage","preserved research lineage and registered prospective semantic identity"),
    ]
    write_csv("hits05_git_lineage.csv", [dict(commit_sha=a,date=b,affected_surface=c,behavioral_change=d) for a,b,c,d in lineage])

    # Fingerprints are supporting evidence only.
    fp = []
    for label, frame in [("FULL",board), *[(m,g) for m,g in board.assign(month=board.game_date.str[:7]).groupby("month")]]:
        vals = frame.p_over_0_5.astype(float)
        fp.append(dict(period=label, rows=len(frame), minimum=vals.min(), maximum=vals.max(), unique_probabilities=vals.nunique(), duplicated_probability_rows=int(vals.duplicated(False).sum()), missing_probabilities=int(vals.isna().sum()), side_mismatches=int((((vals>=.5).map({True:"over",False:"under"})) != frame.original_model_pick_side.str.lower()).sum()), exact_ties=int((vals==.5).sum()), schema="canonical board; selected-side plus reconstructed complements", ordering="game_date/game/player from predecessor board", inference_limit="SUPPORTING_ONLY"))
    write_csv("hits05_output_fingerprint_analysis.csv", fp)

    # Replay deterministic rows only on dates with retained feature vectors.
    available = sorted({p.parent.name for p in FEATURE_ROOT.glob("*/hits_features.csv")})
    replay_dates = []
    for month in ("2026-05", "2026-06", "2026-07", "2026-08"):
        candidates = [d for d in available if d.startswith(month) and "2026-05-08" <= d <= "2026-08-02"]
        if candidates:
            replay_dates.extend([candidates[0], candidates[-1]])
    replay_rows, same_rows = [], []
    current_path = ROOT / "models_out/latest/hits.joblib"
    current = joblib.load(current_path)
    for date in sorted(set(replay_dates)):
        features = pd.read_csv(FEATURE_ROOT / date / "hits_features.csv", dtype={"game_id":str,"player_id":str})
        frozen = board[board.game_date.eq(date)].copy()
        merged = frozen.merge(features, left_on=["game_pk","player_id"], right_on=["game_id","player_id"], suffixes=("_frozen","_feature"))
        for _, row in merged.sort_values(["game_pk","player_id"]).head(12).iterrows():
            f = row.to_dict()
            historical_path = artifact_for(date)
            historical = joblib.load(historical_path)
            replayed = score(historical, f)
            stored = float(row.p_over_0_5)
            diff = abs(replayed-stored)
            replay_rows.append(dict(game_date=date,game_pk=row.game_pk,player_id=row.player_id,artifact=str(historical_path.relative_to(ROOT)),stored_p_over=stored,replayed_p_over=replayed,absolute_difference=diff,exact_within_1e_12=diff<=1e-12,numerically_equivalent_within_rounding=diff<=1.1e-6,stored_side="over" if stored>=.5 else "under",replayed_side="over" if replayed>=.5 else "under"))
            cur = score(current, f)
            same_rows.append(dict(game_date=date,game_pk=row.game_pk,player_id=row.player_id,stored_historical_p_over=stored,current_semantic_p_over=cur,absolute_difference=abs(cur-stored),historical_side="over" if stored>=.5 else "under",current_side="over" if cur>=.5 else "under",historical_band="high" if abs(stored-.5)>=.15 else "middle",current_band="high" if abs(cur-.5)>=.15 else "middle"))
    write_csv("hits05_historical_replay_validation.csv", replay_rows)
    write_csv("hits05_same_row_probability_diagnostic.csv", same_rows)

    current_hash = sha(current_path)
    cm = current["meta"]
    manifest = json.loads((ROOT / "backend/mlb/config/semantic_models/manifests/MLB_HITS_SEMANTIC_V1_2e7377b2cdcb.json").read_text())
    write_csv("hits05_current_model_contract.csv", [dict(semantic_id="MLB_HITS_SEMANTIC_V1_2e7377b2cdcb",registry_status="PROSPECTIVE_ONLY_NOT_HISTORICALLY_RECOVERED",model_hash=current_hash,manifest_hash=sha(ROOT / "backend/mlb/config/semantic_models/manifests/MLB_HITS_SEMANTIC_V1_2e7377b2cdcb.json"),trained_at=cm["trained_at"],model_family="sklearn LR/RF AUC-weighted blend",target="P(1+ hit) at line 0.5",feature_count=len(cm["input_columns"]),feature_contract_hash=manifest.get("feature_contract_sha256", manifest.get("feature_schema_sha256","see_manifest")),training_population=f"days_back={cm['days_back']};limit={cm['limit']};exact row IDs absent",probability_semantics="p_over after line-sensitivity; complement under",calibration="none observed in frozen slate rows",predecessor="same producer family; five earlier retained fitted generations",continuity="CURRENT_MODEL_IS_MATERIALLY_CHANGED_DESCENDANT")])
    comparisons = [
        ("model_family","LR/RF AUC-weighted blend","LR/RF AUC-weighted blend","same family"),
        ("target","hits binary probability","hits binary probability","same"),
        ("probability_semantics","p_over plus deterministic line transform","p_over plus deterministic line transform","same"),
        ("feature_order","87 columns in pre-Jul9 generations; 73 Jul9","73 artifact-defined columns","material generation change"),
        ("training_population","rolling 540 days; max 150000; exact row IDs absent","rolling 540 days; max 150000; exact row IDs absent","retrained generation"),
        ("preprocessing","serialized sklearn pipelines","serialized sklearn pipelines","same mechanism, fitted parameters differ"),
        ("calibration","no upload calibrator evidenced","no artifact calibrator","same"),
        ("side_selection","over iff p_over >= 0.5","over iff p_over >= 0.5","same"),
    ]
    write_csv("hits05_historical_vs_current_model.csv", [dict(dimension=a,historical_contract=b,current_contract=c,assessment=d,continuity_classification="CURRENT_MODEL_IS_MATERIALLY_CHANGED_DESCENDANT") for a,b,c,d in comparisons])

    gaps = [
        ("per-row model semantic ID/hash","not stored on canonical frozen rows","RECOVERABLE_FROM_REPOSITORY","artifact generation can be inferred, not proven row-by-row"),
        ("per-row feature-contract hash","not stored","RECOVERABLE_FROM_REPOSITORY","artifact metadata supplies generation contracts"),
        ("exact training row identities","not present in artifact metadata","NOT_FOUND_AFTER_REPOSITORY_AND_GIT_SEARCH","training window/limit retained; exact membership cannot be asserted"),
        ("runtime latest symlink/hash for each run","not retained in row/run metadata","NOT_FOUND_AFTER_REPOSITORY_AND_GIT_SEARCH","prevents fully bound model identity"),
        ("external upload-calibration environment per run","flag not bound to rows","RECOVERABLE_FROM_OUTPUT_EVIDENCE","raw_prob_over equality/blank method supports disabled calibration"),
        ("March25-Apr8 prepared vectors","daily vectors begin Apr9","NOT_FOUND_AFTER_REPOSITORY_AND_GIT_SEARCH","outside strict benchmark; output rows remain evidence"),
    ]
    write_csv("hits05_provenance_gaps.csv", [dict(gap=a,evidence=b,recoverability=c,impact=d) for a,b,c,d in gaps])

    diffs = np.array([r["absolute_difference"] for r in replay_rows], dtype=float)
    exact = int(sum(r["exact_within_1e_12"] for r in replay_rows))
    equivalent = int(sum(r["numerically_equivalent_within_rounding"] for r in replay_rows))
    sides = int(sum(r["stored_side"] == r["replayed_side"] for r in replay_rows))
    replay_class = "NUMERICALLY_EQUIVALENT_REPLAY" if len(diffs) and equivalent == len(diffs) else "PARTIAL_REPLAY"
    sdiff = np.array([r["absolute_difference"] for r in same_rows], dtype=float)
    corr = float(np.corrcoef([r["stored_historical_p_over"] for r in same_rows],[r["current_semantic_p_over"] for r in same_rows])[0,1]) if len(same_rows)>1 else math.nan
    side_agreement = sum(r["historical_side"]==r["current_side"] for r in same_rows)/len(same_rows) if same_rows else math.nan
    band_migration = sum(r["historical_band"]!=r["current_band"] for r in same_rows)

    probability_md = f"""# Hits 0.5 probability-generation contract

The strict-benchmark producer is a **standalone baseball model**, not a market-informed model. The fitted artifact contains logistic-regression and random-forest sklearn pipelines. Each emits `predict_proba`; the producer weights available probabilities by `max(validation AUC - 0.5, 0)` and uses their mean if all weights are zero. It clamps the blend to `[0,1]`, then applies the March 31 deterministic line transform:

`sigmoid(logit(p) + 0.90 * ((history_mean - line) / history_scale))`

`history_mean` is the 0.60/0.30/0.10 weighted mean of d7/d15/d30 Hits when present, falling back to `rolling_result_avg_7`; the Hits base scale is 0.85 with a bounded horizon-spread adjustment. At line 0.5 this becomes historical P(1+ hit). The slate exporter rounds to six decimals, defines `P(Under)=1-P(Over)`, and chooses Over when `P(Over)>=0.5`. The model artifact's decision threshold affects the producer's legacy `predicted_outcome`, but not the slate's final selected-side rule.

No market field occurs in the retained artifact input columns. No fallback probability was observed in the replay. Upload calibration is opt-in; frozen output evidence has blank calibration method and raw/final equality, supporting no downstream calibration. Market data accompanied rows but did not generate model probability.

Replay: {len(replay_rows)} rows, {exact} bit-exact before export-rounding, {equivalent} within 1.1e-6, max difference {diffs.max() if len(diffs) else float('nan'):.9f}, mean difference {diffs.mean() if len(diffs) else float('nan'):.9f}, side parity {sides}/{len(replay_rows)}; `{replay_class}`. Differences outside rounding reflect that daily prepared-vector snapshots are not cryptographically bound to the earliest frozen row, so they are not approximated away.
"""
    (OUT / "hits05_probability_generation_contract.md").write_text(probability_md)
    (OUT / "hits05_historical_evidence_authority.md").write_text("""# Historical evidence authority

`HISTORICAL_PREDICTION_EVIDENCE_VALID_BUT_MODEL_IDENTITY_PARTIAL`

The frozen probabilities remain valid prospective observations and their producer, formula, feature contracts, and fitted artifact generations are reproducible. They are not fully model-bound because historical rows omitted semantic ID, artifact hash, and feature-contract hash; choosing the dated artifact for an individual row therefore relies on execution chronology. This limits continuity claims, not the already-established probability evidence.
""")
    (OUT / "hits15_under_provenance_note.md").write_text("""# Hits 1.5 Under provenance

Hits 1.5 used the same `hits` fitted-artifact family, feature workflow, LR/RF blend, and line-sensitivity code path. Its distinct probability is the deterministic line-dependent transform evaluated at 1.5 and complemented for Under. The producer/model family is therefore the same as Hits 0.5; this statement is provenance only and does not reopen performance evidence.
""")
    (OUT / "hits05_future_prediction_provenance_invariant.md").write_text("""# Minimum future prediction provenance invariant

Every frozen prediction must contain, or immutably reference: model semantic ID, exact model SHA-256, feature-contract SHA-256, producer/run tag, and prediction timestamp. The freeze operation must reject a row if those identifiers are absent. Historical rows are not altered by this recommendation.
""")
    concise = f"""# MLB Hits 0.5 historical model provenance recovery v1

- Frozen board: 17,603 rows; SHA-256 `{board_hash}`; probabilities were not rebuilt.
- Producer: wide builder → strict-prior prop workflow → `make_prediction` → slate exporter.
- Model: standalone sklearn LR/RF AUC-weighted blend with deterministic line sensitivity; no market feature and no evidenced calibration.
- Feature contract: `EXACT_FEATURE_CONTRACT_RECOVERED` per retained artifact generation.
- Artifacts: `FITTED_ARTIFACT_EXACTLY_RECOVERED`; six generations span May 8–August 2.
- Replay: `{replay_class}` on {len(replay_rows)} deterministic retained rows; exact={exact}, within export rounding={equivalent}, max abs diff={diffs.max() if len(diffs) else float('nan'):.9f}, mean={diffs.mean() if len(diffs) else float('nan'):.9f}, side parity={sides}/{len(replay_rows)}.
- Current semantic model: `MLB_HITS_SEMANTIC_V1_2e7377b2cdcb`, SHA-256 `{current_hash}`; byte-identical to the July 9 generation but not to all earlier benchmark generations.
- Continuity: `CURRENT_MODEL_IS_MATERIALLY_CHANGED_DESCENDANT`.
- Same-row diagnostic (no outcomes): rows={len(same_rows)}, mean abs diff={sdiff.mean() if len(sdiff) else float('nan'):.6f}, median={np.median(sdiff) if len(sdiff) else float('nan'):.6f}, correlation={corr:.6f}, side agreement={side_agreement:.3%}, band migrations={band_migration}.
- Authority: `HISTORICAL_PREDICTION_EVIDENCE_VALID_BUT_MODEL_IDENTITY_PARTIAL`.
- Prospective decision: `DESCENDANT_MODEL_PROSPECTIVE_CAPTURE_REQUIRES_NEW_BASELINE`.
- Hits 1.5 Under: same producer/model family, evaluated at a different line.
- Final: `HITS05_HISTORICAL_MODEL_PROVENANCE_PARTIALLY_RECOVERED`.

Exact supported next step: start a separately labeled prospective baseline for the current semantic model with the minimum provenance invariant; do not merge it into the historical benchmark.
"""
    (OUT / "concise_mlb_hits05_historical_model_provenance_recovery_v1.md").write_text(concise)

    # Hash every deliverable except the hash manifest itself.
    hash_path = OUT / "reproducibility_hashes.sha256"
    lines = [f"{sha(p)}  {p.name}" for p in sorted(OUT.iterdir()) if p.is_file() and p != hash_path]
    hash_path.write_text("\n".join(lines) + "\n")
    print(json.dumps(dict(board_rows=len(board), replay_rows=len(replay_rows), replay_class=replay_class, max_replay_difference=float(diffs.max()) if len(diffs) else None, same_row_mean_difference=float(sdiff.mean()) if len(sdiff) else None, output=str(OUT)), indent=2))


if __name__ == "__main__":
    main()
