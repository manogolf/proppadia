#!/usr/bin/env python3
"""Grade Aug 14 Hits 0.5 canonically and extend the exact-current-model record."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_aug14_canonical_grade_and_prospective_update_v1/2026-08-15"
PRIOR = ROOT / "artifacts/analysis/model_development/mlb_hits_aug3_aug13_original_prospective_evidence_v1/2026-08-14/hits_original_prospective_primary_predictions.csv"
CANONICAL = ROOT / "artifacts/analysis/mlb/prospective_lineage_outcomes/2026-08-14/canonical_outcome_reconciliation.csv"
CANONICAL_SUMMARY = ROOT / "artifacts/analysis/mlb/prospective_lineage_outcomes/2026-08-14/canonical_outcome_reconciliation_summary.json"
FRESH = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_first_principles_season_rebuild_v1/2026-08-14/hits05_august_live_reference_comparison.csv"
FRESH_BASELINE = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_first_principles_season_rebuild_v1/2026-08-14/hits05_baseline_comparison.csv"
AUG15_LEDGER = ROOT / "backend/mlb/exports/prospective_lineage/2026-08-15/prediction_lineage_ledger.csv"
MODEL_ARTIFACT = ROOT / "models_out/latest/hits.joblib"
MODEL_ID = "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb"
MODEL_HASH = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"
DATES = [f"2026-08-{day:02d}" for day in range(3, 15)]
CANONICAL_DATES = [f"2026-08-{day:02d}" for day in range(10, 15)]
BINS = [-np.inf, .35, .40, .45, .50, .55, .60, .65, .70, .75, np.inf]
BIN_LABELS = ["<35%", "35-39.99%", "40-44.99%", "45-49.99%", "50-54.99%", "55-59.99%", "60-64.99%", "65-69.99%", "70-74.99%", ">=75%"]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def log_loss(probability: pd.Series, target: pd.Series) -> float:
    p = np.clip(probability.astype(float).to_numpy(), 1e-12, 1 - 1e-12)
    y = target.astype(float).to_numpy()
    return float(np.mean(-(y * np.log(p) + (1 - y) * np.log(1 - p))))


def ece(probability: pd.Series, target: pd.Series) -> float:
    labels = pd.cut(probability.astype(float), BINS, labels=BIN_LABELS, right=False)
    total = len(probability)
    return float(sum(len(group) / total * abs(group.mean() - target.loc[group.index].mean())
                     for _, group in probability.groupby(labels, observed=False) if len(group)))


def metrics(frame: pd.DataFrame) -> dict:
    resolved = frame[frame.actual_hits.notna()].copy()
    p = resolved.p_over.astype(float)
    y = resolved.target.astype(int)
    return {
        "predictions": int(len(frame)),
        "resolved": int(len(resolved)),
        "unresolved": int(frame.actual_hits.isna().sum()),
        "brier": float(np.mean((p - y) ** 2)),
        "log_loss": log_loss(p, y),
        "ece": ece(p, y),
        "accuracy_at_50": float(((p >= .5).astype(int) == y).mean()),
        "mean_probability": float(p.mean()),
        "observed_rate": float(y.mean()),
        "probability_sd": float(p.std(ddof=0)),
        "probability_min": float(p.min()),
        "probability_max": float(p.max()),
        "calibration_gap_predicted_minus_observed": float(p.mean() - y.mean()),
    }


def ordering(frame: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    resolved = frame[frame.actual_hits.notna()].sort_values(["p_over", "identity"]).copy()
    resolved["pct_rank"] = resolved.p_over.rank(method="first", pct=True)
    definitions = [("bottom20", 0, .2), ("second20", .2, .4), ("middle20", .4, .6),
                   ("fourth20", .6, .8), ("top20", .8, 1.0), ("top10", .9, 1.0)]
    rows = []
    for label, low, high in definitions:
        group = resolved[(resolved.pct_rank > low) & (resolved.pct_rank <= high)]
        result = metrics(group)
        rows.append({"quantile": label, "n": len(group), "mean_predicted": result["mean_probability"],
                     "observed_rate": result["observed_rate"], "brier": result["brier"]})
    rates = [row["observed_rate"] for row in rows if row["quantile"] != "top10"]
    diffs = np.diff(rates)
    if np.all(diffs >= 0):
        status = "MONOTONIC"
    elif int((diffs < 0).sum()) <= 1 and float(diffs.min()) >= -.05:
        status = "NEAR_MONOTONIC"
    elif rates[-1] < rates[0]:
        status = "INVERTED"
    elif max(rates) - min(rates) < .03:
        status = "FLAT"
    else:
        status = "PARTIAL"
    table = pd.DataFrame(rows)
    table["confidence_ordering"] = status
    return table, status


def calibration(frame: pd.DataFrame) -> pd.DataFrame:
    resolved = frame[frame.actual_hits.notna()].copy()
    resolved["probability_bin"] = pd.cut(resolved.p_over, BINS, labels=BIN_LABELS, right=False)
    rows = []
    for label in BIN_LABELS:
        group = resolved[resolved.probability_bin == label]
        if group.empty:
            continue
        mean = float(group.p_over.mean())
        observed = float(group.target.mean())
        rows.append({"probability_bin": label, "n": len(group), "mean_predicted": mean,
                     "observed_hit_rate": observed, "calibration_gap_predicted_minus_observed": mean - observed})
    return pd.DataFrame(rows)


def load_predictions() -> pd.DataFrame:
    frames = []
    for date in DATES:
        path = ROOT / f"backend/mlb/exports/prospective_lineage/{date}/prediction_lineage_ledger.csv"
        frame = pd.read_csv(path, low_memory=False)
        identity = frame.canonical_row_identity.map(json.loads)
        frame = frame.assign(
            date=date,
            game_id=identity.map(lambda x: int(x["game_id"])),
            player_id=identity.map(lambda x: int(x["player_id"])),
            prop_type=identity.map(lambda x: str(x["prop_type"]).lower()),
            line=identity.map(lambda x: float(x["line"])),
        )
        frame["prediction_dt"] = pd.to_datetime(frame.prediction_timestamp, utc=True, errors="coerce")
        frame["start_dt"] = pd.to_datetime(frame.scheduled_game_start, utc=True, errors="coerce")
        frame = frame[(frame.prop_type == "hits") & frame.line.eq(.5) & (frame.prediction_dt < frame.start_dt)].copy()
        frame["identity"] = frame.game_id.astype(str) + ":" + frame.player_id.astype(str) + ":hits:0.5"
        frames.append(frame)
    observations = pd.concat(frames, ignore_index=True)
    primary = observations.sort_values(["identity", "prediction_dt", "bookmaker_key"]).drop_duplicates("identity", keep="first").copy()
    exact = primary.model_semantic_name.eq(MODEL_ID) & primary.model_artifact_sha256.eq(MODEL_HASH)
    if not exact.all():
        raise AssertionError(f"non-current-model primary identities={int((~exact).sum())}")
    if not primary.lineage_status.eq("LINEAGE_CERTIFIED").all():
        raise AssertionError("non-certified primary lineage row")
    primary["p_over"] = pd.to_numeric(primary.model_probability_over, errors="raise")
    return primary


def attach_outcomes(primary: pd.DataFrame) -> pd.DataFrame:
    prior = pd.read_csv(PRIOR, low_memory=False)
    prior = prior[(prior.lane == "HITS_0_5") & prior.model_semantic_name.eq(MODEL_ID)
                  & prior.model_artifact_sha256.eq(MODEL_HASH)][["identity", "actual_hits"]]
    if prior.duplicated("identity").any():
        raise AssertionError("duplicate prior outcome identity")
    legacy = primary[primary.date < CANONICAL_DATES[0]].merge(
        prior, on="identity", how="left", validate="one_to_one")
    canonical_frames = []
    for date in CANONICAL_DATES:
        canonical_path = ROOT / f"artifacts/analysis/mlb/prospective_lineage_outcomes/{date}/canonical_outcome_reconciliation.csv"
        summary_path = ROOT / f"artifacts/analysis/mlb/prospective_lineage_outcomes/{date}/canonical_outcome_reconciliation_summary.json"
        canonical = pd.read_csv(canonical_path, low_memory=False)
        canonical = canonical[(canonical.prop_type == "hits") & canonical.line.eq(.5)].copy()
        canonical["identity"] = canonical.canonical_identity
        canonical["actual_hits"] = pd.to_numeric(canonical.actual_value, errors="coerce")
        dated = primary[primary.date == date].merge(
            canonical[["identity", "actual_hits", "outcome_status"]], on="identity", how="left", validate="one_to_one")
        expected = json.loads(summary_path.read_text())["by_prop_line"]["hits:0.5"]
        actual = {
            "predictions": int(len(dated)),
            "resolved": int(dated.actual_hits.notna().sum()),
            "unresolved": int(dated.actual_hits.isna().sum()),
        }
        if actual != expected:
            raise AssertionError(f"{date} canonical population differs from sidecar summary: {actual} != {expected}")
        canonical_frames.append(dated.drop(columns=["outcome_status"]))
    combined = pd.concat([legacy, *canonical_frames], ignore_index=True)
    combined["target"] = np.where(combined.actual_hits.notna(), (combined.actual_hits >= 1).astype(int), np.nan)
    return combined


def aug15_forward_inventory() -> dict:
    frame = pd.read_csv(AUG15_LEDGER, low_memory=False)
    identity = frame.canonical_row_identity.map(json.loads)
    frame = frame.assign(
        game_id=identity.map(lambda x: int(x["game_id"])),
        player_id=identity.map(lambda x: int(x["player_id"])),
        prop_type=identity.map(lambda x: str(x["prop_type"]).lower()),
        line=identity.map(lambda x: float(x["line"])),
    )
    frame = frame[(frame.prop_type == "hits") & frame.line.eq(.5)].copy()
    frame["prediction_dt"] = pd.to_datetime(frame.prediction_timestamp, utc=True, errors="coerce")
    frame["start_dt"] = pd.to_datetime(frame.scheduled_game_start, utc=True, errors="coerce")
    frame["identity"] = frame.game_id.astype(str) + ":" + frame.player_id.astype(str) + ":hits:0.5"
    primary = frame.sort_values(["identity", "prediction_dt", "bookmaker_key"]).drop_duplicates("identity", keep="first")
    exact = primary.model_semantic_name.eq(MODEL_ID) & primary.model_artifact_sha256.eq(MODEL_HASH)
    if not exact.all() or not primary.lineage_status.eq("LINEAGE_CERTIFIED").all() or not (primary.prediction_dt < primary.start_dt).all():
        raise AssertionError("August 15 forward lineage integrity failed")
    return {"identities": int(len(primary)), "ledger_sha256": sha256(AUG15_LEDGER)}


def effect_classification(before: dict, after: dict) -> str:
    brier_delta = after["brier"] - before["brier"]
    log_delta = after["log_loss"] - before["log_loss"]
    if brier_delta < 0 and log_delta < 0:
        return "AUG14_IMPROVED_CUMULATIVE_RECORD"
    if brier_delta > 0 and log_delta > 0:
        return "AUG14_WEAKENED_CUMULATIVE_RECORD"
    return "AUG14_NEUTRAL_VARIATION"


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    primary = load_predictions()
    scored = attach_outcomes(primary)
    before_frame = scored[scored.date <= "2026-08-13"].copy()
    aug14_frame = scored[scored.date == "2026-08-14"].copy()
    before, aug14, after = metrics(before_frame), metrics(aug14_frame), metrics(scored)
    aug14_order, aug14_order_status = ordering(aug14_frame)
    cumulative_order, cumulative_order_status = ordering(scored)
    classification = effect_classification(before, after)

    pd.DataFrame([{**{"date": "2026-08-14", "confidence_ordering": aug14_order_status}, **aug14}]).to_csv(
        OUT / "hits05_aug14_canonical_metrics.csv", index=False, lineterminator="\n")
    calibration(aug14_frame).to_csv(OUT / "hits05_aug14_calibration.csv", index=False, lineterminator="\n")
    aug14_order.to_csv(OUT / "hits05_aug14_confidence_ordering.csv", index=False, lineterminator="\n")
    cumulative_order.to_csv(OUT / "hits05_exact_current_model_aug3_aug14_confidence_ordering.csv", index=False, lineterminator="\n")

    summary = pd.DataFrame([
        {"scope": "THROUGH_AUG13", "date_start": DATES[0], "date_end": "2026-08-13", "dates_represented": 11,
         "confidence_ordering": ordering(before_frame)[1], **before},
        {"scope": "THROUGH_AUG14", "date_start": DATES[0], "date_end": "2026-08-14", "dates_represented": 12,
         "confidence_ordering": cumulative_order_status, **after},
    ])
    summary.to_csv(OUT / "hits05_exact_current_model_aug3_aug14_summary.csv", index=False, lineterminator="\n")
    delta = {"comparison": "THROUGH_AUG14_MINUS_THROUGH_AUG13", "aug14_effect": classification}
    for key in ("brier", "log_loss", "ece", "calibration_gap_predicted_minus_observed"):
        delta[f"through_aug13_{key}"] = before[key]
        delta[f"through_aug14_{key}"] = after[key]
        delta[f"delta_{key}"] = after[key] - before[key]
    pd.DataFrame([delta]).to_csv(OUT / "hits05_aug13_vs_aug14_cumulative_delta.csv", index=False, lineterminator="\n")

    fresh_overlap = pd.read_csv(FRESH).iloc[0]
    fresh_overall = pd.read_csv(FRESH_BASELINE).query("forecast == 'MODEL'").iloc[0]
    direction = "DIRECTION_REMAINS_INTACT" if after["brier"] < float(fresh_overall.brier) and float(fresh_overlap.live_brier) < float(fresh_overlap.fresh_brier) else "DIRECTION_NOT_INTACT"
    canonical_summary = json.loads(CANONICAL_SUMMARY.read_text())
    aug15 = aug15_forward_inventory()
    confirmation = f"""# MLB Hits 0.5 August 14 canonical grading confirmation

`AUG14_CANONICAL_GRADING_AUTHORITY = CONFIRMED`

- Path: authoritative PA completion -> unchanged exact completeness guard -> canonical prospective outcome sidecar.
- August 14 identities/resolved/unresolved: {aug14['predictions']} / {aug14['resolved']} / {aug14['unresolved']}.
- Sidecar decision: `{canonical_summary['decision']}`; duplicate identities: {canonical_summary['duplicate_identities']}.
- Prediction source: original earliest valid strict-pregame lineage; exact semantic model/hash only.
- The old evaluator supplies only the frozen Aug 3-9 outcome reference; repaired canonical sidecars supply Aug 10-14 outcomes.
- No prediction, probability, outcome, model, calibration, certification, publication, or production authority changed.
- August 15 current Hits 0.5 identities: {aug15['identities']}; ledger SHA-256 `{aug15['ledger_sha256']}`.
- `AUG15_HITS_FORWARD_STATUS = READY`; unfinished August 15 games were not graded.
"""
    (OUT / "hits05_aug14_canonical_grading_confirmation.md").write_text(confirmation)
    concise = f"""# MLB Hits 0.5 August 14 canonical grade and prospective update v1

- August 14: {aug14['predictions']} predictions; {aug14['resolved']} resolved; {aug14['unresolved']} unresolved.
- August 14 Brier/log loss/ECE: {aug14['brier']:.6f} / {aug14['log_loss']:.6f} / {aug14['ece']:.6f}.
- August 14 predicted/observed: {aug14['mean_probability']:.4%} / {aug14['observed_rate']:.4%}; ordering `{aug14_order_status}`.
- Aug 3-14 resolved: {after['resolved']}; Brier/log loss/ECE: {after['brier']:.6f} / {after['log_loss']:.6f} / {after['ece']:.6f}.
- Aug 3-14 predicted/observed: {after['mean_probability']:.4%} / {after['observed_rate']:.4%}; ordering `{cumulative_order_status}`.
- August 14 effect: `{classification}`.
- Fresh-start comparison: `{direction}`; descriptive only.
- `AUG14_CANONICAL_GRADING_AUTHORITY = CONFIRMED`.
- `AUG15_HITS_FORWARD_STATUS = READY`.
- August 15 current Hits 0.5 identities remain intact: {aug15['identities']}.
- No certification, recalibration, replay, publication, or production change.
"""
    (OUT / "concise_mlb_hits05_aug14_canonical_grade_and_prospective_update_v1.md").write_text(concise)

    canonical_inputs = [
        item
        for date in CANONICAL_DATES
        for item in (
            ROOT / f"artifacts/analysis/mlb/prospective_lineage_outcomes/{date}/canonical_outcome_reconciliation.csv",
            ROOT / f"artifacts/analysis/mlb/prospective_lineage_outcomes/{date}/canonical_outcome_reconciliation_summary.json",
        )
    ]
    inputs = [PRIOR, FRESH, FRESH_BASELINE, AUG15_LEDGER, MODEL_ARTIFACT, *canonical_inputs,
              *[ROOT / f"backend/mlb/exports/prospective_lineage/{date}/prediction_lineage_ledger.csv" for date in DATES]]
    outputs = [path for path in OUT.iterdir() if path.name != "reproducibility_hashes.csv"]
    hashes = [{"role": "INPUT", "path": str(path.relative_to(ROOT)), "sha256": sha256(path)} for path in inputs]
    hashes += [{"role": "OUTPUT", "path": str(path.relative_to(ROOT)), "sha256": sha256(path)} for path in sorted(outputs)]
    pd.DataFrame(hashes).to_csv(OUT / "reproducibility_hashes.csv", index=False, lineterminator="\n")
    print(json.dumps({"aug14": aug14, "through_aug13": before, "through_aug14": after,
                      "effect": classification, "confidence_ordering": cumulative_order_status,
                      "fresh_direction": direction, "authority": "CONFIRMED", "aug15": "READY"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
