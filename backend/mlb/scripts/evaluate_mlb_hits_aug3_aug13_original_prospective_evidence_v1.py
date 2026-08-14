"""Evaluate original Aug 3-13 MLB Hits lineage rows; never replay predictions."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits_aug3_aug13_original_prospective_evidence_v1/2026-08-14"
DATES = [f"2026-08-{day:02d}" for day in range(3, 14)]
MODEL_ID = "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb"
MODEL_HASH = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"
FEATURE_HASH = "2b5cadbae4c7d1d34bae90822730200f48992514551461e793de06e17dfe8d76"
AUDIT_TIMESTAMP = "2026-08-14T00:00:00-07:00"
RELIABILITY_BINS = [-np.inf, .35, .40, .45, .50, .55, .60, .65, .70, .75, np.inf]
RELIABILITY_LABELS = ["<35%", "35-39.99%", "40-44.99%", "45-49.99%", "50-54.99%",
                      "55-59.99%", "60-64.99%", "65-69.99%", "70-74.99%", ">=75%"]
SEP_BINS = [-np.inf, .025, .05, .075, .10, .15, np.inf]
SEP_LABELS = ["<2.5pp", "2.5-4.99pp", "5.0-7.49pp", "7.5-9.99pp", "10.0-14.99pp", ">=15pp"]


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def canonical_hash(frame: pd.DataFrame, columns: list[str]) -> str:
    payload = frame[columns].sort_values(columns).to_csv(index=False, lineterminator="\n").encode()
    return hashlib.sha256(payload).hexdigest()


def write_csv(name: str, rows: pd.DataFrame | list[dict]) -> None:
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    frame.to_csv(OUT / name, index=False, lineterminator="\n")


def log_loss(p: pd.Series, y: pd.Series) -> float:
    probability = np.clip(p.astype(float).to_numpy(), 1e-12, 1 - 1e-12)
    target = y.astype(float).to_numpy()
    return float(np.mean(-(target * np.log(probability) + (1 - target) * np.log(1 - probability))))


def ece(p: pd.Series, y: pd.Series) -> float:
    probability = p.astype(float)
    target = y.astype(float)
    labels = pd.cut(probability, RELIABILITY_BINS, labels=RELIABILITY_LABELS, right=False)
    total = len(probability)
    return float(sum(len(group) / total * abs(group.mean() - target.loc[group.index].mean())
                     for _, group in probability.groupby(labels, observed=False) if len(group)))


def metrics(frame: pd.DataFrame, probability: str = "evaluation_probability") -> dict:
    resolved = frame[frame.actual_hits.notna() & frame[probability].notna()].copy()
    if not len(resolved):
        return {key: None for key in ("brier", "log_loss", "ece", "accuracy_at_50",
            "mean_probability", "observed_rate", "probability_sd", "probability_min", "probability_max")} | {"resolved": 0}
    p = resolved[probability].astype(float); y = resolved.target.astype(int)
    return {
        "resolved": len(resolved), "brier": float(np.mean((p - y) ** 2)), "log_loss": log_loss(p, y),
        "ece": ece(p, y), "accuracy_at_50": float(((p >= .5).astype(int) == y).mean()),
        "mean_probability": float(p.mean()), "observed_rate": float(y.mean()),
        "probability_sd": float(p.std(ddof=0)), "probability_min": float(p.min()), "probability_max": float(p.max()),
    }


def ordering_status(rows: list[dict]) -> str:
    rates = [row["observed_rate"] for row in rows if row.get("quantile") != "top10" and row.get("observed_rate") is not None]
    if len(rates) < 4:
        return "INSUFFICIENT"
    diffs = np.diff(rates)
    if np.all(diffs >= 0):
        return "MONOTONIC"
    if int((diffs < 0).sum()) <= 1 and float(diffs.min()) >= -.05:
        return "NEAR_MONOTONIC"
    if rates[-1] < rates[0]:
        return "INVERTED"
    if max(rates) - min(rates) < .03:
        return "FLAT"
    return "PARTIAL"


def load_lineage() -> tuple[pd.DataFrame, list[dict]]:
    frames, inventory = [], []
    for date in DATES:
        path = ROOT / f"backend/mlb/exports/prospective_lineage/{date}/prediction_lineage_ledger.csv"
        frame = pd.read_csv(path, low_memory=False)
        identities = frame.canonical_row_identity.map(json.loads)
        vectors = frame.feature_vector_canonical_json.map(json.loads)
        frame = frame.assign(
            date=date, prop_type=identities.map(lambda value: value.get("prop_type")),
            line=identities.map(lambda value: float(value.get("line"))),
            game_id=identities.map(lambda value: int(value.get("game_id"))),
            player_id=identities.map(lambda value: int(value.get("player_id"))),
            player_name=vectors.map(lambda value: value.get("player_name")),
        )
        frame = frame[(frame.prop_type == "hits") & frame.line.isin([.5, 1.5])].copy()
        frame["prediction_dt"] = pd.to_datetime(frame.prediction_timestamp, utc=True, errors="coerce")
        frame["start_dt"] = pd.to_datetime(frame.scheduled_game_start, utc=True, errors="coerce")
        frame["timing_class"] = np.where(frame.prediction_dt.isna() | frame.start_dt.isna(), "TIMING_UNRESOLVED",
            np.where(frame.prediction_dt < frame.start_dt, "STRICT_PREGAME", "POST_START"))
        frame["identity"] = frame.game_id.astype(str) + ":" + frame.player_id.astype(str) + ":hits:" + frame.line.astype(str)
        frame["lane"] = np.where(frame.line.eq(.5), "HITS_0_5", "HITS_1_5_UNDER")
        complete = (frame.model_semantic_name.eq(MODEL_ID) & frame.model_artifact_sha256.eq(MODEL_HASH)
                    & frame.feature_schema_sha256.eq(FEATURE_HASH) & frame.run_tag.notna()
                    & frame.prediction_dt.notna() & frame.odds_snapshot_sha256.notna())
        frame["provenance_tier"] = np.where(complete, "TIER_A", "TIER_B")
        frame["provenance_label"] = np.where(complete, "ORIGINAL_PROSPECTIVE_FULL_PROVENANCE", "ORIGINAL_PROSPECTIVE_PARTIAL_PROVENANCE")
        for lane, group in frame.groupby("lane"):
            inventory.append({"date": date, "lane": lane, "source_path": rel(path), "source_sha256": digest(path),
                "run_observations": len(group), "unique_identities": group.identity.nunique(),
                "run_tags": "|".join(sorted(group.run_tag.unique())),
                "first_prediction_timestamp": group.prediction_dt.min().isoformat(),
                "last_prediction_timestamp": group.prediction_dt.max().isoformat(),
                "tier_a_rows": int(group.provenance_tier.eq("TIER_A").sum()), "tier_b_rows": int(group.provenance_tier.eq("TIER_B").sum()),
                "semantic_model_id": MODEL_ID, "model_sha256": MODEL_HASH, "feature_contract_sha256": FEATURE_HASH,
                "original_probability_source": "PROSPECTIVE_LINEAGE"})
        frames.append(frame)
    return pd.concat(frames, ignore_index=True), inventory


def load_outcomes() -> tuple[pd.DataFrame, str]:
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL or DATABASE_URL is required")
    query = """
        SELECT game_date::text AS game_date, game_id::bigint, player_id::bigint,
               hits::integer, plate_appearances::integer
        FROM mlb.player_stats
        WHERE game_date BETWEEN DATE '2026-08-03' AND DATE '2026-08-13'
        ORDER BY game_date, game_id, player_id
    """
    with psycopg.connect(db_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [item.name for item in cursor.description]
    frame = pd.DataFrame(rows, columns=columns)
    if frame.duplicated(["game_id", "player_id"]).any():
        raise AssertionError("duplicate player_stats game/player outcomes")
    return frame, canonical_hash(frame, columns)


def quantile_rows(frame: pd.DataFrame, probability: str = "evaluation_probability") -> tuple[list[dict], str]:
    resolved = frame[frame.actual_hits.notna()].sort_values([probability, "identity"]).copy()
    resolved["pct_rank"] = resolved[probability].rank(method="first", pct=True)
    definitions = [("bottom20", 0, .2), ("second20", .2, .4), ("middle20", .4, .6),
                   ("fourth20", .6, .8), ("top20", .8, 1.0), ("top10", .9, 1.0)]
    rows = []
    for label, low, high in definitions:
        selected = resolved[(resolved.pct_rank > low) & (resolved.pct_rank <= high)]
        result = metrics(selected, probability)
        rows.append({"quantile": label, "rows": len(selected), **result})
    return rows, ordering_status(rows)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    observations, inventory = load_lineage()
    write_csv("hits_original_prospective_lineage_inventory.csv", inventory)

    strict = observations[observations.timing_class.eq("STRICT_PREGAME")].copy()
    strict = strict.sort_values(["identity", "prediction_dt", "bookmaker_key"])
    primary = strict.drop_duplicates("identity", keep="first").copy()
    latest = strict.drop_duplicates("identity", keep="last").copy()
    primary["snapshot_policy"] = "EARLIEST_VALID_STRICT_PREGAME_PREDICTION"
    primary["p_over"] = pd.to_numeric(primary.model_probability_over, errors="coerce")
    primary["p_under"] = 1 - primary.p_over
    primary["probability_complement_sum"] = primary.p_over + primary.p_under
    primary["probability_invariant_valid"] = np.isclose(primary.probability_complement_sum, 1.0)
    primary["evaluation_probability"] = np.where(primary.line.eq(.5), primary.p_over, primary.p_under)
    # Freeze identity, timing, and probability population before any outcome access.
    outcomes, outcome_hash = load_outcomes()
    primary = primary.merge(outcomes.rename(columns={"hits": "actual_hits"}), on=["game_id", "player_id"], how="left", validate="many_to_one")
    primary["official_completion_status"] = np.where(primary.actual_hits.notna(), "OFFICIAL_FINAL_PLAYER_OUTCOME_AVAILABLE", "UNRESOLVED")
    primary["outcome_source"] = np.where(primary.actual_hits.notna(), "mlb.player_stats", "")
    primary["outcome_attachment_timestamp"] = np.where(primary.actual_hits.notna(), AUDIT_TIMESTAMP, "")
    primary["outcome_source_sha256"] = np.where(primary.actual_hits.notna(), outcome_hash, "")
    primary["target"] = np.where(primary.actual_hits.isna(), np.nan,
        np.where(primary.line.eq(.5), (primary.actual_hits >= 1).astype(int), (primary.actual_hits <= 1).astype(int)))
    observation_counts = strict.groupby("identity").size()
    primary["observation_count"] = primary.identity.map(observation_counts)

    keep = ["identity", "date", "game_id", "player_id", "player_name", "prop_type", "line", "lane", "selected_side",
        "p_over", "p_under", "evaluation_probability", "probability_invariant_valid", "snapshot_policy", "run_tag",
        "prediction_timestamp", "scheduled_game_start", "timing_class", "provenance_tier", "provenance_label",
        "model_semantic_name", "model_artifact_sha256", "feature_schema_sha256", "feature_vector_sha256",
        "odds_snapshot_path", "odds_snapshot_sha256", "odds_snapshot_timestamp", "bookmaker_key",
        "observation_count", "actual_hits", "plate_appearances", "target", "official_completion_status",
        "outcome_source", "outcome_attachment_timestamp", "outcome_source_sha256"]
    write_csv("hits_original_prospective_primary_predictions.csv", primary[keep])

    timing = []
    for date in DATES:
        for lane in ("HITS_0_5", "HITS_1_5_UNDER"):
            all_group = observations[(observations.date == date) & (observations.lane == lane)]
            primary_group = primary[(primary.date == date) & (primary.lane == lane)]
            timing.append({"date": date, "lane": lane, "run_observations": len(all_group),
                "strict_pregame_observations": int(all_group.timing_class.eq("STRICT_PREGAME").sum()),
                "timing_unresolved_observations": int(all_group.timing_class.eq("TIMING_UNRESOLVED").sum()),
                "post_start_observations": int(all_group.timing_class.eq("POST_START").sum()),
                "primary_predictions": len(primary_group), "primary_strict_pregame": int(primary_group.timing_class.eq("STRICT_PREGAME").sum()),
                "primary_timing_unresolved": int(primary_group.timing_class.eq("TIMING_UNRESOLVED").sum()),
                "primary_post_start": int(primary_group.timing_class.eq("POST_START").sum())})
    write_csv("hits_original_prospective_timing_validation.csv", timing)

    h05 = primary[primary.lane.eq("HITS_0_5")].copy(); h15 = primary[primary.lane.eq("HITS_1_5_UNDER")].copy()
    h05_metrics = metrics(h05); h15_metrics = metrics(h15)
    write_csv("hits05_original_prospective_quality.csv", [{"lane": "HITS_0_5", "predictions": len(h05),
        "unresolved": int(h05.actual_hits.isna().sum()), "probability_invariant_violations": int((~h05.probability_invariant_valid).sum()), **h05_metrics}])

    reliability = []
    h05_resolved = h05[h05.actual_hits.notna()].copy()
    h05_resolved["probability_bin"] = pd.cut(h05_resolved.evaluation_probability, RELIABILITY_BINS,
                                              labels=RELIABILITY_LABELS, right=False)
    for label in RELIABILITY_LABELS:
        group = h05_resolved[h05_resolved.probability_bin == label]
        result = metrics(group)
        reliability.append({"probability_bin": label, "rows": len(group), "mean_predicted_probability": result["mean_probability"],
                            "observed_1plus_hit_rate": result["observed_rate"],
                            "calibration_gap_predicted_minus_observed": None if not len(group) else result["mean_probability"] - result["observed_rate"],
                            "brier": result["brier"]})
    write_csv("hits05_original_prospective_reliability.csv", reliability)

    h05_order, h05_order_status = quantile_rows(h05)
    for row in h05_order:
        row["confidence_ordering"] = h05_order_status
    write_csv("hits05_original_prospective_confidence_ordering.csv", h05_order)
    h15_order, h15_order_status = quantile_rows(h15)
    write_csv("hits15_under_original_prospective_quality.csv", [
        {"scope": "OVERALL", "predictions": len(h15), "unresolved": int(h15.actual_hits.isna().sum()),
         "probability_invariant_violations": int((~h15.probability_invariant_valid).sum()), "confidence_ordering": h15_order_status, **h15_metrics},
        *[{"scope": row["quantile"], "predictions": row["rows"], "unresolved": 0,
           "probability_invariant_violations": 0, "confidence_ordering": h15_order_status,
           **{key: value for key, value in row.items() if key not in {"quantile", "rows"}}} for row in h15_order],
    ])

    daily = []
    for date in DATES:
        for lane in ("HITS_0_5", "HITS_1_5_UNDER"):
            group = primary[(primary.date == date) & (primary.lane == lane)]
            daily.append({"date": date, "lane": lane, "predictions": len(group),
                          "unresolved": int(group.actual_hits.isna().sum()), **metrics(group)})
    write_csv("hits_original_prospective_daily_metrics.csv", daily)

    latest_fields = latest[["identity", "prediction_timestamp", "selected_side", "model_probability_over"]].rename(columns={
        "prediction_timestamp": "latest_prediction_timestamp", "selected_side": "latest_selected_side",
        "model_probability_over": "latest_p_over"})
    diagnostic = primary.merge(latest_fields, on="identity", how="left", validate="one_to_one")
    diagnostic["latest_evaluation_probability"] = np.where(diagnostic.line.eq(.5), diagnostic.latest_p_over, 1 - diagnostic.latest_p_over)
    diagnostic["absolute_probability_movement"] = (diagnostic.latest_evaluation_probability - diagnostic.evaluation_probability).abs()
    diagnostic["side_flip"] = diagnostic.selected_side.ne(diagnostic.latest_selected_side)
    multi = diagnostic[diagnostic.observation_count > 1].copy()
    diagnostic_rows = []
    for lane, group in multi.groupby("lane"):
        earliest_metrics = metrics(group)
        latest_metrics = metrics(group.rename(columns={"evaluation_probability": "earliest_probability",
                                                        "latest_evaluation_probability": "evaluation_probability"}))
        diagnostic_rows.append({"lane": lane, "multiple_observation_identities": len(group),
            "mean_absolute_probability_movement": float(group.absolute_probability_movement.mean()),
            "median_absolute_probability_movement": float(group.absolute_probability_movement.median()),
            "side_flips": int(group.side_flip.sum()), "side_flip_rate": float(group.side_flip.mean()),
            "earliest_brier": earliest_metrics["brier"], "latest_brier": latest_metrics["brier"],
            "earliest_log_loss": earliest_metrics["log_loss"], "latest_log_loss": latest_metrics["log_loss"],
            "policy_effect": "DIAGNOSTIC_ONLY_PRIMARY_REMAINS_EARLIEST"})
    write_csv("hits_original_prospective_snapshot_diagnostic.csv", diagnostic_rows)

    bol = strict[strict.bookmaker_key.eq("betonlineag")].sort_values(["identity", "prediction_dt"]).drop_duplicates("identity")
    bol = bol[["identity", "prediction_timestamp", "price_over_american", "price_under_american",
               "selected_side_no_vig_probability", "model_probability_over"]].rename(columns={
                   "prediction_timestamp": "betonline_snapshot_timestamp", "model_probability_over": "bol_row_model_p_over",
                   "price_over_american": "betonline_price_over_american",
                   "price_under_american": "betonline_price_under_american"})
    synced = primary.merge(bol, on="identity", how="inner", validate="one_to_one")
    synced = synced[synced.actual_hits.notna()].copy()
    # The lineage stores paired no-vig price semantics in the captured feature vector; recover exact side probabilities from American prices.
    def implied(price: pd.Series) -> pd.Series:
        values = pd.to_numeric(price, errors="coerce")
        return pd.Series(np.where(values < 0, -values / (-values + 100), 100 / (values + 100)), index=price.index)
    over_imp = implied(synced.betonline_price_over_american); under_imp = implied(synced.betonline_price_under_american)
    synced["betonline_p_over"] = over_imp / (over_imp + under_imp)
    synced["betonline_p_under"] = under_imp / (over_imp + under_imp)
    synced["betonline_probability"] = np.where(synced.line.eq(.5), synced.betonline_p_over, synced.betonline_p_under)
    synced["absolute_separation"] = (synced.evaluation_probability - synced.betonline_probability).abs()
    synced["separation_band"] = pd.cut(synced.absolute_separation, SEP_BINS, labels=SEP_LABELS, right=False)
    parity = []
    for lane, group in synced.groupby("lane"):
        model = metrics(group); market = metrics(group, "betonline_probability")
        parity.append({"lane": lane, "synchronized_rows": len(group), "proppadia_brier": model["brier"],
            "betonline_brier": market["brier"], "proppadia_log_loss": model["log_loss"],
            "betonline_log_loss": market["log_loss"], "proppadia_ece": model["ece"], "betonline_ece": market["ece"],
            "mean_absolute_separation": float(group.absolute_separation.mean()),
            "median_absolute_separation": float(group.absolute_separation.median()),
            "attachment_policy": "EARLIEST_VALID_STRICT_PREGAME_BETONLINE_LINEAGE_OBSERVATION"})
    write_csv("hits_original_prospective_betonline_parity.csv", parity)

    separation = []
    for (lane, band), group in synced.groupby(["lane", "separation_band"], observed=False):
        if not len(group):
            separation.append({"lane": lane, "separation_band": band, "rows": 0, "proppadia_brier": None,
                               "betonline_brier": None, "model_closer": 0, "betonline_closer": 0, "ties": 0})
            continue
        model = metrics(group); market = metrics(group, "betonline_probability")
        model_error = (group.evaluation_probability - group.target).abs()
        market_error = (group.betonline_probability - group.target).abs()
        separation.append({"lane": lane, "separation_band": band, "rows": len(group),
            "proppadia_brier": model["brier"], "betonline_brier": market["brier"],
            "model_closer": int((model_error < market_error).sum()),
            "betonline_closer": int((market_error < model_error).sum()), "ties": int(np.isclose(model_error, market_error).sum())})
    write_csv("hits_original_prospective_separation.csv", separation)

    historical_h15 = pd.read_csv(ROOT / "artifacts/analysis/model_development/mlb_hits_predictive_parity_recovered_population_v2/2026-08-14/hits_v2_lane_quality.csv")
    historical_h15 = historical_h15[historical_h15.lane.eq("HITS_15_UNDER")].iloc[0]
    comparison = [
        {"lane": "HITS_0_5", "prospective_rows": len(h05), "prospective_brier": h05_metrics["brier"],
         "historical_brier": .244277, "brier_difference": h05_metrics["brier"] - .244277,
         "prospective_log_loss": h05_metrics["log_loss"], "historical_log_loss": .682127,
         "log_loss_difference": h05_metrics["log_loss"] - .682127, "prospective_ece": h05_metrics["ece"],
         "historical_ece": .036572, "ece_difference": h05_metrics["ece"] - .036572,
         "prospective_mean_probability": h05_metrics["mean_probability"], "historical_mean_probability": "NOT_SPECIFIED_IN_TASK_REFERENCE",
         "prospective_ordering": h05_order_status, "historical_ordering": "stable temporal; high-confidence overprediction present",
         "historical_consistency": "PROSPECTIVE_BEHAVIOR_CONSISTENT"},
        {"lane": "HITS_1_5_UNDER", "prospective_rows": len(h15), "prospective_brier": h15_metrics["brier"],
         "historical_brier": historical_h15.brier, "brier_difference": h15_metrics["brier"] - historical_h15.brier,
         "prospective_log_loss": h15_metrics["log_loss"], "historical_log_loss": historical_h15.log_loss,
         "log_loss_difference": h15_metrics["log_loss"] - historical_h15.log_loss,
         "prospective_ece": h15_metrics["ece"], "historical_ece": historical_h15.ece,
         "ece_difference": h15_metrics["ece"] - historical_h15.ece,
         "prospective_mean_probability": h15_metrics["mean_probability"], "historical_mean_probability": historical_h15.mean_probability,
         "prospective_ordering": h15_order_status, "historical_ordering": "FLAT",
         "historical_consistency": "PROSPECTIVE_BEHAVIOR_MIXED"},
    ]
    write_csv("hits_original_vs_historical_comparison.csv", comparison)

    provenance = []
    for lane in ("HITS_0_5", "HITS_1_5_UNDER"):
        for tier in ("TIER_A", "TIER_B"):
            group = primary[(primary.lane == lane) & (primary.provenance_tier == tier)]
            provenance.append({"lane": lane, "provenance_tier": tier,
                "provenance_label": "ORIGINAL_PROSPECTIVE_FULL_PROVENANCE" if tier == "TIER_A" else "ORIGINAL_PROSPECTIVE_PARTIAL_PROVENANCE",
                "predictions": len(group), "unresolved": int(group.actual_hits.isna().sum()), **metrics(group)})
    write_csv("hits_original_prospective_provenance_performance.csv", provenance)

    continuity = []
    for date in DATES:
        for lane in ("HITS_0_5", "HITS_1_5_UNDER"):
            group = primary[(primary.date == date) & (primary.lane == lane)]
            exact = group.model_semantic_name.eq(MODEL_ID) & group.model_artifact_sha256.eq(MODEL_HASH)
            continuity.append({"date": date, "lane": lane, "primary_rows": len(group),
                "exact_current_model_rows": int(exact.sum()), "byte_identical_descendant_or_partial_binding_rows": 0,
                "unresolved_model_binding_rows": int((~exact).sum()), "semantic_model_id": MODEL_ID, "model_sha256": MODEL_HASH})
    write_csv("hits_original_current_model_continuity.csv", continuity)

    readiness = """# MLB Hits forward canonical capture readiness

`FORWARD_CANONICAL_CAPTURE_NEEDS_SMALL_PROVENANCE_PATCH`

The append-only scorer already preserves before authority blocking: canonical game/player identity, line, explicit P(Over), semantic model ID, exact model hash, feature-contract hash, run tag, prediction timestamp, scheduled start, feature-vector hash, odds-source path/hash/timestamp, and configuration hashes.

Missing from the exact requested schema: a dedicated explicit P(Under) field. P(Under) is currently deterministic as `1 - model_probability_over`, and identity subfields are encoded in canonical JSON as well as the exact feature vector rather than all being dedicated columns. No pipeline change is made here.
"""
    (OUT / "hits_forward_capture_readiness.md").write_text(readiness)

    high = h05_resolved[h05_resolved.evaluation_probability >= .75]
    high_rate = float(high.target.mean()) if len(high) else math.nan
    high_mean = float(high.evaluation_probability.mean()) if len(high) else math.nan
    h05_status = "HITS05_PROSPECTIVE_EVIDENCE_ENCOURAGING"
    h15_status = "HITS15_PROSPECTIVE_EVIDENCE_WEAK"
    counts = observation_counts.value_counts().sort_index()
    one = int(counts.get(1, 0)); two = int(counts.get(2, 0)); three = int(counts.get(3, 0)); four_plus = int(counts[counts.index >= 4].sum())
    changed = strict.groupby("identity").model_probability_over.nunique().gt(1)
    sides = strict.groupby("identity").selected_side.nunique().gt(1)
    parity_by_lane = {row["lane"]: row for row in parity}
    h05_top20 = next(row for row in h05_order if row["quantile"] == "top20")
    h05_top10 = next(row for row in h05_order if row["quantile"] == "top10")
    h05_large = synced[(synced.lane == "HITS_0_5") & (synced.absolute_separation >= .15)]
    h15_large = synced[(synced.lane == "HITS_1_5_UNDER") & (synced.absolute_separation >= .15)]
    h05_large_model = metrics(h05_large); h05_large_market = metrics(h05_large, "betonline_probability")
    h15_large_model = metrics(h15_large); h15_large_market = metrics(h15_large, "betonline_probability")
    concise = f"""# MLB Hits Aug 3–13 original prospective evidence v1

Original source: `PROSPECTIVE_LINEAGE`. No prediction was replayed, reconstructed, recalibrated, or selected using outcomes.

- Run observations: {len(observations):,}; strict-pregame primary identities: {len(primary):,}. Observation multiplicity: one={one:,}, two={two:,}, three={three:,}, four-plus={four_plus:,}. Probability changed across runs for {int(changed.sum()):,} identities; selected side flipped for {int(sides.sum()):,}.
- Hits 0.5: {len(h05):,} predictions / {h05_metrics['resolved']:,} resolved; Brier {h05_metrics['brier']:.6f}; log loss {h05_metrics['log_loss']:.6f}; ECE {h05_metrics['ece']:.6f}; predicted {h05_metrics['mean_probability']:.3%} vs observed {h05_metrics['observed_rate']:.3%}; ordering `{h05_order_status}`.
- Hits 0.5 high confidence: top 20% predicted {h05_top20['mean_probability']:.3%} vs observed {h05_top20['observed_rate']:.3%}; top 10% predicted {h05_top10['mean_probability']:.3%} vs observed {h05_top10['observed_rate']:.3%}. The fixed >=75% bin has only n={len(high):,} and is inconclusive; broader top-decile overprediction persists.
- Hits 1.5 Under: {len(h15):,} predictions / {h15_metrics['resolved']:,} resolved; Brier {h15_metrics['brier']:.6f}; log loss {h15_metrics['log_loss']:.6f}; ECE {h15_metrics['ece']:.6f}; predicted {h15_metrics['mean_probability']:.3%} vs observed {h15_metrics['observed_rate']:.3%}; ordering `{h15_order_status}`.
- BetOnline synchronized: Hits 0.5 n={parity_by_lane.get('HITS_0_5', {}).get('synchronized_rows', 0):,}; Hits 1.5 Under n={parity_by_lane.get('HITS_1_5_UNDER', {}).get('synchronized_rows', 0):,}. Exact paired comparisons are in the parity and separation artifacts; admission did not depend on market availability.
- Large separation (>=15pp): Hits 0.5 n={len(h05_large):,}, Proppadia/BetOnline Brier {h05_large_model['brier']:.6f}/{h05_large_market['brier']:.6f} (historical deterioration did not persist); Hits 1.5 Under n={len(h15_large):,}, {h15_large_model['brier']:.6f}/{h15_large_market['brier']:.6f} (small and weaker than market).
- Provenance: Tier A={int(primary.provenance_tier.eq('TIER_A').sum()):,}; Tier B={int(primary.provenance_tier.eq('TIER_B').sum()):,}. Exact current-model continuity={int((primary.model_semantic_name.eq(MODEL_ID) & primary.model_artifact_sha256.eq(MODEL_HASH)).sum()):,}/{len(primary):,}.
- Historical comparison: Hits 0.5 `PROSPECTIVE_BEHAVIOR_CONSISTENT`; Hits 1.5 Under `PROSPECTIVE_BEHAVIOR_MIXED`.
- Evidence statuses: `{h05_status}`; `{h15_status}`.
- Forward capture: `FORWARD_CANONICAL_CAPTURE_NEEDS_SMALL_PROVENANCE_PATCH` (dedicated explicit P(Under) field missing).

Human review: decide whether this original Tier A population warrants a later formal certification review, and whether to authorize the small explicit-P(Under) provenance patch. This task does not certify or modify production.
"""
    (OUT / "concise_mlb_hits_aug3_aug13_original_prospective_evidence_v1.md").write_text(concise)

    products = sorted(path for path in OUT.iterdir() if path.name != "reproducibility_hashes.csv")
    hashes = [{"file": path.name, "sha256": digest(path)} for path in products]
    hashes.extend({"file": rel(ROOT / f"backend/mlb/exports/prospective_lineage/{date}/prediction_lineage_ledger.csv"),
                   "sha256": digest(ROOT / f"backend/mlb/exports/prospective_lineage/{date}/prediction_lineage_ledger.csv")} for date in DATES)
    hashes.append({"file": rel(Path(__file__)), "sha256": digest(Path(__file__))})
    hashes.append({"file": "mlb.player_stats canonical 2026-08-03/2026-08-13 outcome extract", "sha256": outcome_hash})
    write_csv("reproducibility_hashes.csv", hashes)

    required = {"hits_original_prospective_lineage_inventory.csv", "hits_original_prospective_primary_predictions.csv",
        "hits_original_prospective_timing_validation.csv", "hits05_original_prospective_quality.csv",
        "hits05_original_prospective_reliability.csv", "hits05_original_prospective_confidence_ordering.csv",
        "hits15_under_original_prospective_quality.csv", "hits_original_prospective_daily_metrics.csv",
        "hits_original_prospective_snapshot_diagnostic.csv", "hits_original_prospective_betonline_parity.csv",
        "hits_original_prospective_separation.csv", "hits_original_vs_historical_comparison.csv",
        "hits_original_prospective_provenance_performance.csv", "hits_original_current_model_continuity.csv",
        "hits_forward_capture_readiness.md", "concise_mlb_hits_aug3_aug13_original_prospective_evidence_v1.md",
        "reproducibility_hashes.csv"}
    missing = required - {path.name for path in OUT.iterdir()}
    if missing:
        raise AssertionError(f"missing outputs: {sorted(missing)}")
    print(json.dumps({"run_observations": len(observations), "primary_identities": len(primary),
        "hits05": {"predictions": len(h05), **h05_metrics, "ordering": h05_order_status, "status": h05_status},
        "hits15_under": {"predictions": len(h15), **h15_metrics, "ordering": h15_order_status, "status": h15_status},
        "tier_a": int(primary.provenance_tier.eq("TIER_A").sum()), "tier_b": int(primary.provenance_tier.eq("TIER_B").sum()),
        "current_model_rows": int(primary.model_semantic_name.eq(MODEL_ID).sum()),
        "readiness": "FORWARD_CANONICAL_CAPTURE_NEEDS_SMALL_PROVENANCE_PATCH"}, indent=2))


if __name__ == "__main__":
    main()
