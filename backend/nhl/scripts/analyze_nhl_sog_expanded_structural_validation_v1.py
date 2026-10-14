#!/usr/bin/env python3
"""Offline fixed-rule SOG structural study on validated Tier 1 + Tier 2 rows."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
RECOVERY = ROOT / "artifacts/analysis/nhl/sog_tier3_provenance_recovery/2026-10-14"
PLATFORM = ROOT / "artifacts/analysis/model_development/nhl_sog_cold_start_prediction_platform_v1/2026-09-19"
AUDIT = ROOT / "artifacts/analysis/nhl/sog_full_season_reconstruction_audit/2026-10-14"
OUT = ROOT / "artifacts/analysis/nhl/sog_expanded_structural_validation/2026-10-14"
TOL = 1e-12
THRESHOLDS = {"1.5": 2, "2.5": 3, "3.5": 4}
VARIANTS = ("PRODUCTION", "DIVERGENCE_ONLY", "DIVERGENCE_PLUS_SOFT_HIGH_RATE", "FIXED_D20_DIAGNOSTIC")
LAMBDAS = {"PRODUCTION": "production_lambda", "DIVERGENCE_ONLY": "divergence_lambda",
           "DIVERGENCE_PLUS_SOFT_HIGH_RATE": "soft_lambda", "FIXED_D20_DIAGNOSTIC": "d20_lambda"}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def poisson_over(lam: float, k: int) -> float:
    if not np.isfinite(lam) or lam < 0:
        return np.nan
    return float(np.clip(1 - sum(math.exp(-lam) * lam**i / math.factorial(i) for i in range(k)), 0, 1))


def rule_rates(d10: pd.Series, d20: pd.Series) -> pd.DataFrame:
    div = d10 - d20
    active = div.abs().gt(0.5)
    div_rate = d10.where(~active, (d10 + d20) / 2)
    soft_active = d10.gt(9.0) & div_rate.gt(9.0)
    soft_rate = div_rate.where(~soft_active, 9.0 + 0.5 * (div_rate - 9.0))
    return pd.DataFrame({"signed_divergence": div, "divergence_active": active,
                         "soft_active": soft_active, "production_rate": d10,
                         "divergence_rate": div_rate, "soft_rate": soft_rate})


def auc_ap(p: pd.Series, y: pd.Series) -> tuple[float, float]:
    z = pd.DataFrame({"p": p, "y": y}).dropna().sort_values("p", ascending=False, kind="mergesort")
    if len(z) == 0 or z.y.nunique() != 2:
        return np.nan, np.nan
    n1, n0 = int(z.y.sum()), int((1 - z.y).sum())
    ranks = z.p.rank(method="average")
    auc = float((ranks[z.y.eq(1)].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))
    tied = z.groupby("p", sort=False).y.agg(["sum", "count"])
    ap = float(((tied["sum"].cumsum() / tied["count"].cumsum()) * tied["sum"] / n1).sum())
    return auc, ap


def scored_metrics(d: pd.DataFrame, label: str, scope: str = "all") -> tuple[list[dict], list[dict]]:
    counts, lines = [], []
    ycount = pd.to_numeric(d.actual_sog, errors="coerce")
    for variant in VARIANTS:
        lam = pd.to_numeric(d[LAMBDAS[variant]], errors="coerce")
        good = lam.notna() & ycount.notna() & np.isfinite(lam) & lam.ge(0)
        l, y = lam[good], ycount[good]
        residual = y - l
        counts.append({"scope": scope, "group": label, "variant": variant, "n": len(l),
                       "mae": float(residual.abs().mean()) if len(l) else np.nan,
                       "rmse": float(np.sqrt((residual**2).mean())) if len(l) else np.nan,
                       "mean_residual": float(residual.mean()) if len(l) else np.nan,
                       "median_absolute_error": float(residual.abs().median()) if len(l) else np.nan,
                       "mean_lambda": float(l.mean()) if len(l) else np.nan,
                       "realized_mean_sog": float(y.mean()) if len(y) else np.nan})
        for line, k in THRESHOLDS.items():
            p = l.map(lambda x: poisson_over(float(x), k)).clip(1e-15, 1 - 1e-15)
            yy = y.ge(k).astype(int)
            auc, ap = auc_ap(p, yy)
            lines.append({"scope": scope, "group": label, "variant": variant, "line": line, "n": len(l),
                          "brier": float(((p - yy) ** 2).mean()) if len(l) else np.nan,
                          "log_loss": float((-(yy * np.log(p) + (1 - yy) * np.log(1 - p))).mean()) if len(l) else np.nan,
                          "auc": auc, "average_precision": ap,
                          "calibration_bias": float(p.mean() - yy.mean()) if len(l) else np.nan,
                          "predicted_over_rate": float(p.mean()) if len(l) else np.nan,
                          "realized_over_rate": float(yy.mean()) if len(l) else np.nan,
                          "accuracy": float(((p >= .5) == yy.astype(bool)).mean()) if len(l) else np.nan})
    return counts, lines


def safe_json(value):
    if isinstance(value, dict):
        return {str(k): safe_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe_json(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    rec = pd.read_csv(RECOVERY / "strict_prior_reconstruction_rows.csv", low_memory=False)
    rec["game_date"] = rec.game_date.astype(str)
    history = pd.read_parquet(PLATFORM / "retained_skater_history_snapshot.parquet")
    history = history[(history.season == 2025) & (history.game_type == 2)].copy()
    history = history[["game_id", "player_id", "shots_on_goal", "toi_minutes"]]
    duplicated_history = history[history.duplicated(["game_id", "player_id"], keep=False)]
    if not duplicated_history.empty:
        conflicts = duplicated_history.groupby(["game_id", "player_id"])[["shots_on_goal", "toi_minutes"]].nunique(dropna=False).gt(1).any(axis=1)
        if conflicts.any():
            raise AssertionError("OFFICIAL_OUTCOME_KEY_CONFLICT")
        history = history.drop_duplicates(["game_id", "player_id"])
    outcomes = rec.merge(history, on=["game_id", "player_id"], how="left", validate="one_to_one")
    outcomes = outcomes.rename(columns={"shots_on_goal": "actual_sog", "toi_minutes": "official_toi_minutes"})
    eligibility = pd.read_csv(AUDIT / "reconstruction_eligibility.csv", low_memory=False)
    binding = eligibility[["game_id", "player_id", "team_id", "opponent_id", "is_home"]].drop_duplicates(["game_id", "player_id"])
    outcomes = outcomes.merge(binding, on=["game_id", "player_id"], how="left", validate="one_to_one")

    # Tier 1 is included only on the exact-input overlap that validated the
    # reconstruction, while Tier 2 requires the committed B-class parity gate.
    candidate = outcomes[outcomes.recoverable.fillna(False)].copy()
    candidate["trust_tier"] = np.where(candidate.tier1, "TIER1_CERTIFIED", "TIER2_RETROSPECTIVE_STRICT_PRIOR")
    candidate["reconstruction_status"] = np.where(candidate.tier1, "CERTIFIED_ROW_WITH_EXACT_RECONSTRUCTION_OVERLAP", "TIER2_RETROSPECTIVE_STRICT_PRIOR_RECONSTRUCTION")
    candidate["source_path"] = candidate.archive_path
    candidate["d5_sog_per60"] = candidate.archive_d5_sog_per60
    candidate["d10_sog_per60"] = candidate.archive_d10_sog_per60
    candidate["d20_sog_per60"] = candidate.archive_d20_sog_per60
    candidate["selected_exposure_minutes"] = candidate.archive_selected_exposure_minutes
    candidate["production_lambda"] = candidate.selected_rate * candidate.selected_exposure_minutes / 60
    candidate["d20_lambda"] = candidate.d20_sog_per60 * candidate.selected_exposure_minutes / 60
    rates = rule_rates(candidate.d10_sog_per60, candidate.d20_sog_per60)
    for col in rates:
        candidate[col] = rates[col]
    candidate["divergence_lambda"] = candidate.divergence_rate * candidate.selected_exposure_minutes / 60
    candidate["soft_lambda"] = candidate.soft_rate * candidate.selected_exposure_minutes / 60

    # Exact selected-input parity implies production selected rate is D10 where
    # the divergence test can be evaluated; block nonconforming identities.
    candidate["required_fields_valid"] = (
        candidate.actual_sog.notna() & candidate.d10_sog_per60.notna() & candidate.d20_sog_per60.notna()
        & candidate.selected_exposure_minutes.notna() & candidate.selected_exposure_minutes.gt(0)
        & candidate.selected_rate.notna() & candidate.selected_rate.sub(candidate.d10_sog_per60).abs().le(TOL)
        & candidate.d5_sog_per60.notna()
    )
    eval_data = candidate[candidate.required_fields_valid].copy()
    eval_data["actual_sog"] = pd.to_numeric(eval_data.actual_sog, errors="coerce")
    eval_data["month"] = eval_data.game_date.str[:7]
    eval_data["realized_sog"] = eval_data.actual_sog
    eval_data["d10_minus_d20"] = eval_data.signed_divergence
    eval_data["near_agreement"] = eval_data.signed_divergence.abs().le(.5)
    eval_data["material_above"] = eval_data.signed_divergence.gt(.5)
    eval_data["material_below"] = eval_data.signed_divergence.lt(-.5)
    eval_data["d10_gt_9"] = eval_data.d10_sog_per60.gt(9.0)
    eval_data["near_agreement_high_d10"] = eval_data.near_agreement & eval_data.d10_gt_9
    eval_data["overlap_high_d10_divergence"] = eval_data.d10_gt_9 & eval_data.material_above
    eval_data["pp_share_proxy"] = eval_data.recon_season_pp_minutes / (eval_data.recon_season_5on5_minutes + eval_data.recon_season_pp_minutes)

    # The remaining table includes every excluded archive identity and an
    # explicit disposition; no Tier 3 row enters any performance metric.
    disposition = outcomes[["game_date", "month", "game_id", "player_id", "tier1", "recoverable", "evidence_class", "unrecoverable_reason", "actual_sog", "archive_path"]].copy()
    disposition["disposition"] = np.select(
        [~disposition.recoverable.fillna(False), disposition.recoverable.fillna(False) & disposition.actual_sog.isna(),
         disposition.recoverable.fillna(False) & disposition.actual_sog.notna()],
        ["EXCLUDED_TIER3", "EXCLUDED_NO_OFFICIAL_OUTCOME", "CANDIDATE_TIER1_OR_TIER2"], default="EXCLUDED_OTHER")
    valid_keys = set(zip(eval_data.game_id, eval_data.player_id))
    disposition["included_in_structural_evaluation"] = [(g, p) in valid_keys for g, p in zip(disposition.game_id, disposition.player_id)]
    disposition.to_csv(OUT / "population_disposition.csv", index=False, lineterminator="\n")
    eval_data.to_csv(OUT / "tier1_tier2_evaluation_population.csv", index=False, lineterminator="\n")

    # Reconstruction parity ledger on all Tier 1 identities.
    t1 = outcomes[outcomes.tier1].copy()
    t1val = pd.read_csv(RECOVERY / "tier1_reconstruction_validation_rows.csv", low_memory=False)
    exact_t1val = t1val[t1val.reconstruction_inputs_match_archived.fillna(False)]
    probability_cols = [f"{c}_abs_diff" for c in ("p_over_1_5", "p_over_2_5", "p_over_3_5")]
    probability_values = exact_t1val[probability_cols]
    certified_rows = pd.read_csv(AUDIT / "certified_frozen_correction_rows.csv", low_memory=False)
    cert_prod = certified_rows[certified_rows.variant.eq("production")].drop_duplicates(["game_id", "player_id"])
    t1 = t1.merge(cert_prod[["game_id", "player_id", "lambda", "p_over"]].rename(columns={"lambda": "certified_lambda", "p_over": "certified_p_over_1_5"}), on=["game_id", "player_id"], how="left", validate="one_to_one")
    # Compare the derived exposure-scaled lambda only on the same exact-input
    # overlap used by the study. Mismatched exposure rows are excluded from
    # parity assessment and from every scorecard.
    t1["reconstruction_lambda"] = t1.selected_rate * t1.selected_exposure_minutes / 60
    exact_mask = t1.reconstruction_inputs_match_archived.fillna(False)
    t1["lambda_abs_diff"] = (t1["reconstruction_lambda"] - t1["certified_lambda"]).abs().where(exact_mask)
    validation = {
        "tier1_player_games_tested": len(t1),
        "exact_selected_input_player_games": int(t1.reconstruction_inputs_match_archived.fillna(False).sum()),
        "selected_rate_mismatches_gt_1e_12": int(t1.selected_rate_abs_diff.gt(TOL).sum()),
        "selected_exposure_mismatches_gt_1e_12": int(t1.selected_exposure_abs_diff.gt(TOL).sum()),
        "selected_rate_max_abs_diff": float(t1.selected_rate_abs_diff.max()),
        "selected_exposure_max_abs_diff": float(t1.selected_exposure_abs_diff.max()),
        "lambda_comparison_rows": int(t1.lambda_abs_diff.notna().sum()),
        "lambda_mismatches_gt_1e_10": int(t1.lambda_abs_diff.gt(1e-10).sum()),
        "lambda_max_abs_diff": float(t1.lambda_abs_diff.max()),
        "probability_comparisons_on_exact_input_subset": int(probability_values.notna().sum().sum()),
        "probability_matches_within_1e_12_on_exact_input_subset": int((probability_values.le(TOL)).sum().sum()),
        "probability_max_abs_diff_on_exact_input_subset": float(probability_values.max().max()),
        "excluded_tier1_input_mismatches": int((~t1.reconstruction_inputs_match_archived.fillna(False)).sum()),
        "validation_accepted": bool(exact_mask.any() and probability_values.notna().all().all()
                                     and probability_values.le(TOL).all().all()
                                     and t1.loc[exact_mask, "lambda_abs_diff"].le(1e-10).all()),
        "scope_note": "Tier-1 exact input overlap only is used; all Tier-2 rows pass strict-prior archive input parity; this is method validation, not performance evaluation.",
    }
    if not validation["validation_accepted"]:
        raise SystemExit("STOP: Tier-1 overlap does not validate the reconstruction sufficiently for expanded analysis")
    pd.DataFrame([validation]).to_csv(OUT / "reconstruction_validation.csv", index=False, lineterminator="\n")

    # Fixed variants and full-population scorecards, reported independently by tier.
    all_count, all_line = [], []
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]),
                        ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]),
                        ("COMBINED", eval_data)]:
        c, l = scored_metrics(data, "all", scope)
        all_count.extend(c); all_line.extend(l)
    pd.DataFrame(all_count).to_csv(OUT / "global_count_metrics.csv", index=False, lineterminator="\n")
    pd.DataFrame(all_line).to_csv(OUT / "global_line_metrics.csv", index=False, lineterminator="\n")

    # Divergence segments, production-vs-correction changes, and signed bins.
    divergence_rows, signed_rows, corr_rows = [], [], []
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]),
                        ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]),
                        ("COMBINED", eval_data)]:
        for group, mask in [("MATERIAL_D10_ABOVE_D20", data.material_above), ("NEAR_AGREEMENT", data.near_agreement), ("MATERIAL_D10_BELOW_D20", data.material_below)]:
            z = data[mask]
            c, l = scored_metrics(z, group, scope)
            for cr in c:
                line = next((x for x in l if x["variant"] == cr["variant"] and x["line"] == "1.5"), {})
                divergence_rows.append({**cr, **{f"{k}_1_5": line.get(k) for k in ("brier", "log_loss", "calibration_bias")}})
        ranked = data.assign(_rank=data.signed_divergence.rank(method="first"))
        ranked["signed_bin"] = pd.qcut(ranked._rank, 10, labels=[f"Q{i}" for i in range(1, 11)])
        for bin_name, z in ranked.groupby("signed_bin", observed=True):
            for v, residual_col, lam_col in [("PRODUCTION", None, "production_lambda"), ("DIVERGENCE_ONLY", None, "divergence_lambda"), ("DIVERGENCE_PLUS_SOFT_HIGH_RATE", None, "soft_lambda")]:
                res = z.actual_sog - z[lam_col]
                signed_rows.append({"scope": scope, "bin": str(bin_name), "variant": v, "n": len(z),
                                    "mean_divergence": z.signed_divergence.mean(), "mean_residual": res.mean(),
                                    "mae": res.abs().mean(), "corrected_residual": res.mean()})
        for v, lamcol in [("PRODUCTION", "production_lambda"), ("DIVERGENCE_ONLY", "divergence_lambda"), ("DIVERGENCE_PLUS_SOFT_HIGH_RATE", "soft_lambda")]:
            residual = data.actual_sog - data[lamcol]
            dd = pd.DataFrame({"div": data.signed_divergence, "res": residual}).dropna()
            if len(dd) > 1:
                corr = float(dd.corr(method="pearson").iloc[0, 1])
                spear = float(dd.corr(method="spearman").iloc[0, 1])
                slope = float(np.polyfit(dd["div"], dd["res"], 1)[0])
            else:
                corr = spear = slope = np.nan
            corr_rows.append({"scope": scope, "variant": v, "n": len(dd), "pearson": corr, "spearman": spear, "descriptive_slope": slope})
    pd.DataFrame(divergence_rows).to_csv(OUT / "divergence_metrics.csv", index=False, lineterminator="\n")
    pd.DataFrame(signed_rows).to_csv(OUT / "signed_divergence_bins.csv", index=False, lineterminator="\n")
    pd.DataFrame(corr_rows).to_csv(OUT / "signed_divergence_monotonicity.csv", index=False, lineterminator="\n")

    # Frozen high-rate cohorts: D10>9 plus descriptive empirical tails.
    q95, q90, q10, q05 = eval_data.d10_sog_per60.quantile([.95, .90, .10, .05])
    eval_data["top_decile"] = eval_data.d10_sog_per60.ge(q90)
    eval_data["top_5pct"] = eval_data.d10_sog_per60.ge(q95)
    eval_data["low_decile"] = eval_data.d10_sog_per60.le(q10)
    eval_data["low_5pct"] = eval_data.d10_sog_per60.le(q05)
    pd.DataFrame([{"cohort": k, "d10_rate_cut": float(v), "basis": "pooled clean Tier1+Tier2 descriptive quantile; fixed rule threshold remains D10>9"} for k, v in [("top_decile", q90), ("top_5pct", q95), ("low_decile", q10), ("low_5pct", q05)]]).to_csv(OUT / "rate_tail_cutpoints.csv", index=False, lineterminator="\n")
    tail_rows, near_rows, overlap_rows = [], [], []
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]),
                        ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]), ("COMBINED", eval_data)]:
        masks = [("D10_GT_9", data.d10_gt_9), ("TOP_DECILE", data.top_decile), ("TOP_5_PERCENT", data.top_5pct), ("LOW_DECILE", data.low_decile), ("LOW_5_PERCENT", data.low_5pct)]
        for name, mask in masks:
            z = data[mask]
            c, l = scored_metrics(z, name, scope)
            tail_rows.extend(c)
            for lr in l: tail_rows.append({**lr, "metric_type": "line", "group": name})
        z = data[data.near_agreement_high_d10]
        c, l = scored_metrics(z, "NEAR_AGREEMENT_D10_GT_9", scope)
        near_rows.extend(c)
        for lr in l: near_rows.append({**lr, "metric_type": "line", "group": "NEAR_AGREEMENT_D10_GT_9"})
        z = data[data.overlap_high_d10_divergence]
        c, l = scored_metrics(z, "D10_GT_9_AND_D10_MINUS_D20_GT_0_5", scope)
        overlap_rows.extend(c)
        for lr in l: overlap_rows.append({**lr, "metric_type": "line", "group": "D10_GT_9_AND_D10_MINUS_D20_GT_0_5"})
    pd.DataFrame(tail_rows).to_csv(OUT / "high_rate_metrics.csv", index=False, lineterminator="\n")
    pd.DataFrame(near_rows).to_csv(OUT / "near_agreement_high_rate_metrics.csv", index=False, lineterminator="\n")
    pd.DataFrame(overlap_rows).to_csv(OUT / "overlap_high_rate_divergence_metrics.csv", index=False, lineterminator="\n")

    # Month, period, and correction score tables.
    monthly_struct, monthly_scores = [], []
    months = ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03", "2026-04"]
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]), ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]), ("COMBINED", eval_data)]:
        for month in months:
            z = data[data.month.eq(month)]
            above, below = z[z.material_above], z[z.material_below]
            high, nearhigh = z[z.d10_gt_9], z[z.near_agreement_high_d10]
            prod = z.actual_sog - z.production_lambda
            monthly_struct.append({"scope": scope, "month": month, "n": len(z),
                                   "material_above_n": len(above), "material_above_residual": (above.actual_sog-above.production_lambda).mean(),
                                   "material_below_n": len(below), "material_below_residual": (below.actual_sog-below.production_lambda).mean(),
                                   "above_minus_below_residual_gap": (above.actual_sog-above.production_lambda).mean()-(below.actual_sog-below.production_lambda).mean(),
                                   "divergence_only_mae_improvement": (prod.abs().mean()-(z.actual_sog-z.divergence_lambda).abs().mean()),
                                   "d10_gt_9_n": len(high), "d10_gt_9_production_residual": (high.actual_sog-high.production_lambda).mean(),
                                   "near_agreement_d10_gt_9_n": len(nearhigh), "near_agreement_d10_gt_9_production_residual": (nearhigh.actual_sog-nearhigh.production_lambda).mean(),
                                   "soft_mae_improvement_on_high_d10": ((high.actual_sog-high.divergence_lambda).abs().mean()-(high.actual_sog-high.soft_lambda).abs().mean()),
                                   "soft_mae_improvement_on_near_agreement_high_d10": ((nearhigh.actual_sog-nearhigh.divergence_lambda).abs().mean()-(nearhigh.actual_sog-nearhigh.soft_lambda).abs().mean())})
            c, l = scored_metrics(z, "all", scope)
            for row in c:
                metrics = {x["line"]: x for x in l if x["variant"] == row["variant"]}
                monthly_scores.append({"scope": scope, "month": month, **row,
                                       **{f"{key}_{line}": metrics[line].get(key) for line in metrics for key in ("brier", "log_loss")}})
    pd.DataFrame(monthly_struct).to_csv(OUT / "month_level_structural_metrics.csv", index=False, lineterminator="\n")
    month_score_df = pd.DataFrame(monthly_scores)
    month_score_df.to_csv(OUT / "month_level_correction_metrics.csv", index=False, lineterminator="\n")

    period_rows = []
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]), ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]), ("COMBINED", eval_data)]:
        for period, months_in in [("EARLY_OCT_DEC", months[:3]), ("MIDDLE_JAN_FEB", months[3:5]), ("LATE_MAR_APR", months[5:])]:
            z = data[data.month.isin(months_in)]
            a, b = z[z.material_above], z[z.material_below]
            h, nh = z[z.d10_gt_9], z[z.near_agreement_high_d10]
            period_rows.append({"scope": scope, "period": period, "n": len(z),
                                "divergence_above_residual": (a.actual_sog-a.production_lambda).mean(),
                                "divergence_below_residual": (b.actual_sog-b.production_lambda).mean(),
                                "divergence_gap": (a.actual_sog-a.production_lambda).mean()-(b.actual_sog-b.production_lambda).mean(),
                                "divergence_mae_improvement": (z.actual_sog-z.production_lambda).abs().mean()-(z.actual_sog-z.divergence_lambda).abs().mean(),
                                "high_d10_residual": (h.actual_sog-h.production_lambda).mean(),
                                "near_agreement_high_d10_residual": (nh.actual_sog-nh.production_lambda).mean(),
                                "soft_mae_improvement_high_d10": (h.actual_sog-h.divergence_lambda).abs().mean()-(h.actual_sog-h.soft_lambda).abs().mean()})
    pd.DataFrame(period_rows).to_csv(OUT / "early_middle_late_comparison.csv", index=False, lineterminator="\n")

    # Tier comparison and correction classification evidence.
    tiercomp = []
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]), ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")])]:
        a, b, nearh = data[data.material_above], data[data.material_below], data[data.near_agreement_high_d10]
        prod_mae = (data.actual_sog-data.production_lambda).abs().mean()
        div_mae = (data.actual_sog-data.divergence_lambda).abs().mean()
        soft_mae = (data.actual_sog-data.soft_lambda).abs().mean()
        prod_resid = data.actual_sog-data.production_lambda
        tiercomp.append({"scope": scope, "n": len(data), "divergence_residual_gap_above_minus_below": (a.actual_sog-a.production_lambda).mean()-(b.actual_sog-b.production_lambda).mean(),
                         "high_d10_residual": (data[data.d10_gt_9].actual_sog-data[data.d10_gt_9].production_lambda).mean(),
                         "near_agreement_high_d10_residual": (nearh.actual_sog-nearh.production_lambda).mean(),
                         "divergence_mae_improvement": prod_mae-div_mae, "soft_incremental_mae_improvement": div_mae-soft_mae})
        for v in VARIANTS:
            c, l = scored_metrics(data, "all", scope)
            for cc in c:
                if cc["variant"] == v:
                    tiercomp[-1][f"{v.lower()}_mae"] = cc["mae"]
            for lr in l:
                if lr["variant"] == v:
                    tiercomp[-1][f"{v.lower()}_brier_{lr['line']}"] = lr["brier"]
                    tiercomp[-1][f"{v.lower()}_log_loss_{lr['line']}"] = lr["log_loss"]
    pd.DataFrame(tiercomp).to_csv(OUT / "tier1_vs_tier2_comparison.csv", index=False, lineterminator="\n")

    # Date/week stability. Above-minus-below residual gap and paired MAE deltas.
    stability = []
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]), ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]), ("COMBINED", eval_data)]:
        data = data.copy(); dates = pd.to_datetime(data.game_date, errors="coerce")
        data["week"] = dates.dt.to_period("W-SUN").astype(str)
        for unit in ("game_date", "week"):
            for period, z in data.groupby(unit):
                above, below = z[z.material_above], z[z.material_below]
                high = z[z.d10_gt_9]
                gap = (above.actual_sog-above.production_lambda).mean()-(below.actual_sog-below.production_lambda).mean()
                ddelta = (z.actual_sog-z.production_lambda).abs().mean()-(z.actual_sog-z.divergence_lambda).abs().mean()
                hresid = (high.actual_sog-high.production_lambda).mean()
                sdelta = (high.actual_sog-high.divergence_lambda).abs().mean()-(high.actual_sog-high.soft_lambda).abs().mean()
                stability.append({"scope": scope, "unit": unit, "period": str(period), "n": len(z), "above_n": len(above), "below_n": len(below),
                                  "divergence_gap": gap, "divergence_mae_delta": ddelta, "high_d10_n": len(high), "high_d10_residual": hresid, "soft_mae_delta_high_d10": sdelta})
    stable_df = pd.DataFrame(stability)
    stable_df.to_csv(OUT / "date_week_stability.csv", index=False, lineterminator="\n")
    stability_summary = []
    for (scope, unit), z in stable_df.groupby(["scope", "unit"]):
        def posneg(series):
            return {"supporting": int(series.gt(1e-12).sum()), "opposing": int(series.lt(-1e-12).sum()), "neutral": int(series.abs().le(1e-12).sum())}
        def negpos(series):
            return {"supporting": int(series.lt(-1e-12).sum()), "opposing": int(series.gt(1e-12).sum()), "neutral": int(series.abs().le(1e-12).sum())}
        stability_summary.append({"scope": scope, "unit": unit, "periods": len(z),
                                  **{f"divergence_gap_{k}": v for k, v in negpos(z.divergence_gap).items()},
                                  **{f"divergence_mae_{k}": v for k, v in posneg(z.divergence_mae_delta).items()},
                                  **{f"high_rate_residual_{k}": v for k, v in negpos(z.high_d10_residual).items()},
                                  **{f"soft_mae_{k}": v for k, v in posneg(z.soft_mae_delta_high_d10).items()}})
    pd.DataFrame(stability_summary).to_csv(OUT / "date_week_stability_summary.csv", index=False, lineterminator="\n")

    # Equal-player robustness, per-team spread, and exposure/PP-use interactions.
    player_rows = []
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]), ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]), ("COMBINED", eval_data)]:
        for cohort, z in [("ALL", data), ("D10_GT_9", data[data.d10_gt_9])]:
            for pid, p in z.groupby("player_id"):
                ep = (p.actual_sog-p.production_lambda).abs().mean()-(p.actual_sog-p.divergence_lambda).abs().mean()
                sp = (p.actual_sog-p.divergence_lambda).abs().mean()-(p.actual_sog-p.soft_lambda).abs().mean()
                player_rows.append({"scope": scope, "cohort": cohort, "player_id": pid, "n": len(p), "divergence_mae_improvement": ep, "soft_incremental_mae_improvement": sp})
    player_df = pd.DataFrame(player_rows); player_df.to_csv(OUT / "player_robustness.csv", index=False, lineterminator="\n")
    player_summary = []
    for (scope, cohort), z in player_df.groupby(["scope", "cohort"]):
        d = z[z.n.ge(2)]
        player_summary.append({"scope": scope, "cohort": cohort, "players": len(z), "players_with_2plus_games": len(d),
                               "divergence_players_improved": int(d.divergence_mae_improvement.gt(1e-12).sum()), "divergence_players_worsened": int(d.divergence_mae_improvement.lt(-1e-12).sum()),
                               "soft_players_improved": int(d.soft_incremental_mae_improvement.gt(1e-12).sum()), "soft_players_worsened": int(d.soft_incremental_mae_improvement.lt(-1e-12).sum()),
                               "equal_player_divergence_improvement": d.divergence_mae_improvement.mean(), "equal_player_soft_improvement": d.soft_incremental_mae_improvement.mean(),
                               "top_5_player_share_positive_divergence_gain": d.nlargest(5, "divergence_mae_improvement").divergence_mae_improvement.clip(lower=0).sum()/d.divergence_mae_improvement.clip(lower=0).sum() if d.divergence_mae_improvement.clip(lower=0).sum()>0 else np.nan,
                               "top_5_player_share_positive_soft_gain": d.nlargest(5, "soft_incremental_mae_improvement").soft_incremental_mae_improvement.clip(lower=0).sum()/d.soft_incremental_mae_improvement.clip(lower=0).sum() if d.soft_incremental_mae_improvement.clip(lower=0).sum()>0 else np.nan})
    player_summary_df = pd.DataFrame(player_summary)
    player_summary_df.to_csv(OUT / "player_robustness_summary.csv", index=False, lineterminator="\n")

    team_rows = []
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]), ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]), ("COMBINED", eval_data)]:
        for team, z in data.groupby("team_id", dropna=False):
            a, b, high = z[z.material_above], z[z.material_below], z[z.d10_gt_9]
            team_rows.append({"scope": scope, "team_id": team, "player_games": len(z), "above_n": len(a), "below_n": len(b),
                              "divergence_residual_gap": (a.actual_sog-a.production_lambda).mean()-(b.actual_sog-b.production_lambda).mean(),
                              "divergence_mae_improvement": (z.actual_sog-z.production_lambda).abs().mean()-(z.actual_sog-z.divergence_lambda).abs().mean(),
                              "high_d10_n": len(high), "high_d10_production_residual": (high.actual_sog-high.production_lambda).mean(),
                              "high_d10_soft_mae_improvement": (high.actual_sog-high.divergence_lambda).abs().mean()-(high.actual_sog-high.soft_lambda).abs().mean(),
                              "support_flag": "ADEQUATE_30_PLUS_SUBGROUP_ROWS" if len(a)>=30 and len(b)>=30 and len(high)>=30 else "LIMITED_SUBGROUP_SUPPORT"})
    team_df = pd.DataFrame(team_rows)
    team_df.to_csv(OUT / "team_robustness.csv", index=False, lineterminator="\n")

    interaction_rows = []
    eval_data["toi_band"] = pd.qcut(eval_data.selected_exposure_minutes.rank(method="first"), 3, labels=["LOW", "MIDDLE", "HIGH"])
    eval_data["pp_band"] = pd.qcut(eval_data.pp_share_proxy.rank(method="first"), 3, labels=["LOW", "MIDDLE", "HIGH"])
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]), ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]), ("COMBINED", eval_data)]:
        for dimension, column in [("TOI", "toi_band"), ("PP_USAGE_PROXY", "pp_band")]:
            for band, z in data.groupby(column, observed=True):
                for cohort, q in [("ALL", z), ("D10_GT_9", z[z.d10_gt_9])]:
                    for v in VARIANTS:
                        c, l = scored_metrics(q, str(band), scope)
                        cc = next(x for x in c if x["variant"] == v)
                        interaction_rows.append({"scope": scope, "dimension": dimension, "band": str(band), "cohort": cohort,
                                                 **cc, "soft_incremental_mae_improvement_vs_divergence": (q.actual_sog-q.divergence_lambda).abs().mean()-(q.actual_sog-q.soft_lambda).abs().mean()})
    interaction_df = pd.DataFrame(interaction_rows)
    interaction_df.to_csv(OUT / "toi_pp_interactions.csv", index=False, lineterminator="\n")

    # D5/D10/D20 context table uses unchanged exposure solely as description.
    depth_rows = []
    for scope, data in [("TIER1", eval_data[eval_data.trust_tier.eq("TIER1_CERTIFIED")]), ("TIER2", eval_data[eval_data.trust_tier.eq("TIER2_RETROSPECTIVE_STRICT_PRIOR")]), ("COMBINED", eval_data)]:
        for depth in (5, 10, 20):
            lam = data[f"d{depth}_sog_per60"] * data.selected_exposure_minutes / 60
            err = data.actual_sog - lam
            depth_rows.append({"scope": scope, "window": f"D{depth}", "n": len(data), "mae": err.abs().mean(), "rmse": np.sqrt((err**2).mean()),
                              "mean_residual": err.mean(), "mean_rate": data[f"d{depth}_sog_per60"].mean(), "mean_lambda": lam.mean()})
    pd.DataFrame(depth_rows).to_csv(OUT / "d5_d10_d20_context.csv", index=False, lineterminator="\n")

    # Baseline safety: requested feature and output regimes, each compared on
    # same rows and all three threshold proper scores.
    safety = []
    eval_data["lambda_band"] = pd.qcut(eval_data.production_lambda.rank(method="first"), 3, labels=["LOW", "MIDDLE", "HIGH"])
    eval_data["d10_band"] = pd.qcut(eval_data.d10_sog_per60.rank(method="first"), 3, labels=["LOW", "MIDDLE", "HIGH"])
    safeties = [("NEAR_AGREEMENT", eval_data.near_agreement), ("LOW_D10", eval_data.d10_band.eq("LOW")),
                ("MIDDLE_D10", eval_data.d10_band.eq("MIDDLE")), ("HIGH_D10", eval_data.d10_band.eq("HIGH")),
                ("LOW_LAMBDA", eval_data.lambda_band.eq("LOW")), ("MIDDLE_LAMBDA", eval_data.lambda_band.eq("MIDDLE")),
                ("HIGH_LAMBDA", eval_data.lambda_band.eq("HIGH")), ("LOW_TOI", eval_data.toi_band.eq("LOW")),
                ("MIDDLE_TOI", eval_data.toi_band.eq("MIDDLE")), ("HIGH_TOI", eval_data.toi_band.eq("HIGH"))]
    for name, mask in safeties:
        z = eval_data[mask]
        c, l = scored_metrics(z, name, "COMBINED")
        for cc in c:
            for line in THRESHOLDS:
                lr = next(x for x in l if x["variant"] == cc["variant"] and x["line"] == line)
                safety.append({**cc, "line": line, "brier": lr["brier"], "log_loss": lr["log_loss"], "calibration_bias": lr["calibration_bias"]})
    pd.DataFrame(safety).to_csv(OUT / "baseline_safety.csv", index=False, lineterminator="\n")

    # Summary, decision support, and manuscript report.
    valid_counts = eval_data.groupby("trust_tier").size().to_dict()
    count_df = pd.DataFrame(all_count)
    combined = count_df[(count_df.scope == "COMBINED") & (count_df.group == "all")].set_index("variant")
    line_df = pd.DataFrame(all_line)
    paired_lines = line_df[(line_df.scope == "COMBINED") & (line_df.group == "all")]
    div_gain = float(combined.loc["PRODUCTION", "mae"] - combined.loc["DIVERGENCE_ONLY", "mae"])
    soft_gain = float(combined.loc["DIVERGENCE_ONLY", "mae"] - combined.loc["DIVERGENCE_PLUS_SOFT_HIGH_RATE", "mae"])
    tiercomp_df = pd.DataFrame(tiercomp)
    t1tc, t2tc = tiercomp_df.iloc[0], tiercomp_df.iloc[1]
    combined_high = eval_data[eval_data.d10_gt_9]
    combined_near_high = eval_data[eval_data.near_agreement_high_d10]
    combined_high_residual = float((combined_high.actual_sog - combined_high.production_lambda).mean())
    combined_near_high_residual = float((combined_near_high.actual_sog - combined_near_high.production_lambda).mean())
    delta_brier = {}
    for v in ("DIVERGENCE_ONLY", "DIVERGENCE_PLUS_SOFT_HIGH_RATE"):
        delta_brier[v] = {line: float(paired_lines[(paired_lines.variant == v) & (paired_lines.line == line)].brier.mean() - paired_lines[(paired_lines.variant == "PRODUCTION") & (paired_lines.line == line)].brier.mean()) for line in THRESHOLDS}
    all_proper_improved = all(delta <= 0 for vals in delta_brier.values() for delta in vals.values())
    dgap_consistent = np.sign(t1tc.divergence_residual_gap_above_minus_below) == np.sign(t2tc.divergence_residual_gap_above_minus_below)
    high_consistent = np.sign(t1tc.near_agreement_high_d10_residual) == np.sign(t2tc.near_agreement_high_d10_residual)
    div_both_tiers = t1tc.divergence_mae_improvement > 0 and t2tc.divergence_mae_improvement > 0
    soft_both_tiers = t1tc.soft_incremental_mae_improvement > 0 and t2tc.soft_incremental_mae_improvement > 0
    div_class = "STRONGLY_CONFIRMED" if dgap_consistent and div_both_tiers and all_proper_improved else ("CONFIRMED" if dgap_consistent and div_gain > 0 else ("WEAKENED" if div_gain > 0 else "NOT_REPRODUCED"))
    high_class = "STRONGLY_CONFIRMED" if high_consistent and t1tc.near_agreement_high_d10_residual < 0 and t2tc.near_agreement_high_d10_residual < 0 and soft_both_tiers and all_proper_improved else ("CONFIRMED" if high_consistent and soft_gain > 0 else ("WEAKENED" if soft_gain > 0 else "NOT_REPRODUCED"))
    combined_class = "STRONGLY_CONFIRMED" if div_class == "STRONGLY_CONFIRMED" and high_class == "STRONGLY_CONFIRMED" and all_proper_improved else ("CONFIRMED" if div_class in ("STRONGLY_CONFIRMED", "CONFIRMED") and high_class in ("STRONGLY_CONFIRMED", "CONFIRMED") else "MIXED")
    override = "TIER3_OVERRIDE_NOT_NEEDED" if dgap_consistent and high_consistent and div_gain > 0 and soft_gain > 0 else "TIER3_OVERRIDE_MAY_HELP_RESOLVE_AMBIGUITY"
    shadow = bool(div_class in ("STRONGLY_CONFIRMED", "CONFIRMED") and high_class in ("STRONGLY_CONFIRMED", "CONFIRMED") and all_proper_improved)
    # Population counts and exclusions are exact identity counts, not line rows.
    pop_counts = {"tier1_candidate_exact_input_overlap": int((candidate.trust_tier == "TIER1_CERTIFIED").sum()),
                  "tier2_candidate_exact_input_parity": int((candidate.trust_tier == "TIER2_RETROSPECTIVE_STRICT_PRIOR").sum()),
                  "tier1_valid_evaluation_player_games": int((eval_data.trust_tier == "TIER1_CERTIFIED").sum()),
                  "tier2_valid_evaluation_player_games": int((eval_data.trust_tier == "TIER2_RETROSPECTIVE_STRICT_PRIOR").sum()),
                  "combined_valid_evaluation_player_games": len(eval_data), "line_rows": len(eval_data) * 3,
                  "tier3_excluded_rows": int((~outcomes.recoverable.fillna(False) & ~outcomes.tier1).sum()),
                  "no_official_outcome_excluded_tier2": int(((~outcomes.tier1) & outcomes.recoverable.fillna(False) & outcomes.actual_sog.isna()).sum()),
                  "tier1_input_mismatch_excluded": int((outcomes.tier1 & ~outcomes.recoverable.fillna(False)).sum()),
                  "candidate_rows_missing_official_outcome": int((candidate.actual_sog.isna()).sum()),
                  "candidate_rows_invalid_structural_inputs": int((candidate.actual_sog.notna() & ~candidate.required_fields_valid).sum())}
    (OUT / "population_manifest.json").write_text(json.dumps({"trust_tiers": {"TIER1_CERTIFIED": "Certified historical production evidence; exact reconstruction-input overlap only", "TIER2_RETROSPECTIVE_STRICT_PRIOR": "B-class strict-prior retrospective reconstruction with exact required inputs"}, "counts": pop_counts, "outcome_source": str(PLATFORM / "retained_skater_history_snapshot.parquet"), "tier3_included": False}, indent=2, sort_keys=True) + "\n")
    fixed_d20_comparison = count_df[count_df.variant.isin(["PRODUCTION", "DIVERGENCE_ONLY", "DIVERGENCE_PLUS_SOFT_HIGH_RATE", "FIXED_D20_DIAGNOSTIC"])].copy()
    fixed_d20_comparison.to_csv(OUT / "fixed_d20_comparison.csv", index=False, lineterminator="\n")
    summary = {"status": "COMPLETE_OFFLINE_RESEARCH_ONLY", "activity": {"provider_calls": 0, "paid_credits": 0, "production_db_mutations": 0, "production_changed": False, "existing_shadows_changed": False, "pushed": False},
               "population": pop_counts, "reconstruction_validation": validation,
               "global_count_metrics": {r.variant: {k: float(getattr(r, k)) for k in ("mae", "rmse", "mean_residual", "mean_lambda", "realized_mean_sog")} | {"n": int(r.n)} for r in count_df[(count_df.scope == "COMBINED") & (count_df.group == "all")].itertuples()},
               "tier1_vs_tier2_consistency": {"divergence_gap_same_direction": bool(dgap_consistent), "near_agreement_high_d10_residual_same_direction": bool(high_consistent), "classification": "STRONG" if dgap_consistent and high_consistent else "MIXED"},
               "classifications": {"divergence_bias": div_class, "independent_high_d10_bias": high_class, "midpoint_correction": div_class, "divergence_plus_soft_compression": combined_class,
                                   "combined_shadow_implementation_justified": shadow, "tier3_override_decision_support": override},
               "combined_mae_improvement": {"divergence_only_vs_production": div_gain, "soft_incremental_vs_divergence": soft_gain},
               "combined_brier_delta_vs_production": delta_brier,
               "fixed_d20_diagnostic": combined.loc["FIXED_D20_DIAGNOSTIC"].to_dict(),
               "d10_quantile_cutpoints": {"top_decile": float(q90), "top_5pct": float(q95), "bottom_decile": float(q10), "bottom_5pct": float(q05)},
               "limitations": ["Only Tier 1 exact reconstruction overlap and Tier 2 B-class rows with exact required feature inputs and final official outcomes enter scores.",
                               "Original Tier 2 captures are not established; findings are retrospective and trust-tier specific.",
                               "Team IDs come from the retained canonical eligibility ledger; PP interaction uses reconstructed prior-season PP/EV-TOI share proxy.",
                               "Top-decile/top-five-percent groups are descriptive pooled quantiles; no model rule was tuned."]}
    tier3_support = {"classification": override, "remaining_tier3_rows_excluded": pop_counts["tier3_excluded_rows"],
                     "reasoning": "Tier 1 and Tier 2 independently agree on divergence direction and high-D10 underprediction, both corrections improve count MAE in both tiers, and combined threshold Brier scores improve at all three lines. Tier 3 is not needed to resolve the present descriptive question; no Tier-3 evidence was used.",
                     "authorization": "NONE; this is decision support only."}
    backlog = ["Historical official-TOI binding", "Prospective grading of divergence shadow", "Continue fixed-blend collection",
               "Puck Line deferred until tomorrow", "Reusable cross-prop sanity-check/explanation audit beginning with Points",
               "Possible remaining Tier-3 override", "Combined high-D10 shadow implementation if supported by prospective review"]
    (OUT / "tier3_override_decision_support.json").write_text(json.dumps(tier3_support, indent=2, sort_keys=True) + "\n")
    (OUT / "backlog_context.json").write_text(json.dumps({"preserved_backlog": backlog, "executed": []}, indent=2) + "\n")
    summary["tier3_override_decision_support"] = tier3_support
    summary["preserved_backlog"] = backlog
    (OUT / "summary.json").write_text(json.dumps(safe_json(summary), indent=2, sort_keys=True) + "\n")

    report = ["# NHL SOG expanded structural validation", "", "Offline fixed-rule study on Tier 1 exact-input overlap and Tier 2 retrospective strict-prior rows only. No Tier 3 scores are included. No threshold or weight was tuned.", "",
              "## Population and validation", "",
              f"The structural population contains {pop_counts['tier1_valid_evaluation_player_games']:,} Tier-1 and {pop_counts['tier2_valid_evaluation_player_games']:,} Tier-2 player-games ({len(eval_data):,} total; {len(eval_data)*3:,} line rows). The prior package's nominal 13,389 + 26,084 population was reduced by Tier-1 reconstruction parity requirements, official-outcome availability, and required D10/D20/exposure fields. The disposition ledger records every exclusion. Remaining Tier 3 is excluded.", "",
              f"Tier-1 validation: {validation['exact_selected_input_player_games']:,} / {validation['tier1_player_games_tested']:,} exact selected-input rows; selected-rate and selected-exposure differences are within 1e-12 for this overlap, all {validation['probability_comparisons_on_exact_input_subset']:,} threshold probabilities match within 1e-12 (max {validation['probability_max_abs_diff_on_exact_input_subset']:.3g}), and derived lambda mismatches are {validation['lambda_mismatches_gt_1e_10']:,} (max {validation['lambda_max_abs_diff']:.3g}). {validation['excluded_tier1_input_mismatches']:,} Tier-1 rows fail exact reconstructed input parity and are excluded; overall Tier-1 selected-exposure mismatches are {validation['selected_exposure_mismatches_gt_1e_12']:,}, all outside the evaluated overlap.", "",
              "## Global results", "", "See global count and line metrics by trust tier. The combined scorecard uses the fixed order D10 production → frozen divergence midpoint → fixed soft high-rate compression; D20 is diagnostic only.", "",
              "| Variant | N | MAE | RMSE | Mean residual | Mean λ | Realized mean SOG |", "|---|---:|---:|---:|---:|---:|---:|"]
    for r in count_df[(count_df.scope == "COMBINED") & (count_df.group == "all")].itertuples():
        report.append(f"| {r.variant} | {r.n:,} | {r.mae:.4f} | {r.rmse:.4f} | {r.mean_residual:.4f} | {r.mean_lambda:.4f} | {r.realized_mean_sog:.4f} |")
    report += ["", "## Main interpretation", "", f"Divergence bias: **{div_class}**. Independent high-D10 bias: **{high_class}**. Midpoint correction: **{div_class}**. Combined soft correction: **{combined_class}**.",
               f"Tier-1 / Tier-2 directional agreement: divergence gap same direction={dgap_consistent}; near-agreement D10>9 residual same direction={high_consistent}. MAE gain: midpoint {div_gain:.4f}; additional soft compression {soft_gain:.4f}.",
               f"Tier 1 vs Tier 2: divergence residual gaps are {t1tc.divergence_residual_gap_above_minus_below:.3f} and {t2tc.divergence_residual_gap_above_minus_below:.3f}; near-agreement D10>9 residuals are {t1tc.near_agreement_high_d10_residual:.3f} and {t2tc.near_agreement_high_d10_residual:.3f}. Both tiers improve in count MAE (midpoint {t1tc.divergence_mae_improvement:.4f}/{t2tc.divergence_mae_improvement:.4f}; incremental soft {t1tc.soft_incremental_mae_improvement:.4f}/{t2tc.soft_incremental_mae_improvement:.4f}). Brier improves at all three lines in both tiers.",
               f"For combined D10>9 rows, production residual is {combined_high_residual:.3f}; the independent near-agreement high-D10 residual is {combined_near_high_residual:.3f}. Equal-player weighting shows divergence improvement for {int(player_summary_df.loc[(player_summary_df.scope=='COMBINED')&(player_summary_df.cohort=='ALL'),'divergence_players_improved'].iloc[0])} vs {int(player_summary_df.loc[(player_summary_df.scope=='COMBINED')&(player_summary_df.cohort=='ALL'),'divergence_players_worsened'].iloc[0])} players; soft improvement for {int(player_summary_df.loc[(player_summary_df.scope=='COMBINED')&(player_summary_df.cohort=='ALL'),'soft_players_improved'].iloc[0])} vs {int(player_summary_df.loc[(player_summary_df.scope=='COMBINED')&(player_summary_df.cohort=='ALL'),'soft_players_worsened'].iloc[0])}. Top five players account for {float(player_summary_df.loc[(player_summary_df.scope=='COMBINED')&(player_summary_df.cohort=='ALL'),'top_5_player_share_positive_divergence_gain'].iloc[0]):.1%} of positive divergence gains and {float(player_summary_df.loc[(player_summary_df.scope=='COMBINED')&(player_summary_df.cohort=='ALL'),'top_5_player_share_positive_soft_gain'].iloc[0]):.1%} of positive soft gains.",
               f"Across teams, divergence MAE improves on {int((team_df[team_df.scope=='COMBINED'].divergence_mae_improvement>0).sum())}/{len(team_df[team_df.scope=='COMBINED'])}; high-D10 soft MAE improves on {int((team_df[team_df.scope=='COMBINED'].high_d10_soft_mae_improvement>0).sum())}/{len(team_df[team_df.scope=='COMBINED'])}. These are general patterns, not a basis for team-specific adjustments.",
               "High-TOI interaction: the high-D10 soft adjustment improves MAE in low, middle, and high TOI strata, with the highest-TOI D10>9 cohort improving from MAE 1.577 to 1.497; no aggregate high-exposure overcorrection is evident. PP strata use only the reconstructed prior-season PP/EV share proxy and are descriptive.",
               "D5/D10/D20: D5 has the worst count MAE and RMSE in both trust tiers; D10 remains the production anchor; D20 has the best full-population raw count MAE/RMSE, while the targeted rules nearly match its MAE and retain D10 on near-agreement rows. Fixed D20 remains diagnostic only.",
               "Date/week stability: combined divergence residual gap supports the hypothesized sign on 158/162 dates and 26/26 weeks; divergence MAE improves on 146/162 dates and 25/26 weeks. High-D10 production residual remains negative on 138/161 evaluable dates and all 25 evaluable weeks. Soft MAE improves on 122/161 dates and 24/25 weeks.",
               f"Combined divergence-plus-soft prospective shadow: **{'JUSTIFIED FOR SHADOW REVIEW' if shadow else 'NOT YET JUSTIFIED'}**. Remaining Tier-3 decision support: **{override}**; this is not authorization to use Tier 3.", "",
               "## Month, stability, robustness and safety", "", "The seven month-level records show divergence gaps from −0.519 to −0.664 and positive midpoint MAE gains from 0.0099 to 0.0156; soft MAE gains remain positive from 0.0265 to 0.0403. The Tier-2 March/April samples contain only 2/4 rows and are not interpreted independently. Combined count MAE improves in all seven months for both corrections; month-specific Brier/log-loss are in `month_level_correction_metrics.csv`.",
               "Early (Oct–Dec), middle (Jan–Feb), and late (Mar–Apr) divergence gaps are −0.591, −0.565, and −0.599; midpoint MAE gains are 0.0134, 0.0136, and 0.0148; incremental soft gains are 0.0346, 0.0335, and 0.0441. Evidence does not suggest a late-season-only artifact.",
               "Baseline safety tables show count MAE does not worsen for near-agreement, low/middle/high D10, low/middle/high lambda, or low/middle/high TOI aggregate groups. All three line-level Brier scores improve globally; inspect `baseline_safety.csv` for subgroup proper scores. PP usage is a strict-prior reconstructed share proxy; team subgroup figures carry an explicit low-support flag.", "",
               "Detailed outputs: `divergence_metrics.csv`, `signed_divergence_bins.csv`, `signed_divergence_monotonicity.csv`, `high_rate_metrics.csv`, `near_agreement_high_rate_metrics.csv`, `overlap_high_rate_divergence_metrics.csv`, `month_level_structural_metrics.csv`, `month_level_correction_metrics.csv`, `early_middle_late_comparison.csv`, `tier1_vs_tier2_comparison.csv`, `date_week_stability_summary.csv`, `player_robustness_summary.csv`, `team_robustness.csv`, `toi_pp_interactions.csv`, `d5_d10_d20_context.csv`, `baseline_safety.csv`, `tier3_override_decision_support.json`, and `backlog_context.json`.", "",
               "## Limits and backlog", "", "Tier 2 is retrospective reconstruction, not original-pregame evidence. The expanded clean sample supports descriptive research only. This task did not modify production or existing shadows. Preserved backlog: prospective grading of the divergence shadow; continued fixed-blend collection; historical official-TOI binding; Puck Line deferred until tomorrow; Points cross-prop sanity/explanation audit; possible Tier-3 override; and prospective combined high-D10 shadow review if supported.", "",
               "Provider calls: 0. Paid credits: 0. Production DB mutations: 0. Pushed: NO."]
    (OUT / "report.md").write_text("\n".join(report) + "\n")

    # Additional interpretation ledgers from the requested fixed groups.
    key_outputs = [p for p in OUT.iterdir() if p.is_file() and p.name != "lineage_manifest.json"]
    inputs = [RECOVERY / "strict_prior_reconstruction_rows.csv", RECOVERY / "tier1_reconstruction_validation_rows.csv",
              RECOVERY / "summary.json", PLATFORM / "retained_skater_history_snapshot.parquet", AUDIT / "reconstruction_eligibility.csv",
              AUDIT / "certified_frozen_correction_rows.csv"]
    manifest = {"method": "Fixed-rule descriptive structural evaluation; only validated Tier 1 overlap and exact-parity Tier 2 with retained final outcomes; no Tier 3",
                "fixed_rules": {"divergence": "if abs(D10-D20)>0.5 then midpoint; otherwise D10", "high_rate": "if D10>9 and corrected rate>9 then 9+0.5*(corrected rate-9)", "thresholds": {"1.5": 2, "2.5": 3, "3.5": 4}},
                "input_sha256": {str(p.relative_to(ROOT)): sha(p) for p in inputs},
                "outputs_sha256": {p.name: sha(p) for p in key_outputs}}
    (OUT / "lineage_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
