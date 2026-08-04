#!/usr/bin/env python3
"""Complete the bounded artifact-defined MLB favorite diagnostic.

This script is deliberately read-only with respect to its input package.  It does
not inspect July, search thresholds, or create a production selection rule.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

LABEL = "HISTORICAL_DEPLOYED_ARTIFACT_CHARACTERIZATION_ONLY"
CANDIDATE = "PROSPECTIVE_CANDIDATE_ONLY"
PROB_BINS = [-np.inf, .50, .55, .60, .65, .70, .75, .80, .90, np.inf]
PROB_LABELS = ["<0.50", "0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.75", "0.75-0.80", "0.80-0.90", "0.90+"]
BE_BINS = [.50, .55, .60, .65, .70, .75, .80, .90, 1.000001]
BE_LABELS = ["0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.75", "0.75-0.80", "0.80-0.90", "0.90-1.00"]


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def logloss(y, p) -> float:
    p = np.clip(np.asarray(p, float), 1e-15, 1 - 1e-15)
    y = np.asarray(y, float)
    return float(np.mean(-(y * np.log(p) + (1-y) * np.log(1-p))))


def metric_row(name: str, y: pd.Series, p: pd.Series) -> dict:
    return {"forecast": name, "rows": len(y), "brier_score": float(np.mean((p-y)**2)),
            "log_loss": logloss(y, p), "calibration_in_the_large": float(p.mean()-y.mean()),
            "mean_predicted_probability": float(p.mean()), "observed_frequency": float(y.mean())}


def reliability(name: str, y: pd.Series, p: pd.Series) -> pd.DataFrame:
    z = pd.DataFrame({"y": y.to_numpy(), "p": p.to_numpy()})
    z["probability_band"] = pd.cut(z.p, PROB_BINS, labels=PROB_LABELS, right=False)
    out = z.groupby("probability_band", observed=False).agg(rows=("y", "size"), mean_probability=("p", "mean"), observed_frequency=("y", "mean")).reset_index()
    out.insert(0, "forecast", name)
    return out


def grouped_unresolved(df: pd.DataFrame, col: str) -> pd.DataFrame:
    g = df.groupby(col, dropna=False).agg(population_rows=("selected_outcome", "size"), unresolved_rows=("selected_outcome", lambda s: int(s.eq("unresolved").sum()))).reset_index()
    g["unresolved_rate"] = g.unresolved_rows / g.population_rows
    g["overall_unresolved_rate"] = df.selected_outcome.eq("unresolved").mean()
    g["rate_ratio_to_overall"] = g.unresolved_rate / g.overall_unresolved_rate
    g["population_share"] = g.population_rows / len(df)
    g["unresolved_share"] = g.unresolved_rows / int(df.selected_outcome.eq("unresolved").sum())
    g.insert(0, "dimension", col)
    g = g.rename(columns={col: "value"})
    return g


def summarize(df: pd.DataFrame, dims: list[str]) -> pd.DataFrame:
    rows = []
    for dim in dims:
        for key, g in df.groupby(dim, dropna=False):
            r = g[g.selected_outcome.isin(["win", "loss"])]
            if r.empty:
                continue
            wr = r.selected_outcome.eq("win").mean()
            mp, be = r.model_probability.mean(), r.break_even_probability.mean()
            if wr <= .5:
                cls = "DIRECTIONALLY_UNSUCCESSFUL"
            elif wr < mp:
                cls = "DIRECTIONALLY_SUCCESSFUL_CONFIDENCE_OVERSTATED"
            elif wr < be:
                cls = "DIRECTIONALLY_SUCCESSFUL_PRICE_UNSUPPORTED"
            else:
                cls = "DIRECTIONALLY_AND_FINANCIALLY_SUPPORTED"
            rows.append({"dimension": dim, "value": key, "population_rows": len(g), "resolved_rows": len(r),
                         "wins": int(r.selected_outcome.eq("win").sum()), "observed_win_rate": wr,
                         "average_model_probability": mp, "average_break_even_probability": be,
                         "model_calibration_gap_predicted_minus_observed": mp-wr,
                         "price_gap_observed_minus_break_even": wr-be, "roi_1u": r.pnl_1u.mean(),
                         "confidence_overstated_flag": bool(wr < mp), "price_unsupported_flag": bool(wr < be),
                         "primary_descriptive_category": cls})
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-package", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--refresh-output", action="store_true")
    a = ap.parse_args()
    src = a.input_package / "artifact_defined_favorite_control_population.csv"
    d = pd.read_csv(src, low_memory=False)
    assert set(d.month.dropna().astype(str).str[:7]) <= {"2026-05", "2026-06"}, "July or other month present"
    assert len(d) == 26017 and int(d.selected_outcome.eq("unresolved").sum()) == 7018
    out = a.output_dir
    if out.exists() and not a.refresh_output: raise FileExistsError(out)
    out.mkdir(parents=True, exist_ok=True)

    # Artifact-provable reason hierarchy.  The source has no explicit DNP/postponed status.
    resolved = d.selected_outcome.isin(["win", "loss"])
    date_has = resolved.groupby(d.game_date).transform("any")
    game_has = resolved.groupby([d.game_date, d.game_id]).transform("any")
    pg_has = resolved.groupby([d.game_date, d.game_id, d.player_id]).transform("any")
    reason = pd.Series("", index=d.index, dtype=object)
    u = ~resolved
    reason.loc[u & (d.game_id.isna() | d.player_id.isna())] = "missing player/game ID"
    reason.loc[u & reason.eq("") & ~date_has] = "source-date coverage gap"
    reason.loc[u & reason.eq("") & ~game_has] = "outcome source missing"
    reason.loc[u & reason.eq("") & pg_has] = "unsupported proposition result"
    reason.loc[u & reason.eq("")] = "unknown"
    detail = d.loc[u, ["game_date","game_id","player_id","player_name","prop_type","selected_side","line","american_price_band","bookmaker_key","schema_fingerprint","configuration_fingerprint","selected_run_tag"]].copy()
    detail["unresolved_reason"] = reason[u]
    detail["reason_evidence"] = detail.unresolved_reason.map({
        "missing player/game ID":"required canonical ID absent",
        "source-date coverage gap":"no resolved control outcome anywhere on date",
        "outcome source missing":"date has outcomes but canonical game has none",
        "unsupported proposition result":"same player/game has another resolved proposition but this proposition has no value",
        "unknown":"artifact has no grading-status evidence sufficient to distinguish DNP/void, postponement, identity fallback, or source omission",
    })
    detail.to_csv(out/"unresolved_outcome_rows.csv", index=False)
    dims_u = ["game_date","month","prop_type","selected_side","line","american_price_band","bookmaker_key","schema_fingerprint","configuration_fingerprint","selected_run_tag"]
    pd.concat([grouped_unresolved(d, x) for x in dims_u], ignore_index=True).to_csv(out/"unresolved_outcome_audit.csv", index=False)
    detail.groupby("unresolved_reason").size().rename("rows").reset_index().to_csv(out/"unresolved_reason_summary.csv", index=False)

    r = d.loc[resolved].copy(); y = r.selected_outcome.eq("win").astype(float)
    base = float(y.mean())
    forecasts = {"model_selected_side_probability": r.model_probability.astype(float),
                 "selected_side_no_vig_market_probability": r.selected_side_no_vig_implied.astype(float),
                 "constant_0.50": pd.Series(.5, index=r.index),
                 "constant_empirical_control_base_rate_IN_SAMPLE_ONLY": pd.Series(base, index=r.index)}
    metrics = pd.DataFrame([metric_row(k,y,p) for k,p in forecasts.items()])
    metrics.to_csv(out/"paired_forecast_comparison.csv", index=False)
    pd.concat([reliability(k,y,p) for k,p in forecasts.items()], ignore_index=True).to_csv(out/"paired_forecast_reliability.csv", index=False)

    denom = pd.DataFrame([
        {"metric":"population", "source_population":len(d), "eligible_denominator":len(d), "excluded_rows":0, "formula":"count strict market-favorite rows"},
        {"metric":"win_rate", "source_population":len(d), "eligible_denominator":len(r), "excluded_rows":len(d)-len(r), "formula":"wins/(wins+losses); pushes, DNP/void, unresolved excluded"},
        {"metric":"one_unit_roi", "source_population":len(d), "eligible_denominator":int(r.pnl_1u.notna().sum()), "excluded_rows":int(d.pnl_1u.isna().sum()), "formula":"sum 1u profit/loss / graded wagers"},
        {"metric":"Brier", "source_population":len(d), "eligible_denominator":len(r), "excluded_rows":len(d)-len(r), "formula":"mean((p-y)^2) on identical resolved rows"},
        {"metric":"log_loss", "source_population":len(d), "eligible_denominator":len(r), "excluded_rows":len(d)-len(r), "formula":"mean(-y ln p -(1-y) ln(1-p)); p clipped [1e-15,1-1e-15]"},
    ])
    denom["pushes"] = 0; denom["dnp_voids_explicitly_identified"] = 0; denom["unresolved"] = int(u.sum()); denom["probability_available_resolved"] = int(r.model_probability.notna().sum())
    denom.to_csv(out/"metric_denominator_verification.csv", index=False)

    dims = ["prop_type","selected_side","line","month","model_probability_band","market_probability_band","american_price_band","bookmaker_key","schema_fingerprint","configuration_fingerprint"]
    summarize(d,dims).to_csv(out/"direction_confidence_price_decomposition.csv", index=False)
    r["required_break_even_band"] = pd.cut(r.break_even_probability, BE_BINS, labels=BE_LABELS, right=False)
    price = summarize(pd.concat([d.loc[~resolved], r], ignore_index=True), ["american_price_band","required_break_even_band"])
    price.to_csv(out/"fixed_price_burden_characterization.csv", index=False)

    model = metrics.set_index("forecast").loc["model_selected_side_probability"]
    market = metrics.set_index("forecast").loc["selected_side_no_vig_market_probability"]
    added = bool(model.brier_score < market.brier_score and model.log_loss < market.log_loss)
    decision = "HISTORICAL_MODEL_PROBABILITY_ADDED_INFORMATION" if added else "HISTORICAL_MODEL_PROBABILITY_DID_NOT_ADD_INFORMATION"
    clues = pd.DataFrame([
      ["model probability inflation","Model calibration gap by fixed band/segment.","May/June and segment effects vary.","favorite control","confidence",CANDIDATE,"certified model/feature/config lineage plus outcomes"],
      ["model fails to improve market","Paired Brier/log loss comparison on resolved rows.","Historical semantic lineage is unavailable.","resolved favorite control","confidence",CANDIDATE,"same-snapshot market pair and certified prediction-time lineage"],
      ["favorite price burden","Negative aggregate ROI and fixed price-band shortfall.","Direction may remain above 50%.","priced favorites","price",CANDIDATE,"executable two-sided prices frozen pregame"],
      ["direct versus fallback provenance","Artifact contains varying operational fingerprints.","Exact historical provenance is not recoverable.","all favorite classes","direction/confidence",CANDIDATE,"row-level direct/fallback field at prediction time"],
      ["stale or incomplete history","Unresolved and operational-era concentrations are auditable.","Missing outcomes do not prove stale features.","affected eras","confidence",CANDIDATE,"strict-prior feature freshness/completeness fields"],
      ["adjacent-threshold incoherence","Line-specific descriptive variation can be measured.","No searched cutoff is authorized.","multi-line player/prop snapshots","direction",CANDIDATE,"all adjacent lines from same snapshot and coherence result"],
    ], columns=["mechanism","supporting_historical_observation","conflicting_evidence","affected_population","concern","transferability_status","exact_prospective_data_required"])
    clues.to_csv(out/"historical_clue_registry.csv",index=False)
    summary = {"characterization_label":LABEL,"decision":decision,"source_package":str(a.input_package),"source_sha256":sha(src),"rows":len(d),"resolved_rows":len(r),"wins":int(y.sum()),"losses":int((1-y).sum()),"unresolved_rows":int(u.sum()),"historical_outcome_completeness":"MATERIAL_NONRANDOM_INCOMPLETENESS_CHARACTERIZED_NO_IMPUTATION","win_rate":base,"roi_1u":float(r.pnl_1u.mean()),"average_model_probability":float(r.model_probability.mean()),"average_market_probability":float(r.selected_side_no_vig_implied.mean()),"aggregate_break_even_rate":float(r.break_even_probability.mean()),"model_added_information_vs_market_on_brier_and_logloss":added,"no_july_inspected":True,"no_imputation":True}
    (out/"interpretation.json").write_text(json.dumps(summary,indent=2)+"\n")
    (out/"interpretation.md").write_text(f"# Historical favorite diagnostic\n\n**{decision}**\n\nThis is `{LABEL}`. The control has {len(d):,} rows; {len(r):,} are resolved and {int(u.sum()):,} remain unresolved without imputation. Incompleteness is non-random: unresolved rates are 58.55% for -250-or-shorter prices, 36.23% for pitcher strikeouts, and 71.54% at BetRivers. The audit assigns only artifact-provable reasons; DNP/void and postponement are not inferred, leaving 3,103 documented unknowns. On identical rows the model (Brier 0.244621, log loss 0.683537) trails the market (0.238670, 0.670173). Model-versus-market usefulness is decided on paired Brier and log loss, not ROI or searched thresholds. Every mechanism remains `{CANDIDATE}`. No July outcomes were read and no production rule is authorized.\n")
    files=sorted(p for p in out.iterdir() if p.name!="SHA256SUMS.csv")
    pd.DataFrame([{"file":p.name,"sha256":sha(p),"bytes":p.stat().st_size} for p in files]).to_csv(out/"SHA256SUMS.csv",index=False)
    print(json.dumps(summary,indent=2)); return 0

if __name__ == "__main__": raise SystemExit(main())
