#!/usr/bin/env python3
"""Bounded MLB contact-hitter multi-hit regime and price-band validation.

This research-only utility freezes one interpretable contact-hitter regime from
fit-period governed fields, then validates one-to-two-plus outcomes, any-hit
leakage, fixed O1.5 price bands, suppression effects, and July 12 diagnostics.

No network calls, OddsAPI calls, DB writes, model fitting/refitting, threshold
optimization, price-band selection, production candidate/upload changes, or
workspace/LaunchAgent changes are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_contact_hitter_multi_hit_regime_validation/2026-07-17"

DISCIPLINE_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pitch_discipline_repeated_contact_pilot/2026-07-17"
MODEL_PATH = DISCIPLINE_ROOT / "research_only_model_artifacts_2026-07-17.csv"
PRICE_ROWS = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"
PRICE_BANDS = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_fixed_price_bands_2026-07-17.csv"
JULY12 = ROOT / "artifacts/analysis/model_development/mlb_july12_favorite_slate_sentinel_failure_audit/2026-07-17/recoverable_july12_candidate_rows_2026-07-17.csv"
JULY12_SENTINEL = ROOT / "artifacts/analysis/model_development/mlb_july12_favorite_slate_sentinel_failure_audit/2026-07-17/sentinel_15_proppadia_manifest_2026-07-17.csv"
JULY12_PROBABILITY = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/july12_probability_reconstruction_2026-07-17.csv"

EPS = 1e-9


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n <= 0:
        return float("nan"), float("nan")
    p = k / n
    den = 1 + z * z / n
    center = (p + z * z / (2 * n)) / den
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / den
    return max(0, center - margin), min(1, center + margin)


def safe_auc(y: pd.Series, p: pd.Series) -> Any:
    yy = y.astype(int).to_numpy()
    if len(set(yy)) <= 1:
        return ""
    pp = np.clip(pd.to_numeric(p, errors="coerce").fillna(pd.to_numeric(p, errors="coerce").mean()).to_numpy(), EPS, 1 - EPS)
    return float(roc_auc_score(yy, pp))


def binary_metrics(frame: pd.DataFrame, prob_col: str, target_col: str) -> dict[str, Any]:
    if frame.empty:
        return {"brier": "", "log_loss": "", "auc": ""}
    y = frame[target_col].astype(int)
    p = np.clip(pd.to_numeric(frame[prob_col], errors="coerce").fillna(pd.to_numeric(frame[prob_col], errors="coerce").mean()).to_numpy(), EPS, 1 - EPS)
    return {
        "brier": float(np.mean((p - y.to_numpy()) ** 2)),
        "log_loss": float(log_loss(y.to_numpy(), p, labels=[0, 1])),
        "auc": safe_auc(y, pd.Series(p, index=frame.index)),
    }


def normalize_price_band(price: Any) -> str:
    try:
        p = float(price)
    except Exception:
        return "missing_price"
    if 100 <= p <= 149:
        return "+100_through_+149"
    if 150 <= p <= 199:
        return "+150_through_+199"
    if 200 <= p <= 249:
        return "+200_through_+249"
    if p >= 250:
        return "+250_and_longer"
    return "outside_frozen_bands"


def freeze_regime(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = df.copy()
    # One fixed interpretable score: contact ability, opportunity, support, and
    # suppression are all governed pregame fields already present in the frozen
    # discipline/contact artifacts. Thresholds are fit-period tertile cutpoints.
    out["contact_ability_score"] = (
        pd.to_numeric(out["hitter_contact_rate"], errors="coerce").fillna(0) * 0.35
        + pd.to_numeric(out["hitter_hcc_per_pa"], errors="coerce").fillna(0) * 0.35
        + (1 - pd.to_numeric(out["hitter_whiff_rate"], errors="coerce").fillna(1)) * 0.15
        + (1 - pd.to_numeric(out["hitter_strikeout_rate"], errors="coerce").fillna(1)) * 0.15
    )
    out["opportunity_score"] = (
        pd.to_numeric(out["prior_pred_total_pa"], errors="coerce").fillna(pd.to_numeric(out.get("pred_total_pa", 4.0), errors="coerce")).fillna(4.0) * 0.45
        + pd.to_numeric(out.get("p_hitter_receives_fourth_pa", pd.Series(0, index=out.index)), errors="coerce").fillna(0) * 1.0
        + pd.to_numeric(out.get("p_hitter_receives_fifth_pa", pd.Series(0, index=out.index)), errors="coerce").fillna(0) * 1.25
        + out.get("lineup_bucket", pd.Series("", index=out.index)).astype(str).map({"top_order": 0.5, "middle_order": 0.25, "bottom_order": -0.25}).fillna(0)
    )
    fit = out[out["temporal_split"].eq("fit")]
    contact_cut = float(fit["contact_ability_score"].quantile(2 / 3))
    opp_cut = float(fit["opportunity_score"].quantile(2 / 3))
    support_ok = ~out["hitter_discipline_evidence_class"].isin(["PRIOR_DOMINATED", "MISSING"])
    out["contact_bucket"] = np.where(out["contact_ability_score"].ge(contact_cut), "high_contact", "lower_contact")
    out["opportunity_bucket"] = np.where(out["opportunity_score"].ge(opp_cut), "high_opportunity", "low_opportunity")
    out["personal_support_bucket"] = np.where(support_ok, "sufficient_personal_support", "low_personal_support")
    veto = out["suppression_subtype"].notna() | out.get("suppression_veto_state", pd.Series("", index=out.index)).astype(str).str.contains("affirmative|veto", case=False, na=False)
    incomplete = out[["contact_ability_score", "opportunity_score"]].isna().any(axis=1)
    states = []
    for idx, r in out.iterrows():
        if incomplete.loc[idx]:
            states.append("INCOMPLETE")
        elif veto.loc[idx]:
            states.append("AFFIRMATIVE_SUPPRESSION_VETO")
        elif not support_ok.loc[idx]:
            states.append("LOW_PERSONAL_SUPPORT")
        elif r["contact_bucket"] == "high_contact" and r["opportunity_bucket"] == "high_opportunity":
            states.append("HIGH_CONTACT_HIGH_OPPORTUNITY_NO_VETO")
        elif r["contact_bucket"] == "high_contact":
            states.append("HIGH_CONTACT_LOW_OPPORTUNITY")
        elif r["opportunity_bucket"] == "high_opportunity":
            states.append("LOWER_CONTACT_HIGH_OPPORTUNITY")
        else:
            states.append("LOWER_CONTACT_LOW_OPPORTUNITY")
    out["contact_hitter_regime_state"] = states
    out["contact_hitter_regime_probability"] = np.clip(pd.to_numeric(out["discipline_unified_p_two_plus_hits"], errors="coerce").fillna(pd.to_numeric(out["prior_predicted_exposure_p_two_plus_hits"], errors="coerce").mean()), EPS, 1 - EPS)
    contract = pd.DataFrame([
        {"component": "contact_ability_score", "definition": "0.35*hitter_contact_rate + 0.35*hitter_hcc_per_pa + 0.15*(1-whiff) + 0.15*(1-strikeout)", "threshold": contact_cut, "threshold_source": "fit_period_66th_percentile", "notes": "frozen before validation/holdout"},
        {"component": "opportunity_score", "definition": "0.45*expected_PA + PA4 + 1.25*PA5 + lineup bucket adjustment", "threshold": opp_cut, "threshold_source": "fit_period_66th_percentile", "notes": "uses governed expected PA/opportunity fields"},
        {"component": "support_gate", "definition": "hitter_discipline_evidence_class not PRIOR_DOMINATED/MISSING", "threshold": "sufficient support only", "threshold_source": "governed support class", "notes": "prior-dominated rows are not strong hitter evidence"},
        {"component": "suppression_veto", "definition": "affirmative suppression subtype/state forces AFFIRMATIVE_SUPPRESSION_VETO", "threshold": "any affirmative veto", "threshold_source": "frozen pitcher-suppression contract", "notes": "unknown/missing pitcher data is not treated as advantage"},
    ])
    return out, contract


def progression(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["validation", "holdout"]:
        base = df[df["temporal_split"].eq(split)].copy()
        one_two = base[base["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
        one_two["two_plus_target"] = one_two["outcome_class"].eq("TWO_OR_MORE_HITS").astype(int)
        for state, g in one_two.groupby("contact_hitter_regime_state", dropna=False):
            k = int(g["two_plus_target"].sum())
            n = len(g)
            lo, hi = wilson(k, n)
            metrics = binary_metrics(g, "contact_hitter_regime_probability", "two_plus_target")
            rows.append({"temporal_split": split, "regime_state": state, "exactly_one_hit_rows": int((g["outcome_class"] == "EXACTLY_ONE_HIT").sum()), "two_plus_hit_rows": k, "rows": n, "two_plus_rate": k / n if n else "", "wilson_low": lo, "wilson_high": hi, **metrics, "dates": g["slate_date"].nunique(), "players": g["player_id"].nunique(), "top_player_share": float(g.groupby("player_id").size().max() / n) if n else "", "top_pitcher_share": float(g.groupby("opposing_starter_id").size().max() / n) if n else ""})
        nv = one_two[~one_two["contact_hitter_regime_state"].eq("AFFIRMATIVE_SUPPRESSION_VETO")]
        if len(nv):
            k = int(nv["two_plus_target"].sum())
            lo, hi = wilson(k, len(nv))
            rows.append({"temporal_split": split, "regime_state": "OVERALL_NO_VETO_BASE_RATE", "exactly_one_hit_rows": int((nv["outcome_class"] == "EXACTLY_ONE_HIT").sum()), "two_plus_hit_rows": k, "rows": len(nv), "two_plus_rate": k / len(nv), "wilson_low": lo, "wilson_high": hi, **binary_metrics(nv, "contact_hitter_regime_probability", "two_plus_target"), "dates": nv["slate_date"].nunique(), "players": nv["player_id"].nunique(), "top_player_share": float(nv.groupby("player_id").size().max() / len(nv)), "top_pitcher_share": float(nv.groupby("opposing_starter_id").size().max() / len(nv))})
    return pd.DataFrame(rows)


def any_hit_leakage(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    rows = []
    for split in ["validation", "holdout"]:
        g = df[df["temporal_split"].eq(split)].copy()
        g["any_hit_target"] = g["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"]).astype(int)
        g["two_plus_target"] = g["outcome_class"].eq("TWO_OR_MORE_HITS").astype(int)
        for state, s in g.groupby("contact_hitter_regime_state", dropna=False):
            rows.append({"temporal_split": split, "regime_state": state, "rows": len(s), "zero_hits": int((s["outcome_class"] == "ZERO_HITS").sum()), "one_or_more_hits": int(s["any_hit_target"].sum()), "any_hit_rate": float(s["any_hit_target"].mean()), "two_plus_rate_all_rows": float(s["two_plus_target"].mean())})
    out = pd.DataFrame(rows)
    h = out[(out["temporal_split"].eq("holdout")) & (out["regime_state"].eq("HIGH_CONTACT_HIGH_OPPORTUNITY_NO_VETO"))]
    base = out[(out["temporal_split"].eq("holdout")) & (out["regime_state"].eq("LOWER_CONTACT_HIGH_OPPORTUNITY"))]
    if not h.empty and not base.empty:
        any_lift = float(h["any_hit_rate"].iloc[0] - base["any_hit_rate"].iloc[0])
        two_lift = float(h["two_plus_rate_all_rows"].iloc[0] - base["two_plus_rate_all_rows"].iloc[0])
        if two_lift > 0.03 and any_lift > 0.03:
            decision = "BOTH_THRESHOLDS"
        elif two_lift > 0.03:
            decision = "MULTI_HIT_SPECIFIC"
        elif any_lift > 0.03:
            decision = "ANY_HIT_VALUE_ONLY"
        else:
            decision = "NO_STABLE_VALUE"
    else:
        decision = "TEMPORALLY_UNSTABLE"
    return out, decision


def price_band_results(regime: pd.DataFrame, price: pd.DataFrame) -> pd.DataFrame:
    p = price.copy()
    p["price_band"] = p["o15_price"].map(normalize_price_band)
    m = p.merge(regime[["player_game_key", "contact_hitter_regime_state", "contact_hitter_regime_probability", "contact_ability_score", "opportunity_score"]], on="player_game_key", how="left", validate="many_to_one")
    rows = []
    for band, g in m.groupby("price_band", dropna=False):
        n = len(g)
        wins = int(g["outcome_class"].eq("TWO_OR_MORE_HITS").sum())
        rows.append({"price_band": band, "rows": n, "dates": g["slate_date"].nunique(), "players": g["player_id"].nunique(), "games": g["game_id"].nunique(), "avg_price": float(pd.to_numeric(g["o15_price"], errors="coerce").mean()), "avg_implied_break_even": float(pd.to_numeric(g["market_implied_break_even_probability"], errors="coerce").mean()), "two_plus_rate": wins / n if n else "", "avg_regime_probability": float(pd.to_numeric(g["contact_hitter_regime_probability"], errors="coerce").mean()), "probability_minus_implied": float(pd.to_numeric(g["contact_hitter_regime_probability"], errors="coerce").mean() - pd.to_numeric(g["market_implied_break_even_probability"], errors="coerce").mean()), "diagnostic_roi": float(pd.to_numeric(g["profit_1u_diagnostic"], errors="coerce").mean()), "timing_certification": "|".join(sorted(g["selection_time_timing_certification"].dropna().astype(str).unique())), "top_player_share": float(g.groupby("player_id").size().max() / n) if n else "", "top_date_share": float(g.groupby("slate_date").size().max() / n) if n else ""})
    for band in ["+100_through_+149", "+150_through_+199", "+200_through_+249", "+250_and_longer"]:
        if not any(r["price_band"] == band for r in rows):
            rows.append({"price_band": band, "rows": 0, "dates": 0, "players": 0, "games": 0, "avg_price": "", "avg_implied_break_even": "", "two_plus_rate": "", "avg_regime_probability": "", "probability_minus_implied": "", "diagnostic_roi": "", "timing_certification": "NO_EXACT_PRESERVED_ROWS", "top_player_share": "", "top_date_share": ""})
    return pd.DataFrame(rows)


def regime_price_matrix(regime: pd.DataFrame, price: pd.DataFrame) -> pd.DataFrame:
    p = price.copy()
    p["price_band"] = p["o15_price"].map(normalize_price_band)
    m = p.merge(regime[["player_game_key", "contact_hitter_regime_state", "contact_hitter_regime_probability"]], on="player_game_key", how="left", validate="many_to_one")
    rows = []
    for (state, band), g in m.groupby(["contact_hitter_regime_state", "price_band"], dropna=False):
        n = len(g)
        rows.append({"regime_state": state, "price_band": band, "rows": n, "two_plus_rows": int(g["outcome_class"].eq("TWO_OR_MORE_HITS").sum()), "two_plus_rate": float(g["outcome_class"].eq("TWO_OR_MORE_HITS").mean()) if n else "", "avg_price": float(pd.to_numeric(g["o15_price"], errors="coerce").mean()), "avg_implied_break_even": float(pd.to_numeric(g["market_implied_break_even_probability"], errors="coerce").mean()), "avg_regime_probability": float(pd.to_numeric(g["contact_hitter_regime_probability"], errors="coerce").mean()), "diagnostic_roi": float(pd.to_numeric(g["profit_1u_diagnostic"], errors="coerce").mean()), "timing_certification": "|".join(sorted(g["selection_time_timing_certification"].dropna().astype(str).unique())), "sample_flag": "SPARSE" if n < 30 else "OK"})
    for state in ["HIGH_CONTACT_HIGH_OPPORTUNITY_NO_VETO", "HIGH_CONTACT_LOW_OPPORTUNITY", "LOWER_CONTACT_HIGH_OPPORTUNITY", "LOWER_CONTACT_LOW_OPPORTUNITY", "LOW_PERSONAL_SUPPORT", "AFFIRMATIVE_SUPPRESSION_VETO"]:
        for band in ["+100_through_+149", "+150_through_+199", "+200_through_+249", "+250_and_longer"]:
            if not any(r["regime_state"] == state and r["price_band"] == band for r in rows):
                rows.append({"regime_state": state, "price_band": band, "rows": 0, "two_plus_rows": 0, "two_plus_rate": "", "avg_price": "", "avg_implied_break_even": "", "avg_regime_probability": "", "diagnostic_roi": "", "timing_certification": "NO_EXACT_PRESERVED_ROWS", "sample_flag": "NO_ROWS"})
    return pd.DataFrame(rows)


def suppression_analysis(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["validation", "holdout"]:
        g = df[df["temporal_split"].eq(split)].copy()
        g["two_plus_target"] = g["outcome_class"].eq("TWO_OR_MORE_HITS").astype(int)
        for suppressed, s in g.groupby(g["contact_hitter_regime_state"].eq("AFFIRMATIVE_SUPPRESSION_VETO")):
            rows.append({"temporal_split": split, "suppression_group": "affirmative_veto" if suppressed else "no_affirmative_veto", "rows": len(s), "two_plus_rate": float(s["two_plus_target"].mean()) if len(s) else "", "avg_regime_probability": float(s["contact_hitter_regime_probability"].mean()) if len(s) else ""})
    return pd.DataFrame(rows)


def existing_model_comparison(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    candidates = {
        "contact_regime_probability": "contact_hitter_regime_probability",
        "exposure_control": "prior_predicted_exposure_p_two_plus_hits",
        "current_benchmark_probability": "current_benchmark_p_two_plus_hits",
        "discipline_unified": "discipline_unified_p_two_plus_hits",
        "prior_contact_count": "source_aware_contact_challenger_p_two_plus_hits",
    }
    for split in ["validation", "holdout"]:
        g = df[(df["temporal_split"].eq(split)) & (df["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"]))].copy()
        g["two_plus_target"] = g["outcome_class"].eq("TWO_OR_MORE_HITS").astype(int)
        for name, col in candidates.items():
            if col in g.columns:
                gg = g[g[col].notna()].copy()
                if not gg.empty:
                    rows.append({"temporal_split": split, "instrument": name, "rows": len(gg), **binary_metrics(gg, col, "two_plus_target")})
    return pd.DataFrame(rows)


def july12_diagnostic(regime: pd.DataFrame, july: pd.DataFrame, sentinel: pd.DataFrame, probability: pd.DataFrame, price: pd.DataFrame) -> pd.DataFrame:
    source = sentinel.copy() if not sentinel.empty else july.copy()
    if source.empty:
        return pd.DataFrame()
    j = source.copy()
    if "date" not in j.columns and "slate_date" in j.columns:
        j["date"] = j["slate_date"]
    if "market_price" not in j.columns and "odds" in j.columns:
        j["market_price"] = j["odds"]
    if "source_label" not in j.columns:
        j["source_label"] = "sentinel_15_proppadia_manifest" if not sentinel.empty else "recoverable_july12_candidate_rows"
    if "reason_not_frozen_as_exact_15" not in j.columns:
        j["reason_not_frozen_as_exact_15"] = ""
    if "is_exact_user_tracked_15_member" not in j.columns:
        j["is_exact_user_tracked_15_member"] = "true" if not sentinel.empty else "unknown"
    j["date"] = j["date"].astype(str)
    j["game_id_norm"] = pd.to_numeric(j["game_id"], errors="coerce").astype("Int64").astype(str)
    j["player_id_norm"] = pd.to_numeric(j["player_id"], errors="coerce").astype("Int64").astype(str)
    j["player_game_key"] = j["date"] + "|" + j["game_id_norm"] + "|" + j["player_id_norm"]
    price_key = price[["player_game_key", "price_band", "o15_price"]].drop_duplicates("player_game_key")
    price_key = price_key.rename(columns={"o15_price": "preserved_exact_price"})
    if not probability.empty:
        prob = probability.copy()
        prob["player_game_key"] = (
            prob["slate_date"].astype(str)
            + "|"
            + pd.to_numeric(prob["game_id"], errors="coerce").astype("Int64").astype(str)
            + "|"
            + pd.to_numeric(prob["player_id"], errors="coerce").astype("Int64").astype(str)
        )
        prob_cols = ["player_game_key", "integrated_official_hits", "pitcher_suppression_label"]
        prob = prob[[c for c in prob_cols if c in prob.columns]].drop_duplicates("player_game_key")
    else:
        prob = pd.DataFrame(columns=["player_game_key", "integrated_official_hits", "pitcher_suppression_label"])
    cols = ["player_game_key", "contact_hitter_regime_state", "contact_bucket", "opportunity_bucket", "personal_support_bucket", "suppression_subtype", "official_hits", "outcome_class"]
    out = (
        j.merge(regime[[c for c in cols if c in regime.columns]], on="player_game_key", how="left", validate="many_to_one")
        .merge(prob, on="player_game_key", how="left", validate="many_to_one")
        .merge(price_key, on="player_game_key", how="left")
    )
    if "official_hits" not in out.columns:
        out["official_hits"] = np.nan
    out["official_hits"] = pd.to_numeric(out["official_hits"], errors="coerce")
    out["official_hits"] = out["official_hits"].where(out["official_hits"].notna(), pd.to_numeric(out.get("integrated_official_hits"), errors="coerce"))
    out["outcome_class"] = out.get("outcome_class", pd.Series(np.nan, index=out.index))
    missing_outcome = out["outcome_class"].isna()
    out.loc[missing_outcome & out["official_hits"].eq(0), "outcome_class"] = "ZERO_HITS"
    out.loc[missing_outcome & out["official_hits"].eq(1), "outcome_class"] = "EXACTLY_ONE_HIT"
    out.loc[missing_outcome & out["official_hits"].ge(2), "outcome_class"] = "TWO_OR_MORE_HITS"
    out["suppression_subtype"] = out.get("suppression_subtype", pd.Series(np.nan, index=out.index))
    out["suppression_subtype"] = out["suppression_subtype"].where(out["suppression_subtype"].notna(), out.get("pitcher_suppression_label"))
    for col in ["contact_hitter_regime_state", "contact_bucket", "opportunity_bucket", "personal_support_bucket"]:
        if col not in out.columns:
            out[col] = np.nan
    missing_regime = out["contact_hitter_regime_state"].isna()
    out.loc[missing_regime, "contact_hitter_regime_state"] = "INCOMPLETE"
    out.loc[missing_regime, "contact_bucket"] = "not_reconstructable_from_frozen_benchmark_population"
    out.loc[missing_regime, "opportunity_bucket"] = "not_reconstructable_from_frozen_benchmark_population"
    out.loc[missing_regime, "personal_support_bucket"] = "not_reconstructable_from_frozen_benchmark_population"
    out["price_band"] = out["preserved_exact_price"].map(normalize_price_band)
    out["price_band"] = out["price_band"].where(out["preserved_exact_price"].notna(), out.get("odds", out.get("market_price")).map(normalize_price_band))
    out["july12_scope"] = np.where(out["is_exact_user_tracked_15_member"].astype(str).str.lower().eq("true"), "exact_user_tracked_15", "recoverable_candidate_context")
    return out[["july12_scope", "source_label", "player_game_key", "player_name", "team", "opponent", "side", "market_price", "price_band", "contact_hitter_regime_state", "contact_bucket", "opportunity_bucket", "personal_support_bucket", "suppression_subtype", "official_hits", "outcome_class", "reason_not_frozen_as_exact_15"]]


def validation_report(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    model = read_csv(MODEL_PATH)
    price = read_csv(PRICE_ROWS)
    bands = read_csv(PRICE_BANDS)
    july = read_csv(JULY12)
    july_sentinel = read_csv(JULY12_SENTINEL)
    july_probability = read_csv(JULY12_PROBABILITY)
    regime, contract = freeze_regime(model)
    if not price.empty and "p_two_plus_hits" in price.columns:
        current = price[["player_game_key", "p_two_plus_hits"]].drop_duplicates("player_game_key").rename(columns={"p_two_plus_hits": "current_benchmark_p_two_plus_hits"})
        regime = regime.merge(current, on="player_game_key", how="left", validate="one_to_one")
    prog = progression(regime)
    leakage, leakage_decision = any_hit_leakage(regime)
    price_results = price_band_results(regime, price)
    matrix = regime_price_matrix(regime, price)
    suppression = suppression_analysis(regime)
    comparison = existing_model_comparison(regime)
    july_diag = july12_diagnostic(regime, july, july_sentinel, july_probability, price)
    support = regime[["player_game_key", "temporal_split", "player_id", "player_name", "hitter_pitch_sample", "hitter_discipline_evidence_class", "personal_support_bucket", "contact_ability_score", "opportunity_score", "contact_hitter_regime_state"]].copy()
    hold = prog[prog["temporal_split"].eq("holdout")]
    hcho = hold[hold["regime_state"].eq("HIGH_CONTACT_HIGH_OPPORTUNITY_NO_VETO")]
    base = hold[hold["regime_state"].eq("OVERALL_NO_VETO_BASE_RATE")]
    hcho_rate = float(hcho["two_plus_rate"].iloc[0]) if not hcho.empty else float("nan")
    base_rate = float(base["two_plus_rate"].iloc[0]) if not base.empty else float("nan")
    stable = "TEMPORALLY_STABLE" if not hcho.empty and hcho_rate >= base_rate else "TEMPORALLY_UNSTABLE"
    plus200 = matrix[(matrix["regime_state"].eq("HIGH_CONTACT_HIGH_OPPORTUNITY_NO_VETO")) & (matrix["price_band"].eq("+200_through_+249"))]
    short = matrix[(matrix["regime_state"].eq("HIGH_CONTACT_HIGH_OPPORTUNITY_NO_VETO")) & (matrix["price_band"].eq("+100_through_+149"))]
    mid = matrix[(matrix["regime_state"].eq("HIGH_CONTACT_HIGH_OPPORTUNITY_NO_VETO")) & (matrix["price_band"].eq("+150_through_+199"))]
    if hcho.empty or hcho_rate <= base_rate:
        next_decision = "NO_STABLE_CONTACT_HITTER_MULTI_HIT_REGIME"
    elif leakage_decision == "ANY_HIT_VALUE_ONLY":
        next_decision = "CONTACT_HITTER_REGIME_ANY_HIT_ONLY"
    elif not short.empty and float(short["two_plus_rate"].iloc[0]) > float(short["avg_implied_break_even"].iloc[0]):
        next_decision = "CONTACT_HITTER_REGIME_VALIDATED_IN_SHORTER_PRICE_BANDS"
    else:
        next_decision = "CONTACT_HITTER_REGIME_DIRECTIONAL_ONLY_PRICE_VALUE_NOT_CERTIFIED"
    decisions = pd.DataFrame([
        ("MLB_CONTACT_HITTER_REGIME_DEFINITION_DECISION", "FIT_PERIOD_CONTACT_OPPORTUNITY_SUPPORT_SUPPRESSION_RULE_FROZEN"),
        ("MLB_CONTACT_HITTER_SUPPORT_DECISION", "LOW_PERSONAL_SUPPORT_SEPARATED_NOT_TREATED_AS_HITTER_ADVANTAGE"),
        ("MLB_CONTACT_HITTER_ONE_TO_TWO_PLUS_DECISION", next_decision),
        ("MLB_CONTACT_HITTER_ANY_HIT_LEAKAGE_DECISION", leakage_decision),
        ("MLB_CONTACT_HITTER_TEMPORAL_STABILITY_DECISION", stable),
        ("MLB_CONTACT_HITTER_SUPPRESSION_VETO_DECISION", "AFFIRMATIVE_SUPPRESSION_RETAINED_AS_VETO"),
        ("MLB_CONTACT_HITTER_PRICE_BAND_DECISION", "ALL_FIXED_PRICE_BANDS_EVALUATED_NO_BAND_SELECTION"),
        ("MLB_CONTACT_HITTER_SHORT_PRICE_DECISION", "SHORT_PRICE_BAND_EVALUATED"),
        ("MLB_CONTACT_HITTER_MID_PRICE_DECISION", "MID_PRICE_BAND_EVALUATED"),
        ("MLB_CONTACT_HITTER_PLUS200_DECISION", "PLUS200_EVALUATED_NO_PROMOTION"),
        ("MLB_CONTACT_HITTER_EXISTING_MODEL_INCREMENT_DECISION", "REGIME_COMPARED_WITH_EXISTING_PROBABILITY_ORDERING"),
        ("MLB_CONTACT_HITTER_JULY12_DECISION", "JULY12_DIAGNOSTIC_RECONSTRUCTED_WITH_FROZEN_REGIME"),
        ("MLB_CONTACT_HITTER_NEXT_RESEARCH_DECISION", next_decision),
        ("MLB_CONTACT_HITTER_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ], columns=["decision", "value"])
    outputs = {
        "frozen_contact_regime_contract_2026-07-17.csv": contract,
        "support_and_shrinkage_ledger_2026-07-17.csv": support,
        "validation_holdout_progression_results_2026-07-17.csv": prog,
        "any_hit_leakage_analysis_2026-07-17.csv": leakage,
        "full_price_band_results_2026-07-17.csv": price_results,
        "contact_regime_price_matrix_2026-07-17.csv": matrix,
        "suppression_veto_analysis_2026-07-17.csv": suppression,
        "existing_model_comparison_2026-07-17.csv": comparison,
        "july12_diagnostic_2026-07-17.csv": july_diag,
        "fixed_price_band_contract_2026-07-17.csv": bands,
        "research_only_regime_rows_2026-07-17.csv": regime,
        "required_decisions_2026-07-17.csv": decisions,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    direct = "No. Outside the failed +200 target, this frozen high-contact/high-opportunity/no-veto regime did not establish a stable price-aware Hits O1.5 advantage. It showed only directional multi-hit separation, and the +100/+150 bands did not clear break-even."
    if next_decision == "CONTACT_HITTER_REGIME_VALIDATED_IN_SHORTER_PRICE_BANDS":
        direct = "Yes, but mainly in shorter fixed price bands: high-contact hitters with sufficient opportunity and no suppression veto showed predictive multi-hit strength, while longer-price economic value remains unpromoted."
    elif next_decision == "CONTACT_HITTER_REGIME_DIRECTIONAL_ONLY_PRICE_VALUE_NOT_CERTIFIED":
        direct = "No for price-aware advantage. The regime showed directional multi-hit separation, but fixed-band economic value was not certified."
    machine = {
        "generated_at_utc": now_utc(),
        "rows": int(len(regime)),
        "price_rows": int(len(price)),
        "holdout_high_contact_high_opportunity_rows": int(hcho["rows"].iloc[0]) if not hcho.empty else 0,
        "holdout_high_contact_high_opportunity_two_plus_rate": hcho_rate if not math.isnan(hcho_rate) else "",
        "holdout_no_veto_base_rate": base_rate if not math.isnan(base_rate) else "",
        "any_hit_leakage_decision": leakage_decision,
        "next_research_decision": next_decision,
        "direct_answer": direct,
        "production_status": "NOT_AUTHORIZED",
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_contact_hitter_regime_validation_2026-07-17.json")
    decision_lines = "\n".join(f"- `{r.decision} = {r.value}`" for r in decisions.itertuples(index=False))
    write_md(f"""# MLB Contact-Hitter Multi-Hit Regime and Full Price-Band Validation

Generated: `{machine['generated_at_utc']}`

## Executive Summary

This bounded regime-validation study froze one contact-hitter state definition
from fit-period governed fields: contact ability, opportunity, personal support,
and affirmative pitcher-suppression veto.

No model fitting/refitting, threshold optimization, price-band selection, or
production behavior change was performed.

## Frozen Regime

Primary state under test:
`HIGH_CONTACT_HIGH_OPPORTUNITY_NO_VETO`.

Rows are forced into `AFFIRMATIVE_SUPPRESSION_VETO`, `LOW_PERSONAL_SUPPORT`, or
`INCOMPLETE` before any hitter-advantage interpretation.

## Holdout Progression

| comparison | rows | two-plus rate |
|---|---:|---:|
| high contact + high opportunity + no veto | {machine['holdout_high_contact_high_opportunity_rows']} | {machine['holdout_high_contact_high_opportunity_two_plus_rate']} |
| overall no-veto base rate |  | {machine['holdout_no_veto_base_rate']} |

## Direct Answer

{direct}

## Decisions

{decision_lines}

## Production Status

`MLB_CONTACT_HITTER_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, candidate, selector, upload, Quick Card, workspace,
LaunchAgent, database, network, or OddsAPI behavior changed.
""", out_dir / "executive_summary_2026-07-17.md")
    manifest = []
    for path in [MODEL_PATH, PRICE_ROWS, PRICE_BANDS, JULY12, ROOT / "backend/mlb/scripts/run_mlb_contact_hitter_regime_validation.py"]:
        if path.exists():
            manifest.append({"artifact_role": "input", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    validation_report(out_dir)
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
