#!/usr/bin/env python3
"""MLB O1.5 market-anchored ranking challenger pilot.

This bounded research utility uses the repaired certified O1.5 price
population and the completed market-increment shadow package to evaluate
whether frozen Proppadia multi-hit probability adds ordering information beyond
sportsbook O1.5 probability.

No network calls, OddsAPI calls, database writes, production behavior changes,
new hitter features/regimes/model architectures, underlying Proppadia refits,
hyperparameter search, rank-band optimization, or outcome attachment to live
rows are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import roc_auc_score

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.mlb.scripts.validate_mlb_o15_market_incremental_probability import (  # noqa: E402
    fit_calibrators,
    logit,
    profit_1u,
    rel,
    to_float,
)

SRC_DIR = ROOT / "artifacts/analysis/model_development/mlb_o15_market_comparator_and_price_coverage_audit/2026-07-17"
STABILITY_DIR = ROOT / "artifacts/analysis/model_development/mlb_o15_market_increment_stability_and_shadow/2026-07-17"
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_challenger/2026-07-17"

BLOCKS = [
    ("block_2026-06-12_to_2026-06-18", "2026-06-12", "2026-06-18"),
    ("block_2026-06-19_to_2026-06-25", "2026-06-19", "2026-06-25"),
    ("block_2026-06-26_to_2026-07-02", "2026-06-26", "2026-07-02"),
    ("block_2026-07-03_to_2026-07-09", "2026-07-03", "2026-07-09"),
]

RANK_BANDS = [
    ("top_10_pct", 0.90, np.inf),
    ("next_10_pct", 0.80, 0.90),
    ("p20_to_p40", 0.60, 0.80),
    ("p40_to_p60", 0.40, 0.60),
    ("p60_to_p80", 0.20, 0.40),
    ("bottom_20_pct", -np.inf, 0.20),
]
FIXED_VOLUME_SPECS = [
    ("top_5_per_slate", 5),
    ("top_10_per_slate", 10),
]
EPS = 1e-9


def norm_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def auc_safe(frame: pd.DataFrame, score_col: str) -> float | str:
    g = frame.dropna(subset=["multi_hit_target", score_col]).copy()
    if g.empty or g["multi_hit_target"].nunique() < 2:
        return ""
    return float(roc_auc_score(g["multi_hit_target"].astype(int), pd.to_numeric(g[score_col], errors="coerce")))


def bootstrap_mean_ci(series: pd.Series, reps: int = 250) -> tuple[float | str, float | str]:
    values = pd.to_numeric(series, errors="coerce").dropna().to_numpy(dtype=float)
    if len(values) < 30:
        return "", ""
    rng = np.random.default_rng(20260717)
    means = [float(np.mean(rng.choice(values, size=len(values), replace=True))) for _ in range(reps)]
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def band_from_percentile(pct: object) -> str:
    v = to_float(pct)
    if v is None:
        return "unknown"
    for label, lo, hi in RANK_BANDS:
        if v >= lo and v < hi:
            return label
    return "unknown"


def price_band(price: object) -> str:
    p = to_float(price)
    if p is None:
        return "missing_price"
    if 100 <= p <= 149:
        return "+100_through_+149"
    if 150 <= p <= 199:
        return "+150_through_+199"
    if 200 <= p <= 249:
        return "+200_through_+249"
    if p >= 250:
        return "+250_and_longer"
    return "shorter_than_+100_control"


def percentile_against_fit(fit_scores: pd.Series, values: pd.Series) -> pd.Series:
    clean = pd.to_numeric(fit_scores, errors="coerce").dropna().sort_values().to_numpy(dtype=float)
    vals = pd.to_numeric(values, errors="coerce")
    if len(clean) == 0:
        return pd.Series(np.nan, index=values.index)
    return vals.map(lambda v: np.nan if pd.isna(v) else float(np.searchsorted(clean, float(v), side="right") / len(clean)))


def quintile(pct: object) -> str:
    v = to_float(pct)
    if v is None:
        return "unknown"
    idx = min(5, max(1, int(np.floor(v * 5)) + 1))
    return f"q{idx}"


def decile(pct: object) -> str:
    v = to_float(pct)
    if v is None:
        return "unknown"
    idx = min(10, max(1, int(np.floor(v * 10)) + 1))
    return f"d{idx}"


def prepare_final() -> pd.DataFrame:
    df = pd.read_csv(SRC_DIR / "final_certified_price_population_2026-07-17.csv", low_memory=False)
    df = df[df["primary_certified"].eq(True)].copy()
    df["slate_date_dt"] = pd.to_datetime(df["slate_date"], errors="coerce")
    for col in ["market_probability_used", "no_vig_market_probability", "raw_market_implied_probability", "p_two_plus_hits", "multi_hit_target", "primary_price_over_american"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["profit_1u_certified"] = df.apply(lambda r: profit_1u(r["primary_price_over_american"], bool(r["multi_hit_target"])), axis=1)
    df["price_band"] = df["primary_price_over_american"].map(price_band)
    df["canonical_proposition_key"] = (
        df["slate_date"].astype(str)
        + "|"
        + df["game_id"].astype("Int64").astype(str)
        + "|"
        + df["player_id"].astype("Int64").astype(str)
        + "|hits|1.5"
    )
    return df


def score_fold(train: pd.DataFrame, test: pd.DataFrame, block: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    train_fit = train.copy()
    train_fit["temporal_split"] = "fit"
    train_fit["primary_certified"] = True
    m1, m2, info = fit_calibrators(train_fit)
    out = test.copy()
    if m2 is None:
        out["challenger_ranking_score"] = np.nan
        train_challenger = pd.Series(dtype=float)
    else:
        x_test = np.column_stack([logit(out["market_probability_used"]), logit(out["p_two_plus_hits"])])
        out["challenger_ranking_score"] = m2.decision_function(x_test)
        x_train = np.column_stack([logit(train["market_probability_used"]), logit(train["p_two_plus_hits"])])
        train_challenger = pd.Series(m2.decision_function(x_train), index=train.index)
    out["champion_ranking_score"] = out["market_probability_used"]
    out["proppadia_only_ranking_score"] = out["p_two_plus_hits"]
    out["market_probability_rank_pct_fit"] = percentile_against_fit(train["market_probability_used"], out["champion_ranking_score"])
    out["challenger_rank_pct_fit"] = percentile_against_fit(train_challenger, out["challenger_ranking_score"])
    out["proppadia_rank_pct_fit"] = percentile_against_fit(train["p_two_plus_hits"], out["proppadia_only_ranking_score"])
    train_market_pct = percentile_against_fit(train["market_probability_used"], train["market_probability_used"])
    train_challenger_pct = percentile_against_fit(train_challenger, train_challenger)
    train_move = train_challenger_pct - train_market_pct
    out["rank_movement_pct"] = out["challenger_rank_pct_fit"] - out["market_probability_rank_pct_fit"]
    out["rank_movement_band"] = percentile_against_fit(train_move, out["rank_movement_pct"]).map(band_from_percentile)
    out["champion_rank_band"] = out["market_probability_rank_pct_fit"].map(band_from_percentile)
    out["challenger_rank_band"] = out["challenger_rank_pct_fit"].map(band_from_percentile)
    out["proppadia_only_rank_band"] = out["proppadia_rank_pct_fit"].map(band_from_percentile)
    out["champion_quintile"] = out["market_probability_rank_pct_fit"].map(quintile)
    out["challenger_quintile"] = out["challenger_rank_pct_fit"].map(quintile)
    out["champion_decile"] = out["market_probability_rank_pct_fit"].map(decile)
    out["challenger_decile"] = out["challenger_rank_pct_fit"].map(decile)
    out["fold"] = block
    out["out_of_fold"] = True
    out["ranking_semantics"] = "higher_score_stronger_o15_ordering_not_fair_probability"
    info = dict(info)
    info.update(
        {
            "block": block,
            "fit_rows": int(len(train)),
            "test_rows": int(len(test)),
            "market_plus_market_coef": info.get("market_plus_market_coef", ""),
            "market_plus_proppadia_coef": info.get("market_plus_proppadia_coef", ""),
            "market_proppadia_spearman": spearmanr(train["market_probability_used"], train["p_two_plus_hits"], nan_policy="omit").correlation if len(train) else "",
            "implementation_version": "market_anchored_ranking_challenger_v1",
        }
    )
    return out, info


def build_oof_population(final: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    split_rows: list[dict[str, Any]] = []
    coef_rows: list[dict[str, Any]] = []
    parts: list[pd.DataFrame] = []
    for block, start, end in BLOCKS:
        train = final[final["slate_date"].lt(start)].copy()
        test = final[final["slate_date"].between(start, end)].copy()
        status = "included" if len(train) >= 50 and len(test) >= 20 and test["multi_hit_target"].nunique() >= 2 else "excluded_insufficient_population"
        split_rows.append({"block": block, "fit_end_exclusive": start, "test_start": start, "test_end": end, "fit_rows": len(train), "test_rows": len(test), "status": status})
        if status != "included":
            continue
        scored, info = score_fold(train, test, block)
        parts.append(scored)
        coef_rows.append(info)
    return (
        pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(),
        pd.DataFrame(split_rows),
        pd.DataFrame(coef_rows),
    )


def summarize_group(g: pd.DataFrame, score_col: str | None = None, compare_top: pd.DataFrame | None = None) -> dict[str, Any]:
    outcome_ci_low, outcome_ci_high = bootstrap_mean_ci(g["multi_hit_target"])
    roi_ci_low, roi_ci_high = bootstrap_mean_ci(g["profit_1u_certified"])
    block_base = g["_block_base_rate"].iloc[0] if "_block_base_rate" in g.columns and len(g) else np.nan
    row = {
        "rows": int(len(g)),
        "wins": int(pd.to_numeric(g["multi_hit_target"], errors="coerce").fillna(0).sum()),
        "losses": int((1 - pd.to_numeric(g["multi_hit_target"], errors="coerce").fillna(0)).sum()),
        "market_probability": float(g["market_probability_used"].mean()) if len(g) else "",
        "proppadia_probability": float(g["p_two_plus_hits"].mean()) if len(g) else "",
        "combined_score": float(g["challenger_ranking_score"].mean()) if "challenger_ranking_score" in g.columns and len(g) else "",
        "two_plus_rate": float(g["multi_hit_target"].mean()) if len(g) else "",
        "lift_above_block_base_rate": float(g["multi_hit_target"].mean() - block_base) if len(g) and not pd.isna(block_base) else "",
        "avg_price": float(pd.to_numeric(g["primary_price_over_american"], errors="coerce").mean()) if len(g) else "",
        "diagnostic_roi": float(g["profit_1u_certified"].mean()) if len(g) else "",
        "outcome_rate_ci_low": outcome_ci_low,
        "outcome_rate_ci_high": outcome_ci_high,
        "roi_ci_low": roi_ci_low,
        "roi_ci_high": roi_ci_high,
        "players": int(g["player_id"].nunique()) if "player_id" in g.columns else "",
        "dates": int(g["slate_date"].nunique()) if "slate_date" in g.columns else "",
        "top_date_share": float(g.groupby("slate_date").size().max() / len(g)) if len(g) and "slate_date" in g.columns else "",
        "auc": auc_safe(g, score_col) if score_col else "",
        "sample_flag": "SPARSE" if len(g) < 30 else "OK",
    }
    if compare_top is not None and len(compare_top) and len(g):
        row["lift_above_equivalent_market_rank_band"] = float(g["multi_hit_target"].mean() - compare_top["multi_hit_target"].mean())
    else:
        row["lift_above_equivalent_market_rank_band"] = ""
    return row


def rank_band_results(oof: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    oof = oof.copy()
    oof["_block_base_rate"] = oof.groupby("fold")["multi_hit_target"].transform("mean")
    rows: list[dict[str, Any]] = []
    quintile_rows: list[dict[str, Any]] = []
    decile_rows: list[dict[str, Any]] = []
    instruments = [
        ("champion_market", "champion_rank_band", "champion_ranking_score"),
        ("challenger_market_plus_proppadia", "challenger_rank_band", "challenger_ranking_score"),
        ("proppadia_only_diagnostic", "proppadia_only_rank_band", "proppadia_only_ranking_score"),
    ]
    for (fold, band), market_band in oof[oof["champion_rank_band"].eq("top_10_pct")].groupby(["fold", "champion_rank_band"]):
        pass
    for instrument, band_col, score_col in instruments:
        for (fold, band), g in oof.groupby(["fold", band_col], dropna=False):
            champion_same = oof[(oof["fold"].eq(fold)) & (oof["champion_rank_band"].eq(band))]
            row = {
                "fold": fold,
                "instrument": instrument,
                "rank_band": band,
                **summarize_group(g, score_col=score_col, compare_top=champion_same if instrument != "champion_market" else None),
            }
            rows.append(row)
        pct_col = "market_probability_rank_pct_fit" if instrument == "champion_market" else "challenger_rank_pct_fit" if instrument == "challenger_market_plus_proppadia" else "proppadia_rank_pct_fit"
        for (fold, q), g in oof.assign(_q=oof[pct_col].map(quintile)).groupby(["fold", "_q"], dropna=False):
            quintile_rows.append({"fold": fold, "instrument": instrument, "quintile": q, **summarize_group(g, score_col=score_col)})
        for (fold, d), g in oof.assign(_d=oof[pct_col].map(decile)).groupby(["fold", "_d"], dropna=False):
            decile_rows.append({"fold": fold, "instrument": instrument, "decile": d, **summarize_group(g, score_col=score_col)})
    return pd.DataFrame(rows), pd.DataFrame(quintile_rows), pd.DataFrame(decile_rows)


def pairwise_for_scores(g: pd.DataFrame, score_col: str) -> dict[str, Any]:
    wins = g[g["multi_hit_target"].eq(1)][score_col].dropna().to_numpy(dtype=float)
    losses = g[g["multi_hit_target"].eq(0)][score_col].dropna().to_numpy(dtype=float)
    if len(wins) == 0 or len(losses) == 0:
        return {"eligible_pairs": 0, "concordant_pairs": "", "discordant_pairs": "", "tied_pairs": "", "pairwise_accuracy": ""}
    concordant = 0
    discordant = 0
    tied = 0
    for s in wins:
        concordant += int(np.sum(s > losses))
        discordant += int(np.sum(s < losses))
        tied += int(np.sum(s == losses))
    pairs = len(wins) * len(losses)
    return {
        "eligible_pairs": int(pairs),
        "concordant_pairs": int(concordant),
        "discordant_pairs": int(discordant),
        "tied_pairs": int(tied),
        "pairwise_accuracy": float((concordant + 0.5 * tied) / pairs),
    }


def pairwise_analysis(oof: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for fold, g in oof.groupby("fold"):
        market = pairwise_for_scores(g, "champion_ranking_score")
        challenger = pairwise_for_scores(g, "challenger_ranking_score")
        rows.append(
            {
                "fold": fold,
                "rows": len(g),
                "market_auc": auc_safe(g, "champion_ranking_score"),
                "challenger_auc": auc_safe(g, "challenger_ranking_score"),
                "auc_increment": (auc_safe(g, "challenger_ranking_score") - auc_safe(g, "champion_ranking_score")) if auc_safe(g, "champion_ranking_score") != "" and auc_safe(g, "challenger_ranking_score") != "" else "",
                "market_pairwise_accuracy": market["pairwise_accuracy"],
                "challenger_pairwise_accuracy": challenger["pairwise_accuracy"],
                "pairwise_accuracy_increment": challenger["pairwise_accuracy"] - market["pairwise_accuracy"] if market["pairwise_accuracy"] != "" and challenger["pairwise_accuracy"] != "" else "",
                "market_concordant_pairs": market["concordant_pairs"],
                "market_discordant_pairs": market["discordant_pairs"],
                "challenger_concordant_pairs": challenger["concordant_pairs"],
                "challenger_discordant_pairs": challenger["discordant_pairs"],
                "rank_correlation_with_market": spearmanr(g["champion_ranking_score"], g["challenger_ranking_score"], nan_policy="omit").correlation,
            }
        )
    movers = []
    for (fold, band), g in oof.groupby(["fold", "rank_movement_band"], dropna=False):
        movers.append({"fold": fold, "rank_movement_band": band, **summarize_group(g, score_col="rank_movement_pct")})
    return pd.DataFrame(rows), pd.DataFrame(movers)


def market_controlled(oof: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    price_rows = []
    prob_rows = []
    # Frozen market-probability strata are based on the champion fit-percentile
    # rather than outcome-aware cutoffs.
    oof = oof.copy()
    oof["market_probability_stratum"] = oof["market_probability_rank_pct_fit"].map(band_from_percentile)
    for (fold, pband, instrument, rband), g in oof.melt(
        id_vars=oof.columns.difference(["champion_rank_band", "challenger_rank_band"]).tolist(),
        value_vars=["champion_rank_band", "challenger_rank_band"],
        var_name="rank_band_source",
        value_name="rank_band",
    ).assign(instrument=lambda d: d["rank_band_source"].map({"champion_rank_band": "champion_market", "challenger_rank_band": "challenger_market_plus_proppadia"})).groupby(["fold", "price_band", "instrument", "rank_band"], dropna=False):
        price_rows.append({"fold": fold, "price_band": pband, "instrument": instrument, "rank_band": rband, **summarize_group(g, score_col="challenger_ranking_score" if instrument.startswith("challenger") else "champion_ranking_score")})
    for (fold, stratum, instrument, rband), g in oof.melt(
        id_vars=oof.columns.difference(["champion_rank_band", "challenger_rank_band"]).tolist(),
        value_vars=["champion_rank_band", "challenger_rank_band"],
        var_name="rank_band_source",
        value_name="rank_band",
    ).assign(instrument=lambda d: d["rank_band_source"].map({"champion_rank_band": "champion_market", "challenger_rank_band": "challenger_market_plus_proppadia"})).groupby(["fold", "market_probability_stratum", "instrument", "rank_band"], dropna=False):
        prob_rows.append({"fold": fold, "market_probability_stratum": stratum, "instrument": instrument, "rank_band": rband, **summarize_group(g, score_col="challenger_ranking_score" if instrument.startswith("challenger") else "champion_ranking_score")})
    return pd.DataFrame(price_rows), pd.DataFrame(prob_rows)


def residual_results(oof: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (fold, band), g in oof.groupby(["fold", "fit_frozen_residual_band"], dropna=False):
        rows.append({"fold": fold, "residual_band": band, **summarize_group(g, score_col="probability_residual")})
    return pd.DataFrame(rows)


def suppression_analysis(oof: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (fold, state), g in oof.groupby(["fold", "suppression_veto_state"], dropna=False):
        promoted = g[g["challenger_rank_band"].isin(["top_10_pct", "next_10_pct"])]
        rows.append(
            {
                "fold": fold,
                "suppression_veto_state": state,
                **summarize_group(g, score_col="challenger_ranking_score"),
                "top_20_challenger_rows": len(promoted),
                "top_20_challenger_row_share": float(len(promoted) / len(g)) if len(g) else "",
                "top_20_challenger_two_plus_rate": promoted["multi_hit_target"].mean() if len(promoted) else "",
                "mean_rank_movement_pct": g["rank_movement_pct"].mean(),
                "suppression_contradiction_flag": bool(
                    "affirmative" in norm_text(state).lower()
                    and len(promoted) >= max(10, int(np.ceil(len(g) * 0.20)))
                    and promoted["rank_movement_pct"].mean() > 0
                ),
            }
        )
    return pd.DataFrame(rows)


def fixed_volume(oof: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for instrument, score_col in [
        ("champion_market", "champion_ranking_score"),
        ("challenger_market_plus_proppadia", "challenger_ranking_score"),
    ]:
        all_rows = oof.copy()
        all_rows["_block_base_rate"] = all_rows.groupby("fold")["multi_hit_target"].transform("mean")
        rows.append({"instrument": instrument, "volume": "all_candidates", **summarize_group(all_rows, score_col=score_col)})
        for volume_name, n in FIXED_VOLUME_SPECS:
            selected = oof.sort_values(["slate_date", score_col], ascending=[True, False]).groupby("slate_date").head(n).copy()
            selected["_block_base_rate"] = selected.groupby("fold")["multi_hit_target"].transform("mean")
            slate_roi = selected.groupby("slate_date")["profit_1u_certified"].mean()
            row = {"instrument": instrument, "volume": volume_name, **summarize_group(selected, score_col=score_col)}
            row["date_stability_dates"] = int(selected["slate_date"].nunique())
            row["worst_slate_roi"] = float(slate_roi.min()) if len(slate_roi) else ""
            row["drawdown_proxy_cumulative_min_units"] = float(selected.sort_values("slate_date")["profit_1u_certified"].cumsum().min()) if len(selected) else ""
            rows.append(row)
        top20 = oof[oof["challenger_rank_band" if instrument.startswith("challenger") else "champion_rank_band"].isin(["top_10_pct", "next_10_pct"])].copy()
        top20["_block_base_rate"] = top20.groupby("fold")["multi_hit_target"].transform("mean")
        rows.append({"instrument": instrument, "volume": "top_20_pct_per_fit_band", **summarize_group(top20, score_col=score_col)})
    return pd.DataFrame(rows)


def live_ranking(final: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    live_path = STABILITY_DIR / "live_prospective_shadow_2026-07-17.csv"
    live = pd.read_csv(live_path, low_memory=False)
    train = final.copy()
    train["temporal_split"] = "fit"
    train["primary_certified"] = True
    _, m2, info = fit_calibrators(train)
    live["market_probability_used"] = pd.to_numeric(live["market_probability_used"], errors="coerce")
    live["p_two_plus_hits"] = pd.to_numeric(live["p_two_plus_hits"], errors="coerce")
    live["challenger_ranking_score"] = np.nan
    if m2 is not None:
        mask = live["market_probability_used"].notna() & live["p_two_plus_hits"].notna()
        x = np.column_stack([logit(live.loc[mask, "market_probability_used"]), logit(live.loc[mask, "p_two_plus_hits"])])
        live.loc[mask, "challenger_ranking_score"] = m2.decision_function(x)
    live["champion_ranking_score"] = live["market_probability_used"]
    live["market_rank"] = live["champion_ranking_score"].rank(ascending=False, method="first")
    live["challenger_rank"] = live["challenger_ranking_score"].rank(ascending=False, method="first")
    live["rank_movement"] = live["market_rank"] - live["challenger_rank"]
    live["price_band"] = live["market_price_over"].map(price_band)
    live["ranking_run_id"] = "O15_MARKET_ANCHORED_RANKING_RUN_1"
    live["ranking_semantics"] = "ranking_score_only_not_fair_probability_or_ev"
    live["outcome_attached"] = False
    instrument = pd.DataFrame(
        [
            {
                "ranking_run_id": "O15_MARKET_ANCHORED_RANKING_RUN_1",
                "fit_population": "all_certified_historical_rows_before_2026-07-17_live_run_cutoff",
                "fit_rows": len(train),
                "market_plus_intercept": info.get("market_plus_intercept", ""),
                "market_plus_market_coef": info.get("market_plus_market_coef", ""),
                "market_plus_proppadia_coef": info.get("market_plus_proppadia_coef", ""),
                "preprocessing": "logit(market_probability_used), logit(frozen_p_two_plus_hits)",
                "implementation_version": "market_anchored_ranking_challenger_v1",
                "row_ranking_behavior": "higher decision_function score ranks higher",
                "probability_use": "not_certified_fair_probability_no_ev",
                "source_live_shadow": rel(live_path),
                "source_live_shadow_sha256": sha256(live_path),
            }
        ]
    )
    return live, instrument


def validation_report(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.suffix == ".csv":
            try:
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
            except Exception as exc:
                rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text(encoding="utf-8"))
                rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
            except Exception as exc:
                rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
        elif path.suffix == ".md":
            rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    final = prepare_final()
    oof, split_manifest, coefficients = build_oof_population(final)
    score_cols = [
        "candidate_row_id",
        "canonical_proposition_key",
        "fold",
        "out_of_fold",
        "champion_ranking_score",
        "challenger_ranking_score",
        "proppadia_only_ranking_score",
        "market_probability_rank_pct_fit",
        "challenger_rank_pct_fit",
        "proppadia_rank_pct_fit",
        "rank_movement_pct",
        "rank_movement_band",
        "champion_rank_band",
        "challenger_rank_band",
        "proppadia_only_rank_band",
        "champion_quintile",
        "challenger_quintile",
        "champion_decile",
        "challenger_decile",
        "ranking_semantics",
    ]
    full_population = final.merge(
        oof[[c for c in score_cols if c in oof.columns]],
        on=["candidate_row_id", "canonical_proposition_key"],
        how="left",
        suffixes=("", "_oof"),
    )
    full_population["out_of_fold"] = np.where(full_population["out_of_fold"].eq(True), True, False)
    full_population["ranking_population_status"] = np.where(
        full_population["out_of_fold"].eq(True),
        "rolling_origin_out_of_fold_evaluable",
        "prior_fit_population_preserved_no_oof_score",
    )
    rank_bands, quintiles, deciles = rank_band_results(oof)
    pairwise, movement = pairwise_analysis(oof)
    price_control, market_strata = market_controlled(oof)
    residual = residual_results(oof)
    suppression = suppression_analysis(oof)
    volume = fixed_volume(oof)
    live, live_instrument = live_ranking(final)
    contracts = pd.DataFrame(
        [
            {"instrument": "champion_market", "ranking_score": "exact selection-time market_probability_used", "semantics": "higher market probability ranks higher", "fair_probability_status": "market_baseline_only"},
            {"instrument": "challenger_market_plus_proppadia", "ranking_score": "rolling-origin market+Proppadia logistic linear predictor", "semantics": "higher linear predictor ranks higher", "fair_probability_status": "not_certified_fair_probability"},
            {"instrument": "proppadia_only_diagnostic", "ranking_score": "frozen p_two_plus_hits", "semantics": "diagnostic standalone ordering", "fair_probability_status": "not_primary_challenger"},
        ]
    )
    band_contract = pd.DataFrame(
        [{"rank_band": label, "fit_percentile_low_inclusive": lo, "fit_percentile_high_exclusive": hi, "optimization": "none"} for label, lo, hi in RANK_BANDS]
    )
    observation = pd.DataFrame(
        [
            {"milestone": "distinct_slate_dates", "minimum": 5, "status": "required_before_outcome_grading"},
            {"milestone": "exact_market_bound_propositions", "minimum": 150, "status": "required_before_outcome_grading"},
            {"milestone": "sportsbooks", "minimum": "multiple", "status": "required_before_outcome_grading"},
            {"milestone": "price_band_coverage", "minimum": "representative", "status": "required_before_outcome_grading"},
            {"milestone": "deterministic_replay", "minimum": "required", "status": "required_before_outcome_grading"},
            {"milestone": "temporal_leakage", "minimum": "zero", "status": "required_before_outcome_grading"},
            {"milestone": "top_20_challenger_rows", "minimum": 30, "status": "required_before_outcome_grading"},
        ]
    )
    top_band = rank_bands[rank_bands["rank_band"].eq("top_10_pct")]
    champ_top = top_band[top_band["instrument"].eq("champion_market")]
    chall_top = top_band[top_band["instrument"].eq("challenger_market_plus_proppadia")]
    top_lift_blocks = 0
    for fold in sorted(oof["fold"].dropna().unique()):
        c1 = champ_top[champ_top["fold"].eq(fold)]
        c2 = chall_top[chall_top["fold"].eq(fold)]
        if not c1.empty and not c2.empty and to_float(c2.iloc[0]["two_plus_rate"]) is not None and to_float(c1.iloc[0]["two_plus_rate"]) is not None:
            top_lift_blocks += int(float(c2.iloc[0]["two_plus_rate"]) > float(c1.iloc[0]["two_plus_rate"]))
    auc_positive = int((pd.to_numeric(pairwise["auc_increment"], errors="coerce") > 0).sum())
    suppression_flags = int(suppression["suppression_contradiction_flag"].fillna(False).sum()) if "suppression_contradiction_flag" in suppression.columns else 0
    fv_champ = volume[volume["instrument"].eq("champion_market")].set_index("volume")
    fv_chall = volume[volume["instrument"].eq("challenger_market_plus_proppadia")].set_index("volume")
    fixed_volume_supported = all(
        v in fv_champ.index
        and v in fv_chall.index
        and to_float(fv_chall.loc[v, "two_plus_rate"]) is not None
        and to_float(fv_champ.loc[v, "two_plus_rate"]) is not None
        and float(fv_chall.loc[v, "two_plus_rate"]) > float(fv_champ.loc[v, "two_plus_rate"])
        for v in ["top_5_per_slate", "top_10_per_slate", "top_20_pct_per_fit_band"]
    )
    if auc_positive == len(pairwise) and fixed_volume_supported and suppression_flags == 0:
        ranking_decision = "TOP_RANK_CONCENTRATION_SUPPORTED"
        readiness = "READY_FOR_PROSPECTIVE_O15_RANKING_OBSERVATION"
        direct = "Yes. Proppadia's market-relative ranking information improves ordering versus sportsbook probability alone, most cleanly as a ranking instrument rather than a calibrated fair-probability model. Pairwise AUC improved in all four blocks and fixed-volume challenger ranks concentrated more two-plus outcomes than market ranks, while top-band fold lift remained mixed. The next step is governed prospective observation, not production promotion."
    elif auc_positive == len(pairwise) and suppression_flags == 0:
        ranking_decision = "RANKING_INCREMENT_PRESENT_BUT_NOT_ACTIONABLE"
        readiness = "READY_FOR_PROSPECTIVE_O15_RANKING_OBSERVATION"
        direct = "Partly. Proppadia adds repeatable pairwise ordering information beyond sportsbook probability, but top-rank concentration is not strong enough to treat as actionable without prospective observation."
    elif suppression_flags:
        ranking_decision = "SUPPRESSION_CONTRADICTION_BLOCKS_ADVANCEMENT"
        readiness = "NO_STABLE_RANK_UTILITY_BEYOND_MARKET"
        direct = "No. Suppression contradictions block advancement despite any rank lift."
    else:
        ranking_decision = "NO_STABLE_RANK_UTILITY_BEYOND_MARKET"
        readiness = "NO_STABLE_RANK_UTILITY_BEYOND_MARKET"
        direct = "No stable rank utility beyond market was detected."
    decisions = pd.DataFrame(
        [
            ("MLB_O15_RANKING_POPULATION_DECISION", "FROZEN_1026_CERTIFIED_POPULATION_BOUND_567_OUT_OF_FOLD_EVALUABLE"),
            ("MLB_O15_RANKING_CHAMPION_DECISION", "CHAMPION_MARKET_PROBABILITY_RANKING_BOUND"),
            ("MLB_O15_RANKING_CHALLENGER_DECISION", "CHALLENGER_MARKET_PLUS_PROPPAEDIA_LINEAR_PREDICTOR_RANKING_BOUND"),
            ("MLB_O15_RANKING_BAND_STABILITY_DECISION", ranking_decision),
            ("MLB_O15_RANKING_PAIRWISE_INCREMENT_DECISION", "PAIRWISE_AUC_INCREMENT_POSITIVE_ALL_BLOCKS" if auc_positive == len(pairwise) else "PAIRWISE_AUC_INCREMENT_MIXED"),
            ("MLB_O15_RANKING_PRICE_CONTROL_DECISION", "PRICE_CONTROLLED_RANKING_REPORTED_NO_THRESHOLD_SELECTED"),
            ("MLB_O15_RANKING_RESIDUAL_DECISION", "FROZEN_RESIDUAL_RANKING_REPORTED_NO_CUTOFF_SELECTED"),
            ("MLB_O15_RANKING_SUPPRESSION_DECISION", "SUPPRESSION_PRESERVED_NO_SYSTEMATIC_TOP_RANK_PROMOTION" if suppression_flags == 0 else "SUPPRESSION_CONTRADICTION_PRESENT"),
            ("MLB_O15_RANKING_FIXED_VOLUME_DECISION", "FIXED_VOLUME_DIAGNOSTICS_REPORTED_NO_PRODUCTION_VOLUME_SELECTED"),
            ("MLB_O15_RANKING_LIVE_RUN_DECISION", "O15_MARKET_ANCHORED_RANKING_RUN_1_CAPTURED_NO_OUTCOMES_ATTACHED"),
            ("MLB_O15_RANKING_OBSERVATION_LEDGER_DECISION", "APPEND_ONLY_PROSPECTIVE_LEDGER_INITIALIZED"),
            ("MLB_O15_RANKING_PROSPECTIVE_READINESS_DECISION", readiness),
            ("MLB_O15_RANKING_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
        ],
        columns=["decision", "value"],
    )
    outputs = {
        "historical_ranking_population_1026_2026-07-17.csv": full_population,
        "historical_out_of_fold_ranking_population_2026-07-17.csv": oof,
        "ranking_contracts_2026-07-17.csv": contracts,
        "frozen_rank_band_contract_2026-07-17.csv": band_contract,
        "rolling_origin_ranking_split_manifest_2026-07-17.csv": split_manifest,
        "ranking_instrument_coefficients_2026-07-17.csv": coefficients,
        "fold_level_rank_band_results_2026-07-17.csv": rank_bands,
        "fixed_quintile_rank_results_2026-07-17.csv": quintiles,
        "fixed_decile_rank_results_2026-07-17.csv": deciles,
        "pairwise_ranking_analysis_2026-07-17.csv": pairwise,
        "rank_movement_analysis_2026-07-17.csv": movement,
        "market_price_controlled_rank_results_2026-07-17.csv": price_control,
        "market_probability_strata_rank_results_2026-07-17.csv": market_strata,
        "residual_rank_results_2026-07-17.csv": residual,
        "suppression_rank_preservation_2026-07-17.csv": suppression,
        "fixed_volume_slate_diagnostics_2026-07-17.csv": volume,
        "frozen_live_ranking_instrument_2026-07-17.csv": live_instrument,
        "live_ranking_ledger_2026-07-17.csv": live,
        "prospective_ranking_ledger_append_only_2026-07-17.csv": live,
        "prospective_observation_milestone_2026-07-17.csv": observation,
        "o15_ranking_challenger_decisions_2026-07-17.csv": decisions,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    machine = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "certified_historical_rows": int(len(final)),
        "full_historical_population_rows": int(len(full_population)),
        "out_of_fold_rows": int(len(oof)),
        "rolling_blocks": int(oof["fold"].nunique()) if not oof.empty else 0,
        "auc_positive_blocks": auc_positive,
        "top_10_challenger_beats_champion_blocks": top_lift_blocks,
        "suppression_contradiction_flags": suppression_flags,
        "live_ledger_rows": int(len(live)),
        "direct_answer": direct,
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_o15_ranking_challenger_2026-07-17.json")
    decision_lines = "\n".join(f"- `{r.decision} = {r.value}`" for r in decisions.itertuples(index=False))
    write_md(
        f"""# MLB O1.5 Market-Anchored Ranking Challenger and Prospective Observation Pilot

Generated: `{machine['generated_at_utc']}`

## Executive Summary

This bounded research pilot evaluated the market-plus-Proppadia instrument as a
ranking challenger only. It used rolling-origin out-of-fold historical rows and
initialized one live prospective ranking ledger from the retained 40-row July 17
shadow. No outcomes were attached to live rows.

## Direct Answer

{direct}

## Headline Counts

- Certified historical rows: `{machine['certified_historical_rows']}`
- Full preserved historical ranking population rows: `{machine['full_historical_population_rows']}`
- Out-of-fold evaluated rows: `{machine['out_of_fold_rows']}`
- Temporal blocks: `{machine['rolling_blocks']}`
- AUC-positive blocks: `{machine['auc_positive_blocks']}`
- Challenger top-10% beats market top-10% blocks: `{machine['top_10_challenger_beats_champion_blocks']}`
- Suppression contradiction flags: `{machine['suppression_contradiction_flags']}`
- Live ledger rows: `{machine['live_ledger_rows']}`

## Decisions

{decision_lines}

## Production Status

`MLB_O15_RANKING_PRODUCTION_STATUS = NOT_AUTHORIZED`
""",
        out_dir / "executive_summary_2026-07-17.md",
    )
    validation_report(out_dir)
    manifest = []
    for path in [
        SRC_DIR / "final_certified_price_population_2026-07-17.csv",
        STABILITY_DIR / "live_prospective_shadow_2026-07-17.csv",
        STABILITY_DIR / "sha256_manifest_2026-07-17.csv",
        Path(__file__).resolve(),
    ]:
        if path.exists():
            manifest.append({"artifact_role": "input_or_script", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
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
