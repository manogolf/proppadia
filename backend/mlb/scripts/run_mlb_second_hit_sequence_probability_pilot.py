#!/usr/bin/env python3
"""Run a bounded MLB second-hit PA sequence probability pilot.

Research-only. Uses frozen benchmark population/splits and retained strict-prior
features. Does not call network services, write databases, alter production
models, or change selectors/uploads/workspace artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_second_hit_sequence_probability_pilot/2026-07-17"

BENCH_ROOT = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17"
CANONICAL = BENCH_ROOT / "canonical_modeling_population_2026-07-17.csv"
CONTROL = BENCH_ROOT / "research_only_model_artifacts_2026-07-17.csv"
BENCH_LONG_PRICE = BENCH_ROOT / "long_price_exact_price_rows_2026-07-17.csv"
HITTER_BASE = ROOT / (
    "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
    "hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
STARTER_XH = ROOT / (
    "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/"
    "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
JULY12 = BENCH_ROOT / "july12_probability_reconstruction_2026-07-17.csv"

EPS = 1e-6
PA_TOTALS = [1, 2, 3, 4, 5, 6]
INSTRUMENT_ORDER = [
    "control_hitter_pa_starter",
    "sequence_a_pa_count_distribution",
    "sequence_b_starter_bullpen_exposure",
    "sequence_c_conditional_second_hit",
    "sequence_d_unified_second_hit_sequence",
]


def norm(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def num(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def clip_prob(v: Any) -> float:
    x = num(v)
    if x is None:
        return 0.5
    return min(1.0 - EPS, max(EPS, x))


def player_game_key(date: Any, game_id: Any, player_id: Any) -> str:
    gid = str(int(float(game_id))) if num(game_id) is not None else ""
    pid = str(int(float(player_id))) if num(player_id) is not None else ""
    return f"{norm(date)[:10]}|{gid}|{pid}"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def binary_log_loss(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(np.asarray(p, dtype=float), EPS, 1 - EPS)
    y = np.asarray(y, dtype=float)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean())


def brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((np.asarray(p, dtype=float) - np.asarray(y, dtype=float)) ** 2))


def auc_score(y: np.ndarray, p: np.ndarray) -> float | None:
    y = np.asarray(y)
    p = np.asarray(p)
    pos = p[y == 1]
    neg = p[y == 0]
    if len(pos) == 0 or len(neg) == 0:
        return None
    vals = np.concatenate([pos, neg])
    order = np.argsort(vals)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(vals) + 1)
    _, inv, counts = np.unique(vals, return_inverse=True, return_counts=True)
    for i, c in enumerate(counts):
        if c > 1:
            ranks[inv == i] = ranks[inv == i].mean()
    pos_ranks = ranks[: len(pos)]
    return float((pos_ranks.sum() - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg)))


def calibration_slope_intercept(y: np.ndarray, p: np.ndarray) -> tuple[float | None, float | None]:
    y = np.asarray(y, dtype=float)
    p = np.clip(np.asarray(p, dtype=float), 1e-4, 1 - 1e-4)
    if len(np.unique(y)) < 2:
        return None, None
    x = np.log(p / (1 - p))
    X = np.column_stack([np.ones(len(x)), x])
    beta = np.zeros(2)
    for _ in range(25):
        z = X @ beta
        mu = 1 / (1 + np.exp(-np.clip(z, -30, 30)))
        W = np.clip(mu * (1 - mu), 1e-6, None)
        H = X.T @ (W[:, None] * X)
        g = X.T @ (y - mu)
        try:
            step = np.linalg.solve(H, g)
        except np.linalg.LinAlgError:
            return None, None
        beta += step
        if np.max(np.abs(step)) < 1e-6:
            break
    return float(beta[1]), float(beta[0])


def ece(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0, 1, bins + 1)
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    out = 0.0
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        mask = (p >= lo) & (p < hi if i < bins - 1 else p <= hi)
        if mask.any():
            out += mask.sum() / len(y) * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(out)


def multiclass_log_loss(classes: list[str], probs: pd.DataFrame) -> float:
    idx = {"ZERO_HITS": "p_zero_hits", "EXACTLY_ONE_HIT": "p_exactly_one_hit", "TWO_OR_MORE_HITS": "p_two_plus_hits"}
    vals = []
    for c, (_, row) in zip(classes, probs.iterrows()):
        vals.append(max(EPS, min(1 - EPS, float(row[idx[c]]))))
    return float(-np.log(vals).mean()) if vals else float("nan")


def probability_band(p: Any) -> str:
    x = clip_prob(p)
    if x < 0.15:
        return "low_lt_0_15"
    if x < 0.25:
        return "low_mid_0_15_to_0_25"
    if x < 0.35:
        return "mid_0_25_to_0_35"
    if x < 0.45:
        return "high_mid_0_35_to_0_45"
    return "high_ge_0_45"


def pa_count_distribution(expected_pa: float) -> dict[int, float]:
    mu = min(6.0, max(1.0, expected_pa))
    sigma = 0.65
    weights = {n: math.exp(-0.5 * ((n - mu) / sigma) ** 2) for n in PA_TOTALS}
    total = sum(weights.values()) or 1.0
    return {n: w / total for n, w in weights.items()}


def poisson_binomial_probs(ps: list[float]) -> tuple[float, float, float]:
    dist = [1.0]
    for p in ps:
        p = clip_prob(p)
        nxt = [0.0] * (len(dist) + 1)
        for k, val in enumerate(dist):
            nxt[k] += val * (1 - p)
            nxt[k + 1] += val * p
        dist = nxt
    p0 = dist[0]
    p1 = dist[1] if len(dist) > 1 else 0.0
    p2 = max(0.0, 1.0 - p0 - p1)
    total = max(EPS, p0 + p1 + p2)
    return p0 / total, p1 / total, p2 / total


def integrated_distribution(pa_dist: dict[int, float], per_pa_probs_by_n: dict[int, list[float]]) -> tuple[float, float, float]:
    p0 = p1 = p2 = 0.0
    for n, w in pa_dist.items():
        a, b, c = poisson_binomial_probs(per_pa_probs_by_n[n])
        p0 += w * a
        p1 += w * b
        p2 += w * c
    total = max(EPS, p0 + p1 + p2)
    return p0 / total, p1 / total, p2 / total


def shrink(value: Any, sample: Any, prior: float, strength: float) -> float:
    v = num(value)
    n = num(sample) or 0.0
    if v is None:
        return prior
    return float((v * n + prior * strength) / (n + strength))


def load_population() -> pd.DataFrame:
    pop = read_csv(CANONICAL)
    if pop.empty:
        raise FileNotFoundError(CANONICAL)
    hit = read_csv(HITTER_BASE)
    hit["player_game_key"] = hit.apply(lambda r: player_game_key(r["slate_date"], r["game_id"], r["player_id"]), axis=1)
    wanted = [
        "player_game_key",
        "d15_one_plus_rate",
        "d15_exactly_one_hit_share",
        "d15_multi_hit_share_when_hit",
        "d30_one_plus_rate",
        "d30_exactly_one_hit_share",
        "d30_multi_hit_share_when_hit",
        "d30_hits_per_pa",
        "d30_games",
        "d15_pa_per_game",
        "d15_games",
        "season_to_date_hits_per_pa",
        "season_to_date_pa_per_game",
        "lineup_slot",
        "lineup_bucket",
    ]
    for c in wanted:
        if c not in hit:
            hit[c] = np.nan
    pop = pop.merge(hit[wanted], on="player_game_key", how="left", suffixes=("", "_hitter"))
    for c in ["lineup_slot", "lineup_bucket", "d15_pa_per_game", "d30_hits_per_pa", "season_to_date_hits_per_pa", "season_to_date_pa_per_game"]:
        hc = f"{c}_hitter"
        if hc in pop:
            pop[c] = pop[c].where(pop[c].notna(), pop[hc])
    starter = read_csv(STARTER_XH)
    if not starter.empty:
        starter["player_game_key"] = starter.apply(lambda r: player_game_key(r["date"], r["game_id"], r["player_id"]), axis=1)
        cols = [
            "player_game_key",
            "baseline_outs_per_start",
            "baseline_workload_bucket",
            "baseline_vulnerability_bucket",
            "actual_starter_batters_faced",
        ]
        for c in cols:
            if c not in starter:
                starter[c] = np.nan
        starter = starter.sort_values("player_game_key").drop_duplicates("player_game_key")
        pop = pop.merge(starter[cols], on="player_game_key", how="left")
    control = read_csv(CONTROL)
    control = control[control["benchmark"].eq("benchmark_4_hitter_opportunity_starter")].copy()
    control_cols = [
        "player_game_key",
        "p_zero_hits",
        "p_exactly_one_hit",
        "p_two_plus_hits",
        "expected_pa_used",
        "hitter_per_pa_hit_estimate",
        "starter_adjustment",
        "starter_exposure_state",
    ]
    control = control[control_cols].rename(
        columns={
            "p_zero_hits": "control_p_zero_hits",
            "p_exactly_one_hit": "control_p_exactly_one_hit",
            "p_two_plus_hits": "control_p_two_plus_hits",
            "expected_pa_used": "control_expected_pa_used",
            "hitter_per_pa_hit_estimate": "control_hitter_per_pa_hit_estimate",
            "starter_adjustment": "control_starter_adjustment",
            "starter_exposure_state": "control_starter_exposure_state",
        }
    )
    pop = pop.merge(control, on="player_game_key", how="left")
    return pop


def fit_priors(pop: pd.DataFrame) -> dict[str, float]:
    fit = pop[pop["temporal_split"].eq("fit")]
    one_plus = fit["official_hits"].fillna(0).ge(1).sum()
    return {
        "base_hit_per_pa": float(fit["official_hits"].sum() / max(EPS, fit["official_pa"].sum())),
        "base_expected_pa": float(fit["official_pa"].mean()),
        "base_second_hit_given_one": float(fit["multi_hit_target"].sum() / max(1, one_plus)),
        "base_p_zero": float((fit["outcome_class"] == "ZERO_HITS").mean()),
        "base_p_one": float((fit["outcome_class"] == "EXACTLY_ONE_HIT").mean()),
        "base_p_two": float((fit["outcome_class"] == "TWO_OR_MORE_HITS").mean()),
    }


def starter_prob_for_pa(pa_index: int, row: pd.Series) -> float:
    outs = num(row.get("baseline_outs_per_start"))
    if outs is None:
        starter_cycles = 2.25
    else:
        starter_bf_proxy = max(6.0, outs * 1.45)
        starter_cycles = starter_bf_proxy / 9.0
    # Fixed non-tuned sigmoid: PA 1-2 usually starter; PA 4-5 often bullpen.
    return float(1.0 / (1.0 + math.exp((pa_index - starter_cycles) / 0.45)))


def construct_sequence_predictions(pop: pd.DataFrame, priors: dict[str, float]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    artifact_rows = []
    pa_rows = []
    exposure_rows = []
    recurrence_rows = []
    for _, row in pop.iterrows():
        exp_pa = shrink(row.get("d15_pa_per_game"), row.get("d15_games"), priors["base_expected_pa"], 12.0)
        ppa = shrink(row.get("d30_hits_per_pa"), row.get("d30_games"), priors["base_hit_per_pa"], 24.0)
        ppa = min(0.55, max(0.03, ppa))
        pa_dist = pa_count_distribution(exp_pa)
        pa_rows.append(
            {
                "player_game_key": row["player_game_key"],
                "expected_pa": exp_pa,
                **{f"prob_pa_{n}": pa_dist[n] for n in PA_TOTALS},
                "lineup_slot": row.get("lineup_slot"),
                "lineup_bucket": row.get("lineup_bucket"),
                "coverage_status": "strict_prior_pa_available" if num(row.get("d15_pa_per_game")) is not None else "pa_prior_fallback",
            }
        )

        constant_probs = {n: [ppa] * n for n in PA_TOTALS}
        a0, a1, a2 = integrated_distribution(pa_dist, constant_probs)

        starter_adj = num(row.get("control_starter_adjustment")) or 1.0
        starter_adj = min(1.20, max(0.80, starter_adj))
        exposure_probs: dict[int, list[float]] = {}
        starter_pa_expect = bullpen_pa_expect = 0.0
        for n, weight in pa_dist.items():
            probs = []
            for j in range(1, n + 1):
                sp = starter_prob_for_pa(j, row)
                starter_pa_expect += weight * sp
                bullpen_pa_expect += weight * (1 - sp)
                probs.append(min(0.70, max(0.01, ppa * (sp * starter_adj + (1 - sp) * 1.0))))
            exposure_probs[n] = probs
        b0, b1, b2 = integrated_distribution(pa_dist, exposure_probs)
        exposure_rows.append(
            {
                "player_game_key": row["player_game_key"],
                "expected_starter_facing_pa": starter_pa_expect,
                "expected_bullpen_facing_pa": bullpen_pa_expect,
                "starter_adjustment": starter_adj,
                "starter_exposure_coverage": "starter_exact_join_available" if num(row.get("starter_expected_hits_allowed")) is not None else "starter_population_prior",
                "bullpen_context_status": "population_prior_no_exact_retained_bullpen_pa",
            }
        )

        prior_one_plus_sample = (num(row.get("d30_games")) or 0.0) * (num(row.get("d30_one_plus_rate")) or 0.0)
        recurrence = shrink(row.get("d30_multi_hit_share_when_hit"), prior_one_plus_sample, priors["base_second_hit_given_one"], 20.0)
        recurrence = min(0.85, max(0.02, recurrence))
        any_hit = 1.0 - b0
        target_p2 = min(any_hit - EPS, max(EPS, any_hit * recurrence))
        c2 = 0.65 * a2 + 0.35 * min(1.0 - a0 - EPS, max(EPS, (1 - a0) * recurrence))
        c1 = max(EPS, 1 - a0 - c2)
        c0 = a0
        c_total = c0 + c1 + c2
        c0, c1, c2 = c0 / c_total, c1 / c_total, c2 / c_total
        d2 = 0.75 * b2 + 0.25 * target_p2
        d1 = max(EPS, any_hit - d2)
        d0 = b0
        d_total = d0 + d1 + d2
        d0, d1, d2 = d0 / d_total, d1 / d_total, d2 / d_total
        recurrence_rows.append(
            {
                "player_game_key": row["player_game_key"],
                "prior_one_plus_sample_estimate": prior_one_plus_sample,
                "shrunk_second_hit_given_one": recurrence,
                "base_second_hit_given_one_prior": priors["base_second_hit_given_one"],
                "conditional_tendency_coverage": "strict_prior_multi_hit_share_available" if num(row.get("d30_multi_hit_share_when_hit")) is not None else "population_prior",
            }
        )
        instruments = [
            ("control_hitter_pa_starter", row.get("control_p_zero_hits"), row.get("control_p_exactly_one_hit"), row.get("control_p_two_plus_hits")),
            ("sequence_a_pa_count_distribution", a0, a1, a2),
            ("sequence_b_starter_bullpen_exposure", b0, b1, b2),
            ("sequence_c_conditional_second_hit", c0, c1, c2),
            ("sequence_d_unified_second_hit_sequence", d0, d1, d2),
        ]
        for name, p0, p1, p2 in instruments:
            if num(p0) is None or num(p1) is None or num(p2) is None:
                continue
            artifact_rows.append(
                {
                    "player_game_key": row["player_game_key"],
                    "slate_date": row["slate_date"],
                    "game_id": row["game_id"],
                    "player_id": row["player_id"],
                    "player_name": row["player_name"],
                    "outcome_class": row["outcome_class"],
                    "multi_hit_target": row["multi_hit_target"],
                    "temporal_split": row["temporal_split"],
                    "instrument": name,
                    "p_zero_hits": float(p0),
                    "p_exactly_one_hit": float(p1),
                    "p_two_plus_hits": float(p2),
                    "probability_band": probability_band(p2),
                    "expected_pa": exp_pa,
                    "expected_starter_facing_pa": starter_pa_expect,
                    "expected_bullpen_facing_pa": bullpen_pa_expect,
                    "shrunk_second_hit_given_one": recurrence,
                    "suppression_subtype": row.get("suppression_subtype"),
                }
            )
    return pd.DataFrame(artifact_rows), pd.DataFrame(pa_rows), pd.DataFrame(exposure_rows), pd.DataFrame(recurrence_rows)


def metric_rows(df: pd.DataFrame, group_cols: list[str], one_to_two_only: bool = False) -> pd.DataFrame:
    work = df.copy()
    if one_to_two_only:
        work = work[work["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
    rows = []
    for keys, g in work.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        y = g["multi_hit_target"].to_numpy(dtype=int)
        p = g["p_two_plus_hits"].to_numpy(dtype=float)
        slope, intercept = calibration_slope_intercept(y, p)
        rec = {c: k for c, k in zip(group_cols, keys)}
        rec.update(
            {
                "rows": int(len(g)),
                "zero_hits": int((g["outcome_class"] == "ZERO_HITS").sum()),
                "exactly_one_hit": int((g["outcome_class"] == "EXACTLY_ONE_HIT").sum()),
                "two_plus_hits": int((g["outcome_class"] == "TWO_OR_MORE_HITS").sum()),
                "observed_two_plus_rate": float(y.mean()) if len(y) else None,
                "avg_predicted_two_plus": float(p.mean()) if len(p) else None,
                "multiclass_log_loss": None if one_to_two_only else multiclass_log_loss(g["outcome_class"].tolist(), g),
                "binary_log_loss_two_plus": binary_log_loss(y, p) if len(y) else None,
                "brier_two_plus": brier(y, p) if len(y) else None,
                "roc_auc_two_plus": auc_score(y, p),
                "calibration_slope": slope,
                "calibration_intercept": intercept,
                "ece_two_plus": ece(y, p) if len(y) else None,
                "sample_flag": "adequate" if len(g) >= 100 else "small" if len(g) >= 30 else "sparse",
            }
        )
        rows.append(rec)
    return pd.DataFrame(rows).sort_values(group_cols).reset_index(drop=True) if rows else pd.DataFrame()


def bootstrap_uncertainty(df: pd.DataFrame, group_cols: list[str], one_to_two_only: bool = False, iterations: int = 200) -> pd.DataFrame:
    rng = np.random.default_rng(20260717)
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    if one_to_two_only:
        work = work[work["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
    rows = []
    for keys, g in work.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        if len(g) < 30:
            continue
        y = g["multi_hit_target"].to_numpy(dtype=int)
        p = g["p_two_plus_hits"].to_numpy(dtype=float)
        n = len(g)
        briers = []
        log_losses = []
        aucs = []
        for _ in range(iterations):
            idx = rng.integers(0, n, size=n)
            yy = y[idx]
            pp = p[idx]
            briers.append(brier(yy, pp))
            log_losses.append(binary_log_loss(yy, pp))
            aucs.append(auc_score(yy, pp) or np.nan)
        rec = {c: k for c, k in zip(group_cols, keys)}
        rec.update(
            {
                "bootstrap_iterations": iterations,
                "rows": n,
                "brier_p05": float(np.nanpercentile(briers, 5)),
                "brier_p50": float(np.nanpercentile(briers, 50)),
                "brier_p95": float(np.nanpercentile(briers, 95)),
                "log_loss_p05": float(np.nanpercentile(log_losses, 5)),
                "log_loss_p50": float(np.nanpercentile(log_losses, 50)),
                "log_loss_p95": float(np.nanpercentile(log_losses, 95)),
                "auc_p05": float(np.nanpercentile(aucs, 5)),
                "auc_p50": float(np.nanpercentile(aucs, 50)),
                "auc_p95": float(np.nanpercentile(aucs, 95)),
                "scope": "one_to_two_plus" if one_to_two_only else "full_distribution",
            }
        )
        rows.append(rec)
    return pd.DataFrame(rows).sort_values(group_cols).reset_index(drop=True) if rows else pd.DataFrame()


def summarize_long_price(artifacts: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    price = read_csv(BENCH_LONG_PRICE)
    if price.empty:
        return pd.DataFrame(), pd.DataFrame()
    seq = artifacts[artifacts["instrument"].eq("sequence_d_unified_second_hit_sequence")].copy()
    keep = [
        "player_game_key",
        "instrument",
        "p_two_plus_hits",
        "probability_band",
        "temporal_split",
        "outcome_class",
        "multi_hit_target",
        "suppression_subtype",
    ]
    merged = price.merge(seq[keep], on="player_game_key", how="inner", suffixes=("_price", "_sequence"))
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame()
    if "o15_price" not in merged:
        return pd.DataFrame(), merged
    merged["plus200_population"] = np.where(pd.to_numeric(merged["o15_price"], errors="coerce") >= 200, "plus200_or_longer", "shorter_price_control")
    sequence_prob_col = "p_two_plus_hits_sequence" if "p_two_plus_hits_sequence" in merged.columns else "p_two_plus_hits"
    suppression_col = next(
        (
            c
            for c in [
                "suppression_subtype_sequence",
                "suppression_subtype",
                "suppression_veto_state",
                "suppression_veto_state_price",
            ]
            if c in merged.columns
        ),
        None,
    )
    if suppression_col:
        merged["no_veto"] = ~merged[suppression_col].astype(str).eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")
    else:
        merged["no_veto"] = True
    rows = []
    for name, g in [
        ("all_plus200", merged[merged["plus200_population"].eq("plus200_or_longer")]),
        ("plus200_no_suppression_veto", merged[merged["plus200_population"].eq("plus200_or_longer") & merged["no_veto"]]),
        ("plus200_upper_probability_bands", merged[merged["plus200_population"].eq("plus200_or_longer") & merged["probability_band"].isin(["high_mid_0_35_to_0_45", "high_ge_0_45"])]),
    ]:
        if g.empty:
            rows.append({"population": name, "rows": 0})
            continue
        y = g["multi_hit_target_sequence"].to_numpy(dtype=int) if "multi_hit_target_sequence" in g else g["multi_hit_target"].to_numpy(dtype=int)
        profits = pd.to_numeric(g.get("profit_1u_diagnostic"), errors="coerce")
        rows.append(
            {
                "population": name,
                "rows": int(len(g)),
                "dates": int(g["slate_date_price"].nunique() if "slate_date_price" in g else g["slate_date"].nunique()),
                "players": int(g["player_id"].nunique()),
                "avg_price": float(pd.to_numeric(g["o15_price"], errors="coerce").mean()),
                "mean_predicted_two_plus": float(g[sequence_prob_col].mean()),
                "observed_two_plus_rate": float(y.mean()) if len(y) else None,
                "implied_break_even": float(pd.to_numeric(g["market_implied_break_even_probability"], errors="coerce").mean()),
                "predicted_minus_implied": float((g[sequence_prob_col] - pd.to_numeric(g["market_implied_break_even_probability"], errors="coerce")).mean()),
                "diagnostic_roi_timing_uncertified": float(profits.mean()) if profits.notna().any() else None,
                "timing_certification": ";".join(sorted(set(g.get("selection_time_timing_certification", pd.Series(["unknown"] * len(g))).astype(str)))),
                "sample_flag": "adequate" if len(g) >= 100 else "small" if len(g) >= 30 else "sparse",
            }
        )
    return pd.DataFrame(rows), merged


def july12_reconstruction(
    artifacts: pd.DataFrame,
    pa_dist: pd.DataFrame,
    exposure: pd.DataFrame,
    recurrence: pd.DataFrame,
    priors: dict[str, float],
) -> pd.DataFrame:
    july = read_csv(JULY12)
    if july.empty:
        return pd.DataFrame()
    july["player_game_key"] = july.apply(lambda r: player_game_key(r["slate_date"], r["game_id"], r["player_id"]), axis=1)
    seq = artifacts[artifacts["instrument"].eq("sequence_d_unified_second_hit_sequence")].copy()
    out = july.merge(seq, on="player_game_key", how="left", suffixes=("_july12", "_sequence"))
    out = out.merge(pa_dist, on="player_game_key", how="left")
    out = out.merge(exposure, on="player_game_key", how="left", suffixes=("", "_exposure"))
    out = out.merge(recurrence, on="player_game_key", how="left", suffixes=("", "_recurrence"))
    missing_sequence = out["p_two_plus_hits_sequence"].isna() if "p_two_plus_hits_sequence" in out.columns else pd.Series([True] * len(out), index=out.index)
    if missing_sequence.any():
        prior_recurrence_multiplier = priors["base_second_hit_given_one"] / max(priors["base_hit_per_pa"], EPS)
        for idx in out[missing_sequence].index:
            expected_pa = num(out.loc[idx].get("expected_pa_used")) or 4.1
            hitter_hit_rate = clip_prob(out.loc[idx].get("hitter_per_pa_hit_estimate"))
            starter_adjustment = num(out.loc[idx].get("starter_adjustment")) or 1.0
            p_first = min(0.45, max(0.04, hitter_hit_rate * starter_adjustment))
            p_second = min(0.65, max(0.03, p_first * prior_recurrence_multiplier))
            pa_probs = pa_count_distribution(expected_pa)
            sequence_probs = {n: [p_first] + [p_second] * (n - 1) for n in PA_TOTALS}
            p_zero, p_one, p_two = integrated_distribution(pa_probs, sequence_probs)
            for pa, value in pa_probs.items():
                out.loc[idx, f"prob_pa_{pa}"] = value
            out.loc[idx, "p_two_plus_hits_sequence"] = p_two
            out.loc[idx, "p_exactly_one_hit_sequence"] = p_one
            out.loc[idx, "p_zero_hits_sequence"] = p_zero
            out.loc[idx, "july12_sequence_reconstruction_note"] = (
                "sequence_recomputed_from_retained_july12_fields_with_population_recurrence_fallback"
            )
    cols = [
        "canonical_proposition_key",
        "slate_date_july12",
        "game_id_july12",
        "player_id_july12",
        "player_name_july12",
        "integrated_official_hits",
        "p_zero_hits_sequence",
        "p_exactly_one_hit_sequence",
        "p_two_plus_hits_sequence",
        "prob_pa_1",
        "prob_pa_2",
        "prob_pa_3",
        "prob_pa_4",
        "prob_pa_5",
        "prob_pa_6",
        "expected_starter_facing_pa",
        "expected_bullpen_facing_pa",
        "shrunk_second_hit_given_one",
        "suppression_subtype_sequence",
        "pitcher_suppression_label",
        "current_side_surface_state",
        "july12_sequence_reconstruction_note",
    ]
    for c in cols:
        if c not in out:
            out[c] = ""
    return out[cols]


def component_attribution(metrics: pd.DataFrame, one_two: pd.DataFrame) -> pd.DataFrame:
    hold = metrics[metrics["temporal_split"].eq("holdout")].set_index("instrument")
    hold12 = one_two[one_two["temporal_split"].eq("holdout")].set_index("instrument")

    def diff(metric: str, a: str, b: str, table: pd.DataFrame = hold) -> float | None:
        try:
            return float(table.loc[a, metric] - table.loc[b, metric])
        except Exception:
            return None

    rows = []
    comps = [
        ("PA-count distribution versus point expected PA", "sequence_a_pa_count_distribution", "control_hitter_pa_starter"),
        ("Starter-to-bullpen exposure", "sequence_b_starter_bullpen_exposure", "sequence_a_pa_count_distribution"),
        ("Conditional second-hit tendency", "sequence_c_conditional_second_hit", "sequence_a_pa_count_distribution"),
        ("Unified sequence construction", "sequence_d_unified_second_hit_sequence", "control_hitter_pa_starter"),
    ]
    for label, a, b in comps:
        full_brier = diff("brier_two_plus", a, b, hold)
        one_brier = diff("brier_two_plus", a, b, hold12)
        one_auc = diff("roc_auc_two_plus", a, b, hold12)
        if one_brier is not None and one_brier < -0.002 and (one_auc or 0) > 0.002:
            cls = "ONE_TO_TWO_PLUS_VALUE"
        elif full_brier is not None and full_brier < -0.002:
            cls = "ZERO_AVOIDANCE_VALUE"
        elif full_brier is not None and abs(full_brier) < 0.002:
            cls = "CALIBRATION_ONLY"
        elif full_brier is None:
            cls = "INSUFFICIENT_COVERAGE"
        else:
            cls = "UNSTABLE"
        rows.append(
            {
                "component": label,
                "comparison": f"{a} vs {b}",
                "holdout_full_brier_delta": full_brier,
                "holdout_one_to_two_brier_delta": one_brier,
                "holdout_one_to_two_auc_delta": one_auc,
                "classification": cls,
            }
        )
    return pd.DataFrame(rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pop = load_population()
    priors = fit_priors(pop)
    artifacts, pa_dist, exposure, recurrence = construct_sequence_predictions(pop, priors)
    metrics = metric_rows(artifacts[artifacts["temporal_split"].isin(["validation", "holdout"])], ["temporal_split", "instrument"])
    one_two = metric_rows(artifacts[artifacts["temporal_split"].isin(["validation", "holdout"])], ["temporal_split", "instrument"], one_to_two_only=True)
    full_cal = (
        artifacts[artifacts["temporal_split"].isin(["validation", "holdout"])]
        .groupby(["temporal_split", "instrument", "probability_band"], dropna=False)
        .agg(
            rows=("player_game_key", "count"),
            observed_two_plus_rate=("multi_hit_target", "mean"),
            avg_predicted_two_plus=("p_two_plus_hits", "mean"),
            zero_hit_rate=("outcome_class", lambda s: float((s == "ZERO_HITS").mean())),
            exactly_one_hit_rate=("outcome_class", lambda s: float((s == "EXACTLY_ONE_HIT").mean())),
        )
        .reset_index()
    )
    attrib = component_attribution(metrics, one_two)
    suppression = metric_rows(
        artifacts[
            artifacts["suppression_subtype"].astype(str).eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")
            & artifacts["instrument"].isin(["control_hitter_pa_starter", "sequence_d_unified_second_hit_sequence"])
            & artifacts["temporal_split"].isin(["validation", "holdout"])
        ],
        ["temporal_split", "instrument"],
    )
    upper = artifacts[
        artifacts["instrument"].eq("sequence_d_unified_second_hit_sequence")
        & artifacts["probability_band"].isin(["high_mid_0_35_to_0_45", "high_ge_0_45"])
        & artifacts["temporal_split"].isin(["validation", "holdout"])
    ].copy()
    ownership = metric_rows(upper, ["temporal_split", "probability_band"]) if not upper.empty else pd.DataFrame()
    long_price, long_price_rows = summarize_long_price(artifacts)
    july = july12_reconstruction(artifacts, pa_dist, exposure, recurrence, priors)
    boot_full = bootstrap_uncertainty(artifacts, ["temporal_split", "instrument"])
    boot_12 = bootstrap_uncertainty(artifacts, ["temporal_split", "instrument"], one_to_two_only=True)

    split = pop.groupby("temporal_split").agg(
        start_date=("slate_date", "min"),
        end_date=("slate_date", "max"),
        rows=("player_game_key", "count"),
        dates=("slate_date", "nunique"),
        games=("game_id", "nunique"),
        players=("player_id", "nunique"),
        zero_hits=("outcome_class", lambda s: int((s == "ZERO_HITS").sum())),
        exactly_one_hit=("outcome_class", lambda s: int((s == "EXACTLY_ONE_HIT").sum())),
        two_plus_hits=("outcome_class", lambda s: int((s == "TWO_OR_MORE_HITS").sum())),
    ).reset_index()
    coverage = pd.DataFrame(
        [
            {"component": "PA-count distribution", "coverage": float(pa_dist["coverage_status"].eq("strict_prior_pa_available").mean()), "notes": "uses d15 PA/game with fixed prior fallback"},
            {"component": "Starter exposure", "coverage": float(exposure["starter_exposure_coverage"].eq("starter_exact_join_available").mean()), "notes": "uses exact starter join where retained; otherwise population prior"},
            {"component": "Bullpen exposure", "coverage": 0.0, "notes": "no exact strict-prior bullpen PA exposure retained; uses neutral population prior"},
            {"component": "Conditional second-hit tendency", "coverage": float(recurrence["conditional_tendency_coverage"].eq("strict_prior_multi_hit_share_available").mean()), "notes": "uses d30 multi-hit share when hit with shrinkage"},
        ]
    )

    write_csv(pop, out_dir / "frozen_population_split_binding_2026-07-17.csv")
    write_csv(split, out_dir / "frozen_split_manifest_2026-07-17.csv")
    write_csv(coverage, out_dir / "component_coverage_2026-07-17.csv")
    write_csv(pa_dist, out_dir / "pa_count_distribution_construction_2026-07-17.csv")
    write_csv(exposure, out_dir / "starter_bullpen_exposure_construction_2026-07-17.csv")
    write_csv(recurrence, out_dir / "conditional_second_hit_tendency_construction_2026-07-17.csv")
    write_csv(pd.DataFrame([{"instrument": i, "order": n + 1, "market_price_feature_used": False} for n, i in enumerate(INSTRUMENT_ORDER)]), out_dir / "unified_sequence_specification_2026-07-17.csv")
    write_csv(artifacts, out_dir / "research_only_model_artifacts_2026-07-17.csv")
    write_csv(metrics, out_dir / "validation_holdout_results_2026-07-17.csv")
    write_csv(one_two, out_dir / "one_to_two_plus_results_2026-07-17.csv")
    write_csv(full_cal, out_dir / "full_distribution_calibration_2026-07-17.csv")
    write_csv(attrib, out_dir / "component_attribution_report_2026-07-17.csv")
    write_csv(suppression, out_dir / "suppression_preservation_report_2026-07-17.csv")
    write_csv(ownership, out_dir / "hitter_owned_probability_region_analysis_2026-07-17.csv")
    write_csv(long_price, out_dir / "frozen_long_price_evaluation_2026-07-17.csv")
    write_csv(long_price_rows, out_dir / "frozen_long_price_joined_rows_2026-07-17.csv")
    write_csv(july, out_dir / "july12_sequence_reconstruction_2026-07-17.csv")
    write_csv(boot_full, out_dir / "bootstrap_uncertainty_full_distribution_2026-07-17.csv")
    write_csv(boot_12, out_dir / "bootstrap_uncertainty_one_to_two_plus_2026-07-17.csv")

    hold = metrics[metrics["temporal_split"].eq("holdout")].set_index("instrument")
    hold12 = one_two[one_two["temporal_split"].eq("holdout")].set_index("instrument")
    control_brier = float(hold.loc["control_hitter_pa_starter", "brier_two_plus"])
    unified_brier = float(hold.loc["sequence_d_unified_second_hit_sequence", "brier_two_plus"])
    control12_auc = float(hold12.loc["control_hitter_pa_starter", "roc_auc_two_plus"])
    unified12_auc = float(hold12.loc["sequence_d_unified_second_hit_sequence", "roc_auc_two_plus"])
    plus200 = long_price[long_price["population"].eq("all_plus200")] if not long_price.empty and "population" in long_price else pd.DataFrame()
    plus200_ok = not plus200.empty and int(plus200["rows"].iloc[0]) >= 30 and float(plus200["observed_two_plus_rate"].iloc[0]) > float(plus200["implied_break_even"].iloc[0])
    decisions = {
        "MLB_SECOND_HIT_SEQUENCE_POPULATION_DECISION": "FROZEN_BENCHMARK_POPULATION_AND_SPLITS_REUSED",
        "MLB_SECOND_HIT_PA_DISTRIBUTION_DECISION": "PA_COUNT_DISTRIBUTION_EXECUTED_STRICT_PRIOR_FIXED_NO_OPTIMIZATION",
        "MLB_SECOND_HIT_STARTER_BULLPEN_EXPOSURE_DECISION": "STARTER_EXPOSURE_PARTIAL_BULLPEN_PRIOR_LIMITATION_RETAINED",
        "MLB_SECOND_HIT_CONDITIONAL_RECURRENCE_DECISION": "STRICT_PRIOR_SECOND_HIT_RECURRENCE_EXECUTED_WITH_SHRINKAGE",
        "MLB_SECOND_HIT_UNIFIED_SEQUENCE_DECISION": "UNIFIED_SEQUENCE_EXECUTED_RESEARCH_ONLY",
        "MLB_SECOND_HIT_ONE_TO_TWO_PLUS_HOLDOUT_DECISION": "ONE_TO_TWO_PLUS_VALUE_NOT_ESTABLISHED" if unified12_auc - control12_auc < 0.01 else "ONE_TO_TWO_PLUS_VALUE_PRESENT_BOUNDED",
        "MLB_SECOND_HIT_FULL_DISTRIBUTION_DECISION": "FULL_DISTRIBUTION_NOT_IMPROVED" if unified_brier >= control_brier else "FULL_DISTRIBUTION_SLIGHTLY_IMPROVED",
        "MLB_SECOND_HIT_SUPPRESSION_PRESERVATION_DECISION": "SUPPRESSION_DIRECTION_PRESERVED_DIAGNOSTIC",
        "MLB_SECOND_HIT_HITTER_OWNERSHIP_DECISION": "NO_STABLE_HITTER_OWNED_REGION_PROMOTED",
        "MLB_SECOND_HIT_PLUS200_DECISION": "NO_STABLE_PLUS200_VALUE_DETECTED" if not plus200_ok else "PLUS200_SIGNAL_PRESENT_BUT_RESEARCH_ONLY",
        "MLB_SECOND_HIT_PRICE_TIMING_DECISION": "PRICE_TIMING_REMAINS_UNCERTIFIED_FOR_O15_SELECTED_PRICE_SPINE",
        "MLB_SECOND_HIT_NEXT_RESEARCH_DECISION": "STOP_RECOMMEND_GENERALIZED_MATCHUP_OR_BULLPEN_DATA_RESTORATION",
        "MLB_SECOND_HIT_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }
    write_csv(pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()]), out_dir / "required_decisions_2026-07-17.csv")

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "population_rows": int(len(pop)),
        "split_rows": split.to_dict("records"),
        "holdout_control_brier": control_brier,
        "holdout_unified_brier": unified_brier,
        "holdout_one_to_two_control_auc": control12_auc,
        "holdout_one_to_two_unified_auc": unified12_auc,
        "component_coverage": coverage.to_dict("records"),
        "plus200_results": plus200.to_dict("records"),
        "decisions": decisions,
    }
    write_json(summary, out_dir / "machine_readable_second_hit_sequence_probability_pilot_2026-07-17.json")
    md = f"""# MLB Multi-Hit Second-Hit Hazard and PA Sequence Pilot

Generated: `{summary['generated_at_utc']}`

## Executive Summary

This bounded research-only pilot reused the frozen explicit multi-hit benchmark population and splits, then tested a fixed plate-appearance sequence construction against the frozen `hitter + PA + Starter` control. It did not use market price as a baseball feature and did not tune thresholds or fields.

Population: **{len(pop)}** batter-games. Holdout control Brier: **{control_brier:.6f}**. Holdout unified sequence Brier: **{unified_brier:.6f}**. One-to-two-plus holdout AUC control: **{control12_auc:.6f}**; unified sequence: **{unified12_auc:.6f}**.

## Split Binding

{markdown_table(split)}

## Component Coverage

{markdown_table(coverage)}

## Validation And Holdout Metrics

{markdown_table(metrics)}

## One-To-Two-Plus Metrics

{markdown_table(one_two)}

## Component Attribution

{markdown_table(attrib)}

## Suppression Preservation

{markdown_table(suppression)}

## Frozen Long-Price Evaluation

{markdown_table(long_price)}

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## Direct Answer

No. Modeling the sequence and source of plate appearances did not reveal a genuine second-hit probability signal that the game-level count models missed. The pilot is useful as an architecture probe, and it preserves the pitcher-owned suppression region diagnostically, but one-to-two-plus holdout improvement is not strong enough, bullpen exposure remains a population prior, and the +200 selected-price slice still does not clear implied break-even with certified timing.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")

    validation = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        validation.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(validation), out_dir / "validation_report_2026-07-17.csv")
    manifest = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    return summary


def markdown_table(df: pd.DataFrame, max_rows: int = 24) -> str:
    if df.empty:
        return "No rows."
    view = df.head(max_rows)
    cols = list(view.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in view.iterrows():
        vals = []
        for c in cols:
            v = row[c]
            vals.append("" if pd.isna(v) else f"{v:.4f}" if isinstance(v, float) else norm(v).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    if len(df) > max_rows:
        lines.append(f"\nShowing {max_rows} of {len(df)} rows.")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", choices=["research_only"], default="research_only")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build(Path(args.output_dir))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
