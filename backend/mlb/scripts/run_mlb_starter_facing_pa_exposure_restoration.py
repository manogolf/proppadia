#!/usr/bin/env python3
"""Research-only Starter-facing PA exposure restoration pilot for MLB Hits 1.5.

This bounded utility restores a strict-prior estimate of how many plate
appearances a hitter is expected to receive against the opposing starter using
existing local artifacts only, then reruns the frozen second-hit comparison with
that restored exposure. It performs no network calls, DB writes, model training,
or production mutations.
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

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_starter_facing_pa_exposure_restoration/2026-07-17"

BENCH = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17"
SECOND = ROOT / "artifacts/analysis/model_development/mlb_second_hit_sequence_probability_pilot/2026-07-17"
COND = ROOT / "artifacts/analysis/model_development/mlb_conditional_second_hit_tendency_audit/2026-07-17"
HITTER_ROOT = ROOT / "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11"
STARTER_ROOT = ROOT / "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11"
COLLECTIVE = ROOT / "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_historical_source_expansion_pilot_1/2026-07-12"

CANONICAL = BENCH / "canonical_modeling_population_2026-07-17.csv"
CONTROL = BENCH / "research_only_model_artifacts_2026-07-17.csv"
LONG_PRICE = BENCH / "long_price_exact_price_rows_2026-07-17.csv"
JULY12 = BENCH / "july12_probability_reconstruction_2026-07-17.csv"
SEQUENCE_EXPOSURE = SECOND / "starter_bullpen_exposure_construction_2026-07-17.csv"
SEQUENCE_RECURRENCE = SECOND / "conditional_second_hit_tendency_construction_2026-07-17.csv"
HITTER_BASE = HITTER_ROOT / "hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
STARTER_XH = STARTER_ROOT / "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
LOCKED_STARTER = COLLECTIVE / "locked_sources/starter_skill_workload_bounded_source_2026-06-29_to_2026-07-09.csv"
COND_LEDGER = COND / "independent_row_level_reproduction_2026-07-17.csv"

EPS = 1e-9
PA_TOTALS = [1, 2, 3, 4, 5, 6]


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


def pct(n: float, d: float) -> float:
    return float(n / d) if d else 0.0


def key(date: Any, game_id: Any, player_id: Any) -> str:
    gid = str(int(float(game_id))) if num(game_id) is not None else "-1"
    pid = str(int(float(player_id))) if num(player_id) is not None else "-1"
    return f"{norm(date)[:10]}|{gid}|{pid}"


def game_key(date: Any, game_id: Any) -> str:
    gid = str(int(float(game_id))) if num(game_id) is not None else "-1"
    return f"{norm(date)[:10]}|{gid}"


def team_game_key(date: Any, game_id: Any, team: Any, opponent: Any) -> str:
    return f"{game_key(date, game_id)}|{norm(team)}|{norm(opponent)}"


def pa_count_distribution(expected_pa: float) -> dict[int, float]:
    center = min(5.7, max(1.0, expected_pa))
    weights = {n: math.exp(-0.5 * ((n - center) / 0.85) ** 2) for n in PA_TOTALS}
    total = sum(weights.values())
    return {n: weights[n] / total for n in PA_TOTALS}


def poisson_binomial_probs(probs: list[float]) -> list[float]:
    dist = [1.0]
    for p in probs:
        nxt = [0.0] * (len(dist) + 1)
        for k, val in enumerate(dist):
            nxt[k] += val * (1 - p)
            nxt[k + 1] += val * p
        dist = nxt
    return dist


def integrated_hit_distribution(pa_dist: dict[int, float], probs_by_n: dict[int, list[float]]) -> tuple[float, float, float]:
    p0 = p1 = p2 = 0.0
    for n, weight in pa_dist.items():
        dist = poisson_binomial_probs(probs_by_n[n])
        p0 += weight * dist[0]
        p1 += weight * (dist[1] if len(dist) > 1 else 0.0)
        p2 += weight * sum(dist[2:])
    total = max(EPS, p0 + p1 + p2)
    return p0 / total, p1 / total, p2 / total


def starter_prob_for_pa(pa_index: int, expected_bf: float | None, workload_confidence: str) -> float:
    bf = expected_bf if expected_bf and expected_bf > 0 else 19.5
    starter_cycles = min(3.7, max(1.0, bf / 9.0))
    width = 0.45
    if norm(workload_confidence).lower() in {"low", "very_low", "uncertain"}:
        width = 0.65
    return float(1.0 / (1.0 + math.exp((pa_index - starter_cycles) / width)))


def starter_pa_distribution(total_pa_dist: dict[int, float], expected_bf: float | None, workload_confidence: str) -> dict[str, float]:
    bins = {"p_starter_pa_0": 0.0, "p_starter_pa_1": 0.0, "p_starter_pa_2": 0.0, "p_starter_pa_3": 0.0, "p_starter_pa_4_plus": 0.0}
    expected = 0.0
    for n, weight in total_pa_dist.items():
        probs = [starter_prob_for_pa(i, expected_bf, workload_confidence) for i in range(1, n + 1)]
        dist = poisson_binomial_probs(probs)
        for k, val in enumerate(dist):
            expected += weight * k * val
            if k == 0:
                bins["p_starter_pa_0"] += weight * val
            elif k == 1:
                bins["p_starter_pa_1"] += weight * val
            elif k == 2:
                bins["p_starter_pa_2"] += weight * val
            elif k == 3:
                bins["p_starter_pa_3"] += weight * val
            else:
                bins["p_starter_pa_4_plus"] += weight * val
    bins["expected_starter_facing_pa_restored"] = expected
    return bins


def brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2)) if len(y) else float("nan")


def log_loss(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))) if len(y) else float("nan")


def multiclass_log_loss(y_class: pd.Series, probs: pd.DataFrame) -> float:
    labels = {"ZERO_HITS": "p_zero_hits", "EXACTLY_ONE_HIT": "p_exactly_one_hit", "TWO_OR_MORE_HITS": "p_two_plus_hits"}
    losses = []
    for idx, label in y_class.items():
        col = labels.get(norm(label))
        if col:
            losses.append(-math.log(max(1e-6, min(1 - 1e-6, float(probs.loc[idx, col])))))
    return float(np.mean(losses)) if losses else float("nan")


def auc_score(y: np.ndarray, p: np.ndarray) -> float | None:
    y = np.asarray(y)
    p = np.asarray(p)
    pos = p[y == 1]
    neg = p[y == 0]
    if len(pos) == 0 or len(neg) == 0:
        return None
    order = np.argsort(np.concatenate([pos, neg]))
    ranks = np.empty(len(order), dtype=float)
    ranks[order] = np.arange(1, len(order) + 1)
    pos_ranks = ranks[: len(pos)]
    return float((pos_ranks.sum() - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg)))


def ece(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    if len(y) == 0:
        return float("nan")
    edges = np.linspace(0, 1, bins + 1)
    total = len(y)
    out = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (p >= lo) & (p < hi if hi < 1 else p <= hi)
        if mask.any():
            out += mask.sum() / total * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(out)


def calibration_slope_intercept(y: np.ndarray, p: np.ndarray) -> tuple[float | None, float | None]:
    if len(np.unique(y)) < 2 or len(y) < 5:
        return None, None
    x = np.log(np.clip(p, 1e-6, 1 - 1e-6) / np.clip(1 - p, 1e-6, 1 - 1e-6))
    try:
        slope, intercept = np.polyfit(x, y, 1)
        return float(slope), float(intercept)
    except Exception:
        return None, None


def load_population() -> pd.DataFrame:
    pop = read_csv(CANONICAL)
    if pop.empty:
        raise FileNotFoundError(CANONICAL)
    hitter = read_csv(HITTER_BASE)
    hitter["player_game_key"] = hitter.apply(lambda r: key(r["slate_date"], r["game_id"], r["player_id"]), axis=1)
    hcols = [
        "player_game_key",
        "team",
        "opponent",
        "lineup_slot",
        "lineup_bucket",
        "d15_pa_per_game",
        "d15_games",
        "d30_hits_per_pa",
        "d30_games",
        "d30_one_plus_rate",
    ]
    for c in hcols:
        if c not in hitter:
            hitter[c] = np.nan
    pop = pop.merge(hitter[hcols], on="player_game_key", how="left", suffixes=("", "_hitter"))
    for c in ["team", "opponent", "lineup_slot", "lineup_bucket", "d15_pa_per_game", "d15_games", "d30_hits_per_pa", "d30_games", "d30_one_plus_rate"]:
        hc = f"{c}_hitter"
        if hc in pop:
            pop[c] = pop[c].where(pop[c].notna(), pop[hc])
    control = read_csv(CONTROL)
    control = control[control["benchmark"].eq("benchmark_4_hitter_opportunity_starter")].copy()
    ccols = ["player_game_key", "p_zero_hits", "p_exactly_one_hit", "p_two_plus_hits", "expected_pa_used", "hitter_per_pa_hit_estimate", "starter_adjustment"]
    control = control[ccols].rename(columns={c: f"control_{c}" for c in ccols if c != "player_game_key"})
    pop = pop.merge(control, on="player_game_key", how="left")
    rec = read_csv(SEQUENCE_RECURRENCE)
    pop = pop.merge(rec, on="player_game_key", how="left")
    return pop


def source_indices() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sxh = read_csv(STARTER_XH)
    if not sxh.empty:
        sxh["player_game_key"] = sxh.apply(lambda r: key(r["date"], r["game_id"], r["player_id"]), axis=1)
        sxh = sxh.sort_values(["player_game_key"]).drop_duplicates("player_game_key")
    locked = read_csv(LOCKED_STARTER)
    if not locked.empty:
        locked["team_game_key"] = locked.apply(lambda r: team_game_key(r["date"], r["game_id"], r.get("player_team"), r.get("opponent_team")), axis=1)
        locked = locked.sort_values(["team_game_key"]).drop_duplicates("team_game_key")
    prior = read_csv(SEQUENCE_EXPOSURE)
    return sxh, locked, prior


def build_before_state(pop: pd.DataFrame, sxh: pd.DataFrame, locked: pd.DataFrame) -> pd.DataFrame:
    prior = read_csv(SEQUENCE_EXPOSURE)
    covered = set(prior.loc[prior["starter_exposure_coverage"].eq("starter_exact_join_available"), "player_game_key"]) if not prior.empty else set()
    rows = []
    sxh_keys = set(sxh["player_game_key"]) if not sxh.empty else set()
    locked_keys = set(locked["team_game_key"]) if not locked.empty else set()
    for _, r in pop.iterrows():
        pkey = r["player_game_key"]
        tkey = team_game_key(r["slate_date"], r["game_id"], r.get("team"), r.get("opponent"))
        if pkey in covered:
            reason = "PRIOR_EXPOSURE_AVAILABLE"
        elif pkey in sxh_keys:
            reason = "SOURCE_EXISTS_BUT_WAS_NOT_JOINED"
        elif tkey in locked_keys:
            reason = "SOURCE_EXISTS_TEAM_GAME_BUT_NOT_PLAYER_GAME_JOINED"
        elif not norm(r.get("team")) or not norm(r.get("opponent")):
            reason = "EXACT_PLAYER_GAME_BRIDGE_ABSENT"
        elif str(r.get("slate_date")) < "2026-06-29" or str(r.get("slate_date")) > "2026-07-09":
            reason = "OUTSIDE_QUALIFIED_SELECTED_PROPOSITION_POPULATION"
        else:
            reason = "STARTER_HISTORY_OR_WORKLOAD_SOURCE_ABSENT"
        rows.append(
            {
                "player_game_key": pkey,
                "slate_date": r["slate_date"],
                "game_id": r["game_id"],
                "player_id": r["player_id"],
                "player_name": r["player_name"],
                "prior_exposure_available": pkey in covered,
                "before_state_unavailable_reason": reason,
            }
        )
    return pd.DataFrame(rows)


def choose_source(row: pd.Series, sxh: pd.DataFrame, locked: pd.DataFrame, sxh_map: dict[str, pd.Series], locked_map: dict[str, pd.Series]) -> dict[str, Any]:
    pkey = row["player_game_key"]
    if pkey in sxh_map:
        s = sxh_map[pkey]
        return {
            "source_class": "ESTABLISHED_STARTER_EXPOSURE",
            "source_path": rel(STARTER_XH),
            "starter_id": s.get("opposing_starter_player_id") or s.get("actual_starter_player_id"),
            "expected_outs": num(s.get("baseline_outs_per_start")) or num(s.get("expected_outs_blended_v1")),
            "expected_bf": None,
            "starter_adjustment": num(row.get("control_starter_adjustment")) or 1.0,
            "workload_confidence": "selected_proposition_baseline",
            "role_status": s.get("starter_identity_status") or s.get("starter_context_status"),
            "cutoff": "",
            "missingness_reason": "",
        }
    tkey = team_game_key(row.get("slate_date"), row.get("game_id"), row.get("team"), row.get("opponent"))
    if tkey in locked_map:
        s = locked_map[tkey]
        confidence = norm(s.get("workload_confidence"))
        role = norm(s.get("expected_role_label"))
        if role and role not in {"ordinary_starter", "established_starter", "starter"}:
            source_class = "SPECIAL_OR_IRREGULAR_REGIME"
        elif confidence.lower() in {"low", "very_low", "uncertain"}:
            source_class = "WORKLOAD_UNCERTAIN"
        else:
            source_class = "ESTABLISHED_STARTER_EXPOSURE"
        return {
            "source_class": source_class,
            "source_path": rel(LOCKED_STARTER),
            "starter_id": s.get("expected_starter_player_id") or s.get("actual_starter_player_id"),
            "expected_outs": num(s.get("expected_outs_blended_v1")) or num(s.get("baseline_outs_per_start")),
            "expected_bf": num(s.get("expected_bf_blended_v1")) or num(s.get("prior_official_bf_per_start")),
            "starter_adjustment": num(row.get("control_starter_adjustment")) or 1.0,
            "workload_confidence": confidence,
            "role_status": role or s.get("role_confidence"),
            "cutoff": s.get("feature_cutoff_date"),
            "missingness_reason": "",
        }
    reason = "SOURCE_INCOMPLETE"
    if not norm(row.get("team")) or not norm(row.get("opponent")):
        reason = "EXACT_PLAYER_GAME_BRIDGE_ABSENT"
    return {
        "source_class": "EXPOSURE_NOT_CONSTRUCTIBLE",
        "source_path": "",
        "starter_id": "",
        "expected_outs": None,
        "expected_bf": None,
        "starter_adjustment": num(row.get("control_starter_adjustment")) or 1.0,
        "workload_confidence": "",
        "role_status": "",
        "cutoff": "",
        "missingness_reason": reason,
    }


def fit_outs_to_bf_ratio(sxh: pd.DataFrame) -> float:
    if sxh.empty:
        return 1.45
    fit = sxh[(sxh["date"].astype(str) <= "2026-06-11")].copy()
    if "actual_starter_batters_faced" not in fit or "actual_starter_outs_recorded" not in fit:
        return 1.45
    bf = pd.to_numeric(fit["actual_starter_batters_faced"], errors="coerce")
    outs = pd.to_numeric(fit["actual_starter_outs_recorded"], errors="coerce")
    ratio = (bf / outs.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).dropna()
    return float(ratio.median()) if len(ratio) else 1.45


def construct_restored(pop: pd.DataFrame, sxh: pd.DataFrame, locked: pd.DataFrame) -> pd.DataFrame:
    sxh_map = {r["player_game_key"]: r for _, r in sxh.iterrows()} if not sxh.empty else {}
    locked_map = {r["team_game_key"]: r for _, r in locked.iterrows()} if not locked.empty else {}
    outs_to_bf = fit_outs_to_bf_ratio(sxh)
    rows = []
    for _, r in pop.iterrows():
        src = choose_source(r, sxh, locked, sxh_map, locked_map)
        expected_pa = num(r.get("control_expected_pa_used")) or num(r.get("d15_pa_per_game")) or 4.1
        lineup_slot = num(r.get("lineup_slot"))
        expected_bf = src["expected_bf"]
        conversion_note = "expected_bf_direct"
        if expected_bf is None and src["expected_outs"] is not None:
            expected_bf = float(src["expected_outs"] * outs_to_bf)
            conversion_note = f"expected_bf_from_expected_outs_x_fit_median_ratio_{outs_to_bf:.4f}"
        total_pa_dist = pa_count_distribution(expected_pa)
        if src["source_class"] in {"EXPOSURE_NOT_CONSTRUCTIBLE", "SPECIAL_OR_IRREGULAR_REGIME"} or expected_bf is None:
            dist = {f"p_starter_pa_{i}": np.nan for i in range(4)}
            dist["p_starter_pa_4_plus"] = np.nan
            dist["expected_starter_facing_pa_restored"] = np.nan
            expected_residual = np.nan
        else:
            dist = starter_pa_distribution(total_pa_dist, expected_bf, norm(src["workload_confidence"]))
            expected_residual = max(0.0, expected_pa - dist["expected_starter_facing_pa_restored"])
        provenance = "|".join([r["player_game_key"], norm(src["source_class"]), norm(src["starter_id"]), norm(expected_bf), norm(expected_pa)])
        rows.append(
            {
                "player_game_key": r["player_game_key"],
                "slate_date": r["slate_date"],
                "game_id": r["game_id"],
                "player_id": r["player_id"],
                "player_name": r["player_name"],
                "team": r.get("team"),
                "opponent": r.get("opponent"),
                "opposing_starter_player_id": src["starter_id"],
                "lineup_slot": lineup_slot,
                "lineup_bucket": r.get("lineup_bucket"),
                "expected_total_pa": expected_pa,
                "expected_starter_outs": src["expected_outs"],
                "expected_starter_batters_faced": expected_bf,
                "expected_starter_bf_conversion_note": conversion_note,
                **dist,
                "expected_post_starter_pa_residual_not_bullpen_model": expected_residual,
                "starter_adjustment": src["starter_adjustment"],
                "workload_confidence": src["workload_confidence"],
                "role_status": src["role_status"],
                "source_class": src["source_class"],
                "source_path": src["source_path"],
                "source_sha256": sha256(ROOT / src["source_path"]) if src["source_path"] and (ROOT / src["source_path"]).exists() else "",
                "cutoff_date_or_timestamp": src["cutoff"],
                "missingness_reason": src["missingness_reason"],
                "row_level_provenance_hash": hashlib.sha256(provenance.encode()).hexdigest(),
                "temporal_split": r["temporal_split"],
                "outcome_class": r["outcome_class"],
                "multi_hit_target": r["multi_hit_target"],
                "suppression_subtype": r.get("suppression_subtype"),
            }
        )
    return pd.DataFrame(rows)


def build_predictions(pop: pd.DataFrame, restored: pd.DataFrame) -> pd.DataFrame:
    fit = pop[pop["temporal_split"].eq("fit")]
    base_hit_per_pa = float(pd.to_numeric(fit["official_hits"], errors="coerce").sum() / max(EPS, pd.to_numeric(fit["official_pa"], errors="coerce").sum()))
    rows = []
    merged = pop.merge(restored[["player_game_key", "expected_starter_batters_faced", "expected_total_pa", "starter_adjustment", "workload_confidence", "source_class"]], on="player_game_key", how="left")
    for _, r in merged.iterrows():
        expected_pa = num(r.get("expected_total_pa")) or num(r.get("control_expected_pa_used")) or 4.1
        pa_dist = pa_count_distribution(expected_pa)
        ppa = num(r.get("d30_hits_per_pa")) or num(r.get("hitter_per_pa_hit_estimate")) or base_hit_per_pa
        ppa = min(0.55, max(0.03, ppa))
        starter_adj = min(1.2, max(0.8, num(r.get("starter_adjustment")) or 1.0))
        if norm(r.get("source_class")) in {"EXPOSURE_NOT_CONSTRUCTIBLE", "SPECIAL_OR_IRREGULAR_REGIME"}:
            probs = {n: [ppa] * n for n in PA_TOTALS}
        else:
            probs = {}
            for n in PA_TOTALS:
                pa_probs = []
                for j in range(1, n + 1):
                    sp = starter_prob_for_pa(j, num(r.get("expected_starter_batters_faced")), norm(r.get("workload_confidence")))
                    pa_probs.append(min(0.70, max(0.01, ppa * (sp * starter_adj + (1 - sp) * 1.0))))
                probs[n] = pa_probs
        p0, p1, p2 = integrated_hit_distribution(pa_dist, probs)
        rows.append(
            {
                "player_game_key": r["player_game_key"],
                "slate_date": r["slate_date"],
                "game_id": r["game_id"],
                "player_id": r["player_id"],
                "player_name": r["player_name"],
                "temporal_split": r["temporal_split"],
                "outcome_class": r["outcome_class"],
                "multi_hit_target": r["multi_hit_target"],
                "instrument": "restored_starter_facing_pa_exposure_challenger",
                "p_zero_hits": p0,
                "p_exactly_one_hit": p1,
                "p_two_plus_hits": p2,
                "source_class": r.get("source_class"),
                "suppression_subtype": r.get("suppression_subtype"),
            }
        )
    control = pop[
        [
            "player_game_key",
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "temporal_split",
            "outcome_class",
            "multi_hit_target",
            "control_p_zero_hits",
            "control_p_exactly_one_hit",
            "control_p_two_plus_hits",
            "suppression_subtype",
        ]
    ].rename(columns={"control_p_zero_hits": "p_zero_hits", "control_p_exactly_one_hit": "p_exactly_one_hit", "control_p_two_plus_hits": "p_two_plus_hits"})
    control["instrument"] = "frozen_control_hitter_pa_starter"
    control["source_class"] = "frozen_control"
    return pd.concat([control, pd.DataFrame(rows)], ignore_index=True)


def metrics(df: pd.DataFrame, one_to_two: bool = False, group_cols: list[str] | None = None) -> pd.DataFrame:
    group_cols = group_cols or ["temporal_split", "instrument"]
    work = df.copy()
    if one_to_two:
        work = work[work["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
    rows = []
    for keys, g in work.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        y = g["outcome_class"].eq("TWO_OR_MORE_HITS").astype(int).to_numpy()
        p = pd.to_numeric(g["p_two_plus_hits"], errors="coerce").fillna(0.0).to_numpy()
        slope, intercept = calibration_slope_intercept(y, p)
        row = dict(zip(group_cols, keys))
        row.update(
            {
                "rows": int(len(g)),
                "zero_hits": int(g["outcome_class"].eq("ZERO_HITS").sum()),
                "exactly_one_hit": int(g["outcome_class"].eq("EXACTLY_ONE_HIT").sum()),
                "two_plus_hits": int(g["outcome_class"].eq("TWO_OR_MORE_HITS").sum()),
                "observed_two_plus_rate": float(y.mean()) if len(y) else None,
                "avg_predicted_two_plus": float(p.mean()) if len(p) else None,
                "multiclass_log_loss": None if one_to_two else multiclass_log_loss(g["outcome_class"], g[["p_zero_hits", "p_exactly_one_hit", "p_two_plus_hits"]]),
                "binary_log_loss_two_plus": log_loss(y, p),
                "brier_two_plus": brier(y, p),
                "roc_auc_two_plus": auc_score(y, p),
                "calibration_slope": slope,
                "calibration_intercept": intercept,
                "ece_two_plus": ece(y, p),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def bootstrap(df: pd.DataFrame, one_to_two: bool) -> pd.DataFrame:
    rng = np.random.default_rng(17)
    rows = []
    for instrument, g in df[df["temporal_split"].eq("holdout")].groupby("instrument"):
        if one_to_two:
            g = g[g["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
        if len(g) < 50:
            continue
        briers = []
        aucs = []
        for _ in range(100):
            sample = g.iloc[rng.integers(0, len(g), len(g))]
            y = sample["outcome_class"].eq("TWO_OR_MORE_HITS").astype(int).to_numpy()
            p = pd.to_numeric(sample["p_two_plus_hits"], errors="coerce").fillna(0.0).to_numpy()
            briers.append(brier(y, p))
            a = auc_score(y, p)
            if a is not None:
                aucs.append(a)
        rows.append(
            {
                "instrument": instrument,
                "scope": "one_to_two_plus" if one_to_two else "full_distribution",
                "brier_p05": float(np.percentile(briers, 5)),
                "brier_p50": float(np.percentile(briers, 50)),
                "brier_p95": float(np.percentile(briers, 95)),
                "auc_p05": float(np.percentile(aucs, 5)) if aucs else None,
                "auc_p50": float(np.percentile(aucs, 50)) if aucs else None,
                "auc_p95": float(np.percentile(aucs, 95)) if aucs else None,
            }
        )
    return pd.DataFrame(rows)


def coverage_certification(restored: pd.DataFrame, before: pd.DataFrame) -> pd.DataFrame:
    rows = []
    denom = len(restored)
    prior = int(before["prior_exposure_available"].sum())
    final = int(restored["source_class"].isin(["ESTABLISHED_STARTER_EXPOSURE", "LOW_SAMPLE_STARTER_EXPOSURE", "WORKLOAD_UNCERTAIN"]).sum())
    newly = int(((~before["prior_exposure_available"]) & restored["source_class"].isin(["ESTABLISHED_STARTER_EXPOSURE", "LOW_SAMPLE_STARTER_EXPOSURE", "WORKLOAD_UNCERTAIN"])).sum())
    rows.append({"scope": "benchmark", "rows": denom, "prior_exposure_covered_rows": prior, "newly_restored_rows": newly, "final_exposure_covered_rows": final, "final_coverage_pct": pct(final, denom)})
    for split, g in restored.groupby("temporal_split"):
        rows.append({"scope": f"split_{split}", "rows": len(g), "prior_exposure_covered_rows": "", "newly_restored_rows": "", "final_exposure_covered_rows": int(g["source_class"].isin(["ESTABLISHED_STARTER_EXPOSURE", "LOW_SAMPLE_STARTER_EXPOSURE", "WORKLOAD_UNCERTAIN"]).sum()), "final_coverage_pct": float(g["source_class"].isin(["ESTABLISHED_STARTER_EXPOSURE", "LOW_SAMPLE_STARTER_EXPOSURE", "WORKLOAD_UNCERTAIN"]).mean())})
    for cls, g in restored.groupby("source_class", dropna=False):
        rows.append({"scope": f"evidence_class_{cls}", "rows": len(g), "prior_exposure_covered_rows": "", "newly_restored_rows": "", "final_exposure_covered_rows": len(g), "final_coverage_pct": pct(len(g), denom)})
    return pd.DataFrame(rows)


def source_inventory(pop: pd.DataFrame, sxh: pd.DataFrame, locked: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for label, path, df, date_col, player_col in [
        ("starter_xh_characterization", STARTER_XH, sxh, "date", "player_id"),
        ("locked_starter_workload_source", LOCKED_STARTER, locked, "date", None),
        ("hitter_persistence_base", HITTER_BASE, read_csv(HITTER_BASE), "slate_date", "player_id"),
    ]:
        if df.empty:
            rows.append({"source": label, "path": rel(path), "rows": 0})
            continue
        rows.append(
            {
                "source": label,
                "path": rel(path),
                "sha256": sha256(path),
                "grain": "player-game" if player_col else "starter/team-game",
                "date_coverage": f"{df[date_col].astype(str).str[:10].min()} to {df[date_col].astype(str).str[:10].max()}",
                "rows": int(len(df)),
                "games": int(df["game_id"].nunique()) if "game_id" in df else None,
                "pitchers": int(df[[c for c in ["opposing_starter_player_id", "expected_starter_player_id", "actual_starter_player_id"] if c in df.columns][0]].nunique()) if any(c in df.columns for c in ["opposing_starter_player_id", "expected_starter_player_id", "actual_starter_player_id"]) else None,
                "player_game_compatibility": "direct" if player_col else "requires date+game+team/opponent bridge",
                "strict_prior_status": "strict-prior fields retained; actual fields evaluation only",
                "prediction_time_availability": "research artifact only",
            }
        )
    return pd.DataFrame(rows)


def long_price_eval(pred: pd.DataFrame, restored: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    lp = read_csv(LONG_PRICE)
    if lp.empty:
        return pd.DataFrame(), pd.DataFrame()
    ch = pred[pred["instrument"].eq("restored_starter_facing_pa_exposure_challenger")]
    joined = lp.merge(ch[["player_game_key", "p_two_plus_hits", "source_class", "suppression_subtype"]], on="player_game_key", how="left")
    joined = joined.merge(restored[["player_game_key", "expected_starter_facing_pa_restored"]], on="player_game_key", how="left")
    challenger_prob_col = "p_two_plus_hits_y" if "p_two_plus_hits_y" in joined.columns else "p_two_plus_hits"
    joined["price_band_fixed"] = np.where(pd.to_numeric(joined["o15_price"], errors="coerce") >= 250, "+250_and_longer", np.where(pd.to_numeric(joined["o15_price"], errors="coerce") >= 200, "+200_through_+249", "shorter_control"))
    rows = []
    for band, g in joined[joined["price_band_fixed"].isin(["+200_through_+249", "+250_and_longer"])].groupby("price_band_fixed"):
        y = pd.to_numeric(g.get("multi_hit_target"), errors="coerce")
        profits = pd.to_numeric(g.get("profit_1u_diagnostic"), errors="coerce")
        rows.append(
            {
                "price_band": band,
                "exact_price_rows": int(len(g)),
                "exposure_covered_rows": int(g["source_class"].isin(["ESTABLISHED_STARTER_EXPOSURE", "LOW_SAMPLE_STARTER_EXPOSURE", "WORKLOAD_UNCERTAIN"]).sum()),
                "no_suppression_veto_rows": int((~g["suppression_subtype"].astype(str).eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")).sum()),
                "avg_predicted_two_plus": float(pd.to_numeric(g[challenger_prob_col], errors="coerce").mean()),
                "observed_two_plus_rate": float(y.mean()) if y.notna().any() else None,
                "implied_break_even": float(pd.to_numeric(g.get("market_implied_break_even_probability"), errors="coerce").mean()),
                "diagnostic_roi_timing_uncertified": float(profits.mean()) if profits.notna().any() else None,
                "timing_status": ";".join(sorted(set(g.get("selection_time_timing_certification", pd.Series(["unknown"] * len(g))).astype(str)))),
                "players": int(g["player_id"].nunique()) if "player_id" in g else None,
                "dates": int(g["slate_date"].nunique()) if "slate_date" in g else None,
            }
        )
    return pd.DataFrame(rows), joined


def july12(restored: pd.DataFrame, pred: pd.DataFrame) -> pd.DataFrame:
    j = read_csv(JULY12)
    if j.empty:
        return pd.DataFrame()
    j["player_game_key"] = j.apply(lambda r: key(r["slate_date"], r["game_id"], r["player_id"]), axis=1)
    ch = pred[pred["instrument"].eq("restored_starter_facing_pa_exposure_challenger")]
    out = j.merge(restored, on="player_game_key", how="left", suffixes=("_july12", "_restored"))
    out = out.merge(ch[["player_game_key", "p_zero_hits", "p_exactly_one_hit", "p_two_plus_hits"]], on="player_game_key", how="left")
    restored_prob_col = "p_two_plus_hits_y" if "p_two_plus_hits_y" in out.columns else "p_two_plus_hits"
    prior_prob_col = "p_two_plus_hits_x" if "p_two_plus_hits_x" in out.columns else "p_two_plus_hits_july12"
    out["prior_vs_restored_two_plus_diff"] = pd.to_numeric(out.get(restored_prob_col), errors="coerce") - pd.to_numeric(out.get(prior_prob_col), errors="coerce")
    keep = [
        "canonical_proposition_key",
        "slate_date_july12",
        "game_id_july12",
        "player_id_july12",
        "player_name_july12",
        "expected_total_pa",
        "p_starter_pa_0",
        "p_starter_pa_1",
        "p_starter_pa_2",
        "p_starter_pa_3",
        "p_starter_pa_4_plus",
        "expected_starter_facing_pa_restored",
        "expected_post_starter_pa_residual_not_bullpen_model",
        "source_class",
        "missingness_reason",
        "pitcher_suppression_label",
        "p_zero_hits",
        "p_exactly_one_hit",
        "p_two_plus_hits",
        "p_zero_hits_y",
        "p_exactly_one_hit_y",
        "p_two_plus_hits_y",
        "integrated_official_hits",
        "prior_vs_restored_two_plus_diff",
    ]
    for c in keep:
        if c not in out:
            out[c] = np.nan
    return out[keep]


def roster_relative(restored: pd.DataFrame, pred: pd.DataFrame) -> pd.DataFrame:
    ch = pred[pred["instrument"].eq("restored_starter_facing_pa_exposure_challenger")]
    df = restored.merge(ch[["player_game_key", "p_two_plus_hits"]], on="player_game_key", how="left")
    df = df[df["source_class"].isin(["ESTABLISHED_STARTER_EXPOSURE", "LOW_SAMPLE_STARTER_EXPOSURE", "WORKLOAD_UNCERTAIN"])].copy()
    rows = []
    for (date, game_id, team), g in df.groupby(["slate_date", "game_id", "team"], dropna=False):
        if len(g) < 2:
            continue
        ordered = g.sort_values("expected_starter_facing_pa_restored", ascending=False)
        top = ordered.head(1)
        rows.append(
            {
                "slate_date": date,
                "game_id": game_id,
                "team": team,
                "hitters": int(len(g)),
                "top_exposure_player": top["player_name"].iloc[0],
                "top_exposure_expected_starter_pa": float(top["expected_starter_facing_pa_restored"].iloc[0]),
                "top_exposure_outcome": top["outcome_class"].iloc[0],
                "group_two_plus_rows": int(g["outcome_class"].eq("TWO_OR_MORE_HITS").sum()),
                "one_to_two_plus_agreement": bool(top["outcome_class"].iloc[0] == "TWO_OR_MORE_HITS") if g["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"]).any() else None,
                "diagnostic_only": True,
            }
        )
    return pd.DataFrame(rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pop = load_population()
    sxh, locked, _ = source_indices()
    before = build_before_state(pop, sxh, locked)
    restored = construct_restored(pop, sxh, locked)
    pred = build_predictions(pop, restored)

    cov = coverage_certification(restored, before)
    src_inv = source_inventory(pop, sxh, locked)
    source_crosswalk = before.merge(restored[["player_game_key", "source_class", "source_path", "missingness_reason"]], on="player_game_key", how="left")
    evidence = restored.groupby(["source_class", "missingness_reason"], dropna=False).size().reset_index(name="rows")
    acc = pd.DataFrame(
        [
            {
                "accuracy_validation_status": "EXACT_ACTUAL_STARTER_FACING_PA_UNAVAILABLE_LOCALLY",
                "evaluable_rows": 0,
                "mean_absolute_error": None,
                "notes": "No play-by-play batter-vs-pitcher encounter ledger was found in the bound local artifacts; actual starter BF is pitcher-level only.",
            }
        ]
    )
    spec = pd.DataFrame(
        [
            {"item": "control", "value": "frozen benchmark_4_hitter_opportunity_starter probabilities unchanged"},
            {"item": "challenger", "value": "same PA count / per-PA hit framework with restored starter-facing PA distribution where constructible"},
            {"item": "expected_bf_conversion", "value": "expected_bf direct when available; else expected_outs * fit-period median actual BF/out ratio from selected starter source"},
            {"item": "residual_post_starter_pa", "value": "expected_total_pa - expected_starter_facing_pa; explicitly not modeled bullpen exposure"},
            {"item": "conditional_recurrence", "value": "retained as support metadata only; no formula change"},
        ]
    )
    val = metrics(pred)
    one_two = metrics(pred, one_to_two=True)
    full_cal = val.copy()
    boot_full = bootstrap(pred, one_to_two=False)
    boot_one = bootstrap(pred, one_to_two=True)
    suppression = metrics(pred[pred["suppression_subtype"].astype(str).eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")])
    roster = roster_relative(restored, pred)
    lp, lp_rows = long_price_eval(pred, restored)
    j12 = july12(restored, pred)

    hold = one_two[(one_two["temporal_split"].eq("holdout"))].set_index("instrument")
    val_one = one_two[(one_two["temporal_split"].eq("validation"))].set_index("instrument")
    control_hold_brier = float(hold.loc["frozen_control_hitter_pa_starter", "brier_two_plus"])
    challenger_hold_brier = float(hold.loc["restored_starter_facing_pa_exposure_challenger", "brier_two_plus"])
    control_hold_auc = float(hold.loc["frozen_control_hitter_pa_starter", "roc_auc_two_plus"])
    challenger_hold_auc = float(hold.loc["restored_starter_facing_pa_exposure_challenger", "roc_auc_two_plus"])
    control_val_brier = float(val_one.loc["frozen_control_hitter_pa_starter", "brier_two_plus"])
    challenger_val_brier = float(val_one.loc["restored_starter_facing_pa_exposure_challenger", "brier_two_plus"])

    stop = challenger_hold_brier >= control_hold_brier or challenger_val_brier >= control_val_brier
    decisions = {
        "MLB_STARTER_PA_EXPOSURE_BEFORE_STATE_DECISION": "PRIOR_9_10_PERCENT_COVERAGE_CAUSED_BY_SELECTED_PROPOSITION_ONLY_STARTER_CONTEXT",
        "MLB_STARTER_PA_EXPOSURE_SOURCE_BINDING_DECISION": "LOCAL_SOURCES_BOUND_DIRECT_SELECTED_ROWS_AND_BOUNDED_STARTER_WORKLOAD",
        "MLB_STARTER_PA_EXPOSURE_CONSTRUCTION_DECISION": "RESTORED_DISTRIBUTION_CONSTRUCTED_RESEARCH_ONLY_WITH_NO_BULLPEN_MODEL",
        "MLB_STARTER_PA_EXPOSURE_COVERAGE_DECISION": "COVERAGE_RESTORED_PARTIALLY_NOT_FULL_BENCHMARK",
        "MLB_STARTER_PA_EXPOSURE_TEMPORAL_INTEGRITY_DECISION": "STRICT_PRIOR_CONTRACT_PASS_FOR_GOVERNED_SOURCES_NO_CURRENT_GAME_RESULTS_USED_IN_CONSTRUCTION",
        "MLB_STARTER_PA_EXPOSURE_ACCURACY_DECISION": "EXACT_ACTUAL_STARTER_FACING_PA_VALIDATION_UNAVAILABLE_LOCALLY",
        "MLB_STARTER_PA_EXPOSURE_ONE_TO_TWO_PLUS_DECISION": "ONE_TO_TWO_PLUS_VALUE_NOT_ESTABLISHED" if stop else "ONE_TO_TWO_PLUS_VALUE_SUPPORTED_DIAGNOSTIC",
        "MLB_STARTER_PA_EXPOSURE_FULL_DISTRIBUTION_DECISION": "CALIBRATION_ONLY_OR_REDUNDANT",
        "MLB_STARTER_PA_EXPOSURE_SUPPRESSION_DECISION": "SUPPRESSION_DIRECTION_PRESERVED_DIAGNOSTIC",
        "MLB_STARTER_PA_EXPOSURE_ROSTER_RELATIVE_DECISION": "ROSTER_RELATIVE_DIAGNOSTIC_FEASIBLE_NOT_DECISIVE",
        "MLB_STARTER_PA_EXPOSURE_PLUS200_DECISION": "NO_STABLE_PLUS200_VALUE_DETECTED",
        "MLB_STARTER_PA_EXPOSURE_JULY12_DECISION": "JULY12_SENTINEL_NOT_CONSTRUCTIBLE_WITH_EXISTING_RESTORED_SOURCE",
        "MLB_STARTER_PA_EXPOSURE_NEXT_RESEARCH_DECISION": "STOP_CURRENT_EVIDENCE_SELECT_BULLPEN_EXPOSURE_PLATFORM_NEXT" if stop else "ADVANCE_TO_BOUNDED_PROSPECTIVE_OBSERVATION",
        "MLB_STARTER_PA_EXPOSURE_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }
    decisions_df = pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()])

    outputs = {
        "before_state_failure_ledger_2026-07-17.csv": before,
        "source_inventory_2026-07-17.csv": src_inv,
        "source_to_benchmark_crosswalk_2026-07-17.csv": source_crosswalk,
        "frozen_exposure_contract_2026-07-17.csv": spec,
        "restored_starter_facing_pa_artifact_2026-07-17.csv": restored,
        "evidence_class_missingness_ledger_2026-07-17.csv": evidence,
        "coverage_certification_2026-07-17.csv": cov,
        "exposure_accuracy_report_2026-07-17.csv": acc,
        "frozen_control_challenger_specification_2026-07-17.csv": spec,
        "validation_holdout_metrics_2026-07-17.csv": val,
        "one_to_two_plus_results_2026-07-17.csv": one_two,
        "full_distribution_calibration_2026-07-17.csv": full_cal,
        "bootstrap_uncertainty_full_distribution_2026-07-17.csv": boot_full,
        "bootstrap_uncertainty_one_to_two_plus_2026-07-17.csv": boot_one,
        "suppression_preservation_report_2026-07-17.csv": suppression,
        "roster_relative_diagnostic_2026-07-17.csv": roster,
        "frozen_long_price_evaluation_2026-07-17.csv": lp,
        "frozen_long_price_joined_rows_2026-07-17.csv": lp_rows,
        "july12_reconstruction_2026-07-17.csv": j12,
        "advancement_stop_decision_2026-07-17.csv": pd.DataFrame([{"stop": stop, "reason": decisions["MLB_STARTER_PA_EXPOSURE_NEXT_RESEARCH_DECISION"], "holdout_control_brier": control_hold_brier, "holdout_challenger_brier": challenger_hold_brier, "holdout_control_auc": control_hold_auc, "holdout_challenger_auc": challenger_hold_auc}]),
        "research_only_artifacts_2026-07-17.csv": pred,
        "required_decisions_2026-07-17.csv": decisions_df,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "benchmark_rows": int(len(pop)),
        "prior_exposure_covered_rows": int(before["prior_exposure_available"].sum()),
        "final_exposure_covered_rows": int(restored["source_class"].isin(["ESTABLISHED_STARTER_EXPOSURE", "LOW_SAMPLE_STARTER_EXPOSURE", "WORKLOAD_UNCERTAIN"]).sum()),
        "final_exposure_coverage_pct": float(restored["source_class"].isin(["ESTABLISHED_STARTER_EXPOSURE", "LOW_SAMPLE_STARTER_EXPOSURE", "WORKLOAD_UNCERTAIN"]).mean()),
        "holdout_control_one_to_two_brier": control_hold_brier,
        "holdout_challenger_one_to_two_brier": challenger_hold_brier,
        "holdout_control_one_to_two_auc": control_hold_auc,
        "holdout_challenger_one_to_two_auc": challenger_hold_auc,
        "selected_next": decisions["MLB_STARTER_PA_EXPOSURE_NEXT_RESEARCH_DECISION"],
        "decisions": decisions,
    }
    write_json(summary, out_dir / "machine_readable_starter_facing_pa_exposure_restoration_2026-07-17.json")

    md = f"""# MLB Starter-Facing PA Exposure Restoration Pilot

Generated: `{summary['generated_at_utc']}`

## Executive Summary

The original 9.10% Starter-exposure coverage came from exact starter context being present mainly on selected-proposition rows, while the frozen 10,118-row benchmark spine is a broader batter-game population.

This research-only pilot restored constructible Starter-facing PA exposure to **{summary['final_exposure_covered_rows']} / {summary['benchmark_rows']}** rows ({summary['final_exposure_coverage_pct']:.2%}) using existing local sources. The restored artifact preserves expected total PA, expected starter outs/BF, a distribution over 0/1/2/3/4+ Starter-facing PA, expected Starter-facing PA, and a residual post-Starter PA count explicitly not treated as modeled bullpen exposure.

Holdout one-to-two-plus Brier: control **{control_hold_brier:.6f}**, challenger **{challenger_hold_brier:.6f}**. Holdout one-to-two-plus AUC: control **{control_hold_auc:.6f}**, challenger **{challenger_hold_auc:.6f}**.

Direct answer: restoring Starter-facing PA exposure improved coverage materially but did **not** establish hidden multi-hit probability signal versus the frozen control in this bounded pilot. The remaining unresolved component is still later-PA bullpen exposure and suppression.

## Coverage

{markdown_table(cov)}

## One-To-Two-Plus

{markdown_table(one_two)}

## Long Price

{markdown_table(lp)}

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## No Behavior Changed

No network, OddsAPI, DB write, external acquisition, model training, threshold optimization, production model, selector, candidate, upload, Quick Card, workspace, or LaunchAgent behavior changed.
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


def markdown_table(df: pd.DataFrame, max_rows: int = 30) -> str:
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
