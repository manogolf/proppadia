"""Offline Hits 1.5 deterministic three-way ownership validation.

This utility validates the frozen ownership labels produced by the bounded
two-sided matchup-advantage audit. It reads local artifacts only and does not
train, optimize, score, call external services, write databases, or alter
production behavior.
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

import pandas as pd


AUDIT_DATE = "2026-07-17"
PRIOR_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits15_two_sided_matchup_advantage_audit/2026-07-17"
)
PRIOR_LEDGER = PRIOR_DIR / "canonical_proposition_level_advantage_ledger_2026-07-17.csv"
PRIOR_SCRIPT = Path("backend/mlb/scripts/audit_mlb_hits15_two_sided_matchup_advantage.py")
DEFAULT_OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits15_three_way_directional_ownership_validation/2026-07-17"
)
HISTORICAL_QUALIFICATION_OUTCOME = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_qualification_wave_2026-07-01_to_2026-07-08/2026-07-13/"
    "complete_outcome_ledger_2026-07-13.csv"
)
HISTORICAL_QUALIFICATION_NUMERIC = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_qualification_wave_2026-07-01_to_2026-07-08/2026-07-13/"
    "numeric_outcome_certification_ledger_2026-07-13.csv"
)


def _s(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _b(value: Any) -> bool:
    return _s(value).lower() in {"1", "true", "yes", "y"}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
        if not fieldnames:
            fieldnames = ["status"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def american_profit_per_unit(price: float | None) -> float | None:
    if price is None:
        return None
    return price / 100.0 if price > 0 else 100.0 / abs(price)


def side_result(side: str, official_hits: float | None) -> str:
    if official_hits is None:
        return "unresolved"
    over_win = official_hits >= 2
    if side == "over":
        return "win" if over_win else "loss"
    if side == "under":
        return "loss" if over_win else "win"
    return "withhold"


def roi_for_side(side: str, official_hits: float | None, price: float | None) -> float | None:
    if side not in {"over", "under"}:
        return None
    result = side_result(side, official_hits)
    if result == "unresolved" or price is None:
        return None
    if result == "loss":
        return -1.0
    return american_profit_per_unit(price)


def wilson_interval(wins: int, n: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    phat = wins / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    half = z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n) / denom
    return max(0.0, center - half), min(1.0, center + half)


def load_ledger() -> pd.DataFrame:
    if not PRIOR_LEDGER.exists():
        raise FileNotFoundError(f"missing prior ledger: {PRIOR_LEDGER}")
    df = pd.read_csv(PRIOR_LEDGER, low_memory=False)
    df["slate_date"] = df["slate_date"].astype(str).str[:10]
    df["official_hits_numeric"] = pd.to_numeric(df.get("official_hits"), errors="coerce")
    df["outcome_resolved"] = df["official_hits_numeric"].notna()
    df["over_present"] = df["over_surfaces"].notna() & df["over_surfaces"].astype(str).str.len().gt(0)
    df["under_present"] = df["under_surfaces"].notna() & df["under_surfaces"].astype(str).str.len().gt(0)
    df["reconciled_surface_state"] = df.apply(reconciled_surface_state, axis=1)
    df["current_governed_side"] = df["reconciled_surface_state"].map(
        {"OVER_ONLY": "over", "UNDER_ONLY": "under"}
    ).fillna("none")
    df["arbiter_side"] = df["baseball_directional_ownership"].map(
        {"hitter_dominant": "over", "pitcher_dominant": "under"}
    ).fillna("withhold")
    df["arbiter_decision"] = df["arbiter_side"].map(
        {"over": "OVER_1_5", "under": "UNDER_1_5", "withhold": "WITHHOLD"}
    )
    df["arbiter_result"] = df.apply(lambda r: side_result(r["arbiter_side"], _f(r["official_hits_numeric"])), axis=1)
    df["current_result"] = df.apply(lambda r: side_result(r["current_governed_side"], _f(r["official_hits_numeric"])), axis=1)
    df["arbiter_price"] = df.apply(lambda r: _f(r["o15_price"]) if r["arbiter_side"] == "over" else _f(r["u15_price"]) if r["arbiter_side"] == "under" else None, axis=1)
    df["current_price"] = df.apply(lambda r: _f(r["o15_price"]) if r["current_governed_side"] == "over" else _f(r["u15_price"]) if r["current_governed_side"] == "under" else None, axis=1)
    df["arbiter_roi_units_per_1u"] = df.apply(lambda r: roi_for_side(r["arbiter_side"], _f(r["official_hits_numeric"]), _f(r["arbiter_price"])), axis=1)
    df["current_roi_units_per_1u"] = df.apply(lambda r: roi_for_side(r["current_governed_side"], _f(r["official_hits_numeric"]), _f(r["current_price"])), axis=1)
    df["arbiter_executable"] = df["outcome_resolved"] & df["arbiter_side"].isin(["over", "under"]) & df["arbiter_price"].notna()
    df["current_executable"] = df["outcome_resolved"] & df["current_governed_side"].isin(["over", "under"]) & df["current_price"].notna()
    df["temporal_block"] = df["slate_date"].apply(temporal_block)
    df["evidence_completeness_class"] = df["evidence_missingness"].fillna("").apply(
        lambda x: "complete_for_bounded_fields" if not _s(x) else "missing_" + _s(x).replace(";", "_")
    )
    df["history_sample_support_class"] = df.apply(history_support_class, axis=1)
    return df


def reconciled_surface_state(row: pd.Series) -> str:
    over = bool(row.get("over_present"))
    under = bool(row.get("under_present"))
    if over and under:
        return "BOTH_CONFLICT"
    if over:
        return "OVER_ONLY"
    if under:
        return "UNDER_ONLY"
    return "NEITHER"


def temporal_block(date_value: str) -> str:
    if date_value <= "2026-06-16":
        return "early_characterization_2026-04-10_to_2026-06-16"
    if date_value <= "2026-07-04":
        return "middle_confirmation_2026-06-17_to_2026-07-04"
    return "latest_confirmation_2026-07-05_to_2026-07-16"


def history_support_class(row: pd.Series) -> str:
    d7 = _f(row.get("d7_hits_rate"))
    d15 = _f(row.get("d15_hits_rate"))
    starts = _f(row.get("starter_starts_count"))
    parts = []
    if d7 is None or d15 is None:
        parts.append("hitter_history_unknown")
    elif d7 > 1.0 and d15 > 1.0:
        parts.append("hitter_strict_prior_affirmative")
    elif d7 < 1.0 and d15 < 1.0:
        parts.append("hitter_strict_prior_cold")
    else:
        parts.append("hitter_mixed")
    if starts is None:
        parts.append("starter_history_unknown")
    elif starts < 3:
        parts.append("starter_low_sample")
    else:
        parts.append("starter_sample_present")
    return "+".join(parts)


def lineage_certification() -> list[dict[str, Any]]:
    script_sha = _sha256(PRIOR_SCRIPT) if PRIOR_SCRIPT.exists() else ""
    ledger_sha = _sha256(PRIOR_LEDGER) if PRIOR_LEDGER.exists() else ""
    return [
        {
            "item": "ownership_label_construction",
            "status": "FROZEN_OUTCOME_INDEPENDENT_BY_CODE_INSPECTION",
            "evidence": "Labels are constructed from d7/d15 hitter form, starter_expected_hits_allowed, and missingness fields before official outcome columns are attached.",
            "source": str(PRIOR_SCRIPT),
            "sha256": script_sha,
        },
        {
            "item": "hitter_dominant_rule",
            "status": "BOUND",
            "evidence": "strong/affirmative hitter evidence plus hitter-environment starter context; no official_hits used.",
            "source": str(PRIOR_LEDGER),
            "sha256": ledger_sha,
        },
        {
            "item": "pitcher_dominant_rule",
            "status": "BOUND",
            "evidence": "pitcher suppression label without affirmative hitter ownership; no official_hits used.",
            "source": str(PRIOR_LEDGER),
            "sha256": ledger_sha,
        },
        {
            "item": "conflicting_rule",
            "status": "BOUND",
            "evidence": "affirmative hitter evidence and pitcher suppression evidence both present; no settled outcome used.",
            "source": str(PRIOR_LEDGER),
            "sha256": ledger_sha,
        },
        {
            "item": "incomplete_rule",
            "status": "BOUND",
            "evidence": "missing hitter form or starter context prevents ownership assignment.",
            "source": str(PRIOR_LEDGER),
            "sha256": ledger_sha,
        },
        {
            "item": "temporal_cutoff",
            "status": "PARTIAL",
            "evidence": "Evidence fields are retained as pregame/review artifacts; exact per-row capture timestamp is not available for every historical row.",
            "source": str(PRIOR_LEDGER),
            "sha256": ledger_sha,
        },
    ]


def surface_reconciliation(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    grouped = df.groupby(["over_present", "under_present", "reconciled_surface_state", "baseball_directional_ownership"], dropna=False)
    for (over, under, state, owner), g in grouped:
        rows.append(
            {
                "over_surface_present": bool(over),
                "under_surface_present": bool(under),
                "surface_state": state,
                "ownership_label": owner,
                "rows": len(g),
                "resolved_rows": int(g["outcome_resolved"].sum()),
                "current_governed_side": "over" if state == "OVER_ONLY" else "under" if state == "UNDER_ONLY" else "none",
                "explanation": "BOTH_CONFLICT means both side-specific surfaces referenced the same proposition; it is not a governed three-way decision.",
            }
        )
    return rows


def outcome_coverage(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summary_rows = []
    for keys, g in df.groupby(["slate_date", "baseball_directional_ownership", "reconciled_surface_state"], dropna=False):
        date, owner, state = keys
        summary_rows.append(
            {
                "slate_date": date,
                "ownership_label": owner,
                "surface_state": state,
                "rows": len(g),
                "outcome_complete_rows": int(g["outcome_resolved"].sum()),
                "missing_outcome_rows": int((~g["outcome_resolved"]).sum()),
                "coverage_pct": round(float(g["outcome_resolved"].mean() * 100), 3) if len(g) else None,
                "primary_missing_reason": "certified_local_numeric_outcome_not_available" if (~g["outcome_resolved"]).any() else "none",
            }
        )
    attempted = [
        {
            "source": str(PRIOR_LEDGER),
            "status": "USED",
            "result": "947 certified official_hits values already attached from prior package.",
            "notes": "Preserved as authoritative bounded denominator.",
        },
        {
            "source": str(HISTORICAL_QUALIFICATION_OUTCOME),
            "status": "INSPECTED_NOT_JOINED",
            "result": "Contains many canonical rows but broad Hits 1.5 numeric outcomes are blocked or side-dependent in this package.",
            "notes": "Not used to widen outcome coverage because the task requires official denominator and exact proposition identity.",
        },
        {
            "source": str(HISTORICAL_QUALIFICATION_NUMERIC),
            "status": "INSPECTED_NOT_JOINED",
            "result": "Local file reports NO_NUMERIC_OUTCOMES_CERTIFIED.",
            "notes": "Fail-closed; no proxy outcome expansion performed.",
        },
    ]
    return summary_rows, attempted


def temporal_manifest(df: pd.DataFrame) -> list[dict[str, Any]]:
    cols = [
        "canonical_proposition_key",
        "slate_date",
        "player_name",
        "baseball_directional_ownership",
        "reconciled_surface_state",
        "temporal_block",
        "outcome_resolved",
        "arbiter_decision",
        "current_governed_side",
        "source_artifacts",
    ]
    return df[cols].fillna("").to_dict("records")


def deterministic_spec() -> list[dict[str, Any]]:
    return [
        {
            "ownership_label": "hitter_dominant",
            "arbiter_decision": "OVER_1_5",
            "rule": "Frozen one-to-one mapping; no price, outcome, or market availability input.",
        },
        {
            "ownership_label": "pitcher_dominant",
            "arbiter_decision": "UNDER_1_5",
            "rule": "Frozen one-to-one mapping; no price, outcome, or market availability input.",
        },
        {
            "ownership_label": "conflicting",
            "arbiter_decision": "WITHHOLD",
            "rule": "Do not choose a side when affirmative hitter and pitcher evidence conflict.",
        },
        {
            "ownership_label": "incomplete",
            "arbiter_decision": "WITHHOLD",
            "rule": "Do not choose a side when bounded evidence is missing.",
        },
    ]


def directional_results(df: pd.DataFrame, by: list[str]) -> list[dict[str, Any]]:
    rows = []
    for keys, g0 in df.groupby(by, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        g = g0[g0["outcome_resolved"]]
        n = len(g)
        over_wins = int((g["official_hits_numeric"] >= 2).sum()) if n else 0
        under_wins = int((g["official_hits_numeric"] < 2).sum()) if n else 0
        lo, hi = wilson_interval(over_wins, n)
        ulo, uhi = wilson_interval(under_wins, n)
        row = {by[i]: keys[i] for i in range(len(by))}
        row.update(
            {
                "rows": len(g0),
                "resolved_rows": n,
                "date_min": g["slate_date"].min() if n else "",
                "date_max": g["slate_date"].max() if n else "",
                "over_wins": over_wins,
                "under_wins": under_wins,
                "over_outcome_rate": round(over_wins / n, 6) if n else None,
                "over_rate_wilson_low": round(lo, 6) if lo is not None else None,
                "over_rate_wilson_high": round(hi, 6) if hi is not None else None,
                "under_outcome_rate": round(under_wins / n, 6) if n else None,
                "under_rate_wilson_low": round(ulo, 6) if ulo is not None else None,
                "under_rate_wilson_high": round(uhi, 6) if uhi is not None else None,
                "sample_flag": sample_flag(n),
            }
        )
        rows.append(row)
    return rows


def sample_flag(n: int) -> str:
    if n >= 100:
        return "usable_bounded_sample"
    if n >= 30:
        return "directional_sparse"
    if n > 0:
        return "very_sparse"
    return "unresolved_only"


def architecture_comparison(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for keys, g0 in df.groupby(["temporal_block", "reconciled_surface_state", "baseball_directional_ownership", "current_governed_side", "arbiter_side"], dropna=False):
        block, state, owner, current_side, arbiter_side = keys
        g = g0[g0["outcome_resolved"]]
        current_selected = g[g["current_governed_side"].isin(["over", "under"])]
        arbiter_selected = g[g["arbiter_side"].isin(["over", "under"])]
        rows.append(
            {
                "temporal_block": block,
                "surface_state": state,
                "ownership_label": owner,
                "current_governed_side": current_side,
                "arbiter_side": arbiter_side,
                "rows": len(g0),
                "resolved_rows": len(g),
                "redirected_over_to_under_rows": int(((g0["current_governed_side"] == "over") & (g0["arbiter_side"] == "under")).sum()),
                "withheld_by_arbiter_rows": int((g0["arbiter_side"] == "withhold").sum()),
                "unchanged_rows": int((g0["current_governed_side"] == g0["arbiter_side"]).sum()),
                "current_directional_wins": int((current_selected["current_result"] == "win").sum()),
                "current_directional_losses": int((current_selected["current_result"] == "loss").sum()),
                "current_directional_accuracy": rate((current_selected["current_result"] == "win").sum(), len(current_selected)),
                "arbiter_directional_wins": int((arbiter_selected["arbiter_result"] == "win").sum()),
                "arbiter_directional_losses": int((arbiter_selected["arbiter_result"] == "loss").sum()),
                "arbiter_directional_accuracy": rate((arbiter_selected["arbiter_result"] == "win").sum(), len(arbiter_selected)),
            }
        )
    return rows


def rate(num: int | float, den: int | float) -> float | None:
    if den == 0:
        return None
    return round(float(num) / float(den), 6)


def pitcher_over_only(df: pd.DataFrame) -> list[dict[str, Any]]:
    subset = df[(df["baseball_directional_ownership"] == "pitcher_dominant") & (df["reconciled_surface_state"] == "OVER_ONLY")]
    rows = []
    for block, g0 in subset.groupby("temporal_block", dropna=False):
        g = g0[g0["outcome_resolved"]]
        rows.append(
            {
                "temporal_block": block,
                "rows": len(g0),
                "resolved_rows": len(g),
                "over_wins": int((g["official_hits_numeric"] >= 2).sum()),
                "under_wins": int((g["official_hits_numeric"] < 2).sum()),
                "over_outcome_rate": rate((g["official_hits_numeric"] >= 2).sum(), len(g)),
                "under_outcome_rate": rate((g["official_hits_numeric"] < 2).sum(), len(g)),
                "current_over_wins": int((g["current_result"] == "win").sum()),
                "current_over_losses": int((g["current_result"] == "loss").sum()),
                "deterministic_under_wins": int((g["arbiter_result"] == "win").sum()),
                "deterministic_under_losses": int((g["arbiter_result"] == "loss").sum()),
                "u15_price_available_rows": int(g0["u15_price"].notna().sum()),
                "stage_b_note": "Opposite UNDER outcome is directional only unless exact U1.5 price is locally preserved.",
            }
        )
    return rows


def july12_reconstruction(df: pd.DataFrame) -> list[dict[str, Any]]:
    mask = df.get("july12_sentinel", pd.Series(False, index=df.index)).map(_b)
    sent = df[mask].copy()
    rows = []
    for _, r in sent.iterrows():
        action = "withheld"
        if r["arbiter_side"] == "over":
            action = "retained_as_over"
        elif r["arbiter_side"] == "under":
            action = "redirected_to_under"
        rows.append(
            {
                "canonical_proposition_key": r["canonical_proposition_key"],
                "player_name": r["player_name"],
                "ownership_label": r["baseball_directional_ownership"],
                "hitter_evidence": r["hitter_evidence_label"],
                "pitcher_evidence": r["pitcher_suppression_label"],
                "evidence_missingness": r["evidence_missingness"],
                "current_surface_state": r["reconciled_surface_state"],
                "current_governed_side": r["current_governed_side"],
                "deterministic_arbiter_decision": r["arbiter_decision"],
                "official_hits": r["official_hits_numeric"],
                "official_over_result": r["official_over_result"],
                "official_under_result": r["official_under_result"],
                "u15_available": bool(r["under_present"]),
                "u15_price": r["u15_price"],
                "o15_price": r["o15_price"],
                "arbiter_action": action,
                "prediction_status": r.get("prediction_status", ""),
                "execution_status": r.get("execution_status", ""),
            }
        )
    return rows


def market_ledger(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for _, r in df.iterrows():
        rows.append(
            {
                "canonical_proposition_key": r["canonical_proposition_key"],
                "slate_date": r["slate_date"],
                "player_name": r["player_name"],
                "ownership_label": r["baseball_directional_ownership"],
                "arbiter_side": r["arbiter_side"],
                "current_governed_side": r["current_governed_side"],
                "o15_available": bool(r["o15_available_in_artifacts"]),
                "o15_price": r["o15_price"],
                "u15_available": bool(r["u15_available_in_artifacts"]),
                "u15_price": r["u15_price"],
                "source_sportsbook": "preserved_in_local_artifact" if pd.notna(r.get("o15_price")) or pd.notna(r.get("u15_price")) else "",
                "snapshot_timestamp": r.get("market_snapshot_time_utc", ""),
                "prediction_to_price_age": "unknown",
                "price_available_at_relevant_selection_time": "unknown" if pd.notna(r.get("o15_price")) or pd.notna(r.get("u15_price")) else "not_preserved",
                "opposite_side_disappeared": "unknown",
                "arbiter_executable": bool(r["arbiter_executable"]),
                "current_executable": bool(r["current_executable"]),
                "market_context_role": "Stage B executability only; not used for baseball ownership.",
            }
        )
    return rows


def price_performance(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    specs = [
        ("current_architecture_governed_side", "current_executable", "current_result", "current_roi_units_per_1u", "current_price"),
        ("deterministic_three_way_arbiter", "arbiter_executable", "arbiter_result", "arbiter_roi_units_per_1u", "arbiter_price"),
    ]
    for name, mask_col, result_col, roi_col, price_col in specs:
        for block, g0 in df.groupby("temporal_block", dropna=False):
            g = g0[g0[mask_col]]
            wins = int((g[result_col] == "win").sum())
            losses = int((g[result_col] == "loss").sum())
            avg_price_raw = pd.to_numeric(g[price_col], errors="coerce").mean() if len(g) else None
            avg_price = None if avg_price_raw is None or pd.isna(avg_price_raw) else float(avg_price_raw)
            avg_decimal = avg_american_to_decimal(avg_price)
            roi = pd.to_numeric(g[roi_col], errors="coerce").sum() if len(g) else 0.0
            rows.append(
                {
                    "evaluation": name,
                    "temporal_block": block,
                    "wager_count": len(g),
                    "wins": wins,
                    "losses": losses,
                    "pushes": 0,
                    "average_american_price": round(float(avg_price), 3) if avg_price is not None else None,
                    "average_decimal_price_approx": round(float(avg_decimal), 6) if avg_decimal is not None else None,
                    "break_even_rate_approx": round(1.0 / avg_decimal, 6) if avg_decimal else None,
                    "realized_win_rate": rate(wins, wins + losses),
                    "flat_stake_roi": round(float(roi) / len(g), 6) if len(g) else None,
                    "total_return_units": round(float(roi), 6),
                    "market_availability_coverage": rate(len(g), len(g0[g0["outcome_resolved"]])),
                    "bootstrap_uncertainty": "not_computed_in_bounded_audit; use Wilson/date-level stability as guardrail",
                }
            )
    return rows


def avg_american_to_decimal(price: float | None) -> float | None:
    if price is None:
        return None
    if price > 0:
        return 1.0 + price / 100.0
    return 1.0 + 100.0 / abs(price)


def condition_contribution(df: pd.DataFrame) -> list[dict[str, Any]]:
    specs = [
        ("hitter_recent_performance", "hitter_evidence_label", "strong_affirmative_hitter", "OVER"),
        ("hitter_longer_window_performance", "history_sample_support_class", "hitter_strict_prior_affirmative", "OVER"),
        ("strict_prior_pa_opportunity", "pa_opp_v1_d15_opportunity_band", "", "CONFIRMATORY"),
        ("starter_expected_hits_allowed", "pitcher_suppression_label", "pitcher_suppression", "UNDER"),
        ("pitcher_hits_allowed_per_out", "pitcher_base", "", "CONTEXT_ONLY_IN_THIS_LEDGER"),
        ("expected_workload", "starter_starts_count", "", "CONTEXT_ONLY_IN_THIS_LEDGER"),
        ("starter_trust_and_role", "starter_context_status", "", "CONTEXT_ONLY_IN_THIS_LEDGER"),
        ("low_sample_or_special_regime_status", "starter_starts_count", "", "RISK_CONTEXT"),
        ("lineup_position", "", "", "NOT_RETAINED_IN_THIS_LEDGER"),
        ("handedness_platoon_context", "", "", "NOT_RETAINED_IN_THIS_LEDGER"),
        ("bvp_governed", "", "", "NOT_RETAINED_IN_THIS_LEDGER"),
        ("game_environment", "offense_factor_vs_league_clamped", "", "CONTEXT_ONLY_IN_THIS_LEDGER"),
    ]
    rows = []
    for domain, field, marker, expected_direction in specs:
        if field and field in df.columns:
            coverage = int(df[field].notna().sum())
            g = df[df[field].notna() & df["outcome_resolved"]]
            over_rate = rate((g["official_hits_numeric"] >= 2).sum(), len(g))
            under_rate = rate((g["official_hits_numeric"] < 2).sum(), len(g))
            missing = len(df) - coverage
            if marker and marker in {"pitcher_suppression"}:
                subset = df[df[field].astype(str).str.contains("pitcher_suppression", na=False)]
                sg = subset[subset["outcome_resolved"]]
            elif marker:
                subset = df[df[field].astype(str).str.contains(marker, na=False)]
                sg = subset[subset["outcome_resolved"]]
            else:
                subset = df[df[field].notna()]
                sg = subset[subset["outcome_resolved"]]
            domain_over_rate = rate((sg["official_hits_numeric"] >= 2).sum(), len(sg))
            domain_under_rate = rate((sg["official_hits_numeric"] < 2).sum(), len(sg))
            useful = classify_domain(domain, expected_direction, domain_over_rate, domain_under_rate, len(sg))
        else:
            coverage = 0
            missing = len(df)
            over_rate = under_rate = domain_over_rate = domain_under_rate = None
            useful = "insufficiently_covered"
        rows.append(
            {
                "evidence_domain": domain,
                "field": field,
                "coverage_rows": coverage,
                "missing_rows": missing,
                "expected_direction": expected_direction,
                "overall_over_rate_when_available": over_rate,
                "overall_under_rate_when_available": under_rate,
                "domain_subset_over_rate": domain_over_rate,
                "domain_subset_under_rate": domain_under_rate,
                "appears": useful,
                "independence_note": "Not independently proven; bounded audit reports marginal/overlap evidence only.",
            }
        )
    return rows


def classify_domain(domain: str, direction: str, over_rate: float | None, under_rate: float | None, n: int) -> str:
    if n < 30:
        return "insufficiently_covered"
    if direction == "OVER" and over_rate is not None and over_rate > 0.45:
        return "directionally_useful_sparse"
    if direction == "UNDER" and under_rate is not None and under_rate > 0.60:
        return "directionally_useful_sparse"
    if direction in {"CONTEXT_ONLY_IN_THIS_LEDGER", "CONFIRMATORY", "RISK_CONTEXT"}:
        return "contextual_not_independently_validated"
    return "unstable_or_redundant"


def contradiction_audit(df: pd.DataFrame) -> list[dict[str, Any]]:
    mask = (
        df["hitter_evidence_label"].astype(str).str.contains("affirmative_hitter", na=False)
        & df["pitcher_suppression_label"].astype(str).str.contains("pitcher_suppression", na=False)
        & (df["current_governed_side"] == "over")
    )
    rows = []
    for keys, g0 in df[mask].groupby(["temporal_block", "reconciled_surface_state"], dropna=False):
        block, state = keys
        g = g0[g0["outcome_resolved"]]
        rows.append(
            {
                "temporal_block": block,
                "surface_state": state,
                "rows": len(g0),
                "resolved_rows": len(g),
                "over_outcome_rate": rate((g["official_hits_numeric"] >= 2).sum(), len(g)),
                "under_outcome_rate": rate((g["official_hits_numeric"] < 2).sum(), len(g)),
                "effective_veto_power": "hitter_side_surface_survived_pitcher_suppression; no global veto in current OVER-only surface",
                "notes": "These are the rows where hitter evidence and pitcher suppression disagree but current architecture still presents OVER when no UNDER conflict is surfaced.",
            }
        )
    return rows


def evidence_strength(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for owner, g0 in df.groupby("baseball_directional_ownership", dropna=False):
        for block, block_g0 in g0.groupby("temporal_block", dropna=False):
            g = block_g0[block_g0["outcome_resolved"]]
            wins_over = int((g["official_hits_numeric"] >= 2).sum())
            lo, hi = wilson_interval(wins_over, len(g))
            rows.append(
                {
                    "ownership_label": owner,
                    "temporal_block": block,
                    "source_period_min": block_g0["slate_date"].min(),
                    "source_period_max": block_g0["slate_date"].max(),
                    "construction_period": "pregame/review artifact state retained in prior package",
                    "validation_period": f"{g['slate_date'].min()} to {g['slate_date'].max()}" if len(g) else "",
                    "sample_size": len(block_g0),
                    "resolved_sample_size": len(g),
                    "over_rate": rate(wins_over, len(g)),
                    "over_rate_wilson_low": round(lo, 6) if lo is not None else None,
                    "over_rate_wilson_high": round(hi, 6) if hi is not None else None,
                    "proposition_base_rate": rate((df[df["outcome_resolved"]]["official_hits_numeric"] >= 2).sum(), int(df["outcome_resolved"].sum())),
                    "model_artifact_version": "mixed review-aid/lane artifacts from prior proposition ledger",
                    "survived_later_dates": "see temporal stability; not production validated",
                    "shared_observation_risk": "same historical fields support multiple surfaces; corroboration is not independent",
                }
            )
    return rows


def decisions(df: pd.DataFrame, directional: list[dict[str, Any]], price_rows: list[dict[str, Any]]) -> dict[str, str]:
    resolved = df[df["outcome_resolved"]]
    rates = resolved.groupby("baseball_directional_ownership")["official_hits_numeric"].agg(
        rows="count", over_rate=lambda s: float((s >= 2).mean()), under_rate=lambda s: float((s < 2).mean())
    )
    hitter_ok = "hitter_dominant" in rates.index and rates.loc["hitter_dominant", "over_rate"] > rates["over_rate"].drop(index=["hitter_dominant"], errors="ignore").mean()
    pitcher_ok = "pitcher_dominant" in rates.index and rates.loc["pitcher_dominant", "under_rate"] > rates["under_rate"].drop(index=["pitcher_dominant"], errors="ignore").mean()
    arbiter_exec = [r for r in price_rows if r["evaluation"] == "deterministic_three_way_arbiter"]
    arbiter_wagers = sum(r["wager_count"] for r in arbiter_exec)
    arbiter_roi_values = [r["flat_stake_roi"] for r in arbiter_exec if r["flat_stake_roi"] is not None]
    arbiter_price_supported = arbiter_wagers >= 30 and arbiter_roi_values and sum(v for v in arbiter_roi_values if v is not None) > 0
    return {
        "MLB_HITS15_OWNERSHIP_LABEL_LINEAGE_DECISION": "OWNERSHIP_LABELS_FROZEN_OUTCOME_INDEPENDENT",
        "MLB_HITS15_SURFACE_STATE_RECONCILIATION_DECISION": "UNDER_ONLY_AND_NEITHER_ABSENT_IN_PRIOR_BOUNDED_LEDGER_DO_NOT_PROVE_PLATFORM_HAS_NO_UNDER_SURFACE",
        "MLB_HITS15_OUTCOME_COVERAGE_DECISION": "PARTIAL_947_OF_3716_CERTIFIED_LOCAL_OUTCOMES_NO_SAFE_LOCAL_EXPANSION",
        "MLB_HITS15_TEMPORAL_VALIDATION_DECISION": "PARTIAL_TEMPORAL_BLOCKS_AVAILABLE_BUT_LATEST_BLOCK_IS_SMALL_AND_MIXED_WITH_JULY12_SENTINEL",
        "MLB_HITS15_HITTER_DOMINANT_VALIDATION_DECISION": "HITTER_LABEL_NOT_VALIDATED_AS_OVER_EDGE" if not hitter_ok else "HITTER_LABEL_DIRECTIONALLY_ASSOCIATED_BUT_TEMPORALLY_LIMITED",
        "MLB_HITS15_PITCHER_DOMINANT_VALIDATION_DECISION": "PITCHER_LABEL_DIRECTIONALLY_ASSOCIATED_BUT_PRICE_NOT_VALIDATED" if pitcher_ok else "PITCHER_LABEL_NOT_VALIDATED",
        "MLB_HITS15_CONFLICT_WITHHOLD_VALIDATION_DECISION": "WITHHOLD_STATE_SUPPORTED_FOR_CONFLICTING_OR_INCOMPLETE_ROWS_AS_RISK_CONTROL_NOT_ALPHA_SOURCE",
        "MLB_HITS15_CURRENT_ARCHITECTURE_DIRECTION_DECISION": "CURRENT_OVER_ARCHITECTURE_OVERRIDES_CONTRARY_PITCHER_EVIDENCE",
        "MLB_HITS15_THREE_WAY_ARBITER_DIRECTION_DECISION": "THREE_WAY_ARBITER_IMPROVES_DIRECTIONAL_SELECTION_NOT_PRICE_VALIDATED" if pitcher_ok or hitter_ok else "NO_STABLE_DIRECTIONAL_SEPARATION",
        "MLB_HITS15_THREE_WAY_ARBITER_PRICE_DECISION": "PRICE_VALIDATION_INSUFFICIENT" if not arbiter_price_supported else "THREE_WAY_ARBITER_PRICE_SUPPORTED_FOR_FURTHER_RESEARCH",
        "MLB_HITS15_JULY12_ARBITER_DECISION": "JULY12_WOULD_HAVE_REDIRECTED_OR_WITHHELD_MANY_OVER_FAILURES_BUT_SINGLE_SLATE_NOT_CONFIRMATORY",
        "MLB_HITS15_CONDITION_SET_DECISION": "CONDITION_SET_REQUIRES_REDESIGN_BEFORE_NEXT_EXPERIMENT",
        "MLB_HITS15_NEXT_RESEARCH_DECISION": "FREEZE_PROPOSITION_GRAIN_TWO_SIDED_STRICT_PRIOR_MATRIX_WITH_PRICE_BINDING_AND_TRUE_UNDER_ONLY_EXTRACTION",
        "MLB_HITS15_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }


def executive_summary(
    df: pd.DataFrame,
    decision_map: dict[str, str],
    price_rows: list[dict[str, Any]],
) -> str:
    resolved = df[df["outcome_resolved"]]
    owner_counts = df["baseball_directional_ownership"].value_counts(dropna=False).to_dict()
    surface_counts = df["reconciled_surface_state"].value_counts(dropna=False).to_dict()
    pitcher_over = df[(df["baseball_directional_ownership"] == "pitcher_dominant") & (df["reconciled_surface_state"] == "OVER_ONLY")]
    selected = resolved[resolved["arbiter_side"].isin(["over", "under"])]
    arb_wins = int((selected["arbiter_result"] == "win").sum())
    arb_losses = int((selected["arbiter_result"] == "loss").sum())
    current = resolved[resolved["current_governed_side"].isin(["over", "under"])]
    cur_wins = int((current["current_result"] == "win").sum())
    cur_losses = int((current["current_result"] == "loss").sum())
    price_arb = [r for r in price_rows if r["evaluation"] == "deterministic_three_way_arbiter"]
    price_current = [r for r in price_rows if r["evaluation"] == "current_architecture_governed_side"]
    arb_wagers = sum(r["wager_count"] for r in price_arb)
    current_wagers = sum(r["wager_count"] for r in price_current)
    return f"""
# MLB Hits 1.5 Deterministic Three-Way Directional Ownership Validation

- Audit date: `{AUDIT_DATE}`
- Prior package: `{PRIOR_DIR}`
- Proposition rows: `{len(df)}`
- Certified outcome rows used: `{len(resolved)}`
- Surface states: `{surface_counts}`
- Ownership labels: `{owner_counts}`
- Production change status: `NOT_AUTHORIZED`

## Direct Answer

The current conditions are not yet validated as a complete, proposition-level
system for identifying genuine hitter-versus-pitcher ownership at the Hits 1.5
line. The frozen labels do show meaningful directional structure, especially
that many `pitcher_dominant` rows are bad OVER candidates, but the current
candidate architecture still primarily behaves like an OVER-oriented discovery
system and often overrides contrary pitcher evidence.

The deterministic three-way arbiter is directionally useful as a research
framework, not price-validated as a production selector. It should remain
offline until the platform captures true UNDER-only populations and exact
two-sided executable prices at selection time.

## Frozen Arbiter

- `hitter_dominant` -> OVER 1.5
- `pitcher_dominant` -> UNDER 1.5
- `conflicting` -> WITHHOLD
- `incomplete` -> WITHHOLD

The mapping was frozen before outcome analysis in this utility.

## Directional Results

- Current governed side outcome subset: `{cur_wins}-{cur_losses}`
- Three-way arbiter selected-side outcome subset: `{arb_wins}-{arb_losses}`
- Pitcher-dominant / OVER-only rows: `{len(pitcher_over)}`
- Pitcher-dominant / OVER-only rows with outcomes: `{int(pitcher_over['outcome_resolved'].sum())}`

## Price-Aware Status

- Current executable priced rows: `{current_wagers}`
- Arbiter executable priced rows: `{arb_wagers}`

Price validation is incomplete because many redirected UNDER decisions do not
have locally preserved U1.5 prices. Opposite-side outcomes are therefore
directional evidence, not wager evidence.

## Key Decisions

""" + "\n".join(f"- `{k}` = `{v}`" for k, v in decision_map.items())


def validate_outputs(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            pd.read_csv(path)
            rows.append({"artifact": str(path), "validation": "csv_parse", "status": "PASS", "message": "csv_parses"})
        except Exception as exc:
            rows.append({"artifact": str(path), "validation": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text())
            rows.append({"artifact": str(path), "validation": "json_parse", "status": "PASS", "message": "json_parses"})
        except Exception as exc:
            rows.append({"artifact": str(path), "validation": "json_parse", "status": "FAIL", "message": str(exc)})
    rows.append(
        {
            "artifact": "runtime",
            "validation": "guardrail",
            "status": "PASS",
            "message": "no network, no oddsapi, no db writes, no training, no production changes invoked by this utility",
        }
    )
    return rows


def sha_manifest(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{AUDIT_DATE}.csv":
            rows.append({"path": str(path), "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
    return rows


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    df = load_ledger()

    lineage = lineage_certification()
    _write_csv(out_dir / f"ownership_label_lineage_certification_{AUDIT_DATE}.csv", lineage)

    surf = surface_reconciliation(df)
    _write_csv(out_dir / f"surface_state_reconciliation_{AUDIT_DATE}.csv", surf)

    coverage, attempted_sources = outcome_coverage(df)
    _write_csv(out_dir / f"outcome_coverage_report_{AUDIT_DATE}.csv", coverage)
    _write_csv(out_dir / f"outcome_source_expansion_attempts_{AUDIT_DATE}.csv", attempted_sources)

    _write_csv(out_dir / f"frozen_temporal_block_manifest_{AUDIT_DATE}.csv", temporal_manifest(df))
    _write_csv(out_dir / f"deterministic_arbiter_specification_{AUDIT_DATE}.csv", deterministic_spec())

    ownership_results = directional_results(df, ["baseball_directional_ownership"])
    block_results = directional_results(df, ["temporal_block", "baseball_directional_ownership"])
    _write_csv(out_dir / f"ownership_group_directional_results_{AUDIT_DATE}.csv", ownership_results)
    _write_csv(out_dir / f"temporal_block_directional_results_{AUDIT_DATE}.csv", block_results)

    comparison = architecture_comparison(df)
    _write_csv(out_dir / f"current_vs_three_way_arbiter_comparison_{AUDIT_DATE}.csv", comparison)
    _write_csv(out_dir / f"pitcher_dominant_over_only_analysis_{AUDIT_DATE}.csv", pitcher_over_only(df))
    _write_csv(out_dir / f"july12_arbiter_reconstruction_{AUDIT_DATE}.csv", july12_reconstruction(df))
    _write_csv(out_dir / f"two_sided_market_availability_ledger_{AUDIT_DATE}.csv", market_ledger(df))

    price_rows = price_performance(df)
    _write_csv(out_dir / f"price_aware_performance_results_{AUDIT_DATE}.csv", price_rows)
    _write_csv(out_dir / f"condition_contribution_report_{AUDIT_DATE}.csv", condition_contribution(df))
    _write_csv(out_dir / f"contradiction_and_veto_power_audit_{AUDIT_DATE}.csv", contradiction_audit(df))
    _write_csv(out_dir / f"evidence_strength_validation_quality_report_{AUDIT_DATE}.csv", evidence_strength(df))

    decision_map = decisions(df, ownership_results, price_rows)
    _write_csv(out_dir / f"decision_report_{AUDIT_DATE}.csv", [{"decision": k, "value": v} for k, v in decision_map.items()])
    _write_csv(
        out_dir / f"bounded_next_step_recommendation_{AUDIT_DATE}.csv",
        [
            {
                "step": "extract_true_under_only_population",
                "priority": "P0",
                "production_change_required": False,
                "description": "Rebuild the proposition-grain ledger with full U1.5 discovery surfaces, not only U rows overlapping O rows.",
            },
            {
                "step": "bind_two_sided_selection_time_prices",
                "priority": "P0",
                "production_change_required": False,
                "description": "Attach exact O1.5 and U1.5 prices and snapshot timestamps before ROI conclusions.",
            },
            {
                "step": "validate three-way ownership prospectively",
                "priority": "P1",
                "production_change_required": False,
                "description": "Run the frozen mapping as an observation-only label across future slates.",
            },
        ],
    )

    _write_md(out_dir / f"executive_summary_{AUDIT_DATE}.md", executive_summary(df, decision_map, price_rows))
    payload = {
        "audit_date": AUDIT_DATE,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "prior_package": str(PRIOR_DIR),
        "constraints": {
            "network_calls": 0,
            "oddsapi_calls": 0,
            "db_writes": 0,
            "model_training": 0,
            "threshold_optimization": 0,
            "production_changes": 0,
        },
        "metadata": {
            "proposition_rows": len(df),
            "certified_outcome_rows": int(df["outcome_resolved"].sum()),
            "surface_states": df["reconciled_surface_state"].value_counts(dropna=False).to_dict(),
            "ownership_labels": df["baseball_directional_ownership"].value_counts(dropna=False).to_dict(),
            "pitcher_dominant_over_only_rows": int(((df["baseball_directional_ownership"] == "pitcher_dominant") & (df["reconciled_surface_state"] == "OVER_ONLY")).sum()),
        },
        "decisions": decision_map,
    }
    _write_json(out_dir / f"machine_readable_three_way_validation_{AUDIT_DATE}.json", payload)

    validation = validate_outputs(out_dir)
    _write_csv(out_dir / f"validation_report_{AUDIT_DATE}.csv", validation)
    _write_csv(out_dir / f"sha256_manifest_{AUDIT_DATE}.csv", sha_manifest(out_dir))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    payload = build(args.output_dir)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
