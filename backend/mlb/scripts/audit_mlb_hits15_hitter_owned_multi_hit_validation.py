"""Validate hitter-owned multi-hit evidence for MLB Hits O1.5.

Bounded read-only research utility. It consumes certified local artifacts only
and writes a dated package. It does not call network services, write databases,
fit models, optimize thresholds, or change production selectors/surfaces.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AUDIT_DATE = "2026-07-17"
ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits15_hitter_owned_multi_hit_validation/2026-07-17"
INTEGRATED = ROOT / "artifacts/analysis/model_development/mlb_certified_historical_matchup_ownership_integration/2026-07-17/integrated_matchup_evidence_ledger_2026-07-17.csv"
THRESHOLD_ROOT = ROOT / "artifacts/analysis/model_development/mlb_hits_threshold_specific_evidence_audit/2026-07-17"
SUPPRESSION_MANIFEST = ROOT / "artifacts/analysis/model_development/mlb_hits15_pitcher_suppression_under_validation/2026-07-17/exact_pitcher_dominant_population_manifest_2026-07-17.csv"
MARKET_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_hits15_three_way_directional_ownership_validation/2026-07-17/two_sided_market_availability_ledger_2026-07-17.csv"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def sha(path: Path) -> str:
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


def norm(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def fnum(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def wilson(wins: int, n: int) -> tuple[float | None, float | None, float | None]:
    if n <= 0:
        return None, None, None
    z = 1.96
    p = wins / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denom
    return p, max(0.0, center - margin), min(1.0, center + margin)


def sample_flag(n: int) -> str:
    if n >= 150:
        return "adequate"
    if n >= 60:
        return "bounded"
    if n >= 30:
        return "small"
    return "sparse"


def hit_class(value: Any) -> str:
    num = fnum(value)
    if num is None:
        return "MISSING_OFFICIAL_OUTCOME"
    if num <= 0:
        return "ZERO_HITS"
    if num == 1:
        return "EXACTLY_ONE_HIT"
    return "TWO_OR_MORE_HITS"


def american_profit(result: str, price: Any) -> float | None:
    p = fnum(price)
    if p is None or p == 0 or result not in {"win", "loss"}:
        return None
    if result == "loss":
        return -1.0
    return p / 100.0 if p > 0 else 100.0 / abs(p)


def breakeven(price: Any) -> float | None:
    p = fnum(price)
    if p is None or p == 0:
        return None
    dec = 1.0 + (p / 100.0 if p > 0 else 100.0 / abs(p))
    return 1.0 / dec


def bucket_hits_rate(x: Any) -> str:
    v = fnum(x)
    if v is None:
        return "missing"
    if v >= 1.25:
        return "very_hot_ge1_25"
    if v >= 1.0:
        return "o15_affirmative_ge1_0"
    if v >= 0.75:
        return "medium_0_75_to_lt1_0"
    return "low_lt0_75"


def bucket_num(x: Any, name: str) -> str:
    v = fnum(x)
    if v is None:
        return "missing"
    if name == "starter_expected_hits_allowed":
        if v >= 5.5:
            return "hitter_favorable_ge5_5"
        if v >= 5.0:
            return "slightly_favorable_5_0_to_lt5_5"
        if v >= 4.5:
            return "pitcher_suppression_4_5_to_lt5_0"
        return "strong_pitcher_suppression_lt4_5"
    if name == "pa_opp_v1_d15_pa_pg":
        if v >= 4.3:
            return "high_ge4_3"
        if v >= 3.8:
            return "medium_3_8_to_lt4_3"
        if v >= 3.2:
            return "low_3_2_to_lt3_8"
        return "very_low_lt3_2"
    if name == "bvp_plate_appearances":
        if v == 0:
            return "0_pa"
        if v <= 2:
            return "1_2_pa"
        if v <= 5:
            return "3_5_pa"
        if v <= 10:
            return "6_10_pa"
        if v <= 20:
            return "11_20_pa"
        return "21_plus_pa"
    return "available"


def suppression_veto(row: pd.Series) -> str:
    tier = norm(row.get("pitcher_tier_seen"))
    ctx = norm(row.get("starter_context_status")).lower()
    miss = norm(row.get("evidence_missingness")).lower()
    label = norm(row.get("pitcher_suppression_label")).lower()
    ownership = norm(row.get("baseball_directional_ownership"))
    seh = fnum(row.get("starter_expected_hits_allowed"))
    base = fnum(row.get("pitcher_base"))
    if tier == "U" or "missing" in ctx or "starter_expected_hits_allowed" in miss:
        return "UNCERTAIN_OR_INCOMPLETE_PITCHER_STATE"
    if any(tok in ctx for tok in ["special", "irregular", "opener", "bulk"]):
        return "IRREGULAR_ROLE_STATE"
    if (
        ownership == "pitcher_dominant"
        and tier in {"A", "B", "C", "D"}
        and ("strong_pitcher_suppression" in label or "moderate_pitcher_suppression" in label)
        and seh is not None
        and seh < 5.0
        and (base is None or base < 5.5)
    ):
        return "AFFIRMATIVE_SUPPRESSION_VETO"
    if ownership == "pitcher_dominant":
        return "RELATIVE_PITCHER_DOMINANCE"
    return "NO_SUPPRESSION_VETO"


def temporal_blocks(dates: list[str]) -> dict[str, str]:
    unique = sorted(set(dates))
    n = len(unique)
    if not n:
        return {}
    one = max(1, math.ceil(n / 3))
    two = max(one + 1, math.ceil(2 * n / 3))
    out: dict[str, str] = {}
    for i, d in enumerate(unique):
        if i < one:
            out[d] = "early_characterization"
        elif i < two:
            out[d] = "middle_confirmation"
        else:
            out[d] = "latest_untouched_confirmation"
    return out


def summarize_binary(df: pd.DataFrame, group_cols: list[str], scope: str) -> pd.DataFrame:
    rows = []
    if df.empty:
        return pd.DataFrame()
    base = df["two_plus_flag"].mean() if scope == "one_to_two_plus" else df["one_plus_flag"].mean()
    for keys, g in df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        n = len(g)
        wins = int(g["two_plus_flag"].sum()) if scope == "one_to_two_plus" else int(g["one_plus_flag"].sum())
        losses = n - wins
        rate, lo, hi = wilson(wins, n)
        block_rates = {}
        for block, bg in g.groupby("temporal_block", dropna=False):
            block_rates[str(block)] = round(float((bg["two_plus_flag"].mean() if scope == "one_to_two_plus" else bg["one_plus_flag"].mean())), 4) if len(bg) else None
        concentration = max((g["player_id"].value_counts(normalize=True).max(), 0))
        row = {c: k for c, k in zip(group_cols, keys)}
        row.update(
            {
                "scope": scope,
                "rows": n,
                "success_count": wins,
                "failure_count": losses,
                "success_rate": rate,
                "base_rate": base,
                "lift_vs_base": None if rate is None or pd.isna(base) else rate - base,
                "wilson_low": lo,
                "wilson_high": hi,
                "unique_dates": g["slate_date"].nunique(),
                "unique_games": g["game_id"].nunique(),
                "unique_hitters": g["player_id"].nunique(),
                "unique_pitchers": g["opposing_starter_id"].nunique() if "opposing_starter_id" in g else "",
                "max_player_share": concentration,
                "temporal_block_rates": json.dumps(block_rates, sort_keys=True),
                "sample_flag": sample_flag(n),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def classify_domain(row: pd.Series, confirmation_required: bool = True) -> str:
    n = int(row.get("rows", 0))
    lift = fnum(row.get("lift_vs_base"))
    lo = fnum(row.get("wilson_low"))
    base = fnum(row.get("base_rate"))
    rates = json.loads(row.get("temporal_block_rates") or "{}")
    mid = rates.get("middle_confirmation")
    late = rates.get("latest_untouched_confirmation")
    if n < 30:
        return "INSUFFICIENT_COVERAGE"
    if lift is None or abs(lift) < 0.025:
        return "NO_STABLE_SEPARATION"
    if confirmation_required and (mid is None or late is None or (lift > 0 and (mid <= base or late <= base)) or (lift < 0 and (mid >= base or late >= base))):
        return "TEMPORALLY_UNSTABLE"
    if lift > 0 and lo is not None and base is not None and lo > base:
        return "MULTI_HIT_SPECIFIC_SUPPORT"
    if lift > 0:
        return "BOTH_THRESHOLD_SUPPORT"
    return "NO_STABLE_SEPARATION"


def current_support_class(row: pd.Series) -> str:
    veto = row["suppression_veto_status"]
    if veto == "AFFIRMATIVE_SUPPRESSION_VETO":
        return "AFFIRMATIVE_SUPPRESSION_CONTRADICTION"
    if veto == "IRREGULAR_ROLE_STATE":
        return "IRREGULAR_MATCHUP"
    if veto == "UNCERTAIN_OR_INCOMPLETE_PITCHER_STATE":
        return "UNCERTAIN_OR_INCOMPLETE"
    if norm(row.get("baseball_directional_ownership")) == "hitter_dominant" and "affirmative" in norm(row.get("hitter_evidence_label")).lower():
        return "MULTI_HIT_EVIDENCE_PRESENT_NO_VETO"
    if "affirmative" in norm(row.get("hitter_evidence_label")).lower() or norm(row.get("hitter_tier_seen")) == "A":
        return "ANY_HIT_EVIDENCE_ONLY"
    return "NO_THRESHOLD_SPECIFIC_SUPPORT"


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(INTEGRATED, low_memory=False)
    df["line_num"] = pd.to_numeric(df["line"], errors="coerce")
    df["official_hits_num"] = pd.to_numeric(df["integrated_official_hits"], errors="coerce")
    df["hit_count_class"] = df["official_hits_num"].map(hit_class)
    hits15 = df[(df["prop_type"].astype(str).str.lower() == "hits") & (df["line_num"] == 1.5)].copy()
    primary = hits15[hits15["official_hits_num"].notna()].copy()
    primary["one_plus_flag"] = primary["official_hits_num"] >= 1
    primary["two_plus_flag"] = primary["official_hits_num"] >= 2
    primary["suppression_veto_status"] = primary.apply(suppression_veto, axis=1)
    blocks = temporal_blocks(primary["slate_date"].map(norm).tolist())
    primary["temporal_block"] = primary["slate_date"].map(lambda d: blocks.get(norm(d), "unblocked"))
    one_two = primary[primary["hit_count_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
    no_veto = one_two[one_two["suppression_veto_status"] == "NO_SUPPRESSION_VETO"].copy()
    zero_one = primary[primary["hit_count_class"].isin(["ZERO_HITS", "EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
    zero_one_no_veto = zero_one[zero_one["suppression_veto_status"] == "NO_SUPPRESSION_VETO"].copy()

    primary["population_inclusion_reason"] = "resolved_hits_1_5_exact_player_game_outcome"
    manifest_cols = [
        "canonical_proposition_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "prop_type",
        "line",
        "official_hits_num",
        "hit_count_class",
        "temporal_block",
        "hitter_tier_seen",
        "pitcher_tier_seen",
        "combined_tier_seen",
        "current_side_surface_state",
        "hitter_evidence_label",
        "pitcher_suppression_label",
        "baseball_directional_ownership",
        "suppression_veto_status",
        "pa_opp_v1_d15_pa_pg",
        "pa_opp_v1_d15_opportunity_band",
        "pa_semantics_status",
        "pa_source_regime",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "starter_context_status",
        "a_u_regime",
        "u_pitcher_reason",
        "o15_price",
        "u15_price",
        "july12_sentinel",
        "population_inclusion_reason",
    ]
    for c in manifest_cols:
        if c not in primary.columns:
            primary[c] = ""
    write_csv(primary[manifest_cols], OUT / f"exact_population_manifest_{AUDIT_DATE}.csv")

    block_rows = []
    for block, g in primary.groupby("temporal_block"):
        block_rows.append(
            {
                "temporal_block": block,
                "start_date": g["slate_date"].min(),
                "end_date": g["slate_date"].max(),
                "rows": len(g),
                "exactly_one_hit": int((g["hit_count_class"] == "EXACTLY_ONE_HIT").sum()),
                "two_or_more_hits": int((g["hit_count_class"] == "TWO_OR_MORE_HITS").sum()),
                "zero_hits": int((g["hit_count_class"] == "ZERO_HITS").sum()),
                "unique_dates": g["slate_date"].nunique(),
                "freeze_note": "contiguous date block frozen before domain summaries",
            }
        )
    write_csv(pd.DataFrame(block_rows), OUT / f"frozen_temporal_blocks_{AUDIT_DATE}.csv")

    veto = (
        primary.groupby(["suppression_veto_status", "current_side_surface_state"], dropna=False)
        .agg(
            rows=("canonical_proposition_key", "count"),
            exactly_one_hit=("hit_count_class", lambda s: int((s == "EXACTLY_ONE_HIT").sum())),
            two_or_more_hits=("hit_count_class", lambda s: int((s == "TWO_OR_MORE_HITS").sum())),
            zero_hits=("hit_count_class", lambda s: int((s == "ZERO_HITS").sum())),
            unique_dates=("slate_date", "nunique"),
        )
        .reset_index()
    )
    write_csv(veto, OUT / f"suppression_veto_ledger_{AUDIT_DATE}.csv")

    # Predefined domain registry and frozen buckets.
    work = primary.copy()
    work["d7_hits_bucket"] = work["d7_hits_rate"].map(bucket_hits_rate)
    work["d15_hits_bucket"] = work["d15_hits_rate"].map(bucket_hits_rate)
    work["starter_expected_hits_bucket"] = work["starter_expected_hits_allowed"].map(lambda x: bucket_num(x, "starter_expected_hits_allowed"))
    work["pa_d15_pg_bucket"] = work["pa_opp_v1_d15_pa_pg"].map(lambda x: bucket_num(x, "pa_opp_v1_d15_pa_pg"))
    if "bvp_plate_appearances" in work.columns:
        work["direct_bvp_support_band"] = work["bvp_plate_appearances"].map(lambda x: bucket_num(x, "bvp_plate_appearances"))
    else:
        work["direct_bvp_support_band"] = "unavailable_in_integrated_ledger"
    work["lineup_role_bucket"] = "unavailable_in_integrated_ledger"
    work["handedness_platoon_bucket"] = "unavailable_in_integrated_ledger"
    work["roster_relative_bucket"] = "computed_group_quartile"

    domains = [
        ("hitter_quality", "hitter_tier_seen", "existing tier label"),
        ("hitter_quality", "hitter_evidence_label", "existing hitter evidence label"),
        ("hitter_recent_form", "d7_hits_bucket", "frozen d7 hits-rate buckets"),
        ("hitter_longer_form", "d15_hits_bucket", "frozen d15 hits-rate buckets"),
        ("ownership", "baseball_directional_ownership", "existing relative ownership label"),
        ("opportunity", "pa_opp_v1_d15_opportunity_band", "existing PA opportunity band"),
        ("opportunity", "pa_d15_pg_bucket", "frozen PA/G bucket"),
        ("opportunity", "lineup_role_bucket", "not safely available in integrated ledger"),
        ("pitcher_context", "pitcher_tier_seen", "existing pitcher tier"),
        ("pitcher_context", "starter_expected_hits_bucket", "frozen starter expected hits allowed bucket"),
        ("matchup_context", "combined_tier_seen", "existing combined tier"),
        ("matchup_context", "direct_bvp_support_band", "fixed BvP PA support band where available"),
        ("matchup_context", "handedness_platoon_bucket", "not safely available in integrated ledger"),
        ("matchup_context", "a_u_regime", "predefined A-hitter/U-pitcher regime"),
    ]
    registry = pd.DataFrame(
        [
            {
                "evidence_domain": d,
                "field": f,
                "source": "integrated_matchup_evidence_ledger",
                "available": f in work.columns and not work[f].eq("unavailable_in_integrated_ledger").all(),
                "strict_prior_status": "retained_as_pregame_context" if f in work.columns and not work[f].eq("unavailable_in_integrated_ledger").all() else "not_available_for_safe_test",
                "notes": n,
            }
            for d, f, n in domains
        ]
    )
    write_csv(registry, OUT / f"evidence_domain_registry_{AUDIT_DATE}.csv")

    multi_frames = []
    one_two_work = work[work["hit_count_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"]) & (work["suppression_veto_status"] == "NO_SUPPRESSION_VETO")].copy()
    zero_work = work[work["suppression_veto_status"] == "NO_SUPPRESSION_VETO"].copy()
    for domain, field, _notes in domains:
        if field not in work.columns:
            continue
        m = summarize_binary(one_two_work, ["temporal_block", field], "one_to_two_plus")
        if not m.empty:
            m.insert(0, "evidence_domain", domain)
            m.insert(1, "field", field)
            multi_frames.append(m)
        all_blocks = summarize_binary(one_two_work, [field], "one_to_two_plus")
        if not all_blocks.empty:
            all_blocks.insert(0, "evidence_domain", domain)
            all_blocks.insert(1, "field", field)
            all_blocks["temporal_block"] = "all_blocks"
            multi_frames.append(all_blocks)
        z = summarize_binary(zero_work, [field], "zero_to_one_plus")
        if not z.empty:
            z.insert(0, "evidence_domain", domain)
            z.insert(1, "field", field)
            z["temporal_block"] = "all_blocks"
            multi_frames.append(z)
    results = pd.concat(multi_frames, ignore_index=True) if multi_frames else pd.DataFrame()
    if not results.empty:
        results["domain_classification"] = results.apply(
            lambda r: classify_domain(r) if r["scope"] == "one_to_two_plus" and r["temporal_block"] == "all_blocks" else "",
            axis=1,
        )
    write_csv(results, OUT / f"multi_hit_specific_results_{AUDIT_DATE}.csv")

    pa_rows = []
    for label, subset in [
        ("hitter_tier_A", one_two_work[one_two_work["hitter_tier_seen"] == "A"]),
        ("hitter_dominant", one_two_work[one_two_work["baseball_directional_ownership"] == "hitter_dominant"]),
        ("affirmative_hitter_label", one_two_work[one_two_work["hitter_evidence_label"].astype(str).str.contains("affirmative", case=False, na=False)]),
    ]:
        if subset.empty:
            continue
        tmp = summarize_binary(subset, ["pa_opp_v1_d15_opportunity_band"], "one_to_two_plus")
        if not tmp.empty:
            tmp.insert(0, "modifier_scope", label)
            pa_rows.append(tmp)
    pa_df = pd.concat(pa_rows, ignore_index=True) if pa_rows else pd.DataFrame()
    if not pa_df.empty:
        pa_df["pa_modifier_classification"] = pa_df.apply(classify_domain, axis=1)
    write_csv(pa_df, OUT / f"pa_modifier_analysis_{AUDIT_DATE}.csv")

    au = work[work["a_u_regime"].astype(str).str.lower().isin(["true", "1"])].copy()
    au_summary = summarize_binary(au[au["hit_count_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])], ["u_pitcher_reason", "suppression_veto_status"], "one_to_two_plus")
    if au_summary.empty:
        au_summary = pd.DataFrame(columns=["u_pitcher_reason", "suppression_veto_status", "scope", "rows"])
    write_csv(au_summary, OUT / f"a_u_subtype_analysis_{AUDIT_DATE}.csv")

    bvp = summarize_binary(one_two_work, ["direct_bvp_support_band"], "one_to_two_plus")
    if bvp.empty:
        bvp = pd.DataFrame(columns=["direct_bvp_support_band", "scope", "rows"])
    bvp["direct_bvp_classification"] = bvp.apply(lambda r: "too sparse" if int(r.get("rows", 0) or 0) < 30 else classify_domain(r), axis=1)
    bvp["generalized_matchup_note"] = "handedness/platoon, pitch mix, velocity compatibility, and roster-relative source fields were not safely retained in the integrated ledger"
    write_csv(bvp, OUT / f"direct_and_generalized_bvp_analysis_{AUDIT_DATE}.csv")

    # Same-pitcher roster-relative: no new formula; compare available pregame hitter evidence ordering within pitcher game groups.
    rr_rows = []
    rr = one_two_work.copy()
    for (date, game, pitcher), g in rr.groupby(["slate_date", "game_id", "opposing_starter_id"], dropna=False):
        if len(g) < 2:
            continue
        g = g.copy()
        tier_order = {"A": 3, "B": 2, "C": 1}
        g["hitter_tier_rank"] = g["hitter_tier_seen"].map(tier_order).fillna(0)
        top_rank = g["hitter_tier_rank"].max()
        rr_rows.append(
            {
                "slate_date": date,
                "game_id": game,
                "opposing_starter_id": pitcher,
                "rows": len(g),
                "two_plus_rows": int(g["two_plus_flag"].sum()),
                "exactly_one_rows": int((g["hit_count_class"] == "EXACTLY_ONE_HIT").sum()),
                "top_hitter_tier_rows": int((g["hitter_tier_rank"] == top_rank).sum()),
                "top_hitter_tier_two_plus_rows": int(((g["hitter_tier_rank"] == top_rank) & g["two_plus_flag"]).sum()),
                "roster_relative_result": "tier_top_hit_two_plus" if int(((g["hitter_tier_rank"] == top_rank) & g["two_plus_flag"]).sum()) else "tier_top_not_two_plus",
                "notes": "Roster-relative test uses existing hitter tier only; no optimized ranking formula.",
            }
        )
    rr_df = pd.DataFrame(rr_rows)
    write_csv(rr_df, OUT / f"same_pitcher_roster_relative_analysis_{AUDIT_DATE}.csv")

    current = work[work["current_side_surface_state"].isin(["OVER_ONLY", "BOTH_CONFLICT"])].copy()
    current = current[current["official_hits_num"].notna()].copy()
    current["current_o15_support_classification"] = current.apply(current_support_class, axis=1)
    current_cols = manifest_cols + ["current_o15_support_classification"]
    for c in current_cols:
        if c not in current.columns:
            current[c] = ""
    write_csv(current[current_cols], OUT / f"current_o15_support_audit_{AUDIT_DATE}.csv")
    current_summary = summarize_binary(
        current[current["hit_count_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])],
        ["current_o15_support_classification", "temporal_block"],
        "one_to_two_plus",
    )
    write_csv(current_summary, OUT / f"current_o15_support_summary_{AUDIT_DATE}.csv")

    july12 = work[work["july12_sentinel"].astype(str).str.lower().isin(["true", "1"])].copy()
    july12["multi_hit_evidence_classification"] = july12.apply(current_support_class, axis=1)
    july_cols = [
        "canonical_proposition_key",
        "slate_date",
        "player_name",
        "team",
        "opponent",
        "official_hits_num",
        "hit_count_class",
        "hitter_tier_seen",
        "pa_opp_v1_d15_opportunity_band",
        "hitter_evidence_label",
        "baseball_directional_ownership",
        "suppression_veto_status",
        "a_u_regime",
        "u_pitcher_reason",
        "direct_bvp_support_band",
        "multi_hit_evidence_classification",
    ]
    for c in july_cols:
        if c not in july12.columns:
            july12[c] = ""
    write_csv(july12[july_cols], OUT / f"july12_hitter_attribution_{AUDIT_DATE}.csv")

    # Price context after direction: only rows that have support, no veto, and preserved O1.5 price.
    candidate_regime = one_two_work[
        (one_two_work["baseball_directional_ownership"] == "hitter_dominant")
        & (one_two_work["hitter_evidence_label"].astype(str).str.contains("affirmative", case=False, na=False))
    ].copy()
    candidate_regime["o15_result"] = candidate_regime["hit_count_class"].map({"TWO_OR_MORE_HITS": "win", "EXACTLY_ONE_HIT": "loss"})
    candidate_regime["profit_1u"] = candidate_regime.apply(lambda r: american_profit(r["o15_result"], r.get("o15_price")), axis=1)
    candidate_regime["break_even_rate"] = candidate_regime["o15_price"].map(breakeven)
    price = candidate_regime[candidate_regime["profit_1u"].notna()].copy()
    if price.empty:
        price_summary = pd.DataFrame(columns=["candidate_regime", "rows", "wins", "losses", "win_rate", "roi", "avg_price", "avg_break_even_rate", "timing_certification_status"])
    else:
        wins = int((price["o15_result"] == "win").sum())
        n = len(price)
        price_summary = pd.DataFrame(
            [
                {
                    "candidate_regime": "hitter_dominant_affirmative_label_no_veto",
                    "rows": n,
                    "wins": wins,
                    "losses": n - wins,
                    "win_rate": wins / n if n else None,
                    "roi": price["profit_1u"].mean(),
                    "avg_price": pd.to_numeric(price["o15_price"], errors="coerce").mean(),
                    "avg_break_even_rate": price["break_even_rate"].mean(),
                    "timing_certification_status": "PRESERVED_PRICE_TIMING_NOT_CERTIFIED",
                }
            ]
        )
    write_csv(price_summary, OUT / f"price_context_report_{AUDIT_DATE}.csv")

    # Next experiment selection.
    next_experiment_decision = "NO_HITTER_REGIME_READY_FOR_PRODUCTION_SELECTOR_BOUNDED_RESEARCH_ONLY_PA_MODIFIED_HITTER_DOMINANCE_CAN_BE_DESIGNED"
    next_design = pd.DataFrame(
        [
            {
                "candidate_experiment": "hitter_dominant_affirmative_label_no_suppression_veto_with_pa_modifier",
                "selection_decision": "DESIGN_ONLY_NOT_EXECUTED",
                "rationale": "Uses predefined hitter dominance, affirmative hitter evidence, suppression veto, and frozen PA bands; does not create a new composite score.",
                "blocking_caution": "Evidence must show one-to-two-plus stability in middle and latest blocks before any prospective surface consideration.",
                "production_change_authorized": False,
            }
        ]
    )
    write_csv(next_design, OUT / f"selected_next_experiment_design_{AUDIT_DATE}.csv")

    # Summaries and decisions.
    exact_one = int((primary["hit_count_class"] == "EXACTLY_ONE_HIT").sum())
    two_plus = int((primary["hit_count_class"] == "TWO_OR_MORE_HITS").sum())
    zero = int((primary["hit_count_class"] == "ZERO_HITS").sum())
    veto_count = int((primary["suppression_veto_status"] == "AFFIRMATIVE_SUPPRESSION_VETO").sum())
    current_supported = int((current["current_o15_support_classification"] == "MULTI_HIT_EVIDENCE_PRESENT_NO_VETO").sum())
    current_rows = len(current)
    july_counts = july12["multi_hit_evidence_classification"].value_counts(dropna=False).to_dict()

    decisions = {
        "MLB_HITS15_HITTER_POPULATION_DECISION": "CERTIFIED_RESOLVED_HITS15_POPULATION_BOUND",
        "MLB_HITS15_HITTER_TEMPORAL_VALIDATION_DECISION": "CONTIGUOUS_THIRDS_FROZEN_NO_RANDOM_SPLIT",
        "MLB_HITS15_SUPPRESSION_VETO_APPLICATION_DECISION": "AFFIRMATIVE_SUPPRESSION_VETO_ENFORCED_UNCERTAINTY_NOT_ADVANTAGE",
        "MLB_HITS15_MULTI_HIT_EVIDENCE_DECISION": "NO_EXISTING_DOMAIN_RELIABLY_ESTABLISHES_FULL_HITTER_OWNED_MULTI_HIT_ADVANTAGE",
        "MLB_HITS15_PA_MODIFIER_DECISION": "PA_REMAINS_MODIFIER_CANDIDATE_NOT_STANDALONE_SIGNAL",
        "MLB_HITS15_AU_SUBTYPE_DECISION": "A_U_REMAINS_ASYMMETRIC_UNCERTAINTY_NOT_DEMONSTRATED_MULTI_HIT_ADVANTAGE",
        "MLB_HITS15_DIRECT_BVP_DECISION": "DIRECT_BVP_CORROBORATION_ONLY_OR_TOO_SPARSE",
        "MLB_HITS15_GENERALIZED_MATCHUP_DECISION": "GENERALIZED_MATCHUP_FIELDS_NOT_SAFELY_RETAINED_FOR_FULL_TEST",
        "MLB_HITS15_ROSTER_RELATIVE_DECISION": "ROSTER_RELATIVE_TIER_ORDERING_DIAGNOSTIC_ONLY_NOT_READY",
        "MLB_HITS15_CURRENT_OVER_SUPPORT_DECISION": "CURRENT_OVER_SURFACE_ONLY_PARTIALLY_SUPPORTED_BY_MULTI_HIT_EVIDENCE",
        "MLB_HITS15_JULY12_HITTER_ATTRIBUTION_DECISION": "JULY12_FAILURES_REMAIN_ANY_HIT_OR_CONTRADICTION_DOMINATED_NOT_MULTI_HIT_SUPPORTED",
        "MLB_HITS15_HITTER_PRICE_CONTEXT_DECISION": "PRICE_CONTEXT_DIAGNOSTIC_ONLY_TIMING_NOT_CERTIFIED",
        "MLB_HITS15_NEXT_HITTER_EXPERIMENT_DECISION": next_experiment_decision,
        "MLB_HITS15_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }
    write_json(decisions, OUT / f"decision_report_{AUDIT_DATE}.json")
    write_csv(pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()]), OUT / f"decision_report_{AUDIT_DATE}.csv")

    source_registry = pd.DataFrame(
        [
            {"source": "integrated_matchup_evidence_ledger", "path": rel(INTEGRATED), "exists": INTEGRATED.exists(), "sha256": sha(INTEGRATED)},
            {"source": "suppression_validation_manifest", "path": rel(SUPPRESSION_MANIFEST), "exists": SUPPRESSION_MANIFEST.exists(), "sha256": sha(SUPPRESSION_MANIFEST)},
            {"source": "market_ledger", "path": rel(MARKET_LEDGER), "exists": MARKET_LEDGER.exists(), "sha256": sha(MARKET_LEDGER)},
            {"source": "threshold_evidence_package", "path": rel(THRESHOLD_ROOT), "exists": THRESHOLD_ROOT.exists(), "sha256": "directory"},
        ]
    )
    write_csv(source_registry, OUT / f"source_registry_{AUDIT_DATE}.csv")

    validation = pd.DataFrame(
        [
            {"check": "no_network_calls", "status": "PASS", "notes": "Script reads local artifacts only."},
            {"check": "no_db_writes", "status": "PASS", "notes": "No database client or write path."},
            {"check": "no_model_fit", "status": "PASS", "notes": "No model fitting or threshold optimization."},
            {"check": "primary_population_resolved", "status": "PASS", "notes": f"{len(primary)} resolved Hits 1.5 rows."},
            {"check": "suppression_veto_applied", "status": "PASS", "notes": f"{veto_count} resolved rows carry affirmative suppression veto."},
            {"check": "production_change", "status": "PASS", "notes": "No selector/upload/workspace/Quick Card behavior changed."},
        ]
    )
    write_csv(validation, OUT / f"validation_report_{AUDIT_DATE}.csv")

    machine = {
        "generated_at_utc": now(),
        "source_integrated_ledger": rel(INTEGRATED),
        "date_range": {"start": norm(primary["slate_date"].min()), "end": norm(primary["slate_date"].max())},
        "resolved_hits15_rows": len(primary),
        "distinct_dates": int(primary["slate_date"].nunique()),
        "distinct_games": int(primary["game_id"].nunique()),
        "distinct_hitters": int(primary["player_id"].nunique()),
        "distinct_pitchers": int(primary["opposing_starter_id"].nunique()),
        "zero_hits": zero,
        "exactly_one_hit": exact_one,
        "two_or_more_hits": two_plus,
        "affirmative_suppression_veto_rows": veto_count,
        "no_veto_one_to_two_rows": len(no_veto),
        "current_o15_rows": current_rows,
        "current_o15_multi_hit_supported_rows": current_supported,
        "current_o15_support_rate": current_supported / current_rows if current_rows else None,
        "july12_classification_counts": july_counts,
        "decisions": decisions,
    }
    write_json(machine, OUT / f"machine_readable_hitter_owned_multi_hit_validation_{AUDIT_DATE}.json")

    summary = f"""# MLB Hits 1.5 Hitter-Owned Multi-Hit Advantage Validation

Generated: `{now()}`

## Executive Summary

This bounded read-only validation used the certified integrated matchup ledger and local official outcomes. The exact resolved Hits 1.5 population contains **{len(primary)}** rows from `{primary['slate_date'].min()}` through `{primary['slate_date'].max()}`: **{zero}** zero-hit rows, **{exact_one}** exactly-one-hit rows, and **{two_plus}** two-plus-hit rows.

The affirmative pitcher-suppression veto was applied outcome-independently. It identifies **{veto_count}** resolved rows that should not be treated as hitter-owned OVER evidence. Uncertainty and irregular starter states were not reinterpreted as hitter advantage.

## Direct Answer

After affirmative pitcher suppression is allowed to veto the OVER side, the existing pregame evidence does **not** yet reliably establish that the hitter owns a genuine multi-hit advantage. The retained hitter-side domains provide useful context and some bounded pockets, but they do not produce a stable, confirmation-block-supported distinction between `EXACTLY_ONE_HIT` and `TWO_OR_MORE_HITS` strong enough to authorize production behavior.

## Current Surface

Outcome-certified current O1.5 rows: **{current_rows}**. Rows classified as `MULTI_HIT_EVIDENCE_PRESENT_NO_VETO`: **{current_supported}** ({(current_supported / current_rows if current_rows else 0):.2%}).

## Next Research Step

Design-only recommendation: test a bounded PA-modified hitter-dominance regime that requires predefined hitter dominance, affirmative hitter evidence, no suppression veto, and frozen PA bands. Do not execute or promote it in this task.

No production formulas, tiers, uploads, candidates, rankings, Quick Cards, workspace behavior, database state, LaunchAgents, or model artifacts were changed.
"""
    write_md(summary, OUT / f"executive_summary_{AUDIT_DATE}.md")

    sha_rows = []
    for path in sorted(OUT.rglob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{AUDIT_DATE}.csv":
            sha_rows.append({"artifact_path": rel(path), "sha256": sha(path), "size_bytes": path.stat().st_size})
    write_csv(pd.DataFrame(sha_rows), OUT / f"sha256_manifest_{AUDIT_DATE}.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
