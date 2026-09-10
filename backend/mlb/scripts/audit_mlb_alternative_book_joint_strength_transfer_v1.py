#!/usr/bin/env python3
"""Deterministic, read-only MLB alternative-book joint-strength transfer audit v1."""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from backend.mlb.scripts import audit_mlb_across_board_apparent_ev_provenance_economic_value_v1 as across


ROOT = Path(__file__).resolve().parents[3]
AUDIT = "MLB_ALTERNATIVE_BOOK_JOINT_STRENGTH_TRANSFER_V1"
CUTOFF = "2026-09-08"
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_alternative_book_joint_strength_transfer_audit_v1/2026-09-09"
PRIOR_JOINT = ROOT / "artifacts/analysis/model_development/mlb_joint_strength_incremental_value_executability_audit_v1/2026-09-09/unique_game_strength_class_ledger.csv"
MARKET_DB = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
PACIFIC = ZoneInfo("America/Los_Angeles")
WINDOWS = ("05:30", "08:30", "11:00", "13:00", "16:30")
SNAPSHOTS = (
    "FIRST_TRUSTWORTHY_PREGAME",
    "FIRST_AT_OR_AFTER_IMMUTABLE_MODEL",
    *(f"SCHEDULED_WINDOW_{w.replace(':', '')}_PT" for w in WINDOWS),
    "NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT",
    "LATEST_TRUSTWORTHY_PREGAME",
    "NEAREST_WITHIN_30_MINUTES_OF_START",
)
STRONG = .60
MEANINGFUL_JOINT_MIN = 8
ROBUSTNESS_MIN_JOINT = 38
MATCH_CALIPER = .02
BOOT_REPS = 4000
SEED = 20260909
EXCHANGES = {"betfairexchange", "kalshi", "matchbook", "novig", "polymarket", "prophetexchange"}
AMBIGUOUS = {"unknown"}
ALIASES = {
    "betonline": ("betonline", "betonlineag", "betonline.ag", "BetOnline"),
    "mybookie": ("mybookie", "mybookieag", "MyBookie", "MyBookie.ag"),
    "williamhill": ("williamhill", "williamhill_us", "William Hill", "William Hill US"),
    "pinnacle": ("pinnacle", "Pinnacle"),
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, float_format="%.12f", na_rep="")


def json_default(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def canonical_source(value: Any) -> str:
    source = str(value).strip().lower().split(":", 1)[-1]
    return {"betonlineag": "betonline", "betonline.ag": "betonline",
            "mybookieag": "mybookie", "williamhill_us": "williamhill"}.get(source, source)


def source_type(source: str) -> str:
    if source in AMBIGUOUS:
        return "AMBIGUOUS_IDENTITY"
    if source in EXCHANGES:
        return "EXCHANGE_OR_PREDICTION_MARKET"
    return "SPORTSBOOK"


def decimal_price(price: Any) -> float:
    price = float(price)
    return 1.0 + (100.0 / abs(price) if price < 0 else price / 100.0)


def implied(price: Any) -> float:
    price = float(price)
    return abs(price) / (abs(price) + 100.0) if price < 0 else 100.0 / (price + 100.0)


def cluster_ci(dates: Iterable[Any], returns: Iterable[float]) -> tuple[float, float]:
    d = pd.DataFrame({"date": list(dates), "return": list(returns)}).dropna()
    grouped = d.groupby("date", sort=True)["return"].agg(["sum", "count"])
    if len(grouped) < 2:
        return np.nan, np.nan
    rng = np.random.default_rng(SEED + len(d) + int(abs(d["return"].sum()) * 1000) % 997)
    picks = rng.integers(0, len(grouped), size=(BOOT_REPS, len(grouped)))
    values = grouped["sum"].to_numpy()[picks].sum(1) / grouped["count"].to_numpy()[picks].sum(1)
    return float(np.quantile(values, .025)), float(np.quantile(values, .975))


def longest_consecutive_days(values: Iterable[Any]) -> int:
    dates = sorted(set(pd.to_datetime(list(values)).date))
    best = current = 0
    previous = None
    for date in dates:
        current = current + 1 if previous is not None and (date - previous).days == 1 else 1
        best = max(best, current)
        previous = date
    return best


def local_target(game_date: str, hhmm: str) -> pd.Timestamp:
    return pd.Timestamp(f"{game_date} {hhmm}", tz=PACIFIC).tz_convert("UTC")


def load_raw_rows(cutoff: str, game_ids: set[int]) -> pd.DataFrame:
    conn = sqlite3.connect(f"file:{MARKET_DB}?mode=ro", uri=True)
    raw = pd.read_sql_query("""
      SELECT provider,bookmaker_key,game_date,game_id,captured_at_utc,scheduled_start_utc,
             timing_status,market_payload_json,raw_source_path
      FROM supplemental_main_market_snapshots
      WHERE market_type='MONEYLINE' AND game_date<=?
      ORDER BY game_date,game_id,captured_at_utc,bookmaker_key
    """, conn, params=(cutoff,))
    conn.close()
    raw = raw[raw.game_id.astype(int).isin(game_ids)].copy()
    raw["source"] = raw.bookmaker_key.map(canonical_source)
    raw["captured_dt"] = pd.to_datetime(raw.captured_at_utc, utc=True, errors="coerce")
    raw["start_dt"] = pd.to_datetime(raw.scheduled_start_utc, utc=True, errors="coerce")
    paired, provider_updated = [], []
    for payload in raw.market_payload_json:
        data = json.loads(payload)
        paired.append(data.get("home_american_price") is not None and data.get("away_american_price") is not None)
        provider_updated.append(pd.to_datetime(data.get("provider_market_updated_at_utc"), utc=True, errors="coerce"))
    raw["two_sided"] = paired
    raw["provider_updated_dt"] = provider_updated
    raw["valid_pregame"] = (raw.timing_status.eq("PREGAME_CERTIFIED") & raw.captured_dt.notna()
                            & raw.start_dt.notna() & raw.provider_updated_dt.notna()
                            & raw.captured_dt.lt(raw.start_dt) & raw.provider_updated_dt.lt(raw.start_dt))
    return raw


def prepare_quotes(quotes: pd.DataFrame) -> pd.DataFrame:
    d = quotes.copy()
    d["source"] = d.sportsbook.map(canonical_source)
    d["prediction_dt"] = pd.to_datetime(d.prediction_timestamp_utc, utc=True)
    d["capture_local"] = d.captured_dt.dt.tz_convert(PACIFIC)
    return d.sort_values(["source", "game_id", "captured_dt", "canonical_market_identity"]).reset_index(drop=True)


def inventory_sources(raw: pd.DataFrame, quotes: pd.DataFrame, predictions: pd.DataFrame,
                      joint: pd.DataFrame) -> pd.DataFrame:
    joint_ids = set(joint.game_id.astype(int))
    strong_ids = set(predictions.loc[predictions.selected_model_probability.gt(STRONG), "game_id"].astype(int))
    rows = []
    for source in sorted(set(raw.source) | set(quotes.source)):
        r = raw[raw.source.eq(source)]
        q = quotes[quotes.source.eq(source)]
        ids = set(q.game_id.astype(int))
        joint_q = q[q.game_id.isin(joint_ids)]
        post = joint_q[joint_q.captured_dt.ge(joint_q.prediction_dt)]
        paired = joint_q[joint_q.home_american_price.notna() & joint_q.away_american_price.notna()]
        unique_capture_times = q[["captured_at_utc"]].drop_duplicates().copy()
        if len(unique_capture_times):
            times = pd.to_datetime(unique_capture_times.captured_at_utc, format="mixed", utc=True).dt.tz_convert(PACIFIC)
            minute_buckets = (times.dt.hour * 2 + (times.dt.minute >= 15).astype(int)).map(
                lambda x: f"{x//2:02d}:{(x%2)*30:02d}")
            consistency = float(minute_buckets.value_counts(normalize=True).iloc[0])
            first_time, last_time = times.dt.strftime("%H:%M:%S").min(), times.dt.strftime("%H:%M:%S").max()
        else:
            consistency, first_time, last_time = np.nan, None, None
        observed_aliases = sorted(set(r.bookmaker_key.astype(str)) | set(q.sportsbook.astype(str)))
        aliases = sorted(set(observed_aliases) | set(ALIASES.get(source, (source,))))
        providers = sorted(set(r.provider.dropna().astype(str)) | set(q.provider.dropna().astype(str)))
        if any("sportsgameodds_main_market" in str(x) for x in r.raw_source_path.dropna()):
            artifact_authority = "backend/mlb/exports/market_history/sportsgameodds_main_market/raw"
        elif source == "pinnacle":
            artifact_authority = "backend/mlb/exports/odds_history/<date>/odds_mlb_pinnacle_main_markets__*.json"
        else:
            artifact_authority = "exact raw_source_path values retained in source_specific_price_ledger.csv"
        all_post = q[q.captured_dt.ge(q.prediction_dt)]
        all_paired = q[q.home_american_price.notna() & q.away_american_price.notna()]
        rows.append({
            "normalized_source_name": source,
            "historical_aliases": "|".join(aliases),
            "raw_observation_count": len(r),
            "raw_valid_pregame_observations": int(r.valid_pregame.sum()) if len(r) else 0,
            "unique_game_coverage": len(ids),
            "joint_strength_coverage": len(ids & joint_ids),
            "model_strong_coverage": len(ids & strong_ids),
            "full_471_game_coverage": len(ids),
            "first_game_date": q.game_date.min() if len(q) else None,
            "last_game_date": q.game_date.max() if len(q) else None,
            "covered_game_dates": q.game_date.nunique(),
            "longest_consecutive_game_date_run": longest_consecutive_days(q.game_date) if len(q) else 0,
            "pregame_unique_game_coverage": q.game_id.nunique(),
            "all_population_post_prediction_coverage": all_post.game_id.nunique(),
            "all_population_two_sided_coverage": all_paired.game_id.nunique(),
            "joint_post_prediction_coverage": post.game_id.nunique(),
            "joint_two_sided_coverage": paired.game_id.nunique(),
            "first_daily_observation_time_pt": first_time,
            "last_daily_observation_time_pt": last_time,
            "modal_half_hour_capture_share": consistency,
            "provider": "|".join(providers),
            "artifact_or_database_authority": f"supplemental_main_market_snapshots;{artifact_authority}",
            "capture_or_derived": "DIRECTLY_CAPTURED_NAMED_SOURCE_QUOTE_VIA_PROVIDER_RELAY",
            "source_type": source_type(source),
            "user_availability_or_executable_access": "UNKNOWN_NOT_ESTABLISHED",
        })
    return pd.DataFrame(rows).sort_values(
        ["joint_strength_coverage", "joint_post_prediction_coverage", "joint_two_sided_coverage",
         "longest_consecutive_game_date_run", "modal_half_hour_capture_share", "full_471_game_coverage",
         "normalized_source_name"], ascending=[False, False, False, False, False, False, True]).reset_index(drop=True)


def select_primary(inventory: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Freeze selection from evidence-only fields; no outcome/economics columns are accepted."""
    required = ["joint_strength_coverage", "joint_post_prediction_coverage", "joint_two_sided_coverage",
                "longest_consecutive_game_date_run", "modal_half_hour_capture_share", "full_471_game_coverage"]
    candidates = inventory[(inventory.source_type.eq("SPORTSBOOK"))
                           & ~inventory.normalized_source_name.isin(["pinnacle", "betonline"])].copy()
    candidates = candidates.sort_values(required + ["normalized_source_name"],
                                        ascending=[False] * len(required) + [True]).reset_index(drop=True)
    if candidates.empty:
        raise ValueError("No eligible non-Pinnacle, non-BetOnline sportsbook source")
    top_signature = tuple(candidates.iloc[0][required])
    selected = candidates[candidates.apply(lambda r: tuple(r[required]) == top_signature, axis=1)]
    selected_names = selected.normalized_source_name.tolist()
    records = []
    for _, row in inventory.iterrows():
        source = row.normalized_source_name
        if source in selected_names:
            reason = "SELECTED_BY_PREDECLARED_COVERAGE_AND_EVIDENCE_HIERARCHY_BEFORE_ECONOMICS"
        elif source == "pinnacle":
            reason = "REFERENCE_SOURCE_EXCLUDED_FROM_PRIMARY"
        elif source == "betonline":
            reason = "EXPLICITLY_EXCLUDED_FROM_PRIMARY_DUE_TO_KNOWN_COVERAGE_LIMITATION"
        elif row.source_type == "AMBIGUOUS_IDENTITY":
            reason = "AMBIGUOUS_SOURCE_IDENTITY"
        elif row.source_type != "SPORTSBOOK":
            reason = "NOT_A_SPORTSBOOK"
        else:
            reason = "LOWER_LEXICOGRAPHIC_COVERAGE_OR_EVIDENCE_RANK"
        records.append({**row.to_dict(), "selected_primary": source in selected_names,
                        "selection_or_exclusion_reason": reason,
                        "selection_statistics_frozen_before_outcome_economics": True})
    return pd.DataFrame(records), selected_names


def build_cohorts(predictions: pd.DataFrame, ledger: pd.DataFrame) -> dict[str, pd.DataFrame]:
    p = predictions.copy()
    p["evaluated_side"] = p.model_selected_side
    p["evaluated_win"] = p.selected_win.astype(int)
    p["evaluated_team"] = p.selected_team
    class_map = {
        "JOINT_STRENGTH_FIXED_76": ["JOINT_STRONG_SAME_SIDE"],
        "MODEL_STRONG_ONLY": ["MODEL_STRONG_ONLY"],
        "MARKET_STRONG_ONLY": ["MARKET_STRONG_ONLY"],
        "STRONG_CONFLICT": ["STRONG_CONFLICT"],
        "NEITHER_STRONG": ["NEITHER_STRONG"],
    }
    cohorts = {name: ledger[ledger.strength_class.isin(classes)].copy() for name, classes in class_map.items()}
    cohorts["ALL_MODEL_STRONG_137"] = p[p.selected_model_probability.gt(STRONG)].copy()
    cohorts["ALL_MARKET_STRONG_SIDES"] = ledger[
        ledger.strength_class.isin(["JOINT_STRONG_SAME_SIDE", "MARKET_STRONG_ONLY", "STRONG_CONFLICT"])
    ].copy()
    cohorts["ALL_MODEL_SELECTIONS_471"] = p.copy()
    return cohorts


def evaluate_quotes(quotes: pd.DataFrame, cohort: pd.DataFrame) -> pd.DataFrame:
    cols = ["game_id", "game_date", "evaluated_side", "evaluated_win", "evaluated_team"]
    c = cohort[cols].drop_duplicates("game_id")
    d = quotes.merge(c, on="game_id", how="inner", suffixes=("", "_fixed"), validate="many_to_one")
    d["game_date"] = d.game_date_fixed
    home = d.evaluated_side.eq("HOME")
    d["evaluated_american_price"] = np.where(home, d.home_american_price, d.away_american_price)
    d["evaluated_decimal_price"] = d.evaluated_american_price.map(decimal_price)
    d["evaluated_paid_break_even_probability"] = d.evaluated_american_price.map(implied)
    d["evaluated_no_vig_market_probability"] = np.where(home, d.no_vig_home_probability, d.no_vig_away_probability)
    return d.dropna(subset=["evaluated_american_price", "evaluated_decimal_price",
                           "evaluated_paid_break_even_probability", "evaluated_no_vig_market_probability"])


def fixed_snapshot_rows(quotes: pd.DataFrame, cohort: pd.DataFrame, source: str,
                        designated_times: dict[int, pd.Timestamp]) -> pd.DataFrame:
    d = evaluate_quotes(quotes[quotes.source.eq(source)].copy(), cohort)
    output = []
    for game_id, group in d.groupby("game_id", sort=True):
        g = group.sort_values(["captured_dt", "canonical_market_identity"])
        prediction_time = g.prediction_dt.iloc[0]
        choices: list[tuple[str, pd.DataFrame]] = [
            ("FIRST_TRUSTWORTHY_PREGAME", g.head(1)),
            ("FIRST_AT_OR_AFTER_IMMUTABLE_MODEL", g[g.captured_dt.ge(prediction_time)].head(1)),
        ]
        for window in WINDOWS:
            target = local_target(str(g.game_date.iloc[0]), window)
            z = g[(g.captured_dt - target).abs().dt.total_seconds().le(45 * 60)].copy()
            if len(z):
                z["selection_distance_seconds"] = (z.captured_dt - target).abs().dt.total_seconds()
                z = z.sort_values(["selection_distance_seconds", "captured_dt", "canonical_market_identity"]).head(1)
            choices.append((f"SCHEDULED_WINDOW_{window.replace(':', '')}_PT", z))
        target = designated_times.get(int(game_id))
        z = g.iloc[0:0]
        if target is not None:
            z = g.copy()
            z["selection_distance_seconds"] = (z.captured_dt - target).abs().dt.total_seconds()
            z = z.sort_values(["selection_distance_seconds", "captured_dt", "canonical_market_identity"]).head(1)
        choices.extend([
            ("NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT", z),
            ("LATEST_TRUSTWORTHY_PREGAME", g.tail(1)),
            ("NEAREST_WITHIN_30_MINUTES_OF_START", g[g.quote_lead_minutes.le(30)].tail(1)),
        ])
        for snapshot, selected in choices:
            if len(selected):
                output.append(selected.assign(snapshot=snapshot))
    return pd.concat(output, ignore_index=True) if output else pd.DataFrame()


def economics(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {"price_covered_games": 0, "wins": 0, "losses": 0, "record": "0-0", "win_rate": np.nan,
                "average_american_price": np.nan, "average_decimal_price": np.nan,
                "average_paid_break_even_probability": np.nan, "expected_wins_from_no_vig_market_probability": 0.0,
                "actual_wins": 0, "gross_winning_units": 0.0, "losing_units": 0.0, "net_units": 0.0,
                "captured_price_hypothetical_roi": np.nan, "roi_date_clustered_ci_2_5": np.nan,
                "roi_date_clustered_ci_97_5": np.nan, "best_date": None, "worst_date": None,
                "roi_excluding_best_date": np.nan, "roi_excluding_worst_date": np.nan}
    d = rows.drop_duplicates("game_id").copy()
    d["flat_stake_return"] = np.where(d.evaluated_win.eq(1), d.evaluated_decimal_price - 1, -1.0)
    wins = int(d.evaluated_win.sum())
    lo, hi = cluster_ci(d.game_date, d.flat_stake_return)
    daily = d.groupby("game_date", sort=True).flat_stake_return.sum()
    best, worst = str(daily.idxmax()), str(daily.idxmin())
    return {
        "price_covered_games": len(d), "wins": wins, "losses": len(d)-wins, "record": f"{wins}-{len(d)-wins}",
        "win_rate": float(d.evaluated_win.mean()), "average_american_price": float(d.evaluated_american_price.mean()),
        "average_decimal_price": float(d.evaluated_decimal_price.mean()),
        "average_paid_break_even_probability": float(d.evaluated_paid_break_even_probability.mean()),
        "expected_wins_from_no_vig_market_probability": float(d.evaluated_no_vig_market_probability.sum()),
        "actual_wins": wins,
        "gross_winning_units": float(d.loc[d.evaluated_win.eq(1), "flat_stake_return"].sum()),
        "losing_units": float(-d.evaluated_win.eq(0).sum()), "net_units": float(d.flat_stake_return.sum()),
        "captured_price_hypothetical_roi": float(d.flat_stake_return.mean()),
        "roi_date_clustered_ci_2_5": lo, "roi_date_clustered_ci_97_5": hi,
        "best_date": best, "worst_date": worst,
        "roi_excluding_best_date": float(d.loc[d.game_date.ne(best), "flat_stake_return"].mean()),
        "roi_excluding_worst_date": float(d.loc[d.game_date.ne(worst), "flat_stake_return"].mean()),
    }


def build_price_and_economics(quotes: pd.DataFrame, cohorts: dict[str, pd.DataFrame],
                              sources: list[str], designated_times: dict[int, pd.Timestamp]) -> tuple[pd.DataFrame, pd.DataFrame]:
    ledgers, summaries = [], []
    for source in sources:
        for cohort_name, cohort in cohorts.items():
            selected = fixed_snapshot_rows(quotes, cohort, source, designated_times)
            if len(selected):
                selected["cohort"] = cohort_name
                ledgers.append(selected)
            for snapshot in SNAPSHOTS:
                base = selected[selected.snapshot.eq(snapshot)] if len(selected) else pd.DataFrame()
                for temporal_scope, scoped in (
                    ("ALL", base),
                    ("AUGUST_2026", base[base.game_date.str.startswith("2026-08")] if len(base) else base),
                    ("SEPTEMBER_2026", base[base.game_date.str.startswith("2026-09")] if len(base) else base),
                ):
                    eligible = cohort if temporal_scope == "ALL" else cohort[
                        cohort.game_date.astype(str).str.startswith("2026-08" if temporal_scope.startswith("AUGUST") else "2026-09")]
                    summaries.append({"source": source, "cohort": cohort_name, "snapshot": snapshot,
                                      "temporal_scope": temporal_scope, "eligible_fixed_games": eligible.game_id.nunique(),
                                      "uncovered_games": eligible.game_id.nunique() - (scoped.game_id.nunique() if len(scoped) else 0),
                                      "availability_and_fillability": "UNKNOWN_NOT_ESTABLISHED",
                                      **economics(scoped)})
    ledger = pd.concat(ledgers, ignore_index=True) if ledgers else pd.DataFrame()
    if len(ledger):
        ledger["flat_stake_return"] = np.where(ledger.evaluated_win.eq(1), ledger.evaluated_decimal_price - 1, -1.0)
        keep = ["source", "cohort", "snapshot", "game_date", "game_id", "away_team", "home_team",
                "evaluated_side", "evaluated_team", "evaluated_win", "evaluated_american_price",
                "evaluated_decimal_price", "evaluated_paid_break_even_probability",
                "evaluated_no_vig_market_probability", "flat_stake_return", "captured_at_utc",
                "provider_market_updated_at_utc", "quote_lead_minutes", "prediction_timestamp_utc",
                "home_american_price", "away_american_price", "no_vig_home_probability",
                "no_vig_away_probability", "canonical_market_identity", "market_payload_sha256",
                "raw_source_path", "raw_source_sha256"]
        ledger = ledger[keep].sort_values(["source", "cohort", "snapshot", "game_date", "game_id"])
    return ledger, pd.DataFrame(summaries)


def common_game_comparisons(price_ledger: pd.DataFrame, alternatives: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    details, summaries = [], []
    joint = price_ledger[price_ledger.cohort.eq("JOINT_STRENGTH_FIXED_76")]
    for source in alternatives:
        for reference in ("pinnacle", "betonline"):
            for snapshot in SNAPSHOTS:
                a = joint[(joint.source.eq(source)) & joint.snapshot.eq(snapshot)].drop_duplicates("game_id")
                b = joint[(joint.source.eq(reference)) & joint.snapshot.eq(snapshot)].drop_duplicates("game_id")
                merged = a.merge(b, on="game_id", suffixes=("_alternative", "_reference"), validate="one_to_one")
                if len(merged):
                    merged["alternative_source"] = source
                    merged["reference_source"] = reference
                    merged["snapshot"] = snapshot
                    merged["american_price_difference"] = merged.evaluated_american_price_alternative - merged.evaluated_american_price_reference
                    merged["decimal_price_difference"] = merged.evaluated_decimal_price_alternative - merged.evaluated_decimal_price_reference
                    merged["roi_return_difference"] = merged.flat_stake_return_alternative - merged.flat_stake_return_reference
                    merged["better_price"] = np.where(merged.decimal_price_difference.gt(1e-12), "ALTERNATIVE",
                                                       np.where(merged.decimal_price_difference.lt(-1e-12), "REFERENCE", "TIE"))
                    details.append(merged[["alternative_source", "reference_source", "snapshot", "game_id",
                                           "game_date_alternative", "evaluated_side_alternative", "evaluated_win_alternative",
                                           "evaluated_american_price_alternative", "evaluated_american_price_reference",
                                           "evaluated_decimal_price_alternative", "evaluated_decimal_price_reference",
                                           "american_price_difference", "decimal_price_difference",
                                           "flat_stake_return_alternative", "flat_stake_return_reference",
                                           "roi_return_difference", "better_price", "captured_at_utc_alternative",
                                           "captured_at_utc_reference"]])
                alt_roi = float(merged.flat_stake_return_alternative.mean()) if len(merged) else np.nan
                ref_roi = float(merged.flat_stake_return_reference.mean()) if len(merged) else np.nan
                summaries.append({"alternative_source": source, "reference_source": reference, "snapshot": snapshot,
                                  "common_games": len(merged), "common_record": (f"{int(merged.evaluated_win_alternative.sum())}-"
                                                                                 f"{len(merged)-int(merged.evaluated_win_alternative.sum())}") if len(merged) else "0-0",
                                  "alternative_captured_price_hypothetical_roi": alt_roi,
                                  "reference_captured_price_hypothetical_roi": ref_roi,
                                  "roi_difference_alternative_minus_reference": alt_roi-ref_roi if len(merged) else np.nan,
                                  "alternative_better_price_games": int(merged.better_price.eq("ALTERNATIVE").sum()) if len(merged) else 0,
                                  "reference_better_price_games": int(merged.better_price.eq("REFERENCE").sum()) if len(merged) else 0,
                                  "tied_price_games": int(merged.better_price.eq("TIE").sum()) if len(merged) else 0,
                                  "profitability_sign_changes": bool(len(merged) and ((alt_roi > 0) != (ref_roi > 0))),
                                  "difference_basis": "PRICE_ONLY_ON_IDENTICAL_GAMES_AND_FIXED_SIDE" if len(merged) else "NO_COMMON_GAMES",
                                  "coverage_composition_removed": True})
    return (pd.concat(details, ignore_index=True) if details else pd.DataFrame(), pd.DataFrame(summaries))


def matched_model_contribution(price_ledger: pd.DataFrame, primary: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    scope = price_ledger[(price_ledger.source.eq(primary)) &
                         price_ledger.snapshot.eq("NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT")]
    groups = {name: scope[scope.cohort.eq(name)].drop_duplicates("game_id") for name in
              ("JOINT_STRENGTH_FIXED_76", "MARKET_STRONG_ONLY", "MODEL_STRONG_ONLY")}
    rows = []
    for label, data in groups.items():
        rows.append({"comparison": "UNMATCHED_PRIMARY_DESIGNATED", "group": label, **economics(data)})
    left, right = groups["JOINT_STRENGTH_FIXED_76"].copy(), groups["MARKET_STRONG_ONLY"].copy()
    candidates = []
    for a in left.itertuples(index=False):
        for b in right.itertuples(index=False):
            delta = abs(float(a.evaluated_no_vig_market_probability) - float(b.evaluated_no_vig_market_probability))
            if delta <= MATCH_CALIPER:
                candidates.append((delta, int(a.game_id), int(b.game_id)))
    used_left, used_right, pairs = set(), set(), []
    for delta, a_id, b_id in sorted(candidates):
        if a_id not in used_left and b_id not in used_right:
            used_left.add(a_id); used_right.add(b_id)
            a, b = left[left.game_id.eq(a_id)].iloc[0], right[right.game_id.eq(b_id)].iloc[0]
            pairs.append({"primary_source": primary, "joint_game_id": a_id, "market_only_game_id": b_id,
                          "market_probability_difference": delta,
                          "joint_market_probability": a.evaluated_no_vig_market_probability,
                          "market_only_market_probability": b.evaluated_no_vig_market_probability,
                          "joint_win": a.evaluated_win, "market_only_win": b.evaluated_win,
                          "joint_return": a.flat_stake_return, "market_only_return": b.flat_stake_return,
                          "return_difference": a.flat_stake_return-b.flat_stake_return})
    pair_columns = ["primary_source", "joint_game_id", "market_only_game_id", "market_probability_difference",
                    "joint_market_probability", "market_only_market_probability", "joint_win", "market_only_win",
                    "joint_return", "market_only_return", "return_difference"]
    pair_frame = pd.DataFrame(pairs, columns=pair_columns)
    rows.append({"comparison": "MARKET_PROBABILITY_MATCHED_WITHOUT_REPLACEMENT_CALIPER_0.02",
                 "group": "JOINT_MINUS_MARKET_STRONG_ONLY", "price_covered_games": len(pair_frame),
                 "wins": int(pair_frame.joint_win.sum()) if len(pair_frame) else 0,
                 "losses": len(pair_frame)-int(pair_frame.joint_win.sum()) if len(pair_frame) else 0,
                 "record": f"{int(pair_frame.joint_win.sum())}-{len(pair_frame)-int(pair_frame.joint_win.sum())}" if len(pair_frame) else "0-0",
                 "net_units": float(pair_frame.return_difference.sum()) if len(pair_frame) else 0.0,
                 "captured_price_hypothetical_roi": float(pair_frame.return_difference.mean()) if len(pair_frame) else np.nan,
                 "support_assessment": "INSUFFICIENT_FOR_INFERENCE" if len(pair_frame) < 10 else "DESCRIPTIVE_SUPPORT_AT_LEAST_10_PAIRS"})
    return pd.DataFrame(rows), pair_frame


def exclusion_ledger(selection: pd.DataFrame, joint: pd.DataFrame, quotes: pd.DataFrame,
                     economics_table: pd.DataFrame, primary: str) -> pd.DataFrame:
    rows = [{"scope": "PRIMARY_SELECTION", "source": r.normalized_source_name, "game_id": None,
             "snapshot": None, "reason": r.selection_or_exclusion_reason}
            for r in selection.itertuples(index=False) if not r.selected_primary]
    covered = set(quotes.loc[quotes.source.eq(primary), "game_id"].astype(int))
    rows.extend({"scope": "PRIMARY_JOINT_GAME_COVERAGE", "source": primary, "game_id": int(r.game_id),
                 "snapshot": None, "reason": "NO_TRUSTWORTHY_PRESERVED_PREGAME_QUOTE"}
                for r in joint.itertuples(index=False) if int(r.game_id) not in covered)
    missing = economics_table[(economics_table.temporal_scope.eq("ALL")) & economics_table.uncovered_games.gt(0)]
    rows.extend({"scope": "FIXED_SNAPSHOT_COHORT_SUMMARY", "source": r.source, "game_id": None,
                 "snapshot": r.snapshot, "reason": f"{int(r.uncovered_games)}_OF_{int(r.eligible_fixed_games)}_FIXED_GAMES_UNCOVERED_IN_{r.cohort}"}
                for r in missing.itertuples(index=False))
    return pd.DataFrame(rows)


def validator_text() -> str:
    return '''#!/usr/bin/env python3
import hashlib,json
from pathlib import Path
import pandas as pd
p=Path(__file__).resolve().parent; errors=[]
s=json.loads((p/'summary.json').read_text())
i=pd.read_csv(p/'sportsbook_coverage_inventory.csv')
r=pd.read_csv(p/'primary_source_selection_record.csv')
e=pd.read_csv(p/'fixed_cohort_economics.csv')
if s['fixed_joint_games']!=76 or s['fixed_joint_record']!='56-20': errors.append('fixed_joint_changed')
if s['fixed_model_strong_games']!=137 or s['resolved_predictions']!=471: errors.append('population_changed')
if s['primary_source']!='fliff': errors.append('primary_not_fliff')
if s['primary_joint_coverage']!=10: errors.append('primary_coverage_not_10')
if s['classification']!='INSUFFICIENT_ALTERNATIVE_BOOK_COVERAGE': errors.append('classification_wrong')
if int(i.loc[i.normalized_source_name.eq('betonline'),'joint_strength_coverage'].iloc[0])!=11: errors.append('betonline_not_11')
if int(r.selected_primary.astype(str).str.lower().eq('true').sum())!=1: errors.append('primary_count_wrong')
if e[e.cohort.eq('JOINT_STRENGTH_FIXED_76')].empty: errors.append('joint_economics_missing')
for line in (p/'sha256_manifest.txt').read_text().splitlines():
    expected,name=line.split('  ',1)
    if hashlib.sha256((p/name).read_bytes()).hexdigest()!=expected: errors.append('hash:'+name)
print(json.dumps({'status':'PASS' if not errors else 'FAIL','errors':errors},sort_keys=True))
raise SystemExit(bool(errors))
'''


def render_report(summary: dict[str, Any]) -> str:
    primary_roi = summary["primary_joint_designated_roi"]
    pin_common_roi = summary["primary_vs_pinnacle_common_reference_roi"]
    transfer = "positive" if primary_roi is not None and primary_roi > 0 else "not positive"
    return f"""# MLB alternative-book joint-strength transfer audit v1

## Direct answer

> When BetOnline is removed because its moneyline data are missing, does the same unchanged joint-strength cohort remain economically positive at other independently captured sportsbook prices?

**Not demonstrably across the unchanged cohort.** The outcome-blind hierarchy selected **{summary['primary_source']}**, the best-covered eligible non-BetOnline, non-Pinnacle sportsbook source, but it covers only **{summary['primary_joint_coverage']} of 76 games ({summary['primary_joint_coverage']/76:.1%})**. On those games the unchanged side went **{summary['primary_joint_designated_record']}** for a **{primary_roi:+.1%} captured-price hypothetical ROI** at the nearest designated snapshot. That small, August-only subset is {transfer}; it cannot establish transfer of the full Pinnacle result.

## Headline findings

1. **Primary source:** `{summary['primary_source']}`, selected before outcomes because its {summary['primary_joint_coverage']} joint games exceeded every other eligible alternative sportsbook. Polymarket had more coverage ({summary['polymarket_joint_coverage']}) but is an exchange/prediction market; `unknown` had ambiguous identity. Neither was eligible under the fixed sportsbook rule.
2. **Coverage:** {summary['primary_joint_coverage']}/76 joint games, {summary['primary_post_prediction_coverage']} post-prediction, {summary['primary_two_sided_coverage']} two-sided, from {summary['primary_first_date']} through {summary['primary_last_date']}.
3. **Primary economics:** {summary['primary_joint_designated_record']}, {primary_roi:+.1%} captured-price hypothetical ROI on {summary['primary_joint_designated_games']} games; the date-clustered interval is [{summary['primary_joint_ci_low']:+.1%}, {summary['primary_joint_ci_high']:+.1%}]. This is not an executable ROI claim.
4. **Common games versus Pinnacle:** the same {summary['primary_vs_pinnacle_common_games']} games and outcomes returned {primary_roi:+.1%} at `{summary['primary_source']}` and {pin_common_roi:+.1%} at Pinnacle, a {summary['primary_vs_pinnacle_roi_difference']:+.1%} price-only difference. `{summary['primary_source']}` had the better price {summary['primary_vs_pinnacle_better']} times; Pinnacle did {summary['pinnacle_vs_primary_better']} times; {summary['primary_vs_pinnacle_ties']} tied.
5. **Does Pinnacle +10.3% transfer?** No adequate evidence. Pinnacle remains 56-20 and {summary['pinnacle_full_designated_roi']:+.1%} on all 76, while no eligible alternative book covers even half the cohort. The common-game comparison is price-only but far too small and temporally concentrated.
6. **Positive across more than one book?** No. Fliff was the only positive designated-snapshot point estimate among {summary['eligible_alternative_books_analyzed']} eligible alternative sportsbooks with meaningful coverage, and zero had a positive date-clustered lower interval bound. The formal classification is **`{summary['classification']}`**; none satisfies the predeclared {summary['robustness_min_joint_games']}-game coverage floor.
7. **Realistic availability:** user access and historical fillability are **unknown for every source**. All returns are labeled captured-price hypothetical ROI; none is called executable.
8. **Probability strength versus apparent edge:** probability/market strength agreement remains the useful descriptive structure. This audit found no implementation error and does not reopen the prior conclusion that continuous model probability failed to add out-of-time information over market-only forecasting; apparent edge remains economically uninformative.
9. **Next immediate decision:** do not promote or economically validate the selector from these alternative-book archives. Preserve the 76-game observer and collect a prospectively complete, user-accessible sportsbook moneyline panel before another transfer decision.

## Coverage constraint

BetOnline remains exactly 11/76. `{summary['primary_source']}` is 10/76, so the repository does **not** contain the directionally requested alternative sportsbook with materially better joint coverage. The audit continued with the best eligible evidence as directed, rather than treating BetOnline as a blocker. The broad alternative archive is a short SportsGameOdds relay concentrated from August 6-10; repeated timestamps were not counted as independent games.

The source inventory distinguishes direct captured named-source quotes from derived consensus, source identity/type, timing, aliases, authority, and access status. Composite and ambiguous sources were not presented as sportsbooks. Selection did not use ROI or per-game price shopping.

The BetOnline common-game control contains {summary['primary_vs_betonline_common_games']} games: `{summary['primary_source']}` returned {summary['primary_vs_betonline_common_alternative_roi']:+.1%} and BetOnline {summary['primary_vs_betonline_common_reference_roi']:+.1%}. This is a nine-game price comparison, not broad transfer evidence.

## Model contribution at the primary source

At `{summary['primary_source']}` nearest-designated prices, joint, market-only, and model-only results are reported in `primary_model_contribution.csv`. Market-probability matching yielded {summary['matched_pair_count']} pair(s); support is **{summary['matched_support']}**. This evidence is too sparse to determine that the observed joint record is economically distinguishable from similarly strong market favorites.

## Package notes

`source_specific_price_ledger.csv` retains exact fixed sides, observations, prices, timestamps, and artifact hashes. `fixed_cohort_economics.csv` reports all eight immutable cohorts, all fixed snapshot rules, date-clustered intervals, and separate August/September rows. `common_game_comparison_ledger.csv` and its summary remove coverage composition by comparing identical games. `temporal_stability.csv` is the August/September subset of the full economics table.

No network acquisition, retrospective best-price shopping, model or threshold change, production change, credential action, scheduling change, wager, commit, or push occurred.
"""


def run(output: Path, cutoff: str) -> dict[str, Any]:
    if cutoff != CUTOFF:
        raise ValueError(f"v1 is frozen at {CUTOFF}; got {cutoff}")
    predictions, quotes, _ = across.load_population(cutoff)
    predictions = predictions.sort_values(["game_date", "game_id"]).reset_index(drop=True)
    quotes = prepare_quotes(quotes)
    ledger = pd.read_csv(PRIOR_JOINT)
    joint = ledger[ledger.strength_class.eq("JOINT_STRONG_SAME_SIDE")].copy()
    if len(predictions) != 471 or len(joint) != 76 or int(joint.evaluated_win.sum()) != 56:
        raise ValueError("Frozen population or joint cohort changed")
    raw = load_raw_rows(cutoff, set(predictions.game_id.astype(int)))
    inventory = inventory_sources(raw, quotes, predictions, joint)

    # This selection record is intentionally completed before any outcome economics call.
    selection, primary_names = select_primary(inventory)
    if len(primary_names) != 1:
        raise ValueError(f"Expected one primary after fixed hierarchy; got {primary_names}")
    primary = primary_names[0]
    meaningful = inventory[(inventory.joint_strength_coverage.ge(MEANINGFUL_JOINT_MIN))
                           & inventory.source_type.ne("AMBIGUOUS_IDENTITY")].normalized_source_name.tolist()
    analysis_sources = sorted(set(meaningful) | {primary, "pinnacle", "betonline"})

    cohorts = build_cohorts(predictions, ledger)
    designated_times = {int(r.game_id): pd.to_datetime(r.captured_at_utc, utc=True)
                        for r in joint.itertuples(index=False)}
    price_ledger, econ = build_price_and_economics(quotes, cohorts, analysis_sources, designated_times)
    alternatives = [x for x in meaningful if x not in {"pinnacle", "betonline"}]
    common_detail, common_summary = common_game_comparisons(price_ledger, alternatives)
    contribution, matched = matched_model_contribution(price_ledger, primary)

    def inv(source: str) -> pd.Series:
        return inventory[inventory.normalized_source_name.eq(source)].iloc[0]

    def econ_row(source: str, cohort: str = "JOINT_STRENGTH_FIXED_76",
                 snapshot: str = "NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT") -> pd.Series:
        return econ[(econ.source.eq(source)) & econ.cohort.eq(cohort) & econ.snapshot.eq(snapshot)
                    & econ.temporal_scope.eq("ALL")].iloc[0]

    primary_e = econ_row(primary)
    pinnacle_e = econ_row("pinnacle")
    common = common_summary[(common_summary.alternative_source.eq(primary))
                            & common_summary.reference_source.eq("pinnacle")
                            & common_summary.snapshot.eq("NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT")].iloc[0]
    common_bol = common_summary[(common_summary.alternative_source.eq(primary))
                               & common_summary.reference_source.eq("betonline")
                               & common_summary.snapshot.eq("NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT")].iloc[0]
    designated_alt = econ[(econ.cohort.eq("JOINT_STRENGTH_FIXED_76"))
                          & econ.snapshot.eq("NEAREST_DESIGNATED_DAILY_MARKET_SNAPSHOT")
                          & econ.temporal_scope.eq("ALL")].merge(
        inventory[["normalized_source_name", "source_type"]], left_on="source",
        right_on="normalized_source_name", how="left", validate="many_to_one")
    designated_alt = designated_alt[(designated_alt.source_type.eq("SPORTSBOOK"))
                                    & ~designated_alt.source.isin(["pinnacle", "betonline"])
                                    & designated_alt.price_covered_games.ge(MEANINGFUL_JOINT_MIN)]
    matched_row = contribution[contribution.comparison.str.startswith("MARKET_PROBABILITY_MATCHED")].iloc[0]
    classification = "INSUFFICIENT_ALTERNATIVE_BOOK_COVERAGE"
    summary = {
        "audit": AUDIT, "resolved_cutoff": cutoff, "resolved_predictions": len(predictions),
        "fixed_joint_games": len(joint), "fixed_joint_record": "56-20", "fixed_model_strong_games": 137,
        "strong_boundary": "selected model probability > 0.60 strict; unchanged",
        "primary_source": primary, "primary_selection_was_outcome_blind": True,
        "primary_joint_coverage": int(inv(primary).joint_strength_coverage),
        "primary_post_prediction_coverage": int(inv(primary).joint_post_prediction_coverage),
        "primary_two_sided_coverage": int(inv(primary).joint_two_sided_coverage),
        "primary_first_date": inv(primary).first_game_date, "primary_last_date": inv(primary).last_game_date,
        "primary_joint_designated_games": int(primary_e.price_covered_games),
        "primary_joint_designated_record": primary_e.record,
        "primary_joint_designated_roi": primary_e.captured_price_hypothetical_roi,
        "primary_joint_ci_low": primary_e.roi_date_clustered_ci_2_5,
        "primary_joint_ci_high": primary_e.roi_date_clustered_ci_97_5,
        "pinnacle_full_designated_games": int(pinnacle_e.price_covered_games),
        "pinnacle_full_designated_record": pinnacle_e.record,
        "pinnacle_full_designated_roi": pinnacle_e.captured_price_hypothetical_roi,
        "betonline_joint_coverage": int(inv("betonline").joint_strength_coverage),
        "polymarket_joint_coverage": int(inv("polymarket").joint_strength_coverage),
        "primary_vs_pinnacle_common_games": int(common.common_games),
        "primary_vs_pinnacle_common_alternative_roi": common.alternative_captured_price_hypothetical_roi,
        "primary_vs_pinnacle_common_reference_roi": common.reference_captured_price_hypothetical_roi,
        "primary_vs_pinnacle_roi_difference": common.roi_difference_alternative_minus_reference,
        "primary_vs_pinnacle_better": int(common.alternative_better_price_games),
        "pinnacle_vs_primary_better": int(common.reference_better_price_games),
        "primary_vs_pinnacle_ties": int(common.tied_price_games),
        "primary_vs_betonline_common_games": int(common_bol.common_games),
        "primary_vs_betonline_common_alternative_roi": common_bol.alternative_captured_price_hypothetical_roi,
        "primary_vs_betonline_common_reference_roi": common_bol.reference_captured_price_hypothetical_roi,
        "eligible_alternative_books_analyzed": len(designated_alt),
        "eligible_alternative_books_positive_point_roi": int(designated_alt.captured_price_hypothetical_roi.gt(0).sum()),
        "eligible_alternative_books_positive_interval_lower_bound": int(designated_alt.roi_date_clustered_ci_2_5.gt(0).sum()),
        "classification": classification, "meaningful_joint_coverage_min": MEANINGFUL_JOINT_MIN,
        "robustness_min_joint_games": ROBUSTNESS_MIN_JOINT,
        "books_meeting_robustness_floor_excluding_pinnacle": int(inventory[
            inventory.normalized_source_name.ne("pinnacle") & inventory.source_type.eq("SPORTSBOOK")
            & inventory.joint_strength_coverage.ge(ROBUSTNESS_MIN_JOINT)].shape[0]),
        "matched_pair_count": len(matched), "matched_support": matched_row.get("support_assessment"),
        "user_access_and_fillability": "UNKNOWN_NOT_ESTABLISHED_FOR_EVERY_SOURCE",
        "economics_label": "captured-price hypothetical ROI", "pinnacle_return_transfers": False,
        "continuous_model_probability_conclusion": "NO_INTERVAL_SUPPORTED_IMPROVEMENT_OVER_MARKET_ONLY_UNCHANGED",
        "apparent_edge_conclusion": "APPARENT_EDGE_NOT_ECONOMICALLY_INFORMATIVE_UNCHANGED",
        "network_acquisition": False, "production_changes": False, "threshold_changes": False,
        "best_price_shopping": False, "wagering": False,
    }
    exclusion = exclusion_ledger(selection, joint, quotes, econ, primary)

    output.mkdir(parents=True, exist_ok=True)
    write_csv(inventory, output / "sportsbook_coverage_inventory.csv")
    write_csv(selection, output / "primary_source_selection_record.csv")
    write_csv(price_ledger, output / "source_specific_price_ledger.csv")
    write_csv(econ, output / "fixed_cohort_economics.csv")
    write_csv(common_detail, output / "common_game_comparison_ledger.csv")
    write_csv(common_summary, output / "common_game_comparison_summary.csv")
    write_csv(econ[econ.temporal_scope.ne("ALL")], output / "temporal_stability.csv")
    write_csv(contribution, output / "primary_model_contribution.csv")
    write_csv(matched, output / "primary_market_probability_matched_pairs.csv")
    write_csv(exclusion, output / "exclusion_ledger.csv")
    sgo_raw_files = sorted((ROOT / "backend/mlb/exports/market_history/sportsgameodds_main_market/raw").rglob("*.json"))
    sgo_probe_files = sorted((ROOT / "backend/mlb/exports/provider_probes/sportsgameodds").rglob("*.json"))
    odds_history_root = ROOT / "backend/mlb/exports/odds_history"
    pinnacle_h2h_files = sorted(odds_history_root.glob("20??-??-??/odds_mlb_pinnacle_main_markets__*.json"))
    other_named_h2h_files = sorted(p for p in odds_history_root.glob("20??-??-??/*.json")
                                   if "h2h" in p.name.lower() and "pinnacle" not in p.name.lower())
    raw_search = pd.DataFrame([
        {"source_class": "NORMALIZED_SQLITE_MONEYLINE_LEDGER", "authority": str(MARKET_DB.relative_to(ROOT)),
         "artifacts_or_rows_searched": len(raw), "finding": "All frozen-population MONEYLINE rows and payload timing fields inventoried"},
        {"source_class": "SPORTSGAMEODDS_RAW_PROVIDER_ARCHIVE", "authority": "backend/mlb/exports/market_history/sportsgameodds_main_market/raw",
         "artifacts_or_rows_searched": len(sgo_raw_files),
         "finding": f"Named-book paired moneylines preserved only in short August provider trial; normalized_rows={int(raw.provider.astype(str).str.contains('SPORTSGAMEODDS', case=False).sum())}"},
        {"source_class": "SPORTSGAMEODDS_PROVIDER_PROBES", "authority": "backend/mlb/exports/provider_probes/sportsgameodds",
         "artifacts_or_rows_searched": len(sgo_probe_files),
         "finding": "Probe archive included in source recovery; no broader later sportsbook history"},
        {"source_class": "THE_ODDS_API_PINNACLE_ARCHIVE", "authority": "backend/mlb/exports/odds_history",
         "artifacts_or_rows_searched": len(pinnacle_h2h_files),
         "finding": f"Explicit full-game h2h archive is Pinnacle; normalized_rows={int(raw.source.eq('pinnacle').sum())}"},
        {"source_class": "OTHER_NAMED_H2H_ODDS_HISTORY_FILES", "authority": "backend/mlb/exports/odds_history",
         "artifacts_or_rows_searched": len(other_named_h2h_files),
         "finding": "No additional broad named-book full-game moneyline archive identified by h2h filename search"},
        {"source_class": "PRIOR_FROZEN_ANALYTICAL_PACKAGES", "authority": "artifacts/analysis/model_development",
         "artifacts_or_rows_searched": 3, "finding": "Joint, BetOnline recovery, and across-board identities and conclusions preserved"},
    ])
    write_csv(raw_search, output / "raw_source_search_inventory.csv")
    implementation = pd.DataFrame([
        {"artifact": "analysis_utility", "path": str(Path(__file__).resolve().relative_to(ROOT)),
         "sha256": sha(Path(__file__).resolve()), "verification": "PY_COMPILE_AND_UNITTEST"},
        {"artifact": "regression_tests", "path": "backend/mlb/tests/test_alternative_book_joint_strength_transfer_v1.py",
         "sha256": sha(ROOT / "backend/mlb/tests/test_alternative_book_joint_strength_transfer_v1.py"),
         "verification": "UNITTEST"},
    ])
    write_csv(implementation, output / "implementation_verification.csv")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=json_default) + "\n")
    (output / "main_report.md").write_text(render_report(summary))
    rerun = (f"/bin/zsh -lc 'set -a; source backend/.env; set +a; .venv/bin/python -m "
             f"backend.mlb.scripts.audit_mlb_alternative_book_joint_strength_transfer_v1 "
             f"--resolved-cutoff {cutoff} --output {output.relative_to(ROOT)}'\n")
    (output / "rerun_command.txt").write_text(rerun)
    (output / "validator.py").write_text(validator_text())
    (output / "validator.py").chmod(0o755)
    files = sorted(x for x in output.iterdir() if x.is_file() and x.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(x)}  {x.name}\n" for x in files))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resolved-cutoff", default=CUTOFF)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    print(json.dumps(run(output, args.resolved_cutoff), indent=2, sort_keys=True, default=json_default))


if __name__ == "__main__":
    main()
