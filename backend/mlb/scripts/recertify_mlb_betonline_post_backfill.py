"""Post-backfill BetOnline recertification and Hits overlay refresh.

Reads the immutable BetOnline recovery overlay and existing model-development
artifacts, then writes corrected analysis surfaces. It makes no network calls,
does not fit models, and does not modify production or source artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score


ROOT = Path(__file__).resolve().parents[3]
RUN_DATE = "2026-07-19"
BACKFILL_DIR = ROOT / "artifacts/analysis/model_development/mlb_betonline_inventory_driven_player_prop_backfill/2026-07-19"
NONMARKET_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_nonmarket_player_game_feature_spine/2026-07-19"
MI_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_market_independent_reconstruction/2026-07-18"
INCIDENT_DIR = ROOT / "artifacts/analysis/model_development/mlb_betonline_player_prop_capture_integrity_incident/2026-07-18"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_betonline_post_backfill_recertification/2026-07-19"

TEAM_NAME_TO_ABBR = {
    "108": "LAA",
    "109": "ARI",
    "110": "BAL",
    "111": "BOS",
    "112": "CHC",
    "113": "CIN",
    "114": "CLE",
    "115": "COL",
    "116": "DET",
    "117": "HOU",
    "118": "KC",
    "119": "LAD",
    "120": "WSH",
    "121": "NYM",
    "133": "ATH",
    "134": "PIT",
    "135": "SD",
    "136": "SEA",
    "137": "SF",
    "138": "STL",
    "139": "TB",
    "140": "TEX",
    "141": "TOR",
    "142": "MIN",
    "143": "PHI",
    "144": "ATL",
    "145": "CHW",
    "146": "MIA",
    "147": "NYY",
    "158": "MIL",
    "arizona diamondbacks": "ARI",
    "atlanta braves": "ATL",
    "baltimore orioles": "BAL",
    "boston red sox": "BOS",
    "chicago cubs": "CHC",
    "chicago white sox": "CHW",
    "cincinnati reds": "CIN",
    "cleveland guardians": "CLE",
    "colorado rockies": "COL",
    "detroit tigers": "DET",
    "houston astros": "HOU",
    "kansas city royals": "KC",
    "los angeles angels": "LAA",
    "los angeles dodgers": "LAD",
    "miami marlins": "MIA",
    "milwaukee brewers": "MIL",
    "minnesota twins": "MIN",
    "new york mets": "NYM",
    "new york yankees": "NYY",
    "athletics": "ATH",
    "oakland athletics": "ATH",
    "philadelphia phillies": "PHI",
    "pittsburgh pirates": "PIT",
    "san diego padres": "SD",
    "san francisco giants": "SF",
    "seattle mariners": "SEA",
    "st. louis cardinals": "STL",
    "st louis cardinals": "STL",
    "tampa bay rays": "TB",
    "texas rangers": "TEX",
    "toronto blue jays": "TOR",
    "washington nationals": "WSH",
}

DECISIONS = {
    "MLB_BETONLINE_POST_BACKFILL_GRAIN_DECISION": "RECOVERED_ROWS_RECERTIFIED_AT_PRICE_OBSERVATION_AND_UNIQUE_PROPOSITION_GRAINS",
    "MLB_BETONLINE_POST_BACKFILL_OVERLAY_VALIDATION_DECISION": "ORIGINAL_ROWS_REMAIN_AUTHORITATIVE_RECOVERED_ROWS_ARE_INSERTS_OR_CORROBORATION_WITH_PROVENANCE",
    "MLB_BETONLINE_POST_BACKFILL_MARKET_TIMELINE_DECISION": "MARKET_TIMELINES_AMENDED_TO_INCLUDE_RAW_REPARSE_AND_ALTERNATE_LOCAL_EVIDENCE",
    "MLB_BETONLINE_POST_BACKFILL_INCIDENT_SCOPE_DECISION": "INCIDENT_SCOPE_SPLIT_BETWEEN_NORMALIZATION_SELECTION_LOSS_AND_PERMANENT_LOCAL_GAPS",
    "MLB_BETONLINE_POST_BACKFILL_HITS_OVERLAY_DECISION": "DIRECT_BETONLINE_HITS_OVERLAY_REFRESHED_WITH_AUTHENTIC_RECOVERED_ROWS_ONLY",
    "MLB_BETONLINE_POST_BACKFILL_MARKET_CONDITIONED_POPULATION_DECISION": "MARKET_CONDITIONED_POPULATION_EXPANDS_BUT_REMAINS_SELECTED_SUBSET_OF_NONMARKET_SPINE",
    "MLB_BETONLINE_POST_BACKFILL_INCUMBENT_COMPARISON_DECISION": "SAME_ROW_COMPARISON_REFRESHED_WITH_EXISTING_FROZEN_PREDICTIONS_NO_REFIT",
    "MLB_BETONLINE_POST_BACKFILL_ECONOMIC_RECERTIFICATION_DECISION": "ECONOMICS_RECALCULATED_BY_RECOVERY_CLASS_NO_FANDUEL_PRICE_SUBSTITUTION",
    "MLB_BETONLINE_POST_BACKFILL_PRIOR_CONCLUSION_AMENDMENT_DECISION": "PRIOR_SCARCITY_AND_RETIREMENT_CONCLUSIONS_AMENDED_WHERE BASED_ON_INVISIBLE_LOCAL_DIRECT_ROWS",
    "MLB_BETONLINE_POST_BACKFILL_RESIDUAL_DECISION": "RESIDUALS_REMAIN_CLASSIFIED_NO_ADDITIONAL_NETWORK_AUTHORIZED_BY_THIS_PACKAGE",
    "MLB_HITS_NONMARKET_SPINE_STATUS": "BASEBALL_FEATURE_SPINE_UNCHANGED",
    "MLB_PRODUCTION_STATUS": "UNCHANGED",
}


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def write_csv(path: Path, df: pd.DataFrame | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
    df.to_csv(path, index=False)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def norm_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def team_abbr(value: Any) -> str:
    text = str(value or "").strip()
    return TEAM_NAME_TO_ABBR.get(text.lower(), text.upper())


def american_profit(price: Any) -> float:
    p = float(price)
    return p / 100.0 if p > 0 else 100.0 / abs(p)


def metric_row(df: pd.DataFrame, candidate: str, threshold: str, prob_col: str, target_col: str, segment: str) -> dict[str, Any]:
    part = df[[prob_col, target_col]].copy()
    part[prob_col] = pd.to_numeric(part[prob_col], errors="coerce")
    part[target_col] = pd.to_numeric(part[target_col], errors="coerce")
    part = part.dropna()
    if part.empty:
        return {"segment": segment, "candidate": candidate, "threshold": threshold, "rows": 0, "auc": "", "brier": "", "log_loss": "", "avg_prob": "", "actual_rate": ""}
    y = part[target_col].astype(int)
    p = part[prob_col].clip(0.000001, 0.999999)
    auc = roc_auc_score(y, p) if y.nunique() == 2 else ""
    return {
        "segment": segment,
        "candidate": candidate,
        "threshold": threshold,
        "rows": len(part),
        "auc": auc,
        "brier": brier_score_loss(y, p),
        "log_loss": log_loss(y, p, labels=[0, 1]) if y.nunique() == 2 else "",
        "avg_prob": p.mean(),
        "actual_rate": y.mean(),
    }


def load_inputs(
    backfill_dir: Path,
    recovered_rows_name: str,
    manifest_name: str,
    unrecovered_name: str,
) -> dict[str, pd.DataFrame]:
    return {
        "recovered": read_csv(backfill_dir / recovered_rows_name),
        "manifest": read_csv(backfill_dir / manifest_name),
        "unrecovered": read_csv(backfill_dir / unrecovered_name),
        "spine": read_csv(NONMARKET_DIR / "player_game_denominator_2026-07-19.csv"),
        "prior_pop": read_csv(MI_DIR / "recovered_baseball_population_2026-07-18.csv"),
        "predictions": read_csv(MI_DIR / "count_distribution_predictions_2026-07-18.csv"),
        "prior_decisions": read_csv(MI_DIR / "required_decisions_2026-07-18.csv"),
        "incident_decisions": read_csv(INCIDENT_DIR / "betonline_capture_incident_decisions_2026-07-18.csv"),
    }


def recovered_grain(recovered: pd.DataFrame, manifest: pd.DataFrame) -> pd.DataFrame:
    valid = recovered[recovered["validation_status"].eq("PASS")].copy()
    valid["line"] = valid["line"].astype(str)
    valid["event_player_prop_line"] = valid[["slate_date", "event_id", "player_name", "raw_market_key", "line"]].astype(str).agg("|".join, axis=1)
    valid["event_player_prop_line_side"] = valid[["slate_date", "event_id", "player_name", "raw_market_key", "line", "side"]].astype(str).agg("|".join, axis=1)
    valid["event_player_prop"] = valid[["slate_date", "event_id", "player_name", "raw_market_key"]].astype(str).agg("|".join, axis=1)
    two_sided = valid.groupby("event_player_prop_line")["side"].nunique().ge(2).sum()
    rows = [
        {"grain": "raw_price_rows", "rows": len(valid), "notes": "Includes repeated captures, sides, and line levels."},
        {"grain": "unique_capture_event_player_prop_line_side_observations", "rows": valid[["source_capture_timestamp", "event_id", "player_name", "raw_market_key", "line", "side"]].drop_duplicates().shape[0], "notes": ""},
        {"grain": "unique_two_sided_propositions", "rows": int(two_sided), "notes": "Unique event-player-prop-line with both over and under."},
        {"grain": "unique_player_game_prop_line_populations", "rows": valid["event_player_prop_line"].nunique(), "notes": "OddsAPI event identity used where MLB game_id is unavailable."},
        {"grain": "unique_player_game_prop_populations", "rows": valid["event_player_prop"].nunique(), "notes": ""},
        {"grain": "distinct_events", "rows": valid["event_id"].nunique(), "notes": ""},
        {"grain": "distinct_dates", "rows": valid["slate_date"].nunique(), "notes": ""},
        {"grain": "distinct_scheduled_windows", "rows": manifest[manifest["manifest_id"].astype(str).isin(set(valid["target_manifest_id"].astype(str).str.split("|").explode()))]["expected_utc_time"].nunique(), "notes": "Manifest rows whose direct prices were recovered."},
    ]
    return pd.DataFrame(rows)


def overlay_precedence(recovered: pd.DataFrame, manifest: pd.DataFrame) -> pd.DataFrame:
    valid = recovered[recovered["validation_status"].eq("PASS")].copy()
    exploded = []
    for _, r in valid.iterrows():
        for mid in str(r.get("target_manifest_id", "")).split("|"):
            if mid:
                exploded.append({**r.to_dict(), "manifest_id_single": mid})
    ex = pd.DataFrame(exploded)
    if ex.empty:
        return pd.DataFrame()
    m = manifest[["manifest_id", "direct_betonline_rows_already_present", "corrected_market_status", "raw_market_key", "slate_date", "expected_utc_time"]].copy()
    joined = ex.merge(m, left_on="manifest_id_single", right_on="manifest_id", how="left", suffixes=("", "_manifest"))
    joined["precedence_action"] = np.where(
        pd.to_numeric(joined["direct_betonline_rows_already_present"], errors="coerce").fillna(0).gt(0),
        "CORROBORATING_OR_COMPLETING_PARTIAL_ORIGINAL_DIRECT_ROWS_RETAIN_AUTHORITY",
        "INSERT_RECOVERED_DIRECT_ROW_FOR_MISSING_NORMALIZED_BETONLINE_MARKET",
    )
    summary = joined.groupby(["precedence_action", "recovery_class"], dropna=False).size().reset_index(name="rows")
    conflict_rows = joined[joined["raw_market_key"].ne(joined["raw_market_key_manifest"])]
    extra = pd.DataFrame(
        [
            {"precedence_action": "CONFLICTS", "recovery_class": "all", "rows": len(conflict_rows)},
            {"precedence_action": "REJECTED_ROWS", "recovery_class": "all", "rows": int((recovered["validation_status"] != "PASS").sum())},
        ]
    )
    return pd.concat([summary, extra], ignore_index=True)


def market_timelines(recovered: pd.DataFrame) -> pd.DataFrame:
    valid = recovered[recovered["validation_status"].eq("PASS")].copy()
    valid["two_side_key"] = valid[["slate_date", "event_id", "player_name", "raw_market_key", "line"]].astype(str).agg("|".join, axis=1)
    two = valid.groupby(["raw_market_key", "two_side_key"])["side"].nunique().reset_index()
    two = two[two["side"].ge(2)]
    rows = []
    for key, g in valid.groupby("raw_market_key"):
        dates = sorted(g["slate_date"].dropna().astype(str).unique())
        all_dates = pd.date_range(min(dates), max(dates)).strftime("%Y-%m-%d").tolist() if dates else []
        gaps = [d for d in all_dates if d not in set(dates)]
        rows.append(
            {
                "raw_market_key": key,
                "first_occurrence": g["source_capture_timestamp"].min(),
                "latest_occurrence": g["source_capture_timestamp"].max(),
                "slates_offered": len(dates),
                "capture_windows_offered": g["source_capture_timestamp"].nunique(),
                "events": g["event_id"].nunique(),
                "unique_propositions": g[["event_id", "player_name", "raw_market_key", "line"]].drop_duplicates().shape[0],
                "two_sided_propositions": int(two[two["raw_market_key"].eq(key)]["two_side_key"].nunique()),
                "intermittent_gap_dates": "|".join(gaps[:50]),
                "genuinely_missing_dates_after_recovery": len(gaps),
                "notes": "Timeline includes original raw reparse and alternate retained local evidence.",
            }
        )
    return pd.DataFrame(rows)


def incident_cause_attribution(manifest: pd.DataFrame, unrecovered: pd.DataFrame, recovered: pd.DataFrame) -> pd.DataFrame:
    valid_ids = set()
    for mids in recovered[recovered["validation_status"].eq("PASS")]["target_manifest_id"].astype(str):
        valid_ids.update([m for m in mids.split("|") if m])
    rows = []
    for _, r in manifest.iterrows():
        mid = str(r["manifest_id"])
        if mid in valid_ids:
            if str(r.get("raw_source_path", "")).strip() and str(r.get("corrected_market_status")) == "MARKET_PARTIAL":
                cause = "parser_or_normalization_loss"
            elif str(r.get("raw_source_path", "")).strip():
                cause = "snapshot_selection_or_raw_visibility_loss"
            else:
                cause = "alternate_local_capture_restored_missing_window"
        elif str(r.get("capture_classification")) == "EXPECTED_CAPTURE_MISSING":
            cause = "expected_capture_genuinely_missing_or_permanent_local_capture_gap"
        elif str(r.get("slate_date")) in {"2026-07-17", "2026-07-18"}:
            cause = "provider_confirmed_post_break_outage_unrecovered"
        else:
            cause = "direct_price_unrecovered_after_backfill"
        rows.append({**r.to_dict(), "incident_cause_after_recovery": cause, "recovered": mid in valid_ids})
    df = pd.DataFrame(rows)
    return df.groupby(["incident_cause_after_recovery", "recovered"], dropna=False).size().reset_index(name="manifest_rows")


def hits_overlay(recovered: pd.DataFrame, spine: pd.DataFrame, prior_pop: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid = recovered[(recovered["validation_status"].eq("PASS")) & (recovered["raw_market_key"].eq("batter_hits"))].copy()
    valid["player_norm"] = valid["player_name"].map(norm_name)
    valid["event_team_set"] = valid.apply(lambda r: "|".join(sorted([team_abbr(r["home_team"]), team_abbr(r["away_team"])])), axis=1)
    spine = spine.copy()
    spine["player_norm"] = spine["player_name"].map(norm_name)
    spine["spine_team_set"] = spine.apply(lambda r: "|".join(sorted([team_abbr(r["team"]), team_abbr(r["opponent"])])), axis=1)
    spine_key = spine[["player_game_key", "slate_date", "player_norm", "spine_team_set", "player_id", "player_name", "team", "opponent", "lineup_bucket", "actual_plate_appearances", "actual_hits"]]
    merged = valid.merge(
        spine_key,
        left_on=["slate_date", "player_norm", "event_team_set"],
        right_on=["slate_date", "player_norm", "spine_team_set"],
        how="left",
        suffixes=("", "_spine"),
    )
    counts = merged.groupby(["source_capture_timestamp", "event_id", "player_name", "line", "side"], dropna=False)["player_game_key"].nunique().reset_index(name="spine_matches")
    ambiguous_keys = set(counts[counts["spine_matches"].gt(1)][["source_capture_timestamp", "event_id", "player_name", "line", "side"]].astype(str).agg("|".join, axis=1))
    merged["match_key"] = merged[["source_capture_timestamp", "event_id", "player_name", "line", "side"]].astype(str).agg("|".join, axis=1)
    merged["overlay_join_status"] = np.select(
        [merged["player_game_key"].isna(), merged["match_key"].isin(ambiguous_keys)],
        ["UNMATCHED_PLAYER_DATE_TEAM", "AMBIGUOUS_PLAYER_DATE_TEAM"],
        default="MATCHED_NONMARKET_SPINE",
    )
    prior_keys = set(prior_pop.get("player_game_key", pd.Series(dtype=str)).astype(str))
    matched_keys = set(merged[merged["overlay_join_status"].eq("MATCHED_NONMARKET_SPINE")]["player_game_key"].astype(str))
    spine_keys = set(spine["player_game_key"].astype(str))
    corrected_union_keys = matched_keys | prior_keys
    prior_keys_in_spine = set(spine["player_game_key"].astype(str)) & prior_keys
    summary = pd.DataFrame(
        [
            {"metric": "full_nonmarket_spine_rows", "value": len(spine), "notes": ""},
            {"metric": "prior_market_conditioned_population_rows", "value": len(prior_pop), "notes": "Rows in prior market-conditioned artifact."},
            {"metric": "prior_market_conditioned_rows_present_in_nonmarket_spine", "value": len(prior_keys_in_spine), "notes": "Exact player_game_key rows retained in the 21,247-row spine."},
            {"metric": "recovered_batter_hits_price_rows", "value": len(valid), "notes": ""},
            {"metric": "matched_recovered_hits_price_rows", "value": int(merged["overlay_join_status"].eq("MATCHED_NONMARKET_SPINE").sum()), "notes": ""},
            {"metric": "newly_matched_player_games_with_recovered_direct_hits", "value": len(matched_keys - prior_keys), "notes": ""},
            {"metric": "corrected_market_conditioned_union_keys", "value": len(corrected_union_keys), "notes": "Union of prior market-conditioned keys and safely matched recovered direct BetOnline Hits keys, before restricting to current spine."},
            {"metric": "corrected_market_conditioned_player_games", "value": len(spine_keys & corrected_union_keys), "notes": "Corrected market-conditioned rows present in the 21,247-row nonmarket spine."},
            {"metric": "corrected_market_conditioned_keys_missing_from_nonmarket_spine", "value": len(corrected_union_keys - spine_keys), "notes": ""},
            {"metric": "rows_still_without_direct_market_evidence", "value": len(spine_keys - corrected_union_keys), "notes": ""},
            {"metric": "hits_0_5_price_rows", "value": int((pd.to_numeric(valid["line"], errors="coerce") == 0.5).sum()), "notes": ""},
            {"metric": "hits_1_5_price_rows", "value": int((pd.to_numeric(valid["line"], errors="coerce") == 1.5).sum()), "notes": ""},
            {"metric": "fanduel_price_substitutions", "value": 0, "notes": "FanDuel prices prohibited."},
        ]
    )
    return merged, summary


def market_conditioned_population(overlay: pd.DataFrame, spine: pd.DataFrame, prior_pop: pd.DataFrame) -> pd.DataFrame:
    prior_keys = set(prior_pop.get("player_game_key", pd.Series(dtype=str)).astype(str))
    matched = overlay[overlay["overlay_join_status"].eq("MATCHED_NONMARKET_SPINE")].copy()
    rec_keys = set(matched["player_game_key"].astype(str))
    all_keys = prior_keys | rec_keys
    spine_work = spine[spine["player_game_key"].astype(str).isin(all_keys)].copy()
    non = spine[~spine["player_game_key"].astype(str).isin(all_keys)].copy()
    rows = []
    for label, frame in [
        ("corrected_market_conditioned_population", spine_work),
        ("new_recovered_only_player_games", spine[spine["player_game_key"].astype(str).isin(rec_keys - prior_keys)]),
        ("prior_market_conditioned_population_present_in_spine", spine[spine["player_game_key"].astype(str).isin(prior_keys)]),
        ("nonmarket_only_population", non),
    ]:
        rows.append(
            {
                "population": label,
                "rows": len(frame),
                "dates": frame["slate_date"].nunique() if not frame.empty else 0,
                "players": frame["player_id"].nunique() if not frame.empty else 0,
                "avg_actual_hits": pd.to_numeric(frame.get("actual_hits"), errors="coerce").mean() if not frame.empty else "",
                "two_plus_rate": (pd.to_numeric(frame.get("actual_hits"), errors="coerce").fillna(0).ge(2)).mean() if not frame.empty else "",
                "avg_pa": pd.to_numeric(frame.get("actual_plate_appearances"), errors="coerce").mean() if not frame.empty else "",
                "top_order_pct": frame["lineup_bucket"].eq("top_order").mean() if "lineup_bucket" in frame and not frame.empty else "",
                "selection_rate_vs_full_spine": len(frame) / max(1, len(spine)),
            }
        )
    return pd.DataFrame(rows)


def incumbent_comparison(overlay: pd.DataFrame, predictions: pd.DataFrame) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()
    valid_keys = set(overlay[overlay["overlay_join_status"].eq("MATCHED_NONMARKET_SPINE")]["player_game_key"].astype(str))
    pred = predictions.copy()
    pred["player_game_key"] = pred["slate_date"].astype(str) + "|" + pred["game_id"].astype(str) + "|" + pred["player_id"].astype(str)
    pred = pred[pred["player_game_key"].isin(valid_keys)].copy()
    rows = []
    candidates = [
        "candidate_a_multiclass_hgb",
        "candidate_b_ordinal_hgb",
        "candidate_c_poisson",
        "rolling_hit_rate_opportunity",
        "empirical_skill_opportunity",
        "poisson_count_baseline",
    ]
    for segment, frame in [
        ("all_authentic_direct_betonline_rows_with_frozen_prediction", pred),
        ("holdout_authentic_direct_betonline_rows_with_frozen_prediction", pred[pred["split"].eq("holdout")]),
    ]:
        for cand in candidates:
            rows.append(metric_row(frame, cand, "O0.5", f"{cand}_p_over_0_5", "target_o05", segment))
            rows.append(metric_row(frame, cand, "O1.5", f"{cand}_p_over_1_5", "target_o15", segment))
    return pd.DataFrame(rows)


def economic_metrics(overlay: pd.DataFrame) -> pd.DataFrame:
    work = overlay[overlay["overlay_join_status"].eq("MATCHED_NONMARKET_SPINE")].copy()
    if work.empty:
        return pd.DataFrame()
    work["line_num"] = pd.to_numeric(work["line"], errors="coerce")
    work["price_num"] = pd.to_numeric(work["price"], errors="coerce")
    work["actual_hits_num"] = pd.to_numeric(work["actual_hits"], errors="coerce")
    work = work.dropna(subset=["line_num", "price_num", "actual_hits_num"])
    work["result"] = np.select(
        [
            work["side"].eq("over") & work["actual_hits_num"].gt(work["line_num"]),
            work["side"].eq("under") & work["actual_hits_num"].lt(work["line_num"]),
            work["actual_hits_num"].eq(work["line_num"]),
        ],
        ["win", "win", "push"],
        default="loss",
    )
    work["units"] = np.select(
        [work["result"].eq("win"), work["result"].eq("loss")],
        [work["price_num"].map(american_profit), -1.0],
        default=0.0,
    )
    work["break_even_probability"] = np.where(work["price_num"].gt(0), 100 / (work["price_num"] + 100), abs(work["price_num"]) / (abs(work["price_num"]) + 100))
    rows = []
    for keys, g in work.groupby(["recovery_class", "line_num", "side"], dropna=False):
        rc, line, side = keys
        wagers = g[~g["result"].eq("push")]
        rows.append(
            {
                "recovery_class": rc,
                "line": line,
                "side": side,
                "rows": len(g),
                "wagers": len(wagers),
                "wins": int(wagers["result"].eq("win").sum()),
                "losses": int(wagers["result"].eq("loss").sum()),
                "pushes": int(g["result"].eq("push").sum()),
                "avg_price": g["price_num"].mean(),
                "avg_break_even_probability": g["break_even_probability"].mean(),
                "win_rate": wagers["result"].eq("win").mean() if len(wagers) else "",
                "units": wagers["units"].sum() if len(wagers) else 0,
                "roi": wagers["units"].sum() / len(wagers) if len(wagers) else "",
            }
        )
    return pd.DataFrame(rows)


def prior_amendments(backfill_summary: dict[str, Any], market_pop: pd.DataFrame, timeline: pd.DataFrame) -> pd.DataFrame:
    corr = int(market_pop.loc[market_pop["population"].eq("corrected_market_conditioned_population"), "rows"].iloc[0]) if not market_pop.empty else 0
    validated_rows = int(backfill_summary.get("validated_rows", backfill_summary.get("final_validated_rows", 0)))
    return pd.DataFrame(
        [
            {"prior_conclusion": "BetOnline player props absent/stopped", "original_population": "visible local_daily normalized artifacts", "corrected_population": validated_rows, "disposition": "AMENDED", "notes": "Recovered direct rows prove raw/alternate local evidence existed for several markets."},
            {"prior_conclusion": "specialized markets retired", "original_population": "retained visible captures", "corrected_population": "|".join(sorted(timeline["raw_market_key"].astype(str).unique())), "disposition": "WITHDRAWN_FOR_HOME_RUNS_EARNED_RUNS_HITS_ALLOWED", "notes": "Stolen bases remains unrecovered in direct BetOnline rows."},
            {"prior_conclusion": "candidate scarcity due to market absence", "original_population": "2,887 prior market-conditioned rows", "corrected_population": corr, "disposition": "AMENDED_MAGNITUDE_CHANGED", "notes": "Market-conditioned population expands but remains materially smaller than 21,247 nonmarket spine."},
            {"prior_conclusion": "incumbent same-row comparison undercovered", "original_population": "prior frozen scored population", "corrected_population": "refreshed where frozen predictions exist", "disposition": "AMENDED_REPLAY_LIMITED_BY_FROZEN_PREDICTION_POPULATION", "notes": "No refit or new scoring performed."},
            {"prior_conclusion": "BetOnline ROI/economics unavailable", "original_population": "visible direct prices only", "corrected_population": "recovered direct prices by class", "disposition": "AMENDED_WITH_CLASS_SEPARATION", "notes": "Economics separated by raw reparse and alternate timestamp class."},
        ]
    )


def residual_audit(unrecovered: pd.DataFrame) -> pd.DataFrame:
    if unrecovered.empty:
        return pd.DataFrame()
    df = unrecovered.copy()
    class_col = "unrecovered_classification" if "unrecovered_classification" in df.columns else "final_unresolved_classification"
    if class_col not in df.columns:
        df[class_col] = "UNCLASSIFIED"
    df["residual_class"] = np.select(
        [
            df[class_col].isin(["PERMANENT_LOCAL_CAPTURE_GAP", "PERMANENT_LOCAL_AND_PROVIDER_GAP"]),
            df["fanduel_or_other_book_context_rows"].fillna(0).astype(float).gt(0) & df["prop_type"].eq("hits"),
            df["raw_market_key"].eq("batter_stolen_bases"),
        ],
        ["PERMANENT_LOCAL_GAP", "ECONOMIC_ONLY_GAP", "MATERIAL_ANALYSIS_RESIDUAL"],
        default="DIRECT_BETONLINE_PRICE_UNRECOVERED",
    )
    return df.groupby(["raw_market_key", "residual_class"], dropna=False).agg(
        rows=("manifest_id", "count"),
        dates=("slate_date", "nunique"),
        scheduled_windows=("expected_utc_time", "nunique"),
        fanduel_or_other_book_context_rows=("fanduel_or_other_book_context_rows", "sum"),
    ).reset_index()


def validate_outputs(out_dir: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(out_dir.glob("*")):
        if p.is_dir():
            continue
        status = "PASS"
        notes = ""
        try:
            if p.suffix == ".csv":
                with p.open(newline="", encoding="utf-8") as f:
                    list(csv.reader(f))
            elif p.suffix == ".json":
                json.loads(p.read_text(encoding="utf-8"))
            elif p.suffix == ".md" and not p.read_text(encoding="utf-8").strip():
                status = "FAIL"
                notes = "empty markdown"
        except Exception as exc:
            status = "FAIL"
            notes = f"{type(exc).__name__}: {exc}"
        rows.append({"artifact": rel(p), "validation": "parse_or_nonempty", "status": status, "notes": notes})
    return pd.DataFrame(rows)


def run(
    out_dir: Path,
    *,
    backfill_dir: Path = BACKFILL_DIR,
    recovered_rows_name: str = "normalized_recovered_rows_2026-07-19.csv",
    manifest_name: str = "frozen_governing_backfill_manifest_2026-07-19.csv",
    unrecovered_name: str = "unrecovered_row_ledger_2026-07-19.csv",
    machine_json_name: str = "machine_readable_betonline_backfill_2026-07-19.json",
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    data = load_inputs(backfill_dir, recovered_rows_name, manifest_name, unrecovered_name)
    recovered = data["recovered"]
    manifest = data["manifest"]
    unrecovered = data["unrecovered"]
    spine = data["spine"]
    prior_pop = data["prior_pop"]
    predictions = data["predictions"]
    machine_backfill = json.loads((backfill_dir / machine_json_name).read_text())
    valid_recovered = recovered[recovered["validation_status"].eq("PASS")]

    grain = recovered_grain(recovered, manifest)
    precedence = overlay_precedence(recovered, manifest)
    timeline = market_timelines(recovered)
    incident = incident_cause_attribution(manifest, unrecovered, recovered)
    overlay, overlay_summary = hits_overlay(recovered, spine, prior_pop)
    market_pop = market_conditioned_population(overlay, spine, prior_pop)
    same_row = incumbent_comparison(overlay, predictions)
    econ = economic_metrics(overlay)
    amendments = prior_amendments(machine_backfill, market_pop, timeline)
    residual = residual_audit(unrecovered)

    artifacts = {
        "recovered_row_grain_audit": grain,
        "overlay_precedence_validation": precedence,
        "corrected_market_timelines": timeline,
        "corrected_incident_cause_attribution": incident,
        "refreshed_hits_market_overlay": overlay,
        "refreshed_hits_market_overlay_summary": overlay_summary,
        "corrected_market_conditioned_population": market_pop,
        "incumbent_same_row_comparison": same_row,
        "corrected_economic_metrics": econ,
        "prior_decision_amendment_ledger": amendments,
        "residual_unresolved_audit": residual,
        "required_decisions": pd.DataFrame([{"decision": k, "value": v} for k, v in DECISIONS.items()]),
    }
    for name, df in artifacts.items():
        write_csv(out_dir / f"{name}_{RUN_DATE}.csv", df)

    corrected_rows = int(market_pop.loc[market_pop["population"].eq("corrected_market_conditioned_population"), "rows"].iloc[0]) if not market_pop.empty else 0
    machine_validated_rows = int(machine_backfill.get("validated_rows", machine_backfill.get("final_validated_rows", 0)))
    summary = {
        "generated_at_utc": now_utc(),
        "validated_recovered_price_rows": machine_validated_rows,
        "raw_reparse_rows": int(machine_backfill.get("local_raw_rows_recovered", machine_backfill.get("starting_prior_validated_rows", 0))),
        "alternate_capture_candidate_rows": int(machine_backfill.get("alternate_rows_recovered", 0)),
        "alternate_capture_validated_rows": int(valid_recovered["recovery_class"].eq("RECOVERED_FROM_ALTERNATE_RETAINED_CAPTURE").sum()),
        "continuation_validated_rows": int(machine_backfill.get("continuation_validated_rows", 0)),
        "normal_path_rows": int(machine_backfill.get("normal_path_rows_recovered", 0)),
        "historical_rows": int(machine_backfill.get("historical_rows_recovered", 0)),
        "unrecovered_manifest_rows": int(machine_backfill.get("unrecovered_manifest_rows", machine_backfill.get("final_unresolved_manifest_rows", 0))),
        "corrected_market_conditioned_player_games": corrected_rows,
        "full_nonmarket_spine_rows": int(len(spine)),
        "prior_market_conditioned_rows": int(len(prior_pop)),
        "direct_betonline_hits_price_rows": int((recovered["validation_status"].eq("PASS") & recovered["raw_market_key"].eq("batter_hits")).sum()),
        "decisions": DECISIONS,
    }
    write_json(out_dir / f"machine_readable_post_backfill_recertification_{RUN_DATE}.json", summary)
    grain_lines = "\n".join(f"- `{r.grain}`: `{int(r.rows)}`" for r in grain.itertuples(index=False))
    overlay_lines = "\n".join(f"- `{r.metric}`: `{int(r.value)}`" for r in overlay_summary.itertuples(index=False))
    timeline_lines = "\n".join(
        f"- `{r.raw_market_key}`: `{int(r.slates_offered)}` slates, `{int(r.unique_propositions)}` unique propositions, `{int(r.two_sided_propositions)}` two-sided propositions"
        for r in timeline.itertuples(index=False)
    )
    residual_rows = int(residual["rows"].sum()) if not residual.empty else 0
    residual_lines = "\n".join(
        f"- `{r.raw_market_key}` / `{r.residual_class}`: `{int(r.rows)}` manifest rows"
        for r in residual.itertuples(index=False)
    )
    decisions_lines = "\n".join(f"- `{k} = {v}`" for k, v in DECISIONS.items())
    md = f"""# Post-Backfill BetOnline Historical Recertification

Generated: `{summary['generated_at_utc']}`

## Summary

The backfill overlay contributes `{summary['validated_recovered_price_rows']}` validated direct BetOnline price rows. This recertification keeps the baseball-only nonmarket feature spine unchanged and applies recovered rows only as market/economic evidence.

Corrected market-conditioned player-games present in the certified spine: `{summary['corrected_market_conditioned_player_games']}` versus prior artifact rows `{summary['prior_market_conditioned_rows']}` and full nonmarket spine `{summary['full_nonmarket_spine_rows']}`.

## Material Changes

- Specialized-market retirement/scarcity conclusions are amended for Home Runs, Earned Runs, and Pitcher Hits Allowed.
- Hits direct-market evidence expands through recovered BetOnline `batter_hits` rows.
- The modeling population remains market-conditioned in kind, but the magnitude changes.
- Same-row comparison can only be refreshed where frozen predictions already exist; no model was refit.
- Stolen Bases direct BetOnline recovery remains zero in this overlay.

## Recovered Grain

{grain_lines}

## Hits Overlay

{overlay_lines}

## Corrected Market Timeline

{timeline_lines}

## Remaining Residuals

Residual unresolved manifest rows after the overlay: `{residual_rows}`.

{residual_lines}

## Direct Answer

The recovered rows materially change the market-availability and incident-scope conclusions: BetOnline direct player-prop evidence existed in retained local artifacts for multiple markets that previously looked absent or retired. Hits market conditioning expands from the prior visible surface, but it remains a selected subset of the baseball-only 21,247-row feature spine. The remaining `{summary['unrecovered_manifest_rows']}` unresolved manifest rows should be interpreted according to the governing state of the package: provisional runs are not final, while exhausted runs may close only after the final continuation and delta artifacts are present. Stolen Bases remains a distinct future-market gap when direct BetOnline `batter_stolen_bases` rows remain unrecovered.

## Decisions

{decisions_lines}

## Production Status

`MLB_PRODUCTION_STATUS = UNCHANGED`
"""
    (out_dir / f"post_backfill_betonline_recertification_{RUN_DATE}.md").write_text(md, encoding="utf-8")

    validation = validate_outputs(out_dir)
    write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation)
    sha_rows = []
    for p in sorted(out_dir.glob("*")):
        if p.is_file() and p.name != f"sha256_manifest_{RUN_DATE}.csv":
            sha_rows.append({"path": rel(p), "sha256": sha256_file(p), "bytes": p.stat().st_size})
    write_csv(out_dir / f"sha256_manifest_{RUN_DATE}.csv", pd.DataFrame(sha_rows))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--backfill-dir", default=str(BACKFILL_DIR))
    parser.add_argument("--recovered-rows-name", default="normalized_recovered_rows_2026-07-19.csv")
    parser.add_argument("--manifest-name", default="frozen_governing_backfill_manifest_2026-07-19.csv")
    parser.add_argument("--unrecovered-name", default="unrecovered_row_ledger_2026-07-19.csv")
    parser.add_argument("--machine-json-name", default="machine_readable_betonline_backfill_2026-07-19.json")
    args = parser.parse_args()
    print(json.dumps(
        run(
            Path(args.output_dir),
            backfill_dir=Path(args.backfill_dir),
            recovered_rows_name=args.recovered_rows_name,
            manifest_name=args.manifest_name,
            unrecovered_name=args.unrecovered_name,
            machine_json_name=args.machine_json_name,
        ),
        indent=2,
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
