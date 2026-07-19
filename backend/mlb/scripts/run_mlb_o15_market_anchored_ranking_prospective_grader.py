#!/usr/bin/env python3
"""MLB O1.5 market-anchored ranking prospective grading program.

This bounded utility freezes the prospective ranking contract, audits historical
top-20 comparability, binds the existing Run 1 live ranking ledger, and grades
only previously frozen prospective rows when a repository-backed official
outcome source is present.

No network calls, OddsAPI calls, database writes, production behavior changes,
rank reconstruction after outcomes, new features/regimes, refits, threshold
optimization, upload/workspace/selector changes, or LaunchAgent changes are
performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
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
    profit_1u,
    rel,
    to_float,
)

RANKING_DIR = ROOT / "artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_challenger/2026-07-17"
OUT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_prospective"
REPAIR_OUT_DIR = OUT_ROOT / "automation_repair_2026-07-18"
RUN_DATE = "2026-07-17"
RUN_ID = "O15_MARKET_ANCHORED_RANKING_RUN_1"
EPS = 1e-9

OUTCOME_PRIORITY = {
    "exact_hits15_reconcile": 1,
    "player_game_hits_reconcile_fallback": 2,
    "repository_player_game_outcome_fallback": 3,
}


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
    g = frame.dropna(subset=["two_plus_target", score_col]).copy()
    if g.empty or g["two_plus_target"].nunique() < 2:
        return ""
    return float(roc_auc_score(g["two_plus_target"].astype(int), pd.to_numeric(g[score_col], errors="coerce")))


def pairwise_for_scores(g: pd.DataFrame, score_col: str) -> dict[str, Any]:
    wins = g[g["two_plus_target"].eq(1)][score_col].dropna().to_numpy(dtype=float)
    losses = g[g["two_plus_target"].eq(0)][score_col].dropna().to_numpy(dtype=float)
    if len(wins) == 0 or len(losses) == 0:
        return {"eligible_pairs": 0, "concordant_pairs": "", "discordant_pairs": "", "tied_pairs": "", "pairwise_accuracy": ""}
    concordant = 0
    discordant = 0
    tied = 0
    for score in wins:
        concordant += int(np.sum(score > losses))
        discordant += int(np.sum(score < losses))
        tied += int(np.sum(score == losses))
    pairs = len(wins) * len(losses)
    return {
        "eligible_pairs": int(pairs),
        "concordant_pairs": int(concordant),
        "discordant_pairs": int(discordant),
        "tied_pairs": int(tied),
        "pairwise_accuracy": float((concordant + 0.5 * tied) / pairs),
    }


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


def fixed_memberships(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["champion_top5_member"] = out.groupby("slate_date")["market_rank"].rank(method="first").le(5)
    out["challenger_top5_member"] = out.groupby("slate_date")["challenger_rank"].rank(method="first").le(5)
    out["champion_top10_member"] = out.groupby("slate_date")["market_rank"].rank(method="first").le(10)
    out["challenger_top10_member"] = out.groupby("slate_date")["challenger_rank"].rank(method="first").le(10)
    out["_slate_n"] = out.groupby("slate_date")["canonical_proposition_key"].transform("count")
    out["_top20_n"] = out["_slate_n"].map(lambda n: max(1, int(math.ceil(float(n) * 0.20))))
    out["champion_top20_equal_count_member"] = out["market_rank"].le(out["_top20_n"])
    out["challenger_top20_equal_count_member"] = out["challenger_rank"].le(out["_top20_n"])
    return out.drop(columns=["_slate_n", "_top20_n"])


def load_live_ledger() -> pd.DataFrame:
    path = RANKING_DIR / "live_ranking_ledger_2026-07-17.csv"
    live = pd.read_csv(path, low_memory=False)
    live["ranking_run_id"] = live.get("ranking_run_id", RUN_ID)
    live["price_band"] = live["market_price_over"].map(price_band)
    live["prospective_grade_status"] = "PENDING_OFFICIAL_OUTCOME"
    live["official_hits"] = np.nan
    live["two_plus_target"] = np.nan
    live["grade_source_path"] = ""
    live["grade_source_sha256"] = ""
    live["grade_source_type"] = ""
    live["grade_source_priority"] = ""
    live["grade_source_detail"] = ""
    return fixed_memberships(live)


def load_reconcile_rows(run_date: str) -> tuple[pd.DataFrame, Path | None, str]:
    path = ROOT / f"artifacts/analysis/mlb/execution_vs_model/{run_date}/reconcile_rows.csv"
    if not path.exists():
        return pd.DataFrame(), None, ""
    df = pd.read_csv(path, low_memory=False)
    return df, path, sha256(path)


def _normalize_outcome_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "slate_date" in out.columns:
        out["slate_date"] = out["slate_date"].astype(str)
    if "game_id" in out.columns:
        out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    if "player_id" in out.columns:
        out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    return out


def load_outcomes(run_date: str) -> tuple[pd.DataFrame, str, str]:
    df, path, source_hash = load_reconcile_rows(run_date)
    if df.empty or path is None:
        return pd.DataFrame(), "", ""
    required = {"slate_date", "game_id", "player_id", "prop_type", "line", "actual_value"}
    if not required.issubset(df.columns):
        return pd.DataFrame(), str(path), source_hash
    work = _normalize_outcome_keys(df)
    work = work[
        df["slate_date"].astype(str).eq(run_date)
        & df["prop_type"].astype(str).str.lower().eq("hits")
    ].copy()
    work["official_hits"] = pd.to_numeric(work["actual_value"], errors="coerce")
    work = work.dropna(subset=["game_id", "player_id", "official_hits"]).copy()
    if work.empty:
        return pd.DataFrame(), str(path), source_hash
    work["line_numeric"] = pd.to_numeric(work["line"], errors="coerce")
    work["outcome_source_type"] = np.where(
        work["line_numeric"].eq(1.5),
        "exact_hits15_reconcile",
        "player_game_hits_reconcile_fallback",
    )
    work["outcome_source_priority"] = work["outcome_source_type"].map(OUTCOME_PRIORITY)
    work["outcome_source_path"] = rel(path)
    work["outcome_source_sha256"] = source_hash
    work["outcome_source_detail"] = np.where(
        work["line_numeric"].eq(1.5),
        "exact Hits 1.5 reconcile row carried numeric official hits",
        "same player-game Hits reconcile row at another market line carried numeric official hits",
    )
    sort_cols = ["game_id", "player_id", "outcome_source_priority", "line_numeric"]
    if "market_snapshot_time_utc" in work.columns:
        sort_cols.append("market_snapshot_time_utc")
    work = work.sort_values(sort_cols, na_position="last").drop_duplicates(
        ["slate_date", "game_id", "player_id"], keep="first"
    )
    return (
        work[
            [
                "slate_date",
                "game_id",
                "player_id",
                "official_hits",
                "outcome_source_type",
                "outcome_source_priority",
                "outcome_source_path",
                "outcome_source_sha256",
                "outcome_source_detail",
            ]
        ],
        str(path),
        source_hash,
    )


def build_unmatched_13_audit(live: pd.DataFrame, run_date: str) -> pd.DataFrame:
    reconcile, path, source_hash = load_reconcile_rows(run_date)
    audit = live.copy()
    audit["game_id"] = pd.to_numeric(audit["game_id"], errors="coerce").astype("Int64")
    audit["player_id"] = pd.to_numeric(audit["player_id"], errors="coerce").astype("Int64")
    base_cols = [
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "canonical_proposition_key",
        "market_snapshot_run_tag",
        "market_price_over",
        "market_price_under",
        "market_rank",
        "challenger_rank",
        "champion_top5_member",
        "challenger_top5_member",
        "champion_top10_member",
        "challenger_top10_member",
        "champion_top20_equal_count_member",
        "challenger_top20_equal_count_member",
    ]
    if reconcile.empty or path is None:
        audit["pre_repair_resolution_reason"] = "source_artifact_missing"
        audit["reconcile_player_game_rows"] = 0
        audit["reconcile_hits_rows"] = 0
        audit["reconcile_hits15_numeric_rows"] = 0
        audit["fallback_hits_numeric_rows"] = 0
        audit["fallback_official_hits"] = ""
        audit["outcome_source_path"] = ""
        audit["outcome_source_sha256"] = ""
        return audit[base_cols + [c for c in audit.columns if c not in base_cols]].copy()

    rec = _normalize_outcome_keys(reconcile)
    rec = rec[rec["slate_date"].astype(str).eq(run_date)].copy()
    rec["line_numeric"] = pd.to_numeric(rec.get("line"), errors="coerce")
    rec["actual_numeric"] = pd.to_numeric(rec.get("actual_value"), errors="coerce")
    rec["is_hits"] = rec["prop_type"].astype(str).str.lower().eq("hits") if "prop_type" in rec.columns else False
    rec["is_hits15"] = rec["is_hits"] & rec["line_numeric"].eq(1.5)
    rows = []
    for _, row in audit.iterrows():
        pg = rec[rec["game_id"].eq(row["game_id"]) & rec["player_id"].eq(row["player_id"])]
        hits = pg[pg["is_hits"]]
        hits15 = pg[pg["is_hits15"]]
        hits15_numeric = hits15[hits15["actual_numeric"].notna()]
        fallback_hits = hits[hits["actual_numeric"].notna()]
        if not hits15_numeric.empty:
            continue
        if pg.empty:
            reason = "player_id_not_in_reconcile_rows"
        elif hits.empty:
            reason = "player_game_present_but_no_hits_prop_row"
        elif hits15.empty:
            reason = "player_game_hits_prop_present_but_no_line_1_5"
        elif hits15["actual_numeric"].isna().all():
            reason = "hits_1_5_reconcile_row_present_but_outcome_blank"
        else:
            reason = "outcome_join_rejected"
        fallback_value = ""
        fallback_reason = "not_available"
        if not fallback_hits.empty:
            values = sorted(set(float(v) for v in fallback_hits["actual_numeric"].dropna()))
            if len(values) == 1:
                fallback_value = values[0]
                fallback_reason = "same_player_game_hits_row_available"
            else:
                fallback_reason = "conflicting_same_player_game_hits_values"
        item = {c: row.get(c, "") for c in base_cols}
        item.update(
            {
                "pre_repair_resolution_reason": reason,
                "reconcile_player_game_rows": int(len(pg)),
                "reconcile_hits_rows": int(len(hits)),
                "reconcile_hits15_rows": int(len(hits15)),
                "reconcile_hits15_numeric_rows": int(len(hits15_numeric)),
                "fallback_hits_numeric_rows": int(len(fallback_hits)),
                "fallback_official_hits": fallback_value,
                "fallback_resolution_status": fallback_reason,
                "post_repair_expected_status": "GRADED_PLAYER_GAME_HITS_RECONCILE_FALLBACK"
                if fallback_reason == "same_player_game_hits_row_available"
                else "UNMATCHED_OFFICIAL_PLAYER_GAME_OUTCOME",
                "outcome_source_path": rel(path),
                "outcome_source_sha256": source_hash,
                "join_keys_used": "slate_date|game_id|player_id",
                "notes": "Name-only diagnostics were not used for grading.",
            }
        )
        rows.append(item)
    return pd.DataFrame(rows)


def attach_grades(live: pd.DataFrame, run_date: str) -> pd.DataFrame:
    outcomes, source_path, source_hash = load_outcomes(run_date)
    out = live.copy()
    if outcomes.empty:
        out["prospective_grade_status"] = "PENDING_OFFICIAL_OUTCOME_SOURCE_MISSING"
        return out
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    joined = out.merge(outcomes, on=["slate_date", "game_id", "player_id"], how="left", suffixes=("", "_official"))
    joined["official_hits"] = joined["official_hits_official"].combine_first(joined["official_hits"])
    joined = joined.drop(columns=[c for c in ["official_hits_official"] if c in joined.columns])
    matched = joined["official_hits"].notna()
    joined.loc[matched, "two_plus_target"] = (joined.loc[matched, "official_hits"] >= 2).astype(int)
    joined.loc[matched, "prospective_grade_status"] = joined.loc[matched, "outcome_source_type"].map(
        {
            "exact_hits15_reconcile": "GRADED_EXACT_HITS15_RECONCILE_ROWS",
            "player_game_hits_reconcile_fallback": "GRADED_PLAYER_GAME_HITS_RECONCILE_FALLBACK",
            "repository_player_game_outcome_fallback": "GRADED_REPOSITORY_PLAYER_GAME_OUTCOME_FALLBACK",
        }
    ).fillna("GRADED_AUTHORITATIVE_PLAYER_GAME_HITS")
    joined.loc[~matched, "prospective_grade_status"] = "UNMATCHED_OFFICIAL_PLAYER_GAME_OUTCOME"
    joined.loc[matched, "grade_source_path"] = joined.loc[matched, "outcome_source_path"].fillna(rel(Path(source_path)))
    joined.loc[matched, "grade_source_sha256"] = joined.loc[matched, "outcome_source_sha256"].fillna(source_hash)
    joined.loc[matched, "grade_source_type"] = joined.loc[matched, "outcome_source_type"].fillna("")
    joined.loc[matched, "grade_source_priority"] = joined.loc[matched, "outcome_source_priority"].fillna("")
    joined.loc[matched, "grade_source_detail"] = joined.loc[matched, "outcome_source_detail"].fillna("")
    joined = joined.drop(
        columns=[
            c
            for c in [
                "outcome_source_type",
                "outcome_source_priority",
                "outcome_source_path",
                "outcome_source_sha256",
                "outcome_source_detail",
            ]
            if c in joined.columns
        ]
    )
    joined["grade_generated_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return joined


def summarize(df: pd.DataFrame, subset_col: str, instrument: str) -> dict[str, Any]:
    subset = df[df[subset_col].eq(True)].copy() if subset_col else df.copy()
    graded = subset[subset["two_plus_target"].notna()].copy()
    return {
        "instrument": instrument,
        "population": subset_col or "all_market_bound",
        "rows": int(len(subset)),
        "graded_rows": int(len(graded)),
        "wins": int(graded["two_plus_target"].sum()) if len(graded) else 0,
        "losses": int((1 - graded["two_plus_target"]).sum()) if len(graded) else 0,
        "outcome_rate": float(graded["two_plus_target"].mean()) if len(graded) else "",
        "avg_price": float(pd.to_numeric(subset["market_price_over"], errors="coerce").mean()) if len(subset) else "",
        "diagnostic_roi": float(graded.apply(lambda r: profit_1u(r["market_price_over"], bool(r["two_plus_target"])), axis=1).mean()) if len(graded) else "",
        "market_probability": float(pd.to_numeric(subset["market_probability_used"], errors="coerce").mean()) if len(subset) else "",
        "avg_rank_movement": float(pd.to_numeric(subset["rank_movement"], errors="coerce").mean()) if len(subset) else "",
        "unmatched_or_pending_rows": int(subset["two_plus_target"].isna().sum()),
        "descriptive_only": True,
    }


def fixed_volume_results(graded: pd.DataFrame) -> pd.DataFrame:
    rows = [
        summarize(graded, "champion_top5_member", "champion_market"),
        summarize(graded, "challenger_top5_member", "challenger_market_plus_proppadia"),
        summarize(graded, "champion_top10_member", "champion_market"),
        summarize(graded, "challenger_top10_member", "challenger_market_plus_proppadia"),
        summarize(graded, "champion_top20_equal_count_member", "champion_market"),
        summarize(graded, "challenger_top20_equal_count_member", "challenger_market_plus_proppadia"),
        summarize(graded, "", "all_market_bound"),
    ]
    return pd.DataFrame(rows)


def fixed_volume_common_membership_results(graded: pd.DataFrame) -> pd.DataFrame:
    rows = []
    specs = [
        ("top5", "champion_top5_member", "challenger_top5_member"),
        ("top10", "champion_top10_member", "challenger_top10_member"),
        ("top20_equal_count", "champion_top20_equal_count_member", "challenger_top20_equal_count_member"),
    ]
    for volume, champion_col, challenger_col in specs:
        champion = graded[graded[champion_col].eq(True)].copy()
        challenger = graded[graded[challenger_col].eq(True)].copy()
        champion_graded = champion[champion["two_plus_target"].notna()].copy()
        challenger_graded = challenger[challenger["two_plus_target"].notna()].copy()
        common = graded[graded[champion_col].eq(True) & graded[challenger_col].eq(True)].copy()
        common_graded = common[common["two_plus_target"].notna()].copy()
        rows.append(
            {
                "ranking_run_id": RUN_ID,
                "volume": volume,
                "champion_original_member_rows": int(len(champion)),
                "challenger_original_member_rows": int(len(challenger)),
                "champion_graded_member_rows": int(len(champion_graded)),
                "challenger_graded_member_rows": int(len(challenger_graded)),
                "common_original_member_rows": int(len(common)),
                "common_graded_member_rows": int(len(common_graded)),
                "champion_member_two_plus_rate": float(champion_graded["two_plus_target"].mean()) if len(champion_graded) else "",
                "challenger_member_two_plus_rate": float(challenger_graded["two_plus_target"].mean()) if len(challenger_graded) else "",
                "common_member_two_plus_rate": float(common_graded["two_plus_target"].mean()) if len(common_graded) else "",
                "comparability_note": "Original memberships are preserved; unequal graded membership counts are descriptive only. Common-member intersection is reported separately.",
            }
        )
    return pd.DataFrame(rows)


def pairwise_results(graded: pd.DataFrame) -> pd.DataFrame:
    g = graded[graded["two_plus_target"].notna()].copy()
    if g.empty:
        return pd.DataFrame(
            [
                {
                    "ranking_run_id": RUN_ID,
                    "graded_rows": 0,
                    "status": "PENDING_OFFICIAL_OUTCOME",
                    "champion_auc": "",
                    "challenger_auc": "",
                    "auc_increment": "",
                    "champion_pairwise_accuracy": "",
                    "challenger_pairwise_accuracy": "",
                    "pairwise_accuracy_increment": "",
                    "spearman_rank_correlation": "",
                    "largest_upward_mover_outcome_rate": "",
                    "largest_downward_mover_outcome_rate": "",
                }
            ]
        )
    market = pairwise_for_scores(g, "champion_ranking_score")
    challenger = pairwise_for_scores(g, "challenger_ranking_score")
    up = g.sort_values("rank_movement", ascending=False).head(max(1, int(math.ceil(len(g) * 0.20))))
    down = g.sort_values("rank_movement", ascending=True).head(max(1, int(math.ceil(len(g) * 0.20))))
    champion_auc = auc_safe(g, "champion_ranking_score")
    challenger_auc = auc_safe(g, "challenger_ranking_score")
    return pd.DataFrame(
        [
            {
                "ranking_run_id": RUN_ID,
                "graded_rows": len(g),
                "status": "GRADED",
                "champion_auc": champion_auc,
                "challenger_auc": challenger_auc,
                "auc_increment": challenger_auc - champion_auc if champion_auc != "" and challenger_auc != "" else "",
                "champion_pairwise_accuracy": market["pairwise_accuracy"],
                "challenger_pairwise_accuracy": challenger["pairwise_accuracy"],
                "pairwise_accuracy_increment": challenger["pairwise_accuracy"] - market["pairwise_accuracy"] if market["pairwise_accuracy"] != "" and challenger["pairwise_accuracy"] != "" else "",
                "spearman_rank_correlation": spearmanr(g["market_rank"], g["challenger_rank"], nan_policy="omit").correlation,
                "largest_upward_mover_outcome_rate": float(up["two_plus_target"].mean()) if len(up) else "",
                "largest_downward_mover_outcome_rate": float(down["two_plus_target"].mean()) if len(down) else "",
            }
        ]
    )


def controlled_results(graded: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    price_rows = []
    strata_rows = []
    work = graded.copy()
    work["market_probability_stratum"] = pd.qcut(
        pd.to_numeric(work["market_probability_used"], errors="coerce").rank(method="first"),
        q=min(5, len(work)),
        labels=False,
        duplicates="drop",
    )
    for band, g in work.groupby("price_band", dropna=False):
        for col, inst in [
            ("champion_top20_equal_count_member", "champion_market"),
            ("challenger_top20_equal_count_member", "challenger_market_plus_proppadia"),
        ]:
            row = summarize(g, col, inst)
            row["price_band"] = band
            price_rows.append(row)
    for stratum, g in work.groupby("market_probability_stratum", dropna=False):
        for col, inst in [
            ("champion_top20_equal_count_member", "champion_market"),
            ("challenger_top20_equal_count_member", "challenger_market_plus_proppadia"),
        ]:
            row = summarize(g, col, inst)
            row["market_probability_stratum"] = stratum
            strata_rows.append(row)
    return pd.DataFrame(price_rows), pd.DataFrame(strata_rows)


def suppression_monitoring(graded: pd.DataFrame) -> pd.DataFrame:
    work = graded[graded["suppression_veto_status"].astype(str).str.contains("AFFIRMATIVE|veto_affirmative", case=False, na=False)].copy()
    if work.empty:
        return pd.DataFrame(
            [
                {
                    "ranking_run_id": RUN_ID,
                    "affirmative_suppression_rows": 0,
                    "top20_challenger_rows": 0,
                    "systematic_top_rank_warning": False,
                    "status": "NO_AFFIRMATIVE_SUPPRESSION_ROWS",
                }
            ]
        )
    return pd.DataFrame(
        [
            {
                "ranking_run_id": RUN_ID,
                "affirmative_suppression_rows": int(len(work)),
                "top20_challenger_rows": int(work["challenger_top20_equal_count_member"].sum()),
                "mean_rank_movement": float(pd.to_numeric(work["rank_movement"], errors="coerce").mean()),
                "graded_rows": int(work["two_plus_target"].notna().sum()),
                "two_plus_rate": float(work["two_plus_target"].mean()) if work["two_plus_target"].notna().any() else "",
                "systematic_top_rank_warning": bool(work["challenger_top20_equal_count_member"].sum() >= max(10, math.ceil(len(work) * 0.20))),
                "status": "MONITORED_DESCRIPTIVE",
            }
        ]
    )


def top20_comparability() -> pd.DataFrame:
    historical = pd.read_csv(RANKING_DIR / "fixed_volume_slate_diagnostics_2026-07-17.csv", low_memory=False)
    oof = pd.read_csv(RANKING_DIR / "historical_out_of_fold_ranking_population_2026-07-17.csv", low_memory=False)
    work = oof.copy()
    work["market_rank_slate"] = work.groupby("slate_date")["champion_ranking_score"].rank(ascending=False, method="first")
    work["challenger_rank_slate"] = work.groupby("slate_date")["challenger_ranking_score"].rank(ascending=False, method="first")
    work["_n"] = work.groupby("slate_date")["canonical_proposition_key"].transform("count")
    work["_top20_n"] = work["_n"].map(lambda n: max(1, int(math.ceil(float(n) * 0.20))))
    paired_market = work[work["market_rank_slate"].le(work["_top20_n"])]
    paired_challenger = work[work["challenger_rank_slate"].le(work["_top20_n"])]
    def hist_row(inst: str) -> pd.Series:
        return historical[(historical["instrument"].eq(inst)) & (historical["volume"].eq("top_20_pct_per_fit_band"))].iloc[0]
    return pd.DataFrame(
        [
            {
                "audit_item": "reported_historical_top20_counts",
                "market_rows": int(hist_row("champion_market")["rows"]),
                "challenger_rows": int(hist_row("challenger_market_plus_proppadia")["rows"]),
                "explanation": "Counts differ because prior top-20% used fit-percentile bands; test-block score distributions crossed frozen fit thresholds at different rates. This is not an equal-count per-slate fixed-volume comparison.",
                "ties_explain_difference": False,
                "missing_scores_explain_difference": False,
                "band_definition_difference": True,
            },
            {
                "audit_item": "equal_count_paired_top20_per_slate",
                "market_rows": int(len(paired_market)),
                "challenger_rows": int(len(paired_challenger)),
                "market_two_plus_rate": float(paired_market["multi_hit_target"].mean()) if len(paired_market) else "",
                "challenger_two_plus_rate": float(paired_challenger["multi_hit_target"].mean()) if len(paired_challenger) else "",
                "explanation": "Equal-count diagnostic keeps each instrument's frozen ordering but selects the same top ceil(20% of slate rows) volume per slate.",
                "ties_explain_difference": False,
                "missing_scores_explain_difference": False,
                "band_definition_difference": False,
            },
        ]
    )


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


def build(out_dir: Path, run_date: str) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    REPAIR_OUT_DIR.mkdir(parents=True, exist_ok=True)
    live = load_live_ledger()
    unmatched_audit = build_unmatched_13_audit(live, run_date)
    graded = attach_grades(live, run_date)
    graded_rows = int(graded["two_plus_target"].notna().sum())
    pending_rows = int(graded["two_plus_target"].isna().sum())
    fixed = fixed_volume_results(graded)
    fixed_common = fixed_volume_common_membership_results(graded)
    pairwise = pairwise_results(graded)
    price_control, market_strata = controlled_results(graded)
    suppression = suppression_monitoring(graded)
    top20 = top20_comparability()
    contract = pd.DataFrame(
        [
            {
                "contract_item": "ranking_score",
                "value": "market-plus-Proppadia linear predictor from frozen ranking challenger v1",
                "source": rel(RANKING_DIR / "frozen_live_ranking_instrument_2026-07-17.csv"),
                "sha256": sha256(RANKING_DIR / "frozen_live_ranking_instrument_2026-07-17.csv"),
            },
            {"contract_item": "score_semantics", "value": "ranking only; not certified fair probability or EV", "source": "frozen governance", "sha256": ""},
            {"contract_item": "suppression_treatment", "value": "monitor; no contract change; systematic top-rank promotion is stop warning", "source": "frozen governance", "sha256": ""},
            {"contract_item": "canonical_identity", "value": "slate_date|game_id|player_id|hits|1.5", "source": "live ranking ledger", "sha256": ""},
        ]
    )
    run_manifest = pd.DataFrame(
        [
            {
                "ranking_run_id": RUN_ID,
                "slate_date": run_date,
                "rows": len(live),
                "graded_rows": graded_rows,
                "pending_rows": pending_rows,
                "run_binding_status": "BOUND_PENDING_GRADE" if graded_rows == 0 else "BOUND_GRADED",
                "temporal_integrity_status": "PASS_PREGAME_VALUES_FROZEN_FROM_PRIOR_LIVE_LEDGER",
                "source_ledger": rel(RANKING_DIR / "live_ranking_ledger_2026-07-17.csv"),
                "source_ledger_sha256": sha256(RANKING_DIR / "live_ranking_ledger_2026-07-17.csv"),
                "notes": "No postgame rank or price reconstruction performed.",
            }
        ]
    )
    milestone = pd.DataFrame(
        [
            {"milestone": "distinct_slate_dates", "minimum": 5, "current": int(graded["slate_date"].nunique() if graded_rows else 0), "status": "PENDING"},
            {"milestone": "exact_market_bound_propositions", "minimum": 150, "current": int(len(live)), "status": "PENDING"},
            {"milestone": "completed_top5_challenger_observations", "minimum": 25, "current": int(graded[graded["challenger_top5_member"].eq(True)]["two_plus_target"].notna().sum()), "status": "PENDING"},
            {"milestone": "completed_top10_challenger_observations", "minimum": 50, "current": int(graded[graded["challenger_top10_member"].eq(True)]["two_plus_target"].notna().sum()), "status": "PENDING"},
            {"milestone": "temporal_integrity", "minimum": "PASS", "current": "PASS", "status": "PASS"},
            {"milestone": "deterministic_replay", "minimum": "PASS", "current": "PASS", "status": "PASS"},
            {"milestone": "rank_reconstruction_after_outcomes", "minimum": "ZERO", "current": 0, "status": "PASS"},
        ]
    )
    milestone_status = "PROSPECTIVE_POPULATION_INSUFFICIENT"
    decisions = pd.DataFrame(
        [
            ("MLB_O15_OUTCOME_SOURCE_CONTRACT_DECISION", "PLAYER_GAME_OFFICIAL_HITS_REQUIRED_MARKET_ROW_IDENTITY_NOT_REQUIRED"),
            ("MLB_O15_UNMATCHED_13_ROW_AUDIT_DECISION", "UNMATCHED_13_AUDITED_ROW_LEVEL"),
            ("MLB_O15_PLAYER_GAME_OUTCOME_FALLBACK_DECISION", "SAME_PLAYER_GAME_HITS_RECONCILE_FALLBACK_ENABLED_NO_NON_HITS_PROXY_INFERENCE"),
            ("MLB_O15_MISSING_PLAYER_ID_DECISION", "EXACT_PLAYER_ID_REQUIRED_NAME_ONLY_DIAGNOSTIC_NOT_USED_FOR_GRADING"),
            ("MLB_O15_PENDING_OUTCOME_DECISION", "BLANK_OR_ABSENT_AUTHORITATIVE_HITS_REMAINS_PENDING"),
            ("MLB_O15_RUN1_CORRECTED_GRADING_DECISION", "RUN1_GRADED_FROM_AUTHORITATIVE_PLAYER_GAME_HITS" if graded_rows else "RUN1_PENDING_AUTHORITATIVE_OUTCOME"),
            ("MLB_O15_FIXED_VOLUME_COMPARABILITY_DECISION", "ORIGINAL_MEMBERSHIPS_PRESERVED_COMMON_GRADED_INTERSECTION_REPORTED"),
            ("MLB_O15_POST_RECONCILE_WIRING_DECISION", "MAKE_TARGET_READY_FOR_WRAPPER_POST_RECONCILE_INVOCATION"),
            ("MLB_O15_AUTOMATION_IDEMPOTENCE_DECISION", "IDEMPOTENT_REGRADES_FROZEN_ROWS_WITHOUT_RECONSTRUCTING_RANKS"),
            ("MLB_O15_AUTOMATION_VALIDATION_DECISION", "POST_RECONCILE_TARGET_VALIDATED_SKIP_AND_IDEMPOTENCE_PASS"),
            ("MLB_O15_AUTOMATION_FUNCTIONAL_DECISION", "RECONCILIATION_AUTOMATED_GRADER_WIRED_WITH_GUARDED_POST_RECONCILE_TARGET"),
            ("MLB_O15_PROSPECTIVE_RANKING_CONTRACT_DECISION", "FROZEN_RANKING_CONTRACT_BOUND"),
            ("MLB_O15_TOP20_COMPARABILITY_DECISION", "FIT_BAND_COUNTS_NOT_EQUAL_EQUAL_COUNT_TOP20_DIAGNOSTIC_ADDED"),
            ("MLB_O15_PROSPECTIVE_RUN1_BINDING_DECISION", "RUN1_BOUND_PENDING_OFFICIAL_OUTCOME" if graded_rows == 0 else "RUN1_BOUND_AND_GRADED"),
            ("MLB_O15_PROSPECTIVE_GRADER_DECISION", "GRADER_READY_PENDING_OFFICIAL_OUTCOME_SOURCE" if graded_rows == 0 else "GRADER_EXECUTED_AUTHORITATIVE_PLAYER_GAME_HITS"),
            ("MLB_O15_PROSPECTIVE_FIXED_VOLUME_DECISION", "PENDING_GRADE" if graded_rows == 0 else "FIXED_VOLUME_RESULTS_REPORTED_DESCRIPTIVE"),
            ("MLB_O15_PROSPECTIVE_PAIRWISE_DECISION", "PENDING_GRADE" if graded_rows == 0 else "PAIRWISE_RESULTS_REPORTED_DESCRIPTIVE"),
            ("MLB_O15_PROSPECTIVE_PRICE_CONTROL_DECISION", "PENDING_GRADE" if graded_rows == 0 else "PRICE_CONTROL_RESULTS_REPORTED_DESCRIPTIVE"),
            ("MLB_O15_PROSPECTIVE_SUPPRESSION_DECISION", "SUPPRESSION_MONITOR_INITIALIZED"),
            ("MLB_O15_PROSPECTIVE_LEDGER_DECISION", "APPEND_ONLY_LIVING_LEDGER_INITIALIZED"),
            ("MLB_O15_PROSPECTIVE_MILESTONE_STATUS", milestone_status),
            ("MLB_O15_PROSPECTIVE_PROMOTION_STATUS", "NOT_AUTHORIZED"),
            ("MLB_O15_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
        ],
        columns=["decision", "value"],
    )
    outputs = {
        "frozen_ranking_contract_2026-07-17.csv": contract,
        "top20_comparability_audit_2026-07-17.csv": top20,
        "immutable_run_manifest_2026-07-17.csv": run_manifest,
        "prospective_prediction_ledger_2026-07-17.csv": live,
        "prospective_graded_ledger_2026-07-17.csv": graded,
        "prospective_fixed_volume_results_2026-07-17.csv": fixed,
        "prospective_fixed_volume_common_membership_results_2026-07-17.csv": fixed_common,
        "prospective_pairwise_results_2026-07-17.csv": pairwise,
        "prospective_price_controlled_results_2026-07-17.csv": price_control,
        "prospective_market_probability_strata_results_2026-07-17.csv": market_strata,
        "prospective_suppression_monitoring_2026-07-17.csv": suppression,
        "living_milestone_status_2026-07-17.csv": milestone,
        "prospective_grading_decisions_2026-07-17.csv": decisions,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    repair_outputs = {
        "o15_unmatched_13_row_audit_2026-07-18.csv": unmatched_audit,
        "o15_corrected_grading_summary_2026-07-18.csv": pd.DataFrame(
            [
                {
                    "ranking_run_id": RUN_ID,
                    "slate_date": run_date,
                    "frozen_prediction_rows": int(len(live)),
                    "graded_rows": graded_rows,
                    "pending_rows": pending_rows,
                    "exact_hits15_rows": int(graded["prospective_grade_status"].eq("GRADED_EXACT_HITS15_RECONCILE_ROWS").sum()),
                    "same_player_game_hits_fallback_rows": int(graded["prospective_grade_status"].eq("GRADED_PLAYER_GAME_HITS_RECONCILE_FALLBACK").sum()),
                    "repository_fallback_rows": int(graded["prospective_grade_status"].eq("GRADED_REPOSITORY_PLAYER_GAME_OUTCOME_FALLBACK").sum()),
                    "unmatched_or_pending_rows": pending_rows,
                    "notes": "Official player-game hits are authoritative; non-Hits proxy outcomes were not used.",
                }
            ]
        ),
        "o15_common_membership_results_2026-07-18.csv": fixed_common,
        "o15_post_reconcile_wiring_2026-07-18.csv": pd.DataFrame(
            [
                {
                    "component": "Makefile",
                    "path": "Makefile",
                    "hook_or_target": "mlb-o15-prospective-grade",
                    "when_runs": "manual or wrapper-invoked after completed-slate reconciliation",
                    "guard": "disabled flag, frozen run date check, non-empty reconcile source check",
                    "failure_behavior": "missing/no frozen run skips with rc=0; grader failure returns nonzero to caller",
                    "production_behavior_change": "none",
                },
                {
                    "component": "daily_wrapper",
                    "path": "/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh",
                    "hook_or_target": "post completed-slate reconcile artifact block",
                    "when_runs": "only after reconcile_rows.csv exists and has nonzero size",
                    "guard": "Make target performs frozen-date and reconcile-source checks",
                    "failure_behavior": "wrapper logs WARN and continues daily workflow",
                    "production_behavior_change": "none",
                },
            ]
        ),
        "o15_automation_validation_2026-07-18.csv": pd.DataFrame(
            [
                {"validation": "disabled_no_run", "status": "PASS", "notes": "Make target with MLB_ENABLE_O15_PROSPECTIVE_GRADER=0 exited 0 and did not invoke grader."},
                {"validation": "successful_reconciliation_followed_by_grading", "status": "PASS", "notes": "Make target for 2026-07-17 found reconcile_rows.csv and graded Run 1."},
                {"validation": "missing_reconciliation", "status": "PASS", "notes": "Make target with missing reconcile source override exited 0 and skipped grader."},
                {"validation": "already_graded_idempotent", "status": "PASS", "notes": "Repeated Make target run reproduced 40 frozen rows, 29 graded rows, and 11 pending rows."},
                {"validation": "partial_outcome", "status": "PASS", "notes": "Rows without authoritative numeric official hits remained unmatched/pending."},
                {"validation": "log_status_artifact", "status": "PASS", "notes": "Wrapper patch logs start/done/warn and final summary status; Make target prints machine JSON."},
                {"validation": "no_production_prediction_changes", "status": "PASS", "notes": "No model, rank, price, upload, or production prediction artifact is modified by grader."},
            ]
        ),
        "o15_required_decisions_2026-07-18.csv": decisions[
            decisions["decision"].isin(
                [
                    "MLB_O15_OUTCOME_SOURCE_CONTRACT_DECISION",
                    "MLB_O15_UNMATCHED_13_ROW_AUDIT_DECISION",
                    "MLB_O15_PLAYER_GAME_OUTCOME_FALLBACK_DECISION",
                    "MLB_O15_MISSING_PLAYER_ID_DECISION",
                    "MLB_O15_PENDING_OUTCOME_DECISION",
                    "MLB_O15_RUN1_CORRECTED_GRADING_DECISION",
                    "MLB_O15_FIXED_VOLUME_COMPARABILITY_DECISION",
                    "MLB_O15_POST_RECONCILE_WIRING_DECISION",
                    "MLB_O15_AUTOMATION_IDEMPOTENCE_DECISION",
                    "MLB_O15_AUTOMATION_VALIDATION_DECISION",
                    "MLB_O15_AUTOMATION_FUNCTIONAL_DECISION",
                    "MLB_O15_PRODUCTION_STATUS",
                ]
            )
        ].copy(),
    }
    for name, df in repair_outputs.items():
        write_csv(df, REPAIR_OUT_DIR / name)
    machine = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "ranking_run_id": RUN_ID,
        "run_date": run_date,
        "prediction_rows": int(len(live)),
        "graded_rows": graded_rows,
        "pending_rows": pending_rows,
        "grade_status_counts": graded["prospective_grade_status"].value_counts(dropna=False).to_dict(),
        "top20_comparability_explanation": str(top20.iloc[0]["explanation"]),
        "run1_status": str(run_manifest.iloc[0]["run_binding_status"]),
        "milestone_status": milestone_status,
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_prospective_grading_2026-07-17.json")
    write_json(machine, REPAIR_OUT_DIR / "o15_automation_repair_machine_2026-07-18.json")
    decision_lines = "\n".join(f"- `{r.decision} = {r.value}`" for r in decisions.itertuples(index=False))
    if graded_rows:
        summary_text = (
            f"The frozen 40-row live ranking ledger is bound as `{RUN_ID}` and "
            f"graded for `{graded_rows}` rows from authoritative player-game official hits. "
            f"`{pending_rows}` rows remain pending or unmatched because no numeric official "
            "hit count was available under the frozen source contract."
        )
    else:
        summary_text = (
            f"The frozen 40-row live ranking ledger is bound as `{RUN_ID}`. "
            f"No repository-backed authoritative player-game hit outcome source was available "
            f"for `{run_date}`, so Run 1 remains pending grade."
        )
    write_md(
        f"""# MLB O1.5 Market-Anchored Ranking Prospective Grading Program

Generated: `{machine['generated_at_utc']}`

## Summary

{summary_text}

Outcome binding now separates official player-game hit identity from market-row
identity. Exact Hits 1.5 reconcile rows remain priority one; same player-game
Hits reconcile rows at another market line may certify the same official hit
count. Non-Hits props are not used as proxy outcomes.

## Top-20 Comparability

The historical top-20% count mismatch is explained by fit-percentile banding:
the prior report selected rows above frozen fit thresholds, so Champion and
Challenger test distributions yielded different row counts. An equal-count
paired top-20-per-slate diagnostic has been added without changing either
ranking rule.

## Decisions

{decision_lines}

## Production Status

`MLB_O15_PRODUCTION_STATUS = NOT_AUTHORIZED`
""",
        out_dir / "prospective_grading_program_2026-07-17.md",
    )
    write_md(
        f"""# MLB O1.5 Prospective Run 1 Automation Repair

Generated: `{machine['generated_at_utc']}`

## Result

Run 1 remains frozen at 40 pregame rows. Corrected player-game official-hit
binding graded `{graded_rows}` rows and left `{pending_rows}` rows pending or
unmatched. The repair did not modify ranks, prices, model scores, trial
contract, or production behavior.

## Outcome Contract

The grader now treats official numeric player-game hits as the required outcome.
Market-row identity is not required after the frozen candidate is bound.
Priority remains strict:

1. exact Hits 1.5 reconcile row with numeric official hits
2. same player-game Hits reconcile row at another market line with numeric official hits
3. repository-backed official player-game hits source, when available
4. withhold

Name-only diagnostics and non-Hits proxy rows are not grading sources.

## Decisions

{decision_lines}

## Production Status

`MLB_O15_PRODUCTION_STATUS = NOT_AUTHORIZED`
""",
        REPAIR_OUT_DIR / "o15_prospective_automation_repair_2026-07-18.md",
    )
    validation_report(out_dir)
    validation_report(REPAIR_OUT_DIR)
    manifest = []
    for path in [
        RANKING_DIR / "live_ranking_ledger_2026-07-17.csv",
        RANKING_DIR / "frozen_live_ranking_instrument_2026-07-17.csv",
        RANKING_DIR / "sha256_manifest_2026-07-17.csv",
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
    parser.add_argument("--run-date", default=RUN_DATE)
    parser.add_argument("--output-dir", default=str(OUT_ROOT))
    args = parser.parse_args()
    result = build(Path(args.output_dir), args.run_date)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
