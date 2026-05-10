#!/usr/bin/env python3
"""Run fixed hits lane-selector rules across all available forward dates.

No model or threshold changes. This script uses the same lane rules as
build_mlb_hits_lane_selector.py, but rebuilds ranked lanes over the expanded
available window to evaluate stability over time.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine, text


DEFAULT_TRAIN_AUDIT = Path("backend/mlb/exports/model_v2/ranking/audits/hits_residual_feature_audit.csv")
DEFAULT_RESIDUAL_MODEL = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib")
DEFAULT_RESIDUAL_FEATURES = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker_features.json")
DEFAULT_RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_QUICK_CARD_ROOT = Path("backend/mlb/exports/quick_card")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_v2/lanes/hits_lane_selector_expanded_validation.csv")
DEFAULT_SUMMARY_JSON = Path("backend/mlb/exports/model_v2/lanes/hits_lane_selector_expanded_summary.json")

EXCLUDE_EXACT = {
    "actual_value",
    "residual",
    "under_win",
    "player_id",
    "game_id",
    "player_id_key",
    "game_id_key",
    "game_date",
    "date",
    "player_name",
    "prop_type",
    "prop_type_norm",
    "side",
    "source_reconcile_file",
    "joined_to_player_derived_stats",
}
EXCLUDE_SUBSTRINGS = (
    "outcome",
    "pnl",
    "profit",
    "odds",
    "price",
    "model_prob",
    "implied",
    "fair",
    "bookmaker",
    "market",
)


def _db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise SystemExit("DATABASE_URL or SUPABASE_DB_URL must be set.")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _norm_name(value: Any) -> str:
    return " ".join(_clean(value).lower().split())


def _line_key(value: Any) -> float | None:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return None
    return float(round(float(val), 3))


def _discover_dates(root: Path, include_late_2025: bool) -> list[str]:
    dates: list[str] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        parsed = pd.to_datetime(date, errors="coerce")
        if pd.isna(parsed):
            continue
        if date.startswith("2026-") or (include_late_2025 and date >= "2025-09-29" and date.startswith("2025-")):
            dates.append(date)
    return sorted(set(dates))


def _load_reconcile(root: Path, dates: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for date in dates:
        path = root / date / "reconcile_rows.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path, low_memory=False)
        df["source_reconcile_file"] = str(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["date_norm"] = out["game_date"].map(_date_key)
    out["player_name_norm"] = out["player_name"].map(_norm_name)
    out["prop_type_norm"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["line_norm"] = out["line"].map(_line_key)
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["actual_value"] = pd.to_numeric(out["actual_value"], errors="coerce")
    return out


def _load_player_derived_stats(engine, game_ids: list[int], chunk_size: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with engine.connect() as conn:
        for chunk in _chunks(game_ids, chunk_size):
            part = pd.read_sql(
                text("SELECT * FROM mlb.player_derived_stats WHERE game_id = ANY(:game_ids)"),
                conn,
                params={"game_ids": chunk},
            )
            if not part.empty:
                frames.append(part)
    if not frames:
        return pd.DataFrame(columns=["player_id", "game_id"])
    out = pd.concat(frames, ignore_index=True)
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    out["game_id"] = pd.to_numeric(out["game_id"], errors="coerce")
    return out.drop_duplicates(["player_id", "game_id"], keep="last")


def _join_pds(rows: pd.DataFrame, chunk_size: int) -> pd.DataFrame:
    if rows.empty:
        return rows
    game_ids = sorted({int(v) for v in rows.loc[rows["game_id_key"].notna(), "game_id_key"].tolist()})
    pds = _load_player_derived_stats(create_engine(_db_url()), game_ids, chunk_size)
    pds = pds.rename(columns={c: f"pds_{c}" for c in pds.columns if c not in {"player_id", "game_id"}})
    pds["player_id_key"] = pd.to_numeric(pds["player_id"], errors="coerce").astype("Int64")
    pds["game_id_key"] = pd.to_numeric(pds["game_id"], errors="coerce").astype("Int64")
    pds = pds.drop(columns=["player_id", "game_id"], errors="ignore")
    joined = rows.merge(pds, on=["player_id_key", "game_id_key"], how="left", indicator="pds_join_status")
    joined["joined_to_player_derived_stats"] = joined["pds_join_status"].eq("both")
    return joined.drop(columns=["pds_join_status"])


def _train_direct_under_model(train_audit: Path, random_state: int) -> tuple[Pipeline, list[str]]:
    train = pd.read_csv(train_audit, low_memory=False)
    train["prop_type_norm"] = train["prop_type"].astype(str).str.strip().str.lower()
    train["line"] = pd.to_numeric(train["line"], errors="coerce")
    train["actual_value"] = pd.to_numeric(train["actual_value"], errors="coerce")
    joined = (
        train["joined_to_player_derived_stats"].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
        if "joined_to_player_derived_stats" in train.columns
        else pd.Series(True, index=train.index)
    )
    train = train[train["prop_type_norm"].eq("hits") & train["line"].eq(0.5) & train["actual_value"].notna() & joined].copy()
    train["under_win"] = train["actual_value"].eq(0).astype(int)
    feature_cols: list[str] = []
    for col in train.columns:
        low = col.strip().lower()
        if low in EXCLUDE_EXACT:
            continue
        if any(part in low for part in EXCLUDE_SUBSTRINGS):
            continue
        if pd.to_numeric(train[col], errors="coerce").notna().any():
            feature_cols.append(col)
    feature_cols = ["line"] + sorted(c for c in feature_cols if c != "line")
    model = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            (
                "model",
                HistGradientBoostingClassifier(
                    learning_rate=0.06,
                    max_iter=250,
                    min_samples_leaf=30,
                    l2_regularization=0.01,
                    random_state=random_state,
                ),
            ),
        ]
    )
    model.fit(train[feature_cols].apply(pd.to_numeric, errors="coerce"), train["under_win"].astype(int))
    return model, feature_cols


def _score_under_lane(reconcile_pds: pd.DataFrame, train_audit: Path, random_state: int) -> pd.DataFrame:
    model, feature_cols = _train_direct_under_model(train_audit, random_state)
    rows = reconcile_pds[
        reconcile_pds["prop_type_norm"].eq("hits")
        & reconcile_pds["line"].eq(0.5)
        & reconcile_pds["actual_value"].notna()
        & reconcile_pds["joined_to_player_derived_stats"]
    ].copy()
    for col in feature_cols:
        if col not in rows.columns:
            rows[col] = np.nan
    rows["under_win_score"] = model.predict_proba(rows[feature_cols].apply(pd.to_numeric, errors="coerce"))[:, 1]
    rows = rows.sort_values(["date_norm", "under_win_score"], ascending=[True, False])
    rows["rank_position"] = rows.groupby("date_norm")["under_win_score"].rank(method="first", ascending=False).astype(int)
    rows["rank_percentile"] = rows.groupby("date_norm")["under_win_score"].rank(method="average", pct=True, ascending=True)
    rows["rank_bucket"] = pd.qcut(rows["rank_percentile"].rank(method="first"), q=min(10, len(rows)), labels=False, duplicates="drop") + 1
    rows = rows[pd.to_numeric(rows["rank_bucket"], errors="coerce").eq(10)].copy()
    rows["date"] = rows["date_norm"]
    rows["player"] = rows["player_name"]
    rows["side"] = "under"
    rows["source_lane"] = "direct_hitless_under_05_top_decile"
    rows["win_rate_estimate"] = rows["under_win_score"]
    rows["odds"] = pd.to_numeric(rows["price_under_american"], errors="coerce")
    rows["actual_win"] = rows["actual_value"].eq(0)
    rows["pnl"] = pd.to_numeric(rows.get("pnl_under_1u"), errors="coerce")
    return _selector_cols(rows)


def _score_over_lane(reconcile_pds: pd.DataFrame, model_path: Path, features_path: Path) -> pd.DataFrame:
    artifact = joblib.load(model_path)
    model = artifact["model"] if isinstance(artifact, dict) and "model" in artifact else artifact
    feature_payload = json.loads(features_path.read_text(encoding="utf-8"))
    feature_cols = list(feature_payload.get("feature_columns") or artifact.get("feature_columns") or [])
    rows = reconcile_pds[
        reconcile_pds["prop_type_norm"].eq("hits")
        & reconcile_pds["actual_value"].notna()
        & reconcile_pds["line"].notna()
        & reconcile_pds["joined_to_player_derived_stats"]
    ].copy()
    for col in feature_cols:
        if col not in rows.columns:
            rows[col] = np.nan
    rows["predicted_residual"] = model.predict(rows[feature_cols].apply(pd.to_numeric, errors="coerce"))
    rows["rank_score"] = rows["predicted_residual"]
    rows = rows.sort_values(["date_norm", "rank_score"], ascending=[True, False])
    rows["rank_position"] = rows.groupby("date_norm")["rank_score"].rank(method="first", ascending=False).astype(int)
    rows["rank_percentile"] = rows.groupby("date_norm")["rank_score"].rank(method="average", pct=True, ascending=True)
    rows["rank_bucket"] = np.ceil(rows["rank_percentile"] * 10.0).clip(1, 10).astype(int)
    rows = rows[rows["rank_bucket"].eq(9)].copy()
    rows["date"] = rows["date_norm"]
    rows["player"] = rows["player_name"]
    rows["side"] = "over"
    rows["source_lane"] = "residual_ranker_over_bucket_9"
    rows["win_rate_estimate"] = np.nan
    rows["odds"] = pd.to_numeric(rows["price_over_american"], errors="coerce")
    rows["actual_win"] = rows["actual_over_outcome"].astype(str).str.strip().str.lower().eq("win")
    rows["pnl"] = pd.to_numeric(rows.get("pnl_over_1u"), errors="coerce")
    return _selector_cols(rows)


def _load_quick_card(root: Path, reconcile: pd.DataFrame, dates: list[str]) -> pd.DataFrame:
    if reconcile.empty or not root.exists():
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for date in dates:
        path = root / date / "quick_card.csv"
        if not path.exists():
            continue
        qc = pd.read_csv(path, low_memory=False)
        required = {"date", "player_name", "prop_type", "side", "line"}
        if not required.issubset(qc.columns):
            continue
        qc = qc[qc["prop_type"].astype(str).str.strip().str.lower().eq("hits")].copy()
        if qc.empty:
            continue
        qc["date_norm"] = qc["date"].map(_date_key)
        qc["player_name_norm"] = qc["player_name"].map(_norm_name)
        qc["prop_type_norm"] = qc["prop_type"].astype(str).str.strip().str.lower()
        qc["side_norm"] = qc["side"].astype(str).str.strip().str.lower()
        qc["line_norm"] = qc["line"].map(_line_key)
        frames.append(qc)
    if not frames:
        return pd.DataFrame()
    quick = pd.concat(frames, ignore_index=True)
    rec = reconcile[reconcile["prop_type_norm"].eq("hits")].drop_duplicates(
        ["date_norm", "player_name_norm", "prop_type_norm", "line_norm"],
        keep="first",
    )
    merged = quick.merge(
        rec,
        on=["date_norm", "player_name_norm", "prop_type_norm", "line_norm"],
        how="left",
        suffixes=("_quick", "_rec"),
    )
    merged = merged[merged["side_norm"].isin({"over", "under"})].copy()
    resolved = np.where(
        merged["side_norm"].eq("over"),
        merged.get("actual_over_outcome"),
        merged.get("actual_under_outcome"),
    )
    resolved = pd.Series(resolved, index=merged.index).astype(str).str.strip().str.lower()
    merged = merged[resolved.isin({"win", "loss"})].copy()
    if merged.empty:
        return pd.DataFrame()
    merged["date"] = merged["date_norm"]
    merged["player"] = merged.get("player_name_quick", merged.get("player_name"))
    merged["prop_type"] = "hits"
    merged["side"] = merged["side_norm"]
    merged["line"] = merged["line_norm"]
    merged["source_lane"] = "quick_card_hits"
    merged["rank_bucket"] = pd.NA
    merged["win_rate_estimate"] = pd.to_numeric(merged.get("model_prob"), errors="coerce")
    merged["odds"] = np.where(
        merged["side"].eq("over"),
        pd.to_numeric(merged.get("price_over_american"), errors="coerce"),
        pd.to_numeric(merged.get("price_under_american"), errors="coerce"),
    )
    merged["actual_win"] = np.where(
        merged["side"].eq("over"),
        merged["actual_over_outcome"].astype(str).str.lower().eq("win"),
        merged["actual_under_outcome"].astype(str).str.lower().eq("win"),
    )
    merged["pnl"] = np.where(
        merged["side"].eq("over"),
        pd.to_numeric(merged.get("pnl_over_1u"), errors="coerce"),
        pd.to_numeric(merged.get("pnl_under_1u"), errors="coerce"),
    )
    return _selector_cols(merged)


def _selector_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "date",
        "player",
        "prop_type",
        "side",
        "line",
        "source_lane",
        "rank_bucket",
        "win_rate_estimate",
        "odds",
        "actual_win",
        "pnl",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan
    out = df[cols].copy()
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["odds"] = pd.to_numeric(out["odds"], errors="coerce")
    out["pnl"] = pd.to_numeric(out["pnl"], errors="coerce")
    return out


def _metric(df: pd.DataFrame, group: str, value: str) -> dict[str, Any]:
    bets = int(len(df))
    wins = int(df["actual_win"].sum()) if bets else 0
    profit = float(pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0).sum()) if bets else 0.0
    return {
        "group": group,
        "value": value,
        "bets": bets,
        "wins": wins,
        "win_rate": float(wins / bets) if bets else None,
        "profit_units": profit,
        "roi": float(profit / bets) if bets else None,
        "avg_odds": float(pd.to_numeric(df["odds"], errors="coerce").mean(skipna=True)) if bets else None,
    }


def _daily_metrics(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for date, group in selected.groupby("date", dropna=False):
        row = _metric(group, "by_day", str(date))
        row["date"] = str(date)
        rows.append(row)
    out = pd.DataFrame(rows).sort_values("date")
    out["cumulative_bets"] = out["bets"].cumsum()
    out["cumulative_profit_units"] = out["profit_units"].cumsum()
    out["cumulative_roi"] = out["cumulative_profit_units"] / out["cumulative_bets"].replace(0, np.nan)
    running_peak = out["cumulative_profit_units"].cummax()
    out["drawdown_units"] = out["cumulative_profit_units"] - running_peak
    out["rolling_7d_profit_units"] = out["profit_units"].rolling(7, min_periods=1).sum()
    out["rolling_7d_bets"] = out["bets"].rolling(7, min_periods=1).sum()
    out["rolling_7d_roi"] = out["rolling_7d_profit_units"] / out["rolling_7d_bets"].replace(0, np.nan)
    return out


def _weekly_metrics(selected: pd.DataFrame) -> pd.DataFrame:
    work = selected.copy()
    work["week"] = pd.to_datetime(work["date"], errors="coerce").dt.to_period("W-SUN").astype(str)
    return pd.DataFrame([_metric(g, "by_week", str(w)) | {"week": str(w)} for w, g in work.groupby("week", dropna=False)])


def _lane_time(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (date, lane), group in selected.groupby(["date", "source_lane"], dropna=False):
        row = _metric(group, "by_day_lane", f"{date}|{lane}")
        row["date"] = str(date)
        row["source_lane"] = str(lane)
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["date", "source_lane"])


def run(args: argparse.Namespace) -> dict[str, Any]:
    dates = _discover_dates(Path(args.reconcile_root), args.include_late_2025)
    if args.from_date:
        dates = [d for d in dates if d >= args.from_date]
    if args.to_date:
        dates = [d for d in dates if d <= args.to_date]
    if not dates:
        raise SystemExit("No reconcile dates found for expanded validation.")
    reconcile = _load_reconcile(Path(args.reconcile_root), dates)
    hits = reconcile[reconcile["prop_type_norm"].eq("hits") & reconcile["actual_value"].notna() & reconcile["line"].notna()].copy()
    hits_pds = _join_pds(hits, args.chunk_size)

    frames = [
        _score_under_lane(hits_pds, Path(args.training_audit_csv), args.random_state),
        _score_over_lane(hits_pds, Path(args.residual_model), Path(args.residual_features)),
    ]
    if args.include_quick_card:
        frames.append(_load_quick_card(Path(args.quick_card_root), reconcile, dates))
    selected = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    selected = selected.sort_values(["date", "source_lane", "player", "side", "line"]).copy()
    daily = _daily_metrics(selected)
    weekly = _weekly_metrics(selected)
    lane_time = _lane_time(selected)
    selected = selected.merge(
        daily[["date", "cumulative_bets", "cumulative_profit_units", "cumulative_roi", "drawdown_units", "rolling_7d_roi"]],
        on="date",
        how="left",
    )

    out_csv = Path(args.out_csv)
    summary_json = Path(args.summary_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    selected.to_csv(out_csv, index=False)

    summary_metrics = [_metric(selected, "overall", "all")]
    for lane, group in selected.groupby("source_lane", dropna=False):
        summary_metrics.append(_metric(group, "by_lane", str(lane)))
    summary = {
        "training_audit_csv": str(args.training_audit_csv),
        "residual_model": str(args.residual_model),
        "residual_features": str(args.residual_features),
        "reconcile_root": str(args.reconcile_root),
        "quick_card_root": str(args.quick_card_root),
        "include_late_2025": bool(args.include_late_2025),
        "include_quick_card": bool(args.include_quick_card),
        "date_min": min(dates),
        "date_max": max(dates),
        "available_dates": dates,
        "out_csv": str(out_csv),
        "summary_json": str(summary_json),
        "rules": {
            "under_0_5": "direct hitless classifier, global top decile only",
            "over": "residual ranker over rows, within-date rank bucket 9 only",
            "quick_card": "all matched resolved hits Quick Card rows when available",
        },
        "metrics": summary_metrics,
        "roi_by_day": daily.to_dict(orient="records"),
        "roi_by_week": weekly.to_dict(orient="records"),
        "cumulative_roi_curve": daily[
            ["date", "cumulative_bets", "cumulative_profit_units", "cumulative_roi", "drawdown_units", "rolling_7d_roi"]
        ].to_dict(orient="records"),
        "max_drawdown_units": float(daily["drawdown_units"].min()) if not daily.empty else None,
        "lane_contribution_over_time": lane_time.to_dict(orient="records"),
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expanded stability validation for fixed hits lane selector.")
    parser.add_argument("--training-audit-csv", default=str(DEFAULT_TRAIN_AUDIT))
    parser.add_argument("--residual-model", default=str(DEFAULT_RESIDUAL_MODEL))
    parser.add_argument("--residual-features", default=str(DEFAULT_RESIDUAL_FEATURES))
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECONCILE_ROOT))
    parser.add_argument("--quick-card-root", default=str(DEFAULT_QUICK_CARD_ROOT))
    parser.add_argument("--from-date", default="")
    parser.add_argument("--to-date", default="")
    parser.add_argument("--include-late-2025", action="store_true")
    parser.add_argument("--no-quick-card", action="store_true")
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--random-state", type=int, default=42)
    args = parser.parse_args()
    args.include_quick_card = not args.no_quick_card
    return args


def main() -> None:
    summary = run(parse_args())
    overall = next((m for m in summary["metrics"] if m["group"] == "overall"), {})
    print(f"Wrote {summary['out_csv']}")
    print(f"Wrote {summary['summary_json']}")
    print(
        "dates={date_min}..{date_max} bets={bets} win_rate={win_rate:.4f} roi={roi:.4f} max_dd={max_drawdown_units:.2f}".format(
            bets=overall.get("bets", 0),
            win_rate=overall.get("win_rate") or 0.0,
            roi=overall.get("roi") or 0.0,
            **summary,
        )
    )


if __name__ == "__main__":
    main()
