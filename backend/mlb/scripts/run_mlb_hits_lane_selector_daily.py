#!/usr/bin/env python3
"""Run the daily hits lane selector and export upload rows.

This is a lane-selection layer only. It uses frozen rules:
- UNDER 0.5: direct hitless model top decile.
- OVER: residual ranker bucket 9.
- Quick Card: include matched rows as a separate lane when available.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine, text

from backend.mlb.scripts import export_mlb_book_upload as book_upload
from backend.mlb.scripts import tool_upload_8rain


DEFAULT_TRAIN_AUDIT = Path("backend/mlb/exports/model_v2/ranking/audits/hits_residual_feature_audit.csv")
DEFAULT_RESIDUAL_MODEL = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib")
DEFAULT_RESIDUAL_FEATURES = Path("backend/mlb/exports/model_v2/ranking/hits_residual_ranker_features.json")
DEFAULT_DIRECT_UNDER_MODEL = Path("backend/mlb/exports/model_v2/ranking/hits_05_under_direct_target_model.joblib")
DEFAULT_RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_ODDS_HISTORY_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_QUICK_CARD_ROOT = Path("backend/mlb/exports/quick_card")
DEFAULT_OUT_DIR = Path("backend/mlb/exports/model_v2/lanes/today")
DEFAULT_UPLOAD_DIR = Path("backend/mlb/exports/model_v2/upload")

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


def _team_code(value: Any) -> str:
    return _clean(value).upper()


def _upload_team_code(value: Any) -> str:
    return book_upload._normalize_upload_team_code(value)


def _is_int_like(value: Any) -> bool:
    text_value = _clean(value)
    if not text_value:
        return False
    try:
        float_value = float(text_value)
    except ValueError:
        return False
    return float_value.is_integer()


def _resolve_player_team_code(
    player_team_value: Any,
    aliases_value: Any,
    home_raw: Any,
    away_raw: Any,
    home_upload: Any,
    away_upload: Any,
) -> str:
    player_team = _team_code(player_team_value)
    aliases = [_team_code(v) for v in _clean(aliases_value).split("|") if _team_code(v)]
    if not _is_int_like(player_team):
        return player_team
    for candidate in aliases:
        if candidate in {_team_code(home_raw), _team_code(away_raw)}:
            return candidate
    for candidate in aliases:
        if _upload_team_code(candidate) in {_team_code(home_upload), _team_code(away_upload)}:
            return candidate
    return aliases[0] if aliases else player_team


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


def _today_paths(date_value: str) -> tuple[Path, Path, Path]:
    dated_dir = DEFAULT_OUT_DIR / date_value
    out_csv = dated_dir / f"hits_lane_selector_{date_value}.csv"
    summary_json = dated_dir / f"hits_lane_selector_{date_value}_summary.json"
    upload_csv = DEFAULT_UPLOAD_DIR / date_value / f"ranking_tool_upload_{date_value}.csv"
    return out_csv, summary_json, upload_csv


def _quick_card_hits_path(date_value: str) -> Path:
    return DEFAULT_OUT_DIR / date_value / f"quick_card_hits_{date_value}.csv"


def _timestamped_artifact_path(path: Path, run_tag: str) -> Path:
    return path.with_name(f"{path.stem}__{run_tag}{path.suffix}")


def _archive_lane_artifacts(paths: list[Path], run_tag: str) -> list[str]:
    archived: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        timestamped = _timestamped_artifact_path(path, run_tag)
        timestamped.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, timestamped)
        archived.append(str(timestamped))
    return archived


def _default_input_csv(date_value: str) -> Path:
    return DEFAULT_RECONCILE_ROOT / date_value / "reconcile_rows.csv"


def _find_pregame_slate_csv(date_value: str) -> Path:
    root = DEFAULT_ODDS_HISTORY_ROOT / date_value
    preferred = root / "mlb_slate_output.csv"
    if preferred.exists():
        return preferred
    candidates = sorted(root.glob("mlb_slate_output*.csv"))
    if not candidates:
        raise SystemExit(f"No pregame slate CSV found under {root}")
    return sorted(candidates, key=lambda p: (p.stat().st_mtime, p.name))[-1]


def _resolve_input_and_mode(args: argparse.Namespace, date_value: str) -> tuple[Path, str]:
    reconcile_csv = _default_input_csv(date_value)
    if reconcile_csv.exists():
        return (Path(args.input_csv) if args.input_csv else reconcile_csv), "postgame"
    print("Running in PRE-GAME mode (no reconcile data found)")
    return (Path(args.input_csv) if args.input_csv else _find_pregame_slate_csv(date_value)), "pregame"


def _load_today_rows(input_csv: Path, date_value: str) -> pd.DataFrame:
    if not input_csv.exists():
        raise SystemExit(f"Missing daily input CSV: {input_csv}")
    df = pd.read_csv(input_csv, low_memory=False)
    required = {"player_name", "prop_type", "line"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"{input_csv} missing required columns: {missing}")
    out = df.copy()
    for col in ("player_id", "game_id"):
        if col not in out.columns:
            out[col] = pd.NA
    if "game_date" not in out.columns:
        out["game_date"] = out["slate_date"] if "slate_date" in out.columns else date_value
    if "price_over_american" not in out.columns:
        out["price_over_american"] = out["fair_odds_over_american"] if "fair_odds_over_american" in out.columns else pd.NA
    if "price_under_american" not in out.columns:
        out["price_under_american"] = out["fair_odds_under_american"] if "fair_odds_under_american" in out.columns else pd.NA
    for col in ("actual_value", "actual_over_outcome", "actual_under_outcome", "pnl_over_1u", "pnl_under_1u"):
        if col not in out.columns:
            out[col] = pd.NA
    out["date_norm"] = out["game_date"].map(_date_key)
    out = out[out["date_norm"].eq(date_value)].copy()
    out["prop_type_norm"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["player_name_norm"] = out["player_name"].map(_norm_name)
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["line_norm"] = out["line"].map(_line_key)
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out = out[out["prop_type_norm"].eq("hits") & out["line"].notna()].copy()
    out = out.drop_duplicates(["date_norm", "player_id_key", "game_id_key", "prop_type_norm", "line_norm"], keep="first")
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


def _load_latest_player_derived_stats(engine, player_ids: list[int], date_value: str, chunk_size: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if not player_ids:
        return pd.DataFrame(columns=["player_id"])
    with engine.connect() as conn:
        for chunk in _chunks(player_ids, chunk_size):
            part = pd.read_sql(
                text(
                    """
                    SELECT DISTINCT ON (player_id) *
                    FROM mlb.player_derived_stats
                    WHERE player_id = ANY(:player_ids)
                      AND game_date::date <= CAST(:date_value AS date)
                    ORDER BY player_id, game_date DESC NULLS LAST, game_id DESC NULLS LAST
                    """
                ),
                conn,
                params={"player_ids": chunk, "date_value": date_value},
            )
            if not part.empty:
                frames.append(part)
    if not frames:
        return pd.DataFrame(columns=["player_id"])
    out = pd.concat(frames, ignore_index=True)
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    return out.drop_duplicates(["player_id"], keep="last")


def _load_player_team_codes(engine, game_ids: list[int], chunk_size: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if not game_ids:
        return pd.DataFrame(columns=["player_id_key", "game_id_key", "player_team"])
    with engine.connect() as conn:
        for chunk in _chunks(game_ids, chunk_size):
            part = pd.read_sql(
                text(
                    """
                    SELECT
                        pt.player_id,
                        pt.game_id,
                        tm.abbr AS player_team
                    FROM mlb.player_team_by_game pt
                    LEFT JOIN public.mlb_team_map tm
                      ON tm.team_id = pt.team_id
                    WHERE pt.game_id = ANY(:game_ids)
                    """
                ),
                conn,
                params={"game_ids": chunk},
            )
            if not part.empty:
                frames.append(part)
    if not frames:
        return pd.DataFrame(columns=["player_id_key", "game_id_key", "player_team"])
    out = pd.concat(frames, ignore_index=True)
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    return out.drop(columns=["player_id", "game_id"], errors="ignore").drop_duplicates(
        ["player_id_key", "game_id_key"], keep="last"
    )


def _load_team_id_aliases(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        aliases = pd.read_sql(
            text(
                """
                SELECT team_id, string_agg(abbr, '|' ORDER BY abbr) AS player_team_aliases
                FROM public.mlb_team_map
                GROUP BY team_id
                """
            ),
            conn,
        )
    aliases["pds_team_numeric"] = pd.to_numeric(aliases["team_id"], errors="coerce").astype("Int64")
    return aliases.drop(columns=["team_id"], errors="ignore")


def _join_pds(rows: pd.DataFrame, chunk_size: int) -> pd.DataFrame:
    if rows.empty:
        return rows
    date_value = str(rows["date_norm"].dropna().iloc[0]) if "date_norm" in rows.columns and rows["date_norm"].notna().any() else ""
    engine = create_engine(_db_url())
    game_ids = sorted({int(v) for v in rows.loc[rows["game_id_key"].notna(), "game_id_key"].tolist()})
    team_codes = _load_player_team_codes(engine, game_ids, chunk_size)
    if not team_codes.empty:
        rows = rows.merge(team_codes, on=["player_id_key", "game_id_key"], how="left")
    pds = _load_player_derived_stats(engine, game_ids, chunk_size)
    pds = pds.rename(columns={c: f"pds_{c}" for c in pds.columns if c not in {"player_id", "game_id"}})
    pds["player_id_key"] = pd.to_numeric(pds["player_id"], errors="coerce").astype("Int64")
    pds["game_id_key"] = pd.to_numeric(pds["game_id"], errors="coerce").astype("Int64")
    pds = pds.drop(columns=["player_id", "game_id"], errors="ignore")
    joined = rows.merge(pds, on=["player_id_key", "game_id_key"], how="left", indicator="pds_join_status")
    joined["joined_to_player_derived_stats"] = joined["pds_join_status"].eq("both")
    joined = joined.drop(columns=["pds_join_status"])

    missing = ~joined["joined_to_player_derived_stats"]
    if missing.any() and date_value:
        player_ids = sorted({int(v) for v in joined.loc[missing & joined["player_id_key"].notna(), "player_id_key"].tolist()})
        latest = _load_latest_player_derived_stats(engine, player_ids, date_value, chunk_size)
        if not latest.empty:
            latest = latest.rename(columns={c: f"pds_latest_{c}" for c in latest.columns if c != "player_id"})
            latest["player_id_key"] = pd.to_numeric(latest["player_id"], errors="coerce").astype("Int64")
            latest = latest.drop(columns=["player_id"], errors="ignore")
            joined = joined.merge(latest, on=["player_id_key"], how="left")
            latest_cols = [c for c in joined.columns if c.startswith("pds_latest_")]
            for latest_col in latest_cols:
                base_col = "pds_" + latest_col.removeprefix("pds_latest_")
                if base_col not in joined.columns:
                    joined[base_col] = pd.NA
                fill = joined[latest_col]
                joined[base_col] = joined[base_col].where(joined[base_col].notna(), fill)
            latest_any = joined[latest_cols].notna().any(axis=1) if latest_cols else pd.Series(False, index=joined.index)
            joined["joined_to_player_derived_stats"] = joined["joined_to_player_derived_stats"] | latest_any
            joined = joined.drop(columns=latest_cols, errors="ignore")
    if "pds_team" in joined.columns:
        aliases = _load_team_id_aliases(engine)
        joined["pds_team_numeric"] = pd.to_numeric(joined["pds_team"], errors="coerce").astype("Int64")
        joined = joined.merge(aliases, on="pds_team_numeric", how="left")
    return joined


def _train_direct_under_model(train_audit: Path, random_state: int) -> tuple[Pipeline, list[str], str]:
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
    return model, feature_cols, "trained_from_audit_fallback"


def _load_direct_under_model(model_path: Path, train_audit: Path, random_state: int) -> tuple[Any, list[str], str]:
    if model_path.exists():
        artifact = joblib.load(model_path)
        if isinstance(artifact, dict):
            model = artifact.get("model")
            feature_cols = list(artifact.get("feature_columns") or [])
        else:
            model = artifact
            feature_cols = []
        if model is not None and feature_cols:
            return model, feature_cols, str(model_path)
    return _train_direct_under_model(train_audit, random_state)


def _score_over(rows: pd.DataFrame, model_path: Path, features_path: Path) -> pd.DataFrame:
    artifact = joblib.load(model_path)
    model = artifact["model"] if isinstance(artifact, dict) and "model" in artifact else artifact
    feature_payload = json.loads(features_path.read_text(encoding="utf-8"))
    feature_cols = list(feature_payload.get("feature_columns") or artifact.get("feature_columns") or [])
    work = rows[rows["joined_to_player_derived_stats"]].copy()
    if work.empty:
        return work
    for col in feature_cols:
        if col not in work.columns:
            work[col] = np.nan
    work["score"] = model.predict(work[feature_cols].apply(pd.to_numeric, errors="coerce"))
    work["side"] = "over"
    work["rank_score"] = work["score"]
    return _rank_side(work, "rank_score")


def _score_under(rows: pd.DataFrame, model_path: Path, train_audit: Path, random_state: int) -> tuple[pd.DataFrame, str]:
    model, feature_cols, model_source = _load_direct_under_model(model_path, train_audit, random_state)
    work = rows[rows["joined_to_player_derived_stats"] & rows["line"].eq(0.5)].copy()
    if work.empty:
        return work, model_source
    for col in feature_cols:
        if col not in work.columns:
            work[col] = np.nan
    work["score"] = model.predict_proba(work[feature_cols].apply(pd.to_numeric, errors="coerce"))[:, 1]
    work["side"] = "under"
    work["rank_score"] = work["score"]
    return _rank_side(work, "rank_score"), model_source


def _rank_side(rows: pd.DataFrame, score_col: str) -> pd.DataFrame:
    if rows.empty:
        return rows
    out = rows.sort_values(["date_norm", "prop_type_norm", "side", score_col], ascending=[True, True, True, False]).copy()
    group_cols = ["date_norm", "prop_type_norm", "side"]
    out["rank_position"] = out.groupby(group_cols)[score_col].rank(method="first", ascending=False).astype(int)
    out["rank_percentile"] = out.groupby(group_cols)[score_col].rank(method="average", pct=True, ascending=True)
    out["rank_bucket"] = np.ceil(pd.to_numeric(out["rank_percentile"], errors="coerce") * 10.0).clip(1, 10).astype("Int64")
    return out


def _selected_from_ranked(over_rows: pd.DataFrame, under_rows: pd.DataFrame) -> pd.DataFrame:
    over = (
        over_rows[pd.to_numeric(over_rows.get("rank_bucket"), errors="coerce").eq(9)].copy()
        if "rank_bucket" in over_rows.columns
        else pd.DataFrame()
    )
    over["source_lane"] = "residual_ranker_over_bucket_9"
    under = (
        under_rows[pd.to_numeric(under_rows.get("rank_bucket"), errors="coerce").eq(10)].copy()
        if "rank_bucket" in under_rows.columns
        else pd.DataFrame()
    )
    under["source_lane"] = "direct_hitless_under_05_top_decile"
    return pd.concat([over, under], ignore_index=True) if not over.empty or not under.empty else pd.DataFrame()


def _load_quick_card(date_value: str, quick_card_root: Path, today_rows: pd.DataFrame) -> pd.DataFrame:
    path = quick_card_root / date_value / "quick_card.csv"
    if not path.exists() or today_rows.empty:
        return pd.DataFrame()
    qc = pd.read_csv(path, low_memory=False)
    required = {"date", "player_name", "prop_type", "side", "line"}
    if not required.issubset(qc.columns):
        return pd.DataFrame()
    qc = qc[qc["prop_type"].astype(str).str.strip().str.lower().eq("hits")].copy()
    if qc.empty:
        return pd.DataFrame()
    qc["date_norm"] = qc["date"].map(_date_key)
    qc["player_name_norm"] = qc["player_name"].map(_norm_name)
    qc["prop_type_norm"] = qc["prop_type"].astype(str).str.strip().str.lower()
    qc["side"] = qc["side"].astype(str).str.strip().str.lower()
    qc["line_norm"] = qc["line"].map(_line_key)
    base = today_rows.drop_duplicates(["date_norm", "player_name_norm", "prop_type_norm", "line_norm"], keep="first")
    merged = qc.merge(
        base,
        on=["date_norm", "player_name_norm", "prop_type_norm", "line_norm"],
        how="left",
        suffixes=("_quick", ""),
    )
    merged = merged[merged["player_id"].notna()].copy()
    if merged.empty:
        return pd.DataFrame()
    merged["source_lane"] = "quick_card_hits"
    merged["score"] = pd.to_numeric(merged.get("model_prob"), errors="coerce")
    merged["rank_score"] = merged["score"]
    merged["rank_position"] = pd.NA
    merged["rank_percentile"] = pd.NA
    merged["rank_bucket"] = pd.NA
    return merged


def _ensure_quick_card_source(date_value: str, quick_card_root: Path, input_csv: Path) -> dict[str, Any]:
    quick_card_csv = quick_card_root / date_value / "quick_card.csv"
    exists_before = quick_card_csv.exists()
    result: dict[str, Any] = {
        "quick_card_source_csv": str(quick_card_csv),
        "quick_card_source_exists_before": bool(exists_before),
        "quick_card_builder_ran": False,
        "quick_card_source_exists_after": bool(exists_before),
        "quick_card_builder_returncode": None,
        "quick_card_builder_stdout": "",
        "quick_card_builder_stderr": "",
        "quick_card_warning": "",
    }
    if exists_before:
        return result

    odds_root = DEFAULT_ODDS_HISTORY_ROOT / date_value
    book_upload_csv = odds_root / "mlb_book_upload.csv"
    odds_snapshot_json = odds_root / "odds_latest_compatible.json"
    cmd = [
        sys.executable,
        "backend/mlb/scripts/export_mlb_daily_quick_card.py",
        "--date",
        date_value,
        "--slate-csv",
        str(input_csv),
        "--out-csv",
        str(quick_card_csv),
    ]
    if book_upload_csv.exists():
        cmd.extend(["--book-upload-csv", str(book_upload_csv)])
    if odds_snapshot_json.exists():
        cmd.extend(["--odds-snapshot-json", str(odds_snapshot_json)])
    result["quick_card_builder_ran"] = True
    result["quick_card_builder_command"] = cmd
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    result["quick_card_builder_returncode"] = int(proc.returncode)
    result["quick_card_builder_stdout"] = proc.stdout.strip()
    result["quick_card_builder_stderr"] = proc.stderr.strip()
    result["quick_card_source_exists_after"] = quick_card_csv.exists()
    if proc.returncode != 0 or not quick_card_csv.exists():
        warning = "Quick Card unavailable: upstream quick_card.csv missing and build failed"
        print(warning)
        if proc.stdout.strip():
            print(proc.stdout.strip())
        if proc.stderr.strip():
            print(proc.stderr.strip(), file=sys.stderr)
        result["quick_card_warning"] = warning
    return result


def _format_selection(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "player_id",
                "player",
                "player_name",
                "prop_type",
                "side",
                "line",
                "source_lane",
                "rank_bucket",
                "score",
                "rank_score",
                "rank_position",
                "rank_percentile",
                "odds_over",
                "odds_under",
                "player_team",
                "home_raw",
                "away_raw",
                "home_upload",
                "away_upload",
                "team_match_ok",
                "home_team_code",
                "away_team_code",
                "selected_flag",
            ]
        )
    player_team_source = pd.Series("", index=rows.index)
    for col in ("player_team", "team", "team_code", "pds_team"):
        if col in rows.columns:
            player_team_source = rows[col]
            break
    home_raw = rows.get("home_team_code", pd.Series("", index=rows.index)).map(_team_code)
    away_raw = rows.get("away_team_code", pd.Series("", index=rows.index)).map(_team_code)
    home_upload = home_raw.map(_upload_team_code)
    away_upload = away_raw.map(_upload_team_code)
    aliases = rows.get("player_team_aliases", pd.Series("", index=rows.index))
    player_team = pd.Series(
        [
            _resolve_player_team_code(team, alias, home, away, home_up, away_up)
            for team, alias, home, away, home_up, away_up in zip(
                player_team_source,
                aliases,
                home_raw,
                away_raw,
                home_upload,
                away_upload,
            )
        ],
        index=rows.index,
    )
    team_match_ok = (
        player_team.ne("")
        & (
            player_team.eq(home_raw)
            | player_team.eq(away_raw)
            | player_team.eq(home_upload)
            | player_team.eq(away_upload)
        )
    )
    out = pd.DataFrame(
        {
            "date": rows["date_norm"],
            "player_id": rows.get("player_id"),
            "player": rows["player_name"],
            "player_name": rows["player_name"],
            "prop_type": "hits",
            "side": rows["side"].astype(str).str.lower(),
            "line": pd.to_numeric(rows["line"], errors="coerce"),
            "source_lane": rows["source_lane"],
            "rank_bucket": rows.get("rank_bucket"),
            "score": pd.to_numeric(rows.get("score"), errors="coerce"),
            "rank_score": pd.to_numeric(rows.get("rank_score"), errors="coerce"),
            "rank_position": pd.to_numeric(rows.get("rank_position"), errors="coerce"),
            "rank_percentile": pd.to_numeric(rows.get("rank_percentile"), errors="coerce"),
            "odds_over": pd.to_numeric(rows.get("price_over_american"), errors="coerce"),
            "odds_under": pd.to_numeric(rows.get("price_under_american"), errors="coerce"),
            "player_team": player_team,
            "home_raw": home_raw,
            "away_raw": away_raw,
            "home_upload": home_upload,
            "away_upload": away_upload,
            "team_match_ok": team_match_ok,
            "home_team_code": home_upload,
            "away_team_code": away_upload,
            "selected_flag": True,
        }
    )
    return out.sort_values(["source_lane", "rank_bucket", "rank_position", "player_name"], ascending=[True, False, True, True])


def _upload_identity_summary(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {
            "raw_teams": [],
            "upload_teams": [],
            "team_match_ok_true": 0,
            "team_match_ok_false": 0,
            "false_rows": [],
            "team_normalizer": "backend.mlb.scripts.export_mlb_book_upload._normalize_upload_team_code",
        }
    raw_teams = sorted(
        {
            _team_code(v)
            for col in ("home_raw", "away_raw")
            if col in rows.columns
            for v in rows[col].dropna().tolist()
            if _team_code(v)
        }
    )
    upload_teams = sorted(
        {
            _team_code(v)
            for col in ("home_upload", "away_upload")
            if col in rows.columns
            for v in rows[col].dropna().tolist()
            if _team_code(v)
        }
    )
    match = rows.get("team_match_ok", pd.Series(False, index=rows.index)).astype(bool)
    false_rows = rows[~match].copy()
    sample_cols = ["player_name", "player_team", "home_raw", "away_raw", "home_upload", "away_upload"]
    false_sample = false_rows[[c for c in sample_cols if c in false_rows.columns]].head(50).to_dict(orient="records")
    return {
        "raw_teams": raw_teams,
        "upload_teams": upload_teams,
        "team_match_ok_true": int(match.sum()),
        "team_match_ok_false": int((~match).sum()),
        "false_rows": false_sample,
        "team_normalizer": "backend.mlb.scripts.export_mlb_book_upload._normalize_upload_team_code",
        "team_alias_map": book_upload.UPLOAD_TEAM_CODE_ALIASES,
    }


def _counts_summary(selected: pd.DataFrame, empty_lanes: dict[str, bool], model_sources: dict[str, str]) -> dict[str, Any]:
    by_lane = {}
    for lane, group in selected.groupby("source_lane", dropna=False):
        odds = np.where(
            group["side"].eq("over"),
            pd.to_numeric(group["odds_over"], errors="coerce"),
            pd.to_numeric(group["odds_under"], errors="coerce"),
        )
        by_lane[str(lane)] = {
            "count": int(len(group)),
            "avg_odds": float(pd.Series(odds).mean(skipna=True)) if len(group) else None,
        }
    return {
        "total_selected": int(len(selected)),
        "counts_by_lane": by_lane,
        "empty_lane_flags": empty_lanes,
        "model_sources": model_sources,
    }


def _run_upload_export(
    selected_csv: Path,
    date_value: str,
    upload_csv: Path,
    diagnostics_csv: Path,
    args: argparse.Namespace,
    run_tag: str,
) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "backend/mlb/scripts/export_mlb_ranking_tool_upload.py",
        "--rank-csv",
        str(selected_csv),
        "--date",
        date_value,
        "--out-csv",
        str(upload_csv),
        "--diagnostics-csv",
        str(diagnostics_csv),
        "--win-format",
        args.win_format,
    ]
    if args.allow_low_sample_upload:
        cmd.append("--allow-low-sample")
    if args.allow_8rain_public_catalog_fetch:
        cmd.append("--allow-8rain-public-catalog-fetch")
    if args.upload_history_from_date:
        cmd.extend(["--from-date", args.upload_history_from_date])
    if args.upload_history_to_date:
        cmd.extend(["--to-date", args.upload_history_to_date])
    env = os.environ.copy()
    env["MLB_UPLOAD_RUN_TAG"] = run_tag
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True, env=env)
    return {
        "command": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _validate_upload_home_away(upload_csv: Path) -> dict[str, Any]:
    if not upload_csv.exists():
        return {"ok": False, "reason": f"missing upload csv: {upload_csv}"}
    df = pd.read_csv(upload_csv, low_memory=False)
    if df.empty:
        return {"ok": True, "rows": 0, "missing_home": 0, "missing_away": 0, "sample_affected_rows": []}
    missing_cols = [c for c in ("HOME", "AWAY") if c not in df.columns]
    if missing_cols:
        return {"ok": False, "reason": f"missing columns: {missing_cols}"}
    home_missing = df["HOME"].isna() | df["HOME"].astype(str).str.strip().eq("")
    away_missing = df["AWAY"].isna() | df["AWAY"].astype(str).str.strip().eq("")
    affected = df[home_missing | away_missing].copy()
    sample_cols = [c for c in ["SELECTOR", "HOME", "AWAY", "POINT", "SIDE", "WIN %"] if c in affected.columns]
    return {
        "ok": bool(affected.empty),
        "rows": int(len(df)),
        "missing_home": int(home_missing.sum()),
        "missing_away": int(away_missing.sum()),
        "sample_affected_rows": affected[sample_cols].head(10).to_dict(orient="records"),
    }


def _bucket_label_from_percentile(value: Any, bucket_size: float = 0.10) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return ""
    if val > 1.0:
        val = val / 100.0
    val = min(max(float(val), 0.0), 1.0)
    low = np.floor(val / bucket_size) * bucket_size
    high = min(low + bucket_size, 1.0)
    if val >= 1.0:
        low = max(0.0, 1.0 - bucket_size)
        high = 1.0
    return f"{low:.2f}-{high:.2f}"


def _line_bucket_label(value: Any) -> str:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return ""
    return f"{float(val):.3f}".rstrip("0").rstrip(".")


def _count_breakdown(df: pd.DataFrame, column: str) -> dict[str, int]:
    if df.empty or column not in df.columns:
        return {}
    return {str(k): int(v) for k, v in df.groupby(column, dropna=False).size().sort_index().to_dict().items()}


def _write_upload_mapping_diagnostics(
    *,
    selected: pd.DataFrame,
    ranking_upload_input: pd.DataFrame,
    quick_card_rows: pd.DataFrame,
    upload_csv: Path,
    upload_diagnostics_csv: Path,
    out_json: Path,
    allow_low_sample: bool,
) -> dict[str, Any]:
    selected_rows = int(len(selected))
    ranking_input_rows = int(len(ranking_upload_input))
    quick_card_rows_count = int(len(quick_card_rows))
    upload_rows = 0
    if upload_csv.exists():
        try:
            upload_rows = int(len(pd.read_csv(upload_csv, low_memory=False)))
        except Exception:
            upload_rows = 0

    required_cols = ["date", "player_name", "prop_type", "side", "line", "rank_score", "rank_position", "rank_percentile"]
    work = ranking_upload_input.copy()

    diag = pd.DataFrame()
    if upload_diagnostics_csv.exists():
        try:
            diag = pd.read_csv(upload_diagnostics_csv, low_memory=False)
        except Exception:
            diag = pd.DataFrame()

    if not diag.empty and len(diag) == len(work):
        # The exporter preserves input row order in diagnostics. Use that row
        # alignment to avoid false matches when many players share prop/side/line
        # and rank bucket.
        diag = diag.reset_index(drop=True)
        work = work.reset_index(drop=True)
        for col in ("empirical_win_pct", "sample_size", "uploaded_win_value"):
            work[col] = diag[col] if col in diag.columns else np.nan
    elif not diag.empty:
        diag["line_bucket"] = diag["line"].map(_line_bucket_label)
        diag["rank_bucket_calc"] = diag["rank_percentile"].map(_bucket_label_from_percentile)
        work["line_bucket"] = work["line"].map(_line_bucket_label)
        work["rank_bucket_calc"] = work["rank_percentile"].map(_bucket_label_from_percentile)
        merge_keys = ["player_name", "prop_type", "side", "line_bucket", "rank_bucket_calc"]
        diag_small = diag[
            merge_keys + [c for c in ("empirical_win_pct", "sample_size", "uploaded_win_value") if c in diag.columns]
        ].copy()
        diag_small = diag_small.drop_duplicates(merge_keys, keep="first")
        work = work.merge(diag_small, on=merge_keys, how="left")
    else:
        work["empirical_win_pct"] = np.nan
        work["sample_size"] = np.nan

    missing_required_mask = pd.Series(False, index=work.index)
    for col in required_cols:
        if col not in work.columns:
            missing_required_mask = pd.Series(True, index=work.index)
            break
        vals = work[col]
        missing_required_mask |= vals.isna() | vals.astype(str).str.strip().str.lower().isin({"", "nan", "none", "null", "<na>"})

    sample_size = pd.to_numeric(work.get("sample_size"), errors="coerce")
    mapped_mask = work.get("empirical_win_pct", pd.Series(np.nan, index=work.index)).notna()
    low_sample_mask = mapped_mask & sample_size.lt(50)
    uploaded_mask = mapped_mask & (sample_size.ge(50) | bool(allow_low_sample)) & ~missing_required_mask
    excluded_unmapped_mask = ~mapped_mask & ~missing_required_mask
    excluded_low_sample_mask = low_sample_mask & ~bool(allow_low_sample) & ~missing_required_mask
    excluded_missing_required_mask = missing_required_mask
    known_excluded = excluded_unmapped_mask | excluded_low_sample_mask | excluded_missing_required_mask
    excluded_other_mask = ~uploaded_mask & ~known_excluded

    matched_sample = sample_size[mapped_mask].dropna()
    would_pass_allow_low_sample = int((mapped_mask & ~missing_required_mask).sum())
    diagnostics = {
        "selected_rows": selected_rows,
        "ranking_upload_input_rows": ranking_input_rows,
        "quick_card_rows_excluded_from_ranking_upload": quick_card_rows_count,
        "upload_rows": upload_rows,
        "excluded_rows": int(ranking_input_rows - upload_rows),
        "excluded_unmapped_bucket": int(excluded_unmapped_mask.sum()),
        "excluded_low_sample": int(excluded_low_sample_mask.sum()),
        "excluded_missing_required_fields": int(excluded_missing_required_mask.sum()),
        "excluded_other": int(excluded_other_mask.sum()),
        "breakdown_by_source_lane": _count_breakdown(work, "source_lane"),
        "breakdown_by_side": _count_breakdown(work, "side"),
        "breakdown_by_line": _count_breakdown(work.assign(line=work["line"].map(_line_bucket_label)), "line"),
        "exclusions_by_source_lane": {
            str(lane): {
                "selected": int(len(group)),
                "unmapped_bucket": int(excluded_unmapped_mask.loc[group.index].sum()),
                "low_sample": int(excluded_low_sample_mask.loc[group.index].sum()),
                "missing_required_fields": int(excluded_missing_required_mask.loc[group.index].sum()),
                "other": int(excluded_other_mask.loc[group.index].sum()),
            }
            for lane, group in work.groupby("source_lane", dropna=False)
        },
        "exclusions_by_side": {
            str(side): {
                "selected": int(len(group)),
                "unmapped_bucket": int(excluded_unmapped_mask.loc[group.index].sum()),
                "low_sample": int(excluded_low_sample_mask.loc[group.index].sum()),
                "missing_required_fields": int(excluded_missing_required_mask.loc[group.index].sum()),
                "other": int(excluded_other_mask.loc[group.index].sum()),
            }
            for side, group in work.groupby("side", dropna=False)
        },
        "exclusions_by_line": {
            str(line): {
                "selected": int(len(group)),
                "unmapped_bucket": int(excluded_unmapped_mask.loc[group.index].sum()),
                "low_sample": int(excluded_low_sample_mask.loc[group.index].sum()),
                "missing_required_fields": int(excluded_missing_required_mask.loc[group.index].sum()),
                "other": int(excluded_other_mask.loc[group.index].sum()),
            }
            for line, group in work.assign(line_label=work["line"].map(_line_bucket_label)).groupby("line_label", dropna=False)
        },
        "matched_sample_size": {
            "min": float(matched_sample.min()) if not matched_sample.empty else None,
            "median": float(matched_sample.median()) if not matched_sample.empty else None,
            "max": float(matched_sample.max()) if not matched_sample.empty else None,
        },
        "would_pass_with_allow_low_sample_upload": would_pass_allow_low_sample,
        "allow_low_sample_upload": bool(allow_low_sample),
        "quick_card_lane": {
            "included_in_selector_summary": True,
            "sent_to_ranking_upload": False,
            "rows": quick_card_rows_count,
        },
        "upload_csv": str(upload_csv),
        "upload_diagnostics_csv": str(upload_diagnostics_csv),
        "out_json": str(out_json),
    }
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")
    return diagnostics


def run(args: argparse.Namespace) -> dict[str, Any]:
    date_value = _date_key(args.date)
    if not date_value:
        raise SystemExit("--date YYYY-MM-DD is required")
    run_tag = tool_upload_8rain.upload_run_tag()
    input_csv, mode = _resolve_input_and_mode(args, date_value)
    out_csv = Path(args.out_csv) if args.out_csv else _today_paths(date_value)[0]
    summary_json = Path(args.summary_json) if args.summary_json else _today_paths(date_value)[1]
    upload_csv = Path(args.upload_csv) if args.upload_csv else _today_paths(date_value)[2]
    quick_card_hits_csv = Path(args.quick_card_hits_csv) if args.quick_card_hits_csv else _quick_card_hits_path(date_value)
    diagnostics_csv = Path(args.upload_diagnostics_csv) if args.upload_diagnostics_csv else upload_csv.with_name(
        f"ranking_tool_upload_diagnostics_{date_value}.csv"
    )
    upload_mapping_diagnostics_json = (
        Path(args.upload_mapping_diagnostics_json)
        if args.upload_mapping_diagnostics_json
        else out_csv.with_name(f"hits_lane_selector_{date_value}_upload_diagnostics.json")
    )

    today = _load_today_rows(input_csv, date_value)
    today_pds = _join_pds(today, args.chunk_size)
    over_ranked = _score_over(today_pds, Path(args.residual_model), Path(args.residual_features))
    under_ranked, under_model_source = _score_under(
        today_pds,
        Path(args.direct_under_model),
        Path(args.training_audit_csv),
        args.random_state,
    )
    ranking_selected_ranked = _selected_from_ranked(over_ranked, under_ranked)
    selected_ranked = ranking_selected_ranked.copy()
    quick_card_status = _ensure_quick_card_source(date_value, Path(args.quick_card_root), input_csv)
    quick = _load_quick_card(date_value, Path(args.quick_card_root), today)
    if not quick.empty:
        selected_ranked = quick.copy() if selected_ranked.empty else pd.concat([selected_ranked, quick], ignore_index=True)
    selected = _format_selection(selected_ranked)
    ranking_upload_input = _format_selection(ranking_selected_ranked)
    quick_card_rows = selected[selected["source_lane"].eq("quick_card_hits")].copy() if not selected.empty else pd.DataFrame()

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    quick_card_hits_csv.parent.mkdir(parents=True, exist_ok=True)
    selected.to_csv(out_csv, index=False)
    ranking_upload_input_csv = out_csv.with_name(f"hits_lane_selector_{date_value}_ranking_upload_input.csv")
    upload_identity = _upload_identity_summary(ranking_upload_input)
    print("Ranking upload raw HOME/AWAY teams: " + json.dumps(upload_identity["raw_teams"]))
    print("Ranking upload normalized HOME/AWAY teams: " + json.dumps(upload_identity["upload_teams"]))
    print(
        "Ranking upload team_match_ok "
        f"true={upload_identity['team_match_ok_true']} false={upload_identity['team_match_ok_false']}"
    )
    if upload_identity["team_match_ok_false"]:
        print("Ranking upload team_match_ok=false rows:")
        print(json.dumps(upload_identity["false_rows"], indent=2))
    ranking_upload_export_input = ranking_upload_input
    if args.drop_team_mismatch_upload and "team_match_ok" in ranking_upload_input.columns:
        ranking_upload_export_input = ranking_upload_input[ranking_upload_input["team_match_ok"].astype(bool)].copy()
    upload_identity["drop_team_mismatch_upload"] = bool(args.drop_team_mismatch_upload)
    upload_identity["ranking_upload_input_rows_before_team_drop"] = int(len(ranking_upload_input))
    upload_identity["ranking_upload_input_rows_after_team_drop"] = int(len(ranking_upload_export_input))
    ranking_upload_export_input.to_csv(ranking_upload_input_csv, index=False)
    quick_card_rows.to_csv(quick_card_hits_csv, index=False)

    expected_lanes = {
        "direct_hitless_under_05_top_decile": "direct_hitless_under_05_top_decile",
        "residual_ranker_over_bucket_9": "residual_ranker_over_bucket_9",
        "quick_card_hits": "quick_card_hits",
    }
    empty_lanes = {name: not selected["source_lane"].eq(lane).any() for name, lane in expected_lanes.items()}
    summary = _counts_summary(
        selected,
        empty_lanes,
        {
            "residual_over": str(args.residual_model),
            "direct_under": under_model_source,
        },
    )
    summary.update(
        {
            "date": date_value,
            "run_tag": run_tag,
            "mode": mode,
            "note": "no outcomes available" if mode == "pregame" else "",
            "input_csv": str(input_csv),
            "out_csv": str(out_csv),
            "summary_json": str(summary_json),
            "upload_csv": str(upload_csv),
            "upload_diagnostics_csv": str(diagnostics_csv),
            "upload_mapping_diagnostics_json": str(upload_mapping_diagnostics_json),
            "ranking_upload_input_csv": str(ranking_upload_input_csv),
            "quick_card_hits_csv": str(quick_card_hits_csv),
            **quick_card_status,
            "quick_card_hits_rows": int(len(quick_card_rows)),
            "upload_identity_validation": upload_identity,
            "pds_join_rows": int(today_pds["joined_to_player_derived_stats"].sum()) if "joined_to_player_derived_stats" in today_pds else 0,
            "candidate_hits_rows": int(len(today)),
            "rules": {
                "under_0_5": "direct hitless model rank_bucket == 10",
                "over": "residual ranker rank_bucket == 9",
                "quick_card": "all matched hits quick card rows",
            },
        }
    )

    if args.export_upload:
        summary["upload_export"] = _run_upload_export(
            ranking_upload_input_csv,
            date_value,
            upload_csv,
            diagnostics_csv,
            args,
            run_tag,
        )
        summary["upload_home_away_validation"] = _validate_upload_home_away(upload_csv)
        if not summary["upload_home_away_validation"].get("ok"):
            print("Ranking upload has blank HOME/AWAY rows; sample affected rows:")
            print(json.dumps(summary["upload_home_away_validation"].get("sample_affected_rows", []), indent=2))
        summary["upload_mapping_diagnostics"] = _write_upload_mapping_diagnostics(
            selected=selected,
            ranking_upload_input=ranking_upload_export_input,
            quick_card_rows=quick_card_rows,
            upload_csv=upload_csv,
            upload_diagnostics_csv=diagnostics_csv,
            out_json=upload_mapping_diagnostics_json,
            allow_low_sample=bool(args.allow_low_sample_upload),
        )
    else:
        summary["upload_export"] = {"skipped": True}
        summary["upload_mapping_diagnostics"] = {"skipped": True}

    archive_candidates = [
        out_csv,
        ranking_upload_input_csv,
        quick_card_hits_csv,
        summary_json,
        upload_mapping_diagnostics_json,
    ]
    summary["timestamped_lane_artifacts"] = [
        str(_timestamped_artifact_path(path, run_tag)) for path in archive_candidates
    ]
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    archived = _archive_lane_artifacts(archive_candidates, run_tag)
    summary["timestamped_lane_artifacts_written"] = archived
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    _archive_lane_artifacts([summary_json], run_tag)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily MLB hits lane selector.")
    parser.add_argument("--date", required=True, help="Slate date YYYY-MM-DD.")
    parser.add_argument("--input-csv", default="", help="Daily reconcile/slate-like CSV. Defaults to execution_vs_model/<date>/reconcile_rows.csv.")
    parser.add_argument("--training-audit-csv", default=str(DEFAULT_TRAIN_AUDIT))
    parser.add_argument("--residual-model", default=str(DEFAULT_RESIDUAL_MODEL))
    parser.add_argument("--residual-features", default=str(DEFAULT_RESIDUAL_FEATURES))
    parser.add_argument("--direct-under-model", default=str(DEFAULT_DIRECT_UNDER_MODEL))
    parser.add_argument("--quick-card-root", default=str(DEFAULT_QUICK_CARD_ROOT))
    parser.add_argument("--out-csv", default="")
    parser.add_argument("--summary-json", default="")
    parser.add_argument("--upload-csv", default="")
    parser.add_argument("--upload-diagnostics-csv", default="")
    parser.add_argument("--upload-mapping-diagnostics-json", default="")
    parser.add_argument("--quick-card-hits-csv", default="")
    parser.add_argument("--export-upload", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--allow-low-sample-upload", action="store_true")
    parser.add_argument(
        "--allow-8rain-public-catalog-fetch",
        action="store_true",
        default=tool_upload_8rain.public_catalog_fetch_allowed(),
        help="Opt in to documented public 8rain catalog GET requests. Default is cache-only.",
    )
    parser.add_argument("--drop-team-mismatch-upload", action="store_true")
    parser.add_argument("--win-format", choices=["pct", "decimal", "american"], default="decimal")
    parser.add_argument("--upload-history-from-date", default="")
    parser.add_argument("--upload-history-to-date", default="")
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    summary = run(parse_args())
    print(f"Wrote {summary['out_csv']}")
    print(f"Wrote {summary['summary_json']}")
    print(f"Wrote {summary['upload_csv']}")
    print("counts_by_lane=" + json.dumps(summary["counts_by_lane"], sort_keys=True))
    if summary.get("upload_export", {}).get("returncode") not in (None, 0):
        if summary["upload_export"].get("stdout"):
            print(summary["upload_export"]["stdout"])
        if summary["upload_export"].get("stderr"):
            print(summary["upload_export"]["stderr"], file=sys.stderr)
        raise SystemExit(int(summary["upload_export"]["returncode"]))
    if summary.get("upload_home_away_validation", {}).get("ok") is False:
        print(json.dumps(summary["upload_home_away_validation"], indent=2), file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
