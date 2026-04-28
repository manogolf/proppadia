#!/usr/bin/env python3
"""Build and evaluate a singles-only shadow upload variant (read-only to base/weighted).

This script is intentionally isolated:
- keeps base and weighted uploads untouched
- writes a new shadow upload file for manual comparison
- optionally grades the shadow rows when reconcile outcomes are available
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from backend.mlb.scripts import compare_upload_variants_postgame as compare_utils


ET = ZoneInfo("America/New_York")

UPLOAD_COLUMNS = [
    "LEAGUE",
    "DATE",
    "HOME",
    "AWAY",
    "DOUBLEHEADER",
    "SECTION",
    "MARKET",
    "SELECTOR",
    "POINT",
    "SIDE",
    "WIN %",
]

KEY_COLUMNS = [
    "LEAGUE",
    "DATE",
    "HOME",
    "AWAY",
    "MARKET",
    "SELECTOR",
    "POINT",
    "SIDE",
]


@dataclass
class VariantStats:
    total_rows: int
    graded_rows: int
    wins: int
    losses: int
    pushes: int
    win_rate_ex_push: Optional[float]
    false_over: int
    false_under: int
    pred_over_rate: Optional[float]
    actual_over_rate: Optional[float]


def _parse_iso_date(value: str) -> str:
    parsed = compare_utils._parse_date(value)  # noqa: SLF001
    if not parsed:
        raise ValueError(f"could not parse date: {value}")
    return parsed


def _yyyymmdd(date_iso: str) -> str:
    return str(date_iso).replace("-", "")


def _run(cmd: List[str], *, env: Dict[str, str], cwd: Path, log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as logf:
        logf.write(f"$ {' '.join(cmd)}\n\n")
        logf.flush()
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=logf,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )
    if proc.returncode != 0:
        raise RuntimeError(f"command failed (exit={proc.returncode}): {' '.join(cmd)}; log={log_path}")


def _inject_backend_env(env: Dict[str, str], repo_root: Path) -> Dict[str, str]:
    """Load backend/.env key=value pairs into subprocess env when not already set."""
    merged = dict(env)
    env_path = repo_root / "backend/.env"
    if not env_path.exists():
        return merged
    try:
        raw = env_path.read_text(encoding="utf-8")
    except Exception:
        return merged
    token_re = re.compile(r"\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)")

    def _expand_vars(raw_value: str) -> str:
        def repl(match: re.Match[str]) -> str:
            key = match.group(1) or match.group(2) or ""
            return str(merged.get(key, ""))

        return token_re.sub(repl, raw_value)

    for line in raw.splitlines():
        txt = line.strip()
        if not txt or txt.startswith("#") or "=" not in txt:
            continue
        key, val = txt.split("=", 1)
        key = key.strip()
        if not key or key in merged:
            continue
        value = val.strip().strip("'\"")
        value = _expand_vars(value)
        merged[key] = value
    return merged


def _resolve_default_singles_model(repo_root: Path) -> Path:
    last_path = repo_root / "tmp/experiments/hits_family/singles/.last_experiment_path"
    if last_path.exists():
        exp_root = Path(last_path.read_text(encoding="utf-8").strip()).expanduser()
        candidate = exp_root / "models/baseline540/latest/singles.joblib"
        if candidate.exists():
            return candidate.resolve()

    fallback = repo_root / "tmp/experiments/hits_family/singles/singles_compare_20260426_195548/models/baseline540/latest/singles.joblib"
    if fallback.exists():
        return fallback.resolve()

    prod = repo_root / "models_out/latest/singles.joblib"
    if prod.exists():
        return prod.resolve()

    raise FileNotFoundError(
        "could not locate singles model artifact; set --singles-model-path explicitly"
    )


def _build_singles_slate(
    *,
    repo_root: Path,
    venv_python: Path,
    date_iso: str,
    odds_snapshot_in: Path,
    out_dir: Path,
    singles_model_path: Path,
) -> Tuple[Path, Path]:
    pred_csv = out_dir / "predictions_wide_singles_shadow.csv"
    slate_csv = out_dir / "slate_output_singles_shadow.csv"
    odds_snapshot_out = out_dir / "odds_snapshot_singles_shadow.json"
    overlay_latest = out_dir / "models/latest"
    overlay_latest.mkdir(parents=True, exist_ok=True)
    overlay_model = overlay_latest / "singles.joblib"
    shutil.copy2(singles_model_path, overlay_model)

    cmd_env = _inject_backend_env(os.environ.copy(), repo_root)
    cmd_env["MODEL_DIR"] = str(out_dir / "models")
    # Use any-book two-sided coverage for singles.
    cmd_env["MLB_PREDICT_TWO_SIDED_BOOKMAKER"] = ""

    _run(
        [
            str(venv_python),
            "backend/mlb/scripts/build_mlb_predictions_wide.py",
            "--slate-date",
            date_iso,
            "--output",
            str(pred_csv),
            "--odds-snapshot-in",
            str(odds_snapshot_in),
            "--odds-snapshot-out",
            str(odds_snapshot_out),
            "--prop-types",
            "singles",
            "--require-two-sided",
        ],
        env=cmd_env,
        cwd=repo_root,
        log_path=out_dir / "build_singles_predictions.log",
    )

    _run(
        [
            str(venv_python),
            "backend/mlb/scripts/build_mlb_slate_output.py",
            "--slate-date",
            date_iso,
            "--pred-csv",
            str(pred_csv),
            "--out-csv",
            str(slate_csv),
            "--prop-type",
            "singles",
        ],
        env=cmd_env,
        cwd=repo_root,
        log_path=out_dir / "build_singles_slate.log",
    )
    return pred_csv, slate_csv


def _key_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["LEAGUE"] = out["LEAGUE"].map(lambda v: str(v).strip().upper())
    out["DATE"] = out["DATE"].map(lambda v: str(v).strip())
    out["HOME"] = out["HOME"].map(lambda v: str(v).strip().upper())
    out["AWAY"] = out["AWAY"].map(lambda v: str(v).strip().upper())
    out["MARKET"] = out["MARKET"].map(lambda v: str(v).strip().lower())
    out["SELECTOR"] = pd.to_numeric(out["SELECTOR"], errors="coerce").astype("Int64")
    out["POINT"] = pd.to_numeric(out["POINT"], errors="coerce").round(4)
    out["SIDE"] = out["SIDE"].map(lambda v: str(v).strip().lower())
    return out[KEY_COLUMNS].dropna(subset=["SELECTOR", "POINT"]).drop_duplicates()


def _side_from_threshold(*, prob_over: float, threshold: float) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    p_over = float(prob_over)
    p_under = float(1.0 - p_over)
    if p_over >= float(threshold):
        return "over", p_over, p_under
    if p_under >= float(threshold):
        return "under", p_over, p_under
    return None, None, None


def _select_singles_rows(
    *,
    base_upload: pd.DataFrame,
    singles_slate: pd.DataFrame,
    date_iso: str,
    threshold: float,
    top_n: int,
    max_rows_per_player: int,
    max_abs_win_pct: float,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    if singles_slate.empty:
        raise RuntimeError("singles slate output is empty; cannot build singles shadow rows")

    required_cols = {
        "league",
        "slate_date",
        "home_team_code",
        "away_team_code",
        "market_key",
        "player_id",
        "player_name",
        "line",
        "prob_over",
        "fair_odds_over_american",
        "fair_odds_under_american",
    }
    missing = sorted(c for c in required_cols if c not in singles_slate.columns)
    if missing:
        raise RuntimeError(f"singles slate missing required columns: {missing}")

    base_keys = _key_frame(base_upload)

    records: List[Dict[str, Any]] = []
    for row in singles_slate.to_dict(orient="records"):
        side, p_over, p_under = _side_from_threshold(prob_over=float(row["prob_over"]), threshold=threshold)
        if not side:
            continue
        win_val = (
            row.get("fair_odds_over_american")
            if side == "over"
            else row.get("fair_odds_under_american")
        )
        try:
            win_int = int(round(float(win_val)))
        except Exception:
            continue
        if abs(win_int) >= float(max_abs_win_pct):
            continue

        rec = {
            "LEAGUE": str(row.get("league") or "MLB").strip().upper(),
            "DATE": _yyyymmdd(date_iso),
            "HOME": str(row.get("home_team_code") or "").strip().upper(),
            "AWAY": str(row.get("away_team_code") or "").strip().upper(),
            "DOUBLEHEADER": np.nan,
            "SECTION": "player_prop",
            "MARKET": str(row.get("market_key") or "batter_singles").strip().lower(),
            "SELECTOR": int(float(row.get("player_id"))),
            "POINT": float(row.get("line")),
            "SIDE": str(side),
            "WIN %": int(win_int),
            "side_probability": float(p_over if side == "over" else p_under),
            "prob_over": float(p_over),
            "prob_under": float(p_under),
            "player_name": str(row.get("player_name") or "").strip(),
            "prop_type": "singles",
        }
        records.append(rec)

    counts: Dict[str, int] = {
        "slate_rows": int(len(singles_slate)),
        "threshold_qualified_rows": int(len(records)),
    }
    if not records:
        return pd.DataFrame(columns=UPLOAD_COLUMNS), pd.DataFrame(), counts

    cand = pd.DataFrame(records).drop_duplicates(
        subset=["LEAGUE", "DATE", "HOME", "AWAY", "MARKET", "SELECTOR", "POINT", "SIDE"],
        keep="first",
    )
    counts["deduped_rows"] = int(len(cand))

    cand["_abs_win"] = pd.to_numeric(cand["WIN %"], errors="coerce").abs()
    cand = cand.sort_values(
        by=["side_probability", "_abs_win", "SELECTOR", "POINT"],
        ascending=[False, False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    cand["_player_rank"] = cand.groupby(["SELECTOR"], dropna=False).cumcount() + 1
    before_player_cap = int(len(cand))
    cand = cand[cand["_player_rank"] <= int(max_rows_per_player)].copy()
    counts["dropped_player_cap"] = int(before_player_cap - len(cand))

    cand_keys = _key_frame(cand)
    merged = cand_keys.merge(base_keys, on=KEY_COLUMNS, how="left", indicator=True)
    keep_keys = merged[merged["_merge"] == "left_only"][KEY_COLUMNS].copy()
    counts["rows_already_in_base"] = int((merged["_merge"] != "left_only").sum())

    if keep_keys.empty:
        return pd.DataFrame(columns=UPLOAD_COLUMNS), pd.DataFrame(), counts

    cand = cand.merge(keep_keys, on=KEY_COLUMNS, how="inner")
    counts["new_rows_vs_base"] = int(len(cand))

    selected = cand.head(int(top_n)).copy()
    counts["selected_rows"] = int(len(selected))
    counts["top_n"] = int(top_n)

    upload_rows = selected[UPLOAD_COLUMNS].copy()
    detail_rows = selected.drop(columns=["_player_rank", "_abs_win"], errors="ignore").copy()
    return upload_rows, detail_rows, counts


def _merge_base_and_shadow(base_df: pd.DataFrame, added_df: pd.DataFrame) -> pd.DataFrame:
    if added_df.empty:
        return base_df.copy()
    cols = [c for c in UPLOAD_COLUMNS if c in base_df.columns]
    missing = [c for c in cols if c not in added_df.columns]
    for c in missing:
        added_df[c] = np.nan
    merged = pd.concat([base_df[cols], added_df[cols]], ignore_index=True)
    return merged


def _build_diff(base_df: pd.DataFrame, shadow_df: pd.DataFrame) -> pd.DataFrame:
    base_norm = compare_utils._normalize_upload_df(base_df, variant="base")  # noqa: SLF001
    shadow_norm = compare_utils._normalize_upload_df(shadow_df, variant="singles_shadow")  # noqa: SLF001

    left = base_norm[
        compare_utils.SHARED_KEY_COLS + ["WIN %", "prop_type", "implied_win_prob"]  # noqa: SLF001
    ].rename(
        columns={
            "WIN %": "base_win",
            "prop_type": "base_prop_type",
            "implied_win_prob": "base_implied_prob",
        }
    )
    right = shadow_norm[
        compare_utils.SHARED_KEY_COLS + ["WIN %", "prop_type", "implied_win_prob"]  # noqa: SLF001
    ].rename(
        columns={
            "WIN %": "shadow_win",
            "prop_type": "shadow_prop_type",
            "implied_win_prob": "shadow_implied_prob",
        }
    )
    diff = left.merge(right, on=compare_utils.SHARED_KEY_COLS, how="outer", indicator=True)  # noqa: SLF001
    diff["status"] = diff["_merge"].map(
        {
            "left_only": "only_base",
            "right_only": "only_singles_shadow",
            "both": "shared",
        }
    )
    return diff.drop(columns=["_merge"])


def _safe_rate(num: int, den: int) -> Optional[float]:
    if den <= 0:
        return None
    return float(num / den)


def _stats_from_eval(df: pd.DataFrame) -> VariantStats:
    total = int(len(df))
    graded = int(df.get("is_graded", pd.Series(dtype=bool)).sum())
    wins = int(df.get("is_win", pd.Series(dtype=bool)).sum())
    losses = int(df.get("is_loss", pd.Series(dtype=bool)).sum())
    pushes = int(df.get("is_push", pd.Series(dtype=bool)).sum())
    wl = wins + losses
    win_rate = float(wins / wl) if wl > 0 else None

    graded_df = df[df.get("is_graded", pd.Series(dtype=bool)).astype(bool)].copy()
    false_over = int(
        ((graded_df.get("key_side") == "over") & (graded_df.get("outcome") == "loss")).sum()
    )
    false_under = int(
        ((graded_df.get("key_side") == "under") & (graded_df.get("outcome") == "loss")).sum()
    )

    pred_over = int((df.get("key_side") == "over").sum())
    pred_over_rate = _safe_rate(pred_over, total)

    actual_over_rate: Optional[float] = None
    if not graded_df.empty:
        has_actual = graded_df["actual_value"].notna() & graded_df["key_point"].notna()
        if bool(has_actual.any()):
            actual_over = int((graded_df.loc[has_actual, "actual_value"] > graded_df.loc[has_actual, "key_point"]).sum())
            actual_over_rate = _safe_rate(actual_over, int(has_actual.sum()))

    return VariantStats(
        total_rows=total,
        graded_rows=graded,
        wins=wins,
        losses=losses,
        pushes=pushes,
        win_rate_ex_push=win_rate,
        false_over=false_over,
        false_under=false_under,
        pred_over_rate=pred_over_rate,
        actual_over_rate=actual_over_rate,
    )


def _fmt_pct(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "n/a"
    return f"{100.0 * float(v):.2f}%"


def _write_summary_md(
    *,
    out_path: Path,
    date_iso: str,
    shadow_csv_path: Path,
    added_rows: pd.DataFrame,
    exposure_counts: Dict[str, int],
    base_stats: VariantStats,
    shadow_stats: VariantStats,
    added_stats: VariantStats,
    grading_meta: Dict[str, Any],
) -> None:
    total_shadow = int(shadow_stats.total_rows)
    added_count = int(len(added_rows))
    added_pct = _safe_rate(added_count, total_shadow)
    side_counts = (
        added_rows["SIDE"].astype(str).str.lower().value_counts().to_dict()
        if not added_rows.empty
        else {}
    )
    line_counts = (
        added_rows["POINT"].astype(float).value_counts().sort_index().to_dict()
        if not added_rows.empty
        else {}
    )

    if added_stats.graded_rows == 0:
        over_bias_note = "Pending grading (no post-game outcomes yet)."
        stability_note = "Noisy/unknown until outcomes are available."
        readiness = "B) further tuning (pending real outcomes)"
        better_vs_baseline = "Inconclusive (no graded singles shadow outcomes yet)."
    else:
        po = added_stats.pred_over_rate or 0.0
        ao = added_stats.actual_over_rate or 0.0
        over_bias_note = (
            f"Pred over={_fmt_pct(po)} vs actual over={_fmt_pct(ao)}; "
            f"false_over={added_stats.false_over}, false_under={added_stats.false_under}."
        )
        if added_stats.graded_rows < 20:
            stability_note = "Noisy (sample too small for stable decision)."
            readiness = "B) further tuning"
        else:
            stability_note = "Reasonably stable for a one-day shadow read."
            if (added_stats.win_rate_ex_push or 0.0) >= (base_stats.win_rate_ex_push or 0.0):
                readiness = "A) inclusion candidate for enhanced upload (shadow only next)"
            else:
                readiness = "B) further tuning"
        better_vs_baseline = (
            f"Added singles win rate ex push={_fmt_pct(added_stats.win_rate_ex_push)} "
            f"vs base overall={_fmt_pct(base_stats.win_rate_ex_push)}."
        )

    lines: List[str] = []
    lines.append(f"# Singles Shadow Summary — {date_iso}")
    lines.append("")
    lines.append("## Shadow Build")
    lines.append(f"- Shadow CSV: `{shadow_csv_path}`")
    lines.append(f"- Added singles rows: {added_count}")
    lines.append(f"- Added singles share of shadow surface: {_fmt_pct(added_pct)}")
    lines.append(f"- Side distribution (added singles): {json.dumps(side_counts)}")
    lines.append(f"- Line distribution (added singles): {json.dumps(line_counts)}")
    lines.append("")
    lines.append("## Exposure Controls")
    for k in sorted(exposure_counts.keys()):
        lines.append(f"- {k}: {exposure_counts[k]}")
    lines.append("")
    lines.append("## Postgame (if available)")
    lines.append(f"- Grading source: {grading_meta.get('grading_source')}")
    lines.append(f"- Grading loaded: {grading_meta.get('grading_loaded')}")
    lines.append(f"- Grading date rows: {grading_meta.get('grading_rows_date')}")
    if grading_meta.get("grading_note"):
        lines.append(f"- Note: {grading_meta.get('grading_note')}")
    lines.append(f"- Base win rate (ex push): {_fmt_pct(base_stats.win_rate_ex_push)}")
    lines.append(f"- Singles shadow win rate (ex push): {_fmt_pct(shadow_stats.win_rate_ex_push)}")
    lines.append(
        "- Added singles rows: "
        f"graded={added_stats.graded_rows}/{added_stats.total_rows}, "
        f"wins={added_stats.wins}, losses={added_stats.losses}, pushes={added_stats.pushes}, "
        f"win_rate_ex_push={_fmt_pct(added_stats.win_rate_ex_push)}"
    )
    lines.append("")
    lines.append("## Decision Questions")
    lines.append(f"- Did singles rows perform better than baseline? {better_vs_baseline}")
    lines.append(f"- Did threshold reduce over-bias in real outcomes? {over_bias_note}")
    lines.append(f"- Were results stable or noisy? {stability_note}")
    lines.append(f"- Readiness call: {readiness}")
    lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Generate and evaluate singles-only shadow upload.")
    parser.add_argument("--date", default=datetime.now(ET).date().isoformat(), help="Slate date YYYY-MM-DD.")
    parser.add_argument(
        "--base-csv",
        default="",
        help="Base upload CSV path (default backend/mlb/data/processed/mlb_uploads/{date}/05_book_upload_base.csv).",
    )
    parser.add_argument(
        "--shadow-csv",
        default="",
        help="Output shadow CSV path (default backend/mlb/data/processed/mlb_uploads/{date}/05_book_upload_singles_shadow.csv).",
    )
    parser.add_argument(
        "--out-dir",
        default="",
        help="Experiment output folder (default tmp/experiments/singles_shadow/{date}_{timestamp}).",
    )
    parser.add_argument(
        "--odds-snapshot-in",
        default="",
        help="Odds snapshot JSON input (default backend/mlb/exports/odds_history/{date}/odds_latest_compatible.json).",
    )
    parser.add_argument(
        "--graded-rows-csv",
        default="tmp/mlb_base_vs_market_rows_anybook.csv",
        help="Grading rows CSV for next-day evaluation.",
    )
    parser.add_argument(
        "--singles-model-path",
        default="",
        help="Path to singles.joblib (default resolves from latest singles experiment).",
    )
    parser.add_argument("--threshold", type=float, default=0.55, help="Singles side threshold.")
    parser.add_argument("--top-n", type=int, default=25, help="Max singles rows to add.")
    parser.add_argument("--max-rows-per-player", type=int, default=2, help="Max added rows per player.")
    parser.add_argument("--max-abs-win-pct", type=float, default=500.0, help="Drop rows where abs(WIN %) >= this value.")
    parser.add_argument("--venv-python", default=".venv/bin/python", help="Python executable for pipeline scripts.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    repo_root = Path(__file__).resolve().parents[3]
    date_iso = _parse_iso_date(str(args.date))
    stamp = datetime.now(ET).strftime("%Y%m%d_%H%M%S")

    base_csv = (
        Path(args.base_csv).expanduser()
        if str(args.base_csv).strip()
        else repo_root / f"backend/mlb/data/processed/mlb_uploads/{date_iso}/05_book_upload_base.csv"
    )
    shadow_csv = (
        Path(args.shadow_csv).expanduser()
        if str(args.shadow_csv).strip()
        else repo_root / f"backend/mlb/data/processed/mlb_uploads/{date_iso}/05_book_upload_singles_shadow.csv"
    )
    out_dir = (
        Path(args.out_dir).expanduser()
        if str(args.out_dir).strip()
        else repo_root / f"tmp/experiments/singles_shadow/{date_iso}_{stamp}"
    )
    odds_snapshot_in = (
        Path(args.odds_snapshot_in).expanduser()
        if str(args.odds_snapshot_in).strip()
        else repo_root / f"backend/mlb/exports/odds_history/{date_iso}/odds_latest_compatible.json"
    )
    graded_rows_csv = Path(args.graded_rows_csv).expanduser()
    venv_python = Path(args.venv_python).expanduser()

    if not base_csv.exists():
        raise FileNotFoundError(f"base csv not found: {base_csv}")
    if not odds_snapshot_in.exists():
        raise FileNotFoundError(f"odds snapshot not found: {odds_snapshot_in}")
    if not venv_python.exists():
        raise FileNotFoundError(f"venv python not found: {venv_python}")

    singles_model_path = (
        Path(args.singles_model_path).expanduser().resolve()
        if str(args.singles_model_path).strip()
        else _resolve_default_singles_model(repo_root)
    )
    if not singles_model_path.exists():
        raise FileNotFoundError(f"singles model path not found: {singles_model_path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    _, singles_slate_csv = _build_singles_slate(
        repo_root=repo_root,
        venv_python=venv_python,
        date_iso=date_iso,
        odds_snapshot_in=odds_snapshot_in,
        out_dir=out_dir,
        singles_model_path=singles_model_path,
    )

    base_upload = pd.read_csv(base_csv)
    singles_slate = pd.read_csv(singles_slate_csv)
    singles_slate = singles_slate[singles_slate["prop_type"].astype(str).str.lower() == "singles"].copy()

    added_upload, added_detail, exposure_counts = _select_singles_rows(
        base_upload=base_upload,
        singles_slate=singles_slate,
        date_iso=date_iso,
        threshold=float(args.threshold),
        top_n=int(args.top_n),
        max_rows_per_player=int(args.max_rows_per_player),
        max_abs_win_pct=float(args.max_abs_win_pct),
    )

    shadow_upload = _merge_base_and_shadow(base_upload, added_upload)

    shadow_csv.parent.mkdir(parents=True, exist_ok=True)
    shadow_upload.to_csv(shadow_csv, index=False)
    (out_dir / "05_book_upload_singles_shadow.csv").write_text(
        shadow_upload.to_csv(index=False),
        encoding="utf-8",
    )

    singles_rows_csv = out_dir / "singles_shadow_rows.csv"
    added_detail.to_csv(singles_rows_csv, index=False)

    diff_df = _build_diff(base_upload, shadow_upload)
    diff_csv = out_dir / "singles_shadow_vs_base.csv"
    diff_df.to_csv(diff_csv, index=False)

    base_norm = compare_utils._normalize_upload_df(base_upload, variant="base")  # noqa: SLF001
    shadow_norm = compare_utils._normalize_upload_df(shadow_upload, variant="singles_shadow")  # noqa: SLF001
    grading_df, grading_meta = compare_utils._load_grading_rows(graded_rows_csv, target_date=date_iso)  # noqa: SLF001

    base_eval = compare_utils._apply_outcomes(base_norm, grading_df)  # noqa: SLF001
    shadow_eval = compare_utils._apply_outcomes(shadow_norm, grading_df)  # noqa: SLF001

    if added_upload.empty:
        added_eval = pd.DataFrame(columns=list(shadow_eval.columns))
    else:
        added_norm = compare_utils._normalize_upload_df(added_upload, variant="added")  # noqa: SLF001
        added_eval = shadow_eval.merge(
            added_norm[compare_utils.SHARED_KEY_COLS].drop_duplicates(),  # noqa: SLF001
            on=compare_utils.SHARED_KEY_COLS,  # noqa: SLF001
            how="inner",
        )

    base_stats = _stats_from_eval(base_eval)
    shadow_stats = _stats_from_eval(shadow_eval)
    added_stats = _stats_from_eval(added_eval)

    summary_md = out_dir / "singles_shadow_summary.md"
    _write_summary_md(
        out_path=summary_md,
        date_iso=date_iso,
        shadow_csv_path=shadow_csv,
        added_rows=added_upload,
        exposure_counts=exposure_counts,
        base_stats=base_stats,
        shadow_stats=shadow_stats,
        added_stats=added_stats,
        grading_meta=grading_meta,
    )

    summary_json = out_dir / "singles_shadow_summary.json"
    payload = {
        "date": date_iso,
        "inputs": {
            "base_csv": str(base_csv),
            "odds_snapshot_in": str(odds_snapshot_in),
            "singles_model_path": str(singles_model_path),
            "graded_rows_csv": str(graded_rows_csv),
        },
        "controls": {
            "threshold": float(args.threshold),
            "top_n": int(args.top_n),
            "max_rows_per_player": int(args.max_rows_per_player),
            "max_abs_win_pct": float(args.max_abs_win_pct),
        },
        "exposure_counts": exposure_counts,
        "stats": {
            "base": base_stats.__dict__,
            "shadow": shadow_stats.__dict__,
            "added_singles": added_stats.__dict__,
        },
        "grading": grading_meta,
        "outputs": {
            "shadow_csv": str(shadow_csv),
            "singles_shadow_rows_csv": str(singles_rows_csv),
            "singles_shadow_vs_base_csv": str(diff_csv),
            "singles_shadow_summary_md": str(summary_md),
        },
    }
    summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"[singles-shadow] date={date_iso}")
    print(f"[singles-shadow] base_csv={base_csv} rows={len(base_upload)}")
    print(f"[singles-shadow] added_singles_rows={len(added_upload)}")
    print(f"[singles-shadow] shadow_csv={shadow_csv} rows={len(shadow_upload)}")
    print(f"[singles-shadow] singles_rows_csv={singles_rows_csv}")
    print(f"[singles-shadow] diff_csv={diff_csv}")
    print(f"[singles-shadow] summary_md={summary_md}")
    if grading_meta.get("grading_note"):
        print(f"[singles-shadow] grading_note={grading_meta.get('grading_note')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
