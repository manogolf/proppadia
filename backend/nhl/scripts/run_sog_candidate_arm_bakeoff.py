#!/usr/bin/env python3
"""Build NHL SOG candidate-card arms and emit a single comparison report.

Arms:
  1) base
  2) calibrated (segmented recency calibration applied to base predictions)
  3) defense_blend_calibrated (shadow/blend predictions + segmented calibration)
  4) base_v2_calibrated (recency-weighted ridge base_v2 shadow + segmented calibration)
  5) residual_prob (base market transformed by residual p_over model)
  6) full_refit_prob (historical full-refit model applied to live market + features)
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
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPTS_DIR = Path(__file__).resolve().parent
DEFAULT_BASE_PRED = Path("backend/nhl/data/processed/sog_predictions_wide_calibrated.csv")
DEFAULT_SHADOW_PRED = Path("backend/nhl/data/processed/sog_predictions_wide_defense_surprise_shadow.csv")
DEFAULT_POLICY_JSON = Path("tmp/nhl_sog_walkforward_summary.json")
DEFAULT_ODDS_JSON = Path("nhl/site/data/odds_latest.json")
DEFAULT_OUT_ROOT = Path("tmp/analysis/arm_bakeoff")
DEFAULT_RESIDUAL_HISTORY_ROWS = Path("tmp/nhl_sog_base_vs_betonline_rows.csv")
DEFAULT_FULL_REFIT_DATASET = Path("backend/nhl/data/analysis/sog_poisson_residual_dataset_season_2025.csv")
DEFAULT_FULL_REFIT_ODDS_ROOT = Path("backend/nhl/exports/odds_history")


@dataclass
class RunResult:
    ok: bool
    cmd: list[str]
    stdout: str
    stderr: str
    returncode: int

    @property
    def stdout_tail(self) -> list[str]:
        return [x for x in (self.stdout or "").splitlines()[-12:] if x.strip()]

    @property
    def stderr_tail(self) -> list[str]:
        return [x for x in (self.stderr or "").splitlines()[-12:] if x.strip()]


def _run_cmd(cmd: list[str], *, env: dict[str, str] | None = None) -> RunResult:
    cp = subprocess.run(cmd, text=True, capture_output=True, env=env)
    return RunResult(
        ok=cp.returncode == 0,
        cmd=[str(x) for x in cmd],
        stdout=cp.stdout or "",
        stderr=cp.stderr or "",
        returncode=int(cp.returncode),
    )


def _run_score_base_v2(
    *,
    py: str,
    features_csv: Path,
    out_pred_csv: Path,
    slate_date: str,
    history_from_date: str,
    history_to_date: str,
    ridge_alpha: float,
    half_life_days: float,
    min_train_rows: int,
    min_multiplier: float,
    max_multiplier: float,
    min_coverage_weight: float,
) -> RunResult:
    cmd = [
        py,
        str(SCRIPTS_DIR / "score_sog_poisson_base_v2_shadow.py"),
        "--in",
        str(features_csv),
        "--out",
        str(out_pred_csv),
        "--slate-date",
        str(slate_date),
        "--ridge-alpha",
        str(float(ridge_alpha)),
        "--half-life-days",
        str(float(half_life_days)),
        "--min-train-rows",
        str(int(min_train_rows)),
        "--min-multiplier",
        str(float(min_multiplier)),
        "--max-multiplier",
        str(float(max_multiplier)),
        "--min-coverage-weight",
        str(float(min_coverage_weight)),
    ]
    if str(history_from_date).strip():
        cmd.extend(["--history-from-date", str(history_from_date).strip()])
    if str(history_to_date).strip():
        cmd.extend(["--history-to-date", str(history_to_date).strip()])
    return _run_cmd(cmd, env=dict(os.environ))


def _read_single_slate(pred_csv: Path) -> str:
    if not pred_csv.exists():
        raise FileNotFoundError(f"pred csv not found: {pred_csv}")
    df = pd.read_csv(pred_csv, usecols=lambda c: c in {"game_date"})
    if "game_date" not in df.columns:
        raise RuntimeError(f"{pred_csv} missing game_date column")
    dates = sorted(set(df["game_date"].dropna().astype(str).tolist()))
    if not dates:
        raise RuntimeError(f"{pred_csv} has no game_date values")
    if len(dates) == 1:
        return str(dates[0])
    raise RuntimeError(f"{pred_csv} has multiple game_date values: {dates[:5]} (n={len(dates)})")


def _choose_shadow_prefix(df: pd.DataFrame, preferred_prefixes: list[str]) -> str:
    cols = set(df.columns.tolist())
    for pref in preferred_prefixes:
        if (
            f"{pref}_over_1_5" in cols
            and f"{pref}_over_2_5" in cols
            and f"{pref}_over_3_5" in cols
        ):
            return pref

    cands: list[str] = []
    for col in cols:
        m = re.match(r"^(p_[A-Za-z0-9_]+)_over_1_5$", str(col))
        if not m:
            continue
        pref = m.group(1)
        if f"{pref}_over_2_5" in cols and f"{pref}_over_3_5" in cols:
            cands.append(pref)
    if not cands:
        raise RuntimeError("No usable shadow probability prefix found for _over_{1_5,2_5,3_5}.")

    def score(prefix: str) -> tuple[int, str]:
        low = prefix.lower()
        if "blend" in low:
            rank = 0
        elif "projected" in low:
            rank = 1
        elif "offense" in low:
            rank = 2
        else:
            rank = 3
        return (rank, prefix)

    cands.sort(key=score)
    return cands[0]


def _build_shadow_wide(
    shadow_csv: Path,
    out_csv: Path,
    preferred_prefixes: list[str],
) -> dict[str, Any]:
    if not shadow_csv.exists():
        raise FileNotFoundError(f"shadow csv not found: {shadow_csv}")
    df = pd.read_csv(shadow_csv)
    for c in ("player_id", "game_id", "game_date"):
        if c not in df.columns:
            raise RuntimeError(f"shadow csv missing required column: {c}")

    prefix = _choose_shadow_prefix(df, preferred_prefixes)
    out = df[["player_id", "game_id", "game_date"]].copy()
    out["p_over_1_5"] = pd.to_numeric(df[f"{prefix}_over_1_5"], errors="coerce")
    out["p_over_2_5"] = pd.to_numeric(df[f"{prefix}_over_2_5"], errors="coerce")
    out["p_over_3_5"] = pd.to_numeric(df[f"{prefix}_over_3_5"], errors="coerce")
    out = out.dropna(subset=["player_id", "game_id", "game_date", "p_over_1_5", "p_over_2_5", "p_over_3_5"]).copy()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    return {"prefix": prefix, "rows": int(len(out)), "csv": str(out_csv)}


def _remove_db_env(env: dict[str, str]) -> dict[str, str]:
    out = dict(env)
    out.pop("SUPABASE_DB_URL", None)
    out.pop("DATABASE_URL", None)
    return out


def _arm_keyset(card_csv: Path) -> set[tuple[int, int, float, str]]:
    if (not card_csv) or (not card_csv.exists()) or (not card_csv.is_file()):
        return set()
    df = pd.read_csv(card_csv)
    need = {"player_id", "game_id", "line", "model_pick"}
    if not need.issubset(set(df.columns)):
        return set()
    out: set[tuple[int, int, float, str]] = set()
    for row in df.itertuples(index=False):
        try:
            pid = int(float(getattr(row, "player_id")))
            gid = int(float(getattr(row, "game_id")))
            line = round(float(getattr(row, "line")), 1)
            pick = str(getattr(row, "model_pick")).strip().lower()
        except Exception:
            continue
        if pick not in {"over", "under"}:
            continue
        out.add((pid, gid, line, pick))
    return out


def _pairwise_overlap(arm_sets: dict[str, set[tuple[int, int, float, str]]]) -> list[dict[str, Any]]:
    names = sorted(arm_sets.keys())
    out: list[dict[str, Any]] = []
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a = names[i]
            b = names[j]
            sa = arm_sets[a]
            sb = arm_sets[b]
            inter = sa & sb
            union = sa | sb
            out.append(
                {
                    "arm_a": a,
                    "arm_b": b,
                    "count_a": int(len(sa)),
                    "count_b": int(len(sb)),
                    "intersection": int(len(inter)),
                    "only_a": int(len(sa - sb)),
                    "only_b": int(len(sb - sa)),
                    "jaccard": (float(len(inter)) / float(len(union))) if union else None,
                }
            )
    return out


def _run_calibration(
    *,
    py: str,
    pred_csv: Path,
    model_family: str,
    model_version: str,
    lookback_days: int,
    segment_min_rows: int,
    blend_alpha: float,
    decay_half_life_days: float,
) -> RunResult:
    cmd = [
        py,
        str(SCRIPTS_DIR / "calibrate_sog_segmented_recency.py"),
        "--pred-csv",
        str(pred_csv),
        "--out-csv",
        str(pred_csv),
        "--model-family",
        str(model_family),
        "--model-version",
        str(model_version),
        "--lines",
        "1.5,2.5,3.5",
        "--lookback-days",
        str(int(lookback_days)),
        "--segment-min-rows",
        str(int(segment_min_rows)),
        "--blend-alpha",
        str(float(blend_alpha)),
        "--decay-half-life-days",
        str(float(decay_half_life_days)),
    ]
    return _run_cmd(cmd, env=dict(os.environ))


def _run_build_market(
    *,
    py: str,
    pred_csv: Path,
    names_csv: Path,
    odds_json: Path,
    slate_date: str,
    out_csv: Path,
    unmatched_csv: Path,
) -> RunResult:
    cmd = [
        py,
        str(SCRIPTS_DIR / "build_sog_with_market.py"),
        "--pred",
        str(pred_csv),
        "--names",
        str(names_csv),
        "--odds-json",
        str(odds_json),
        "--out",
        str(out_csv),
        "--unmatched",
        str(unmatched_csv),
        "--slate-date",
        str(slate_date),
    ]
    # Force CSV spine path for deterministic arm comparisons.
    env = _remove_db_env(dict(os.environ))
    return _run_cmd(cmd, env=env)


def _run_select(
    *,
    py: str,
    market_csv: Path,
    policy_json: Path,
    slate_date: str,
    out_csv: Path,
    out_json: Path,
) -> RunResult:
    cmd = [
        py,
        str(SCRIPTS_DIR / "select_sog_candidates_live.py"),
        "--market-csv",
        str(market_csv),
        "--policy-json",
        str(policy_json),
        "--game-date",
        str(slate_date),
        "--out-csv",
        str(out_csv),
        "--out-json",
        str(out_json),
    ]
    return _run_cmd(cmd, env=dict(os.environ))


def _run_build_residual_market(
    *,
    py: str,
    history_rows_csv: Path,
    market_csv_in: Path,
    market_csv_out: Path,
    summary_json: Path,
    game_date: str,
    min_train_rows_per_line: int,
    blend_alpha: float,
) -> RunResult:
    cmd = [
        py,
        str(SCRIPTS_DIR / "build_sog_market_residual_arm.py"),
        "--history-rows-csv",
        str(history_rows_csv),
        "--market-csv-in",
        str(market_csv_in),
        "--market-csv-out",
        str(market_csv_out),
        "--summary-json",
        str(summary_json),
        "--game-date",
        str(game_date),
        "--min-train-rows-per-line",
        str(int(min_train_rows_per_line)),
        "--blend-alpha",
        str(float(blend_alpha)),
    ]
    return _run_cmd(cmd, env=dict(os.environ))


def _run_build_full_refit_market(
    *,
    py: str,
    dataset_csv: Path,
    odds_root: Path,
    bookmaker: str,
    market_csv_in: Path,
    features_csv: Path,
    market_csv_out: Path,
    summary_json: Path,
    game_date: str,
    train_from_date: str,
    train_to_date: str,
    min_train_rows_per_line: int,
    blend_alpha: float,
) -> RunResult:
    cmd = [
        py,
        str(SCRIPTS_DIR / "build_sog_market_full_refit_arm.py"),
        "--dataset-csv",
        str(dataset_csv),
        "--odds-root",
        str(odds_root),
        "--bookmaker",
        str(bookmaker),
        "--market-csv-in",
        str(market_csv_in),
        "--features-csv",
        str(features_csv),
        "--market-csv-out",
        str(market_csv_out),
        "--summary-json",
        str(summary_json),
        "--game-date",
        str(game_date),
        "--min-train-rows-per-line",
        str(int(min_train_rows_per_line)),
        "--blend-alpha",
        str(float(blend_alpha)),
    ]
    if str(train_from_date).strip():
        cmd.extend(["--train-from-date", str(train_from_date).strip()])
    if str(train_to_date).strip():
        cmd.extend(["--train-to-date", str(train_to_date).strip()])
    return _run_cmd(cmd, env=dict(os.environ))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _run_arm(
    *,
    arm_name: str,
    py: str,
    pred_src_csv: Path,
    work_dir: Path,
    names_csv: Path,
    odds_json: Path,
    policy_json: Path,
    slate_date: str,
    apply_calibration: bool,
    strict_calibration: bool,
    cal_model_family: str,
    cal_model_version: str,
    cal_lookback_days: int,
    cal_segment_min_rows: int,
    cal_blend_alpha: float,
    cal_decay_half_life_days: float,
) -> dict[str, Any]:
    arm_dir = work_dir / arm_name
    arm_dir.mkdir(parents=True, exist_ok=True)

    pred_csv = arm_dir / f"sog_predictions_{arm_name}.csv"
    shutil.copy2(pred_src_csv, pred_csv)

    out: dict[str, Any] = {
        "arm": arm_name,
        "ok": True,
        "pred_src_csv": str(pred_src_csv),
        "pred_csv": str(pred_csv),
        "calibration": {"requested": bool(apply_calibration), "applied": False, "fallback_identity": False},
    }

    if apply_calibration:
        cal_res = _run_calibration(
            py=py,
            pred_csv=pred_csv,
            model_family=cal_model_family,
            model_version=cal_model_version,
            lookback_days=cal_lookback_days,
            segment_min_rows=cal_segment_min_rows,
            blend_alpha=cal_blend_alpha,
            decay_half_life_days=cal_decay_half_life_days,
        )
        out["calibration"]["cmd"] = cal_res.cmd
        out["calibration"]["returncode"] = cal_res.returncode
        out["calibration"]["stdout_tail"] = cal_res.stdout_tail
        out["calibration"]["stderr_tail"] = cal_res.stderr_tail
        if cal_res.ok:
            out["calibration"]["applied"] = True
        else:
            out["calibration"]["error"] = "segmented calibration failed"
            if strict_calibration:
                out["ok"] = False
                out["error"] = "calibration failed and strict_calibration=true"
                return out
            out["calibration"]["fallback_identity"] = True

    market_csv = arm_dir / f"sog_with_market_{arm_name}.csv"
    unmatched_csv = arm_dir / f"unmatched_sog_{arm_name}.csv"
    market_res = _run_build_market(
        py=py,
        pred_csv=pred_csv,
        names_csv=names_csv,
        odds_json=odds_json,
        slate_date=slate_date,
        out_csv=market_csv,
        unmatched_csv=unmatched_csv,
    )
    out["build_market"] = {
        "cmd": market_res.cmd,
        "returncode": market_res.returncode,
        "stdout_tail": market_res.stdout_tail,
        "stderr_tail": market_res.stderr_tail,
        "market_csv": str(market_csv),
        "unmatched_csv": str(unmatched_csv),
    }
    if not market_res.ok:
        out["ok"] = False
        out["error"] = "build_sog_with_market failed"
        return out

    card_csv = arm_dir / f"nhl_sog_card_{arm_name}.csv"
    card_json = arm_dir / f"nhl_sog_card_{arm_name}_summary.json"
    sel_res = _run_select(
        py=py,
        market_csv=market_csv,
        policy_json=policy_json,
        slate_date=slate_date,
        out_csv=card_csv,
        out_json=card_json,
    )
    out["select"] = {
        "cmd": sel_res.cmd,
        "returncode": sel_res.returncode,
        "stdout_tail": sel_res.stdout_tail,
        "stderr_tail": sel_res.stderr_tail,
        "card_csv": str(card_csv),
        "card_summary_json": str(card_json),
    }
    if not sel_res.ok:
        out["ok"] = False
        out["error"] = "select_sog_candidates_live failed"
        return out

    out["selected_summary"] = _read_json(card_json)
    out["card_rows"] = int(len(pd.read_csv(card_csv))) if card_csv.exists() else 0
    return out


def _run_residual_market_arm(
    *,
    arm_name: str,
    py: str,
    base_market_csv: Path,
    history_rows_csv: Path,
    work_dir: Path,
    policy_json: Path,
    slate_date: str,
    min_train_rows_per_line: int,
    blend_alpha: float,
) -> dict[str, Any]:
    arm_dir = work_dir / arm_name
    arm_dir.mkdir(parents=True, exist_ok=True)

    market_csv = arm_dir / f"sog_with_market_{arm_name}.csv"
    residual_summary_json = arm_dir / f"residual_model_{arm_name}_summary.json"

    out: dict[str, Any] = {
        "arm": arm_name,
        "ok": True,
        "base_market_csv": str(base_market_csv),
        "history_rows_csv": str(history_rows_csv),
        "market_csv": str(market_csv),
        "residual_model": {
            "min_train_rows_per_line": int(min_train_rows_per_line),
            "blend_alpha": float(blend_alpha),
        },
    }

    res = _run_build_residual_market(
        py=py,
        history_rows_csv=history_rows_csv,
        market_csv_in=base_market_csv,
        market_csv_out=market_csv,
        summary_json=residual_summary_json,
        game_date=slate_date,
        min_train_rows_per_line=int(min_train_rows_per_line),
        blend_alpha=float(blend_alpha),
    )
    out["residual_model"]["cmd"] = res.cmd
    out["residual_model"]["returncode"] = res.returncode
    out["residual_model"]["stdout_tail"] = res.stdout_tail
    out["residual_model"]["stderr_tail"] = res.stderr_tail
    out["residual_model"]["summary_json"] = str(residual_summary_json)
    out["residual_model"]["summary"] = _read_json(residual_summary_json)
    if not res.ok:
        out["ok"] = False
        out["error"] = "build_sog_market_residual_arm failed"
        return out

    card_csv = arm_dir / f"nhl_sog_card_{arm_name}.csv"
    card_json = arm_dir / f"nhl_sog_card_{arm_name}_summary.json"
    sel_res = _run_select(
        py=py,
        market_csv=market_csv,
        policy_json=policy_json,
        slate_date=slate_date,
        out_csv=card_csv,
        out_json=card_json,
    )
    out["select"] = {
        "cmd": sel_res.cmd,
        "returncode": sel_res.returncode,
        "stdout_tail": sel_res.stdout_tail,
        "stderr_tail": sel_res.stderr_tail,
        "card_csv": str(card_csv),
        "card_summary_json": str(card_json),
    }
    if not sel_res.ok:
        out["ok"] = False
        out["error"] = "select_sog_candidates_live failed"
        return out

    out["selected_summary"] = _read_json(card_json)
    out["card_rows"] = int(len(pd.read_csv(card_csv))) if card_csv.exists() else 0
    return out


def _run_full_refit_market_arm(
    *,
    arm_name: str,
    py: str,
    dataset_csv: Path,
    odds_root: Path,
    bookmaker: str,
    base_market_csv: Path,
    features_csv: Path,
    work_dir: Path,
    policy_json: Path,
    slate_date: str,
    train_from_date: str,
    train_to_date: str,
    min_train_rows_per_line: int,
    blend_alpha: float,
) -> dict[str, Any]:
    arm_dir = work_dir / arm_name
    arm_dir.mkdir(parents=True, exist_ok=True)

    market_csv = arm_dir / f"sog_with_market_{arm_name}.csv"
    model_summary_json = arm_dir / f"full_refit_model_{arm_name}_summary.json"

    out: dict[str, Any] = {
        "arm": arm_name,
        "ok": True,
        "dataset_csv": str(dataset_csv),
        "odds_root": str(odds_root),
        "bookmaker": str(bookmaker),
        "base_market_csv": str(base_market_csv),
        "features_csv": str(features_csv),
        "market_csv": str(market_csv),
        "full_refit_model": {
            "train_from_date": (str(train_from_date).strip() or None),
            "train_to_date": (str(train_to_date).strip() or None),
            "min_train_rows_per_line": int(min_train_rows_per_line),
            "blend_alpha": float(blend_alpha),
        },
    }

    res = _run_build_full_refit_market(
        py=py,
        dataset_csv=dataset_csv,
        odds_root=odds_root,
        bookmaker=bookmaker,
        market_csv_in=base_market_csv,
        features_csv=features_csv,
        market_csv_out=market_csv,
        summary_json=model_summary_json,
        game_date=slate_date,
        train_from_date=str(train_from_date).strip(),
        train_to_date=str(train_to_date).strip(),
        min_train_rows_per_line=int(min_train_rows_per_line),
        blend_alpha=float(blend_alpha),
    )
    out["full_refit_model"]["cmd"] = res.cmd
    out["full_refit_model"]["returncode"] = res.returncode
    out["full_refit_model"]["stdout_tail"] = res.stdout_tail
    out["full_refit_model"]["stderr_tail"] = res.stderr_tail
    out["full_refit_model"]["summary_json"] = str(model_summary_json)
    out["full_refit_model"]["summary"] = _read_json(model_summary_json)
    if not res.ok:
        out["ok"] = False
        out["error"] = "build_sog_market_full_refit_arm failed"
        return out

    card_csv = arm_dir / f"nhl_sog_card_{arm_name}.csv"
    card_json = arm_dir / f"nhl_sog_card_{arm_name}_summary.json"
    sel_res = _run_select(
        py=py,
        market_csv=market_csv,
        policy_json=policy_json,
        slate_date=slate_date,
        out_csv=card_csv,
        out_json=card_json,
    )
    out["select"] = {
        "cmd": sel_res.cmd,
        "returncode": sel_res.returncode,
        "stdout_tail": sel_res.stdout_tail,
        "stderr_tail": sel_res.stderr_tail,
        "card_csv": str(card_csv),
        "card_summary_json": str(card_json),
    }
    if not sel_res.ok:
        out["ok"] = False
        out["error"] = "select_sog_candidates_live failed"
        return out

    out["selected_summary"] = _read_json(card_json)
    out["card_rows"] = int(len(pd.read_csv(card_csv))) if card_csv.exists() else 0
    return out


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run NHL SOG multi-arm candidate bakeoff and write one comparison report.")
    ap.add_argument("--slate-date", default="", help="YYYY-MM-DD. Default: infer from base prediction CSV.")
    ap.add_argument("--base-pred-csv", default=str(DEFAULT_BASE_PRED))
    ap.add_argument("--shadow-pred-csv", default=str(DEFAULT_SHADOW_PRED))
    ap.add_argument("--policy-json", default=str(DEFAULT_POLICY_JSON))
    ap.add_argument("--odds-json", default=str(DEFAULT_ODDS_JSON))
    ap.add_argument(
        "--names-csv",
        default="",
        help="Default: backend/nhl/exports/daily/names/names_<slate>.csv",
    )
    ap.add_argument("--out-root", default=str(DEFAULT_OUT_ROOT))
    ap.add_argument(
        "--shadow-prefix-preference",
        default="p_blend_w0_4,p_projected_a0_4,p_projected_a0_5,p_projected_a0_6,p_offense",
        help="Comma-separated prefix priority for shadow conversion.",
    )
    ap.add_argument(
        "--cal-model-family",
        default=os.environ.get("NHL_SOG_MODEL_FAMILY") or "poisson_baseline",
    )
    ap.add_argument(
        "--cal-model-version",
        default=os.environ.get("NHL_SOG_MODEL_VERSION") or "baseline_v1",
    )
    ap.add_argument("--cal-lookback-days", type=int, default=120)
    ap.add_argument("--cal-segment-min-rows", type=int, default=120)
    ap.add_argument("--cal-blend-alpha", type=float, default=0.65)
    ap.add_argument("--cal-decay-half-life-days", type=float, default=21.0)
    ap.add_argument(
        "--strict-calibration",
        action="store_true",
        help="Fail arm if calibration fails (default falls back to identity).",
    )
    ap.add_argument(
        "--strict-arms",
        action="store_true",
        help="Exit non-zero if any arm fails.",
    )
    ap.add_argument(
        "--enable-base-v2-arm",
        action="store_true",
        help="Enable base_v2_calibrated arm (recency-weighted ridge adjustment on top of base lambda).",
    )
    ap.add_argument(
        "--base-v2-features-csv",
        default="",
        help="Default: backend/nhl/exports/daily/sog_features/sog_features_<slate>_denali.csv",
    )
    ap.add_argument(
        "--base-v2-history-from-date",
        default="",
        help="Optional inclusive lower bound for base_v2 training history.",
    )
    ap.add_argument(
        "--base-v2-history-to-date",
        default="",
        help="Optional exclusive upper bound for base_v2 training history (default: slate date).",
    )
    ap.add_argument("--base-v2-ridge-alpha", type=float, default=25.0)
    ap.add_argument("--base-v2-half-life-days", type=float, default=45.0)
    ap.add_argument("--base-v2-min-train-rows", type=int, default=5000)
    ap.add_argument("--base-v2-min-multiplier", type=float, default=0.75)
    ap.add_argument("--base-v2-max-multiplier", type=float, default=1.30)
    ap.add_argument("--base-v2-min-coverage-weight", type=float, default=0.50)
    ap.add_argument(
        "--disable-residual-arm",
        action="store_true",
        help="Skip residual_prob arm (enabled by default).",
    )
    ap.add_argument(
        "--residual-history-rows-csv",
        default=str(DEFAULT_RESIDUAL_HISTORY_ROWS),
        help="Historical rows CSV used to fit residual_prob arm.",
    )
    ap.add_argument(
        "--residual-min-train-rows-per-line",
        type=int,
        default=400,
        help="Minimum train rows per line for residual_prob line model.",
    )
    ap.add_argument(
        "--residual-blend-alpha",
        type=float,
        default=1.0,
        help="Blend alpha for residual arm output (1.0 = pure residual model).",
    )
    ap.add_argument(
        "--disable-full-refit-arm",
        action="store_true",
        help="Skip full_refit_prob arm (enabled by default).",
    )
    ap.add_argument(
        "--full-refit-dataset-csv",
        default=str(DEFAULT_FULL_REFIT_DATASET),
        help="Historical feature dataset used for full_refit_prob training.",
    )
    ap.add_argument(
        "--full-refit-odds-root",
        default=str(DEFAULT_FULL_REFIT_ODDS_ROOT),
        help="Historical odds root used for full_refit_prob training.",
    )
    ap.add_argument(
        "--full-refit-bookmaker",
        default="betonlineag",
        help="Bookmaker key used for full_refit_prob training.",
    )
    ap.add_argument(
        "--full-refit-features-csv",
        default="",
        help="Default: backend/nhl/exports/daily/sog_features/sog_features_<slate>_denali.csv",
    )
    ap.add_argument(
        "--full-refit-train-from-date",
        default="",
        help="Optional inclusive train lower date for full_refit_prob.",
    )
    ap.add_argument(
        "--full-refit-train-to-date",
        default="",
        help="Optional inclusive train upper date for full_refit_prob (before game-date cutoff).",
    )
    ap.add_argument(
        "--full-refit-min-train-rows-per-line",
        type=int,
        default=400,
        help="Minimum train rows per line for full_refit_prob line model.",
    )
    ap.add_argument(
        "--full-refit-blend-alpha",
        type=float,
        default=1.0,
        help="Blend alpha for full_refit_prob output (1.0 = pure full-refit model).",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    py = sys.executable

    base_pred_csv = Path(args.base_pred_csv)
    shadow_pred_csv = Path(args.shadow_pred_csv)
    policy_json = Path(args.policy_json)
    odds_json = Path(args.odds_json)
    residual_history_rows = Path(args.residual_history_rows_csv)
    full_refit_dataset_csv = Path(args.full_refit_dataset_csv)
    full_refit_odds_root = Path(args.full_refit_odds_root)

    slate_date = str(args.slate_date).strip() or _read_single_slate(base_pred_csv)
    names_csv = Path(args.names_csv) if str(args.names_csv).strip() else Path(f"backend/nhl/exports/daily/names/names_{slate_date}.csv")
    base_v2_features_csv = (
        Path(args.base_v2_features_csv)
        if str(args.base_v2_features_csv).strip()
        else Path(f"backend/nhl/exports/daily/sog_features/sog_features_{slate_date}_denali.csv")
    )
    full_refit_features_csv = (
        Path(args.full_refit_features_csv)
        if str(args.full_refit_features_csv).strip()
        else Path(f"backend/nhl/exports/daily/sog_features/sog_features_{slate_date}_denali.csv")
    )
    out_dir = Path(args.out_root) / slate_date
    out_dir.mkdir(parents=True, exist_ok=True)

    if not base_pred_csv.exists():
        raise SystemExit(f"base pred csv not found: {base_pred_csv}")
    if not policy_json.exists():
        raise SystemExit(f"policy json not found: {policy_json}")
    if not odds_json.exists():
        raise SystemExit(f"odds json not found: {odds_json}")
    if not names_csv.exists():
        raise SystemExit(f"names csv not found: {names_csv}")
    if bool(args.enable_base_v2_arm) and (not base_v2_features_csv.exists()):
        raise SystemExit(f"base_v2 features csv not found: {base_v2_features_csv}")
    if (not bool(args.disable_residual_arm)) and (not residual_history_rows.exists()):
        raise SystemExit(f"residual history rows csv not found: {residual_history_rows}")
    if not bool(args.disable_full_refit_arm):
        if not full_refit_dataset_csv.exists():
            raise SystemExit(f"full_refit dataset csv not found: {full_refit_dataset_csv}")
        if not full_refit_odds_root.exists():
            raise SystemExit(f"full_refit odds root not found: {full_refit_odds_root}")
        if not full_refit_features_csv.exists():
            raise SystemExit(f"full_refit features csv not found: {full_refit_features_csv}")

    preferred_prefixes = [x.strip() for x in str(args.shadow_prefix_preference).split(",") if x.strip()]
    shadow_wide_csv = out_dir / "sog_predictions_shadow_wide.csv"
    shadow_meta: dict[str, Any] = {"ok": False}
    try:
        shadow_meta = {"ok": True, **_build_shadow_wide(shadow_pred_csv, shadow_wide_csv, preferred_prefixes)}
    except Exception as exc:
        shadow_meta = {"ok": False, "error": str(exc), "shadow_pred_csv": str(shadow_pred_csv)}

    arms: dict[str, dict[str, Any]] = {}
    arms["base"] = _run_arm(
        arm_name="base",
        py=py,
        pred_src_csv=base_pred_csv,
        work_dir=out_dir,
        names_csv=names_csv,
        odds_json=odds_json,
        policy_json=policy_json,
        slate_date=slate_date,
        apply_calibration=False,
        strict_calibration=False,
        cal_model_family=args.cal_model_family,
        cal_model_version=args.cal_model_version,
        cal_lookback_days=args.cal_lookback_days,
        cal_segment_min_rows=args.cal_segment_min_rows,
        cal_blend_alpha=args.cal_blend_alpha,
        cal_decay_half_life_days=args.cal_decay_half_life_days,
    )

    arms["calibrated"] = _run_arm(
        arm_name="calibrated",
        py=py,
        pred_src_csv=base_pred_csv,
        work_dir=out_dir,
        names_csv=names_csv,
        odds_json=odds_json,
        policy_json=policy_json,
        slate_date=slate_date,
        apply_calibration=True,
        strict_calibration=bool(args.strict_calibration),
        cal_model_family=args.cal_model_family,
        cal_model_version=args.cal_model_version,
        cal_lookback_days=args.cal_lookback_days,
        cal_segment_min_rows=args.cal_segment_min_rows,
        cal_blend_alpha=args.cal_blend_alpha,
        cal_decay_half_life_days=args.cal_decay_half_life_days,
    )

    if bool(args.enable_base_v2_arm):
        base_v2_pred_csv = out_dir / "sog_predictions_base_v2_shadow.csv"
        base_v2_score = _run_score_base_v2(
            py=py,
            features_csv=base_v2_features_csv,
            out_pred_csv=base_v2_pred_csv,
            slate_date=slate_date,
            history_from_date=str(args.base_v2_history_from_date),
            history_to_date=str(args.base_v2_history_to_date),
            ridge_alpha=float(args.base_v2_ridge_alpha),
            half_life_days=float(args.base_v2_half_life_days),
            min_train_rows=int(args.base_v2_min_train_rows),
            min_multiplier=float(args.base_v2_min_multiplier),
            max_multiplier=float(args.base_v2_max_multiplier),
            min_coverage_weight=float(args.base_v2_min_coverage_weight),
        )
        if base_v2_score.ok and base_v2_pred_csv.exists():
            arm_base_v2 = _run_arm(
                arm_name="base_v2_calibrated",
                py=py,
                pred_src_csv=base_v2_pred_csv,
                work_dir=out_dir,
                names_csv=names_csv,
                odds_json=odds_json,
                policy_json=policy_json,
                slate_date=slate_date,
                apply_calibration=True,
                strict_calibration=bool(args.strict_calibration),
                cal_model_family=args.cal_model_family,
                cal_model_version=args.cal_model_version,
                cal_lookback_days=args.cal_lookback_days,
                cal_segment_min_rows=args.cal_segment_min_rows,
                cal_blend_alpha=args.cal_blend_alpha,
                cal_decay_half_life_days=args.cal_decay_half_life_days,
            )
            arm_base_v2["base_v2_score"] = {
                "cmd": base_v2_score.cmd,
                "returncode": base_v2_score.returncode,
                "stdout_tail": base_v2_score.stdout_tail,
                "stderr_tail": base_v2_score.stderr_tail,
            }
            try:
                arm_base_v2["base_v2_score"]["summary"] = json.loads(base_v2_score.stdout)
            except Exception:
                pass
            arms["base_v2_calibrated"] = arm_base_v2
        else:
            arms["base_v2_calibrated"] = {
                "arm": "base_v2_calibrated",
                "ok": False,
                "error": "base_v2 scoring failed",
                "base_v2_score": {
                    "cmd": base_v2_score.cmd,
                    "returncode": base_v2_score.returncode,
                    "stdout_tail": base_v2_score.stdout_tail,
                    "stderr_tail": base_v2_score.stderr_tail,
                },
            }
    else:
        arms["base_v2_calibrated"] = {
            "arm": "base_v2_calibrated",
            "ok": False,
            "error": "disabled by --enable-base-v2-arm=false",
        }

    if shadow_meta.get("ok"):
        arms["defense_blend_calibrated"] = _run_arm(
            arm_name="defense_blend_calibrated",
            py=py,
            pred_src_csv=shadow_wide_csv,
            work_dir=out_dir,
            names_csv=names_csv,
            odds_json=odds_json,
            policy_json=policy_json,
            slate_date=slate_date,
            apply_calibration=True,
            strict_calibration=bool(args.strict_calibration),
            cal_model_family=args.cal_model_family,
            cal_model_version=args.cal_model_version,
            cal_lookback_days=args.cal_lookback_days,
            cal_segment_min_rows=args.cal_segment_min_rows,
            cal_blend_alpha=args.cal_blend_alpha,
            cal_decay_half_life_days=args.cal_decay_half_life_days,
        )
    else:
        arms["defense_blend_calibrated"] = {
            "arm": "defense_blend_calibrated",
            "ok": False,
            "error": "shadow conversion failed",
        }

    if bool(args.disable_residual_arm):
        arms["residual_prob"] = {
            "arm": "residual_prob",
            "ok": False,
            "error": "disabled by --disable-residual-arm",
        }
    else:
        base_market_csv_raw = (
            arms.get("base", {}).get("build_market", {}).get("market_csv")
            if isinstance(arms.get("base", {}).get("build_market"), dict)
            else None
        )
        base_market_csv = Path(base_market_csv_raw) if base_market_csv_raw else Path("__missing__")
        if not arms.get("base", {}).get("ok", False):
            arms["residual_prob"] = {
                "arm": "residual_prob",
                "ok": False,
                "error": "base arm failed; residual_prob requires base market csv",
            }
        elif not base_market_csv.exists():
            arms["residual_prob"] = {
                "arm": "residual_prob",
                "ok": False,
                "error": f"base market csv missing: {base_market_csv}",
            }
        else:
            arms["residual_prob"] = _run_residual_market_arm(
                arm_name="residual_prob",
                py=py,
                base_market_csv=base_market_csv,
                history_rows_csv=residual_history_rows,
                work_dir=out_dir,
                policy_json=policy_json,
                slate_date=slate_date,
                min_train_rows_per_line=int(args.residual_min_train_rows_per_line),
                blend_alpha=float(args.residual_blend_alpha),
            )

    if bool(args.disable_full_refit_arm):
        arms["full_refit_prob"] = {
            "arm": "full_refit_prob",
            "ok": False,
            "error": "disabled by --disable-full-refit-arm",
        }
    else:
        base_market_csv_raw = (
            arms.get("base", {}).get("build_market", {}).get("market_csv")
            if isinstance(arms.get("base", {}).get("build_market"), dict)
            else None
        )
        base_market_csv = Path(base_market_csv_raw) if base_market_csv_raw else Path("__missing__")
        if not arms.get("base", {}).get("ok", False):
            arms["full_refit_prob"] = {
                "arm": "full_refit_prob",
                "ok": False,
                "error": "base arm failed; full_refit_prob requires base market csv",
            }
        elif not base_market_csv.exists():
            arms["full_refit_prob"] = {
                "arm": "full_refit_prob",
                "ok": False,
                "error": f"base market csv missing: {base_market_csv}",
            }
        else:
            arms["full_refit_prob"] = _run_full_refit_market_arm(
                arm_name="full_refit_prob",
                py=py,
                dataset_csv=full_refit_dataset_csv,
                odds_root=full_refit_odds_root,
                bookmaker=str(args.full_refit_bookmaker),
                base_market_csv=base_market_csv,
                features_csv=full_refit_features_csv,
                work_dir=out_dir,
                policy_json=policy_json,
                slate_date=slate_date,
                train_from_date=str(args.full_refit_train_from_date),
                train_to_date=str(args.full_refit_train_to_date),
                min_train_rows_per_line=int(args.full_refit_min_train_rows_per_line),
                blend_alpha=float(args.full_refit_blend_alpha),
            )

    arm_sets: dict[str, set[tuple[int, int, float, str]]] = {}
    for arm_name, info in arms.items():
        card_csv_raw = info.get("select", {}).get("card_csv") if isinstance(info.get("select"), dict) else None
        card_csv = Path(card_csv_raw) if card_csv_raw else Path("__missing__")
        arm_sets[arm_name] = _arm_keyset(card_csv)

    summary = {
        "ok": True,
        "slate_date": slate_date,
        "inputs": {
            "base_pred_csv": str(base_pred_csv),
            "shadow_pred_csv": str(shadow_pred_csv),
            "policy_json": str(policy_json),
            "odds_json": str(odds_json),
            "names_csv": str(names_csv),
            "base_v2_features_csv": str(base_v2_features_csv),
            "residual_history_rows_csv": str(residual_history_rows),
            "full_refit_dataset_csv": str(full_refit_dataset_csv),
            "full_refit_odds_root": str(full_refit_odds_root),
            "full_refit_features_csv": str(full_refit_features_csv),
            "calibration": {
                "model_family": args.cal_model_family,
                "model_version": args.cal_model_version,
                "lookback_days": int(args.cal_lookback_days),
                "segment_min_rows": int(args.cal_segment_min_rows),
                "blend_alpha": float(args.cal_blend_alpha),
                "decay_half_life_days": float(args.cal_decay_half_life_days),
                "strict_calibration": bool(args.strict_calibration),
            },
            "base_v2_arm": {
                "enabled": bool(args.enable_base_v2_arm),
                "history_from_date": (str(args.base_v2_history_from_date).strip() or None),
                "history_to_date": (str(args.base_v2_history_to_date).strip() or None),
                "ridge_alpha": float(args.base_v2_ridge_alpha),
                "half_life_days": float(args.base_v2_half_life_days),
                "min_train_rows": int(args.base_v2_min_train_rows),
                "min_multiplier": float(args.base_v2_min_multiplier),
                "max_multiplier": float(args.base_v2_max_multiplier),
                "min_coverage_weight": float(args.base_v2_min_coverage_weight),
            },
            "residual_arm": {
                "enabled": not bool(args.disable_residual_arm),
                "min_train_rows_per_line": int(args.residual_min_train_rows_per_line),
                "blend_alpha": float(args.residual_blend_alpha),
            },
            "full_refit_arm": {
                "enabled": not bool(args.disable_full_refit_arm),
                "bookmaker": str(args.full_refit_bookmaker),
                "train_from_date": (str(args.full_refit_train_from_date).strip() or None),
                "train_to_date": (str(args.full_refit_train_to_date).strip() or None),
                "min_train_rows_per_line": int(args.full_refit_min_train_rows_per_line),
                "blend_alpha": float(args.full_refit_blend_alpha),
            },
        },
        "shadow_conversion": shadow_meta,
        "arms": arms,
        "comparisons": {
            "row_counts": {k: int(len(v)) for k, v in arm_sets.items()},
            "pairwise_overlap": _pairwise_overlap(arm_sets),
        },
        "out_dir": str(out_dir),
    }

    if any(not arm.get("ok", False) for arm in arms.values()):
        summary["ok"] = False if args.strict_arms else True

    out_json = out_dir / f"nhl_sog_arm_bakeoff_{slate_date}.json"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps(
        {
            "ok": summary["ok"],
            "slate_date": slate_date,
            "row_counts": summary["comparisons"]["row_counts"],
            "out_json": str(out_json),
        },
        indent=2,
    ))

    if args.strict_arms and any(not arm.get("ok", False) for arm in arms.values()):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
