#!/usr/bin/env python3
"""
Apply segmented recency isotonic calibration to NHL SOG wide predictions.

This is a production-safe post-processing step:
  - fits calibrators from historical resolved rows in nhl.predictions
  - calibrates per line and expected_sog_bucket
  - falls back to line-only calibration when bucket calibration is unavailable
  - falls back to identity when line calibration is unavailable

Input CSV is expected to contain:
  - player_id, game_id, game_date
  - p_over_1_5, p_over_2_5, p_over_3_5
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression

from backend.shared.db.pg import pg_fetchall, pg_fetchone


TRAIN_SQL = """
SELECT
  g.game_date::date AS game_date,
  p.line::float8 AS line,
  p.p_over::float8 AS p_over,
  CASE
    WHEN s.shots_on_goal IS NULL THEN NULL
    WHEN s.shots_on_goal >= (floor(p.line)::int + 1) THEN 1
    ELSE 0
  END::int AS y,
  CASE
    WHEN f.d10_sog_per60 IS NULL OR f.d10_toi_min_avg IS NULL THEN 'missing'
    WHEN ((f.d10_sog_per60 * f.d10_toi_min_avg) / 60.0) < 1.5 THEN '<1.5'
    WHEN ((f.d10_sog_per60 * f.d10_toi_min_avg) / 60.0) < 2.5 THEN '1.5-2.5'
    WHEN ((f.d10_sog_per60 * f.d10_toi_min_avg) / 60.0) < 3.5 THEN '2.5-3.5'
    ELSE '3.5+'
  END AS expected_sog_bucket
FROM nhl.predictions p
JOIN nhl.games g
  ON g.game_id = p.game_id
LEFT JOIN nhl.skater_game_logs_raw s
  ON s.game_id = p.game_id
 AND s.player_id = p.player_id
LEFT JOIN nhl.training_features_nhl_sog_enriched_pregame_v2 f
  ON f.game_id = p.game_id
 AND f.player_id = p.player_id
WHERE p.prop = 'shots_on_goal'
  AND p.model_family = %s
  AND p.model_version = %s
  AND p.line = ANY(string_to_array(%s, ',')::float8[])
  AND g.game_date BETWEEN %s::date AND %s::date
"""

SLATE_BUCKET_SQL = """
SELECT
  f.player_id::bigint AS player_id,
  f.game_id::bigint AS game_id,
  CASE
    WHEN f.d10_sog_per60 IS NULL OR f.d10_toi_min_avg IS NULL THEN 'missing'
    WHEN ((f.d10_sog_per60 * f.d10_toi_min_avg) / 60.0) < 1.5 THEN '<1.5'
    WHEN ((f.d10_sog_per60 * f.d10_toi_min_avg) / 60.0) < 2.5 THEN '1.5-2.5'
    WHEN ((f.d10_sog_per60 * f.d10_toi_min_avg) / 60.0) < 3.5 THEN '2.5-3.5'
    ELSE '3.5+'
  END AS expected_sog_bucket
FROM nhl.training_features_nhl_sog_enriched_pregame_v2 f
WHERE f.game_date = %s::date
"""


def _parse_lines(raw: str) -> List[float]:
    vals: List[float] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        vals.append(float(token))
    if not vals:
        raise ValueError("No valid lines parsed from --lines")
    return sorted(set(vals))


def _line_tag(line: float) -> str:
    return f"{line:.1f}".replace(".", "_")


def _prob_col(line: float) -> str:
    return f"p_over_{_line_tag(line)}"


def _half_life_weights(game_dates: pd.Series, asof: date, half_life_days: float) -> np.ndarray:
    if half_life_days <= 0:
        return np.ones(len(game_dates), dtype=float)
    ages = np.array([(asof - d).days for d in game_dates], dtype=float)
    ages = np.clip(ages, 0.0, None)
    return np.power(0.5, ages / float(half_life_days))


def _fit_iso(x: np.ndarray, y: np.ndarray, w: np.ndarray) -> Optional[IsotonicRegression]:
    if x.size == 0:
        return None
    if np.unique(y).size < 2:
        return None
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(x, y, sample_weight=w)
    return iso


def _require_db() -> str:
    db = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db:
        raise RuntimeError("Missing SUPABASE_DB_URL (or DATABASE_URL).")
    return db


def _latest_truth_date() -> date:
    row = pg_fetchone(
        """
        SELECT MAX(game_date)::text AS d
        FROM nhl.skater_game_logs_raw
        WHERE shots_on_goal IS NOT NULL
        """
    ) or {}
    d = row.get("d")
    if not d:
        raise RuntimeError("No truth date found in nhl.skater_game_logs_raw.")
    return date.fromisoformat(str(d))


def _load_training(
    model_family: str,
    model_version: str,
    lines: Sequence[float],
    train_from: date,
    train_to: date,
) -> pd.DataFrame:
    lines_csv = ",".join(f"{x:g}" for x in lines)
    rows = pg_fetchall(
        TRAIN_SQL,
        (model_family, model_version, lines_csv, train_from.isoformat(), train_to.isoformat()),
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["p_over"] = pd.to_numeric(df["p_over"], errors="coerce")
    df["y"] = pd.to_numeric(df["y"], errors="coerce")
    df["expected_sog_bucket"] = df["expected_sog_bucket"].fillna("missing").astype(str)
    df = df.dropna(subset=["game_date", "line", "p_over", "y"]).copy()
    df["y"] = df["y"].astype(int)
    df["p_over"] = df["p_over"].clip(0.0, 1.0)
    return df


def _load_slate_bucket_map(slate_date: date) -> pd.DataFrame:
    rows = pg_fetchall(SLATE_BUCKET_SQL, (slate_date.isoformat(),))
    m = pd.DataFrame(rows)
    if m.empty:
        return m
    m["player_id"] = pd.to_numeric(m["player_id"], errors="coerce").astype("Int64")
    m["game_id"] = pd.to_numeric(m["game_id"], errors="coerce").astype("Int64")
    m["expected_sog_bucket"] = m["expected_sog_bucket"].fillna("missing").astype(str)
    return m.dropna(subset=["player_id", "game_id"])


def _enforce_monotone(df: pd.DataFrame) -> None:
    need = ["p_over_1_5", "p_over_2_5", "p_over_3_5"]
    if not all(c in df.columns for c in need):
        return
    p15 = pd.to_numeric(df["p_over_1_5"], errors="coerce").to_numpy(dtype=float)
    p25 = pd.to_numeric(df["p_over_2_5"], errors="coerce").to_numpy(dtype=float)
    p35 = pd.to_numeric(df["p_over_3_5"], errors="coerce").to_numpy(dtype=float)
    p15 = np.clip(p15, 0.0, 1.0)
    p25 = np.clip(np.minimum(p25, p15), 0.0, 1.0)
    p35 = np.clip(np.minimum(p35, p25), 0.0, 1.0)
    df["p_over_1_5"] = p15
    df["p_over_2_5"] = p25
    df["p_over_3_5"] = p35


@dataclass
class FitPack:
    line_models: Dict[float, Optional[IsotonicRegression]]
    seg_models: Dict[Tuple[float, str], Optional[IsotonicRegression]]
    meta: Dict[str, Any]


def _fit_models(
    train_df: pd.DataFrame,
    lines: Sequence[float],
    asof: date,
    segment_min_rows: int,
    half_life_days: float,
) -> FitPack:
    line_models: Dict[float, Optional[IsotonicRegression]] = {}
    seg_models: Dict[Tuple[float, str], Optional[IsotonicRegression]] = {}
    meta: Dict[str, Any] = {"line": {}, "segment": {}}

    for line in lines:
        tline = train_df[train_df["line"] == float(line)].copy()
        if tline.empty:
            line_models[line] = None
            meta["line"][str(line)] = {"status": "missing", "rows": 0}
            continue
        w = _half_life_weights(tline["game_date"], asof, half_life_days)
        iso_line = _fit_iso(
            x=tline["p_over"].to_numpy(dtype=float),
            y=tline["y"].to_numpy(dtype=int),
            w=w,
        )
        line_models[line] = iso_line
        if iso_line is None:
            meta["line"][str(line)] = {"status": "identity", "rows": int(len(tline))}
        else:
            meta["line"][str(line)] = {"status": "ok", "rows": int(len(tline))}

        meta["segment"][str(line)] = {}
        for bucket, tseg in tline.groupby("expected_sog_bucket"):
            seg_key = str(bucket)
            if len(tseg) < int(segment_min_rows):
                seg_models[(line, seg_key)] = None
                meta["segment"][str(line)][seg_key] = {
                    "status": "fallback_line_iso",
                    "rows": int(len(tseg)),
                    "reason": "insufficient_rows",
                }
                continue
            ws = _half_life_weights(tseg["game_date"], asof, half_life_days)
            iso_seg = _fit_iso(
                x=tseg["p_over"].to_numpy(dtype=float),
                y=tseg["y"].to_numpy(dtype=int),
                w=ws,
            )
            seg_models[(line, seg_key)] = iso_seg
            if iso_seg is None:
                meta["segment"][str(line)][seg_key] = {
                    "status": "fallback_line_iso",
                    "rows": int(len(tseg)),
                    "reason": "single_class_or_invalid",
                }
            else:
                meta["segment"][str(line)][seg_key] = {
                    "status": "ok",
                    "rows": int(len(tseg)),
                }
    return FitPack(line_models=line_models, seg_models=seg_models, meta=meta)


def _apply_calibration(
    pred_df: pd.DataFrame,
    fit: FitPack,
    lines: Sequence[float],
    blend_alpha: float,
) -> Dict[str, Any]:
    blend_alpha = float(np.clip(blend_alpha, 0.0, 1.0))
    stats: Dict[str, Any] = {
        "line": {},
        "segment": {},
    }
    for line in lines:
        col = _prob_col(line)
        if col not in pred_df.columns:
            stats["line"][str(line)] = {"status": "missing_col"}
            continue
        raw = pd.to_numeric(pred_df[col], errors="coerce").to_numpy(dtype=float)
        raw = np.clip(raw, 0.0, 1.0)
        line_model = fit.line_models.get(line)
        if line_model is None:
            line_cal = raw.copy()
            stats["line"][str(line)] = {"status": "identity"}
        else:
            line_iso = line_model.predict(raw)
            line_cal = (1.0 - blend_alpha) * raw + blend_alpha * line_iso
            stats["line"][str(line)] = {"status": "ok", "blend_alpha": blend_alpha}

        seg_values = pred_df["expected_sog_bucket"].fillna("missing").astype(str).tolist()
        out = line_cal.copy()
        seg_stat = {"fallback_to_line": 0, "applied_segment_rows": 0}
        for bucket in sorted(set(seg_values)):
            mask = np.array([v == bucket for v in seg_values], dtype=bool)
            if not mask.any():
                continue
            seg_model = fit.seg_models.get((line, bucket))
            if seg_model is None:
                seg_stat["fallback_to_line"] += int(mask.sum())
                continue
            seg_iso = seg_model.predict(raw[mask])
            out[mask] = (1.0 - blend_alpha) * raw[mask] + blend_alpha * seg_iso
            seg_stat["applied_segment_rows"] += int(mask.sum())

        pred_df[col] = np.clip(out, 0.0, 1.0)
        stats["segment"][str(line)] = seg_stat

    _enforce_monotone(pred_df)
    return stats


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Apply segmented recency calibration to NHL SOG wide predictions.")
    ap.add_argument("--pred-csv", required=True, help="Input wide predictions CSV.")
    ap.add_argument("--out-csv", default="", help="Output CSV path (default: in-place).")
    ap.add_argument("--model-family", default="denali_blend")
    ap.add_argument("--model-version", default="phoenix_v2")
    ap.add_argument("--lines", default="1.5,2.5,3.5")
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--segment-min-rows", type=int, default=120)
    ap.add_argument("--blend-alpha", type=float, default=0.65)
    ap.add_argument("--decay-half-life-days", type=float, default=21.0)
    ap.add_argument("--asof-date", default="", help="Training as-of date YYYY-MM-DD (default auto).")
    ap.add_argument("--strict", action="store_true", help="Fail on missing bucket map / contract issues.")
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        _require_db()

        pred_path = Path(args.pred_csv)
        out_path = Path(args.out_csv) if args.out_csv else pred_path
        if not pred_path.exists():
            raise RuntimeError(f"pred-csv not found: {pred_path}")

        lines = _parse_lines(args.lines)
        pred_df = pd.read_csv(pred_path).copy()
        for need in ("player_id", "game_id", "game_date"):
            if need not in pred_df.columns:
                raise RuntimeError(f"pred-csv missing required column: {need}")
        pred_df["player_id"] = pd.to_numeric(pred_df["player_id"], errors="coerce").astype("Int64")
        pred_df["game_id"] = pd.to_numeric(pred_df["game_id"], errors="coerce").astype("Int64")
        pred_df["game_date"] = pd.to_datetime(pred_df["game_date"], errors="coerce").dt.date
        pred_df = pred_df.dropna(subset=["player_id", "game_id", "game_date"]).copy()
        if pred_df.empty:
            raise RuntimeError("pred-csv has no valid rows after coercion.")

        slate_dates = sorted(set(pred_df["game_date"].tolist()))
        if len(slate_dates) != 1:
            raise RuntimeError(f"pred-csv must contain exactly one slate date; got {slate_dates}")
        slate_date = slate_dates[0]

        if args.asof_date.strip():
            asof = date.fromisoformat(args.asof_date.strip())
        else:
            latest_truth = _latest_truth_date()
            asof = min(latest_truth, slate_date - timedelta(days=1))
        if asof < date(2000, 1, 1):
            raise RuntimeError("Resolved asof-date is invalid.")
        train_from = asof - timedelta(days=max(1, int(args.lookback_days)) - 1)

        train_df = _load_training(
            model_family=args.model_family,
            model_version=args.model_version,
            lines=lines,
            train_from=train_from,
            train_to=asof,
        )
        if train_df.empty:
            raise RuntimeError("No training rows found for requested training window.")

        bucket_map = _load_slate_bucket_map(slate_date)
        if bucket_map.empty:
            msg = f"No expected_sog_bucket map found for slate {slate_date}."
            if args.strict:
                raise RuntimeError(msg)
            pred_df["expected_sog_bucket"] = "missing"
            map_coverage = 0.0
        else:
            pred_df = pred_df.merge(bucket_map, on=["player_id", "game_id"], how="left")
            pred_df["expected_sog_bucket"] = pred_df["expected_sog_bucket"].fillna("missing").astype(str)
            matched = int((pred_df["expected_sog_bucket"] != "missing").sum())
            map_coverage = matched / max(1, len(pred_df))

        fit_pack = _fit_models(
            train_df=train_df,
            lines=lines,
            asof=asof,
            segment_min_rows=max(1, int(args.segment_min_rows)),
            half_life_days=float(args.decay_half_life_days),
        )
        apply_stats = _apply_calibration(
            pred_df=pred_df,
            fit=fit_pack,
            lines=lines,
            blend_alpha=float(args.blend_alpha),
        )

        pred_df.to_csv(out_path, index=False)

        summary = {
            "ok": True,
            "status": "pass",
            "input": str(pred_path),
            "output": str(out_path),
            "rows": int(len(pred_df)),
            "slate_date": slate_date.isoformat(),
            "asof_date": asof.isoformat(),
            "train_from": train_from.isoformat(),
            "train_to": asof.isoformat(),
            "model_family": args.model_family,
            "model_version": args.model_version,
            "lines": lines,
            "map_coverage": round(float(map_coverage), 4),
            "blend_alpha": float(args.blend_alpha),
            "segment_min_rows": int(args.segment_min_rows),
            "decay_half_life_days": float(args.decay_half_life_days),
            "fit_meta": fit_pack.meta,
            "apply_stats": apply_stats,
        }
        print(json.dumps(summary, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "status": "fail", "error": str(exc)}, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
