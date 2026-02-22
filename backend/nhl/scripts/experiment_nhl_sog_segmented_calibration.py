#!/usr/bin/env python3
"""Evaluate segmented recency calibration for NHL SOG probabilities."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss, roc_auc_score

from backend.shared.db.pg import pg_fetchall, pg_fetchone


BASE_SQL = """
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


def _to_date(s: str | None) -> str | None:
    if s is None:
        return None
    out = str(s).strip()
    date.fromisoformat(out)
    return out


def _latest_game_date(model_family: str, model_version: str, lines_csv: str) -> str:
    row = pg_fetchone(
        """
        SELECT MAX(g.game_date)::text AS to_date
        FROM nhl.predictions p
        JOIN nhl.games g ON g.game_id = p.game_id
        WHERE p.prop = 'shots_on_goal'
          AND p.model_family = %s
          AND p.model_version = %s
          AND p.line = ANY(string_to_array(%s, ',')::float8[])
        """,
        (model_family, model_version, lines_csv),
    ) or {}
    out = row.get("to_date")
    if not out:
        raise RuntimeError("No NHL SOG predictions found for selected model/lines.")
    return str(out)


@dataclass
class Window:
    from_date: str
    to_date: str
    holdout_from: str
    holdout_days: int


def _resolve_window(
    model_family: str,
    model_version: str,
    lines_csv: str,
    from_date_raw: str | None,
    to_date_raw: str | None,
    lookback_days: int,
    holdout_days: int,
) -> Window:
    to_date = _to_date(to_date_raw)
    from_date = _to_date(from_date_raw)
    if to_date is None:
        to_date = _latest_game_date(model_family, model_version, lines_csv)

    to_d = date.fromisoformat(to_date)
    if from_date is None:
        from_date = (to_d - timedelta(days=max(1, lookback_days - 1))).isoformat()
    from_d = date.fromisoformat(from_date)
    if from_d > to_d:
        raise ValueError("from-date must be <= to-date")

    hold = max(1, holdout_days)
    holdout_from = max(from_d, to_d - timedelta(days=hold - 1))
    return Window(
        from_date=from_d.isoformat(),
        to_date=to_d.isoformat(),
        holdout_from=holdout_from.isoformat(),
        holdout_days=hold,
    )


def _load_rows(
    model_family: str,
    model_version: str,
    lines_csv: str,
    w: Window,
) -> pd.DataFrame:
    rows = pg_fetchall(
        BASE_SQL,
        (model_family, model_version, lines_csv, w.from_date, w.to_date),
    )
    if not rows:
        raise RuntimeError("No rows returned for the selected configuration.")
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Empty dataframe returned for selected configuration.")

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df["p_over"] = pd.to_numeric(df["p_over"], errors="coerce")
    df["y"] = pd.to_numeric(df["y"], errors="coerce")
    df["expected_sog_bucket"] = df["expected_sog_bucket"].fillna("missing").astype(str)
    df = df.dropna(subset=["game_date", "line", "p_over", "y"]).copy()
    df["y"] = df["y"].astype(int)
    df["p_over"] = df["p_over"].clip(0.0, 1.0)
    return df


def _fit_iso(x: np.ndarray, y: np.ndarray, w: np.ndarray | None = None) -> IsotonicRegression | None:
    if x.size == 0:
        return None
    if np.unique(y).size < 2:
        return None
    iso = IsotonicRegression(out_of_bounds="clip")
    if w is None:
        iso.fit(x, y)
    else:
        iso.fit(x, y, sample_weight=w)
    return iso


def _half_life_weights(game_dates: pd.Series, to_date: date, half_life_days: float) -> np.ndarray:
    if half_life_days <= 0:
        return np.ones(len(game_dates), dtype=float)
    ages = np.array([(to_date - d).days for d in game_dates], dtype=float)
    ages = np.clip(ages, 0.0, None)
    return np.power(0.5, ages / float(half_life_days))


def _build_predictions(
    train: pd.DataFrame,
    holdout: pd.DataFrame,
    lines: Sequence[float],
    segment_min_rows: int,
    blend_alpha: float,
    half_life_days: float,
    train_to_date: date,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    blend_alpha = float(np.clip(blend_alpha, 0.0, 1.0))
    result = holdout.copy()
    result["p_raw"] = result["p_over"].astype(float).clip(0.0, 1.0)
    result["p_line_iso"] = result["p_raw"]
    result["p_segmented_iso"] = result["p_raw"]

    fit_meta: Dict[str, Any] = {"line": {}, "segment": {}}

    for line in lines:
        tline = train[train["line"] == float(line)].copy()
        hmask = result["line"] == float(line)
        if tline.empty or not hmask.any():
            fit_meta["line"][str(line)] = {"status": "missing_train_or_holdout", "rows": int(len(tline))}
            continue

        weights = _half_life_weights(
            game_dates=tline["game_date"],
            to_date=train_to_date,
            half_life_days=half_life_days,
        )
        iso_line = _fit_iso(
            x=tline["p_over"].to_numpy(dtype=float),
            y=tline["y"].to_numpy(dtype=int),
            w=weights,
        )
        if iso_line is None:
            fit_meta["line"][str(line)] = {"status": "identity", "rows": int(len(tline))}
            line_pred = result.loc[hmask, "p_raw"].to_numpy(dtype=float)
        else:
            raw = result.loc[hmask, "p_raw"].to_numpy(dtype=float)
            line_pred = (1.0 - blend_alpha) * raw + blend_alpha * iso_line.predict(raw)
            fit_meta["line"][str(line)] = {
                "status": "ok",
                "rows": int(len(tline)),
                "blend_alpha": blend_alpha,
                "half_life_days": float(half_life_days),
            }
        result.loc[hmask, "p_line_iso"] = np.clip(line_pred, 0.0, 1.0)

        fit_meta["segment"][str(line)] = {}
        for bucket, tseg in tline.groupby("expected_sog_bucket"):
            seg_key = str(bucket)
            if len(tseg) < int(segment_min_rows):
                fit_meta["segment"][str(line)][seg_key] = {
                    "status": "fallback_line_iso",
                    "rows": int(len(tseg)),
                    "reason": "insufficient_rows",
                }
                continue
            seg_weights = _half_life_weights(
                game_dates=tseg["game_date"],
                to_date=train_to_date,
                half_life_days=half_life_days,
            )
            iso_seg = _fit_iso(
                x=tseg["p_over"].to_numpy(dtype=float),
                y=tseg["y"].to_numpy(dtype=int),
                w=seg_weights,
            )
            if iso_seg is None:
                fit_meta["segment"][str(line)][seg_key] = {
                    "status": "fallback_line_iso",
                    "rows": int(len(tseg)),
                    "reason": "single_class_or_invalid",
                }
                continue

            seg_mask = hmask & (result["expected_sog_bucket"] == seg_key)
            if not seg_mask.any():
                fit_meta["segment"][str(line)][seg_key] = {
                    "status": "ok_train_only",
                    "rows": int(len(tseg)),
                    "holdout_rows": 0,
                }
                continue
            raw_seg = result.loc[seg_mask, "p_raw"].to_numpy(dtype=float)
            seg_pred = (1.0 - blend_alpha) * raw_seg + blend_alpha * iso_seg.predict(raw_seg)
            result.loc[seg_mask, "p_segmented_iso"] = np.clip(seg_pred, 0.0, 1.0)
            fit_meta["segment"][str(line)][seg_key] = {
                "status": "ok",
                "rows": int(len(tseg)),
                "holdout_rows": int(seg_mask.sum()),
                "blend_alpha": blend_alpha,
            }

    return result, fit_meta


def _metric_frame(df: pd.DataFrame, prob_col: str, group_cols: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if df.empty:
        return out
    for keys, sub in df.groupby(list(group_cols), dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        y = sub["y"].to_numpy(dtype=int)
        p = np.clip(sub[prob_col].to_numpy(dtype=float), 1e-6, 1 - 1e-6)
        row: Dict[str, Any] = {k: v for k, v in zip(group_cols, keys)}
        row["n"] = int(len(sub))
        row["avg_p"] = round(float(np.mean(p)), 4)
        row["hit_rate"] = round(float(np.mean(y)), 4)
        row["gap"] = round(row["avg_p"] - row["hit_rate"], 4)
        row["brier"] = round(float(np.mean((p - y) ** 2)), 4)
        try:
            row["logloss"] = round(float(log_loss(y, p, labels=[0, 1])), 4)
        except Exception:
            row["logloss"] = None
        if len(np.unique(y)) >= 2:
            try:
                row["auc"] = round(float(roc_auc_score(y, p)), 4)
            except Exception:
                row["auc"] = None
        else:
            row["auc"] = None
        out.append(row)
    return out


def _delta_vs_raw(by_method_line: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    raw_rows = {float(r["line"]): r for r in by_method_line.get("raw", [])}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for method, rows in by_method_line.items():
        if method == "raw":
            continue
        deltas: List[Dict[str, Any]] = []
        for r in rows:
            line = float(r["line"])
            raw = raw_rows.get(line)
            if not raw:
                continue
            deltas.append(
                {
                    "line": line,
                    "delta_brier_vs_raw": round(float(r["brier"]) - float(raw["brier"]), 4),
                    "delta_logloss_vs_raw": round(float(r["logloss"]) - float(raw["logloss"]), 4)
                    if r.get("logloss") is not None and raw.get("logloss") is not None
                    else None,
                    "delta_gap_vs_raw": round(float(r["gap"]) - float(raw["gap"]), 4),
                }
            )
        out[method] = sorted(deltas, key=lambda x: x["line"])
    return out


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Experiment NHL SOG segmented recency calibration.")
    ap.add_argument("--model-family", default="denali_blend")
    ap.add_argument("--model-version", default="phoenix_v2")
    ap.add_argument("--lines", default="1.5,2.5,3.5")
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--holdout-days", type=int, default=14)
    ap.add_argument("--segment-min-rows", type=int, default=120)
    ap.add_argument("--blend-alpha", type=float, default=0.65)
    ap.add_argument("--decay-half-life-days", type=float, default=21.0)
    ap.add_argument("--output", default="")
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        lines = _parse_lines(args.lines)
        lines_csv = ",".join(f"{x:g}" for x in lines)
        w = _resolve_window(
            model_family=args.model_family,
            model_version=args.model_version,
            lines_csv=lines_csv,
            from_date_raw=args.from_date,
            to_date_raw=args.to_date,
            lookback_days=max(1, int(args.lookback_days)),
            holdout_days=max(1, int(args.holdout_days)),
        )
        df = _load_rows(
            model_family=args.model_family,
            model_version=args.model_version,
            lines_csv=lines_csv,
            w=w,
        )
        holdout_from = date.fromisoformat(w.holdout_from)
        train = df[df["game_date"] < holdout_from].copy()
        holdout = df[df["game_date"] >= holdout_from].copy()
        if train.empty:
            raise RuntimeError("Training split is empty. Increase lookback window or reduce holdout-days.")
        if holdout.empty:
            raise RuntimeError("Holdout split is empty. Reduce holdout-days.")

        preds, fit_meta = _build_predictions(
            train=train,
            holdout=holdout,
            lines=lines,
            segment_min_rows=max(1, int(args.segment_min_rows)),
            blend_alpha=float(args.blend_alpha),
            half_life_days=float(args.decay_half_life_days),
            train_to_date=(holdout_from - timedelta(days=1)),
        )

        by_method_line = {
            "raw": _metric_frame(preds, "p_raw", ["line"]),
            "line_iso": _metric_frame(preds, "p_line_iso", ["line"]),
            "segmented_iso": _metric_frame(preds, "p_segmented_iso", ["line"]),
        }
        by_method_bucket = {
            "raw": _metric_frame(preds, "p_raw", ["line", "expected_sog_bucket"]),
            "line_iso": _metric_frame(preds, "p_line_iso", ["line", "expected_sog_bucket"]),
            "segmented_iso": _metric_frame(preds, "p_segmented_iso", ["line", "expected_sog_bucket"]),
        }

        payload: Dict[str, Any] = {
            "ok": True,
            "status": "pass",
            "config": {
                "model_family": args.model_family,
                "model_version": args.model_version,
                "lines": lines,
                "from_date": w.from_date,
                "to_date": w.to_date,
                "holdout_from": w.holdout_from,
                "holdout_days": int(args.holdout_days),
                "segment_min_rows": int(args.segment_min_rows),
                "blend_alpha": float(args.blend_alpha),
                "decay_half_life_days": float(args.decay_half_life_days),
            },
            "counts": {
                "all_rows": int(len(df)),
                "train_rows": int(len(train)),
                "holdout_rows": int(len(holdout)),
                "train_dates": int(train["game_date"].nunique()),
                "holdout_dates": int(holdout["game_date"].nunique()),
            },
            "fit_meta": fit_meta,
            "holdout_by_method_line": by_method_line,
            "holdout_deltas_vs_raw": _delta_vs_raw(by_method_line),
            "holdout_by_method_bucket": by_method_bucket,
        }

        rendered = json.dumps(payload, indent=2, default=str)
        print(rendered)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as fh:
                fh.write(rendered + "\n")
        return 0
    except Exception as exc:
        payload = {"ok": False, "status": "fail", "error": str(exc)}
        print(json.dumps(payload, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
