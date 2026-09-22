#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import warnings
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

from backend._legacy.scripts.recompute_mlb_training_predictions import _actual_side, _build_features, _score_probability
from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    HashedProposalAuthority,
    source_type_is_recognized,
)
from backend.shared.db.pg import pg_fetchall
import backend.mlb.prediction.make_prediction as mp


# Keep scorer output readable for long cohort runs.
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn.impute._base")


BASE_SQL = """
SELECT
  m.id,
  m.game_date::date AS game_date,
  m.player_id,
  m.player_name,
  m.game_id,
  m.prop_type,
  m.line,
  m.prop_value,
  m.over_under,
  m.outcome,
  m.prop_source,
  m.team,
  m.team_id,
  m.opponent,
  m.opponent_team_id,
  m.is_home,
  m.game_day_of_week,
  m.time_of_day_bucket,
  to_jsonb(m)->>'game_type' AS legacy_source_game_type,
  row_to_json(pds)::jsonb AS pds_stats
FROM mlb.model_training_props m
LEFT JOIN mlb.player_derived_stats pds
  ON pds.player_id = m.player_id
 AND pds.game_id = m.game_id
WHERE lower(trim(m.prop_type)) = lower(trim(%s))
  AND m.prop_source = %s
  AND lower(trim(m.outcome)) IN ('win','loss')
  AND (m.team IS NULL OR m.team = '' OR m.team ~ '^[0-9]+$')
  AND m.game_date::date >= %s::date
  AND m.game_date::date <= %s::date
ORDER BY m.game_date DESC, m.id DESC
"""


PHASE_GATE_DECISIONS = (
    "ADMITTED_REGULAR_SEASON",
    "EXCLUDED_PRESEASON",
    "EXCLUDED_POSTSEASON",
    "BLOCKED_MISSING_GAME_PK",
    "BLOCKED_ABSENT_AUTHORITY",
    "BLOCKED_SPECIAL_TYPE",
    "BLOCKED_UNKNOWN_TYPE",
    "BLOCKED_CONFLICTING_TYPE",
    "BLOCKED_STALE_AUTHORITY",
)


def _decile_bucket(p: float) -> str:
    if p >= 1.0:
        return "0.9-1.0"
    lo = math.floor(p * 10.0) / 10.0
    hi = lo + 0.1
    return f"{lo:.1f}-{hi:.1f}"


def _clear_prediction_caches() -> None:
    for name in ("_load_model_cached", "_load_artifact_meta", "_input_columns_for"):
        fn = getattr(mp, name, None)
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


def _fetch_rows(
    *,
    prop_type: str,
    prop_source: str,
    from_date: str,
    to_date: str,
) -> pd.DataFrame:
    # Pull in small chunks to avoid statement_timeout on long ranges.
    start = datetime.fromisoformat(str(from_date)).date()
    end = datetime.fromisoformat(str(to_date)).date()
    rows: List[Dict[str, Any]] = []
    cur = start
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=30))
        part = pg_fetchall(
            BASE_SQL,
            (
                prop_type,
                prop_source,
                cur.isoformat(),
                chunk_end.isoformat(),
            ),
        )
        if part:
            rows.extend(part)
        cur = chunk_end + timedelta(days=1)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"], keep="first")
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df


def _legacy_regular_season_admission(raw_type: Any) -> bool:
    """Reproduce the superseded SQL predicate for bounded comparison only."""

    if raw_type is None or (not isinstance(raw_type, str) and pd.isna(raw_type)):
        value = "R"
    else:
        value = str(raw_type).strip().upper() or "R"
    return value == "R"


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _phase_gate_decision(
    row: Dict[str, Any],
    authority: CanonicalGamePhaseAuthority,
) -> tuple[str, int | None]:
    game_pk_value = row.get("game_id")
    try:
        record = authority.lookup_exact(game_pk_value)
    except GamePhaseAuthorityError as exc:
        error_decisions = {
            "GAME_PHASE_GAME_PK_MISSING": "BLOCKED_MISSING_GAME_PK",
            "GAME_PHASE_ABSENT": "BLOCKED_ABSENT_AUTHORITY",
            "GAME_PHASE_SPECIAL_EXCLUDED": "BLOCKED_SPECIAL_TYPE",
            "GAME_PHASE_CONFLICT_BLOCKED": "BLOCKED_CONFLICTING_TYPE",
            "GAME_PHASE_CORRECTION_REVIEW_REQUIRED": "BLOCKED_CONFLICTING_TYPE",
            "GAME_PHASE_AUTHORITY_STALE": "BLOCKED_STALE_AUTHORITY",
        }
        return error_decisions.get(exc.code, "BLOCKED_UNKNOWN_TYPE"), exc.game_pk

    raw_type = row.get("legacy_source_game_type")
    raw_missing = raw_type is None or (
        not isinstance(raw_type, str) and pd.isna(raw_type)
    ) or (isinstance(raw_type, str) and raw_type == "")
    if not raw_missing:
        if not source_type_is_recognized(raw_type):
            return "BLOCKED_UNKNOWN_TYPE", record.game_pk
        if raw_type != record.source_game_type:
            return "BLOCKED_CONFLICTING_TYPE", record.game_pk

    if record.season_phase == "REGULAR_SEASON":
        return "ADMITTED_REGULAR_SEASON", record.game_pk
    if record.season_phase == "PRESEASON":
        return "EXCLUDED_PRESEASON", record.game_pk
    if record.season_phase == "POSTSEASON":
        return "EXCLUDED_POSTSEASON", record.game_pk
    return "BLOCKED_UNKNOWN_TYPE", record.game_pk


def _apply_regular_season_authority(
    df_rows: pd.DataFrame,
    *,
    authority: CanonicalGamePhaseAuthority,
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """Apply exact-gamePk positive membership before model scoring.

    Missing/unknown/conflicting authority is recorded separately from expected
    preseason/postseason exclusions.  The caller must abort when the report's
    blocked row count is non-zero.
    """

    decisions: Counter[str] = Counter()
    game_pks_by_decision: defaultdict[str, set[int]] = defaultdict(set)
    admitted_indexes: List[Any] = []
    legacy_admitted_game_pks: set[int] = set()
    newly_excluded_or_blocked_game_pks: set[int] = set()

    for row_index, row in df_rows.iterrows():
        row_dict = row.to_dict()
        decision, game_pk = _phase_gate_decision(row_dict, authority)
        decisions[decision] += 1
        if game_pk is not None:
            game_pks_by_decision[decision].add(int(game_pk))
        legacy_admitted = _legacy_regular_season_admission(
            row_dict.get("legacy_source_game_type")
        )
        if legacy_admitted and game_pk is not None:
            legacy_admitted_game_pks.add(int(game_pk))
        if decision == "ADMITTED_REGULAR_SEASON":
            admitted_indexes.append(row_index)
        elif legacy_admitted and game_pk is not None:
            newly_excluded_or_blocked_game_pks.add(int(game_pk))

    decision_row_counts = {
        decision: int(decisions.get(decision, 0)) for decision in PHASE_GATE_DECISIONS
    }
    decision_game_pks = {
        decision: sorted(game_pks_by_decision.get(decision, set()))
        for decision in PHASE_GATE_DECISIONS
    }
    blocked_row_count = sum(
        count for decision, count in decision_row_counts.items() if decision.startswith("BLOCKED_")
    )
    excluded_row_count = sum(
        count for decision, count in decision_row_counts.items() if decision.startswith("EXCLUDED_")
    )
    valid_input_game_pks = sorted(
        {
            int(value)
            for value in df_rows.get("game_id", pd.Series(dtype="object")).tolist()
            if value is not None
            and not (not isinstance(value, str) and pd.isna(value))
            and str(value).strip()
        }
    )
    row_identities = [
        {
            "id": str(row.get("id")),
            "game_pk": None
            if row.get("game_id") is None
            or (not isinstance(row.get("game_id"), str) and pd.isna(row.get("game_id")))
            else int(row.get("game_id")),
        }
        for row in df_rows.to_dict(orient="records")
    ]
    admitted = df_rows.loc[admitted_indexes].copy()
    report = {
        "authority": authority.metadata.to_dict(),
        "gate": "POSITIVE_REGULAR_SEASON_MEMBERSHIP",
        "input_row_count": int(len(df_rows)),
        "input_distinct_game_pk_count": len(valid_input_game_pks),
        "input_game_pks": valid_input_game_pks,
        "input_identity_sha256": _canonical_hash(row_identities),
        "legacy_admitted_distinct_game_pk_count": len(legacy_admitted_game_pks),
        "admitted_row_count": int(len(admitted)),
        "admitted_distinct_game_pk_count": len(
            game_pks_by_decision.get("ADMITTED_REGULAR_SEASON", set())
        ),
        "excluded_row_count": int(excluded_row_count),
        "blocked_row_count": int(blocked_row_count),
        "decision_row_counts": decision_row_counts,
        "decision_game_pks": decision_game_pks,
        "newly_excluded_or_blocked_game_pks": sorted(
            newly_excluded_or_blocked_game_pks
        ),
    }
    return admitted, report


def _score_all_rows_for_model(
    *,
    df_rows: pd.DataFrame,
    model_root: Path,
    prop_type: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    os.environ["MODEL_DIR"] = str(model_root)
    _clear_prediction_caches()

    scored: List[Dict[str, Any]] = []
    failures = 0
    failure_samples: List[Dict[str, Any]] = []

    rows = df_rows.to_dict(orient="records")
    for i, row in enumerate(rows, start=1):
        try:
            features = _build_features(row)
            p_over, model_threshold = _score_probability(
                prop_type=prop_type,
                features=features,
                allow_heuristic=False,
            )
            actual_side = _actual_side(str(row.get("over_under") or ""), str(row.get("outcome") or ""))
            if actual_side not in {"over", "under"}:
                continue
            predicted_side = "over" if float(p_over) >= float(model_threshold) else "under"
            scored.append(
                {
                    "id": row.get("id"),
                    "game_date": row.get("game_date"),
                    "p_over": float(p_over),
                    "model_threshold": float(model_threshold),
                    "predicted_side": predicted_side,
                    "actual_side": actual_side,
                    "y_true": 1 if actual_side == "over" else 0,
                }
            )
        except Exception as e:
            failures += 1
            if len(failure_samples) < 8:
                failure_samples.append(
                    {
                        "id": row.get("id"),
                        "game_date": str(row.get("game_date")),
                        "error": f"{type(e).__name__}: {e}",
                    }
                )
        if i % 5000 == 0:
            print(f"[score] model_root={model_root} processed={i}/{len(rows)}", flush=True)

    df_scored = pd.DataFrame(scored)
    meta = {
        "model_root": str(model_root),
        "rows_input": int(len(rows)),
        "rows_scored": int(len(df_scored)),
        "score_failures": int(failures),
        "failure_samples": failure_samples,
    }
    return df_scored, meta


def _metrics_for_slice(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "attempted": 0,
            "scored": 0,
            "correct": 0,
            "accuracy_pct": None,
            "auc_p_over": None,
            "brier_score": None,
            "log_loss": None,
            "pred_over_pct": None,
            "actual_over_pct": None,
            "false_over": 0,
            "false_under": 0,
            "calibration_decile_error_pp": None,
        }

    p = df["p_over"].astype(float).clip(0.0, 1.0)
    y = df["y_true"].astype(int)
    pred_over = (df["predicted_side"] == "over").astype(int)
    correct = int((pred_over == y).sum())
    scored = int(len(df))
    false_over = int(((pred_over == 1) & (y == 0)).sum())
    false_under = int(((pred_over == 0) & (y == 1)).sum())

    auc = None
    if y.nunique() > 1:
        auc = float(roc_auc_score(y, p))
    brier = float(brier_score_loss(y, p))
    ll = float(log_loss(y, p, labels=[0, 1]))

    # Weighted decile calibration absolute error in percentage points.
    d = df.copy()
    d["bucket"] = p.map(_decile_bucket)
    dec_err = []
    for _, g in d.groupby("bucket", sort=True):
        avg_p = float(g["p_over"].mean()) * 100.0
        act_o = float((g["y_true"] == 1).mean()) * 100.0
        dec_err.append((len(g), abs(avg_p - act_o)))
    calibration_mae = None
    if dec_err:
        tot = float(sum(n for n, _ in dec_err))
        calibration_mae = float(sum(n * e for n, e in dec_err) / tot) if tot > 0 else None

    return {
        "attempted": scored,
        "scored": scored,
        "correct": correct,
        "accuracy_pct": round(100.0 * correct / scored, 4),
        "auc_p_over": round(auc, 6) if auc is not None else None,
        "brier_score": round(brier, 6),
        "log_loss": round(ll, 6),
        "pred_over_pct": round(100.0 * float((pred_over == 1).mean()), 4),
        "actual_over_pct": round(100.0 * float((y == 1).mean()), 4),
        "false_over": false_over,
        "false_under": false_under,
        "calibration_decile_error_pp": round(calibration_mae, 6) if calibration_mae is not None else None,
    }


def _deciles_for_slice(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "bucket",
                "rows",
                "avg_p_over",
                "actual_over_rate_pct",
                "bucket_accuracy_pct",
                "decile_abs_error_pp",
            ]
        )
    out_rows: List[Dict[str, Any]] = []
    for bucket, g in df.groupby(df["p_over"].astype(float).map(_decile_bucket), sort=True):
        avg_p = float(g["p_over"].mean()) * 100.0
        act_o = float((g["y_true"] == 1).mean()) * 100.0
        acc = float((g["predicted_side"] == g["actual_side"]).mean()) * 100.0
        out_rows.append(
            {
                "bucket": bucket,
                "rows": int(len(g)),
                "avg_p_over": round(avg_p / 100.0, 6),
                "actual_over_rate_pct": round(act_o, 4),
                "bucket_accuracy_pct": round(acc, 4),
                "decile_abs_error_pp": round(abs(avg_p - act_o), 4),
            }
        )
    return pd.DataFrame(out_rows)


def _threshold_table_for_slice(df: pd.DataFrame) -> pd.DataFrame:
    thresholds = [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]
    rows: List[Dict[str, Any]] = []
    if df.empty:
        return pd.DataFrame(rows)
    y = df["y_true"].astype(int)
    p = df["p_over"].astype(float)
    scored = int(len(df))
    for t in thresholds:
        pred = (p >= float(t)).astype(int)
        correct = int((pred == y).sum())
        false_over = int(((pred == 1) & (y == 0)).sum())
        false_under = int(((pred == 0) & (y == 1)).sum())
        rows.append(
            {
                "threshold": round(float(t), 2),
                "accuracy_pct": round(100.0 * correct / scored, 4),
                "pred_over_pct": round(100.0 * float((pred == 1).mean()), 4),
                "false_over": false_over,
                "false_under": false_under,
            }
        )
    return pd.DataFrame(rows)


def _cohort_slices(df_scored_all: pd.DataFrame, *, gate_from: str, gate_to: str, monthly_from: str | None) -> List[Tuple[str, str, pd.DataFrame]]:
    out: List[Tuple[str, str, pd.DataFrame]] = []

    gate_from_ts = pd.Timestamp(gate_from)
    gate_to_ts = pd.Timestamp(gate_to)
    gate_df = df_scored_all[(df_scored_all["game_date"] >= gate_from_ts) & (df_scored_all["game_date"] <= gate_to_ts)].copy()
    out.append(("fixed_gate", f"{gate_from}_to_{gate_to}", gate_df))

    if monthly_from:
        monthly_start = pd.Timestamp(monthly_from)
        monthly_df = df_scored_all[(df_scored_all["game_date"] >= monthly_start) & (df_scored_all["game_date"] <= gate_to_ts)].copy()
        if not monthly_df.empty:
            monthly_df["cohort_month"] = monthly_df["game_date"].dt.to_period("M").astype(str)
            for month, g in monthly_df.groupby("cohort_month", sort=True):
                out.append(("monthly", str(month), g.copy()))
    return out


def _parse_candidates(items: Iterable[str], *, prop_type: str) -> List[Tuple[str, Path]]:
    out: List[Tuple[str, Path]] = []
    prop_key = str(prop_type or "").strip().lower()
    artifact_name = f"{prop_key}.joblib"
    for raw in items:
        s = str(raw or "").strip()
        if not s or "=" not in s:
            raise ValueError(f"invalid --candidate '{raw}', expected name=/path/to/model_root")
        name, path = s.split("=", 1)
        name = name.strip()
        root = Path(path.strip()).expanduser().resolve()
        artifact_path = root / "latest" / artifact_name
        if not artifact_path.exists():
            raise FileNotFoundError(f"candidate '{name}' missing artifact: {artifact_path}")
        out.append((name, root))
    if not out:
        raise ValueError("at least one --candidate is required")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate multiple hits model candidates on fixed/monthly cohorts.")
    ap.add_argument("--prop-type", default="hits")
    ap.add_argument("--prop-source", default="mlb_api")
    ap.add_argument("--gate-from", default="2026-03-23")
    ap.add_argument("--gate-to", default="2026-04-22")
    ap.add_argument("--monthly-from", default="2025-03-01")
    ap.add_argument("--require-regular-season", action="store_true")
    ap.add_argument("--candidate", action="append", default=[])
    ap.add_argument("--out-dir", default="tmp/analysis/hits_model_compare")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    prop_key = str(args.prop_type).strip().lower()
    candidates = _parse_candidates(args.candidate, prop_type=prop_key)
    print(json.dumps({"candidates": [{"name": n, "root": str(p)} for n, p in candidates]}, indent=2))

    # Fetch once across all requested cohorts.
    fetch_from = args.monthly_from or args.gate_from
    authority: CanonicalGamePhaseAuthority | None = None
    if args.require_regular_season:
        authority = HashedProposalAuthority()
        authority.require_supported_window(fetch_from, args.gate_to)

    df_fetched = _fetch_rows(
        prop_type=str(args.prop_type),
        prop_source=str(args.prop_source),
        from_date=str(fetch_from),
        to_date=str(args.gate_to),
    )
    if df_fetched.empty:
        raise RuntimeError("no rows fetched for requested cohort range")

    phase_gate_report: Dict[str, Any] | None = None
    if authority is not None:
        df_rows, phase_gate_report = _apply_regular_season_authority(
            df_fetched,
            authority=authority,
        )
        phase_gate_path = out_dir / "phase_gate_report.json"
        phase_gate_path.write_text(
            json.dumps(phase_gate_report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if int(phase_gate_report["blocked_row_count"]) > 0:
            raise RuntimeError(
                "canonical game-phase authority blocked evaluation; "
                f"see {phase_gate_path}"
            )
    else:
        df_rows = df_fetched

    if df_rows.empty:
        raise RuntimeError("no rows admitted for requested cohort range")

    leaderboard_rows: List[Dict[str, Any]] = []
    manifest: Dict[str, Any] = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "prop_type": args.prop_type,
        "prop_source": args.prop_source,
        "gate_window": {"from": args.gate_from, "to": args.gate_to},
        "monthly_from": args.monthly_from,
        "require_regular_season": bool(args.require_regular_season),
        "rows_fetched_before_phase_gate": int(len(df_fetched)),
        "rows_fetched": int(len(df_rows)),
        "phase_gate": phase_gate_report,
        "candidates": [],
        "files": {},
    }

    for model_name, model_root in candidates:
        print(f"[eval] scoring model={model_name} root={model_root}", flush=True)
        df_scored, model_meta = _score_all_rows_for_model(
            df_rows=df_rows,
            model_root=model_root,
            prop_type=str(args.prop_type),
        )

        scored_csv = out_dir / f"scored_{model_name}.csv"
        df_scored.to_csv(scored_csv, index=False)

        candidate_manifest: Dict[str, Any] = {
            "name": model_name,
            "model_root": str(model_root),
            "artifact_path": str((model_root / "latest" / f"{prop_key}.joblib").resolve()),
            "score_meta": model_meta,
            "decile_files": {},
            "threshold_files": {},
        }

        for cohort_type, cohort_label, cohort_df in _cohort_slices(
            df_scored,
            gate_from=str(args.gate_from),
            gate_to=str(args.gate_to),
            monthly_from=str(args.monthly_from) if args.monthly_from else None,
        ):
            m = _metrics_for_slice(cohort_df)
            leaderboard_rows.append(
                {
                    "model_name": model_name,
                    "model_root": str(model_root),
                    "cohort_type": cohort_type,
                    "cohort_label": cohort_label,
                    **m,
                }
            )

            dec = _deciles_for_slice(cohort_df)
            thr = _threshold_table_for_slice(cohort_df)

            safe_label = cohort_label.replace("-", "_").replace(":", "_")
            dec_path = out_dir / f"deciles_{model_name}_{cohort_type}_{safe_label}.csv"
            thr_path = out_dir / f"thresholds_{model_name}_{cohort_type}_{safe_label}.csv"
            dec.to_csv(dec_path, index=False)
            thr.to_csv(thr_path, index=False)
            candidate_manifest["decile_files"][f"{cohort_type}:{cohort_label}"] = str(dec_path)
            candidate_manifest["threshold_files"][f"{cohort_type}:{cohort_label}"] = str(thr_path)

        manifest["candidates"].append(candidate_manifest)

    leaderboard = pd.DataFrame(leaderboard_rows)
    leaderboard_path = out_dir / "leaderboard.csv"
    leaderboard.to_csv(leaderboard_path, index=False)
    manifest["files"]["leaderboard_csv"] = str(leaderboard_path)

    # Fixed-window ranking snapshot (higher accuracy, higher AUC, lower proper-loss/calibration error).
    fixed = leaderboard[leaderboard["cohort_type"] == "fixed_gate"].copy()
    if not fixed.empty:
        fixed["rank_accuracy"] = fixed["accuracy_pct"].rank(ascending=False, method="min")
        fixed["rank_auc"] = fixed["auc_p_over"].rank(ascending=False, method="min")
        fixed["rank_brier"] = fixed["brier_score"].rank(ascending=True, method="min")
        fixed["rank_logloss"] = fixed["log_loss"].rank(ascending=True, method="min")
        fixed["rank_calib"] = fixed["calibration_decile_error_pp"].rank(ascending=True, method="min")
        fixed["rank_sum"] = fixed[["rank_accuracy", "rank_auc", "rank_brier", "rank_logloss", "rank_calib"]].sum(axis=1)
        fixed = fixed.sort_values(["rank_sum", "accuracy_pct"], ascending=[True, False])
        fixed_rank_path = out_dir / "leaderboard_fixed_ranked.csv"
        fixed.to_csv(fixed_rank_path, index=False)
        manifest["files"]["leaderboard_fixed_ranked_csv"] = str(fixed_rank_path)
        manifest["best_fixed_model"] = str(fixed.iloc[0]["model_name"])

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"wrote {leaderboard_path}")
    if "leaderboard_fixed_ranked_csv" in manifest["files"]:
        print(f"wrote {manifest['files']['leaderboard_fixed_ranked_csv']}")
    print(f"wrote {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
