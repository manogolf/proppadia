#!/usr/bin/env python3
"""Recompute historical MLB training predictions with current feature hydration."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Sequence

from backend.shared.db.pg import pg_connect, pg_fetchall, pg_fetchone


def _n(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _actual_side(over_under: str, outcome: str) -> str | None:
    o = str(over_under or "").strip().lower()
    r = str(outcome or "").strip().lower()
    if o not in {"over", "under"}:
        return None
    if r == "win":
        return o
    if r == "loss":
        return "under" if o == "over" else "over"
    return None


def _heuristic_probability(features: Dict[str, Any]) -> float:
    line_diff = _n(features.get("line_diff")) or 0.0
    hit_streak = _n(features.get("hit_streak")) or 0.0
    win_streak = _n(features.get("win_streak")) or 0.0
    is_home = 1.0 if bool(features.get("is_home")) else 0.0
    z = 0.0
    z += 0.9 * math.tanh(line_diff)
    z += 0.07 * max(-8.0, min(8.0, hit_streak))
    z += 0.05 * max(-8.0, min(8.0, win_streak))
    z += 0.10 * (1.0 if is_home else -1.0)
    p = 1.0 / (1.0 + math.exp(-z))
    return max(0.0, min(1.0, p))


def _score_probability(prop_type: str, features: Dict[str, Any], allow_heuristic: bool) -> float:
    try:
        from backend.mlb.prediction.make_prediction import predict as model_predict

        out = model_predict(prop_type=prop_type, features=features)
        p = _n(out.get("probability_over"))
        if p is None:
            p = _n(out.get("probability"))
        if p is not None:
            return max(0.0, min(1.0, float(p)))
        raise RuntimeError("model returned no probability")
    except Exception as e:
        if not allow_heuristic:
            raise RuntimeError(f"model scoring unavailable for {prop_type}: {e}") from e
    return _heuristic_probability(features)


def _window_dates(from_date: str | None, to_date: str | None, days_back: int) -> tuple[str, str]:
    if from_date and to_date:
        return from_date, to_date
    max_day_row = pg_fetchone("SELECT MAX(game_date)::date AS d FROM model_training_props WHERE game_date IS NOT NULL") or {}
    max_day = str(max_day_row.get("d") or "")
    if not max_day:
        raise RuntimeError("unable to determine max game_date from model_training_props")
    end = datetime.fromisoformat(max_day).date()
    start = end - timedelta(days=max(1, int(days_back)))
    if from_date:
        start = datetime.fromisoformat(from_date).date()
    if to_date:
        end = datetime.fromisoformat(to_date).date()
    return start.isoformat(), end.isoformat()


def _fetch_rows(
    from_date: str,
    to_date: str,
    prop_types: Sequence[str],
    prop_source: str,
    limit: int,
) -> List[Dict[str, Any]]:
    placeholders = ", ".join(["%s"] * len(prop_types))
    limit_sql = "LIMIT %s" if int(limit or 0) > 0 else ""
    params: List[Any] = [str(prop_source), str(from_date), str(to_date), *[str(p) for p in prop_types]]
    if int(limit or 0) > 0:
        params.append(int(limit))

    rows = pg_fetchall(
        f"""
SELECT
  m.id,
  m.player_id,
  m.game_id,
  m.game_date::date AS game_date,
  m.prop_type,
  m.prop_value,
  m.over_under,
  m.outcome,
  m.team,
  m.team_id,
  m.opponent,
  m.opponent_team_id,
  m.is_home,
  m.game_day_of_week,
  m.time_of_day_bucket,
  row_to_json(pds)::jsonb AS pds_stats
FROM model_training_props m
LEFT JOIN player_derived_stats pds
  ON pds.player_id = m.player_id
 AND pds.game_id = m.game_id
WHERE m.prop_source = %s
  AND lower(trim(m.outcome)) IN ('win','loss')
  AND m.game_date::date >= %s::date
  AND m.game_date::date <= %s::date
  AND m.prop_type IN ({placeholders})
ORDER BY m.game_date, m.id
{limit_sql}
""",
        tuple(params),
    )
    return list(rows or [])


def _build_features(row: Dict[str, Any]) -> Dict[str, Any]:
    prop_type = str(row.get("prop_type") or "").strip()
    prop_value = _n(row.get("prop_value")) or 0.0
    pds = row.get("pds_stats") if isinstance(row.get("pds_stats"), dict) else {}
    features: Dict[str, Any] = {
        "player_id": int(row.get("player_id")),
        "team_id": row.get("team_id"),
        "team": row.get("team"),
        "game_id": row.get("game_id"),
        "game_date": str(row.get("game_date")),
        "prop_type": prop_type,
        "prop_value": prop_value,
        "over_under": row.get("over_under"),
        "is_home": bool(row.get("is_home")),
        "opponent": row.get("opponent"),
        "opponent_team_id": row.get("opponent_team_id"),
        "game_day_of_week": row.get("game_day_of_week"),
        "time_of_day_bucket": row.get("time_of_day_bucket"),
    }
    for k, v in pds.items():
        if not isinstance(k, str):
            continue
        if not (k.startswith("d7_") or k.startswith("d15_") or k.startswith("d30_")):
            continue
        n = _n(v)
        if n is not None:
            features[k] = n

    # Derive combo rolling stats when direct lane columns are missing.
    if prop_type == "runs_rbis":
        for window in ("d7", "d15", "d30"):
            key = f"{window}_{prop_type}"
            if _n(features.get(key)) is None:
                runs_val = _n(features.get(f"{window}_runs_scored"))
                rbis_val = _n(features.get(f"{window}_rbis"))
                if runs_val is not None and rbis_val is not None:
                    features[key] = runs_val + rbis_val
    elif prop_type == "hits_runs_rbis":
        for window in ("d7", "d15", "d30"):
            key = f"{window}_{prop_type}"
            if _n(features.get(key)) is None:
                hits_val = _n(features.get(f"{window}_hits"))
                runs_val = _n(features.get(f"{window}_runs_scored"))
                rbis_val = _n(features.get(f"{window}_rbis"))
                if hits_val is not None and runs_val is not None and rbis_val is not None:
                    features[key] = hits_val + runs_val + rbis_val

    d7 = _n(features.get(f"d7_{prop_type}"))
    if d7 is not None:
        features["rolling_result_avg_7"] = d7
        features["line_diff"] = d7 - prop_value
    return features


def recompute(
    *,
    from_date: str,
    to_date: str,
    prop_types: Sequence[str],
    prop_source: str,
    limit: int,
    allow_heuristic: bool,
    gate_min_total_per_prop: int,
    gate_min_accuracy_pct: float,
) -> Dict[str, Any]:
    rows = _fetch_rows(from_date, to_date, prop_types, prop_source, limit)
    attempted = len(rows)
    updated = 0
    skipped_by_gate = 0
    failures = 0
    by_prop: Dict[str, Dict[str, int]] = {}
    error_samples: List[Dict[str, Any]] = []
    max_error_samples = 12
    now_ts = datetime.now(timezone.utc).isoformat()
    precomputed_scores: Dict[str, Dict[str, Any]] = {}
    scoring_failures_by_prop: Dict[str, int] = {}
    gate_enabled = float(gate_min_accuracy_pct) >= 0.0
    gate_by_prop: Dict[str, Dict[str, Any]] = {}
    blocked_props: List[str] = []
    blocked_reasons: Dict[str, str] = {}

    # Phase 1: score all candidate rows first (no writes yet).
    for row in rows:
        prop_type = str(row.get("prop_type") or "")
        gate_bucket = gate_by_prop.setdefault(
            prop_type,
            {"attempted": 0, "scored": 0, "correct": 0, "accuracy_pct": None, "blocked": False},
        )
        gate_bucket["attempted"] += 1
        try:
            features = _build_features(row)
            p_over = _score_probability(
                prop_type=prop_type,
                features=features,
                allow_heuristic=allow_heuristic,
            )
            predicted = "over" if p_over >= 0.5 else "under"
            actual = _actual_side(str(row.get("over_under") or ""), str(row.get("outcome") or ""))
            was_correct = (predicted == actual) if actual in {"over", "under"} else None
            gate_bucket["scored"] += 1
            if was_correct is True:
                gate_bucket["correct"] += 1
            precomputed_scores[str(row.get("id"))] = {
                "p_over": p_over,
                "predicted": predicted,
                "was_correct": was_correct,
            }
        except Exception:
            failures += 1
            scoring_failures_by_prop[prop_type] = int(scoring_failures_by_prop.get(prop_type, 0)) + 1
            if len(error_samples) < max_error_samples:
                try:
                    import traceback

                    err_msg = traceback.format_exc(limit=2).strip()
                except Exception:
                    err_msg = "unknown_error"
                error_samples.append(
                    {
                        "id": str(row.get("id")) if row.get("id") is not None else None,
                        "prop_type": prop_type,
                        "player_id": str(row.get("player_id")) if row.get("player_id") is not None else None,
                        "game_id": str(row.get("game_id")) if row.get("game_id") is not None else None,
                        "error": err_msg,
                    }
                )

    # Gate decisions: quality threshold + scoring errors.
    for prop_type, gate_bucket in gate_by_prop.items():
        scored = int(gate_bucket.get("scored") or 0)
        correct = int(gate_bucket.get("correct") or 0)
        if scored > 0:
            gate_bucket["accuracy_pct"] = round((100.0 * correct) / scored, 2)
        if gate_enabled and scored >= int(gate_min_total_per_prop) and gate_bucket.get("accuracy_pct") is not None:
            if float(gate_bucket["accuracy_pct"]) < float(gate_min_accuracy_pct):
                gate_bucket["blocked"] = True
                blocked_props.append(prop_type)
                blocked_reasons[prop_type] = "gate_accuracy"
        if int(scoring_failures_by_prop.get(prop_type, 0)) > 0:
            gate_bucket["blocked"] = True
            if prop_type not in blocked_props:
                blocked_props.append(prop_type)
            blocked_reasons[prop_type] = "scoring_errors"

    # Phase 2: apply updates only for non-blocked props.
    with pg_connect() as conn, conn.cursor() as cur:
        for row in rows:
            prop_type = str(row.get("prop_type") or "")
            bucket = by_prop.setdefault(prop_type, {"attempted": 0, "updated": 0, "failed": 0, "skipped_by_gate": 0})
            bucket["attempted"] += 1
            if prop_type in blocked_props:
                skipped_by_gate += 1
                bucket["skipped_by_gate"] += 1
                continue
            try:
                score = precomputed_scores.get(str(row.get("id")))
                if score is None:
                    raise RuntimeError("missing precomputed score for row")
                p_over = float(score.get("p_over"))
                predicted = str(score.get("predicted") or "")
                was_correct = score.get("was_correct")
                cur.execute(
                    """
UPDATE model_training_props
SET
  predicted_outcome = %s,
  confidence_score = %s,
  was_correct = %s,
  prediction_timestamp = %s
WHERE id = %s
""",
                    (
                        predicted,
                        p_over,
                        was_correct,
                        now_ts,
                        row.get("id"),
                    ),
                )
                updated += 1
                bucket["updated"] += 1
            except Exception:
                failures += 1
                bucket["failed"] += 1
        conn.commit()

    ok = (failures == 0) and (not blocked_props)
    return {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "from_date": from_date,
        "to_date": to_date,
        "prop_source": prop_source,
        "allow_heuristic": bool(allow_heuristic),
        "gate_enabled": gate_enabled,
        "gate_min_total_per_prop": int(gate_min_total_per_prop),
        "gate_min_accuracy_pct": float(gate_min_accuracy_pct),
        "blocked_props": sorted(set(blocked_props)),
        "blocked_reasons": {k: blocked_reasons[k] for k in sorted(blocked_reasons)},
        "gate_by_prop": gate_by_prop,
        "prop_types": [str(p) for p in prop_types],
        "attempted": attempted,
        "updated": updated,
        "skipped_by_gate": skipped_by_gate,
        "failures": failures,
        "by_prop": by_prop,
        "error_samples": error_samples,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Recompute model_training_props predictions with current MLB feature logic.")
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--days-back", type=int, default=35)
    ap.add_argument("--prop-types", default="runs_scored,runs_rbis,hits_runs_rbis")
    ap.add_argument("--prop-source", default="mlb_api")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--allow-heuristic", action="store_true")
    ap.add_argument("--gate-min-total-per-prop", type=int, default=200)
    ap.add_argument("--gate-min-accuracy-pct", type=float, default=-1.0)
    args = ap.parse_args(list(argv) if argv is not None else None)

    prop_types = [p.strip() for p in str(args.prop_types).split(",") if p.strip()]
    if not prop_types:
        raise SystemExit("prop_types is required")
    from_date, to_date = _window_dates(args.from_date, args.to_date, int(args.days_back))
    payload = recompute(
        from_date=from_date,
        to_date=to_date,
        prop_types=prop_types,
        prop_source=str(args.prop_source),
        limit=max(0, int(args.limit)),
        allow_heuristic=bool(args.allow_heuristic),
        gate_min_total_per_prop=max(1, int(args.gate_min_total_per_prop)),
        gate_min_accuracy_pct=float(args.gate_min_accuracy_pct),
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
