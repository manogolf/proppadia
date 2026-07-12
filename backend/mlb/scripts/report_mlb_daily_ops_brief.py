#!/usr/bin/env python3
"""Build a human-readable MLB daily ops brief from existing daily artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _date_key(value: Any) -> str:
    try:
        raw = str(value or "").strip()
        if not raw:
            return ""
        return date.fromisoformat(raw[:10]).isoformat()
    except Exception:
        return ""


def _previous_date(value: str) -> str:
    key = _date_key(value)
    if not key:
        return ""
    return (date.fromisoformat(key) - timedelta(days=1)).isoformat()


def _path_mtime_utc(path: Path) -> str:
    try:
        if not path.exists():
            return ""
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


def _source_meta(path: Path, err: Optional[str], source_date: str, expected_date: str, cadence: str) -> Dict[str, Any]:
    return {
        "source_file": str(path),
        "source_date": source_date or "",
        "expected_date": expected_date or "",
        "mtime_utc": _path_mtime_utc(path),
        "cadence": cadence,
        "load_status": err or "ok",
    }


def _fmt_meta(meta: Dict[str, Any]) -> str:
    return (
        f"source `{meta.get('source_file') or 'derived'}` | "
        f"source_date `{meta.get('source_date') or 'n/a'}` | "
        f"expected `{meta.get('expected_date') or 'n/a'}` | "
        f"mtime/generated `{meta.get('mtime_utc') or meta.get('generated_at_utc') or 'n/a'}` | "
        f"freshness `{meta.get('freshness_status') or 'n/a'}` | "
        f"cadence `{meta.get('expected_refresh_cadence') or meta.get('cadence') or 'n/a'}`"
    )


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _as_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def _max_date_from_rows(rows: Sequence[Dict[str, Any]], keys: Sequence[str]) -> str:
    vals: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in keys:
            val = _date_key(row.get(key))
            if val:
                vals.append(val)
    return max(vals) if vals else ""


def _pct(v: Any, digits: int = 2) -> str:
    f = _as_float(v)
    if f is None:
        return "n/a"
    return f"{f * 100:.{digits}f}%"


def _num_fmt(v: Any, digits: int = 2) -> str:
    f = _as_float(v)
    if f is None:
        return "n/a"
    return f"{f:.{digits}f}"


def _ensure_parent(path: Path) -> None:
    if path.parent and str(path.parent) not in {"", "."}:
        path.parent.mkdir(parents=True, exist_ok=True)


def _load_json(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        if not path.exists():
            return None, "missing"
        return json.loads(path.read_text(encoding="utf-8")), None
    except Exception as exc:
        return None, f"error:{type(exc).__name__}"


def _resolve_rolling_candidate_obs_mode(raw_mode: Any, *, force_enabled: bool) -> str:
    if force_enabled:
        return "explicit_enabled"
    raw = str(raw_mode if raw_mode is not None else "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on", "enabled"}:
        return "explicit_enabled"
    if raw in {"0", "false", "no", "n", "off", "disabled"}:
        return "disabled"
    return "auto_detected"


def _load_optional_rolling_candidate_obs(path: Path, *, mode: str, expected_date: str) -> Dict[str, Any]:
    source_mtime = _path_mtime_utc(path)
    if mode == "disabled":
        return {
            "enabled": False,
            "source_state": "disabled",
            "source_path": str(path),
            "rolling_observation_mode": "disabled",
            "rolling_observation_source": str(path),
            "rolling_observation_source_mtime": source_mtime,
        }
    raw, err = _load_json(path)
    if err:
        enabled = mode == "explicit_enabled"
        return {
            "enabled": enabled,
            "source_state": f"warn_{err}" if enabled else f"unavailable_{err}",
            "source_path": str(path),
            "warning": f"Rolling market-late observation artifact unavailable: {err}",
            "rolling_observation_mode": mode if enabled else "unavailable",
            "rolling_observation_source": str(path),
            "rolling_observation_source_mtime": source_mtime,
        }
    if not isinstance(raw, dict):
        enabled = mode == "explicit_enabled"
        return {
            "enabled": enabled,
            "source_state": "warn_invalid_payload" if enabled else "unavailable_invalid_payload",
            "source_path": str(path),
            "warning": "Rolling market-late observation artifact is not a JSON object.",
            "rolling_observation_mode": mode if enabled else "unavailable",
            "rolling_observation_source": str(path),
            "rolling_observation_source_mtime": source_mtime,
        }
    payload_date = _date_key(raw.get("date") or raw.get("slate_date") or raw.get("current_slate_date"))
    if expected_date and payload_date and payload_date != expected_date:
        enabled = mode == "explicit_enabled"
        return {
            "enabled": enabled,
            "source_state": "warn_stale_date" if enabled else "unavailable_stale_date",
            "source_path": str(path),
            "warning": (
                f"Rolling market-late observation artifact date {payload_date} "
                f"does not match current slate date {expected_date}."
            ),
            "rolling_observation_mode": mode if enabled else "unavailable",
            "rolling_observation_source": str(path),
            "rolling_observation_source_mtime": source_mtime,
            "rolling_observation_payload_date": payload_date,
        }
    if expected_date and not payload_date:
        enabled = mode == "explicit_enabled"
        return {
            "enabled": enabled,
            "source_state": "warn_missing_date" if enabled else "unavailable_missing_date",
            "source_path": str(path),
            "warning": "Rolling market-late observation artifact does not declare a same-date slate date.",
            "rolling_observation_mode": mode if enabled else "unavailable",
            "rolling_observation_source": str(path),
            "rolling_observation_source_mtime": source_mtime,
        }
    payload = dict(raw)
    payload["enabled"] = True
    payload["source_state"] = "ok"
    payload["source_path"] = str(path)
    payload["rolling_observation_mode"] = mode
    payload["rolling_observation_source"] = str(path)
    payload["rolling_observation_source_mtime"] = source_mtime
    payload["rolling_observation_payload_date"] = payload_date
    return payload


def _load_last_jsonl(path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        if not path.exists():
            return None, "missing"
        lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        if not lines:
            return None, "empty"
        obj = json.loads(lines[-1])
        if not isinstance(obj, dict):
            return None, "last_row_not_object"
        return obj, None
    except Exception as exc:
        return None, f"error:{type(exc).__name__}"


def _load_jsonl_objects(path: Path) -> List[Dict[str, Any]]:
    try:
        if not path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        return rows
    except Exception:
        return []


def _alert_sig(prefix: str, row: Dict[str, Any]) -> str:
    bits = [
        prefix,
        str(row.get("severity") or ""),
        str(row.get("type") or ""),
        str(row.get("message") or ""),
    ]
    return "|".join(bits)


def _previous_alert_first_seen(history_rows: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    first_seen: Dict[str, str] = {}
    for payload in history_rows:
        generated = str(payload.get("generated_at_utc") or "")
        postgrade = payload.get("postgrade") if isinstance(payload.get("postgrade"), dict) else {}
        for alert in postgrade.get("alerts") or []:
            if isinstance(alert, dict):
                sig = _alert_sig("postgrade", alert)
                first_seen.setdefault(sig, generated)
        model = payload.get("model_vs_fade") if isinstance(payload.get("model_vs_fade"), dict) else {}
        if model.get("fade_beating_model_alert"):
            first_seen.setdefault("model_vs_fade|fade_beating_model_alert", generated)
    return first_seen


def _annotate_alerts(
    *,
    report_date: str,
    generated_at_utc: str,
    postgrade: Dict[str, Any],
    model_vs_fade: Dict[str, Any],
    history_rows: Sequence[Dict[str, Any]],
    fresh_source_dates: Sequence[str],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    first_seen = _previous_alert_first_seen(history_rows)
    fresh_sources = {_date_key(d) for d in fresh_source_dates if _date_key(d)}
    annotated: List[Dict[str, Any]] = []
    new_count = 0
    persistent_count = 0
    for alert in postgrade.get("alerts") or []:
        if not isinstance(alert, dict):
            continue
        row = dict(alert)
        sig = _alert_sig("postgrade", row)
        seen_at = first_seen.get(sig) or generated_at_utc
        seen_date = _date_key(seen_at)
        source_date = _date_key(postgrade.get("report_date")) or report_date
        age_basis = seen_date if sig in first_seen and seen_date else source_date
        is_new = sig not in first_seen and source_date in fresh_sources
        row.update(
            {
                "alert_source_date": source_date,
                "alert_generated_at": generated_at_utc,
                "alert_last_changed_at": seen_at if (sig in first_seen or is_new) else "",
                "alert_age_days": max(0, (date.fromisoformat(report_date) - date.fromisoformat(age_basis or report_date)).days),
                "alert_is_new_today": bool(is_new),
                "alert_is_persistent": not bool(is_new),
            }
        )
        new_count += int(bool(is_new))
        persistent_count += int(not bool(is_new))
        annotated.append(row)
    postgrade = dict(postgrade)
    postgrade["alerts"] = annotated

    model_alert = {
        "alert_active": bool(model_vs_fade.get("fade_beating_model_alert")),
        "alert_source_date": model_vs_fade.get("window_game_date_max") or "",
        "alert_generated_at": generated_at_utc,
        "alert_last_changed_at": "",
        "alert_age_days": 0,
        "alert_is_new_today": False,
        "alert_is_persistent": False,
    }
    if model_alert["alert_active"]:
        sig = "model_vs_fade|fade_beating_model_alert"
        seen_at = first_seen.get(sig) or generated_at_utc
        seen_date = _date_key(seen_at)
        source_date = _date_key(model_alert["alert_source_date"])
        age_basis = seen_date if sig in first_seen and seen_date else source_date
        is_new = sig not in first_seen and source_date in fresh_sources
        model_alert.update(
            {
                "alert_last_changed_at": seen_at if (sig in first_seen or is_new) else "",
                "alert_age_days": max(0, (date.fromisoformat(report_date) - date.fromisoformat(age_basis or report_date)).days),
                "alert_is_new_today": bool(is_new),
                "alert_is_persistent": not bool(is_new),
            }
        )
        new_count += int(bool(is_new))
        persistent_count += int(not bool(is_new))
    model_vs_fade = dict(model_vs_fade)
    model_vs_fade["alert_state"] = model_alert
    return postgrade, {
        "new_alerts_count": int(new_count),
        "persistent_alerts_count": int(persistent_count),
    } | model_vs_fade


def _extract_pipeline(last_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not last_row:
        return {}
    checks = last_row.get("checks") or []
    check_rows: List[Dict[str, Any]] = []
    for c in checks:
        if not isinstance(c, dict):
            continue
        check_rows.append(
            {
                "name": c.get("name"),
                "status": c.get("status"),
                "ok": bool(c.get("ok")),
            }
        )
    return {
        "captured_at": last_row.get("captured_at"),
        "status": last_row.get("status"),
        "ok": bool(last_row.get("ok")),
        "failures": last_row.get("failures") or [],
        "degraded_prop_lanes": last_row.get("degraded_prop_lanes") or [],
        "checks": check_rows,
    }


def _extract_ops(last_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not last_row:
        return {}
    checks = last_row.get("checks") or {}
    out_checks: Dict[str, Any] = {}
    if isinstance(checks, dict):
        for k in ("status", "health", "incident"):
            v = checks.get(k)
            if isinstance(v, dict):
                out_checks[k] = {
                    "status": v.get("status"),
                    "ok": bool(v.get("ok")),
                }
    return {
        "captured_at": last_row.get("captured_at"),
        "status": last_row.get("status"),
        "ok": bool(last_row.get("ok")),
        "failures": last_row.get("failures") or [],
        "checks": out_checks,
    }


def _extract_postgrade(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    alerts = js.get("alerts") or []
    alert_rows: List[Dict[str, Any]] = []
    for a in alerts:
        if not isinstance(a, dict):
            continue
        alert_rows.append(
            {
                "code": a.get("code"),
                "severity": a.get("severity"),
                "type": a.get("type"),
                "message": a.get("message"),
                "recommendation": a.get("recommendation"),
            }
        )
    return {
        "report_date": js.get("report_date"),
        "status": js.get("status"),
        "alerts_count": _as_int(js.get("alerts_count")) or 0,
        "critical_count": _as_int(js.get("critical_count")) or 0,
        "warning_count": _as_int(js.get("warning_count")) or 0,
        "alerts": alert_rows,
    }


def _extract_model_vs_fade(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    overall = js.get("overall") or {}
    counts = js.get("counts") or {}
    window = js.get("window") or {}
    return {
        "rows_csv": js.get("rows_csv"),
        "window_game_date_min": window.get("game_date_min"),
        "window_game_date_max": window.get("game_date_max"),
        "paired_bets": _as_int(overall.get("paired_bets")),
        "model_roi_1u": _as_float(overall.get("model_roi_1u")),
        "fade_roi_1u": _as_float(overall.get("fade_roi_1u")),
        "delta_fade_minus_model_1u": _as_float(overall.get("delta_fade_minus_model_1u")),
        "model_win_rate": _as_float(overall.get("model_win_rate")),
        "fade_win_rate": _as_float(overall.get("fade_win_rate")),
        "fade_beating_model_alert": bool(overall.get("fade_beating_model_alert")),
        "rows_input": _as_int(counts.get("rows_input")),
        "rows_paired_for_fade": _as_int(counts.get("rows_paired_for_fade")),
    }


def _load_csv_rows(path: Path) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    try:
        if not path.exists():
            return [], "missing"
        with path.open("r", encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh)), None
    except Exception as exc:
        return [], f"error:{type(exc).__name__}"


def _extract_prop_regime(rows: Sequence[Dict[str, Any]], path: Path) -> Dict[str, Any]:
    props = [str(r.get("prop_type") or "").strip().lower() for r in rows if str(r.get("prop_type") or "").strip()]
    dates = [str(r.get("latest_usable_date") or "").strip() for r in rows if str(r.get("latest_usable_date") or "").strip()]
    return {
        "path": str(path),
        "prop_count": len(set(props)),
        "max_latest_usable_date": max(dates) if dates else None,
        "outs_recorded_present": "outs_recorded" in set(props),
    }


def _extract_reporting_alignment(rows: Sequence[Dict[str, Any]], path: Path) -> Dict[str, Any]:
    statuses = [str(r.get("alignment_status") or "").strip() for r in rows]
    return {
        "path": str(path),
        "rows": len(rows),
        "stale_outlook_source_count": sum(1 for s in statuses if s == "stale_outlook_source"),
    }


def _extract_model_performance(
    summary_rows: Sequence[Dict[str, Any]],
    daily_rows: Sequence[Dict[str, Any]],
    *,
    summary_path: Path,
    daily_path: Path,
) -> Dict[str, Any]:
    props = [str(r.get("prop_type") or "").strip().lower() for r in summary_rows if str(r.get("prop_type") or "").strip()]
    source_types = sorted(
        {
            str(r.get("source_type") or "").strip()
            for r in list(summary_rows) + list(daily_rows)
            if str(r.get("source_type") or "").strip()
        }
    )
    missing_reason_count = sum(
        1
        for r in list(summary_rows) + list(daily_rows)
        if str(r.get("missing_reason") or "").strip()
    )
    critical = sorted(
        str(r.get("prop_type") or "").strip().lower()
        for r in summary_rows
        if str(r.get("status") or "").strip().lower() == "critical" and str(r.get("prop_type") or "").strip()
    )
    watch = sorted(
        str(r.get("prop_type") or "").strip().lower()
        for r in summary_rows
        if str(r.get("status") or "").strip().lower() == "watch" and str(r.get("prop_type") or "").strip()
    )
    return {
        "summary_path": str(summary_path),
        "daily_path": str(daily_path),
        "source_date": _max_date_from_rows(
            list(summary_rows) + list(daily_rows),
            ("game_date", "date", "report_date", "label_date", "as_of_date"),
        ),
        "source_type": ",".join(source_types) if source_types else None,
        "active_prop_count": len(set(props)),
        "missing_reason_count": missing_reason_count,
        "critical_props": critical,
        "watch_props": watch,
    }


def _extract_bvp_impact(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    prop_impact = js.get("prop_impact") or []
    top_prop = None
    if isinstance(prop_impact, list) and prop_impact:
        try:
            top_prop = sorted(
                [p for p in prop_impact if isinstance(p, dict)],
                key=lambda x: float(x.get("mean_abs_delta_prob") or 0.0),
                reverse=True,
            )[0]
        except Exception:
            top_prop = None
    return {
        "generated_at_utc": js.get("generated_at_utc"),
        "label_date": js.get("label_date"),
        "requested_slate_date": js.get("requested_slate_date"),
        "rows_total_slate": _as_int(js.get("rows_total_slate")),
        "rows_evaluated": _as_int(js.get("rows_evaluated")),
        "rows_nonzero_delta": _as_int(js.get("rows_nonzero_delta")),
        "mean_abs_delta_prob": _as_float(js.get("mean_abs_delta_prob")),
        "max_abs_delta_prob": _as_float(js.get("max_abs_delta_prob")),
        "props_using_bvp": js.get("props_using_bvp") or [],
        "top_prop_impact": top_prop,
    }


def _validate_bvp_impact_freshness(
    *,
    bvp_impact: Dict[str, Any],
    bvp_err: Optional[str],
    report_date: str,
    require_fresh: bool,
) -> Tuple[str, List[str]]:
    if bvp_err:
        return bvp_err, [f"bvp_impact_json={bvp_err}"] if require_fresh else []

    label_date = str(bvp_impact.get("label_date") or "").strip()
    requested_slate_date = str(bvp_impact.get("requested_slate_date") or "").strip()
    if not label_date:
        state = "missing_label_date"
        return state, [f"stale_bvp_impact:{state}"] if require_fresh else []

    if label_date != str(report_date).strip():
        state = f"stale_bvp_impact:label_date:{label_date}"
        return state, [f"{state} expected={report_date}"] if require_fresh else []

    if requested_slate_date and requested_slate_date != str(report_date).strip():
        state = f"stale_bvp_impact:requested_slate_date:{requested_slate_date}"
        return state, [f"{state} expected={report_date}"] if require_fresh else []

    return "ok", []


def _freshness_row(
    *,
    section: str,
    source_file: str,
    source_date: str,
    expected_date: str,
    generated_at_utc: str = "",
    mtime_utc: str = "",
    load_status: str = "ok",
    cadence: str = "",
    freshness_status: str = "",
    note: str = "",
) -> Dict[str, Any]:
    source_raw = str(source_date or "").strip()
    expected_raw = str(expected_date or "").strip()
    source_date = _date_key(source_raw) if len(source_raw) >= 10 and " or " not in source_raw else source_raw
    source_date = source_date or source_raw
    expected_date = _date_key(expected_raw) if len(expected_raw) >= 10 and " or " not in expected_raw else expected_raw
    expected_date = expected_date or expected_raw
    load_status = str(load_status or "ok")
    if not freshness_status:
        if load_status == "missing":
            freshness_status = "missing-unexpected"
        elif load_status.startswith("error") or load_status in {"empty", "last_row_not_object"}:
            freshness_status = "stale-unexpected"
        elif expected_date and source_date == expected_date:
            freshness_status = "fresh"
        elif source_date:
            freshness_status = "stale-unexpected"
        else:
            freshness_status = "missing-unexpected"
    return {
        "section": section,
        "source_file": source_file,
        "source_date": source_date,
        "expected_date": expected_date,
        "generated_at_utc": generated_at_utc or "",
        "mtime_utc": mtime_utc or "",
        "load_status": load_status,
        "expected_refresh_cadence": cadence,
        "freshness_status": freshness_status,
        "note": note,
    }


def _build_freshness_audit(
    *,
    report_date: str,
    completed_slate_date: str,
    current_slate_date: str,
    generated_at_utc: str,
    source_states: Dict[str, Any],
    paths: Dict[str, Path],
    pipeline: Dict[str, Any],
    ops: Dict[str, Any],
    postgrade: Dict[str, Any],
    model_vs_fade: Dict[str, Any],
    prop_regime: Dict[str, Any],
    model_performance: Dict[str, Any],
    reporting_alignment: Dict[str, Any],
    bvp_impact: Dict[str, Any],
    hits_env: Dict[str, Any],
    overlap_watch: Dict[str, Any],
    qc_bottom_order_watch: Dict[str, Any],
    hits_15_tier_backtest: Dict[str, Any],
    review_aid_performance: Dict[str, Any],
    total_bases_shadow_summary: Dict[str, Any],
    total_bases_shadow_evaluation: Dict[str, Any],
    feature_lineage_health: Dict[str, Any],
    today_workspace: Dict[str, Any],
    input_refresh_status: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    refresh_results = [r for r in (input_refresh_status.get("results") or []) if isinstance(r, dict)]
    refresh_issue_count = sum(
        1
        for row in refresh_results
        if str(row.get("status") or "") in {"dependency_missing", "stale_after_refresh", "refresh_failed"}
    )
    if input_refresh_status:
        refresh_source_date = _date_key(input_refresh_status.get("completed_slate_date"))
        refresh_status = "fresh" if refresh_issue_count == 0 and refresh_source_date == completed_slate_date else "refresh_failed"
        if int(input_refresh_status.get("dependency_missing_count") or 0) > 0:
            refresh_status = "dependency_missing"
        refresh_note = (
            f"dependency_missing_count={input_refresh_status.get('dependency_missing_count', 0)}; "
            f"refresh_failed_count={input_refresh_status.get('refresh_failed_count', 0)}; "
            f"stale_after_refresh_count={input_refresh_status.get('stale_after_refresh_count', 0)}; "
            f"reconcile_rows_csv={input_refresh_status.get('reconcile_rows_csv', '')}; "
            f"reconcile_rows_exists={input_refresh_status.get('reconcile_rows_exists', False)}"
        )
    else:
        refresh_source_date = ""
        refresh_status = "missing-unexpected"
        refresh_note = "Missing input-refresh status artifact; cannot prove dependency refresh ran before brief generation."
    rows.append(
        _freshness_row(
            section="Ops Brief Input Refresh",
            source_file=str(paths["input_refresh_status_json"]),
            source_date=refresh_source_date,
            expected_date=completed_slate_date,
            mtime_utc=_path_mtime_utc(paths["input_refresh_status_json"]),
            load_status=str(source_states.get("input_refresh_status_json") or "ok"),
            cadence="each brief run before report generation",
            freshness_status=refresh_status,
            note=refresh_note,
        )
    )
    pipe_date = max([d for d in (_date_key(pipeline.get("captured_at")), _date_key(ops.get("captured_at"))) if d] or [""])
    rows.append(
        _freshness_row(
            section="Pipeline & Ops",
            source_file=f"{paths['pipeline_history_jsonl']} ; {paths['ops_history_jsonl']}",
            source_date=pipe_date,
            expected_date=report_date,
            mtime_utc=max(_path_mtime_utc(paths["pipeline_history_jsonl"]), _path_mtime_utc(paths["ops_history_jsonl"])),
            load_status="ok" if source_states.get("pipeline_history_jsonl") == "ok" and source_states.get("ops_history_jsonl") == "ok" else "error",
            cadence="persistent history; updates when daily gate/ops capture runs",
            freshness_status="fresh" if pipe_date == report_date else "persistent-state",
            note="Latest captured gate state carries forward until a new gate capture is written.",
        )
    )
    rows.append(
        _freshness_row(
            section="Postgrade Alerts",
            source_file=str(paths["postgrade_alerts_json"]),
            source_date=_date_key(postgrade.get("report_date")),
            expected_date=completed_slate_date,
            mtime_utc=_path_mtime_utc(paths["postgrade_alerts_json"]),
            load_status=str(source_states.get("postgrade_alerts_json") or "ok"),
            cadence="daily after completed slate grading",
            note="Alert rows include new-vs-persistent fields below.",
        )
    )
    rows.append(
        _freshness_row(
            section="Model vs Fade",
            source_file=str(paths["model_vs_fade_json"]),
            source_date=_date_key(model_vs_fade.get("window_game_date_max")),
            expected_date=completed_slate_date,
            mtime_utc=_path_mtime_utc(paths["model_vs_fade_json"]),
            load_status=str(source_states.get("model_vs_fade_json") or "ok"),
            cadence="daily after actual wager reconcile",
            note="Fade/model warning may persist; alert state records last change.",
        )
    )
    rows.append(
        _freshness_row(
            section="Prop Outlook Freshness",
            source_file=str(paths["prop_regime_csv"]),
            source_date=_date_key(prop_regime.get("max_latest_usable_date")),
            expected_date=completed_slate_date,
            mtime_utc=_path_mtime_utc(paths["prop_regime_csv"]),
            load_status=str(source_states.get("prop_regime_csv") or "ok"),
            cadence="daily after source outlook generation",
            note="Uses max latest_usable_date across prop regime rows.",
        )
    )
    perf_source_date = str(model_performance.get("source_date") or "")
    rows.append(
        _freshness_row(
            section="Model Performance By Prop",
            source_file=f"{paths['model_performance_summary_csv']} ; {paths['model_performance_daily_csv']}",
            source_date=perf_source_date,
            expected_date=completed_slate_date,
            mtime_utc=max(_path_mtime_utc(paths["model_performance_summary_csv"]), _path_mtime_utc(paths["model_performance_daily_csv"])),
            load_status=(
                "ok"
                if source_states.get("model_performance_summary_csv") == "ok"
                and source_states.get("model_performance_daily_csv") == "ok"
                else "missing"
            ),
            cadence="daily after model outcome grading",
            note="Uses max date found in rolling/daily performance CSV rows.",
        )
    )
    rows.append(
        _freshness_row(
            section="Path Forward",
            source_file="derived",
            source_date=report_date,
            expected_date=report_date,
            generated_at_utc=generated_at_utc,
            cadence="derived each brief run",
            freshness_status="fresh",
            note="Recommendations are generated from the loaded section states.",
        )
    )
    bvp_label = _date_key(bvp_impact.get("label_date"))
    bvp_requested = _date_key(bvp_impact.get("requested_slate_date"))
    bvp_status = ""
    bvp_note = "BvP can legitimately reflect the prior completed slate after later reruns."
    if str(source_states.get("bvp_impact_json") or "") != "ok":
        bvp_status = "missing-unexpected" if source_states.get("bvp_impact_json") == "missing" else "stale-unexpected"
    elif bvp_label == current_slate_date:
        bvp_status = "fresh"
        bvp_note = "BvP impact matches the current slate."
    elif bvp_label == completed_slate_date and (not bvp_requested or bvp_requested == completed_slate_date):
        bvp_status = "stale-but-expected"
    elif bvp_label:
        bvp_status = "stale-unexpected"
        bvp_note = "BvP impact is older than the accepted current/prior completed lifecycle."
    bvp_prewarm_note = _bvp_prewarm_failure_note(
        current_slate_date=current_slate_date,
        completed_slate_date=completed_slate_date,
    )
    if bvp_status == "stale-unexpected" and bvp_prewarm_note:
        bvp_status = "refresh_failed"
        bvp_note = bvp_prewarm_note
    rows.append(
        _freshness_row(
            section="BvP Impact",
            source_file=str(paths["bvp_impact_json"]),
            source_date=bvp_label,
            expected_date=f"{current_slate_date} or {completed_slate_date}",
            generated_at_utc=str(bvp_impact.get("generated_at_utc") or ""),
            mtime_utc=_path_mtime_utc(paths["bvp_impact_json"]),
            load_status=str(source_states.get("bvp_impact_json") or "ok"),
            cadence="before first morning slate run; may carry prior completed slate afterward",
            freshness_status=bvp_status,
            note=bvp_note,
        )
    )
    hits_status = "refresh_failed" if str(hits_env.get("status") or "").lower() == "fail" else ""
    hits_requested_date = _date_key(hits_env.get("requested_as_of_date"))
    hits_evaluation_date = _date_key(hits_env.get("evaluation_date"))
    hits_note = (
        f"Requested as-of {hits_requested_date or 'n/a'}; "
        f"league evaluation date {hits_evaluation_date or 'n/a'}; "
        f"team eval context as-of {_date_key(hits_env.get('team_eval_context_as_of_date')) or 'n/a'}."
    )
    if hits_status:
        failures = ",".join(str(x) for x in (hits_env.get("failures") or []) if str(x).strip())
        warnings = ",".join(str(x) for x in (hits_env.get("warnings") or []) if str(x).strip())
        hits_note = f"Refresh failed: {failures or warnings or 'unknown'}"
    if hits_env.get("fallback_used"):
        hits_note = (
            f"{hits_note} | display fallback={hits_env.get('fallback_source_file')} "
            f"({hits_env.get('fallback_reason')})"
        )
    if hits_env.get("team_eval_fallback_used"):
        hits_note = (
            f"{hits_note} | team eval fallback={hits_env.get('team_eval_fallback_source_file')} "
            f"generated={hits_env.get('team_eval_fallback_generated_at_utc') or 'n/a'} "
            f"({hits_env.get('team_eval_fallback_reason')})"
        )
    rows.append(
        _freshness_row(
            section="Hits Environment & Matchups",
            source_file=str(paths["hits_environment_json"]),
            source_date=hits_requested_date or hits_evaluation_date,
            expected_date=current_slate_date,
            generated_at_utc=str(hits_env.get("generated_at_utc") or ""),
            mtime_utc=_path_mtime_utc(paths["hits_environment_json"]),
            load_status=str(source_states.get("hits_environment_json") or "ok"),
            cadence="daily for current slate workspace context",
            freshness_status=hits_status,
            note=hits_note,
        )
    )
    rows.append(
        _freshness_row(
            section="Ranking/QC Overlap Watch",
            source_file=str(paths["overlap_watch_json"]),
            source_date=_date_key(overlap_watch.get("latest_completed_slate")),
            expected_date=completed_slate_date,
            generated_at_utc=str(overlap_watch.get("generated_at") or ""),
            mtime_utc=_path_mtime_utc(paths["overlap_watch_json"]),
            load_status=str(source_states.get("overlap_watch_json") or "ok"),
            cadence="daily after completed-slate reconcile and actual wager matching",
            note=(
                f"composition_drift_flag={overlap_watch.get('composition_drift_flag') or 'n/a'}; "
                f"action={overlap_watch.get('action_annotation') or 'n/a'}"
            ),
        )
    )
    rows.append(
        _freshness_row(
            section="QC Bottom-Order Under Watch",
            source_file=str(paths["qc_bottom_order_watch_json"]),
            source_date=_date_key(qc_bottom_order_watch.get("latest_reconcile_date")),
            expected_date=completed_slate_date,
            generated_at_utc=str(qc_bottom_order_watch.get("generated_at") or ""),
            mtime_utc=_path_mtime_utc(paths["qc_bottom_order_watch_json"]),
            load_status=str(source_states.get("qc_bottom_order_watch_json") or "ok"),
            cadence="daily after completed-slate reconcile and actual wager matching",
            note=(
                f"recommendation={qc_bottom_order_watch.get('recommendation') or 'n/a'}; "
                f"reason={qc_bottom_order_watch.get('recommendation_reason') or 'n/a'}"
            ),
        )
    )
    for section, key, note in (
        (
            "Hits Over 1.5 Watch Candidates",
            "hits_o15_watch_candidates_csv",
            "current-slate board generated by mlb-hits-o15-watch-candidates inside upload prep.",
        ),
        (
            "Hits Over 1.5 Layered Candidates",
            "hits_o15_layered_candidates_csv",
            "current-slate board generated by mlb-hits-o15-layered-candidates inside upload prep.",
        ),
        (
            "Hits Under 1.5 Favorite Audit",
            "hits_u15_favorite_audit_csv",
            "current-slate board generated by mlb-hits-u15-favorite-audit inside upload prep.",
        ),
        (
            "Hits 1.5 Alternate Discovery",
            "hits_o15_alternate_discovery_csv",
            "current-slate discovery-only board generated by mlb-hits-o15-alternate-discovery when alternate source rows are available.",
        ),
    ):
        load_state = str(source_states.get(key) or "ok")
        optional_missing = key == "hits_o15_alternate_discovery_csv" and load_state == "missing"
        rows.append(
            _freshness_row(
                section=section,
                source_file=str(paths[key]),
                source_date=current_slate_date if load_state == "ok" else "",
                expected_date=current_slate_date,
                mtime_utc=_path_mtime_utc(paths[key]),
                load_status=load_state,
                cadence="daily current-slate upload prep before Ops Brief render",
                freshness_status=(
                    "fresh" if load_state == "ok" else "optional-missing" if optional_missing else "missing-unexpected"
                ),
                note=note if load_state == "ok" else (
                    f"OPTIONAL_MISSING: {paths[key]}" if optional_missing else f"MISSING_INPUT: {paths[key]}"
                ),
            )
        )
    perf_status = str(review_aid_performance.get("status") or "")
    rows.append(
        _freshness_row(
            section="Review Aid Performance",
            source_file=str(paths["review_aid_performance_json"]),
            source_date=_date_key(review_aid_performance.get("latest_completed_slate")),
            expected_date=completed_slate_date,
            generated_at_utc=str(review_aid_performance.get("generated_at") or ""),
            mtime_utc=_path_mtime_utc(paths["review_aid_performance_json"]),
            load_status=str(source_states.get("review_aid_performance_json") or "ok"),
            cadence="daily after completed-slate reconcile; review aid reporting only",
            freshness_status=("source_not_ready" if perf_status == "source_not_ready" else ""),
            note=(
                f"status={perf_status or 'n/a'}; "
                f"board_rows={review_aid_performance.get('board_rows_loaded', 'n/a')}; "
                f"matched={review_aid_performance.get('matched_rows', 'n/a')}"
            ),
        )
    )
    rows.append(
        _freshness_row(
            section="Total Bases Shadow Candidate",
            source_file=str(paths["total_bases_shadow_summary_json"]),
            source_date=_date_key(total_bases_shadow_summary.get("slate_date")),
            expected_date=current_slate_date,
            generated_at_utc=str(total_bases_shadow_summary.get("generated_at") or ""),
            mtime_utc=_path_mtime_utc(paths["total_bases_shadow_summary_json"]),
            load_status=str(source_states.get("total_bases_shadow_summary_json") or "ok"),
            cadence="daily current-slate shadow scoring; analysis-only",
            freshness_status=(
                "fresh"
                if _date_key(total_bases_shadow_summary.get("slate_date")) == current_slate_date
                else ("missing-unexpected" if not total_bases_shadow_summary else "stale-unexpected")
            ),
            note=(
                f"rows={total_bases_shadow_summary.get('shadow_rows', 0)}; "
                f"side_changed={total_bases_shadow_summary.get('side_changed_rows', 0)}; "
                f"production_outputs_changed={total_bases_shadow_summary.get('production_outputs_changed', False)}"
            ),
        )
    )
    scanned = total_bases_shadow_evaluation.get("shadow_dates_scanned") or []
    eval_source_date = max([_date_key(x) for x in scanned if _date_key(x)] or [""])
    rows.append(
        _freshness_row(
            section="Total Bases Shadow Evaluation",
            source_file=str(paths["total_bases_shadow_evaluation_json"]),
            source_date=eval_source_date,
            expected_date=current_slate_date,
            generated_at_utc=str(total_bases_shadow_evaluation.get("generated_at") or ""),
            mtime_utc=_path_mtime_utc(paths["total_bases_shadow_evaluation_json"]),
            load_status=str(source_states.get("total_bases_shadow_evaluation_json") or "ok"),
            cadence="daily cumulative read-only evaluation after shadow scoring",
            freshness_status=(
                "fresh"
                if eval_source_date == current_slate_date
                else ("missing-unexpected" if not total_bases_shadow_evaluation else "stale-unexpected")
            ),
            note=(
                f"rows_scored={total_bases_shadow_evaluation.get('rows_scored', 0)}; "
                f"rows_with_outcomes={total_bases_shadow_evaluation.get('rows_with_outcomes', 0)}; "
                f"side_changed={total_bases_shadow_evaluation.get('side_changed_rows', 0)}"
            ),
        )
    )
    lineage_status_raw = str(feature_lineage_health.get("status") or "").lower()
    if lineage_status_raw == "pass":
        lineage_freshness = "fresh"
    elif lineage_status_raw == "warn":
        lineage_freshness = "feature-lineage-warn"
    elif lineage_status_raw == "fail":
        lineage_freshness = "feature-lineage-fail"
    elif source_states.get("feature_lineage_health_json") == "missing":
        lineage_freshness = "missing-unexpected"
    else:
        lineage_freshness = "missing-unexpected" if not feature_lineage_health else "feature-lineage-warn"
    lineage_summary = feature_lineage_health.get("summary") if isinstance(feature_lineage_health.get("summary"), dict) else {}
    rows.append(
        _freshness_row(
            section="Feature Lineage Health",
            source_file=str(paths["feature_lineage_health_json"]),
            source_date=_date_key(feature_lineage_health.get("slate_date")),
            expected_date=current_slate_date,
            generated_at_utc=str(feature_lineage_health.get("generated_at_utc") or ""),
            mtime_utc=_path_mtime_utc(paths["feature_lineage_health_json"]),
            load_status=str(source_states.get("feature_lineage_health_json") or "ok"),
            cadence="daily after current-slate selector/upload diagnostics are written",
            freshness_status=lineage_freshness,
            note=(
                f"status={feature_lineage_health.get('status') or 'missing'}; "
                f"pass={lineage_summary.get('pass_count', 0)}; "
                f"warn={lineage_summary.get('warn_count', 0)}; "
                f"fail={lineage_summary.get('fail_count', 0)}; "
                f"bvp_payload_artifacts={lineage_summary.get('bvp_artifacts_with_payload', 0)}; "
                f"bvp_missing_required={len(lineage_summary.get('bvp_missing_required_columns') or [])}"
            ),
        )
    )
    align_status = "fresh"
    align_note = "Expected after reporting alignment audit for the completed slate."
    if source_states.get("reporting_alignment_csv") == "missing":
        align_status = "missing-unexpected"
        align_note = "Missing is actionable when a completed-slate brief expects reporting alignment output."
    rows.append(
        _freshness_row(
            section="Source Health",
            source_file=str(paths["reporting_alignment_csv"]),
            source_date=completed_slate_date if source_states.get("reporting_alignment_csv") == "ok" else "",
            expected_date=completed_slate_date,
            mtime_utc=_path_mtime_utc(paths["reporting_alignment_csv"]),
            load_status=str(source_states.get("reporting_alignment_csv") or "ok"),
            cadence="daily after reporting alignment audit",
            freshness_status=align_status if source_states.get("reporting_alignment_csv") == "missing" else "",
            note=align_note,
        )
    )
    workspace_status = str(today_workspace.get("status") or "")
    if workspace_status == "pass":
        ws_status = "fresh"
    elif workspace_status == "not_refreshed":
        ws_status = "not-refreshed"
    else:
        ws_status = "refresh_failed"
    rows.append(
        _freshness_row(
            section="MLB Today Workspace",
            source_file="backend.app.services.mlb.today_workspace_service.fetch_today_workspace",
            source_date=_date_key(today_workspace.get("active_slate_date") or today_workspace.get("requested_slate_date")),
            expected_date=current_slate_date,
            generated_at_utc=generated_at_utc,
            load_status=str(source_states.get("today_workspace") or "ok"),
            cadence="current slate staging",
            freshness_status=ws_status,
            note=str(
                (today_workspace.get("diagnostics") or {}).get("failure_classification")
                or today_workspace.get("reason")
                or "ok"
            ),
        )
    )
    return rows


def _extract_hits_env(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    league = (js.get("league_hits_environment") or {}).get("today_vs_baseline") or {}
    starter = js.get("starter_hits_allowed_residual") or {}
    weighted = starter.get("weighted_baseline") or {}
    slate_ctx = js.get("slate_hits_allowed_context") or {}
    team_eval = js.get("team_hits_allowed_matchup_evaluation") or {}
    lifecycle = js.get("starter_market_lifecycle") or {}
    return {
        "generated_at_utc": js.get("generated_at_utc"),
        "requested_as_of_date": js.get("requested_as_of_date"),
        "evaluation_date": js.get("evaluation_date"),
        "status": js.get("status"),
        "ok": bool(js.get("ok")),
        "failures": js.get("failures") or [],
        "warnings": js.get("warnings") or [],
        "league_signal": league.get("signal"),
        "league_hits_per_game": _as_float(league.get("hits_per_game")),
        "league_zscore": _as_float(league.get("zscore")),
        "starter_rows": _as_int(starter.get("rows")),
        "starter_residual_vs_d7_avg": _as_float(starter.get("residual_vs_d7_avg")),
        "starter_residual_vs_weighted_avg": _as_float(weighted.get("residual_vs_weighted_avg")),
        "slate_rows": _as_int(slate_ctx.get("rows")),
        "slate_rows_with_expected": _as_int(slate_ctx.get("rows_with_expected_hits_allowed_matchup")),
        "slate_avg_expected_matchup": _as_float(slate_ctx.get("avg_expected_hits_allowed_matchup")),
        "slate_avg_line_minus_expected": _as_float(slate_ctx.get("avg_line_minus_expected_hits_allowed_matchup")),
        "slate_rows_with_bullpen_blended": _as_int(slate_ctx.get("rows_with_bullpen_hits_allowed_form_blended")),
        "slate_avg_bullpen_blended": _as_float(slate_ctx.get("avg_bullpen_hits_allowed_form_blended")),
        "slate_rows_with_team_expected": _as_int(slate_ctx.get("rows_with_expected_team_hits_allowed_matchup")),
        "slate_avg_team_expected": _as_float(slate_ctx.get("avg_expected_team_hits_allowed_matchup")),
        "top_expected_matchups": slate_ctx.get("top_expected_hits_allowed_matchups") or [],
        "lowest_expected_matchups": slate_ctx.get("lowest_expected_hits_allowed_matchups") or [],
        "top_expected_team_matchups": slate_ctx.get("top_expected_team_hits_allowed_matchups") or [],
        "lowest_expected_team_matchups": slate_ctx.get("lowest_expected_team_hits_allowed_matchups") or [],
        "forecast_available_rows": _as_int(slate_ctx.get("forecast_available_rows")),
        "forecast_unavailable_rows": _as_int(slate_ctx.get("forecast_unavailable_rows")),
        "forecast_unavailable_by_reason": slate_ctx.get("forecast_unavailable_by_reason") or {},
        "forecast_unavailable_pitchers": slate_ctx.get("forecast_unavailable_pitchers") or [],
        "starter_market_lifecycle_summary": lifecycle.get("summary") or {},
        "starter_market_lifecycle_warnings": lifecycle.get("warnings") or [],
        "team_eval_context_as_of_date": team_eval.get("context_as_of_date"),
        "team_eval_rows_with_expected": _as_int(team_eval.get("rows_with_expected")),
        "team_eval_rows_with_actual": _as_int(team_eval.get("rows_with_actual")),
        "team_eval_coverage_pct": _as_float(team_eval.get("coverage_pct")),
        "team_eval_expected_avg": _as_float(team_eval.get("expected_team_hits_allowed_avg")),
        "team_eval_actual_avg": _as_float(team_eval.get("actual_offense_hits_avg")),
        "team_eval_residual_avg": _as_float(team_eval.get("residual_avg")),
        "team_eval_residual_total": _as_float(team_eval.get("residual_total")),
        "team_eval_mae": _as_float(team_eval.get("mae")),
        "team_eval_rmse": _as_float(team_eval.get("rmse")),
        "team_eval_top_over": team_eval.get("top_over_expected_matchups") or [],
        "team_eval_top_under": team_eval.get("top_under_expected_matchups") or [],
    }


def _avg_float(rows: Sequence[Dict[str, Any]], key: str) -> Optional[float]:
    values = [_as_float(row.get(key)) for row in rows]
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def _latest_hits_environment_snapshot(date_value: str) -> Optional[Path]:
    date_key = _date_key(date_value)
    if not date_key:
        return None
    root = Path("artifacts/analysis/mlb/hits_environment_snapshots") / date_key
    paths = sorted(root.glob(f"mlb_hits_environment_hits_allowed_rows_{date_key}__*.csv"))
    return paths[-1] if paths else None


def _load_hits_environment_snapshot_summary(path: Path) -> Dict[str, Any]:
    rows, err = _load_csv_rows(path)
    if err or not rows:
        return {}
    starter_rows = [row for row in rows if _as_float(row.get("expected_hits_allowed_matchup")) is not None]
    team_rows = [row for row in rows if _as_float(row.get("expected_team_hits_allowed_matchup")) is not None]
    bullpen_rows = [row for row in rows if _as_float(row.get("bullpen_hits_allowed_form_blended")) is not None]
    unavailable_pitchers = [
        dict(row)
        for row in rows
        if str(row.get("forecast_diagnostic") or row.get("forecast_note") or "").strip()
        and _as_float(row.get("expected_hits_allowed_matchup")) is None
    ]
    unavailable_reasons: Dict[str, int] = {}
    for row in unavailable_pitchers:
        reason = str(row.get("forecast_diagnostic") or row.get("forecast_note") or "unknown").strip() or "unknown"
        unavailable_reasons[reason] = unavailable_reasons.get(reason, 0) + 1

    def sort_rows(items: Sequence[Dict[str, Any]], key: str, reverse: bool) -> List[Dict[str, Any]]:
        return sorted(
            (dict(row) for row in items),
            key=lambda row: _as_float(row.get(key)) if _as_float(row.get(key)) is not None else float("-inf"),
            reverse=reverse,
        )

    return {
        "fallback_snapshot_path": str(path),
        "slate_rows": len(rows),
        "slate_rows_with_expected": len(starter_rows),
        "slate_avg_expected_matchup": _avg_float(starter_rows, "expected_hits_allowed_matchup"),
        "slate_avg_line_minus_expected": _avg_float(starter_rows, "line_minus_expected_hits_allowed_matchup"),
        "slate_rows_with_bullpen_blended": len(bullpen_rows),
        "slate_avg_bullpen_blended": _avg_float(bullpen_rows, "bullpen_hits_allowed_form_blended"),
        "slate_rows_with_team_expected": len(team_rows),
        "slate_avg_team_expected": _avg_float(team_rows, "expected_team_hits_allowed_matchup"),
        "top_expected_matchups": sort_rows(starter_rows, "expected_hits_allowed_matchup", True),
        "lowest_expected_matchups": sort_rows(starter_rows, "expected_hits_allowed_matchup", False),
        "top_expected_team_matchups": sort_rows(team_rows, "expected_team_hits_allowed_matchup", True),
        "lowest_expected_team_matchups": sort_rows(team_rows, "expected_team_hits_allowed_matchup", False),
        "forecast_unavailable_rows": len(unavailable_pitchers),
        "forecast_unavailable_by_reason": unavailable_reasons,
        "forecast_unavailable_pitchers": unavailable_pitchers,
    }


def _apply_hits_environment_snapshot_fallback(hits_env: Dict[str, Any], current_slate_date: str) -> Dict[str, Any]:
    if hits_env.get("top_expected_matchups") or hits_env.get("slate_rows_with_expected"):
        return hits_env
    if str(hits_env.get("status") or "").lower() != "fail":
        return hits_env
    snapshot = _latest_hits_environment_snapshot(current_slate_date)
    if not snapshot:
        return hits_env
    fallback = _load_hits_environment_snapshot_summary(snapshot)
    if not fallback or not fallback.get("slate_rows_with_expected"):
        return hits_env
    out = dict(hits_env)
    out.update({k: v for k, v in fallback.items() if k != "fallback_snapshot_path"})
    out["fallback_used"] = True
    out["fallback_source_file"] = fallback["fallback_snapshot_path"]
    out["fallback_reason"] = "latest hits environment JSON failed without slate_hits_allowed_context"
    out["warnings"] = list(out.get("warnings") or []) + ["using_current_date_hits_environment_snapshot_fallback"]
    return out


def _extract_hits_environment_team_eval(team_eval: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "team_eval_context_as_of_date": team_eval.get("context_as_of_date"),
        "team_eval_rows_with_expected": _as_int(team_eval.get("rows_with_expected")),
        "team_eval_rows_with_actual": _as_int(team_eval.get("rows_with_actual")),
        "team_eval_coverage_pct": _as_float(team_eval.get("coverage_pct")),
        "team_eval_expected_avg": _as_float(team_eval.get("expected_team_hits_allowed_avg")),
        "team_eval_actual_avg": _as_float(team_eval.get("actual_offense_hits_avg")),
        "team_eval_residual_avg": _as_float(team_eval.get("residual_avg")),
        "team_eval_residual_total": _as_float(team_eval.get("residual_total")),
        "team_eval_mae": _as_float(team_eval.get("mae")),
        "team_eval_rmse": _as_float(team_eval.get("rmse")),
        "team_eval_top_over": team_eval.get("top_over_expected_matchups") or [],
        "team_eval_top_under": team_eval.get("top_under_expected_matchups") or [],
    }


def _apply_hits_environment_team_eval_history_fallback(
    hits_env: Dict[str, Any],
    *,
    current_slate_date: str,
    completed_slate_date: str,
    history_jsonl: Path = Path("artifacts/analysis/mlb/mlb_hits_environment_history.jsonl"),
) -> Dict[str, Any]:
    if hits_env.get("team_eval_top_over") or hits_env.get("team_eval_top_under"):
        return hits_env
    if str(hits_env.get("status") or "").lower() != "fail":
        return hits_env

    current_key = _date_key(current_slate_date)
    completed_key = _date_key(completed_slate_date)
    if not current_key or not completed_key:
        return hits_env

    for payload in reversed(_load_jsonl_objects(history_jsonl)):
        if str(payload.get("status") or "").lower() != "pass":
            continue
        if _date_key(payload.get("requested_as_of_date")) != current_key:
            continue
        if _date_key(payload.get("evaluation_date")) != completed_key:
            continue
        team_eval = payload.get("team_hits_allowed_matchup_evaluation") or {}
        if not isinstance(team_eval, dict):
            continue
        top_over = team_eval.get("top_over_expected_matchups") or []
        top_under = team_eval.get("top_under_expected_matchups") or []
        if not top_over and not top_under:
            continue

        out = dict(hits_env)
        out.update(_extract_hits_environment_team_eval(team_eval))
        out["team_eval_fallback_used"] = True
        out["team_eval_fallback_source_file"] = str(history_jsonl)
        out["team_eval_fallback_generated_at_utc"] = str(payload.get("generated_at_utc") or "")
        out["team_eval_fallback_reason"] = (
            "latest hits environment JSON failed without team_hits_allowed_matchup_evaluation"
        )
        out["warnings"] = list(out.get("warnings") or []) + ["using_hits_environment_history_team_eval_fallback"]
        return out
    return hits_env


def _extract_qc_bottom_order_watch(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    rows = [row for row in (js.get("windows") or []) if isinstance(row, dict)]

    def find(group: str, window: str) -> Dict[str, Any]:
        for row in rows:
            if str(row.get("watch_group") or "") == group and str(row.get("window") or "") == window:
                return row
        return {}

    target = "qc_only_bottom_order_under_0.5"
    comparison = "qc_only_non_bottom_order_under_0.5"
    overlap = "overlap_bottom_order_under_0.5"
    target_windows = {window: find(target, window) for window in ("full_history", "last_30_days", "last_14_days", "last_7_days")}
    comparison_full = find(comparison, "full_history")
    overlap_full = find(overlap, "full_history")
    return {
        "generated_at": js.get("generated_at"),
        "latest_reconcile_date": js.get("latest_reconcile_date"),
        "recommendation": js.get("recommendation") or "",
        "recommendation_reason": js.get("recommendation_reason") or "",
        "group_diagnostics": js.get("group_diagnostics") or {},
        "target_group": target,
        "comparison_group": comparison,
        "overlap_group": overlap,
        "target_windows": target_windows,
        "comparison_full_history": comparison_full,
        "overlap_full_history": overlap_full,
        "windows": rows,
    }


def _extract_overlap_watch(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    comp = js.get("composition_diagnostics") or {}
    periods = comp.get("periods") or {}

    def period(name: str) -> Dict[str, Any]:
        row = periods.get(name) or {}
        return dict(row) if isinstance(row, dict) else {}

    last7 = period("last_7")
    full = period("full_history")
    return {
        "latest_reconcile_date_found": js.get("latest_reconcile_date_found") or "",
        "latest_overlap_date_included": js.get("latest_overlap_date_included") or "",
        "stale": bool(js.get("stale")),
        "latest_completed_slate": comp.get("latest_completed_slate") or "",
        "composition_drift_flag": comp.get("composition_drift_flag") or "",
        "composition_drift_reasons": comp.get("composition_drift_reasons") or [],
        "action_annotation": comp.get("action_annotation") or "",
        "status": comp.get("status") or "",
        "full_history": full,
        "pre_2026_05_29": period("pre_2026_05_29"),
        "from_2026_05_29_onward": period("from_2026_05_29_onward"),
        "last_30": period("last_30"),
        "last_14": period("last_14"),
        "last_7": last7,
        "last_7_row_count": last7.get("rows"),
        "bottom_order_share_full_history": full.get("bottom_order_share"),
        "bottom_order_share_last_7": last7.get("bottom_order_share"),
        "avg_qc_score_full_history": full.get("avg_qc_score"),
        "avg_qc_score_last_7": last7.get("avg_qc_score"),
        "avg_v2_ranking_score_full_history": full.get("avg_v2_ranking_score"),
        "avg_v2_ranking_score_last_7": last7.get("avg_v2_ranking_score"),
        "qc_probability_55_60_share_last_7": last7.get("qc_probability_55_60_share"),
        "odds_minus_150_to_minus_120_share_last_7": last7.get("odds_minus_150_to_minus_120_share"),
        "performance": js.get("performance") or [],
        "totals": js.get("totals") or {},
    }


def _extract_hits_o15_watch_candidates(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    items = [dict(row) for row in rows if isinstance(row, dict)]
    tier_counts: Dict[str, int] = {}
    for row in items:
        tier = str(row.get("combined_tier") or "missing")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    def sort_key(row: Dict[str, Any]) -> tuple[int, int, float, str]:
        hitter_rank = {"A": 0, "B": 1, "C": 2}.get(str(row.get("hitter_tier") or "C"), 9)
        pitcher_rank = {"A": 0, "B": 1, "C": 2, "D": 3, "U": 4}.get(str(row.get("pitcher_tier") or "U"), 9)
        qc_score = _as_float(row.get("qc_score"))
        return (
            hitter_rank,
            pitcher_rank,
            -(qc_score if qc_score is not None else -1.0),
            str(row.get("player_name") or ""),
        )

    return {
        "row_count": len(items),
        "tier_counts": dict(sorted(tier_counts.items())),
        "aa_count": tier_counts.get("A/A", 0),
        "ab_count": tier_counts.get("A/B", 0),
        "top_candidates": sorted(items, key=sort_key)[:5],
    }


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _extract_hits_o15_layered_candidates(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    items = [dict(row) for row in rows if isinstance(row, dict)]

    def sort_key(row: Dict[str, Any]) -> tuple[int, int, float, float, float, float, str]:
        hitter_rank = {"A": 0, "B": 1, "C": 2}.get(str(row.get("hitter_tier") or "C"), 9)
        pitcher_rank = {"A": 0, "B": 1, "C": 2, "D": 3, "U": 4}.get(str(row.get("pitcher_tier") or "U"), 9)
        d7 = _as_float(row.get("d7_hits_rate"))
        d15 = _as_float(row.get("d15_hits_rate"))
        starter = _as_float(row.get("starter_expected_hits_allowed"))
        market = _as_float(row.get("market_price"))
        return (
            hitter_rank,
            pitcher_rank,
            -(d7 if d7 is not None else -1.0),
            -(d15 if d15 is not None else -1.0),
            -(starter if starter is not None else -1.0),
            -(market if market is not None else -9999.0),
            str(row.get("player") or row.get("player_name") or ""),
        )

    def top(layer: str, limit: int = 3) -> List[Dict[str, Any]]:
        return sorted(
            [row for row in items if str(row.get("layer_label") or "") == layer],
            key=sort_key,
        )[:limit]

    def tier_count(layer: str, tier: str) -> int:
        return sum(
            1
            for row in items
            if str(row.get("layer_label") or "") == layer and str(row.get("combined_tier") or "") == tier
        )

    return {
        "row_count": len(items),
        "d7_hot_count": sum(1 for row in items if _truthy(row.get("d7_hot_candidate"))),
        "d7_d15_count": sum(
            1
            for row in items
            if _truthy(row.get("d7_hot_candidate")) and _truthy(row.get("d15_consistent_candidate"))
        ),
        "d7_d15_favorable_starter_count": sum(
            1
            for row in items
            if _truthy(row.get("d7_hot_candidate"))
            and _truthy(row.get("d15_consistent_candidate"))
            and _truthy(row.get("favorable_starter_candidate"))
        ),
        "qc_watch_count": sum(1 for row in items if _truthy(row.get("watch_candidate"))),
        "aa_counts": {
            "layer_4_qc_d7_d15_starter": tier_count("layer_4_qc_d7_d15_starter", "A/A"),
            "layer_3_d7_d15_starter_non_qc": tier_count("layer_3_d7_d15_starter_non_qc", "A/A"),
            "layer_2_d7_d15_no_favorable_starter": tier_count("layer_2_d7_d15_no_favorable_starter", "A/A"),
            "layer_1_d7_hot_not_d15_consistent": tier_count("layer_1_d7_hot_not_d15_consistent", "A/A"),
        },
        "ab_counts": {
            "layer_4_qc_d7_d15_starter": tier_count("layer_4_qc_d7_d15_starter", "A/B"),
            "layer_3_d7_d15_starter_non_qc": tier_count("layer_3_d7_d15_starter_non_qc", "A/B"),
            "layer_2_d7_d15_no_favorable_starter": tier_count("layer_2_d7_d15_no_favorable_starter", "A/B"),
            "layer_1_d7_hot_not_d15_consistent": tier_count("layer_1_d7_hot_not_d15_consistent", "A/B"),
        },
        "top_qc_watch_candidates": top("layer_4_qc_d7_d15_starter", 3),
        "top_non_qc_d7_d15_favorable_starter": top("layer_3_d7_d15_starter_non_qc", 3),
    }


def _extract_hits_u15_favorite_audit(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    items = [dict(row) for row in rows if isinstance(row, dict)]

    def sort_key(row: Dict[str, Any]) -> tuple[int, int, float, float, float, float, str]:
        layer_rank = {
            "layer_4_qc_d7_d15_tough_starter": 0,
            "layer_3_d7_d15_tough_starter_non_qc": 1,
            "layer_2_d7_d15_no_tough_starter": 2,
            "layer_1_d7_cold_not_d15_consistent": 3,
            "all_u15_other": 4,
        }.get(str(row.get("layer_label") or ""), 9)
        hitter_rank = {"A": 0, "B": 1, "C": 2}.get(str(row.get("hitter_tier") or "C"), 9)
        pitcher_rank = {"A": 0, "B": 1, "C": 2, "D": 3, "U": 4}.get(str(row.get("pitcher_tier") or "U"), 9)
        d7 = _as_float(row.get("d7_hits_rate"))
        d15 = _as_float(row.get("d15_hits_rate"))
        starter = _as_float(row.get("starter_expected_hits_allowed"))
        model_prob = _as_float(row.get("model_prob"))
        return (
            layer_rank,
            hitter_rank,
            pitcher_rank,
            d7 if d7 is not None else 999.0,
            d15 if d15 is not None else 999.0,
            starter if starter is not None else 999.0,
            -(model_prob if model_prob is not None else -1.0),
            str(row.get("player") or row.get("player_name") or ""),
        )

    def top(layer: str, limit: int = 3) -> List[Dict[str, Any]]:
        return sorted(
            [row for row in items if str(row.get("layer_label") or "") == layer],
            key=sort_key,
        )[:limit]

    def tier_count(layer: str, tier: str) -> int:
        return sum(
            1
            for row in items
            if str(row.get("layer_label") or "") == layer and str(row.get("combined_tier") or "") == tier
        )

    return {
        "row_count": len(items),
        "d7_cold_count": sum(1 for row in items if _truthy(row.get("d7_cold_candidate"))),
        "d7_d15_cold_count": sum(
            1
            for row in items
            if _truthy(row.get("d7_cold_candidate")) and _truthy(row.get("d15_cold_consistent_candidate"))
        ),
        "d7_d15_tough_starter_count": sum(
            1
            for row in items
            if _truthy(row.get("d7_cold_candidate"))
            and _truthy(row.get("d15_cold_consistent_candidate"))
            and _truthy(row.get("tough_starter_candidate"))
        ),
        "qc_watch_count": sum(1 for row in items if _truthy(row.get("watch_candidate"))),
        "aa_counts": {
            "layer_4_qc_d7_d15_tough_starter": tier_count("layer_4_qc_d7_d15_tough_starter", "A/A"),
            "layer_3_d7_d15_tough_starter_non_qc": tier_count("layer_3_d7_d15_tough_starter_non_qc", "A/A"),
            "layer_2_d7_d15_no_tough_starter": tier_count("layer_2_d7_d15_no_tough_starter", "A/A"),
            "layer_1_d7_cold_not_d15_consistent": tier_count("layer_1_d7_cold_not_d15_consistent", "A/A"),
        },
        "ab_counts": {
            "layer_4_qc_d7_d15_tough_starter": tier_count("layer_4_qc_d7_d15_tough_starter", "A/B"),
            "layer_3_d7_d15_tough_starter_non_qc": tier_count("layer_3_d7_d15_tough_starter_non_qc", "A/B"),
            "layer_2_d7_d15_no_tough_starter": tier_count("layer_2_d7_d15_no_tough_starter", "A/B"),
            "layer_1_d7_cold_not_d15_consistent": tier_count("layer_1_d7_cold_not_d15_consistent", "A/B"),
        },
        "top_qc_watch_candidates": top("layer_4_qc_d7_d15_tough_starter", 3),
        "top_non_qc_d7_d15_tough_starter": top("layer_3_d7_d15_tough_starter_non_qc", 3),
    }


def _extract_hits_o15_alternate_discovery(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    items = [dict(row) for row in rows if isinstance(row, dict)]

    def sort_key(row: Dict[str, Any]) -> tuple[int, int, float, float, float, float, str]:
        layer_rank = {
            "alternate_layer_a_d7_d15_starter": 0,
            "alternate_layer_b_d7_d15": 1,
            "alternate_layer_c_d7_hot": 2,
            "alternate_other": 3,
        }.get(str(row.get("alternate_layer") or ""), 9)
        hitter_rank = {"A": 0, "B": 1, "C": 2}.get(str(row.get("hitter_tier") or "C"), 9)
        pitcher_rank = {"A": 0, "B": 1, "C": 2, "D": 3, "U": 4}.get(str(row.get("pitcher_tier") or "U"), 9)
        d7 = _as_float(row.get("d7_hits_rate"))
        d15 = _as_float(row.get("d15_hits_rate"))
        starter = _as_float(row.get("starter_expected_hits_allowed"))
        price = _as_float(row.get("best_over_price"))
        return (
            layer_rank,
            hitter_rank,
            pitcher_rank,
            -(d7 if d7 is not None else -1.0),
            -(d15 if d15 is not None else -1.0),
            -(starter if starter is not None else -1.0),
            -(price if price is not None else -9999.0),
            str(row.get("player") or row.get("player_name") or ""),
        )

    return {
        "row_count": len(items),
        "d7_hot_count": sum(1 for row in items if _truthy(row.get("d7_hot_candidate"))),
        "d7_d15_count": sum(
            1
            for row in items
            if _truthy(row.get("d7_hot_candidate")) and _truthy(row.get("d15_consistent_candidate"))
        ),
        "d7_d15_starter_count": sum(
            1
            for row in items
            if _truthy(row.get("d7_hot_candidate"))
            and _truthy(row.get("d15_consistent_candidate"))
            and _truthy(row.get("favorable_starter_candidate"))
        ),
        "top_candidates": sorted(items, key=sort_key)[:3],
    }


def _extract_review_aid_performance(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    return dict(js)


def _extract_total_bases_shadow_summary(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    return dict(js)


def _extract_total_bases_shadow_evaluation(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    return dict(js)


def _fetch_today_workspace_status(slate_date: str) -> Tuple[Dict[str, Any], Optional[str]]:
    attempted_function = "backend.app.services.mlb.today_workspace_service.fetch_today_workspace"
    attempted_url = f"/api/mlb/today/workspace?slate_date={slate_date}&limit=1&offset=0"
    status: Dict[str, Any] = {
        "requested_slate_date": slate_date,
        "active_slate_date": None,
        "is_ready": False,
        "row_count": 0,
        "last_updated": None,
        "status": "fail",
        "reason": "not_staged",
        "diagnostics": {
            "attempted_function": attempted_function,
            "attempted_url": attempted_url,
            "slate_date_requested": slate_date,
            "exception_type": "",
            "exception_message": "",
            "failure_classification": "",
            "retry_attempted": False,
            "retry_succeeded": False,
        },
    }
    try:
        from backend.app.services.mlb.today_workspace_service import fetch_today_workspace
    except Exception as exc:
        diag = status["diagnostics"]
        diag.update(
            {
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
                "failure_classification": "import_or_environment_issue",
            }
        )
        status["reason"] = f"error:{type(exc).__name__}:{str(exc)}"
        return status, f"error:{type(exc).__name__}"

    def _call_workspace() -> Dict[str, Any]:
        return fetch_today_workspace(slate_date=slate_date, limit=1, offset=0)

    last_exc: Optional[Exception] = None
    for attempt in range(2):
        if attempt == 1:
            status["diagnostics"]["retry_attempted"] = True
            time.sleep(0.2)
        try:
            payload = _call_workspace()
            if attempt == 1:
                status["diagnostics"]["retry_succeeded"] = True
            break
        except Exception as exc:
            last_exc = exc
    else:
        exc = last_exc or RuntimeError("unknown workspace fetch failure")
        exc_type = type(exc).__name__
        exc_message = str(exc)
        msg_lower = exc_message.lower()
        if (
            "not configured" in msg_lower
            or "database_url" in msg_lower
            or "supabase_db_url" in msg_lower
            or "nodename nor servname provided" in msg_lower
            or "could not translate host name" in msg_lower
        ):
            classification = "connection/env issue"
        elif "does not exist" in msg_lower or "undefinedtable" in msg_lower or "undefined column" in msg_lower:
            classification = "missing table/view or bad query"
        elif "timeout" in msg_lower or "timed out" in msg_lower:
            classification = "timeout"
        elif exc_type == "OperationalError":
            classification = "transient DB/API failure or connection issue"
        else:
            classification = "bad query or service failure"
        status["diagnostics"].update(
            {
                "exception_type": exc_type,
                "exception_message": exc_message,
                "failure_classification": classification,
            }
        )
        status["reason"] = f"error:{exc_type}:{exc_message}"
        return status, f"error:{exc_type}"

    try:
        requested = str(payload.get("requested_slate_date") or slate_date)
        active = payload.get("active_slate_date")
        is_ready = bool(payload.get("is_ready"))
        total_rows = _as_int(payload.get("total"))
        if total_rows is None:
            total_rows = _as_int(payload.get("count")) or 0
        last_updated = payload.get("last_updated")
        if isinstance(last_updated, datetime):
            last_updated = last_updated.isoformat()

        reasons: List[str] = []
        if not is_ready:
            reasons.append("not_staged")
        if str(active or "") != requested:
            reasons.append("wrong_active_date")
        if int(total_rows) <= 0:
            reasons.append("zero_rows")

        status.update(
            {
                "requested_slate_date": requested,
                "active_slate_date": active,
                "is_ready": is_ready,
                "row_count": int(total_rows),
                "last_updated": last_updated,
                "status": "pass" if not reasons else "fail",
                "reason": ",".join(reasons) if reasons else "ok",
            }
        )
        return status, None
    except Exception as exc:
        status["diagnostics"].update(
            {
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
                "failure_classification": "response parsing failure",
            }
        )
        status["reason"] = f"error:{type(exc).__name__}:{str(exc)}"
        return status, f"error:{type(exc).__name__}"


def _bvp_prewarm_failure_note(*, current_slate_date: str, completed_slate_date: str) -> str:
    out_log = Path("artifacts/ops/mlb_bvp_prewarm_daily.out.log")
    err_log = Path("artifacts/ops/mlb_bvp_prewarm_daily.err.log")
    try:
        out_text = out_log.read_text(encoding="utf-8") if out_log.exists() else ""
        err_text = err_log.read_text(encoding="utf-8") if err_log.exists() else ""
    except Exception:
        return ""

    candidates = [d for d in (current_slate_date, completed_slate_date) if d]
    started = [d for d in candidates if f"MLB_DATE_ET={d}" in out_text]
    failed = [d for d in candidates if f"date={d}" in err_text or f"/date={d}" in err_text or f"MLB_DATE_ET={d}" in err_text]
    if not started and not failed:
        return ""

    err_lower = err_text.lower()
    if "nodename nor servname provided" in err_lower or "failed to resolve" in err_lower:
        reason = "DNS/name resolution failure contacting statsapi.mlb.com"
    elif "certificate verify failed" in err_lower or "sslcertverificationerror" in err_lower:
        reason = "TLS certificate verification failure contacting statsapi.mlb.com"
    elif "connectionerror" in err_lower or "maxretryerror" in err_lower:
        reason = "network/API connection failure contacting statsapi.mlb.com"
    else:
        reason = "prewarm producer failed; inspect BvP prewarm logs"

    attempted = ",".join(started or failed)
    return (
        f"BvP prewarm producer attempted date(s) {attempted} but failed before impact refresh: {reason}. "
        f"logs: {out_log}; {err_log}"
    )


def _workspace_matches_slate(workspace: Dict[str, Any], slate_date: str) -> bool:
    requested = _date_key(workspace.get("requested_slate_date"))
    active = _date_key(workspace.get("active_slate_date"))
    return slate_date in {requested, active}


def _cached_today_workspace_status(
    *,
    slate_date: str,
    out_json_path: Path,
    history_jsonl_path: Path,
    generated_at_utc: str,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """Reuse a previous workspace status during render-only brief passes.

    The final LaunchAgent render runs after current-slate artifacts are produced
    and should not re-open DB/API dependencies. Prefer the most recent same-slate
    passing workspace status from brief history, since a local render may already
    have overwritten latest.json with an environment-specific failure.
    """

    def _decorate_cached(payload: Dict[str, Any], source: str, source_generated_at: str = "") -> Dict[str, Any]:
        cached = json.loads(json.dumps(payload))
        diagnostics = cached.setdefault("diagnostics", {})
        diagnostics.update(
            {
                "workspace_fetch_skipped": True,
                "workspace_cache_reused": True,
                "workspace_cache_source": source,
                "workspace_cached_generated_at": source_generated_at,
                "refresh_mode": "render_only",
            }
        )
        return cached

    history_rows = _load_jsonl_objects(history_jsonl_path)
    for row in reversed(history_rows):
        if _date_key(row.get("current_slate_date")) != slate_date:
            continue
        workspace = row.get("today_workspace")
        if not isinstance(workspace, dict):
            continue
        if workspace.get("status") != "pass" or not _workspace_matches_slate(workspace, slate_date):
            continue
        return _decorate_cached(
            workspace,
            str(history_jsonl_path),
            str(row.get("generated_at_utc") or ""),
        ), None

    latest_raw, _ = _load_json(out_json_path)
    if isinstance(latest_raw, dict):
        workspace = latest_raw.get("today_workspace")
        if (
            isinstance(workspace, dict)
            and workspace.get("status") == "pass"
            and _workspace_matches_slate(workspace, slate_date)
        ):
            return _decorate_cached(
                workspace,
                str(out_json_path),
                str(latest_raw.get("generated_at_utc") or ""),
            ), None

    status: Dict[str, Any] = {
        "requested_slate_date": slate_date,
        "active_slate_date": None,
        "is_ready": None,
        "row_count": 0,
        "last_updated": None,
        "status": "not_refreshed",
        "reason": "not refreshed in final render",
        "diagnostics": {
            "workspace_fetch_skipped": True,
            "workspace_cache_reused": False,
            "workspace_cache_source": "",
            "refresh_mode": "render_only",
            "generated_at_utc": generated_at_utc,
            "failure_classification": "not refreshed in final render",
            "retry_attempted": False,
            "retry_succeeded": False,
        },
    }
    return status, "not_refreshed"


def _derive_overall_status(
    *,
    pipeline: Dict[str, Any],
    ops: Dict[str, Any],
    postgrade: Dict[str, Any],
    hits_env: Dict[str, Any],
    model_vs_fade: Optional[Dict[str, Any]] = None,
    freshness_audit: Optional[Sequence[Dict[str, Any]]] = None,
) -> Tuple[str, List[str]]:
    issues: List[str] = []
    fatal = False
    actionable_freshness = [
        row
        for row in (freshness_audit or [])
        if str(row.get("freshness_status") or "")
        in {
            "stale-unexpected",
            "missing-unexpected",
            "refresh_failed",
            "dependency_missing",
            "feature-lineage-warn",
            "feature-lineage-fail",
        }
    ]
    if actionable_freshness:
        issues.append(
            "actionable_source_health="
            + ",".join(str(row.get("section") or "unknown") for row in actionable_freshness)
        )

    if pipeline and pipeline.get("status") not in {"pass", "ok"}:
        fatal = True
        issues.append(f"pipeline_status={pipeline.get('status')}")
    if ops and ops.get("status") not in {"pass", "ok"}:
        fatal = True
        issues.append(f"ops_status={ops.get('status')}")
    postgrade_alerts = [a for a in (postgrade.get("alerts") or []) if isinstance(a, dict)]
    new_critical_count = sum(
        1
        for alert in postgrade_alerts
        if str(alert.get("severity") or "").lower() == "critical" and bool(alert.get("alert_is_new_today"))
    )
    persistent_critical_count = sum(
        1
        for alert in postgrade_alerts
        if str(alert.get("severity") or "").lower() == "critical" and bool(alert.get("alert_is_persistent"))
    )
    if new_critical_count > 0:
        fatal = True
        issues.append(f"new_critical_alerts={new_critical_count}")
    if persistent_critical_count > 0:
        issues.append(f"persistent_actionable_critical_alerts={persistent_critical_count}")
    if int(postgrade.get("warning_count") or 0) > 0:
        issues.append(f"warning_alerts={postgrade.get('warning_count')}")
    model_alert = (model_vs_fade or {}).get("alert_state") or {}
    if model_alert.get("alert_active"):
        age = model_alert.get("alert_age_days")
        if model_alert.get("alert_is_new_today"):
            issues.append("new_model_vs_fade_alert=1")
        else:
            issues.append(f"persistent_actionable_model_vs_fade_alert_age_days={age}")
    if hits_env and hits_env.get("warnings"):
        issues.append(f"hits_env_warnings={len(hits_env.get('warnings') or [])}")

    if fatal:
        return "fail", issues
    if issues:
        return "warn", issues
    return "pass", issues


def _csv_props(rows: Sequence[Dict[str, Any]]) -> str:
    out: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw = str(row.get("prop_type") or "").strip().lower()
        if not raw or raw in out:
            continue
        out.append(raw)
    return ",".join(out)


def _derive_path_forward(
    *,
    report_date: str,
    overall_status: str,
    pipeline: Dict[str, Any],
    ops: Dict[str, Any],
    postgrade: Dict[str, Any],
    model_vs_fade: Dict[str, Any],
    bvp_impact: Dict[str, Any],
    hits_env: Dict[str, Any],
) -> List[Dict[str, str]]:
    actions: List[Dict[str, str]] = []
    postgrade_report_date = str(postgrade.get("report_date") or report_date).strip() or report_date

    pipeline_status = str(pipeline.get("status") or "").strip().lower()
    ops_status = str(ops.get("status") or "").strip().lower()
    if pipeline_status not in {"pass", "ok"} or ops_status not in {"pass", "ok"}:
        actions.append(
            {
                "title": "Stabilize daily lane first",
                "description": (
                    f"Daily health is not green (pipeline={pipeline.get('status')}, ops={ops.get('status')}). "
                    "Re-run with incident payload capture before any promotion decisions."
                ),
                "command": f"make mlb-prod12-daily-gate-incident MLB_BASE_URL=<url> MLB_DATE={report_date}",
            }
        )

    if int(postgrade.get("critical_count") or 0) > 0 or bool(model_vs_fade.get("fade_beating_model_alert")):
        delta_pct = _pct(model_vs_fade.get("delta_fade_minus_model_1u"))
        actions.append(
            {
                "title": "Run ROOT-CAUSE checks on model vs fade",
                "description": (
                    "Model-vs-fade alert is active "
                    f"(delta fade-model {delta_pct}). Verify paired results and open incident context."
                ),
                "command": (
                    f"make mlb-model-vs-fade MLB_POST_GRADE_DATE={postgrade_report_date} "
                    "&& make mlb-prod12-incident"
                ),
            }
        )

    degraded = [d for d in (pipeline.get("degraded_prop_lanes") or []) if isinstance(d, dict)]
    degraded_props_csv = _csv_props(degraded)
    if degraded_props_csv:
        actions.append(
            {
                "title": "Retrain degraded prop lanes with market-only profile",
                "description": (
                    f"Current degraded lanes: {degraded_props_csv}. Retrain only impacted props, "
                    "then rerun candidate eval before considering publish."
                ),
                "command": (
                    f"make mlb-retrain-bol-market-only MLB_RETRAIN_BOL_PROP_TYPES=\"{degraded_props_csv}\" "
                    f"&& make mlb-candidate-eval-prod12 MLB_PROD12_CANDIDATE_PROP_TYPES=\"{degraded_props_csv}\""
                ),
            }
        )

    bvp_rows = int(bvp_impact.get("rows_evaluated") or 0)
    bvp_nonzero = int(bvp_impact.get("rows_nonzero_delta") or 0)
    if bvp_rows > 0 and bvp_nonzero == 0:
        actions.append(
            {
                "title": "Confirm BvP feature intent for this profile",
                "description": (
                    "BvP impact shows zero non-zero deltas. If market-only profile is expected, keep as-is. "
                    "If BvP should influence picks, validate BvP hydration path before next retrain."
                ),
                "command": "make mlb-feature-health-prod12",
            }
        )

    if hits_env.get("warnings"):
        actions.append(
            {
                "title": "Resolve hits-environment warnings",
                "description": "Hits environment emitted warnings; inspect context drift before tightening lane thresholds.",
                "command": "make mlb-prod12-health-report",
            }
        )

    actions.append(
        {
            "title": "Hold publish behind strict weekly phase2 gate",
            "description": (
                "After remedial steps, require strict weekly readiness + candidate checks to pass before any promotion."
            ),
            "command": f"make mlb-prod12-phase2-weekly-gate MLB_BASE_URL=<url> MLB_DATE={report_date}",
        }
    )

    if overall_status == "pass" and len(actions) == 1:
        actions.insert(
            0,
            {
                "title": "Keep standard monitoring cadence",
                "description": "No active red signals. Continue daily cycle and monitor quality drift.",
                "command": f"make mlb-prod12-daily-cycle MLB_BASE_URL=<url> MLB_DATE={report_date}",
            },
        )

    return actions


def _format_matchup_row(row: Dict[str, Any]) -> str:
    player = str(row.get("player_name") or "unknown")
    exp = _as_float(row.get("expected_hits_allowed_matchup"))
    base = _as_float(row.get("pitcher_expected_hits_allowed_weighted"))
    factor = _as_float(row.get("offense_factor_vs_league_clamped"))
    exp_s = f"{exp:.2f}" if exp is not None else "n/a"
    base_s = f"{base:.2f}" if base is not None else "n/a"
    factor_s = f"{factor:.3f}" if factor is not None else "n/a"
    return (
        f"- {player} ({row.get('pitcher_team')} vs {row.get('offense_team')}): "
        f"expected {exp_s}, pitcher_base {base_s}, offense_factor {factor_s}"
    )


def _format_team_matchup_row(row: Dict[str, Any]) -> str:
    player = str(row.get("player_name") or "unknown")
    team_exp = _as_float(row.get("expected_team_hits_allowed_matchup"))
    starter_exp = _as_float(row.get("expected_hits_allowed_matchup"))
    bullpen_addon = _as_float(row.get("bullpen_hits_allowed_form_blended"))
    team_exp_s = f"{team_exp:.2f}" if team_exp is not None else "n/a"
    starter_exp_s = f"{starter_exp:.2f}" if starter_exp is not None else "n/a"
    bullpen_addon_s = f"{bullpen_addon:.2f}" if bullpen_addon is not None else "n/a"
    return (
        f"- {player} ({row.get('pitcher_team')} vs {row.get('offense_team')}): "
        f"team_expected {team_exp_s} = starter_expected {starter_exp_s} + bullpen_add_on {bullpen_addon_s}"
    )


def _format_team_eval_row(row: Dict[str, Any]) -> str:
    player = str(row.get("player_name") or "unknown")
    expected = _as_float(row.get("expected_team_hits_allowed_matchup"))
    actual = _as_float(row.get("actual_offense_hits"))
    residual = _as_float(row.get("residual_actual_minus_expected_team"))
    expected_s = f"{expected:.2f}" if expected is not None else "n/a"
    actual_s = f"{actual:.2f}" if actual is not None else "n/a"
    residual_s = f"{residual:+.2f}" if residual is not None else "n/a"
    return (
        f"- {player} ({row.get('pitcher_team')} vs {row.get('offense_team')}): "
        f"expected_team {expected_s}, actual {actual_s}, residual {residual_s}"
    )


def _format_category_separator(label: str) -> str:
    return f"### {label}"


def _markdown_link_target(path: Path, *, relative_to_md: Path) -> str:
    try:
        base_dir = relative_to_md.parent if str(relative_to_md.parent) else Path(".")
        return os.path.relpath(path, start=base_dir)
    except Exception:
        return str(path)


def _format_artifact_markdown_link(label: str, raw_path: Any, *, relative_to_md: Path) -> str:
    path_text = str(raw_path or "").strip()
    if not path_text or path_text.lower() == "n/a":
        return f"{label}: `unavailable`"
    path = Path(path_text)
    target = _markdown_link_target(path, relative_to_md=relative_to_md)
    suffix = "" if path.exists() else " (missing)"
    return f"{label}: [{label}]({target}){suffix}"


def _append_rolling_candidate_obs_section(
    lines: List[str],
    payload: Dict[str, Any],
    *,
    ops_brief_md_path: Path,
) -> None:
    if not payload.get("enabled"):
        return
    lines.append("## Rolling Market-Late Candidate Observation")
    state = str(payload.get("source_state") or "unknown")
    lines.append(
        f"- {_format_artifact_markdown_link('Ops Brief input JSON', payload.get('source_path'), relative_to_md=ops_brief_md_path)}"
    )
    lines.append(
        f"- Observation mode: `{payload.get('rolling_observation_mode') or 'unknown'}` | "
        f"source mtime `{payload.get('rolling_observation_source_mtime') or 'n/a'}`"
    )
    if state != "ok":
        lines.append(f"- Status: `WARN` | {payload.get('warning') or state}")
        lines.append("")
        return
    lines.append("- Status: `OBSERVATION_ONLY` | production uploads unchanged")
    lines.append(
        f"- Latest run tag: `{payload.get('latest_run_tag') or 'n/a'}` | "
        f"runs inspected `{payload.get('runs_inspected', 'n/a')}`"
    )
    lines.append(
        f"- Ledger rows `{payload.get('ledger_rows', 'n/a')}` | "
        f"current projection rows `{payload.get('current_projection_rows', 'n/a')}` | "
        f"current eligible rows `{payload.get('current_eligible_rows', 'n/a')}` | "
        f"current Hits 1.5 rows `{payload.get('current_hits_15_rows', 'n/a')}`"
    )
    lines.append(
        f"- Late-discovered current rows `{payload.get('current_late_discovered_rows', 'n/a')}` | "
        f"historical disappeared rows `{payload.get('historical_disappeared_rows', 'n/a')}` | "
        f"reappeared rows `{payload.get('reappeared_rows', 'n/a')}` | "
        f"confirmed lineup overlays `{payload.get('confirmed_lineup_overlay_count', 'n/a')}`"
    )
    if "morning_candidates" in payload or "late_discovered_candidates" in payload:
        lines.append(
            f"- Morning candidates `{payload.get('morning_candidates', 'n/a')}` | "
            f"late-discovered candidates `{payload.get('late_discovered_candidates', 'n/a')}` | "
            f"current eligible late-discovered `{payload.get('current_eligible_late_discovered_candidates', 'n/a')}`"
        )
        lines.append(
            f"- Hits 1.5 morning `{payload.get('hits_15_morning_count', 'n/a')}` | "
            f"Hits 1.5 late-discovered `{payload.get('hits_15_late_discovered_count', 'n/a')}` | "
            f"Hits 1.5 current eligible `{payload.get('hits_15_current_eligible_count', 'n/a')}`"
        )
    if payload.get("delta_summary_csv"):
        lines.append(
            f"- {_format_artifact_markdown_link('Delta summary CSV', payload.get('delta_summary_csv'), relative_to_md=ops_brief_md_path)}"
        )
    lines.append(
        f"- {_format_artifact_markdown_link('Pivot source CSV', payload.get('pivot_source_csv'), relative_to_md=ops_brief_md_path)}"
    )
    lines.append(
        f"- {_format_artifact_markdown_link('Current projection CSV', payload.get('current_projection_csv'), relative_to_md=ops_brief_md_path)}"
    )
    lines.append(
        f"- {_format_artifact_markdown_link('Observation note', payload.get('rolling_observation_md') or payload.get('report_md'), relative_to_md=ops_brief_md_path)}"
    )
    if payload.get("upload_behavior"):
        lines.append(f"- Upload behavior: `{payload.get('upload_behavior')}`")
    lines.append("")


def build_markdown(
    *,
    ops_brief_md_path: Path,
    report_date: str,
    completed_slate_date: str,
    current_slate_date: str,
    generated_at_utc: str,
    overall_status: str,
    overall_issues: Sequence[str],
    pipeline: Dict[str, Any],
    ops: Dict[str, Any],
    postgrade: Dict[str, Any],
    model_vs_fade: Dict[str, Any],
    bvp_impact: Dict[str, Any],
    hits_env: Dict[str, Any],
    overlap_watch: Dict[str, Any],
    qc_bottom_order_watch: Dict[str, Any],
    hits_o15_watch_candidates: Dict[str, Any],
    hits_o15_layered_candidates: Dict[str, Any],
    hits_u15_favorite_audit: Dict[str, Any],
    hits_o15_alternate_discovery: Dict[str, Any],
    hits_15_tier_backtest: Dict[str, Any],
    review_aid_performance: Dict[str, Any],
    total_bases_shadow_summary: Dict[str, Any],
    total_bases_shadow_evaluation: Dict[str, Any],
    feature_lineage_health: Dict[str, Any],
    prop_regime: Dict[str, Any],
    model_performance: Dict[str, Any],
    reporting_alignment: Dict[str, Any],
    today_workspace: Dict[str, Any],
    rolling_candidate_obs: Dict[str, Any],
    path_forward: Sequence[Dict[str, str]],
    source_states: Dict[str, Any],
    freshness_audit: Sequence[Dict[str, Any]],
) -> str:
    freshness_by_section = {str(row.get("section") or ""): row for row in freshness_audit}

    def provenance(section: str) -> str:
        return f"- Source: {_fmt_meta(freshness_by_section.get(section) or {})}"

    def board_source_line(payload: Dict[str, Any], source_key: str) -> str:
        state = str(payload.get("source_state") or source_states.get(source_key) or "ok")
        path = str(payload.get("source_path") or "")
        if state == "ok":
            return f"- Source: `{path or 'ok'}`"
        if source_key == "hits_o15_alternate_discovery_csv" and state == "missing":
            return f"- Source: `OPTIONAL_MISSING: {path or source_key}`"
        if state == "missing":
            return f"- Source: `MISSING_INPUT: {path or source_key}`"
        return f"- Source: `{state}: {path or source_key}`"

    def board_source_ok(payload: Dict[str, Any], source_key: str) -> bool:
        return str(payload.get("source_state") or source_states.get(source_key) or "ok") == "ok"

    lines: List[str] = []
    lines.append(f"# MLB Daily Ops Brief — {report_date}")
    lines.append("")
    lines.append(f"- Generated (UTC): `{generated_at_utc}`")
    lines.append(f"- Completed slate date: `{completed_slate_date}`")
    lines.append(f"- Current slate date: `{current_slate_date}`")
    lines.append(f"- Overall Status: `{overall_status.upper()}`")
    if overall_issues:
        lines.append(f"- Issues: `{', '.join(overall_issues)}`")
    lines.append("")
    lines.append("## Morning Workflow Handoff")
    lines.append("")
    lines.append("- Real Ops Brief status: existing section order is preserved; full three-phase body rewrite remains prototype-only.")
    lines.append("- Phase 1 intent: System Readiness — can I trust today's platform?")
    lines.append(f"- Operational status: `{overall_status.upper()}`")
    lines.append("- Review Pipeline & Ops, source health, identity, freshness, feature lineage, invariants, and critical warnings before candidate review.")
    lines.append("")
    lines.append("- Phase 2 intent: Today's Baseball — what kind of baseball day is today?")
    lines.append("- Continue through the baseball context sections below before opening candidate CSVs.")
    lines.append("")
    lines.append("- Phase 3 handoff: Begin Candidate Review — transition from observation to decision.")
    lines.append("- Open Morning Workbench: [Morning Workbench](review_aids/performance/o15_morning_workbench.md)")
    lines.append("")
    lines.append("## Snapshot")
    lines.append(
        f"- Pipeline: `{pipeline.get('status','n/a')}` | Ops: `{ops.get('status','n/a')}` | "
        f"Postgrade alerts: `{postgrade.get('critical_count',0)} critical / {postgrade.get('warning_count',0)} warning`"
    )
    lines.append(
        f"- Model vs Fade ({model_vs_fade.get('window_game_date_min') or 'n/a'} to "
        f"{model_vs_fade.get('window_game_date_max') or 'n/a'}, paired={model_vs_fade.get('paired_bets','n/a')}): "
        f"model ROI `{_pct(model_vs_fade.get('model_roi_1u'))}` vs fade ROI `{_pct(model_vs_fade.get('fade_roi_1u'))}`"
    )
    lines.append(
        f"- Hits Environment: signal `{hits_env.get('league_signal','n/a')}`, "
        f"starter rows `{hits_env.get('starter_rows','n/a')}`, "
        f"slate expected rows `{hits_env.get('slate_rows_with_expected','n/a')}`"
    )
    lines.append("")

    lines.append("## Pipeline & Ops")
    lines.append(provenance("Pipeline & Ops"))
    lines.append(
        f"- Pipeline captured: `{pipeline.get('captured_at','n/a')}` | failures: `{len(pipeline.get('failures') or [])}`"
    )
    checks = pipeline.get("checks") or []
    if checks:
        lines.append("- Pipeline checks:")
        for c in checks:
            lines.append(f"  - `{c.get('name')}`: `{c.get('status')}`")
    degraded = pipeline.get("degraded_prop_lanes") or []
    if degraded:
        lines.append("- Degraded prop lanes:")
        for d in degraded:
            lines.append(
                f"  - `{d.get('prop_type')}`: `{d.get('reason')}` "
                f"(accuracy `{d.get('accuracy_pct')}` vs min `{d.get('min_accuracy_pct')}`; total `{d.get('total')}`)"
            )
    else:
        lines.append("- Degraded prop lanes: none")
    lines.append(
        f"- Ops captured: `{ops.get('captured_at','n/a')}` | status `{ops.get('status','n/a')}`"
    )
    for k in ("status", "health", "incident"):
        ck = (ops.get("checks") or {}).get(k) or {}
        if ck:
            lines.append(f"  - `{k}`: `{ck.get('status')}`")
    lines.append("")

    lines.append("## Postgrade Alerts")
    lines.append(provenance("Postgrade Alerts"))
    lines.append(
        f"- Report date: `{postgrade.get('report_date','n/a')}` | "
        f"alerts `{postgrade.get('alerts_count',0)}` "
        f"(critical `{postgrade.get('critical_count',0)}`, warning `{postgrade.get('warning_count',0)}`)"
    )
    alerts = postgrade.get("alerts") or []
    if alerts:
        for a in alerts[:8]:
            alert_label = a.get("code") or a.get("type") or "n/a"
            lines.append(
                f"- [{a.get('severity','n/a')}] `{alert_label}`: {a.get('message') or 'n/a'}"
            )
            lines.append(
                f"  - Alert state: source_date `{a.get('alert_source_date','n/a')}`, "
                f"generated_at `{a.get('alert_generated_at','n/a')}`, "
                f"last_changed_at `{a.get('alert_last_changed_at','n/a')}`, "
                f"age_days `{a.get('alert_age_days','n/a')}`, "
                f"new_today `{a.get('alert_is_new_today')}`, persistent `{a.get('alert_is_persistent')}`"
            )
            if a.get("recommendation"):
                lines.append(f"  - Recommendation: {a.get('recommendation')}")
    else:
        lines.append("- No active postgrade alerts.")
    lines.append("")

    lines.append("## Model vs Fade")
    lines.append(provenance("Model vs Fade"))
    lines.append(
        f"- Source window: `{model_vs_fade.get('window_game_date_min') or 'n/a'}` to "
        f"`{model_vs_fade.get('window_game_date_max') or 'n/a'}`"
    )
    lines.append(f"- Rows CSV: `{model_vs_fade.get('rows_csv') or 'n/a'}`")
    lines.append(f"- Paired bets: `{model_vs_fade.get('paired_bets','n/a')}`")
    lines.append(
        f"- Model: win rate `{_pct(model_vs_fade.get('model_win_rate'))}`, ROI `{_pct(model_vs_fade.get('model_roi_1u'))}`"
    )
    lines.append(
        f"- Fade: win rate `{_pct(model_vs_fade.get('fade_win_rate'))}`, ROI `{_pct(model_vs_fade.get('fade_roi_1u'))}`"
    )
    lines.append(
        f"- Delta (fade - model): `{_pct(model_vs_fade.get('delta_fade_minus_model_1u'))}` | "
        f"fade_beating_model_alert `{model_vs_fade.get('fade_beating_model_alert')}`"
    )
    alert_state = model_vs_fade.get("alert_state") or {}
    if alert_state:
        lines.append(
            f"- Alert state: active `{alert_state.get('alert_active')}`, "
            f"source_date `{alert_state.get('alert_source_date','n/a')}`, "
            f"generated_at `{alert_state.get('alert_generated_at','n/a')}`, "
            f"last_changed_at `{alert_state.get('alert_last_changed_at','n/a')}`, "
            f"age_days `{alert_state.get('alert_age_days','n/a')}`, "
            f"new_today `{alert_state.get('alert_is_new_today')}`, "
            f"persistent `{alert_state.get('alert_is_persistent')}`"
        )
    lines.append("")

    lines.append("## Prop Outlook Freshness")
    lines.append(provenance("Prop Outlook Freshness"))
    lines.append(f"- Regime CSV: `{prop_regime.get('path','n/a')}`")
    lines.append(
        f"- Max latest_usable_date: `{prop_regime.get('max_latest_usable_date','n/a')}` | "
        f"prop count `{prop_regime.get('prop_count','n/a')}` | "
        f"outs_recorded present `{prop_regime.get('outs_recorded_present')}`"
    )
    lines.append(
        f"- Reporting alignment CSV: `{reporting_alignment.get('path','n/a')}` | "
        f"stale_outlook_source count `{reporting_alignment.get('stale_outlook_source_count','n/a')}`"
    )
    lines.append("")

    lines.append("## Model Performance By Prop")
    lines.append(provenance("Model Performance By Prop"))
    lines.append(f"- Rolling summary CSV: `{model_performance.get('summary_path','n/a')}`")
    lines.append(f"- Daily performance CSV: `{model_performance.get('daily_path','n/a')}`")
    lines.append(
        f"- source_type `{model_performance.get('source_type','n/a')}` | "
        f"active prop count `{model_performance.get('active_prop_count','n/a')}` | "
        f"missing_reason count `{model_performance.get('missing_reason_count','n/a')}`"
    )
    lines.append(
        f"- Critical props: `{', '.join(model_performance.get('critical_props') or []) or 'none'}`"
    )
    lines.append(f"- Watch props: `{', '.join(model_performance.get('watch_props') or []) or 'none'}`")
    lines.append("")

    lines.append("## Ranking/QC Overlap Watch")
    lines.append(provenance("Ranking/QC Overlap Watch"))
    full_overlap = overlap_watch.get("full_history") or {}
    last30_overlap = overlap_watch.get("last_30") or {}
    last14_overlap = overlap_watch.get("last_14") or {}
    last7_overlap = overlap_watch.get("last_7") or {}
    lines.append(
        f"- Action annotation: `{overlap_watch.get('action_annotation') or 'n/a'}` | "
        f"composition_drift_flag `{overlap_watch.get('composition_drift_flag') or 'n/a'}` | "
        f"reasons `{', '.join(overlap_watch.get('composition_drift_reasons') or []) or 'none'}`"
    )
    lines.append(
        f"- Latest completed slate `{overlap_watch.get('latest_completed_slate') or 'n/a'}` | "
        f"latest overlap date `{overlap_watch.get('latest_overlap_date_included') or 'n/a'}` | "
        f"stale `{overlap_watch.get('stale')}`"
    )
    lines.append(
        f"- Overlap ROI: full history `{_pct(full_overlap.get('roi'))}`, "
        f"last 30 `{_pct(last30_overlap.get('roi'))}`, "
        f"last 14 `{_pct(last14_overlap.get('roi'))}`, "
        f"last 7 `{_pct(last7_overlap.get('roi'))}`."
    )
    lines.append(
        f"- Last-7 rows `{last7_overlap.get('rows','n/a')}` / resolved `{last7_overlap.get('resolved_rows','n/a')}` | "
        f"WR `{_pct(last7_overlap.get('wr'))}` | units `{_num_fmt(last7_overlap.get('units'))}`."
    )
    lines.append(
        f"- Bottom-order share: full history `{_pct(overlap_watch.get('bottom_order_share_full_history'))}`, "
        f"last 7 `{_pct(overlap_watch.get('bottom_order_share_last_7'))}`."
    )
    lines.append(
        f"- Avg QC score: full history `{_num_fmt(overlap_watch.get('avg_qc_score_full_history'))}`, "
        f"last 7 `{_num_fmt(overlap_watch.get('avg_qc_score_last_7'))}`."
    )
    lines.append(
        f"- Avg V2 ranking score: full history `{_num_fmt(overlap_watch.get('avg_v2_ranking_score_full_history'))}`, "
        f"last 7 `{_num_fmt(overlap_watch.get('avg_v2_ranking_score_last_7'))}`."
    )
    lines.append(
        f"- Last-7 concentration: QC probability 55-60 share "
        f"`{_pct(overlap_watch.get('qc_probability_55_60_share_last_7'))}`; "
        f"odds -150 to -120 share `{_pct(overlap_watch.get('odds_minus_150_to_minus_120_share_last_7'))}`."
    )
    lines.append("")

    lines.append("## QC Bottom-Order Under Watch")
    lines.append(provenance("QC Bottom-Order Under Watch"))
    target_windows = qc_bottom_order_watch.get("target_windows") or {}
    watch_rows: List[Dict[str, Any]] = []
    for window in ("full_history", "last_30_days", "last_14_days", "last_7_days"):
        row = target_windows.get(window) or {}
        watch_rows.append(
            {
                "window": window,
                "bets": row.get("bets", "n/a"),
                "wr": _pct(row.get("wr")),
                "roi": _pct(row.get("roi")),
                "units": _num_fmt(row.get("units")),
                "avg_odds": _num_fmt(row.get("avg_odds")),
                "sample_warning": row.get("sample_warning", "n/a"),
                "drift_flag": row.get("drift_flag", "n/a"),
            }
        )
    lines.append(
        f"- Segment: `QC-only + bottom-order hitter + under 0.5` | "
        f"latest_reconcile_date `{qc_bottom_order_watch.get('latest_reconcile_date') or 'n/a'}`"
    )
    qc_diag = qc_bottom_order_watch.get("group_diagnostics") or {}
    qc_group_diag = qc_diag.get("groups") or {}
    lines.append(
        f"- Action annotation: `{qc_bottom_order_watch.get('recommendation') or 'n/a'}` | "
        f"{qc_bottom_order_watch.get('recommendation_reason') or 'n/a'}"
    )
    lines.append(
        f"- Qualifying-row cadence: latest target date `{qc_diag.get('target_latest_qualifying_date') or 'n/a'}`; "
        f"latest comparison date `{qc_diag.get('comparison_latest_qualifying_date') or 'n/a'}`; "
        f"new rows on latest completed slate target `{qc_diag.get('target_new_rows_latest_completed_slate', 0)}`, "
        f"comparisons `{qc_diag.get('comparison_new_rows_latest_completed_slate', 0)}`."
    )
    lines.append(
        f"- Last 7 qualifying rows: target `{qc_diag.get('target_last_7_rows', 0)}`, "
        f"comparisons `{qc_diag.get('comparison_last_7_rows', 0)}`."
    )
    lines.append("| window | bets | WR | ROI | units | avg odds | sample warning | drift flag |")
    lines.append("|---|---:|---:|---:|---:|---:|---|---|")
    for row in watch_rows:
        lines.append(
            f"| {row['window']} | `{row['bets']}` | `{row['wr']}` | `{row['roi']}` | "
            f"`{row['units']}` | `{row['avg_odds']}` | `{row['sample_warning']}` | `{row['drift_flag']}` |"
        )
    comp = qc_bottom_order_watch.get("comparison_full_history") or {}
    ov = qc_bottom_order_watch.get("overlap_full_history") or {}
    lines.append(
        f"- Full-history baseline comparisons: QC-only non-bottom-order ROI `{_pct(comp.get('roi'))}` "
        f"(bets `{comp.get('bets','n/a')}`); overlap same-profile ROI `{_pct(ov.get('roi'))}` "
        f"(bets `{ov.get('bets','n/a')}`)."
    )
    comp_diag = qc_group_diag.get("qc_only_non_bottom_order_under_0.5") or {}
    ov_diag = qc_group_diag.get("overlap_bottom_order_under_0.5") or {}
    lines.append(
        f"- Baseline update note: these full-history values only change when new qualifying rows enter the comparison groups. "
        f"QC-only non-bottom latest qualifying date `{comp_diag.get('latest_qualifying_date') or 'n/a'}`, "
        f"latest-slate rows `{comp_diag.get('new_rows_latest_completed_slate', 0)}`, last-7 rows `{comp_diag.get('last_7_rows', 0)}`; "
        f"overlap same-profile latest qualifying date `{ov_diag.get('latest_qualifying_date') or 'n/a'}`, "
        f"latest-slate rows `{ov_diag.get('new_rows_latest_completed_slate', 0)}`, last-7 rows `{ov_diag.get('last_7_rows', 0)}`."
    )
    lines.append("")

    lines.append("## Hits Over 1.5 Watch Candidates")
    lines.append("- Scope: review aid only; not a production rule, selector, upload filter, or threshold change.")
    lines.append(board_source_line(hits_o15_watch_candidates, "hits_o15_watch_candidates_csv"))
    lines.append(
        f"- Candidate definition: Quick Card candidate + hits over 1.5 + `d7_hits_per_game > 1.0` + "
        f"`starter_expected_hits_allowed >= 5.0`."
    )
    lines.append(
        f"- Row count: `{hits_o15_watch_candidates.get('row_count', 0)}` | "
        f"A/A `{hits_o15_watch_candidates.get('aa_count', 0)}` | "
        f"A/B `{hits_o15_watch_candidates.get('ab_count', 0)}`"
    )
    top_candidates = [
        row for row in (hits_o15_watch_candidates.get("top_candidates") or []) if isinstance(row, dict)
    ][:5]
    if top_candidates:
        lines.append("| player | team | opp | tier | odds | d7 | d15 | starter exp | QC score | ranking score |")
        lines.append("|---|---|---|---|---:|---:|---:|---:|---:|---:|")
        for row in top_candidates:
            lines.append(
                f"| {row.get('player_name') or row.get('player') or ''} | {row.get('team') or ''} | "
                f"{row.get('opponent') or ''} | `{row.get('combined_tier') or ''}` | "
                f"`{_num_fmt(row.get('market_price'))}` | `{_num_fmt(row.get('d7_hits_rate'))}` | "
                f"`{_num_fmt(row.get('d15_hits_rate'))}` | "
                f"`{_num_fmt(row.get('starter_expected_hits_allowed'))}` | "
                f"`{_num_fmt(row.get('qc_score'))}` | `{_num_fmt(row.get('ranking_score'))}` |"
            )
    elif not board_source_ok(hits_o15_watch_candidates, "hits_o15_watch_candidates_csv"):
        lines.append("- Top candidates: unavailable because the current-slate watch candidate CSV is missing or unreadable.")
    else:
        lines.append("- Top candidates: none available from the current-slate watch candidate artifact.")
    lines.append("")

    lines.append("## Hits Over 1.5 Layered Candidates")
    lines.append("- Scope: review aid only; not a production rule, selector, upload filter, or threshold change.")
    lines.append(board_source_line(hits_o15_layered_candidates, "hits_o15_layered_candidates_csv"))
    lines.append(
        f"- Counts: d7_hot `{hits_o15_layered_candidates.get('d7_hot_count', 0)}` | "
        f"d7+d15 `{hits_o15_layered_candidates.get('d7_d15_count', 0)}` | "
        f"d7+d15+starter `{hits_o15_layered_candidates.get('d7_d15_favorable_starter_count', 0)}` | "
        f"QC watch `{hits_o15_layered_candidates.get('qc_watch_count', 0)}`"
    )
    aa_counts = hits_o15_layered_candidates.get("aa_counts") if isinstance(hits_o15_layered_candidates.get("aa_counts"), dict) else {}
    ab_counts = hits_o15_layered_candidates.get("ab_counts") if isinstance(hits_o15_layered_candidates.get("ab_counts"), dict) else {}
    lines.append(
        f"- A/A counts: QC watch `{aa_counts.get('layer_4_qc_d7_d15_starter', 0)}` | "
        f"d7+d15+starter non-QC `{aa_counts.get('layer_3_d7_d15_starter_non_qc', 0)}` | "
        f"d7+d15 no-starter `{aa_counts.get('layer_2_d7_d15_no_favorable_starter', 0)}` | "
        f"d7-only discovery `{aa_counts.get('layer_1_d7_hot_not_d15_consistent', 0)}`"
    )
    lines.append(
        f"- A/B counts: QC watch `{ab_counts.get('layer_4_qc_d7_d15_starter', 0)}` | "
        f"d7+d15+starter non-QC `{ab_counts.get('layer_3_d7_d15_starter_non_qc', 0)}` | "
        f"d7+d15 no-starter `{ab_counts.get('layer_2_d7_d15_no_favorable_starter', 0)}` | "
        f"d7-only discovery `{ab_counts.get('layer_1_d7_hot_not_d15_consistent', 0)}`"
    )

    def layered_candidate_line(row: Dict[str, Any]) -> str:
        return (
            f"{row.get('player') or row.get('player_name') or ''} "
            f"({row.get('team') or ''} vs {row.get('opponent') or ''}, "
            f"{row.get('combined_tier') or ''}, odds `{_num_fmt(row.get('market_price'))}`, "
            f"d7 `{_num_fmt(row.get('d7_hits_rate'))}`, d15 `{_num_fmt(row.get('d15_hits_rate'))}`, "
            f"starter `{_num_fmt(row.get('starter_expected_hits_allowed'))}`)"
        )

    top_layered_qc = [
        row for row in (hits_o15_layered_candidates.get("top_qc_watch_candidates") or []) if isinstance(row, dict)
    ][:3]
    top_layered_non_qc = [
        row
        for row in (hits_o15_layered_candidates.get("top_non_qc_d7_d15_favorable_starter") or [])
        if isinstance(row, dict)
    ][:3]
    if top_layered_qc:
        lines.append("- Top QC watch candidates: " + "; ".join(layered_candidate_line(row) for row in top_layered_qc))
    elif not board_source_ok(hits_o15_layered_candidates, "hits_o15_layered_candidates_csv"):
        lines.append("- Top QC watch candidates: unavailable because the current-slate layered candidate CSV is missing or unreadable.")
    else:
        lines.append("- Top QC watch candidates: none available.")
    if top_layered_non_qc:
        lines.append(
            "- Top non-QC d7+d15+favorable starter candidates: "
            + "; ".join(layered_candidate_line(row) for row in top_layered_non_qc)
        )
    elif not board_source_ok(hits_o15_layered_candidates, "hits_o15_layered_candidates_csv"):
        lines.append(
            "- Top non-QC d7+d15+favorable starter candidates: unavailable because the current-slate layered candidate CSV is missing or unreadable."
        )
    else:
        lines.append("- Top non-QC d7+d15+favorable starter candidates: none available.")
    lines.append("")

    lines.append("## Hits Under 1.5 Favorite Audit")
    lines.append("- Scope: review aid only; not a production rule, selector, upload filter, or threshold change.")
    lines.append(board_source_line(hits_u15_favorite_audit, "hits_u15_favorite_audit_csv"))
    lines.append(
        f"- Counts: all u1.5 `{hits_u15_favorite_audit.get('row_count', 0)}` | "
        f"d7 cold `{hits_u15_favorite_audit.get('d7_cold_count', 0)}` | "
        f"d7+d15 cold `{hits_u15_favorite_audit.get('d7_d15_cold_count', 0)}` | "
        f"d7+d15+tough starter `{hits_u15_favorite_audit.get('d7_d15_tough_starter_count', 0)}` | "
        f"QC watch `{hits_u15_favorite_audit.get('qc_watch_count', 0)}`"
    )
    u15_aa_counts = (
        hits_u15_favorite_audit.get("aa_counts")
        if isinstance(hits_u15_favorite_audit.get("aa_counts"), dict)
        else {}
    )
    u15_ab_counts = (
        hits_u15_favorite_audit.get("ab_counts")
        if isinstance(hits_u15_favorite_audit.get("ab_counts"), dict)
        else {}
    )
    lines.append(
        f"- A/A counts: QC watch `{u15_aa_counts.get('layer_4_qc_d7_d15_tough_starter', 0)}` | "
        f"d7+d15+tough starter non-QC `{u15_aa_counts.get('layer_3_d7_d15_tough_starter_non_qc', 0)}` | "
        f"d7+d15 no-tough-starter `{u15_aa_counts.get('layer_2_d7_d15_no_tough_starter', 0)}` | "
        f"d7-only discovery `{u15_aa_counts.get('layer_1_d7_cold_not_d15_consistent', 0)}`"
    )
    lines.append(
        f"- A/B counts: QC watch `{u15_ab_counts.get('layer_4_qc_d7_d15_tough_starter', 0)}` | "
        f"d7+d15+tough starter non-QC `{u15_ab_counts.get('layer_3_d7_d15_tough_starter_non_qc', 0)}` | "
        f"d7+d15 no-tough-starter `{u15_ab_counts.get('layer_2_d7_d15_no_tough_starter', 0)}` | "
        f"d7-only discovery `{u15_ab_counts.get('layer_1_d7_cold_not_d15_consistent', 0)}`"
    )

    def u15_candidate_line(row: Dict[str, Any]) -> str:
        return (
            f"{row.get('player') or row.get('player_name') or ''} "
            f"({row.get('team') or ''} vs {row.get('opponent') or ''}, "
            f"{row.get('combined_tier') or ''}, odds `{_num_fmt(row.get('market_price'))}`, "
            f"d7 `{_num_fmt(row.get('d7_hits_rate'))}`, d15 `{_num_fmt(row.get('d15_hits_rate'))}`, "
            f"starter `{_num_fmt(row.get('starter_expected_hits_allowed'))}`)"
        )

    top_u15_qc = [
        row for row in (hits_u15_favorite_audit.get("top_qc_watch_candidates") or []) if isinstance(row, dict)
    ][:3]
    top_u15_non_qc = [
        row
        for row in (hits_u15_favorite_audit.get("top_non_qc_d7_d15_tough_starter") or [])
        if isinstance(row, dict)
    ][:3]
    if top_u15_qc:
        lines.append("- Top QC watch candidates: " + "; ".join(u15_candidate_line(row) for row in top_u15_qc))
    elif not board_source_ok(hits_u15_favorite_audit, "hits_u15_favorite_audit_csv"):
        lines.append("- Top QC watch candidates: unavailable because the current-slate u1.5 audit CSV is missing or unreadable.")
    else:
        lines.append("- Top QC watch candidates: none available.")
    if top_u15_non_qc:
        lines.append(
            "- Top non-QC d7+d15+tough starter candidates: "
            + "; ".join(u15_candidate_line(row) for row in top_u15_non_qc)
        )
    elif not board_source_ok(hits_u15_favorite_audit, "hits_u15_favorite_audit_csv"):
        lines.append(
            "- Top non-QC d7+d15+tough starter candidates: unavailable because the current-slate u1.5 audit CSV is missing or unreadable."
        )
    else:
        lines.append("- Top non-QC d7+d15+tough starter candidates: none available.")
    lines.append("")

    lines.append("## Hits 1.5 Alternate Discovery")
    lines.append("- Scope: DISCOVERY ONLY; alternate market; Over-only feed; not production scoring, uploads, or grading.")
    lines.append(board_source_line(hits_o15_alternate_discovery, "hits_o15_alternate_discovery_csv"))
    lines.append(
        f"- Counts: total `{hits_o15_alternate_discovery.get('row_count', 0)}` | "
        f"d7+d15 `{hits_o15_alternate_discovery.get('d7_d15_count', 0)}` | "
        f"d7+d15+starter `{hits_o15_alternate_discovery.get('d7_d15_starter_count', 0)}`"
    )

    def alternate_candidate_line(row: Dict[str, Any]) -> str:
        return (
            f"{row.get('player') or row.get('player_name') or ''} "
            f"({row.get('team') or ''} vs {row.get('opponent') or ''}, "
            f"{row.get('combined_tier') or ''}, best over `{_num_fmt(row.get('best_over_price'))}`, "
            f"d7 `{_num_fmt(row.get('d7_hits_rate'))}`, d15 `{_num_fmt(row.get('d15_hits_rate'))}`, "
            f"starter `{_num_fmt(row.get('starter_expected_hits_allowed'))}`)"
        )

    top_alternate = [
        row for row in (hits_o15_alternate_discovery.get("top_candidates") or []) if isinstance(row, dict)
    ][:3]
    if top_alternate:
        lines.append("- Top alternate candidates: " + "; ".join(alternate_candidate_line(row) for row in top_alternate))
    elif not board_source_ok(hits_o15_alternate_discovery, "hits_o15_alternate_discovery_csv"):
        lines.append("- Top alternate candidates: unavailable because the current-slate alternate discovery CSV is missing or unreadable.")
    else:
        lines.append("- Top alternate candidates: none available.")
    lines.append("")

    lines.append("## Review Aid Performance")
    lines.append(provenance("Review Aid Performance"))
    lines.append("- Scope: review aid outcome tracking only; not a production rule, selector, upload filter, or threshold change.")
    lines.append(
        f"- Status: `{review_aid_performance.get('status') or 'n/a'}` | "
        f"latest completed slate `{review_aid_performance.get('latest_completed_slate') or 'n/a'}` | "
        f"board rows `{review_aid_performance.get('board_rows_loaded', 'n/a')}` | "
        f"matched `{review_aid_performance.get('matched_rows', 'n/a')}`"
    )

    def perf_line(label: str, row: Dict[str, Any]) -> str:
        descriptions = {
            "o1.5 Layer 4": "QC + d7/d15 + starter context",
            "o1.5 Layer 3": "d7/d15 + starter context",
            "o1.5 alternate Layer A": "alternate d7/d15 + favorable starter, no QC",
            "u1.5 Layer 4": "QC + d7/d15 + starter context",
            "u1.5 Layer 3": "d7/d15 + starter context",
            "u1.5 Layer 2": "d7/d15 form only",
        }
        display_label = f"{label} ({descriptions[label]})" if label in descriptions else label
        if not row:
            return f"- {display_label}: no latest completed slate rows."
        denominator = str(row.get("denominator") or "").strip()
        denominator_note = f"; denominator `{denominator}`" if denominator else ""
        return (
            f"- {display_label}: `{row.get('wins', 0)}-{row.get('losses', 0)}-{row.get('pushes', 0)}` "
            f"ROI `{_pct(row.get('roi'))}` over `{row.get('resolved', 0)}` resolved "
            f"(rows `{row.get('rows', 0)}`{denominator_note})."
        )

    callouts = review_aid_performance.get("callouts") if isinstance(review_aid_performance.get("callouts"), dict) else {}
    lines.append("- Layer = review-aid provenance, not A/A-style hitter/starter tier.")
    lines.append(perf_line("o1.5 Layer 4", callouts.get("o15_layer_4_latest") or {}))
    lines.append(perf_line("o1.5 Layer 3", callouts.get("o15_layer_3_latest") or {}))
    lines.append(perf_line("o1.5 alternate Layer A", callouts.get("o15_alternate_layer_a_latest") or {}))
    lines.append(perf_line("u1.5 Layer 4", callouts.get("u15_layer_4_latest") or {}))
    lines.append(perf_line("u1.5 Layer 3", callouts.get("u15_layer_3_latest") or {}))
    lines.append(perf_line("u1.5 Layer 2", callouts.get("u15_layer_2_latest") or {}))
    lines.append(perf_line("u1.5 A/A", callouts.get("u15_aa_latest") or {}))
    if str(review_aid_performance.get("status") or "") == "source_not_ready":
        lines.append(f"- Source-not-ready detail: {review_aid_performance.get('status_detail') or 'n/a'}")
    lines.append("")

    tier_top_o15 = [
        row for row in (hits_15_tier_backtest.get("o15_top_recent_combined_tiers") or []) if isinstance(row, dict)
    ][:3]
    tier_top_u15 = [
        row for row in (hits_15_tier_backtest.get("u15_top_recent_combined_tiers") or []) if isinstance(row, dict)
    ][:3]
    if tier_top_o15 or tier_top_u15:
        lines.append("## Reconstructed Hits 1.5 All-Market Tier Audit")
        lines.append(
            "- Scope: reconstructed all-market research audit from execution reconcile rows; "
            "not actual generated board artifact performance and not a production rule, selector, upload filter, or threshold change."
        )
        lines.append(
            f"- Latest completed slate: `{hits_15_tier_backtest.get('latest_completed_slate') or 'n/a'}` | "
            f"source `artifacts/analysis/mlb/review_aids/hits_15_tier_backtest_summary.json`."
        )
        lines.append("| board | window | tier | resolved | WR | ROI | sample |")
        lines.append("|---|---|---:|---:|---:|---:|---|")
        for board, rows in (("o1.5", tier_top_o15), ("u1.5", tier_top_u15)):
            for row in rows:
                lines.append(
                    f"| {board} | `{row.get('window')}` | `{row.get('tier')}` | `{row.get('resolved', 0)}` | "
                    f"`{_pct(row.get('wr'))}` | `{_pct(row.get('roi'))}` | `{row.get('sample_warning') or 'n/a'}` |"
                )
        lines.append("")

    lines.append("## Total Bases Shadow Candidate")
    lines.append(provenance("Total Bases Shadow Candidate"))
    eval_note = str(total_bases_shadow_evaluation.get("interpretation_note") or "").strip()
    lines.append(
        f"- Shadow scoring ran: `{bool(total_bases_shadow_summary)}` | "
        f"slate `{total_bases_shadow_summary.get('slate_date') or 'n/a'}` | "
        f"rows scored `{total_bases_shadow_summary.get('shadow_rows', 0)}` | "
        f"balanced side changes `{total_bases_shadow_summary.get('tb_rolling_balanced_side_changed_rows', total_bases_shadow_summary.get('side_changed_rows', 0))}` "
        f"(`{_pct(total_bases_shadow_summary.get('tb_rolling_balanced_side_changed_rate', total_bases_shadow_summary.get('side_changed_rate')))}`) | "
        f"unweighted side changes `{total_bases_shadow_summary.get('tb_rolling_unweighted_side_changed_rows', 0)}` "
        f"(`{_pct(total_bases_shadow_summary.get('tb_rolling_unweighted_side_changed_rate'))}`)."
    )
    lines.append(
        f"- Avg probability: production over `{_num_fmt(total_bases_shadow_summary.get('avg_production_prob_over'), 4)}` | "
        f"balanced shadow over `{_num_fmt(total_bases_shadow_summary.get('avg_tb_rolling_balanced_prob_over', total_bases_shadow_summary.get('avg_shadow_prob_over')), 4)}` "
        f"(delta `{_num_fmt(total_bases_shadow_summary.get('avg_tb_rolling_balanced_probability_delta_over', total_bases_shadow_summary.get('avg_probability_delta_over')), 4)}`) | "
        f"unweighted shadow over `{_num_fmt(total_bases_shadow_summary.get('avg_tb_rolling_unweighted_prob_over'), 4)}` "
        f"(delta `{_num_fmt(total_bases_shadow_summary.get('avg_tb_rolling_unweighted_probability_delta_over'), 4)}`)."
    )
    lines.append(
        f"- Coverage: rolling present `{_pct(total_bases_shadow_summary.get('rolling_context_present_rate'))}` | "
        f"rolling complete `{_pct(total_bases_shadow_summary.get('rolling_context_complete_rate'))}` | "
        f"training rows `{total_bases_shadow_summary.get('training_rows', 'n/a')}` through "
        f"`{total_bases_shadow_summary.get('training_train_through') or 'n/a'}`."
    )
    lines.append(provenance("Total Bases Shadow Evaluation"))
    lines.append(
        f"- Cumulative rows scored `{total_bases_shadow_evaluation.get('rows_scored', 0)}` | "
        f"rows with outcomes `{total_bases_shadow_evaluation.get('rows_with_outcomes', 0)}` | "
        f"outcome coverage `{_pct(total_bases_shadow_evaluation.get('outcome_coverage'))}`."
    )
    metric_rows = [
        row
        for row in (total_bases_shadow_evaluation.get("cumulative_metrics") or [])
        if isinstance(row, dict)
    ]
    if metric_rows:
        lines.append("| model | rows | Brier | log loss | AUC | avg over prob | actual over rate | overconfidence gap |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for row in metric_rows:
            lines.append(
                f"| {row.get('model','n/a')} | `{row.get('rows', 0)}` | `{_num_fmt(row.get('brier'), 4)}` | "
                f"`{_num_fmt(row.get('log_loss'), 4)}` | `{_num_fmt(row.get('auc'), 4)}` | "
                f"`{_num_fmt(row.get('avg_prob'), 4)}` | `{_num_fmt(row.get('actual_over_rate'), 4)}` | "
                f"`{_num_fmt(row.get('overconfidence_gap'), 4)}` |"
            )
    else:
        lines.append("- Cumulative metrics unavailable until at least one shadow slate has resolved outcomes.")
    if eval_note:
        lines.append(f"- Interpretation note: {eval_note}")
    lines.append("- Status: balanced shadow is research-only and not promotion-ready; unweighted shadow is research-only pending a larger live sample.")
    lines.append("- Guardrail: shadow-only; production predictions/uploads/selectors remain unchanged.")
    lines.append("")

    lines.append("## Path Forward")
    lines.append(provenance("Path Forward"))
    if path_forward:
        for idx, step in enumerate(path_forward, start=1):
            title = str(step.get("title") or f"Action {idx}").strip()
            desc = str(step.get("description") or "").strip()
            cmd = str(step.get("command") or "").strip()
            line = f"{idx}. {title}"
            if desc:
                line += f": {desc}"
            if cmd:
                line += f" Command: `{cmd}`."
            lines.append(line)
    else:
        lines.append("1. No immediate actions suggested.")
    lines.append("")

    lines.append("## BvP Impact")
    lines.append(provenance("BvP Impact"))
    lines.append(
        f"- Label date: `{bvp_impact.get('label_date','n/a')}` | requested slate date: "
        f"`{bvp_impact.get('requested_slate_date') or bvp_impact.get('label_date','n/a')}` | "
        f"rows evaluated `{bvp_impact.get('rows_evaluated','n/a')}` "
        f"of `{bvp_impact.get('rows_total_slate','n/a')}`"
    )
    lines.append(
        f"- Non-zero probability deltas: `{bvp_impact.get('rows_nonzero_delta','n/a')}` | "
        f"mean abs delta `{_pct(bvp_impact.get('mean_abs_delta_prob'))}` | "
        f"max abs delta `{_pct(bvp_impact.get('max_abs_delta_prob'))}`"
    )
    top_prop = bvp_impact.get("top_prop_impact") or {}
    if top_prop:
        lines.append(
            f"- Top impacted prop type: `{top_prop.get('prop_type')}` "
            f"(rows `{top_prop.get('rows')}`, mean abs delta `{_pct(top_prop.get('mean_abs_delta_prob'))}`)"
        )
    lines.append("")

    lines.append("## Hits Environment & Matchups")
    lines.append(provenance("Hits Environment & Matchups"))
    if hits_env.get("fallback_used"):
        lines.append(
            f"- Display fallback: using current-date hits-environment snapshot "
            f"`{hits_env.get('fallback_source_file')}` because `{hits_env.get('fallback_reason')}`. "
            "Refresh failure remains visible; health is not marked PASS by this fallback."
        )
    if hits_env.get("team_eval_fallback_used"):
        lines.append(
            f"- Expected-vs-actual fallback: using latest successful hits-environment history payload "
            f"`{hits_env.get('team_eval_fallback_source_file')}` "
            f"(generated `{hits_env.get('team_eval_fallback_generated_at_utc') or 'n/a'}`) because "
            f"`{hits_env.get('team_eval_fallback_reason')}`. Refresh failure remains visible; health is not marked PASS by this fallback."
        )
    lines.append(
        f"- Eval date `{hits_env.get('evaluation_date','n/a')}` | signal `{hits_env.get('league_signal','n/a')}` | "
        f"hits/game `{hits_env.get('league_hits_per_game')}` | z-score `{hits_env.get('league_zscore')}`"
    )
    lines.append(
        f"- Starter residuals: rows `{hits_env.get('starter_rows','n/a')}`, "
        f"vs d7 avg `{hits_env.get('starter_residual_vs_d7_avg')}`, "
        f"vs weighted avg `{hits_env.get('starter_residual_vs_weighted_avg')}`"
    )
    lines.append(
        f"- Slate expected matchups: rows `{hits_env.get('slate_rows_with_expected','n/a')}` / "
        f"`{hits_env.get('slate_rows','n/a')}`, avg expected `{hits_env.get('slate_avg_expected_matchup')}`, "
        f"avg line-expected `{hits_env.get('slate_avg_line_minus_expected')}`"
    )
    lines.append(
        f"- Full-game context (starter + bullpen proxy): rows `{hits_env.get('slate_rows_with_team_expected','n/a')}` / "
        f"`{hits_env.get('slate_rows','n/a')}`, avg bullpen add-on `{hits_env.get('slate_avg_bullpen_blended')}`, "
        f"avg team expected `{hits_env.get('slate_avg_team_expected')}`"
    )
    unavailable_pitchers = hits_env.get("forecast_unavailable_pitchers") or []
    if unavailable_pitchers:
        reason_counts = hits_env.get("forecast_unavailable_by_reason") or {}
        reason_text = ", ".join(f"{k}={v}" for k, v in sorted(reason_counts.items())) or "unknown"
        lines.append(
            f"- Forecast unavailable starters: rows `{hits_env.get('forecast_unavailable_rows')}` | reasons `{reason_text}`"
        )
        lines.append(_format_category_separator(f"Starters present in odds without hits-allowed forecast (n={len(unavailable_pitchers)})"))
        for r in unavailable_pitchers:
            prior = r.get("prior_starter_games")
            line = r.get("line")
            books = r.get("odds_books_seen")
            lines.append(
                f"- {r.get('player_name')} ({r.get('pitcher_team')} vs {r.get('offense_team')}): "
                f"reason `{r.get('forecast_diagnostic') or r.get('forecast_note')}`"
                f"{f', prior starts `{prior}`' if prior is not None else ''}"
                f"{f', line `{line}`' if line is not None else ''}"
                f"{f', books `{books}`' if books is not None else ''}"
            )
    lifecycle_warnings = hits_env.get("starter_market_lifecycle_warnings") or []
    if lifecycle_warnings:
        lines.append(_format_category_separator(f"Starter / Market Lifecycle Warnings (n={len(lifecycle_warnings)})"))
        for r in lifecycle_warnings[:10]:
            lines.append(
                f"- {r.get('player_name')} ({r.get('pitcher_team')} vs {r.get('offense_team')}): "
                f"identity `{r.get('identity_status')}`, role `{r.get('role_status')}`, "
                f"market `{r.get('market_status')}`, forecast `{r.get('forecast_status')}` / `{r.get('forecast_diagnostic')}`, "
                f"actual `{r.get('actual_usage_status')}`, warning `{r.get('lifecycle_warning')}`"
            )
    top_rows = hits_env.get("top_expected_matchups") or []
    low_rows = hits_env.get("lowest_expected_matchups") or []
    top_team_rows = hits_env.get("top_expected_team_matchups") or []
    low_team_rows = hits_env.get("lowest_expected_team_matchups") or []
    if top_rows:
        lines.append(_format_category_separator(f"Highest expected hits-allowed matchups (n={len(top_rows)})"))
        for r in top_rows:
            lines.append(_format_matchup_row(r))
    if low_rows:
        lines.append(_format_category_separator(f"Lowest expected hits-allowed matchups (n={len(low_rows)})"))
        for r in low_rows:
            lines.append(_format_matchup_row(r))
    if top_team_rows:
        lines.append(_format_category_separator(f"Highest expected team hits allowed (starter + bullpen) (n={len(top_team_rows)})"))
        for r in top_team_rows:
            lines.append(_format_team_matchup_row(r))
    if low_team_rows:
        lines.append(_format_category_separator(f"Lowest expected team hits allowed (starter + bullpen) (n={len(low_team_rows)})"))
        for r in low_team_rows:
            lines.append(_format_team_matchup_row(r))
    lines.append(
        f"- Team-level expected vs actual eval (context as-of `{hits_env.get('team_eval_context_as_of_date','n/a')}`): "
        f"rows `{hits_env.get('team_eval_rows_with_actual','n/a')}` / "
        f"`{hits_env.get('team_eval_rows_with_expected','n/a')}`, "
        f"coverage `{hits_env.get('team_eval_coverage_pct')}`, "
        f"expected avg `{hits_env.get('team_eval_expected_avg')}`, "
        f"actual avg `{hits_env.get('team_eval_actual_avg')}`, "
        f"residual avg `{hits_env.get('team_eval_residual_avg')}`, "
        f"MAE `{hits_env.get('team_eval_mae')}`, RMSE `{hits_env.get('team_eval_rmse')}`"
    )
    top_over_eval = hits_env.get("team_eval_top_over") or []
    top_under_eval = hits_env.get("team_eval_top_under") or []
    if top_over_eval:
        lines.append(_format_category_separator(f"Biggest over-expected misses (n={len(top_over_eval)})"))
        for r in top_over_eval:
            lines.append(_format_team_eval_row(r))
    if top_under_eval:
        lines.append(_format_category_separator(f"Biggest under-expected misses (n={len(top_under_eval)})"))
        for r in top_under_eval:
            lines.append(_format_team_eval_row(r))
    if hits_env.get("warnings"):
        lines.append(f"- Warnings: `{'; '.join([str(w) for w in hits_env.get('warnings')])}`")
    lines.append("")

    lines.append("## Freshness Audit")
    lines.append("| section | source_date | expected_date | generated_at / mtime | freshness_status | note |")
    lines.append("|---|---:|---:|---|---|---|")
    for row in freshness_audit:
        generated_or_mtime = row.get("generated_at_utc") or row.get("mtime_utc") or "n/a"
        cadence = str(row.get("expected_refresh_cadence") or "").strip()
        note_raw = str(row.get("note") or "").strip()
        note = (f"{cadence}. {note_raw}" if cadence and note_raw else cadence or note_raw).replace("|", "/")
        lines.append(
            f"| {row.get('section') or 'n/a'} | `{row.get('source_date') or 'n/a'}` | "
            f"`{row.get('expected_date') or 'n/a'}` | `{generated_or_mtime}` | "
            f"`{row.get('freshness_status') or 'n/a'}` | {note} |"
        )
    lines.append("")

    lines.append("## Source Health")
    lines.append(provenance("Source Health"))
    for name, state in source_states.items():
        lines.append(f"- {name}: `{state}`")
    if feature_lineage_health:
        summary = feature_lineage_health.get("summary") if isinstance(feature_lineage_health.get("summary"), dict) else {}
        lines.append(
            f"- Feature lineage health: status `{feature_lineage_health.get('status') or 'unknown'}` | "
            f"slate_date `{feature_lineage_health.get('slate_date') or 'n/a'}` | "
            f"pass `{summary.get('pass_count', 0)}` warn `{summary.get('warn_count', 0)}` fail `{summary.get('fail_count', 0)}` | "
            f"BvP payload artifacts `{summary.get('bvp_artifacts_with_payload', 0)}` | "
            f"BvP missing required `{len(summary.get('bvp_missing_required_columns') or [])}`"
        )
        bvp_rates = summary.get("bvp_payload_rates") if isinstance(summary.get("bvp_payload_rates"), dict) else {}
        if bvp_rates:
            rate_parts = []
            for artifact_name, rate in sorted(bvp_rates.items()):
                if rate is None:
                    rate_parts.append(f"{artifact_name}=n/a")
                else:
                    try:
                        rate_parts.append(f"{artifact_name}={float(rate):.1%}")
                    except Exception:
                        rate_parts.append(f"{artifact_name}=n/a")
            lines.append(f"  - BvP compact payload rates: {'; '.join(rate_parts)}")
        for artifact in (feature_lineage_health.get("artifacts") or [])[:8]:
            if not isinstance(artifact, dict):
                continue
            if artifact.get("status") == "pass":
                continue
            lines.append(
                f"  - {artifact.get('artifact')}: `{artifact.get('status')}` rows `{artifact.get('row_count')}` "
                f"path `{artifact.get('path')}` issues `{'; '.join(str(x) for x in artifact.get('issues') or []) or 'n/a'}`"
            )
    lines.append("")

    lines.append("## MLB Today Workspace")
    lines.append(provenance("MLB Today Workspace"))
    lines.append(f"requested_slate_date: {today_workspace.get('requested_slate_date')}")
    lines.append(f"active_slate_date: {today_workspace.get('active_slate_date')}")
    lines.append(f"row_count: {today_workspace.get('row_count')}")
    lines.append(f"last_updated: {today_workspace.get('last_updated')}")
    ws_status = str(today_workspace.get("status") or "fail").upper()
    lines.append("")
    lines.append(f"Status: {ws_status}")
    if ws_status != "PASS":
        lines.append(f"Reason: {today_workspace.get('reason') or 'unknown'}")
        diag = today_workspace.get("diagnostics") or {}
        if diag:
            lines.append(f"attempted_function: {diag.get('attempted_function')}")
            lines.append(f"attempted_url: {diag.get('attempted_url')}")
            lines.append(f"exception_type: {diag.get('exception_type') or 'n/a'}")
            lines.append(f"exception_message: {diag.get('exception_message') or 'n/a'}")
            lines.append(f"failure_classification: {diag.get('failure_classification') or 'n/a'}")
            lines.append(f"retry_attempted: {diag.get('retry_attempted')}")
            lines.append(f"retry_succeeded: {diag.get('retry_succeeded')}")
    lines.append("")
    _append_rolling_candidate_obs_section(lines, rolling_candidate_obs, ops_brief_md_path=ops_brief_md_path)
    return "\n".join(lines).rstrip() + "\n"


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Build MLB daily human-readable ops brief.")
    ap.add_argument("--report-date", default=date.today().isoformat(), help="Label date (YYYY-MM-DD)")
    ap.add_argument("--completed-slate-date", default="", help="Completed/reconcile slate date (default: report-date - 1)")
    ap.add_argument("--current-slate-date", default="", help="Current/workspace slate date (default: report-date)")
    ap.add_argument("--postgrade-alerts-json", default="artifacts/analysis/mlb/mlb_postgrade_alerts_latest.json")
    ap.add_argument("--model-vs-fade-json", default="tmp/analysis/mlb_model_vs_fade_summary.json")
    ap.add_argument(
        "--prop-regime-csv",
        default="backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv",
    )
    ap.add_argument(
        "--model-performance-summary-csv",
        default="backend/mlb/exports/model_performance/prop_rolling_summary.csv",
    )
    ap.add_argument(
        "--model-performance-daily-csv",
        default="backend/mlb/exports/model_performance/prop_daily_performance.csv",
    )
    ap.add_argument(
        "--reporting-alignment-csv",
        default="backend/mlb/exports/reporting_alignment/reporting_alignment_{completed_slate_date}.csv",
    )
    ap.add_argument("--bvp-impact-json", default="artifacts/analysis/mlb/mlb_bvp_impact_latest.json")
    ap.add_argument(
        "--require-fresh-bvp-impact",
        type=int,
        default=1,
        help="Fail the brief when BvP impact label_date does not match report-date (default: 1).",
    )
    ap.add_argument("--hits-environment-json", default="artifacts/analysis/mlb/mlb_hits_environment_latest.json")
    ap.add_argument("--overlap-watch-json", default="artifacts/analysis/mlb/v2_qc_diagnostics/ranking_vs_quick_card_overlap_watch.json")
    ap.add_argument("--qc-bottom-order-watch-json", default="artifacts/analysis/mlb/qc_bottom_order_under_watch.json")
    ap.add_argument(
        "--hits-o15-watch-candidates-csv",
        default="artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_{current_slate_date}.csv",
    )
    ap.add_argument(
        "--hits-o15-layered-candidates-csv",
        default="artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_{current_slate_date}.csv",
    )
    ap.add_argument(
        "--hits-u15-favorite-audit-csv",
        default="artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_{current_slate_date}.csv",
    )
    ap.add_argument(
        "--hits-o15-alternate-discovery-csv",
        default="artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_{current_slate_date}.csv",
    )
    ap.add_argument("--hits-15-tier-backtest-json", default="artifacts/analysis/mlb/review_aids/hits_15_tier_backtest_summary.json")
    ap.add_argument(
        "--review-aid-performance-json",
        default="artifacts/analysis/mlb/review_aids/performance/review_aid_performance_summary.json",
    )
    ap.add_argument(
        "--total-bases-shadow-summary-json",
        default="artifacts/analysis/mlb/model_quality/total_bases_shadow/{current_slate_date}/total_bases_shadow_summary_{current_slate_date}.json",
    )
    ap.add_argument(
        "--total-bases-shadow-evaluation-json",
        default="artifacts/analysis/mlb/model_quality/total_bases_shadow/evaluation/total_bases_shadow_evaluation_summary.json",
    )
    ap.add_argument(
        "--feature-lineage-health-json",
        default="artifacts/analysis/mlb/feature_lineage/daily_feature_lineage_health_latest.json",
    )
    ap.add_argument(
        "--input-refresh-status-json",
        default="artifacts/analysis/mlb/mlb_daily_ops_brief_input_refresh_latest.json",
    )
    ap.add_argument("--pipeline-history-jsonl", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--ops-history-jsonl", default="artifacts/mlb_prod12_ops_history.jsonl")
    ap.add_argument("--out-md", default="artifacts/analysis/mlb/mlb_daily_ops_brief_latest.md")
    ap.add_argument("--dated-out-md", default="", help="Optional dated markdown output path")
    ap.add_argument("--out-json", default="artifacts/analysis/mlb/mlb_daily_ops_brief_latest.json")
    ap.add_argument("--history-jsonl", default="artifacts/analysis/mlb/mlb_daily_ops_brief_history.jsonl")
    ap.add_argument(
        "--rolling-candidate-obs-json",
        default="",
        help="Optional rolling market-late observation JSON path. Used only when the section is enabled.",
    )
    ap.add_argument(
        "--rolling-candidate-obs-mode",
        default="",
        help="Rolling market-late observation mode: auto, 1/true, or 0/false.",
    )
    ap.add_argument(
        "--enable-rolling-candidate-obs",
        action="store_true",
        help="Opt in to the Rolling Market-Late Candidate Observation section.",
    )
    ap.add_argument(
        "--skip-today-workspace-fetch",
        action="store_true",
        help="Render from cached Today Workspace status instead of opening DB/API dependencies.",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    generated_at_utc = _utc_now_iso()
    report_date = _date_key(args.report_date) or date.today().isoformat()
    completed_slate_date = _date_key(args.completed_slate_date) or _previous_date(report_date)
    current_slate_date = _date_key(args.current_slate_date) or report_date
    rolling_candidate_obs_mode = _resolve_rolling_candidate_obs_mode(
        args.rolling_candidate_obs_mode or os.environ.get("MLB_ENABLE_ROLLING_CANDIDATE_OBS"),
        force_enabled=args.enable_rolling_candidate_obs,
    )
    rolling_candidate_obs_path = Path(
        args.rolling_candidate_obs_json
        or os.environ.get("MLB_ROLLING_CANDIDATE_OBS_JSON", "")
        or (
            "artifacts/analysis/mlb/market_late_candidate_discovery/"
            f"rolling_observation_{current_slate_date}/"
            f"rolling_candidate_ops_brief_input_{current_slate_date}.json"
        )
    )

    paths: Dict[str, Path] = {
        "postgrade_alerts_json": Path(args.postgrade_alerts_json),
        "model_vs_fade_json": Path(args.model_vs_fade_json),
        "prop_regime_csv": Path(args.prop_regime_csv),
        "model_performance_summary_csv": Path(args.model_performance_summary_csv),
        "model_performance_daily_csv": Path(args.model_performance_daily_csv),
        "bvp_impact_json": Path(args.bvp_impact_json),
        "hits_environment_json": Path(args.hits_environment_json),
        "overlap_watch_json": Path(args.overlap_watch_json),
        "qc_bottom_order_watch_json": Path(args.qc_bottom_order_watch_json),
        "hits_15_tier_backtest_json": Path(args.hits_15_tier_backtest_json),
        "review_aid_performance_json": Path(args.review_aid_performance_json),
        "total_bases_shadow_evaluation_json": Path(args.total_bases_shadow_evaluation_json),
        "feature_lineage_health_json": Path(args.feature_lineage_health_json),
        "input_refresh_status_json": Path(args.input_refresh_status_json),
        "pipeline_history_jsonl": Path(args.pipeline_history_jsonl),
        "ops_history_jsonl": Path(args.ops_history_jsonl),
        "rolling_candidate_obs_json": rolling_candidate_obs_path,
    }
    paths["total_bases_shadow_summary_json"] = Path(
        str(args.total_bases_shadow_summary_json).format(
            report_date=report_date,
            completed_slate_date=completed_slate_date,
            current_slate_date=current_slate_date,
        )
    )
    paths["hits_o15_watch_candidates_csv"] = Path(
        str(args.hits_o15_watch_candidates_csv).format(
            report_date=report_date,
            completed_slate_date=completed_slate_date,
            current_slate_date=current_slate_date,
        )
    )
    paths["hits_o15_layered_candidates_csv"] = Path(
        str(args.hits_o15_layered_candidates_csv).format(
            report_date=report_date,
            completed_slate_date=completed_slate_date,
            current_slate_date=current_slate_date,
        )
    )
    paths["hits_u15_favorite_audit_csv"] = Path(
        str(args.hits_u15_favorite_audit_csv).format(
            report_date=report_date,
            completed_slate_date=completed_slate_date,
            current_slate_date=current_slate_date,
        )
    )
    paths["hits_o15_alternate_discovery_csv"] = Path(
        str(args.hits_o15_alternate_discovery_csv).format(
            report_date=report_date,
            completed_slate_date=completed_slate_date,
            current_slate_date=current_slate_date,
        )
    )
    reporting_alignment_path = Path(
        str(args.reporting_alignment_csv).format(
            report_date=report_date,
            completed_slate_date=completed_slate_date,
            current_slate_date=current_slate_date,
        )
    )
    paths["reporting_alignment_csv"] = reporting_alignment_path

    postgrade_raw, postgrade_err = _load_json(paths["postgrade_alerts_json"])
    model_raw, model_err = _load_json(paths["model_vs_fade_json"])
    prop_regime_rows, prop_regime_err = _load_csv_rows(paths["prop_regime_csv"])
    model_perf_summary_rows, model_perf_summary_err = _load_csv_rows(paths["model_performance_summary_csv"])
    model_perf_daily_rows, model_perf_daily_err = _load_csv_rows(paths["model_performance_daily_csv"])
    reporting_alignment_rows, reporting_alignment_err = _load_csv_rows(reporting_alignment_path)
    bvp_raw, bvp_err = _load_json(paths["bvp_impact_json"])
    hits_raw, hits_err = _load_json(paths["hits_environment_json"])
    overlap_watch_raw, overlap_watch_err = _load_json(paths["overlap_watch_json"])
    qc_watch_raw, qc_watch_err = _load_json(paths["qc_bottom_order_watch_json"])
    hits_o15_watch_candidate_rows, hits_o15_watch_candidates_err = _load_csv_rows(
        paths["hits_o15_watch_candidates_csv"]
    )
    hits_o15_layered_candidate_rows, hits_o15_layered_candidates_err = _load_csv_rows(
        paths["hits_o15_layered_candidates_csv"]
    )
    hits_u15_favorite_audit_rows, hits_u15_favorite_audit_err = _load_csv_rows(
        paths["hits_u15_favorite_audit_csv"]
    )
    hits_o15_alternate_discovery_rows, hits_o15_alternate_discovery_err = _load_csv_rows(
        paths["hits_o15_alternate_discovery_csv"]
    )
    hits_15_tier_raw, hits_15_tier_err = _load_json(paths["hits_15_tier_backtest_json"])
    review_aid_performance_raw, review_aid_performance_err = _load_json(paths["review_aid_performance_json"])
    total_bases_shadow_summary_raw, total_bases_shadow_summary_err = _load_json(paths["total_bases_shadow_summary_json"])
    total_bases_shadow_evaluation_raw, total_bases_shadow_evaluation_err = _load_json(paths["total_bases_shadow_evaluation_json"])
    feature_lineage_health_raw, feature_lineage_health_err = _load_json(paths["feature_lineage_health_json"])
    input_refresh_status, input_refresh_err = _load_json(paths["input_refresh_status_json"])
    pipeline_raw, pipeline_err = _load_last_jsonl(paths["pipeline_history_jsonl"])
    ops_raw, ops_err = _load_last_jsonl(paths["ops_history_jsonl"])
    rolling_candidate_obs = _load_optional_rolling_candidate_obs(
        paths["rolling_candidate_obs_json"],
        mode=rolling_candidate_obs_mode,
        expected_date=current_slate_date,
    )
    if args.skip_today_workspace_fetch:
        today_workspace, today_workspace_err = _cached_today_workspace_status(
            slate_date=current_slate_date,
            out_json_path=Path(args.out_json),
            history_jsonl_path=Path(args.history_jsonl),
            generated_at_utc=generated_at_utc,
        )
    else:
        today_workspace, today_workspace_err = _fetch_today_workspace_status(current_slate_date)
    history_rows = _load_jsonl_objects(Path(args.history_jsonl))

    source_states = {
        "postgrade_alerts_json": postgrade_err or "ok",
        "model_vs_fade_json": model_err or "ok",
        "prop_regime_csv": prop_regime_err or "ok",
        "model_performance_summary_csv": model_perf_summary_err or "ok",
        "model_performance_daily_csv": model_perf_daily_err or "ok",
        "reporting_alignment_csv": reporting_alignment_err or "ok",
        "bvp_impact_json": bvp_err or "ok",
        "hits_environment_json": hits_err or "ok",
        "overlap_watch_json": overlap_watch_err or "ok",
        "qc_bottom_order_watch_json": qc_watch_err or "ok",
        "hits_o15_watch_candidates_csv": hits_o15_watch_candidates_err or "ok",
        "hits_o15_layered_candidates_csv": hits_o15_layered_candidates_err or "ok",
        "hits_u15_favorite_audit_csv": hits_u15_favorite_audit_err or "ok",
        "hits_o15_alternate_discovery_csv": hits_o15_alternate_discovery_err or "ok",
        "hits_15_tier_backtest_json": hits_15_tier_err or "ok",
        "review_aid_performance_json": review_aid_performance_err or "ok",
        "total_bases_shadow_summary_json": total_bases_shadow_summary_err or "ok",
        "total_bases_shadow_evaluation_json": total_bases_shadow_evaluation_err or "ok",
        "feature_lineage_health_json": feature_lineage_health_err or "ok",
        "input_refresh_status_json": input_refresh_err or "ok",
        "pipeline_history_jsonl": pipeline_err or "ok",
        "ops_history_jsonl": ops_err or "ok",
        "today_workspace": today_workspace_err or "ok",
    }

    postgrade = _extract_postgrade(postgrade_raw if isinstance(postgrade_raw, dict) else None)
    model_vs_fade = _extract_model_vs_fade(model_raw if isinstance(model_raw, dict) else None)
    postgrade, model_vs_fade = _annotate_alerts(
        report_date=report_date,
        generated_at_utc=generated_at_utc,
        postgrade=postgrade,
        model_vs_fade=model_vs_fade,
        history_rows=history_rows,
        fresh_source_dates=(report_date, completed_slate_date),
    )
    prop_regime = _extract_prop_regime(prop_regime_rows, paths["prop_regime_csv"])
    model_performance = _extract_model_performance(
        model_perf_summary_rows,
        model_perf_daily_rows,
        summary_path=paths["model_performance_summary_csv"],
        daily_path=paths["model_performance_daily_csv"],
    )
    reporting_alignment = _extract_reporting_alignment(reporting_alignment_rows, reporting_alignment_path)
    bvp_impact = _extract_bvp_impact(bvp_raw if isinstance(bvp_raw, dict) else None)
    hits_env = _extract_hits_env(hits_raw if isinstance(hits_raw, dict) else None)
    hits_env = _apply_hits_environment_snapshot_fallback(hits_env, current_slate_date)
    hits_env = _apply_hits_environment_team_eval_history_fallback(
        hits_env,
        current_slate_date=current_slate_date,
        completed_slate_date=completed_slate_date,
    )
    overlap_watch = _extract_overlap_watch(overlap_watch_raw if isinstance(overlap_watch_raw, dict) else None)
    qc_bottom_order_watch = _extract_qc_bottom_order_watch(qc_watch_raw if isinstance(qc_watch_raw, dict) else None)
    hits_o15_watch_candidates = _extract_hits_o15_watch_candidates(hits_o15_watch_candidate_rows)
    hits_o15_layered_candidates = _extract_hits_o15_layered_candidates(hits_o15_layered_candidate_rows)
    hits_u15_favorite_audit = _extract_hits_u15_favorite_audit(hits_u15_favorite_audit_rows)
    hits_o15_alternate_discovery = _extract_hits_o15_alternate_discovery(hits_o15_alternate_discovery_rows)
    for payload, source_key in (
        (hits_o15_watch_candidates, "hits_o15_watch_candidates_csv"),
        (hits_o15_layered_candidates, "hits_o15_layered_candidates_csv"),
        (hits_u15_favorite_audit, "hits_u15_favorite_audit_csv"),
        (hits_o15_alternate_discovery, "hits_o15_alternate_discovery_csv"),
    ):
        payload["source_path"] = str(paths[source_key])
        payload["source_state"] = source_states.get(source_key) or "ok"
    hits_15_tier_backtest = hits_15_tier_raw if isinstance(hits_15_tier_raw, dict) else {}
    review_aid_performance = _extract_review_aid_performance(
        review_aid_performance_raw if isinstance(review_aid_performance_raw, dict) else None
    )
    total_bases_shadow_summary = _extract_total_bases_shadow_summary(
        total_bases_shadow_summary_raw if isinstance(total_bases_shadow_summary_raw, dict) else None
    )
    total_bases_shadow_evaluation = _extract_total_bases_shadow_evaluation(
        total_bases_shadow_evaluation_raw if isinstance(total_bases_shadow_evaluation_raw, dict) else None
    )
    feature_lineage_health = feature_lineage_health_raw if isinstance(feature_lineage_health_raw, dict) else {}
    pipeline = _extract_pipeline(pipeline_raw)
    ops = _extract_ops(ops_raw)
    freshness_audit = _build_freshness_audit(
        report_date=report_date,
        completed_slate_date=completed_slate_date,
        current_slate_date=current_slate_date,
        generated_at_utc=generated_at_utc,
        source_states=source_states,
        paths=paths,
        pipeline=pipeline,
        ops=ops,
        postgrade=postgrade,
        model_vs_fade=model_vs_fade,
        prop_regime=prop_regime,
        model_performance=model_performance,
        reporting_alignment=reporting_alignment,
        bvp_impact=bvp_impact,
        hits_env=hits_env,
        overlap_watch=overlap_watch,
        qc_bottom_order_watch=qc_bottom_order_watch,
        hits_15_tier_backtest=hits_15_tier_backtest,
        review_aid_performance=review_aid_performance,
        total_bases_shadow_summary=total_bases_shadow_summary,
        total_bases_shadow_evaluation=total_bases_shadow_evaluation,
        feature_lineage_health=feature_lineage_health,
        today_workspace=today_workspace,
        input_refresh_status=input_refresh_status,
    )

    overall_status, overall_issues = _derive_overall_status(
        pipeline=pipeline,
        ops=ops,
        postgrade=postgrade,
        hits_env=hits_env,
        model_vs_fade=model_vs_fade,
        freshness_audit=freshness_audit,
    )
    path_forward = _derive_path_forward(
        report_date=report_date,
        overall_status=overall_status,
        pipeline=pipeline,
        ops=ops,
        postgrade=postgrade,
        model_vs_fade=model_vs_fade,
        bvp_impact=bvp_impact,
        hits_env=hits_env,
    )

    out_md = Path(args.out_md)
    md_text = build_markdown(
        ops_brief_md_path=out_md,
        report_date=report_date,
        completed_slate_date=completed_slate_date,
        current_slate_date=current_slate_date,
        generated_at_utc=generated_at_utc,
        overall_status=overall_status,
        overall_issues=overall_issues,
        pipeline=pipeline,
        ops=ops,
        postgrade=postgrade,
        model_vs_fade=model_vs_fade,
        bvp_impact=bvp_impact,
        hits_env=hits_env,
        overlap_watch=overlap_watch,
        qc_bottom_order_watch=qc_bottom_order_watch,
        hits_o15_watch_candidates=hits_o15_watch_candidates,
        hits_o15_layered_candidates=hits_o15_layered_candidates,
        hits_u15_favorite_audit=hits_u15_favorite_audit,
        hits_o15_alternate_discovery=hits_o15_alternate_discovery,
        hits_15_tier_backtest=hits_15_tier_backtest,
        review_aid_performance=review_aid_performance,
        total_bases_shadow_summary=total_bases_shadow_summary,
        total_bases_shadow_evaluation=total_bases_shadow_evaluation,
        feature_lineage_health=feature_lineage_health,
        prop_regime=prop_regime,
        model_performance=model_performance,
        reporting_alignment=reporting_alignment,
        today_workspace=today_workspace,
        rolling_candidate_obs=rolling_candidate_obs,
        path_forward=path_forward,
        source_states=source_states,
        freshness_audit=freshness_audit,
    )

    payload: Dict[str, Any] = {
        "generated_at_utc": generated_at_utc,
        "report_date": report_date,
        "completed_slate_date": completed_slate_date,
        "current_slate_date": current_slate_date,
        "status": overall_status,
        "ok": overall_status == "pass",
        "issues": overall_issues,
        "source_states": source_states,
        "freshness_audit": freshness_audit,
        "pipeline": pipeline,
        "ops": ops,
        "postgrade": postgrade,
        "model_vs_fade": model_vs_fade,
        "prop_regime": prop_regime,
        "model_performance": model_performance,
        "reporting_alignment": reporting_alignment,
        "bvp_impact": bvp_impact,
        "hits_environment": hits_env,
        "ranking_qc_overlap_watch": overlap_watch,
        "qc_bottom_order_watch": qc_bottom_order_watch,
        "hits_o15_watch_candidates": hits_o15_watch_candidates,
        "hits_o15_layered_candidates": hits_o15_layered_candidates,
        "hits_u15_favorite_audit": hits_u15_favorite_audit,
        "hits_o15_alternate_discovery": hits_o15_alternate_discovery,
        "hits_15_tier_backtest": hits_15_tier_backtest,
        "review_aid_performance": review_aid_performance,
        "total_bases_shadow_summary": total_bases_shadow_summary,
        "total_bases_shadow_evaluation": total_bases_shadow_evaluation,
        "feature_lineage_health": feature_lineage_health,
        "today_workspace": today_workspace,
        "input_refresh_status": input_refresh_status,
        "path_forward": path_forward,
        "outputs": {
            "out_md": str(args.out_md),
            "dated_out_md": str(args.dated_out_md) if args.dated_out_md else None,
            "out_json": str(args.out_json),
            "history_jsonl": str(args.history_jsonl),
        },
    }
    if rolling_candidate_obs_mode != "disabled":
        payload["rolling_candidate_observation"] = rolling_candidate_obs

    _ensure_parent(out_md)
    out_md.write_text(md_text, encoding="utf-8")

    if str(args.dated_out_md).strip():
        dated_md = Path(str(args.dated_out_md).strip())
        _ensure_parent(dated_md)
        dated_md_text = md_text
        if dated_md.parent != out_md.parent:
            dated_md_text = build_markdown(
                ops_brief_md_path=dated_md,
                report_date=report_date,
                completed_slate_date=completed_slate_date,
                current_slate_date=current_slate_date,
                generated_at_utc=generated_at_utc,
                overall_status=overall_status,
                overall_issues=overall_issues,
                pipeline=pipeline,
                ops=ops,
                postgrade=postgrade,
                model_vs_fade=model_vs_fade,
                bvp_impact=bvp_impact,
                hits_env=hits_env,
                overlap_watch=overlap_watch,
                qc_bottom_order_watch=qc_bottom_order_watch,
                hits_o15_watch_candidates=hits_o15_watch_candidates,
                hits_o15_layered_candidates=hits_o15_layered_candidates,
                hits_u15_favorite_audit=hits_u15_favorite_audit,
                hits_o15_alternate_discovery=hits_o15_alternate_discovery,
                hits_15_tier_backtest=hits_15_tier_backtest,
                review_aid_performance=review_aid_performance,
                total_bases_shadow_summary=total_bases_shadow_summary,
                total_bases_shadow_evaluation=total_bases_shadow_evaluation,
                feature_lineage_health=feature_lineage_health,
                prop_regime=prop_regime,
                model_performance=model_performance,
                reporting_alignment=reporting_alignment,
                today_workspace=today_workspace,
                rolling_candidate_obs=rolling_candidate_obs,
                path_forward=path_forward,
                source_states=source_states,
                freshness_audit=freshness_audit,
            )
        dated_md.write_text(dated_md_text, encoding="utf-8")

    out_json = Path(args.out_json)
    _ensure_parent(out_json)
    out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    history_jsonl = Path(args.history_jsonl)
    _ensure_parent(history_jsonl)
    with history_jsonl.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True))
        fh.write("\n")

    fresh_count = sum(1 for row in freshness_audit if row.get("freshness_status") == "fresh")
    expected_stale_count = sum(1 for row in freshness_audit if row.get("freshness_status") in {"stale-but-expected", "persistent-state"})
    unexpected_count = sum(1 for row in freshness_audit if row.get("freshness_status") in {"stale-unexpected", "missing-unexpected"})
    new_alerts_count = int(model_vs_fade.get("new_alerts_count") or 0)
    persistent_alerts_count = int(model_vs_fade.get("persistent_alerts_count") or 0)
    print(
        "[mlb-daily-ops-brief] "
        f"brief_date={report_date} completed_slate_date={completed_slate_date} "
        f"current_slate_date={current_slate_date} fresh_sections={fresh_count} "
        f"expected_stale_sections={expected_stale_count} "
        f"unexpected_stale_or_missing={unexpected_count} "
        f"new_alerts={new_alerts_count} persistent_alerts={persistent_alerts_count} "
        f"status={overall_status} out_md={out_md}"
    )
    print(md_text)
    return 0 if overall_status != "fail" else 2


if __name__ == "__main__":
    raise SystemExit(main())
