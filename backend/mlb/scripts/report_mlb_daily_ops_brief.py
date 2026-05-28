#!/usr/bin/env python3
"""Build a human-readable MLB daily ops brief from existing daily artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def _pct(v: Any, digits: int = 2) -> str:
    f = _as_float(v)
    if f is None:
        return "n/a"
    return f"{f * 100:.{digits}f}%"


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


def _extract_hits_env(js: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    league = (js.get("league_hits_environment") or {}).get("today_vs_baseline") or {}
    starter = js.get("starter_hits_allowed_residual") or {}
    weighted = starter.get("weighted_baseline") or {}
    slate_ctx = js.get("slate_hits_allowed_context") or {}
    team_eval = js.get("team_hits_allowed_matchup_evaluation") or {}
    return {
        "generated_at_utc": js.get("generated_at_utc"),
        "evaluation_date": js.get("evaluation_date"),
        "status": js.get("status"),
        "ok": bool(js.get("ok")),
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


def _fetch_today_workspace_status(slate_date: str) -> Tuple[Dict[str, Any], Optional[str]]:
    status: Dict[str, Any] = {
        "requested_slate_date": slate_date,
        "active_slate_date": None,
        "is_ready": False,
        "row_count": 0,
        "last_updated": None,
        "status": "fail",
        "reason": "not_staged",
    }
    try:
        # Use the exact service function wired behind GET /api/mlb/today/workspace.
        from backend.app.services.mlb.today_workspace_service import fetch_today_workspace

        payload = fetch_today_workspace(slate_date=slate_date, limit=1, offset=0)
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
        status["reason"] = f"error:{type(exc).__name__}"
        return status, f"error:{type(exc).__name__}"


def _derive_overall_status(
    *,
    pipeline: Dict[str, Any],
    ops: Dict[str, Any],
    postgrade: Dict[str, Any],
    hits_env: Dict[str, Any],
    source_issues: Optional[Sequence[str]] = None,
) -> Tuple[str, List[str]]:
    issues: List[str] = []
    fatal = False
    if source_issues:
        fatal = True
        issues.extend(str(issue) for issue in source_issues if str(issue).strip())

    if pipeline and pipeline.get("status") not in {"pass", "ok"}:
        fatal = True
        issues.append(f"pipeline_status={pipeline.get('status')}")
    if ops and ops.get("status") not in {"pass", "ok"}:
        fatal = True
        issues.append(f"ops_status={ops.get('status')}")
    if int(postgrade.get("critical_count") or 0) > 0:
        fatal = True
        issues.append(f"critical_alerts={postgrade.get('critical_count')}")
    if int(postgrade.get("warning_count") or 0) > 0:
        issues.append(f"warning_alerts={postgrade.get('warning_count')}")
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
                    "Critical postgrade signal is active and fade is beating model "
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


def build_markdown(
    *,
    report_date: str,
    generated_at_utc: str,
    overall_status: str,
    overall_issues: Sequence[str],
    pipeline: Dict[str, Any],
    ops: Dict[str, Any],
    postgrade: Dict[str, Any],
    model_vs_fade: Dict[str, Any],
    bvp_impact: Dict[str, Any],
    hits_env: Dict[str, Any],
    prop_regime: Dict[str, Any],
    model_performance: Dict[str, Any],
    reporting_alignment: Dict[str, Any],
    today_workspace: Dict[str, Any],
    path_forward: Sequence[Dict[str, str]],
    source_states: Dict[str, Any],
) -> str:
    lines: List[str] = []
    lines.append(f"# MLB Daily Ops Brief — {report_date}")
    lines.append("")
    lines.append(f"- Generated (UTC): `{generated_at_utc}`")
    lines.append(f"- Overall Status: `{overall_status.upper()}`")
    if overall_issues:
        lines.append(f"- Issues: `{', '.join(overall_issues)}`")
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
    lines.append(
        f"- Report date: `{postgrade.get('report_date','n/a')}` | "
        f"alerts `{postgrade.get('alerts_count',0)}` "
        f"(critical `{postgrade.get('critical_count',0)}`, warning `{postgrade.get('warning_count',0)}`)"
    )
    alerts = postgrade.get("alerts") or []
    if alerts:
        for a in alerts[:8]:
            lines.append(
                f"- [{a.get('severity','n/a')}] `{a.get('type','n/a')}`: {a.get('message') or 'n/a'}"
            )
            if a.get("recommendation"):
                lines.append(f"  - Recommendation: {a.get('recommendation')}")
    else:
        lines.append("- No active postgrade alerts.")
    lines.append("")

    lines.append("## Model vs Fade")
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
    lines.append("")

    lines.append("## Prop Outlook Freshness")
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

    lines.append("## Path Forward")
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
    top_rows = hits_env.get("top_expected_matchups") or []
    low_rows = hits_env.get("lowest_expected_matchups") or []
    top_team_rows = hits_env.get("top_expected_team_matchups") or []
    low_team_rows = hits_env.get("lowest_expected_team_matchups") or []
    if top_rows:
        lines.append(f"- Highest expected hits-allowed matchups (n={len(top_rows)}):")
        for r in top_rows:
            lines.append(_format_matchup_row(r))
    if low_rows:
        lines.append(f"- Lowest expected hits-allowed matchups (n={len(low_rows)}):")
        for r in low_rows:
            lines.append(_format_matchup_row(r))
    if top_team_rows:
        lines.append(f"- Highest expected team hits allowed (starter + bullpen) (n={len(top_team_rows)}):")
        for r in top_team_rows:
            lines.append(_format_team_matchup_row(r))
    if low_team_rows:
        lines.append(f"- Lowest expected team hits allowed (starter + bullpen) (n={len(low_team_rows)}):")
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
        lines.append(f"- Biggest over-expected misses (n={len(top_over_eval)}):")
        for r in top_over_eval:
            lines.append(_format_team_eval_row(r))
    if top_under_eval:
        lines.append(f"- Biggest under-expected misses (n={len(top_under_eval)}):")
        for r in top_under_eval:
            lines.append(_format_team_eval_row(r))
    if hits_env.get("warnings"):
        lines.append(f"- Warnings: `{'; '.join([str(w) for w in hits_env.get('warnings')])}`")
    lines.append("")

    lines.append("## Source Health")
    for name, state in source_states.items():
        lines.append(f"- {name}: `{state}`")
    lines.append("")

    lines.append("## MLB Today Workspace")
    lines.append(f"requested_slate_date: {today_workspace.get('requested_slate_date')}")
    lines.append(f"active_slate_date: {today_workspace.get('active_slate_date')}")
    lines.append(f"row_count: {today_workspace.get('row_count')}")
    lines.append(f"last_updated: {today_workspace.get('last_updated')}")
    ws_status = str(today_workspace.get("status") or "fail").upper()
    lines.append("")
    lines.append(f"Status: {ws_status}")
    if ws_status != "PASS":
        lines.append(f"Reason: {today_workspace.get('reason') or 'unknown'}")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Build MLB daily human-readable ops brief.")
    ap.add_argument("--report-date", default=date.today().isoformat(), help="Label date (YYYY-MM-DD)")
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
        default="backend/mlb/exports/reporting_alignment/reporting_alignment_{report_date}.csv",
    )
    ap.add_argument("--bvp-impact-json", default="artifacts/analysis/mlb/mlb_bvp_impact_latest.json")
    ap.add_argument(
        "--require-fresh-bvp-impact",
        type=int,
        default=1,
        help="Fail the brief when BvP impact label_date does not match report-date (default: 1).",
    )
    ap.add_argument("--hits-environment-json", default="artifacts/analysis/mlb/mlb_hits_environment_latest.json")
    ap.add_argument("--pipeline-history-jsonl", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--ops-history-jsonl", default="artifacts/mlb_prod12_ops_history.jsonl")
    ap.add_argument("--out-md", default="artifacts/analysis/mlb/mlb_daily_ops_brief_latest.md")
    ap.add_argument("--dated-out-md", default="", help="Optional dated markdown output path")
    ap.add_argument("--out-json", default="artifacts/analysis/mlb/mlb_daily_ops_brief_latest.json")
    ap.add_argument("--history-jsonl", default="artifacts/analysis/mlb/mlb_daily_ops_brief_history.jsonl")
    args = ap.parse_args(list(argv) if argv is not None else None)

    generated_at_utc = _utc_now_iso()
    report_date = str(args.report_date).strip()

    postgrade_raw, postgrade_err = _load_json(Path(args.postgrade_alerts_json))
    model_raw, model_err = _load_json(Path(args.model_vs_fade_json))
    prop_regime_rows, prop_regime_err = _load_csv_rows(Path(args.prop_regime_csv))
    model_perf_summary_rows, model_perf_summary_err = _load_csv_rows(Path(args.model_performance_summary_csv))
    model_perf_daily_rows, model_perf_daily_err = _load_csv_rows(Path(args.model_performance_daily_csv))
    reporting_alignment_path = Path(str(args.reporting_alignment_csv).format(report_date=report_date))
    reporting_alignment_rows, reporting_alignment_err = _load_csv_rows(reporting_alignment_path)
    bvp_raw, bvp_err = _load_json(Path(args.bvp_impact_json))
    hits_raw, hits_err = _load_json(Path(args.hits_environment_json))
    pipeline_raw, pipeline_err = _load_last_jsonl(Path(args.pipeline_history_jsonl))
    ops_raw, ops_err = _load_last_jsonl(Path(args.ops_history_jsonl))
    today_workspace, today_workspace_err = _fetch_today_workspace_status(report_date)

    bvp_state, bvp_source_issues = _validate_bvp_impact_freshness(
        bvp_impact=_extract_bvp_impact(bvp_raw if isinstance(bvp_raw, dict) else None),
        bvp_err=bvp_err,
        report_date=report_date,
        require_fresh=int(args.require_fresh_bvp_impact) == 1,
    )

    source_states = {
        "postgrade_alerts_json": postgrade_err or "ok",
        "model_vs_fade_json": model_err or "ok",
        "prop_regime_csv": prop_regime_err or "ok",
        "model_performance_summary_csv": model_perf_summary_err or "ok",
        "model_performance_daily_csv": model_perf_daily_err or "ok",
        "reporting_alignment_csv": reporting_alignment_err or "ok",
        "bvp_impact_json": bvp_state,
        "hits_environment_json": hits_err or "ok",
        "pipeline_history_jsonl": pipeline_err or "ok",
        "ops_history_jsonl": ops_err or "ok",
        "today_workspace": today_workspace_err or "ok",
    }

    postgrade = _extract_postgrade(postgrade_raw if isinstance(postgrade_raw, dict) else None)
    model_vs_fade = _extract_model_vs_fade(model_raw if isinstance(model_raw, dict) else None)
    prop_regime = _extract_prop_regime(prop_regime_rows, Path(args.prop_regime_csv))
    model_performance = _extract_model_performance(
        model_perf_summary_rows,
        model_perf_daily_rows,
        summary_path=Path(args.model_performance_summary_csv),
        daily_path=Path(args.model_performance_daily_csv),
    )
    reporting_alignment = _extract_reporting_alignment(reporting_alignment_rows, reporting_alignment_path)
    bvp_impact = _extract_bvp_impact(bvp_raw if isinstance(bvp_raw, dict) else None)
    hits_env = _extract_hits_env(hits_raw if isinstance(hits_raw, dict) else None)
    pipeline = _extract_pipeline(pipeline_raw)
    ops = _extract_ops(ops_raw)

    overall_status, overall_issues = _derive_overall_status(
        pipeline=pipeline,
        ops=ops,
        postgrade=postgrade,
        hits_env=hits_env,
        source_issues=bvp_source_issues,
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

    md_text = build_markdown(
        report_date=report_date,
        generated_at_utc=generated_at_utc,
        overall_status=overall_status,
        overall_issues=overall_issues,
        pipeline=pipeline,
        ops=ops,
        postgrade=postgrade,
        model_vs_fade=model_vs_fade,
        bvp_impact=bvp_impact,
        hits_env=hits_env,
        prop_regime=prop_regime,
        model_performance=model_performance,
        reporting_alignment=reporting_alignment,
        today_workspace=today_workspace,
        path_forward=path_forward,
        source_states=source_states,
    )

    payload: Dict[str, Any] = {
        "generated_at_utc": generated_at_utc,
        "report_date": report_date,
        "status": overall_status,
        "ok": overall_status == "pass",
        "issues": overall_issues,
        "source_states": source_states,
        "pipeline": pipeline,
        "ops": ops,
        "postgrade": postgrade,
        "model_vs_fade": model_vs_fade,
        "prop_regime": prop_regime,
        "model_performance": model_performance,
        "reporting_alignment": reporting_alignment,
        "bvp_impact": bvp_impact,
        "hits_environment": hits_env,
        "today_workspace": today_workspace,
        "path_forward": path_forward,
        "outputs": {
            "out_md": str(args.out_md),
            "dated_out_md": str(args.dated_out_md) if args.dated_out_md else None,
            "out_json": str(args.out_json),
            "history_jsonl": str(args.history_jsonl),
        },
    }

    out_md = Path(args.out_md)
    _ensure_parent(out_md)
    out_md.write_text(md_text, encoding="utf-8")

    if str(args.dated_out_md).strip():
        dated_md = Path(str(args.dated_out_md).strip())
        _ensure_parent(dated_md)
        dated_md.write_text(md_text, encoding="utf-8")

    out_json = Path(args.out_json)
    _ensure_parent(out_json)
    out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    history_jsonl = Path(args.history_jsonl)
    _ensure_parent(history_jsonl)
    with history_jsonl.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True))
        fh.write("\n")

    print(f"[mlb-daily-ops-brief] report_date={report_date} status={overall_status} out_md={out_md}")
    print(md_text)
    return 0 if overall_status != "fail" else 2


if __name__ == "__main__":
    raise SystemExit(main())
