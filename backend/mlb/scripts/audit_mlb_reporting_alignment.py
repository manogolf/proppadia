#!/usr/bin/env python3
"""Audit cross-surface MLB prop reporting alignment.

This report intentionally does not change any business logic. It explains why
Ops Brief, full-slate reconciliation, and /mlb/today Prop Outlook can disagree:
they are built from different source artifacts, windows, and metrics.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_POSTGRADE_ALERTS_JSON = Path("artifacts/analysis/mlb/mlb_postgrade_alerts_latest.json")
DEFAULT_MODEL_VS_FADE_JSON = Path("tmp/analysis/mlb_model_vs_fade_summary.json")
DEFAULT_MODEL_VS_FADE_BY_PROP_CSV = Path("tmp/analysis/mlb_model_vs_fade_by_prop.csv")
DEFAULT_OUTLOOK_CSV = Path("backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv")


def _load_json(path: Path) -> tuple[dict[str, Any], str | None]:
    if not path.exists():
        return {}, f"missing: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {}, f"failed to read {path}: {exc}"
    if not isinstance(payload, dict):
        return {}, f"expected object json: {path}"
    return payload, None


def _load_csv(path: Path) -> tuple[pd.DataFrame, str | None]:
    if not path.exists():
        return pd.DataFrame(), f"missing: {path}"
    try:
        return pd.read_csv(path, low_memory=False), None
    except Exception as exc:
        return pd.DataFrame(), f"failed to read {path}: {exc}"


def _clean_prop(value: Any) -> str:
    return str(value or "").strip().lower()


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if pd.isna(out):
            return None
        return out
    except Exception:
        return None


def _int(value: Any) -> int | None:
    n = _num(value)
    if n is None:
        return None
    return int(n)


def _date_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _fmt_pct(value: Any) -> str:
    n = _num(value)
    if n is None:
        return "n/a"
    return f"{n * 100:.1f}%"


def _read_ops_sources() -> tuple[dict[str, Any], dict[str, dict[str, Any]], list[str]]:
    notes: list[str] = []
    postgrade, err = _load_json(DEFAULT_POSTGRADE_ALERTS_JSON)
    if err:
        notes.append(err)
    model_summary, err = _load_json(DEFAULT_MODEL_VS_FADE_JSON)
    if err:
        notes.append(err)
    by_prop, err = _load_csv(DEFAULT_MODEL_VS_FADE_BY_PROP_CSV)
    if err:
        notes.append(err)

    ops_report_date = _date_text(postgrade.get("report_date"))
    window = model_summary.get("window", {}) if isinstance(model_summary.get("window"), dict) else {}
    ops_model_date = _date_text(window.get("game_date_max")) or _date_text(window.get("game_date_min"))
    alerts = postgrade.get("alerts") if isinstance(postgrade.get("alerts"), list) else []
    alerts_by_prop: dict[str, list[dict[str, Any]]] = {}
    for alert in alerts:
        if not isinstance(alert, dict):
            continue
        prop = _clean_prop(alert.get("prop_type"))
        if prop:
            alerts_by_prop.setdefault(prop, []).append(alert)

    by_prop_rows: dict[str, dict[str, Any]] = {}
    if not by_prop.empty:
        by_prop = by_prop.copy()
        if "prop_type" in by_prop.columns:
            by_prop["prop_type"] = by_prop["prop_type"].map(_clean_prop)
            for _, row in by_prop.iterrows():
                prop = _clean_prop(row.get("prop_type"))
                if prop:
                    by_prop_rows[prop] = row.to_dict()

    out: dict[str, dict[str, Any]] = {}
    for prop in sorted(set(by_prop_rows) | set(alerts_by_prop)):
        row = by_prop_rows.get(prop, {})
        prop_alerts = alerts_by_prop.get(prop, [])
        alert_text = "no_active_prop_alert"
        if prop_alerts:
            alert_text = "; ".join(
                f"{a.get('severity', 'alert')}:{a.get('code', 'unknown')}" for a in prop_alerts
            )
        elif str(row.get("fade_beating_model_alert", "")).strip().lower() in {"true", "1", "yes"}:
            alert_text = "fade_beating_model_alert"
        out[prop] = {
            "ops_postgrade_date": ops_report_date or ops_model_date,
            "ops_model_window_date": ops_model_date,
            "ops_model_roi": _num(row.get("model_roi_1u")),
            "ops_model_win_rate": _num(row.get("model_win_rate")),
            "ops_paired_bets": _int(row.get("paired_bets")),
            "ops_status_or_alert": alert_text,
        }
    return {"postgrade": postgrade, "model_summary": model_summary}, out, notes


def _read_reconcile_sources(date_text: str) -> tuple[dict[str, dict[str, Any]], list[str]]:
    notes: list[str] = []
    base = Path("artifacts/analysis/mlb/execution_vs_model") / date_text
    summary, err = _load_json(base / "reconcile_summary.json")
    if err:
        notes.append(err)
    by_prop, err = _load_csv(base / "full_slate_by_prop.csv")
    if err:
        notes.append(err)
    reconcile_rows, err = _load_csv(base / "reconcile_rows.csv")
    if err:
        notes.append(err)

    reconcile_date = _date_text(summary.get("to_date")) or _date_text(summary.get("from_date")) or date_text
    out: dict[str, dict[str, Any]] = {}
    if not by_prop.empty and "prop_type" in by_prop.columns:
        by_prop = by_prop.copy()
        by_prop["prop_type"] = by_prop["prop_type"].map(_clean_prop)
        for _, row in by_prop.iterrows():
            prop = _clean_prop(row.get("prop_type"))
            if not prop:
                continue
            out[prop] = {
                "reconcile_date": reconcile_date,
                "reconcile_model_roi": _num(row.get("model_roi")),
                "reconcile_model_win_rate": _num(row.get("model_win_rate")),
                "reconcile_paired_bets": _int(row.get("rows")),
            }
        return out, notes

    required = {"prop_type", "actual_model_pick_outcome", "pnl_model_pick_1u"}
    if reconcile_rows.empty or not required.issubset(reconcile_rows.columns):
        return out, notes

    rows = reconcile_rows.copy()
    rows["prop_type"] = rows["prop_type"].map(_clean_prop)
    rows["pnl_model_pick_1u"] = pd.to_numeric(rows["pnl_model_pick_1u"], errors="coerce")
    rows = rows[rows["pnl_model_pick_1u"].notna()].copy()
    outcomes = rows["actual_model_pick_outcome"].astype(str).str.lower().str.strip()
    rows = rows[outcomes.isin({"win", "loss", "push"})].copy()
    outcomes = rows["actual_model_pick_outcome"].astype(str).str.lower().str.strip()
    for prop, g in rows.groupby("prop_type", dropna=False):
        prop = _clean_prop(prop)
        if not prop:
            continue
        wl = outcomes.loc[g.index].isin({"win", "loss"})
        wins = outcomes.loc[g.index].eq("win")
        out[prop] = {
            "reconcile_date": reconcile_date,
            "reconcile_model_roi": _num(g["pnl_model_pick_1u"].mean()),
            "reconcile_model_win_rate": _num(wins[wl].mean()) if bool(wl.any()) else None,
            "reconcile_paired_bets": int(len(g)),
        }
    if out:
        notes.append("full_slate_by_prop.csv unavailable; derived reconcile metrics from reconcile_rows.csv")
    return out, notes


def _read_outlook_source() -> tuple[dict[str, dict[str, Any]], list[str]]:
    notes: list[str] = []
    df, err = _load_csv(DEFAULT_OUTLOOK_CSV)
    if err:
        notes.append(err)
        return {}, notes
    out: dict[str, dict[str, Any]] = {}
    if df.empty or "prop_type" not in df.columns:
        notes.append(f"no prop_type rows in {DEFAULT_OUTLOOK_CSV}")
        return out, notes
    df = df.copy()
    df["prop_type"] = df["prop_type"].map(_clean_prop)
    for _, row in df.iterrows():
        prop = _clean_prop(row.get("prop_type"))
        if not prop:
            continue
        out[prop] = {
            "outlook_latest_usable_date": _date_text(row.get("latest_usable_date")),
            "outlook_label": _date_text(row.get("regime_context_label")),
            "outlook_score": _num(row.get("regime_context_score")),
        }
    return out, notes


def _try_today_workspace_counts(date_text: str) -> tuple[dict[str, int], str | None]:
    """Use the same service behind /api/mlb/today/workspace when available.

    This is read-only. In sandboxed/offline contexts it may fail because the
    workspace rows live in Postgres; the audit remains useful without counts.
    """
    try:
        from backend.domains.mlb.today_workspace import fetch_today_workspace_rows

        payload = fetch_today_workspace_rows(slate_date=date_text, limit=5000, offset=0)
    except Exception as exc:
        return {}, f"workspace DB/API source unavailable for today_rows: {exc}"

    counts: dict[str, int] = {}
    for row in payload.get("regime_context_by_prop") or []:
        if not isinstance(row, dict):
            continue
        prop = _clean_prop(row.get("prop_type"))
        if prop:
            counts[prop] = int(_int(row.get("today_rows")) or _int(row.get("row_count")) or 0)
    return counts, None


def _classify(
    *,
    requested_date: str,
    ops: dict[str, Any] | None,
    reconcile: dict[str, Any] | None,
    outlook: dict[str, Any] | None,
) -> tuple[str, str]:
    if not reconcile:
        return "missing_reconcile", "No daily full-slate reconciliation row exists for this prop/date."
    if not outlook:
        return "missing_outlook", "No Prop Outlook regime context row exists for this prop."

    ops_date = _date_text((ops or {}).get("ops_model_window_date") or (ops or {}).get("ops_postgrade_date"))
    outlook_date = _date_text(outlook.get("outlook_latest_usable_date"))
    if outlook_date and outlook_date < requested_date:
        status = "stale_outlook_source"
        explanation = (
            f"Prop Outlook latest_usable_date is {outlook_date}, while the audit date is {requested_date}; "
            "the card is showing regime context from an older validation window."
        )
        if ops_date and ops_date != requested_date:
            explanation += f" Ops model-vs-fade window is also {ops_date}."
        return status, explanation

    if not ops:
        return (
            "expected_difference_due_to_window",
            "Ops Brief has no by-prop model-vs-fade row for this prop; daily reconciliation and Prop Outlook still have their own sources.",
        )

    if ops_date and ops_date != requested_date:
        return (
            "expected_difference_due_to_window",
            f"Ops model-vs-fade is dated {ops_date}, while reconciliation is dated {requested_date}.",
        )

    ops_roi = _num(ops.get("ops_model_roi"))
    rec_roi = _num(reconcile.get("reconcile_model_roi"))
    if ops_roi is None or rec_roi is None:
        return "potential_conflict_needs_review", "One surface is missing ROI, so alignment cannot be numerically verified."

    if abs(ops_roi - rec_roi) <= 0.03:
        return "aligned", "Ops model-vs-fade and full-slate reconciliation are directionally close; Prop Outlook is contextual."

    return (
        "expected_difference_due_to_metric",
        "Ops model-vs-fade uses paired two-sided model-vs-opposite-side rows; daily reconciliation uses full-slate model-pick rows. "
        f"ROI differs by {ops_roi - rec_roi:+.3f}, which can be expected when row sets differ.",
    )


def _build_markdown(date_text: str, rows: list[dict[str, Any]], notes: list[str]) -> str:
    counts = pd.Series([r["alignment_status"] for r in rows]).value_counts().to_dict() if rows else {}
    lines = [
        f"# MLB Reporting Alignment Audit - {date_text}",
        "",
        "This audit compares Ops Brief model-vs-fade artifacts, daily full-slate reconciliation artifacts, and the Prop Outlook regime context source.",
        "",
        "## Status Counts",
    ]
    if counts:
        for status, count in counts.items():
            lines.append(f"- `{status}`: {count}")
    else:
        lines.append("- No prop rows found.")

    if notes:
        lines.extend(["", "## Source Notes"])
        for note in notes:
            lines.append(f"- {note}")

    lines.extend(["", "## Prop Details", ""])
    header = "| prop_type | alignment_status | ops ROI/WR | reconcile ROI/WR | outlook | explanation |"
    sep = "|---|---|---:|---:|---|---|"
    lines.extend([header, sep])
    for row in rows:
        ops_metrics = f"{_fmt_pct(row.get('ops_model_roi'))} / {_fmt_pct(row.get('ops_model_win_rate'))}"
        rec_metrics = f"{_fmt_pct(row.get('reconcile_model_roi'))} / {_fmt_pct(row.get('reconcile_model_win_rate'))}"
        outlook = (
            f"{row.get('outlook_label') or 'n/a'} "
            f"({_fmt_pct((row.get('outlook_score') or 0) / 100) if row.get('outlook_score') is not None else 'n/a'}, "
            f"date {row.get('outlook_latest_usable_date') or 'n/a'}, rows {row.get('outlook_today_rows') if row.get('outlook_today_rows') is not None else 'n/a'})"
        )
        explanation = str(row.get("explanation") or "").replace("|", "\\|")
        lines.append(
            f"| `{row.get('prop_type')}` | `{row.get('alignment_status')}` | {ops_metrics} | {rec_metrics} | {outlook} | {explanation} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit alignment across MLB reporting surfaces by prop.")
    ap.add_argument("--date", required=True, help="Date to audit, YYYY-MM-DD.")
    ap.add_argument(
        "--out-csv",
        default="backend/mlb/exports/reporting_alignment/reporting_alignment_{date}.csv",
        help="Output CSV path. May include {date}.",
    )
    ap.add_argument(
        "--out-md",
        default="backend/mlb/exports/reporting_alignment/reporting_alignment_{date}.md",
        help="Output Markdown path. May include {date}.",
    )
    args = ap.parse_args()

    date_text = str(args.date).strip()
    pd.Timestamp(date_text)

    _, ops_by_prop, ops_notes = _read_ops_sources()
    reconcile_by_prop, reconcile_notes = _read_reconcile_sources(date_text)
    outlook_by_prop, outlook_notes = _read_outlook_source()
    today_counts, counts_note = _try_today_workspace_counts(date_text)
    today_counts_available = counts_note is None

    notes = ops_notes + reconcile_notes + outlook_notes
    if counts_note:
        notes.append(counts_note)

    props = sorted(set(ops_by_prop) | set(reconcile_by_prop) | set(outlook_by_prop) | set(today_counts))
    rows: list[dict[str, Any]] = []
    for prop in props:
        ops = ops_by_prop.get(prop)
        reconcile = reconcile_by_prop.get(prop)
        outlook = outlook_by_prop.get(prop)
        status, explanation = _classify(
            requested_date=date_text,
            ops=ops,
            reconcile=reconcile,
            outlook=outlook,
        )
        rows.append(
            {
                "prop_type": prop,
                "ops_postgrade_date": (ops or {}).get("ops_postgrade_date"),
                "ops_model_roi": (ops or {}).get("ops_model_roi"),
                "ops_model_win_rate": (ops or {}).get("ops_model_win_rate"),
                "ops_status_or_alert": (ops or {}).get("ops_status_or_alert"),
                "reconcile_date": (reconcile or {}).get("reconcile_date"),
                "reconcile_model_roi": (reconcile or {}).get("reconcile_model_roi"),
                "reconcile_model_win_rate": (reconcile or {}).get("reconcile_model_win_rate"),
                "reconcile_paired_bets": (reconcile or {}).get("reconcile_paired_bets"),
                "outlook_latest_usable_date": (outlook or {}).get("outlook_latest_usable_date"),
                "outlook_label": (outlook or {}).get("outlook_label"),
                "outlook_score": (outlook or {}).get("outlook_score"),
                "outlook_today_rows": today_counts.get(prop, 0) if today_counts_available else None,
                "alignment_status": status,
                "explanation": explanation,
            }
        )

    out_csv = Path(str(args.out_csv).format(date=date_text))
    out_md = Path(str(args.out_md).format(date=date_text))
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(rows)
    cols = [
        "prop_type",
        "ops_postgrade_date",
        "ops_model_roi",
        "ops_model_win_rate",
        "ops_status_or_alert",
        "reconcile_date",
        "reconcile_model_roi",
        "reconcile_model_win_rate",
        "reconcile_paired_bets",
        "outlook_latest_usable_date",
        "outlook_label",
        "outlook_score",
        "outlook_today_rows",
        "alignment_status",
        "explanation",
    ]
    df = df.reindex(columns=cols)
    df.to_csv(out_csv, index=False)
    out_md.write_text(_build_markdown(date_text, rows, notes), encoding="utf-8")

    counts = df["alignment_status"].value_counts().to_dict() if not df.empty else {}
    print(
        json.dumps(
            {
                "date": date_text,
                "rows": int(len(df)),
                "alignment_status_counts": counts,
                "out_csv": str(out_csv),
                "out_md": str(out_md),
                "notes": notes,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
