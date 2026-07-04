#!/usr/bin/env python3
"""Research-only MLB Hits 1.5 PA opportunity shadow test."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())

PA_FIELDS = [
    "plate_appearances",
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
    "pa_source",
    "pa_backfilled_at",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "null", "<na>"}


def _f(value: Any) -> float | None:
    if _is_empty(value):
        return None
    try:
        out = float(str(value).strip())
        return None if math.isnan(out) else out
    except Exception:
        return None


def _i(value: Any) -> int | None:
    number = _f(value)
    return int(number) if number is not None else None


def _s(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader], list(reader.fieldnames or [])


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _side(row: dict[str, Any], default: str = "") -> str:
    side = _s(row.get("side") or row.get("model_pick_side") or default).lower()
    if side in {"o", "over"}:
        return "over"
    if side in {"u", "under"}:
        return "under"
    return side


def _market(row: dict[str, Any], side: str) -> str:
    line = _f(row.get("line"))
    if line is not None and abs(line - 1.5) < 1e-9:
        return "O1.5" if side == "over" else "U1.5" if side == "under" else "Hits 1.5"
    if line is not None and abs(line - 0.5) < 1e-9 and side == "under":
        return "U0.5_context"
    return "other"


def _american_profit(price: float | None, won: bool) -> float | None:
    if price is None:
        return None
    if not won:
        return -1.0
    if price > 0:
        return price / 100.0
    if price < 0:
        return 100.0 / abs(price)
    return None


def _price_for_side(row: dict[str, Any], side: str) -> float | None:
    if side == "over":
        return _f(row.get("price_over_american") or row.get("market_price_over") or row.get("best_over_price") or row.get("market_price") or row.get("board_price"))
    if side == "under":
        return _f(row.get("price_under_american") or row.get("market_price_under") or row.get("market_price") or row.get("board_price"))
    return _f(row.get("market_price") or row.get("board_price") or row.get("odds_used"))


def _outcome_from_actual(row: dict[str, Any], side: str) -> tuple[bool, bool, bool, str]:
    actual = _f(row.get("actual_value"))
    line = _f(row.get("line"))
    if actual is None or line is None:
        return False, False, False, "unresolved"
    if actual == line:
        return False, False, True, "push"
    won = actual > line if side == "over" else actual < line
    return won, not won, False, "win" if won else "loss"


def _normalized_base(row: dict[str, Any], source_family: str, source_path: Path) -> dict[str, Any]:
    side = _side(row)
    market = _market(row, side)
    price = _price_for_side(row, side)
    win = str(row.get("win")).lower() == "true" if "win" in row else False
    loss = str(row.get("loss")).lower() == "true" if "loss" in row else False
    push = str(row.get("push")).lower() == "true" if "push" in row else False
    result = _s(row.get("result") or row.get("outcome") or row.get("win_loss")).lower()
    if result in {"win", "won"}:
        win, loss, push = True, False, False
    elif result in {"loss", "lost"}:
        win, loss, push = False, True, False
    elif result == "push":
        win, loss, push = False, False, True
    elif "actual_value" in row:
        win, loss, push, result = _outcome_from_actual(row, side)
    if not result:
        result = "win" if win else "loss" if loss else "push" if push else "unresolved"
    resolved = win or loss or push or result in {"win", "loss", "push"}
    units = _f(row.get("units") or row.get("roi_result"))
    if units is None:
        if side == "over":
            units = _f(row.get("pnl_over_1u"))
        elif side == "under":
            units = _f(row.get("pnl_under_1u"))
    if units is None and (win or loss) and price is not None:
        units = _american_profit(price, win)
    player_id = _i(row.get("player_id") or row.get("canonical_player_id") or row.get("reconcile_player_id"))
    out = {
        "date": _s(row.get("date") or row.get("game_date") or row.get("slate_date"))[:10],
        "player_id": player_id or "",
        "player_name": _s(row.get("player_name") or row.get("player") or row.get("market_player_name")),
        "team": _s(row.get("team") or row.get("canonical_team") or row.get("player_team")),
        "opponent": _s(row.get("opponent") or row.get("canonical_opponent")),
        "game_id": _s(row.get("game_id") or row.get("canonical_game_id")),
        "market": market,
        "side": side,
        "line": _f(row.get("line")),
        "price": price,
        "resolved": resolved,
        "win": win,
        "loss": loss,
        "push": push,
        "result": result,
        "units": units,
        "actual_value": _f(row.get("actual_value")),
        "source_family": source_family,
        "source_artifact": _rel(source_path),
        "hitter_tier": _s(row.get("hitter_tier") or row.get("current_hitter_tier")),
        "pitcher_tier": _s(row.get("pitcher_tier") or row.get("current_pitcher_tier")),
        "combined_tier": _s(row.get("combined_tier") or row.get("current_combined_tier") or row.get("classification_value")),
        "qc_candidate": _s(row.get("qc_candidate")),
        "qc_score": _f(row.get("qc_score")),
        "qc_overlap": _s(row.get("qc_candidate")).lower() == "true" and bool(_s(row.get("ranking_score") or row.get("ranking_source_lane"))),
        "environment_profile": _s(row.get("env_v2_beta_profile_family") or row.get("v2_beta_profile_family")),
        "provenance_layer": _s(row.get("provenance_layer") or row.get("layer_label") or row.get("layer_label_display") or row.get("alternate_layer")),
        "population": _s(row.get("population") or row.get("manual_population") or row.get("board_name")),
        "board_name": _s(row.get("board_name") or row.get("board")),
    }
    for field in PA_FIELDS:
        out[field] = row.get(field, "")
    for field in ["hit_by_pitch", "sacrifice_flies", "sacrifice_hits", "catcher_interference"]:
        out[field] = row.get(field, "")
    return out


def _load_env_profiles(ledger_path: Path) -> dict[tuple[str, str, str, str, str], str]:
    rows, _ = _read_csv(ledger_path)
    out: dict[tuple[str, str, str, str, str], str] = {}
    for row in rows:
        key = (
            _s(row.get("date"))[:10],
            _s(row.get("player_id") or row.get("canonical_player_id")),
            _s(row.get("game_id") or row.get("canonical_game_id")),
            _side(row),
            str(_f(row.get("line")) or ""),
        )
        profile = _s(row.get("env_v2_beta_profile_family"))
        if profile:
            out[key] = profile
    return out


def _dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = (
            row.get("date"),
            row.get("player_id"),
            row.get("game_id"),
            row.get("market"),
            row.get("side"),
            row.get("line"),
            row.get("source_family"),
            row.get("source_artifact"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _pa_bucket(value: float | None) -> str:
    if value is None:
        return "missing"
    if value < 3.6:
        return "low_<3.6"
    if value < 4.0:
        return "watch_3.6_3.99"
    if value <= 4.4:
        return "standard_4.0_4.4"
    return "high_>4.4"


def _trend_bucket(a: float | None, b: float | None, label: str) -> str:
    if a is None or b is None:
        return f"{label}_missing"
    delta = a - b
    if delta >= 0.35:
        return f"{label}_up"
    if delta <= -0.35:
        return f"{label}_down"
    return f"{label}_stable"


def _ab_per_pa_proxy(row: dict[str, Any]) -> float | None:
    pa = _f(row.get("plate_appearances"))
    if not pa or pa <= 0:
        return None
    hbp = _f(row.get("hit_by_pitch")) or 0.0
    sf = _f(row.get("sacrifice_flies")) or 0.0
    sh = _f(row.get("sacrifice_hits")) or 0.0
    ci = _f(row.get("catcher_interference")) or 0.0
    # Walks are not retained yet; this is an upper-bound proxy for AB opportunity.
    return max(pa - hbp - sf - sh - ci, 0.0) / pa


def _tag(row: dict[str, Any]) -> str:
    side = row.get("side")
    d7 = _f(row.get("d7_plate_appearances"))
    d15 = _f(row.get("d15_plate_appearances"))
    d30 = _f(row.get("d30_plate_appearances"))
    if d7 is None and d15 is None and d30 is None:
        return "missing_pa"
    unstable = (d7 is not None and d15 is not None and abs(d7 - d15) >= 0.5) or (
        d15 is not None and d30 is not None and abs(d15 - d30) >= 0.45
    )
    if unstable:
        return "unstable_opportunity"
    if side == "over" and d15 is not None and d7 is not None and d15 >= 4.4 and d7 >= 4.2:
        return "high_pa_over_support"
    if side == "under" and ((d15 is not None and d15 < 4.0) or (d7 is not None and d7 < 3.8)):
        return "low_pa_under_context"
    if d15 is not None and d15 < 3.8:
        return "opportunity_warning"
    if d15 is not None and d15 >= 4.4:
        return "opportunity_supportive"
    return "neutral_opportunity"


def _sample_flag(resolved: int) -> str:
    if resolved < 10:
        return "tiny"
    if resolved < 25:
        return "small"
    if resolved < 50:
        return "thin"
    return "ok"


def _summarize(rows: list[dict[str, Any]], group_fields: list[str], label: str) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(field, "") for field in group_fields)].append(row)
    out: list[dict[str, Any]] = []
    for key, items in sorted(groups.items(), key=lambda kv: (kv[0],)):
        resolved_items = [r for r in items if r.get("resolved")]
        wins = sum(1 for r in resolved_items if r.get("win"))
        losses = sum(1 for r in resolved_items if r.get("loss"))
        pushes = sum(1 for r in resolved_items if r.get("push"))
        units_vals = [_f(r.get("units")) for r in resolved_items if _f(r.get("units")) is not None]
        price_vals = [_f(r.get("price")) for r in resolved_items if _f(r.get("price")) is not None]
        d7_vals = [_f(r.get("d7_plate_appearances")) for r in items if _f(r.get("d7_plate_appearances")) is not None]
        d15_vals = [_f(r.get("d15_plate_appearances")) for r in items if _f(r.get("d15_plate_appearances")) is not None]
        d30_vals = [_f(r.get("d30_plate_appearances")) for r in items if _f(r.get("d30_plate_appearances")) is not None]
        resolved = len(resolved_items)
        units = sum(units_vals)
        row = {
            "summary_type": label,
            "rows": len(items),
            "resolved": resolved,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "wr": wins / (wins + losses) if wins + losses else "",
            "roi": units / resolved if resolved else "",
            "units": units if units_vals else "",
            "avg_odds": sum(price_vals) / len(price_vals) if price_vals else "",
            "median_odds": median(price_vals) if price_vals else "",
            "sample_flag": _sample_flag(resolved),
            "avg_d7_plate_appearances": sum(d7_vals) / len(d7_vals) if d7_vals else "",
            "avg_d15_plate_appearances": sum(d15_vals) / len(d15_vals) if d15_vals else "",
            "avg_d30_plate_appearances": sum(d30_vals) / len(d30_vals) if d30_vals else "",
        }
        for field, value in zip(group_fields, key):
            row[field] = value
        out.append(row)
    return out


def _enrich_rows(rows: list[dict[str, Any]], env_profiles: dict[tuple[str, str, str, str, str], str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if row.get("market") not in {"O1.5", "U1.5", "U0.5_context"}:
            continue
        d7 = _f(row.get("d7_plate_appearances"))
        d15 = _f(row.get("d15_plate_appearances"))
        d30 = _f(row.get("d30_plate_appearances"))
        row["d7_pa_bucket"] = _pa_bucket(d7)
        row["d15_pa_bucket"] = _pa_bucket(d15)
        row["d30_pa_bucket"] = _pa_bucket(d30)
        row["pa_trend_d7_vs_d15"] = _trend_bucket(d7, d15, "d7_vs_d15")
        row["pa_trend_d15_vs_d30"] = _trend_bucket(d15, d30, "d15_vs_d30")
        row["pa_missing_flag"] = d7 is None and d15 is None and d30 is None
        row["ab_per_pa_proxy"] = _ab_per_pa_proxy(row)
        row["pa_shadow_tag"] = _tag(row)
        if not row.get("environment_profile"):
            key = (
                _s(row.get("date"))[:10],
                _s(row.get("player_id")),
                _s(row.get("game_id")),
                _s(row.get("side")),
                str(_f(row.get("line")) or ""),
            )
            row["environment_profile"] = env_profiles.get(key, "")
        out.append(row)
    return out


def _load_sources(args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_counts: Counter[str] = Counter()

    for path in sorted((ROOT / args.reconcile_root).glob("20??-??-??/reconcile_rows.csv")):
        raw_rows, fields = _read_csv(path)
        if not {"prop_type", "line"}.issubset(fields):
            continue
        for raw in raw_rows:
            if _s(raw.get("prop_type")) != "hits":
                continue
            line = _f(raw.get("line"))
            if line is None or abs(line - 1.5) > 1e-9:
                continue
            for side in ("over", "under"):
                item = dict(raw)
                item["side"] = side
                row = _normalized_base(item, "execution_reconcile", path)
                if row["market"] in {"O1.5", "U1.5"}:
                    rows.append(row)
                    source_counts["execution_reconcile"] += 1

    expanded = ROOT / args.expanded_rows_csv
    expanded_rows, _ = _read_csv(expanded)
    for raw in expanded_rows:
        row = _normalized_base(raw, "expanded_o15_universe", expanded)
        if row["market"] == "O1.5":
            rows.append(row)
            source_counts["expanded_o15_universe"] += 1

    review_root = ROOT / args.review_aids_root
    for path in sorted(review_root.glob("hits_*15_*_2026-*.csv")):
        raw_rows, _ = _read_csv(path)
        source_family = path.stem.rsplit("_", 1)[0]
        for raw in raw_rows:
            row = _normalized_base(raw, source_family, path)
            if row["market"] in {"O1.5", "U1.5", "U0.5_context"}:
                rows.append(row)
                source_counts[source_family] += 1

    env_profiles = _load_env_profiles(ROOT / args.environment_ledger_csv)
    rows = _enrich_rows(_dedupe(rows), env_profiles)
    return rows, {"source_counts": dict(source_counts), "env_profile_keys": len(env_profiles)}


def _recent_misses(rows: list[dict[str, Any]], limit: int = 200) -> list[dict[str, Any]]:
    candidates = [
        r for r in rows
        if r.get("resolved")
        and r.get("loss")
        and (
            r.get("combined_tier") in {"A/A", "A/B", "C/A"}
            or str(r.get("qc_candidate")).lower() == "true"
            or r.get("pa_shadow_tag") in {"opportunity_warning", "unstable_opportunity"}
        )
    ]
    candidates.sort(key=lambda r: str(r.get("date") or ""), reverse=True)
    out: list[dict[str, Any]] = []
    for row in candidates[:limit]:
        reasons = []
        if row.get("combined_tier") in {"A/A", "A/B", "C/A"}:
            reasons.append(f"strong_or_focus_tier:{row.get('combined_tier')}")
        if str(row.get("qc_candidate")).lower() == "true":
            reasons.append("qc_candidate_loss")
        if row.get("pa_shadow_tag") in {"opportunity_warning", "unstable_opportunity"}:
            reasons.append(f"pa_context:{row.get('pa_shadow_tag')}")
        out.append({
            "date": row.get("date"),
            "market": row.get("market"),
            "player_name": row.get("player_name"),
            "team": row.get("team"),
            "opponent": row.get("opponent"),
            "side": row.get("side"),
            "line": row.get("line"),
            "price": row.get("price"),
            "combined_tier": row.get("combined_tier"),
            "qc_candidate": row.get("qc_candidate"),
            "environment_profile": row.get("environment_profile"),
            "pa_shadow_tag": row.get("pa_shadow_tag"),
            "d7_plate_appearances": row.get("d7_plate_appearances"),
            "d15_plate_appearances": row.get("d15_plate_appearances"),
            "d30_plate_appearances": row.get("d30_plate_appearances"),
            "actual_value": row.get("actual_value"),
            "units": row.get("units"),
            "explanation_flags": ";".join(reasons),
            "source_family": row.get("source_family"),
            "source_artifact": row.get("source_artifact"),
        })
    return out


def _report_md(
    path: Path,
    *,
    date_value: str,
    rows: list[dict[str, Any]],
    buckets: list[dict[str, Any]],
    interactions: list[dict[str, Any]],
    misses: list[dict[str, Any]],
    meta: dict[str, Any],
) -> None:
    resolved = [r for r in rows if r.get("resolved")]
    dates = sorted({str(r.get("date")) for r in rows if r.get("date")})
    by_market = Counter(r.get("market") for r in rows)
    by_tag = Counter(r.get("pa_shadow_tag") for r in rows)

    def top(rows_in: list[dict[str, Any]], market: str) -> list[dict[str, Any]]:
        candidates = [
            r for r in rows_in
            if r.get("summary_type") == "shadow_tag_by_market"
            and r.get("market") == market
            and isinstance(r.get("roi"), float)
            and int(r.get("resolved") or 0) >= 10
        ]
        return sorted(candidates, key=lambda r: float(r.get("roi") or -999), reverse=True)[:3]

    o_top = top(buckets, "O1.5")
    u_top = top(buckets, "U1.5")
    strongest_candidates = [
        r for r in buckets
        if r.get("summary_type") == "shadow_tag_by_market"
        and isinstance(r.get("roi"), float)
        and int(r.get("resolved") or 0) >= 10
    ]
    strongest = sorted(strongest_candidates, key=lambda r: float(r.get("roi") or -999), reverse=True)[:1]
    weakest_candidates = [
        r for r in buckets
        if r.get("summary_type") == "shadow_tag_by_market"
        and isinstance(r.get("roi"), float)
        and int(r.get("resolved") or 0) >= 10
    ]
    weakest = sorted(weakest_candidates, key=lambda r: float(r.get("roi") or 999))[:1]
    baseline_rows = [
        r for r in buckets
        if r.get("summary_type")
        in {
            "baseline_by_market",
            "baseline_excluding_opportunity_warning",
            "baseline_excluding_unstable_opportunity",
            "pa_supportive_subset",
        }
    ]

    lines = [
        "# MLB PA Opportunity Shadow Test",
        "",
        f"- Date: `{date_value}`",
        f"- Generated at: `{_utc_now()}`",
        f"- Mode: research-only shadow test.",
        f"- Production behavior changed: `no`",
        "",
        "## Observation Window",
        "",
        f"- First row date: `{dates[0] if dates else ''}`",
        f"- Latest row date: `{dates[-1] if dates else ''}`",
        f"- Total shadow rows: `{len(rows)}`",
        f"- Resolved rows: `{len(resolved)}`",
        f"- Markets: `{dict(by_market)}`",
        f"- Source counts: `{meta.get('source_counts')}`",
        "",
        "## PA Shadow Tags",
        "",
        "| tag | rows |",
        "|---|---:|",
    ]
    for tag, count in by_tag.most_common():
        lines.append(f"| {tag} | `{count}` |")
    lines.extend([
        "",
        "## Baseline Comparison",
        "",
        "| market | view | resolved | W-L-P | WR | ROI | units | sample |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ])
    for row in baseline_rows:
        lines.append(
            f"| {row.get('market')} | {row.get('summary_type')} | `{row.get('resolved')}` | "
            f"`{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | "
            f"`{_pct(row.get('wr'))}` | `{_pct(row.get('roi'))}` | `{_num(row.get('units'))}` | {row.get('sample_flag')} |"
        )
    lines.extend([
        "",
        "## Strongest Shadow Tags",
        "",
        "| market | tag | resolved | W-L-P | WR | ROI | units | sample |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ])
    for row in o_top + u_top:
        lines.append(
            f"| {row.get('market')} | {row.get('pa_shadow_tag')} | `{row.get('resolved')}` | "
            f"`{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | "
            f"`{_pct(row.get('wr'))}` | `{_pct(row.get('roi'))}` | `{_num(row.get('units'))}` | {row.get('sample_flag')} |"
        )
    lines.extend([
        "",
        "## Recent Miss Explanation",
        "",
        f"- Recent/focus losses reviewed: `{len(misses)}`",
        "- The CSV lists strong-tier, QC, unstable, and warning-tag misses with PA context attached.",
        "",
        "## Early Interpretation",
        "",
        "- PA appears most useful as an explanatory/context badge in this first pass, especially around opportunity warnings and unstable opportunity.",
        "- O1.5 and U1.5 should remain separate; the PA tags behave differently by side.",
        "- This test does not justify production selectors, thresholds, or upload changes.",
        "",
        "## Final Answers",
        "",
        f"- PA useful for O1.5: `{_directional_answer(o_top)}`",
        f"- PA useful for U1.5: `{_directional_answer(u_top)}`",
        f"- Strongest PA opportunity tag: `{strongest[0].get('pa_shadow_tag') if strongest else 'insufficient_sample'}`",
        f"- Weakest PA opportunity tag: `{weakest[0].get('pa_shadow_tag') if weakest else 'insufficient_sample'}`",
        "- Recommended status: `continue research-only shadow cohort; consider Morning Workbench context only after more resolved U1.5/O1.5 live rows`",
        "- Evidence still needed: larger resolved samples by side, direct AB/walk retention, and postgame validation over multiple slates.",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _pct(value: Any) -> str:
    number = _f(value)
    return "" if number is None else f"{number * 100:.2f}%"


def _num(value: Any) -> str:
    number = _f(value)
    return "" if number is None else f"{number:.2f}"


def _directional_answer(top_rows: list[dict[str, Any]]) -> str:
    if not top_rows:
        return "insufficient_sample"
    best = top_rows[0]
    if int(best.get("resolved") or 0) < 25:
        return "promising_but_thin"
    return "directionally_useful_as_context" if float(best.get("roi") or 0) > 0 else "not_clearly_useful_yet"


def run(args: argparse.Namespace) -> int:
    out_dir = ROOT / args.out_dir
    date_value = args.date
    rows, meta = _load_sources(args)

    bucket_rows: list[dict[str, Any]] = []
    bucket_rows.extend(_summarize(rows, ["market"], "baseline_by_market"))
    bucket_rows.extend(_summarize([r for r in rows if r.get("pa_shadow_tag") != "opportunity_warning"], ["market"], "baseline_excluding_opportunity_warning"))
    bucket_rows.extend(_summarize([r for r in rows if r.get("pa_shadow_tag") != "unstable_opportunity"], ["market"], "baseline_excluding_unstable_opportunity"))
    bucket_rows.extend(_summarize([r for r in rows if r.get("pa_shadow_tag") in {"high_pa_over_support", "low_pa_under_context", "opportunity_supportive"}], ["market"], "pa_supportive_subset"))
    bucket_rows.extend(_summarize(rows, ["market", "d15_pa_bucket"], "d15_pa_bucket_by_market"))
    bucket_rows.extend(_summarize(rows, ["market", "d30_pa_bucket"], "d30_pa_bucket_by_market"))
    bucket_rows.extend(_summarize(rows, ["market", "pa_trend_d7_vs_d15"], "pa_trend_d7_vs_d15_by_market"))
    bucket_rows.extend(_summarize(rows, ["market", "pa_trend_d15_vs_d30"], "pa_trend_d15_vs_d30_by_market"))
    bucket_rows.extend(_summarize(rows, ["market", "pa_shadow_tag"], "shadow_tag_by_market"))

    interaction_rows: list[dict[str, Any]] = []
    for field in ["combined_tier", "qc_candidate", "qc_overlap", "environment_profile", "provenance_layer", "population"]:
        interaction_rows.extend(_summarize(rows, ["market", field, "pa_shadow_tag"], f"{field}_x_pa_shadow_tag"))
    for tier in ["A/A", "A/B", "C/A", "B/A", "B/B", "C/U"]:
        subset = [r for r in rows if r.get("combined_tier") == tier]
        interaction_rows.extend(_summarize(subset, ["market", "combined_tier", "d15_pa_bucket"], f"{tier}_d15_pa_bucket"))

    misses = _recent_misses(rows)

    row_path = out_dir / f"pa_opportunity_shadow_rows_{date_value}.csv"
    bucket_path = out_dir / f"pa_opportunity_shadow_buckets_{date_value}.csv"
    interaction_path = out_dir / f"pa_opportunity_shadow_interactions_{date_value}.csv"
    miss_path = out_dir / f"pa_opportunity_recent_miss_explanation_{date_value}.csv"
    report_path = out_dir / f"pa_opportunity_shadow_test_{date_value}.md"
    summary_path = out_dir / f"pa_opportunity_shadow_summary_{date_value}.json"

    _write_csv(row_path, rows)
    _write_csv(bucket_path, bucket_rows)
    _write_csv(interaction_path, interaction_rows)
    _write_csv(miss_path, misses)
    _report_md(report_path, date_value=date_value, rows=rows, buckets=bucket_rows, interactions=interaction_rows, misses=misses, meta=meta)
    _write_json(summary_path, {
        "date": date_value,
        "generated_at": _utc_now(),
        "rows": len(rows),
        "resolved": sum(1 for r in rows if r.get("resolved")),
        "markets": dict(Counter(r.get("market") for r in rows)),
        "source_counts": meta.get("source_counts"),
        "outputs": {
            "report": _rel(report_path),
            "rows_csv": _rel(row_path),
            "buckets_csv": _rel(bucket_path),
            "interactions_csv": _rel(interaction_path),
            "misses_csv": _rel(miss_path),
        },
        "production_behavior_changed": False,
    })
    print(json.dumps({"rows": len(rows), "resolved": sum(1 for r in rows if r.get("resolved")), "out": _rel(report_path)}, indent=2))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--out-dir", default="artifacts/analysis/mlb/pa_foundation")
    parser.add_argument("--reconcile-root", default="artifacts/analysis/mlb/execution_vs_model")
    parser.add_argument("--expanded-rows-csv", default="artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
    parser.add_argument("--review-aids-root", default="artifacts/analysis/mlb/review_aids")
    parser.add_argument("--environment-ledger-csv", default="artifacts/analysis/mlb/environment_v2/ledger/environment_v2_beta_profile_ledger.csv")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
