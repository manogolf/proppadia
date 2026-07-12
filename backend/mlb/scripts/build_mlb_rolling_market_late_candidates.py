#!/usr/bin/env python3
"""Build dry-run rolling market-late candidate observation artifacts.

This utility reads existing run-tagged daily artifacts and produces a candidate
ledger plus a latest/current projection. It does not fetch odds, write to the
database, mutate source artifacts, or change upload behavior.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_ODDS_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_LANE_ROOT = Path("backend/mlb/exports/model_v2/lanes/today")
DEFAULT_LINEUP_ROOT = Path("artifacts/analysis/mlb/pregame_lineup_capture/dry_runs")


@dataclass(frozen=True)
class RunArtifact:
    run_tag: str
    path: Path
    run_order: int


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _num(value: Any) -> float | None:
    try:
        out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    except Exception:
        return None
    return float(out) if pd.notna(out) else None


def _id_text(value: Any) -> str:
    num = _num(value)
    if num is not None:
        return str(int(num))
    return _clean_text(value)


def _run_tag_from_slate_path(path: Path) -> str:
    stem = path.stem
    if "__" in stem:
        return stem.split("__", 1)[1]
    return "canonical_latest"


def _discover_slate_runs(odds_root: Path, date_value: str) -> list[RunArtifact]:
    day_dir = odds_root / date_value
    paths = sorted(day_dir.glob("mlb_slate_output__local_daily_*.csv"))
    return [
        RunArtifact(run_tag=_run_tag_from_slate_path(path), path=path, run_order=i + 1)
        for i, path in enumerate(paths)
    ]


def _candidate_key(row: pd.Series) -> str:
    return "|".join(
        [
            _id_text(row.get("game_id")),
            _id_text(row.get("player_id")),
            _clean_text(row.get("prop_type")).lower(),
            str(_num(row.get("line")) if _num(row.get("line")) is not None else _clean_text(row.get("line"))),
            _clean_text(row.get("model_pick_side") or row.get("side")).lower(),
        ]
    )


def _line_key(row: pd.Series) -> str:
    return "|".join(
        [
            _id_text(row.get("game_id")),
            _id_text(row.get("player_id")),
            _clean_text(row.get("prop_type")).lower(),
            str(_num(row.get("line")) if _num(row.get("line")) is not None else _clean_text(row.get("line"))),
        ]
    )


def _bucket_lineup_slot(value: Any) -> str:
    slot = _num(value)
    if slot is None:
        return "unknown"
    if 1 <= slot <= 3:
        return "top_order"
    if 4 <= slot <= 6:
        return "middle_order"
    if 7 <= slot <= 9:
        return "bottom_order"
    return "unknown"


def _is_before_timestamp(left: Any, right: Any) -> bool | str:
    left_text = _clean_text(left)
    right_text = _clean_text(right)
    if not left_text or not right_text:
        return ""
    left_dt = pd.to_datetime(left_text, errors="coerce", utc=True)
    right_dt = pd.to_datetime(right_text, errors="coerce", utc=True)
    if pd.isna(left_dt) or pd.isna(right_dt):
        return ""
    return bool(left_dt < right_dt)


def _load_lineup_overlay(lineup_root: Path, date_value: str) -> pd.DataFrame:
    date_root = lineup_root / date_value
    paths = sorted(date_root.glob("run_*/pregame_lineup_player_rows_*.csv"))
    if not paths:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            frame = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        if frame.empty:
            continue
        required = {"game_id", "player_id"}
        if not required.issubset(frame.columns):
            continue
        frame = frame.copy()
        frame["_source_path"] = str(path)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["game_id_key"] = out["game_id"].map(_id_text)
    out["player_id_key"] = out["player_id"].map(_id_text)
    out["_source_ts"] = out.get("source_fetched_at_utc", pd.Series("", index=out.index)).map(_clean_text)
    out = out.sort_values(["_source_ts", "_source_path"], kind="stable")
    out = out.drop_duplicates(["game_id_key", "player_id_key"], keep="last")
    keep = [
        "game_id_key",
        "player_id_key",
        "lineup_slot",
        "lineup_bucket",
        "team_lineup_status",
        "lineup_slot_semantics",
        "source_fetched_at_utc",
        "_source_path",
    ]
    for col in keep:
        if col not in out.columns:
            out[col] = pd.NA
    out["lineup_bucket"] = out["lineup_bucket"].where(
        out["lineup_bucket"].notna(),
        out["lineup_slot"].map(_bucket_lineup_slot),
    )
    return out[keep].rename(
        columns={
            "lineup_slot": "latest_lineup_slot",
            "lineup_bucket": "latest_lineup_bucket",
            "team_lineup_status": "latest_lineup_status",
            "lineup_slot_semantics": "lineup_slot_semantics",
            "source_fetched_at_utc": "lineup_source_fetched_at_utc",
            "_source_path": "lineup_source_artifact",
        }
    )


def _read_slate_run(run: RunArtifact) -> pd.DataFrame:
    frame = pd.read_csv(run.path, low_memory=False)
    if frame.empty:
        return frame
    out = frame.copy()
    out["run_tag"] = run.run_tag
    out["run_order"] = run.run_order
    out["source_artifact"] = str(run.path)
    if "market_snapshot_run_tag" not in out.columns:
        out["market_snapshot_run_tag"] = run.run_tag
    if "market_snapshot_time_utc" not in out.columns:
        out["market_snapshot_time_utc"] = pd.NA
    if "game_time" not in out.columns:
        out["game_time"] = pd.NA
    if "model_pick_side" not in out.columns:
        out["model_pick_side"] = pd.NA
    out["candidate_key"] = out.apply(_candidate_key, axis=1)
    out["line_key"] = out.apply(_line_key, axis=1)
    return out


def _summarize_key(group: pd.DataFrame, *, latest_order: int, baseline_order: int) -> dict[str, Any]:
    group = group.sort_values("run_order", kind="stable")
    first = group.iloc[0]
    last = group.iloc[-1]
    orders = [int(x) for x in group["run_order"].dropna().tolist()]
    present_orders = set(orders)
    current_present = latest_order in present_orders
    baseline_present = baseline_order in present_orders
    reappeared = False
    if len(orders) >= 2:
        for left, right in zip(orders, orders[1:]):
            if right - left > 1 and right == latest_order:
                reappeared = True
                break
    if not current_present:
        discovery_class = "disappeared_candidate"
    elif reappeared:
        discovery_class = "reappeared_candidate"
    elif not baseline_present:
        discovery_class = "late_discovered_candidate"
    elif len(present_orders) == latest_order - baseline_order + 1:
        discovery_class = "persistent_candidate"
    else:
        discovery_class = "original_morning_candidate"

    first_ts = _clean_text(first.get("market_snapshot_time_utc")) or _clean_text(first.get("generated_at_utc"))
    last_ts = _clean_text(last.get("market_snapshot_time_utc")) or _clean_text(last.get("generated_at_utc"))
    game_start_time = _clean_text(last.get("game_time"))
    discovered_before_game_start = _is_before_timestamp(first_ts, game_start_time)
    current_before_game_start = _is_before_timestamp(last_ts, game_start_time)
    price_over_first = _num(first.get("market_price_over"))
    price_over_last = _num(last.get("market_price_over"))
    price_under_first = _num(first.get("market_price_under"))
    price_under_last = _num(last.get("market_price_under"))
    if not current_present:
        line_freshness_status = "stale_missing_current"
        operational_status = "historical_only"
    elif current_before_game_start is False:
        line_freshness_status = "stale_game_started"
        operational_status = "excluded_game_started"
    else:
        line_freshness_status = "fresh_current"
        operational_status = "current_eligible"
    odds_movement_status = "unchanged"
    if current_present and (
        price_over_first != price_over_last
        or price_under_first != price_under_last
        or _num(first.get("market_book_count_two_sided")) != _num(last.get("market_book_count_two_sided"))
    ):
        odds_movement_status = "moved_price_or_book_count"

    return {
        "candidate_key": first.get("candidate_key"),
        "line_key": first.get("line_key"),
        "game_id": _id_text(last.get("game_id")),
        "game_date": _clean_text(last.get("game_date") or last.get("slate_date")),
        "game_start_time": game_start_time,
        "player_id": _id_text(last.get("player_id")),
        "player_name": _clean_text(last.get("player_name")),
        "team": _clean_text(last.get("team")),
        "opponent": _clean_text(last.get("opponent")),
        "prop_type": _clean_text(last.get("prop_type")).lower(),
        "market_key": _clean_text(last.get("market_key")),
        "line": _num(last.get("line")),
        "side": _clean_text(last.get("model_pick_side")).lower(),
        "first_seen_timestamp": first_ts,
        "first_seen_run_tag": _clean_text(first.get("run_tag")),
        "first_seen_run_order": int(first.get("run_order")),
        "last_seen_timestamp": last_ts,
        "last_seen_run_tag": _clean_text(last.get("run_tag")),
        "last_seen_run_order": int(last.get("run_order")),
        "current_surface_present": bool(current_present),
        "market_available_now": bool(current_present),
        "discovery_class": discovery_class,
        "game_start_time_present": bool(_clean_text(last.get("game_time"))),
        "discovered_before_game_start": discovered_before_game_start,
        "current_before_game_start": current_before_game_start,
        "model_context_run_tag": _clean_text(last.get("run_tag")),
        "odds_snapshot_run_tag": _clean_text(last.get("market_snapshot_run_tag")) or _clean_text(last.get("run_tag")),
        "line_freshness_status": line_freshness_status,
        "operational_candidate_status": operational_status,
        "odds_movement_status": odds_movement_status,
        "price_over_first": price_over_first,
        "price_under_first": price_under_first,
        "price_over_current_or_last": price_over_last,
        "price_under_current_or_last": price_under_last,
        "book_count_first": _num(first.get("market_book_count_two_sided")),
        "book_count_current_or_last": _num(last.get("market_book_count_two_sided")),
        "prob_over_current_or_last": _num(last.get("prob_over")),
        "prob_under_current_or_last": _num(last.get("prob_under")),
        "model_pick_prob_current_or_last": _num(last.get("model_pick_prob")),
        "selected_side_price_current_or_last": _num(last.get("selected_side_price")),
        "model_vs_market_gap_current_or_last": _num(last.get("model_vs_market_gap")),
        "times_seen": int(group["run_order"].nunique()),
        "run_orders_seen": ",".join(str(x) for x in sorted(present_orders)),
        "source_artifact_current_or_last": _clean_text(last.get("source_artifact")),
    }


def _build_growth_summary(ledger: pd.DataFrame, runs: list[RunArtifact]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    prior_present: set[str] = set()
    for run in runs:
        present = set(
            ledger.loc[
                ledger["run_orders_seen"].astype(str).str.split(",").map(lambda vals: str(run.run_order) in vals),
                "candidate_key",
            ].astype(str)
        )
        new = present - seen
        carried = present & prior_present
        removed = prior_present - present
        reappeared = present & seen - prior_present
        rows.append(
            {
                "run_order": run.run_order,
                "run_tag": run.run_tag,
                "source_artifact": str(run.path),
                "candidates_present": len(present),
                "new_candidates": len(new),
                "carried_forward_candidates": len(carried),
                "removed_since_previous_run": len(removed),
                "reappeared_candidates": len(reappeared),
                "hits_15_candidates_present": int(
                    ledger[
                        ledger["candidate_key"].isin(present)
                        & ledger["prop_type"].eq("hits")
                        & pd.to_numeric(ledger["line"], errors="coerce").eq(1.5)
                    ].shape[0]
                ),
            }
        )
        seen |= present
        prior_present = present
    return pd.DataFrame(rows)


def _side_value(row: pd.Series, over_col: str, under_col: str) -> Any:
    side = _clean_text(row.get("side")).lower()
    if side == "over":
        return row.get(over_col)
    if side == "under":
        return row.get(under_col)
    return pd.NA


def _discovery_bucket(row: pd.Series, *, latest_order: int) -> str:
    if _clean_text(row.get("operational_candidate_status")) == "excluded_game_started":
        return "post-start-excluded"
    first_order = _num(row.get("first_seen_run_order"))
    if first_order is None:
        return "unknown"
    if int(first_order) <= 1:
        return "morning"
    if int(first_order) >= latest_order:
        return "late"
    return "midday"


def _build_delta_summary(ledger: pd.DataFrame, *, date_value: str, latest_order: int) -> pd.DataFrame:
    if ledger.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "first_seen_run_tag",
                "current_run_tag",
                "prop_type",
                "side",
                "line",
                "player_name",
                "team",
                "opponent",
                "discovery_bucket",
                "is_current",
                "is_current_eligible",
                "is_late_discovered",
                "disappeared_after_first_seen",
                "reappeared",
                "latest_book_count",
                "latest_price",
                "latest_no_vig",
                "lineup_confirmed",
                "batting_order",
                "role_bucket",
            ]
        )
    rows: list[dict[str, Any]] = []
    for _, row in ledger.iterrows():
        is_current = bool(row.get("current_surface_present"))
        is_current_eligible = _clean_text(row.get("operational_candidate_status")) == "current_eligible"
        discovery_class = _clean_text(row.get("discovery_class"))
        rows.append(
            {
                "date": date_value,
                "first_seen_run_tag": _clean_text(row.get("first_seen_run_tag")),
                "current_run_tag": _clean_text(row.get("last_seen_run_tag")),
                "prop_type": _clean_text(row.get("prop_type")),
                "side": _clean_text(row.get("side")),
                "line": row.get("line"),
                "player_name": _clean_text(row.get("player_name")),
                "team": _clean_text(row.get("team")),
                "opponent": _clean_text(row.get("opponent")),
                "discovery_bucket": _discovery_bucket(row, latest_order=latest_order),
                "is_current": is_current,
                "is_current_eligible": is_current_eligible,
                "is_late_discovered": discovery_class == "late_discovered_candidate",
                "disappeared_after_first_seen": discovery_class == "disappeared_candidate",
                "reappeared": discovery_class == "reappeared_candidate",
                "latest_book_count": row.get("book_count_current_or_last"),
                "latest_price": _side_value(row, "price_over_current_or_last", "price_under_current_or_last"),
                "latest_no_vig": _side_value(row, "prob_over_current_or_last", "prob_under_current_or_last"),
                "lineup_confirmed": _clean_text(row.get("latest_lineup_status")) == "confirmed_full",
                "batting_order": row.get("latest_lineup_slot"),
                "role_bucket": _clean_text(row.get("latest_lineup_bucket")) or "unknown",
            }
        )
    return pd.DataFrame(rows)


def _delta_count(delta: pd.DataFrame, **filters: Any) -> int:
    if delta.empty:
        return 0
    mask = pd.Series(True, index=delta.index)
    for col, expected in filters.items():
        if col not in delta.columns:
            return 0
        if isinstance(expected, (set, tuple, list)):
            mask &= delta[col].isin(expected)
        else:
            mask &= delta[col].eq(expected)
    return int(mask.sum())


def _write_report(
    path: Path,
    *,
    date_value: str,
    runs: list[RunArtifact],
    ledger: pd.DataFrame,
    current: pd.DataFrame,
    growth: pd.DataFrame,
    delta: pd.DataFrame,
    lineup_overlay_rows: int,
) -> None:
    late_current = int(current["discovery_class"].eq("late_discovered_candidate").sum()) if not current.empty else 0
    disappeared = int(ledger["discovery_class"].eq("disappeared_candidate").sum()) if not ledger.empty else 0
    reappeared = int(ledger["discovery_class"].eq("reappeared_candidate").sum()) if not ledger.empty else 0
    hits15_current = int(
        current[current["prop_type"].eq("hits") & pd.to_numeric(current["line"], errors="coerce").eq(1.5)].shape[0]
    ) if not current.empty else 0
    morning_candidates = _delta_count(delta, discovery_bucket="morning")
    late_discovered = int(delta["is_late_discovered"].sum()) if not delta.empty else 0
    current_eligible_late = _delta_count(delta, is_current_eligible=True, is_late_discovered=True)
    disappeared = _delta_count(delta, disappeared_after_first_seen=True)
    reappeared = _delta_count(delta, reappeared=True)
    hits15_mask = (
        delta["prop_type"].eq("hits") & pd.to_numeric(delta["line"], errors="coerce").eq(1.5)
        if not delta.empty
        else pd.Series(dtype=bool)
    )
    hits15_morning = int((hits15_mask & delta["discovery_bucket"].eq("morning")).sum()) if not delta.empty else 0
    hits15_late = int((hits15_mask & delta["is_late_discovered"].astype(bool)).sum()) if not delta.empty else 0
    hits15_current_eligible = int((hits15_mask & delta["is_current_eligible"].astype(bool)).sum()) if not delta.empty else 0
    lines = [
        f"# Rolling Market-Late Candidate Observation — {date_value}",
        "",
        "## Scope",
        "",
        "Dry-run/read-only observation artifacts generated from existing local run-tagged files. No OddsAPI calls, DB writes, production upload changes, model formula changes, or immutable artifact rewrites were performed.",
        "",
        "## Run Inputs",
        "",
    ]
    for run in runs:
        lines.append(f"- `{run.run_tag}`: `{run.path}`")
    lines.extend(
        [
            "",
            "## Current Observation Summary",
            "",
            f"- Runs inspected: `{len(runs)}`",
            f"- Ledger candidate rows: `{len(ledger)}`",
            f"- Current projection rows: `{len(current)}`",
            f"- Current Hits 1.5 rows: `{hits15_current}`",
            f"- Current late-discovered rows: `{late_current}`",
            f"- Historical disappeared rows: `{disappeared}`",
            f"- Reappeared rows: `{reappeared}`",
            f"- Lineup overlay rows available: `{lineup_overlay_rows}`",
            "",
            "## Morning vs Rolling Delta Summary",
            "",
            f"- Morning candidates: `{morning_candidates}`",
            f"- Late-discovered candidates: `{late_discovered}`",
            f"- Current eligible late-discovered candidates: `{current_eligible_late}`",
            f"- Disappeared candidates: `{disappeared}`",
            f"- Reappeared candidates: `{reappeared}`",
            f"- Hits 1.5 morning count: `{hits15_morning}`",
            f"- Hits 1.5 late-discovered count: `{hits15_late}`",
            f"- Hits 1.5 current eligible count: `{hits15_current_eligible}`",
            "",
            "## Operational Interpretation",
            "",
            "The ledger is the historical memory. The current projection is the live research/ops truth for the observation window: it includes only rows present in the latest market-backed slate output. Disappeared rows remain retrievable in the ledger but are excluded from current candidate surfaces.",
            "",
            "External upload behavior remains unchanged. Any future upload-ready late candidate lane should start as shadow-only and require current market availability, pregame status, clean identity, and fresh line/price checks.",
            "",
            "## Growth By Refresh",
            "",
        ]
    )
    if growth.empty:
        lines.append("- No growth rows available.")
    else:
        lines.append("| run | candidates | new | carried | removed | reappeared | hits 1.5 |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for _, row in growth.iterrows():
            lines.append(
                f"| `{row['run_tag']}` | `{row['candidates_present']}` | `{row['new_candidates']}` | "
                f"`{row['carried_forward_candidates']}` | `{row['removed_since_previous_run']}` | "
                f"`{row['reappeared_candidates']}` | `{row['hits_15_candidates_present']}` |"
            )
    lines.extend(
        [
            "",
            "## Ops Brief Patch Plan",
            "",
            "- Add an informational section sourced from `rolling_candidate_ops_brief_input_<DATE>.json`.",
            "- Show current projection count, late-discovered current count, disappeared historical count, reappeared count, and links to ledger/current/pivot CSVs.",
            "- Keep upload status separate and clearly labeled unchanged/shadow-only.",
            "- Do not block Morning Gate on this section during the observation week.",
            "",
            "## No Behavior Changed",
            "",
            "This pass produced observation artifacts only.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def build(date_value: str, output_dir: Path, odds_root: Path, lane_root: Path, lineup_root: Path) -> dict[str, Any]:
    runs = _discover_slate_runs(odds_root, date_value)
    if not runs:
        raise FileNotFoundError(f"no run-tagged slate outputs found under {odds_root / date_value}")

    frames = [_read_slate_run(run) for run in runs]
    all_rows = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    latest_order = max(run.run_order for run in runs)
    baseline_order = min(run.run_order for run in runs)
    ledger_rows = [
        _summarize_key(group, latest_order=latest_order, baseline_order=baseline_order)
        for _, group in all_rows.groupby("candidate_key", sort=False)
    ]
    ledger = pd.DataFrame(ledger_rows)

    lineup = _load_lineup_overlay(lineup_root, date_value)
    if not lineup.empty and not ledger.empty:
        ledger["game_id_key"] = ledger["game_id"].map(_id_text)
        ledger["player_id_key"] = ledger["player_id"].map(_id_text)
        ledger = ledger.merge(lineup, on=["game_id_key", "player_id_key"], how="left")
        ledger = ledger.drop(columns=["game_id_key", "player_id_key"])
    else:
        for col in [
            "latest_lineup_slot",
            "latest_lineup_bucket",
            "latest_lineup_status",
            "lineup_slot_semantics",
            "lineup_source_fetched_at_utc",
            "lineup_source_artifact",
        ]:
            ledger[col] = pd.NA

    current = ledger[ledger["current_surface_present"].astype(bool)].copy()
    growth = _build_growth_summary(ledger, runs)
    delta = _build_delta_summary(ledger, date_value=date_value, latest_order=latest_order)
    pivot = current.copy()
    if not pivot.empty:
        pivot["is_hits_o15"] = pivot["prop_type"].eq("hits") & pd.to_numeric(pivot["line"], errors="coerce").eq(1.5)
        pivot["is_late_discovered"] = pivot["discovery_class"].eq("late_discovered_candidate")
        pivot["has_confirmed_lineup_overlay"] = pivot["latest_lineup_status"].astype(str).eq("confirmed_full")

    output_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = output_dir / f"rolling_candidate_ledger_{date_value}.csv"
    current_path = output_dir / f"rolling_candidate_current_projection_{date_value}.csv"
    growth_path = output_dir / f"rolling_candidate_growth_summary_{date_value}.csv"
    delta_path = output_dir / f"rolling_candidate_delta_summary_{date_value}.csv"
    pivot_path = output_dir / f"rolling_candidate_pivot_source_{date_value}.csv"
    ops_json_path = output_dir / f"rolling_candidate_ops_brief_input_{date_value}.json"
    report_path = output_dir / f"rolling_market_late_candidate_observation_{date_value}.md"

    ledger.to_csv(ledger_path, index=False)
    current.to_csv(current_path, index=False)
    growth.to_csv(growth_path, index=False)
    delta.to_csv(delta_path, index=False)
    pivot.to_csv(pivot_path, index=False)

    current_eligible_rows = 0
    confirmed_lineup_overlay_count = 0
    if not current.empty:
        if "operational_candidate_status" in current.columns:
            current_eligible_rows = int(current["operational_candidate_status"].eq("current_eligible").sum())
        if "latest_lineup_status" in current.columns:
            confirmed_lineup_overlay_count = int(current["latest_lineup_status"].astype(str).eq("confirmed_full").sum())

    ops_payload = {
        "date": date_value,
        "mode": "dry_run_read_only",
        "runs_inspected": len(runs),
        "ledger_rows": int(len(ledger)),
        "current_projection_rows": int(len(current)),
        "current_eligible_rows": current_eligible_rows,
        "current_late_discovered_rows": int(current["discovery_class"].eq("late_discovered_candidate").sum()) if not current.empty else 0,
        "historical_disappeared_rows": int(ledger["discovery_class"].eq("disappeared_candidate").sum()) if not ledger.empty else 0,
        "reappeared_rows": int(ledger["discovery_class"].eq("reappeared_candidate").sum()) if not ledger.empty else 0,
        "current_hits_15_rows": int(current[current["prop_type"].eq("hits") & pd.to_numeric(current["line"], errors="coerce").eq(1.5)].shape[0]) if not current.empty else 0,
        "confirmed_lineup_overlay_count": confirmed_lineup_overlay_count,
        "morning_candidates": _delta_count(delta, discovery_bucket="morning"),
        "late_discovered_candidates": int(delta["is_late_discovered"].sum()) if not delta.empty else 0,
        "current_eligible_late_discovered_candidates": _delta_count(
            delta,
            is_current_eligible=True,
            is_late_discovered=True,
        ),
        "disappeared_candidates": _delta_count(delta, disappeared_after_first_seen=True),
        "hits_15_morning_count": _delta_count(delta, discovery_bucket="morning", prop_type="hits", line=1.5),
        "hits_15_late_discovered_count": _delta_count(delta, is_late_discovered=True, prop_type="hits", line=1.5),
        "hits_15_current_eligible_count": _delta_count(delta, is_current_eligible=True, prop_type="hits", line=1.5),
        "latest_run_tag": runs[-1].run_tag,
        "ledger_csv": str(ledger_path),
        "current_projection_csv": str(current_path),
        "growth_summary_csv": str(growth_path),
        "delta_summary_csv": str(delta_path),
        "pivot_source_csv": str(pivot_path),
        "rolling_observation_md": str(report_path),
        "upload_behavior": "unchanged; no production upload behavior modified",
    }
    import json

    ops_json_path.write_text(json.dumps(ops_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_report(
        report_path,
        date_value=date_value,
        runs=runs,
        ledger=ledger,
        current=current,
        growth=growth,
        delta=delta,
        lineup_overlay_rows=len(lineup),
    )
    return {
        "ledger_csv": str(ledger_path),
        "current_projection_csv": str(current_path),
        "growth_summary_csv": str(growth_path),
        "delta_summary_csv": str(delta_path),
        "pivot_source_csv": str(pivot_path),
        "ops_brief_input_json": str(ops_json_path),
        "report_md": str(report_path),
        **ops_payload,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build dry-run rolling market-late candidate observation artifacts.")
    parser.add_argument("--date", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--odds-root", default=str(DEFAULT_ODDS_ROOT))
    parser.add_argument("--lane-root", default=str(DEFAULT_LANE_ROOT))
    parser.add_argument("--lineup-root", default=str(DEFAULT_LINEUP_ROOT))
    args = parser.parse_args()

    result = build(
        date_value=str(args.date),
        output_dir=Path(args.output_dir),
        odds_root=Path(args.odds_root),
        lane_root=Path(args.lane_root),
        lineup_root=Path(args.lineup_root),
    )
    for key, value in result.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
