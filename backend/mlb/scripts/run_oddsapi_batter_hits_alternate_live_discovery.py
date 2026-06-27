#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from backend.app.services.mlb.market_odds_service import EVENTS_BASE, _bookmakers_query_csv
from backend.mlb.scripts.run_mlb_hits_o15_review_board import _load_starter_context


ET = ZoneInfo("America/New_York")
MARKETS = ("batter_hits", "batter_hits_alternate")


def _date_et_today() -> str:
    return datetime.now(ET).date().isoformat()


def _snapshot_ts() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _safe_ts_for_file(ts: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "", ts)[:15] or "snapshot"


def _norm_name(value: Any) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^0-9A-Za-z ]+", " ", text).lower()
    return re.sub(r"\s+", " ", text).strip()


def _event_date_et(event: dict[str, Any]) -> str | None:
    raw = event.get("commence_time")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return dt.astimezone(ET).date().isoformat()
    except Exception:
        return None


def _fetch_events(api_key: str, game_date: str) -> list[dict[str, Any]]:
    try:
        res = requests.get(
            EVENTS_BASE,
            params={"apiKey": api_key, "dateFormat": "iso"},
            timeout=25,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"OddsAPI events request failed: {type(exc).__name__}") from None
    res.raise_for_status()
    payload = res.json()
    if not isinstance(payload, list):
        raise RuntimeError("unexpected OddsAPI events payload shape")
    return [ev for ev in payload if isinstance(ev, dict) and _event_date_et(ev) == game_date]


def _fetch_event_market(
    *,
    api_key: str,
    event_id: str,
    market_key: str,
    regions: str,
    bookmakers: str,
) -> dict[str, Any] | None:
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": market_key,
        "oddsFormat": "american",
        "dateFormat": "iso",
        "includeBetLimits": "true",
    }
    if bookmakers:
        params["bookmakers"] = bookmakers
    try:
        res = requests.get(f"{EVENTS_BASE}/{event_id}/odds", params=params, timeout=25)
    except requests.RequestException as exc:
        raise RuntimeError(
            f"OddsAPI event odds request failed market={market_key} event_id={event_id}: {type(exc).__name__}"
        ) from None
    if res.status_code == 422:
        return None
    res.raise_for_status()
    payload = res.json()
    return payload if isinstance(payload, dict) else None


def _load_player_map(date_str: str) -> dict[tuple[str, str], dict[str, Any]]:
    paths = [
        Path(f"backend/mlb/exports/odds_history/{date_str}/mlb_slate_output.csv"),
        Path(f"backend/mlb/exports/odds_history/{date_str}/mlb_predictions_wide_calibrated.csv"),
    ]
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for path in paths:
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if "player_name" not in df.columns:
            continue
        for _, row in df.iterrows():
            name = _norm_name(row.get("player_name"))
            team = str(row.get("team") or "").strip().upper()
            if not name:
                continue
            payload = {
                "player_id": row.get("player_id") if "player_id" in df.columns else None,
                "player_name": row.get("player_name"),
                "team": team or None,
                "opponent": row.get("opponent") if "opponent" in df.columns else None,
                "d7_hits_rate": row.get("d7_hits") if "d7_hits" in df.columns else None,
                "d15_hits_rate": row.get("d15_hits") if "d15_hits" in df.columns else None,
                "game_time": row.get("game_time") if "game_time" in df.columns else None,
            }
            if team:
                out.setdefault((name, team), payload)
            out.setdefault((name, ""), payload)
    return out


def _load_board(date_str: str) -> pd.DataFrame:
    path = Path(f"artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_{date_str}.csv")
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if not df.empty:
        df["normalized_player_name"] = df.get("player_name", df.get("player", "")).map(_norm_name)
    return df


def _team_for_player(player_norm: str, home_team: str, away_team: str, player_map: dict[tuple[str, str], dict[str, Any]]) -> tuple[str | None, dict[str, Any]]:
    home_abbr = str(home_team or "").strip().upper()
    away_abbr = str(away_team or "").strip().upper()
    # Local slate rows use abbreviations; OddsAPI event teams are full names, so the
    # generic name-only mapping is often the most reliable for discovery reporting.
    generic = player_map.get((player_norm, "")) or {}
    team = str(generic.get("team") or "").strip().upper() or None
    return team, generic


def _extract_book_rows(
    *,
    payloads: dict[str, list[dict[str, Any]]],
    snapshot_time: str,
    player_map: dict[tuple[str, str], dict[str, Any]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for market_key, events in payloads.items():
        for ev in events:
            event_id = str(ev.get("id") or "").strip()
            home = str(ev.get("home_team") or "").strip()
            away = str(ev.get("away_team") or "").strip()
            game = f"{away} @ {home}" if home or away else ""
            commence = ev.get("commence_time")
            for book in ev.get("bookmakers") or []:
                book_key = str(book.get("key") or "").strip()
                for market in book.get("markets") or []:
                    if str(market.get("key") or "").strip() != market_key:
                        continue
                    for outcome in market.get("outcomes") or []:
                        side = str(outcome.get("name") or "").strip()
                        if side.lower() not in {"over", "under"}:
                            continue
                        player = str(outcome.get("description") or "").strip()
                        if not player:
                            continue
                        player_norm = _norm_name(player)
                        team, mapped = _team_for_player(player_norm, home, away, player_map)
                        rows.append(
                            {
                                "event_id": event_id,
                                "game": game,
                                "home_team": home,
                                "away_team": away,
                                "commence_time": commence,
                                "bookmaker_key": book_key,
                                "bookmaker_title": book.get("title"),
                                "market_key": market_key,
                                "player_name": player,
                                "normalized_player_name": player_norm,
                                "player_id": mapped.get("player_id"),
                                "team": team,
                                "opponent": mapped.get("opponent"),
                                "side": side.lower(),
                                "line": outcome.get("point"),
                                "price": outcome.get("price"),
                                "snapshot_timestamp": snapshot_time,
                            }
                        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["line"] = pd.to_numeric(df["line"], errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    return df


def _availability(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "market_key",
                "bookmaker_key",
                "line",
                "player_count",
                "over_rows",
                "under_rows",
                "two_sided_player_line_pairs",
                "one_sided_over_only_player_line_pairs",
                "one_sided_under_only_player_line_pairs",
            ]
        )
    records: list[dict[str, Any]] = []
    for (market_key, bookmaker_key, line), g in df.groupby(["market_key", "bookmaker_key", "line"], dropna=False):
        side_sets = g.groupby(["event_id", "normalized_player_name", "line"], dropna=False)["side"].apply(lambda s: set(str(x).lower() for x in s))
        records.append(
            {
                "market_key": market_key,
                "bookmaker_key": bookmaker_key,
                "line": line,
                "player_count": int(g["normalized_player_name"].nunique()),
                "over_rows": int((g["side"] == "over").sum()),
                "under_rows": int((g["side"] == "under").sum()),
                "two_sided_player_line_pairs": int(sum({"over", "under"}.issubset(sides) for sides in side_sets)),
                "one_sided_over_only_player_line_pairs": int(sum(sides == {"over"} for sides in side_sets)),
                "one_sided_under_only_player_line_pairs": int(sum(sides == {"under"} for sides in side_sets)),
            }
        )
    # Add all-book aggregate rows where over/under can be from different books.
    for (market_key, line), g in df.groupby(["market_key", "line"], dropna=False):
        side_sets = g.groupby(["event_id", "normalized_player_name", "line"], dropna=False)["side"].apply(lambda s: set(str(x).lower() for x in s))
        records.append(
            {
                "market_key": market_key,
                "bookmaker_key": "__all_books__",
                "line": line,
                "player_count": int(g["normalized_player_name"].nunique()),
                "over_rows": int((g["side"] == "over").sum()),
                "under_rows": int((g["side"] == "under").sum()),
                "two_sided_player_line_pairs": int(sum({"over", "under"}.issubset(sides) for sides in side_sets)),
                "one_sided_over_only_player_line_pairs": int(sum(sides == {"over"} for sides in side_sets)),
                "one_sided_under_only_player_line_pairs": int(sum(sides == {"under"} for sides in side_sets)),
            }
        )
    return pd.DataFrame(records).sort_values(["market_key", "bookmaker_key", "line"]).reset_index(drop=True)


def _markdown_table(df: pd.DataFrame, columns: list[str] | None = None, *, max_rows: int | None = None) -> str:
    if df.empty:
        return "_No rows._"
    out = df.copy()
    if columns:
        out = out[[c for c in columns if c in out.columns]]
    if max_rows is not None:
        out = out.head(int(max_rows))
    headers = [str(c) for c in out.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in out.iterrows():
        vals = []
        for col in out.columns:
            val = row.get(col)
            if pd.isna(val):
                vals.append("")
            else:
                vals.append(str(val).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def _line15_pairs(df: pd.DataFrame, market_key: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    sub = df[(df["market_key"] == market_key) & (df["line"].round(3) == 1.5)].copy()
    if sub.empty:
        return sub
    grouped = (
        sub.groupby(["event_id", "normalized_player_name", "line"], dropna=False)
        .agg(
            player_name=("player_name", "first"),
            player_id=("player_id", "first"),
            team=("team", "first"),
            opponent=("opponent", "first"),
            game=("game", "first"),
            over_books=("bookmaker_key", lambda s: ",".join(sorted(set(str(b) for b in sub.loc[s.index][sub.loc[s.index, "side"] == "over"]["bookmaker_key"])))),
            under_books=("bookmaker_key", lambda s: ",".join(sorted(set(str(b) for b in sub.loc[s.index][sub.loc[s.index, "side"] == "under"]["bookmaker_key"])))),
            books=("bookmaker_key", lambda s: ",".join(sorted(set(str(b) for b in s)))),
            over_price_best=("price", lambda s: pd.to_numeric(sub.loc[s.index][sub.loc[s.index, "side"] == "over"]["price"], errors="coerce").max()),
            under_price_best=("price", lambda s: pd.to_numeric(sub.loc[s.index][sub.loc[s.index, "side"] == "under"]["price"], errors="coerce").max()),
        )
        .reset_index()
    )
    grouped["has_over"] = grouped["over_books"].astype(str).str.len() > 0
    grouped["has_under"] = grouped["under_books"].astype(str).str.len() > 0
    grouped["two_sided_any_book"] = grouped["has_over"] & grouped["has_under"]
    grouped["betonline_present"] = grouped["books"].astype(str).str.contains("betonlineag", case=False, na=False)
    grouped["espn_present"] = grouped["books"].astype(str).str.contains("espn", case=False, na=False)
    return grouped


def _added_candidates(
    *,
    book_rows: pd.DataFrame,
    board: pd.DataFrame,
    player_map: dict[tuple[str, str], dict[str, Any]],
    starter_context: dict[tuple[str, str], dict[str, Any]],
    date_str: str,
) -> pd.DataFrame:
    main = _line15_pairs(book_rows, "batter_hits")
    alt = _line15_pairs(book_rows, "batter_hits_alternate")
    if alt.empty:
        return pd.DataFrame()

    main_keys = set(zip(main.get("event_id", []), main.get("normalized_player_name", []), main.get("line", []))) if not main.empty else set()
    board_names = set(board.get("normalized_player_name", pd.Series(dtype=str)).dropna().astype(str)) if not board.empty else set()
    records: list[dict[str, Any]] = []
    board_by_name = {}
    if not board.empty:
        for _, row in board.iterrows():
            board_by_name.setdefault(str(row.get("normalized_player_name") or ""), row.to_dict())

    for _, row in alt.iterrows():
        key = (row.get("event_id"), row.get("normalized_player_name"), row.get("line"))
        name_key = str(row.get("normalized_player_name") or "")
        board_row = board_by_name.get(name_key, {})
        player_ctx = player_map.get((name_key, "")) or {}
        in_main = key in main_keys
        in_board = name_key in board_names
        d7_source = board_row.get("d7_hits_rate") if board_row else player_ctx.get("d7_hits_rate")
        d15_source = board_row.get("d15_hits_rate") if board_row else player_ctx.get("d15_hits_rate")
        team = str(row.get("team") or board_row.get("team") or player_ctx.get("team") or "").strip().upper()
        opponent = str(row.get("opponent") or board_row.get("opponent") or player_ctx.get("opponent") or "").strip().upper()
        starter_row = starter_context.get((team, opponent), {}) if team and opponent else {}
        starter_source = (
            board_row.get("starter_expected_hits_allowed")
            if board_row and board_row.get("starter_expected_hits_allowed") not in {None, ""}
            else starter_row.get("starter_expected_hits_allowed")
        )
        d7 = pd.to_numeric(pd.Series([d7_source]), errors="coerce").iloc[0]
        d15 = pd.to_numeric(pd.Series([d15_source]), errors="coerce").iloc[0]
        starter = pd.to_numeric(pd.Series([starter_source]), errors="coerce").iloc[0]
        records.append(
            {
                "date": date_str,
                "player_name": row.get("player_name"),
                "normalized_player_name": name_key,
                "player_id": row.get("player_id") or player_ctx.get("player_id"),
                "team": team,
                "opponent": opponent,
                "game": row.get("game"),
                "line": row.get("line"),
                "alternate_only_vs_main": not in_main,
                "in_current_layered_board": in_board,
                "two_sided_any_book": row.get("two_sided_any_book"),
                "betonline_present": row.get("betonline_present"),
                "espn_present": row.get("espn_present"),
                "books": row.get("books"),
                "over_books": row.get("over_books"),
                "under_books": row.get("under_books"),
                "best_over_price": row.get("over_price_best"),
                "d7_hits_rate": d7,
                "d15_hits_rate": d15,
                "starter_expected_hits_allowed": starter,
                "starter_context_status": board_row.get("starter_context_status") or starter_row.get("starter_context_status"),
                "opposing_starter": board_row.get("opposing_starter") or starter_row.get("opposing_starter"),
                "d7_hot_candidate": bool(d7 is not None and pd.notna(d7) and float(d7) > 1.0),
                "d15_consistent_candidate": bool(d15 is not None and pd.notna(d15) and float(d15) > 1.0),
                "favorable_starter_candidate": bool(starter is not None and pd.notna(starter) and float(starter) >= 5.0),
                "would_be_layer_4_if_qc_joined": bool(
                    d7 is not None
                    and d15 is not None
                    and starter is not None
                    and pd.notna(d7)
                    and pd.notna(d15)
                    and pd.notna(starter)
                    and float(d7) > 1.0
                    and float(d15) > 1.0
                    and float(starter) >= 5.0
                ),
            }
        )
    out = pd.DataFrame(records)
    if not out.empty:
        out = out.sort_values(
            ["alternate_only_vs_main", "would_be_layer_4_if_qc_joined", "d7_hits_rate", "starter_expected_hits_allowed"],
            ascending=[False, False, False, False],
        )
    return out


def _write_two_sided_audit(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "# Two-Sided Filtering Audit",
                "",
                "## Current Production/Slate Path",
                "",
                "- `backend/mlb/scripts/build_mlb_predictions_wide.py` builds book-side groups by `(event_id, prop_type, normalized player, home, away, line)`.",
                "- With `--require-two-sided`, if `--two-sided-bookmaker` is blank, it requires at least one captured bookmaker to have both Over and Under for that same player/line.",
                "- The Makefile default is `MLB_PREDICT_TWO_SIDED_BOOKMAKER ?=` blank, so the normal prediction-wide/slate row universe is not BetOnline-only by default.",
                "- If a target bookmaker is explicitly supplied, the same code requires that target book to have both sides, except optional fallback props configured in `MLB_PREDICT_TWO_SIDED_OPTIONAL_TARGET_BOOK_PROPS`.",
                "- Selected market prices in `build_mlb_predictions_wide.py` come from the target book when configured; otherwise they come from the first sorted bookmaker with a two-sided pair.",
                "",
                "## Reconcile Path",
                "",
                "- `backend/mlb/scripts/build_mlb_reconcile_rows.py` indexes all captured books and counts `book_count_two_sided` across all books with same player/line.",
                "- Its CLI default bookmaker is `betonlineag`; `_choose_book()` selects BetOnline prices when present.",
                "- When `--require-two-sided` is active and `--include-single-book` is not active, reconcile keeps only rows with valid Over and Under prices and `book_count_two_sided >= 2`.",
                "",
                "## Answer",
                "",
                "For current slate generation, hits 1.5 two-sided filtering is evaluated across the captured OddsAPI books when no target bookmaker is configured. It is not inherently BetOnline-only. However, some reconcile/reporting paths select BetOnline as the canonical price surface while still using all-book coverage counts, so a BetOnline-centric view can appear downstream depending on the command.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=_date_et_today())
    ap.add_argument("--out-root", default="artifacts/analysis/mlb/review_aids/oddsapi_batter_hits_alternate_live_discovery")
    ap.add_argument(
        "--from-existing-dir",
        default="",
        help="Render reports from previously saved raw discovery files without making live OddsAPI calls.",
    )
    args = ap.parse_args()

    game_date = str(args.date)
    out_dir = Path(args.out_root) / game_date
    out_dir.mkdir(parents=True, exist_ok=True)
    snapshot_time = _snapshot_ts()
    file_ts = _safe_ts_for_file(snapshot_time)

    regions = str(os.getenv("MLB_ODDS_REGIONS", "us") or "us")
    bookmakers = _bookmakers_query_csv()

    payloads: dict[str, list[dict[str, Any]]] = {m: [] for m in MARKETS}
    errors: list[dict[str, Any]] = []
    from_existing = Path(str(args.from_existing_dir)).expanduser() if str(args.from_existing_dir or "").strip() else None
    if from_existing:
        events_files = sorted(from_existing.glob("events_live_*.json"))
        if not events_files:
            raise SystemExit(f"No events_live_*.json files found under {from_existing}")
        events = json.loads(events_files[-1].read_text(encoding="utf-8"))
        for market_key in MARKETS:
            files = sorted(from_existing.glob(f"{market_key}_live_raw_*.json"))
            if not files:
                payloads[market_key] = []
                continue
            payloads[market_key] = json.loads(files[-1].read_text(encoding="utf-8"))
    else:
        api_key = str(os.getenv("ODDS_API_KEY", "") or "").strip()
        if not api_key:
            raise SystemExit("ODDS_API_KEY missing")
        events = _fetch_events(api_key, game_date)
        (out_dir / f"events_live_{file_ts}.json").write_text(json.dumps(events, indent=2), encoding="utf-8")
        for market_key in MARKETS:
            for ev in events:
                event_id = str(ev.get("id") or "").strip()
                if not event_id:
                    continue
                try:
                    payload = _fetch_event_market(
                        api_key=api_key,
                        event_id=event_id,
                        market_key=market_key,
                        regions=regions,
                        bookmakers=bookmakers,
                    )
                    if payload:
                        payloads[market_key].append(payload)
                except Exception as exc:
                    errors.append({"market_key": market_key, "event_id": event_id, "error": repr(exc)})

            (out_dir / f"{market_key}_live_raw_{file_ts}.json").write_text(
                json.dumps(payloads[market_key], indent=2),
                encoding="utf-8",
            )

    player_map = _load_player_map(game_date)
    starter_context, _unavailable_starter_context, starter_meta = _load_starter_context(
        Path("artifacts/analysis/mlb/mlb_hits_environment_latest.json"),
        game_date,
        history_path=Path("artifacts/analysis/mlb/mlb_hits_environment_history.jsonl"),
        snapshot_dir=Path("artifacts/analysis/mlb/hits_environment_snapshots"),
        policy="fullest_valid_projected_starter_artifact",
        required_min_starts=5,
    )
    board = _load_board(game_date)
    book_rows = _extract_book_rows(payloads=payloads, snapshot_time=snapshot_time, player_map=player_map)
    book_rows.to_csv(out_dir / "live_alternate_book_level_rows.csv", index=False)

    availability = _availability(book_rows)
    availability.to_csv(out_dir / "live_alternate_book_line_availability.csv", index=False)

    added = _added_candidates(
        book_rows=book_rows,
        board=board,
        player_map=player_map,
        starter_context=starter_context,
        date_str=game_date,
    )
    added.to_csv(out_dir / "live_alternate_added_o15_candidates.csv", index=False)

    _write_two_sided_audit(out_dir / "two_sided_filtering_audit.md")

    line_summary: list[dict[str, Any]] = []
    for market_key in MARKETS:
        sub = book_rows[book_rows["market_key"] == market_key] if not book_rows.empty else pd.DataFrame()
        if sub.empty:
            continue
        for line, g in sub.groupby("line", dropna=False):
            side_sets = g.groupby(["event_id", "normalized_player_name", "line"])["side"].apply(lambda s: set(str(x) for x in s))
            line_summary.append(
                {
                    "market_key": market_key,
                    "line": line,
                    "players": int(g["normalized_player_name"].nunique()),
                    "over_rows": int((g["side"] == "over").sum()),
                    "under_rows": int((g["side"] == "under").sum()),
                    "two_sided_any_book_pairs": int(sum({"over", "under"}.issubset(sides) for sides in side_sets)),
                    "over_only_any_book_pairs": int(sum(sides == {"over"} for sides in side_sets)),
                    "under_only_any_book_pairs": int(sum(sides == {"under"} for sides in side_sets)),
                }
            )

    board_count = int(len(board)) if not board.empty else 0
    main15 = _line15_pairs(book_rows, "batter_hits")
    alt15 = _line15_pairs(book_rows, "batter_hits_alternate")
    alt_only = int(added["alternate_only_vs_main"].sum()) if not added.empty and "alternate_only_vs_main" in added.columns else 0
    if not added.empty:
        alt_only_mask = added["alternate_only_vs_main"].fillna(False).astype(bool)
        d7_mask = added["d7_hot_candidate"].fillna(False).astype(bool)
        d15_mask = added["d15_consistent_candidate"].fillna(False).astype(bool)
        starter_mask = added["favorable_starter_candidate"].fillna(False).astype(bool)
        alt_only_d7 = int((alt_only_mask & d7_mask).sum())
        alt_only_d7_d15 = int((alt_only_mask & d7_mask & d15_mask).sum())
        alt_only_d7_d15_starter = int((alt_only_mask & d7_mask & d15_mask & starter_mask).sum())
    else:
        alt_only_d7 = 0
        alt_only_d7_d15 = 0
        alt_only_d7_d15_starter = 0
    alt15_bol = int(alt15["betonline_present"].fillna(False).astype(bool).sum()) if not alt15.empty else 0
    alt15_espn = int(alt15["espn_present"].fillna(False).astype(bool).sum()) if not alt15.empty else 0
    casey_rows = book_rows[book_rows["normalized_player_name"].eq(_norm_name("Casey Schmitt"))] if not book_rows.empty else pd.DataFrame()
    book_counter = Counter(book_rows.get("bookmaker_key", pd.Series(dtype=str)).dropna().astype(str)) if not book_rows.empty else Counter()

    report = [
        "# Live OddsAPI Batter Hits Alternate Discovery",
        "",
        f"- Date: `{game_date}`",
        f"- Snapshot timestamp: `{snapshot_time}`",
        f"- Regions: `{regions}`",
        f"- Bookmakers requested: `{bookmakers or 'all OddsAPI region books'}`",
            f"- Events fetched for date: `{len(events)}`",
            f"- API errors: `{len(errors)}`",
            f"- Starter context selected artifact: `{starter_meta.get('selected_artifact_path', '')}`",
            f"- Starter context selected coverage: `{starter_meta.get('selected_artifact_coverage', '')}`",
            "",
        "## Line Summary",
        "",
    ]
    if line_summary:
        line_df = pd.DataFrame(line_summary).sort_values(["market_key", "line"])
        report.append(_markdown_table(line_df))
    else:
        report.append("_No live batter hits rows returned._")
    report.extend(
        [
            "",
            "## Hits 1.5 Comparison",
            "",
            f"- Current layered o1.5 board rows: `{board_count}`",
            f"- Live `batter_hits` line 1.5 player-line pairs: `{0 if main15.empty else len(main15)}`",
            f"- Live `batter_hits_alternate` line 1.5 player-line pairs: `{0 if alt15.empty else len(alt15)}`",
            f"- Alternate line 1.5 pairs not present in live main `batter_hits`: `{alt_only}`",
            f"- Alternate line 1.5 pairs already in current board by player name: `{0 if added.empty else int(added['in_current_layered_board'].sum())}`",
            f"- Alternate line 1.5 pairs with BetOnline present: `{alt15_bol}`",
            f"- Alternate line 1.5 pairs with ESPN-like book key present: `{alt15_espn}`",
            f"- Alternate-only line 1.5 with d7 > 1.0 from current slate context: `{alt_only_d7}`",
            f"- Alternate-only line 1.5 with d7 > 1.0 and d15 > 1.0: `{alt_only_d7_d15}`",
            f"- Alternate-only line 1.5 with d7+d15+starter >= 5.0: `{alt_only_d7_d15_starter}`",
            "",
            "## Casey Schmitt",
            "",
            f"- Casey rows in live discovery: `{len(casey_rows)}`",
        ]
    )
    if not casey_rows.empty:
        report.append(_markdown_table(casey_rows, ["market_key", "bookmaker_key", "side", "line", "price", "game"]))
    else:
        report.append("- Casey Schmitt did not appear in the live OddsAPI `batter_hits` or `batter_hits_alternate` responses captured by this configured request.")
    report.extend(
        [
            "",
            "## Books Represented",
            "",
            ", ".join(f"`{k}` ({v})" for k, v in sorted(book_counter.items())) or "_No books returned._",
            "",
            "## Recommendation",
            "",
            "- Keep `batter_hits_alternate` discovery-only until the one-sided/two-sided and book coverage behavior is reviewed over multiple live slates.",
            "- Adding alternate lines to daily captured odds for review aids only is reasonable if this discovery shows material additional line-1.5 players.",
            "- Do not merge alternate lines into production scoring/upload without a separate schema and row-universe policy, because alternate markets can be one-sided and may not share the same bookmaker coverage as the main market.",
            "- If a manually visible BetOnline/ESPN Bet line is absent here, classify it as source/feed/timing coverage rather than a local board filter loss.",
            "",
        ]
    )
    if errors:
        report.extend(["## API Errors", "", _markdown_table(pd.DataFrame(errors)), ""])
    (out_dir / "live_alternate_discovery_report.md").write_text("\n".join(report), encoding="utf-8")

    summary = {
        "date": game_date,
        "snapshot_timestamp": snapshot_time,
        "events": len(events),
        "book_rows": int(len(book_rows)),
        "board_rows": board_count,
        "main_hits_15_pairs": int(0 if main15.empty else len(main15)),
        "alternate_hits_15_pairs": int(0 if alt15.empty else len(alt15)),
        "alternate_only_hits_15_pairs": int(alt_only),
        "alternate_hits_15_betonline_present": int(alt15_bol),
        "alternate_hits_15_espn_present": int(alt15_espn),
        "alternate_only_d7_hot": int(alt_only_d7),
        "alternate_only_d7_d15": int(alt_only_d7_d15),
        "alternate_only_d7_d15_starter": int(alt_only_d7_d15_starter),
        "starter_context_selected_artifact": starter_meta.get("selected_artifact_path", ""),
        "starter_context_selected_coverage": starter_meta.get("selected_artifact_coverage", ""),
        "api_errors": errors,
        "output_dir": str(out_dir),
    }
    (out_dir / "live_alternate_discovery_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
