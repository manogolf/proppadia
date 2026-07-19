#!/usr/bin/env python3
"""Repair historical Hits U1.5 candidate-to-odds snapshot alignment.

This utility is intentionally local and read-only. It inventories preserved
candidate artifacts and odds snapshots, recovers governed candidate timestamps
where repository-backed timestamps exist, and attempts strict at-or-before
U1.5 price binding without requesting new odds.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.mlb.scripts.build_mlb_reconcile_rows import (  # noqa: E402
    _build_team_name_reverse,
    _line_key,
    _load_events,
    _norm_name,
)

AUDIT_DATE = "2026-07-17"
DEFAULT_MANIFEST = Path(
    "artifacts/analysis/model_development/mlb_existing_u15_tracking_suppression_reconciliation/"
    "2026-07-17/existing_u15_tracked_population_manifest_2026-07-17.csv"
)
DEFAULT_OUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_u15_candidate_odds_snapshot_alignment_repair/2026-07-17"
)
DEFAULT_ODDS_ROOT = Path("backend/mlb/exports/odds_history")
DEFAULT_REVIEW_ROOT = Path("artifacts/analysis/mlb/review_aids")

TS_RE = re.compile(r"(\d{8}T\d{6})Z?")
ODDS_TAG_RE = re.compile(r"odds_mlb_playerprops(?:__|_)([^.]+)\.json$")
TAGGED_ODDS_RE = re.compile(r".*(?:__|_)\d{8}T\d{6}Z?(?:_\d+)?\.json$")


@dataclass(frozen=True)
class SnapshotRef:
    path: Path
    timestamp: datetime | None
    run_tag: str
    alias_class: str
    sha256: str


def _norm_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text


def _to_int(value: object) -> int | None:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        return None
    return int(num)


def _to_float(value: object) -> float | None:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        return None
    return float(num)


def _key_int_text(value: object) -> str:
    integer = _to_int(value)
    return str(integer) if integer is not None else ""


def _key_float_text(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return ""
    if float(number).is_integer():
        return str(int(number))
    return f"{number:.6f}".rstrip("0").rstrip(".")


def _parse_timestamp(value: object) -> datetime | None:
    text = _norm_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        pass
    match = TS_RE.search(text)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _iso(value: datetime | None) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value else ""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _american_profit(price: object, won: bool) -> float:
    odds = _to_float(price)
    if odds is None:
        return 0.0
    if not won:
        return -1.0
    if odds > 0:
        return odds / 100.0
    return 100.0 / abs(odds)


def _break_even(price: object) -> float | None:
    odds = _to_float(price)
    if odds is None:
        return None
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def _candidate_key(row: pd.Series) -> str:
    date_value = _norm_text(row.get("slate_date"))
    if not date_value:
        date_value = _norm_text(row.get("date"))
    game_id = _key_int_text(row.get("game_id"))
    if not game_id:
        game_id = _key_int_text(row.get("canonical_game_id"))
    player_id = _key_int_text(row.get("player_id"))
    if not player_id:
        player_id = _key_int_text(row.get("canonical_player_id"))
    line = _key_float_text(row.get("line"))
    price = _key_float_text(row.get("market_price"))
    if not price:
        price = _key_float_text(row.get("selected_side_price"))
    return "|".join([date_value, game_id, player_id, "hits", line, "under", price])


def _load_review_lookup(review_root: Path, dates: Iterable[str]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for date_value in sorted(set(dates)):
        path = review_root / f"hits_u15_favorite_audit_{date_value}.csv"
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        if "slate_date" not in df.columns and "date" in df.columns:
            df["slate_date"] = df["date"]
        if "side" in df.columns:
            df = df[df["side"].astype(str).str.lower().eq("under")].copy()
        if "prop_type" not in df.columns:
            df["prop_type"] = "hits"
        for col in ("game_id", "canonical_game_id", "player_id", "canonical_player_id", "line", "market_price"):
            if col not in df.columns:
                df[col] = pd.NA
        for _, row in df.iterrows():
            key = _candidate_key(row)
            lookup.setdefault(
                key,
                {
                    "review_source_path": str(path),
                    "qc_source_file": _norm_text(row.get("qc_source_file")),
                    "environment_artifact_timestamp": _norm_text(row.get("environment_artifact_timestamp")),
                    "canonical_game_id": _norm_text(row.get("canonical_game_id")),
                    "canonical_player_id": _norm_text(row.get("canonical_player_id")),
                    "identity_status": _norm_text(row.get("identity_status")),
                    "identity_method": _norm_text(row.get("identity_method")),
                },
            )
    return lookup


def _recover_candidate_timestamp(row: pd.Series, review: dict[str, Any]) -> tuple[datetime | None, str, str]:
    run_tag = _norm_text(row.get("run_tag"))
    run_ts = _parse_timestamp(run_tag)
    if run_ts is not None:
        return run_ts, "exact_candidate_run_tag", run_tag

    qc_source = _norm_text(review.get("qc_source_file"))
    qc_ts = _parse_timestamp(qc_source)
    if qc_ts is not None:
        return qc_ts, "parent_quick_card_artifact_timestamp", qc_source

    # The previous reconciliation carried a selection_timestamp derived from
    # source file mtime. That is intentionally not accepted here because the
    # task requires repository-backed decision timestamps rather than mtime.
    env_ts = _parse_timestamp(review.get("environment_artifact_timestamp"))
    if env_ts is not None:
        return None, "unresolved_parent_environment_context_only", _norm_text(review.get("environment_artifact_timestamp"))

    return None, "unresolved_no_repository_backed_decision_timestamp", ""


def _snapshot_alias_class(path: Path) -> str:
    name = path.name
    if "__" in name:
        return "run_tagged_snapshot"
    if TAGGED_ODDS_RE.match(name):
        return "timestamped_snapshot"
    if name in {"odds_mlb_playerprops.json", "odds_mlb_playerprops_final.json"}:
        return "latest_or_final_alias"
    if any(name.endswith(f"_{label}.json") for label in ("earliest", "mid", "late")):
        return "daily_alias"
    return "other_alias"


def _snapshot_run_tag(path: Path) -> str:
    if "__" in path.name:
        return path.name.split("__", 1)[1].rsplit(".", 1)[0]
    match = ODDS_TAG_RE.match(path.name)
    return match.group(1) if match else ""


def _load_snapshot_refs(odds_root: Path, date_value: str) -> list[SnapshotRef]:
    day_dir = odds_root / date_value
    paths = sorted(day_dir.glob("odds_mlb_playerprops*.json"))
    refs: list[SnapshotRef] = []
    for path in paths:
        ts = None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            ts = _parse_timestamp(raw.get("captured_at_utc") if isinstance(raw, dict) else "")
        except Exception:
            ts = None
        if ts is None:
            ts = _parse_timestamp(path.name)
        refs.append(
            SnapshotRef(
                path=path,
                timestamp=ts,
                run_tag=_snapshot_run_tag(path),
                alias_class=_snapshot_alias_class(path),
                sha256=_sha256(path),
            )
        )
    refs.sort(key=lambda r: (_iso(r.timestamp), str(r.path)))
    return refs


def _load_slate_identity_lookup(odds_root: Path, date_value: str) -> dict[tuple[str, str, str, float, str], dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    for path in sorted((odds_root / date_value).glob("mlb_slate_output__*.csv")):
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        required = {"slate_date", "game_id", "player_id", "player_name", "home_team_code", "away_team_code", "prop_type", "line"}
        if not required.issubset(df.columns):
            continue
        df = df[df["slate_date"].astype(str).eq(date_value)].copy()
        if df.empty:
            continue
        df["_source_path"] = str(path)
        frames.append(df)
    if not frames:
        return {}
    all_rows = pd.concat(frames, ignore_index=True)
    all_rows["prop_type"] = all_rows["prop_type"].astype(str).str.lower()
    all_rows["line"] = pd.to_numeric(all_rows["line"], errors="coerce")
    all_rows["player_name_norm"] = all_rows["player_name"].map(_norm_name)
    all_rows = all_rows.dropna(subset=["game_id", "player_id", "line"])
    all_rows = all_rows.sort_values("_source_path", kind="stable").drop_duplicates(
        ["home_team_code", "away_team_code", "prop_type", "line", "player_name_norm"],
        keep="last",
    )
    lookup: dict[tuple[str, str, str, float, str], dict[str, Any]] = {}
    for _, row in all_rows.iterrows():
        key = (
            _norm_text(row.get("home_team_code")).upper(),
            _norm_text(row.get("away_team_code")).upper(),
            _norm_text(row.get("prop_type")).lower(),
            float(row.get("line")),
            _norm_text(row.get("player_name_norm")),
        )
        lookup[key] = {
            "slate_date": _norm_text(row.get("slate_date")),
            "game_id": int(row.get("game_id")),
            "player_id": int(row.get("player_id")),
            "player_name": _norm_text(row.get("player_name")),
            "team": _norm_text(row.get("team")),
            "opponent": _norm_text(row.get("opponent")),
            "home_team_code": _norm_text(row.get("home_team_code")).upper(),
            "away_team_code": _norm_text(row.get("away_team_code")).upper(),
            "slate_identity_source_path": _norm_text(row.get("_source_path")),
        }
    return lookup


def _flatten_snapshot_prices(
    *,
    ref: SnapshotRef,
    date_value: str,
    slate_lookup: dict[tuple[str, str, str, float, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    team_rev = _build_team_name_reverse()
    rows: list[dict[str, Any]] = []
    counts = {
        "raw_outcomes": 0,
        "matched_price_rows": 0,
        "unmatched_to_slate_identity": 0,
        "unsupported_market": 0,
        "missing_team_map": 0,
    }
    try:
        events = _load_events(ref.path)
    except Exception:
        return rows, counts
    for event in events:
        home = team_rev.get(_norm_name(event.get("home_team")))
        away = team_rev.get(_norm_name(event.get("away_team")))
        if not home or not away:
            counts["missing_team_map"] += 1
            continue
        for book in event.get("bookmakers") or []:
            book_key = _norm_text(book.get("key") or book.get("title")).lower()
            if not book_key:
                continue
            for market in book.get("markets") or []:
                market_key = _norm_text(market.get("key"))
                if market_key != "batter_hits":
                    counts["unsupported_market"] += 1
                    continue
                grouped: dict[tuple[str, float], dict[str, Any]] = {}
                for outcome in market.get("outcomes") or []:
                    side = _norm_text(outcome.get("name")).lower()
                    if side not in {"over", "under"}:
                        continue
                    player_name = _norm_text(outcome.get("description"))
                    line = _line_key(outcome.get("point"))
                    price = _to_float(outcome.get("price"))
                    if not player_name or line is None or price is None:
                        continue
                    counts["raw_outcomes"] += 1
                    rec = grouped.setdefault(
                        (_norm_name(player_name), float(line)),
                        {
                            "player_name_norm": _norm_name(player_name),
                            "player_name": player_name,
                            "line": float(line),
                            "price_over_american": None,
                            "price_under_american": None,
                        },
                    )
                    rec[f"price_{side}_american"] = price
                for rec in grouped.values():
                    key = (str(home).upper(), str(away).upper(), "hits", float(rec["line"]), rec["player_name_norm"])
                    ident = slate_lookup.get(key)
                    if not ident:
                        counts["unmatched_to_slate_identity"] += 1
                        continue
                    rows.append(
                        {
                            "slate_date": date_value,
                            "snapshot_timestamp": _iso(ref.timestamp),
                            "snapshot_run_tag": ref.run_tag,
                            "snapshot_alias_class": ref.alias_class,
                            "snapshot_source_path": str(ref.path),
                            "snapshot_source_sha256": ref.sha256,
                            "game_id": ident["game_id"],
                            "player_id": ident["player_id"],
                            "player_name": ident["player_name"] or rec["player_name"],
                            "team": ident["team"],
                            "opponent": ident["opponent"],
                            "home_team_code": ident["home_team_code"],
                            "away_team_code": ident["away_team_code"],
                            "prop_type": "hits",
                            "market_key": "batter_hits",
                            "line": float(rec["line"]),
                            "side": "under",
                            "sportsbook": book_key,
                            "price_under_american": rec["price_under_american"],
                            "price_over_american": rec["price_over_american"],
                            "slate_identity_source_path": ident["slate_identity_source_path"],
                            "primary_alignment_snapshot": ref.alias_class in {"run_tagged_snapshot", "timestamped_snapshot"},
                        }
                    )
                    counts["matched_price_rows"] += 1
    return rows, counts


def _price_inventory(odds_root: Path, dates: Iterable[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    snapshot_rows: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    parse_rows: list[dict[str, Any]] = []
    for date_value in sorted(set(dates)):
        refs = _load_snapshot_refs(odds_root, date_value)
        slate_lookup = _load_slate_identity_lookup(odds_root, date_value)
        for ref in refs:
            snapshot_rows.append(
                {
                    "slate_date": date_value,
                    "snapshot_source_path": str(ref.path),
                    "snapshot_filename": ref.path.name,
                    "snapshot_timestamp": _iso(ref.timestamp),
                    "snapshot_run_tag": ref.run_tag,
                    "snapshot_alias_class": ref.alias_class,
                    "primary_alignment_snapshot": ref.alias_class in {"run_tagged_snapshot", "timestamped_snapshot"},
                    "sha256": ref.sha256,
                }
            )
            rows, counts = _flatten_snapshot_prices(ref=ref, date_value=date_value, slate_lookup=slate_lookup)
            price_rows.extend(rows)
            parse_rows.append(
                {
                    "slate_date": date_value,
                    "snapshot_source_path": str(ref.path),
                    "snapshot_timestamp": _iso(ref.timestamp),
                    **counts,
                }
            )
    prices = pd.DataFrame(price_rows)
    if not prices.empty:
        prices = prices.dropna(subset=["price_under_american"]).copy()
        prices["price_under_american"] = pd.to_numeric(prices["price_under_american"], errors="coerce")
    return pd.DataFrame(snapshot_rows), prices, pd.DataFrame(parse_rows)


def _align_candidates(candidates: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if prices.empty:
        prices = pd.DataFrame(columns=["slate_date", "game_id", "player_id", "prop_type", "line", "side"])
    primary_prices = prices[prices.get("primary_alignment_snapshot", False).eq(True)].copy() if not prices.empty else prices
    primary_prices["snapshot_dt"] = pd.to_datetime(primary_prices.get("snapshot_timestamp"), errors="coerce", utc=True)
    for _, c in candidates.iterrows():
        date_value = _norm_text(c.get("slate_date"))
        game_id = _to_int(c.get("game_id"))
        player_id = _to_int(c.get("player_id"))
        line = _to_float(c.get("line"))
        decision_ts = _parse_timestamp(c.get("governed_candidate_timestamp"))
        candidate_price = _to_float(c.get("candidate_market_price"))
        base = c.to_dict()
        if decision_ts is None:
            status = "UNRESOLVED_CANDIDATE_TIMESTAMP"
            same = pd.DataFrame()
        elif game_id is None or player_id is None or line is None:
            status = "UNRESOLVED_EXACT_IDENTITY"
            same = pd.DataFrame()
        else:
            mask = (
                primary_prices.get("slate_date", pd.Series(dtype=str)).astype(str).eq(date_value)
                & pd.to_numeric(primary_prices.get("game_id"), errors="coerce").eq(game_id)
                & pd.to_numeric(primary_prices.get("player_id"), errors="coerce").eq(player_id)
                & primary_prices.get("prop_type", pd.Series(dtype=str)).astype(str).str.lower().eq("hits")
                & pd.to_numeric(primary_prices.get("line"), errors="coerce").eq(line)
                & primary_prices.get("side", pd.Series(dtype=str)).astype(str).str.lower().eq("under")
            )
            same = primary_prices[mask].copy()
            status = ""
        if status:
            later = pd.DataFrame()
            earlier = pd.DataFrame()
            chosen = None
        else:
            earlier = same[same["snapshot_dt"].le(decision_ts)]
            later = same[same["snapshot_dt"].gt(decision_ts)]
            if earlier.empty:
                chosen = None
                status = "LATER_ONLY_PRICE" if not later.empty else "NO_MARKET_ROW"
            else:
                latest_ts = earlier["snapshot_dt"].max()
                latest = earlier[earlier["snapshot_dt"].eq(latest_ts)].copy()
                if candidate_price is not None:
                    latest_price = latest[pd.to_numeric(latest["price_under_american"], errors="coerce").eq(candidate_price)]
                else:
                    latest_price = latest
                if latest_price.empty:
                    chosen = None
                    status = "AT_OR_BEFORE_MARKET_AVAILABLE_PRICE_MISMATCH"
                elif latest_price["sportsbook"].nunique(dropna=True) == 1:
                    chosen = latest_price.sort_values(["snapshot_source_path", "sportsbook"], kind="stable").iloc[0]
                    status = "CERTIFIED_AT_OR_BEFORE_PRICE"
                else:
                    chosen = None
                    status = "AT_OR_BEFORE_PRICE_FOUND_SPORTSBOOK_AMBIGUOUS"

        first_later = None
        if not same.empty and decision_ts is not None:
            later_all = same[same["snapshot_dt"].gt(decision_ts)].sort_values("snapshot_dt", kind="stable")
            if not later_all.empty:
                first_later = later_all.iloc[0]
        latest_any = None
        if not same.empty:
            latest_any = same.sort_values("snapshot_dt", kind="stable").iloc[-1]
        chosen_ts = _parse_timestamp(chosen.get("snapshot_timestamp")) if chosen is not None else None
        age_minutes = (decision_ts - chosen_ts).total_seconds() / 60.0 if decision_ts and chosen_ts else None
        rows.append(
            {
                **base,
                "primary_alignment_status": status,
                "primary_certified": status == "CERTIFIED_AT_OR_BEFORE_PRICE",
                "primary_sportsbook": _norm_text(chosen.get("sportsbook")) if chosen is not None else "",
                "primary_price_under_american": chosen.get("price_under_american") if chosen is not None else "",
                "primary_snapshot_timestamp": _norm_text(chosen.get("snapshot_timestamp")) if chosen is not None else "",
                "primary_snapshot_run_tag": _norm_text(chosen.get("snapshot_run_tag")) if chosen is not None else "",
                "primary_snapshot_source_path": _norm_text(chosen.get("snapshot_source_path")) if chosen is not None else "",
                "primary_snapshot_source_sha256": _norm_text(chosen.get("snapshot_source_sha256")) if chosen is not None else "",
                "snapshot_age_minutes": round(age_minutes, 3) if age_minutes is not None else "",
                "same_run_tag_price": bool(chosen is not None and _norm_text(chosen.get("snapshot_run_tag")) == _norm_text(c.get("candidate_run_tag"))),
                "at_or_before_snapshot_count": int(len(earlier)) if "earlier" in locals() else 0,
                "later_snapshot_count": int(len(later)) if "later" in locals() else 0,
                "first_later_snapshot_timestamp": _norm_text(first_later.get("snapshot_timestamp")) if first_later is not None else "",
                "first_later_price_under_american": first_later.get("price_under_american") if first_later is not None else "",
                "first_later_sportsbook": _norm_text(first_later.get("sportsbook")) if first_later is not None else "",
                "closing_or_latest_snapshot_timestamp": _norm_text(latest_any.get("snapshot_timestamp")) if latest_any is not None else "",
                "closing_or_latest_price_under_american": latest_any.get("price_under_american") if latest_any is not None else "",
                "closing_or_latest_sportsbook": _norm_text(latest_any.get("sportsbook")) if latest_any is not None else "",
                "diagnostic_same_identity_book_prices_found": int(len(same)) if not same.empty else 0,
            }
        )
    return pd.DataFrame(rows)


def _performance(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    work = df[df["primary_certified"].eq(True)].copy()
    work = work[work["result"].astype(str).str.lower().isin(["win", "loss"])].copy()
    if work.empty:
        cols = [*group_cols, "wagers", "wins", "losses", "average_price", "break_even_rate", "flat_stake_roi", "units"]
        return pd.DataFrame(columns=cols)
    work["win_bool"] = work["result"].astype(str).str.lower().eq("win")
    work["profit_1u"] = work.apply(lambda r: _american_profit(r["primary_price_under_american"], bool(r["win_bool"])), axis=1)
    work["break_even"] = work["primary_price_under_american"].map(_break_even)
    rows: list[dict[str, Any]] = []
    for key, g in work.groupby(group_cols, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        rec = {col: val for col, val in zip(group_cols, key)}
        rec.update(
            {
                "wagers": int(len(g)),
                "wins": int(g["win_bool"].sum()),
                "losses": int((~g["win_bool"]).sum()),
                "average_price": round(float(pd.to_numeric(g["primary_price_under_american"], errors="coerce").mean()), 4),
                "break_even_rate": round(float(pd.to_numeric(g["break_even"], errors="coerce").mean()), 6),
                "flat_stake_roi": round(float(g["profit_1u"].mean()), 6),
                "units": round(float(g["profit_1u"].sum()), 6),
            }
        )
        rows.append(rec)
    return pd.DataFrame(rows)


def _summary_counts(aligned: pd.DataFrame, snapshots: pd.DataFrame, prices: pd.DataFrame) -> dict[str, Any]:
    certified = aligned[aligned["primary_certified"].eq(True)]
    resolved_outcomes = aligned["outcome_resolved"].astype(str).str.lower().isin(["true", "1"]).sum()
    return {
        "tracked_u15_candidates": int(len(aligned)),
        "outcome_certified_candidates": int(resolved_outcomes),
        "candidates_with_authoritative_decision_timestamps": int(aligned["governed_candidate_timestamp"].astype(str).str.len().gt(0).sum()),
        "preserved_odds_snapshots": int(len(snapshots)),
        "primary_alignment_price_rows": int(prices.get("primary_alignment_snapshot", pd.Series(dtype=bool)).eq(True).sum()) if not prices.empty else 0,
        "exact_at_or_before_prices_recovered": int(len(certified)),
        "same_run_tag_prices": int(certified["same_run_tag_price"].sum()) if not certified.empty else 0,
        "earlier_valid_prices": int((certified["same_run_tag_price"].eq(False)).sum()) if not certified.empty else 0,
        "later_only_prices": int(aligned["primary_alignment_status"].eq("LATER_ONLY_PRICE").sum()),
        "no_market_rows": int(aligned["primary_alignment_status"].eq("NO_MARKET_ROW").sum()),
        "unresolved_candidate_timestamps": int(aligned["primary_alignment_status"].eq("UNRESOLVED_CANDIDATE_TIMESTAMP").sum()),
        "unresolved_identities": int(aligned["primary_alignment_status"].eq("UNRESOLVED_EXACT_IDENTITY").sum()),
        "sportsbooks_represented": int(certified["primary_sportsbook"].nunique(dropna=True)) if not certified.empty else 0,
    }


def build(manifest_path: Path, odds_root: Path, review_root: Path, out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = pd.read_csv(manifest_path, low_memory=False)
    manifest["slate_date"] = manifest["slate_date"].astype(str)
    dates = sorted(manifest["slate_date"].dropna().astype(str).unique())
    review_lookup = _load_review_lookup(review_root, dates)

    candidate_rows: list[dict[str, Any]] = []
    for idx, row in manifest.reset_index(drop=False).iterrows():
        review = review_lookup.get(_candidate_key(row), {})
        ts, ts_source, ts_evidence = _recover_candidate_timestamp(row, review)
        game_id = _to_int(row.get("game_id"))
        player_id = _to_int(row.get("player_id"))
        candidate_rows.append(
            {
                "candidate_row_id": int(idx),
                "canonical_u15_key": _norm_text(row.get("canonical_u15_key")),
                "slate_date": _norm_text(row.get("slate_date")),
                "game_id": game_id if game_id is not None else "",
                "player_id": player_id if player_id is not None else "",
                "player_name": _norm_text(row.get("player_name")),
                "team": _norm_text(row.get("team")),
                "opponent": _norm_text(row.get("opponent")),
                "prop_type": "hits",
                "line": 1.5,
                "side": "under",
                "candidate_market_price": row.get("market_price"),
                "candidate_authoritative_price": row.get("authoritative_price"),
                "candidate_run_tag": _norm_text(row.get("run_tag")),
                "governed_candidate_timestamp": _iso(ts),
                "candidate_timestamp_source": ts_source,
                "candidate_timestamp_evidence": ts_evidence,
                "source_artifact": _norm_text(row.get("source_artifact")),
                "review_source_path": _norm_text(review.get("review_source_path")),
                "qc_source_file": _norm_text(review.get("qc_source_file")),
                "identity_status": _norm_text(review.get("identity_status")),
                "identity_method": _norm_text(review.get("identity_method")),
                "outcome_resolved": row.get("outcome_resolved"),
                "result": _norm_text(row.get("result")).lower(),
                "outcome_source_path": _norm_text(row.get("outcome_source_path")),
                "suppression_classification": _norm_text(row.get("suppression_classification")),
                "current_side_surface_state": _norm_text(row.get("current_side_surface_state")),
            }
        )
    candidates = pd.DataFrame(candidate_rows)
    snapshots, prices, parse_summary = _price_inventory(odds_root, dates)
    aligned = _align_candidates(candidates, prices)

    status_summary = (
        aligned.groupby(["slate_date", "primary_alignment_status"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["slate_date", "primary_alignment_status"], kind="stable")
    )
    age = aligned[aligned["primary_certified"].eq(True)].copy()
    if not age.empty:
        age["snapshot_age_minutes_num"] = pd.to_numeric(age["snapshot_age_minutes"], errors="coerce")
        age["snapshot_age_bucket"] = pd.cut(
            age["snapshot_age_minutes_num"],
            bins=[-0.001, 15, 30, 60, 120, 240, 10_000],
            labels=["0-15m", "15-30m", "30-60m", "60-120m", "120-240m", "240m+"],
        ).astype(str)
        age_summary = age.groupby("snapshot_age_bucket", dropna=False).size().reset_index(name="certified_rows")
    else:
        age_summary = pd.DataFrame(columns=["snapshot_age_bucket", "certified_rows"])

    sportsbook_summary = (
        aligned[aligned["primary_certified"].eq(True)]
        .groupby("primary_sportsbook", dropna=False)
        .agg(
            certified_rows=("primary_certified", "size"),
            avg_price=("primary_price_under_american", lambda s: pd.to_numeric(s, errors="coerce").mean()),
            avg_snapshot_age_minutes=("snapshot_age_minutes", lambda s: pd.to_numeric(s, errors="coerce").mean()),
        )
        .reset_index()
        if aligned["primary_certified"].any()
        else pd.DataFrame(columns=["primary_sportsbook", "certified_rows", "avg_price", "avg_snapshot_age_minutes"])
    )
    perf_overall = _performance(aligned.assign(all_rows="all"), ["all_rows"])
    perf_by_date = _performance(aligned, ["slate_date"])
    perf_by_age = _performance(age if not age.empty else aligned, ["snapshot_age_bucket"]) if not age.empty else pd.DataFrame()

    logic_inventory = pd.DataFrame(
        [
            {
                "utility_or_contract": "backend/mlb/scripts/build_mlb_today_workspace.py",
                "matching_behavior": "raw odds JSON is flattened by snapshot timestamp, mapped to game_id/player_id through slate rows by teams, player name, prop, line, and bookmaker.",
                "reuse_in_this_repair": "policy_shape_reused_no_db_writes",
                "difference": "candidate-decision-time matching selects latest exact U1.5 UNDER row at or before candidate timestamp; workspace stages all latest descriptive rows.",
            },
            {
                "utility_or_contract": "backend/mlb/scripts/export_mlb_book_upload.py",
                "matching_behavior": "book upload can extract book-level prices from raw odds JSON through market index and policy plan book keys.",
                "reuse_in_this_repair": "book-level raw odds interpretation reused conceptually",
                "difference": "this repair does not create upload rows and does not infer a sportsbook when candidate artifacts dropped it.",
            },
            {
                "utility_or_contract": "backend/mlb/scripts/certify_mlb_hits15_suppression_price_timing_and_shadow.py",
                "matching_behavior": "frozen U1.5 timing policy requires governed decision timestamp and at-or-before snapshot; previous audit failed closed.",
                "reuse_in_this_repair": "same frozen timing policy retained",
                "difference": "this repair recovers timestamps from parent quick-card/run-tag artifacts and attempts exact snapshot alignment.",
            },
        ]
    )

    decisions = _decisions(aligned, snapshots, prices)

    outputs = {
        "candidate_timestamp_recovery": out_dir / f"u15_candidate_timestamp_recovery_{AUDIT_DATE}.csv",
        "odds_snapshot_inventory": out_dir / f"u15_odds_snapshot_inventory_{AUDIT_DATE}.csv",
        "odds_snapshot_price_inventory": out_dir / f"u15_odds_snapshot_price_inventory_{AUDIT_DATE}.csv",
        "odds_snapshot_parse_summary": out_dir / f"u15_odds_snapshot_parse_summary_{AUDIT_DATE}.csv",
        "alignment_ledger": out_dir / f"u15_candidate_to_odds_alignment_ledger_{AUDIT_DATE}.csv",
        "alignment_status_summary": out_dir / f"u15_candidate_price_alignment_status_summary_{AUDIT_DATE}.csv",
        "snapshot_age_distribution": out_dir / f"u15_snapshot_age_distribution_{AUDIT_DATE}.csv",
        "sportsbook_coverage": out_dir / f"u15_sportsbook_coverage_{AUDIT_DATE}.csv",
        "certified_performance": out_dir / f"u15_certified_at_or_before_price_performance_{AUDIT_DATE}.csv",
        "date_performance": out_dir / f"u15_date_level_certified_price_performance_{AUDIT_DATE}.csv",
        "temporal_stability": out_dir / f"u15_temporal_stability_certified_price_performance_{AUDIT_DATE}.csv",
        "snapshot_logic_inventory": out_dir / f"u15_existing_snapshot_aware_logic_inventory_{AUDIT_DATE}.csv",
        "decisions": out_dir / f"u15_candidate_odds_alignment_decisions_{AUDIT_DATE}.csv",
    }
    _write_csv(candidates, outputs["candidate_timestamp_recovery"])
    _write_csv(snapshots, outputs["odds_snapshot_inventory"])
    _write_csv(prices, outputs["odds_snapshot_price_inventory"])
    _write_csv(parse_summary, outputs["odds_snapshot_parse_summary"])
    _write_csv(aligned, outputs["alignment_ledger"])
    _write_csv(status_summary, outputs["alignment_status_summary"])
    _write_csv(age_summary, outputs["snapshot_age_distribution"])
    _write_csv(sportsbook_summary, outputs["sportsbook_coverage"])
    _write_csv(perf_overall, outputs["certified_performance"])
    _write_csv(perf_by_date, outputs["date_performance"])
    _write_csv(perf_by_age, outputs["temporal_stability"])
    _write_csv(logic_inventory, outputs["snapshot_logic_inventory"])
    _write_csv(decisions, outputs["decisions"])

    summary = _summary_counts(aligned, snapshots, prices)
    summary_path = out_dir / f"u15_candidate_odds_snapshot_alignment_repair_{AUDIT_DATE}.md"
    summary_path.write_text(_markdown(summary, aligned, decisions, outputs), encoding="utf-8")
    machine_path = out_dir / f"machine_readable_u15_candidate_odds_snapshot_alignment_repair_{AUDIT_DATE}.json"
    machine_path.write_text(json.dumps({"summary": summary, "outputs": {k: str(v) for k, v in outputs.items()}}, indent=2), encoding="utf-8")

    sha_rows = []
    for path in [*outputs.values(), summary_path, machine_path]:
        sha_rows.append({"path": str(path), "sha256": _sha256(path), "bytes": path.stat().st_size})
    sha_path = out_dir / f"sha256_manifest_{AUDIT_DATE}.csv"
    _write_csv(pd.DataFrame(sha_rows), sha_path)
    return {"summary": summary, "outputs": outputs, "summary_path": summary_path, "machine_path": machine_path, "sha_path": sha_path}


def _decisions(aligned: pd.DataFrame, snapshots: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    timestamp_count = int(aligned["governed_candidate_timestamp"].astype(str).str.len().gt(0).sum())
    certified_count = int(aligned["primary_certified"].sum())
    unresolved_ts = int(aligned["primary_alignment_status"].eq("UNRESOLVED_CANDIDATE_TIMESTAMP").sum())
    unresolved_identity = int(aligned["primary_alignment_status"].eq("UNRESOLVED_EXACT_IDENTITY").sum())
    rows = [
        {
            "decision_key": "MLB_U15_CANDIDATE_TIMESTAMP_RECOVERY_DECISION",
            "decision_value": "PARTIAL_RECOVERY_FROM_RUN_TAG_AND_PARENT_QUICK_CARD_ARTIFACTS",
            "evidence": f"{timestamp_count}/{len(aligned)} candidates have repository-backed decision timestamps; {unresolved_ts} remain unresolved.",
        },
        {
            "decision_key": "MLB_U15_ODDS_SNAPSHOT_INVENTORY_DECISION",
            "decision_value": "PRESERVED_LOCAL_SNAPSHOTS_INVENTORIED_NO_NEW_ODDS_REQUESTED",
            "evidence": f"{len(snapshots)} odds JSON files inventoried; {len(prices)} exact slate-mapped U1.5 book price rows available.",
        },
        {
            "decision_key": "MLB_U15_CANDIDATE_PRICE_ALIGNMENT_DECISION",
            "decision_value": "STRICT_AT_OR_BEFORE_ALIGNMENT_APPLIED_WITH_SPORTSBOOK_FAIL_CLOSED",
            "evidence": f"{certified_count} rows received unique sportsbook-certified at-or-before prices; {unresolved_identity} rows lack exact identity.",
        },
        {
            "decision_key": "MLB_U15_SELECTION_TIME_PRICE_COVERAGE_DECISION",
            "decision_value": "RECOVERED_PARTIAL_SELECTION_TIME_PRICE_COVERAGE",
            "evidence": f"Certified coverage is {certified_count}/{len(aligned)} tracked U1.5 rows under strict candidate timestamp and sportsbook identity rules.",
        },
        {
            "decision_key": "MLB_U15_CERTIFIED_HISTORICAL_PRICE_PERFORMANCE_DECISION",
            "decision_value": "CERTIFIED_PERFORMANCE_RECALCULATED_ONLY_FOR_AT_OR_BEFORE_ROWS",
            "evidence": "Later-only, no-market, unresolved timestamp, unresolved identity, and sportsbook-ambiguous rows are excluded from certified price ROI.",
        },
        {
            "decision_key": "MLB_U15_PRICE_ALIGNMENT_REMAINING_GAP_DECISION",
            "decision_value": "REMAINING_GAPS_ARE_TIMESTAMP_IDENTITY_AND_SPORTSBOOK_LINEAGE",
            "evidence": "Rows fail closed when candidate timestamp is missing, game/player identity is missing, or the retained selected price cannot be bound to one sportsbook.",
        },
        {
            "decision_key": "MLB_U15_PRODUCTION_STATUS",
            "decision_value": "NOT_AUTHORIZED",
            "evidence": "This package is read-only historical lineage repair; no production behavior is authorized.",
        },
    ]
    return pd.DataFrame(rows)


def _markdown(summary: dict[str, Any], aligned: pd.DataFrame, decisions: pd.DataFrame, outputs: dict[str, Path]) -> str:
    certified = aligned[aligned["primary_certified"].eq(True)].copy()
    perf = _performance(aligned.assign(all_rows="all"), ["all_rows"])
    if not perf.empty:
        p = perf.iloc[0].to_dict()
        perf_line = (
            f"{int(p['wagers'])} wagers, {int(p['wins'])}-{int(p['losses'])}, "
            f"avg price {p['average_price']}, break-even {p['break_even_rate']}, "
            f"flat-stake ROI {p['flat_stake_roi']}, units {p['units']}."
        )
    else:
        perf_line = "No certified at-or-before rows with win/loss outcomes."
    status_lines = aligned["primary_alignment_status"].value_counts(dropna=False).to_dict()
    status_md = "\n".join(f"- {k}: {v}" for k, v in status_lines.items())
    decision_md = "\n".join(
        f"- `{r.decision_key} = {r.decision_value}`" for r in decisions.itertuples(index=False)
    )
    output_md = "\n".join(f"- {name}: `{path}`" for name, path in outputs.items())
    return f"""# U1.5 Candidate-to-Odds Snapshot Alignment Repair — {AUDIT_DATE}

## Executive Summary

This bounded repair rechecked historical Hits U1.5 selection-time pricing using only preserved local candidate and odds artifacts. It did not request odds, write to the database, alter production behavior, infer UNDER prices from OVER prices, or use later snapshots as primary selection-time evidence.

Strict at-or-before price recovery: **{summary['exact_at_or_before_prices_recovered']} / {summary['tracked_u15_candidates']}** tracked U1.5 candidates.

Authoritative candidate timestamps recovered: **{summary['candidates_with_authoritative_decision_timestamps']} / {summary['tracked_u15_candidates']}**.

Preserved odds snapshots inventoried: **{summary['preserved_odds_snapshots']}**.

Certified performance on exact at-or-before rows: {perf_line}

## Alignment Status

{status_md}

## Direct Answer

With four preserved odds captures per day, the historical U1.5 price record can be recovered only where three things coexist: a repository-backed candidate timestamp, exact game/player identity, and a unique sportsbook binding for the retained selected UNDER price. Under the strict frozen policy this package recovered **{summary['exact_at_or_before_prices_recovered']}** certified at-or-before rows. Additional diagnostic market evidence exists in the ledger, but it remains excluded when it is later-only, identity-unresolved, timestamp-unresolved, price-mismatched, or sportsbook-ambiguous.

## Decisions

{decision_md}

## Outputs

{output_md}

## Guardrails

- No network or OddsAPI calls.
- No database writes.
- No inferred opposite-side prices.
- No later snapshot used as primary selection-time price.
- No threshold, timing, sportsbook, or price optimization.
- No production changes.
"""


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Repair U1.5 candidate-to-odds snapshot alignment from local artifacts.")
    ap.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    ap.add_argument("--odds-root", default=str(DEFAULT_ODDS_ROOT))
    ap.add_argument("--review-root", default=str(DEFAULT_REVIEW_ROOT))
    ap.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--mode", default="read_only", choices=["read_only"])
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    result = build(Path(args.manifest), Path(args.odds_root), Path(args.review_root), Path(args.output_dir))
    print(json.dumps(result["summary"], indent=2, sort_keys=True))
    print(f"summary={result['summary_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
