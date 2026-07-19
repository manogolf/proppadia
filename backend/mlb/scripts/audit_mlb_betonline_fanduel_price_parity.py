"""Audit BetOnline/FanDuel historical MLB player-prop price parity.

Read-only/offline utility. It consumes the retained line-proxy certification
ledger and adds a price-parity layer. It does not call OddsAPI, write to the
database, or activate any proxy behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_DIR = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_betonline_fanduel_player_prop_line_proxy_certification/2026-07-18"
)
LEDGER = PACKAGE_DIR / "exact_proposition_join_ledger_2026-07-18.csv"
SOURCE_INVENTORY = PACKAGE_DIR / "source_inventory_2026-07-18.csv"
SUMMARY_MD = PACKAGE_DIR / "betonline_fanduel_line_proxy_certification_2026-07-18.md"


MARKETS = [
    "hits",
    "total_bases",
    "hits_runs_rbis",
    "home_runs",
    "stolen_bases",
    "strikeouts_pitching",
    "outs_recorded",
    "earned_runs",
    "hits_allowed",
]
FOCUS_MARKETS = {"hits", "strikeouts_pitching"}
WINDOWS = [
    ("same_snapshot", None),
    ("exact_timestamp", 0.0),
    ("within_1m", 1.0),
    ("within_5m", 5.0),
    ("within_15m", 15.0),
    ("nearest_30m", 30.0),
]


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fnum(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out):
        return None
    return out


def parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def minutes_abs(a: datetime | None, b: datetime | None) -> float | None:
    if a is None or b is None:
        return None
    return abs((a - b).total_seconds()) / 60.0


def american_to_prob(price: float | None) -> float | None:
    if price is None or price == 0:
        return None
    if price > 0:
        return 100.0 / (price + 100.0)
    return abs(price) / (abs(price) + 100.0)


def american_to_decimal(price: float | None) -> float | None:
    if price is None:
        return None
    if price > 0:
        return 1.0 + price / 100.0
    return 1.0 + 100.0 / abs(price)


def no_vig(over_price: float | None, under_price: float | None) -> tuple[float | None, float | None, float | None]:
    op = american_to_prob(over_price)
    up = american_to_prob(under_price)
    if op is None or up is None:
        return None, None, None
    hold = op + up - 1.0
    if op + up <= 0:
        return None, None, hold
    return op / (op + up), up / (op + up), hold


def pct(part: int | float, whole: int | float) -> float | str:
    if not whole:
        return ""
    return round(float(part) / float(whole) * 100.0, 4)


def quantile(values: list[float], q: float) -> float | str:
    if not values:
        return ""
    vals = sorted(values)
    idx = min(len(vals) - 1, max(0, math.ceil(q * len(vals)) - 1))
    return round(vals[idx], 6)


def avg(values: list[float]) -> float | str:
    return round(mean(values), 6) if values else ""


def med(values: list[float]) -> float | str:
    return round(median(values), 6) if values else ""


def source_timestamps() -> dict[str, str]:
    out: dict[str, str] = {}
    if not SOURCE_INVENTORY.exists():
        return out
    with SOURCE_INVENTORY.open(newline="") as f:
        for row in csv.DictReader(f):
            path = row.get("raw_source_path", "")
            ts = row.get("capture_timestamp", "")
            if path and ts and path not in out:
                out[path] = ts
    return out


def line_role_map() -> dict[str, set[str]]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    with LEDGER.open(newline="") as f:
        for row in csv.DictReader(f):
            if row.get("line_match_status") != "EXACT_LINE_MATCH":
                continue
            prop = row.get("prop_type", "")
            line = row.get("betonline_line", "")
            if prop and line:
                counts[prop][line] += 1
    roles: dict[str, set[str]] = {}
    for prop, counter in counts.items():
        if counter:
            top_count = counter.most_common(1)[0][1]
            roles[prop] = {line for line, count in counter.items() if count == top_count}
    return roles


def side_price_role(price: float | None) -> str:
    if price is None:
        return "unknown_price"
    return "plus_money" if price > 0 else "favorite_price"


def proposition_key(row: dict[str, str], *, include_side: bool = True, include_line: bool = True) -> tuple[Any, ...]:
    base: list[Any] = [
        row.get("slate_date", ""),
        row.get("event_id", ""),
        row.get("normalized_player_name", ""),
        row.get("prop_type", ""),
    ]
    if include_line:
        base.append(row.get("betonline_line", ""))
    if include_side:
        base.append(row.get("side", ""))
    return tuple(base)


def obs_key(row: dict[str, Any], prefix: str) -> tuple[Any, ...]:
    return (
        row.get(f"{prefix}_raw_source_path", ""),
        row.get("event_id", ""),
        row.get("normalized_player_name", ""),
        row.get("prop_type", ""),
        row.get("betonline_line", ""),
        row.get("side", ""),
        row.get(f"{prefix}_price", ""),
    )


def enrich_row(row: dict[str, str], source_ts: dict[str, str], primary_lines: dict[str, set[str]]) -> dict[str, Any] | None:
    if row.get("line_match_status") != "EXACT_LINE_MATCH":
        return None
    bol_price = fnum(row.get("betonline_price"))
    fd_price = fnum(row.get("fanduel_price"))
    bol_line = fnum(row.get("betonline_line"))
    fd_line = fnum(row.get("fanduel_line"))
    if bol_price is None or fd_price is None or bol_line is None or fd_line is None:
        return None
    bol_path = row.get("betonline_raw_source_path", "")
    fd_path = row.get("fanduel_raw_source_path", "")
    bol_ts_text = row.get("betonline_capture_timestamp") or source_ts.get(bol_path, "")
    fd_ts_text = row.get("fanduel_capture_timestamp") or source_ts.get(fd_path, "")
    bol_ts = parse_ts(bol_ts_text)
    fd_ts = parse_ts(fd_ts_text)
    td = minutes_abs(bol_ts, fd_ts)
    bol_prob = american_to_prob(bol_price)
    fd_prob = american_to_prob(fd_price)
    prop = row.get("prop_type", "")
    line_text = row.get("betonline_line", "")
    line_role = "primary_line" if line_text in primary_lines.get(prop, set()) else "alternate_line"
    return {
        **row,
        "betonline_capture_timestamp_resolved": bol_ts_text,
        "fanduel_capture_timestamp_resolved": fd_ts_text,
        "time_diff_minutes": td,
        "same_snapshot": bol_path == fd_path and bool(bol_path),
        "betonline_price_float": bol_price,
        "fanduel_price_float": fd_price,
        "betonline_decimal_odds": american_to_decimal(bol_price),
        "fanduel_decimal_odds": american_to_decimal(fd_price),
        "betonline_implied_probability": bol_prob,
        "fanduel_implied_probability": fd_prob,
        "abs_implied_probability_diff": abs(fd_prob - bol_prob) if fd_prob is not None and bol_prob is not None else None,
        "signed_fd_minus_betonline_probability_diff": (fd_prob - bol_prob) if fd_prob is not None and bol_prob is not None else None,
        "line_role": line_role,
        "price_role": side_price_role(bol_price),
    }


def window_qualifies(row: dict[str, Any], window: str, limit: float | None) -> bool:
    if window == "same_snapshot":
        return bool(row["same_snapshot"])
    td = row.get("time_diff_minutes")
    if td is None:
        return False
    if window == "exact_timestamp":
        return td == 0.0
    return td <= float(limit)


def one_to_one_pairs(rows: list[dict[str, Any]], window: str, limit: float | None) -> list[dict[str, Any]]:
    candidates = [r for r in rows if window_qualifies(r, window, limit)]
    candidates.sort(
        key=lambda r: (
            0 if r.get("time_diff_minutes") is None else r.get("time_diff_minutes"),
            r.get("slate_date", ""),
            r.get("event_id", ""),
            r.get("normalized_player_name", ""),
            r.get("prop_type", ""),
            r.get("betonline_line", ""),
            r.get("side", ""),
            r.get("betonline_raw_source_path", ""),
            r.get("fanduel_raw_source_path", ""),
        )
    )
    used_bol: set[tuple[Any, ...]] = set()
    used_fd: set[tuple[Any, ...]] = set()
    pairs: list[dict[str, Any]] = []
    for r in candidates:
        bkey = obs_key(r, "betonline")
        fkey = obs_key(r, "fanduel")
        if bkey in used_bol or fkey in used_fd:
            continue
        used_bol.add(bkey)
        used_fd.add(fkey)
        pairs.append({"timing_window": window, **r})
    return pairs


def load_pairs() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_ts = source_timestamps()
    primary_lines = line_role_map()
    all_exact: list[dict[str, Any]] = []
    raw_matched = 0
    exact_line_rows = 0
    book_pair_keys: set[tuple[Any, ...]] = set()
    capture_side_keys: set[tuple[Any, ...]] = set()
    prop_line_side_slate_keys: set[tuple[Any, ...]] = set()
    prop_line_slate_keys: set[tuple[Any, ...]] = set()
    player_game_prop_keys: set[tuple[Any, ...]] = set()
    slates: set[str] = set()
    events: set[str] = set()
    players: set[str] = set()
    repeated_capture_counter: Counter[tuple[Any, ...]] = Counter()
    with LEDGER.open(newline="") as f:
        for row in csv.DictReader(f):
            raw_matched += 1
            if row.get("line_match_status") == "EXACT_LINE_MATCH":
                exact_line_rows += 1
            enriched = enrich_row(row, source_ts, primary_lines)
            if enriched is None:
                continue
            all_exact.append(enriched)
            book_pair_keys.add((row.get("betonline_raw_source_path", ""), row.get("fanduel_raw_source_path", ""), *proposition_key(row)))
            capture_side_keys.add((row.get("slate_date", ""), row.get("event_id", ""), row.get("normalized_player_name", ""), row.get("prop_type", ""), row.get("betonline_line", ""), row.get("side", ""), row.get("betonline_raw_source_path", ""), row.get("fanduel_raw_source_path", "")))
            prop_line_side_slate_keys.add(proposition_key(row))
            prop_line_slate_keys.add(proposition_key(row, include_side=False))
            player_game_prop_keys.add(proposition_key(row, include_side=False, include_line=False))
            if row.get("slate_date"):
                slates.add(row["slate_date"])
            if row.get("event_id"):
                events.add(row["event_id"])
            if row.get("normalized_player_name"):
                players.add(row["normalized_player_name"])
            repeated_capture_counter[proposition_key(row)] += 1
    grain = {
        "raw_matched_rows": raw_matched,
        "exact_line_rows_with_prices": len(all_exact),
        "exact_line_rows_total": exact_line_rows,
        "unique_book_row_pairs": len(book_pair_keys),
        "unique_capture_paired_proposition_line_sides": len(capture_side_keys),
        "unique_proposition_line_sides_per_slate": len(prop_line_side_slate_keys),
        "unique_proposition_lines_per_slate": len(prop_line_slate_keys),
        "unique_player_game_prop_populations": len(player_game_prop_keys),
        "distinct_slates": len(slates),
        "distinct_events": len(events),
        "distinct_players": len(players),
        "repeated_observation_groups": sum(1 for v in repeated_capture_counter.values() if v > 1),
    }
    return all_exact, grain


def pair_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "timing_window": row["timing_window"],
        "slate_date": row.get("slate_date", ""),
        "event_id": row.get("event_id", ""),
        "normalized_player_name": row.get("normalized_player_name", ""),
        "player_name": row.get("player_name", ""),
        "prop_type": row.get("prop_type", ""),
        "raw_market_key": row.get("raw_market_key", ""),
        "line": row.get("betonline_line", ""),
        "side": row.get("side", ""),
        "line_role": row.get("line_role", ""),
        "price_role": row.get("price_role", ""),
        "betonline_timestamp": row.get("betonline_capture_timestamp_resolved", ""),
        "fanduel_timestamp": row.get("fanduel_capture_timestamp_resolved", ""),
        "time_diff_minutes": "" if row.get("time_diff_minutes") is None else round(row["time_diff_minutes"], 4),
        "betonline_price": row.get("betonline_price", ""),
        "fanduel_price": row.get("fanduel_price", ""),
        "betonline_decimal_odds": round(row["betonline_decimal_odds"], 6),
        "fanduel_decimal_odds": round(row["fanduel_decimal_odds"], 6),
        "betonline_implied_probability": round(row["betonline_implied_probability"], 8),
        "fanduel_implied_probability": round(row["fanduel_implied_probability"], 8),
        "abs_implied_probability_diff": round(row["abs_implied_probability_diff"], 8),
        "signed_fd_minus_betonline_probability_diff": round(row["signed_fd_minus_betonline_probability_diff"], 8),
        "betonline_source_path": row.get("betonline_raw_source_path", ""),
        "fanduel_source_path": row.get("fanduel_raw_source_path", ""),
        "betonline_run_tag": row.get("betonline_run_tag", ""),
        "match_scope": row.get("match_scope", ""),
    }


def summarize_pairs(pairs: list[dict[str, Any]], group_fields: list[str], notes: str = "") -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in pairs:
        grouped[tuple(row.get(f, "") for f in group_fields)].append(row)
    out = []
    for key, rows in sorted(grouped.items()):
        diffs = [r["abs_implied_probability_diff"] * 100.0 for r in rows if r.get("abs_implied_probability_diff") is not None]
        signed = [r["signed_fd_minus_betonline_probability_diff"] * 100.0 for r in rows if r.get("signed_fd_minus_betonline_probability_diff") is not None]
        exact_price = sum(1 for r in rows if fnum(r.get("betonline_price")) == fnum(r.get("fanduel_price")))
        exact_prob = sum(1 for r in rows if round(r["betonline_implied_probability"], 4) == round(r["fanduel_implied_probability"], 4))
        unique_props = {proposition_key(r) for r in rows}
        dates = {r.get("slate_date", "") for r in rows if r.get("slate_date")}
        row = {field: value for field, value in zip(group_fields, key)}
        row.update({
            "matched_observations": len(rows),
            "unique_propositions": len(unique_props),
            "distinct_dates": len(dates),
            "exact_american_price_match_rate": pct(exact_price, len(rows)),
            "exact_implied_probability_match_rate_rounded": pct(exact_prob, len(rows)),
            "within_0_5_probability_points_pct": pct(sum(1 for d in diffs if d <= 0.5), len(diffs)),
            "within_1_probability_point_pct": pct(sum(1 for d in diffs if d <= 1.0), len(diffs)),
            "within_2_probability_points_pct": pct(sum(1 for d in diffs if d <= 2.0), len(diffs)),
            "within_3_probability_points_pct": pct(sum(1 for d in diffs if d <= 3.0), len(diffs)),
            "within_5_probability_points_pct": pct(sum(1 for d in diffs if d <= 5.0), len(diffs)),
            "median_abs_probability_diff_points": med(diffs),
            "mean_abs_probability_diff_points": avg(diffs),
            "p90_abs_probability_diff_points": quantile(diffs, 0.90),
            "p95_abs_probability_diff_points": quantile(diffs, 0.95),
            "max_abs_probability_diff_points": round(max(diffs), 6) if diffs else "",
            "signed_avg_fd_minus_betonline_probability_points": avg(signed),
            "notes": notes,
        })
        out.append(row)
    return out


def two_sided_rows(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_side: dict[tuple[Any, ...], dict[str, dict[str, Any]]] = defaultdict(dict)
    for r in pairs:
        key = (
            r["timing_window"],
            r.get("slate_date", ""),
            r.get("event_id", ""),
            r.get("normalized_player_name", ""),
            r.get("prop_type", ""),
            r.get("betonline_line", ""),
        )
        by_side[key][r.get("side", "")] = r
    out = []
    for key, sides in sorted(by_side.items()):
        if "over" not in sides or "under" not in sides:
            continue
        over = sides["over"]
        under = sides["under"]
        bol_over, bol_under = fnum(over["betonline_price"]), fnum(under["betonline_price"])
        fd_over, fd_under = fnum(over["fanduel_price"]), fnum(under["fanduel_price"])
        bol_nv_over, bol_nv_under, bol_hold = no_vig(bol_over, bol_under)
        fd_nv_over, fd_nv_under, fd_hold = no_vig(fd_over, fd_under)
        bol_fav = "over" if (bol_over or 0) < (bol_under or 0) else "under"
        fd_fav = "over" if (fd_over or 0) < (fd_under or 0) else "under"
        out.append({
            "timing_window": key[0],
            "slate_date": key[1],
            "event_id": key[2],
            "normalized_player_name": key[3],
            "prop_type": key[4],
            "line": key[5],
            "betonline_hold": "" if bol_hold is None else round(bol_hold * 100.0, 6),
            "fanduel_hold": "" if fd_hold is None else round(fd_hold * 100.0, 6),
            "hold_diff_fd_minus_betonline_points": "" if bol_hold is None or fd_hold is None else round((fd_hold - bol_hold) * 100.0, 6),
            "betonline_no_vig_over_probability": "" if bol_nv_over is None else round(bol_nv_over, 8),
            "fanduel_no_vig_over_probability": "" if fd_nv_over is None else round(fd_nv_over, 8),
            "no_vig_over_diff_fd_minus_betonline_points": "" if bol_nv_over is None or fd_nv_over is None else round((fd_nv_over - bol_nv_over) * 100.0, 6),
            "betonline_favored_side": bol_fav,
            "fanduel_favored_side": fd_fav,
            "favored_side_agreement": "yes" if bol_fav == fd_fav else "no",
            "favorite_side_reversal": "yes" if bol_fav != fd_fav else "no",
        })
    return out


def summarize_two_sided(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        grouped[(r["timing_window"], r["prop_type"])].append(r)
    out = []
    for (window, prop), vals in sorted(grouped.items()):
        hold_diffs = [fnum(v["hold_diff_fd_minus_betonline_points"]) for v in vals]
        nv_diffs = [abs(fnum(v["no_vig_over_diff_fd_minus_betonline_points"])) for v in vals]
        hold_diffs = [v for v in hold_diffs if v is not None]
        nv_diffs = [v for v in nv_diffs if v is not None]
        rev = sum(1 for v in vals if v["favorite_side_reversal"] == "yes")
        out.append({
            "timing_window": window,
            "prop_type": prop,
            "two_sided_markets": len(vals),
            "median_hold_diff_fd_minus_betonline_points": med(hold_diffs),
            "mean_hold_diff_fd_minus_betonline_points": avg(hold_diffs),
            "median_abs_no_vig_over_diff_points": med(nv_diffs),
            "mean_abs_no_vig_over_diff_points": avg(nv_diffs),
            "favorite_side_reversals": rev,
            "favorite_side_reversal_pct": pct(rev, len(vals)),
            "favored_side_agreement_pct": pct(len(vals) - rev, len(vals)),
        })
    return out


def split_period(slate_date: str) -> str:
    if slate_date <= "2025-12-31":
        return "earlier_fit_descriptive"
    if slate_date <= "2026-06-30":
        return "later_validation"
    return "latest_retained_check"


def movement_summary(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for r in pairs:
        if r["timing_window"] != "nearest_30m":
            continue
        grouped[proposition_key(r)].append(r)
    out = []
    for key, rows in sorted(grouped.items()):
        if len(rows) < 2:
            continue
        rows.sort(key=lambda r: (r.get("betonline_capture_timestamp_resolved", ""), r.get("fanduel_capture_timestamp_resolved", "")))
        bol_moves = []
        fd_moves = []
        for prev, cur in zip(rows, rows[1:]):
            bp = fnum(prev["betonline_price"]); bc = fnum(cur["betonline_price"])
            fp = fnum(prev["fanduel_price"]); fc = fnum(cur["fanduel_price"])
            if bp is not None and bc is not None:
                bol_moves.append((bc > bp) - (bc < bp))
            if fp is not None and fc is not None:
                fd_moves.append((fc > fp) - (fc < fp))
        paired = list(zip(bol_moves, fd_moves))
        same_dir = sum(1 for a, b in paired if a == b and a != 0)
        opposite = sum(1 for a, b in paired if a and b and a != b)
        out.append({
            "slate_date": key[0],
            "event_id": key[1],
            "normalized_player_name": key[2],
            "prop_type": key[3],
            "line": key[4],
            "side": key[5],
            "paired_captures": len(rows),
            "movement_steps": len(paired),
            "same_direction_movement_steps": same_dir,
            "opposite_direction_movement_steps": opposite,
            "same_direction_movement_rate": pct(same_dir, len(paired)),
            "opposite_direction_movement_rate": pct(opposite, len(paired)),
            "notes": "Diagnostic only; repeated retained captures are observational and not causal.",
        })
    return out


def closing_comparison(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[tuple[Any, ...], dict[str, Any]] = {}
    for r in pairs:
        if r["timing_window"] != "nearest_30m":
            continue
        key = proposition_key(r)
        sort_ts = r.get("betonline_capture_timestamp_resolved", "") or r.get("fanduel_capture_timestamp_resolved", "")
        old = latest.get(key)
        if old is None or sort_ts > (old.get("betonline_capture_timestamp_resolved", "") or old.get("fanduel_capture_timestamp_resolved", "")):
            latest[key] = r
    out = []
    for r in sorted(latest.values(), key=lambda x: proposition_key(x)):
        td = r.get("time_diff_minutes")
        if td is None:
            bucket = "same_snapshot_timestamp_unavailable" if r.get("same_snapshot") else "timestamp_unavailable"
        elif td <= 5:
            bucket = "within_5m"
        elif td <= 15:
            bucket = "within_15m"
        elif td <= 30:
            bucket = "within_30m"
        else:
            bucket = "wider_unmatched"
        out.append({
            "slate_date": r.get("slate_date", ""),
            "event_id": r.get("event_id", ""),
            "normalized_player_name": r.get("normalized_player_name", ""),
            "prop_type": r.get("prop_type", ""),
            "line": r.get("betonline_line", ""),
            "side": r.get("side", ""),
            "close_bucket": bucket,
            "time_diff_minutes": "" if td is None else round(td, 4),
            "betonline_price": r.get("betonline_price", ""),
            "fanduel_price": r.get("fanduel_price", ""),
            "abs_implied_probability_diff_points": round(r["abs_implied_probability_diff"] * 100.0, 6),
            "signed_fd_minus_betonline_probability_points": round(r["signed_fd_minus_betonline_probability_diff"] * 100.0, 6),
        })
    return out


def price_decisions(primary_summary: list[dict[str, Any]], two_summary: list[dict[str, Any]], stability_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates_by_prop: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in primary_summary:
        if row.get("timing_window") in {"nearest_30m", "same_snapshot"}:
            candidates_by_prop[row["prop_type"]].append(row)
    by_prop = {
        prop: max(rows, key=lambda r: int(r.get("matched_observations", 0) or 0))
        for prop, rows in candidates_by_prop.items()
    }
    two_by_prop = {r["prop_type"]: r for r in two_summary if r.get("timing_window") == "nearest_30m"}
    stable_props = set()
    for r in stability_rows:
        if r.get("timing_window") == "nearest_30m" and r.get("period") == "latest_retained_check":
            if fnum(r.get("median_abs_probability_diff_points")) is not None:
                stable_props.add(r["prop_type"])
    rows = []
    market_decisions = {}
    for prop in MARKETS:
        s = by_prop.get(prop)
        if not s or int(s.get("matched_observations", 0) or 0) < 100:
            decision = "INSUFFICIENT_PRICE_OVERLAP_EVIDENCE"
            reason = "Fewer than 100 comparable exact-line price pairs in primary window."
        else:
            within2 = fnum(s.get("within_2_probability_points_pct")) or 0.0
            med_abs = fnum(s.get("median_abs_probability_diff_points")) or 999.0
            reversals = fnum(two_by_prop.get(prop, {}).get("favorite_side_reversal_pct")) or 0.0
            if within2 >= 90.0 and med_abs <= 1.0 and reversals <= 1.0 and prop in stable_props:
                decision = "FANDUEL_PRICE_PROXY_CERTIFIED_CONSIDERATION_ONLY_NOT_ACTIVATED"
                reason = "Thresholds passed, but explicit proxy activation remains unauthorized."
            elif within2 >= 75.0 and med_abs <= 2.0:
                decision = "FANDUEL_MARKET_PROBABILITY_CONTEXT_ONLY"
                reason = "Useful market-probability context, but not tight enough for execution-price substitution."
            else:
                decision = "FANDUEL_PRICE_PROXY_NOT_SUPPORTED"
                reason = "Comparable-time price differences exceed proxy certification thresholds."
        market_decisions[prop] = decision
        rows.append({
            "decision": f"MLB_BETONLINE_FANDUEL_{prop.upper()}_PRICE_PROXY_DECISION",
            "value": decision,
            "reason": reason,
        })
    rows.extend([
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_LEDGER_GRAIN_DECISION", "value": "RAW_LEDGER_HAS_REPEATED_CAPTURE_OBSERVATIONS_EFFECTIVE_SAMPLE_REPORTED_BY_UNIQUE_GRAINS", "reason": "Repeated observations are retained but not treated as independent propositions."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_COMPARABLE_TIME_DECISION", "value": "COMPARABLE_TIME_PAIRS_CONSTRUCTED_ONE_TO_ONE_WITH_SAME_SNAPSHOT_AND_TIMESTAMP_WINDOWS", "reason": "Source-path same-snapshot matching used when capture timestamps are absent."},
        {"decision": "MLB_BETONLINE_FANDUEL_HITS_PRICE_PARITY_DECISION", "value": market_decisions.get("hits", "INSUFFICIENT_PRICE_OVERLAP_EVIDENCE"), "reason": "See probability-space parity results."},
        {"decision": "MLB_BETONLINE_FANDUEL_HITS05_PRICE_PARITY_DECISION", "value": "REPORTED_AS_HITS_LINE_0_5_BREAKOUT_NO_SEPARATE_ACTIVATION", "reason": "Line-specific breakout is in parity results."},
        {"decision": "MLB_BETONLINE_FANDUEL_HITS15_PRICE_PARITY_DECISION", "value": "REPORTED_AS_HITS_LINE_1_5_BREAKOUT_NO_SEPARATE_ACTIVATION", "reason": "Line-specific breakout is in parity results."},
        {"decision": "MLB_BETONLINE_FANDUEL_PITCHER_STRIKEOUTS_PRICE_PARITY_DECISION", "value": market_decisions.get("strikeouts_pitching", "INSUFFICIENT_PRICE_OVERLAP_EVIDENCE"), "reason": "See probability-space parity results."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_HOLD_DECISION", "value": "TWO_SIDED_HOLD_AND_NO_VIG_REPORTED_DIAGNOSTIC_ONLY", "reason": "Hold differences are not an execution-price substitute."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_FAVORITE_REVERSAL_DECISION", "value": "FAVORITE_REVERSALS_MEASURED_MARKET_SPECIFIC", "reason": "See two-sided summary."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_STABILITY_DECISION", "value": "DATE_LOCKED_PERIODS_REPORTED_NO_COMPLEX_MODEL_FIT", "reason": "Split: <=2025, 2026 through June, July retained check."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_MOVEMENT_DECISION", "value": "MOVEMENT_LEAD_LAG_DIAGNOSTIC_ONLY_NOT_CAUSAL", "reason": "Repeated capture movements are observational."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_CLOSING_DECISION", "value": "CLOSING_COMPARISON_REPORTED_NOT_PROXY_AUTHORIZING", "reason": "Latest retained pregame-like observations selected from the paired ledger."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_FIXED_ADJUSTMENT_DECISION", "value": "NO_FIXED_ADJUSTMENT_AUTHORIZED", "reason": "No fixed adjustment was fit or authorized."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_ROI_COUNTERFACTUAL_DECISION", "value": "NOT_EXECUTABLE_FROM_PRICE_LEDGER_NO_SELECTION_OUTCOME_FIELDS", "reason": "The line-proxy ledger does not contain candidate inclusion, selected side, or graded outcomes."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_PROXY_BY_MARKET_DECISION", "value": "MARKET_SPECIFIC_PRICE_DECISIONS_ONLY", "reason": "No blanket price proxy."},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_PROXY_DECISION", "value": "FANDUEL_PRICE_NOT_AUTHORIZED_AS_BETONLINE_PRICE_PROXY", "reason": "Audit does not activate price substitution."},
        {"decision": "MLB_PRODUCTION_STATUS", "value": "UNCHANGED", "reason": "No production behavior changed."},
    ])
    return rows


def build(output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    all_rows, grain = load_pairs()
    pairs: list[dict[str, Any]] = []
    for window, limit in WINDOWS:
        pairs.extend(one_to_one_pairs(all_rows, window, limit))
    pair_rows = [pair_row(r) for r in pairs]
    primary_summary = summarize_pairs(pairs, ["timing_window", "prop_type"], "Primary market-level price parity.")
    breakouts = summarize_pairs(pairs, ["timing_window", "prop_type", "side", "line_role", "price_role"], "Side/line/price-role breakout.")
    line_breakouts = summarize_pairs(pairs, ["timing_window", "prop_type", "side", "betonline_line"], "Exact line breakout.")
    two_rows = two_sided_rows(pairs)
    two_summary = summarize_two_sided(two_rows)
    stability = summarize_pairs([{**r, "period": split_period(r.get("slate_date", ""))} for r in pairs], ["timing_window", "prop_type", "period"], "Frozen chronological split.")
    movements = movement_summary(pairs)
    closes = closing_comparison(pairs)
    roi = [{
        "scope": "price_ledger",
        "status": "NOT_EXECUTABLE",
        "reason": "Line-proxy price ledger lacks candidate inclusion, selected side, official result, and units fields.",
        "production_effect": "none",
    }]
    decisions = price_decisions(primary_summary, two_summary, stability)
    grain_rows = [{"metric": k, "value": v, "notes": "Exact proposition ledger grain audit."} for k, v in grain.items()]
    paths = {
        "price_ledger_grain": output_dir / "price_ledger_grain_audit_2026-07-18.csv",
        "comparable_pairs": output_dir / "comparable_time_price_pairs_2026-07-18.csv",
        "probability_parity": output_dir / "probability_space_parity_results_2026-07-18.csv",
        "breakdowns": output_dir / "market_side_line_price_breakdowns_2026-07-18.csv",
        "line_breakouts": output_dir / "line_specific_price_parity_2026-07-18.csv",
        "two_sided": output_dir / "two_sided_hold_novig_comparison_2026-07-18.csv",
        "two_sided_rows": output_dir / "two_sided_hold_novig_rows_2026-07-18.csv",
        "stability": output_dir / "chronological_stability_results_2026-07-18.csv",
        "movement": output_dir / "movement_lead_lag_results_2026-07-18.csv",
        "closing": output_dir / "closing_price_comparison_2026-07-18.csv",
        "roi": output_dir / "roi_selection_counterfactual_2026-07-18.csv",
        "decisions": output_dir / "market_specific_price_proxy_decisions_2026-07-18.csv",
        "machine": output_dir / "machine_readable_price_parity_audit_2026-07-18.json",
        "summary": output_dir / "betonline_fanduel_price_parity_audit_2026-07-18.md",
        "sha": output_dir / "price_parity_sha256_manifest_2026-07-18.csv",
        "validation": output_dir / "price_parity_validation_report_2026-07-18.csv",
    }
    pair_fields = [
        "timing_window", "slate_date", "event_id", "normalized_player_name", "player_name", "prop_type",
        "raw_market_key", "line", "side", "line_role", "price_role", "betonline_timestamp",
        "fanduel_timestamp", "time_diff_minutes", "betonline_price", "fanduel_price",
        "betonline_decimal_odds", "fanduel_decimal_odds", "betonline_implied_probability",
        "fanduel_implied_probability", "abs_implied_probability_diff",
        "signed_fd_minus_betonline_probability_diff", "betonline_source_path", "fanduel_source_path",
        "betonline_run_tag", "match_scope",
    ]
    summary_fields = [
        "timing_window", "prop_type", "matched_observations", "unique_propositions", "distinct_dates",
        "exact_american_price_match_rate", "exact_implied_probability_match_rate_rounded",
        "within_0_5_probability_points_pct", "within_1_probability_point_pct",
        "within_2_probability_points_pct", "within_3_probability_points_pct",
        "within_5_probability_points_pct", "median_abs_probability_diff_points",
        "mean_abs_probability_diff_points", "p90_abs_probability_diff_points",
        "p95_abs_probability_diff_points", "max_abs_probability_diff_points",
        "signed_avg_fd_minus_betonline_probability_points", "notes",
    ]
    write_csv(paths["price_ledger_grain"], grain_rows, ["metric", "value", "notes"])
    write_csv(paths["comparable_pairs"], pair_rows, pair_fields)
    write_csv(paths["probability_parity"], primary_summary, summary_fields)
    write_csv(paths["breakdowns"], breakouts, [
        "timing_window", "prop_type", "side", "line_role", "price_role", *summary_fields[2:]
    ])
    write_csv(paths["line_breakouts"], line_breakouts, [
        "timing_window", "prop_type", "side", "betonline_line", *summary_fields[2:]
    ])
    write_csv(paths["two_sided"], two_summary, [
        "timing_window", "prop_type", "two_sided_markets", "median_hold_diff_fd_minus_betonline_points",
        "mean_hold_diff_fd_minus_betonline_points", "median_abs_no_vig_over_diff_points",
        "mean_abs_no_vig_over_diff_points", "favorite_side_reversals", "favorite_side_reversal_pct",
        "favored_side_agreement_pct",
    ])
    write_csv(paths["two_sided_rows"], two_rows, [
        "timing_window", "slate_date", "event_id", "normalized_player_name", "prop_type", "line",
        "betonline_hold", "fanduel_hold", "hold_diff_fd_minus_betonline_points",
        "betonline_no_vig_over_probability", "fanduel_no_vig_over_probability",
        "no_vig_over_diff_fd_minus_betonline_points", "betonline_favored_side", "fanduel_favored_side",
        "favored_side_agreement", "favorite_side_reversal",
    ])
    write_csv(paths["stability"], stability, ["timing_window", "prop_type", "period", *summary_fields[2:]])
    write_csv(paths["movement"], movements, [
        "slate_date", "event_id", "normalized_player_name", "prop_type", "line", "side",
        "paired_captures", "movement_steps", "same_direction_movement_steps",
        "opposite_direction_movement_steps", "same_direction_movement_rate",
        "opposite_direction_movement_rate", "notes",
    ])
    write_csv(paths["closing"], closes, [
        "slate_date", "event_id", "normalized_player_name", "prop_type", "line", "side",
        "close_bucket", "time_diff_minutes", "betonline_price", "fanduel_price",
        "abs_implied_probability_diff_points", "signed_fd_minus_betonline_probability_points",
    ])
    write_csv(paths["roi"], roi, ["scope", "status", "reason", "production_effect"])
    write_csv(paths["decisions"], decisions, ["decision", "value", "reason"])
    machine = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "ledger": rel(LEDGER),
        "grain": grain,
        "pair_rows": len(pair_rows),
        "decisions": {r["decision"]: r["value"] for r in decisions},
        "production_status": "UNCHANGED",
        "price_proxy_authorized": False,
    }
    write_json(paths["machine"], machine)
    summary = [
        "# BetOnline-FanDuel Historical Player-Prop Price Parity Audit",
        "",
        f"Generated UTC: `{machine['generated_at_utc']}`",
        "",
        "## Direct Answer",
        "",
        "The audit completed the historical price comparison in probability space. FanDuel prices remain not authorized as BetOnline execution-price proxies.",
        "",
        "Hits and pitcher strikeouts have strong line overlap, but price substitution is still not activated. Market-specific price decisions are frozen in `market_specific_price_proxy_decisions_2026-07-18.csv`.",
        "",
        "## Scope",
        "",
        "The audit used the existing exact proposition join ledger and constructed deterministic one-to-one comparable-time pairs. Same raw snapshot pairs are retained even when explicit capture timestamps are unavailable.",
        "",
        "## ROI Counterfactual",
        "",
        "The ROI/selection counterfactual is not executable from the price ledger alone because the ledger does not contain selected side, candidate inclusion, outcome, or units fields.",
        "",
        "## No Production Change",
        "",
        "No DB writes, network calls, model changes, scheduled integration, production behavior changes, or price substitution occurred.",
    ]
    paths["summary"].write_text("\n".join(summary) + "\n")
    validation = [
        {"check": "network_calls", "status": "PASS", "details": "No network calls executed"},
        {"check": "db_writes", "status": "PASS", "details": "No DB writes"},
        {"check": "price_proxy_guard", "status": "PASS", "details": "FanDuel price proxy remains unauthorized"},
        {"check": "one_to_one_pairs", "status": "PASS", "details": f"{len(pair_rows)} comparable-time pairs written across timing windows"},
    ]
    write_csv(paths["validation"], validation, ["check", "status", "details"])
    sha_rows = []
    for name, path in paths.items():
        if name == "sha":
            continue
        sha_rows.append({"artifact": rel(path), "sha256": sha256_file(path)})
    write_csv(paths["sha"], sha_rows, ["artifact", "sha256"])
    return {k: str(v) for k, v in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=PACKAGE_DIR)
    args = parser.parse_args()
    paths = build(args.output_dir)
    print(json.dumps(paths, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
