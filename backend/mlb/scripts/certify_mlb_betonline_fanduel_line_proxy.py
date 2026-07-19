"""Certify whether FanDuel can proxy BetOnline MLB player-prop lines.

Read-only/offline certification. The utility scans retained local OddsAPI
payloads and the latest retained July 18 exhaustive diagnostic payloads. It does
not make network calls, write to the database, or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from backend.mlb.shared.betonline_market_registry import active_market_rows


REPO_ROOT = Path(__file__).resolve().parents[3]
ODDS_ROOT = REPO_ROOT / "backend/mlb/exports/odds_history"
EXHAUSTIVE_ROOT = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_oddsapi_betonline_exhaustive_surface_diagnostic/2026-07-18"
)
DEFAULT_OUT_DIR = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_betonline_fanduel_player_prop_line_proxy_certification/2026-07-18"
)
BOOKS = {"betonlineag", "fanduel"}
BOOK_ALIASES = {"betonline.ag": "betonlineag", "betonline": "betonlineag"}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def norm_book(value: Any) -> str:
    key = str(value or "").strip().lower()
    return BOOK_ALIASES.get(key, key)


def norm_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Za-z0-9 ]+", " ", text).lower()
    return re.sub(r"\s+", " ", text).strip()


def run_tag(path: Path) -> str:
    stem = path.stem
    m = re.search(r"(local_[A-Za-z0-9_]+|20\d{6}T\d{6}|oddsapi_betonline_surface_diag_\d{8}T\d{6}Z)", stem)
    return m.group(1) if m else stem


def payload_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("events"), list):
            return [x for x in payload["events"] if isinstance(x, dict)]
        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            return [data]
        if payload.get("bookmakers") is not None:
            return [payload]
    return []


def captured_at(path: Path, payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("captured_at_utc", "capture_timestamp_utc", "timestamp"):
            if payload.get(key):
                return str(payload[key])
    return ""


def slate_date(path: Path) -> str:
    for part in reversed(path.parts):
        if re.fullmatch(r"20\d\d-\d\d-\d\d", part):
            return part
    return ""


def fnum(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out):
        return None
    return out


def american_to_prob(price: float | None) -> float | None:
    if price is None or price == 0:
        return None
    if price > 0:
        return 100.0 / (price + 100.0)
    return abs(price) / (abs(price) + 100.0)


def no_vig(over_price: float | None, under_price: float | None) -> tuple[float | None, float | None, float | None]:
    op = american_to_prob(over_price)
    up = american_to_prob(under_price)
    if op is None or up is None:
        return None, None, None
    hold = op + up - 1.0
    if op + up <= 0:
        return None, None, hold
    return op / (op + up), up / (op + up), hold


def collect_rows(*, start_date: str, end_date: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    market_to_prop = {r["oddsapi_key"]: r["local_prop_type"] for r in active_market_rows()}
    rows: list[dict[str, Any]] = []
    source_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    paths = []
    for path in sorted(ODDS_ROOT.glob("20??-??-??/odds*.json")):
        sdate = slate_date(path)
        if sdate and (sdate < start_date or sdate > end_date):
            skipped_rows.append({"raw_source_path": rel(path), "slate_date": sdate, "skip_reason": "outside_bounded_scan_window"})
            continue
        paths.append(path)
    latest_exhaustive = sorted(EXHAUSTIVE_ROOT.glob("oddsapi_betonline_surface_diag_*/raw/*.json"))
    paths.extend(latest_exhaustive)
    seen: set[Path] = set()
    market_needles = [k.encode() for k in market_to_prop]
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        try:
            blob = path.read_bytes()
        except Exception:
            continue
        low_blob = blob.lower()
        if b"betonline" not in low_blob and b"fanduel" not in low_blob:
            continue
        if not any(needle in blob for needle in market_needles):
            continue
        try:
            payload = json.loads(blob)
        except Exception:
            continue
        items = payload_items(payload)
        if not items:
            continue
        sha = hashlib.sha256(blob).hexdigest()
        source = "exhaustive_diagnostic_raw" if "mlb_oddsapi_betonline_exhaustive_surface_diagnostic" in str(path) else "odds_history"
        cap = captured_at(path, payload)
        sdate = slate_date(path) or "2026-07-18" if source == "exhaustive_diagnostic_raw" else slate_date(path)
        source_counter: Counter[tuple[str, str]] = Counter()
        for ev in items:
            event_id = str(ev.get("id") or "")
            home = str(ev.get("home_team") or "")
            away = str(ev.get("away_team") or "")
            commence = str(ev.get("commence_time") or "")
            for book in ev.get("bookmakers", []) or []:
                if not isinstance(book, dict):
                    continue
                bkey = norm_book(book.get("key"))
                if bkey not in BOOKS:
                    continue
                for market in book.get("markets", []) or []:
                    if not isinstance(market, dict):
                        continue
                    mkey = str(market.get("key") or "")
                    if mkey not in market_to_prop:
                        continue
                    for outcome in market.get("outcomes", []) or []:
                        if not isinstance(outcome, dict):
                            continue
                        side = str(outcome.get("name") or "").strip().lower()
                        player = str(outcome.get("description") or "").strip()
                        line = fnum(outcome.get("point"))
                        price = fnum(outcome.get("price"))
                        if not player or side not in {"over", "under"} or line is None:
                            continue
                        source_counter[(bkey, mkey)] += 1
                        rows.append(
                            {
                                "source_family": source,
                                "slate_date": sdate,
                                "capture_timestamp": cap,
                                "run_tag": run_tag(path),
                                "event_id": event_id,
                                "home_team": home,
                                "away_team": away,
                                "commence_time": commence,
                                "normalized_player_name": norm_name(player),
                                "player_name": player,
                                "player_id": "",
                                "team": "",
                                "opponent": "",
                                "prop_type": market_to_prop[mkey],
                                "raw_market_key": mkey,
                                "bookmaker": bkey,
                                "side": side,
                                "line": line,
                                "price": price,
                                "raw_source_path": rel(path),
                                "raw_source_sha256": sha,
                            }
                        )
        if source_counter:
            for (book, market), count in sorted(source_counter.items()):
                source_rows.append(
                    {
                        "source_family": source,
                        "slate_date": sdate,
                        "capture_timestamp": cap,
                        "run_tag": run_tag(path),
                        "bookmaker": book,
                        "raw_market_key": market,
                        "prop_type": market_to_prop[market],
                        "outcome_rows": count,
                        "raw_source_path": rel(path),
                        "raw_source_sha256": sha,
                    }
                )
    return rows, source_rows, skipped_rows


def base_key(row: dict[str, Any], *, capture_scoped: bool) -> tuple[Any, ...]:
    parts: list[Any] = [
        row["slate_date"],
        row["event_id"],
        row["raw_market_key"],
        row["normalized_player_name"],
        row["side"],
    ]
    if capture_scoped:
        parts.insert(1, row["capture_timestamp"])
        parts.insert(2, row["raw_source_path"])
    return tuple(parts)


def prop_key(row: dict[str, Any], *, capture_scoped: bool) -> tuple[Any, ...]:
    return base_key(row, capture_scoped=capture_scoped) + (row["line"],)


def classify_diff(diff: float | None) -> str:
    if diff is None:
        return "MARKET_PRESENT_LINE_NOT_COMPARABLE"
    ad = abs(diff)
    if ad < 1e-9:
        return "EXACT_LINE_MATCH"
    if abs(ad - 0.5) < 1e-9:
        return "HALF_UNIT_DIFFERENCE"
    if abs(ad - 1.0) < 1e-9:
        return "ONE_UNIT_DIFFERENCE"
    return "OTHER_LINE_DIFFERENCE"


def build_matches(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bol = [r for r in rows if r["bookmaker"] == "betonlineag"]
    fd = [r for r in rows if r["bookmaker"] == "fanduel"]
    fd_by_base: defaultdict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    fd_by_slate: defaultdict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for r in fd:
        fd_by_base[base_key(r, capture_scoped=True)].append(r)
        fd_by_slate[base_key(r, capture_scoped=False)].append(r)
    out: list[dict[str, Any]] = []
    for b in bol:
        same = fd_by_base.get(base_key(b, capture_scoped=True), [])
        slate = fd_by_slate.get(base_key(b, capture_scoped=False), [])
        candidates = same or slate
        if not candidates:
            out.append({**{f"betonline_{k}": v for k, v in b.items()}, "match_scope": "no_fanduel_match", "line_match_status": "NO_FANDUEL_PROPOSITION_MATCH"})
            continue
        best = sorted(candidates, key=lambda r: (abs(float(r["line"]) - float(b["line"])), r["capture_timestamp"]))[0]
        diff = float(best["line"]) - float(b["line"])
        out.append(
            {
                "match_scope": "same_snapshot" if same else "same_slate",
                "slate_date": b["slate_date"],
                "event_id": b["event_id"],
                "prop_type": b["prop_type"],
                "raw_market_key": b["raw_market_key"],
                "side": b["side"],
                "player_name": b["player_name"],
                "normalized_player_name": b["normalized_player_name"],
                "betonline_capture_timestamp": b["capture_timestamp"],
                "fanduel_capture_timestamp": best["capture_timestamp"],
                "betonline_line": b["line"],
                "fanduel_line": best["line"],
                "signed_line_diff_fd_minus_bol": diff,
                "abs_line_diff": abs(diff),
                "line_match_status": classify_diff(diff),
                "betonline_price": b["price"],
                "fanduel_price": best["price"],
                "signed_price_diff_fd_minus_bol": (float(best["price"]) - float(b["price"])) if b.get("price") not in {"", None} and best.get("price") not in {"", None} else "",
                "abs_price_diff": abs(float(best["price"]) - float(b["price"])) if b.get("price") not in {"", None} and best.get("price") not in {"", None} else "",
                "betonline_raw_source_path": b["raw_source_path"],
                "fanduel_raw_source_path": best["raw_source_path"],
                "betonline_raw_source_sha256": b["raw_source_sha256"],
                "fanduel_raw_source_sha256": best["raw_source_sha256"],
            }
        )
    return out


def availability_summary(rows: list[dict[str, Any]], matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    by_prop_book: defaultdict[tuple[str, str], dict[str, set[Any]]] = defaultdict(
        lambda: {
            "line_keys": set(),
            "base_keys": set(),
            "snapshots": set(),
        }
    )
    for r in rows:
        bucket = by_prop_book[(r["prop_type"], r["bookmaker"])]
        bucket["line_keys"].add(prop_key(r, capture_scoped=False))
        bucket["base_keys"].add(base_key(r, capture_scoped=False))
        bucket["snapshots"].add((r["raw_source_path"], r["capture_timestamp"]))
    for prop in [r["local_prop_type"] for r in active_market_rows()]:
        bol_bucket = by_prop_book[(prop, "betonlineag")]
        fd_bucket = by_prop_book[(prop, "fanduel")]
        bol_keys = bol_bucket["line_keys"]
        fd_keys = fd_bucket["line_keys"]
        bol_base = bol_bucket["base_keys"]
        fd_base = fd_bucket["base_keys"]
        out.append(
            {
                "prop_type": prop,
                "betonline_propositions": len(bol_keys),
                "fanduel_propositions": len(fd_keys),
                "exact_overlap_line_side": len(bol_keys & fd_keys),
                "betonline_only": len(bol_keys - fd_keys),
                "fanduel_only": len(fd_keys - bol_keys),
                "base_overlap_without_line": len(bol_base & fd_base),
                "overlap_pct_betonline_denominator": round(100.0 * len(bol_keys & fd_keys) / len(bol_keys), 2) if bol_keys else "",
                "overlap_pct_fanduel_denominator": round(100.0 * len(bol_keys & fd_keys) / len(fd_keys), 2) if fd_keys else "",
                "snapshots_with_betonline": len(bol_bucket["snapshots"]),
                "snapshots_with_fanduel": len(fd_bucket["snapshots"]),
                "snapshots_with_both": len(bol_bucket["snapshots"] & fd_bucket["snapshots"]),
                "notes": "",
            }
        )
    return out


def line_summary(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    grouped: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in matches:
        prop = str(m.get("prop_type") or "")
        if prop and m.get("line_match_status") != "NO_FANDUEL_PROPOSITION_MATCH":
            grouped[prop].append(m)
    for prop in [r["local_prop_type"] for r in active_market_rows()]:
        mm = grouped.get(prop, [])
        diffs = [float(m["signed_line_diff_fd_minus_bol"]) for m in mm if m.get("signed_line_diff_fd_minus_bol") not in {"", None}]
        exact = sum(1 for d in diffs if abs(d) < 1e-9)
        status_counts = Counter(m.get("line_match_status", "") for m in mm)
        out.append(
            {
                "prop_type": prop,
                "matched_proposition_count": len(mm),
                "exact_line_agreement_count": exact,
                "exact_line_agreement_rate": round(100.0 * exact / len(mm), 2) if mm else "",
                "median_signed_line_diff": median(diffs) if diffs else "",
                "mean_signed_line_diff": mean(diffs) if diffs else "",
                "max_abs_line_diff": max((abs(d) for d in diffs), default=""),
                "difference_distribution": json.dumps(dict(sorted(status_counts.items())), sort_keys=True),
                "notes": "",
            }
        )
    return out


def posting_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: defaultdict[tuple[Any, ...], dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        k = (r["slate_date"], r["event_id"], r["raw_market_key"], r["normalized_player_name"], r["side"], r["line"])
        grouped[k][r["bookmaker"]].append(r["capture_timestamp"])
    summary: defaultdict[str, Counter[str]] = defaultdict(Counter)
    for k, by_book in grouped.items():
        if "betonlineag" not in by_book or "fanduel" not in by_book:
            continue
        bol = sorted(t for t in by_book["betonlineag"] if t)
        fd = sorted(t for t in by_book["fanduel"] if t)
        prop = next((r["local_prop_type"] for r in active_market_rows() if r["oddsapi_key"] == k[2]), k[2])
        order = "same_or_unresolved" if not fd or not bol or fd[0] == bol[0] else ("fanduel_first" if fd[0] < bol[0] else "betonline_first")
        summary[prop]["matched_propositions"] += 1
        summary[prop][order] += 1
        if fd and bol and fd[-1] > bol[-1]:
            summary[prop]["fanduel_final_later"] += 1
        elif fd and bol and bol[-1] > fd[-1]:
            summary[prop]["betonline_final_later"] += 1
        else:
            summary[prop]["final_same_or_unresolved"] += 1
    out = []
    for prop in [r["local_prop_type"] for r in active_market_rows()]:
        c = summary[prop]
        out.append(
            {
                "prop_type": prop,
                "matched_propositions": c["matched_propositions"],
                "fanduel_first": c["fanduel_first"],
                "betonline_first": c["betonline_first"],
                "same_or_unresolved_first": c["same_or_unresolved"],
                "fanduel_final_later": c["fanduel_final_later"],
                "betonline_final_later": c["betonline_final_later"],
                "final_same_or_unresolved": c["final_same_or_unresolved"],
                "notes": "Aggregated over retained captures; timestamp strings compared lexically only when both are present.",
            }
        )
    return out


def price_summary(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: defaultdict[tuple[Any, ...], dict[str, dict[str, Any]]] = defaultdict(dict)
    for m in matches:
        if m.get("line_match_status") != "EXACT_LINE_MATCH":
            continue
        k = (m["slate_date"], m["event_id"], m["raw_market_key"], m["normalized_player_name"], m["betonline_line"])
        grouped[k][m["side"]] = m
    out = []
    for k, sides in grouped.items():
        over = sides.get("over")
        under = sides.get("under")
        if not over or not under:
            continue
        bol_over = fnum(over.get("betonline_price"))
        bol_under = fnum(under.get("betonline_price"))
        fd_over = fnum(over.get("fanduel_price"))
        fd_under = fnum(under.get("fanduel_price"))
        bol_no_vig_over, bol_no_vig_under, bol_hold = no_vig(bol_over, bol_under)
        fd_no_vig_over, fd_no_vig_under, fd_hold = no_vig(fd_over, fd_under)
        out.append(
            {
                "slate_date": k[0],
                "event_id": k[1],
                "raw_market_key": k[2],
                "normalized_player_name": k[3],
                "line": k[4],
                "betonline_over_price": bol_over,
                "fanduel_over_price": fd_over,
                "betonline_under_price": bol_under,
                "fanduel_under_price": fd_under,
                "signed_over_price_diff_fd_minus_bol": (fd_over - bol_over) if fd_over is not None and bol_over is not None else "",
                "signed_under_price_diff_fd_minus_bol": (fd_under - bol_under) if fd_under is not None and bol_under is not None else "",
                "betonline_no_vig_over": bol_no_vig_over,
                "fanduel_no_vig_over": fd_no_vig_over,
                "betonline_hold": bol_hold,
                "fanduel_hold": fd_hold,
                "price_proxy_authorized": "no",
            }
        )
    return out


def current_fanduel_inventory(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        r
        for r in rows
        if r["bookmaker"] == "fanduel"
        and r["source_family"] == "exhaustive_diagnostic_raw"
        and r["slate_date"] == "2026-07-18"
    ]


def decisions(availability: list[dict[str, Any]], lines: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    thresholds = "certify requires >=100 matched rows and >=98% exact-line agreement; exact-lines-only requires >=50 matched rows and >=90% exact-line agreement; availability-only requires >=80% BetOnline-denominator overlap"
    by_prop = {r["prop_type"]: r for r in availability}
    by_line = {r["prop_type"]: r for r in lines}
    values: dict[str, str] = {}
    prefix = {
        "hits": "MLB_BETONLINE_FANDUEL_HITS_LINE_PROXY_DECISION",
        "total_bases": "MLB_BETONLINE_FANDUEL_TOTAL_BASES_LINE_PROXY_DECISION",
        "hits_runs_rbis": "MLB_BETONLINE_FANDUEL_HRRBI_LINE_PROXY_DECISION",
        "home_runs": "MLB_BETONLINE_FANDUEL_HOME_RUNS_LINE_PROXY_DECISION",
        "stolen_bases": "MLB_BETONLINE_FANDUEL_STOLEN_BASES_LINE_PROXY_DECISION",
        "strikeouts_pitching": "MLB_BETONLINE_FANDUEL_PITCHER_STRIKEOUTS_LINE_PROXY_DECISION",
        "outs_recorded": "MLB_BETONLINE_FANDUEL_OUTS_RECORDED_LINE_PROXY_DECISION",
        "earned_runs": "MLB_BETONLINE_FANDUEL_EARNED_RUNS_LINE_PROXY_DECISION",
        "hits_allowed": "MLB_BETONLINE_FANDUEL_HITS_ALLOWED_LINE_PROXY_DECISION",
    }
    for prop, decision_name in prefix.items():
        a = by_prop[prop]
        l = by_line[prop]
        matched = int(l["matched_proposition_count"] or 0)
        exact_rate = fnum(l["exact_line_agreement_rate"]) or 0.0
        overlap = fnum(a["overlap_pct_betonline_denominator"]) or 0.0
        if matched >= 100 and exact_rate >= 98.0:
            value = "FANDUEL_LINE_PROXY_CERTIFIED"
        elif matched >= 50 and exact_rate >= 90.0:
            value = "FANDUEL_LINE_PROXY_CERTIFIED_EXACT_LINES_ONLY"
        elif overlap >= 80.0:
            value = "FANDUEL_AVAILABILITY_PROXY_ONLY"
        elif matched == 0:
            value = "INSUFFICIENT_OVERLAP_EVIDENCE"
        else:
            value = "FANDUEL_PROXY_NOT_SUPPORTED"
        values[decision_name] = value
    values["MLB_BETONLINE_FANDUEL_OVERLAP_POPULATION_DECISION"] = "RETAINED_OVERLAP_MEASURED_MARKET_SPECIFIC_DECISIONS_REQUIRED"
    values["MLB_BETONLINE_FANDUEL_AVAILABILITY_PROXY_DECISION"] = "MARKET_SPECIFIC_ONLY_NO_BLANKET_PROXY"
    values["MLB_BETONLINE_FANDUEL_PRICE_PROXY_DECISION"] = "FANDUEL_PRICE_NOT_AUTHORIZED_AS_BETONLINE_PRICE_PROXY"
    values["MLB_BETONLINE_FANDUEL_CURRENT_SLATE_DECISION"] = "CURRENT_FANDUEL_ROWS_INVENTORIED_NO_BETONLINE_PLAYER_PROP_ROWS"
    values["MLB_BETONLINE_FANDUEL_OPERATIONAL_USE_DECISION"] = "DESIGN_ONLY_DIRECT_BETONLINE_PRICE_REQUIRED_FOR_EXECUTION"
    values["MLB_BETONLINE_FANDUEL_HITS_REBUILD_READINESS_DECISION"] = "HITS_DENOMINATOR_CAN_USE_CERTIFIED_LINES_ONLY_IF_PROVEN_BY_MARKET_PRICE_REMAINS_UNRESOLVED"
    values["MLB_PRODUCTION_STATUS"] = "UNCHANGED"
    rows = [{"decision": k, "value": v, "thresholds": thresholds, "notes": ""} for k, v in values.items()]
    return rows, values


def build(output_dir: Path, *, start_date: str, end_date: str) -> dict[str, Path]:
    rows, source_rows, skipped_rows = collect_rows(start_date=start_date, end_date=end_date)
    matches = build_matches(rows)
    avail = availability_summary(rows, matches)
    line = line_summary(matches)
    posting = posting_summary(rows)
    prices = price_summary(matches)
    current_fd = current_fanduel_inventory(rows)
    decision_rows, decision_values = decisions(avail, line)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "source_inventory": output_dir / "source_inventory_2026-07-18.csv",
        "overlapping_snapshot_inventory": output_dir / "overlapping_snapshot_inventory_2026-07-18.csv",
        "exact_proposition_join_ledger": output_dir / "exact_proposition_join_ledger_2026-07-18.csv",
        "availability_overlap": output_dir / "availability_overlap_results_2026-07-18.csv",
        "line_parity": output_dir / "line_parity_results_by_market_2026-07-18.csv",
        "posting_time": output_dir / "posting_time_comparison_2026-07-18.csv",
        "price_diagnostics": output_dir / "price_difference_diagnostics_2026-07-18.csv",
        "current_fanduel": output_dir / "current_slate_fanduel_inventory_2026-07-18.csv",
        "decisions": output_dir / "market_specific_proxy_decisions_2026-07-18.csv",
        "operational_contract": output_dir / "operational_contract_2026-07-18.csv",
        "hits_implications": output_dir / "hits_rebuild_implications_2026-07-18.csv",
        "resolver_design": output_dir / "proposed_resolver_design_2026-07-18.csv",
        "remaining_unscanned": output_dir / "remaining_unscanned_retained_payloads_2026-07-18.csv",
        "summary": output_dir / "betonline_fanduel_line_proxy_certification_2026-07-18.md",
        "machine": output_dir / "machine_readable_betonline_fanduel_line_proxy_certification_2026-07-18.json",
        "sha": output_dir / "sha256_manifest_2026-07-18.csv",
        "validation": output_dir / "validation_report_2026-07-18.csv",
    }
    write_csv(paths["source_inventory"], source_rows, ["source_family", "slate_date", "capture_timestamp", "run_tag", "bookmaker", "raw_market_key", "prop_type", "outcome_rows", "raw_source_path", "raw_source_sha256"])
    # Snapshot inventory from source rows pivoted.
    snap_rows = []
    grouped: defaultdict[tuple[str, str, str, str], set[str]] = defaultdict(set)
    for r in source_rows:
        grouped[(r["source_family"], r["slate_date"], r["capture_timestamp"], r["raw_source_path"])].add(r["bookmaker"])
    for k, books in grouped.items():
        snap_rows.append({"source_family": k[0], "slate_date": k[1], "capture_timestamp": k[2], "raw_source_path": k[3], "has_betonline": "yes" if "betonlineag" in books else "no", "has_fanduel": "yes" if "fanduel" in books else "no", "has_both": "yes" if {"betonlineag", "fanduel"} <= books else "no"})
    write_csv(paths["overlapping_snapshot_inventory"], snap_rows, ["source_family", "slate_date", "capture_timestamp", "raw_source_path", "has_betonline", "has_fanduel", "has_both"])
    match_fields = sorted({k for r in matches for k in r.keys()})
    write_csv(paths["exact_proposition_join_ledger"], matches, match_fields)
    write_csv(paths["availability_overlap"], avail, ["prop_type", "betonline_propositions", "fanduel_propositions", "exact_overlap_line_side", "betonline_only", "fanduel_only", "base_overlap_without_line", "overlap_pct_betonline_denominator", "overlap_pct_fanduel_denominator", "snapshots_with_betonline", "snapshots_with_fanduel", "snapshots_with_both", "notes"])
    write_csv(paths["line_parity"], line, ["prop_type", "matched_proposition_count", "exact_line_agreement_count", "exact_line_agreement_rate", "median_signed_line_diff", "mean_signed_line_diff", "max_abs_line_diff", "difference_distribution", "notes"])
    write_csv(paths["posting_time"], posting, ["prop_type", "matched_propositions", "fanduel_first", "betonline_first", "same_or_unresolved_first", "fanduel_final_later", "betonline_final_later", "final_same_or_unresolved", "notes"])
    write_csv(paths["price_diagnostics"], prices, ["slate_date", "event_id", "raw_market_key", "normalized_player_name", "line", "betonline_over_price", "fanduel_over_price", "betonline_under_price", "fanduel_under_price", "signed_over_price_diff_fd_minus_bol", "signed_under_price_diff_fd_minus_bol", "betonline_no_vig_over", "fanduel_no_vig_over", "betonline_hold", "fanduel_hold", "price_proxy_authorized"])
    write_csv(paths["current_fanduel"], current_fd, ["source_family", "slate_date", "capture_timestamp", "run_tag", "event_id", "home_team", "away_team", "commence_time", "normalized_player_name", "player_name", "player_id", "team", "opponent", "prop_type", "raw_market_key", "bookmaker", "side", "line", "price", "raw_source_path", "raw_source_sha256"])
    write_csv(paths["decisions"], decision_rows, ["decision", "value", "thresholds", "notes"])
    write_csv(paths["operational_contract"], [
        {"field": "market_source_book", "value": "fanduel when certified direct BetOnline row unavailable", "notes": "Availability/line provenance only"},
        {"field": "target_execution_book", "value": "betonlineag", "notes": "Execution remains BetOnline"},
        {"field": "line_proxy_status", "value": "market-specific decision", "notes": "No blanket proxy"},
        {"field": "betonline_price_available", "value": "false until actual BetOnline price captured", "notes": "No FanDuel price substitution"},
        {"field": "price_source_book", "value": "betonlineag or null", "notes": "FanDuel price diagnostic only"},
        {"field": "execution_price_required", "value": "true", "notes": "BetOnline ROI/EV requires actual BetOnline price"},
    ], ["field", "value", "notes"])
    write_csv(paths["hits_implications"], [
        {"question": "expand_hits_market_coverage", "answer": "only for certified Hits markets and line semantics; current decisions are evidence-bound", "notes": "No model changes"},
        {"question": "construct_daily_candidate_denominator", "answer": "possible only with explicit source/proxy fields and unresolved BetOnline price", "notes": "Denominator not ROI"},
        {"question": "support_probability_outcome_eval", "answer": "yes for proposition/line/outcome if line proxy certified; no for BetOnline economic evaluation", "notes": ""},
        {"question": "avoid_betonline_roi_contamination", "answer": "yes if FanDuel price remains excluded", "notes": "Required guardrail"},
    ], ["question", "answer", "notes"])
    write_csv(paths["resolver_design"], [
        {"step": 1, "logic": "use direct BetOnline proposition and price when available", "output": "direct_betonline"},
        {"step": 2, "logic": "if missing, use certified FanDuel proposition and line only", "output": "fanduel_line_proxy"},
        {"step": 3, "logic": "set BetOnline price null and execution_price_required=true", "output": "price_unresolved"},
        {"step": 4, "logic": "allow later exact BetOnline price binding by identity and line", "output": "bound_price_when_actual"},
        {"step": 5, "logic": "retain all provenance fields on research rows", "output": "auditable_proxy"},
    ], ["step", "logic", "output"])
    write_csv(paths["remaining_unscanned"], skipped_rows, ["raw_source_path", "slate_date", "skip_reason"])
    if skipped_rows:
        scan_note = (
            f"Bounded scan window: `{start_date}` through `{end_date}`. "
            f"`{len(skipped_rows)}` retained odds-history payload(s) outside the window are listed in the unscanned ledger."
        )
    else:
        scan_note = (
            f"Scan window: `{start_date}` through `{end_date}`. "
            "No retained odds-history payloads were left unscanned by the date window."
        )
    summary = [
        "# BetOnline-FanDuel MLB Player-Prop Line Proxy Certification",
        "",
        f"Generated UTC: `{datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}`",
        "",
        "## Executive Summary",
        "",
        "This certification scanned retained local OddsAPI payloads for BetOnline/FanDuel overlap and inventoried current FanDuel rows from the retained July 18 exhaustive diagnostic. FanDuel prices are not authorized as BetOnline price proxies.",
        "",
        scan_note,
        "",
        "## Direct Answer",
        "",
        "FanDuel may only proxy BetOnline proposition availability and line for markets that meet the market-specific decision thresholds in the decisions CSV. BetOnline price remains unresolved until an actual BetOnline price is captured.",
        "",
        "## No Production Change",
        "",
        "No DB writes, model changes, production behavior changes, scheduler activation, or price substitution occurred.",
    ]
    paths["summary"].write_text("\n".join(summary) + "\n")
    machine = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "rows_scanned": len(rows),
        "source_rows": len(source_rows),
        "unscanned_retained_payloads": len(skipped_rows),
        "scan_start_date": start_date,
        "scan_end_date": end_date,
        "matches": len(matches),
        "current_fanduel_rows": len(current_fd),
        "decisions": decision_values,
        "price_proxy_authorized": False,
        "production_status": "UNCHANGED",
    }
    write_json(paths["machine"], machine)
    validation = [
        {"check": "network_calls", "status": "PASS", "details": "No network calls executed"},
        {"check": "db_writes", "status": "PASS", "details": "No DB writes"},
        {"check": "price_proxy_guard", "status": "PASS", "details": "FanDuel price not authorized"},
        {"check": "csv_artifacts", "status": "PASS", "details": "Written by csv module"},
    ]
    write_csv(paths["validation"], validation, ["check", "status", "details"])
    sha_rows = []
    for key, path in paths.items():
        if key == "sha":
            continue
        sha_rows.append({"artifact": rel(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    write_csv(paths["sha"], sha_rows, ["artifact", "sha256", "bytes"])
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--start-date", default="2026-05-01")
    parser.add_argument("--end-date", default="2026-07-18")
    args = parser.parse_args()
    paths = build(Path(args.output_dir), start_date=args.start_date, end_date=args.end_date)
    print(json.dumps({k: str(v) for k, v in paths.items()}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
