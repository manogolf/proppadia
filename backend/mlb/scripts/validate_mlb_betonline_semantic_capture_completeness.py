"""Validate semantic completeness of one retained MLB BetOnline odds capture.

Transport health is not enough for governed-player-prop capture. This validator
checks whether the retained payload contains the expected BetOnline player-prop
markets and reports FanDuel counts as a comparison signal. It is read-only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.mlb.shared.betonline_market_registry import active_market_to_prop_map, active_market_rows


TARGET_BOOK = "betonlineag"
COMPARE_BOOK = "fanduel"
BOOK_ALIASES = {"betonline": "betonlineag", "betonline.ag": "betonlineag", "fanduel sportsbook": "fanduel"}
SCHEDULED_WINDOWS = [
    ("05:30", "12:30Z"),
    ("09:30", "16:30Z"),
    ("11:00", "18:00Z"),
    ("13:00", "20:00Z"),
    ("16:30", "23:30Z"),
]
WINDOW_BINDING_TOLERANCE_MINUTES = 45
CORE_MARKETS = {"hits", "total_bases", "hits_runs_rbis", "strikeouts_pitching", "outs_recorded"}
_SCHEDULED_WINDOW_LABELS = {pt for pt, _ in SCHEDULED_WINDOWS} | {utc for _, utc in SCHEDULED_WINDOWS}
SCHEDULER_IDENTITY = "com.proppadia.mlb.refresh.daily"
SCHEDULER_STDOUT_PATH = "artifacts/ops/mlb_refresh_daily.out.log"
SCHEDULER_STDERR_PATH = "artifacts/ops/mlb_refresh_daily.err.log"


def normalize_book(value: Any) -> str:
    text = str(value or "").strip().lower()
    return BOOK_ALIASES.get(text, text)


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


def captured_at(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("captured_at_utc", "capture_timestamp_utc", "timestamp"):
            if payload.get(key):
                return str(payload[key])
    return ""


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _date_key(value: Any) -> str:
    try:
        raw = str(value or "").strip()
        return date.fromisoformat(raw[:10]).isoformat() if raw else ""
    except Exception:
        return ""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def normalize_scheduled_window(value: Any) -> str:
    """Map slightly delayed wrapper invocations back to the governed window."""
    text = str(value or "").strip()
    if not text:
        return ""
    if text in _SCHEDULED_WINDOW_LABELS:
        return text
    try:
        hour, minute = text.replace("Z", "").split(":", 1)
        actual_minutes = int(hour) * 60 + int(minute[:2])
    except Exception:
        return text
    nearest = ""
    nearest_delta = 10**9
    for pacific, _ in SCHEDULED_WINDOWS:
        sched_hour, sched_minute = pacific.split(":", 1)
        sched_minutes = int(sched_hour) * 60 + int(sched_minute)
        delta = abs(actual_minutes - sched_minutes)
        if delta < nearest_delta:
            nearest = pacific
            nearest_delta = delta
    return nearest if nearest_delta <= 20 else text


def validate_payload(path: Path, *, expected_run_time: str = "", previous_counts: dict[str, int] | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    market_to_prop = active_market_to_prop_map()
    previous_counts = previous_counts or {}
    raw_status = "RAW_PRESENT"
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except Exception as exc:
        rows = []
        for market in active_market_rows():
            rows.append(
                {
                    "prop_type": market["local_prop_type"],
                    "raw_market_key": market["oddsapi_key"],
                    "status": "REQUEST_FAILED",
                    "betonline_rows": 0,
                    "fanduel_rows": 0,
                    "betonline_games_covered": 0,
                    "betonline_players_covered": 0,
                    "betonline_two_sided_propositions": 0,
                    "previous_betonline_rows": previous_counts.get(market["local_prop_type"], ""),
                    "notes": f"payload parse failed: {type(exc).__name__}",
                }
            )
        return rows, {"raw_status": "RAW_PRESENT_PARSE_FAILED", "error": str(exc)}

    counts: dict[tuple[str, str], int] = Counter()
    games: dict[tuple[str, str], set[str]] = defaultdict(set)
    players: dict[tuple[str, str], set[str]] = defaultdict(set)
    sides: dict[tuple[str, str, str, str, str], set[str]] = defaultdict(set)
    book_keys = set()
    for ev in payload_items(payload):
        event_id = str(ev.get("id") or "")
        for book in ev.get("bookmakers", []) or []:
            if not isinstance(book, dict):
                continue
            book_key = normalize_book(book.get("key") or book.get("title"))
            if book_key:
                book_keys.add(book_key)
            for market in book.get("markets", []) or []:
                if not isinstance(market, dict):
                    continue
                market_key = str(market.get("key") or "")
                prop = market_to_prop.get(market_key)
                if not prop:
                    continue
                for outcome in market.get("outcomes", []) or []:
                    if not isinstance(outcome, dict):
                        continue
                    side = str(outcome.get("name") or "").strip().lower()
                    if side not in {"over", "under"}:
                        continue
                    player = str(outcome.get("description") or "").strip().lower()
                    line = str(outcome.get("point") or "").strip()
                    counts[(book_key, prop)] += 1
                    games[(book_key, prop)].add(event_id)
                    if player:
                        players[(book_key, prop)].add(player)
                    sides[(book_key, prop, event_id, player, line)].add(side)

    rows = []
    betonline_present = TARGET_BOOK in book_keys
    for market in active_market_rows():
        prop = market["local_prop_type"]
        raw_key = market["oddsapi_key"]
        b_rows = counts[(TARGET_BOOK, prop)]
        f_rows = counts[(COMPARE_BOOK, prop)]
        two_sided = sum(1 for key, side_set in sides.items() if key[0] == TARGET_BOOK and key[1] == prop and {"over", "under"}.issubset(side_set))
        if b_rows > 0 and two_sided > 0:
            status = "MARKET_PRESENT"
        elif b_rows > 0:
            status = "MARKET_PARTIAL"
        elif betonline_present and f_rows > 0:
            status = "MARKET_ABSENT_EXPECTED"
        elif betonline_present:
            status = "STATUS_UNRESOLVED"
        elif f_rows > 0:
            status = "BOOK_ABSENT"
        else:
            status = "STATUS_UNRESOLVED"
        rows.append(
            {
                "prop_type": prop,
                "raw_market_key": raw_key,
                "status": status,
                "betonline_rows": b_rows,
                "fanduel_rows": f_rows,
                "betonline_games_covered": len(games[(TARGET_BOOK, prop)]),
                "betonline_players_covered": len(players[(TARGET_BOOK, prop)]),
                "betonline_two_sided_propositions": two_sided,
                "previous_betonline_rows": previous_counts.get(prop, ""),
                "notes": "semantic player-prop coverage check",
            }
        )
    meta = {
        "raw_status": raw_status,
        "source_path": str(path),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "capture_timestamp": captured_at(payload),
        "expected_run_time": expected_run_time,
        "bookmakers_returned": "|".join(sorted(book_keys)),
        "betonline_present": betonline_present,
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    return rows, meta


def summarize_status(rows: list[dict[str, Any]], meta: dict[str, Any]) -> dict[str, Any]:
    total_betonline = sum(_safe_int(r.get("betonline_rows")) for r in rows)
    total_fanduel = sum(_safe_int(r.get("fanduel_rows")) for r in rows)
    present = [r["prop_type"] for r in rows if r.get("status") in {"MARKET_PRESENT", "MARKET_PARTIAL"}]
    missing = [r["prop_type"] for r in rows if r.get("status") in {"MARKET_ABSENT_EXPECTED", "BOOK_ABSENT", "REQUEST_FAILED"}]
    partial = [r["prop_type"] for r in rows if r.get("status") == "MARKET_PARTIAL"]
    core_expected_absent = [r["prop_type"] for r in rows if r.get("prop_type") in CORE_MARKETS and r.get("status") in {"MARKET_ABSENT_EXPECTED", "BOOK_ABSENT"}]
    if meta.get("raw_status") == "RAW_PRESENT_PARSE_FAILED":
        overall = "REQUEST_FAILED"
    elif total_betonline > 0 and not core_expected_absent and not partial:
        overall = "BETONLINE_CAPTURE_SEMANTIC_PASS"
    elif total_betonline > 0:
        overall = "BETONLINE_CAPTURE_PARTIAL"
    elif meta.get("betonline_present"):
        overall = "BETONLINE_FEATURED_PRESENT_PLAYER_PROPS_ABSENT"
    elif total_fanduel > 0:
        overall = "BETONLINE_BOOK_ABSENT_OTHER_BOOKS_PRESENT"
    else:
        overall = "STATUS_UNRESOLVED"
    execution_auth = "AUTHORIZED_DIRECT_BETONLINE_ROWS_PRESENT" if total_betonline > 0 else "NOT_EXECUTABLE_MISSING_DIRECT_BETONLINE_PRICE"
    daily_class = "HEALTHY" if overall == "BETONLINE_CAPTURE_SEMANTIC_PASS" else "DEGRADED" if total_betonline > 0 else "FAILED" if total_fanduel > 0 or meta.get("betonline_present") else "UNRESOLVED"
    return {
        "overall_semantic_status": overall,
        "daily_classification": daily_class,
        "betonline_player_prop_rows": total_betonline,
        "fanduel_comparison_rows": total_fanduel,
        "markets_present": present,
        "markets_missing_or_partial": sorted(set(missing + partial)),
        "partial_markets": partial,
        "core_expected_absent_markets": core_expected_absent,
        "betonline_execution_authorization": execution_auth,
        "betonline_direct_price_required": True,
        "non_economic_processing_status": "MAY_CONTINUE_WITH_EXPLICIT_PROVENANCE",
        "line_only_proxy_status": "REMAINS_DISABLED_NON_EXECUTABLE",
    }


def write_run_status(
    *,
    rows: list[dict[str, Any]],
    meta: dict[str, Any],
    slate_date: str,
    run_tag: str,
    scheduled_window: str,
    output_root: Path,
    normalized_odds_path: str = "",
) -> dict[str, Any]:
    slate_date = _date_key(slate_date) or _date_key(meta.get("expected_run_time")) or _date_key(meta.get("capture_timestamp"))
    run_tag = run_tag or "unknown_run"
    scheduled_window = normalize_scheduled_window(scheduled_window)
    out_dir = output_root / slate_date / run_tag
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = summarize_status(rows, meta)
    payload = {
        "slate_date": slate_date,
        "scheduled_window": scheduled_window,
        "actual_capture_time": meta.get("capture_timestamp") or "",
        "run_tag": run_tag,
        "eligible_unstarted_games": "UNRESOLVED_FROM_RETAINED_ODDS_PAYLOAD",
        "bookmakers_present": meta.get("bookmakers_returned", ""),
        "betonline_featured_market_presence": bool(meta.get("betonline_present")),
        "raw_response_path": meta.get("source_path", ""),
        "raw_response_sha256": meta.get("source_sha256", ""),
        "normalized_odds_path": normalized_odds_path,
        "generated_at_utc": _utc_now_iso(),
        **summary,
        "markets": rows,
    }
    json_path = out_dir / f"betonline_capture_semantic_status_{slate_date}_{run_tag}.json"
    csv_path = out_dir / f"betonline_capture_semantic_markets_{slate_date}_{run_tag}.csv"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    write_csv(
        csv_path,
        rows,
        [
            "prop_type",
            "raw_market_key",
            "status",
            "betonline_rows",
            "fanduel_rows",
            "betonline_games_covered",
            "betonline_players_covered",
            "betonline_two_sided_propositions",
            "previous_betonline_rows",
            "notes",
        ],
    )
    return {**payload, "status_json": str(json_path), "market_csv": str(csv_path)}


def _load_run_payloads(root: Path, slate_date: str) -> list[dict[str, Any]]:
    out = []
    for path in sorted((root / slate_date).glob("*/betonline_capture_semantic_status_*.json")):
        try:
            obj = json.loads(path.read_text())
            if isinstance(obj, dict):
                obj["_source_path"] = str(path)
                out.append(obj)
        except Exception:
            continue
    return out


def _parse_utc_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _expected_window_dt(slate_date: str, utc_label: str) -> datetime | None:
    expected_time = f"{slate_date}T{utc_label.replace('Z', ':00Z')}"
    return _parse_utc_datetime(expected_time)


def _canonical_window_for_run(run: dict[str, Any], slate_date: str) -> str:
    recorded = str(run.get("scheduled_window") or "").strip()
    if recorded in _SCHEDULED_WINDOW_LABELS:
        if recorded in {pt for pt, _ in SCHEDULED_WINDOWS}:
            return recorded
        for pt, utc_label in SCHEDULED_WINDOWS:
            if recorded == utc_label:
                return pt
    run_dt = (
        _parse_utc_datetime(run.get("actual_capture_time"))
        or _parse_utc_datetime(run.get("expected_run_time"))
        or _parse_utc_datetime(run.get("generated_at_utc"))
    )
    if run_dt is None:
        return ""
    matches: list[tuple[float, str]] = []
    for pacific, utc_label in SCHEDULED_WINDOWS:
        expected_dt = _expected_window_dt(slate_date, utc_label)
        if expected_dt is None:
            continue
        delta_minutes = abs((run_dt - expected_dt).total_seconds()) / 60.0
        if delta_minutes <= WINDOW_BINDING_TOLERANCE_MINUTES:
            matches.append((delta_minutes, pacific))
    if len(matches) != 1:
        return ""
    return sorted(matches)[0][1]


def write_daily_summary(*, slate_date: str, output_root: Path) -> dict[str, Any]:
    slate_date = _date_key(slate_date)
    root = output_root / slate_date
    root.mkdir(parents=True, exist_ok=True)
    runs = _load_run_payloads(output_root, slate_date)
    by_window: dict[str, dict[str, Any]] = {}
    ambiguous_windows: set[str] = set()
    for run in runs:
        window = _canonical_window_for_run(run, slate_date)
        if not window:
            continue
        if window in by_window:
            existing_dt = _parse_utc_datetime(by_window[window].get("actual_capture_time"))
            run_dt = _parse_utc_datetime(run.get("actual_capture_time"))
            if existing_dt and run_dt and run_dt > existing_dt:
                ambiguous_windows.add(window)
                by_window[window] = run
            else:
                ambiguous_windows.add(window)
        else:
            by_window[window] = run
    now = datetime.now(timezone.utc)
    rows = []
    for pacific, utc_label in SCHEDULED_WINDOWS:
        expected_time = f"{slate_date}T{utc_label.replace('Z', ':00Z')}"
        try:
            expected_dt = datetime.fromisoformat(expected_time.replace("Z", "+00:00"))
        except Exception:
            expected_dt = now
        run = {} if pacific in ambiguous_windows else by_window.get(pacific) or {}
        if run:
            status = str(run.get("overall_semantic_status") or "STATUS_UNRESOLVED")
            executed = True
            alert = status if status != "BETONLINE_CAPTURE_SEMANTIC_PASS" else ""
        elif pacific in ambiguous_windows:
            status = "AMBIGUOUS_WINDOW_ASSIGNMENT"
            executed = False
            alert = "AMBIGUOUS_WINDOW_ASSIGNMENT"
        elif expected_dt > now:
            status = "PENDING_FUTURE_WINDOW"
            executed = False
            alert = ""
        else:
            status = "EXPECTED_CAPTURE_MISSING"
            executed = False
            alert = "EXPECTED_CAPTURE_MISSING"
        rows.append(
            {
                "slate_date": slate_date,
                "expected_pacific_time": pacific,
                "expected_utc_time": expected_time,
                "executed": executed,
                "actual_run_time": run.get("actual_capture_time", ""),
                "run_tag": run.get("run_tag", ""),
                "semantic_status": status,
                "daily_classification": run.get("daily_classification", "UNRESOLVED" if not run else ""),
                "betonline_rows": run.get("betonline_player_prop_rows", 0),
                "fanduel_rows": run.get("fanduel_comparison_rows", 0),
                "markets_present": "|".join(run.get("markets_present", []) or []),
                "missing_or_partial_markets": "|".join(run.get("markets_missing_or_partial", []) or []),
                "betonline_execution_authorization": run.get("betonline_execution_authorization", "NOT_EXECUTABLE_MISSING_DIRECT_BETONLINE_PRICE" if status in {"EXPECTED_CAPTURE_MISSING", "BETONLINE_BOOK_ABSENT_OTHER_BOOKS_PRESENT"} else ""),
                "alert": alert,
                "status_json": run.get("_source_path", ""),
            }
        )
    executed_rows = [r for r in rows if r["executed"]]
    successful_direct_rows = [r for r in executed_rows if _safe_int(r.get("betonline_rows")) > 0]
    last_successful_run = successful_direct_rows[-1].get("run_tag", "") if successful_direct_rows else ""
    failed_rows = [r for r in rows if r["semantic_status"] not in {"BETONLINE_CAPTURE_SEMANTIC_PASS", "PENDING_FUTURE_WINDOW"}]
    latest_direct = ""
    for r in reversed(executed_rows):
        if _safe_int(r.get("betonline_rows")) > 0:
            latest_direct = str(r.get("actual_run_time") or r.get("expected_utc_time") or "")
            break
    for r in rows:
        r["scheduler_identity"] = SCHEDULER_IDENTITY
        r["expected_naming_convention"] = (
            f"{output_root}/{slate_date}/<run_tag>/"
            f"betonline_capture_semantic_status_{slate_date}_<run_tag>.json"
        )
        r["last_successful_run"] = last_successful_run
        r["stdout_log_path"] = SCHEDULER_STDOUT_PATH
        r["stderr_log_path"] = SCHEDULER_STDERR_PATH
        r["operator_action"] = (
            "inspect scheduler logs and retained odds_history run-tagged artifacts"
            if r["semantic_status"] == "EXPECTED_CAPTURE_MISSING"
            else "none"
        )
    daily_class = "HEALTHY" if executed_rows and not failed_rows else "FAILED" if any(r["semantic_status"] in {"EXPECTED_CAPTURE_MISSING", "BETONLINE_BOOK_ABSENT_OTHER_BOOKS_PRESENT", "BETONLINE_FEATURED_PRESENT_PLAYER_PROPS_ABSENT"} for r in rows) else "DEGRADED" if failed_rows else "UNRESOLVED"
    payload = {
        "slate_date": slate_date,
        "generated_at_utc": _utc_now_iso(),
        "daily_classification": daily_class,
        "expected_windows": len(SCHEDULED_WINDOWS),
        "executed_windows": len(executed_rows),
        "missing_eligible_windows": sum(1 for r in rows if r["semantic_status"] == "EXPECTED_CAPTURE_MISSING"),
        "scheduler_identity": SCHEDULER_IDENTITY,
        "stdout_log_path": SCHEDULER_STDOUT_PATH,
        "stderr_log_path": SCHEDULER_STDERR_PATH,
        "last_successful_run": last_successful_run,
        "latest_direct_betonline_player_prop_capture": latest_direct,
        "current_outage_status": "DIRECT_BETONLINE_PLAYER_PROP_ROWS_PRESENT" if latest_direct else "NO_DIRECT_BETONLINE_PLAYER_PROP_ROWS_RETAINED",
        "betonline_execution_authorization": "AUTHORIZED_DIRECT_BETONLINE_ROWS_PRESENT" if latest_direct else "NOT_EXECUTABLE_MISSING_DIRECT_BETONLINE_PRICE",
        "windows": rows,
    }
    json_path = root / f"betonline_capture_integrity_daily_summary_{slate_date}.json"
    csv_path = root / f"betonline_capture_integrity_daily_summary_{slate_date}.csv"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    write_csv(csv_path, rows, list(rows[0].keys()) if rows else ["slate_date"])
    return {**payload, "summary_json": str(json_path), "summary_csv": str(csv_path)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--odds-json", type=Path, required=True)
    ap.add_argument("--expected-run-time", default="")
    ap.add_argument("--out-csv", type=Path, default=None)
    ap.add_argument("--out-json", type=Path, default=None)
    ap.add_argument("--slate-date", default="")
    ap.add_argument("--run-tag", default="")
    ap.add_argument("--scheduled-window", default="")
    ap.add_argument("--output-root", type=Path, default=None)
    ap.add_argument("--normalized-odds-path", default="")
    ap.add_argument("--write-run-status", action="store_true")
    ap.add_argument("--write-daily-summary", action="store_true")
    args = ap.parse_args()
    rows, meta = validate_payload(args.odds_json, expected_run_time=args.expected_run_time)
    run_status = None
    if args.write_run_status:
        output_root = args.output_root or Path("artifacts/analysis/mlb/betonline_capture_integrity")
        run_status = write_run_status(
            rows=rows,
            meta=meta,
            slate_date=args.slate_date,
            run_tag=args.run_tag,
            scheduled_window=args.scheduled_window,
            output_root=output_root,
            normalized_odds_path=args.normalized_odds_path,
        )
    daily_summary = None
    if args.write_daily_summary:
        output_root = args.output_root or Path("artifacts/analysis/mlb/betonline_capture_integrity")
        daily_summary = write_daily_summary(slate_date=args.slate_date or _date_key(args.expected_run_time), output_root=output_root)
    if args.out_csv:
        write_csv(
            args.out_csv,
            rows,
            [
                "prop_type",
                "raw_market_key",
                "status",
                "betonline_rows",
                "fanduel_rows",
                "betonline_games_covered",
                "betonline_players_covered",
                "betonline_two_sided_propositions",
                "previous_betonline_rows",
                "notes",
            ],
        )
    if args.out_json:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(json.dumps({"meta": meta, "rows": rows}, indent=2, sort_keys=True) + "\n")
    summary = summarize_status(rows, meta)
    print(
        "BetOnline semantic capture "
        f"status={summary['overall_semantic_status']} "
        f"betonline_rows={summary['betonline_player_prop_rows']} "
        f"markets_present={','.join(summary['markets_present']) or 'none'} "
        f"missing_or_partial={','.join(summary['markets_missing_or_partial']) or 'none'} "
        f"fanduel_rows={summary['fanduel_comparison_rows']} "
        f"execution={summary['betonline_execution_authorization']} "
        f"result={run_status.get('status_json') if run_status else args.out_json or 'stdout_only'}"
    )
    print(json.dumps({"meta": meta, "summary": summary, "rows": rows, "run_status": run_status, "daily_summary": daily_summary}, indent=2, sort_keys=True))
    return 0 if summary["overall_semantic_status"] == "BETONLINE_CAPTURE_SEMANTIC_PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
