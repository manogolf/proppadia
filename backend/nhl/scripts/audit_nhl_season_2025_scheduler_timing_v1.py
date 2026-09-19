#!/usr/bin/env python3
"""Build the no-network NHL season-2025 scheduler and odds-timing audit."""
from __future__ import annotations

import csv
import hashlib
import json
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/nhl_season_2025_scheduler_timing_audit_v1/2026-09-18"
PT = ZoneInfo("America/Los_Angeles")
ET = ZoneInfo("America/New_York")
UTC = timezone.utc
LOG_ROOT = ROOT / "nhl daily run logs"
ODDS_ROOT = ROOT / "backend/nhl/exports/odds_history"


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def iso(dt: datetime | None) -> str:
    return "" if dt is None else dt.isoformat()


def write_csv(name: str, rows: list[dict], fields: list[str] | None = None) -> None:
    path = OUT / name
    if fields is None:
        fields = list(rows[0]) if rows else []
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(name: str, payload: object) -> None:
    (OUT / name).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def market_family(key: str) -> str:
    return {
        "h2h": "FULL_GAME_MONEYLINE",
        "spreads": "PUCK_LINE",
        "player_shots_on_goal": "SOG",
        "player_shots_on_goal_alternate": "SOG",
        "player_points": "POINTS",
        "player_total_saves": "GOALIE_SAVES",
    }.get(key, f"OTHER:{key}")


def lead_band(minutes: float | None) -> str:
    if minutes is None:
        return "TIMING_UNKNOWN"
    if minutes < 0:
        return "POST_START"
    if minutes < 30:
        return "UNDER_30_MIN"
    if minutes < 60:
        return "30_TO_60_MIN"
    if minutes < 120:
        return "1_TO_2_HOURS"
    if minutes < 240:
        return "2_TO_4_HOURS"
    if minutes < 480:
        return "4_TO_8_HOURS"
    return "MORE_THAN_8_HOURS"


def local_logs() -> tuple[list[dict], dict]:
    rows: list[dict] = []
    by_date: dict[str, list[dict]] = defaultdict(list)
    pattern = re.compile(r"nhl_daily_(\d{4}-\d{2}-\d{2})_(\d{6})\.log$")
    for path in sorted(LOG_ROOT.glob("nhl_daily_*.log")):
        match = pattern.match(path.name)
        if not match:
            continue
        date_text, clock = match.groups()
        start_pt = datetime.strptime(date_text + clock, "%Y-%m-%d%H%M%S").replace(tzinfo=PT)
        text = path.read_text(errors="replace")
        attempt_values = [int(value) for value in re.findall(r"\[run\] attempt (\d+)/3", text)]
        attempt_count = max(attempt_values, default=0)
        success = "[run] cli.py completed successfully" in text
        pipeline_complete = "Daily pipeline complete" in text
        minute = start_pt.hour * 60 + start_pt.minute
        if abs(minute - 345) <= 2:
            window = "AUTOMATOR_0545"
            invocation_class = "INITIAL_SCHEDULED_INVOCATION"
            scheduled = start_pt.replace(hour=5, minute=45, second=0)
        elif abs(minute - 870) <= 2:
            window = "AUTOMATOR_1430"
            invocation_class = "INITIAL_SCHEDULED_INVOCATION"
            scheduled = start_pt.replace(hour=14, minute=30, second=0)
        else:
            window = "OUTSIDE_DOMINANT_WINDOWS"
            invocation_class = "MANUAL_RECOVERY_OR_TEST_UNRESOLVED"
            scheduled = None
        row = {
            "date": date_text,
            "season": 2025,
            "scheduler_type": "MACOS_AUTOMATOR_RETAINED_LOG",
            "population_count": 1,
            "scheduled_time_pt": iso(scheduled),
            "scheduled_time_utc": iso(scheduled.astimezone(UTC)) if scheduled else "",
            "actual_start_pt": iso(start_pt),
            "actual_start_utc": iso(start_pt.astimezone(UTC)),
            "actual_end_pt": "",
            "actual_end_utc": "",
            "duration_seconds": "",
            "run_tag": path.stem,
            "command_form": ".venv/bin/python -m backend.nhl.cli daily --with-odds",
            "exit_status": 0 if success else "NONZERO_OR_INCOMPLETE_EXACT_CODE_UNRETAINED",
            "attempt_count": attempt_count,
            "retry_number": max(attempt_count - 1, 0),
            "manual_automatic_classification": invocation_class,
            "prediction_phase": "UNKNOWN_NO_RETAINED_PHASE_CONTRACT",
            "market_capture_phase": "UNKNOWN_NO_RETAINED_PHASE_CONTRACT",
            "scheduler_window": window,
            "pipeline_complete_marker": pipeline_complete,
            "raw_snapshot_path": "",
            "raw_snapshot_sha256": "",
            "earliest_game_start_utc": "",
            "minutes_before_first_puck": "",
            "timing_qualification": "END_TIME_NOT_TIMESTAMPED_IN_LOG",
            "evidence_path": str(path.relative_to(ROOT)),
            "evidence_sha256": sha(path),
        }
        rows.append(row)
        by_date[date_text].append(row)

    # Dated odds_latest is mutable/overwritten. Bind it only to the final retained
    # invocation on that date, never to both scheduled invocations.
    for date_text, date_rows in by_date.items():
        odds = ODDS_ROOT / date_text / "odds_latest.json"
        if not odds.exists():
            continue
        target = max(date_rows, key=lambda item: item["actual_start_utc"])
        events = json.loads(odds.read_text())
        slate_events = [event for event in events if parse_time(event.get("commence_time")) and
                        parse_time(event["commence_time"]).astimezone(ET).date().isoformat() == date_text]
        starts = [parse_time(event["commence_time"]) for event in slate_events]
        latest_updates = []
        for event in slate_events:
            for book in event.get("bookmakers", []):
                for market in book.get("markets", []):
                    stamp = parse_time(market.get("last_update"))
                    if stamp:
                        latest_updates.append(stamp)
        earliest = min(starts) if starts else None
        proxy = max(latest_updates) if latest_updates else None
        target["raw_snapshot_path"] = str(odds.relative_to(ROOT))
        target["raw_snapshot_sha256"] = sha(odds)
        target["earliest_game_start_utc"] = iso(earliest)
        target["minutes_before_first_puck"] = (f"{(earliest - proxy).total_seconds()/60:.3f}"
                                                if earliest and proxy else "")
        target["timing_qualification"] = (
            "RAW_FILE_IS_FINAL_DAILY_OVERWRITE; RESPONSE_TIME_UNRETAINED; "
            "LEAD_USES_LATEST_PROVIDER_MARKET_UPDATE_AS_UPPER_BOUND"
        )

    starts = [parse_time(row["actual_start_pt"]) for row in rows]
    window_starts: dict[str, list[datetime]] = defaultdict(list)
    for row in rows:
        if row["scheduler_window"] != "OUTSIDE_DOMINANT_WINDOWS":
            window_starts[row["scheduler_window"]].append(parse_time(row["actual_start_pt"]))
    count_by_date = Counter(row["date"] for row in rows)
    counts_by_day = Counter(count_by_date.values())
    scheduled_count_by_date = Counter(row["date"] for row in rows
                                      if row["scheduler_window"] != "OUTSIDE_DOMINANT_WINDOWS")
    scheduled_days = Counter(scheduled_count_by_date.values())
    all_dates = {(min(starts).date()).fromordinal(value).isoformat()
                 for value in range(min(starts).date().toordinal(), max(starts).date().toordinal() + 1)}
    observed_dates = set(count_by_date)
    summary = {
        "rows": len(rows),
        "dates": len({row["date"] for row in rows}),
        "first_start_pt": iso(min(starts)),
        "last_start_pt": iso(max(starts)),
        "successes": sum(row["exit_status"] == 0 for row in rows),
        "failures_or_incomplete": sum(row["exit_status"] != 0 for row in rows),
        "full_pipeline_markers": sum(bool(row["pipeline_complete_marker"]) for row in rows),
        "window_counts": dict(Counter(row["scheduler_window"] for row in rows)),
        "days_by_all_invocation_count": {str(key): value for key, value in sorted(counts_by_day.items())},
        "dates_by_intended_window_count": {str(key): value for key, value in sorted(scheduled_days.items())},
        "dates_with_no_intended_window_but_other_invocation": sum(date not in scheduled_count_by_date for date in observed_dates),
        "missing_dates_in_observed_span": sorted(all_dates - observed_dates),
        "invocations_with_internal_retry": sum(int(row["attempt_count"]) > 1 for row in rows),
        "actual_start_clock_earliest_pt": min(value.strftime("%H:%M:%S") for value in starts),
        "actual_start_clock_latest_pt": max(value.strftime("%H:%M:%S") for value in starts),
        "window_medians_pt": {
            key: statistics.median([value.hour * 3600 + value.minute * 60 + value.second for value in values])
            for key, values in window_starts.items()
        },
        "end_time_status": "UNRETAINED",
    }
    summary["window_medians_pt"] = {
        key: f"{int(value)//3600:02d}:{(int(value)%3600)//60:02d}:{int(value)%60:02d}"
        for key, value in summary["window_medians_pt"].items()
    }
    return rows, summary


def odds_ledgers() -> tuple[list[dict], list[dict], dict]:
    bundle_rows: list[dict] = []
    band_counts: Counter[tuple[str, str, str]] = Counter()
    object_counts: Counter[tuple[str, str]] = Counter()
    for directory in sorted(path for path in ODDS_ROOT.iterdir() if path.is_dir()):
        date_text = directory.name
        wrappers = directory / "odds_event_wrappers.json"
        manifest = directory / "manifest.json"
        if wrappers.exists() and manifest.exists():
            wrapper_data = json.loads(wrappers.read_text())
            manifest_data = json.loads(manifest.read_text())
            snapshot = parse_time(manifest_data.get("snapshot_utc"))
            events = [wrapper.get("data", {}) for wrapper in wrapper_data]
            starts = [parse_time(event.get("commence_time")) for event in events if parse_time(event.get("commence_time"))]
            books, families, objects, game_ids = set(), set(), 0, set()
            for event in events:
                event_start = parse_time(event.get("commence_time"))
                if event.get("id"):
                    game_ids.add(event["id"])
                for book in event.get("bookmakers", []):
                    books.add(book.get("key", "UNKNOWN"))
                    for market in book.get("markets", []):
                        family = market_family(market.get("key", ""))
                        families.add(family)
                        objects += 1
                        stamp = parse_time(market.get("last_update")) or parse_time(wrapper_data[0].get("timestamp"))
                        minutes = ((event_start - stamp).total_seconds() / 60) if event_start and stamp else None
                        band_counts[("HISTORICAL_PROVIDER_AS_OF", family, lead_band(minutes))] += 1
                        object_counts[("HISTORICAL_PROVIDER_AS_OF", family)] += 1
            earliest = min(starts) if starts else None
            lead = (earliest - snapshot).total_seconds() / 60 if earliest and snapshot else None
            bundle_rows.append({
                "slate_date": date_text,
                "source_family": "HISTORICAL_ODDS_API_AS_OF_BUNDLE",
                "actual_response_capture_timestamp_utc": "",
                "provider_snapshot_as_of_utc": iso(snapshot),
                "provider_latest_market_update_utc": "",
                "sportsbook_count": len(books),
                "market_families": ";".join(sorted(families)),
                "games_covered": len(game_ids),
                "earliest_game_start_utc": iso(earliest),
                "minutes_before_first_puck": "" if lead is None else f"{lead:.3f}",
                "lead_time_band": lead_band(lead),
                "scheduler_correspondence": "NONE_BACKFILL_RETRIEVED_2026_03_05_TO_2026_03_09",
                "timing_classification": "PREGAME" if lead is not None and lead >= 0 else "POST_FIRST_PUCK_OR_UNKNOWN",
                "lead_time_semantics": "PROVIDER_HISTORICAL_AS_OF_NOT_LOCAL_RESPONSE_TIME",
                "raw_snapshot_path": str(wrappers.relative_to(ROOT)),
                "raw_snapshot_sha256": sha(wrappers),
            })
        latest = directory / "odds_latest.json"
        if latest.exists():
            events_all = json.loads(latest.read_text())
            events = [event for event in events_all if parse_time(event.get("commence_time")) and
                      parse_time(event["commence_time"]).astimezone(ET).date().isoformat() == date_text]
            starts = [parse_time(event["commence_time"]) for event in events]
            books, families, objects, game_ids, latest_updates = set(), set(), 0, set(), []
            for event in events:
                event_start = parse_time(event.get("commence_time"))
                if event.get("id"):
                    game_ids.add(event["id"])
                for book in event.get("bookmakers", []):
                    books.add(book.get("key", "UNKNOWN"))
                    for market in book.get("markets", []):
                        family = market_family(market.get("key", ""))
                        families.add(family)
                        objects += 1
                        stamp = parse_time(market.get("last_update"))
                        if stamp:
                            latest_updates.append(stamp)
                        minutes = ((event_start - stamp).total_seconds() / 60) if event_start and stamp else None
                        band_counts[("CONTEMPORANEOUS_FINAL_DAILY_FILE", family, lead_band(minutes))] += 1
                        object_counts[("CONTEMPORANEOUS_FINAL_DAILY_FILE", family)] += 1
            earliest = min(starts) if starts else None
            proxy = max(latest_updates) if latest_updates else None
            lead = (earliest - proxy).total_seconds() / 60 if earliest and proxy else None
            bundle_rows.append({
                "slate_date": date_text,
                "source_family": "CONTEMPORANEOUS_FINAL_DAILY_ODDS_FILE",
                "actual_response_capture_timestamp_utc": "",
                "provider_snapshot_as_of_utc": "",
                "provider_latest_market_update_utc": iso(proxy),
                "sportsbook_count": len(books),
                "market_families": ";".join(sorted(families)),
                "games_covered": len(game_ids),
                "earliest_game_start_utc": iso(earliest),
                "minutes_before_first_puck": "" if lead is None else f"{lead:.3f}",
                "lead_time_band": lead_band(lead),
                "scheduler_correspondence": "FINAL_RETAINED_INVOCATION_OF_DATE; USUALLY_1430_PT",
                "timing_classification": "PREGAME" if lead is not None and lead >= 0 else "POST_FIRST_PUCK_OR_UNKNOWN",
                "lead_time_semantics": "LATEST_PROVIDER_UPDATE_PROXY; RESPONSE_TIMESTAMP_UNRETAINED",
                "raw_snapshot_path": str(latest.relative_to(ROOT)),
                "raw_snapshot_sha256": sha(latest),
            })
    band_rows = [{"source_family": source, "market_family": family, "lead_time_band": band, "market_objects": count}
                 for (source, family, band), count in sorted(band_counts.items())]
    positive_bundle_leads: dict[str, list[float]] = defaultdict(list)
    for row in bundle_rows:
        if row["minutes_before_first_puck"] != "" and float(row["minutes_before_first_puck"]) >= 0:
            positive_bundle_leads[row["source_family"]].append(float(row["minutes_before_first_puck"]))
    summary = {
        "historical_bundle_rows": sum(row["source_family"].startswith("HISTORICAL") for row in bundle_rows),
        "contemporaneous_capture_rows": sum(row["source_family"].startswith("CONTEMPORANEOUS") for row in bundle_rows),
        "market_object_counts": {f"{source}|{family}": count for (source, family), count in sorted(object_counts.items())},
        "mainline_historical_objects": sum(count for (source, family), count in object_counts.items() if family == "FULL_GAME_MONEYLINE"),
        "puck_line_historical_objects": sum(count for (source, family), count in object_counts.items() if family == "PUCK_LINE"),
        "closest_nonnegative_bundle_lead_minutes_by_source": {
            source: min(values) for source, values in sorted(positive_bundle_leads.items())
        },
    }
    return bundle_rows, band_rows, summary


def scheduler_inventory() -> list[dict]:
    return [
        {"mechanism":"GITHUB_ACTIONS","identity":".github/workflows/nhl-daily-refresh.yml @ 70ccd17b","active_range":"2025-09-23..2025-10-03 (run evidence begins 2025-09-27)","configured_timezone":"UTC","scheduled_times":"10:15 UTC = 03:15 PDT","command":"direct Python/psql daily workflow","odds":"revision-dependent; no local step artifacts","predictions":"YES","automation":"AUTOMATIC_PLUS_WORKFLOW_DISPATCH","retry":"workflow helper revision-dependent","evidence":"git history + prior public run metadata","strength":"STRONG"},
        {"mechanism":"GITHUB_ACTIONS","identity":".github/workflows/nhl-daily-refresh.yml @ 5a80166e","active_range":"2025-10-04..2025-10-23","configured_timezone":"UTC","scheduled_times":"11:15 UTC = 04:15 PDT; 05:15 UTC = 22:15 PDT previous local day","command":"direct Python/psql daily workflow","odds":"player-prop fetch in workflow revisions","predictions":"YES","automation":"AUTOMATIC_PLUS_WORKFLOW_DISPATCH","retry":"bounded retry helper; concurrency cancel-in-progress","evidence":"git history + prior public run metadata","strength":"STRONG"},
        {"mechanism":"GITHUB_ACTIONS","identity":".github/workflows/nhl-daily-refresh.yml @ 3d6d919d and successors","active_range":"2025-10-24..last run 2025-11-03; file later removed 2026-02-22","configured_timezone":"UTC","scheduled_times":"12:30,20:30,00:45,07:30 UTC; through 2025-11-01 PDT = 05:30,13:30,17:45 previous local day,00:30; from 2025-11-02 PST = 04:30,12:30,16:45 previous local day,23:30 previous local day","command":"direct Python/psql; fetch-odds/build market outputs","odds":"YES_PLAYER_PROPS_ONLY","predictions":"YES","automation":"AUTOMATIC_PLUS_WORKFLOW_DISPATCH","retry":"guard/retry helpers; concurrency group nhl-daily cancel-in-progress=true","evidence":"git history + 186-run prior public metadata aggregate","strength":"STRONG"},
        {"mechanism":"MACOS_AUTOMATOR_CALENDAR","identity":"wrapper definition missing; 270 retained child logs","active_range":"2025-12-09..2026-04-16","configured_timezone":"America/Los_Angeles inferred from local log headers","scheduled_times":"05:45 and 14:30 PT dominant; exact Calendar/Automator definitions not retained","command":".venv/bin/python -m backend.nhl.cli daily --with-odds","odds":"YES_PLAYER_PROPS_ONLY","predictions":"YES_SOG_POINTS_SAVES","automation":"202 dominant-window logs automatic; 68 other invocations unresolved manual/recovery/test","retry":"up to 3 attempts inside one invocation","evidence":"270 logs on 124 dates","strength":"STRONG_EXECUTION_INFERENCE; CONFIG_MISSING"},
        {"mechanism":"HISTORICAL_LAUNCHAGENT","identity":"none found for season-2025 NHL daily runner","active_range":"none proven","configured_timezone":"local if it existed","scheduled_times":"none proven","command":"none","odds":"NO_PROOF","predictions":"NO_PROOF","automation":"SEARCHED_NOT_FOUND","retry":"n/a","evidence":"installed/repository plist search","strength":"NEGATIVE_SEARCH"},
        {"mechanism":"CRON","identity":"user cron unreadable in sandbox; retained reconciliation says root cron empty on 2026-09-02","active_range":"season-2025 state not recoverable","configured_timezone":"unknown","scheduled_times":"none proven","command":"none proven","odds":"NO_PROOF","predictions":"NO_PROOF","automation":"UNKNOWN","retry":"unknown","evidence":"retained audit + current sandbox denial","strength":"LIMITED"},
        {"mechanism":"MANUAL_ALIAS","identity":"bin/nhl_ops.sh","active_range":"repository history/current","configured_timezone":"n/a","scheduled_times":"none","command":".venv/bin/python -m backend.nhl.cli daily --with-odds","odds":"YES","predictions":"YES","automation":"MANUAL_NOT_SCHEDULER","retry":"none in alias","evidence":"tracked script","strength":"STRONG"},
        {"mechanism":"LOGIN_ITEM_OR_SHORTCUT","identity":"none relevant found","active_range":"none proven","configured_timezone":"n/a","scheduled_times":"none","command":"none","odds":"NO_PROOF","predictions":"NO_PROOF","automation":"SEARCHED_NOT_FOUND","retry":"n/a","evidence":"login defaults and discoverable filesystem search","strength":"NEGATIVE_SEARCH"},
        {"mechanism":"UNRELATED_WEB_APP","identity":"/Users/jerrystrain/Applications/% to Odds.app","active_range":"current","configured_timezone":"n/a","scheduled_times":"none","command":"Safari web app to static image URL","odds":"NO","predictions":"NO","automation":"NOT_AUTOMATOR_AND_NOT_RELEVANT","retry":"n/a","evidence":"Info.plist","strength":"STRONG"},
    ]


def current_snapshot() -> dict:
    return {
        "as_of_local_date": "2026-09-18",
        "morning": {
            "label": "com.proppadia.nhl.morning-orchestration", "loaded": True, "enabled": True,
            "state": "not running", "last_exit_code": 0, "launchctl_runs": 1,
            "schedule": "07:30 local daily", "run_at_load": False, "keep_alive": False,
            "program": ".venv/bin/python backend/nhl/scripts/run_nhl_morning_orchestration.py --slate-date today --env-file backend/.env",
            "lock": "per-date orchestration/run identity", "odds_capture": False,
            "template_matches_installed": True,
            "plist_sha256": "c50d2c746c2fe74a695cbd20e70c2ff08972ff9bb44c7319d6a8d862d27c39cb",
        },
        "conditional_shadow": {
            "label": "com.proppadia.nhl.mainline-cross-market-shadow", "loaded": True,
            "enabled": True, "state": "not running", "last_exit_code": 0, "launchctl_runs": 110,
            "schedule": "StartInterval=900 seconds from load/last dispatch; RunAtLoad=true",
            "phase_gate": "MIDDAY during 12:00-12:30 PT; FINAL_PREGAME when first future start is 20-75 minutes away",
            "program": ".venv/bin/python backend/nhl/scripts/run_nhl_mainline_cross_market_capture_warn_only.py --slate-date today --phase AUTO --env-file backend/.env",
            "lock": "per-slate acquisition flock + create-only paid-attempt claim; prior attempt blocks automatic retry",
            "template_matches_installed": True,
            "plist_sha256": "9cb03c63d20fe50cbc5dfcc3c50e9cb62a5a1c5445e08923ded59fd9b40cf2da",
        },
        "mlb_collision": {"08:30_refresh": True, "07:30_exact_collision": False,
                          "noon_or_final_conditional_overlap": "possible with other MLB windows depending first puck; isolated locks/outputs, shared host/network/database"},
        "power": {"sleep": 0, "displaysleep": 0, "repeating_wake": "05:27 every day",
                  "requirement": "LaunchAgent calendar intervals do not wake sleeping Mac; current AC sleep=0 keeps host awake"},
        "important_correction": "07:30 is the only fixed NHL calendar run, but it is not the only automatic NHL activity; the loaded 900-second conditional poll can make one guarded MIDDAY and one guarded FINAL_PREGAME capture.",
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    invocation_rows, local_summary = local_logs()
    invocation_rows.insert(0, {
        "date":"2025-09-27..2025-11-03","season":2025,"scheduler_type":"GITHUB_ACTIONS_AGGREGATE",
        "population_count":186,"scheduled_time_pt":"MULTIPLE_BY_WORKFLOW_REVISION","scheduled_time_utc":"MULTIPLE_BY_WORKFLOW_REVISION",
        "actual_start_pt":"ROW_LEVEL_METADATA_NOT_RETAINED_LOCALLY","actual_start_utc":"2025-09-27T02:19:49Z..2025-11-03T07:36:47Z",
        "actual_end_pt":"","actual_end_utc":"","duration_seconds":"","run_tag":"PRIOR_PUBLIC_METADATA_AGGREGATE",
        "command_form":"GitHub workflow direct Python/psql daily pipeline","exit_status":"80_SUCCESS_105_FAILURE_1_CANCELLED",
        "attempt_count":"","retry_number":"","manual_automatic_classification":"86_SCHEDULED_100_WORKFLOW_DISPATCH",
        "prediction_phase":"WORKFLOW_REVISION_DEPENDENT","market_capture_phase":"WORKFLOW_REVISION_DEPENDENT_PLAYER_PROPS",
        "scheduler_window":"SEE_SCHEDULER_INVENTORY","pipeline_complete_marker":"","raw_snapshot_path":"","raw_snapshot_sha256":"",
        "earliest_game_start_utc":"","minutes_before_first_puck":"","timing_qualification":"EXACT_PER_RUN_TIMESTAMPS_NOT_PRESERVED_IN_LOCAL_EVIDENCE",
        "evidence_path":"artifacts/analysis/model_development/nhl_season_2025_returning_prop_operational_lineage_recovery_v1/2026-09-08/nhl_launchagent_job_inventory_2026-09-08.csv",
        "evidence_sha256":sha(ROOT/"artifacts/analysis/model_development/nhl_season_2025_returning_prop_operational_lineage_recovery_v1/2026-09-08/nhl_launchagent_job_inventory_2026-09-08.csv")})
    odds_rows, band_rows, odds_summary = odds_ledgers()
    inventory = scheduler_inventory()

    histogram = []
    for (window, clock), count in sorted(Counter((row["scheduler_window"], parse_time(row["actual_start_pt"]).strftime("%H:%M"))
                                                  for row in invocation_rows[1:]).items()):
        histogram.append({"scheduler":"AUTOMATOR","window":window,"actual_start_hhmm_pt":clock,"invocations":count})
    histogram.append({"scheduler":"GITHUB_ACTIONS","window":"ACTUAL_ROW_LEVEL_TIMES_UNAVAILABLE","actual_start_hhmm_pt":"UNKNOWN","invocations":186})

    phase_rows = [
        {"term":"MORNING","season_2025_status":"Not embedded in retained local logs; GitHub comment called 12:30 UTC Morning seed","season_2026_status":"Actual 07:30 prerequisite phase","conclusion":"PROVEN_CURRENT; PARTIAL_HISTORICAL"},
        {"term":"MIDDAY","season_2025_status":"GitHub comment called 20:30 UTC Midday refresh; Automator 14:30 phase contract absent","season_2026_status":"Formal phase; AUTO gate is local 12:00-12:30","conclusion":"HISTORICAL_GITHUB_COMMENT_AND_CURRENT_CONTRACT; NOT_PROVEN_AUTOMATOR_LABEL"},
        {"term":"FINAL_PREGAME","season_2025_status":"No exact season-2025 artifact phase label found","season_2026_status":"Formal new phase; AUTO gate is 20-75 minutes before first future start","conclusion":"SEASON_2026_REHEARSAL_AND_RUNTIME_LABEL"},
        {"term":"PRE_PUCK","season_2025_status":"GitHub workflow comment named 00:45 UTC Pre-puck check","season_2026_status":"Not the current formal run_type","conclusion":"PROVEN_HISTORICAL_GITHUB_COMMENT_ONLY"},
        {"term":"FINAL_SWEEP","season_2025_status":"GitHub workflow comment named 07:30 UTC PST final sweep; local-date relationship varies with DST","season_2026_status":"Not a formal capture phase","conclusion":"PROVEN_HISTORICAL_GITHUB_COMMENT_ONLY"},
    ]
    current = current_snapshot()
    proposal_rows = [
        {"window":"07:30 PT fixed","purpose":"Morning schedule/history/roster prerequisites and immutable prediction inputs","incremental_coverage":"construction, not market coverage","lanes":"Moneyline,SOG,Points,Saves prerequisites","prediction_policy":"construct/freeze governed prediction inputs","market_refresh":"NO","latest_safe_completion":"before first conditional market capture","mlb_risk":"No exact 08:30 collision; observe real-slate duration","credit_implication":"0 Odds API credits"},
        {"window":"12:00-12:30 PT conditional (existing poll)","purpose":"MIDDAY market capture","incremental_coverage":"not quantitatively proven versus morning because paired raw snapshots were not retained","lanes":"Moneyline/SOG/Points; Saves conditional P/M shadow","prediction_policy":"do not mutate morning source identity; phase creates immutable shadow artifacts","market_refresh":"YES once, guarded","latest_safe_completion":"well before 16:00 first puck","mlb_risk":"Potential 13:00 MLB proximity only if delayed; isolated locks/outputs but shared host/network/DB","credit_implication":"one governed live request/phase; actual credits per provider contract"},
        {"window":"14:45-15:40 PT dynamic for 16:00 first puck (existing 20-75 minute gate)","purpose":"FINAL_PREGAME market refresh","incremental_coverage":"historical 14:30 final files prove useful player-prop availability; incremental benefit over midday is not directly measurable","lanes":"Moneyline/SOG/Points; Saves conditional P/M shadow","prediction_policy":"new immutable phase identity; no overwrite of MIDDAY","market_refresh":"YES once, guarded","latest_safe_completion":"before 16:00; fail closed once no prestart game remains","mlb_risk":"Avoid/serialize any long MLB work; 16:30 MLB is after first puck and not a valid fallback","credit_implication":"one governed live request/phase; prior durable attempt blocks automatic retry"},
        {"window":"Recovery only","purpose":"operator-reviewed recovery after durable failed attempt","incremental_coverage":"none assumed","lanes":"only authorized lanes","prediction_policy":"no silent retry or phase replacement","market_refresh":"only explicit authorized override","latest_safe_completion":"must remain pregame","mlb_risk":"choose after inspecting locks","credit_implication":"may consume another request; never automatic"},
    ]

    write_csv("scheduler_inventory.csv", inventory)
    write_csv("invocation_ledger.csv", invocation_rows)
    write_csv("historical_time_histogram.csv", histogram)
    write_csv("odds_capture_lead_time_ledger.csv", odds_rows)
    write_csv("market_family_lead_time_histogram.csv", band_rows)
    write_csv("phase_reconciliation.csv", phase_rows)
    write_json("current_season_2026_schedule_snapshot.json", current)
    write_csv("proposed_cadence_comparison.csv", proposal_rows)

    summary = {
        "task_id":"NHL_SEASON_2025_SCHEDULER_TIMING_AUDIT_V1",
        "season_convention":"season 2025 includes January-April 2026",
        "github":{"runs":186,"scheduled":86,"manual_dispatch":100,"success":80,"failure":105,"cancelled":1,
                  "first_start_utc":"2025-09-27T02:19:49Z","last_start_utc":"2025-11-03T07:36:47Z",
                  "row_level_actual_timing":"NOT_RETAINED_LOCALLY"},
        "automator":local_summary,
        "odds":odds_summary,
        "transition":{"last_github_run_utc":"2025-11-03T07:36:47Z","first_automator_start_pt":"2025-12-09T14:30:04-08:00",
                      "gap":"No automatic NHL invocation evidence retained for 2025-11-04 through 2025-12-08"},
        "historical_windows":{"github":"configuration changed: 03:15 PDT; then 04:15/22:15 PDT; then four UTC windows with DST-dependent PT",
                              "automator":"05:45 and 14:30 PT dominant; 202 scheduled-window logs plus 68 other invocations"},
        "phase_conclusion":"MIDDAY existed as a GitHub comment and is a formal 2026 gate; FINAL_PREGAME is a season-2026 formal phase, not a proven season-2025 Automator label.",
        "coverage_conclusion":"153 historical bundles are 16:00 PT provider as-of backfills, not contemporaneous executions. Forty-two contemporaneous final daily files preserve mainly the later local run; early-versus-late paired coverage is unavailable.",
        "recommendation":"NO_NEW_SCHEDULE_REQUIRED: retain 07:30 morning plus the existing guarded 900-second AUTO poll for one noon MIDDAY and one 20-75-minute FINAL_PREGAME capture.",
        "current":current,
        "uncertainties":["GitHub per-run metadata was observed previously but raw response was not retained locally",
                         "Automator/Calendar source definition and exact end timestamps are missing",
                         "42 contemporaneous odds files are mutable final daily copies, so paired 05:45 versus 14:30 coverage cannot be measured",
                         "goalie confirmation and lineup improvement by time are not directly preserved"],
    }
    write_json("summary.json", summary)

    report = f"""# NHL season-2025 scheduler timing audit

Audit date: 2026-09-18 PT
Mode: retained evidence only; no network/API request and no NHL pipeline execution

## Direct answer

Season 2025 did not have one stable clock. GitHub Actions changed from **03:15 PDT** daily, to **04:15 and 22:15 PDT**, then to four fixed UTC cron expressions (`12:30`, `20:30`, `00:45`, `07:30`) whose PT meanings changed at DST. The later local era has two strongly proven automatic clusters: **05:45 PT (106 logs)** and **14:30 PT (96 logs)**. The other 68 local invocations are manual/recovery/test unresolved; they are not additional intended windows.

The 186 GitHub runs comprise 86 scheduled and 100 manual dispatches (80 success, 105 failure, one cancelled) from 2025-09-27T02:19:49Z through 2025-11-03T07:36:47Z. Exact per-run GitHub timestamps were not preserved in the local audit evidence, so this package does not fabricate them. The Automator population is exact: {local_summary['rows']} logs on {local_summary['dates']} dates, {local_summary['successes']} success markers, {local_summary['failures_or_incomplete']} non-success/incomplete logs, and {local_summary['full_pipeline_markers']} full-pipeline markers. Log bodies do not timestamp completion, so exact ends and durations are unavailable.

Across the local era, 34 dates had one intended-window invocation and 84 had both; {local_summary['dates_with_no_intended_window_but_other_invocation']} observed dates had only an off-window invocation. Internal wrapper retries occurred in {local_summary['invocations_with_internal_retry']} invocation logs and remain part of those invocations, not extra daily windows. The five dates with no retained invocation at all were {', '.join(local_summary['missing_dates_in_observed_span'])}. Median starts were {local_summary['window_medians_pt']['AUTOMATOR_0545']} and {local_summary['window_medians_pt']['AUTOMATOR_1430']} PT.

## Scheduler eras

1. GitHub `70ccd17b` (from 2025-09-23): `10:15 UTC` = `03:15 PDT`.
2. GitHub `5a80166e` (2025-10-04 through 2025-10-23): `11:15 UTC` = `04:15 PDT`; `05:15 UTC` = `22:15 PDT` on the prior local date.
3. GitHub `3d6d919d` (from 2025-10-24; last observed run 2025-11-03): `12:30/20:30/00:45/07:30 UTC`. Before the 2025-11-02 DST transition those map to `05:30/13:30/17:45/00:30 PDT` with local-date caveats; afterward they map to `04:30/12:30/16:45/23:30 PST`, again with prior-local-day mapping for 00:45 and 07:30 UTC.
4. No retained invocation evidence spans 2025-11-04 through 2025-12-08.
5. Local Automator evidence begins 2025-12-09 14:30:04 PST and ends 2026-04-16 14:30:01 PDT. Its exact workflow/Calendar definition is missing, but execution headers and filename clusters prove the two intended local windows.

No season-2025 NHL LaunchAgent was found. The `% to Odds.app` application is an unrelated Safari web app, not Automator. `bin/nhl_ops.sh` is a manual alias, not a scheduler. Current user cron could not be read in this sandbox; retained root-cron verification was empty on 2026-09-02, but that current fact is not projected backward.

## Odds timing and coverage

The archive contains exactly {odds_summary['historical_bundle_rows']} historical provider-as-of bundles and {odds_summary['contemporaneous_capture_rows']} contemporaneous final daily files. The historical bundles all use a 16:00 PT as-of timestamp but were retrieved March 5-9, 2026; they are independent provider-time evidence, not proof a local 16:00 scheduler ran. The contemporaneous files are final overwrite-capable daily copies and usually bind to the last 14:30 run. Exact HTTP response timestamps are absent; the ledger labels provider market timestamps as proxies rather than local capture times.

The retained historical archive has zero full-game Moneyline and zero puck-line objects; it is player-prop evidence. SOG, Points, and Saves are separated in the lead-time histogram. The only paired historical operational comparison available is weak: later final files survive, while the 05:45 raw state was overwritten. Therefore the audit cannot prove that later runs materially improved Points, SOG, Saves, goalie confirmation, or lineup availability. It can prove that the later 14:30 run produced usable player-prop files, including April 16's pregame Points/SOG/Saves coverage.

At bundle level, the closest nonnegative historical as-of snapshot was {odds_summary['closest_nonnegative_bundle_lead_minutes_by_source']['HISTORICAL_ODDS_API_AS_OF_BUNDLE']:.3f} minutes before first puck. The closest contemporaneous final-file proxy was {odds_summary['closest_nonnegative_bundle_lead_minutes_by_source']['CONTEMPORANEOUS_FINAL_DAILY_ODDS_FILE']:.3f} minutes. Neither is an exact local HTTP completion time: the former is a provider historical as-of and the latter uses the latest provider market update, so the latter is only an upper bound on response lead time.

## Phase terminology

- `MIDDAY` is a comment in the final GitHub cron era and a formal current season-2026 phase. It is not proven as the name of the 14:30 Automator invocation.
- `FINAL_PREGAME` is a current 2026 rehearsal/runtime label. No retained season-2025 Automator artifact applies that label.
- `PRE_PUCK` is a historical GitHub workflow comment for the `00:45 UTC` cron.
- Phase labels are therefore not inferred solely from the local clock.

## Current season-2026 scheduler

`com.proppadia.nhl.morning-orchestration` is loaded/enabled at 07:30 local, runs prerequisites only, and matches its repository plist byte-for-byte. It is the only fixed NHL calendar time. It is **not** the only NHL automation: loaded `com.proppadia.nhl.mainline-cross-market-shadow` runs every 900 seconds with `RunAtLoad`, selects `MIDDAY` only at 12:00-12:30 PT and `FINAL_PREGAME` when the first future puck is 20-75 minutes away, and uses a per-slate acquisition lock plus durable create-only paid-attempt claim. A prior attempt blocks automatic retry.

MLB's fixed 08:30 refresh does not exactly overlap the 07:30 NHL start. Real-slate duration still needs observation because both use the host, network, Python environment, and PostgreSQL. Outputs and locks are isolated. AC settings currently show system sleep disabled (`sleep=0`) and a repeating 05:27 wake; calendar LaunchAgents do not themselves wake a sleeping Mac.

## Minimal September 19 recommendation (first puck 16:00 PT)

**Do not install another schedule.** Retain the existing 07:30 prerequisite run and existing conditional poll:

- 07:30: construct/certify morning prerequisites and governed prediction inputs; no odds credit.
- One MIDDAY acquisition during 12:00-12:30: immutable phase artifacts; Moneyline and Points remain prediction/market shadow, SOG remains authorized prediction/market/candidate-lineage shadow without upload/execution, Saves remains conditional prediction/market shadow only.
- One FINAL_PREGAME acquisition between 14:45 and 15:40 for a 16:00 puck: a new immutable phase identity, not an overwrite or model-policy change.
- Recovery only after operator review of the durable attempt claim; never automatic replacement of a possibly charged request.

This reproduces the demonstrated two-part operational shape with the fewest paid windows while using current atomic duplicate protection. Expected incremental coverage is **unquantified**, because the historical early raw snapshot was not retained. The 16:30 MLB run is after first puck and cannot serve as a fallback capture slot.

## Evidence boundary

See `scheduler_inventory.csv`, `invocation_ledger.csv`, `historical_time_histogram.csv`, `odds_capture_lead_time_ledger.csv`, `market_family_lead_time_histogram.csv`, `phase_reconciliation.csv`, `current_season_2026_schedule_snapshot.json`, and `proposed_cadence_comparison.csv`. `summary.json` is the machine-readable decision record. No schedule, pipeline, database, model, capture, or publication state was changed.
"""
    (OUT / "report.md").write_text(report)
    manifest_lines = []
    for path in sorted(item for item in OUT.iterdir() if item.is_file() and item.name != "SHA256SUMS"):
        manifest_lines.append(f"{sha(path)}  {path.name}")
    (OUT / "SHA256SUMS").write_text("\n".join(manifest_lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
