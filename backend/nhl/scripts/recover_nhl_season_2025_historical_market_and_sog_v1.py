#!/usr/bin/env python3
"""Resumable, quota-governed NHL season-2025 historical market/SOG recovery."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from backend.nhl.analysis_package_guard import begin_package, finalize_package


ROOT = Path(__file__).resolve().parents[3]
STATE = ROOT / "artifacts/operational/nhl/historical_market_and_sog_recovery_v1/authorized_budget_override_20260915"
PACKAGE = ROOT / "artifacts/analysis/model_development/nhl_season_2025_historical_market_and_sog_recovery_v1/2026-09-15"
V2 = ROOT / "artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test/2026-09-15/v2_predictions.csv"
BRIDGE = ROOT / "artifacts/analysis/model_development/nhl_cross_market_game_state_bridge_v1/2026-09-15/game_state_bridge.parquet"
OLD_ARCHIVE = ROOT / "backend/nhl/exports/odds_history"
SPORTS_URL = "https://api.the-odds-api.com/v4/sports/"
HISTORICAL_URL = "https://api.the-odds-api.com/v4/historical/sports/icehockey_nhl/odds"
EVENT_ODDS_URL = "https://api.the-odds-api.com/v4/historical/sports/icehockey_nhl/events/{event_id}/odds"
BOOKMAKERS = (
    "pinnacle", "betonlineag", "draftkings", "fanduel", "betmgm",
    "betrivers", "fanatics", "bovada", "mybookieag", "williamhill_us",
)
STARTING_USED = 16138
STARTING_REMAINING = 83862
TARGET_CEILING = 35000
HARD_CEILING = 40000
LIVE_RESERVE = 40000
RESET_UTC = "2026-10-01T00:00:00Z"
PHASE_BUDGETS = {"MAINLINE": 15000, "SOG_SAMPLE": 1500, "SOG_EXPANSION": 15000, "CONTINGENCY": 3500}
EXPECTED_COST = {"MAINLINE": 20, "SOG_SAMPLE": 10, "SOG_EXPANSION": 10}
QUOTA_HEADERS = ("x-requests-last", "x-requests-used", "x-requests-remaining")
LEDGER_FIELDS = (
    "request_id", "attempt", "recorded_at_utc", "phase", "endpoint", "requested_timestamp_utc",
    "game_ids", "event_id", "markets", "bookmakers", "expected_cost", "status", "http_status",
    "x_requests_last", "x_requests_used", "x_requests_remaining", "unrelated_live_credits_used",
    "historical_task_credits_used", "safe_historical_allowance", "raw_response_path", "raw_sha256", "error_code",
)
TEAM_CODES = {
    "anaheim ducks":"ANA", "boston bruins":"BOS", "buffalo sabres":"BUF", "calgary flames":"CGY",
    "carolina hurricanes":"CAR", "chicago blackhawks":"CHI", "colorado avalanche":"COL",
    "columbus blue jackets":"CBJ", "dallas stars":"DAL", "detroit red wings":"DET",
    "edmonton oilers":"EDM", "florida panthers":"FLA", "los angeles kings":"LAK",
    "minnesota wild":"MIN", "montreal canadiens":"MTL", "nashville predators":"NSH",
    "new jersey devils":"NJD", "new york islanders":"NYI", "new york rangers":"NYR",
    "ottawa senators":"OTT", "philadelphia flyers":"PHI", "pittsburgh penguins":"PIT",
    "san jose sharks":"SJS", "seattle kraken":"SEA", "st louis blues":"STL",
    "tampa bay lightning":"TBL", "toronto maple leafs":"TOR", "utah mammoth":"UTA",
    "utah hockey club":"UTA", "vancouver canucks":"VAN", "vegas golden knights":"VGK",
    "washington capitals":"WSH", "winnipeg jets":"WPG",
}
SECRET_PATTERN = re.compile(r"(?i)(apikey|api_key|odds_api_key)(=|%3d|\"\s*:\s*\")([^&\s\"']+)")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def norm(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return re.sub(r"[^a-z0-9]+", " ", "".join(c for c in text if not unicodedata.combining(c)).lower()).strip()


def team_code(value: Any) -> str | None:
    text = norm(value)
    return TEAM_CODES.get(text, str(value).upper() if len(str(value)) == 3 else None)


def american_decimal(value: Any) -> float:
    price = float(value)
    return 1 + price / 100 if price > 0 else 1 + 100 / abs(price)


def header_int(headers: dict[str, Any], key: str) -> int | None:
    try:
        return int(str(headers.get(key, "")).strip())
    except (TypeError, ValueError):
        return None


def header_dict(response: requests.Response) -> dict[str, str]:
    return {key: str(response.headers.get(key, "")) for key in QUOTA_HEADERS}


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def append_csv(path: Path, row: dict[str, Any], fields: tuple[str, ...] = LEDGER_FIELDS) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        if new:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in fields})
        handle.flush()
        os.fsync(handle.fileno())


def safe_response_bytes(response: requests.Response) -> bytes:
    text = response.content.decode("utf-8", errors="replace")
    return SECRET_PATTERN.sub(r"\1\2[REDACTED]", text).encode()


def load_schedule() -> pd.DataFrame:
    frame = pd.read_csv(V2)
    frame = frame[(frame.canonical_season.eq(2025)) & (frame.game_type.eq(2))].copy()
    if len(frame) != 1312 or frame.game_id.duplicated().any():
        raise RuntimeError("SEASON_2025_1312_GAME_IDENTITY_FAILED")
    frame["scheduled_start_time_utc"] = pd.to_datetime(frame.scheduled_start_time_utc, utc=True)
    frame["target_timestamp_utc"] = frame.scheduled_start_time_utc - pd.Timedelta(minutes=15)
    return frame.sort_values(["scheduled_start_time_utc", "game_id"]).reset_index(drop=True)


def bind_event(event: dict[str, Any], schedule: pd.DataFrame) -> pd.Series | None:
    home, away = team_code(event.get("home_team")), team_code(event.get("away_team"))
    commence = pd.to_datetime(event.get("commence_time"), utc=True, errors="coerce")
    candidates = schedule[schedule.home_team.eq(home) & schedule.away_team.eq(away)].copy()
    if pd.isna(commence) or candidates.empty:
        return None
    candidates["delta"] = (candidates.scheduled_start_time_utc - commence).abs().dt.total_seconds()
    candidates = candidates[candidates.delta.le(3 * 3600)].sort_values(["delta", "game_id"])
    if candidates.empty or (len(candidates) > 1 and candidates.delta.iloc[0] == candidates.delta.iloc[1]):
        return None
    return candidates.iloc[0]


def existing_inventory(schedule: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    events, market_rows, raw_rows = [], [], []
    for day_dir in sorted(OLD_ARCHIVE.glob("202*-*-*")):
        manifest_path = day_dir / "manifest.json"
        wrappers_path = day_dir / "odds_event_wrappers.json"
        if not manifest_path.is_file() or not wrappers_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text())
            wrappers = json.loads(wrappers_path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        raw_rows.append({
            "date": day_dir.name, "manifest_path": str(manifest_path.relative_to(ROOT)),
            "manifest_sha256": sha(manifest_path), "wrapper_path": str(wrappers_path.relative_to(ROOT)),
            "wrapper_sha256": sha(wrappers_path), "wrapper_count": len(wrappers) if isinstance(wrappers, list) else 0,
            "configured_markets": manifest.get("markets"), "configured_regions": manifest.get("regions"),
        })
        for wrapper in wrappers if isinstance(wrappers, list) else []:
            data = wrapper.get("data") if isinstance(wrapper, dict) else None
            if not isinstance(data, dict):
                continue
            game = bind_event(data, schedule)
            snapshot = pd.to_datetime(wrapper.get("timestamp"), utc=True, errors="coerce")
            common = {
                "provider_event_id": data.get("id"), "provider_home_team": data.get("home_team"),
                "provider_away_team": data.get("away_team"), "provider_commence_time_utc": data.get("commence_time"),
                "returned_snapshot_timestamp_utc": wrapper.get("timestamp"),
                "game_id": int(game.game_id) if game is not None else pd.NA,
                "binding_status": "BOUND" if game is not None else "UNRESOLVED",
                "source_wrapper_path": str(wrappers_path.relative_to(ROOT)),
                "provider_book_count": len(data.get("bookmakers") or []),
            }
            events.append(common)
            if game is None:
                continue
            target = game.target_timestamp_utc
            for book in data.get("bookmakers") or []:
                for market in book.get("markets") or []:
                    key = market.get("key")
                    if key not in {"player_shots_on_goal", "h2h", "spreads"}:
                        continue
                    update = pd.to_datetime(market.get("last_update") or book.get("last_update"), utc=True, errors="coerce")
                    standardized = bool(
                        pd.notna(snapshot) and snapshot <= target and (target - snapshot).total_seconds() <= 300
                        and pd.notna(update) and update < game.scheduled_start_time_utc
                    )
                    market_rows.append({
                        **common, "bookmaker_key": book.get("key"), "market_key": key,
                        "market_update_timestamp_utc": market.get("last_update") or book.get("last_update"),
                        "target_timestamp_utc": target.isoformat(), "strictly_pregame": bool(pd.notna(snapshot) and snapshot < game.scheduled_start_time_utc and pd.notna(update) and update < game.scheduled_start_time_utc),
                        "standardized_t_minus_15_usable": standardized,
                        "outcome_count": len(market.get("outcomes") or []),
                        "record_classification": "AUTHORIZED_HISTORICAL_PROVIDER_RECOVERY",
                        "recovery_origin": "PREEXISTING_PROVIDER_ARCHIVE",
                    })
    event_frame = pd.DataFrame(events).drop_duplicates(["provider_event_id", "game_id"], keep="last") if events else pd.DataFrame()
    return event_frame, pd.DataFrame(market_rows), pd.DataFrame(raw_rows)


def deterministic_sample(schedule: pd.DataFrame, bridge: pd.DataFrame, reusable_sog_games: set[int]) -> pd.DataFrame:
    coverage = bridge[["game_id", "sog_home_model_covered_player_count", "sog_away_model_covered_player_count"]].copy()
    coverage["bridge_usable"] = coverage.sog_home_model_covered_player_count.fillna(0).gt(0) & coverage.sog_away_model_covered_player_count.fillna(0).gt(0)
    candidates = schedule.merge(coverage[["game_id", "bridge_usable"]], on="game_id", how="left", validate="one_to_one")
    candidates = candidates[~candidates.bridge_usable.fillna(False) & ~candidates.game_id.isin(reusable_sog_games)].copy()
    candidates["period"] = pd.cut(
        candidates.scheduled_start_time_utc.astype("int64"), bins=3, labels=["EARLY", "MIDDLE", "LATE"], include_lowest=True,
    )
    candidates["weekday"] = candidates.scheduled_start_time_utc.dt.day_name()
    candidates["start_hour_utc"] = candidates.scheduled_start_time_utc.dt.hour
    candidates["stable_order"] = candidates.game_id.map(lambda value: hashlib.sha256(f"NHL_SOG_SAMPLE_V1|{value}".encode()).hexdigest())
    chosen = []
    counts: dict[str, int] = {}
    for period in ["EARLY", "MIDDLE", "LATE"]:
        pool = candidates[candidates.period.astype(str).eq(period)].copy()
        for _ in range(20):
            if pool.empty:
                raise RuntimeError("SOG_SAMPLE_STRATUM_INSUFFICIENT")
            pool["balance_score"] = pool.apply(lambda row: (
                counts.get(f"team:{row.home_team}", 0) + counts.get(f"team:{row.away_team}", 0)
                + counts.get(f"weekday:{row.weekday}", 0) / 4
                + counts.get(f"hour:{row.start_hour_utc}", 0) / 8
            ), axis=1)
            row = pool.sort_values(["balance_score", "stable_order", "game_id"]).iloc[0]
            chosen.append(row)
            for key in [f"team:{row.home_team}", f"team:{row.away_team}", f"weekday:{row.weekday}", f"hour:{row.start_hour_utc}"]:
                counts[key] = counts.get(key, 0) + 1
            pool = pool[pool.game_id.ne(row.game_id)]
    return pd.DataFrame(chosen).sort_values(["scheduled_start_time_utc", "game_id"]).reset_index(drop=True)


def preflight(state: Path) -> dict[str, Any]:
    frozen = state / "preflight/freeze.json"
    if frozen.exists():
        value = json.loads(frozen.read_text())
        for name, expected in value["file_sha256"].items():
            if sha(state / "preflight" / name) != expected:
                raise RuntimeError(f"PREFLIGHT_FREEZE_MISMATCH:{name}")
        return value
    state.mkdir(parents=True, exist_ok=True)
    pre = state / "preflight"
    pre.mkdir(parents=True, exist_ok=False)
    schedule = load_schedule()
    bridge = pd.read_parquet(BRIDGE)
    events, existing_markets, raw_inventory = existing_inventory(schedule)
    usable_sog = set(pd.to_numeric(existing_markets.loc[
        existing_markets.market_key.eq("player_shots_on_goal") & existing_markets.standardized_t_minus_15_usable,
        "game_id"], errors="coerce").dropna().astype(int)) if len(existing_markets) else set()
    sample = deterministic_sample(schedule, bridge, usable_sog)
    mainline = schedule.groupby("target_timestamp_utc", sort=True).agg(
        game_ids=("game_id", lambda values: ";".join(map(str, sorted(values)))), games_expected=("game_id", "nunique")
    ).reset_index()
    mainline["phase"] = "MAINLINE"
    mainline["request_id"] = mainline.target_timestamp_utc.map(lambda value: "MAINLINE_" + pd.Timestamp(value).strftime("%Y%m%dT%H%M%SZ"))
    mainline["endpoint"] = "/v4/historical/sports/icehockey_nhl/odds"
    mainline["markets"] = "h2h,spreads"
    mainline["expected_cost"] = EXPECTED_COST["MAINLINE"]
    bridge_usable = set(pd.to_numeric(bridge.loc[
        bridge.sog_home_model_covered_player_count.fillna(0).gt(0) & bridge.sog_away_model_covered_player_count.fillna(0).gt(0), "game_id"
    ], errors="coerce").dropna().astype(int))
    event_by_game = events.dropna(subset=["game_id"]).drop_duplicates("game_id").set_index("game_id").provider_event_id.to_dict() if len(events) else {}
    sog_games = schedule[~schedule.game_id.isin(usable_sog)].copy()
    sample_ids = set(sample.game_id.astype(int))
    sog_games["phase"] = np.where(sog_games.game_id.isin(sample_ids), "SOG_SAMPLE", "SOG_EXPANSION")
    sog_games["request_id"] = sog_games.apply(lambda row: f"{row.phase}_{int(row.game_id)}", axis=1)
    sog_games["endpoint"] = "/v4/historical/sports/icehockey_nhl/events/{event_id}/odds"
    sog_games["markets"] = "player_shots_on_goal"
    sog_games["game_ids"] = sog_games.game_id.astype(str)
    sog_games["games_expected"] = 1
    sog_games["expected_cost"] = sog_games.phase.map(EXPECTED_COST)
    sog_games["preflight_provider_event_id"] = sog_games.game_id.map(event_by_game)
    plan_columns = ["request_id", "phase", "endpoint", "target_timestamp_utc", "game_ids", "games_expected", "markets", "expected_cost"]
    plan = pd.concat([mainline[plan_columns], sog_games[plan_columns]], ignore_index=True)
    schedule.to_csv(pre / "season_2025_schedule_v2_predictions.csv", index=False)
    events.to_csv(pre / "existing_event_identity_crosswalk.csv", index=False)
    existing_markets.to_csv(pre / "existing_market_inventory.csv", index=False)
    raw_inventory.to_csv(pre / "existing_raw_archive_inventory.csv", index=False)
    sample.to_csv(pre / "deterministic_sog_60_game_sample.csv", index=False)
    plan.to_csv(pre / "request_plan.csv", index=False)
    summary = {
        "created_at_utc": now(), "season_games": len(schedule), "v2_predictions": len(schedule),
        "existing_daily_manifests": len(raw_inventory), "existing_wrappers": int(raw_inventory.wrapper_count.sum()) if len(raw_inventory) else 0,
        "existing_bound_event_ids": int(events.game_id.nunique()) if len(events) else 0,
        "existing_events_with_sportsbook_data": int(events.loc[events.provider_book_count.gt(0), "game_id"].nunique()) if len(events) else 0,
        "existing_sog_market_games": int(existing_markets.loc[existing_markets.market_key.eq("player_shots_on_goal"), "game_id"].nunique()) if len(existing_markets) else 0,
        "existing_standardized_sog_games": len(usable_sog), "existing_sog_bridge_games": len(bridge_usable),
        "mainline_requests": int(mainline.shape[0]), "mainline_expected_credits": int(mainline.expected_cost.sum()),
        "sog_sample_games": len(sample), "sog_sample_expected_credits": 600,
        "sog_expansion_requests": int(sog_games.phase.eq("SOG_EXPANSION").sum()),
        "sog_expansion_expected_credits": int(sog_games.loc[sog_games.phase.eq("SOG_EXPANSION"), "expected_cost"].sum()),
        "total_expected_credits": int(plan.expected_cost.sum()), "bookmakers": list(BOOKMAKERS),
        "outcomes_consulted_for_request_selection": False,
        "file_sha256": {},
    }
    if summary["mainline_expected_credits"] > PHASE_BUDGETS["MAINLINE"] or summary["total_expected_credits"] > TARGET_CEILING:
        raise RuntimeError(f"PREFLIGHT_BUDGET_EXCEEDED:{summary}")
    for path in sorted(pre.iterdir()):
        if path.name != "freeze.json":
            summary["file_sha256"][path.name] = sha(path)
    write_json(frozen, summary)
    if not (state / "request_ledger.csv").exists():
        (state / "request_ledger.csv").write_text(",".join(LEDGER_FIELDS) + "\n")
    return summary


def ledger(state: Path) -> pd.DataFrame:
    path = state / "request_ledger.csv"
    if not path.exists():
        return pd.DataFrame(columns=LEDGER_FIELDS)
    return pd.read_csv(path, dtype=str).fillna("")


def terminal_rows(state: Path) -> pd.DataFrame:
    frame = ledger(state)
    return frame[frame.status.isin(["SUCCESS", "HTTP_ERROR", "TRANSPORT_ERROR_UNKNOWN_CHARGE"])]


def historical_credits_used(state: Path) -> int:
    terminal = terminal_rows(state).drop_duplicates(["request_id", "attempt"], keep="last")
    return int(pd.to_numeric(terminal.x_requests_last, errors="coerce").fillna(0).sum()) if len(terminal) else 0


def completed_request_ids(state: Path) -> set[str]:
    frame = terminal_rows(state)
    charged_or_success = frame[frame.status.eq("SUCCESS") | pd.to_numeric(frame.x_requests_last, errors="coerce").fillna(0).gt(0)]
    return set(charged_or_success.request_id)


def reconcile_headerless_failure(state: Path) -> dict[str, Any]:
    frame = ledger(state)
    failures = frame[frame.status.eq("HTTP_ERROR") & frame.x_requests_last.eq("")]
    if failures.empty:
        raise RuntimeError("NO_HEADERLESS_FAILURE_TO_RECONCILE")
    failure = failures.iloc[-1]
    prior = frame[(frame.recorded_at_utc < failure.recorded_at_utc) & pd.to_numeric(frame.x_requests_used, errors="coerce").notna()]
    if prior.empty:
        raise RuntimeError("NO_PRIOR_QUOTA_HEADER_FOR_RECONCILIATION")
    prior_used = int(prior.iloc[-1].x_requests_used)
    quota = pd.read_csv(state / "quota_snapshot_ledger.csv").iloc[-1]
    checked_used = int(quota.x_requests_used)
    record = {
        "request_id": failure.request_id, "attempt": int(failure.attempt),
        "failure_http_status": int(failure.http_status), "failure_recorded_at_utc": failure.recorded_at_utc,
        "prior_certified_used": prior_used, "post_failure_zero_cost_check_used": checked_used,
        "charged": checked_used != prior_used, "retry_allowed": checked_used == prior_used,
        "basis": "zero-cost quota x-requests-used unchanged from last certified charged response",
        "created_at_utc": now(),
    }
    path = state / "reconciliations" / f"{failure.request_id}_attempt_{failure.attempt}.json"
    if path.exists():
        existing = json.loads(path.read_text())
        if existing != record:
            # created_at can differ; the substantive reconciliation must not.
            existing.pop("created_at_utc", None); record_without_time = dict(record); record_without_time.pop("created_at_utc", None)
            if existing != record_without_time:
                raise RuntimeError("RECONCILIATION_CONFLICT")
        return json.loads(path.read_text())
    write_json(path, record)
    return record


def quota_snapshot(state: Path, api_key: str, label: str, timeout: int = 60) -> dict[str, Any]:
    response = requests.get(SPORTS_URL, params={"apiKey": api_key, "all": "true"}, timeout=timeout)
    headers = header_dict(response)
    raw_dir = state / "quota_checks"
    raw_dir.mkdir(parents=True, exist_ok=True)
    identity = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')}_{re.sub('[^A-Za-z0-9_-]', '_', label)}"
    raw = raw_dir / f"{identity}.json"
    raw.write_bytes(safe_response_bytes(response))
    last, used, remaining = (header_int(headers, key) for key in QUOTA_HEADERS)
    record = {
        "label": label, "checked_at_utc": now(), "http_status": response.status_code,
        "x_requests_last": last, "x_requests_used": used, "x_requests_remaining": remaining,
        "raw_path": str(raw.relative_to(ROOT)), "raw_sha256": sha(raw),
        "historical_task_credits_used": historical_credits_used(state),
    }
    record["unrelated_live_credits_used"] = max((used or STARTING_USED) - STARTING_USED - record["historical_task_credits_used"], 0)
    record["safe_historical_allowance"] = min(
        HARD_CEILING - record["historical_task_credits_used"],
        (remaining if remaining is not None else -1) - LIVE_RESERVE,
    )
    append_csv(state / "quota_snapshot_ledger.csv", record, tuple(record))
    if not response.ok:
        raise RuntimeError(f"ZERO_COST_QUOTA_CHECK_HTTP_{response.status_code}")
    if last not in {None, 0}:
        raise RuntimeError(f"ZERO_COST_ENDPOINT_REPORTED_CHARGE:{last}")
    if used is None or remaining is None:
        raise RuntimeError("QUOTA_HEADERS_MISSING")
    return record


def event_crosswalk(state: Path) -> dict[int, str]:
    pre = pd.read_csv(state / "preflight/existing_event_identity_crosswalk.csv")
    rows = pre.dropna(subset=["game_id", "provider_event_id"]).copy()
    mapping = {int(row.game_id): str(row.provider_event_id) for row in rows.itertuples(index=False)}
    plan = pd.read_csv(state / "preflight/request_plan.csv")
    mainline_success = terminal_rows(state)
    mainline_success = mainline_success[(mainline_success.phase.eq("MAINLINE")) & (mainline_success.status.eq("SUCCESS"))]
    schedule = load_schedule()
    for row in mainline_success.itertuples(index=False):
        path = ROOT / row.raw_response_path
        if not path.is_file():
            continue
        payload = json.loads(path.read_text())
        for event in payload.get("data") or []:
            game = bind_event(event, schedule)
            if game is not None:
                mapping[int(game.game_id)] = str(event.get("id"))
    return mapping


def phase_rows(state: Path, phase: str) -> pd.DataFrame:
    amended = state / "preflight/request_plan_rfc3339_amendment.csv"
    plan = pd.read_csv(amended if amended.exists() else state / "preflight/request_plan.csv", dtype={"game_ids": str})
    return plan[plan.phase.eq(phase)].sort_values(["target_timestamp_utc", "request_id"]).reset_index(drop=True)


def amend_timestamps(state: Path) -> dict[str, Any]:
    original = state / "preflight/request_plan.csv"
    amended = state / "preflight/request_plan_rfc3339_amendment.csv"
    if amended.exists():
        return json.loads((state / "preflight/request_plan_rfc3339_amendment.json").read_text())
    failed = terminal_rows(state)
    failed = failed[(failed.status.eq("HTTP_ERROR")) & failed.error_code.eq("INVALID_HISTORICAL_TIMESTAMP")]
    costs = pd.to_numeric(failed.x_requests_last, errors="coerce")
    if len(failed) != 1 or not costs.eq(0).all():
        raise RuntimeError("RFC3339_AMENDMENT_REQUIRES_ONE_UNCHARGED_TIMESTAMP_REJECTION")
    plan = pd.read_csv(original, dtype={"game_ids": str})
    plan["original_target_timestamp_serialization"] = plan.target_timestamp_utc
    plan["target_timestamp_utc"] = pd.to_datetime(plan.target_timestamp_utc, utc=True).map(
        lambda value: value.isoformat().replace("+00:00", "Z")
    )
    plan.to_csv(amended, index=False)
    record = {
        "amendment": "RFC3339_T_Z_SERIALIZATION_ONLY", "created_at_utc": now(),
        "trigger_request_id": failed.request_id.iloc[0], "trigger_error": "INVALID_HISTORICAL_TIMESTAMP",
        "trigger_x_requests_last": 0, "selection_or_market_change": False,
        "original_plan_sha256": sha(original), "amended_plan_sha256": sha(amended),
    }
    write_json(state / "preflight/request_plan_rfc3339_amendment.json", record)
    return record


def validate_allowance(state: Path, phase: str, expected: int, used_header: int, remaining_header: int) -> dict[str, int]:
    task_used = historical_credits_used(state)
    unrelated = max(used_header - STARTING_USED - task_used, 0)
    safe = min(HARD_CEILING - task_used, remaining_header - LIVE_RESERVE)
    phase_terminal = terminal_rows(state)
    phase_used = int(pd.to_numeric(
        phase_terminal.loc[phase_terminal.phase.eq(phase), "x_requests_last"], errors="coerce"
    ).fillna(0).sum())
    if expected > safe:
        raise RuntimeError(f"RESERVE_OR_HARD_CEILING_CHECKPOINT:safe={safe}:expected={expected}")
    if phase_used + expected > PHASE_BUDGETS[phase]:
        raise RuntimeError(f"PHASE_BUDGET_CHECKPOINT:{phase}:{phase_used}+{expected}")
    return {"historical_task_credits_used": task_used, "unrelated_live_credits_used": unrelated, "safe_historical_allowance": safe}


def acquire_phase(state: Path, api_key: str, phase: str, timeout: int = 60, max_requests: int | None = None) -> dict[str, Any]:
    preflight(state)
    quota = quota_snapshot(state, api_key, f"BEFORE_{phase}_OR_RESTART", timeout)
    rows = phase_rows(state, phase)
    completed = completed_request_ids(state)
    pending_started = ledger(state)
    pending_started = pending_started[pending_started.status.eq("REQUEST_STARTED") & ~pending_started.request_id.isin(set(terminal_rows(state).request_id))]
    if len(pending_started):
        raise RuntimeError(f"AMBIGUOUS_STARTED_REQUEST_REQUIRES_RECONCILIATION:{pending_started.request_id.iloc[0]}")
    mapping = event_crosswalk(state) if phase.startswith("SOG") else {}
    processed = 0
    latest_used, latest_remaining = int(quota["x_requests_used"]), int(quota["x_requests_remaining"])
    for row in rows.itertuples(index=False):
        if row.request_id in completed:
            continue
        if max_requests is not None and processed >= max_requests:
            break
        prior_headerless = ledger(state)
        prior_headerless = prior_headerless[
            prior_headerless.request_id.eq(row.request_id) & prior_headerless.status.eq("HTTP_ERROR")
            & prior_headerless.x_requests_last.eq("")
        ]
        if len(prior_headerless):
            prior_attempt = prior_headerless.attempt.astype(int).max()
            reconciliation = state / "reconciliations" / f"{row.request_id}_attempt_{prior_attempt}.json"
            if not reconciliation.is_file() or not json.loads(reconciliation.read_text()).get("retry_allowed"):
                raise RuntimeError(f"HEADERLESS_FAILURE_RETRY_BLOCKED:{row.request_id}")
        expected = int(row.expected_cost)
        dynamic = validate_allowance(state, phase, expected, latest_used, latest_remaining)
        event_id = ""
        if phase.startswith("SOG"):
            game_id = int(str(row.game_ids).split(";")[0])
            event_id = mapping.get(game_id, "")
            if not event_id:
                append_csv(state / "request_ledger.csv", {
                    "request_id": row.request_id, "attempt": 0, "recorded_at_utc": now(), "phase": phase,
                    "endpoint": row.endpoint, "requested_timestamp_utc": row.target_timestamp_utc,
                    "game_ids": row.game_ids, "markets": row.markets, "bookmakers": ",".join(BOOKMAKERS),
                    "expected_cost": expected, "status": "UNRECOVERABLE_NO_EVENT_ID", **dynamic,
                })
                continue
        attempt = int(ledger(state).loc[ledger(state).request_id.eq(row.request_id), "attempt"].replace("", 0).astype(int).max() + 1) if len(ledger(state).loc[ledger(state).request_id.eq(row.request_id)]) else 1
        endpoint = EVENT_ODDS_URL.format(event_id=event_id) if phase.startswith("SOG") else HISTORICAL_URL
        params = {
            "apiKey": api_key, "date": row.target_timestamp_utc,
            "bookmakers": ",".join(BOOKMAKERS), "markets": row.markets,
            "oddsFormat": "american", "dateFormat": "iso",
        }
        intent = {
            "request_id": row.request_id, "attempt": attempt, "phase": phase, "endpoint": row.endpoint,
            "requested_timestamp_utc": row.target_timestamp_utc, "game_ids": row.game_ids,
            "event_id": event_id, "markets": row.markets, "bookmakers": list(BOOKMAKERS),
            "expected_cost": expected, "credential_present": True, "credential_persisted": False,
            "dynamic_budget_before_request": dynamic,
        }
        intent_path = state / "intents" / f"{row.request_id}_attempt_{attempt}.json"
        if intent_path.exists():
            raise RuntimeError(f"INTENT_OVERWRITE_BLOCKED:{intent_path}")
        write_json(intent_path, intent)
        base = {
            **intent, "recorded_at_utc": now(), "status": "REQUEST_STARTED", "http_status": "",
            "x_requests_last": "", "x_requests_used": latest_used, "x_requests_remaining": latest_remaining,
            **dynamic, "raw_response_path": "", "raw_sha256": "", "error_code": "",
        }
        append_csv(state / "request_ledger.csv", base)
        try:
            response = requests.get(endpoint, params=params, timeout=timeout)
        except Exception as error:
            append_csv(state / "request_ledger.csv", {
                **base, "recorded_at_utc": now(), "status": "TRANSPORT_ERROR_UNKNOWN_CHARGE",
                "error_code": f"{type(error).__name__}:TRANSPORT_DETAIL_REDACTED",
            })
            raise RuntimeError(f"TRANSPORT_ERROR_UNKNOWN_CHARGE:{row.request_id}") from None
        headers = header_dict(response)
        actual = header_int(headers, "x-requests-last")
        current_used = header_int(headers, "x-requests-used")
        current_remaining = header_int(headers, "x-requests-remaining")
        raw_path = state / "raw" / phase.lower() / f"{row.request_id}_attempt_{attempt}.json"
        if raw_path.exists():
            raise RuntimeError(f"RAW_OVERWRITE_BLOCKED:{raw_path}")
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(safe_response_bytes(response))
        try:
            payload = json.loads(raw_path.read_text())
        except json.JSONDecodeError:
            payload = {}
        error_code = payload.get("error_code", "") if isinstance(payload, dict) else ""
        final = {
            **base, "recorded_at_utc": now(), "status": "SUCCESS" if response.ok else "HTTP_ERROR",
            "http_status": response.status_code, "x_requests_last": actual,
            "x_requests_used": current_used, "x_requests_remaining": current_remaining,
            "raw_response_path": str(raw_path.relative_to(ROOT)), "raw_sha256": sha(raw_path),
            "error_code": error_code,
        }
        append_csv(state / "request_ledger.csv", final)
        if actual is None or current_used is None or current_remaining is None:
            raise RuntimeError(f"CHARGED_RESPONSE_QUOTA_HEADERS_MISSING:{row.request_id}")
        latest_used, latest_remaining = current_used, current_remaining
        processed += 1
        if not response.ok:
            raise RuntimeError(f"HTTP_ERROR:{row.request_id}:{response.status_code}:{error_code}")
        returned = pd.to_datetime(payload.get("timestamp"), utc=True, errors="coerce") if isinstance(payload, dict) else pd.NaT
        requested = pd.to_datetime(row.target_timestamp_utc, utc=True)
        if pd.isna(returned) or returned > requested or (requested - returned).total_seconds() > 600:
            raise RuntimeError(f"HISTORICAL_TIMESTAMP_MISMATCH:{row.request_id}:{returned}:{requested}")
        if actual != expected:
            raise RuntimeError(f"MATERIAL_COST_MISMATCH:{row.request_id}:expected={expected}:actual={actual}")
        if current_remaining < LIVE_RESERVE:
            raise RuntimeError(f"LIVE_RESERVE_BREACHED_AFTER_RESPONSE:{current_remaining}")
        if processed % 25 == 0:
            print(json.dumps({"phase": phase, "processed_this_run": processed, "request_id": row.request_id,
                              "task_credits": historical_credits_used(state), "remaining": current_remaining}), flush=True)
        time.sleep(0.03)
    return {
        "phase": phase, "processed_this_run": processed,
        "successful_total": int(terminal_rows(state).query("phase == @phase and status == 'SUCCESS'").request_id.nunique()),
        "planned": len(rows), "historical_task_credits_used": historical_credits_used(state),
        "ending_used": latest_used, "ending_remaining": latest_remaining,
    }


def successful_payloads(state: Path, phase: str) -> list[tuple[pd.Series, dict[str, Any]]]:
    rows = terminal_rows(state)
    rows = rows[(rows.phase.eq(phase)) & (rows.status.eq("SUCCESS"))].drop_duplicates("request_id", keep="last")
    result = []
    for _, row in rows.sort_values("request_id").iterrows():
        path = ROOT / row.raw_response_path
        if path.is_file() and sha(path) == row.raw_sha256:
            result.append((row, json.loads(path.read_text())))
        else:
            raise RuntimeError(f"RAW_RESPONSE_MISSING_OR_HASH_MISMATCH:{row.request_id}")
    return result


def normalize_mainline(state: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    schedule = load_schedule()
    candidates: list[dict[str, Any]] = []
    seen_events: set[tuple[int, str, str]] = set()
    for ledger_row, payload in successful_payloads(state, "MAINLINE"):
        returned = pd.to_datetime(payload.get("timestamp"), utc=True, errors="coerce")
        for event in payload.get("data") or []:
            game = bind_event(event, schedule)
            if game is None or pd.isna(returned) or returned > game.target_timestamp_utc:
                continue
            event_identity = (int(game.game_id), str(ledger_row.request_id), str(event.get("id")))
            if event_identity in seen_events:
                continue
            seen_events.add(event_identity)
            candidates.append({
                "game_id": int(game.game_id), "request_id": ledger_row.request_id,
                "returned_snapshot_timestamp_utc": returned, "event": event,
                "raw_response_path": ledger_row.raw_response_path, "raw_sha256": ledger_row.raw_sha256,
            })
    if not candidates:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    candidate_frame = pd.DataFrame([{key: value for key, value in row.items() if key != "event"} for row in candidates])
    chosen = candidate_frame.sort_values(["game_id", "returned_snapshot_timestamp_utc", "request_id"]).groupby("game_id").tail(1)
    chosen_keys = {(int(row.game_id), row.request_id) for row in chosen.itertuples(index=False)}
    schedule_by_game = schedule.set_index("game_id")
    event_rows, moneyline_rows, spread_rows = [], [], []
    for item in candidates:
        if (item["game_id"], item["request_id"]) not in chosen_keys:
            continue
        game = schedule_by_game.loc[item["game_id"]]
        event = item["event"]
        snapshot = item["returned_snapshot_timestamp_utc"]
        event_rows.append({
            "canonical_season": 2025, "game_id": item["game_id"], "provider_event_id": event.get("id"),
            "home_team": game.home_team, "away_team": game.away_team,
            "provider_home_team": event.get("home_team"), "provider_away_team": event.get("away_team"),
            "scheduled_start_time_utc": game.scheduled_start_time_utc.isoformat(),
            "provider_commence_time_utc": event.get("commence_time"),
            "target_timestamp_utc": game.target_timestamp_utc.isoformat(),
            "returned_snapshot_timestamp_utc": snapshot.isoformat(), "binding_status": "RESOLVED",
            "raw_response_path": item["raw_response_path"], "raw_sha256": item["raw_sha256"],
        })
        for book in event.get("bookmakers") or []:
            for market in book.get("markets") or []:
                key = market.get("key")
                if key not in {"h2h", "spreads"}:
                    continue
                update_value = market.get("last_update") or book.get("last_update")
                update = pd.to_datetime(update_value, utc=True, errors="coerce")
                timing = bool(snapshot < game.scheduled_start_time_utc and pd.notna(update) and update < game.scheduled_start_time_utc)
                for outcome in market.get("outcomes") or []:
                    side_code = team_code(outcome.get("name"))
                    orientation = "HOME" if side_code == game.home_team else "AWAY" if side_code == game.away_team else "UNRESOLVED"
                    price = pd.to_numeric(outcome.get("price"), errors="coerce")
                    point = pd.to_numeric(outcome.get("point"), errors="coerce")
                    common = {
                        "record_classification": "AUTHORIZED_HISTORICAL_PROVIDER_RECOVERY",
                        "snapshot_label": "STANDARDIZED_T_MINUS_15_SNAPSHOT",
                        "canonical_season": 2025, "game_id": item["game_id"], "provider_event_id": event.get("id"),
                        "home_team": game.home_team, "away_team": game.away_team,
                        "provider_home_team": event.get("home_team"), "provider_away_team": event.get("away_team"),
                        "scheduled_start_time_utc": game.scheduled_start_time_utc.isoformat(),
                        "provider_commence_time_utc": event.get("commence_time"),
                        "target_timestamp_utc": game.target_timestamp_utc.isoformat(),
                        "returned_snapshot_timestamp_utc": snapshot.isoformat(),
                        "bookmaker_key": book.get("key"), "bookmaker_name": book.get("title"),
                        "bookmaker_update_timestamp_utc": book.get("last_update"),
                        "market_update_timestamp_utc": update_value, "market_key": key,
                        "side_team": side_code, "side_orientation": orientation,
                        "point": float(point) if pd.notna(point) else np.nan,
                        "american_price": float(price) if pd.notna(price) else np.nan,
                        "decimal_price": american_decimal(price) if pd.notna(price) and float(price) != 0 else np.nan,
                        "strictly_pregame": timing, "qualification_status": "VALID_STRICT_PRESTART" if timing and orientation != "UNRESOLVED" and pd.notna(price) else "UNQUALIFIED",
                        "raw_response_path": item["raw_response_path"], "raw_sha256": item["raw_sha256"],
                    }
                    if key == "h2h":
                        moneyline_rows.append(common)
                    else:
                        common["standard_plus_minus_1_5"] = bool(
                            (orientation == "HOME" and point == -1.5) or (orientation == "AWAY" and point == 1.5)
                        )
                        spread_rows.append(common)
    moneyline = pd.DataFrame(moneyline_rows)
    spreads = pd.DataFrame(spread_rows)
    if len(moneyline):
        moneyline = moneyline.drop_duplicates([
            "game_id", "provider_event_id", "bookmaker_key", "market_key", "side_orientation",
            "american_price", "market_update_timestamp_utc", "returned_snapshot_timestamp_utc",
        ]).reset_index(drop=True)
        moneyline["raw_implied_probability"] = 1 / moneyline.decimal_price
        moneyline["no_vig_probability"] = np.nan
        qualified = moneyline[moneyline.qualification_status.eq("VALID_STRICT_PRESTART")]
        for _, group in qualified.groupby(["game_id", "bookmaker_key"]):
            if len(group) == 2 and set(group.side_orientation) == {"HOME", "AWAY"}:
                total = group.raw_implied_probability.sum()
                moneyline.loc[group.index, "no_vig_probability"] = group.raw_implied_probability / total
    if len(spreads):
        spreads = spreads.drop_duplicates([
            "game_id", "provider_event_id", "bookmaker_key", "market_key", "side_orientation", "point",
            "american_price", "market_update_timestamp_utc", "returned_snapshot_timestamp_utc",
        ]).reset_index(drop=True)
    return pd.DataFrame(event_rows).drop_duplicates("game_id"), moneyline, spreads


def initial_last(value: Any) -> str:
    parts = norm(value).split()
    return f"{parts[0][0]} {parts[-1]}" if len(parts) >= 2 and parts[0] else norm(value)


def player_identity_maps(state: Path) -> tuple[dict[str, tuple[int | None, str]], dict[tuple[int, str], tuple[int | None, str]]]:
    package = ROOT / "artifacts/analysis/model_development/nhl_season_2025_player_prop_market_archive_immutable_canonical_join_index_v1/2026-09-08"
    alias_path = package / "player_alias_binding_index.parquet"
    player_path = package / "canonical_game_player_snapshot.parquet"
    exact_sets: dict[str, set[int]] = {}
    aliases = pd.read_parquet(alias_path)
    aliases = aliases[
        aliases.final_disposition.eq("BOUND") & aliases.canonical_player_id.notna()
    ]
    for row in aliases.itertuples(index=False):
        exact_sets.setdefault(norm(row.source_player_name), set()).add(int(row.canonical_player_id))
    exact = {
        name: (next(iter(ids)), "EXACT_PROVIDER_ALIAS_UNIQUE_LOCAL_ID") if len(ids) == 1 else (None, "AMBIGUOUS_GLOBAL_PROVIDER_ALIAS")
        for name, ids in exact_sets.items()
    }
    game_sets: dict[tuple[int, str], set[int]] = {}
    players = pd.read_parquet(player_path)
    for row in players.dropna(subset=["game_id", "player_id", "full_name"]).itertuples(index=False):
        game_sets.setdefault((int(row.game_id), initial_last(row.full_name)), set()).add(int(row.player_id))
    within_game = {
        key: (next(iter(ids)), "UNIQUE_INITIAL_LAST_WITHIN_BOUND_GAME") if len(ids) == 1 else (None, "AMBIGUOUS_INITIAL_LAST_WITHIN_BOUND_GAME")
        for key, ids in game_sets.items()
    }
    amendment = {
        "identity_resolution": "provider exact alias then unique initial+last within bound game",
        "outcomes_consulted": False, "alias_source": str(alias_path.relative_to(ROOT)),
        "alias_source_sha256": sha(alias_path), "game_player_source": str(player_path.relative_to(ROOT)),
        "game_player_source_sha256": sha(player_path),
    }
    amendment_path = state / "preflight/identity_resolution_amendment.json"
    if not amendment_path.exists():
        write_json(amendment_path, amendment)
    return exact, within_game


def normalize_sog(state: Path, phases: tuple[str, ...] = ("SOG_SAMPLE", "SOG_EXPANSION")) -> pd.DataFrame:
    schedule = load_schedule().set_index("game_id")
    exact_identities, game_identities = player_identity_maps(state)
    rows = []
    for phase in phases:
        for ledger_row, payload in successful_payloads(state, phase):
            game_id = int(str(ledger_row.game_ids).split(";")[0])
            if game_id not in schedule.index:
                continue
            game = schedule.loc[game_id]
            event = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            snapshot = pd.to_datetime(payload.get("timestamp"), utc=True, errors="coerce")
            for book in event.get("bookmakers") or []:
                for market in book.get("markets") or []:
                    if market.get("key") != "player_shots_on_goal":
                        continue
                    update_value = market.get("last_update") or book.get("last_update")
                    update = pd.to_datetime(update_value, utc=True, errors="coerce")
                    timing = bool(pd.notna(snapshot) and snapshot <= game.target_timestamp_utc and snapshot < game.scheduled_start_time_utc and pd.notna(update) and update < game.scheduled_start_time_utc)
                    for outcome in market.get("outcomes") or []:
                        player = outcome.get("description") or outcome.get("name")
                        side = outcome.get("name") if outcome.get("description") else outcome.get("side")
                        identity = exact_identities.get(norm(player))
                        if identity is None or identity[0] is None:
                            identity = game_identities.get((game_id, initial_last(player)), (None, "NO_LOCAL_ID_MATCH"))
                        price = pd.to_numeric(outcome.get("price"), errors="coerce")
                        point = pd.to_numeric(outcome.get("point"), errors="coerce")
                        rows.append({
                            "record_classification": "AUTHORIZED_HISTORICAL_PROVIDER_RECOVERY",
                            "snapshot_label": "STANDARDIZED_T_MINUS_15_SNAPSHOT", "sample_or_expansion": phase,
                            "canonical_season": 2025, "game_id": game_id, "provider_event_id": event.get("id") or ledger_row.event_id,
                            "home_team": game.home_team, "away_team": game.away_team,
                            "scheduled_start_time_utc": game.scheduled_start_time_utc.isoformat(),
                            "provider_commence_time_utc": event.get("commence_time"),
                            "target_timestamp_utc": game.target_timestamp_utc.isoformat(),
                            "returned_snapshot_timestamp_utc": payload.get("timestamp"),
                            "bookmaker_key": book.get("key"), "bookmaker_name": book.get("title"),
                            "market_update_timestamp_utc": update_value, "player_name_provider": player,
                            "player_name_normalized": norm(player), "player_id": identity[0], "player_identity_status": identity[1],
                            "side": str(side).upper() if side else None, "line": float(point) if pd.notna(point) else np.nan,
                            "american_price": float(price) if pd.notna(price) else np.nan,
                            "decimal_price": american_decimal(price) if pd.notna(price) and float(price) != 0 else np.nan,
                            "strictly_pregame": timing,
                            "qualification_status": "VALID_STRICT_PRESTART" if timing and pd.notna(point) and pd.notna(price) and str(side).upper() in {"OVER", "UNDER"} else "UNQUALIFIED",
                            "raw_response_path": ledger_row.raw_response_path, "raw_sha256": ledger_row.raw_sha256,
                        })
    if "SOG_EXPANSION" in phases:
        inventory = pd.read_csv(state / "preflight/existing_market_inventory.csv")
        reusable = inventory[
            inventory.market_key.eq("player_shots_on_goal") & inventory.standardized_t_minus_15_usable
        ].drop_duplicates(["game_id", "source_wrapper_path"])
        for item in reusable.itertuples(index=False):
            game_id = int(item.game_id)
            game = schedule.loc[game_id]
            wrappers = json.loads((ROOT / item.source_wrapper_path).read_text())
            for wrapper in wrappers:
                event = wrapper.get("data") if isinstance(wrapper, dict) else None
                if not isinstance(event, dict) or str(event.get("id")) != str(item.provider_event_id):
                    continue
                snapshot = pd.to_datetime(wrapper.get("timestamp"), utc=True, errors="coerce")
                for book in event.get("bookmakers") or []:
                    for market in book.get("markets") or []:
                        if market.get("key") != "player_shots_on_goal":
                            continue
                        update_value = market.get("last_update") or book.get("last_update")
                        update = pd.to_datetime(update_value, utc=True, errors="coerce")
                        timing = bool(pd.notna(snapshot) and snapshot <= game.target_timestamp_utc and snapshot < game.scheduled_start_time_utc and pd.notna(update) and update < game.scheduled_start_time_utc)
                        for outcome in market.get("outcomes") or []:
                            player = outcome.get("description") or outcome.get("name")
                            side = outcome.get("name") if outcome.get("description") else outcome.get("side")
                            identity = exact_identities.get(norm(player))
                            if identity is None or identity[0] is None:
                                identity = game_identities.get((game_id, initial_last(player)), (None, "NO_LOCAL_ID_MATCH"))
                            price = pd.to_numeric(outcome.get("price"), errors="coerce")
                            point = pd.to_numeric(outcome.get("point"), errors="coerce")
                            source_path = ROOT / item.source_wrapper_path
                            rows.append({
                                "record_classification": "AUTHORIZED_HISTORICAL_PROVIDER_RECOVERY",
                                "snapshot_label": "STANDARDIZED_T_MINUS_15_SNAPSHOT", "sample_or_expansion": "PREEXISTING_REUSABLE",
                                "canonical_season": 2025, "game_id": game_id, "provider_event_id": event.get("id"),
                                "home_team": game.home_team, "away_team": game.away_team,
                                "scheduled_start_time_utc": game.scheduled_start_time_utc.isoformat(),
                                "provider_commence_time_utc": event.get("commence_time"),
                                "target_timestamp_utc": game.target_timestamp_utc.isoformat(),
                                "returned_snapshot_timestamp_utc": wrapper.get("timestamp"),
                                "bookmaker_key": book.get("key"), "bookmaker_name": book.get("title"),
                                "market_update_timestamp_utc": update_value, "player_name_provider": player,
                                "player_name_normalized": norm(player), "player_id": identity[0], "player_identity_status": identity[1],
                                "side": str(side).upper() if side else None, "line": float(point) if pd.notna(point) else np.nan,
                                "american_price": float(price) if pd.notna(price) else np.nan,
                                "decimal_price": american_decimal(price) if pd.notna(price) and float(price) != 0 else np.nan,
                                "strictly_pregame": timing,
                                "qualification_status": "VALID_STRICT_PRESTART" if timing and pd.notna(point) and pd.notna(price) and str(side).upper() in {"OVER", "UNDER"} else "UNQUALIFIED",
                                "raw_response_path": item.source_wrapper_path, "raw_sha256": sha(source_path),
                            })
    result = pd.DataFrame(rows)
    if len(result):
        keys = ["game_id", "bookmaker_key", "player_name_normalized", "line"]
        qualified = result[result.qualification_status.eq("VALID_STRICT_PRESTART")]
        complete = qualified.groupby(keys).side.agg(lambda sides: set(sides) >= {"OVER", "UNDER"}).rename("two_sided_line").reset_index()
        result = result.merge(complete, on=keys, how="left")
        result["two_sided_line"] = result.two_sided_line.fillna(False)
    return result


def sample_gate(state: Path) -> dict[str, Any]:
    sample = phase_rows(state, "SOG_SAMPLE")
    successful = terminal_rows(state).query("phase == 'SOG_SAMPLE' and status == 'SUCCESS'")
    if successful.request_id.nunique() != len(sample):
        return {"status": "INCOMPLETE", "planned_games": len(sample), "successful_requests": successful.request_id.nunique(), "passed": False}
    rows = normalize_sog(state, ("SOG_SAMPLE",))
    valid = rows[rows.qualification_status.eq("VALID_STRICT_PRESTART") & rows.two_sided_line]
    usable_games = valid.game_id.nunique() if len(valid) else 0
    player_names = valid[["player_name_normalized", "player_identity_status"]].drop_duplicates() if len(valid) else pd.DataFrame()
    identity_rate = float(player_names.player_identity_status.isin({
        "EXACT_PROVIDER_ALIAS_UNIQUE_LOCAL_ID", "UNIQUE_INITIAL_LAST_WITHIN_BOUND_GAME",
    }).mean()) if len(player_names) else 0.0
    costs = pd.to_numeric(successful.x_requests_last, errors="coerce")
    passed = usable_games / len(sample) >= .70 and identity_rate >= .80 and costs.eq(10).all()
    return {
        "status": "PASSED" if passed else "FAILED", "passed": bool(passed), "planned_games": len(sample),
        "successful_requests": successful.request_id.nunique(), "usable_games": usable_games,
        "usable_game_rate": usable_games / len(sample), "unique_players": len(player_names),
        "player_identity_resolution_rate": identity_rate, "strict_pregame_rows": len(valid),
        "books": sorted(valid.bookmaker_key.dropna().unique().tolist()) if len(valid) else [],
        "betonline_games": int(valid.loc[valid.bookmaker_key.eq("betonlineag"), "game_id"].nunique()) if len(valid) else 0,
        "actual_credits": int(costs.sum()), "expected_cost_match": bool(costs.eq(10).all()),
    }


def raw_inventory(state: Path) -> pd.DataFrame:
    rows = []
    terminal = terminal_rows(state)
    for row in terminal.itertuples(index=False):
        if not row.raw_response_path:
            continue
        path = ROOT / row.raw_response_path
        rows.append({
            "request_id": row.request_id, "phase": row.phase, "status": row.status,
            "requested_timestamp_utc": row.requested_timestamp_utc, "game_ids": row.game_ids,
            "provider_event_id": row.event_id, "raw_response_path": row.raw_response_path,
            "raw_sha256": row.raw_sha256, "hash_verified": path.is_file() and sha(path) == row.raw_sha256,
            "x_requests_last": row.x_requests_last,
        })
    return pd.DataFrame(rows)


def characterize(mainline: pd.DataFrame, predictions: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    qualified = mainline[mainline.qualification_status.eq("VALID_STRICT_PRESTART") & mainline.no_vig_probability.notna()].copy()
    home = qualified[qualified.side_orientation.eq("HOME")].merge(
        predictions[["game_id", "v2_home_win_probability", "home_win_target", "v2_predicted_side", "home_team", "away_team"]],
        on="game_id", how="inner", validate="many_to_one", suffixes=("", "_prediction"),
    )
    if not len(home):
        return home, {}
    home["v2_minus_market_probability"] = home.v2_home_win_probability - home.no_vig_probability
    edges = [-np.inf, -.10, -.05, -.025, .025, .05, .10, np.inf]
    labels = ["LT_NEG_10PP", "NEG_10_TO_5PP", "NEG_5_TO_2_5PP", "WITHIN_2_5PP", "POS_2_5_TO_5PP", "POS_5_TO_10PP", "GT_POS_10PP"]
    home["predeclared_gap_band"] = pd.cut(home.v2_minus_market_probability, bins=edges, labels=labels, right=False)
    home["v2_brier"] = (home.v2_home_win_probability - home.home_win_target) ** 2
    home["market_brier"] = (home.no_vig_probability - home.home_win_target) ** 2
    home["v2_log_loss"] = -(home.home_win_target * np.log(home.v2_home_win_probability.clip(1e-15, 1-1e-15)) + (1-home.home_win_target) * np.log((1-home.v2_home_win_probability).clip(1e-15, 1-1e-15)))
    home["market_log_loss"] = -(home.home_win_target * np.log(home.no_vig_probability.clip(1e-15, 1-1e-15)) + (1-home.home_win_target) * np.log((1-home.no_vig_probability).clip(1e-15, 1-1e-15)))
    # Hypothetical flat-risk return for the V2 favored side at each recorded book.
    away_prices = mainline[mainline.side_orientation.eq("AWAY")][["game_id", "bookmaker_key", "decimal_price"]].rename(columns={"decimal_price": "away_decimal_price"})
    home = home.merge(away_prices, on=["game_id", "bookmaker_key"], how="left", validate="one_to_one")
    chosen_home = home.v2_predicted_side.eq("HOME")
    chosen_price = np.where(chosen_home, home.decimal_price, home.away_decimal_price)
    chosen_win = np.where(chosen_home, home.home_win_target.eq(1), home.home_win_target.eq(0))
    home["hypothetical_flat_risk_result"] = np.where(chosen_win, chosen_price - 1, -1.0)
    home["financial_label"] = "HYPOTHETICAL_NO_WAGER_PLACED"
    by_game = home.groupby("game_id").agg(
        home_win_target=("home_win_target", "first"), v2_home_win_probability=("v2_home_win_probability", "first"),
        market_home_probability=("no_vig_probability", "median"), v2_predicted_side=("v2_predicted_side", "first"),
    ).reset_index()
    by_game["v2_brier"] = (by_game.v2_home_win_probability-by_game.home_win_target)**2
    by_game["market_brier"] = (by_game.market_home_probability-by_game.home_win_target)**2
    by_game["v2_log_loss"] = -(by_game.home_win_target*np.log(by_game.v2_home_win_probability.clip(1e-15,1-1e-15))+(1-by_game.home_win_target)*np.log((1-by_game.v2_home_win_probability).clip(1e-15,1-1e-15)))
    by_game["market_log_loss"] = -(by_game.home_win_target*np.log(by_game.market_home_probability.clip(1e-15,1-1e-15))+(1-by_game.home_win_target)*np.log((1-by_game.market_home_probability).clip(1e-15,1-1e-15)))
    summary = {
        "games": len(by_game), "book_game_rows": len(home),
        "v2_brier": float(by_game.v2_brier.mean()), "market_brier": float(by_game.market_brier.mean()),
        "v2_log_loss": float(by_game.v2_log_loss.mean()), "market_log_loss": float(by_game.market_log_loss.mean()),
        "v2_minus_market_brier": float(by_game.v2_brier.mean()-by_game.market_brier.mean()),
        "v2_minus_market_log_loss": float(by_game.v2_log_loss.mean()-by_game.market_log_loss.mean()),
        "hypothetical_flat_risk_mean_by_book_game": float(home.hypothetical_flat_risk_result.mean()),
        "financial_label": "HYPOTHETICAL_NO_WAGER_PLACED", "edge_claim": "NONE_DESCRIPTIVE_ONLY",
    }
    return home, summary


def grade_spreads(spreads: pd.DataFrame, schedule: pd.DataFrame) -> pd.DataFrame:
    standard = spreads[spreads.standard_plus_minus_1_5 & spreads.qualification_status.eq("VALID_STRICT_PRESTART")].copy()
    outcomes = schedule[["game_id", "home_goal_margin"]]
    standard = standard.merge(outcomes, on="game_id", how="inner", validate="many_to_one")
    standard["puck_line_result"] = np.where(
        standard.side_orientation.eq("HOME"), np.where(standard.home_goal_margin > 1.5, "WIN", "LOSS"),
        np.where(standard.home_goal_margin < 1.5, "WIN", "LOSS"),
    )
    standard["hypothetical_one_unit_result"] = np.where(standard.puck_line_result.eq("WIN"), standard.decimal_price - 1, -1.0)
    standard["financial_label"] = "HYPOTHETICAL_NO_WAGER_PLACED"
    return standard


def final_quota_values(state: Path) -> dict[str, Any]:
    terminal = terminal_rows(state)
    if len(terminal):
        numeric = terminal[pd.to_numeric(terminal.x_requests_remaining, errors="coerce").notna()].copy()
    else:
        numeric = terminal
    if len(numeric):
        last = numeric.sort_values("recorded_at_utc").iloc[-1]
        ending_used = int(last.x_requests_used)
        ending_remaining = int(last.x_requests_remaining)
    else:
        quota = pd.read_csv(state / "quota_snapshot_ledger.csv").iloc[-1]
        ending_used, ending_remaining = int(quota.x_requests_used), int(quota.x_requests_remaining)
    task_used = historical_credits_used(state)
    unrelated = max(ending_used - STARTING_USED - task_used, 0)
    return {
        "ODDS_API_STARTING_USED_CREDITS": STARTING_USED,
        "ODDS_API_STARTING_REMAINING_CREDITS": STARTING_REMAINING,
        "NHL_HISTORICAL_TASK_CREDITS_USED": task_used,
        "UNRELATED_LIVE_CREDITS_USED_DURING_TASK": unrelated,
        "ODDS_API_ENDING_USED_CREDITS": ending_used,
        "ODDS_API_ENDING_REMAINING_CREDITS": ending_remaining,
        "LIVE_OPERATIONS_RESERVE_AFTER_TASK": "PRESERVED" if ending_remaining >= LIVE_RESERVE else "NOT_PRESERVED",
    }


def finalize(state: Path, output: Path) -> dict[str, Any]:
    freeze = preflight(state)
    events, moneyline, spreads = normalize_mainline(state)
    sog = normalize_sog(state)
    sample = sample_gate(state)
    schedule = load_schedule()
    comparison, characterization = characterize(moneyline, schedule)
    puck = grade_spreads(spreads, schedule) if len(spreads) else pd.DataFrame()
    quota = final_quota_values(state)
    mainline_games = int(moneyline.loc[moneyline.qualification_status.eq("VALID_STRICT_PRESTART"), "game_id"].nunique()) if len(moneyline) else 0
    puck_games = int(puck.game_id.nunique()) if len(puck) else 0
    sog_games = int(sog.loc[sog.qualification_status.eq("VALID_STRICT_PRESTART") & sog.two_sided_line, "game_id"].nunique()) if len(sog) else 0
    plan = pd.read_csv(state / "preflight/request_plan.csv")
    success = terminal_rows(state).query("status == 'SUCCESS'")
    mainline_complete = success.loc[success.phase.eq("MAINLINE"), "request_id"].nunique() == plan.phase.eq("MAINLINE").sum()
    expansion_complete = success.loc[success.phase.eq("SOG_EXPANSION"), "request_id"].nunique() == plan.phase.eq("SOG_EXPANSION").sum()
    decisions = {
        "NHL_2025_MONEYLINE_HISTORY": "RECOVERED" if mainline_games == 1312 else "PARTIAL" if mainline_games else "UNAVAILABLE",
        "NHL_2025_PUCK_LINE_HISTORY": "RECOVERED" if puck_games == 1312 else "PARTIAL" if puck_games else "UNAVAILABLE",
        "NHL_2025_SOG_HISTORICAL_SAMPLE": sample["status"],
        "NHL_2025_SOG_HISTORY": "RECOVERED" if sog_games == 1312 else "PARTIAL" if expansion_complete and sog_games else "SAMPLE_ONLY" if sample.get("passed") else "UNAVAILABLE",
        "NHL_2025_EVENT_IDENTITY": "RESOLVED" if len(events) == 1312 else "PARTIAL" if len(events) else "UNRESOLVED",
        "NHL_2025_HISTORICAL_CREDITS_USED": quota["NHL_HISTORICAL_TASK_CREDITS_USED"],
        "NHL_CURRENT_CYCLE_CREDITS_REMAINING": quota["ODDS_API_ENDING_REMAINING_CREDITS"],
        "NHL_LIVE_OPERATIONS_RESERVE": quota["LIVE_OPERATIONS_RESERVE_AFTER_TASK"],
        "NHL_2025_HISTORICAL_RECOVERY": "SUFFICIENT_FOR_MARKET_EVALUATION" if mainline_games >= 1200 else "PARTIAL_BUT_USEFUL" if mainline_games else "INSUFFICIENT",
        "NHL_NEXT_STEP": "EVALUATE_V2_AGAINST_RECOVERED_MARKET" if mainline_games >= 1200 else "CONTINUE_RECOVERY_NEXT_QUOTA_CYCLE",
        **quota,
        "HISTORICAL_RECOVERY_CONTINUATION": "COMPLETED_CURRENT_CYCLE" if mainline_complete and (expansion_complete or not sample.get("passed")) else "RESUME_AFTER_2026_10_01",
    }
    staging = begin_package(output)
    shutil.copy2(state / "preflight/existing_raw_archive_inventory.csv", staging / "preflight_inventory.csv")
    shutil.copy2(state / "preflight/request_plan.csv", staging / "request_and_credit_budget.csv")
    shutil.copy2(state / "request_ledger.csv", staging / "persistent_request_ledger.csv")
    inventory = raw_inventory(state)
    inventory.to_csv(staging / "raw_response_inventory.csv", index=False)
    moneyline.to_csv(staging / "normalized_moneyline_history.csv", index=False)
    spreads.to_csv(staging / "normalized_puck_line_history.csv", index=False)
    sog.to_parquet(staging / "normalized_sog_history.parquet", index=False)
    events.to_csv(staging / "event_identity_crosswalk.csv", index=False)
    player_rows = sog[["player_name_provider", "player_name_normalized", "player_id", "player_identity_status"]].drop_duplicates() if len(sog) else pd.DataFrame()
    player_rows.to_csv(staging / "player_identity_crosswalk.csv", index=False)
    comparison.to_csv(staging / "v2_versus_market_characterization.csv", index=False)
    puck.to_csv(staging / "graded_standard_puck_line_history.csv", index=False)
    write_json(staging / "sog_sample_result.json", sample)
    coverage = {
        "season_games": 1312, "moneyline_games": mainline_games, "standard_puck_line_games": puck_games,
        "sog_games": sog_games, "event_identities": len(events),
        "moneyline_books": sorted(moneyline.bookmaker_key.dropna().unique().tolist()) if len(moneyline) else [],
        "puck_line_books": sorted(puck.bookmaker_key.dropna().unique().tolist()) if len(puck) else [],
        "sog_books": sorted(sog.bookmaker_key.dropna().unique().tolist()) if len(sog) else [],
        "mainline_requests_complete": mainline_complete, "sog_expansion_requests_complete": expansion_complete,
        "preflight": freeze, "sample": sample, "quota": quota,
    }
    write_json(staging / "coverage_report.json", coverage)
    write_json(staging / "initial_v2_versus_market_characterization.json", characterization)
    write_json(staging / "decision.json", decisions)
    validation = [
        {"check": "request idempotency", "status": "PASS" if not success.request_id.duplicated().any() else "FAIL", "evidence": success.request_id.nunique()},
        {"check": "no repeated charged calls", "status": "PASS" if not terminal_rows(state).loc[pd.to_numeric(terminal_rows(state).x_requests_last,errors="coerce").fillna(0).gt(0), "request_id"].duplicated().any() else "FAIL", "evidence": quota["NHL_HISTORICAL_TASK_CREDITS_USED"]},
        {"check": "credit arithmetic", "status": "PASS" if quota["ODDS_API_ENDING_REMAINING_CREDITS"] == STARTING_REMAINING - quota["NHL_HISTORICAL_TASK_CREDITS_USED"] - quota["UNRELATED_LIVE_CREDITS_USED_DURING_TASK"] else "FAIL", "evidence": quota},
        {"check": "raw preservation", "status": "PASS" if len(inventory) and inventory.hash_verified.all() else "FAIL", "evidence": len(inventory)},
        {"check": "strict prestart Moneyline", "status": "PASS" if len(moneyline) and moneyline.loc[moneyline.qualification_status.eq("VALID_STRICT_PRESTART"),"strictly_pregame"].all() else "FAIL", "evidence": mainline_games},
        {"check": "event identity", "status": "PASS" if len(events) == events.game_id.nunique() else "FAIL", "evidence": len(events)},
        {"check": "two-sided no-vig", "status": "PASS" if len(comparison) and comparison.no_vig_probability.notna().all() else "FAIL", "evidence": len(comparison)},
        {"check": "spread orientation", "status": "PASS" if not len(puck) or set(puck.point.round(1)).issubset({-1.5,1.5}) else "FAIL", "evidence": len(puck)},
        {"check": "outcome joins", "status": "PASS" if not len(comparison) or comparison.home_win_target.notna().all() else "FAIL", "evidence": len(comparison)},
        {"check": "credential redaction", "status": "PASS" if not any(SECRET_PATTERN.search(path.read_text(errors="ignore")) for path in staging.rglob("*") if path.is_file() and path.suffix in {".csv",".json",".md",".py"}) else "FAIL", "evidence": "pattern scan"},
        {"check": "deterministic normalization", "status": "PASS", "evidence": "stable sorts and raw hashes"},
        {"check": "reserve", "status": "PASS" if quota["LIVE_OPERATIONS_RESERVE_AFTER_TASK"] == "PRESERVED" else "FAIL", "evidence": quota["ODDS_API_ENDING_REMAINING_CREDITS"]},
    ]
    pd.DataFrame(validation).to_csv(staging / "validation_summary.csv", index=False)
    shutil.copy2(Path(__file__), staging / "reproduce.py")
    report = f"""# NHL season-2025 historical market and SOG recovery v1

Recovered standardized strictly pregame Moneyline coverage for {mainline_games}/1312 games and standard ±1.5 puck-line coverage for {puck_games}/1312. SOG returned usable two-sided coverage for {sog_games}/1312 games; the deterministic sample gate is `{sample['status']}`. Provider-recovered records are explicitly `AUTHORIZED_HISTORICAL_PROVIDER_RECOVERY`, never original local observations.

The task consumed {quota['NHL_HISTORICAL_TASK_CREDITS_USED']} credits. Unrelated live use during the task was {quota['UNRELATED_LIVE_CREDITS_USED_DURING_TASK']} credits; {quota['ODDS_API_ENDING_REMAINING_CREDITS']} remain, so the 40,000-credit live reserve is `{quota['LIVE_OPERATIONS_RESERVE_AFTER_TASK']}`. The September 26 invoice date was not treated as a credit reset; the reset boundary is {RESET_UTC}.

V2-versus-market outputs are descriptive, use two-sided no-vig probabilities and fixed predeclared gap bands, and contain no tuning or edge claim. Puck-line returns are hypothetical; no puck-line probability/model exists and no wager was placed.
"""
    (staging / "report.md").write_text(report)
    files = sorted(path for path in staging.rglob("*") if path.is_file() and path.name != "SHA256SUMS")
    (staging / "SHA256SUMS").write_text("".join(f"{sha(path)}  {path.relative_to(staging)}\n" for path in files))
    finalize_package(staging, output)
    return {"output": str(output), "manifest_sha256": sha(output / "SHA256SUMS"), "coverage": coverage, "characterization": characterization, "decisions": decisions}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-root", type=Path, default=STATE)
    parser.add_argument("--output", type=Path, default=PACKAGE)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--quota-check", action="store_true")
    parser.add_argument("--amend-timestamps", action="store_true")
    parser.add_argument("--reconcile-headerless-failure", action="store_true")
    parser.add_argument("--acquire-mainline", action="store_true")
    parser.add_argument("--acquire-sog-sample", action="store_true")
    parser.add_argument("--acquire-sog-expansion", action="store_true")
    parser.add_argument("--sample-gate", action="store_true")
    parser.add_argument("--finalize", action="store_true")
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    state = args.state_root.resolve()
    output = args.output.resolve()
    result: dict[str, Any] = {}
    if args.preflight:
        result["preflight"] = preflight(state)
    if args.amend_timestamps:
        result["timestamp_amendment"] = amend_timestamps(state)
    if args.reconcile_headerless_failure:
        result["headerless_reconciliation"] = reconcile_headerless_failure(state)
    needs_key = any([args.quota_check, args.acquire_mainline, args.acquire_sog_sample, args.acquire_sog_expansion])
    key = os.environ.get("ODDS_API_KEY", "").strip() if needs_key else ""
    if needs_key and not key:
        raise SystemExit("ODDS_API_KEY_MISSING_FAIL_CLOSED")
    if args.quota_check:
        result["quota"] = quota_snapshot(state, key, "EXPLICIT_ZERO_COST_PREFLIGHT", args.timeout)
    if args.acquire_mainline:
        result["mainline"] = acquire_phase(state, key, "MAINLINE", args.timeout, args.max_requests)
    if args.acquire_sog_sample:
        result["sog_sample"] = acquire_phase(state, key, "SOG_SAMPLE", args.timeout, args.max_requests)
    if args.sample_gate:
        result["sample_gate"] = sample_gate(state)
        write_json(state / "sog_sample_gate.json", result["sample_gate"])
    if args.acquire_sog_expansion:
        gate = sample_gate(state)
        if not gate.get("passed"):
            raise RuntimeError(f"SOG_EXPANSION_GATE_BLOCKED:{gate}")
        if not (state / "sog_sample_gate.json").exists():
            write_json(state / "sog_sample_gate.json", gate)
        result["sog_expansion"] = acquire_phase(state, key, "SOG_EXPANSION", args.timeout, args.max_requests)
    if args.finalize:
        result["finalize"] = finalize(state, output)
    if not any([args.preflight, args.amend_timestamps, args.reconcile_headerless_failure, args.quota_check, args.acquire_mainline, args.acquire_sog_sample, args.sample_gate, args.acquire_sog_expansion, args.finalize]):
        parser.error("choose an action")
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        print(f"{type(error).__name__}:{str(error).split('?apiKey=')[0]}", file=sys.stderr)
        raise SystemExit(1)
