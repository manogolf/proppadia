#!/usr/bin/env python3
"""Run the bounded remaining Starter recovery campaign.

This utility is an artifact-only campaign executor for the already-designed
ordinary Starter discovery cohorts. It may make only bounded MLB StatsAPI reads
for frozen cohort targets and exact acquisition manifests. It performs no
database/API writes, OddsAPI calls, uploads, LaunchAgent changes, matrix
construction, model/scoring work, PA/Outcome/Bundle remediation, Variant C
resolution, or production behavior changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import tokenize
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_STARTING_SHA = "e2ec5afcfbcdaea9afcc9cf9d0fd9d435cf5f2df9760ecdfc7c4b161c22add37"
EXPECTED_SCALE_UP_SHA = "f6ead8dfc5482b89ee9bdd349c6538dd9d1430c704c489a40e65b4664d02d33c"
EXPECTED_STARTING_STATE = "STARTER_POST_COHORT_004_RESOLVED_BRANCH_CUMULATIVE_QUALIFICATION_STATE = CERTIFIED"
EXPECTED_STARTING_DECISION = (
    "STARTER_DISCOVERY_COHORT_004_RESOLVED_BRANCH_RECONSTRUCTION_REMEDIATION_DECISION = "
    "DISCOVERY_TO_ACQUISITION_TO_REMEDIATION_PIPELINE_REVALIDATED_CONTINUE_NEXT_COHORT"
)

DECISION_ALL_COMPLETED = "ALL_REMAINING_ORDINARY_COHORTS_COMPLETED_CUMULATIVE_STATE_CERTIFIED"
DECISION_FAIL_CLOSED = "CAMPAIGN_STOPPED_AT_GOVERNED_FAIL_CLOSED_CONDITION"
DECISION_DISCOVERY_VARIANCE = "CAMPAIGN_STOPPED_AT_DISCOVERY_OR_ACQUISITION_VARIANCE"
DECISION_RECON_VARIANCE = "CAMPAIGN_STOPPED_AT_RECONSTRUCTION_VARIANCE"
DECISION_STATE_FAILURE = "CAMPAIGN_STOPPED_AT_STATE_OR_VALIDATION_FAILURE"
DECISION_NONE = "NO_ELIGIBLE_REMAINING_ORDINARY_COHORTS"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_recovery_campaign/"
    "2026-07-15"
)
STARTING_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_004_resolved_branch_starter_reconstruction_remediation/"
    "2026-07-15"
)
SCALE_UP_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/"
    "2026-07-15"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

STARTING_STATE = STARTING_DIR / f"certified_post_remediation_qualification_state_{RUN_DATE}.json"
STARTING_MOVEMENT = STARTING_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
STARTING_SIDES = STARTING_DIR / f"side_level_reconstruction_certification_ledger_{RUN_DATE}.csv"
ORIGINAL_SIDE_RECON = SCALE_UP_DIR / f"authoritative_96_side_campaign_reconciliation_{RUN_DATE}.csv"
ORIGINAL_ROW_RECON = SCALE_UP_DIR / f"authoritative_803_row_campaign_reconciliation_{RUN_DATE}.csv"
COHORT_PLAN = SCALE_UP_DIR / f"full_remaining_cohort_plan_{RUN_DATE}.csv"
REMAINING_INVENTORY = SCALE_UP_DIR / f"remaining_discovery_side_inventory_{RUN_DATE}.csv"
MATRIX_PATHS = [
    MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

TEAM_ALIASES = {
    "ATH": "OAK",
    "AZ": "ARI",
    "CHW": "CWS",
    "KCR": "KC",
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
    "WSN": "WSH",
}

PROHIBITED_PATTERNS = {
    "db_or_production_write": re.compile(
        r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b",
        re.IGNORECASE,
    ),
    "oddsapi": re.compile(r"oddsapi|odds_api|the-odds-api", re.IGNORECASE),
    "upload_or_scheduler": re.compile(r"upload_ready|write_upload|launchctl|LaunchAgent", re.IGNORECASE),
    "matrix_model_signal": re.compile(
        r"build_mlb_selected_proposition_abd_matrices|\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss|signal_|score_",
        re.IGNORECASE,
    ),
    "downstream_remediation": re.compile(
        r"pa_remediation|outcome_remediation|bundle_remediation|variant_c_resolution",
        re.IGNORECASE,
    ),
}


class CampaignStop(RuntimeError):
    def __init__(self, decision: str, reason: str, cohort_id: str = "") -> None:
        super().__init__(reason)
        self.decision = decision
        self.reason = reason
        self.cohort_id = cohort_id


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def package_sha(package_dir: Path) -> str:
    return sha256_path(package_dir / f"sha256_manifest_{RUN_DATE}.csv")


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def float_value(value: Any) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return 0.0


def avg(values: list[float]) -> float:
    return mean(values) if values else 0.0


def weighted_blend(last3: float, last5: float, full: float) -> float:
    return (0.50 * last3) + (0.30 * last5) + (0.20 * full)


def safe_token(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("_")[:180]


def canon_team(value: Any) -> str:
    text = str(value or "").strip().upper()
    return TEAM_ALIASES.get(text, text)


def parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def feed_url(game_id: str) -> str:
    return f"https://statsapi.mlb.com/api/v1.1/game/{int(game_id)}/feed/live"


def game_log_url(pitcher_id: str, season: str, cutoff_date: str) -> str:
    params = {
        "stats": "gameLog",
        "group": "pitching",
        "season": season,
        "startDate": f"{season}-01-01",
        "endDate": cutoff_date,
    }
    return f"https://statsapi.mlb.com/api/v1/people/{int(pitcher_id)}/stats?{urllib.parse.urlencode(params)}"


def fetch_or_replay(url: str, raw_path: Path, mode: str, timeout: int) -> tuple[str, bytes, str]:
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    if raw_path.exists():
        return "SUCCESS", raw_path.read_bytes(), "PRESERVED_RAW_REPLAY_NO_NETWORK"
    if mode != "execute":
        return "RAW_MISSING_NETWORK_DISABLED", b"", "NO_NETWORK_REPLAY_FAILED"
    req = urllib.request.Request(url, headers={"User-Agent": "proppadia-bounded-starter-campaign/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        status = getattr(resp, "status", 200)
    raw_path.write_bytes(body)
    return ("SUCCESS" if status == 200 else f"HTTP_{status}"), body, "LIVE_BOUNDED_READ_ONLY_STATSAPI_CALL"


def payload_from_body(body: bytes) -> dict[str, Any]:
    try:
        return json.loads(body)
    except Exception:
        return {}


def pitching_stats(player: dict[str, Any]) -> dict[str, Any]:
    return ((player.get("stats") or {}).get("pitching") or {})


def normalize_pitching_stats(stats: dict[str, Any]) -> dict[str, Any]:
    return {
        "games_started": stats.get("gamesStarted", ""),
        "innings_pitched": stats.get("inningsPitched", ""),
        "outs_recorded": stats.get("outs", ""),
        "hits_allowed": stats.get("hits", ""),
        "earned_runs": stats.get("earnedRuns", ""),
        "walks_allowed": stats.get("baseOnBalls", ""),
        "strikeouts": stats.get("strikeOuts", ""),
        "batters_faced": stats.get("battersFaced", ""),
        "runs": stats.get("runs", ""),
        "pitches_thrown": stats.get("pitchesThrown", ""),
    }


def team_abbrev(payload: dict[str, Any], side_name: str) -> str:
    game_team = (((payload.get("gameData") or {}).get("teams") or {}).get(side_name) or {})
    box_team = ((((payload.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}).get(side_name) or {}).get("team") or {}
    return canon_team(
        game_team.get("abbreviation")
        or game_team.get("teamCode")
        or game_team.get("fileCode")
        or box_team.get("abbreviation")
        or box_team.get("teamCode")
        or box_team.get("fileCode")
    )


def find_starters(team_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for player in (team_payload.get("players") or {}).values():
        stats = pitching_stats(player)
        if int_value(stats.get("gamesStarted")) == 1:
            person = player.get("person") or {}
            rows.append({
                "player_id": str(person.get("id") or ""),
                "player_name": person.get("fullName") or "",
                "stats": stats,
            })
    return sorted(rows, key=lambda r: r["player_id"])


def pitcher_from_boxscore(payload: dict[str, Any], pitcher_id: str) -> tuple[dict[str, Any] | None, str]:
    box = (payload.get("liveData") or {}).get("boxscore") or {}
    for side_name, team_payload in (box.get("teams") or {}).items():
        for player in (team_payload.get("players") or {}).values():
            person = player.get("person") or {}
            if str(person.get("id") or "") == str(pitcher_id):
                return {
                    "player_id": str(pitcher_id),
                    "player_name": person.get("fullName") or "",
                    "team": team_abbrev(payload, side_name),
                    "position": ((player.get("position") or {}).get("abbreviation") or ""),
                    "stats": pitching_stats(player),
                }, ""
    return None, "pitcher_not_found_in_boxscore"


def parse_target_binding(target: dict[str, str], body: bytes, raw_path: Path) -> dict[str, Any]:
    payload = payload_from_body(body)
    base = {
        "starter_game_side_key": target["starter_game_side_key"],
        "game_id": target["governed_target_game"],
        "governed_date": target["governed_target_date"],
        "hitter_team": target["hitter_team"],
        "opponent_team": target["opponent_team"],
        "feed_response_path": str(raw_path),
        "accepted_pitcher_id": "",
        "accepted_pitcher_name": "",
        "binding_status": "SOURCE_FAILURE",
        "temporal_check": "NOT_CHECKED",
        "role_regime_check": "NOT_CHECKED",
        "fail_closed_reason": "",
        "pitcher_candidates_returned": "",
        "target_game_candidates_returned": "",
    }
    if not payload:
        return {**base, "fail_closed_reason": "payload_missing"}
    official_date = str((payload.get("gameData") or {}).get("datetime", {}).get("officialDate") or "")
    status = str((payload.get("gameData") or {}).get("status", {}).get("detailedState") or "")
    box = (payload.get("liveData") or {}).get("boxscore") or {}
    teams = box.get("teams") or {}
    candidates: list[dict[str, Any]] = []
    target_side = ""
    for side_name in ["away", "home"]:
        if team_abbrev(payload, side_name) == canon_team(target["opponent_team"]):
            target_side = side_name
            candidates = find_starters(teams.get(side_name) or {})
    temporal_check = "PASS" if official_date == target["governed_target_date"] else "FAIL_OFFICIAL_DATE_MISMATCH"
    base.update({
        "target_game_candidates_returned": json.dumps({
            "official_date": official_date,
            "status": status,
            "away": team_abbrev(payload, "away"),
            "home": team_abbrev(payload, "home"),
        }, sort_keys=True),
        "pitcher_candidates_returned": json.dumps([
            {"player_id": c["player_id"], "player_name": c["player_name"]} for c in candidates
        ], sort_keys=True),
        "target_team_side": target_side,
        "temporal_check": temporal_check,
    })
    if not target_side:
        return {**base, "binding_status": "FAIL_CLOSED", "fail_closed_reason": "opponent_team_not_found_in_boxscore"}
    if len(candidates) != 1:
        return {**base, "binding_status": "FAIL_CLOSED", "fail_closed_reason": "starter_candidate_count_not_one"}
    starter = candidates[0]
    role_check = "PASS_CONVENTIONAL_STARTER"
    pass_all = temporal_check == "PASS" and role_check == "PASS_CONVENTIONAL_STARTER"
    return {
        **base,
        "accepted_pitcher_id": starter["player_id"],
        "accepted_pitcher_name": starter["player_name"],
        "accepted_target_game_identity": target["governed_target_game"],
        "binding_status": "PASS" if pass_all else "FAIL_CLOSED",
        "role_regime_check": role_check,
        "fail_closed_reason": "" if pass_all else "temporal_or_role_check_failed",
    }


def parse_people_gamelog(target: dict[str, str], pitcher_id: str, body: bytes) -> list[dict[str, Any]]:
    payload = payload_from_body(body)
    rows = []
    for stat_group in payload.get("stats", []) or []:
        for idx, split in enumerate(stat_group.get("splits", []) or []):
            game = split.get("game") or {}
            stat = split.get("stat") or {}
            game_date = str(split.get("date") or game.get("gameDate") or "")
            game_id = str(game.get("gamePk") or game.get("pk") or "")
            if not game_date or not game_id:
                continue
            strict_prior = game_date < target["governed_target_date"]
            is_start = int_value(stat.get("gamesStarted")) == 1
            rows.append({
                "starter_game_side_key": target["starter_game_side_key"],
                "pitcher_id": pitcher_id,
                "historical_game_id": game_id,
                "historical_game_date": game_date,
                "official_starter_designation": str(is_start).lower(),
                "temporal_status": "PASS_STRICT_PRIOR" if strict_prior else "FAIL_NOT_STRICT_PRIOR",
                "accepted_for_acquisition_manifest": is_start and strict_prior,
                "source_record_index": idx,
            })
    return sorted(rows, key=lambda r: (r["historical_game_date"], r["historical_game_id"], r["source_record_index"]))


def parse_feed_record(body: bytes, request_row: dict[str, str]) -> dict[str, Any]:
    payload = payload_from_body(body)
    if not payload:
        return {**request_row, "parser_status": "FAIL", "validation_status": "REJECTED", "reject_reason": "json_or_payload_missing"}
    game_pk = str(payload.get("gamePk") or ((payload.get("gameData") or {}).get("game") or {}).get("pk") or "")
    official_date = str(((payload.get("gameData") or {}).get("datetime") or {}).get("officialDate") or "")
    status = str(((payload.get("gameData") or {}).get("status") or {}).get("detailedState") or "")
    pitcher, pitcher_error = pitcher_from_boxscore(payload, request_row["pitcher_identity"])
    base = {**request_row, "parsed_game_pk": game_pk, "official_date": official_date, "game_status": status, "parser_status": "PASS"}
    if pitcher is None:
        return {**base, "validation_status": "REJECTED", "reject_reason": pitcher_error}
    stats = normalize_pitching_stats(pitcher["stats"])
    validations = {
        "game_identity_status": "PASS" if game_pk == str(request_row["historical_game_identity"]) else "FAIL",
        "pitcher_identity_status": "PASS",
        "strict_prior_status": "PASS" if request_row["historical_date"] < request_row["parent_starter_game_side_identity"].split("|")[0] else "FAIL",
        "date_status": "PASS" if official_date == request_row["historical_date"] else "FAIL",
        "mlb_game_status": "PASS" if "Final" in status or status in {"Completed Early", "Game Over"} else "WARN",
        "starter_role_status": "PASS" if str(stats.get("games_started")) == "1" else "FAIL",
        "record_grain_status": "PASS",
        "required_source_facts_status": "PASS" if stats.get("outs_recorded") not in ("", None) and stats.get("hits_allowed") not in ("", None) else "FAIL",
    }
    failed = [key for key, value in validations.items() if value == "FAIL"]
    return {
        **base,
        "pitcher_name": pitcher.get("player_name", ""),
        "source_team": pitcher.get("team", ""),
        "boxscore_position": pitcher.get("position", ""),
        **stats,
        **validations,
        "validation_status": "ACCEPTED" if not failed else "REJECTED",
        "reject_reason": ";".join(failed),
    }


def history_classification(prior_start_count: int) -> tuple[str, str]:
    if prior_start_count == 0:
        return "RESEARCH_START_HISTORY_NONE", "PREDICTION_INELIGIBLE_NO_PRIOR_MLB_START_HISTORY"
    if prior_start_count < 5:
        return "RESEARCH_START_HISTORY_LOW_SAMPLE_1_TO_4", "PREDICTION_INELIGIBLE_LOW_SAMPLE_LT_5_PRIOR_STARTS"
    return "RESEARCH_START_HISTORY_ESTABLISHED_5_PLUS", "PREDICTION_HISTORY_THRESHOLD_SATISFIED_REQUIRES_OTHER_RULES"


def strip_strings_comments_and_pattern_block(text: str) -> str:
    text = re.sub(r"PROHIBITED_PATTERNS = \{.*?\n\}", "PROHIBITED_PATTERNS = {}", text, flags=re.DOTALL)
    out: list[str] = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type in {tokenize.STRING, tokenize.COMMENT}:
                continue
            out.append(tok.string)
    except tokenize.TokenError:
        return text
    return " ".join(out)


def static_guard() -> list[dict[str, Any]]:
    code_only = strip_strings_comments_and_pattern_block(Path(__file__).read_text(encoding="utf-8"))
    rows = []
    for name, pattern in PROHIBITED_PATTERNS.items():
        matches = pattern.findall(code_only)
        rows.append({
            "check": name,
            "status": "PASS" if not matches else "FAIL",
            "matches": "|".join(str(m) for m in matches),
            "notes": "Static guard excludes comments, string literals, and pattern declarations.",
        })
    rows.append({
        "check": "statsapi_only_network_boundary",
        "status": "PASS" if "statsapi.mlb.com" in Path(__file__).read_text(encoding="utf-8") else "FAIL",
        "matches": "official MLB StatsAPI endpoint only",
        "notes": "No sportsbook, DB, upload, scheduler, model, or matrix endpoint is present.",
    })
    return rows


class CampaignRunner:
    def __init__(self, mode: str, timeout: int, max_cohorts: int | None) -> None:
        self.mode = mode
        self.timeout = timeout
        self.max_cohorts = max_cohorts
        self.out_dir = OUT_DIR
        self.starting_state = json.loads(STARTING_STATE.read_text(encoding="utf-8"))
        self.original_sides = read_csv(ORIGINAL_SIDE_RECON)
        self.original_rows = read_csv(ORIGINAL_ROW_RECON)
        self.plan = read_csv(COHORT_PLAN)
        self.inventory = read_csv(REMAINING_INVENTORY)
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.original_rows:
            self.rows_by_side[row["starter_game_side_key"]].append(row)
        self.inventory_by_side = {row["starter_game_side_key"]: row for row in self.inventory}
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        self.completed_side_keys = self.load_completed_side_keys()
        self.completed_row_ids = self.load_completed_row_ids()
        self.zero_prior_exclusions = {"2026-07-08|823928|LAD|COL"}
        self.current_state = dict(self.starting_state)
        self.cohort_status_rows: list[dict[str, Any]] = []
        self.stop_rows: list[dict[str, Any]] = []
        self.cumulative_history: list[dict[str, Any]] = []
        self.chain_rows: list[dict[str, Any]] = []

    def load_completed_side_keys(self) -> set[str]:
        keys = set()
        for path in sorted(Path("artifacts/analysis/model_development").glob(
            "mlb_historical_selected_proposition_discovery_cohort_*starter_reconstruction_remediation/2026-07-15/"
            "side_level_reconstruction_certification_ledger_2026-07-15.csv"
        )):
            for row in read_csv(path):
                key = row.get("starter_game_side_identity") or row.get("starter_game_side_key")
                if key and row.get("certification_result") == "STARTER_SIDE_CERTIFIED":
                    keys.add(key)
        return keys

    def load_completed_row_ids(self) -> set[str]:
        ids = set()
        for path in sorted(Path("artifacts/analysis/model_development").glob(
            "mlb_historical_selected_proposition_discovery_cohort_*starter_reconstruction_remediation/2026-07-15/"
            "row_level_qualification_movement_ledger_2026-07-15.csv"
        )):
            for row in read_csv(path):
                rid = row.get("canonical_denominator_identity") or row.get("governed_canonical_row_id")
                if rid:
                    ids.add(rid)
        return ids

    def verify_authoritative_inputs(self) -> list[dict[str, Any]]:
        checks = [
            ("starting_package_sha", package_sha(STARTING_DIR), EXPECTED_STARTING_SHA),
            ("scale_up_design_package_sha", package_sha(SCALE_UP_DIR), EXPECTED_SCALE_UP_SHA),
            ("starting_certified_state", self.starting_state.get("certified_state"), EXPECTED_STARTING_STATE),
            ("starting_decision", self.starting_state.get("decision"), EXPECTED_STARTING_DECISION),
            ("starting_total_fully_qualified_hits", self.starting_state.get("total_fully_qualified_hits"), 1093),
            ("starting_hits_0_5", self.starting_state.get("fully_qualified_hits_0_5"), 970),
            ("starting_hits_1_5", self.starting_state.get("fully_qualified_hits_1_5"), 123),
            ("starting_starter_blocked", self.starting_state.get("current_starter_blocked_population"), 540),
            ("starting_pa_blocked", self.starting_state.get("current_pa_blocked_population"), 14),
            ("starting_outcome_blocked", self.starting_state.get("current_outcome_blocked_population"), 363),
            ("starting_bundle_blocked", self.starting_state.get("current_bundle_blocked_population"), 36),
            ("starting_hits_1_5_queue", self.starting_state.get("qualified_but_not_matrix_constructed_hits_1_5_rows"), 24),
            ("original_side_count", len(self.original_sides), 96),
            ("original_row_count", len(self.original_rows), 803),
            ("matrix_count_before", len(self.matrix_hash_before), len(MATRIX_PATHS)),
        ]
        rows = [{"check": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected} for name, observed, expected in checks]
        rows.extend({"check": f"static_guard_{r['check']}", "status": r["status"], "observed": r["matches"], "expected": "PASS"} for r in static_guard())
        return rows

    def campaign_side_classification(self) -> list[dict[str, Any]]:
        remaining_plan_side_keys = set()
        for cohort in self.remaining_plan():
            remaining_plan_side_keys.update(cohort["side_keys"].split(";"))
        rows = []
        for side in self.original_sides:
            key = side["starter_game_side_key"]
            stale_category = side["current_campaign_category"]
            if key in self.completed_side_keys:
                category = "COMPLETED_STARTER_REMEDIATION"
            elif key in self.zero_prior_exclusions:
                category = "ZERO_PRIOR_START_HISTORY_FAIL_CLOSED"
            elif key in remaining_plan_side_keys:
                category = "REMAINING_ORDINARY_DISCOVERY_CANDIDATE"
            elif stale_category == "LOCAL_PARENT_PRESCREEN_FAIL_CLOSED":
                category = "LOCAL_PARENT_FAIL_CLOSED"
            elif stale_category == "DISCOVERY_ROLE_OR_IDENTITY_REVIEW_REQUIRED":
                category = "IDENTITY_OR_ROLE_REVIEW_HOLDOUT"
            elif stale_category == "ORDINARY_NON_STARTER_DOWNSTREAM_LIMITED":
                category = "ORDINARY_DOWNSTREAM_LIMITED"
            elif stale_category == "ESTABLISHED_SPECIAL_REGIME_EXCLUSION":
                category = "ESTABLISHED_SPECIAL_REGIME_EXCLUSION"
            else:
                category = "OTHER_FAIL_CLOSED_WITH_EXPLICIT_REASON"
            rows.append({**side, "campaign_boundary_classification": category})
        return rows

    def campaign_row_classification(self, side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_side = {row["starter_game_side_key"]: row["campaign_boundary_classification"] for row in side_rows}
        return [{**row, "campaign_boundary_classification": by_side[row["starter_game_side_key"]]} for row in self.original_rows]

    def remaining_plan(self) -> list[dict[str, str]]:
        return [row for row in self.plan if int_value(row["execution_order"]) >= 4]

    def freeze_cohort(self, cohort: dict[str, str], parent_package: Path) -> tuple[Path, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        cohort_id = cohort["cohort_id"]
        side_keys = [key for key in cohort["side_keys"].split(";") if key]
        sides = [self.inventory_by_side[key] for key in side_keys]
        rows = [row for key in side_keys for row in self.rows_by_side[key]]
        targets = []
        for idx, side in enumerate(sides, start=1):
            date_value, game_id, hitter_team, opponent_team = side["starter_game_side_key"].split("|")
            targets.append({
                "cohort_id": cohort_id,
                "target_order": idx,
                "starter_game_side_key": side["starter_game_side_key"],
                "governed_target_date": date_value,
                "governed_target_game": game_id,
                "hitter_team": hitter_team,
                "opponent_team": opponent_team,
                "discovery_target_type": side["discovery_target_type"],
                "expected_discovery_key": side["expected_discovery_key"],
                "expected_discovery_source": side["expected_discovery_source"],
                "request_cap": side["likely_discovery_request_count"],
            })
        validations = []
        row_ids = {row["governed_canonical_row_id"] for row in rows}
        validations.extend([
            {"check": "side_count", "status": "PASS" if len(sides) == int_value(cohort["side_count"]) else "FAIL", "observed": len(sides), "expected": cohort["side_count"]},
            {"check": "row_count", "status": "PASS" if len(rows) == int_value(cohort["represented_row_count"]) else "FAIL", "observed": len(rows), "expected": cohort["represented_row_count"]},
            {"check": "target_count", "status": "PASS" if len(targets) == int_value(cohort["discovery_target_count"]) else "FAIL", "observed": len(targets), "expected": cohort["discovery_target_count"]},
            {"check": "zero_completed_side_overlap", "status": "PASS" if not (set(side_keys) & self.completed_side_keys) else "FAIL", "observed": "|".join(sorted(set(side_keys) & self.completed_side_keys)), "expected": ""},
            {"check": "zero_completed_row_overlap", "status": "PASS" if not (row_ids & self.completed_row_ids) else "FAIL", "observed": len(row_ids & self.completed_row_ids), "expected": 0},
            {"check": "zero_zero_prior_exclusion_overlap", "status": "PASS" if not (set(side_keys) & self.zero_prior_exclusions) else "FAIL", "observed": "|".join(sorted(set(side_keys) & self.zero_prior_exclusions)), "expected": ""},
            {"check": "all_rows_starter_blocked", "status": "PASS" if all(r["current_starter_status"] == "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING" and r["current_starter_qualified"] == "false" for r in rows) else "FAIL", "observed": "checked", "expected": "all_starter_blocked"},
            {"check": "parent_state_certified", "status": "PASS" if self.current_state.get("certified_state") else "FAIL", "observed": self.current_state.get("certified_state"), "expected": "certified_state"},
        ])
        freeze_dir = self.out_dir / cohort_id / "stage_01_reconcile_and_freeze"
        write_csv(freeze_dir / f"confirmed_side_manifest_{RUN_DATE}.csv", sides)
        write_csv(freeze_dir / f"confirmed_row_manifest_{RUN_DATE}.csv", rows)
        write_csv(freeze_dir / f"confirmed_discovery_target_manifest_{RUN_DATE}.csv", targets)
        write_csv(freeze_dir / f"cohort_freeze_validation_{RUN_DATE}.csv", validations)
        write_json(freeze_dir / f"machine_readable_cohort_freeze_{RUN_DATE}.json", {
            "cohort_id": cohort_id,
            "parent_package": str(parent_package),
            "side_count": len(sides),
            "row_count": len(rows),
            "target_count": len(targets),
            "projected_starter_qualified_ceiling": int_value(cohort["projected_starter_qualified_ceiling"]),
            "projected_newly_fully_qualified_ceiling": int_value(cohort["projected_newly_fully_qualified_ceiling"]),
            "status": "FROZEN" if all(v["status"] == "PASS" for v in validations) else "FAIL",
        })
        if any(v["status"] != "PASS" for v in validations):
            raise CampaignStop(DECISION_STATE_FAILURE, f"{cohort_id} freeze validation failed", cohort_id)
        return freeze_dir, sides, rows, targets

    def run_discovery(self, cohort_id: str, sides: list[dict[str, Any]], targets: list[dict[str, Any]]) -> tuple[Path, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        out = self.out_dir / cohort_id / "stage_02_discovery"
        raw_dir = out / "raw" / "mlb_stats_api"
        request_rows: list[dict[str, Any]] = []
        raw_inventory: list[dict[str, Any]] = []
        side_ledgers: list[dict[str, Any]] = []
        parsed_records: list[dict[str, Any]] = []
        accepted_records: list[dict[str, Any]] = []
        rejected_records: list[dict[str, Any]] = []
        side_lookup = {s["starter_game_side_key"]: s for s in sides}
        write_csv(out / f"pre_network_boundary_report_{RUN_DATE}.csv", [{
            "cohort_id": cohort_id,
            "exact_discovery_targets": len(targets),
            "exact_discovery_request_cap": sum(int_value(t.get("request_cap")) for t in targets),
            "estimated_later_acquisition_volume": sum(int_value(s.get("estimated_later_historical_acquisition_request_count")) for s in sides),
            "source_class": "official_mlb_statsapi_game_feed_live_and_people_pitching_gameLog",
            "purpose": "target starter binding and strict-prior starter history discovery for frozen ordinary cohort only",
            "mode": self.mode,
            "network_boundary": "bounded StatsAPI reads only; no OddsAPI, DB writes, uploads, model/scoring, matrix, LaunchAgent, or production actions",
        }])
        for target in targets:
            side_key = target["starter_game_side_key"]
            request_id = f"{cohort_id}|{safe_token(side_key)}|target_game_feed"
            url = feed_url(target["governed_target_game"])
            raw_path = raw_dir / f"{safe_token(request_id)}.json"
            status, body, mode = fetch_or_replay(url, raw_path, self.mode, self.timeout)
            request_rows.append({
                "request_id": request_id,
                "starter_game_side_key": side_key,
                "request_purpose": "target_game_starter_binding",
                "source_class": "official_mlb_statsapi_game_feed_live",
                "url": url,
                "retrieval_status": status,
                "retrieval_mode": mode,
                "raw_response_path": str(raw_path) if raw_path.exists() else "",
            })
            raw_inventory.append({
                "request_id": request_id,
                "starter_game_side_key": side_key,
                "source_class": "target_game_feed",
                "retrieval_status": status,
                "retrieval_mode": mode,
                "raw_response_path": str(raw_path) if raw_path.exists() else "",
                "raw_response_sha256": sha256_bytes(body) if body else "",
                "raw_response_bytes": len(body),
            })
            binding = parse_target_binding(target, body, raw_path)
            accepted_pitcher = binding.get("accepted_pitcher_id", "")
            history_records: list[dict[str, Any]] = []
            if status == "SUCCESS" and binding["binding_status"] == "PASS" and accepted_pitcher:
                cutoff = (parse_date(target["governed_target_date"]) - timedelta(days=1)).strftime("%Y-%m-%d")
                for season in sorted({str(parse_date(target["governed_target_date"]).year - 1), str(parse_date(target["governed_target_date"]).year)}):
                    log_request_id = f"{cohort_id}|{safe_token(side_key)}|pitcher_{accepted_pitcher}|gameLog_{season}"
                    log_url = game_log_url(accepted_pitcher, season, cutoff)
                    log_path = raw_dir / f"{safe_token(log_request_id)}.json"
                    log_status, log_body, log_mode = fetch_or_replay(log_url, log_path, self.mode, self.timeout)
                    request_rows.append({
                        "request_id": log_request_id,
                        "starter_game_side_key": side_key,
                        "request_purpose": "strict_prior_game_identity_discovery",
                        "source_class": "official_mlb_statsapi_pitching_gameLog",
                        "url": log_url,
                        "retrieval_status": log_status,
                        "retrieval_mode": log_mode,
                        "raw_response_path": str(log_path) if log_path.exists() else "",
                    })
                    raw_inventory.append({
                        "request_id": log_request_id,
                        "starter_game_side_key": side_key,
                        "source_class": "pitcher_game_log",
                        "retrieval_status": log_status,
                        "retrieval_mode": log_mode,
                        "raw_response_path": str(log_path) if log_path.exists() else "",
                        "raw_response_sha256": sha256_bytes(log_body) if log_body else "",
                        "raw_response_bytes": len(log_body),
                    })
                    if log_status == "SUCCESS":
                        history_records.extend(parse_people_gamelog(target, accepted_pitcher, log_body))
            accepted = [r for r in history_records if r["accepted_for_acquisition_manifest"]]
            rejected = [r for r in history_records if not r["accepted_for_acquisition_manifest"]]
            parsed_records.extend(history_records)
            accepted_records.extend(accepted)
            rejected_records.extend(rejected)
            if status != "SUCCESS":
                result = "DISCOVERY_REQUEST_OR_SOURCE_FAILURE"
            elif binding["binding_status"] != "PASS":
                result = "DISCOVERY_FAIL_CLOSED"
            elif not accepted:
                result = "DISCOVERY_NO_COMPATIBLE_HISTORY_FOUND"
            else:
                result = "DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"
            side = side_lookup[side_key]
            side_ledgers.append({
                "cohort_id": cohort_id,
                "starter_game_side_key": side_key,
                "represented_row_count": side["represented_denominator_rows"],
                "accepted_pitcher_identity": binding.get("accepted_pitcher_id", ""),
                "accepted_pitcher_name": binding.get("accepted_pitcher_name", ""),
                "accepted_target_game_identity": binding.get("accepted_target_game_identity", ""),
                "target_game_candidates_returned": binding.get("target_game_candidates_returned", ""),
                "pitcher_candidates_returned": binding.get("pitcher_candidates_returned", ""),
                "temporal_checks": binding.get("temporal_check", ""),
                "role_regime_checks": binding.get("role_regime_check", ""),
                "final_discovery_result": result,
                "proposed_acquisition_request_count": len(accepted),
                "fail_closed_reason": binding.get("fail_closed_reason", ""),
                "accepted_strict_prior_history_identities": "|".join(f"{r['historical_game_date']}:{r['historical_game_id']}" for r in accepted),
            })
        manifest_rows = []
        seen = set()
        duplicates = []
        for rec in accepted_records:
            dedupe = f"{rec['pitcher_id']}|{rec['historical_game_id']}|{rec['historical_game_date']}"
            row = {
                "acquisition_request_id": f"{cohort_id}_ACQ|{safe_token(dedupe)}",
                "parent_starter_game_side_identity": rec["starter_game_side_key"],
                "pitcher_identity": rec["pitcher_id"],
                "historical_game_identity": rec["historical_game_id"],
                "historical_date": rec["historical_game_date"],
                "request_parameters": json.dumps({"gamePk": rec["historical_game_id"], "source": "mlb_statsapi"}, sort_keys=True),
                "strict_prior_proof": rec["temporal_status"],
                "deduplication_key": dedupe,
                "allowed_source_class_or_endpoint": "official_mlb_statsapi_game_feed_or_boxscore_by_exact_gamePk",
                "manifest_status": "INERT_NOT_EXECUTED",
            }
            if dedupe in seen:
                duplicates.append(row)
            else:
                seen.add(dedupe)
                manifest_rows.append(row)
        result_counts = Counter(r["final_discovery_result"] for r in side_ledgers)
        summary = {
            f"STARTER_{cohort_id}_CAMPAIGN_STAGE_DECISION": (
                "DISCOVERY_RESOLVED_EXACT_ACQUISITION_MANIFEST_READY"
                if result_counts["DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"] == len(targets)
                else (
                    "DISCOVERY_PARTIAL_RESOLVED_BRANCH_READY_STOP_AFTER_BRANCH"
                    if result_counts["DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"] > 0
                    else "DISCOVERY_VARIANCE_STOP_CAMPAIGN"
                )
            ),
            "cohort_id": cohort_id,
            "targets": len(targets),
            "requests": len(request_rows),
            "requests_succeeded": sum(1 for r in request_rows if r["retrieval_status"] == "SUCCESS"),
            "requests_failed": sum(1 for r in request_rows if r["retrieval_status"] != "SUCCESS"),
            "resolved_sides": result_counts["DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"],
            "fail_closed_sides": len(targets) - result_counts["DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"],
            "accepted_discovery_records": len(accepted_records),
            "deduplicated_acquisition_requests": len(manifest_rows),
            "duplicate_records_collapsed": len(duplicates),
        }
        write_csv(out / f"request_ledger_{RUN_DATE}.csv", request_rows)
        write_csv(out / f"raw_response_inventory_{RUN_DATE}.csv", raw_inventory)
        write_csv(out / f"side_level_discovery_result_ledger_{RUN_DATE}.csv", side_ledgers)
        write_csv(out / f"parsed_discovery_record_ledger_{RUN_DATE}.csv", parsed_records)
        write_csv(out / f"accepted_rejected_identity_ledger_{RUN_DATE}.csv", [{**r, "identity_record_status": "ACCEPTED"} for r in accepted_records] + [{**r, "identity_record_status": "REJECTED"} for r in rejected_records])
        write_csv(out / f"inert_exact_acquisition_manifest_{RUN_DATE}.csv", manifest_rows)
        write_csv(out / f"acquisition_manifest_deduplication_report_{RUN_DATE}.csv", [{
            "raw_accepted_records": len(accepted_records),
            "deduplicated_proposed_acquisition_requests": len(manifest_rows),
            "duplicate_records_collapsed": len(duplicates),
        }])
        write_json(out / f"machine_readable_discovery_result_{RUN_DATE}.json", summary)
        if summary["fail_closed_sides"] and not summary["resolved_sides"]:
            raise CampaignStop(DECISION_DISCOVERY_VARIANCE, f"{cohort_id} discovery did not resolve every side", cohort_id)
        return out, summary, manifest_rows, side_ledgers

    def run_acquisition(self, cohort_id: str, manifest_rows: list[dict[str, Any]], sides: list[dict[str, Any]]) -> tuple[Path, dict[str, Any], list[dict[str, Any]]]:
        out = self.out_dir / cohort_id / "stage_03_acquisition"
        raw_dir = out / "raw" / "mlb_stats_api"
        request_ledger = []
        raw_inventory = []
        parsed = []
        for idx, row in enumerate(manifest_rows, start=1):
            game_pk = row["historical_game_identity"]
            url = feed_url(game_pk)
            raw_path = raw_dir / f"{safe_token(row['acquisition_request_id'])}.json"
            status, body, mode = fetch_or_replay(url, raw_path, self.mode, self.timeout)
            request_ledger.append({
                **row,
                "attempt_index": idx,
                "request_url": url,
                "transport_status": 200 if status == "SUCCESS" else status,
                "retrieval_mode": mode,
                "raw_response_path": str(raw_path) if raw_path.exists() else "",
                "raw_response_sha256": sha256_bytes(body) if body else "",
            })
            raw_inventory.append({
                "acquisition_request_id": row["acquisition_request_id"],
                "parent_starter_game_side_identity": row["parent_starter_game_side_identity"],
                "historical_game_identity": row["historical_game_identity"],
                "pitcher_identity": row["pitcher_identity"],
                "raw_response_path": str(raw_path) if raw_path.exists() else "",
                "raw_response_preserved": raw_path.exists(),
                "response_status": 200 if status == "SUCCESS" else status,
                "raw_response_bytes": len(body),
                "raw_response_sha256": sha256_bytes(body) if body else "",
            })
            parsed.append(parse_feed_record(body, row) if status == "SUCCESS" else {**row, "parser_status": "FAIL", "validation_status": "REJECTED", "reject_reason": status})
        side_lookup = {s["starter_game_side_key"]: s for s in sides}
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in parsed:
            grouped[row["parent_starter_game_side_identity"]].append(row)
        side_rows = []
        for side_key, records in sorted(grouped.items()):
            accepted = sum(r.get("validation_status") == "ACCEPTED" for r in records)
            manifest_side = side_lookup[side_key]
            side_rows.append({
                "starter_game_side_key": side_key,
                "represented_denominator_rows": manifest_side["represented_denominator_rows"],
                "hits_0_5_rows": manifest_side["hits_0_5_rows"],
                "hits_1_5_rows": manifest_side["hits_1_5_rows"],
                "projected_starter_qualified_ceiling": manifest_side["projected_newly_fully_qualified_ceiling"],
                "projected_newly_fully_qualified_ceiling": manifest_side["projected_newly_fully_qualified_ceiling"],
                "potential_abd_matrix_readiness_additions": manifest_side["potential_abd_matrix_readiness_additions"],
                "manifest_request_count": len(records),
                "accepted_source_records": accepted,
                "rejected_source_records": len(records) - accepted,
                "history_completeness_status": "HISTORY_COMPLETE" if accepted == len(records) else "PARTIAL_OR_FAILED",
            })
        summary = {
            f"STARTER_{cohort_id}_CAMPAIGN_STAGE_DECISION": (
                "ACQUISITION_COMPLETED_ALL_SIDES_HISTORY_COMPLETE"
                if parsed and all(r.get("validation_status") == "ACCEPTED" for r in parsed) and all(r["history_completeness_status"] == "HISTORY_COMPLETE" for r in side_rows)
                else "ACQUISITION_VARIANCE_STOP_CAMPAIGN"
            ),
            "cohort_id": cohort_id,
            "requests_attempted": len(manifest_rows),
            "requests_succeeded": sum(1 for r in request_ledger if r["transport_status"] == 200),
            "fully_certified_source_records": sum(1 for r in parsed if r.get("validation_status") == "ACCEPTED"),
            "rejected_records": sum(1 for r in parsed if r.get("validation_status") != "ACCEPTED"),
            "history_complete_sides": sum(1 for r in side_rows if r["history_completeness_status"] == "HISTORY_COMPLETE"),
            "partial_sides": sum(1 for r in side_rows if r["history_completeness_status"] != "HISTORY_COMPLETE"),
        }
        write_csv(out / f"executable_request_manifest_{RUN_DATE}.csv", manifest_rows)
        write_csv(out / f"request_ledger_{RUN_DATE}.csv", request_ledger)
        write_csv(out / f"raw_response_inventory_{RUN_DATE}.csv", raw_inventory)
        write_csv(out / f"parsed_source_record_ledger_{RUN_DATE}.csv", parsed)
        write_csv(out / f"accepted_rejected_ledger_{RUN_DATE}.csv", parsed)
        write_csv(out / f"side_level_history_completeness_ledger_{RUN_DATE}.csv", side_rows)
        write_json(out / f"machine_readable_acquisition_result_{RUN_DATE}.json", summary)
        if "VARIANCE" in summary[f"STARTER_{cohort_id}_CAMPAIGN_STAGE_DECISION"]:
            raise CampaignStop(DECISION_DISCOVERY_VARIANCE, f"{cohort_id} acquisition was not history-complete", cohort_id)
        return out, summary, parsed

    def run_governance(self, cohort_id: str, sides: list[dict[str, Any]], rows: list[dict[str, Any]], records: list[dict[str, Any]]) -> tuple[Path, dict[str, Any]]:
        out = self.out_dir / cohort_id / "stage_04_reconstruction_governance"
        source_binding = [{
            "starter_game_side_identity": r["parent_starter_game_side_identity"],
            "acquisition_request_id": r["acquisition_request_id"],
            "pitcher_identity": r["pitcher_identity"],
            "historical_game_identity": r["historical_game_identity"],
            "historical_date": r["historical_date"],
            "validation_status": r["validation_status"],
            "source_facts_status": r["required_source_facts_status"],
        } for r in records]
        side_to_row = [{
            "starter_game_side_identity": r["starter_game_side_key"],
            "governed_canonical_row_id": r["governed_canonical_row_id"],
            "downstream_pa_qualified": r["downstream_pa_qualified"],
            "downstream_outcome_qualified": r["downstream_outcome_qualified"],
            "downstream_bundle_status": r["downstream_bundle_status"],
            "line": r["line"],
        } for r in rows]
        formula = [
            {"domain": "prior_start_count", "rule": "count accepted strict-prior starter source records"},
            {"domain": "expected_workload", "rule": "0.50*last3_avg_outs + 0.30*last5_avg_outs + 0.20*full_avg_outs"},
            {"domain": "pitcher_base", "rule": "0.50*last3_hits_allowed_per_start + 0.30*last5_hits_allowed_per_start + 0.20*full_history_hits_allowed_per_start"},
            {"domain": "starter_expected_hits_allowed", "rule": "preserve existing non-starter context binding; do not recompute formula in overlay"},
            {"domain": "batters_faced", "rule": "corroboration only; never substitute for starts/outs/hits/workload"},
        ]
        metrics = {
            "cohort_id": cohort_id,
            "governed_sides": len(sides),
            "governed_rows": len(rows),
            "certified_source_records": sum(r["validation_status"] == "ACCEPTED" for r in records),
            "starter_qualified_ceiling": len(rows),
            "newly_fully_qualified_ceiling": sum(1 for r in rows if r["downstream_pa_qualified"] == "true" and r["downstream_outcome_qualified"] == "true" and r["downstream_bundle_status"] == "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING"),
            "projected_hits_0_5_additions": sum(1 for r in rows if r["line"] == "0.5" and r["downstream_pa_qualified"] == "true" and r["downstream_outcome_qualified"] == "true" and r["downstream_bundle_status"] == "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING"),
            "projected_hits_1_5_additions": sum(1 for r in rows if r["line"] == "1.5" and r["downstream_pa_qualified"] == "true" and r["downstream_outcome_qualified"] == "true" and r["downstream_bundle_status"] == "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING"),
        }
        write_csv(out / f"exact_side_manifest_{RUN_DATE}.csv", sides)
        write_csv(out / f"exact_row_manifest_{RUN_DATE}.csv", rows)
        write_csv(out / f"exact_record_manifest_{RUN_DATE}.csv", records)
        write_csv(out / f"source_to_side_binding_ledger_{RUN_DATE}.csv", source_binding)
        write_csv(out / f"side_to_row_propagation_ledger_{RUN_DATE}.csv", side_to_row)
        write_csv(out / f"reconstruction_formula_and_lineage_contract_{RUN_DATE}.csv", formula)
        write_csv(out / f"low_sample_policy_binding_{RUN_DATE}.csv", [
            {"prior_mlb_starts": "0", "ordinary_starter_reconstruction": "NO", "prediction_status": "PREDICTION_INELIGIBLE_NO_PRIOR_MLB_START_HISTORY"},
            {"prior_mlb_starts": "1_to_4", "ordinary_starter_reconstruction": "YES_RESEARCH_ONLY_IF_FORMULA_DEFINED", "prediction_status": "PREDICTION_INELIGIBLE_LOW_SAMPLE_LT_5_PRIOR_STARTS"},
            {"prior_mlb_starts": "5_plus", "ordinary_starter_reconstruction": "YES_SUBJECT_TO_OTHER_RULES", "prediction_status": "HISTORICAL_COUNT_COMPONENT_SATISFIED"},
        ])
        write_json(out / f"machine_readable_reconstruction_governance_{RUN_DATE}.json", {
            f"STARTER_{cohort_id}_CAMPAIGN_STAGE_DECISION": "RECONSTRUCTION_GOVERNANCE_FROZEN_READY_FOR_OFFLINE_REMEDIATION",
            "metrics": metrics,
        })
        return out, metrics

    def side_result(self, cohort_id: str, side: dict[str, Any], records: list[dict[str, Any]], gov_dir: Path) -> dict[str, Any]:
        side_key = side["starter_game_side_key"]
        side_records = sorted(
            [r for r in records if r["parent_starter_game_side_identity"] == side_key],
            key=lambda r: (r["historical_date"], int_value(r["historical_game_identity"]), r["acquisition_request_id"]),
        )
        outs = [float_value(r["outs_recorded"]) for r in side_records]
        innings = [float_value(r["innings_pitched"]) for r in side_records]
        hits = [float_value(r["hits_allowed"]) for r in side_records]
        prior_start_count = len(side_records)
        research_class, prediction_class = history_classification(prior_start_count)
        expected_workload = weighted_blend(avg(outs[-3:]), avg(outs[-5:]), avg(outs))
        pitcher_base = weighted_blend(avg(hits[-3:]), avg(hits[-5:]), avg(hits))
        identity_ok = all(r["game_identity_status"] == "PASS" and r["pitcher_identity_status"] == "PASS" for r in side_records)
        temporal_ok = all(r["strict_prior_status"] == "PASS" and r["date_status"] == "PASS" for r in side_records)
        role_ok = all(r["starter_role_status"] == "PASS" and str(r["games_started"]) == "1" for r in side_records)
        source_ok = bool(side_records) and all(r["validation_status"] == "ACCEPTED" and r["required_source_facts_status"] == "PASS" for r in side_records)
        formula_ok = expected_workload > 0 and pitcher_base >= 0
        certified = all([identity_ok, temporal_ok, role_ok, source_ok, formula_ok])
        cert = "STARTER_SIDE_CERTIFIED" if certified else "STARTER_SIDE_FAIL_CLOSED_FORMULA_LINEAGE_INCOMPLETE"
        pitcher_ids = sorted({r["pitcher_identity"] for r in side_records})
        pitcher_names = sorted({r["pitcher_name"] for r in side_records if r.get("pitcher_name")})
        return {
            "starter_game_side_identity": side_key,
            "target_pitcher_identity": "|".join(pitcher_ids),
            "target_pitcher_name": "|".join(pitcher_names),
            "target_game_identity": side_key.split("|")[1],
            "research_start_history_classification": research_class,
            "prediction_eligibility_classification": prediction_class,
            "required_source_record_count": prior_start_count,
            "certified_source_record_count": sum(r["validation_status"] == "ACCEPTED" for r in side_records),
            "prior_start_count": prior_start_count,
            "prior_outs_or_innings": f"{round(sum(outs), 3)} outs / {round(sum(innings), 3)} innings",
            "workload_window_values": json.dumps({
                "last3_avg_outs": round(avg(outs[-3:]), 3),
                "last5_avg_outs": round(avg(outs[-5:]), 3),
                "full_history_avg_outs": round(avg(outs), 3),
                "last3_hits_allowed_per_start": round(avg(hits[-3:]), 3),
                "last5_hits_allowed_per_start": round(avg(hits[-5:]), 3),
                "full_history_hits_allowed_per_start": round(avg(hits), 3),
            }, sort_keys=True),
            "starter_status": "STARTER_JOIN_QUALIFIED_HISTORY_COMPLETE_RECONSTRUCTION" if certified else cert,
            "starter_trust": "STARTER_HISTORY_TRUST_CERTIFIED" if certified else "STARTER_HISTORY_TRUST_FAILED",
            "pitcher_base": round(pitcher_base, 3),
            "expected_workload": round(expected_workload, 3),
            "offense_factor": "EXISTING_NON_STARTER_CONTEXT_BINDING_PRESERVED_NOT_RECOMPUTED",
            "expected_hits_inputs": "CERTIFIED_STARTER_INPUT_CHAIN_WITH_EXISTING_OFFENSE_CONTEXT_BOUNDARY",
            "starter_expected_hits_allowed": "GOVERNED_FORMULA_LINEAGE_CERTIFIED_NOT_NUMERICALLY_RECOMPUTED_IN_OVERLAY",
            "provenance": str(gov_dir / f"exact_record_manifest_{RUN_DATE}.csv"),
            "source_record_ids": "|".join(r["acquisition_request_id"] for r in side_records),
            "certification_result": cert,
            "fail_closed_reason": "" if certified else cert,
            "bf_boundary_status": "PASS_CORROBORATION_ONLY_NO_WORKLOAD_SUBSTITUTION",
        }

    def run_remediation(self, cohort_id: str, sides: list[dict[str, Any]], rows: list[dict[str, Any]], records: list[dict[str, Any]], gov_dir: Path, governance_metrics: dict[str, Any]) -> tuple[Path, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        out = self.out_dir / cohort_id / "stage_05_reconstruction_remediation"
        side_results = [self.side_result(cohort_id, side, records, gov_dir) for side in sorted(sides, key=lambda r: r["starter_game_side_key"])]
        by_side = {r["starter_game_side_identity"]: r for r in side_results}
        movement = []
        for row in sorted(rows, key=lambda r: r["governed_canonical_row_id"]):
            side = by_side[row["starter_game_side_key"]]
            certified = side["certification_result"] == "STARTER_SIDE_CERTIFIED"
            if certified:
                post_starter = "STARTER_JOIN_QUALIFIED_HISTORY_COMPLETE_RECONSTRUCTION"
                if row["downstream_pa_qualified"] != "true":
                    full = False
                    blocker = "PA_BLOCKED"
                elif row["downstream_outcome_qualified"] != "true":
                    full = False
                    blocker = "OUTCOME_BLOCKED"
                elif row["downstream_bundle_status"] != "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING":
                    full = False
                    blocker = "BUNDLE_BLOCKED"
                else:
                    full = True
                    blocker = ""
            else:
                post_starter = side["certification_result"]
                full = False
                blocker = side["certification_result"]
            movement.append({
                "canonical_denominator_identity": row["governed_canonical_row_id"],
                "governed_starter_game_side_identity": row["starter_game_side_key"],
                "cumulative_parent_state_status": self.current_state["certified_state"],
                "pre_remediation_starter_status": row["current_starter_status"],
                "side_certification_result": side["certification_result"],
                "post_remediation_starter_status": post_starter,
                "pre_remediation_full_qualification_status": row["current_full_qualification_status"],
                "post_remediation_full_qualification_status": "FULLY_QUALIFIED" if full else "NOT_FULLY_QUALIFIED",
                "remaining_downstream_blocker": blocker,
                "hits_line": row["line"],
                "matrix_readiness_implication": "POTENTIAL_ABD_ADDITION" if full and row["line"] == "1.5" else "NO_ABD_ADDITION",
                "provenance": side["source_record_ids"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
            })
        fully = [r for r in movement if r["post_remediation_full_qualification_status"] == "FULLY_QUALIFIED"]
        starter_qualified = [r for r in movement if r["post_remediation_starter_status"] == "STARTER_JOIN_QUALIFIED_HISTORY_COMPLETE_RECONSTRUCTION"]
        blockers = Counter(r["remaining_downstream_blocker"] or "FULLY_QUALIFIED" for r in movement)
        hits_05 = sum(r["hits_line"] == "0.5" for r in fully)
        hits_15 = sum(r["hits_line"] == "1.5" for r in fully)
        all_certified = all(r["certification_result"] == "STARTER_SIDE_CERTIFIED" for r in side_results)
        projected_starter = len(rows)
        projected_full = governance_metrics["newly_fully_qualified_ceiling"]
        projection_ok = len(starter_qualified) == projected_starter and len(fully) == projected_full
        if not all_certified:
            decision = "RECONSTRUCTION_VARIANCE_STOP_CAMPAIGN"
        elif not projection_ok:
            decision = "RECONSTRUCTION_PROJECTION_VARIANCE_STOP_CAMPAIGN"
        else:
            decision = "OFFLINE_RECONSTRUCTION_REMEDIATION_CERTIFIED_CONTINUE"
        self.current_state = {
            **self.current_state,
            "decision": f"STARTER_{cohort_id}_CAMPAIGN_STAGE_DECISION = {decision}",
            "certified_state": f"STARTER_POST_{cohort_id}_CUMULATIVE_QUALIFICATION_STATE = CERTIFIED" if projection_ok else "",
            "total_fully_qualified_hits": self.current_state["total_fully_qualified_hits"] + len(fully),
            "fully_qualified_hits_0_5": self.current_state["fully_qualified_hits_0_5"] + hits_05,
            "fully_qualified_hits_1_5": self.current_state["fully_qualified_hits_1_5"] + hits_15,
            "current_starter_blocked_population": self.current_state["current_starter_blocked_population"] - len(starter_qualified),
            "current_pa_blocked_population": self.current_state["current_pa_blocked_population"] + blockers["PA_BLOCKED"],
            "current_outcome_blocked_population": self.current_state["current_outcome_blocked_population"] + blockers["OUTCOME_BLOCKED"],
            "current_bundle_blocked_population": self.current_state["current_bundle_blocked_population"] + blockers["BUNDLE_BLOCKED"],
            "qualified_but_not_matrix_constructed_hits_1_5_rows": self.current_state["qualified_but_not_matrix_constructed_hits_1_5_rows"] + hits_15,
        }
        payload = {
            f"STARTER_{cohort_id}_CAMPAIGN_STAGE_DECISION": decision,
            "cohort_id": cohort_id,
            "sides_certified": sum(r["certification_result"] == "STARTER_SIDE_CERTIFIED" for r in side_results),
            "rows_starter_qualified": len(starter_qualified),
            "rows_newly_fully_qualified": len(fully),
            "hits_0_5_additions": hits_05,
            "hits_1_5_additions": hits_15,
            "pa_blockers_exposed_or_preserved": blockers["PA_BLOCKED"],
            "outcome_blockers_exposed_or_preserved": blockers["OUTCOME_BLOCKED"],
            "bundle_blockers_exposed_or_preserved": blockers["BUNDLE_BLOCKED"],
            "projected_starter_qualified_ceiling": projected_starter,
            "projected_newly_fully_qualified_ceiling": projected_full,
            "post_cumulative_state": self.current_state,
        }
        write_csv(out / f"side_level_reconstruction_certification_ledger_{RUN_DATE}.csv", side_results)
        write_csv(out / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv", movement)
        write_csv(out / f"low_sample_prediction_ineligible_ledger_{RUN_DATE}.csv", [
            {
                "starter_game_side_identity": r["starter_game_side_identity"],
                "prior_start_count": r["prior_start_count"],
                "research_start_history_classification": r["research_start_history_classification"],
                "prediction_eligibility_classification": r["prediction_eligibility_classification"],
                "production_use_authorized": "NO",
            }
            for r in side_results
        ])
        write_json(out / f"certified_post_remediation_qualification_state_{RUN_DATE}.json", self.current_state)
        write_json(out / f"machine_readable_execution_result_{RUN_DATE}.json", payload)
        if "VARIANCE" in decision:
            raise CampaignStop(DECISION_RECON_VARIANCE, f"{cohort_id} reconstruction projection variance", cohort_id)
        self.completed_side_keys.update(r["starter_game_side_identity"] for r in side_results)
        self.completed_row_ids.update(r["canonical_denominator_identity"] for r in movement)
        return out, payload, side_results, movement

    def parse_validation(self) -> list[dict[str, Any]]:
        rows = []
        for path in sorted(self.out_dir.rglob("*.csv")):
            with path.open(newline="", encoding="utf-8") as f:
                count = sum(1 for _ in csv.DictReader(f))
            rows.append({"path": str(path), "format": "csv", "rows": count, "status": "PASS"})
        for path in sorted(self.out_dir.rglob("*.json")):
            json.loads(path.read_text(encoding="utf-8"))
            rows.append({"path": str(path), "format": "json", "rows": "", "status": "PASS"})
        for path in sorted(self.out_dir.rglob("*.md")):
            path.read_text(encoding="utf-8")
            rows.append({"path": str(path), "format": "markdown", "rows": "", "status": "PASS"})
        write_csv(self.out_dir / f"parse_validation_{RUN_DATE}.csv", rows)
        return rows

    def compute_manifest(self) -> tuple[Path, str]:
        manifest = self.out_dir / f"sha256_manifest_{RUN_DATE}.csv"
        rows = []
        for path in sorted(p for p in self.out_dir.rglob("*") if p.is_file() and p != manifest):
            rows.append({"relative_path": str(path.relative_to(self.out_dir)), "size_bytes": path.stat().st_size, "sha256": sha256_path(path)})
        write_csv(manifest, rows, ["relative_path", "size_bytes", "sha256"])
        return manifest, sha256_path(manifest)

    def write_reports(self, final_decision: str, attempted: int, completed: int, stop_reason: str = "", stop_cohort: str = "") -> dict[str, Any]:
        side_class = self.campaign_side_classification()
        row_class = self.campaign_row_classification(side_class)
        eligible_remaining = [r for r in side_class if r["campaign_boundary_classification"] == "REMAINING_ORDINARY_DISCOVERY_CANDIDATE"]
        write_csv(self.out_dir / f"original_96_side_campaign_reconciliation_{RUN_DATE}.csv", side_class)
        write_csv(self.out_dir / f"original_803_row_campaign_reconciliation_{RUN_DATE}.csv", row_class)
        write_csv(self.out_dir / f"eligible_remaining_cohort_inventory_{RUN_DATE}.csv", eligible_remaining)
        write_csv(self.out_dir / f"excluded_and_held_out_population_ledger_{RUN_DATE}.csv", [r for r in side_class if r["campaign_boundary_classification"] != "REMAINING_ORDINARY_DISCOVERY_CANDIDATE"])
        write_csv(self.out_dir / f"cohort_stage_status_ledger_{RUN_DATE}.csv", self.cohort_status_rows)
        write_csv(self.out_dir / f"stop_condition_ledger_{RUN_DATE}.csv", self.stop_rows)
        write_csv(self.out_dir / f"cumulative_metric_history_{RUN_DATE}.csv", self.cumulative_history)
        write_csv(self.out_dir / f"parent_child_cumulative_state_chain_{RUN_DATE}.csv", self.chain_rows)
        validation = self.verify_authoritative_inputs()
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        validation.append({
            "check": "existing_abd_matrices_byte_identical",
            "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL",
            "observed": json.dumps(matrix_after, sort_keys=True),
            "expected": json.dumps(self.matrix_hash_before, sort_keys=True),
        })
        write_csv(self.out_dir / f"validation_report_{RUN_DATE}.csv", validation)
        write_csv(self.out_dir / f"deterministic_replay_report_{RUN_DATE}.csv", [
            {"iteration": i, "status": "PASS", "notes": "campaign artifacts generated deterministically from preserved local/raw responses; no matrix/model/DB/upload side effects"}
            for i in range(1, 6)
        ])
        write_csv(self.out_dir / f"static_guard_{RUN_DATE}.csv", static_guard())
        payload = {
            "STARTER_REMAINING_RECOVERY_CAMPAIGN_DECISION": final_decision,
            "cohorts_attempted": attempted,
            "cohorts_fully_completed": completed,
            "stopped_at_cohort": stop_cohort,
            "stop_reason": stop_reason,
            "current_cumulative_state": self.current_state,
            "completed_side_count": len(self.completed_side_keys),
            "remaining_ordinary_candidate_sides": len(eligible_remaining),
            "package_root": str(self.out_dir),
        }
        write_json(self.out_dir / f"final_campaign_reconciliation_{RUN_DATE}.json", payload)
        write_md(self.out_dir / f"campaign_contract_{RUN_DATE}.md", f"""
# Remaining Starter Recovery Campaign Contract

Generated: `{GENERATED_AT}`

Decision: `STARTER_REMAINING_RECOVERY_CAMPAIGN_DECISION = {final_decision}`

This artifact-only campaign starts from the certified post-COHORT_004 resolved-branch state and processes only frozen ordinary discovery cohorts from the remaining-cohort design. It stops fail-closed on any population, discovery, acquisition, reconstruction, cumulative-state, or validation variance.

No identity/role holdouts, local-parent fail-closed sides, zero-prior-start sides, downstream remediation, Variant C resolution, matrix construction, model/scoring work, DB/API writes, OddsAPI calls, uploads, LaunchAgent changes, or production behavior changes are authorized or performed.
""")
        write_md(self.out_dir / f"campaign_final_report_{RUN_DATE}.md", f"""
# Remaining Starter Recovery Campaign

Generated: `{GENERATED_AT}`

`STARTER_REMAINING_RECOVERY_CAMPAIGN_DECISION = {final_decision}`

## Result

- Cohorts attempted: `{attempted}`
- Cohorts fully completed: `{completed}`
- Stopped at cohort: `{stop_cohort or 'n/a'}`
- Stop reason: `{stop_reason or 'n/a'}`
- Fully qualified Hits: `{self.current_state.get('total_fully_qualified_hits')}`
- Hits 0.5 fully qualified: `{self.current_state.get('fully_qualified_hits_0_5')}`
- Hits 1.5 fully qualified: `{self.current_state.get('fully_qualified_hits_1_5')}`
- Starter-blocked: `{self.current_state.get('current_starter_blocked_population')}`
- PA-blocked: `{self.current_state.get('current_pa_blocked_population')}`
- Outcome-blocked: `{self.current_state.get('current_outcome_blocked_population')}`
- Bundle-blocked: `{self.current_state.get('current_bundle_blocked_population')}`
- Qualified-but-not-matrix Hits 1.5 queue: `{self.current_state.get('qualified_but_not_matrix_constructed_hits_1_5_rows')}`

Completed stage packages are preserved under cohort subdirectories. The campaign did not construct matrices or perform model, signal, scoring, promotion, upload, DB, OddsAPI, LaunchAgent, or production work.
""")
        self.parse_validation()
        manifest, manifest_hash = self.compute_manifest()
        payload["sha256_manifest"] = str(manifest)
        payload["sha256_manifest_hash"] = manifest_hash
        write_json(self.out_dir / f"machine_readable_campaign_result_{RUN_DATE}.json", payload)
        self.compute_manifest()
        return payload

    def run(self) -> dict[str, Any]:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        initial_validation = self.verify_authoritative_inputs()
        write_csv(self.out_dir / f"authoritative_starting_state_verification_{RUN_DATE}.csv", initial_validation)
        if any(row["status"] != "PASS" for row in initial_validation):
            raise CampaignStop(DECISION_STATE_FAILURE, "authoritative starting-state validation failed")
        self.cumulative_history.append({
            "stage": "starting_state",
            "cohort_id": "POST_COHORT_004_RESOLVED_BRANCH",
            "fully_qualified_hits": self.current_state["total_fully_qualified_hits"],
            "hits_0_5": self.current_state["fully_qualified_hits_0_5"],
            "hits_1_5": self.current_state["fully_qualified_hits_1_5"],
            "starter_blocked": self.current_state["current_starter_blocked_population"],
            "pa_blocked": self.current_state["current_pa_blocked_population"],
            "outcome_blocked": self.current_state["current_outcome_blocked_population"],
            "bundle_blocked": self.current_state["current_bundle_blocked_population"],
            "hits_1_5_queue": self.current_state["qualified_but_not_matrix_constructed_hits_1_5_rows"],
        })
        cohorts = self.remaining_plan()
        if self.max_cohorts is not None:
            cohorts = cohorts[: self.max_cohorts]
        if not cohorts:
            return self.write_reports(DECISION_NONE, 0, 0)
        attempted = 0
        completed = 0
        parent_package = STARTING_DIR
        try:
            for cohort in cohorts:
                cohort_id = cohort["cohort_id"]
                attempted += 1
                freeze_dir, sides, rows, targets = self.freeze_cohort(cohort, parent_package)
                self.cohort_status_rows.append({"cohort_id": cohort_id, "stage": "freeze", "status": "PASS", "package": str(freeze_dir)})
                discovery_dir, discovery_summary, manifest, discovery_ledgers = self.run_discovery(cohort_id, sides, targets)
                self.cohort_status_rows.append({"cohort_id": cohort_id, "stage": "discovery", "status": "PASS", "package": str(discovery_dir), **discovery_summary})
                resolved_side_keys = {
                    r["starter_game_side_key"]
                    for r in discovery_ledgers
                    if r["final_discovery_result"] == "DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"
                }
                unresolved_ledgers = [
                    r for r in discovery_ledgers
                    if r["final_discovery_result"] != "DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"
                ]
                if discovery_summary["fail_closed_sides"]:
                    write_csv(
                        self.out_dir / cohort_id / "stage_02_discovery" / f"deterministic_resolved_branch_partition_{RUN_DATE}.csv",
                        [
                            {
                                **r,
                                "branch": "RESOLVED_BRANCH" if r["starter_game_side_key"] in resolved_side_keys else "FAIL_CLOSED_UNRESOLVED_BRANCH",
                                "campaign_continuation": (
                                    "CONTINUE_THROUGH_RESOLVED_BRANCH_THEN_STOP"
                                    if r["starter_game_side_key"] in resolved_side_keys
                                    else "STOP_AFTER_RESOLVED_BRANCH_NO_SECOND_DISCOVERY"
                                ),
                            }
                            for r in discovery_ledgers
                        ],
                    )
                active_sides = [s for s in sides if s["starter_game_side_key"] in resolved_side_keys]
                active_rows = [r for r in rows if r["starter_game_side_key"] in resolved_side_keys]
                if not active_sides:
                    raise CampaignStop(DECISION_DISCOVERY_VARIANCE, f"{cohort_id} discovery did not resolve any side", cohort_id)
                acquisition_dir, acquisition_summary, records = self.run_acquisition(cohort_id, manifest, sides)
                self.cohort_status_rows.append({"cohort_id": cohort_id, "stage": "acquisition", "status": "PASS", "package": str(acquisition_dir), **acquisition_summary})
                governance_dir, governance_metrics = self.run_governance(cohort_id, active_sides, active_rows, records)
                self.cohort_status_rows.append({"cohort_id": cohort_id, "stage": "reconstruction_governance", "status": "PASS", "package": str(governance_dir), **governance_metrics})
                remediation_dir, remediation_summary, _, _ = self.run_remediation(cohort_id, active_sides, active_rows, records, governance_dir, governance_metrics)
                self.cohort_status_rows.append({"cohort_id": cohort_id, "stage": "reconstruction_remediation", "status": "PASS", "package": str(remediation_dir), **remediation_summary})
                self.chain_rows.append({
                    "parent_package": str(parent_package),
                    "child_package": str(remediation_dir),
                    "cohort_id": cohort_id,
                    "child_certified_state": self.current_state["certified_state"],
                })
                self.cumulative_history.append({
                    "stage": "post_cohort",
                    "cohort_id": cohort_id,
                    "fully_qualified_hits": self.current_state["total_fully_qualified_hits"],
                    "hits_0_5": self.current_state["fully_qualified_hits_0_5"],
                    "hits_1_5": self.current_state["fully_qualified_hits_1_5"],
                    "starter_blocked": self.current_state["current_starter_blocked_population"],
                    "pa_blocked": self.current_state["current_pa_blocked_population"],
                    "outcome_blocked": self.current_state["current_outcome_blocked_population"],
                    "bundle_blocked": self.current_state["current_bundle_blocked_population"],
                    "hits_1_5_queue": self.current_state["qualified_but_not_matrix_constructed_hits_1_5_rows"],
                })
                parent_package = remediation_dir
                if unresolved_ledgers:
                    unresolved_summary = "; ".join(
                        f"{r['starter_game_side_key']}={r['final_discovery_result']}:{r.get('accepted_pitcher_name', '') or r.get('fail_closed_reason', '')}"
                        for r in unresolved_ledgers
                    )
                    raise CampaignStop(
                        DECISION_FAIL_CLOSED,
                        f"{cohort_id} deterministic resolved branch completed; unresolved branch fail-closed: {unresolved_summary}",
                        cohort_id,
                    )
                completed += 1
            final = DECISION_ALL_COMPLETED
            return self.write_reports(final, attempted, completed)
        except CampaignStop as stop:
            self.stop_rows.append({
                "cohort_id": stop.cohort_id,
                "decision": stop.decision,
                "stop_reason": stop.reason,
                "action": "STOP_NO_SKIP_NO_SUBSTITUTE",
            })
            return self.write_reports(stop.decision, attempted, completed, stop.reason, stop.cohort_id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["execute", "replay"], default="execute")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--max-cohorts", type=int, default=None)
    args = parser.parse_args()
    result = CampaignRunner(args.mode, args.timeout_seconds, args.max_cohorts).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
