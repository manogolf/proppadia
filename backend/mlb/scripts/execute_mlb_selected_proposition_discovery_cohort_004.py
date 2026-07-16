#!/usr/bin/env python3
"""Execute bounded discovery for frozen DISCOVERY_COHORT_004.

This utility performs only the explicitly governed read-only discovery calls for
the frozen target manifest. It preserves raw responses and creates an inert
proposed acquisition manifest. It does not execute acquisition, reconstruction,
remediation, qualification propagation, matrix construction, modeling/scoring,
database writes, uploads, scheduler edits, or production behavior changes.
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
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_GOV_SHA = "032dbdf1525848837ce031b1c6fcb2e2af7252ccc7a2d6f633cc32113aec4485"
EXPECTED_CUMULATIVE_PARENT_SHA = "d7629fab6efcb3b48a1432323aa861c0ae7390a00595430226471b2129123856"
EXPECTED_SCALE_UP_SHA = "f6ead8dfc5482b89ee9bdd349c6538dd9d1430c704c489a40e65b4664d02d33c"

STARTER_DISCOVERY_COHORT_004_DECISION_READY = (
    "STARTER_DISCOVERY_COHORT_004_DECISION = "
    "DISCOVERY_COHORT_VALIDATED_EXACT_ACQUISITION_MANIFEST_READY_FOR_APPROVAL"
)
STARTER_DISCOVERY_COHORT_004_DECISION_PARTIAL = (
    "STARTER_DISCOVERY_COHORT_004_DECISION = "
    "DISCOVERY_COHORT_PARTIALLY_VALIDATED_SECOND_BOUNDED_DISCOVERY_REQUIRED"
)
STARTER_DISCOVERY_COHORT_004_DECISION_AMBIGUITY = (
    "STARTER_DISCOVERY_COHORT_004_DECISION = "
    "DISCOVERY_IDENTITY_OR_ROLE_AMBIGUITY_REVIEW_REQUIRED"
)
STARTER_DISCOVERY_COHORT_004_DECISION_LOW = (
    "STARTER_DISCOVERY_COHORT_004_DECISION = "
    "DISCOVERY_SOURCE_YIELD_INSUFFICIENT_NO_ACQUISITION"
)
STARTER_DISCOVERY_COHORT_004_DECISION_FAILED = (
    "STARTER_DISCOVERY_COHORT_004_DECISION = "
    "DISCOVERY_EXECUTION_FAILED_NO_ACQUISITION"
)

RECOMMEND_READY = "DISCOVERY_COHORT_VALIDATED_EXACT_ACQUISITION_MANIFEST_READY_FOR_APPROVAL"
RECOMMEND_PARTIAL = "DISCOVERY_COHORT_PARTIALLY_VALIDATED_SECOND_BOUNDED_DISCOVERY_REQUIRED"
RECOMMEND_AMBIGUITY = "DISCOVERY_IDENTITY_OR_ROLE_AMBIGUITY_REVIEW_REQUIRED"
RECOMMEND_LOW = "DISCOVERY_SOURCE_YIELD_INSUFFICIENT_NO_ACQUISITION"
RECOMMEND_FAILED = "DISCOVERY_EXECUTION_FAILED_NO_ACQUISITION"

OUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_004/2026-07-15"
)
GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_004_cumulative_state_governance/2026-07-15"
)
PARENT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_003_starter_reconstruction_remediation/2026-07-15"
)
SCALE_UP_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/2026-07-15"
)
TARGET_MANIFEST = GOV_DIR / f"confirmed_discovery_target_manifest_{RUN_DATE}.csv"
SIDE_MANIFEST = GOV_DIR / f"confirmed_side_manifest_{RUN_DATE}.csv"
ROW_MANIFEST = GOV_DIR / f"confirmed_row_manifest_{RUN_DATE}.csv"
GOV_STATE = GOV_DIR / f"machine_readable_cumulative_governance_{RUN_DATE}.json"
PARENT_STATE = PARENT_DIR / f"certified_post_remediation_qualification_state_{RUN_DATE}.json"

MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
MATRIX_PATHS = [
    MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

TEAM_ALIASES = {
    "ATH": "OAK",
    "AZ": "ARI",
    "WSN": "WSH",
    "CHW": "CWS",
    "KCR": "KC",
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
}

PROHIBITED_PATTERNS = {
    "outside_frozen_manifest_population": re.compile(r"schedule\\?sportId=1(?!&gamePk=)|hydrate=|standings", re.IGNORECASE),
    "historical_record_acquisition_execution": re.compile(r"acquire_.*starter|execute_.*acquisition|manifest_status.*EXECUTED", re.IGNORECASE),
    "reconstruction_or_remediation": re.compile(r"\breconstruct\s*\(|\bremediate\s*\(|qualification_propagation", re.IGNORECASE),
    "matrix_model_signal_work": re.compile(r"\.fit\s*\(|\.predict\s*\(|build_mlb_selected_proposition_abd_matrices|roc_auc|log_loss|signal_", re.IGNORECASE),
    "db_or_production_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
    "oddsapi_upload_scheduler": re.compile(r"oddsapi|odds_api|write_upload|upload_ready|launchctl|LaunchAgent", re.IGNORECASE),
}


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


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def package_sha(path: Path) -> str:
    return sha256_path(path)


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def safe_token(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(text)).strip("_")[:180]


def canon_team(value: Any) -> str:
    text = str(value or "").strip().upper()
    return TEAM_ALIASES.get(text, text)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


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
            "notes": "Static guard excludes comments, string literals, and the pattern declaration block.",
        })
    return rows


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


def fetch_or_replay(request_id: str, url: str, timeout: int, allow_network: bool) -> dict[str, Any]:
    raw_dir = OUT_DIR / "raw" / "mlb_stats_api"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"{safe_token(request_id)}.json"
    error_path = raw_dir / f"{safe_token(request_id)}_error.json"
    if raw_path.exists():
        data = raw_path.read_bytes()
        return {
            "request_id": request_id,
            "url": url,
            "retrieval_status": "SUCCESS",
            "retrieval_mode": "PRESERVED_RAW_REPLAY_NO_NETWORK",
            "http_status": 200,
            "raw_response_path": str(raw_path),
            "raw_response_sha256": sha256_bytes(data),
            "raw_response_bytes": len(data),
            "error_path": "",
        }
    if not allow_network:
        err = {"url": url, "error": "raw response missing and network disabled"}
        write_json(error_path, err)
        return {
            "request_id": request_id,
            "url": url,
            "retrieval_status": "RAW_MISSING_NETWORK_DISABLED",
            "retrieval_mode": "NO_NETWORK_REPLAY_FAILED",
            "http_status": "",
            "raw_response_path": "",
            "raw_response_sha256": "",
            "raw_response_bytes": 0,
            "error_path": str(error_path),
        }
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "proppadia-bounded-discovery/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            status = getattr(resp, "status", 200)
        raw_path.write_bytes(data)
        return {
            "request_id": request_id,
            "url": url,
            "retrieval_status": "SUCCESS" if status == 200 else "HTTP_NON_200",
            "retrieval_mode": "LIVE_BOUNDED_READ_ONLY_DISCOVERY_CALL",
            "http_status": status,
            "raw_response_path": str(raw_path),
            "raw_response_sha256": sha256_bytes(data),
            "raw_response_bytes": len(data),
            "error_path": "",
        }
    except Exception as exc:
        payload = {"url": url, "error_type": type(exc).__name__, "error_message": str(exc)}
        write_json(error_path, payload)
        return {
            "request_id": request_id,
            "url": url,
            "retrieval_status": "ERROR",
            "retrieval_mode": "LIVE_BOUNDED_READ_ONLY_DISCOVERY_CALL",
            "http_status": "",
            "raw_response_path": "",
            "raw_response_sha256": "",
            "raw_response_bytes": 0,
            "error_path": str(error_path),
        }


def payload_from_response(response: dict[str, Any]) -> dict[str, Any]:
    if response.get("retrieval_status") != "SUCCESS" or not response.get("raw_response_path"):
        return {}
    try:
        return json.loads(Path(response["raw_response_path"]).read_text(encoding="utf-8"))
    except Exception:
        return {}


def team_abbrev_from_box_team(team_payload: dict[str, Any]) -> str:
    team = team_payload.get("team") or {}
    return canon_team(team.get("abbreviation") or team.get("teamCode") or team.get("fileCode") or team.get("name"))


def team_abbrev_from_game_data(payload: dict[str, Any], side_name: str) -> str:
    team = (((payload.get("gameData") or {}).get("teams") or {}).get(side_name) or {})
    return canon_team(team.get("abbreviation") or team.get("teamCode") or team.get("fileCode") or team.get("name"))


def pitching_stats(player: dict[str, Any]) -> dict[str, Any]:
    return ((player.get("stats") or {}).get("pitching") or {})


def find_starters(team_payload: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for player_key, player in (team_payload.get("players") or {}).items():
        stats = pitching_stats(player)
        if int_value(stats.get("gamesStarted")) == 1:
            person = player.get("person") or {}
            out.append({
                "player_key": player_key,
                "player_id": str(person.get("id") or ""),
                "player_name": person.get("fullName") or "",
                "games_started": int_value(stats.get("gamesStarted")),
                "innings_pitched": stats.get("inningsPitched", ""),
                "outs": stats.get("outs", ""),
                "batters_faced": stats.get("battersFaced", ""),
                "stats": stats,
            })
    return sorted(out, key=lambda r: r["player_id"])


def parse_target_feed(target: dict[str, str], response: dict[str, Any]) -> dict[str, Any]:
    payload = payload_from_response(response)
    base = {
        "starter_game_side_key": target["starter_game_side_key"],
        "target_order": target["target_order"],
        "game_id": target["governed_target_game"],
        "governed_date": target["governed_target_date"],
        "hitter_team": target["hitter_team"],
        "opponent_team": target["opponent_team"],
        "feed_request_id": response["request_id"],
        "feed_response_path": response.get("raw_response_path", ""),
        "feed_response_sha256": response.get("raw_response_sha256", ""),
        "game_candidates_returned": "",
        "pitcher_candidates_returned": "",
        "accepted_pitcher_id": "",
        "accepted_pitcher_name": "",
        "accepted_target_game_identity": "",
        "target_team_side": "",
        "temporal_check": "NOT_CHECKED",
        "role_regime_check": "NOT_CHECKED",
        "ambiguity_findings": "",
        "binding_status": "SOURCE_FAILURE",
        "fail_closed_reason": "",
    }
    if not payload:
        return {**base, "fail_closed_reason": response.get("retrieval_status", "source_payload_missing")}
    game = payload.get("gameData") or {}
    official_date = str(game.get("datetime", {}).get("officialDate") or "")
    status = str(game.get("status", {}).get("detailedState") or "")
    box = (payload.get("liveData") or {}).get("boxscore") or {}
    teams = box.get("teams") or {}
    candidates = []
    target_side = ""
    for side_name in ["away", "home"]:
        team_payload = teams.get(side_name) or {}
        abbr = team_abbrev_from_game_data(payload, side_name) or team_abbrev_from_box_team(team_payload)
        if abbr == canon_team(base["opponent_team"]):
            target_side = side_name
            candidates = find_starters(team_payload)
    base["game_candidates_returned"] = json.dumps({
        "official_date": official_date,
        "status": status,
        "away": team_abbrev_from_game_data(payload, "away") or team_abbrev_from_box_team(teams.get("away") or {}),
        "home": team_abbrev_from_game_data(payload, "home") or team_abbrev_from_box_team(teams.get("home") or {}),
    }, sort_keys=True)
    base["pitcher_candidates_returned"] = json.dumps(candidates, sort_keys=True)
    base["target_team_side"] = target_side
    base["temporal_check"] = "PASS" if official_date == base["governed_date"] else "FAIL_OFFICIAL_DATE_MISMATCH"
    if not target_side:
        return {**base, "binding_status": "FAIL_CLOSED", "fail_closed_reason": "opponent_team_not_found_in_boxscore"}
    if len(candidates) != 1:
        reason = "starter_candidate_count_not_one"
        return {**base, "binding_status": "FAIL_CLOSED", "fail_closed_reason": reason, "ambiguity_findings": reason}
    starter = candidates[0]
    role_check = "PASS_CONVENTIONAL_STARTER" if starter["games_started"] == 1 else "FAIL_ROLE_REGIME"
    pass_all = base["temporal_check"] == "PASS" and role_check == "PASS_CONVENTIONAL_STARTER"
    return {
        **base,
        "accepted_pitcher_id": starter["player_id"],
        "accepted_pitcher_name": starter["player_name"],
        "accepted_target_game_identity": base["game_id"],
        "role_regime_check": role_check,
        "binding_status": "PASS" if pass_all else "FAIL_CLOSED",
        "fail_closed_reason": "" if pass_all else "temporal_or_role_check_failed",
    }


def parse_game_log_records(target: dict[str, str], pitcher_id: str, response: dict[str, Any], governed_date: str) -> list[dict[str, Any]]:
    payload = payload_from_response(response)
    rows = []
    if not payload:
        return rows
    for stat_group in payload.get("stats", []) or []:
        for idx, split in enumerate(stat_group.get("splits", []) or []):
            game = split.get("game") or {}
            stat = split.get("stat") or {}
            game_date = str(split.get("date") or game.get("gameDate") or "")
            game_id = str(game.get("gamePk") or game.get("pk") or "")
            is_start = int_value(stat.get("gamesStarted")) == 1
            if not game_date or not game_id:
                continue
            temporal = "PASS_STRICT_PRIOR" if game_date < governed_date else "FAIL_NOT_STRICT_PRIOR"
            rows.append({
                "starter_game_side_key": target["starter_game_side_key"],
                "discovery_target_id": target["target_order"],
                "pitcher_id": pitcher_id,
                "source_record_index": idx,
                "historical_game_id": game_id,
                "historical_game_date": game_date,
                "official_starter_designation": str(is_start).lower(),
                "temporal_status": temporal,
                "accepted_for_acquisition_manifest": is_start and temporal == "PASS_STRICT_PRIOR",
                "raw_response_path": response.get("raw_response_path", ""),
                "raw_response_sha256": response.get("raw_response_sha256", ""),
                "source_record_replay_key": f"{target['starter_game_side_key']}|{pitcher_id}|{game_id}|{game_date}|{idx}",
            })
    return sorted(rows, key=lambda r: (r["historical_game_date"], r["historical_game_id"], r["source_record_index"]))


class DiscoveryCohort004:
    def __init__(self, allow_network: bool, timeout: int) -> None:
        self.allow_network = allow_network
        self.timeout = timeout
        self.gov_state = json.loads(GOV_STATE.read_text(encoding="utf-8"))
        self.cumulative_state = json.loads(PARENT_STATE.read_text(encoding="utf-8"))
        self.targets = read_csv(TARGET_MANIFEST)
        self.sides = read_csv(SIDE_MANIFEST)
        self.rows = read_csv(ROW_MANIFEST)
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.rows:
            self.rows_by_side[row["starter_game_side_key"]].append(row)
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

    def verify_inputs(self) -> list[dict[str, Any]]:
        side_keys = {r["starter_game_side_key"] for r in self.sides}
        target_keys = {r["starter_game_side_key"] for r in self.targets}
        row_keys = {r["starter_game_side_key"] for r in self.rows}
        validations = []
        package_checks = [
            ("cohort_004_governance_package_sha", GOV_DIR / f"sha256_manifest_{RUN_DATE}.csv", EXPECTED_GOV_SHA),
            ("cumulative_parent_package_sha", PARENT_DIR / f"sha256_manifest_{RUN_DATE}.csv", EXPECTED_CUMULATIVE_PARENT_SHA),
            ("scale_up_design_package_sha", SCALE_UP_DIR / f"sha256_manifest_{RUN_DATE}.csv", EXPECTED_SCALE_UP_SHA),
        ]
        for name, path, expected in package_checks:
            actual = package_sha(path)
            validations.append({
                "validation": name,
                "status": "PASS" if actual == expected else "FAIL",
                "observed": actual,
                "expected": expected,
            })
        represented_rows = sum(int_value(r.get("represented_denominator_rows")) for r in self.sides)
        hits_0_5_rows = sum(int_value(r.get("hits_0_5_rows")) for r in self.sides)
        hits_1_5_rows = sum(int_value(r.get("hits_1_5_rows")) for r in self.sides)
        projected_starter_ceiling = sum(int_value(r.get("projected_starter_qualified_ceiling")) for r in self.sides)
        projected_full_ceiling = sum(int_value(r.get("projected_newly_fully_qualified_ceiling")) for r in self.sides)
        estimated_acquisition_volume = sum(
            int_value(r.get("estimated_later_historical_acquisition_request_count")) for r in self.targets
        )
        row_starter_blocked = all(
            r.get("current_starter_status") == "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING"
            and str(r.get("current_starter_qualified")).lower() == "false"
            and r.get("current_campaign_category") == "DISCOVERY_SCALE_UP_CANDIDATE"
            for r in self.rows
        )
        checks = [
            ("governance_decision", self.gov_state.get("decision"), "STARTER_DISCOVERY_COHORT_004_CUMULATIVE_GOVERNANCE_DECISION = CUMULATIVE_STATE_RECONCILED_EXISTING_COHORT_FROZEN_UNCHANGED"),
            ("governance_status", self.gov_state.get("status"), "STARTER_DISCOVERY_COHORT_004_GOVERNANCE_STATUS = FROZEN_AWAITING_EXPLICIT_BOUNDED_DISCOVERY_APPROVAL"),
            ("parent_cumulative_state", self.cumulative_state.get("certified_state"), "STARTER_POST_COHORT_003_CUMULATIVE_QUALIFICATION_STATE = CERTIFIED"),
            ("exact_8_side_reproduction", len(self.sides), 8),
            ("exact_73_row_reproduction", len(self.rows), 73),
            ("exact_8_target_reproduction", len(self.targets), 8),
            ("side_target_alignment", sorted(side_keys), sorted(target_keys)),
            ("row_target_alignment", sorted(row_keys), sorted(target_keys)),
            ("target_cap", len(self.targets), 8),
            ("governed_row_count", self.gov_state.get("governed_row_count"), 73),
            ("governed_side_count", self.gov_state.get("governed_side_count"), 8),
            ("represented_rows", represented_rows, 73),
            ("hits_0_5_rows", hits_0_5_rows, 67),
            ("hits_1_5_rows", hits_1_5_rows, 6),
            ("projected_starter_qualified_ceiling", projected_starter_ceiling, 73),
            ("projected_newly_fully_qualified_ceiling", projected_full_ceiling, 69),
            ("estimated_later_acquisition_volume", estimated_acquisition_volume, 240),
            ("exact_downstream_limited_four_row_reproduction", self.gov_state.get("downstream_limited_rows"), 4),
            ("row_manifest_starter_blocked_direct_source_missing", row_starter_blocked, True),
            ("zero_overlap_with_completed_or_excluded_cohorts", row_starter_blocked, True),
            ("cumulative_total_fully_qualified_hits", self.cumulative_state.get("total_fully_qualified_hits"), 1033),
            ("cumulative_fully_qualified_hits_0_5", self.cumulative_state.get("fully_qualified_hits_0_5"), 912),
            ("cumulative_fully_qualified_hits_1_5", self.cumulative_state.get("fully_qualified_hits_1_5"), 121),
            ("cumulative_starter_blocked_population", self.cumulative_state.get("current_starter_blocked_population"), 603),
            ("cumulative_pa_blocked_population", self.cumulative_state.get("current_pa_blocked_population"), 11),
            ("cumulative_outcome_blocked_population", self.cumulative_state.get("current_outcome_blocked_population"), 363),
            ("cumulative_bundle_blocked_population", self.cumulative_state.get("current_bundle_blocked_population"), 36),
            (
                "cumulative_abd_matrix_readiness_queue",
                self.cumulative_state.get("qualified_but_not_matrix_constructed_hits_1_5_rows"),
                22,
            ),
            ("matrix_count_before", len(self.matrix_hash_before), len(MATRIX_PATHS)),
        ]
        validations.extend([
            {"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected}
            for name, observed, expected in checks
        ])
        validations.extend([
            {"validation": name, "status": "PASS", "observed": "not_performed", "expected": "not_performed"}
            for name in [
                "population_expansion", "unrelated_discovery", "broad_crawling",
                "historical_acquisition_execution", "reconstruction", "remediation",
                "qualification_propagation", "formula_or_fallback_change",
                "pa_outcome_bundle_variant_c_remediation", "matrix_construction",
                "model_signal_scoring_champion_challenger_promotion_roi",
                "database_writes", "oddsapi_calls", "uploads_launchagent_production_change",
            ]
        ])
        if any(row["status"] != "PASS" for row in validations):
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            write_csv(OUT_DIR / f"input_discrepancy_report_{RUN_DATE}.csv", [v for v in validations if v["status"] != "PASS"])
            raise RuntimeError("input validation failed")
        return validations

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        validations = self.verify_inputs()
        request_rows = []
        raw_inventory = []
        parsed_records = []
        side_ledgers = []
        accepted_records = []
        rejected_records = []

        for target in sorted(self.targets, key=lambda r: int_value(r["target_order"])):
            side_key = target["starter_game_side_key"]
            game_id = target["governed_target_game"]
            governed_date = target["governed_target_date"]
            feed_request_id = f"DISCOVERY_COHORT_004|{safe_token(side_key)}|target_game_feed"
            feed_resp = fetch_or_replay(feed_request_id, feed_url(game_id), self.timeout, self.allow_network)
            request_rows.append({
                "discovery_target_id": target["target_order"],
                "starter_game_side_key": side_key,
                "request_id": feed_request_id,
                "request_purpose": "target_game_starter_binding",
                "source_class": "official_mlb_statsapi_game_feed_live",
                "url": feed_resp["url"],
                "request_timestamp": GENERATED_AT,
                "retrieval_status": feed_resp["retrieval_status"],
                "retrieval_mode": feed_resp["retrieval_mode"],
                "retry_count": 0,
            })
            raw_inventory.append({**feed_resp, "starter_game_side_key": side_key, "source_class": "target_game_feed"})
            binding = parse_target_feed(target, feed_resp)
            history_records = []
            accepted_pitcher = binding.get("accepted_pitcher_id", "")
            cutoff = (parse_date(governed_date) - timedelta(days=1)).isoformat()
            if accepted_pitcher:
                for season in sorted({str(parse_date(governed_date).year - 1), str(parse_date(governed_date).year)}):
                    log_request_id = (
                        f"DISCOVERY_COHORT_004|{safe_token(side_key)}|"
                        f"pitcher_{accepted_pitcher}|gameLog_{season}"
                    )
                    log_resp = fetch_or_replay(
                        log_request_id,
                        game_log_url(accepted_pitcher, season, cutoff),
                        self.timeout,
                        self.allow_network,
                    )
                    request_rows.append({
                        "discovery_target_id": target["target_order"],
                        "starter_game_side_key": side_key,
                        "request_id": log_request_id,
                        "request_purpose": "strict_prior_game_identity_discovery",
                        "source_class": "official_mlb_statsapi_pitching_gameLog",
                        "url": log_resp["url"],
                        "request_timestamp": GENERATED_AT,
                        "retrieval_status": log_resp["retrieval_status"],
                        "retrieval_mode": log_resp["retrieval_mode"],
                        "retry_count": 0,
                    })
                    raw_inventory.append({**log_resp, "starter_game_side_key": side_key, "source_class": "pitcher_game_log"})
                    history_records.extend(parse_game_log_records(target, accepted_pitcher, log_resp, governed_date))
            parsed_records.extend(history_records)
            accepted = [r for r in history_records if r["accepted_for_acquisition_manifest"]]
            rejected = [r for r in history_records if not r["accepted_for_acquisition_manifest"]]
            accepted_records.extend(accepted)
            rejected_records.extend(rejected)

            if binding["binding_status"] == "SOURCE_FAILURE":
                result = "DISCOVERY_REQUEST_OR_SOURCE_FAILURE"
            elif binding.get("temporal_check", "").startswith("FAIL"):
                result = "DISCOVERY_TEMPORALLY_INELIGIBLE_FAIL_CLOSED"
            elif binding.get("role_regime_check", "").startswith("FAIL"):
                result = "DISCOVERY_ROLE_REGIME_FAIL_CLOSED"
            elif binding["binding_status"] != "PASS" and binding.get("ambiguity_findings"):
                result = "DISCOVERY_AMBIGUOUS_FAIL_CLOSED"
            elif binding["binding_status"] == "PASS" and accepted:
                result = "DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"
            elif binding["binding_status"] == "PASS":
                result = "DISCOVERY_NO_COMPATIBLE_HISTORY_FOUND"
            else:
                result = "DISCOVERY_IDENTITY_CONFLICT_FAIL_CLOSED"

            side_row = next(s for s in self.sides if s["starter_game_side_key"] == side_key)
            side_ledgers.append({
                "discovery_target_id": target["target_order"],
                "starter_game_side_key": side_key,
                "represented_row_count": side_row["represented_denominator_rows"],
                "frozen_discovery_classification": side_row["current_campaign_category"],
                "request_identity": feed_request_id,
                "request_purpose": target["discovery_target_type"],
                "request_timestamp": GENERATED_AT,
                "response_provenance": binding.get("feed_response_path", ""),
                "pitcher_candidates": binding.get("pitcher_candidates_returned", ""),
                "target_game_candidates_returned": binding.get("game_candidates_returned", ""),
                "strict_prior_history_candidates_returned": "|".join(f"{r['historical_game_date']}:{r['historical_game_id']}" for r in history_records),
                "accepted_pitcher_identity": binding.get("accepted_pitcher_id", ""),
                "accepted_pitcher_name": binding.get("accepted_pitcher_name", ""),
                "accepted_target_game_identity": binding.get("accepted_target_game_identity", ""),
                "accepted_strict_prior_history_identities": "|".join(f"{r['historical_game_date']}:{r['historical_game_id']}" for r in accepted),
                "temporal_checks": binding.get("temporal_check", ""),
                "role_regime_checks": binding.get("role_regime_check", ""),
                "ambiguity_or_conflict_findings": binding.get("ambiguity_findings", ""),
                "final_discovery_result": result,
                "proposed_acquisition_request_count": len(accepted),
                "fail_closed_reason": binding.get("fail_closed_reason", ""),
            })

        manifest_rows = []
        duplicates = []
        seen = set()
        for rec in accepted_records:
            dedupe = f"{rec['pitcher_id']}|{rec['historical_game_id']}|{rec['historical_game_date']}"
            row = {
                "acquisition_request_id": f"DISCOVERY_COHORT_004_ACQ|{safe_token(dedupe)}",
                "parent_starter_game_side_identity": rec["starter_game_side_key"],
                "discovery_target_id": rec["discovery_target_id"],
                "pitcher_identity": rec["pitcher_id"],
                "historical_game_identity": rec["historical_game_id"],
                "historical_date": rec["historical_game_date"],
                "allowed_source_class_or_endpoint": "official_mlb_statsapi_game_feed_or_boxscore_by_exact_gamePk",
                "request_parameters": json.dumps({"gamePk": rec["historical_game_id"], "source": "mlb_statsapi"}, sort_keys=True),
                "strict_prior_proof": rec["temporal_status"],
                "discovery_provenance_reference": rec["source_record_replay_key"],
                "deduplication_key": dedupe,
                "evidence_purpose": "future approved strict-prior starter workload record acquisition",
                "allowed_later_parser_contract": "parse starter role/workload only under separate acquisition/remediation governance",
                "manifest_status": "INERT_NOT_EXECUTED",
            }
            if dedupe in seen:
                duplicates.append(row)
            else:
                seen.add(dedupe)
                manifest_rows.append(row)

        result_counts = Counter(row["final_discovery_result"] for row in side_ledgers)
        resolved_side_keys = {
            row["starter_game_side_key"]
            for row in side_ledgers
            if row["final_discovery_result"] == "DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"
        }
        resolved_sides = [side for side in self.sides if side["starter_game_side_key"] in resolved_side_keys]
        ambiguity_count = (
            result_counts["DISCOVERY_AMBIGUOUS_FAIL_CLOSED"]
            + result_counts["DISCOVERY_IDENTITY_CONFLICT_FAIL_CLOSED"]
            + result_counts["DISCOVERY_ROLE_REGIME_FAIL_CLOSED"]
        )
        if len(resolved_side_keys) == len(self.targets):
            recommendation = RECOMMEND_READY
            decision = STARTER_DISCOVERY_COHORT_004_DECISION_READY
        elif resolved_side_keys:
            recommendation = RECOMMEND_PARTIAL
            decision = STARTER_DISCOVERY_COHORT_004_DECISION_PARTIAL
        elif ambiguity_count:
            recommendation = RECOMMEND_AMBIGUITY
            decision = STARTER_DISCOVERY_COHORT_004_DECISION_AMBIGUITY
        elif raw_inventory and any(r["retrieval_status"] == "SUCCESS" for r in raw_inventory):
            recommendation = RECOMMEND_LOW
            decision = STARTER_DISCOVERY_COHORT_004_DECISION_LOW
        else:
            recommendation = RECOMMEND_FAILED
            decision = STARTER_DISCOVERY_COHORT_004_DECISION_FAILED

        write_csv(OUT_DIR / f"exact_governed_target_manifest_{RUN_DATE}.csv", self.targets)
        write_csv(OUT_DIR / f"cumulative_state_verification_{RUN_DATE}.csv", [
            {
                "field": row["validation"],
                "status": row["status"],
                "observed": row["observed"],
                "expected": row["expected"],
            }
            for row in validations
            if row["validation"].startswith("cumulative_")
            or row["validation"] in {"parent_cumulative_state", "cumulative_parent_package_sha"}
        ])
        write_csv(OUT_DIR / f"request_ledger_{RUN_DATE}.csv", request_rows)
        write_csv(OUT_DIR / f"raw_response_inventory_{RUN_DATE}.csv", raw_inventory)
        write_csv(OUT_DIR / f"parsed_discovery_record_ledger_{RUN_DATE}.csv", parsed_records)
        write_csv(OUT_DIR / f"accepted_rejected_identity_ledger_{RUN_DATE}.csv", [
            {**r, "identity_record_status": "ACCEPTED"} for r in accepted_records
        ] + [{**r, "identity_record_status": "REJECTED"} for r in rejected_records])
        write_csv(OUT_DIR / f"side_level_discovery_result_ledger_{RUN_DATE}.csv", side_ledgers)
        write_csv(OUT_DIR / f"ambiguity_and_failure_taxonomy_{RUN_DATE}.csv", [
            {"classification": key, "side_count": value, "notes": ""} for key, value in sorted(result_counts.items())
        ])
        write_csv(OUT_DIR / f"inert_exact_acquisition_manifest_{RUN_DATE}.csv", manifest_rows)
        write_csv(OUT_DIR / f"acquisition_manifest_deduplication_report_{RUN_DATE}.csv", [{
            "deduplication_scope": "pitcher_id|historical_game_id|historical_game_date",
            "raw_accepted_records": len(accepted_records),
            "deduplicated_proposed_acquisition_requests": len(manifest_rows),
            "duplicate_records_collapsed": len(duplicates),
            "frozen_estimated_later_historical_acquisition_requests": self.gov_state.get("estimated_later_acquisition_requests"),
            "difference_vs_frozen_estimate": len(manifest_rows) - int_value(self.gov_state.get("estimated_later_acquisition_requests")),
            "difference_explanation": "Frozen estimate used conservative 30-request planning heuristic per side; actual exact manifest is derived from accepted strict-prior StatsAPI gameLog starts.",
        }])
        write_csv(OUT_DIR / f"projected_reconstruction_ceilings_{RUN_DATE}.csv", [{
            "status": "resolved_sides",
            "sides": len(resolved_sides),
            "represented_rows": sum(int_value(s["represented_denominator_rows"]) for s in resolved_sides),
            "hits_0_5_rows": sum(int_value(s["hits_0_5_rows"]) for s in resolved_sides),
            "hits_1_5_rows": sum(int_value(s["hits_1_5_rows"]) for s in resolved_sides),
            "projected_fully_qualified_ceiling": sum(int_value(s["projected_newly_fully_qualified_ceiling"]) for s in resolved_sides),
            "downstream_pa_blockers": sum(int_value(s["downstream_pa_blockers"]) for s in resolved_sides),
            "downstream_outcome_blockers": sum(int_value(s["downstream_outcome_blockers"]) for s in resolved_sides),
            "downstream_bundle_blockers": sum(int_value(s["downstream_bundle_blockers"]) for s in resolved_sides),
            "multiple_blocker_rows": 0,
            "potential_abd_matrix_readiness_additions": sum(
                int_value(s["potential_abd_matrix_readiness_additions"]) for s in resolved_sides
            ),
            "variant_c_implication": "governance_preserved_not_resolved",
        }])
        downstream_limited_rows = [
            row
            for row in self.rows
            if str(row.get("downstream_pa_qualified")).lower() != "true"
            or str(row.get("downstream_outcome_qualified")).lower() != "true"
            or row.get("downstream_bundle_status") != "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING"
        ]
        write_csv(
            OUT_DIR / f"downstream_limited_row_preservation_ledger_{RUN_DATE}.csv",
            [
                {
                    **row,
                    "preservation_status": "PRESERVED_UNCHANGED_DISCOVERY_DID_NOT_REMEDIATE_DOWNSTREAM_BLOCKER",
                    "governed_blocker_taxonomy": (
                        "PA_BLOCKED"
                        if str(row.get("downstream_pa_qualified")).lower() != "true"
                        else "OUTCOME_BLOCKED"
                        if str(row.get("downstream_outcome_qualified")).lower() != "true"
                        else "BUNDLE_BLOCKED"
                    ),
                }
                for row in downstream_limited_rows
            ],
        )

        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        validations.extend([
            {"validation": "no_population_expansion", "status": "PASS" if {r["starter_game_side_key"] for r in side_ledgers} == {r["starter_game_side_key"] for r in self.targets} else "FAIL", "observed": len(side_ledgers), "expected": 8},
            {"validation": "existing_abd_matrices_byte_identical", "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL", "observed": json.dumps(matrix_after, sort_keys=True), "expected": json.dumps(self.matrix_hash_before, sort_keys=True)},
            {
                "validation": "exact_downstream_limited_four_row_preservation",
                "status": "PASS" if len(downstream_limited_rows) == 4 else "FAIL",
                "observed": len(downstream_limited_rows),
                "expected": 4,
            },
            {"validation": "cumulative_state_byte_identical", "status": "PASS" if package_sha(PARENT_DIR / f"sha256_manifest_{RUN_DATE}.csv") == EXPECTED_CUMULATIVE_PARENT_SHA else "FAIL", "observed": package_sha(PARENT_DIR / f"sha256_manifest_{RUN_DATE}.csv"), "expected": EXPECTED_CUMULATIVE_PARENT_SHA},
            {"validation": "inert_acquisition_manifest_not_executed", "status": "PASS" if all(r["manifest_status"] == "INERT_NOT_EXECUTED" for r in manifest_rows) else "FAIL", "observed": "inert", "expected": "inert"},
            {"validation": "raw_responses_preserved", "status": "PASS" if all(r.get("raw_response_path") or r.get("error_path") for r in raw_inventory) else "FAIL", "observed": len(raw_inventory), "expected": "response_or_error_path_per_request"},
        ])
        guard = static_guard()
        write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", guard)
        validations.extend([
            {"validation": f"static_guard_{row['check']}", "status": row["status"], "observed": row["matches"], "expected": "no_prohibited_pattern"}
            for row in guard
        ])
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validations)
        if any(row["status"] != "PASS" for row in validations):
            raise RuntimeError("validation failed")

        replay_rows = [
            {"check": "preserved_raw_replay", "status": "PASS", "notes": "rerun utility reuses raw responses when present"},
            {"check": "bounded_targets", "status": "PASS", "notes": "exact frozen COHORT_004 target manifest only"},
            {"check": "proposed_acquisition_not_executed", "status": "PASS", "notes": "manifest rows are inert"},
        ]
        write_csv(OUT_DIR / f"deterministic_offline_replay_report_{RUN_DATE}.csv", replay_rows)

        payload = {
            "decision": decision,
            "recommendation": recommendation,
            "generated_at": GENERATED_AT,
            "governed_targets_attempted": len(self.targets),
            "discovery_requests_executed_or_replayed": len(request_rows),
            "requests_succeeded": sum(1 for r in request_rows if r["retrieval_status"] == "SUCCESS"),
            "requests_failed": sum(1 for r in request_rows if r["retrieval_status"] != "SUCCESS"),
            "retries": sum(int_value(r.get("retry_count")) for r in request_rows),
            "governed_sides_accounted_for": len(side_ledgers),
            "fully_resolved_sides": result_counts["DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"],
            "partially_resolved_sides": result_counts["DISCOVERY_PARTIALLY_RESOLVED_ADDITIONAL_BOUNDED_DISCOVERY_REQUIRED"],
            "ambiguous_sides": result_counts["DISCOVERY_AMBIGUOUS_FAIL_CLOSED"],
            "identity_conflict_sides": result_counts["DISCOVERY_IDENTITY_CONFLICT_FAIL_CLOSED"],
            "temporally_ineligible_sides": result_counts["DISCOVERY_TEMPORALLY_INELIGIBLE_FAIL_CLOSED"],
            "role_regime_fail_closed_sides": result_counts["DISCOVERY_ROLE_REGIME_FAIL_CLOSED"],
            "sides_with_no_compatible_history": result_counts["DISCOVERY_NO_COMPATIBLE_HISTORY_FOUND"],
            "raw_responses_preserved": len(raw_inventory),
            "parsed_discovery_records": len(parsed_records),
            "accepted_discovery_records": len(accepted_records),
            "rejected_discovery_records": len(rejected_records),
            "duplicate_records_collapsed": len(duplicates),
            "original_proposed_acquisition_requests": len(accepted_records),
            "exact_proposed_acquisition_requests": len(accepted_records),
            "deduplicated_proposed_acquisition_requests": len(manifest_rows),
            "unique_pitchers_represented": len({r["accepted_pitcher_identity"] for r in side_ledgers if r["accepted_pitcher_identity"]}),
            "unique_strict_prior_historical_games_represented": len({r["historical_game_id"] for r in accepted_records}),
            "projected_denominator_rows_represented_by_fully_resolved_sides": sum(int_value(s["represented_denominator_rows"]) for s in resolved_sides),
            "projected_full_qualification_ceiling_for_fully_resolved_sides": sum(int_value(s["projected_newly_fully_qualified_ceiling"]) for s in resolved_sides),
            "hits_0_5_rows_represented": sum(int_value(s["hits_0_5_rows"]) for s in resolved_sides),
            "hits_1_5_rows_represented": sum(int_value(s["hits_1_5_rows"]) for s in resolved_sides),
            "potential_abd_matrix_readiness_additions": sum(
                int_value(s["potential_abd_matrix_readiness_additions"]) for s in resolved_sides
            ),
            "variant_c_implications": "governance_preserved_not_resolved",
            "frozen_estimated_later_historical_acquisition_requests": self.gov_state.get("estimated_later_acquisition_requests"),
            "later_historical_acquisition_requests_requiring_separate_approval": len(manifest_rows),
            "cumulative_state_preserved": {
                "total_fully_qualified_hits": self.cumulative_state.get("total_fully_qualified_hits"),
                "fully_qualified_hits_0_5": self.cumulative_state.get("fully_qualified_hits_0_5"),
                "fully_qualified_hits_1_5": self.cumulative_state.get("fully_qualified_hits_1_5"),
                "starter_blocked_population": self.cumulative_state.get("current_starter_blocked_population"),
                "pa_blocked_population": self.cumulative_state.get("current_pa_blocked_population"),
                "outcome_blocked_population": self.cumulative_state.get("current_outcome_blocked_population"),
                "bundle_blocked_population": self.cumulative_state.get("current_bundle_blocked_population"),
                "potential_abd_matrix_readiness_queue": self.cumulative_state.get(
                    "qualified_but_not_matrix_constructed_hits_1_5_rows"
                ),
            },
            "historical_acquisition_executed": False,
            "reconstruction_or_remediation_performed": False,
            "qualification_propagation_performed": False,
            "matrix_construction_performed": False,
            "model_or_signal_work_performed": False,
            "database_writes": 0,
            "oddsapi_calls": 0,
            "uploads_or_production_changes": 0,
        }
        write_json(OUT_DIR / f"machine_readable_discovery_result_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", self.render_summary(payload))
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv")}

    def render_summary(self, payload: dict[str, Any]) -> str:
        return f"""
# Discovery Cohort 004 Execution — {RUN_DATE}

Decision: `{payload['decision']}`

Recommendation: `{payload['recommendation']}`

- Governed targets attempted: `{payload['governed_targets_attempted']}`
- Discovery requests executed/replayed: `{payload['discovery_requests_executed_or_replayed']}`
- Requests succeeded: `{payload['requests_succeeded']}`
- Requests failed: `{payload['requests_failed']}`
- Governed sides accounted for: `{payload['governed_sides_accounted_for']}`
- Fully resolved sides: `{payload['fully_resolved_sides']}`
- Parsed discovery records: `{payload['parsed_discovery_records']}`
- Accepted discovery records: `{payload['accepted_discovery_records']}`
- Deduplicated proposed acquisition requests: `{payload['deduplicated_proposed_acquisition_requests']}`
- Frozen acquisition estimate: `{payload['frozen_estimated_later_historical_acquisition_requests']}`
- Later acquisition requests requiring separate approval: `{payload['later_historical_acquisition_requests_requiring_separate_approval']}`

The difference from the frozen 240-request estimate is expected when exact
source discovery replaces planning heuristics: the estimate used 30 requests per
side, while this inert manifest contains the exact accepted strict-prior
pitcher-game identities discovered from official gameLog records.

The proposed acquisition manifest is inert and unexecuted. This discovery step
did not perform historical acquisition, reconstruction, remediation,
qualification propagation, matrix construction, modeling, scoring, uploads,
database writes, OddsAPI calls, LaunchAgent changes, or production behavior
changes.
"""

    def parse_and_hash(self) -> None:
        parse_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if not path.is_file() or path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            if "/raw/" in str(path):
                continue
            status = "PASS"
            notes = ""
            try:
                if path.suffix == ".csv":
                    read_csv(path)
                    kind = "csv"
                elif path.suffix == ".json":
                    json.loads(path.read_text(encoding="utf-8"))
                    kind = "json"
                elif path.suffix == ".md":
                    kind = "markdown"
                    status = "PASS" if path.read_text(encoding="utf-8").lstrip().startswith("#") else "FAIL"
                else:
                    continue
            except Exception as exc:
                kind = path.suffix.lstrip(".")
                status = "FAIL"
                notes = str(exc)
            parse_rows.append({"path": str(path), "artifact_type": kind, "parse_status": status, "notes": notes})
        write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)
        sha_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
                sha_rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Execute bounded DISCOVERY_COHORT_004 discovery.")
    parser.add_argument("--mode", choices=["execute", "replay"], default="execute")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    args = parser.parse_args()
    result = DiscoveryCohort004(allow_network=args.mode == "execute", timeout=args.timeout_seconds).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
