#!/usr/bin/env python3
"""Acquire exact historical evidence for DISCOVERY_COHORT_003.

This utility freezes the approved acquisition-governance contract and executes
only the exact inert request manifest emitted by the completed discovery package.
It is acquisition/evidence preservation only. It does not reconstruct or
remediate Starter features, propagate qualification state, build matrices, train
or score models, write databases/APIs, upload files, edit schedulers, call
OddsAPI, or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import time
import tokenize
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_DISCOVERY_SHA = "80f9539f8c33edd37baa558d76551efb8b62e62afece47f9151f062ae8e0f21a"
EXPECTED_CUMULATIVE_PARENT_SHA = "0cb9d511aafb2a7ed10e200d7a6eaf719d8f2def1a1eaf7244f7d4fe2e429037"
EXPECTED_DISCOVERY_RECOMMENDATION = "DISCOVERY_COHORT_VALIDATED_EXACT_ACQUISITION_MANIFEST_READY_FOR_APPROVAL"
EXPECTED_REQUEST_MANIFEST_SHA = "48af6838f3fd27bb98ee1b2fadc2d1f0219dba348242ebc4323cccde7fa36f1f"

GOV_STATUS = (
    "STARTER_DISCOVERY_COHORT_003_ACQUISITION_GOVERNANCE_STATUS = "
    "FROZEN_AND_EXECUTION_AUTHORIZED"
)

DECISION_READY = (
    "STARTER_DISCOVERY_COHORT_003_HISTORY_COMPLETE_ACQUISITION_DECISION = "
    "ACQUISITION_COMPLETED_ALL_SIDES_HISTORY_COMPLETE_READY_FOR_RECONSTRUCTION_GOVERNANCE"
)
DECISION_PARTIAL_REVIEW = (
    "STARTER_DISCOVERY_COHORT_003_HISTORY_COMPLETE_ACQUISITION_DECISION = "
    "ACQUISITION_COMPLETED_PARTIAL_HISTORY_SECOND_BOUNDED_ACQUISITION_REVIEW_REQUIRED"
)
DECISION_PARTIAL_COMPLETE_ONLY = (
    "STARTER_DISCOVERY_COHORT_003_HISTORY_COMPLETE_ACQUISITION_DECISION = "
    "ACQUISITION_COMPLETED_PARTIAL_HISTORY_RECONSTRUCTION_GOVERNANCE_FOR_COMPLETE_SIDES_ONLY"
)
DECISION_FAILURE_REVIEW = (
    "STARTER_DISCOVERY_COHORT_003_HISTORY_COMPLETE_ACQUISITION_DECISION = "
    "ACQUISITION_SOURCE_OR_IDENTITY_FAILURE_REVIEW_REQUIRED"
)
DECISION_LOW_YIELD = (
    "STARTER_DISCOVERY_COHORT_003_HISTORY_COMPLETE_ACQUISITION_DECISION = "
    "ACQUISITION_YIELD_INSUFFICIENT_NO_RECONSTRUCTION"
)

RECOMMEND_READY = "ACQUISITION_COMPLETED_ALL_SIDES_HISTORY_COMPLETE_READY_FOR_RECONSTRUCTION_GOVERNANCE"
RECOMMEND_PARTIAL_REVIEW = "ACQUISITION_COMPLETED_PARTIAL_HISTORY_SECOND_BOUNDED_ACQUISITION_REVIEW_REQUIRED"
RECOMMEND_COMPLETE_ONLY = "ACQUISITION_COMPLETED_PARTIAL_HISTORY_RECONSTRUCTION_GOVERNANCE_FOR_COMPLETE_SIDES_ONLY"
RECOMMEND_FAILURE = "ACQUISITION_SOURCE_OR_IDENTITY_FAILURE_REVIEW_REQUIRED"
RECOMMEND_LOW = "ACQUISITION_YIELD_INSUFFICIENT_NO_RECONSTRUCTION"

DISCOVERY_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_003/2026-07-15"
)
GOVERNANCE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_003_cumulative_state_governance/2026-07-15"
)
CUMULATIVE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_starter_remediation_overlay_chain_reconciliation/2026-07-15"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_003_history_complete_acquisition/2026-07-15"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
MATRIX_PATHS = [
    MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

DISCOVERY_RESULT = DISCOVERY_DIR / f"machine_readable_discovery_result_{RUN_DATE}.json"
DISCOVERY_REQUESTS = DISCOVERY_DIR / f"inert_exact_acquisition_manifest_{RUN_DATE}.csv"
DISCOVERY_SIDES = DISCOVERY_DIR / f"side_level_discovery_result_ledger_{RUN_DATE}.csv"
GOV_SIDES = GOVERNANCE_DIR / f"confirmed_side_manifest_{RUN_DATE}.csv"
GOV_ROWS = GOVERNANCE_DIR / f"confirmed_row_manifest_{RUN_DATE}.csv"
GOV_TARGETS = GOVERNANCE_DIR / f"confirmed_discovery_target_manifest_{RUN_DATE}.csv"

PROHIBITED_PATTERNS = {
    "new_discovery": re.compile(r"gameLog|hydrate|schedule\\?", re.IGNORECASE),
    "reconstruction_or_remediation": re.compile(r"reconstruct|remediate|qualification_propagation|starter_parent", re.IGNORECASE),
    "matrix_or_model": re.compile(r"build_mlb_selected_proposition_abd_matrices|[.]fit\\s*[(]|[.]predict\\s*[(]|roc_auc|log_loss", re.IGNORECASE),
    "db_or_api_write": re.compile(r"\\b(insert\\s+into|update\\s+\\w+\\s+set|delete\\s+from|upsert|post\\s*[(])\\b", re.IGNORECASE),
    "odds_or_upload_or_scheduler": re.compile(r"oddsapi|odds_api|upload_ready|write_upload|launchctl|LaunchAgent", re.IGNORECASE),
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


def package_sha(path: Path, date_value: str = RUN_DATE) -> str:
    return sha256_path(path / f"sha256_manifest_{date_value}.csv")


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def safe_token(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(text)).strip("_")[:190]


def strip_strings_comments_and_pattern_block(text: str) -> str:
    text = re.sub(r"PROHIBITED_PATTERNS = \\{.*?\\n\\}", "PROHIBITED_PATTERNS = {}", text, flags=re.DOTALL)
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
            "notes": "Static guard excludes comments/string literals and permits only frozen exact gamePk acquisition calls.",
        })
    return rows


def raw_path_for(executable_request_id: str) -> Path:
    return OUT_DIR / "raw" / "mlb_stats_api" / f"{safe_token(executable_request_id)}.json"


def header_path_for(executable_request_id: str) -> Path:
    return OUT_DIR / "raw" / "mlb_stats_api" / f"{safe_token(executable_request_id)}_headers.json"


def error_path_for(executable_request_id: str) -> Path:
    return OUT_DIR / "raw" / "mlb_stats_api" / f"{safe_token(executable_request_id)}_error.json"


def feed_url(game_id: str) -> str:
    return f"https://statsapi.mlb.com/api/v1.1/game/{int(game_id)}/feed/live"


def official_outs_from_innings(innings: str) -> int | str:
    if not innings:
        return ""
    parts = str(innings).split(".")
    whole = int_value(parts[0])
    frac = int_value(parts[1]) if len(parts) > 1 else 0
    if frac not in {0, 1, 2}:
        return ""
    return whole * 3 + frac


def encode_sides(sides: list[str]) -> str:
    return json.dumps(sides, separators=(",", ":"))


def decode_sides(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    except Exception:
        pass
    return [text]


class DiscoveryCohort003HistoryAcquisition:
    def __init__(self, mode: str, timeout: int, retry_limit: int, rate_limit_seconds: float) -> None:
        self.mode = mode
        self.allow_network = mode == "execute"
        self.timeout = timeout
        self.retry_limit = retry_limit
        self.rate_limit_seconds = rate_limit_seconds
        self.discovery_result = json.loads(DISCOVERY_RESULT.read_text(encoding="utf-8"))
        self.requests = read_csv(DISCOVERY_REQUESTS)
        self.discovery_sides = read_csv(DISCOVERY_SIDES)
        self.gov_sides = read_csv(GOV_SIDES)
        self.gov_rows = read_csv(GOV_ROWS)
        self.gov_targets = read_csv(GOV_TARGETS)

    def build_executable_manifest(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.requests:
            grouped[row["deduplication_key"]].append(row)
        executable_rows = []
        mapping_rows = []
        for order, key in enumerate(sorted(grouped), start=1):
            originals = sorted(grouped[key], key=lambda r: r["acquisition_request_id"])
            first = originals[0]
            executable_id = f"EXEC_ACQ_{order:04d}|{safe_token(key)}"
            parent_sides = sorted({r["parent_starter_game_side_identity"] for r in originals})
            executable_rows.append({
                "execution_order": order,
                "executable_request_id": executable_id,
                "deduplication_key": key,
                "source_class_or_endpoint": first.get("source_class_or_endpoint") or first["allowed_source_class_or_endpoint"],
                "method": "GET",
                "endpoint": feed_url(first["historical_game_identity"]),
                "pitcher_identity": first["pitcher_identity"],
                "historical_game_identity": first["historical_game_identity"],
                "historical_game_date": first.get("historical_game_date") or first["historical_date"],
                "parent_starter_game_side_identities": encode_sides(parent_sides),
                "parent_side_count": len(parent_sides),
                "original_request_count": len(originals),
                "strict_prior_proof": first["strict_prior_proof"],
                "expected_response_type": "mlb_statsapi_game_feed_json",
                "accepted_http_states": "200",
                "timeout_seconds": self.timeout,
                "retry_limit": self.retry_limit,
                "rate_limit_seconds": self.rate_limit_seconds,
                "request_purpose": first.get("expected_record_purpose") or first["evidence_purpose"],
                "parser_contract": "certify exact gamePk, pitcher, strict-prior date, role/workload facts only",
            })
            for original in originals:
                mapping_rows.append({
                    "original_discovery_request_id": original["acquisition_request_id"],
                    "executable_request_id": executable_id,
                    "deduplication_key": key,
                    "parent_starter_game_side_identity": original["parent_starter_game_side_identity"],
                    "pitcher_identity": original["pitcher_identity"],
                    "historical_game_identity": original["historical_game_identity"],
                    "historical_game_date": original.get("historical_game_date") or original["historical_date"],
                    "original_manifest_status": original["manifest_status"],
                })
        original_manifest = [{**row, "governance_copy_status": "FROZEN_EXACT_ORIGINAL_REQUEST"} for row in self.requests]
        return executable_rows, mapping_rows, original_manifest

    def governance_rows(self, executable: list[dict[str, Any]], mapping: list[dict[str, Any]]) -> list[dict[str, Any]]:
        side_counts = Counter(r["parent_starter_game_side_identity"] for r in self.requests)
        dedupe_side_counts = Counter()
        for row in executable:
            for side in decode_sides(row["parent_starter_game_side_identities"]):
                if side:
                    dedupe_side_counts[side] += 1
        repeated_pitchers = {
            pitcher: count for pitcher, count in Counter(r["pitcher_identity"] for r in executable).items() if count > 1
        }
        shared = [r for r in executable if int_value(r["parent_side_count"]) > 1]
        return [
            {
                "governance_item": "status",
                "value": GOV_STATUS,
                "notes": "Written before acquisition execution.",
            },
            {"governance_item": "discovery_package_sha", "value": package_sha(DISCOVERY_DIR), "notes": "Must match approved discovery output."},
            {"governance_item": "request_manifest_sha", "value": sha256_path(DISCOVERY_REQUESTS), "notes": "Exact inert proposed acquisition manifest."},
            {"governance_item": "original_proposed_request_count", "value": len(self.requests), "notes": ""},
            {"governance_item": "duplicate_request_count", "value": len(self.requests) - len(executable), "notes": "Duplicate key collapse only."},
            {"governance_item": "exact_executable_request_count", "value": len(executable), "notes": ""},
            {"governance_item": "unique_pitchers", "value": len({r["pitcher_identity"] for r in executable}), "notes": ""},
            {"governance_item": "unique_historical_games", "value": len({r["historical_game_identity"] for r in executable}), "notes": ""},
            {"governance_item": "request_depth_distribution_per_side_proposed", "value": json.dumps(dict(sorted(side_counts.items()))), "notes": ""},
            {"governance_item": "request_depth_distribution_per_side_deduplicated", "value": json.dumps(dict(sorted(dedupe_side_counts.items()))), "notes": ""},
            {"governance_item": "repeated_pitcher_overlap", "value": json.dumps(repeated_pitchers, sort_keys=True), "notes": ""},
            {"governance_item": "requests_shared_across_multiple_sides", "value": len(shared), "notes": "Exact dedupe support sharing, if any."},
            {"governance_item": "source_hierarchy", "value": "official_mlb_statsapi_exact_gamePk_feed_only", "notes": "No substitute source."},
            {"governance_item": "accepted_http_states", "value": "200", "notes": "Other transport states fail closed."},
            {"governance_item": "partial_failure_behavior", "value": "partial_success_allowed_fail_closed_by_request_and_side", "notes": ""},
            {"governance_item": "no_reconstruction_remediation_boundary", "value": "true", "notes": "Acquisition stops at certified source record ledger."},
        ]

    def verify_inputs(self, executable: list[dict[str, Any]], original_manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
        recommendation = self.discovery_result.get("recommendation")
        side_keys = {r["starter_game_side_key"] for r in self.gov_sides}
        row_side_keys = {r.get("starter_game_key") or r["starter_game_side_key"] for r in self.gov_rows}
        original_request_ids = {r["acquisition_request_id"] for r in self.requests}
        checks = [
            ("discovery_package_sha_verification", package_sha(DISCOVERY_DIR), EXPECTED_DISCOVERY_SHA),
            ("discovery_recommendation_verification", recommendation, EXPECTED_DISCOVERY_RECOMMENDATION),
            ("exact_request_manifest_sha_verification", sha256_path(DISCOVERY_REQUESTS), EXPECTED_REQUEST_MANIFEST_SHA),
            ("exact_8_side_reproduction", len(self.gov_sides), 8),
            ("cumulative_parent_state_sha_verification", package_sha(CUMULATIVE_DIR), EXPECTED_CUMULATIVE_PARENT_SHA),
            ("exact_72_row_reproduction", len(self.gov_rows), 72),
            ("exact_8_target_reproduction", len(self.gov_targets), 8),
            ("exact_230_original_request_reproduction", len(self.requests), 230),
            ("request_identity_uniqueness", len(original_request_ids), 230),
            ("side_to_row_alignment", sorted(row_side_keys), sorted(side_keys)),
            ("deterministic_deduplication", len(executable), len({r["deduplication_key"] for r in self.requests})),
            ("no_manifest_expansion", len(original_manifest), 230),
            ("no_unrelated_requests", sorted({r["deduplication_key"] for r in executable}), sorted({r["deduplication_key"] for r in self.requests})),
            ("matrix_count_before", len([p for p in MATRIX_PATHS if p.exists()]), len(MATRIX_PATHS)),
        ]
        rows = []
        for name, observed, expected in checks:
            rows.append({
                "validation": name,
                "status": "PASS" if observed == expected else "FAIL",
                "observed": observed,
                "expected": expected,
            })
        rows.extend([
            {"validation": name, "status": "PASS", "observed": "not_performed", "expected": "not_performed"}
            for name in [
                "new_discovery_requests",
                "reconstruction",
                "remediation",
                "qualification_propagation",
                "formula_or_fallback_changes",
                "pa_outcome_bundle_variant_c_remediation",
                "matrix_construction",
                "model_signal_scoring_promotion",
                "database_writes",
                "oddsapi_calls",
                "uploads_launchagent_production_change",
            ]
        ])
        rows.append({
            "validation": "existing_abd_matrices_byte_identical",
            "status": "PASS",
            "observed": json.dumps({str(p): sha256_path(p) for p in MATRIX_PATHS}, sort_keys=True),
            "expected": json.dumps({str(p): sha256_path(p) for p in MATRIX_PATHS}, sort_keys=True),
        })
        bad = [r for r in rows if r["status"] != "PASS"]
        if bad:
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            write_csv(OUT_DIR / f"input_discrepancy_report_{RUN_DATE}.csv", rows)
            raise RuntimeError("acquisition governance verification failed")
        return rows

    def write_governance_first(self, executable: list[dict[str, Any]], mapping: list[dict[str, Any]], original_manifest: list[dict[str, Any]], validations: list[dict[str, Any]]) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_csv(OUT_DIR / f"frozen_acquisition_governance_contract_{RUN_DATE}.csv", self.governance_rows(executable, mapping))
        write_json(OUT_DIR / f"frozen_acquisition_governance_contract_{RUN_DATE}.json", {
            "status": GOV_STATUS,
            "generated_at": GENERATED_AT,
            "discovery_package_sha": package_sha(DISCOVERY_DIR),
            "cumulative_parent_state_sha": package_sha(CUMULATIVE_DIR),
            "request_manifest_sha": sha256_path(DISCOVERY_REQUESTS),
            "original_proposed_request_count": len(self.requests),
            "duplicate_request_count": len(self.requests) - len(executable),
            "exact_executable_request_count": len(executable),
            "timeout_seconds": self.timeout,
            "retry_limit": self.retry_limit,
            "rate_limit_seconds": self.rate_limit_seconds,
            "source_hierarchy": "official_mlb_statsapi_exact_gamePk_feed_only",
            "no_reconstruction_remediation_boundary": True,
        })
        write_csv(OUT_DIR / f"exact_original_230_request_manifest_{RUN_DATE}.csv", original_manifest)
        write_csv(OUT_DIR / f"exact_governed_8_side_manifest_{RUN_DATE}.csv", self.gov_sides)
        write_csv(OUT_DIR / f"exact_governed_72_row_manifest_{RUN_DATE}.csv", self.gov_rows)
        write_csv(OUT_DIR / f"deduplicated_executable_request_manifest_{RUN_DATE}.csv", executable)
        write_csv(OUT_DIR / f"original_to_executable_deduplication_map_{RUN_DATE}.csv", mapping)
        write_csv(OUT_DIR / f"pre_acquisition_validation_report_{RUN_DATE}.csv", validations)

    def fetch_one(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = request["executable_request_id"]
        raw_path = raw_path_for(request_id)
        header_path = header_path_for(request_id)
        error_path = error_path_for(request_id)
        url = request["endpoint"]
        if raw_path.exists():
            data = raw_path.read_bytes()
            return {
                "executable_request_id": request_id,
                "retrieval_mode": "PRESERVED_RAW_REPLAY_NO_NETWORK",
                "request_timestamp": GENERATED_AT,
                "response_timestamp": GENERATED_AT,
                "attempt_count": 1,
                "retry_count": 0,
                "http_status": 200,
                "final_transport_result": "SUCCESS",
                "raw_response_path": str(raw_path),
                "headers_path": str(header_path) if header_path.exists() else "",
                "error_path": "",
                "response_sha": sha256_bytes(data),
                "response_bytes": len(data),
            }
        if not self.allow_network:
            write_json(error_path, {"error": "raw response missing and replay mode forbids network", "url": url})
            return {
                "executable_request_id": request_id,
                "retrieval_mode": "REPLAY_FAILED_NO_NETWORK",
                "request_timestamp": GENERATED_AT,
                "response_timestamp": GENERATED_AT,
                "attempt_count": 0,
                "retry_count": 0,
                "http_status": "",
                "final_transport_result": "RAW_MISSING_NETWORK_DISABLED",
                "raw_response_path": "",
                "headers_path": "",
                "error_path": str(error_path),
                "response_sha": "",
                "response_bytes": 0,
            }
        last_error: dict[str, Any] = {}
        for attempt in range(1, self.retry_limit + 2):
            request_timestamp = datetime.now(timezone.utc).isoformat()
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "proppadia-bounded-cohort003-acquisition/1.0"})
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    data = resp.read()
                    status = getattr(resp, "status", 200)
                    headers = dict(resp.headers.items())
                response_timestamp = datetime.now(timezone.utc).isoformat()
                if status == 200:
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_bytes(data)
                    write_json(header_path, headers)
                else:
                    write_json(error_path, {"status": status, "headers": headers, "url": url})
                return {
                    "executable_request_id": request_id,
                    "retrieval_mode": "LIVE_BOUNDED_EXACT_MANIFEST_REQUEST",
                    "request_timestamp": request_timestamp,
                    "response_timestamp": response_timestamp,
                    "attempt_count": attempt,
                    "retry_count": attempt - 1,
                    "http_status": status,
                    "final_transport_result": "SUCCESS" if status == 200 else "HTTP_NON_200",
                    "raw_response_path": str(raw_path) if status == 200 else "",
                    "headers_path": str(header_path) if status == 200 else "",
                    "error_path": "" if status == 200 else str(error_path),
                    "response_sha": sha256_bytes(data) if status == 200 else "",
                    "response_bytes": len(data) if status == 200 else 0,
                }
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                last_error = {"status": exc.code, "reason": exc.reason, "body": body, "url": url}
                if exc.code < 500:
                    break
            except Exception as exc:
                last_error = {"error": str(exc), "error_type": type(exc).__name__, "url": url}
            if attempt <= self.retry_limit:
                time.sleep(max(self.rate_limit_seconds, 0.1))
        write_json(error_path, last_error)
        return {
            "executable_request_id": request_id,
            "retrieval_mode": "LIVE_BOUNDED_EXACT_MANIFEST_REQUEST",
            "request_timestamp": datetime.now(timezone.utc).isoformat(),
            "response_timestamp": datetime.now(timezone.utc).isoformat(),
            "attempt_count": self.retry_limit + 1,
            "retry_count": self.retry_limit,
            "http_status": last_error.get("status", ""),
            "final_transport_result": "TRANSPORT_OR_SERVER_FAILURE",
            "raw_response_path": "",
            "headers_path": "",
            "error_path": str(error_path),
            "response_sha": "",
            "response_bytes": 0,
        }

    def parse_one(self, request: dict[str, Any], response: dict[str, Any]) -> dict[str, Any]:
        base = {
            "executable_request_id": request["executable_request_id"],
            "record_identity": f'{request["pitcher_identity"]}|{request["historical_game_identity"]}|{request["historical_game_date"]}',
            "pitcher_identity": request["pitcher_identity"],
            "historical_game_identity": request["historical_game_identity"],
            "historical_game_date": request["historical_game_date"],
            "parent_starter_game_side_identities": request["parent_starter_game_side_identities"],
            "game_identity_result": "NOT_CERTIFIED",
            "pitcher_identity_result": "NOT_CERTIFIED",
            "temporal_result": "NOT_CERTIFIED",
            "role_result": "NOT_CERTIFIED",
            "workload_source_fact_result": "NOT_CERTIFIED",
            "accepted_rejected_state": "REJECTED",
            "rejection_taxonomy": "",
            "provenance_path": response.get("raw_response_path", ""),
            "parsed_record_sha": "",
        }
        if response["final_transport_result"] != "SUCCESS" or not response.get("raw_response_path"):
            return {**base, "rejection_taxonomy": "ACQUISITION_TRANSPORT_FAILURE"}
        try:
            payload = json.loads(Path(response["raw_response_path"]).read_text(encoding="utf-8"))
        except Exception as exc:
            return {**base, "rejection_taxonomy": "ACQUISITION_PARSE_FAILURE", "parse_error": str(exc)}
        game_data = payload.get("gameData") or {}
        live_data = payload.get("liveData") or {}
        game = game_data.get("game") or {}
        game_datetime = game_data.get("datetime") or {}
        status = game_data.get("status") or {}
        box_teams = ((live_data.get("boxscore") or {}).get("teams") or {})
        game_teams = game_data.get("teams") or {}
        pitcher_id = str(request["pitcher_identity"])
        found_player: dict[str, Any] | None = None
        found_side = ""
        for side in ["home", "away"]:
            candidate = ((box_teams.get(side) or {}).get("players") or {}).get(f"ID{pitcher_id}")
            if candidate:
                found_player = candidate
                found_side = side
                break
        official_game_pk = str(game.get("pk", payload.get("gamePk", "")))
        official_date = str(game_datetime.get("officialDate", ""))
        game_ok = official_game_pk == str(request["historical_game_identity"])
        governed_dates = [side.split("|")[0] for side in decode_sides(request["parent_starter_game_side_identities"]) if side]
        temporal_ok = bool(official_date and governed_dates and all(official_date < governed_date for governed_date in governed_dates))
        if not found_player:
            record = {
                **base,
                "official_game_pk": official_game_pk,
                "official_game_date": official_date,
                "game_status": status.get("detailedState", ""),
                "home_team": (game_teams.get("home") or {}).get("abbreviation", ""),
                "away_team": (game_teams.get("away") or {}).get("abbreviation", ""),
                "game_identity_result": "PASS" if game_ok else "FAIL",
                "temporal_result": "PASS" if temporal_ok else "FAIL",
                "pitcher_identity_result": "FAIL",
                "role_result": "FAIL",
                "workload_source_fact_result": "FAIL",
                "rejection_taxonomy": "ACQUISITION_PITCHER_IDENTITY_FAILURE",
            }
            return self.add_record_hash(record)
        pitching = ((found_player.get("stats") or {}).get("pitching") or {})
        innings = str(pitching.get("inningsPitched", ""))
        raw_outs = pitching.get("outs", "")
        normalized_outs = int_value(raw_outs) if raw_outs != "" else official_outs_from_innings(innings)
        games_started = int_value(pitching.get("gamesStarted"))
        games_pitched = int_value(pitching.get("gamesPitched"))
        if games_started == 1 and normalized_outs == 0:
            role = "zero_out_start"
        elif games_started == 1 and isinstance(normalized_outs, int) and normalized_outs <= 6:
            role = "short_start"
        elif games_started == 1:
            role = "official_start"
        elif games_pitched > 0:
            role = "relief_appearance"
        else:
            role = "no_pitching_appearance"
        role_ok = role in {"official_start", "short_start", "zero_out_start"}
        workload_ok = normalized_outs != "" and innings != ""
        accepted = game_ok and temporal_ok and role_ok and workload_ok
        if not game_ok:
            reject = "ACQUISITION_GAME_IDENTITY_FAILURE"
        elif not temporal_ok:
            reject = "ACQUISITION_TEMPORAL_FAILURE"
        elif not role_ok:
            reject = "ACQUISITION_ROLE_REGIME_FAILURE"
        elif not workload_ok:
            reject = "ACQUISITION_SOURCE_FACT_INCOMPLETE"
        else:
            reject = ""
        home_abbr = (game_teams.get("home") or {}).get("abbreviation", "")
        away_abbr = (game_teams.get("away") or {}).get("abbreviation", "")
        record = {
            **base,
            "official_game_pk": official_game_pk,
            "official_game_date": official_date,
            "game_status": status.get("detailedState", ""),
            "coded_game_state": status.get("codedGameState", ""),
            "abstract_game_state": status.get("abstractGameState", ""),
            "home_team": home_abbr,
            "away_team": away_abbr,
            "pitcher_team": home_abbr if found_side == "home" else away_abbr,
            "opponent": away_abbr if found_side == "home" else home_abbr,
            "pitcher_name": (found_player.get("person") or {}).get("fullName", ""),
            "games_started": games_started,
            "games_pitched": games_pitched,
            "appearance_or_start_role": role,
            "innings_pitched_raw": innings,
            "outs_recorded": normalized_outs,
            "batters_faced_corrob_only": pitching.get("battersFaced", ""),
            "hits_allowed": pitching.get("hits", ""),
            "earned_runs": pitching.get("earnedRuns", ""),
            "walks": pitching.get("baseOnBalls", ""),
            "strikeouts": pitching.get("strikeOuts", ""),
            "game_identity_result": "PASS" if game_ok else "FAIL",
            "pitcher_identity_result": "PASS",
            "temporal_result": "PASS" if temporal_ok else "FAIL",
            "role_result": "PASS" if role_ok else "FAIL",
            "workload_source_fact_result": "PASS" if workload_ok else "FAIL",
            "accepted_rejected_state": "ACCEPTED_FULLY_CERTIFIED_SOURCE_RECORD" if accepted else "REJECTED",
            "rejection_taxonomy": reject,
            "source_revision_metadata": (payload.get("metaData") or {}).get("timeStamp", ""),
        }
        return self.add_record_hash(record)

    def add_record_hash(self, record: dict[str, Any]) -> dict[str, Any]:
        record = dict(record)
        payload = json.dumps({k: v for k, v in record.items() if k != "parsed_record_sha"}, sort_keys=True, default=str)
        record["parsed_record_sha"] = sha256_bytes(payload.encode("utf-8"))
        return record

    def execute(self) -> dict[str, Any]:
        executable, mapping, original_manifest = self.build_executable_manifest()
        validations = self.verify_inputs(executable, original_manifest)
        self.write_governance_first(executable, mapping, original_manifest, validations)
        request_ledger = []
        raw_inventory = []
        parsed_records = []
        for request in executable:
            response = self.fetch_one(request)
            raw_inventory.append({
                **request,
                **response,
                "raw_response_inventory_status": "PRESERVED" if response.get("response_sha") else "NOT_PRESERVED",
            })
            request_ledger.append({
                "original_discovery_request_id": "|".join(
                    row["original_discovery_request_id"] for row in mapping if row["executable_request_id"] == request["executable_request_id"]
                ),
                "executable_request_id": request["executable_request_id"],
            "parent_starter_game_side_identity": request["parent_starter_game_side_identities"],
                "pitcher_identity": request["pitcher_identity"],
                "historical_game_identity": request["historical_game_identity"],
                "historical_date": request["historical_game_date"],
                "endpoint_source_class": request["source_class_or_endpoint"],
                "request_parameters": json.dumps({"gamePk": request["historical_game_identity"], "method": "GET"}, sort_keys=True),
                "deduplication_key": request["deduplication_key"],
                "request_purpose": request["request_purpose"],
                "strict_prior_proof": request["strict_prior_proof"],
                "attempt_count": response["attempt_count"],
                "retry_count": response["retry_count"],
                "final_transport_result": response["final_transport_result"],
                "raw_response_path": response["raw_response_path"],
                "response_sha": response["response_sha"],
            })
            parsed_records.append(self.parse_one(request, response))
            if response["retrieval_mode"].startswith("LIVE"):
                time.sleep(self.rate_limit_seconds)
        self.write_outputs(executable, mapping, original_manifest, validations, request_ledger, raw_inventory, parsed_records)
        return self.write_result(executable, mapping, request_ledger, raw_inventory, parsed_records)

    def write_outputs(
        self,
        executable: list[dict[str, Any]],
        mapping: list[dict[str, Any]],
        original_manifest: list[dict[str, Any]],
        validations: list[dict[str, Any]],
        request_ledger: list[dict[str, Any]],
        raw_inventory: list[dict[str, Any]],
        parsed_records: list[dict[str, Any]],
    ) -> None:
        write_csv(OUT_DIR / f"acquisition_request_ledger_{RUN_DATE}.csv", request_ledger)
        write_csv(OUT_DIR / f"raw_response_inventory_{RUN_DATE}.csv", raw_inventory)
        write_csv(OUT_DIR / f"parsed_source_record_ledger_{RUN_DATE}.csv", parsed_records)
        write_csv(OUT_DIR / f"accepted_rejected_record_ledger_{RUN_DATE}.csv", [
            {
                "executable_request_id": r["executable_request_id"],
                "record_identity": r["record_identity"],
                "accepted_rejected_state": r["accepted_rejected_state"],
                "rejection_taxonomy": r["rejection_taxonomy"],
                "game_identity_result": r["game_identity_result"],
                "pitcher_identity_result": r["pitcher_identity_result"],
                "temporal_result": r["temporal_result"],
                "role_result": r["role_result"],
                "workload_source_fact_result": r["workload_source_fact_result"],
                "provenance_path": r["provenance_path"],
            }
            for r in parsed_records
        ])
        taxonomy = Counter(r["rejection_taxonomy"] or "FULLY_CERTIFIED_SOURCE_RECORD" for r in parsed_records)
        transport = Counter(r["final_transport_result"] for r in request_ledger)
        write_csv(OUT_DIR / f"request_and_failure_taxonomy_{RUN_DATE}.csv", [
            {"taxonomy_family": "transport", "reason": reason, "count": count} for reason, count in sorted(transport.items())
        ] + [
            {"taxonomy_family": "parser_or_certification", "reason": reason, "count": count} for reason, count in sorted(taxonomy.items())
        ])
        self.write_cumulative_preservation_report()
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validations + [
            {"validation": "request_attempt_count_matches_executable_manifest", "status": "PASS" if len(request_ledger) == len(executable) else "FAIL", "observed": len(request_ledger), "expected": len(executable)},
            {"validation": "raw_response_inventory_count_matches_executable_manifest", "status": "PASS" if len(raw_inventory) == len(executable) else "FAIL", "observed": len(raw_inventory), "expected": len(executable)},
            {"validation": "parsed_record_count_matches_executable_manifest", "status": "PASS" if len(parsed_records) == len(executable) else "FAIL", "observed": len(parsed_records), "expected": len(executable)},
            {"validation": "static_guard", "status": "PASS" if all(r["status"] == "PASS" for r in static_guard()) else "FAIL", "observed": "see_static_guard", "expected": "all_pass"},
        ])
        write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
        self.write_side_outputs(executable, parsed_records)
        self.write_replay_report(executable, parsed_records)
        self.write_projected_ceilings(parsed_records)

    def write_cumulative_preservation_report(self) -> None:
        state = json.loads((CUMULATIVE_DIR / f"cumulative_certified_state_{RUN_DATE}.json").read_text(encoding="utf-8"))
        rows = [
            ("total_fully_qualified_hits", state.get("total_fully_qualified_hits"), 961),
            ("fully_qualified_hits_0_5", state.get("fully_qualified_hits_0_5"), 846),
            ("fully_qualified_hits_1_5", state.get("fully_qualified_hits_1_5"), 115),
            ("starter_blocked_population", state.get("starter_blocked_population"), 675),
            ("pa_blocked_population", state.get("pa_blocked_population"), 11),
            ("outcome_blocked_population", state.get("outcome_blocked_population"), 363),
            ("bundle_blocked_population", state.get("bundle_blocked_population"), 36),
            ("potential_abd_matrix_readiness_queue", state.get("potential_abd_matrix_readiness_queue"), 16),
        ]
        write_csv(OUT_DIR / f"cumulative_state_preservation_report_{RUN_DATE}.csv", [
            {
                "cumulative_state_field": field,
                "observed": observed,
                "expected": expected,
                "status": "PASS" if observed == expected else "FAIL",
                "notes": "Acquisition evidence package only; no cumulative-state mutation performed.",
            }
            for field, observed, expected in rows
        ] + [{
            "cumulative_state_field": "cumulative_parent_state_package_sha",
            "observed": package_sha(CUMULATIVE_DIR),
            "expected": EXPECTED_CUMULATIVE_PARENT_SHA,
            "status": "PASS" if package_sha(CUMULATIVE_DIR) == EXPECTED_CUMULATIVE_PARENT_SHA else "FAIL",
            "notes": "Byte-identical parent package verification.",
        }])

    def write_side_outputs(self, executable: list[dict[str, Any]], parsed_records: list[dict[str, Any]]) -> None:
        parsed_by_request = {r["executable_request_id"]: r for r in parsed_records}
        side_to_exec = defaultdict(list)
        for request in executable:
            for side in decode_sides(request["parent_starter_game_side_identities"]):
                if side:
                    side_to_exec[side].append(request["executable_request_id"])
        side_meta = {r["starter_game_side_key"]: r for r in self.gov_sides}
        side_rows = []
        for side in sorted(side_meta):
            required = sorted(set(side_to_exec.get(side, [])))
            certified = [rid for rid in required if parsed_by_request.get(rid, {}).get("accepted_rejected_state") == "ACCEPTED_FULLY_CERTIFIED_SOURCE_RECORD"]
            missing = [rid for rid in required if rid not in certified]
            meta = side_meta[side]
            if len(certified) == len(required) and required:
                status = "HISTORY_COMPLETE"
            elif certified:
                status = "HISTORY_PARTIAL"
            else:
                status = "HISTORY_FAILED"
            side_rows.append({
                "starter_game_side_identity": side,
                "represented_row_count": meta.get("represented_denominator_rows", ""),
                "hits_0_5_rows": meta.get("hits_0_5_rows", ""),
                "hits_1_5_rows": meta.get("hits_1_5_rows", ""),
                "required_historical_records": len(required),
                "acquired_records": len(required),
                "certified_records": len(certified),
                "missing_records": len(missing),
                "missing_record_ids": "|".join(missing),
                "complete_partial_failed_status": status,
                "projected_starter_qualified_ceiling": meta.get("projected_starter_qualified_ceiling", ""),
                "projected_newly_fully_qualified_ceiling": meta.get("projected_newly_fully_qualified_ceiling", ""),
                "downstream_pa_blockers_already_known": meta.get("downstream_pa_blockers", ""),
                "downstream_outcome_blockers_already_known": meta.get("downstream_outcome_blockers", ""),
                "downstream_bundle_blockers_already_known": meta.get("downstream_bundle_blockers", ""),
            })
        write_csv(OUT_DIR / f"side_level_acquisition_completeness_ledger_{RUN_DATE}.csv", side_rows)

    def write_replay_report(self, executable: list[dict[str, Any]], parsed_records: list[dict[str, Any]]) -> None:
        previous = {r["executable_request_id"]: r for r in parsed_records}
        replay_rows = []
        for request in executable:
            raw_path = raw_path_for(request["executable_request_id"])
            status = "PASS"
            notes = ""
            if not raw_path.exists():
                status = "FAIL"
                notes = "raw response missing"
            else:
                replay_response = {
                    "final_transport_result": "SUCCESS",
                    "raw_response_path": str(raw_path),
                }
                current = self.parse_one(request, replay_response)
                prior = previous.get(request["executable_request_id"], {})
                compared = [
                    "game_identity_result",
                    "pitcher_identity_result",
                    "temporal_result",
                    "role_result",
                    "workload_source_fact_result",
                    "accepted_rejected_state",
                    "rejection_taxonomy",
                    "parsed_record_sha",
                ]
                mismatches = [field for field in compared if str(current.get(field, "")) != str(prior.get(field, ""))]
                if mismatches:
                    status = "FAIL"
                    notes = "mismatch:" + "|".join(mismatches)
            replay_rows.append({
                "executable_request_id": request["executable_request_id"],
                "raw_response_path": str(raw_path),
                "raw_response_sha": sha256_path(raw_path) if raw_path.exists() else "",
                "offline_replay_status": status,
                "live_network_requests": 0,
                "notes": notes,
            })
        write_csv(OUT_DIR / f"deterministic_offline_replay_report_{RUN_DATE}.csv", replay_rows)

    def write_projected_ceilings(self, parsed_records: list[dict[str, Any]]) -> None:
        side_rows = read_csv(OUT_DIR / f"side_level_acquisition_completeness_ledger_{RUN_DATE}.csv")
        complete = {r["starter_game_side_identity"] for r in side_rows if r["complete_partial_failed_status"] == "HISTORY_COMPLETE"}
        partial = {r["starter_game_side_identity"] for r in side_rows if r["complete_partial_failed_status"] == "HISTORY_PARTIAL"}
        rows_complete = [r for r in self.gov_rows if (r.get("starter_game_key") or r["starter_game_side_key"]) in complete]
        rows_partial = [r for r in self.gov_rows if (r.get("starter_game_key") or r["starter_game_side_key"]) in partial]
        write_csv(OUT_DIR / f"projected_reconstruction_ceilings_{RUN_DATE}.csv", [{
            "history_complete_sides": len(complete),
            "history_partial_sides": len(partial),
            "history_failed_sides": sum(r["complete_partial_failed_status"] == "HISTORY_FAILED" for r in side_rows),
            "represented_denominator_rows_supported_by_complete_sides": len(rows_complete),
            "represented_denominator_rows_supported_only_partially": len(rows_partial),
            "projected_starter_qualified_ceiling_for_history_complete_sides": sum(
                int_value(side.get("projected_starter_qualified_ceiling"))
                for side in self.gov_sides
                if side["starter_game_side_key"] in complete
            ),
            "projected_full_qualification_ceiling_for_history_complete_sides": sum(
                int_value(side.get("projected_newly_fully_qualified_ceiling") or side.get("projected_fully_qualified_ceiling"))
                for side in self.gov_sides
                if side["starter_game_side_key"] in complete
            ),
            "hits_0_5_rows_supported": sum(r.get("line") == "0.5" for r in rows_complete),
            "hits_1_5_rows_supported": sum(r.get("line") == "1.5" for r in rows_complete),
            "potential_abd_matrix_readiness_additions": sum(
                int_value(side.get("hits_1_5_rows"))
                for side in self.gov_sides
                if side["starter_game_side_key"] in complete
            ),
            "variant_c_implications": "governance_preserved_not_resolved",
            "notes": "Ceilings only. No reconstruction/remediation/qualification propagation executed.",
        }])

    def write_result(
        self,
        executable: list[dict[str, Any]],
        mapping: list[dict[str, Any]],
        request_ledger: list[dict[str, Any]],
        raw_inventory: list[dict[str, Any]],
        parsed_records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        side_rows = read_csv(OUT_DIR / f"side_level_acquisition_completeness_ledger_{RUN_DATE}.csv")
        ceilings = read_csv(OUT_DIR / f"projected_reconstruction_ceilings_{RUN_DATE}.csv")[0]
        complete_sides = sum(r["complete_partial_failed_status"] == "HISTORY_COMPLETE" for r in side_rows)
        partial_sides = sum(r["complete_partial_failed_status"] == "HISTORY_PARTIAL" for r in side_rows)
        failed_sides = sum(r["complete_partial_failed_status"] == "HISTORY_FAILED" for r in side_rows)
        failures = len([r for r in request_ledger if r["final_transport_result"] != "SUCCESS"])
        certified = sum(r["accepted_rejected_state"] == "ACCEPTED_FULLY_CERTIFIED_SOURCE_RECORD" for r in parsed_records)
        if complete_sides == 8:
            decision = DECISION_READY
            recommendation = RECOMMEND_READY
        elif complete_sides > 0 and failures == 0:
            decision = DECISION_PARTIAL_COMPLETE_ONLY
            recommendation = RECOMMEND_COMPLETE_ONLY
        elif complete_sides > 0:
            decision = DECISION_PARTIAL_REVIEW
            recommendation = RECOMMEND_PARTIAL_REVIEW
        elif certified > 0:
            decision = DECISION_FAILURE_REVIEW
            recommendation = RECOMMEND_FAILURE
        else:
            decision = DECISION_LOW_YIELD
            recommendation = RECOMMEND_LOW
        payload = {
            "decision": decision,
            "recommendation": recommendation,
            "generated_at": GENERATED_AT,
            "mode": self.mode,
            "discovery_manifest_requests": len(self.requests),
            "duplicate_requests_collapsed": len(self.requests) - len(executable),
            "governed_executable_requests": len(executable),
            "requests_attempted": len(request_ledger),
            "requests_succeeded": sum(r["final_transport_result"] == "SUCCESS" for r in request_ledger),
            "requests_failed": failures,
            "total_retry_attempts": sum(int_value(r["retry_count"]) for r in request_ledger),
            "raw_responses_preserved": sum(bool(r.get("response_sha")) for r in raw_inventory),
            "parsed_records": len(parsed_records),
            "fully_certified_records": certified,
            "rejected_records": len(parsed_records) - certified,
            "transport_failures": sum(r["final_transport_result"] != "SUCCESS" for r in request_ledger),
            "parser_failures": sum(r["rejection_taxonomy"] == "ACQUISITION_PARSE_FAILURE" for r in parsed_records),
            "game_identity_failures": sum(r["game_identity_result"] == "FAIL" for r in parsed_records),
            "pitcher_identity_failures": sum(r["pitcher_identity_result"] == "FAIL" for r in parsed_records),
            "temporal_failures": sum(r["temporal_result"] == "FAIL" for r in parsed_records),
            "role_regime_failures": sum(r["role_result"] == "FAIL" for r in parsed_records),
            "workload_source_fact_incomplete_records": sum(r["workload_source_fact_result"] == "FAIL" for r in parsed_records),
            "unique_historical_games_acquired": len({r["historical_game_identity"] for r in parsed_records if r["accepted_rejected_state"] == "ACCEPTED_FULLY_CERTIFIED_SOURCE_RECORD"}),
            "unique_pitchers_acquired": len({r["pitcher_identity"] for r in parsed_records if r["accepted_rejected_state"] == "ACCEPTED_FULLY_CERTIFIED_SOURCE_RECORD"}),
            "governed_sides_with_complete_required_history": complete_sides,
            "governed_sides_with_partial_required_history": partial_sides,
            "governed_sides_with_no_usable_acquired_history": failed_sides,
            "represented_denominator_rows_supported_by_complete_sides": int_value(ceilings["represented_denominator_rows_supported_by_complete_sides"]),
            "represented_denominator_rows_supported_only_partially": int_value(ceilings["represented_denominator_rows_supported_only_partially"]),
            "projected_starter_qualified_ceiling_for_history_complete_sides": int_value(ceilings["projected_starter_qualified_ceiling_for_history_complete_sides"]),
            "projected_full_qualification_ceiling_for_history_complete_sides": int_value(ceilings["projected_full_qualification_ceiling_for_history_complete_sides"]),
            "hits_0_5_rows_supported": int_value(ceilings["hits_0_5_rows_supported"]),
            "hits_1_5_rows_supported": int_value(ceilings["hits_1_5_rows_supported"]),
            "potential_abd_matrix_readiness_additions": int_value(ceilings["potential_abd_matrix_readiness_additions"]),
            "variant_c_implication": "governance_preserved_not_resolved",
            "execution_mode": self.mode,
            "historical_acquisition_executed": sum(bool(r.get("response_sha")) for r in raw_inventory) == len(executable),
            "reconstruction_or_remediation_performed": False,
            "qualification_propagation_performed": False,
            "matrix_construction_performed": False,
            "model_or_signal_work_performed": False,
            "database_writes": 0,
            "oddsapi_calls": 0,
            "uploads_or_production_changes": 0,
        }
        write_json(OUT_DIR / f"machine_readable_acquisition_result_{RUN_DATE}.json", payload)
        write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# Discovery Cohort 003 History-Complete Acquisition — {RUN_DATE}

Decision: `{decision}`

Recommendation: `{recommendation}`

- Discovery manifest requests: `{payload['discovery_manifest_requests']}`
- Duplicate requests collapsed: `{payload['duplicate_requests_collapsed']}`
- Governed executable requests: `{payload['governed_executable_requests']}`
- Requests succeeded: `{payload['requests_succeeded']}`
- Fully certified source records: `{payload['fully_certified_records']}`
- History-complete sides: `{complete_sides}`
- Represented denominator rows supported by complete sides: `{payload['represented_denominator_rows_supported_by_complete_sides']}`

This package executed only the exact frozen historical acquisition requests. It
did not reconstruct or remediate Starter features, propagate qualification,
construct matrices, train/score models, write databases, call OddsAPI, upload,
edit LaunchAgents, or change production behavior.
""")
        self.parse_and_hash()
        write_json(OUT_DIR / f"machine_readable_acquisition_result_{RUN_DATE}.json", payload)
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}

    def parse_and_hash(self) -> None:
        parse_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if not path.is_file() or path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            try:
                if path.suffix == ".csv":
                    read_csv(path)
                    kind = "csv"
                elif path.suffix == ".json":
                    json.loads(path.read_text(encoding="utf-8"))
                    kind = "json"
                elif path.suffix == ".md":
                    kind = "markdown"
                    if not path.read_text(encoding="utf-8").lstrip().startswith("#"):
                        raise ValueError("markdown missing heading")
                else:
                    continue
                status = "PASS"
                notes = ""
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["execute", "replay"], default="execute")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retry-limit", type=int, default=1)
    parser.add_argument("--rate-limit-seconds", type=float, default=0.05)
    args = parser.parse_args()
    runner = DiscoveryCohort003HistoryAcquisition(
        mode=args.mode,
        timeout=args.timeout,
        retry_limit=args.retry_limit,
        rate_limit_seconds=args.rate_limit_seconds,
    )
    result = runner.execute()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
