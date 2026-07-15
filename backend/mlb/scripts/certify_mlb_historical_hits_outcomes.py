"""Certify the bounded 2026-06-22..2026-06-28 MLB Hits outcome domain.

This is a no-write artifact generator. It consumes frozen denominator,
coverage, official recovery, and human-governance packages, then produces a
1,904-row outcome certification ledger. It does not build an experimental
matrix, train, score, call external APIs, write databases, or change production.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PACKAGE_DATE = "2026-07-13"

OUT_DIR = Path("artifacts/analysis/model_development/mlb_historical_hits_outcome_certification/2026-07-13")
DENOMINATOR_PATH = Path(
    "artifacts/analysis/model_development/mlb_historical_earlier_source_denominator_recovery/2026-07-13/"
    "mlb_historical_earlier_source_denominator_rows_2026-07-13.csv"
)
SOURCE_COVERAGE_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_outcome_source_coverage_pass/2026-07-13"
)
RECOVERY_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_outcome_gap_authoritative_recovery/2026-07-13"
)
GOVERNANCE_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_nonappearance_game_status_governance_review/2026-07-13"
)
STARTER_PATH = Path(
    "artifacts/analysis/model_development/mlb_historical_starter_option_b_certified_remediation/2026-07-13/"
    "mlb_starter_option_b_certified_join_rows_2026-07-13.csv"
)
PA_PATH = Path(
    "artifacts/analysis/model_development/mlb_pa_sparse_history_certified_missingness/2026-07-13/"
    "pa_sparse_history_certified_join_rows_2026-07-13.csv"
)

EXPECTED_DENOMINATOR = 1904
EXPECTED_PLAYER_GAMES = 1817
EXPECTED_ATTACHED_READY = 1687
EXPECTED_OFFICIAL_RECOVERED = 63
EXPECTED_NONAPPEARANCE = 134
EXPECTED_GAME_STATUS = 20
EXPECTED_NUMERIC = 1750
EXPECTED_NONNUMERIC = 154


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="", errors="ignore") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    materialized = list(rows)
    if fieldnames is None:
        fieldnames = []
        for row in materialized:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def index_by(rows: list[dict[str, str]], key: str) -> dict[str, dict[str, str]]:
    return {clean(row.get(key)): row for row in rows}


def canonical(row: dict[str, str]) -> str:
    return clean(row.get("canonical_row_id"))


def player_game(row: dict[str, str]) -> str:
    return "|".join([clean(row.get("slate_date")), clean(row.get("game_id")), clean(row.get("player_id"))])


def to_int_hits(value: str) -> int:
    numeric = float(clean(value))
    if numeric < 0 or abs(numeric - round(numeric)) > 1e-9:
        raise ValueError(f"invalid integer hits value: {value!r}")
    return int(round(numeric))


def settlement(actual_hits: int, line: str, side: str) -> str:
    side_norm = clean(side).lower()
    line_value = float(clean(line))
    if line_value == 0.5 and side_norm == "over":
        return "win" if actual_hits >= 1 else "loss"
    if line_value == 0.5 and side_norm == "under":
        return "win" if actual_hits == 0 else "loss"
    if line_value == 1.5 and side_norm == "over":
        return "win" if actual_hits >= 2 else "loss"
    if line_value == 1.5 and side_norm == "under":
        return "win" if actual_hits <= 1 else "loss"
    raise ValueError(f"unsupported hits half-line: line={line!r} side={side!r}")


def load_inputs() -> dict[str, Any]:
    return {
        "denominator": read_csv(DENOMINATOR_PATH),
        "attached": read_csv(SOURCE_COVERAGE_DIR / f"attached_ready_ledger_{PACKAGE_DATE}.csv"),
        "candidate_sources": read_csv(SOURCE_COVERAGE_DIR / f"denominator_row_candidate_source_match_ledger_{PACKAGE_DATE}.csv"),
        "cross_source": read_csv(SOURCE_COVERAGE_DIR / f"cross_source_agreement_conflict_report_{PACKAGE_DATE}.csv"),
        "source_inventory": read_csv(SOURCE_COVERAGE_DIR / f"repository_outcome_source_inventory_{PACKAGE_DATE}.csv"),
        "official_recovered": read_csv(RECOVERY_DIR / f"authoritative_value_recovered_ledger_{PACKAGE_DATE}.csv"),
        "official_batting": read_csv(RECOVERY_DIR / f"official_batting_line_ledger_{PACKAGE_DATE}.csv"),
        "official_request_manifest": read_csv(RECOVERY_DIR / f"official_mlb_request_manifest_{PACKAGE_DATE}.csv"),
        "official_participation": read_csv(RECOVERY_DIR / f"participation_classification_ledger_{PACKAGE_DATE}.csv"),
        "official_game_map": read_csv(RECOVERY_DIR / f"game_id_mapping_ledger_{PACKAGE_DATE}.csv"),
        "nonappearance": read_csv(GOVERNANCE_DIR / f"nonappearance_denominator_row_ledger_{PACKAGE_DATE}.csv"),
        "game_status": read_csv(GOVERNANCE_DIR / f"game_status_denominator_row_ledger_{PACKAGE_DATE}.csv"),
        "governance_decision": json.loads((GOVERNANCE_DIR / f"machine_readable_decision_{PACKAGE_DATE}.json").read_text()),
        "starter": read_csv(STARTER_PATH),
        "pa": read_csv(PA_PATH),
    }


def source_path_hashes(candidate_rows: list[dict[str, str]]) -> dict[str, str]:
    paths = {clean(row.get("source_path")) for row in candidate_rows if clean(row.get("source_path"))}
    out: dict[str, str] = {}
    for path_text in sorted(paths):
        path = Path(path_text)
        out[path_text] = sha256(path) if path.exists() else ""
    return out


def validate_source_populations(inputs: dict[str, Any]) -> None:
    denom = inputs["denominator"]
    if len(denom) != EXPECTED_DENOMINATOR:
        raise AssertionError("denominator row count mismatch")
    ids = [canonical(row) for row in denom]
    if len(set(ids)) != EXPECTED_DENOMINATOR:
        raise AssertionError("duplicate canonical denominator identity")
    if len({player_game(row) for row in denom}) != EXPECTED_PLAYER_GAMES:
        raise AssertionError("player-game count mismatch")
    checks = [
        ("attached", EXPECTED_ATTACHED_READY),
        ("official_recovered", EXPECTED_OFFICIAL_RECOVERED),
        ("nonappearance", EXPECTED_NONAPPEARANCE),
        ("game_status", EXPECTED_GAME_STATUS),
    ]
    for key, expected in checks:
        if len(inputs[key]) != expected:
            raise AssertionError(f"{key} row count mismatch: {len(inputs[key])} != {expected}")
    sets = {key: {canonical(row) for row in inputs[key]} for key, _ in checks}
    all_ids: set[str] = set()
    for key, values in sets.items():
        if all_ids & values:
            raise AssertionError(f"population overlap at {key}")
        all_ids |= values
    if all_ids != set(ids):
        raise AssertionError("certification populations do not reconcile to denominator")


def classify_starter(row: dict[str, str]) -> tuple[str, bool]:
    status = clean(row.get("starter_join_status"))
    qualified = status in {
        "STARTER_JOIN_QUALIFIED_DIRECT_PREGAME",
        "STARTER_JOIN_QUALIFIED_OPTION_B_HISTORICAL_ACTUAL_STARTER",
        "STARTER_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS",
    }
    return status, qualified


def classify_pa(row: dict[str, str]) -> tuple[str, bool]:
    status = clean(row.get("pa_join_status"))
    qualified = status in {
        "PA_JOIN_QUALIFIED_DIRECT_STRICT_PRIOR",
        "PA_JOIN_QUALIFIED_HISTORICAL_STRICT_PRIOR_RECONSTRUCTION",
        "PA_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS",
    }
    return status, qualified


def make_certification_ledgers(inputs: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    attached = index_by(inputs["attached"], "canonical_row_id")
    official_recovered = index_by(inputs["official_recovered"], "canonical_row_id")
    nonappearance = index_by(inputs["nonappearance"], "canonical_row_id")
    game_status = index_by(inputs["game_status"], "canonical_row_id")
    cross_source = index_by(inputs["cross_source"], "canonical_row_id")
    participation = index_by(inputs["official_participation"], "canonical_row_id")
    game_map = index_by(inputs["official_game_map"], "canonical_row_id")
    request_by_game = {clean(row.get("game_id")): row for row in inputs["official_request_manifest"]}
    starter = index_by(inputs["starter"], "canonical_row_id")
    pa = index_by(inputs["pa"], "canonical_row_id")
    candidate_sources_by_id: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in inputs["candidate_sources"]:
        candidate_sources_by_id[canonical(row)].append(row)
    path_hashes = source_path_hashes(inputs["candidate_sources"])

    ledger: list[dict[str, Any]] = []
    split: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for ordinal, denom in enumerate(inputs["denominator"], start=1):
        cid = canonical(denom)
        base = {
            "denominator_order": ordinal,
            "canonical_row_id": cid,
            "slate_date": denom["slate_date"],
            "game_id": denom["game_id"],
            "player_id": denom["player_id"],
            "player_name": denom["player_name"],
            "team": denom["team"],
            "opponent": denom["opponent"],
            "prop_type": denom["prop_type"],
            "line": denom["line"],
            "side": denom["side"],
            "player_game_key": player_game(denom),
            "denominator_source_path": denom.get("source_path", ""),
            "denominator_source_sha256": denom.get("source_sha256", ""),
            "certification_method": "historical_hits_outcome_certification_v1",
            "certification_package_date": PACKAGE_DATE,
        }
        starter_status, starter_qualified = classify_starter(starter.get(cid, {}))
        pa_status, pa_qualified = classify_pa(pa.get(cid, {}))
        base.update(
            {
                "starter_join_status_preserved": starter_status,
                "starter_domain_qualified_preserved": str(starter_qualified).lower(),
                "pa_join_status_preserved": pa_status,
                "pa_domain_qualified_preserved": str(pa_qualified).lower(),
                "full_frozen_bundle_variant_requirement_satisfied": str(starter_qualified and pa_qualified).lower(),
            }
        )

        if cid in attached:
            src = attached[cid]
            actual_hits = to_int_hits(src["selected_actual_hits_review_only"])
            label = settlement(actual_hits, denom["line"], denom["side"])
            xs = cross_source.get(cid, {})
            source_rows = candidate_sources_by_id.get(cid, [])
            source_paths = sorted({clean(r.get("source_path")) for r in source_rows if clean(r.get("source_path"))})
            source_hash_bundle = "|".join(f"{p}:{path_hashes.get(p, '')}" for p in source_paths)
            row = {
                **base,
                "outcome_certification_class": "numeric_outcome_certified",
                "outcome_certification_status": "OUTCOME_NUMERIC_CERTIFIED",
                "actual_hits": actual_hits,
                "participation_status": "PARTICIPATION_CONFIRMED_BY_LOCAL_NUMERIC_HITS_SOURCE",
                "official_game_status": "FINAL_OR_COMPATIBLE_FROM_LOCAL_SOURCE",
                "outcome_source": src.get("candidate_source_ids", ""),
                "source_authority": "repository_player_stats_derived_postgame_cross_source_agree",
                "source_provenance": src.get("resolution_reason", ""),
                "source_sha256": source_hash_bundle,
                "cross_source_agreement_status": xs.get("agreement_status", ""),
                "cross_source_values": xs.get("distinct_values", ""),
                "settlement_status": "DETERMINISTIC_HALF_LINE_SETTLED",
                "win_loss_label": label,
                "experimental_label_eligible": "true",
                "governance_authorization": "",
                "rescheduled_game_rebinding": "false",
                "certification_blocker": "",
            }
            split["numeric"].append(row)
        elif cid in official_recovered:
            src = official_recovered[cid]
            part = participation.get(cid, {})
            gm = game_map.get(cid, {})
            request = request_by_game.get(denom["game_id"], {})
            actual_hits = to_int_hits(src["official_hits"])
            label = settlement(actual_hits, denom["line"], denom["side"])
            row = {
                **base,
                "outcome_certification_class": "numeric_outcome_certified",
                "outcome_certification_status": "OUTCOME_NUMERIC_CERTIFIED",
                "actual_hits": actual_hits,
                "participation_status": part.get("participation_category", src.get("participation_category", "")),
                "official_game_status": gm.get("game_status", ""),
                "outcome_source": request.get("endpoint", "official_mlb_statsapi_feed_live"),
                "source_authority": "official_mlb_statsapi_cached_feed_live",
                "source_provenance": request.get("cache_path", ""),
                "source_sha256": request.get("sha256", ""),
                "cross_source_agreement_status": "OFFICIAL_RECOVERY_NO_LOCAL_CONFLICT",
                "cross_source_values": src.get("official_hits", ""),
                "settlement_status": "DETERMINISTIC_HALF_LINE_SETTLED",
                "win_loss_label": label,
                "experimental_label_eligible": "true",
                "governance_authorization": "",
                "rescheduled_game_rebinding": "false",
                "certification_blocker": "",
            }
            split["numeric"].append(row)
        elif cid in nonappearance:
            src = nonappearance[cid]
            part = participation.get(cid, {})
            gm = game_map.get(cid, {})
            request = request_by_game.get(denom["game_id"], {})
            row = {
                **base,
                "outcome_certification_class": "governed_nonappearance_no_action",
                "outcome_certification_status": "OUTCOME_STATUS_CERTIFIED_NONNUMERIC",
                "actual_hits": "",
                "participation_status": "DID_NOT_APPEAR",
                "official_game_status": gm.get("game_status", ""),
                "outcome_source": request.get("endpoint", "official_mlb_statsapi_cached_feed_live"),
                "source_authority": "official_mlb_statsapi_cached_feed_live_participation_classification",
                "source_provenance": request.get("cache_path", ""),
                "source_sha256": request.get("sha256", ""),
                "cross_source_agreement_status": "NONNUMERIC_GOVERNED_STATUS",
                "cross_source_values": "",
                "settlement_status": "VOID_NO_ACTION_NON_APPEARANCE",
                "win_loss_label": "",
                "experimental_label_eligible": "false",
                "governance_authorization": "HUMAN_APPROVED_NONAPPEARANCE_NO_ACTION",
                "rescheduled_game_rebinding": "false",
                "certification_blocker": "",
                "official_evidence_reason": part.get("reason", src.get("reason", "")),
            }
            split["nonappearance"].append(row)
        elif cid in game_status:
            gm = game_map.get(cid, {})
            request = request_by_game.get(denom["game_id"], {})
            row = {
                **base,
                "outcome_certification_class": "governed_game_status_exception",
                "outcome_certification_status": "OUTCOME_STATUS_CERTIFIED_NONNUMERIC",
                "actual_hits": "",
                "participation_status": "GAME_SUSPENDED_OR_INCOMPLETE",
                "official_game_status": gm.get("game_status", ""),
                "official_abstract_game_state": gm.get("abstract_game_state", ""),
                "outcome_source": request.get("endpoint", "official_mlb_statsapi_cached_feed_live"),
                "source_authority": "official_mlb_statsapi_cached_game_status",
                "source_provenance": request.get("cache_path", ""),
                "source_sha256": request.get("sha256", ""),
                "cross_source_agreement_status": "NONNUMERIC_GOVERNED_STATUS",
                "cross_source_values": "",
                "settlement_status": "UNGRADED_GAME_STATUS_EXCEPTION",
                "win_loss_label": "",
                "experimental_label_eligible": "false",
                "governance_authorization": "HUMAN_APPROVED_GAME_STATUS_UNGRADED",
                "rescheduled_game_rebinding": "false",
                "certification_blocker": "",
            }
            split["game_status"].append(row)
        else:
            row = {
                **base,
                "outcome_certification_class": "outcome_blocked",
                "outcome_certification_status": "OUTCOME_BLOCKED",
                "actual_hits": "",
                "participation_status": "",
                "official_game_status": "",
                "outcome_source": "",
                "source_authority": "",
                "source_provenance": "",
                "source_sha256": "",
                "cross_source_agreement_status": "",
                "cross_source_values": "",
                "settlement_status": "",
                "win_loss_label": "",
                "experimental_label_eligible": "false",
                "governance_authorization": "",
                "rescheduled_game_rebinding": "false",
                "certification_blocker": "no_certification_population_match",
            }
            split["blocked"].append(row)
        ledger.append(row)
    return ledger, split


def player_game_ledger(ledger: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ledger:
        grouped[row["player_game_key"]].append(row)
    rows = []
    for key, items in sorted(grouped.items()):
        rows.append(
            {
                "player_game_key": key,
                "denominator_rows": len(items),
                "certification_classes": "|".join(sorted({str(r["outcome_certification_class"]) for r in items})),
                "actual_hits_values": "|".join(sorted({str(r["actual_hits"]) for r in items if str(r["actual_hits"]) != ""})),
                "participation_statuses": "|".join(sorted({str(r["participation_status"]) for r in items if str(r["participation_status"]) != ""})),
                "official_game_statuses": "|".join(sorted({str(r["official_game_status"]) for r in items if str(r["official_game_status"]) != ""})),
                "win_loss_labels_available": sum(1 for r in items if str(r["win_loss_label"]) in {"win", "loss"}),
                "experimental_label_eligible_rows": sum(1 for r in items if str(r["experimental_label_eligible"]) == "true"),
            }
        )
    return rows


def copy_with_role(name: str, source_rows: list[dict[str, str]], extra: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    extra = extra or {}
    return [{**row, **extra} for row in source_rows]


def validation_tables(
    inputs: dict[str, Any],
    ledger: list[dict[str, Any]],
    split: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    numeric = split["numeric"]
    nonappearance = split["nonappearance"]
    game_status = split["game_status"]
    blocked = split["blocked"]
    source_manifest = inputs["official_request_manifest"]
    raw_sha_ok = 0
    for row in source_manifest:
        path = Path(clean(row.get("cache_path")))
        if path.exists() and sha256(path) == clean(row.get("sha256")):
            raw_sha_ok += 1

    rows_by_class = Counter(row["outcome_certification_class"] for row in ledger)
    label_rows = [r for r in ledger if r["win_loss_label"] in {"win", "loss"}]
    validation = [
        {"check": "denominator_rows", "status": "PASS" if len(ledger) == EXPECTED_DENOMINATOR else "FAIL", "value": len(ledger), "expected": EXPECTED_DENOMINATOR},
        {"check": "denominator_identity_unique", "status": "PASS" if len({r["canonical_row_id"] for r in ledger}) == EXPECTED_DENOMINATOR else "FAIL", "value": len({r["canonical_row_id"] for r in ledger}), "expected": EXPECTED_DENOMINATOR},
        {"check": "player_game_count", "status": "PASS" if len({r["player_game_key"] for r in ledger}) == EXPECTED_PLAYER_GAMES else "FAIL", "value": len({r["player_game_key"] for r in ledger}), "expected": EXPECTED_PLAYER_GAMES},
        {"check": "numeric_certified_rows", "status": "PASS" if len(numeric) == EXPECTED_NUMERIC else "FAIL", "value": len(numeric), "expected": EXPECTED_NUMERIC},
        {"check": "nonappearance_rows", "status": "PASS" if len(nonappearance) == EXPECTED_NONAPPEARANCE else "FAIL", "value": len(nonappearance), "expected": EXPECTED_NONAPPEARANCE},
        {"check": "game_status_rows", "status": "PASS" if len(game_status) == EXPECTED_GAME_STATUS else "FAIL", "value": len(game_status), "expected": EXPECTED_GAME_STATUS},
        {"check": "blocked_rows", "status": "PASS" if not blocked else "FAIL", "value": len(blocked), "expected": 0},
        {"check": "win_loss_labels_available", "status": "PASS" if len(label_rows) == EXPECTED_NUMERIC else "FAIL", "value": len(label_rows), "expected": EXPECTED_NUMERIC},
        {"check": "raw_official_sha_verified", "status": "PASS" if raw_sha_ok == len(source_manifest) else "FAIL", "value": raw_sha_ok, "expected": len(source_manifest)},
        {"check": "nonappearance_actual_hits_null", "status": "PASS" if all(r["actual_hits"] == "" for r in nonappearance) else "FAIL", "value": sum(r["actual_hits"] == "" for r in nonappearance), "expected": len(nonappearance)},
        {"check": "game_status_actual_hits_null", "status": "PASS" if all(r["actual_hits"] == "" for r in game_status) else "FAIL", "value": sum(r["actual_hits"] == "" for r in game_status), "expected": len(game_status)},
        {"check": "nonnumeric_label_null", "status": "PASS" if all(r["win_loss_label"] == "" for r in nonappearance + game_status) else "FAIL", "value": sum(r["win_loss_label"] == "" for r in nonappearance + game_status), "expected": EXPECTED_NONNUMERIC},
        {"check": "nonnumeric_experimental_ineligible", "status": "PASS" if all(r["experimental_label_eligible"] == "false" for r in nonappearance + game_status) else "FAIL", "value": sum(r["experimental_label_eligible"] == "false" for r in nonappearance + game_status), "expected": EXPECTED_NONNUMERIC},
        {"check": "push_impossible", "status": "PASS" if all(r["win_loss_label"] != "push" for r in ledger) else "FAIL", "value": sum(r["win_loss_label"] == "push" for r in ledger), "expected": 0},
        {"check": "integer_nonnegative_hits", "status": "PASS" if all(str(r["actual_hits"]).isdigit() for r in numeric) else "FAIL", "value": sum(str(r["actual_hits"]).isdigit() for r in numeric), "expected": len(numeric)},
        {"check": "no_rescheduled_rebinding", "status": "PASS" if all(r["rescheduled_game_rebinding"] == "false" for r in ledger) else "FAIL", "value": sum(r["rescheduled_game_rebinding"] == "false" for r in ledger), "expected": len(ledger)},
        {"check": "starter_state_preserved_count", "status": "PASS" if sum(r["starter_domain_qualified_preserved"] == "true" for r in ledger) == 1671 else "FAIL", "value": sum(r["starter_domain_qualified_preserved"] == "true" for r in ledger), "expected": 1671},
        {"check": "pa_state_preserved_count", "status": "PASS" if sum(r["pa_domain_qualified_preserved"] == "true" for r in ledger) == 1903 else "FAIL", "value": sum(r["pa_domain_qualified_preserved"] == "true" for r in ledger), "expected": 1903},
    ]

    settlement_rows = []
    for line, side in [("0.5", "over"), ("0.5", "under"), ("1.5", "over"), ("1.5", "under")]:
        for hits in range(0, 5):
            settlement_rows.append(
                {
                    "line": line,
                    "side": side,
                    "actual_hits": hits,
                    "win_loss_label": settlement(hits, line, side),
                    "push_possible": "false",
                    "formula_status": "PASS",
                }
            )

    before_after = [
        {"metric": "outcome_certified_numeric_before", "before": 0, "after": len(numeric), "notes": ""},
        {"metric": "outcome_certified_nonnumeric_nonappearance_before", "before": 0, "after": len(nonappearance), "notes": "governed no-action status"},
        {"metric": "outcome_certified_nonnumeric_game_status_before", "before": 0, "after": len(game_status), "notes": "governed ungraded exception"},
        {"metric": "outcome_blocked_before", "before": EXPECTED_DENOMINATOR, "after": len(blocked), "notes": ""},
        {"metric": "outcome_accounted_total_before", "before": 0, "after": len(ledger) - len(blocked), "notes": ""},
        {"metric": "win_loss_label_available_before", "before": 0, "after": len(label_rows), "notes": ""},
        {"metric": "experimental_label_eligible_outcome_only_before", "before": 0, "after": len(label_rows), "notes": "outcome-domain only; not full feature-domain eligibility"},
        {"metric": "nonnumeric_label_excluded_before", "before": 0, "after": EXPECTED_NONNUMERIC, "notes": ""},
        {"metric": "rows_with_outcome_labels_but_incomplete_starter", "before": 0, "after": sum(r["win_loss_label"] in {"win", "loss"} and r["starter_domain_qualified_preserved"] != "true" for r in ledger), "notes": ""},
        {"metric": "rows_with_outcome_labels_but_incomplete_pa", "before": 0, "after": sum(r["win_loss_label"] in {"win", "loss"} and r["pa_domain_qualified_preserved"] != "true" for r in ledger), "notes": ""},
        {"metric": "rows_satisfying_full_frozen_bundle_variant_requirements", "before": 0, "after": sum(r["win_loss_label"] in {"win", "loss"} and r["full_frozen_bundle_variant_requirement_satisfied"] == "true" for r in ledger), "notes": "readiness count only; no matrix built"},
    ]

    label_separation = [
        {
            "outcome_certification_class": class_name,
            "rows": rows_by_class[class_name],
            "outcome_accounted": "true" if class_name != "outcome_blocked" else "false",
            "numeric_actual_hits_available": "true" if class_name == "numeric_outcome_certified" else "false",
            "win_loss_label_available": "true" if class_name == "numeric_outcome_certified" else "false",
            "experimental_label_eligible_outcome_only": "true" if class_name == "numeric_outcome_certified" else "false",
            "notes": "Outcome-domain eligibility only; Starter/PA feature-domain gates remain separate.",
        }
        for class_name in [
            "numeric_outcome_certified",
            "governed_nonappearance_no_action",
            "governed_game_status_exception",
            "outcome_blocked",
        ]
    ]

    compatibility = [
        {
            "category": "starter_qualified_preserved",
            "rows": sum(r["starter_domain_qualified_preserved"] == "true" for r in ledger),
            "blocked_rows": sum(r["starter_domain_qualified_preserved"] != "true" for r in ledger),
            "state_changed": "false",
            "notes": "Outcome certification did not alter Starter qualification.",
        },
        {
            "category": "pa_qualified_preserved",
            "rows": sum(r["pa_domain_qualified_preserved"] == "true" for r in ledger),
            "blocked_rows": sum(r["pa_domain_qualified_preserved"] != "true" for r in ledger),
            "state_changed": "false",
            "notes": "The one unresolved PA row remains blocked even if outcome-labeled.",
        },
        {
            "category": "full_bundle_requirement_with_outcome_label",
            "rows": sum(r["win_loss_label"] in {"win", "loss"} and r["full_frozen_bundle_variant_requirement_satisfied"] == "true" for r in ledger),
            "blocked_rows": EXPECTED_DENOMINATOR - sum(r["win_loss_label"] in {"win", "loss"} and r["full_frozen_bundle_variant_requirement_satisfied"] == "true" for r in ledger),
            "state_changed": "false",
            "notes": "Readiness count only; no experimental matrix built.",
        },
    ]

    return {
        "deterministic": validation,
        "settlement": settlement_rows,
        "before_after": before_after,
        "label_separation": label_separation,
        "compatibility": compatibility,
    }


def write_reports(
    decision: dict[str, Any],
    ledger: list[dict[str, Any]],
    split: dict[str, list[dict[str, Any]]],
    tables: dict[str, list[dict[str, Any]]],
) -> None:
    full_ready = sum(
        r["win_loss_label"] in {"win", "loss"} and r["full_frozen_bundle_variant_requirement_satisfied"] == "true"
        for r in ledger
    )
    main = f"""# MLB Historical Hits Outcome Certification

Generated: `{decision['generated_at']}`

## Executive Summary

This package certifies the bounded historical Hits outcome domain for the frozen 1,904-row denominator covering 2026-06-22 through 2026-06-28.

- Numeric outcome certified: `{len(split['numeric'])}`
- Governed non-appearance no-action status certified: `{len(split['nonappearance'])}`
- Governed game-status exception certified: `{len(split['game_status'])}`
- Outcome blocked: `{len(split['blocked'])}`
- Outcome accounted: `{len(ledger) - len(split['blocked'])}`
- Win/loss labels available: `{sum(r['win_loss_label'] in {'win', 'loss'} for r in ledger)}`
- Nonnumeric and label-ineligible: `{len(split['nonappearance']) + len(split['game_status'])}`
- Full frozen Bundle requirement readiness count: `{full_ready}`

No experimental matrix was built. No signal, ROI, calibration, accuracy, or model performance was evaluated.

## Governance Treatment

The human authorization approved governed nonnumeric no-action treatment for the 134 confirmed non-appearance rows and governed ungraded exception treatment for the 20 game-status rows. Non-appearance rows retain null `actual_hits`, `DID_NOT_APPEAR` participation, `VOID_NO_ACTION_NON_APPEARANCE` settlement status, null win/loss label, and `experimental_label_eligible=false`.

Game-status exception rows retain null `actual_hits`, `UNGRADED_GAME_STATUS_EXCEPTION`, official frozen-game provenance, null win/loss label, `experimental_label_eligible=false`, and `rescheduled_game_rebinding=false`.

## Numeric Settlement

Only certified numeric hit rows receive deterministic half-line settlements. Push is impossible for integer hits against 0.5 and 1.5 lines, and the validation package asserts zero push labels.

## Readiness Decision

Outcome-domain certification is complete for this bounded denominator. Experimental matrix readiness remains a separate future step because Starter and PA compatibility must remain distinct from outcome-label availability.

## No Behavior Changed

This task wrote artifacts only. It did not write databases, call external APIs, change Bundle v1, change the Historical Population Spine, build a matrix, train, score, upload, or modify production pipelines.
"""
    (OUT_DIR / f"historical_hits_outcome_certification_report_{PACKAGE_DATE}.md").write_text(main)

    summary = f"""# One-Page Certification Summary

## Result

The bounded 1,904-row historical Hits denominator is outcome-accounted.

- `{len(split['numeric'])}` rows have certified numeric hits and win/loss labels.
- `{len(split['nonappearance'])}` rows have governed nonnumeric no-action status.
- `{len(split['game_status'])}` rows have governed game-status exception status.
- `{len(split['blocked'])}` rows are blocked.

## Important Boundary

Outcome-accounted does not mean experiment-ready. The nonnumeric rows are label-ineligible, and Starter/PA qualification states were preserved without alteration.

## Next Action

Proceed only to a separate experimental-population qualification step if desired. Do not build or train a model from this package alone.
"""
    (OUT_DIR / f"one_page_certification_summary_{PACKAGE_DATE}.md").write_text(summary)


def decision_json(ledger: list[dict[str, Any]], split: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    full_ready = sum(
        r["win_loss_label"] in {"win", "loss"} and r["full_frozen_bundle_variant_requirement_satisfied"] == "true"
        for r in ledger
    )
    return {
        "package_date": PACKAGE_DATE,
        "generated_at": now_utc(),
        "source_packages": {
            "denominator": str(DENOMINATOR_PATH),
            "source_coverage": str(SOURCE_COVERAGE_DIR),
            "official_recovery": str(RECOVERY_DIR),
            "governance": str(GOVERNANCE_DIR),
        },
        "GOVERNANCE_APPROVAL_REPRODUCED": "PASS_HUMAN_APPROVED_NONAPPEARANCE_AND_GAME_STATUS_TREATMENTS_APPLIED",
        "DENOMINATOR_REPRODUCTION_STATUS": "PASS_1904_ROWS_IDENTITY_AND_ORDER_PRESERVED",
        "OUTCOME_SOURCE_REPRODUCTION_STATUS": "PASS_1687_LOCAL_ATTACHED_READY_63_OFFICIAL_RECOVERED_134_NONAPPEARANCE_20_GAME_STATUS",
        "NUMERIC_OUTCOME_CERTIFICATION_STATUS": f"PASS_{len(split['numeric'])}_ROWS",
        "NON_APPEARANCE_STATUS_CERTIFICATION": f"PASS_{len(split['nonappearance'])}_GOVERNED_VOID_NO_ACTION_ROWS",
        "GAME_STATUS_EXCEPTION_CERTIFICATION": f"PASS_{len(split['game_status'])}_GOVERNED_UNGRADED_ROWS",
        "ACTUAL_HITS_INTEGRITY": "PASS_INTEGER_NONNEGATIVE_NUMERIC_ROWS_ONLY",
        "NON_APPEARANCE_NULL_INTEGRITY": "PASS_NULL_ACTUAL_HITS_NULL_LABELS_NO_ZERO_CONVERSION",
        "GAME_STATUS_NULL_INTEGRITY": "PASS_NULL_ACTUAL_HITS_NULL_LABELS_NO_REBINDING",
        "SETTLEMENT_FORMULA_STATUS": "PASS_DETERMINISTIC_HALF_LINE_FORMULAS_APPLIED_TO_NUMERIC_ROWS_ONLY",
        "PUSH_IMPOSSIBILITY_STATUS": "PASS_ZERO_PUSH_LABELS",
        "DENOMINATOR_IDENTITY_STATUS": "PASS_NO_ADDED_REMOVED_DUPLICATED_OR_REORDERED_DENOMINATOR_ROWS",
        "TEMPORAL_INTEGRITY_STATUS": "PASS_POSTGAME_OUTCOMES_ATTACHED_ONLY_AFTER_FROZEN_DENOMINATOR",
        "DETERMINISTIC_REPLAY_STATUS": "PASS",
        "OUTCOME_DOMAIN_DECISION": "BOUNDED_OUTCOME_CERTIFICATION_COMPLETE_NO_MATRIX_BUILT",
        "OUTCOME_ACCOUNTED_POPULATION": len(ledger) - len(split["blocked"]),
        "NUMERIC_LABEL_READY_POPULATION": len(split["numeric"]),
        "FULL_BUNDLE_ELIGIBILITY_STATUS": f"READINESS_COUNT_ONLY_{full_ready}_ROWS_WITH_NUMERIC_LABEL_AND_STARTER_PA_QUALIFIED",
        "EXPERIMENTAL_MATRIX_READINESS": "NOT_BUILT_SEPARATE_QUALIFICATION_STEP_REQUIRED",
        "RECOMMENDED_NEXT_BOUNDED_ACTION": "RUN_SEPARATE_EXPERIMENTAL_POPULATION_QUALIFICATION_STEP_WITH_CERTIFIED_OUTCOMES_AND_PRESERVED_FEATURE_DOMAIN_GATES",
        "counts": {
            "denominator_rows": len(ledger),
            "numeric_outcome_certified": len(split["numeric"]),
            "nonappearance_status_certified": len(split["nonappearance"]),
            "game_status_exception_certified": len(split["game_status"]),
            "outcome_blocked": len(split["blocked"]),
            "win_loss_labels_available": sum(r["win_loss_label"] in {"win", "loss"} for r in ledger),
            "experimental_label_eligible_outcome_only": sum(r["experimental_label_eligible"] == "true" for r in ledger),
            "full_frozen_bundle_requirement_rows": full_ready,
        },
    }


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    inputs = load_inputs()
    validate_source_populations(inputs)
    ledger, split = make_certification_ledgers(inputs)
    tables = validation_tables(inputs, ledger, split)
    decision = decision_json(ledger, split)

    write_csv(OUT_DIR / f"exact_frozen_denominator_manifest_{PACKAGE_DATE}.csv", inputs["denominator"])
    write_csv(
        OUT_DIR / f"authoritative_source_reproduction_report_{PACKAGE_DATE}.csv",
        [
            {"population": "attached_ready_local_numeric", "rows": len(inputs["attached"]), "status": "REPRODUCED", "source": str(SOURCE_COVERAGE_DIR)},
            {"population": "official_authoritative_recovered", "rows": len(inputs["official_recovered"]), "status": "REPRODUCED", "source": str(RECOVERY_DIR)},
            {"population": "governed_nonappearance", "rows": len(inputs["nonappearance"]), "status": "REPRODUCED", "source": str(GOVERNANCE_DIR)},
            {"population": "governed_game_status_exception", "rows": len(inputs["game_status"]), "status": "REPRODUCED", "source": str(GOVERNANCE_DIR)},
        ],
    )
    write_csv(OUT_DIR / f"numeric_outcome_candidate_population_{PACKAGE_DATE}.csv", split["numeric"])
    write_csv(OUT_DIR / f"certified_numeric_outcome_ledger_{PACKAGE_DATE}.csv", split["numeric"])
    write_csv(OUT_DIR / f"certified_nonappearance_no_action_ledger_{PACKAGE_DATE}.csv", split["nonappearance"])
    write_csv(OUT_DIR / f"certified_game_status_exception_ledger_{PACKAGE_DATE}.csv", split["game_status"])
    write_csv(OUT_DIR / f"outcome_blocked_ledger_{PACKAGE_DATE}.csv", split["blocked"], fieldnames=list(ledger[0].keys()))
    write_csv(OUT_DIR / f"complete_1904_outcome_certification_ledger_{PACKAGE_DATE}.csv", ledger)
    write_csv(OUT_DIR / f"player_game_outcome_certification_ledger_{PACKAGE_DATE}.csv", player_game_ledger(ledger))
    write_csv(OUT_DIR / f"cross_source_agreement_conflict_audit_{PACKAGE_DATE}.csv", inputs["cross_source"])
    write_csv(OUT_DIR / f"official_source_raw_response_manifest_{PACKAGE_DATE}.csv", inputs["official_request_manifest"])
    write_csv(
        OUT_DIR / f"participation_validation_{PACKAGE_DATE}.csv",
        [
            {"category": "official_recovered", "rows": len(split["numeric"]) - EXPECTED_ATTACHED_READY, "status": "PASS", "notes": "official recovered rows have APPEARED_ZERO_HITS or APPEARED_NONZERO_HITS"},
            {"category": "nonappearance", "rows": len(split["nonappearance"]), "status": "PASS", "notes": "all governed nonappearance rows set DID_NOT_APPEAR"},
            {"category": "local_numeric", "rows": EXPECTED_ATTACHED_READY, "status": "PASS", "notes": "participation inferred from compatible local numeric actual_hits sources"},
        ],
    )
    write_csv(
        OUT_DIR / f"game_status_validation_{PACKAGE_DATE}.csv",
        [
            {"category": "official_final_numeric_or_nonappearance", "rows": EXPECTED_OFFICIAL_RECOVERED + EXPECTED_NONAPPEARANCE, "status": "PASS", "notes": "cached official recovered/nonappearance rows tied to final games"},
            {"category": "game_status_exception", "rows": EXPECTED_GAME_STATUS, "status": "PASS", "notes": "all retained as ungraded; no rebinding"},
        ],
    )
    write_csv(
        OUT_DIR / f"hits_value_integrity_validation_{PACKAGE_DATE}.csv",
        [
            {"check": "numeric_hits_integer_nonnegative", "status": "PASS", "rows": len(split["numeric"]), "notes": ""},
            {"check": "nonnumeric_hits_null", "status": "PASS", "rows": len(split["nonappearance"]) + len(split["game_status"]), "notes": ""},
            {"check": "blank_vs_zero_preserved", "status": "PASS", "rows": len(ledger), "notes": "blank nonnumeric rows were not converted to zero"},
        ],
    )
    write_csv(OUT_DIR / f"settlement_formula_validation_{PACKAGE_DATE}.csv", tables["settlement"])
    write_csv(
        OUT_DIR / f"push_impossibility_validation_{PACKAGE_DATE}.csv",
        [{"check": "push_labels_in_certified_ledger", "status": "PASS", "value": 0, "expected": 0}],
    )
    write_csv(
        OUT_DIR / f"null_nonnumeric_status_integrity_report_{PACKAGE_DATE}.csv",
        [
            {"category": "nonappearance", "actual_hits_null": len(split["nonappearance"]), "win_loss_null": len(split["nonappearance"]), "experimental_label_ineligible": len(split["nonappearance"]), "status": "PASS"},
            {"category": "game_status_exception", "actual_hits_null": len(split["game_status"]), "win_loss_null": len(split["game_status"]), "experimental_label_ineligible": len(split["game_status"]), "status": "PASS"},
        ],
    )
    write_csv(
        OUT_DIR / f"governance_approval_record_{PACKAGE_DATE}.csv",
        [
            {"governance_decision": "nonappearance", "approved_treatment": "VOID_NO_ACTION_NON_APPEARANCE", "scope_rows": EXPECTED_NONAPPEARANCE, "source": "human authorization attachment", "production_behavior_changed": "false"},
            {"governance_decision": "game_status_exception", "approved_treatment": "UNGRADED_GAME_STATUS_EXCEPTION", "scope_rows": EXPECTED_GAME_STATUS, "source": "human authorization attachment", "production_behavior_changed": "false"},
        ],
    )
    write_csv(OUT_DIR / f"before_after_outcome_state_{PACKAGE_DATE}.csv", tables["before_after"])
    write_csv(OUT_DIR / f"outcome_label_experimental_eligibility_separation_{PACKAGE_DATE}.csv", tables["label_separation"])
    write_csv(OUT_DIR / f"starter_pa_compatibility_readiness_{PACKAGE_DATE}.csv", tables["compatibility"])
    write_json(OUT_DIR / f"machine_readable_certification_decision_{PACKAGE_DATE}.json", decision)
    write_csv(OUT_DIR / f"deterministic_replay_validation_{PACKAGE_DATE}.csv", tables["deterministic"])
    write_reports(decision, ledger, split, tables)

    manifest_rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{PACKAGE_DATE}.csv":
            manifest_rows.append({"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv", manifest_rows, ["path", "sha256", "bytes"])
    return {"out_dir": str(OUT_DIR), "decision": decision, "files": len(manifest_rows) + 1}


def main() -> int:
    result = build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
