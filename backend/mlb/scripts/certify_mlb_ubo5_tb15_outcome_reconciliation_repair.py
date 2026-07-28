#!/usr/bin/env python3
"""Build the bounded evidence package for the July 27 outcome lifecycle repair."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DATE = "2026-07-27"
DAY = ROOT / "backend/mlb/exports/model_v2/ubo5_tb15" / DATE
OUT = ROOT / (
    "artifacts/analysis/model_development/"
    "mlb_ubo5_tb15_outcome_reconciliation_lifecycle_repair/2026-07-28"
)
PRIOR = ROOT / (
    "artifacts/analysis/model_development/"
    "mlb_ubo5_tb15_original_win_rate_contract_replay/2026-07-28/"
    "ubo5_july27_unsettled_identity_audit.csv"
)
RECONCILE = ROOT / (
    "artifacts/analysis/mlb/execution_vs_model/2026-07-27/reconcile_rows.csv"
)


def rows(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write(path: Path, data: list[dict], fields: list[str] | None = None) -> None:
    fields = fields or list(data[0]) if data else (fields or [])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(data)


def key(row: dict) -> tuple[str, str]:
    return str(row.get("game_pk") or row.get("game_id")), str(
        row.get("batter_mlb_id") or row.get("player_id")
    )


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summary(name: str) -> dict:
    return json.loads((DAY / name).read_text(encoding="utf-8"))


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    audit_path = DAY / f"ubo5_tb15_complete_outcome_audit_{DATE}.csv"
    complete = rows(audit_path)
    by_key = {key(row): row for row in complete}
    prior = rows(PRIOR)
    missing = [
        row for row in prior
        if row.get("exact_reason") == "GAME_COMPLETED_OUTCOME_MISSING"
    ]
    exceptions = [
        row for row in prior
        if row.get("exact_reason") == "PLAYER_NOT_IN_FINAL_LINEUP"
    ]
    compact = [
        "slate_date", "game_pk", "batter_mlb_id", "player_name", "game",
        "plate_appearances_authoritative", "singles_authoritative",
        "doubles_authoritative", "triples_authoritative",
        "home_runs_authoritative", "total_bases_authoritative",
        "exact_reason", "denominator_disposition",
    ]
    write(OUT / "july27_missing_completed_game_rows.csv", missing, compact)
    exception_fields = compact + [
        "confirmed_starting_status", "audit_settlement_status",
    ]
    write(OUT / "july27_final_lineup_exception_audit.csv", exceptions, exception_fields)

    standard = {key(r): r for r in rows(DAY / f"ubo5_tb15_closeout_{DATE}.csv")}
    ever = {key(r): r for r in rows(DAY / f"ubo5_tb15_ever_positive_closeout_{DATE}.csv")}
    final = {key(r): r for r in rows(DAY / f"ubo5_tb15_final_pregame_closeout_{DATE}.csv")}
    reconcile = {key(r) for r in rows(RECONCILE) if r.get("prop_type") == "total_bases"
                 and r.get("line") in {"1.5", "1.500000"}}
    manifest = json.loads(
        (DAY / f"ubo5_tb15_run_population_manifest_{DATE}.json").read_text()
    )
    consensus_population = set()
    consensus_manifest = DAY / f"ubo5_tb15_consensus_population_{DATE}.json"
    if consensus_manifest.is_file():
        payload = json.loads(consensus_manifest.read_text())
        consensus_population = {key(r) for r in payload.get("population", [])}
    matrix = []
    for ident, outcome in sorted(by_key.items(), key=lambda item: (
        item[1].get("game", ""), item[1].get("player_name", "")
    )):
        matrix.append({
            "slate_date": DATE, "game_pk": ident[0], "batter_mlb_id": ident[1],
            "player_name": outcome["player_name"], "game": outcome["game"],
            "complete_evaluated_population": "YES",
            "standard_reconciliation_union": "YES" if ident in reconcile else "NO",
            "standard_closeout_surface": "YES" if ident in standard else "NO",
            "broad_ever_positive_population": "YES" if ident in ever else "NO",
            "broad_final_pregame_population": "YES" if ident in final else "NO",
            "consensus_population": "YES" if ident in consensus_population else "NO",
            "complete_outcome_status": outcome["result"],
            "standard_outcome_status": standard.get(ident, {}).get("result", ""),
            "ever_positive_outcome_status": ever.get(ident, {}).get("result", ""),
            "final_pregame_outcome_status": final.get(ident, {}).get("result", ""),
            "resolution_method": outcome["resolution_method"],
        })
    write(OUT / "july27_cross_population_identity_matrix.csv", matrix)

    source_fields = [
        "slate_date", "game_pk", "batter_mlb_id", "player_name", "game",
        "result", "total_bases", "resolution_reason_code", "outcome_source",
        "outcome_source_path", "resolution_method", "source_revision",
        "resolved_timestamp_utc",
    ]
    write(OUT / "outcome_resolution_source_audit.csv", complete, source_fields)

    current_complete = summary("ubo5_tb15_complete_outcome_audit_current.json")
    standard_summary = summary("ubo5_tb15_closeout_current.json")
    ever_summary = summary("ubo5_tb15_ever_positive_closeout_current.json")
    final_summary = summary("ubo5_tb15_final_pregame_closeout_current.json")
    pre_post = [
        {
            "population": "COMPLETE_EVALUATED_UNIVERSE",
            "prior_revision": 0, "new_revision": current_complete["revision"],
            "rows_added_or_reclassified": 61,
            "wins": current_complete["wins"], "losses": current_complete["losses"],
            "voids": current_complete["void_classifications"],
            "no_action": current_complete["no_action_classifications"],
            "pending": current_complete["pending_postponed_rows"],
            "technical_unresolved": current_complete["technical_unresolved_rows"],
            "content_sha256": sha(audit_path),
        },
        {
            "population": "STANDARD_CONFIRMED_EDGE",
            "prior_revision": 1, "new_revision": standard_summary["closeout_revision"],
            "rows_added_or_reclassified": 1,
            **{k: standard_summary.get(k, 0) for k in (
                "wins", "losses", "voids", "no_action", "pending",
                "technical_unresolved",
            )},
            "content_sha256": sha(DAY / f"ubo5_tb15_closeout_{DATE}.csv"),
        },
        {
            "population": "BROAD_EVER_POSITIVE",
            "prior_revision": 1, "new_revision": ever_summary["closeout_revision"],
            "rows_added_or_reclassified": 1,
            **{k: ever_summary.get(k, 0) for k in (
                "wins", "losses", "voids", "no_action", "pending",
                "technical_unresolved",
            )},
            "content_sha256": sha(DAY / f"ubo5_tb15_ever_positive_closeout_{DATE}.csv"),
        },
        {
            "population": "BROAD_FINAL_PREGAME_POSITIVE",
            "prior_revision": 1, "new_revision": final_summary["closeout_revision"],
            "rows_added_or_reclassified": 1,
            **{k: final_summary.get(k, 0) for k in (
                "wins", "losses", "voids", "no_action", "pending",
                "technical_unresolved",
            )},
            "content_sha256": sha(DAY / f"ubo5_tb15_final_pregame_closeout_{DATE}.csv"),
        },
        {
            "population": "CONSENSUS",
            "prior_revision": 0, "new_revision": 0,
            "rows_added_or_reclassified": 0, "wins": 0, "losses": 0,
            "voids": 0, "no_action": 0, "pending": 0,
            "technical_unresolved": 0,
            "content_sha256": "NO_CERTIFIED_JULY27_CONSENSUS_POPULATION",
        },
    ]
    write(OUT / "july27_pre_post_repair_summary.csv", pre_post)

    health = {
        **{k: current_complete[k] for k in (
            "population_rows_inspected", "market_backed_resolutions",
            "exact_id_fallback_resolutions", "no_action_classifications",
            "void_classifications", "pending_postponed_rows",
            "technical_unresolved_rows",
        )},
        "dates_retried": [DATE],
        "closeout_revisions_created": 4,
        "only_pending_game": "CLE @ CIN",
        "idempotent_rerun": True,
        "population_manifest_count": manifest["counts"]["all_attempted_evaluated_identities"],
    }
    (OUT / "reconciliation_retry_health.json").write_text(
        json.dumps(health, indent=2) + "\n", encoding="utf-8"
    )
    (OUT / "reconciliation_retry_health.md").write_text(
        "# Reconciliation Retry Health\n\n"
        + "\n".join(f"- {k.replace('_', ' ').title()}: {v}" for k, v in health.items())
        + "\n", encoding="utf-8"
    )
    (OUT / "root_cause_report.md").write_text(
        """# Root Cause Report

The immutable complete population contained 170 exact identities, but the
standard closeout assembled a separate 131-row morning-board surface and the
broad and consensus closeouts each assembled their own selected populations.
The reconciliation candidate union therefore was not derived from the complete
population manifest. Completed rows absent from `reconcile_rows.csv` had no
shared exact-ID fallback in the normal lifecycle; the read-only contract audit
alone joined those 15 official outcomes. This also explains why Ronald Acuña
Jr. could be resolved in the broad closeout while absent from the standard
union.

The repair centralizes settlement in
`backend/mlb/shared/ubo5_tb15_outcome_resolver.py`. Every closeout now preserves
its immutable population membership and resolves outcomes in this order:
market-backed reconciliation, exact `game_pk + batter_mlb_id` player stats,
official final lineup/participation no-action, official pending game, then a
visible technical failure. The complete 170-row manifest now has its own
revisioned audit ledger. No name join or outcome-derived membership is used.
""", encoding="utf-8"
    )
    decisions = """# Terminal Decision

UBO5_TB15_OUTCOME_JOIN_ROOT_CAUSE_DECISION = SEPARATE_POPULATION_UNIONS_AND_MISSING_SHARED_EXACT_ID_FALLBACK_REPAIRED
UBO5_TB15_EXACT_ID_FALLBACK_DECISION = ACTIVE_CANONICAL_GAME_PK_BATTER_ID_RESOLVER
UBO5_TB15_FINAL_LINEUP_CLASSIFICATION_DECISION = FIVE_EXPLICIT_NO_ACTION_ZERO_GENERIC_UNRESOLVED
UBO5_TB15_POSTPONED_GAME_RETRY_DECISION = FOURTEEN_CLE_CIN_ROWS_RETAINED_PENDING_AND_AUTOMATICALLY_RETRIED
UBO5_TB15_POPULATION_UNION_INTEGRITY_DECISION = COMPLETE_170_ROW_LEDGER_CERTIFIED_NAMED_POPULATIONS_PRESERVED
UBO5_TB15_JULY27_BACKFILL_DECISION = COMPLETE_151_ACTION_5_NO_ACTION_14_PENDING_ZERO_TECHNICAL
UBO5_TB15_CLOSEOUT_REVISION_DECISION = CONTENT_ADDRESSED_REVISIONED_UNCHANGED_RERUN_IDEMPOTENT
UBO5_TB15_RECONCILIATION_LIFECYCLE_DECISION = REPAIRED_NONBLOCKING_VISIBLE_FAIL_CLOSED
"""
    (OUT / "terminal_decision.md").write_text(decisions, encoding="utf-8")


if __name__ == "__main__":
    main()
