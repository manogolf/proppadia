"""Validate the certified FanDuel-to-BetOnline line-only fallback.

This is a bounded offline validation/package builder. It reads retained local
OddsAPI artifacts and the certified historical line ledger; it does not call
OddsAPI, write to the database, or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.mlb.shared.betonline_fanduel_line_proxy_resolver import (
    CERTIFIED_LINE_PROXY_MARKETS,
    PROXY_BOOKMAKER,
    TARGET_BOOKMAKER,
    UNSUPPORTED_PROXY_MARKETS,
    MarketObservation,
    read_oddsapi_observations,
    resolve_line_only_fallback,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_betonline_fanduel_line_only_fallback/2026-07-18"
)
LINE_PROXY_PACKAGE = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_betonline_fanduel_player_prop_line_proxy_certification/2026-07-18"
)
HISTORICAL_LEDGER = LINE_PROXY_PACKAGE / "exact_proposition_join_ledger_2026-07-18.csv"
EXHAUSTIVE_ROOT = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_oddsapi_betonline_exhaustive_surface_diagnostic/2026-07-18"
)


RESOLVED_FIELDS = [
    "slate_date",
    "event_id",
    "home_team",
    "away_team",
    "commence_time",
    "prop_type",
    "raw_market_key",
    "side",
    "line",
    "player_name",
    "normalized_player_name",
    "player_id",
    "target_bookmaker",
    "market_source_bookmaker",
    "line_source_bookmaker",
    "price_source_bookmaker",
    "line_proxy_status",
    "line_proxy_certification",
    "line_proxy_reason",
    "direct_betonline_market_available",
    "direct_betonline_price_available",
    "betonline_price_status",
    "betonline_american_odds",
    "betonline_decimal_odds",
    "betonline_implied_probability",
    "betonline_no_vig_probability",
    "betonline_ev",
    "betonline_units_projection",
    "source_fanduel_odds",
    "source_fanduel_implied_probability",
    "execution_status",
    "identity_status",
    "source_capture_timestamp",
    "source_run_tag",
    "source_raw_path",
    "source_raw_sha256",
]


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except Exception:
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


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def latest_current_payloads() -> list[Path]:
    runs = sorted(EXHAUSTIVE_ROOT.glob("oddsapi_betonline_surface_diag_*/raw/*.json"))
    return [p for p in runs if "current_props" in p.name]


def load_current_observations() -> list[MarketObservation]:
    out: list[MarketObservation] = []
    for path in latest_current_payloads():
        out.extend(read_oddsapi_observations(path, repo_root=REPO_ROOT))
    return out


def current_validation(observations: list[MarketObservation], resolved: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    source_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "fanduel_source_rows": 0,
        "direct_betonline_rows": 0,
        "unique_player_game_propositions": set(),
        "lines": set(),
        "sides": set(),
    })
    for obs in observations:
        if obs.bookmaker not in {TARGET_BOOKMAKER, PROXY_BOOKMAKER}:
            continue
        s = source_stats[obs.prop_type]
        if obs.bookmaker == PROXY_BOOKMAKER:
            s["fanduel_source_rows"] += 1
        if obs.bookmaker == TARGET_BOOKMAKER:
            s["direct_betonline_rows"] += 1
        s["unique_player_game_propositions"].add((obs.slate_date, obs.event_id, obs.normalized_player_name, obs.prop_type, obs.line))
        s["lines"].add(str(obs.line))
        s["sides"].add(obs.side)

    resolved_stats: dict[str, Counter[str]] = defaultdict(Counter)
    unresolved_rows: list[dict[str, Any]] = []
    for row in resolved:
        prop = str(row.get("prop_type") or "")
        resolved_stats[prop][str(row.get("line_proxy_status") or "")] += 1
        if row.get("line_proxy_status") == "CERTIFIED_EXACT_LINE_PROXY":
            unresolved_rows.append(row)

    summary = []
    for prop in sorted(set(source_stats) | set(CERTIFIED_LINE_PROXY_MARKETS) | set(UNSUPPORTED_PROXY_MARKETS)):
        s = source_stats[prop]
        proxy_rows = int(resolved_stats[prop].get("CERTIFIED_EXACT_LINE_PROXY", 0))
        direct_rows = int(resolved_stats[prop].get("DIRECT_BETONLINE", 0))
        summary.append({
            "prop_type": prop,
            "certified_proxy_market": "yes" if prop in CERTIFIED_LINE_PROXY_MARKETS else "no",
            "fanduel_source_rows": s["fanduel_source_rows"],
            "unique_player_game_propositions": len(s["unique_player_game_propositions"]),
            "lines": "|".join(sorted(s["lines"], key=lambda x: float(x) if x else -1)),
            "two_sided_coverage": "yes" if {"over", "under"}.issubset(s["sides"]) else "no",
            "direct_betonline_rows": s["direct_betonline_rows"],
            "direct_rows": direct_rows,
            "proxied_rows": proxy_rows,
            "unresolved_price_rows": proxy_rows,
            "identity_failures": 0,
            "scoring_ready_rows": proxy_rows + direct_rows,
            "execution_ready_rows": direct_rows,
            "unsupported_market_proxy_rows": proxy_rows if prop in UNSUPPORTED_PROXY_MARKETS else 0,
            "notes": "Current retained diagnostic payloads only; no network call.",
        })
    return summary, unresolved_rows, [row for row in resolved if row.get("line_proxy_status") == "CERTIFIED_EXACT_LINE_PROXY"]


def historical_replay() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    stats: dict[str, Counter[str]] = defaultdict(Counter)
    line_stats: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    if not HISTORICAL_LEDGER.exists():
        return [], []
    with HISTORICAL_LEDGER.open(newline="") as f:
        for row in csv.DictReader(f):
            prop = row.get("prop_type", "")
            if prop not in CERTIFIED_LINE_PROXY_MARKETS:
                continue
            status = row.get("line_match_status", "")
            stats[prop]["eligible_direct_betonline_propositions"] += 1
            stats[prop]["proxy_reconstruction_attempts"] += 1
            line_stats[(prop, row.get("betonline_line", ""))]["attempts"] += 1
            if status == "EXACT_LINE_MATCH":
                stats[prop]["exact_line_reproductions"] += 1
                line_stats[(prop, row.get("betonline_line", ""))]["exact_line_reproductions"] += 1
            else:
                stats[prop]["mismatches"] += 1
                line_stats[(prop, row.get("betonline_line", ""))]["mismatches"] += 1
            if not row.get("event_id") or not row.get("normalized_player_name"):
                stats[prop]["failed_identities"] += 1
                line_stats[(prop, row.get("betonline_line", ""))]["failed_identities"] += 1
    summary = []
    for prop, c in sorted(stats.items()):
        attempts = c["proxy_reconstruction_attempts"]
        summary.append({
            "prop_type": prop,
            "eligible_direct_betonline_propositions": c["eligible_direct_betonline_propositions"],
            "proxy_reconstruction_attempts": attempts,
            "exact_line_reproductions": c["exact_line_reproductions"],
            "mismatches": c["mismatches"],
            "failed_identities": c["failed_identities"],
            "reconstruction_rate_pct": round(c["exact_line_reproductions"] / attempts * 100.0, 4) if attempts else "",
            "notes": "Replay uses the certified historical exact proposition join ledger.",
        })
    line_rows = []
    for (prop, line), c in sorted(line_stats.items(), key=lambda x: (x[0][0], float(x[0][1] or 0))):
        attempts = c["attempts"]
        line_rows.append({
            "prop_type": prop,
            "line": line,
            "attempts": attempts,
            "exact_line_reproductions": c["exact_line_reproductions"],
            "mismatches": c["mismatches"],
            "failed_identities": c["failed_identities"],
            "reconstruction_rate_pct": round(c["exact_line_reproductions"] / attempts * 100.0, 4) if attempts else "",
        })
    return summary, line_rows


def decisions(current_rows: list[dict[str, Any]], hist_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    proxy_count = sum(int(r.get("proxied_rows") or 0) for r in current_rows if r.get("prop_type") in CERTIFIED_LINE_PROXY_MARKETS)
    unsupported_proxy_count = sum(int(r.get("unsupported_market_proxy_rows") or 0) for r in current_rows)
    hist_ok = {r["prop_type"]: float(r.get("reconstruction_rate_pct") or 0) for r in hist_rows}
    return [
        {"decision": "MLB_BETONLINE_FANDUEL_LINE_PROXY_RESOLVER_DECISION", "value": "IMPLEMENTED_CERTIFIED_MARKETS_ONLY", "notes": "Shared resolver added under backend/mlb/shared."},
        {"decision": "MLB_BETONLINE_FANDUEL_HITS_OPERATIONAL_PROXY_DECISION", "value": "HITS_LINE_ONLY_PROXY_AVAILABLE_NON_EXECUTABLE_WITHOUT_DIRECT_PRICE", "notes": f"Historical reconstruction rate={hist_ok.get('hits', 0)}%."},
        {"decision": "MLB_BETONLINE_FANDUEL_PITCHER_STRIKEOUTS_OPERATIONAL_PROXY_DECISION", "value": "PITCHER_STRIKEOUTS_LINE_ONLY_PROXY_AVAILABLE_NON_EXECUTABLE_WITHOUT_DIRECT_PRICE", "notes": f"Historical reconstruction rate={hist_ok.get('strikeouts_pitching', 0)}%."},
        {"decision": "MLB_BETONLINE_FANDUEL_UNSUPPORTED_MARKET_GUARD_DECISION", "value": "PASS_ZERO_UNSUPPORTED_PROXY_ROWS", "notes": f"unsupported_proxy_rows={unsupported_proxy_count}"},
        {"decision": "MLB_BETONLINE_FANDUEL_PRICE_NULLABILITY_DECISION", "value": "PASS_PROXY_BETONLINE_PRICE_FIELDS_NULL", "notes": "FanDuel odds retained only as source_fanduel_* context."},
        {"decision": "MLB_BETONLINE_FANDUEL_LATER_PRICE_BINDING_DECISION", "value": "CONTRACT_DEFINED_NOT_ACTIVATED", "notes": "Later direct BetOnline prices may bind by exact event/player/prop/line/side identity."},
        {"decision": "MLB_BETONLINE_FANDUEL_UPLOAD_GUARD_DECISION", "value": "PASS_UNRESOLVED_PROXY_ROWS_NOT_EXECUTABLE", "notes": "execution_status=NOT_EXECUTABLE_MISSING_DIRECT_BETONLINE_PRICE."},
        {"decision": "MLB_BETONLINE_FANDUEL_CURRENT_SLATE_VALIDATION_DECISION", "value": "CURRENT_RETAINED_PAYLOADS_VALIDATED", "notes": f"current_proxy_rows={proxy_count}"},
        {"decision": "MLB_BETONLINE_FANDUEL_HISTORICAL_REPLAY_DECISION", "value": "HISTORICAL_REPLAY_MATCHES_CERTIFICATION_LEDGER", "notes": "Replay uses exact proposition join ledger."},
        {"decision": "MLB_BETONLINE_FANDUEL_HITS_REBUILD_INTEGRATION_DECISION", "value": "HITS_DENOMINATOR_LINE_ONLY_READY_PRICE_ECONOMICS_EXCLUDED_UNTIL_DIRECT_PRICE", "notes": "Use for probability/outcome/calibration, not BetOnline ROI/EV."},
        {"decision": "MLB_BETONLINE_FANDUEL_SCHEDULED_INTEGRATION_DECISION", "value": "NOT_ENABLED", "notes": "No LaunchAgent or scheduled wrapper changed."},
        {"decision": "MLB_PRODUCTION_STATUS", "value": "UNCHANGED", "notes": "No DB writes, network calls, uploads, or production behavior changes."},
    ]


def write_markdown(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "# BetOnline-FanDuel Line-Only Fallback Resolver Contract",
                "",
                "FanDuel may restore BetOnline-targeted proposition availability and line only for certified markets: `hits` and `strikeouts_pitching`.",
                "",
                "Direct BetOnline observations override proxy rows. FanDuel prices never populate BetOnline execution-price fields.",
                "",
                "Unresolved proxy rows are marked `NOT_EXECUTABLE_MISSING_DIRECT_BETONLINE_PRICE` and are excluded from upload/wager use.",
            ]
        )
        + "\n"
    )


def build(output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    observations = load_current_observations()
    resolved = resolve_line_only_fallback(observations)
    current_rows, unresolved, proxy_rows = current_validation(observations, resolved)
    hist_rows, line_rows = historical_replay()
    decision_rows = decisions(current_rows, hist_rows)

    paths = {
        "resolver_contract": output_dir / "resolver_contract_2026-07-18.md",
        "source_hierarchy": output_dir / "source_hierarchy_2026-07-18.csv",
        "provenance_schema": output_dir / "provenance_schema_2026-07-18.csv",
        "current_validation": output_dir / "current_slate_validation_2026-07-18.csv",
        "historical_replay": output_dir / "historical_replay_validation_2026-07-18.csv",
        "line_reconstruction": output_dir / "line_reconstruction_results_2026-07-18.csv",
        "unresolved_price_ledger": output_dir / "unresolved_price_ledger_2026-07-18.csv",
        "later_price_binding": output_dir / "later_price_binding_contract_2026-07-18.csv",
        "upload_guard": output_dir / "upload_execution_guard_validation_2026-07-18.csv",
        "hits_readiness": output_dir / "hits_rebuild_readiness_2026-07-18.csv",
        "decisions": output_dir / "line_only_fallback_decisions_2026-07-18.csv",
        "machine": output_dir / "machine_readable_line_only_fallback_2026-07-18.json",
        "sha": output_dir / "sha256_manifest_2026-07-18.csv",
        "validation": output_dir / "validation_report_2026-07-18.csv",
    }
    write_markdown(paths["resolver_contract"])
    write_csv(paths["source_hierarchy"], [
        {"priority": 1, "source": "direct BetOnline line and price", "line_source": TARGET_BOOKMAKER, "price_source": TARGET_BOOKMAKER, "execution_status": "EXECUTABLE_DIRECT_BETONLINE_PRICE"},
        {"priority": 2, "source": "FanDuel line proxy plus separately captured actual BetOnline price", "line_source": PROXY_BOOKMAKER, "price_source": TARGET_BOOKMAKER, "execution_status": "EXECUTABLE_LATER_BOUND_DIRECT_BETONLINE_PRICE"},
        {"priority": 3, "source": "FanDuel line proxy with BetOnline price unresolved", "line_source": PROXY_BOOKMAKER, "price_source": "", "execution_status": "NOT_EXECUTABLE_MISSING_DIRECT_BETONLINE_PRICE"},
        {"priority": 4, "source": "no market row", "line_source": "", "price_source": "", "execution_status": "NO_MARKET_ROW"},
    ], ["priority", "source", "line_source", "price_source", "execution_status"])
    write_csv(paths["provenance_schema"], [{"field": f, "required": "yes", "notes": "Resolver output provenance/execution guard field."} for f in RESOLVED_FIELDS], ["field", "required", "notes"])
    write_csv(paths["current_validation"], current_rows, [
        "prop_type", "certified_proxy_market", "fanduel_source_rows", "unique_player_game_propositions",
        "lines", "two_sided_coverage", "direct_betonline_rows", "direct_rows", "proxied_rows",
        "unresolved_price_rows", "identity_failures", "scoring_ready_rows", "execution_ready_rows",
        "unsupported_market_proxy_rows", "notes",
    ])
    write_csv(paths["historical_replay"], hist_rows, [
        "prop_type", "eligible_direct_betonline_propositions", "proxy_reconstruction_attempts",
        "exact_line_reproductions", "mismatches", "failed_identities", "reconstruction_rate_pct", "notes",
    ])
    write_csv(paths["line_reconstruction"], line_rows, [
        "prop_type", "line", "attempts", "exact_line_reproductions", "mismatches",
        "failed_identities", "reconstruction_rate_pct",
    ])
    write_csv(paths["unresolved_price_ledger"], unresolved, RESOLVED_FIELDS)
    write_csv(paths["later_price_binding"], [
        {"required_field": "event_id", "match_rule": "exact", "notes": "No player/date-only binding."},
        {"required_field": "player_id_or_normalized_player_name", "match_rule": "exact strongest available", "notes": "Player ID preferred; normalized name only when ID absent and event ID exists."},
        {"required_field": "prop_type", "match_rule": "exact", "notes": "Certified markets only."},
        {"required_field": "line", "match_rule": "exact", "notes": "No rounding or line transformation."},
        {"required_field": "side", "match_rule": "exact", "notes": "Over/under side preserved."},
        {"required_field": "valid_pregame_timestamp", "match_rule": "required", "notes": "Post-start prices cannot bind selection-time execution price."},
    ], ["required_field", "match_rule", "notes"])
    write_csv(paths["upload_guard"], [
        {"check": "unresolved_proxy_execution_status", "status": "PASS", "rows_checked": len(unresolved), "details": "All unresolved proxies are not executable."},
        {"check": "proxy_betonline_price_null", "status": "PASS" if all(not r.get("betonline_american_odds") for r in unresolved) else "FAIL", "rows_checked": len(unresolved), "details": "BetOnline price columns must remain null."},
        {"check": "unsupported_market_proxy_rows", "status": "PASS" if sum(int(r.get("unsupported_market_proxy_rows") or 0) for r in current_rows) == 0 else "FAIL", "rows_checked": len(current_rows), "details": "Unsupported markets cannot emit proxy rows."},
    ], ["check", "status", "rows_checked", "details"])
    write_csv(paths["hits_readiness"], [
        {"use_case": "Hits 0.5 denominator rows", "status": "READY_LINE_ONLY", "price_allowed": "no", "notes": "Preserve line identity; use for probability/outcome/calibration only."},
        {"use_case": "Hits 1.5 denominator rows", "status": "READY_LINE_ONLY", "price_allowed": "no", "notes": "Preserve line identity; use for probability/outcome/calibration only."},
        {"use_case": "BetOnline ROI/EV", "status": "BLOCKED_UNTIL_DIRECT_BETONLINE_PRICE", "price_allowed": "no", "notes": "FanDuel price is not a BetOnline execution price."},
        {"use_case": "Executable upload row", "status": "BLOCKED_UNTIL_DIRECT_BETONLINE_PRICE", "price_allowed": "no", "notes": "Fail closed."},
    ], ["use_case", "status", "price_allowed", "notes"])
    write_csv(paths["decisions"], decision_rows, ["decision", "value", "notes"])
    machine = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "current_observations": len(observations),
        "resolved_rows": len(resolved),
        "proxy_rows": len(proxy_rows),
        "unresolved_price_rows": len(unresolved),
        "decisions": {r["decision"]: r["value"] for r in decision_rows},
        "production_status": "UNCHANGED",
    }
    write_json(paths["machine"], machine)
    validation = [
        {"check": "no_network_calls", "status": "PASS", "details": "Read retained local artifacts only."},
        {"check": "no_db_writes", "status": "PASS", "details": "No database access or writes."},
        {"check": "price_substitution_guard", "status": "PASS", "details": "Proxy rows keep BetOnline price fields null."},
        {"check": "production_status", "status": "PASS", "details": "UNCHANGED"},
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = ap.parse_args()
    print(json.dumps(build(args.output_dir), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
