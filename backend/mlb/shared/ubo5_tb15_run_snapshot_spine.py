"""Immutable complete-run evaluation snapshots for UBO-5 TB 1.5.

This spine is intentionally independent of the consensus selection lifecycle.
It freezes the complete authentic market evaluation for each run and maintains
derived daily population manifests without grading outcomes.
"""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

FIELDS = [
    "slate_date", "run_tag", "snapshot_timestamp_utc", "game_pk",
    "batter_mlb_id", "player_name", "team", "opponent", "game",
    "scheduled_start_utc", "prop_type", "line",
    "evaluation_scope", "evaluation_status", "lineup_status", "batting_order",
    "betonline_over_price", "betonline_under_price", "no_vig_over_probability",
    "ubo5_probability_over", "ubo5_over_edge_pp", "positive_over_edge_flag",
    "route_status", "feature_vector_sha256",
    "full_1_to_9_classification", "hybrid_display_status",
    "hard_confirm_flag", "likely_confirm_if_starting_flag",
    "prelineup_unscored_reason", "market_snapshot_path", "identity_source_path",
    "route_ledger_path", "prelineup_audit_path",
]
POPULATION_FIELDS = [
    "slate_date", "game_pk", "batter_mlb_id", "player_name", "game",
    "prop_type", "line", "first_run_tag", "first_timestamp_utc",
    "governing_run_tag", "governing_timestamp_utc", "ubo5_probability_over",
    "no_vig_over_probability", "ubo5_over_edge_pp",
    "full_1_to_9_classification", "hybrid_display_status",
    "run_observation_count", "first_evaluation_status", "last_evaluation_status",
]


def _read(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truth(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _identity(row: dict) -> tuple[str, int, int, str, float]:
    return (
        str(row["slate_date"]), int(float(row["game_pk"])),
        int(float(row["batter_mlb_id"])), "total_bases", 1.5,
    )


def _iso_time(value: object) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def _relative(path: Path, repository_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repository_root.resolve()))
    except ValueError:
        return str(path)


def _write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_observations(output_root: Path, date: str) -> list[dict]:
    rows = []
    for path in sorted((output_root / date / "run_snapshots").glob("ubo5_tb15_run_snapshot_*.csv")):
        rows.extend(_read(path))
    return rows


def _population_row(row: dict, first: dict | None = None) -> dict:
    first = first or row
    return {
        "slate_date": row["slate_date"], "game_pk": int(float(row["game_pk"])),
        "batter_mlb_id": int(float(row["batter_mlb_id"])),
        "player_name": row["player_name"], "game": row["game"],
        "prop_type": "total_bases", "line": 1.5,
        "first_run_tag": first["run_tag"],
        "first_timestamp_utc": first["snapshot_timestamp_utc"],
        "governing_run_tag": row["run_tag"],
        "governing_timestamp_utc": row["snapshot_timestamp_utc"],
        "ubo5_probability_over": row.get("ubo5_probability_over", ""),
        "no_vig_over_probability": row.get("no_vig_over_probability", ""),
        "ubo5_over_edge_pp": row.get("ubo5_over_edge_pp", ""),
        "full_1_to_9_classification": row.get("full_1_to_9_classification", ""),
        "hybrid_display_status": row.get("hybrid_display_status", ""),
    }


def rebuild_daily_manifest(output_root: Path, date: str) -> dict:
    observations = _run_observations(output_root, date)
    by_identity: dict[tuple, list[dict]] = {}
    for row in observations:
        by_identity.setdefault(_identity(row), []).append(row)
    for rows in by_identity.values():
        rows.sort(key=lambda row: (row["snapshot_timestamp_utc"], row["run_tag"]))

    broad, final_positive, hard, likely, attempted = [], [], [], [], []
    scheduled_starts = []
    for _, rows in sorted(by_identity.items()):
        first = rows[0]
        last = rows[-1]
        attempted.append({
            **_population_row(last, first),
            "run_observation_count": len(rows),
            "first_evaluation_status": first["evaluation_status"],
            "last_evaluation_status": last["evaluation_status"],
        })
        positives = [row for row in rows if _truth(row["positive_over_edge_flag"])]
        if positives:
            broad.append(_population_row(positives[0]))
        routed = [row for row in rows if row.get("route_status") == "UBO5_ROUTED"]
        if routed and _truth(routed[-1]["positive_over_edge_flag"]):
            final_positive.append(_population_row(routed[-1], routed[0]))
        hard_rows = [row for row in rows if _truth(row["hard_confirm_flag"])]
        if hard_rows:
            hard.append(_population_row(hard_rows[0]))
        likely_rows = [row for row in rows if _truth(row["likely_confirm_if_starting_flag"])]
        if likely_rows:
            likely.append(_population_row(likely_rows[0]))
        for row in rows:
            if row.get("scheduled_start_utc"):
                try:
                    scheduled_starts.append(_iso_time(row["scheduled_start_utc"]))
                except ValueError:
                    pass
    now = datetime.now(timezone.utc)
    final_status = (
        "FINAL_PREGAME_POPULATION_CERTIFIED"
        if scheduled_starts and now >= max(scheduled_starts)
        else "PROVISIONAL_UNTIL_ALL_OBSERVED_GAMES_START"
    )
    inventory = []
    identity_reject_count = 0
    for path in sorted((output_root / date / "run_snapshots").glob("ubo5_tb15_run_snapshot_*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        inventory.append({
            "run_tag": payload["run_tag"], "snapshot_timestamp_utc": payload["snapshot_timestamp_utc"],
            "row_count": payload["row_count"], "snapshot_sha256": payload["snapshot_sha256"],
            "path": str(path),
        })
        identity_reject_count += int(payload.get("identity_reject_count", 0))
    payload = {
        "slate_date": date,
        "spine_status": "CERTIFIED_COMPLETE_RUN_SNAPSHOTS" if inventory else "NO_RUN_SNAPSHOTS",
        "identity_rule": "slate_date|game_pk|batter_mlb_id|prop_type|line",
        "run_snapshot_count": len(inventory),
        "run_inventory": inventory,
        "population_definitions": {
            "broad_ever_positive": "first confirmed exact-order UBO-5 positive-edge appearance",
            "final_pregame_positive": "last preserved confirmed exact-order UBO-5 evaluation before game start",
            "prelineup_hard_confirm": "first full-1-to-9 ROBUST_CONFIRM appearance",
            "prelineup_likely_confirm": "first separately labeled LIKELY CONFIRM IF STARTING appearance",
            "all_attempted_evaluated": "one identity summary over every preserved run observation",
        },
        "final_pregame_population_status": final_status,
        "counts": {
            "all_run_observations": len(observations),
            "all_identity_rejected_attempts": identity_reject_count,
            "all_attempted_rows_including_identity_rejects": len(observations) + identity_reject_count,
            "all_attempted_evaluated_identities": len(attempted),
            "broad_ever_positive": len(broad),
            "final_pregame_positive": len(final_positive),
            "prelineup_hard_confirm": len(hard),
            "prelineup_likely_confirm": len(likely),
        },
        "populations": {
            "broad_ever_positive": broad,
            "final_pregame_positive": final_positive,
            "prelineup_hard_confirm": hard,
            "prelineup_likely_confirm": likely,
            "all_attempted_evaluated": attempted,
        },
    }
    path = output_root / date / f"ubo5_tb15_run_population_manifest_{date}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    population_dir = output_root / date / "run_populations"
    population_dir.mkdir(parents=True, exist_ok=True)
    for label, rows in payload["populations"].items():
        csv_path = population_dir / f"ubo5_tb15_{label}_{date}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=POPULATION_FIELDS, extrasaction="ignore"
            )
            writer.writeheader()
            writer.writerows(rows)
    return payload


def freeze_complete_run(
    *,
    repository_root: Path,
    output_root: Path,
    date: str,
    run_tag: str,
    market_snapshot_path: Path,
    identity_source_path: Path,
    route_ledger_path: Path,
    prelineup_audit_path: Path,
    identity_rejects: list[dict] | None = None,
) -> dict:
    """Freeze one complete run after confirmed and pre-lineup evaluation."""
    snapshot = json.loads(market_snapshot_path.read_text(encoding="utf-8"))
    captured = str(snapshot.get("captured_at_utc") or "")
    audits = _read(prelineup_audit_path)
    routes = {
        (int(float(row["game_pk"])), int(float(row["batter_mlb_id"]))): row
        for row in _read(route_ledger_path)
        if _num(row.get("game_pk")) is not None and _num(row.get("batter_mlb_id")) is not None
    }
    identities = {
        (int(float(row.get("game_id") or row.get("game_pk"))),
         int(float(row.get("player_id") or row.get("batter_mlb_id")))): row
        for row in _read(identity_source_path)
        if _num(row.get("game_id") or row.get("game_pk")) is not None
        and _num(row.get("player_id") or row.get("batter_mlb_id")) is not None
    }
    rows = []
    for audit in audits:
        key = (int(float(audit["game_pk"])), int(float(audit["batter_mlb_id"])))
        route, identity = routes.get(key, {}), identities.get(key, {})
        probability = _num(route.get("ubo5_probability_over"))
        no_vig = _num(audit.get("no_vig_over_probability"))
        edge = probability - no_vig if probability is not None and no_vig is not None else None
        batting = route.get("batting_order_position") or route.get("confirmed_batting_order") or ""
        route_is_ubo5 = str(route.get("model_source") or "").startswith("UBO5") or (
            probability is not None and not route.get("exclusion_reason")
        )
        pre_class = str(audit.get("full_1_to_9_classification") or "")
        hybrid = str(audit.get("hybrid_display_status") or "")
        team = route.get("team") or identity.get("team") or identity.get("team_code") or ""
        opponent = route.get("opponent") or identity.get("opponent") or identity.get("opponent_code") or ""
        scheduled = route.get("scheduled_start_utc") or identity.get("game_time") or ""
        rows.append({
            "slate_date": date, "run_tag": run_tag,
            "snapshot_timestamp_utc": captured or audit.get("snapshot_timestamp_utc", ""),
            "game_pk": key[0], "batter_mlb_id": key[1],
            "player_name": audit.get("player_name", ""), "team": team, "opponent": opponent,
            "game": audit.get("game", ""), "scheduled_start_utc": scheduled,
            "prop_type": "total_bases", "line": "1.5",
            "evaluation_scope": "AUTHENTIC_UNSTARTED_TWO_SIDED_BETONLINE_TB15",
            "evaluation_status": "SCORED" if pre_class or probability is not None else "ATTEMPTED_UNSCORED",
            "lineup_status": audit.get("lineup_status", ""), "batting_order": batting,
            "betonline_over_price": audit.get("BetOnline_over_price", ""),
            "betonline_under_price": audit.get("BetOnline_under_price", ""),
            "no_vig_over_probability": audit.get("no_vig_over_probability", ""),
            "ubo5_probability_over": "" if probability is None else f"{probability:.10f}",
            "ubo5_over_edge_pp": "" if edge is None else f"{edge * 100:.10f}",
            "positive_over_edge_flag": str(bool(route_is_ubo5 and edge is not None and edge > 0)).lower(),
            "route_status": "UBO5_ROUTED" if route_is_ubo5 else (
                "INCUMBENT_FALLBACK" if route else "PRELINEUP_ONLY"
            ),
            "feature_vector_sha256": route.get("feature_vector_sha256", ""),
            "full_1_to_9_classification": pre_class,
            "hybrid_display_status": hybrid,
            "hard_confirm_flag": str(pre_class == "ROBUST_CONFIRM").lower(),
            "likely_confirm_if_starting_flag": str(hybrid == "LIKELY CONFIRM IF STARTING").lower(),
            "prelineup_unscored_reason": audit.get("unscored_reason", ""),
            "market_snapshot_path": _relative(market_snapshot_path, repository_root),
            "identity_source_path": _relative(identity_source_path, repository_root),
            "route_ledger_path": _relative(route_ledger_path, repository_root),
            "prelineup_audit_path": _relative(prelineup_audit_path, repository_root),
        })
    rows.sort(key=lambda row: (int(row["game_pk"]), int(row["batter_mlb_id"])))
    canonical = json.dumps(
        {"rows": rows, "identity_rejects": identity_rejects or []},
        sort_keys=True, separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    folder = output_root / date / "run_snapshots"
    folder.mkdir(parents=True, exist_ok=True)
    csv_path = folder / f"ubo5_tb15_run_snapshot_{run_tag}.csv"
    json_path = folder / f"ubo5_tb15_run_snapshot_{run_tag}.json"
    if csv_path.exists() or json_path.exists():
        prior = json.loads(json_path.read_text(encoding="utf-8"))
        if prior.get("snapshot_sha256") != digest:
            raise RuntimeError(f"IMMUTABLE_COMPLETE_RUN_SNAPSHOT_CONFLICT:{run_tag}")
    else:
        _write_csv(csv_path, rows)
        json_path.write_text(json.dumps({
            "slate_date": date, "run_tag": run_tag, "snapshot_timestamp_utc": captured,
            "population_label": "All attempted/evaluated UBO-5 TB 1.5 rows for this run",
            "row_count": len(rows), "snapshot_sha256": digest,
            "identity_reject_count": len(identity_rejects or []),
            "identity_rejects": identity_rejects or [],
            "source_paths": {
                "market_snapshot": _relative(market_snapshot_path, repository_root),
                "identity_source": _relative(identity_source_path, repository_root),
                "route_ledger": _relative(route_ledger_path, repository_root),
                "prelineup_audit": _relative(prelineup_audit_path, repository_root),
            },
            "source_sha256": {
                "market_snapshot": _sha256(market_snapshot_path),
                "identity_source": _sha256(identity_source_path),
                "route_ledger": _sha256(route_ledger_path),
                "prelineup_audit": _sha256(prelineup_audit_path),
            },
            "rows": rows,
        }, indent=2) + "\n", encoding="utf-8")
    return rebuild_daily_manifest(output_root, date)
