#!/usr/bin/env python3
"""Classify and grade retained NHL predictions in an isolated research population.

Read-only inputs are immutable operational packages and completed local
reconciliation artifacts. Outputs are append-only research artifacts under
``artifacts/analysis`` and never enter governed reconciliation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.nhl.postgame_reconcile.core import _grade_points, _grade_saves
from backend.nhl.research.prediction_evidence import (
    CHRONOLOGY_UNVERIFIED, CORROBORATED_PREGAME, VERIFIED_PROSPECTIVE,
    canonical_group_id, classify_evidence, select_latest_groups, sha256_file,
    verify_manifest, compare_replay,
)
from backend.nhl.sog_cold_start.core import build_predictions


ROOT = Path(__file__).resolve().parents[3]
OPS = ROOT / "artifacts/operational/nhl"
DEFAULT_OUT = ROOT / "artifacts/analysis/nhl/prediction_evidence_research"
SOG_LINES = {1.5, 2.5, 3.5}
POINTS_LINES = {0.5, 1.5, 2.5}
SAVES_LINES = {18.5 + i for i in range(13)}


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _truth(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes"}


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str,
                                     separators=(",", ":")).encode()).hexdigest()


def _field(row: pd.Series, names: tuple[str, ...]) -> Any:
    for name in names:
        if name in row and pd.notna(row[name]) and str(row[name]) not in {"", "nan", "None"}:
            return row[name]
    return None


def _find_status(lane: str, slate: str, run_dir: Path) -> tuple[Path | None, dict[str, Any] | None]:
    status_root = OPS / f"{lane.lower()}_prediction_only/status/{slate}"
    for path in sorted(status_root.glob("*.json")) if status_root.exists() else []:
        try:
            status = _json(path)
        except (OSError, ValueError):
            continue
        if Path(str(status.get("run_dir", ""))).resolve() == run_dir.resolve():
            return path, status
    return None, None


def _find_parent(lane: str, slate: str, status_path: Path | None) -> tuple[Path | None, dict[str, Any] | None]:
    if status_path is None:
        return None, None
    roots = [OPS / f"cross_market_shadow/orchestration_status/{slate}"]
    status = _json(status_path)
    source_time = pd.Timestamp(status.get("invocation_timestamp_utc"))
    fallback: tuple[Path, dict[str, Any]] | None = None
    for root in roots:
        for path in sorted(root.glob("capture_*.json")) if root.exists() else []:
            try:
                parent = _json(path)
            except (OSError, ValueError):
                continue
            link = (parent.get("player_prop_observers") or {}).get(lane)
            if link and Path(str(link.get("receipt_path", ""))).resolve() == status_path.resolve():
                return path, parent
            # Older mainline receipts predate explicit child-status path fields.
            # Matching same-process identity and a narrow post-child timestamp
            # corroborates execution, but does not establish governed lineage.
            try:
                parent_time = pd.Timestamp(parent.get("invocation_timestamp_utc"))
                delta = (parent_time - source_time).total_seconds()
            except (TypeError, ValueError):
                continue
            if (parent.get("process_id") == status.get("process_id")
                    and status.get("status") == "CAPTURED" and 0 <= delta <= 30):
                fallback = (path, parent)
    return fallback if fallback else (None, None)


def _schedule_starts(slate: str, reconciliation: Path | None,
                     fallback_schedule: Path | None) -> dict[int, str]:
    path = reconciliation / "canonical_admitted_slate.csv" if reconciliation else fallback_schedule
    if path is None or not path.is_file():
        return {}
    schedule = pd.read_csv(path)
    if not {"game_id", "scheduled_start_time_utc"}.issubset(schedule.columns):
        return {}
    if "slate_date" in schedule and not schedule.slate_date.astype(str).eq(slate).all():
        return {}
    return {int(row.game_id): str(row.scheduled_start_time_utc)
            for row in schedule.itertuples(index=False)}


def _verify_final_outcomes(slate: str) -> tuple[Path | None, dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    parent = OPS / f"postgame_reconciliation/{slate}"
    candidates = sorted(parent.glob("reconciliation=*")) if parent.exists() else []
    for directory in reversed(candidates):
        summary_path = directory / "summary.json"
        marker = directory / "RUN_COMPLETE.json"
        manifest = verify_manifest(directory)
        if not summary_path.is_file() or not marker.is_file() or not manifest["valid"]:
            continue
        summary = _json(summary_path)
        if summary.get("status") != "COMPLETE" or str(summary.get("slate_date")) != slate:
            continue
        schedule = pd.read_csv(directory / "canonical_admitted_slate.csv")
        games = pd.read_csv(directory / "canonical_game_outcomes.csv")
        skaters = pd.read_csv(directory / "canonical_skater_outcomes.csv")
        goalies = pd.read_csv(directory / "canonical_goalie_outcomes.csv")
        game_ids = set(schedule.game_id.astype(int))
        if (len(game_ids) != len(schedule) or set(games.game_id.astype(int)) != game_ids
                or not games.official_final.map(_truth).all()):
            continue
        return directory, {"status": "CERTIFIED_FINAL_PACKAGE_VERIFIED",
                           "package": str(directory), "manifest_sha256": manifest["manifest_sha256"],
                           "games": len(game_ids), "summary_sha256": sha256_file(summary_path)}, \
            schedule, skaters, goalies
    return None, {"status": "NO_COMPLETE_FINAL_OUTCOME_PACKAGE", "slate_date": slate}, \
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def _rows_integrity(lane: str, frame: pd.DataFrame) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if frame.empty:
        return False, ["PREDICTION_FILE_EMPTY"]
    if lane == "SOG":
        keys = ["game_id", "player_id", "contract_arm", "line"]
        lines = SOG_LINES
        pcol = "p_over"
        model_cols = ["model_family", "model_version", "contract_version", "contract_sha256"]
    elif lane == "POINTS":
        keys = ["game_id", "player_id", "model_version", "line"]
        lines = POINTS_LINES
        pcol = "prob_over"
        model_cols = ["model", "model_version", "probability_construction"]
    else:
        keys = ["game_id", "goalie_id", "model_version", "line"]
        lines = SAVES_LINES
        pcol = "prob_over"
        model_cols = ["model", "model_version", "prediction_semantics"]
    if not set(keys + [pcol, *model_cols]).issubset(frame.columns):
        return False, ["PREDICTION_SCHEMA_INCOMPLETE"]
    if frame.duplicated(keys).any():
        duplicate_keys = set(tuple(row) for row in frame.loc[frame.duplicated(keys, keep=False), keys[:-1]].itertuples(index=False, name=None))
        reasons.extend(f"DUPLICATE_PREDICTION_IDENTITY:{key}" for key in sorted(duplicate_keys))
    probs = pd.to_numeric(frame[pcol], errors="coerce")
    invalid_prob_rows = frame.loc[probs.isna() | (probs < 0) | (probs > 1)]
    for key in sorted(set(tuple(row) for row in invalid_prob_rows[keys[:-1]].itertuples(index=False, name=None))):
        reasons.append(f"PROBABILITY_INVALID:{key}")
    for key, group in frame.groupby(keys[:-1], dropna=False):
        actual_lines = set(pd.to_numeric(group.line, errors="coerce").dropna().astype(float))
        if actual_lines != lines:
            reasons.append(f"ATOMIC_LINE_SET_INCOMPLETE:{key}")
        for col in [c for c in ["phase", "run_id", "prediction_timestamp_utc",
                                "feature_cutoff_utc", "feature_cutoff_timestamp_utc",
                                "input_cutoff_timestamp_utc", *model_cols] if c in group]:
            if group[col].astype(str).nunique(dropna=False) != 1:
                reasons.append(f"ATOMIC_SOURCE_FIELD_CONFLICT:{col}:{key}")
    return True, reasons


def _group_candidates(lane: str, package: Path, status_path: Path | None,
                      status: dict[str, Any] | None, parent_path: Path | None,
                      parent: dict[str, Any] | None, manifest: dict[str, Any],
                      prediction_file: Path, frame: pd.DataFrame,
                      input_file: Path | None, inputs: pd.DataFrame,
                      starts: dict[int, str], current_contract_hash: str | None = None
                      ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid_rows, row_reasons = _rows_integrity(lane, frame)
    if lane == "SOG":
        group_cols, id_col = ["game_id", "player_id", "contract_arm"], "player_id"
        timestamp_cols = ("feature_cutoff_utc", "input_cutoff_utc")
        pcol = "p_over"
    elif lane == "POINTS":
        group_cols, id_col = ["game_id", "player_id", "model_version"], "player_id"
        grouped_source_col = "model_version"
        timestamp_cols = ("feature_cutoff_timestamp_utc", "input_cutoff_timestamp_utc")
        pcol = "prob_over"
    else:
        group_cols, id_col = ["game_id", "goalie_id", "model_version"], "goalie_id"
        grouped_source_col = "model_version"
        timestamp_cols = ("feature_cutoff_timestamp_utc", "input_cutoff_timestamp_utc")
        pcol = "prob_over"
    if lane == "SOG":
        grouped_source_col = "contract_arm"
    source_hash = sha256_file(prediction_file)
    status_hash = sha256_file(status_path) if status_path else None
    parent_hash = sha256_file(parent_path) if parent_path else None
    records: list[dict[str, Any]] = []
    detail: list[dict[str, Any]] = []
    for key, group in frame.groupby(group_cols, sort=True, dropna=False):
        parts = key if isinstance(key, tuple) else (key,)
        game_id = int(group.iloc[0].game_id)
        identity = canonical_group_id(lane, tuple(parts))
        local_valid = valid_rows and not any(reason.endswith(str(key)) for reason in row_reasons)
        source_cutoff = _field(group.iloc[0], timestamp_cols)
        prediction_at = _field(group.iloc[0], ("prediction_timestamp_utc", "observation_timestamp_utc"))
        publication_at = None
        publication_method = None
        if status:
            publication_at = status.get("durable_publication_timestamp_utc")
            if publication_at:
                publication_method = "CHILD_STATUS_POST_PUBLICATION_TIMESTAMP"
        if not publication_at and parent:
            publication_at = parent.get("invocation_timestamp_utc")
            if publication_at:
                publication_method = "LINKED_MAINLINE_PARENT_RECEIPT_COMPLETION_WITNESS"
        if not publication_at and status:
            publication_at = status.get("invocation_timestamp_utc")
            if publication_at:
                publication_method = "CHILD_INVOCATION_ONLY_NOT_DURABLE"
        # For invocation-only records, withholding durable time prevents an
        # upgrade: the observation remains chronology-unverified.
        durable_for_class = publication_at if publication_method != "CHILD_INVOCATION_ONLY_NOT_DURABLE" else None

        input_key = ["game_id", id_col]
        input_ok = input_file is not None and input_file.is_file() and not inputs.empty
        if input_ok and set(input_key).issubset(inputs.columns):
            present = set(zip(inputs.game_id.astype(int), inputs[id_col].astype(int)))
            input_ok = (game_id, int(group.iloc[0][id_col])) in present
        else:
            input_ok = False
        model_cols = (["model_family", "model_version", "contract_version", "contract_sha256"]
                      if lane == "SOG" else ["model_version", "probability_construction"]
                      if lane == "POINTS" else ["model_version", "prediction_semantics"])
        model_ok = all(str(group.iloc[0].get(col) or "").strip() not in {"", "nan", "None"}
                       for col in model_cols)
        if lane == "SOG" and current_contract_hash:
            model_ok = model_ok and group.contract_sha256.astype(str).eq(current_contract_hash).all()
        if lane == "SOG":
            start = starts.get(game_id)
            cutoff_col = "feature_cutoff_utc"
        elif lane == "POINTS":
            start = starts.get(game_id)
            cutoff_col = "feature_cutoff_timestamp_utc"
        else:
            start = starts.get(game_id)
            cutoff_col = "input_cutoff_timestamp_utc"
        cutoff = group.iloc[0].get(cutoff_col)
        if (cutoff is None or pd.isna(cutoff)) and "input_cutoff_timestamp_utc" in group:
            cutoff = group.iloc[0].get("input_cutoff_timestamp_utc")
        roster_bound = bool((status or {}).get("current_slate_roster_observation"))
        cert_bound = bool((status or {}).get("prior_day_reconciliation_certificate"))
        payload_rows = group.sort_values("line").replace({np.nan: None}).to_dict("records")
        payload_hash = _digest(payload_rows)
        evidence = classify_evidence(
            scheduled_start_utc=start, feature_cutoff_utc=cutoff,
            prediction_timestamp_utc=prediction_at,
            durable_publication_timestamp_utc=durable_for_class,
            contemporaneous_execution_witness=bool(status and status_path and parent and parent_path),
            source_content_hash_bound_at_publication=bool(
                status and status.get("source_manifest_sha256") == manifest.get("manifest_sha256")
            ),
            canonical_identity_valid=bool(local_valid and game_id in starts),
            prediction_values_valid=bool(local_valid), input_snapshot_complete=bool(input_ok),
            model_identity_valid=bool(model_ok), manifest_valid=bool(manifest.get("valid")),
            reconciliation_certificate_bound=cert_bound, roster_evidence_bound=roster_bound,
            conflicting_evidence=not local_valid,
        )
        # The child CAPTURED receipt and exact parent link witness execution;
        # status alone is not enough to establish post-publication durability.
        record = {
            "lane": lane, "slate_date": str(group.iloc[0].get("slate_date", package.parent.parent.name.split("=")[-1])),
            "atomic_group_identity": identity, "game_id": game_id,
            "entity_id": int(group.iloc[0][id_col]), "model_identity": ":".join(
                str(group.iloc[0].get(col, "")) for col in model_cols),
            "grouped_source_value": str(group.iloc[0].get(grouped_source_col, "")),
            "source_phase": str(group.iloc[0].get("phase", package.parent.name.split("=")[-1])),
            "source_path": str(package), "source_prediction_file": str(prediction_file),
            "source_sha256": source_hash, "source_manifest_sha256": manifest.get("manifest_sha256"),
            "status_receipt_path": str(status_path) if status_path else None,
            "status_receipt_sha256": status_hash,
            "parent_receipt_path": str(parent_path) if parent_path else None,
            "parent_receipt_sha256": parent_hash,
            "prediction_timestamp_utc": str(prediction_at) if prediction_at is not None else None,
            "feature_cutoff_timestamp_utc": str(cutoff) if cutoff is not None else None,
            "durable_publication_timestamp_utc": str(publication_at) if publication_at else None,
            "publication_witness_method": (
                "EXACT_LINKED_CHILD_STATUS_AND_PARENT" if parent and
                (parent.get("player_prop_observers") or {}).get(lane) else
                "SAME_PROCESS_CHILD_AND_MAINLINE_RECEIPTS" if parent else publication_method),
            "input_snapshot_path": str(input_file) if input_file else None,
            "input_snapshot_sha256": sha256_file(input_file) if input_file and input_file.is_file() else None,
            "input_availability_source": str(group.iloc[0].get("roster_source", "")),
            "classification": evidence.classification,
            "evidence": evidence.as_dict(), "row_count": int(len(group)),
            "line_count": int(group.line.nunique()), "line_values": sorted(
                pd.to_numeric(group.line, errors="coerce").dropna().astype(float).tolist()),
            "prediction_payload_sha256": payload_hash,
            "package_completion_marker_present": (package / "RUN_COMPLETE.json").is_file(),
            "status": (status or {}).get("status"),
            "status_readiness_dependency": (status or {}).get("morning_readiness_dependency"),
            "status_prior_certificate_present": cert_bound,
            "status_roster_evidence_present": roster_bound,
            "replay_status": "NOT_ATTEMPTED",
            "replay_max_absolute_difference": None,
        }
        records.append({**record, "durable_publication_timestamp_utc":
                        str(durable_for_class) if durable_for_class else None})
        detail.append({**record, "exclusion_reasons": row_reasons if not local_valid else []})
    return records, detail


def _replay_sog(package: Path, frame: pd.DataFrame, meta: dict[str, Any]) -> dict[str, Any]:
    feature_path = package / "feature_source_snapshot.csv"
    if not feature_path.is_file():
        return {"status": "FAILED_MISSING_FEATURE_SNAPSHOT"}
    try:
        features = pd.read_csv(feature_path)
        timestamp = str(meta.get("prediction_timestamp_utc"))
        replay, _, _ = build_predictions(
            features, slate_date=str(meta["slate_date"]), phase=str(meta["phase"]),
            prediction_timestamp_utc=timestamp, input_cutoff_utc=timestamp,
        )
        keys = ["game_id", "player_id", "contract_arm", "line", "prediction_identity"]
        source = frame.sort_values(keys).reset_index(drop=True)
        replay = replay.sort_values(keys).reset_index(drop=True)
        comparison = compare_replay(source.replace({np.nan: None}).to_dict("records"),
                                    replay.replace({np.nan: None}).to_dict("records"),
                                    identity_fields=tuple(keys),
                                    value_fields=("expected_sog", "p_over", "p_under"))
        comparison["replayed_prediction_sha256"] = _digest(
            replay.replace({np.nan: None}).to_dict("records"))
        return comparison
    except Exception as error:
        return {"status": f"FAILED_{type(error).__name__}:{error}"}


def _replay_points(package: Path, frame: pd.DataFrame) -> dict[str, Any]:
    try:
        from backend.nhl.points_shadow.core import (
            APPROVED_SHADOW_FEATURE_CONTRACT_PATH, construct_coherent_ladders,
            evaluate_ladder_coherence, score_frozen, verify_frozen_identity,
        )
        feature_contract = _json(APPROVED_SHADOW_FEATURE_CONTRACT_PATH)
        feature_contract["_identity_path"] = str(APPROVED_SHADOW_FEATURE_CONTRACT_PATH)
        identity = verify_frozen_identity(feature_contract)
        features = pd.read_csv(package / "feature_input_snapshot.csv")
        raw = construct_coherent_ladders(score_frozen(features, identity))
        ladder = evaluate_ladder_coherence(raw, identity)
        replay = raw.merge(ladder[["game_id", "player_id", "ladder_coherence_decision"]],
                           on=["game_id", "player_id"], validate="many_to_one")
        replay = replay.loc[replay.ladder_coherence_decision.isin(
            {"PASS_LADDER_COHERENCE", "WARNING_MINOR_LADDER_INCOHERENCE"})]
        keys = ["game_id", "player_id", "line"]
        comparison = compare_replay(frame.replace({np.nan: None}).to_dict("records"),
                                    replay.replace({np.nan: None}).to_dict("records"),
                                    identity_fields=tuple(keys), value_fields=("prob_over",))
        comparison["model_identity"] = identity.get("model_version")
        return comparison
    except Exception as error:
        return {"status": f"FAILED_{type(error).__name__}:{error}"}


def _replay_saves(package: Path, frame: pd.DataFrame) -> dict[str, Any]:
    try:
        from backend.nhl.saves_shadow.core import score_frozen, verify_frozen_identity
        identity = verify_frozen_identity()
        features = pd.read_csv(package / "feature_input_snapshot.csv")
        features["player_id"] = features["goalie_id"].astype(int)
        replay, _ = score_frozen(features, 1.0, identity)
        keys = ["game_id", "goalie_id", "line"]
        comparison = compare_replay(frame.replace({np.nan: None}).to_dict("records"),
                                    replay.replace({np.nan: None}).to_dict("records"),
                                    identity_fields=tuple(keys),
                                    value_fields=("expected_saves", "raw_prob_over", "prob_over"))
        comparison["model_identity"] = identity.get("model_version")
        return comparison
    except Exception as error:
        return {"status": f"FAILED_{type(error).__name__}:{error}"}


def _discover_lane(lane: str, slate: str, starts: dict[int, str], current_contract_hash: str | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    root = OPS / f"{lane.lower()}_prediction_only/season=2026/slate_date={slate}"
    all_records: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    replays: dict[str, Any] = {}
    patterns = ["phase=*/run_id=*/run_metadata.json"]
    for metadata_path in sorted(root.glob(patterns[0])) if root.exists() else []:
        package = metadata_path.parent
        meta = _json(metadata_path)
        pred_name = {"SOG": "immutable_predictions.csv", "POINTS": "immutable_predictions.csv",
                     "SAVES": "immutable_conditional_predictions.csv"}[lane]
        pred_path = package / pred_name
        input_name = {"SOG": "feature_source_snapshot.csv", "POINTS": "feature_input_snapshot.csv",
                      "SAVES": "feature_input_snapshot.csv"}[lane]
        input_path = package / input_name
        if not pred_path.is_file():
            continue
        frame = pd.read_csv(pred_path)
        inputs = pd.read_csv(input_path) if input_path.is_file() else pd.DataFrame()
        required_snapshot_fields = {
            "SOG": {"game_id", "player_id", "feature_cutoff_utc", "scheduled_start_time_utc",
                    "roster_status", "lineup_status", "prior_minutes", "prior_sog_per60",
                    "prior_toi_per_game", "older_minutes", "older_sog_per60", "older_toi_per_game"},
            "POINTS": {"game_id", "player_id", "feature_cutoff_timestamp_utc",
                       "feature_history_max_timestamp_utc", "roster_source_timestamp_utc",
                       "d10_sog_per60", "attempts_d10_per60"},
            "SAVES": {"game_id", "goalie_id", "feature_cutoff_timestamp_utc",
                      "feature_history_max_timestamp_utc", "roster_source_timestamp_utc",
                      "d10_shots_faced_per60", "d10_save_pct"},
        }[lane]
        snapshot_schema_complete = bool(input_path.is_file() and not inputs.empty
                                        and required_snapshot_fields.issubset(inputs.columns))
        manifest = verify_manifest(package)
        status_path, status = _find_status(lane, slate, package)
        parent_path, parent = _find_parent(lane, slate, status_path)
        if status and status.get("status") != "CAPTURED":
            status = None
        records, rows = _group_candidates(
            lane, package, status_path, status, parent_path, parent, manifest,
            pred_path, frame, input_path if input_path.is_file() else None,
            inputs, starts, current_contract_hash,
        )
        if not snapshot_schema_complete:
            for collection in (records, rows):
                for record in collection:
                    record["classification"] = CHRONOLOGY_UNVERIFIED
                    record["evidence"]["input_availability"] = "INCOMPLETE_OR_UNVERIFIED"
                    record["evidence"]["reasons"].append("INPUT_SNAPSHOT_SCHEMA_INCOMPLETE")
        all_records.extend(records)
        details.extend(rows)
        replay_func = {"SOG": lambda: _replay_sog(package, frame, meta),
                       "POINTS": lambda: _replay_points(package, frame),
                       "SAVES": lambda: _replay_saves(package, frame)}[lane]
        replays[str(package)] = replay_func()
        for record in all_records:
            if record["source_path"] == str(package):
                record["replay_status"] = replays[str(package)]["status"]
                record["replay_max_absolute_difference"] = max(
                    replays[str(package)].get("max_absolute_differences", {}).values(), default=None)
                if replays[str(package)]["status"].startswith("FAILED"):
                    record["classification"] = CHRONOLOGY_UNVERIFIED
                    record["evidence"]["reasons"].append("ORIGINAL_MODEL_REPLAY_FAILED")
        for record in details:
            if record["source_path"] == str(package):
                record["replay_status"] = replays[str(package)]["status"]
                record["replay_max_absolute_difference"] = max(
                    replays[str(package)].get("max_absolute_differences", {}).values(), default=None)
                if replays[str(package)]["status"].startswith("FAILED"):
                    record["classification"] = CHRONOLOGY_UNVERIFIED
                    record["evidence"]["reasons"].append("ORIGINAL_MODEL_REPLAY_FAILED")
    return all_records, details, replays


def _metric_rows(lane: str, graded: pd.DataFrame, actual_col: str, probability_col: str,
                 state_col: str) -> list[dict[str, Any]]:
    if graded.empty:
        return []
    eligible_states = {"WIN", "LOSS", "PUSH", "SETTLED", "REGULAR_SEASON_GRADED"}
    eligible = graded.loc[graded[state_col].astype(str).isin(eligible_states)].copy()
    if eligible.empty:
        return []
    result = []
    role_col = "contract_arm" if lane == "SOG" else "model_version"
    for (role, line), group in eligible.groupby([role_col, "line"], dropna=False):
        outcome = pd.to_numeric(group[actual_col], errors="coerce")
        line_values = pd.to_numeric(group.line, errors="coerce")
        prob = pd.to_numeric(group[probability_col], errors="coerce").clip(1e-6, 1 - 1e-6)
        over = outcome.gt(line_values).astype(float)
        group_key = "player_id" if lane != "SAVES" else "goalie_id"
        result.append({
            "lane": lane, "model_role": str(role), "line": float(line),
            "prediction_rows": int(len(group)), "unique_games": int(group.game_id.nunique()),
            "unique_entities": int(group[["game_id", group_key]].drop_duplicates().shape[0]),
            "unique_game_entity_line": int(group[["game_id", group_key, "line"]].drop_duplicates().shape[0]),
            "graded_rows": int(len(group)),
            "wins": int(group[state_col].astype(str).eq("WIN").sum()),
            "losses": int(group[state_col].astype(str).eq("LOSS").sum()),
            "pushes": int(group[state_col].astype(str).eq("PUSH").sum()),
            "observed_over_rate": float(over.mean()), "mean_predicted_over_probability": float(prob.mean()),
            "brier": float(np.mean((prob.to_numpy() - over.to_numpy()) ** 2)),
            "log_loss": float(np.mean(-(over.to_numpy() * np.log(prob.to_numpy())
                                        + (1 - over.to_numpy()) * np.log(1 - prob.to_numpy())))),
        })
    return result


def _identity_set(frame: pd.DataFrame, columns: list[str]) -> set[tuple[str, ...]]:
    if frame.empty or not set(columns).issubset(frame.columns):
        return set()
    return {tuple(str(row[column]) for column in columns)
            for row in frame[columns].drop_duplicates().to_dict("records")}


def _grade_lane(lane: str, selected: list[dict[str, Any]],
                all_records: list[dict[str, Any]], reconciliation: Path,
                schedule: pd.DataFrame, skaters: pd.DataFrame,
                goalies: pd.DataFrame, slate: str) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if not selected:
        return pd.DataFrame(), []
    rows: list[pd.DataFrame] = []
    source_frames: dict[str, pd.DataFrame] = {}
    for selected_group in selected:
        package = Path(selected_group["source_path"])
        pred_name = {"SOG": "immutable_predictions.csv", "POINTS": "immutable_predictions.csv",
                     "SAVES": "immutable_conditional_predictions.csv"}[lane]
        if str(package) not in source_frames:
            source_frames[str(package)] = pd.read_csv(package / pred_name)
        frame = source_frames[str(package)]
        entity_col = "player_id" if lane != "SAVES" else "goalie_id"
        group_col = "contract_arm" if lane == "SOG" else "model_version"
        frame = frame.loc[(frame.game_id.astype(int) == int(selected_group["game_id"]))
                          & (frame[entity_col].astype(int) == int(selected_group["entity_id"]))
                          & (frame[group_col].astype(str) == str(selected_group["grouped_source_value"]))].copy()
        frame["research_evidence_class"] = selected_group["classification"]
        frame["research_source_path"] = selected_group["source_path"]
        frame["research_source_sha256"] = selected_group["source_sha256"]
        frame["research_manifest_sha256"] = selected_group["source_manifest_sha256"]
        frame["research_parent_receipt_path"] = selected_group["parent_receipt_path"]
        frame["research_parent_receipt_sha256"] = selected_group["parent_receipt_sha256"]
        frame["research_atomic_group_identity"] = selected_group["atomic_group_identity"]
        rows.append(frame)
    predictions = pd.concat(rows, ignore_index=True)
    if predictions.duplicated(["game_id", "player_id" if lane != "SAVES" else "goalie_id",
                               "line", "contract_arm" if lane == "SOG" else "model_version"]).any():
        raise RuntimeError(f"RESEARCH_DUPLICATE_SELECTED_PREDICTION:{lane}:{slate}")
    if lane == "SOG":
        outcomes = skaters.rename(columns={"participation_state": "participation_status",
                                           "official_sog": "official_sog"}).copy()
        outcomes["official_final"] = outcomes.official_final.map(_truth)
        graded = predictions.merge(
            outcomes[["canonical_season", "slate_date", "game_id", "player_id", "official_sog",
                      "participation_status", "official_final", "outcome_source",
                      "outcome_source_timestamp_utc"]],
            on=["canonical_season", "slate_date", "game_id", "player_id"], how="left",
            validate="many_to_one")
        state = []
        for row in graded.itertuples(index=False):
            if pd.isna(row.participation_status) or not bool(row.official_final):
                state.append("UNRESOLVED_UNGRADED")
            elif row.participation_status != "PARTICIPATED" or pd.isna(row.official_sog):
                state.append("NONPARTICIPANT_UNGRADED")
            elif float(row.official_sog) == float(row.line):
                state.append("PUSH")
            else:
                side = "OVER" if float(row.official_sog) > float(row.line) else "UNDER"
                state.append("WIN" if side == row.selected_side else "LOSS")
        graded["grading_state"] = state
        metrics = _metric_rows(lane, graded, "official_sog", "p_over", "grading_state")
    elif lane == "POINTS":
        grade_frames = _grade_points(None, schedule, skaters, predictions=predictions)
        graded = grade_frames["points"].copy()
        metrics = _metric_rows(lane, graded, "official_points", "prob_over", "grading_status")
    else:
        grade_frames = _grade_saves(None, schedule, goalies, predictions=predictions)
        graded = grade_frames["saves"].copy()
        metrics = _metric_rows(lane, graded, "official_saves", "prob_over", "grading_status")
    graded["research_population"] = "RESEARCH_ONLY_CORROBORATED_PREGAME"
    graded["research_slate_date"] = slate
    return graded, metrics


def run_evaluation(output_root: Path = DEFAULT_OUT,
                   slates: tuple[str, ...] = ("2026-10-14", "2026-10-17", "2026-10-18")) -> Path:
    contract_path = ROOT / "backend/nhl/sog_cold_start/frozen_contract_v1.json"
    current_contract_hash = sha256_file(contract_path)
    summary: dict[str, Any] = {
        "schema_version": "NHL_PREDICTION_EVIDENCE_RESEARCH_V1",
        "evaluation_population": "RESEARCH_ONLY_CORROBORATED_PREGAME",
        "not_governed_reconciliation": True,
        "promotion_eligible": False,
        "betting_edge_claims_allowed": False,
        "outcome_independent_classification": True,
        "source_replay": {}, "slates": {},
        "limitations": ["No source-time receipt binds historical package hashes unless explicitly stated.",
                        "Research classifications do not establish reconciliation-first compliance.",
                        "Replay was available for the retained SOG, Points, and Saves packages listed in source_replay; it does not prove the historical inputs were available at their asserted cutoff."],
    }
    index_rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    grade_outputs: dict[str, list[pd.DataFrame]] = {lane: [] for lane in ["SOG", "POINTS", "SAVES"]}
    metrics: list[dict[str, Any]] = []
    finality: dict[str, Any] = {}
    legacy_hashes_before: dict[str, str] = {}
    legacy_grade_paths: dict[tuple[str, str], Path] = {}
    artifact_inputs: dict[str, str] = {"sog_contract": current_contract_hash}
    artifact_inputs["research_evaluator_sha256"] = sha256_file(Path(__file__).resolve())
    artifact_inputs["research_evidence_module_sha256"] = sha256_file(
        ROOT / "backend/nhl/research/prediction_evidence.py")

    for slate in slates:
        reconciliation, finality[slate], schedule, skaters, goalies = _verify_final_outcomes(slate)
        if reconciliation:
            for lane, filename in [("SOG", "graded_sog.csv"), ("POINTS", "graded_points.csv"),
                                   ("SAVES", "graded_saves.csv")]:
                path = reconciliation / filename
                if path.is_file():
                    legacy_grade_paths[(slate, lane)] = path
                    legacy_hashes_before[str(path)] = sha256_file(path)
        fallback_schedule = None
        if not reconciliation:
            morning = OPS / f"morning/{slate}/runs"
            candidates = sorted(morning.glob("*/canonical_game_spine.csv")) if morning.exists() else []
            fallback_schedule = candidates[-1] if candidates else None
        starts = _schedule_starts(slate, reconciliation, fallback_schedule)
        summary["slates"][slate] = {"outcomes": finality[slate], "canonical_games": len(starts),
                                    "lanes": {}}
        per_lane_selected: dict[str, list[dict[str, Any]]] = {}
        for lane in ["SOG", "POINTS", "SAVES"]:
            candidates, detail, replays = _discover_lane(lane, slate, starts,
                                                        current_contract_hash if lane == "SOG" else None)
            summary["source_replay"].update(replays)
            index_rows.extend(detail)
            for record in candidates:
                artifact_inputs[record["source_path"] + ":manifest"] = str(record.get("source_manifest_sha256"))
                artifact_inputs[record["source_prediction_file"]] = str(record.get("source_sha256"))
            selected, selection_exclusions = select_latest_groups(candidates)
            per_lane_selected[lane] = selected
            selected_rows.extend({**row, "research_population": "RESEARCH_ONLY_CORROBORATED_PREGAME"}
                                 for row in selected)
            lane_summary = {
                "candidate_atomic_groups": len(candidates),
                "classification_counts": pd.Series([r["classification"] for r in candidates]).value_counts().to_dict(),
                "selected_atomic_groups": len(selected),
                "selected_games": len({int(row["game_id"]) for row in selected}),
                "selection_exclusions": len(selection_exclusions),
                "research_grade_status": "NOT_RUN_NO_CERTIFIED_FINAL_OUTCOMES" if not reconciliation else "READY",
            }
            if reconciliation and selected:
                grade, lane_metrics = _grade_lane(lane, selected, candidates, reconciliation,
                                                  schedule, skaters, goalies, slate)
                grade_outputs[lane].append(grade)
                metrics.extend(lane_metrics)
                state_col = "grading_state" if "grading_state" in grade else "grading_status"
                lane_summary.update({"graded_rows": int(grade[state_col].isin(
                    ["WIN", "LOSS", "PUSH", "SETTLED", "REGULAR_SEASON_GRADED"]).sum()),
                    "prediction_rows": int(len(grade)),
                    "unique_game_entity_line": int(grade[["game_id", "player_id" if lane != "SAVES" else "goalie_id", "line"]].drop_duplicates().shape[0])})
                legacy_path = legacy_grade_paths.get((slate, lane))
                key_columns = ({"SOG": ["game_id", "player_id", "contract_arm", "line"],
                                "POINTS": ["game_id", "player_id", "model_version", "line"],
                                "SAVES": ["game_id", "goalie_id", "model_version", "line"]}[lane])
                legacy = pd.read_csv(legacy_path) if legacy_path else pd.DataFrame()
                research_ids = _identity_set(grade, key_columns)
                legacy_ids = _identity_set(legacy, key_columns)
                lane_summary.update({
                    "legacy_unique_predictions": len(legacy_ids),
                    "research_unique_predictions": len(research_ids),
                    "research_only_beyond_legacy": len(research_ids - legacy_ids),
                    "governed_resilient_prior_selected_groups": (
                        0 if slate in {"2026-10-14", "2026-10-17"} else None),
                    "research_groups_beyond_prior_governed_resilient": (
                        len(selected) if slate in {"2026-10-14", "2026-10-17"} else None),
                })
            summary["slates"][slate]["lanes"][lane] = lane_summary

    # Anchor deterministic output identity to all source packages and outcome
    # packages. Never overwrite an earlier research result.
    out_id = _digest({"inputs": artifact_inputs, "slates": slates,
                      "evaluator_sha256": sha256_file(Path(__file__)),
                      "contract": "NHL_PREDICTION_EVIDENCE_RESEARCH_V1"})[:16]
    out = output_root / f"evaluation={out_id}"
    out.mkdir(parents=True, exist_ok=False)
    pd.DataFrame(index_rows).to_csv(out / "evidence_index.csv", index=False)
    pd.DataFrame(selected_rows).to_csv(out / "selected_research_groups.csv", index=False)
    pd.DataFrame(metrics).to_csv(out / "research_metrics.csv", index=False)
    output_hashes = {}
    for lane, frames in grade_outputs.items():
        if frames:
            graded = pd.concat(frames, ignore_index=True, sort=False)
            target = out / f"research_graded_{lane.lower()}.csv"
            graded.to_csv(target, index=False)
            output_hashes[target.name] = sha256_file(target)
    summary["output_hashes"] = output_hashes
    summary["output_directory"] = str(out)
    summary["input_set_sha256"] = _digest(artifact_inputs)
    summary["governed_legacy_outputs_modified"] = False
    summary["legacy_grade_file_sha256_before"] = legacy_hashes_before
    summary["legacy_grade_file_sha256_after"] = {
        path: sha256_file(Path(path)) for path in legacy_hashes_before
    }
    summary["legacy_grade_hashes_unchanged"] = (
        summary["legacy_grade_file_sha256_before"] == summary["legacy_grade_file_sha256_after"])
    (out / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--slates", nargs="+", default=["2026-10-14", "2026-10-17", "2026-10-18"])
    args = parser.parse_args()
    output = run_evaluation(args.output_root, tuple(args.slates))
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
