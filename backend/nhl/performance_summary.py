"""Compact descriptive summaries over governed NHL postgame grade artifacts."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from backend.nhl.sog_attachment_integrity import verify_sog_integrity_package


SCHEMA_VERSION = "NHL_DAILY_PERFORMANCE_SUMMARY_V6"
_WIN = "WIN"
_LOSS = "LOSS"
_PUSH = "PUSH"


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_value(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _unique_values(frame: pd.DataFrame, columns: tuple[str, ...]) -> dict[str, Any]:
    identity: dict[str, Any] = {}
    for column in columns:
        if column not in frame:
            continue
        values = sorted({str(value) for value in frame[column].dropna().tolist()})
        if values:
            identity[column] = values[0] if len(values) == 1 else values
    return identity


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(pd.NA, index=frame.index, dtype="boolean")
    values = frame[column]
    if values.dtype == object:
        values = values.map(lambda value: value if pd.isna(value) or isinstance(value, bool)
                            else str(value).strip().lower() in {"true", "1", "yes"})
    return values.astype("boolean")


def _probability_context(frame: pd.DataFrame, correct: pd.Series,
                         probability: pd.Series) -> dict[str, Any]:
    probability = pd.to_numeric(probability, errors="coerce")
    values: dict[str, Any] = {
        "average_predicted_probability_overall": _json_value(probability.mean()),
        "average_predicted_probability_for_wins": _json_value(probability.loc[correct.eq(True)].mean()),
        "average_predicted_probability_for_losses": _json_value(probability.loc[correct.eq(False)].mean()),
    }
    return values


def _game_summary(frame: pd.DataFrame, *, lane: str, model_name: str,
                  challenger: bool = False) -> dict[str, Any]:
    if frame.empty:
        return {"model_identity": {"name": model_name}, "graded": 0, "correct": 0,
                "incorrect": 0, "pushes": 0, "pushes_applicable": False,
                "accuracy": None, "unresolved": 0}
    if challenger:
        eligible = (frame.get("evaluation_status", pd.Series("", index=frame.index))
                    .astype("string").eq("REGULAR_SEASON_GRADED"))
        model_columns = ("model_name", "model_version", "model_family", "control_name")
    else:
        eligible = (frame.get("grading_status", pd.Series("", index=frame.index))
                    .astype("string").eq("REGULAR_SEASON_GRADED"))
        model_columns = ("model_family", "model_version", "control_name", "control_artifact_sha256",
                         "control_model_sha256")
    correct_flag = _bool_series(frame, "correct" if challenger else "prediction_correct")
    evaluated = eligible & correct_flag.notna()
    correct = int((evaluated & correct_flag.fillna(False)).sum())
    incorrect = int((evaluated & ~correct_flag.fillna(False)).sum())
    unresolved = int((~evaluated & eligible).sum())
    non_evaluation = int((~eligible).sum())
    result: dict[str, Any] = {
        "model_identity": {"name": model_name, **_unique_values(frame, model_columns)},
        "graded": correct + incorrect,
        "correct": correct,
        "incorrect": incorrect,
        "pushes": 0,
        "pushes_applicable": False,
        "accuracy": correct / (correct + incorrect) if correct + incorrect else None,
        "unresolved": unresolved,
        "non_evaluation": non_evaluation,
    }
    probability_column = "predicted_win_probability" if challenger else (
        "v2_home_win_probability" if lane == "moneyline" else None)
    if lane == "moneyline" and probability_column in frame:
        if challenger:
            probability = pd.to_numeric(frame[probability_column], errors="coerce")
        else:
            home_prob = pd.to_numeric(frame[probability_column], errors="coerce")
            home = frame.get("home_team", pd.Series("", index=frame.index)).astype(str)
            favored = frame.get("model_favored_team", pd.Series("", index=frame.index)).astype(str)
            probability = home_prob.where(home.eq(favored), 1 - home_prob)
        result["probability_context"] = _probability_context(frame, correct_flag, probability)
    if lane == "puck_line":
        if "actual_margin_class" in frame:
            result["realized_margin_class_counts"] = {
                str(key): int(value) for key, value in
                frame.loc[eligible, "actual_margin_class"].dropna().value_counts().items()
            }
        result["correct_realized_margin_class"] = correct
    return result


def _prop_outcome(frame: pd.DataFrame, *, lane: str) -> tuple[pd.Series, ...]:
    if lane == "sog":
        state = frame.get("grading_state", pd.Series("UNRESOLVED_UNGRADED", index=frame.index)).astype(str)
        side = frame.get("selected_side", pd.Series("", index=frame.index)).astype("string").str.upper()
        p_over = pd.to_numeric(frame.get("p_over", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        p_under = pd.to_numeric(frame.get("p_under", pd.Series(pd.NA, index=frame.index)), errors="coerce")
    else:
        state = frame.get("grading_status", pd.Series("OUTCOME_UNRESOLVED", index=frame.index)).astype(str)
        side = frame.get("model_side", pd.Series("", index=frame.index)).astype("string").str.upper()
        prob_over_col = "prob_over" if lane == "points" else "prob_over"
        p_over = pd.to_numeric(frame.get(prob_over_col, pd.Series(pd.NA, index=frame.index)), errors="coerce")
        if lane == "saves" and "prob_over" not in frame:
            p_over = pd.to_numeric(frame.get("raw_prob_over", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        p_under = 1 - p_over
    wins = state.eq("WIN") if lane == "sog" else state.eq("SETTLED") & _bool_series(frame, "prediction_correct").fillna(False)
    losses = state.eq("LOSS") if lane == "sog" else state.eq("SETTLED") & ~_bool_series(frame, "prediction_correct").fillna(False)
    pushes = state.eq("PUSH")
    probability = p_over.where(side.eq("OVER"), p_under.where(side.eq("UNDER")))
    return wins, losses, pushes, state, side, probability


def _count_rows(frame: pd.DataFrame, mask: pd.Series) -> int:
    return int(mask.fillna(False).sum())


def _prop_counts(frame: pd.DataFrame, mask: pd.Series, *, lane: str,
                 status: pd.Series, wins: pd.Series, losses: pd.Series,
                 pushes: pd.Series) -> dict[str, Any]:
    selected = mask.fillna(False)
    w, l, p = (_count_rows(frame, selected & part) for part in (wins, losses, pushes))
    decided = w + l
    return {
        "settled": w + l + p, "wins": w, "losses": l, "pushes": p,
        "unresolved": _count_rows(frame, selected & ~status.isin(
            ["WIN", "LOSS", "PUSH"] if lane == "sog" else ["SETTLED", "PUSH",
                "DID_NOT_START_NOT_GRADEABLE_CONDITIONAL", "NONSTARTER_EXCLUDED_FROM_CONDITIONAL_EVALUATION"])),
        "win_rate": w / decided if decided else None,
    }


def _grouped_props(frame: pd.DataFrame, *, lane: str) -> dict[str, Any]:
    wins, losses, pushes, status, side, probability = _prop_outcome(frame, lane=lane)
    all_rows = pd.Series(True, index=frame.index)
    overall = _prop_counts(frame, all_rows, lane=lane, status=status,
                           wins=wins, losses=losses, pushes=pushes)
    if lane == "saves":
        overall["confirmed_did_not_start_not_gradeable"] = int(status.isin([
            "DID_NOT_START_NOT_GRADEABLE_CONDITIONAL",
            "NONSTARTER_EXCLUDED_FROM_CONDITIONAL_EVALUATION"]).sum())
        overall["starter_status_unresolved"] = int(status.eq("STARTER_STATUS_UNRESOLVED").sum())
        overall["not_gradeable"] = overall["confirmed_did_not_start_not_gradeable"]
    elif lane == "points":
        overall["not_gradeable"] = int(status.isin([
            "NONPARTICIPANT_UNGRADED", "PARTICIPATION_STATUS_UNRESOLVED"]).sum())
    side_stats = {}
    for side_name in ("OVER", "UNDER"):
        side_stats[side_name] = _prop_counts(
            frame, side.eq(side_name), lane=lane, status=status,
            wins=wins, losses=losses, pushes=pushes)
    by_line: dict[str, Any] = {}
    if "line" in frame:
        lines = pd.to_numeric(frame.line, errors="coerce")
        for value in sorted(lines.dropna().unique().tolist()):
            mask = lines.eq(value)
            by_line[f"{value:g}"] = _prop_counts(
                frame, mask, lane=lane, status=status,
                wins=wins, losses=losses, pushes=pushes)
            if lane == "saves":
                by_line[f"{value:g}"].update({
                    "not_gradeable": int((mask & status.isin([
                        "DID_NOT_START_NOT_GRADEABLE_CONDITIONAL",
                        "NONSTARTER_EXCLUDED_FROM_CONDITIONAL_EVALUATION"])).sum()),
                    "unresolved": int((mask & status.eq("STARTER_STATUS_UNRESOLVED")).sum()),
                })
    correct = wins
    result: dict[str, Any] = {
        "overall": overall,
        "by_side": side_stats,
        "by_line": by_line,
        "probability_context": _probability_context(frame, correct, probability),
        "win_rate_denominator": "wins_plus_losses; pushes excluded",
    }
    return result


def _model_groups(frame: pd.DataFrame, *, lane: str) -> dict[str, Any]:
    if lane != "sog":
        return {"reference": {
            "model_identity": _unique_values(frame, ("model", "model_version", "model_family", "control_name")),
            **_grouped_props(frame, lane=lane),
        }}
    arm_col = "contract_arm" if "contract_arm" in frame else "evaluation_lane"
    groups: dict[str, Any] = {}
    group_items = frame.groupby(arm_col, dropna=False, sort=True) if arm_col in frame else [("REFERENCE", frame)]
    for key, group in group_items:
        name = str(key) if pd.notna(key) else "UNKNOWN"
        groups[name] = {
            "model_identity": {"contract_arm": name,
                               **_unique_values(group, ("model_family", "model_version", "contract_version", "contract_sha256"))},
            **_grouped_props(group, lane="sog"),
        }
    return groups


def _unscored_sog(package: Path) -> dict[str, Any]:
    path = package / "graded_sog_source_exclusions.csv"
    missing = package / "graded_sog_missing_predictions.csv"
    frame = pd.read_csv(path) if path.is_file() else pd.DataFrame()
    missing_frame = pd.read_csv(missing) if missing.is_file() else pd.DataFrame()
    reasons: dict[str, int] = {}
    for rows, column, label in (
        (frame, "exclusion_reason", "SOURCE_EXCLUSION"),
        (missing_frame, "reason", "MISSING_PREDICTION"),
    ):
        if column in rows:
            reasons.update({f"{label}:{key}": int(value) for key, value in
                            rows[column].dropna().astype(str).value_counts().items()})
    identity_columns = [column for column in ("game_id", "player_id")
                        if column in frame.columns or column in missing_frame.columns]
    identities: set[tuple[Any, ...]] = set()
    for rows in (frame, missing_frame):
        if identity_columns and set(identity_columns).issubset(rows.columns):
            identities.update(rows[identity_columns].dropna(how="all").itertuples(
                index=False, name=None))
    unscored_count = len(identities) if identity_columns else len(frame) + len(missing_frame)
    return {
        "unscored_identities": int(unscored_count),
        "source_excluded_identities": int(len(frame)),
        "outcome_without_prediction_identities": int(len(missing_frame)),
        "unscored_reason_counts": reasons,
    }


def _record_keys(frame: pd.DataFrame, columns: tuple[str, ...]) -> set[tuple[Any, ...]]:
    if not set(columns).issubset(frame.columns) or frame.duplicated(list(columns)).any():
        raise ValueError("PREDICTION_KEYS_MISSING_OR_DUPLICATED")
    result = set()
    for row in frame[list(columns)].itertuples(index=False, name=None):
        result.add(tuple(float(value) if column == "line" else int(value)
                         for column, value in zip(columns, row)))
    return result


def _verify_daily_receipt(receipt_path: Path, slate_date: str) -> tuple[dict[str, Any], str]:
    run_dir = receipt_path.parent
    receipt = json.loads(receipt_path.read_text())
    marker = json.loads((run_dir / "RUN_COMPLETE.json").read_text())
    if receipt.get("slate_date") != slate_date or marker.get("parent_daily_run_id") != receipt.get("parent_daily_run_id"):
        raise ValueError("DAILY_RECEIPT_SLATE_OR_RUN_MISMATCH")
    entries: dict[str, str] = {}
    for line in (run_dir / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        candidate = run_dir / name
        if Path(name).name != name or not candidate.is_file() or _sha(candidate) != digest:
            raise ValueError("DAILY_RECEIPT_PACKAGE_HASH_MISMATCH")
        entries[name] = digest
    if entries.get("parent_receipt.json") != _sha(receipt_path) or entries.get("RUN_COMPLETE.json") != _sha(run_dir / "RUN_COMPLETE.json"):
        raise ValueError("DAILY_RECEIPT_PACKAGE_INCOMPLETE")
    if marker.get("final_classification") not in {"READY", "READY_WITH_BOUNDED_LANE_WARNING"}:
        raise ValueError("DAILY_RECEIPT_NOT_READY")
    return receipt, _sha(run_dir / "SHA256SUMS")


def discover_daily_market_coverage(
    *, slate_date: str, grades: dict[str, pd.DataFrame],
    daily_run_root: Path, integrity_archive_root: Path,
) -> dict[str, dict[str, Any]]:
    """Bind retained attachment integrity counts to the exact graded key population."""
    result: dict[str, dict[str, Any]] = {}
    for lane in ("points", "saves", "sog"):
        grade = grades.get(lane, pd.DataFrame())
        result[lane] = {
            "status": "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE",
            "prediction_rows": int(len(grade)),
            "matched": None, "unmatched": None, "ambiguous": None,
        }
    receipt_paths = sorted(Path(daily_run_root).glob("run_id=*/parent_receipt.json"))
    candidates = []
    for path in receipt_paths:
        try:
            receipt, manifest_sha = _verify_daily_receipt(path, slate_date)
            candidates.append((str(receipt.get("ended_at_utc") or ""), path, receipt, manifest_sha))
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            continue
    candidates.sort(key=lambda item: item[0], reverse=True)

    # Prefer an immutable game-specific reconstruction when one is available.
    # It selects the latest strictly prestart snapshot independently per game.
    reconstruction_root = (Path(daily_run_root).parent
                           / "sog_market_coverage_reconstructions"
                           / "season=2026" / f"slate_date={slate_date}")
    for package in sorted(reconstruction_root.glob("reconstruction=*"), reverse=True):
        try:
            from backend.nhl.daily_capture import verify_package
            manifest_sha = verify_package(package)
            coverage_path = package / "coverage.json"
            report = json.loads(coverage_path.read_text())
            if (report.get("schema_version") != "NHL_SOG_GAME_SPECIFIC_PRESTART_RECONSTRUCTION_V1"
                    or report.get("classification") != "GAME_SPECIFIC_PRESTART_RECONSTRUCTION"
                    or report.get("status") != "PASS" or report.get("slate_date") != slate_date):
                continue
            if report.get("identity_inputs", {}).get("algorithm_sha256") != _sha(
                    Path(__file__).resolve().parents[1] / "scripts" / "reconstruct_nhl_sog_game_specific_coverage.py"):
                continue
            for source in report.get("source_snapshots", []):
                source_package = Path(source["package_path"])
                if verify_package(source_package) != source["package_manifest_sha256"]:
                    raise ValueError("SOG_RECONSTRUCTION_SOURCE_PACKAGE_MISMATCH")
                source_odds = Path(source["odds_observation_path"])
                if verify_package(source_odds) != source["odds_manifest_sha256"]:
                    raise ValueError("SOG_RECONSTRUCTION_SOURCE_ODDS_MISMATCH")
            counts = report["counts"]
            rows = int(counts["prestart_eligible_prediction_keys"])
            result["sog"] = {
                "status": "AVAILABLE_PARTIAL" if counts.get("poststart_excluded_prediction_keys", 0) else "AVAILABLE",
                "prediction_rows": rows,
                "eligible_prestart": rows,
                "matched": int(counts["prestart_matched"]),
                "unmatched": int(counts["prestart_unmatched"]),
                "ambiguous": int(counts["ambiguous"]),
                "poststart_excluded": int(counts["poststart_excluded_prediction_keys"]),
                "per_arm": report.get("per_arm", {}),
                "source_artifact": str(coverage_path.resolve()),
                "source_artifact_sha256": _sha(coverage_path),
                "integrity_package_manifest_sha256": manifest_sha,
                "prediction_artifact_sha256": report["identity_inputs"]["prediction_sha256"],
                "odds_observation_manifest_sha256": hashlib.sha256(json.dumps(
                    sorted(source["odds_manifest_sha256"] for source in report["source_snapshots"]),
                    separators=(",", ":")).encode()).hexdigest(),
                "daily_receipt_manifest_sha256": None,
                "reconstructed_from_retained_evidence": True,
                "population_binding": "EXACT_GAME_PLAYER_PROP_LINE_KEYS_PER_ARM",
                "affects_grading_denominator": False,
            }
            break
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue

    for lane in ("points", "saves"):
        grade = grades.get(lane, pd.DataFrame())
        for _, receipt_path, receipt, receipt_manifest_sha in candidates:
            run_id = str(receipt.get("parent_daily_run_id") or "")
            attach_lane = receipt.get("lanes", {}).get(f"{lane}_attachment", {})
            prediction_lane = receipt.get("lanes", {}).get(lane, {})
            if attach_lane.get("status") != "COMPLETE" or prediction_lane.get("status") != "COMPLETE":
                continue
            integrity_identity = next((item for item in attach_lane.get("outputs", [])
                                       if str(item.get("path", "")).endswith(
                                           f"{lane}_attachment_integrity.json")), None)
            prediction_identity = next((item for item in prediction_lane.get("outputs", [])
                                        if str(item.get("path", "")).endswith("_predictions.csv")), None)
            if not integrity_identity or not prediction_identity:
                continue
            archive_path = Path(integrity_archive_root) / slate_date / Path(integrity_identity["path"]).name
            prediction_path = Path(prediction_identity.get("path", ""))
            try:
                if (_sha(archive_path) != integrity_identity.get("sha256")
                        or _sha(prediction_path) != prediction_identity.get("sha256")):
                    continue
                report = json.loads(archive_path.read_text())
                if report.get("status") != "PASS" or report.get("lane") != lane:
                    continue
                if report.get("parent_daily_run_id") != run_id:
                    continue
                if report.get("prediction_artifact_sha256") != prediction_identity.get("sha256"):
                    continue
                if str(Path(report.get("prediction_artifact_path", "")).resolve()) != str(prediction_path.resolve()):
                    continue
                odds_inputs = [item for item in attach_lane.get("inputs", [])
                               if item.get("manifest_sha256")]
                matching_odds = [item for item in odds_inputs
                                 if item.get("manifest_sha256") == report.get(
                                     "odds_observation_manifest_sha256")]
                if not matching_odds or not report.get("odds_observation_manifest_sha256"):
                    continue
                counts = report.get("counts", {})
                source_rows = int(prediction_identity.get("conditional_prediction_count")
                                  or prediction_identity.get("row_count") or -1)
                report_rows = int(counts.get("prediction_row_count", -1))
                if (report_rows != source_rows or int(counts.get("attachment_row_count", -1)) != source_rows
                        or counts.get("missing_prediction_key_count") != 0
                        or counts.get("extra_attachment_key_count") != 0
                        or counts.get("duplicate_prediction_key_count") != 0
                        or counts.get("duplicate_attachment_key_count") != 0
                        or counts.get("unique_prediction_key_count") != source_rows
                        or counts.get("unique_attachment_key_count") != source_rows):
                    continue
                if any(report.get("checks", {}).get(key) is not True for key in (
                    "prediction_keys_unique", "attachment_keys_unique",
                    "prediction_attachment_key_set_equal", "lineage_matches",
                    "output_count_equals_prediction_count", "statuses_exhaustive",
                )):
                    continue

                predictions = pd.read_csv(prediction_path)
                if ("game_date" in grade
                        and grade["game_date"].astype(str).ne(slate_date).any()):
                    continue
                if lane == "points":
                    if predictions.get("game_date", pd.Series(dtype=str)).astype(str).ne(slate_date).any():
                        continue
                    prediction_keys = _record_keys(predictions, ("player_id", "game_id", "line"))
                    grade_keys = _record_keys(grade, ("player_id", "game_id", "line"))
                else:
                    if predictions.get("game_date", pd.Series(dtype=str)).astype(str).ne(slate_date).any():
                        continue
                    columns = [column for column in predictions if re.fullmatch(r"p_over_\d+_\d+", column)]
                    if not columns:
                        continue
                    expanded_rows = []
                    for row in predictions.itertuples(index=False):
                        for column in columns:
                            if pd.notna(getattr(row, column)):
                                line = float(column.removeprefix("p_over_").replace("_", "."))
                                expanded_rows.append((int(row.player_id), int(row.game_id), line))
                    expanded = pd.DataFrame(expanded_rows, columns=["goalie_id", "game_id", "line"])
                    prediction_keys = _record_keys(expanded, ("goalie_id", "game_id", "line"))
                    grade_keys = _record_keys(grade, ("goalie_id", "game_id", "line"))
                if prediction_keys != grade_keys or len(prediction_keys) != report_rows:
                    continue
                matched = int(counts["matched_count"])
                unmatched = int(counts["unmatched_count"])
                ambiguous = int(counts["ambiguous_count"])
                if matched + unmatched + ambiguous != report_rows:
                    continue
                result[lane] = {
                    "status": "AVAILABLE", "prediction_rows": report_rows,
                    "matched": matched, "unmatched": unmatched,
                    "ambiguous": ambiguous,
                    "match_rate": matched / report_rows if report_rows else None,
                    "source_artifact": str(archive_path.resolve()),
                    "source_artifact_sha256": integrity_identity["sha256"],
                    "daily_run_id": run_id,
                    "daily_receipt_manifest_sha256": receipt_manifest_sha,
                    "prediction_artifact_sha256": prediction_identity["sha256"],
                    "odds_observation_manifest_sha256": report[
                        "odds_observation_manifest_sha256"],
                    "population_binding": "EXACT_GAME_PLAYER_LINE_KEYS",
                }
                break
            except (OSError, ValueError, KeyError, TypeError, pd.errors.ParserError,
                    json.JSONDecodeError):
                continue
    sog_report_path: Path | None = None
    sog_receipt_manifest_sha: str | None = None
    for _, receipt_path, receipt, receipt_manifest_sha in candidates:
        lane = (receipt.get("lanes") or {}).get("sog_attachment") or {}
        if lane.get("status") != "COMPLETE":
            continue
        identity = next((item for item in lane.get("outputs", [])
                         if str(item.get("path", "")).endswith(
                             "sog_attachment_integrity.json")), None)
        if identity is None:
            continue
        candidate = Path(identity["path"])
        try:
            if _sha(candidate) != identity.get("sha256"):
                continue
            report, package_sha = verify_sog_integrity_package(candidate, slate_date)
            if report.get("parent_daily_run_id") != receipt.get("parent_daily_run_id"):
                continue
            sog_report_path = candidate
            sog_receipt_manifest_sha = receipt_manifest_sha
            break
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue
    if sog_report_path is None:
        reconstruction_root = (Path(daily_run_root).parent
                               / "sog_market_coverage_reconstructions"
                               / "season=2026" / f"slate_date={slate_date}")
        reconstruction_reports = sorted(
            reconstruction_root.glob("reconstruction=*/sog_attachment_integrity.json"),
            reverse=True,
        ) if reconstruction_root.exists() else []
        for candidate in reconstruction_reports:
            try:
                report, _ = verify_sog_integrity_package(candidate, slate_date)
                if not report.get("reconstructed_from_retained_evidence"):
                    continue
                source_receipt_path = Path(report["source_daily_receipt_path"])
                _, receipt_manifest_sha = _verify_daily_receipt(source_receipt_path, slate_date)
                if receipt_manifest_sha != report.get("source_daily_receipt_manifest_sha256"):
                    continue
                sog_report_path = candidate
                sog_receipt_manifest_sha = receipt_manifest_sha
                break
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                continue
    if sog_report_path is not None and result["sog"].get("status") == "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE":
        try:
            report, _ = verify_sog_integrity_package(sog_report_path, slate_date)
            counts = report["counts"]
            rows = int(counts["prediction_row_count"])
            matched = int(counts["matched_count"])
            unmatched = int(counts["unmatched_count"])
            ambiguous = int(counts["ambiguous_count"])
            if matched + unmatched + ambiguous != rows:
                raise ValueError("SOG_MARKET_COVERAGE_COUNT_MISMATCH")
            result["sog"] = {
                "status": "AVAILABLE", "prediction_rows": rows,
                "matched": matched, "unmatched": unmatched,
                "ambiguous": ambiguous,
                "match_rate": matched / rows if rows else None,
                "source_artifact": str(sog_report_path.resolve()),
                "source_artifact_sha256": _sha(sog_report_path),
                "integrity_package_manifest_sha256": verify_sog_integrity_package(
                    sog_report_path, slate_date)[1],
                "daily_run_id": report.get("parent_daily_run_id"),
                "daily_receipt_manifest_sha256": sog_receipt_manifest_sha,
                "prediction_artifact_sha256": report["prediction_artifact_sha256"],
                "odds_observation_manifest_sha256": report[
                    "odds_observation_manifest_sha256"],
                "population_binding": "EXACT_GAME_PLAYER_PROP_LINE_KEYS",
                "reconstructed_from_retained_evidence": bool(
                    report.get("reconstructed_from_retained_evidence")),
                "affects_grading_denominator": False,
            }
            timing = report.get("game_specific_prestart")
            if timing:
                result["sog"].update({
                    "status": "AVAILABLE_PARTIAL" if timing.get(
                        "poststart_ineligible_prediction_keys", 0) else "AVAILABLE",
                    "eligible_prestart": timing.get("prestart_eligible_prediction_keys", 0),
                    "matched": timing.get("prestart_matched", 0),
                    "unmatched": timing.get("prestart_unmatched", 0),
                    "poststart_excluded": timing.get("poststart_ineligible_prediction_keys", 0),
                    "eligible_game_count": timing.get("eligible_game_count", 0),
                })
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            pass
    return result


def summarize_frames(*, slate_date: str, games: int, phase: str,
                     reconciliation_status: str, package_identity: str,
                     grades: dict[str, pd.DataFrame], source_artifacts: dict[str, str],
                     generated_at_utc: str | None = None,
                     challengers: dict[str, pd.DataFrame] | None = None,
                     market_coverage: dict[str, dict[str, Any]] | None = None,
                     package: Path | None = None) -> dict[str, Any]:
    challengers = challengers or {}
    moneyline = grades.get("moneyline", pd.DataFrame())
    puck_line = grades.get("puck_line", pd.DataFrame())
    sog = grades.get("sog", pd.DataFrame())
    points = grades.get("points", pd.DataFrame())
    saves = grades.get("saves", pd.DataFrame())
    models = {
        "moneyline": {"reference": _game_summary(
            moneyline, lane="moneyline", model_name="moneyline_reference")},
        "puck_line": {"reference": _game_summary(
            puck_line, lane="puck_line", model_name="puck_line_reference")},
        "sog": _model_groups(sog, lane="sog"),
        "points": _model_groups(points, lane="points"),
        "saves": _model_groups(saves, lane="saves"),
    }
    if "moneyline" in challengers and not challengers["moneyline"].empty:
        models["moneyline"]["challenger"] = _game_summary(
            challengers["moneyline"], lane="moneyline",
            model_name="moneyline_shot_finishing_challenger_v3", challenger=True)
    if "puck_line" in challengers and not challengers["puck_line"].empty:
        models["puck_line"]["challenger"] = _game_summary(
            challengers["puck_line"], lane="puck_line",
            model_name="puck_line_v2_shot_prior_challenger", challenger=True)
    if package is not None:
        models["sog"]["unscored"] = _unscored_sog(package)
    market_coverage = market_coverage or {}
    for lane, frame in (("points", points), ("saves", saves), ("sog", sog)):
        models[lane]["market_coverage"] = dict(market_coverage.get(lane) or {
            "status": "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE",
            "prediction_rows": int(len(frame)),
            "matched": None, "unmatched": None, "ambiguous": None,
        })
        models[lane]["market_coverage"]["affects_grading_denominator"] = False
        if lane == "sog" and models[lane]["market_coverage"].get("per_arm"):
            models[lane]["arm_market_coverage"] = dict(
                models[lane]["market_coverage"]["per_arm"])
    models["points"]["realized_points_definition"] = "official_goals + official_assists"
    unresolved = {
        "moneyline": models["moneyline"]["reference"].get("unresolved", 0),
        "puck_line": models["puck_line"]["reference"].get("unresolved", 0),
        "sog": {key: value["overall"].get("unresolved", 0)
                for key, value in models["sog"].items() if isinstance(value, dict) and "overall" in value},
        "points": models["points"]["reference"]["overall"].get("unresolved", 0),
        "saves": models["saves"]["reference"]["overall"].get("unresolved", 0),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "slate_date": slate_date,
        "canonical_phase": phase,
        "game_count": int(games),
        "reconciliation_status": reconciliation_status,
        "official_outcomes_status": "FINAL",
        "reconciliation_package": package_identity,
        "generated_at_utc": generated_at_utc or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "models": models,
        "unresolved_summary": unresolved,
        "source_artifacts": dict(sorted(source_artifacts.items())),
    }


def render_markdown(summary: dict[str, Any]) -> str:
    def rate(value: Any) -> str:
        return "n/a" if value is None else f"{100 * float(value):.1f}%"

    def game_line(value: dict[str, Any]) -> str:
        return (f"{value['correct']}-{value['incorrect']} | {rate(value.get('accuracy'))}"
                f"; graded {value['graded']}; unresolved {value['unresolved']}")

    def prop_body(display: dict[str, Any], title: str) -> list[str]:
        lines: list[str] = []
        overall = display.get("overall", {})
        lines.append(
            f"Overall: {overall.get('wins', 0)}-{overall.get('losses', 0)}-"
            f"{overall.get('pushes', 0)} | {rate(overall.get('win_rate'))}; "
            f"settled {overall.get('settled', 0)}; unresolved {overall.get('unresolved', 0)}"
        )
        if title == "Saves":
            lines.append(f"Did not start: {overall.get('confirmed_did_not_start_not_gradeable', 0)}; "
                         f"starter unresolved: {overall.get('starter_status_unresolved', 0)}")
        side = display.get("by_side", {})
        if side:
            lines.append("By side: " + "; ".join(
                f"{name} {v.get('wins', 0)}-{v.get('losses', 0)}-{v.get('pushes', 0)}"
                for name, v in side.items()))
        by_line = display.get("by_line", {})
        if by_line:
            lines += ["", "By line:", "| Line | Settled | W | L | P | Unresolved | Win % |",
                      "|---:|---:|---:|---:|---:|---:|---:|"]
            for line, values in by_line.items():
                lines.append(f"| {line} | {values.get('settled', 0)} | {values.get('wins', 0)} | "
                             f"{values.get('losses', 0)} | {values.get('pushes', 0)} | "
                             f"{values.get('unresolved', 0)} | {rate(values.get('win_rate'))} |")
        return lines

    def prop_section(title: str, models: dict[str, Any]) -> list[str]:
        lines = [f"## {title}"]
        if title == "SOG":
            for name, display in models.items():
                if not isinstance(display, dict) or "overall" not in display:
                    continue
                lines.append(f"{name}: " + prop_body(display, title)[0])
                lines.extend(prop_body(display, title)[1:])
            return lines
        lines.extend(prop_body(models.get("reference", {}), title))
        if title == "Points":
            lines.append("Realized total: official goals + official assists.")
        return lines

    models = summary["models"]
    lines = [f"# NHL Daily Performance — {summary['slate_date']}", "",
             f"Slate: {summary['game_count']} games; {summary['canonical_phase']}; "
             f"reconciliation {summary['reconciliation_status']}; "
             f"official outcomes {summary['official_outcomes_status']}", "",
             "## Moneyline",
             "Reference: " + game_line(models["moneyline"]["reference"])]
    if "challenger" in models["moneyline"]:
        lines.append("Challenger: " + game_line(models["moneyline"]["challenger"]))
    lines += ["", "## Puck Line",
              "Reference: " + game_line(models["puck_line"]["reference"])]
    if "challenger" in models["puck_line"]:
        lines.append("Challenger: " + game_line(models["puck_line"]["challenger"]))
    lines += ["", *prop_section("SOG", models["sog"]), ""]
    unscored = models["sog"].get("unscored", {})
    if unscored:
        lines.append(f"Unscored identities: {unscored.get('unscored_identities', 0)}")
    lines += ["", *prop_section("Points", models["points"]), "",
              *prop_section("Saves", models["saves"]), "", "## Notes",
              "- Win rate excludes pushes; pushes are reported separately.",
              "- Market match status does not determine whether a prediction is graded.",
              "- No selection policy or promotion rule was applied."]
    coverage_rows = []
    for lane in ("points", "saves", "sog"):
        context = models.get(lane, {}).get("market_coverage", {})
        if context.get("status") in {"AVAILABLE", "AVAILABLE_PARTIAL"}:
            coverage_size = (context.get("eligible_prestart", context["prediction_rows"])
                             if lane == "sog" and context.get("poststart_excluded") is not None
                             else context["prediction_rows"])
            coverage_rows.append(
                f"{lane.upper() if lane == 'sog' else lane.title()}: {context['matched']} matched; "
                f"{context['unmatched']} unmatched; "
                f"{coverage_size} {'prestart eligible' if lane == 'sog' and context.get('poststart_excluded') is not None else 'total'}"
            )
            if lane == "sog" and context.get("poststart_excluded") is not None:
                coverage_rows[-1] += f"; {context['poststart_excluded']} poststart excluded"
                for arm, values in sorted(context.get("per_arm", {}).items()):
                    coverage_rows.append(
                        f"  {arm}: {values['matched']} matched; {values['unmatched']} unmatched; "
                        f"{values['eligible_prestart']} prestart eligible; "
                        f"{values['poststart_excluded']} poststart excluded; "
                        f"{values.get('not_in_operational_population', 0)} outside operational population"
                    )
        else:
            coverage_rows.append(f"{lane.upper() if lane == 'sog' else lane.title()}: "
                                 "retained bound coverage evidence unavailable")
    lines[-4:-4] = ["", "## Market Coverage Context", *coverage_rows,
                    "Quote matching is descriptive and does not affect grading."]
    return "\n".join(lines).rstrip() + "\n"


def _write_immutable_package(destination: Path, summary: dict[str, Any]) -> tuple[Path, Path]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    json_path = destination / "performance_summary.json"
    markdown_path = destination / "performance_summary.md"
    if destination.exists():
        verify_summary_package(destination)
        return json_path, markdown_path
    staging = destination.with_name(f".{destination.name}.{os.getpid()}.incomplete")
    staging.mkdir(parents=True, exist_ok=False)
    try:
        json_path_staging = staging / json_path.name
        md_path_staging = staging / markdown_path.name
        json_path_staging.write_text(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n")
        md_path_staging.write_text(render_markdown(summary))
        sums = staging / "SHA256SUMS"
        sums.write_text(f"{_sha(json_path_staging)}  {json_path_staging.name}\n"
                        f"{_sha(md_path_staging)}  {md_path_staging.name}\n")
        os.replace(staging, destination)
    except BaseException:
        import shutil
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return json_path, markdown_path


def verify_summary_package(path: Path) -> None:
    path = Path(path)
    entries = {}
    for line in (path / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        candidate = path / name
        if Path(name).name != name or not candidate.is_file() or _sha(candidate) != digest:
            raise RuntimeError("NHL_PERFORMANCE_SUMMARY_PACKAGE_HASH_MISMATCH")
        entries[name] = digest
    if not {"performance_summary.json", "performance_summary.md"}.issubset(entries):
        raise RuntimeError("NHL_PERFORMANCE_SUMMARY_PACKAGE_INCOMPLETE")


def generate_from_artifacts(*, package: Path, restatement: Path,
                            reconciliation_status: str,
                            challengers: dict[str, pd.DataFrame] | None = None,
                            challenger_source_artifacts: dict[str, Path] | None = None,
                            market_coverage: dict[str, dict[str, Any]] | None = None,
                            output_root: Path | None = None) -> tuple[Path, Path, dict[str, Any]]:
    """Build/reuse a summary package from retained reconciliation grade rows."""
    package = Path(package).resolve()
    restatement = Path(restatement).resolve()
    source_summary = json.loads((package / "summary.json").read_text())
    source_marker = json.loads((package / "RUN_COMPLETE.json").read_text())
    grade_paths = {
        "moneyline": restatement / "graded_moneyline.csv",
        "puck_line": restatement / "graded_puck_line.csv",
        "sog": package / "graded_sog.csv",
        "points": restatement / "graded_points.csv",
        "saves": restatement / "graded_saves.csv",
    }
    grades = {key: pd.read_csv(path) for key, path in grade_paths.items() if path.is_file()}
    source_hashes = {
        "reconciliation_manifest_sha256": _sha(package / "SHA256SUMS"),
        "reconciliation_summary_sha256": _sha(package / "summary.json"),
        "canonical_game_outcomes_sha256": _sha(package / "canonical_game_outcomes.csv"),
        "phase_restatement_lineage_sha256": _sha(restatement / "lineage.json"),
        **{f"{key}_grade_sha256": _sha(path) for key, path in grade_paths.items() if path.is_file()},
    }
    for lane, coverage in (market_coverage or {}).items():
        if coverage.get("status") in {"AVAILABLE", "AVAILABLE_PARTIAL"}:
            source_hashes[f"{lane}_attachment_integrity_sha256"] = str(
                coverage["source_artifact_sha256"])
            if coverage.get("daily_receipt_manifest_sha256"):
                source_hashes[f"{lane}_daily_receipt_manifest_sha256"] = str(
                    coverage["daily_receipt_manifest_sha256"])
            source_hashes[f"{lane}_attached_prediction_artifact_sha256"] = str(
                coverage["prediction_artifact_sha256"])
            source_hashes[f"{lane}_odds_observation_manifest_sha256"] = str(
                coverage["odds_observation_manifest_sha256"])
            if coverage.get("integrity_package_manifest_sha256"):
                source_hashes[f"{lane}_integrity_package_manifest_sha256"] = str(
                    coverage["integrity_package_manifest_sha256"])
    for name in ("graded_sog_source_exclusions.csv", "graded_sog_missing_predictions.csv"):
        path = package / name
        if path.is_file():
            source_hashes[f"{name}_sha256"] = _sha(path)
    if challengers:
        challenger_source_artifacts = challenger_source_artifacts or {}
        for lane, frame in challengers.items():
            candidate = challenger_source_artifacts.get(lane)
            if candidate is not None and Path(candidate).is_file():
                source_hashes[f"{lane}_challenger_grade_sha256"] = _sha(Path(candidate))
                continue
            source_hashes[f"{lane}_challenger_rows_sha256"] = hashlib.sha256(
                frame.to_csv(index=False, lineterminator="\n").encode()).hexdigest()
    identity_payload = {
        "schema_version": SCHEMA_VERSION,
        "slate_date": source_summary["slate_date"],
        "package_identity": source_summary.get("substantive_identity"),
        "source_artifacts": source_hashes,
    }
    identity = hashlib.sha256(json.dumps(identity_payload, sort_keys=True,
                                         separators=(",", ":")).encode()).hexdigest()
    root = Path(output_root) if output_root else package.parent.parent / "learning_restatements" / package.parent.name
    destination = root / f"performance_summary={identity[:20]}"
    admitted_path = package / "canonical_admitted_slate.csv"
    phase_source = admitted_path if admitted_path.is_file() else package / "canonical_game_outcomes.csv"
    phase_values = pd.read_csv(phase_source).get("game_type_code", pd.Series(dtype="int64"))
    phase = ("REGULAR_SEASON" if len(phase_values) and phase_values.astype(int).eq(2).all()
             else "PRESEASON" if len(phase_values) and phase_values.astype(int).eq(1).all()
             else "MIXED_OR_UNRESOLVED")
    summary = summarize_frames(
        slate_date=str(source_summary["slate_date"]),
        games=int(source_summary.get("games", 0)), phase=phase,
        reconciliation_status=reconciliation_status,
        package_identity=str(source_summary.get("substantive_identity") or package.name),
        grades=grades, source_artifacts=source_hashes,
        generated_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        challengers=challengers, market_coverage=market_coverage, package=package,
    )
    summary["official_outcomes_status"] = (
        "FINAL" if source_summary.get("status") == "COMPLETE"
        and source_marker.get("status") == "COMPLETE" else "INCOMPLETE")
    summary["summary_identity"] = identity
    summary["summary_package"] = str(destination.resolve())
    json_path, markdown_path = _write_immutable_package(destination, summary)
    summary = json.loads(json_path.read_text())
    return json_path.resolve(), markdown_path.resolve(), summary
