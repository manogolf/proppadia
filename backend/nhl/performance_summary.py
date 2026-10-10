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
from backend.nhl.sog_coverage_annotations import validate_annotation
from backend.nhl.daily_capture import verify_package
from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.sog_cold_start.core import grade_predictions as grade_sog_predictions


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
    selected_lines = pd.to_numeric(frame.loc[selected].get(
        "line", pd.Series(dtype="float64")), errors="coerce").dropna()
    applicability = any(float(value).is_integer() for value in selected_lines.tolist())
    impossible = selected & pushes & pd.to_numeric(
        frame.get("line", pd.Series(pd.NA, index=frame.index)), errors="coerce"
    ).map(lambda value: pd.notna(value) and not float(value).is_integer())
    impossible_count = _count_rows(frame, impossible)
    if impossible_count:
        lines = sorted({str(value) for value in frame.loc[impossible, "line"].dropna().tolist()})
        raise ValueError(
            "IMPOSSIBLE_PUSH_FOR_NON_PUSHABLE_CONTRACT:"
            f"market={lane};lines={','.join(lines)};push_count={impossible_count}"
        )
    settled = w + l + p
    if not applicability and settled != w + l:
        raise ValueError(
            "NONPUSHABLE_SETTLED_COUNT_MISMATCH:"
            f"market={lane};settled={settled};wins={w};losses={l};pushes={p}"
        )
    decided = w + l
    return {
        "settled": settled, "wins": w, "losses": l, "pushes": p,
        "pushes_applicable": applicability,
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
    pushable = bool(overall["pushes_applicable"])
    result: dict[str, Any] = {
        "overall": overall,
        "by_side": side_stats,
        "by_line": by_line,
        "probability_context": _probability_context(frame, correct, probability),
        "win_rate_denominator": (
            "wins_plus_losses; pushes excluded" if pushable else "wins_plus_losses"
        ),
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


def _assert_game_market_nonpushable(
    frame: pd.DataFrame, *, market: str, slate_date: str, grade_source: str,
) -> None:
    if frame.empty:
        return
    push = pd.Series(False, index=frame.index)
    for column in ("grading_status", "evaluation_status", "settlement_status",
                   "settled_side", "outcome", "result"):
        if column in frame:
            push |= frame[column].astype("string").str.upper().eq("PUSH").fillna(False)
    count = _count_rows(frame, push)
    if count:
        model_columns = ("model_name", "model_version", "model_family", "control_name")
        model_identity = ",".join(
            f"{column}={','.join(sorted(frame.loc[push, column].dropna().astype(str).unique()))}"
            for column in model_columns if column in frame and frame.loc[push, column].notna().any()
        ) or "model=unknown"
        raise ValueError(
            "IMPOSSIBLE_PUSH_FOR_NON_PUSHABLE_CONTRACT:"
            f"slate_date={slate_date};market={market};{model_identity};line=n/a;"
            f"push_count={count};grade_source={grade_source}"
        )
    eligible_column = "evaluation_status" if "evaluation_status" in frame else "grading_status"
    if eligible_column in frame:
        eligible = frame[eligible_column].astype("string").eq("REGULAR_SEASON_GRADED")
        correct_column = "correct" if eligible_column == "evaluation_status" else "prediction_correct"
        if correct_column in frame:
            settled = int((eligible & _bool_series(frame, correct_column).notna()).sum())
            wins = int((eligible & _bool_series(frame, correct_column).eq(True)).sum())
            losses = int((eligible & _bool_series(frame, correct_column).eq(False)).sum())
            if settled != wins + losses:
                raise ValueError(
                    "NONPUSHABLE_SETTLED_COUNT_MISMATCH:"
                    f"slate_date={slate_date};market={market};settled={settled};"
                    f"wins={wins};losses={losses};grade_source={grade_source}"
                )


def _assert_prop_push_integrity(
    frame: pd.DataFrame, *, market: str, slate_date: str, grade_source: str,
) -> None:
    if frame.empty:
        return
    if "line" not in frame:
        raise ValueError(f"PUSH_APPLICABILITY_LINE_MISSING:slate_date={slate_date};market={market}")
    _, _, pushes, _, _, _ = _prop_outcome(frame, lane=market)
    lines = pd.to_numeric(frame["line"], errors="coerce")
    missing_line_pushes = pushes & lines.isna()
    if missing_line_pushes.any():
        raise ValueError(
            "IMPOSSIBLE_PUSH_FOR_NON_PUSHABLE_CONTRACT:"
            f"slate_date={slate_date};market={market};line=UNKNOWN;"
            f"push_count={int(missing_line_pushes.sum())};grade_source={grade_source}"
        )
    impossible = pushes & lines.notna() & ~lines.map(float.is_integer)
    if impossible.any():
        context_columns = [column for column in ("contract_arm", "evaluation_lane", "model", "model_version")
                           if column in frame]
        for line, group in frame.loc[impossible].groupby(lines.loc[impossible]):
            model = ",".join(
                f"{column}={','.join(sorted(group[column].dropna().astype(str).unique()))}"
                for column in context_columns
            ) or "model=reference"
            raise ValueError(
                "IMPOSSIBLE_PUSH_FOR_NON_PUSHABLE_CONTRACT:"
                f"slate_date={slate_date};market={market};{model};line={line:g};"
                f"push_count={len(group)};grade_source={grade_source}"
            )


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


def build_production_sog_reference(
    *, slate_date: str, package: Path, daily_run_root: Path,
    market_coverage: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Grade the immutable baseline SOG predictions for a reconciled slate."""
    package = Path(package).resolve()
    daily_run_root = Path(daily_run_root).resolve()
    coverage = dict(market_coverage or {})
    run_id = str(coverage.get("daily_run_id") or "")
    if not run_id:
        candidates = []
        for receipt_path in daily_run_root.glob("run_id=*/parent_receipt.json"):
            try:
                receipt, receipt_manifest_sha = _verify_daily_receipt(receipt_path, slate_date)
                candidates.append((str(receipt.get("ended_at_utc") or ""), receipt_path,
                                  receipt, receipt_manifest_sha))
            except (OSError, ValueError, KeyError, json.JSONDecodeError):
                continue
        if not candidates:
            raise RuntimeError("PRODUCTION_SOG_DAILY_RECEIPT_NOT_FOUND")
        _, receipt_path, receipt, receipt_manifest_sha = sorted(candidates, reverse=True)[0]
        run_id = str(receipt.get("parent_daily_run_id") or "")
    else:
        receipt_path = daily_run_root / f"run_id={run_id}" / "parent_receipt.json"
        receipt, receipt_manifest_sha = _verify_daily_receipt(receipt_path, slate_date)
    if receipt.get("parent_daily_run_id") != run_id:
        raise ValueError("PRODUCTION_SOG_PARENT_RUN_ID_MISMATCH")
    lane = receipt.get("lanes", {}).get("legacy_sog", {})
    if lane.get("status") != "COMPLETE":
        raise ValueError("PRODUCTION_SOG_LANE_NOT_COMPLETE")
    evidence_items = [item for item in lane.get("outputs", [])
                      if item.get("fitted_model_evidence")]
    if len(evidence_items) != 1:
        raise ValueError("PRODUCTION_SOG_FITTED_EVIDENCE_CARDINALITY")
    evidence = evidence_items[0]["fitted_model_evidence"]
    if (evidence.get("model_family") != "poisson_baseline"
            or evidence.get("model_version") != "baseline_v1"
            or evidence.get("scoring_run_id") != run_id):
        raise ValueError("PRODUCTION_SOG_MODEL_IDENTITY_MISMATCH")
    scorer = next((item for item in evidence.get("component_artifacts", [])
                   if item.get("role") == "scorer"), None)
    expected_scorer_path = "backend/nhl/scripts/score_sog_poisson_baseline.py"
    if not scorer or scorer.get("canonical_artifact_path") != expected_scorer_path:
        raise ValueError("PRODUCTION_SOG_SCORER_IDENTITY_MISMATCH")
    prediction_path = Path(str(evidence.get("prediction_artifact_path", ""))).resolve()
    prediction_sha = str(evidence.get("prediction_artifact_sha256") or "")
    output_identity = next((item for item in lane.get("outputs", [])
                            if item.get("path") == str(prediction_path)), None)
    if (not prediction_path.is_file() or _sha(prediction_path) != prediction_sha
            or not output_identity or output_identity.get("sha256") != prediction_sha):
        raise ValueError("PRODUCTION_SOG_PREDICTION_ARTIFACT_BINDING_MISMATCH")
    feature_binding: dict[str, Any] = {}
    if lane.get("outputs") and output_identity.get("feature_input_sha256"):
        feature_path = Path(str(output_identity.get("feature_input_path") or "")).resolve()
        manifest_path = Path(str(output_identity.get("feature_input_manifest_path") or "")).resolve()
        manifest_sha = str(output_identity.get("feature_input_manifest_sha256") or "")
        if (not feature_path.is_file() or _sha(feature_path) != output_identity.get("feature_input_sha256")
                or not manifest_path.is_file() or _sha(manifest_path) != manifest_sha):
            raise ValueError("PRODUCTION_SOG_FEATURE_INPUT_ARTIFACT_BINDING_MISMATCH")
        verify_package(manifest_path.parent)
        feature_manifest = json.loads(manifest_path.read_text())
        if (feature_manifest.get("contract") != "NHL_SOG_PRODUCTION_FEATURE_INPUT_V1"
                or feature_manifest.get("parent_daily_run_id") != run_id
                or feature_manifest.get("feature_input_sha256") != output_identity.get("feature_input_sha256")
                or feature_manifest.get("prediction_artifact_sha256") != prediction_sha
                or feature_manifest.get("fitted_model_identity_sha256") != evidence.get("fitted_model_identity_sha256")
                or feature_manifest.get("canonical_game_set_hash") != receipt.get("canonical_game_set_hash")):
            raise ValueError("PRODUCTION_SOG_FEATURE_PREDICTION_LINEAGE_MISMATCH")
        feature_binding = {
            "source_daily_run_id": run_id,
            "feature_input_path": str(feature_path),
            "feature_input_sha256": output_identity["feature_input_sha256"],
            "feature_input_manifest_sha256": manifest_sha,
            "feature_contract": feature_manifest["contract"],
            "feature_cutoff_utc": feature_manifest["feature_cutoff_utc"],
        }
    predictions = pd.read_csv(prediction_path)
    if predictions.duplicated(["game_id", "player_id"]).any():
        raise ValueError("PRODUCTION_SOG_DUPLICATE_PLAYER_GAME_IDENTITY")
    canonical_ids = sorted(map(int, receipt.get("canonical_game_ids", [])))
    game_hash = canonical_game_set_hash(canonical_ids)
    if (game_hash != receipt.get("canonical_game_set_hash")
            or sorted(map(int, predictions.game_id.unique())) != canonical_ids
            or output_identity.get("canonical_game_set_hash") != game_hash):
        raise ValueError("PRODUCTION_SOG_CANONICAL_GAME_SET_MISMATCH")
    if len(predictions) != int(output_identity.get("natural_identity_count", len(predictions))):
        raise ValueError("PRODUCTION_SOG_PREDICTION_ROW_COUNT_MISMATCH")

    outcomes_path = package / "canonical_skater_outcomes.csv"
    outcomes = pd.read_csv(outcomes_path)
    required_outcomes = {"canonical_season", "slate_date", "game_id", "player_id", "official_final",
                         "official_sog", "participation_state", "outcome_source",
                         "outcome_source_timestamp_utc"}
    if not required_outcomes.issubset(outcomes.columns):
        raise ValueError("PRODUCTION_SOG_OFFICIAL_OUTCOME_SCHEMA_MISSING")
    if not outcomes.slate_date.astype(str).eq(slate_date).all():
        raise ValueError("PRODUCTION_SOG_OUTCOME_SLATE_MISMATCH")
    outcomes = outcomes.rename(columns={"participation_state": "participation_status"})
    outcomes["participation_status"] = outcomes.participation_status.replace({"PARTICIPATED": "APPEARED"})
    outcomes["canonical_season"] = pd.to_numeric(outcomes.canonical_season, errors="raise").astype(int)

    line_probabilities = ((1.5, "p_over_1_5"), (2.5, "p_over_2_5"), (3.5, "p_over_3_5"))
    long_rows = []
    for line, probability_column in line_probabilities:
        if probability_column not in predictions:
            raise ValueError("PRODUCTION_SOG_THRESHOLD_PROBABILITY_MISSING")
        rows = predictions[["game_id", "player_id", "expected_sog", probability_column]].copy()
        rows = rows.rename(columns={probability_column: "p_over"})
        rows["p_over"] = pd.to_numeric(rows.p_over, errors="coerce")
        if rows.p_over.isna().any() or (~rows.p_over.between(0, 1)).any():
            raise ValueError("PRODUCTION_SOG_INVALID_THRESHOLD_PROBABILITY")
        rows["p_under"] = 1.0 - rows.p_over
        rows["line"] = line
        rows["canonical_season"] = int(receipt.get("canonical_season"))
        rows["slate_date"] = slate_date
        rows["selected_side"] = rows.p_over.ge(0.5).map({True: "OVER", False: "UNDER"})
        long_rows.append(rows)
    graded = grade_sog_predictions(
        pd.concat(long_rows, ignore_index=True), outcomes,
        grading_timestamp_utc=datetime.now(timezone.utc).isoformat())
    display = _grouped_props(graded, lane="sog")
    total = display["overall"]
    if total["settled"] != total["wins"] + total["losses"] + total["pushes"]:
        raise ValueError("PRODUCTION_SOG_OVERALL_SETTLEMENT_TOTAL_MISMATCH")
    for key in ("wins", "losses", "settled", "unresolved"):
        if sum(values[key] for values in display["by_line"].values()) != total[key]:
            raise ValueError(f"PRODUCTION_SOG_LINE_TOTAL_MISMATCH:{key}")
        if sum(values[key] for values in display["by_side"].values()) != total[key]:
            raise ValueError(f"PRODUCTION_SOG_SIDE_TOTAL_MISMATCH:{key}")
    display["model_identity"] = {
        "name": "sog_reference", "model_family": "poisson_baseline",
        "model_version": "baseline_v1", "parent_daily_run_id": run_id,
        "fitted_model_identity_sha256": evidence.get("fitted_model_identity_sha256"),
        "prediction_artifact_sha256": prediction_sha,
        **feature_binding,
        "scorer": expected_scorer_path, "scorer_sha256": scorer.get("sha256"),
        "canonical_game_ids": canonical_ids, "canonical_game_set_hash": game_hash,
        "downstream_export_reference": True,
        "downstream_export": "bin/nhl_ops.sh eight-rain-export --latest-refresh",
        "side_selection": "OVER when p_over >= 0.5; otherwise UNDER",
    }

    prediction_keys = set(map(tuple, predictions[["game_id", "player_id"]].astype("int64").to_numpy()))
    outcome_keys = set(map(tuple, outcomes[["game_id", "player_id"]].astype("int64").to_numpy()))
    missing_prediction_keys = outcome_keys - prediction_keys
    prediction_without_outcome_keys = prediction_keys - outcome_keys
    missing_predictions_path = package / "graded_sog_missing_predictions.csv"
    missing_predictions_sha = None
    if missing_predictions_path.is_file():
        missing_predictions_sha = _sha(missing_predictions_path)
        missing_predictions = pd.read_csv(missing_predictions_path)
        if not {"game_id", "player_id", "reason"}.issubset(missing_predictions.columns):
            raise ValueError("PRODUCTION_SOG_MISSING_PREDICTIONS_SCHEMA_MISSING")
        package_missing_keys = set(map(tuple, missing_predictions[["game_id", "player_id"]]
                                       .astype("int64").to_numpy()))
        if package_missing_keys != missing_prediction_keys:
            raise ValueError("PRODUCTION_SOG_MISSING_PREDICTIONS_IDENTITY_MISMATCH")
    elif missing_prediction_keys:
        raise ValueError("PRODUCTION_SOG_MISSING_PREDICTIONS_ARTIFACT_MISSING")
    unscored_item = next((item for item in lane.get("outputs", [])
                          if str(item.get("path", "")).endswith("sog_predictions_unscored.csv")), None)
    unscored_rows = pd.DataFrame()
    unscored_sha = None
    if unscored_item:
        unscored_path = Path(str(unscored_item["path"])).resolve()
        unscored_sha = _sha(unscored_path)
        if unscored_sha != unscored_item.get("sha256"):
            raise ValueError("PRODUCTION_SOG_UNSCORED_ARTIFACT_HASH_MISMATCH")
        unscored_rows = pd.read_csv(unscored_path)
    reason_counts = ({str(key): int(value) for key, value in
                      unscored_rows.reason.fillna("UNSPECIFIED").value_counts().items()}
                     if "reason" in unscored_rows else {})
    if missing_prediction_keys:
        for reason, count in missing_predictions.reason.fillna("UNSPECIFIED").value_counts().items():
            key = f"MISSING_PREDICTION:{reason}"
            reason_counts[key] = reason_counts.get(key, 0) + int(count)
    source_exclusion_keys = (set(map(tuple, unscored_rows[["game_id", "player_id"]]
                                     .drop_duplicates().astype("int64").to_numpy()))
                             if {"game_id", "player_id"}.issubset(unscored_rows.columns)
                             else set())
    unscored_union = missing_prediction_keys | source_exclusion_keys
    display["unscored"] = {
        "missing_prediction_identities": len(missing_prediction_keys),
        "source_exclusions": len(source_exclusion_keys),
        "unscored_identities": len(unscored_union),
        "predictions_without_official_outcome_identities": len(prediction_without_outcome_keys),
        "unscored_reason_counts": reason_counts,
        "source_exclusion_reason_counts": ({str(key): int(value) for key, value in
            unscored_rows.reason.fillna("UNSPECIFIED").value_counts().items()}
            if "reason" in unscored_rows else {}),
        "source_unscored_artifact_sha256": unscored_sha,
        "canonical_missing_predictions_sha256": missing_predictions_sha,
    }
    display["prediction_rows"] = int(len(graded))
    display["prediction_artifact_sha256"] = prediction_sha
    if coverage.get("status") in {"AVAILABLE", "AVAILABLE_PARTIAL"}:
        if (coverage.get("prediction_artifact_sha256") != prediction_sha
                or coverage.get("daily_run_id") != run_id):
            raise ValueError("PRODUCTION_SOG_MARKET_COVERAGE_REFERENCE_MISMATCH")
        bound_coverage = dict(coverage)
        bound_coverage.update({"reference_name": "sog_reference",
            "model_family": "poisson_baseline", "model_version": "baseline_v1",
            "prediction_rows": int(coverage.get("prediction_rows", len(graded))),
            "prediction_artifact_sha256": prediction_sha,
            "odds_observation_sha": coverage.get("odds_observation_manifest_sha256"),
            "population_binding": coverage.get("population_binding"),
            "affects_grading_denominator": False})
    else:
        bound_coverage = {"status": coverage.get("status", "UNAVAILABLE_FROM_RETAINED_BOUND_EVIDENCE"),
            "reference_name": "sog_reference", "model_family": "poisson_baseline",
            "model_version": "baseline_v1", "prediction_rows": int(len(graded)),
            "matched": None, "unmatched": None, "match_rate": None,
            "prediction_artifact_sha256": prediction_sha,
            "odds_observation_sha": None, "population_binding": None,
            "affects_grading_denominator": False}
    display["market_coverage"] = bound_coverage
    stable_grade = graded.drop(columns=["grading_timestamp_utc"], errors="ignore")
    grade_csv_sha = hashlib.sha256(
        stable_grade.to_csv(index=False, lineterminator="\n").encode()).hexdigest()
    provenance = {
        "sog_reference_prediction_artifact_sha256": prediction_sha,
        "sog_reference_daily_receipt_manifest_sha256": receipt_manifest_sha,
        "sog_reference_fitted_model_identity_sha256": str(evidence.get("fitted_model_identity_sha256") or ""),
        "sog_reference_scorer_sha256": str(scorer.get("sha256") or ""),
        "sog_reference_grade_rows_sha256": grade_csv_sha,
    }
    if feature_binding:
        provenance.update({
            "sog_reference_feature_input_sha256": str(feature_binding["feature_input_sha256"]),
            "sog_reference_feature_input_manifest_sha256": str(feature_binding["feature_input_manifest_sha256"]),
            "sog_reference_source_daily_run_id": run_id,
        })
    if unscored_sha:
        provenance["sog_reference_unscored_artifact_sha256"] = unscored_sha
    return display, provenance


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
    annotation_reconstruction_ids: set[str] = set()
    for annotation_dir in reconstruction_root.glob("coverage_gap_annotation=*"):
        try:
            verify_package(annotation_dir)
            annotation = validate_annotation(
                json.loads((annotation_dir / "annotation.json").read_text()), slate_date=slate_date)
            annotation_reconstruction_ids.add(str(annotation.get("source_reconstruction_identity")))
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue
    def has_matching_annotation(package: Path) -> bool:
        try:
            identity = json.loads((package / "coverage.json").read_text()).get(
                "reconstruction_identity")
            return identity in annotation_reconstruction_ids
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return False

    reconstruction_packages = sorted(reconstruction_root.glob("reconstruction=*"), reverse=True)
    # Prefer evidence with a valid matching cause annotation. Newer reconstruction
    # code can produce a different package identity for the same retained evidence.
    reconstruction_packages.sort(key=lambda item: not has_matching_annotation(item))
    for package in reconstruction_packages:
        try:
            manifest_sha = verify_package(package)
            coverage_path = package / "coverage.json"
            report = json.loads(coverage_path.read_text())
            if (report.get("schema_version") != "NHL_SOG_GAME_SPECIFIC_PRESTART_RECONSTRUCTION_V1"
                    or report.get("classification") != "GAME_SPECIFIC_PRESTART_RECONSTRUCTION"
                    or report.get("status") != "PASS" or report.get("slate_date") != slate_date):
                continue
            if report.get("identity_inputs", {}).get("algorithm_sha256") != _sha(
                    Path(__file__).resolve().parents[0] / "scripts" / "reconstruct_nhl_sog_game_specific_coverage.py"):
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
            for annotation_dir in sorted(package.parent.glob("coverage_gap_annotation=*"), reverse=True):
                try:
                    annotation_manifest_sha = verify_package(annotation_dir)
                    annotation_path = annotation_dir / "annotation.json"
                    annotation = validate_annotation(
                        json.loads(annotation_path.read_text()), slate_date=slate_date)
                    if annotation.get("source_reconstruction_identity") != report.get(
                            "reconstruction_identity"):
                        continue
                    result["sog"]["cause_annotations"] = annotation["game_annotations"]
                    result["sog"]["coverage_state_counts"] = annotation.get(
                        "coverage_state_counts", {})
                    result["sog"]["cause_annotation_path"] = str(annotation_path.resolve())
                    result["sog"]["cause_annotation_sha256"] = _sha(annotation_path)
                    result["sog"]["cause_annotation_package_manifest_sha256"] = annotation_manifest_sha
                    break
                except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                    continue
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
                projected_points = lane == "points" and grade_keys < prediction_keys
                if prediction_keys != grade_keys and not projected_points:
                    continue
                if len(prediction_keys) != report_rows:
                    continue
                if projected_points:
                    # Descriptive availability is a property of the proposition key.
                    # Verify the retained full attachment, then project its unmatched
                    # keys onto the governed grade population. The prediction hash may
                    # differ when probabilities/features changed while proposition
                    # identity stayed fixed.
                    attachment_path = Path(integrity_archive_root) / slate_date / "points_with_market.csv"
                    unmatched_path = Path(integrity_archive_root) / slate_date / "unmatched_points.csv"
                    if (_sha(attachment_path) != report.get("attachment_sha256")
                            or _sha(unmatched_path) != report.get("unmatched_sha256")):
                        continue
                    attached = pd.read_csv(attachment_path)
                    unmatched_frame = pd.read_csv(unmatched_path)
                    attached_keys = _record_keys(attached, ("player_id", "game_id", "line"))
                    unmatched_keys = _record_keys(unmatched_frame, ("player_id", "game_id", "line"))
                    if (attached_keys != prediction_keys or not unmatched_keys.issubset(attached_keys)
                            or not grade_keys.issubset(attached_keys)):
                        continue
                    observation_path = Path(next((item.get("path", "") for item in odds_inputs
                        if item.get("manifest_sha256") == report.get(
                            "odds_observation_manifest_sha256")), ""))
                    observation = json.loads((observation_path / "observation_summary.json").read_text())
                    observed_at = pd.Timestamp(observation["observation_timestamp_utc"])
                    start_column = ("canonical_scheduled_start_time_utc"
                                    if "canonical_scheduled_start_time_utc" in grade
                                    else "scheduled_start_time_utc")
                    if start_column not in grade:
                        continue
                    starts = grade.groupby("game_id")[start_column].first()
                    if any(observed_at >= pd.Timestamp(value) for value in starts):
                        continue
                    matched = len(grade_keys - unmatched_keys)
                    unmatched = len(grade_keys & unmatched_keys)
                    ambiguous = 0
                else:
                    matched = int(counts["matched_count"])
                    unmatched = int(counts["unmatched_count"])
                    ambiguous = int(counts["ambiguous_count"])
                    if matched + unmatched + ambiguous != report_rows:
                        continue
                result[lane] = {
                    "status": "AVAILABLE", "prediction_rows": len(grade_keys),
                    "matched": matched, "unmatched": unmatched,
                    "ambiguous": ambiguous,
                    "match_rate": matched / len(grade_keys) if grade_keys else None,
                    "source_artifact": str(archive_path.resolve()),
                    "source_artifact_sha256": integrity_identity["sha256"],
                    "daily_run_id": run_id,
                    "daily_receipt_manifest_sha256": receipt_manifest_sha,
                    "prediction_artifact_sha256": prediction_identity["sha256"],
                    "odds_observation_manifest_sha256": report[
                        "odds_observation_manifest_sha256"],
                    "population_binding": "EXACT_GAME_PLAYER_LINE_KEYS",
                }
                if projected_points:
                    result[lane].update({
                        "source_prediction_rows": report_rows,
                        "source_prediction_artifact_sha256": prediction_identity["sha256"],
                        "population_binding": "EXACT_GRADE_KEY_PROJECTION_FROM_RETAINED_SUPERSET",
                        "eligible_game_count": int(len(starts)),
                        "eligible_prestart": len(grade_keys),
                        "poststart_excluded": 0,
                        "matched_key_projection": True,
                        "attachment_path": str(attachment_path.resolve()),
                        "attachment_sha256": report["attachment_sha256"],
                        "unmatched_attachment_path": str(unmatched_path.resolve()),
                        "unmatched_attachment_sha256": report["unmatched_sha256"],
                        "daily_receipt_path": str(receipt_path.resolve()),
                        "odds_observation_path": str(observation_path.resolve()),
                    })
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
    reconstruction_root = (Path(daily_run_root).parent
                           / "sog_market_coverage_reconstructions"
                           / "season=2026" / f"slate_date={slate_date}")
    reconstruction_reports = sorted(
        reconstruction_root.glob("reconstruction=*/sog_attachment_integrity.json"),
        reverse=True,
    ) if reconstruction_root.exists() else []
    reconstruction_reports.sort(key=lambda candidate: not has_matching_annotation(candidate.parent))
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
    if sog_report_path is not None:
        try:
            report, _ = verify_sog_integrity_package(sog_report_path, slate_date)
            cause_fields = {key: value for key, value in result["sog"].items()
                            if key.startswith("cause_") or key == "coverage_state_counts"}
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
            result["sog"].update(cause_fields)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            pass
    return result


def summarize_frames(*, slate_date: str, games: int, phase: str,
                     reconciliation_status: str, package_identity: str,
                     grades: dict[str, pd.DataFrame], source_artifacts: dict[str, str],
                     generated_at_utc: str | None = None,
                     challengers: dict[str, pd.DataFrame] | None = None,
                     market_coverage: dict[str, dict[str, Any]] | None = None,
                     production_sog_reference: dict[str, Any] | None = None,
                     points_authority_context: dict[str, Any] | None = None,
                     sog_fixed_blend_context: dict[str, Any] | None = None,
                     package: Path | None = None) -> dict[str, Any]:
    challengers = challengers or {}
    moneyline = grades.get("moneyline", pd.DataFrame())
    puck_line = grades.get("puck_line", pd.DataFrame())
    sog = grades.get("sog", pd.DataFrame())
    points = grades.get("points", pd.DataFrame())
    saves = grades.get("saves", pd.DataFrame())
    source_artifacts = dict(source_artifacts)
    if points_authority_context:
        source_artifacts["points_authority_context_sha256"] = hashlib.sha256(
            json.dumps(points_authority_context, sort_keys=True,
                       separators=(",", ":")).encode()).hexdigest()
    for lane, frame in (("moneyline", moneyline), ("puck_line", puck_line)):
        _assert_game_market_nonpushable(
            frame, market=lane, slate_date=slate_date,
            grade_source=source_artifacts.get(f"{lane}_grade_sha256", "UNBOUND"),
        )
    for lane, frame in challengers.items():
        if lane in {"moneyline", "puck_line"}:
            _assert_game_market_nonpushable(
                frame, market=lane, slate_date=slate_date,
                grade_source=source_artifacts.get(
                    f"{lane}_challenger_grade_sha256", "UNBOUND"),
            )
    for lane, frame in (("sog", sog), ("points", points), ("saves", saves)):
        _assert_prop_push_integrity(
            frame, market=lane, slate_date=slate_date,
            grade_source=source_artifacts.get(f"{lane}_grade_sha256", "UNBOUND"),
        )
    models = {
        "moneyline": {"reference": _game_summary(
            moneyline, lane="moneyline", model_name="moneyline_reference")},
        "puck_line": {"reference": _game_summary(
            puck_line, lane="puck_line", model_name="puck_line_reference")},
        "sog": _model_groups(sog, lane="sog"),
        "points": _model_groups(points, lane="points"),
        "saves": _model_groups(saves, lane="saves"),
    }
    if production_sog_reference is not None:
        models["sog"]["reference"] = dict(production_sog_reference)
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
    if production_sog_reference is not None:
        reference_coverage = production_sog_reference.get("market_coverage")
        if reference_coverage is not None:
            market_coverage = dict(market_coverage)
            market_coverage["sog"] = dict(reference_coverage)
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
    if points_authority_context:
        models["points"]["authority_context"] = dict(points_authority_context)
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
        "sog_fixed_blend_prospective": dict(sog_fixed_blend_context or {
            "status": "NO_IMMUTABLE_SHADOW_CAPTURE", "grades": []}),
    }


def render_markdown(summary: dict[str, Any]) -> str:
    def rate(value: Any) -> str:
        return "n/a" if value is None else f"{100 * float(value):.1f}%"

    def game_line(value: dict[str, Any]) -> str:
        record = f"{value['correct']}-{value['incorrect']}"
        if value.get("pushes_applicable", False):
            record += f"-{value.get('pushes', 0)}"
        return (f"{record} | {rate(value.get('accuracy'))}"
                f"; graded {value['graded']}; unresolved {value['unresolved']}")

    def prop_body(display: dict[str, Any], title: str) -> list[str]:
        lines: list[str] = []
        overall = display.get("overall", {})
        record = f"{overall.get('wins', 0)}-{overall.get('losses', 0)}"
        if overall.get("pushes_applicable", False):
            record += f"-{overall.get('pushes', 0)}"
        lines.append(
            f"Overall: {record} | {rate(overall.get('win_rate'))}; "
            f"settled {overall.get('settled', 0)}; unresolved {overall.get('unresolved', 0)}"
        )
        if title == "Saves":
            lines.append(f"Did not start: {overall.get('confirmed_did_not_start_not_gradeable', 0)}; "
                         f"starter unresolved: {overall.get('starter_status_unresolved', 0)}")
        side = display.get("by_side", {})
        if side:
            lines.append("By side: " + "; ".join(
                f"{name} {v.get('wins', 0)}-{v.get('losses', 0)}"
                + (f"-{v.get('pushes', 0)}" if v.get("pushes_applicable", False) else "")
                for name, v in side.items()))
        by_line = display.get("by_line", {})
        if by_line:
            has_pushable_line = any(values.get("pushes_applicable", False)
                                    for values in by_line.values())
            if has_pushable_line:
                lines += ["", "By line:", "| Line | Settled | W | L | P | Unresolved | Win % |",
                          "|---:|---:|---:|---:|---:|---:|---:|"]
            else:
                lines += ["", "By line:", "| Line | Settled | W | L | Unresolved | Win % |",
                          "|---:|---:|---:|---:|---:|---:|"]
            for line, values in by_line.items():
                rendered = (f"| {line} | {values.get('settled', 0)} | {values.get('wins', 0)} | "
                            f"{values.get('losses', 0)} | ")
                if has_pushable_line:
                    rendered += (f"{values.get('pushes', 0)} | "
                                 if values.get("pushes_applicable", False) else "— | ")
                rendered += f"{values.get('unresolved', 0)} | {rate(values.get('win_rate'))} |"
                lines.append(rendered)
        return lines

    def prop_section(title: str, models: dict[str, Any]) -> list[str]:
        lines = [f"## {title}"]
        if title == "SOG":
            reference = models.get("reference")
            if isinstance(reference, dict) and "overall" in reference:
                identity = reference.get("model_identity", {})
                lines.append(
                    "Production/reference (8rain export): "
                    f"{identity.get('model_family', 'unknown')} / "
                    f"{identity.get('model_version', 'unknown')} — "
                    + prop_body(reference, title)[0])
                lines.extend(prop_body(reference, title)[1:])
                probability = reference.get("probability_context", {})
                if probability:
                    lines.append("Reference probability context: " + "; ".join(
                        f"{key.removeprefix('average_predicted_probability_')} "
                        f"{rate(value)}" for key, value in probability.items()))
                lines.append("Cold-start shadow arms:")
            for name, display in models.items():
                if name == "reference" or not isinstance(display, dict) or "overall" not in display:
                    continue
                lines.append(f"{name}: " + prop_body(display, title)[0])
                lines.extend(prop_body(display, title)[1:])
            return lines
        lines.extend(prop_body(models.get("reference", {}), title))
        if title == "Points":
            lines.append("Realized total: official goals + official assists.")
            authority = models.get("authority_context")
            if authority:
                production = authority.get("production", {})
                shadow = authority.get("incumbent_shadow", {})
                lines.append(
                    f"Production authority: {production.get('model', 'unknown')} "
                    f"(prediction SHA {production.get('prediction_sha256', 'unavailable')}).")
                lines.append(
                    f"Incumbent shadow: {shadow.get('model', 'unknown')} "
                    f"({shadow.get('status', 'unavailable')}; prediction SHA "
                    f"{shadow.get('sha256') or shadow.get('prediction_sha256') or 'unavailable'}).")
                evaluation = authority.get("hgb_evaluation", {})
                if evaluation:
                    metrics = evaluation.get("metrics", {})
                    lines.append(
                        f"HGB {evaluation.get('role', 'shadow').lower()} grade: "
                        f"n={metrics.get('participated_graded_count', metrics.get('n', 0))}; "
                        f"average threshold log loss="
                        f"{metrics.get('average_threshold_log_loss', 'unavailable')}; "
                        f"grade SHA={evaluation.get('grade_sha256', 'unavailable')}.")
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
    has_pushable_contract = any(
        isinstance(model, dict) and bool(model.get("overall", {}).get("pushes_applicable"))
        for lane in ("sog", "points", "saves")
        for model in models.get(lane, {}).values()
    )
    notes = (["- Pushes are reported separately only for contracts where they are applicable."]
             if has_pushable_contract else [
                 "- Current Moneyline, ±1.5 Puck Line, and half-point prop contracts are non-pushable."
             ])
    lines += ["", *prop_section("Points", models["points"]), "",
              *prop_section("Saves", models["saves"]), "", "## Notes",
              *notes,
              "- Market match status does not determine whether a prediction is graded.",
              "- No selection policy or promotion rule was applied."]
    fixed_blend = summary.get("sog_fixed_blend_prospective") or {}
    lines += ["", "## SOG Fixed Blend Prospective Shadows",
              f"Status: {fixed_blend.get('status', 'NO_IMMUTABLE_SHADOW_CAPTURE')}"]
    for grade in fixed_blend.get("grades", []):
        lines.append(
            f"- {grade.get('model_identity')}: settled player-games "
            f"{grade.get('settled_player_games', 0)}, paired rows {grade.get('paired_n', 0)}, "
            f"count MAE difference vs d10 {grade.get('paired_count_mae_difference_vs_d10')}, "
            f"1.5 Brier difference {grade.get('paired_brier_difference_1_5_vs_d10')}."
        )
    lines.append("These are research shadows; production SOG authority remains d10.")
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
    for annotation in models.get("sog", {}).get("market_coverage", {}).get(
            "cause_annotations", []):
        if annotation.get("cause_classification") == (
                "PRESTART_EVIDENCE_UNAVAILABLE_DUE_TO_PIPELINE_REPAIR_DELAY"):
            coverage_rows.append(
                f"SOG coverage caveat: Game {annotation['game_id']} has no valid prestart market snapshot "
                "because the SOG market-retention repair was not completed before puck drop. "
                "This is an operational evidence gap; bookmaker market absence is not inferred."
            )
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


_LINEAGE_GRADE_KEYS = (
    "moneyline_grade_sha256", "puck_line_grade_sha256", "sog_grade_sha256",
    "points_grade_sha256", "saves_grade_sha256", "moneyline_challenger_grade_sha256",
    "puck_line_challenger_grade_sha256",
)
_LINEAGE_RECON_KEYS = (
    "reconciliation_manifest_sha256", "reconciliation_summary_sha256",
    "canonical_game_outcomes_sha256", "phase_restatement_lineage_sha256",
)


def _summary_lineage_key(summary: dict[str, Any]) -> str:
    """Stable governed grade lineage, intentionally excluding descriptive evidence."""
    sources = summary.get("source_artifacts") or {}
    facts = {
        "slate_date": summary.get("slate_date"),
        "package_identity": summary.get("package_identity"),
        "canonical_phase": summary.get("canonical_phase"),
        "source_artifacts": {key: sources[key] for key in (*_LINEAGE_RECON_KEYS, *_LINEAGE_GRADE_KEYS)
                             if key in sources},
    }
    return hashlib.sha256(json.dumps(facts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _summary_descriptive_identity(summary: dict[str, Any]) -> str:
    sources = summary.get("source_artifacts") or {}
    descriptive_sources = {key: value for key, value in sources.items()
                           if key not in (*_LINEAGE_RECON_KEYS, *_LINEAGE_GRADE_KEYS)}
    facts = {"models": summary.get("models"), "source_artifacts": descriptive_sources}
    return hashlib.sha256(json.dumps(facts, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _compatible_summary_versions(root: Path, lineage_key: str) -> list[tuple[Path, dict[str, Any]]]:
    found = []
    for candidate in root.glob("performance_summary=*"):
        try:
            verify_summary_package(candidate)
            summary = json.loads((candidate / "performance_summary.json").read_text())
            if (summary.get("revision_lineage_key") or summary.get("summary_lineage_key")
                    or _summary_lineage_key(summary)) == lineage_key:
                found.append((candidate, summary))
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue
    return found


def _version_summary(summary: dict[str, Any], root: Path) -> tuple[Path, dict[str, Any]]:
    """Allocate an immutable monotonic revision for a logical summary lineage."""
    lineage_key = _summary_lineage_key(summary)
    existing = _compatible_summary_versions(root, lineage_key)
    descriptive_identity = _summary_descriptive_identity(summary)
    for path, prior in existing:
        if (isinstance(prior.get("summary_revision"), int)
                and prior.get("revision_lineage_key") == lineage_key
                and prior.get("descriptive_identity") == descriptive_identity):
            return path / "performance_summary.json", prior

    explicit = [(path, prior) for path, prior in existing
                if isinstance(prior.get("summary_revision"), int)]
    revision = max((int(prior["summary_revision"]) for _, prior in explicit), default=0) + 1
    if explicit:
        supersedes = sorted({prior["summary_identity"] for _, prior in explicit
                             if int(prior["summary_revision"]) == revision - 1})
    else:
        supersedes = sorted({prior["summary_identity"] for _, prior in existing
                             if prior.get("summary_identity")})

    versioned = dict(summary)
    versioned.update({"summary_revision": revision,
                      "supersedes_summary_identities": supersedes,
                      "revision_lineage_key": lineage_key,
                      "descriptive_identity": descriptive_identity})
    identity_payload = {"schema_version": versioned.get("schema_version"),
                        "summary_lineage_key": lineage_key,
                        "summary_revision": revision,
                        "descriptive_identity": descriptive_identity,
                        "supersedes_summary_identities": supersedes}
    identity = hashlib.sha256(json.dumps(identity_payload, sort_keys=True,
                                         separators=(",", ":")).encode()).hexdigest()
    versioned["summary_identity"] = identity
    destination = root / f"performance_summary={identity[:20]}"
    versioned["summary_package"] = str(destination.resolve())
    json_path, _ = _write_immutable_package(destination, versioned)
    return json_path.resolve(), json.loads(json_path.read_text())


def _grade_projection(summary: dict[str, Any]) -> dict[str, Any]:
    def strip_coverage(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: strip_coverage(item) for key, item in value.items()
                    if key not in {"market_coverage", "arm_market_coverage"}}
        if isinstance(value, list):
            return [strip_coverage(item) for item in value]
        return value
    return strip_coverage(summary.get("models") or {})


def _descriptive_evidence_is_valid(summary: dict[str, Any]) -> bool:
    points = ((summary.get("models") or {}).get("points") or {}).get("market_coverage") or {}
    if points.get("matched_key_projection"):
        try:
            report_path = Path(points["source_artifact"])
            attachment_path = Path(points["attachment_path"])
            unmatched_path = Path(points["unmatched_attachment_path"])
            receipt_path = Path(points["daily_receipt_path"])
            observation_path = Path(points["odds_observation_path"])
            if (_sha(report_path) != points.get("source_artifact_sha256")
                    or _sha(attachment_path) != points.get("attachment_sha256")
                    or _sha(unmatched_path) != points.get("unmatched_attachment_sha256")):
                return False
            report = json.loads(report_path.read_text())
            if (report.get("status") != "PASS"
                    or report.get("parent_daily_run_id") != points.get("daily_run_id")
                    or report.get("prediction_artifact_sha256") != points.get(
                        "source_prediction_artifact_sha256")
                    or report.get("odds_observation_manifest_sha256") != points.get(
                        "odds_observation_manifest_sha256")):
                return False
            _, receipt_manifest = _verify_daily_receipt(receipt_path, str(summary["slate_date"]))
            if receipt_manifest != points.get("daily_receipt_manifest_sha256"):
                return False
            if verify_package(observation_path) != points.get("odds_observation_manifest_sha256"):
                return False
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            return False
    coverage = ((summary.get("models") or {}).get("sog") or {}).get("market_coverage") or {}
    source_path = coverage.get("source_artifact")
    annotation_path = coverage.get("cause_annotation_path")
    reconstruction_identity = None
    if source_path:
        path = Path(source_path)
        if not path.is_file() or _sha(path) != coverage.get("source_artifact_sha256"):
            return False
        try:
            reconstruction_identity = json.loads(path.read_text()).get("reconstruction_identity")
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return False
        if coverage.get("integrity_package_manifest_sha256"):
            try:
                if verify_package(path.parent) != coverage["integrity_package_manifest_sha256"]:
                    return False
            except (OSError, ValueError, KeyError, TypeError):
                return False
    if annotation_path:
        path = Path(annotation_path)
        try:
            annotation = validate_annotation(json.loads(path.read_text()),
                                             slate_date=str(summary.get("slate_date")))
            if (_sha(path) != coverage.get("cause_annotation_sha256")
                    or verify_package(path.parent) != coverage.get(
                        "cause_annotation_package_manifest_sha256")
                    or (reconstruction_identity is not None and annotation.get(
                        "source_reconstruction_identity") != reconstruction_identity)):
                return False
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            return False
    return True


def select_authoritative_summary(*, root: Path, expected: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    """Select only complete summaries matching the current governed grade state."""
    root = Path(root)
    lineage_key = _summary_lineage_key(expected)
    compatible = []
    expected_sources = expected.get("source_artifacts") or {}
    expected_grade_sources = {key: value for key, value in expected_sources.items()
                              if key in (*_LINEAGE_GRADE_KEYS, *_LINEAGE_RECON_KEYS)}
    for path, candidate in _compatible_summary_versions(root, lineage_key):
        sources = candidate.get("source_artifacts") or {}
        if (candidate.get("slate_date") != expected.get("slate_date")
                or candidate.get("package_identity") != expected.get("package_identity")
                or candidate.get("canonical_phase") != expected.get("canonical_phase")
                or candidate.get("official_outcomes_status") != "FINAL"
                or candidate.get("summary_identity") is None
                or candidate.get("summary_identity", "")[:20] != path.name.removeprefix("performance_summary=")
                or {key: value for key, value in sources.items()
                    if key in (*_LINEAGE_GRADE_KEYS, *_LINEAGE_RECON_KEYS)} != expected_grade_sources
                or _grade_projection(candidate) != _grade_projection(expected)
                or not _descriptive_evidence_is_valid(candidate)):
            continue
        compatible.append((path, candidate))

    if not compatible:
        raise RuntimeError("NO_VALID_PERFORMANCE_SUMMARY_FOR_GOVERNED_LINEAGE")
    versioned = [(path, summary) for path, summary in compatible
                 if isinstance(summary.get("summary_revision"), int)]
    if not versioned:
        if len(compatible) != 1:
            raise RuntimeError("LEGACY_SUMMARY_AUTHORITY_AMBIGUOUS")
        return compatible[0][0] / "performance_summary.json", compatible[0][1]
    highest = max(int(summary["summary_revision"]) for _, summary in versioned)
    winners = [(path, summary) for path, summary in versioned
               if int(summary["summary_revision"]) == highest]
    identities = {summary.get("summary_identity") for _, summary in winners}
    if len(identities) != 1:
        raise RuntimeError("PERFORMANCE_SUMMARY_REVISION_CONFLICT")
    return winners[0][0] / "performance_summary.json", winners[0][1]


def generate_from_artifacts(*, package: Path, restatement: Path,
                            reconciliation_status: str,
                            challengers: dict[str, pd.DataFrame] | None = None,
                            challenger_source_artifacts: dict[str, Path] | None = None,
                            market_coverage: dict[str, dict[str, Any]] | None = None,
                            production_sog_reference: dict[str, Any] | None = None,
                            production_sog_reference_sources: dict[str, str] | None = None,
                            points_authority_context: dict[str, Any] | None = None,
                            sog_fixed_blend_context: dict[str, Any] | None = None,
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
    source_hashes.update(production_sog_reference_sources or {})
    if points_authority_context:
        authority_sha = hashlib.sha256(json.dumps(
            points_authority_context, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        source_hashes["points_authority_context_sha256"] = authority_sha
    if sog_fixed_blend_context:
        source_hashes["sog_fixed_blend_prospective_context_sha256"] = hashlib.sha256(
            json.dumps(sog_fixed_blend_context, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
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
            if lane == "sog" and coverage.get("reconstructed_from_retained_evidence"):
                source_hashes["sog_game_specific_reconstruction_sha256"] = str(
                    coverage.get("source_artifact_sha256") or "")
                source_hashes["sog_game_specific_reconstruction_manifest_sha256"] = str(
                    coverage.get("integrity_package_manifest_sha256") or "")
            if lane == "sog" and coverage.get("cause_annotation_sha256"):
                source_hashes["sog_coverage_cause_annotation_sha256"] = str(
                    coverage["cause_annotation_sha256"])
                source_hashes["sog_coverage_cause_annotation_package_manifest_sha256"] = str(
                    coverage.get("cause_annotation_package_manifest_sha256") or "")
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
        challengers=challengers, market_coverage=market_coverage,
        production_sog_reference=production_sog_reference,
        points_authority_context=points_authority_context, package=package,
        sog_fixed_blend_context=sog_fixed_blend_context,
    )
    summary["official_outcomes_status"] = (
        "FINAL" if source_summary.get("status") == "COMPLETE"
        and source_marker.get("status") == "COMPLETE" else "INCOMPLETE")
    summary["summary_identity"] = identity
    summary["summary_package"] = str(destination.resolve())
    json_path, summary = _version_summary(summary, root)
    return json_path, json_path.parent / "performance_summary.md", summary
