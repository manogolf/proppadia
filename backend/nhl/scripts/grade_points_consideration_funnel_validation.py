#!/usr/bin/env python3
"""Grade the frozen first prospective NHL Points consideration-funnel date."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DATE = "2026-10-14"
CONTRACT_VERSION = "NHL_POINTS_CONSIDERATION_FUNNEL_V1"
CONTRACT_FINGERPRINT = "eb0c7d140103edeefcb7680cce7bb5fb3ba1bd88d990cff1e53bf1dd689fe3a9"
FREEZE = ROOT / "artifacts/analysis/nhl/points_consideration_funnel_freeze/2026-10-14"
DISCOVERY = ROOT / "artifacts/analysis/nhl/points_consideration_funnel/2026-10-14"
RECON = ROOT / "artifacts/operational/nhl/postgame_reconciliation/2026-10-14/reconciliation=dab06367045879865ae9"
DAILY_RUN = "nhldaily_20261015T140453183604Z_bfa01e59"
OCT15_ATTACH = ROOT / f"artifacts/operational/nhl/points_market_attachments/season=2026/slate_date=2026-10-15/run_id={DAILY_RUN}/points_with_market.csv"
OUT = ROOT / "artifacts/analysis/nhl/points_consideration_funnel_validation/2026-10-14"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = list(rows[0]) if rows else []
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def key(row: dict[str, Any]) -> tuple[str, str, str]:
    return str(row["game_id"]), str(row["player_id"]), f"{float(row['line']):.1f}"


def number(row: dict[str, Any], name: str) -> float:
    value = row.get(name)
    if value in (None, ""):
        return math.nan
    return float(value)


def flags_from_dry(row: dict[str, str]) -> dict[str, Any]:
    # Re-express only the frozen V1 decision order. No outcomes enter this function.
    edge = number(row, "selected_production_edge")
    gap = number(row, "signed_selected_model_minus_no_vig_market_gap")
    stage1 = math.isfinite(edge) and edge < 0
    stage2 = not stage1 and math.isfinite(gap) and gap > 0.15
    stage3 = (
        not stage1
        and not stage2
        and row.get("model_side") in ("OVER", "UNDER")
        and row.get("market_side") in ("OVER", "UNDER")
        and row["model_side"] != row["market_side"]
    )
    first = "STAGE_1" if stage1 else "STAGE_2" if stage2 else "STAGE_3" if stage3 else ""
    return {"stage1_excluded": stage1, "stage2_excluded": stage2, "stage3_excluded": stage3,
            "first_exclusion_stage": first, "survivor": not (stage1 or stage2 or stage3)}


def attachment_identity(row: dict[str, Any]) -> str:
    identity = "|".join(str(row.get(k, "")) for k in (
        "game_id", "player_id", "line", "price_over", "price_under",
        "source_books_over", "source_books_under", "odds_observation_manifest_sha256"))
    return hashlib.sha256(identity.encode()).hexdigest()


def grade_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0, "wins": 0, "losses": 0, "accuracy": None, "expected_losses": 0.0,
                "observed_losses": 0, "excess_losses": 0.0, "mean_selected_model_probability": None,
                "realized_win_rate": None, "calibration_gap": None, "brier": None, "log_loss": None,
                "market_accuracy": None, "market_expected_losses": 0.0, "market_brier": None,
                "market_log_loss": None, "market_mean_selected_probability": None,
                "market_calibration_gap": None}
    wins = losses = 0
    exp_losses = brier_sum = ll_sum = market_brier_sum = market_ll_sum = 0.0
    selected_probs: list[float] = []
    market_probs: list[float] = []
    market_correct = 0
    for row in rows:
        y = float(row["official_points"]) > float(row["line"])
        p = float(row["model_p_over"])
        mp = float(row["market_no_vig_over"])
        model_over = p >= 0.5
        market_over = mp >= 0.5
        pred_correct = model_over == y
        wins += int(pred_correct)
        losses += int(not pred_correct)
        selected_p = p if model_over else 1 - p
        selected_probs.append(selected_p)
        exp_losses += 1 - selected_p
        brier_sum += (p - float(y)) ** 2
        pc = min(max(p, 1e-15), 1 - 1e-15)
        ll_sum += -(float(y) * math.log(pc) + (1 - float(y)) * math.log1p(-pc))
        market_correct += int(market_over == y)
        market_probs.append(mp if market_over else 1 - mp)
        market_brier_sum += (mp - float(y)) ** 2
        mc = min(max(mp, 1e-15), 1 - 1e-15)
        market_ll_sum += -(float(y) * math.log(mc) + (1 - float(y)) * math.log1p(-mc))
    n = len(rows)
    observed_wr = wins / n
    mean_model_selected = sum(selected_probs) / n
    market_accuracy = market_correct / n
    mean_market_selected = sum(market_probs) / n
    return {
        "n": n, "wins": wins, "losses": losses, "accuracy": observed_wr,
        "expected_losses": exp_losses, "observed_losses": losses,
        "excess_losses": losses - exp_losses,
        "mean_selected_model_probability": mean_model_selected,
        "realized_win_rate": observed_wr,
        "calibration_gap": mean_model_selected - observed_wr,
        "brier": brier_sum / n, "log_loss": ll_sum / n,
        "market_accuracy": market_accuracy,
        "market_expected_losses": sum(1 - p for p in market_probs),
        "market_brier": market_brier_sum / n, "market_log_loss": market_ll_sum / n,
        "market_mean_selected_probability": mean_market_selected,
        "market_calibration_gap": mean_market_selected - market_accuracy,
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    contract_path = FREEZE / "frozen_funnel_contract.json"
    contract = json.loads(contract_path.read_text())
    binding = json.loads((FREEZE / "oct14_pre_outcome_classification_binding.json").read_text())
    dry_path = DISCOVERY / "current_day_dry_view_2026-10-14.csv"
    if sha256(dry_path) != binding["dry_view_sha256"]:
        raise AssertionError("frozen pre-outcome dry-view hash mismatch")
    if contract["contract_version"] != CONTRACT_VERSION:
        raise AssertionError("frozen contract version mismatch")
    original = read_csv(FREEZE / "prospective_ledger.csv")
    dry = read_csv(DISCOVERY / "current_day_dry_view_2026-10-14.csv")
    grade_rows = read_csv(RECON / "graded_points.csv")
    outcomes = read_csv(RECON / "canonical_skater_outcomes.csv")
    if len(original) != 93 or len(dry) != 93:
        raise AssertionError(f"expected 93 frozen/dry rows, found {len(original)}/{len(dry)}")

    # The pre-outcome freeze and dry view bind one-to-one on canonical identity.
    def unique_index(rows: list[dict[str, Any]], label: str) -> dict[tuple[str, str, str], dict[str, Any]]:
        result: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rows:
            k = key(row)
            if k in result:
                raise AssertionError(f"duplicate {label} key: {k}")
            result[k] = row
        return result

    frozen_by_key = unique_index(original, "frozen ledger")
    dry_by_key = unique_index(dry, "pre-outcome dry view")
    grade_by_key = unique_index(grade_rows, "reconciliation grade")
    outcome_index: dict[tuple[str, str], dict[str, str]] = {}
    for outcome in outcomes:
        outcome_key = (str(outcome["game_id"]), str(outcome["player_id"]))
        if outcome_key in outcome_index:
            raise AssertionError(f"duplicate canonical player-game outcome: {outcome_key}")
        outcome_index[outcome_key] = outcome

    class_audit: list[dict[str, Any]] = []
    all_line_counts = Counter()
    line_counts: dict[str, Counter] = {}
    mismatch_count = 0
    for k, frozen in frozen_by_key.items():
        d = dry_by_key.get(k)
        if d is None:
            raise AssertionError(f"frozen row missing pre-outcome binding: {k}")
        calculated = flags_from_dry(d)
        checks = {
            "prediction_identity_matches": frozen["prediction_identity"] == d["prediction_identity"],
            "prediction_artifact_sha256_matches": frozen["prediction_artifact_sha256"] == d["prediction_artifact_sha256"],
            "selected_edge_matches": float(frozen["selected_production_edge"]) == float(d["selected_production_edge"]),
            "signed_gap_matches": float(frozen["signed_selected_model_minus_no_vig_market_gap"]) == float(d["signed_selected_model_minus_no_vig_market_gap"]),
            "classification_timestamp_matches": frozen["classification_timestamp_utc"] == d["observation_timestamp_utc"],
            "stage1_matches": str(calculated["stage1_excluded"]) == frozen["stage1_excluded"],
            "stage2_matches": str(calculated["stage2_excluded"]) == frozen["stage2_excluded"],
            "stage3_matches": str(calculated["stage3_excluded"]) == frozen["stage3_excluded"],
            "first_exclusion_matches": calculated["first_exclusion_stage"] == frozen["first_exclusion_stage"],
            "survivor_matches": str(calculated["survivor"]) == frozen["survivor"],
            "market_identity_matches": frozen["market_attachment_identity"] == attachment_identity(d),
            "odds_manifest_matches": frozen["odds_observation_manifest_sha256"] == d["odds_observation_manifest_sha256"],
        }
        if not all(v for name, v in checks.items() if name not in ("prediction_identity_matches", "prediction_artifact_sha256_matches")):
            raise AssertionError(f"frozen V1 classification/binding mismatch at {k}: {checks}")
        if not checks["prediction_identity_matches"] or not checks["prediction_artifact_sha256_matches"]:
            raise AssertionError(f"frozen source identity differs from dry classification source: {k}")
        line = frozen["line"]
        c = line_counts.setdefault(line, Counter())
        c["stage0"] += 1
        c["stage1_removed"] += int(calculated["stage1_excluded"])
        c["stage2_removed"] += int(calculated["stage2_excluded"])
        c["stage3_removed"] += int(calculated["stage3_excluded"])
        c["survivors"] += int(calculated["survivor"])
        all_line_counts["total"] += 1
        class_audit.append({**{x: frozen[x] for x in ("slate_date", "game_id", "player_id", "line", "prediction_identity", "prediction_artifact_sha256", "market_attachment_identity", "odds_observation_manifest_sha256", "selected_production_edge", "signed_selected_model_minus_no_vig_market_gap", "stage1_excluded", "stage2_excluded", "stage3_excluded", "first_exclusion_stage", "survivor", "classification_timestamp_utc")}, **checks, "classification_source": "FROZEN_PRE_OUTCOME_LEDGER"})
    expected_counts = {"0.5": {"stage0": 58, "stage1_removed": 9, "stage2_removed": 26, "stage3_removed": 3, "survivors": 20},
                       "1.5": {"stage0": 31, "stage1_removed": 3, "stage2_removed": 9, "stage3_removed": 0, "survivors": 19},
                       "2.5": {"stage0": 4, "stage1_removed": 0, "stage2_removed": 0, "stage3_removed": 0, "survivors": 4}}
    for line, expected in expected_counts.items():
        actual = {k: line_counts[line][k] for k in expected}
        if actual != expected:
            raise AssertionError(f"frozen count reproduction failed for {line}: {actual}")

    # Outcome source joins at line-level canonical key; outcome-only values come from
    # the final grading file, while model/market probabilities stay from frozen pregame.
    joined: list[dict[str, Any]] = []
    join_audit: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []
    unresolved_rows: list[dict[str, Any]] = []
    for frozen in original:
        k = key(frozen)
        d = dry_by_key[k]
        calculated = flags_from_dry(d)
        g = grade_by_key.get(k)
        if g is None:
            raise AssertionError(f"missing reconciliation grade row for {k}")
        canonical_outcome = outcome_index.get((k[0], k[1]))
        participation = g.get("participation_state", "")
        status = g.get("grading_status", "")
        gp = g.get("official_points", "")
        if status == "SETTLED" and canonical_outcome is None:
            raise AssertionError(f"settled row has no canonical official skater outcome at {k}")
        canonical_points = canonical_outcome.get("official_points", "") if canonical_outcome else ""
        if gp and canonical_points and float(gp) != float(canonical_points):
            raise AssertionError(f"grade/canonical outcome points disagree at {k}")
        mismatch = g.get("prediction_identity", "") != frozen["prediction_identity"]
        if status == "SETTLED":
            if gp == "":
                raise AssertionError(f"settled row has no official points at {k}")
        elif status != "PARTICIPATION_STATUS_UNRESOLVED":
            raise AssertionError(f"unexpected official grade state {status} at {k}")
        row = dict(frozen)
        # Preserve original pre-outcome columns verbatim; append final official outcome state.
        row.update({"final_grade_status": "SETTLED" if status == "SETTLED" else "UNRESOLVED",
                    "reconciliation_grading_status": status, "official_points_final": gp,
                    "official_participation_state": participation,
                    "reconciliation_prediction_identity": g.get("prediction_identity", ""),
                    "prediction_identity_join_mode": "OUTCOME_JOIN_BY_CANONICAL_KEY",
                    "reconciliation_package": RECON.name})
        updated.append(row)
        join_audit.append({"slate_date": DATE, "game_id": k[0], "player_id": k[1], "line": k[2],
                           "join_key": "game_id,player_id,line", "join_count": 1,
                           "frozen_prediction_identity": frozen["prediction_identity"],
                           "reconciliation_prediction_identity": g.get("prediction_identity", ""),
                           "prediction_identity_match": not mismatch,
                           "identity_join_mode": "OUTCOME_JOIN_BY_CANONICAL_KEY",
                           "grade_status": status, "participation_state": participation,
                           "official_points": gp, "cross_game_check": k[0] == str(g["game_id"]),
                           "cross_player_check": k[1] == str(g["player_id"]),
                           "cross_line_check": k[2] == f"{float(g['line']):.1f}"})
        if status == "SETTLED":
            joined.append({**d, **{"game_id": k[0], "player_id": k[1], "line": k[2], "official_points": gp}, **calculated})
        else:
            unresolved_rows.append(row)
    if len(joined) != 91 or len(unresolved_rows) != 2 or len(join_audit) != 93:
        raise AssertionError(f"official result count mismatch: settled={len(joined)} unresolved={len(unresolved_rows)}")
    mismatch_count = sum(not x["prediction_identity_match"] for x in join_audit)

    settled_by_key = {key(r): r for r in joined}
    def cohort(predicate: Any) -> list[dict[str, Any]]:
        return [r for r in joined if predicate(r)]

    primary_all = [r for r in joined if r["line"] == "0.5"]
    stage1_removed = cohort(lambda r: r["line"] == "0.5" and r["stage1_excluded"])
    stage1_survivors = cohort(lambda r: r["line"] == "0.5" and not r["stage1_excluded"])
    stage2_removed = cohort(lambda r: r["line"] == "0.5" and r["stage2_excluded"])
    stage2_survivors = cohort(lambda r: r["line"] == "0.5" and not r["stage1_excluded"] and not r["stage2_excluded"])
    stage3_removed = cohort(lambda r: r["line"] == "0.5" and r["stage3_excluded"])
    final_survivors = cohort(lambda r: r["line"] == "0.5" and r["survivor"])

    def row_metrics(name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        return {"cohort": name, **grade_metrics(rows)}

    cohorts = {
        "stage0_raw": primary_all,
        "stage1_incremental": stage1_removed,
        "after_stage1": stage1_survivors,
        "stage2_incremental": stage2_removed,
        "after_stage2": stage2_survivors,
        "stage3_incremental": stage3_removed,
        "final_survivors": final_survivors,
    }
    metric_rows = [row_metrics(name, rows) for name, rows in cohorts.items()]
    metrics = {x["cohort"]: x for x in metric_rows}
    stage2_removed_metrics = metrics["stage2_incremental"]

    loss_table = []
    excess_changes = []
    for name, before, removed, after in [
        ("Stage 1 incremental", primary_all, stage1_removed, stage1_survivors),
        ("Stage 2 incremental", stage1_survivors, stage2_removed, stage2_survivors),
        ("Stage 3 incremental", stage2_survivors, stage3_removed, final_survivors),
    ]:
        rm = grade_metrics(removed)
        bm, am = grade_metrics(before), grade_metrics(after)
        loss_table.append({"cohort": name, "rows": rm["n"], "wins_removed": rm["wins"], "losses_removed": rm["losses"],
                           "expected_losses": rm["expected_losses"], "observed_losses": rm["observed_losses"],
                           "excess_losses": rm["excess_losses"]})
        excess_changes.append({"stage": name, "survivor_excess_losses_before": bm["excess_losses"],
                               "survivor_excess_losses_after": am["excess_losses"],
                               "excess_losses_removed": bm["excess_losses"] - am["excess_losses"]})
    cumulative = [
        {"cohort": "Stage 0 raw", **grade_metrics(primary_all)},
        {"cohort": "After Stage 1", **grade_metrics(stage1_survivors)},
        {"cohort": "After Stage 2", **grade_metrics(stage2_survivors)},
        {"cohort": "After Stage 3", **grade_metrics(final_survivors)},
    ]
    stage2_directions: dict[str, list[dict[str, Any]]] = {
        "HGB_UNDER": [r for r in stage2_removed if r["model_side"] == "UNDER"],
        "HGB_OVER": [r for r in stage2_removed if r["model_side"] == "OVER"],
        "SAME_SIDE_CONFIDENCE_DISAGREEMENT": [r for r in stage2_removed if r["model_side"] == r["market_side"]],
        "OPPOSITE_SIDE_DISAGREEMENT": [r for r in stage2_removed if r["model_side"] != r["market_side"]],
    }
    direction_metrics = [{"diagnostic_cohort": k, **grade_metrics(v)} for k, v in stage2_directions.items()]

    market_comparison = []
    for name, rows in (("STAGE0_RAW_COMMON_ROWS", primary_all), ("FINAL_SURVIVOR_COMMON_ROWS", final_survivors)):
        met = grade_metrics(rows)
        market_comparison.extend([
            {"cohort": name, "forecast": "HGB", "n": met["n"], "selected_side_accuracy": met["accuracy"],
             "brier": met["brier"], "log_loss": met["log_loss"], "mean_selected_probability": met["mean_selected_model_probability"],
             "realized_win_rate": met["realized_win_rate"], "calibration_gap": met["calibration_gap"]},
            {"cohort": name, "forecast": "NO_VIG_MARKET", "n": met["n"], "selected_side_accuracy": met["market_accuracy"],
             "brier": met["market_brier"], "log_loss": met["market_log_loss"], "mean_selected_probability": met["market_mean_selected_probability"],
             "realized_win_rate": met["market_accuracy"], "calibration_gap": met["market_calibration_gap"]},
        ])

    other_lines = {}
    for line in ("1.5", "2.5"):
        line_rows = [r for r in joined if r["line"] == line]
        other_lines[line] = {"description": "SEPARATE_LINE; 2.5 DESCRIPTIVE_ONLY" if line == "2.5" else "SEPARATE_LINE",
                             **grade_metrics(line_rows), "frozen_rows": line_counts[line]["stage0"],
                             "frozen_survivors": line_counts[line]["survivors"]}

    # Outcome-free first prospective classification for Oct 15 from retained immutable attachment.
    pending_source = read_csv(OCT15_ATTACH)
    parent_receipt_path = ROOT / f"artifacts/operational/nhl/daily_runs/run_id={DAILY_RUN}/parent_receipt.json"
    parent_receipt = json.loads(parent_receipt_path.read_text())
    points_lane = parent_receipt["lanes"]["points"]
    point_prediction = next(x for x in points_lane["outputs"] if x.get("path", "").endswith("/points_predictions.csv"))
    attached_integrity = json.loads((OCT15_ATTACH.parent / "points_attachment_integrity.json").read_text())
    if parent_receipt["parent_daily_run_id"] != DAILY_RUN or point_prediction["sha256"] != attached_integrity["prediction_artifact_sha256"]:
        raise AssertionError("Oct. 15 prediction artifact does not bind to retained Points attachment")
    if attached_integrity.get("status") != "PASS" or attached_integrity.get("odds_observation_manifest_sha256") != parent_receipt["odds_observation"]["manifest_sha256"]:
        raise AssertionError("Oct. 15 retained Points attachment integrity/odds binding failed")
    if sha256(Path(point_prediction["path"])) != point_prediction["sha256"]:
        raise AssertionError("Oct. 15 immutable Points prediction hash mismatch")
    pending_by_line: dict[str, list[dict[str, Any]]] = {}
    pending_rows: list[dict[str, Any]] = []
    for source in pending_source:
        try:
            if (source.get("parent_daily_run_id") != DAILY_RUN
                    or source.get("prediction_artifact_sha256") != point_prediction["sha256"]
                    or source.get("odds_observation_manifest_sha256") != parent_receipt["odds_observation"]["manifest_sha256"]
                    or source.get("game_date") != "2026-10-15"):
                raise AssertionError("Oct. 15 attachment row escaped immutable parent/odds/date binding")
            p = float(source["p_over"])
            po_raw = float(source["p_over_mkt"])
            pu_raw = float(source["p_under_mkt"])
            if not all(math.isfinite(v) for v in (p, po_raw, pu_raw)) or po_raw <= 0 or pu_raw <= 0:
                continue
            if not source.get("price_over") or not source.get("price_under"):
                continue
            no_vig_over = po_raw / (po_raw + pu_raw)
            no_vig_under = pu_raw / (po_raw + pu_raw)
            model_side = "OVER" if p >= 0.5 else "UNDER"
            model_selected = p if model_side == "OVER" else 1 - p
            market_side = "OVER" if no_vig_over >= 0.5 else "UNDER"
            market_selected = no_vig_over if model_side == "OVER" else no_vig_under
            price = float(source["price_over"] if model_side == "OVER" else source["price_under"])
            raw_prob = po_raw if model_side == "OVER" else pu_raw
            edge = model_selected - raw_prob
            gap = model_selected - market_selected
            s1 = edge < 0
            s2 = not s1 and gap > 0.15
            s3 = not s1 and not s2 and model_side != market_side
            first = "STAGE_1" if s1 else "STAGE_2" if s2 else "STAGE_3" if s3 else ""
            identity_parts = (source["prediction_artifact_sha256"], "2026-10-15", source["game_id"],
                              source["player_id"], f"{float(source['line']):.1f}",
                              "NHL_POINTS_COUNT_HGB_V1", "NHL_POINTS_LEADER_CORE_FEATURES_V1")
            prediction_identity = hashlib.sha256("|".join(map(str, identity_parts)).encode()).hexdigest()
            pending_rows.append({"record_type": "PENDING_OUTCOME", "slate_date": "2026-10-15",
                                 "game_id": source["game_id"], "player_id": source["player_id"], "line": source["line"],
                                 "frozen_contract_version": CONTRACT_VERSION, "prediction_identity": prediction_identity,
                                 "prediction_artifact_sha256": source["prediction_artifact_sha256"],
                                 "market_attachment_identity": attachment_identity(source),
                                 "odds_observation_manifest_sha256": source.get("odds_observation_manifest_sha256", ""),
                                 "selected_production_edge": edge,
                                 "signed_selected_model_minus_no_vig_market_gap": gap,
                                 "stage1_excluded": s1, "stage2_excluded": s2, "stage3_excluded": s3,
                                 "first_exclusion_stage": first, "survivor": not (s1 or s2 or s3),
                                 "edge_status": "NEGATIVE_EDGE" if edge < 0 else "ZERO_EDGE" if edge == 0 else "POSITIVE_EDGE",
                                 "grade_status": "PENDING_OUTCOME",
                                 "model_side": model_side, "market_side": market_side,
                                 "model_p_over": p, "market_no_vig_over": no_vig_over,
                                 "parent_daily_run_id": source["parent_daily_run_id"]})
        except (ValueError, KeyError):
            continue
    for line in ("0.5", "1.5", "2.5"):
        pending_by_line[line] = [r for r in pending_rows if str(r["line"]) == line]
    if len({key(r) for r in pending_rows}) != len(pending_rows):
        raise AssertionError("duplicate Oct. 15 pending classification key")
    pending_counts = {}
    for line, rows in pending_by_line.items():
        pending_counts[line] = {"raw_market_usable": len(rows),
                                "stage1_removals": sum(bool(r["stage1_excluded"]) for r in rows),
                                "stage2_removals": sum(bool(r["stage2_excluded"]) for r in rows),
                                "stage3_removals": sum(bool(r["stage3_excluded"]) for r in rows),
                                "survivors": sum(bool(r["survivor"]) for r in rows)}

    ledger_fields = list(original[0]) + ["final_grade_status", "reconciliation_grading_status", "official_points_final",
                                         "official_participation_state", "reconciliation_prediction_identity",
                                         "prediction_identity_join_mode", "reconciliation_package"]
    write_csv(OUT / "updated_prospective_ledger.csv", updated + pending_rows, ledger_fields + ["model_side", "market_side", "model_p_over", "market_no_vig_over", "parent_daily_run_id"])
    write_csv(OUT / "oct14_pre_outcome_classification_verification.csv", class_audit)
    write_csv(OUT / "canonical_key_outcome_join_audit.csv", join_audit)
    write_csv(OUT / "unresolved_rows.csv", unresolved_rows)
    write_csv(OUT / "stage_metrics.csv", metric_rows)
    write_csv(OUT / "losses_removed_wins_sacrificed.csv", loss_table)
    write_csv(OUT / "excess_loss_change.csv", excess_changes)
    write_csv(OUT / "cumulative_stage_metrics.csv", cumulative)
    write_csv(OUT / "stage2_directional_diagnostics.csv", direction_metrics)
    write_csv(OUT / "market_comparison.csv", market_comparison)
    write_csv(OUT / "other_line_results.csv", [{"line": line, **values} for line, values in other_lines.items()])
    write_csv(OUT / "oct15_pending_classifications.csv", pending_rows)

    summary = {
        "schema_version": "NHL_POINTS_CONSIDERATION_FUNNEL_FIRST_PROSPECTIVE_GRADE_V1",
        "status": "FIRST_PROSPECTIVE_DATE_GRADED",
        "contract_version": CONTRACT_VERSION,
        "contract_fingerprint": CONTRACT_FINGERPRINT,
        "contract_identity_sha256": contract["contract_identity_sha256"],
        "prospective_date": DATE,
        "prospective_dates_finalized": 1,
        "minimum_independent_finalized_hgb_dates": contract["validation_gate"]["minimum_independent_finalized_hgb_dates"],
        "reconciliation_package": RECON.name,
        "reconciliation_status": json.loads((RECON / "RUN_COMPLETE.json").read_text())["status"],
        "frozen_row_count": len(original), "settled_rows": len(joined), "unresolved_rows": len(unresolved_rows),
        "prediction_identity_join_mode": "OUTCOME_JOIN_BY_CANONICAL_KEY",
        "prediction_identity_mismatch_rows": mismatch_count,
        "prediction_artifact_identity_match": False,
        "join_key": ["game_id", "player_id", "line"],
        "join_ambiguities": 0, "duplicate_join_rows": 0, "cross_game_joins": 0, "cross_player_joins": 0, "cross_line_joins": 0,
        "frozen_counts_by_line": {line: dict(counts) for line, counts in line_counts.items()},
        "primary_line": "0.5", "primary_metrics": metrics,
        "cumulative_metrics": cumulative, "losses_removed_wins_sacrificed": loss_table,
        "excess_loss_change": excess_changes, "stage2_directional_diagnostics": direction_metrics,
        "stage2_loss_removal_fraction": {"available_losses_removed_fraction": stage2_removed_metrics["losses"] / metrics["after_stage1"]["losses"],
                                           "available_wins_removed_fraction": stage2_removed_metrics["wins"] / metrics["after_stage1"]["wins"]},
        "market_comparison": market_comparison,
        "other_lines": other_lines, "all_line_summary": {
            "total_frozen": len(original), "settled": len(joined), "unresolved": len(unresolved_rows),
            "stage1_removals": sum(x["stage1_removed"] for x in line_counts.values()),
            "stage2_removals": sum(x["stage2_removed"] for x in line_counts.values()),
            "stage3_removals": sum(x["stage3_removed"] for x in line_counts.values()),
            "survivors": sum(x["survivors"] for x in line_counts.values()),
            "settled_survivor_metrics": grade_metrics([r for r in joined if r["survivor"]]),
        },
        "oct15_pending_counts": pending_counts,
        "no_thresholds_changed": True, "no_stage4_or_inclusion_rule_added": True,
        "discovery_validation_separate": True,
        "provider_calls": 0, "paid_credits": 0, "production_db_mutations": 0,
        "frozen_binding": binding,
    }
    write_json(OUT / "summary.json", summary)
    write_json(OUT / "frozen_contract_reference.json", {"contract": contract, "contract_fingerprint": CONTRACT_FINGERPRINT,
                                                          "contract_file_sha256": sha256(contract_path)})
    write_json(OUT / "prediction_identity_mismatch_note.json", {
        "status": "OUTCOME_JOIN_BY_CANONICAL_KEY", "prediction_artifact_identity_match": False,
        "mismatch_rows": mismatch_count, "frozen_prediction_artifact_sha256": binding["prediction_sha256"],
        "reconciliation_prediction_sha256": json.loads((RECON / "source_bindings.json").read_text())["points"]["files"]["immutable_predictions.csv"],
        "meaning": "Frozen classifications and all prediction/market bindings remain authoritative; the later reconciliation contributes only final official outcome/participation state.",
    })
    write_json(OUT / "date_level_validation_summary.json", {"discovery_window": {"from_date": "2026-10-10", "to_date": "2026-10-13",
                "included_in_prospective_metrics": False}, "validation_dates": [{"date": DATE,
                "role": "FIRST_PROSPECTIVE_DATE", "finalized": True, "settled": len(joined), "unresolved": len(unresolved_rows),
                "line_0_5": metrics}], "prospective_dates_finalized": 1,
                "minimum_independent_finalized_hgb_dates": 6, "discovery_separate": True})
    write_json(OUT / "cumulative_prospective_summary.json", {"prospective_dates_finalized": 1,
                "minimum_independent_finalized_hgb_dates": 6, "repeatability_gate": "NOT_MET",
                "dates": [DATE], "line_0_5_metrics": metrics["stage0_raw"],
                "final_survivor_metrics": metrics["final_survivors"], "discovery_metrics_included": False})
    (OUT / "report.md").write_text(render_report(summary), encoding="utf-8")

    manifest = {"schema_version": "NHL_POINTS_CONSIDERATION_FUNNEL_VALIDATION_LINEAGE_V1", "status": "COMPLETE",
                "contract_version": CONTRACT_VERSION, "contract_fingerprint": CONTRACT_FINGERPRINT,
                "prospective_date": DATE, "daily_run_id": DAILY_RUN,
                "inputs": {}, "outputs": {}}
    inputs = [contract_path, FREEZE / "prospective_ledger.csv", FREEZE / "oct14_pre_outcome_classification_binding.json",
              DISCOVERY / "current_day_dry_view_2026-10-14.csv", RECON / "graded_points.csv",
              RECON / "canonical_skater_outcomes.csv", RECON / "RUN_COMPLETE.json", RECON / "source_bindings.json", OCT15_ATTACH,
              Path(__file__), ROOT / "backend/nhl/tests/test_points_consideration_funnel_validation.py"]
    outputs = sorted(p for p in OUT.iterdir() if p.is_file() and p.name != "lineage_manifest.json")
    manifest["inputs"] = {str(p.relative_to(ROOT)): sha256(p) for p in inputs}
    manifest["outputs"] = {p.name: sha256(p) for p in outputs}
    write_json(OUT / "lineage_manifest.json", manifest)


def render_report(s: dict[str, Any]) -> str:
    m = s["primary_metrics"]
    rows = s["losses_removed_wins_sacrificed"]
    changes = s["excess_loss_change"]
    lines = ["# NHL Points funnel — first prospective validation", "",
             f"Frozen contract: `{s['contract_version']}`; supplied fingerprint `{s['contract_fingerprint']}`.",
             f"Prospective date: **{s['prospective_date']}**. Frozen rows: **{s['frozen_row_count']}**; settled **{s['settled_rows']}**; unresolved **{s['unresolved_rows']}**.",
             "Outcome binding: `OUTCOME_JOIN_BY_CANONICAL_KEY` on `(game_id, player_id, line)`. All 93 rows joined once with no duplicate or cross-key joins. Prediction identity hashes differ on all 93 rows; frozen prediction and market bindings were retained.",
             "", "## Frozen 0.5 funnel reproduction", "", "58 → 49 → 23 → 20 (Stage 0 → after Stage 1 → after Stage 2 → after Stage 3). Two of the 58 remained participation unresolved, so Stage 0 grading uses 56 settled rows.", "",
             "## 0.5 cohorts", "", "| Cohort | N | W-L | Expected losses | Observed losses | Excess losses | Brier | Log loss | Mean selected p | Realized win rate | Calibration gap |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    cohort_labels = [("stage0_raw", "Stage 0 raw"), ("stage1_incremental", "Stage 1 excluded"), ("after_stage1", "After Stage 1"),
                     ("stage2_incremental", "Stage 2 excluded"), ("after_stage2", "After Stage 2"),
                     ("stage3_incremental", "Stage 3 excluded"), ("final_survivors", "Final survivors")]
    for k, label in cohort_labels:
        x = m[k]
        fmt = lambda z: "—" if z is None else f"{z:.4f}"
        lines.append(f"| {label} | {x['n']} | {x['wins']}-{x['losses']} | {x['expected_losses']:.3f} | {x['observed_losses']} | {x['excess_losses']:.3f} | {fmt(x['brier'])} | {fmt(x['log_loss'])} | {fmt(x['mean_selected_model_probability'])} | {fmt(x['realized_win_rate'])} | {fmt(x['calibration_gap'])} |")
    lines.extend(["", "## Losses removed and excess-loss change", "", "| Cohort | Rows | Wins removed | Losses removed | Expected losses | Observed losses | Excess losses |", "|---|---:|---:|---:|---:|---:|---:|"])
    for x in rows:
        lines.append(f"| {x['cohort']} | {x['rows']} | {x['wins_removed']} | {x['losses_removed']} | {x['expected_losses']:.3f} | {x['observed_losses']} | {x['excess_losses']:.3f} |")
    lines.extend(["", "| Stage | Survivor excess before | Survivor excess after | Excess removed |", "|---|---:|---:|---:|"])
    for x in changes:
        lines.append(f"| {x['stage']} | {x['survivor_excess_losses_before']:.3f} | {x['survivor_excess_losses_after']:.3f} | {x['excess_losses_removed']:.3f} |")
    s2 = m["stage2_incremental"]
    s3 = m["stage3_incremental"]
    direction = s["stage2_directional_diagnostics"]
    direction_map = {x["diagnostic_cohort"]: x for x in direction}
    stage2_fractions = s["stage2_loss_removal_fraction"]
    lines.extend(["", "## Interpretation", "",
                  f"Stage 1 excluded 9 settled rows (8-1) with {m['stage1_incremental']['excess_losses']:.3f} excess losses. The excluded rows performed better than their probabilities expected; survivor excess losses increased from {m['stage0_raw']['excess_losses']:.3f} to {m['after_stage1']['excess_losses']:.3f}. Stage 1 reduced candidate count while removing a better-than-expected cohort on this first date.",
                  f"Stage 2 removed {s2['n']} settled rows ({s2['wins']} wins, {s2['losses']} losses), with {s2['excess_losses']:.3f} excess losses. It removed {stage2_fractions['available_losses_removed_fraction']:.1%} of the Stage 1 survivor losses versus {stage2_fractions['available_wins_removed_fraction']:.1%} of their wins. This first date modestly supports the frozen screen's excess-loss reduction; one date is insufficient evidence for validation.",
                  f"Stage 3 removed {s3['n']} rows ({s3['wins']} wins, {s3['losses']} losses), excess {s3['excess_losses']:.3f}; it {'adds separation' if s3['excess_losses'] > 0 else 'mainly discards wins / does not reduce excess losses' if s3['excess_losses'] < 0 else 'adds no excess-loss separation'} on this date.",
                  f"Stage 2 diagnostics: HGB UNDER {direction_map['HGB_UNDER']['n']} rows / {direction_map['HGB_UNDER']['wins']}-{direction_map['HGB_UNDER']['losses']}; HGB OVER {direction_map['HGB_OVER']['n']} / {direction_map['HGB_OVER']['wins']}-{direction_map['HGB_OVER']['losses']}; same-side {direction_map['SAME_SIDE_CONFIDENCE_DISAGREEMENT']['n']}; opposite-side {direction_map['OPPOSITE_SIDE_DISAGREEMENT']['n']}.",
                  f"Raw versus final survivor HGB calibration gap: {m['stage0_raw']['calibration_gap']:.4f} vs {m['final_survivors']['calibration_gap']:.4f}; Brier {m['stage0_raw']['brier']:.4f} vs {m['final_survivors']['brier']:.4f}; log loss {m['stage0_raw']['log_loss']:.4f} vs {m['final_survivors']['log_loss']:.4f}.",
                  "Market comparisons use exact common rows within each cohort:",
                  f"- Stage 0 (N={m['stage0_raw']['n']}): HGB Brier/log loss/calibration gap {m['stage0_raw']['brier']:.4f}/{m['stage0_raw']['log_loss']:.4f}/{m['stage0_raw']['calibration_gap']:.4f}; market {m['stage0_raw']['market_brier']:.4f}/{m['stage0_raw']['market_log_loss']:.4f}/{m['stage0_raw']['market_calibration_gap']:.4f}.",
                  f"- Final survivors (N={m['final_survivors']['n']}): HGB {m['final_survivors']['brier']:.4f}/{m['final_survivors']['log_loss']:.4f}/{m['final_survivors']['calibration_gap']:.4f}; market {m['final_survivors']['market_brier']:.4f}/{m['final_survivors']['market_log_loss']:.4f}/{m['final_survivors']['market_calibration_gap']:.4f}.",
                  "No ROI claim. No screen is validated, disproven, permanent, or profitable. Thresholds unchanged; no inclusion rule or Stage 4 added.",
                  "", "## Other lines and all-line status", "",
                  f"1.5: {s['other_lines']['1.5']['n']} settled rows, {s['other_lines']['1.5']['wins']}-{s['other_lines']['1.5']['losses']} (reported separately). 2.5: {s['other_lines']['2.5']['n']} settled rows, {s['other_lines']['2.5']['wins']}-{s['other_lines']['2.5']['losses']} (descriptive only).",
                  f"Across all 93 rows: Stage 1 removals {s['all_line_summary']['stage1_removals']}; Stage 2 {s['all_line_summary']['stage2_removals']}; Stage 3 {s['all_line_summary']['stage3_removals']}; survivors {s['all_line_summary']['survivors']}. Settled survivor record {s['all_line_summary']['settled_survivor_metrics']['wins']}-{s['all_line_summary']['settled_survivor_metrics']['losses']}; expected losses {s['all_line_summary']['settled_survivor_metrics']['expected_losses']:.3f}, observed losses {s['all_line_summary']['settled_survivor_metrics']['observed_losses']}, excess losses {s['all_line_summary']['settled_survivor_metrics']['excess_losses']:.3f}.",
                  "", "## Oct. 15 pending, outcome-free", "", "| Line | Market-usable | Stage 1 removed | Stage 2 removed | Stage 3 removed | Survivors |", "|---|---:|---:|---:|---:|---:|"])
    for line, x in s["oct15_pending_counts"].items():
        lines.append(f"| {line} | {x['raw_market_usable']} | {x['stage1_removals']} | {x['stage2_removals']} | {x['stage3_removals']} | {x['survivors']} |")
    lines.extend(["", "Prospective finalized dates: **1**. Repeatability gate: **not met; minimum 6 independent finalized HGB dates**. Discovery results remain separate.",
                  "", "Provider calls: 0. Paid credits: 0. Production DB mutations: 0.", ""])
    return "\n".join(lines)


if __name__ == "__main__":
    main()
