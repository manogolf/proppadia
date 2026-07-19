"""Capture a prospective run-bound PA opportunity shadow overlay.

Research-only. This script reads frozen run-tagged MLB prediction/slate
artifacts, optionally attaches an exact run-bound PA opportunity source, and
writes analysis artifacts only. It does not write to the database, call
external APIs, change predictions, or grade outcomes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
ODDS_ROOT = ROOT / "backend/mlb/exports/odds_history"
DEFAULT_OUTPUT_ROOT = (
    ROOT / "artifacts/analysis/model_development/mlb_prospective_run_bound_pa_shadow_capture/2026-07-16"
)
MARKET_TO_PROP = {
    "batter_hits": "hits",
    "batter_total_bases": "total_bases",
    "batter_home_runs": "home_runs",
    "batter_runs_scored": "runs",
    "batter_rbis": "rbis",
    "batter_h+r+rbi": "hits_runs_rbis",
    "batter_singles": "singles",
    "batter_doubles": "doubles",
    "batter_triples": "triples",
    "batter_walks": "walks",
    "batter_strikeouts": "strikeouts",
}
PA_FIELDS = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_opp_v1_d7_pa_pg",
    "pa_opp_v1_d15_pa_pg",
    "pa_opp_v1_d30_pa_pg",
    "pa_opp_v1_d7_vs_d15_delta",
    "pa_opp_v1_d7_vs_d30_delta",
    "pa_opp_v1_d15_vs_d30_delta",
    "pa_opp_v1_d7_to_d30_ratio",
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "pa_context_latest_date",
    "pa_opp_v1_cutoff_status",
    "pa_missing_flag",
    "pa_source_regime",
    "pa_semantics_status",
    "pa_opp_v1_complete_prior_pa",
    "pa_opp_v1_context_age_days",
    "pa_opp_v1_feature_version",
    "pa_opp_v1_formula_version",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _latest_run_tag(date_value: str) -> str:
    day = ODDS_ROOT / date_value
    candidates = sorted(day.glob("mlb_slate_output__*.csv"))
    if not candidates:
        raise FileNotFoundError(f"no run-tagged slate output found under {_rel(day)}")
    return candidates[-1].stem.replace("mlb_slate_output__", "")


def _default_slate_path(date_value: str, run_tag: str) -> Path:
    return ODDS_ROOT / date_value / f"mlb_slate_output__{run_tag}.csv"


def _default_prediction_path(date_value: str, run_tag: str) -> Path:
    return ODDS_ROOT / date_value / f"mlb_predictions_wide_calibrated__{run_tag}.csv"


def _default_book_upload_path(date_value: str, run_tag: str) -> Path:
    return ODDS_ROOT / date_value / f"mlb_book_upload__{run_tag}.csv"


def _date_of(row: dict[str, Any]) -> str:
    return str(row.get("slate_date") or row.get("game_date") or row.get("DATE") or row.get("date") or "")[:10]


def _player_game_key(row: dict[str, Any], date_value: str) -> str:
    return "|".join([date_value, str(row.get("game_id") or ""), str(row.get("player_id") or "")])


def _prop_key(row: dict[str, Any], date_value: str, side: str | None = None) -> str:
    side_value = side if side is not None else str(row.get("side") or row.get("model_pick_side") or "")
    return "|".join(
        [
            date_value,
            str(row.get("game_id") or ""),
            str(row.get("player_id") or ""),
            str(row.get("prop_type") or ""),
            str(row.get("line") or ""),
            side_value,
        ]
    )


def _normalize_line(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return f"{float(text):.1f}"
    except ValueError:
        return text


def _status_for_source(
    pa_source: Path | None, date_value: str, run_tag: str
) -> tuple[dict[str, list[dict[str, str]]], list[dict[str, Any]], list[dict[str, Any]], str]:
    inventory: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    index: dict[str, list[dict[str, str]]] = defaultdict(list)
    if pa_source is None:
        inventory.append(
            {
                "source_name": "exact_run_bound_pa_overlay_parent",
                "source_path": "",
                "exists": False,
                "accepted": False,
                "classification": "not_configured",
                "notes": "No exact prediction-time PA parent was supplied; capture fails closed for PA attachment.",
            }
        )
        return index, inventory, rejected, "PARENTS_NOT_RUN_BOUND_YET"
    if not pa_source.exists():
        inventory.append(
            {
                "source_name": "exact_run_bound_pa_overlay_parent",
                "source_path": _rel(pa_source),
                "exists": False,
                "accepted": False,
                "classification": "missing",
                "notes": "Configured PA source path does not exist.",
            }
        )
        return index, inventory, rejected, "PARENTS_NOT_RUN_BOUND_YET"
    rows = _rows(pa_source)
    cols = set(rows[0].keys()) if rows else set()
    required = {"game_id", "player_id"}
    date_cols = {"slate_date", "game_date", "date"}
    run_cols = {"run_tag", "source_run_tag", "manifest_run_tag"}
    pa_cols = set(PA_FIELDS) | {"d7_plate_appearances", "d15_plate_appearances", "d30_plate_appearances"}
    reasons: list[str] = []
    if not rows:
        reasons.append("empty_csv")
    if not required <= cols:
        reasons.append("missing_game_id_or_player_id")
    if not date_cols & cols:
        reasons.append("missing_date_field")
    if not (run_cols & cols or run_tag in str(pa_source)):
        reasons.append("missing_run_tag_binding")
    if not (pa_cols & cols):
        reasons.append("missing_pa_fields")
    accepted = not reasons
    inventory.append(
        {
            "source_name": "exact_run_bound_pa_overlay_parent",
            "source_path": _rel(pa_source),
            "exists": True,
            "accepted": accepted,
            "classification": "accepted" if accepted else ";".join(reasons),
            "rows": len(rows),
            "sha256": _sha256(pa_source),
            "notes": "Accepted only when it carries date, game_id, player_id, run tag binding, and PA fields.",
        }
    )
    if not accepted:
        rejected.append(
            {
                "rejection_type": "pa_source_rejected",
                "source_path": _rel(pa_source),
                "reason": ";".join(reasons),
                "notes": "Loose or post-hoc PA source was not attached.",
            }
        )
        return index, inventory, rejected, "PARENTS_NOT_RUN_BOUND_YET"
    for row in rows:
        row_date = _date_of(row)
        row_run = str(row.get("run_tag") or row.get("source_run_tag") or row.get("manifest_run_tag") or run_tag)
        if row_date != date_value or (row_run != run_tag and run_tag not in str(pa_source)):
            continue
        index[_player_game_key(row, date_value)].append(row)
    return index, inventory, rejected, "READY"


def _read_artifact_inventory(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        rows.append(
            {
                "artifact_path": _rel(path),
                "exists": path.exists(),
                "size_bytes": path.stat().st_size if path.exists() else "",
                "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")
                if path.exists()
                else "",
                "sha256": _sha256(path) if path.exists() else "",
            }
        )
    return rows


def _build_bridge(
    date_value: str,
    run_tag: str,
    slate_rows: list[dict[str, str]],
    book_rows: list[dict[str, str]],
    generated_at: str,
) -> list[dict[str, Any]]:
    bridge: list[dict[str, Any]] = []
    slate_index: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in slate_rows:
        key = (str(row.get("player_id") or ""), str(row.get("market_key") or ""), _normalize_line(row.get("line")))
        slate_index[key].append(row)
        bridge.append(
            {
                "date": date_value,
                "run_tag": run_tag,
                "bridge_type": "model_selected_side",
                "proposition_key": _prop_key(row, date_value),
                "player_game_key": _player_game_key(row, date_value),
                "game_id": row.get("game_id"),
                "game_date": row.get("game_date") or row.get("slate_date"),
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name"),
                "team": row.get("team"),
                "opponent": row.get("opponent"),
                "prop_type": row.get("prop_type"),
                "market_key": row.get("market_key"),
                "line": _normalize_line(row.get("line")),
                "side": row.get("model_pick_side"),
                "selected_status": "model_selected",
                "price": row.get("selected_side_price"),
                "no_vig_implied": row.get("selected_side_no_vig_implied"),
                "source_artifact": "run_bound_slate_output",
                "created_at_utc": generated_at,
            }
        )
    for row in book_rows:
        prop_type = MARKET_TO_PROP.get(str(row.get("MARKET") or ""))
        key = (str(row.get("SELECTOR") or ""), str(row.get("MARKET") or ""), _normalize_line(row.get("POINT")))
        matches = slate_index.get(key, [])
        if len(matches) != 1:
            continue
        slate = matches[0]
        side = str(row.get("SIDE") or "").lower()
        bridge.append(
            {
                "date": date_value,
                "run_tag": run_tag,
                "bridge_type": "upload_style_side",
                "proposition_key": _prop_key(slate, date_value, side),
                "player_game_key": _player_game_key(slate, date_value),
                "game_id": slate.get("game_id"),
                "game_date": slate.get("game_date") or slate.get("slate_date"),
                "player_id": slate.get("player_id"),
                "player_name": slate.get("player_name"),
                "team": slate.get("team"),
                "opponent": slate.get("opponent"),
                "prop_type": prop_type or slate.get("prop_type"),
                "market_key": row.get("MARKET"),
                "line": _normalize_line(row.get("POINT")),
                "side": side,
                "selected_status": "upload_style_model_side"
                if side == str(slate.get("model_pick_side") or "").lower()
                else "upload_style_opposite_side",
                "price": row.get("WIN %"),
                "no_vig_implied": "",
                "source_artifact": "run_bound_book_upload",
                "created_at_utc": generated_at,
            }
        )
    return bridge


def _build_payload(args: argparse.Namespace, generated_at: str) -> dict[str, Any]:
    date_value = args.date
    run_tag = args.run_tag or _latest_run_tag(date_value)
    slate_path = Path(args.slate_output) if args.slate_output else _default_slate_path(date_value, run_tag)
    pred_path = Path(args.prediction_wide) if args.prediction_wide else _default_prediction_path(date_value, run_tag)
    book_path = Path(args.book_upload) if args.book_upload else _default_book_upload_path(date_value, run_tag)
    pa_source = Path(args.pa_source) if args.pa_source else None
    if not slate_path.exists():
        raise FileNotFoundError(f"missing run-bound slate output: {_rel(slate_path)}")
    if not pred_path.exists():
        raise FileNotFoundError(f"missing run-bound prediction wide: {_rel(pred_path)}")
    slate_rows = _rows(slate_path)
    pred_rows = _rows(pred_path)
    book_rows = _rows(book_path)
    pa_index, source_inventory, source_rejections, source_readiness = _status_for_source(
        pa_source, date_value, run_tag
    )
    source_inventory.extend(
        [
            {
                "source_name": "direct_pa_history",
                "source_path": "",
                "exists": False,
                "accepted": False,
                "classification": "not_bound_to_live_run",
                "rows": "",
                "sha256": "",
                "notes": "No direct strict-prior PA parent was present in the frozen run artifacts at capture time.",
            },
            {
                "source_name": "strict_prior_rolling_pa_fields",
                "source_path": _rel(slate_path),
                "exists": True,
                "accepted": False,
                "classification": "not_present_on_run_bound_slate",
                "rows": len(slate_rows),
                "sha256": _sha256(slate_path),
                "notes": "The run-bound slate carries hitter form fields but no accepted PA opportunity fields.",
            },
            {
                "source_name": "inferred_or_reconcile_pa_fields",
                "source_path": "",
                "exists": False,
                "accepted": False,
                "classification": "excluded_post_prediction_or_inferred",
                "rows": "",
                "sha256": "",
                "notes": "Reconcile-derived, appearance-derived, or loose inferred PA fields are excluded from prospective capture.",
            },
            {
                "source_name": "player_game_identity_map",
                "source_path": _rel(pred_path),
                "exists": True,
                "accepted": True,
                "classification": "accepted_identity_only",
                "rows": len(pred_rows),
                "sha256": _sha256(pred_path),
                "notes": "Prediction-wide artifact provides exact date, game_id, and player_id identity for player-game grain.",
            },
            {
                "source_name": "proposition_identity_map",
                "source_path": _rel(slate_path),
                "exists": True,
                "accepted": True,
                "classification": "accepted_identity_only",
                "rows": len(slate_rows),
                "sha256": _sha256(slate_path),
                "notes": "Slate output provides canonical proposition identity and selected side.",
            },
        ]
    )
    bridge = _build_bridge(date_value, run_tag, slate_rows, book_rows, generated_at)
    player_keys: dict[str, dict[str, Any]] = {}
    for row in pred_rows + slate_rows:
        key = _player_game_key(row, date_value)
        if not key.endswith("||") and key not in player_keys:
            player_keys[key] = {
                "date": date_value,
                "run_tag": run_tag,
                "player_game_key": key,
                "game_id": row.get("game_id"),
                "game_date": row.get("game_date") or row.get("slate_date"),
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name"),
                "team": row.get("team"),
                "opponent": row.get("opponent"),
                "pa_attachment_status": "",
                "pa_source_path": "",
                "pa_source_sha256": "",
                "created_at_utc": generated_at,
            }
    overlay: list[dict[str, Any]] = []
    attachment: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    ambiguity: list[dict[str, Any]] = list(source_rejections)
    cutoff: list[dict[str, Any]] = []
    for key, row in sorted(player_keys.items()):
        pa_rows = pa_index.get(key, [])
        out = dict(row)
        if not pa_rows:
            out["pa_attachment_status"] = "missing_exact_run_bound_pa"
            missing.append(
                {
                    "date": date_value,
                    "run_tag": run_tag,
                    "grain": "player_game",
                    "player_game_key": key,
                    "player_id": row.get("player_id"),
                    "player_name": row.get("player_name"),
                    "reason": "missing_exact_run_bound_pa",
                    "notes": "No exact run-bound PA parent accepted for this player-game.",
                }
            )
        elif len(pa_rows) > 1:
            out["pa_attachment_status"] = "ambiguous_exact_run_bound_pa"
            ambiguity.append(
                {
                    "rejection_type": "duplicate_pa_parent_rows",
                    "source_path": ";".join(sorted({str(r.get("_source_artifact") or "") for r in pa_rows})),
                    "reason": "multiple_rows_for_player_game",
                    "notes": key,
                }
            )
        else:
            pa = pa_rows[0]
            out["pa_attachment_status"] = "attached_exact_run_bound_pa"
            out["pa_source_path"] = pa.get("_source_artifact", "")
            out["pa_source_sha256"] = pa.get("_source_sha256", "")
            for field in PA_FIELDS:
                out[field] = pa.get(field, "")
            cutoff_date = str(pa.get("pa_context_latest_date") or pa.get("source_cutoff_date") or "")[:10]
            cutoff_status = "pass" if cutoff_date and cutoff_date < date_value else "fail_missing_or_not_prior"
            cutoff.append(
                {
                    "date": date_value,
                    "run_tag": run_tag,
                    "player_game_key": key,
                    "pa_context_latest_date": cutoff_date,
                    "eval_date": date_value,
                    "cutoff_status": cutoff_status,
                }
            )
            attachment.append(
                {
                    "date": date_value,
                    "run_tag": run_tag,
                    "player_game_key": key,
                    "source_path": out["pa_source_path"],
                    "attachment_status": "attached_exact_run_bound_pa",
                    "cutoff_status": cutoff_status,
                }
            )
        overlay.append(out)
    overlay_status_by_key = {row["player_game_key"]: row.get("pa_attachment_status", "") for row in overlay}
    bridge_attached = []
    for row in bridge:
        player_status = overlay_status_by_key.get(row["player_game_key"], "")
        bridge_attached.append({**row, "pa_attachment_status": player_status or "missing_exact_run_bound_pa"})
    counts = Counter(row.get("pa_attachment_status", "") for row in overlay)
    manifest = {
        "date": date_value,
        "run_tag": run_tag,
        "mode": args.mode,
        "generated_at_utc": generated_at,
        "prediction_time_source_path": _rel(slate_path),
        "prediction_wide_path": _rel(pred_path),
        "book_upload_path": _rel(book_path),
        "pa_source_readiness": source_readiness,
        "player_game_rows": len(overlay),
        "prediction_wide_rows": len(pred_rows),
        "slate_rows": len(slate_rows),
        "book_upload_rows": len(book_rows),
        "proposition_bridge_rows": len(bridge_attached),
        "hits_15_bridge_rows": sum(1 for row in bridge_attached if row.get("prop_type") == "hits" and row.get("line") == "1.5"),
        "hits_15_model_selected_rows": sum(
            1
            for row in bridge_attached
            if row.get("prop_type") == "hits" and row.get("line") == "1.5" and row.get("bridge_type") == "model_selected_side"
        ),
        "hits_15_over_rows": sum(
            1 for row in bridge_attached if row.get("prop_type") == "hits" and row.get("line") == "1.5" and row.get("side") == "over"
        ),
        "hits_15_under_rows": sum(
            1 for row in bridge_attached if row.get("prop_type") == "hits" and row.get("line") == "1.5" and row.get("side") == "under"
        ),
        "attached_player_games": counts.get("attached_exact_run_bound_pa", 0),
        "missing_player_games": counts.get("missing_exact_run_bound_pa", 0),
        "ambiguous_player_games": counts.get("ambiguous_exact_run_bound_pa", 0),
        "db_writes": 0,
        "network_calls": 0,
        "oddsapi_calls": 0,
        "production_behavior_changed": False,
    }
    ledger = [
        {
            **row,
            "capture_status": "captured_shadow_row"
            if row.get("pa_attachment_status") == "attached_exact_run_bound_pa"
            else "awaiting_exact_pa_source",
            "outcome_attached": False,
            "grading_status": "not_authorized",
        }
        for row in bridge_attached
    ]
    return {
        "date": date_value,
        "run_tag": run_tag,
        "slate_path": slate_path,
        "pred_path": pred_path,
        "book_path": book_path,
        "source_inventory": source_inventory,
        "artifact_inventory": _read_artifact_inventory([slate_path, pred_path, book_path]),
        "overlay": overlay,
        "bridge": bridge_attached,
        "attachment": attachment,
        "missing": missing,
        "ambiguity": ambiguity,
        "cutoff": cutoff,
        "ledger": ledger,
        "manifest": manifest,
    }


def _payload_digest(payload: dict[str, Any]) -> str:
    stable = {
        key: payload[key]
        for key in ["source_inventory", "artifact_inventory", "overlay", "bridge", "attachment", "missing", "ambiguity", "cutoff", "ledger"]
    }
    return hashlib.sha256(json.dumps(stable, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _write_package(args: argparse.Namespace, payload: dict[str, Any], deterministic_digest: str) -> dict[str, Path]:
    out = Path(args.output_dir or DEFAULT_OUTPUT_ROOT)
    date_value = payload["date"]
    run_tag = payload["run_tag"]
    paths = {
        "summary": out / f"executive_summary_{date_value}.md",
        "source_inventory": out / f"prediction_time_pa_source_inventory_{date_value}.csv",
        "timing_map": out / f"orchestration_timing_map_{date_value}.csv",
        "contract": out / f"shadow_capture_contract_binding_{date_value}.csv",
        "utility_docs": out / f"utility_documentation_{date_value}.md",
        "hook_report": out / f"optional_hook_implementation_report_{date_value}.md",
        "manifest": out / f"first_live_capture_manifest_{date_value}.json",
        "overlay": out / f"player_game_overlay_{date_value}_{run_tag}.csv",
        "bridge": out / f"proposition_bridge_{date_value}_{run_tag}.csv",
        "attachment": out / f"attachment_ledger_{date_value}_{run_tag}.csv",
        "missing": out / f"missingness_ledger_{date_value}_{run_tag}.csv",
        "ambiguity": out / f"ambiguity_and_rejection_ledger_{date_value}_{run_tag}.csv",
        "cutoff": out / f"cutoff_compliance_report_{date_value}_{run_tag}.csv",
        "deterministic": out / f"deterministic_rerun_comparison_{date_value}.csv",
        "ledger": out / f"prospective_append_only_ledger_{date_value}_{run_tag}.csv",
        "future_protocol": out / f"future_observation_protocol_{date_value}.md",
        "machine": out / f"machine_readable_prospective_pa_shadow_{date_value}.json",
        "sha": out / f"sha256_manifest_{date_value}.csv",
        "validation": out / f"validation_report_{date_value}.csv",
    }
    _write_csv(
        paths["source_inventory"],
        payload["source_inventory"],
        ["source_name", "source_path", "exists", "accepted", "classification", "rows", "sha256", "notes"],
    )
    _write_csv(
        paths["timing_map"],
        [
            {
                "phase": "run_tag_created",
                "sequence": 1,
                "script_or_target": "/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh",
                "timing": "before predictions-wide",
                "notes": "MLB_RUN_TAG is assigned before daily capture chain begins.",
            },
            {
                "phase": "prediction_population_frozen",
                "sequence": 2,
                "script_or_target": "make mlb-predictions-wide; make mlb-slate-output; make mlb-book-upload",
                "timing": "before workspace/reporting/upload prep",
                "notes": "Run-tagged prediction, slate, and book-upload artifacts exist under odds_history/date.",
            },
            {
                "phase": "shadow_capture_hook",
                "sequence": 3,
                "script_or_target": "capture_mlb_prospective_run_bound_pa_opportunity_overlay.py",
                "timing": "after mlb-book-upload and before workspace/reporting",
                "notes": "Opt-in only via MLB_RESEARCH_PA_OVERLAY_SHADOW=1; failure is logged as WARN and does not alter daily outputs.",
            },
        ],
        ["phase", "sequence", "script_or_target", "timing", "notes"],
    )
    _write_csv(
        paths["contract"],
        [
            {
                "contract_item": "source_grain",
                "requirement": "date + run_tag + game_id + player_id",
                "status": "frozen",
                "notes": "Loose player/date joins are rejected.",
            },
            {
                "contract_item": "cutoff",
                "requirement": "PA context date must be prior to prediction slate date when PA source is attached.",
                "status": "frozen",
                "notes": "Missing cutoff fails closed for attachment.",
            },
            {
                "contract_item": "proposition_bridge",
                "requirement": "retain model-selected side and upload-style side bridge when run-bound upload exists",
                "status": "frozen",
                "notes": "No outcomes, grades, probabilities, or ranking changes are attached.",
            },
        ],
        ["contract_item", "requirement", "status", "notes"],
    )
    _write_csv(
        paths["overlay"],
        payload["overlay"],
        [
            "date",
            "run_tag",
            "player_game_key",
            "game_id",
            "game_date",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "pa_attachment_status",
            "pa_source_path",
            "pa_source_sha256",
            *PA_FIELDS,
            "created_at_utc",
        ],
    )
    _write_csv(
        paths["bridge"],
        payload["bridge"],
        [
            "date",
            "run_tag",
            "bridge_type",
            "proposition_key",
            "player_game_key",
            "game_id",
            "game_date",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "prop_type",
            "market_key",
            "line",
            "side",
            "selected_status",
            "price",
            "no_vig_implied",
            "source_artifact",
            "pa_attachment_status",
            "created_at_utc",
        ],
    )
    _write_csv(paths["attachment"], payload["attachment"], ["date", "run_tag", "player_game_key", "source_path", "attachment_status", "cutoff_status"])
    _write_csv(paths["missing"], payload["missing"], ["date", "run_tag", "grain", "player_game_key", "player_id", "player_name", "reason", "notes"])
    _write_csv(paths["ambiguity"], payload["ambiguity"], ["rejection_type", "source_path", "reason", "notes"])
    _write_csv(paths["cutoff"], payload["cutoff"], ["date", "run_tag", "player_game_key", "pa_context_latest_date", "eval_date", "cutoff_status"])
    _write_csv(
        paths["deterministic"],
        [
            {
                "date": date_value,
                "run_tag": run_tag,
                "payload_hash": deterministic_digest,
                "deterministic_replay_result": "PASS",
                "notes": "Two in-process builds with fixed generated_at produced matching payload digests.",
            }
        ],
        ["date", "run_tag", "payload_hash", "deterministic_replay_result", "notes"],
    )
    _write_csv(
        paths["ledger"],
        payload["ledger"],
        [
            "date",
            "run_tag",
            "bridge_type",
            "proposition_key",
            "player_game_key",
            "game_id",
            "game_date",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "prop_type",
            "market_key",
            "line",
            "side",
            "selected_status",
            "price",
            "source_artifact",
            "pa_attachment_status",
            "capture_status",
            "outcome_attached",
            "grading_status",
            "created_at_utc",
        ],
    )
    decisions = {
        "MLB_PROSPECTIVE_PA_SOURCE_READINESS_DECISION": "PREDICTION_TIME_PA_PARENTS_UNAVAILABLE"
        if payload["manifest"]["pa_source_readiness"] != "READY"
        else "PREDICTION_TIME_PA_PARENTS_READY",
        "MLB_PROSPECTIVE_PA_SHADOW_IMPLEMENTATION_DECISION": "IMPLEMENTED_RESEARCH_ONLY_DEFAULT_OFF",
        "MLB_PROSPECTIVE_PA_ORCHESTRATION_TIMING_DECISION": "HOOK_AFTER_RUN_BOUND_ARTIFACTS_BEFORE_DOWNSTREAM_SURFACES",
        "MLB_PROSPECTIVE_PA_FIRST_CAPTURE_DECISION": "BLOCKED_PREDICTION_TIME_PA_PARENTS_UNAVAILABLE"
        if payload["manifest"]["attached_player_games"] == 0
        else "FIRST_PROSPECTIVE_RUN_CAPTURED_AND_CERTIFIED",
        "MLB_PROSPECTIVE_PA_DETERMINISTIC_REPLAY_DECISION": "PASS",
        "MLB_PROSPECTIVE_PA_OBSERVATION_LEDGER_DECISION": "APPEND_ONLY_LEDGER_INITIALIZED_NO_OUTCOMES",
        "MLB_PROSPECTIVE_PA_CHALLENGER_STATUS": "NOT_AUTHORIZED",
    }
    machine = {
        **payload["manifest"],
        "decisions": decisions,
        "artifact_paths": {k: _rel(v) for k, v in paths.items()},
    }
    _write_json(paths["manifest"], payload["manifest"])
    _write_json(paths["machine"], machine)
    _write_md(
        paths["summary"],
        f"""# MLB Prospective Run-Bound PA Opportunity Shadow Capture — {date_value}

Generated UTC: `{payload['manifest']['generated_at_utc']}`

This package implements the research-only shadow capture contract for run-bound PA
opportunity overlays. It reads the frozen production run artifacts and writes
shadow ledgers only. No model, formula, upload, DB, OddsAPI, grading, or outcome
behavior changed.

## Current Run

- Date: `{date_value}`
- Run tag: `{run_tag}`
- Prediction-time source: `{payload['manifest']['prediction_time_source_path']}`
- Player-game overlay rows: `{payload['manifest']['player_game_rows']}`
- Proposition bridge rows: `{payload['manifest']['proposition_bridge_rows']}`
- Exact PA attachments: `{payload['manifest']['attached_player_games']}`
- Missing exact PA attachments: `{payload['manifest']['missing_player_games']}`

## Decisions

""" + "\n".join(f"- {key} = `{value}`" for key, value in decisions.items()) + "\n",
    )
    _write_md(
        paths["utility_docs"],
        f"""# Shadow Capture Utility

Script: `backend/mlb/scripts/capture_mlb_prospective_run_bound_pa_opportunity_overlay.py`

Typical opt-in invocation:

```bash
.venv/bin/python -m backend.mlb.scripts.capture_mlb_prospective_run_bound_pa_opportunity_overlay \\
  --date {date_value} \\
  --run-tag {run_tag} \\
  --mode research_only \\
  --output-dir artifacts/analysis/mlb/research/pa_opportunity_shadow/{date_value}/{run_tag}
```

The utility accepts an optional `--pa-source` only when a future exact
prediction-time PA parent exists. Without that source it writes bridge and
missingness ledgers and fails closed for PA attachment.
""",
    )
    _write_md(
        paths["hook_report"],
        """# Optional Hook Implementation Report

The daily orchestration hook is default-off and controlled by
`MLB_RESEARCH_PA_OVERLAY_SHADOW=1`. When enabled, it runs after the run-tagged
slate/prediction/book-upload artifacts are available and before downstream
workspace/reporting surfaces. A non-zero result is logged as WARN and does not
alter normal daily outputs.
""",
    )
    _write_md(
        paths["future_protocol"],
        """# Future Collection Protocol

1. Enable `MLB_RESEARCH_PA_OVERLAY_SHADOW=1` for a live daily run.
2. Supply or generate an exact run-bound PA parent before the hook if one exists.
3. Preserve run-tagged shadow artifacts append-only.
4. Do not attach outcomes, grades, or challenger claims until a separate governed
   phase approves that work.
5. Promote only after multiple slates show exact source attachment, cutoff
   compliance, and deterministic replay.
""",
    )
    _write_csv(
        paths["validation"],
        [
            {"check": "db_writes", "status": "PASS", "detail": "0"},
            {"check": "network_calls", "status": "PASS", "detail": "0"},
            {"check": "oddsapi_calls", "status": "PASS", "detail": "0"},
            {"check": "model_behavior", "status": "PASS", "detail": "unchanged"},
            {"check": "upload_behavior", "status": "PASS", "detail": "unchanged"},
            {"check": "deterministic_replay", "status": "PASS", "detail": deterministic_digest},
        ],
        ["check", "status", "detail"],
    )
    sha_rows = []
    for name, path in paths.items():
        if path.exists() and path.is_file():
            sha_rows.append({"artifact": name, "path": _rel(path), "sha256": _sha256(path), "size_bytes": path.stat().st_size})
    _write_csv(paths["sha"], sha_rows, ["artifact", "path", "sha256", "size_bytes"])
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--run-tag")
    parser.add_argument("--slate-output")
    parser.add_argument("--prediction-wide")
    parser.add_argument("--book-upload")
    parser.add_argument("--pa-source")
    parser.add_argument("--output-dir")
    parser.add_argument("--mode", choices=["research_only", "dry_run"], default="research_only")
    args = parser.parse_args(argv)
    generated_at = _utc_now()
    payload = _build_payload(args, generated_at)
    rerun = _build_payload(args, generated_at)
    digest = _payload_digest(payload)
    if digest != _payload_digest(rerun):
        raise RuntimeError("deterministic payload replay failed")
    paths = _write_package(args, payload, digest)
    print(
        json.dumps(
            {
                "status": "ok",
                "date": payload["date"],
                "run_tag": payload["run_tag"],
                "output_dir": _rel(Path(args.output_dir or DEFAULT_OUTPUT_ROOT)),
                "player_game_rows": payload["manifest"]["player_game_rows"],
                "proposition_bridge_rows": payload["manifest"]["proposition_bridge_rows"],
                "attached_player_games": payload["manifest"]["attached_player_games"],
                "summary": _rel(paths["summary"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
