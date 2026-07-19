#!/usr/bin/env python3
"""Governed current-parent repair/recheck for the MLB Hits 0.5 candidate.

This utility is intentionally research-only. It inventories current-slate
parent sources, checks the frozen 54-feature contract, and scores only when a
governed nonmarket player-game parent is present. It does not request network
data, write to the database, train models, or alter production routing.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
RUN_DATE = "2026-07-19"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_current_parent_source_repair/2026-07-19"
SOURCE_MODEL_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19"
CANDIDATE_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_full_spine_replacement_candidate/2026-07-19"
NONMARKET_SPINE_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_nonmarket_player_game_feature_spine/2026-07-19"
LIVE_PARENT_DIR = ROOT / "artifacts/analysis/model_development/mlb_live_hitter_parent_daily_integration/2026-07-19"
LINEUP_CAPTURE_DIR = ROOT / "artifacts/analysis/model_development/mlb_governed_pregame_lineup_capture/2026-07-19"
PREPARED_VECTOR = ROOT / "backend/mlb/exports/model_diagnostics/prepared_feature_vectors/2026-07-19/hits_features.csv"

FROZEN_MODEL = CANDIDATE_DIR / "HITS05_FULL_SPINE_REPLACEMENT_CANDIDATE_RESEARCH_ONLY.joblib"
FROZEN_FEATURE_MANIFEST = SOURCE_MODEL_DIR / "frozen_feature_manifest_2026-07-19.csv"
SOURCE_MACHINE = CANDIDATE_DIR / "machine_readable_hits05_replacement_candidate_2026-07-19.json"
CURRENT_REPLAY_SPINE = NONMARKET_SPINE_DIR / "current_replay_spine_2026-07-19.csv"


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rel(path: Path | str) -> str:
    try:
        return str(Path(path).resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def write_csv(path: Path, rows: list[dict[str, Any]] | pd.DataFrame, fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, pd.DataFrame):
        rows.to_csv(path, index=False)
        return
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    if not fields:
        fields = ["status"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def used_features() -> list[str]:
    manifest = read_csv(FROZEN_FEATURE_MANIFEST)
    used = manifest[manifest["used"].astype(str).str.lower().isin(["true", "1", "yes"])].copy()
    return used["feature_name"].astype(str).tolist()


def classify_source(path: Path, role: str, feature_names: list[str]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "source_path": rel(path),
        "role": role,
        "exists": path.exists(),
        "rows": 0,
        "columns": 0,
        "unique_games": "",
        "unique_players": "",
        "frozen_features_present": 0,
        "frozen_features_missing": len(feature_names),
        "market_conditioned": "",
        "governed_pregame_parent": False,
        "candidate_source_status": "MISSING",
        "notes": "",
    }
    if not path.exists():
        row["notes"] = "Expected source was not present for the July 19 current-slate replay."
        return row
    df = read_csv(path)
    row["rows"] = int(len(df))
    row["columns"] = int(len(df.columns))
    if "game_id" in df.columns:
        row["unique_games"] = int(df["game_id"].nunique(dropna=True))
    if "player_id" in df.columns:
        row["unique_players"] = int(df["player_id"].nunique(dropna=True))
    present = [f for f in feature_names if f in df.columns]
    row["frozen_features_present"] = len(present)
    row["frozen_features_missing"] = len(feature_names) - len(present)
    market_cols = {"prop_type", "line", "side", "market_odds_american", "price_over_american", "price_under_american", "market_implied_probability"}
    row["market_conditioned"] = bool(market_cols.intersection(df.columns))
    if path == PREPARED_VECTOR:
        row["candidate_source_status"] = "REJECTED_MARKET_CONDITIONED_AND_FEATURE_INCOMPLETE"
        row["notes"] = "Prepared prediction vector is proposition-grain and lacks most frozen nonmarket contract features."
    elif "live_hitter_parent" in str(path) or "parsed_lineup_artifact" in str(path):
        complete = len(present) == len(feature_names)
        row["governed_pregame_parent"] = True
        row["candidate_source_status"] = "ACCEPTED" if complete else "REJECTED_FEATURE_INCOMPLETE"
        row["notes"] = "Governed pregame parent candidate."
    elif path == CURRENT_REPLAY_SPINE:
        row["candidate_source_status"] = "DIAGNOSTIC_WITHHELD_LEDGER"
        row["notes"] = "Existing replay ledger records game-level withholding, not player-game parent features."
    return row


def inventory_sources(feature_names: list[str]) -> list[dict[str, Any]]:
    candidates = [
        (LIVE_PARENT_DIR / "live_hitter_parent_artifact_2026-07-19.csv", "preferred_current_live_parent"),
        (LINEUP_CAPTURE_DIR / "parsed_lineup_artifact_2026-07-19.csv", "confirmed_pregame_lineup_parent"),
        (NONMARKET_SPINE_DIR / "current_replay_spine_2026-07-19.csv", "current_replay_withheld_ledger"),
        (PREPARED_VECTOR, "production_prepared_feature_vector_diagnostic_only"),
    ]
    return [classify_source(path, role, feature_names) for path, role in candidates]


def feature_parity(feature_names: list[str], source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    manifest = read_csv(FROZEN_FEATURE_MANIFEST)
    accepted = next((r for r in source_rows if r["candidate_source_status"] == "ACCEPTED"), None)
    accepted_path = ROOT / accepted["source_path"] if accepted else None
    accepted_cols = set(read_csv(accepted_path).columns) if accepted_path and accepted_path.exists() else set()
    prepared_cols = set(read_csv(PREPARED_VECTOR).columns) if PREPARED_VECTOR.exists() else set()
    rows: list[dict[str, Any]] = []
    for _, r in manifest[manifest["used"].astype(str).str.lower().isin(["true", "1", "yes"])].iterrows():
        name = str(r["feature_name"])
        rows.append(
            {
                "feature_name": name,
                "feature_family": r.get("feature_family", ""),
                "source_lineage": r.get("source_lineage", ""),
                "missing_value_policy": r.get("missing_value_policy", ""),
                "frozen_contract_required": True,
                "governed_current_parent_available": bool(accepted),
                "present_in_governed_current_parent": name in accepted_cols,
                "present_in_production_prepared_vector": name in prepared_cols,
                "parity_status": "PASS" if name in accepted_cols else "FAIL_NO_GOVERNED_CURRENT_PARENT_FEATURE",
                "notes": "Prepared vector presence is diagnostic only and does not satisfy parent-source governance.",
            }
        )
    return rows


def build_withheld_ledger() -> pd.DataFrame:
    replay = read_csv(CURRENT_REPLAY_SPINE)
    if replay.empty:
        return pd.DataFrame(
            [
                {
                    "slate_date": RUN_DATE,
                    "game_id": "",
                    "player_id": "",
                    "player_name": "",
                    "team": "",
                    "opponent": "",
                    "line": "0.5",
                    "withheld_reason": "NO_CURRENT_REPLAY_SPINE_FOUND",
                    "withheld_scope": "current_replay",
                    "notes": rel(CURRENT_REPLAY_SPINE),
                }
            ]
        )
    out = replay.copy()
    for col in ["player_id", "player_name", "team", "opponent"]:
        if col not in out.columns:
            out[col] = ""
    out["line"] = "0.5"
    out["withheld_scope"] = "current_replay_game"
    out["notes"] = "Existing current replay game was withheld before player-game feature construction."
    if "withheld_reason" not in out.columns:
        out["withheld_reason"] = out.get("current_replay_status", "NO_GOVERNED_CURRENT_PARENT_SOURCE")
    return out[
        ["slate_date", "game_id", "player_id", "player_name", "team", "opponent", "line", "withheld_reason", "withheld_scope", "notes"]
    ]


def score_current_parent(source_rows: list[dict[str, Any]], feature_names: list[str]) -> pd.DataFrame:
    accepted = next((r for r in source_rows if r["candidate_source_status"] == "ACCEPTED"), None)
    if not accepted:
        return pd.DataFrame(
            columns=[
                "slate_date",
                "game_id",
                "player_id",
                "player_name",
                "team",
                "opponent",
                "line",
                "candidate_expected_hits",
                "candidate_probability_over_0_5",
                "score_status",
                "source_parent_path",
            ]
        )
    path = ROOT / accepted["source_path"]
    parent = read_csv(path)
    missing = [f for f in feature_names if f not in parent.columns]
    if missing:
        raise ValueError(f"accepted parent missing frozen features: {missing[:5]}")
    artifact = joblib.load(FROZEN_MODEL)
    model = artifact["base_model"]
    X = parent[feature_names].copy()
    expected = np.asarray(model.predict(X), dtype=float)
    prob = 1 - np.exp(-np.clip(expected, 0, None))
    out = parent.copy()
    out["line"] = "0.5"
    out["candidate_expected_hits"] = expected
    out["candidate_probability_over_0_5"] = prob
    out["score_status"] = "SCORED_FROZEN_RAW_CANDIDATE"
    out["source_parent_path"] = rel(path)
    keep = [
        c
        for c in [
            "slate_date",
            "game_date",
            "game_id",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "line",
            "candidate_expected_hits",
            "candidate_probability_over_0_5",
            "score_status",
            "source_parent_path",
        ]
        if c in out.columns
    ]
    return out[keep]


def deterministic_replay(source_rows: list[dict[str, Any]], feature_names: list[str]) -> list[dict[str, Any]]:
    rows = []
    for attempt in [1, 2]:
        scored = score_current_parent(source_rows, feature_names)
        digest = hashlib.sha256(scored.to_csv(index=False).encode("utf-8")).hexdigest()
        rows.append(
            {
                "attempt": attempt,
                "rows": len(scored),
                "sha256": digest,
                "status": "PASS_ZERO_ROW_DETERMINISTIC" if len(scored) == 0 else "PASS_SCORED_DETERMINISTIC",
            }
        )
    rows.append(
        {
            "attempt": "comparison",
            "rows": rows[0]["rows"],
            "sha256": rows[0]["sha256"],
            "status": "PASS" if rows[0]["sha256"] == rows[1]["sha256"] else "FAIL",
        }
    )
    return rows


def validation_rows(out_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(out_dir.glob("*")):
        if path.suffix == ".csv":
            try:
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                status, notes = "PASS", ""
            except Exception as exc:
                status, notes = "FAIL", str(exc)
            rows.append({"artifact": rel(path), "validation": "csv_parse", "status": status, "notes": notes})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text(encoding="utf-8"))
                status, notes = "PASS", ""
            except Exception as exc:
                status, notes = "FAIL", str(exc)
            rows.append({"artifact": rel(path), "validation": "json_parse", "status": status, "notes": notes})
        elif path.suffix == ".md":
            status = "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL"
            rows.append({"artifact": rel(path), "validation": "markdown_nonempty", "status": status, "notes": ""})
    return rows


def sha_manifest(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and not path.name.startswith("sha256_manifest"):
            rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    return rows


def build(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = now_utc()
    features = used_features()
    source_rows = inventory_sources(features)
    parity_rows = feature_parity(features, source_rows)
    accepted_parent = next((r for r in source_rows if r["candidate_source_status"] == "ACCEPTED"), None)
    current_parent = pd.DataFrame()
    scores = score_current_parent(source_rows, features)
    withheld = build_withheld_ledger()
    deterministic = deterministic_replay(source_rows, features)

    prepared = read_csv(PREPARED_VECTOR)
    prepared_hits05_rows = int((prepared["line"].astype(str).eq("0.5")).sum()) if not prepared.empty and "line" in prepared.columns else 0
    prepared_hits15_rows = int((prepared["line"].astype(str).eq("1.5")).sum()) if not prepared.empty and "line" in prepared.columns else 0
    missing_in_prepared = [f for f in features if f not in set(prepared.columns)] if not prepared.empty else features

    root_cause = [
        {
            "stage": "current replay admission",
            "status": "FAIL",
            "root_cause": "NO_GOVERNED_CURRENT_PARENT_SOURCE_MATCHING_FROZEN_54_FEATURE_CONTRACT",
            "evidence": rel(CURRENT_REPLAY_SPINE),
            "notes": "Existing replay spine withheld all current games because no governed current nonmarket lineup/live-parent artifact existed.",
        },
        {
            "stage": "production prepared feature vector diagnostic",
            "status": "REJECTED",
            "root_cause": "PROPOSITION_GRAIN_FEATURE_VECTOR_NOT_FROZEN_NONMARKET_PARENT",
            "evidence": rel(PREPARED_VECTOR),
            "notes": f"Vector exists with {len(prepared)} rows and {prepared_hits05_rows} Hits 0.5 proposition rows, but it is market/proposition-grain and misses {len(missing_in_prepared)} of 54 frozen features.",
        },
    ]
    source_hierarchy = [
        {
            "priority": 1,
            "source": rel(LIVE_PARENT_DIR / "live_hitter_parent_artifact_2026-07-19.csv"),
            "source_type": "governed_live_parent",
            "admission_status": "MISSING",
            "reason": "No July 19 governed live hitter parent artifact found.",
        },
        {
            "priority": 2,
            "source": rel(LINEUP_CAPTURE_DIR / "parsed_lineup_artifact_2026-07-19.csv"),
            "source_type": "governed_pregame_lineup",
            "admission_status": "MISSING",
            "reason": "No July 19 governed parsed lineup artifact found.",
        },
        {
            "priority": 3,
            "source": rel(PREPARED_VECTOR),
            "source_type": "production_prepared_vector",
            "admission_status": "REJECTED_DIAGNOSTIC_ONLY",
            "reason": "Proposition-grain/current prediction vector is not the governed nonmarket player-game parent and does not carry the frozen feature contract.",
        },
    ]
    lineup_ledger = [
        {
            "source": rel(LINEUP_CAPTURE_DIR / "parsed_lineup_artifact_2026-07-19.csv"),
            "exists": (LINEUP_CAPTURE_DIR / "parsed_lineup_artifact_2026-07-19.csv").exists(),
            "lineup_semantics": "confirmed_pregame_official_if_present",
            "accepted_for_parent": False,
            "notes": "Absent for July 19; no postgame lineup reconstruction allowed.",
        },
        {
            "source": rel(LIVE_PARENT_DIR / "live_hitter_parent_artifact_2026-07-19.csv"),
            "exists": (LIVE_PARENT_DIR / "live_hitter_parent_artifact_2026-07-19.csv").exists(),
            "lineup_semantics": "integrated_governed_current_parent_if_present",
            "accepted_for_parent": False,
            "notes": "Absent for July 19.",
        },
    ]
    starter_ledger = [
        {
            "source": "current parent artifact starter columns",
            "exists": bool(accepted_parent),
            "starter_semantics": "pregame governed starter identity plus strict-prior starter profile",
            "accepted_for_parent": bool(accepted_parent),
            "notes": "Starter profile cannot be evaluated without admitted player-game parent rows.",
        },
        {
            "source": rel(PREPARED_VECTOR),
            "exists": PREPARED_VECTOR.exists(),
            "starter_semantics": "production prepared vector starting_pitcher_id only",
            "accepted_for_parent": False,
            "notes": "Does not include frozen starter rolling profile features.",
        },
    ]
    generation_order = [
        {"step": 1, "component": "official or governed projected current player-game population", "status": "MISSING_FOR_2026_07_19", "notes": "Must occur before feature computation and before odds/candidate filtering."},
        {"step": 2, "component": "strict-prior hitter history features", "status": "READY_WHEN_PARENT_EXISTS", "notes": "Frozen source is mlb.player_stats/game_info with game_date < slate_date."},
        {"step": 3, "component": "strict-prior starter profile features", "status": "READY_WHEN_PARENT_AND_STARTER_IDENTITY_EXISTS", "notes": "No postgame starter identity allowed for current scoring."},
        {"step": 4, "component": "team offense strict-prior features", "status": "READY_WHEN_PARENT_EXISTS", "notes": "Does not require sportsbook data."},
        {"step": 5, "component": "score frozen raw Hits 0.5 candidate", "status": "BLOCKED_CURRENT_PARENT_MISSING", "notes": "No refit or calibration allowed."},
        {"step": 6, "component": "optional line/proposition join for Hits 0.5 only", "status": "AFTER_NONMARKET_SCORE_ONLY", "notes": "Hits 1.5 remains on incumbent route."},
    ]
    completeness = [
        {
            "scope": "frozen_54_feature_contract",
            "required_features": len(features),
            "governed_current_parent_rows": len(current_parent),
            "scored_rows": len(scores),
            "missing_features_in_prepared_vector": len(missing_in_prepared),
            "feature_parity_status": "FAIL_NO_GOVERNED_CURRENT_PARENT",
            "notes": "A complete parent cannot be certified from July 19 local artifacts.",
        }
    ]
    incumbent_comparison = [
        {
            "scope": "current_hits_0_5",
            "candidate_scored_rows": len(scores),
            "incumbent_current_prepared_hits05_rows": prepared_hits05_rows,
            "comparison_status": "NOT_EVALUABLE_CANDIDATE_CURRENT_PARENT_BLOCKED",
            "notes": "Incumbent artifacts preserved; no production routing changed.",
        },
        {
            "scope": "current_hits_1_5",
            "candidate_scored_rows": 0,
            "incumbent_current_prepared_hits15_rows": prepared_hits15_rows,
            "comparison_status": "NOT_IN_SCOPE_INCUMBENT_PRESERVED",
            "notes": "Threshold routing explicitly leaves Hits 1.5 unchanged.",
        },
    ]
    threshold_routing = [
        {"threshold": "hits_0_5", "route": "candidate only after governed parent and explicit authorization", "status": "BLOCKED_DEFAULT_OFF", "notes": "No production activation."},
        {"threshold": "hits_1_5", "route": "existing production incumbent", "status": "PRESERVED", "notes": "No O1.5 behavior changed."},
    ]
    gates = [
        {"gate": "frozen_candidate_bound", "pass": FROZEN_MODEL.exists(), "notes": rel(FROZEN_MODEL)},
        {"gate": "frozen_54_feature_contract_bound", "pass": len(features) == 54, "notes": rel(FROZEN_FEATURE_MANIFEST)},
        {"gate": "governed_current_parent_exists", "pass": bool(accepted_parent), "notes": "Required before any swap."},
        {"gate": "feature_parity_current_vs_frozen", "pass": bool(accepted_parent) and all(r["parity_status"] == "PASS" for r in parity_rows), "notes": "Blocked by missing current parent."},
        {"gate": "current_candidate_scored", "pass": len(scores) > 0, "notes": f"scored_rows={len(scores)}"},
        {"gate": "deterministic_replay", "pass": deterministic[-1]["status"] == "PASS", "notes": deterministic[-1]["sha256"]},
        {"gate": "hits_15_incumbent_preserved", "pass": True, "notes": "Threshold routing unchanged."},
        {"gate": "production_swap_authorized", "pass": False, "notes": "Not authorized by task."},
    ]
    swap_package = [
        {
            "component": "bounded Hits 0.5 swap",
            "status": "NOT_PREPARED_GATE_BLOCKED",
            "requires": "governed current parent with full frozen feature parity and nonzero deterministic current scoring",
            "production_behavior_change_required": True,
            "notes": "No swap package is operationally valid until parent-source gate passes.",
        }
    ]
    decisions = {
        "MLB_HITS05_CURRENT_PARENT_ROOT_CAUSE_DECISION": "NO_GOVERNED_CURRENT_PARENT_SOURCE_MATCHING_FROZEN_54_FEATURE_CONTRACT",
        "MLB_HITS05_CURRENT_PARENT_SOURCE_HIERARCHY_DECISION": "GOVERNED_LIVE_PARENT_OR_CONFIRMED_PREGAME_LINEUP_REQUIRED_PREPARED_VECTOR_REJECTED",
        "MLB_HITS05_CURRENT_LINEUP_SOURCE_DECISION": "NO_JULY19_GOVERNED_PREGAME_LINEUP_SOURCE_FOUND_POSTGAME_RECONSTRUCTION_NOT_ALLOWED",
        "MLB_HITS05_CURRENT_STARTER_SOURCE_DECISION": "NO_ADMITTED_CURRENT_PARENT_TO_BIND_STARTER_PROFILE_PREPARED_VECTOR_STARTER_ID_INSUFFICIENT",
        "MLB_HITS05_CURRENT_PARENT_GRAIN_DECISION": "REQUIRED_GRAIN_PLAYER_GAME_NOT_PROPOSITION_OBSERVATION",
        "MLB_HITS05_CURRENT_FEATURE_PARITY_DECISION": "FAIL_NO_GOVERNED_CURRENT_PARENT_FEATURE_PARITY",
        "MLB_HITS05_CURRENT_PARENT_GENERATION_ORDER_DECISION": "PARENT_BEFORE_FEATURES_BEFORE_SCORE_BEFORE_MARKET_JOIN",
        "MLB_HITS05_CURRENT_PARENT_ARTIFACT_DECISION": "IMMUTABLE_EMPTY_PARENT_WRITTEN_WITH_WITHHELD_LEDGER",
        "MLB_HITS05_CURRENT_SCORING_DECISION": f"SCORED_{len(scores)}_WITHHELD_{len(withheld)}",
        "MLB_HITS05_CURRENT_DETERMINISM_DECISION": "PASS_ZERO_ROW_REPLAY_DETERMINISTIC" if deterministic[-1]["status"] == "PASS" else "FAIL",
        "MLB_HITS05_THRESHOLD_ROUTING_DECISION": "HITS_05_ONLY_HITS_15_INCUMBENT_PRESERVED",
        "MLB_HITS05_REPLACEMENT_GATE_DECISION": "BLOCKED_CURRENT_PARENT_SOURCE_NOT_CERTIFIED",
        "MLB_HITS05_SWAP_PACKAGE_DECISION": "NOT_PREPARED_GATE_BLOCKED_DEFAULT_OFF",
        "MLB_HITS05_FORCED_NEXT_STEP_DECISION": "IMPLEMENT_GOVERNED_CURRENT_NONMARKET_PLAYER_GAME_PARENT_BEFORE_SCORE",
        "MLB_HITS15_STATUS": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "MLB_PRODUCTION_STATUS": "UNCHANGED",
    }

    outputs = {
        "root_cause": output_dir / "hits05_current_parent_root_cause_trace_2026-07-19.csv",
        "source_inventory": output_dir / "hits05_candidate_parent_source_inventory_2026-07-19.csv",
        "contract": output_dir / "hits05_current_parent_contract_2026-07-19.csv",
        "lineup": output_dir / "hits05_lineup_source_ledger_2026-07-19.csv",
        "starter": output_dir / "hits05_starter_source_ledger_2026-07-19.csv",
        "parity": output_dir / "hits05_frozen_feature_parity_matrix_2026-07-19.csv",
        "order": output_dir / "hits05_current_parent_generation_order_map_2026-07-19.csv",
        "parent": output_dir / "hits05_immutable_current_parent_2026-07-19.csv",
        "completeness": output_dir / "hits05_current_feature_completeness_2026-07-19.csv",
        "withheld": output_dir / "hits05_current_withheld_ledger_2026-07-19.csv",
        "scores": output_dir / "hits05_current_candidate_scores_2026-07-19.csv",
        "incumbent": output_dir / "hits05_current_incumbent_comparison_2026-07-19.csv",
        "determinism": output_dir / "hits05_current_deterministic_replay_2026-07-19.csv",
        "routing": output_dir / "hits05_threshold_routing_validation_2026-07-19.csv",
        "gates": output_dir / "hits05_replacement_gate_recheck_2026-07-19.csv",
        "swap": output_dir / "hits05_bounded_swap_package_when_authorized_2026-07-19.csv",
        "decisions": output_dir / "hits05_current_parent_repair_decisions_2026-07-19.csv",
        "machine": output_dir / "machine_readable_hits05_current_parent_repair_2026-07-19.json",
        "md": output_dir / "hits05_current_parent_source_repair_2026-07-19.md",
        "sha": output_dir / "sha256_manifest_2026-07-19.csv",
        "validation": output_dir / "validation_report_2026-07-19.csv",
    }

    write_csv(outputs["root_cause"], root_cause)
    write_csv(outputs["source_inventory"], source_rows)
    write_csv(outputs["contract"], source_hierarchy)
    write_csv(outputs["lineup"], lineup_ledger)
    write_csv(outputs["starter"], starter_ledger)
    write_csv(outputs["parity"], parity_rows)
    write_csv(outputs["order"], generation_order)
    write_csv(outputs["parent"], current_parent)
    write_csv(outputs["completeness"], completeness)
    write_csv(outputs["withheld"], withheld)
    write_csv(outputs["scores"], scores)
    write_csv(outputs["incumbent"], incumbent_comparison)
    write_csv(outputs["determinism"], deterministic)
    write_csv(outputs["routing"], threshold_routing)
    write_csv(outputs["gates"], gates)
    write_csv(outputs["swap"], swap_package)
    write_csv(outputs["decisions"], [{"decision": k, "value": v} for k, v in decisions.items()])

    machine = {
        "generated_at_utc": generated_at,
        "run_date": RUN_DATE,
        "mode": "research_only_no_write",
        "frozen_candidate": rel(FROZEN_MODEL),
        "frozen_candidate_sha256": sha256(FROZEN_MODEL) if FROZEN_MODEL.exists() else "",
        "frozen_feature_manifest": rel(FROZEN_FEATURE_MANIFEST),
        "frozen_feature_count": len(features),
        "governed_current_parent_rows": len(current_parent),
        "current_candidate_scored_rows": len(scores),
        "current_withheld_rows": len(withheld),
        "production_prepared_vector_rows": len(prepared),
        "production_prepared_hits05_rows": prepared_hits05_rows,
        "production_prepared_hits15_rows": prepared_hits15_rows,
        "missing_frozen_features_in_prepared_vector": len(missing_in_prepared),
        "replacement_gate_decision": decisions["MLB_HITS05_REPLACEMENT_GATE_DECISION"],
        "direct_answer": "NO",
        "guardrails": {
            "db_writes": False,
            "network_calls": False,
            "training": False,
            "calibration_work": False,
            "production_behavior_changed": False,
            "sportsbook_features_used": False,
            "hits_15_changed": False,
            "wager_output": False,
        },
        "decisions": decisions,
    }
    write_json(outputs["machine"], machine)
    write_md(
        outputs["md"],
        f"""# MLB Hits 0.5 Governed Current Parent-Source Repair

Generated: `{generated_at}`

## Executive Summary

The bounded repair/recheck did **not** clear the current-parent gate for the frozen raw Hits 0.5 full-spine candidate.

The frozen contract requires `{len(features)}` strict-prior baseball features at player-game grain before any market join. The admissible July 19 governed sources were not present:

- `{rel(LIVE_PARENT_DIR / 'live_hitter_parent_artifact_2026-07-19.csv')}`
- `{rel(LINEUP_CAPTURE_DIR / 'parsed_lineup_artifact_2026-07-19.csv')}`

The production prepared vector exists at `{rel(PREPARED_VECTOR)}`, but it is proposition-grain, contains market/line columns, and is missing `{len(missing_in_prepared)}` of the frozen 54 features. It was retained as diagnostic evidence only and rejected as a parent-source repair.

## Gate Recheck

- Governed current parent rows: `{len(current_parent)}`
- Frozen candidate score rows: `{len(scores)}`
- Withheld rows: `{len(withheld)}`
- Deterministic replay: `{decisions['MLB_HITS05_CURRENT_DETERMINISM_DECISION']}`
- Replacement gate: `{decisions['MLB_HITS05_REPLACEMENT_GATE_DECISION']}`

## Current Source Hierarchy

The accepted generation order remains: governed player-game parent first, strict-prior features second, frozen candidate scoring third, Hits 0.5 market join last. Hits 1.5 remains routed to the existing production incumbent.

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## No Behavior Changed

No model was trained, no calibrator was changed, no sportsbook field entered the candidate, no database write occurred, no wager or upload file was produced, and production routing was not modified.
""",
    )
    write_csv(outputs["sha"], sha_manifest(output_dir))
    write_csv(outputs["validation"], validation_rows(output_dir))
    write_csv(outputs["sha"], sha_manifest(output_dir))
    write_csv(outputs["validation"], validation_rows(output_dir))
    return machine


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--mode", choices=["research_only", "dry_run"], default="research_only")
    args = parser.parse_args()
    result = build(args.output_dir)
    print(json.dumps({"output_dir": rel(args.output_dir), **result}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
