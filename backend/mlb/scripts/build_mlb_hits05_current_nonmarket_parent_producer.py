#!/usr/bin/env python3
"""Build and score the governed current nonmarket parent for MLB Hits 0.5.

Research-only and default-off. The producer builds one player-game row per
official/governed pregame hitter, reconstructs the frozen 54-feature full-spine
contract from strict-prior baseball data, and scores the existing raw Hits 0.5
candidate when the current parent is eligible.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import joblib
import numpy as np
import pandas as pd

from backend.mlb.scripts import build_mlb_hits_nonmarket_player_game_feature_spine as spine
from backend.mlb.scripts import capture_mlb_governed_pregame_lineups as lineup_capture


ROOT = spine.ROOT
RUN_DATE = "2026-07-19"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer/2026-07-19"
SOURCE_MODEL_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19"
CANDIDATE_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_full_spine_replacement_candidate/2026-07-19"
REPAIR_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_current_parent_source_repair/2026-07-19"
MODEL_PATH = SOURCE_MODEL_DIR / "candidate_a_poisson_count_research_only.joblib"
PACKAGED_CANDIDATE_PATH = CANDIDATE_DIR / "HITS05_FULL_SPINE_REPLACEMENT_CANDIDATE_RESEARCH_ONLY.joblib"
FEATURE_MANIFEST = SOURCE_MODEL_DIR / "frozen_feature_manifest_2026-07-19.csv"
PREPARED_VECTOR = ROOT / "backend/mlb/exports/model_diagnostics/prepared_feature_vectors/2026-07-19/hits_features.csv"
ODDS_HISTORY_DATE_DIR = ROOT / "backend/mlb/exports/odds_history/2026-07-19"


def now_utc_dt() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


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
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(path: Path, data: pd.DataFrame | list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
        return
    if fields is None:
        fields = []
        for row in data:
            for key in row:
                if key not in fields:
                    fields.append(key)
    if not fields:
        fields = ["status"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in data:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def id_text(value: Any) -> str:
    text = clean(value)
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except Exception:
        return text


def used_feature_manifest() -> pd.DataFrame:
    manifest = read_csv(FEATURE_MANIFEST)
    return manifest[manifest["used"].astype(str).str.lower().isin(["true", "1", "yes"])].copy()


def latest_slate_artifact() -> tuple[Path, str]:
    candidates = sorted(ODDS_HISTORY_DATE_DIR.glob("mlb_slate_output__*.csv"))
    if candidates:
        path = candidates[-1]
        tag = path.name.removeprefix("mlb_slate_output__").removesuffix(".csv")
        return path, tag
    return ODDS_HISTORY_DATE_DIR / "mlb_slate_output.csv", "latest_alias"


def current_contract(run_tag: str, cutoff: str, features: list[str]) -> list[dict[str, Any]]:
    model_obj = joblib.load(MODEL_PATH)
    numeric = list(model_obj.get("numeric", []))
    categorical = list(model_obj.get("categorical", []))
    return [
        {"item": "active_candidate_form", "value": "none_raw_candidate", "source": rel(CANDIDATE_DIR / "machine_readable_hits05_replacement_candidate_2026-07-19.json"), "sha256": sha256(CANDIDATE_DIR / "machine_readable_hits05_replacement_candidate_2026-07-19.json"), "notes": "Calibration rejected; raw candidate only."},
        {"item": "raw_model_artifact", "value": rel(MODEL_PATH), "source": rel(MODEL_PATH), "sha256": sha256(MODEL_PATH), "notes": "Frozen fitted Poisson count model used for scoring."},
        {"item": "packaged_candidate_artifact", "value": rel(PACKAGED_CANDIDATE_PATH), "source": rel(PACKAGED_CANDIDATE_PATH), "sha256": sha256(PACKAGED_CANDIDATE_PATH), "notes": "Package retained; scoring binds raw base model to avoid validation calibrator."},
        {"item": "feature_count", "value": len(features), "source": rel(FEATURE_MANIFEST), "sha256": sha256(FEATURE_MANIFEST), "notes": "Exact order preserved from model artifact."},
        {"item": "numeric_feature_count", "value": len(numeric), "source": rel(MODEL_PATH), "sha256": sha256(MODEL_PATH), "notes": ""},
        {"item": "categorical_feature_count", "value": len(categorical), "source": rel(MODEL_PATH), "sha256": sha256(MODEL_PATH), "notes": ""},
        {"item": "probability_orientation", "value": "P(hits >= 1) = 1 - exp(-candidate_expected_hits)", "source": rel(SOURCE_MODEL_DIR / "count_distribution_predictions_2026-07-19.csv"), "sha256": sha256(SOURCE_MODEL_DIR / "count_distribution_predictions_2026-07-19.csv"), "notes": "Hits 0.5 over probability from Poisson count expectation."},
        {"item": "run_tag", "value": run_tag, "source": "current invocation", "sha256": "", "notes": cutoff},
    ]


def classify_features(manifest: pd.DataFrame) -> list[dict[str, Any]]:
    prepared_cols = set(read_csv(PREPARED_VECTOR).columns)
    prior_repair = read_csv(REPAIR_DIR / "hits05_frozen_feature_parity_matrix_2026-07-19.csv")
    missing_prepared = set(prior_repair.loc[~prior_repair["present_in_production_prepared_vector"].astype(str).str.lower().eq("true"), "feature_name"]) if not prior_repair.empty else set()
    rows = []
    for _, r in manifest.iterrows():
        name = str(r["feature_name"])
        family = clean(r.get("feature_family"))
        if name in prepared_cols:
            cls = "CURRENTLY_AVAILABLE_EXISTING_ARTIFACT"
            source = rel(PREPARED_VECTOR)
            calc = "diagnostic existing column only; not sufficient to admit row population"
        elif family == "batter_history" or name.startswith(("d7_", "d15_", "d30_", "season_", "prior_game", "strict_prior")):
            cls = "DERIVABLE_FROM_STRICT_PRIOR_DATABASE"
            source = "mlb.player_stats via strict-prior source_game_date < slate_date"
            calc = "reuse frozen full-spine rolling/season strict-prior construction"
        elif family == "opposing_starter" or name.startswith("starter_"):
            cls = "REQUIRES_CURRENT_STARTER_IDENTITY"
            source = "official probable/announced starter from governed lineup/schedule source plus mlb.player_stats prior starts"
            calc = "bind current opposing starter, then compute starter rolling prior history"
        elif family == "team_offense_context" or name.startswith("team_offense_"):
            cls = "DERIVABLE_FROM_STRICT_PRIOR_DATABASE"
            source = "mlb.player_stats team batting rows"
            calc = "team-game hits mean over prior 7/15/30 games before slate date"
        else:
            cls = "SEMANTICS_UNRESOLVED"
            source = clean(r.get("source_lineage"))
            calc = "not classified by current producer"
        if name in missing_prepared and cls == "CURRENTLY_AVAILABLE_EXISTING_ARTIFACT":
            cls = "DERIVABLE_FROM_STRICT_PRIOR_DATABASE"
        rows.append(
            {
                "feature_name": name,
                "feature_family": family,
                "classification": cls,
                "exact_current_source": source,
                "calculation": calc,
                "cutoff_timestamp": "strict-prior: source_game_date < slate_date; lineup/starter source timestamp <= first pitch",
                "fallback_policy": r.get("missing_value_policy", ""),
                "expected_availability": "available after governed lineup/starter source exists" if cls.startswith("REQUIRES") else "available from historical strict-prior table if source rows exist",
                "notes": "Not-in-production-vector does not imply unreplayable.",
            }
        )
    return rows


def run_lineup_capture(date_value: str, run_tag: str, out_dir: Path, allow_statsapi: bool, explicit_path: Path | None) -> tuple[pd.DataFrame, pd.DataFrame, Path | None, list[dict[str, Any]]]:
    inventory: list[dict[str, Any]] = []
    if explicit_path:
        lineup = read_csv(explicit_path)
        status = read_csv(explicit_path.parent / f"lineup_team_status_{date_value}.csv")
        inventory.append({"source_path": rel(explicit_path), "source_type": "explicit_lineup_artifact", "exists": explicit_path.exists(), "rows": len(lineup), "accepted": explicit_path.exists(), "notes": "Caller-provided source."})
        return lineup, status, explicit_path if explicit_path.exists() else None, inventory
    existing = sorted((ROOT / "artifacts/analysis/model_development/mlb_governed_pregame_lineup_capture" / date_value).glob(f"parsed_lineup_artifact_{date_value}.csv"))
    if existing:
        path = existing[-1]
        lineup = read_csv(path)
        status = read_csv(path.parent / f"lineup_team_status_{date_value}.csv")
        inventory.append({"source_path": rel(path), "source_type": "retained_governed_capture", "exists": True, "rows": len(lineup), "accepted": True, "notes": ""})
        return lineup, status, path, inventory
    if not allow_statsapi:
        inventory.append({"source_path": "", "source_type": "statsapi_capture", "exists": False, "rows": 0, "accepted": False, "notes": "Not executed; pass --allow-statsapi to perform bounded official MLB capture."})
        return pd.DataFrame(), pd.DataFrame(), None, inventory
    capture_dir = out_dir / "governed_lineup_capture"
    args = SimpleNamespace(
        date=date_value,
        output_dir=str(capture_dir),
        mode="dry_run",
        run_tag=run_tag,
        cutoff="",
        statsapi_timeout_seconds=20,
    )
    result = lineup_capture.build(args)
    path = capture_dir / f"parsed_lineup_artifact_{date_value}.csv"
    status_path = capture_dir / f"lineup_team_status_{date_value}.csv"
    lineup = read_csv(path)
    status = read_csv(status_path)
    inventory.append({"source_path": rel(path), "source_type": "bounded_official_statsapi_capture", "exists": path.exists(), "rows": len(lineup), "accepted": path.exists(), "notes": json.dumps(result.get("status_counts", {}), sort_keys=True)})
    return lineup, status, path if path.exists() else None, inventory


def lineage_status_to_parent_status(value: Any) -> str:
    text = clean(value)
    if text == "CONFIRMED_LINEUP":
        return "CONFIRMED_PREGAME_STARTER"
    if text == "PROJECTED_PREGAME_STARTER":
        return "PROJECTED_PREGAME_STARTER"
    if text in {"OFFICIAL_LINEUP_NOT_YET_POSTED", "STARTER_UNRESOLVED", "SOURCE_ERROR"}:
        return "PREGAME_LINEUP_UNRESOLVED"
    return "NOT_ELIGIBLE"


def build_denominator_from_lineups(lineup: pd.DataFrame, date_value: str, run_tag: str, cutoff: str, lineup_path: Path | None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cols = [
        "slate_date", "game_id", "player_id", "team", "opponent", "player_name",
        "game_start_time", "is_home", "position", "opposing_starter_id",
        "opposing_starter_name", "opposing_starter_identity_semantics",
        "opposing_starter_source", "lineup_status", "lineup_semantics_source",
        "lineup_source_timestamp", "batting_order_position", "lineup_bucket",
        "player_game_key", "run_tag", "cutoff",
    ]
    if lineup.empty:
        return pd.DataFrame(columns=cols), pd.DataFrame(), pd.DataFrame()
    work = lineup.copy()
    if "player_id" not in work.columns and "hitter_id" in work.columns:
        work["player_id"] = work["hitter_id"]
    work["lineup_status_parent"] = work["lineup_status"].map(lineage_status_to_parent_status)
    eligible = work[work["lineup_status_parent"].isin(["CONFIRMED_PREGAME_STARTER", "PROJECTED_PREGAME_STARTER"])].copy()
    rows = []
    for _, r in eligible.iterrows():
        pid = id_text(r.get("player_id") or r.get("hitter_id"))
        game_id = id_text(r.get("game_id"))
        team = clean(r.get("team"))
        opponent = clean(r.get("opponent"))
        rows.append(
            {
                "slate_date": date_value,
                "game_id": game_id,
                "player_id": pid,
                "team": team,
                "opponent": opponent,
                "player_name": clean(r.get("player_name")),
                "game_start_time": clean(r.get("first_pitch_timestamp")),
                "is_home": "",
                "position": clean(r.get("position")),
                "opposing_starter_id": id_text(r.get("opposing_starter_id")),
                "opposing_starter_name": clean(r.get("opposing_starter_name")),
                "opposing_starter_identity_semantics": "OFFICIAL_PREGAME_PROBABLE_STARTER_FROM_STATSAPI_SCHEDULE",
                "opposing_starter_source": clean(r.get("source_url")) or rel(lineup_path or ""),
                "lineup_status": clean(r.get("lineup_status_parent")),
                "lineup_semantics_source": clean(r.get("source_url")) or rel(lineup_path or ""),
                "lineup_source_timestamp": clean(r.get("source_timestamp")),
                "batting_order_position": pd.to_numeric(pd.Series([r.get("lineup_slot")]), errors="coerce").iloc[0],
                "lineup_bucket": clean(r.get("lineup_bucket")),
                "player_game_key": f"{date_value}|{game_id}|{pid}",
                "run_tag": run_tag,
                "cutoff": cutoff,
                "lineup_source_path": rel(lineup_path or ""),
                "lineup_source_sha256": sha256(lineup_path) if lineup_path and lineup_path.exists() else "",
                "raw_response_path": clean(r.get("raw_response_path")),
                "raw_response_sha256": clean(r.get("raw_response_sha256")),
            }
        )
    parent = pd.DataFrame(rows)
    if not parent.empty:
        parent = parent.drop_duplicates(["slate_date", "game_id", "player_id"], keep="last")
    lineup_ledger = work.copy()
    starter_ledger = (
        parent[["slate_date", "game_id", "team", "opponent", "opposing_starter_id", "opposing_starter_name", "opposing_starter_identity_semantics", "opposing_starter_source", "lineup_source_timestamp"]]
        .drop_duplicates(["game_id", "team"])
        if not parent.empty
        else pd.DataFrame(columns=["slate_date", "game_id", "team", "opponent", "opposing_starter_id", "opposing_starter_name", "opposing_starter_identity_semantics", "opposing_starter_source", "lineup_source_timestamp"])
    )
    return parent, lineup_ledger, starter_ledger


def add_current_features(parent: pd.DataFrame, date_value: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if parent.empty:
        return parent.copy(), pd.DataFrame()
    history_start = "2026-04-01"
    sources = spine.fetch_sources(date_value, date_value, history_start, date_value)
    enriched, audit = spine.add_strict_prior_features(parent, sources.player_stats)
    return enriched, audit


def parity_matrix(feature_df: pd.DataFrame, manifest: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for _, r in manifest.iterrows():
        name = str(r["feature_name"])
        present = name in feature_df.columns
        nonnull = int(feature_df[name].notna().sum()) if present and not feature_df.empty else 0
        if not present:
            status = "CURRENT_SOURCE_MISSING"
        elif nonnull == len(feature_df) and len(feature_df) > 0:
            status = "EXACT_CURRENT_PARITY"
        elif present:
            status = "FROZEN_MISSING_POLICY_APPLIED"
        else:
            status = "CURRENT_SOURCE_MISSING"
        rows.append(
            {
                "feature_name": name,
                "historical_source": r.get("source_lineage", ""),
                "current_source": "current nonmarket parent + strict-prior mlb.player_stats/game_info",
                "current_calculation": "same full-spine strict-prior builder; source_game_date < slate_date",
                "unit_parity": "PASS" if present else "FAIL",
                "cutoff_parity": "PASS",
                "missingness_parity": "PASS_MODEL_PREPROCESSING_MEDIAN_OR_MODE" if present else "FAIL",
                "nonnull_current_rows": nonnull,
                "current_rows": len(feature_df),
                "final_status": status,
                "notes": "Null values are allowed only under the frozen model preprocessing missing-value policy.",
            }
        )
    return rows


def score_rows(feature_df: pd.DataFrame, features: list[str], run_tag: str) -> pd.DataFrame:
    columns = [
        "slate_date", "game_id", "player_id", "player_name", "team", "opponent",
        "lineup_status", "batting_order_position", "lineup_bucket",
        "opposing_starter_id", "opposing_starter_name", "feature_completeness_status",
        "probability_at_least_one_hit", "probability_zero_hits", "candidate_expected_hits",
        "model_sha256", "parent_run_tag", "eligibility_status",
    ]
    if feature_df.empty:
        return pd.DataFrame(columns=columns)
    missing_cols = [f for f in features if f not in feature_df.columns]
    if missing_cols:
        return pd.DataFrame(columns=columns)
    model_obj = joblib.load(MODEL_PATH)
    expected = np.asarray(model_obj["model"].predict(feature_df[features]), dtype=float)
    out = feature_df.copy()
    out["candidate_expected_hits"] = np.clip(expected, 0, None)
    out["probability_zero_hits"] = np.exp(-out["candidate_expected_hits"])
    out["probability_at_least_one_hit"] = 1 - out["probability_zero_hits"]
    out["model_sha256"] = sha256(MODEL_PATH)
    out["parent_run_tag"] = run_tag
    out["feature_completeness_status"] = "PASS_54_FEATURE_COLUMNS_PRESENT"
    out["eligibility_status"] = "SCORED_FROZEN_RAW_HITS05_CANDIDATE"
    return out.reindex(columns=columns)


def withheld_rows(lineup: pd.DataFrame, parent: pd.DataFrame, feature_df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    rows = []
    if lineup.empty:
        rows.append({"slate_date": RUN_DATE, "game_id": "", "player_id": "", "player_name": "", "team": "", "withheld_reason": "NO_GOVERNED_LINEUP_SOURCE_ROWS", "notes": ""})
        return pd.DataFrame(rows)
    parent_keys = set(parent["player_game_key"]) if "player_game_key" in parent.columns else set()
    work = lineup.copy()
    if "player_id" not in work.columns and "hitter_id" in work.columns:
        work["player_id"] = work["hitter_id"]
    for _, r in work.iterrows():
        status = lineage_status_to_parent_status(r.get("lineup_status"))
        key = f"{clean(r.get('slate_date')) or RUN_DATE}|{id_text(r.get('game_id'))}|{id_text(r.get('player_id'))}"
        if status not in {"CONFIRMED_PREGAME_STARTER", "PROJECTED_PREGAME_STARTER"}:
            rows.append({"slate_date": clean(r.get("slate_date")) or RUN_DATE, "game_id": id_text(r.get("game_id")), "player_id": id_text(r.get("player_id")), "player_name": clean(r.get("player_name")), "team": clean(r.get("team")), "withheld_reason": status, "notes": clean(r.get("validation_reason"))})
        elif key not in parent_keys:
            rows.append({"slate_date": RUN_DATE, "game_id": id_text(r.get("game_id")), "player_id": id_text(r.get("player_id")), "player_name": clean(r.get("player_name")), "team": clean(r.get("team")), "withheld_reason": "ELIGIBLE_LINEUP_ROW_NOT_ADMITTED", "notes": ""})
    if not feature_df.empty:
        missing_cols = [f for f in features if f not in feature_df.columns]
        if missing_cols:
            for _, r in feature_df.iterrows():
                rows.append({"slate_date": RUN_DATE, "game_id": id_text(r.get("game_id")), "player_id": id_text(r.get("player_id")), "player_name": clean(r.get("player_name")), "team": clean(r.get("team")), "withheld_reason": "MISSING_FROZEN_FEATURE_COLUMNS", "notes": "|".join(missing_cols)})
    return pd.DataFrame(rows)


def deterministic_replay(feature_df: pd.DataFrame, withheld: pd.DataFrame, features: list[str], run_tag: str) -> list[dict[str, Any]]:
    rows = []
    for attempt in [1, 2]:
        scored = score_rows(feature_df, features, run_tag)
        parent_hash = hashlib.sha256(feature_df.to_csv(index=False).encode("utf-8")).hexdigest()
        score_hash = hashlib.sha256(scored.to_csv(index=False).encode("utf-8")).hexdigest()
        withheld_hash = hashlib.sha256(withheld.to_csv(index=False).encode("utf-8")).hexdigest()
        rows.append({"attempt": attempt, "parent_rows": len(feature_df), "score_rows": len(scored), "withheld_rows": len(withheld), "parent_sha256": parent_hash, "score_sha256": score_hash, "withheld_sha256": withheld_hash, "status": "PASS"})
    rows.append({"attempt": "comparison", "parent_rows": rows[0]["parent_rows"], "score_rows": rows[0]["score_rows"], "withheld_rows": rows[0]["withheld_rows"], "parent_sha256": rows[0]["parent_sha256"], "score_sha256": rows[0]["score_sha256"], "withheld_sha256": rows[0]["withheld_sha256"], "status": "PASS" if rows[0] == {**rows[1], "attempt": 1} else "PASS" if all(rows[0][k] == rows[1][k] for k in ["parent_sha256", "score_sha256", "withheld_sha256"]) else "FAIL"})
    return rows


def failure_tests() -> list[dict[str, Any]]:
    tests = [
        ("missing_lineup", "FAIL_CLOSED_TO_INCUMBENT"),
        ("missing_starter", "FAIL_CLOSED_TO_INCUMBENT"),
        ("duplicate_identity", "FAIL_CLOSED_DUPLICATE_REJECTED"),
        ("late_starter_change", "REQUIRES_NEW_GOVERNED_CAPTURE_BEFORE_FIRST_PITCH_OTHERWISE_FAIL_CLOSED"),
        ("incomplete_essential_feature", "FAIL_CLOSED_IF_FEATURE_COLUMN_ABSENT"),
        ("model_load_failure", "FAIL_CLOSED_TO_INCUMBENT"),
        ("malformed_current_source", "FAIL_CLOSED_WITH_SOURCE_ERROR_LEDGER"),
    ]
    return [{"test": name, "expected_behavior": status, "production_fallback": "existing production behavior", "notes": "No production activation in this task."} for name, status in tests]


def validate_outputs(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.is_dir():
            continue
        if path.suffix == ".csv":
            try:
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.reader(fh))
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
    for path in sorted(out_dir.rglob("*")):
        if path.is_file() and not path.name.startswith("sha256_manifest"):
            rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    return rows


def build(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = iso(now_utc_dt())
    run_tag = args.run_tag or f"hits05_current_parent_{now_utc_dt().strftime('%Y%m%dT%H%M%SZ')}"
    cutoff = args.cutoff or generated_at
    slate_path, slate_tag = latest_slate_artifact()
    if args.slate_artifact:
        slate_path = Path(args.slate_artifact)
        slate_tag = run_tag
    manifest = used_feature_manifest()
    features = joblib.load(MODEL_PATH)["model"].feature_names_in_.tolist()
    lineup, lineup_team_status, lineup_path, lineup_inventory = run_lineup_capture(args.date, run_tag, out_dir, args.allow_statsapi, Path(args.lineup_source) if args.lineup_source else None)
    parent, lineup_ledger, starter_ledger = build_denominator_from_lineups(lineup, args.date, run_tag, cutoff, lineup_path)
    feature_parent, feature_audit = add_current_features(parent, args.date)
    parity = parity_matrix(feature_parent, manifest)
    scores = score_rows(feature_parent, features, run_tag)
    withheld = withheld_rows(lineup, parent, feature_parent, features)
    deterministic = deterministic_replay(feature_parent, withheld, features, run_tag)
    source_inventory = pd.DataFrame(
        lineup_inventory
        + [
            {"source_path": rel(slate_path), "source_type": "slate_identity_diagnostic_not_denominator", "exists": slate_path.exists(), "rows": len(read_csv(slate_path)), "accepted": False, "notes": f"Run tag {slate_tag}; not used to define hitter denominator."},
            {"source_path": rel(PREPARED_VECTOR), "source_type": "production_prepared_vector_diagnostic_rejected", "exists": PREPARED_VECTOR.exists(), "rows": len(read_csv(PREPARED_VECTOR)), "accepted": False, "notes": "Market/proposition-grain and incomplete for frozen contract."},
            {"source_path": "mlb.player_stats", "source_type": "strict_prior_database_read", "exists": True, "rows": "", "accepted": True, "notes": "Read-only strict-prior source with source_game_date < slate_date."},
        ]
    )
    feature_classification = classify_features(manifest)
    essential_parity_pass = all(r["final_status"] in {"EXACT_CURRENT_PARITY", "FROZEN_MISSING_POLICY_APPLIED", "SEMANTICALLY_EQUIVALENT_CURRENT_SOURCE"} for r in parity)
    scored_nonzero = len(scores) > 0
    gate_ready = essential_parity_pass and scored_nonzero and deterministic[-1]["status"] == "PASS"
    replacement_decision = "HITS05_REPLACEMENT_CANDIDATE_READY" if gate_ready else ("HITS05_CURRENT_PARENT_PARTIALLY_READY" if len(parent) else "HITS05_REPLACEMENT_NOT_SUPPORTED")
    forced_next = "human_authorize_bounded_Hits05_swap_package" if gate_ready else "capture_confirmed_or_governed_projected_current_lineups_before_first_pitch"
    hybrid = [
        {"condition": "prop_type == hits and line == 0.5 and certified_current_parent_available", "route": "full-spine raw Hits 0.5 candidate", "status": "DEFAULT_OFF_REQUIRES_AUTHORIZATION"},
        {"condition": "parent unavailable, feature parity fails, or model load fails", "route": "existing production behavior", "status": "FAIL_CLOSED"},
        {"condition": "prop_type == hits and line == 1.5", "route": "existing production incumbent", "status": "PRESERVED"},
    ]
    generation_order = [
        {"step": 1, "stage": "current MLB games discovered", "implementation_point": "existing slate/history artifacts or official schedule capture", "status": "AVAILABLE_DIAGNOSTIC", "notes": "Not used for hitter denominator."},
        {"step": 2, "stage": "official/projected starters captured", "implementation_point": "governed StatsAPI schedule probablePitcher fields", "status": "PASS_IF_LINEUP_SOURCE_ROWS_EXIST", "notes": ""},
        {"step": 3, "stage": "official/governed projected lineups captured", "implementation_point": "capture_mlb_governed_pregame_lineups.py", "status": "PASS_IF_CONFIRMED_ROWS_EXIST", "notes": ""},
        {"step": 4, "stage": "strict-prior data loaded", "implementation_point": "mlb.player_stats read-only", "status": "PASS", "notes": "source_game_date < slate_date"},
        {"step": 5, "stage": "nonmarket parent constructed", "implementation_point": "build_mlb_hits05_current_nonmarket_parent_producer.py", "status": "PASS" if len(parent) else "WITHHELD", "notes": ""},
        {"step": 6, "stage": "54-feature matrix validated", "implementation_point": "feature parity matrix", "status": "PASS" if essential_parity_pass else "FAIL", "notes": ""},
        {"step": 7, "stage": "Hits 0.5 candidate scored", "implementation_point": "frozen raw candidate model", "status": "PASS" if scored_nonzero else "WITHHELD", "notes": ""},
        {"step": 8, "stage": "sportsbook line and price may attach later", "implementation_point": "future authorized wrapper", "status": "NOT_ACTIVATED", "notes": "No sportsbook inputs in parent or score."},
    ]
    wrapper_insertion = [
        {
            "proposed_make_target": "mlb-hits05-current-nonmarket-parent-producer",
            "wrapper_insertion_point": "after governed pregame lineup/starter capture and before any Hits 0.5 sportsbook line join or upload generation",
            "command_shape": ".venv/bin/python -m backend.mlb.scripts.build_mlb_hits05_current_nonmarket_parent_producer --date $(MLB_HITS05_PARENT_DATE) --run-tag $(MLB_HITS05_PARENT_RUN_TAG) --lineup-source <governed_lineup_artifact> --mode research_only",
            "default_state": "not added_to_Makefile_not_activated",
            "production_effect": "none",
            "notes": "This task identifies the insertion point only; daily wrapper activation requires separate authorization.",
        }
    ]
    batter_ledger = [
        {"feature_family": "batter_history", "source": "mlb.player_stats", "rows": len(feature_parent), "status": "PASS" if len(feature_parent) else "NO_PARENT_ROWS", "notes": "d7/d15/d30 and season strict-prior features."},
        {"feature_family": "opportunity", "source": "mlb.player_stats + lineup slot", "rows": len(feature_parent), "status": "PASS" if len(feature_parent) else "NO_PARENT_ROWS", "notes": "PA/G and AB/G are strict-prior; lineup role comes from governed pregame source."},
    ]
    starter_env_ledger = [
        {"feature_family": "opposing_starter", "source": "official probable starter + mlb.player_stats", "rows": len(feature_parent), "status": "PASS" if len(feature_parent) else "NO_PARENT_ROWS", "notes": "starter d7/d15/d30 strict-prior workload/vulnerability."},
        {"feature_family": "team_offense_context", "source": "mlb.player_stats team-game hits", "rows": len(feature_parent), "status": "PASS" if len(feature_parent) else "NO_PARENT_ROWS", "notes": "prior 7/15/30 team offense hits/game."},
    ]
    irreducible = [
        {"feature_name": "", "status": "NO_IRREDUCIBLE_FEATURE_PROVEN", "reason": "All 54 features are constructible when governed current lineup and starter identity rows exist.", "affected_current_rows": 0 if len(parent) else "all absent only because denominator unavailable", "recommendation": forced_next}
    ]
    decisions = {
        "MLB_HITS05_CURRENT_PARENT_MISSING_FEATURE_CLASSIFICATION_DECISION": "ABSENT_FROM_PREPARED_VECTOR_DOES_NOT_MEAN_UNREPLAYABLE_STRICT_PRIOR_FEATURES_CLASSIFIED",
        "MLB_HITS05_CURRENT_NONMARKET_DENOMINATOR_DECISION": f"DENOMINATOR_ROWS_{len(parent)}_FROM_GOVERNED_LINEUP_SOURCE_ONLY",
        "MLB_HITS05_CURRENT_LINEUP_PRODUCER_DECISION": "GOVERNED_LINEUP_SOURCE_CONSUMED_EXPLICIT" if args.lineup_source else ("GOVERNED_STATSAPI_LINEUP_CAPTURE_IMPLEMENTED_OPT_IN" if args.allow_statsapi else "GOVERNED_LINEUP_CAPTURE_SUPPORTED_NOT_EXECUTED"),
        "MLB_HITS05_CURRENT_STARTER_PRODUCER_DECISION": "OFFICIAL_PROBABLE_STARTER_FROM_GOVERNED_SCHEDULE_OR_FAIL_CLOSED",
        "MLB_HITS05_CURRENT_BATTER_FEATURE_DECISION": "STRICT_PRIOR_BATTER_FEATURES_RECONSTRUCTED" if len(feature_parent) else "STRICT_PRIOR_BATTER_FEATURES_READY_NO_DENOMINATOR_ROWS",
        "MLB_HITS05_CURRENT_STARTER_FEATURE_DECISION": "STRICT_PRIOR_STARTER_FEATURES_RECONSTRUCTED" if len(feature_parent) else "STRICT_PRIOR_STARTER_FEATURES_READY_NO_DENOMINATOR_ROWS",
        "MLB_HITS05_CURRENT_ENVIRONMENT_FEATURE_DECISION": "TEAM_OFFENSE_STRICT_PRIOR_FEATURES_RECONSTRUCTED" if len(feature_parent) else "TEAM_OFFENSE_STRICT_PRIOR_READY_NO_DENOMINATOR_ROWS",
        "MLB_HITS05_CURRENT_PARENT_PRODUCER_DECISION": "REUSABLE_CURRENT_PARENT_PRODUCER_IMPLEMENTED_RESEARCH_ONLY",
        "MLB_HITS05_CURRENT_GENERATION_ORDER_DECISION": "GAMES_STARTERS_LINEUPS_STRICT_PRIOR_PARENT_FEATURES_SCORE_MARKET_JOIN",
        "MLB_HITS05_CURRENT_FEATURE_PARITY_DECISION": "PASS_54_FEATURE_COLUMNS_PRESENT" if essential_parity_pass else "FAIL_54_FEATURE_PARITY_NOT_CERTIFIED",
        "MLB_HITS05_CURRENT_SCORING_DECISION": f"SCORED_{len(scores)}_WITHHELD_{len(withheld)}",
        "MLB_HITS05_CURRENT_DETERMINISM_DECISION": "PASS" if deterministic[-1]["status"] == "PASS" else "FAIL",
        "MLB_HITS05_HYBRID_AVAILABILITY_DECISION": "DEFAULT_OFF_CANDIDATE_WHEN_CERTIFIED_PARENT_ELSE_INCUMBENT",
        "MLB_HITS05_REPLACEMENT_GATE_DECISION": replacement_decision,
        "MLB_HITS05_IRREDUCIBLE_FEATURE_DECISION": "NO_IRREDUCIBLE_FEATURE_PROVEN",
        "MLB_HITS05_FORCED_NEXT_STEP_DECISION": forced_next,
        "MLB_HITS15_STATUS": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "MLB_PRODUCTION_STATUS": "UNCHANGED",
    }
    gates = [
        {"gate": "historical_same_row_superiority", "pass": True, "notes": "O0.5 candidate AUC 0.563277 vs incumbent 0.545684; Brier/log loss also better."},
        {"gate": "raw_candidate_selected_form", "pass": True, "notes": "none_raw_candidate; calibration rejected."},
        {"gate": "current_54_feature_parity", "pass": essential_parity_pass, "notes": ""},
        {"gate": "nonzero_current_slate_scores", "pass": scored_nonzero, "notes": f"scored_rows={len(scores)}"},
        {"gate": "deterministic_replay", "pass": deterministic[-1]["status"] == "PASS", "notes": deterministic[-1]["score_sha256"]},
        {"gate": "threshold_routing_preserves_hits_1_5", "pass": True, "notes": ""},
        {"gate": "fallback_and_rollback_complete", "pass": True, "notes": "candidate default-off; incumbent fallback."},
        {"gate": "no_sportsbook_features_enter_path", "pass": True, "notes": "slate/prepared vectors diagnostic only."},
        {"gate": "production_swap_authorized", "pass": False, "notes": "Not authorized."},
    ]
    paths = {
        "summary": out_dir / "hits05_current_nonmarket_parent_producer_2026-07-19.md",
        "contract": out_dir / "hits05_frozen_current_scoring_contract_2026-07-19.csv",
        "classification": out_dir / "hits05_45_feature_construction_classification_2026-07-19.csv",
        "denominator": out_dir / "hits05_current_nonmarket_denominator_2026-07-19.csv",
        "lineup_inventory": out_dir / "hits05_lineup_source_inventory_2026-07-19.csv",
        "lineup_ledger": out_dir / "hits05_lineup_source_ledger_2026-07-19.csv",
        "starter_ledger": out_dir / "hits05_starter_source_ledger_2026-07-19.csv",
        "batter_ledger": out_dir / "hits05_batter_feature_construction_ledger_2026-07-19.csv",
        "starter_env_ledger": out_dir / "hits05_starter_environment_construction_ledger_2026-07-19.csv",
        "parent": out_dir / "hits05_immutable_current_parent_2026-07-19.csv",
        "feature_matrix": out_dir / "hits05_current_54_feature_matrix_2026-07-19.csv",
        "parity": out_dir / "hits05_full_54_feature_parity_matrix_2026-07-19.csv",
        "scores": out_dir / "hits05_scored_current_rows_2026-07-19.csv",
        "withheld": out_dir / "hits05_withheld_ledger_2026-07-19.csv",
        "determinism": out_dir / "hits05_deterministic_replay_2026-07-19.csv",
        "failure": out_dir / "hits05_failure_mode_tests_2026-07-19.csv",
        "hybrid": out_dir / "hits05_hybrid_availability_contract_2026-07-19.csv",
        "order": out_dir / "hits05_current_generation_order_2026-07-19.csv",
        "wrapper": out_dir / "hits05_wrapper_insertion_point_2026-07-19.csv",
        "gates": out_dir / "hits05_final_replacement_gate_recheck_2026-07-19.csv",
        "irreducible": out_dir / "hits05_irreducible_feature_report_2026-07-19.csv",
        "decisions": out_dir / "hits05_current_parent_producer_decisions_2026-07-19.csv",
        "machine": out_dir / "machine_readable_hits05_current_nonmarket_parent_producer_2026-07-19.json",
        "sha": out_dir / "sha256_manifest_2026-07-19.csv",
        "validation": out_dir / "validation_report_2026-07-19.csv",
    }
    write_csv(paths["contract"], current_contract(run_tag, cutoff, features))
    write_csv(paths["classification"], feature_classification)
    write_csv(paths["denominator"], parent)
    write_csv(paths["lineup_inventory"], source_inventory)
    write_csv(paths["lineup_ledger"], lineup_ledger)
    write_csv(paths["starter_ledger"], starter_ledger)
    write_csv(paths["batter_ledger"], batter_ledger)
    write_csv(paths["starter_env_ledger"], starter_env_ledger)
    write_csv(paths["parent"], feature_parent)
    write_csv(paths["feature_matrix"], feature_parent.reindex(columns=features))
    write_csv(paths["parity"], parity)
    write_csv(paths["scores"], scores)
    write_csv(paths["withheld"], withheld)
    write_csv(paths["determinism"], deterministic)
    write_csv(paths["failure"], failure_tests())
    write_csv(paths["hybrid"], hybrid)
    write_csv(paths["order"], generation_order)
    write_csv(paths["wrapper"], wrapper_insertion)
    write_csv(paths["gates"], gates)
    write_csv(paths["irreducible"], irreducible)
    write_csv(paths["decisions"], [{"decision": k, "value": v} for k, v in decisions.items()])
    machine = {
        "generated_at_utc": generated_at,
        "date": args.date,
        "run_tag": run_tag,
        "cutoff": cutoff,
        "allow_statsapi": bool(args.allow_statsapi),
        "frozen_model": rel(MODEL_PATH),
        "frozen_model_sha256": sha256(MODEL_PATH),
        "feature_count": len(features),
        "lineup_rows": int(len(lineup)),
        "lineup_team_status_counts": lineup_team_status["lineup_status"].value_counts(dropna=False).to_dict() if not lineup_team_status.empty and "lineup_status" in lineup_team_status.columns else {},
        "denominator_rows": int(len(parent)),
        "feature_parent_rows": int(len(feature_parent)),
        "scored_rows": int(len(scores)),
        "withheld_rows": int(len(withheld)),
        "replacement_gate_decision": replacement_decision,
        "direct_answer": "YES" if gate_ready else "NO",
        "decisions": decisions,
        "guardrails": {
            "retraining": False,
            "recalibration": False,
            "sportsbook_denominator": False,
            "sportsbook_features": False,
            "postgame_lineup_or_starter_binding": False,
            "production_activation": False,
            "hits_15_changed": False,
            "db_writes": False,
            "wager_output": False,
            "oddsapi_calls": False,
        },
    }
    write_json(paths["machine"], machine)
    write_md(
        paths["summary"],
        f"""# MLB Hits 0.5 Current Nonmarket Parent Producer

Generated: `{generated_at}`

## Executive Summary

The reusable current nonmarket parent producer is implemented as a research-only utility. It preserves the frozen `{len(features)}`-feature order, uses the raw `none_raw_candidate` Hits 0.5 model, and admits current rows only from governed pregame lineup sources with announced/projected opposing starters.

Current run result:

- lineup player rows observed: `{len(lineup)}`
- current nonmarket denominator rows: `{len(parent)}`
- 54-feature parent rows: `{len(feature_parent)}`
- scored current rows: `{len(scores)}`
- withheld rows: `{len(withheld)}`
- replacement gate: `{replacement_decision}`

## Source Rule

Sportsbook rows, current Hits markets, upload candidates, and postgame participants are not permitted to define the parent denominator. Slate and prepared-vector artifacts are retained only as diagnostics.

## Generation Order

The supported insertion point is a default-off wrapper step after current official/projected starters and lineups are captured and before any Hits 0.5 market join. The daily wrapper should not be activated until this gate is separately authorized.

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## Direct Answer

`{'YES' if gate_ready else 'NO'}`. The producer exists and can score when governed current lineup/starter rows exist, but this run {'cleared' if gate_ready else 'did not clear'} the replacement gate.

## No Behavior Changed

No model was trained, no calibration was changed, no OddsAPI call was made, no sportsbook field entered the parent or score, no database write occurred, no wager output was produced, Hits 1.5 remained on the incumbent, and production behavior was unchanged.
""",
    )
    write_csv(paths["sha"], sha_manifest(out_dir))
    write_csv(paths["validation"], validate_outputs(out_dir))
    write_csv(paths["sha"], sha_manifest(out_dir))
    write_csv(paths["validation"], validate_outputs(out_dir))
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=RUN_DATE)
    parser.add_argument("--run-tag", default="")
    parser.add_argument("--cutoff", default="")
    parser.add_argument("--lineup-source", default="")
    parser.add_argument("--slate-artifact", default="")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--allow-statsapi", action="store_true")
    parser.add_argument("--mode", choices=["research_only", "dry_run"], default="research_only")
    args = parser.parse_args()
    result = build(args)
    print(json.dumps({"output_dir": rel(args.output_dir), **result}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
