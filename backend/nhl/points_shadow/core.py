"""Frozen NHL Points scoring, ladder containment, and immutable shadow runs."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import shutil
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from backend.nhl.points_quote_capture.core import QUALIFIED, parse_utc, sha256_file, write_manifest
from backend.nhl.scripts.score_points_phoenix import load_line_model

ROOT = Path(__file__).resolve().parents[3]
IDENTITY_PATH = Path(__file__).with_name("frozen_points_v1.json")
GAME_TYPES = {1: "PRESEASON", 2: "REGULAR_SEASON", 3: "POSTSEASON"}
RUN_TYPES = {"MIDDAY", "FINAL_PREGAME"}
LINES = [0.5, 1.5, 2.5]
PLAYER_IDENTITY_COLUMNS = [
    "canonical_season", "slate_date", "game_id", "player_id", "player_name", "team",
    "opponent", "scheduled_start_time_utc", "game_type_code", "feature_cutoff_timestamp_utc",
    "feature_history_max_timestamp_utc", "pregame_participation_state", "roster_source_timestamp_utc",
]
POLICY_STATUS = "RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG"


def digest(value: Any) -> str:
    def serializable(item: Any) -> Any:
        if isinstance(item, np.generic):
            return item.item()
        if isinstance(item, pd.Timestamp):
            return item.isoformat()
        raise TypeError(f"not JSON serializable: {type(item).__name__}")
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=serializable)
    return hashlib.sha256(raw.encode()).hexdigest()


def _array_hash(*values: Any) -> str:
    out = hashlib.sha256()
    for value in values:
        array = np.asarray(value)
        out.update(str(array.dtype).encode())
        out.update(str(array.shape).encode())
        out.update(array.tobytes())
    return out.hexdigest()


def _configuration_hash(pipeline: Any) -> str:
    scaler, model = pipeline.named_steps["scaler"], pipeline.named_steps["lr"]
    return digest({
        "pipeline_steps": list(pipeline.named_steps),
        "scaler": {"with_mean": scaler.with_mean, "with_std": scaler.with_std},
        "classifier": model.get_params(),
    })


def frozen_identity() -> dict[str, Any]:
    return json.loads(IDENTITY_PATH.read_text())


def verify_frozen_identity() -> dict[str, Any]:
    identity = frozen_identity()
    scorer = ROOT / identity["scorer_path"]
    model_root = ROOT / identity["model_root"]
    if sha256_file(scorer) != identity["scorer_sha256"]:
        raise RuntimeError("POINTS_SCORER_CODE_HASH_DRIFT")
    if sha256_file(ROOT / identity["feature_construction"]["path"]) != identity["feature_construction"]["sha256"]:
        raise RuntimeError("POINTS_FEATURE_CONSTRUCTION_HASH_DRIFT")
    for line in LINES:
        expected = identity["models"][str(line)]
        directory = model_root / str(line).replace(".", "_")
        model_path, metadata_path = directory / "lr.joblib", directory / "feature_metadata.json"
        if sha256_file(model_path) != expected["joblib_sha256"]:
            raise RuntimeError(f"POINTS_MODEL_HASH_DRIFT:{line}")
        if sha256_file(metadata_path) != expected["feature_metadata_sha256"]:
            raise RuntimeError(f"POINTS_FEATURE_METADATA_HASH_DRIFT:{line}")
        metadata = json.loads(metadata_path.read_text())
        if digest(metadata["features"]) != expected["feature_order_sha256"]:
            raise RuntimeError(f"POINTS_FEATURE_ORDER_HASH_DRIFT:{line}")
        pipeline = joblib.load(model_path)
        if list(pipeline.named_steps) != ["scaler", "lr"]:
            raise RuntimeError(f"POINTS_PIPELINE_CONFIGURATION_DRIFT:{line}")
        scaler, model = pipeline.named_steps["scaler"], pipeline.named_steps["lr"]
        if _array_hash(scaler.mean_, scaler.scale_, scaler.var_, np.array([scaler.n_samples_seen_])) != expected["scaler_sha256"]:
            raise RuntimeError(f"POINTS_SCALER_HASH_DRIFT:{line}")
        if _array_hash(model.classes_, model.coef_, model.intercept_) != expected["coefficient_sha256"]:
            raise RuntimeError(f"POINTS_COEFFICIENT_HASH_DRIFT:{line}")
        if _configuration_hash(pipeline) != expected["configuration_sha256"]:
            raise RuntimeError(f"POINTS_CONFIGURATION_HASH_DRIFT:{line}")
    return identity


def score_frozen(features: pd.DataFrame, identity: dict[str, Any] | None = None) -> pd.DataFrame:
    """Execute the frozen three-classifier loop without changing its semantics."""
    identity = identity or verify_frozen_identity()
    for column in ("player_id", "game_id"):
        if column not in features:
            raise ValueError(f"POINTS_FEATURE_INPUT_MISSING:{column}")
    rows: list[dict[str, Any]] = []
    model_root = ROOT / identity["model_root"]
    for line in LINES:
        model_dir = model_root / str(line).replace(".", "_")
        feature_names, model = load_line_model(model_dir)
        missing = [name for name in feature_names if name not in features]
        if missing:
            raise ValueError(f"POINTS_FEATURE_INPUT_MISSING:{line}:{','.join(missing)}")
        matrix = features[feature_names].astype(float).values
        probabilities = model.predict_proba(matrix)[:, 1]
        for (player_id, game_id), probability in zip(zip(features.player_id.values, features.game_id.values), probabilities):
            rows.append({
                "player_id": int(player_id), "game_id": int(game_id), "line": float(line),
                "prob_over": float(probability), "model": identity["model_name"],
            })
    return pd.DataFrame(rows, columns=["player_id", "game_id", "line", "prob_over", "model"])


def verify_fixed_input_parity() -> dict[str, Any]:
    identity = verify_frozen_identity()
    input_path, output_path = ROOT / identity["fixed_input"]["path"], ROOT / identity["fixed_output"]["path"]
    if sha256_file(input_path) != identity["fixed_input"]["sha256"]:
        raise RuntimeError("POINTS_FIXED_INPUT_HASH_DRIFT")
    if sha256_file(output_path) != identity["fixed_output"]["sha256"]:
        raise RuntimeError("POINTS_RETAINED_OUTPUT_HASH_DRIFT")
    scored = score_frozen(pd.read_csv(input_path), identity)
    serialized = scored.to_csv(index=False).encode()
    if hashlib.sha256(serialized).hexdigest() != identity["fixed_output"]["sha256"]:
        raise RuntimeError("POINTS_FIXED_INPUT_EXACT_PARITY_FAILURE")
    return {
        "status": "EXACT_BYTE_PARITY", "input_rows": identity["fixed_input"]["rows"],
        "prediction_rows": len(scored), "input_sha256": identity["fixed_input"]["sha256"],
        "output_sha256": identity["fixed_output"]["sha256"],
    }


def evaluate_ladder_coherence(predictions: pd.DataFrame, identity: dict[str, Any] | None = None) -> pd.DataFrame:
    """Evaluate raw probabilities. This function never modifies prediction values."""
    identity = identity or frozen_identity()
    tolerance = float(identity["gate_contract"]["numerical_tolerance"])
    material = float(identity["gate_contract"]["material_crossing_probability"])
    rows = []
    for (game_id, player_id), group in predictions.groupby(["game_id", "player_id"], dropna=False, sort=True):
        counts = group.line.value_counts()
        exact = len(group) == 3 and set(group.line) == set(LINES) and counts.eq(1).all()
        model_ok = group.model.eq(identity["model_name"]).all()
        probability_ok = pd.to_numeric(group.prob_over, errors="coerce").between(0, 1, inclusive="both").all()
        if not exact or not model_ok or not probability_ok:
            values = {float(r.line): float(r.prob_over) for r in group.itertuples() if pd.notna(r.line) and pd.notna(r.prob_over)}
            c01 = c12 = max_cross = math.nan
            decision = "BLOCKED_LADDER_COHERENCE_NOT_EVALUABLE"
            reason = "INCOMPLETE_DUPLICATE_OR_IDENTITY_INCONSISTENT_LADDER"
        else:
            values = {float(r.line): float(r.prob_over) for r in group.itertuples()}
            c01, c12 = values[1.5] - values[0.5], values[2.5] - values[1.5]
            max_cross = max(c01, c12, 0.0)
            if max_cross >= material:
                decision, reason = "BLOCKED_MATERIAL_LADDER_INCOHERENCE", "MAX_ADJACENT_CROSSING_GE_1PP"
            elif max_cross > tolerance:
                decision, reason = "WARNING_MINOR_LADDER_INCOHERENCE", "MAX_ADJACENT_CROSSING_GT_NUMERIC_TOLERANCE_LT_1PP"
            else:
                decision, reason = "PASS_LADDER_COHERENCE", "NO_ADJACENT_CROSSING_BEYOND_NUMERIC_TOLERANCE"
        violating = []
        if pd.notna(c01) and c01 > tolerance:
            violating.append("0.5->1.5")
        if pd.notna(c12) and c12 > tolerance:
            violating.append("1.5->2.5")
        rows.append({
            "game_id": game_id, "player_id": player_id,
            "model_name": identity["model_name"], "model_version": identity["model_version"],
            "scorer_sha256": identity["scorer_sha256"],
            "model_joblib_sha256_0_5": identity["models"]["0.5"]["joblib_sha256"],
            "model_joblib_sha256_1_5": identity["models"]["1.5"]["joblib_sha256"],
            "model_joblib_sha256_2_5": identity["models"]["2.5"]["joblib_sha256"],
            "scaler_sha256_0_5": identity["models"]["0.5"]["scaler_sha256"],
            "scaler_sha256_1_5": identity["models"]["1.5"]["scaler_sha256"],
            "scaler_sha256_2_5": identity["models"]["2.5"]["scaler_sha256"],
            "coefficient_sha256_0_5": identity["models"]["0.5"]["coefficient_sha256"],
            "coefficient_sha256_1_5": identity["models"]["1.5"]["coefficient_sha256"],
            "coefficient_sha256_2_5": identity["models"]["2.5"]["coefficient_sha256"],
            "feature_order_sha256": identity["models"]["0.5"]["feature_order_sha256"],
            "p_over_0_5": values.get(0.5), "p_over_1_5": values.get(1.5), "p_over_2_5": values.get(2.5),
            "crossing_0_5_to_1_5_probability": c01, "crossing_0_5_to_1_5_pp": None if pd.isna(c01) else 100 * c01,
            "crossing_1_5_to_2_5_probability": c12, "crossing_1_5_to_2_5_pp": None if pd.isna(c12) else 100 * c12,
            "violating_adjacent_pairs": "|".join(violating),
            "maximum_adjacent_crossing_probability": max_cross,
            "maximum_adjacent_crossing_pp": None if pd.isna(max_cross) else 100 * max_cross,
            "ladder_coherence_decision": decision, "ladder_coherence_reason": reason,
            "numerical_tolerance": tolerance, "material_tolerance_probability": material,
        })
    return pd.DataFrame(rows)


def _read_manifest(path: Path) -> dict[str, str]:
    entries = {}
    for raw in path.read_text().splitlines():
        expected, name = raw.split("  ", 1)
        entries[name] = expected
    return entries


def _verify_manifest(directory: Path) -> None:
    manifest = directory / "SHA256SUMS"
    if not manifest.exists() or not (directory / "RUN_COMPLETE.json").exists():
        raise RuntimeError("PARENT_INCOMPLETE_OR_UNMANIFESTED")
    for name, expected in _read_manifest(manifest).items():
        if sha256_file(directory / name) != expected:
            raise RuntimeError(f"PARENT_HASH_MISMATCH:{name}")


def _validate_inputs(games: pd.DataFrame, players: pd.DataFrame, slate_date: str, run_timestamp_utc: str) -> None:
    required_game = {"canonical_season", "slate_date", "game_id", "home_team", "away_team", "scheduled_start_time_utc", "game_type_code"}
    missing_game = required_game - set(games)
    missing_player = set(PLAYER_IDENTITY_COLUMNS) - set(players)
    if missing_game or missing_player:
        raise ValueError(f"IDENTITY_SCHEMA_INCOMPLETE:games={sorted(missing_game)}:players={sorted(missing_player)}")
    if games.empty or games.game_id.duplicated().any() or not games.canonical_season.eq(2026).all():
        raise RuntimeError("GAME_SPINE_IDENTITY_FAILURE")
    if not games.slate_date.astype(str).eq(slate_date).all() or not games.game_type_code.isin(GAME_TYPES).all():
        raise RuntimeError("WRONG_SLATE_SEASON_OR_GAME_TYPE")
    starts = pd.to_datetime(games.scheduled_start_time_utc, utc=True, errors="coerce")
    run_stamp = parse_utc(run_timestamp_utc)
    if starts.isna().any() or (run_stamp >= starts).any():
        raise RuntimeError("RUN_NOT_STRICTLY_PREGAME")
    if players.empty or players.duplicated(["game_id", "player_id"]).any() or players.player_id.isna().any():
        raise RuntimeError("PLAYER_GAME_IDENTITY_FAILURE")
    participation = {"SCHEDULED", "ACTIVE", "SCRATCHED", "NONPARTICIPANT", "UNRESOLVED"}
    if not set(players.pregame_participation_state).issubset(participation):
        raise RuntimeError("UNKNOWN_PREGAME_PARTICIPATION_STATE")
    joined = players.merge(games, on="game_id", how="left", suffixes=("_player", "_game"), validate="many_to_one")
    team = joined.team.astype(str)
    valid_team = team.eq(joined.home_team.astype(str)) | team.eq(joined.away_team.astype(str))
    expected_opponent = np.where(team.eq(joined.home_team.astype(str)), joined.away_team, joined.home_team)
    valid_start = pd.to_datetime(joined.scheduled_start_time_utc_player, utc=True, errors="coerce").eq(pd.to_datetime(joined.scheduled_start_time_utc_game, utc=True, errors="coerce"))
    if joined.canonical_season_game.isna().any() or not joined.canonical_season_player.eq(joined.canonical_season_game).all():
        raise RuntimeError("PLAYER_GAME_PARENT_MISMATCH")
    if not joined.slate_date_player.astype(str).eq(joined.slate_date_game.astype(str)).all():
        raise RuntimeError("PLAYER_GAME_SLATE_MISMATCH")
    if not pd.to_numeric(joined.game_type_code_player, errors="coerce").eq(pd.to_numeric(joined.game_type_code_game, errors="coerce")).all():
        raise RuntimeError("PLAYER_GAME_TYPE_MISMATCH")
    if not valid_team.all() or not joined.opponent.astype(str).eq(pd.Series(expected_opponent, index=joined.index).astype(str)).all() or not valid_start.all():
        raise RuntimeError("PLAYER_GAME_ORIENTATION_MISMATCH")
    cutoff = pd.to_datetime(players.feature_cutoff_timestamp_utc, utc=True, errors="coerce")
    history_max = pd.to_datetime(players.feature_history_max_timestamp_utc, utc=True, errors="coerce")
    roster_time = pd.to_datetime(players.roster_source_timestamp_utc, utc=True, errors="coerce")
    player_start = pd.to_datetime(players.scheduled_start_time_utc, utc=True, errors="coerce")
    if cutoff.isna().any() or history_max.isna().any() or roster_time.isna().any() or (cutoff > run_stamp).any() or (cutoff >= player_start).any() or (history_max >= player_start).any() or (roster_time >= player_start).any():
        raise RuntimeError("STRICT_PRIOR_FEATURE_TIMING_FAILURE")


def _market_view(quotes: pd.DataFrame, shadow_run_id: str) -> pd.DataFrame:
    q = quotes[quotes.quote_qualification_status.isin(QUALIFIED) & quotes.canonical_prop_type.eq("player_points")].copy()
    q["line"] = pd.to_numeric(q.line, errors="coerce")
    keys = ["game_id", "player_id", "line"]
    rows = []
    for key, group in q.groupby(keys, dropna=False, sort=True):
        side = group.groupby("side").agg(
            median_american_price=("raw_price", "median"),
            median_decimal_price=("decimal_price", "median"),
            quote_count=("sportsbook", "size"),
        )
        evidence = group[["sportsbook", "provider_event_id", "provider_market_id", "provider_outcome_id", "raw_payload_sha256", "capture_timestamp_utc"]].sort_values(list(group[["sportsbook", "provider_event_id", "provider_market_id", "provider_outcome_id", "raw_payload_sha256", "capture_timestamp_utc"]].columns)).to_dict("records")
        row = {"run_id": shadow_run_id, "game_id": key[0], "player_id": key[1], "line": key[2]}
        for label in ["OVER", "UNDER"]:
            row[f"price_{label.lower()}"] = side.loc[label, "median_american_price"] if label in side.index else np.nan
            row[f"decimal_{label.lower()}"] = side.loc[label, "median_decimal_price"] if label in side.index else np.nan
            row[f"quote_count_{label.lower()}"] = int(side.loc[label, "quote_count"]) if label in side.index else 0
        row["sportsbooks"] = "|".join(sorted(set(group.sportsbook.astype(str))))
        row["market_evidence_sha256"] = digest(evidence)
        row["market_snapshot_identity"] = digest({"run_id": shadow_run_id, "key": key, "evidence": evidence})
        rows.append(row)
    columns = ["run_id", "game_id", "player_id", "line", "price_over", "decimal_over", "quote_count_over", "price_under", "decimal_under", "quote_count_under", "sportsbooks", "market_evidence_sha256", "market_snapshot_identity"]
    return pd.DataFrame(rows, columns=columns)


def make_run_id(slate_date: str, run_timestamp_utc: str, run_type: str) -> str:
    if run_type not in RUN_TYPES:
        raise ValueError("INVALID_RUN_TYPE")
    stamp = parse_utc(run_timestamp_utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"nhlpointsshadow_s2026_d{slate_date.replace('-', '')}_t{stamp}_{run_type}_v1"


def run_shadow(
    *, game_spine_csv: Path, game_spine_manifest: Path, player_inputs_csv: Path,
    player_inputs_manifest: Path, quote_run_dir: Path, output_root: Path,
    slate_date: str, run_timestamp_utc: str, run_type: str,
    effective_policy_json: Path | None = None,
) -> Path:
    """Create one immutable P/M observation run; C/U/E fail closed without policy."""
    if effective_policy_json is not None:
        raise RuntimeError("UNCERTIFIED_POINTS_POLICY_CONFIG_NOT_ACCEPTED")
    parity, identity = verify_fixed_input_parity(), verify_frozen_identity()
    run_id = make_run_id(slate_date, run_timestamp_utc, run_type)
    destination = output_root / "2026" / slate_date / run_id
    staging = destination.with_name(destination.name + ".incomplete")
    if destination.exists() or staging.exists():
        raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    lock_dir = output_root / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock = (lock_dir / f"{slate_date}_{run_type}.lock").open("a+")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise RuntimeError("POINTS_SHADOW_RUN_ALREADY_ACTIVE") from exc
    game_entries, player_entries = _read_manifest(game_spine_manifest), _read_manifest(player_inputs_manifest)
    if game_spine_csv.name not in game_entries or sha256_file(game_spine_csv) != game_entries[game_spine_csv.name]:
        raise RuntimeError("PARENT_GAME_SPINE_HASH_MISMATCH_OR_MUTABLE_INPUT")
    if player_inputs_csv.name not in player_entries or sha256_file(player_inputs_csv) != player_entries[player_inputs_csv.name]:
        raise RuntimeError("PLAYER_INPUT_HASH_MISMATCH_OR_MUTABLE_INPUT")
    games, players = pd.read_csv(game_spine_csv), pd.read_csv(player_inputs_csv)
    _validate_inputs(games, players, slate_date, run_timestamp_utc)
    _verify_manifest(quote_run_dir)
    quote_meta = json.loads((quote_run_dir / "run_metadata.json").read_text())
    if quote_meta.get("canonical_season") != 2026 or str(quote_meta.get("slate_date")) != slate_date or quote_meta.get("run_type") != run_type:
        raise RuntimeError("QUOTE_RUN_SLATE_OR_PHASE_MISMATCH")
    quotes = pd.read_csv(quote_run_dir / "points_quotes.csv")
    quote_capture = pd.to_datetime(quotes.capture_timestamp_utc, utc=True, errors="coerce")
    if quote_capture.isna().any() or (quote_capture > parse_utc(run_timestamp_utc)).any():
        raise RuntimeError("QUOTE_CAPTURE_AFTER_DECLARED_RUN_TIMESTAMP")
    raw = score_frozen(players, identity)
    ladder = evaluate_ladder_coherence(raw, identity)
    player_fields = players[PLAYER_IDENTITY_COLUMNS].copy()
    predictions = raw.merge(player_fields, on=["game_id", "player_id"], how="left", validate="many_to_one")
    predictions = predictions.merge(ladder[["game_id", "player_id", "ladder_coherence_decision", "ladder_coherence_reason", "maximum_adjacent_crossing_probability", "maximum_adjacent_crossing_pp"]], on=["game_id", "player_id"], validate="many_to_one")
    predictions["run_id"] = run_id
    predictions["run_type"] = run_type
    predictions["run_timestamp_utc"] = parse_utc(run_timestamp_utc).isoformat()
    predictions["prop_type"] = "player_points"
    predictions["side"] = "OVER"
    predictions["model_version"] = identity["model_version"]
    predictions["model_joblib_sha256"] = predictions.line.map(lambda x: identity["models"][str(float(x))]["joblib_sha256"])
    predictions["scaler_sha256"] = predictions.line.map(lambda x: identity["models"][str(float(x))]["scaler_sha256"])
    predictions["coefficient_sha256"] = predictions.line.map(lambda x: identity["models"][str(float(x))]["coefficient_sha256"])
    predictions["feature_order_sha256"] = predictions.line.map(lambda x: identity["models"][str(float(x))]["feature_order_sha256"])
    predictions["player_game_identity"] = predictions.apply(lambda r: digest({"season": 2026, "slate_date": slate_date, "game_id": int(r.game_id), "player_id": int(r.player_id), "run_id": run_id}), axis=1)
    predictions["prediction_identity"] = predictions.apply(lambda r: digest({"player_game_identity": r.player_game_identity, "line": float(r.line), "side": "OVER", "model_hash": r.model_joblib_sha256}), axis=1)
    market = _market_view(quotes, run_id)
    eligible_decisions = {"PASS_LADDER_COHERENCE", "WARNING_MINOR_LADDER_INCOHERENCE"}
    market_population = predictions[predictions.ladder_coherence_decision.isin(eligible_decisions)].merge(market, on=["run_id", "game_id", "player_id", "line"], how="inner", validate="one_to_one")
    ledger_rows = []
    for row in predictions.itertuples():
        ladder_pass = row.ladder_coherence_decision in eligible_decisions
        ledger_rows.extend([
            {"run_id": run_id, "prediction_identity": row.prediction_identity, "game_id": row.game_id, "player_id": row.player_id, "line": row.line, "side": "OVER", "rule_order": 10, "rule_id": "LADDER_COHERENCE_GATE", "rule_result": "PASS" if ladder_pass else "FAIL", "reason_code": row.ladder_coherence_reason if not ladder_pass else row.ladder_coherence_decision},
            {"run_id": run_id, "prediction_identity": row.prediction_identity, "game_id": row.game_id, "player_id": row.player_id, "line": row.line, "side": "OVER", "rule_order": 20, "rule_id": "EFFECTIVE_POINTS_POLICY_CONFIG", "rule_result": "FAIL" if ladder_pass else "NOT_EVALUATED_PRIOR_FAILURE", "reason_code": POLICY_STATUS if ladder_pass else "BLOCKED_BY_LADDER_GATE"},
        ])
    ledger = pd.DataFrame(ledger_rows)
    candidates = pd.DataFrame(columns=["run_id", "candidate_identity", "prediction_identity", "candidate_status", "reason_code"])
    manual = pd.DataFrame(columns=["manual_action_id", "run_id", "action", "actor", "reason", "timestamp_utc", "target_identity"])
    execution = pd.DataFrame(columns=["execution_identity", "run_id", "candidate_identity", "status", "timestamp_utc"])
    blocked_ids = set(ladder.loc[~ladder.ladder_coherence_decision.isin(eligible_decisions)].apply(lambda r: (r.game_id, r.player_id), axis=1))
    market_ids = set(market_population.apply(lambda r: (r.game_id, r.player_id), axis=1))
    blocked_leak = sorted(blocked_ids & market_ids)
    if blocked_leak:
        raise RuntimeError("BLOCKED_LADDER_ENTERED_MARKET_POPULATION")
    populations = pd.DataFrame([
        {"population_code": "P", "population": "PREDICTION", "rows": len(predictions), "eligible": True, "reason": "RAW_FROZEN_OUTPUT_RETAINED"},
        {"population_code": "M", "population": "MARKET_QUALIFIED", "rows": len(market_population), "eligible": True, "reason": "COHERENCE_ELIGIBLE_AND_BOUND_QUOTE"},
        {"population_code": "C", "population": "CANDIDATE", "rows": 0, "eligible": False, "reason": POLICY_STATUS},
        {"population_code": "U", "population": "UPLOAD", "rows": 0, "eligible": False, "reason": POLICY_STATUS},
        {"population_code": "E", "population": "EXECUTION", "rows": 0, "eligible": False, "reason": POLICY_STATUS},
        {"population_code": "G", "population": "GRADED", "rows": 0, "eligible": False, "reason": "SEPARATE_APPEND_ONLY_GRADING_RUN_REQUIRED"},
    ])
    sentinel_checks = [
        {"check": "frozen_identity", "state": "PASS", "critical": True, "evidence": identity["scorer_sha256"]},
        {"check": "fixed_input_parity", "state": "PASS", "critical": True, "evidence": parity["status"]},
        {"check": "blocked_ladder_population_leak", "state": "PASS" if not blocked_leak else "FAIL", "critical": True, "evidence": len(blocked_leak)},
        {"check": "post_start_qualified", "state": "PASS" if not ((quotes.quote_qualification_status.eq("POST_START_INVALID")) & quotes.quote_qualification_status.isin(QUALIFIED)).any() else "FAIL", "critical": True, "evidence": int(quotes.quote_qualification_status.eq("POST_START_INVALID").sum())},
        {"check": "candidate_policy", "state": "EXPECTED_FAIL_CLOSED", "critical": False, "evidence": POLICY_STATUS},
        {"check": "upload_execution_empty", "state": "PASS", "critical": True, "evidence": "U=0,E=0"},
    ]
    sentinel = {
        "schema_version": "nhl_points_live_failure_sentinel_v1", "run_id": run_id,
        "overall_status": "YELLOW_REDUCED_COVERAGE", "critical_failures": [],
        "bounded_reasons": [POLICY_STATUS], "checks": sentinel_checks,
    }
    staging.mkdir(parents=True, exist_ok=False)
    try:
        games.sort_values("game_id").to_csv(staging / "canonical_game_spine.csv", index=False)
        players.sort_values(["game_id", "player_id"]).to_csv(staging / "player_feature_snapshot.csv", index=False)
        predictions.sort_values(["game_id", "player_id", "line"]).to_csv(staging / "points_predictions.csv", index=False)
        ladder.sort_values(["game_id", "player_id"]).to_csv(staging / "ladder_coherence_diagnostics.csv", index=False)
        shutil.copy2(quote_run_dir / "raw_odds_response.json", staging / "raw_odds_response.json")
        quotes.sort_values(["game_id", "player_id", "line", "side", "sportsbook"], na_position="last").to_csv(staging / "points_quotes.csv", index=False)
        market.sort_values(["game_id", "player_id", "line"]).to_csv(staging / "market_view_derivation.csv", index=False)
        market_population.sort_values(["game_id", "player_id", "line"]).to_csv(staging / "market_qualified_population.csv", index=False)
        (staging / "candidate_policy_effective_config.json").write_text(json.dumps({"status": POLICY_STATUS, "source_evidence": "frontend research ranking only; no governed Points candidate policy", "silent_defaults_used": False, "candidate_creation_allowed": False}, indent=2, sort_keys=True) + "\n")
        ledger.sort_values(["prediction_identity", "rule_order"]).to_csv(staging / "candidate_rule_ledger.csv", index=False)
        candidates.to_csv(staging / "candidates.csv", index=False)
        manual.to_csv(staging / "manual_action_ledger.csv", index=False)
        execution.to_csv(staging / "execution_ledger.csv", index=False)
        populations.to_csv(staging / "population_membership.csv", index=False)
        (staging / "points_live_failure_sentinel.json").write_text(json.dumps(sentinel, indent=2, sort_keys=True) + "\n")
        metadata = {
            "schema_version": "nhl_points_immutable_shadow_v1", "run_id": run_id,
            "canonical_season": 2026, "slate_date": slate_date, "run_type": run_type,
            "run_timestamp_utc": parse_utc(run_timestamp_utc).isoformat(), "mode": "PRESEASON_SHADOW_OBSERVATION_ONLY",
            "game_spine_sha256": sha256_file(game_spine_csv), "game_spine_manifest_sha256": sha256_file(game_spine_manifest),
            "player_input_sha256": sha256_file(player_inputs_csv), "player_input_manifest_sha256": sha256_file(player_inputs_manifest),
            "quote_run_id": quote_meta["run_id"], "quote_manifest_sha256": sha256_file(quote_run_dir / "SHA256SUMS"),
            "frozen_identity_sha256": sha256_file(IDENTITY_PATH), "scorer_parity": parity,
            "ladder_counts": {str(key): int(value) for key, value in ladder.ladder_coherence_decision.value_counts().items()},
            "population_counts": {row.population_code: int(row.rows) for row in populations.itertuples()},
            "candidate_policy_status": POLICY_STATUS, "recommendations_generated": 0,
            "upload_rows": 0, "execution_rows": 0, "overall_status": "PASS_WITH_REDUCED_COVERAGE",
        }
        (staging / "run_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        (staging / "frozen_points_scorer_identity.json").write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")
        (staging / "RUN_COMPLETE.json").write_text(json.dumps({"run_id": run_id, "status": "COMPLETE"}, sort_keys=True) + "\n")
        write_manifest(staging, complete_only=True)
        staging.rename(destination)
    except BaseException:
        raise
    return destination


def grade_run(
    run_dir: Path, outcomes_csv: Path, grade_root: Path, grading_timestamp_utc: str,
    correction_of_grade_id: str | None = None,
) -> Path:
    """Append-only outcome observation. Pregame files are verified and untouched."""
    _verify_manifest(run_dir)
    before = {path.name: sha256_file(path) for path in run_dir.iterdir() if path.is_file()}
    metadata = json.loads((run_dir / "run_metadata.json").read_text())
    predictions, outcomes = pd.read_csv(run_dir / "points_predictions.csv"), pd.read_csv(outcomes_csv)
    required = {"canonical_season", "slate_date", "game_id", "player_id", "official_points", "participation_state", "outcome_source", "outcome_source_timestamp_utc", "source_correction_status"}
    if required - set(outcomes):
        raise ValueError(f"OUTCOME_SCHEMA_INCOMPLETE:{sorted(required-set(outcomes))}")
    if outcomes.duplicated(["canonical_season", "slate_date", "game_id", "player_id"]).any():
        raise RuntimeError("OUTCOME_IDENTITY_DUPLICATE")
    if not outcomes.canonical_season.eq(metadata["canonical_season"]).all() or not outcomes.slate_date.astype(str).eq(metadata["slate_date"]).all():
        raise RuntimeError("OUTCOME_RUN_IDENTITY_MISMATCH")
    allowed = {"SCHEDULED", "ACTIVE", "PARTICIPATED", "SCRATCHED", "NONPARTICIPANT", "UNRESOLVED", "POSTPONED"}
    if not set(outcomes.participation_state).issubset(allowed):
        raise RuntimeError("UNKNOWN_PARTICIPATION_STATE")
    merged = predictions.merge(outcomes, on=["canonical_season", "slate_date", "game_id", "player_id"], how="left", validate="many_to_one")
    grading_stamp = parse_utc(grading_timestamp_utc).isoformat()
    statuses, observed = [], []
    for row in merged.itertuples():
        if int(row.game_type_code) == 1:
            status, value = "PRESEASON_NON_EVALUATION", None
        elif int(row.game_type_code) != 2:
            status, value = "NON_REGULAR_SEASON_NON_EVALUATION", None
        elif pd.isna(row.participation_state) or row.participation_state in {"SCHEDULED", "ACTIVE", "UNRESOLVED"}:
            status, value = "UNRESOLVED_UNGRADED", None
        elif row.participation_state in {"SCRATCHED", "NONPARTICIPANT", "POSTPONED"}:
            status, value = "NONPARTICIPANT_UNGRADED", None
        elif row.participation_state == "PARTICIPATED" and pd.notna(row.official_points):
            status, value = "REGULAR_SEASON_OBSERVED", int(float(row.official_points) > float(row.line))
        else:
            status, value = "UNRESOLVED_UNGRADED", None
        statuses.append(status)
        observed.append(value)
    merged["grading_status"] = statuses
    merged["observed_over"] = pd.Series(observed, dtype="Int64")
    merged["grading_timestamp_utc"] = grading_stamp
    merged["correction_of_grade_id"] = correction_of_grade_id or ""
    grade_id = "points_grade_" + parse_utc(grading_timestamp_utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = grade_root / run_dir.name / grade_id
    staging = destination.with_name(destination.name + ".incomplete")
    if destination.exists() or staging.exists():
        raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    staging.mkdir(parents=True, exist_ok=False)
    merged.to_csv(staging / "graded_points_predictions.csv", index=False)
    grade_metadata = {
        "schema_version": "nhl_points_append_only_grading_v1", "grade_id": grade_id,
        "source_run_id": run_dir.name, "source_run_manifest_sha256": sha256_file(run_dir / "SHA256SUMS"),
        "grading_timestamp_utc": grading_stamp, "correction_of_grade_id": correction_of_grade_id,
        "rows": len(merged), "preseason_non_evaluation_rows": int(merged.grading_status.eq("PRESEASON_NON_EVALUATION").sum()),
        "regular_season_observed_rows": int(merged.grading_status.eq("REGULAR_SEASON_OBSERVED").sum()),
        "nonparticipant_ungraded_rows": int(merged.grading_status.eq("NONPARTICIPANT_UNGRADED").sum()),
        "entered_regular_season_feature_history_rows": 0,
    }
    (staging / "grading_metadata.json").write_text(json.dumps(grade_metadata, indent=2, sort_keys=True) + "\n")
    (staging / "RUN_COMPLETE.json").write_text(json.dumps({"grade_id": grade_id, "status": "COMPLETE"}, sort_keys=True) + "\n")
    write_manifest(staging, complete_only=True)
    staging.rename(destination)
    if before != {path.name: sha256_file(path) for path in run_dir.iterdir() if path.is_file()}:
        raise RuntimeError("PREGAME_RUN_MUTATED_DURING_GRADING")
    return destination
