"""Fail-closed game lineage for comprehensive NHL Points and Saves lanes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash


EASTERN = ZoneInfo("America/New_York")
POINTS_LINES = (0.5, 1.5, 2.5)
SAVES_LINES = tuple(float(value) + 0.5 for value in range(18, 31))
SOG_LINES = (1.5, 2.5, 3.5)
SOG_WIDE_LINE_COLUMNS = {1.5: "p_over_1_5", 2.5: "p_over_2_5", 3.5: "p_over_3_5"}
LINEAGE_COLUMNS = (
    "game_date",
    "game_start_utc",
    "home_team_id",
    "away_team_id",
    "parent_daily_run_id",
    "feature_input_cutoff_utc",
    "canonical_game_set_hash",
)


@dataclass(frozen=True)
class CanonicalGameLineage:
    game_id: int
    game_date: str
    game_start_utc: str
    home_team_id: int
    away_team_id: int


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _strict_integer_ids(series: pd.Series, *, label: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().any():
        raise RuntimeError(f"{label}_NULL_OR_INVALID")
    fractional = numeric.astype(float) % 1
    if (fractional != 0).any():
        raise RuntimeError(f"{label}_NON_INTEGER")
    result = numeric.astype("int64")
    if (result <= 0).any():
        raise RuntimeError(f"{label}_NONPOSITIVE")
    return result


def build_canonical_game_map(
    canonical_games: Iterable[Any], *, slate: str,
    expected_game_set_hash: str | None = None,
) -> dict[int, CanonicalGameLineage]:
    """Build the sole allowed game-id-to-date mapping from the verified spine."""
    result: dict[int, CanonicalGameLineage] = {}
    for game in canonical_games:
        game_id = int(_field(game, "game_id"))
        start_raw = str(_field(game, "start_time_utc") or "").strip()
        if not start_raw:
            raise RuntimeError(f"CANONICAL_GAME_START_MISSING:{game_id}")
        try:
            start = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
        except ValueError as error:
            raise RuntimeError(f"CANONICAL_GAME_START_INVALID:{game_id}") from error
        if start.tzinfo is None:
            raise RuntimeError(f"CANONICAL_GAME_START_NAIVE:{game_id}")
        game_date = start.astimezone(EASTERN).date().isoformat()
        if game_date != slate:
            raise RuntimeError(
                f"CANONICAL_GAME_DATE_MISMATCH:{game_id}:{game_date}:{slate}")
        home_team_id = _field(game, "home_team_id")
        away_team_id = _field(game, "away_team_id")
        if home_team_id is None or away_team_id is None:
            raise RuntimeError(f"CANONICAL_TEAM_ID_MISSING:{game_id}")
        if game_id in result:
            raise RuntimeError(f"CANONICAL_GAME_ID_DUPLICATE:{game_id}")
        result[game_id] = CanonicalGameLineage(
            game_id=game_id,
            game_date=game_date,
            game_start_utc=start.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z"),
            home_team_id=int(home_team_id),
            away_team_id=int(away_team_id),
        )
    if not result:
        raise RuntimeError("CANONICAL_GAME_MAP_EMPTY")
    actual_hash = canonical_game_set_hash(result)
    if expected_game_set_hash is not None and actual_hash != expected_game_set_hash:
        raise RuntimeError("CANONICAL_GAME_SET_HASH_MISMATCH")
    return result


def prepare_scoring_input(
    *, source_path: Path, output_path: Path, canonical_games: Iterable[Any],
    slate: str, parent_daily_run_id: str, feature_input_cutoff_utc: str,
    expected_game_set_hash: str, identity_column: str = "player_id",
    allow_partial_slate: bool = False,
    constant_feature_values: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """Validate a feature export and write a run-local, canonically joined input."""
    source_path, output_path = Path(source_path), Path(output_path)
    frame = pd.read_csv(source_path)
    required = {identity_column, "game_id"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"SCORING_INPUT_REQUIRED_COLUMNS_MISSING:{','.join(missing)}")
    if frame.empty:
        raise RuntimeError("SCORING_INPUT_EMPTY")

    canonical = build_canonical_game_map(
        canonical_games, slate=slate, expected_game_set_hash=expected_game_set_hash)
    frame[identity_column] = _strict_integer_ids(
        frame[identity_column], label="SCORING_INPUT_IDENTITY")
    frame["game_id"] = _strict_integer_ids(frame["game_id"], label="SCORING_INPUT_GAME_ID")
    observed_games = set(frame["game_id"].tolist())
    unknown = sorted(observed_games - set(canonical))
    if unknown:
        raise RuntimeError(f"SCORING_INPUT_NONCANONICAL_GAMES:{unknown}")
    if not allow_partial_slate and observed_games != set(canonical):
        missing_games = sorted(set(canonical) - observed_games)
        raise RuntimeError(f"SCORING_INPUT_PARTIAL_SLATE:{missing_games}")
    if frame.duplicated([identity_column, "game_id"]).any():
        raise RuntimeError("SCORING_INPUT_DUPLICATE_IDENTITY")

    mapped_dates = frame["game_id"].map(lambda game_id: canonical[int(game_id)].game_date)
    if "game_date" in frame.columns:
        source_dates = frame["game_date"].astype("string").str.strip()
        if source_dates.isna().any() or (source_dates == "").any():
            raise RuntimeError("SCORING_INPUT_GAME_DATE_NULL")
        conflicts = source_dates != mapped_dates.astype("string")
        if conflicts.any():
            raise RuntimeError("SCORING_INPUT_GAME_DATE_CONFLICT")
    frame["game_date"] = mapped_dates
    frame["game_start_utc"] = frame["game_id"].map(
        lambda game_id: canonical[int(game_id)].game_start_utc)
    frame["home_team_id"] = frame["game_id"].map(
        lambda game_id: canonical[int(game_id)].home_team_id)
    frame["away_team_id"] = frame["game_id"].map(
        lambda game_id: canonical[int(game_id)].away_team_id)
    if not str(parent_daily_run_id).strip():
        raise RuntimeError("PARENT_DAILY_RUN_ID_MISSING")
    if not str(feature_input_cutoff_utc).strip():
        raise RuntimeError("FEATURE_INPUT_CUTOFF_MISSING")
    frame["parent_daily_run_id"] = parent_daily_run_id
    frame["feature_input_cutoff_utc"] = feature_input_cutoff_utc
    frame["canonical_game_set_hash"] = expected_game_set_hash

    # Some lanes have an explicit operational feature contract. Apply those
    # values after row eligibility and identity validation, before writing the
    # scorer input, so generic missing-value handling cannot
    # manufacture their semantic value.
    for feature, value in (constant_feature_values or {}).items():
        if feature not in frame.columns:
            raise RuntimeError(f"SCORING_INPUT_CONTRACT_FEATURE_MISSING:{feature}")
        frame[feature] = float(value)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.tmp")
    frame.to_csv(temporary, index=False)
    temporary.replace(output_path)
    return {
        "path": str(output_path.resolve()),
        "row_count": len(frame),
        "identity_count": len(frame),
        "canonical_game_count": len(canonical),
        "canonical_game_set_hash": expected_game_set_hash,
        "parent_daily_run_id": parent_daily_run_id,
        "feature_input_cutoff_utc": feature_input_cutoff_utc,
    }


def wide_line_column(line: float) -> str:
    return f"p_over_{line:.1f}".replace(".", "_")


def bind_scorer_output_lineage(
    *, scoring_input: Path, unbound_output: Path, output: Path,
    output_cardinality: str,
) -> None:
    """Join a frozen scorer output to canonical lineage by exact natural identity."""
    source = pd.read_csv(scoring_input)
    predictions = pd.read_csv(unbound_output)
    identity_columns = ["player_id", "game_id"]
    required_source = [*identity_columns, *LINEAGE_COLUMNS]
    missing_source = sorted(set(required_source) - set(source.columns))
    if missing_source:
        raise RuntimeError(f"SCORING_INPUT_LINEAGE_MISSING:{','.join(missing_source)}")
    missing_output = sorted(set(identity_columns) - set(predictions.columns))
    if missing_output:
        raise RuntimeError(f"SCORER_IDENTITY_MISSING:{','.join(missing_output)}")
    if source.empty or predictions.empty:
        raise RuntimeError("SCORER_POPULATION_EMPTY")
    if source[required_source].isna().any().any():
        raise RuntimeError("SCORING_INPUT_LINEAGE_NULL")
    if source.duplicated(identity_columns).any():
        raise RuntimeError("SCORING_INPUT_IDENTITY_DUPLICATE")
    if predictions[identity_columns].isna().any().any():
        raise RuntimeError("SCORER_IDENTITY_NULL")
    if output_cardinality == "one_to_one":
        if predictions.duplicated(identity_columns).any():
            raise RuntimeError("SCORER_IDENTITY_DUPLICATE")
        merge_validation = "one_to_one"
    elif output_cardinality == "many_to_one":
        merge_validation = "many_to_one"
    else:
        raise ValueError(f"unsupported scorer output cardinality: {output_cardinality}")

    source_keys = set(map(tuple, source[identity_columns].itertuples(index=False, name=None)))
    output_keys = set(map(tuple, predictions[identity_columns].itertuples(index=False, name=None)))
    if source_keys != output_keys:
        raise RuntimeError("SCORER_IDENTITY_SET_MISMATCH")
    lineage = source[required_source]
    bound = predictions.merge(
        lineage, on=identity_columns, how="left", validate=merge_validation)
    ordered = [*identity_columns, *LINEAGE_COLUMNS]
    ordered.extend(column for column in bound.columns if column not in ordered)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.bound.tmp")
    bound[ordered].to_csv(temporary, index=False)
    temporary.replace(output)


def expand_wide_predictions(
    frame: pd.DataFrame, *, lines: Sequence[float], probability_name: str = "prob_over",
) -> pd.DataFrame:
    """Expand governed wide probabilities while retaining every lineage field."""
    id_columns = ["player_id", "game_id", *LINEAGE_COLUMNS]
    missing = sorted(set(id_columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"PREDICTION_LINEAGE_COLUMNS_MISSING:{','.join(missing)}")
    chunks: list[pd.DataFrame] = []
    for line in lines:
        column = wide_line_column(float(line))
        if column not in frame.columns:
            raise RuntimeError(f"PREDICTION_LINE_COLUMN_MISSING:{column}")
        chunk = frame[id_columns].copy()
        chunk["line"] = float(line)
        chunk[probability_name] = frame[column]
        chunks.append(chunk)
    if not chunks:
        return pd.DataFrame(columns=[*id_columns, "line", probability_name])
    return pd.concat(chunks, ignore_index=True)


def validate_prediction_output(
    *, path: Path, lane: str, canonical_games: Iterable[Any], slate: str,
    parent_daily_run_id: str, feature_input_cutoff_utc: str,
    expected_game_set_hash: str, expected_lines: Sequence[float],
) -> dict[str, Any]:
    """Validate a run-local scorer artifact before any DB or market consumer."""
    path = Path(path)
    if not path.is_file() or path.stat().st_size == 0:
        raise RuntimeError(f"CURRENT_RUN_PREDICTION_MISSING:{path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise RuntimeError("PREDICTION_OUTPUT_EMPTY")
    required = {"player_id", "game_id", *LINEAGE_COLUMNS}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"PREDICTION_LINEAGE_COLUMNS_MISSING:{','.join(missing)}")

    canonical = build_canonical_game_map(
        canonical_games, slate=slate, expected_game_set_hash=expected_game_set_hash)
    frame["player_id"] = _strict_integer_ids(frame["player_id"], label="PREDICTION_PLAYER_ID")
    frame["game_id"] = _strict_integer_ids(frame["game_id"], label="PREDICTION_GAME_ID")
    unknown = sorted(set(frame["game_id"]) - set(canonical))
    if unknown:
        raise RuntimeError(f"PREDICTION_NONCANONICAL_GAMES:{unknown}")
    for column in LINEAGE_COLUMNS:
        values = frame[column].astype("string").str.strip()
        if values.isna().any() or (values == "").any():
            raise RuntimeError(f"PREDICTION_LINEAGE_NULL:{column}")
    expected_dates = frame["game_id"].map(lambda game_id: canonical[int(game_id)].game_date)
    if not frame["game_date"].astype(str).eq(expected_dates).all():
        raise RuntimeError("PREDICTION_GAME_DATE_CONFLICT")
    expected_starts = frame["game_id"].map(lambda game_id: canonical[int(game_id)].game_start_utc)
    if not frame["game_start_utc"].astype(str).eq(expected_starts).all():
        raise RuntimeError("PREDICTION_GAME_START_CONFLICT")
    for column in ("home_team_id", "away_team_id"):
        actual = _strict_integer_ids(frame[column], label=f"PREDICTION_{column.upper()}")
        expected = frame["game_id"].map(
            lambda game_id: getattr(canonical[int(game_id)], column)).astype("int64")
        if not actual.eq(expected).all():
            raise RuntimeError(f"PREDICTION_{column.upper()}_CONFLICT")
    exact_values = {
        "parent_daily_run_id": parent_daily_run_id,
        "feature_input_cutoff_utc": feature_input_cutoff_utc,
        "canonical_game_set_hash": expected_game_set_hash,
    }
    for column, expected in exact_values.items():
        if set(frame[column].astype(str)) != {str(expected)}:
            raise RuntimeError(f"PREDICTION_{column.upper()}_CONFLICT")

    expected_lines = tuple(float(value) for value in expected_lines)
    if lane == "saves":
        parseable = {
            column for column in frame.columns
            if column.startswith("p_over_") and column[len("p_over_"):].replace("_", ".", 1).replace(".", "", 1).isdigit()
        }
        governed = {wide_line_column(line) for line in expected_lines}
        if parseable != governed:
            raise RuntimeError("PREDICTION_SAVES_LINE_SET_MISMATCH")
        expanded = expand_wide_predictions(frame, lines=expected_lines)
    elif lane == "points":
        if not {"line", "prob_over"}.issubset(frame.columns):
            raise RuntimeError("PREDICTION_POINTS_LINE_COLUMNS_MISSING")
        expanded = frame.copy()
    else:
        raise ValueError(f"unsupported prediction lane: {lane}")

    expanded["line"] = pd.to_numeric(expanded["line"], errors="coerce")
    expanded["prob_over"] = pd.to_numeric(expanded["prob_over"], errors="coerce")
    if expanded["line"].isna().any() or expanded["prob_over"].isna().any():
        raise RuntimeError("PREDICTION_LINE_OR_PROBABILITY_NULL")
    if not expanded["prob_over"].between(0.0, 1.0, inclusive="both").all():
        raise RuntimeError("PREDICTION_PROBABILITY_OUT_OF_RANGE")
    if set(expanded["line"].astype(float)) != set(expected_lines):
        raise RuntimeError("PREDICTION_LINE_SET_MISMATCH")
    natural_key = ["game_date", "game_id", "player_id", "line"]
    if expanded.duplicated(natural_key).any():
        raise RuntimeError("PREDICTION_DUPLICATE_NATURAL_KEY")
    groups = expanded.groupby(["game_id", "player_id"])["line"].agg(list)
    expected_line_set = set(expected_lines)
    if any(len(lines) != len(expected_lines) or set(map(float, lines)) != expected_line_set for lines in groups):
        raise RuntimeError("PREDICTION_LINE_POPULATION_MISMATCH")
    if set(expanded["game_id"]) != set(canonical):
        raise RuntimeError("PREDICTION_PARTIAL_SLATE")

    return {
        "row_count": len(frame),
        "natural_identity_count": len(groups),
        "conditional_prediction_count": len(expanded),
        "line_count": len(expected_lines),
        "canonical_game_count": len(canonical),
        "canonical_game_set_hash": expected_game_set_hash,
        "parent_daily_run_id": parent_daily_run_id,
        "feature_input_cutoff_utc": feature_input_cutoff_utc,
        "validated_prediction_identity": True,
    }


def validate_sog_prediction_artifacts(
    *, scored_path: Path, unscored_path: Path | None, slate: str,
    parent_daily_run_id: str, canonical_game_ids: Iterable[int],
    expected_game_set_hash: str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Validate legacy SOG prediction-grain artifacts and summarize their rows.

    The legacy scorer emits one wide row per scored player/game identity and a
    separate line-grain CSV for identities it could not score.  These counts
    are derived from those files; population and roster diagnostics are not
    used as substitutes.
    """
    scored_path = Path(scored_path)
    if not scored_path.is_file() or scored_path.stat().st_size == 0:
        raise RuntimeError(f"SOG_SCORED_ARTIFACT_MISSING:{scored_path}")
    if not parent_daily_run_id or scored_path.parent.name != parent_daily_run_id:
        raise RuntimeError("SOG_PREDICTION_RUN_ID_PATH_MISMATCH")

    game_ids = tuple(sorted({int(value) for value in canonical_game_ids}))
    if not game_ids or canonical_game_set_hash(game_ids) != expected_game_set_hash:
        raise RuntimeError("SOG_CANONICAL_GAME_SET_HASH_MISMATCH")
    canonical = set(game_ids)
    scored = pd.read_csv(scored_path)
    required = {"game_id", "player_id", "game_date", *SOG_WIDE_LINE_COLUMNS.values()}
    missing = sorted(required - set(scored.columns))
    if missing:
        raise RuntimeError(f"SOG_SCORED_COLUMNS_MISSING:{','.join(missing)}")
    if scored.empty:
        raise RuntimeError("SOG_SCORED_ARTIFACT_EMPTY")
    scored["game_id"] = _strict_integer_ids(scored["game_id"], label="SOG_GAME_ID")
    scored["player_id"] = _strict_integer_ids(scored["player_id"], label="SOG_PLAYER_ID")
    if not set(scored["game_id"]).issubset(canonical):
        raise RuntimeError("SOG_NONCANONICAL_GAME_ID")
    if not scored["game_date"].astype(str).eq(str(slate)).all():
        raise RuntimeError("SOG_SLATE_DATE_MISMATCH")
    scored_identity = ["game_id", "player_id"]
    if scored.duplicated(scored_identity).any():
        raise RuntimeError("SOG_DUPLICATE_SCORED_IDENTITY")
    for column in SOG_WIDE_LINE_COLUMNS.values():
        values = pd.to_numeric(scored[column], errors="coerce")
        if values.isna().any() or not values.between(0.0, 1.0, inclusive="both").all():
            raise RuntimeError(f"SOG_INVALID_LINE_PROBABILITY:{column}")

    scored_identity_count = int(scored[scored_identity].drop_duplicates().shape[0])
    scored_row_count = int(len(scored))
    line_count = len(SOG_WIDE_LINE_COLUMNS)
    scored_metadata: dict[str, Any] = {
        "canonical_game_count": len(canonical),
        "canonical_game_set_hash": expected_game_set_hash,
        "natural_identity_count": scored_identity_count,
        "line_count": line_count,
        "conditional_prediction_count": scored_row_count * line_count,
        "row_count": scored_row_count,
        "parent_daily_run_id": parent_daily_run_id,
        "validated_prediction_identity": True,
    }

    if unscored_path is None or not Path(unscored_path).is_file():
        scored_metadata["unscored_artifact_available"] = False
        return scored_metadata, None
    unscored_path = Path(unscored_path)
    if unscored_path.parent.name != parent_daily_run_id:
        raise RuntimeError("SOG_UNSCORED_RUN_ID_PATH_MISMATCH")
    scored_metadata["unscored_artifact_available"] = True
    unscored = pd.read_csv(unscored_path)
    unscored_required = {"game_id", "player_id", "line", "reason", "game_date"}
    missing = sorted(unscored_required - set(unscored.columns))
    if missing:
        raise RuntimeError(f"SOG_UNSCORED_COLUMNS_MISSING:{','.join(missing)}")
    if unscored.empty:
        unscored_metadata: dict[str, Any] = {
            "unscored_identity_count": 0,
            "unscored_row_count": 0,
            "unscored_reason_counts": {},
        }
        return scored_metadata, unscored_metadata
    unscored["game_id"] = _strict_integer_ids(
        unscored["game_id"], label="SOG_UNSCORED_GAME_ID")
    unscored["player_id"] = _strict_integer_ids(
        unscored["player_id"], label="SOG_UNSCORED_PLAYER_ID")
    if not set(unscored["game_id"]).issubset(canonical):
        raise RuntimeError("SOG_UNSCORED_NONCANONICAL_GAME_ID")
    if not unscored["game_date"].astype(str).eq(str(slate)).all():
        raise RuntimeError("SOG_UNSCORED_SLATE_DATE_MISMATCH")
    unscored["line"] = pd.to_numeric(unscored["line"], errors="coerce")
    if unscored["line"].isna().any():
        raise RuntimeError("SOG_UNSCORED_LINE_INVALID")
    unscored_key = ["game_id", "player_id", "line"]
    if unscored.duplicated(unscored_key).any():
        raise RuntimeError("SOG_DUPLICATE_UNSCORED_PREDICTION_KEY")
    expected_lines = set(SOG_LINES)
    for _, group in unscored.groupby(scored_identity, sort=False):
        observed_lines = set(group["line"].astype(float))
        if observed_lines != expected_lines or len(group) != len(SOG_LINES):
            raise RuntimeError("SOG_UNSCORED_LINE_POPULATION_MISMATCH")
        if group["reason"].isna().any() or group["reason"].astype(str).str.strip().eq("").any():
            raise RuntimeError("SOG_UNSCORED_REASON_MISSING")
        if group["reason"].nunique(dropna=False) != 1:
            raise RuntimeError("SOG_UNSCORED_REASON_CONFLICT")
    scored_keys = set(map(tuple, scored[scored_identity].drop_duplicates().to_numpy()))
    unscored_keys = set(map(tuple, unscored[scored_identity].drop_duplicates().to_numpy()))
    if scored_keys & unscored_keys:
        raise RuntimeError("SOG_SCORED_UNSCORED_IDENTITY_OVERLAP")
    unscored_metadata = {
        "unscored_identity_count": len(unscored_keys),
        "unscored_row_count": int(len(unscored)),
        # Counts are line-grain rows, matching the unscored artifact's grain.
        "unscored_reason_counts": {
            str(reason): int(count)
            for reason, count in unscored["reason"].value_counts(dropna=False).items()
        },
    }
    return scored_metadata, unscored_metadata
