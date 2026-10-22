from __future__ import annotations

import pandas as pd
import pytest

from backend.nhl.points_shadow.core import construct_coherent_ladders


FIXTURE = "backend/nhl/points_shadow/fixtures/retained_2026-04-16/points_predictions.csv"


def _crossing_count(frame: pd.DataFrame) -> int:
    wide = frame.pivot(index=["game_id", "player_id"], columns="line", values="prob_over")
    return int(((wide[1.5] > wide[0.5] + 1e-12) |
                 (wide[2.5] > wide[1.5] + 1e-12)).sum())


def test_retained_310_player_fixture_projects_222_crossings_to_zero():
    raw = pd.read_csv(FIXTURE)
    assert raw.groupby(["game_id", "player_id"]).size().eq(3).all()
    assert raw.groupby(["game_id", "player_id"]).ngroups == 310
    assert _crossing_count(raw) == 222

    coherent = construct_coherent_ladders(raw)
    assert _crossing_count(coherent) == 0
    assert coherent.raw_prob_over.equals(raw.prob_over)
    assert coherent.probability_construction.eq("EQUAL_WEIGHT_ISOTONIC_EXCEEDANCE_V1").all()


def test_probability_projection_preserves_line_meaning_and_row_identity():
    # Input order is deliberately scrambled. Each line still represents its
    # original threshold event; only the three probabilities are jointly fitted.
    raw = pd.DataFrame([
        {"game_id": 7, "player_id": 9, "line": 2.5, "prob_over": 0.2, "model": "m2"},
        {"game_id": 7, "player_id": 9, "line": 0.5, "prob_over": 0.4, "model": "m0"},
        {"game_id": 7, "player_id": 9, "line": 1.5, "prob_over": 0.8, "model": "m1"},
    ])
    result = construct_coherent_ladders(raw)
    by_line = result.set_index("line")
    assert by_line.loc[0.5, "raw_prob_over"] == 0.4
    assert by_line.loc[1.5, "raw_prob_over"] == 0.8
    assert by_line.loc[2.5, "raw_prob_over"] == 0.2
    assert by_line.loc[0.5, "prob_over"] == pytest.approx(0.6)
    assert by_line.loc[1.5, "prob_over"] == pytest.approx(0.6)
    assert by_line.loc[2.5, "prob_over"] == pytest.approx(0.2)
    assert by_line.loc[0.5, "model"] == "m0"
    assert by_line.loc[1.5, "model"] == "m1"
    assert by_line.loc[2.5, "model"] == "m2"
    assert result[["game_id", "player_id", "line"]].equals(raw[["game_id", "player_id", "line"]])


def test_partition_matches_strict_projection_on_valid_ladders():
    from backend.nhl.points_shadow.core import partition_coherent_ladders
    raw = pd.read_csv(FIXTURE)
    coherent, exclusions = partition_coherent_ladders(raw)
    assert exclusions.empty
    pd.testing.assert_frame_equal(coherent, construct_coherent_ladders(raw))


def test_partition_excludes_only_the_invalid_player_games():
    from backend.nhl.points_shadow.core import partition_coherent_ladders

    def ladder(player_id, values, lines=(0.5, 1.5, 2.5)):
        return [{"game_id": 7, "player_id": player_id, "line": line, "prob_over": value}
                for line, value in zip(lines, values)]

    raw = pd.DataFrame(
        ladder(1, (0.6, 0.3, 0.1))
        + ladder(2, (0.6, 0.3), lines=(0.5, 1.5))            # incomplete
        + ladder(3, (0.6, float("nan"), 0.1))                # non-finite
        + ladder(4, (1.2, 0.3, 0.1))                         # out of range
        + ladder(5, (0.6, 0.3, 0.1, 0.2), lines=(0.5, 1.5, 2.5, 2.5)))  # duplicate line
    coherent, exclusions = partition_coherent_ladders(raw)
    assert set(coherent.player_id) == {1}
    assert dict(zip(exclusions.player_id, exclusions.reason)) == {
        2: "POINTS_INCOMPLETE_LADDER", 3: "POINTS_INVALID_LADDER_PROBABILITY",
        4: "POINTS_INVALID_LADDER_PROBABILITY", 5: "POINTS_INCOMPLETE_LADDER"}
    with pytest.raises(ValueError, match="POINTS_INCOMPLETE_LADDER:7:2"):
        construct_coherent_ladders(raw)
