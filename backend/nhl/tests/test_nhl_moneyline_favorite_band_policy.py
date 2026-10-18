from backend.nhl.scripts.nhl_moneyline_favorite_band_policy import (
    decimal_odds,
    favorite_band,
    favorite_side,
    model_market_disagreement,
    model_pick,
    no_vig_home_probability,
    role_at,
    select_latest_quote_pair,
)


def test_fixed_market_favorite_probability_bands_and_pickem():
    assert [favorite_band(p) for p in (.50, .5499, .55, .60, .70, .91)] == [
        "PICKEM", "50_to_55", "55_to_60", "60_to_70", "70_or_more", "70_or_more"
    ]
    assert favorite_band(None) == "MISSING_PRICE"


def test_pick_orientation_and_no_vig_odds_conversion():
    assert model_pick(.5) == "HOME"
    assert model_pick(.499) == "AWAY"
    assert favorite_side(.5) == "PICKEM"
    assert decimal_odds(-150) == 1 + 100 / 150
    assert decimal_odds(+130) == 2.3
    assert round(no_vig_home_probability(-150, +130), 6) == round(
        (1 / (1 + 100 / 150)) / ((1 / (1 + 100 / 150)) + (1 / 2.3)), 6
    )


def test_disagreement_keeps_direction_and_magnitude_descriptive():
    result = model_market_disagreement(.58, .62)
    assert result["model_side"] == "HOME"
    assert result["market_favorite_side"] == "HOME"
    assert result["model_picks_market_underdog"] is False
    assert abs(result["signed_home_probability_gap"] + .04) < 1e-12
    dog = model_market_disagreement(.42, .62)
    assert dog["model_picks_market_underdog"] is True
    assert abs(dog["absolute_probability_gap"] - .2) < 1e-12


def test_quote_selection_requires_complete_pair_and_strict_chronology():
    def quote(side, obs, source="2026-10-17T18:00:00Z", capture="capture"):
        return {"sportsbook_key": "betonlineag", "quote_qualification_status": "QUALIFIED_AUDITED_MONEYLINE_LEDGER",
                "source_capture_identity": capture, "source_quote_timestamp_utc": source,
                "observation_timestamp_utc": obs, "quote_side": side, "american_price": "-110"}

    rows = [quote("HOME", "2026-10-17T18:01:00Z"), quote("AWAY", "2026-10-17T18:01:00Z")]
    pair = select_latest_quote_pair(rows, prediction="2026-10-17T18:02:00Z",
                                    scheduled_start="2026-10-17T20:00:00Z")
    assert set(pair) == {"HOME", "AWAY"}
    assert select_latest_quote_pair(rows[:1], prediction="2026-10-17T18:02:00Z",
                                    scheduled_start="2026-10-17T20:00:00Z") is None
    conflict = rows + [{**rows[0], "american_price": "-120"}]
    assert select_latest_quote_pair(conflict, prediction="2026-10-17T18:02:00Z",
                                    scheduled_start="2026-10-17T20:00:00Z") is None
    late = [quote("HOME", "2026-10-17T18:03:00Z"), quote("AWAY", "2026-10-17T18:03:00Z")]
    assert select_latest_quote_pair(late, prediction="2026-10-17T18:02:00Z",
                                    scheduled_start="2026-10-17T20:00:00Z") is None
    at_prediction = [quote("HOME", "2026-10-17T18:02:00Z"),
                     quote("AWAY", "2026-10-17T18:02:00Z")]
    assert select_latest_quote_pair(at_prediction, prediction="2026-10-17T18:02:00Z",
                                    scheduled_start="2026-10-17T20:00:00Z") is not None
    at_start = [quote("HOME", "2026-10-17T20:00:00Z"),
                quote("AWAY", "2026-10-17T20:00:00Z")]
    assert select_latest_quote_pair(at_start, prediction="2026-10-17T20:00:00Z",
                                    scheduled_start="2026-10-17T20:00:00Z") is None


def test_historical_model_role_transition_uses_prediction_timestamp():
    assert role_at("2026-10-14T15:27:10Z") == ("V3_CHALLENGER", "V2_REFERENCE")
    assert role_at("2026-10-14T15:27:11Z") == ("V3_PRODUCTION", "V2_SHADOW")
