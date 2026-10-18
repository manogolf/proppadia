"""Frozen, offline policy helpers for prospective NHL Moneyline evaluation."""

from __future__ import annotations

from datetime import datetime


ROLE_CUTOVER_UTC = datetime.fromisoformat("2026-10-14T15:27:11+00:00")
FAVORITE_BANDS = (
    (0.50, 0.55, "50_to_55"),
    (0.55, 0.60, "55_to_60"),
    (0.60, 0.70, "60_to_70"),
    (0.70, 1.000000000001, "70_or_more"),
)


def decimal_odds(american_price: float | int | str) -> float:
    """Convert a nonzero American price to decimal odds."""
    price = float(american_price)
    if price == 0:
        raise ValueError("American price cannot be zero")
    return 1.0 + (price / 100.0 if price > 0 else 100.0 / abs(price))


def implied_probability(american_price: float | int | str) -> float:
    return 1.0 / decimal_odds(american_price)


def no_vig_home_probability(home_price: float | int | str,
                            away_price: float | int | str) -> float:
    home = implied_probability(home_price)
    away = implied_probability(away_price)
    return home / (home + away)


def model_pick(home_probability: float) -> str:
    """Keep the established deterministic tie rule: an exact 0.5 selects HOME."""
    if not 0.0 <= home_probability <= 1.0:
        raise ValueError("home probability must be in [0, 1]")
    return "HOME" if home_probability >= 0.5 else "AWAY"


def favorite_side(home_probability: float) -> str:
    if not 0.0 <= home_probability <= 1.0:
        raise ValueError("home probability must be in [0, 1]")
    if home_probability == 0.5:
        return "PICKEM"
    return "HOME" if home_probability > 0.5 else "AWAY"


def favorite_band(no_vig_favorite_probability: float | None) -> str:
    if no_vig_favorite_probability is None:
        return "MISSING_PRICE"
    if no_vig_favorite_probability == 0.5:
        return "PICKEM"
    for lower, upper, label in FAVORITE_BANDS:
        if lower <= no_vig_favorite_probability < upper:
            return label
    raise ValueError("favorite probability must be in [0.5, 1]")


def model_market_disagreement(home_probability: float,
                              market_home_probability: float) -> dict[str, float | str | bool]:
    model_side = model_pick(home_probability)
    market_side = favorite_side(market_home_probability)
    return {
        "model_side": model_side,
        "market_favorite_side": market_side,
        "model_picks_market_underdog": market_side in ("HOME", "AWAY") and model_side != market_side,
        "signed_home_probability_gap": home_probability - market_home_probability,
        "absolute_probability_gap": abs(home_probability - market_home_probability),
    }


def role_at(prediction_timestamp: str) -> tuple[str, str]:
    timestamp = datetime.fromisoformat(prediction_timestamp.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("prediction timestamp must include a timezone")
    if timestamp < ROLE_CUTOVER_UTC:
        return "V3_CHALLENGER", "V2_REFERENCE"
    return "V3_PRODUCTION", "V2_SHADOW"


def quote_pair_is_eligible(*, source_update: str, observation: str,
                           prediction: str, scheduled_start: str) -> bool:
    """True only for a complete same-capture quote observed before prediction."""
    def parse(value: str) -> datetime:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError("quote and prediction timestamps must include a timezone")
        return result

    source, observed, predicted, start = map(
        parse, (source_update, observation, prediction, scheduled_start)
    )
    return source <= observed <= predicted < start


def select_latest_quote_pair(rows: list[dict[str, str]], *, prediction: str,
                             scheduled_start: str) -> dict[str, dict[str, str]] | None:
    """Select latest eligible BetOnline capture only when HOME and AWAY pair."""
    eligible: list[tuple[datetime, dict[str, dict[str, str]]]] = []
    captures: dict[tuple[str, str, str], dict[str, dict[str, str]]] = {}
    for row in rows:
        if row.get("sportsbook_key") != "betonlineag":
            continue
        if row.get("quote_qualification_status") not in {
            "QUALIFIED_AUDITED_MONEYLINE_LEDGER",
            "QUALIFIED_SOURCE_AND_OBSERVATION_PRECEDE_PREDICTION",
        }:
            continue
        key = (row.get("source_capture_identity", ""),
               row.get("source_quote_timestamp_utc", ""),
               row.get("observation_timestamp_utc", ""))
        side = row.get("quote_side", "")
        if side not in {"HOME", "AWAY"}:
            continue
        paired_sides = captures.setdefault(key, {})
        prior = paired_sides.get(side)
        if prior is not None and prior.get("american_price") != row.get("american_price"):
            # Conflicting prices under one capture identity are not a valid pair.
            paired_sides[side] = {"_conflict": "true"}
        else:
            paired_sides[side] = row
    for (_, source_update, observation), sides in captures.items():
        if set(sides) != {"HOME", "AWAY"} or any("_conflict" in side for side in sides.values()):
            continue
        if quote_pair_is_eligible(source_update=source_update, observation=observation,
                                  prediction=prediction, scheduled_start=scheduled_start):
            eligible.append((datetime.fromisoformat(observation.replace("Z", "+00:00")), sides))
    if not eligible:
        return None
    eligible.sort(key=lambda item: item[0])
    return eligible[-1][1]
