"""Canonical MLB identity helpers.

The package is intentionally small and side-effect free. It gives MLB callers a
shared vocabulary for identity status, confidence, fallback usage, and canonical
keys without forcing production join behavior to change all at once.
"""

from backend.mlb.identity.canonical_game_identity import GameIdentityInput, GameIdentityResolver, GameIdentityResult
from backend.mlb.identity.canonical_market_identity import MarketIdentityInput, MarketIdentityResult, resolve_market_identity
from backend.mlb.identity.canonical_player_identity import PlayerIdentityInput, PlayerIdentityResolver, PlayerIdentityResult
from backend.mlb.identity.canonical_team_identity import TeamIdentityResult, canonical_team_code

__all__ = [
    "GameIdentityInput",
    "GameIdentityResolver",
    "GameIdentityResult",
    "MarketIdentityInput",
    "MarketIdentityResult",
    "PlayerIdentityInput",
    "PlayerIdentityResolver",
    "PlayerIdentityResult",
    "TeamIdentityResult",
    "canonical_team_code",
    "resolve_market_identity",
]
