"""Backend-neutral, dry-run-only read interfaces for MLB feature grains."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Protocol, Sequence

from backend.mlb.exact_game_features.contract_v1 import ExactGameFeatureStateV1


@dataclass(frozen=True)
class ExactGameFactRecordV1:
    player_id: int
    game_pk: int
    official_date: date
    payload: Mapping[str, Any]
    source_sha256: str


@dataclass(frozen=True)
class ExactGameFeatureRecordV1:
    state: ExactGameFeatureStateV1


@dataclass(frozen=True)
class LegacyDailyAggregateRecordV1:
    """Explicit wrapper for non-exact legacy daily evidence."""

    player_id: int
    game_date: date
    legacy_row_id: str
    payload: Mapping[str, Any]
    grain: str = "PLAYER_DATE_POSTGAME_AGGREGATE_WITH_MIXED_GAME_ID_SEMANTICS"


class ExactGameFactReaderV1(Protocol):
    def by_exact_game(self, *, player_id: int, game_pk: int) -> ExactGameFactRecordV1 | None: ...


class ExactGameFeatureStateReaderV1(Protocol):
    def by_exact_game(
        self,
        *,
        player_id: int,
        game_pk: int,
        contract_version: str,
    ) -> ExactGameFeatureRecordV1 | None: ...


class LegacyDailyAggregateReaderV1(Protocol):
    def by_player_date(
        self,
        *,
        player_id: int,
        game_date: date,
    ) -> Sequence[LegacyDailyAggregateRecordV1]: ...


@dataclass(frozen=True)
class DryRunReaderBundleV1:
    """Names all three grains; no implicit fallback is defined or permitted."""

    exact_facts: ExactGameFactReaderV1
    exact_features: ExactGameFeatureStateReaderV1
    legacy_daily: LegacyDailyAggregateReaderV1
