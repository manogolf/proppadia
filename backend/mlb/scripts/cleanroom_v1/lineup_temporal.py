#!/usr/bin/env python3
"""Canonical clean-room prospective lineup temporal-admissibility rules."""

from __future__ import annotations

from datetime import datetime


VALID = "LINEUP_VALID_PREGAME"


def classify_lineup(
    *,
    observed_at: datetime | None,
    scheduled_first_pitch: datetime | None,
    governing_market_at: datetime | None,
    ingestion_completed_at: datetime | None,
    governing_run_started_at: datetime | None,
    exact_player_identity: bool = True,
    confirmed_official_order: bool = True,
) -> str:
    if not exact_player_identity or not confirmed_official_order:
        return "LINEUP_IDENTITY_UNRESOLVED"
    if observed_at is None:
        return "LINEUP_TIME_MISSING"
    if scheduled_first_pitch is None:
        return "LINEUP_SCHEDULE_TIME_MISSING"
    if observed_at >= scheduled_first_pitch:
        return "LINEUP_POST_FIRST_PITCH"
    if governing_market_at is None or observed_at > governing_market_at:
        return "LINEUP_AFTER_GOVERNING_CAPTURE"
    if (
        ingestion_completed_at is None
        or governing_run_started_at is None
        or ingestion_completed_at > governing_run_started_at
    ):
        return "LINEUP_NOT_RUN_VISIBLE"
    return VALID


def top_order_action(classification: str, batting_order: int | None) -> str:
    if classification != VALID or batting_order is None:
        return "ORDER_NOT_CONFIRMED"
    return "REJECT_TOP_ORDER" if batting_order in (1, 2, 3) else "RETAIN_CONFIRMED_NON_TOP_ORDER"
