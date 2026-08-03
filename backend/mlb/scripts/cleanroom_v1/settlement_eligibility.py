"""Separate official MLB outcomes from BetOnline market settlement eligibility."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
CONTRACT_PATH = ROOT / "backend/mlb/contracts/betonline_baseball_props_settlement_v1.json"

BOOK_SETTLED_OFFICIAL_RESULT = "BOOK_SETTLED_OFFICIAL_RESULT"
BOOK_VOID_SHORTENED_GAME = "BOOK_VOID_SHORTENED_GAME"
BOOK_VOID_PLAYER_DID_NOT_APPEAR = "BOOK_VOID_PLAYER_DID_NOT_APPEAR"
BOOK_VOID_ZERO_PA = "BOOK_VOID_ZERO_PA"
BOOK_VOID_OTHER_RULE = "BOOK_VOID_OTHER_RULE"
BOOK_SETTLEMENT_PENDING = "BOOK_SETTLEMENT_PENDING"
BOOK_RULE_UNCERTIFIED = "BOOK_RULE_UNCERTIFIED"
BOOK_SETTLEMENT_CONFLICT = "BOOK_SETTLEMENT_CONFLICT"


def load_contract(path: Path = CONTRACT_PATH) -> dict:
    data = json.loads(path.read_text())
    data["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return data


def classify_game(status: dict, innings_completed: int | None = None) -> str:
    abstract = str(status.get("abstractGameState", "")).lower()
    coded = str(status.get("codedGameState", "")).upper()
    detailed = str(status.get("detailedState", "")).lower()
    reason = str(status.get("reason", "")).lower()
    text = f"{detailed} {reason}"
    if "postpon" in text or "cancel" in text:
        return "POSTPONED_OR_CANCELLED"
    if "suspend" in text and abstract == "final":
        return "SUSPENDED_FINAL"
    if "completed early" in text or (abstract == "final" and any(x in text for x in ("rain", "called", "forfeit", "tie"))):
        return "COMPLETED_EARLY_OFFICIAL"
    if abstract == "final" or coded == "F":
        return "NORMAL_FINAL"
    return "NONSTANDARD_FINAL_REQUIRES_BOOK_RULE"


def classify_book_settlement(game_class: str, plate_appearances: int | None, *,
                             slate_date: str, contract: dict | None = None,
                             actual_ticket_status: str | None = None) -> str:
    contract = contract or load_contract()
    if actual_ticket_status and actual_ticket_status not in {"VOID", "SETTLED", "PENDING"}:
        return BOOK_SETTLEMENT_CONFLICT
    if plate_appearances == 0:
        return BOOK_VOID_PLAYER_DID_NOT_APPEAR
    if game_class == "POSTPONED_OR_CANCELLED":
        return BOOK_VOID_OTHER_RULE
    if game_class in {"COMPLETED_EARLY_OFFICIAL", "NONSTANDARD_FINAL_REQUIRES_BOOK_RULE"}:
        if contract.get("applicability_decision") != "CERTIFIED_FOR_2026-08-02" or slate_date > contract.get("historical_applicability_through", ""):
            return BOOK_RULE_UNCERTIFIED
        decision = BOOK_VOID_SHORTENED_GAME if contract.get("completed_early_treatment") == "VOID" else BOOK_SETTLED_OFFICIAL_RESULT
    elif game_class == "SUSPENDED_FINAL":
        return BOOK_RULE_UNCERTIFIED
    elif game_class == "NORMAL_FINAL":
        decision = BOOK_SETTLED_OFFICIAL_RESULT
    else:
        return BOOK_SETTLEMENT_PENDING
    if actual_ticket_status == "VOID" and decision == BOOK_SETTLED_OFFICIAL_RESULT:
        return BOOK_SETTLEMENT_CONFLICT
    if actual_ticket_status == "SETTLED" and decision.startswith("BOOK_VOID"):
        return BOOK_SETTLEMENT_CONFLICT
    return decision


def american_profit(odds: int, stake: float = 5.0) -> float:
    return stake * odds / 100 if odds > 0 else stake * 100 / abs(odds)


def settle_side(total_bases: int, side: str, odds: int, book_status: str, stake: float = 5.0) -> dict:
    official_win = total_bases > 1 if side == "OVER" else total_bases <= 1
    official_outcome = f"{side}_{'WIN' if official_win else 'LOSS'}"
    if book_status.startswith("BOOK_VOID"):
        return {"official_outcome": official_outcome, "book_outcome": "VOID", "stake_at_risk": 0.0, "returned_stake": stake, "net": 0.0}
    if book_status != BOOK_SETTLED_OFFICIAL_RESULT:
        return {"official_outcome": official_outcome, "book_outcome": book_status, "stake_at_risk": 0.0, "returned_stake": 0.0, "net": 0.0}
    return {"official_outcome": official_outcome, "book_outcome": "WIN" if official_win else "LOSS", "stake_at_risk": stake, "returned_stake": 0.0, "net": american_profit(odds, stake) if official_win else -stake}
