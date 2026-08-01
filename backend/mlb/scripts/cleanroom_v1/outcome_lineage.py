#!/usr/bin/env python3
"""Pure fail-closed outcome and settlement certification helpers."""
def reconstruct_stats(hits, doubles, triples, home_runs):
    if None in (hits,doubles,triples,home_runs): return "MISSING_REQUIRED_STAT", None, None
    singles=hits-doubles-triples-home_runs
    if singles < 0: return "NEGATIVE_SINGLES", singles, None
    tb=singles+2*doubles+3*triples+4*home_runs
    if tb != hits+doubles+2*triples+3*home_runs: return "HIT_COMPONENT_MISMATCH", singles, tb
    return "TB_ARITHMETIC_CERTIFIED", singles, tb

def classify_outcome(join_count, game_final, plate_appearances, total_bases):
    if not game_final: return "PENDING"
    if join_count == 0: return "MISSING_PLAYER_RESULT"
    if join_count != 1: return "DUPLICATE_PLAYER_RESULT"
    if plate_appearances == 0: return "NO_ACTION"
    if plate_appearances is None or total_bases is None: return "TECHNICAL_UNRESOLVED"
    return "OVER_WIN" if total_bases >= 2 else "OVER_LOSS"

def american_profit(stake, odds, won):
    if not won: return -stake
    return stake*odds/100 if odds > 0 else stake*100/abs(odds)
