import csv
from pathlib import Path

ROOT=Path(__file__).resolve().parents[4]
REGISTRY=ROOT/'backend/mlb/exports/cleanroom_v1/certification/historical_exception_registry.csv'

def exceptions():
    return list(csv.DictReader(REGISTRY.open())) if REGISTRY.exists() else []

def slate_signal_eligible(slate: str) -> bool:
    return not any(slate in row['slate_date'].split('/') and row['future_research_may_use_evidence']=='NO' for row in exceptions())

def identity_certifiable(slate: str, game_pk: int, player_mlb_id: int) -> bool:
    return not any(row['slate_date']==slate and row['game_pk']==str(game_pk) and row['player_mlb_id']==str(player_mlb_id) and 'QUARANTINED' in row['disposition'] for row in exceptions())

def assert_signal_eligible(slate: str) -> None:
    if not slate_signal_eligible(slate):
        raise RuntimeError('HISTORICAL_EXCEPTION_VOID_FOR_SIGNAL_INFERENCE')
