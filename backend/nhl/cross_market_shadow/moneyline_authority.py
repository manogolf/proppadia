"""Prospective NHL Moneyline authority contract, separate from fitted model IDs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

AUTHORITY_PATH = Path(__file__).resolve().parent / "moneyline_authority_v1.json"
OLD_CHAMPION = "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2"
NEW_CHAMPION = "NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3"


def load_authority(path: Path = AUTHORITY_PATH) -> dict[str, Any]:
    contract = json.loads(Path(path).read_text())
    if contract.get("schema_version") != "NHL_MONEYLINE_PROSPECTIVE_AUTHORITY_V1":
        raise RuntimeError("MONEYLINE_AUTHORITY_SCHEMA_INVALID")
    if (contract.get("production_role") != NEW_CHAMPION
            or contract.get("shadow_role") != OLD_CHAMPION
            or contract.get("new_champion") != NEW_CHAMPION
            or contract.get("new_challenger") != OLD_CHAMPION
            or contract.get("prior_champion") != OLD_CHAMPION
            or contract.get("prior_challenger") != NEW_CHAMPION
            or contract.get("prospective_only") is not True):
        raise RuntimeError("MONEYLINE_AUTHORITY_ROLE_CONFLICT")
    if any(contract.get(key) is not False for key in (
        "model_parameters_changed", "training_data_changed", "feature_construction_changed",
        "probability_calculation_changed", "calibration_changed", "market_logic_changed",
    )):
        raise RuntimeError("MONEYLINE_AUTHORITY_MODEL_MUTATION_DECLARED")
    if not contract.get("effective_at_utc") or not contract.get("evidence_package"):
        raise RuntimeError("MONEYLINE_AUTHORITY_CUTOVER_IDENTITY_INCOMPLETE")
    return contract
