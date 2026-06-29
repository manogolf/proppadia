from __future__ import annotations

from typing import Any


ONTOLOGY_FIELDS = [
    "universe",
    "population",
    "classification_type",
    "classification_value",
    "opportunity_type",
    "provenance_layer",
    "board_name",
    "research_status",
]

UNIVERSES = {"main", "alternate", "expanded"}
POPULATIONS = {
    "simple_filter",
    "watch",
    "expanded_review",
    "alternate_discovery",
    "expanded_universe",
    "main_only",
    "alternate_only",
    "shared",
    "user_proxy",
    "outside_proxy",
}
CLASSIFICATION_TYPES = {"tier", "price_bucket", "opportunity", "context", "unclassified"}
RESEARCH_STATUSES = {
    "operational_research",
    "manual_research",
    "research_only",
    "proxy_research",
}

BOARD_ONTOLOGY = {
    "o15": {
        "universe": "main",
        "population": "simple_filter",
        "board_name": "hits_o15_simple_filter",
        "research_status": "operational_research",
        "provenance_layer": "main_source",
    },
    "watch_o15": {
        "universe": "main",
        "population": "watch",
        "board_name": "hits_o15_watch_candidates",
        "research_status": "operational_research",
        "provenance_layer": "watch_population",
    },
    "layered_o15": {
        "universe": "main",
        "population": "expanded_review",
        "board_name": "hits_o15_layered_candidates",
        "research_status": "operational_research",
        "provenance_layer": "",
    },
    "alternate_o15": {
        "universe": "alternate",
        "population": "alternate_discovery",
        "board_name": "hits_o15_alternate_discovery",
        "research_status": "manual_research",
        "provenance_layer": "",
    },
}

LAYER_DISPLAY = {
    "layer_4_qc_d7_d15_starter": "Layer 4",
    "layer_3_d7_d15_starter_non_qc": "Layer 3",
    "layer_2_d7_d15_no_favorable_starter": "Layer 2",
    "layer_1_d7_hot_not_d15_consistent": "Layer 1",
    "all_o15_other": "All O1.5 Other",
    "alternate_layer_a_d7_d15_starter": "Layer A",
    "alternate_layer_b_d7_d15": "Layer B",
    "alternate_layer_c_d7_hot": "Layer C",
    "alternate_other": "Alternate Other",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _combined_tier(row: dict[str, Any]) -> str:
    combined = _text(row.get("combined_tier"))
    if combined:
        return combined
    hitter = _text(row.get("hitter_tier"))
    pitcher = _text(row.get("pitcher_tier"))
    if hitter and pitcher:
        return f"{hitter}/{pitcher}"
    return "unclassified"


def _provenance_layer(row: dict[str, Any], board: str, defaults: dict[str, str]) -> str:
    raw = ""
    if board == "layered_o15":
        raw = _text(row.get("layer_label"))
    elif board == "alternate_o15":
        raw = _text(row.get("alternate_layer"))
    return LAYER_DISPLAY.get(raw, raw or defaults.get("provenance_layer") or "none")


def infer_o15_opportunity_type(row: dict[str, Any]) -> str:
    hitter_tier = _text(row.get("hitter_tier"))
    d7 = _float(row.get("d7_hits_rate"))
    price = _float(row.get("best_over_price") or row.get("market_price") or row.get("expanded_price"))
    team_expected = _float(row.get("team_expected_hits_allowed"))
    starter_expected = _float(row.get("starter_expected_hits_allowed"))
    if hitter_tier == "A" or (d7 is not None and d7 > 1.3):
        return "public_hot"
    if price is not None and 201 <= price <= 300 and (
        (team_expected is not None and team_expected >= 9.0)
        or (starter_expected is not None and starter_expected >= 5.0)
    ):
        return "context_supported_plus_money"
    if hitter_tier == "C" or (d7 is not None and d7 <= 1.0):
        return "quiet_hitter"
    return "unclassified"


def apply_o15_board_ontology(rows: list[dict[str, Any]], board: str) -> None:
    defaults = BOARD_ONTOLOGY.get(board, BOARD_ONTOLOGY["o15"])
    if board not in BOARD_ONTOLOGY:
        return
    for row in rows:
        row.update(
            {
                "universe": defaults["universe"],
                "population": defaults["population"],
                "classification_type": "tier",
                "classification_value": _combined_tier(row),
                "opportunity_type": infer_o15_opportunity_type(row),
                "provenance_layer": _provenance_layer(row, board, defaults),
                "board_name": defaults["board_name"],
                "research_status": defaults["research_status"],
            }
        )


def apply_expanded_o15_ontology(row: dict[str, Any]) -> dict[str, Any]:
    source_bucket = _text(row.get("source_bucket"))
    if not source_bucket:
        if _boolish(row.get("from_both")):
            source_bucket = "shared"
        elif _boolish(row.get("from_alternate")):
            source_bucket = "alternate_only"
        elif _boolish(row.get("from_main")):
            source_bucket = "main_only"
        else:
            source_bucket = "expanded_universe"
    layer = _text(row.get("alternate_layer") or row.get("layer_label"))
    row.update(
        {
            "universe": "expanded",
            "population": source_bucket if source_bucket in POPULATIONS else "expanded_universe",
            "classification_type": "tier",
            "classification_value": _combined_tier(row),
            "opportunity_type": infer_o15_opportunity_type(row),
            "provenance_layer": LAYER_DISPLAY.get(layer, layer or source_bucket or "expanded_universe"),
            "board_name": "expanded_o15_universe",
            "research_status": "research_only",
        }
    )
    return row


def ontology_health_warnings(row: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    for field in ONTOLOGY_FIELDS:
        if not _text(row.get(field)):
            warnings.append(f"missing_{field}")
    universe = _text(row.get("universe"))
    population = _text(row.get("population"))
    classification_type = _text(row.get("classification_type"))
    research_status = _text(row.get("research_status"))
    if universe and universe not in UNIVERSES:
        warnings.append("invalid_universe")
    if population and population not in POPULATIONS:
        warnings.append("invalid_population")
    if classification_type and classification_type not in CLASSIFICATION_TYPES:
        warnings.append("invalid_classification_type")
    if research_status and research_status not in RESEARCH_STATUSES:
        warnings.append("invalid_research_status")
    if universe == "alternate" and population != "alternate_discovery":
        warnings.append("invalid_alternate_population")
    if universe == "main" and population in {"alternate_discovery", "alternate_only"}:
        warnings.append("invalid_main_population")
    if universe == "expanded" and _text(row.get("board_name")) != "expanded_o15_universe":
        warnings.append("invalid_expanded_board_name")
    return warnings
