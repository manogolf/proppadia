"""Small SQL helper for preserving existing raw PP-TOI on unknown input."""


def preserve_existing_pp_toi_conflict_assignment(target_relation: str) -> str:
    """Accept an incoming observed value, including zero; keep raw on NULL."""
    if target_relation != "nhl.skater_game_logs_raw":
        raise ValueError("UNSUPPORTED_PP_TOI_TARGET")
    return (
        "pp_toi_minutes = COALESCE(EXCLUDED.pp_toi_minutes, "
        f"{target_relation}.pp_toi_minutes)"
    )
