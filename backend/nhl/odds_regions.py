"""Shared region authority for prospective NHL Odds API observations."""

NHL_ODDS_REGIONS = ("us", "us2", "us_ex")
NHL_ODDS_REGIONS_CSV = ",".join(NHL_ODDS_REGIONS)

# Books outside those regions, requested by name in a second per-event call so the
# region books stay exactly as before (the provider ignores regions when bookmakers
# is given). Pinnacle: operator-approved 2026-11-02 for game lines and props.
NHL_ODDS_SUPPLEMENTAL_BOOKMAKERS = ("pinnacle",)
NHL_ODDS_SUPPLEMENTAL_BOOKMAKERS_CSV = ",".join(NHL_ODDS_SUPPLEMENTAL_BOOKMAKERS)
