# MLB Today workspace reporting audit

The legacy `today_workspace_service.fetch_today_workspace` source stages the old player-prop workspace. It is not the certified moneyline `/mlb/today` panel. Its `NOT_REFRESHED` state is therefore `inactive-by-authority` while `NO_QUALIFIED_MLB_PROP_MODEL` remains in force and must not degrade active moneyline health. The live page behavior was not changed.
