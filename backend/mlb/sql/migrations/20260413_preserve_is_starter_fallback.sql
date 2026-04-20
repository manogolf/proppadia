-- Preserve ingest-provided is_starter when authoritative starter mapping is unavailable.
-- Context: mlb.player_stats trigger trg_set_is_starter currently nulls NEW.is_starter when
-- public.opp_starter_per_game has no row for (game_id, team), which can erase valid ingest flags.

CREATE OR REPLACE FUNCTION public.set_is_starter()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
  team_int   int;
  starter_id bigint;
BEGIN
  -- Normalize incoming flag to {0,1,NULL}; keep it as fallback when mapping is unavailable.
  IF NEW.is_starter IS NOT NULL AND NEW.is_starter NOT IN (0, 1) THEN
    NEW.is_starter := NULL;
  END IF;

  -- If team is missing, retain incoming value.
  IF NEW.team IS NULL OR NEW.team = '' THEN
    RETURN NEW;
  END IF;

  -- Normalize team to numeric id.
  IF NEW.team ~ '^[0-9]+$' THEN
    team_int := NEW.team::int;
  ELSE
    SELECT m.team_id INTO team_int
    FROM public.mlb_team_map m
    WHERE m.abbr = UPPER(TRIM(NEW.team));
  END IF;

  -- If team cannot be resolved, retain incoming value.
  IF team_int IS NULL THEN
    RETURN NEW;
  END IF;

  -- Fetch authoritative starter for this (game, team) when available.
  SELECT g.starter_pitcher_id
  INTO starter_id
  FROM public.opp_starter_per_game g
  WHERE g.game_id::bigint = NEW.game_id::bigint
    AND COALESCE(
          CASE WHEN g.team ~ '^[0-9]+$' THEN g.team::int END,
          (SELECT m2.team_id FROM public.mlb_team_map m2 WHERE m2.abbr = UPPER(TRIM(g.team)))
        ) = team_int
  LIMIT 1;

  -- If mapping is missing, retain ingest-provided value.
  IF starter_id IS NULL THEN
    RETURN NEW;
  END IF;

  -- Authoritative override when starter mapping exists.
  NEW.is_starter := CASE WHEN NEW.player_id = starter_id THEN 1 ELSE 0 END;
  RETURN NEW;
END;
$function$;

