\set slate_date :'slate_date'
COPY (
  SELECT json_build_object(
           'game_id',  g.game_id,
           'game_pk',  g.game_id,         -- we use game_id as NHL gamePk
           'game_date', to_char(g.game_date,'YYYY-MM-DD')
         )::text
  FROM nhl.games g
  WHERE g.game_date = DATE :'slate_date'
  ORDER BY g.game_id
) TO STDOUT;
