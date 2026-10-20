-- The new function runs as postgres with a fixed search path.
-- Expect: security_definer = true, owner = postgres, config = {search_path=pg_catalog}.
SELECT prosecdef AS security_definer,
       pg_get_userbyid(proowner) AS owner,
       proconfig AS config
  FROM pg_proc
 WHERE oid = 'mlb.append_phase_authority_chain_entry(text,bigint,bigint,text,text,text)'::regprocedure;
