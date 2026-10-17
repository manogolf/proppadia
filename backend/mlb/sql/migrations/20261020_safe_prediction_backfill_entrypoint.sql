-- Fixed initial-only prediction materialization. Realized outcome fields,
-- including was_correct, remain outside this operation.
DO $preflight$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='proppadia_mlb_ingest') THEN
        RAISE EXCEPTION 'required role proppadia_mlb_ingest is missing';
    END IF;
    IF to_regclass('mlb.model_training_props') IS NULL THEN
        RAISE EXCEPTION 'required model_training_props relation is missing';
    END IF;
END
$preflight$;

CREATE OR REPLACE FUNCTION mlb.safe_fill_prediction_backfill(
    p_id uuid,
    p_predicted_outcome text,
    p_confidence_score numeric,
    p_prediction_timestamp timestamptz
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_prop_source text;
    v_status text;
    v_outcome text;
    v_prediction text;
    v_confidence numeric;
    v_timestamp timestamptz;
    n integer;
BEGIN
    IF p_id IS NULL OR p_predicted_outcome NOT IN ('over','under')
       OR p_confidence_score IS NULL OR p_confidence_score < 0 OR p_confidence_score > 1
       OR p_prediction_timestamp IS NULL THEN
        RAISE EXCEPTION 'invalid prediction backfill payload' USING ERRCODE='22023';
    END IF;

    SELECT prop_source,status,outcome,predicted_outcome,confidence_score,prediction_timestamp
      INTO v_prop_source,v_status,v_outcome,v_prediction,v_confidence,v_timestamp
      FROM mlb.model_training_props
     WHERE id=p_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'prediction target is missing' USING ERRCODE='23503';
    END IF;
    IF v_prop_source IS DISTINCT FROM 'mlb_api' OR v_status IS DISTINCT FROM 'resolved'
       OR v_outcome IS NULL THEN
        RAISE EXCEPTION 'prediction target is not an established resolved row' USING ERRCODE='23514';
    END IF;

    IF v_prediction IS NULL AND v_confidence IS NULL AND v_timestamp IS NULL THEN
        UPDATE mlb.model_training_props
           SET predicted_outcome=p_predicted_outcome,
               confidence_score=p_confidence_score,
               prediction_timestamp=p_prediction_timestamp
         WHERE id=p_id;
        GET DIAGNOSTICS n=ROW_COUNT;
        RETURN n;
    END IF;

    IF (v_prediction,v_confidence,v_timestamp) IS NOT DISTINCT FROM
       (p_predicted_outcome,p_confidence_score,p_prediction_timestamp) THEN
        RETURN 0;
    END IF;
    RAISE EXCEPTION 'prediction backfill conflicts with established prediction state' USING ERRCODE='23514';
END
$function$;

REVOKE ALL ON FUNCTION mlb.safe_fill_prediction_backfill(uuid,text,numeric,timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mlb.safe_fill_prediction_backfill(uuid,text,numeric,timestamptz) TO proppadia_mlb_ingest;
