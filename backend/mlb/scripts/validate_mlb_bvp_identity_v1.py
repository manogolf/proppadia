"""Read-only September 18 exclusion regression; no acquisition or outcome access."""
from __future__ import annotations

import json
from backend.mlb.shared.bvp_identity import CanonicalSlateIdentityError, certified_rows, exclusion_receipt, validate_exclusion_set
from backend.shared.db.pg import pg_connect


def validate() -> dict:
    with pg_connect() as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("""
            SELECT prop_type,player_id,game_id,game_date,features,feature_set_tag,
                   model_tag,lineup_slot,is_probable_sp,computed_at
            FROM mlb.prop_features_precomputed
            WHERE game_date='2026-09-18' AND feature_set_tag='v1'
              AND model_tag='bvp_pvb_refresh_v1'
            ORDER BY prop_type,player_id,game_id,feature_set_tag
        """)
        rows = list(cursor.fetchall())
        cursor.execute("""
            SELECT DISTINCT game_id,game_date::text AS game_date
            FROM mlb.public_game_moneyline_predictions
            WHERE game_date IN ('2026-09-17','2026-09-18')
              AND model_version='MLB_GAME_PYTHAGOREAN_LOG5_V1'
              AND prediction_snapshot_class='DESIGNATED_DAILY_PUBLIC_SNAPSHOT'
              AND admission_status='ADMITTED_SHADOW'
        """)
        authority: dict[int,set[str]] = {}
        for row in cursor.fetchall():
            authority.setdefault(row["game_id"],set()).add(row["game_date"])
    receipt = exclusion_receipt()
    result = validate_exclusion_set(rows,receipt)
    eligible = certified_rows(rows,authority=authority,exclusions=receipt["excluded_rows"])
    if len(eligible)!=1846:
        raise RuntimeError("CANONICAL_CERTIFICATION_POPULATION_MISMATCH")
    result.update({"canonical_game_identity_eligible_rows":len(eligible),
                   "eligible_games":len({r["game_id"] for r in eligible}),
                   "eligible_batters":len({r["player_id"] for r in eligible}),
                   "pitcher_lineage_certification":"LEGACY_NOT_RECONSTRUCTABLE",
                   "database_writes":0,"external_acquisition_requests":0})
    return result


if __name__ == "__main__":
    try:
        print(json.dumps(validate(),sort_keys=True))
    except Exception as error:
        # Database connection errors must not expose hostnames or credentials.
        print(json.dumps({"status":"FAIL","reason":str(error) if isinstance(error,CanonicalSlateIdentityError)
                          else "BVP_IDENTITY_VALIDATION_TECHNICAL_FAILURE","exception_type":type(error).__name__}))
        raise SystemExit(1) from None
