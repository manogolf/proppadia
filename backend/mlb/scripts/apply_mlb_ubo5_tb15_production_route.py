#!/usr/bin/env python3
"""Apply the certified UBO-5 TB1.5 route to a wide prediction file."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.mlb.shared.ubo5_tb15_production_route import (
    ARTIFACT_SHA256, ENABLE_FLAG, route_rows,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate-date", required=True)
    ap.add_argument("--wide-csv", required=True, type=Path)
    ap.add_argument("--feature-ledger", required=True, type=Path)
    ap.add_argument("--artifact", required=True, type=Path)
    ap.add_argument("--ledger-out", required=True, type=Path)
    ap.add_argument("--health-out", required=True, type=Path)
    ap.add_argument("--producer-status-json", type=Path)
    args = ap.parse_args()
    enabled = os.environ.get(ENABLE_FLAG, "0").strip().lower() in {"1", "true", "yes", "on"}
    wide = pd.read_csv(args.wide_csv)
    producer = {}
    if args.producer_status_json and args.producer_status_json.is_file():
        producer = json.loads(args.producer_status_json.read_text())
    integration_status = "OK"
    if not args.feature_ledger.is_file():
        feature_rows = pd.DataFrame()
        ledger = pd.DataFrame([{"slate_date": args.slate_date, "route_flag_enabled": enabled,
                                "route_eligibility": False, "model_source": "EXISTING_PRODUCTION",
                                "exclusion_reason": "INTEGRATION_ERROR_FEATURE_LEDGER_MISSING"}])
        integration_status = "ERROR_FEATURE_LEDGER_MISSING"
    else:
        try:
            feature_rows = (
                pd.read_parquet(args.feature_ledger)
                if args.feature_ledger.suffix.lower() in {".parquet", ".pq"}
                else pd.read_csv(args.feature_ledger)
            )
            if feature_rows.empty:
                if producer.get("producer_status") == "PRODUCER_ERROR":
                    ledger = pd.DataFrame([{"slate_date": args.slate_date, "route_flag_enabled": enabled,
                                            "route_eligibility": False, "model_source": "EXISTING_PRODUCTION",
                                            "exclusion_reason": "INTEGRATION_ERROR_FEATURE_LEDGER_PRODUCER"}])
                    integration_status = "ERROR_FEATURE_LEDGER_PRODUCER"
                else:
                    ledger = pd.DataFrame(columns=["slate_date", "route_flag_enabled", "route_eligibility", "model_source", "exclusion_reason"])
                    integration_status = "NO_CURRENT_CANDIDATES"
            else:
                aliases = {"game_date": "slate_date", "history_depth_pa": "strict_prior_pa"}
                feature_rows = feature_rows.rename(
                    columns={k: v for k, v in aliases.items() if k in feature_rows and v not in feature_rows}
                )
                if not feature_rows["slate_date"].astype(str).eq(args.slate_date).all():
                    raise ValueError("WRONG_SLATE_DATE")
                feature_rows["prop_type"] = "total_bases"
                feature_rows["line"] = pd.to_numeric(feature_rows.get("line", 1.5), errors="coerce")
                feature_rows["starter_certification"] = feature_rows.get(
                    "lineup_certification_status", ""
                ).map(lambda value: "CERTIFIED_PREGAME_STARTER" if value == "CONFIRMED_LINEUP" else "UNCERTIFIED")
                feature_rows["batter_identity_certified"] = feature_rows.get(
                    "batter_identity_certified",
                    pd.to_numeric(feature_rows.get("batter_mlb_id"), errors="coerce").notna(),
                )
                feature_rows["identity_ambiguous"] = feature_rows.duplicated(
                    ["slate_date", "game_pk", "batter_mlb_id", "prop_type", "line"], keep=False
                )
                incumbent = wide.loc[wide["prop_type"].eq("total_bases"), ["game_id", "player_id", "p_over_1_5"]].rename(
                    columns={"game_id": "game_pk", "player_id": "batter_mlb_id", "p_over_1_5": "production_prob_over"}
                )
                feature_rows = feature_rows.drop(columns=["production_prob_over"], errors="ignore").merge(
                    incumbent, on=["game_pk", "batter_mlb_id"], how="left", validate="many_to_one"
                )
                ledger = route_rows(feature_rows, artifact=args.artifact, enabled=enabled)
                routed = ledger[ledger.route_eligibility]
                key = wide["prop_type"].eq("total_bases")
                for row in routed.itertuples():
                    mask = key & wide["game_id"].eq(row.game_pk) & wide["player_id"].eq(row.batter_mlb_id)
                    if mask.sum() == 1:
                        wide.loc[mask, "p_over_1_5"] = row.active_probability
                    else:
                        ledger.loc[ledger.index == row.Index, ["route_eligibility", "model_source", "exclusion_reason"]] = [False, "EXISTING_PRODUCTION", "WIDE_IDENTITY_NOT_UNIQUE"]
        except Exception as exc:
            ledger = pd.DataFrame([{"slate_date": args.slate_date, "route_flag_enabled": enabled,
                                    "route_eligibility": False, "model_source": "EXISTING_PRODUCTION",
                                    "exclusion_reason": f"MALFORMED_FEATURE_LEDGER:{type(exc).__name__}:{exc}"}])
            integration_status = "ERROR_MALFORMED_FEATURE_LEDGER"
    args.ledger_out.parent.mkdir(parents=True, exist_ok=True)
    ledger.to_csv(args.ledger_out, index=False)
    if enabled and args.feature_ledger.is_file() and ledger.route_eligibility.any():
        tmp = args.wide_csv.with_suffix(args.wide_csv.suffix + ".ubo5_tmp")
        wide.to_csv(tmp, index=False)
        tmp.replace(args.wide_csv)
    failures = ledger.loc[~ledger.get("route_eligibility", False).astype(bool), "exclusion_reason"].value_counts().to_dict()
    fallback_categories = ledger.loc[~ledger.get("route_eligibility", False).astype(bool)].get(
        "primary_fallback_category", pd.Series(dtype=str)
    ).value_counts().to_dict()
    fallback_mask = ~ledger.get("route_eligibility", pd.Series(dtype=bool)).astype(bool)
    missing_tokens = []
    for raw in ledger.loc[fallback_mask].get("exact_missing_features", pd.Series(dtype=str)).fillna(""):
        missing_tokens.extend(x for x in str(raw).split("|") if x)
    supported_null_tokens = []
    for raw in ledger.loc[~fallback_mask].get("exact_missing_features", pd.Series(dtype=str)).fillna(""):
        supported_null_tokens.extend(x for x in str(raw).split("|") if x)
    health = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "slate_date": args.slate_date, "route_enabled": enabled,
        "feature_ledger_producer_status": producer.get("producer_status", "MISSING_PRODUCER_STATUS"),
        "feature_ledger_path": str(args.feature_ledger),
        "feature_ledger_rows": int(len(feature_rows)),
        "integration_status": integration_status,
        "eligible_rows": int(ledger.get("route_eligibility", pd.Series(dtype=bool)).sum()),
        "routed_rows": int(ledger.get("route_eligibility", pd.Series(dtype=bool)).sum()),
        "fallback_rows": int((~ledger.get("route_eligibility", pd.Series(dtype=bool)).astype(bool)).sum()),
        "artifact_hash_status": "PASS" if (ledger.get("artifact_hash_status", pd.Series(dtype=str)) == "PASS").any() else "NOT_ROUTED",
        "route_failures_by_reason": failures,
        "fallbacks_by_exact_category": fallback_categories,
        "top_missing_features": pd.Series(missing_tokens, dtype=str).value_counts().to_dict(),
        "model_supported_null_features": pd.Series(supported_null_tokens, dtype=str).value_counts().to_dict(),
        "legitimate_history_fallbacks": int(ledger.get("primary_fallback_category", pd.Series(dtype=str)).eq("B_INSUFFICIENT_STRICT_PRIOR_HISTORY").sum()),
        "repairable_integration_fallbacks": int(ledger.get("repair_possible", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
        "source_refresh_failures": int(ledger.get("primary_fallback_category", pd.Series(dtype=str)).eq("E_SOURCE_REFRESH_OMISSION").sum()),
        "feature_schema_status": "PASS" if integration_status in {"OK", "NO_CURRENT_CANDIDATES"} else "NOT_VERIFIED",
        "temporal_integrity_status": "PASS" if ledger.get("temporal_integrity_status", pd.Series(dtype=str)).eq("PASS").any() else ("NO_CURRENT_CANDIDATES" if integration_status == "NO_CURRENT_CANDIDATES" else "NOT_ROUTED"),
        "route_ledger_path": str(args.ledger_out),
        "last_successful_routed_execution": datetime.now(timezone.utc).isoformat() if ledger.get("route_eligibility", pd.Series(dtype=bool)).any() else None,
    }
    args.health_out.parent.mkdir(parents=True, exist_ok=True)
    args.health_out.write_text(json.dumps(health, indent=2) + "\n")
    print(json.dumps(health))


if __name__ == "__main__":
    main()
