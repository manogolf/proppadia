#!/usr/bin/env python3
"""Freeze Points consideration rules and initialize the prospective ledger.

This consumes the already classified, outcome-free Oct. 14 dry view. It does
not read later outcomes when assigning any stage.
"""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DISCOVERY = ROOT / "artifacts/analysis/nhl/points_consideration_funnel/2026-10-14"
OUT = ROOT / "artifacts/analysis/nhl/points_consideration_funnel_freeze/2026-10-14"
CONTRACT_ID = "NHL_POINTS_CONSIDERATION_FUNNEL_V1"
DISCOVERY_COMMIT = "63dde3d091482e5fb9da3fb30a6665302bbf6f54"
DISCOVERY_COUNTS = {"0.5": {"stage_0": 658, "stage_1_survivors": 538,
                            "stage_2_survivors": 263, "stage_3_survivors": 227}}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = list(dict.fromkeys(k for row in rows for k in row))
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def append_only_csv(path: Path, rows: list[dict], fields: list[str], key_fields: tuple[str, ...]) -> None:
    """Append unseen event keys; reject any attempted rewrite of a saved event."""
    existing = read_csv(path) if path.exists() else []
    for prior in existing:
        if "record_type" in key_fields and not prior.get("record_type"):
            prior["record_type"] = "PRE_OUTCOME_CLASSIFICATION" if prior.get("population") else "CLASSIFICATION"
    by_key = {tuple(r.get(k, "") for k in key_fields): r for r in existing}
    for row in rows:
        key = tuple(str(row.get(k, "")) for k in key_fields)
        prior = by_key.get(key)
        if prior is not None:
            if any(str(prior.get(k, "")) != str(row.get(k, "")) for k in fields):
                raise AssertionError(f"append-only record differs for {key}; create a new event/version")
        else:
            added = {k: str(row.get(k, "")) for k in fields}
            existing.append(added)
            by_key[key] = added
    write_csv(path, existing, fields)


def classify(edge: float | None, gap: float | None, model_side: str | None,
             market_side: str | None) -> dict:
    """Pure outcome-blind V1 classification, with strict signed comparisons."""
    s1 = edge is not None and edge < 0
    s2 = (not s1) and gap is not None and gap > 0.15
    s3 = (not s1 and not s2 and model_side in {"OVER", "UNDER"}
          and market_side in {"OVER", "UNDER"} and model_side != market_side)
    first = "STAGE_1" if s1 else "STAGE_2" if s2 else "STAGE_3" if s3 else ""
    return {"stage1_excluded": s1, "stage2_excluded": s2,
            "stage3_excluded": s3, "first_exclusion_stage": first,
            "survivor": not (s1 or s2 or s3),
            "edge_status": "EDGE_UNAVAILABLE" if edge is None else
                "NEGATIVE_EDGE" if edge < 0 else "ZERO_EDGE" if edge == 0 else "POSITIVE_EDGE"}


def classify_row(row: dict[str, str]) -> dict:
    """Read only pregame classification inputs; outcome fields are ignored."""
    return classify(nfloat(row.get("selected_production_edge")),
                    nfloat(row.get("signed_selected_model_minus_no_vig_market_gap")),
                    row.get("model_side"), row.get("market_side"))


def nfloat(value: str | None) -> float | None:
    if value in (None, "", "None"):
        return None
    return float(value)


def json_write(name: str, value: dict) -> None:
    (OUT / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    prior_csv = DISCOVERY / "current_day_dry_view_2026-10-14.csv"
    prior_summary = DISCOVERY / "summary.json"
    rows = read_csv(prior_csv)
    if len(rows) != 93:
        raise AssertionError(f"expected frozen Oct. 14 dry view N=93, got {len(rows)}")
    prior_counts = {r["stage"]: r for r in read_csv(DISCOVERY/"canonical_funnel_table.csv")
                    if r["line"] == "0.5" and r["stage"] in {"STAGE_0", "STAGE_1", "STAGE_2", "STAGE_3"}}
    reproduced = (int(prior_counts["STAGE_0"]["remaining_n"]),
                  int(prior_counts["STAGE_1"]["remaining_n"]),
                  int(prior_counts["STAGE_2"]["remaining_n"]),
                  int(prior_counts["STAGE_3"]["remaining_n"]))
    if reproduced != (658, 538, 263, 227):
        raise AssertionError(f"committed discovery funnel mismatch: {reproduced}")

    ledger = []
    for row in rows:
        flags = classify_row(row)
        # The prior pre-outcome classification is itself evidence; require exact
        # agreement before writing the frozen copy.
        expected = {
            "stage1_status": "REMOVED_NEGATIVE_EDGE" if flags["stage1_excluded"] else "SURVIVES_STAGE_1",
            "stage2_status": "REMOVED_EXTREME_GAP" if flags["stage2_excluded"] else "SURVIVES_STAGE_2" if not flags["stage1_excluded"] else "NOT_REACHED",
            "stage3_status": "REMOVED_OPPOSITE_SIDE" if flags["stage3_excluded"] else "SURVIVES_STAGE_3" if not flags["stage1_excluded"] and not flags["stage2_excluded"] else "NOT_REACHED",
        }
        for key, value in expected.items():
            if row.get(key) != value:
                raise AssertionError(f"Oct. 14 pre-outcome {key} mismatch: {row.get(key)} != {value}")
        attachment_key = "|".join(str(row.get(k, "")) for k in (
            "game_id", "player_id", "line", "price_over", "price_under",
            "source_books_over", "source_books_under", "odds_observation_manifest_sha256"))
        ledger.append({**row, **flags, "record_type":"CLASSIFICATION", "frozen_contract_version": CONTRACT_ID,
                       "market_attachment_identity": hashlib.sha256(attachment_key.encode()).hexdigest(),
                       "classification_timestamp_utc": row.get("observation_timestamp_utc", ""),
                       "classification_source": "PRE_OUTCOME_DRY_VIEW_2026-10-14",
                       "official_points": "", "grade_status": "AWAITING_FINAL_OUTCOMES"})

    # Reproduce all frozen Oct. 14 line counts from the retained pre-outcome rows.
    line_rows = {line: [r for r in ledger if float(r["line"]) == float(line)] for line in ("0.5", "1.5", "2.5")}
    dry_counts = []
    for line, subset in line_rows.items():
        n0 = len(subset)
        n1 = sum(not r["stage1_excluded"] for r in subset)
        n2 = sum(not r["stage1_excluded"] and not r["stage2_excluded"] for r in subset)
        n3 = sum(r["survivor"] for r in subset)
        dry_counts.append({"line": line, "stage_0_market_usable": n0,
                           "stage_1_removed": n0-n1, "stage_1_survivors": n1,
                           "stage_2_removed_incremental": n1-n2, "stage_2_survivors": n2,
                           "stage_3_removed_incremental": n2-n3, "stage_3_survivors": n3,
                           "survivor_share": n3/n0 if n0 else None})
    if [(x["line"],x["stage_0_market_usable"],x["stage_1_removed"],x["stage_2_removed_incremental"],x["stage_3_removed_incremental"],x["stage_3_survivors"]) for x in dry_counts] != [
        ("0.5",58,9,26,3,20),("1.5",31,3,9,0,19),("2.5",4,0,0,0,4)]:
        raise AssertionError("Oct. 14 frozen pre-outcome funnel counts differ from prior dry artifact")

    summary_prior = json.loads(prior_summary.read_text())
    rules = {
      "stage_0": {"name":"ALL_MARKET_USABLE_PREDICTIONS","population":"Pregame HGB prediction matched to legitimate two-sided retained Points market evidence."},
      "stage_1": {"name":"RESEARCH_EXCLUDE_NEGATIVE_EDGE","rule":"Exclude only when selected-side production edge < 0.","edge_definition":"Selected model-side probability minus raw American implied probability calculated from that side's separately aggregated median attachment price; raw probability includes vig.","missing":"EDGE_UNAVAILABLE remains classified and is not silently excluded.","discovery_rationale":"NONCOMPETITIVE_FOR_CONSIDERATION; not a validated failure region."},
      "stage_2": {"name":"RESEARCH_EXCLUDE_EXTREME_MODEL_MARKET_GAP","rule":"Among Stage 1 survivors, exclude only when selected_side_model_probability - selected_side_no_vig_market_probability > 0.15.","gap":"Signed positive selected-side probability gap; no absolute value. Strict > 0.15 (15 percentage points).","no_vig_market":"Convert both side median American prices to raw implied probabilities, normalize each by their two-side sum, then select probability for the model-selected side.","missing":"Unavailable gap is retained and separately identifiable; it is not treated as exceeding threshold.","diagnostic_cohorts":{"UNDER":"model-selected side is UNDER","OVER":"model-selected side is OVER","SAME_SIDE_CONFIDENCE_DISAGREEMENT":"model side equals no-vig market side within Stage 2; the >15pp signed selected-side gap is the confidence disagreement","OPPOSITE_SIDE":"model side differs from no-vig market side"}},
      "stage_3": {"name":"PROVISIONAL_STAGE_3_OPPOSITE_SIDE_DISAGREEMENT","rule":"Among Stage 2 survivors only, exclude when model_side != market_side.","model_side":"OVER when model p_over >= 0.5, otherwise UNDER.","market_side":"OVER when normalized no-vig p_over >= 0.5, otherwise UNDER.","status":"Provisional, independently evaluated, not merged into Stage 2."},
    }
    contract_core = {
      "contract_version": CONTRACT_ID, "status":"FROZEN_FOR_PROSPECTIVE_VALIDATION",
      "freeze_date":"2026-10-14", "prospective_start_date":"2026-10-14",
      "discovery_window":{"start":"2026-10-10","end":"2026-10-13","population":"finalized HGB Points x retained market evidence","future_rule_changes_from_discovery":False},
      "production_model":{"name":"NHL_POINTS_COUNT_HGB_V1","model_version":"NHL_POINTS_LEADER_CORE_FEATURES_V1"},
      "primary_line":0.5,"applicable_lines":[0.5,1.5,2.5],"stage_rules":rules,
      "order":["STAGE_0","STAGE_1","STAGE_2","STAGE_3","SURVIVORS"],
      "first_exclusion_only":True,"later_overlap_flags_diagnostic_only":True,
      "missing_value_semantics":{"edge":"EDGE_UNAVAILABLE; retain in Stage 1; continue to later screens if their inputs are available","gap":"GAP_UNAVAILABLE; do not exclude at Stage 2; continue to Stage 3 if sides are available","side":"SIDE_UNAVAILABLE; do not exclude at Stage 3"},
      "classification_effect":"RESEARCH_CONSIDERATION_EXCLUSIONS only; predictions remain generated, exported, immutable and gradeable; no production authority changes.",
      "no_inclusion_rules":True,"no_candidate_target":True,
      "source_commits":{"discovery":"63dde3d091482e5fb9da3fb30a6665302bbf6f54","freeze_preparation":"63dde3d091482e5fb9da3fb30a6665302bbf6f54"},
      "amendment_policy":"Any change to order, edge, 15 pp threshold, signed/absolute gap, Stage 3 disagreement rule, or missing-value semantics requires a new explicit contract version (for example V2); V1 is immutable.",
      "validation_gate":{"minimum_independent_finalized_hgb_dates":6,"minimum_is_not_proof":True,"assessment":"directional consistency, material excess-loss reduction, acceptable wins sacrificed, no single-date domination, sufficient sample; no automatic production promotion"},
      "research_limits":{"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"manual_nhl_com_review":False,"stage_4_discovery":False},
    }
    fingerprint = hashlib.sha256(json.dumps(contract_core, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    contract = {**contract_core, "contract_identity_sha256": fingerprint}
    json_write("frozen_funnel_contract.json", contract)
    json_write("discovery_window_manifest.json", {
      "population":"DISCOVERY", "dates":["2026-10-10","2026-10-11","2026-10-12","2026-10-13"],
      "source_package":"artifacts/analysis/nhl/points_consideration_funnel/2026-10-14/",
      "source_summary_sha256":sha(prior_summary), "retained_as_descriptive_reference_only":True,
      "prohibited_for_post_freeze_rule_changes":True})
    json_write("edge_contract.json", rules["stage_1"])
    json_write("market_gap_contract.json", rules["stage_2"])
    json_write("stage_1_rule.json", rules["stage_1"])
    json_write("stage_2_rule.json", rules["stage_2"])
    json_write("stage_3_rule.json", rules["stage_3"])
    json_write("ordering_contract.json", {"order":contract_core["order"],"first_exclusion_only":True,"diagnostic_overlap_flags":True,"double_counting":"Prohibited; each row contributes to only its first exclusion cohort."})
    json_write("missing_value_semantics.json", contract_core["missing_value_semantics"])
    json_write("frozen_discovery_counts.json", {"population":"DISCOVERY","line":"0.5","counts":DISCOVERY_COUNTS["0.5"],"source":"Prior package, reproduced in its committed implementation; not recomputed into a changed definition."})
    json_write("oct14_pre_outcome_classification_binding.json", {
      "date":"2026-10-14","classification_status":"FROZEN_PRE_OUTCOME","outcomes_included":False,
      "dry_view_path":"artifacts/analysis/nhl/points_consideration_funnel/2026-10-14/current_day_dry_view_2026-10-14.csv",
      "dry_view_sha256":sha(prior_csv), "prior_integrity_status":summary_prior["dry_view"]["integrity_status"],
      "parent_daily_run_id":summary_prior["dry_view"]["parent_daily_run_id"],
      "prediction_rows":summary_prior["dry_view"]["immutable_prediction_rows"],"market_usable_rows":len(rows),
      "survivors_all_lines":sum(r["survivor"] for r in ledger),"lines":dry_counts,
      "classification_timestamp_utc":summary_prior["dry_view"]["observation_timestamp_utc"],
      "prediction_sha256":"71e2972de2d26791df0af03a8aa74a48972cd858bb7d1f82311068de9874816e",
      "market_attachment_sha256":"a964969cc39c394adb2ccc063c55fde56fb9556e0372de90a4d761e79a2ab0a1",
      "integrity_artifact_sha256":"c5790e8e5e65177b32324c9d492497ebcb9bee3d414d0ff0f62c6e58848a1db3",
      "all_market_rows_predated_game_start":True,"classification_derived_without_outcomes":True})

    schema_fields = ["record_type","slate_date","game_id","player_id","line","frozen_contract_version","prediction_identity","prediction_artifact_sha256","market_attachment_identity","odds_observation_manifest_sha256","selected_production_edge","signed_selected_model_minus_no_vig_market_gap","stage1_excluded","stage2_excluded","stage3_excluded","first_exclusion_stage","survivor","edge_status","classification_timestamp_utc","classification_source","official_points","grade_status"]
    append_only_csv(OUT/"prospective_ledger.csv", ledger, schema_fields,
                    ("slate_date","game_id","player_id","line","record_type"))
    (OUT/"prospective_ledger_schema.json").write_text(json.dumps({"key":["slate_date","game_id","player_id","line","record_type"],"natural_prediction_key":["slate_date","game_id","player_id","line"],"record_types":["CLASSIFICATION","OUTCOME_GRADE"],"append_only":True,"immutable_rows":True,"columns":schema_fields,"outcome_join":"Only after pre-outcome classification is frozen. Add an OUTCOME_GRADE event; never edit a saved CLASSIFICATION event."},indent=2)+"\n")
    date_summary_schema = ["population","slate_date","games","raw_market_usable","stage1_removed","stage1_survivors","stage2_removed_incremental","stage2_survivors","stage3_removed_incremental","survivors","survivor_percentage","raw_wins","raw_losses","survivor_wins","survivor_losses","expected_losses","observed_losses","excess_losses","model_brier","model_log_loss","market_brier","market_log_loss","stage1_wins_removed","stage1_losses_removed","stage1_loss_share_removed","stage1_excess_losses_removed","stage1_pct_original_removed","stage2_wins_removed","stage2_losses_removed","stage2_loss_share_removed","stage2_excess_losses_removed","stage2_pct_original_removed","stage3_wins_removed","stage3_losses_removed","stage3_loss_share_removed","stage3_excess_losses_removed","stage3_pct_original_removed","stage2_under_n","stage2_over_n","stage2_same_side_confidence_disagreement_n","stage2_opposite_side_n","stage2_excess_losses_change","stage3_excess_losses_change","survivor_selected_side_calibration_gap","grade_status"]
    date_summary_schema.insert(1,"record_type")
    append_only_csv(OUT/"date_level_validation_summary.csv", [{"population":"PROSPECTIVE_VALIDATION","record_type":"PRE_OUTCOME_CLASSIFICATION","slate_date":"2026-10-14","games":2,"raw_market_usable":93,"grade_status":"AWAITING_FINAL_OUTCOMES"}], date_summary_schema, ("population","slate_date","record_type"))
    cumulative_fields = ["population","finalized_dates","minimum_dates_for_repeatability_review","raw_market_usable","stage1_removed","stage1_survivors","stage2_removed_incremental","stage2_survivors","stage3_removed_incremental","survivors","survivor_percentage","raw_wins","raw_losses","survivor_wins","survivor_losses","expected_losses","observed_losses","excess_losses","model_brier","model_log_loss","market_brier","market_log_loss","stage1_wins_removed","stage1_losses_removed","stage1_excess_losses_removed","stage2_wins_removed","stage2_losses_removed","stage2_excess_losses_removed","stage3_wins_removed","stage3_losses_removed","stage3_excess_losses_removed","status","discovery_rows_mixed_in"]
    write_csv(OUT/"cumulative_validation_summary.csv", [{"population":"PROSPECTIVE_VALIDATION","finalized_dates":0,"minimum_dates_for_repeatability_review":6,"status":"AWAITING_FINAL_OUTCOMES","discovery_rows_mixed_in":False}], cumulative_fields)
    write_csv(OUT/"losses_removed_wins_sacrificed.csv", [{"population":"PROSPECTIVE_VALIDATION","slate_date":"2026-10-14","line":line,"stage":stage,"rows_removed":"","wins_removed":"","losses_removed":"","loss_share_removed":"","excess_losses_removed":"","pct_original_slate_removed":"","grade_status":"AWAITING_FINAL_OUTCOMES"} for line in (0.5,1.5,2.5) for stage in ("STAGE_1","STAGE_2","STAGE_3")])
    write_csv(OUT/"stage2_diagnostic_cohorts.csv", [{"population":"PROSPECTIVE_VALIDATION","slate_date":"2026-10-14","line":line,"cohort":cohort,"n":"","wins":"","losses":"","expected_losses":"","observed_losses":"","excess_losses":"","brier":"","log_loss":"","grade_status":"AWAITING_FINAL_OUTCOMES"} for line in (0.5,1.5,2.5) for cohort in ("STAGE_2_ALL","STAGE_2_UNDER","STAGE_2_OVER","STAGE_2_SAME_SIDE_CONFIDENCE_DISAGREEMENT","STAGE_2_OPPOSITE_SIDE")])
    json_write("primary_validation_questions.json", {"questions":["A stage removals and survivor counts","B survivor performance","C excluded-row performance","D cohort wins/losses and scores","E whether Stage 2 removes disproportionately more losses than wins","F whether Stage 2 reduces excess losses","G whether Stage 3 adds separation beyond Stage 2","H survivor calibration versus raw population","I whether market outperforms HGB in large-disagreement rows","J where survivor losses accumulate"],"definitions_frozen":True})
    json_write("eventual_decision_state_options.json", {"decision_assigned_now":False,"stage_1":["KEEP_AS_CONSIDERATION_SCREEN","REMOVE_FROM_FUNNEL","INSUFFICIENT_EVIDENCE"],"stage_2":["VALIDATED_FAILURE_AVOIDANCE_CANDIDATE","POSSIBLE_FAILURE_REMAINS","NOT_REPRODUCED","INSUFFICIENT_EVIDENCE"],"stage_3":["ADDS_DISTINCT_VALUE","REDUNDANT_AFTER_STAGE_2","HARMFUL_OVEREXCLUSION","INSUFFICIENT_EVIDENCE"]})
    json_write("validation_gate_specification.json", contract_core["validation_gate"])
    json_write("amendment_policy.json", {"current_version":CONTRACT_ID,"policy":contract_core["amendment_policy"],"silent_mutation_allowed":False})
    (OUT/"backlog_context.md").write_text("""# Frozen funnel context and backlog\n\n- Continue prospective validation of Points Funnel V1; discovery remains closed at Oct. 10–13.\n- Research survivor losses only after sufficient new evidence; do not add Stage 4 to this ledger. Record hypotheses as `FUTURE_RESEARCH_CANDIDATE`.\n- Add inclusion criteria only after exclusion validation stabilizes. No target count, cap, rank, EV, minimum edge or confidence rule exists.\n- Design an eventual read-only daily consideration view; do not wire V1 into production CLI, exports, Eight Rain, eligibility, or database filtering.\n- Decide later whether manual NHL.com review can be abandoned; it is outside this contract.\n- Preserve first live capture/grading of `NHL_SOG_D10_STRUCTURAL_CORRECTION_V1`, prospective SOG structural-shadow grading, fixed-blend collection, official-TOI binding, deferred Puck Line work, and no current need for Tier3 override.\n- Excluded predictions remain in immutable artifacts and remain gradeable.\n""")

    report = f"""# NHL Points consideration funnel V1 freeze\n\n## Frozen contract\n\n`{CONTRACT_ID}` is `FROZEN_FOR_PROSPECTIVE_VALIDATION` (fingerprint `{fingerprint}`). Discovery is finalized HGB Points × retained market evidence from 2026-10-10 through 2026-10-13. Prospective validation starts 2026-10-14. No Oct. 10–13 outcomes were used to tune or alter the frozen rules.\n\n- **Stage 1 — `RESEARCH_EXCLUDE_NEGATIVE_EDGE`:** exclude iff selected-side production edge < 0. The edge is selected-side model probability minus raw American implied probability from that side’s separately aggregated median attachment price; this includes vig. Missing edge is `EDGE_UNAVAILABLE` and is retained.\n- **Stage 2 — `RESEARCH_EXCLUDE_EXTREME_MODEL_MARKET_GAP`:** among Stage 1 survivors, exclude iff signed selected-side model probability minus selected-side no-vig market probability > 0.15. This is strict and positive-only.\n- **Stage 3 — `PROVISIONAL_STAGE_3_OPPOSITE_SIDE_DISAGREEMENT`:** among Stage 2 survivors, exclude iff model side and no-vig market favorite differ. It remains provisional and evaluated separately.\n\nOrder is Stage 0 → Stage 1 → Stage 2 → Stage 3. Each row has only one first-exclusion stage; overlapping later flags are diagnostics and never double-count removals. These are `RESEARCH_CONSIDERATION_EXCLUSIONS`, with no effect on prediction generation, immutable predictions, scoring, exports, production authority, or grading. No inclusion criteria, candidate target, or Stage 4 was added.\n\n## Frozen reference and first unseen date\n\nDiscovery 0.5 reference counts reproduce as 658 → 538 → 263 → 227. Oct. 14’s pre-outcome dry view remains bound to its prior artifact: 93 market-usable rows across lines, 43 survivors; at 0.5, 58 → 49 → 23 → 20. Its classification timestamp is {summary_prior['dry_view']['observation_timestamp_utc']}.\n\nExisting official reconciliation has no Oct. 14 package; the latest available final reconciliation is Oct. 13. Therefore `OCT14_PROSPECTIVE_VALIDATION = AWAITING_FINAL_OUTCOMES`. No outcome has been joined, and this is not a graded validation date.\n\nThe append-only ledger is initialized with all 93 pre-outcome classifications and blank outcomes. Discovery and prospective populations are separate; cumulative prospective grades begin at zero. At least six independent finalized HGB dates are required before a repeatability review, and six dates alone do not establish proof.\n\n## Validation design\n\nFor each future finalized date, the schemas reserve counts removed/surviving at each stage, cohort wins/losses, expected/observed/excess losses, model and market Brier/log loss, calibration gap, Stage 2 loss/win removal and Stage 3 incremental separation. Stage 2 subgroup reporting (UNDER/OVER, same-side confidence disagreement, opposite-side) is diagnostic only. Stage 1 rationale remains `NONCOMPETITIVE_FOR_CONSIDERATION`, not a validated failure region. Any later possible regime is recorded only as `FUTURE_RESEARCH_CANDIDATE`; no same-ledger Stage 4 test.\n\nAny rule, order, threshold, edge, signed/absolute gap, Stage 3, or missing-value change requires a new version such as V2 after an explicit research decision. There is no automatic production promotion.\n\nProvider calls: 0. Paid credits: 0. Production DB mutations: 0. Manual NHL.com review: not part of this funnel.\n"""
    (OUT/"report.md").write_text(report)

    inputs = {str(p.relative_to(ROOT)): sha(p) for p in [prior_csv, prior_summary,
        DISCOVERY/"lineage_manifest.json"]}
    outputs = {p.name: sha(p) for p in sorted(OUT.iterdir()) if p.is_file() and p.name != "lineage_manifest.json"}
    json_write("summary.json", {"contract_version":CONTRACT_ID,"status":"FROZEN_FOR_PROSPECTIVE_VALIDATION",
        "contract_fingerprint":fingerprint,"discovery_dates":["2026-10-10","2026-10-11","2026-10-12","2026-10-13"],
        "prospective_start_date":"2026-10-14","discovery_counts_0_5":DISCOVERY_COUNTS["0.5"],
        "oct14":{"final_outcomes_available":False,"validation_status":"AWAITING_FINAL_OUTCOMES","pre_outcome_market_usable":93,"pre_outcome_survivors":43,"line_counts":dry_counts},
        "ledger_rows":len(ledger),"ledger_status":"PRE_OUTCOME_CLASSIFICATIONS_FROZEN_OUTCOMES_PENDING",
        "prospective_finalized_dates":0,"minimum_independent_finalized_dates":6,
        "discovery_and_validation_separate":True,"provider_calls":0,"paid_credits":0,"production_db_mutations":0})
    outputs = {p.name: sha(p) for p in sorted(OUT.iterdir()) if p.is_file() and p.name != "lineage_manifest.json"}
    json_write("lineage_manifest.json", {"contract_version":CONTRACT_ID,"source_commit":DISCOVERY_COMMIT,
        "inputs":inputs,"outputs":outputs,"generation":"./bin/python -m backend.nhl.scripts.freeze_nhl_points_consideration_funnel",
        "official_reconciliation_inspected":"artifacts/operational/nhl/postgame_reconciliation/; no 2026-10-14 reconciliation package exists"})


if __name__ == "__main__":
    main()
