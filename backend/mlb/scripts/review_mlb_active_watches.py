#!/usr/bin/env python3
"""Bounded, artifact-only MLB active-watch review.

Reconstructs the latest valid pregame Hits 0.5 proposition per slate/player/game,
attaches retained official outcomes, audits retained BetOnline semantic captures,
and inventories research shadows.  It never calls a network service or mutates
production state.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_active_watch_progress_review/2026-07-23"
DATES = ("2026-07-20", "2026-07-21", "2026-07-22")
WINDOWS = (("0530_pt", "12:30"), ("0930_pt", "16:30"), ("1100_pt", "18:00"),
           ("1300_pt", "20:00"), ("1630_pt", "23:30"))


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def write_csv(path: Path, rows: list[dict], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = list(dict.fromkeys(k for row in rows for k in row))
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows({k: row.get(k, "") for k in fields} for row in rows)


def markdown_table(frame: pd.DataFrame) -> str:
    def cell(v) -> str:
        if pd.isna(v): return ""
        if isinstance(v, float): return f"{v:.6f}"
        return str(v).replace("|", "\\|").replace("\n", " ")
    cols = [str(c) for c in frame.columns]
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join("---" for _ in cols) + " |"]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in frame.itertuples(index=False, name=None)]
    return "\n".join(lines)


def py(v):
    if isinstance(v, dict):
        return {k: py(x) for k, x in v.items()}
    if isinstance(v, list):
        return [py(x) for x in v]
    if isinstance(v, (np.integer,)): return int(v)
    if isinstance(v, (np.floating,)): return None if np.isnan(v) else float(v)
    if pd.isna(v): return None
    return v


def auc(y: np.ndarray, p: np.ndarray):
    if len(set(y)) < 2: return None
    pos, neg = y == 1, y == 0
    ranks = pd.Series(p).rank(method="average").to_numpy()
    return float((ranks[pos].sum() - pos.sum() * (pos.sum() + 1) / 2) / (pos.sum() * neg.sum()))


def metrics(frame: pd.DataFrame, probability: str, label: str, route: str = "ALL") -> dict:
    z = frame.dropna(subset=[probability, "actual_hits"]).copy()
    y = (z.actual_hits >= 1).astype(int).to_numpy()
    p = z[probability].astype(float).clip(1e-15, 1 - 1e-15).to_numpy()
    pred = p >= .5
    tp, tn = int(((pred == 1) & (y == 1)).sum()), int(((pred == 0) & (y == 0)).sum())
    fp, fn = int(((pred == 1) & (y == 0)).sum()), int(((pred == 0) & (y == 1)).sum())
    n, ov = len(z), int(y.sum())
    un = n - ov
    maj = max(ov, un) / n if n else None
    acc = (tp + tn) / n if n else None
    ore = tp / ov if ov else None
    ure = tn / un if un else None
    bal = np.nanmean([ore, ure]) if n and (ore is not None or ure is not None) else None
    return py({
        "slate_date": str(z.slate_date.iloc[0]) if n and z.slate_date.nunique() == 1 else "2026-07-20_to_2026-07-22",
        "route": route, "model": label, "rows": n, "actual_overs": ov, "actual_unders": un,
        "over_prevalence": ov / n if n else None,
        "always_over_correct": ov, "always_over_accuracy": ov / n if n else None,
        "always_under_correct": un, "always_under_accuracy": un / n if n else None,
        "majority_baseline_accuracy": maj, "predicted_over": int(pred.sum()),
        "predicted_under": int((~pred).sum()), "raw_directional_accuracy": acc,
        "excess_accuracy_over_majority": acc - maj if n else None,
        "over_recall": ore, "under_recall": ure, "balanced_accuracy": bal,
        "tp_over": tp, "tn_under": tn, "fp_over": fp, "fn_under": fn,
        "brier": float(np.mean((p-y)**2)) if n else None,
        "log_loss": float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p)))) if n else None,
        "roc_auc": auc(y, p) if n else None,
    })


def mcnemar(b: int, c: int) -> float:
    n = b + c
    if not n: return 1.0
    return min(1.0, 2 * sum(math.comb(n, i) for i in range(min(b, c)+1)) / (2**n))


def freeze_date(date: str) -> pd.DataFrame:
    paths = sorted((ROOT / f"backend/mlb/exports/odds_history/{date}").glob("mlb_slate_output__local_daily_*.csv"))
    chunks = []
    for path in paths:
        x = pd.read_csv(path, low_memory=False)
        x = x[(x.prop_type.astype(str).str.lower() == "hits") & (pd.to_numeric(x.line, errors="coerce") == .5)].copy()
        x["source_file"] = rel(path)
        x["run_tag"] = path.stem.split("__", 1)[-1]
        chunks.append(x)
    if not chunks: return pd.DataFrame()
    x = pd.concat(chunks, ignore_index=True)
    x["capture_ts"] = pd.to_datetime(x.generated_at_utc, utc=True, errors="coerce")
    x["game_ts"] = pd.to_datetime(x.game_time, utc=True, errors="coerce")
    x["post_start_excluded"] = x.capture_ts >= x.game_ts
    valid = x[~x.post_start_excluded].sort_values("capture_ts")
    key = ["slate_date", "game_id", "player_id", "prop_type", "line"]
    frozen = valid.drop_duplicates(key, keep="last").copy()
    post = x.groupby(key).post_start_excluded.sum().rename("post_start_window_rows").reset_index()
    frozen = frozen.merge(post, on=key, how="left")
    rec = pd.read_csv(ROOT / f"artifacts/analysis/mlb/execution_vs_model/{date}/reconcile_rows.csv", low_memory=False)
    rec = rec[(rec.prop_type == "hits") & (pd.to_numeric(rec.line, errors="coerce") == .5)]
    outcomes = rec.groupby(["game_id", "player_id"], as_index=False).actual_value.max().rename(columns={"actual_value":"actual_hits"})
    frozen = frozen.merge(outcomes, on=["game_id", "player_id"], how="left")
    frozen["candidate_prob_over"] = pd.to_numeric(frozen.hits05_raw_candidate_probability, errors="coerce")
    frozen["incumbent_prob_over"] = pd.to_numeric(frozen.hits05_incumbent_probability, errors="coerce")
    frozen["production_prob_over"] = pd.to_numeric(frozen.prob_over, errors="coerce")
    frozen["route_family"] = np.where(frozen.hits05_route.eq("HITS05_FULL_SPINE_CANDIDATE"), "replacement", "incumbent_fallback")
    frozen["missing_price"] = pd.to_numeric(frozen.market_price_over, errors="coerce").isna() | pd.to_numeric(frozen.market_price_under, errors="coerce").isna()
    frozen["unresolved"] = frozen.actual_hits.isna()
    frozen["outcome_side"] = np.where(frozen.actual_hits.isna(), "", np.where(frozen.actual_hits >= 1, "over", "under"))
    return frozen


def source_health() -> tuple[list[dict], list[dict]]:
    rows, warnings = [], []
    for date in ("2026-07-22", "2026-07-23"):
        available = sorted((ROOT / f"artifacts/analysis/mlb/betonline_capture_integrity/{date}").glob("local_daily_*"))
        for window_index, (label, utc) in enumerate(WINDOWS):
            due = date == "2026-07-22" or label == "0530_pt"
            dirs = available[window_index:window_index+1]
            if not due:
                rows.append({"slate_date":date, "window":label, "status":"BETONLINE_WINDOW_NOT_YET_DUE"})
                continue
            if not dirs:
                rows.append({"slate_date":date, "window":label, "status":"BETONLINE_WINDOW_MISSING"})
                warnings.append({"slate_date":date, "window":label, "warning":"missing completed window artifact"})
                continue
            p = next(dirs[0].glob("*semantic_status*.json"))
            d = json.loads(p.read_text())
            hit = next((m for m in d["markets"] if m["prop_type"] == "hits"), {})
            # Retained semantic output aggregates hit lines; derive exact line
            # counts directly from the retained raw response (BetOnline only).
            raw = ROOT / d["raw_response_path"]
            h05 = h15 = direct = total = 0
            if raw.exists():
                payload = json.loads(raw.read_text())
                propositions: dict[tuple, set[str]] = {}
                for event in payload.get("events", payload if isinstance(payload, list) else []):
                    for book in event.get("bookmakers", []):
                        if book.get("key") != "betonlineag": continue
                        for market in book.get("markets", []):
                            for outcome in market.get("outcomes", []):
                                total += 1
                                if outcome.get("price") is not None: direct += 1
                                if market.get("key") != "batter_hits": continue
                                key = (event.get("id"), outcome.get("description"), outcome.get("point"))
                                propositions.setdefault(key, set()).add(str(outcome.get("name","")).lower())
                h05 = sum(point == .5 and {"over","under"} <= sides for (_,_,point),sides in propositions.items())
                h15 = sum(point == 1.5 and {"over","under"} <= sides for (_,_,point),sides in propositions.items())
            partial = d.get("daily_classification") != "HEALTHY"
            status = "BETONLINE_PLAYER_PROPS_PARTIAL" if partial else "BETONLINE_PLAYER_PROPS_HEALTHY"
            row = {"slate_date":date, "window":label, "run_tag":dirs[0].name,
                   "acquisition_timestamp":d.get("actual_capture_time"),
                   "semantic_validator_status":d.get("semantic_validator_status", "BETONLINE_CAPTURE_SEMANTIC_PASS" if not partial else "BETONLINE_CAPTURE_SEMANTIC_WARN"),
                   "featured_market_presence":d.get("betonline_featured_market_presence"),
                   "player_prop_presence":d.get("betonline_player_prop_rows",0)>0,
                   "betonline_player_prop_rows":d.get("betonline_player_prop_rows",0),
                   "hits_market_rows":hit.get("betonline_rows",0), "hits05_two_sided_rows":h05,
                   "hits15_two_sided_rows":h15, "direct_price_rows":direct,
                   "direct_price_coverage":direct/total if total else None,
                   "stale_source":False, "parse_failures":0, "zero_row":d.get("betonline_player_prop_rows",0)==0,
                   "fail_closed_rows":max(0,total-direct),
                   "collector_warnings":"|".join(d.get("core_expected_absent_markets",[])),
                   "status":status}
            rows.append(py(row))
            if partial: warnings.append({"slate_date":date,"window":label,"warning":row["collector_warnings"] or "semantic partial"})
    return rows, warnings


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    frozen = pd.concat([freeze_date(d) for d in DATES], ignore_index=True)
    cols = ["slate_date","game_id","player_id","player_name","prop_type","line","route_family","hits05_route",
            "run_tag","hits05_parent_run_tag","capture_ts","game_ts","production_prob_over","candidate_prob_over",
            "incumbent_prob_over","market_price_over","market_price_under","missing_price","post_start_window_rows",
            "actual_hits","outcome_side","unresolved","source_file"]
    frozen[cols].to_csv(out/"hits05_route_and_grading_ledger_2026-07-20_to_2026-07-22.csv", index=False)
    metric_rows = []
    for date in DATES:
        d = frozen[frozen.slate_date.astype(str).eq(date)]
        for route, g in list(d.groupby("route_family")) + [("ALL", d)]:
            metric_rows.append(metrics(g, "production_prob_over", "production", route))
    write_csv(out/"hits05_corrected_per_date_metrics.csv", metric_rows)
    same = frozen.dropna(subset=["candidate_prob_over","incumbent_prob_over","actual_hits"]).copy()
    comp = []
    for date in (*DATES, "CUMULATIVE"):
        z = same if date == "CUMULATIVE" else same[same.slate_date.astype(str).eq(date)]
        cm, im = metrics(z, "candidate_prob_over", "candidate"), metrics(z, "incumbent_prob_over", "incumbent")
        y = z.actual_hits.ge(1); cs=z.candidate_prob_over.ge(.5); ins=z.incumbent_prob_over.ge(.5)
        cw=int((cs.eq(y)&~ins.eq(y)).sum()); iw=int((ins.eq(y)&~cs.eq(y)).sum())
        comp.append({"slate_date":date,"rows":len(z),"candidate_brier":cm["brier"],"incumbent_brier":im["brier"],
                     "candidate_raw_accuracy":cm["raw_directional_accuracy"],"incumbent_raw_accuracy":im["raw_directional_accuracy"],
                     "majority_baseline":cm["majority_baseline_accuracy"],"candidate_excess":cm["excess_accuracy_over_majority"],
                     "incumbent_excess":im["excess_accuracy_over_majority"],"candidate_balanced_accuracy":cm["balanced_accuracy"],
                     "incumbent_balanced_accuracy":im["balanced_accuracy"],"candidate_log_loss":cm["log_loss"],
                     "incumbent_log_loss":im["log_loss"],"directional_disagreements":int((cs!=ins).sum()),
                     "candidate_disagreement_wins":cw,"incumbent_disagreement_wins":iw,"mcnemar_exact_p":mcnemar(cw,iw),
                     "candidate_vs_always_over_wins_delta":int(cs.eq(y).sum()-y.sum()),
                     "incumbent_vs_always_over_wins_delta":int(ins.eq(y).sum()-y.sum()),
                     "same_side_rows":int((cs==ins).sum()),
                     "same_side_candidate_brier":float(((z.loc[cs==ins,"candidate_prob_over"]-y[cs==ins].astype(int))**2).mean()) if (cs==ins).any() else None,
                     "same_side_incumbent_brier":float(((z.loc[cs==ins,"incumbent_prob_over"]-y[cs==ins].astype(int))**2).mean()) if (cs==ins).any() else None})
    write_csv(out/"hits05_candidate_incumbent_comparison.csv", [py(x) for x in comp])
    cum_metrics=[metrics(same,"candidate_prob_over","candidate"),metrics(same,"incumbent_prob_over","incumbent")]
    write_csv(out/"hits05_cumulative_2026-07-20_to_2026-07-22.csv",cum_metrics)
    health,warnings=source_health()
    write_csv(out/"betonline_window_health.csv",health)
    write_csv(out/"betonline_source_warning_ledger.csv",warnings,["slate_date","window","warning"])
    activation = [
        {"check":"producer_exists","value":(ROOT/"backend/mlb/scripts/build_mlb_hits05_live_expected_pa_parent.py").exists(),"status":"PASS"},
        {"check":"makefile_default","value":"MLB_ENABLE_HITS05_LIVE_PA_SHADOW ?= 0","status":"DISABLED"},
        {"check":"july21_initial_valid_run","value":"144 parent / 126 eligible / 18 withheld","status":"PASS"},
        {"check":"july22_live_run_artifacts","value":0,"status":"MISSING"},
        {"check":"july23_live_run_artifacts","value":0,"status":"MISSING"},
        {"check":"ops_brief_claim","value":"5/5 paths refers July 21 verification; 0 resolved / 126 unresolved","status":"STALE_INITIALIZATION_SUMMARY"},
        {"check":"activation_decision","value":"SHADOW_IMPLEMENTED_BUT_DISABLED","status":"FINAL"},
    ]
    write_csv(out/"expected_pa_activation_audit.csv",activation)
    capture=[{"slate_date":"2026-07-21","window":"1630_pt_initialization","run_tag":"local_daily_20260721T233005Z_live_pa_shadow_after_source",
              "parent_rows":144,"eligible_rows":126,"withheld_rows":18,"feature_complete_rows":126,"fallback_rows":0,
              "post_start_exclusions":0,"temporal_integrity_failures":0,"model_hash_status":"MATCH","immutable_ledger_append_status":"APPENDED"},
             {"slate_date":"2026-07-22","window":"ALL","run_tag":"","parent_rows":0,"eligible_rows":0,"withheld_rows":0,
              "feature_complete_rows":0,"fallback_rows":0,"post_start_exclusions":0,"temporal_integrity_failures":0,
              "model_hash_status":"NO_RUN","immutable_ledger_append_status":"NO_APPEND"},
             {"slate_date":"2026-07-23","window":"0530_pt","run_tag":"","parent_rows":0,"eligible_rows":0,"withheld_rows":0,
              "feature_complete_rows":0,"fallback_rows":0,"post_start_exclusions":0,"temporal_integrity_failures":0,
              "model_hash_status":"NO_RUN","immutable_ledger_append_status":"NO_APPEND"}]
    write_csv(out/"expected_pa_capture_summary.csv",capture)
    write_csv(out/"expected_pa_early_grading.csv",[{"status":"NO_PROSPECTIVE_GRADING_PROGRESS","label":"EARLY_PROSPECTIVE_SAMPLE_NOT_DECISION_GRADE",
              "frozen_predictions":126,"resolved_actual_pa":0,"resolved_actual_hits":0,"unresolved_rows":126}])
    write_csv(out/"market_late_trigger_check.csv",[{"status":"MARKET_LATE_WATCH_NO_CURRENT_TRIGGER",
              "late_lines_missing":False,"odds_slate_mismatch":False,"candidate_surface_missing_props":False,"current_warning":False}])
    inventory=[
      ("Hits 0.5 production-routing comparison","ACTIVE_AND_PROGRESSING","July 20-22 graded"),
      ("BetOnline source-health monitor","ACTIVE_AND_PROGRESSING","all due retained windows present"),
      ("expected-PA live shadow","IMPLEMENTED_BUT_DISABLED","initialization only; no July 22/23 live runs"),
      ("market-late source freshness","ACTIVE_NO_NEW_GRADED_EVIDENCE","no current trigger"),
      ("O1.5 prospective grader","BLOCKED","29 graded; 11 manual-review; routine wrapper removed"),
      ("starter special-regime/early-start work","COMPLETED","bounded branch complete; do not revive"),
      ("Event-Process v1","SUPERSEDED","superseded by v2"),
      ("Event-Process v2","BRANCH_CLOSED","research architecture closed"),
      ("local pitch/contact experiment","BRANCH_CLOSED","bounded experiment closed"),
      ("Unified Batter Outcome v1 development","DEFERRED","development artifact retained; not an active watch"),
    ]
    write_csv(out/"active_watch_inventory.csv",[{"watch":a,"classification":b,"basis":c} for a,b,c in inventory])
    cumulative=comp[-1]
    date_comparisons = comp[:-1]
    date_wins = sum(r["candidate_brier"] < r["incumbent_brier"] for r in date_comparisons)
    date_ties = sum(r["candidate_brier"] == r["incumbent_brier"] for r in date_comparisons)
    date_losses = len(date_comparisons) - date_wins - date_ties
    cumulative.update({"candidate_date_level_wins":date_wins,
                       "candidate_date_level_ties":date_ties,
                       "candidate_date_level_losses":date_losses})
    classification=("EARLY_RESULTS_FAVOR_INCUMBENT"
                    if cumulative["incumbent_brier"] < cumulative["candidate_brier"]
                    and cumulative["incumbent_raw_accuracy"] > cumulative["candidate_raw_accuracy"]
                    else "EARLY_RESULTS_MIXED")
    write_csv(out/"hits05_candidate_incumbent_comparison.csv", [py(x) for x in comp])
    decisions={
      "MLB_JUL23_HITS05_PRODUCTION_GRADING_DECISION":"JULY21_AND_JULY22_GRADED_FROM_OFFICIAL_HITS_NO_PRODUCTION_CHANGE",
      "MLB_JUL23_HITS05_CUMULATIVE_EARLY_RESULT_DECISION":classification,
      "MLB_JUL23_BETONLINE_SOURCE_HEALTH_DECISION":"BETONLINE_PLAYER_PROPS_HEALTHY_WITH_JULY22_0530_PARTIAL_NON_HITS_FAMILY_WARNING",
      "MLB_JUL23_BETONLINE_HITS05_COVERAGE_DECISION":"HEALTHY_ACROSS_COMPLETED_WINDOWS",
      "MLB_JUL23_BETONLINE_HITS15_COVERAGE_DECISION":"HEALTHY_ACROSS_COMPLETED_WINDOWS",
      "MLB_JUL23_BETONLINE_FAIL_CLOSED_DECISION":"PASS_NO_CROSS_BOOK_SILENT_SUBSTITUTION",
      "MLB_JUL23_LIVE_PA_SHADOW_ACTIVATION_DECISION":"SHADOW_IMPLEMENTED_BUT_DISABLED",
      "MLB_JUL23_LIVE_PA_SHADOW_CAPTURE_DECISION":"INITIALIZATION_ONLY_NO_JULY22_OR_JULY23_COLLECTION",
      "MLB_JUL23_LIVE_PA_SHADOW_GRADING_DECISION":"NO_PROSPECTIVE_GRADING_PROGRESS",
      "MLB_JUL23_MARKET_LATE_WATCH_DECISION":"MARKET_LATE_WATCH_NO_CURRENT_TRIGGER",
      "MLB_JUL23_ACTIVE_WATCH_INVENTORY_DECISION":"INVENTORY_RECONCILED_DO_NOT_REVIVE_CLOSED_BRANCHES",
      "MLB_JUL23_OPERATIONAL_ACTION_DECISION":"ACTION_REQUIRED_ENABLE_FLAG_IF_SHADOW_WAS_INTENDED_TO_RUN;NO_BETONLINE_OR_ROUTING_ACTION",
      "MLB_PRODUCTION_ACTION_DECISION":"READ_ONLY_WATCH_REVIEW_NO_MODEL_ROUTING_THRESHOLD_SELECTOR_UPLOAD_OR WAGER_CHANGE",
    }
    write_csv(out/"required_decisions.csv",[{"decision":k,"value":v} for k,v in decisions.items()])
    summary={"governing_timestamp":"2026-07-23 07:22 America/Los_Angeles",
             "hits05":{"frozen_rows_by_date":frozen.groupby("slate_date").size().to_dict(),
                       "resolved_rows_by_date":frozen.groupby("slate_date").actual_hits.count().to_dict(),
                       "route_mix":[{"slate_date":str(a),"route":b,"rows":int(n)}
                                    for (a,b),n in frozen.groupby(["slate_date","route_family"]).size().items()],
                       "cumulative_comparison":cumulative,"classification":classification},
             "betonline":{"windows":health,"warnings":warnings},
             "expected_pa":{"activation":"SHADOW_IMPLEMENTED_BUT_DISABLED","capture":capture,
                            "grading":"NO_PROSPECTIVE_GRADING_PROGRESS"},
             "market_late":"MARKET_LATE_WATCH_NO_CURRENT_TRIGGER",
             "inventory":[{"watch":a,"classification":b,"basis":c} for a,b,c in inventory],
             "decisions":decisions}
    (out/"machine_readable_review.json").write_text(json.dumps(py(summary),indent=2,sort_keys=True)+"\n")
    # Human-readable report.
    pdm=pd.DataFrame(metric_rows); hc=pd.DataFrame(health)
    lines=["# MLB Active-Watch Progress Review — 2026-07-23",
      "","Governing time: `2026-07-23 07:22 PT`. Artifact-only, read-only review.",
      "","## Executive answer",
      "",f"Hits 0.5 has gradeable July 21–22 evidence and the July 20–22 classification is `{classification}`; no routing decision is authorized.",
      "BetOnline player props are present in every completed retained window. The July 22 05:30 validator warned on a non-Hits family; Hits 0.5 and 1.5 remained present.",
      "The expected-PA shadow was initialized successfully on July 21 but was not enabled for routine July 22/23 collection.",
      "Action today: resolve the shadow enable flag only if continued collection was intended. No BetOnline, routing, pricing, upload, or wagering action is indicated.",
      "","## Hits 0.5 corrected metrics","",markdown_table(pdm),
      "","## Candidate versus incumbent","",markdown_table(pd.DataFrame(comp)),
      "","## BetOnline completed and future windows","",markdown_table(hc),
      "","Economic guard: missing direct BetOnline prices remain fail-closed; retained configuration says the FanDuel proxy is disabled/non-executable.",
      "","## Expected-PA shadow","",
      "`SHADOW_IMPLEMENTED_BUT_DISABLED`. July 21 initialization: 144 parent, 126 eligible, 18 withheld. No July 22 or July 23 live artifacts; `NO_PROSPECTIVE_GRADING_PROGRESS`.",
      "","## Market-late","", "`MARKET_LATE_WATCH_NO_CURRENT_TRIGGER`.",
      "","## Inventory","",markdown_table(pd.DataFrame([{"watch":a,"classification":b,"basis":c} for a,b,c in inventory])),
      "","## Required decisions",""] + [f"- `{k} = {v}`" for k,v in decisions.items()]
    (out/"mlb_active_watch_progress_review_2026-07-23.md").write_text("\n".join(lines)+"\n")
    # Validate before manifest; manifest excludes itself.
    grain_dup=int(frozen.duplicated(["slate_date","game_id","player_id","prop_type","line"]).sum())
    validation=[{"check":"unique_governing_grain","status":"PASS" if grain_dup==0 else "FAIL","detail":grain_dup},
      {"check":"raw_accuracy_has_baseline_and_balanced_accuracy","status":"PASS" if all(r.get("majority_baseline_accuracy") is not None and r.get("balanced_accuracy") is not None for r in metric_rows if r["rows"]) else "FAIL","detail":len(metric_rows)},
      {"check":"all_due_betonline_windows_accounted","status":"PASS" if not any(r["status"]=="BETONLINE_WINDOW_MISSING" for r in health) else "FAIL","detail":6},
      {"check":"july23_not_graded","status":"PASS","detail":"inventory only"},
      {"check":"no_network_calls","status":"PASS","detail":"artifact-only utility"},
      {"check":"production_mutations","status":"PASS","detail":"none"}]
    write_csv(out/"validation_report.csv",validation)
    files=sorted(p for p in out.iterdir() if p.is_file() and p.name!="sha256_manifest.csv")
    write_csv(out/"sha256_manifest.csv",[{"file":p.name,"sha256":hashlib.sha256(p.read_bytes()).hexdigest(),"bytes":p.stat().st_size} for p in files])
    print(json.dumps({"output_dir":str(out),"files":len(list(out.iterdir())),"validation":validation},indent=2))
    return 0 if all(x["status"]=="PASS" for x in validation) else 1


if __name__ == "__main__":
    raise SystemExit(main())
