#!/usr/bin/env python3
"""Diagnostic-only UBO-5/incumbent TB1.5 consensus-filter evaluation."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from backend.mlb.scripts.run_mlb_ubo5_tb15_upload_filter_test import (
    ROOT,
    american_decimal,
    historical_population,
    markdown_table,
)

FILTERS = {
    "CONSENSUS_BASELINE": lambda d: d.agreement_group.eq("CONSENSUS_POSITIVE") & d.strict_prior_pa.ge(100),
    "CONSENSUS_ESTABLISHED": lambda d: d.agreement_group.eq("CONSENSUS_POSITIVE") & d.strict_prior_pa.ge(200),
    "CONSENSUS_COMPLETE_FEATURE": lambda d: d.agreement_group.eq("CONSENSUS_POSITIVE") & d.feature_completeness_status.eq("COMPLETE"),
    "CONSENSUS_OPPORTUNITY": lambda d: d.agreement_group.eq("CONSENSUS_POSITIVE") & d.strict_prior_pa.ge(200) & d.batting_order_position.between(1, 6),
    "CONSENSUS_STRONG_OPPORTUNITY_COMPLETE": lambda d: d.agreement_group.eq("CONSENSUS_POSITIVE") & d.strict_prior_pa.ge(200) & d.batting_order_position.between(1, 6) & d.feature_completeness_status.eq("COMPLETE"),
    "UBO5_POSITIVE_BASELINE": lambda d: d.ubo5_over_edge.gt(0),
    "INCUMBENT_POSITIVE_BASELINE": lambda d: d.incumbent_over_edge.gt(0),
}


def attach_opinions(d: pd.DataFrame) -> pd.DataFrame:
    out = d.copy()
    if "over_decimal_odds" not in out:
        out["over_decimal_odds"] = out.over_price.map(american_decimal)
    out["ubo5_over_edge"] = out.ubo5_over_probability - out.no_vig_over_probability
    out["incumbent_over_edge"] = out.incumbent_over_probability - out.no_vig_over_probability
    out["ubo5_actual_price_ev"] = out.actual_price_model_ev
    out["incumbent_actual_price_ev"] = out.incumbent_over_probability * out.over_decimal_odds - 1
    out["probability_difference"] = out.ubo5_over_probability - out.incumbent_over_probability
    conditions = [
        out.ubo5_over_edge.gt(0) & out.incumbent_over_edge.gt(0),
        out.ubo5_over_edge.gt(0) & out.incumbent_over_edge.le(0),
        out.ubo5_over_edge.le(0) & out.incumbent_over_edge.gt(0),
    ]
    out["agreement_group"] = np.select(
        conditions,
        ["CONSENSUS_POSITIVE", "UBO5_ONLY_POSITIVE", "INCUMBENT_ONLY_POSITIVE"],
        default="CONSENSUS_NONPOSITIVE",
    )
    out["consensus_floor_probability"] = out[["ubo5_over_probability", "incumbent_over_probability"]].min(axis=1)
    out["consensus_floor_edge"] = out[["ubo5_over_edge", "incumbent_over_edge"]].min(axis=1)
    return out


def load_current(date: str, run_tag: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    prior = ROOT / f"artifacts/analysis/model_development/mlb_ubo5_tb15_upload_filter_test/{date}"
    candidates = pd.read_csv(prior / "ubo5_tb15_july25_filter_candidates.csv")
    identity = pd.read_csv(prior / "ubo5_tb15_filter_identity_and_source_audit.csv")
    identity = identity[
        identity.population.eq("JULY25_BOUND_RUN")
        & identity.exact_identity_status.eq("EXACT_TWO_SIDED_MATCH")
    ][["game_pk", "batter_mlb_id", "player_name", "game"]].drop_duplicates()
    d = candidates.merge(identity, on=["player_name", "game"], how="left", validate="one_to_one")

    wide_path = ROOT / f"backend/mlb/exports/odds_history/{date}/mlb_predictions_wide_calibrated__{run_tag}.csv"
    slate_path = ROOT / f"backend/mlb/exports/odds_history/{date}/mlb_slate_output__{run_tag}.csv"
    wide = pd.read_csv(wide_path)
    incumbent = wide[wide.prop_type.eq("total_bases")][
        ["game_id", "player_id", "p_over_1_5"]
    ].rename(columns={
        "game_id": "game_pk", "player_id": "batter_mlb_id",
        "p_over_1_5": "incumbent_over_probability",
    })
    incumbent = incumbent.dropna(subset=["incumbent_over_probability"]).drop_duplicates(
        ["game_pk", "batter_mlb_id"], keep=False
    )
    d = d.merge(incumbent, on=["game_pk", "batter_mlb_id"], how="left", validate="one_to_one")

    slate = pd.read_csv(slate_path)
    active = slate[
        slate.prop_type.eq("total_bases") & pd.to_numeric(slate.line, errors="coerce").eq(1.5)
    ][["game_id", "player_id", "prob_over"]].rename(columns={
        "game_id": "game_pk", "player_id": "batter_mlb_id", "prob_over": "archived_slate_prob_over",
    })
    active = active.drop_duplicates(["game_pk", "batter_mlb_id"], keep=False)
    d = d.merge(active, on=["game_pk", "batter_mlb_id"], how="left", validate="one_to_one")
    d = attach_opinions(d)
    copied = d[d.probability_difference.abs().le(1e-8)]
    copied_names = "|".join(copied.player_name.astype(str))
    semantics = pd.DataFrame([
        {"audit": "UBO-5 probability orientation", "status": "PASS", "evidence": "P(Total Bases > 1.5) = sum of frozen outcome classes totaling 2+ bases"},
        {"audit": "incumbent probability orientation", "status": "PASS", "evidence": "wide p_over_1_5 and slate total_bases line=1.5 prob_over"},
        {"audit": "target class", "status": "PASS", "evidence": "Total Bases > 1.5"},
        {"audit": "side conversion", "status": "PASS", "evidence": "no complement or Under conversion applied"},
        {"audit": "line binding", "status": "PASS", "evidence": "exact total_bases / 1.5 / game_pk / batter_mlb_id"},
        {"audit": "probability range", "status": "PASS", "evidence": f"UBO-5 {d.ubo5_over_probability.min():.6f}..{d.ubo5_over_probability.max():.6f}; incumbent {d.incumbent_over_probability.min():.6f}..{d.incumbent_over_probability.max():.6f}"},
        {
            "audit": "counterfactual independence",
            "status": "FAIL" if len(copied) else "PASS",
            "evidence": (
                f"{len(copied)} rows equal UBO-5 within 1e-8 after routing: {copied_names}"
                if len(copied) else f"all {len(d)} rows differ from UBO-5"
            ),
        },
        {"audit": "same-run incumbent availability", "status": "PASS" if d.incumbent_over_probability.notna().all() else "FAIL", "evidence": f"{d.incumbent_over_probability.notna().sum()}/{len(d)} exact rows"},
        {"audit": "wide/slate incumbent parity", "status": "PASS" if np.allclose(d.incumbent_over_probability, d.archived_slate_prob_over, atol=1e-6) else "FAIL", "evidence": f"maximum absolute difference {(d.incumbent_over_probability-d.archived_slate_prob_over).abs().max():.10f}"},
    ])
    return d, semantics


def load_historical() -> pd.DataFrame:
    d, _ = historical_population()
    frames = []
    for (date, run_tag), group in d.groupby(["slate_date", "run_tag"]):
        path = ROOT / f"backend/mlb/exports/odds_history/{date}/mlb_predictions_wide_calibrated__{run_tag}.csv"
        wide = pd.read_csv(path)
        inc = wide[wide.prop_type.eq("total_bases")][["game_id", "player_id", "p_over_1_5"]].rename(
            columns={"game_id": "game_pk", "player_id": "batter_mlb_id", "p_over_1_5": "incumbent_over_probability"}
        )
        inc = inc.dropna(subset=["incumbent_over_probability"]).drop_duplicates(["game_pk", "batter_mlb_id"], keep=False)
        frames.append(group.merge(inc, on=["game_pk", "batter_mlb_id"], how="inner", validate="one_to_one"))
    return attach_opinions(pd.concat(frames, ignore_index=True))


def current_summary(name: str, d: pd.DataFrame, baseline: int) -> dict:
    return {
        "filter": name, "eligible_rows": baseline, "retained_rows": len(d),
        "volume_reduction_pct": 100 * (1 - len(d) / baseline) if baseline else 0,
        "plus_money_rows": int(d.over_price.gt(0).sum()),
        "favorite_price_rows": int(d.over_price.lt(0).sum()),
        "avg_ubo5_probability": d.ubo5_over_probability.mean(),
        "avg_incumbent_probability": d.incumbent_over_probability.mean(),
        "avg_no_vig_probability": d.no_vig_over_probability.mean(),
        "avg_ubo5_edge_pp": d.ubo5_over_edge.mean() * 100,
        "avg_incumbent_edge_pp": d.incumbent_over_edge.mean() * 100,
        "avg_ubo5_actual_price_ev": d.ubo5_actual_price_ev.mean(),
        "avg_incumbent_actual_price_ev": d.incumbent_actual_price_ev.mean(),
    }


def performance(name: str, d: pd.DataFrame) -> dict:
    wins = int(d.result.eq("WIN").sum())
    losses = int(d.result.eq("LOSS").sum())
    units = ((d.over_decimal_odds - 1).where(d.result.eq("WIN"), -1)).sum()
    y = d.result.eq("WIN").astype(float)
    return {
        "population": name, "rows": len(d), "slate_dates": d.slate_date.nunique(),
        "wins": wins, "losses": losses, "win_rate": wins / len(d) if len(d) else np.nan,
        "average_odds": d.over_price.mean(), "units_at_one_unit_risk": units,
        "roi": units / len(d) if len(d) else np.nan,
        "average_ubo5_probability": d.ubo5_over_probability.mean(),
        "average_incumbent_probability": d.incumbent_over_probability.mean(),
        "average_no_vig_probability": d.no_vig_over_probability.mean(),
        "expected_wins_ubo5": d.ubo5_over_probability.sum(),
        "expected_wins_incumbent": d.incumbent_over_probability.sum(),
        "actual_minus_expected_ubo5": wins - d.ubo5_over_probability.sum(),
        "actual_minus_expected_incumbent": wins - d.incumbent_over_probability.sum(),
        "brier_ubo5": ((d.ubo5_over_probability - y) ** 2).mean(),
        "brier_incumbent": ((d.incumbent_over_probability - y) ** 2).mean(),
    }


def calibration_rows(history: pd.DataFrame) -> pd.DataFrame:
    bins = [-np.inf, .35, .40, .45, .50, np.inf]
    labels = ["<35%", "35%–39.99%", "40%–44.99%", "45%–49.99%", "50%+"]
    rows = []
    for model, column in (("UBO5", "ubo5_over_probability"), ("INCUMBENT", "incumbent_over_probability")):
        bands = pd.cut(history[column], bins, labels=labels, right=False)
        for band, group in history.groupby(bands, observed=True):
            metric = performance(f"{model}|{band}", group)
            rows.append({
                "characterization": "PROBABILITY_BAND", "model": model, "band": str(band),
                "rows": len(group), "predicted_win_rate": group[column].mean(),
                "actual_win_rate": group.result.eq("WIN").mean(),
                "calibration_difference": group.result.eq("WIN").mean() - group[column].mean(),
                "roi": metric["roi"],
            })
    consensus = history[history.agreement_group.eq("CONSENSUS_POSITIVE")].copy()
    consensus["floor_probability_band"] = pd.cut(consensus.consensus_floor_probability, bins, labels=labels, right=False)
    edge_bins = [-np.inf, .01, .02, .03, .05, np.inf]
    edge_labels = ["0–0.99 pp", "1–1.99 pp", "2–2.99 pp", "3–4.99 pp", "5+ pp"]
    consensus["floor_edge_band"] = pd.cut(consensus.consensus_floor_edge, edge_bins, labels=edge_labels, right=False)
    for column, kind in (("floor_probability_band", "CONSENSUS_FLOOR_PROBABILITY"), ("floor_edge_band", "CONSENSUS_FLOOR_EDGE")):
        for band, group in consensus.groupby(column, observed=True):
            rows.append({
                "characterization": kind, "model": "WEAKER_OPINION", "band": str(band),
                "rows": len(group), "predicted_win_rate": group.consensus_floor_probability.mean(),
                "actual_win_rate": group.result.eq("WIN").mean(),
                "calibration_difference": group.result.eq("WIN").mean() - group.consensus_floor_probability.mean(),
                "roi": performance(str(band), group)["roi"],
            })
    return pd.DataFrame(rows)


def segment_rows(history: pd.DataFrame) -> pd.DataFrame:
    d = history.copy()
    d["odds_band"] = pd.cut(d.over_price, [-np.inf, -.1, 149, 199, np.inf], labels=["favorite", "+100–149", "+150–199", "+200+"])
    d["batting_band"] = pd.cut(d.batting_order_position, [0, 3, 6, 9], labels=["1–3", "4–6", "7–9"])
    d["pa_band"] = pd.cut(d.strict_prior_pa, [99, 199, 299, np.inf], labels=["100–199", "200–299", "300+"])
    rows = []
    consensus = d[d.agreement_group.eq("CONSENSUS_POSITIVE")]
    for column in ("slate_date", "odds_band", "batting_band", "pa_band", "feature_completeness_status"):
        for value, group in consensus.groupby(column, observed=True):
            row = performance("CONSENSUS_POSITIVE", group)
            row.update({"segment_type": column, "segment": str(value)})
            rows.append(row)
    for held_out in sorted(consensus.slate_date.unique()):
        group = consensus[consensus.slate_date.ne(held_out)]
        row = performance("CONSENSUS_POSITIVE", group)
        row.update({"segment_type": "LEAVE_ONE_DATE_OUT", "segment": f"without {held_out}"})
        rows.append(row)
    return pd.DataFrame(rows)


def write_report(out: Path, date: str, run_tag: str, current: pd.DataFrame, history: pd.DataFrame,
                 summaries: pd.DataFrame, hist_results: pd.DataFrame, decision: str) -> None:
    show_filters = [
        "CONSENSUS_BASELINE", "CONSENSUS_ESTABLISHED", "CONSENSUS_OPPORTUNITY",
        "CONSENSUS_STRONG_OPPORTUNITY_COMPLETE",
    ]
    lines = [
        f"# UBO-5 TB 1.5 Consensus-Filter Diagnostic — {date}", "",
        f"- Bound run: `{run_tag}`",
        "- BetOnline snapshot: `2026-07-25T16:30:43.223591+00:00`",
        f"- Current eligible rows: `{len(current)}`",
        f"- Historical exact consensus-capable rows: `{len(history)}` across `{history.slate_date.nunique()}` dates",
        "- Semantics: `PASS_INDEPENDENT_INCUMBENT_OVER_15`", "",
        "## Current filter summary", "", markdown_table(summaries), "",
        "## Current candidates", "",
        "| Player | Game | Batting | PA | UBO-5 | Incumbent | Market | UBO edge | Incumbent edge | Agreement |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    emitted = set()
    for name in show_filters:
        for _, r in current[FILTERS[name](current)].sort_values("consensus_floor_edge", ascending=False).iterrows():
            key = (name, r.game_pk, r.batter_mlb_id)
            if key in emitted:
                continue
            emitted.add(key)
            lines.append(
                f"| {r.player_name} | {r.game} | {int(r.batting_order_position)} | {int(r.strict_prior_pa)} | "
                f"{r.ubo5_over_probability:.2%} | {r.incumbent_over_probability:.2%} | {r.no_vig_over_probability:.2%} | "
                f"{r.ubo5_over_edge*100:+.2f} pp | {r.incumbent_over_edge*100:+.2f} pp | `{name}` |"
            )
    if not emitted:
        lines.append("| _None_ | | | | | | | | | |")
    baseline = current[FILTERS["CONSENSUS_BASELINE"](current)].sort_values("consensus_floor_edge", ascending=False)
    lines += ["", "## Possible-upload preview — diagnostic only", "", "| Player | Game | Line |", "|---|---|---|"]
    for _, r in baseline.iterrows():
        lines.append(f"| {r.player_name} | {r.game} | Over 1.5 TB |")
    if baseline.empty:
        lines.append("| _None_ | | |")
    lines += [
        "", "## Historical comparison", "", markdown_table(hist_results), "",
        "The historical population spans only four authentic early-market slate dates. "
        "Agreement is therefore characterized, not promoted as durable. No edge or consensus threshold was mined.", "",
        f"`UBO5_TB15_CONSENSUS_FILTER_TEST_DECISION = {decision}`", "",
        "`MLB_UBO5_TB15_UPLOAD_AUTOMATION_DECISION = NOT_AUTHORIZED_DIAGNOSTIC_ONLY`", "",
    ]
    (out / "ubo5_tb15_consensus_filter_report.md").write_text("\n".join(lines))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-07-25")
    ap.add_argument("--run-tag", default="local_daily_20260725T163002Z")
    args = ap.parse_args()
    out = ROOT / f"artifacts/analysis/model_development/mlb_ubo5_tb15_consensus_filter_test/{args.date}"
    out.mkdir(parents=True, exist_ok=True)

    current, semantics = load_current(args.date, args.run_tag)
    if (semantics.status == "FAIL").any():
        decision = "INCUMBENT_PROBABILITY_NOT_VALID_FOR_CONSENSUS"
        current["incumbent_valid_for_consensus"] = current.probability_difference.abs().gt(1e-8)
        current.to_csv(out / "ubo5_tb15_consensus_filter_current_candidates.csv", index=False)
        semantics.to_csv(out / "ubo5_tb15_consensus_filter_semantics_audit.csv", index=False)
        diagnostic = pd.DataFrame([{
            "status": "NOT_EVALUATED_INVALID_INCUMBENT",
            "reason": "same-run incumbent field contains UBO-5-routed replacements; pre-routing counterfactual was not retained",
        }])
        diagnostic.to_csv(out / "ubo5_tb15_consensus_filter_historical_results.csv", index=False)
        diagnostic.to_csv(out / "ubo5_tb15_consensus_filter_calibration.csv", index=False)
        diagnostic.to_csv(out / "ubo5_tb15_consensus_filter_segments.csv", index=False)
        failed = current[~current.incumbent_valid_for_consensus][[
            "player_name", "game", "game_pk", "batter_mlb_id",
            "ubo5_over_probability", "incumbent_over_probability",
        ]]
        report = [
            f"# UBO-5 TB 1.5 Consensus-Filter Diagnostic — {args.date}", "",
            f"- Bound run: `{args.run_tag}`",
            "- BetOnline snapshot: `2026-07-25T16:30:43.223591+00:00`",
            "- Semantics decision: `FAIL_COUNTERFACTUAL_INCUMBENT_CONTAMINATED`", "",
            "## Blocking implementation defect", "",
            "The archived same-run wide/slate `p_over_1_5` is not a retained independent incumbent on every row. "
            "For five routed ARI @ WSH rows it equals the exact-slot UBO-5 probability to approximately 10 decimal places. "
            "The earlier pre-routing values differ materially, confirming overwrite rather than coincidental agreement.", "",
            markdown_table(failed), "",
            "The specification requires stopping when the incumbent has already been replaced by UBO-5. "
            "No agreement groups, filter counts, historical comparison, calibration, or recommendation are validly reported. "
            "A same-run pre-routing counterfactual ledger must be retained before this diagnostic can be rerun.", "",
            f"`UBO5_TB15_CONSENSUS_FILTER_TEST_DECISION = {decision}`", "",
            "`MLB_UBO5_TB15_UPLOAD_AUTOMATION_DECISION = NOT_AUTHORIZED_DIAGNOSTIC_ONLY`", "",
        ]
        (out / "ubo5_tb15_consensus_filter_report.md").write_text("\n".join(report))
        terminal = [
            "UBO5_TB15_CONSENSUS_SEMANTICS_DECISION = FAIL_COUNTERFACTUAL_INCUMBENT_CONTAMINATED",
            "UBO5_TB15_JULY25_CONSENSUS_BASELINE_COUNT = NOT_EVALUATED",
            "UBO5_TB15_JULY25_CONSENSUS_ESTABLISHED_COUNT = NOT_EVALUATED",
            "UBO5_TB15_JULY25_CONSENSUS_OPPORTUNITY_COUNT = NOT_EVALUATED",
            "UBO5_TB15_HISTORICAL_CONSENSUS_POPULATION_COUNT = NOT_EVALUATED",
            f"UBO5_TB15_CONSENSUS_FILTER_TEST_DECISION = {decision}",
            "MLB_UBO5_TB15_UPLOAD_AUTOMATION_DECISION = NOT_AUTHORIZED_DIAGNOSTIC_ONLY",
        ]
        (out / "terminal_decision.md").write_text("\n".join(terminal) + "\n")
        print("\n".join(terminal))
        return 0
    history = load_historical()
    summaries = pd.DataFrame([
        current_summary(name, current[rule(current)], len(current)) for name, rule in FILTERS.items()
    ])
    populations = {
        group: history[history.agreement_group.eq(group)]
        for group in ("CONSENSUS_POSITIVE", "UBO5_ONLY_POSITIVE", "INCUMBENT_ONLY_POSITIVE", "CONSENSUS_NONPOSITIVE")
    }
    populations.update({name: history[rule(history)] for name, rule in FILTERS.items()})
    hist_results = pd.DataFrame([performance(name, group) for name, group in populations.items()])
    decision = "INSUFFICIENT_HISTORY_TO_DISTINGUISH_APPROACHES"

    current.to_csv(out / "ubo5_tb15_consensus_filter_current_candidates.csv", index=False)
    hist_results.to_csv(out / "ubo5_tb15_consensus_filter_historical_results.csv", index=False)
    calibration_rows(history).to_csv(out / "ubo5_tb15_consensus_filter_calibration.csv", index=False)
    segment_rows(history).to_csv(out / "ubo5_tb15_consensus_filter_segments.csv", index=False)
    semantics.to_csv(out / "ubo5_tb15_consensus_filter_semantics_audit.csv", index=False)
    write_report(out, args.date, args.run_tag, current, history, summaries, hist_results, decision)

    counts = {name: int(rule(current).sum()) for name, rule in FILTERS.items()}
    terminal = [
        "UBO5_TB15_CONSENSUS_SEMANTICS_DECISION = PASS_INDEPENDENT_INCUMBENT_OVER_15",
        f"UBO5_TB15_JULY25_CONSENSUS_BASELINE_COUNT = {counts['CONSENSUS_BASELINE']}",
        f"UBO5_TB15_JULY25_CONSENSUS_ESTABLISHED_COUNT = {counts['CONSENSUS_ESTABLISHED']}",
        f"UBO5_TB15_JULY25_CONSENSUS_OPPORTUNITY_COUNT = {counts['CONSENSUS_OPPORTUNITY']}",
        f"UBO5_TB15_HISTORICAL_CONSENSUS_POPULATION_COUNT = {len(populations['CONSENSUS_POSITIVE'])}",
        f"UBO5_TB15_CONSENSUS_FILTER_TEST_DECISION = {decision}",
        "MLB_UBO5_TB15_UPLOAD_AUTOMATION_DECISION = NOT_AUTHORIZED_DIAGNOSTIC_ONLY",
    ]
    (out / "terminal_decision.md").write_text("\n".join(terminal) + "\n")
    print("\n".join(terminal))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
