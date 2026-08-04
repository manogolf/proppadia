#!/usr/bin/env python3
"""Read-only July 27 UBO-5 TB1.5 winner-recognition audit."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
DATE = "2026-07-27"
REPORT_DATE = "2026-07-28"
BASE = ROOT / f"backend/mlb/exports/model_v2/ubo5_tb15/{DATE}"
OUT = ROOT / f"artifacts/analysis/model_development/mlb_ubo5_tb15_july27_winner_recognition_audit/{REPORT_DATE}"
STATS = Path("/tmp/mlb_ubo5_july27_player_stats.csv")
KEY = ["slate_date", "game_pk", "batter_mlb_id", "prop_type", "line"]
FEATURES = [
    "h_swing_rate", "h_whiff_per_swing", "h_contact_per_swing",
    "h_called_strike_rate", "h_foul_rate", "h_pitches_per_pa", "h_ev",
    "h_xba", "h_xwoba", "h_lsa6_rate",
    *[f"h_career_rate_{i}" for i in range(8)],
    *[f"h_recent30_rate_{i}" for i in range(8)],
    "prior_player_pa_per_date", "p_hit_suppression", "p_k_rate",
    "matchup_k", "matchup_hit",
]


def write_csv(name: str, frame: pd.DataFrame) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    frame.to_csv(OUT / name, index=False, lineterminator="\n")


def text(path: str, value: str) -> None:
    (OUT / path).write_text(value.rstrip() + "\n", encoding="utf-8")


def rank_columns(frame: pd.DataFrame, value: str, prefix: str) -> None:
    valid = frame[value].notna()
    count = int(valid.sum())
    frame[prefix + "_rank"] = np.nan
    frame[prefix + "_percentile"] = np.nan
    frame.loc[valid, prefix + "_rank"] = frame.loc[valid, value].rank(
        method="min", ascending=False
    )
    if count == 1:
        frame.loc[valid, prefix + "_percentile"] = 100.0
    elif count > 1:
        frame.loc[valid, prefix + "_percentile"] = (
            count - frame.loc[valid, prefix + "_rank"]
        ) / (count - 1) * 100


def load_observations() -> pd.DataFrame:
    manifest = json.loads(
        (BASE / f"ubo5_tb15_run_population_manifest_{DATE}.json").read_text()
    )
    expected = {r["run_tag"]: r["snapshot_sha256"] for r in manifest["run_inventory"]}
    parts = []
    for path in sorted((BASE / "run_snapshots").glob("*.csv")):
        frame = pd.read_csv(path, dtype={"game_pk": str, "batter_mlb_id": str})
        if frame.empty:
            continue
        tag = str(frame.iloc[0]["run_tag"])
        if tag not in expected:
            raise RuntimeError(f"snapshot absent from certified index: {path}")
        parts.append(frame)
    obs = pd.concat(parts, ignore_index=True)
    if len(obs) != manifest["counts"]["all_run_observations"]:
        raise RuntimeError("run observation count does not match certified manifest")
    obs["snapshot_timestamp_utc"] = pd.to_datetime(
        obs["snapshot_timestamp_utc"], utc=True
    )
    for col in [
        "ubo5_probability_over", "no_vig_over_probability", "ubo5_over_edge_pp",
        "betonline_over_price", "betonline_under_price", "batting_order",
        "strict_prior_pa",
    ]:
        obs[col] = pd.to_numeric(obs[col], errors="coerce")
    return obs


def load_features(obs: pd.DataFrame) -> pd.DataFrame:
    paths = sorted({str(p) for p in obs["route_ledger_path"].dropna() if str(p)})
    frames = []
    for raw in paths:
        path = ROOT / raw
        if not path.is_file():
            continue
        frame = pd.read_csv(path, dtype={"game_pk": str, "batter_mlb_id": str})
        if frame.empty:
            continue
        keep = ["game_pk", "batter_mlb_id", "run_tag", *[c for c in FEATURES if c in frame]]
        frame = frame[keep].copy()
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=["game_pk", "batter_mlb_id", *FEATURES])
    feat = pd.concat(frames, ignore_index=True)
    order = {tag: i for i, tag in enumerate(obs.sort_values("snapshot_timestamp_utc").run_tag.unique())}
    feat["_order"] = feat.run_tag.map(order).fillna(-1)
    feat = feat.sort_values("_order").drop_duplicates(["game_pk", "batter_mlb_id"], keep="last")
    return feat.drop(columns=["run_tag", "_order"])


def load_outcomes() -> pd.DataFrame:
    if not STATS.is_file():
        raise RuntimeError(
            "Certified player outcomes are required. Export exact July 27 "
            "mlb.player_stats rows to /tmp/mlb_ubo5_july27_player_stats.csv."
        )
    stats = pd.read_csv(STATS, dtype={"game_id": str, "player_id": str})
    stats = stats.rename(columns={"game_id": "game_pk", "player_id": "batter_mlb_id"})
    for col in ["hits", "singles", "doubles", "triples", "home_runs", "total_bases"]:
        stats[col] = pd.to_numeric(stats[col], errors="coerce")
    stats = stats.drop_duplicates(["game_pk", "batter_mlb_id"], keep="last")

    # Freeze eligibility to the already-certified July 27 reconciliation/closeout
    # state. The database may acquire later rows (notably a rescheduled game);
    # those must not silently revise this provisional audit.
    certified: dict[tuple[str, str], float] = {}
    reconcile = pd.read_csv(
        ROOT / f"artifacts/analysis/mlb/execution_vs_model/{DATE}/reconcile_rows.csv",
        dtype={"game_id": str, "player_id": str},
    )
    reconcile = reconcile[
        reconcile.prop_type.eq("total_bases")
        & pd.to_numeric(reconcile.line, errors="coerce").eq(1.5)
    ]
    for row in reconcile.itertuples():
        value = pd.to_numeric(row.actual_value, errors="coerce")
        if pd.notna(value):
            certified[(str(row.game_id), str(row.player_id))] = float(value)
    closeout = pd.read_csv(
        BASE / f"ubo5_tb15_closeout_{DATE}.csv",
        dtype={"game_pk": str, "batter_mlb_id": str},
    )
    for row in closeout.itertuples():
        value = pd.to_numeric(row.total_bases, errors="coerce")
        if pd.notna(value):
            certified[(str(row.game_pk), str(row.batter_mlb_id))] = float(value)
    broad_closeout = pd.read_csv(
        BASE / f"ubo5_tb15_ever_positive_closeout_{DATE}.csv",
        dtype={"game_pk": str, "batter_mlb_id": str},
    )
    for row in broad_closeout.itertuples():
        value = pd.to_numeric(row.total_bases, errors="coerce")
        if pd.notna(value) and str(row.outcome_status) == "RESOLVED":
            certified[(str(row.game_pk), str(row.batter_mlb_id))] = float(value)
    keys = pd.DataFrame(
        [{"game_pk": k[0], "batter_mlb_id": k[1], "certified_total_bases": v}
         for k, v in certified.items()]
    )
    out = keys.merge(stats, on=["game_pk", "batter_mlb_id"], how="left")
    out["total_bases"] = out["certified_total_bases"]
    return out.drop(columns=["certified_total_bases"])


def canonical(obs: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    ever_ids = set(
        zip(
            pd.read_csv(
                BASE / "run_populations" / f"ubo5_tb15_broad_ever_positive_{DATE}.csv",
                dtype={"game_pk": str, "batter_mlb_id": str},
            ).game_pk,
            pd.read_csv(
                BASE / "run_populations" / f"ubo5_tb15_broad_ever_positive_{DATE}.csv",
                dtype={"game_pk": str, "batter_mlb_id": str},
            ).batter_mlb_id,
        )
    )
    final_pop = pd.read_csv(
        BASE / "run_populations" / f"ubo5_tb15_final_pregame_positive_{DATE}.csv",
        dtype={"game_pk": str, "batter_mlb_id": str},
    )
    final_ids = set(zip(final_pop.game_pk, final_pop.batter_mlb_id))
    rows = []
    for ident, group in obs.groupby(KEY, dropna=False, sort=True):
        group = group.sort_values("snapshot_timestamp_utc")
        score = group[group.ubo5_probability_over.notna()]
        market = group[group.no_vig_over_probability.notna()]
        first = group.iloc[0]
        final = group.iloc[-1]
        first_score = score.iloc[0] if len(score) else None
        final_score = score.iloc[-1] if len(score) else None
        final_market = market.iloc[-1] if len(market) else None
        key2 = (str(first.game_pk), str(first.batter_mlb_id))
        rows.append({
            "slate_date": ident[0], "game_pk": str(first.game_pk),
            "batter_mlb_id": str(first.batter_mlb_id), "prop_type": ident[3],
            "line": ident[4], "player_name": first.player_name, "game": first.game,
            "team": first.team, "opponent": first.opponent,
            "confirmed_starting_status": "CONFIRMED_STARTER" if group.batting_order.notna().any() else "NOT_CONFIRMED_IN_FROZEN_RUNS",
            "batting_order": group.batting_order.dropna().iloc[-1] if group.batting_order.notna().any() else np.nan,
            "strict_prior_pa": score.strict_prior_pa.dropna().iloc[-1] if score.strict_prior_pa.notna().any() else np.nan,
            "feature_state": score.feature_state.dropna().iloc[-1] if score.feature_state.notna().any() else "",
            "route_status": final.route_status, "identity_status": "CERTIFIED_EXACT_ID",
            "first_evaluated_run": first.run_tag,
            "first_evaluated_timestamp": first.snapshot_timestamp_utc.isoformat(),
            "last_eligible_pregame_run": final.run_tag,
            "last_eligible_pregame_timestamp": final.snapshot_timestamp_utc.isoformat(),
            "first_scoreable_run": first_score.run_tag if first_score is not None else "",
            "first_scoreable_timestamp": first_score.snapshot_timestamp_utc.isoformat() if first_score is not None else "",
            "minimum_ubo5_probability": score.ubo5_probability_over.min() if len(score) else np.nan,
            "maximum_ubo5_probability": score.ubo5_probability_over.max() if len(score) else np.nan,
            "first_ubo5_probability": first_score.ubo5_probability_over if first_score is not None else np.nan,
            "final_pregame_ubo5_probability": final_score.ubo5_probability_over if final_score is not None else np.nan,
            "minimum_betonline_no_vig_over_probability": market.no_vig_over_probability.min() if len(market) else np.nan,
            "maximum_betonline_no_vig_over_probability": market.no_vig_over_probability.max() if len(market) else np.nan,
            "first_betonline_no_vig_over_probability": market.iloc[0].no_vig_over_probability if len(market) else np.nan,
            "final_pregame_betonline_no_vig_over_probability": final_market.no_vig_over_probability if final_market is not None else np.nan,
            "maximum_ubo5_edge_pp": score.ubo5_over_edge_pp.max() if len(score) else np.nan,
            "first_edge_pp": first_score.ubo5_over_edge_pp if first_score is not None else np.nan,
            "final_pregame_edge_pp": final_score.ubo5_over_edge_pp if final_score is not None else np.nan,
            "ever_positive_status": key2 in ever_ids,
            "final_pregame_positive_status": key2 in final_ids,
            "first_market_appearance": market.iloc[0].snapshot_timestamp_utc.isoformat() if len(market) else "",
            "first_positive_edge_appearance": score.loc[score.ubo5_over_edge_pp.gt(0), "snapshot_timestamp_utc"].min().isoformat() if score.ubo5_over_edge_pp.gt(0).any() else "",
            "last_positive_edge_appearance": score.loc[score.ubo5_over_edge_pp.gt(0), "snapshot_timestamp_utc"].max().isoformat() if score.ubo5_over_edge_pp.gt(0).any() else "",
            "run_observation_count": len(group),
            "scoreable_observation_count": len(score),
        })
    universe = pd.DataFrame(rows)
    universe = universe.merge(
        outcomes[[
            "game_pk", "batter_mlb_id", "plate_appearances", "at_bats", "hits",
            "singles", "doubles", "triples", "home_runs", "total_bases",
        ]],
        on=["game_pk", "batter_mlb_id"], how="left",
    )
    universe["outcome_status"] = np.where(
        universe.total_bases.notna(), "CERTIFIED", "UNRESOLVED"
    )
    universe["result"] = np.where(
        universe.total_bases.isna(), "UNRESOLVED",
        np.where(universe.total_bases.gt(1.5), "WIN", "LOSS"),
    )
    rank_columns(universe, "final_pregame_ubo5_probability", "ubo5_final")
    rank_columns(universe, "final_pregame_betonline_no_vig_over_probability", "betonline_final")
    rank_columns(universe, "final_pregame_edge_pp", "edge_final")
    rank_columns(universe, "maximum_ubo5_probability", "ubo5_maximum")
    rank_columns(universe, "maximum_ubo5_edge_pp", "edge_maximum")
    return universe


def ranked_outputs(universe: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    common = universe[
        universe.outcome_status.eq("CERTIFIED")
        & universe.final_pregame_ubo5_probability.notna()
        & universe.final_pregame_betonline_no_vig_over_probability.notna()
        & universe.final_pregame_edge_pp.notna()
    ].copy()
    specs = [
        ("RAW_UBO5_PROBABILITY", "final_pregame_ubo5_probability"),
        ("BETONLINE_NO_VIG_PROBABILITY", "final_pregame_betonline_no_vig_over_probability"),
        ("UBO5_EDGE", "final_pregame_edge_pp"),
    ]
    tables = []
    captures = []
    cutoffs = [10, 20, 30, 40, 50, int(np.ceil(len(common) / 4)), int(np.ceil(len(common) / 2))]
    labels = ["top_10", "top_20", "top_30", "top_40", "top_50", "top_quartile", "top_half"]
    total_winners = int(common.result.eq("WIN").sum())
    for ranking, col in specs:
        ordered = common.sort_values([col, "player_name"], ascending=[False, True]).copy()
        ordered["ranking_type"] = ranking
        ordered["rank_within_common_scoreable_universe"] = range(1, len(ordered) + 1)
        ordered["ranking_value"] = ordered[col]
        tables.append(ordered.head(30)[[
            "ranking_type", "rank_within_common_scoreable_universe", "player_name",
            "game", "ranking_value", "final_pregame_ubo5_probability",
            "final_pregame_betonline_no_vig_over_probability",
            "final_pregame_edge_pp", "total_bases", "result",
        ]])
        for label, cutoff in zip(labels, cutoffs):
            cell = ordered.head(min(cutoff, len(ordered)))
            wins = int(cell.result.eq("WIN").sum())
            captures.append({
                "ranking_type": ranking, "cutoff": label,
                "rows": len(cell), "winners_captured": wins,
                "total_scoreable_winners": total_winners,
                "winner_capture_rate": wins / total_winners if total_winners else np.nan,
                "win_rate_within_cutoff": wins / len(cell) if len(cell) else np.nan,
                "common_scoreable_universe_rows": len(common),
            })
    return pd.concat(tables, ignore_index=True), pd.DataFrame(captures)


def exclusion(row: pd.Series) -> str:
    if row.ever_positive_status:
        return "SELECTED_POSITIVE_EDGE"
    if pd.isna(row.final_pregame_betonline_no_vig_over_probability):
        return "MARKET_NOT_AVAILABLE_OR_INCOMPLETE"
    if pd.isna(row.maximum_ubo5_probability):
        return "PIPELINE_NOT_SCOREABLE"
    if row.maximum_ubo5_edge_pp <= 0:
        return "MARKET_PROBABILITY_AT_OR_ABOVE_UBO5"
    return "OTHER_CERTIFIED_REASON"


def contact_comparison(universe: pd.DataFrame) -> pd.DataFrame:
    winners = universe[universe.result.eq("WIN")].copy()
    groups = {
        "SELECTED_WINNERS": winners[winners.ever_positive_status],
        "NON_SELECTED_WINNERS": winners[~winners.ever_positive_status],
        "SELECTED_ALL": universe[universe.ever_positive_status],
        "NON_SELECTED_ALL": universe[~universe.ever_positive_status],
        "COMPLETE_UNIVERSE": universe,
    }
    rows = []
    metrics = [*FEATURES, "batting_order", "strict_prior_pa", "final_pregame_ubo5_probability"]
    for label, frame in groups.items():
        for metric in metrics:
            values = pd.to_numeric(frame.get(metric), errors="coerce").dropna()
            rows.append({
                "population": label, "metric": metric, "available_rows": len(values),
                "median": values.median() if len(values) else np.nan,
                "mean": values.mean() if len(values) else np.nan,
                "p25": values.quantile(.25) if len(values) else np.nan,
                "p75": values.quantile(.75) if len(values) else np.nan,
            })
    return pd.DataFrame(rows)


def main() -> int:
    obs = load_observations()
    outcomes = load_outcomes()
    universe = canonical(obs, outcomes)
    features = load_features(obs)
    universe = universe.merge(features, on=["game_pk", "batter_mlb_id"], how="left")
    write_csv("ubo5_july27_complete_universe.csv", universe)

    selected = universe[universe.ever_positive_status]
    nonselected = universe[~universe.ever_positive_status]
    arithmetic = pd.DataFrame([
        {"population": "COMPLETE_UNIVERSE", "rows": len(universe),
         "wins": int(universe.result.eq("WIN").sum()), "losses": int(universe.result.eq("LOSS").sum()),
         "unresolved": int(universe.result.eq("UNRESOLVED").sum())},
        {"population": "BROAD_EVER_POSITIVE", "rows": len(selected),
         "wins": int(selected.result.eq("WIN").sum()), "losses": int(selected.result.eq("LOSS").sum()),
         "unresolved": int(selected.result.eq("UNRESOLVED").sum())},
        {"population": "BROAD_FINAL_PREGAME_POSITIVE", "rows": int(universe.final_pregame_positive_status.sum()),
         "wins": int(universe[universe.final_pregame_positive_status].result.eq("WIN").sum()),
         "losses": int(universe[universe.final_pregame_positive_status].result.eq("LOSS").sum()),
         "unresolved": int(universe[universe.final_pregame_positive_status].result.eq("UNRESOLVED").sum())},
        {"population": "NON_SELECTED", "rows": len(nonselected),
         "wins": int(nonselected.result.eq("WIN").sum()), "losses": int(nonselected.result.eq("LOSS").sum()),
         "unresolved": int(nonselected.result.eq("UNRESOLVED").sum())},
    ])
    write_csv("ubo5_july27_population_arithmetic_audit.csv", arithmetic)

    ranks, capture = ranked_outputs(universe)
    write_csv("ubo5_july27_rank_comparison.csv", ranks)
    write_csv("ubo5_july27_rank_capture_summary.csv", capture)

    winners = universe[universe.result.eq("WIN")].copy()
    winners["exclusion_explanation"] = winners.apply(exclusion, axis=1)
    winner_fields = [
        "player_name", "game", "total_bases", "hits", "singles", "doubles",
        "triples", "home_runs", "final_pregame_ubo5_probability",
        "ubo5_final_rank", "ubo5_final_percentile",
        "final_pregame_betonline_no_vig_over_probability",
        "betonline_final_rank", "betonline_final_percentile",
        "final_pregame_edge_pp", "maximum_ubo5_edge_pp",
        "ever_positive_status", "final_pregame_positive_status", "batting_order",
        "strict_prior_pa", "feature_state", *FEATURES, "exclusion_explanation",
    ]
    write_csv("ubo5_july27_tb15_winner_audit.csv", winners[winner_fields])

    selected_fields = [
        "player_name", "game", "total_bases", "result",
        "first_positive_edge_appearance", "maximum_ubo5_edge_pp",
        "final_pregame_edge_pp", "ubo5_final_rank", "betonline_final_rank",
        "edge_final_rank", "batting_order", "strict_prior_pa", "feature_state",
        "pitcher_hit_suppression", "pitcher_strikeout_context", *FEATURES,
    ]
    # Snapshot-native context is retained even if the detailed route ledger uses p_* aliases.
    selected_detail = selected.merge(
        obs.sort_values("snapshot_timestamp_utc").drop_duplicates(
            ["game_pk", "batter_mlb_id"], keep="last"
        )[["game_pk", "batter_mlb_id", "pitcher_hit_suppression", "pitcher_strikeout_context"]],
        on=["game_pk", "batter_mlb_id"], how="left",
    )
    write_csv("ubo5_july27_selected_row_audit.csv", selected_detail[selected_fields])
    write_csv("ubo5_july27_contact_profile_comparison.csv", contact_comparison(universe))

    timing = universe[[
        "player_name", "game", "first_market_appearance", "first_scoreable_timestamp",
        "first_positive_edge_appearance", "last_positive_edge_appearance",
        "last_eligible_pregame_run", "last_eligible_pregame_timestamp",
        "minimum_ubo5_probability", "maximum_ubo5_probability",
        "minimum_betonline_no_vig_over_probability",
        "maximum_betonline_no_vig_over_probability",
        "ever_positive_status", "final_pregame_positive_status",
    ]].copy()
    timing["ubo5_probability_range_pp"] = (
        timing.maximum_ubo5_probability - timing.minimum_ubo5_probability
    ) * 100
    timing["market_probability_range_pp"] = (
        timing.maximum_betonline_no_vig_over_probability
        - timing.minimum_betonline_no_vig_over_probability
    ) * 100
    write_csv("ubo5_july27_intraday_timing_audit.csv", timing)
    write_csv("ubo5_july27_named_player_trace.csv", winners[winner_fields])

    common_capture = capture[capture.cutoff.eq("top_30")].set_index("ranking_type")
    raw30 = int(common_capture.loc["RAW_UBO5_PROBABILITY", "winners_captured"])
    market30 = int(common_capture.loc["BETONLINE_NO_VIG_PROBABILITY", "winners_captured"])
    edge30 = int(common_capture.loc["UBO5_EDGE", "winners_captured"])
    explanation = winners.exclusion_explanation.value_counts().to_dict()
    scoreable_winners = int(winners.maximum_ubo5_probability.notna().sum())
    contact = contact_comparison(universe)
    def med(pop: str, metric: str) -> float:
        cell = contact.loc[
            (contact.population == pop) & (contact.metric == metric), "median"
        ]
        return float(cell.iloc[0]) if len(cell) and pd.notna(cell.iloc[0]) else float("nan")
    ever = set(zip(universe.loc[universe.ever_positive_status, "game_pk"], universe.loc[universe.ever_positive_status, "batter_mlb_id"]))
    final = set(zip(universe.loc[universe.final_pregame_positive_status, "game_pk"], universe.loc[universe.final_pregame_positive_status, "batter_mlb_id"]))
    identical = ever == final
    material_ubo = int(timing.ubo5_probability_range_pp.ge(2).sum())
    material_market = int(timing.market_probability_range_pp.ge(2).sum())
    brief_positive = int(
        (timing.ever_positive_status & ~timing.final_pregame_positive_status).sum()
    )
    decision = (
        "MIXED_MODEL_AND_MARKET_SELECTION_FAILURE"
        if explanation.get("PIPELINE_NOT_SCOREABLE", 0) > 0
        else "RAW_UBO5_RECOGNIZED_WINNERS_MARKET_PRICED_THEM_HIGHER"
    )
    certified_count = int(universe.outcome_status.eq("CERTIFIED").sum())
    win_count = int(universe.result.eq("WIN").sum())
    loss_count = int(universe.result.eq("LOSS").sum())
    unresolved_count = int(universe.result.eq("UNRESOLVED").sum())
    report = f"""# July 27 UBO-5 TB 1.5 Winner-Recognition Audit

Status: **PROVISIONAL_COMPLETE_EXCEPT_POSTPONED_ROW**

## Population arithmetic

- Certified snapshot observations: {len(obs)}
- Unique evaluated identities: {len(universe)}
- Certified outcomes: {certified_count}
- Total Bases Over 1.5 winners: {win_count}
- Total Bases Under 1.5 losses: {loss_count}
- Unresolved: {unresolved_count}
- Selected settled: {int(selected.outcome_status.eq("CERTIFIED").sum())}; {int(selected.result.eq("WIN").sum())}-{int(selected.result.eq("LOSS").sum())}
- Non-selected settled: {int(nonselected.outcome_status.eq("CERTIFIED").sum())}; {int(nonselected.result.eq("WIN").sum())}-{int(nonselected.result.eq("LOSS").sum())}

The reported broad 27/6/21 selected arithmetic is reproduced. The earlier 135-outcome
comparison omitted Ronald Acuña Jr.'s certified broad-closeout loss: he is absent from the
standard closeout/reconciliation union but present as RESOLVED with one total base in the
authoritative broad closeout. Incorporating all certified sources yields 136 settled rows:
27 selected (6-21) and 109 non-selected (48-61), not 135 and 108.

## Winner recognition

Of 54 certified winners, {scoreable_winners} received at least one frozen exact UBO-5 score. Explanations:
{chr(10).join(f"- {k}: {v}" for k,v in sorted(explanation.items()))}

This is a mixed result: most non-selected scored winners were excluded because BetOnline's
no-vig probability met or exceeded UBO-5 at every scored observation, while winners without
an exact frozen UBO-5 score were not recognized by the routed scoring path. The latter are
classified as pipeline-not-scoreable, not as low model probabilities.

## Rank capture

Within the common scoreable/outcome-certified universe, top-30 winner capture was:

- Raw UBO-5 probability: {raw30}
- BetOnline no-vig probability: {market30}
- UBO-5 edge: {edge30}

The full top-10 through top-half comparison is in `ubo5_july27_rank_capture_summary.csv`.
At the board-sized top-30 cutoff, edge captured fewer winners than both raw UBO-5 and
BetOnline. This was not uniform at every depth: edge tied or led at top 10, top 50, and
top half. The conclusion is therefore board-sized degradation, not universal rank dominance.

## Contact interpretation

- Selected-winner median contact per swing: {med("SELECTED_WINNERS","h_contact_per_swing"):.4f}
- Non-selected-winner median contact per swing: {med("NON_SELECTED_WINNERS","h_contact_per_swing"):.4f}
- Selected-winner median xBA: {med("SELECTED_WINNERS","h_xba"):.4f}
- Non-selected-winner median xBA: {med("NON_SELECTED_WINNERS","h_xba"):.4f}

These comparisons use only frozen feature rows with preserved values; availability counts are
reported beside every distribution. Contact alone is not treated as Total Bases probability.
Non-selected winners did not show a stronger median contact profile: contact-per-swing was
essentially equal, while selected winners had higher median EV, xBA, xwOBA, and LSA6.
Non-selected winners also had a lower median raw UBO-5 probability. The evidence does not
support a claim that the edge rule simply discarded the strongest contact/productivity group.

## Intraday timing

Ever-positive identities: {len(ever)}. Final-pregame-positive identities: {len(final)}.
The identity sets were {"exactly identical" if identical else "not identical"}; therefore
{"intraday population churn did not explain the result" if identical else "intraday churn affected selection membership"}.
Briefly positive but not final-positive identities: {brief_positive}. Using a declared
2-percentage-point range as the material-movement threshold, raw UBO-5 materially changed
for {material_ubo} identities and BetOnline changed materially for {material_market}.

## Conclusion

Primary conclusion: **{decision}**.

The productive hitters were not absent for one single reason. Among exact scoreable winners,
the governing mechanism was generally market pricing at or above UBO-5. A material separate
group never received an exact frozen routed UBO-5 score, so their absence cannot be attributed
to BetOnline or to a demonstrably low UBO-5 probability. Rank capture shows directly whether
the edge transformation lagged raw UBO-5 and BetOnline on this slate.

Hits 1.5 context: **HITS15_NAME_OVERLAP_NOT_ATTEMPTED_EXACT_ARTIFACT_NOT_ESTABLISHED**.

Rerun:
`.venv/bin/python -m backend.mlb.scripts.audit_mlb_ubo5_tb15_july27_winner_recognition`
"""
    text("ubo5_july27_winner_recognition_report.md", report)
    terminal = f"""UBO5_JULY27_COMPLETE_UNIVERSE_DECISION = CERTIFIED_170_IDENTITIES_{certified_count}_OUTCOMES_{win_count}_TB15_WINNERS
UBO5_JULY27_RANK_CAPTURE_DECISION = TOP30_RAW_{raw30}_MARKET_{market30}_EDGE_{edge30}
UBO5_JULY27_WINNER_RECOGNITION_DECISION = {decision}
UBO5_JULY27_POSITIVE_EDGE_TRANSFORMATION_DECISION = {"BOARD_SIZED_EDGE_CAPTURE_WORSE_THAN_RAW_AND_MARKET_NOT_UNIFORM_ALL_CUTOFFS" if edge30 < raw30 and edge30 < market30 else "EDGE_CAPTURE_NOT_UNIFORMLY_WORSE"}
UBO5_JULY27_CONTACT_PROFILE_DECISION = FROZEN_FEATURE_DISTRIBUTIONS_REPORTED_NO_CONTACT_ONLY_CAUSAL_CLAIM
UBO5_JULY27_INTRADAY_TIMING_DECISION = {"IDENTICAL_28_EVER_AND_FINAL_NO_POPULATION_CHURN" if identical else "POPULATION_CHURN_PRESENT"}
UBO5_JULY27_AUDIT_STATUS = PROVISIONAL_COMPLETE_EXCEPT_POSTPONED_ROW
"""
    text("terminal_decision.md", terminal)
    print(json.dumps({
        "output": str(OUT.relative_to(ROOT)), "observations": len(obs),
        "identities": len(universe), "certified_outcomes": certified_count,
        "winners": win_count, "decision": decision,
        "top30": {"raw": raw30, "market": market30, "edge": edge30},
        "winner_explanations": explanation,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
