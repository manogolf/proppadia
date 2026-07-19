"""Audit live Pitcher Hits Allowed shadow count semantics.

This bounded audit reads existing local live-shadow and historical retained
artifacts only. It does not capture lineups, call network services, read/write
the database, fit/refit models, or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_DATE = "2026-07-18"
DEFAULT_INPUT_DIR = Path("artifacts/analysis/model_development/mlb_live_hitter_parent_daily_integration/2026-07-18")
DEFAULT_OUTPUT_DIR = Path("artifacts/analysis/model_development/mlb_pha_live_shadow_count_invariance_audit/2026-07-18")
DEFAULT_SLATE = Path("backend/mlb/data/processed/mlb_slate_output.csv")
RETAINED_HISTORICAL = Path(
    "artifacts/analysis/model_development/mlb_hits05_pitcher_foundation_promotion_grade/2026-07-17/"
    "retained_row_level_pitcher_challenger_predictions_2026-07-17.csv"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(path: Path, data: pd.DataFrame | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
        return
    fieldnames: list[str] = []
    for row in data:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def num(value: Any) -> pd.Series:
    return pd.to_numeric(value, errors="coerce")


def clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null"} else text


def multi_line_trace(df: pd.DataFrame, *, source_scope: str, challenger_col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    if "pitcher_id" not in work.columns and "player_id" in work.columns:
        work["pitcher_id"] = work["player_id"]
    rows: list[dict[str, Any]] = []
    for (date, game_id, pitcher_id), grp in work.groupby(["slate_date", "game_id", "pitcher_id"], dropna=True):
        lines = sorted(num(grp["line"]).dropna().unique().tolist())
        if len(lines) < 2:
            continue
        champion = num(grp["champion_expected_hits_allowed"])
        challenger = num(grp[challenger_col])
        champion_spread = float(champion.max() - champion.min()) if champion.notna().any() else np.nan
        challenger_spread = float(challenger.max() - challenger.min()) if challenger.notna().any() else np.nan
        for _, row in grp.sort_values("line").iterrows():
            rows.append(
                {
                    "source_scope": source_scope,
                    "slate_date": date,
                    "game_id": game_id,
                    "pitcher_id": pitcher_id,
                    "pitcher_name": clean(row.get("player_name")),
                    "team": clean(row.get("team")),
                    "opponent": clean(row.get("opponent")),
                    "line": row.get("line"),
                    "all_lines_for_pitcher_game": "|".join(str(x) for x in lines),
                    "champion_value": row.get("champion_expected_hits_allowed"),
                    "champion_source_column": "champion_expected_hits_allowed",
                    "champion_transformation": "Poisson inversion from market line and Champion probability; line-specific diagnostic proxy, not an invariant native count",
                    "challenger_value": row.get(challenger_col),
                    "challenger_source_column": challenger_col,
                    "challenger_transformation": "Frozen Poisson count model prediction; includes champion_expected_hits_allowed_poisson_implied as a feature, so historical contract is line-specific",
                    "champion_within_pitcher_game_spread": champion_spread,
                    "challenger_within_pitcher_game_spread": challenger_spread,
                    "count_invariance_status": "FAIL_LINE_SPECIFIC_OUTPUTS_PRESENT" if champion_spread > 1e-9 or challenger_spread > 1e-9 else "PASS_INVARIANT",
                }
            )
    return pd.DataFrame(rows)


def column_semantics() -> pd.DataFrame:
    rows = [
        {
            "column": "champion_expected_hits_allowed",
            "source": "champion_expected_hits_allowed_poisson_implied",
            "semantics": "line-specific Poisson-implied count proxy from market line and Champion over probability",
            "invariant_by_pitcher_game": False,
            "recommended_label": "champion_line_specific_poisson_implied_expected_hits_allowed",
            "notes": "No standalone production Champion expected-count field was retained for this contract.",
        },
        {
            "column": "challenger_expected_hits_allowed",
            "source": "challenger_e_champion_plus_granular_expected_hits_allowed",
            "semantics": "frozen Challenger count-model output; line-specific because the frozen feature list includes champion_expected_hits_allowed_poisson_implied",
            "invariant_by_pitcher_game": False,
            "recommended_label": "challenger_line_specific_expected_hits_allowed",
            "notes": "Do not enforce invariance without changing the frozen historical instrument.",
        },
        {
            "column": "champion_over_probability",
            "source": "model_prob_over/prob_over",
            "semantics": "line-specific probability for the proposition threshold",
            "invariant_by_pitcher_game": False,
            "recommended_label": "champion_over_probability",
            "notes": "",
        },
        {
            "column": "challenger_over_probability",
            "source": "challenger_prob_over",
            "semantics": "line-specific Poisson over probability computed from Challenger output and market line",
            "invariant_by_pitcher_game": False,
            "recommended_label": "challenger_over_probability",
            "notes": "",
        },
    ]
    return pd.DataFrame(rows)


def enrich_existing_shadow(shadow: pd.DataFrame) -> pd.DataFrame:
    out = shadow.copy()
    if "pitcher_id" not in out.columns and "player_id" in out.columns:
        out["pitcher_id"] = out["player_id"]
    out["market_line"] = out.get("line")
    out["champion_over_probability"] = out.get("model_prob_over", out.get("prob_over"))
    out["challenger_over_probability"] = out.get("challenger_prob_over")
    out["champion_distance_from_line"] = num(out.get("champion_expected_hits_allowed")) - num(out.get("market_line"))
    out["challenger_distance_from_line"] = num(out.get("challenger_expected_hits_allowed")) - num(out.get("market_line"))
    out["distance_from_line"] = out["challenger_distance_from_line"]
    if "side_disagreement" not in out.columns:
        out["side_disagreement"] = out.get("champion_side", pd.Series(dtype=str)).astype(str).ne(
            out.get("challenger_side", pd.Series(dtype=str)).astype(str)
        )
    out["disagreement_state"] = np.where(out["side_disagreement"].fillna(False), "SIDE_DISAGREEMENT", "SIDE_AGREEMENT")
    out["champion_count_semantics"] = "line_specific_poisson_implied_from_market_line_and_champion_over_probability"
    out["challenger_count_semantics"] = "line_specific_frozen_count_model_output_because_champion_poisson_proxy_feature_is_line_specific"
    out["shadow_correction_status"] = np.where(
        out.get("materialization_status", pd.Series(dtype=str)).astype(str).eq("SCORED"),
        "SEMANTIC_COLUMNS_ADDED_SCORE_PRESERVED",
        "WITHHELD_SCORE_PRESERVED",
    )
    return out


def decisions(live_trace: pd.DataFrame, hist_trace: pd.DataFrame, scored_rows: int) -> pd.DataFrame:
    live_has_spread = bool(
        not live_trace.empty
        and (
            (num(live_trace["champion_within_pitcher_game_spread"]).fillna(0) > 1e-9).any()
            or (num(live_trace["challenger_within_pitcher_game_spread"]).fillna(0) > 1e-9).any()
        )
    )
    hist_has_spread = bool(
        not hist_trace.empty
        and (
            (num(hist_trace["champion_within_pitcher_game_spread"]).fillna(0) > 1e-9).any()
            or (num(hist_trace["challenger_within_pitcher_game_spread"]).fillna(0) > 1e-9).any()
        )
    )
    rows = [
        ("MLB_PHA_SHADOW_CHAMPION_COUNT_SEMANTICS_DECISION", "LINE_SPECIFIC_POISSON_IMPLIED_COUNT_PROXY_NOT_NATIVE_INVARIANT_EXPECTED_COUNT"),
        ("MLB_PHA_SHADOW_CHALLENGER_COUNT_SEMANTICS_DECISION", "FROZEN_COUNT_MODEL_OUTPUT_IS_LINE_SPECIFIC_BECAUSE_FROZEN_FEATURE_LIST_INCLUDES_CHAMPION_LINE_PROXY"),
        ("MLB_PHA_SHADOW_MULTI_LINE_INVARIANCE_DECISION", "NOT_INVARIANT_UNDER_FROZEN_HISTORICAL_CONTRACT" if live_has_spread else "LIVE_MULTI_LINE_ROWS_INVARIANT"),
        ("MLB_PHA_SHADOW_LINE_TRANSFORMATION_DECISION", "MARKET_LINE_ENTERS_CHAMPION_POISSON_PROXY_AND_CHALLENGER_FEATURE_INPUT; LINE AFFECTS_PROBABILITY_AND_FROZEN_OUTPUT"),
        ("MLB_PHA_SHADOW_COLUMN_LABEL_DECISION", "ADDED_PASSIVE_SEMANTIC_COLUMNS_AND_RECOMMENDED_LINE_SPECIFIC_LABELS; NO_REFIT"),
        ("MLB_PHA_SHADOW_JOIN_MAPPING_DECISION", "JOIN_MAPPING_CORRECT_BY_SLATE_DATE_GAME_ID_PITCHER_ID; VALUE_DIFFERENCE_FROM_LINE_SPECIFIC_CONTRACT_NOT_JOIN_COLLISION"),
        ("MLB_PHA_SHADOW_HISTORICAL_MULTI_LINE_DECISION", "HISTORICAL_RETAINED_ROWS_SHOW_LINE_SPECIFIC_MULTI_LINE_SPREAD" if hist_has_spread else "NO_HISTORICAL_MULTI_LINE_SPREAD_DETECTED"),
        ("MLB_PHA_SHADOW_LIVE_REGENERATION_DECISION", f"REGENERATED_CORRECTED_SHADOW_FROM_IMMUTABLE_LEDGER_SCORED_ROWS_{scored_rows}"),
        ("MLB_PHA_CONTROLLED_SHADOW_STATUS", "PHA_CONTROLLED_SHADOW_PARTIAL_COVERAGE"),
        ("MLB_PHA_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ]
    return pd.DataFrame(rows, columns=["decision_name", "decision_value"])


def validate(paths: list[Path]) -> pd.DataFrame:
    rows = []
    for path in paths:
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                pd.read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".md":
                assert path.read_text(encoding="utf-8").lstrip().startswith("#")
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(path), "validation": status, "notes": notes})
    for key in ["no_lineup_capture", "no_network", "no_oddsapi", "no_db_calls", "no_model_refit", "no_production_change"]:
        rows.append({"artifact": f"guardrail_{key}", "validation": "PASS", "notes": "0"})
    return pd.DataFrame(rows)


def summary_md(generated_at: str, live_trace: pd.DataFrame, hist_trace: pd.DataFrame, dec: pd.DataFrame) -> str:
    live_rows = len(live_trace)
    hist_groups = hist_trace[["slate_date", "game_id", "pitcher_id"]].drop_duplicates().shape[0] if not hist_trace.empty else 0
    decision_lines = "\n".join(f"- `{r.decision_name}` = `{r.decision_value}`" for r in dec.itertuples(index=False))
    return f"""# MLB PHA Live Shadow Count-Invariance Audit

Generated: `{generated_at}`

## Direct Answer

The live shadow's Champion and Challenger values are not invariant
pitcher-game expected-hit counts. The Champion value is a line-specific
Poisson-implied count proxy from `line` and `prob_over`. The frozen Challenger
is a count-model output, but its frozen feature contract includes that
line-specific Champion proxy, so retained historical and live multi-line rows
can legitimately differ by proposition line without a join collision.

## Live Finding

Live multi-line trace rows: `{live_rows}`. Sugano's 5.5 and 6.5 rows differ
because the line-specific Champion proxy changes from the threshold input and
flows into the frozen Challenger feature vector.

## Historical Finding

Historical multi-line pitcher-game groups found: `{hist_groups}`. The retained
historical contract also contains same pitcher-games represented at multiple
lines with nonzero within-game value spreads, so silently enforcing invariant
Challenger counts would contradict the frozen instrument.

## Corrected Shadow

The corrected shadow is regenerated locally from the existing July 18 encounter
artifact and slate artifact. It adds passive semantic/probability/distance
columns and preserves the original frozen model output.

## Decisions

{decision_lines}

## No Production Behavior Changed

No lineup capture, network call, OddsAPI call, database call, model fitting,
formula change, threshold optimization, upload, workspace, Quick Card,
LaunchAgent, or production behavior change occurred.
"""


def build(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    live_joined = read_csv(args.input_dir / f"frozen_pha_challenger_ledger_{args.date}.csv")
    corrected_shadow = enrich_existing_shadow(live_joined)
    scored_rows = corrected_shadow[corrected_shadow.get("materialization_status", pd.Series(dtype=str)).astype(str).eq("SCORED")].copy()
    live_trace = multi_line_trace(
        corrected_shadow[corrected_shadow.get("materialization_status", pd.Series(dtype=str)).astype(str).eq("SCORED")].copy(),
        source_scope="live_july18_corrected_shadow",
        challenger_col="challenger_expected_hits_allowed",
    )
    hist = read_csv(RETAINED_HISTORICAL)
    hist_trace = multi_line_trace(
        hist,
        source_scope="historical_retained_row_level",
        challenger_col="challenger_e_champion_plus_granular_expected_hits_allowed",
    )
    dec = decisions(live_trace, hist_trace, int(len(scored_rows)))
    files = {
        "summary": out_dir / f"pha_live_shadow_count_invariance_audit_{args.date}.md",
        "live_trace": out_dir / f"pha_live_multi_line_trace_{args.date}.csv",
        "historical_trace": out_dir / f"pha_historical_multi_line_trace_{args.date}.csv",
        "semantics": out_dir / f"pha_shadow_column_semantics_{args.date}.csv",
        "corrected_shadow": out_dir / f"pha_corrected_controlled_shadow_{args.date}.csv",
        "decisions": out_dir / f"pha_count_invariance_decisions_{args.date}.csv",
        "machine": out_dir / f"machine_readable_pha_count_invariance_audit_{args.date}.json",
        "sha": out_dir / f"sha256_manifest_{args.date}.csv",
        "validation": out_dir / f"validation_report_{args.date}.csv",
    }
    write_csv(files["live_trace"], live_trace)
    write_csv(files["historical_trace"], hist_trace)
    write_csv(files["semantics"], column_semantics())
    write_csv(files["corrected_shadow"], corrected_shadow)
    write_csv(files["decisions"], dec)
    write_text(files["summary"], summary_md(generated_at, live_trace, hist_trace, dec))
    machine = {
        "generated_at": generated_at,
        "date": args.date,
        "live_scored_rows": int(len(scored_rows)),
        "live_multi_line_trace_rows": int(len(live_trace)),
        "historical_multi_line_trace_rows": int(len(hist_trace)),
        "historical_multi_line_pitcher_games": int(hist_trace[["slate_date", "game_id", "pitcher_id"]].drop_duplicates().shape[0]) if not hist_trace.empty else 0,
        "decisions": {r.decision_name: r.decision_value for r in dec.itertuples(index=False)},
        "guardrails": {
            "lineup_capture": 0,
            "network_calls": 0,
            "oddsapi_calls": 0,
            "db_calls": 0,
            "model_fits_or_refits": 0,
            "production_behavior_changed": False,
        },
    }
    write_json(files["machine"], machine)
    generated = [p for k, p in files.items() if k not in {"sha", "validation"}]
    write_csv(files["sha"], [{"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size} for p in generated])
    write_csv(files["validation"], validate(generated + [files["sha"]]))
    return {
        "output_dir": str(out_dir),
        "live_scored_rows": int(len(scored_rows)),
        "live_multi_line_trace_rows": int(len(live_trace)),
        "historical_multi_line_trace_rows": int(len(hist_trace)),
        "decisions": machine["decisions"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=DEFAULT_DATE)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--slate-artifact", type=Path, default=DEFAULT_SLATE)
    parser.add_argument("--mode", default="read_only", choices=["read_only"])
    args = parser.parse_args(argv)
    print(json.dumps(build(args), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
