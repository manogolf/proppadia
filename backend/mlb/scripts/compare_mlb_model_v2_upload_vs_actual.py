#!/usr/bin/env python3
"""Compare model-v2 ranking upload rows to actual graded v2 wagers."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.mlb.shared.team_name_map import teamIdMap


MARKET_TO_PROP = {
    "batter_hits": "hits",
    "batter_runs": "runs_scored",
    "batter_rbis": "rbis",
    "batter_bases": "total_bases",
    "batter_total_bases": "total_bases",
    "batter_h+r+rbi": "hits_runs_rbis",
    "batter_hits_runs_rbis": "hits_runs_rbis",
    "pitcher_hits": "hits_allowed",
    "pitcher_hits_allowed": "hits_allowed",
}

PROP_TO_MARKET = {v: k for k, v in MARKET_TO_PROP.items()}
PROP_TO_BET_LABEL = {
    "hits": "Hits",
    "hits_allowed": "Hits Allowed",
    "total_bases": "Total Bases",
    "runs_scored": "Runs",
    "rbis": "RBIs",
}

UPLOAD_TO_CANON_TEAM = {
    "AZ": "ARI",
    "ARI": "ARI",
    "ATH": "OAK",
    "OAK": "OAK",
    "CWS": "CWS",
    "CHW": "CWS",
    "SF": "SF",
    "SFG": "SF",
    "SD": "SD",
    "SDP": "SD",
    "KC": "KC",
    "KCR": "KC",
    "TB": "TB",
    "TBR": "TB",
    "WAS": "WSH",
    "WSH": "WSH",
}

BET_RE = re.compile(
    r"^(?P<player>.+?)\s+"
    r"(?P<label>Hits Allowed|Hits|Total Bases|Runs|RBIs?)\s+"
    r"(?P<side>Over|Under)\s+"
    r"(?P<line>-?\d+(?:\.\d+)?)$",
    re.IGNORECASE,
)


def _norm_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return str(value).strip()


def _norm_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _norm_text(value).lower()).strip()


def _norm_key(value: Any) -> str:
    return _norm_text(value).lower().strip()


def _norm_side(value: Any) -> str:
    side = _norm_key(value)
    if side in {"o", "over"}:
        return "over"
    if side in {"u", "under"}:
        return "under"
    return side


def _date_key(value: Any) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""
    if raw.isdigit() and len(raw) == 8:
        dt = pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(raw, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _to_id_text(value: Any) -> str:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return ""
        return str(int(float(value)))
    except Exception:
        return _norm_text(value)


def _line(value: Any) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return np.nan
    return round(float(parsed), 4)


def _to_float(value: Any) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _norm_team(value: Any) -> str:
    raw = _norm_text(value).upper()
    return UPLOAD_TO_CANON_TEAM.get(raw, raw)


def _team_name_reverse() -> dict[str, str]:
    out: dict[str, str] = {}
    for info in teamIdMap.values():
        abbr = _norm_team(info.get("abbr"))
        full = _norm_name(info.get("fullName"))
        if full and abbr:
            out[full] = abbr
    out[_norm_name("Athletics")] = "OAK"
    out[_norm_name("Arizona Diamondbacks")] = "ARI"
    out[_norm_name("Washington Nationals")] = "WSH"
    out[_norm_name("Chicago White Sox")] = "CWS"
    out[_norm_name("Kansas City Royals")] = "KC"
    out[_norm_name("San Diego Padres")] = "SD"
    out[_norm_name("San Francisco Giants")] = "SF"
    out[_norm_name("Tampa Bay Rays")] = "TB"
    return out


def _market_to_prop(value: Any) -> str:
    market = _norm_key(value)
    return MARKET_TO_PROP.get(market, market)


def _prop_from_bet_label(value: Any) -> str:
    label = _norm_key(value)
    if label == "hits":
        return "hits"
    if label == "hits allowed":
        return "hits_allowed"
    if label == "total bases":
        return "total_bases"
    if label == "runs":
        return "runs_scored"
    if label in {"rbi", "rbis"}:
        return "rbis"
    return label.replace(" ", "_")


def _result_from_grade(value: Any) -> str:
    grade = _norm_key(value)
    if grade == "win":
        return "win"
    if grade == "loss":
        return "loss"
    if grade in {"push", "void"}:
        return "push"
    return "unresolved"


def _american_profit(price: Any, won: bool) -> float | None:
    px = _to_float(price)
    if px is None:
        return None
    if not won:
        return -1.0
    if px > 0:
        return px / 100.0
    if px < 0:
        return 100.0 / abs(px)
    return None


def _load_upload(path: Path, date_value: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    out = df.copy()
    out["date_norm"] = out["DATE"].map(_date_key)
    out = out[out["date_norm"].eq(date_value)].copy()
    out["player_id_norm"] = out["SELECTOR"].map(_to_id_text)
    out["prop_type_norm"] = out["MARKET"].map(_market_to_prop)
    out["line_norm"] = out["POINT"].map(_line)
    out["side_norm"] = out["SIDE"].map(_norm_side)
    out["home_norm"] = out["HOME"].map(_norm_team)
    out["away_norm"] = out["AWAY"].map(_norm_team)
    out["upload_win_prob"] = pd.to_numeric(out.get("WIN %"), errors="coerce")
    out["upload_row_id"] = [f"upload_{i}" for i in range(len(out))]
    return out


def _prepare_reconcile(path: Path) -> pd.DataFrame:
    rec = pd.read_csv(path, low_memory=False)
    rec = rec.copy()
    rec["date_norm"] = rec["game_date"].map(_date_key)
    rec["player_id_norm"] = rec["player_id"].map(_to_id_text)
    rec["player_name_norm"] = rec["player_name"].map(_norm_name)
    rec["prop_type_norm"] = rec["prop_type"].map(_norm_key)
    rec["line_norm"] = rec["line"].map(_line)
    rec["home_norm"] = rec["home_team_code"].map(_norm_team)
    rec["away_norm"] = rec["away_team_code"].map(_norm_team)
    return rec


def _attach_upload_outcomes(upload: pd.DataFrame, rec: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    grouped = {
        key: group
        for key, group in rec.groupby(["date_norm", "player_id_norm", "prop_type_norm", "line_norm"], dropna=False)
    }
    for _, row in upload.iterrows():
        side = row["side_norm"]
        matches = grouped.get((row["date_norm"], row["player_id_norm"], row["prop_type_norm"], row["line_norm"]), pd.DataFrame())
        if matches.empty:
            enriched = row.to_dict()
            enriched.update({"upload_result": "missing", "upload_pnl_1u": np.nan, "price_uploaded_or_reconcile": np.nan, "player_name": ""})
            rows.append(enriched)
            continue
        price_col = "price_over_american" if side == "over" else "price_under_american"
        result_col = "actual_over_outcome" if side == "over" else "actual_under_outcome"
        pnl_col = "pnl_over_1u" if side == "over" else "pnl_under_1u"
        price_sort = pd.to_numeric(matches[price_col], errors="coerce").fillna(-999999)
        match = matches.loc[int(price_sort.idxmax())]
        result = _norm_key(match.get(result_col, ""))
        pnl = _to_float(match.get(pnl_col))
        if pnl is None and result in {"win", "loss"}:
            pnl = _american_profit(match.get(price_col), result == "win")
        enriched = row.to_dict()
        enriched.update(
            {
                "upload_result": result if result else "unresolved",
                "upload_pnl_1u": pnl,
                "price_uploaded_or_reconcile": match.get(price_col, np.nan),
                "player_name": match.get("player_name", ""),
                "reconcile_bookmaker_key": match.get("bookmaker_key", ""),
                "actual_value": match.get("actual_value", np.nan),
            }
        )
        rows.append(enriched)
    return pd.DataFrame(rows)


def _load_actual(path: Path, date_value: str, rec: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_csv(path)
    notes = df.get("Notes", pd.Series("", index=df.index)).map(_norm_key)
    v2 = df[notes.eq("v2")].copy()
    v2["date_norm"] = v2["Event Date"].map(_date_key)
    v2 = v2[v2["date_norm"].eq(date_value)].copy()
    team_rev = _team_name_reverse()
    v2["home_norm"] = v2["Home"].map(lambda x: team_rev.get(_norm_name(x), _norm_team(x)))
    v2["away_norm"] = v2["Away"].map(lambda x: team_rev.get(_norm_name(x), _norm_team(x)))
    parsed = v2["Bet"].map(_parse_bet)
    parsed_df = pd.DataFrame(list(parsed), index=v2.index)
    v2 = pd.concat([v2, parsed_df], axis=1)
    v2["actual_wager_result"] = v2["Grade"].map(_result_from_grade)
    v2["actual_pnl"] = pd.to_numeric(v2.get("$ W/L"), errors="coerce")
    v2["actual_amount"] = pd.to_numeric(v2.get("Amount"), errors="coerce")
    v2["actual_pnl_units"] = v2["actual_pnl"] / v2["actual_amount"].replace(0, np.nan)
    v2["price_actually_bet"] = pd.to_numeric(v2.get("Odds"), errors="coerce")
    v2["actual_row_id"] = [f"actual_{i}" for i in range(len(v2))]
    v2 = _attach_actual_player_ids(v2, rec)
    return v2


def _parse_bet(value: Any) -> dict[str, Any]:
    text = _norm_text(value)
    match = BET_RE.match(text)
    if not match:
        return {
            "parsed_player_name": "",
            "parsed_player_name_norm": "",
            "prop_type_norm": "",
            "side_norm": "",
            "line_norm": np.nan,
            "parse_ok": False,
        }
    player = match.group("player")
    return {
        "parsed_player_name": player,
        "parsed_player_name_norm": _norm_name(player),
        "prop_type_norm": _prop_from_bet_label(match.group("label")),
        "side_norm": _norm_side(match.group("side")),
        "line_norm": _line(match.group("line")),
        "parse_ok": True,
    }


def _attach_actual_player_ids(actual: pd.DataFrame, rec: pd.DataFrame) -> pd.DataFrame:
    out = actual.copy()
    lookup_cols = ["date_norm", "player_name_norm", "prop_type_norm", "line_norm"]
    rec_lookup = rec.drop_duplicates(lookup_cols)[lookup_cols + ["player_id_norm", "player_name"]].copy()
    merged = out.merge(
        rec_lookup,
        how="left",
        left_on=["date_norm", "parsed_player_name_norm", "prop_type_norm", "line_norm"],
        right_on=lookup_cols,
        suffixes=("", "_rec"),
    )
    merged["player_id_norm"] = merged["player_id_norm"].fillna("")
    return merged


def _key(df: pd.DataFrame) -> pd.Series:
    return (
        df["date_norm"].astype(str)
        + "|"
        + df["player_id_norm"].astype(str)
        + "|"
        + df["prop_type_norm"].astype(str)
        + "|"
        + df["line_norm"].astype(str)
        + "|"
        + df["side_norm"].astype(str)
    )


def _build_comparison(upload: pd.DataFrame, actual: pd.DataFrame) -> pd.DataFrame:
    upload = upload.copy()
    actual = actual.copy()
    upload["common_key"] = _key(upload)
    actual["common_key"] = _key(actual)
    actual_by_key = actual.sort_values("actual_row_id").drop_duplicates("common_key")
    upload_by_key = upload.sort_values("upload_row_id").drop_duplicates("common_key")

    left = upload.merge(
        actual_by_key[
            [
                "common_key",
                "actual_row_id",
                "Wager ID",
                "Bet",
                "Book",
                "actual_wager_result",
                "actual_pnl",
                "actual_pnl_units",
                "price_actually_bet",
                "actual_amount",
            ]
        ],
        on="common_key",
        how="left",
    )
    left["in_upload"] = True
    left["in_actual_graded_v2"] = left["actual_row_id"].notna()
    left["group"] = np.where(left["in_actual_graded_v2"], "upload_intersection_actual", "upload_only")

    actual_only = actual[~actual["common_key"].isin(set(upload["common_key"]))].copy()
    if not actual_only.empty:
        actual_only = actual_only.merge(
            upload_by_key[["common_key", "upload_row_id"]],
            on="common_key",
            how="left",
        )
        actual_only["in_upload"] = False
        actual_only["in_actual_graded_v2"] = True
        actual_only["group"] = "actual_only"
        for col in left.columns:
            if col not in actual_only.columns:
                actual_only[col] = np.nan
        actual_only["player_name"] = actual_only.get("player_name", actual_only["parsed_player_name"])
        actual_only["upload_result"] = np.nan
        actual_only["upload_pnl_1u"] = np.nan
        actual_only["price_uploaded_or_reconcile"] = np.nan
        actual_only["upload_win_prob"] = np.nan
        left = pd.concat([left, actual_only[left.columns]], ignore_index=True)

    left["side_line_player_mismatch"] = False
    upload_player_prop = set(zip(upload["player_id_norm"], upload["prop_type_norm"]))
    for idx, row in left[left["group"].eq("actual_only")].iterrows():
        left.loc[idx, "side_line_player_mismatch"] = (row["player_id_norm"], row["prop_type_norm"]) in upload_player_prop
    return left


def _summary_for(group: pd.DataFrame, result_col: str, pnl_col: str) -> dict[str, Any]:
    resolved = group[group[result_col].isin(["win", "loss", "push"])].copy()
    wins = int((resolved[result_col] == "win").sum())
    losses = int((resolved[result_col] == "loss").sum())
    pushes = int((resolved[result_col] == "push").sum())
    profit = float(pd.to_numeric(resolved[pnl_col], errors="coerce").fillna(0).sum())
    risk = wins + losses + pushes
    return {
        "count": int(len(group)),
        "resolved_count": int(len(resolved)),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate": wins / (wins + losses) if wins + losses else None,
        "roi": profit / risk if risk else None,
        "units": profit,
    }


def _build_summary(comp: pd.DataFrame, upload: pd.DataFrame, actual: pd.DataFrame, date_value: str) -> dict[str, Any]:
    groups = {}
    for name, group in comp.groupby("group", dropna=False):
        if name == "actual_only":
            groups[str(name)] = _summary_for(group, "actual_wager_result", "actual_pnl_units")
        else:
            groups[str(name)] = _summary_for(group, "upload_result", "upload_pnl_1u")
    upload_resolved = upload[upload["upload_result"].isin(["win", "loss", "push"])]
    actual_resolved = actual[actual["actual_wager_result"].isin(["win", "loss", "push"])]
    upload_only = comp[comp["group"].eq("upload_only")]
    actual_only = comp[comp["group"].eq("actual_only")]
    upload_only_wins = int((upload_only["upload_result"] == "win").sum())
    upload_only_losses = int((upload_only["upload_result"] == "loss").sum())
    actual_only_losses = int((actual_only["actual_wager_result"] == "loss").sum())
    mismatch_count = int(comp.get("side_line_player_mismatch", pd.Series(False, index=comp.index)).sum())
    return {
        "date": date_value,
        "upload_rows": int(len(upload)),
        "upload_resolved_record": {
            "wins": int((upload_resolved["upload_result"] == "win").sum()),
            "losses": int((upload_resolved["upload_result"] == "loss").sum()),
            "pushes": int((upload_resolved["upload_result"] == "push").sum()),
        },
        "actual_v2_rows": int(len(actual)),
        "actual_v2_resolved_record": {
            "wins": int((actual_resolved["actual_wager_result"] == "win").sum()),
            "losses": int((actual_resolved["actual_wager_result"] == "loss").sum()),
            "pushes": int((actual_resolved["actual_wager_result"] == "push").sum()),
            "unresolved": int((~actual["actual_wager_result"].isin(["win", "loss", "push"])).sum()),
        },
        "groups": groups,
        "uploaded_wins_not_actually_bet": upload_only_wins,
        "uploaded_losses_not_actually_bet": upload_only_losses,
        "actual_v2_losses_not_in_ranking_upload": actual_only_losses,
        "side_line_player_mismatch_count": mismatch_count,
        "explanation": _explain(upload_only_wins, upload_only_losses, actual_only_losses, mismatch_count),
    }


def _explain(upload_only_wins: int, upload_only_losses: int, actual_only_losses: int, mismatch_count: int) -> str:
    parts = []
    if upload_only_wins or upload_only_losses:
        parts.append(
            f"ranking upload had {upload_only_wins} wins and {upload_only_losses} losses that were not actually bet"
        )
    if actual_only_losses:
        parts.append(f"actual v2 wagers included {actual_only_losses} losses that were not in the ranking upload")
    if mismatch_count:
        parts.append(f"{mismatch_count} actual-only rows share player/prop with upload but differ by side or line")
    if not parts:
        return "No upload-vs-actual population gap found; investigate grading or key normalization."
    return "; ".join(parts) + "."


def _write_md(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        f"# V2 Upload Vs Actual: {summary['date']}",
        "",
        "## Headline",
        f"- Ranking upload resolved record: `{summary['upload_resolved_record']['wins']}-{summary['upload_resolved_record']['losses']}`",
        f"- Actual v2 resolved record: `{summary['actual_v2_resolved_record']['wins']}-{summary['actual_v2_resolved_record']['losses']}`",
        f"- Actual unresolved v2 rows: `{summary['actual_v2_resolved_record']['unresolved']}`",
        "",
        "## Groups",
    ]
    for name, stats in summary["groups"].items():
        wr = "n/a" if stats["win_rate"] is None else f"{stats['win_rate']:.2%}"
        roi = "n/a" if stats["roi"] is None else f"{stats['roi']:.2%}"
        lines.append(
            f"- `{name}`: count `{stats['count']}`, resolved `{stats['resolved_count']}`, "
            f"record `{stats['wins']}-{stats['losses']}-{stats['pushes']}`, win rate `{wr}`, ROI `{roi}`, units `{stats['units']:.3f}`"
        )
    lines.extend(
        [
            "",
            "## Delta Checks",
            f"- Uploaded wins not actually bet: `{summary['uploaded_wins_not_actually_bet']}`",
            f"- Uploaded losses not actually bet: `{summary['uploaded_losses_not_actually_bet']}`",
            f"- Actual v2 losses not in ranking upload: `{summary['actual_v2_losses_not_in_ranking_upload']}`",
            f"- Side/line/player mismatch count: `{summary['side_line_player_mismatch_count']}`",
            "",
            "## Explanation",
            summary["explanation"],
        ]
    )
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--upload-csv", default="")
    parser.add_argument("--graded-csv", default="")
    parser.add_argument("--reconcile-csv", default="")
    parser.add_argument("--out-csv", default="")
    parser.add_argument("--summary-json", default="")
    parser.add_argument("--summary-md", default="")
    args = parser.parse_args()

    date_value = _date_key(args.date)
    if not date_value:
        raise SystemExit(f"Invalid --date: {args.date}")
    compact_under = date_value.replace("-", "_")

    if args.upload_csv:
        upload_csv = Path(args.upload_csv)
    else:
        dated_upload = Path(f"backend/mlb/exports/model_v2/upload/{date_value}/ranking_tool_upload_{date_value}.csv")
        legacy_upload = Path(f"backend/mlb/exports/model_v2/upload/ranking_tool_upload_{date_value}.csv")
        upload_csv = dated_upload if dated_upload.exists() else legacy_upload
    graded_csv = Path(args.graded_csv or f"/Users/jerrystrain/Downloads/8rainstation_daily_{compact_under}.csv")
    reconcile_csv = Path(args.reconcile_csv or f"artifacts/analysis/mlb/execution_vs_model/{date_value}/reconcile_rows.csv")
    out_csv = Path(args.out_csv or f"backend/mlb/exports/model_v2/reconcile/v2_upload_vs_actual_{date_value}.csv")
    summary_json = Path(args.summary_json or f"backend/mlb/exports/model_v2/reconcile/v2_upload_vs_actual_{date_value}_summary.json")
    summary_md = Path(args.summary_md or f"backend/mlb/exports/model_v2/reconcile/v2_upload_vs_actual_{date_value}_summary.md")

    for path in [upload_csv, graded_csv, reconcile_csv]:
        if not path.exists():
            raise SystemExit(f"Missing required input: {path}")

    rec = _prepare_reconcile(reconcile_csv)
    upload = _attach_upload_outcomes(_load_upload(upload_csv, date_value), rec)
    actual = _load_actual(graded_csv, date_value, rec)
    comp = _build_comparison(upload, actual)
    summary = _build_summary(comp, upload, actual, date_value)
    summary.update(
        {
            "upload_csv": str(upload_csv),
            "graded_csv": str(graded_csv),
            "reconcile_csv": str(reconcile_csv),
            "out_csv": str(out_csv),
        }
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    comp.to_csv(out_csv, index=False)
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    _write_md(summary_md, summary)

    print(f"Wrote {out_csv}")
    print(f"Wrote {summary_json}")
    print(f"Wrote {summary_md}")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
