"""Evaluate retained prospective SOG production and D10/D20 blend predictions."""
from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from backend.nhl.reconciliation_grades import restated_grade_path

ROOT = Path(__file__).resolve().parents[3]
EXPORT = ROOT / "artifacts/analysis/nhl/all_candidate_evaluation_export/full_window_2026-10-17"
OUT = ROOT / "artifacts/analysis/nhl/sog_paired_blend_evaluation_full_window_2026-10-17"
DOC_REPORT = ROOT / "docs/nhl/2026_sog_paired_blend_evaluation_2026-10-17.md"
DOC_SUMMARY = ROOT / "docs/nhl/2026_sog_paired_blend_evaluation_2026-10-17.summary.json"
PRODUCTION = "baseline_v1"
BLEND25 = "NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1"
BLEND50 = "NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1"
ARMS = (PRODUCTION, BLEND25, BLEND50)
LABELS = {PRODUCTION: "production_d10", BLEND25: "blend_25_75", BLEND50: "blend_50_50"}
LINES = (1.5, 2.5, 3.5)
BOOK = "betonlineag"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def num(value: str) -> float | None:
    if value in (None, ""):
        return None
    x = float(value)
    return x if math.isfinite(x) else None


def close(a: str, b: str, tolerance: float = 1e-10) -> bool:
    x, y = num(a), num(b)
    return x is not None and y is not None and abs(x - y) <= tolerance


def write_csv(path: Path, rows: list[dict], columns: list[str] | None = None) -> None:
    if columns is None:
        columns = list(rows[0]) if rows else []
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def outcome_over(actual: float, line: float) -> int:
    return int(actual > line)


def outcome_selected(actual: float, line: float, side: str) -> int:
    over = outcome_over(actual, line)
    return int((side == "OVER" and over) or (side == "UNDER" and not over))


def paired_probability_delta(rows: list[dict], challenger_label: str) -> dict:
    """Return challenger-minus-production probability deltas on paired rows."""
    if not rows:
        raise ValueError("paired probability delta requires at least one row")
    brier_deltas = []
    log_loss_deltas = []
    disagreements = 0
    for row in rows:
        y = outcome_over(float(row["actual_sog"]), float(row["market_line"]))
        production_p = float(row["production_d10_probability_over"])
        challenger_p = float(row[f"{challenger_label}_probability_over"])
        brier_deltas.append((challenger_p - y) ** 2 - (production_p - y) ** 2)
        log_loss_deltas.append(logloss(y, challenger_p) - logloss(y, production_p))
        disagreements += row[f"{challenger_label}_selected_side"] != row["production_d10_selected_side"]
    return {
        "brier_delta_vs_production": sum(brier_deltas) / len(rows),
        "log_loss_delta_vs_production": sum(log_loss_deltas) / len(rows),
        "brier_improved_rows": sum(delta < -1e-15 for delta in brier_deltas),
        "brier_worsened_rows": sum(delta > 1e-15 for delta in brier_deltas),
        "selected_side_disagreements": disagreements,
    }


def logloss(y: int, p: float) -> float:
    p = min(max(p, 1e-15), 1 - 1e-15)
    return -(y * math.log(p) + (1 - y) * math.log(1 - p))


def american_profit(price: float) -> float:
    if price > 0:
        return price / 100.0
    if price < 0:
        return 100.0 / abs(price)
    raise ValueError("American price cannot be zero")


def source_path(value: str) -> Path:
    p = Path(value)
    return p if p.is_absolute() else ROOT / p


def qualified_quote_books(paired: list[dict], quote_rows: list[dict]) -> dict[str, set[str]]:
    """Validate selected-side quote chronology and return books by candidate."""
    candidates = {row[f"{label}_candidate_key"]: (row, label)
                  for row in paired for label in LABELS.values()}
    books: dict[str, set[str]] = defaultdict(set)
    seen = set()
    for quote in quote_rows:
        if quote["quote_qualification_status"] != "QUALIFIED_SOURCE_AND_OBSERVATION_PRECEDE_PREDICTION":
            continue
        found = candidates.get(quote["candidate_key"])
        if found is None:
            continue
        candidate, label = found
        if quote["quote_side"] != candidate[f"{label}_selected_side"]:
            raise ValueError(f"qualified quote side differs from model side: {quote['candidate_key']}")
        source_time = utc(quote["source_quote_timestamp_utc"])
        observation_time = utc(quote["observation_timestamp_utc"])
        prediction_time = utc(candidate[f"{label}_prediction_timestamp_utc"])
        start_time = utc(candidate["scheduled_start_utc"])
        if not source_time <= observation_time <= prediction_time < start_time:
            raise ValueError(f"qualified quote chronology failed: {quote['candidate_key']}")
        key = (quote["candidate_key"], quote["sportsbook_key"])
        if key in seen:
            raise ValueError(f"duplicate qualified candidate/book quote: {key}")
        seen.add(key)
        books[quote["candidate_key"]].add(quote["sportsbook_key"])
    return books


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    input_predictions = EXPORT / "candidate_predictions.csv"
    input_quotes = EXPORT / "candidate_quotes.csv"
    all_prediction_rows = [r for r in read_csv(input_predictions) if r["market_key"] == "shots_on_goal"]
    prediction_rows = [r for r in all_prediction_rows if r["model_version"] in ARMS]
    quote_rows = [r for r in read_csv(input_quotes)
                  if r["market_key"] == "shots_on_goal" and r["model_version"] in ARMS]
    by_arm_key: dict[tuple[str, str, str, str, str], dict] = {}
    source_files: dict[str, dict] = {}
    for r in prediction_rows:
        key = (r["slate_date"], r["game_id"], r["player_id"], r["market_line"], r["model_version"])
        if key in by_arm_key:
            raise ValueError(f"duplicate arm identity: {key}")
        if r["model_role"] != ("PRODUCTION" if r["model_version"] == PRODUCTION else "SHADOW"):
            raise ValueError(f"unexpected model role for {key}: {r['model_role']}")
        pred_time, start_time, cutoff = map(utc, (r["prediction_timestamp_utc"], r["scheduled_start_utc"], r["feature_cutoff_utc"]))
        if not cutoff <= pred_time < start_time:
            raise ValueError(f"prediction chronology failed: {key}")
        p = num(r["probability_over"])
        u = num(r["probability_under"])
        lam = num(r["expected_count"])
        if p is None or u is None or lam is None or not 0 <= p <= 1 or abs(p + u - 1) > 1e-8:
            raise ValueError(f"invalid probability/count for {key}")
        if r["selected_side"] != ("OVER" if p >= 0.5 else "UNDER"):
            raise ValueError(f"probability/side mismatch for {key}")
        src = source_path(r["source_path"])
        if not src.is_file():
            raise FileNotFoundError(src)
        if str(src) not in source_files:
            source_files[str(src)] = {"path": str(src.relative_to(ROOT)), "sha256": sha(src)}
        r["_source_file"] = src
        by_arm_key[key] = r

    # Check each exported value against its original retained prediction ledger.
    ledger_cache: dict[str, dict] = {}
    for r in prediction_rows:
        path = r["_source_file"]
        cache_key = str(path)
        if cache_key not in ledger_cache:
            ledger_cache[cache_key] = {}
            for x in read_csv(path):
                k = (x["game_id"], x["player_id"], x.get("line", "") if r["model_version"] != PRODUCTION else "")
                if k in ledger_cache[cache_key]:
                    raise ValueError(f"duplicate retained source identity: {path} {k}")
                ledger_cache[cache_key][k] = x
        source_line = "" if r["model_version"] == PRODUCTION else r["market_line"]
        srcrow = ledger_cache[cache_key].get((r["game_id"], r["player_id"], source_line))
        if srcrow is None:
            raise ValueError(f"export row absent from source ledger: {r['candidate_key']}")
        if r["model_version"] == PRODUCTION:
            pcol = f"p_over_{str(r['market_line']).replace('.', '_')}"
            sp, slambda = srcrow.get(pcol, ""), srcrow.get("expected_sog", "")
        else:
            sp, slambda = srcrow.get("p_over", ""), srcrow.get("shadow_lambda", "")
            if srcrow.get("model_identity") != r["model_version"]:
                raise ValueError(f"shadow source model identity mismatch: {r['candidate_key']}")
            if srcrow.get("selected_side") != r["selected_side"] or not close(r["probability_under"], srcrow.get("p_under", "")):
                raise ValueError(f"export/source under probability or selected side mismatch: {r['candidate_key']}")
        if not close(r["probability_over"], sp) or not close(r["expected_count"], slambda):
            raise ValueError(f"export/source probability or count mismatch: {r['candidate_key']}")

    identity_keys = sorted({k[:4] for k in by_arm_key})
    paired = []
    missing_arms = Counter()
    for identity in identity_keys:
        arms = {a: by_arm_key.get((*identity, a)) for a in ARMS}
        for a, row in arms.items():
            if row is None:
                missing_arms[a] += 1
        if any(row is None for row in arms.values()):
            continue
        base = arms[PRODUCTION]
        for a in ARMS[1:]:
            row = arms[a]
            for field in ("scheduled_start_utc", "outcome_status", "outcome_value", "outcome_source", "outcome_source_timestamp_utc"):
                if row[field] != base[field]:
                    raise ValueError(f"paired source outcome mismatch {identity} field={field}")
        actual = num(base["outcome_value"]) if base["outcome_status"] == "SETTLED" else None
        if base["outcome_status"] == "SETTLED" and actual is None:
            raise ValueError(f"settled outcome lacks count {identity}")
        line = float(base["market_line"])
        row = {"slate_date": identity[0], "game_id": identity[1], "player_id": identity[2], "market_line": base["market_line"],
               "outcome_status": base["outcome_status"], "actual_sog": "" if actual is None else actual,
               "outcome_source": base["outcome_source"], "outcome_source_timestamp_utc": base["outcome_source_timestamp_utc"],
               "scheduled_start_utc": base["scheduled_start_utc"], "feature_cutoff_utc": base["feature_cutoff_utc"],
               "prediction_timestamp_utc": base["prediction_timestamp_utc"], "nonparticipating_status": "NOT_RECORDED_IN_CANDIDATE_EXPORT",
               "candidate_policy_status": "NOT_APPLIED"}
        for a in ARMS:
            r = arms[a]
            label = LABELS[a]
            row.update({f"{label}_probability_over": r["probability_over"], f"{label}_probability_under": r["probability_under"],
                        f"{label}_selected_side": r["selected_side"], f"{label}_expected_count": r["expected_count"],
                        f"{label}_prediction_timestamp_utc": r["prediction_timestamp_utc"],
                        f"{label}_feature_cutoff_utc": r["feature_cutoff_utc"],
                        f"{label}_model_role": r["model_role"], f"{label}_prediction_identity": r["prediction_identity"],
                        f"{label}_source_path": r["source_path"], f"{label}_source_sha256": r["source_sha256"],
                        f"{label}_parent_run_id": r["parent_run_id"], f"{label}_candidate_key": r["candidate_key"],
                        f"{label}_model_identity_sha256": r["model_identity_sha256"]})
        row["d10_sog_per60"] = ""
        row["d20_sog_per60"] = ""
        row["d10_d20_abs_gap"] = ""
        row["rate_group"] = ""
        row["divergence_group"] = ""
        # Retained 25/75 source ledger supplies the common D10/D20 rates and TOI.
        s25 = ledger_cache[str(arms[BLEND25]["_source_file"])][(identity[1], identity[2], identity[3])]
        row["d10_sog_per60"] = s25["d10_sog_per60"]
        row["d20_sog_per60"] = s25["d20_sog_per60"]
        d10, d20 = num(s25["d10_sog_per60"]), num(s25["d20_sog_per60"])
        gap = abs(d10 - d20)
        row["d10_d20_abs_gap"] = gap
        row["d10_d20_direction"] = "D10_ABOVE_D20" if d10 > d20 else ("D20_ABOVE_D10" if d20 > d10 else "EQUAL")
        row["selected_toi_minutes"] = s25.get("selected_toi_minutes", "")
        paired.append(row)

    # Data-derived cut points are computed from the prospective D10 feature only.
    d10_values = sorted(num(r["d10_sog_per60"]) for r in paired if num(r["d10_sog_per60"]) is not None)
    d10_values_pg: dict[tuple[str, str, str], float] = {}
    for r in paired:
        d10_values_pg[(r["slate_date"], r["game_id"], r["player_id"])] = float(r["d10_sog_per60"])
    d10pg = sorted(d10_values_pg.values())
    def quantile(values: list[float], q: float) -> float:
        return values[min(len(values)-1, max(0, round((len(values)-1)*q)))]
    q50, q80 = quantile(d10pg, .5), quantile(d10pg, .8)
    for r in paired:
        rate = float(r["d10_sog_per60"])
        r["rate_group"] = "HIGH_D10_TOP_20_PERCENT" if rate >= q80 else ("LOW_D10_BOTTOM_HALF" if rate <= q50 else "MID_D10")
        gap = float(r["d10_d20_abs_gap"])
        r["divergence_group"] = "LARGE_GAP_GE_2" if gap >= 2 else ("MODERATE_GAP_1_TO_2" if gap >= 1 else "SMALL_GAP_LT_1")

    # Participation is an auxiliary retained official-grade field when present.
    participation = {}
    outcome_sources = {}
    for date in sorted({r["slate_date"] for r in paired}):
        paths = sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation" / date).glob("reconciliation=*/graded_sog.csv"))
        if not paths:
            continue
        p = restated_grade_path(paths[-1].parent, "graded_sog.csv")
        outcome_sources[str(p)] = {"path": str(p.relative_to(ROOT)), "sha256": sha(p)}
        for x in read_csv(p):
            k = (date, x["game_id"], x["player_id"])
            if x.get("prop_type") == "shots_on_goal" and x.get("participation_status"):
                participation[k] = x["participation_status"]
    for r in paired:
        stat = participation.get((r["slate_date"], r["game_id"], r["player_id"]))
        if stat:
            r["nonparticipating_status"] = stat
        elif r["outcome_status"] == "UNRESOLVED" and r["slate_date"] == "2026-10-17":
            r["nonparticipating_status"] = "NOT_YET_RECONCILED"
        elif r["outcome_status"] == "UNRESOLVED":
            r["nonparticipating_status"] = "UNRESOLVED_IN_EXPORT"
        else:
            r["nonparticipating_status"] = "NOT_RECORDED"

    # One fixed pricing policy: BetOnline, exported latest qualified quote per candidate/book/side.
    quote_by_candidate = {}
    for q in quote_rows:
        if q["sportsbook_key"] != BOOK:
            continue
        if q["quote_qualification_status"] != "QUALIFIED_SOURCE_AND_OBSERVATION_PRECEDE_PREDICTION":
            continue
        key = q["candidate_key"]
        if key in quote_by_candidate:
            raise ValueError(f"duplicate fixed-book candidate quote: {key}")
        if q["quote_side"] != q["selected_side"]:
            raise ValueError(f"SOG fixed-book quote side differs from model side: {key}")
        quote_by_candidate[key] = q
        if utc(q["source_quote_timestamp_utc"]) > utc(q["observation_timestamp_utc"]):
            raise ValueError(f"quote source timestamp follows observation: {key}")
    for r in paired:
        quote_flags = []
        for a in ARMS:
            label = LABELS[a]
            q = quote_by_candidate.get(r[f"{label}_candidate_key"])
            if q:
                if not utc(q["observation_timestamp_utc"]) <= utc(r["prediction_timestamp_utc"]) < utc(r["scheduled_start_utc"]):
                    raise ValueError("quote/prediction/start chronology failed")
                r.update({f"{label}_betonline_quote_status": "QUALIFIED", f"{label}_betonline_side": q["quote_side"],
                          f"{label}_betonline_american_price": q["american_price"], f"{label}_betonline_decimal_price": q["decimal_price"],
                          f"{label}_betonline_quote_timestamp_utc": q["source_quote_timestamp_utc"],
                          f"{label}_betonline_observation_timestamp_utc": q["observation_timestamp_utc"],
                          f"{label}_betonline_source_path": q["source_path"], f"{label}_betonline_source_sha256": q["source_sha256"]})
                quote_flags.append(True)
            else:
                r.update({f"{label}_betonline_quote_status": "NO_QUALIFIED_QUOTE", f"{label}_betonline_side": "",
                          f"{label}_betonline_american_price": "", f"{label}_betonline_decimal_price": "",
                          f"{label}_betonline_quote_timestamp_utc": "", f"{label}_betonline_observation_timestamp_utc": "",
                          f"{label}_betonline_source_path": "", f"{label}_betonline_source_sha256": ""})
                quote_flags.append(False)
        r["betonline_all_three_arms_quoted"] = all(quote_flags)

    # Outcome and metric rows; probability observations retain line correlation explicitly.
    qualified_books = qualified_quote_books(paired, quote_rows)
    metric_rows = []
    calibration_rows = []
    groupers = [("overall", lambda r: "ALL"), ("date", lambda r: r["slate_date"]),
                ("line", lambda r: r["market_line"]), ("rate_group", lambda r: r["rate_group"]),
                ("divergence_group", lambda r: r["divergence_group"]),
                ("d10_d20_direction", lambda r: r["d10_d20_direction"]),
                ("production_selected_side", lambda r: r["production_d10_selected_side"])]
    for group_name, key_fn in groupers:
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in paired:
            if r["outcome_status"] == "SETTLED":
                groups[key_fn(r)].append(r)
        for group_key, rows in sorted(groups.items()):
            for a in ARMS:
                label = LABELS[a]
                scored = []
                for r in rows:
                    p = float(r[f"{label}_probability_over"])
                    y = outcome_over(float(r["actual_sog"]), float(r["market_line"]))
                    scored.append((r, p, y))
                brier = sum((p-y)**2 for _, p, y in scored) / len(scored)
                ll = sum(logloss(y,p) for _,p,y in scored) / len(scored)
                accuracy = sum(outcome_selected(float(r["actual_sog"]),float(r["market_line"]),r[f"{label}_selected_side"]) for r,_,_ in scored) / len(scored)
                bias = sum(p-y for _,p,y in scored) / len(scored)
                row = {"group_type": group_name, "group": group_key, "model": label, "settled_line_rows": len(scored),
                       "settled_player_games": len({(r["slate_date"],r["game_id"],r["player_id"]) for r,_,_ in scored}),
                       "brier": brier, "log_loss": ll, "selected_side_accuracy": accuracy,
                       "observed_over_rate": sum(y for _,_,y in scored)/len(scored),
                       "mean_predicted_over_probability": sum(p for _,p,_ in scored)/len(scored), "calibration_bias_pred_minus_obs": bias}
                if group_name == "overall":
                    ordered = sorted(scored, key=lambda item: (item[1], item[0]["slate_date"], item[0]["game_id"], item[0]["player_id"], item[0]["market_line"]))
                    bins = []
                    for i in range(10):
                        bucket = ordered[i*len(ordered)//10:(i+1)*len(ordered)//10]
                        observed = sum(y for _,_,y in bucket) / len(bucket)
                        forecast = sum(p for _,p,_ in bucket) / len(bucket)
                        bins.append({"model":label,"probability_decile":i+1,"n_line_rows":len(bucket),
                                     "mean_predicted_over_probability":forecast,"observed_over_rate":observed,
                                     "calibration_gap_pred_minus_obs":forecast-observed})
                    row["equal_count_decile_ece"] = sum(b["n_line_rows"] * abs(b["calibration_gap_pred_minus_obs"]) for b in bins) / len(scored)
                    calibration_rows.extend(bins)
                if group_name in ("overall", "date", "rate_group", "divergence_group"):
                    pg = {}
                    for r,_,_ in scored:
                        pg[(r["slate_date"],r["game_id"],r["player_id"])] = r
                    count_errors = [float(r[f"{label}_expected_count"]) - float(r["actual_sog"]) for r in pg.values()]
                    row.update({"count_player_games": len(count_errors), "count_mae": sum(abs(x) for x in count_errors)/len(count_errors),
                                "count_rmse": math.sqrt(sum(x*x for x in count_errors)/len(count_errors)),
                                "mean_predicted_count": sum(float(r[f"{label}_expected_count"]) for r in pg.values())/len(pg)})
                metric_rows.append(row)

    # Paired deltas per settled line row/date and full-population date stability.
    comparisons = []
    settled = [r for r in paired if r["outcome_status"] == "SETTLED"]
    candidate_by_arm = {
        label: {row[f"{label}_candidate_key"] for row in paired}
        for label in LABELS.values()
    }
    all_books = sorted({book for books in qualified_books.values() for book in books})
    sportsbook_coverage = []
    for book in all_books:
        for label in LABELS.values():
            keys = candidate_by_arm[label]
            all_rows = [r for r in paired if r[f"{label}_candidate_key"] in keys and book in qualified_books.get(r[f"{label}_candidate_key"], set())]
            settled_rows = [r for r in settled if book in qualified_books.get(r[f"{label}_candidate_key"], set())]
            sportsbook_coverage.append({"sportsbook": book, "model": label,
                "paired_line_rows": len(paired), "qualified_paired_quote_rows": len(all_rows),
                "paired_quote_coverage": len(all_rows) / len(paired) if paired else None,
                "settled_line_rows": len(settled), "qualified_settled_quote_rows": len(settled_rows),
                "settled_quote_coverage": len(settled_rows) / len(settled) if settled else None})
        common_settled = sum(all(book in qualified_books.get(r[f"{label}_candidate_key"], set()) for label in LABELS.values()) for r in settled)
        common_paired = sum(all(book in qualified_books.get(r[f"{label}_candidate_key"], set()) for label in LABELS.values()) for r in paired)
        sportsbook_coverage.append({"sportsbook": book, "model": "all_three_arms",
            "paired_line_rows": len(paired), "qualified_paired_quote_rows": common_paired,
            "paired_quote_coverage": common_paired / len(paired) if paired else None,
            "settled_line_rows": len(settled), "qualified_settled_quote_rows": common_settled,
            "settled_quote_coverage": common_settled / len(settled) if settled else None})
    for scope, fn in [("overall", lambda r:"ALL"),("date",lambda r:r["slate_date"]),("line",lambda r:r["market_line"]),
                      ("rate_group",lambda r:r["rate_group"]),("divergence_group",lambda r:r["divergence_group"]),
                      ("d10_d20_direction",lambda r:r["d10_d20_direction"]),
                      ("production_selected_side",lambda r:r["production_d10_selected_side"])]:
        groups=defaultdict(list)
        for r in settled: groups[fn(r)].append(r)
        for k, rows in sorted(groups.items()):
            for arm in (BLEND25,BLEND50):
                label=LABELS[arm]
                delta = paired_probability_delta(rows, label)
                comparisons.append({"group_type":scope,"group":k,"challenger":label,"settled_line_rows":len(rows),**delta})

    # Quote coverage and one-unit return: the exact same single book/policy; no best-price selection.
    quote_metrics=[]
    common=[r for r in settled if r["betonline_all_three_arms_quoted"]]
    for scope, rows in [("arm_quote_population", settled),("common_three_arm_quote_population", common)]:
        for a in ARMS:
            label=LABELS[a]
            available=[r for r in rows if r[f"{label}_betonline_quote_status"]=="QUALIFIED"]
            profit=[]
            for r in available:
                won=outcome_selected(float(r["actual_sog"]),float(r["market_line"]),r[f"{label}_selected_side"])
                price=float(r[f"{label}_betonline_american_price"])
                profit.append(american_profit(price) if won else -1.0)
            quote_metrics.append({"population":scope,"sportsbook":BOOK,"quote_policy":"latest retained chronology-qualified quote per candidate/book/side; model-selected side",
                                  "model":label,"settled_candidate_line_rows":len(rows),"qualified_quote_rows":len(available),
                                  "quote_coverage":len(available)/len(rows) if rows else None,"wins":sum(x>0 for x in profit),
                                  "losses":sum(x<0 for x in profit),"one_unit_net_profit":sum(profit),
                                  "roi_per_one_unit_risked":sum(profit)/len(profit) if profit else None})
    for date in sorted({r["slate_date"] for r in settled}):
        d=[r for r in settled if r["slate_date"]==date]
        for a in ARMS:
            label=LABELS[a]
            available=[r for r in d if r[f"{label}_betonline_quote_status"]=="QUALIFIED"]
            scores=[]
            for r in available:
                won=outcome_selected(float(r["actual_sog"]),float(r["market_line"]),r[f"{label}_selected_side"])
                price=float(r[f"{label}_betonline_american_price"])
                scores.append(american_profit(price) if won else -1.0)
            quote_metrics.append({"population":"date_arm_quote_population","slate_date":date,"sportsbook":BOOK,
                                  "quote_policy":"latest retained chronology-qualified quote per candidate/book/side; model-selected side",
                                  "model":label,"settled_candidate_line_rows":len(d),"qualified_quote_rows":len(available),
                                  "quote_coverage":len(available)/len(d) if d else None,"wins":sum(x>0 for x in scores),"losses":sum(x<0 for x in scores),
                                  "one_unit_net_profit":sum(scores),"roi_per_one_unit_risked":sum(scores)/len(scores) if scores else None})

    # Attach nonparticipation evidence from retained grading without treating missing evidence as nonparticipation.
    paired.sort(key=lambda r:(r["slate_date"],int(r["game_id"]),int(r["player_id"]),float(r["market_line"])))
    write_csv(OUT/"paired_sog_candidates_unfiltered.csv", paired)
    write_csv(OUT/"metrics.csv", metric_rows)
    write_csv(OUT/"paired_deltas.csv", comparisons)
    write_csv(OUT/"betonline_pricing.csv", quote_metrics)
    write_csv(OUT/"sportsbook_coverage.csv", sportsbook_coverage)
    write_csv(OUT/"calibration_bins.csv", calibration_rows)

    cold_arms = sorted({r["model_version"] for r in all_prediction_rows if "cold_start_shadow" in r["model_version"]})
    cold_summary = []
    for arm in cold_arms:
        xs = [r for r in all_prediction_rows if r["model_version"] == arm]
        settled_xs = [r for r in xs if r["outcome_status"] == "SETTLED" and num(r["outcome_value"]) is not None]
        by_pg = {}
        for r in settled_xs:
            by_pg[(r["slate_date"], r["game_id"], r["player_id"])] = r
        if settled_xs:
            ys=[outcome_over(float(r["outcome_value"]),float(r["market_line"])) for r in settled_xs]
            ps=[float(r["probability_over"]) for r in settled_xs]
            errs=[float(r["expected_count"])-float(r["outcome_value"]) for r in by_pg.values()]
            cold_summary.append({"model_version":arm,"model_role":xs[0]["model_role"],"candidate_lines":len(xs),
                                 "player_games":len({(r["slate_date"],r["game_id"],r["player_id"]) for r in xs}),
                                 "settled_line_rows":len(settled_xs),"unresolved_line_rows":sum(r["outcome_status"]=="UNRESOLVED" for r in xs),
                                 "brier":sum((p-y)**2 for p,y in zip(ps,ys))/len(ps),
                                 "log_loss":sum(logloss(y,p) for p,y in zip(ps,ys))/len(ps),
                                 "selected_side_accuracy":sum(outcome_selected(float(r["outcome_value"]),float(r["market_line"]),r["selected_side"]) for r in settled_xs)/len(settled_xs),
                                 "calibration_bias_pred_minus_obs":sum(ps)/len(ps)-sum(ys)/len(ys),
                                 "count_player_games":len(errs),"count_mae":sum(abs(e) for e in errs)/len(errs),
                                 "count_rmse":math.sqrt(sum(e*e for e in errs)/len(errs)),
                                 "interpretation":"unpaired population; not directly comparable to production/blends"})
    cold_summary.sort(key=lambda x:x["model_version"])
    write_csv(OUT/"cold_start_arm_metrics.csv", cold_summary)

    arm_counts = {}
    for arm in ARMS:
        source_rows=[r for r in prediction_rows if r["model_version"]==arm]
        arm_counts[LABELS[arm]]={"line_candidates":len(source_rows),"player_games":len({(r["slate_date"],r["game_id"],r["player_id"]) for r in source_rows}),
                                 "settled_line_rows":sum(r["outcome_status"]=="SETTLED" for r in source_rows),
                                 "unresolved_line_rows":sum(r["outcome_status"]=="UNRESOLVED" for r in source_rows),
                                 "dates":sorted({r["slate_date"] for r in source_rows})}
    statuses=Counter(r["outcome_status"] for r in paired)
    participation_by_player_game = {}
    for r in paired:
        participation_by_player_game[(r["slate_date"], r["game_id"], r["player_id"])] = r["nonparticipating_status"]
    parts=Counter(participation_by_player_game.values())
    settled_player_games = len({(r["slate_date"], r["game_id"], r["player_id"]) for r in paired if r["outcome_status"] == "SETTLED"})
    unresolved_player_games = len({(r["slate_date"], r["game_id"], r["player_id"]) for r in paired if r["outcome_status"] == "UNRESOLVED"})
    old_grade_sources = []
    old_grade_settled_export_unresolved = set()
    for date in ("2026-10-10", "2026-10-11", "2026-10-12", "2026-10-13"):
        candidates = [r for r in prediction_rows if r["slate_date"] == date and r["model_version"] == PRODUCTION]
        if not candidates:
            continue
        run_id = candidates[0]["parent_run_id"]
        grade_path = ROOT / "artifacts/operational/nhl/sog_fixed_blend_grades" / f"slate_date={date}" / f"run_id={run_id}" / f"model={BLEND25}" / "graded_predictions.csv"
        if not grade_path.is_file():
            continue
        old_grade_sources.append({"path":str(grade_path.relative_to(ROOT)),"sha256":sha(grade_path)})
        for g in read_csv(grade_path):
            key=(date,g["game_id"],g["player_id"],g["line"],BLEND25)
            current=by_arm_key.get(key)
            if g.get("outcome_status")=="SETTLED" and current and current["outcome_status"]=="UNRESOLVED":
                old_grade_settled_export_unresolved.add((date,g["game_id"],g["player_id"],g["line"]))
    discrepant_player_games=len({x[:3] for x in old_grade_settled_export_unresolved})
    manifest={"schema_version":"NHL_SOG_PAIRED_BLEND_EVALUATION_V1","study":"prospective exact three-arm SOG comparison",
              "source_export_manifest_path":str((EXPORT/"manifest.json").relative_to(ROOT)),"source_export_manifest_sha256":sha(EXPORT/"manifest.json"),
              "source_predictions_sha256":sha(input_predictions),"source_quotes_sha256":sha(input_quotes),
              "prediction_source_artifacts":sorted(source_files.values(),key=lambda x:x["path"]),"official_grade_participation_sources":sorted(outcome_sources.values(),key=lambda x:x["path"]),
              "prior_grade_crosscheck":{"grade_sources":old_grade_sources,"prior_settled_export_unresolved_line_rows":len(old_grade_settled_export_unresolved),
                                        "prior_settled_export_unresolved_player_games":discrepant_player_games,
                                        "primary_outcome_policy":"honor frozen full-window candidate export; do not promote older grade labels into its outcome status"},
              "sportsbook_quote_policy":{"sportsbook":BOOK,"selection":"latest retained chronology-qualified quote per candidate/book/side; bet only model-selected side","hindsight_best_price_selection":False},
              "pricing_rows_source_sha256":sha(input_quotes),"date_window":{"start":min(r["slate_date"] for r in paired),"end":max(r["slate_date"] for r in paired),"date_count":len({r["slate_date"] for r in paired})},
              "population":{"arm_counts":arm_counts,"exact_common_line_rows":len(paired),"exact_common_player_games":len({(r["slate_date"],r["game_id"],r["player_id"]) for r in paired}),
                            "outcome_status_counts_line_rows":dict(statuses),"participation_status_counts_player_games":dict(parts),
                            "explicit_nonparticipating_player_games":sum(n for k,n in parts.items() if "NONPARTICIP" in k),
                            "missing_arm_identity_counts":dict(missing_arms),"started_game_exclusions":0,"candidate_policy":"NOT_APPLIED"},
              "validation":{"all_export_probabilities_and_expected_counts_match_retained_source_ledgers":True,"prediction_chronology_pass":True,
                            "all_three_arms_share_outcome_and_scheduled_start":True,"quotes_source_observation_prediction_start_order_validated":True,
                            "quote_rows_unique_per_candidate_and_book":True},
              "cold_start_arms_separate":{"arm_count":len(cold_summary),"arms":cold_summary},
              "outputs":{p.name:sha(p) for p in [OUT/"paired_sog_candidates_unfiltered.csv",OUT/"metrics.csv",OUT/"paired_deltas.csv",OUT/"betonline_pricing.csv",OUT/"sportsbook_coverage.csv",OUT/"calibration_bins.csv",OUT/"cold_start_arm_metrics.csv"]},
              "limitations":["Only eight dates with retained common production/25-75/50-50 prospective captures are evaluable; the export begins earlier, but these arms were not retained before October 10.",
                              "Probabilistic metrics count correlated 1.5/2.5/3.5 lines; count error is once per player-game.",
                              "Only BetOnline model-selected-side prices are used for descriptive one-unit ROI; the market/quote population is incomplete and selection is not a wager record.",
                              "Official participation is reported only where the retained grade file records it; unresolved and blank status are not relabeled as nonparticipation.",
                              "Evaluation is descriptive, small-window prospective evidence and does not authorize model promotion."]}
    (OUT/"manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    overall = {r["model"]:r for r in metric_rows if r["group_type"]=="overall"}
    delta_by = {r["challenger"]:r for r in comparisons if r["group_type"]=="overall"}
    date_deltas=[r for r in comparisons if r["group_type"]=="date"]
    quote_overall=[r for r in quote_metrics if r["population"]=="common_three_arm_quote_population"]
    lines=["# NHL 2026 paired SOG production and D10/D20 blend evaluation","",
           f"The exact common prospective population is {len(paired):,} candidate-line rows over {len({(r['slate_date'],r['game_id'],r['player_id']) for r in paired}):,} player-games and {len({r['slate_date'] for r in paired})} dates ({min(r['slate_date'] for r in paired)} through {max(r['slate_date'] for r in paired)}). All three model arms are present on every included slate/game/player/line identity.","",
           "The compact machine-readable summary is `docs/nhl/2026_sog_paired_blend_evaluation_2026-10-17.summary.json`. The unfiltered paired CSV and detailed result CSVs are generated local artifacts, ignored by the repository's `artifacts/**` rule, and are not tracked. Recreate them by first running the documented full-window exporter, then `./bin/python -m backend.nhl.scripts.evaluate_nhl_sog_paired_blends_full_window`.","",
           "## Population and outcomes","",
           "| Arm | Candidate lines | Player-games | Settled lines | Unresolved lines |","|---|---:|---:|---:|---:|"]
    for label,x in arm_counts.items(): lines.append(f"| {label} | {x['line_candidates']:,} | {x['player_games']:,} | {x['settled_line_rows']:,} | {x['unresolved_line_rows']:,} |")
    lines += ["",f"The primary scored population has {statuses.get('SETTLED',0):,} settled line rows ({settled_player_games:,} settled player-games) and {statuses.get('UNRESOLVED',0):,} unresolved rows ({unresolved_player_games:,} unresolved player-games). The retained data contains no explicit nonparticipating status for this common population; zero SOG counts are settled observations, while blank or unresolved participation is not assumed to mean nonparticipation.","",
           "## Overall settled metrics","","| Model | n line rows | Brier | Log loss | Selected-side accuracy | Observed OVER | Mean predicted OVER | Calibration bias | Decile ECE | Count MAE (player-game) | Count RMSE |","|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for label in LABELS.values():
        x=overall[label]; lines.append(f"| {label} | {x['settled_line_rows']:,} | {x['brier']:.5f} | {x['log_loss']:.5f} | {x['selected_side_accuracy']:.3%} | {x['observed_over_rate']:.3%} | {x['mean_predicted_over_probability']:.3%} | {x['calibration_bias_pred_minus_obs']:+.5f} | {x['equal_count_decile_ece']:.5f} | {x.get('count_mae',float('nan')):.4f} | {x.get('count_rmse',float('nan')):.4f} |")
    lines += ["", "## Paired changes from production","", "| Challenger | Brier delta | Log-loss delta | Rows with lower Brier | Rows with higher Brier | Side disagreements |","|---|---:|---:|---:|---:|---:|"]
    for label,x in delta_by.items(): lines.append(f"| {label} | {x['brier_delta_vs_production']:+.6f} | {x['log_loss_delta_vs_production']:+.6f} | {x['brier_improved_rows']:,} | {x['brier_worsened_rows']:,} | {x['selected_side_disagreements']:,} |")
    metric_lookup = {(r["group_type"], r["group"], r["model"]): r for r in metric_rows}
    delta_lookup = {(r["group_type"], r["group"], r["challenger"]): r for r in comparisons}
    lines += ["", "## Lines, rate groups, and D10/D20 divergence", "",
              "| Segment | Player-games | Challenger | Brier delta vs production | Log-loss delta vs production | Count MAE |",
              "|---|---:|---|---:|---:|---:|"]
    for gtype in ("line", "rate_group", "divergence_group", "d10_d20_direction", "production_selected_side"):
        for group in sorted({r["group"] for r in metric_rows if r["group_type"] == gtype}):
            for challenger in ("blend_25_75", "blend_50_50"):
                delta = delta_lookup[(gtype, group, challenger)]
                metric = metric_lookup.get((gtype, group, challenger), {})
                count_mae = metric.get("count_mae")
                mae_text = f"{count_mae:.4f}" if count_mae is not None else ""
                lines.append(f"| {gtype}: {group} | {metric.get('settled_player_games', '')} | {challenger} | {delta['brier_delta_vs_production']:+.6f} | {delta['log_loss_delta_vs_production']:+.6f} | {mae_text} |")
    lines += ["", "## Date stability","", "| Date | Challenger | Settled lines | Brier delta | Log-loss delta |","|---|---|---:|---:|---:|"]
    for x in date_deltas:
        lines.append(f"| {x['group']} | {x['challenger']} | {x['settled_line_rows']:,} | {x['brier_delta_vs_production']:+.6f} | {x['log_loss_delta_vs_production']:+.6f} |")
    date_summaries = []
    for label in ("blend_25_75", "blend_50_50"):
        xs = [x for x in date_deltas if x["challenger"] == label]
        better = [x["group"] for x in xs if x["brier_delta_vs_production"] < 0 and x["log_loss_delta_vs_production"] < 0]
        worse = [x["group"] for x in xs if x["brier_delta_vs_production"] > 0 and x["log_loss_delta_vs_production"] > 0]
        date_summaries.append(f"{label} improved both scores on {len(better)} dates ({', '.join(better)}) and worsened both on {len(worse)} ({', '.join(worse) or 'none'}).")
    lines += ["", "Across the seven settled dates, " + " ".join(date_summaries)]
    for label in ("blend_25_75", "blend_50_50"):
        slices = [x for x in comparisons if x["challenger"] == label and x["group_type"] in ("rate_group", "divergence_group", "d10_d20_direction") and x["group"] != "EQUAL"]
        strongest = min(slices, key=lambda x:x["brier_delta_vs_production"])
        weakest = max(slices, key=lambda x:x["brier_delta_vs_production"])
        lines.append(f"{label} strongest slice: {strongest['group_type']} {strongest['group']} ({strongest['settled_line_rows']:,} lines, Brier {strongest['brier_delta_vs_production']:+.6f}, log loss {strongest['log_loss_delta_vs_production']:+.6f}); weakest: {weakest['group_type']} {weakest['group']} ({weakest['settled_line_rows']:,} lines, Brier {weakest['brier_delta_vs_production']:+.6f}, log loss {weakest['log_loss_delta_vs_production']:+.6f}).")
    over_delta = delta_lookup[("production_selected_side", "OVER", "blend_25_75")]
    under_delta = delta_lookup[("production_selected_side", "UNDER", "blend_25_75")]
    over_pg = metric_lookup[("production_selected_side", "OVER", "production_d10")]["settled_player_games"]
    under_pg = metric_lookup[("production_selected_side", "UNDER", "production_d10")]["settled_player_games"]
    lines.append(f"On production-selected OVER lines ({over_pg:,} player-games), 25/75 changes Brier/log loss by {over_delta['brier_delta_vs_production']:+.6f}/{over_delta['log_loss_delta_vs_production']:+.6f}; on production-selected UNDER lines ({under_pg:,} player-games), by {under_delta['brier_delta_vs_production']:+.6f}/{under_delta['log_loss_delta_vs_production']:+.6f}. These are fixed slices by the production selection, not post-hoc challenger thresholds.")
    lines.append("A player-game can appear in both side slices when production selected different sides at different lines; line-level rows are correlated.")
    lines += ["", "## Sportsbook coverage and fixed-book pricing","",
              "Coverage below counts a candidate only when the selected-side quote is source- and observation-qualified before that model arm's prediction, with prediction before scheduled start. The denominator is settled candidate-line rows; a line can be counted once for each sportsbook. The complete per-book/per-arm results are in `sportsbook_coverage.csv`.","",
              "| Sportsbook | Three-arm common settled lines | Coverage |","|---|---:|---:|"]
    common_book_rows = [r for r in sportsbook_coverage if r["model"] == "all_three_arms"]
    for x in sorted(common_book_rows, key=lambda r:(-r["qualified_settled_quote_rows"],r["sportsbook"]))[:5]:
        lines.append(f"| {x['sportsbook']} | {x['qualified_settled_quote_rows']:,}/{x['settled_line_rows']:,} | {x['settled_quote_coverage']:.1%} |")
    lines += ["",f"Pricing uses BetOnline only, at the model-selected side, with the full export's latest chronology-qualified candidate/book/side quote rule. On the common settled population where all three arms have a BetOnline quote:","","| Model | Common quote rows | One-unit net profit | Descriptive ROI |","|---|---:|---:|---:|"]
    for x in quote_overall: lines.append(f"| {x['model']} | {x['qualified_quote_rows']:,} | {x['one_unit_net_profit']:+.2f} | {x['roi_per_one_unit_risked']:+.2%} |")
    lines += ["", "The fixed-book ROI rows are descriptive. Correlated lines and repeated players mean these are not independent wager samples. No alternative-book price substitution was made. Ten probability deciles are in `calibration_bins.csv`; the complete per-date ROI, line/date/rate/divergence metrics, quote coverage, and unfiltered paired rows are in the adjacent CSVs.","",
              "## Cold-start arms","","A/B/C/D/G cold-start arms are evaluated separately in `cold_start_arm_metrics.csv`. Their populations and model contracts differ from the production and fixed-blend trio, so those scores are descriptive and are not ranked against the paired comparison.","",
              "| Arm | Candidate lines | Settled lines | Brier | Log loss | Selected-side accuracy | Count MAE |","|---|---:|---:|---:|---:|---:|---:|"]
    for x in cold_summary:
        arm_label = x["model_version"].split(":", 1)[-1]
        lines.append(f"| {arm_label} | {x['candidate_lines']:,} | {x['settled_line_rows']:,} | {x['brier']:.5f} | {x['log_loss']:.5f} | {x['selected_side_accuracy']:.3%} | {x['count_mae']:.4f} |")
    lines += ["", "Separate cold-start metrics and source provenance are in `cold_start_arm_metrics.csv` and the manifest.","",
              f"The previous October 10–13 prospective grade package contains {discrepant_player_games} player-games ({len(old_grade_settled_export_unresolved)} line rows) marked settled that the frozen full-window candidate export leaves unresolved. The primary analysis follows the frozen export's outcome status and excludes them from settled metrics. The source-coverage mismatch is recorded for follow-up; the export and older grade rows were not rewritten.","",
              "## Readout","","Continue the 25/75 blend as the primary prospective challenger, with 50/50 retained as a comparator. Selection is based on measured paired probability and count metrics across the retained window, not on ROI. This is not a promotion recommendation: only eight dates are available, observations are correlated within games and players, and date-level results must be monitored prospectively. Cold-start A/B/C/D/G arms are outside the exact three-arm comparison because their forecast populations differ.","",
              "The paired export includes every exact three-arm candidate line before filtering, including unresolved outcomes. It validates each probability and expected count against the originating immutable prediction ledger and verifies source-quote/observation/prediction/scheduled-start chronology. Started-game exclusions are zero in this exact common capture population; the full export's other SOG arms and dates are not mixed into the primary paired comparison.","",
              "Nonparticipation cannot be fully separated from the exported outcome population because the retained grading source does not provide a complete explicit status for every paired player-game. The CSV records `NOT_RECORDED`, `UNRESOLVED_IN_EXPORT`, `NOT_YET_RECONCILED`, or retained participation labels without converting missing evidence into a participation assumption.","",
              "Source export and artifact hashes are recorded in `manifest.json`. No provider requests, database writes, scoring, or production-authority changes were performed."]
    report_text = "\n".join(lines)+"\n"
    (OUT/"report.md").write_text(report_text,encoding="utf-8")
    DOC_REPORT.parent.mkdir(parents=True, exist_ok=True)
    DOC_REPORT.write_text(report_text,encoding="utf-8")
    manifest["outputs"]["report.md"] = sha(OUT/"report.md")
    manifest["outputs"][str(DOC_REPORT.relative_to(ROOT))] = sha(DOC_REPORT)
    summary = {
        "schema_version": "NHL_SOG_PAIRED_BLEND_SUMMARY_V1",
        "study": "prospective exact three-arm SOG comparison",
        "source": {
            "export_manifest_path": str((EXPORT / "manifest.json").relative_to(ROOT)),
            "export_manifest_sha256": sha(EXPORT / "manifest.json"),
            "predictions_sha256": sha(input_predictions),
            "quotes_sha256": sha(input_quotes),
        },
        "date_window": manifest["date_window"],
        "population": manifest["population"],
        "overall_metrics": [r for r in metric_rows if r["group_type"] == "overall"],
        "date_metrics": [r for r in metric_rows if r["group_type"] == "date"],
        "line_metrics": [r for r in metric_rows if r["group_type"] == "line"],
        "date_deltas": [r for r in comparisons if r["group_type"] == "date"],
        "line_deltas": [r for r in comparisons if r["group_type"] == "line"],
        "subgroup_deltas": [r for r in comparisons if r["group_type"] in (
            "rate_group", "divergence_group", "d10_d20_direction", "production_selected_side")],
        "calibration_bins": calibration_rows,
        "sportsbook_coverage": sportsbook_coverage,
        "betonline_pricing": quote_metrics,
        "local_unfiltered_paired_csv": {
            "path": str((OUT / "paired_sog_candidates_unfiltered.csv").relative_to(ROOT)),
            "sha256": sha(OUT / "paired_sog_candidates_unfiltered.csv"),
        },
        "validation": manifest["validation"],
        "limitations": manifest["limitations"],
    }
    DOC_SUMMARY.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest["outputs"][str(DOC_SUMMARY.relative_to(ROOT))] = sha(DOC_SUMMARY)
    (OUT/"manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(json.dumps({"output":str(OUT.relative_to(ROOT)),"arm_counts":arm_counts,"paired_line_rows":len(paired),"outcomes":dict(statuses),"overall":overall,"overall_deltas":delta_by,"common_book_quotes":quote_overall,"date_deltas":date_deltas,"cold_start_arms":cold_summary},indent=2,sort_keys=True))


if __name__ == "__main__":
    main()
