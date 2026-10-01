#!/usr/bin/env python3
"""
Build Goalie SAVES CSV with market columns.

Inputs
------
--pred       backend/nhl/data/processed/saves_predictions.csv   (wide: p_over_18.5,...)
--names      exports/train_goalie_saves_v2.csv                  (has full_name, game_date, ids)
--odds-json  explicit immutable observation raw_response.json   (optional)
--out        nhl/site/data/saves_with_market.csv
--unmatched  nhl/site/data/unmatched_saves.csv

Env
---
SLATE_DATE=YYYY-MM-DD  (required; Pacific operational date)

Output columns:
full_name, player_id, game_id, team_id, line, p_over,
price_over, p_over_mkt, edge_over, fair_over, game_date
"""
from __future__ import annotations
import argparse, json, math, os, sys, re, uuid
from pathlib import Path
import pandas as pd
import unicodedata as ud

from backend.nhl.attachment_integrity import (
    AttachmentIntegrityError,
    canonical_attachment_keys,
    stable_candidate_identity,
    validate_attachment_frame,
    validate_odds_observation,
)
from backend.nhl.daily_capture import canonical_game_set_hash, sha256_file

# -------------------- util --------------------

def die(msg: str, code: int = 2):
    print(f"[saves_with_market] FATAL: {msg}", file=sys.stderr)
    sys.exit(code)

def norm_name(s: str) -> str:
    """Deterministic comparison form for accents, punctuation, and suffixes."""
    if not isinstance(s, str):
        return ""
    s = ud.normalize("NFKD", s)
    s = "".join(ch for ch in s if ud.category(ch) != "Mn")  # strip accents
    s = s.replace("’", "'").replace("-", " ").replace("'", "").replace(".", "")
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    parts = s.split()
    while parts and parts[-1] in {"jr", "sr", "ii", "iii", "iv", "v"}:
        parts.pop()
    return " ".join(parts)


def aliases_for_name(full_name: str) -> list[dict[str, object]]:
    """Return ranked aliases; rank can only choose equivalent matches."""
    base = norm_name(full_name)
    if not base:
        return []
    aliases: list[dict[str, object]] = [
        {"alias_value": str(full_name).strip(), "alias_type": "AUTHORITATIVE_FULL_NAME",
         "alias_rank": 0, "normalized_alias": base},
        {"alias_value": base, "alias_type": "NORMALIZED_FULL_NAME",
         "alias_rank": 1, "normalized_alias": base},
    ]
    parts = base.split()
    if len(parts) >= 2:
        aliases.append({
            "alias_value": f"{parts[0][0]} {parts[-1]}",
            "alias_type": "INITIAL_LAST", "alias_rank": 2,
            "normalized_alias": f"{parts[0][0]} {parts[-1]}",
        })
    return aliases

def line_key(x) -> str:
    """Canonical text key for a line. Matches the site’s dropdown keys."""
    try:
        v = float(x)
    except Exception:
        return ""
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))        # 24
    return f"{round(v, 1)}"              # 24.5

def american_to_prob(a) -> float:
    try:
        A = float(a)
    except Exception:
        return float("nan")
    if not math.isfinite(A) or A == 0:
        return float("nan")
    return 100.0 / (A + 100.0) if A > 0 else (-A) / ((-A) + 100.0)

def prob_to_american(p) -> str:
    if not (isinstance(p,(int,float)) and 0 < p < 1):
        return ""
    return f"-{round((p/(1-p))*100)}" if p >= 0.5 else f"+{round(((1-p)/p)*100)}"

def read_csv_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        die(f"missing CSV: {path}")
    try:
        return pd.read_csv(path)
    except Exception as e:
        die(f"failed reading CSV {path}: {e}")

def load_odds_json(path: Path | None) -> list | dict | None:
    # Explicit run-bound input only; mutable compatibility files must never be
    # an implicit fallback for a new prediction run.
    for p in ([path] if path is not None else []):
        if p and p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    return None

# -------------------- shaping --------------------

def melt_preds_wide_to_long(pred: pd.DataFrame) -> pd.DataFrame:
    """Expect columns: player_id, game_id, and p_over_XX[.5] columns."""
    need = [c for c in ["player_id","game_id"] if c not in pred.columns]
    if need:
        die(f"pred file missing columns: {need}")

    # detect p_over_* columns
    pat = re.compile(r"^p_over_(\d+(?:[._]\d+)?)$")
    pcols = [c for c in pred.columns if pat.match(str(c))]
    if not pcols:
        die("pred file lacks p_over_* probability columns (e.g., p_over_18_5, p_over_24.5)")

    # melt → long
    long = pred.melt(id_vars=["player_id","game_id"], value_vars=pcols,
                     var_name="pcol", value_name="p_over")

    # extract numeric line from pcol
    def parse_line(s: str) -> float:
        s = s.replace("p_over_", "").replace("_", ".")
        try:
            return float(s)
        except Exception:
            return float("nan")

    long["line"] = long["pcol"].map(parse_line).astype(float)
    long = long.drop(columns=["pcol"])
    # drop rows where p_over is NaN
    long = long[pd.to_numeric(long["p_over"], errors="coerce").notna()].copy()
    return long

def parse_odds_candidates(raw) -> pd.DataFrame:
    """Return provider identities and ranked lookup aliases without expanding predictions."""
    if raw is None:
        return pd.DataFrame(columns=[
            "normalized_alias", "provider_player_identity", "provider_player_name",
            "market_identity", "line_str", "price_over", "price_under",
            "source_quote_count_over", "source_quote_count_under",
            "source_books_over", "source_books_under",
        ])
    quotes: list[dict[str, object]] = []

    def walk(x, *, event_id: str = "", bookmaker_key: str = ""):
        if isinstance(x, dict):
            next_event_id = event_id
            next_bookmaker_key = bookmaker_key
            if "commence_time" in x and x.get("id") is not None:
                next_event_id = str(x.get("id"))
            if "markets" in x and x.get("key") is not None:
                next_bookmaker_key = str(x.get("key"))
            if x.get("key") == "player_total_saves":
                for o in x.get("outcomes",[]) or []:
                    side = str(o.get("name") or "").strip().lower()
                    if side not in {"over", "under"}:
                        continue
                    base_name = (o.get("description") or o.get("player") or "").strip()
                    pt = o.get("point")
                    pr = o.get("price")
                    provider_identity = norm_name(base_name)
                    if provider_identity and (pt is not None) and (pr is not None):
                        quotes.append({
                            "event_id": next_event_id,
                            "bookmaker_key": next_bookmaker_key,
                            "provider_player_identity": provider_identity,
                            "provider_player_name": base_name,
                            "line_str": line_key(pt),
                            "side": side,
                            "price": float(pr),
                        })
            for v in x.values():
                walk(v, event_id=next_event_id, bookmaker_key=next_bookmaker_key)
        elif isinstance(x, list):
            for it in x:
                walk(it, event_id=event_id, bookmaker_key=bookmaker_key)
    walk(raw)
    if not quotes:
        return pd.DataFrame(columns=[
            "normalized_alias", "provider_player_identity", "provider_player_name",
            "market_identity", "line_str", "price_over", "price_under",
            "source_quote_count_over", "source_quote_count_under",
            "source_books_over", "source_books_under",
        ])
    quote_frame = pd.DataFrame(quotes)
    grouped = quote_frame.groupby(
        ["event_id", "provider_player_identity", "line_str", "side"],
        as_index=False, dropna=False,
    ).agg(
        price=("price", "median"),
        provider_player_name=("provider_player_name", "first"),
        source_quote_count=("price", "size"),
        source_books=("bookmaker_key", lambda values: "|".join(sorted(set(values)))),
    )
    indices = ["event_id", "provider_player_identity", "line_str"]
    prices = grouped.pivot(index=indices, columns="side", values="price")
    counts = grouped.pivot(index=indices, columns="side", values="source_quote_count")
    books = grouped.pivot(index=indices, columns="side", values="source_books")
    names = quote_frame.groupby(indices, as_index=True).provider_player_name.first()
    markets = prices.rename(columns={"over": "price_over", "under": "price_under"})
    markets["source_quote_count_over"] = counts.get("over")
    markets["source_quote_count_under"] = counts.get("under")
    markets["source_books_over"] = books.get("over")
    markets["source_books_under"] = books.get("under")
    markets["provider_player_name"] = names
    markets = markets.reset_index()
    candidates: list[dict[str, object]] = []
    for row in markets.to_dict("records"):
        market_identity = stable_candidate_identity([
            row["event_id"], row["provider_player_identity"], row["line_str"],
        ])
        for alias in aliases_for_name(str(row["provider_player_name"])):
            candidates.append({
                **alias,
                "provider_player_identity": row["provider_player_identity"],
                "provider_player_name": row["provider_player_name"],
                "market_identity": market_identity,
                "line_str": row["line_str"],
                "price_over": row["price_over"],
                "price_under": row.get("price_under"),
                "source_quote_count_over": row.get("source_quote_count_over"),
                "source_quote_count_under": row.get("source_quote_count_under"),
                "source_books_over": row.get("source_books_over"),
                "source_books_under": row.get("source_books_under"),
            })
    return pd.DataFrame(candidates)


def build_match_candidates(predictions: pd.DataFrame, odds: pd.DataFrame) -> pd.DataFrame:
    """Build the many-row alias search relation without changing prediction grain."""
    rows: list[dict[str, object]] = []
    for prediction_index, full_name, line in predictions[["full_name", "line"]].itertuples():
        for alias in aliases_for_name(full_name):
            rows.append({"prediction_index": prediction_index, "line_str": line_key(line), **alias})
    aliases = pd.DataFrame(rows)
    if aliases.empty or odds.empty:
        return pd.DataFrame(columns=[
            "prediction_index", "alias_value", "alias_type", "alias_rank",
            "normalized_alias", "provider_player_identity", "provider_player_name",
            "market_identity", "line_str", "price_over", "price_under",
            "source_quote_count_over", "source_quote_count_under",
            "source_books_over", "source_books_under",
        ])
    return aliases.merge(odds, on=["normalized_alias", "line_str"], how="inner",
                         suffixes=("_prediction", "_odds"))


def reduce_match_candidates(
    predictions: pd.DataFrame, candidates: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reduce candidate matches to exactly one decision per prediction row."""
    result = predictions.copy()
    result["attachment_status"] = "UNMATCHED"
    result["price_over"] = pd.NA
    result["price_under"] = pd.NA
    result["source_quote_count_over"] = pd.NA
    result["source_quote_count_under"] = pd.NA
    result["source_books_over"] = ""
    result["source_books_under"] = ""
    result["matched_alias_type"] = ""
    result["matched_alias_value"] = ""
    result["matched_provider_player_identity"] = ""
    result["matched_market_identity"] = ""
    result["alias_candidate_count"] = 0
    result["distinct_market_candidate_count"] = 0
    result["matched_alias_types"] = ""
    ambiguous_rows: list[dict[str, object]] = []
    if candidates.empty:
        return result, pd.DataFrame(columns=list(candidates.columns))

    for prediction_index, group in candidates.groupby("prediction_index", sort=False):
        exact = group.drop_duplicates(subset=["market_identity", "price_over", "price_under"]).copy()
        result.loc[prediction_index, "alias_candidate_count"] = len(group)
        result.loc[prediction_index, "distinct_market_candidate_count"] = len(exact)
        if len(exact) == 1:
            equivalent = group[
                (group["market_identity"] == exact.iloc[0]["market_identity"])
                & (group["price_over"].eq(exact.iloc[0]["price_over"]) | (group["price_over"].isna() & pd.isna(exact.iloc[0]["price_over"])))
                & (group["price_under"].eq(exact.iloc[0]["price_under"]) | (group["price_under"].isna() & pd.isna(exact.iloc[0]["price_under"])))
            ].sort_values(["alias_rank_prediction", "alias_type_prediction", "alias_value_prediction"])
            selected = equivalent.iloc[0]
            result.loc[prediction_index, "attachment_status"] = "MATCHED"
            result.loc[prediction_index, "price_over"] = selected["price_over"]
            result.loc[prediction_index, "price_under"] = selected["price_under"]
            for column in ("source_quote_count_over", "source_quote_count_under", "source_books_over", "source_books_under"):
                result.loc[prediction_index, column] = selected.get(column, pd.NA)
            result.loc[prediction_index, "matched_alias_type"] = selected["alias_type_prediction"]
            result.loc[prediction_index, "matched_alias_value"] = selected["alias_value_prediction"]
            result.loc[prediction_index, "matched_provider_player_identity"] = selected["provider_player_identity"]
            result.loc[prediction_index, "matched_market_identity"] = selected["market_identity"]
            result.loc[prediction_index, "matched_alias_types"] = "|".join(
                sorted(set(equivalent["alias_type_prediction"].astype(str))))
            continue
        result.loc[prediction_index, "attachment_status"] = "AMBIGUOUS_ALIAS_MATCH"
        for row in exact.sort_values(
            ["market_identity", "price_over", "alias_rank_prediction"]
        ).to_dict("records"):
            ambiguous_rows.append({**row, "prediction_index": prediction_index})
    ambiguous_columns = list(candidates.columns)
    ambiguous = pd.DataFrame(ambiguous_rows, columns=ambiguous_columns)
    return result, ambiguous

# -------------------- main --------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True)
    ap.add_argument("--names", required=True)
    ap.add_argument("--odds-json", default=None)
    ap.add_argument("--out", required=True)
    ap.add_argument("--unmatched", required=True)
    ap.add_argument("--ambiguous", default=None)
    ap.add_argument("--integrity-report", default=None)
    ap.add_argument("--strict-current-run", action="store_true")
    ap.add_argument("--parent-run-id", default=None)
    ap.add_argument("--expected-pred-sha256", default=None)
    ap.add_argument("--odds-observation-dir", default=None)
    ap.add_argument("--expected-odds-manifest-sha256", default=None)
    ap.add_argument("--odds-season", type=int, default=None)
    ap.add_argument("--odds-phase", default=None)
    ap.add_argument("--odds-replayed", action="store_true")
    args = ap.parse_args()

    slate = os.environ.get("SLATE_DATE")
    if not slate:
        die("SLATE_DATE env is required (ET YYYY-MM-DD)")

    out_path = Path(args.out); out_path.parent.mkdir(parents=True, exist_ok=True)
    unmatched_path = Path(args.unmatched); unmatched_path.parent.mkdir(parents=True, exist_ok=True)
    ambiguous_path = Path(args.ambiguous or out_path.with_name("ambiguous_saves_alias_matches.csv"))
    report_path = Path(args.integrity_report or out_path.with_name("saves_attachment_integrity.json"))
    pred_path = Path(args.pred)

    if args.strict_current_run:
        required = {
            "parent-run-id": args.parent_run_id,
            "expected-pred-sha256": args.expected_pred_sha256,
            "integrity-report": args.integrity_report,
            "ambiguous": args.ambiguous,
        }
        missing = sorted(name for name, value in required.items() if not value)
        if missing:
            raise AttachmentIntegrityError(
                f"STRICT_CURRENT_RUN_ARGUMENTS_MISSING:{','.join(missing)}")
        if sha256_file(pred_path) != args.expected_pred_sha256:
            raise AttachmentIntegrityError("PREDICTION_ARTIFACT_HASH_MISMATCH")

    # --- load & reshape predictions ---
    pred_wide = read_csv_required(pred_path)
    if args.strict_current_run:
        if "parent_daily_run_id" not in pred_wide.columns:
            raise AttachmentIntegrityError("PREDICTION_PARENT_RUN_ID_MISSING")
        parent_ids = set(pred_wide["parent_daily_run_id"].fillna("").astype(str))
        if parent_ids != {args.parent_run_id}:
            raise AttachmentIntegrityError("PREDICTION_PARENT_RUN_ID_MISMATCH")
    long = melt_preds_wide_to_long(pred_wide)   # player_id, game_id, line, p_over
    expected_prediction_count = len(long)

    carry_cols = [column for column in [
        "player_id", "game_id", "full_name", "team_id", "game_date",
    ] if column in pred_wide.columns]
    carry = pred_wide[carry_cols].drop_duplicates(subset=["player_id", "game_id"])
    long = long.merge(carry, on=["player_id", "game_id"], how="left")

    # Names are enrichment only; they cannot create prediction rows.
    names = read_csv_required(Path(args.names))
    keep = [c for c in ["player_id","game_id","team_id","full_name","game_date"] if c in names.columns]
    names = names[keep].drop_duplicates().copy()

    keys = [k for k in ["player_id","game_id"] if k in long.columns and k in names.columns]
    if not keys:
        die("cannot merge names: missing both player_id and game_id in one of the files")
    conflicting_name_keys = names.groupby(keys, dropna=False).agg(
        full_name_count=("full_name", lambda values: values.dropna().astype(str).nunique())
    ) if "full_name" in names.columns else pd.DataFrame()
    if not conflicting_name_keys.empty and (conflicting_name_keys["full_name_count"] > 1).any():
        raise AttachmentIntegrityError("NAMES_ENRICHMENT_IDENTITY_CONFLICT")
    names = names.drop_duplicates(subset=keys, keep="first")
    df = long.merge(names, on=keys, how="left", suffixes=("_pred", "_names"))
    for column in ("full_name", "team_id", "game_date"):
        pred_column, names_column = f"{column}_pred", f"{column}_names"
        if pred_column in df.columns and names_column in df.columns:
            df[column] = df[pred_column].combine_first(df[names_column])
        elif pred_column in df.columns:
            df[column] = df[pred_column]
        elif names_column in df.columns:
            df[column] = df[names_column]

    if "game_date" in df.columns:
        df = df[df["game_date"].astype(str) == slate].copy()
    else:
        raise AttachmentIntegrityError("PREDICTION_GAME_DATE_MISSING")
    df = df.reset_index(drop=True)
    if args.strict_current_run and len(df) != expected_prediction_count:
        raise AttachmentIntegrityError(
            "PREDICTION_ROWS_LOST_DURING_NAME_ENRICHMENT_OR_SLATE_FILTER")

    prediction_frame = df[["game_date", "game_id", "player_id", "line"]].copy()
    prediction_keys = canonical_attachment_keys(prediction_frame)
    if len(prediction_keys) != len(set(prediction_keys)):
        raise AttachmentIntegrityError("PREDICTION_NATURAL_KEY_DUPLICATE")

    odds_lineage: dict[str, object] = {}
    odds_raw = load_odds_json(Path(args.odds_json) if args.odds_json else None)
    if args.odds_json:
        if args.strict_current_run and not (
            args.odds_observation_dir and args.expected_odds_manifest_sha256
        ):
            raise AttachmentIntegrityError("STRICT_CURRENT_RUN_ODDS_LINEAGE_MISSING")
        if args.odds_observation_dir:
            odds_lineage = validate_odds_observation(
                observation_dir=Path(args.odds_observation_dir),
                odds_json=Path(args.odds_json),
                expected_manifest_sha256=args.expected_odds_manifest_sha256,
                expected_parent_daily_run_id=args.parent_run_id,
                expected_slate_date=slate,
                expected_season=args.odds_season,
                expected_phase=args.odds_phase,
                expected_game_set_hash=canonical_game_set_hash(
                    pd.to_numeric(pred_wide["game_id"], errors="raise").astype(int)),
                replayed=args.odds_replayed,
            )
    odds_candidates = parse_odds_candidates(odds_raw)
    candidates = build_match_candidates(df, odds_candidates)
    df, ambiguous = reduce_match_candidates(df, candidates)

    df["parent_daily_run_id"] = args.parent_run_id or ""
    df["prediction_artifact_sha256"] = args.expected_pred_sha256 or sha256_file(pred_path)
    df["odds_observation_manifest_sha256"] = odds_lineage.get(
        "odds_observation_manifest_sha256", "")
    df["odds_raw_response_sha256"] = odds_lineage.get("odds_raw_response_sha256", "")

    # compute market prob, edge, fair odds
    df["p_over"] = pd.to_numeric(df["p_over"], errors="coerce")
    df["p_over_mkt"] = df["price_over"].map(american_to_prob)
    df["p_under"] = 1.0 - df["p_over"]
    df["p_under_mkt"] = df["price_under"].map(american_to_prob)

    def edge(a, b):
        if isinstance(a, float) and isinstance(b, float) and math.isfinite(a) and math.isfinite(b):
            return a - b
        return float("nan")

    df["edge_over"] = [edge(a,b) for a,b in zip(df["p_over"], df["p_over_mkt"])]
    df["edge_under"] = [edge(a,b) for a,b in zip(df["p_under"], df["p_under_mkt"])]
    df["fair_over"] = df["p_over"].map(prob_to_american)
    df["fair_under"] = df["p_under"].map(prob_to_american)

    unmatched = df[df["attachment_status"] != "MATCHED"].copy()
    unmatched_cols = [c for c in ["full_name","player_id","game_id","team_id","line","p_over","game_date"] if c in df.columns]

    out_cols = [c for c in [
        "full_name","player_id","game_id","team_id",
        "line","p_over","p_under","price_over","price_under","p_over_mkt","p_under_mkt",
        "edge_over","edge_under","fair_over","fair_under","source_quote_count_over",
        "source_quote_count_under","source_books_over","source_books_under","game_date",
        "attachment_status", "matched_alias_type", "matched_alias_value",
        "matched_provider_player_identity", "matched_market_identity", "alias_candidate_count",
        "distinct_market_candidate_count", "matched_alias_types",
        "parent_daily_run_id", "prediction_artifact_sha256",
        "odds_observation_manifest_sha256", "odds_raw_response_sha256",
    ] if c in df.columns]
    output = df[out_cols].copy()
    expected_odds_manifest = (
        args.expected_odds_manifest_sha256 if args.odds_json else None
    )
    validation = validate_attachment_frame(
        prediction_frame=prediction_frame,
        attachment_frame=output,
        expected_parent_daily_run_id=args.parent_run_id if args.strict_current_run else None,
        expected_prediction_sha256=args.expected_pred_sha256 if args.strict_current_run else None,
        expected_odds_manifest_sha256=expected_odds_manifest,
    )

    token = uuid.uuid4().hex
    staged_out = out_path.with_name(f".{out_path.name}.{token}.tmp")
    staged_unmatched = unmatched_path.with_name(f".{unmatched_path.name}.{token}.tmp")
    staged_ambiguous = ambiguous_path.with_name(f".{ambiguous_path.name}.{token}.tmp")
    staged_report = report_path.with_name(f".{report_path.name}.{token}.tmp")
    output.to_csv(staged_out, index=False)
    unmatched[unmatched_cols].to_csv(staged_unmatched, index=False)
    ambiguous.to_csv(staged_ambiguous, index=False)
    report = {
        "schema_version": "NHL_ATTACHMENT_INTEGRITY_V1",
        "lane": "saves",
        "status": "PASS",
        "canonical_key_columns": ["game_date", "game_id", "player_id", "line"],
        "parent_daily_run_id": args.parent_run_id,
        "prediction_artifact_path": str(pred_path.resolve()),
        "prediction_artifact_sha256": args.expected_pred_sha256 or sha256_file(pred_path),
        "attachment_path": str(out_path.resolve()),
        "attachment_sha256": sha256_file(staged_out),
        "unmatched_sha256": sha256_file(staged_unmatched),
        "ambiguous_inventory_sha256": sha256_file(staged_ambiguous),
        **odds_lineage,
        **validation,
    }
    staged_report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for staged, final in (
        (staged_out, out_path), (staged_unmatched, unmatched_path),
        (staged_ambiguous, ambiguous_path), (staged_report, report_path),
    ):
        staged.replace(final)

    # logs
    kept = len(output)
    matched = int(output["attachment_status"].eq("MATCHED").sum())
    ambiguous_count = int(output["attachment_status"].eq("AMBIGUOUS_ALIAS_MATCH").sum())
    lines_present = sorted(df["line"].dropna().unique().tolist())
    print(f"[saves_with_market] filter SLATE_DATE={slate}: kept {kept}")
    print(f"[saves_with_market] rows={kept}  matched_prices={matched}/{kept}")
    print(f"[saves_with_market] ambiguous_alias_matches={ambiguous_count}")
    print(f"[saves_with_market] lines present: {lines_present}")
    print(f"[saves_with_market] ✅ wrote: {out_path}")
    print(f"[saves_with_market]     unmatched: {unmatched_path}  rows={len(unmatched)}")

if __name__ == "__main__":
    main()
