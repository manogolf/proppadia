import argparse, pandas as pd
from infer_common import score

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)       # today's features
    ap.add_argument("--out", required=True)       # where to write preds
    ap.add_argument("--line", type=float, default=0.5)
    ap.add_argument("--models_dir", default="ml/models")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    p  = score(df, kind="runs", line=args.line, models_dir=args.models_dir)

    out = df[["game_id","player_id"]].copy()
    out[f"p_runs_over_{str(args.line).replace('.','_')}"] = p
    out.to_csv(args.out, index=False)
    print(f"Wrote {args.out}")
