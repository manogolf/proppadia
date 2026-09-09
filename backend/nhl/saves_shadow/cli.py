"""CLI for the immutable NHL Saves conditional-start shadow."""
from __future__ import annotations

import argparse
from pathlib import Path

from .core import grade_shadow, run_shadow, verify_historical_parity, verify_operational_amendment, verify_policy_c_historical_fixture


def main() -> int:
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest="command",required=True)
    sub.add_parser("verify")
    run=sub.add_parser("run")
    for name in ["game-spine-csv","game-spine-manifest","goalie-inputs-csv","goalie-inputs-manifest","quote-run-dir","output-root"]:
        run.add_argument(f"--{name}",type=Path,required=True)
    run.add_argument("--slate-date",required=True);run.add_argument("--run-timestamp-utc",required=True);run.add_argument("--run-type",choices=["MIDDAY","FINAL_PREGAME"],required=True)
    grade=sub.add_parser("grade")
    grade.add_argument("--shadow-run-dir",type=Path,required=True);grade.add_argument("--outcomes-csv",type=Path,required=True);grade.add_argument("--output-root",type=Path,required=True);grade.add_argument("--correction-reason")
    args=parser.parse_args()
    if args.command=="verify": print({"historical":verify_historical_parity(),"amendment":verify_operational_amendment(),"policy_c":verify_policy_c_historical_fixture()})
    elif args.command=="run": print(run_shadow(**{k:v for k,v in vars(args).items() if k!="command"}))
    else: print(grade_shadow(**{k:v for k,v in vars(args).items() if k!="command"}))
    return 0


if __name__=="__main__": raise SystemExit(main())
