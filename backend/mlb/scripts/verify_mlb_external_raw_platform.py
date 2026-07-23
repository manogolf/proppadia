#!/usr/bin/env python3
"""Verify every certified raw file against the frozen acquisition manifest."""
import hashlib,json
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[3];A=ROOT/'artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/2026-07-22';OUT=ROOT/'artifacts/analysis/model_development/mlb_external_batter_event_platform_v1_normalization/2026-07-22'
def sha(p):
 h=hashlib.sha256();
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()
def main():
 rows=[]
 for r in pd.read_csv(A/'raw_file_manifest.csv').itertuples():
  p=ROOT/r.path;actual=sha(p) if p.exists() else '';rows.append({'source':r.source,'path':r.path,'expected_sha256':r.sha256,'actual_sha256':actual,'exists':p.exists(),'hash_match':actual==r.sha256,'size_match':p.exists() and p.stat().st_size==int(r.size_bytes)})
 OUT.mkdir(parents=True,exist_ok=True);d=pd.DataFrame(rows);d.to_csv(OUT/'raw_source_verification.csv',index=False);print(json.dumps({'files':len(d),'failures':int((~(d.hash_match&d.size_match)).sum())},indent=2));raise SystemExit(1 if (~(d.hash_match&d.size_match)).any() else 0)
if __name__=='__main__':main()
