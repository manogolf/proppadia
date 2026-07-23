#!/usr/bin/env python3
"""Acquire and verify the official Retrosheet master CSV archive."""
import argparse,csv,hashlib,json,zipfile,shutil
from datetime import datetime,timezone
from pathlib import Path
from urllib.request import Request,urlopen
URL='https://www.retrosheet.org/downloads/csvdownloads.zip'; EXPECT={'allplayers.csv','gameinfo.csv','teamstats.csv','batting.csv','pitching.csv','fielding.csv','plays.csv'}
def shab(b): return hashlib.sha256(b).hexdigest()
def shap(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--out-root',default='backend/mlb/data/external/retrosheet/raw/csv_release_through_2025'); ap.add_argument('--timeout',type=int,default=600); a=ap.parse_args(); out=Path(a.out_root); out.mkdir(parents=True,exist_ok=True); z=out/'csvdownloads.zip'
 ts=datetime.now(timezone.utc).isoformat()
 if not z.exists():
  tmp=z.with_suffix('.zip.partial')
  with urlopen(Request(URL,headers={'User-Agent':'proppadia-authoritative-research/1.0'}),timeout=a.timeout) as r, tmp.open('wb') as f: shutil.copyfileobj(r,f,length=1<<20)
  tmp.replace(z)
 members=[]
 with zipfile.ZipFile(z) as q:
  names=q.namelist(); basenames={Path(n).name for n in names}
  for n in names:
   if n.endswith('/'): continue
   info=q.getinfo(n); dest=out/'extracted'/Path(n).name; dest.parent.mkdir(exist_ok=True)
   if not dest.exists():
    tmp=dest.with_suffix(dest.suffix+'.partial')
    with q.open(n) as src,tmp.open('wb') as dst:shutil.copyfileobj(src,dst,length=1<<20)
    tmp.replace(dest)
   rows=-1; cols=[]
   if dest.suffix.lower()=='.csv':
    with dest.open(errors='replace',newline='') as f: r=csv.reader(f); cols=next(r,[]); rows=sum(1 for _ in r)
   members.append({'member':n,'path':str(dest),'size_bytes':info.file_size,'sha256':shap(dest),'rows':rows,'schema':cols})
 missing=sorted(EXPECT-basenames); m={'source_url':URL,'retrieval_timestamp_utc':ts,'release_evidence':'official archive covering compiled games through 2025','zip_path':str(z),'zip_size':z.stat().st_size,'zip_sha256':shap(z),'members':members,'missing_expected_members':missing,'status':'ACQUIRED_AND_VALIDATED' if not missing else 'SCHEMA_DRIFT'}; (out/'release_manifest.json').write_text(json.dumps(m,indent=2)+'\n'); print(json.dumps({'status':m['status'],'zip_size':z.stat().st_size,'members':len(members),'missing':missing},indent=2))
if __name__=='__main__': main()
