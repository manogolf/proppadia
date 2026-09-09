#!/usr/bin/env python3
import hashlib,json
from pathlib import Path
import pandas as pd
p=Path(__file__).resolve().parent; errors=[]
s=json.loads((p/'summary.json').read_text()); c=pd.read_csv(p/'frozen_joint_strength_cohort.csv')
d=pd.read_csv(p/'date_request_plan.csv'); b=pd.read_csv(p/'frozen_bookmaker_request_list.csv')
r=pd.read_csv(p/'reconciled_bookmaker_price_ledger.csv'); q=pd.read_csv(p/'append_only_request_ledger.csv')
if len(c)!=76 or int(c.evaluated_win.sum())!=56: errors.append('cohort_changed')
if len(d)!=29 or int(d.expected_credit_cost.sum())!=290: errors.append('request_plan_changed')
if len(b)>10 or 'pinnacle' not in set(b.bookmaker_key) or 'betonlineag' not in set(b.bookmaker_key): errors.append('book_list_wrong')
if 'fliff' in set(b.bookmaker_key): errors.append('fliff_wrongly_requested')
if len(r)!=760: errors.append('reconciliation_grid_not_760')
success=q[q.status.eq('SUCCESS')]
if success.request_id.nunique()!=29 or pd.to_numeric(success.x_requests_last).sum()>400: errors.append('acquisition_incomplete_or_over_budget')
if not set(r.admission_class).issubset({'PRICE_UNAVAILABLE','BOOKMAKER_ABSENT','ONE_SIDED_MARKET','STALE_OR_POST_START_PRICE','IDENTITY_MISMATCH','VALID_PREGAME_PRICE'}): errors.append('bad_class')
if s['classification'] not in {'ALTERNATIVE_BOOK_TRANSFER_SUPPORTED','ALTERNATIVE_BOOK_TRANSFER_NOT_SUPPORTED','ALTERNATIVE_BOOK_TRANSFER_INSUFFICIENT'}: errors.append('bad_final_class')
for line in (p/'sha256_manifest.txt').read_text().splitlines():
 expected,name=line.split('  ',1)
 if hashlib.sha256((p/name).read_bytes()).hexdigest()!=expected: errors.append('hash:'+name)
print(json.dumps({'status':'PASS' if not errors else 'FAIL','errors':errors},sort_keys=True)); raise SystemExit(bool(errors))
