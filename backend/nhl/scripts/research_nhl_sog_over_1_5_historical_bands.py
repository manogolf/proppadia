#!/usr/bin/env python3
"""Research (P2.43): SOG Over 1.5 model-underdog fair-odds bands on season 2025-26 prices.

Read-only. The current SOG model family (HGB count model, `sog_hgb_features`) is
refit out of sample on the frozen training frame: fold A fits season 2024 and
scores season 2025 games before game 590 (about 2026-01-01); fold B fits season
2024 plus those games and scores the rest. Prices are the archived 7 pm ET
Odds API snapshot per slate (`backend/nhl/exports/odds_history`, US books;
BetOnline stands in for the operator's books). Each Over 1.5 is settled at one
unit at the best price. Output: artifacts/analysis/nhl/price_view/historical_2025_over_1_5/.
"""
import glob, json, os, re, unicodedata
from pathlib import Path
import numpy as np, pandas as pd, psycopg
from dotenv import dotenv_values
from scipy.stats import poisson
from backend.nhl.database_access import database_url
from backend.nhl.sog_hgb_features import BASE_KW, fit, predict
from backend.nhl.scripts import report_nhl_sog_points_price_view as view

f = pd.read_csv('artifacts/analysis/nhl/sog_hgb_count_model/frozen_2026-10-25/training_frame_rebuilt.csv.gz')
f['season'] = f.game_id // 1_000_000; f['gno'] = f.game_id % 10_000
CUT = 590
s24, early, late = f[f.season == 2024], f[(f.season == 2025) & (f.gno < CUT)], f[(f.season == 2025) & (f.gno >= CUT)]
early = early.assign(pred=predict(fit(s24, BASE_KW), early), fold='A: fit 2024-25')
late = late.assign(pred=predict(fit(pd.concat([s24, f[(f.season == 2025) & (f.gno < CUT)]]), BASE_KW), late), fold='B: fit to Dec')
test = pd.concat([early, late])

env = {**os.environ, **dotenv_values('backend/.env')}
with psycopg.connect(database_url('research', environ=env), prepare_threshold=None) as c, c.cursor() as cur:
    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    cur.execute("""SELECT game_id::bigint, game_date, home_team_code, away_team_code FROM nhl.games WHERE season=2025""")
    games = pd.DataFrame(cur.fetchall(), columns=['game_id', 'game_date', 'home', 'away'])
    cur.execute("SELECT player_id::bigint, full_name FROM nhl.players")
    players = pd.DataFrame(cur.fetchall(), columns=['player_id', 'full_name'])
norm = lambda s: re.sub(r'[^a-z ]', '', unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode().lower().replace('-', ' ')).strip()
NICK = {'ducks':'ANA','bruins':'BOS','sabres':'BUF','flames':'CGY','hurricanes':'CAR','blackhawks':'CHI','avalanche':'COL',
 'blue jackets':'CBJ','stars':'DAL','red wings':'DET','oilers':'EDM','panthers':'FLA','kings':'LAK','wild':'MIN','canadiens':'MTL',
 'predators':'NSH','devils':'NJD','islanders':'NYI','rangers':'NYR','senators':'OTT','flyers':'PHI','penguins':'PIT','sharks':'SJS',
 'kraken':'SEA','blues':'STL','lightning':'TBL','maple leafs':'TOR','mammoth':'UTA','hockey club':'UTA','canucks':'VAN',
 'golden knights':'VGK','capitals':'WSH','jets':'WPG'}
def tnorm(s):
    s = norm(s)
    if len(s) == 3: return s.upper()
    return next((code for nick, code in sorted(NICK.items(), key=lambda kv: -len(kv[0])) if s.endswith(nick)), s)
games['key'] = games.game_date.astype(str) + '|' + games.home.map(tnorm) + '|' + games.away.map(tnorm)
test = test.merge(games[['game_id', 'game_date']], on='game_id').merge(players, on='player_id', how='left')
test['nkey'] = test.game_id.astype(str) + '|' + test.full_name.map(norm)

quotes = []
for d in sorted(glob.glob('backend/nhl/exports/odds_history/2025-1*') + glob.glob('backend/nhl/exports/odds_history/2026-0[1-4]*')):
    p = d + '/odds_event_wrappers.json'
    if not os.path.exists(p): continue
    day = os.path.basename(d)
    for w in json.load(open(p)):
        ev = w.get('data') or {}
        key = f"{day}|{tnorm(ev.get('home_team'))}|{tnorm(ev.get('away_team'))}"
        for b in ev.get('bookmakers', []):
            for m in b.get('markets', []):
                if m['key'] not in ('player_shots_on_goal', 'player_shots_on_goal_alternate'): continue
                for o in m['outcomes']:
                    if o.get('point') == 1.5:
                        quotes.append((key, norm(o.get('description')), o['name'].upper(), b['key'], m['key'], o['price']))
q = pd.DataFrame(quotes, columns=['key', 'name', 'side', 'book', 'market', 'american'])
q = q.merge(games[['key', 'game_id']], on='key', how='left')
print('quotes', len(q), 'unmatched game share', round(q.game_id.isna().mean(), 3), 'books', sorted(q.book.unique()))
q = q.dropna(subset=['game_id']); q['nkey'] = q.game_id.astype('int64').astype(str) + '|' + q.name
q['decimal'] = np.where(q.american > 0, 1 + q.american / 100, 1 + 100 / q.american.abs())
q = q[q.side == 'OVER']
rows = test[test.nkey.isin(q.nkey)].copy()
print('model rows', len(test), 'with an Over 1.5 quote', len(rows), 'quoted names matched', round(q.nkey.isin(test.nkey).mean(), 3))
rows['p'] = 1 - poisson.cdf(1, rows.pred)
rows['won'] = rows.shots_on_goal >= 2
out = []
for scope, sub in (('all books', q), ('BetOnline', q[q.book == 'betonlineag'])):
    best = sub.sort_values('decimal').groupby('nkey').tail(1)[['nkey', 'decimal', 'book']]
    r = rows.merge(best, on='nkey')
    r['ev'] = r.p * r.decimal - 1
    r['profit'] = np.where(r.won, r.decimal - 1, -1.0)
    r['fair_band'] = r.p.map(lambda p: view._band(max(view.fair_american(p), 100), view.FOCUS_BANDS) if p < 0.5 else None)
    r['scope'] = scope
    out.append(r)
r = pd.concat(out)
dog = r[r.fair_band.notna()]
order = [b for _, _, b in view.FOCUS_BANDS]
def tab(df):
    t = df.groupby(['scope', 'fair_band'], observed=True).agg(picks=('won', 'size'), wins=('won', 'sum'), win=('won', 'mean'),
        model=('p', 'mean'), ret=('profit', 'mean')).reset_index()
    t['fair_band'] = pd.Categorical(t.fair_band, order); return t.sort_values(['scope', 'fair_band']).round(3)
print('\n== positive EV'); print(tab(dog[dog.ev > 0]).to_string(index=False))
print('\n== any EV'); print(tab(dog).to_string(index=False))
print('\n== positive EV by fold (all books)'); print(dog[(dog.ev > 0) & (dog.scope == 'all books')].groupby(['fold', 'fair_band']).agg(n=('won', 'size'), ret=('profit', 'mean')).round(3).to_string())
OUT = Path('artifacts/analysis/nhl/price_view/historical_2025_over_1_5')
OUT.mkdir(parents=True, exist_ok=True)
r.to_csv(OUT / 'rows.csv.gz', index=False)
tab(dog[dog.ev > 0]).to_csv(OUT / 'positive_ev_bands.csv', index=False)
tab(dog).to_csv(OUT / 'any_ev_bands.csv', index=False)
