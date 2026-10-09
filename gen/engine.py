"""Frozen prospective public-Spot screening. No exchange accounts or orders."""
from __future__ import annotations
import argparse,concurrent.futures,contextlib,datetime as dt,fcntl,gzip,hashlib,json,math,pathlib,re,sqlite3,statistics,time,urllib.parse,urllib.request,uuid
from decimal import Decimal,ROUND_DOWN
from zoneinfo import ZoneInfo
ROOT=pathlib.Path(__file__).resolve().parent
MINUTE=60000; WARM=7*1440+65
BASE='https://data-api.binance.vision/api/v3/'
ALLOWED={'time','exchangeInfo','ticker/24hr','klines','depth'}
D=Decimal

def utc(t):return dt.datetime.fromtimestamp(t/1000,dt.timezone.utc).isoformat()
def nz(t):return dt.datetime.fromtimestamp(t/1000,dt.timezone.utc).astimezone(ZoneInfo('Pacific/Auckland')).isoformat()
def clock_ms():return int(time.time()*1000)
def digest(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def config():return json.loads((ROOT/'config.json').read_text())
def connect(path=None):
 c=sqlite3.connect(path or ROOT/'state.sqlite',timeout=1);c.row_factory=sqlite3.Row
 c.executescript('''
 CREATE TABLE IF NOT EXISTS bars(symbol TEXT,t INTEGER,raw TEXT,received INTEGER,PRIMARY KEY(symbol,t));
 CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY,v TEXT);
 CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,model TEXT,symbol TEXT,signal INTEGER,due INTEGER,deadline INTEGER,feature TEXT,status TEXT,reason TEXT);
 CREATE TABLE IF NOT EXISTS accounts(k TEXT PRIMARY KEY,model TEXT,capital INTEGER,slip INTEGER,cash REAL,fees REAL,entries INTEGER,peak REAL,halt INTEGER);
 CREATE TABLE IF NOT EXISTS positions(account TEXT,symbol TEXT,qty TEXT,mid REAL,entered INTEGER,due INTEGER,event TEXT,PRIMARY KEY(account,symbol));
 CREATE TABLE IF NOT EXISTS fills(id TEXT PRIMARY KEY,account TEXT,event TEXT,side TEXT,symbol TEXT,ts INTEGER,qty TEXT,price REAL,fee REAL,shortfall REAL,reason TEXT,book TEXT);
 CREATE TABLE IF NOT EXISTS marks(account TEXT,ts INTEGER,equity REAL,deployed REAL,fees REAL,stale TEXT,PRIMARY KEY(account,ts));
 CREATE TABLE IF NOT EXISTS issues(id INTEGER PRIMARY KEY,ts INTEGER,kind TEXT,detail TEXT);
 ''');return c

def getkv(c,k,default=None):
 row=c.execute('SELECT v FROM kv WHERE k=?',(k,)).fetchone();return json.loads(row[0]) if row else default

def setkv(c,k,v):c.execute('INSERT INTO kv VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v',(k,json.dumps(v,allow_nan=False)))
def issue(c,kind,detail):c.execute('INSERT INTO issues(ts,kind,detail) VALUES(?,?,?)',(clock_ms(),kind,str(detail)))

def request(endpoint,params=None):
 if endpoint not in ALLOWED:raise ValueError('Public endpoint not allowed')
 params=params or {};url=BASE+endpoint+('?' +urllib.parse.urlencode(params) if params else '')
 before=time.monotonic();started=clock_ms();rec={'url':url,'started':started};obj=None
 try:
  with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'Spot-alert-comparison/1'}),timeout=config()['request_timeout_seconds']) as resp:
   if resp.status!=200 or resp.geturl()!=url:raise ValueError('Unexpected status/redirect')
   body=resp.read(16000001)
   if len(body)>16000000:raise ValueError('Response size cap')
  obj=json.loads(body);rec.update(raw=body.decode(),sha256=hashlib.sha256(body).hexdigest(),status=200)
 except Exception as ex:rec['error']=type(ex).__name__+': '+str(ex)
 rec['received']=clock_ms();rec['duration']=time.monotonic()-before
 return obj,rec

def validate_bar(x):
 if not isinstance(x,list) or len(x)!=12:raise ValueError('Kline schema')
 if not isinstance(x[0],int) or x[0]%MINUTE or x[6]!=x[0]+MINUTE-1:raise ValueError('Minute timestamp')
 values=[float(x[i]) for i in [1,2,3,4,5,7,10]]
 if not all(math.isfinite(v) for v in values) or min(values[:4])<=0 or min(values[4:])<0:raise ValueError('Nonfinite/nonpositive OHLCV')
 o,h,l,cl=values[:4]
 if not l<=min(o,cl)<=max(o,cl)<=h or float(x[10])>float(x[7])+1e-7:raise ValueError('Invalid OHLC/turnover')

def store_bars(c,symbol,rows,received,closed_before):
 for x in rows:
  validate_bar(x)
  if x[6]>=closed_before:continue
  raw=json.dumps(x,separators=(',',':'));old=c.execute('SELECT raw FROM bars WHERE symbol=? AND t=?',(symbol,x[0])).fetchone()
  if old and old[0]!=raw:raise ValueError('Candle changed: '+symbol+' '+str(x[0]))
  c.execute('INSERT OR IGNORE INTO bars VALUES(?,?,?,?)',(symbol,x[0],raw,received))

def feature_series(rows,cfg):
 q=[0.0];v=[0.0]
 for x in rows:q.append(q[-1]+float(x[7]));v.append(v[-1]+float(x[5]))
 def at(i):
  if i<WARM-1:return None
  w=rows[i-WARM+1:i+1]
  if any(y[0]-x[0]!=MINUTE for x,y in zip(w,w[1:])):return None
  turn=q[i+1]-q[i-59];base=(q[i-59]-q[i-1499])/24
  seasonal=statistics.median(q[i+1-k*1440]-q[i-59-k*1440] for k in range(1,8))
  qty=v[i+1]-v[i-59]
  if base<=0 or seasonal<=0 or qty<=0:return None
  ret=float(rows[i][4])/float(rows[i-60][4])-1
  high=max(float(x[2]) for x in rows[i-1499:i-59])
  f={'return_1h':ret,'rv24':turn/base,'rv_seasonal':turn/seasonal,'prior_high':high,'close':float(rows[i][4]),'hour_vwap':turn/qty,'hour_quote':turn}
  a0=ret>=cfg['price_return_threshold'] and f['rv24']>=cfg['relative_volume_threshold']
  a1=a0 and f['close']>high
  a2=ret>=cfg['price_return_threshold'] and f['rv_seasonal']>=cfg['relative_volume_threshold'] and f['close']>high
  f['qualifies']={'A0':a0,'A1':a1,'A2':a2};return f
 return at

def pick_universe(info,tickers,cfg):
 ticker={x['symbol']:x for x in tickers};out=[]
 for x in info['symbols']:
  s=x['symbol'];t=ticker.get(s)
  if x['quoteAsset']!='USDT' or x['status']!='TRADING' or not x.get('isSpotTradingAllowed') or x['baseAsset'] in cfg['excluded_bases'] or not re.fullmatch('[A-Z0-9]{2,40}',s) or not t:continue
  vol=float(t.get('quoteVolume',0))
  if math.isfinite(vol) and vol>=cfg['minimum_ticker_quote_volume']:out.append((vol,s,x))
 return [x for _,_,x in sorted(out,key=lambda v:(-v[0],v[1]))[:cfg['universe_top_n']]]

def save_raw(records,label):
 (ROOT/'raw').mkdir(exist_ok=True)
 name=f'{clock_ms()}-{label}-{uuid.uuid4().hex[:8]}.json.gz';p=ROOT/'raw'/name
 with gzip.open(p,'wt') as fh:json.dump(records,fh,separators=(',',':'))
 return 'raw/'+name

def load_freeze():
 f=json.loads((ROOT/'FREEZE.json').read_text())
 for name,sha in f['hashes'].items():
  if digest(ROOT/name)!=sha:raise ValueError('Frozen file changed: '+name)
 return f

def book_quote(raw,meta,budget,cfg):
 """Rounded affordable quantity and haircut costs, all Decimal arithmetic."""
 def levels(side):
  a=[(D(p),D(q)) for p,q in raw[side]]
  if not a or any(not p.is_finite() or not q.is_finite() or p<=0 or q<=0 for p,q in a):raise ValueError('Bad book')
  prices=[p for p,_ in a]
  if len(set(prices))!=len(prices) or prices!=sorted(prices,reverse=side=='bids'):raise ValueError('Book ordering')
  return a
 asks,bids=levels('asks'),levels('bids')
 if bids[0][0]>=asks[0][0]:raise ValueError('Crossed/locked book')
 mid=(asks[0][0]+bids[0][0])/2;filters={x['filterType']:x for x in meta['filters']}
 lot=filters['LOT_SIZE'];step=D(lot['stepSize']);fee=D(str(cfg['fee']));hair=D(str(cfg['book_haircut']))
 if step<=0:raise ValueError('Bad step')
 # Quantity anchored to worst scenario price so all variants share a valid cash ceiling.
 px=mid*(1+D(max(cfg['slippage_bps']))/10000)
 qty=(D(str(budget))/(px*(1+fee))/step).to_integral_value(rounding=ROUND_DOWN)*step
 if qty<=0 or qty<D(lot['minQty']) or qty>D(lot['maxQty']):raise ValueError('Lot size')
 def walk(a,q):
  left=q;value=D(0)
  for p,available in a:
   take=min(left,available*hair);value+=take*p;left-=take
   if left==0:return value/q
  raise ValueError('Insufficient depth')
 buy,sell=walk(asks,qty),walk(bids,qty)
 for kind in ['NOTIONAL','MIN_NOTIONAL']:
  if kind in filters:
   x=filters[kind]
   if min(qty*buy,qty*sell)<D(x['minNotional']):raise ValueError('Minimum notional')
   if kind=='NOTIONAL' and qty*max(buy,sell)>D(x['maxNotional']):raise ValueError('Maximum notional')
 bc=(buy/mid-1)*10000;sc=(1-sell/mid)*10000
 if max(bc,sc)>D(str(cfg['maximum_book_cost_bps'])):raise ValueError('Visible book cost gate')
 return {'mid':float(mid),'quantity':str(qty),'buy_bps':float(bc),'sell_bps':float(sc)}

def sell_quote(raw,quantity,cfg,meta=None):
 # Cost/fee gates never block containment; exchange lot/notional constraints still apply.
 asks=[(D(p),D(q)) for p,q in raw['asks']];bids=[(D(p),D(q)) for p,q in raw['bids']]
 if not asks or not bids or any(not p.is_finite() or not q.is_finite() or p<=0 or q<=0 for p,q in asks+bids) or bids[0][0]>=asks[0][0]:raise ValueError('Invalid exit book')
 prices=[p for p,_ in bids]
 if len(set(prices))!=len(prices) or prices!=sorted(prices,reverse=True):raise ValueError('Unsorted exit book')
 mid=(asks[0][0]+bids[0][0])/2;left=D(quantity);value=D(0)
 for p,q in bids:
  take=min(left,q*D(str(cfg['book_haircut'])));value+=take*p;left-=take
  if left==0:break
 if left:raise ValueError('Insufficient exit depth; position stays unresolved')
 if meta:
  fs={x['filterType']:x for x in meta['filters']};lot=fs['LOT_SIZE'];qty=D(quantity)
  if qty<D(lot['minQty']) or qty>D(lot['maxQty']) or qty%D(lot['stepSize']):raise ValueError('Exit lot filter unresolved')
  for kind in ['NOTIONAL','MIN_NOTIONAL']:
   if kind in fs and value<D(fs[kind]['minNotional']):raise ValueError('Exit dust/minimum notional unresolved')
 return float(mid),float((1-value/D(quantity)/mid)*10000)

def create_accounts(c,cfg):
 for model in cfg['models']:
  for cap in cfg['capitals']:
   for slip in cfg['slippage_bps']:
    k=f'{model}:{cap}:{slip}';c.execute('INSERT OR IGNORE INTO accounts VALUES(?,?,?,?,?,?,?,?,?)',(k,model,cap,slip,float(cap),0,0,float(cap),0))

def process_signals(c,symbol,rows,cfg,f,received):
 if not rows:return
 prev=getkv(c,'qual:'+symbol,{m:False for m in ['A0','A1','A2']});processed=getkv(c,'processed:'+symbol,rows[0][0]-MINUTE);at=feature_series(rows,cfg)
 for i,x in enumerate(rows):
  if x[0]<=processed:continue
  feat=at(i)
  if not feat:
   # Unknown is not false: suppress rising edges until a complete prior window exists.
   prev={m:None for m in prev};processed=x[0];continue
  ts=x[6]+1
  for model,yes in feat['qualifies'].items():
   edge=yes and prev.get(model) is False
   if ts>=f['start'] and ts<f['end'] and edge:
    models=[model,'A3'] if model=='A2' else [model]
    for m in models:
     eid=f'{m}:{symbol}:{ts}';last=getkv(c,'last_event:'+m+':'+symbol,0)
     reason='';status='PENDING_CONFIRMATION' if m=='A3' else 'PENDING'
     if ts-last<cfg['cooldown_seconds']*1000:status='REJECTED';reason='COOLDOWN'
     else:setkv(c,'last_event:'+m+':'+symbol,ts)
     confirm=cfg['confirmation_minutes']*MINUTE if m=='A3' else 0
     due=ts+confirm+cfg['entry_delay_seconds']*1000
     c.execute('INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?)',(eid,m,symbol,ts,due,due+cfg['entry_grace_seconds']*1000,json.dumps(feat),status,reason))
   prev[model]=yes
  processed=x[0]
 setkv(c,'qual:'+symbol,prev);setkv(c,'processed:'+symbol,processed)
 # Confirm against the actual five subsequent candle closes, not a later endpoint.
 by_t={x[0]:x for x in rows}
 for e in c.execute("SELECT * FROM events WHERE symbol=? AND status='PENDING_CONFIRMATION'",(symbol,)).fetchall():
  times=[e['signal']+i*MINUTE for i in range(cfg['confirmation_minutes'])]
  if rows[-1][6]+1<times[-1]+MINUTE:continue
  anchor=json.loads(e['feature'])['hour_vwap']
  passed=all(t in by_t and float(by_t[t][4])>=anchor for t in times)
  c.execute('UPDATE events SET status=?,reason=? WHERE id=?',('PENDING' if passed else 'REJECTED','' if passed else 'CONFIRMATION_FAILED_OR_MISSING',e['id']))

def mark_accounts(c,prices,ts,cfg):
 for a in c.execute('SELECT * FROM accounts').fetchall():
  pos=c.execute('SELECT * FROM positions WHERE account=?',(a['k'],)).fetchall();dep=0;stale=[]
  for p in pos:
   px,pt=prices.get(p['symbol'],(p['mid'],p['entered']));dep+=float(p['qty'])*px
   if ts-pt>2*MINUTE:stale.append(p['symbol'])
  equity=a['cash']+dep;peak=max(a['peak'],equity);halt=a['halt'] or equity<=peak*(1-cfg['account_breaker'])
  if equity<0 or a['cash']<-1e-8:raise ValueError('Nonnegative cash/equity invariant')
  c.execute('UPDATE accounts SET peak=?,halt=? WHERE k=?',(peak,int(halt),a['k']))
  c.execute('INSERT OR IGNORE INTO marks VALUES(?,?,?,?,?,?)',(a['k'],ts,equity,dep,a['fees'],json.dumps(stale)))

def exits(c,prices,books,cfg,f,now,book_ref):
 for p in c.execute('SELECT * FROM positions').fetchall():
  a=c.execute('SELECT * FROM accounts WHERE k=?',(p['account'],)).fetchone();px,pt=prices.get(p['symbol'],(p['mid'],p['entered']))
  reason=''
  if a['halt']:reason='ACCOUNT_BREAKER'
  elif now>=f['end']:reason='STUDY_CUTOFF'
  elif getkv(c,'stop:'+p['account']+':'+p['symbol']) is not None:reason='OBSERVED_CLOSE_STOP'
  elif now>=p['due']:reason='FOUR_HOUR_EXPIRY'
  if not reason or p['symbol'] not in books:continue
  current=getkv(c,'current_meta',{}).get(p['symbol'])
  if current and (current['status']!='TRADING' or not current.get('isSpotTradingAllowed')):
   issue(c,'EXIT_VENUE_UNAVAILABLE',p['symbol']);continue
  raw,rec=books[p['symbol']]
  try:
   mid,observed=sell_quote(raw,p['qty'],cfg,getkv(c,'current_meta',{}).get(p['symbol']));bps=max(float(a['slip']),observed);fill=mid*(1-bps/10000)
   if fill<=0:raise ValueError('Nonpositive exit')
   q=float(p['qty']);fee=q*fill*cfg['fee'];proceeds=q*fill-fee;fid=p['account']+':'+p['event']+':SELL'
   c.execute('INSERT INTO fills VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(fid,a['k'],p['event'],'SELL',p['symbol'],rec['received'],p['qty'],fill,fee,q*(mid-fill),json.dumps({'kind':reason,'due':getkv(c,'stop:'+p['account']+':'+p['symbol'],p['due'] if reason=='FOUR_HOUR_EXPIRY' else now),'received':rec['received']}),book_ref))
   c.execute('UPDATE accounts SET cash=cash+?,fees=fees+? WHERE k=?',(proceeds,fee,a['k']));c.execute('DELETE FROM positions WHERE account=? AND symbol=?',(a['k'],p['symbol']));c.execute('DELETE FROM kv WHERE k=?',('stop:'+p['account']+':'+p['symbol'],))
  except (ValueError,KeyError,ArithmeticError) as ex:issue(c,'EXIT_UNRESOLVED',p['symbol']+': '+str(ex))

def entries(c,books,meta,cfg,f,now,book_ref):
 for e in c.execute("SELECT * FROM events WHERE status='PENDING' ORDER BY due,symbol,model").fetchall():
  if now>e['deadline'] or now>=f['end']:
   c.execute("UPDATE events SET status='REJECTED',reason=? WHERE id=?",('ENTRY_DEADLINE_OR_END',e['id']));continue
  if now<e['due'] or e['symbol'] not in books:continue
  raw,rec=books[e['symbol']]
  if rec.get('duration',99)>cfg['maximum_book_request_seconds'] or not e['due']<=rec['received']<=e['deadline']:continue
  decisions=[]
  for a in c.execute('SELECT * FROM accounts WHERE model=?',(e['model'],)).fetchall():
   held=c.execute('SELECT * FROM positions WHERE account=?',(a['k'],)).fetchall();why=''
   if a['halt']:why='ACCOUNT_HALTED'
   elif a['entries']>=cfg['maximum_entries']:why='TRADE_BUDGET'
   elif any(p['symbol']==e['symbol'] for p in held):why='ALREADY_HELD'
   elif len(held)>=cfg['maximum_positions']:why='POSITION_CAP'
   try:
    budget=a['capital']*cfg['risk_fraction']/(cfg['stop_fraction']+cfg['baseline_risk_cost_buffer'])
    if why:raise ValueError(why)
    quote=book_quote(raw,meta[e['symbol']],budget,cfg);mid=quote['mid'];q=float(quote['quantity']);bp=max(float(a['slip']),quote['buy_bps']);fill=mid*(1+bp/10000);fee=q*fill*cfg['fee'];outlay=q*fill+fee
    allowance=a['capital']*.03*cfg['duration_days']/365
    reserves=sum(float(p['qty'])*p['mid']*cfg['fee']*2 for p in held)
    if a['fees']+reserves+2*fee>allowance:raise ValueError('FEE_BUDGET')
    if outlay>budget+1e-8 or outlay>a['cash']+1e-8:raise ValueError('CASH_CLIP')
    fid=a['k']+':'+e['id']+':BUY';c.execute('INSERT INTO fills VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(fid,a['k'],e['id'],'BUY',e['symbol'],rec['received'],quote['quantity'],fill,fee,q*(fill-mid),'SIGNAL',book_ref))
    c.execute('UPDATE accounts SET cash=cash-?,fees=fees+?,entries=entries+1 WHERE k=?',(outlay,fee,a['k']))
    c.execute('INSERT INTO positions VALUES(?,?,?,?,?,?,?)',(a['k'],e['symbol'],quote['quantity'],mid,rec['received'],rec['received']+cfg['hold_seconds']*1000,e['id']))
    decisions.append({'account':a['k'],'accepted':True})
   except (ValueError,KeyError,ArithmeticError) as ex:decisions.append({'account':a['k'],'accepted':False,'reason':str(ex)})
  c.execute("UPDATE events SET status='DECIDED',reason=? WHERE id=?",(json.dumps(decisions),e['id']))

def mark_minute_paths(c,by_symbol,cfg,f,now):
 # Historical candle recovery supplies marks only, never backfilled fills.
 accounts=c.execute('SELECT * FROM accounts').fetchall()
 latest={a['k']:c.execute('SELECT MAX(ts) FROM marks WHERE account=?',(a['k'],)).fetchone()[0] or f['start'] for a in accounts}
 first=min(latest.values()) if latest else now
 timeline=sorted({x[6]+1 for rows in by_symbol.values() for x in rows if first<x[6]+1<=now and x[6]+1>=f['start']})
 series={s:{x[6]+1:float(x[4]) for x in rows} for s,rows in by_symbol.items()}
 positions={a['k']:c.execute('SELECT * FROM positions WHERE account=?',(a['k'],)).fetchall() for a in accounts}
 for a in accounts:
  for ts in timeline:
   if ts<=latest[a['k']]:continue
   dep=0;stale=[]
   for p in positions[a['k']]:
    px=series.get(p['symbol'],{}).get(ts)
    if px is None:stale.append(p['symbol']);px=p['mid']
    dep+=float(p['qty'])*px
    if px<=p['mid']*(1-cfg['stop_fraction']) and getkv(c,'stop:'+a['k']+':'+p['symbol']) is None:setkv(c,'stop:'+a['k']+':'+p['symbol'],ts)
   equity=a['cash']+dep;peak=max(a['peak'],equity);halt=a['halt'] or equity<=peak*(1-cfg['account_breaker'])
   c.execute('UPDATE accounts SET peak=?,halt=? WHERE k=?',(peak,int(halt),a['k']))
   c.execute('INSERT OR IGNORE INTO marks VALUES(?,?,?,?,?,?)',(a['k'],ts,equity,dep,a['fees'],json.dumps(stale)))
   a=dict(a);a['peak']=peak;a['halt']=int(halt)

def fetch_symbol(symbol,start,end,max_pages):
 records=[];bars=[];cursor=start
 for _ in range(max_pages):
  if cursor>=end:break
  obj,rec=request('klines',{'symbol':symbol,'interval':'1m','startTime':cursor,'endTime':end-1,'limit':1000});records.append(rec)
  if not isinstance(obj,list) or not obj:break
  closed=[x for x in obj if x[6]<end];bars.extend(closed)
  if not closed:break
  nxt=closed[-1][0]+MINUTE
  if nxt<=cursor:raise ValueError('Pagination did not advance')
  cursor=nxt
 return symbol,bars,records

def initialize():
 if (ROOT/'FREEZE.json').exists():raise ValueError('Already frozen')
 cfg=config()
 if not (ROOT/'UNIVERSE.json').exists():
  info,ir=request('exchangeInfo',{'permissions':'SPOT','showPermissionSets':'false'});tick,tr=request('ticker/24hr');save_raw([ir,tr],'inception')
  if not info or not tick:raise ValueError('Inception public data unavailable')
  members=pick_universe(info,tick,cfg)
  if not members:raise ValueError('No eligible inception symbols')
  lookup={x['symbol']:x for x in info['symbols']};symbols=sorted(set(x['symbol'] for x in members)|{'BTCUSDT','ETHUSDT'})
  end=clock_ms()//MINUTE*MINUTE
  with (ROOT/'UNIVERSE.json').open('x') as h:json.dump({'captured':clock_ms(),'warmup_end':end,'members':members,'collected_symbols':symbols,'benchmark_metadata':{s:lookup[s] for s in ['BTCUSDT','ETHUSDT']}},h,indent=2)
 u=json.loads((ROOT/'UNIVERSE.json').read_text());symbols=u['collected_symbols'];end=u['warmup_end'];start=end-WARM*MINUTE;records=[];c=connect()
 with concurrent.futures.ThreadPoolExecutor(max_workers=cfg['maximum_workers']) as pool:
  for symbol,b,rr in pool.map(lambda s:fetch_symbol(s,start,end,12),symbols):
   records.extend(rr);save_raw(rr,'warmup-'+symbol);store_bars(c,symbol,b,clock_ms(),end);c.commit()
   if len(b)!=WARM or [x[0] for x in b]!=list(range(start,end,MINUTE)):raise ValueError('Incomplete warmup: '+symbol)
   feat=feature_series(b,cfg)(len(b)-1)
   setkv(c,'processed:'+symbol,b[-1][0]);setkv(c,'qual:'+symbol,feat['qualifies'] if feat else {m:None for m in ['A0','A1','A2']})
   print(symbol,len(b),flush=True)
 c.commit();save_raw(records,'warmup');c.close()
 print('Warmup complete; no performance evaluated.')

def freeze():
 if (ROOT/'FREEZE.json').exists():raise ValueError('Already frozen')
 tested=json.loads((ROOT/'TEST_RESULTS.json').read_text())
 if not tested['success'] or any(digest(ROOT/n)!=h for n,h in tested['hashes'].items()):raise ValueError('Tests not current/passing')
 cfg=config();c=connect();u=json.loads((ROOT/'UNIVERSE.json').read_text())
 if not (ROOT/'COHORT.json').exists():raise ValueError('Classification not finalized')
 for s in u['collected_symbols']:
  if c.execute('SELECT COUNT(*) FROM bars WHERE symbol=?',(s,)).fetchone()[0]<WARM:raise ValueError('Missing warmup')
 now=clock_ms();start=((now+5*MINUTE+5*MINUTE-1)//(5*MINUTE))*(5*MINUTE);end=start+cfg['duration_days']*86400000
 f={'frozen':now,'start':start,'end':end,'tail_end':end+cfg['settlement_tail_hours']*3600000,'start_utc':utc(start),'end_utc':utc(end),'start_nzdt':nz(start),'end_nzdt':nz(end),'hashes':{n:digest(ROOT/n) for n in ['study.py','tests.py','config.json','DESIGN.md','UNIVERSE.json','COHORT.json','CLASSIFICATION.json','TEST_RESULTS.json','run_collector.py','install_collector.py','collector.plist']},'authority':'PUBLIC_DATA_AND_SIMULATED_SCREENING_ONLY'}
 with (ROOT/'FREEZE.json').open('x') as h:json.dump(f,h,indent=2)
 create_accounts(c,cfg);setkv(c,'stage','WARMUP_BEFORE_FUTURE_START');c.commit();c.close();print(json.dumps(f,indent=2))

def tick(preflight=False):
 f=({'start':clock_ms()+3600000,'end':clock_ms()+31*86400000,'tail_end':clock_ms()+31*86400000+14400000,'start_nzdt':'PREFLIGHT_ONLY','end_nzdt':'PREFLIGHT_ONLY','hashes':{}} if preflight else load_freeze());cfg=config();u=json.loads((ROOT/'COHORT.json').read_text());now=clock_ms();c=connect()
 if now>=f['tail_end']:setkv(c,'stage','COMPLETE_OR_UNRESOLVED_AT_TAIL');c.commit();write_status(c,f);c.close();report();return
 size=sum(x.stat().st_size for x in ROOT.rglob('*') if x.is_file())
 if size>cfg['disk_limit_bytes']:issue(c,'DISK_CAP','Collection halted');setkv(c,'stage','DISK_CAP_HALT');c.commit();write_status(c,f);c.close();return
 # Every attempt is scoped to the frozen symbol set; all responses retain their timestamps.
 end=now//MINUTE*MINUTE;todo=[]
 for s in u['collected_symbols']:
  last=c.execute('SELECT MAX(t) FROM bars WHERE symbol=?',(s,)).fetchone()[0];todo.append((s,last-MINUTE,end,cfg['max_pages_per_symbol_per_tick']))
 rr=[];new_rows={}
 with concurrent.futures.ThreadPoolExecutor(max_workers=cfg['maximum_workers']) as pool:
  for s,b,records in pool.map(lambda args:fetch_symbol(*args),todo):
   rr.extend(records)
   try:store_bars(c,s,b,clock_ms(),end)
   except ValueError as ex:issue(c,'CANDLE_INTEGRITY',str(ex));setkv(c,'integrity_halt',True)
   if any(x.get('error') for x in records):issue(c,'PUBLIC_FETCH_ERROR',s+': '+str([x.get('error') for x in records if x.get('error')]))
   processed=getkv(c,'processed:'+s,end-MINUTE)
   new_rows[s]=[json.loads(x[0]) for x in c.execute('SELECT raw FROM bars WHERE symbol=? AND t>=? ORDER BY t',(s,processed-WARM*MINUTE))]
 raw_ref=save_raw(rr,'candles')
 if getkv(c,'integrity_halt',False):setkv(c,'stage','INTEGRITY_HALT');c.commit();write_status(c,f);c.close();return
 member_meta={x['symbol']:x for x in u['members']}
 last_tick=getkv(c,'last_tick',now)
 if now-last_tick>2*MINUTE and now>=f['start']:issue(c,'COLLECTOR_DOWNTIME',str((now-last_tick)/1000)+' seconds; missed books not recoverable')
 # Capture current metadata daily; changes disable entries rather than silently changing frozen filters.
 checked=getkv(c,'metadata_checked',0)
 if now-checked>=86400000:
  info,rec=request('exchangeInfo',{'symbols':json.dumps(sorted(set(member_meta)|{'BTCUSDT','ETHUSDT'}),separators=(',',':')),'showPermissionSets':'false'});save_raw([rec],'metadata')
  if info:
   current={x['symbol']:x for x in info['symbols']};disabled=[];setkv(c,'current_meta',{s:current.get(s) for s in member_meta})
   for s,m in member_meta.items():
    x=current.get(s)
    if not x or x['status']!='TRADING' or not x.get('isSpotTradingAllowed') or x['filters']!=m['filters']:disabled.append(s)
   setkv(c,'disabled_symbols',disabled);setkv(c,'metadata_checked',now)
  else:issue(c,'METADATA_UNAVAILABLE','New entries disabled until successful metadata refresh');setkv(c,'disabled_symbols',list(member_meta))
 for s in member_meta:process_signals(c,s,new_rows[s],cfg,f,clock_ms())
 # Only current endpoint observations can fund hypothetical entries. Missing books remain explicit.
 disabled=set(getkv(c,'disabled_symbols',[]))
 for s in disabled:c.execute("UPDATE events SET status='REJECTED',reason='CURRENT_SYMBOL_METADATA' WHERE symbol=? AND status IN ('PENDING','PENDING_CONFIRMATION')",(s,))
 c.commit()
 book_symbols={x[0] for x in c.execute("SELECT DISTINCT symbol FROM events WHERE status='PENDING' AND due<=? AND deadline>=?",(clock_ms(),clock_ms()))}
 book_symbols|={x[0] for x in c.execute('SELECT DISTINCT symbol FROM positions')};books={};br=[]
 with concurrent.futures.ThreadPoolExecutor(max_workers=cfg['maximum_workers']) as pool:
  for s,(raw,rec) in zip(sorted(book_symbols),pool.map(lambda s:request('depth',{'symbol':s,'limit':100}),sorted(book_symbols))):
   br.append(rec)
   if raw and rec.get('duration',99)<=cfg['maximum_book_request_seconds']:books[s]=(raw,rec)
   else:issue(c,'BOOK_UNAVAILABLE',s)
 book_ref=save_raw(br,'books') if br else ''
 prices={}
 for s in u['collected_symbols']:
  row=c.execute('SELECT raw,t FROM bars WHERE symbol=? ORDER BY t DESC LIMIT 1',(s,)).fetchone();prices[s]=(float(json.loads(row[0])[4]),row[1]+MINUTE)
 for s,(book,rec) in books.items():
  try:prices[s]=((float(book['asks'][0][0])+float(book['bids'][0][0]))/2,rec['received'])
  except (KeyError,ValueError,IndexError):pass
 now=clock_ms()
 if now>=f['start']:
  mark_minute_paths(c,new_rows,cfg,f,now)
  mark_accounts(c,prices,now,cfg);exits(c,prices,books,cfg,f,now,book_ref);entries(c,books,member_meta,cfg,f,now,book_ref);mark_accounts(c,prices,now+1,cfg)
 setkv(c,'last_tick',now);setkv(c,'last_raw',raw_ref);setkv(c,'stage','COLLECTING' if now>=f['start'] and now<f['end'] else ('SETTLEMENT_TAIL' if now>=f['end'] else 'WARMUP_BEFORE_FUTURE_START'))
 c.commit();write_status(c,f);c.close()

def write_status(c,f):
 cfg=config();u=json.loads((ROOT/'COHORT.json').read_text());coverage=[]
 for s in u['collected_symbols']:
  row=c.execute('SELECT COUNT(*),MIN(t),MAX(t) FROM bars WHERE symbol=?',(s,)).fetchone();forward=c.execute('SELECT COUNT(*) FROM bars WHERE symbol=? AND t>=? AND t<?',(s,f['start'],f['end'])).fetchone()[0];coverage.append({'symbol':s,'bars':row[0],'warmup_bars':row[0]-forward,'prospective_bars':forward,'first':row[1],'last':row[2],'latest_close_age_seconds':max(0,(clock_ms()-row[2]-MINUTE)/1000)})
 obj={'stage':getkv(c,'stage'),'now_utc':utc(clock_ms()),'start_nzdt':f['start_nzdt'],'end_nzdt':f['end_nzdt'],'last_tick':getkv(c,'last_tick'),'universe_count':len(u['members']),'coverage':coverage,'event_counts':[dict(x) for x in c.execute('SELECT model,status,COUNT(*) AS n FROM events GROUP BY model,status')],'open_positions':c.execute('SELECT COUNT(*) FROM positions').fetchone()[0],'simulated_entries':c.execute("SELECT COUNT(*) FROM fills WHERE side='BUY'").fetchone()[0],'prospective_bar_receipts':sum(x['prospective_bars'] for x in coverage),'warmup_bar_receipts':sum(x['warmup_bars'] for x in coverage),'issues':c.execute('SELECT COUNT(*) FROM issues').fetchone()[0],'recent_issues':[dict(x) for x in c.execute('SELECT * FROM issues ORDER BY id DESC LIMIT 10')],'frozen_identity_ok':bool(f.get('hashes')),'performance_evaluation':'DEFERRED_TO_FIXED_END','limits':'Local collector needs awake/online Mac; recovered candles do not recover missed books'}
 (ROOT/'STATUS.json').write_text(json.dumps(obj,indent=2));print(json.dumps({k:v for k,v in obj.items() if k not in ['coverage','recent_issues']},indent=2))

def report():
 f=load_freeze();cfg=config();c=connect()
 if clock_ms()<f['end']:write_status(c,f);c.close();return
 # Continuous marks include fees in equity, starting capital as initial peak, never partition resets.
 rows=[]
 for a in c.execute('SELECT * FROM accounts ORDER BY capital,slip,model').fetchall():
  marks=c.execute('SELECT * FROM marks WHERE account=? ORDER BY ts',(a['k'],)).fetchall();peak=float(a['capital']);dd=0;weighted=0;stales=0
  for i,m in enumerate(marks):
   peak=max(peak,m['equity']);dd=min(dd,m['equity']/peak-1);stales+=bool(json.loads(m['stale']))
   if i+1<len(marks):weighted+=(marks[i+1]['ts']-m['ts'])*(m['deployed']/m['equity'] if m['equity'] else 0)
  # Include pre-first-mark idle interval, and mark latest exposure to the report timestamp only within tail.
  last=marks[-1] if marks else None;end=min(clock_ms(),f['tail_end']);span=max(1,end-f['start'])
  if last:weighted+=max(0,end-last['ts'])*(last['deployed']/last['equity'] if last['equity'] else 0)
  equity=last['equity'] if last else float(a['capital']);fee_allow=a['capital']*.03*cfg['duration_days']/365
  rows.append({'account':a['k'],'capital':a['capital'],'model':a['model'],'slip':a['slip'],'period_return':equity/a['capital']-1,'max_drawdown':dd,'ending_equity':equity,'deployment':weighted/span,'fees':a['fees'],'fee_allowance':fee_allow,'fee_budget_breach':a['fees']>fee_allow,'entries':a['entries'],'halt':bool(a['halt']),'stale_marks':stales,'open_positions':c.execute('SELECT COUNT(*) FROM positions WHERE account=?',(a['k'],)).fetchone()[0]})
 clusters=c.execute("SELECT COUNT(*) FROM (SELECT DISTINCT e.symbol,CAST(e.signal/86400000 AS INTEGER) FROM events e JOIN fills f ON e.id=f.event WHERE f.side='BUY')").fetchone()[0]
 coverage=[]
 for symbol in json.loads((ROOT/'COHORT.json').read_text())['collected_symbols']:
  expected=max(0,(f['end']-f['start'])//MINUTE);actual=c.execute('SELECT COUNT(*) FROM bars WHERE symbol=? AND t>=? AND t<?',(symbol,f['start'],f['end'])).fetchone()[0]
  coverage.append({'symbol':symbol,'expected_minutes':expected,'captured_minutes':actual,'missing_minutes':expected-actual})
 benchmark={'cash':{'period_return':0.0,'max_drawdown':0.0,'yield_assumption':0.0,'deployment':0.0}}
 btc=[json.loads(x[0]) for x in c.execute('SELECT raw FROM bars WHERE symbol=? AND t>=? AND t<? ORDER BY t',('BTCUSDT',f['start'],f['end']))]
 if len(btc)==(f['end']-f['start'])//MINUTE and btc[0][0]==f['start']:
  entry=float(btc[0][1])*(1+.0015);units=(1/(1+cfg['fee']))/entry;peak=1;dd=0
  for x in btc:
   eq=units*float(x[4]);peak=max(peak,eq);dd=min(dd,eq/peak-1)
  benchmark['btc']={'period_return':units*float(btc[-1][4])-1,'max_drawdown':dd,'basis':'OHLC reference, full-account,15bps entry shortfall plus 0.10% entry fee; ending inventory marked, no hypothetical exit fee','actual_fill':False,'deployment':1.0}
 else:benchmark['btc']={'status':'UNESTABLISHED_INCOMPLETE_MINUTES'}
 result={'status':'SCREEN_ONLY' if clusters>=cfg['minimum_union_event_clusters'] else 'INCONCLUSIVE_SAMPLE','accepted_symbol_day_clusters':clusters,'accounts':rows,'coverage':coverage,'benchmarks':benchmark,'cash_yield_on_idle_assumed':0.0,'evaluated_through':utc(min(clock_ms(),f['tail_end'])),'cagr':None,'qualification':'UNVALIDATED; not full cycle; depth availability not actual fills; no qualified inferential claim','window':f}
 (ROOT/'RESULTS.json').write_text(json.dumps(result,indent=2));lines=['# Prospective Spot alert comparison','',f"Accepted symbol/day clusters: {clusters}. Status: {result['status']}.",'','Continuous simulated accounts, pre-tax. No CAGR for this short window. Open positions and stale marks prevent drawdown/settlement qualification. No actual fills.','', '| Model | Capital | Shortfall/side | Period return | Max DD | Deployment | Fees USDT | Entries | Open | Stale marks |','|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
 for x in rows:lines.append(f"| {x['model']} | {x['capital']} | {x['slip']}bps | {x['period_return']:+.4%} | {x['max_drawdown']:.4%} | {x['deployment']:.4%} | {x['fees']:.4f} | {x['entries']} | {x['open_positions']} | {x['stale_marks']} |")
 lines+=['','Benchmarks: '+json.dumps(benchmark),'','Missing minute coverage: '+json.dumps([x for x in coverage if x['missing_minutes']]),'','Idle-cash yield assumed zero; BTC opportunity cost is a separate reference comparison, not measured cash yield. Settlement can include the capped four-hour tail; account return and deployment describe that disclosed interval. Candidate rankings and formal inference are not established. Missing live books are rejected entries, not historical paper fills. Individual account simulations are alternatives, not one aggregate allocation. Fee overruns, missed collection and unresolved positions are defects, never waivers.','']
 (ROOT/'RESULTS.md').write_text('\n'.join(lines));c.close();print(result['status'])

def main():
 p=argparse.ArgumentParser();p.add_argument('mode',choices=['init','freeze','preflight','tick','status','report']);a=p.parse_args()
 with (ROOT/'collector.lock').open('a') as lock:
  try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:print('Existing tick owns lock; no duplicate collection');return
  if a.mode=='init':initialize()
  elif a.mode=='freeze':freeze()
  elif a.mode=='tick':tick()
  elif a.mode=='preflight':tick(preflight=True)
  elif a.mode=='report':report()
  else:
   c=connect();write_status(c,load_freeze());c.close()
if __name__=='__main__':main()
