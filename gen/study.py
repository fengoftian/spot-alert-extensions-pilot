"""Independent B0–B3 prospective Spot screen with a concurrent A2 control."""
import argparse,fcntl,importlib.util,json,math,pathlib,statistics

ROOT=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('extensions_common_engine',ROOT/'engine.py')
engine=importlib.util.module_from_spec(spec);spec.loader.exec_module(engine)
engine.ROOT=ROOT
MINUTE=engine.MINUTE
MODELS=('C_A2','B0','B1','B2')

def __getattr__(name):return getattr(engine,name)

def daily_return_threshold(c,symbol,day,cfg):
 key=f'b0_calibration:{symbol}:{day}'
 cached=engine.getkv(c,key)
 if cached is not None:return cached
 start=day-cfg['b0_calibration_days']*86400000
 rows=[json.loads(x[0]) for x in c.execute('SELECT raw FROM bars WHERE symbol=? AND t>=? AND t<? ORDER BY t',(symbol,start-MINUTE,day))]
 expected=cfg['b0_calibration_days']*1440+1
 if len(rows)!=expected or any(x[0]!=start-MINUTE+i*MINUTE for i,x in enumerate(rows)):return None
 n=cfg['b0_window_minutes'];returns=sorted(float(rows[i+n][4])/float(rows[i][4])-1 for i in range(0,len(rows)-n,n))
 rank=math.ceil(cfg['b0_return_quantile']*len(returns))-1
 result={'quantile':returns[rank],'floor':cfg['b0_return_floor'],'observations':len(returns),'baseline_start':start,'baseline_end':day}
 engine.setkv(c,key,result)
 return result

def conditions(c,symbol,rows,i,cfg,base_feature,btc):
 if base_feature is None:return None
 f=dict(base_feature);f.pop('qualifies');x=rows[i];signal=x[6]+1
 control=base_feature['qualifies']['A2']
 calibration=daily_return_threshold(c,symbol,(x[0]//86400000)*86400000,cfg)
 n=cfg['b0_window_minutes'];current=rows[i-n+1:i+1]
 total=sum(float(z[7]) for z in current)
 seasonal=statistics.median(sum(float(z[7]) for z in rows[i-n+1-k*1440:i+1-k*1440]) for k in range(1,8))
 short_return=float(x[4])/float(rows[i-n][4])-1
 previous_high=max(float(z[2]) for z in rows[i-n-1439:i-n+1])
 early=(short_return>=max(calibration['quantile'],calibration['floor']) and seasonal>0 and total/seasonal>=cfg['relative_volume_threshold'] and float(x[4])>previous_high) if calibration else None
 reference=rows[i-1499:i-59]
 needed=[x]+reference
 relative=None
 if all(z[0] in btc and btc[z[0]]>0 for z in needed):
  ratio=float(x[4])/btc[x[0]];ratio_high=max(float(z[4])/btc[z[0]] for z in reference)
  relative=control and ratio>ratio_high;f.update(btc_ratio=ratio,prior_btc_ratio_high=ratio_high)
 recent=rows[i-cfg['b2_minutes']+1:i+1];turn=sum(float(z[7]) for z in recent);buy=sum(float(z[10]) for z in recent)
 pressure=control and turn>0 and buy/turn>=cfg['b2_buy_share']
 f.update(return_15m=short_return,rv_15m=total/seasonal if seasonal>0 else None,b0_calibration=calibration,b0_prior_high=previous_high,taker_buy_share_5m=buy/turn if turn>0 else None)
 f['qualifies']={'C_A2':control,'B0':early,'B1':relative,'B2':pressure}
 return f

def register(c,model,symbol,signal,feature,cfg):
 last=engine.getkv(c,f'last_event:{model}:{symbol}',0)
 reason='';status='PENDING_RETEST' if model=='B3' else 'PENDING'
 if last and signal-last<cfg['cooldown_seconds']*1000:status='REJECTED';reason='COOLDOWN'
 else:engine.setkv(c,f'last_event:{model}:{symbol}',signal)
 due=signal+cfg['entry_delay_seconds']*1000
 if model=='B3':due+=cfg['b3_watch_minutes']*MINUTE
 eid=f'{model}:{symbol}:{signal}'
 c.execute('INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?)',(eid,model,symbol,signal,due,due+cfg['entry_grace_seconds']*1000,json.dumps(feature,allow_nan=False),status,reason))

def confirm_retests(c,symbol,rows,cfg,received):
 by_t={x[0]:x for x in rows}
 for e in c.execute("SELECT * FROM events WHERE symbol=? AND model='B3' AND status='PENDING_RETEST' ORDER BY signal",(symbol,)).fetchall():
  feature=json.loads(e['feature']);level=feature['prior_high'];stop=e['signal']+cfg['b3_watch_minutes']*MINUTE;confirmed=False
  for t in range(e['signal'],stop-MINUTE,MINUTE):
   first=by_t.get(t);second=by_t.get(t+MINUTE)
   if first is None or second is None:continue
   close=second[6]+1
   if close>received or close>stop:continue
   if float(first[3])<=level<=float(first[4]) and float(second[4])>float(first[2]):
    feature.update(retest_candle=t,retest_confirmation=close)
    due=close+cfg['entry_delay_seconds']*1000
    c.execute("UPDATE events SET status='PENDING',due=?,deadline=?,feature=? WHERE id=?",(due,due+cfg['entry_grace_seconds']*1000,json.dumps(feature),e['id']));confirmed=True;break
  if not confirmed and received>=stop:
   c.execute("UPDATE events SET status='REJECTED',reason='RETEST_WINDOW_EXPIRED_OR_MISSING' WHERE id=?",(e['id'],))

def process_signals(c,symbol,rows,cfg,f,received):
 if not rows:return
 prev=engine.getkv(c,'qual:'+symbol,{m:None for m in MODELS});processed=engine.getkv(c,'processed:'+symbol,rows[0][0]-MINUTE)
 at=engine.feature_series(rows,cfg)
 lower=rows[0][0]
 btc={r[1]:float(json.loads(r[0])[4]) for r in c.execute('SELECT raw,t FROM bars WHERE symbol=? AND t>=? AND t<=?',('BTCUSDT',lower,rows[-1][0]))}
 for i,x in enumerate(rows):
  if x[0]<=processed:continue
  feature=conditions(c,symbol,rows,i,cfg,at(i),btc)
  if feature is None:prev={m:None for m in MODELS};processed=x[0];continue
  signal=x[6]+1
  for model,yes in feature['qualifies'].items():
   if yes is True and prev.get(model) is False and f['start']<=signal<f['end']:
    register(c,model,symbol,signal,feature,cfg)
    if model=='C_A2':register(c,'B3',symbol,signal,feature,cfg)
   prev[model]=yes
  processed=x[0]
 engine.setkv(c,'qual:'+symbol,prev);engine.setkv(c,'processed:'+symbol,processed)
 confirm_retests(c,symbol,rows,cfg,received)

engine.process_signals=process_signals

def main():
 p=argparse.ArgumentParser();p.add_argument('mode',choices=['tick','status','report']);a=p.parse_args()
 with (ROOT/'collector.lock').open('a') as lock:
  try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:raise RuntimeError('Another collector owns the study lock')
  if a.mode=='tick':engine.tick()
  elif a.mode=='report':engine.report()
  else:
   c=engine.connect();engine.write_status(c,engine.load_freeze());c.close()

if __name__=='__main__':main()
