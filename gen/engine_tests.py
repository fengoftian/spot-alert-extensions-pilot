import copy,hashlib,json,pathlib,tempfile,unittest
import study as s

CFG=s.config()
def bar(t,price=100.0,quote=100.0):
 return [t,str(price),str(price*1.001),str(price*.999),str(price),str(quote/price if quote else 0),t+59999,str(quote),1,str(quote/price/2 if quote else 0),str(quote/2),'0']
def history():
 b=[bar(i*s.MINUTE,1,100) for i in range(s.WARM)]
 for i in range(len(b)-60,len(b)):b[i]=bar(i*s.MINUTE,1.11,400)
 return b
META={'symbol':'TESTUSDT','status':'TRADING','isSpotTradingAllowed':True,'filters':[{'filterType':'LOT_SIZE','stepSize':'0.1','minQty':'0.1','maxQty':'100000'},{'filterType':'NOTIONAL','minNotional':'5','maxNotional':'10000000'}]}
def book(mid=100,qty='1000'):
 return {'asks':[[str(mid+.01),qty]],'bids':[[str(mid-.01),qty]]}
def event(c,model='A0',ts=1000000,status='PENDING'):
 eid=model+':TESTUSDT:'+str(ts);due=ts+60000
 c.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?)',(eid,model,'TESTUSDT',ts,due,due+120000,'{}',status,''));return eid,due

class FeatureTests(unittest.TestCase):
 def test_nonoverlapping_baseline_and_seasonal(self):
  b=history();f=s.feature_series(b,CFG)(len(b)-1)
  self.assertAlmostEqual(f['return_1h'],.11);self.assertAlmostEqual(f['rv24'],4);self.assertAlmostEqual(f['rv_seasonal'],4)
  self.assertEqual(f['qualifies'],{'A0':True,'A1':True,'A2':True})
 def test_future_candles_do_not_change_past_signal(self):
  b=history();i=len(b)-1;before=s.feature_series(b,CFG)(i);b.append(bar((i+1)*s.MINUTE,99999,1e12))
  self.assertEqual(before,s.feature_series(b,CFG)(i))
 def test_gap_is_unknown_not_false(self):
  b=history();del b[5000]
  self.assertIsNone(s.feature_series(b,CFG)(len(b)-1))
 def test_zero_baseline_fails_closed(self):
  b=[bar(i*s.MINUTE,1,0) for i in range(s.WARM)]
  for i in range(len(b)-60,len(b)):b[i]=bar(i*s.MINUTE,1.2,400)
  self.assertIsNone(s.feature_series(b,CFG)(len(b)-1))
 def test_breakout_is_separate_condition(self):
  b=history();b[-70][2]='1.2';f=s.feature_series(b,CFG)(len(b)-1)
  self.assertTrue(f['qualifies']['A0']);self.assertFalse(f['qualifies']['A1']);self.assertFalse(f['qualifies']['A2'])
 def test_seasonal_baseline_can_change_decision(self):
  b=history()
  for k in range(1,8):
   for i in range(len(b)-60-k*1440,len(b)-k*1440):b[i]=bar(i*s.MINUTE,1,800)
  f=s.feature_series(b,CFG)(len(b)-1)
  self.assertTrue(f['qualifies']['A1']);self.assertFalse(f['qualifies']['A2'])

class StateTests(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.c=s.connect(pathlib.Path(self.t.name)/'test.sqlite');s.create_accounts(self.c,CFG)
 def tearDown(self):self.c.close();self.t.cleanup()
 def test_duplicate_candle_is_idempotent_but_revision_rejected(self):
  x=bar(0);s.store_bars(self.c,'TESTUSDT',[x],100000,60000);s.store_bars(self.c,'TESTUSDT',[x],110000,60000)
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM bars').fetchone()[0],1)
  y=copy.deepcopy(x);y[4]='100.01'
  with self.assertRaises(ValueError):s.store_bars(self.c,'TESTUSDT',[y],120000,60000)
 def test_incomplete_candle_never_stored(self):
  s.store_bars(self.c,'TESTUSDT',[bar(0)],50000,50000)
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM bars').fetchone()[0],0)
 def test_rising_edge_and_same_tick_repeat(self):
  b=history();s.setkv(self.c,'qual:TESTUSDT',{m:False for m in ['A0','A1','A2']});s.setkv(self.c,'processed:TESTUSDT',b[-2][0]);f={'start':b[-1][0],'end':b[-1][0]+86400000}
  s.process_signals(self.c,'TESTUSDT',b,CFG,f,b[-1][6]+2);s.process_signals(self.c,'TESTUSDT',b,CFG,f,b[-1][6]+3)
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM events').fetchone()[0],4)
 def test_left_truncated_conditions_do_not_create_entries(self):
  b=history();s.setkv(self.c,'qual:TESTUSDT',{m:True for m in ['A0','A1','A2']});s.setkv(self.c,'processed:TESTUSDT',b[-2][0]);f={'start':b[-1][0],'end':b[-1][0]+86400000}
  s.process_signals(self.c,'TESTUSDT',b,CFG,f,b[-1][6]+2)
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM events').fetchone()[0],0)
 def test_confirmation_needs_all_five_completed_closes(self):
  b=history();signal=b[-1][6]+1;eid,due=event(self.c,'A3',signal,'PENDING_CONFIRMATION');self.c.execute('UPDATE events SET feature=? WHERE id=?',(json.dumps({'hour_vwap':1.11}),eid))
  for i in range(5):b.append(bar(signal+i*s.MINUTE,1.12 if i!=2 else 1.10,100))
  s.setkv(self.c,'processed:TESTUSDT',b[-1][0]);s.process_signals(self.c,'TESTUSDT',b,CFG,{'start':signal,'end':signal+86400000},b[-1][6]+2)
  self.assertEqual(self.c.execute('SELECT status FROM events WHERE id=?',(eid,)).fetchone()[0],'REJECTED')
 def test_cash_fees_once_exit_once_and_three_stress_entries(self):
  eid,due=event(self.c);now=due+1;rec={'received':now,'duration':.1};f={'start':0,'end':10**10}
  s.entries(self.c,{'TESTUSDT':(book(),rec)},{'TESTUSDT':META},CFG,f,now,'test-book')
  self.assertEqual(self.c.execute("SELECT COUNT(*) FROM fills WHERE side='BUY'").fetchone()[0],6)
  a=self.c.execute("SELECT * FROM accounts WHERE k='A0:2000:15'").fetchone()
  # 0.7 units at 100.15, fee .070105 USDT.
  self.assertAlmostEqual(a['cash'],2000-70.105-.070105);self.assertAlmostEqual(a['fees'],.070105)
  s.entries(self.c,{'TESTUSDT':(book(),rec)},{'TESTUSDT':META},CFG,f,now,'test-book')
  self.assertEqual(self.c.execute("SELECT COUNT(*) FROM fills WHERE side='BUY'").fetchone()[0],6)
  for p in self.c.execute('SELECT * FROM positions').fetchall():s.setkv(self.c,'stop:'+p['account']+':TESTUSDT',now+60000)
  exrec={'received':now+60000,'duration':.1};s.exits(self.c,{'TESTUSDT':(92,now+60000)},{'TESTUSDT':(book(92),exrec)},CFG,f,now+60000,'exit-book')
  s.exits(self.c,{'TESTUSDT':(92,now+60000)},{'TESTUSDT':(book(92),exrec)},CFG,f,now+60000,'exit-book')
  a=self.c.execute("SELECT * FROM accounts WHERE k='A0:2000:15'").fetchone()
  self.assertAlmostEqual(a['cash'],1994.0639916);self.assertEqual(self.c.execute("SELECT COUNT(*) FROM fills WHERE side='SELL'").fetchone()[0],6)
 def test_expired_book_cannot_create_past_fill(self):
  _,due=event(self.c);now=due+120001
  s.entries(self.c,{'TESTUSDT':(book(),{'received':now,'duration':.1})},{'TESTUSDT':META},CFG,{'end':10**10},now,'late')
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM positions').fetchone()[0],0)
  self.assertEqual(self.c.execute('SELECT status FROM events').fetchone()[0],'REJECTED')
 def test_minute_stop_survives_recovery_and_missing_exit_book(self):
  eid,due=event(self.c);now=due+1;s.entries(self.c,{'TESTUSDT':(book(),{'received':now,'duration':.1})},{'TESTUSDT':META},CFG,{'end':10**10},now,'entry')
  s.mark_accounts(self.c,{'TESTUSDT':(100,now)},now,CFG)
  t=(now//s.MINUTE+1)*s.MINUTE;b=[bar(t,92),bar(t+s.MINUTE,100)]
  s.mark_minute_paths(self.c,{'TESTUSDT':b},CFG,{'start':0},t+2*s.MINUTE)
  self.assertIsNotNone(s.getkv(self.c,'stop:A0:2000:15:TESTUSDT'))
  s.exits(self.c,{'TESTUSDT':(100,t+2*s.MINUTE)},{},CFG,{'end':10**10},t+2*s.MINUTE,'none')
  self.assertGreater(self.c.execute('SELECT COUNT(*) FROM positions').fetchone()[0],0)
 def test_breaker_peak_never_resets(self):
  self.c.execute("UPDATE accounts SET cash=2100 WHERE k='A0:2000:15'");s.mark_accounts(self.c,{},1000,CFG)
  self.c.execute("UPDATE accounts SET cash=2030 WHERE k='A0:2000:15'");s.mark_accounts(self.c,{},2000,CFG)
  a=self.c.execute("SELECT * FROM accounts WHERE k='A0:2000:15'").fetchone();self.assertEqual(a['peak'],2100);self.assertEqual(a['halt'],1)
 def test_trade_limit_blocks_new_exposure(self):
  self.c.execute("UPDATE accounts SET entries=24 WHERE model='A0'");eid,due=event(self.c)
  s.entries(self.c,{'TESTUSDT':(book(),{'received':due+1,'duration':.1})},{'TESTUSDT':META},CFG,{'end':10**10},due+1,'book')
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM positions').fetchone()[0],0)
 def test_fee_cap_blocks_entries_but_not_exits(self):
  self.c.execute("UPDATE accounts SET fees=100 WHERE model='A0'");eid,due=event(self.c)
  s.entries(self.c,{'TESTUSDT':(book(),{'received':due+1,'duration':.1})},{'TESTUSDT':META},CFG,{'end':10**10},due+1,'book')
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM positions').fetchone()[0],0)
 def test_depth_haircut_and_locked_book_rejected(self):
  for raw in [book(qty='.5'),{'asks':[['100','100']],'bids':[['100','100']]}]:
   with self.assertRaises(ValueError):s.book_quote(raw,META,76.9230769,CFG)

 def test_restart_does_not_repeat_cash_movement(self):
  eid,due=event(self.c);self.c.commit()
  books={'TESTUSDT':(book(),{'received':due+1,'duration':.1})}
  s.entries(self.c,books,{'TESTUSDT':META},CFG,{'end':10**10},due+1,'book');self.c.commit()
  before=self.c.execute("SELECT cash FROM accounts WHERE k='A0:2000:15'").fetchone()[0]
  self.c.close();self.c=s.connect(pathlib.Path(self.t.name)/'test.sqlite')
  s.entries(self.c,books,{'TESTUSDT':META},CFG,{'end':10**10},due+1,'book')
  self.assertEqual(before,self.c.execute("SELECT cash FROM accounts WHERE k='A0:2000:15'").fetchone()[0])
  self.assertEqual(self.c.execute("SELECT COUNT(*) FROM fills WHERE side='BUY'").fetchone()[0],6)
 def test_uncommitted_fill_rolls_back_as_one_transaction(self):
  eid,due=event(self.c);self.c.commit();books={'TESTUSDT':(book(),{'received':due+1,'duration':.1})}
  s.entries(self.c,books,{'TESTUSDT':META},CFG,{'end':10**10},due+1,'book');self.c.close();self.c=s.connect(pathlib.Path(self.t.name)/'test.sqlite')
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM fills').fetchone()[0],0)
  self.assertEqual(self.c.execute("SELECT cash FROM accounts WHERE k='A0:2000:15'").fetchone()[0],2000)
  s.entries(self.c,books,{'TESTUSDT':META},CFG,{'end':10**10},due+1,'book')
  self.assertEqual(self.c.execute("SELECT COUNT(*) FROM fills WHERE side='BUY'").fetchone()[0],6)
 def test_fee_budget_does_not_block_risk_reducing_exit(self):
  eid,due=event(self.c);books={'TESTUSDT':(book(),{'received':due+1,'duration':.1})}
  s.entries(self.c,books,{'TESTUSDT':META},CFG,{'end':10**10},due+1,'book')
  self.c.execute("UPDATE accounts SET fees=100 WHERE model='A0'")
  s.exits(self.c,{'TESTUSDT':(100,due+2)},books,CFG,{'end':due+1},due+2,'exit')
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM positions').fetchone()[0],0)
 def test_exit_dust_is_not_fabricated_as_cash(self):
  with self.assertRaises(ValueError):s.sell_quote(book(1),'0.7',CFG,META)
 def test_book_latency_cannot_create_an_entry(self):
  eid,due=event(self.c);books={'TESTUSDT':(book(),{'received':due+1,'duration':5.1})}
  s.entries(self.c,books,{'TESTUSDT':META},CFG,{'end':10**10},due+1,'book')
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM positions').fetchone()[0],0)

if __name__=='__main__':
 suite=unittest.defaultTestLoader.loadTestsFromModule(__import__(__name__));result=unittest.TextTestRunner(verbosity=2).run(suite)
 obj={'success':result.wasSuccessful(),'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'hashes':{n:s.digest(s.ROOT/n) for n in ['study.py','tests.py','config.json']}}
 (s.ROOT/'TEST_RESULTS.json').write_text(json.dumps(obj,indent=2))
 raise SystemExit(0 if result.wasSuccessful() else 1)
