import copy,importlib.util,json,pathlib,tempfile,unittest
ROOT=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('b_study',ROOT/'gen/study.py');s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)
CFG=s.config();M=s.MINUTE
def bar(t,p=100,q=100,buy=.5):return [t,str(p),str(p*1.001),str(p*.999),str(p),str(q/p),t+M-1,str(q),1,str(q*buy/p),str(q*buy),'0']
def history():
 b=[bar(i*M) for i in range(s.engine.WARM)]
 for i in range(len(b)-60,len(b)):b[i]=bar(i*M,111,400,.65)
 b[-1]=bar(b[-1][0],125,400,.65)
 return b
class AlertTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.c=s.connect(pathlib.Path(self.tmp.name)/'state.sqlite')
  self.rows=history();day=self.rows[-1][0]//86400000*86400000
  s.setkv(self.c,f'b0_calibration:TESTUSDT:{day}',{'quantile':.015,'floor':.01,'observations':2880,'baseline_start':day-30*86400000,'baseline_end':day})
 def tearDown(self):self.c.close();self.tmp.cleanup()
 def feature(self,rows=None,btc=None):
  rows=rows or self.rows;i=len(rows)-1;btc=btc if btc is not None else {x[0]:100 for x in rows}
  return s.conditions(self.c,'TESTUSDT',rows,i,CFG,s.engine.feature_series(rows,CFG)(i),btc)
 def test_control_exactly_matches_original_a2(self):
  f=self.feature();original=s.engine.feature_series(self.rows,CFG)(len(self.rows)-1)
  self.assertEqual(f['qualifies']['C_A2'],original['qualifies']['A2']);self.assertTrue(f['qualifies']['C_A2'])
 def test_earlier_b0_can_trigger_before_ten_percent_hour(self):
  rows=[bar(i*M) for i in range(s.engine.WARM)]
  for i in range(len(rows)-15,len(rows)):rows[i]=bar(i*M,102,400)
  f=self.feature(rows);self.assertTrue(f['qualifies']['B0']);self.assertFalse(f['qualifies']['C_A2'])
 def test_b0_requires_a_complete_calibration(self):
  self.c.execute("DELETE FROM kv WHERE k LIKE 'b0_calibration:%'")
  self.assertIsNone(self.feature()['qualifies']['B0'])
 def test_b0_floor_rejects_tiny_move_even_if_quantile_is_low(self):
  rows=[bar(i*M) for i in range(s.engine.WARM)]
  for i in range(len(rows)-15,len(rows)):rows[i]=bar(i*M,100.5,400)
  day=rows[-1][0]//86400000*86400000;s.setkv(self.c,f'b0_calibration:TESTUSDT:{day}',{'quantile':.001,'floor':.01})
  self.assertFalse(self.feature(rows)['qualifies']['B0'])
 def test_b1_rejects_move_fully_mirrored_by_btc(self):
  btc={x[0]:float(x[4]) for x in self.rows};self.assertFalse(self.feature(btc=btc)['qualifies']['B1'])
 def test_b1_missing_synchronized_btc_is_unknown(self):
  btc={x[0]:100 for x in self.rows};del btc[self.rows[-70][0]]
  self.assertIsNone(self.feature(btc=btc)['qualifies']['B1'])
 def test_b2_uses_five_minute_quote_volume_share(self):
  f=self.feature();self.assertAlmostEqual(f['taker_buy_share_5m'],.65);self.assertTrue(f['qualifies']['B2'])
  for x in self.rows[-5:]:x[10]=str(float(x[7])*.59)
  self.assertFalse(self.feature()['qualifies']['B2'])
 def test_retest_requires_two_consecutive_completed_candles(self):
  signal=20*M;s.register(self.c,'B3','TESTUSDT',signal,{'prior_high':100},CFG)
  first=bar(signal,102);first[3]='100';second=bar(signal+M,104)
  s.confirm_retests(self.c,'TESTUSDT',[first,second],CFG,signal+2*M-1)
  self.assertEqual(self.c.execute('SELECT status FROM events').fetchone()[0],'PENDING_RETEST')
  s.confirm_retests(self.c,'TESTUSDT',[first,second],CFG,signal+2*M)
  row=self.c.execute('SELECT status,due,deadline FROM events').fetchone();self.assertEqual(tuple(row),('PENDING',signal+3*M,signal+5*M))
 def test_missing_retest_candle_expires_without_confirmation(self):
  signal=20*M;s.register(self.c,'B3','TESTUSDT',signal,{'prior_high':100},CFG)
  first=bar(signal,102);first[3]='100';second=bar(signal+2*M,104)
  s.confirm_retests(self.c,'TESTUSDT',[first,second],CFG,signal+15*M)
  self.assertEqual(self.c.execute('SELECT status FROM events').fetchone()[0],'REJECTED')
  self.assertEqual(self.c.execute('SELECT reason FROM events').fetchone()[0],'RETEST_WINDOW_EXPIRED_OR_MISSING')
 def test_late_recovered_retest_cannot_backfill_an_entry(self):
  signal=20*M;s.register(self.c,'B3','TESTUSDT',signal,{'prior_high':100},CFG)
  first=bar(signal,102);first[3]='100';second=bar(signal+M,104)
  s.confirm_retests(self.c,'TESTUSDT',[first,second],CFG,signal+10*M)
  s.create_accounts(self.c,CFG);s.entries(self.c,{}, {},CFG,{'end':10**12},signal+10*M,'none')
  self.assertEqual(self.c.execute('SELECT status FROM events').fetchone()[0],'REJECTED');self.assertEqual(self.c.execute('SELECT count(*) FROM fills').fetchone()[0],0)
 def test_five_models_use_45_separate_accounts(self):
  s.create_accounts(self.c,CFG);self.assertEqual(self.c.execute('SELECT COUNT(*) FROM accounts').fetchone()[0],45)
 def test_rising_edge_cooldown_and_replay_do_not_duplicate_events(self):
  for x in self.rows:s.store_bars(self.c,'BTCUSDT',[bar(x[0])],x[6]+1,x[6]+1)
  s.setkv(self.c,'qual:TESTUSDT',{m:False for m in s.MODELS});s.setkv(self.c,'processed:TESTUSDT',self.rows[-2][0])
  f={'start':self.rows[-1][0],'end':self.rows[-1][0]+86400000}
  s.process_signals(self.c,'TESTUSDT',self.rows,CFG,f,self.rows[-1][6]+1)
  s.process_signals(self.c,'TESTUSDT',self.rows,CFG,f,self.rows[-1][6]+2)
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM events').fetchone()[0],5)
 def test_warmup_true_conditions_do_not_create_initial_events(self):
  for x in self.rows:s.store_bars(self.c,'BTCUSDT',[bar(x[0])],x[6]+1,x[6]+1)
  s.setkv(self.c,'qual:TESTUSDT',{m:True for m in s.MODELS});s.setkv(self.c,'processed:TESTUSDT',self.rows[-2][0])
  s.process_signals(self.c,'TESTUSDT',self.rows,CFG,{'start':0,'end':10**12},self.rows[-1][6]+1)
  self.assertEqual(self.c.execute('SELECT COUNT(*) FROM events').fetchone()[0],0)
 def test_future_candles_do_not_change_past_features(self):
  i=len(self.rows)-1;f=self.feature();b=self.rows+[bar(self.rows[-1][0]+M,99999,1e12)];btc={x[0]:100 for x in b}
  after=s.conditions(self.c,'TESTUSDT',b,i,CFG,s.engine.feature_series(b,CFG)(i),btc);self.assertEqual(f,after)
 def test_daily_calibration_has_2880_nonoverlapping_returns_and_excludes_current_day(self):
  c=self.c;day=31*86400000;start=day-30*86400000
  for i in range(30*1440+1):
   x=bar(start-M+i*M);c.execute('INSERT INTO bars VALUES(?,?,?,?)',('CALUSDT',x[0],json.dumps(x),day))
  c.execute('INSERT INTO bars VALUES(?,?,?,?)',('CALUSDT',day,json.dumps(bar(day,99999)),day))
  out=s.daily_return_threshold(c,'CALUSDT',day,CFG);self.assertEqual(out['observations'],2880);self.assertEqual(out['quantile'],0);self.assertEqual(out['baseline_end'],day)
if __name__=='__main__':unittest.main(verbosity=2)
