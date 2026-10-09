"""Cloud-only public warmup, future freeze and initial durable observations seed."""
import concurrent.futures,datetime as dt,gzip,hashlib,importlib.util,json,os,pathlib,shutil,sqlite3,subprocess,tempfile,time
ROOT=pathlib.Path(__file__).resolve().parent;GEN=ROOT/'gen'
spec=importlib.util.spec_from_file_location('extension_study',GEN/'study.py');s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)

def git(*args,cwd=ROOT):
 p=subprocess.run(['git','-c','user.name=github-actions[bot]','-c','user.email=41898282+github-actions[bot]@users.noreply.github.com',*args],cwd=cwd,capture_output=True,text=True,check=True);return p.stdout.strip()

def main():
 if os.environ.get('GITHUB_ACTIONS')!='true' or os.environ.get('GITHUB_REPOSITORY')!='fengoftian/spot-alert-extensions-pilot':raise ValueError('Cloud repository identity required')
 if (GEN/'FREEZE.json').exists() or (GEN/'state.sqlite').exists():raise ValueError('Existing study cannot be reset')
 cfg=s.config();seed_cohort=json.loads((GEN/'COHORT_SEED.json').read_text());symbols=seed_cohort['collected_symbols']
 info,receipt=s.request('exchangeInfo',{'symbols':json.dumps(symbols,separators=(',',':')),'showPermissionSets':'false'})
 if not info or receipt.get('status')!=200:raise ValueError('New inception metadata unavailable')
 current={x['symbol']:x for x in info['symbols']}
 if set(current)!=set(symbols) or any(current[x]['status']!='TRADING' or not current[x].get('isSpotTradingAllowed') for x in symbols):raise ValueError('Frozen cohort member unavailable; no replacement permitted')
 end=s.clock_ms()//s.MINUTE*s.MINUTE;start=end-(31*1440+65)*s.MINUTE
 cohort={'captured':s.clock_ms(),'warmup_end':end,'members':[current[x['symbol']] for x in seed_cohort['members']],
  'collected_symbols':symbols,'benchmark_metadata':{x:current[x] for x in ['BTCUSDT','ETHUSDT']},
  'selection':'Same 44 inception identities as the owner-authorized A study, no outcome selection or replacement; metadata refreshed before B freeze'}
 (GEN/'COHORT.json').write_text(json.dumps(cohort,indent=2)+'\n');s.save_raw([receipt],'new-inception')
 c=s.connect();count=31*1440+65
 with concurrent.futures.ThreadPoolExecutor(max_workers=cfg['maximum_workers']) as pool:
  for symbol,b,rr in pool.map(lambda symbol:s.fetch_symbol(symbol,start,end,46),symbols):
   s.save_raw(rr,'warmup-'+symbol)
   if len(b)!=count or any(x[0]!=start+i*s.MINUTE for i,x in enumerate(b)):raise ValueError('Incomplete public warmup: '+symbol)
   s.store_bars(c,symbol,b,s.clock_ms(),end);c.commit();print('Warmup',symbol,len(b),flush=True)
 s.create_accounts(c,cfg)
 # Prime rising-edge state after all BTC and cohort candles exist. No warmup events are eligible.
 for symbol in symbols:
  rows=[json.loads(x[0]) for x in c.execute('SELECT raw FROM bars WHERE symbol=? ORDER BY t',(symbol,))]
  s.setkv(c,'processed:'+symbol,rows[-2][0]);s.process_signals(c,symbol,rows,cfg,{'start':end+10**12,'end':end+10**12+86400000},end)
  day=rows[-1][0]//86400000*86400000
  if s.daily_return_threshold(c,symbol,day,cfg) is None:raise ValueError('Incomplete B0 calibration: '+symbol)
 s.setkv(c,'stage','WARMUP_BEFORE_FUTURE_START');c.commit()
 tested=json.loads((GEN/'TEST_RESULTS.json').read_text())
 if not tested['success'] or any(s.digest(GEN/n)!=sha for n,sha in tested['hashes'].items()):raise ValueError('Tests not current')
 now=s.clock_ms();future=((now+20*s.MINUTE+5*s.MINUTE-1)//(5*s.MINUTE))*(5*s.MINUTE);finish=future+30*86400000
 names=['engine.py','study.py','config.json','DESIGN.md','COHORT_SEED.json','COHORT.json','TEST_RESULTS.json','engine_tests.py','engine_test_config.json','../test_alerts.py']
 freeze={'frozen':now,'start':future,'end':finish,'tail_end':finish+4*3600000,'start_utc':s.utc(future),'end_utc':s.utc(finish),'start_nzdt':s.nz(future),'end_nzdt':s.nz(finish),
  'hashes':{n:s.digest(GEN/n) for n in names},'authority':'OWNER_AUTHORIZED_PUBLIC_DATA_AND_SIMULATED_B_SCREEN_ONLY','comparisons':['B0-C_A2','B1-C_A2','B2-C_A2','B3-C_A2']}
 (GEN/'FREEZE.json').write_text(json.dumps(freeze,indent=2)+'\n');s.engine.write_status(c,freeze)
 counts={t:c.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ['bars','kv','events','accounts','positions','fills','marks','issues']}
 if counts['accounts']!=45 or any(counts[t] for t in ['events','positions','fills','marks']):raise ValueError('Warmup unexpectedly exercised prospective economics')
 c.commit();c.close()
 git('add','gen/COHORT.json','gen/FREEZE.json','gen/TEST_RESULTS.json');git('commit','-m','Freeze independent B study before its future start');git('push','origin','HEAD:main')
 with tempfile.TemporaryDirectory(prefix='spot-seed-export-') as tmp:
  tmp=pathlib.Path(tmp);archive=tmp/'seed.sqlite.gz'
  with (GEN/'state.sqlite').open('rb') as src,gzip.open(archive,'wb') as dst:shutil.copyfileobj(src,dst)
  obs=tmp/'observations';git('worktree','add','-b','observations',str(obs),'HEAD');git('rm','-r','--','.',cwd=obs)
  (obs/'seed').mkdir();parts=[]
  with archive.open('rb') as h:
   i=0
   while True:
    body=h.read(64*1024*1024)
    if not body:break
    name=f'seed/{i:04d}.part';(obs/name).write_bytes(body);parts.append({'path':name,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()});i+=1
  manifest={'seed_sha256':s.digest(archive),'sqlite_sha256':s.digest(GEN/'state.sqlite'),'seed_parts':parts,'snapshot_counts':counts,
   'warmup_only':True,'freeze_sha256':s.digest(GEN/'FREEZE.json'),'source_commit':git('rev-parse','HEAD'),'created_utc':dt.datetime.now(dt.timezone.utc).isoformat()}
  (obs/'SEED.json').write_text(json.dumps(manifest,indent=2)+'\n');shutil.copyfile(GEN/'STATUS.json',obs/'STATUS.json');shutil.copytree(GEN/'raw',obs/'raw')
  (obs/'journal').mkdir();git('add','--','.',cwd=obs);git('commit','-m','Save public warmup-only B-study seed and raw receipts',cwd=obs);git('push','origin','HEAD:observations',cwd=obs)
 print(json.dumps({'frozen_window':freeze,'seed_parts':len(parts),'accounts':45,'warmup_bars':counts['bars'],'paid_services':False},indent=2),flush=True)

if __name__=='__main__':main()
