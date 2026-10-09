"""GitHub-hosted minute collector, bounded lease and durable state journal."""
import argparse,datetime as dt,gzip,hashlib,importlib.util,json,os,pathlib,shutil,sqlite3,subprocess,sys,tempfile,time
import journal
HERE=pathlib.Path(__file__).resolve().parent;GEN=HERE/'gen'

def load_study():
 spec=importlib.util.spec_from_file_location('frozen_spot_study',GEN/'study.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);m.load_freeze();return m

def git(data,*args):
 p=subprocess.run(['git','-C',str(data),*args],capture_output=True,text=True)
 if p.returncode:raise RuntimeError('Git journal persistence failed: '+p.stderr[:500])
 return p.stdout.strip()

def archive_runtime(data):
 # Archive only new raw receipts; their original relative paths stay recorded.
 source=GEN/'raw';dest=data/'raw';dest.mkdir(exist_ok=True);staged=[]
 if source.exists():
  for p in source.iterdir():
   if not p.is_file():continue
   target=dest/p.name
   if target.exists() and journal.sha(target)!=journal.sha(p):raise ValueError('Raw receipt collision')
   if not target.exists():shutil.copyfile(p,target)
   staged.append('raw/'+p.name)
 for n in ['STATUS.json','RESULTS.json','RESULTS.md']:
  if (GEN/n).exists():shutil.copyfile(GEN/n,data/n);staged.append(n)
 return staged

def persist(data,c,seq,parent):
 seq+=1;p=data/'journal'/f'{seq:08d}.json.gz'
 if p.exists():
  with gzip.open(p,'rt') as h:existing=json.load(h)
  if existing['sequence']!=seq or existing['parent_sha256']!=parent:raise ValueError('Cannot reuse a conflicting unpublished checkpoint')
  new_parent=journal.sha(p)
 else:p,new_parent=journal.checkpoint(c,data/'journal',seq,parent)
 # Identify the actual producer without changing the frozen engine's status output.
 status=json.loads((GEN/'STATUS.json').read_text()) if (GEN/'STATUS.json').exists() else {}
 (data/'CLOUD_STATUS.json').write_text(json.dumps({
  'host':'GITHUB_ACTIONS' if os.environ.get('GITHUB_ACTIONS')=='true' else 'LOCAL_ADAPTER_TEST',
  'repository':os.environ.get('GITHUB_REPOSITORY'),
  'run_id':os.environ.get('GITHUB_RUN_ID'),
  'source_commit':os.environ.get('GITHUB_SHA'),
  'checkpoint_sequence':seq,'checkpoint_sha256':new_parent,
  'published_state_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
  'stage':status.get('stage'),'last_tick':status.get('last_tick'),
  'scientific_rules_changed':False
 },indent=2)+'\n')
 staged=archive_runtime(data)
 staged.append('CLOUD_STATUS.json')
 git(data,'add','--','journal')
 if staged:git(data,'add','--sparse','--',*staged)
 changed=subprocess.run(['git','-C',str(data),'diff','--cached','--quiet']).returncode
 if changed:git(data,'-c','user.name=github-actions[bot]','-c','user.email=41898282+github-actions[bot]@users.noreply.github.com','commit','-m',f'Append study checkpoint {seq}')
 git(data,'push','origin','HEAD:observations')
 # Raw observations remain durably archived; remove ephemeral duplicates only AFTER successful push.
 if (GEN/'raw').exists():
  for p in (GEN/'raw').iterdir():
   if p.is_file() and (data/'raw'/p.name).exists() and journal.sha(p)==journal.sha(data/'raw'/p.name):p.unlink()
 return seq,new_parent

def preflight(out):
 m=load_study();receipts=[];details=[]
 for endpoint,params in [('time',{}),('klines',{'symbol':'BTCUSDT','interval':'1m','limit':4}),('depth',{'symbol':'OGNUSDT','limit':100})]:
  obj,rec=m.request(endpoint,params);receipts.append(rec)
  if rec.get('status')!=200:raise RuntimeError('Hosted public-data preflight failed: '+str(rec.get('error')))
  if endpoint=='klines':
   for x in obj:m.validate_bar(x)
  details.append({'endpoint':endpoint,'status':rec['status'],'duration':rec['duration'],'sha256':rec['sha256']})
 pathlib.Path(out).write_text(json.dumps({'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'frozen_identity':'MATCH','public_transport':details,'strategy_evaluated':False},indent=2))
 print('Public transport and frozen identity preflight: PASS')

def check_engine(c,m):
 stage=m.getkv(c,'stage')
 if stage in {'INTEGRITY_HALT','DISK_CAP_HALT'} or m.getkv(c,'integrity_halt',False):
  raise RuntimeError('Frozen engine halted: '+str(stage)+'; state saved, operator review required')

def hosted_repo():
 repo=os.environ.get('GITHUB_REPOSITORY','')
 if repo!='fengoftian/spot-alert-extensions-pilot':raise ValueError('Unexpected hosted repository identity')
 return repo

def handoff_or_stop(m,f):
 if os.environ.get('GITHUB_ACTIONS')!='true':return
 repo=hosted_repo()
 if m.clock_ms()>=f['tail_end']:
  subprocess.run(['gh','api','repos/'+repo+'/actions/workflows/collector.yml/disable','--method','PUT'],check=True)
 else:
  # Dispatch only after a healthy lease and successful durable final checkpoint.
  # The single-writer group holds the successor until this run releases it.
  subprocess.run(['gh','workflow','run','collector.yml','--repo',repo,'--ref','main','-f','mode=collect'],check=True)
  print('Successor requested; cron remains a best-effort fallback')

def collect(data,lease_seconds,checkpoint_seconds):
 data=pathlib.Path(data).resolve();m=load_study();f=m.load_freeze()
 with journal.materialized_seed(data) as seed:
  seq,parent=journal.restore(seed,data/'SEED.json',data/'journal',GEN/'state.sqlite')
 c=sqlite3.connect(GEN/'state.sqlite');deadline=min(time.monotonic()+lease_seconds,time.monotonic()+max(0,(f['tail_end']-m.clock_ms())/1000));last=time.monotonic()
 try:
  while time.monotonic()<deadline:
   began=time.monotonic();subprocess.run([sys.executable,str(GEN/'study.py'),'tick'],check=True)
   check_engine(c,m)
   if time.monotonic()-last>=checkpoint_seconds:seq,parent=persist(data,c,seq,parent);last=time.monotonic()
   time.sleep(max(1,60-(time.monotonic()-began)))
  if m.clock_ms()>=f['tail_end']:
   # The frozen terminal tick updates completion status and reports without fetching market data.
   subprocess.run([sys.executable,str(GEN/'study.py'),'tick'],check=True)
 finally:
  # Fail closed if the final state cannot be saved; the next job restores only a committed chain.
  seq,parent=persist(data,c,seq,parent);c.close()
 handoff_or_stop(m,f)
 print('Hosted lease ended; last durable checkpoint',seq)

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('mode',choices=['preflight','collect']);p.add_argument('--output',default='cloud-preflight.json');p.add_argument('--data',default='observations');p.add_argument('--lease-seconds',type=int,default=20100);p.add_argument('--checkpoint-seconds',type=int,default=300);a=p.parse_args()
 if a.lease_seconds<60 or a.lease_seconds>20100 or a.checkpoint_seconds<60:raise ValueError('Lease/checkpoint bounds')
 if a.mode=='preflight':preflight(a.output)
 else:collect(a.data,a.lease_seconds,a.checkpoint_seconds)
