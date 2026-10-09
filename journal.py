"""Immutable SQLite state deltas for the frozen research runner."""
import contextlib,gzip,hashlib,json,pathlib,sqlite3,tempfile
TABLES=('bars','kv','events','accounts','positions','fills','marks','issues')
@contextlib.contextmanager
def materialized_seed(root):
 root=pathlib.Path(root);info=json.loads((root/'SEED.json').read_text())
 if (root/'seed.sqlite.gz').exists():
  yield root/'seed.sqlite.gz';return
 with tempfile.TemporaryDirectory(prefix='spot-seed-') as tmp:
  dst=pathlib.Path(tmp)/'seed.sqlite.gz'
  with dst.open('wb') as out:
   for part in info['seed_parts']:
    name=pathlib.PurePosixPath(part['path'])
    if name.parent!=pathlib.PurePosixPath('seed') or '..' in name.parts:raise ValueError('Invalid seed part path')
    p=root/pathlib.Path(name)
    if p.stat().st_size!=part['bytes'] or sha(p)!=part['sha256']:raise ValueError('Seed part identity mismatch')
    with p.open('rb') as h:
     while True:
      b=h.read(1048576)
      if not b:break
      out.write(b)
  if sha(dst)!=info['seed_sha256']:raise ValueError('Combined seed hash mismatch')
  yield dst
def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def schema(c,t):
 if t not in TABLES:raise ValueError('Unknown journal table')
 info=c.execute('PRAGMA table_info('+t+')').fetchall();cols=[x[1] for x in info];keys=[x[1] for x in sorted(info,key=lambda x:x[5]) if x[5]]
 if not keys:raise ValueError('Table lacks primary key')
 return cols,keys

def install(c):
 c.execute('CREATE TABLE IF NOT EXISTS cloud_dirty(tab TEXT,key_json TEXT,PRIMARY KEY(tab,key_json))')
 for t in TABLES:
  _,keys=schema(c,t)
  for op,ref in [('INSERT','NEW'),('UPDATE','NEW'),('DELETE','OLD')]:
   vals=','.join(ref+'.'+k for k in keys)
   c.execute(f"CREATE TRIGGER IF NOT EXISTS cloud_{t}_{op} AFTER {op} ON {t} BEGIN INSERT INTO cloud_dirty SELECT '{t}',json_array({vals}) WHERE NOT EXISTS (SELECT 1 FROM cloud_dirty WHERE tab='{t}' AND key_json=json_array({vals})); END")
 c.commit()

def checkpoint(c,root,seq,parent):
 root=pathlib.Path(root);root.mkdir(parents=True,exist_ok=True);changes=[]
 c.execute('BEGIN IMMEDIATE')
 try:
  for t,key in c.execute('SELECT tab,key_json FROM cloud_dirty ORDER BY tab,key_json').fetchall():
   cols,keys=schema(c,t);values=json.loads(key);where=' AND '.join(k+'=?' for k in keys)
   row=c.execute('SELECT '+','.join(cols)+' FROM '+t+' WHERE '+where,values).fetchone()
   changes.append({'table':t,'key':values,'columns':cols,'row':list(row) if row is not None else None})
  payload={'sequence':seq,'parent_sha256':parent,'changes':changes}
  p=root/f'{seq:08d}.json.gz'
  if p.exists():raise ValueError('Checkpoint overwrite forbidden')
  with gzip.open(p,'wt') as h:json.dump(payload,h,separators=(',',':'),allow_nan=False)
  c.execute('DELETE FROM cloud_dirty');c.commit();return p,sha(p)
 except BaseException:c.rollback();raise

def replay(c,paths,parent):
 previous=parent;seq=0
 for p in sorted(paths):
  with gzip.open(p,'rt') as h:data=json.load(h)
  if data['sequence']!=seq+1 or data['parent_sha256']!=previous:raise ValueError('Broken checkpoint chain')
  with c:
   for x in data['changes']:
    t=x['table'];cols,keys=schema(c,t)
    if x['columns']!=cols or len(x['key'])!=len(keys):raise ValueError('Checkpoint schema mismatch')
    if x['row'] is None:c.execute('DELETE FROM '+t+' WHERE '+' AND '.join(k+'=?' for k in keys),x['key'])
    else:
     if len(x['row'])!=len(cols):raise ValueError('Checkpoint row mismatch')
     # REPLACE is safe here: the frozen SQLite schema has no foreign-key cascades.
     c.execute('INSERT OR REPLACE INTO '+t+' ('+','.join(cols)+') VALUES ('+','.join('?' for _ in cols)+')',x['row'])
  previous=sha(p);seq+=1
 return seq,previous

def restore(seed,manifest,deltas,destination):
 info=json.loads(pathlib.Path(manifest).read_text())
 if sha(seed)!=info['seed_sha256']:raise ValueError('Seed hash mismatch')
 dst=pathlib.Path(destination)
 if dst.exists():raise ValueError('Existing runtime state must not be overwritten')
 with gzip.open(seed,'rb') as r,dst.open('wb') as w:
  while True:
   b=r.read(1048576)
   if not b:break
   w.write(b)
 c=sqlite3.connect(dst)
 if c.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise ValueError('SQLite seed integrity')
 seq,parent=replay(c,pathlib.Path(deltas).glob('*.json.gz'),info['seed_sha256']);install(c);c.close();return seq,parent
