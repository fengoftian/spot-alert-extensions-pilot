import gzip,json,pathlib,sqlite3,tempfile,unittest
import journal
class JournalTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.tmp.name);self.c=sqlite3.connect(self.root/'source.sqlite')
  for t in journal.TABLES:self.c.execute('CREATE TABLE '+t+'(id INTEGER PRIMARY KEY,value TEXT)')
  self.c.execute("INSERT INTO accounts VALUES(1,'cash=2000')");self.c.commit();self.seed=self.root/'seed.sqlite.gz'
  with open(self.root/'source.sqlite','rb') as r,gzip.open(self.seed,'wb') as w:w.write(r.read())
  (self.root/'SEED.json').write_text(json.dumps({'seed_sha256':journal.sha(self.seed)}));journal.install(self.c)
 def tearDown(self):self.c.close();self.tmp.cleanup()
 def test_roundtrip_updates_inserts_and_deletes(self):
  self.c.execute("UPDATE accounts SET value='cash=1990' WHERE id=1");self.c.execute("INSERT INTO positions VALUES(1,'held')");self.c.commit()
  p,parent=journal.checkpoint(self.c,self.root/'journal',1,journal.sha(self.seed))
  self.c.execute('DELETE FROM positions');self.c.execute("INSERT INTO fills VALUES(1,'one fill')");self.c.commit();journal.checkpoint(self.c,self.root/'journal',2,parent)
  seq,_=journal.restore(self.seed,self.root/'SEED.json',self.root/'journal',self.root/'restored.sqlite');r=sqlite3.connect(self.root/'restored.sqlite')
  self.assertEqual(seq,2);self.assertEqual(r.execute('SELECT value FROM accounts').fetchone()[0],'cash=1990');self.assertEqual(r.execute('SELECT COUNT(*) FROM positions').fetchone()[0],0);self.assertEqual(r.execute('SELECT COUNT(*) FROM fills').fetchone()[0],1);r.close()
 def test_checkpoint_cannot_be_overwritten(self):
  journal.checkpoint(self.c,self.root/'journal',1,journal.sha(self.seed))
  with self.assertRaises(ValueError):journal.checkpoint(self.c,self.root/'journal',1,journal.sha(self.seed))
 def test_missing_delta_fails_chain(self):
  p,parent=journal.checkpoint(self.c,self.root/'journal',1,journal.sha(self.seed));p2,_=journal.checkpoint(self.c,self.root/'journal',2,parent)
  r=sqlite3.connect(self.root/'other.sqlite')
  for t in journal.TABLES:r.execute('CREATE TABLE '+t+'(id INTEGER PRIMARY KEY,value TEXT)')
  with self.assertRaises(ValueError):journal.replay(r,[p2],journal.sha(self.seed))
  r.close()
 def test_tampered_seed_fails(self):
  with self.seed.open('ab') as h:h.write(b'bad')
  with self.assertRaises(ValueError):journal.restore(self.seed,self.root/'SEED.json',self.root/'journal',self.root/'bad.sqlite')
 def test_existing_state_never_reset(self):
  dst=self.root/'existing.sqlite';dst.write_bytes(b'keep')
  with self.assertRaises(ValueError):journal.restore(self.seed,self.root/'SEED.json',self.root/'journal',dst)
  self.assertEqual(dst.read_bytes(),b'keep')
if __name__=='__main__':unittest.main(verbosity=2)
