import contextlib,gzip,importlib.util,json,pathlib,sqlite3,tempfile,unittest
import journal
ROOT=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('engine',ROOT/'gen/study.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class RealSchemaTests(unittest.TestCase):
 def test_compound_keys_and_account_state_survive_restart(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=pathlib.Path(tmp);c=m.connect(p/'initial.sqlite');m.create_accounts(c,m.config());c.commit()
   with (p/'initial.sqlite').open('rb') as r,gzip.open(p/'seed.sqlite.gz','wb') as w:w.write(r.read())
   sha=journal.sha(p/'seed.sqlite.gz');(p/'SEED.json').write_text(json.dumps({'seed_sha256':sha}));journal.install(c)
   c.execute("UPDATE accounts SET cash=1990,fees=1,entries=1,peak=2020 WHERE k='C_A2:2000:15'")
   c.execute("INSERT INTO positions VALUES('C_A2:2000:15','BTCUSDT','0.01',100,1000,5000,'event')")
   c.execute("INSERT INTO marks VALUES('C_A2:2000:15',1000,2000,10,1,'[]')");c.commit()
   f,h=journal.checkpoint(c,p/'journal',1,sha)
   c.execute("DELETE FROM positions WHERE account='C_A2:2000:15'");c.commit();journal.checkpoint(c,p/'journal',2,h)
   seq,parent=journal.restore(p/'seed.sqlite.gz',p/'SEED.json',p/'journal',p/'restored.sqlite');r=sqlite3.connect(p/'restored.sqlite')
   self.assertEqual(seq,2);self.assertEqual(r.execute("SELECT cash,fees,entries,peak FROM accounts WHERE k='C_A2:2000:15'").fetchone(),(1990,1,1,2020));self.assertEqual(r.execute('SELECT COUNT(*) FROM positions').fetchone()[0],0);self.assertEqual(r.execute('SELECT COUNT(*) FROM marks').fetchone()[0],1)
   r.close();c.close()
 def test_original_upsert_can_update_same_dirty_key_repeatedly(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=pathlib.Path(tmp);c=m.connect(p/'state.sqlite');journal.install(c)
   for i in range(3):m.setkv(c,'processed:BTCUSDT',i);c.commit()
   self.assertEqual(c.execute("SELECT COUNT(*) FROM cloud_dirty WHERE tab='kv'").fetchone()[0],1)
   self.assertEqual(m.getkv(c,'processed:BTCUSDT'),2)
   c.close()
if __name__=='__main__':unittest.main(verbosity=2)
