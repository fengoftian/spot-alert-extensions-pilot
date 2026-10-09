"""Synthetic tests only; all original-engine output is confined to a disposable copy."""
import hashlib,json,pathlib,shutil,subprocess,sys,tempfile
ROOT=pathlib.Path(__file__).resolve().parent;GEN=ROOT/'gen'
with tempfile.TemporaryDirectory() as tmp:
 p=pathlib.Path(tmp)
 for src,dst in [('engine.py','study.py'),('engine_tests.py','tests.py'),('engine_test_config.json','config.json')]:shutil.copyfile(GEN/src,p/dst)
 subprocess.run([sys.executable,str(p/'tests.py')],check=True)
subprocess.run([sys.executable,'-m','unittest','test_journal','test_real_schema','test_cloud_runner','test_seed_parts','test_alerts','-v'],cwd=ROOT,check=True)
names=['engine.py','study.py','config.json','engine_tests.py','engine_test_config.json','../test_alerts.py']
obj={'success':True,'scope':'SYNTHETIC_ONLY_NO_EDGE_CLAIM','hashes':{n:hashlib.sha256((GEN/n).read_bytes()).hexdigest() for n in names}}
(GEN/'TEST_RESULTS.json').write_text(json.dumps(obj,indent=2)+'\n')
