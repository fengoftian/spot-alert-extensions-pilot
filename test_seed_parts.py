import hashlib,json,pathlib,tempfile,unittest
import journal
class SeedPartTests(unittest.TestCase):
 def test_parts_reassemble_and_tampering_is_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=pathlib.Path(tmp);(p/'seed').mkdir();parts=[];body=b'first-partsecond-part'
   for i,b in enumerate([b'first-part',b'second-part']):
    name=f'seed/{i:04d}.part';(p/name).write_bytes(b);parts.append({'path':name,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()})
   (p/'SEED.json').write_text(json.dumps({'seed_parts':parts,'seed_sha256':hashlib.sha256(body).hexdigest()}))
   with journal.materialized_seed(p) as seed:self.assertEqual(seed.read_bytes(),body)
   (p/'seed/0000.part').write_bytes(b'bad')
   with self.assertRaises(ValueError):
    with journal.materialized_seed(p):pass
 def test_parent_path_cannot_be_used_for_seed_parts(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=pathlib.Path(tmp);(p/'SEED.json').write_text(json.dumps({'seed_parts':[{'path':'../outside','bytes':1,'sha256':'x'}],'seed_sha256':'x'}))
   with self.assertRaises(ValueError):
    with journal.materialized_seed(p):pass
if __name__=='__main__':unittest.main(verbosity=2)
