import unittest
import bootstrap
from test_alerts import bar,M
class WarmupAvailabilityTests(unittest.TestCase):
 def test_newer_history_prefix_is_recorded_without_shortening_calibration(self):
  rows=[bar(i*M) for i in range(bootstrap.s.engine.WARM)]
  out=bootstrap.warmup_coverage('TESTUSDT',rows,[{'status':200}],-100*M,len(rows)*M)
  self.assertEqual(out['unavailable_prefix_minutes'],100);self.assertTrue(out['control_warmup_complete'])
 def test_internal_gap_is_rejected(self):
  rows=[bar(i*M) for i in range(bootstrap.s.engine.WARM+2)];del rows[100]
  with self.assertRaises(ValueError):bootstrap.warmup_coverage('TESTUSDT',rows,[{'status':200}],0,(bootstrap.s.engine.WARM+2)*M)
 def test_request_failure_is_not_treated_as_unavailable_listing_history(self):
  rows=[bar(i*M) for i in range(bootstrap.s.engine.WARM)]
  with self.assertRaises(ValueError):bootstrap.warmup_coverage('TESTUSDT',rows,[{'error':'timeout'}],0,len(rows)*M)
if __name__=='__main__':unittest.main(verbosity=2)
