import contextlib,os,subprocess,unittest
from unittest.mock import Mock,patch
import cloud_runner as runner

class CloudLifecycleTests(unittest.TestCase):
 def engine(self,now=100):
  m=Mock();m.clock_ms.return_value=now
  m.getkv.side_effect=lambda c,k,default=None: {'stage':'COLLECTING'}.get(k,default)
  return m
 def test_integrity_halt_fails_instead_of_looking_healthy(self):
  m=self.engine();m.getkv.side_effect=lambda c,k,default=None: {'stage':'INTEGRITY_HALT','integrity_halt':True}.get(k,default)
  with self.assertRaisesRegex(RuntimeError,'halted'):runner.check_engine(None,m)
 def test_disk_cap_fails_instead_of_looking_healthy(self):
  m=self.engine();m.getkv.side_effect=lambda c,k,default=None: {'stage':'DISK_CAP_HALT'}.get(k,default)
  with self.assertRaisesRegex(RuntimeError,'halted'):runner.check_engine(None,m)
 def test_healthy_lease_requests_same_workflow_successor(self):
  with patch.dict(os.environ,{'GITHUB_ACTIONS':'true','GITHUB_REPOSITORY':'fengoftian/spot-alert-extensions-pilot'}),patch.object(runner.subprocess,'run') as run:
   runner.handoff_or_stop(self.engine(),{'tail_end':200})
   args=run.call_args.args[0];self.assertEqual(args[:4],['gh','workflow','run','collector.yml']);self.assertIn('mode=collect',args)
 def test_tail_disables_workflow_and_never_requests_successor(self):
  with patch.dict(os.environ,{'GITHUB_ACTIONS':'true','GITHUB_REPOSITORY':'fengoftian/spot-alert-extensions-pilot'}),patch.object(runner.subprocess,'run') as run:
   runner.handoff_or_stop(self.engine(200),{'tail_end':200})
   self.assertIn('/disable',run.call_args.args[0][2]);self.assertNotIn('workflow',run.call_args.args[0])
 def test_local_tests_do_not_dispatch_or_disable_workflows(self):
  with patch.dict(os.environ,{'GITHUB_ACTIONS':'false'}),patch.object(runner.subprocess,'run') as run:
   runner.handoff_or_stop(self.engine(),{'tail_end':200});run.assert_not_called()
 def test_unexpected_repository_is_rejected(self):
  with patch.dict(os.environ,{'GITHUB_ACTIONS':'true','GITHUB_REPOSITORY':'elsewhere/repo'}),patch.object(runner.subprocess,'run') as run:
   with self.assertRaises(ValueError):runner.handoff_or_stop(self.engine(),{'tail_end':200})
   run.assert_not_called()
 def test_tick_failure_persists_state_without_dispatching_successor(self):
  m=self.engine();m.load_freeze.return_value={'tail_end':200000};c=Mock()
  with patch.object(runner,'load_study',return_value=m),patch.object(runner.journal,'materialized_seed',return_value=contextlib.nullcontext('test-seed')),patch.object(runner.journal,'restore',return_value=(4,'parent')),patch.object(runner.sqlite3,'connect',return_value=c),patch.object(runner.time,'monotonic',return_value=0),patch.object(runner.subprocess,'run',side_effect=subprocess.CalledProcessError(1,['tick'])),patch.object(runner,'persist',return_value=(5,'saved')) as save,patch.object(runner,'handoff_or_stop') as handoff:
   with self.assertRaises(subprocess.CalledProcessError):runner.collect('.',60,300)
   save.assert_called_once();c.close.assert_called_once();handoff.assert_not_called()
 def test_terminal_tick_is_used_to_publish_completion_status(self):
  m=self.engine(200);m.load_freeze.return_value={'tail_end':200};c=Mock()
  with patch.object(runner,'load_study',return_value=m),patch.object(runner.journal,'materialized_seed',return_value=contextlib.nullcontext('test-seed')),patch.object(runner.journal,'restore',return_value=(4,'parent')),patch.object(runner.sqlite3,'connect',return_value=c),patch.object(runner.time,'monotonic',return_value=0),patch.object(runner.subprocess,'run') as run,patch.object(runner,'persist',return_value=(5,'saved')) as save,patch.object(runner,'handoff_or_stop') as handoff:
   runner.collect('.',60,300)
   self.assertEqual(run.call_args.args[0][-1],'tick');m.report.assert_not_called();save.assert_called_once();handoff.assert_called_once()

if __name__=='__main__':unittest.main(verbosity=2)
