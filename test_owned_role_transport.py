"""Actual disposable native process/tree checks; never invokes a provider."""
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from owned_role_transport import run
from worker import TimeoutConfig


@unittest.skipUnless(os.name=='nt','Windows native Job Object boundary')
class OwnedRoleTests(unittest.TestCase):
    def invoke(self,code,*,stdin='',cap=5):
        events=[]
        with tempfile.TemporaryDirectory() as root:
            result=run(cmd=[sys.executable,'-B','-c',code],stdin_data=stdin,cwd=root,
                cfg=TimeoutConfig(analysis_max=cap,hard_limit=cap,inactivity=cap),
                runtime_event=events.append,execution_role='REVIEWER',attempt_id='ROLE-1',
                runtime_name='fixture',execution_id='INV-FIXTURE')
        self.assertTrue(events[0]['process_identity'])
        self.assertTrue(events[-1]['process_tree_owned'])
        self.assertTrue(events[-1]['quiescent'])
        self.assertFalse(events[-1]['process_alive'])
        return result,events

    def test_stdin_utf8_and_birth_identity(self):
        result,events=self.invoke("import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())",stdin='한글')
        self.assertEqual((0,'한글','',None),result)
        self.assertEqual(events[0]['process_identity'],events[-1]['process_identity'])

    def test_lingering_child_is_fenced_and_cannot_pass(self):
        code="import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); print('PASS')"
        result,_=self.invoke(code)
        self.assertEqual(-1,result[0])
        self.assertEqual('LINGERING_CHILD',result[3])

    def test_timeout_owned_tree_is_quiet(self):
        result,_=self.invoke('import time; time.sleep(20)',cap=.3)
        self.assertEqual(-1,result[0]);self.assertEqual('TIMEOUT',result[3])

    def test_oversized_output_cannot_pass(self):
        result,_=self.invoke("import sys; sys.stdout.buffer.write(b'x'*(3*1024*1024))")
        self.assertEqual(-1,result[0]);self.assertEqual('OUTPUT_LIMIT',result[3])

    def test_malformed_utf8_cannot_pass(self):
        result,_=self.invoke("import sys; sys.stdout.buffer.write(b'\\xff')")
        self.assertEqual(-1,result[0]);self.assertEqual('MALFORMED_UTF8',result[3])

    def test_cleanup_failure_reports_unknown_before_owned_handle_closes(self):
        events=[]
        code="import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)"
        with tempfile.TemporaryDirectory() as root,\
             patch('hermes_process_boundary.OwnedProcess.terminate_and_wait',side_effect=RuntimeError('fixture cleanup failure')):
            with self.assertRaisesRegex(RuntimeError,'cleanup failure'):
                run(cmd=[sys.executable,'-B','-c',code],stdin_data='',cwd=root,
                    cfg=TimeoutConfig(analysis_max=5,hard_limit=5),runtime_event=events.append,
                    execution_role='REVIEWER',attempt_id='ROLE-FAILED-CLEANUP',runtime_name='fixture')
        self.assertFalse(events[-1]['quiescent'])
        self.assertTrue(events[-1]['process_alive'])


if __name__=='__main__':unittest.main()
