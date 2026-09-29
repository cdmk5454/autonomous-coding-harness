"""Pi candidate route registration and owned disposable service selection."""

import unittest
from unittest.mock import Mock

from bounded_execution import migrate_capacity_disposable
from harness_11.capacity import CapacityPolicy
from job_contract import JobContractError, JobRequest
from manager import Manager
from pi_process_boundary import PI_MODEL, PI_VERSION
from runtime_contract import descriptor_for
from runtime_selection import RuntimeSelectionError, select_runtime
from task_state import TaskState


def request(**changes):
    value = {'requirement':'Change app/a.py', 'worker':'pi', 'model':PI_MODEL,
             'pi_candidate':True, 'target_modules':['app'],
             'target_resources':['app/a.py']}
    value.update(changes)
    return JobRequest.from_mapping(value, allowed_modules=['app'])


class PiFactoryTest(unittest.TestCase):
    def test_default_route_and_candidate_gate(self):
        default = JobRequest.from_mapping({'requirement':'Change app/a.py',
            'target_modules':['app'], 'target_resources':['app/a.py']}, allowed_modules=['app'])
        self.assertEqual('codex',default.worker)
        with self.assertRaisesRegex(JobContractError,'PI_CANDIDATE_GATE_REQUIRED'):
            request(pi_candidate=False)
        with self.assertRaisesRegex(RuntimeSelectionError,'PI_CANDIDATE_GATE_REQUIRED'):
            select_runtime('pi',PI_MODEL)
        self.assertEqual('pi',select_runtime('pi',PI_MODEL,candidate_enabled=True).selected_runtime)
        self.assertEqual(PI_VERSION,descriptor_for('pi').version)

    def test_manager_selects_only_bound_pi_factory(self):
        task = TaskState(task_id='DISPOSABLE-PI-TASK',requirement='Change app/a.py',
                         worker='pi',pi_model=PI_MODEL)
        task._isolated_pi = True
        manager = Manager.__new__(Manager)
        manager.choose_worker = lambda _: 'pi'
        chosen = Mock()
        manager.isolated_pi_worker_factory = Mock(return_value=chosen)
        worker, runtime = manager._get_worker(task)
        self.assertIs(worker,chosen)
        self.assertEqual('pi',runtime)
        manager.isolated_pi_worker_factory.assert_called_once_with(model=PI_MODEL)
        task._isolated_pi = False
        with self.assertRaisesRegex(RuntimeError,'PI_EXECUTION_OWNER_REQUIRED'):
            manager._get_worker(task)

    def test_actual_isolation_factory_selects_pi_candidate(self):
        from test_production_isolation import CompositionTests
        fixture = CompositionTests('test_wrong_route_rejects_without_allocation')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.schema6()
        fixture.control.store.pause('disposable Pi migration')
        migrate_capacity_disposable(fixture.repo.path,allocation_root=fixture.root,
            backup_path=fixture.root/'schema6-pi.bak',
            policy=CapacityPolicy(workers=3,wip=3),quiescence_probe=lambda _:True)
        fixture.control.store.resume_queue()
        job = {'job_id':'DISPOSABLE-PI-JOB','request':request().to_dict()}
        # Allocation is host-owned and still copies/validates the same canonical bytes.
        child = fixture.control.isolation(job,fixture.control.store.repository)
        self.assertEqual('pi',child.isolated_execution.route)
        self.assertEqual('pi',child.isolated_execution.descriptor().runtime)
        self.assertNotEqual(child.working_dir,fixture.workspace)


if __name__ == '__main__':
    unittest.main()
