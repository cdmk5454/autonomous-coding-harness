"""Pi candidate Worker using the existing Harness verification pipeline.

Native completion only returns Worker execution evidence. Build, Test, Review,
QA and canonical publication remain owned by Manager and QueueSupervisor.
"""

from __future__ import annotations

import time
from pathlib import Path

from harness_11.core import Binding, Resource, Scope, BoundaryError, digest
from harness_11.pi_adapter import PiRuntimeAdapter
from pi_process_boundary import DockerPiProcessBoundary, PI_MODEL, PI_VERSION


class PiWorkerError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class PiWorker:
    def __init__(self, *, adapter_factory, model, prompt_transform=None,
                 ephemeral_session=True, timeout=600, **_):
        if model != PI_MODEL or ephemeral_session is not True:
            raise PiWorkerError('PI_CANDIDATE_CONTRACT_REQUIRED')
        self.adapter_factory = adapter_factory
        self.model = model
        self.prompt_transform = prompt_transform or (lambda text: text)
        self.timeout = min(float(timeout), 900)

    def execute(self, requirement, working_dir, task=None):
        from worker import WorkerResult
        from production_isolation import inventory
        from control_repository import ControlRepository
        if (task is None or task.worker != 'pi' or task.pi_model != PI_MODEL
                or not task.materialized_execution or not task.execution_id
                or not task.attempt_id or not task.candidate_id or not task.execution_epoch
                or not task.control_repository_path):
            raise PiWorkerError('PI_WORKER_DISPATCH_CONTEXT_MISSING')
        boundary = self.adapter_factory()
        if not isinstance(boundary,DockerPiProcessBoundary) or Path(working_dir).resolve() != boundary.candidate:
            raise PiWorkerError('PI_BOUNDARY_NOT_OWNED')
        execution = task.materialized_execution
        if execution.get('execution_id') != task.execution_id:
            raise PiWorkerError('PI_EXECUTION_BINDING_MISMATCH')
        resources = tuple(Resource('repo/'+path, 'WRITE') for path in task.target_resources)
        resources += tuple(Resource(**item) for item in execution['request'].get('shared_resources') or ())
        scope = Scope(str(boundary.candidate),str(boundary.canonical),
                      tuple(task.target_resources),resources)
        initial = inventory(boundary.candidate, include_git=True)
        binding = Binding(task.job_id,task.execution_id,task.attempt_id,task.candidate_id,
            digest(execution),digest(initial),digest({'initial_diff':[]}),task.execution_epoch)
        adapter = PiRuntimeAdapter(binding,scope,boundary=boundary,runtime_version=PI_VERSION,
                                   provider='opencode-go',model='deepseek-v4.1-flash',enabled=True)
        from worker import _runtime_contract_failure
        drift = _runtime_contract_failure(task, adapter.preflight(), 'WORKER')
        if drift:
            raise PiWorkerError(drift)
        repository = ControlRepository(task.control_repository_path)
        started = time.monotonic()
        process_id = ''
        failure = ''
        try:
            process = adapter.start()
            recorded = repository.record_runtime_process(task.attempt_id,runtime='pi',
                pid=process.pid,process_identity=process.key)
            process_id = recorded['process_id']
            task.runtime_process_id = process_id
            task.runtime_adapter = adapter.preflight().public()
            until = started + 20
            while adapter.state == 'STARTING' and time.monotonic() < until:
                adapter.poll_once()
                time.sleep(.02)
            if adapter.state != 'READY':
                raise PiWorkerError('PI_STARTUP_ACK_TIMEOUT')
            with repository.hold():
                row = repository._connection.execute(
                    'SELECT slot,state,process_identity FROM execution_slots WHERE job_id=?',
                    (task.job_id,)).fetchone()
                if row is None or row['state'] != 'RUNNING' or row['process_identity'] != process.key:
                    raise PiWorkerError('PI_SLOT_PROCESS_BINDING_MISSING')
                token = row['slot']
            adapter.bind_admission(token)
            prompt = self.prompt_transform(str(requirement))
            prompt += ('\nOnly edit the declared candidate write paths. Do not commit, publish, '
                       'or claim Harness acceptance. The Harness runs independent gates.')
            invocation = adapter.create_session(prompt,cwd=str(boundary.candidate),
                                                model='deepseek-v4.1-flash')
            adapter.dispatch(invocation)
            task.native_session_id = adapter.session_id
            task.command_id = adapter.command_id
            next_cancel_check = time.monotonic()
            while adapter.state not in {'SETTLED','FAILED'} and time.monotonic()-started < self.timeout:
                if time.monotonic() >= next_cancel_check:
                    from bounded_execution import cancel_requested
                    if cancel_requested(repository, task.job_id, process.key):
                        adapter.interrupt(adapter.session_id, adapter.command_id)
                        raise PiWorkerError('PI_CANCELLED')
                    next_cancel_check = time.monotonic() + .5
                adapter.poll_once()
                time.sleep(.05)
            if adapter.state != 'SETTLED' or adapter.failure_code:
                raise PiWorkerError(adapter.failure_code or 'PI_RUNTIME_TIMEOUT')
        except (BoundaryError, PiWorkerError, OSError, ValueError) as exc:
            failure = getattr(exc,'code','PI_RUNTIME_UNAVAILABLE')
            task.retry_domain = 'RUNTIME_RECOVERY'
        finally:
            if adapter.process is not None:
                try:
                    adapter.fence(timeout=15)
                except (BoundaryError, OSError):
                    failure = 'PI_WRITER_QUIESCENCE_UNKNOWN'
            else:
                boundary.stop_local_server()
            if process_id:
                if boundary.confirm_quiescence():
                    repository.finish_runtime_process(process_id,state='KILLED')
                else:
                    failure = 'PI_WRITER_QUIESCENCE_UNKNOWN'
        if failure:
            return WorkerResult(False,1,'',failure,['pi','rpc'],
                actual_executable='pinned-docker-pi',execution_failure_code=failure)
        return WorkerResult(True,0,adapter.result_text,'',['pi','rpc'],
            actual_executable='pinned-docker-pi',actual_invocation_id=adapter.command_id)
