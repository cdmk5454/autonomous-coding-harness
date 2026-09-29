"""Bounded Windows Job-Object transport for the new gate-only native roles."""
from datetime import datetime, timedelta
import os
from pathlib import Path
import tempfile
import time
import uuid


class _ScratchDir(tempfile.TemporaryDirectory):
    """Owned-tree members can hold stdio handles for a few ms after the Job
    closes; cleanup retries that bounded teardown window instead of failing."""
    def cleanup(self):
        for delay in (0, .05, .1, .25, .5, 1, 2):
            try:
                super().cleanup()
                return
            except PermissionError:
                if delay >= 2:
                    raise
                time.sleep(delay)


def run(*, cmd, stdin_data, cwd, cfg, requirement='', task_mode='ANALYSIS', env=None,
        runtime_event=None, execution_id='', execution_role='', attempt_id='',
        runtime_name='', display_events=False, log_prefix='', **unused):
    from runtime_safety import assert_secret_free_argv, trusted_executable, scrub_secrets
    if os.name != 'nt':
        raise RuntimeError('GATE_OWNED_PROCESS_PLATFORM_UNAVAILABLE')
    if execution_role not in {'TESTER','REVIEWER'} or not attempt_id or not callable(runtime_event):
        raise RuntimeError('GATE_OWNED_PROCESS_BINDING_REQUIRED')
    from hermes_process_boundary import OwnedProcess, identity
    from harness_temp import scratch_root
    argv=list(cmd);argv[0]=trusted_executable(argv[0],forbidden_root=cwd)
    assert_secret_free_argv(argv,env=env)
    cap=min(float(cfg.analysis_max),float(cfg.hard_limit))
    if not 0 < cap <= 7200:
        raise RuntimeError('GATE_OWNED_PROCESS_TIMEOUT_INVALID')
    run_id=execution_id or 'INV-GATE-'+uuid.uuid4().hex
    stdout=stderr='';reason=None;code=-1;owner=None;quiet=False
    started=datetime.now().astimezone().isoformat()
    with _ScratchDir(prefix='gate-role-',dir=scratch_root('runtime')) as temp:
        root=Path(temp)
        with (root/'stdin').open('w+b') as inp,(root/'stdout').open('w+b') as out,(root/'stderr').open('w+b') as err:
            inp.write((stdin_data or '').encode('utf-8'));inp.flush();inp.seek(0)
            owner=OwnedProcess(argv[0],argv,env,cwd,out,stderr=err,stdin=inp)
            birth=identity(owner.pid)
            if not birth.get('creation_time_100ns'):
                owner.terminate_and_wait();owner.release()
                raise RuntimeError('GATE_PROCESS_BIRTH_IDENTITY_REQUIRED')
            process_identity=str(owner.pid)+':'+str(birth['creation_time_100ns'])+':'+run_id
            def emit(kind, **values):
                current=datetime.now().astimezone()
                runtime_event(dict(event=kind,execution_id=run_id,execution_role=execution_role,
                    attempt_id=attempt_id,runtime=runtime_name,pid=owner.pid,started_at=started,
                    process_identity=process_identity,observed_at=current.isoformat(),
                    last_runtime_heartbeat_at=current.isoformat(),
                    lease_expires_at=(current+timedelta(seconds=cfg.runtime_lease)).isoformat(),
                    progress_timeout_seconds=cfg.inactivity,**values))
            start=time.monotonic();last_output=start;next_beat=start;size=0
            try:
                emit('STARTED',process_alive=True)
                while owner.poll() is None:
                    current=time.monotonic()
                    length=os.fstat(out.fileno()).st_size+os.fstat(err.fileno()).st_size
                    if length>2*1024*1024:
                        reason='OUTPUT_LIMIT';break
                    if length!=size:last_output=current;size=length
                    if current-start>=cap:
                        reason='TIMEOUT';break
                    if current-last_output>=float(cfg.inactivity):
                        reason='INACTIVITY';break
                    if current>=next_beat:
                        emit('HEARTBEAT',process_alive=True)
                        next_beat=current+max(1,float(cfg.runtime_heartbeat))
                    time.sleep(.05)
                if reason:
                    owner.terminate_and_wait(timeout=5)
                else:
                    code=owner.wait(timeout=1)
                    # Windows removes transient job members (console host)
                    # shortly after the owned process exits; a real child
                    # outlives this bounded drain window and stays fenced.
                    drain=time.monotonic()+2
                    while owner.members() and time.monotonic()<drain:
                        time.sleep(.01)
                    if owner.members():
                        reason='LINGERING_CHILD'
                        owner.terminate_and_wait(timeout=5)
                quiet=not owner.members() and owner.poll() is not None
                if not quiet:
                    reason=reason or 'QUIESCENCE_UNPROVEN'
                out.seek(0);err.seek(0)
                raw_out=out.read(1024*1024+1);raw_err=err.read(1024*1024+1)
                if len(raw_out)>1024*1024 or len(raw_err)>1024*1024:
                    reason=reason or 'OUTPUT_LIMIT'
                try:
                    stdout=raw_out[:1024*1024].decode('utf-8',errors='strict')
                    stderr=raw_err[:1024*1024].decode('utf-8',errors='strict')
                except UnicodeError:
                    reason=reason or 'MALFORMED_UTF8'
            except BaseException:
                reason=reason or 'OWNED_PROCESS_ERROR'
                try:
                    owner.terminate_and_wait(timeout=5)
                    quiet=not owner.members()
                except Exception:
                    quiet=False
                raise
            finally:
                try:
                    emit('ENDED',ended_at=datetime.now().astimezone().isoformat(),exit_code=code,
                        termination_reason=reason or ('EXITED' if code==0 else 'NONZERO_EXIT'),
                        process_alive=not quiet,quiescent=quiet,process_tree_owned=True)
                finally:
                    owner.release()
    return (code if reason is None else -1,scrub_secrets(stdout),scrub_secrets(stderr),reason)
