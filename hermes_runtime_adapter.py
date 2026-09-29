"""Hermes public Runs transport. Native completion is evidence, never Job success.

Construction is execution-scoped. A Harness-owned boundary must independently
validate admission; callers cannot provide a boolean bypass. No private Hermes
modules or state stores are used.
"""
from __future__ import annotations
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
import json
from pathlib import Path
from urllib.request import Request, build_opener, ProxyHandler, HTTPRedirectHandler
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from runtime_adapter import (RuntimeCapabilityError, RuntimeCapabilities,
    RuntimeDescriptor, NormalizedRuntimeEvent, SessionQuery, TurnInvocation,
    payload_sha256, CancellationState, CancellationResult)

class HermesError(RuntimeCapabilityError):
    pass

@dataclass(frozen=True)
class HermesBinding:
    job_id: str
    execution_id: str
    attempt_id: str
    binding_id: str
    command_id: str
    candidate_id: str
    context_pack_digest: str
    execution_epoch: str
    server_incarnation: str
    provider: str
    model: str
    role: str
    host_boundary_identity: str
    def __post_init__(self):
        if any(not isinstance(v, str) or not v for v in asdict(self).values()):
            raise HermesError('EXECUTION_BINDING_INCOMPLETE')
        if self.role not in {'WORKER', 'REVIEWER', 'TESTER'}:
            raise HermesError('EXECUTION_ROLE_INVALID')
    @property
    def fingerprint(self):
        return payload_sha256(asdict(self))
    @property
    def idempotency_key(self):
        return 'harness-' + payload_sha256([self.job_id, self.execution_id,
                                          self.attempt_id, self.command_id])

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise HermesError('HERMES_REDIRECT_DENIED')

class HermesRuntimeAdapter:
    runtime_name = 'hermes'
    def __init__(self, *, binding: HermesBinding, repository, boundary,
                 base_url: str, api_key: str, model_options=None, timeout=20):
        url = urlparse(base_url)
        if url.scheme != 'http' or url.hostname != '127.0.0.1' or url.username or url.password or url.path not in {'', '/'}:
            raise HermesError('HERMES_ENDPOINT_DENIED')
        if not api_key:
            raise HermesError('HERMES_AUTH_REQUIRED')
        self.binding, self.repository, self.boundary = binding, repository, boundary
        self.base_url, self._key, self.timeout = base_url.rstrip('/'), api_key, timeout
        self.options = json.loads(json.dumps(model_options or {}))
        self.run_id = self.session_id = ''
        self.native_binding_id = ''
        self._terminal = False
        self._pending = None
        self._opener = build_opener(ProxyHandler({}), NoRedirect())

    def preflight(self):
        self.boundary.validate_binding(self.binding)
        return self.catalog_descriptor()

    @staticmethod
    def catalog_descriptor():
        # Protocol catalogue only; never grants runtime admission.
        return RuntimeDescriptor(runtime='hermes', version='0.21.3 (2026.9.14)',
            capabilities=RuntimeCapabilities(True, True, True, True, True, True, True, True),
            protocol_revision='hermes-public-runs/2026.9.14',
            permission_tool_profile='harness-docker-admission/1',
            sandbox_semantics='INSPECT_VERIFIED_ROLE_MOUNTS',
            session_process_lifetime='SESSION_PERSISTENT_CONTAINER_EXTERNALLY_OWNED',
            capability_contract={k: v for k, v in {
                'session.create':'SUPPORTED', 'session.resume':'PARTIAL',
                'session.read_query':'SUPPORTED', 'turn.submit':'SUPPORTED',
                'turn.steer':'PARTIAL', 'turn.interrupt':'PARTIAL',
                'events.subscribe':'PARTIAL', 'events.reconnect':'PARTIAL',
                'events.terminal_query':'SUPPORTED', 'interaction.pending_approval':'PARTIAL',
                'interaction.permission_response':'PARTIAL', 'output.structured':'PARTIAL',
                'runtime.protocol_revision':'SUPPORTED'}.items()},
            validation_disposition='NOT_RUN', detail='PER_BINDING_ADMISSION_REQUIRED')

    def _current(self, *, terminal_ok=False):
        b = self.binding
        self.boundary.validate_binding(b)
        self.boundary.assert_server(b)
        a = self.repository.attempt_identity(b.attempt_id)
        latest = self.repository.latest_attempt(b.execution_id)
        m = self.repository.active_execution(b.job_id)
        if not a or a['execution_id'] != b.execution_id or a['role'] != b.role or not m or m['job_id'] != b.job_id or m['execution_id'] != b.execution_id:
            raise HermesError('FOREIGN_ATTEMPT')
        from hermes_identity import ProviderModelIdentity
        try:
            expected = ProviderModelIdentity(b.provider, b.model)
            actual = ProviderModelIdentity.parse(latest.get('model', '') if latest else '', provider=b.provider)
        except ValueError:
            raise HermesError('INVOCATION_MODEL_BINDING_MISMATCH') from None
        if actual != expected:
            raise HermesError('INVOCATION_MODEL_BINDING_MISMATCH')
        if a['status'] != 'RUNNING' or not latest or latest['attempt_id'] != b.attempt_id:
            raise HermesError('LATE')
        command = self.repository.command(b.command_id)
        if command and command.get('aborted'):
            raise HermesError('LATE')
        if self._terminal and not terminal_ok:
            raise HermesError('TERMINAL_INTERACTION_REJECTED')

    def request(self, method, path, body=None, *, idempotency_key=''):
        headers = {'Authorization':'Bearer '+self._key, 'Content-Type':'application/json'}
        if idempotency_key:
            headers['Idempotency-Key'] = idempotency_key
        request = Request(self.base_url+path, data=None if body is None else json.dumps(body).encode(),
                          headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                return json.loads(response.read())
        except HTTPError as exc:
            raise HermesError('HERMES_HTTP_'+str(exc.code)) from None
        except (URLError, OSError, ValueError):
            raise HermesError('HERMES_TRANSPORT_UNKNOWN') from None

    def create_session(self, prompt, *, cwd, model='', reasoning_effort='medium', command_id=''):
        return self.submit_turn('', prompt, cwd=cwd, model=model,
                                reasoning_effort=reasoning_effort, command_id=command_id)

    def submit_turn(self, session_id, prompt, *, cwd, model='', reasoning_effort='medium', command_id=''):
        self._current()
        b = self.binding
        if command_id != b.command_id or model != b.model:
            raise HermesError('INVOCATION_BINDING_MISMATCH')
        if Path(cwd).resolve() != self.boundary.candidate.resolve():
            raise HermesError('CANDIDATE_MISMATCH')
        if session_id:
            self.boundary.authorize_resume(b, session_id, prompt)
        options = dict(self.options)
        options['reasoning_effort'] = reasoning_effort
        body = {'input':prompt, 'provider':b.provider, 'model':b.model, 'model_options':options}
        if session_id:
            body['session_id'] = session_id
        elif getattr(self, 'initial_session_id', ''):
            body['session_id'] = self.initial_session_id
        return TurnInvocation(command_id=b.command_id, transport='HTTP', method='POST',
                              url=self.base_url+'/v1/runs', body=body, session_id=session_id,
                              resume=bool(session_id))

    resume_session = submit_turn

    def dispatch(self, invocation):
        self._current()
        self.boundary.admit(self.binding)
        b = self.binding
        if invocation.command_id != b.command_id or invocation.url != self.base_url+'/v1/runs' or invocation.method != 'POST':
            raise HermesError('INVOCATION_BINDING_MISMATCH')
        if invocation.body.get('provider') != b.provider or invocation.body.get('model') != b.model:
            raise HermesError('INVOCATION_BINDING_MISMATCH')
        existing = self.repository.command(b.command_id)
        digest = payload_sha256({'binding':asdict(b), 'body':dict(invocation.body)})
        if existing:
            if existing['payload_sha256'] != digest:
                raise HermesError('COMMAND_IDEMPOTENCY_CONFLICT')
            if existing['delivery_status'] != 'PREPARED':
                # Never resend after a timeout, process loss, or provider TTL expiry.
                if existing.get('native_turn_id'):
                    return {'run_id':existing['native_turn_id'], 'replayed':True}
                raise HermesError('RECONCILIATION_REQUIRED')
        else:
            self.repository.prepare_command(b.execution_id, b.attempt_id,
                idempotency_key=b.idempotency_key, payload_sha256=digest, command_id=b.command_id)
        self.repository.record_runtime_recovery(b.execution_id, b.attempt_id,
            failure_code='HERMES_BINDING_PREPARED', evidence={'binding':asdict(b),
                'payload_fingerprint':digest, 'boundary':self.boundary.receipt()})
        self.repository.set_command_delivery(b.command_id, 'SENT')
        try:
            result = self.request('POST', '/v1/runs', invocation.body, idempotency_key=b.idempotency_key)
            run = result.get('run_id')
            if not isinstance(run, str) or not run:
                raise HermesError('NATIVE_RUN_ID_MISSING')
            self.run_id = run
            self.repository.set_command_delivery(b.command_id, 'ACKNOWLEDGED', native_turn_id=run)
            self._bind_status(self.request('GET', '/v1/runs/'+quote(run, safe='')))
            return result
        except Exception:
            if self.repository.command(b.command_id)['delivery_status'] == 'SENT':
                self.repository.set_command_delivery(b.command_id, 'UNKNOWN')
            self.gap('DISPATCH_ACK_UNKNOWN')
            raise

    def _bind_status(self, status):
        b = self.binding
        if status.get('run_id') != self.run_id:
            raise HermesError('FOREIGN_RUN')
        sid = status.get('session_id')
        if not sid:
            raise HermesError('NATIVE_SESSION_ID_MISSING')
        if self.session_id and self.session_id != sid:
            raise HermesError('FOREIGN_SESSION')
        if status.get('model') and status['model'] != b.model:
            raise HermesError('MODEL_BINDING_MISMATCH')
        if not self.session_id:
            self.session_id = sid
            native = self.repository.bind_native_session(b.execution_id, runtime='hermes',
                native_session_id=sid, adapter_revision='runtime-adapter/2',
                runtime_version='0.21.3', role=b.role, candidate_hash=b.candidate_id,
                allow_fresh=True)
            self.native_binding_id = native['binding_id']
            self.repository.attach_command_binding(b.command_id, self.native_binding_id)
            self.repository.bind_native_turn(self.native_binding_id, b.command_id, self.run_id)
            self.repository.record_runtime_recovery(b.execution_id, b.attempt_id,
                failure_code='HERMES_NATIVE_BOUND', binding_id=self.native_binding_id,
                evidence={'binding':asdict(b), 'run_id':self.run_id, 'session_id':sid,
                          'boundary':self.boundary.receipt()})
        return status

    def observe_status(self):
        self._current(terminal_ok=True)
        status = self._bind_status(self.request('GET', '/v1/runs/'+quote(self.run_id, safe='')))
        self._record(status, 'STATUS_RECONCILIATION')
        self._terminal = status.get('status') in {'completed','failed','cancelled','interrupted'}
        self._pending = status.get('approval') or status.get('pending_approval')
        return status

    def _record(self, payload, origin):
        b = self.binding
        # No fabricated provider event id, cursor, or native sequence. Existing
        # repository intentionally retains these as AMBIGUOUS observations.
        return self.repository.record_inbound_event(b.execution_id, b.attempt_id,
            binding_id=self.native_binding_id, runtime='hermes',
            native_session_id=self.session_id, native_turn_id=self.run_id,
            event_type=str(payload.get('event') or payload.get('type') or payload.get('status') or 'observation'),
            payload_sha256=payload_sha256(payload), detail={'origin':origin,
                'binding':asdict(b), 'observed_at':datetime.now(timezone.utc).isoformat()})

    def normalize_event(self, event):
        self._current(terminal_ok=True)
        if event.get('run_id') != self.run_id or event.get('session_id', self.session_id) != self.session_id:
            raise HermesError('FOREIGN_RUN_OR_SESSION')
        receipt = self._record(event, 'TRANSIENT_SSE')
        if receipt['duplicate']:
            raise HermesError('DUPLICATE_RESULT')
        kind = str(event.get('event') or event.get('type') or 'observation')
        return (NormalizedRuntimeEvent(kind=kind, runtime='hermes',
            session_id=self.session_id, turn_id=self.run_id, success=None,
            terminal=False, raw_type=kind, result_text='',
            execution_id=self.binding.execution_id, attempt_id=self.binding.attempt_id,
            binding_id=self.binding.binding_id, command_id=self.binding.command_id,
            execution_epoch=self.binding.execution_epoch, server_incarnation=self.binding.server_incarnation,
            observed_at=receipt['received_at'], payload_fingerprint=payload_sha256(event),
            observation_origin='TRANSIENT_SSE', local_receipt_id=receipt['event_id']),)

    def gap(self, reason='SSE_GAP'):
        b = self.binding
        self.repository.record_runtime_recovery(b.execution_id, b.attempt_id,
            binding_id=self.native_binding_id, failure_code='RECONCILIATION_REQUIRED',
            evidence={'binding':asdict(b), 'reason':reason,
                      'native_replay_available':False, 'candidate_evidence_required':True})

    def subscribe_events(self, session_id, *, cursor=''):
        self._session(session_id)
        if cursor:
            raise HermesError('NATIVE_REPLAY_UNSUPPORTED')
        request = Request(self.base_url+'/v1/runs/'+quote(self.run_id,safe='')+'/events',
            headers={'Authorization':'Bearer '+self._key, 'Accept':'text/event-stream'})
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                for raw in response:
                    line = raw.decode('utf8').strip()
                    if line.startswith('data:'):
                        yield from self.normalize_event(json.loads(line[5:]))
        finally:
            # Even an apparently clean EOF has no native durable replay proof.
            self.gap()

    def replay_events(self, session_id, *, cursor):
        raise HermesError('NATIVE_REPLAY_UNSUPPORTED')

    def fork_session(self, *args, **kwargs):
        raise HermesError('SESSION_FORK_UNSUPPORTED')

    def _session(self, session_id, turn_id=''):
        self._current()
        if session_id != self.session_id or (turn_id and turn_id != self.run_id):
            raise HermesError('FOREIGN_RUN_OR_SESSION')

    def query_session(self, session_id, *, cwd):
        if session_id != self.session_id:
            raise HermesError('FOREIGN_SESSION')
        s = self.observe_status()
        state = 'TERMINAL' if self._terminal else 'WAITING_INPUT' if s.get('status') == 'waiting_for_approval' else 'ACTIVE'
        return SessionQuery(session_id, state, current_turn_id=self.run_id,
            terminal=self._terminal, detail='RUNTIME_OBSERVATION_NOT_JOB_SUCCESS')

    def query_terminal(self, session_id, turn_id='', *, cwd):
        if turn_id and turn_id != self.run_id:
            raise HermesError('FOREIGN_RUN')
        return self.query_session(session_id, cwd=cwd)
    reconcile = query_session
    pending_approval_or_question = query_session

    def interrupt(self, session_id, turn_id=''):
        self._session(session_id, turn_id)
        try:
            self.request('POST', '/v1/runs/'+quote(self.run_id,safe='')+'/stop', {})
            state = CancellationState.ACKNOWLEDGED
        except HermesError:
            state = CancellationState.UNKNOWN
        return CancellationResult(state, self.binding.binding_id, self.run_id)

    def steer_turn(self, session_id, turn_id, prompt):
        self._session(session_id, turn_id)
        raise HermesError('STEER_AUTHORIZATION_REQUIRED')

    def respond_interaction(self, session_id, interaction_id, response):
        self._session(session_id)
        self.observe_status()
        self._session(session_id)
        pending = self._pending or {}
        if pending.get('run_id',self.run_id)!=self.run_id or pending.get('session_id',self.session_id)!=self.session_id:
            raise HermesError('FOREIGN_APPROVAL')
        if pending.get('request_id') != interaction_id or response.get('choice') not in {'once','deny'}:
            raise HermesError('INVALID_APPROVAL_REQUEST')
        self.boundary.authorize_interaction(self.binding, interaction_id, pending, response)
        self.request('POST', '/v1/runs/'+quote(self.run_id,safe='')+'/approval',
                     {'request_id':interaction_id,'choice':response['choice']})
        return True

    @staticmethod
    def structured_outcome(text):
        try:
            value = json.loads(text)
        except (ValueError, TypeError):
            raise HermesError('STRUCTURED_OUTCOME_INVALID') from None
        if not isinstance(value,dict) or value.get('outcome') not in {'EVIDENCE','NEEDS_INPUT','REVIEW_PASS','REVIEW_FAIL','TEST_RESULT'}:
            raise HermesError('STRUCTURED_OUTCOME_INVALID')
        if value['outcome'] == 'NEEDS_INPUT' and (not isinstance(value.get('question'),str) or not value['question'].strip()):
            raise HermesError('QUESTION_REQUIRED')
        if value['outcome'] == 'TEST_RESULT':
            if (value.get('status') not in {'PASS','FAIL','ERROR'}
                    or not isinstance(value.get('summary'),str)
                    or not isinstance(value.get('evidence'),list)
                    or any(not isinstance(x,str) or not x.strip() for x in value['evidence'])
                    or (value['status'] in {'PASS','FAIL'} and not value['evidence'])):
                raise HermesError('TEST_OUTCOME_INVALID')
        return value
