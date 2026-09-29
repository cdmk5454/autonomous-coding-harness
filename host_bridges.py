"""Optional 1.1 observations at existing Harness decision points.

Every result is evidence only. A failed or absent optional provider leaves the
frozen Harness decision and its original context untouched.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import queue
import re
import subprocess
import tempfile
import threading
import time
from dataclasses import asdict
from pathlib import Path

from bounded_transport import BoundedTransportError, run_bounded
from harness_11.core import Binding, Mode, PolicySnapshot, Risk, digest, strict_json
from harness_11.jev import Choice, JevAdapter, JevConfig, Question, UseCase
from harness_11.review_support import security_activation
from harness_11.context import ToolIdentity
from harness_11.context import (ContextBoundary, SERENA_TOOLS, CONTEXT7_TOOLS,
                                CHROME_TOOLS, read_source_bytes)
from runtime_safety import scrub_secrets


BRIDGE_VERSION = 'h11-host-bridge/1'
_COMMANDS = frozenset(('advisor', 'repomix', 'alibaba_review', 'security_audit'))


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class JsonCommand:
    """Pinned executable with bounded byte transport and no Product cwd."""
    def __init__(self, name: str, spec: dict):
        if (name not in _COMMANDS or type(spec) is not dict
                or not {'path', 'sha256', 'version'} <= set(spec)
                or set(spec) - {'path', 'sha256', 'version', 'script', 'script_sha256'}):
            raise ValueError('HOST_COMMAND_CONTRACT_INVALID')
        path = Path(spec['path'])
        if (not path.is_absolute() or not path.is_file() or not re.fullmatch('[0-9a-f]{64}', spec['sha256'])
                or _hash_file(path) != spec['sha256'] or not isinstance(spec['version'], str)
                or not spec['version'] or len(spec['version']) > 100):
            raise ValueError('HOST_COMMAND_IDENTITY_INVALID')
        self.script = None
        if 'script' in spec:
            script = Path(spec['script'])
            if (not script.is_absolute() or not script.is_file()
                    or _hash_file(script) != spec.get('script_sha256')):
                raise ValueError('HOST_SCRIPT_IDENTITY_INVALID')
            self.script = script
        self.name, self.path, self.spec = name, path, spec

    def call(self, value: dict, *, timeout=5.0, max_bytes=65536) -> dict:
        payload = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
        if len(payload) > 65536:
            raise ValueError('HOST_INPUT_TOO_LARGE')
        with tempfile.TemporaryDirectory(prefix='h11-host-') as folder:
            result = run_bounded([str(self.path)] + ([str(self.script)] if self.script else []), input_bytes=payload, cwd=folder,
                                 env={'PATH': os.environ.get('PATH', ''), 'SystemRoot': os.environ.get('SystemRoot', '')},
                                 timeout=timeout, output_limit=max_bytes)
        if result.returncode:
            raise ValueError('HOST_COMMAND_FAILED')
        parsed = strict_json(result.stdout, max_bytes)
        if type(parsed) is not dict:
            raise ValueError('HOST_OUTPUT_SCHEMA_INVALID')
        return parsed


class MCPReadOnly:
    """One-shot stdio MCP client; only a pinned symbol-navigation tool is called."""
    def __init__(self, spec: dict, candidate_root: Path, *, kind='serena'):
        if (type(spec) is not dict or set(spec) != {'path','sha256','version','script',
                                                   'script_sha256','read_only'} or spec['read_only'] is not True):
            raise ValueError('MCP_READONLY_CONFIG_REQUIRED')
        self.command = JsonCommand.__new__(JsonCommand)
        self.command.path = Path(spec['path'])
        self.command.script = Path(spec['script'])
        if (not self.command.path.is_absolute() or not self.command.script.is_absolute()
                or not self.command.path.is_file() or not self.command.script.is_file()
                or _hash_file(self.command.path) != spec['sha256']
                or _hash_file(self.command.script) != spec['script_sha256']):
            raise ValueError('MCP_IDENTITY_INVALID')
        self.spec = spec
        self.candidate_root = Path(candidate_root).resolve()
        if kind not in {'serena','context7','chrome_devtools'}:
            raise ValueError('MCP_KIND_INVALID')
        self.kind = kind
        self.allowed = {'serena':SERENA_TOOLS,'context7':CONTEXT7_TOOLS,
                        'chrome_devtools':CHROME_TOOLS}[kind]

    def attest(self, identity, binding, scope):
        return (identity.name == self.kind and self.candidate_root == Path(scope.candidate_root)
                and self.candidate_root != Path(scope.canonical_root))

    def call(self, tool: str, arguments: dict, *, timeout: float, max_bytes: int) -> dict:
        if tool not in self.allowed:
            raise ValueError('MCP_CAPABILITY_DENIED')
        process = subprocess.Popen([str(self.command.path), str(self.command.script)],
            cwd=str(self.candidate_root), env={'PATH':os.environ.get('PATH',''),
                'SystemRoot':os.environ.get('SystemRoot','')}, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        output = queue.Queue(maxsize=64)
        stderr_count = [0]
        def reader():
            try:
                for line in process.stdout:
                    if len(line) > max_bytes or output.qsize() >= 60:
                        output.put_nowait(None); return
                    output.put_nowait(line)
            except (OSError, queue.Full):
                pass
        def drain():
            try:
                for chunk in iter(lambda: process.stderr.read(4096), b''):
                    stderr_count[0] += len(chunk)
                    if stderr_count[0] > max_bytes:
                        process.kill(); return
            except OSError:
                pass
        threads = [threading.Thread(target=reader,daemon=True), threading.Thread(target=drain,daemon=True)]
        for thread in threads: thread.start()
        deadline = time.monotonic() + timeout
        def send(value):
            raw = json.dumps(value, separators=(',',':')).encode('utf-8') + b'\n'
            process.stdin.write(raw); process.stdin.flush()
        def receive(request_id):
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0: raise TimeoutError('MCP_TIMEOUT')
                try: raw = output.get(timeout=remaining)
                except queue.Empty: raise TimeoutError('MCP_TIMEOUT') from None
                if raw is None: raise ValueError('MCP_OUTPUT_TOO_LARGE')
                value = strict_json(raw,max_bytes)
                if type(value) is dict and value.get('id') == request_id:
                    if 'error' in value or type(value.get('result')) is not dict:
                        raise ValueError('MCP_RESPONSE_INVALID')
                    return value['result']
        try:
            send({'jsonrpc':'2.0','id':1,'method':'initialize','params':{
                'protocolVersion':'2025-03-26','capabilities':{},
                'clientInfo':{'name':'harness-readonly','version':BRIDGE_VERSION}}})
            initialized = receive(1)
            if not isinstance(initialized.get('protocolVersion'),str):
                raise ValueError('MCP_HANDSHAKE_INVALID')
            send({'jsonrpc':'2.0','method':'notifications/initialized'})
            send({'jsonrpc':'2.0','id':2,'method':'tools/list','params':{}})
            listed = receive(2)
            rows = listed.get('tools')
            if type(rows) is not list:
                raise ValueError('MCP_TOOL_SCHEMA_INVALID')
            matches = [row for row in rows if type(row) is dict and row.get('name') == tool]
            if len(matches) != 1 or type(matches[0].get('inputSchema')) is not dict:
                raise ValueError('MCP_TOOL_SCHEMA_INVALID')
            schema = matches[0]['inputSchema']
            if set((schema.get('properties') or {})) - self.allowed[tool]:
                raise ValueError('MCP_TOOL_SCHEMA_DRIFT')
            if matches[0].get('annotations',{}).get('readOnlyHint') is not True:
                raise ValueError('MCP_READONLY_ATTESTATION_REQUIRED')
            send({'jsonrpc':'2.0','id':3,'method':'tools/call','params':{'name':tool,'arguments':arguments}})
            return receive(3)
        finally:
            process.kill() if process.poll() is None else None
            try: process.wait(timeout=2)
            except subprocess.TimeoutExpired: pass
            for pipe in (process.stdin, process.stdout, process.stderr):
                try: pipe.close()
                except OSError: pass
            for thread in threads: thread.join(timeout=.5)


class HostBridges:
    def __init__(self, control_root: str | Path, *, config_path: str | Path | None = None,
                 commands: dict[str, JsonCommand] | None = None, jev: JevAdapter | None = None):
        self.control_root = Path(control_root).resolve()
        self.config_path = Path(config_path) if config_path else self.control_root / 'host-bridges.json'
        self.commands = commands
        self.jev = jev

    @classmethod
    def discover(cls, control_root: str | Path):
        """Return an explicitly configured bridge or leave 1.0 behavior intact.

        Merely importing 1.1 code or starting Harness must not activate optional
        host observations.  The only automatic discovery surface is a real
        ``host-bridges.json`` inside the selected Control root.  Source-adjacent
        templates are never runtime configuration.
        """
        control_root = Path(control_root).resolve()
        config_path = control_root / 'host-bridges.json'
        if not config_path.is_file():
            return None
        bridge = cls(control_root, config_path=config_path)
        return bridge if bridge._config() else None

    def _config(self):
        path = self.config_path
        if not path.is_file():
            return {}
        raw = path.read_bytes()
        if len(raw) > 16384:
            return {}
        try:
            value = strict_json(raw, 16384)
            return value if (type(value) is dict and value.get('schema') == BRIDGE_VERSION
                and Path(value.get('control_root','')).resolve() == self.control_root) else {}
        except Exception:
            return {}

    def _command(self, name):
        if self.commands is not None:
            return self.commands.get(name)
        spec = self._config().get('commands', {}).get(name)
        if spec is None:
            return None
        try:
            return JsonCommand(name, spec)
        except Exception:
            return None

    @staticmethod
    def _binding(job_id: str, execution_id: str, attempt_id: str, contract: object,
                 *, candidate_id='NO_CANDIDATE', candidate_hash=None) -> Binding:
        zero = '0' * 64
        return Binding(str(job_id), str(execution_id), str(attempt_id), candidate_id,
                       digest(contract), candidate_hash or zero, zero, 'HOST_OBSERVED')

    def _save(self, kind: str, binding: Binding, payload: dict):
        # Opaque receipt IDs allow retries to be idempotent. No provider payload
        # or external prose enters the immutable Execution or policy decision.
        from harness_11.evidence import ReceiptStore
        from harness_11.core import canonical
        safe = json.loads(scrub_secrets(canonical(payload).decode('utf-8')))
        receipt_id = hashlib.sha256((kind + binding.key + digest(safe)).encode()).hexdigest()
        try:
            return ReceiptStore(self.control_root / 'host-bridge-receipts').append(
                receipt_id, binding, kind=kind, payload=safe, provenance='HOST_OBSERVED')
        except Exception:
            return {'receipt_id': '', 'status': 'RECEIPT_UNAVAILABLE', 'product_pass': False}

    def _jev_adapter(self):
        if self.jev is not None:
            return self.jev
        config = self._config()
        if not config.get('jev_shadow_enabled'):
            return None
        # The existing per-user key is read in memory only; neither its value
        # nor a secret-bearing environment map is persisted or logged.
        secret = os.environ.get('jev_key', '')
        if not secret and os.name == 'nt':
            try:
                import winreg
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER, 'Environment') as key:
                    secret = winreg.QueryValueEx(key, 'jev_key')[0]
            except OSError:
                pass
        if not secret:
            return None
        return JevAdapter(JevConfig(model='jev-latest', mode=Mode.SHADOW,
            external_calls_authorized=True, max_calls=4), env={'JEV_API_KEY': secret})

    def materialized(self, job: dict, execution: dict, policy: dict) -> dict:
        """Observe the actual frozen routing and Supervisor admission outcome."""
        risk_label = (policy.get('resolved_reviewer') or {}).get('risk_level', 'LOW')
        actual = {'route': job['request']['worker'],
                  'needs_supervisor': bool((policy.get('supervisor_policy') or {}).get('supervisor_required'))
                      or risk_label in {'HIGH','CRITICAL'},
                  'risk': risk_label,
                  'admission': execution.get('supervisor_admission', {})}
        binding = self._binding(job['job_id'], execution['execution_id'], 'ADMISSION', execution)
        result = {'version': BRIDGE_VERSION, 'actual': actual, 'case': 'ADMISSION', 'jev': 'OFF'}
        try:
            risk = Risk(actual['risk'])
            adapter = self._jev_adapter()
            if adapter is not None:
                questions = (
                    Question('needs_supervisor', UseCase.NEEDS_SUPERVISOR,
                             Choice('Would this case need Supervisor?', (('yes', None), ('no', None)))),
                    Question('runtime_route', UseCase.RUNTIME_ROUTE_SIGNAL,
                             Choice('Which approved route?', tuple((x, None) for x in ('codex','droid','hermes','opencode','pi')))),
                )
                snapshot = PolicySnapshot(risk, frozenset(), ('codex','droid','hermes','opencode','pi'),
                                          actual['route'], actual['needs_supervisor'], digest(policy))
                state = {'case': 'ADMISSION', 'job_id': job['job_id'], 'execution_id': execution['execution_id'],
                         'risk': actual['risk'], 'actual_route': actual['route'],
                         'actual_needs_supervisor': actual['needs_supervisor']}
                result['jev'] = adapter.shadow(binding, snapshot, state=state,
                                               approved_state_hash=digest(state), questions=questions)
            result['schema_valid'] = result['jev'] == 'OFF' or result['jev'].get('status') == 'SHADOW_RECORDED'
            result['provider_error'] = '' if result['jev'] == 'OFF' else result['jev'].get('error_code','')
            result['elapsed_seconds'] = 0.0 if result['jev'] == 'OFF' else result['jev'].get('elapsed_seconds',0.0)
        except Exception:
            result.update(jev='SAFE_FALLBACK', schema_valid=False, provider_error='JEV_BRIDGE_FAILED',
                          elapsed_seconds=0.0)
        result['receipt'] = self._save('ADMISSION_SHADOW', binding, result)
        return result

    def semantic_failure(self, task, *, reason: str, event_id: str) -> dict:
        """Called only after Harness classifies a Product semantic retry."""
        binding = self._binding(task.job_id, task.execution_id, task.attempt_id,
                                task.materialized_execution, candidate_id=task.candidate_id,
                                candidate_hash=digest(task.changed_file_sha256))
        result = {'version': BRIDGE_VERSION, 'case': 'SEMANTIC_FAILURE_SIGNAL',
                  'actual': {'retry_domain': task.retry_domain, 'reason': reason},
                  'advisor': 'OFF', 'jev': 'OFF'}
        if task.retry_domain != 'PRODUCT_SEMANTIC_RETRY':
            result['status'] = 'TECHNICAL_EVENT_IGNORED'
            return result
        try:
            command = self._command('advisor')
            if command:
                from harness_11.advisor import Advisor, AdvisorPolicy, Failure
                failure = Failure(event_id,task.job_id,task.execution_id,task.attempt_id,
                    binding.contract_hash,task.retry_domain,'SEMANTIC_REWORK',
                    'HOST_CLASSIFIED:'+reason)
                result['advisor'] = Advisor(AdvisorPolicy(mode=Mode.SHADOW,threshold=1)).evaluate(
                    binding,(failure,),verify_receipt=lambda item: item == failure,
                    invoke=lambda payload, **bounds: command.call(payload, **bounds))
            else:
                result['advisor'] = 'ADVISOR_UNAVAILABLE'
        except Exception:
            result['advisor'] = 'SAFE_FALLBACK'
        try:
            adapter = self._jev_adapter()
            if adapter is not None:
                risk = Risk((task.review_policy or {}).get('risk_level', 'LOW'))
                route = str(task.worker)
                snapshot = PolicySnapshot(risk, frozenset(), (route,), route,
                    bool((task.effective_policy.get('supervisor_policy') or {}).get('supervisor_required')),
                    digest(task.effective_policy))
                state = {'event_id': event_id, 'retry_domain': task.retry_domain, 'reason': reason}
                result['jev'] = adapter.shadow(binding, snapshot, state=state,
                    approved_state_hash=digest(state), questions=(Question('semantic_failure',
                    UseCase.SEMANTIC_FAILURE_SIGNAL, Choice('Is this a semantic failure?',
                    (('yes',None),('no',None)))),))
        except Exception:
            result['jev'] = 'SAFE_FALLBACK'
        result['jev_schema_valid'] = result['jev'] == 'OFF' or (
            isinstance(result['jev'],dict) and result['jev'].get('status') == 'SHADOW_RECORDED')
        result['jev_provider_error'] = (result['jev'].get('error_code','')
            if isinstance(result['jev'],dict) else '' if result['jev']=='OFF' else 'JEV_BRIDGE_FAILED')
        result['jev_elapsed_seconds'] = (result['jev'].get('elapsed_seconds',0.0)
            if isinstance(result['jev'],dict) else 0.0)
        result['receipt'] = self._save('SEMANTIC_ADVISORY', binding, result)
        return result

    def review_shadow(self, task, actual_review) -> dict:
        binding = self._binding(task.job_id, task.execution_id, task.attempt_id,
            task.materialized_execution, candidate_id=task.candidate_id,
            candidate_hash=digest(task.changed_file_sha256))
        result = {'version': BRIDGE_VERSION, 'actual_status': actual_review.status,
                  'ocr_status': 'CREDENTIAL_REQUIRED_OPTIONAL_FEATURE', 'security_status': 'OFF',
                  'product_pass': False, 'publication_authority': False}
        try:
            risk = Risk(str(getattr(actual_review, 'risk', '') or (task.review_policy or {}).get('risk_level', 'LOW')))
            tags = frozenset(task.security_tags or ())
            explicit = task.security_audit_requested is True
            identity = ToolIdentity('security_audit','host-command','1','0'*64,'0'*64,'LOCAL') if self._command('security_audit') else None
            activation = security_activation(risk=risk, tags=tags, explicit_audit=explicit,
                mode=Mode.SHADOW if identity else Mode.OFF, installed_identity=identity)
            result['security_status'] = activation['status']
            if activation['activate']:
                result['security_result'] = self._command('security_audit').call({
                    'schema': BRIDGE_VERSION, 'risk': risk.value, 'tags': sorted(tags),
                    'changed_files': list(task.changed_files), 'authority': 'SHADOW_ONLY'})
        except Exception:
            result['security_status'] = 'SAFE_FALLBACK'
        try:
            command = self._command('alibaba_review')
            credential_ref = self._config().get('ocr_credential_env','')
            credential = (os.environ.get(credential_ref,'') if isinstance(credential_ref,str)
                and re.fullmatch('[A-Z][A-Z0-9_]{0,63}',credential_ref) else '')
            if command and (self.commands is not None or credential):
                from harness_11.review_support import Finding
                response = command.call({'schema': BRIDGE_VERSION, 'diff': task.git_diff[:60000],
                    'changed_files': list(task.changed_files), 'authority': 'SHADOW_ONLY',
                    'provider_credential': credential})
                if credential and credential in json.dumps(response,ensure_ascii=False):
                    raise ValueError('OCR_CREDENTIAL_ECHO')
                if set(response) != {'findings','cost_usd'} or type(response['findings']) is not list:
                    raise ValueError('OCR_SCHEMA_INVALID')
                cost = response['cost_usd']
                if cost is not None and (not isinstance(cost,(int,float)) or isinstance(cost,bool)
                                         or not math.isfinite(cost) or cost < 0):
                    raise ValueError('OCR_COST_INVALID')
                findings = [asdict(Finding(**row)) for row in response['findings']]
                if any(row['path'] not in set(task.changed_files) for row in findings):
                    raise ValueError('OCR_FINDING_OUTSIDE_CHANGED_SCOPE')
                result.update(ocr_status='SHADOW_EVIDENCE', ocr_findings=findings)
        except Exception:
            result['ocr_status'] = 'SAFE_FALLBACK'
        result['receipt'] = self._save('REVIEW_SHADOW', binding, result)
        return result

    def context_observation(self, task, pack, *, workspace: Path) -> dict:
        """Observe a valid native context pack without replacing its authority."""
        from harness_11.core import Resource, Scope
        from harness_11.retrieval import Record, retrieve
        frozen = task.materialized_execution or {}
        binding = self._binding(str(task.job_id or task.task_id),
            str(task.execution_id or task.task_id), str(task.attempt_id or 'CONTEXT'),
            frozen or pack.manifest)
        result = {'version': BRIDGE_VERSION, 'actual_context_pack_id': pack.manifest['context_pack_id'],
                  'actual_context_hash': hashlib.sha256(pack.prompt.encode('utf-8')).hexdigest(),
                  'serena_status': 'OFF', 'repomix_status': 'OFF', 'retrieval_status': 'OFF',
                  'jev_status': 'OFF', 'context7_status': 'OFF', 'chrome_devtools_status': 'OFF',
                  'context_replaced': False, 'product_pass': False}
        config = self._config()
        try:
            command = self._command('repomix')
            if command:
                response = command.call({'schema':BRIDGE_VERSION, 'context':pack.prompt,
                    'budget_bytes':len(pack.prompt.encode('utf-8')), 'authority':'OPTIONAL_PACKAGING'},
                    max_bytes=min(65536, max(1024,len(pack.prompt.encode('utf-8')) * 2)))
                candidate = response.get('context')
                if type(candidate) is not str or len(candidate.encode('utf-8')) > len(pack.prompt.encode('utf-8')):
                    raise ValueError('REPOMIX_OUTPUT_INVALID')
                result.update(repomix_status='PACKAGED_SHADOW',
                    repomix_hash=hashlib.sha256(candidate.encode('utf-8')).hexdigest())
                from harness_11.context import ContextBudget
                candidate_root = Path(workspace).resolve()
                canonical_root = (Path(config['canonical_root']).resolve()
                    if config.get('canonical_root') else candidate_root)
                files = tuple(str(name).replace('\\','/') for name in task.changed_files
                    if isinstance(name,str) and (candidate_root/name).is_file())
                if files and candidate_root != canonical_root:
                    scope = Scope(str(candidate_root),str(canonical_root),('context-read',),
                                  (Resource('repo/source','READ'),))
                    identity = ToolIdentity('repomix','host-command',command.spec['version'],
                        command.spec['sha256'],hashlib.sha256(BRIDGE_VERSION.encode()).hexdigest(),
                        'PINNED_LOCAL_PROCESS')
                    budget = ContextBudget(byte_budget=65536,token_budget=65536,mode=Mode.SHADOW)
                    measured = budget.package(binding,scope,files,package_identity=identity,
                        packager=lambda parts, **_: command.call({'schema':BRIDGE_VERSION,
                            'parts':parts,'authority':'OPTIONAL_PACKAGING'})['context'])
                    result['repomix_budget_status'] = measured['status']
                    result['repomix_native_fallback'] = measured.get('packager_used') is False
        except Exception:
            result['repomix_status'] = 'SAFE_FALLBACK_OR_BUDGET'
        try:
            rows = config.get('retrieval_records') or []
            if rows:
                records = tuple(Record(**{**row,'tags':tuple(row['tags'])}) for row in rows[:1000])
                profile_hash = pack.manifest['profile_snapshot_sha256']
                source_revision = str((frozen.get('harness_source_identity') or {}).get('source_fingerprint') or 'CURRENT')
                hints = retrieve(str(task.requirement)[:2000], records,
                    project=pack.manifest['profile_id'], profile_hash=profile_hash,
                    source_revision=source_revision, mode=Mode.SHADOW,
                    verify_evidence=lambda row: Path(row.evidence_ref).is_file())
                result.update(retrieval_status=hints['status'], retrieval_items=hints['items'])
        except Exception:
            result['retrieval_status'] = 'SAFE_FALLBACK'
        try:
            spec = config.get('serena')
            candidate = Path(workspace).resolve()
            canonical = Path(config.get('canonical_root','')).resolve() if config.get('canonical_root') else candidate
            if spec and candidate != canonical:
                files = [str(x) for x in task.changed_files if isinstance(x,str)]
                relative = next((x.replace('\\','/') for x in files if (candidate / x).is_file()), None)
                if relative:
                    original_scope = Scope(str(candidate),str(canonical),('context-read',),
                                           (Resource('repo/source','READ'),))
                    source_bytes = read_source_bytes(original_scope,relative,1_048_576)
                    with tempfile.TemporaryDirectory(prefix='h11-serena-') as folder:
                        snapshot = Path(folder)
                        copied = snapshot/relative
                        copied.parent.mkdir(parents=True,exist_ok=True)
                        copied.write_bytes(source_bytes)
                        scope = Scope(str(snapshot),str(canonical),('context-read',),
                                      (Resource('repo/source','READ'),))
                        transport = MCPReadOnly(spec,snapshot)
                        identity = ToolIdentity('serena','stdio',str(spec['version']),str(spec['sha256']),
                            hashlib.sha256(BRIDGE_VERSION.encode()).hexdigest(),'HOST_PINNED_READONLY')
                        boundary = ContextBoundary('serena',mode=Mode.SHADOW,identity=identity,
                            transport=transport,timeout_seconds=5)
                        native = lambda: {'path':relative,'sha256':hashlib.sha256(source_bytes).hexdigest()}
                        observed = boundary.query(binding,scope,'get_symbols_overview',
                            {'relative_path':relative},external_data_approved=True,fallback=native)
                    result['serena_status'] = observed['status']
                    result['serena_error_code'] = observed.get('error_code','')
                    result['serena_native_fallback'] = 'fallback_source_search' in observed
        except Exception:
            result['serena_status'] = 'SAFE_FALLBACK'
        for kind, tool, arguments in (
            ('context7','resolve-library-id',{'libraryName':'python','query':'documented API'}),
            ('chrome_devtools','list_console_messages',{'pageSize':20,'pageIdx':0}),
        ):
            try:
                spec = config.get(kind)
                if not spec or config.get(kind + '_data_approved') is not True:
                    continue
                with tempfile.TemporaryDirectory(prefix='h11-mcp-') as folder:
                    snapshot = Path(folder)
                    canonical = Path(config.get('canonical_root','')).resolve() if config.get('canonical_root') else Path(workspace).resolve()
                    scope = Scope(str(snapshot),str(canonical),('context-read',),
                                  (Resource('repo/source','READ'),))
                    transport = MCPReadOnly(spec,snapshot,kind=kind)
                    identity = ToolIdentity(kind,'stdio',str(spec['version']),str(spec['sha256']),
                        hashlib.sha256(BRIDGE_VERSION.encode()).hexdigest(),'HOST_PINNED_READONLY')
                    observation = ContextBoundary(kind,mode=Mode.SHADOW,identity=identity,
                        transport=transport,timeout_seconds=5).query(binding,scope,tool,arguments,
                        external_data_approved=True)
                result[kind + '_status'] = observation['status']
                result[kind + '_error_code'] = observation.get('error_code','')
            except Exception:
                result[kind + '_status'] = 'SAFE_FALLBACK'
        try:
            adapter = self._jev_adapter()
            if adapter:
                risk = Risk((task.review_policy or {}).get('risk_level','LOW'))
                route = str(task.worker or 'codex')
                snapshot = PolicySnapshot(risk,frozenset(),(route,),route,
                    bool((task.effective_policy.get('supervisor_policy') or {}).get('supervisor_required')),
                    digest(task.effective_policy or {}))
                state = {'context_pack_id':pack.manifest['context_pack_id'],
                         'selected_resources':len(pack.manifest['selected_resources']),
                         'actual_context_hash':result['actual_context_hash']}
                observed = adapter.shadow(binding,snapshot,state=state,approved_state_hash=digest(state),
                    questions=(Question('context_relevance',UseCase.CONTEXT_RELEVANCE,
                    Choice('Is the selected context relevant?',(('yes',None),('no',None)))),))
                result['jev_status'] = observed['status']
                result['jev'] = observed
        except Exception:
            result['jev_status'] = 'SAFE_FALLBACK'
        result['jev_schema_valid'] = result['jev_status'] in {'OFF','SHADOW_RECORDED'}
        result['jev_provider_error'] = (result.get('jev',{}).get('error_code','')
            if isinstance(result.get('jev'),dict) else 'JEV_BRIDGE_FAILED' if result['jev_status']=='SAFE_FALLBACK' else '')
        result['jev_elapsed_seconds'] = (result.get('jev',{}).get('elapsed_seconds',0.0)
            if isinstance(result.get('jev'),dict) else 0.0)
        result['receipt'] = self._save('CONTEXT_SHADOW',binding,result)
        return result
