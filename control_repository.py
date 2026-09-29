"""Transactional single-host control authority. Historical files remain evidence."""

from __future__ import annotations

from contextlib import contextmanager, closing
from datetime import datetime
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import uuid

SCHEMA_VERSION = 5
CANDIDATE_HANDOFF_CONFIRMATION = "HANDOFF_PRESERVED_CANDIDATE_OWNERSHIP_ONLY"
SCHEMA = """
CREATE TABLE IF NOT EXISTS control_state (
  key TEXT PRIMARY KEY, revision INTEGER NOT NULL CHECK(revision >= 0),
  payload TEXT NOT NULL CHECK(json_valid(payload))
);
CREATE TABLE IF NOT EXISTS logical_jobs (
  job_id TEXT PRIMARY KEY, logical_job_id TEXT NOT NULL,
  status TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision >= 0),
  phase TEXT NOT NULL CHECK(phase IN ('PLANNED','READY','MATERIALIZED','RUNNING','TERMINAL')),
  contract_revision INTEGER NOT NULL DEFAULT 1, owner_id TEXT NOT NULL DEFAULT '',
  payload TEXT NOT NULL CHECK(json_valid(payload)), legacy_job_ref TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS executions (
  execution_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES logical_jobs(job_id),
  logical_job_id TEXT NOT NULL, contract_revision INTEGER NOT NULL,
  phase TEXT NOT NULL CHECK(phase IN ('MATERIALIZED','RUNNING','TERMINAL')),
  profile_revision TEXT NOT NULL, policy_revision TEXT NOT NULL,
  effective_policy_hash TEXT NOT NULL, baseline_hash TEXT NOT NULL,
  execution_surface_hash TEXT NOT NULL, materialized_at TEXT NOT NULL,
  payload TEXT NOT NULL CHECK(json_valid(payload)), result TEXT CHECK(result IS NULL OR json_valid(result))
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_execution ON executions((1)) WHERE phase = 'RUNNING';
CREATE TRIGGER IF NOT EXISTS immutable_terminal_execution BEFORE UPDATE ON executions
WHEN OLD.phase = 'TERMINAL' BEGIN SELECT RAISE(ABORT, 'TERMINAL_EXECUTION_IMMUTABLE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_execution_contract BEFORE UPDATE ON executions
WHEN OLD.payload != NEW.payload OR OLD.execution_surface_hash != NEW.execution_surface_hash
OR OLD.contract_revision != NEW.contract_revision OR OLD.profile_revision != NEW.profile_revision
OR OLD.policy_revision != NEW.policy_revision OR OLD.effective_policy_hash != NEW.effective_policy_hash
OR OLD.baseline_hash != NEW.baseline_hash OR OLD.job_id != NEW.job_id
BEGIN SELECT RAISE(ABORT, 'MATERIALIZED_CONTRACT_IMMUTABLE'); END;
CREATE TABLE IF NOT EXISTS attempts (
  attempt_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(execution_id),
  role TEXT NOT NULL, model TEXT NOT NULL, profile_revision TEXT NOT NULL,
  policy_revision TEXT NOT NULL, execution_surface_hash TEXT NOT NULL,
  candidate_hash TEXT NOT NULL, started_at TEXT NOT NULL,
  payload TEXT NOT NULL CHECK(json_valid(payload))
);
CREATE TABLE IF NOT EXISTS materializations (
  materialization_id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL UNIQUE REFERENCES executions(execution_id),
  created_at TEXT NOT NULL,
  payload TEXT NOT NULL CHECK(json_valid(payload))
);
CREATE TABLE IF NOT EXISTS attempt_outcomes (
  attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id),
  status TEXT NOT NULL CHECK(status IN ('RUNNING','SUCCEEDED','FAILED','INTERRUPTED','UNKNOWN')),
  retry_domain TEXT NOT NULL CHECK(retry_domain IN ('','RUNTIME_RECOVERY','TECHNICAL_EXECUTION_RETRY','PRODUCT_SEMANTIC_RETRY')),
  failure_code TEXT NOT NULL DEFAULT '', product_retry_delta INTEGER NOT NULL DEFAULT 0 CHECK(product_retry_delta IN (0,1)),
  finished_at TEXT NOT NULL DEFAULT '', payload TEXT NOT NULL CHECK(json_valid(payload))
);
CREATE TABLE IF NOT EXISTS native_sessions (
  binding_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(execution_id),
  runtime TEXT NOT NULL, role TEXT NOT NULL,
  native_session_id TEXT NOT NULL, logical_job_id TEXT NOT NULL,
  contract_revision INTEGER NOT NULL, workspace_identity TEXT NOT NULL,
  source_view_id TEXT NOT NULL, runtime_contract_hash TEXT NOT NULL,
  candidate_hash TEXT NOT NULL, review_contract_hash TEXT NOT NULL,
  state TEXT NOT NULL,
  durable INTEGER NOT NULL CHECK(durable IN (0,1)), adapter_revision TEXT NOT NULL,
  runtime_version TEXT NOT NULL, bound_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
  payload TEXT NOT NULL CHECK(json_valid(payload))
);
CREATE TABLE IF NOT EXISTS runtime_processes (
  process_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
  binding_id TEXT REFERENCES native_sessions(binding_id), runtime TEXT NOT NULL,
  pid INTEGER NOT NULL CHECK(pid >= 0), process_identity TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('STARTING','RUNNING','EXITED','KILLED','LOST','UNKNOWN')),
  started_at TEXT NOT NULL, ended_at TEXT NOT NULL DEFAULT '', exit_code INTEGER,
  payload TEXT NOT NULL CHECK(json_valid(payload))
);
CREATE TABLE IF NOT EXISTS commands (
  command_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(execution_id),
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
  binding_id TEXT REFERENCES native_sessions(binding_id), idempotency_key TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK(ordinal > 0), payload_sha256 TEXT NOT NULL,
  delivery_status TEXT NOT NULL CHECK(delivery_status IN ('PREPARED','SENT','ACKNOWLEDGED','COMPLETED','FAILED_BEFORE_SEND','UNKNOWN')),
  native_command_id TEXT NOT NULL DEFAULT '', native_turn_id TEXT NOT NULL DEFAULT '',
  prepared_at TEXT NOT NULL, sent_at TEXT NOT NULL DEFAULT '', acknowledged_at TEXT NOT NULL DEFAULT '',
  completed_at TEXT NOT NULL DEFAULT '', payload TEXT NOT NULL CHECK(json_valid(payload)),
  UNIQUE(execution_id,idempotency_key), UNIQUE(execution_id,ordinal)
);
CREATE TABLE IF NOT EXISTS native_turns (
  turn_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL REFERENCES native_sessions(binding_id),
  command_id TEXT NOT NULL UNIQUE REFERENCES commands(command_id), native_turn_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('SUBMITTED','RUNNING','WAITING_INPUT','SUCCEEDED','FAILED','INTERRUPTED','UNKNOWN')),
  started_at TEXT NOT NULL, finished_at TEXT NOT NULL DEFAULT '',
  payload TEXT NOT NULL CHECK(json_valid(payload)), UNIQUE(binding_id,native_turn_id)
);
CREATE TABLE IF NOT EXISTS runtime_recoveries (
  recovery_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(execution_id),
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id), binding_id TEXT REFERENCES native_sessions(binding_id),
  failure_code TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal > 0),
  state TEXT NOT NULL CHECK(state IN ('STARTED','RESUMED','FRESH_ATTEMPT_REQUIRED','EXHAUSTED','FAILED')),
  product_retry_delta INTEGER NOT NULL DEFAULT 0 CHECK(product_retry_delta = 0),
  recorded_at TEXT NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload)),
  UNIQUE(execution_id,ordinal)
);
CREATE TABLE IF NOT EXISTS execution_authority (
  execution_id TEXT PRIMARY KEY REFERENCES executions(execution_id),
  state TEXT NOT NULL CHECK(state IN ('ACTIVE','SUSPENDED','RELEASED')),
  revision INTEGER NOT NULL CHECK(revision >= 0), reason TEXT NOT NULL,
  updated_at TEXT NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload))
);
CREATE TABLE IF NOT EXISTS inbound_events (
  event_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(execution_id),
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
  binding_id TEXT NOT NULL REFERENCES native_sessions(binding_id), runtime TEXT NOT NULL,
  provider_event_id TEXT NOT NULL, native_session_id TEXT NOT NULL,
  native_turn_id TEXT NOT NULL, event_type TEXT NOT NULL,
  sequence TEXT NOT NULL, cursor TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
  event_fingerprint TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK(status IN ('RECEIVED','APPLIED','AMBIGUOUS','LATE')),
  received_at TEXT NOT NULL, applied_revision INTEGER,
  applied_state TEXT NOT NULL DEFAULT '', payload TEXT NOT NULL CHECK(json_valid(payload))
);
CREATE TABLE IF NOT EXISTS decisions (
  decision_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES logical_jobs(job_id),
  contract_revision INTEGER NOT NULL, decision_type TEXT NOT NULL CHECK(decision_type IN ('CANDIDATE','CONTRACT','POLICY','DEFERRED')),
  status TEXT NOT NULL CHECK(status IN ('OPEN','RESOLVED','REJECTED','STALE')),
  question TEXT NOT NULL, resolution TEXT NOT NULL DEFAULT '', fingerprint TEXT NOT NULL,
  created_at TEXT NOT NULL, resolved_at TEXT NOT NULL DEFAULT '',
  payload TEXT NOT NULL CHECK(json_valid(payload)), UNIQUE(job_id,contract_revision,fingerprint)
);
CREATE TABLE IF NOT EXISTS validation_dispositions (
  validation_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(execution_id),
  kind TEXT NOT NULL, disposition TEXT NOT NULL CHECK(disposition IN ('PASS','FAIL','NOT_RUN_BY_POLICY','NOT_RUN','UNAVAILABLE')),
  product_retry_delta INTEGER NOT NULL DEFAULT 0 CHECK(product_retry_delta IN (0,1)),
  evidence_ref TEXT NOT NULL DEFAULT '', recorded_at TEXT NOT NULL,
  payload TEXT NOT NULL CHECK(json_valid(payload)), UNIQUE(execution_id,kind)
);
CREATE TABLE IF NOT EXISTS verification_evidence (
  verification_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(execution_id),
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id), scenario_id TEXT NOT NULL,
  candidate_hash TEXT NOT NULL, contract_revision INTEGER NOT NULL,
  environment_hash TEXT NOT NULL, verification_depth TEXT NOT NULL,
  result TEXT NOT NULL CHECK(result IN ('PASS','FAIL','NOT_RUN_BY_POLICY','NOT_RUN','UNAVAILABLE')),
  failed_step TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL,
  completed_at TEXT NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload)),
  UNIQUE(execution_id,scenario_id,candidate_hash,contract_revision,environment_hash)
);
CREATE TABLE IF NOT EXISTS product_readiness (
  job_id TEXT PRIMARY KEY REFERENCES logical_jobs(job_id), contract_revision INTEGER NOT NULL,
  readiness TEXT NOT NULL CHECK(readiness IN ('COMPLETE','SKELETON_READY','FINALIZATION_PENDING')),
  user_action_required INTEGER NOT NULL CHECK(user_action_required IN (0,1)),
  open_decision_count INTEGER NOT NULL CHECK(open_decision_count >= 0),
  updated_at TEXT NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload))
);
CREATE TABLE IF NOT EXISTS dependencies (
  job_id TEXT NOT NULL REFERENCES logical_jobs(job_id), depends_on_job_id TEXT NOT NULL,
  payload TEXT NOT NULL CHECK(json_valid(payload)), PRIMARY KEY(job_id, depends_on_job_id)
);
CREATE TABLE IF NOT EXISTS conflicts (
  job_id TEXT NOT NULL REFERENCES logical_jobs(job_id), resource TEXT NOT NULL,
  PRIMARY KEY(job_id, resource)
);
CREATE TABLE IF NOT EXISTS ownership (
  path TEXT PRIMARY KEY, job_id TEXT NOT NULL, candidate_hash TEXT NOT NULL,
  classification TEXT NOT NULL, evidence_ref TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS notifications (
  channel TEXT NOT NULL, subject TEXT NOT NULL, fingerprint TEXT NOT NULL,
  delivered_at TEXT NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload)),
  PRIMARY KEY(channel, subject)
);
CREATE TRIGGER IF NOT EXISTS no_attempt_on_terminal_execution BEFORE INSERT ON attempts
WHEN (SELECT phase FROM executions WHERE execution_id=NEW.execution_id)='TERMINAL'
BEGIN SELECT RAISE(ABORT, 'TERMINAL_EXECUTION_IMMUTABLE'); END;
CREATE TRIGGER IF NOT EXISTS no_command_on_terminal_execution BEFORE INSERT ON commands
WHEN (SELECT phase FROM executions WHERE execution_id=NEW.execution_id)='TERMINAL'
BEGIN SELECT RAISE(ABORT, 'TERMINAL_EXECUTION_IMMUTABLE'); END;
"""


NATIVE_SESSIONS_V3 = """
CREATE TABLE native_sessions_new (
  binding_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(execution_id),
  runtime TEXT NOT NULL, role TEXT NOT NULL,
  native_session_id TEXT NOT NULL, logical_job_id TEXT NOT NULL,
  contract_revision INTEGER NOT NULL, workspace_identity TEXT NOT NULL,
  source_view_id TEXT NOT NULL, runtime_contract_hash TEXT NOT NULL,
  candidate_hash TEXT NOT NULL, review_contract_hash TEXT NOT NULL,
  state TEXT NOT NULL,
  durable INTEGER NOT NULL CHECK(durable IN (0,1)), adapter_revision TEXT NOT NULL,
  runtime_version TEXT NOT NULL, bound_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
  payload TEXT NOT NULL CHECK(json_valid(payload))
);
"""


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def now():
    return datetime.now().astimezone().isoformat()


class RepositoryError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class ControlRepository:
    def __init__(self, path, *, read_only=False, create=False, legacy_root=None):
        self.path = Path(path).resolve()
        self.read_only = read_only
        self.legacy_root = Path(legacy_root or self.path.parent).resolve()
        self._local = threading.local()
        self._guard = threading.RLock()
        self._bound_control_epoch = None
        self._database_file_identity = None
        if create:
            if read_only:
                raise RepositoryError("CONTROL_REPOSITORY_READ_ONLY")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with closing(sqlite3.connect(self.path)) as connection:
                connection.row_factory = sqlite3.Row
                connection.executescript(SCHEMA)
                connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                connection.commit()
        if not self.path.is_file():
            raise RepositoryError("CONTROL_REPOSITORY_MISSING")
        if not read_only:
            with closing(sqlite3.connect(self.path)) as connection:
                connection.row_factory = sqlite3.Row
                version = int(connection.execute("PRAGMA user_version").fetchone()[0])
                if version in {1, 2, 3, 4}:
                    # Source migrations preserve queue/job/execution payloads.
                    # Later schemas add authority/event/evidence records and
                    # role/source-view fields to native session bindings.
                    connection.executescript(SCHEMA)
                    native_columns = {
                        row[1] for row in connection.execute("PRAGMA table_info(native_sessions)")
                    }
                    native_sql_row = connection.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' AND name='native_sessions'"
                    ).fetchone()
                    native_sql = "".join(str(native_sql_row[0] or "").split()).casefold()
                    if ("role" not in native_columns
                            or "unique(runtime,native_session_id)" in native_sql):
                        connection.execute("PRAGMA foreign_keys=OFF")
                        connection.executescript(NATIVE_SESSIONS_V3)
                        rows = connection.execute(
                            "SELECT n.*,e.logical_job_id,e.contract_revision,e.payload AS execution_payload "
                            "FROM native_sessions n JOIN executions e ON e.execution_id=n.execution_id"
                        ).fetchall()
                        for row in rows:
                            execution_payload = json.loads(row["execution_payload"])
                            context = dict(execution_payload.get("execution_context") or {})
                            payload = json.loads(row["payload"])
                            role = str(row["role"] if "role" in native_columns else "WORKER")
                            workspace_identity = str(
                                row["workspace_identity"] if "workspace_identity" in native_columns
                                else context.get("workspace_identity_sha256", "")
                            )
                            source_view_id = str(
                                row["source_view_id"] if "source_view_id" in native_columns
                                else execution_payload.get("source_view_id", "")
                            )
                            runtime_contract_hash = str(
                                row["runtime_contract_hash"] if "runtime_contract_hash" in native_columns
                                else execution_payload.get("runtime_contract_hash", "")
                            )
                            candidate_hash = str(row["candidate_hash"] if "candidate_hash" in native_columns else "")
                            review_contract_hash = str(
                                row["review_contract_hash"] if "review_contract_hash" in native_columns else ""
                            )
                            payload.update({
                                "role": role,
                                "logical_job_id": row["logical_job_id"],
                                "contract_revision": int(row["contract_revision"]),
                                "workspace_identity": workspace_identity,
                                "source_view_id": source_view_id,
                                "runtime_contract_hash": runtime_contract_hash,
                                "candidate_hash": candidate_hash,
                                "review_contract_hash": review_contract_hash,
                            })
                            connection.execute(
                                "INSERT INTO native_sessions_new VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (
                                    row["binding_id"], row["execution_id"], row["runtime"], role,
                                    row["native_session_id"], row["logical_job_id"], int(row["contract_revision"]),
                                    workspace_identity, source_view_id,
                                    runtime_contract_hash, candidate_hash, review_contract_hash,
                                    row["state"], row["durable"],
                                    row["adapter_revision"], row["runtime_version"], row["bound_at"],
                                    row["last_seen_at"], canonical(payload),
                                ),
                            )
                        connection.execute("DROP TABLE native_sessions")
                        connection.execute("ALTER TABLE native_sessions_new RENAME TO native_sessions")
                    rows = connection.execute(
                        "SELECT execution_id,materialized_at FROM executions"
                    ).fetchall()
                    for execution_id, materialized_at in rows:
                        materialization_id = "MAT-" + hashlib.sha256(
                            str(execution_id).encode("utf-8")
                        ).hexdigest()[:24].upper()
                        payload = {
                            "materialization_id": materialization_id,
                            "execution_id": execution_id,
                            "created_at": materialized_at,
                            "migrated_from_schema": 1,
                        }
                        connection.execute(
                            "INSERT OR IGNORE INTO materializations VALUES(?,?,?,?)",
                            (materialization_id, execution_id, materialized_at, canonical(payload)),
                        )
                    for row in connection.execute(
                        "SELECT execution_id,phase FROM executions"
                    ).fetchall():
                        state = "ACTIVE" if row["phase"] == "RUNNING" else (
                            "SUSPENDED" if row["phase"] == "MATERIALIZED" else "RELEASED"
                        )
                        reason = "MIGRATED_RUNNING" if state == "ACTIVE" else (
                            "MATERIALIZED" if state == "SUSPENDED" else "TERMINAL"
                        )
                        updated = now()
                        authority = {
                            "execution_id": row["execution_id"], "state": state,
                            "revision": 0, "reason": reason, "updated_at": updated,
                        }
                        connection.execute(
                            "INSERT OR IGNORE INTO execution_authority VALUES(?,?,?,?,?,?)",
                            (row["execution_id"], state, 0, reason, updated, canonical(authority)),
                        )
                    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                    connection.commit()
                    connection.execute("PRAGMA foreign_keys=ON")
        stat = self.path.stat()
        self._database_file_identity = (stat.st_dev, stat.st_ino)
        with self.hold():
            if self._connection.execute("PRAGMA user_version").fetchone()[0] not in {SCHEMA_VERSION, 6}:
                raise RepositoryError("CONTROL_SCHEMA_UNSUPPORTED")
            required = {
                'control_state','logical_jobs','executions','attempts','materializations',
                'attempt_outcomes','native_sessions','runtime_processes','commands',
                'native_turns','runtime_recoveries','decisions','validation_dispositions',
                'execution_authority','inbound_events','verification_evidence','product_readiness','dependencies','conflicts','ownership','notifications',
            }
            tables = {row[0] for row in self._connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not required <= tables:
                raise RepositoryError("CONTROL_SCHEMA_INCOMPLETE")

    @property
    def _connection(self):
        return self._local.connection

    @contextmanager
    def hold(self):
        """One transaction spans each existing JobStore operation, including nested reads."""
        with self._guard:
            if getattr(self._local, "connection", None) is not None:
                yield
                return
            mode = "ro" if self.read_only else "rw"
            connection = sqlite3.connect(self.path.as_uri() + "?mode=" + mode,
                                         uri=True, timeout=10, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA synchronous=FULL")
            self._local.connection = connection
            try:
                connection.execute("BEGIN" if self.read_only else "BEGIN IMMEDIATE")
                stat = self.path.stat()
                if self._database_file_identity != (stat.st_dev, stat.st_ino):
                    raise RepositoryError("CONTROL_DATABASE_REPLACED")
                from control_epoch import current as current_epoch
                epoch = current_epoch(connection)
                if self._bound_control_epoch is None:
                    self._bound_control_epoch = epoch
                elif self._bound_control_epoch != epoch:
                    raise RepositoryError("CONTROL_RESTORE_EPOCH_STALE")
                yield
                stat = self.path.stat()
                if self._database_file_identity != (stat.st_dev, stat.st_ino):
                    raise RepositoryError("CONTROL_DATABASE_REPLACED")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                self._local.connection = None
                connection.close()

    def health(self):
        with self.hold():
            check = self._connection.execute("PRAGMA integrity_check").fetchall()
            fk = self._connection.execute("PRAGMA foreign_key_check").fetchall()
            valid = [row[0] for row in check] == ["ok"] and not fk
            return {"status": "VALID" if valid else "INVALID", "schema_version": self._connection.execute("PRAGMA user_version").fetchone()[0],
                    "authority": "SQLITE", "path": str(self.path), "integrity_check": [row[0] for row in check],
                    "foreign_key_errors": len(fk)}

    def record_metric_receipt(self, attempt_id, *, receipt_id, receipt_sha256, values):
        from operating_observations import record_usage
        return record_usage(self, attempt_id, receipt_id=receipt_id,
                            receipt_sha256=receipt_sha256, values=values)

    def observed_operating_metrics(self):
        from operating_observations import collect
        from operating_metrics import aggregate_metrics
        observed = collect(self)
        return {**observed, "metrics": aggregate_metrics(observed["observations"],
                                                        coverage=observed["coverage"])}

    def read_queue(self):
        with self.hold():
            row = self._connection.execute("SELECT payload FROM control_state WHERE key='session'").fetchone()
            if row is None:
                raise RepositoryError("CONTROL_SESSION_MISSING")
            return json.loads(row[0])

    def global_stop(self):
        with self.hold():
            row = self._connection.execute("SELECT payload,revision FROM control_state WHERE key='global_stop'").fetchone()
            return {**json.loads(row[0]),'revision':row[1]} if row else {"active": False,'revision':0}

    def notification(self, channel, subject):
        with self.hold():
            row = self._connection.execute("SELECT * FROM notifications WHERE channel=? AND subject=?", (channel,subject)).fetchone()
            return dict(row) if row else None

    def record_notification(self, channel, subject, fingerprint, payload):
        with self.hold():
            self._connection.execute("INSERT INTO notifications VALUES(?,?,?,?,?) ON CONFLICT(channel,subject) DO UPDATE SET fingerprint=excluded.fingerprint,delivered_at=excluded.delivered_at,payload=excluded.payload",
                                     (channel,subject,fingerprint,now(),canonical(payload)))

    def import_legacy_notifications(self):
        from operator_notifications import terminal_projection, semantic_fingerprint
        with self.hold():
            queue = self.read_queue()
            imported = []
            for row in self.worklist():
                job = self.read_job(row['job_id'])
                code = str(dict(job.get('last_result') or {}).get('failure_code') or '')
                delivered = [entry for entry in (job.get('terminal_notifications') or {}).values()
                    if dict(entry.get('evidence') or {}).get('delivered') is True
                    and (not code or code in str(dict(entry.get('projection') or {}).get('message','')))]
                subject = job['job_id'] + ':TERMINAL'
                if delivered and self.notification('telegram', subject) is None:
                    payload = terminal_projection(job, queue)
                    self.record_notification('telegram', subject, semantic_fingerprint(payload), payload)
                    imported.append(job['job_id'])
            return imported

    def preserve_candidate_ownership(self, job_id, files, *, workspace, evidence_ref):
        with self.hold():
            job = self.read_job(job_id)
            if job.get('status') != 'AWAITING_QA':
                raise RepositoryError('CANDIDATE_OWNERSHIP_STATUS_INVALID')
            root = Path(workspace).resolve()
            for name, expected in files.items():
                path = (root / name).resolve()
                if not path.is_relative_to(root) or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                    raise RepositoryError('CANDIDATE_OWNERSHIP_INTEGRITY_FAILED')
                self._connection.execute('INSERT INTO ownership VALUES(?,?,?,?,?)',
                    (name,job_id,expected,'PRE_EXISTING_UNVERIFIED_CANDIDATE',str(evidence_ref)))

    def candidate_ownership(self, workspace, *, accepted_baseline_hashes=None, current_only=False):
        """Current workspace candidate custody.

        ``current_only=True`` answers only current-custody questions (active
        unresolved ownership, exact hash integrity, accepted current baseline
        input) and skips both historical forensic scans: the verified
        candidate-history reconstruction and the successful-Job hash scan.
        Those remain available through the default mode for incident/operator
        forensic paths that must prove past candidate provenance.
        """
        root = Path(workspace).resolve()
        accepted = {
            str(path): str(value).casefold()
            for path, value in dict(accepted_baseline_hashes or {}).items()
        }
        with self.hold():
            rows = [dict(row) for row in self._connection.execute('SELECT * FROM ownership ORDER BY path')]
            historical = {} if current_only else self._verified_candidate_history(rows)
            successful_hashes = {}
            if not current_only:
                baseline = self.read_queue().get('active_baseline_id')
                successes = [json.loads(r[0]) for r in self._connection.execute(
                    "SELECT payload FROM logical_jobs WHERE status IN ('SUCCEEDED','SKELETON_READY')")]
                for job in sorted(successes, key=lambda j:j.get('updated_at', '')):
                    result = job.get('last_result') or {}
                    from skeleton_policy import valid_result
                    if (job.get('active_baseline_id') == baseline and (
                            (result.get('success') is True and result.get('verification_status') == 'VERIFIED')
                            or valid_result(result))):
                        successful_hashes.update(result.get('changed_file_sha256') or {})
        invalid = []
        active = []
        accepted_history = []
        for row in rows:
            path = (root / row['path']).resolve()
            actual_hash = (
                hashlib.sha256(path.read_bytes()).hexdigest()
                if path.is_relative_to(root) and path.is_file()
                else ""
            )
            if accepted.get(row['path']) == actual_hash.casefold():
                accepted_history.append(row)
                continue
            if row['path'] in historical:
                if (not historical[row['path']] or not path.is_relative_to(root)
                        or not path.is_file() or actual_hash
                        != successful_hashes.get(row['path'], row['candidate_hash'])):
                    invalid.append(row['path'])
                continue
            active.append(row)
            if not path.is_relative_to(root) or not path.is_file() or actual_hash != row['candidate_hash']:
                invalid.append(row['path'])
        return {'valid': not invalid, 'invalid_files': invalid, 'files': active,
                'historical_files': [r for r in rows if r['path'] in historical] + accepted_history,
                'accepted_baseline_files': accepted_history,
                'historical_artifact_integrity': all(historical.values())}

    def _verified_candidate_history(self, rows, *, target_job_id=None):
        """Read-only historical artifact proof; never promote candidate evidence."""
        historical = {}
        queue = self.read_queue()
        for stored in self._connection.execute("SELECT payload FROM logical_jobs WHERE status='SUCCEEDED'"):
            target = json.loads(stored[0])
            if target_job_id is not None and target['job_id'] != target_job_id:
                continue
            event = target.get('technical_candidate_input') or {}
            source_id = event.get('source_job_id')
            owned = {r['path']:r['candidate_hash'] for r in rows if r['job_id'] == source_id}
            last = target.get('last_result') or {}
            if not owned or last.get('success') is not True or last.get('verification_status') != 'VERIFIED':
                continue
            source = self.read_job(source_id)
            fresh = target.get('fresh_input') or {}
            if (source.get('revision') != event.get('source_revision')
                    or target.get('corrects_job_id') != source_id
                    or event.get('target_job_id') != target['job_id']
                    or event.get('classification') != 'PRE_EXISTING_UNVERIFIED_CANDIDATE'
                    or event.get('candidate_hashes') != owned
                    or fresh.get('parent_job_id') != source_id
                    or fresh.get('parent_revision') != source.get('revision')
                    or target.get('active_baseline_id') != queue.get('active_baseline_id')):
                continue
            bound = False
            for execution in self._connection.execute(
                    "SELECT payload,result FROM executions WHERE job_id=? AND phase='TERMINAL'", (target['job_id'],)):
                payload, result = json.loads(execution[0]), json.loads(execution[1] or '{}')
                if (payload.get('technical_candidate_input') == event
                        and result.get('task_id') == last.get('task_id') and last.get('task_id')
                        and result.get('success') is True and result.get('verification_status') == 'VERIFIED'):
                    bound = True
            if not bound:
                continue
            valid = False
            try:
                path = Path(str(fresh.get('manifest_path', ''))).resolve()
                if path.is_relative_to((self.path.parent/'cumulative/job-snapshots').resolve()):
                    data = path.read_bytes()
                    snapshot = json.loads(data)
                    valid = (hashlib.sha256(data).hexdigest() == fresh.get('manifest_sha256')
                        and snapshot.get('job_id') == source_id
                        and snapshot.get('snapshot_id') == fresh.get('snapshot_id')
                        and snapshot.get('baseline_id') == target.get('active_baseline_id'))
                    entries = {e['path']:e for e in snapshot.get('files', [])}
                    for name, expected in owned.items():
                        entry = entries.get(name, {})
                        blob = (path.parent/str(entry.get('snapshot', ''))).resolve()
                        valid = bool(valid and entry.get('state') == 'PRESENT'
                            and entry.get('sha256') == expected and blob.is_relative_to(path.parent)
                            and blob.is_file() and hashlib.sha256(blob.read_bytes()).hexdigest() == expected)
            except (OSError, ValueError, KeyError, TypeError):
                valid = False
            historical.update({name:valid for name in owned})
        return historical

    def preserved_candidate_records(self, job_id):
        with self.hold():
            return [dict(row) for row in self._connection.execute(
                'SELECT * FROM ownership WHERE job_id=? ORDER BY path', (job_id,))]

    def candidate_handoff_allows(self, source_job_id, target_job_id):
        """Only a committed handoff exempts its recipient from source quarantine."""
        with self.hold():
            source = self.read_job(source_job_id)
            record = source.get('candidate_ownership_handoff') or {}
            if not record or record.get('target_job_id') != target_job_id:
                return False
            target = self.read_job(target_job_id)
            rows = self.preserved_candidate_records(target_job_id)
            return (target.get('status') in {'QUEUED', 'RUNNING'}
                    and target.get('candidate_ownership_received') == record
                    and target.get('corrects_job_id') == source_job_id
                    and {r['path']: r['candidate_hash'] for r in rows} == record.get('candidate_hashes'))

    def technical_candidate_input_allows(self, source_job_id, target_job_id):
        """An immutable source can be referenced, never reassigned or verified."""
        with self.hold():
            source, target = self.read_job(source_job_id), self.read_job(target_job_id)
            event = target.get('technical_candidate_input') or {}
            rows = self.preserved_candidate_records(source_job_id)
            return bool(event and source.get('status') == 'FAILED_FINAL'
                and source.get('revision') == event.get('source_revision')
                and target.get('status') in {'QUEUED','RUNNING'}
                and target.get('corrects_job_id') == source_job_id
                and event.get('target_job_id') == target_job_id
                and event.get('source_job_id') == source_job_id
                and event.get('classification') == 'PRE_EXISTING_UNVERIFIED_CANDIDATE'
                and {r['path']:r['candidate_hash'] for r in rows} == event.get('candidate_hashes'))

    def _preview_technical_candidate_input(self, source, target_job_id, *, workspace,
            integrity_snapshot_provider, execution_safe, expected_source_revision=None,
            expected_target_revision=None, candidate_scope=None, expected_candidate_hash=None):
        """Narrow FAILED_FINAL/no-delta lineage; source and ownership are read-only."""
        from qa_quarantine import paths_overlap
        target = self.read_job(target_job_id) if target_job_id else None
        queue = self.read_queue()
        ownership = self.candidate_ownership(workspace)
        rows = [r for r in ownership['files'] if r['job_id'] == source['job_id']]
        hashes = {r['path']:r['candidate_hash'] for r in rows}
        scope = sorted(hashes)
        conflicts = []
        last = source.get('last_result') or {}
        origin, code = last.get('failure_origin',''), last.get('failure_code','')
        evidence_ref = ''
        # Older IPC incidents were misclassified. Only bound runtime stderr,
        # not a Worker narrative or an operator-provided label, corrects routing.
        technical = origin in {'CONTROL','INFRASTRUCTURE','HARNESS_CAUSED','WORKER_RUNTIME'}
        technical = technical or code in {'PROFILE_DRIFT','WORKER_IPC_DECODE_ERROR','CODEX_EXECUTION_SURFACE_MISMATCH'}
        if not technical and code == 'NO_MEANINGFUL_SOURCE_CHANGE':
            path = Path(str(last.get('task_state_path',''))).resolve()
            task_root = self.path.parent.parent / '.tasks'
            if path.is_relative_to(task_root.resolve()) and path.is_file():
                data = path.read_bytes()
                task = json.loads(data)
                if (task.get('job_id') == source['job_id'] and task.get('task_id') == last.get('task_id')
                        and task.get('actual_invocation_id') == last.get('actual_invocation_id')
                        and 'failed to decode code-mode IPC frame' in task.get('worker_stderr','')):
                    technical = True
                    origin, code = 'HARNESS_CAUSED', 'WORKER_IPC_DECODE_ERROR'
                    evidence_ref = {'path':str(path),'sha256':hashlib.sha256(data).hexdigest()}
        if not technical:
            conflicts.append('CANDIDATE_FAILURE_NOT_TECHNICAL')
        if (last.get('review_status') in {'REVIEW_FAILED','REVIEW_FAIL','REJECTED'}
                or last.get('worktree_disposition') in {'ROLLED_BACK','DISCARDED','ROLLBACK'}
                or last.get('candidate_rejected') or last.get('semantic_rejection')
                or last.get('review_violation_codes')):
            conflicts.append('CANDIDATE_PRODUCT_REJECTED')
        if not scope or any(r['classification'] != 'PRE_EXISTING_UNVERIFIED_CANDIDATE' for r in rows):
            conflicts.append('CANDIDATE_DISPOSITION_INELIGIBLE')
        if not ownership['valid']:
            conflicts.append('CANDIDATE_OWNERSHIP_INTEGRITY_FAILED')
        if candidate_scope is not None and sorted(candidate_scope) != scope:
            conflicts.append('CANDIDATE_SCOPE_MISMATCH')
        if expected_candidate_hash is not None and expected_candidate_hash != digest(hashes):
            conflicts.append('CANDIDATE_HASH_MISMATCH')
        prior = (target or {}).get('technical_candidate_input') or {}
        replayed = bool(prior and prior.get('source_job_id') == source['job_id'])
        if (expected_source_revision is not None and expected_source_revision != source['revision']
                or not replayed and target and expected_target_revision is not None and expected_target_revision != target['revision']):
            conflicts.append('CONTROL_REVISION_CONFLICT')
        fresh = (target or {}).get('fresh_input') or {}
        anchor = (queue.get('replacement_anchors') or {}).get(source['job_id']) or {}
        if (not target or target.get('corrects_job_id') != source['job_id']
                or anchor.get('replacement_job_id') != target_job_id
                or fresh.get('parent_job_id') != source['job_id']
                or fresh.get('parent_revision') != source['revision']
                or fresh.get('kind') != 'VERIFIED_NO_TASK_DELTA_FRESH_INPUT'):
            conflicts.append('TARGET_NOT_LINKED_CORRECTIVE')
        if target and (target.get('status') != 'QUEUED' or target.get('owner_id')
                or target.get('outer_attempt') or target.get('task_ids') or target.get('last_result')):
            conflicts.append('CANDIDATE_TARGET_ALREADY_STARTED')
        resources = (target or {}).get('request',{}).get('target_resources') or []
        source_scope = source.get('request',{}).get('target_resources') or []
        if not scope or not set(scope).issubset(resources) or not set(scope).issubset(source_scope):
            conflicts.append('QUARANTINED_CANDIDATE_SCOPE_CONFLICT')
        if any(r['job_id'] != source['job_id'] and paths_overlap([r['path']], resources)
               for r in ownership['files']):
            conflicts.append('QUARANTINED_CANDIDATE_SCOPE_CONFLICT')
        if queue.get('running_job_id') or not execution_safe() or source.get('owner_id'):
            conflicts.append('ACTIVE_EXECUTION')
        integrity = integrity_snapshot_provider(queue)
        baseline = queue.get('active_baseline_id')
        if (not baseline or source.get('active_baseline_id') != baseline
                or fresh.get('active_baseline_id') != baseline
                or integrity.get('active_baseline_id') != baseline
                or integrity.get('baseline_declaration_integrity') is not True
                or integrity.get('external_frozen_integrity') is not True
                or integrity.get('unexpected_runtime_dirty_files') != []):
            conflicts.append('CANDIDATE_HANDOFF_INTEGRITY_INVALID')
        return {'eligible':not conflicts,'conflicts':conflicts,'replayed':replayed,
            'source':{'job_id':source['job_id'],'revision':source['revision'],'status':source['status']},
            'target':({'job_id':target_job_id,'revision':target['revision'],'status':target['status']} if target else None),
            'candidate_scope':scope,'candidate_hashes':hashes,'expected_candidate_hash':digest(hashes),
            'expected_source_revision':source['revision'],'expected_target_revision':target['revision'] if target else None,
            'confirmation_required':CANDIDATE_HANDOFF_CONFIRMATION,'baseline_id':baseline,
            'source_failure_origin':origin,'source_failure_code':code,'technical_evidence':evidence_ref,
            'ownership':ownership['files'],'candidate_integrity':ownership['valid'],'dispatched':False}

    def preview_candidate_ownership_handoff(
        self, source_job_id, target_job_id='', *, workspace,
        integrity_snapshot_provider, execution_safe,
        expected_source_revision=None, expected_target_revision=None,
        candidate_scope=None, expected_candidate_hash=None,
    ):
        """Read-only eligibility computed under the same transaction as mutation."""
        from project_profile import normalize_relative_path, ProfileError
        from qa_quarantine import paths_overlap
        with self.hold():
            source = self.read_job(source_job_id)
            if source.get('status') == 'FAILED_FINAL':
                return self._preview_technical_candidate_input(source,target_job_id,
                    workspace=workspace,integrity_snapshot_provider=integrity_snapshot_provider,
                    execution_safe=execution_safe,expected_source_revision=expected_source_revision,
                    expected_target_revision=expected_target_revision,candidate_scope=candidate_scope,
                    expected_candidate_hash=expected_candidate_hash)
            prior = source.get('candidate_ownership_handoff') or {}
            ownership = self.candidate_ownership(workspace)
            rows = [r for r in ownership['files'] if r['job_id'] == source_job_id]
            hashes = dict(prior.get('candidate_hashes') or {r['path']:r['candidate_hash'] for r in rows})
            scope = sorted(hashes)
            scope_hash = digest(hashes)
            target = self.read_job(target_job_id) if target_job_id else None
            conflicts = []
            def fail(code):
                if code not in conflicts:
                    conflicts.append(code)
            if not target:
                fail('LINKED_CORRECTIVE_TARGET_REQUIRED')
            if not scope or any(r['classification'] != 'PRE_EXISTING_UNVERIFIED_CANDIDATE' for r in rows):
                fail('CANDIDATE_DISPOSITION_INELIGIBLE')
            if source.get('status') != 'AWAITING_QA' or source.get('owner_id'):
                fail('CANDIDATE_SOURCE_NOT_HISTORICAL')
            if self._connection.execute("SELECT 1 FROM executions WHERE job_id=? AND phase!='TERMINAL'", (source_job_id,)).fetchone():
                fail('CANDIDATE_SOURCE_NOT_HISTORICAL')
            if not self.has_job(source_job_id):
                fail('SQLITE_SOURCE_JOB_REQUIRED')
            if not ownership['valid']:
                fail('CANDIDATE_OWNERSHIP_INTEGRITY_FAILED')
            if candidate_scope is not None:
                try:
                    supplied = [normalize_relative_path(p) for p in candidate_scope] if isinstance(candidate_scope,list) else []
                    if sorted(supplied) != scope or len(set(supplied)) != len(supplied):
                        fail('CANDIDATE_SCOPE_MISMATCH')
                except ProfileError:
                    fail('CANDIDATE_SCOPE_MISMATCH')
            if expected_candidate_hash is not None and expected_candidate_hash != scope_hash:
                fail('CANDIDATE_HASH_MISMATCH')
            replayed = bool(prior and prior.get('target_job_id') == target_job_id)
            if prior and not replayed:
                fail('CANDIDATE_ALREADY_HANDED_OFF')
            revisions = {
                'source': expected_source_revision is None or (type(expected_source_revision) is int and expected_source_revision == source['revision']),
                'target': bool(target) and (expected_target_revision is None or (type(expected_target_revision) is int and expected_target_revision == target['revision'])),
            }
            if not replayed and (not revisions['source'] or (target and not revisions['target'])):
                fail('CONTROL_REVISION_CONFLICT')
            queue = self.read_queue()
            link = dict(queue.get('qa_corrective_successors') or {}).get(source_job_id) or {}
            if target:
                if (target_job_id == source_job_id or target.get('corrects_job_id') != source_job_id
                        or link.get('corrective_job_id') != target_job_id):
                    fail('TARGET_NOT_LINKED_CORRECTIVE')
                if not self.has_job(target_job_id):
                    fail('SQLITE_TARGET_JOB_REQUIRED')
                target_rows = self.preserved_candidate_records(target_job_id)
                if replayed:
                    if (target.get('candidate_ownership_received') != prior
                            or {r['path']:r['candidate_hash'] for r in target_rows} != hashes):
                        fail('CANDIDATE_HANDOFF_PROVENANCE_MISMATCH')
                else:
                    if (target.get('status') != 'QUEUED' or target.get('owner_id')
                            or target.get('outer_attempt',0) or target.get('attempts')
                            or target.get('task_ids') or target.get('last_result')
                            or target.get('worker_invocation_count',0) or target.get('current_claim_task_started')
                            or self._connection.execute('SELECT 1 FROM executions WHERE job_id=?', (target_job_id,)).fetchone()):
                        fail('CANDIDATE_TARGET_ALREADY_STARTED')
                    if target_rows or target.get('candidate_id') or target.get('candidate_ownership_received'):
                        fail('CANDIDATE_TARGET_ALREADY_OWNS_CANDIDATE')
                resources = target.get('request',{}).get('target_resources') or target.get('request',{}).get('target_modules') or []
                if not resources or any(not any(p.casefold()==str(r).casefold() or p.casefold().startswith(str(r).casefold().rstrip('/')+'/') for r in resources) for p in scope):
                    fail('CANDIDATE_OUTSIDE_TARGET_SCOPE')
            for row in ownership['files']:
                if row['job_id'] not in {source_job_id,target_job_id} and scope and paths_overlap(
                        [row['path'].replace('\\','/').casefold()], [p.casefold() for p in scope]):
                    fail('THIRD_PARTY_CANDIDATE_SCOPE_CONFLICT')
            if (queue.get('running_job_id') or not execution_safe()
                    or self._connection.execute("SELECT 1 FROM executions WHERE phase='RUNNING'").fetchone()
                    or self._connection.execute("SELECT 1 FROM logical_jobs WHERE status='RUNNING' OR phase='RUNNING' OR owner_id!=''").fetchone()):
                fail('ACTIVE_EXECUTION')
            integrity = integrity_snapshot_provider(queue)
            baseline = str(queue.get('active_baseline_id') or '')
            if (not baseline or integrity.get('active_baseline_id') != baseline
                    or source.get('active_baseline_id') != baseline
                    or (target and target.get('active_baseline_id') != baseline)
                    or integrity.get('baseline_declaration_integrity') is not True
                    or integrity.get('external_frozen_integrity') is not True
                    or integrity.get('unexpected_runtime_dirty_files') != []):
                fail('CANDIDATE_HANDOFF_INTEGRITY_INVALID')
            return {'eligible':not conflicts, 'replayed':replayed,
                'source':{'job_id':source_job_id,'status':source['status'],'revision':source['revision']},
                'target':({'job_id':target_job_id,'status':target['status'],'revision':target['revision']} if target else None),
                'candidate_scope':scope,'candidate_hashes':hashes,'expected_candidate_hash':scope_hash,
                'candidate_integrity':ownership['valid'],'revision_compatibility':revisions,
                'expected_source_revision':source['revision'],'expected_target_revision':target['revision'] if target else None,
                'baseline_id':baseline,'conflicts':conflicts,'ownership':ownership['files'],
                'expected_resulting_ownership':{'job_id':target_job_id,'candidate_scope':scope},
                'confirmation_required':CANDIDATE_HANDOFF_CONFIRMATION,
                'target_requirements':{'status':'QUEUED','worker_started':False,
                    'link_operation':'record_qa_corrective_successor','corrects_job_id':source_job_id,
                    'same_active_baseline':True,'must_cover_candidate_scope':True},
                'linked_target_job_id':link.get('corrective_job_id',''), 'dispatched':False}

    def handoff_candidate_ownership(
        self, source_job_id, target_job_id, *, expected_source_revision,
        expected_target_revision, candidate_scope, expected_candidate_hash,
        confirmation, request_id, workspace, integrity_snapshot_provider, execution_safe,
    ):
        """CAS both Jobs and ownership/provenance in one BEGIN IMMEDIATE commit."""
        from job_contract import validate_idempotency_key
        if self.read_only:
            raise RepositoryError('CONTROL_REPOSITORY_READ_ONLY')
        if confirmation != CANDIDATE_HANDOFF_CONFIRMATION:
            raise RepositoryError('CANDIDATE_HANDOFF_CONFIRMATION_REQUIRED')
        if (type(expected_source_revision) is not int or type(expected_target_revision) is not int
                or not candidate_scope or not expected_candidate_hash or not target_job_id):
            raise RepositoryError('CANDIDATE_HANDOFF_INPUT_REQUIRED')
        request_ref = digest(validate_idempotency_key(request_id, 'request_id'))
        with self.hold():
            preview = self.preview_candidate_ownership_handoff(source_job_id, target_job_id,
                workspace=workspace, integrity_snapshot_provider=integrity_snapshot_provider, execution_safe=execution_safe,
                expected_source_revision=expected_source_revision, expected_target_revision=expected_target_revision,
                candidate_scope=candidate_scope, expected_candidate_hash=expected_candidate_hash)
            if not preview['eligible']:
                raise RepositoryError(preview['conflicts'][0])
            source, target = self.read_job(source_job_id), self.read_job(target_job_id)
            if source.get('status') == 'FAILED_FINAL':
                if preview['replayed']:
                    return {'replayed':True,'event':target['technical_candidate_input'],'dispatched':False}
                event = {'event':'TECHNICAL_CANDIDATE_INPUT_HANDED_OFF','handoff_at':now(),
                    'source_job_id':source_job_id,'source_revision':source['revision'],
                    'source_execution_id':source.get('last_result',{}).get('task_id',''),
                    'source_candidate_id':expected_candidate_hash,'source_candidate_hash':expected_candidate_hash,
                    'source_candidate_scope':preview['candidate_scope'],'candidate_hashes':preview['candidate_hashes'],
                    'source_failure_origin':preview['source_failure_origin'],'source_failure_code':preview['source_failure_code'],
                    'technical_evidence':preview['technical_evidence'],
                    'target_logical_job_id':target.get('logical_job_id') or target.get('client_job_id'),
                    'target_job_id':target_job_id,'target_execution_id':'EXEC-'+uuid.uuid4().hex.upper(),
                    'classification':'PRE_EXISTING_UNVERIFIED_CANDIDATE',
                    'handoff_reason':'EXPLICIT_TECHNICAL_NO_DELTA_FRESH_INPUT','request_ref':request_ref}
                target['technical_candidate_input'] = event
                target.setdefault('history',[]).append(copy.deepcopy(event))
                target['revision'] = expected_target_revision+1
                self.write_job(target,expected_revision=expected_target_revision)
                return {'replayed':False,'event':event,'dispatched':False}
            if preview['replayed']:
                return {'replayed':True,'event':source['candidate_ownership_handoff'],'dispatched':False}
            at = now()
            event = {'event':'CANDIDATE_OWNERSHIP_HANDED_OFF','at':at,
                'source_job_id':source_job_id,'target_job_id':target_job_id,
                'candidate_scope':preview['candidate_scope'],'candidate_hashes':preview['candidate_hashes'],
                'candidate_scope_sha256':expected_candidate_hash,'baseline_id':preview['baseline_id'],
                'source_revision_before':expected_source_revision,'source_revision_after':expected_source_revision+1,
                'target_revision_before':expected_target_revision,'target_revision_after':expected_target_revision+1,
                'request_ref':request_ref,'confirmation':confirmation}
            source['candidate_ownership_handoff'] = copy.deepcopy(event)
            target['candidate_ownership_received'] = copy.deepcopy(event)
            for job, revision in ((source,expected_source_revision),(target,expected_target_revision)):
                job.setdefault('history',[]).append(copy.deepcopy(event))
                job['revision'] = revision+1
                job['updated_at'] = at
                self.write_job(job,expected_revision=revision)
            cursor = self._connection.execute('UPDATE ownership SET job_id=? WHERE job_id=?', (target_job_id,source_job_id))
            if cursor.rowcount != len(candidate_scope):
                raise RepositoryError('CONTROL_REVISION_CONFLICT')
            return {'replayed':False,'event':event,'dispatched':False}

    def bind_corrective_baseline(self, source_job_id, target_job_id, *,
                                expected_source_revision, expected_target_revision,
                                integrity_snapshot_provider, execution_safe, preview=False):
        with self.hold():
            source, target = self.read_job(source_job_id), self.read_job(target_job_id)
            queue = self.read_queue()
            baseline = source.get('active_baseline_id')
            link = (queue.get('qa_corrective_successors') or {}).get(source_job_id) or {}
            if (source_job_id == target_job_id or target.get('corrects_job_id') != source_job_id
                    or link.get('corrective_job_id') != target_job_id):
                raise RepositoryError('TARGET_NOT_LINKED_CORRECTIVE')
            if (type(expected_source_revision) is not int or type(expected_target_revision) is not int
                    or source['revision'] != expected_source_revision or target['revision'] != expected_target_revision):
                raise RepositoryError('CONTROL_REVISION_CONFLICT')
            if (target.get('status') != 'QUEUED' or target.get('owner_id') or target.get('outer_attempt')
                    or target.get('attempts') or target.get('task_ids') or target.get('last_result')
                    or target.get('worker_invocation_count') or target.get('current_claim_task_started')
                    or self._connection.execute('SELECT 1 FROM executions WHERE job_id=?', (target_job_id,)).fetchone()):
                raise RepositoryError('CANDIDATE_TARGET_ALREADY_STARTED')
            if queue.get('running_job_id') or not execution_safe():
                raise RepositoryError('ACTIVE_EXECUTION')
            integrity = integrity_snapshot_provider(queue)
            if (not baseline or baseline != queue.get('active_baseline_id')
                    or baseline != integrity.get('active_baseline_id')
                    or integrity.get('baseline_declaration_integrity') is not True
                    or integrity.get('external_frozen_integrity') is not True
                    or integrity.get('unexpected_runtime_dirty_files') != []):
                raise RepositoryError('CORRECTIVE_BASELINE_INTEGRITY_INVALID')
            current = target.get('active_baseline_id')
            if current and current != baseline:
                raise RepositoryError('CORRECTIVE_BASELINE_CONFLICT')
            result = {'eligible': True, 'replayed': current == baseline, 'source_job_id': source_job_id,
                      'target_job_id': target_job_id, 'baseline_id': baseline,
                      'expected_source_revision': source['revision'],
                      'expected_target_revision': target['revision'], 'dispatched': False}
            if preview or current == baseline:
                return result
            if self.read_only:
                raise RepositoryError('CONTROL_REPOSITORY_READ_ONLY')
            at = now()
            event = {'event': 'CORRECTIVE_BASELINE_BOUND', 'at': at, 'source_job_id': source_job_id,
                     'target_job_id': target_job_id, 'baseline_id': baseline,
                     'source_revision': source['revision'], 'target_revision_before': target['revision'],
                     'target_revision_after': target['revision'] + 1}
            target['active_baseline_id'] = baseline
            target['corrective_baseline_binding'] = event
            target.setdefault('history', []).append(copy.deepcopy(event))
            target['revision'] += 1
            target['updated_at'] = at
            self.write_job(target, expected_revision=expected_target_revision)
            return {**result, 'event': event}

    def set_global_stop(self, reason):
        with self.hold():
            value = {"active": True, "disposition": "GLOBAL_STOP", "reason": str(reason), "set_at": now()}
            self._connection.execute("INSERT INTO control_state VALUES('global_stop',1,?) ON CONFLICT(key) DO UPDATE SET revision=revision+1,payload=excluded.payload", (canonical(value),))
            return value

    def release_global_stop(self, *, expected_revision, reason):
        with self.hold():
            current = self.global_stop()
            if current['revision'] != expected_revision or not current.get('active'):
                raise RepositoryError('CONTROL_REVISION_CONFLICT')
            if self.read_queue().get('running_job_id') or self._connection.execute("SELECT 1 FROM executions WHERE phase='RUNNING'").fetchone():
                raise RepositoryError('ACTIVE_EXECUTION')
            value = {'active':False,'released_at':now(),'reason':str(reason)}
            self._connection.execute("UPDATE control_state SET revision=revision+1,payload=? WHERE key='global_stop' AND revision=?",(canonical(value),expected_revision))
            return self.global_stop()

    def write_queue(self, queue, *, expected_revision):
        with self.hold():
            cursor = self._connection.execute(
                "UPDATE control_state SET revision=?, payload=? WHERE key='session' AND revision=?",
                (queue["revision"], canonical(queue), expected_revision))
            if cursor.rowcount != 1:
                raise RepositoryError("CONTROL_REVISION_CONFLICT")

    def operator_reanchor(self, *, expected_revision, request_id, acknowledgement,
                          workspace, profile_id, prepare, verify):
        """Explicit epoch boundary. Receipt, current custody and session commit atomically.

        Preparation writes immutable baseline artifacts only. An aborted transaction
        may leave an unreferenced artifact, never a partly activated generation.
        """
        if self.read_only:
            raise RepositoryError("CONTROL_REPOSITORY_READ_ONLY")
        if acknowledgement != f"REANCHOR {profile_id.upper()}":
            raise RepositoryError("REANCHOR_ACKNOWLEDGEMENT_REQUIRED")
        if not request_id or len(request_id) > 128 or type(expected_revision) is not int:
            raise RepositoryError("REANCHOR_REQUEST_INVALID")
        key = "operator_reanchor:" + digest(request_id)
        identity = {"expected_revision": expected_revision, "workspace": str(Path(workspace).resolve()),
                    "profile_id": profile_id, "acknowledgement": acknowledgement}
        with self.hold():
            prior = self._connection.execute("SELECT payload FROM control_state WHERE key=?", (key,)).fetchone()
            if prior:
                receipt = json.loads(prior[0])
                if receipt["request"] != identity:
                    raise RepositoryError("REANCHOR_IDEMPOTENCY_CONFLICT")
                return receipt, True
            queue = self.read_queue()
            if queue["revision"] != expected_revision:
                raise RepositoryError("CONTROL_REVISION_CONFLICT")
            if self.health()["status"] != "VALID":
                raise RepositoryError("REANCHOR_CONTROL_INVALID")
            if (queue.get("running_job_id") or queue.get("gate_reason") or queue.get("blocked_by_job_id")
                    or queue.get("paused") or self.global_stop().get("active")):
                raise RepositoryError("REANCHOR_QUEUE_NOT_IDLE")
            if self._connection.execute("SELECT 1 FROM executions WHERE phase!='TERMINAL' LIMIT 1").fetchone():
                raise RepositoryError("REANCHOR_ACTIVE_EXECUTION")
            if self._connection.execute("SELECT 1 FROM logical_jobs WHERE status='RUNNING' OR owner_id!='' LIMIT 1").fetchone():
                raise RepositoryError("REANCHOR_ACTIVE_WRITER")
            if self._connection.execute("SELECT 1 FROM execution_authority WHERE state!='RELEASED' LIMIT 1").fetchone():
                raise RepositoryError("REANCHOR_EXECUTION_AUTHORITY_HELD")
            for job_id in queue.get("order", [])[int(queue.get("active_from_index", 0)):]:
                job = self.read_job(job_id)
                if job.get("status") in {"QUEUED", "READY", "HOLD", "AWAITING_ENRICHMENT", "BLOCKED", "INTERRUPTED"}:
                    raise RepositoryError("REANCHOR_PENDING_JOB")
            before = copy.deepcopy(queue)
            ownership = [dict(r) for r in self._connection.execute("SELECT * FROM ownership ORDER BY path")]
            for row in ownership:
                owner = self.read_job(row['job_id'])
                if owner.get('owner_id') or owner.get('status') not in {'FAILED_FINAL','SUCCEEDED','SKIPPED','CANCELLED'}:
                    raise RepositoryError('REANCHOR_CANDIDATE_CUSTODY_UNRESOLVED')
            verify()
            generation = int(queue["generation"]) + 1
            fields = prepare(generation, str(queue.get("active_baseline_id", "")))
            if (not fields.get("active_baseline_id") or fields.get("commit_policy") != "BATCH_FINAL_COMMIT"
                    or any(fields.get(k) is not True for k in ("external_frozen_integrity",
                        "baseline_declaration_integrity", "repository_head_integrity",
                        "dirty_overlay_path_set_match", "dirty_overlay_hash_match"))
                    or fields.get("unexpected_runtime_dirty_files")):
                raise RepositoryError("REANCHOR_BASELINE_INVALID")
            preconditions = verify()
            queue.update(fields)
            queue.update({"generation": generation, "active_from_index": len(queue.get("order", [])),
                          "batches": {}, "execution_mode": "ATTENDED", "paused": False,
                          "running_job_id": "", "blocked_by_job_id": "", "gate_reason": "",
                          "operator_paused": False, "operator_pause_reason": "", "batch_delta_files": [],
                          "revision": expected_revision + 1, "updated_at": now()})
            for name in ("baseline_advance_eligible", "batch_final_commit_eligible", "batch_final_staging_eligible"):
                queue[name] = False
            for name in ("job_commit_deferred_count", "job_delta_preserved_count", "job_validation_success_count"):
                queue[name] = 0
            event = {"event": "OPERATOR_REANCHORED_CURRENT_WORKSPACE", "at": now(),
                     "receipt_key": key, "previous_generation": before["generation"],
                     "generation": generation, "baseline_id": fields["active_baseline_id"]}
            queue.setdefault("history", []).append(event)
            receipt = {"request": identity, **event, "previous_session": before,
                       "archived_ownership": ownership, "previous_baseline_id": before.get("active_baseline_id", ""),
                       "queue_revision": queue["revision"], "validation_boundary": "STARTING_BASELINE_ONLY",
                       "preconditions": preconditions,
                       "accepted_dirty_path_count": len(fields.get("task_owned_baseline_files", []))
                           + len(fields.get("external_frozen_baseline_files", []))}
            self._connection.execute("INSERT INTO control_state(key,revision,payload) VALUES(?,0,?)", (key, canonical(receipt)))
            # ownership is current custody; its exact prior rows now live in the
            # immutable receipt. Jobs/Attempts/Reviews remain untouched.
            self._connection.execute("DELETE FROM ownership")
            self.write_queue(queue, expected_revision=expected_revision)
            return receipt, False

    def has_job(self, job_id):
        with self.hold():
            return self._connection.execute("SELECT 1 FROM logical_jobs WHERE job_id=?", (job_id,)).fetchone() is not None

    def read_job(self, job_id):
        with self.hold():
            row = self._connection.execute("SELECT payload FROM logical_jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is not None:
                return json.loads(row[0])
            # These files are immutable legacy evidence, never active scheduling authority.
            refs = self._connection.execute("SELECT payload FROM control_state WHERE key='legacy_jobs'").fetchone()
            reference = json.loads(refs[0]).get(job_id) if refs else None
            if not reference:
                raise RepositoryError("JOB_NOT_FOUND")
            path = (self.legacy_root / reference["path"]).resolve()
            if not path.is_relative_to(self.legacy_root):
                raise RepositoryError("LEGACY_PATH_ESCAPE")
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != reference["sha256"]:
                raise RepositoryError("LEGACY_EVIDENCE_CHANGED")
            return json.loads(raw)

    @staticmethod
    def phase(job):
        return "PLANNED" if job.get("status") == "QUEUED" else "RUNNING" if job.get("status") == "RUNNING" else "TERMINAL"

    def _insert_job(self, job, legacy_ref=""):
        job_id = job["job_id"]
        logical_id = str(job.get("logical_job_id") or job.get("client_job_id") or job_id)
        self._connection.execute(
            "INSERT INTO logical_jobs(job_id,logical_job_id,status,revision,phase,contract_revision,owner_id,payload,legacy_job_ref) VALUES(?,?,?,?,?,?,?,?,?)",
            (job_id, logical_id, job["status"], int(job.get("revision", 0)), self.phase(job),
             int(job.get("contract_revision", 1)), job.get("owner_id", ""), canonical(job), legacy_ref))
        self._relations(job)

    def _relations(self, job):
        job_id = job["job_id"]
        self._connection.execute("DELETE FROM dependencies WHERE job_id=?", (job_id,))
        self._connection.execute("DELETE FROM conflicts WHERE job_id=?", (job_id,))
        for dependency in job.get("depends_on") or []:
            self._connection.execute("INSERT INTO dependencies VALUES(?,?,?)",
                                     (job_id, str(dependency), canonical({"kind": job.get("dependency_semantics", "FIFO")})))
        for resource in sorted(set(job.get("request", {}).get("target_resources") or job.get("request", {}).get("target_modules") or [])):
            self._connection.execute("INSERT INTO conflicts VALUES(?,?)", (job_id, resource))

    def write_job(self, job, *, expected_revision):
        with self.hold():
            previous = self._connection.execute("SELECT payload,phase FROM logical_jobs WHERE job_id=?", (job["job_id"],)).fetchone()
            if previous is None:
                if expected_revision != 0:
                    raise RepositoryError("LEGACY_JOB_IMMUTABLE")
                self._insert_job(job)
                return
            old = json.loads(previous[0])
            if previous[1] in {'MATERIALIZED','RUNNING'} and any(old.get(key)!=job.get(key) for key in ('request','current_requirement','contract_revision','execution_context')):
                raise RepositoryError("MATERIALIZED_CONTRACT_IMMUTABLE")
            if old.get("request") != job.get("request") and previous[1] not in {"PLANNED", "READY"}:
                raise RepositoryError("MATERIALIZED_CONTRACT_IMMUTABLE")
            cursor = self._connection.execute(
                "UPDATE logical_jobs SET status=?,revision=?,owner_id=?,payload=?,contract_revision=? WHERE job_id=? AND revision=?",
                (job["status"], job["revision"], job.get("owner_id", ""), canonical(job),
                 int(job.get("contract_revision", 1)), job["job_id"], expected_revision))
            if cursor.rowcount != 1:
                raise RepositoryError("CONTROL_REVISION_CONFLICT")
            self._relations(job)
            if job.get("status") == "RUNNING" and previous[1] == "PLANNED":
                self._connection.execute("UPDATE logical_jobs SET phase='READY' WHERE job_id=?", (job["job_id"],))
            if job.get("status") == "QUEUED" and previous[1] == "TERMINAL":
                self._connection.execute("UPDATE logical_jobs SET phase='PLANNED' WHERE job_id=?", (job["job_id"],))
            if job.get("status") not in {"QUEUED", "RUNNING"}:
                self._connection.execute("UPDATE logical_jobs SET phase='TERMINAL' WHERE job_id=?", (job["job_id"],))
                self._connection.execute("UPDATE executions SET phase='TERMINAL',result=? WHERE job_id=? AND phase!='TERMINAL'",
                                         (canonical(job.get("last_result") or {}), job["job_id"]))
                pending_decision = job.get("status") in {
                    "AWAITING_QA", "AWAITING_DEPENDENCY_QA", "SKELETON_READY"
                }
                for row in self._connection.execute(
                    "SELECT execution_id FROM executions WHERE job_id=?", (job["job_id"],)
                ).fetchall():
                    authority = self.writer_authority(row['execution_id'])
                    if pending_decision and authority and authority['state'] == 'RELEASED':
                        # Completed Product/Reviewer work never regains authority
                        # merely because the Job is projected into a QA hold.
                        continue
                    self._set_writer_authority_locked(
                        row["execution_id"],
                        "SUSPENDED" if pending_decision else "RELEASED",
                        "PENDING_DECISION" if pending_decision else "TERMINAL",
                    )
            self.project_product_readiness(job["job_id"])

    def write_scope_custody_recovery(self, job, *, expected_revision):
        """Allow only the audited terminal scope-custody transition to a fresh attempt."""
        with self.hold():
            row = self._connection.execute(
                "SELECT payload,phase FROM logical_jobs WHERE job_id=?", (job["job_id"],)
            ).fetchone()
            if row is None:
                raise RepositoryError("JOB_NOT_FOUND")
            old = json.loads(row[0])
            old_request = dict(old.get("request") or {})
            new_request = dict(job.get("request") or {})
            old_scope = set(old_request.get("target_resources") or [])
            new_scope = set(new_request.get("target_resources") or [])
            rejected = list(job.get("rejected_candidates") or [])
            route = dict(job.get("technical_retry_route") or {})
            immutable_request_changed = any(
                old_request.get(key) != new_request.get(key)
                for key in set(old_request) | set(new_request)
                if key != "target_resources"
            )
            invalid = (
                row[1] != "TERMINAL"
                or old.get("status") != "AWAITING_QA"
                or int(old.get("revision", -1)) != int(expected_revision)
                or job.get("status") != "QUEUED"
                or int(job.get("revision", -1)) != int(expected_revision) + 1
                or int(job.get("contract_revision", 0))
                   != int(old.get("contract_revision", 1)) + 1
                or immutable_request_changed
                or not old_scope < new_scope
                or str(old.get("candidate_id") or "") == ""
                or job.get("candidate_id") not in {"", None}
                or job.get("candidate_manifest") not in {"", None}
                or len(rejected) != len(list(old.get("rejected_candidates") or [])) + 1
                or rejected[-1].get("candidate_id") != old.get("candidate_id")
                or rejected[-1].get("disposition")
                   != "REJECTED_ISOLATED_CANDIDATE_PRESERVED"
                or rejected[-1].get("accepted") is not False
                or rejected[-1].get("published") is not False
                or route.get("policy") != "FROZEN_JOB_ROUTE_ONLY"
                or route.get("worker") != new_request.get("worker")
                or route.get("model") != new_request.get("model")
                or route.get("reasoning_effort") != new_request.get("reasoning_effort")
                or job.get("pending_attempt_kind") != "TECHNICAL_RECOVERY"
                or job.get("execution_context") != old.get("execution_context")
                or job.get("depends_on") != old.get("depends_on")
                or job.get("batch_id") != old.get("batch_id")
                or job.get("last_result") != old.get("last_result")
                or job.get("attempt_reservations") != old.get("attempt_reservations")
                or job.get("outer_attempt") != old.get("outer_attempt")
            )
            if invalid:
                raise RepositoryError("SCOPE_CUSTODY_RECOVERY_TRANSITION_INVALID")
            cursor = self._connection.execute(
                "UPDATE logical_jobs SET status=?,revision=?,phase='PLANNED',owner_id='',"
                "payload=?,contract_revision=? WHERE job_id=? AND revision=? AND phase='TERMINAL'",
                (job["status"], job["revision"], canonical(job),
                 int(job["contract_revision"]), job["job_id"], expected_revision),
            )
            if cursor.rowcount != 1:
                raise RepositoryError("CONTROL_REVISION_CONFLICT")
            self._relations(job)
            self.project_product_readiness(job["job_id"])

    def bootstrap(self, queue, jobs, legacy_refs):
        with self.hold():
            if self._connection.execute("SELECT 1 FROM control_state").fetchone():
                raise RepositoryError("CONTROL_REPOSITORY_NOT_EMPTY")
            self._connection.execute("INSERT INTO control_state VALUES('session',?,?)", (queue["revision"], canonical(queue)))
            self._connection.execute("INSERT INTO control_state VALUES('legacy_jobs',0,?)", (canonical(legacy_refs),))
            for job_id, job in sorted(jobs.items()):
                self._insert_job(job, legacy_refs[job_id]["path"])
            self.import_legacy_notifications()

    def worklist(self):
        with self.hold():
            return [dict(row) for row in self._connection.execute(
                "SELECT job_id,logical_job_id,status,revision,phase,contract_revision,owner_id,legacy_job_ref FROM logical_jobs ORDER BY rowid")]

    def revise_planned(self, job_id, request, *, expected_revision):
        with self.hold():
            row = self._connection.execute("SELECT phase FROM logical_jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None or row[0] not in {"PLANNED", "READY"}:
                raise RepositoryError("MATERIALIZED_CONTRACT_IMMUTABLE")
            job = self.read_job(job_id)
            if job["revision"] != expected_revision or job["status"] != "QUEUED":
                raise RepositoryError("CONTROL_REVISION_CONFLICT")
            for key in ("client_job_id", "depends_on", "dependency_semantics", "qa_type", "qa_hold_scope", "replaces_job_id"):
                if request.get(key) != job["request"].get(key):
                    raise RepositoryError("PLANNED_LINEAGE_CHANGE_REQUIRES_NEW_JOB")
            job.setdefault("original_request", copy.deepcopy(job["request"]))
            job["request"] = copy.deepcopy(request)
            job["current_requirement"] = request["requirement"]
            job["request_sha256"] = digest(request)
            job["contract_revision"] = int(job.get("contract_revision", 1)) + 1
            job.setdefault("contract_revisions", []).append({"revision": job["contract_revision"],
                "request_sha256": digest(request), "approved_at": now()})
            job["revision"] += 1
            job["updated_at"] = now()
            self.write_job(job, expected_revision=expected_revision)
            return job

    def amend_preserved_test_plan(self, job_id, **kwargs):
        from testplan_amendment import amend
        return amend(self, job_id, **kwargs)

    def admit_reviewer_successor(self, task, *, expected_revision, checkpoint):
        from reviewer_successor import admit
        return admit(self, task, expected_revision=expected_revision, checkpoint=checkpoint)

    def finish_reviewer_successor(self, execution_id, *, result):
        from reviewer_successor import finish
        return finish(self, execution_id, result=result)

    def materialize(self, job, *, context, policy, baseline_hash, execution_surface_hash, extra=None):
        protected = {"execution_id", "materialization_id", "job_id", "logical_job_id",
                     "contract_revision", "profile_revision", "policy_revision", "effective_policy_hash",
                     "baseline_hash", "execution_surface_hash", "materialized_at", "execution_context",
                     "effective_policy", "request", "control_epoch"}
        if not isinstance(extra or {}, dict) or protected.intersection(extra or {}):
            raise RepositoryError("MATERIALIZATION_EXTRA_AUTHORITY_OVERRIDE")
        with self.hold():
            if self.global_stop().get("active"):
                raise RepositoryError("GLOBAL_STOP")
            active = self._connection.execute("SELECT payload FROM executions WHERE job_id=? AND phase!='TERMINAL'", (job["job_id"],)).fetchone()
            if active:
                existing = json.loads(active[0])
                __import__("control_epoch").assert_execution(self, existing["execution_id"])
                return existing
            stored = self.read_job(job["job_id"])
            if stored["revision"] != job["revision"] or stored["status"] not in {"QUEUED", "RUNNING"}:
                raise RepositoryError("CONTROL_REVISION_CONFLICT")
            self._connection.execute("UPDATE logical_jobs SET phase='READY' WHERE job_id=?", (job["job_id"],))
            input_handoff = job.get('technical_candidate_input') or {}
            execution_id = "EXEC-" + uuid.uuid4().hex.upper()
            materialization_id = "MAT-" + uuid.uuid4().hex.upper()
            frozen_extra = dict(extra or {})
            reservation = dict(job.get("active_attempt_reservation") or {})
            provenance = dict(reservation.get("provenance") or {})
            recovery_source_id = str(provenance.get("source_execution_id") or "")
            if (
                reservation.get("attempt_kind") == "TECHNICAL_RECOVERY"
                and provenance.get("fresh_session_reason") == "TERMINAL_EXECUTION_FENCED"
            ):
                source_row = self._connection.execute(
                    "SELECT phase,payload FROM executions WHERE execution_id=?",
                    (recovery_source_id,),
                ).fetchone()
                if source_row is None or source_row["phase"] != "TERMINAL":
                    raise RepositoryError("TECHNICAL_RECOVERY_SOURCE_NOT_TERMINAL")
                source_execution = json.loads(source_row["payload"])
                same_identity = (
                    source_execution.get("job_id") == job.get("job_id")
                    and source_execution.get("logical_job_id")
                    == str(job.get("logical_job_id") or job.get("client_job_id") or job["job_id"])
                    and int(source_execution.get("contract_revision", 0))
                    == int(job.get("contract_revision", 1))
                    and str(source_execution.get("baseline_hash") or "") == str(baseline_hash)
                    and str(dict(source_execution.get("execution_context") or {}).get(
                        "workspace_identity_sha256", ""
                    )) == str(context.get("workspace_identity_sha256") or "")
                )
                if not same_identity or not source_execution.get("source_view_id"):
                    raise RepositoryError("TECHNICAL_RECOVERY_SOURCE_IDENTITY_MISMATCH")
                frozen_extra["source_view_id"] = source_execution["source_view_id"]
                frozen_extra["handoff_source_execution_id"] = recovery_source_id
                frozen_extra["fresh_session_reason"] = "TERMINAL_EXECUTION_FENCED"
            from source_view import scope_contract, source_view_manifest
            workspace_identity = str(context.get("workspace_identity_sha256") or "")
            source_view = source_view_manifest(
                logical_job_id=str(job.get("logical_job_id") or job.get("client_job_id") or job["job_id"]),
                contract_revision=int(job.get("contract_revision", 1)), role="WORKER",
                workspace_identity=workspace_identity, baseline_hash=baseline_hash,
                materialization_id=materialization_id,
                predecessor_delta=job.get("inherited_batch_delta_files") or (),
                candidate_overlay=input_handoff,
            )
            if frozen_extra.get("source_view_id"):
                source_view["source_view_id"] = str(frozen_extra["source_view_id"])
            frozen_extra.setdefault("source_view_id", source_view["source_view_id"])
            frozen_extra.setdefault("source_view_manifest", source_view)
            frozen_extra.setdefault("mutation_scope", scope_contract(job.get("request") or {}))
            if frozen_extra.get("runtime_contracts"):
                from runtime_host import runtime_host_manifest
                frozen_extra.setdefault("runtime_host", runtime_host_manifest(
                    workspace_identity=workspace_identity,
                    runtime_contracts=frozen_extra["runtime_contracts"],
                ))
            execution = {"execution_id": execution_id, "materialization_id": materialization_id,
                         "job_id": job["job_id"],
                         "logical_job_id": str(job.get("logical_job_id") or job.get("client_job_id") or job["job_id"]),
                         "contract_revision": int(job.get("contract_revision", 1)),
                         "profile_revision": context["profile_snapshot_sha256"], "policy_revision": context["policy_snapshot_sha256"],
                         "effective_policy_hash": policy["effective_policy_sha256"], "baseline_hash": baseline_hash,
                         "execution_surface_hash": execution_surface_hash, "materialized_at": now(),
                         "execution_context": dict(context), "effective_policy": copy.deepcopy(policy),
                         "control_epoch": __import__("control_epoch").current(self._connection),
                         "request": copy.deepcopy(job["request"]), **frozen_extra}
            if input_handoff:
                execution['technical_candidate_input'] = copy.deepcopy(input_handoff)
                source = self._connection.execute(
                    "SELECT execution_id FROM executions WHERE job_id IN (?,?) AND phase='TERMINAL' "
                    "ORDER BY CASE WHEN job_id=? THEN 0 ELSE 1 END, materialized_at DESC LIMIT 1",
                    (job['job_id'], input_handoff.get('source_job_id', ''), job['job_id']),
                ).fetchone()
                if source:
                    execution['handoff_source_execution_id'] = source[0]
            self._connection.execute("INSERT INTO executions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
                (execution["execution_id"], job["job_id"], execution["logical_job_id"], execution["contract_revision"],
                 "MATERIALIZED", execution["profile_revision"], execution["policy_revision"], execution["effective_policy_hash"],
                 baseline_hash, execution_surface_hash, execution["materialized_at"], canonical(execution)))
            self._connection.execute(
                "INSERT INTO materializations VALUES(?,?,?,?)",
                (materialization_id, execution_id, execution["materialized_at"], canonical({
                    "materialization_id": materialization_id,
                    "execution_id": execution_id,
                    "contract_revision": execution["contract_revision"],
                    "execution_surface_hash": execution_surface_hash,
                    "created_at": execution["materialized_at"],
                })),
            )
            authority = {
                "execution_id": execution_id,
                "state": "SUSPENDED",
                "revision": 0,
                "reason": "MATERIALIZED",
                "updated_at": execution["materialized_at"],
            }
            self._connection.execute(
                "INSERT INTO execution_authority VALUES(?,?,?,?,?,?)",
                (
                    execution_id, "SUSPENDED", 0, "MATERIALIZED",
                    execution["materialized_at"], canonical(authority),
                ),
            )
            self._connection.execute("UPDATE logical_jobs SET phase='MATERIALIZED' WHERE job_id=?", (job["job_id"],))
            return execution

    def start_attempt(self, execution, *, attempt_id, role, model, candidate_hash=""):
        role = str(role).upper()
        if (role not in {"WORKER", "TESTER", "REVIEWER", "PLANNER"}
                or not isinstance(attempt_id, str) or not attempt_id.strip()
                or not isinstance(model, str) or not model.strip()):
            raise RepositoryError("ATTEMPT_IDENTITY_INVALID")
        with self.hold():
            if self.global_stop().get("active"):
                raise RepositoryError("GLOBAL_STOP")
            row = self._connection.execute("SELECT phase,payload FROM executions WHERE execution_id=?", (execution["execution_id"],)).fetchone()
            if row is None or row[0] == "TERMINAL":
                raise RepositoryError("TERMINAL_EXECUTION_IMMUTABLE")
            if canonical(execution) != row[1]:
                raise RepositoryError("MATERIALIZED_CONTRACT_MISMATCH")
            __import__("control_epoch").assert_execution(self, execution["execution_id"])
            if execution.get('role') == 'REVIEWER' and role != 'REVIEWER':
                raise RepositoryError('REVIEWER_SUCCESSOR_ROLE_REQUIRED')
            if execution.get('role') in {'CANONICAL_REVALIDATION', 'BATCH_VERIFICATION'} and role != 'TESTER':
                raise RepositoryError('REVALIDATION_TESTER_ROLE_REQUIRED')
            if role == "WORKER":
                active_attempt = self._connection.execute(
                    "SELECT 1 FROM attempts a JOIN attempt_outcomes o ON o.attempt_id=a.attempt_id "
                    "WHERE a.execution_id=? AND a.role='WORKER' AND o.status='RUNNING' LIMIT 1",
                    (execution["execution_id"],)).fetchone()
                if active_attempt:
                    raise RepositoryError("WRITER_ATTEMPT_ALREADY_ACTIVE")
                live_process = self._connection.execute(
                    "SELECT 1 FROM runtime_processes p JOIN attempts a ON a.attempt_id=p.attempt_id "
                    "WHERE a.execution_id=? AND a.role='WORKER' "
                    "AND p.state IN ('STARTING','RUNNING','UNKNOWN','LOST') LIMIT 1",
                    (execution["execution_id"],)).fetchone()
                if live_process:
                    raise RepositoryError("OLD_RUNTIME_WRITER_ACTIVE")
                authority = self.writer_authority(execution["execution_id"])
                if not authority or (
                    authority["state"] == "SUSPENDED"
                    and authority["reason"] not in {"MATERIALIZED", "MIGRATED_MATERIALIZED"}
                ):
                    raise RepositoryError("WRITER_AUTHORITY_SUSPENDED")
                if authority["state"] == "RELEASED":
                    raise RepositoryError("WRITER_AUTHORITY_RELEASED")
                self._set_writer_authority_locked(
                    execution["execution_id"], "ACTIVE", "WORKER_ATTEMPT_STARTED"
                )
            attempt = {"attempt_id": attempt_id, "execution_id": execution["execution_id"], "role": role,
                       "model": model, "profile_revision": execution["profile_revision"], "policy_revision": execution["policy_revision"],
                       "execution_surface_hash": execution["execution_surface_hash"], "candidate_hash": candidate_hash,
                       "control_epoch": execution.get("control_epoch", __import__("control_epoch").LEGACY_EPOCH),
                       "started_at": now()}
            if "harness_source_identity" in execution:
                from runtime_identity import attempt_provenance
                attempt["harness_provenance"] = attempt_provenance(execution,
                    attempt_id=attempt_id, role=role, model=model,
                    harness_root=Path(__file__).resolve().parent)
            self._connection.execute("INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?)",
                                     (*[attempt[key] for key in ["attempt_id", "execution_id", "role", "model", "profile_revision", "policy_revision", "execution_surface_hash", "candidate_hash", "started_at"]], canonical(attempt)))
            self._connection.execute(
                "INSERT INTO attempt_outcomes VALUES(?,?,?,?,?,?,?)",
                (attempt_id, "RUNNING", "", "", 0, "", canonical({
                    "attempt_id": attempt_id, "status": "RUNNING", "retry_domain": "",
                })),
            )
            self._connection.execute("UPDATE executions SET phase='RUNNING' WHERE execution_id=?", (execution["execution_id"],))
            self._connection.execute("UPDATE logical_jobs SET phase='RUNNING' WHERE job_id=?", (execution["job_id"],))
            return attempt

    def _set_writer_authority_locked(self, execution_id, state, reason):
        if state not in {"ACTIVE", "SUSPENDED", "RELEASED"}:
            raise RepositoryError("WRITER_AUTHORITY_STATE_INVALID")
        if state == 'ACTIVE':
            execution = self.execution(execution_id)
            if execution and execution.get('role') == 'REVIEWER':
                raise RepositoryError('REVIEWER_READ_ONLY')
        row = self._connection.execute(
            "SELECT state,revision,payload FROM execution_authority WHERE execution_id=?",
            (execution_id,),
        ).fetchone()
        if row is None:
            raise RepositoryError("WRITER_AUTHORITY_MISSING")
        if row["state"] == "RELEASED" and state != "RELEASED":
            raise RepositoryError("WRITER_AUTHORITY_RELEASED")
        if row["state"] == state and json.loads(row["payload"]).get("reason") == reason:
            return json.loads(row["payload"])
        revision = int(row["revision"]) + 1
        updated = now()
        payload = {
            "execution_id": execution_id, "state": state, "revision": revision,
            "reason": str(reason), "updated_at": updated,
        }
        self._connection.execute(
            "UPDATE execution_authority SET state=?,revision=?,reason=?,updated_at=?,payload=? "
            "WHERE execution_id=?",
            (state, revision, str(reason), updated, canonical(payload), execution_id),
        )
        return payload

    def writer_authority(self, execution_id):
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM execution_authority WHERE execution_id=?", (execution_id,)
            ).fetchone()
            return json.loads(row[0]) if row else None

    def suspend_writer_authority(self, execution_id, *, reason):
        with self.hold():
            return self._set_writer_authority_locked(execution_id, "SUSPENDED", reason)

    def materialization(self, execution_id):
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM materializations WHERE execution_id=?", (execution_id,)
            ).fetchone()
            return json.loads(row[0]) if row else None

    def finish_attempt(self, attempt_id, *, status, retry_domain="", failure_code=""):
        from runtime_adapter import RetryDomain
        allowed_status = {"SUCCEEDED", "FAILED", "INTERRUPTED", "UNKNOWN"}
        domain = str(retry_domain or "")
        if status not in allowed_status:
            raise RepositoryError("ATTEMPT_STATUS_INVALID")
        if domain and domain not in {item.value for item in RetryDomain}:
            raise RepositoryError("RETRY_DOMAIN_INVALID")
        product_delta = int(domain == RetryDomain.PRODUCT_SEMANTIC_RETRY.value and status == "FAILED")
        with self.hold():
            __import__("control_epoch").assert_attempt(self, attempt_id)
            row = self._connection.execute(
                "SELECT status FROM attempt_outcomes WHERE attempt_id=?", (attempt_id,)
            ).fetchone()
            if row is None:
                raise RepositoryError("ATTEMPT_NOT_FOUND")
            if row[0] not in {"RUNNING", "UNKNOWN"}:
                if row[0] == status:
                    prior = self.attempt_outcome(attempt_id)
                    if (prior.get("retry_domain", "") != domain
                            or prior.get("failure_code", "") != str(failure_code)):
                        raise RepositoryError("ATTEMPT_OUTCOME_CONFLICT")
                    return prior
                raise RepositoryError("ATTEMPT_TERMINAL_IMMUTABLE")
            finished = now()
            payload = {"attempt_id": attempt_id, "status": status, "retry_domain": domain,
                       "failure_code": str(failure_code), "product_retry_delta": product_delta,
                       "finished_at": finished}
            self._connection.execute(
                "UPDATE attempt_outcomes SET status=?,retry_domain=?,failure_code=?,product_retry_delta=?,finished_at=?,payload=? WHERE attempt_id=?",
                (status, domain, str(failure_code), product_delta, finished, canonical(payload), attempt_id),
            )
            return payload

    def attempt_outcome(self, attempt_id):
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM attempt_outcomes WHERE attempt_id=?", (attempt_id,)
            ).fetchone()
            return json.loads(row[0]) if row else None

    def latest_attempt(self, execution_id, role=""):
        with self.hold():
            query = (
                "SELECT a.payload,o.payload FROM attempts a JOIN attempt_outcomes o ON o.attempt_id=a.attempt_id "
                "WHERE a.execution_id=?" + (" AND a.role=?" if role else "") + " ORDER BY a.started_at DESC,a.rowid DESC LIMIT 1"
            )
            params = (execution_id, role) if role else (execution_id,)
            row = self._connection.execute(query, params).fetchone()
            if row is None:
                return None
            return {**json.loads(row[0]), "outcome": json.loads(row[1])}

    def attempt_provenance(self, attempt_id):
        """Read the immutable Attempt payload; missing legacy provenance stays unknown."""
        with self.hold():
            row = self._connection.execute("SELECT payload FROM attempts WHERE attempt_id=?",
                                           (attempt_id,)).fetchone()
            if row is None:
                return None
            return json.loads(row[0]).get("harness_provenance")

    def attempt_identity(self, attempt_id):
        """Canonical runtime-launch lineage proof for one attempt."""
        with self.hold():
            row = self._connection.execute(
                "SELECT a.execution_id,a.role,o.status FROM attempts a "
                "LEFT JOIN attempt_outcomes o ON o.attempt_id=a.attempt_id "
                "WHERE a.attempt_id=?",
                (attempt_id,),
            ).fetchone()
            if row is None:
                return None
            return {"execution_id": row[0], "role": row[1], "status": row[2] or ""}

    def bind_native_session(self, execution_id, *, runtime, native_session_id,
                            adapter_revision, runtime_version, durable=True, state="ACTIVE",
                            role="WORKER", source_view_id="", workspace_identity="",
                            runtime_contract_hash="", candidate_hash="",
                            review_contract_hash="", allow_fresh=False):
        from runtime_adapter import SessionState
        valid_states = {item.value for item in SessionState}
        role = str(role).upper()
        if (state not in valid_states or not runtime or not native_session_id
                or role not in {"WORKER", "REVIEWER", "TESTER", "PLANNER"}):
            raise RepositoryError("NATIVE_SESSION_BINDING_INVALID")
        with self.hold():
            __import__("control_epoch").assert_execution(self, execution_id)
            execution = self._connection.execute(
                "SELECT phase,payload FROM executions WHERE execution_id=?", (execution_id,)
            ).fetchone()
            if execution is None:
                raise RepositoryError("EXECUTION_NOT_FOUND")
            if execution[0] == "TERMINAL":
                raise RepositoryError("TERMINAL_EXECUTION_IMMUTABLE")
            frozen = json.loads(execution["payload"])
            if frozen.get('role') == 'REVIEWER' and role != 'REVIEWER':
                raise RepositoryError('REVIEWER_SUCCESSOR_ROLE_REQUIRED')
            context = dict(frozen.get("execution_context") or {})
            expected = {
                "logical_job_id": str(frozen.get("logical_job_id", "")),
                "contract_revision": int(frozen.get("contract_revision", 0)),
                "workspace_identity": str(context.get("workspace_identity_sha256", "")),
                "source_view_id": str(frozen.get("source_view_id", "")),
                "runtime_contract_hash": str(frozen.get("runtime_contract_hash", "")),
            }
            supplied = {
                "workspace_identity": str(workspace_identity),
                "source_view_id": str(source_view_id),
                "runtime_contract_hash": str(runtime_contract_hash),
            }
            for key, value in supplied.items():
                if value and expected[key] and value != expected[key]:
                    raise RepositoryError("SESSION_EXECUTION_BINDING_MISMATCH")
            existing_native = self._connection.execute(
                "SELECT * FROM native_sessions WHERE runtime=? AND native_session_id=?",
                (runtime, native_session_id),
            ).fetchone()
            if existing_native:
                payload = json.loads(existing_native["payload"])
                if existing_native["execution_id"] != execution_id:
                    raise RepositoryError("NATIVE_SESSION_ALREADY_BOUND")
                if existing_native["role"] != role:
                    raise RepositoryError("NATIVE_SESSION_ROLE_SHARE_FORBIDDEN")
                if review_contract_hash and existing_native["review_contract_hash"] != review_contract_hash:
                    raise RepositoryError("NATIVE_SESSION_REVIEW_CONTRACT_MISMATCH")
                payload.update({"state": state, "last_seen_at": now()})
                self._connection.execute(
                    "UPDATE native_sessions SET state=?,last_seen_at=?,payload=? WHERE binding_id=?",
                    (state, payload["last_seen_at"], canonical(payload), existing_native["binding_id"]),
                )
                return payload
            existing = self._connection.execute(
                "SELECT * FROM native_sessions WHERE execution_id=? AND runtime=? AND role=? "
                "ORDER BY bound_at DESC LIMIT 1",
                (execution_id, runtime, role),
            ).fetchone()
            if existing and not allow_fresh:
                raise RepositoryError("DURABLE_SESSION_REBIND_FORBIDDEN")
            observed = now()
            binding_id = "BIND-" + uuid.uuid4().hex.upper()
            payload = {"binding_id": binding_id, "execution_id": execution_id,
                       "runtime": runtime, "role": role,
                       "native_session_id": native_session_id,
                       "logical_job_id": expected["logical_job_id"],
                       "contract_revision": expected["contract_revision"],
                       "workspace_identity": supplied["workspace_identity"] or expected["workspace_identity"],
                       "source_view_id": supplied["source_view_id"] or expected["source_view_id"],
                       "runtime_contract_hash": supplied["runtime_contract_hash"] or expected["runtime_contract_hash"],
                       "candidate_hash": str(candidate_hash),
                       "review_contract_hash": str(review_contract_hash),
                       "state": state, "durable": bool(durable),
                       "adapter_revision": adapter_revision, "runtime_version": runtime_version,
                       "bound_at": observed, "last_seen_at": observed}
            try:
                self._connection.execute(
                    "INSERT INTO native_sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        binding_id, execution_id, runtime, role, native_session_id,
                        payload["logical_job_id"], payload["contract_revision"],
                        payload["workspace_identity"], payload["source_view_id"],
                        payload["runtime_contract_hash"], payload["candidate_hash"],
                        payload["review_contract_hash"], state, int(bool(durable)),
                        adapter_revision, runtime_version, observed, observed, canonical(payload),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise RepositoryError("NATIVE_SESSION_ALREADY_BOUND") from exc
            return payload

    def session_binding(self, execution_id, runtime="", *, role="WORKER",
                        review_contract_hash=""):
        with self.hold():
            clauses = ["execution_id=?", "role=?"]
            params = [execution_id, str(role).upper()]
            if runtime:
                clauses.append("runtime=?")
                params.append(runtime)
            if review_contract_hash:
                clauses.append("review_contract_hash=?")
                params.append(review_contract_hash)
            row = self._connection.execute(
                "SELECT payload FROM native_sessions WHERE " + " AND ".join(clauses)
                + " ORDER BY bound_at DESC LIMIT 1", tuple(params),
            ).fetchone()
            return json.loads(row[0]) if row else None

    def update_session_state(self, binding_id, state):
        from runtime_adapter import SessionState
        if state not in {item.value for item in SessionState}:
            raise RepositoryError("NATIVE_SESSION_STATE_INVALID")
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM native_sessions WHERE binding_id=?", (binding_id,)
            ).fetchone()
            if row is None:
                raise RepositoryError("NATIVE_SESSION_NOT_FOUND")
            payload = json.loads(row[0])
            __import__("control_epoch").assert_execution(self, payload["execution_id"])
            payload.update({"state": state, "last_seen_at": now()})
            self._connection.execute(
                "UPDATE native_sessions SET state=?,last_seen_at=?,payload=? WHERE binding_id=?",
                (state, payload["last_seen_at"], canonical(payload), binding_id),
            )
            return payload

    def record_runtime_process(self, attempt_id, *, runtime, pid, process_identity,
                               binding_id="", state="RUNNING"):
        if state not in {"STARTING", "RUNNING"}:
            raise RepositoryError("RUNTIME_PROCESS_STATE_INVALID")
        if (type(pid) is not int or pid <= 0 or not isinstance(runtime, str) or not runtime.strip()
                or not isinstance(process_identity, str) or not process_identity.strip()):
            raise RepositoryError("RUNTIME_PROCESS_IDENTITY_INVALID")
        with self.hold():
            __import__("control_epoch").assert_attempt(self, attempt_id)
            attempt = self._connection.execute(
                "SELECT execution_id,role FROM attempts WHERE attempt_id=?", (attempt_id,)
            ).fetchone()
            if attempt is None:
                raise RepositoryError("ATTEMPT_NOT_FOUND")
            if (self.attempt_outcome(attempt_id) or {}).get("status") != "RUNNING":
                raise RepositoryError("ATTEMPT_NOT_RUNNING")
            if attempt["role"] == "WORKER":
                authority = self.writer_authority(attempt["execution_id"])
                if not authority or authority["state"] != "ACTIVE":
                    raise RepositoryError("WRITER_AUTHORITY_SUSPENDED")
            active = self._connection.execute(
                "SELECT process_id,pid,process_identity FROM runtime_processes WHERE attempt_id=? AND state IN ('STARTING','RUNNING') ORDER BY started_at DESC LIMIT 1",
                (attempt_id,),
            ).fetchone()
            if active:
                if active["pid"] == int(pid) and active["process_identity"] == process_identity:
                    prior = self.runtime_process(active["process_id"])
                    if prior.get("runtime") != runtime or prior.get("binding_id", "") != binding_id:
                        raise RepositoryError("RUNTIME_PROCESS_IDENTITY_CONFLICT")
                    return prior
                raise RepositoryError("OLD_RUNTIME_WRITER_ACTIVE")
            if binding_id:
                binding = self._connection.execute(
                    "SELECT execution_id,role,runtime FROM native_sessions WHERE binding_id=?", (binding_id,)
                ).fetchone()
                if (binding is None or binding[0] != attempt["execution_id"]
                        or binding["role"] != attempt["role"] or binding["runtime"] != runtime):
                    raise RepositoryError("PROCESS_SESSION_EXECUTION_MISMATCH")
            process_id = "PROC-" + uuid.uuid4().hex.upper()
            started = now()
            payload = {"process_id": process_id, "attempt_id": attempt_id,
                       "execution_id": attempt["execution_id"], "binding_id": binding_id,
                       "runtime": runtime, "pid": int(pid),
                       "process_identity": process_identity, "state": state,
                       "started_at": started, "ended_at": "", "exit_code": None}
            provenance = self.attempt_provenance(attempt_id) or {}
            payload["attempt_provenance_sha256"] = digest(provenance) if provenance else "NOT_CAPTURED"
            payload["control_epoch"] = __import__("control_epoch").current(self._connection)
            payload["observed_runtime"] = runtime
            payload["harness_process_incarnation"] = provenance.get("process_incarnation", "NOT_CAPTURED")
            self._connection.execute(
                "INSERT INTO runtime_processes VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (process_id, attempt_id, binding_id or None, runtime, int(pid),
                 process_identity, state, started, "", None, canonical(payload)),
            )
            return payload

    def runtime_process(self, process_id):
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM runtime_processes WHERE process_id=?", (process_id,)
            ).fetchone()
            return json.loads(row[0]) if row else None

    def active_runtime_process(self, attempt_id):
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM runtime_processes WHERE attempt_id=? AND state IN ('STARTING','RUNNING') ORDER BY started_at DESC LIMIT 1",
                (attempt_id,),
            ).fetchone()
            return json.loads(row[0]) if row else None

    def authorize_runtime_recovery(self, execution_id, attempt_id, *, candidate_disposition,
                                   candidate_digest, confirmation):
        """Harness policy handoff after fencing; never an implicit semantic retry."""
        if confirmation != "AUTHORIZE_NEW_ATTEMPT_AFTER_RECONCILIATION" or candidate_disposition != "PRESERVE_FOR_REVERIFICATION" or len(candidate_digest) != 64:
            raise RepositoryError("RECOVERY_DISPOSITION_REQUIRED")
        with self.hold():
            __import__("control_epoch").assert_execution(self, execution_id)
            latest = self.latest_attempt(execution_id)
            attempt = self.attempt_identity(attempt_id)
            if not latest or latest["attempt_id"] != attempt_id or not attempt or attempt["status"] not in {"FAILED", "INTERRUPTED", "UNKNOWN"}:
                raise RepositoryError("OLD_ATTEMPT_NOT_RECONCILED")
            materialized = self._connection.execute("SELECT job_id FROM executions WHERE execution_id=?", (execution_id,)).fetchone()
            if self.global_stop().get("active") or self.open_decisions(materialized["job_id"]):
                raise RepositoryError("HARNESS_AUTHORIZATION_BLOCKED")
            row = self._connection.execute("SELECT payload FROM runtime_recoveries WHERE execution_id=? AND attempt_id=? ORDER BY ordinal DESC LIMIT 1",(execution_id,attempt_id)).fetchone()
            recovery=json.loads(row[0]) if row else {}
            evidence=recovery.get("evidence",{})
            if not evidence.get("writer_quiescence",{}).get("quiesced") or evidence.get("candidate_digest")!=candidate_digest:
                raise RepositoryError("RECOVERY_QUIESCENCE_REQUIRED")
            command_id=evidence.get("binding",{}).get("command_id","")
            command=self.command(command_id)
            if not command or not command.get("aborted"):
                raise RepositoryError("COMMAND_RECONCILIATION_REQUIRED")
            self.record_runtime_recovery(execution_id,attempt_id,failure_code="RECOVERY_POLICY_AUTHORIZED",
                state="FRESH_ATTEMPT_REQUIRED",evidence={**evidence,"candidate_disposition":candidate_disposition,"candidate_digest":candidate_digest})
            return self._set_writer_authority_locked(execution_id,"ACTIVE","RECOVERY_NEW_ATTEMPT_AUTHORIZED")

    def record_execution_observation(self, attempt_id, *, identity, observation):
        """Persist bound liveness in existing schema-5 process payload, atomically."""
        with self.hold():
            __import__("control_epoch").assert_attempt(self, attempt_id)
            attempt = self.attempt_identity(attempt_id)
            execution = self.active_execution(identity.get("job_id", ""))
            latest = self.latest_attempt(identity.get("execution_id", ""))
            if (not attempt or attempt.get("status") != "RUNNING" or not execution
                    or execution["execution_id"] != identity.get("execution_id")
                    or attempt["execution_id"] != execution["execution_id"]
                    or identity.get("attempt_id") != attempt_id
                    or not latest or latest["attempt_id"] != attempt_id):
                raise RepositoryError("FOREIGN_ATTEMPT")
            process = self.active_runtime_process(attempt_id)
            if not process:
                if self._connection.execute("PRAGMA user_version").fetchone()[0] != 6:
                    raise RepositoryError("RUNTIME_PROCESS_NOT_FOUND")
                if observation.get("identity") != identity:
                    raise RepositoryError("HEARTBEAT_IDENTITY_MISMATCH")
                old = self._connection.execute("SELECT identity,payload FROM execution_observations WHERE attempt_id=?", (attempt_id,)).fetchone()
                if old and old[0] != canonical(identity):
                    raise RepositoryError("HEARTBEAT_IDENTITY_MISMATCH")
                previous = json.loads(old[1]) if old else {}
                if previous.get("state") in {"FENCING","QUIESCED","QA_WAIT"} and observation.get("state")=="ACTIVE":
                    raise RepositoryError("LEASE_RENEWAL_AFTER_FENCING")
                self._connection.execute("INSERT INTO execution_observations VALUES(?,?,?) ON CONFLICT(attempt_id) DO UPDATE SET payload=excluded.payload", (attempt_id,canonical(identity),canonical(observation)))
                return observation
            previous = process.get("execution_observation", {})
            if previous and previous.get("identity") != identity:
                raise RepositoryError("HEARTBEAT_IDENTITY_MISMATCH")
            if previous.get("state") in {"FENCING", "QUIESCED", "QA_WAIT"} and observation.get("state") == "ACTIVE":
                raise RepositoryError("LEASE_RENEWAL_AFTER_FENCING")
            if observation.get("identity") != identity:
                raise RepositoryError("HEARTBEAT_IDENTITY_MISMATCH")
            process["execution_observation"] = copy.deepcopy(observation)
            self._connection.execute("UPDATE runtime_processes SET payload=? WHERE process_id=?",
                                     (canonical(process), process["process_id"]))
            return process

    def reconcile_runtime_process(self, attempt_id, *, process_alive):
        with self.hold():
            active = self.active_runtime_process(attempt_id)
            if not active:
                return {"action": "NO_ACTIVE_PROCESS", "process": None}
            alive = bool(process_alive(int(active["pid"]), str(active.get("started_at", ""))))
            if alive:
                return {"action": "OLD_WRITER_ACTIVE", "process": active}
            lost = self.finish_runtime_process(active["process_id"], state="LOST", exit_code=None)
            return {"action": "PROCESS_LOST_SESSION_PRESERVED", "process": lost}

    def finish_runtime_process(self, process_id, *, state, exit_code=None):
        if state not in {"EXITED", "KILLED", "LOST", "UNKNOWN"}:
            raise RepositoryError("RUNTIME_PROCESS_STATE_INVALID")
        with self.hold():
            row = self._connection.execute(
                "SELECT state,payload FROM runtime_processes WHERE process_id=?", (process_id,)
            ).fetchone()
            if row is None:
                raise RepositoryError("RUNTIME_PROCESS_NOT_FOUND")
            if row[0] in {"EXITED", "KILLED", "LOST"}:
                payload = json.loads(row[1])
                __import__("control_epoch").assert_execution(self, payload["execution_id"])
                if payload.get("state") == state and payload.get("exit_code") == exit_code:
                    return payload
                raise RepositoryError("RUNTIME_PROCESS_TERMINAL_IMMUTABLE")
            payload = json.loads(row[1])
            __import__("control_epoch").assert_execution(self, payload["execution_id"])
            payload.update({"state": state, "exit_code": exit_code, "ended_at": now()})
            self._connection.execute(
                "UPDATE runtime_processes SET state=?,ended_at=?,exit_code=?,payload=? WHERE process_id=?",
                (state, payload["ended_at"], exit_code, canonical(payload), process_id),
            )
            return payload

    def prepare_command(self, execution_id, attempt_id, *, idempotency_key,
                        payload_sha256, binding_id="", command_id=""):
        from runtime_adapter import CommandDelivery
        if not idempotency_key or len(payload_sha256) != 64:
            raise RepositoryError("COMMAND_IDENTITY_INVALID")
        with self.hold():
            __import__("control_epoch").assert_execution(self, execution_id)
            attempt = self._connection.execute(
                "SELECT execution_id,role FROM attempts WHERE attempt_id=?", (attempt_id,)
            ).fetchone()
            if attempt is None or attempt[0] != execution_id:
                raise RepositoryError("COMMAND_ATTEMPT_EXECUTION_MISMATCH")
            if attempt["role"] == "WORKER":
                authority = self.writer_authority(execution_id)
                if not authority or authority["state"] != "ACTIVE":
                    raise RepositoryError("WRITER_AUTHORITY_SUSPENDED")
            existing = self._connection.execute(
                "SELECT payload FROM commands WHERE execution_id=? AND idempotency_key=?",
                (execution_id, idempotency_key),
            ).fetchone()
            if existing:
                payload = json.loads(existing[0])
                if (payload["payload_sha256"] != payload_sha256 or payload.get("binding_id", "") != binding_id
                        or payload.get("attempt_id") != attempt_id
                        or (command_id and payload.get("command_id") != command_id)):
                    raise RepositoryError("COMMAND_IDEMPOTENCY_CONFLICT")
                return payload
            if (self.attempt_outcome(attempt_id) or {}).get("status") != "RUNNING":
                raise RepositoryError("ATTEMPT_NOT_RUNNING")
            if binding_id:
                binding = self._connection.execute(
                    "SELECT execution_id,role FROM native_sessions WHERE binding_id=?", (binding_id,)
                ).fetchone()
                if binding is None or binding[0] != execution_id or binding["role"] != attempt["role"]:
                    raise RepositoryError("COMMAND_SESSION_EXECUTION_MISMATCH")
            ordinal = int(self._connection.execute(
                "SELECT COALESCE(MAX(ordinal),0)+1 FROM commands WHERE execution_id=?", (execution_id,)
            ).fetchone()[0])
            command_id = command_id or "CMD-" + uuid.uuid4().hex.upper()
            prepared = now()
            payload = {"command_id": command_id, "execution_id": execution_id,
                       "attempt_id": attempt_id, "binding_id": binding_id,
                       "idempotency_key": idempotency_key, "ordinal": ordinal,
                       "payload_sha256": payload_sha256,
                       "delivery_status": CommandDelivery.PREPARED.value,
                       "native_command_id": "", "native_turn_id": "",
                       "native_response_message_id": "",
                       "prepared_at": prepared, "sent_at": "",
                       "acknowledged_at": "", "completed_at": ""}
            self._connection.execute(
                "INSERT INTO commands VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (command_id, execution_id, attempt_id, binding_id or None,
                 idempotency_key, ordinal, payload_sha256,
                 CommandDelivery.PREPARED.value, "", "", prepared, "", "", "",
                 canonical(payload)),
            )
            return payload

    def command(self, command_id):
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM commands WHERE command_id=?", (command_id,)
            ).fetchone()
            return json.loads(row[0]) if row else None

    def mark_command_aborted(self, command_id, *, reason):
        """Make an aborted in-flight command's late response non-publishable.

        The delivery lifecycle is unchanged; the payload marker records that
        the owning runtime aborted (or attempted to abort) this command after
        a technical failure, so a late native assistant response correlated to
        it must never be adopted as the current result.
        """
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM commands WHERE command_id=?", (command_id,)
            ).fetchone()
            if row is None:
                raise RepositoryError("COMMAND_NOT_FOUND")
            payload = json.loads(row[0])
            __import__("control_epoch").assert_execution(self, payload["execution_id"])
            if payload.get("aborted"):
                return payload
            payload["aborted"] = {
                "reason": str(reason)[:200],
                "recorded_at": now(),
            }
            self._connection.execute(
                "UPDATE commands SET payload=? WHERE command_id=?",
                (canonical(payload), command_id),
            )
            return payload

    def unconfirmed_writer_quiescence(self, execution_id, *, binding_id=""):
        """Latest writer-quiescence evidence for an execution, if unconfirmed.

        Returns the recorded quiescence evidence dict when the most recent
        runtime recovery for the execution (optionally narrowed to a binding)
        closed a native writer without a bounded terminal/idle/lost
        confirmation; otherwise returns ``None``.
        """
        with self.hold():
            clauses = ["execution_id=?"]
            params = [execution_id]
            if binding_id:
                clauses.append("binding_id=?")
                params.append(binding_id)
            row = self._connection.execute(
                "SELECT payload FROM runtime_recoveries WHERE " + " AND ".join(clauses)
                + " ORDER BY ordinal DESC LIMIT 1", tuple(params),
            ).fetchone()
            if row is None:
                return None
            payload = json.loads(row[0])
            quiescence = dict(
                dict(payload.get("evidence") or {}).get("writer_quiescence") or {}
            )
            if quiescence and quiescence.get("quiesced") is not True:
                return quiescence
            return None

    def set_command_delivery(self, command_id, status, *, native_command_id="", native_turn_id="",
                             native_response_message_id=""):
        from runtime_adapter import CommandDelivery
        allowed = {
            CommandDelivery.PREPARED.value: {CommandDelivery.SENT.value, CommandDelivery.FAILED_BEFORE_SEND.value},
            CommandDelivery.SENT.value: {CommandDelivery.ACKNOWLEDGED.value, CommandDelivery.UNKNOWN.value},
            CommandDelivery.UNKNOWN.value: {CommandDelivery.ACKNOWLEDGED.value, CommandDelivery.COMPLETED.value},
            CommandDelivery.ACKNOWLEDGED.value: {CommandDelivery.COMPLETED.value},
        }
        if status not in {item.value for item in CommandDelivery}:
            raise RepositoryError("COMMAND_DELIVERY_STATUS_INVALID")
        with self.hold():
            row = self._connection.execute(
                "SELECT delivery_status,payload FROM commands WHERE command_id=?", (command_id,)
            ).fetchone()
            if row is None:
                raise RepositoryError("COMMAND_NOT_FOUND")
            payload = json.loads(row[1])
            __import__("control_epoch").assert_execution(self, payload["execution_id"])
            for key, incoming in (("native_command_id", native_command_id),
                                  ("native_turn_id", native_turn_id),
                                  ("native_response_message_id", native_response_message_id)):
                if not isinstance(incoming, str):
                    raise RepositoryError("COMMAND_NATIVE_IDENTITY_INVALID")
                prior = payload.get(key, "")
                if incoming and ((prior and incoming != prior) or
                                 (row[0] == status and incoming != prior)):
                    raise RepositoryError("COMMAND_NATIVE_IDENTITY_CONFLICT")
            if row[0] == status:
                return payload
            if status not in allowed.get(row[0], set()):
                raise RepositoryError("COMMAND_DELIVERY_TRANSITION_INVALID")
            stamp = now()
            payload["delivery_status"] = status
            if status == CommandDelivery.SENT.value:
                payload["sent_at"] = stamp
            elif status == CommandDelivery.ACKNOWLEDGED.value:
                payload["acknowledged_at"] = stamp
            elif status == CommandDelivery.COMPLETED.value:
                payload["completed_at"] = stamp
            payload["native_command_id"] = native_command_id or payload.get("native_command_id", "")
            payload["native_turn_id"] = native_turn_id or payload.get("native_turn_id", "")
            payload["native_response_message_id"] = (
                native_response_message_id
                or payload.get("native_response_message_id", "")
            )
            self._connection.execute(
                "UPDATE commands SET delivery_status=?,native_command_id=?,native_turn_id=?,sent_at=?,acknowledged_at=?,completed_at=?,payload=? WHERE command_id=?",
                (status, payload["native_command_id"], payload["native_turn_id"],
                 payload["sent_at"], payload["acknowledged_at"], payload["completed_at"],
                 canonical(payload), command_id),
            )
            return payload

    def attach_command_binding(self, command_id, binding_id):
        with self.hold():
            command = self._connection.execute(
                "SELECT execution_id,binding_id,payload FROM commands WHERE command_id=?", (command_id,)
            ).fetchone()
            binding = self._connection.execute(
                "SELECT execution_id FROM native_sessions WHERE binding_id=?", (binding_id,)
            ).fetchone()
            if command is None or binding is None or command[0] != binding[0]:
                raise RepositoryError("COMMAND_SESSION_EXECUTION_MISMATCH")
            if command[1] and command[1] != binding_id:
                raise RepositoryError("COMMAND_SESSION_REBIND_FORBIDDEN")
            __import__("control_epoch").assert_execution(self, command[0])
            payload = json.loads(command[2])
            payload["binding_id"] = binding_id
            self._connection.execute(
                "UPDATE commands SET binding_id=?,payload=? WHERE command_id=?",
                (binding_id, canonical(payload), command_id),
            )
            return payload

    def bind_native_turn(self, binding_id, command_id, native_turn_id, *, state="RUNNING"):
        valid = {"SUBMITTED", "RUNNING", "WAITING_INPUT", "SUCCEEDED", "FAILED", "INTERRUPTED", "UNKNOWN"}
        if state not in valid or not native_turn_id:
            raise RepositoryError("NATIVE_TURN_INVALID")
        with self.hold():
            command = self._connection.execute(
                "SELECT binding_id,payload FROM commands WHERE command_id=?", (command_id,)
            ).fetchone()
            if command is None or str(command[0] or "") != binding_id:
                raise RepositoryError("TURN_COMMAND_BINDING_MISMATCH")
            __import__("control_epoch").assert_execution(self, json.loads(command[1])["execution_id"])
            existing = self._connection.execute(
                "SELECT payload FROM native_turns WHERE command_id=?", (command_id,)
            ).fetchone()
            if existing:
                payload = json.loads(existing[0])
                if payload["native_turn_id"] != native_turn_id:
                    raise RepositoryError("NATIVE_TURN_REBIND_FORBIDDEN")
                return payload
            turn_id = "TURN-" + uuid.uuid4().hex.upper()
            started = now()
            payload = {"turn_id": turn_id, "binding_id": binding_id,
                       "command_id": command_id, "native_turn_id": native_turn_id,
                       "state": state, "started_at": started, "finished_at": ""}
            self._connection.execute(
                "INSERT INTO native_turns VALUES(?,?,?,?,?,?,?,?)",
                (turn_id, binding_id, command_id, native_turn_id, state, started, "", canonical(payload)),
            )
            return payload

    def reconcile_command(self, execution_id):
        """Return a safe next action; an uncertain send is never resendable."""
        from runtime_adapter import CommandDelivery
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM commands WHERE execution_id=? ORDER BY ordinal DESC LIMIT 1",
                (execution_id,),
            ).fetchone()
            binding = self.session_binding(execution_id)
            if row is None:
                return {"action": "SUBMIT_NEW_COMMAND", "binding": binding, "command": None}
            command = json.loads(row[0])
            status = command["delivery_status"]
            if status in {CommandDelivery.SENT.value, CommandDelivery.UNKNOWN.value}:
                return {"action": "QUERY_SESSION_DO_NOT_RESEND", "binding": binding, "command": command}
            if status == CommandDelivery.ACKNOWLEDGED.value:
                return {"action": "RESUME_OR_QUERY_BOUND_SESSION", "binding": binding, "command": command}
            if status == CommandDelivery.COMPLETED.value:
                return {"action": "SUBMIT_NEXT_TURN", "binding": binding, "command": command}
            return {"action": "SEND_PREPARED_COMMAND", "binding": binding, "command": command}

    def record_runtime_recovery(self, execution_id, attempt_id, *, failure_code,
                                binding_id="", state="STARTED", evidence=None):
        valid = {"STARTED", "RESUMED", "FRESH_ATTEMPT_REQUIRED", "EXHAUSTED", "FAILED"}
        if state not in valid:
            raise RepositoryError("RUNTIME_RECOVERY_STATE_INVALID")
        with self.hold():
            __import__("control_epoch").assert_execution(self, execution_id)
            ordinal = int(self._connection.execute(
                "SELECT COALESCE(MAX(ordinal),0)+1 FROM runtime_recoveries WHERE execution_id=?",
                (execution_id,),
            ).fetchone()[0])
            recovery_id = "REC-" + uuid.uuid4().hex.upper()
            payload = {"recovery_id": recovery_id, "execution_id": execution_id,
                       "attempt_id": attempt_id, "binding_id": binding_id,
                       "failure_code": failure_code, "ordinal": ordinal,
                       "state": state, "retry_domain": "RUNTIME_RECOVERY",
                       "product_retry_delta": 0, "recorded_at": now(),
                       "evidence": copy.deepcopy(evidence or {})}
            self._connection.execute(
                "INSERT INTO runtime_recoveries VALUES(?,?,?,?,?,?,?,?,?,?)",
                (recovery_id, execution_id, attempt_id, binding_id or None,
                 failure_code, ordinal, state, 0, payload["recorded_at"], canonical(payload)),
            )
            return payload

    def record_inbound_event(self, execution_id, attempt_id, *, binding_id, runtime,
                             provider_event_id="", native_session_id="",
                             native_turn_id="", event_type, sequence="", cursor="",
                             payload_sha256, detail=None):
        if not runtime or not event_type or len(str(payload_sha256)) != 64:
            raise RepositoryError("INBOUND_EVENT_IDENTITY_INVALID")
        identity = {
            "runtime": str(runtime), "provider_event_id": str(provider_event_id),
            "native_session_id": str(native_session_id),
            "native_turn_id": str(native_turn_id), "event_type": str(event_type),
            "sequence": str(sequence), "cursor": str(cursor),
            "payload_sha256": str(payload_sha256),
        }
        fingerprint = digest(identity)
        with self.hold():
            __import__("control_epoch").assert_execution(self, execution_id)
            duplicate = self._connection.execute(
                "SELECT payload FROM inbound_events WHERE event_fingerprint=?", (fingerprint,)
            ).fetchone()
            if duplicate:
                previous = json.loads(duplicate[0])
                if (previous.get("execution_id"), previous.get("attempt_id"), previous.get("binding_id")) != (execution_id, attempt_id, binding_id):
                    raise RepositoryError("INBOUND_EVENT_BINDING_MISMATCH")
                return {**previous, "duplicate": True}
            if provider_event_id:
                conflict = self._connection.execute(
                    "SELECT event_fingerprint FROM inbound_events "
                    "WHERE runtime=? AND provider_event_id=?",
                    (runtime, provider_event_id),
                ).fetchone()
                if conflict and conflict[0] != fingerprint:
                    raise RepositoryError("INBOUND_EVENT_ID_CONFLICT")
            binding = self._connection.execute(
                "SELECT execution_id,native_session_id FROM native_sessions WHERE binding_id=?",
                (binding_id,),
            ).fetchone()
            attempt = self._connection.execute(
                "SELECT execution_id FROM attempts WHERE attempt_id=?", (attempt_id,)
            ).fetchone()
            if (binding is None or attempt is None or binding["execution_id"] != execution_id
                    or attempt["execution_id"] != execution_id):
                raise RepositoryError("INBOUND_EVENT_BINDING_MISMATCH")
            if native_session_id and native_session_id != binding["native_session_id"]:
                raise RepositoryError("INBOUND_EVENT_SESSION_MISMATCH")
            latest_attempt = self._connection.execute(
                "SELECT attempt_id FROM attempts WHERE execution_id=? "
                "ORDER BY started_at DESC,rowid DESC LIMIT 1", (execution_id,),
            ).fetchone()
            latest_turn = self._connection.execute(
                "SELECT native_turn_id FROM commands WHERE attempt_id=? AND native_turn_id!='' "
                "ORDER BY ordinal DESC LIMIT 1", (attempt_id,),
            ).fetchone()
            ambiguous = not (provider_event_id or sequence or cursor)
            late = bool(latest_attempt and latest_attempt[0] != attempt_id) or bool(
                native_turn_id and latest_turn and latest_turn[0] != native_turn_id
            )
            status = "LATE" if late else "AMBIGUOUS" if ambiguous else "RECEIVED"
            event_id = "EVT-" + uuid.uuid4().hex.upper()
            received = now()
            record = {
                "event_id": event_id, "execution_id": execution_id,
                "attempt_id": attempt_id, "binding_id": binding_id,
                **identity, "event_fingerprint": fingerprint, "status": status,
                "received_at": received, "applied_revision": None,
                "applied_state": "", "detail": copy.deepcopy(detail or {}),
                "duplicate": False,
            }
            self._connection.execute(
                "INSERT INTO inbound_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (event_id, execution_id, attempt_id, binding_id, runtime,
                 str(provider_event_id), str(native_session_id or binding["native_session_id"]),
                 str(native_turn_id), str(event_type), str(sequence), str(cursor),
                 str(payload_sha256), fingerprint, status, received, None, "", canonical(record)),
            )
            return record

    def apply_inbound_event(self, event_id, *, applied_revision, applied_state):
        if type(applied_revision) is not int or applied_revision < 0 or not isinstance(applied_state, str) or not applied_state:
            raise RepositoryError("INBOUND_EVENT_APPLICATION_INVALID")
        with self.hold():
            row = self._connection.execute(
                "SELECT status,attempt_id,execution_id,payload FROM inbound_events WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row is None:
                raise RepositoryError("INBOUND_EVENT_NOT_FOUND")
            __import__("control_epoch").assert_execution(self, row["execution_id"])
            record = json.loads(row["payload"])
            if row["status"] == "APPLIED":
                if (record.get("applied_revision") == int(applied_revision)
                        and record.get("applied_state") == str(applied_state)):
                    return record
                raise RepositoryError("INBOUND_EVENT_ALREADY_APPLIED")
            if row["status"] in {"AMBIGUOUS", "LATE"}:
                raise RepositoryError("INBOUND_EVENT_RECONCILIATION_REQUIRED")
            latest = self._connection.execute(
                "SELECT attempt_id FROM attempts WHERE execution_id=? "
                "ORDER BY started_at DESC,rowid DESC LIMIT 1", (row["execution_id"],),
            ).fetchone()
            if latest is None or latest[0] != row["attempt_id"]:
                self._connection.execute(
                    "UPDATE inbound_events SET status='LATE',payload=? WHERE event_id=?",
                    (canonical({**record, "status": "LATE"}), event_id),
                )
                raise RepositoryError("INBOUND_EVENT_RECONCILIATION_REQUIRED")
            record.update({"status": "APPLIED", "applied_revision": int(applied_revision),
                           "applied_state": str(applied_state)})
            self._connection.execute(
                "UPDATE inbound_events SET status='APPLIED',applied_revision=?,"
                "applied_state=?,payload=? WHERE event_id=? AND status='RECEIVED'",
                (int(applied_revision), str(applied_state), canonical(record), event_id),
            )
            return record

    def record_decision(self, job_id, *, contract_revision, decision_type, question,
                        fingerprint="", payload=None):
        valid = {"CANDIDATE", "CONTRACT", "POLICY", "DEFERRED"}
        decision_type = str(decision_type).upper()
        if decision_type not in valid or not str(question).strip():
            raise RepositoryError("DECISION_INVALID")
        protected = {"decision_id", "job_id", "contract_revision", "decision_type", "status",
                     "question", "resolution", "fingerprint", "created_at", "resolved_at"}
        if not isinstance(payload or {}, dict) or protected.intersection(payload or {}):
            raise RepositoryError("DECISION_PAYLOAD_AUTHORITY_OVERRIDE")
        fingerprint = fingerprint or digest({"type": decision_type, "question": question})
        with self.hold():
            job = self._connection.execute(
                "SELECT contract_revision FROM logical_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if job is None:
                raise RepositoryError("JOB_NOT_FOUND")
            if int(contract_revision) != int(job[0]):
                raise RepositoryError("STALE_DECISION_CONTRACT")
            existing = self._connection.execute(
                "SELECT payload FROM decisions WHERE job_id=? AND contract_revision=? AND fingerprint=?",
                (job_id, int(contract_revision), fingerprint),
            ).fetchone()
            if existing:
                return json.loads(existing[0])
            decision_id = "DEC-" + uuid.uuid4().hex.upper()
            record = {"decision_id": decision_id, "job_id": job_id,
                      "contract_revision": int(contract_revision), "decision_type": decision_type,
                      "status": "OPEN", "question": str(question).strip(), "resolution": "",
                      "fingerprint": fingerprint, "created_at": now(), "resolved_at": "",
                      **copy.deepcopy(payload or {})}
            self._connection.execute(
                "INSERT INTO decisions VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (decision_id, job_id, int(contract_revision), decision_type, "OPEN",
                 record["question"], "", fingerprint, record["created_at"], "", canonical(record)),
            )
            for execution in self._connection.execute(
                "SELECT execution_id FROM executions WHERE job_id=? AND phase!='TERMINAL'",
                (job_id,),
            ).fetchall():
                self._set_writer_authority_locked(
                    execution["execution_id"], "SUSPENDED", "PENDING_DECISION"
                )
            self.project_product_readiness(job_id)
            return record

    def resolve_decision(self, decision_id, *, resolution, expected_status="OPEN"):
        with self.hold():
            row = self._connection.execute(
                "SELECT status,payload FROM decisions WHERE decision_id=?", (decision_id,)
            ).fetchone()
            if row is None:
                raise RepositoryError("DECISION_NOT_FOUND")
            record = json.loads(row[1])
            if row[0] == "RESOLVED":
                if record.get("resolution") == resolution:
                    return record
                raise RepositoryError("DECISION_RESOLUTION_CONFLICT")
            if row[0] != expected_status:
                raise RepositoryError("STALE_DECISION")
            record.update({"status": "RESOLVED", "resolution": str(resolution),
                           "resolved_at": now()})
            self._connection.execute(
                "UPDATE decisions SET status='RESOLVED',resolution=?,resolved_at=?,payload=? WHERE decision_id=? AND status=?",
                (record["resolution"], record["resolved_at"], canonical(record), decision_id, expected_status),
            )
            self.project_product_readiness(record["job_id"])
            return record

    def resume_writer_after_decision(self, execution_id, decision_id, *, contract_revision):
        """Re-authorize a resolved same-contract question after writer fencing.

        No native status is a quiescence proof. The Harness boundary owner must
        have persisted its independent fencing evidence before this operation.
        Job/Attempt terminal states are never changed here.
        """
        with self.hold():
            __import__("control_epoch").assert_execution(self, execution_id)
            if self.global_stop().get("active"):
                raise RepositoryError("GLOBAL_STOP")
            execution = self._connection.execute(
                "SELECT phase,payload FROM executions WHERE execution_id=?", (execution_id,)
            ).fetchone()
            decision = self._connection.execute(
                "SELECT status,payload FROM decisions WHERE decision_id=?", (decision_id,)
            ).fetchone()
            if not execution or execution[0] == "TERMINAL" or not decision or decision[0] != "RESOLVED":
                raise RepositoryError("QA_CONTINUATION_NOT_AUTHORIZED")
            frozen, resolved = json.loads(execution[1]), json.loads(decision[1])
            if (frozen["job_id"] != resolved["job_id"]
                    or int(frozen["contract_revision"]) != int(contract_revision)
                    or int(resolved["contract_revision"]) != int(contract_revision)
                    or self.open_decisions(frozen["job_id"])):
                raise RepositoryError("QA_CONTINUATION_CONTRACT_MISMATCH")
            recovery = self._connection.execute(
                "SELECT payload FROM runtime_recoveries WHERE execution_id=? ORDER BY ordinal DESC LIMIT 1",
                (execution_id,),
            ).fetchone()
            evidence = json.loads(recovery[0]).get("evidence", {}) if recovery else {}
            if (evidence.get("writer_quiescence", {}).get("quiesced") is not True
                    or digest(evidence.get("binding", {})) != resolved.get("runtime_binding")
                    or resolved.get("writer_quiesced") is not True):
                raise RepositoryError("WRITER_QUIESCENCE_REQUIRED")
            authority = self.writer_authority(execution_id)
            if not authority or authority["state"] != "SUSPENDED" or authority["reason"] != "PENDING_DECISION":
                raise RepositoryError("QA_CONTINUATION_AUTHORITY_MISMATCH")
            return self._set_writer_authority_locked(execution_id, "ACTIVE", "RESOLVED_DECISION_FENCED")

    def open_decisions(self, job_id):
        with self.hold():
            return [json.loads(row[0]) for row in self._connection.execute(
                "SELECT payload FROM decisions WHERE job_id=? AND status='OPEN' ORDER BY created_at,decision_id",
                (job_id,),
            )]

    def decisions(self, job_id, status=""):
        with self.hold():
            sql = "SELECT payload FROM decisions WHERE job_id=?"
            params = [job_id]
            if status:
                sql += " AND status=?"
                params.append(status)
            sql += " ORDER BY created_at,decision_id"
            return [json.loads(row[0]) for row in self._connection.execute(sql, tuple(params))]

    def project_product_readiness(self, job_id):
        with self.hold():
            row = self._connection.execute(
                "SELECT status,contract_revision,payload FROM logical_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if row is None:
                raise RepositoryError("JOB_NOT_FOUND")
            open_items = self.open_decisions(job_id)
            from product_readiness import derive_product_readiness
            derived = derive_product_readiness(row[0], open_items)
            readiness = derived["product_readiness"]
            projection = {"job_id": job_id, "contract_revision": int(row[1]),
                          "readiness": readiness,
                          "user_action_required": derived["user_action_required"],
                          "open_decision_count": derived["open_decision_count"],
                          "open_decisions": derived["open_decisions"], "updated_at": now()}
            self._connection.execute(
                "INSERT INTO product_readiness VALUES(?,?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET contract_revision=excluded.contract_revision,readiness=excluded.readiness,user_action_required=excluded.user_action_required,open_decision_count=excluded.open_decision_count,updated_at=excluded.updated_at,payload=excluded.payload",
                (job_id, int(row[1]), readiness, int(bool(open_items)), len(open_items),
                 projection["updated_at"], canonical(projection)),
            )
            return projection

    def product_readiness(self, job_id):
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM product_readiness WHERE job_id=?", (job_id,)
            ).fetchone()
            if row:
                return json.loads(row[0])
            logical = self._connection.execute(
                "SELECT contract_revision,payload FROM logical_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if logical is not None and not self.read_only:
                return self.project_product_readiness(job_id)
            job = json.loads(logical[1]) if logical is not None else self.read_job(job_id)
            open_items = self.open_decisions(job_id) if logical is not None else []
            from product_readiness import derive_product_readiness, false_complete_guard
            derived = derive_product_readiness(str(job.get("status", "")), open_items)
            false_complete_guard(str(job.get("status", "")), derived)
            return {
                "job_id": job_id,
                "contract_revision": int(
                    logical[0] if logical is not None else job.get("contract_revision", 1)
                ),
                "readiness": derived["product_readiness"],
                "user_action_required": derived["user_action_required"],
                "open_decision_count": derived["open_decision_count"],
                "open_decisions": derived["open_decisions"],
                "updated_at": str(job.get("updated_at") or job.get("created_at") or ""),
            }

    def record_validation(self, execution_id, *, kind, disposition, evidence_ref="",
                          product_retry=False, detail=None):
        from runtime_adapter import ValidationDisposition
        dispositions = {item.value for item in ValidationDisposition}
        if disposition not in dispositions:
            raise RepositoryError("VALIDATION_DISPOSITION_INVALID")
        if product_retry and disposition != ValidationDisposition.FAIL.value:
            raise RepositoryError("VALIDATION_NOT_PRODUCT_FAILURE")
        with self.hold():
            __import__("control_epoch").assert_execution(self, execution_id)
            validation_id = "VAL-" + uuid.uuid4().hex.upper()
            payload = {"validation_id": validation_id, "execution_id": execution_id,
                       "kind": kind, "disposition": disposition,
                       "product_retry_delta": int(bool(product_retry)),
                       "evidence_ref": evidence_ref, "recorded_at": now(),
                       "detail": copy.deepcopy(detail or {})}
            existing = self._connection.execute(
                "SELECT payload FROM validation_dispositions WHERE execution_id=? AND kind=?",
                (execution_id, kind),
            ).fetchone()
            if existing:
                prior = json.loads(existing[0])
                if (prior["disposition"] == disposition and prior["evidence_ref"] == evidence_ref
                        and prior.get("detail", {}) == (detail or {})
                        and prior.get("product_retry_delta", 0) == int(bool(product_retry))):
                    return prior
                raise RepositoryError("VALIDATION_DISPOSITION_IMMUTABLE")
            self._connection.execute(
                "INSERT INTO validation_dispositions VALUES(?,?,?,?,?,?,?,?)",
                (validation_id, execution_id, kind, disposition, int(bool(product_retry)),
                 evidence_ref, payload["recorded_at"], canonical(payload)),
            )
            return payload

    def record_verification_evidence(self, execution_id, attempt_id, *, scenario_id,
                                     candidate_hash, contract_revision, environment_hash,
                                     result, artifacts, machine_observed,
                                     verification_depth="", failed_step="",
                                     started_at="", completed_at="", detail=None):
        from verification_contract import validate_evidence, VerificationContractError
        with self.hold():
            __import__("control_epoch").assert_execution(self, execution_id)
            execution = self._connection.execute(
                "SELECT contract_revision,payload FROM executions WHERE execution_id=?",
                (execution_id,),
            ).fetchone()
            attempt = self._connection.execute(
                "SELECT execution_id,candidate_hash FROM attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()
            if execution is None or attempt is None or attempt["execution_id"] != execution_id:
                raise RepositoryError("VERIFICATION_EXECUTION_BINDING_INVALID")
            frozen = json.loads(execution["payload"])
            contract = dict(frozen.get("verification_contract") or {
                "depth": verification_depth or "STATIC"
            })
            expected_candidate = str(attempt["candidate_hash"] or candidate_hash)
            expected_environment = str(
                frozen.get("verification_environment_hash") or environment_hash
            )
            try:
                validate_evidence(
                    contract=contract, candidate_hash=expected_candidate,
                    contract_revision=int(execution["contract_revision"]),
                    environment_hash=expected_environment, result=result,
                    artifacts=list(artifacts or []), machine_observed=bool(machine_observed),
                    supplied_candidate_hash=str(candidate_hash),
                    supplied_contract_revision=int(contract_revision),
                    supplied_environment_hash=str(environment_hash),
                )
            except VerificationContractError as exc:
                raise RepositoryError(exc.code) from exc
            verification_id = "VER-" + uuid.uuid4().hex.upper()
            started = str(started_at or now())
            completed = str(completed_at or now())
            depth = str(verification_depth or contract.get("depth") or "STATIC")
            payload = {
                "verification_id": verification_id, "execution_id": execution_id,
                "attempt_id": attempt_id, "scenario_id": str(scenario_id),
                "candidate_hash": str(candidate_hash),
                "contract_revision": int(contract_revision),
                "environment_hash": str(environment_hash),
                "verification_depth": depth, "result": str(result).upper(),
                "failed_step": str(failed_step), "started_at": started,
                "completed_at": completed, "artifacts": copy.deepcopy(list(artifacts or [])),
                "machine_observed": bool(machine_observed),
                "human_trace_provenance": copy.deepcopy(
                    dict(detail or {}).get("human_trace_provenance") or {}
                ),
                "auto_e2e_generation": False,
                "detail": copy.deepcopy(detail or {}),
            }
            try:
                self._connection.execute(
                    "INSERT INTO verification_evidence VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (verification_id, execution_id, attempt_id, str(scenario_id),
                     str(candidate_hash), int(contract_revision), str(environment_hash), depth,
                     str(result).upper(), str(failed_step), started, completed, canonical(payload)),
                )
            except sqlite3.IntegrityError as exc:
                raise RepositoryError("VERIFICATION_EVIDENCE_ALREADY_RECORDED") from exc
            return payload

    def active_execution(self, job_id):
        with self.hold():
            row = self._connection.execute("SELECT payload FROM executions WHERE job_id=? AND phase!='TERMINAL'", (job_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def execution(self, execution_id):
        with self.hold():
            row = self._connection.execute(
                "SELECT payload FROM executions WHERE execution_id=?", (execution_id,)
            ).fetchone()
            return json.loads(row[0]) if row else None


def legacy_projection(root, active_ids):
    root = Path(root)
    queue = json.loads((root / "queue.json").read_text(encoding="utf-8"))
    jobs = {job_id: json.loads((root / "jobs" / (job_id + ".json")).read_text(encoding="utf-8")) for job_id in active_ids}
    return {"queue": queue, "jobs": jobs}


def field_diff(before, after, path="$"):
    if isinstance(before, dict) and isinstance(after, dict):
        differences = []
        for key in sorted(set(before) | set(after)):
            if key not in before or key not in after:
                differences.append({"field": path + "." + key, "before": before.get(key), "after": after.get(key)})
            else:
                differences.extend(field_diff(before[key], after[key], path + "." + key))
        return differences
    return [] if before == after else [{"field": path, "before": before, "after": after}]
