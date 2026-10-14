"""Administrator migration loop: frozen sources, bounded batches, native checks.

No database, scheduler, or exact resume state. A rerun reuses immutable snapshots
and native identity/content readback. All prepared batches pass preflight before
any target write; the staging directory is disposable, private local data.
"""
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
import resource
import shutil
import signal
import sys
import threading
import time
import traceback
import uuid

from snapshot import canonical, digest, private_write, read_snapshot, save_snapshot, strict_loads, private_jsonl
from loop_source import LoopSource, limits


# A native validation command can fail transiently while Kubernetes is replacing
# a Pod or its API connection.  This is deliberately separate from a validator
# response of {"verified": false}: the latter is a deterministic verdict on
# historical data and must be handled by the per-record skip path instead.
NATIVE_PREFLIGHT_ATTEMPTS = 3
NATIVE_PREFLIGHT_RETRY_SECONDS = 3


def atomic_json(path, value):
    temporary = path.with_suffix('.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        stream.write(canonical(value) + '\n')
    os.replace(temporary, path)


class Progress:
    def __init__(self, directory):
        self.directory = directory
        self.started = time.monotonic()
        self.values = {'phase': 'precheck', 'source_records': 0, 'prepared_batches': 0,
                       'completed_batches': 0, 'verified_records': 0}
        self.stop = threading.Event()
        self.lock = threading.RLock()
        self.thread = threading.Thread(target=self.heartbeat, daemon=True)

    def __enter__(self):
        self.update()
        self.thread.start()
        return self

    def update(self, **values):
        with self.lock:
            self.values.update(values)
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            self.values.update(utc=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                               elapsed_seconds=round(time.monotonic() - self.started, 1),
                               peak_rss_bytes=peak if sys.platform == 'darwin' else peak * 1024)
            atomic_json(self.directory / 'progress.json', self.values)
            print(canonical(self.values), file=sys.stderr, flush=True)

    def heartbeat(self):
        while not self.stop.wait(5):
            self.update()

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()

    def error(self, error):
        # Never serialize repr/locals or arbitrary SQL/native exception strings.
        raw = str(error)
        prefixes = ('source_', 'snapshot_', 'core_', 'agent_', 'retained_', 'thread_', 'native_',
                    'dependency_', 'historical_', 'invalid_history_', 'migration_', 'SQL_')
        safe = raw.split(':', 1)[0]
        if not safe.startswith(prefixes) or not safe.replace('_', '').isalnum():
            safe = 'private_detail_redacted'
        frames = [{'file': Path(frame.filename).name, 'function': frame.name, 'line': frame.lineno}
                  for frame in traceback.extract_tb(error.__traceback__)]
        detail = {'phase': self.values['phase'],
                  'exception_class': type(error).__name__, 'message': safe, 'stack': frames,
                  'batch': self.values.get('batch'), 'source_snapshots': 'reuse frozen snapshots on rerun'}
        # Persist identifiers only; never expose message bodies, checkpoints or blobs.
        fields = ('reason', 'source_kind', 'source_id', 'source_message_id',
                  'thread_id', 'task_id', 'conversation_id', 'interaction_id')
        issues = []
        for issue in getattr(error, 'thread_issues', ()):
            if isinstance(issue, dict):
                safe_issue = {key: issue[key] for key in fields
                              if key in issue and isinstance(issue[key], (str, int, float, bool, type(None)))}
                if safe_issue:
                    issues.append(safe_issue)
        if issues:
            detail['thread_issue_count'] = len(issues)
            detail['thread_issues'] = issues
        atomic_json(self.directory / 'error.json', detail)
        self.update(error_file=str(self.directory / 'error.json'))


@contextmanager
def interruption(directory):
    previous = {}
    def stop(signum, frame):
        print('Migration interrupted; rerun using frozen sources in ' + str(directory.parent),
              file=sys.stderr, flush=True)
        raise InterruptedError('migration_interrupted')
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, stop)
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def freeze(runtime, state_root, deployment, before, progress):
    from core_snapshot import export_core, read_core_backup
    from agent_history import export_agent_source
    instance = state_root / deployment['instance']
    source = instance / 'source'
    progress.update(phase='source_snapshot', exported_records=0)
    if not source.exists():
        records, tables = runtime.snapshot()
        save_snapshot(source, records, deployment['instance'], {'tables': tables, 'deployment': deployment},
                      on_record=lambda n: progress.update(exported_records=n))
    manifest = strict_loads((source / 'snapshot.json').read_text())
    frozen = manifest.get('metadata', {}).get('deployment', {})
    if frozen.get('cluster_uid') != deployment['cluster_uid'] or manifest['source_deployment'] != deployment['instance']:
        raise ValueError('snapshot_deployment_mismatch')
    # Cutover is stable across processes, including reruns with an unset variable.
    cutover_file = instance / 'cutover.json'
    if cutover_file.exists():
        fixed = strict_loads(cutover_file.read_text())['before']
        if os.environ.get('BKN_HISTORY_BEFORE') and before != fixed:
            raise ValueError('historical_cutover_snapshot_conflict')
        before = fixed
    else:
        private_write(cutover_file, canonical({'before': before}).encode())
    backup = os.environ.get('BKN_HISTORY_CORE_BACKUP')
    capture_key = digest(canonical([before, str(Path(backup).resolve()) if backup else 'live']).encode())[:16]
    core = instance / ('core-source-' + capture_key)
    progress.update(phase='core_snapshot')
    if not core.exists():
        records, metadata = read_core_backup(backup, stream=True) if backup else export_core(runtime.source, stream=True)
        metadata.update(before=before, cluster_uid=deployment['cluster_uid'])
        save_snapshot(core, records, deployment['instance'], metadata,
                      on_record=lambda n: progress.update(exported_records=n))
    agent = instance / ('agent-source-' + digest(before.encode())[:16])
    progress.update(phase='agent_snapshot')
    if not agent.exists():
        records, tables = export_agent_source(runtime.source, before)
        save_snapshot(agent, records, deployment['instance'],
                      {'before': before, 'cluster_uid': deployment['cluster_uid'], 'tables': tables},
                      on_record=lambda n: progress.update(exported_records=n))
    for path in (core, agent):
        meta = strict_loads((path / 'snapshot.json').read_text())
        if (meta['source_deployment'] != deployment['instance'] or
                meta['metadata'].get('before') != before or
                meta['metadata'].get('cluster_uid') != deployment['cluster_uid']):
            raise ValueError('snapshot_deployment_mismatch')
    return [source, core, agent], before


def _native_preflight(runtime, flag, data):
    """Return a safe, private diagnostic for one native validation request."""
    for attempt in range(1, NATIVE_PREFLIGHT_ATTEMPTS + 1):
        try:
            raw = runtime._native([flag], data)
            break
        except (OSError, ValueError) as error:
            # run_upgrade.py can be loaded both as __main__ and as a module.
            # Do not rely on exception-class identity across those two module
            # objects; command failures carry subprocess result attributes.
            command_error = (isinstance(error, OSError) or
                             all(hasattr(error, field) for field in ('returncode', 'stdout', 'stderr')))
            if not command_error:
                raise
            if attempt < NATIVE_PREFLIGHT_ATTEMPTS:
                time.sleep(NATIVE_PREFLIGHT_RETRY_SECONDS)
                continue
            detail = {
                'reason': 'native_validator_command_failed',
                'native_attempts': attempt,
                'native_error_class': type(error).__name__,
            }
            if isinstance(error, OSError):
                detail['native_os_error_errno'] = error.errno
            else:
                detail.update({
                    'native_exit_code': error.returncode,
                    'native_stdout': error.stdout.decode(errors='replace'),
                    'native_stderr': error.stderr.decode(errors='replace'),
                })
            raise RuntimeError('native_validator_command_failed:' + canonical(detail)) from error
    try:
        answer = strict_loads(raw)
    except (TypeError, ValueError):
        raise RuntimeError('native_validator_invalid_response:' + canonical({
            'reason': 'native_validator_invalid_response',
            'native_stdout': raw[-16384:].decode(errors='replace'),
        }))
    if (not isinstance(answer, dict)
            or (answer.get('verified') is not True and answer.get('verified') is not False)):
        raise RuntimeError('native_validator_invalid_response:' + canonical({
            'reason': 'native_validator_invalid_response',
            'native_response': answer,
        }))
    if answer.get('verified') is True:
        return True, {'verified': True}
    return False, {
        'reason': answer.get('reason', 'native_core_preflight_rejected'),
        'native_response': answer,
    }


def _core_failure_stage(runtime, unit):
    """Locate the first rejected Core category without writing target data."""
    from core_history_batches import HISTORY_CORE_FIELDS
    original = strict_loads(unit)
    probe = {key: [] for key in HISTORY_CORE_FIELDS}
    for key in HISTORY_CORE_FIELDS:
        probe[key] = original[key]
        passed, detail = _native_preflight(runtime, '--validate-core-records', canonical(probe).encode())
        if not passed:
            return key, detail
    return 'complete_core_unit', {'reason': 'native_core_preflight_rejected'}


def _references_skipped(value, interaction_ids, operation_ids, receipt_ids):
    """Check structured relationship fields only; never search opaque content text."""
    keys = {
        'interaction_id': interaction_ids,
        'source_interaction_id': interaction_ids,
        'target_interaction_id': interaction_ids,
        'operation_id': operation_ids,
        'parent_operation_id': operation_ids,
        'receipt_id': receipt_ids,
    }
    if isinstance(value, dict):
        for key, candidate in value.items():
            if key in keys and isinstance(candidate, str) and candidate in keys[key]:
                return True
            if _references_skipped(candidate, interaction_ids, operation_ids, receipt_ids):
                return True
    elif isinstance(value, list):
        return any(_references_skipped(item, interaction_ids, operation_ids, receipt_ids) for item in value)
    elif isinstance(value, str) and value[:1] in '[{':
        try:
            return _references_skipped(strict_loads(value), interaction_ids, operation_ids, receipt_ids)
        except (TypeError, ValueError):
            return False
    return False


def _prune_skipped_core(plan, interaction_ids):
    """Remove every target-side dependent of rejected Interaction units."""
    interaction_ids = set(interaction_ids)
    core = plan['core']
    operation_ids = {row['operation_id'] for row in core['operations']
                     if row.get('interaction_id') in interaction_ids}
    receipt_ids = {row['receipt_id'] for row in core['receipts']
                   if row.get('interaction_id') in interaction_ids or row.get('operation_id') in operation_ids}
    before = {}
    def filter_rows(name, predicate):
        rows = plan[name] if name in plan else core[name]
        before[name] = len(rows)
        kept = [row for row in rows if not predicate(row)]
        if name in plan:
            plan[name] = kept
        else:
            core[name] = kept
    filter_rows('interactions', lambda row: row.get('interaction_id') in interaction_ids)
    filter_rows('operations', lambda row: row.get('interaction_id') in interaction_ids or row.get('operation_id') in operation_ids)
    filter_rows('receipts', lambda row: row.get('interaction_id') in interaction_ids or row.get('operation_id') in operation_ids or row.get('receipt_id') in receipt_ids)
    filter_rows('call_facts', lambda row: row.get('interaction_id') in interaction_ids or row.get('operation_id') in operation_ids or row.get('receipt_id') in receipt_ids)
    filter_rows('assembly_revisions', lambda row: row.get('interaction_id') in interaction_ids)
    filter_rows('idempotency_records', lambda row: row.get('resource_type') == 'interaction' and row.get('resource_id') in interaction_ids)
    for name in ('artifacts', 'ledger', 'historical_projections', 'explanations', 'mapped_records'):
        filter_rows(name, lambda row: _references_skipped(row, interaction_ids, operation_ids, receipt_ids))
    if 'thread_mappings' in plan:
        mappings = []
        for row in plan['thread_mappings']:
            copy = dict(row)
            copy['interaction_ids'] = [iid for iid in copy.get('interaction_ids', []) if iid not in interaction_ids]
            if copy['interaction_ids']:
                mappings.append(copy)
        plan['thread_mappings'] = mappings
    if 'thread_defaults' in plan:
        plan['thread_defaults'] = [row for row in plan['thread_defaults']
                                   if row.get('interaction_id') not in interaction_ids]
    if 'mappings' in plan:
        plan['mappings'] = [row for row in plan['mappings']
                            if not _references_skipped(row, interaction_ids, operation_ids, receipt_ids)]
    return {
        'interaction_ids': sorted(interaction_ids),
        'operation_ids': sorted(operation_ids),
        'receipt_ids': sorted(receipt_ids),
        'removed': {name: count - (len(plan[name]) if name in plan else len(core[name]))
                    for name, count in before.items()},
    }


def _stage_core_batches(runtime, plan, directory, progress):
    """Stage valid Core batches; isolate and retain only rejected Interaction units."""
    from core_history_batches import core_history_import_batches
    folder = directory / 'core'
    folder.mkdir(mode=0o700)
    progress.update(phase='preflight_core')
    number, skipped = 0, []
    for data in core_history_import_batches(plan['core'], max_bytes=limits()[1]):
        if len(data) > limits()[1]:
            raise ValueError('native_batch_exceeds_content_limit')
        passed, detail = _native_preflight(runtime, '--validate-core-records', data)
        if passed:
            private_write(folder / ('%08d.json' % number), data)
            number += 1
            continue
        # A rejected aggregate is decomposed into complete single-Interaction units.
        unit_plan = strict_loads(data)
        units = list(core_history_import_batches(unit_plan, max_bytes=limits()[1], max_interactions=1))
        for unit in units:
            unit_rows = strict_loads(unit)
            interaction_rows = unit_rows['interactions']
            unit_passed, unit_detail = _native_preflight(runtime, '--validate-core-records', unit)
            if unit_passed:
                private_write(folder / ('%08d.json' % number), unit)
                number += 1
                continue
            if len(interaction_rows) != 1:
                raise ValueError('native_core_noninteraction_preflight_failed')
            interaction = interaction_rows[0]
            stage, stage_detail = _core_failure_stage(runtime, unit)
            skipped.append({
                'reason': 'native_core_preflight_rejected',
                'interaction_id': interaction['interaction_id'],
                'conversation_id': interaction['conversation_id'],
                'failure_stage': stage,
                'aggregate_preflight': detail,
                'unit_preflight': unit_detail,
                'stage_preflight': stage_detail,
                'core_counts': {key: len(value) for key, value in unit_rows.items()},
            })
    return skipped


def _ledger_skip_detail(row, native_preflight):
    envelope = row.get('envelope')
    if isinstance(envelope, str):
        try:
            envelope = strict_loads(envelope)
        except (TypeError, ValueError):
            envelope = {}
    event = (envelope or {}).get('envelope', {})
    event = event.get('event', event)
    return {
        'reason': 'native_ledger_preflight_rejected',
        'interaction_id': row.get('interaction_id'),
        'conversation_id': row.get('conversation_id'),
        'ledger_id': row.get('ledger_id') or row.get('event_id') or row.get('id'),
        'event_type': event.get('event_type') or event.get('type'),
        'native_preflight': native_preflight,
    }


def _stage_ledger_batches(runtime, batches, directory, progress):
    """Stage valid Ledger rows; retain rejected historical events in source only."""
    folder = directory / 'ledger'
    folder.mkdir(mode=0o700)
    progress.update(phase='preflight_ledger')
    number, skipped = 0, []
    for data in batches:
        if len(data) > limits()[1]:
            raise ValueError('native_batch_exceeds_content_limit')
        passed, detail = _native_preflight(runtime, '--validate-ledger-records', data)
        if passed:
            private_write(folder / ('%08d.json' % number), data)
            number += 1
            continue
        # Validate each original Ledger event independently. A failure does not
        # invalidate its already-validated Core interaction; it omits only the
        # historical Ledger timeline event from 020.
        for row in strict_loads(data)['ledger']:
            unit = canonical({'ledger': [row]}).encode()
            unit_passed, unit_detail = _native_preflight(runtime, '--validate-ledger-records', unit)
            if unit_passed:
                private_write(folder / ('%08d.json' % number), unit)
                number += 1
            else:
                skipped.append(_ledger_skip_detail(row, unit_detail))
    return skipped


def stage_batch(runtime, records, before, deployment, directory, progress):
    from retained_runtime import select_core_before, merge_thread_history, normalize_artifacts_lossy, provenance_import_batches
    from history_restore import plan_retained_history
    from agent_thread_history import plan_agent_thread_history
    from core_history_batches import core_history_import_batches
    from native_evidence import plan_aggregates
    from run_upgrade import artifact_import_batches
    from logs import convert_log
    tables, agent, evidence, audits = {}, [], [], []
    for record in records:
        kind = record['kind']
        if kind == 'core_source':
            tables.setdefault(record['locator'], []).append(record['row'])
        elif kind.startswith('agent_'):
            agent.append(record)
        elif kind == 'evidence':
            evidence.append(record)
        elif kind == 'audit':
            audits.append(record)
        else:
            raise ValueError('source_kind_not_supported')
    plan = plan_retained_history(select_core_before(tables, before), agent, evidence)
    threads = plan_agent_thread_history(agent, before, existing_core=plan['core'],
                                        actor_names=getattr(runtime, 'actor_names', {}))
    merge_thread_history(plan, threads, runtime)
    supplemental = {'business_snapshot_no_operation_reference', 'business_snapshot_operation_missing'}
    if any(issue['reason'] not in supplemental for issue in plan['issues']):
        raise ValueError('retained_source_conversion_issues')
    progress.update(phase='preflight_artifact')
    normalize_artifacts_lossy(runtime, plan)
    settings = {'tls_verify': runtime._tls_options(False)['verify_tls']}
    if runtime._tls_options(False).get('ca_file'):
        settings['ca_pem'] = Path(runtime._tls_options(False)['ca_file']).read_text()
    directory.mkdir(mode=0o700)
    def write_batches(category, batches, validation=None):
        folder = directory / category
        folder.mkdir(mode=0o700)
        progress.update(phase='preflight_' + category)
        for number, data in enumerate(batches):
            if len(data) > limits()[1]:
                raise ValueError('native_batch_exceeds_content_limit')
            if validation:
                passed, detail = _native_preflight(runtime, validation, data)
                if not passed:
                    # Keep the exact failed request and bounded native response private;
                    # public progress/error files remain content-free.
                    private_write(directory / ('failed-' + category + '-preflight-%08d.json' % number), data)
                    private_write(directory / ('failed-' + category + '-preflight-%08d-detail.json' % number),
                                  canonical({'category': category, 'batch': number, 'validation': validation,
                                             'native_preflight': detail}).encode())
                    raise ValueError('native_batch_preflight_failed')
            private_write(folder / ('%08d.json' % number), data)
    skipped_core = _stage_core_batches(runtime, plan, directory, progress)
    skipped_core_detail = _prune_skipped_core(plan, {item['interaction_id'] for item in skipped_core}) if skipped_core else {
        'interaction_ids': [], 'operation_ids': [], 'receipt_ids': [], 'removed': {}}
    # These need to survive a later Ledger/Projection preflight failure.
    private_write(directory / 'skipped-core-interactions.json', canonical(skipped_core).encode())
    private_write(directory / 'skipped-core-dependencies.json', canonical(skipped_core_detail).encode())
    write_batches('artifact', artifact_import_batches(plan['artifacts'], settings, max_bytes=min(8 << 20, limits()[1])))
    # The plan is already bounded; Ledger is still emitted as bounded transports.
    def ledger_batches():
        rows, size = [], len(b'{"ledger":[]}')
        for row in plan['ledger']:
            added = len(canonical(row).encode()) + 1
            if rows and size + added > limits()[1]:
                yield canonical({'ledger': rows}).encode()
                rows, size = [], len(b'{"ledger":[]}')
            rows.append(row)
            size += added
            if size > limits()[1]:
                raise ValueError('native_ledger_record_exceeds_limit')
        if rows:
            yield canonical({'ledger': rows}).encode()
    skipped_ledger = _stage_ledger_batches(runtime, ledger_batches(), directory, progress)
    # Persist independently from the final summary so a later projection check
    # cannot erase the ledger-level reconciliation evidence.
    private_write(directory / 'skipped-ledger-records.json', canonical(skipped_ledger).encode())
    write_batches('provenance', provenance_import_batches(plan, max_bytes=limits()[1]), '--validate-provenance-records')
    receipts = [r for r in plan['core']['receipts'] if r.get('trace_id') and r.get('terminal_at')]
    documents, rejected = plan_aggregates(plan['mapped_records'], datetime.now(timezone.utc), receipts=receipts)
    if rejected:
        raise ValueError('native_evidence_conversion_incomplete')
    # Streaming publication consumes documents in byte/count bounded chunks.
    write_batches('evidence', (canonical(item).encode() for item in documents))
    clock = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    requests, items, candidates, reasons = [], [], [], Counter()
    for record in audits:
        converted = convert_log(record['source_id'], record['row'], deployment['environment'], deployment['instance'])
        item = {'kind': 'audit', 'source_id': record['source_id'], **converted}
        if converted['disposition'] == 'convert':
            requests.append({'kind': 'audit', 'payload': converted['event'], 'broker_time': clock})
            candidates.append(len(items))
        items.append(item)
    progress.update(phase='preflight_audit')
    if sum(len(canonical(request).encode()) + 1 for request in requests) > limits()[1]:
        raise ValueError('native_audit_batch_exceeds_content_limit')
    answers = runtime.validate(requests)
    if len(answers) != len(requests):
        raise ValueError('native_validation_count_mismatch')
    for position, request, answer in zip(candidates, requests, answers):
        item = items[position]
        if answer.get('accepted') is not True or canonical(answer.get('canonical_payload')) != canonical(request['payload']):
            item.update(disposition='archive', reason='native_validation_rejected')
        else:
            item.update(payload=answer['canonical_payload'], target_id=answer['event_id'], content_hash=answer['content_hash'])
    identities = {}
    for item in items:
        if item['disposition'] == 'convert':
            identities.setdefault(item['target_id'], set()).add(item['content_hash'])
    for item in items:
        if item['disposition'] == 'convert' and len(identities[item['target_id']]) > 1:
            item.update(disposition='archive', reason='source_identity_content_conflict')
        if item['disposition'] != 'convert':
            reasons[item['reason']] += 1
    private_jsonl(directory / 'audits.jsonl', items)
    defaults = plan.get('defaults', [])
    skipped = plan.get('thread_skipped_items', [])
    skipped_reasons = Counter(item.get('reason', 'thread_message_skipped') for item in skipped)
    skipped_message_ledger = plan.get('thread_message_ledger_skipped', [])
    skipped_message_ledger_reasons = Counter(
        item.get('reason', 'native_message_ledger_preflight_rejected')
        for item in skipped_message_ledger)
    for name, value in [('original-identity-mappings', plan['mappings']), ('original-content-issues', plan['issues']),
                        ('thread-identity-mappings', plan['thread_mappings']), ('thread-field-defaults', plan['thread_defaults']),
                        ('retained-artifact-field-defaults', defaults), ('skipped-thread-message-rounds', skipped),
                        ('skipped-thread-message-ledger-events', skipped_message_ledger)]:
        private_write(directory / (name + '.json'), canonical(value).encode())
    # The old accounting counts Core/ledger/projection source rows. Each skipped
    # dependency is deliberately retained in the immutable source snapshot rather
    # than counted as a verified target record.
    skipped_source_count = sum(skipped_core_detail['removed'].get(name, 0) for name in (
        'interactions', 'operations', 'receipts', 'call_facts', 'idempotency_records',
        'assembly_revisions', 'ledger', 'historical_projections', 'explanations')) + len(skipped_ledger)
    original_count = max(0, plan['stats']['verified_source_count'] - skipped_source_count)
    evidence_count = max(0, len(evidence) - skipped_core_detail['removed'].get('mapped_records', 0))
    summary = {'source_count': len(audits) + evidence_count + original_count,
               'original_count': original_count, 'evidence_count': evidence_count,
               'audit_count': len(audits), 'field_conversion_count': len(defaults), 'reasons': dict(reasons),
               'skipped_count': len(skipped), 'skipped_reasons': dict(skipped_reasons),
               'message_ledger_skipped_count': len(skipped_message_ledger),
               'message_ledger_skipped_reasons': dict(skipped_message_ledger_reasons),
               'core_skipped_count': len(skipped_core),
               'core_skipped_source_count': skipped_source_count,
               'core_skipped_reasons': {'native_core_preflight_rejected': len(skipped_core)} if skipped_core else {},
               'ledger_skipped_count': len(skipped_ledger),
               'ledger_skipped_reasons': {'native_ledger_preflight_rejected': len(skipped_ledger)} if skipped_ledger else {}}
    private_write(directory / 'summary.json', canonical(summary).encode())
    return summary


def paths(folder):
    # Numeric contiguous files avoid constructing a list of all staged batches.
    number = 0
    while (folder / ('%08d.json' % number)).exists():
        yield folder / ('%08d.json' % number)
        number += 1


AUDIT_READBACK_ROUNDS = 5
AUDIT_READBACK_WINDOW_SECONDS = 8


def import_batch(runtime, directory, progress):
    from reconcile import check
    verified, already, retained = 0, 0, 0
    reasons = Counter()
    for category in ('artifact', 'core', 'ledger', 'provenance'):
        progress.update(phase='import_' + category)
        for path in paths(directory / category):
            answer = strict_loads(runtime._native(['--import-' + category + '-records'], path.read_bytes()))
            if answer.get('verified') is not True:
                raise ValueError('retained_history_readback_failed')
    summary = strict_loads((directory / 'summary.json').read_text())
    verified += summary['original_count']
    def documents():
        for path in paths(directory / 'evidence'):
            yield strict_loads(path.read_bytes())
    if summary['evidence_count']:
        from opensearch_history import OpenSearchHistoryWriter
        progress.update(phase='import_evidence')
        with runtime._opensearch_connection() as (endpoint, forwarded):
            writer = OpenSearchHistoryWriter(endpoint, runtime.opensearch_evidence_index,
                runtime.opensearch_username, runtime.opensearch_password, **runtime._tls_options(forwarded))
            counts = writer.publish_documents(documents())
        if counts['conflict']:
            raise ValueError('native_evidence_content_conflict')
        verified += summary['evidence_count']
    progress.update(phase='import_audit', audit_total=0, audit_already_verified=0,
                    audit_published=0, audit_verified=0, audit_pending=0,
                    audit_retained=0, audit_readback_round=0, audit_readback_elapsed_seconds=0)
    # At most the current source batch is in memory, including audit validation/cache.
    with (directory / 'audits.jsonl').open('rb') as stream:
        from snapshot import bounded_lines
        items = [strict_loads(line) for line in bounded_lines(stream)]
    progress.update(audit_total=len(items))
    runtime.validate([{'kind': 'audit', 'payload': item['payload'], 'broker_time': '2000-01-01T00:00:00Z'}
                      for item in items if item['disposition'] == 'convert'])
    candidates = [item for item in items if item['disposition'] == 'convert']
    rows = runtime.fetch_batch(candidates)
    audit_verified, audit_retained, published = 0, 0, 0
    missing = []
    for item in items:
        if item['disposition'] != 'convert':
            retained += 1
            audit_retained += 1
            reasons[item['reason']] += 1
            continue
        status = check([item], lambda candidate: rows.get(candidate['target_id']))['results'][0]['status']
        if status == 'verified':
            verified += 1
            already += 1
            audit_verified += 1
            item.update(disposition='writer_verified', state='already_verified')
        elif status == 'missing':
            missing.append(item)
        else:
            retained += 1
            audit_retained += 1
            reasons['target_content_conflict'] += 1
            item.update(disposition='requires_reconciliation', reason='target_content_conflict')
    progress.update(audit_total=len(items), audit_already_verified=already,
                    audit_verified=audit_verified, audit_pending=len(missing), audit_retained=audit_retained)
    if missing:
        clock = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        try:
            replies = runtime.publish([{'kind': 'audit', 'payload': item['payload'], 'broker_time': clock} for item in missing])
            published = sum(reply.get('accepted') is True for reply in replies)
        except InterruptedError:
            raise
        except (OSError, RuntimeError, ValueError) as error:
            progress.error(error)
        pending = missing
        started = time.monotonic()
        deadline = started + AUDIT_READBACK_WINDOW_SECONDS
        progress.update(audit_published=published)
        for attempt in range(AUDIT_READBACK_ROUNDS):
            if attempt and time.monotonic() > deadline:
                break
            rows = runtime.fetch_batch(pending)
            remaining = []
            for item in pending:
                status = check([item], lambda candidate: rows.get(candidate['target_id']))['results'][0]['status']
                if status == 'verified':
                    verified += 1
                    audit_verified += 1
                    item.update(disposition='writer_verified', state='created')
                elif status == 'conflict':
                    retained += 1
                    audit_retained += 1
                    reasons['target_content_conflict'] += 1
                    item.update(disposition='requires_reconciliation', reason='target_content_conflict')
                else:
                    remaining.append(item)
            pending = remaining
            progress.update(audit_verified=audit_verified, audit_pending=len(pending),
                            audit_retained=audit_retained, audit_readback_round=attempt + 1,
                            audit_readback_elapsed_seconds=round(time.monotonic() - started, 1))
            budget = deadline - time.monotonic()
            if not pending or budget <= 0 or attempt == AUDIT_READBACK_ROUNDS - 1:
                break
            time.sleep(min(2 ** attempt, budget))
        for item in pending:
            retained += 1
            audit_retained += 1
            reasons['publication_requires_readback'] += 1
            item.update(disposition='requires_reconciliation', reason='publication_requires_readback')
        progress.update(audit_retained=audit_retained,
                        audit_readback_elapsed_seconds=round(time.monotonic() - started, 1))
    private_jsonl(directory / 'final-items.jsonl', items)
    return verified, already, retained, reasons


def run_loop(runtime, state_root):
    from run_upgrade import _report
    state_root = Path(state_root)
    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = state_root / ('run-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8])
    directory.mkdir(mode=0o700)
    result = {'complete': False, 'state': 'precheck_failed', 'source_count': 0, 'target_verified_count': 0,
              'already_verified_count': 0, 'retained_count': 0, 'skipped_count': 0,
              'core_skipped_count': 0, 'ledger_skipped_count': 0,
              'message_ledger_skipped_count': 0, 'run_directory': str(directory)}
    result.update({key: 0 for key in ('audit_total', 'audit_published', 'audit_verified', 'audit_pending', 'audit_retained')})
    reasons = Counter()
    # Temporary files can contain business content, even though logs cannot.
    os.chmod(directory, 0o700)
    with Progress(directory) as progress, interruption(directory):
        try:
            count, size = limits()
            from snapshot import record_limit
            record_limit()
            free = shutil.disk_usage(state_root).free
            if free < 256 << 20:
                raise ValueError('historical_workspace_space_insufficient')
            progress.update(batch_record_limit=count, batch_byte_limit=size, workspace_free_bytes=free)
            deployment = runtime.discover()
            import re
            if not re.fullmatch(r'[a-zA-Z0-9_.-]+', deployment['instance']):
                raise ValueError('invalid_instance_identity')
            before = os.environ.get('BKN_HISTORY_BEFORE') or datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            snapshots, before = freeze(runtime, state_root, deployment, before, progress)
            # Only Agent history needs the EE converter. Audit/Core-only
            # upgrades must not gain an unrelated Enterprise prerequisite.
            if strict_loads((Path(snapshots[-1]) / 'snapshot.json').read_text())['record_count']:
                from thread_provenance import precheck_thread_projections
                progress.update(phase='precheck_ee_projection')
                precheck_thread_projections(runtime._native_ee_projection)
            result['agent_cutover_before'] = before
            result['captured_source_records'] = sum(strict_loads((Path(p) / 'snapshot.json').read_text())['record_count']
                                                    for p in snapshots)
            progress.update(phase='index_sources')
            source = LoopSource(directory / 'source-lookup', snapshots, deployment)
            source.observer = progress
            with source:
                progress.update(phase='prepare', source_records=source.count)
                batches = directory / 'batches'
                batches.mkdir(mode=0o700)
                total = 0
                for number, records in enumerate(source.batches()):
                    progress.update(phase='prepare', batch=number + 1, batch_records=len(records))
                    runtime.actor_names = source.actor_names(records)
                    summary = stage_batch(runtime, records, before, deployment, batches / ('%08d' % number), progress)
                    result['source_count'] += summary['source_count']
                    result['field_conversion_count'] = result.get('field_conversion_count', 0) + summary['field_conversion_count']
                    result['skipped_count'] += summary.get('skipped_count', 0)
                    result['core_skipped_count'] += summary.get('core_skipped_count', 0)
                    result['ledger_skipped_count'] += summary.get('ledger_skipped_count', 0)
                    result['message_ledger_skipped_count'] += summary.get('message_ledger_skipped_count', 0)
                    reasons.update(summary.get('skipped_reasons', {}))
                    reasons.update(summary.get('core_skipped_reasons', {}))
                    reasons.update(summary.get('ledger_skipped_reasons', {}))
                    reasons.update(summary.get('message_ledger_skipped_reasons', {}))
                    total = number + 1
                    progress.update(prepared_batches=total, prepared_records=result['source_count'])
                    records = None
            # Release the final batch before any imports; preparation is complete.
            records = None
            for number in range(total):
                progress.update(batch=number + 1)
                verified, already, retained, issues = import_batch(runtime, batches / ('%08d' % number), progress)
                result['target_verified_count'] += verified
                result['already_verified_count'] += already
                result['retained_count'] += retained
                reasons.update(issues)
                for key in ('audit_total', 'audit_published', 'audit_verified', 'audit_pending', 'audit_retained'):
                    result[key] += progress.values[key]
                progress.update(completed_batches=number + 1, verified_records=result['target_verified_count'])
            progress.update(phase='rebuild_core_projection')
            result['core_projection'] = runtime.rebuild_core_projection()
            result['complete'] = result['target_verified_count'] == result['source_count'] and result['retained_count'] == 0
            result['state'] = ('completed_with_skipped_history' if result['complete'] and (result['skipped_count'] or result['core_skipped_count'] or result['ledger_skipped_count'] or result['message_ledger_skipped_count'])
                               else 'completed' if result['complete'] else 'partial_requires_reconciliation')
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
            progress.error(error)
            result['state'] = progress.values['phase'] + '_failed'
            reasons[result['state']] += 1
            result['retained_count'] = max(result['retained_count'], result['source_count'] - result['target_verified_count'])
        _report(directory, result, reasons)
        progress.update(phase=result['state'])
    return result
