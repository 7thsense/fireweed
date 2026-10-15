"""Exact route and libtest-result validation for PostgreSQL qualification."""
import json
import re


def load_routes(root):
    manifest = json.loads((root / 'scripts/ci/queue-pg-routes.json').read_text())
    if manifest.get('schema') != 'fireweed.queue_pg_routes.v1' or manifest.get('scope') != 'correctness_only_not_tp005':
        raise ValueError('unsupported PostgreSQL route manifest')
    inventory = json.loads((root / 'scripts/ci/storage-remediation-inventory.json').read_text())
    debts = {item['id']: item for item in inventory['debt_registries']['ignored_tests']
             if item['status'] == 'debt' and item['category'] == 'ignored_test'
             and item['path'].startswith(('crates/fireweed-postgres/', 'crates/fireweed/'))}
    entries = manifest['entries']
    ids = [entry['debt_id'] for entry in entries]
    if len(ids) != len(set(ids)) or set(ids) != set(debts):
        raise ValueError('route IDs must cover current debt exactly once')
    special = {
        'child': ('bf01244d5101bf1c8ce6', 'process_loss_child'),
        'process_parent': ('50a5bcd4becd6cc64aa7', 'acknowledged_atomic_group_survives_writer_process_loss'),
    }
    for kind, (debt_id, test) in special.items():
        matches = [e for e in entries if e['kind'] == kind]
        if len(matches) != 1 or (matches[0]['debt_id'], matches[0]['test']) != (debt_id, test):
            raise ValueError('only the declared process-loss parent/child exemption is supported')
        if matches[0]['package'] != 'fireweed-postgres' or matches[0]['target'] != ['--test', 'queue_process_loss']:
            raise ValueError('process-loss exemption must use its exact binary')
    names = set()
    for entry in entries:
        if entry['path'] != debts[entry['debt_id']]['path']:
            raise ValueError('route source path differs from inventory')
        debt = debts[entry['debt_id']]
        lines = (root / entry['path']).read_text().splitlines()
        offset = debt['line'] - 1
        if lines[offset].strip() != debt['identity']:
            raise ValueError('ignored declaration changed; regenerate inventory')
        declaration = re.search(r'\b(?:async\s+)?fn\s+([a-z_0-9]+)\s*\(',
                                '\n'.join(lines[offset + 1:offset + 9]))
        if declaration is None:
            raise ValueError('ignored route has no immediately following test declaration')
        prefix = {
            'crates/fireweed-postgres/src/queue_store.rs': 'queue_store::tests::',
            'crates/fireweed-postgres/src/queue_store/administration.rs': 'queue_store::administration::tests::',
        }.get(entry['path'], '')
        if entry['test'] != prefix + declaration.group(1):
            raise ValueError('route selector does not name its exact ignored declaration')

        package, target = entry['package'], entry['target']
        if package not in ('fireweed-postgres', 'fireweed'):
            raise ValueError('unexpected package')
        if target != ['--lib'] and not (len(target) == 2 and target[0] == '--test'
                and re.fullmatch(r'[a-z_]+', target[1])):
            raise ValueError('unexpected Cargo target')
        if not re.fullmatch(r'[a-z_][a-z_0-9]*(?:::[a-z_][a-z_0-9]*)*', entry['test']):
            raise ValueError('invalid exact test selector')
        identity = (package, tuple(target), entry['test'])
        if identity in names:
            raise ValueError('duplicate test route')
        names.add(identity)
        if entry['kind'] not in ('test', 'child', 'process_parent'):
            raise ValueError('unsupported execution kind')
        if entry['kind'] == 'child':
            parents = [p for p in entries if p['debt_id'] == entry['parent_debt_id']]
            if len(parents) != 1 or parents[0]['kind'] != 'process_parent' or (
                    parents[0]['package'], parents[0]['target']) != (package, target):
                raise ValueError('child lacks exact same-binary process parent')
    return entries


def cargo_route(entry):
    args = ['cargo', 'test', '--locked', '-p', entry['package'], *entry['target']]
    if entry['package'] == 'fireweed':
        args += ['--features', 'postgres,objectlog,turso']
    return args + ['--', '--ignored', '--exact', entry['test']]


def validate_listing(entry, output):
    tests = re.findall(r'^(.+): test$', output, re.MULTILINE)
    if tests != [entry['test']]:
        raise ValueError(f'exact compiled test missing or ambiguous: {tests}')


def validate_execution(entry, output):
    summaries = re.findall(
        r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', output)
    expected_count = 2 if entry['kind'] == 'process_parent' else 1
    if summaries != [('ok', '1', '0', '0')] * expected_count:
        raise ValueError(f'unexpected libtest summaries: {summaries}')
    names = re.findall(r'^test ([a-z_0-9:]+) \.\.\. ok$', output, re.MULTILINE)
    expected_names = (['process_loss_child', entry['test']]
                      if entry['kind'] == 'process_parent' else [entry['test']])
    if names != expected_names:
        raise ValueError(f'executed test names differ: {names}')
