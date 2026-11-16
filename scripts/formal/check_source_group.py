#!/usr/bin/env python3
"""Bootstrap the captured source group authority runner after invalidating prior evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import types

SCHEMA = 'fireweed-api007-source-group-formal-v1'


def output_path(root, requested):
    target = root / 'target'
    if target.is_symlink():
        raise ValueError('symlink target directory forbidden')
    path = (requested if requested.is_absolute() else root / requested).resolve()
    allowed = (target / 'api007-formal').resolve()
    if not allowed.is_relative_to(target.resolve()) or not path.is_relative_to(allowed) or path == allowed:
        raise ValueError('output must remain inside repository target/api007-formal')
    return path


def write_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.report-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as handle:
            handle.write(json.dumps(report, sort_keys=True, indent=2) + '\n')
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def load_runtime():
    path = Path(__file__).resolve().with_name('source_group_gate.py')
    source = path.read_bytes()
    runtime = types.ModuleType('source_group_gate')
    runtime.__file__ = str(path)
    sys.modules[runtime.__name__] = runtime
    exec(compile(source, str(path), 'exec'), runtime.__dict__)
    runtime.helper.LOADED_SOURCES[str(path)] = hashlib.sha256(source).hexdigest()
    return runtime


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--max-states', type=int, default=100000)
    parser.add_argument('--output', type=Path, default=Path('target/api007-formal/source-group.json'))
    arguments = list(sys.argv[1:] if argv is None else argv)
    root = Path(__file__).resolve().parents[2]
    path = None
    try:
        # Locate the destination before validating other CLI values, so a bad
        # bound cannot leave previous evidence marked PASS.
        requested = Path('target/api007-formal/source-group.json')
        for index, argument in enumerate(arguments):
            if argument == '--output' and index + 1 < len(arguments):
                requested = Path(arguments[index + 1])
            elif argument.startswith('--output='):
                requested = Path(argument.split('=', 1)[1])
        path = output_path(root, requested)
        write_report(path, {'schema': SCHEMA, 'status': 'INCOMPLETE'})
        try:
            args = parser.parse_args(arguments)
        except SystemExit as error:
            if error.code == 0:
                return 0
            raise ValueError('invalid source group authority CLI arguments') from error
        runtime = load_runtime()
        return runtime.main(args.max_states, path, root)
    except Exception as error:
        failure = {'schema': SCHEMA, 'status': 'FAIL', 'error': str(error)}
        if path is not None:
            write_report(path, failure)
        print(json.dumps(failure, sort_keys=True))
        return 1


if __name__ == '__main__':
    sys.exit(main())
