"""Live PostgreSQL daemon ownership and cleanup failure controls."""
from pathlib import Path
import socket
import tempfile
from unittest.mock import patch

from queue_owned_process import drain_adopted, require
from queue_pg_fixture import QueuePostgresFixture


def run(pg_bin):
    with tempfile.TemporaryDirectory(prefix='pg-fixture-controls-') as parent:
        fixture = QueuePostgresFixture(pg_bin, parent)
        try:
            fixture.start()
            child = fixture.crash_parent / 'api007-server-crash-control'
            child.mkdir()
            data = child / 'data'
            fixture.checked('initdb', '-D', data, '--no-locale', '--encoding=UTF8', '--auth=trust')
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                port = listener.getsockname()[1]
            def start():
                fixture.checked('pg_ctl', '-D', data, '-l', child / 'server.log',
                    '-w', '-t', '30', '-o', f"-h 127.0.0.1 -p {port} -c unix_socket_directories=''", 'start')
            start()
            fixture.checked('pg_ctl', '-D', data, '-m', 'immediate', '-w', '-t', '30', 'stop')
            start()
            # Simulate a test process dying after its restarted postmaster has
            # detached; runner cleanup must stop it independently of test groups.
        finally:
            fixture.close()
        reaped = drain_adopted()
        require(fixture.cleaned and not fixture.root.exists(), 'detached cluster cleanup failed')
        require(len(reaped) >= 3, 'ordinary and crash/restart postmasters were not adopted/reaped')

        fixture = QueuePostgresFixture(pg_bin, parent)
        fixture.start()
        original = Path.iterdir
        def failed_listing(path):
            if path == fixture.crash_parent:
                raise PermissionError('intentional crash-directory enumeration failure')
            return original(path)
        try:
            with patch.object(Path, 'iterdir', failed_listing):
                try:
                    fixture.close()
                except RuntimeError:
                    pass
                else:
                    raise RuntimeError('enumeration failure falsely reported clean')
            require(fixture.root.exists() and not fixture.cleaned, 'failed cleanup must retain evidence root')
            require(fixture.command('pg_ctl', '-D', fixture.data, 'status').returncode == 3,
                    'enumeration failure skipped primary cluster stop')
        finally:
            fixture.close()
        drain_adopted()
    print('PASS: detached PostgreSQL crash/restart reaping and enumeration-failure cleanup controls', flush=True)
