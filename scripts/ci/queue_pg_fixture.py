"""Owned PostgreSQL 16 fixture for the API-007 execution lane.

Only data directories created beneath this instance's private root may be stopped.
The ordinary test URL always uses a verified non-superuser database owner.
"""
from pathlib import Path
import os
import shutil
import socket
import subprocess
import tempfile

from queue_owned_process import owned_process


class QueuePostgresFixture:
    def __init__(self, pg_bin, parent):
        self.bin = Path(pg_bin).resolve(strict=True)
        for name in ('postgres', 'initdb', 'pg_ctl', 'psql'):
            if not (self.bin / name).is_file():
                raise ValueError(f'missing PostgreSQL binary: {name}')
        self.root = Path(tempfile.mkdtemp(prefix='fireweed-pg-', dir=parent)).resolve()
        self.data = self.root / 'data'
        self.crash_parent = self.root / 'crash'
        self.crash_parent.mkdir()
        self.tmp = self.root / 'tmp'
        self.tmp.mkdir()
        self.environment = dict(os.environ, LC_ALL='C')
        for key in list(self.environment):
            if key.startswith('PG'):
                del self.environment[key]
        self.cleaned = False

    def command(self, name, *args):
        with owned_process([str(self.bin / name), *map(str, args)],
                           cwd=self.root, env=self.environment, text=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            stdout, stderr = process.communicate(timeout=60)
            return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)

    def checked(self, name, *args):
        result = self.command(name, *args)
        if result.returncode:
            raise RuntimeError(f'{name} failed: {result.stdout}\n{result.stderr}')
        return result.stdout.strip()

    def start(self):
        self.version = self.checked('postgres', '--version')
        if not self.version.startswith('postgres (PostgreSQL) 16.'):
            raise ValueError(f'PostgreSQL 16 required: {self.version}')
        self.checked('initdb', '-D', self.data, '--no-locale', '--encoding=UTF8',
                     '--auth=trust', '--username=fireweed_admin')
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        options = (f'-h 127.0.0.1 -p {port} -c unix_socket_directories=\'\' '
                   '-c fsync=on -c synchronous_commit=on -c full_page_writes=on')
        self.checked('pg_ctl', '-D', self.data, '-l', self.root / 'server.log',
                     '-w', '-t', '30', '-o', options, 'start')
        admin = f'postgresql://fireweed_admin@127.0.0.1:{port}/postgres'
        actual_data = self.checked('psql', '-XAt', '-v', 'ON_ERROR_STOP=1', admin,
                                   '-c', 'SHOW data_directory')
        if Path(actual_data) != self.data:
            raise RuntimeError('connected to a different PostgreSQL data directory')
        self.checked('psql', '-X', '-v', 'ON_ERROR_STOP=1', admin, '-c',
                     'CREATE ROLE fireweed_queue LOGIN NOSUPERUSER NOCREATEDB '
                     'NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS')
        self.checked('psql', '-X', '-v', 'ON_ERROR_STOP=1', admin, '-c',
                     'CREATE DATABASE fireweed_queue OWNER fireweed_queue')
        self.url = f'postgresql://fireweed_queue@127.0.0.1:{port}/fireweed_queue'
        verification = self.checked('psql', '-XAt', '-v', 'ON_ERROR_STOP=1', self.url,
            '-c', "SELECT current_user, rolsuper, rolcreatedb, rolcreaterole, "
            "rolreplication, rolbypassrls, current_setting('fsync'), "
            "current_setting('synchronous_commit'), current_setting('full_page_writes'), current_setting('server_version_num') "
            "FROM pg_roles WHERE rolname=current_user")
        parts = verification.split('|')
        if '|'.join(parts[:-1]) != 'fireweed_queue|f|f|f|f|f|on|on|on' or int(parts[-1]) // 10000 != 16:
            raise RuntimeError(f'fixture role/durability verification failed: {verification}')
        self.qualification = {
            'current_user': parts[0],
            'privileges': dict.fromkeys(('superuser', 'createdb', 'createrole', 'replication', 'bypassrls'), False),
            'durability': dict.fromkeys(('fsync', 'synchronous_commit', 'full_page_writes'), 'on'),
            'data_directory_verified': True,
            'server_version_num': int(parts[-1]),
        }
        return self

    def test_environment(self):
        return dict(self.environment,
                    FIREWEED_API007_PG_TEST_URL=self.url,
                    FIREWEED_CONTROL_PLANE_TEST_URL=self.url,
                    FIREWEED_API007_PG_BIN=str(self.bin),
                    FIREWEED_API007_PG_CRASH_PARENT=str(self.crash_parent),
                    TMPDIR=str(self.tmp))

    def close(self):
        # A failed stop must not prevent attempts to stop other owned daemons.
        failures = []
        directories = [self.data]
        try:
            children = list(self.crash_parent.iterdir())
        except OSError as error:
            failures.append(f'cannot enumerate crash fixtures: {error}')
            children = []
        for child in children:
            if child.is_symlink() or not child.is_dir() or not child.name.startswith('api007-server-crash-'):
                failures.append(f'unexpected crash fixture path: {child}')
                continue
            directories.append(child / 'data')
        for data in directories:
            try:
                if data.is_symlink() or not data.resolve().is_relative_to(self.root):
                    raise RuntimeError('refusing cleanup outside owned fixture')
                if not data.exists():
                    continue
                status = self.command('pg_ctl', '-D', data, 'status')
                if status.returncode == 0:
                    self.checked('pg_ctl', '-D', data, '-m', 'fast', '-w', '-t', '30', 'stop')
                    status = self.command('pg_ctl', '-D', data, 'status')
                if status.returncode != 3:
                    raise RuntimeError(f'cannot confirm PostgreSQL stopped: {status.stderr}')
            except Exception as error:
                failures.append(f'{data}: {error}')
        if failures:
            raise RuntimeError('owned fixture retained: ' + '; '.join(failures))
        shutil.rmtree(self.root)
        self.cleaned = True

    def __enter__(self):
        try:
            return self.start()
        except BaseException:
            self.close()
            raise

    def __exit__(self, *_):
        self.close()
