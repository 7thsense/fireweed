"""Require same-job PostgreSQL evidence production and closure consumption."""


def validate(workflow):
    marker = '\n  api007-postgres:\n'
    if workflow.count(marker) != 1:
        raise ValueError('mandatory api007-postgres job missing/duplicated')
    job = workflow.split(marker, 1)[1]
    # Stop at another job, without treating nested step indentation as a job.
    import re
    job = re.split(r'\n  [a-zA-Z0-9_-]+:\n', job, maxsplit=1)[0]
    for forbidden in ('continue-on-error:', 'needs:', 'strategy:'):
        if forbidden in job:
            raise ValueError('PostgreSQL correctness job cannot be optional: ' + forbidden)
    if job.count('if:') != 1 or '        if: always()' not in job:
        raise ValueError('only evidence upload may be conditional')
    required = (
        '    runs-on: ubuntu-24.04\n',
        '    timeout-minutes: 30\n',
        'sudo apt-get install --yes postgresql-16 ripgrep',
        '      - name: Qualification runner cancellation controls\n        run: python3 scripts/ci/queue_pg_controls.py --pg-bin /usr/lib/postgresql/16/bin\n',
        '''      - name: Execute exact PostgreSQL correctness routes
        run: |
          python3 scripts/ci/queue-pg-execution.py \\
            --pg-bin /usr/lib/postgresql/16/bin \\
            --fixture-parent "$RUNNER_TEMP" \\
            --output target/queue-pg-execution
''',
        '      - name: Storage remediation policy\n        run: bash scripts/ci/storage-remediation-policy.sh --mode-file scripts/ci/storage-remediation-policy.mode --queue-evidence target/queue-pg-execution/report.json\n',
        '      - name: Retain PostgreSQL correctness execution evidence\n        if: always()\n        uses: actions/upload-artifact@v4\n',
        '          path: target/queue-pg-execution\n',
        '          if-no-files-found: error\n',
    )
    for fragment in required:
        if job.count(fragment) != 1:
            raise ValueError('required PostgreSQL correctness job binding missing/duplicated: ' + fragment)
    if job.index('Execute exact PostgreSQL correctness routes') > job.index('Storage remediation policy'):
        raise ValueError('closure must consume evidence produced earlier in the same job')


def self_test(workflow):
    validate(workflow)
    for old, new in (
        ('api007-postgres:', 'disabled-postgres:'),
        ('runs-on: ubuntu-24.04', 'runs-on: ubuntu-latest'),
        ('--pg-bin /usr/lib/postgresql/16/bin', '--pg-bin /usr/bin'),
        ('--queue-evidence target/queue-pg-execution/report.json', ''),
        ('--output target/queue-pg-execution', '--output target/unrelated'),
        ('run: python3 scripts/ci/queue_pg_controls.py --pg-bin /usr/lib/postgresql/16/bin', 'run: true'),
        ('    runs-on: ubuntu-24.04', '    if: false\n    runs-on: ubuntu-24.04'),
        ('    runs-on: ubuntu-24.04', '    continue-on-error: true\n    runs-on: ubuntu-24.04'),
        ('if-no-files-found: error', 'if-no-files-found: ignore'),
        ('if: always()', 'if: always() && false'),
        ('      - name: Retain PostgreSQL correctness execution evidence\n        if: always()',
         '        if: always() && false\n      - name: Retain PostgreSQL correctness execution evidence'),
    ):
        if old not in workflow:
            raise ValueError('workflow negative control did not apply')
        try:
            head, tail = workflow.split('\n  api007-postgres:\n', 1)
            candidate = (head + '\n  api007-postgres:\n' + tail.replace(old, new, 1))
            if old == 'api007-postgres:':
                candidate = workflow.replace(old, new, 1)
            validate(candidate)
        except ValueError:
            continue
        raise ValueError('unsafe PostgreSQL workflow binding accepted: ' + old)


if __name__ == '__main__':
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    self_test((root / '.github/workflows/ci.yml').read_text())
    print('PostgreSQL correctness workflow bindings and negative controls passed')
