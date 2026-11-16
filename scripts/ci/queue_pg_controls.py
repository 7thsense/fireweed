"""Failure and cancellation controls for the mandatory owned S3 runner."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import zipfile


def run(runner):
    valid = "test control_test ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;"
    for output, code in [("test control_test ... ok\ntest result: ok. 0 passed; 0 failed; 0 ignored;", 0),
                         ("test control_test ... ok\ntest result: ok. 0 passed; 0 failed; 1 ignored;", 0),
                         (valid + "\ntest result: ok. 1 passed; 0 failed; 0 ignored;", 0), (valid, 1)]:
        try:
            runner.test([sys.executable, "-c", f"print({output!r}); raise SystemExit({code})"], os.environ.copy(), 1)
        except RuntimeError:
            pass
        else:
            raise RuntimeError("mandatory count/status negative control escaped")
    runner.test([sys.executable, "-c", f"print({valid!r})"], os.environ.copy(), 1)
    registered = []

    def pending(process):
        registered.append(process.pid)
        os.kill(os.getpid(), signal.SIGTERM)

    try:
        with runner.owned_process([sys.executable, "-c", "import time; time.sleep(60)"], _registered=pending):
            raise RuntimeError("pending spawn-registration signal was not delivered")
    except KeyboardInterrupt:
        pass
    finally:
        runner._cancelling = False  # Only this intentional, fully drained control recovers.
    runner.require(registered and not Path(f"/proc/{registered[0]}").exists(),
                   "spawn-registration cancellation leaked its child")
    with tempfile.TemporaryDirectory(prefix="native-s3-controls-") as raw:
        directory = Path(raw)
        for mode in ("selector-failure", "termination"):
            ready = directory / (mode + ".json")
            cleanup = directory / (mode + ".cleanup")
            grand_ready = directory / (mode + ".grandchild")
            worker = directory / (mode + ".py")
            grandchild = f'import signal,time; from pathlib import Path; signal.signal(signal.SIGTERM,signal.SIG_IGN); Path({str(grand_ready)!r}).write_text("ready"); time.sleep(60)'
            child = f'''
import json,os,signal,subprocess,sys,time
from pathlib import Path
signal.signal(signal.SIGTERM,lambda *_:Path({str(cleanup)!r}).write_text("cleanup started"))
grandchild=subprocess.Popen([sys.executable,"-c",{grandchild!r}],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
while not Path({str(grand_ready)!r}).exists():time.sleep(0.01)
Path({str(ready)!r}).write_text(json.dumps({{"child":os.getpid(),"grandchild":grandchild.pid,"server":SERVER_PID}}))
MODE_ACTION
'''
            action = 'print("test result: ok. 0 passed; 0 failed; 0 ignored;",flush=True); raise SystemExit(1)' if mode == "selector-failure" else "time.sleep(60)"
            worker.write_text(f'''
import os,subprocess,sys
sys.path.insert(0,{str(Path(runner.__file__).parent)!r})
from queue_owned_process import own_descendants,owned_process
from queue_pg_controls import test
own_descendants()
with owned_process([sys.executable,"-c","import time; time.sleep(60)"]) as server:
    child={child!r}.replace("SERVER_PID",str(server.pid)).replace("MODE_ACTION",{action!r})
    test([sys.executable,"-c",child],os.environ.copy(),1)
''')
            owned = {}
            with runner.owned_process([sys.executable, str(worker)], stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, text=True) as process:
                try:
                    deadline = time.monotonic() + 10
                    while not owned:
                        try:
                            owned = json.loads(ready.read_text())
                        except (OSError, ValueError):
                            runner.require(time.monotonic() < deadline and process.poll() is None,
                                           "lifecycle control failed before active child")
                            time.sleep(0.01)
                    if mode == "termination":
                        process.send_signal(signal.SIGTERM)
                        while not cleanup.exists():
                            runner.require(time.monotonic() < deadline and process.poll() is None,
                                           "cleanup control never entered active drainage")
                            time.sleep(0.01)
                        process.send_signal(signal.SIGTERM)
                        process.send_signal(signal.SIGINT)
                    output, _ = process.communicate(timeout=10)
                    runner.require(process.returncode != 0, "lifecycle control unexpectedly succeeded")
                    for pid in owned.values():
                        runner.require(not Path(f"/proc/{pid}").exists(),
                                       f"{mode} leaked owned process {pid}: {output}")
                finally:
                    previous = signal.pthread_sigmask(signal.SIG_BLOCK, runner.CANCELLATION_SIGNALS)
                    try:
                        # A failed control may orphan separate worker-owned
                        # sessions. Clean only their recorded groups and reap.
                        for name in ("child", "server"):
                            if name in owned:
                                try:
                                    os.killpg(owned[name], signal.SIGKILL)
                                except ProcessLookupError:
                                    pass
                                deadline = time.monotonic() + 5
                                while True:
                                    try:
                                        child_pid, _ = os.waitpid(-owned[name], os.WNOHANG)
                                    except ChildProcessError:
                                        break
                                    if child_pid == 0:
                                        runner.require(time.monotonic() < deadline, "control cleanup did not drain")
                                        time.sleep(0.01)
                    finally:
                        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    print("PASS: count/status, spawn-registration, failed-selector and repeated-cancellation controls", flush=True)



def test(command, env, expected):
    import queue_owned_process as lifecycle
    from queue_pg_routes import validate_execution
    lifecycle.require(expected == 1, 'control expected count must be explicit')
    with tempfile.TemporaryDirectory(prefix='pg-command-control-') as raw:
        output = lifecycle.run_owned(command, Path(raw) / 'command.log', env, 30, Path.cwd())
        try:
            validate_execution({'kind': 'test', 'test': 'control_test'}, output)
        except ValueError as error:
            raise RuntimeError(str(error)) from error


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--pg-bin', type=Path)
    args = parser.parse_args()
    import queue_owned_process as lifecycle
    lifecycle.own_descendants()
    import unittest
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromName("test_queue_pg_evidence"))
    lifecycle.require(result.wasSuccessful(), "closure evidence controls failed")
    lifecycle.test = test
    run(lifecycle)
    lifecycle.drain_adopted()
    from queue_pg_producer_controls import run as producer_controls
    producer_controls()
    lifecycle.drain_adopted()
    if args.pg_bin:
        from queue_pg_fixture_controls import run as fixture_controls
        fixture_controls(args.pg_bin)
