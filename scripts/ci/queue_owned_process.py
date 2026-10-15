"""Owned subprocess lifecycle shared by the PostgreSQL qualification lane."""
import ctypes
from contextlib import contextmanager
import os
import signal
import subprocess
import sys
import time


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


_cancelling = False


def termination(_number, _frame):
    global _cancelling
    if _cancelling:
        return
    _cancelling = True
    raise KeyboardInterrupt("qualification terminated")


def own_descendants():
    # The pinned Linux fixture runner adopts and reaps its own orphaned test
    # descendants; it does not signal unrelated server or cargo processes.
    require(sys.platform == "linux", "pinned RustFS qualification requires Linux")
    control = ctypes.CDLL(None, use_errno=True).prctl
    control.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
    control.restype = ctypes.c_int
    require(control(36, 1, 0, 0, 0) == 0,
            "cannot establish qualification descendant ownership")
    signal.signal(signal.SIGTERM, termination)
    signal.signal(signal.SIGINT, termination)


CANCELLATION_SIGNALS = {signal.SIGINT, signal.SIGTERM}


def child_signals():
    # This standalone Linux runner is single-threaded. Children must not inherit
    # the parent's temporary registration/cleanup signal mask across exec.
    signal.pthread_sigmask(signal.SIG_UNBLOCK, CANCELLATION_SIGNALS)


def drain_group(process):
    def send(number):
        try:
            os.killpg(process.pid, number)
        except ProcessLookupError:
            pass
    send(signal.SIGTERM)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass
    send(signal.SIGKILL)
    process.wait()
    deadline = time.monotonic() + 5
    while True:
        try:
            child, _ = os.waitpid(-process.pid, os.WNOHANG)
        except ChildProcessError:
            return
        if child == 0:
            require(time.monotonic() < deadline, "owned command descendants did not drain")
            time.sleep(0.01)


@contextmanager
def owned_process(command, *, _registered=None, **options):
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, CANCELLATION_SIGNALS)
    restore_mask = previous
    try:
        process = subprocess.Popen(command, start_new_session=True,
                                   preexec_fn=child_signals, **options)
        try:
            if _registered is not None:
                _registered(process)
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
            yield process
        finally:
            restore_mask = signal.pthread_sigmask(signal.SIG_BLOCK, CANCELLATION_SIGNALS)
            drain_group(process)
    finally:
        # Pending cancellation is delivered only after the complete owned group
        # has drained. Preserve a caller mask installed while a long-lived
        # owner was active, so nested cleanup cannot deliver cancellation early.
        signal.pthread_sigmask(signal.SIG_SETMASK, restore_mask)



def run_owned(command, log, environment, timeout, cwd):
    with log.open('x') as output:
        with owned_process(command, cwd=cwd, env=environment, stdout=output,
                           stderr=subprocess.STDOUT) as process:
            code = process.wait(timeout=timeout)
    if code:
        raise RuntimeError(f'command failed ({code}); see {log}')
    return log.read_text()


def drain_adopted(timeout=5):
    """After every owner closes, reap all remaining children of this runner.

    Linux subreaper ownership covers daemonized crash/restart generations even
    after their private data directory and postmaster.pid have been removed.
    This standalone runner owns every child; ECHILD proves complete drainage.
    """
    reaped = []
    deadline = time.monotonic() + timeout
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return reaped
        if pid:
            reaped.append({'pid': pid, 'status': status})
        else:
            require(time.monotonic() < deadline, 'owned adopted descendants remain alive')
            time.sleep(0.01)
