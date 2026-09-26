#!/usr/bin/env python3
"""A terminated cloud runner must not publish a successful result code."""
import os
import pathlib
import signal
import subprocess
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
with tempfile.TemporaryDirectory(prefix='xata-runner-signal-') as directory:
    work = pathlib.Path(directory)
    docker = work / 'docker'
    docker.write_text('#!/bin/sh\ntouch "$XATA_SIGNAL_READY"\nexec sleep 60\n')
    docker.chmod(0o755)
    ready = work / 'ready'
    artifacts = work / 'results'
    env = os.environ | {
        'PATH': str(work) + os.pathsep + os.environ['PATH'],
        'XATA_SIGNAL_READY': str(ready),
        'XATA_MATRIX_ARTIFACTS': str(artifacts),
    }
    process = subprocess.Popen(
        ['bash', str(ROOT / 'scripts/helm/run-gcp-tests.sh')], env=env,
        start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists():
            assert process.poll() is None, 'Runner exited before reaching the controlled wait'
            assert time.monotonic() < deadline, 'Runner did not reach the controlled wait'
            time.sleep(0.05)
        os.killpg(process.pid, signal.SIGTERM)
        assert process.wait(timeout=10) == 143, 'Interrupted runner returned success'
        assert (artifacts / 'result-code.txt').read_text().strip() == '143'
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
        process.stderr.close()
print('PASS: SIGTERM records exit status 143 without creating any cluster')
