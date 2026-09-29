#!/usr/bin/env python3
"""Ensure API errors and remaining hard NFS mounts cannot trigger node deletion."""
import os
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
with tempfile.TemporaryDirectory(prefix='xata-cleanup-contract-') as directory:
    root = pathlib.Path(directory)
    executable = '''#!/usr/bin/env python3
import os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
if name == 'kubectl':
    if args == ['config', 'current-context']:
        print('k3d-xata-chart-cleanup')
    elif 'nfs-server' in args:
        if os.environ['SCENARIO'] == 'api-error':
            print('API unavailable', file=sys.stderr); sys.exit(1)
        if os.environ['SCENARIO'] == 'mounted': print('deployment.apps/nfs-server')
    elif args[:2] == ['get', 'nodes']: print('k3d-xata-chart-cleanup-agent-0')
elif name == 'docker': print('mounted - nfs4 server:/export')
elif name == 'k3d': pathlib.Path(os.environ['DELETION_MARKER']).touch()
'''
    for name in ('kubectl', 'docker', 'k3d', 'sleep'):
        p = root / name
        p.write_text(executable)
        p.chmod(0o755)
    marker = root / 'deleted'
    env = os.environ | {'PATH': str(root)+os.pathsep+os.environ['PATH'],
                        'KUBECONFIG': str(root/'kubeconfig'), 'DELETION_MARKER': str(marker)}
    for scenario, expected_success in [('api-error', False), ('mounted', False), ('absent', True)]:
        result = subprocess.run(['bash', str(ROOT/'scripts/helm/delete-test-cluster.sh'),
                                 'k3d', 'xata-chart-cleanup'],
                                env=env | {'SCENARIO': scenario}, capture_output=True, text=True, timeout=30)
        assert (result.returncode == 0) == expected_success, result.stderr
        assert marker.exists() == expected_success, 'Node deletion bypassed the NFS safety gate'
        print('PASS: cleanup '+scenario)
