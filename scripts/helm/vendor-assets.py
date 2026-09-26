#!/usr/bin/env python3
"""Refresh pinned CRDs and plugin manifests. Review the diff before packaging."""
import pathlib
import subprocess
import urllib.request
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
CHART = ROOT / 'charts/xata'

def save_crd(document):
    # API approval annotations on protected Kubernetes groups must be preserved.
    (CHART / 'crds' / (document['metadata']['name']+'.yaml')).write_text(yaml.safe_dump(document, sort_keys=False))

for archive, flag in [('cloudnative-pg-0.0.0-g139a814.tgz','crds.create=true'), ('cert-manager-v1.18.2.tgz','crds.enabled=true')]:
    rendered = subprocess.check_output(['helm','template','vendor',str(CHART/'charts'/archive),'--kube-version','1.34.1','--include-crds','--set',flag], text=True)
    for document in yaml.safe_load_all(rendered):
        if document and document.get('kind') == 'CustomResourceDefinition':
            save_crd(document)

certificates = []
for name, url in [
    ('barman','https://github.com/cloudnative-pg/plugin-barman-cloud/releases/download/v0.10.0/manifest.yaml'),
    ('scale-to-zero','https://github.com/xataio/cnpg-i-scale-to-zero/releases/download/v0.1.8/manifest.yaml'),
]:
    with urllib.request.urlopen(url, timeout=60) as response:
        documents = list(yaml.safe_load_all(response.read()))
    output = []
    for document in documents:
        if not document or document['kind'] == 'Namespace':
            continue
        if document['kind'] == 'CustomResourceDefinition':
            save_crd(document)
            continue
        if name == 'scale-to-zero':
            if document['kind'] == 'ClusterRole' and document['metadata']['name'] == 'cnpg-scale-to-zero-sidecar-role':
                document['rules'].append({'apiGroups':['xata.io'],'resources':['branches'],'verbs':['get','update','patch']})
            if document['kind'] == 'Deployment':
                document['spec']['template']['spec']['containers'][0].setdefault('env', []).append({
                    'name':'SIDECAR_IMAGE',
                    'value':'ghcr.io/xataio/xata/scale-to-zero-sidecar@sha256:e38118ea1fc21dcb2c698cd6252e98456ad6ee7f537110e97e94e6663fd73f33',
                })
        rendered = yaml.safe_dump(document, sort_keys=False).replace('cnpg-system','{{ .Release.Namespace }}')
        if document['kind'] in ['Certificate','Issuer']:
            certificates.append(rendered)
        else:
            output.append(rendered)
    (CHART/'templates'/('plugin-'+name+'.yaml')).write_text('{{- if .Values.plugins.enabled }}\n'+'---\n'.join(output)+'\n{{- end }}\n')
(CHART/'files/plugin-certificates.yaml').write_text('---\n'.join(certificates))
print('Updated pinned assets. Review annotations, RBAC and image changes before release.')
