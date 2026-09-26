#!/usr/bin/env bash
# Delete only the named, labeled disposable test VM and its dedicated network.
set -euo pipefail
project=${1:?Usage: delete-gcp-test-host.sh PROJECT ZONE xata-chart-NAME}
zone=${2:?Missing zone}
name=${3:?Missing test VM name}
region=${zone%-*}
case "$name" in xata-chart-*) ;; *) exit 2;; esac
instance=$(gcloud compute instances list --project "$project" --filter="name=$name" --format='value(name)')
if [[ -n "$instance" ]]; then
  purpose=$(gcloud compute instances describe "$name" --project "$project" --zone "$zone" --format='value(labels.purpose)')
  [[ "$purpose" == xata-chart-tests ]] || { echo 'Refusing to delete a VM without the test label' >&2; exit 2; }
  gcloud compute instances delete "$name" --project "$project" --zone "$zone" --quiet
fi
# The boot disk is created with autoDelete=true; never silently leave one billed.
disks=$(gcloud compute disks list --project "$project" --filter="name=$name" --format='value(name)')
[[ -z "$disks" ]] || { echo "Boot disk remains: $disks; inspect before cleanup" >&2; exit 1; }
for resource in firewall-rules networks; do
  target="$name"
  [[ "$resource" == firewall-rules ]] && target="$name-iap"
  if [[ "$resource" == networks ]]; then
    subnet=$(gcloud compute networks subnets list --project "$project" --filter="name=$name" --format='value(name)')
    if [[ -n "$subnet" ]]; then
      gcloud compute networks subnets delete "$name" --project "$project" --region "$region" --quiet
    fi
  fi
  found=$(gcloud compute "$resource" list --project "$project" --filter="name=$target" --format='value(name)')
  if [[ -n "$found" ]]; then gcloud compute "$resource" delete "$target" --project "$project" --quiet; fi
done
echo "PASS: $name VM, boot disk, firewall, subnet and network removed"
