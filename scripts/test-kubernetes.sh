#!/usr/bin/env bash
set -euo pipefail

rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT
kubectl kustomize k8s > "$rendered"
grep -q '^apiVersion: batch/v1' "$rendered"
grep -q '^kind: Job' "$rendered"
grep -q '^kind: ServiceAccount' "$rendered"
grep -q 'runAsNonRoot: true' "$rendered"
grep -q 'readOnlyRootFilesystem: true' "$rendered"
grep -q 'automountServiceAccountToken: false' "$rendered"
echo "Offline Kubernetes render and security assertions passed"
