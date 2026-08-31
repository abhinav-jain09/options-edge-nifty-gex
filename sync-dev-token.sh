#!/usr/bin/env bash
# Copy the CURRENT Dhan token from the prod secret (which the prod pod keeps fresh
# via auto-renewal) into the dev cluster's secret and local .env. Run before scaling
# dev up or running locally — older copies of the token are invalidated by renewal.
set -euo pipefail
cd "$(dirname "$0")"
TOK=$(KUBECONFIG=~/.kube/prod-k3s.yaml kubectl -n options-edge get secret dhan-credentials -o jsonpath='{.data.DHAN_ACCESS_TOKEN}' | base64 -d)
CID=$(KUBECONFIG=~/.kube/prod-k3s.yaml kubectl -n options-edge get secret dhan-credentials -o jsonpath='{.data.DHAN_CLIENT_ID}' | base64 -d)
printf 'DHAN_ACCESS_TOKEN=%s\nDHAN_CLIENT_ID=%s\n' "$TOK" "$CID" > .env
chmod 600 .env
AS=(--context docker-desktop --as=system:serviceaccount:options-edge:jenkins-deployer)
kubectl "${AS[@]}" -n options-edge create secret generic dhan-credentials \
  --from-literal=DHAN_ACCESS_TOKEN="$TOK" --from-literal=DHAN_CLIENT_ID="$CID" \
  --dry-run=client -o yaml | kubectl "${AS[@]}" apply -f -
echo "dev secret + .env synced from prod"
