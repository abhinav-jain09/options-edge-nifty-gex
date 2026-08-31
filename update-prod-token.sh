#!/usr/bin/env bash
# Rotate the Dhan access token in prod after regenerating it in Dhan web.
# Usage: put the new token in .env first (DHAN_ACCESS_TOKEN=...), then run this.
set -euo pipefail
cd "$(dirname "$0")"
set -a; source ./.env; set +a
export KUBECONFIG=~/.kube/prod-k3s.yaml
AS=(--as=system:serviceaccount:options-edge:jenkins-deployer)
kubectl "${AS[@]}" -n options-edge create secret generic dhan-credentials \
  --from-literal=DHAN_ACCESS_TOKEN="$DHAN_ACCESS_TOKEN" \
  --from-literal=DHAN_CLIENT_ID="$DHAN_CLIENT_ID" \
  --dry-run=client -o yaml | kubectl "${AS[@]}" apply -f -
kubectl "${AS[@]}" -n options-edge rollout restart deploy/nifty-gex-service
kubectl -n options-edge rollout status deploy/nifty-gex-service --timeout=120s
curl -sS https://bleadingoptions.com/nifty-gex/health; echo
