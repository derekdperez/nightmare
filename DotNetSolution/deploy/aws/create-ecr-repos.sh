#!/usr/bin/env bash
set -euo pipefail

: "${AWS_REGION:?Set AWS_REGION}"
: "${ECR_PREFIX:=nightmare-v2}"

services=(
  command-center
  gatekeeper
  worker-spider
  worker-enum
  worker-portscan
  worker-highvalue
)

for service in "${services[@]}"; do
  repo="${ECR_PREFIX}/${service}"
  aws ecr describe-repositories --region "$AWS_REGION" --repository-names "$repo" >/dev/null 2>&1     || aws ecr create-repository --region "$AWS_REGION" --repository-name "$repo" >/dev/null
  echo "Ensured ECR repository: $repo"
done
