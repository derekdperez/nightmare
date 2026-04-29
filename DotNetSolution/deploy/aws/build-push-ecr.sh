#!/usr/bin/env bash
set -euo pipefail

: "${AWS_REGION:?Set AWS_REGION}"
: "${AWS_ACCOUNT_ID:?Set AWS_ACCOUNT_ID}"
: "${ECR_PREFIX:=nightmare-v2}"
: "${IMAGE_TAG:=latest}"

registry="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

aws ecr get-login-password --region "$AWS_REGION"   | docker login --username AWS --password-stdin "$registry"

build_and_push() {
  local service="$1"
  local dockerfile="$2"
  local project_dir="$3"
  local app_dll="$4"
  local image="${registry}/${ECR_PREFIX}/${service}:${IMAGE_TAG}"

  echo "Building ${image}"
  docker build     -f "$dockerfile"     --build-arg PROJECT_DIR="$project_dir"     --build-arg APP_DLL="$app_dll"     -t "$image"     .

  docker push "$image"
}

build_and_push "command-center" "deploy/Dockerfile.web" "NightmareV2.CommandCenter" "NightmareV2.CommandCenter.dll"
build_and_push "gatekeeper" "deploy/Dockerfile.worker" "NightmareV2.Gatekeeper" "NightmareV2.Gatekeeper.dll"
build_and_push "worker-spider" "deploy/Dockerfile.worker" "NightmareV2.Workers.Spider" "NightmareV2.Workers.Spider.dll"
build_and_push "worker-enum" "deploy/Dockerfile.worker" "NightmareV2.Workers.Enum" "NightmareV2.Workers.Enum.dll"
build_and_push "worker-portscan" "deploy/Dockerfile.worker" "NightmareV2.Workers.PortScan" "NightmareV2.Workers.PortScan.dll"
build_and_push "worker-highvalue" "deploy/Dockerfile.worker" "NightmareV2.Workers.HighValue" "NightmareV2.Workers.HighValue.dll"
