#!/usr/bin/env bash
set -euo pipefail

: "${AWS_REGION:?Set AWS_REGION}"
: "${ECS_CLUSTER:?Set ECS_CLUSTER}"
: "${WORKER_SPIDER_SERVICE:?Set WORKER_SPIDER_SERVICE}"
: "${COMMAND_CENTER_URL:?Set COMMAND_CENTER_URL}"

target="${HTTP_QUEUE_TARGET_BACKLOG_PER_TASK:-100}"
min_tasks="${HTTP_QUEUE_MIN_TASKS:-1}"
max_tasks="${HTTP_QUEUE_MAX_TASKS:-50}"

queue_json="$(curl -fsS "${COMMAND_CENTER_URL%/}/api/http-request-queue?take=5000")"

queued_count="$(
  printf '%s' "$queue_json"     | python3 -c 'import json,sys; rows=json.load(sys.stdin); print(sum(1 for r in rows if str(r.get("state","")).lower() in ("queued","retryscheduled","retry_scheduled")))'
)"

running_count="$(
  aws ecs describe-services     --region "$AWS_REGION"     --cluster "$ECS_CLUSTER"     --services "$WORKER_SPIDER_SERVICE"     --query 'services[0].runningCount'     --output text
)"

if [[ "$running_count" == "None" || -z "$running_count" ]]; then
  running_count=0
fi

desired=$(( (queued_count + target - 1) / target ))

if (( desired < min_tasks )); then desired="$min_tasks"; fi
if (( desired > max_tasks )); then desired="$max_tasks"; fi

current_desired="$(
  aws ecs describe-services     --region "$AWS_REGION"     --cluster "$ECS_CLUSTER"     --services "$WORKER_SPIDER_SERVICE"     --query 'services[0].desiredCount'     --output text
)"

echo "HTTP queue queued=${queued_count} running=${running_count} currentDesired=${current_desired} targetBacklogPerTask=${target} nextDesired=${desired}"

if [[ "$current_desired" != "$desired" ]]; then
  aws ecs update-service     --region "$AWS_REGION"     --cluster "$ECS_CLUSTER"     --service "$WORKER_SPIDER_SERVICE"     --desired-count "$desired" >/dev/null

  echo "Updated ${WORKER_SPIDER_SERVICE} desired count to ${desired}"
else
  echo "No scale change needed"
fi
