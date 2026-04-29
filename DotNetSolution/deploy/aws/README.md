# AWS ECS deployment helpers for NightmareV2

These helpers keep the production stack self-hosted: Postgres, Redis, and RabbitMQ remain containers, while the .NET services run as independently scalable ECS services.

## Intent

- No Amazon RDS, ElastiCache, Amazon MQ, or paid custom CloudWatch metrics are required.
- Container images are pushed to ECR.
- ECS services are scaled with the included autoscaler script, which reads the app's own HTTP queue API and calls `aws ecs update-service`.
- `docker-compose.yml` remains the local development deployment.

## Required environment

Copy `.env.example` to `.env` and set the values for your AWS account, region, cluster, and repositories.

## Build and push images

```bash
cd DotNetSolution
set -a
. deploy/aws/.env
set +a
deploy/aws/build-push-ecr.sh
```

## Scale HTTP workers from the app queue

Run this from a small always-on host, a cron job, or a scheduled ECS task:

```bash
set -a
. deploy/aws/.env
set +a
deploy/aws/autoscale-http-workers.sh
```

The autoscaler reads:

```txt
${COMMAND_CENTER_URL}/api/http-request-queue
```

and adjusts the spider worker ECS service because the spider worker drains the durable HTTP request queue.

## ECS service model

Recommended services:

- command-center
- gatekeeper
- worker-spider
- worker-enum
- worker-portscan
- worker-highvalue

Keep Postgres, Redis, and RabbitMQ self-hosted in your own ECS/EC2 containers unless you decide to move to managed AWS services later.
