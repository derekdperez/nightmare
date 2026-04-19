#!/usr/bin/env bash
set -euo pipefail

# Edit these values, then run this script from repo root:
#   ./deploy/deploy-central-with-logdb.sh

AUTO_PROVISION_WORKERS="${AUTO_PROVISION_WORKERS:-5}"
AWS_AMI_ID="${AWS_AMI_ID:-ami-REPLACE_ME}"
AWS_INSTANCE_TYPE="${AWS_INSTANCE_TYPE:-m7i-flex.large}"
AWS_SUBNET_ID="${AWS_SUBNET_ID:-subnet-REPLACE_ME}"
AWS_SECURITY_GROUP_IDS="${AWS_SECURITY_GROUP_IDS:-sg-REPLACE_ME}"
AWS_IAM_INSTANCE_PROFILE="${AWS_IAM_INSTANCE_PROFILE:-ec2}"
AWS_KEY_NAME="${AWS_KEY_NAME:-kp1}"
AWS_REGION="${AWS_REGION:-us-east-1}"
REPO_URL="${REPO_URL:-https://github.com/derekdperez/nightmare.git}"
REPO_BRANCH="${REPO_BRANCH:-main}"

./deploy/bootstrap-central-auto.sh \
  --auto-provision-workers "${AUTO_PROVISION_WORKERS}" \
  --aws-ami-id "${AWS_AMI_ID}" \
  --aws-instance-type "${AWS_INSTANCE_TYPE}" \
  --aws-subnet-id "${AWS_SUBNET_ID}" \
  --aws-security-group-ids "${AWS_SECURITY_GROUP_IDS}" \
  --aws-iam-instance-profile "${AWS_IAM_INSTANCE_PROFILE}" \
  --aws-key-name "${AWS_KEY_NAME}" \
  --aws-region "${AWS_REGION}" \
  --repo-url "${REPO_URL}" \
  --repo-branch "${REPO_BRANCH}"
