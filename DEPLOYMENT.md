# Distributed Deployment (EC2 + Docker + Postgres)

## Overview
- One EC2 VM runs the **central coordinator server** (`server.py`) + Postgres.
- Any number of EC2 worker VMs run `coordinator.py`.
- Workers claim jobs from central APIs, heartbeat leases, upload artifacts/session checkpoints, and continue pipeline stages (`nightmare -> fozzy -> extractor`) without duplicate locks.

## 1) Build Image
From repo root:

```bash
docker build -t nightmare:latest .
```

## 2) Central VM
1. Copy repo to the central VM.
2. Copy `deploy/.env.example` to `deploy/.env` and set real values.
3. Ensure TLS cert/key files exist on host (for port 443).
4. Start:

```bash
cd deploy
docker compose -f docker-compose.central.yml --env-file .env up -d --build
```

### Fast path (auto-generate secrets + certs + env)
On central VM:

```bash
chmod +x deploy/bootstrap-central-auto.sh
./deploy/bootstrap-central-auto.sh
```

Optional:

```bash
./deploy/bootstrap-central-auto.sh --base-url https://your-public-hostname --force
```

This script:
- generates a strong Postgres password and coordinator API token,
- detects a base URL from EC2 metadata/public IP (unless `--base-url` is set),
- creates self-signed TLS cert/key in `deploy/tls/`,
- writes `deploy/.env`,
- writes `deploy/worker.env.generated` for worker VMs,
- installs missing Linux dependencies automatically on `yum`/`dnf` hosts (with `apt-get` fallback) for `docker`, `docker compose`, `curl`, and `openssl`,
- rebuilds and starts the central stack.

Bootstrap scripts support both Compose CLIs:
- `docker compose` (plugin)
- `docker-compose` (standalone fallback on older images)

Amazon Linux note:
- Package installs use `curl-minimal` on `yum`/`dnf` to avoid `curl` vs `curl-minimal` conflicts on AL2023.
- If `docker-compose-plugin` is not available in distro repos, bootstrap falls back to installing standalone `docker-compose` binary automatically.

### Fully automatic: central + 20 workers from central machine
If AWS CLI is configured on central, run:

```bash
chmod +x deploy/bootstrap-central-auto.sh deploy/provision-workers-aws.sh
./deploy/bootstrap-central-auto.sh \
  --auto-provision-workers 20 \
  --aws-ami-id ami-xxxxxxxx \
  --aws-subnet-id subnet-xxxxxxxx \
  --aws-security-group-ids sg-aaaa,sg-bbbb \
  --aws-instance-type t3.small \
  --repo-url https://github.com/<owner>/<repo>.git \
  --repo-branch main \
  --aws-region us-east-1
```

Optional AWS flags:
- `--aws-key-name <keypair>`
- `--aws-iam-instance-profile <instance-profile-name>`

The central bootstrap will:
- start coordinator + Postgres,
- generate secure `.env` and worker token file,
- launch worker EC2 instances,
- pass cloud-init that installs dependencies, clones repo, and starts worker containers automatically.

`provision-workers-aws.sh` automatic coordinator settings:
- If `--coordinator-base-url` and `--api-token` are omitted, the script auto-loads them from `deploy/.env` (`COORDINATOR_BASE_URL`, `COORDINATOR_API_TOKEN`).
- This allows running worker provisioning directly on the central VM without manually copying token/URL flags.

AWS credentials requirement for auto-provisioning:
- The central VM running `bootstrap-central-auto.sh --auto-provision-workers ...` must have valid AWS credentials.
- Best practice: attach an IAM instance profile role to the central EC2 instance with:
  - `ec2:RunInstances`
  - `ec2:DescribeInstances`
  - `ec2:CreateTags`
  - `ec2:DescribeSubnets`
  - `ec2:DescribeSecurityGroups`
  - `iam:PassRole` (only if using `--aws-iam-instance-profile`)

### Windows fast path (PowerShell)
On central Windows VM:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\bootstrap-windows.ps1 -Role Central
```

Optional:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\bootstrap-windows.ps1 -Role Central -BaseUrl https://your-public-hostname -Force
```

On worker Windows VM:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\bootstrap-windows.ps1 -Role Worker -BaseUrl https://<central-host> -ApiToken <COORDINATOR_API_TOKEN>
```

If `deploy/worker.env.generated` from central is copied to the worker, you can omit `-BaseUrl` and `-ApiToken`.

## 3) Register Targets
Use coordinator API token:

```bash
python register_targets.py \
  --server-base-url https://<central-host> \
  --api-token <COORDINATOR_API_TOKEN> \
  --targets-file targets.txt
```

## 4) Worker VM(s)
1. Copy repo to each worker VM.
2. Use the generated central file `deploy/worker.env.generated` as the worker `deploy/.env`.
3. Set `config/coordinator.json` (or env vars) with:
   - `server_base_url` (or `COORDINATOR_BASE_URL` env)
   - `api_token` (or `COORDINATOR_API_TOKEN` env)
4. Start worker container:

```bash
cd deploy
docker compose -f docker-compose.worker.yml --env-file .env up -d --build
```

## Security / Networking
- Open inbound ports:
  - `443/tcp` to worker VMs (and admins) for coordinator API.
  - `80/tcp` optional (HTTP endpoint).
- AWS security group baseline:
  - Allow `443` from worker subnet/security-group only.
  - Optional allow `80` from admin IPs or disable `http_port`.
  - Do not expose Postgres (`5432`) publicly; keep internal only.
- Restrict Postgres to internal container/network only on central VM.
- Use strong `COORDINATOR_API_TOKEN`.
- Do not hardcode API keys in images; pass via env or secret manager.

## Resume / Locking Model
- Target queue lock: `/api/coord/claim` + lease heartbeat + `/complete`.
- Stage queue lock: `/api/coord/stage/claim` + heartbeat + `/complete`.
- Session checkpoint: workers periodically POST `/api/coord/session` while Nightmare runs.
- Artifact replication: workers upload/download artifacts through `/api/coord/artifact` so other VMs can continue.

## Worker Status API (central server)
Use this endpoint from the central machine to see all known worker VM process identities and current heartbeat/lease state:

```bash
curl -sk \
  -H "Authorization: Bearer <COORDINATOR_API_TOKEN>" \
  "https://<central-host>/api/coord/workers"
```

Optional stale threshold override (seconds):

```bash
curl -sk \
  -H "Authorization: Bearer <COORDINATOR_API_TOKEN>" \
  "https://<central-host>/api/coord/workers?stale_after_seconds=180"
```

Response includes:
- aggregate `counts` (`total_workers`, `online_workers`, `stale_workers`),
- per-worker `status` (`online`/`stale`),
- `last_heartbeat_at_utc` + `seconds_since_heartbeat`,
- active/running lease counts for targets and stage tasks,
- active stage names for currently-running stage work.
