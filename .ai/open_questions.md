# Open Questions

- `production_checklist.txt` currently lives at `config/production_checklist.txt` (not repo root). Confirm whether this is the intended canonical location for future completion tracking.
- `fozzy.py` currently hard-fails if quick fuzz list is missing. Confirm whether fallback defaults should allow dry/incremental preflight without that file.
- For thin-shell Fozzy HTML reports that load summary JSON at runtime, confirm whether the expected usage is via local HTTP hosting (instead of `file://`) due browser local-file fetch restrictions.
- For master HTML log viewer, confirm whether reading logs via browser fetch/XHR from local files should be considered supported only under local HTTP hosting.
- Environment risk observed on 2026-04-13: `OSError: [Errno 28] No space left on device` while regenerating `output/all_domains.results_summary.json`; until disk space is freed, master summary regeneration may fail and existing master JSON may be stale/corrupt.
- 2026-04-13: Should secret baking into Docker image remain enabled long-term? Current setup supports it per operator request, but this increases blast radius if an image is leaked. Consider moving to runtime-only secrets via AWS SSM/Secrets Manager.
- Dedicated log-database host provisioning is now supported at compose level (`deploy/docker-compose.log-store.yml`) and via `LOG_DATABASE_URL`, but automatic bootstrap/provision scripts do not yet create/manage a separate EC2 log server in one command.
- Full DB-transaction capture is partially addressed via existing Postgres container logs; if full SQL statement-level auditing is required, postgres config flags (`log_statement`, `log_min_duration_statement`) still need explicit operator tuning.
- Validate VPC/Security Group rules for the log DB VM: inbound TCP/5432 must allow traffic from the central server security group/private subnet, otherwise central startup will fail with required log DB connectivity errors.
