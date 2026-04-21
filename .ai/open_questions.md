# Open Questions

- `production_checklist.txt` now exists in both repo root and `config/production_checklist.txt`. Confirm the canonical location so checklist state does not drift.
- `fozzy.py` currently hard-fails if quick fuzz list is missing. Confirm whether fallback defaults should allow dry/incremental preflight without that file.
- For thin-shell Fozzy HTML reports that load summary JSON at runtime, confirm whether the expected usage is via local HTTP hosting (instead of `file://`) due browser local-file fetch restrictions.
- For master HTML log viewer, confirm whether reading logs via browser fetch/XHR from local files should be considered supported only under local HTTP hosting.
- Environment risk observed on 2026-04-13: `OSError: [Errno 28] No space left on device` while regenerating `output/all_domains.results_summary.json`; until disk space is freed, master summary regeneration may fail and existing master JSON may be stale/corrupt.
- 2026-04-13: Should secret baking into Docker image remain enabled long-term? Current setup supports it per operator request, but this increases blast radius if an image is leaked. Consider moving to runtime-only secrets via AWS SSM/Secrets Manager.
- Dedicated log-database host provisioning is now supported at compose level (`deploy/docker-compose.log-store.yml`) and via `LOG_DATABASE_URL`, but automatic bootstrap/provision scripts do not yet create/manage a separate EC2 log server in one command.
- Full DB-transaction capture is partially addressed via existing Postgres container logs; if full SQL statement-level auditing is required, postgres config flags (`log_statement`, `log_min_duration_statement`) still need explicit operator tuning.
- Validate VPC/Security Group rules for the log DB VM: inbound TCP/5432 must allow traffic from the central server security group/private subnet, otherwise central startup will fail with required log DB connectivity errors.
- Worker VM full-log downloads now avoid explicit byte caps in app code, but AWS SSM command-plugin output may still impose service-side size limits. Confirm whether we should switch remote log-download path to SSM output-to-S3 (or chunked retrieval) for guaranteed unbounded exports.
- Register-target full-replace currently truncates only `coordinator_targets`. Confirm whether stage/session/artifact tables should also be cleaned when replacing the target list to avoid stale non-target rows for removed domains.

- Repeated runtime failure still unresolved in crawler logic: `nightmare.py` crash `NameError: name 'verify_timeout' is not defined` (seen in uploaded nightmare logs, around `crawl_domain` path). This coordinator patch improves reporting but does not fix the underlying crawler bug.
- If full deploy still reports API unreachable with coordinator container up, likely external dependency: `LOG_DATABASE_URL` path/connectivity (SG/NACL/route/credentials). New probe diagnostics should now show all URL probe codes and reveal this explicitly in server logs.
