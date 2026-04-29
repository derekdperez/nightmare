using Microsoft.EntityFrameworkCore;
using NightmareV2.Domain.Entities;

namespace NightmareV2.Infrastructure.Data;

/// <summary>
/// EF <c>EnsureCreated</c> does not add new columns to existing databases; run these patches after it on upgrade.
/// </summary>
public static class NightmareDbSchemaPatches
{
    public static async Task ApplyAfterEnsureCreatedAsync(NightmareDbContext db, CancellationToken cancellationToken = default)
    {
        await db.Database.ExecuteSqlRawAsync(
                """
                CREATE EXTENSION IF NOT EXISTS pgcrypto;
                """,
                cancellationToken)
            .ConfigureAwait(false);

        await db.Database.ExecuteSqlRawAsync(
                """
                ALTER TABLE bus_journal ADD COLUMN IF NOT EXISTS host_name character varying(256) NOT NULL DEFAULT '';
                """,
                cancellationToken)
            .ConfigureAwait(false);

        await db.Database.ExecuteSqlRawAsync(
                """
                DO $patch$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = 'bus_journal' AND column_name = 'consumer_type'
                    ) THEN
                        ALTER TABLE bus_journal ALTER COLUMN consumer_type TYPE character varying(512);
                    ELSIF EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'bus_journal'
                    ) THEN
                        ALTER TABLE bus_journal ADD COLUMN consumer_type character varying(512);
                    END IF;
                END
                $patch$;
                """,
                cancellationToken)
            .ConfigureAwait(false);

        await db.Database.ExecuteSqlRawAsync(
                """
                ALTER TABLE stored_assets ADD COLUMN IF NOT EXISTS discovery_context character varying(512) NOT NULL DEFAULT '';
                """,
                cancellationToken)
            .ConfigureAwait(false);

        await db.Database.ExecuteSqlRawAsync(
                """
                CREATE TABLE IF NOT EXISTS high_value_findings (
                    id uuid NOT NULL PRIMARY KEY,
                    target_id uuid NOT NULL REFERENCES recon_targets("Id") ON DELETE CASCADE,
                    source_asset_id uuid NULL,
                    finding_type character varying(64) NOT NULL,
                    severity character varying(32) NOT NULL,
                    pattern_name character varying(256) NOT NULL,
                    category character varying(128) NULL,
                    matched_text text NULL,
                    source_url character varying(4096) NOT NULL,
                    worker_name character varying(128) NOT NULL,
                    importance_score integer NULL,
                    discovered_at_utc timestamp with time zone NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_high_value_findings_target_id ON high_value_findings (target_id);
                CREATE INDEX IF NOT EXISTS ix_high_value_findings_discovered_at ON high_value_findings (discovered_at_utc DESC);
                """,
                cancellationToken)
            .ConfigureAwait(false);

        await db.Database.ExecuteSqlRawAsync(
                """
                CREATE TABLE IF NOT EXISTS outbox_messages (
                    id uuid NOT NULL PRIMARY KEY,
                    message_type character varying(512) NOT NULL,
                    payload_json text NOT NULL,
                    event_id uuid NOT NULL,
                    correlation_id uuid NOT NULL,
                    causation_id uuid NOT NULL,
                    occurred_at_utc timestamp with time zone NOT NULL,
                    producer character varying(128) NOT NULL,
                    state character varying(32) NOT NULL DEFAULT 'Pending',
                    attempt_count integer NOT NULL DEFAULT 0,
                    created_at_utc timestamp with time zone NOT NULL DEFAULT now(),
                    updated_at_utc timestamp with time zone NOT NULL DEFAULT now(),
                    next_attempt_at_utc timestamp with time zone NOT NULL DEFAULT now(),
                    dispatched_at_utc timestamp with time zone NULL,
                    last_error character varying(2048) NULL,
                    locked_by character varying(256) NULL,
                    locked_until_utc timestamp with time zone NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_messages_event_id ON outbox_messages (event_id);
                CREATE INDEX IF NOT EXISTS ix_outbox_messages_state_next_attempt ON outbox_messages (state, next_attempt_at_utc);
                CREATE INDEX IF NOT EXISTS ix_outbox_messages_created_at ON outbox_messages (created_at_utc DESC);

                CREATE TABLE IF NOT EXISTS inbox_messages (
                    id uuid NOT NULL PRIMARY KEY,
                    event_id uuid NOT NULL,
                    consumer character varying(256) NOT NULL,
                    processed_at_utc timestamp with time zone NOT NULL DEFAULT now()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_inbox_messages_event_consumer ON inbox_messages (event_id, consumer);
                """,
                cancellationToken)
            .ConfigureAwait(false);



        await db.Database.ExecuteSqlRawAsync(
                """
                CREATE TABLE IF NOT EXISTS http_request_queue_settings (
                    id integer NOT NULL PRIMARY KEY,
                    enabled boolean NOT NULL DEFAULT true,
                    global_requests_per_minute integer NOT NULL DEFAULT 120,
                    per_domain_requests_per_minute integer NOT NULL DEFAULT 6,
                    max_concurrency integer NOT NULL DEFAULT 8,
                    request_timeout_seconds integer NOT NULL DEFAULT 30,
                    updated_at_utc timestamp with time zone NOT NULL DEFAULT now()
                );

                INSERT INTO http_request_queue_settings (
                    id,
                    enabled,
                    global_requests_per_minute,
                    per_domain_requests_per_minute,
                    max_concurrency,
                    request_timeout_seconds,
                    updated_at_utc
                )
                VALUES (1, true, 120, 6, 8, 30, now())
                ON CONFLICT (id) DO NOTHING;

                CREATE TABLE IF NOT EXISTS http_request_queue (
                    id uuid NOT NULL PRIMARY KEY,
                    asset_id uuid NOT NULL REFERENCES stored_assets("Id") ON DELETE CASCADE,
                    target_id uuid NOT NULL,
                    asset_kind integer NOT NULL,
                    method character varying(16) NOT NULL DEFAULT 'GET',
                    request_url character varying(4096) NOT NULL,
                    domain_key character varying(253) NOT NULL,
                    state character varying(32) NOT NULL DEFAULT 'Queued',
                    priority integer NOT NULL DEFAULT 0,
                    attempt_count integer NOT NULL DEFAULT 0,
                    max_attempts integer NOT NULL DEFAULT 3,
                    created_at_utc timestamp with time zone NOT NULL DEFAULT now(),
                    updated_at_utc timestamp with time zone NOT NULL DEFAULT now(),
                    next_attempt_at_utc timestamp with time zone NOT NULL DEFAULT now(),
                    locked_by character varying(256) NULL,
                    locked_until_utc timestamp with time zone NULL,
                    started_at_utc timestamp with time zone NULL,
                    completed_at_utc timestamp with time zone NULL,
                    duration_ms bigint NULL,
                    last_http_status integer NULL,
                    last_error character varying(2048) NULL,
                    request_headers_json text NULL,
                    request_body text NULL,
                    response_headers_json text NULL,
                    response_body text NULL,
                    response_content_type character varying(256) NULL,
                    response_content_length bigint NULL,
                    final_url character varying(4096) NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS ux_http_request_queue_asset_id ON http_request_queue (asset_id);
                CREATE INDEX IF NOT EXISTS ix_http_request_queue_state_next_attempt ON http_request_queue (state, next_attempt_at_utc);
                CREATE INDEX IF NOT EXISTS ix_http_request_queue_domain_started ON http_request_queue (domain_key, started_at_utc);
                CREATE INDEX IF NOT EXISTS ix_http_request_queue_created_at ON http_request_queue (created_at_utc DESC);
                """,
                cancellationToken)
            .ConfigureAwait(false);

        await BackfillLegacyDiscoveredAssetsAsync(db, cancellationToken).ConfigureAwait(false);
        await BackfillHttpRequestQueueAsync(db, cancellationToken).ConfigureAwait(false);
    }



    private static async Task BackfillHttpRequestQueueAsync(NightmareDbContext db, CancellationToken cancellationToken)
    {
        await db.Database.ExecuteSqlRawAsync(
                """
                INSERT INTO http_request_queue (
                    id,
                    asset_id,
                    target_id,
                    asset_kind,
                    method,
                    request_url,
                    domain_key,
                    state,
                    priority,
                    created_at_utc,
                    updated_at_utc,
                    next_attempt_at_utc
                )
                SELECT
                    gen_random_uuid(),
                    a."Id",
                    a."TargetId",
                    a."Kind",
                    'GET',
                    CASE
                        WHEN a."Kind" IN (0, 1) THEN 'https://' || trim(trailing '/' from a."RawValue") || '/'
                        WHEN position('://' in a."RawValue") > 0 THEN a."RawValue"
                        ELSE 'https://' || a."RawValue"
                    END,
                    lower(
                        CASE
                            WHEN a."Kind" IN (0, 1) THEN trim(trailing '/' from a."RawValue")
                            ELSE regexp_replace(regexp_replace(a."RawValue", '^[a-zA-Z][a-zA-Z0-9+.-]*://', ''), '[:/].*$', '')
                        END
                    ),
                    'Queued',
                    0,
                    COALESCE(a."DiscoveredAtUtc", now()),
                    now(),
                    now()
                FROM stored_assets a
                WHERE a."LifecycleStatus" = 'Queued'
                  AND a."Kind" IN (0, 1, 10, 11, 12, 33)
                  AND NOT EXISTS (
                      SELECT 1 FROM http_request_queue q WHERE q.asset_id = a."Id"
                  );
                """,
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Normalize legacy statuses after introducing Queued as the default initial status.</summary>
    private static async Task BackfillLegacyDiscoveredAssetsAsync(NightmareDbContext db, CancellationToken cancellationToken)
    {
        await db.Assets
            .Where(a => a.LifecycleStatus == "Discovered")
            .ExecuteUpdateAsync(
                s => s.SetProperty(a => a.LifecycleStatus, AssetLifecycleStatus.Queued),
                cancellationToken)
            .ConfigureAwait(false);
    }
}
