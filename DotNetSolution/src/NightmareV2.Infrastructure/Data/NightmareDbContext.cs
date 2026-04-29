using Microsoft.EntityFrameworkCore;
using NightmareV2.Domain.Entities;

namespace NightmareV2.Infrastructure.Data;

public sealed class NightmareDbContext(DbContextOptions<NightmareDbContext> options) : DbContext(options)
{
    public DbSet<ReconTarget> Targets => Set<ReconTarget>();
    public DbSet<StoredAsset> Assets => Set<StoredAsset>();
    public DbSet<BusJournalEntry> BusJournal => Set<BusJournalEntry>();
    public DbSet<WorkerSwitch> WorkerSwitches => Set<WorkerSwitch>();
    public DbSet<HighValueFinding> HighValueFindings => Set<HighValueFinding>();
    public DbSet<HttpRequestQueueItem> HttpRequestQueue => Set<HttpRequestQueueItem>();
    public DbSet<HttpRequestQueueSettings> HttpRequestQueueSettings => Set<HttpRequestQueueSettings>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ReconTarget>(e =>
        {
            e.ToTable("recon_targets");
            e.HasKey(x => x.Id);
            e.Property(x => x.RootDomain).HasMaxLength(253).IsRequired();
            e.HasIndex(x => x.RootDomain).IsUnique();
        });

        modelBuilder.Entity<StoredAsset>(e =>
        {
            e.ToTable("stored_assets");
            e.HasKey(x => x.Id);
            // Match raw SQL patches / FKs (PostgreSQL: unquoted "id"); legacy DBs may have quoted "Id" — see NightmareDbSchemaPatches.
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.CanonicalKey).HasMaxLength(2048).IsRequired();
            e.Property(x => x.RawValue).HasMaxLength(4096).IsRequired();
            e.Property(x => x.DiscoveredBy).HasMaxLength(128).IsRequired();
            e.Property(x => x.DiscoveryContext).HasMaxLength(512).IsRequired().HasColumnName("discovery_context");
            e.Property(x => x.LifecycleStatus).HasMaxLength(32).IsRequired();
            e.Property(x => x.TypeDetailsJson);
            e.HasIndex(x => new { x.TargetId, x.CanonicalKey }).IsUnique();
            e.HasOne(x => x.Target)
                .WithMany()
                .HasForeignKey(x => x.TargetId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<BusJournalEntry>(e =>
        {
            e.ToTable("bus_journal");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).UseIdentityAlwaysColumn();
            e.Property(x => x.Direction).HasMaxLength(16).IsRequired();
            e.Property(x => x.MessageType).HasMaxLength(256).IsRequired();
            e.Property(x => x.ConsumerType).HasMaxLength(512);
            e.Property(x => x.PayloadJson).IsRequired();
            e.Property(x => x.HostName).HasMaxLength(256).IsRequired().HasColumnName("host_name");
            e.HasIndex(x => x.OccurredAtUtc);
        });

        modelBuilder.Entity<WorkerSwitch>(e =>
        {
            e.ToTable("worker_switches");
            e.HasKey(x => x.WorkerKey);
            e.Property(x => x.WorkerKey).HasMaxLength(64);
        });



        modelBuilder.Entity<HttpRequestQueueItem>(e =>
        {
            e.ToTable("http_request_queue");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.AssetId).HasColumnName("asset_id");
            e.Property(x => x.TargetId).HasColumnName("target_id");
            e.Property(x => x.AssetKind).HasColumnName("asset_kind");
            e.Property(x => x.Method).HasColumnName("method").HasMaxLength(16).IsRequired();
            e.Property(x => x.RequestUrl).HasColumnName("request_url").HasMaxLength(4096).IsRequired();
            e.Property(x => x.DomainKey).HasColumnName("domain_key").HasMaxLength(253).IsRequired();
            e.Property(x => x.State).HasColumnName("state").HasMaxLength(32).IsRequired();
            e.Property(x => x.Priority).HasColumnName("priority");
            e.Property(x => x.AttemptCount).HasColumnName("attempt_count");
            e.Property(x => x.MaxAttempts).HasColumnName("max_attempts");
            e.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            e.Property(x => x.UpdatedAtUtc).HasColumnName("updated_at_utc");
            e.Property(x => x.NextAttemptAtUtc).HasColumnName("next_attempt_at_utc");
            e.Property(x => x.LockedBy).HasColumnName("locked_by").HasMaxLength(256);
            e.Property(x => x.LockedUntilUtc).HasColumnName("locked_until_utc");
            e.Property(x => x.StartedAtUtc).HasColumnName("started_at_utc");
            e.Property(x => x.CompletedAtUtc).HasColumnName("completed_at_utc");
            e.Property(x => x.DurationMs).HasColumnName("duration_ms");
            e.Property(x => x.LastHttpStatus).HasColumnName("last_http_status");
            e.Property(x => x.LastError).HasColumnName("last_error").HasMaxLength(2048);
            e.Property(x => x.RequestHeadersJson).HasColumnName("request_headers_json");
            e.Property(x => x.RequestBody).HasColumnName("request_body");
            e.Property(x => x.ResponseHeadersJson).HasColumnName("response_headers_json");
            e.Property(x => x.ResponseBody).HasColumnName("response_body");
            e.Property(x => x.ResponseContentType).HasColumnName("response_content_type").HasMaxLength(256);
            e.Property(x => x.ResponseContentLength).HasColumnName("response_content_length");
            e.Property(x => x.FinalUrl).HasColumnName("final_url").HasMaxLength(4096);
            e.HasIndex(x => x.AssetId).IsUnique();
            e.HasIndex(x => new { x.State, x.NextAttemptAtUtc });
            e.HasIndex(x => new { x.DomainKey, x.StartedAtUtc });
            e.HasOne(x => x.Asset)
                .WithMany()
                .HasForeignKey(x => x.AssetId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<HttpRequestQueueSettings>(e =>
        {
            e.ToTable("http_request_queue_settings");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Enabled).HasColumnName("enabled");
            e.Property(x => x.GlobalRequestsPerMinute).HasColumnName("global_requests_per_minute");
            e.Property(x => x.PerDomainRequestsPerMinute).HasColumnName("per_domain_requests_per_minute");
            e.Property(x => x.MaxConcurrency).HasColumnName("max_concurrency");
            e.Property(x => x.RequestTimeoutSeconds).HasColumnName("request_timeout_seconds");
            e.Property(x => x.UpdatedAtUtc).HasColumnName("updated_at_utc");
        });

        modelBuilder.Entity<HighValueFinding>(e =>
        {
            e.ToTable("high_value_findings");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.TargetId).HasColumnName("target_id");
            e.Property(x => x.SourceAssetId).HasColumnName("source_asset_id");
            e.Property(x => x.FindingType).HasColumnName("finding_type").HasMaxLength(64).IsRequired();
            e.Property(x => x.Severity).HasColumnName("severity").HasMaxLength(32).IsRequired();
            e.Property(x => x.PatternName).HasColumnName("pattern_name").HasMaxLength(256).IsRequired();
            e.Property(x => x.Category).HasColumnName("category").HasMaxLength(128);
            e.Property(x => x.MatchedText).HasColumnName("matched_text");
            e.Property(x => x.SourceUrl).HasColumnName("source_url").HasMaxLength(4096).IsRequired();
            e.Property(x => x.WorkerName).HasColumnName("worker_name").HasMaxLength(128).IsRequired();
            e.Property(x => x.ImportanceScore).HasColumnName("importance_score");
            e.Property(x => x.DiscoveredAtUtc).HasColumnName("discovered_at_utc");
            e.HasIndex(x => x.TargetId);
            e.HasIndex(x => x.DiscoveredAtUtc);
            e.HasOne(x => x.Target)
                .WithMany()
                .HasForeignKey(x => x.TargetId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<OutboxMessage>(e =>
        {
            e.ToTable("outbox_messages");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.MessageType).HasColumnName("message_type").HasMaxLength(512).IsRequired();
            e.Property(x => x.PayloadJson).HasColumnName("payload_json").IsRequired();
            e.Property(x => x.EventId).HasColumnName("event_id");
            e.Property(x => x.CorrelationId).HasColumnName("correlation_id");
            e.Property(x => x.CausationId).HasColumnName("causation_id");
            e.Property(x => x.OccurredAtUtc).HasColumnName("occurred_at_utc");
            e.Property(x => x.Producer).HasColumnName("producer").HasMaxLength(128).IsRequired();
            e.Property(x => x.State).HasColumnName("state").HasMaxLength(32).IsRequired();
            e.Property(x => x.AttemptCount).HasColumnName("attempt_count");
            e.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            e.Property(x => x.UpdatedAtUtc).HasColumnName("updated_at_utc");
            e.Property(x => x.NextAttemptAtUtc).HasColumnName("next_attempt_at_utc");
            e.Property(x => x.DispatchedAtUtc).HasColumnName("dispatched_at_utc");
            e.Property(x => x.LastError).HasColumnName("last_error").HasMaxLength(2048);
            e.Property(x => x.LockedBy).HasColumnName("locked_by").HasMaxLength(256);
            e.Property(x => x.LockedUntilUtc).HasColumnName("locked_until_utc");
            e.HasIndex(x => new { x.State, x.NextAttemptAtUtc });
            e.HasIndex(x => x.EventId).IsUnique();
        });

        modelBuilder.Entity<InboxMessage>(e =>
        {
            e.ToTable("inbox_messages");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.EventId).HasColumnName("event_id");
            e.Property(x => x.Consumer).HasColumnName("consumer").HasMaxLength(256).IsRequired();
            e.Property(x => x.ProcessedAtUtc).HasColumnName("processed_at_utc");
            e.HasIndex(x => new { x.EventId, x.Consumer }).IsUnique();
        });
    }
}
