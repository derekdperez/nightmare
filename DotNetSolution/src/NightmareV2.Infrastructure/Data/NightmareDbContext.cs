using Microsoft.EntityFrameworkCore;
using NightmareV2.Domain.Entities;

namespace NightmareV2.Infrastructure.Data;

public sealed class NightmareDbContext(DbContextOptions<NightmareDbContext> options) : DbContext(options)
{
    public DbSet<ReconTarget> Targets => Set<ReconTarget>();
    public DbSet<StoredAsset> Assets => Set<StoredAsset>();
    public DbSet<BusJournalEntry> BusJournal => Set<BusJournalEntry>();
    public DbSet<WorkerSwitch> WorkerSwitches => Set<WorkerSwitch>();

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
            e.Property(x => x.CanonicalKey).HasMaxLength(2048).IsRequired();
            e.Property(x => x.RawValue).HasMaxLength(4096).IsRequired();
            e.Property(x => x.DiscoveredBy).HasMaxLength(128).IsRequired();
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
            e.Property(x => x.ConsumerType).HasMaxLength(256);
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
    }
}
