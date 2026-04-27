namespace NightmareV2.Domain.Entities;

public class ReconTarget
{
    public Guid Id { get; set; }
    public string RootDomain { get; set; } = "";
    public int GlobalMaxDepth { get; set; } = 10;
    public DateTimeOffset CreatedAtUtc { get; set; }
}
