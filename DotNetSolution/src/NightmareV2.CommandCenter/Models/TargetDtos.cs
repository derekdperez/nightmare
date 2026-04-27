namespace NightmareV2.CommandCenter.Models;

public sealed record CreateTargetRequest(string RootDomain, int GlobalMaxDepth = 12);

public sealed record TargetSummary(Guid Id, string RootDomain, int GlobalMaxDepth, DateTimeOffset CreatedAtUtc);
