namespace NightmareV2.CommandCenter.Models;

public sealed record CreateTargetRequest(string RootDomain, int GlobalMaxDepth = 12);

public sealed record UpdateTargetRequest(string RootDomain, int GlobalMaxDepth = 12);

public sealed record BulkImportRequest(IReadOnlyList<string>? Domains, int GlobalMaxDepth = 12);

public sealed record BulkImportResult(int Created, int SkippedAlreadyExist, int SkippedEmptyOrInvalid, int SkippedDuplicateInBatch);

public sealed record TargetSummary(Guid Id, string RootDomain, int GlobalMaxDepth, DateTimeOffset CreatedAtUtc);
