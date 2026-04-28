namespace NightmareV2.CommandCenter.Models;

public sealed record MaintenanceStatusDto(bool Enabled, bool ApiKeyConfigured);

public sealed record MaintenancePhraseBody(string ConfirmationPhrase);

public sealed record MaintenanceClearDomainBody(string RootDomain, string ConfirmationPhrase);

public sealed record MaintenanceClearAssetsFilteredBody(
    string ConfirmationPhrase,
    string? Kind,
    string? LifecycleStatus,
    string? DiscoveredByContains);

public sealed record MaintenanceDeleteResult(string Operation, long RowsDeleted, string? Note);
