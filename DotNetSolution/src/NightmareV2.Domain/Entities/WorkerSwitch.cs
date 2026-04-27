namespace NightmareV2.Domain.Entities;

/// <summary>Operator-controlled enablement for a worker type reacting to the bus.</summary>
public sealed class WorkerSwitch
{
    public string WorkerKey { get; set; } = "";
    public bool IsEnabled { get; set; } = true;
    public DateTimeOffset UpdatedAtUtc { get; set; }
}
