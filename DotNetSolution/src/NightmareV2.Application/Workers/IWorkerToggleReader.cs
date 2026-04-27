namespace NightmareV2.Application.Workers;

public interface IWorkerToggleReader
{
    Task<bool> IsWorkerEnabledAsync(string workerKey, CancellationToken cancellationToken = default);
}
