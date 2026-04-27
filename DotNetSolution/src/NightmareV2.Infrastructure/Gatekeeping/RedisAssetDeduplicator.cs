using System.Text.Json;
using NightmareV2.Application.Gatekeeping;
using StackExchange.Redis;

namespace NightmareV2.Infrastructure.Gatekeeping;

public sealed class RedisAssetDeduplicator(IConnectionMultiplexer redis) : IAssetDeduplicator
{
    private const string KeyPrefix = "nm2:dedupe:";

    public async Task<bool> TryReserveAsync(Guid targetId, string canonicalKey, CancellationToken cancellationToken = default)
    {
        var db = redis.GetDatabase();
        var key = KeyPrefix + targetId + ":" + JsonSerializer.Serialize(canonicalKey);
        return await db.StringSetAsync(key, "1", when: When.NotExists).ConfigureAwait(false);
    }

    public async Task ReleaseAsync(Guid targetId, string canonicalKey, CancellationToken cancellationToken = default)
    {
        var db = redis.GetDatabase();
        var key = KeyPrefix + targetId + ":" + JsonSerializer.Serialize(canonicalKey);
        await db.KeyDeleteAsync(key).ConfigureAwait(false);
    }
}
