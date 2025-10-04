// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Microsoft.Extensions.Caching.Distributed;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Interfaces.Classes.Asp;

public class MemoryServiceDistributedCache(IMemoryService memoryService, IMemoryScope scope) : IDistributedCache
{
    private readonly string _scopeCompiled = scope.Compile();

    private MemoryScopeLambda GetScope(string key) => new($"{_scopeCompiled}:{key}");

    private const string KeyAttribute = "key";
    private const string OriginalTtlAttribute = "original-ttl";
    private const string InfiniteTtlValue = "infinite";

    public byte[]? Get(string key)
    {
        return GetAsync(key).GetAwaiter().GetResult();
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken token = new())
    {
        if (string.IsNullOrWhiteSpace(key))
            return null;

        try
        {
            var result = await memoryService.GetKeyValueAsync(GetScope(key), KeyAttribute, token);
            if (!result.IsSuccessful || result.Data == null)
                return null;

            return result.Data.Kind == PrimitiveKind.ByteArray
                ? result.Data.AsByteArray
                : null;
        }
        catch (Exception)
        {
            return null;
        }
    }

    public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        SetAsync(key, value, options).GetAwaiter().GetResult();
    }

    public async Task SetAsync(string key, byte[]? value, DistributedCacheEntryOptions options, CancellationToken token = new())
    {
        if (string.IsNullOrWhiteSpace(key) || value == null)
            return;

        try
        {
            var scope = GetScope(key);

            // Handle expiration
            TimeSpan? expirationSpan = null;
            if (options.AbsoluteExpiration.HasValue)
            {
                var timeToLive = options.AbsoluteExpiration.Value - DateTimeOffset.UtcNow;
                if (timeToLive > TimeSpan.Zero)
                {
                    expirationSpan = timeToLive;
                }
            }
            else if (options.SlidingExpiration.HasValue)
            {
                expirationSpan = options.SlidingExpiration.Value;
            }
            else if (options.AbsoluteExpirationRelativeToNow.HasValue)
            {
                expirationSpan = options.AbsoluteExpirationRelativeToNow.Value;
            }

            await memoryService.SetKeyValuesAsync(
                scope, [
                    new KeyValuePair<string, Primitive>(KeyAttribute, new Primitive(value)),
                    new KeyValuePair<string, Primitive>(OriginalTtlAttribute,
                        new Primitive(expirationSpan.HasValue
                            ? expirationSpan.Value.ToString("c")
                            : InfiniteTtlValue))
                ],
                false,
                token);
            if (expirationSpan.HasValue)
                await memoryService.SetKeyExpireTimeAsync(scope, expirationSpan.Value, token);
        }
        catch (Exception)
        {
            // Ignore errors to match IDistributedCache behavior
        }
    }

    public void Refresh(string key)
    {
        RefreshAsync(key).GetAwaiter().GetResult();
    }

    public async Task RefreshAsync(string key, CancellationToken token = new())
    {
        if (string.IsNullOrWhiteSpace(key))
            return;

        try
        {
            var scope = GetScope(key);

            // Get the original TTL that was stored when the item was cached
            var originalTtlResult = await memoryService.GetKeyValueAsync(scope, OriginalTtlAttribute, token);

            if (originalTtlResult.IsSuccessful && originalTtlResult.Data != null)
            {
                var originalTtlString = originalTtlResult.Data.AsString;
                if (originalTtlString == InfiniteTtlValue) return;
                if (TimeSpan.TryParseExact(originalTtlString, "c", null, out var originalTtl))
                {
                    // Reset expiration to the full original sliding window
                    await memoryService.SetKeyExpireTimeAsync(scope, originalTtl, token);
                }
            }
        }
        catch (Exception)
        {
            // Ignore errors to match IDistributedCache behavior
        }
    }

    public void Remove(string key)
    {
        RemoveAsync(key).GetAwaiter().GetResult();
    }

    public async Task RemoveAsync(string key, CancellationToken token = new())
    {
        if (string.IsNullOrWhiteSpace(key))
            return;

        try
        {
            await memoryService.DeleteAllKeysAsync(GetScope(key), false, token);
        }
        catch (Exception)
        {
            // Ignore errors to match IDistributedCache behavior
        }
    }
}
