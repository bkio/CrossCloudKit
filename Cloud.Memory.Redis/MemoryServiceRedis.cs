// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using Cloud.Interfaces;
using Cloud.Memory.Redis.Common;
using StackExchange.Redis;
using Utilities.Common;

namespace Cloud.Memory.Redis;

/// <summary>
/// Modern Redis-based implementation of IMemoryService with async patterns and proper error handling.
/// </summary>
public sealed class MemoryServiceRedis : RedisCommonFunctionalities, IMemoryService
{
    private readonly IPubSubService? _pubSubService;

    /// <summary>
    /// Initializes a new instance of the MemoryServiceRedis class.
    /// </summary>
    /// <param name="connectionOptions">Redis connection configuration options.</param>
    /// <param name="pubSubService">Optional pub/sub service for change notifications.</param>
    public MemoryServiceRedis(RedisConnectionOptions connectionOptions, IPubSubService? pubSubService = null) : base(connectionOptions)
    {
        _pubSubService = pubSubService;
    }

    public bool IsInitialized => Initialized;

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetKeyExpireTimeAsync(
        IMemoryServiceScope memoryScope,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        return await ExecuteRedisOperationAsync(async database =>
        {
            var scope = memoryScope.Compile();

            var exists = await database.KeyExpireAsync(scope, timeToLive).ConfigureAwait(false);
            if (exists) return true;

            // Create the key if it doesn't exist
            await database.SetAddAsync(scope, "").ConfigureAwait(false);
            return await database.KeyExpireAsync(scope, timeToLive).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<TimeSpan?>> GetKeyExpireTimeAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        var scope = memoryScope.Compile();
        return await ExecuteRedisOperationAsync(async database => await database.KeyTimeToLiveAsync(scope).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetKeyValuesAsync(
        IMemoryServiceScope memoryScope,
        IEnumerable<KeyValuePair<string, PrimitiveType>> keyValues,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        var keyValueArray = keyValues.ToArray();
        if (keyValueArray.Length == 0)
            return OperationResult<bool>.Failure("Key values are empty.");

        var hashEntries = keyValueArray
            .Select(kv => new HashEntry(kv.Key, ConvertPrimitiveTypeToRedisValue(kv.Value)))
            .ToArray();

        if (hashEntries.Length == 0)
            return OperationResult<bool>.Failure("Hash entries are empty.");

        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database =>
        {
            await database.HashSetAsync(scope, hashEntries).ConfigureAwait(false);
            return true;
        }, cancellationToken).ConfigureAwait(false);

        if (result.IsSuccessful && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                keyValueArray.ToDictionary(kv => kv.Key, kv => kv.Value), cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetKeyValueConditionallyAsync(
        IMemoryServiceScope memoryScope,
        string key,
        PrimitiveType value,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        const string script = """
          if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then
              return redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
          else
              return nil
          end
          """;

        var redisValue = ConvertPrimitiveTypeToRedisValue(value);

        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database =>
        {
            var scriptResult = await database.ScriptEvaluateAsync(script,
                [scope],
                [key, redisValue]).ConfigureAwait(false);

            return !scriptResult.IsNull;
        }, cancellationToken).ConfigureAwait(false);

        if (result.IsSuccessful && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                new Dictionary<string, PrimitiveType> { [key] = value }, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType?>> GetKeyValueAsync(
        IMemoryServiceScope memoryScope,
        string key,
        CancellationToken cancellationToken = default)
    {
        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database =>
        {
            var value = await database.HashGetAsync(scope, key).ConfigureAwait(false);
            return ConvertRedisValueToPrimitiveType(value);
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<Dictionary<string, PrimitiveType>>> GetKeyValuesAsync(
        IMemoryServiceScope memoryScope,
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default)
    {
        var keyArray = keys.ToArray();
        if (keyArray.Length == 0)
            return OperationResult<Dictionary<string, PrimitiveType>>.Failure("Keys are empty.");

        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database =>
        {
            var values = await database.HashGetAsync(scope, keyArray.Select(k => (RedisValue)k).ToArray()).ConfigureAwait(false);

            var result = new Dictionary<string, PrimitiveType>(keyArray.Length);
            for (var i = 0; i < keyArray.Length; i++)
            {
                var primitiveValue = ConvertRedisValueToPrimitiveType(values[i]);
                if (primitiveValue is not null)
                {
                    result[keyArray[i]] = primitiveValue;
                }
            }

            return result;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<Dictionary<string, PrimitiveType>>> GetAllKeyValuesAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database =>
        {
            var hashEntries = await database.HashGetAllAsync(scope).ConfigureAwait(false);

            var result = new Dictionary<string, PrimitiveType>(hashEntries.Length);
            foreach (var entry in hashEntries)
            {
                var primitiveValue = ConvertRedisValueToPrimitiveType(entry.Value);
                if (primitiveValue is not null)
                {
                    result[entry.Name.ToString()] = primitiveValue;
                }
            }

            return result;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteKeyAsync(
        IMemoryServiceScope memoryScope,
        string key,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database => await database.HashDeleteAsync(scope, key).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);

        if (result.IsSuccessful && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "DeleteKey", key, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteAllKeysAsync(
        IMemoryServiceScope memoryScope,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database => await database.KeyDeleteAsync(scope).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);

        if (result.IsSuccessful && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync<string>(_pubSubService, memoryScope, "DeleteAllKeys", null, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<OperationResult<ReadOnlyCollection<string>>> GetKeysAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database =>
        {
            var keys = await database.HashKeysAsync(scope).ConfigureAwait(false);
            return keys.Where(k => !k.IsNullOrEmpty)
                .Select(k => k.ToString())
                .ToList()
                .AsReadOnly();
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> GetKeysCountAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database => await database.HashLengthAsync(scope).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<Dictionary<string, long>>> IncrementKeyValuesAsync(
        IMemoryServiceScope memoryScope,
        IEnumerable<KeyValuePair<string, long>> keyIncrements,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        var incrementArray = keyIncrements.ToArray();
        if (incrementArray.Length == 0)
            return OperationResult<Dictionary<string, long>>.Failure("Key increments array is empty.");

        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database =>
        {
            var resultDict = new Dictionary<string, long>(incrementArray.Length);

            foreach (var (key, increment) in incrementArray)
            {
                var newValue = await database.HashIncrementAsync(scope, key, increment).ConfigureAwait(false);
                resultDict[key] = newValue;
            }

            return resultDict;
        }, cancellationToken).ConfigureAwait(false);

        if (result is { IsSuccessful: true, Data.Count: > 0 } && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                result.Data.ToDictionary(kv => kv.Key, kv => new PrimitiveType(kv.Value)), cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> IncrementKeyByValueAndGetAsync(
        IMemoryServiceScope memoryScope,
        string key,
        long incrementBy,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database => await database.HashIncrementAsync(scope, key, incrementBy).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);

        if (result.IsSuccessful && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                new Dictionary<string, PrimitiveType> { [key] = new(result.Data) }, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    // List Operations

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PushToListTailAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfExists = false,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        return await Common_PushToListAsync(memoryScope, listName, values, true, onlyIfExists, publishChange, _pubSubService, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PushToListHeadAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfExists = false,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        return await Common_PushToListAsync(memoryScope, listName, values, false, onlyIfExists, publishChange, _pubSubService, cancellationToken).ConfigureAwait(false);
    }


    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType?>> PopLastElementOfListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        return await Common_PopFromListAsync(memoryScope, listName, true, publishChange, _pubSubService, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType?>> PopFirstElementOfListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        return await Common_PopFromListAsync(memoryScope, listName, false, publishChange, _pubSubService, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<IEnumerable<PrimitiveType?>>> RemoveElementsFromListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        return await Common_RemoveElementsFromListAsync(memoryScope, listName, values, publishChange, _pubSubService, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<ReadOnlyCollection<PrimitiveType>>> GetAllElementsOfListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        return await Common_GetAllElementsOfListAsync(memoryScope, listName, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EmptyListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        return await Common_EmptyListAsync(memoryScope, listName, publishChange, _pubSubService, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EmptyListAndSublistsAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        string sublistPrefix,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        return await Common_EmptyListAndSublistsAsync(memoryScope, listName, sublistPrefix, publishChange, _pubSubService, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> GetListSizeAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        return await Common_GetListSizeAsync(memoryScope, listName, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> ListContainsAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        PrimitiveType value,
        CancellationToken cancellationToken = default)
    {
        return await Common_ListContainsAsync(memoryScope, listName, value, cancellationToken).ConfigureAwait(false);
    }
}
