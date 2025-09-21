// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Net;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Memory.Redis.Common;
using StackExchange.Redis;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Memory.Redis;

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
    public async Task<OperationResult<string?>> MemoryMutexLock(
        IMemoryScope memoryScope,
        string mutexValue,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<string?>.Failure("Redis connection is not initialized", HttpStatusCode.ServiceUnavailable);

        if (string.IsNullOrEmpty(mutexValue))
            return OperationResult<string?>.Failure("Mutex value is empty", HttpStatusCode.BadRequest);

        if (timeToLive <= TimeSpan.Zero)
            return OperationResult<string?>.Failure("Time to live must be positive", HttpStatusCode.BadRequest);

        // Use a prefixed key to avoid conflicts with regular memory operations
        var lockKey = $"CrossCloudKit.Memory.Redis.MemoryServiceRedis.Mutex:{mutexValue}";

        // Generate a unique lock ID to identify the lock holder
        var lockId = Environment.MachineName + ":" + Environment.ProcessId + ":" + Guid.NewGuid().ToString("N");

        return await ExecuteRedisOperationAsync(async database =>
        {
            // Use Lua script for atomic hash field set with expiry
            const string lockScript = """
                local current = redis.call('hget', KEYS[1], ARGV[1])
                if not current then
                    redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
                    redis.call('expire', KEYS[1], ARGV[3])
                    return ARGV[2]
                elseif current == ARGV[2] then
                    redis.call('expire', KEYS[1], ARGV[3])
                    return ARGV[2]
                else
                    return nil
                end
                """;

            var compiled = memoryScope.Compile();

            var result = await database.ScriptEvaluateAsync(
                lockScript,
                [compiled],
                [lockKey, lockId, (int)timeToLive.TotalSeconds]);

            // Return the lock ID if acquired, null if not acquired
            return result.IsNull ? null : ((string?)result).NotNull();
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> MemoryMutexUnlock(
        IMemoryScope memoryScope,
        string mutexValue,
        string lockId,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized", HttpStatusCode.ServiceUnavailable);

        if (string.IsNullOrEmpty(mutexValue))
            return OperationResult<bool>.Failure("Mutex value is empty", HttpStatusCode.BadRequest);

        if (string.IsNullOrEmpty(lockId))
            return OperationResult<bool>.Failure("Lock ID is required", HttpStatusCode.BadRequest);

        // Use the same prefixed key format as in lock
        var lockKey = $"CrossCloudKit.Memory.Redis.MemoryServiceRedis.Mutex:{mutexValue}";

        // Use Lua script to ensure atomic check-and-delete operation on hash field
        // This verifies the lock ID matches before deleting, ensuring only the lock holder can unlock
        const string unlockScript = """
            if redis.call("hget", KEYS[1], ARGV[1]) == ARGV[2] then
                redis.call("hdel", KEYS[1], ARGV[1])
                return 1
            else
                return 0
            end
            """;

        return await ExecuteRedisOperationAsync(async database =>
        {
            var result = await database.ScriptEvaluateAsync(
                unlockScript,
                [memoryScope.Compile()],
                [lockKey, lockId]);

            // Return true if the hash field was deleted (lock was released by the correct holder)
            // Return false if the field didn't exist or had a different value (not our lock)
            return (bool)result;
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyCollection<string>>> ScanMemoryScopesWithPattern(
        string pattern,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<IReadOnlyCollection<string>>.Failure("Redis connection is not initialized", HttpStatusCode.ServiceUnavailable);

        var result = await ExecuteRedisOperationAsync(async database =>
        {
            var server = RedisConnection.NotNull().GetServer(RedisConnection.NotNull().GetEndPoints().First());

            var scopes = new List<string>();

            await foreach (var key in server.KeysAsync(database: database.Database, pattern: pattern, pageSize: 1000).WithCancellation(cancellationToken))
            {
                var addKey = (string?)key;
                if (addKey == null) continue;
                if (addKey.Contains(ScopeListDelimiter)
                    && TrySplittingScopeAndListName(addKey, out var justScope, out _))
                {
                    addKey = justScope.NotNull();
                }

                var ttl = await database.KeyTimeToLiveAsync(key);
                if (ttl.HasValue)
                {
                    switch (ttl.Value.TotalMilliseconds)
                    {
                        case <= 0:
                            break;
                        default:
                            scopes.Add(addKey);
                            break;
                    }
                }
                else
                {
                    scopes.Add(addKey);
                }
            }

            return (IReadOnlyCollection<string>)scopes.AsReadOnly();
        }, cancellationToken);
        return result;
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetKeyExpireTimeAsync(
        IMemoryScope memoryScope,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized", HttpStatusCode.ServiceUnavailable);

        return await ExecuteRedisOperationAsync(async database =>
        {
            var scope = memoryScope.Compile();

            var exists = await database.KeyExpireAsync(scope, timeToLive);
            if (!exists)
            {
                // Create the key as a hash if it doesn't exist (since we use hash operations for key-value storage)
                if (!await database.HashSetAsync(scope, "CrossCloudKit.Memory.Redis.MemoryServiceRedis.SetKeyExpireTimeAsync", "ignore"))
                    return false;
                if (!await database.KeyExpireAsync(scope, timeToLive))
                    return false;
            }

            var listPattern = BuildListKey(scope, "*");

            var server = RedisConnection.NotNull().GetServer(RedisConnection.NotNull().GetEndPoints().First());

            await foreach (var key in server.KeysAsync(database: database.Database, pattern: listPattern, pageSize: 1000).WithCancellation(cancellationToken))
            {
                if (await database.KeyTypeAsync(key) != RedisType.List) continue;
                if (!await database.KeyExpireAsync(key, timeToLive))
                {
                    return false;
                }
            }
            return true;
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<TimeSpan?>> GetKeyExpireTimeAsync(
        IMemoryScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<TimeSpan?>.Failure("Redis connection is not initialized", HttpStatusCode.ServiceUnavailable);

        var scope = memoryScope.Compile();
        return await ExecuteRedisOperationAsync(async database => await database.KeyTimeToLiveAsync(scope), cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetKeyValuesAsync(
        IMemoryScope memoryScope,
        IEnumerable<KeyValuePair<string, PrimitiveType>> keyValues,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized", HttpStatusCode.ServiceUnavailable);

        var keyValueArray = keyValues.ToArray();
        if (keyValueArray.Length == 0)
            return OperationResult<bool>.Failure("Key values are empty.", HttpStatusCode.BadRequest);

        var hashEntries = keyValueArray
            .Select(kv => new HashEntry(kv.Key, ConvertPrimitiveTypeToRedisValue(kv.Value)))
            .ToArray();

        if (hashEntries.Length == 0)
            return OperationResult<bool>.Failure("Hash entries are empty.", HttpStatusCode.BadRequest);

        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database =>
        {
            await database.HashSetAsync(scope, hashEntries);
            return true;
        }, cancellationToken);

        if (result.IsSuccessful && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                keyValueArray.ToDictionary(kv => kv.Key, kv => kv.Value), cancellationToken);
        }

        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetKeyValueConditionallyAsync(
        IMemoryScope memoryScope,
        string key,
        PrimitiveType value,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        var result = await SetKeyValueConditionallyAndReturnValueRegardlessAsync(memoryScope, key, value, publishChange, cancellationToken);
        return result.IsSuccessful ? OperationResult<bool>.Success(result.Data.Item1) : OperationResult<bool>.Failure(result.ErrorMessage, result.StatusCode);
    }

    /// <inheritdoc />
    public async Task<OperationResult<(bool newlySet, PrimitiveType? value)>> SetKeyValueConditionallyAndReturnValueRegardlessAsync(
        IMemoryScope memoryScope,
        string key,
        PrimitiveType value,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<(bool newlySet, PrimitiveType? value)>.Failure("Redis connection is not initialized", HttpStatusCode.ServiceUnavailable);

        const string script = """
              local val = redis.call('hget', KEYS[1], ARGV[1])
              if not val then
                  redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
                  return {true, ARGV[2]}
              else
                  return {false, val}
              end
          """;

        var redisValue = ConvertPrimitiveTypeToRedisValue(value);

        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database =>
        {
            var scriptResult = await database.ScriptEvaluateAsync(script,
                [scope],
                [key, redisValue]);

            var res = ((RedisResult[]?)scriptResult).NotNull();
            return ((bool)res[0], ConvertRedisValueToPrimitiveType((RedisValue)res[1]));
        }, cancellationToken);

        if (result.IsSuccessful && publishChange && result.Data.Item1 && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                new Dictionary<string, PrimitiveType> { [key] = value }, cancellationToken);
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType?>> GetKeyValueAsync(
        IMemoryScope memoryScope,
        string key,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<PrimitiveType?>.Failure("Redis connection is not initialized", HttpStatusCode.ServiceUnavailable);

        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database =>
        {
            var value = await database.HashGetAsync(scope, key);
            return ConvertRedisValueToPrimitiveType(value);
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<Dictionary<string, PrimitiveType>>> GetKeyValuesAsync(
        IMemoryScope memoryScope,
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<Dictionary<string, PrimitiveType>>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);

        var keyArray = keys.ToArray();
        if (keyArray.Length == 0)
            return OperationResult<Dictionary<string, PrimitiveType>>.Failure("Keys are empty.", HttpStatusCode.BadRequest);

        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database =>
        {
            var values = await database.HashGetAsync(scope, keyArray.Select(k => (RedisValue)k).ToArray());

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
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<Dictionary<string, PrimitiveType>>> GetAllKeyValuesAsync(
        IMemoryScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<Dictionary<string, PrimitiveType>>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);

        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database =>
        {
            var hashEntries = await database.HashGetAllAsync(scope);

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
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteKeyAsync(
        IMemoryScope memoryScope,
        string key,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);

        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database => await database.HashDeleteAsync(scope, key), cancellationToken);

        if (result.IsSuccessful && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "DeleteKey", key, cancellationToken);
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteAllKeysAsync(
        IMemoryScope memoryScope,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);

        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database => await database.KeyDeleteAsync(scope), cancellationToken);

        if (result.IsSuccessful && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync<string>(_pubSubService, memoryScope, "DeleteAllKeys", null, cancellationToken);
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<OperationResult<ReadOnlyCollection<string>>> GetKeysAsync(
        IMemoryScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<ReadOnlyCollection<string>>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);

        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database =>
        {
            var keys = await database.HashKeysAsync(scope);
            return keys.Where(k => !k.IsNullOrEmpty)
                .Select(k => k.ToString())
                .ToList()
                .AsReadOnly();
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> GetKeysCountAsync(
        IMemoryScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<long>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);

        var scope = memoryScope.Compile();

        return await ExecuteRedisOperationAsync(async database => await database.HashLengthAsync(scope), cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<Dictionary<string, long>>> IncrementKeyValuesAsync(
        IMemoryScope memoryScope,
        IEnumerable<KeyValuePair<string, long>> keyIncrements,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<Dictionary<string, long>>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);

        var incrementArray = keyIncrements.ToArray();
        if (incrementArray.Length == 0)
            return OperationResult<Dictionary<string, long>>.Failure("Key increments array is empty.", HttpStatusCode.BadRequest);

        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database =>
        {
            var resultDict = new Dictionary<string, long>(incrementArray.Length);

            foreach (var (key, increment) in incrementArray)
            {
                var newValue = await database.HashIncrementAsync(scope, key, increment);
                resultDict[key] = newValue;
            }

            return resultDict;
        }, cancellationToken);

        if (result is { IsSuccessful: true, Data.Count: > 0 } && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                result.Data.ToDictionary(kv => kv.Key, kv => new PrimitiveType(kv.Value)), cancellationToken);
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> IncrementKeyByValueAndGetAsync(
        IMemoryScope memoryScope,
        string key,
        long incrementBy,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<long>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);

        var scope = memoryScope.Compile();

        var result = await ExecuteRedisOperationAsync(async database => await database.HashIncrementAsync(scope, key, incrementBy), cancellationToken);

        if (result.IsSuccessful && publishChange && _pubSubService is not null)
        {
            await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                new Dictionary<string, PrimitiveType> { [key] = new(result.Data) }, cancellationToken);
        }

        return result;
    }

    // List Operations

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PushToListTailAsync(
        IMemoryScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfListExists = false,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_PushToListAsync(memoryScope, listName, values, true, onlyIfListExists, publishChange, _pubSubService, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PushToListHeadAsync(
        IMemoryScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfListExists = false,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_PushToListAsync(memoryScope, listName, values, false, onlyIfListExists, publishChange, _pubSubService, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType[]>> PushToListTailIfValuesNotExistsAsync(
        IMemoryScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<PrimitiveType[]>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_PushToListTailIfValuesNotExistsAsync(memoryScope, listName, values, publishChange, _pubSubService, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType?>> PopLastElementOfListAsync(
        IMemoryScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<PrimitiveType?>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_PopFromListAsync(memoryScope, listName, true, publishChange, _pubSubService, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType?>> PopFirstElementOfListAsync(
        IMemoryScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<PrimitiveType?>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_PopFromListAsync(memoryScope, listName, false, publishChange, _pubSubService, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<IEnumerable<PrimitiveType?>>> RemoveElementsFromListAsync(
        IMemoryScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<IEnumerable<PrimitiveType?>>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_RemoveElementsFromListAsync(memoryScope, listName, values, publishChange, _pubSubService, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<ReadOnlyCollection<PrimitiveType>>> GetAllElementsOfListAsync(
        IMemoryScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<ReadOnlyCollection<PrimitiveType>>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_GetAllElementsOfListAsync(memoryScope, listName, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EmptyListAsync(
        IMemoryScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_EmptyListAsync(memoryScope, listName, publishChange, _pubSubService, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EmptyListAndSublistsAsync(
        IMemoryScope memoryScope,
        string listName,
        string sublistPrefix,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_EmptyListAndSublistsAsync(memoryScope, listName, sublistPrefix, publishChange, _pubSubService, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> GetListSizeAsync(
        IMemoryScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<long>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_GetListSizeAsync(memoryScope, listName, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> ListContainsAsync(
        IMemoryScope memoryScope,
        string listName,
        PrimitiveType value,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Redis connection is not initialized.", HttpStatusCode.ServiceUnavailable);
        return await Common_ListContainsAsync(memoryScope, listName, value, cancellationToken);
    }
}
