// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;

namespace CrossCloudKit.Memory.Basic;

/// <summary>
/// In-memory implementation of IMemoryService using concurrent collections.
/// </summary>
public sealed class MemoryServiceBasic(IPubSubService? pubSubService = null) : IMemoryService
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, PrimitiveType>> _scopedKeyValues = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, List<PrimitiveType>>> _scopedLists = new();
    private readonly ConcurrentDictionary<string, (DateTime ExpiryTime, CancellationTokenSource ExpiryCts)> _expirations = new();
    private readonly Lock _lockObject = new();

    private record LockIdAndExpiry(string LockId, CancellationTokenSource ExpiryCts);
    private readonly Dictionary<string, LockIdAndExpiry> _memoryMutexes = new();

    public bool IsInitialized => true;

    /// <inheritdoc />
    public Task<OperationResult<string?>> MemoryMutexLock(IMemoryServiceScope memoryScope, string mutexValue, TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<string?>.Failure("Service connection is not initialized"));

        if (string.IsNullOrEmpty(mutexValue))
            return Task.FromResult(OperationResult<string?>.Failure("Mutex value is empty"));

        if (timeToLive <= TimeSpan.Zero)
            return Task.FromResult(OperationResult<string?>.Failure("Time to live must be positive"));

        var lockKey = $"{memoryScope.Compile()}:{mutexValue}";

        // Generate a unique lock ID to identify the lock holder
        var lockId = Guid.NewGuid().ToString("N");

        lock (_memoryMutexes)
        {
            if (_memoryMutexes.ContainsKey(lockKey)) return Task.FromResult(OperationResult<string?>.Success(null));
            var cts = new CancellationTokenSource(timeToLive);
            cts.Token.Register(() => //ExpiryCts disposes CancellationTokenRegistration so no need to dispose that explicitly.
            {
                try
                {
                    lock (_memoryMutexes)
                    {
                        if (!_memoryMutexes.TryGetValue(lockKey, out var existingLock)
                            || existingLock.LockId != lockId) return;

                        _memoryMutexes.Remove(lockKey);
                        existingLock.ExpiryCts.Dispose();
                    }
                }
                catch (Exception)
                {
                    // ignored
                }
            });
            _memoryMutexes.Add(lockKey, new LockIdAndExpiry(lockId, cts));
            return Task.FromResult(OperationResult<string?>.Success(lockId));
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> MemoryMutexUnlock(IMemoryServiceScope memoryScope, string mutexValue, string lockId,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized"));

        if (string.IsNullOrEmpty(mutexValue))
            return Task.FromResult(OperationResult<bool>.Failure("Mutex value is empty"));

        if (string.IsNullOrEmpty(lockId))
            return Task.FromResult(OperationResult<bool>.Failure("Lock ID is required"));

        var lockKey = $"{memoryScope.Compile()}:{mutexValue}";

        lock (_memoryMutexes)
        {
            if (!_memoryMutexes.TryGetValue(lockKey, out var existingLock)
                || existingLock.LockId != lockId) return Task.FromResult(OperationResult<bool>.Success(false));
            _memoryMutexes.Remove(lockKey);
            try
            {
                existingLock.ExpiryCts.Dispose();
            }
            catch (Exception)
            {
                // ignored
            }
            return Task.FromResult(OperationResult<bool>.Success(true));
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> SetKeyExpireTimeAsync(
        IMemoryServiceScope memoryScope,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized"));

        var scope = memoryScope.Compile();
        var expiryTime = DateTime.UtcNow.Add(timeToLive);

        var cts = new CancellationTokenSource(timeToLive);
        cts.Token.Register(() =>
        {
            try
            {
                // Clean up expired scope
                _scopedKeyValues.TryRemove(scope, out _);
                _scopedLists.TryRemove(scope, out _);
                _expirations.TryRemove(scope, out var expiry);
                expiry.ExpiryCts?.Dispose();
            }
            catch (Exception)
            {
                // ignored
            }
        });

        _expirations.AddOrUpdate(scope, (expiryTime, cts), (_, _) => (expiryTime, cts));

        return Task.FromResult(OperationResult<bool>.Success(true));
    }

    /// <inheritdoc />
    public Task<OperationResult<TimeSpan?>> GetKeyExpireTimeAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<TimeSpan?>.Failure("Service is not initialized"));

        var scope = memoryScope.Compile();

        if (!_expirations.TryGetValue(scope, out var expiry))
            return Task.FromResult(OperationResult<TimeSpan?>.Success(null));

        var remainingTime = expiry.ExpiryTime - DateTime.UtcNow;
        return Task.FromResult(OperationResult<TimeSpan?>.Success(remainingTime > TimeSpan.Zero ? remainingTime : null));
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> SetKeyValuesAsync(
        IMemoryServiceScope memoryScope,
        IEnumerable<KeyValuePair<string, PrimitiveType>> keyValues,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service is not initialized");

        var keyValueArray = keyValues.ToArray();
        if (keyValueArray.Length == 0)
            return OperationResult<bool>.Failure("Key values are empty.");

        var scope = memoryScope.Compile();
        var scopeDict = _scopedKeyValues.GetOrAdd(scope, _ => new ConcurrentDictionary<string, PrimitiveType>());

        foreach (var (key, value) in keyValueArray)
        {
            scopeDict.AddOrUpdate(key, value, (_, _) => value);
        }

        if (publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "SetKeyValue",
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
        var result = await SetKeyValueConditionallyAndReturnValueRegardlessAsync(memoryScope, key, value, publishChange, cancellationToken);
        return result.IsSuccessful ? OperationResult<bool>.Success(result.Data.newlySet) : OperationResult<bool>.Failure(result.ErrorMessage!);
    }

    /// <inheritdoc />
    public async Task<OperationResult<(bool newlySet, PrimitiveType? value)>> SetKeyValueConditionallyAndReturnValueRegardlessAsync(
        IMemoryServiceScope memoryScope,
        string key,
        PrimitiveType value,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<(bool newlySet, PrimitiveType? value)>.Failure("Service is not initialized");

        var scope = memoryScope.Compile();
        var scopeDict = _scopedKeyValues.GetOrAdd(scope, _ => new ConcurrentDictionary<string, PrimitiveType>());

        var addedSuccessfully = scopeDict.TryAdd(key, value);
        var existingValue = addedSuccessfully ? value : scopeDict.GetValueOrDefault(key);

        if (addedSuccessfully && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "SetKeyValue",
                new Dictionary<string, PrimitiveType> { [key] = value }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<(bool newlySet, PrimitiveType? value)>.Success((addedSuccessfully, existingValue));
    }

    /// <inheritdoc />
    public Task<OperationResult<PrimitiveType?>> GetKeyValueAsync(
        IMemoryServiceScope memoryScope,
        string key,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<PrimitiveType?>.Failure("Service is not initialized"));

        var scope = memoryScope.Compile();

        if (!_scopedKeyValues.TryGetValue(scope, out var scopeDict))
            return Task.FromResult(OperationResult<PrimitiveType?>.Success(null));

        var value = scopeDict.GetValueOrDefault(key);
        return Task.FromResult(OperationResult<PrimitiveType?>.Success(value));
    }

    /// <inheritdoc />
    public Task<OperationResult<Dictionary<string, PrimitiveType>>> GetKeyValuesAsync(
        IMemoryServiceScope memoryScope,
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Failure("Service is not initialized."));

        var keyArray = keys.ToArray();
        if (keyArray.Length == 0)
            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Failure("Keys are empty."));

        var scope = memoryScope.Compile();
        var result = new Dictionary<string, PrimitiveType>();

        if (!_scopedKeyValues.TryGetValue(scope, out var scopeDict))
            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Success(result));

        foreach (var key in keyArray)
        {
            if (scopeDict.TryGetValue(key, out var value))
            {
                result[key] = value;
            }
        }

        return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Success(result));
    }

    /// <inheritdoc />
    public Task<OperationResult<Dictionary<string, PrimitiveType>>> GetAllKeyValuesAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Failure("Service is not initialized."));

        var scope = memoryScope.Compile();
        var result = new Dictionary<string, PrimitiveType>();

        if (!_scopedKeyValues.TryGetValue(scope, out var scopeDict))
            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Success(result));
        foreach (var kvp in scopeDict)
        {
            result[kvp.Key] = kvp.Value;
        }

        return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Success(result));
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteKeyAsync(
        IMemoryServiceScope memoryScope,
        string key,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service is not initialized.");

        var scope = memoryScope.Compile();
        var wasRemoved = false;

        if (_scopedKeyValues.TryGetValue(scope, out var scopeDict))
        {
            wasRemoved = scopeDict.TryRemove(key, out _);
        }

        if (wasRemoved && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "DeleteKey", key, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<bool>.Success(wasRemoved);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> DeleteAllKeysAsync(
        IMemoryServiceScope memoryScope,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service is not initialized.");

        var scope = memoryScope.Compile();
        var wasRemoved = _scopedKeyValues.TryRemove(scope, out _);
        _scopedLists.TryRemove(scope, out _);

        if (wasRemoved && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync<string>(pubSubService, memoryScope, "DeleteAllKeys", null!, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<bool>.Success(wasRemoved);
    }

    /// <inheritdoc />
    public Task<OperationResult<ReadOnlyCollection<string>>> GetKeysAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<ReadOnlyCollection<string>>.Failure("Service is not initialized."));

        var scope = memoryScope.Compile();
        var keys = new List<string>();

        if (_scopedKeyValues.TryGetValue(scope, out var scopeDict))
        {
            keys.AddRange(scopeDict.Keys);
        }

        return Task.FromResult(OperationResult<ReadOnlyCollection<string>>.Success(keys.AsReadOnly()));
    }

    /// <inheritdoc />
    public Task<OperationResult<long>> GetKeysCountAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<long>.Failure("Service is not initialized."));

        var scope = memoryScope.Compile();
        var count = 0L;

        if (_scopedKeyValues.TryGetValue(scope, out var scopeDict))
        {
            count = scopeDict.Count;
        }

        return Task.FromResult(OperationResult<long>.Success(count));
    }

    /// <inheritdoc />
    public async Task<OperationResult<Dictionary<string, long>>> IncrementKeyValuesAsync(
        IMemoryServiceScope memoryScope,
        IEnumerable<KeyValuePair<string, long>> keyIncrements,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<Dictionary<string, long>>.Failure("Service is not initialized.");

        var incrementArray = keyIncrements.ToArray();
        if (incrementArray.Length == 0)
            return OperationResult<Dictionary<string, long>>.Failure("Key increments array is empty.");

        var scope = memoryScope.Compile();
        var scopeDict = _scopedKeyValues.GetOrAdd(scope, _ => new ConcurrentDictionary<string, PrimitiveType>());
        var result = new Dictionary<string, long>();

        lock (_lockObject) // Ensure atomic increment operations
        {
            foreach (var (key, increment) in incrementArray)
            {
                var currentValue = scopeDict.TryGetValue(key, out var val) && val.Kind == PrimitiveTypeKind.Integer ? val.AsInteger : 0L;
                var newValue = currentValue + increment;
                scopeDict.AddOrUpdate(key, new PrimitiveType(newValue), (_, _) => new PrimitiveType(newValue));
                result[key] = newValue;
            }
        }

        if (result.Count > 0 && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "SetKeyValue",
                result.ToDictionary(kv => kv.Key, kv => new PrimitiveType(kv.Value)), cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<Dictionary<string, long>>.Success(result);
    }

    /// <inheritdoc />
    public async Task<OperationResult<long>> IncrementKeyByValueAndGetAsync(
        IMemoryServiceScope memoryScope,
        string key,
        long incrementBy,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<long>.Failure("Service is not initialized.");

        var scope = memoryScope.Compile();
        var scopeDict = _scopedKeyValues.GetOrAdd(scope, _ => new ConcurrentDictionary<string, PrimitiveType>());

        long newValue;
        lock (_lockObject) // Ensure atomic increment operation
        {
            var currentValue = scopeDict.TryGetValue(key, out var val) && val.Kind == PrimitiveTypeKind.Integer ? val.AsInteger : 0L;
            newValue = currentValue + incrementBy;
            scopeDict.AddOrUpdate(key, new PrimitiveType(newValue), (_, _) => new PrimitiveType(newValue));
        }

        if (publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "SetKeyValue",
                new Dictionary<string, PrimitiveType> { [key] = new(newValue) }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<long>.Success(newValue);
    }

    // List Operations

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PushToListTailAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfListExists = false,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service is not initialized.");

        var valueArray = values.ToArray();
        if (valueArray.Length == 0)
            return OperationResult<bool>.Success(true);

        var scope = memoryScope.Compile();
        var scopeLists = _scopedLists.GetOrAdd(scope, _ => new ConcurrentDictionary<string, List<PrimitiveType>>());

        lock (_lockObject)
        {
            if (onlyIfListExists && !scopeLists.ContainsKey(listName))
                return OperationResult<bool>.Success(false);

            var list = scopeLists.GetOrAdd(listName, _ => []);
            list.AddRange(valueArray);
        }

        if (publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "PushToListTail",
                new { ListName = listName, Values = valueArray }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> PushToListHeadAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfListExists = false,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service is not initialized.");

        var valueArray = values.ToArray();
        if (valueArray.Length == 0)
            return OperationResult<bool>.Success(true);

        var scope = memoryScope.Compile();
        var scopeLists = _scopedLists.GetOrAdd(scope, _ => new ConcurrentDictionary<string, List<PrimitiveType>>());

        lock (_lockObject)
        {
            if (onlyIfListExists && !scopeLists.ContainsKey(listName))
                return OperationResult<bool>.Success(false);

            var list = scopeLists.GetOrAdd(listName, _ => []);
            list.InsertRange(0, valueArray);
        }

        if (publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "PushToListHead",
                new { ListName = listName, Values = valueArray }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<bool>.Success(true);
    }

    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType[]>> PushToListTailIfValuesNotExistsAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<PrimitiveType[]>.Failure("Service is not initialized.");

        var valueArray = values.ToArray();
        if (valueArray.Length == 0)
            return OperationResult<PrimitiveType[]>.Success([]);

        var scope = memoryScope.Compile();
        var scopeLists = _scopedLists.GetOrAdd(scope, _ => new ConcurrentDictionary<string, List<PrimitiveType>>());
        var addedValues = new List<PrimitiveType>();

        lock (_lockObject)
        {
            var list = scopeLists.GetOrAdd(listName, _ => []);

            foreach (var value in valueArray)
            {
                if (list.Contains(value)) continue;
                list.Add(value);
                addedValues.Add(value);
            }
        }

        if (addedValues.Count > 0 && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "PushToListTail",
                new { ListName = listName, Values = addedValues }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<PrimitiveType[]>.Success(addedValues.ToArray());
    }

    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType?>> PopLastElementOfListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<PrimitiveType?>.Failure("Service is not initialized.");

        var scope = memoryScope.Compile();

        if (!_scopedLists.TryGetValue(scope, out var scopeLists) ||
            !scopeLists.TryGetValue(listName, out var list))
            return OperationResult<PrimitiveType?>.Success(null);

        PrimitiveType? poppedValue = null;
        lock (_lockObject)
        {
            if (list.Count > 0)
            {
                var lastIndex = list.Count - 1;
                poppedValue = list[lastIndex];
                list.RemoveAt(lastIndex);
            }
        }

        if (poppedValue != null && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "PopFromList",
                new { ListName = listName, Value = poppedValue }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<PrimitiveType?>.Success(poppedValue);
    }

    /// <inheritdoc />
    public async Task<OperationResult<PrimitiveType?>> PopFirstElementOfListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<PrimitiveType?>.Failure("Service is not initialized.");

        var scope = memoryScope.Compile();

        if (!_scopedLists.TryGetValue(scope, out var scopeLists) ||
            !scopeLists.TryGetValue(listName, out var list))
            return OperationResult<PrimitiveType?>.Success(null);

        PrimitiveType? poppedValue = null;
        lock (_lockObject)
        {
            if (list.Count > 0)
            {
                poppedValue = list[0];
                list.RemoveAt(0);
            }
        }

        if (poppedValue != null && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "PopFromList",
                new { ListName = listName, Value = poppedValue }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<PrimitiveType?>.Success(poppedValue);
    }

    /// <inheritdoc />
    public async Task<OperationResult<IEnumerable<PrimitiveType?>>> RemoveElementsFromListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<IEnumerable<PrimitiveType?>>.Failure("Service is not initialized.");

        var valueArray = values.ToArray();
        if (valueArray.Length == 0)
            return OperationResult<IEnumerable<PrimitiveType?>>.Success([]);

        var scope = memoryScope.Compile();

        if (!_scopedLists.TryGetValue(scope, out var scopeLists) ||
            !scopeLists.TryGetValue(listName, out var list))
            return OperationResult<IEnumerable<PrimitiveType?>>.Success([]);

        var removedValues = new List<PrimitiveType?>();
        lock (_lockObject)
        {
            removedValues.AddRange(valueArray.Where(value => list.Remove(value)));
        }

        if (removedValues.Count > 0 && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "RemoveFromList",
                new { ListName = listName, Values = removedValues }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<IEnumerable<PrimitiveType?>>.Success(removedValues);
    }

    /// <inheritdoc />
    public Task<OperationResult<ReadOnlyCollection<PrimitiveType>>> GetAllElementsOfListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<ReadOnlyCollection<PrimitiveType>>.Failure("Service is not initialized."));

        var scope = memoryScope.Compile();

        if (!_scopedLists.TryGetValue(scope, out var scopeLists) ||
            !scopeLists.TryGetValue(listName, out var list))
            return Task.FromResult(OperationResult<ReadOnlyCollection<PrimitiveType>>.Success(Array.Empty<PrimitiveType>().ToList().AsReadOnly()));

        List<PrimitiveType> snapshot;
        lock (_lockObject)
        {
            snapshot = new List<PrimitiveType>(list);
        }

        return Task.FromResult(OperationResult<ReadOnlyCollection<PrimitiveType>>.Success(snapshot.AsReadOnly()));
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EmptyListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service is not initialized.");

        var scope = memoryScope.Compile();

        if (!_scopedLists.TryGetValue(scope, out var scopeLists) ||
            !scopeLists.TryGetValue(listName, out var list))
            return OperationResult<bool>.Success(false);

        bool wasNotEmpty;
        lock (_lockObject)
        {
            wasNotEmpty = list.Count > 0;
            list.Clear();
        }

        if (wasNotEmpty && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "EmptyList",
                new { ListName = listName }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<bool>.Success(wasNotEmpty);
    }

    /// <inheritdoc />
    public async Task<OperationResult<bool>> EmptyListAndSublistsAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        string sublistPrefix,
        bool publishChange = true,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<bool>.Failure("Service is not initialized.");

        var scope = memoryScope.Compile();

        if (!_scopedLists.TryGetValue(scope, out var scopeLists))
            return OperationResult<bool>.Success(false);

        var clearedAny = false;
        var clearedLists = new List<string>();

        lock (_lockObject)
        {
            // Clear the main list
            if (scopeLists.TryGetValue(listName, out var mainList) && mainList.Count > 0)
            {
                mainList.Clear();
                clearedAny = true;
                clearedLists.Add(listName);
            }

            // Clear sublists with a prefix
            var sublistPattern = $"{listName}:{sublistPrefix}";
            var matchingKeys = scopeLists.Keys.Where(key => key.StartsWith(sublistPattern)).ToList();

            foreach (var key in matchingKeys)
            {
                if (!scopeLists.TryGetValue(key, out var sublist) || sublist.Count <= 0) continue;
                sublist.Clear();
                clearedAny = true;
                clearedLists.Add(key);
            }
        }

        if (clearedAny && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "EmptyListAndSublists",
                new { ListName = listName, SublistPrefix = sublistPrefix, ClearedLists = clearedLists }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<bool>.Success(clearedAny);
    }

    /// <inheritdoc />
    public Task<OperationResult<long>> GetListSizeAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<long>.Failure("Service is not initialized."));

        var scope = memoryScope.Compile();

        if (!_scopedLists.TryGetValue(scope, out var scopeLists) ||
            !scopeLists.TryGetValue(listName, out var list))
            return Task.FromResult(OperationResult<long>.Success(0L));

        long count;
        lock (_lockObject)
        {
            count = list.Count;
        }

        return Task.FromResult(OperationResult<long>.Success(count));
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> ListContainsAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        PrimitiveType value,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized."));

        var scope = memoryScope.Compile();

        if (!_scopedLists.TryGetValue(scope, out var scopeLists) ||
            !scopeLists.TryGetValue(listName, out var list))
            return Task.FromResult(OperationResult<bool>.Success(false));

        bool contains;
        lock (_lockObject)
        {
            contains = list.Contains(value);
        }

        return Task.FromResult(OperationResult<bool>.Success(contains));
    }

    public async ValueTask DisposeAsync()
    {
        // Dispose all expiry cancellation tokens
        foreach (var expiry in _expirations.Values)
        {
            try
            {
                expiry.ExpiryCts.Dispose();
            }
            catch (Exception)
            {
                // ignored
            }
        }

        // Dispose all mutex cancellation tokens
        lock (_memoryMutexes)
        {
            foreach (var mutex in _memoryMutexes.Values)
            {
                try
                {
                    mutex.ExpiryCts.Dispose();
                }
                catch (Exception)
                {
                    // ignored
                }
            }
            _memoryMutexes.Clear();
        }

        _expirations.Clear();
        _scopedKeyValues.Clear();
        _scopedLists.Clear();

        await Task.CompletedTask;
    }

    private static async Task PublishChangeNotificationAsync<T>(IPubSubService? pubSubService,
        IMemoryServiceScope memoryScope,
        string operation,
        T? changes,
        CancellationToken cancellationToken)
    {
        if (pubSubService is null)
        {
            OperationResult<bool>.Failure("Pub/Sub service is not configured.");
            return;
        }

        var scope = memoryScope.Compile();

        try
        {
            var notification = new
            {
                operation,
                changes
            };

            var message = JsonConvert.SerializeObject(notification);
            await pubSubService.PublishAsync(scope, message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            OperationResult<bool>.Failure($"Failed to publish change notification for operation {operation} on scope {memoryScope}: {ex.Message}, Trace: {ex.StackTrace}");
        }
    }
}
