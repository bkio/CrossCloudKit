// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Text;
using System.Text.RegularExpressions;
using CrossCloudKit.Interfaces;
using CrossCloudKit.Utilities.Common;
using Newtonsoft.Json;
// ReSharper disable NullableWarningSuppressionIsUsed

namespace CrossCloudKit.Memory.Basic;

/// <summary>
/// Cross-process implementation of IMemoryService using memory-mapped files and OS-level synchronization primitives.
/// Enables memory sharing and mutex operations across multiple processes on the same machine.
/// </summary>
public sealed class MemoryServiceBasic : IMemoryService
{
    private readonly IPubSubService? _pubSubService;
    private readonly string _storageDirectory;
    private readonly ConcurrentDictionary<string, Timer> _expirationTimers = new();
    private readonly Timer _cleanupTimer;
    private bool _disposed;

    private const string RootFolderName = "CrossCloudKit.Memory.Basic";

    public MemoryServiceBasic(IPubSubService? pubSubService = null)
    {
        _pubSubService = pubSubService;
        _storageDirectory = Path.Combine(Path.GetTempPath(), RootFolderName);
        Directory.CreateDirectory(_storageDirectory);

        // Start background cleanup every 5 minutes
        _cleanupTimer = new Timer(CleanupExpiredFiles, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

    public bool IsInitialized => !_disposed;

    /// <inheritdoc />
    public Task<OperationResult<string?>> MemoryMutexLock(
        IMemoryServiceScope memoryScope,
        string mutexValue,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<string?>.Failure("Service connection is not initialized"));

        if (string.IsNullOrEmpty(mutexValue))
            return Task.FromResult(OperationResult<string?>.Failure("Mutex value is empty"));

        if (timeToLive <= TimeSpan.Zero)
            return Task.FromResult(OperationResult<string?>.Failure("Time to live must be positive"));

        // Use a prefixed key similar to Redis implementation
        var lockKey = $"CrossCloudKit.Memory.Basic.MemoryServiceBasic.Mutex:{mutexValue}";

        // Generate a unique lock ID to identify the lock holder
        var lockId = Environment.MachineName + ":" + Environment.ProcessId + ":" + Guid.NewGuid().ToString("N");

        try
        {
            var scopeKey = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scopeKey);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<string?>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var mutexData = GetOrCreateMutexData(scopeKey);

            // Check if mutex already exists and is not expired
            if (mutexData.TryGetValue(lockKey, out var existingMutex))
            {
                var isExistingMutexSameOwner = existingMutex.LockId == lockId;
                if (existingMutex.ExpiryTime <= DateTime.UtcNow
                    || isExistingMutexSameOwner)
                {
                    // Remove the expired lock
                    mutexData.Remove(lockKey);

                    // Cancel expiration timer
                    if (_expirationTimers.TryRemove(lockKey, out var expiredTimer))
                        expiredTimer.Dispose();
                }
                else if (!isExistingMutexSameOwner)
                {
                    return Task.FromResult(OperationResult<string?>.Success(null)); // Lock already taken
                }
            }

            // Acquire the lock
            var expiryTime = DateTime.UtcNow.Add(timeToLive);
            mutexData[lockKey] = new MutexLockData { LockId = lockId, ExpiryTime = expiryTime };

            // Set up the expiration timer
            var timer = new Timer(_ =>
            {
                try
                {
                    var timerMutexWrapper = CreateMutex(scopeKey);
                    if (!timerMutexWrapper.IsSuccessful)
                        return;

                    using var timerMutex = timerMutexWrapper.Data!;

                    var currentData = GetOrCreateMutexData(scopeKey);
                    if (currentData.TryGetValue(lockKey, out var value) && value.LockId == lockId)
                    {
                        currentData.Remove(lockKey);
                        SaveMutexData(scopeKey, currentData);
                    }

                    _expirationTimers.TryRemove(lockKey, out var _);
                }
                catch (Exception)
                {
                    // ignored
                }
            }, null, timeToLive, Timeout.InfiniteTimeSpan);

            _expirationTimers[lockKey] = timer;
            SaveMutexData(scopeKey, mutexData);

            return Task.FromResult(OperationResult<string?>.Success(lockId));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<string?>.Failure($"Failed to acquire mutex lock: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> MemoryMutexUnlock(
        IMemoryServiceScope memoryScope,
        string mutexValue,
        string lockId,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized"));

        if (string.IsNullOrEmpty(mutexValue))
            return Task.FromResult(OperationResult<bool>.Failure("Mutex value is empty"));

        if (string.IsNullOrEmpty(lockId))
            return Task.FromResult(OperationResult<bool>.Failure("Lock ID is required"));

        // Use the same prefixed key format as in lock
        var lockKey = $"CrossCloudKit.Memory.Basic.MemoryServiceBasic.Mutex:{mutexValue}";

        try
        {
            var scopeKey = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scopeKey);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var mutexData = GetOrCreateMutexData(scopeKey);

            // Check if the lock exists and matches the provided lock ID
            if (!mutexData.TryGetValue(lockKey, out var existingLock))
                return Task.FromResult(OperationResult<bool>.Success(false));

            if (existingLock.LockId != lockId)
                return Task.FromResult(OperationResult<bool>.Success(false));

            // Remove the lock
            mutexData.Remove(lockKey);
            SaveMutexData(scopeKey, mutexData);

            // Cancel expiration timer
            if (!_expirationTimers.TryRemove(lockKey, out var timer))
                return Task.FromResult(OperationResult<bool>.Success(true));

            timer.Dispose();

            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure($"Failed to release mutex lock: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public async Task<OperationResult<IReadOnlyCollection<string>>> ScanMemoryScopesWithPattern(
        string pattern,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return OperationResult<IReadOnlyCollection<string>>.Failure("Service is not initialized");

        try
        {
            var matchingScopes = new List<string>();

            if (!Directory.Exists(_storageDirectory))
                return OperationResult<IReadOnlyCollection<string>>.Success(matchingScopes.AsReadOnly());

            // Get all JSON files, excluding mutex files
            var files = Directory.GetFiles(_storageDirectory, "*.json")
                .Where(f => !Path.GetFileName(f).Contains("_mutex"))
                .ToArray();

            foreach (var file in files)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    // Extract scope name from filename
                    var fileName = Path.GetFileNameWithoutExtension(file);

                    // Decode the Base64-encoded filename back to the original scope
                    var scopeName = EncodingUtilities.Base64DecodeNoPadding(fileName);

                    // Check if the scope matches the pattern
                    if (MatchesPattern(scopeName, pattern))
                    {
                        // Verify the file contains valid data and hasn't expired
                        var json = await File.ReadAllTextAsync(file, Encoding.UTF8, cancellationToken);
                        var data = JsonConvert.DeserializeObject<StoredData>(json);

                        // Skip expired data
                        if (data?.ExpiryTime.HasValue == true && data.ExpiryTime <= DateTime.UtcNow)
                            continue;

                        matchingScopes.Add(scopeName);
                    }
                }
                catch (Exception)
                {
                    // Ignore individual file processing errors (corrupted files, etc.)
                }
            }

            return OperationResult<IReadOnlyCollection<string>>.Success(matchingScopes.AsReadOnly());
        }
        catch (Exception ex)
        {
            return OperationResult<IReadOnlyCollection<string>>.Failure($"Failed to scan memory scopes: {ex.Message}");
        }
    }

    private static bool MatchesPattern(string scopeName, string pattern)
    {
        // Handle simple wildcard patterns like Redis does
        if (pattern == "*")
            return true;

        if (!pattern.Contains('*'))
            return scopeName.Equals(pattern, StringComparison.Ordinal);

        // Convert simple wildcards to regex pattern
        var regexPattern = "^" + Regex.Escape(pattern).Replace("\\*", ".*") + "$";
        return Regex.IsMatch(scopeName, regexPattern, RegexOptions.Compiled);
    }

    /// <inheritdoc />
    public Task<OperationResult<bool>> SetKeyExpireTimeAsync(
        IMemoryServiceScope memoryScope,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<bool>.Failure("Service is not initialized"));

        try
        {
            var scopeKey = memoryScope.Compile();
            var expiryTime = DateTime.UtcNow.Add(timeToLive);

            var mutexWrapper = CreateMutex(scopeKey);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scopeKey);
            var newData = data with { ExpiryTime = expiryTime };
            SaveStoredData(scopeKey, newData);

            // Set up expiration timer
            var timer = new Timer(_ =>
            {
                try
                {
                    var filePath = GetScopeFilePath(scopeKey);
                    if (File.Exists(filePath))
                        File.Delete(filePath);

                    var mutexFilePath = GetScopeFilePath(scopeKey, "_mutex");
                    if (File.Exists(mutexFilePath))
                        File.Delete(mutexFilePath);
                }
                catch (Exception)
                {
                    // ignored
                }
            }, null, timeToLive, Timeout.InfiniteTimeSpan);

            _expirationTimers[scopeKey] = timer;

            return Task.FromResult(OperationResult<bool>.Success(true));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure($"Failed to set expiration: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<TimeSpan?>> GetKeyExpireTimeAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<TimeSpan?>.Failure("Service is not initialized"));

        try
        {
            var scope = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<TimeSpan?>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (!data.ExpiryTime.HasValue)
                return Task.FromResult(OperationResult<TimeSpan?>.Success(null));

            var remainingTime = data.ExpiryTime.Value - DateTime.UtcNow;
            return Task.FromResult(OperationResult<TimeSpan?>.Success(remainingTime > TimeSpan.Zero ? remainingTime : null));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<TimeSpan?>.Failure($"Failed to get expiration time: {ex.Message}"));
        }
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

        try
        {
            var scope = memoryScope.Compile();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            foreach (var (key, value) in keyValueArray)
            {
                data.KeyValues[key] = value;
            }

            SaveStoredData(scope, data);

            if (publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                    keyValueArray.ToDictionary(kv => kv.Key, kv => kv.Value), cancellationToken);
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Failed to set key values: {ex.Message}");
        }
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
        // ReSharper disable once NullableWarningSuppressionIsUsed
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

        try
        {
            var scope = memoryScope.Compile();
            bool addedSuccessfully;
            PrimitiveType? existingValue;

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<(bool newlySet, PrimitiveType? value)>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (data.KeyValues.TryGetValue(key, out var keyValue))
            {
                addedSuccessfully = false;
                existingValue = keyValue;
            }
            else
            {
                addedSuccessfully = true;
                data.KeyValues[key] = value;
                existingValue = value;
                SaveStoredData(scope, data);
            }

            if (addedSuccessfully && publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                    new Dictionary<string, PrimitiveType> { [key] = value }, cancellationToken);
            }

            return OperationResult<(bool newlySet, PrimitiveType? value)>.Success((addedSuccessfully, existingValue));
        }
        catch (Exception ex)
        {
            return OperationResult<(bool newlySet, PrimitiveType? value)>.Failure($"Failed to set key value conditionally: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<PrimitiveType?>> GetKeyValueAsync(
        IMemoryServiceScope memoryScope,
        string key,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<PrimitiveType?>.Failure("Service is not initialized"));

        try
        {
            var scope = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<PrimitiveType?>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);
            var value = data.KeyValues.GetValueOrDefault(key);
            return Task.FromResult(OperationResult<PrimitiveType?>.Success(value));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<PrimitiveType?>.Failure($"Failed to get key value: {ex.Message}"));
        }
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

        try
        {
            var scope = memoryScope.Compile();
            var result = new Dictionary<string, PrimitiveType>();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            foreach (var key in keyArray)
            {
                if (data.KeyValues.TryGetValue(key, out var value))
                {
                    result[key] = value;
                }
            }

            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Success(result));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Failure($"Failed to get key values: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<Dictionary<string, PrimitiveType>>> GetAllKeyValuesAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Failure("Service is not initialized."));

        try
        {
            var scope = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);
            var result = new Dictionary<string, PrimitiveType>(data.KeyValues);
            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Success(result));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<Dictionary<string, PrimitiveType>>.Failure($"Failed to get all key values: {ex.Message}"));
        }
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

        try
        {
            var scope = memoryScope.Compile();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);
            var wasRemoved = data.KeyValues.Remove(key);

            if (wasRemoved)
                SaveStoredData(scope, data);

            if (wasRemoved && publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "DeleteKey", key, cancellationToken);
            }

            return OperationResult<bool>.Success(wasRemoved);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Failed to delete key: {ex.Message}");
        }
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

        var mutexWrapper = CreateMutex(scope);
        if (!mutexWrapper.IsSuccessful)
            return OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
        using var mutex = mutexWrapper.Data!;

        try
        {
            var filePath = GetScopeFilePath(scope);
            var mutexFilePath = GetScopeFilePath(scope, "_mutex");

            var wasRemoved = File.Exists(filePath);

            if (File.Exists(filePath))
                File.Delete(filePath);
            if (File.Exists(mutexFilePath))
                File.Delete(mutexFilePath);

            if (wasRemoved && publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync<string>(_pubSubService, memoryScope, "DeleteAllKeys", null, cancellationToken);
            }

            return OperationResult<bool>.Success(wasRemoved);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Failed to delete all keys: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<ReadOnlyCollection<string>>> GetKeysAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<ReadOnlyCollection<string>>.Failure("Service is not initialized."));

        try
        {
            var scope = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<ReadOnlyCollection<string>>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);
            var keys = data.KeyValues.Keys.ToList();
            return Task.FromResult(OperationResult<ReadOnlyCollection<string>>.Success(keys.AsReadOnly()));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<ReadOnlyCollection<string>>.Failure($"Failed to get keys: {ex.Message}"));
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<long>> GetKeysCountAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<long>.Failure("Service is not initialized."));

        try
        {
            var scope = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<long>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);
            var count = (long)data.KeyValues.Count;
            return Task.FromResult(OperationResult<long>.Success(count));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<long>.Failure($"Failed to get keys count: {ex.Message}"));
        }
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

        try
        {
            var scope = memoryScope.Compile();
            var result = new Dictionary<string, long>();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<Dictionary<string, long>>.Failure(
                    $"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            foreach (var (key, increment) in incrementArray)
            {
                var currentValue = data.KeyValues.TryGetValue(key, out var val) && val.Kind == PrimitiveTypeKind.Integer ? val.AsInteger : 0L;
                var newValue = currentValue + increment;
                data.KeyValues[key] = new PrimitiveType(newValue);
                result[key] = newValue;
            }

            SaveStoredData(scope, data);

            if (result.Count > 0 && publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                    result.ToDictionary(kv => kv.Key, kv => new PrimitiveType(kv.Value)), cancellationToken);
            }

            return OperationResult<Dictionary<string, long>>.Success(result);
        }
        catch (Exception ex)
        {
            return OperationResult<Dictionary<string, long>>.Failure($"Failed to increment key values: {ex.Message}");
        }
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

        try
        {
            var scope = memoryScope.Compile();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<long>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);
            var currentValue = data.KeyValues.TryGetValue(key, out var val) && val.Kind == PrimitiveTypeKind.Integer ? val.AsInteger : 0L;
            var newValue = currentValue + incrementBy;
            data.KeyValues[key] = new PrimitiveType(newValue);
            SaveStoredData(scope, data);

            if (publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "SetKeyValue",
                    new Dictionary<string, PrimitiveType> { [key] = new(newValue) }, cancellationToken);
            }

            return OperationResult<long>.Success(newValue);
        }
        catch (Exception ex)
        {
            return OperationResult<long>.Failure($"Failed to increment key value: {ex.Message}");
        }
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

        try
        {
            var scope = memoryScope.Compile();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (onlyIfListExists && !data.Lists.ContainsKey(listName))
                return OperationResult<bool>.Success(false);

            if (!data.Lists.ContainsKey(listName))
                data.Lists[listName] = new List<PrimitiveType>();

            data.Lists[listName].AddRange(valueArray);
            SaveStoredData(scope, data);

            if (publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "PushToListTail",
                    new { ListName = listName, Values = valueArray }, cancellationToken);
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Failed to push to list tail: {ex.Message}");
        }
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

        try
        {
            var scope = memoryScope.Compile();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<bool>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (onlyIfListExists && !data.Lists.ContainsKey(listName))
                return OperationResult<bool>.Success(false);

            if (!data.Lists.ContainsKey(listName))
                data.Lists[listName] = [];

            data.Lists[listName].InsertRange(0, valueArray);
            SaveStoredData(scope, data);

            if (publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "PushToListHead",
                    new { ListName = listName, Values = valueArray }, cancellationToken);
            }

            return OperationResult<bool>.Success(true);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Failed to push to list head: {ex.Message}");
        }
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
            return OperationResult<PrimitiveType[]>.Failure("Value array is empty.");

        try
        {
            var scope = memoryScope.Compile();
            var addedValues = new List<PrimitiveType>();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<PrimitiveType[]>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (!data.Lists.ContainsKey(listName))
                data.Lists[listName] = [];

            var list = data.Lists[listName];
            foreach (var value in valueArray)
            {
                if (list.Contains(value)) continue;
                list.Add(value);
                addedValues.Add(value);
            }

            if (addedValues.Count > 0)
                SaveStoredData(scope, data);

            if (addedValues.Count > 0 && publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "PushToListTailIfNotExists",
                    new { ListName = listName, Values = addedValues }, cancellationToken);
            }

            return OperationResult<PrimitiveType[]>.Success(addedValues.ToArray());
        }
        catch (Exception ex)
        {
            return OperationResult<PrimitiveType[]>.Failure($"Failed to push to list tail if values not exist: {ex.Message}");
        }
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

        try
        {
            var scope = memoryScope.Compile();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<PrimitiveType?>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (data.Lists.TryGetValue(listName, out var list) && list.Count > 0)
            {
                var lastIndex = list.Count - 1;
                var poppedValue = list[lastIndex];
                list.RemoveAt(lastIndex);
                SaveStoredData(scope, data);

                if (publishChange && _pubSubService is not null)
                {
                    await PublishChangeNotificationAsync(_pubSubService, memoryScope, "PopLastElementOfList",
                        new { ListName = listName, Value = poppedValue }, cancellationToken);
                }

                return OperationResult<PrimitiveType?>.Success(poppedValue);
            }

            return OperationResult<PrimitiveType?>.Failure("List is empty or does not exist.");
        }
        catch (Exception ex)
        {
            return OperationResult<PrimitiveType?>.Failure($"Failed to pop last element from list: {ex.Message}");
        }
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

        try
        {
            var scope = memoryScope.Compile();
            PrimitiveType? poppedValue = null;

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<PrimitiveType?>.Failure($"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (data.Lists.TryGetValue(listName, out var list) && list.Count > 0)
            {
                poppedValue = list[0];
                list.RemoveAt(0);
                SaveStoredData(scope, data);
            }

            if (poppedValue != null && publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "PopFirstElementOfList",
                    new { ListName = listName, Value = poppedValue }, cancellationToken);
            }

            return OperationResult<PrimitiveType?>.Success(poppedValue);
        }
        catch (Exception ex)
        {
            return OperationResult<PrimitiveType?>.Failure($"Failed to pop first element from list: {ex.Message}");
        }
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

        try
        {
            var scope = memoryScope.Compile();
            var removedValues = new List<PrimitiveType?>();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<IEnumerable<PrimitiveType?>>.Failure(
                    $"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (data.Lists.TryGetValue(listName, out var list))
            {
                removedValues.AddRange(valueArray.Where(value => list.Remove(value)));

                if (removedValues.Count > 0)
                    SaveStoredData(scope, data);
            }

            if (removedValues.Count > 0 && publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "RemoveElementsFromList",
                    new { ListName = listName, Values = removedValues }, cancellationToken);
            }

            return OperationResult<IEnumerable<PrimitiveType?>>.Success(removedValues);
        }
        catch (Exception ex)
        {
            return OperationResult<IEnumerable<PrimitiveType?>>.Failure($"Failed to remove elements from list: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<ReadOnlyCollection<PrimitiveType>>> GetAllElementsOfListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<ReadOnlyCollection<PrimitiveType>>.Failure("Service is not initialized."));

        try
        {
            var scope = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<ReadOnlyCollection<PrimitiveType>>.Failure(
                    $"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (!data.Lists.TryGetValue(listName, out var list))
                return Task.FromResult(
                    OperationResult<ReadOnlyCollection<PrimitiveType>>.Success(Array.Empty<PrimitiveType>().ToList()
                        .AsReadOnly()));

            var snapshot = new List<PrimitiveType>(list);
            return Task.FromResult(OperationResult<ReadOnlyCollection<PrimitiveType>>.Success(snapshot.AsReadOnly()));

        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<ReadOnlyCollection<PrimitiveType>>.Failure($"Failed to get all elements of list: {ex.Message}"));
        }
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

        try
        {
            var scope = memoryScope.Compile();
            var wasNotEmpty = false;

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<bool>.Failure(
                    $"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            if (data.Lists.TryGetValue(listName, out var list) && list.Count > 0)
            {
                list.Clear();
                wasNotEmpty = true;
                SaveStoredData(scope, data);
            }

            if (wasNotEmpty && publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "EmptyList",
                    new { ListName = listName }, cancellationToken);
            }

            return OperationResult<bool>.Success(wasNotEmpty);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Failed to empty list: {ex.Message}");
        }
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

        try
        {
            var scope = memoryScope.Compile();
            var clearedAny = false;
            var clearedLists = new List<string>();

            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return OperationResult<bool>.Failure(
                    $"Failed to create mutex: {mutexWrapper.ErrorMessage}");
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            // Clear the main list
            if (data.Lists.TryGetValue(listName, out var mainList) && mainList.Count > 0)
            {
                mainList.Clear();
                clearedAny = true;
                clearedLists.Add(listName);
            }

            // Clear sublists with a prefix
            var sublistPattern = $"{listName}:{sublistPrefix}";
            var matchingKeys = data.Lists.Keys.Where(key => key.StartsWith(sublistPattern)).ToList();

            foreach (var key in matchingKeys)
            {
                if (!data.Lists.TryGetValue(key, out var sublist) || sublist.Count <= 0) continue;
                sublist.Clear();
                clearedAny = true;
                clearedLists.Add(key);
            }

            if (clearedAny)
                SaveStoredData(scope, data);

            if (clearedAny && publishChange && _pubSubService is not null)
            {
                await PublishChangeNotificationAsync(_pubSubService, memoryScope, "EmptyListAndSublists",
                    new { ListName = listName, SublistPrefix = sublistPrefix, ClearedLists = clearedLists }, cancellationToken);
            }

            return OperationResult<bool>.Success(clearedAny);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Failed to empty list and sublists: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public Task<OperationResult<long>> GetListSizeAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        if (!IsInitialized)
            return Task.FromResult(OperationResult<long>.Failure("Service is not initialized."));

        try
        {
            var scope = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<long>.Failure(
                    $"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            return Task.FromResult(data.Lists.TryGetValue(listName, out var list)
                ? OperationResult<long>.Success(list.Count)
                : OperationResult<long>.Success(0L));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<long>.Failure($"Failed to get list size: {ex.Message}"));
        }
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

        try
        {
            var scope = memoryScope.Compile();
            var mutexWrapper = CreateMutex(scope);
            if (!mutexWrapper.IsSuccessful)
                return Task.FromResult(OperationResult<bool>.Failure(
                    $"Failed to create mutex: {mutexWrapper.ErrorMessage}"));
            using var mutex = mutexWrapper.Data!;

            var data = GetOrCreateStoredData(scope);

            return Task.FromResult(data.Lists.TryGetValue(listName, out var list)
                ? OperationResult<bool>.Success(list.Contains(value))
                : OperationResult<bool>.Success(false));
        }
        catch (Exception ex)
        {
            return Task.FromResult(OperationResult<bool>.Failure($"Failed to check if list contains value: {ex.Message}"));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            // Dispose cleanup timer
            try
            {
                await _cleanupTimer.DisposeAsync();
            }
            catch (Exception)
            {
                // ignored
            }

            // Dispose all expiration timers
            foreach (var timer in _expirationTimers.Values)
            {
                try
                {
                    await timer.DisposeAsync();
                }
                catch (Exception)
                {
                    // ignored
                }
            }
            _expirationTimers.Clear();
        }
        catch (Exception)
        {
            // Ignore disposal errors
        }

        await Task.CompletedTask;
    }

    private static OperationResult<AutoMutex> CreateMutex(string scope)
    {
        // Create a safe mutex name from the scope
        var mutexName = "CrossCloudKit.Memory.Basic." + Convert.ToBase64String(Encoding.UTF8.GetBytes(scope))
            .Replace('/', '_').Replace('+', '-').Replace("=", "");

        try
        {
            return OperationResult<AutoMutex>.Success(new AutoMutex(mutexName));
        }
        catch (Exception e)
        {
            return OperationResult<AutoMutex>.Failure($"Failed to create mutex: {e.Message}");
        }
    }

    private void CleanupExpiredFiles(object? state)
    {
        if (_disposed)
            return;

        try
        {
            if (!Directory.Exists(_storageDirectory))
                return;

            var files = Directory.GetFiles(_storageDirectory, "*.json");
            var now = DateTime.UtcNow;

            foreach (var file in files)
            {
                try
                {
                    // Skip mutex files, they will be cleaned up with their main files
                    if (Path.GetFileName(file).Contains("_mutex"))
                        continue;

                    var json = File.ReadAllText(file, Encoding.UTF8);
                    var data = JsonConvert.DeserializeObject<StoredData>(json);

                    if (data?.ExpiryTime.HasValue == true && data.ExpiryTime <= now)
                    {
                        // Delete the main file
                        File.Delete(file);

                        // Delete associated mutex file if it exists
                        var mutexFile = file.Replace(".json", "_mutex.json");
                        if (File.Exists(mutexFile))
                            File.Delete(mutexFile);
                    }
                }
                catch (Exception)
                {
                    // Ignore errors for individual files
                    // They might be in use by other processes
                }
            }
        }
        catch (Exception)
        {
            // Ignore cleanup errors
        }
    }

    private record MutexLockData
    {
        public string LockId { get; init; } = string.Empty;
        public DateTime ExpiryTime { get; init; }
    }

    private record StoredData
    {
        public Dictionary<string, PrimitiveType> KeyValues { get; init; } = new();
        public Dictionary<string, List<PrimitiveType>> Lists { get; init; } = new();
        public DateTime? ExpiryTime { get; init; }
    }

    private string GetScopeFilePath(string scope, string suffix = "")
    {
        var fileName = EncodingUtilities.Base64EncodeNoPadding(scope + suffix);
        return Path.Combine(_storageDirectory, $"{fileName}.json");
    }

    private StoredData GetOrCreateStoredData(string scope)
    {
        var filePath = GetScopeFilePath(scope);

        if (!File.Exists(filePath))
            return new StoredData();

        try
        {
            var json = File.ReadAllText(filePath, Encoding.UTF8);
            var data = JsonConvert.DeserializeObject<StoredData>(json);

            // Check if data has expired
            if (data?.ExpiryTime.HasValue != true || !(data.ExpiryTime <= DateTime.UtcNow))
                return data ?? new StoredData();
            File.Delete(filePath);
            return new StoredData();

        }
        catch (Exception)
        {
            return new StoredData();
        }
    }

    private void SaveStoredData(string scope, StoredData data)
    {
        var filePath = GetScopeFilePath(scope);
        if ((data.ExpiryTime.HasValue && data.ExpiryTime <= DateTime.UtcNow)
            || (!data.ExpiryTime.HasValue && data.KeyValues.Count == 0
                                          && (data.Lists.Count == 0 || data.Lists.Values.All(d => d.Count == 0))))
        {
            File.Delete(filePath);
            return;
        }
        var json = JsonConvert.SerializeObject(data, Formatting.None);
        FileSystemUtilities.WriteToFileEnsureWrittenToDisk(json, filePath);
    }

    private Dictionary<string, MutexLockData> GetOrCreateMutexData(string scope)
    {
        var filePath = GetScopeFilePath(scope, "_mutex");

        if (!File.Exists(filePath))
            return new Dictionary<string, MutexLockData>();

        try
        {
            var json = File.ReadAllText(filePath, Encoding.UTF8);
            var data = JsonConvert.DeserializeObject<Dictionary<string, MutexLockData>>(json);

            // Remove expired locks
            if (data == null) return data ?? new Dictionary<string, MutexLockData>();
            var expiredKeys = data.Where(kvp => kvp.Value.ExpiryTime <= DateTime.UtcNow)
                .Select(kvp => kvp.Key).ToList();

            foreach (var expiredKey in expiredKeys)
            {
                data.Remove(expiredKey);
            }

            return data;
        }
        catch (Exception)
        {
            return new Dictionary<string, MutexLockData>();
        }
    }

    private void SaveMutexData(string scope, Dictionary<string, MutexLockData> data)
    {
        var filePath = GetScopeFilePath(scope, "_mutex");
        if (data.Count == 0)
        {
            File.Delete(filePath);
            return;
        }
        var json = JsonConvert.SerializeObject(data, Formatting.None);
        FileSystemUtilities.WriteToFileEnsureWrittenToDisk(json, filePath);
    }

    private static async Task PublishChangeNotificationAsync<T>(IPubSubService? pubSubService,
        IMemoryServiceScope memoryScope,
        string operation,
        T? changes,
        CancellationToken cancellationToken)
    {
        if (pubSubService is null)
        {
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
            await pubSubService.PublishAsync(scope, message, cancellationToken);
        }
        catch (Exception)
        {
            // Ignore pub/sub errors to not affect memory operations
        }
    }
}
