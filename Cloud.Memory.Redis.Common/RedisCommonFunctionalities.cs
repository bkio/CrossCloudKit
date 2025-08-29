// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using Cloud.Interfaces;
using Newtonsoft.Json;
using StackExchange.Redis;
using Utilities.Common;

// ReSharper disable MemberCanBePrivate.Global

namespace Cloud.Memory.Redis.Common;

/// <summary>
/// Redis connection configuration options record for dependency injection and immutability.
/// </summary>
public sealed record RedisConnectionOptions
{
    public required string Host { get; init; }
    public required int Port { get; init; }
    public string? Username { get; init; }
    public string? Password { get; init; }
    public bool SslEnabled { get; init; }
    public bool EnableRetryPolicy { get; init; } = true;
    public int RetryAttempts { get; init; } = 3;
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan SyncTimeout { get; init; } = TimeSpan.FromSeconds(20);
}

/// <summary>
/// Base class providing common Redis functionality with modern async patterns and proper resource management.
/// </summary>
public abstract class RedisCommonFunctionalities : IAsyncDisposable
{
    private readonly RedisConnectionOptions _options;
    private readonly SemaphoreSlim _connectionSemaphore;
    private readonly ConfigurationOptions _redisConfig;
    private readonly Action<string>? _errorMessageAction;

    protected ConnectionMultiplexer? RedisConnection { get; private set; }

    protected bool Initialized;

    protected RedisCommonFunctionalities(
        RedisConnectionOptions options,
        Action<string>? errorMessageAction = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _errorMessageAction = errorMessageAction;
        _connectionSemaphore = new SemaphoreSlim(1, 1);

        _redisConfig = CreateRedisConfiguration();

        var initTask = InitializeConnectionAsync();
        initTask.Wait();
    }

    private ConfigurationOptions CreateRedisConfiguration()
    {
        var config = new ConfigurationOptions
        {
            EndPoints = { { _options.Host, _options.Port } },
            SyncTimeout = (int)_options.SyncTimeout.TotalMilliseconds,
            AbortOnConnectFail = false,
            ConnectRetry = _options.RetryAttempts,
            Ssl = _options.SslEnabled,
            ReconnectRetryPolicy = new ExponentialRetry((int)_options.RetryDelay.TotalMilliseconds)
        };

        if (!string.IsNullOrEmpty(_options.Username))
        {
            config.User = _options.Username;
        }

        if (!string.IsNullOrEmpty(_options.Password) && _options.Password != "N/A")
        {
            config.Password = _options.Password;
        }

        return config;
    }

    private async Task InitializeConnectionAsync()
    {
        try
        {
            if (Initialized)
                return;

            _errorMessageAction?.Invoke($"Initializing Redis connection to {_options.Host}:{_options.Port}");

            RedisConnection = await ConnectionMultiplexer.ConnectAsync(_redisConfig).ConfigureAwait(false);
            Initialized = RedisConnection.IsConnected;

            if (Initialized)
            {
                _errorMessageAction?.Invoke("Redis connection initialized successfully");

                // Setup connection event handlers
                RedisConnection.ConnectionFailed += OnConnectionFailed;
                RedisConnection.ConnectionRestored += OnConnectionRestored;
            }
            else
            {
                _errorMessageAction?.Invoke("Failed to initialize Redis connection");
            }
        }
        catch (Exception ex)
        {
            _errorMessageAction?.Invoke($"Error initializing Redis connection: {ex.Message}, Trace: {ex.StackTrace}");
            Initialized = false;
        }
    }

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs e)
    {
        _errorMessageAction?.Invoke($"Redis connection failed: {e.Exception?.Message}");
    }

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e)
    {
        _errorMessageAction?.Invoke("Redis connection restored");
    }

    /// <summary>
    /// Ensures the Redis connection is available and ready for use.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the connection is available; otherwise, false.</returns>
    private async ValueTask<bool> EnsureConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (Initialized && RedisConnection is { IsConnected: true })
            return true;

        await _connectionSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Initialized && RedisConnection is { IsConnected: true })
                return true;

            _errorMessageAction?.Invoke("Attempting to reconnect to Redis");
            await InitializeConnectionAsync().ConfigureAwait(false);

            return Initialized;
        }
        finally
        {
            _connectionSemaphore.Release();
        }
    }

    /// <summary>
    /// Executes a Redis operation with retry logic and proper error handling.
    /// </summary>
    /// <typeparam name="T">The return type of the operation.</typeparam>
    /// <param name="operation">The Redis operation to execute.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The result of the operation.</returns>
    protected async Task<OperationResult<T>> ExecuteRedisOperationAsync<T>(
        Func<IDatabase, ValueTask<T>> operation,
        CancellationToken cancellationToken = default)
    {
        if (RedisConnection == null)
            return OperationResult<T>.Failure("Redis connection is not initialized");

        if (!await EnsureConnectionAsync(cancellationToken).ConfigureAwait(false))
        {
            return OperationResult<T>.Failure("Redis connection is not available");
        }

        var database = RedisConnection.GetDatabase();

        if (!_options.EnableRetryPolicy)
        {
            try
            {
                return OperationResult<T>.Success(await operation(database).ConfigureAwait(false));
            }
            catch (Exception ex)
            {
                return OperationResult<T>.Failure($"Redis operation failed: {ex.Message}, Trace: {ex.StackTrace}");
            }
        }

        var attempts = 0;
        var maxAttempts = _options.RetryAttempts;
        Exception? lastException = null;

        while (attempts < maxAttempts)
        {
            try
            {
                return OperationResult<T>.Success(await operation(database).ConfigureAwait(false));
            }
            catch (Exception ex) when (IsRetriableException(ex) && attempts < maxAttempts - 1)
            {
                attempts++;
                lastException = ex;

                await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);

                // Try to ensure connection is still valid
                if (!await EnsureConnectionAsync(cancellationToken).ConfigureAwait(false))
                {
                    return OperationResult<T>.Failure("Redis connection lost and could not be restored");
                }

                database = RedisConnection.GetDatabase();
            }
            catch (Exception ex)
            {
                return OperationResult<T>.Failure($"Redis operation failed: {ex.Message}, Trace: {ex.StackTrace}");
            }
        }

        return OperationResult<T>.Failure($"Maximum retry attempts ({maxAttempts}) exceeded. Last error: {lastException?.Message}");
    }

    /// <summary>
    /// Determines if an exception is retriable.
    /// </summary>
    private static bool IsRetriableException(Exception exception)
    {
        return exception is RedisException or TimeoutException or RedisConnectionException;
    }

    protected static RedisValue ConvertPrimitiveTypeToRedisValue(PrimitiveType value)
    {
        return JsonConvert.SerializeObject(value);
    }
    protected static PrimitiveType? ConvertRedisValueToPrimitiveType(RedisValue input)
    {
        return input.IsNullOrEmpty ? null : JsonConvert.DeserializeObject<PrimitiveType>(input.ToString());
    }
    // ReSharper disable once UnusedMethodReturnValue.Local
    protected static async Task<OperationResult<bool>> PublishChangeNotificationAsync<T>(
        IPubSubService? pubSubService,
        IMemoryServiceScope memoryScope,
        string operation,
        T? changes,
        CancellationToken cancellationToken)
    {
        if (pubSubService is null)
            return OperationResult<bool>.Failure("Pub/Sub service is not configured.");

        var scope = memoryScope.Compile();

        try
        {
            var notification = new
            {
                operation,
                changes
            };

            var message = JsonConvert.SerializeObject(notification);
            return await pubSubService.PublishAsync(scope, message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return OperationResult<bool>.Failure($"Failed to publish change notification for operation {operation} on scope {memoryScope}: {ex.Message}, Trace: {ex.StackTrace}");
        }
    }
    protected async Task<OperationResult<bool>> Common_PushToListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool toTail,
        bool onlyIfExists,
        bool publishChange,
        IPubSubService? pubSubService,
        CancellationToken cancellationToken)
    {
        var valueArray = values.ToArray();
        if (valueArray.Length == 0)
            return OperationResult<bool>.Failure("Value array is empty.");

        var scope = memoryScope.Compile();
        var listKey = $"{scope}:{listName}";
        var redisValues = valueArray.Select(ConvertPrimitiveTypeToRedisValue).ToArray();

        var result = await ExecuteRedisOperationAsync(async database =>
        {
            bool success;

            if (onlyIfExists)
            {
                var transaction = database.CreateTransaction();
                transaction.AddCondition(Condition.KeyExists(listKey));

                _ = toTail
                    ? transaction.ListRightPushAsync(listKey, redisValues)
                    : transaction.ListLeftPushAsync(listKey, redisValues);

                var committed = await transaction.ExecuteAsync().ConfigureAwait(false);
                success = committed;
            }
            else
            {
                var count = toTail
                    ? await database.ListRightPushAsync(listKey, redisValues).ConfigureAwait(false)
                    : await database.ListLeftPushAsync(listKey, redisValues).ConfigureAwait(false);
                success = count > 0;
            }

            if (!success) return false;

            var ttl = await database.KeyTimeToLiveAsync(scope).ConfigureAwait(false);
            if (ttl.HasValue)
            {
                await database.KeyExpireAsync(listKey, ttl.Value).ConfigureAwait(false);
            }
            return true;

        }, cancellationToken).ConfigureAwait(false);

        if (result.IsSuccessful && publishChange && pubSubService is not null)
        {
            var operation = toTail ? "PushToListTail" : "PushToListHead";
            await PublishChangeNotificationAsync(pubSubService, memoryScope, operation,
                new { List = listName, Pushed = valueArray }, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }
    protected async Task<OperationResult<PrimitiveType?>> Common_PopFromListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        bool fromTail,
        bool publishChange,
        IPubSubService? pubSubService,
        CancellationToken cancellationToken)
    {
        var listKey = $"{memoryScope.Compile()}:{listName}";

        var poppedValue = await ExecuteRedisOperationAsync(async database => fromTail
            ? await database.ListRightPopAsync(listKey).ConfigureAwait(false)
            : await database.ListLeftPopAsync(listKey).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);

        if (!poppedValue.IsSuccessful)
            return OperationResult<PrimitiveType?>.Failure($"Redis operation failed(1): {poppedValue.ErrorMessage}");

        var primitiveValue = ConvertRedisValueToPrimitiveType(poppedValue.Data);
        if (primitiveValue is null)
            return OperationResult<PrimitiveType?>.Failure($"Redis operation failed(2).");

        if (publishChange && pubSubService is not null)
        {
            var operation = fromTail ? "PopLastElementOfList" : "PopFirstElementOfList";
            await PublishChangeNotificationAsync(pubSubService, memoryScope, operation,
                new { List = listName, Popped = primitiveValue }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<PrimitiveType?>.Success(primitiveValue);
    }
    protected async Task<OperationResult<IEnumerable<PrimitiveType?>>> Common_RemoveElementsFromListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool publishChange = true,
        IPubSubService? pubSubService = null,
        CancellationToken cancellationToken = default)
    {
        var valueArray = values.ToArray();
        if (valueArray.Length == 0)
            return OperationResult<IEnumerable<PrimitiveType?>>.Failure("Value array is empty.");

        var listKey = $"{memoryScope.Compile()}:{listName}";
        var redisValues = valueArray.Select(ConvertPrimitiveTypeToRedisValue).ToArray();

        var result = await ExecuteRedisOperationAsync(async database =>
        {
            var tasks = redisValues.Select(async redisValue =>
            {
                var removeResult = await database.ListRemoveAsync(listKey, redisValue);
                var hasRemoved = removeResult > 0;
                return (redisValue, hasRemoved);
            });
            var res = await Task.WhenAll(tasks);
            return res.Where(r => r.hasRemoved).Select(s => s.redisValue).Select(ConvertRedisValueToPrimitiveType);

        }, cancellationToken).ConfigureAwait(false);

        if (result.IsSuccessful && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "RemoveElementsFromList",
                new { List = listName, Removed = valueArray }, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }
    protected async Task<OperationResult<ReadOnlyCollection<PrimitiveType>>> Common_GetAllElementsOfListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        var listKey = $"{memoryScope.Compile()}:{listName}";

        return await ExecuteRedisOperationAsync(async database =>
        {
            var values = await database.ListRangeAsync(listKey).ConfigureAwait(false);
            if (values.Length == 0)
                return new List<PrimitiveType>(0).AsReadOnly();

            var result = new List<PrimitiveType>(values.Length);
            result.AddRange(values.Select(ConvertRedisValueToPrimitiveType).OfType<PrimitiveType>());

            return result.AsReadOnly();
        }, cancellationToken).ConfigureAwait(false);
    }
    protected async Task<OperationResult<bool>> Common_EmptyListAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        bool publishChange = true,
        IPubSubService? pubSubService = null,
        CancellationToken cancellationToken = default)
    {
        var listKey = $"{memoryScope.Compile()}:{listName}";

        var result = await ExecuteRedisOperationAsync(async database => await database.KeyDeleteAsync(listKey).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);

        if (result.IsSuccessful && publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "EmptyList", new { List = listName }, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }
    protected async Task<OperationResult<bool>> Common_EmptyListAndSublistsAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        string sublistPrefix,
        bool publishChange = true,
        IPubSubService? pubSubService = null,
        CancellationToken cancellationToken = default)
    {
        const string script = """
          local results = redis.call('lrange', KEYS[1], 0, -1)
          for _, key in ipairs(results) do
              redis.call('del', ARGV[1] .. key)
          end
          redis.call('del', KEYS[1])
          """;

        var scope = memoryScope.Compile();
        var result = await ExecuteRedisOperationAsync(async database =>
        {
            await database.ScriptEvaluateAsync(script,
                [$"{scope}:{listName}"],
                [$"{scope}:{sublistPrefix}"]).ConfigureAwait(false);

            return ValueTask.CompletedTask;
        }, cancellationToken).ConfigureAwait(false);

        if (!result.IsSuccessful)
            return OperationResult<bool>.Failure($"Redis operation failed: {result.ErrorMessage}");

        if (publishChange && pubSubService is not null)
        {
            await PublishChangeNotificationAsync(pubSubService, memoryScope, "EmptyListAndSublists",
                new { List = listName, SublistPrefix = sublistPrefix }, cancellationToken).ConfigureAwait(false);
        }

        return OperationResult<bool>.Success(true);
    }
    protected async Task<OperationResult<long>> Common_GetListSizeAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default)
    {
        var listKey = $"{memoryScope.Compile()}:{listName}";

        return await ExecuteRedisOperationAsync(async database => await database.ListLengthAsync(listKey).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Disposes the Redis connection and other resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (RedisConnection is not null)
        {
            RedisConnection.ConnectionFailed -= OnConnectionFailed;
            RedisConnection.ConnectionRestored -= OnConnectionRestored;

            await RedisConnection.DisposeAsync().ConfigureAwait(false);
        }

        _connectionSemaphore.Dispose();

        GC.SuppressFinalize(this);
    }
    protected async Task<OperationResult<bool>> Common_ListContainsAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        PrimitiveType value,
        CancellationToken cancellationToken = default)
    {
        var elements = await Common_GetAllElementsOfListAsync(memoryScope, listName, cancellationToken).ConfigureAwait(false);
        if (!elements.IsSuccessful || elements.Data == null)
            return OperationResult<bool>.Failure($"Redis operation failed: {elements.ErrorMessage}");
        return OperationResult<bool>.Success(elements.Data.Any(element => element.Equals(value)));
    }

    public static RedisConnectionOptions GetRedisConnectionOptionsFromEnvironmentForTesting()
    {
        var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST");
        var redisPortStr = Environment.GetEnvironmentVariable("REDIS_PORT");
        var redisUser = Environment.GetEnvironmentVariable("REDIS_USER");
        var redisPassword = Environment.GetEnvironmentVariable("REDIS_PASSWORD");
        if (string.IsNullOrEmpty(redisHost)
            || string.IsNullOrEmpty(redisPortStr) || !int.TryParse(redisPortStr, out var redisPort)
            || string.IsNullOrEmpty(redisUser)
            || string.IsNullOrEmpty(redisPassword))
        {
            throw new ArgumentException("Redis credentials are not set in environment variables.");
        }

        return new RedisConnectionOptions()
        {
            Host = redisHost,
            Port = redisPort,
            Username = redisUser,
            Password = redisPassword,
            EnableRetryPolicy = true,
            SslEnabled = false
        };
    }
}
