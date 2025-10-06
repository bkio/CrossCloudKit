// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Interfaces.Classes;

/// <summary>
/// A scope-based mutex wrapper around <see cref="IMemoryService"/>.
///
/// Usage pattern:
///
/// Async:
/// <code>
/// await using (var mutex = await MemoryScopeMutex.CreateScopeAsync(memoryService, memoryScope, "myKey", TimeSpan.FromSeconds(30)))
/// {
///     // Critical section (async-safe)
///     await DoSomethingAsync();
/// }
/// // Mutex is released here
/// </code>
///
/// Sync:
/// <code>
/// using (var mutex = MemoryScopeMutex.Create(memoryService, memoryScope, "myKey", TimeSpan.FromSeconds(30)))
/// {
///     // Critical section (sync-safe)
///     DoSomething();
/// }
/// // Mutex is released here
/// </code>
///
/// Notes:
/// - TTL ensures stale locks eventually expire if not released.
/// - Use <c>await using</c> whenever possible to avoid blocking threads.
/// - If unlock fails during Dispose/DisposeAsync, an exception is thrown (you may want to log instead).
/// - mutexValue cannot be "master". This word is reserved for the master mutex.
/// </summary>
public sealed class MemoryScopeMutex : IDisposable, IAsyncDisposable
{
    private readonly IMemoryService _memoryService;
    private readonly IMemoryScope _memoryScope;
    private readonly string? _mutexValue;
    private readonly TimeSpan _timeToLive;
    private readonly CancellationToken _cancellationToken;
    private string? _lockId;
    private readonly bool _isMasterLock;

    // Entity lock is used for the entity mutex
    private MemoryScopeMutex(
        IMemoryService memoryService,
        IMemoryScope memoryScope,
        string mutexValue,
        TimeSpan timeToLive,
        CancellationToken cancellationToken)
    {
        _memoryService = memoryService;
        _memoryScope = memoryScope;
        _mutexValue = mutexValue;
        _timeToLive = timeToLive;
        _isMasterLock = false;
        _cancellationToken = cancellationToken;
    }
    // Master lock is used for the master mutex
    private MemoryScopeMutex(
        IMemoryService memoryService,
        IMemoryScope memoryScope,
        TimeSpan timeToLive,
        CancellationToken cancellationToken)
    {
        _memoryService = memoryService;
        _memoryScope = memoryScope;
        _timeToLive = timeToLive;
        _isMasterLock = true;
        _cancellationToken = cancellationToken;
    }

    /// <summary>
    /// Creates and acquires a distributed mutex for a given memory scope asynchronously.
    /// This method is intended for use in async code and supports the <c>await using</c> pattern
    /// to ensure proper disposal and release of the mutex.
    /// </summary>
    /// <param name="memoryService">
    /// The <see cref="IMemoryService"/> instance used to manage the underlying memory locks.
    /// Cannot be <c>null</c>.
    /// </param>
    /// <param name="memoryScope">
    /// The <see cref="IMemoryScope"/> that defines the scope of the mutex.
    /// Locks acquired are limited to this scope.
    /// </param>
    /// <param name="mutexValue">
    /// A unique string identifier for the mutex within the given memory scope.
    /// <b>Important:</b> The value <c>"master"</c> is reserved for the master mutex and cannot be used here.
    /// </param>
    /// <param name="timeToLive">
    /// The maximum duration the mutex will be held before automatically expiring.
    /// Should be longer than the expected operation duration but not excessively long to avoid
    /// blocking other operations.
    /// </param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> that can be used to cancel the mutex acquisition asynchronously.
    /// </param>
    /// <returns>
    /// A <see cref="MemoryScopeMutex"/> instance representing the acquired lock.
    /// This object must be disposed (or used with <c>await using</c>) to release the mutex properly.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown if <paramref name="memoryService"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown if <paramref name="mutexValue"/> is <c>"master"</c>.
    /// </exception>
    /// <remarks>
    /// This method ensures that only one process or thread can hold a mutex with the same
    /// <paramref name="mutexValue"/> in the same <paramref name="memoryScope"/> at a time.
    /// Use this method to safely coordinate access to shared resources in distributed or concurrent environments.
    /// </remarks>
    public static async Task<MemoryScopeMutex> CreateEntityScopeAsync(
        IMemoryService memoryService,
        IMemoryScope memoryScope,
        string mutexValue,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(memoryService);
        if (mutexValue.Equals("master", StringComparison.CurrentCultureIgnoreCase))
            throw new ArgumentException("Mutex value cannot be 'master'");

        var scopeMutex = new MemoryScopeMutex(memoryService, memoryScope, mutexValue, timeToLive, cancellationToken);
        await scopeMutex.LockAsync();
        return scopeMutex;
    }

    /// <summary>
    /// Creates and acquires the master mutex for a given memory scope asynchronously.
    /// The master mutex is a special lock that prevents any other entity-specific locks
    /// within the same scope from being acquired while held.
    /// Recommended for use in async code with <c>await using</c>.
    /// </summary>
    /// <param name="memoryService">
    /// The <see cref="IMemoryService"/> instance used to manage the underlying memory locks.
    /// Cannot be <c>null</c>.
    /// </param>
    /// <param name="memoryScope">
    /// The <see cref="IMemoryScope"/> that defines the scope of the master mutex.
    /// Acquiring the master lock will prevent other locks in this scope from being obtained.
    /// </param>
    /// <param name="timeToLive">
    /// The maximum duration the master mutex will be held before automatically expiring.
    /// Should be longer than the expected operation duration but not excessively long
    /// to avoid blocking other operations within the scope.
    /// </param>
    /// <param name="cancellationToken">
    /// Optional <see cref="CancellationToken"/> to cancel the lock acquisition.
    /// </param>
    /// <returns>
    /// A <see cref="MemoryScopeMutex"/> instance representing the acquired master lock.
    /// Dispose or release the object to free the lock for other operations.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown if <paramref name="memoryService"/> is <c>null</c>.
    /// </exception>
    /// <remarks>
    /// Only one master mutex can exist per memory scope at a time.
    /// While the master lock is held, no entity-specific mutexes within the same scope
    /// can be acquired until it is released.
    /// </remarks>
    public static async Task<MemoryScopeMutex> CreateMasterScopeAsync(
        IMemoryService memoryService,
        IMemoryScope memoryScope,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(memoryService);

        var scopeMutex = new MemoryScopeMutex(memoryService, memoryScope, timeToLive, cancellationToken);
        await scopeMutex.LockAsync();
        return scopeMutex;
    }

    /// <summary>
    /// Creates and acquires a distributed mutex for a given memory scope synchronously.
    /// <b>Warning:</b> This method blocks the calling thread and should be avoided in asynchronous contexts.
    /// Prefer <see cref="CreateEntityScopeAsync"/> in async code.
    /// </summary>
    /// <param name="memoryService">
    /// The <see cref="IMemoryService"/> instance used to manage the underlying memory locks.
    /// Cannot be <c>null</c>.
    /// </param>
    /// <param name="memoryScope">
    /// The <see cref="IMemoryScope"/> that defines the scope of the mutex.
    /// Locks acquired are limited to this scope.
    /// </param>
    /// <param name="mutexValue">
    /// A unique string identifier for the mutex within the given memory scope.
    /// <b>Important:</b> The value <c>"master"</c> is reserved for the master mutex and cannot be used here.
    /// </param>
    /// <param name="timeToLive">
    /// The maximum duration the mutex will be held before automatically expiring.
    /// Should be longer than the expected operation duration but not excessively long to avoid
    /// blocking other operations.
    /// </param>
    /// <returns>
    /// A <see cref="MemoryScopeMutex"/> instance representing the acquired lock.
    /// This object must be disposed to release the mutex properly.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown if <paramref name="memoryService"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown if <paramref name="mutexValue"/> is <c>"master"</c>.
    /// </exception>
    /// <remarks>
    /// This method internally calls the asynchronous version and blocks until the mutex is acquired.
    /// It ensures that only one process or thread can hold a mutex with the same
    /// <paramref name="mutexValue"/> in the same <paramref name="memoryScope"/> at a time.
    /// </remarks>
    public static MemoryScopeMutex CreateEntityScope(
        IMemoryService memoryService,
        IMemoryScope memoryScope,
        string mutexValue,
        TimeSpan timeToLive)
        => CreateEntityScopeAsync(memoryService, memoryScope, mutexValue, timeToLive).GetAwaiter().GetResult();

    /// <summary>
    /// Creates and acquires the master mutex for a given memory scope synchronously.
    /// This method blocks the calling thread until the lock is acquired and should
    /// be avoided in asynchronous contexts to prevent thread starvation.
    /// The master mutex prevents any entity-specific locks within the same scope from
    /// being acquired while held.
    /// </summary>
    /// <param name="memoryService">
    /// The <see cref="IMemoryService"/> instance used to manage the underlying memory locks.
    /// Cannot be <c>null</c>.
    /// </param>
    /// <param name="memoryScope">
    /// The <see cref="IMemoryScope"/> that defines the scope of the master mutex.
    /// Acquiring the master lock will prevent other locks in this scope from being obtained.
    /// </param>
    /// <param name="timeToLive">
    /// The maximum duration the master mutex will be held before automatically expiring.
    /// Should be longer than the expected operation duration but not excessively long
    /// to avoid blocking other operations within the scope.
    /// </param>
    /// <returns>
    /// A <see cref="MemoryScopeMutex"/> instance representing the acquired master lock.
    /// Dispose or release the object to free the lock for other operations.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown if <paramref name="memoryService"/> is <c>null</c>.
    /// </exception>
    /// <remarks>
    /// Only one master mutex can exist per memory scope at a time.
    /// While the master lock is held, no entity-specific mutexes within the same scope
    /// can be acquired until it is released.
    /// </remarks>
    public static MemoryScopeMutex CreateMasterScope(
        IMemoryService memoryService,
        IMemoryScope memoryScope,
        TimeSpan timeToLive)
        => CreateMasterScopeAsync(memoryService, memoryScope, timeToLive).GetAwaiter().GetResult();

    /// <summary>
    /// Attempts to acquire the lock, retrying until successful or canceled.
    /// </summary>
    private async Task LockAsync()
    {
        while (!_cancellationToken.IsCancellationRequested)
        {
            var lockResult = _isMasterLock
                ? await _memoryService.MemoryMutexMasterLock(_memoryScope, _timeToLive, _cancellationToken)
                : await _memoryService.MemoryMutexLock(_memoryScope, _mutexValue.NotNull(), _timeToLive, _cancellationToken);
            if (!lockResult.IsSuccessful)
                throw new Exception($"Memory service - Lock failed with: {lockResult.ErrorMessage}");

            var lockId = lockResult.Data;
            if (lockId != null)
            {
                _lockId = lockId;
                break;
            }

            // Backoff before retrying
            // Random is to avoid “thundering herd” contention
            await Task.Delay(Random.Shared.Next(50, 150), _cancellationToken);
        }

        // If we exit the loop, cancellation was requested
        _cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Releases the mutex asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_lockId == null)
            return; // No lock to release

        var unlockResult = _isMasterLock
            ? await _memoryService.MemoryMutexMasterUnlock(_memoryScope, _lockId, _cancellationToken)
            : await _memoryService.MemoryMutexUnlock(_memoryScope, _mutexValue.NotNull(), _lockId, _cancellationToken);
        if (!unlockResult.IsSuccessful)
        {
            throw new Exception($"Memory service - Unlock failed with: {unlockResult.ErrorMessage}");
        }

        _lockId = null;
    }

    /// <summary>
    /// Releases the mutex synchronously.
    /// Use only in non-async contexts (blocks until released).
    /// </summary>
    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}

