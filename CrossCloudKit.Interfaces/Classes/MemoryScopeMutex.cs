// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

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
/// </summary>
public sealed class MemoryScopeMutex : IDisposable, IAsyncDisposable
{
    private readonly IMemoryService _memoryService;
    private readonly IMemoryScope _memoryScope;
    private readonly string _mutexValue;
    private readonly TimeSpan _timeToLive;
    private readonly CancellationToken _cancellationToken;
    private string? _lockId;

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
        _cancellationToken = cancellationToken;
    }

    /// <summary>
    /// Creates and acquires a mutex asynchronously.
    /// Recommended in async code with <c>await using</c>.
    /// NOTE: <paramref name="timeToLive"/> will affect the entire <paramref name="memoryScope"/>! Consider creating a separate scope for this operation.
    /// </summary>
    // ReSharper disable once MemberCanBePrivate.Global
    public static async Task<MemoryScopeMutex> CreateScopeAsync(
        IMemoryService memoryService,
        IMemoryScope memoryScope,
        string mutexValue,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(memoryService);

        var scopeMutex = new MemoryScopeMutex(memoryService, memoryScope, mutexValue, timeToLive, cancellationToken);
        await scopeMutex.LockAsync();
        return scopeMutex;
    }

    /// <summary>
    /// Creates and acquires a mutex synchronously.
    /// Avoid it in async contexts (may block threads).
    /// NOTE: <paramref name="timeToLive"/> will affect the entire <paramref name="memoryScope"/>! Consider creating a separate scope for this operation.
    /// </summary>
    public static MemoryScopeMutex CreateScope(
        IMemoryService memoryService,
        IMemoryScope memoryScope,
        string mutexValue,
        TimeSpan timeToLive)
        => CreateScopeAsync(memoryService, memoryScope, mutexValue, timeToLive).GetAwaiter().GetResult();

    /// <summary>
    /// Attempts to acquire the lock, retrying until successful or canceled.
    /// </summary>
    private async Task LockAsync()
    {
        while (!_cancellationToken.IsCancellationRequested)
        {
            var lockResult = await _memoryService.MemoryMutexLock(_memoryScope, _mutexValue, _timeToLive, _cancellationToken);
            if (!lockResult.IsSuccessful)
                throw new Exception($"Memory service - Lock failed with: {lockResult.ErrorMessage}");

            var lockId = lockResult.Data;
            if (lockId != null)
            {
                _lockId = lockId;
                break;
            }

            // Backoff before retrying
            await Task.Delay(100, _cancellationToken);
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

        var unlockResult = await _memoryService.MemoryMutexUnlock(_memoryScope, _mutexValue, _lockId, _cancellationToken);
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

