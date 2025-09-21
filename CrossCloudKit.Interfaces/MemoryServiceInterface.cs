// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using CrossCloudKit.Interfaces.Classes;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.Interfaces;

/// <summary>
/// Modern async interface for abstracting Memory Services across multiple providers (Redis, Memcached, etc.)
/// Provides unified access with proper error handling and .NET 10 features.
/// </summary>
public interface IMemoryService : IAsyncDisposable
{
    /// <summary>
    /// Gets a value indicating whether the memory service has been successfully initialized.
    /// </summary>
    bool IsInitialized { get; }

    /// <summary>
    /// Method for acquiring a distributed mutex lock with time-to-live expiration.
    /// Note: Consider using <see cref="MemoryScopeMutex"/> which is a wrapper for lock/unlock based on the scope.
    /// This method is used internally by MemoryScopeMutex to implement thread-safe operations
    /// across distributed systems. The mutex prevents concurrent access to shared resources
    /// by multiple processes or application instances.
    /// </summary>
    /// <param name="memoryScope">
    /// The memory scope key for the operation.
    /// NOTE: <paramref name="timeToLive"/> will affect the entire scope!
    /// </param>
    /// <param name="mutexValue">
    /// The unique identifier for the mutex lock. This value should be consistent across
    /// all processes that need to coordinate access to the same resource.
    /// </param>
    /// <param name="timeToLive">
    /// NOTE: timeToLive will affect entire <paramref name="memoryScope"/>!
    /// The maximum duration of the lock should be held before automatically expiring.
    /// This prevents deadlocks in case the lock holder crashes or fails to release the lock.
    /// Should be set longer than the expected operation duration but not too long to avoid
    /// unnecessary blocking of other processes.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancellation token to allow early termination of the lock acquisition attempt.
    /// </param>
    /// <returns>
    /// An OperationResult containing a string value:
    /// - Non-null string containing the unique lock ID if the lock was successfully acquired
    /// - Null if the lock is already held by another process
    /// - Failure result if an error occurred during the operation
    /// The returned lock ID uniquely identifies this lock acquisition and must be used
    /// when releasing the lock to ensure only the lock holder can unlock it.
    /// </returns>
    Task<OperationResult<string?>> MemoryMutexLock(
        IMemoryScope memoryScope,
        string mutexValue,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Method for releasing a previously acquired distributed mutex lock.
    /// Note: Consider using <see cref="MemoryScopeMutex"/> which is a wrapper for lock/unlock based on the scope.
    /// This method is used internally by MemoryScopeMutex to clean up locks
    /// and allow other processes to acquire the mutex. Should only be called by
    /// the same process that successfully acquired the lock.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="mutexValue">
    /// The unique identifier for the mutex lock that was previously acquired.
    /// This must match the mutexValue used in the corresponding MemoryMutexLock call.
    /// </param>
    /// <param name="lockId">
    /// The unique lock ID that was returned by MemoryMutexLock when the lock was acquired.
    /// This ensures only the process that acquired the lock can release it.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancellation token to allow early termination of the unlock operation.
    /// </param>
    /// <returns>
    /// An OperationResult containing a boolean value:
    /// - True if the lock was successfully released by the correct lock holder
    /// - False if the lock was not found, already expired, or held by a different process
    /// - Failure result if an error occurred during the unlock operation
    /// </returns>
    Task<OperationResult<bool>> MemoryMutexUnlock(
        IMemoryScope memoryScope,
        string mutexValue,
        string lockId,
        CancellationToken cancellationToken = default);

    // Key-Value Operations

    /// <summary>
    /// Scans memory service for memory-scopes (keys) matching the specified pattern and returns them as a read-only collection.
    /// </summary>
    /// <param name="pattern">Pattern to match (e.g. "user:*" to match all scopes starting with "user:").</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>
    /// An <see cref="OperationResult{T}"/> containing the collection of matching memory scopes as
    /// <see cref="IReadOnlyCollection{String}"/> if successful, or a failure with an error message
    /// </returns>
    Task<OperationResult<IReadOnlyCollection<string>>> ScanMemoryScopesWithPattern(
        string pattern,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the expire time for the given memory scope key.
    /// </summary>
    /// <param name="memoryScope">The memory scope key to set expiration for.</param>
    /// <param name="timeToLive">Time to live duration.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the operation was successful; otherwise, false.</returns>
    Task<OperationResult<bool>> SetKeyExpireTimeAsync(
        IMemoryScope memoryScope,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the expire time for the given memory scope key.
    /// </summary>
    /// <param name="memoryScope">The memory scope key to check expiration for.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The time to live if found; otherwise, null.</returns>
    Task<OperationResult<TimeSpan?>> GetKeyExpireTimeAsync(
        IMemoryScope memoryScope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets key-value pairs within the given namespace and optionally publishes change notifications.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="keyValues">Collection of key-value pairs to set.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the operation was successful; otherwise, false.</returns>
    Task<OperationResult<bool>> SetKeyValuesAsync(
        IMemoryScope memoryScope,
        IEnumerable<KeyValuePair<string, PrimitiveType>> keyValues,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets a key-value pair conditionally (only if the key does not exist).
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="key">The key to set.</param>
    /// <param name="value">The value to set.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the key was set; false if it already existed.</returns>
    Task<OperationResult<bool>> SetKeyValueConditionallyAsync(
        IMemoryScope memoryScope,
        string key,
        PrimitiveType value,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets a key-value pair conditionally (only if the key does not exist) and gets the value after this operation.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="key">The key to set.</param>
    /// <param name="value">The value to set.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Pair of "True if the key was set; false if it already existed" and "value of the key after this operation"</returns>
    public Task<OperationResult<(bool newlySet, PrimitiveType? value)>> SetKeyValueConditionallyAndReturnValueRegardlessAsync(
            IMemoryScope memoryScope,
            string key,
            PrimitiveType value,
            bool publishChange = true,
            CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the value for the specified key within the given namespace.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="key">The key to retrieve.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The value if found; otherwise, null.</returns>
    Task<OperationResult<PrimitiveType?>> GetKeyValueAsync(
        IMemoryScope memoryScope,
        string key,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets values for multiple keys within the given namespace.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="keys">Collection of keys to retrieve.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Dictionary of found key-value pairs.</returns>
    Task<OperationResult<Dictionary<string, PrimitiveType>>> GetKeyValuesAsync(
        IMemoryScope memoryScope,
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all key-value pairs within the given namespace.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Dictionary of all key-value pairs in the namespace.</returns>
    Task<OperationResult<Dictionary<string, PrimitiveType>>> GetAllKeyValuesAsync(
        IMemoryScope memoryScope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a key within the given namespace and optionally publishes change notifications.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="key">The key to delete.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the key was deleted; otherwise, false.</returns>
    Task<OperationResult<bool>> DeleteKeyAsync(
        IMemoryScope memoryScope,
        string key,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all keys within the given namespace and optionally publishes change notifications.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if keys were deleted; otherwise, false.</returns>
    Task<OperationResult<bool>> DeleteAllKeysAsync(
        IMemoryScope memoryScope,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all keys within the given namespace.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Collection of keys in the namespace.</returns>
    Task<OperationResult<ReadOnlyCollection<string>>> GetKeysAsync(
        IMemoryScope memoryScope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the number of keys within the given namespace.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The number of keys in the namespace.</returns>
    Task<OperationResult<long>> GetKeysCountAsync(
        IMemoryScope memoryScope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Increments multiple keys by their respective values and optionally publishes change notifications.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="keyIncrements">Collection of key-increment value pairs.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Dictionary of keys and their new values after increment.</returns>
    Task<OperationResult<Dictionary<string, long>>> IncrementKeyValuesAsync(
        IMemoryScope memoryScope,
        IEnumerable<KeyValuePair<string, long>> keyIncrements,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Increments a key by the specified value and returns the new value.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="key">The key to increment.</param>
    /// <param name="incrementBy">The value to increment by.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The new value after increment.</returns>
    Task<OperationResult<long>> IncrementKeyByValueAndGetAsync(
        IMemoryScope memoryScope,
        string key,
        long incrementBy,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    // List Operations

    /// <summary>
    /// Pushes values to the tail of the specified list.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="values">Values to push to the list.</param>
    /// <param name="onlyIfListExists">Only push if the list already exists.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the operation was successful; otherwise, false.</returns>
    Task<OperationResult<bool>> PushToListTailAsync(
        IMemoryScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfListExists = false,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pushes values to the tail of the specified list only if they these values do not already exist in the list.
    /// Each value given is individually checked for existence.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="values">Values to push to the list if they don't exist.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Array of values that were successfully pushed</returns>
    Task<OperationResult<PrimitiveType[]>> PushToListTailIfValuesNotExistsAsync(
        IMemoryScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pushes values to the head of the specified list.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="values">Values to push to the list.</param>
    /// <param name="onlyIfListExists">Only push if the list already exists.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the operation was successful; otherwise, false.</returns>
    Task<OperationResult<bool>> PushToListHeadAsync(
        IMemoryScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfListExists = false,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pops the last element from the specified list.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The popped element if found; otherwise, null.</returns>
    Task<OperationResult<PrimitiveType?>> PopLastElementOfListAsync(
        IMemoryScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pops the first element from the specified list.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The popped element if found; otherwise, null.</returns>
    Task<OperationResult<PrimitiveType?>> PopFirstElementOfListAsync(
        IMemoryScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes given values from the specified list.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="values">Values to be removed from the list.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Removed values</returns>
    Task<OperationResult<IEnumerable<PrimitiveType?>>> RemoveElementsFromListAsync(
        IMemoryScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all elements from the specified list.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Collection of all elements in the list.</returns>
    Task<OperationResult<ReadOnlyCollection<PrimitiveType>>> GetAllElementsOfListAsync(
        IMemoryScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Empties the specified list.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the list was emptied; otherwise, false.</returns>
    Task<OperationResult<bool>> EmptyListAsync(
        IMemoryScope memoryScope,
        string listName,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Empties the specified list and all its sublists based on the prefix pattern.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the main list.</param>
    /// <param name="sublistPrefix">Prefix pattern for sublists.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the list was emptied; otherwise, false.</returns>
    Task<OperationResult<bool>> EmptyListAndSublistsAsync(
        IMemoryScope memoryScope,
        string listName,
        string sublistPrefix,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the size (number of elements) of the specified list.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The number of elements in the list.</returns>
    Task<OperationResult<long>> GetListSizeAsync(
        IMemoryScope memoryScope,
        string listName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if the specified list contains the given value.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="value">The value to search for.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the list contains the value; otherwise, false.</returns>
    Task<OperationResult<bool>> ListContainsAsync(
        IMemoryScope memoryScope,
        string listName,
        PrimitiveType value,
        CancellationToken cancellationToken = default);
}
