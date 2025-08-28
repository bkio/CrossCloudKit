// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using Utilities.Common;

namespace Cloud.Interfaces;

/// <summary>
/// Extend this interface to define a memory scope.
/// </summary>
public interface IMemoryServiceScope
{
    public string Compile();
}

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

    // Key-Value Operations

    /// <summary>
    /// Sets the expire time for the given memory scope key.
    /// </summary>
    /// <param name="memoryScope">The memory scope key to set expiration for.</param>
    /// <param name="timeToLive">Time to live duration.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the operation was successful; otherwise, false.</returns>
    Task<OperationResult<bool>> SetKeyExpireTimeAsync(
        IMemoryServiceScope memoryScope,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the expire time for the given memory scope key.
    /// </summary>
    /// <param name="memoryScope">The memory scope key to check expiration for.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The time to live if found; otherwise, null.</returns>
    Task<OperationResult<TimeSpan?>> GetKeyExpireTimeAsync(
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all key-value pairs within the given namespace.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Dictionary of all key-value pairs in the namespace.</returns>
    Task<OperationResult<Dictionary<string, PrimitiveType>>> GetAllKeyValuesAsync(
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all keys within the given namespace.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>Collection of keys in the namespace.</returns>
    Task<OperationResult<ReadOnlyCollection<string>>> GetKeysAsync(
        IMemoryServiceScope memoryScope,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the number of keys within the given namespace.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The number of keys in the namespace.</returns>
    Task<OperationResult<long>> GetKeysCountAsync(
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
    /// <param name="onlyIfExists">Only push if the list already exists.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the operation was successful; otherwise, false.</returns>
    Task<OperationResult<bool>> PushToListTailAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfExists = false,
        bool publishChange = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pushes values to the head of the specified list.
    /// </summary>
    /// <param name="memoryScope">The memory scope key for the operation.</param>
    /// <param name="listName">The name of the list.</param>
    /// <param name="values">Values to push to the list.</param>
    /// <param name="onlyIfExists">Only push if the list already exists.</param>
    /// <param name="publishChange">Whether to publish change notifications.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>True if the operation was successful; otherwise, false.</returns>
    Task<OperationResult<bool>> PushToListHeadAsync(
        IMemoryServiceScope memoryScope,
        string listName,
        IEnumerable<PrimitiveType> values,
        bool onlyIfExists = false,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
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
        IMemoryServiceScope memoryScope,
        string listName,
        PrimitiveType value,
        CancellationToken cancellationToken = default);
}
